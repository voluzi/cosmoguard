package cosmoguard

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// migrationInterval is how often the broker scans its pinned WS
// subscriptions for unhealthy upstreams and re-subscribes them on a
// survivor. Sized below connectRetryPeriod (5s) so a restarting
// cosmos node — which takes ≥30s — doesn't keep clients without
// events for a whole reconnect cycle.
const migrationInterval = 2 * time.Second

type Broker struct {
	IdGen          *util.UniqueID
	pool           *UpstreamPool
	log            *Entry
	sm             *SubscriptionManager
	upstreamSubMux sync.Mutex
	stopMigrate    chan struct{}
	// migrateDone is closed by migrateLoop when it exits, so Stop()
	// can wait for it before tearing down the pool. Without the
	// join, runMigration could be mid-pool.MigrateUnhealthy while
	// pool.Stop() closes the same conns underneath it — touching
	// freed state. Closed exactly once by the loop goroutine.
	migrateDone chan struct{}
	// migrateStarted gates the join in Stop: if Start was never
	// called, no goroutine will ever close migrateDone and
	// `<-migrateDone` would deadlock.
	migrateStarted atomic.Bool
	startOnce      sync.Once
	stopOnce       sync.Once
}

// NewBroker constructs a broker fronted by a pool of WS upstream
// connections. backends is the list of backend hosts (host:port). When
// the list has one entry, the pool dials all n connections to it (v3
// behavior). When multiple, connections are spread evenly across all
// backends so a single cosmoguard replica can absorb load across nodes.
//
// Subscriptions stick to the upstream connection they were placed on
// for normal operation. When that connection becomes persistently
// unhealthy (see UpstreamConnManager.IsHealthy), a background
// migration loop (Start → migrateLoop, every migrationInterval)
// re-issues the subscription on a healthy backend; the upstream's new
// ID gets rewired through the SubscriptionManager so client-facing
// IDs (Cosmos) keep working through the swap.
func NewBroker(backends []string, path string, n int, upstreamConstructor UpstreamConnManagerConstructor) *Broker {
	b := Broker{
		IdGen:       &util.UniqueID{},
		sm:          NewSubscriptionManager(),
		stopMigrate: make(chan struct{}),
		migrateDone: make(chan struct{}),
	}
	b.pool = NewUpstreamPool(backends, path, n, b.onSubscriptionMessage, upstreamConstructor)
	return &b
}

func (b *Broker) Start(log *Entry) error {
	b.log = log
	if err := b.pool.Start(log); err != nil {
		return err
	}
	// Cross-backend sticky-subscription re-establishment. Walks the
	// pool's pinned subscriptions on a timer; subscriptions whose
	// upstream is unhealthy get re-issued on a survivor connection
	// (subscription ID changes; the SubscriptionManager rewires its
	// internal maps so clients keep receiving events without noticing).
	//
	// Idempotent: subsequent Start calls are no-ops, so harness tests
	// or hot-reload paths can't accidentally spawn duplicate
	// migrators.
	b.startOnce.Do(func() {
		b.migrateStarted.Store(true)
		go b.migrateLoop()
	})
	return nil
}

// Stop cancels the background subscription migrator AND tells every
// pool conn manager to shut down its Run loop. Safe to call multiple
// times. Joins the migrator goroutine before tearing down the pool
// so a concurrent runMigration() can't touch conns that pool.Stop()
// is freeing.
func (b *Broker) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopMigrate)
		// Join the migrator goroutine before yanking the pool. Only
		// safe when Start actually spawned it — otherwise nothing
		// will ever close migrateDone and the receive would hang.
		if b.migrateStarted.Load() {
			<-b.migrateDone
		}
		if b.pool != nil {
			b.pool.Stop()
		}
	})
}

func (b *Broker) migrateLoop() {
	defer close(b.migrateDone)
	t := time.NewTicker(migrationInterval)
	defer t.Stop()
	for {
		select {
		case <-b.stopMigrate:
			return
		case <-t.C:
			b.runMigration()
		}
	}
}

func (b *Broker) runMigration() {
	b.upstreamSubMux.Lock()
	defer b.upstreamSubMux.Unlock()

	for _, m := range b.pool.MigrateUnhealthy() {
		if !b.sm.ReplaceSubscriptionID(m.OldID, m.NewID) {
			b.log.WithFields(Fields{
				"oldID": m.OldID,
				"newID": m.NewID,
				"param": m.Param,
			}).Warn("migration: subscription manager unaware of oldID")
		}
	}
}

// SubStats returns the active upstream subscriptions enriched with the
// backend each is pinned to. One row per upstream subscription, not per
// client — the broker dedups identical params onto a single upstream
// subscription and fans the result out, so Subscribers is the fan-out
// width.
func (b *Broker) SubStats() []WSSubInfo {
	subs := b.sm.Snapshot()
	out := make([]WSSubInfo, 0, len(subs))
	for _, s := range subs {
		out = append(out, WSSubInfo{
			Param:       s.Param,
			Subscribers: s.Subscribers,
			Upstream:    b.pool.SubscriptionTarget(s.ID),
		})
	}
	return out
}

// UpstreamStats returns the per-connection upstream pool snapshot.
func (b *Broker) UpstreamStats() []ConnStat { return b.pool.ConnStats() }

// ClientSubCount returns how many upstream subscriptions a single client
// is fanned out from.
func (b *Broker) ClientSubCount(c *JsonRpcWsClient) int {
	return len(b.sm.GetSubscriptions(c))
}

func (b *Broker) HandleRequest(msg *JsonRpcMsg) (*JsonRpcMsg, error) {
	res, err := b.pool.MakeRequest(msg)
	if err != nil {
		return nil, err
	}
	return res.CloneWithID(msg.ID), nil
}

func (b *Broker) HandleSubscription(client *JsonRpcWsClient, msg *JsonRpcMsg) (*JsonRpcMsg, error) {
	b.log.WithField("client", client).Debug("handling subscription")
	client.SetOnDisconnectCallback(b.onClientDisconnect)

	switch msg.Method {
	case methodSubscribeCosmos:
		_, err := b.addSubscription(client, msg)
		if err != nil {
			return ErrorResponse(msg, -100, err.Error(), nil), nil
		}
		return EmptyResult(msg), nil

	case methodUnsubscribeCosmos:
		if err := b.removeSubscription(client, msg); err != nil {
			return ErrorResponse(msg, -100, err.Error(), nil), nil
		}
		return EmptyResult(msg), nil

	case methodUnsubscribeAllCosmos:
		if err := b.removeAllSubscriptions(client); err != nil {
			return nil, err
		}
		return EmptyResult(msg), nil

	case methodSubscribeEth:
		id, err := b.addSubscription(client, msg)
		if err != nil {
			return ErrorResponse(msg, -100, err.Error(), nil), nil
		}
		return WithResult(msg, id), nil

	case methodUnsubscribeEth:
		if err := b.removeSubscription(client, msg); err != nil {
			return ErrorResponse(msg, -100, err.Error(), nil), nil
		}
		return WithResult(msg, true), nil

	default:
		// This should never happen
		return nil, fmt.Errorf("unsupported method on subscriptions")
	}
}

func (b *Broker) addSubscription(client *JsonRpcWsClient, msg *JsonRpcMsg) (string, error) {
	param, err := getSubscriptionParam(msg)
	if err != nil {
		return "", err
	}

	b.upstreamSubMux.Lock()
	defer b.upstreamSubMux.Unlock()

	id, exists := b.sm.GetSubscriptionID(param)
	if !exists {
		b.log.WithField("client", client).Debug("upstream subscription does not exist")

		// Subscribe in upstream
		id, err = b.pool.Subscribe(param)
		if err != nil {
			return "", err
		}
		b.sm.AddSubscription(param, id)

		b.log.WithFields(map[string]interface{}{
			"id":    id,
			"param": param,
		}).Info("subscribed upstream")
	}

	if msg.Method == methodSubscribeEth {
		// Eth does not send the ID on subscription notifications. Lets do the same
		b.sm.SubscribeClient(id, client, nil)
	} else {
		b.sm.SubscribeClient(id, client, msg.ID)
	}

	b.log.WithFields(map[string]interface{}{
		"id":     id,
		"client": client,
	}).Debug("subscribed client")

	return id, nil
}

func (b *Broker) removeSubscription(client *JsonRpcWsClient, msg *JsonRpcMsg) error {
	b.log.WithField("client", client).Debug("unsubscribing client")
	subParam, err := getSubscriptionParam(msg)
	if err != nil {
		return err
	}

	var subID string
	if isEthSubscriptionID(subParam) {
		subID = subParam
	} else {
		id, exists := b.sm.GetSubscriptionID(subParam)
		if !exists {
			return fmt.Errorf("subscription does not exist")
		}
		subID = id
	}

	if !b.sm.ClientSubscribed(subID, client) {
		return fmt.Errorf("subscription does not exist")
	}

	b.sm.UnsubscribeClient(subID, client)
	b.log.WithFields(map[string]interface{}{
		"id":     subID,
		"client": client,
	}).Debug("unsubscribed client")

	b.upstreamSubMux.Lock()
	defer b.upstreamSubMux.Unlock()

	if b.sm.SubscriptionEmpty(subID) {
		if err = b.pool.Unsubscribe(subID); err != nil {
			return err
		}
		param, _ := b.sm.GetSubscriptionParam(subID)
		b.sm.RemoveSubscription(subID)

		b.log.WithFields(map[string]interface{}{
			"ID":    subID,
			"param": param,
		}).Warn("unsubscribed upstream")
	}

	return nil
}

func (b *Broker) removeAllSubscriptions(client *JsonRpcWsClient) error {
	b.log.WithField("client", client).Debug("unsubscribing client from all subscriptions")

	b.upstreamSubMux.Lock()
	defer b.upstreamSubMux.Unlock()

	// Collect errors across the loop instead of returning on the
	// first failure. Previously a single transient pool.Unsubscribe
	// error (e.g. upstream WS hiccup) aborted the loop mid-flight,
	// leaving the remaining subscriptions with UnsubscribeClient
	// already called (so the client is gone from sm) but
	// pool.subscriptionConn still pinned — a permanent upstream
	// subscription leak for every subsequent sub of this client
	// (called from onClientDisconnect, so the client is gone for
	// good). Now every sub gets its chance to drain; the joined
	// error still surfaces to the caller via errors.Join semantics.
	var errs []error
	for _, subscriptionID := range b.sm.GetSubscriptions(client) {
		b.log.WithField("ID", subscriptionID).Debug("unsubscribing client")
		b.sm.UnsubscribeClient(subscriptionID, client)

		if b.sm.SubscriptionEmpty(subscriptionID) {
			b.log.WithField("ID", subscriptionID).Debug("unsubscribing upstream")
			if err := b.pool.Unsubscribe(subscriptionID); err != nil {
				errs = append(errs, fmt.Errorf("subscription %s: %w", subscriptionID, err))
				continue
			}
			b.sm.RemoveSubscription(subscriptionID)

			param, _ := b.sm.GetSubscriptionParam(subscriptionID)
			b.log.WithFields(map[string]interface{}{
				"ID":    subscriptionID,
				"param": param,
			}).Warn("unsubscribed upstream")

		}
	}
	return errors.Join(errs...)
}

func (b *Broker) onSubscriptionMessage(msg *JsonRpcMsg) {
	// Upstream is expected to echo the subscription ID (a string we
	// minted via util.UniqueID). Defend against malformed upstreams
	// that send a non-string ID — a raw type assertion here used to
	// panic the broker's read goroutine.
	msgID, ok := msg.ID.(string)
	if !ok {
		b.log.WithField("ID", msg.ID).Warn("dropped subscription message with non-string ID")
		return
	}

	clients := b.sm.GetSubscriptionClients(msgID)
	if len(clients) == 0 {
		b.log.WithField("ID", msg.ID).Warn("no subscribers for message")
		return
	}

	b.log.WithFields(map[string]interface{}{
		"ID":      msgID,
		"clients": len(clients),
	}).Info("broadcasting message to subscribers")

	for client, id := range clients {
		go func(client *JsonRpcWsClient, id interface{}) {
			if err := client.SendMsg(msg.CloneWithID(id)); err != nil {
				b.log.Errorf("error sending message to client: %v", err)
				// Treat any send failure as "this client is unhealthy"
				// and close the conn so the next broadcast skips it
				// fast via IsClosed, and the proxy's HandleConnection
				// loop observes a closed read → calls onDisconnect →
				// drains subscriptions. Without this, a slow consumer
				// re-enters this code path on every upstream message
				// and burns wsClientWriteDeadline (10 s) of goroutine
				// time per push before failing — for as long as the
				// process lives. Close is idempotent so this is safe
				// when the client already closed itself via reads.
				_ = client.Close()
			}
		}(client, id)
	}
}

func (b *Broker) onClientDisconnect(client *JsonRpcWsClient) {
	b.log.Warn("removing all subscriptions for client")
	if err := b.removeAllSubscriptions(client); err != nil {
		b.log.Errorf("error removing all subscriptions for client: %v", err)
	}
}
