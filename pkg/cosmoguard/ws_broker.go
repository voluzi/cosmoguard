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
	IdGen     *util.UniqueID
	pool      *UpstreamPool
	admission *wsAdmissionController
	log       *Entry
	sm        *SubscriptionManager
	// paramLocks serialises subscription changes per subscription param,
	// including the upstream round trips, so a slow upstream only stalls
	// callers of that param.
	paramLocksMu sync.Mutex
	paramLocks   map[string]*brokerParamLock
	// requests holds the subscribe request behind each client membership
	// so a rule reload can re-evaluate it.
	requestsMu  sync.Mutex
	requests    map[brokerMembership]*JsonRpcMsg
	stopMigrate chan struct{}
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
		admission:   newWSAdmissionController(WebSocketLimits{}),
		sm:          NewSubscriptionManager(),
		stopMigrate: make(chan struct{}),
		migrateDone: make(chan struct{}),
	}
	b.pool = NewUpstreamPool(backends, path, n, b.onSubscriptionMessage, upstreamConstructor)
	return &b
}

func (b *Broker) setAdmissionController(admission *wsAdmissionController) {
	if admission == nil {
		admission = newWSAdmissionController(WebSocketLimits{})
	}
	b.admission = admission
	b.pool.SetMaxSubscriptionsPerConnection(admission.Limits().MaxSubscriptionsPerUpstreamConnection)
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
	b.pool.migrateUnhealthy(b.lockParam, func(m SubscriptionMigration) {
		if !b.sm.Migrate(m.OldID, m.NewID) {
			b.log.WithFields(Fields{
				"oldID": m.OldID,
				"newID": m.NewID,
				"param": m.Param,
			}).Warn("migration: subscription manager unaware of oldID")
		}
	})
}

type brokerParamLock struct {
	mu   sync.Mutex
	refs int
}

// lockParam locks param and returns its unlock function.
func (b *Broker) lockParam(param string) func() {
	b.paramLocksMu.Lock()
	if b.paramLocks == nil {
		b.paramLocks = make(map[string]*brokerParamLock)
	}
	l := b.paramLocks[param]
	if l == nil {
		l = &brokerParamLock{}
		b.paramLocks[param] = l
	}
	l.refs++
	b.paramLocksMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		b.paramLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(b.paramLocks, param)
		}
		b.paramLocksMu.Unlock()
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
		// s.ID is the canonical id; the pool tracks the current upstream id.
		upstreamID, _ := b.sm.UpstreamID(s.ID)
		out = append(out, WSSubInfo{
			Param:       s.Param,
			Subscribers: s.Subscribers,
			Upstream:    b.pool.SubscriptionTarget(upstreamID),
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

func (b *Broker) HandleSubscription(client *JsonRpcWsClient, msg *JsonRpcMsg, identity ...string) (*JsonRpcMsg, error) {
	b.log.WithField("client", client).Debug("handling subscription")
	client.SetOnDisconnectCallback(b.onClientDisconnect)
	if client.IsClosed() {
		return nil, ErrClosed
	}

	switch msg.Method {
	case methodSubscribeCosmos:
		_, err := b.addSubscription(client, msg, identity...)
		if err != nil {
			if data := wsResourceExhaustedData(err); data != nil {
				return ErrorResponse(msg, -32005, "WebSocket resource exhausted", data), nil
			}
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
		id, err := b.addSubscription(client, msg, identity...)
		if err != nil {
			if data := wsResourceExhaustedData(err); data != nil {
				return ErrorResponse(msg, -32005, "WebSocket resource exhausted", data), nil
			}
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

func (b *Broker) addSubscription(client *JsonRpcWsClient, msg *JsonRpcMsg, identity ...string) (string, error) {
	param, err := getSubscriptionParam(msg)
	if err != nil {
		return "", err
	}

	unlock := b.lockParam(param)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	if client.IsClosed() {
		return "", ErrClosed
	}

	id, exists := b.sm.GetSubscriptionID(param)
	if exists && b.sm.ClientSubscribed(id, client) {
		if msg.Method == methodSubscribeCosmos && msg.ID != nil {
			b.sm.SubscribeClient(id, client, msg.ID)
		}
		return id, nil
	}
	identityName := ""
	if len(identity) > 0 {
		identityName = identity[0]
	}
	if err := b.admission.reserveSubscription(client, identityName); err != nil {
		return "", err
	}
	admitted := true
	defer func() {
		if admitted {
			b.admission.releaseSubscription(client)
		}
	}()
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
	b.setRequest(client, id, msg)
	if client.IsClosed() {
		// The disconnect callback may have listed the client's
		// subscriptions before this one was added.
		admitted = false
		unlock()
		locked = false
		return "", errors.Join(ErrClosed, b.removeAllSubscriptions(client))
	}
	admitted = false

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

	param := subParam
	if isEthSubscriptionID(subParam) {
		if param, _ = b.sm.GetSubscriptionParam(subParam); param == "" {
			return fmt.Errorf("subscription does not exist")
		}
	}
	defer b.lockParam(param)()

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

	clientSubID := b.sm.GetSubscriptionClients(subID)[client]
	request := b.setRequest(client, subID, nil)
	b.sm.UnsubscribeClient(subID, client)
	b.log.WithFields(map[string]interface{}{
		"id":     subID,
		"client": client,
	}).Debug("unsubscribed client")

	if b.sm.SubscriptionEmpty(subID) {
		// pool.Unsubscribe keys on the CURRENT upstream id, which differs
		// from the canonical subID after a migration.
		upstreamID, _ := b.sm.UpstreamID(subID)
		if err = b.pool.Unsubscribe(upstreamID); err != nil {
			if !isUncertainWSUpstreamOutcome(err) && !client.IsClosed() {
				b.sm.SubscribeClient(subID, client, clientSubID)
				b.setRequest(client, subID, request)
				return err
			}
			b.admission.releaseSubscription(client)
			b.abandonEmptySubscription(subID, upstreamID, err)
			return err
		}
		param, _ := b.sm.GetSubscriptionParam(subID)
		b.sm.RemoveSubscription(subID)
		b.admission.releaseSubscription(client)

		b.log.WithFields(map[string]interface{}{
			"ID":    subID,
			"param": param,
		}).Warn("unsubscribed upstream")
	} else {
		b.admission.releaseSubscription(client)
	}

	return nil
}

func (b *Broker) removeAllSubscriptions(client *JsonRpcWsClient) error {
	b.log.WithField("client", client).Debug("unsubscribing client from all subscriptions")

	// Admission belongs to downstream membership, so all memberships are
	// released before any upstream cleanup can wait on network I/O.
	var emptySubscriptions []string
	for _, subscriptionID := range b.sm.GetSubscriptions(client) {
		param, ok := b.sm.GetSubscriptionParam(subscriptionID)
		if !ok {
			continue
		}
		unlock := b.lockParam(param)
		if b.detachClientLocked(client, subscriptionID) {
			emptySubscriptions = append(emptySubscriptions, subscriptionID)
		}
		unlock()
	}

	var errs []error
	for _, subscriptionID := range emptySubscriptions {
		param, ok := b.sm.GetSubscriptionParam(subscriptionID)
		if !ok {
			continue
		}
		unlock := b.lockParam(param)
		// Another client may have joined since the detach.
		if id, _ := b.sm.GetSubscriptionID(param); id == subscriptionID && b.sm.SubscriptionEmpty(subscriptionID) {
			if err := b.removeEmptySubscriptionLocked(subscriptionID); err != nil {
				errs = append(errs, err)
			}
		}
		unlock()
	}
	return errors.Join(errs...)
}

// detachClientLocked drops client from subscriptionID and reports whether
// the subscription is left without clients. The caller holds the param lock.
func (b *Broker) detachClientLocked(client *JsonRpcWsClient, subscriptionID string) bool {
	if !b.sm.ClientSubscribed(subscriptionID, client) {
		return false
	}
	b.log.WithField("ID", subscriptionID).Debug("unsubscribing client")
	b.setRequest(client, subscriptionID, nil)
	b.sm.UnsubscribeClient(subscriptionID, client)
	b.admission.releaseSubscription(client)
	return b.sm.SubscriptionEmpty(subscriptionID)
}

type brokerMembership struct {
	client *JsonRpcWsClient
	id     string
}

// setRequest records the subscribe request behind a membership, or forgets
// it when request is nil, and returns the previous one.
func (b *Broker) setRequest(client *JsonRpcWsClient, id string, request *JsonRpcMsg) *JsonRpcMsg {
	b.requestsMu.Lock()
	defer b.requestsMu.Unlock()
	key := brokerMembership{client: client, id: id}
	previous := b.requests[key]
	if request == nil {
		delete(b.requests, key)
	} else {
		if b.requests == nil {
			b.requests = make(map[brokerMembership]*JsonRpcMsg)
		}
		b.requests[key] = request
	}
	return previous
}

// revokeDenied ends every client membership whose subscribe request allowed
// rejects, and tells the client.
func (b *Broker) revokeDenied(allowed func(*JsonRpcMsg) bool) {
	b.requestsMu.Lock()
	var denied []brokerMembership
	for membership, request := range b.requests {
		if !allowed(request) {
			denied = append(denied, membership)
		}
	}
	b.requestsMu.Unlock()
	for _, membership := range denied {
		b.revoke(membership.client, membership.id)
	}
}

func (b *Broker) revoke(client *JsonRpcWsClient, id string) {
	param, ok := b.sm.GetSubscriptionParam(id)
	if !ok {
		return
	}
	unlock := b.lockParam(param)
	clientSubID, subscribed := b.sm.GetSubscriptionClients(id)[client]
	request := b.setRequest(client, id, nil)
	if !subscribed || request == nil {
		unlock()
		return
	}
	if b.detachClientLocked(client, id) {
		if err := b.removeEmptySubscriptionLocked(id); err != nil {
			b.log.WithError(err).Warn("error removing revoked subscription upstream")
		}
	}
	unlock()

	b.log.WithFields(Fields{"ID": id, "client": client}).Warn("subscription revoked by rule reload")
	if request.Method == methodSubscribeEth {
		// eth_subscribe has no server-side cancellation message; closing
		// the socket is what the client library notices.
		_ = client.Close()
		return
	}
	// CometBFT's own cancellation notice, sent under the subscribe id.
	notice := &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      clientSubID,
		Error: &JsonRpcError{
			Code:    -32000,
			Message: "Server error",
			Data:    "subscription was canceled (reason: denied by policy)",
		},
	}
	if err := client.enqueueNotification(notice, wsNotificationSharedCost(notice)); err != nil && !errors.Is(err, ErrClosed) {
		b.log.WithError(err).WithField("client", client).Warn("dropping slow websocket subscriber")
	}
}

func (b *Broker) removeEmptySubscriptionLocked(subscriptionID string) error {
	b.log.WithField("ID", subscriptionID).Debug("unsubscribing upstream")
	upstreamID, _ := b.sm.UpstreamID(subscriptionID)
	if err := b.pool.Unsubscribe(upstreamID); err != nil {
		b.abandonEmptySubscription(subscriptionID, upstreamID, err)
		return fmt.Errorf("subscription %s: %w", subscriptionID, err)
	}
	param, _ := b.sm.GetSubscriptionParam(subscriptionID)
	b.sm.RemoveSubscription(subscriptionID)
	b.log.WithFields(map[string]interface{}{
		"ID":    subscriptionID,
		"param": param,
	}).Warn("unsubscribed upstream")
	return nil
}

func (b *Broker) abandonEmptySubscription(id, upstreamID string, err error) {
	if isUncertainWSUpstreamOutcome(err) {
		b.forgetSettlingSubscription(id, err)
		return
	}
	b.pool.retireSubscription(upstreamID)
	b.sm.RemoveSubscription(id)
}

func (b *Broker) forgetSettlingSubscription(id string, err error) {
	if isUncertainWSUpstreamOutcome(err) && uncertainWSUpstreamOutcomeSettlement(err) != nil {
		b.sm.RemoveSubscription(id)
	}
}

func (b *Broker) onSubscriptionMessage(msg *JsonRpcMsg) {
	// Upstream is expected to echo the subscription ID (a string we
	// minted via util.UniqueID). Defend against malformed upstreams
	// that send a non-string ID — a raw type assertion here used to
	// panic the broker's read goroutine.
	upstreamID, ok := msg.ID.(string)
	if !ok {
		b.log.WithField("ID", msg.ID).Warn("dropped subscription message with non-string ID")
		return
	}

	// The notification carries the CURRENT upstream subscription id, which
	// differs from the stable canonical id after a migration. Route on the
	// canonical id so the client set is found, and — for EVM, where the id
	// is echoed to the client inside params.subscription — rewrite that
	// field back to the canonical id the client actually subscribed with.
	// Without this rewrite, web3 clients match on params.subscription and
	// silently drop every event after a migration.
	msgID, translated := b.sm.CanonicalID(upstreamID)
	if translated && msgID != upstreamID {
		if params, ok := msg.Params.(map[string]interface{}); ok {
			if _, has := params["subscription"]; has {
				params["subscription"] = msgID
			}
		}
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

	sharedCost := wsNotificationSharedCost(msg)
	for client, id := range clients {
		cost := wsNotificationCostWithID(sharedCost, id)
		if err := client.enqueueNotification(msg.CloneWithID(id), cost); err != nil && !errors.Is(err, ErrClosed) {
			b.log.WithError(err).WithField("client", client).Warn("dropping slow websocket subscriber")
		}
	}
}

func (b *Broker) onClientDisconnect(client *JsonRpcWsClient) {
	b.log.Warn("removing all subscriptions for client")
	if err := b.removeAllSubscriptions(client); err != nil {
		b.log.Errorf("error removing all subscriptions for client: %v", err)
	}
}
