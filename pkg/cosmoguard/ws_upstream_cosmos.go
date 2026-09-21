package cosmoguard

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/voluzi/cosmoguard/pkg/util"
)

type UpstreamConnManagerCosmos struct {
	url            url.URL
	dialer         *websocket.Dialer
	log            *Entry
	IdGen          *util.UniqueID
	lifecycle      *wsSubscriptionLifecycle
	requestIDs     wsInternalRequestIDs
	requestTimeout time.Duration

	respMap map[wsResponseKey]chan *JsonRpcMsg
	respMux sync.Mutex

	onSubscriptionMessage func(msg *JsonRpcMsg)

	beforeUnsubscribeCommit func()
	beforeInstall           func(*JsonRpcWsClient)
	afterInstall            func(*JsonRpcWsClient)
	afterResubmit           func()

	// failedReconnects counts consecutive Dial failures. Reset to 0 on
	// any successful connect. Read by IsHealthy so the pool's
	// migration loop can see "stuck" connections and re-route their
	// subscriptions to a survivor.
	failedReconnects atomic.Int32

	// stopped is the shutdown flag set by Stop(). The Run loop checks
	// it before each reconnect attempt so a dead-backend goroutine
	// doesn't survive CosmoGuard.Shutdown.
	stopped atomic.Bool
	// stopCh is closed by Stop() so the reconnect-backoff sleep wakes
	// up immediately instead of parking the goroutine for up to
	// connectRetryPeriod past shutdown. Lazily allocated to avoid
	// breaking direct &UpstreamConnManagerCosmos{} literal uses.
	stopCh   chan struct{}
	stopOnce sync.Once
	initOnce sync.Once
}

// initStopCh is called by Run before the loop starts and again by
// Stop, idempotently allocating stopCh. Done this way (instead of in
// the constructor) so existing call sites that build the struct via
// a literal don't get a nil channel.
func (u *UpstreamConnManagerCosmos) initStopCh() {
	u.initOnce.Do(func() {
		u.stopCh = make(chan struct{})
	})
}

// curClient returns the currently-attached WS client under
// clientMu.RLock. nil when there's no live connection.
func (u *UpstreamConnManagerCosmos) curClient() *JsonRpcWsClient {
	return u.lifecycle.currentClient()
}

func CosmosUpstreamConnManager(url url.URL, idGen *util.UniqueID, onSubscriptionMessage func(msg *JsonRpcMsg)) UpstreamConnManager {
	return &UpstreamConnManagerCosmos{
		url: url,
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: connectTimeout,
		},
		IdGen:                 idGen,
		lifecycle:             newWSSubscriptionLifecycle(),
		respMap:               make(map[wsResponseKey]chan *JsonRpcMsg),
		onSubscriptionMessage: onSubscriptionMessage,
	}
}

func (u *UpstreamConnManagerCosmos) Run(log *Entry) error {
	u.log = log
	u.initStopCh()
	for {
		if u.stopped.Load() {
			return nil
		}
		cli := u.curClient()
		if cli == nil || cli.IsClosed() {
			if err := u.connect(); err != nil {
				u.log.Errorf("error connecting: %v", err)
				u.failedReconnects.Add(1)
				// Interruptible sleep so Stop() takes effect
				// immediately instead of parking this goroutine
				// for up to connectRetryPeriod past shutdown.
				select {
				case <-u.stopCh:
					return nil
				case <-time.After(connectRetryPeriod):
				}
				continue
			}
			u.failedReconnects.Store(0)
			cli = u.curClient()
			// Resubmit must run off the Run goroutine: Run is the only
			// reader on the socket, so if it blocked waiting on the
			// subscribe response (the old behaviour) the response could
			// never be received — every reconnect ended in a 10 s
			// timeout. The goroutine captures the newly-connected client
			// so a fast subsequent reconnect can't redirect its calls
			// to a different connection.
			go func(cli *JsonRpcWsClient) {
				if err := u.reSubmitSubscriptionsOnClient(cli); err != nil {
					u.log.Errorf("error re-submitting subscriptions: %v", err)
					_ = cli.Close()
				} else if u.afterResubmit != nil {
					u.afterResubmit()
				}
			}(cli)
		}
		if cli == nil {
			continue
		}
		msg, err := cli.ReceiveMsg()
		if err != nil {
			if errors.Is(err, ErrClosed) {
				u.log.Errorf("websocket closed: %v", err)
				cli.Close()
			} else {
				u.log.Errorf("error receiving message from upstream: %v", err)
				// Don't busy-loop on persistent parse errors —
				// brief sleep mirrors the connect-retry backoff,
				// interruptible by Stop().
				select {
				case <-u.stopCh:
					return nil
				case <-time.After(connectRetryPeriod / 10):
				}
			}
			continue
		}
		u.onUpstreamMessage(cli, msg)
	}
}

// IsHealthy reports usable-connection state. Healthy = the WS client
// exists, isn't closed, and hasn't accumulated multiple consecutive
// connect failures (so a backend in repeated reconnect backoff is
// flagged unhealthy and the pool can migrate its subscriptions).
func (u *UpstreamConnManagerCosmos) IsHealthy() bool {
	cli := u.curClient()
	if cli == nil || cli.IsClosed() {
		return false
	}
	return u.failedReconnects.Load() < unhealthyAfterFailedReconnects
}

// unhealthyAfterFailedReconnects is the consecutive-failure threshold
// past which a connection is considered "stuck" rather than briefly
// flaky. ~3 × connectRetryPeriod (15s) gives the network a chance to
// recover before the migrator starts moving subscriptions around.
const unhealthyAfterFailedReconnects = 3

func (u *UpstreamConnManagerCosmos) connect() error {
	u.log.Debug("connecting to upstream websocket")
	conn, _, err := u.dialer.Dial(u.url.String(), nil)
	if err == nil {
		u.log.Info("upstream websocket connected")
		// Cap inbound frame size before any read can land — gorilla's
		// default is unbounded. See upstreamWSReadLimit's doc comment
		// in ws_upstream.go for the size rationale.
		conn.SetReadLimit(upstreamWSReadLimit)
		client := NewJsonRpcWsClient(conn)
		if u.beforeInstall != nil {
			u.beforeInstall(client)
		}
		if !u.lifecycle.install(client) {
			_ = client.Close()
			return ErrClosed
		}
		if u.afterInstall != nil {
			u.afterInstall(client)
		}
	}
	return err
}

func (u *UpstreamConnManagerCosmos) onUpstreamMessage(client *JsonRpcWsClient, msg *JsonRpcMsg) {
	// Defence-in-depth: a single malformed/duplicate upstream frame must
	// never take down the process. The Run goroutine that calls this has
	// no recover of its own.
	defer func() {
		if r := recover(); r != nil {
			u.log.WithField("panic", r).Error("recovered from panic handling upstream message")
		}
	}()

	if msg.ID == nil {
		u.log.Errorf("dropped message from upstream with no ID")
		return
	}

	var msgID string

	switch v := msg.ID.(type) {
	case string:
		msgID = v
	case int:
		msgID = strconv.Itoa(v)
	default:
		u.log.WithField("ID", msg.ID).Error("wrong ID type")
		return
	}

	// Let's first check if it's a response to a request. Read under
	// respMux so we don't race with concurrent writers in
	// makeRequestWithID. Delete the entry while still holding the lock,
	// BEFORE send+close: a buggy/malicious upstream that emits two
	// responses with the same ID would otherwise re-load the same closed
	// channel and panic with "send on closed channel", crashing the
	// process. Deleting first makes the second frame a no-op.
	u.respMux.Lock()
	key := wsResponseKey{client: client, id: msgID}
	wc, ok := u.respMap[key]
	if ok {
		delete(u.respMap, key)
	}
	u.respMux.Unlock()
	if ok {
		u.log.WithField("ID", msgID).Debug("got response for request")
		wc <- msg
		close(wc)
		return
	}

	// Otherwise let's check if it's a cosmos subscription notification.
	handle, ok := u.lifecycle.route(client, msgID)
	if ok {
		u.log.WithFields(map[string]interface{}{
			"ID": handle,
		}).Debug("got message from subscription")
		msg.ID = handle
		u.onSubscriptionMessage(msg)
		return
	}

	u.log.Errorf("dropped message from upstream with ID: %v", msg.ID)
}

func (u *UpstreamConnManagerCosmos) makeRequestWithID(id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
	return u.makeRequestWithIDOnClient(u.curClient(), id, req)
}

// makeRequestWithIDOnClient drives a single round-trip over an explicitly
// chosen client. The resubmit-after-reconnect path uses this so it stays
// bound to the freshly-connected socket even if Run reconnects again
// underneath it. The select wakes on the client's Closed() channel so a
// disconnect cancels the wait instead of stalling for responseTimeout.
func (u *UpstreamConnManagerCosmos) makeRequestWithIDOnClient(cli *JsonRpcWsClient, id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
	request := req.CloneWithID(id)

	if cli == nil || cli.IsClosed() {
		return nil, ErrClosed
	}

	// Create a buffered channel to prevent goroutine leak on timeout
	respChan := make(chan *JsonRpcMsg, 1)
	key := wsResponseKey{client: cli, id: id}

	u.respMux.Lock()
	u.respMap[key] = respChan
	u.respMux.Unlock()

	u.log.WithFields(map[string]interface{}{
		"ID":     id,
		"method": request.Method,
	}).Debug("submitting request")
	if err := cli.SendMsg(request); err != nil {
		u.respMux.Lock()
		delete(u.respMap, key)
		u.respMux.Unlock()
		var writeErr *wsWriteError
		if errors.As(err, &writeErr) {
			_ = cli.Close()
			return nil, uncertainWSUpstreamOutcomeUntil(err, cli.Closed())
		}
		return nil, err
	}

	// Stoppable timer (not time.After) so the happy path frees the
	// runtime timer slot immediately instead of leaving it parked
	// for the full responseTimeout window. On a hot subscription
	// workload time.After accumulates thousands of zombie timers,
	// costing both memory and timer-heap rebalance CPU.
	timeout := time.NewTimer(effectiveWSResponseTimeout(u.requestTimeout))
	defer timeout.Stop()
	select {
	case response := <-respChan:
		u.respMux.Lock()
		delete(u.respMap, key)
		u.respMux.Unlock()

		response.ID = req.ID
		return response, nil

	case <-cli.Closed():
		u.respMux.Lock()
		delete(u.respMap, key)
		u.respMux.Unlock()
		return nil, uncertainWSUpstreamOutcomeUntil(ErrClosed, cli.Closed())

	case <-timeout.C:
		u.respMux.Lock()
		delete(u.respMap, key)
		u.respMux.Unlock()

		return nil, uncertainWSUpstreamOutcomeUntil(fmt.Errorf("timeout waiting for response for request with ID %s", id), cli.Closed())
	}
}

func (u *UpstreamConnManagerCosmos) MakeRequest(req *JsonRpcMsg) (*JsonRpcMsg, error) {
	return u.makeRequestWithID(u.requestIDs.next(), req)
}

func (u *UpstreamConnManagerCosmos) HasSubscription(param string) bool {
	return u.lifecycle.hasParam(param)
}

func (u *UpstreamConnManagerCosmos) Subscribe(param string) (string, error) {
	id := u.IdGen.ID()
	return u.lifecycle.subscribe(param, id, u.curClient(), u)
}

func (u *UpstreamConnManagerCosmos) subscribeWithIDOnClient(cli *JsonRpcWsClient, id, param string, resubmit bool) error {
	if resubmit {
		record := u.lifecycle.lookupParam(param)
		if record == nil {
			return nil
		}
		return u.lifecycle.resubmitRecord(cli, record, u)
	}
	_, err := u.lifecycle.subscribe(param, id, cli, u)
	return err
}

func (u *UpstreamConnManagerCosmos) subscribeOn(cli *JsonRpcWsClient, id, param string, _ bool) (string, error) {
	msg := &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      id,
		Method:  methodSubscribeCosmos,
		Params:  []interface{}{param},
	}

	response, err := u.makeRequestWithIDOnClient(cli, id, msg)
	if err != nil {
		return id, err
	}
	if err := validateWSSubscribeResponse(methodSubscribeCosmos, response, cli.Closed()); err != nil {
		return id, err
	}
	return id, nil
}

func (u *UpstreamConnManagerCosmos) Unsubscribe(id string) error {
	return u.lifecycle.unsubscribe(id, u)
}

func (u *UpstreamConnManagerCosmos) LocalUnsubscribe(param string) <-chan error {
	return u.lifecycle.retire(u.lifecycle.lookupParam(param), u)
}

func (u *UpstreamConnManagerCosmos) unsubscribeOn(binding wsSubscriptionBinding, param string) error {
	requestID := u.requestIDs.next()
	response, err := u.makeRequestWithIDOnClient(binding.client, requestID, &JsonRpcMsg{
		Version: jsonRpcVersion,
		Method:  methodUnsubscribeCosmos,
		Params:  []interface{}{param},
	})
	if u.beforeUnsubscribeCommit != nil {
		u.beforeUnsubscribeCommit()
	}
	if err != nil {
		return err
	}
	return validateCosmosUnsubscribeResponse(response)
}

func (u *UpstreamConnManagerCosmos) stableHandle(provisional, _ string) string {
	return provisional
}

func (u *UpstreamConnManagerCosmos) releaseHandle(handle string) {
	u.IdGen.Release(handle)
}

func validateCosmosUnsubscribeResponse(response *JsonRpcMsg) error {
	if err := validateWSJSONRPCResponse(methodUnsubscribeCosmos, response); err != nil {
		return err
	}
	var acknowledgement map[string]any
	if err := json.Unmarshal(response.Result, &acknowledgement); err != nil {
		return fmt.Errorf("decode %s acknowledgement: %w", methodUnsubscribeCosmos, err)
	}
	if acknowledgement == nil {
		return fmt.Errorf("decode %s acknowledgement: expected object", methodUnsubscribeCosmos)
	}
	return nil
}

// Stop signals the Run loop to exit and closes the live WS client.
// Idempotent. After Stop returns, the loop will exit at its next
// iteration; in-flight ReceiveMsg unblocks because Close() forces
// it to surface ErrClosed.
func (u *UpstreamConnManagerCosmos) Stop() {
	u.stopOnce.Do(func() {
		u.initStopCh()
		u.stopped.Store(true)
		close(u.stopCh)
		u.lifecycle.stop(u)
	})
}

func (u *UpstreamConnManagerCosmos) reSubmitSubscriptionsOnClient(cli *JsonRpcWsClient) error {
	return u.lifecycle.resubmit(cli, u)
}
