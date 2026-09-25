package cosmoguard

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/voluzi/cosmoguard/v5/pkg/util"
)

type UpstreamConnManagerEth struct {
	url            url.URL
	dialer         *websocket.Dialer
	log            *Entry
	IdGen          *util.UniqueID
	lifecycle      *wsSubscriptionLifecycle
	requestIDs     wsInternalRequestIDs
	requestTimeout time.Duration
	pingPeriod     time.Duration

	respMap map[wsResponseKey]chan *JsonRpcMsg
	// respHooks run on the reader goroutine before a response is handed to
	// its waiter, so they finish before the next upstream frame is read.
	respHooks map[wsResponseKey]func(*JsonRpcMsg)
	respMux   sync.Mutex

	onSubscriptionMessage func(msg *JsonRpcMsg)

	beforeUnsubscribeCommit func()
	beforeInstall           func(*JsonRpcWsClient)
	afterInstall            func(*JsonRpcWsClient)
	afterResubmit           func()

	// failedReconnects — consecutive dial failures; see Cosmos impl.
	failedReconnects atomic.Int32

	// Lifecycle — see Cosmos impl for shape. stopCh closes on Stop()
	// so the reconnect-backoff sleep wakes immediately; lazily
	// allocated to keep direct-literal construction safe.
	stopped   atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	initOnce  sync.Once
	stateOnce sync.Once
}

func (u *UpstreamConnManagerEth) initState() {
	u.stateOnce.Do(func() {
		if u.lifecycle == nil {
			u.lifecycle = newWSSubscriptionLifecycle()
		}
		if u.IdGen == nil {
			u.IdGen = &util.UniqueID{}
		}
		if u.respMap == nil {
			u.respMap = make(map[wsResponseKey]chan *JsonRpcMsg)
		}
		if u.dialer == nil {
			u.dialer = &websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: connectTimeout}
		}
		if u.log == nil {
			u.log = log
		}
		if u.onSubscriptionMessage == nil {
			u.onSubscriptionMessage = func(*JsonRpcMsg) {}
		}
	})
}

func (u *UpstreamConnManagerEth) subscriptionLifecycle() *wsSubscriptionLifecycle {
	u.initState()
	return u.lifecycle
}

func (u *UpstreamConnManagerEth) initStopCh() {
	u.initOnce.Do(func() {
		u.stopCh = make(chan struct{})
	})
}

func (u *UpstreamConnManagerEth) curClient() *JsonRpcWsClient {
	return u.subscriptionLifecycle().currentClient()
}

func EthUpstreamConnManager(url url.URL, idGen *util.UniqueID, onSubscriptionMessage func(msg *JsonRpcMsg)) UpstreamConnManager {
	return &UpstreamConnManagerEth{
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

func (u *UpstreamConnManagerEth) Run(log *Entry) error {
	u.initState()
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
				select {
				case <-u.stopCh:
					return nil
				case <-time.After(connectRetryPeriod):
				}
				continue
			}
			u.failedReconnects.Store(0)
			cli = u.curClient()
			// See cosmos manager: resubmit must run off Run so it can
			// read its own subscribe response.
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

func (u *UpstreamConnManagerEth) connect() error {
	u.log.Debug("connecting to upstream websocket")
	conn, _, err := u.dialer.Dial(u.url.String(), nil)
	if err == nil {
		u.log.Info("upstream websocket connected")
		// Cap inbound frame size before any read can land — gorilla's
		// default is unbounded. See upstreamWSReadLimit's doc comment
		// in ws_upstream.go for the size rationale.
		conn.SetReadLimit(upstreamWSReadLimit)
		client := NewJsonRpcWsClient(conn)
		startUpstreamKeepalive(conn, client, u.pingPeriod)
		if u.beforeInstall != nil {
			u.beforeInstall(client)
		}
		if !u.subscriptionLifecycle().install(client) {
			_ = client.Close()
			return ErrClosed
		}
		if u.afterInstall != nil {
			u.afterInstall(client)
		}
	}
	return err
}

func (u *UpstreamConnManagerEth) onUpstreamMessage(client *JsonRpcWsClient, msg *JsonRpcMsg) {
	u.initState()
	// Defence-in-depth: a single malformed/duplicate upstream frame must
	// never take down the process. The Run goroutine that calls this has
	// no recover of its own.
	defer func() {
		if r := recover(); r != nil {
			u.log.WithField("panic", r).Error("recovered from panic handling upstream message")
		}
	}()

	// This is a subscription notification
	if msg.ID == nil {
		if msg.Params == nil {
			u.log.Errorf("dropped message from upstream with no ID and no params")
			return
		}
		params, ok := msg.Params.(map[string]interface{})
		if !ok {
			u.log.Errorf("dropped message from upstream: params is not a map")
			return
		}
		sub, ok := params["subscription"]
		if !ok {
			u.log.Errorf("dropped message from upstream with no ID")
			return
		}

		subscriptionID, ok := sub.(string)
		if !ok {
			u.log.Errorf("dropped message from upstream: subscription ID is not a string")
			return
		}
		handle, ok := u.subscriptionLifecycle().route(client, subscriptionID)
		if ok {
			u.log.WithFields(map[string]interface{}{
				"ID": handle,
			}).Debug("got message from subscription")
			msg.ID = handle
			params["subscription"] = handle
			u.onSubscriptionMessage(msg)
			return
		}
	} else {
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

		// Delete the entry under the lock BEFORE send+close so a
		// duplicate upstream response with the same ID can't re-load the
		// same closed channel and panic ("send on closed channel"),
		// which would crash the whole process from this recover-less
		// goroutine.
		u.respMux.Lock()
		key := wsResponseKey{client: client, id: msgID}
		wc, ok := u.respMap[key]
		hook := u.respHooks[key]
		if ok {
			delete(u.respMap, key)
			delete(u.respHooks, key)
		}
		u.respMux.Unlock()
		if ok {
			u.log.WithField("ID", msgID).Debug("got response for request")
			if hook != nil {
				hook(msg)
			}
			wc <- msg
			close(wc)
			return
		}
	}

	u.log.Errorf("dropped message from upstream with ID: %v", msg.ID)
}

func (u *UpstreamConnManagerEth) makeRequestWithID(id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
	return u.makeRequestWithIDOnClient(u.curClient(), id, req)
}

// makeRequestWithIDOnClient drives a single round-trip over an explicit
// client. See cosmos counterpart.
func (u *UpstreamConnManagerEth) makeRequestWithIDOnClient(cli *JsonRpcWsClient, id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
	u.initState()
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
	// runtime timer slot immediately — see the matching comment in
	// the cosmos variant.
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

func (u *UpstreamConnManagerEth) MakeRequest(req *JsonRpcMsg) (*JsonRpcMsg, error) {
	return u.makeRequestWithID(u.requestIDs.next(), req)
}

func (u *UpstreamConnManagerEth) HasSubscription(subID string) bool {
	return u.subscriptionLifecycle().hasParam(subID)
}

func (u *UpstreamConnManagerEth) Subscribe(query string) (string, error) {
	return u.subscribeCreated(query, nil)
}

func (u *UpstreamConnManagerEth) subscribeCreated(param string, created func(string)) (string, error) {
	u.initState()
	id := u.IdGen.ID()
	return u.subscriptionLifecycle().subscribe(param, id, u.curClient(), u, created)
}

func (u *UpstreamConnManagerEth) subscribeWithIDOnClient(cli *JsonRpcWsClient, id, param string, resubmit bool) (string, error) {
	if resubmit {
		record := u.subscriptionLifecycle().lookupParam(param)
		if record == nil {
			return "", nil
		}
		return record.handle, u.subscriptionLifecycle().resubmitRecord(cli, record, u)
	}
	return u.subscriptionLifecycle().subscribe(param, id, cli, u, nil)
}

func (u *UpstreamConnManagerEth) subscribeOn(cli *JsonRpcWsClient, id, param string, _ bool) (string, error) {
	requestID := u.requestIDs.next()
	msg := &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      requestID,
		Method:  methodSubscribeEth,
		Params:  ethSubscribeParams(param),
	}

	// An event can follow the acknowledgement immediately; bind its id
	// before the reader moves on to it.
	hookKey := wsResponseKey{client: cli, id: requestID}
	u.respMux.Lock()
	if u.respHooks == nil {
		u.respHooks = make(map[wsResponseKey]func(*JsonRpcMsg))
	}
	u.respHooks[hookKey] = func(resp *JsonRpcMsg) {
		var subID string
		if resp.Error == nil && json.Unmarshal(resp.Result, &subID) == nil && subID != "" {
			u.subscriptionLifecycle().bindEarly(cli, param, subID)
		}
	}
	u.respMux.Unlock()
	resp, err := u.makeRequestWithIDOnClient(cli, requestID, msg)
	u.respMux.Lock()
	delete(u.respHooks, hookKey)
	u.respMux.Unlock()
	if err != nil {
		return "", err
	}
	if err := validateWSSubscribeResponse(methodSubscribeEth, resp, cli.Closed()); err != nil {
		return "", err
	}

	// `Result` is held as RawMessage so the upstream's exact bytes are
	// preserved. The eth_subscribe contract is that result is a JSON
	// string carrying the subscription ID — decode it.
	var subID string
	if err := json.Unmarshal(resp.Result, &subID); err != nil {
		outcome := uncertainWSUpstreamOutcomeUntil(fmt.Errorf("decode subscription ID %q: %w", string(resp.Result), err), cli.Closed())
		return "", outcome
	}
	return subID, nil
}

func (u *UpstreamConnManagerEth) Unsubscribe(id string) error {
	return u.subscriptionLifecycle().unsubscribe(id, u)
}

func (u *UpstreamConnManagerEth) LocalUnsubscribe(param string) <-chan error {
	lifecycle := u.subscriptionLifecycle()
	return lifecycle.retire(lifecycle.lookupParam(param), u)
}

func (u *UpstreamConnManagerEth) localUnsubscribePreservingHandle(param string) <-chan error {
	lifecycle := u.subscriptionLifecycle()
	return lifecycle.retirePreservingHandle(lifecycle.lookupParam(param), u)
}

func (u *UpstreamConnManagerEth) reservationForHandle(handle string) (string, bool) {
	return u.subscriptionLifecycle().reservationForHandle(handle)
}

func (u *UpstreamConnManagerEth) unsubscribeOn(binding wsSubscriptionBinding, _ string) error {
	requestID := u.requestIDs.next()
	response, err := u.makeRequestWithIDOnClient(binding.client, requestID, &JsonRpcMsg{
		Version: jsonRpcVersion,
		Method:  methodUnsubscribeEth,
		Params:  []string{binding.wireID},
	})
	if u.beforeUnsubscribeCommit != nil {
		u.beforeUnsubscribeCommit()
	}
	if err != nil {
		return err
	}
	return validateEthUnsubscribeResponse(response)
}

const evmLogicalHandleDomain = "cg:"

// knownWireID: the upstream picks eth subscription ids; subscribeOn binds
// the id when the acknowledgement is read instead.
func (u *UpstreamConnManagerEth) knownWireID(string) (string, bool) {
	return "", false
}

func (u *UpstreamConnManagerEth) stableHandle(provisional string) string {
	return "0x" + hex.EncodeToString([]byte(evmLogicalHandleDomain+provisional))
}

func (u *UpstreamConnManagerEth) releaseHandle(reservation string) {
	u.initState()
	u.IdGen.Release(reservation)
}

func validateEthUnsubscribeResponse(response *JsonRpcMsg) error {
	if err := validateWSJSONRPCResponse(methodUnsubscribeEth, response); err != nil {
		return err
	}
	var acknowledged bool
	if err := json.Unmarshal(response.Result, &acknowledged); err != nil {
		return fmt.Errorf("decode %s acknowledgement: %w", methodUnsubscribeEth, err)
	}
	if !acknowledged {
		return fmt.Errorf("upstream %s did not confirm cleanup", methodUnsubscribeEth)
	}
	return nil
}

func (u *UpstreamConnManagerEth) reSubmitSubscriptionsOnClient(cli *JsonRpcWsClient) error {
	return u.subscriptionLifecycle().resubmit(cli, u)
}

// IsHealthy mirrors the Cosmos manager's check: nil/closed client →
// unhealthy; persistent reconnect failures → unhealthy. The pool's
// migrator looks at this to decide whether to redistribute this
// upstream's subscriptions.
func (u *UpstreamConnManagerEth) IsHealthy() bool {
	cli := u.curClient()
	if cli == nil || cli.IsClosed() {
		return false
	}
	return u.failedReconnects.Load() < unhealthyAfterFailedReconnects
}

// Stop signals the Run loop to exit and closes the live WS client.
// Idempotent — see Cosmos impl.
func (u *UpstreamConnManagerEth) Stop() {
	u.stopOnce.Do(func() {
		u.initState()
		u.initStopCh()
		u.stopped.Store(true)
		close(u.stopCh)
		u.subscriptionLifecycle().stop(u)
	})
}
