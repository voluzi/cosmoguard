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

	"github.com/voluzi/cosmoguard/pkg/util"
)

type UpstreamConnManagerEth struct {
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

	// failedReconnects — consecutive dial failures; see Cosmos impl.
	failedReconnects atomic.Int32

	// Lifecycle — see Cosmos impl for shape. stopCh closes on Stop()
	// so the reconnect-backoff sleep wakes immediately; lazily
	// allocated to keep direct-literal construction safe.
	stopped  atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
	initOnce sync.Once
}

func (u *UpstreamConnManagerEth) initStopCh() {
	u.initOnce.Do(func() {
		u.stopCh = make(chan struct{})
	})
}

func (u *UpstreamConnManagerEth) curClient() *JsonRpcWsClient {
	return u.lifecycle.currentClient()
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

func (u *UpstreamConnManagerEth) onUpstreamMessage(client *JsonRpcWsClient, msg *JsonRpcMsg) {
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
		handle, ok := u.lifecycle.route(client, subscriptionID)
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
	}

	u.log.Errorf("dropped message from upstream with ID: %v", msg.ID)
}

func (u *UpstreamConnManagerEth) makeRequestWithID(id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
	return u.makeRequestWithIDOnClient(u.curClient(), id, req)
}

// makeRequestWithIDOnClient drives a single round-trip over an explicit
// client. See cosmos counterpart.
func (u *UpstreamConnManagerEth) makeRequestWithIDOnClient(cli *JsonRpcWsClient, id string, req *JsonRpcMsg) (*JsonRpcMsg, error) {
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
	return u.lifecycle.hasParam(subID)
}

func (u *UpstreamConnManagerEth) Subscribe(query string) (string, error) {
	id := u.IdGen.ID()
	return u.lifecycle.subscribe(query, id, u.curClient(), u)
}

func (u *UpstreamConnManagerEth) subscribeWithIDOnClient(cli *JsonRpcWsClient, id, param string, resubmit bool) (string, error) {
	if resubmit {
		record := u.lifecycle.lookupParam(param)
		if record == nil {
			return "", nil
		}
		return record.handle, u.lifecycle.resubmitRecord(cli, record, u)
	}
	return u.lifecycle.subscribe(param, id, cli, u)
}

func (u *UpstreamConnManagerEth) subscribeOn(cli *JsonRpcWsClient, id, param string, _ bool) (string, error) {
	requestID := u.requestIDs.next()
	msg := &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      requestID,
		Method:  methodSubscribeEth,
		Params:  []interface{}{param},
	}

	resp, err := u.makeRequestWithIDOnClient(cli, requestID, msg)
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
	return u.lifecycle.unsubscribe(id, u)
}

func (u *UpstreamConnManagerEth) LocalUnsubscribe(param string) <-chan error {
	return u.lifecycle.retire(u.lifecycle.lookupParam(param), u)
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

func (u *UpstreamConnManagerEth) stableHandle(provisional, _ string) string {
	return "0x" + hex.EncodeToString([]byte(evmLogicalHandleDomain+provisional))
}

func (u *UpstreamConnManagerEth) releaseHandle(reservation string) {
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
	return u.lifecycle.resubmit(cli, u)
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
		u.initStopCh()
		u.stopped.Store(true)
		close(u.stopCh)
		u.lifecycle.stop(u)
	})
}
