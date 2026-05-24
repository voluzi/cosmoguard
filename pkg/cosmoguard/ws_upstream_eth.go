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

type UpstreamConnManagerEth struct {
	clientMu sync.RWMutex
	client   *JsonRpcWsClient
	url      url.URL
	dialer   *websocket.Dialer
	log      *Entry
	IdGen    *util.UniqueID

	respMap map[string]chan *JsonRpcMsg
	respMux sync.Mutex

	onSubscriptionMessage func(msg *JsonRpcMsg)
	subByID               map[string]string
	subByParam            map[string]string
	subMux                sync.Mutex

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
	u.clientMu.RLock()
	defer u.clientMu.RUnlock()
	return u.client
}

func EthUpstreamConnManager(url url.URL, idGen *util.UniqueID, onSubscriptionMessage func(msg *JsonRpcMsg)) UpstreamConnManager {
	return &UpstreamConnManagerEth{
		url: url,
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: connectTimeout,
		},
		IdGen:                 idGen,
		subByParam:            make(map[string]string),
		subByID:               make(map[string]string),
		respMap:               make(map[string]chan *JsonRpcMsg),
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
			u.resetAll()
			cli = u.curClient()
			// See cosmos manager: resubmit must run off Run so it can
			// read its own subscribe response.
			go func(cli *JsonRpcWsClient) {
				if err := u.reSubmitSubscriptionsOnClient(cli); err != nil {
					u.log.Errorf("error re-submitting subscriptions: %v", err)
					_ = cli.Close()
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
		u.onUpstreamMessage(msg)
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
		u.clientMu.Lock()
		u.client = NewJsonRpcWsClient(conn)
		u.clientMu.Unlock()
	}
	return err
}

func (u *UpstreamConnManagerEth) onUpstreamMessage(msg *JsonRpcMsg) {
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
		msg.ID = subscriptionID
		u.subMux.Lock()
		query, ok := u.subByID[subscriptionID]
		u.subMux.Unlock()
		if ok {
			u.log.WithFields(map[string]interface{}{
				"ID":    subscriptionID,
				"query": query,
			}).Debug("got message from subscription")
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

		u.respMux.Lock()
		wc, ok := u.respMap[msgID]
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

	u.respMux.Lock()
	u.respMap[id] = respChan
	u.respMux.Unlock()

	u.log.WithFields(map[string]interface{}{
		"ID":     id,
		"method": request.Method,
	}).Debug("submitting request")
	if err := cli.SendMsg(request); err != nil {
		u.respMux.Lock()
		delete(u.respMap, id)
		u.respMux.Unlock()
		return nil, err
	}

	// Stoppable timer (not time.After) so the happy path frees the
	// runtime timer slot immediately — see the matching comment in
	// the cosmos variant.
	timeout := time.NewTimer(responseTimeout)
	defer timeout.Stop()
	select {
	case response := <-respChan:
		u.respMux.Lock()
		delete(u.respMap, id)
		u.respMux.Unlock()

		response.ID = req.ID
		return response, nil

	case <-cli.Closed():
		u.respMux.Lock()
		delete(u.respMap, id)
		u.respMux.Unlock()
		return nil, ErrClosed

	case <-timeout.C:
		u.respMux.Lock()
		delete(u.respMap, id)
		u.respMux.Unlock()

		return nil, fmt.Errorf("timeout waiting for response for request with ID %s", id)
	}
}

func (u *UpstreamConnManagerEth) MakeRequest(req *JsonRpcMsg) (*JsonRpcMsg, error) {
	// Generate unique ID for request
	ID := u.IdGen.ID()
	defer u.IdGen.Release(ID)
	return u.makeRequestWithID(ID, req)
}

func (u *UpstreamConnManagerEth) HasSubscription(subID string) bool {
	u.subMux.Lock()
	defer u.subMux.Unlock()
	_, ok := u.subByParam[subID]
	return ok
}

func (u *UpstreamConnManagerEth) Subscribe(query string) (string, error) {
	// Check if this is already subscribed
	if u.HasSubscription(query) {
		return "", ErrSubscriptionExists
	}

	// Generate unique ID for subscription
	ID := u.IdGen.ID()
	return u.subscribeWithID(ID, query)
}

func (u *UpstreamConnManagerEth) subscribeWithID(id string, param string) (string, error) {
	return u.subscribeWithIDOnClient(u.curClient(), id, param)
}

// subscribeWithIDOnClient binds the subscribe round-trip to an explicit
// client so the resubmit-after-reconnect path can't get redirected to a
// new connection mid-flight.
func (u *UpstreamConnManagerEth) subscribeWithIDOnClient(cli *JsonRpcWsClient, id, param string) (string, error) {
	defer u.IdGen.Release(id)

	// Send subscribe request
	msg := &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      id,
		Method:  methodSubscribeEth,
		Params:  []interface{}{param},
	}

	resp, err := u.makeRequestWithIDOnClient(cli, id, msg)
	if err != nil {
		return "", err
	}

	// `Result` is held as RawMessage so the upstream's exact bytes are
	// preserved. The eth_subscribe contract is that result is a JSON
	// string carrying the subscription ID — decode it.
	var subID string
	if err := json.Unmarshal(resp.Result, &subID); err != nil {
		return "", fmt.Errorf("unexpected subscription ID: %s", string(resp.Result))
	}

	u.subMux.Lock()
	defer u.subMux.Unlock()

	u.subByParam[param] = subID
	u.subByID[subID] = param

	u.log.WithFields(map[string]interface{}{
		"ID":    subID,
		"param": param,
	}).Debug("registered subscription with upstream")
	return subID, nil
}

func (u *UpstreamConnManagerEth) Unsubscribe(id string) error {
	msg := &JsonRpcMsg{
		Version: jsonRpcVersion,
		Method:  methodUnsubscribeEth,
		Params:  []string{id},
	}

	_, err := u.MakeRequest(msg)
	if err != nil {
		return err
	}

	u.subMux.Lock()
	defer u.subMux.Unlock()

	// Bail out if the subscription was already removed (e.g. by a
	// concurrent migration). Without the existence check we'd happily
	// `delete(subByParam, "")` and pollute the map with phantom keys.
	param, ok := u.subByID[id]
	if !ok {
		return nil
	}
	delete(u.subByID, id)
	delete(u.subByParam, param)

	u.log.WithFields(map[string]interface{}{
		"ID":    id,
		"param": param,
	}).Debug("removed subscription with upstream")
	return nil
}

func (u *UpstreamConnManagerEth) resetAll() {
	u.subMux.Lock()
	u.subByParam = make(map[string]string)
	u.subByID = make(map[string]string)
	u.subMux.Unlock()

	u.respMux.Lock()
	u.respMap = make(map[string]chan *JsonRpcMsg)
	u.respMux.Unlock()
}

func (u *UpstreamConnManagerEth) reSubmitSubscriptionsOnClient(cli *JsonRpcWsClient) error {
	// Snapshot under subMux so we don't race with concurrent
	// Subscribe/Unsubscribe callers from the pool.
	u.subMux.Lock()
	pending := make([]string, 0, len(u.subByParam))
	for param := range u.subByParam {
		pending = append(pending, param)
	}
	u.subMux.Unlock()

	if len(pending) == 0 {
		return nil
	}
	u.log.Info("re-submitting subscriptions")
	for _, param := range pending {
		u.log.WithFields(map[string]interface{}{
			"param": param,
		}).Debug("re-submitting subscription")
		if u.HasSubscription(param) {
			continue
		}
		id := u.IdGen.ID()
		if _, err := u.subscribeWithIDOnClient(cli, id, param); err != nil {
			return err
		}
	}
	return nil
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
		u.clientMu.Lock()
		if u.client != nil {
			u.client.Close()
		}
		u.clientMu.Unlock()
	})
}
