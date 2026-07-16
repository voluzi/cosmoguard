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
	// migratedAway tombstones params LocalUnsubscribe removed due to a pool
	// migration, so a concurrent reconnect-resubmit can't resurrect them —
	// see the Cosmos impl for the full race. Guarded by subMux.
	migratedAway map[string]struct{}
	subMux       sync.Mutex

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
		migratedAway:          make(map[string]struct{}),
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
			// Reset only the per-connection response map. The
			// subscription maps (subByParam / subByID) MUST survive the
			// reconnect: reSubmitSubscriptionsOnClient reads subByParam to
			// know what to re-issue. Wiping them here (the old resetAll)
			// left an empty map, so client subscriptions were silently
			// never re-established after an EVM WS reconnect. The new
			// upstream subscription IDs are rewritten into subByID as each
			// param is re-subscribed below.
			u.resetConnectionState()
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

		// Delete the entry under the lock BEFORE send+close so a
		// duplicate upstream response with the same ID can't re-load the
		// same closed channel and panic ("send on closed channel"),
		// which would crash the whole process from this recover-less
		// goroutine.
		u.respMux.Lock()
		wc, ok := u.respMap[msgID]
		if ok {
			delete(u.respMap, msgID)
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
	return u.subscribeWithIDOnClient(u.curClient(), id, param, false)
}

// subscribeWithIDOnClient binds the subscribe round-trip to an explicit
// client so the resubmit-after-reconnect path can't get redirected to a
// new connection mid-flight. When resubmit is true and the param was
// migrated away mid-flight, the commit is aborted and the freshly-created
// upstream subscription is torn down — see migratedAway.
func (u *UpstreamConnManagerEth) subscribeWithIDOnClient(cli *JsonRpcWsClient, id, param string, resubmit bool) (string, error) {
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
	if resubmit {
		if _, migrated := u.migratedAway[param]; migrated {
			// Migrated onto another conn while we re-subscribed. Don't
			// re-track it (would be a duplicate); best-effort eth_unsubscribe
			// the id we just minted so the upstream stops pushing on it.
			u.subMux.Unlock()
			u.log.WithField("param", param).Debug("resubmit aborted: param migrated away")
			// Send the eth_unsubscribe on the SAME client the subscribe was
			// created on — subID belongs to `cli`. Using MakeRequest (which
			// targets curClient()) could hit a different socket after a
			// concurrent reconnect, leaving the subscription streaming on the
			// old connection with no mapping.
			_, _ = u.makeRequestWithIDOnClient(cli, u.IdGen.ID(), &JsonRpcMsg{
				Version: jsonRpcVersion,
				Method:  methodUnsubscribeEth,
				Params:  []string{subID},
			})
			return subID, nil
		}
	} else {
		// A genuine (non-resubmit) subscription clears any stale migration
		// tombstone for this param — it's legitimately live on this conn now.
		delete(u.migratedAway, param)
	}
	u.subByParam[param] = subID
	u.subByID[subID] = param
	u.subMux.Unlock()

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

// LocalUnsubscribe drops a subscription param from the local maps with
// no network I/O — see the interface doc. subByParam maps param → the
// current upstream subscription id; both entries are removed so a
// reconnect won't re-issue this (migrated-away) subscription.
func (u *UpstreamConnManagerEth) LocalUnsubscribe(param string) {
	u.subMux.Lock()
	// Tombstone so a concurrent resubmit can't re-add the migrated param.
	// Persists until a genuine new subscription clears it (not reset per
	// resubmit epoch — see the Cosmos impl for the ordering rationale).
	u.migratedAway[param] = struct{}{}
	id, ok := u.subByParam[param]
	if ok {
		delete(u.subByParam, param)
		delete(u.subByID, id)
	}
	u.subMux.Unlock()
	if ok {
		// Best-effort eth_unsubscribe: a racing reconnect-resubmit may have
		// re-created this subscription on the reconnected socket between the
		// migration commit and this call; tear it down so it doesn't stream
		// with no manager mapping. Harmless when the conn is dead.
		_, _ = u.MakeRequest(&JsonRpcMsg{
			Version: jsonRpcVersion,
			Method:  methodUnsubscribeEth,
			Params:  []string{id},
		})
	}
}

// resetConnectionState clears the per-connection in-flight response map
// AND the upstream-subscription-ID index (subByID): both are scoped to
// the dead connection — response channels will never be fulfilled, and
// the new connection mints fresh eth_subscribe IDs. subByParam is
// deliberately PRESERVED: it is the source of truth for which params
// must be re-subscribed on the new connection
// (reSubmitSubscriptionsOnClient reads it). Wiping subByParam here — the
// old resetAll behaviour — left an empty map, so client subscriptions
// were silently never re-established after an EVM WS reconnect.
func (u *UpstreamConnManagerEth) resetConnectionState() {
	u.subMux.Lock()
	u.subByID = make(map[string]string)
	u.subMux.Unlock()

	u.respMux.Lock()
	u.respMap = make(map[string]chan *JsonRpcMsg)
	u.respMux.Unlock()
}

func (u *UpstreamConnManagerEth) reSubmitSubscriptionsOnClient(cli *JsonRpcWsClient) error {
	// Snapshot under subMux so we don't range over the map while
	// subscribeWithIDOnClient writes to it. subByParam survived the
	// reconnect (see resetConnectionState); each param is re-issued on
	// the new client, and subscribeWithIDOnClient rewrites subByParam /
	// subByID with the new upstream subscription ID.
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
		id := u.IdGen.ID()
		if _, err := u.subscribeWithIDOnClient(cli, id, param, true); err != nil {
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
