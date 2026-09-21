package cosmoguard

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/pkg/util"
	"gotest.tools/assert"
)

type limitingUpstream struct {
	healthy            atomic.Bool
	subscribeCalls     atomic.Int32
	requestCalls       atomic.Int32
	failSubscribe      atomic.Bool
	uncertainSubscribe atomic.Bool
	failUnsubscribe    atomic.Bool
	subscribeStarted   chan struct{}
	subscribeRelease   chan struct{}
	unsubscribeStarted chan struct{}
	unsubscribeRelease chan struct{}
	once               sync.Once
}

type wsProtocolCase struct {
	name        string
	constructor UpstreamConnManagerConstructor
	evm         bool
}

type migrationSubscribeProbe struct {
	UpstreamConnManager
	healthy atomic.Bool
	calls   atomic.Int32
}

func (p *migrationSubscribeProbe) Subscribe(param string) (string, error) {
	if p.calls.Add(1) > 1 {
		return "", errors.New("unexpected migration retry")
	}
	return p.UpstreamConnManager.Subscribe(param)
}

func (p *migrationSubscribeProbe) IsHealthy() bool { return p.healthy.Load() }

func wsProtocolCases() []wsProtocolCase {
	return []wsProtocolCase{
		{name: "cosmos", constructor: CosmosUpstreamConnManager},
		{name: "evm", constructor: EthUpstreamConnManager, evm: true},
	}
}

func (p wsProtocolCase) subscribeSuccess(req *JsonRpcMsg, suffix string) *JsonRpcMsg {
	if p.evm {
		return WithResult(req, "subscription-"+suffix)
	}
	return WithResult(req, map[string]any{})
}

func providerRejectedResponse(req *JsonRpcMsg) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      req.ID,
		Error:   &JsonRpcError{Code: -32000, Message: "provider rejected request"},
	}
}

type controlledWSBackend struct {
	requests  chan *JsonRpcMsg
	responses chan *JsonRpcMsg
}

type wsJSONPeer interface {
	ReadJSON(any) error
	WriteMessage(int, []byte) error
}

func newControlledWSBackend(manager UpstreamConnManager, client *JsonRpcWsClient, peer wsJSONPeer) *controlledWSBackend {
	backend := &controlledWSBackend{
		requests:  make(chan *JsonRpcMsg),
		responses: make(chan *JsonRpcMsg),
	}
	go func() {
		for {
			msg, err := client.ReceiveMsg()
			if err != nil {
				return
			}
			dispatchManagerMessageFromClient(manager, client, msg)
		}
	}()
	go func() {
		for {
			var req JsonRpcMsg
			if err := peer.ReadJSON(&req); err != nil {
				return
			}
			backend.requests <- &req
			response, ok := <-backend.responses
			if !ok {
				return
			}
			encoded, err := response.Marshal()
			if err != nil {
				return
			}
			if err := peer.WriteMessage(websocket.TextMessage, encoded); err != nil {
				return
			}
		}
	}()
	return backend
}

func dispatchManagerMessage(manager UpstreamConnManager, msg *JsonRpcMsg) {
	dispatchManagerMessageFromClient(manager, currentManagerClient(manager), msg)
}

func dispatchManagerMessageFromClient(manager UpstreamConnManager, client *JsonRpcWsClient, msg *JsonRpcMsg) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.onUpstreamMessage(client, msg)
	case *UpstreamConnManagerEth:
		manager.onUpstreamMessage(client, msg)
	}
}

func TestControlledWSBackendDispatchesResponseToReaderSocket(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			oldClient, oldPeer := newWSCacheClient(t)
			newClient, _ := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, oldClient)
			setManagerLog(manager, log.WithField("test", t.Name()))
			_ = newControlledWSBackend(manager, oldClient, oldPeer)

			oldResponse := make(chan *JsonRpcMsg, 1)
			newResponse := make(chan *JsonRpcMsg, 1)
			setManagerResponseWaiter(manager, wsResponseKey{client: oldClient, id: "shared"}, oldResponse)
			setManagerResponseWaiter(manager, wsResponseKey{client: newClient, id: "shared"}, newResponse)
			setManagerClient(manager, newClient)

			response := &JsonRpcMsg{Version: jsonRpcVersion, ID: "shared", Result: []byte(`{}`)}
			encoded, err := response.Marshal()
			assert.NilError(t, err)
			assert.NilError(t, oldPeer.WriteMessage(websocket.TextMessage, encoded))
			assert.Assert(t, mustRecv(t, oldResponse, "old socket response") != nil)
			select {
			case <-newResponse:
				t.Fatal("old socket response was delivered to the replacement waiter")
			default:
			}
			assert.NilError(t, oldClient.Close())
			assert.NilError(t, newClient.Close())
		})
	}
}

func setManagerResponseWaiter(manager UpstreamConnManager, key wsResponseKey, waiter chan *JsonRpcMsg) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.respMux.Lock()
		manager.respMap[key] = waiter
		manager.respMux.Unlock()
	case *UpstreamConnManagerEth:
		manager.respMux.Lock()
		manager.respMap[key] = waiter
		manager.respMux.Unlock()
	}
}

type subscribeCallResult struct {
	id  string
	err error
}

func startSubscribe(upstream interface {
	Subscribe(string) (string, error)
}, param string) <-chan subscribeCallResult {
	result := make(chan subscribeCallResult, 1)
	go func() {
		id, err := upstream.Subscribe(param)
		result <- subscribeCallResult{id: id, err: err}
	}()
	return result
}

func startUnsubscribe(upstream interface{ Unsubscribe(string) error }, id string) <-chan error {
	result := make(chan error, 1)
	go func() { result <- upstream.Unsubscribe(id) }()
	return result
}

func newRealManagerPool(t *testing.T, protocol wsProtocolCase, limit int) (*UpstreamPool, UpstreamConnManager, *controlledWSBackend) {
	t.Helper()
	client, peer := newWSCacheClient(t)
	manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
	setManagerClient(manager, client)
	setManagerLog(manager, log.WithField("test", protocol.name+"-protocol"))
	pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {},
		func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
	pool.SetMaxSubscriptionsPerConnection(limit)
	return pool, manager, newControlledWSBackend(manager, client, peer)
}

func newLimitingUpstream() *limitingUpstream {
	u := &limitingUpstream{}
	u.healthy.Store(true)
	return u
}

func (u *limitingUpstream) Run(*Entry) error { return nil }
func (u *limitingUpstream) MakeRequest(msg *JsonRpcMsg) (*JsonRpcMsg, error) {
	u.requestCalls.Add(1)
	return EmptyResult(msg), nil
}
func (u *limitingUpstream) HasSubscription(string) bool { return false }
func (u *limitingUpstream) Subscribe(param string) (string, error) {
	n := u.subscribeCalls.Add(1)
	if u.subscribeStarted != nil {
		u.once.Do(func() { close(u.subscribeStarted) })
		<-u.subscribeRelease
	}
	if u.failSubscribe.Load() {
		return "", errors.New("subscribe failed")
	}
	if u.uncertainSubscribe.Load() {
		return "", uncertainWSUpstreamOutcome(errors.New("acknowledgement lost"))
	}
	return fmt.Sprintf("%s-%d", param, n), nil
}
func (u *limitingUpstream) Unsubscribe(string) error {
	if u.unsubscribeStarted != nil {
		u.unsubscribeStarted <- struct{}{}
		<-u.unsubscribeRelease
	}
	if u.failUnsubscribe.Load() {
		return errors.New("unsubscribe failed")
	}
	return nil
}
func (*limitingUpstream) LocalUnsubscribe(string) <-chan error { return nil }
func (u *limitingUpstream) IsHealthy() bool                    { return u.healthy.Load() }
func (*limitingUpstream) Stop()                                {}

func limitingConstructor(u *limitingUpstream) UpstreamConnManagerConstructor {
	return func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return u }
}

func intPtr(v int) *int { return &v }

func TestWebSocketLimitsEffectiveValues(t *testing.T) {
	limits := (&ServerConfig{}).EffectiveWebSocketLimits()
	assert.Equal(t, limits.MaxSubscriptionsPerClient, 32)
	assert.Equal(t, limits.MaxSubscriptionsPerIdentity, 128)
	assert.Equal(t, limits.MaxSubscriptionsPerUpstreamConnection, 4)
	assert.Equal(t, limits.MaxConnectionsPerIP, 16)

	zero := &ServerConfig{WebSocketLimits: WebSocketLimitsConfig{
		MaxSubscriptionsPerClient:             intPtr(0),
		MaxSubscriptionsPerIdentity:           intPtr(0),
		MaxSubscriptionsPerUpstreamConnection: intPtr(0),
		MaxConnectionsPerIP:                   intPtr(0),
	}}
	effective := zero.EffectiveWebSocketLimits()
	assert.Equal(t, effective.MaxSubscriptionsPerClient, 0)
	assert.Equal(t, effective.MaxSubscriptionsPerIdentity, 0)
	assert.Equal(t, effective.MaxSubscriptionsPerUpstreamConnection, 0)
	assert.Equal(t, effective.MaxConnectionsPerIP, 0)

	negative := &ServerConfig{WebSocketLimits: WebSocketLimitsConfig{MaxSubscriptionsPerClient: intPtr(-1)}}
	assert.ErrorContains(t, validateServerLimits(negative), "maxSubscriptionsPerClient")
	assert.Assert(t, serverRuntimeImmutableChanged(&ServerConfig{}, zero))
	explicitDefaults := &ServerConfig{WebSocketLimits: WebSocketLimitsConfig{
		MaxSubscriptionsPerClient:             intPtr(32),
		MaxSubscriptionsPerIdentity:           intPtr(128),
		MaxSubscriptionsPerUpstreamConnection: intPtr(4),
		MaxConnectionsPerIP:                   intPtr(16),
	}}
	assert.Assert(t, !serverRuntimeImmutableChanged(&ServerConfig{}, explicitDefaults))
}

func TestWSAdmissionControllerSharesClientIdentityAndIPLimits(t *testing.T) {
	admission := newWSAdmissionController(WebSocketLimits{
		MaxSubscriptionsPerClient:   1,
		MaxSubscriptionsPerIdentity: 1,
		MaxConnectionsPerIP:         1,
	})
	clientA := &JsonRpcWsClient{}
	clientB := &JsonRpcWsClient{}

	assert.NilError(t, admission.reserveConnection("192.0.2.1"))
	err := admission.reserveConnection("192.0.2.1")
	var exhausted *WSResourceExhaustedError
	assert.Assert(t, errors.As(err, &exhausted))
	assert.Equal(t, exhausted.Scope, wsLimitScopeSourceIP)
	admission.releaseConnection("192.0.2.1")
	assert.NilError(t, admission.reserveConnection("192.0.2.1"))

	assert.NilError(t, admission.reserveSubscription(clientA, "alice"))
	err = admission.reserveSubscription(clientA, "alice")
	assert.Assert(t, errors.As(err, &exhausted))
	assert.Equal(t, exhausted.Scope, wsLimitScopeClient)
	err = admission.reserveSubscription(clientB, "alice")
	assert.Assert(t, errors.As(err, &exhausted))
	assert.Equal(t, exhausted.Scope, wsLimitScopeIdentity)

	admission.releaseSubscription(clientA)
	assert.NilError(t, admission.reserveSubscription(clientB, "alice"))
	admission.releaseSubscription(clientB)
	assert.NilError(t, admission.reserveSubscription(clientA, ""))
	assert.NilError(t, admission.reserveSubscription(clientB, ""), "anonymous clients must not share an identity bucket")
}

func TestWSConnectionLimitRejectsBeforeUpgrade(t *testing.T) {
	admission := newWSAdmissionController(WebSocketLimits{MaxConnectionsPerIP: 1})
	assert.NilError(t, admission.reserveConnection("192.0.2.10"))
	p := &JsonRpcWebSocketProxy{admission: admission}
	r := httptest.NewRequest(http.MethodGet, "http://example.test/websocket", nil)
	r.RemoteAddr = "192.0.2.10:1234"
	w := httptest.NewRecorder()
	p.HandleConnection(w, r)
	assert.Equal(t, w.Code, http.StatusTooManyRequests)
}

func TestBrokerSubscriptionLimitResponsesAndRelease(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", "ws-limits")
	broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{
		MaxSubscriptionsPerClient:   1,
		MaxSubscriptionsPerIdentity: 1,
	}))
	client := &JsonRpcWsClient{}

	cosmos := &JsonRpcMsg{Version: "2.0", ID: 11, Method: methodSubscribeCosmos, Params: []any{"tm.event='NewBlock'"}}
	response, err := broker.HandleSubscription(client, cosmos, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil)
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1))

	// Repeating the same membership is idempotent and consumes no new slot.
	response, err = broker.HandleSubscription(client, cosmos, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil)
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1))

	evm := &JsonRpcMsg{Version: "2.0", ID: "evm-id", Method: methodSubscribeEth, Params: []any{"newHeads"}}
	response, err = broker.HandleSubscription(client, evm, "alice")
	assert.NilError(t, err)
	assert.Equal(t, response.ID, "evm-id")
	assert.Equal(t, response.Error.Code, -32005)
	assert.Equal(t, response.Error.Message, "WebSocket resource exhausted")
	data := response.Error.Data.(map[string]any)
	assert.Equal(t, data["scope"], wsLimitScopeClient)
	assert.Equal(t, data["limit"], 1)
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1), "rejected membership must not call upstream")

	unsub := &JsonRpcMsg{Version: "2.0", ID: 12, Method: methodUnsubscribeCosmos, Params: []any{"tm.event='NewBlock'"}}
	_, err = broker.HandleSubscription(client, unsub, "alice")
	assert.NilError(t, err)
	response, err = broker.HandleSubscription(client, evm, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil, "unsubscribe must release the downstream slot")

	broker.onClientDisconnect(client)
	other := &JsonRpcWsClient{}
	response, err = broker.HandleSubscription(other, cosmos, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil, "disconnect must release the identity slot exactly once")
}

func TestBrokerDisconnectReleasesAllAdmissionBeforeUpstreamCleanup(t *testing.T) {
	upstream := newLimitingUpstream()
	upstream.unsubscribeStarted = make(chan struct{}, 2)
	upstream.unsubscribeRelease = make(chan struct{})
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{
		MaxSubscriptionsPerClient:   2,
		MaxSubscriptionsPerIdentity: 2,
	}))
	client := &JsonRpcWsClient{}

	for i, param := range []string{"first", "second"} {
		response, err := broker.HandleSubscription(client, &JsonRpcMsg{
			Version: jsonRpcVersion,
			ID:      i + 1,
			Method:  methodSubscribeCosmos,
			Params:  []any{param},
		}, "alice")
		assert.NilError(t, err)
		assert.Assert(t, response.Error == nil)
	}

	disconnected := make(chan error, 1)
	go func() { disconnected <- broker.removeAllSubscriptions(client) }()
	<-upstream.unsubscribeStarted

	replacement := &JsonRpcWsClient{}
	assert.NilError(t, broker.admission.reserveSubscription(replacement, "alice"))
	assert.NilError(t, broker.admission.reserveSubscription(replacement, "alice"),
		"all disconnected memberships must release admission before cleanup I/O")
	broker.admission.releaseSubscription(replacement)
	broker.admission.releaseSubscription(replacement)

	close(upstream.unsubscribeRelease)
	assert.NilError(t, <-disconnected)
}

func TestBrokerDisconnectDuringSubscribeReleasesAllAdmissionBeforeRollback(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{
		MaxSubscriptionsPerClient:   3,
		MaxSubscriptionsPerIdentity: 3,
	}))
	client := NewJsonRpcWsClient(nil)
	client.SetOnDisconnectCallback(broker.onClientDisconnect)
	for i, param := range []string{"first", "second"} {
		response, err := broker.HandleSubscription(client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: i + 1, Method: methodSubscribeCosmos, Params: []any{param},
		}, "alice")
		assert.NilError(t, err)
		assert.Assert(t, response.Error == nil)
	}

	upstream.subscribeStarted = make(chan struct{})
	upstream.subscribeRelease = make(chan struct{})
	upstream.unsubscribeStarted = make(chan struct{}, 3)
	upstream.unsubscribeRelease = make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := broker.addSubscription(client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: 3, Method: methodSubscribeCosmos, Params: []any{"third"},
		}, "alice")
		result <- err
	}()
	<-upstream.subscribeStarted
	assert.NilError(t, client.Close())
	close(upstream.subscribeRelease)
	<-upstream.unsubscribeStarted

	replacement := &JsonRpcWsClient{}
	for i := 0; i < 3; i++ {
		assert.NilError(t, broker.admission.reserveSubscription(replacement, "alice"),
			"disconnect rollback must release every membership before upstream cleanup")
	}
	for i := 0; i < 3; i++ {
		broker.admission.releaseSubscription(replacement)
	}
	close(upstream.unsubscribeRelease)
	assert.Assert(t, errors.Is(<-result, ErrClosed))
}

func TestBrokerFailedSubscribeRollsBackDownstreamAdmission(t *testing.T) {
	upstream := newLimitingUpstream()
	upstream.failSubscribe.Store(true)
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", "ws-limits")
	broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{MaxSubscriptionsPerClient: 1}))
	client := &JsonRpcWsClient{}
	first := &JsonRpcMsg{ID: 1, Method: methodSubscribeEth, Params: []any{"newHeads"}}
	response, err := broker.HandleSubscription(client, first, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error != nil)

	upstream.failSubscribe.Store(false)
	second := &JsonRpcMsg{ID: 2, Method: methodSubscribeEth, Params: []any{"logs"}}
	response, err = broker.HandleSubscription(client, second, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil, "failed upstream subscribe must release downstream admission")
}

func TestBrokerSharedIdentityLimitAndUnsubscribeAllRelease(t *testing.T) {
	admission := newWSAdmissionController(WebSocketLimits{
		MaxSubscriptionsPerClient:   2,
		MaxSubscriptionsPerIdentity: 2,
	})
	upstreamA := newLimitingUpstream()
	upstreamB := newLimitingUpstream()
	brokerA := NewBroker([]string{"ws://rpc.test"}, "/websocket", 1, limitingConstructor(upstreamA))
	brokerB := NewBroker([]string{"ws://evm.test"}, "/", 1, limitingConstructor(upstreamB))
	brokerA.log = log.WithField("test", "rpc")
	brokerB.log = log.WithField("test", "evm")
	brokerA.setAdmissionController(admission)
	brokerB.setAdmissionController(admission)
	clientA := &JsonRpcWsClient{}
	clientB := &JsonRpcWsClient{}

	for id, param := range []string{"tm.event='NewBlock'", "tm.event='Tx'"} {
		request := &JsonRpcMsg{ID: id + 1, Method: methodSubscribeCosmos, Params: []any{param}}
		response, err := brokerA.HandleSubscription(clientA, request, "alice")
		assert.NilError(t, err)
		assert.Assert(t, response.Error == nil)
	}
	response, err := brokerB.HandleSubscription(clientB,
		&JsonRpcMsg{ID: 3, Method: methodSubscribeEth, Params: []any{"newHeads"}}, "alice")
	assert.NilError(t, err)
	assert.Equal(t, response.Error.Code, -32005)
	assert.Equal(t, response.Error.Data.(map[string]any)["scope"], wsLimitScopeIdentity)

	_, err = brokerA.HandleSubscription(clientA,
		&JsonRpcMsg{ID: 4, Method: methodUnsubscribeAllCosmos}, "alice")
	assert.NilError(t, err)
	response, err = brokerB.HandleSubscription(clientB,
		&JsonRpcMsg{ID: 5, Method: methodSubscribeEth, Params: []any{"newHeads"}}, "alice")
	assert.NilError(t, err)
	assert.Assert(t, response.Error == nil, "unsubscribe_all must release all identity slots")
}

func TestBrokerDeduplicatedSubscriptionStillConsumesDownstreamQuota(t *testing.T) {
	admission := newWSAdmissionController(WebSocketLimits{MaxSubscriptionsPerIdentity: 2})
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://rpc.test"}, "/websocket", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", "dedup")
	broker.setAdmissionController(admission)
	request := &JsonRpcMsg{ID: 1, Method: methodSubscribeCosmos, Params: []any{"tm.event='NewBlock'"}}

	for _, client := range []*JsonRpcWsClient{{}, {}} {
		response, err := broker.HandleSubscription(client, request, "alice")
		assert.NilError(t, err)
		assert.Assert(t, response.Error == nil)
	}
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1), "deduplicated membership must not use another upstream slot")

	response, err := broker.HandleSubscription(&JsonRpcWsClient{}, request, "alice")
	assert.NilError(t, err)
	assert.Equal(t, response.Error.Code, -32005)
	assert.Equal(t, response.Error.Data.(map[string]any)["scope"], wsLimitScopeIdentity)
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1), "identity rejection must not call upstream")
}

func TestUpstreamSubscriptionLimitReservesConcurrentLastSlot(t *testing.T) {
	upstream := newLimitingUpstream()
	upstream.subscribeStarted = make(chan struct{})
	upstream.subscribeRelease = make(chan struct{})
	pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {}, limitingConstructor(upstream))
	pool.SetMaxSubscriptionsPerConnection(1)

	firstDone := make(chan error, 1)
	go func() {
		_, err := pool.Subscribe("first")
		firstDone <- err
	}()
	<-upstream.subscribeStarted
	_, err := pool.Subscribe("second")
	var exhausted *WSResourceExhaustedError
	assert.Assert(t, errors.As(err, &exhausted))
	assert.Equal(t, exhausted.Scope, wsLimitScopeUpstreamConnection)
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1), "capacity must reserve before network I/O")
	close(upstream.subscribeRelease)
	assert.NilError(t, <-firstDone)
}

func TestUpstreamLimitRollbackMigrationAndOrdinaryRPC(t *testing.T) {
	failed := newLimitingUpstream()
	failed.failSubscribe.Store(true)
	pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {}, limitingConstructor(failed))
	pool.SetMaxSubscriptionsPerConnection(1)
	_, err := pool.Subscribe("first")
	assert.ErrorContains(t, err, "subscribe failed")
	failed.failSubscribe.Store(false)
	_, err = pool.Subscribe("second")
	assert.NilError(t, err, "failed subscribe must roll back the upstream reservation")
	_, err = pool.MakeRequest(&JsonRpcMsg{ID: 1, Method: "status"})
	assert.NilError(t, err, "ordinary RPC must remain independent of subscription capacity")
	assert.Equal(t, failed.requestCalls.Load(), int32(1))

	dead := newLimitingUpstream()
	alive := newLimitingUpstream()
	dead.healthy.Store(false)
	migrationPool := &UpstreamPool{
		conn:                    []UpstreamConnManager{dead, alive},
		subscriptionConn:        map[string]UpstreamConnManager{"old": dead},
		subscriptionID:          map[string]string{"victim": "old", "full": "full-id"},
		subscriptionParam:       map[string]string{"old": "victim", "full-id": "full"},
		subCount:                map[UpstreamConnManager]*atomic.Int64{dead: {}, alive: {}},
		maxSubscriptionsPerConn: 1,
	}
	migrationPool.subCount[dead].Store(1)
	migrationPool.subCount[alive].Store(1)
	assert.Equal(t, len(migrationPool.MigrateUnhealthy()), 0)
	assert.Equal(t, alive.subscribeCalls.Load(), int32(0), "migration must not exceed survivor capacity")
}

func TestUpstreamFailedUnsubscribeKeepsCapacityOccupied(t *testing.T) {
	upstream := newLimitingUpstream()
	pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {}, limitingConstructor(upstream))
	pool.SetMaxSubscriptionsPerConnection(1)
	id, err := pool.Subscribe("first")
	assert.NilError(t, err)
	upstream.failUnsubscribe.Store(true)
	assert.ErrorContains(t, pool.Unsubscribe(id), "unsubscribe failed")
	_, err = pool.Subscribe("second")
	var exhausted *WSResourceExhaustedError
	assert.Assert(t, errors.As(err, &exhausted), "failed cleanup must conservatively retain capacity")
	assert.Equal(t, upstream.subscribeCalls.Load(), int32(1))
}

func TestProviderRejectedSubscribeReleasesCapacity(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			pool, _, backend := newRealManagerPool(t, protocol, 1)

			firstCall := startSubscribe(pool, "first")
			firstReq := <-backend.requests
			backend.responses <- providerRejectedResponse(firstReq)
			first := <-firstCall
			assert.ErrorContains(t, first.err, "provider rejected request")
			assert.Assert(t, !isUncertainWSUpstreamOutcome(first.err), "provider rejection is a definite failure")

			secondCall := startSubscribe(pool, "second")
			select {
			case secondReq := <-backend.requests:
				backend.responses <- protocol.subscribeSuccess(secondReq, "second")
				second := <-secondCall
				assert.NilError(t, second.err)
			case second := <-secondCall:
				assert.NilError(t, second.err, "definite rejection must release upstream capacity")
			}
		})
	}
}

func TestEVMLogicalHandleIsU256Compatible(t *testing.T) {
	protocol := wsProtocolCase{name: "evm", constructor: EthUpstreamConnManager, evm: true}
	pool, _, backend := newRealManagerPool(t, protocol, 1)

	subscribeCall := startSubscribe(pool, "newHeads")
	request := <-backend.requests
	backend.responses <- WithResult(request, "provider-wire-id")
	subscription := <-subscribeCall
	assert.NilError(t, subscription.err)
	assertEVMU256Handle(t, subscription.id)
}

func assertEVMU256Handle(t *testing.T, handle string) {
	t.Helper()
	if !strings.HasPrefix(handle, "0x") {
		t.Fatalf("EVM subscription handle %q is not 0x-prefixed", handle)
	}
	value, ok := new(big.Int).SetString(strings.TrimPrefix(handle, "0x"), 16)
	if !ok {
		t.Fatalf("EVM subscription handle %q is not hexadecimal", handle)
	}
	if value.BitLen() > 256 {
		t.Fatalf("EVM subscription handle %q exceeds U256", handle)
	}
}

func TestMissingResultUncertaintyIsSubscribeSpecific(t *testing.T) {
	response := &JsonRpcMsg{Version: jsonRpcVersion, ID: "request"}
	ordinaryErr := validateWSJSONRPCResponse("status", response)
	assert.ErrorContains(t, ordinaryErr, "missing result")
	assert.Assert(t, !isUncertainWSUpstreamOutcome(ordinaryErr))

	settled := make(chan struct{})
	subscribeErr := validateWSSubscribeResponse(methodSubscribeEth, response, settled)
	assert.ErrorContains(t, subscribeErr, "missing result")
	assert.Assert(t, isUncertainWSUpstreamOutcome(subscribeErr))
	assert.Equal(t, uncertainWSUpstreamOutcomeSettlement(subscribeErr), (<-chan struct{})(settled))
}

func TestMissingSubscribeResultRetainsCapacityUntilOriginCloses(t *testing.T) {
	responses := []struct {
		name   string
		result []byte
	}{
		{name: "missing"},
		{name: "null", result: []byte("null")},
	}
	for _, protocol := range wsProtocolCases() {
		for _, response := range responses {
			t.Run(protocol.name+"/"+response.name, func(t *testing.T) {
				pool, manager, backend := newRealManagerPool(t, protocol, 1)
				origin := currentManagerClient(manager)

				firstCall := startSubscribe(pool, "first")
				firstReq := <-backend.requests
				backend.responses <- &JsonRpcMsg{
					Version: jsonRpcVersion,
					ID:      firstReq.ID,
					Result:  response.result,
				}
				first := <-firstCall
				assert.ErrorContains(t, first.err, "missing result")
				assert.Assert(t, isUncertainWSUpstreamOutcome(first.err))
				assert.Assert(t, !manager.HasSubscription("first"))

				secondCall := startSubscribe(pool, "second")
				select {
				case request := <-backend.requests:
					t.Fatalf("ambiguous acknowledgement admitted another upstream request: %+v", request)
				case second := <-secondCall:
					var exhausted *WSResourceExhaustedError
					assert.Assert(t, errors.As(second.err, &exhausted))
				}

				settled := uncertainWSUpstreamOutcomeSettlement(first.err)
				assert.Assert(t, settled != nil)
				assert.NilError(t, origin.Close())
				<-settled

				pool.subMux.Lock()
				count := pool.subCount[manager].Load()
				pool.subMux.Unlock()
				assert.Equal(t, count, int64(0))

				replacement, peer := newWSCacheClient(t)
				setManagerClient(manager, replacement)
				replacementBackend := newControlledWSBackend(manager, replacement, peer)
				thirdCall := startSubscribe(pool, "third")
				thirdRequest := <-replacementBackend.requests
				replacementBackend.responses <- protocol.subscribeSuccess(thirdRequest, "third")
				third := <-thirdCall
				assert.NilError(t, third.err, "closing the exact origin must reclaim capacity")
			})
		}
	}
}

func TestInvalidUnsubscribeAcknowledgementKeepsPoolCapacityOccupied(t *testing.T) {
	acknowledgements := []struct {
		name     string
		response func(*JsonRpcMsg) *JsonRpcMsg
	}{
		{name: "provider error", response: providerRejectedResponse},
		{name: "malformed result", response: func(req *JsonRpcMsg) *JsonRpcMsg {
			return WithResult(req, "not-an-unsubscribe-acknowledgement")
		}},
		{name: "false result", response: func(req *JsonRpcMsg) *JsonRpcMsg {
			return WithResult(req, false)
		}},
	}
	for _, protocol := range wsProtocolCases() {
		for _, acknowledgement := range acknowledgements {
			t.Run(protocol.name+"/"+acknowledgement.name, func(t *testing.T) {
				pool, manager, backend := newRealManagerPool(t, protocol, 1)

				subscribeCall := startSubscribe(pool, "first")
				subscribeReq := <-backend.requests
				backend.responses <- protocol.subscribeSuccess(subscribeReq, "first")
				subscribe := <-subscribeCall
				assert.NilError(t, subscribe.err)

				unsubscribeCall := startUnsubscribe(pool, subscribe.id)
				unsubscribeReq := <-backend.requests
				backend.responses <- acknowledgement.response(unsubscribeReq)
				assert.Assert(t, <-unsubscribeCall != nil, "invalid acknowledgement must fail cleanup")
				assert.Assert(t, manager.HasSubscription("first"), "failed cleanup must preserve manager membership")

				secondCall := startSubscribe(pool, "second")
				var second subscribeCallResult
				select {
				case secondReq := <-backend.requests:
					backend.responses <- protocol.subscribeSuccess(secondReq, "second")
					second = <-secondCall
				case second = <-secondCall:
				}
				var exhausted *WSResourceExhaustedError
				assert.Assert(t, errors.As(second.err, &exhausted), "failed cleanup must retain upstream capacity")
			})
		}
	}
}

func TestConcurrentSameParamSubscribeCoalescesCreation(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			clientA, peerA := newWSCacheClient(t)
			clientB, peerB := newWSCacheClient(t)
			managerA := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			managerB := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(managerA, clientA)
			setManagerClient(managerB, clientB)
			setManagerLog(managerA, log.WithField("test", protocol.name+"-duplicate-a"))
			setManagerLog(managerB, log.WithField("test", protocol.name+"-duplicate-b"))
			backendA := newControlledWSBackend(managerA, clientA, peerA)
			backendB := newControlledWSBackend(managerB, clientB, peerB)
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{managerA, managerB},
				subscriptionConn:        make(map[string]UpstreamConnManager),
				subscriptionID:          make(map[string]string),
				subscriptionParam:       make(map[string]string),
				subCount:                map[UpstreamConnManager]*atomic.Int64{managerA: {}, managerB: {}},
				maxSubscriptionsPerConn: 1,
			}
			joined := make(chan struct{})
			pool.afterJoinPendingCreate = func() { close(joined) }

			firstCall := startSubscribe(pool, "same")
			firstReq := <-backendA.requests
			secondCall := startSubscribe(pool, "same")
			<-joined
			select {
			case req := <-backendB.requests:
				t.Fatalf("duplicate subscribe reached a second upstream: %+v", req)
			default:
			}
			backendA.responses <- protocol.subscribeSuccess(firstReq, "winner")
			first := <-firstCall
			second := <-secondCall
			assert.NilError(t, first.err)
			assert.NilError(t, second.err)
			assert.Equal(t, first.id, second.id)
			assert.Assert(t, managerA.HasSubscription("same"), "winning upstream subscription must stay live")
			assert.Assert(t, !managerB.HasSubscription("same"), "coalesced creation must not install a duplicate")

			thirdCall := startSubscribe(pool, "other")
			thirdReq := <-backendB.requests
			backendB.responses <- protocol.subscribeSuccess(thirdReq, "other")
			third := <-thirdCall
			assert.NilError(t, third.err)
		})
	}
}

func TestProviderRejectedMigrationCleanupKeepsCapacityOccupied(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", protocol.name+"-migration-cleanup"))
			backend := newControlledWSBackend(manager, client, peer)
			dead := newLimitingUpstream()
			dead.healthy.Store(false)
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{dead, manager},
				subscriptionConn:        map[string]UpstreamConnManager{"old": dead},
				subscriptionID:          map[string]string{"victim": "old"},
				subscriptionParam:       map[string]string{"old": "victim"},
				subCount:                map[UpstreamConnManager]*atomic.Int64{dead: {}, manager: {}},
				maxSubscriptionsPerConn: 1,
			}
			pool.subCount[dead].Store(1)

			migrationCall := make(chan []SubscriptionMigration, 1)
			go func() { migrationCall <- pool.MigrateUnhealthy() }()
			subscribeReq := <-backend.requests
			pool.subMux.Lock()
			delete(pool.subscriptionConn, "old")
			delete(pool.subscriptionID, "victim")
			delete(pool.subscriptionParam, "old")
			pool.addSubCount(dead, -1)
			pool.subMux.Unlock()
			backend.responses <- protocol.subscribeSuccess(subscribeReq, "replacement")
			cleanupReq := <-backend.requests
			backend.responses <- providerRejectedResponse(cleanupReq)
			assert.Equal(t, len(<-migrationCall), 0)
			assert.Assert(t, !manager.HasSubscription("victim"), "abandoned replacement must not remain desired after cleanup rejection")

			secondCall := startSubscribe(pool, "second")
			var second subscribeCallResult
			select {
			case secondReq := <-backend.requests:
				backend.responses <- protocol.subscribeSuccess(secondReq, "second")
				second = <-secondCall
			case second = <-secondCall:
			}
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(second.err, &exhausted), "rejected migration cleanup must retain upstream capacity")
		})
	}
}

func TestUncertainMigrationBlocksRepeatUntilExactSocketSettles(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)
			alternate := &migrationSubscribeProbe{UpstreamConnManager: manager}
			alternate.healthy.Store(true)
			source := newLimitingUpstream()
			source.healthy.Store(false)
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{source, alternate},
				subscriptionConn:        map[string]UpstreamConnManager{"old": source},
				subscriptionID:          map[string]string{"victim": "old"},
				subscriptionParam:       map[string]string{"old": "victim"},
				subCount:                map[UpstreamConnManager]*atomic.Int64{source: {}, alternate: {}},
				maxSubscriptionsPerConn: 2,
			}
			pool.subCount[source].Store(1)

			firstScan := make(chan []SubscriptionMigration, 1)
			go func() { firstScan <- pool.MigrateUnhealthy() }()
			request := <-backend.requests
			backend.responses <- &JsonRpcMsg{Version: jsonRpcVersion, ID: request.ID}
			assert.Equal(t, len(<-firstScan), 0)
			assert.Equal(t, alternate.calls.Load(), int32(1))

			assert.Equal(t, len(pool.MigrateUnhealthy()), 0)
			assert.Equal(t, alternate.calls.Load(), int32(1),
				"an ambiguous migration must not be retried before exact socket settlement")
			_, err := pool.Subscribe("victim")
			var pending *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &pending), "ambiguous migration must block joins until settlement")

			assert.NilError(t, client.Close())
			deadline := time.Now().Add(time.Second)
			for {
				pool.subMux.Lock()
				_, moving := pool.migrating["old"]
				_, draining := pool.drainingParams["victim"]
				count := pool.subCount[alternate].Load()
				pool.subMux.Unlock()
				if !moving && !draining && count == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("ambiguous migration did not settle: moving=%v draining=%v count=%d", moving, draining, count)
				}
				runtime.Gosched()
			}

			assert.Equal(t, len(pool.MigrateUnhealthy()), 0)
			assert.Equal(t, alternate.calls.Load(), int32(2), "settlement must allow a later migration attempt")
		})
	}
}

func TestOverlappingMigrationAndSourceDrainsBlockUntilBothSettle(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			sourceClient, sourcePeer := newWSCacheClient(t)
			source := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(source, sourceClient)
			setManagerLog(source, log.WithField("test", t.Name()+"-source"))
			sourceBackend := newControlledWSBackend(source, sourceClient, sourcePeer)
			created := startSubscribe(source, "victim")
			request := <-sourceBackend.requests
			sourceBackend.responses <- protocol.subscribeSuccess(request, "source")
			initial := <-created
			assert.NilError(t, initial.err)
			assert.NilError(t, sourceClient.Close())

			destinationClient, destinationPeer := newWSCacheClient(t)
			destinationManager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(destinationManager, destinationClient)
			setManagerLog(destinationManager, log.WithField("test", t.Name()+"-destination"))
			destinationBackend := newControlledWSBackend(destinationManager, destinationClient, destinationPeer)
			destination := &migrationSubscribeProbe{UpstreamConnManager: destinationManager}
			destination.healthy.Store(true)
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{source, destination},
				subscriptionConn:        map[string]UpstreamConnManager{initial.id: source},
				subscriptionID:          map[string]string{"victim": initial.id},
				subscriptionParam:       map[string]string{initial.id: "victim"},
				subCount:                map[UpstreamConnManager]*atomic.Int64{source: {}, destination: {}},
				maxSubscriptionsPerConn: 2,
			}
			pool.subCount[source].Store(1)

			migration := make(chan []SubscriptionMigration, 1)
			go func() { migration <- pool.MigrateUnhealthy() }()
			request = <-destinationBackend.requests
			destinationBackend.responses <- &JsonRpcMsg{Version: jsonRpcVersion, ID: request.ID}
			assert.Equal(t, len(<-migration), 0)

			reconnectedClient, reconnectedPeer := newWSCacheClient(t)
			setManagerClient(source, reconnectedClient)
			reconnectedBackend := newControlledWSBackend(source, reconnectedClient, reconnectedPeer)
			replayed := startManagerResubscribe(source, reconnectedClient, "victim", initial.id)
			request = <-reconnectedBackend.requests
			reconnectedBackend.responses <- protocol.subscribeSuccess(request, "replayed")
			assert.NilError(t, <-replayed)

			removed := startUnsubscribe(pool, initial.id)
			<-reconnectedBackend.requests
			close(reconnectedBackend.responses)
			assert.NilError(t, reconnectedClient.Close())
			removeErr := <-removed
			assert.Assert(t, isUncertainWSUpstreamOutcome(removeErr))
			if settled := uncertainWSUpstreamOutcomeSettlement(removeErr); settled != nil {
				<-settled
			}

			_, err := pool.Subscribe("victim")
			var pending *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &pending),
				"settling the source drain must not clear the outstanding destination drain")
			assert.Equal(t, destination.calls.Load(), int32(1))

			assert.NilError(t, destinationClient.Close())
			deadline := time.Now().Add(time.Second)
			for {
				pool.subMux.Lock()
				_, draining := pool.drainingParams["victim"]
				pool.subMux.Unlock()
				if !draining {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("destination drain did not settle")
				}
				runtime.Gosched()
			}
		})
	}
}

func TestLocalRetirementReleasesPhysicalHandleReservation(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			candidates := make(chan string, 3)
			candidates <- "retired"
			candidates <- "retired"
			candidates <- "fallback"
			idGen := util.NewUniqueID(func() string { return <-candidates })
			reservation := idGen.ID()
			client, _ := newWSCacheClient(t)
			assert.NilError(t, client.Close())
			manager := protocol.constructor(url.URL{}, idGen, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			handle := reservation
			if protocol.evm {
				handle = manager.(*UpstreamConnManagerEth).stableHandle(reservation, "")
			}
			seedManagerSubscriptionWithReservation(manager, "victim", handle, reservation)

			settled := manager.LocalUnsubscribe("victim")
			assert.NilError(t, <-settled)
			reused := idGen.ID()
			assert.Equal(t, reused, reservation, "retired physical handle reservation must be reusable")
			idGen.Release(reused)
		})
	}
}

func TestMigratedCanonicalReservationReleasesOnFinalRemoval(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			candidates := make(chan string, 8)
			idGen := util.NewUniqueID(func() string { return <-candidates })
			sourceClient, sourcePeer := newWSCacheClient(t)
			source := protocol.constructor(url.URL{}, idGen, func(*JsonRpcMsg) {})
			setManagerClient(source, sourceClient)
			setManagerLog(source, log.WithField("test", t.Name()+"-source"))
			sourceBackend := newControlledWSBackend(source, sourceClient, sourcePeer)

			candidates <- "canonical"
			created := startSubscribe(source, "victim")
			request := <-sourceBackend.requests
			sourceBackend.responses <- protocol.subscribeSuccess(request, "source")
			initial := <-created
			assert.NilError(t, initial.err)
			assert.NilError(t, sourceClient.Close())

			alternateClient, alternatePeer := newWSCacheClient(t)
			alternate := protocol.constructor(url.URL{}, idGen, func(*JsonRpcMsg) {})
			setManagerClient(alternate, alternateClient)
			setManagerLog(alternate, log.WithField("test", t.Name()+"-alternate"))
			alternateBackend := newControlledWSBackend(alternate, alternateClient, alternatePeer)
			finalInitialClient, _ := newWSCacheClient(t)
			assert.NilError(t, finalInitialClient.Close())
			finalManager := protocol.constructor(url.URL{}, idGen, func(*JsonRpcMsg) {})
			setManagerClient(finalManager, finalInitialClient)
			setManagerLog(finalManager, log.WithField("test", t.Name()+"-final"))
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{source, alternate, finalManager},
				IdGen:                   idGen,
				subscriptionConn:        map[string]UpstreamConnManager{initial.id: source},
				subscriptionID:          map[string]string{"victim": initial.id},
				subscriptionParam:       map[string]string{initial.id: "victim"},
				subCount:                map[UpstreamConnManager]*atomic.Int64{source: {}, alternate: {}, finalManager: {}},
				maxSubscriptionsPerConn: 2,
			}
			pool.subCount[source].Store(1)

			candidates <- "destination"
			migratedCall := make(chan []SubscriptionMigration, 1)
			go func() { migratedCall <- pool.MigrateUnhealthy() }()
			request = <-alternateBackend.requests
			alternateBackend.responses <- protocol.subscribeSuccess(request, "alternate")
			migrations := <-migratedCall
			assert.Equal(t, len(migrations), 1)

			candidates <- "canonical"
			candidates <- "probe"
			probe := idGen.ID()
			assert.Equal(t, probe, "probe", "stable client-facing reservation must remain held after migration")
			idGen.Release(probe)

			assert.NilError(t, alternateClient.Close())
			finalClient, finalPeer := newWSCacheClient(t)
			setManagerClient(finalManager, finalClient)
			finalBackend := newControlledWSBackend(finalManager, finalClient, finalPeer)
			candidates <- "replacement"
			migratedAgainCall := make(chan []SubscriptionMigration, 1)
			go func() { migratedAgainCall <- pool.MigrateUnhealthy() }()
			request = <-finalBackend.requests
			finalBackend.responses <- protocol.subscribeSuccess(request, "final")
			migratedAgain := <-migratedAgainCall
			assert.Equal(t, len(migratedAgain), 1)
			deadline := time.Now().Add(time.Second)
			for pool.subCount[alternate].Load() != 0 {
				if time.Now().After(deadline) {
					t.Fatal("intermediate source cleanup did not settle")
				}
				runtime.Gosched()
			}

			candidates <- "destination"
			physicalResult := make(chan string, 1)
			go func() { physicalResult <- idGen.ID() }()
			var physical string
			select {
			case physical = <-physicalResult:
			case <-time.After(time.Second):
				candidates <- "physical-fallback"
				physical = <-physicalResult
			}
			assert.Equal(t, physical, "destination", "intermediate physical reservation must retire after migration")
			idGen.Release(physical)

			removed := startUnsubscribe(pool, migratedAgain[0].NewID)
			request = <-finalBackend.requests
			if protocol.evm {
				finalBackend.responses <- WithResult(request, true)
			} else {
				finalBackend.responses <- WithResult(request, map[string]any{})
			}
			assert.NilError(t, <-removed)

			candidates <- "canonical"
			reusedResult := make(chan string, 1)
			go func() { reusedResult <- idGen.ID() }()
			var reused string
			select {
			case reused = <-reusedResult:
			case <-time.After(time.Second):
				candidates <- "fallback"
				reused = <-reusedResult
			}
			assert.Equal(t, reused, "canonical", "final removal must release the stable reservation")
			idGen.Release(reused)
		})
	}
}

func TestRejectedSourceCleanupAfterMigrationKeepsCapacityOccupied(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			sourceInitialClient, _ := newWSCacheClient(t)
			assert.NilError(t, sourceInitialClient.Close())
			source := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(source, sourceInitialClient)
			setManagerLog(source, log.WithField("test", protocol.name+"-migration-source"))
			seedManagerSubscription(source, "victim", "source-subscription")

			alternateClient, alternatePeer := newWSCacheClient(t)
			alternate := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(alternate, alternateClient)
			setManagerLog(alternate, log.WithField("test", protocol.name+"-migration-alternate"))
			alternateBackend := newControlledWSBackend(alternate, alternateClient, alternatePeer)

			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{source, alternate},
				subscriptionConn:        map[string]UpstreamConnManager{"source-subscription": source},
				subscriptionID:          map[string]string{"victim": "source-subscription"},
				subscriptionParam:       map[string]string{"source-subscription": "victim"},
				subCount:                map[UpstreamConnManager]*atomic.Int64{source: {}, alternate: {}},
				maxSubscriptionsPerConn: 1,
			}
			pool.subCount[source].Store(1)

			migrationCall := make(chan []SubscriptionMigration, 1)
			go func() { migrationCall <- pool.MigrateUnhealthy() }()
			alternateSubscribeReq := <-alternateBackend.requests

			sourceClient, sourcePeer := newWSCacheClient(t)
			setManagerClient(source, sourceClient)
			sourceBackend := newControlledWSBackend(source, sourceClient, sourcePeer)
			resubscribeCall := startManagerResubscribe(source, sourceClient, "victim", "source-subscription")
			sourceSubscribeReq := <-sourceBackend.requests
			sourceBackend.responses <- protocol.subscribeSuccess(sourceSubscribeReq, "source-resubscribe")
			assert.NilError(t, <-resubscribeCall)

			alternateBackend.responses <- protocol.subscribeSuccess(alternateSubscribeReq, "alternate")
			sourceCleanupReq := <-sourceBackend.requests
			assert.Equal(t, len(<-migrationCall), 1)
			sourceBackend.responses <- providerRejectedResponse(sourceCleanupReq)

			secondCall := startSubscribe(pool, "second")
			var second subscribeCallResult
			select {
			case sourceSecondReq := <-sourceBackend.requests:
				sourceBackend.responses <- protocol.subscribeSuccess(sourceSecondReq, "source-second")
				second = <-secondCall
			case alternateSecondReq := <-alternateBackend.requests:
				alternateBackend.responses <- protocol.subscribeSuccess(alternateSecondReq, "alternate-second")
				second = <-secondCall
			case second = <-secondCall:
			}
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(second.err, &exhausted), "rejected source cleanup must retain its occupied slot")
		})
	}
}

func TestLifecycleRetirementJoinsReplayInFlight(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			oldClient, _ := newWSCacheClient(t)
			assert.NilError(t, oldClient.Close())
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, oldClient)
			seedManagerSubscription(manager, "victim", "stable-subscription")

			client, peer := newWSCacheClient(t)
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)
			replay := startManagerResubscribe(manager, client, "victim", "stable-subscription")
			subscribeRequest := <-backend.requests

			cleanup := manager.LocalUnsubscribe("victim")
			backend.responses <- protocol.subscribeSuccess(subscribeRequest, "replayed")
			assert.NilError(t, <-replay)

			unsubscribeRequest := <-backend.requests
			if protocol.evm {
				backend.responses <- WithResult(unsubscribeRequest, true)
			} else {
				backend.responses <- WithResult(unsubscribeRequest, map[string]any{})
			}
			assert.NilError(t, <-cleanup)
			assert.Assert(t, !manager.HasSubscription("victim"))
			assert.Assert(t, !client.IsClosed())
		})
	}
}

func TestLifecycleStopSettlesInFlightSubscribe(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)

			created := startSubscribe(manager, "victim")
			<-backend.requests
			manager.Stop()
			close(backend.responses)
			result := <-created
			assert.Assert(t, result.err != nil)
			assert.Assert(t, !manager.HasSubscription("victim"))
			assert.Assert(t, client.IsClosed())
		})
	}
}

func TestLifecycleStopSettlesInFlightUnsubscribe(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			seedManagerSubscription(manager, "victim", "stable-subscription")
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)

			removed := startUnsubscribe(manager, "stable-subscription")
			<-backend.requests
			manager.Stop()
			close(backend.responses)
			assert.Assert(t, <-removed != nil)
			assert.Assert(t, !manager.HasSubscription("victim"))
			assert.Assert(t, client.IsClosed())
		})
	}
}

func TestCosmosReconnectFailurePreservesCanonicalID(t *testing.T) {
	cases := []struct {
		name      string
		response  func(*JsonRpcMsg) *JsonRpcMsg
		uncertain bool
	}{
		{name: "provider rejection", response: providerRejectedResponse},
		{name: "missing result", uncertain: true, response: func(req *JsonRpcMsg) *JsonRpcMsg {
			return &JsonRpcMsg{Version: jsonRpcVersion, ID: req.ID}
		}},
		{name: "null result", uncertain: true, response: func(req *JsonRpcMsg) *JsonRpcMsg {
			return &JsonRpcMsg{Version: jsonRpcVersion, ID: req.ID, Result: []byte("null")}
		}},
		{name: "socket close", uncertain: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var candidate atomic.Uint32
			ids := []string{"canonical", "next"}
			idGen := util.NewUniqueID(func() string {
				return ids[(candidate.Add(1)-1)%uint32(len(ids))]
			})
			canonicalID := idGen.ID()
			oldClient, _ := newWSCacheClient(t)
			assert.NilError(t, oldClient.Close())
			client, peer := newWSCacheClient(t)
			manager := CosmosUpstreamConnManager(url.URL{}, idGen, func(*JsonRpcMsg) {}).(*UpstreamConnManagerCosmos)
			setManagerClient(manager, oldClient)
			seedManagerSubscription(manager, "victim", canonicalID)
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)

			resubmit := startManagerResubscribe(manager, client, "victim", canonicalID)
			request := <-backend.requests
			if tc.response == nil {
				close(backend.responses)
				assert.NilError(t, client.Close())
			} else {
				backend.responses <- tc.response(request)
			}
			err := <-resubmit
			assert.Assert(t, err != nil)
			assert.Equal(t, isUncertainWSUpstreamOutcome(err), tc.uncertain)
			assert.Assert(t, manager.HasSubscription("victim"), "failed reconnect must retain logical membership")

			candidate.Store(0)
			nextID := idGen.ID()
			assert.Assert(t, nextID != canonicalID,
				"failed reconnect released canonical ID %s for reuse", canonicalID)
			idGen.Release(nextID)
		})
	}
}

func seedManagerSubscription(manager UpstreamConnManager, param, id string) {
	seedManagerSubscriptionWithReservation(manager, param, id, id)
}

func seedManagerSubscriptionWithReservation(manager UpstreamConnManager, param, id, reservation string) {
	lifecycle := managerLifecycle(manager)
	client := lifecycle.currentClient()
	record := &wsSubscriptionRecord{
		param: param, handle: id, reservation: reservation, state: wsSubscriptionActive, desired: true,
		binding: &wsSubscriptionBinding{client: client, wireID: id}, settled: make(chan struct{}),
	}
	lifecycle.mu.Lock()
	lifecycle.byParam[param] = record
	lifecycle.byHandle[id] = record
	lifecycle.byWire[wsSubscriptionBindingKey{client: client, wireID: id}] = record
	lifecycle.mu.Unlock()
}

func startManagerResubscribe(manager UpstreamConnManager, client *JsonRpcWsClient, param, id string) <-chan error {
	result := make(chan error, 1)
	go func() {
		switch manager := manager.(type) {
		case *UpstreamConnManagerCosmos:
			result <- manager.subscribeWithIDOnClient(client, id, param, true)
		case *UpstreamConnManagerEth:
			_, err := manager.subscribeWithIDOnClient(client, id, param, true)
			result <- err
		}
	}()
	return result
}

func TestUpstreamUncertainSubscribeReservationEndsWithOriginSocket(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			firstClient, firstPeer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, firstClient)
			setManagerLog(manager, log.WithField("test", protocol.name+"-uncertain"))
			pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {},
				func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
			pool.SetMaxSubscriptionsPerConnection(1)

			firstCall := startSubscribe(pool, "first")
			var firstReq JsonRpcMsg
			assert.NilError(t, firstPeer.ReadJSON(&firstReq))

			_, err := pool.Subscribe("second")
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &exhausted), "pending uncertain request must hold the slot")

			assert.NilError(t, firstClient.Close())
			first := <-firstCall
			assert.Assert(t, errors.Is(first.err, ErrClosed), "the original cause must remain discoverable")

			secondClient, secondPeer := newWSCacheClient(t)
			setManagerClient(manager, secondClient)
			secondCall := startSubscribe(pool, "second")
			go func() {
				var req JsonRpcMsg
				if secondPeer.ReadJSON(&req) == nil {
					encoded, _ := protocol.subscribeSuccess(&req, "second").Marshal()
					_ = secondPeer.WriteMessage(websocket.TextMessage, encoded)
				}
			}()
			go func() {
				if msg, receiveErr := secondClient.ReceiveMsg(); receiveErr == nil {
					dispatchManagerMessage(manager, msg)
				}
			}()
			second := <-secondCall
			assert.NilError(t, second.err, "closing the exact origin socket must reclaim capacity")
		})
	}
}

func TestUpstreamWriteFailureClosesOriginAndReclaimsReservation(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, _ := newWSCacheClient(t)
			tcp, ok := client.conn.UnderlyingConn().(*net.TCPConn)
			assert.Assert(t, ok)
			assert.NilError(t, tcp.CloseWrite())

			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {},
				func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
			pool.SetMaxSubscriptionsPerConnection(1)

			_, err := pool.Subscribe("first")
			assert.Assert(t, isUncertainWSUpstreamOutcome(err))
			assert.Assert(t, client.IsClosed(), "ambiguous write failure must settle by closing its origin")

			secondClient, secondPeer := newWSCacheClient(t)
			setManagerClient(manager, secondClient)
			backend := newControlledWSBackend(manager, secondClient, secondPeer)
			secondCall := startSubscribe(pool, "second")
			request := <-backend.requests
			backend.responses <- protocol.subscribeSuccess(request, "second")
			second := <-secondCall
			assert.NilError(t, second.err, "closed write-failure socket must release its reservation")
		})
	}
}

func TestUncertainFinalUnsubscribeDrainsAfterOriginCloses(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			pool, manager, backend := newRealManagerPool(t, protocol, 1)
			client := currentManagerClient(manager)

			firstCall := startSubscribe(pool, "first")
			subscribeRequest := <-backend.requests
			subscribeResponse := protocol.subscribeSuccess(subscribeRequest, "first")
			if protocol.evm {
				subscribeResponse = WithResult(subscribeRequest, "0xfirst")
			}
			backend.responses <- subscribeResponse
			first := <-firstCall
			assert.NilError(t, first.err)

			unsubscribeCall := startUnsubscribe(pool, first.id)
			<-backend.requests
			_, err := pool.Subscribe("first")
			var pendingRemoval *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &pendingRemoval), "pending removal must not accept new membership")
			_, err = pool.Subscribe("second")
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &exhausted), "pending removal must remain charged")

			close(backend.responses)
			assert.NilError(t, client.Close())
			unsubscribeErr := <-unsubscribeCall
			assert.Assert(t, isUncertainWSUpstreamOutcome(unsubscribeErr))
			<-uncertainWSUpstreamOutcomeSettlement(unsubscribeErr)
			assert.Assert(t, !manager.HasSubscription("first"), "settled removal must not survive for reconnect")

			pool.subMux.Lock()
			_, stillPinned := pool.subscriptionConn[first.id]
			count := pool.subCount[manager].Load()
			pool.subMux.Unlock()
			assert.Assert(t, !stillPinned)
			assert.Equal(t, count, int64(0))

			secondClient, secondPeer := newWSCacheClient(t)
			setManagerClient(manager, secondClient)
			secondBackend := newControlledWSBackend(manager, secondClient, secondPeer)
			assert.NilError(t, resubmitManagerSubscriptions(manager, secondClient))
			select {
			case request := <-secondBackend.requests:
				t.Fatalf("settled removal was resubmitted: %+v", request)
			default:
			}

			secondCall := startSubscribe(pool, "second")
			secondRequest := <-secondBackend.requests
			secondBackend.responses <- protocol.subscribeSuccess(secondRequest, "second")
			second := <-secondCall
			assert.NilError(t, second.err)
		})
	}
}

func TestBrokerForgetsSettlingFinalUnsubscribe(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			upstreamClient, upstreamPeer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, upstreamClient)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, upstreamClient, upstreamPeer)
			broker := NewBroker([]string{"ws://upstream.test"}, "/", 1,
				func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
			broker.log = log.WithField("test", t.Name())
			downstream := &JsonRpcWsClient{}
			param := "first"
			subscribeMethod := methodSubscribeCosmos
			if protocol.evm {
				subscribeMethod = methodSubscribeEth
			}
			subscribeCall := make(chan *JsonRpcMsg, 1)
			go func() {
				response, _ := broker.HandleSubscription(downstream, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: 1, Method: subscribeMethod, Params: []any{param},
				})
				subscribeCall <- response
			}()
			subscribeRequest := <-backend.requests
			subscribeResponse := protocol.subscribeSuccess(subscribeRequest, "first")
			if protocol.evm {
				subscribeResponse = WithResult(subscribeRequest, "0xfirst")
			}
			backend.responses <- subscribeResponse
			assert.Assert(t, (<-subscribeCall).Error == nil)
			canonicalID, exists := broker.sm.GetSubscriptionID(param)
			assert.Assert(t, exists)

			unsubscribeMethod := methodUnsubscribeCosmos
			unsubscribeParam := param
			if protocol.evm {
				unsubscribeMethod = methodUnsubscribeEth
				unsubscribeParam = canonicalID
			}
			unsubscribeCall := make(chan *JsonRpcMsg, 1)
			go func() {
				response, _ := broker.HandleSubscription(downstream, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: 2, Method: unsubscribeMethod, Params: []any{unsubscribeParam},
				})
				unsubscribeCall <- response
			}()
			<-backend.requests
			close(backend.responses)
			assert.NilError(t, upstreamClient.Close())
			assert.Assert(t, (<-unsubscribeCall).Error != nil)
			_, exists = broker.sm.GetSubscriptionID(param)
			assert.Assert(t, !exists, "settling final unsubscribe must tombstone broker membership")
		})
	}
}

func currentManagerClient(manager UpstreamConnManager) *JsonRpcWsClient {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		return manager.curClient()
	case *UpstreamConnManagerEth:
		return manager.curClient()
	default:
		return nil
	}
}

func resubmitManagerSubscriptions(manager UpstreamConnManager, client *JsonRpcWsClient) error {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		return manager.reSubmitSubscriptionsOnClient(client)
	case *UpstreamConnManagerEth:
		return manager.reSubmitSubscriptionsOnClient(client)
	default:
		return nil
	}
}

func TestMigrationUncertainSubscribeOutcomeKeepsCapacityOccupied(t *testing.T) {
	dead := newLimitingUpstream()
	alive := newLimitingUpstream()
	dead.healthy.Store(false)
	alive.uncertainSubscribe.Store(true)
	pool := &UpstreamPool{
		conn:                    []UpstreamConnManager{dead, alive},
		subscriptionConn:        map[string]UpstreamConnManager{"old": dead},
		subscriptionID:          map[string]string{"victim": "old"},
		subscriptionParam:       map[string]string{"old": "victim"},
		subCount:                map[UpstreamConnManager]*atomic.Int64{dead: {}, alive: {}},
		maxSubscriptionsPerConn: 1,
	}
	pool.subCount[dead].Store(1)

	assert.Equal(t, len(pool.MigrateUnhealthy()), 0)
	assert.Equal(t, pool.subCount[alive].Load(), int64(1), "uncertain migration must retain its reservation")
	_, err := pool.Subscribe("second")
	var exhausted *WSResourceExhaustedError
	assert.Assert(t, errors.As(err, &exhausted))
	assert.Equal(t, alive.subscribeCalls.Load(), int32(1), "full survivor must not receive another subscribe")
}

func setManagerClient(manager UpstreamConnManager, client *JsonRpcWsClient) {
	managerLifecycle(manager).install(client)
}

func managerLifecycle(manager UpstreamConnManager) *wsSubscriptionLifecycle {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		return manager.subscriptionLifecycle()
	case *UpstreamConnManagerEth:
		return manager.subscriptionLifecycle()
	default:
		return nil
	}
}

func setManagerLog(manager UpstreamConnManager, entry *Entry) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.log = entry
	case *UpstreamConnManagerEth:
		manager.log = entry
	}
}

func TestWebSocketDashboardIncludesLimitsAndPreservesAggregateSemantics(t *testing.T) {
	limits := WebSocketLimits{32, 128, 4, 16}
	p := &JsonRpcWebSocketProxy{
		broker:    NewBroker([]string{"ws://backend.test"}, "/", 1, fakeConstructor),
		admission: newWSAdmissionController(limits),
		section:   "rpc.jsonrpc",
		path:      "/websocket",
		conns:     map[*JsonRpcWsClient]*wsConnInfo{},
	}
	snapshot := p.StatsSnapshot()
	assert.DeepEqual(t, snapshot.Limits, limits)
	assert.Assert(t, snapshot.LimitsAvailable)
	assert.Assert(t, snapshot.LimitsConsistent)

	body := []byte(`{"sections":[{"section":"rpc.jsonrpc","limits":{"max_subscriptions_per_client":32,"max_subscriptions_per_identity":128,"max_subscriptions_per_upstream_connection":4,"max_connections_per_ip":16}}]}`)
	out := aggregateWebSocket([]peerResponse{{Body: body}, {Body: body}})
	assert.Equal(t, len(out), 1)
	assert.DeepEqual(t, out[0].Limits, limits)
	assert.Assert(t, out[0].LimitsAvailable)
	assert.Assert(t, out[0].LimitsConsistent)
}
