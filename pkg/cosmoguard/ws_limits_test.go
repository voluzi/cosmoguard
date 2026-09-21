package cosmoguard

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

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
	once               sync.Once
}

type wsProtocolCase struct {
	name        string
	constructor UpstreamConnManagerConstructor
	evm         bool
}

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
			dispatchManagerMessage(manager, msg)
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
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.onUpstreamMessage(msg)
	case *UpstreamConnManagerEth:
		manager.onUpstreamMessage(msg)
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
	broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{MaxSubscriptionsPerClient: 1}))
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

func TestProviderRejectedDuplicateCleanupKeepsCapacityOccupied(t *testing.T) {
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

			firstCall := startSubscribe(pool, "same")
			firstReq := <-backendA.requests
			secondCall := startSubscribe(pool, "same")
			secondReq := <-backendB.requests
			backendB.responses <- protocol.subscribeSuccess(secondReq, "winner")
			second := <-secondCall
			assert.NilError(t, second.err)

			backendA.responses <- protocol.subscribeSuccess(firstReq, "loser")
			cleanupReq := <-backendA.requests
			backendA.responses <- providerRejectedResponse(cleanupReq)
			first := <-firstCall
			assert.NilError(t, first.err)
			assert.Equal(t, first.id, second.id)
			assert.Assert(t, managerA.HasSubscription("same"), "rejected cleanup may leave the duplicate upstream subscription live")

			third := <-startSubscribe(pool, "other")
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(third.err, &exhausted), "both cap-one connections must remain occupied")
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
			assert.Assert(t, manager.HasSubscription("victim"), "rejected cleanup may leave the replacement upstream subscription live")

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

func seedManagerSubscription(manager UpstreamConnManager, param, id string) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.subByParam[param] = id
		manager.subByID[id] = param
	case *UpstreamConnManagerEth:
		manager.subByParam[param] = id
		manager.subByID[id] = param
	}
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

func TestUpstreamUncertainSubscribeOutcomeKeepsCapacityOccupied(t *testing.T) {
	tests := []struct {
		name        string
		constructor UpstreamConnManagerConstructor
	}{
		{name: "cosmos", constructor: CosmosUpstreamConnManager},
		{name: "evm", constructor: EthUpstreamConnManager},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			firstClient, firstBackend := newWSCacheClient(t)
			manager := tt.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, firstClient)
			setManagerLog(manager, log.WithField("test", tt.name+"-uncertain"))
			pool := NewUpstreamPool([]string{"ws://upstream.test"}, "/", 1, func(*JsonRpcMsg) {},
				func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
			pool.SetMaxSubscriptionsPerConnection(1)

			firstReceived := make(chan struct{})
			go func() {
				if _, _, err := firstBackend.ReadMessage(); err == nil {
					close(firstReceived)
				}
				_ = firstBackend.Close()
			}()
			go func() { _, _ = firstClient.ReceiveMsg() }()

			_, err := pool.Subscribe("first")
			<-firstReceived
			assert.Assert(t, errors.Is(err, ErrClosed), "the original cause must remain discoverable")

			secondClient, secondBackend := newWSCacheClient(t)
			setManagerClient(manager, secondClient)
			unexpectedSecondCall := make(chan struct{}, 1)
			go func() {
				if _, _, err := secondBackend.ReadMessage(); err == nil {
					unexpectedSecondCall <- struct{}{}
				}
				_ = secondBackend.Close()
			}()
			go func() { _, _ = secondClient.ReceiveMsg() }()

			_, err = pool.Subscribe("second")
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(err, &exhausted), "uncertain first outcome must retain the reserved slot")
			assert.Equal(t, exhausted.Scope, wsLimitScopeUpstreamConnection)
			select {
			case <-unexpectedSecondCall:
				t.Fatal("capacity bypass sent a second upstream subscribe")
			default:
			}
		})
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
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.clientMu.Lock()
		manager.client = client
		manager.clientMu.Unlock()
	case *UpstreamConnManagerEth:
		manager.clientMu.Lock()
		manager.client = client
		manager.clientMu.Unlock()
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

	body := []byte(`{"sections":[{"section":"rpc.jsonrpc","limits":{"max_subscriptions_per_client":32,"max_subscriptions_per_identity":128,"max_subscriptions_per_upstream_connection":4,"max_connections_per_ip":16}}]}`)
	out := aggregateWebSocket([]peerResponse{{Body: body}, {Body: body}})
	assert.Equal(t, len(out), 1)
	assert.DeepEqual(t, out[0].Limits, limits)
}
