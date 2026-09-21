package cosmoguard

import (
	"errors"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/pkg/util"
	"gotest.tools/assert"
)

type brokerCallResult struct {
	response *JsonRpcMsg
	err      error
}

func newRealManagerBroker(t *testing.T, protocol wsProtocolCase, limits WebSocketLimits) (*Broker, UpstreamConnManager, *controlledWSBackend) {
	t.Helper()
	upstreamClient, upstreamPeer := newWSCacheClient(t)
	manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
	setManagerClient(manager, upstreamClient)
	setManagerLog(manager, log.WithField("test", t.Name()))
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1,
		func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return manager })
	broker.log = log.WithField("test", t.Name())
	broker.setAdmissionController(newWSAdmissionController(limits))
	return broker, manager, newControlledWSBackend(manager, upstreamClient, upstreamPeer)
}

func startBrokerCall(broker *Broker, client *JsonRpcWsClient, msg *JsonRpcMsg, identity string) <-chan brokerCallResult {
	result := make(chan brokerCallResult, 1)
	go func() {
		response, err := broker.HandleSubscription(client, msg, identity)
		result <- brokerCallResult{response: response, err: err}
	}()
	return result
}

func subscribeBrokerClient(t *testing.T, protocol wsProtocolCase, broker *Broker, backend *controlledWSBackend, client *JsonRpcWsClient, param string, requestID any) string {
	t.Helper()
	method := methodSubscribeCosmos
	if protocol.evm {
		method = methodSubscribeEth
	}
	call := startBrokerCall(broker, client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: requestID, Method: method, Params: []any{param},
	}, "alice")
	request := mustRecv(t, backend.requests, "upstream subscribe")
	backend.responses <- protocol.subscribeSuccess(request, "initial")
	result := mustRecv(t, call, "downstream subscribe")
	assert.NilError(t, result.err)
	assert.Assert(t, result.response.Error == nil)
	id, ok := broker.sm.GetSubscriptionID(param)
	assert.Assert(t, ok)
	return id
}

func brokerUnsubscribeMessage(protocol wsProtocolCase, param, id string, requestID any) *JsonRpcMsg {
	method := methodUnsubscribeCosmos
	unsubscribeParam := param
	if protocol.evm {
		method = methodUnsubscribeEth
		unsubscribeParam = id
	}
	return &JsonRpcMsg{
		Version: jsonRpcVersion, ID: requestID, Method: method, Params: []any{unsubscribeParam},
	}
}

func unsubscribeSuccess(protocol wsProtocolCase, request *JsonRpcMsg) *JsonRpcMsg {
	if protocol.evm {
		return WithResult(request, true)
	}
	return WithResult(request, map[string]any{})
}

func TestBrokerDefiniteUnsubscribeFailureRestoresLiveMembership(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			broker, _, backend := newRealManagerBroker(t, protocol, WebSocketLimits{
				MaxSubscriptionsPerClient:   1,
				MaxSubscriptionsPerIdentity: 1,
			})
			downstream := NewJsonRpcWsClient(nil)
			param := "victim"
			canonicalID := subscribeBrokerClient(t, protocol, broker, backend, downstream, param, "subscribe-id")
			originalNotificationID := broker.sm.GetSubscriptionClients(canonicalID)[downstream]

			first := startBrokerCall(broker, downstream,
				brokerUnsubscribeMessage(protocol, param, canonicalID, "first-unsubscribe"), "alice")
			firstRequest := mustRecv(t, backend.requests, "first upstream unsubscribe")
			backend.responses <- providerRejectedResponse(firstRequest)
			firstResult := mustRecv(t, first, "first downstream unsubscribe")
			assert.NilError(t, firstResult.err)
			assert.Assert(t, firstResult.response.Error != nil)
			assert.Assert(t, broker.sm.ClientSubscribed(canonicalID, downstream),
				"definite upstream rejection must restore the live downstream owner")
			assert.DeepEqual(t, broker.sm.GetSubscriptionClients(canonicalID)[downstream], originalNotificationID)
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(broker.admission.reserveSubscription(downstream, "alice"), &exhausted),
				"restored membership must retain its admission reservation")

			retry := startBrokerCall(broker, downstream,
				brokerUnsubscribeMessage(protocol, param, canonicalID, "retry-unsubscribe"), "alice")
			retryRequest := mustRecv(t, backend.requests, "retry upstream unsubscribe")
			backend.responses <- unsubscribeSuccess(protocol, retryRequest)
			retryResult := mustRecv(t, retry, "retry downstream unsubscribe")
			assert.NilError(t, retryResult.err)
			assert.Assert(t, retryResult.response.Error == nil)
			assert.Assert(t, !broker.sm.ClientSubscribed(canonicalID, downstream))
			assert.NilError(t, broker.admission.reserveSubscription(downstream, "alice"),
				"successful retry must release admission exactly once")
			broker.admission.releaseSubscription(downstream)
		})
	}
}

func TestBrokerDisconnectDefiniteUnsubscribeFailureRetiresOrphan(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			broker, manager, backend := newRealManagerBroker(t, protocol, WebSocketLimits{
				MaxSubscriptionsPerIdentity:           1,
				MaxSubscriptionsPerUpstreamConnection: 1,
			})
			origin := currentManagerClient(manager)
			downstream := NewJsonRpcWsClient(nil)
			param := "victim"
			canonicalID := subscribeBrokerClient(t, protocol, broker, backend, downstream, param, "subscribe-id")

			disconnected := make(chan error, 1)
			go func() { disconnected <- broker.removeAllSubscriptions(downstream) }()
			firstRequest := mustRecv(t, backend.requests, "disconnect upstream unsubscribe")
			backend.responses <- providerRejectedResponse(firstRequest)
			retireRequest := mustRecv(t, backend.requests, "orphan retirement unsubscribe")
			backend.responses <- providerRejectedResponse(retireRequest)
			assert.ErrorContains(t, mustRecv(t, disconnected, "disconnect cleanup"), "provider rejected request")
			_, exists := broker.sm.GetSubscriptionID(param)
			assert.Assert(t, !exists, "disconnected membership must become a cleanup tombstone")
			assert.Assert(t, !manager.HasSubscription(param), "cleanup tombstone must not replay")

			replacementDownstream := NewJsonRpcWsClient(nil)
			assert.NilError(t, broker.admission.reserveSubscription(replacementDownstream, "alice"),
				"disconnected admission must remain released")
			broker.admission.releaseSubscription(replacementDownstream)
			blocked := startBrokerCall(broker, replacementDownstream, &JsonRpcMsg{
				Version: jsonRpcVersion, ID: "blocked", Method: map[bool]string{false: methodSubscribeCosmos, true: methodSubscribeEth}[protocol.evm], Params: []any{param},
			}, "alice")
			blockedResult := mustRecv(t, blocked, "subscription blocked by cleanup tombstone")
			assert.Assert(t, blockedResult.response.Error != nil)
			assert.Equal(t, blockedResult.response.Error.Code, -32005)

			assert.NilError(t, origin.Close())
			deadline := time.Now().Add(2 * time.Second)
			for {
				broker.pool.subMux.Lock()
				_, draining := broker.pool.drainingParams[param]
				count := broker.pool.subCount[manager].Load()
				broker.pool.subMux.Unlock()
				if !draining && count == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("orphan retirement did not settle: draining=%v count=%d", draining, count)
				}
				runtime.Gosched()
			}

			replacementUpstream, peer := newWSCacheClient(t)
			setManagerClient(manager, replacementUpstream)
			replacementBackend := newControlledWSBackend(manager, replacementUpstream, peer)
			newID := subscribeBrokerClient(t, protocol, broker, replacementBackend, replacementDownstream, param, "replacement")
			assert.Assert(t, newID != canonicalID)
		})
	}
}

func TestCosmosSubscribeResultShape(t *testing.T) {
	settled := make(chan struct{})
	cases := []struct {
		name   string
		result []byte
		valid  bool
	}{
		{name: "missing"},
		{name: "null", result: []byte("null")},
		{name: "object", result: []byte(`{}`), valid: true},
		{name: "false", result: []byte(`false`)},
		{name: "string", result: []byte(`"subscription"`)},
		{name: "array", result: []byte(`[]`)},
		{name: "malformed", result: []byte(`{`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCosmosSubscribeResponse(&JsonRpcMsg{
				Version: jsonRpcVersion, ID: "request", Result: tc.result,
			}, settled)
			if tc.valid {
				assert.NilError(t, err)
				return
			}
			assert.Assert(t, isUncertainWSUpstreamOutcome(err))
			assert.Equal(t, uncertainWSUpstreamOutcomeSettlement(err), (<-chan struct{})(settled))
		})
	}
}

func TestInvalidCosmosSubscribeResultRetainsCapacityUntilOriginCloses(t *testing.T) {
	cases := []struct {
		name   string
		result any
	}{
		{name: "false", result: false},
		{name: "string", result: "subscription"},
		{name: "array", result: []any{}},
	}
	protocol := wsProtocolCase{name: "cosmos", constructor: CosmosUpstreamConnManager}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, manager, backend := newRealManagerPool(t, protocol, 1)
			origin := currentManagerClient(manager)
			first := startSubscribe(pool, "first")
			request := mustRecv(t, backend.requests, "invalid Cosmos subscribe")
			backend.responses <- WithResult(request, tc.result)
			firstResult := mustRecv(t, first, "invalid Cosmos subscribe result")
			assert.Assert(t, isUncertainWSUpstreamOutcome(firstResult.err))

			second := startSubscribe(pool, "second")
			secondResult := mustRecv(t, second, "capacity rejection")
			var exhausted *WSResourceExhaustedError
			assert.Assert(t, errors.As(secondResult.err, &exhausted))

			settled := uncertainWSUpstreamOutcomeSettlement(firstResult.err)
			assert.Assert(t, settled != nil)
			assert.NilError(t, origin.Close())
			mustWait(t, settled, "invalid acknowledgement settlement")
			pool.subMux.Lock()
			count := pool.subCount[manager].Load()
			pool.subMux.Unlock()
			assert.Equal(t, count, int64(0))
		})
	}
}
