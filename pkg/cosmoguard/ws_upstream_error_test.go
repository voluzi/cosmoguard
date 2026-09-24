package cosmoguard

import (
	"errors"
	"net/url"
	"sync/atomic"
	"testing"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// upstreamRejection is a JSON-RPC error as a node sends it, with the
// reason in data.
func upstreamRejection(req *JsonRpcMsg) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: jsonRpcVersion,
		ID:      req.ID,
		Error:   &JsonRpcError{Code: -32602, Message: "Invalid params", Data: "failed to parse query: offset 23: EOF"},
	}
}

// assertRelayedRejection checks the client got the upstream's own error,
// field for field, under the client's own id.
func assertRelayedRejection(t *testing.T, result brokerCallResult, clientID any) {
	t.Helper()
	assert.NilError(t, result.err)
	assert.Assert(t, result.response != nil && result.response.Error != nil)
	got, err := json.Marshal(result.response.Error)
	assert.NilError(t, err)
	assert.Equal(t, string(got), `{"code":-32602,"message":"Invalid params","data":"failed to parse query: offset 23: EOF"}`)
	assert.DeepEqual(t, result.response.ID, clientID)
}

func TestSubscribeRelaysUpstreamRejection(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			broker, _, backend := newRealManagerBroker(t, protocol, WebSocketLimits{})
			method := methodSubscribeCosmos
			if protocol.evm {
				method = methodSubscribeEth
			}
			subscribe := func(clientID any) <-chan brokerCallResult {
				client, _ := newWSCacheClient(t)
				return startBrokerCall(broker, client, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: clientID, Method: method, Params: []any{"tm.event='NewBlock' AND"},
				}, "alice")
			}

			call := subscribe(7)
			request := mustRecv(t, backend.requests, "upstream subscribe")
			backend.responses <- upstreamRejection(request)
			assertRelayedRejection(t, mustRecv(t, call, "downstream subscribe"), 7)

			// A later client asking for the same query makes its own
			// upstream call and gets the same answer.
			call = subscribe("second")
			request = mustRecv(t, backend.requests, "second upstream subscribe")
			backend.responses <- upstreamRejection(request)
			assertRelayedRejection(t, mustRecv(t, call, "second downstream subscribe"), "second")
		})
	}
}

func TestUnsubscribeRelaysUpstreamRejection(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			broker, _, backend := newRealManagerBroker(t, protocol, WebSocketLimits{})
			client, _ := newWSCacheClient(t)
			const param = "tm.event='NewBlock'"
			id := subscribeBrokerClient(t, protocol, broker, backend, client, param, 1)

			call := startBrokerCall(broker, client, brokerUnsubscribeMessage(protocol, param, id, 9), "alice")
			request := mustRecv(t, backend.requests, "upstream unsubscribe")
			backend.responses <- upstreamRejection(request)
			assertRelayedRejection(t, mustRecv(t, call, "downstream unsubscribe"), 9)
		})
	}
}

func TestSubscriptionErrorResponseKeepsOwnFailures(t *testing.T) {
	msg := &JsonRpcMsg{Version: jsonRpcVersion, ID: 3, Method: methodSubscribeCosmos}
	resp := subscriptionErrorResponse(msg, ErrClosed)
	assert.Equal(t, resp.Error.Code, -100, "cosmoguard's own failures keep -100")
	assert.Equal(t, resp.Error.Message, ErrClosed.Error())
}

// TestCoalescedSubscribeSharesUpstreamRejection parks a second caller on
// the first caller's in-flight create, then rejects it upstream: both get
// the upstream's error, and each client answer is its own copy.
func TestCoalescedSubscribeSharesUpstreamRejection(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, &util.UniqueID{}, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			backend := newControlledWSBackend(manager, client, peer)
			pool := &UpstreamPool{
				conn:                    []UpstreamConnManager{manager},
				subscriptionConn:        make(map[string]UpstreamConnManager),
				subscriptionID:          make(map[string]string),
				subscriptionParam:       make(map[string]string),
				subCount:                map[UpstreamConnManager]*atomic.Int64{manager: {}},
				maxSubscriptionsPerConn: 10,
			}
			joined := make(chan struct{})
			pool.afterJoinPendingCreate = func() { close(joined) }

			firstCall := startSubscribe(pool, "same")
			request := mustRecv(t, backend.requests, "upstream subscribe")
			secondCall := startSubscribe(pool, "same")
			<-joined
			backend.responses <- upstreamRejection(request)

			method := methodSubscribeCosmos
			if protocol.evm {
				method = methodSubscribeEth
			}
			var answers []*JsonRpcMsg
			for i, call := range []<-chan subscribeCallResult{firstCall, secondCall} {
				result := mustRecv(t, call, "subscribe")
				var upstream *upstreamRPCError
				assert.Assert(t, errors.As(result.err, &upstream), "caller %d: %v", i, result.err)
				answers = append(answers, subscriptionErrorResponse(&JsonRpcMsg{Version: jsonRpcVersion, ID: i, Method: method}, result.err))
			}
			for i, answer := range answers {
				assertRelayedRejection(t, brokerCallResult{response: answer}, i)
			}
			assert.Assert(t, answers[0].Error != answers[1].Error, "each client gets its own error object")
		})
	}
}

func TestSubscriptionErrorResponseMatchesOperation(t *testing.T) {
	rejected := &upstreamRPCError{method: methodUnsubscribeCosmos, rpc: &JsonRpcError{Code: -32603, Message: "unsubscribe failed"}}
	// A subscribe that failed alongside an unsubscribe rejection does not
	// answer with the unsubscribe's error.
	subscribe := &JsonRpcMsg{Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos}
	resp := subscriptionErrorResponse(subscribe, errors.Join(ErrClosed, rejected))
	assert.Equal(t, resp.Error.Code, -100)
	unsubscribe := &JsonRpcMsg{Version: jsonRpcVersion, ID: 2, Method: methodUnsubscribeEth}
	resp = subscriptionErrorResponse(unsubscribe, rejected)
	assert.Equal(t, resp.Error.Code, -32603, "unsubscribe and eth_unsubscribe are the same operation")
}
