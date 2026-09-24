package cosmoguard

import (
	"testing"

	"gotest.tools/assert"
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

			// A later client asking for the same query gets the same answer.
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
