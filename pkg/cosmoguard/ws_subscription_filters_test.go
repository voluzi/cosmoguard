package cosmoguard

import (
	"testing"

	"gotest.tools/assert"
)

func TestEthSubscribeForwardsFiltersAndKeysOnCompleteParams(t *testing.T) {
	protocol := wsProtocolCases()[1]
	broker, manager, backend := newRealManagerBroker(t, protocol, WebSocketLimits{})
	first, _ := newWSCacheClient(t)
	second, _ := newWSCacheClient(t)
	third, _ := newWSCacheClient(t)

	filterA := []any{"logs", map[string]any{
		"address": "0x1111111111111111111111111111111111111111",
		"topics":  []any{"0xaa"},
	}}
	filterB := []any{"logs", map[string]any{"address": "0x2222222222222222222222222222222222222222"}}
	subscribe := func(client *JsonRpcWsClient, id int, params []any) <-chan brokerCallResult {
		return startBrokerCall(broker, client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: id, Method: methodSubscribeEth, Params: params,
		}, "alice")
	}

	call := subscribe(first, 1, filterA)
	request := mustRecv(t, backend.requests, "first upstream subscribe")
	assert.DeepEqual(t, request.Params, filterA)
	backend.responses <- protocol.subscribeSuccess(request, "a")
	resultA := mustRecv(t, call, "first subscribe")
	assert.NilError(t, resultA.err)

	call = subscribe(second, 2, filterB)
	request = mustRecv(t, backend.requests, "second upstream subscribe")
	assert.DeepEqual(t, request.Params, filterB)
	backend.responses <- protocol.subscribeSuccess(request, "b")
	resultB := mustRecv(t, call, "second subscribe")
	assert.NilError(t, resultB.err)
	assert.Assert(t, string(resultA.response.Result) != string(resultB.response.Result),
		"distinct filters must not share a subscription")

	// Equal to the first filter; the key encoder sorts object keys, so
	// it does not depend on how the client ordered them.
	sameAsA := []any{"logs", map[string]any{
		"topics":  []any{"0xaa"},
		"address": "0x1111111111111111111111111111111111111111",
	}}
	resultC := mustRecv(t, subscribe(third, 3, sameAsA), "equivalent subscribe")
	assert.NilError(t, resultC.err)
	assert.Equal(t, string(resultC.response.Result), string(resultA.response.Result))

	// A reconnect resubmits the complete params.
	replacement, replacementPeer := newWSCacheClient(t)
	setManagerClient(manager, replacement)
	replacementBackend := newControlledWSBackend(manager, replacement, replacementPeer)
	resubmitted := make(chan error, 1)
	go func() { resubmitted <- resubmitManagerSubscriptions(manager, replacement) }()
	seen := map[string]bool{}
	for range 2 {
		request := mustRecv(t, replacementBackend.requests, "resubmitted subscribe")
		key, err := getSubscriptionParam(&JsonRpcMsg{Method: methodSubscribeEth, Params: request.Params})
		assert.NilError(t, err)
		seen[key] = true
		replacementBackend.responses <- protocol.subscribeSuccess(request, key)
	}
	assert.NilError(t, mustRecv(t, resubmitted, "resubmit"))
	for _, params := range [][]any{filterA, filterB} {
		key, err := getSubscriptionParam(&JsonRpcMsg{Method: methodSubscribeEth, Params: params})
		assert.NilError(t, err)
		assert.Assert(t, seen[key], "resubmit lost filter %s", key)
	}
}

func TestEthSubscribeWithoutOptionsKeepsNameKey(t *testing.T) {
	key, err := getSubscriptionParam(&JsonRpcMsg{Method: methodSubscribeEth, Params: []any{"newHeads"}})
	assert.NilError(t, err)
	assert.Equal(t, key, "newHeads")
	assert.DeepEqual(t, ethSubscribeParams(key), []any{"newHeads"})
}

func TestEthSubscribeNameCannotAliasEncodedParams(t *testing.T) {
	filtered := []any{"logs", map[string]any{"address": "0x1"}}
	filteredKey, err := getSubscriptionParam(&JsonRpcMsg{Method: methodSubscribeEth, Params: filtered})
	assert.NilError(t, err)
	nameKey, err := getSubscriptionParam(&JsonRpcMsg{Method: methodSubscribeEth, Params: []any{filteredKey}})
	assert.NilError(t, err)
	assert.Assert(t, nameKey != filteredKey)
	assert.DeepEqual(t, ethSubscribeParams(nameKey), []any{filteredKey})
}
