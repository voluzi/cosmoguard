package cosmoguard

import (
	"strings"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/v5/pkg/util"
	"gotest.tools/assert"
)

func TestEthRunReconnectReplaysEverySubscriptionAndScopesResponses(t *testing.T) {
	backend := newLifecycleRunBackend(t)
	replayed := make(chan struct{}, 2)
	notifications := make(chan *JsonRpcMsg, 2)
	manager := EthUpstreamConnManager(backend.wsURL(t), &util.UniqueID{}, func(msg *JsonRpcMsg) {
		notifications <- msg
	}).(*UpstreamConnManagerEth)
	manager.afterResubmit = func() { replayed <- struct{}{} }
	done := make(chan error, 1)
	go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
	t.Cleanup(func() {
		manager.Stop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("manager Run did not stop")
		}
	})

	connA := backend.nextConn(t)
	mustWait(t, replayed, "initial replay pass")
	logicalByParam := make(map[string]string)
	for _, param := range []string{"newHeads", "logs"} {
		created := startSubscribe(manager, param)
		request := connA.nextRequest(t)
		connA.respond(t, WithResult(request, "wire-a-"+param))
		result := mustRecv(t, created, param+" subscribe result")
		assert.NilError(t, result.err)
		logicalByParam[param] = result.id
	}
	oldClient := manager.curClient()

	assert.NilError(t, connA.close())
	connB := backend.nextConn(t)
	wireByParam := make(map[string]string)
	for range 2 {
		request := connB.nextRequest(t)
		params := request.Params.([]any)
		param := params[0].(string)
		wireByParam[param] = "wire-b-" + param
		connB.respond(t, WithResult(request, wireByParam[param]))
	}
	mustWait(t, replayed, "reconnect replay pass")
	assert.Assert(t, manager.HasSubscription("newHeads"))
	assert.Assert(t, manager.HasSubscription("logs"))

	rpcResult := make(chan *JsonRpcMsg, 1)
	rpcErr := make(chan error, 1)
	go func() {
		response, err := manager.MakeRequest(&JsonRpcMsg{Version: jsonRpcVersion, Method: "status"})
		rpcResult <- response
		rpcErr <- err
	}()
	rpcRequest := connB.nextRequest(t)
	manager.onUpstreamMessage(oldClient, WithResult(rpcRequest, map[string]any{"socket": "old"}))
	assert.Equal(t, managerResponseCount(manager), 1, "old socket response must not consume the replacement waiter")
	connB.respond(t, WithResult(rpcRequest, map[string]any{"socket": "current"}))
	assert.NilError(t, mustRecv(t, rpcErr, "ordinary RPC error"))
	response := mustRecv(t, rpcResult, "ordinary RPC response")
	assert.Assert(t, response != nil)
	assert.Assert(t, strings.Contains(string(response.Result), "current"))

	for _, param := range []string{"newHeads", "logs"} {
		connB.respond(t, &JsonRpcMsg{Version: jsonRpcVersion, Params: map[string]any{
			"subscription": wireByParam[param], "result": param,
		}})
	}
	routed := map[any]bool{}
	for range 2 {
		routed[mustRecv(t, notifications, "replayed subscription notification").ID] = true
	}
	assert.Assert(t, routed[logicalByParam["newHeads"]])
	assert.Assert(t, routed[logicalByParam["logs"]])
}
