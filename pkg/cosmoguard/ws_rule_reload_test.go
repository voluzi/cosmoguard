package cosmoguard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWSRuleReloadRevokesDeniedSubscriptions(t *testing.T) {
	upstream := newLimitingUpstream()
	proxy, err := NewJsonRpcWebSocketProxy(t.Name(), []string{"ws://upstream.test"}, "/websocket", 1,
		limitingConstructor(upstream), nil, false, nil)
	require.NoError(t, err)
	proxy.log = log.WithField("test", t.Name())
	proxy.broker.log = proxy.log
	proxy.SetRules(nil, RuleActionAllow, nil)

	cosmos, cosmosPeer := newWSCacheClient(t)
	evm, _ := newWSCacheClient(t)
	kept, _ := newWSCacheClient(t)
	subscribe := func(client *JsonRpcWsClient, id int, method string, params any) {
		t.Helper()
		res, err := proxy.broker.HandleSubscription(client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: id, Method: method, Params: params,
		})
		require.NoError(t, err)
		require.Nil(t, res.Error)
	}
	subscribe(cosmos, 11, methodSubscribeCosmos, map[string]any{"query": "tm.event='Tx'"})
	subscribe(evm, 12, methodSubscribeEth, []any{"logs", map[string]any{"address": "0x1"}})
	subscribe(kept, 13, methodSubscribeCosmos, []any{"tm.event='NewBlock'"})

	denyTx := &JsonRpcRule{Action: RuleActionDeny, Methods: []string{methodSubscribeCosmos}, Params: map[string]any{"query": "tm.event='Tx'"}}
	denyLogs := &JsonRpcRule{Action: RuleActionDeny, Methods: []string{methodSubscribeEth}}
	require.NoError(t, denyTx.Compile())
	require.NoError(t, denyLogs.Compile())
	proxy.SetRules([]*JsonRpcRule{denyTx, denyLogs}, RuleActionAllow, nil)

	require.NoError(t, cosmosPeer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var notice JsonRpcMsg
	require.NoError(t, cosmosPeer.ReadJSON(&notice))
	require.EqualValues(t, 11, notice.ID)
	require.NotNil(t, notice.Error)
	require.Equal(t, -32000, notice.Error.Code)

	require.Eventually(t, evm.IsClosed, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return proxy.broker.ClientSubCount(cosmos) == 0 && upstream.unsubscribeCalls.Load() == 2
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, proxy.broker.ClientSubCount(kept))
	require.False(t, kept.IsClosed())
}

func TestBrokerForgetsSubscribeRequestsWithMembership(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	client := NewJsonRpcWsClient(nil)
	for i, query := range []string{"a", "b", "c"} {
		_, err := broker.HandleSubscription(client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: i, Method: methodSubscribeCosmos, Params: []any{query},
		})
		require.NoError(t, err)
	}
	requests := func() int {
		broker.requestsMu.Lock()
		defer broker.requestsMu.Unlock()
		return len(broker.requests)
	}
	require.Equal(t, 3, requests())

	_, err := broker.HandleSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 9, Method: methodUnsubscribeCosmos, Params: []any{"a"},
	})
	require.NoError(t, err)
	require.Equal(t, 2, requests())
	require.NoError(t, broker.removeAllSubscriptions(client))
	require.Equal(t, 0, requests())
}
