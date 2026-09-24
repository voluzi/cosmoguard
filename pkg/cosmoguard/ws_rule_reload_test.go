package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
		broker.membershipMu.Lock()
		defer broker.membershipMu.Unlock()
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

func TestWSRuleReloadDuringSubscribeRevokesIt(t *testing.T) {
	upstream := newLimitingUpstream()
	proxy, err := NewJsonRpcWebSocketProxy(t.Name(), []string{"ws://upstream.test"}, "/websocket", 1,
		limitingConstructor(upstream), nil, false, nil)
	require.NoError(t, err)
	proxy.log = log.WithField("test", t.Name())
	proxy.broker.log = proxy.log
	proxy.cgDashboard = newDashboardObservability()
	proxy.SetRules(nil, RuleActionAllow, nil)
	client, peer := newWSCacheClient(t)

	upstream.subscribeStarted = make(chan struct{})
	upstream.subscribeRelease = make(chan struct{})
	handled := make(chan error, 1)
	go func() {
		handled <- proxy.handleRequest(client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: 5, Method: methodSubscribeCosmos, Params: []any{"tm.event='Tx'"},
		}, "192.0.2.1", nil)
	}()
	<-upstream.subscribeStarted
	proxy.SetRules(nil, RuleActionDeny, nil)
	// Let the reload's own scan finish while the membership does not exist.
	time.Sleep(50 * time.Millisecond)
	close(upstream.subscribeRelease)
	require.NoError(t, <-handled)

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var ack, notice JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&ack))
	require.Nil(t, ack.Error)
	require.NoError(t, peer.ReadJSON(&notice))
	require.EqualValues(t, 5, notice.ID)
	require.NotNil(t, notice.Error)
	require.Eventually(t, func() bool { return proxy.broker.ClientSubCount(client) == 0 }, 2*time.Second, 5*time.Millisecond)
}

func TestBrokerRevokeRechecksCurrentRules(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	client := NewJsonRpcWsClient(nil)
	id, err := broker.addSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)

	// Denied when scanned, allowed again by the time revoke holds the lock.
	broker.revoke(client, id, func(*JsonRpcWsClient, *JsonRpcMsg) func() { return nil })
	require.Equal(t, 1, broker.ClientSubCount(client))
	require.Zero(t, upstream.unsubscribeCalls.Load())
}

func newReloadTestProxy(t *testing.T, upstream *limitingUpstream) *JsonRpcWebSocketProxy {
	t.Helper()
	proxy, err := NewJsonRpcWebSocketProxy(t.Name(), []string{"ws://upstream.test"}, "/websocket", 1,
		limitingConstructor(upstream), nil, false, nil)
	require.NoError(t, err)
	proxy.log = log.WithField("test", t.Name())
	proxy.broker.log = proxy.log
	proxy.cgDashboard = newDashboardObservability()
	proxy.section = "rpc.jsonrpc"
	proxy.SetRules(nil, RuleActionAllow, nil)
	return proxy
}

// A subscribe queued behind another subscribe of the same param joins after
// the reload's scan; the recheck after it covers that, id or no id.
func TestWSRuleReloadDuringQueuedSubscribeRevokesIt(t *testing.T) {
	allow := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{methodSubscribeCosmos}}
	require.NoError(t, allow.Compile())
	for _, tc := range []struct {
		id    any
		rules []*JsonRpcRule
	}{{nil, nil}, {7, nil}, {nil, []*JsonRpcRule{allow}}, {7, []*JsonRpcRule{allow}}} {
		id := tc.id
		upstream := newLimitingUpstream()
		proxy := newReloadTestProxy(t, upstream)
		if tc.rules != nil {
			proxy.SetRules(tc.rules, RuleActionDeny, nil)
		}
		first, _ := newWSCacheClient(t)
		queued, _ := newWSCacheClient(t)
		subscribe := func(client *JsonRpcWsClient, id any) <-chan error {
			done := make(chan error, 1)
			go func() {
				done <- proxy.handleRequest(client, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: id, Method: methodSubscribeCosmos, Params: []any{"q"},
				}, "192.0.2.1", nil)
			}()
			return done
		}

		upstream.subscribeStarted = make(chan struct{})
		upstream.subscribeRelease = make(chan struct{})
		// Straight to the broker, so only the queued request rechecks.
		firstDone := make(chan error, 1)
		go func() {
			_, err := proxy.broker.HandleSubscription(first, &JsonRpcMsg{
				Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"},
			})
			firstDone <- err
		}()
		<-upstream.subscribeStarted
		queuedDone := subscribe(queued, id)
		time.Sleep(50 * time.Millisecond)
		proxy.SetRules(nil, RuleActionDeny, nil)
		time.Sleep(50 * time.Millisecond)
		close(upstream.subscribeRelease)
		require.NoError(t, <-firstDone)
		require.NoError(t, <-queuedDone)
		require.Eventually(t, func() bool {
			return proxy.broker.ClientSubCount(first) == 0 && proxy.broker.ClientSubCount(queued) == 0
		}, 2*time.Second, 5*time.Millisecond, "id %v", id)
	}
}

func TestWSRuleReloadDuringFailedUnsubscribeRevokesIt(t *testing.T) {
	upstream := newLimitingUpstream()
	proxy := newReloadTestProxy(t, upstream)
	client, peer := newWSCacheClient(t)
	_, err := proxy.broker.HandleSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 4, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)

	upstream.failUnsubscribe.Store(true)
	upstream.unsubscribeStarted = make(chan struct{}, 2)
	upstream.unsubscribeRelease = make(chan struct{})
	unsubscribed := startBrokerCall(proxy.broker, client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 5, Method: methodUnsubscribeCosmos, Params: []any{"q"},
	}, "")
	<-upstream.unsubscribeStarted
	proxy.SetRules(nil, RuleActionDeny, nil)
	time.Sleep(50 * time.Millisecond)
	close(upstream.unsubscribeRelease)
	result := mustRecv(t, unsubscribed, "failed unsubscribe")
	require.NoError(t, result.err)
	require.NotNil(t, result.response.Error)

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var notice JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&notice))
	require.EqualValues(t, 4, notice.ID)
	require.NotNil(t, notice.Error)
	require.Equal(t, 0, proxy.broker.ClientSubCount(client))
}

func TestWSRuleReloadRevocationIsRecordedAsDenial(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	client, _ := newWSCacheClient(t)
	proxy.registerConn(client, "198.51.100.7", nil, time.Now())
	_, err := proxy.broker.HandleSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)

	deny := &JsonRpcRule{Action: RuleActionDeny, Tag: "no-subs", Methods: []string{methodSubscribeCosmos}}
	require.NoError(t, deny.Compile())
	proxy.SetRules([]*JsonRpcRule{deny}, RuleActionAllow, nil)
	require.Eventually(t, func() bool { return len(proxy.cgDashboard.denied.Snapshot()) == 1 }, 2*time.Second, 5*time.Millisecond)
	record := proxy.cgDashboard.denied.Snapshot()[0]
	require.Equal(t, "rule", record.Reason)
	require.Equal(t, "no-subs", record.RuleTag)
	require.Equal(t, methodSubscribeCosmos, record.Method)
	require.Equal(t, "198.51.100.7", record.SourceIP)
	require.Equal(t, "rpc.jsonrpc", record.Section)
}

func TestWSRuleReloadAppliesRuleAuthToLiveSubscriptions(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	proxy.auth = &Authenticator{}
	alice, _ := newWSCacheClient(t)
	bob, bobPeer := newWSCacheClient(t)
	for i, c := range []struct {
		client *JsonRpcWsClient
		name   string
	}{{alice, "alice"}, {bob, "bob"}} {
		proxy.registerConn(c.client, "192.0.2.1", &Identity{Name: c.name, Method: "apikey"}, time.Now())
		_, err := proxy.broker.HandleSubscription(c.client, &JsonRpcMsg{
			Version: jsonRpcVersion, ID: i + 1, Method: methodSubscribeCosmos, Params: []any{"q"},
		})
		require.NoError(t, err)
	}

	onlyAlice := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{methodSubscribeCosmos},
		Auth: &RuleAuthConfig{Identities: []string{"alice"}}}
	require.NoError(t, onlyAlice.Compile())
	proxy.SetRules([]*JsonRpcRule{onlyAlice}, RuleActionDeny, nil)

	require.NoError(t, bobPeer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var notice JsonRpcMsg
	require.NoError(t, bobPeer.ReadJSON(&notice))
	require.EqualValues(t, 2, notice.ID)
	require.NotNil(t, notice.Error)
	require.Equal(t, 0, proxy.broker.ClientSubCount(bob))
	require.Equal(t, 1, proxy.broker.ClientSubCount(alice))
	require.Equal(t, "auth", proxy.cgDashboard.denied.Snapshot()[0].Reason)
}

func TestWSConnectionKeepsResolvedIdentity(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	identity := &Identity{Name: "alice", Method: "apikey"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.HandleConnection(w, r.WithContext(context.WithValue(r.Context(), identityCtxKey{}, identity)))
	}))
	t.Cleanup(server.Close)
	peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	require.Eventually(t, func() bool {
		proxy.connsMu.Lock()
		defer proxy.connsMu.Unlock()
		for _, info := range proxy.conns {
			return info.resolved == identity
		}
		return false
	}, 2*time.Second, 5*time.Millisecond)
}

// A revocation that lands while the subscribe acknowledgement is unwritten
// sends its cancellation after the acknowledgement.
func TestWSRevocationNoticeFollowsAcknowledgement(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	client, peer := newWSCacheClient(t)
	res, delivered, err := proxy.broker.handleSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 3, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)
	proxy.SetRules(nil, RuleActionDeny, nil)
	require.Eventually(t, func() bool { return proxy.broker.ClientSubCount(client) == 0 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, client.SendMsg(res))
	delivered()

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var ack, notice JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&ack))
	require.Nil(t, ack.Error, "acknowledgement must come first")
	require.NoError(t, peer.ReadJSON(&notice))
	require.NotNil(t, notice.Error)
	require.EqualValues(t, 3, notice.ID)
}

func TestWSRevocationSkipsClosingClient(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	client := NewJsonRpcWsClient(nil)
	id, err := proxy.broker.addSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)
	client.closed.Store(true)
	require.Nil(t, proxy.broker.revoke(client, id, func(*JsonRpcWsClient, *JsonRpcMsg) func() { return func() {} }))
	require.Equal(t, 1, proxy.broker.ClientSubCount(client))
}

func TestWSRevocationOfNotificationSubscribeSendsNothing(t *testing.T) {
	proxy := newReloadTestProxy(t, newLimitingUpstream())
	client, peer := newWSCacheClient(t)
	_, err := proxy.broker.HandleSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, Method: methodSubscribeCosmos, Params: []any{"q"},
	})
	require.NoError(t, err)
	proxy.SetRules(nil, RuleActionDeny, nil)
	require.Eventually(t, func() bool { return proxy.broker.ClientSubCount(client) == 0 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	_, _, err = peer.ReadMessage()
	require.Error(t, err, "a notification must get no reply")
	require.False(t, client.IsClosed())
}
