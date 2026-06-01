package cosmoguard

import (
	stdjson "encoding/json"
	"testing"
	"time"

	"gotest.tools/assert"
)

// TestSubscriptionManagerSnapshot pins the dedup-aware snapshot: one
// row per upstream subscription, subscriber count = client fan-out.
func TestSubscriptionManagerSnapshot(t *testing.T) {
	sm := NewSubscriptionManager()
	c1, c2 := &JsonRpcWsClient{}, &JsonRpcWsClient{}
	sm.AddSubscription("tm.event='NewBlock'", "sub-1")
	sm.SubscribeClient("sub-1", c1, 1)
	sm.SubscribeClient("sub-1", c2, 2)
	sm.AddSubscription("newHeads", "sub-2")
	sm.SubscribeClient("sub-2", c1, 3)

	snap := sm.Snapshot()
	assert.Equal(t, len(snap), 2)
	byParam := map[string]SubStat{}
	for _, s := range snap {
		byParam[s.Param] = s
	}
	assert.Equal(t, byParam["tm.event='NewBlock'"].Subscribers, 2)
	assert.Equal(t, byParam["newHeads"].Subscribers, 1)
	assert.Equal(t, byParam["tm.event='NewBlock'"].ID, "sub-1")
}

// TestUpstreamPoolConnStats verifies per-conn target/health/subcount
// and that SubscriptionTarget resolves the backend a sub pinned to.
func TestUpstreamPoolConnStats(t *testing.T) {
	pool := NewUpstreamPool([]string{"ws://backend:26657"}, "/websocket", 2, func(*JsonRpcMsg) {}, fakeConstructor)

	stats := pool.ConnStats()
	assert.Equal(t, len(stats), 2)
	for _, s := range stats {
		assert.Equal(t, s.Target, "backend:26657")
		assert.Equal(t, s.Healthy, true)
		assert.Equal(t, s.Subscriptions, 0)
	}

	id, err := pool.Subscribe("tm.event='NewBlock'")
	assert.NilError(t, err)
	assert.Equal(t, pool.SubscriptionTarget(id), "backend:26657")

	// Exactly one conn now carries the pinned subscription.
	total := 0
	for _, s := range pool.ConnStats() {
		total += s.Subscriptions
	}
	assert.Equal(t, total, 1)

	assert.Equal(t, pool.SubscriptionTarget("does-not-exist"), "")
}

// TestBrokerSubStatsJoinsUpstream confirms the broker joins the
// subscription manager's fan-out with the pool's pin target.
func TestBrokerSubStatsJoinsUpstream(t *testing.T) {
	b := NewBroker([]string{"ws://backend:26657"}, "/websocket", 2, fakeConstructor)
	b.log = log.WithField("t", "ws")
	c := &JsonRpcWsClient{}
	_, err := b.addSubscription(c, &JsonRpcMsg{Method: methodSubscribeCosmos, Params: []any{"tm.event='NewBlock'"}})
	assert.NilError(t, err)

	subs := b.SubStats()
	assert.Equal(t, len(subs), 1)
	assert.Equal(t, subs[0].Param, "tm.event='NewBlock'")
	assert.Equal(t, subs[0].Subscribers, 1)
	assert.Equal(t, subs[0].Upstream, "backend:26657")

	ups := b.UpstreamStats()
	assert.Equal(t, len(ups), 2)
	assert.Equal(t, b.ClientSubCount(c), 1)
}

// TestWSRecordOutcomeFeedsRequestLog confirms a WS frame lands in the
// Live-traffic request log with the section, path, and method set.
func TestWSRecordOutcomeFeedsRequestLog(t *testing.T) {
	rl := enabledLog(t, 100)
	p := &JsonRpcWebSocketProxy{section: "rpc.jsonrpc", path: "/websocket", log: log.WithField("t", "ws")}
	p.SetRequestLog(rl)

	p.recordOutcome(&JsonRpcMsg{Method: "eth_subscribe", ID: 1}, "9.9.9.9",
		cacheMiss, string(RuleActionAllow), "tag-x", time.Now(), "request allowed")

	entries := rl.Snapshot(nil, 0)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Section, "rpc.jsonrpc")
	assert.Equal(t, entries[0].Path, "/websocket")
	assert.Equal(t, entries[0].Method, "eth_subscribe")
	assert.Equal(t, entries[0].SourceIP, "9.9.9.9")
	assert.Equal(t, entries[0].Status, 200)
}

// TestWSRecordLifecycleFeedsRequestLog confirms CONNECT/CLOSE rows are
// pushed with the right method labels.
func TestWSRecordLifecycleFeedsRequestLog(t *testing.T) {
	rl := enabledLog(t, 100)
	p := &JsonRpcWebSocketProxy{section: "evm.ws", path: "/", log: log.WithField("t", "ws")}
	p.SetRequestLog(rl)

	p.recordLifecycle("CONNECT", "1.1.1.1", "alice", 101, 0)
	p.recordLifecycle("CLOSE", "1.1.1.1", "alice", 200, 0)

	entries := rl.Snapshot(nil, 0)
	assert.Equal(t, len(entries), 2)
	methods := map[string]bool{}
	for _, e := range entries {
		methods[e.Method] = true
		assert.Equal(t, e.Section, "evm.ws")
		assert.Equal(t, e.Identity, "alice")
	}
	assert.Equal(t, methods["CONNECT"], true)
	assert.Equal(t, methods["CLOSE"], true)
}

// TestAggregateWebSocketMergesSections checks the cluster aggregator:
// counters sum across pods and subscription params merge by
// (section, param) summing subscribers.
func TestAggregateWebSocketMergesSections(t *testing.T) {
	pod := func(conns, clientSubs, upSubs, upHealthy, upTotal int, subParam string, subscribers int) peerResponse {
		body, _ := stdjson.Marshal(map[string]any{
			"sections": []WSSectionStats{{
				Section:               "rpc.jsonrpc",
				Path:                  "/websocket",
				Connections:           conns,
				ClientSubscriptions:   clientSubs,
				UpstreamSubscriptions: upSubs,
				UpstreamConnsHealthy:  upHealthy,
				UpstreamConnsTotal:    upTotal,
				Conns:                 []WSConnInfo{{SourceIP: "1.2.3.4"}},
				Subs:                  []WSSubInfo{{Param: subParam, Subscribers: subscribers, Upstream: "backend:26657"}},
				Upstreams:             []ConnStat{{Target: "backend:26657", Healthy: true, Subscriptions: subscribers}},
			}},
		})
		return peerResponse{Body: body}
	}

	out := aggregateWebSocket([]peerResponse{
		pod(3, 5, 2, 2, 2, "tm.event='NewBlock'", 4),
		pod(2, 3, 1, 1, 2, "tm.event='NewBlock'", 1),
	})
	assert.Equal(t, len(out), 1)
	s := out[0]
	assert.Equal(t, s.Connections, 5)
	assert.Equal(t, s.ClientSubscriptions, 8)
	assert.Equal(t, s.UpstreamSubscriptions, 3)
	assert.Equal(t, s.UpstreamConnsHealthy, 3)
	assert.Equal(t, s.UpstreamConnsTotal, 4)
	// Conns + upstreams concatenated across pods.
	assert.Equal(t, len(s.Conns), 2)
	assert.Equal(t, len(s.Upstreams), 2)
	// Same param merged, subscribers summed.
	assert.Equal(t, len(s.Subs), 1)
	assert.Equal(t, s.Subs[0].Subscribers, 5)
}

// TestAggregateWebSocketSkipsErroredPeers ensures an unreachable peer
// (Err set, empty body) is ignored rather than poisoning the merge.
func TestAggregateWebSocketSkipsErroredPeers(t *testing.T) {
	good, _ := stdjson.Marshal(map[string]any{
		"sections": []WSSectionStats{{Section: "evm.ws", Connections: 1}},
	})
	out := aggregateWebSocket([]peerResponse{
		{Err: "timeout"},
		{Body: good},
	})
	assert.Equal(t, len(out), 1)
	assert.Equal(t, out[0].Connections, 1)
}
