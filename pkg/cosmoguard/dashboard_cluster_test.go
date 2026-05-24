package cosmoguard

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// peerResponseFromJSON is a small test helper — packages a JSON
// payload and a pod id into the same peerResponse shape clusterFanout
// would produce on a live cluster, so the aggregator unit tests can
// drive realistic inputs without spinning up two olric daemons.
func peerResponseFromJSON(t *testing.T, podID string, payload any) peerResponse {
	t.Helper()
	body, err := stdjson.Marshal(payload)
	require.NoError(t, err)
	return peerResponse{PodID: podID, Addr: podID, Body: body}
}

// TestClusterPeerApiPort_DefaultsToBindPortPlusOne pins the documented
// default — a single explicit BindPort gets a predictable peer-API
// port one above it. The "0 in cluster off" case is the cold-start
// guard that lets installPeerAPIServer short-circuit cleanly.
func TestClusterPeerApiPort_DefaultsToBindPortPlusOne(t *testing.T) {
	cases := []struct {
		name string
		in   *ClusterConfig
		want int
	}{
		// nil ClusterConfig = embedded mode (no cluster:= block in
		// operator config); peer API doesn't apply, port == 0.
		{"nil", nil, 0},
		{"default", &ClusterConfig{BindPort: 3320}, 3321},
		{"explicit", &ClusterConfig{BindPort: 3320, PeerApiPort: 9000}, 9000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clusterPeerApiPort(tc.in))
		})
	}
}

// TestAggregateUnmatched merges two peers' unmatched maps. Counts
// must sum on (section, method, path) collisions; FirstSeen must pick
// the smaller (earlier) value; LastSeen must pick the larger (later)
// value. The "deny-path" key on pod-A alone must survive untouched —
// no cross-peer deletion semantics.
func TestAggregateUnmatched(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"sections": map[string][]UnmatchedEntry{
			"lcd": {
				{Section: "lcd", Method: "GET", Path: "/foo", Count: 5, FirstSeen: 200, LastSeen: 300},
				{Section: "lcd", Method: "POST", Path: "/bar", Count: 1, FirstSeen: 100, LastSeen: 100},
			},
		},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"sections": map[string][]UnmatchedEntry{
			"lcd": {
				{Section: "lcd", Method: "GET", Path: "/foo", Count: 3, FirstSeen: 150, LastSeen: 500},
			},
		},
	})

	out := aggregateUnmatched([]peerResponse{a, b})
	require.Len(t, out["lcd"], 2)

	// Ordered count-desc so the dashboard renders the highest-traffic
	// entries first.
	require.Equal(t, "GET", out["lcd"][0].Method)
	require.Equal(t, uint64(8), out["lcd"][0].Count)
	require.Equal(t, int64(150), out["lcd"][0].FirstSeen, "FirstSeen takes the earlier value")
	require.Equal(t, int64(500), out["lcd"][0].LastSeen, "LastSeen takes the later value")

	require.Equal(t, "POST", out["lcd"][1].Method)
	require.Equal(t, uint64(1), out["lcd"][1].Count)
}

// TestAggregateUnmatched_SkipsFailedPeers verifies the soft-error
// posture: a peer that returned an Err is excluded from the merge,
// but the remaining peer's data still surfaces — the dashboard never
// goes empty just because one peer was unreachable.
func TestAggregateUnmatched_SkipsFailedPeers(t *testing.T) {
	good := peerResponseFromJSON(t, "pod-A", map[string]any{
		"sections": map[string][]UnmatchedEntry{
			"lcd": {{Section: "lcd", Method: "GET", Path: "/foo", Count: 2}},
		},
	})
	bad := peerResponse{PodID: "pod-B", Err: "connection refused"}

	out := aggregateUnmatched([]peerResponse{good, bad})
	require.Len(t, out["lcd"], 1)
	require.Equal(t, uint64(2), out["lcd"][0].Count)
}

// TestAggregateDenied merges two ring buffers, asserts newest-first
// ordering, and confirms the cluster-wide cap caps the output. Without
// the cap an N-pod cluster could return N× the local ring-buffer size
// in one response.
func TestAggregateDenied(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"denied": []DenyRecord{
			{TimestampMs: 300, Reason: "rule"},
			{TimestampMs: 100, Reason: "rate_limit"},
		},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"denied": []DenyRecord{
			{TimestampMs: 250, Reason: "auth"},
		},
	})

	out := aggregateDenied([]peerResponse{a, b})
	require.Len(t, out, 3)
	require.Equal(t, int64(300), out[0].TimestampMs)
	require.Equal(t, int64(250), out[1].TimestampMs)
	require.Equal(t, int64(100), out[2].TimestampMs)
}

// TestAggregateDenied_AppliesCap drives more entries than the cluster-
// wide cap to confirm the cap actually fires.
func TestAggregateDenied_AppliesCap(t *testing.T) {
	// 250 records each across 2 peers = 500 > 200 cap.
	mk := func(podID string, base int64) peerResponse {
		records := make([]DenyRecord, 250)
		for i := range records {
			records[i] = DenyRecord{TimestampMs: base + int64(i), Reason: "rule"}
		}
		return peerResponseFromJSON(t, podID, map[string]any{"denied": records})
	}
	out := aggregateDenied([]peerResponse{mk("pod-A", 1000), mk("pod-B", 2000)})
	require.Len(t, out, maxDeniedAcrossCluster)
	// Top entry must be the latest across both peers.
	require.Equal(t, int64(2249), out[0].TimestampMs)
}

// TestAggregateDiscoveryLog mirrors the denied test on the discovery-
// event ring buffer.
func TestAggregateDiscoveryLog(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"events": []DiscoveryEvent{
			{TimestampMs: 100, Type: "tick", Template: "node.local"},
			{TimestampMs: 300, Type: "add", Template: "node.local"},
		},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"events": []DiscoveryEvent{
			{TimestampMs: 200, Type: "remove", Template: "node.local"},
		},
	})
	out := aggregateDiscoveryLog([]peerResponse{a, b})
	require.Len(t, out, 3)
	require.Equal(t, int64(300), out[0].TimestampMs)
	require.Equal(t, int64(200), out[1].TimestampMs)
	require.Equal(t, int64(100), out[2].TimestampMs)
}

// TestAggregateCardinality sums distinct-key counts on (section,
// rule_tag) collisions and unions hot samples up to the per-rule cap.
// Documents the "upper bound" caveat — overlapping keys across peers
// would double-count by design (the v4 design trade-off in the plan).
func TestAggregateCardinality(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"sections": map[string][]CardinalityRule{
			"lcd": {{RuleTag: "balances", DistinctKeys: 50, HotSamples: []string{"a", "b"}}},
		},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"sections": map[string][]CardinalityRule{
			"lcd": {{RuleTag: "balances", DistinctKeys: 30, HotSamples: []string{"c", "d", "e"}}},
		},
	})

	out := aggregateCardinality([]peerResponse{a, b})
	require.Len(t, out["lcd"], 1)
	row := out["lcd"][0]
	require.Equal(t, "balances", row.RuleTag)
	require.Equal(t, 80, row.DistinctKeys, "sum of distinct keys (upper bound)")
	require.Len(t, row.HotSamples, maxCardinalityHotSamples, "samples capped at maxCardinalityHotSamples")
}

// TestAggregateCardinality_DedupesHotSamplesAcrossPeers — when two
// peers report the same hot key (the common case for sticky cache
// hotspots), the merged HotSamples list must surface that key once,
// not once per peer. The chip slot it would otherwise burn is a
// scarce resource (cap of maxCardinalityHotSamples), and duplicates
// hide the next-hottest key from the operator.
func TestAggregateCardinality_DedupesHotSamplesAcrossPeers(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"sections": map[string][]CardinalityRule{
			"lcd": {{RuleTag: "balances", DistinctKeys: 50, HotSamples: []string{"k1", "k2"}}},
		},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"sections": map[string][]CardinalityRule{
			"lcd": {{RuleTag: "balances", DistinctKeys: 30, HotSamples: []string{"k1", "k3"}}},
		},
	})

	out := aggregateCardinality([]peerResponse{a, b})
	require.Len(t, out["lcd"], 1)
	row := out["lcd"][0]
	require.Equal(t, 80, row.DistinctKeys)
	// k1 appeared in both peers; k2 and k3 are unique. The merged
	// list must hold {k1, k2, k3} — three entries, not four.
	counts := map[string]int{}
	for _, s := range row.HotSamples {
		counts[s]++
	}
	require.Equal(t, 1, counts["k1"], "duplicate hot key collapsed to single entry")
	require.Equal(t, 1, counts["k2"])
	require.Equal(t, 1, counts["k3"])
	require.Len(t, row.HotSamples, 3, "three distinct samples after dedup")
}

// TestCollectReloadStatus_PreservesPerPeerRows — unlike the other
// aggregators we never merge reload-status, because the operator's
// question is "did every pod pick up the new config" which requires
// per-pod visibility. A peer that failed to respond must still appear
// as an empty row so the dashboard can flag it.
func TestCollectReloadStatus_PreservesPerPeerRows(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"reload": ReloadStatus{
			TimestampMs: 1000, Success: true,
			Sections: map[string]ReloadSection{"lcd": {Before: 5, After: 7, Added: 2}},
		},
	})
	b := peerResponse{PodID: "pod-B", Err: "timeout"}
	c := peerResponseFromJSON(t, "pod-C", map[string]any{
		"reload": ReloadStatus{
			TimestampMs: 2000, Success: false, Error: "config rejected",
			Sections: map[string]ReloadSection{},
		},
	})

	out := collectReloadStatus([]peerResponse{a, b, c})
	require.Len(t, out, 3)

	// Sorted by PodID for stable rendering.
	require.Equal(t, "pod-A", out[0].PodID)
	require.True(t, out[0].Reload.Success)
	require.Equal(t, 2, out[0].Reload.Sections["lcd"].Added)

	require.Equal(t, "pod-B", out[1].PodID)
	require.False(t, out[1].Reload.Success, "unreachable peer surfaces as empty success-false row")
	require.NotNil(t, out[1].Reload.Sections)

	require.Equal(t, "pod-C", out[2].PodID)
	require.False(t, out[2].Reload.Success)
	require.Equal(t, "config rejected", out[2].Reload.Error)
}

// TestAggregateMetrics sums histograms (count + sum + per-bucket
// cumulative count) and per-label maps across peers. The merged
// shape is shaped to be a valid MetricsSnapshot the dashboard's
// client-side rate math can chew on without special-casing.
func TestAggregateMetrics(t *testing.T) {
	a := peerResponseFromJSON(t, "pod-A", MetricsSnapshot{
		TimestampMs: 100,
		Protocols: map[string]ProtocolMetrics{
			"lcd": {
				Histogram: HistogramView{Sum: 1.0, Count: 10, Buckets: []HistBucketV{{Le: 0.1, Count: 4}, {Le: 1.0, Count: 10}}},
				ByCache:   map[string]uint64{"hit": 6, "miss": 4},
			},
		},
		Upstreams: []UpstreamHealth{{Pool: "lcd", Upstream: "node-a", Healthy: 1}},
	})
	b := peerResponseFromJSON(t, "pod-B", MetricsSnapshot{
		TimestampMs: 200,
		Protocols: map[string]ProtocolMetrics{
			"lcd": {
				Histogram: HistogramView{Sum: 2.0, Count: 20, Buckets: []HistBucketV{{Le: 0.1, Count: 8}, {Le: 1.0, Count: 20}}},
				ByCache:   map[string]uint64{"hit": 15, "miss": 5},
			},
		},
		Upstreams: []UpstreamHealth{{Pool: "lcd", Upstream: "node-a", Healthy: 0}},
	})

	out := aggregateMetrics([]peerResponse{a, b})
	require.NotNil(t, out)
	lcd, ok := out.Protocols["lcd"]
	require.True(t, ok)
	require.Equal(t, 3.0, lcd.Histogram.Sum)
	require.Equal(t, uint64(30), lcd.Histogram.Count)
	require.Equal(t, uint64(21), lcd.ByCache["hit"])
	require.Equal(t, uint64(9), lcd.ByCache["miss"])

	// Buckets must come back sorted ascending so the client's
	// percentile math (which assumes sorted Le) doesn't trip.
	require.Equal(t, []HistBucketV{{Le: 0.1, Count: 12}, {Le: 1.0, Count: 30}}, lcd.Histogram.Buckets)

	// Upstream merge — max across peers, so one healthy peer reports
	// the upstream as healthy. We expect a single entry, not the
	// per-peer split.
	require.Len(t, out.Upstreams, 1)
	require.Equal(t, float64(1), out.Upstreams[0].Healthy)
}

// TestAggregateMetricsHistory buckets per-peer history entries into
// 5 s windows and sums each window. The resulting series is ordered
// oldest-first so the client can append live polls onto the tail —
// same contract as the single-pod /metrics/history endpoint.
func TestAggregateMetricsHistory(t *testing.T) {
	mk := func(ts int64, count uint64) MetricsSnapshot {
		return MetricsSnapshot{
			TimestampMs: ts,
			Protocols: map[string]ProtocolMetrics{
				"lcd": {Histogram: HistogramView{Count: count}},
			},
		}
	}
	a := peerResponseFromJSON(t, "pod-A", map[string]any{
		"history": []MetricsSnapshot{mk(1000, 10), mk(6000, 30)},
	})
	b := peerResponseFromJSON(t, "pod-B", map[string]any{
		"history": []MetricsSnapshot{mk(1100, 20), mk(6500, 40)}, // both within ±5s of pod-A entries
	})

	out := aggregateMetricsHistory([]peerResponse{a, b})
	require.Len(t, out, 2, "two 5 s buckets covering the four input samples")
	require.Equal(t, int64(0), out[0].TimestampMs, "first bucket aligned to 5 s grid")
	require.Equal(t, uint64(30), out[0].Protocols["lcd"].Histogram.Count, "1000ms + 1100ms summed")
	require.Equal(t, int64(5000), out[1].TimestampMs)
	require.Equal(t, uint64(70), out[1].Protocols["lcd"].Histogram.Count, "6000ms + 6500ms summed")
}

// TestPeerMembershipGate_LoopbackAllowed — tests of the peer-API
// gate use a fake CosmoGuard with no cluster runtime (clusterMembers
// returns nil). Loopback callers must still get through so test
// harnesses + health probes work without needing to join the cluster.
func TestPeerMembershipGate_LoopbackAllowed(t *testing.T) {
	gate := peerMembershipGate(&CosmoGuard{})
	srv := httptest.NewServer(gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/ping")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestPeerMembershipGate_ForeignIPRejected — the gate's whole reason
// to exist is to reject requests from off-cluster IPs. We can't
// easily forge RemoteAddr through net/http, so this test reaches in
// at the handler level instead.
func TestPeerMembershipGate_ForeignIPRejected(t *testing.T) {
	gate := peerMembershipGate(&CosmoGuard{})
	h := gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.RemoteAddr = "203.0.113.5:54321" // TEST-NET-3, never loopback, no chance of being a real peer
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

// TestFanoutGet_RoundTrip exercises the GET helper against an
// httptest server so the timeout + 4 MiB cap path are covered.
func TestFanoutGet_RoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"history": []}`))
	}))
	t.Cleanup(srv.Close)

	body, err := fanoutGet(t.Context(), srv.Client(), srv.URL)
	require.NoError(t, err)
	require.Contains(t, string(body), "history")
}

// TestFanoutGet_NonOKStatusIsError — a 4xx/5xx from a peer must
// surface as an error so the aggregator can flag the peer in the
// envelope rather than treating an empty body as valid state.
func TestFanoutGet_NonOKStatusIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	_, err := fanoutGet(t.Context(), srv.Client(), srv.URL)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "500"))
}

// TestAggregators_AllPeersFailedReturnNonNilContainers pins the
// "every peer is unreachable" envelope shape. Without this guard a
// future regression could let an aggregator return a nil map / nil
// slice for the empty-input case, and a dashboard client doing
// `for k, v := range payload.sections` (JS) or `for _, x := range
// payload.denied` (Go) would silently see "no data" — indistinguishable
// from "data was empty". With non-nil empties, the on-wire JSON is
// `{}` / `[]` and clients iterate zero times against a real container,
// which is the contract `clusterEnvelope.Data` advertises.
//
// Catches the pattern where an aggregator's `out := make(...)` gets
// refactored to `var out map[...]...` and silently flips the empty
// case from `{}` to `null` — a JSON change a Go-level test would
// miss because nil-map iteration is fine in Go but null is not a
// container in JS.
func TestAggregators_AllPeersFailedReturnNonNilContainers(t *testing.T) {
	failed := []peerResponse{
		{PodID: "pod-A", Err: "connection refused"},
		{PodID: "pod-B", Err: "i/o timeout"},
	}

	t.Run("unmatched", func(t *testing.T) {
		out := aggregateUnmatched(failed)
		require.NotNil(t, out, "aggregateUnmatched must return non-nil map even when every peer failed")
		require.Empty(t, out)
		// Round-trip through JSON to prove on-wire shape is "{}" not "null".
		b, err := stdjson.Marshal(out)
		require.NoError(t, err)
		require.Equal(t, "{}", string(b))
	})

	t.Run("denied", func(t *testing.T) {
		out := aggregateDenied(failed)
		require.NotNil(t, out)
		b, err := stdjson.Marshal(out)
		require.NoError(t, err)
		require.Equal(t, "[]", string(b), "all-failed denied must render as `[]`, not `null`")
	})

	t.Run("discoveryLog", func(t *testing.T) {
		out := aggregateDiscoveryLog(failed)
		require.NotNil(t, out)
		b, err := stdjson.Marshal(out)
		require.NoError(t, err)
		require.Equal(t, "[]", string(b))
	})

	t.Run("cardinality", func(t *testing.T) {
		out := aggregateCardinality(failed)
		require.NotNil(t, out)
		b, err := stdjson.Marshal(out)
		require.NoError(t, err)
		require.Equal(t, "{}", string(b))
	})

	t.Run("metrics", func(t *testing.T) {
		out := aggregateMetrics(failed)
		require.NotNil(t, out, "aggregateMetrics must return a non-nil *MetricsSnapshot")
		require.NotNil(t, out.Protocols, "Protocols map must be non-nil so the client can range it")
		require.NotNil(t, out.Batches)
		require.NotNil(t, out.Upstreams)
	})

	t.Run("metricsHistory", func(t *testing.T) {
		out := aggregateMetricsHistory(failed)
		require.NotNil(t, out)
		b, err := stdjson.Marshal(out)
		require.NoError(t, err)
		require.Equal(t, "[]", string(b))
	})

	t.Run("reloadStatus", func(t *testing.T) {
		// collectReloadStatus preserves per-peer rows so the operator
		// sees WHICH peer was unreachable — non-empty even on total
		// failure, with each row carrying an empty ReloadStatus.
		out := collectReloadStatus(failed)
		require.Len(t, out, 2, "collectReloadStatus keeps a row per failed peer so unreachable pods are visible")
		for _, row := range out {
			require.NotNil(t, row.Reload.Sections, "Sections must be non-nil even for unreachable peers")
		}
	})
}

// TestAggregators_MalformedJSONSkippedNotPropagated proves the
// "this peer returned 200 but the body is garbage" path doesn't
// poison the aggregate. A misbehaving peer (truncated response, half-
// upgraded with a different schema, body-rewriter MITM) must be
// dropped from the merge — the dashboard's contract is "show what you
// can see," not "fail closed on first malformed peer."
func TestAggregators_MalformedJSONSkippedNotPropagated(t *testing.T) {
	good := peerResponseFromJSON(t, "pod-A", map[string]any{
		"sections": map[string][]UnmatchedEntry{
			"lcd": {{Section: "lcd", Method: "GET", Path: "/x", Count: 5}},
		},
	})
	garbage := peerResponse{PodID: "pod-B", Body: []byte("not json {{{")}

	out := aggregateUnmatched([]peerResponse{good, garbage})
	require.Len(t, out["lcd"], 1, "good peer's payload must survive a sibling's garbage")
	require.Equal(t, uint64(5), out["lcd"][0].Count)
}

// TestClusterFanout_NoMembersReturnsNil pins the "single-instance
// pod / cluster off" short-circuit. Without it, fan-out would spawn
// goroutines against an empty member list and the handler would
// return an envelope with Peers=[] but Data built from a nil
// peerResponse slice — fine in Go, but the test pins that the slice
// IS nil so callers (and the toPeerEnvelopes(nil) helper) know
// what to expect.
func TestClusterFanout_NoMembersReturnsNil(t *testing.T) {
	cg := &CosmoGuard{} // no cluster runtime → clusterMembers returns nil
	out := clusterFanout(t.Context(), cg, "unmatched", 3321)
	require.Nil(t, out, "fan-out against an empty member set must short-circuit to nil")

	// toPeerEnvelopes(nil) must still produce a non-nil empty slice so
	// the JSON envelope's "peers" field renders as `[]` not `null`.
	envs := toPeerEnvelopes(out)
	require.NotNil(t, envs)
	b, err := stdjson.Marshal(envs)
	require.NoError(t, err)
	require.Equal(t, "[]", string(b))
}
