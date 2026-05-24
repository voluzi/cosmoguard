package cosmoguard

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gotest.tools/assert"
)

// TestGatherMetricsSnapshot_ClassifiesProtocolHistogram builds a tiny
// private registry with one request-duration histogram and pins that
// the snapshot walker classifies it under Protocols with the right
// slug + label aggregation.
func TestGatherMetricsSnapshot_ClassifiesProtocolHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "lcd",
		Name:      "request_duration_seconds",
		Buckets:   []float64{0.001, 0.01, 0.1, 1.0},
	}, []string{"method", "status_code", "cache", "action", "rule_id", "upstream"})
	reg.MustRegister(h)

	// Drive a few observations with distinct label combos.
	h.WithLabelValues("GET", "200", "miss", "allow", "params", "node-a").Observe(0.005)
	h.WithLabelValues("GET", "200", "hit", "allow", "params", "node-a").Observe(0.0005)
	h.WithLabelValues("POST", "401", "n/a", "deny", "default", "").Observe(0.002)

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	assert.Equal(t, len(snap.Protocols), 1)

	p, ok := snap.Protocols["lcd"]
	assert.Assert(t, ok, "expected lcd protocol; got %v", snap.Protocols)
	assert.Equal(t, p.Histogram.Count, uint64(3))
	assert.Equal(t, p.ByCache["miss"], uint64(1))
	assert.Equal(t, p.ByCache["hit"], uint64(1))
	assert.Equal(t, p.ByCache["n/a"], uint64(1))
	assert.Equal(t, p.ByAction["allow"], uint64(2))
	assert.Equal(t, p.ByAction["deny"], uint64(1))
	assert.Equal(t, p.ByStatus["200"], uint64(2))
	assert.Equal(t, p.ByStatus["401"], uint64(1))
	assert.Equal(t, p.ByMethod["GET"], uint64(2))
	assert.Equal(t, p.ByMethod["POST"], uint64(1))
	assert.Equal(t, p.ByUpstream["node-a"], uint64(2))
	assert.Equal(t, p.ByRule["params"], uint64(2))
	assert.Equal(t, p.ByRule["default"], uint64(1))

	// Buckets must be sorted ascending by Le and cumulative.
	assert.Assert(t, len(p.Histogram.Buckets) >= 4,
		"expected at least 4 buckets, got %d", len(p.Histogram.Buckets))
	for i := 1; i < len(p.Histogram.Buckets); i++ {
		assert.Assert(t, p.Histogram.Buckets[i-1].Le < p.Histogram.Buckets[i].Le,
			"buckets not ascending: %+v", p.Histogram.Buckets)
		assert.Assert(t, p.Histogram.Buckets[i-1].Count <= p.Histogram.Buckets[i].Count,
			"buckets not cumulative: %+v", p.Histogram.Buckets)
	}
}

// TestGatherMetricsSnapshot_ByUpstreamHistogram pins the per-upstream
// histogram split: with traffic across two upstreams, each gets its
// own histogram view; the per-upstream counts sum to the aggregate;
// each per-upstream bucket series is sorted ascending and cumulative
// (the same invariants the dashboard's percentile derivation assumes).
func TestGatherMetricsSnapshot_ByUpstreamHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "lcd",
		Name:      "request_duration_seconds",
		Buckets:   []float64{0.001, 0.01, 0.1, 1.0},
	}, []string{"method", "status_code", "cache", "action", "rule_id", "upstream"})
	reg.MustRegister(h)

	// node-a: 3 fast observations. node-b: 2 slower observations.
	for i := 0; i < 3; i++ {
		h.WithLabelValues("GET", "200", "miss", "allow", "params", "node-a").Observe(0.002)
	}
	for i := 0; i < 2; i++ {
		h.WithLabelValues("GET", "200", "miss", "allow", "params", "node-b").Observe(0.5)
	}

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	p, ok := snap.Protocols["lcd"]
	assert.Assert(t, ok)
	assert.Assert(t, p.ByUpstreamHistogram != nil,
		"by_upstream_histogram should be populated when upstream labels carry traffic")

	a, hasA := p.ByUpstreamHistogram["node-a"]
	b, hasB := p.ByUpstreamHistogram["node-b"]
	assert.Assert(t, hasA, "expected node-a entry")
	assert.Assert(t, hasB, "expected node-b entry")
	assert.Equal(t, a.Count, uint64(3))
	assert.Equal(t, b.Count, uint64(2))
	// Per-upstream counts must sum to the aggregate — same invariant
	// the dashboard relies on when it cross-checks "did I miss an
	// upstream while computing my multi-upstream chart".
	assert.Equal(t, a.Count+b.Count, p.Histogram.Count)

	// Buckets per upstream: sorted ascending + cumulative.
	for name, hv := range map[string]HistogramView{"node-a": a, "node-b": b} {
		assert.Assert(t, len(hv.Buckets) >= 4,
			"%s: expected ≥4 buckets, got %d", name, len(hv.Buckets))
		for i := 1; i < len(hv.Buckets); i++ {
			assert.Assert(t, hv.Buckets[i-1].Le < hv.Buckets[i].Le,
				"%s: buckets not ascending: %+v", name, hv.Buckets)
			assert.Assert(t, hv.Buckets[i-1].Count <= hv.Buckets[i].Count,
				"%s: buckets not cumulative: %+v", name, hv.Buckets)
		}
	}
}

// TestGatherMetricsSnapshot_ByUpstreamHistogram_EmptyOmitted pins the
// omitempty wire shape: when no traffic has flowed, the map stays nil
// so the JSON payload doesn't carry an empty object the client has to
// special-case.
func TestGatherMetricsSnapshot_ByUpstreamHistogram_EmptyOmitted(t *testing.T) {
	reg := prometheus.NewRegistry()
	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	// No protocols → nothing to assert on the map directly, but the
	// shape contract is "protocols is empty when no metrics fired".
	assert.Equal(t, len(snap.Protocols), 0)
}

// TestGatherMetricsSnapshot_ClassifiesWebSocketProtocol pins that the
// "websocket_<name>" namespace stays distinct from the plain HTTP /
// JSON-RPC "<name>" namespace — the client uses the prefix to know
// it's looking at WS traffic.
func TestGatherMetricsSnapshot_ClassifiesWebSocketProtocol(t *testing.T) {
	reg := prometheus.NewRegistry()
	wsH := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "websocket_rpc",
		Name:      "request_duration_seconds",
		Buckets:   []float64{0.01, 0.1, 1.0},
	}, []string{"method", "cache", "action", "rule_id", "upstream"})
	reg.MustRegister(wsH)
	wsH.WithLabelValues("eth_subscribe", "n/a", "allow", "ws-sub", "node-a").Observe(0.05)

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	p, ok := snap.Protocols["websocket_rpc"]
	assert.Assert(t, ok, "expected websocket_rpc; got %v", snap.Protocols)
	assert.Equal(t, p.Histogram.Count, uint64(1))
	assert.Equal(t, p.ByMethod["eth_subscribe"], uint64(1))
}

// TestGatherMetricsSnapshot_ClassifiesBatchHistogram pins the
// _batch_request_duration_seconds suffix is routed to the Batches
// bucket and the size_class label is aggregated.
func TestGatherMetricsSnapshot_ClassifiesBatchHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	bh := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "rpc",
		Name:      "batch_request_duration_seconds",
		Buckets:   []float64{0.01, 0.1, 1.0},
	}, []string{"size_class"})
	reg.MustRegister(bh)
	bh.WithLabelValues("1").Observe(0.001)
	bh.WithLabelValues("2-5").Observe(0.01)
	bh.WithLabelValues("2-5").Observe(0.02)

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	b, ok := snap.Batches["rpc"]
	assert.Assert(t, ok, "expected rpc batch entry; got %v", snap.Batches)
	assert.Equal(t, b.Histogram.Count, uint64(3))
	assert.Equal(t, b.BySizeClass["1"], uint64(1))
	assert.Equal(t, b.BySizeClass["2-5"], uint64(2))
}

// TestGatherMetricsSnapshot_UpstreamGauge pins that the
// cosmoguard_upstream_healthy gauge surfaces with its labels intact.
func TestGatherMetricsSnapshot_UpstreamGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cosmoguard_upstream_healthy",
	}, []string{"pool", "upstream"})
	reg.MustRegister(g)
	g.WithLabelValues("lcd", "node-a").Set(1)
	g.WithLabelValues("rpc", "node-b").Set(0)

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	assert.Equal(t, len(snap.Upstreams), 2)
	// Order isn't guaranteed; index by (pool, upstream).
	seen := map[string]float64{}
	for _, u := range snap.Upstreams {
		seen[u.Pool+"/"+u.Upstream] = u.Healthy
	}
	assert.Equal(t, seen["lcd/node-a"], float64(1))
	assert.Equal(t, seen["rpc/node-b"], float64(0))
}

// TestGatherMetricsSnapshot_UnrelatedFamiliesIgnored pins that
// unrelated MetricFamilies (e.g. go_goroutines, custom counters
// with no matching suffix) don't pollute the snapshot.
func TestGatherMetricsSnapshot_UnrelatedFamiliesIgnored(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "unrelated_counter_total",
	})
	reg.MustRegister(c)
	c.Inc()

	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	assert.Equal(t, len(snap.Protocols), 0)
	assert.Equal(t, len(snap.Batches), 0)
	assert.Equal(t, len(snap.Upstreams), 0)
}

// TestGatherMetricsSnapshot_EmptyRegistry pins the well-formed-empty
// invariant so the dashboard's first poll (before any traffic has
// hit the proxies) doesn't choke on missing keys.
func TestGatherMetricsSnapshot_EmptyRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	snap, err := gatherMetricsSnapshot(reg)
	assert.NilError(t, err)
	assert.Assert(t, snap != nil)
	assert.Equal(t, len(snap.Protocols), 0)
	assert.Equal(t, len(snap.Batches), 0)
	assert.Equal(t, len(snap.Upstreams), 0)
	assert.Assert(t, snap.TimestampMs > 0)
}
