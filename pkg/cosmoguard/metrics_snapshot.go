package cosmoguard

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// MetricsSnapshot is the JSON-friendly shape returned by /api/v1/metrics.
// All cumulative values (counts, histogram counts) are raw counters; the
// dashboard client computes rates by differencing successive snapshots.
type MetricsSnapshot struct {
	// TimestampMs is the snapshot's wall-clock at the moment the
	// Gatherer.Gather() call returned. Used by the client to compute
	// dt across polls.
	TimestampMs int64 `json:"timestamp_ms"`
	// Protocols keys by the per-proxy namespace ("lcd", "rpc",
	// "evm_rpc", "websocket_rpc", etc.). One entry per request
	// histogram family the registry advertises.
	Protocols map[string]ProtocolMetrics `json:"protocols"`
	// Batches keys by the JSON-RPC handler namespace (matching the
	// Protocols key the handler advertises under
	// "<name>_request_duration_seconds"). Empty when no batch
	// histogram has fired.
	Batches map[string]BatchMetrics `json:"batches"`
	// Upstreams is the per-pool / per-upstream healthy gauge —
	// 1 healthy, 0 unhealthy. Matches the gauge series in /metrics.
	Upstreams []UpstreamHealth `json:"upstreams"`
}

// ProtocolMetrics aggregates one request-time histogram family by every
// label dimension the per-request observation carries. The client
// derives `cache_hit_rate = by_cache["hit"] / sum(by_cache)`,
// `error_rate = (by_status["4xx"]+by_status["5xx"]) / histogram.count`,
// etc. — all from the deltas across polls.
type ProtocolMetrics struct {
	Histogram  HistogramView     `json:"histogram"`
	ByCache    map[string]uint64 `json:"by_cache,omitempty"`
	ByAction   map[string]uint64 `json:"by_action,omitempty"`
	ByStatus   map[string]uint64 `json:"by_status,omitempty"`
	ByUpstream map[string]uint64 `json:"by_upstream,omitempty"`
	ByRule     map[string]uint64 `json:"by_rule,omitempty"`
	ByMethod   map[string]uint64 `json:"by_method,omitempty"`
	// ByUpstreamHistogram is the full per-upstream latency view —
	// keyed by upstream label, value is the histogram for that
	// upstream's observations only. The dashboard derives per-upstream
	// P50/P95 from these so an operator can see "node-b is slow"
	// instead of only the aggregate which the slow upstream hides
	// behind the fast ones. The aggregate (Histogram above) stays
	// authoritative for the protocol-wide percentile chart.
	ByUpstreamHistogram map[string]HistogramView `json:"by_upstream_histogram,omitempty"`
}

// BatchMetrics is the JSON-RPC batch histogram + its size-class
// breakdown. Kept separate from ProtocolMetrics so the client can
// chart batch sizes without conflating them with the per-item
// request shape.
type BatchMetrics struct {
	Histogram   HistogramView     `json:"histogram"`
	BySizeClass map[string]uint64 `json:"by_size_class,omitempty"`
}

// HistogramView reproduces a Prometheus histogram with cumulative
// bucket counts so the client can compute bucket-delta counts (and
// from those, latency percentiles) without ever needing to scrape
// the raw /metrics endpoint.
type HistogramView struct {
	Sum     float64       `json:"sum"`
	Count   uint64        `json:"count"`
	Buckets []HistBucketV `json:"buckets,omitempty"`
}

// HistBucketV mirrors Prometheus' bucket boundary + cumulative count.
// Le is the upper bound (≤); Count is cumulative — all observations
// less than or equal to Le. The final +Inf bucket equals Count.
type HistBucketV struct {
	Le    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// UpstreamHealth mirrors one (pool, upstream) entry from the
// cosmoguard_upstream_healthy gauge.
type UpstreamHealth struct {
	Pool     string  `json:"pool"`
	Upstream string  `json:"upstream"`
	Healthy  float64 `json:"healthy"`
}

// histogramFamily / upstreamHealthyMetric / batch suffixes etc. are
// the family-name signposts metric-classification keys off. Kept as
// constants so a future rename in metrics.go has a single sync point.
const (
	requestHistogramSuffix      = "_request_duration_seconds"
	batchHistogramSuffix        = "_batch_request_duration_seconds"
	websocketProtocolPrefix     = "websocket_"
	upstreamHealthyMetricFamily = "cosmoguard_upstream_healthy"
)

// gatherMetricsSnapshot walks every MetricFamily exposed by g and
// produces the JSON-friendly snapshot. Stateless — the client
// computes time-series by differencing snapshots over its 5s poll
// cadence. Returns an error only if Gather() fails; an empty
// registry just yields an empty (but well-formed) snapshot.
func gatherMetricsSnapshot(g prometheus.Gatherer) (*MetricsSnapshot, error) {
	families, err := g.Gather()
	if err != nil {
		return nil, err
	}
	out := &MetricsSnapshot{
		TimestampMs: time.Now().UnixMilli(),
		Protocols:   map[string]ProtocolMetrics{},
		Batches:     map[string]BatchMetrics{},
		Upstreams:   []UpstreamHealth{},
	}
	for _, fam := range families {
		name := fam.GetName()
		switch {
		case name == upstreamHealthyMetricFamily:
			out.Upstreams = append(out.Upstreams, gatherUpstreamGauge(fam)...)
		case strings.HasSuffix(name, batchHistogramSuffix):
			// Batch family — derive the protocol slug by stripping
			// the suffix. JSON-RPC handlers register batch under
			// "<name>_batch_request_duration_seconds".
			slug := strings.TrimSuffix(name, batchHistogramSuffix)
			out.Batches[slug] = gatherBatchHistogram(fam)
		case strings.HasSuffix(name, requestHistogramSuffix):
			// Per-request histogram. Protocol slug strips the
			// suffix; the WS proxy uses a "websocket_" prefix on
			// the namespace, which we KEEP so the client can
			// distinguish a "websocket_rpc" stream from an HTTP
			// "rpc" stream that share the same handler namespace.
			slug := strings.TrimSuffix(name, requestHistogramSuffix)
			out.Protocols[slug] = gatherProtocolHistogram(fam)
		default:
			// Unrelated families (Go runtime, process, the
			// upstream-healthy gauge above) silently skipped —
			// adding them to the snapshot would balloon the JSON
			// payload without value for the dashboard.
		}
	}
	return out, nil
}

// gatherProtocolHistogram aggregates every (label-set) sample in a
// request-time histogram MetricFamily into a single ProtocolMetrics
// view, summing across all label combinations for the histogram
// view and accumulating per-label bucket counts for the by_X maps.
func gatherProtocolHistogram(fam *dto.MetricFamily) ProtocolMetrics {
	view := ProtocolMetrics{
		Histogram:           HistogramView{Buckets: []HistBucketV{}},
		ByCache:             map[string]uint64{},
		ByAction:            map[string]uint64{},
		ByStatus:            map[string]uint64{},
		ByUpstream:          map[string]uint64{},
		ByRule:              map[string]uint64{},
		ByMethod:            map[string]uint64{},
		ByUpstreamHistogram: map[string]HistogramView{},
	}
	bucketAgg := map[float64]uint64{}
	// upstreamHistograms accumulates per-upstream bucket maps in the
	// same shape as bucketAgg so the final pass through
	// sortedBucketCounts can keep buckets sorted ascending. Keyed by
	// upstream label value; nested map is Le → cumulative count.
	upstreamHistograms := map[string]*perUpstreamHist{}
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		view.Histogram.Sum += h.GetSampleSum()
		view.Histogram.Count += h.GetSampleCount()
		for _, b := range h.GetBucket() {
			bucketAgg[b.GetUpperBound()] += b.GetCumulativeCount()
		}
		cnt := h.GetSampleCount()
		// Find the upstream label up-front so we can pour this metric's
		// bucket counts into the right per-upstream histogram below.
		// Empty upstream label (the WS path observes upstream="" today;
		// see ws_proxy.go) collapses onto the empty-string key — the
		// dashboard can choose to filter it out.
		var upstream string
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "upstream" {
				upstream = lp.GetValue()
				break
			}
		}
		uh, ok := upstreamHistograms[upstream]
		if !ok {
			uh = &perUpstreamHist{buckets: map[float64]uint64{}}
			upstreamHistograms[upstream] = uh
		}
		uh.sum += h.GetSampleSum()
		uh.count += h.GetSampleCount()
		for _, b := range h.GetBucket() {
			uh.buckets[b.GetUpperBound()] += b.GetCumulativeCount()
		}

		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "cache":
				view.ByCache[lp.GetValue()] += cnt
			case "action":
				view.ByAction[lp.GetValue()] += cnt
			case "status_code":
				view.ByStatus[lp.GetValue()] += cnt
			case "upstream":
				view.ByUpstream[lp.GetValue()] += cnt
			case "rule_id":
				view.ByRule[lp.GetValue()] += cnt
			case "method":
				view.ByMethod[lp.GetValue()] += cnt
			}
		}
	}
	view.Histogram.Buckets = sortedBucketCounts(bucketAgg)
	for u, uh := range upstreamHistograms {
		view.ByUpstreamHistogram[u] = HistogramView{
			Sum:     uh.sum,
			Count:   uh.count,
			Buckets: sortedBucketCounts(uh.buckets),
		}
	}
	dropEmptyMaps(&view)
	return view
}

// perUpstreamHist accumulates the histogram triple (sum, count, buckets)
// for one upstream label value as we walk the metric family. Pulled out
// of gatherProtocolHistogram's body to keep the inner loop readable —
// otherwise we'd need a three-level map.
type perUpstreamHist struct {
	sum     float64
	count   uint64
	buckets map[float64]uint64
}

// gatherBatchHistogram is the batch-shape variant — same idea, but
// the only label we care about is size_class (the bounded
// jsonrpc_handler.go:194 enumeration).
func gatherBatchHistogram(fam *dto.MetricFamily) BatchMetrics {
	view := BatchMetrics{
		Histogram:   HistogramView{Buckets: []HistBucketV{}},
		BySizeClass: map[string]uint64{},
	}
	bucketAgg := map[float64]uint64{}
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		view.Histogram.Sum += h.GetSampleSum()
		view.Histogram.Count += h.GetSampleCount()
		for _, b := range h.GetBucket() {
			bucketAgg[b.GetUpperBound()] += b.GetCumulativeCount()
		}
		cnt := h.GetSampleCount()
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "size_class" {
				view.BySizeClass[lp.GetValue()] += cnt
			}
		}
	}
	view.Histogram.Buckets = sortedBucketCounts(bucketAgg)
	if len(view.BySizeClass) == 0 {
		view.BySizeClass = nil
	}
	return view
}

// gatherUpstreamGauge reads each (pool, upstream) sample of the
// cosmoguard_upstream_healthy gauge.
func gatherUpstreamGauge(fam *dto.MetricFamily) []UpstreamHealth {
	out := make([]UpstreamHealth, 0, len(fam.GetMetric()))
	for _, m := range fam.GetMetric() {
		g := m.GetGauge()
		if g == nil {
			continue
		}
		entry := UpstreamHealth{Healthy: g.GetValue()}
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "pool":
				entry.Pool = lp.GetValue()
			case "upstream":
				entry.Upstream = lp.GetValue()
			}
		}
		out = append(out, entry)
	}
	return out
}

// sortedBucketCounts returns the aggregated bucket map as a slice of
// (Le, Count) sorted by Le ascending. Prometheus emits the +Inf
// bucket as math.MaxFloat64 in some libraries; we keep it as-is so
// the client sees an ordinary sortable upper bound.
func sortedBucketCounts(m map[float64]uint64) []HistBucketV {
	if len(m) == 0 {
		return nil
	}
	out := make([]HistBucketV, 0, len(m))
	for le, c := range m {
		out = append(out, HistBucketV{Le: le, Count: c})
	}
	// Sort in place by Le ascending. Inlined to avoid pulling sort
	// into a hot path that the JSON encoder runs anyway.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Le > out[j].Le; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// dropEmptyMaps nils out by_X maps that received no samples, so the
// JSON payload uses omitempty cleanly — keeps the wire shape tidy
// for protocols that only observe a subset of labels.
func dropEmptyMaps(v *ProtocolMetrics) {
	if len(v.ByCache) == 0 {
		v.ByCache = nil
	}
	if len(v.ByAction) == 0 {
		v.ByAction = nil
	}
	if len(v.ByStatus) == 0 {
		v.ByStatus = nil
	}
	if len(v.ByUpstream) == 0 {
		v.ByUpstream = nil
	}
	if len(v.ByRule) == 0 {
		v.ByRule = nil
	}
	if len(v.ByMethod) == 0 {
		v.ByMethod = nil
	}
	if len(v.ByUpstreamHistogram) == 0 {
		v.ByUpstreamHistogram = nil
	}
}
