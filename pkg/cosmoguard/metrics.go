package cosmoguard

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// responseTimeBuckets is the shared histogram bucket spec for every
// per-request response-time histogram (HTTP / JSON-RPC single & batch
// / WS). Extends the legacy Prometheus default low end down to 100µs
// so cache hits don't all collapse to "<=0.005" — the previous floor
// at 5ms made p50 read as "5ms" for cached endpoints whose real value
// is 0.3-2ms (Redis hits) or sub-100µs (in-memory hits). Keeps the
// upper end out to 10s for slow upstreams.
var responseTimeBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025,
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// upstreamHealthyGauge exposes a per-upstream 0/1 gauge so operators can
// see pool state from /metrics even when traffic is idle. Without this
// the only signal that an upstream is healthy is the per-request
// histogram's `upstream` label — useless when nothing is currently
// flowing. Labels:
//   - pool: the proxy this upstream belongs to ("lcd", "rpc", "grpc",
//     "evm_rpc", "evm_rpc_ws"). Bounded, comes from the proxy
//     constructor.
//   - upstream: the node's configured name. Bounded, comes from
//     NodeConfig.
//
// Value semantics: 1 when the active healthcheck's last verdict was
// healthy (or when no healthcheck is configured — the optimistic
// default). 0 when the healthcheck has flipped the upstream out of the
// picker after UnhealthyAfter consecutive failures. Circuit-breaker
// state is NOT folded in — operators wanting that signal can use a
// separate metric in a future slice.
var upstreamHealthyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cosmoguard_upstream_healthy",
	Help: "1 when the upstream is in the pool's healthy set, 0 otherwise. Pool + upstream identify the node.",
}, []string{"pool", "upstream"})

// cacheEvictionsCounter counts entries dropped from an in-process (L1)
// response cache because it hit its byte/item budget (issue #15) — NOT
// ordinary TTL expiry. A rising rate is the operator's signal that the L1
// budget is undersized for the workload (raise cache.memory.maxBytes or the
// pod's memory limit). Labelled by `cache` (the proxy name).
var cacheEvictionsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cosmoguard_cache_evictions_total",
	Help: "Entries evicted from an in-process response cache because it hit its byte/item budget (excludes TTL expiry), by cache.",
}, []string{"cache"})

// registerSharedMetricsOnce guards the process-wide registration so
// multiple cosmoguards in one process (the test harness builds several)
// don't panic on duplicate Register.
var registerSharedMetricsOnce sync.Once

// registerSharedMetrics is idempotent — safe to call from every pool
// constructor. The gauge itself is package-global so all pools push to
// the same metric, with `pool` distinguishing them.
func registerSharedMetrics() {
	registerSharedMetricsOnce.Do(func() {
		// Register (not MustRegister) so a re-registration from another
		// cosmoguard instance is a no-op rather than a panic.
		_ = prometheus.Register(upstreamHealthyGauge)
		_ = prometheus.Register(cacheEvictionsCounter)
	})
}

// recordCacheEviction bumps the eviction counter for the named cache. Wired
// into the L1 cache via cache.OnEvict in newResponseCache.
func recordCacheEviction(name string) {
	cacheEvictionsCounter.WithLabelValues(name).Inc()
}

// setUpstreamHealthy is a small wrapper so the call site reads cleanly
// at the probe-result update points in each pool.
func setUpstreamHealthy(pool, upstream string, healthy bool) {
	if pool == "" || upstream == "" {
		return
	}
	v := 0.0
	if healthy {
		v = 1
	}
	upstreamHealthyGauge.WithLabelValues(pool, upstream).Set(v)
}

// deleteUpstreamHealthy removes the gauge series for an upstream that
// no longer exists in the pool. Called from RemoveUpstream so an
// auto-scaled-down pod doesn't permanently haunt /metrics as an
// unhealthy upstream — without this, Prometheus would keep scraping
// the stale label set forever (the gauge value of 0 is correct, but
// the time series itself shouldn't exist anymore once the upstream
// is gone). Idempotent and no-op for empty labels.
func deleteUpstreamHealthy(pool, upstream string) {
	if pool == "" || upstream == "" {
		return
	}
	upstreamHealthyGauge.DeleteLabelValues(pool, upstream)
}
