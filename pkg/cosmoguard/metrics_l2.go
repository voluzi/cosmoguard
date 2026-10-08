package cosmoguard

import (
	"runtime/metrics"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var l2StorageRejections = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cosmoguard_l2_storage_rejections_total", Help: "Receiving response storage capacity rejections."}, []string{"path"})
var l2ImportDrops = prometheus.NewCounter(prometheus.CounterOpts{Name: "cosmoguard_l2_import_dropped_entries_total", Help: "Response entries omitted on capacity during acknowledged import."})
var l2WriteSkips = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cosmoguard_l2_write_skips_total", Help: "Skipped response L2 insertions by known cause."}, []string{"reason"})

func recordL2StorageRejection(path string) {
	if path == "import_drop" {
		l2ImportDrops.Inc()
		return
	}
	l2StorageRejections.WithLabelValues(path).Inc()
}

type l2Collector struct {
	mu       sync.Mutex
	runtimes map[*clusterRuntime]bool
	descs    []*prometheus.Desc
}

var l2Metrics = newL2Collector()

func newL2Collector() *l2Collector {
	c := &l2Collector{runtimes: map[*clusterRuntime]bool{}}
	for _, name := range []string{"storage_allocated_bytes", "storage_inuse_bytes", "storage_entries", "storage_capacity_bytes", "codec_bytes", "codec_capacity_bytes"} {
		c.descs = append(c.descs, prometheus.NewDesc("cosmoguard_l2_"+name, "Active runtime "+name+"; capacity zero means unlimited.", []string{"pool"}, nil))
	}
	c.descs = append(c.descs, prometheus.NewDesc("cosmoguard_l2_operation_bytes", "Active response operation reservations.", nil, nil), prometheus.NewDesc("cosmoguard_l2_operation_capacity_bytes", "Active response operation capacities.", nil, nil))
	c.descs = append(c.descs,
		prometheus.NewDesc("cosmoguard_gc_cpu_seconds_total", "Process GC CPU seconds.", nil, nil),
		prometheus.NewDesc("cosmoguard_gc_limiter_last_enabled_cycle", "Last GC cycle that enabled the runtime limiter.", nil, nil))
	return c
}
func (c *l2Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}
func (c *l2Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	runtimes := make([]*clusterRuntime, 0, len(c.runtimes))
	for cr := range c.runtimes {
		runtimes = append(runtimes, cr)
	}
	c.mu.Unlock()
	for _, pool := range []string{"response", "security"} {
		var values [6]uint64
		for _, cr := range runtimes {
			p := cr.responsePool
			if pool == "security" {
				p = cr.securityPool
			}
			s := p.Snapshot()
			v := [6]uint64{s.Allocated, s.Inuse, s.Entries, s.Capacity, s.Codec.Reserved, s.Codec.Limit}
			for i, n := range v {
				values[i] += n
			}
		}
		for i, n := range values {
			ch <- prometheus.MustNewConstMetric(c.descs[i], prometheus.GaugeValue, float64(n), pool)
		}
	}
	var reserved, capacity uint64
	for _, cr := range runtimes {
		r, n := cr.responseOperations.OperationBytes()
		reserved += r
		capacity += n
	}
	ch <- prometheus.MustNewConstMetric(c.descs[6], prometheus.GaugeValue, float64(reserved))
	ch <- prometheus.MustNewConstMetric(c.descs[7], prometheus.GaugeValue, float64(capacity))
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/limiter/last-enabled:gc-cycle"}}
	metrics.Read(samples)
	ch <- prometheus.MustNewConstMetric(c.descs[8], prometheus.CounterValue, samples[0].Value.Float64())
	ch <- prometheus.MustNewConstMetric(c.descs[9], prometheus.GaugeValue, float64(samples[1].Value.Uint64()))

}
func addL2Metrics(cr *clusterRuntime) func() {
	registerSharedMetrics()
	l2Metrics.mu.Lock()
	l2Metrics.runtimes[cr] = true
	l2Metrics.mu.Unlock()
	return func() { l2Metrics.mu.Lock(); delete(l2Metrics.runtimes, cr); l2Metrics.mu.Unlock() }
}
