package cosmoguard

import (
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"gotest.tools/assert"
)

// splitHostPortInt parses an httptest server URL ("http://127.0.0.1:NNNN")
// into a (host, port) pair so NodeConfig consumers can plug the port
// directly into LcdPort/RpcPort.
func splitHostPortInt(t *testing.T, urlStr string) (string, int) {
	t.Helper()
	urlStr = strings.TrimPrefix(urlStr, "http://")
	h, p, err := net.SplitHostPort(urlStr)
	assert.NilError(t, err)
	port, err := strconv.Atoi(p)
	assert.NilError(t, err)
	return h, port
}

// TestUpstreamHealthyGauge_TransitionsTrackProbeVerdict proves the gauge
// flips with healthcheck state — that's the value-add over the per-
// request histograms, which can't observe an idle pool.
//
// The package-global gauge is shared across all tests in this package
// so we use unique pool + upstream label combinations to keep this test
// isolated from anything else updating the gauge concurrently.
func TestUpstreamHealthyGauge_TransitionsTrackProbeVerdict(t *testing.T) {
	const pool = "test_pool_transitions"
	const up = "test_up"

	// Initial seed (matches what NewHttpUpstreamPool does on construct).
	setUpstreamHealthy(pool, up, true)
	g, err := upstreamHealthyGauge.GetMetricWithLabelValues(pool, up)
	assert.NilError(t, err)
	assert.Equal(t, testutil.ToFloat64(g), 1.0)

	// Healthcheck flips to unhealthy after UnhealthyAfter probes — gauge
	// must follow.
	setUpstreamHealthy(pool, up, false)
	assert.Equal(t, testutil.ToFloat64(g), 0.0)

	// Recovery flips it back.
	setUpstreamHealthy(pool, up, true)
	assert.Equal(t, testutil.ToFloat64(g), 1.0)
}

// TestUpstreamHealthyGauge_EmptyLabelsIgnored guards against accidental
// emission of a metric with empty labels — which would otherwise create
// `{pool="", upstream="..."}` or `{pool="...", upstream=""}` series in
// /metrics that no operator could interpret. Asserts on the exact label
// pairs the no-op guard suppresses, so removing the guard would flip
// these series from absent (0.0) to set (1.0) and the test would fail.
func TestUpstreamHealthyGauge_EmptyLabelsIgnored(t *testing.T) {
	setUpstreamHealthy("", "empty-pool-probe", true)
	setUpstreamHealthy("empty-up-probe", "", true)

	emptyPool, err := upstreamHealthyGauge.GetMetricWithLabelValues("", "empty-pool-probe")
	assert.NilError(t, err)
	assert.Equal(t, testutil.ToFloat64(emptyPool), 0.0,
		`setUpstreamHealthy("", "empty-pool-probe", true) must be a no-op`)

	emptyUp, err := upstreamHealthyGauge.GetMetricWithLabelValues("empty-up-probe", "")
	assert.NilError(t, err)
	assert.Equal(t, testutil.ToFloat64(emptyUp), 0.0,
		`setUpstreamHealthy("empty-up-probe", "", true) must be a no-op`)
}

// TestUpstreamHealthyGauge_SeededOnPoolConstruct proves a freshly built
// HttpUpstreamPool emits gauge=1 for every upstream before any probe
// runs. This is the "Prometheus sees the pool from boot" guarantee
// operators rely on — without it, a quiet replica would appear absent
// in dashboards until the first request landed.
func TestUpstreamHealthyGauge_SeededOnPoolConstruct(t *testing.T) {
	// Unique-per-test upstream names so we don't collide with other
	// tests in this package that build pools labelled pool="lcd".
	nodes := []NodeConfig{
		{Name: "gaugeseed-a", Host: "127.0.0.1", LcdPort: 1, RpcPort: 1, GrpcPort: 1},
		{Name: "gaugeseed-b", Host: "127.0.0.1", LcdPort: 1, RpcPort: 1, GrpcPort: 1},
	}
	logger := log.WithField("test", "seed")
	pool, err := NewHttpUpstreamPool(nodes, "lcd", nil, logger)
	assert.NilError(t, err)
	defer pool.Shutdown()

	for _, n := range nodes {
		g, err := upstreamHealthyGauge.GetMetricWithLabelValues("lcd", n.Name)
		assert.NilError(t, err)
		assert.Equal(t, testutil.ToFloat64(g), 1.0, "upstream %s should be seeded as healthy on construct", n.Name)
	}
}

// TestUpstreamHealthyGauge_RemoveCleansSeriesUnderHealthchecks is the
// regression pin for the in-cluster stale-gauge bug: a constructor-
// seeded upstream (discovery-expanded ones land here) whose
// healthcheck goroutine probes a dead IP after RemoveUpstream used
// to keep re-publishing `cosmoguard_upstream_healthy=0` for the
// already-removed upstream via setUpstreamHealthy on probe failure.
// Root cause was a missing per-upstream stop channel for constructor-
// seeded upstreams — RemoveUpstream's close was no-op'd, the
// goroutine leaked, the gauge series came back.
//
// The fix gives every upstream (constructor-seeded or AddUpstream-
// added) a stop channel at build time. This test exercises the full
// chain: stand up a healthcheck against a probe server that returns
// 500 (so the goroutine will want to call setUpstreamHealthy(..., false)
// on every tick), remove the upstream, then sleep past several
// healthcheck intervals and assert the series stays absent.
func TestUpstreamHealthyGauge_RemoveCleansSeriesUnderHealthchecks(t *testing.T) {
	// A probe target that 500s on every request so the leaked
	// healthcheck goroutine (if the bug regressed) would try to mark
	// the upstream unhealthy. The pre-fix bug then re-publishes the
	// gauge series via setUpstreamHealthy.
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer probe.Close()

	probeHost, probePort := splitHostPortInt(t, probe.URL)

	hcEnabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/",
		Service:        "lcd",
		Interval:       20 * time.Millisecond,
		Timeout:        50 * time.Millisecond,
		UnhealthyAfter: 1, // flip on the first failure to make the bug fire fast
		HealthyAfter:   1,
	}
	const poolLabel = "lcd_remove_cleanup_test"
	const upstreamLabel = "gaugeremove-a"
	node := NodeConfig{
		Name:        upstreamLabel,
		Host:        probeHost,
		LcdPort:     probePort,
		RpcPort:     probePort,
		GrpcPort:    1,
		Healthcheck: hc,
	}

	pool, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "gauge-remove"), WithPoolName(poolLabel))
	assert.NilError(t, err)
	pool.StartHealthchecks()

	// Drop the upstream while healthchecks are running.
	pool.RemoveUpstream(upstreamLabel)

	// Sleep longer than several probe intervals so a leaked goroutine
	// (the bug) would have multiple chances to re-publish the series.
	time.Sleep(150 * time.Millisecond)

	// The series for the removed upstream must be absent from the gauge.
	// Gather all collected series and assert none match our labels.
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "cosmoguard_upstream_healthy" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var p, u string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "pool":
					p = lp.GetValue()
				case "upstream":
					u = lp.GetValue()
				}
			}
			if p == poolLabel && u == upstreamLabel {
				t.Fatalf("stale gauge series re-appeared after RemoveUpstream: pool=%q upstream=%q value=%v",
					p, u, m.GetGauge().GetValue())
			}
		}
	}

	pool.Shutdown()
}

// TestUpstreamHealthyGauge_RemoveBeforeStartHealthchecksIsFast pins the
// probeStarted gate. Before that gate existed, RemoveUpstream's done-wait
// fired whenever hcConfig != nil — including the pre-StartHealthchecks
// path where no goroutine was ever spawned, so the `done` channel never
// closes and the caller burned the full 5s timeout. With the gate the
// wait is skipped when no probe goroutine has started, so Remove returns
// in well under a millisecond.
func TestUpstreamHealthyGauge_RemoveBeforeStartHealthchecksIsFast(t *testing.T) {
	hcEnabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/",
		Service:        "lcd",
		Interval:       1 * time.Second,
		Timeout:        50 * time.Millisecond,
		UnhealthyAfter: 1,
		HealthyAfter:   1,
	}
	const upstreamLabel = "gauge-fast-remove"
	node := NodeConfig{
		Name:        upstreamLabel,
		Host:        "127.0.0.1",
		LcdPort:     1, // unreachable on purpose; healthchecks never start
		RpcPort:     1,
		GrpcPort:    1,
		Healthcheck: hc,
	}

	pool, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "gauge-fast-remove"), WithPoolName("lcd_fast_remove_test"))
	assert.NilError(t, err)
	// Deliberately NOT calling pool.StartHealthchecks() — so the
	// healthcheck goroutine for this upstream is never spawned and
	// probeStarted stays false.

	start := time.Now()
	pool.RemoveUpstream(upstreamLabel)
	elapsed := time.Since(start)

	// 250ms is generous (the actual call should be sub-millisecond);
	// the regression we're guarding against blocks for 5s.
	if elapsed > 250*time.Millisecond {
		t.Fatalf("RemoveUpstream blocked for %s on the done-wait of a never-spawned probe; "+
			"probeStarted gate is missing or broken", elapsed)
	}
	pool.Shutdown()
}

// TestUpstreamHealthyGauge_ShutdownDrainsConcurrentStarts pins the
// sync.WaitGroup Add/Wait race fix. Pre-fix, Shutdown could observe
// hcCancel != nil (set by StartHealthchecks under hcMu), call
// hcWG.Wait() with counter=0 — because StartHealthchecks hadn't yet
// reached its hcWG.Add(1) call — and return prematurely while a probe
// goroutine was still about to spawn. The fix adds an hcSpawning
// barrier that Shutdown waits on before hcWG.Wait(), so every Add
// the spawn loop will emit has landed.
//
// Two things must hold after Shutdown returns:
//  1. The race detector (under -race) must not flag the Add/Wait pair.
//     The pre-fix code triggers the race because Add(1) is not ordered
//     with Wait() when counter==0; the fix establishes happens-before
//     via hcSpawning.
//  2. No goroutine spawned by StartHealthchecks may outlive Shutdown.
//     We check via runtime.NumGoroutine — after a settling window,
//     the count must return to baseline.
func TestUpstreamHealthyGauge_ShutdownDrainsConcurrentStarts(t *testing.T) {
	// 500ing probe so any leaked goroutine's initial probe lands fast
	// and would be observable on /metrics if probeAborted didn't catch
	// it. The server is also a soft latency floor — keeps the probe
	// in-flight long enough to overlap Shutdown's cancel().
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer probe.Close()
	probeHost, probePort := splitHostPortInt(t, probe.URL)

	hcEnabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/",
		Service:        "lcd",
		Interval:       1 * time.Millisecond,
		Timeout:        50 * time.Millisecond,
		UnhealthyAfter: 1,
		HealthyAfter:   1,
	}
	node := NodeConfig{
		Name: "gauge-shutdown-drain", Host: probeHost,
		LcdPort: probePort, RpcPort: probePort, GrpcPort: 1,
		Healthcheck: hc,
	}

	// Baseline goroutine count, captured AFTER one warm-up cycle so any
	// lazy stdlib goroutines (http.DefaultTransport, etc.) are already
	// running and don't pollute the diff.
	{
		warm, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "warm"))
		assert.NilError(t, err)
		warm.StartHealthchecks()
		warm.Shutdown()
	}
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Hammer Start/Shutdown concurrently. 100 iterations is enough to
	// surface the race window on -race; with the fix in place, the
	// counter is properly ordered and the race detector stays silent.
	for i := 0; i < 100; i++ {
		pool, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "shutdown-drain"))
		assert.NilError(t, err)

		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			close(started)
			pool.StartHealthchecks()
			close(done)
		}()
		<-started
		pool.Shutdown()
		<-done
		// NOTE: a single Shutdown is sufficient. With the hcShutdown
		// sentinel in place, if Shutdown wins the hcMu race against
		// StartHealthchecks, StartHealthchecks observes hcShutdown=true
		// and bails without spawning any probes — so no leaked
		// goroutine survives this iteration. The earlier version of
		// this test called Shutdown twice to mop up that leak, which
		// would have masked a missing sentinel; deliberately not doing
		// that anymore.
	}

	// Settle: give any leaked goroutine a chance to surface in
	// runtime.NumGoroutine.
	time.Sleep(200 * time.Millisecond)
	final := runtime.NumGoroutine()
	// Allow a small slack — Go's runtime can spin up background workers
	// (GC, netpoll) during the test that aren't related to the pool.
	// goleak.VerifyTestMain in the package is the real safety net; this
	// threshold catches obvious 100x leaks early in the failure stream.
	if final-baseline > 10 {
		t.Fatalf("goroutine leak after concurrent Start/Shutdown: baseline=%d final=%d (delta=%d); "+
			"hcSpawning barrier or hcShutdown sentinel is broken",
			baseline, final, final-baseline)
	}
}

// TestUpstreamHealthyGauge_ShutdownBeforeStartIsCleanNoLeak is the
// regression pin for the S-1 audit finding: when Shutdown takes hcMu
// before StartHealthchecks ever does (the SIGTERM-during-fast-startup
// scenario operators see during rolling deploys or crashloops), the
// pre-fix code observed hcCancel == nil, returned immediately, and let
// StartHealthchecks proceed to set a fresh hcCancel and spawn probes
// that nobody would ever cancel — leaking them past pool teardown.
//
// The fix is the hcShutdown sentinel: Shutdown latches it under hcMu;
// a subsequent StartHealthchecks reads it under hcMu and bails before
// spawning anything. This test forces the "Shutdown first" interleave
// by calling Shutdown before StartHealthchecks ever runs, then asserts
// no probe goroutine exists — verified by NumGoroutine settling back
// to baseline. goleak.VerifyTestMain in the package is the backstop.
func TestUpstreamHealthyGauge_ShutdownBeforeStartIsCleanNoLeak(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer probe.Close()
	probeHost, probePort := splitHostPortInt(t, probe.URL)

	hcEnabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/",
		Service:        "lcd",
		Interval:       1 * time.Millisecond,
		Timeout:        50 * time.Millisecond,
		UnhealthyAfter: 1,
		HealthyAfter:   1,
	}
	node := NodeConfig{
		Name:        "shutdown-before-start",
		Host:        probeHost,
		LcdPort:     probePort,
		RpcPort:     probePort,
		GrpcPort:    1,
		Healthcheck: hc,
	}

	// Warm-up so lazy stdlib goroutines don't pollute the diff.
	{
		warm, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "warm-shutdown-before"))
		assert.NilError(t, err)
		warm.StartHealthchecks()
		warm.Shutdown()
	}
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Two interleavings exercised in tandem, each iteration:
	//   1. Serialized: Shutdown(); StartHealthchecks(). Pre-fix this
	//      leaks because the late Start finds hcCancel == nil and
	//      proceeds to spawn probes nobody can cancel.
	//   2. Concurrent: StartHealthchecks() in a goroutine racing
	//      Shutdown() on the main goroutine. This is the *real*
	//      SIGTERM-during-fast-startup interleave — Shutdown takes
	//      hcMu in between StartHealthchecks's entry and its hcMu
	//      acquisition. The hcShutdown latch is the ordering the
	//      late Start must observe under the same lock.
	// Running both forms in every iteration keeps the test robust
	// against schedule jitter: at least one of the two reliably hits
	// the targeted window on any given run. 100 iterations make a
	// real leak (one goroutine per iteration) impossible to hide in
	// NumGoroutine noise. settle below is bounded by hc.Timeout (50ms)
	// because any in-flight probe must return within that.
	for i := 0; i < 100; i++ {
		// Serialized form.
		serial, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "shutdown-before-start"))
		assert.NilError(t, err)
		serial.Shutdown()
		serial.StartHealthchecks()

		// Concurrent form: race the two calls. BOTH calls run on
		// goroutines so the scheduler can interleave them — running one
		// on the main goroutine pinned Shutdown to fire first every time
		// because the `<-started` rendezvous gates only on the spawn,
		// not on the StartHealthchecks entry. Two extra Gosched()s and
		// a barrier give the scheduler a real opportunity to land
		// Shutdown in between StartHealthchecks's function entry and
		// its hcMu.Lock(). Across 100 iters with GOMAXPROCS>=2 both
		// interleavings ("Start wins / Shutdown wins the lock") are
		// hit reliably; with -race a torn read/write on hcShutdown or
		// hcCancel would surface.
		race, err := NewHttpUpstreamPool([]NodeConfig{node}, "lcd", nil, log.WithField("test", "shutdown-race-start"))
		assert.NilError(t, err)
		var wg sync.WaitGroup
		barrier := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-barrier
			runtime.Gosched()
			race.StartHealthchecks()
		}()
		go func() {
			defer wg.Done()
			<-barrier
			runtime.Gosched()
			race.Shutdown()
		}()
		close(barrier)
		wg.Wait()
	}

	// 200ms is comfortably > hc.Timeout (50ms) so any in-flight probe
	// has finished and any leaked goroutine is countable.
	time.Sleep(200 * time.Millisecond)
	final := runtime.NumGoroutine()
	if final-baseline > 10 {
		t.Fatalf("hcShutdown sentinel leaked goroutines on Shutdown-before/racing-Start: "+
			"baseline=%d final=%d (delta=%d)", baseline, final, final-baseline)
	}
}
