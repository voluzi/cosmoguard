package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"gotest.tools/assert"
)

func newTestGrpcUpstream(name string, weight int) *GrpcUpstream {
	u := &GrpcUpstream{Name: name, Target: "fake:0", Weight: weight}
	return u
}

// newTestGrpcPool builds a GrpcUpstreamPool around a pre-built set of
// upstreams. Tests use this because the pool's `upstreams` field is an
// atomic.Pointer that the picker reads lock-free — writers must
// publish through storeUpstreams so reads see a consistent slice header.
func newTestGrpcPool(strategy string, ups ...*GrpcUpstream) *GrpcUpstreamPool {
	p := &GrpcUpstreamPool{strategy: strategy}
	p.storeUpstreams(ups)
	return p
}

func TestGrpcPickerWeightedRR(t *testing.T) {
	a := newTestGrpcUpstream("a", 3)
	b := newTestGrpcUpstream("b", 1)
	pool := newTestGrpcPool("weighted-round-robin", a, b)
	// healthySet falls back to all when none are healthy (we never
	// dialled), so the picker still rotates by weight.
	hits := map[string]int{}
	for i := 0; i < 40; i++ {
		hits[pool.Pick().Name]++
	}
	if hits["a"] < 2*hits["b"] {
		t.Fatalf("grpc weighted RR: a=%d b=%d", hits["a"], hits["b"])
	}
}

func TestGrpcPickerLeastConn(t *testing.T) {
	a := newTestGrpcUpstream("a", 1)
	b := newTestGrpcUpstream("b", 1)
	c := newTestGrpcUpstream("c", 1)
	a.inFlight.Store(7)
	b.inFlight.Store(2)
	c.inFlight.Store(5)

	pool := newTestGrpcPool("least-conn", a, b, c)
	for i := 0; i < 5; i++ {
		got := pool.Pick()
		if got != b {
			t.Fatalf("grpc least-conn iter %d: got %s want b", i, got.Name)
		}
	}
}

func TestGrpcPickerSingleUpstreamFastPath(t *testing.T) {
	a := newTestGrpcUpstream("a", 1)
	pool := newTestGrpcPool("weighted-round-robin", a)
	if pool.Pick() != a {
		t.Fatal("single-upstream pool should always return that upstream")
	}
}

// TestGrpcPool_CloseBeforeStartIsCleanNoLeak is the gRPC analog of the
// HTTP-pool TestUpstreamHealthyGauge_ShutdownBeforeStartIsCleanNoLeak.
// Matters more here than on the HTTP pool because GrpcUpstreamPool.Close
// is wrapped in sync.Once — a second Close cannot mop up a leaked probe
// goroutine, so the hcShutdown sentinel is the only mechanism that keeps
// a Shutdown-during-fast-startup interleave clean.
func TestGrpcPool_CloseBeforeStartIsCleanNoLeak(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer probe.Close()
	probeHost, probePort := splitHostPortInt(t, probe.URL)

	hcEnabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/",
		Service:        "rpc",
		Interval:       1 * time.Millisecond,
		Timeout:        50 * time.Millisecond,
		UnhealthyAfter: 1,
		HealthyAfter:   1,
	}
	// grpc.NewClient is lazy — no connection is established until an RPC
	// is dispatched — so an unreachable GrpcPort is fine. The probe rides
	// the HTTP RPC path, which is what we point at the httptest server.
	node := NodeConfig{
		Name:        "grpc-close-before-start",
		Host:        probeHost,
		LcdPort:     probePort,
		RpcPort:     probePort,
		GrpcPort:    1,
		Healthcheck: hc,
	}

	// Warm-up to settle lazy stdlib + gRPC goroutines.
	{
		warm, err := NewGrpcUpstreamPool("grpc", []NodeConfig{node}, log.WithField("test", "warm-grpc"))
		assert.NilError(t, err)
		warm.StartHealthchecks()
		warm.Close()
	}
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Same two-interleaving pattern as the HTTP-pool regression test —
	// the serialized form catches a missing latch; the concurrent form
	// exercises the actual SIGTERM-during-fast-startup race where
	// Close takes hcMu between StartHealthchecks's entry and its
	// hcMu acquisition.
	for i := 0; i < 50; i++ {
		// Serialized.
		serial, err := NewGrpcUpstreamPool("grpc", []NodeConfig{node}, log.WithField("test", "grpc-close-before-start"))
		assert.NilError(t, err)
		serial.Close()
		serial.StartHealthchecks()

		// Concurrent — race Close against StartHealthchecks.
		race, err := NewGrpcUpstreamPool("grpc", []NodeConfig{node}, log.WithField("test", "grpc-close-race-start"))
		assert.NilError(t, err)
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			close(started)
			race.StartHealthchecks()
			close(done)
		}()
		<-started
		race.Close()
		<-done
	}

	// 300ms is comfortably > hc.Timeout (50ms) so any in-flight probe
	// has returned and any leaked goroutine is countable.
	time.Sleep(300 * time.Millisecond)
	final := runtime.NumGoroutine()
	if final-baseline > 15 {
		t.Fatalf("hcShutdown sentinel leaked goroutines on Close-before-Start in gRPC pool: "+
			"baseline=%d final=%d (delta=%d)", baseline, final, final-baseline)
	}
}

// TestBuildGrpcUpstream_NormalizesUnslashedHealthcheckPath is the
// regression test for the gRPC probe-URL bug: a healthcheck path without
// a leading slash ("status") must produce a valid "http://host:port/status"
// probe URL, not the string-concatenated "http://host:portstatus" that
// fails parsing and falsely marks the gRPC pool unhealthy.
func TestBuildGrpcUpstream_NormalizesUnslashedHealthcheckPath(t *testing.T) {
	hcEnabled := true
	node := NodeConfig{
		Name:     "g",
		Host:     "node-a",
		RpcPort:  26657,
		GrpcPort: 1,
		Healthcheck: &NodeHealthcheckConfig{
			Enable:         &hcEnabled,
			Path:           "status", // no leading slash
			Service:        "rpc",
			Interval:       time.Second,
			Timeout:        time.Second,
			UnhealthyAfter: 1,
			HealthyAfter:   1,
		},
	}
	u, err := buildGrpcUpstream(node)
	if err != nil {
		t.Fatalf("buildGrpcUpstream: %v", err)
	}
	t.Cleanup(func() { _ = u.conn.Close() })

	if u.probeAddr != "http://node-a:26657/status" {
		t.Fatalf("unslashed healthcheck path must normalize to http://node-a:26657/status, got %q", u.probeAddr)
	}
}

// TestHealthcheckProbeURL pins the shared normalizer used by both pool
// builders: leading-slash insertion, query-string splitting, and the
// empty-path root case.
func TestHealthcheckProbeURL(t *testing.T) {
	cases := []struct{ scheme, host, path, want string }{
		{"http", "h:1", "status", "http://h:1/status"},
		{"http", "h:1", "/status", "http://h:1/status"},
		{"https", "h:1", "/health?verbose=1", "https://h:1/health?verbose=1"},
		{"http", "h:1", "", "http://h:1"},
	}
	for _, c := range cases {
		if got := healthcheckProbeURL(c.scheme, c.host, c.path); got != c.want {
			t.Fatalf("healthcheckProbeURL(%q,%q,%q) = %q, want %q", c.scheme, c.host, c.path, got, c.want)
		}
	}
}
