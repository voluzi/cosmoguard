package cosmoguard

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// newTestUpstream returns an HttpUpstream wired to a fixed reverse proxy.
// Healthy by default. Used by picker tests where the proxy implementation
// is irrelevant — only the picker behavior is under test.
func newTestUpstream(name string, weight int) *HttpUpstream {
	target, _ := url.Parse("http://" + name + ":1")
	rp := httputil.NewSingleHostReverseProxy(target)
	u := &HttpUpstream{Name: name, Target: target, Weight: weight, proxy: rp}
	u.healthy.Store(true)
	return u
}

// newTestHTTPPool builds an HttpUpstreamPool around a pre-built set of
// upstreams. Tests use this instead of a struct literal because the
// pool's `upstreams` field is an atomic.Pointer that the picker reads
// lock-free — writers must publish through storeUpstreams so reads
// see a consistent slice header.
func newTestHTTPPool(strategy string, maxRetry int, ups ...*HttpUpstream) *HttpUpstreamPool {
	p := &HttpUpstreamPool{strategy: strategy, maxRetry: maxRetry}
	p.storeUpstreams(ups)
	return p
}

func TestPickerLeastConn(t *testing.T) {
	a := newTestUpstream("a", 1)
	b := newTestUpstream("b", 1)
	c := newTestUpstream("c", 1)
	a.inFlight.Store(5)
	b.inFlight.Store(2)
	c.inFlight.Store(7)

	pool := newTestHTTPPool("least-conn", 0, a, b, c)
	for i := 0; i < 10; i++ {
		got := pool.Pick()
		if got != b {
			t.Fatalf("least-conn iter %d: got %s, want b", i, got.Name)
		}
	}
}

func TestPickerLeastConnTieBreak(t *testing.T) {
	// All three upstreams at the same in-flight count — picker should
	// rotate so no node gets monopolized.
	a := newTestUpstream("a", 1)
	b := newTestUpstream("b", 1)
	c := newTestUpstream("c", 1)
	pool := newTestHTTPPool("least-conn", 0, a, b, c)
	hits := map[string]int{}
	for i := 0; i < 30; i++ {
		hits[pool.Pick().Name]++
	}
	for _, n := range []string{"a", "b", "c"} {
		if hits[n] == 0 {
			t.Fatalf("least-conn tie: %s never picked", n)
		}
	}
}

func TestPickerPrimaryFailover(t *testing.T) {
	a := newTestUpstream("a", 1)
	b := newTestUpstream("b", 1)
	c := newTestUpstream("c", 1)
	pool := newTestHTTPPool("primary-failover", 0, a, b, c)
	// All healthy → always pick a (the primary).
	for i := 0; i < 5; i++ {
		if pool.Pick() != a {
			t.Fatalf("iter %d: expected primary a", i)
		}
	}
	// Primary down → pick b.
	a.healthy.Store(false)
	for i := 0; i < 5; i++ {
		if pool.Pick() != b {
			t.Fatalf("primary down: expected b, got %s", pool.Pick().Name)
		}
	}
	// Both down → pick c.
	b.healthy.Store(false)
	for i := 0; i < 5; i++ {
		if pool.Pick() != c {
			t.Fatalf("primary+secondary down: expected c")
		}
	}
}

func TestPickerWeightedRR(t *testing.T) {
	a := newTestUpstream("a", 3)
	b := newTestUpstream("b", 1)
	pool := newTestHTTPPool("weighted-round-robin", 0, a, b)
	hits := map[string]int{}
	for i := 0; i < 40; i++ {
		hits[pool.Pick().Name]++
	}
	// Weight ratio 3:1 → a should land roughly 3x more than b.
	if hits["a"] < 2*hits["b"] {
		t.Fatalf("weighted RR: a=%d b=%d (want a >> b)", hits["a"], hits["b"])
	}
}

func TestPickerRoundRobinIgnoresWeight(t *testing.T) {
	a := newTestUpstream("a", 100)
	b := newTestUpstream("b", 1)
	pool := newTestHTTPPool("round-robin", 0, a, b)
	hits := map[string]int{}
	for i := 0; i < 20; i++ {
		hits[pool.Pick().Name]++
	}
	if hits["a"] != hits["b"] {
		t.Fatalf("round-robin should be even: a=%d b=%d", hits["a"], hits["b"])
	}
}

// TestRetryOnTransportFailure: the first upstream listens then refuses
// connections (closed listener). The retry should pick a healthy one and
// return its body.
func TestRetryOnTransportFailure(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	}))
	t.Cleanup(good.Close)

	// Build upstream pool by hand: bad URL points at a closed loopback
	// port; good URL points at the live test server.
	badURL, _ := url.Parse("http://127.0.0.1:1") // unlikely to be open
	goodURL, _ := url.Parse(good.URL)

	bad := &HttpUpstream{Name: "bad", Target: badURL, Weight: 1, proxy: httputil.NewSingleHostReverseProxy(badURL)}
	bad.healthy.Store(true)
	gd := &HttpUpstream{Name: "good", Target: goodURL, Weight: 1, proxy: httputil.NewSingleHostReverseProxy(goodURL)}
	gd.healthy.Store(true)

	// Install ErrorHandlers like NewHttpProxy does: honor retry context.
	for _, u := range []*HttpUpstream{bad, gd} {
		u := u
		u.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
			if rs := retryStateFromCtx(r.Context()); rs != nil {
				rs.failed = true
				return
			}
			w.WriteHeader(http.StatusBadGateway)
		}
	}

	// Force the picker to deterministically return bad first by using
	// primary-failover with bad as the primary.
	pool := newTestHTTPPool("primary-failover", 2, bad, gd)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example/foo", nil)
	pool.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("retry: expected 200 from good upstream, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "OK") {
		t.Fatalf("retry: body=%q want OK", rec.Body.String())
	}
}

// TestRetryNotForPOST: POST is non-idempotent → no retry. Bad upstream
// returns 502 directly.
func TestRetryNotForPOST(t *testing.T) {
	badURL, _ := url.Parse("http://127.0.0.1:1")
	bad := &HttpUpstream{Name: "bad", Target: badURL, Weight: 1, proxy: httputil.NewSingleHostReverseProxy(badURL)}
	bad.healthy.Store(true)
	bad.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
		if rs := retryStateFromCtx(r.Context()); rs != nil {
			rs.failed = true
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}

	gd := newTestUpstream("good", 1)
	pool := newTestHTTPPool("primary-failover", 2, bad, gd)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://example/foo", strings.NewReader(""))
	pool.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("POST retry should not happen; got %d", rec.Code)
	}
}

// TestInFlightCounted verifies inFlight is incremented during dispatch
// and decremented after — least-conn relies on this.
func TestInFlightCounted(t *testing.T) {
	var seen int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// At handler entry, inFlight should already be > 0.
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	target, _ := url.Parse(srv.URL)
	rp := httputil.NewSingleHostReverseProxy(target)
	u := &HttpUpstream{Name: "n", Target: target, Weight: 1, proxy: rp}
	u.healthy.Store(true)

	// Wrap the proxy so we can sample inFlight while the handler is in flight.
	rp.ModifyResponse = func(*http.Response) error {
		atomic.StoreInt32(&seen, u.inFlight.Load())
		return nil
	}

	// Need >1 upstream so the fast-path branch isn't taken.
	dummy := newTestUpstream("dummy", 0) // weight 0 → never picked by weighted RR
	dummy.healthy.Store(false)           // also unhealthy so it's not in the set
	pool := newTestHTTPPool("round-robin", 0, u, dummy)

	// Force pick to land on u: dummy is unhealthy, so healthySet returns [u].
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://x/", nil).WithContext(context.Background())
	pool.ServeHTTP(rec, req)

	if seen != 1 {
		t.Fatalf("expected inFlight==1 during handler, got %d", seen)
	}
	if got := u.inFlight.Load(); got != 0 {
		t.Fatalf("expected inFlight==0 after handler, got %d", got)
	}
}

// TestBuildHttpUpstream_HealthcheckService verifies that the per-node
// Healthcheck.Service field re-targets the probe at a sibling service's
// URL — so a single shared probe (typically RPC /status) is the source
// of truth for "is this node alive" across the LCD / RPC / EVM pools.
func TestBuildHttpUpstream_HealthcheckService(t *testing.T) {
	enabled := true
	hc := &NodeHealthcheckConfig{
		Enable:         &enabled,
		Path:           "/status",
		Service:        "rpc",
		Interval:       50 * 1e6, // 50ms
		Timeout:        50 * 1e6,
		UnhealthyAfter: 2,
		HealthyAfter:   2,
	}
	node := NodeConfig{
		Name:        "n1",
		Host:        "10.0.0.1",
		LcdPort:     1317,
		RpcPort:     26657,
		EvmRpcPort:  8545,
		Healthcheck: hc,
	}

	cases := []struct {
		proxySvc string
		want     string
	}{
		{"lcd", "http://10.0.0.1:26657/status"},
		{"rpc", "http://10.0.0.1:26657/status"},
		{"evm_rpc", "http://10.0.0.1:26657/status"},
	}
	for _, tc := range cases {
		u, err := buildHttpUpstream(node, tc.proxySvc, nil)
		if err != nil {
			t.Fatalf("%s: buildHttpUpstream: %v", tc.proxySvc, err)
		}
		if u.probeAddr != tc.want {
			t.Fatalf("%s: probeAddr=%q, want %q", tc.proxySvc, u.probeAddr, tc.want)
		}
	}

	// Service override resolution honors per-service URL overrides so
	// edge-TLS deployments (rpc.<chain>.example / lcd.<chain>.example
	// on :443) get the right probe URL.
	node.RpcURL = "https://rpc.example.com"
	u, err := buildHttpUpstream(node, "lcd", nil)
	if err != nil {
		t.Fatalf("buildHttpUpstream with override: %v", err)
	}
	if got, want := u.probeAddr, "https://rpc.example.com/status"; got != want {
		t.Fatalf("probeAddr=%q, want %q", got, want)
	}

	// Unknown service is rejected at build time rather than silently
	// falling through to the proxy's own target — a typo in
	// healthcheck.service shouldn't quietly probe the wrong endpoint.
	bad := node
	bad.Healthcheck = &NodeHealthcheckConfig{Enable: &enabled, Path: "/status", Service: "nope"}
	if _, err := buildHttpUpstream(bad, "lcd", nil); err == nil {
		t.Fatalf("expected error for unknown healthcheck.service, got nil")
	}
}

// A healthcheck-backed upstream must start UNHEALTHY so /readyz gates the pod
// out of the load balancer until the first probe confirms the upstream is
// reachable — otherwise a fresh/scaled-up pod is marked Ready and 502s traffic
// before its upstream connection is warm. Without a healthcheck the upstream
// stays optimistically healthy (nothing to confirm).
func TestHealthcheckGatesReadinessUntilFirstProbe(t *testing.T) {
	on := true
	hc := &NodeHealthcheckConfig{Enable: &on, Path: "/status", Service: "rpc"}

	gated, err := NewHttpUpstreamPool(
		[]NodeConfig{{Name: "hc", Host: "127.0.0.1", RpcPort: 9, LcdPort: 9, GrpcPort: 9, Healthcheck: hc}},
		"rpc", nil, log.WithField("test", "readyz-gate"))
	if err != nil {
		t.Fatalf("build gated pool: %v", err)
	}
	if gated.AnyHealthy() {
		t.Fatal("healthcheck-backed upstream must start unhealthy so /readyz gates until the first probe")
	}

	optimistic, err := NewHttpUpstreamPool(
		[]NodeConfig{{Name: "nohc", Host: "127.0.0.1", RpcPort: 9, LcdPort: 9, GrpcPort: 9}},
		"rpc", nil, log.WithField("test", "readyz-nohc"))
	if err != nil {
		t.Fatalf("build optimistic pool: %v", err)
	}
	if !optimistic.AnyHealthy() {
		t.Fatal("no-healthcheck upstream must stay optimistically healthy")
	}
}

// The cold-start readiness gate must clear on the FIRST successful probe, even
// when HealthyAfter > 1 — that threshold is runtime flap protection, not a
// startup delay. Without the success-counter seed this needs HealthyAfter probes.
func TestColdStartGateClearsOnFirstProbe(t *testing.T) {
	on := true
	hc := &NodeHealthcheckConfig{Enable: &on, Path: "/status", Service: "rpc", HealthyAfter: 3}
	pool, err := NewHttpUpstreamPool(
		[]NodeConfig{{Name: "hc", Host: "127.0.0.1", RpcPort: 9, LcdPort: 9, GrpcPort: 9, Healthcheck: hc}},
		"rpc", nil, log.WithField("test", "coldstart-firstprobe"))
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	if pool.AnyHealthy() {
		t.Fatal("must start gated (unhealthy) with a healthcheck configured")
	}
	pool.recordProbeResult(pool.Upstreams()[0], true, nil) // one successful probe
	if !pool.AnyHealthy() {
		t.Fatal("first successful probe must clear the cold-start gate even with HealthyAfter=3")
	}
}

// healthyAfter / unhealthyAfter must fit the int32 probe counters — an
// out-of-range value would overflow the cold-start seed and leave a pod
// permanently NotReady, so config prep rejects it.
func TestValidateNodeHealthcheckThresholdRange(t *testing.T) {
	on := true
	tooBig := &NodeConfig{Healthcheck: &NodeHealthcheckConfig{Enable: &on, HealthyAfter: math.MaxInt32 + 1, UnhealthyAfter: 3}}
	if err := validateNodeHealthcheck(tooBig); err == nil {
		t.Fatal("healthyAfter beyond int32 range must be rejected")
	}
	tooBigFail := &NodeConfig{Healthcheck: &NodeHealthcheckConfig{Enable: &on, HealthyAfter: 2, UnhealthyAfter: math.MaxInt32 + 1}}
	if err := validateNodeHealthcheck(tooBigFail); err == nil {
		t.Fatal("unhealthyAfter beyond int32 range must be rejected")
	}
	ok := &NodeConfig{Healthcheck: &NodeHealthcheckConfig{Enable: &on, HealthyAfter: 2, UnhealthyAfter: 3}}
	if err := validateNodeHealthcheck(ok); err != nil {
		t.Fatalf("valid thresholds must pass: %v", err)
	}
}
