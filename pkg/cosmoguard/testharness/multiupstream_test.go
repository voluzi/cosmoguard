package testharness_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestE_MultiUpstream_RoundRobin: two upstream LCD servers; cosmoguard
// rotates requests between them. Six requests → each upstream sees three.
func TestE_MultiUpstream_RoundRobin(t *testing.T) {
	var aHits, bHits atomic.Int64

	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node":"a"}`))
	}))
	defer upA.Close()
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node":"b"}`))
	}))
	defer upB.Close()

	aHost, aPort := splitHostPort(t, upA.URL)
	bHost, bPort := splitHostPort(t, upB.URL)

	cfg := multiUpstreamBaseConfig()
	// Distinct closed RpcPort per node — keeps WS broker dials from
	// landing on the LCD hit counters.
	cfg.Nodes = []cosmoguard.NodeConfig{
		{Name: "a", Host: aHost, LcdPort: aPort, RpcPort: freePort(t), GrpcPort: 1, EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1},
		{Name: "b", Host: bHost, LcdPort: bPort, RpcPort: freePort(t), GrpcPort: 1, EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))

	for i := 0; i < 6; i++ {
		r := h.GET(t, h.LCDURL+"/whatever")
		assert.Equal(t, r.StatusCode, http.StatusOK)
	}

	// Round-robin → 3 each.
	assert.Equal(t, aHits.Load(), int64(3), "node A should have served 3 requests, got %d", aHits.Load())
	assert.Equal(t, bHits.Load(), int64(3), "node B should have served 3 requests, got %d", bHits.Load())
}

// TestE_MultiUpstream_WeightedRoundRobin: one node weighted 3, the other
// weighted 1 → over a full cycle (4 requests) the 3-weight node gets
// three picks and the 1-weight node gets one.
func TestE_MultiUpstream_WeightedRoundRobin(t *testing.T) {
	var aHits, bHits atomic.Int64

	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("a"))
	}))
	defer upA.Close()
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("b"))
	}))
	defer upB.Close()

	aHost, aPort := splitHostPort(t, upA.URL)
	bHost, bPort := splitHostPort(t, upB.URL)

	cfg := multiUpstreamBaseConfig()
	cfg.Nodes = []cosmoguard.NodeConfig{
		{Name: "a", Host: aHost, LcdPort: aPort, RpcPort: freePort(t), GrpcPort: 1, EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 3},
		{Name: "b", Host: bHost, LcdPort: bPort, RpcPort: freePort(t), GrpcPort: 1, EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))

	// 8 requests = 2 full cycles of length (3+1=4).
	for i := 0; i < 8; i++ {
		r := h.GET(t, h.LCDURL+"/whatever")
		assert.Equal(t, r.StatusCode, http.StatusOK)
	}

	// 8 requests × 3/(3+1) = 6 to a; 8 × 1/(3+1) = 2 to b.
	assert.Equal(t, aHits.Load(), int64(6),
		"node a (weight 3) should have served 6 of 8 requests, got %d", aHits.Load())
	assert.Equal(t, bHits.Load(), int64(2),
		"node b (weight 1) should have served 2 of 8 requests, got %d", bHits.Load())
}

// TestE_MultiUpstream_HealthcheckFailover: one upstream returns 500 on
// its healthcheck path; cosmoguard takes it out of the picker after
// UnhealthyAfter consecutive failures, all traffic flows to the healthy
// one.
func TestE_MultiUpstream_HealthcheckFailover(t *testing.T) {
	var sickHits, healthyHits atomic.Int64

	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			// Healthcheck always fails.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sickHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"node":"sick"}`))
	}))
	defer sick.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		healthyHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"node":"healthy"}`))
	}))
	defer healthy.Close()

	sickHost, sickPort := splitHostPort(t, sick.URL)
	healthyHost, healthyPort := splitHostPort(t, healthy.URL)

	hcEnabled := true
	hc := &cosmoguard.NodeHealthcheckConfig{
		Enable:         &hcEnabled,
		Path:           "/status",
		Service:        "lcd", // probe the same port as LCD traffic
		Interval:       50 * time.Millisecond,
		Timeout:        100 * time.Millisecond,
		UnhealthyAfter: 2,
		HealthyAfter:   2,
	}

	cfg := multiUpstreamBaseConfig()
	cfg.Nodes = []cosmoguard.NodeConfig{
		{
			Name: "sick", Host: sickHost,
			LcdPort: sickPort, RpcPort: sickPort, GrpcPort: 1,
			EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1,
			Healthcheck: hc,
		},
		{
			Name: "healthy", Host: healthyHost,
			LcdPort: healthyPort, RpcPort: healthyPort, GrpcPort: 1,
			EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1,
			Healthcheck: hc,
		},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))

	// Readiness gating: both nodes start unhealthy until their first probe, so
	// first WAIT for the healthy node to be confirmed up (2 successes × 50ms ≈
	// 100ms), then assert it STAYS up while the sick node's probes fail it out.
	readyDeadline := time.Now().Add(3 * time.Second)
	for !h.CosmoGuard.LcdPool().AnyHealthy() {
		if time.Now().After(readyDeadline) {
			t.Fatal("healthy node never became ready after its healthcheck probes")
		}
		time.Sleep(25 * time.Millisecond)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !h.CosmoGuard.LcdPool().AnyHealthy() {
			t.Fatal("healthy node flapped to unhealthy — it should stay up while only the sick node fails")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Steady state: sick node is out. Issue a fresh batch of requests;
	// every one should land on the healthy node. The sick node may have
	// received a couple of hits during the startup grace window, which
	// is correct behavior — we assert on the post-stabilization behavior.
	sickBefore := sickHits.Load()
	healthyBefore := healthyHits.Load()

	for i := 0; i < 10; i++ {
		r := h.GET(t, h.LCDURL+"/whatever")
		assert.Equal(t, r.StatusCode, http.StatusOK)
	}

	sickPost := sickHits.Load() - sickBefore
	healthyPost := healthyHits.Load() - healthyBefore
	assert.Equal(t, sickPost, int64(0),
		"sick node received %d requests after healthcheck stabilization", sickPost)
	assert.Equal(t, healthyPost, int64(10),
		"healthy node should have received all 10 post-stabilization requests, got %d", healthyPost)
}

// TestE_CircuitBreaker_TripsOnConsecutiveFailures: one upstream always
// returns 500. After ConsecutiveFailures back-to-back failures, the
// circuit opens and the other upstream serves everything until the
// cooldown elapses.
func TestE_CircuitBreaker_TripsOnConsecutiveFailures(t *testing.T) {
	var sickHits, healthyHits atomic.Int64

	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sickHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // always fails
	}))
	defer sick.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer healthy.Close()

	sickHost, sickPort := splitHostPort(t, sick.URL)
	healthyHost, healthyPort := splitHostPort(t, healthy.URL)

	cbEnabled := true
	cb := &cosmoguard.CircuitBreakerConfig{
		Enable:              &cbEnabled,
		ConsecutiveFailures: 3,
		CooldownPeriod:      30 * time.Second,
	}

	cfg := multiUpstreamBaseConfig()
	cfg.Nodes = []cosmoguard.NodeConfig{
		{Name: "sick", Host: sickHost, LcdPort: sickPort, RpcPort: freePort(t), GrpcPort: 1,
			EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1, CircuitBreaker: cb},
		{Name: "healthy", Host: healthyHost, LcdPort: healthyPort, RpcPort: freePort(t), GrpcPort: 1,
			EvmRpcPort: 1, EvmRpcWsPort: 1, Weight: 1, CircuitBreaker: cb},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))

	// 30 requests. Round-robin means the sick node gets ~15 of them.
	// After 3 consecutive failures (slots 1, 3, 5 — sick is alternate),
	// the circuit opens. The healthy node then serves all remaining.
	for i := 0; i < 30; i++ {
		_ = h.GET(t, h.LCDURL+"/whatever")
	}

	// The sick node MUST have stopped receiving traffic at some point.
	// Without the breaker, round-robin would send 15 requests to sick.
	// With the breaker tripping after 3 consecutive failures... actually
	// in pure RR alternation sick gets [1,3,5,7,...] so consecutive
	// failures only counts the SICK NODE'S consecutive failures — and
	// since RR alternates, each sick visit is "consecutive" from the
	// breaker's perspective. So 3 visits = trip.
	assert.Assert(t, sickHits.Load() <= 5,
		"breaker should have tripped early; sick saw %d (expected ≤ 5)", sickHits.Load())
	// Remaining traffic on healthy.
	assert.Assert(t, healthyHits.Load() >= 25,
		"healthy should have absorbed the post-trip traffic; got %d", healthyHits.Load())
}

// freePort grabs an ephemeral TCP port, closes it, and returns the
// number. The port is then unbound — anything dialing it will get
// ECONNREFUSED. Used in multi-upstream tests to assign each node a
// distinct RpcPort that's NOT the hit-counting LCD port, so WS broker
// dials don't pollute the LCD round-robin counters.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// helpers
func splitHostPort(t *testing.T, urlStr string) (string, int) {
	t.Helper()
	// httptest URLs are "http://127.0.0.1:NNNNN".
	const prefix = "http://"
	if len(urlStr) <= len(prefix) {
		t.Fatalf("bad url: %s", urlStr)
	}
	hostPort := urlStr[len(prefix):]
	h, p, err := net.SplitHostPort(hostPort)
	assert.NilError(t, err)
	port, err := strconv.Atoi(p)
	assert.NilError(t, err)
	return h, port
}

func multiUpstreamBaseConfig() *cosmoguard.Config {
	return &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
}

var _ = fmt.Sprintf // keep fmt import in case future helpers need it
