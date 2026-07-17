package testharness_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestJ_HealthzReady verifies the /healthz and /readyz endpoints land on
// the metrics-server port and return 200 with the expected body. These
// are the standard Kubernetes liveness/readiness probe targets.
func TestJ_HealthzReady(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(true)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithMetricsEnabled(),
	)

	// The harness has assigned a port to Metrics.Port; we read it from the
	// running config to know where to hit /healthz and /readyz.
	port := h.CosmoGuard.MetricsPort()
	assert.Assert(t, port > 0, "metrics port should be set")

	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// /healthz
	r := h.GET(t, base+"/healthz")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Equal(t, string(r.Body), "ok")

	// /readyz
	r = h.GET(t, base+"/readyz")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Equal(t, string(r.Body), "ready")

	// /metrics still works (Prometheus exposition format)
	r = h.GET(t, base+"/metrics")
	assert.Equal(t, r.StatusCode, http.StatusOK)

	// /info — JSON with version, started_at, uptime, upstreams count.
	r = h.GET(t, base+"/info")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Equal(t, r.Header.Get("Content-Type"), "application/json")
	// Body should at least mention "uptime_seconds" and "upstreams".
	body := string(r.Body)
	for _, want := range []string{"uptime_seconds", "upstreams", "started_at"} {
		if !contains(body, want) {
			t.Fatalf("/info body missing %q: %s", want, body)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestJ_InfoUpstreamCountTracksPool is the regression pin for the
// in-cluster /info bug: after discovery (or any future dynamic
// add/remove) mutated the pool past the configured Nodes count, the
// /info handler kept reporting len(cfg.Nodes) — frozen at boot. The
// fix routes the count through countUpstreams which reads the live
// pool snapshot. We exercise the live-mutation path directly via
// AddUpstream / RemoveUpstream on the LCD pool so the test doesn't
// need a DNS resolver; the contract under test is identical.
func TestJ_InfoUpstreamCountTracksPool(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(true)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithMetricsEnabled(),
	)
	port := h.CosmoGuard.MetricsPort()
	assert.Assert(t, port > 0)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	read := func() (int, int) {
		t.Helper()
		r := h.GET(t, base+"/info")
		assert.Equal(t, r.StatusCode, http.StatusOK)
		var body struct {
			Upstreams struct {
				Count   int `json:"count"`
				Healthy int `json:"healthy"`
			} `json:"upstreams"`
		}
		assert.NilError(t, json.Unmarshal(r.Body, &body))
		return body.Upstreams.Count, body.Upstreams.Healthy
	}

	// Baseline: single configured node — the harness's default config
	// installs one. /info should reflect that.
	count, _ := read()
	assert.Equal(t, count, 1, "baseline upstream count")

	// Add an upstream directly to the LCD pool. /info must observe
	// the new pool size, NOT the unchanged cfg.Nodes count.
	lcd := h.CosmoGuard.LcdPool()
	assert.Assert(t, lcd != nil)
	added := cosmoguard.NodeConfig{
		Name:    "dynamic-pod-1",
		Host:    "10.42.0.5",
		LcdPort: 1317,
		RpcPort: 26657,
	}
	assert.NilError(t, lcd.AddUpstream(added))

	count, _ = read()
	assert.Equal(t, count, 2, "/info count must follow pool growth")

	// Remove it. /info must drop back to the original count.
	lcd.RemoveUpstream("dynamic-pod-1")
	count, _ = read()
	assert.Equal(t, count, 1, "/info count must follow pool shrink")
}
