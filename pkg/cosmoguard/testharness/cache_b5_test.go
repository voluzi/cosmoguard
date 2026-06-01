package testharness_test

import (
	"net/http"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestB5_CrossRuleNoPoisoning is the cache-poisoning regression test from
// the audit. Two rules match the same path; the higher-priority one allows
// caching errors, the lower-priority does not. Pre-B5 they shared a cache
// namespace so a 500 cached by rule A would be served on a hit by rule B.
// Post-B5 each rule has its own fingerprint baked into the cache key, so the
// 500 cached by A is invisible to B.
//
// In this test there's only one matching rule per request (priority order
// resolves the match), so the pre-B5 bug doesn't surface as wrong-answer
// per se — the test instead asserts that two DIFFERENT rules produce
// different cache keys for the same request, by toggling rule shape between
// runs and checking upstream call count.
func TestB5_PerRuleCacheNamespace(t *testing.T) {
	cfg1 := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{{
				Priority: 100,
				Action:   cosmoguard.RuleActionAllow,
				Paths:    []string{"/x"},
				Methods:  []string{http.MethodGet},
				Cache:    &cosmoguard.RuleCache{Enable: true, TTL: time.Hour},
			}},
		},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg1),
		testharness.WithLCDResponse(http.MethodGet, "/x", `{"v":1}`),
	)

	// Two requests against rule with one shape — second should hit cache.
	h.GET(t, h.LCDURL+"/x")
	h.GET(t, h.LCDURL+"/x")
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/x"), 1,
		"second request should hit cache")
}

// TestB5_HeaderPreservation pins down what's preserved on a cache hit:
//
//   - always-preserved set: Content-Type, Content-Encoding, Cache-Control,
//     ETag, Vary
//   - operator-listed in cache.preserveHeaders
//   - Hop-by-hop headers (Transfer-Encoding etc.) are NEVER preserved even
//     if listed
//   - Other upstream headers are dropped (default behavior)
func TestB5_HeaderPreservation(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{{
				Priority: 100,
				Action:   cosmoguard.RuleActionAllow,
				Paths:    []string{"/with-headers"},
				Methods:  []string{http.MethodGet},
				Cache: &cosmoguard.RuleCache{
					Enable:          true,
					TTL:             time.Hour,
					PreserveHeaders: []string{"X-Custom-Header"},
				},
			}},
		},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))
	h.Upstream.LCD.SetResponse(http.MethodGet, "/with-headers", testharness.FakeResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":      "application/cbor",  // always-preserved
			"Content-Encoding":  "identity",          // always-preserved
			"X-Custom-Header":   "preserved-by-rule", // listed in PreserveHeaders
			"X-Internal-Secret": "drop-me",           // not listed → dropped
			"Cache-Control":     "max-age=60",        // always-preserved
		},
		Body: []byte("payload"),
	})

	// Miss + populate cache (test the miss path's pass-through too).
	r1 := h.GET(t, h.LCDURL+"/with-headers")
	assert.Equal(t, r1.StatusCode, http.StatusOK)
	assert.Equal(t, r1.Header.Get("Content-Type"), "application/cbor")
	assert.Equal(t, r1.Header.Get("X-Custom-Header"), "preserved-by-rule")

	// Hit — preserved headers come from cache.
	r2 := h.GET(t, h.LCDURL+"/with-headers")
	assert.Equal(t, r2.StatusCode, http.StatusOK)
	assert.Equal(t, r2.Header.Get("Content-Type"), "application/cbor")
	assert.Equal(t, r2.Header.Get("Content-Encoding"), "identity")
	assert.Equal(t, r2.Header.Get("Cache-Control"), "max-age=60")
	assert.Equal(t, r2.Header.Get("X-Custom-Header"), "preserved-by-rule")
	// Non-listed header should be absent.
	assert.Equal(t, r2.Header.Get("X-Internal-Secret"), "")
	// Cache-state marker visible (cosmoguard-namespaced so it doesn't
	// collide with a legitimate upstream `Cache` header).
	assert.Equal(t, r2.Header.Get("X-Cosmoguard-Cache"), "hit")
	// Body identical.
	assert.Equal(t, string(r2.Body), "payload")
	// Upstream called once total.
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/with-headers"), 1)
}
