package testharness_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestD_RateLimitPerIP is the headline integration test for Phase D rate
// limiting: a rule with rate=2/s and burst=2 admits the first two
// requests instantly, then returns 429 with a Retry-After header.
//
// The fake upstream tracks call count so we can prove rate-limited
// requests never reach it.
func TestD_RateLimitPerIP(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{{
				Priority: 100,
				Action:   cosmoguard.RuleActionAllow,
				Paths:    []string{"/x"},
				Methods:  []string{http.MethodGet},
				RateLimit: &cosmoguard.RateLimitConfig{
					Rate:  cosmoguard.Rate{PerSecond: 2, Spec: "2/s"},
					Burst: 2,
					Scope: cosmoguard.RateLimitScopePerIP,
				},
			}},
		},
		RPC:  cosmoguard.RpcConfig{Default: cosmoguard.RuleActionAllow, JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow}},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/x", `{"v":1}`),
	)

	for i := 0; i < 2; i++ {
		resp := h.GET(t, h.LCDURL+"/x")
		assert.Equal(t, resp.StatusCode, http.StatusOK,
			"first %d requests should pass (slot %d)", i+1, i)
	}

	// Third request: same client, bucket empty → 429 with Retry-After.
	resp := h.GET(t, h.LCDURL+"/x")
	assert.Equal(t, resp.StatusCode, http.StatusTooManyRequests)
	ra := resp.Header.Get("Retry-After")
	assert.Assert(t, ra != "", "Retry-After header should be set on 429")
	n, err := strconv.Atoi(ra)
	assert.NilError(t, err)
	assert.Assert(t, n >= 1, "Retry-After should be at least 1s, got %s", ra)

	// Upstream must have been called exactly twice.
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/x"), 2)
}

// TestD_RateLimitGlobal verifies the global scope: all clients share one
// bucket, so two different X-Real-Ip values count against the same budget.
func TestD_RateLimitGlobal(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{{
				Priority: 100,
				Action:   cosmoguard.RuleActionAllow,
				Paths:    []string{"/x"},
				Methods:  []string{http.MethodGet},
				RateLimit: &cosmoguard.RateLimitConfig{
					Rate:  cosmoguard.Rate{PerSecond: 1, Spec: "1/s"},
					Burst: 1,
					Scope: cosmoguard.RateLimitScopeGlobal,
				},
			}},
		},
		RPC:  cosmoguard.RpcConfig{Default: cosmoguard.RuleActionAllow, JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow}},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/x", `{"v":1}`),
	)

	// Client A consumes the only burst slot.
	req1, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/x", nil)
	req1.Header.Set("X-Real-Ip", "1.2.3.4")
	r1 := h.Do(t, req1)
	assert.Equal(t, r1.StatusCode, http.StatusOK)

	// Client B from a different IP — should ALSO be 429 because the bucket
	// is global, not per-ip.
	req2, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/x", nil)
	req2.Header.Set("X-Real-Ip", "5.6.7.8")
	r2 := h.Do(t, req2)
	assert.Equal(t, r2.StatusCode, http.StatusTooManyRequests)
}

// TestD_RateLimitDoesNotPoisonOtherRules verifies that a rate-limited rule's
// 429 doesn't leak into other rules. A second rule on a different path
// should serve normally even when the first is over budget.
func TestD_RateLimitDoesNotPoisonOtherRules(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100,
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/limited"},
					Methods:  []string{http.MethodGet},
					RateLimit: &cosmoguard.RateLimitConfig{
						Rate:  cosmoguard.Rate{PerSecond: 1, Spec: "1/s"},
						Burst: 1,
						Scope: cosmoguard.RateLimitScopeGlobal,
					},
				},
				{
					Priority: 200,
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/free"},
					Methods:  []string{http.MethodGet},
				},
			},
		},
		RPC:  cosmoguard.RpcConfig{Default: cosmoguard.RuleActionAllow, JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow}},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/limited", `{}`),
		testharness.WithLCDResponse(http.MethodGet, "/free", `{}`),
	)

	// Consume the /limited bucket.
	assert.Equal(t, h.GET(t, h.LCDURL+"/limited").StatusCode, http.StatusOK)
	assert.Equal(t, h.GET(t, h.LCDURL+"/limited").StatusCode, http.StatusTooManyRequests)

	// /free must still work.
	assert.Equal(t, h.GET(t, h.LCDURL+"/free").StatusCode, http.StatusOK)
}
