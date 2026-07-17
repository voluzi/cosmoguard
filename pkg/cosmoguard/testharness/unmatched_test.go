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

// TestDashboard_Unmatched: requests that don't match any rule are
// surfaced by /api/v1/unmatched under the right section with their
// (method, path) tuple and a count that matches the drive volume.
//
// The proxy is configured with `default: allow` and no rules so every
// request falls through. This is the workflow the panel exists for:
// operators integrating a chain against `default: allow` need a signal
// for "the API is being hit on a path my rule set doesn't cover".
func TestDashboard_Unmatched(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithDashboardEnabled("", ""),
		testharness.WithLCDResponse("GET", "/cosmos/bank/v1beta1/params", `{"params":{}}`),
	)

	// Drive 5 LCD requests through the proxy. No rule matches → every
	// one of them is recorded under the "lcd" section.
	for i := 0; i < 5; i++ {
		r := h.GET(t, h.LCDURL+"/cosmos/bank/v1beta1/params")
		assert.Equal(t, r.StatusCode, http.StatusOK)
	}

	r := h.GET(t, h.DashboardURL+"/api/v1/unmatched")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var doc struct {
		Sections map[string][]struct {
			Section string `json:"section"`
			Method  string `json:"method"`
			Path    string `json:"path"`
			Count   uint64 `json:"count"`
		} `json:"sections"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &doc))

	entries := doc.Sections["lcd"]
	assert.Assert(t, len(entries) >= 1, "expected at least one unmatched entry in lcd section")
	var hit bool
	for _, e := range entries {
		if e.Method == http.MethodGet && e.Path == "/cosmos/bank/v1beta1/params" {
			assert.Equal(t, e.Count, uint64(5),
				"unmatched count for GET /cosmos/bank/v1beta1/params should be 5, got %d", e.Count)
			hit = true
		}
	}
	assert.Assert(t, hit, "expected GET /cosmos/bank/v1beta1/params in lcd unmatched entries")
}

// TestDashboard_UnmatchedBoundedLRU: feeding more distinct (method,
// path) tuples than the counter's capacity must not blow up the
// process — the LRU evicts old entries silently. This guards the
// dashboard against being a DoS amplifier when a misbehaving client
// drives unique paths.
func TestDashboard_UnmatchedBoundedLRU(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithDashboardEnabled("", ""),
	)

	// 300 distinct paths > the 256 cap. Counter must absorb them all
	// without growing the underlying map past its bound. The fake
	// upstream answers 404 for unmatched paths, but the unmatched
	// counter fires before the upstream call so 404 is fine here.
	for i := 0; i < 300; i++ {
		_ = h.GET(t, fmt.Sprintf("%s/test/path/%d", h.LCDURL, i))
	}

	r := h.GET(t, h.DashboardURL+"/api/v1/unmatched")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var doc struct {
		Sections map[string][]any `json:"sections"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &doc))
	// LRU caps the per-section entry count at 256.
	assert.Assert(t, len(doc.Sections["lcd"]) <= 256,
		"lcd unmatched bucket must not exceed LRU cap (got %d)", len(doc.Sections["lcd"]))
}
