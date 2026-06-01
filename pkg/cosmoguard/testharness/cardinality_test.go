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

// TestDashboard_CacheCardinality: each cacheable rule that writes a
// distinct key bumps the per-rule cardinality counter. Driving N
// requests with distinct query strings against a cacheable rule
// should surface N distinct keys for that rule on the dashboard so
// an operator can spot a runaway key-derivation.
func TestDashboard_CacheCardinality(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache: cosmoguard.CacheGlobalConfig{TTL: time.Hour},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100, Action: cosmoguard.RuleActionAllow,
					Tag:   "hot-rule",
					Paths: []string{"/cosmos/bank/v1beta1/params"},
					Cache: &cosmoguard.RuleCache{Enable: true, TTL: time.Hour},
				},
			},
		},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithDashboardEnabled("", ""),
		testharness.WithLCDResponse(http.MethodGet, "/cosmos/bank/v1beta1/params", `{"params":{}}`),
	)

	// 60 requests with distinct query strings → 60 distinct cache
	// keys, all attributed to the same rule tag. The query string is
	// part of the HTTP cache key, so each one is a fresh write.
	for i := 0; i < 60; i++ {
		r := h.GET(t, fmt.Sprintf("%s/cosmos/bank/v1beta1/params?cursor=%d", h.LCDURL, i))
		assert.Equal(t, r.StatusCode, http.StatusOK)
	}

	r := h.GET(t, h.DashboardURL+"/api/v1/cache-cardinality")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var doc struct {
		Sections map[string][]struct {
			RuleTag      string   `json:"rule_tag"`
			DistinctKeys int      `json:"distinct_keys"`
			HotSamples   []string `json:"hot_samples"`
		} `json:"sections"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &doc))

	lcd, ok := doc.Sections["lcd"]
	assert.Assert(t, ok, "expected lcd section in cardinality payload: %v", doc.Sections)
	var found bool
	for _, row := range lcd {
		if row.RuleTag != "hot-rule" {
			continue
		}
		found = true
		assert.Assert(t, row.DistinctKeys >= 50,
			"hot-rule should have >=50 distinct keys, got %d", row.DistinctKeys)
		assert.Assert(t, len(row.HotSamples) > 0,
			"hot_samples should carry at least one key for operator triage")
	}
	assert.Assert(t, found, "expected hot-rule row in lcd cardinality payload: %v", lcd)
}
