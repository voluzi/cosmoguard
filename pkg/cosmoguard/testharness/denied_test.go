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

// TestDashboard_Denied: requests that fall through to a `default:
// deny` LCD section are recorded in the denied ring buffer and
// surfaced by /api/v1/denied with the matching reason + method +
// path.
func TestDashboard_Denied(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionDeny},
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

	// Drive one default-deny request.
	_ = h.GET(t, h.LCDURL+"/cosmos/auth/v1beta1/accounts/foo")

	r := h.GET(t, h.DashboardURL+"/api/v1/denied")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var doc struct {
		Denied []struct {
			TimestampMs int64  `json:"timestamp_ms"`
			Section     string `json:"section"`
			Reason      string `json:"reason"`
			SourceIP    string `json:"source_ip"`
			Method      string `json:"method"`
			Path        string `json:"path"`
			RuleTag     string `json:"rule_tag"`
		} `json:"denied"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &doc))
	assert.Assert(t, len(doc.Denied) >= 1, "expected at least one deny record")

	rec := doc.Denied[0]
	assert.Equal(t, rec.Section, "lcd")
	assert.Equal(t, rec.Reason, "default")
	assert.Equal(t, rec.Method, http.MethodGet)
	assert.Equal(t, rec.Path, "/cosmos/auth/v1beta1/accounts/foo")
	assert.Assert(t, rec.TimestampMs > 0, "deny record should carry a timestamp")
}

// TestDashboard_DeniedRingBuffer: driving more denies than the
// buffer's capacity (64) wraps without growing — the bounded buffer
// is what keeps an attacker hammering the deny path from blowing up
// the dashboard's memory.
func TestDashboard_DeniedRingBuffer(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionDeny},
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

	for i := 0; i < 80; i++ {
		_ = h.GET(t, fmt.Sprintf("%s/deny/path/%d", h.LCDURL, i))
	}

	r := h.GET(t, h.DashboardURL+"/api/v1/denied")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var doc struct {
		Denied []any `json:"denied"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &doc))
	assert.Equal(t, len(doc.Denied), 64,
		"denied ring buffer should stay capped at 64 entries (got %d)", len(doc.Denied))
}
