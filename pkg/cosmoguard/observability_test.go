package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRuleTagOrFingerprint: explicit Tag wins; empty Tag falls back to
// the "r-<hex>" form.
func TestRuleTagOrFingerprint(t *testing.T) {
	if got := ruleTagOrFingerprint("balance-query", 0x1234); got != "balance-query" {
		t.Fatalf("expected operator tag, got %q", got)
	}
	if got := ruleTagOrFingerprint("", 0xdeadbeef); got != "r-deadbeef" {
		t.Fatalf("expected hex fallback, got %q", got)
	}
}

// TestStatsLabels_NoStats: a request without WithRequestStats applied
// returns ("default", "") — the same defaults we'd want for code paths
// that short-circuit before reaching the pool.
func TestStatsLabels_NoStats(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://x/y", nil)
	ruleID, upstream := statsLabels(req)
	if ruleID != "default" {
		t.Fatalf("rule_id default: got %q", ruleID)
	}
	if upstream != "" {
		t.Fatalf("upstream default: got %q", upstream)
	}
}

// TestStatsLabels_FromCtx: when both fields are populated, statsLabels
// returns them verbatim.
func TestStatsLabels_FromCtx(t *testing.T) {
	ctx, stats := WithRequestStats(context.Background())
	stats.RuleTag = "lcd-balance"
	stats.Upstream = "node-2"
	req := httptest.NewRequest(http.MethodGet, "http://x/y", nil).WithContext(ctx)
	ruleID, upstream := statsLabels(req)
	if ruleID != "lcd-balance" || upstream != "node-2" {
		t.Fatalf("labels mismatch: ruleID=%q upstream=%q", ruleID, upstream)
	}
}
