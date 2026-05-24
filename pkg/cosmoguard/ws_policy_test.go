package cosmoguard

import (
	"context"
	"testing"
	"time"
)

// wsDenyLimiter is a RateLimiter that always denies, for proving the WS
// per-rule rate-limit gate fires.
type wsDenyLimiter struct{}

func (wsDenyLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return false, time.Second, nil
}
func (wsDenyLimiter) Close() error { return nil }

// TestWSPolicyVerdict_EnforcesPerRuleAuthAndRate is the regression test
// for the WebSocket JSON-RPC policy bypass: per-rule auth scopes and
// rate limits must be enforced on WS frames, mirroring the HTTP single
// and batch paths. Before the fix, handleRequest only checked
// match/action/cache, so any upgraded client bypassed these gates.
func TestWSPolicyVerdict_EnforcesPerRuleAuthAndRate(t *testing.T) {
	p := &JsonRpcWebSocketProxy{
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
		log:         log.WithField("t", "ws"),
	}
	req := &JsonRpcMsg{Method: "subscribe", ID: float64(1)}

	// Rule gated on the "admin" scope.
	authRule := &JsonRpcRule{Tag: "sub", Action: RuleActionAllow, Auth: &RuleAuthConfig{Scopes: []string{"admin"}}}

	// Anonymous → denied with -32001 (missing scope).
	ok, code, _ := p.policyVerdict(req, authRule, nil, "1.2.3.4", nil)
	if ok || code != -32001 {
		t.Fatalf("anonymous + admin-scope rule: ok=%v code=%d, want deny -32001", ok, code)
	}

	// Identity holding the scope → allowed.
	admin := &Identity{Name: "admin-1", Method: "api-key", Scopes: []string{"admin"}}
	if ok, _, _ := p.policyVerdict(req, authRule, admin, "1.2.3.4", nil); !ok {
		t.Fatalf("admin identity: expected allow")
	}

	// Per-rule rate limit denies → -32005, regardless of identity.
	rateRule := &JsonRpcRule{Tag: "rl", Action: RuleActionAllow, RateLimit: &RateLimitConfig{Scope: RateLimitScopePerIP}}
	limiters := map[uint64]RateLimiter{rateRule.Fingerprint: wsDenyLimiter{}}
	if ok, code, _ := p.policyVerdict(req, rateRule, admin, "1.2.3.4", limiters); ok || code != -32005 {
		t.Fatalf("rate-limited rule: ok=%v code=%d, want deny -32005", ok, code)
	}
}
