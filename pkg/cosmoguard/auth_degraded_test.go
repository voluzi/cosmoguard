package cosmoguard

import "testing"

// TestAuthorize_DegradedIdentity pins the two-sided fail-open contract
// for a degraded identity (minted when an auth backend is unreachable and
// failureMode is fail-open):
//
//   - AUTHENTICATION is relaxed: a degraded id satisfies "require auth" /
//     defaultRequire, so traffic keeps flowing during an outage instead
//     of failing closed on every protected route.
//   - AUTHORIZATION is NOT relaxed: a rule naming specific scopes or
//     identities still fails closed, because the backend that would prove
//     those claims is exactly what's down — granting them would be a
//     silent privilege escalation.
func TestAuthorize_DegradedIdentity(t *testing.T) {
	a := authenticatorWithDefaultRequire(t) // defaultRequire = true

	deg := degradedIdentity("external-validator")
	if !deg.Degraded {
		t.Fatal("degradedIdentity must set Degraded")
	}

	// Authentication availability: passes the bare defaultRequire gate
	// and a rule that only requires auth (no scopes/identities).
	if ok, _ := a.Authorize(nil, deg); !ok {
		t.Fatal("degraded identity must pass the defaultRequire gate (fail-open auth)")
	}
	reqTrue := true
	if ok, _ := a.Authorize(&RuleAuthConfig{Require: &reqTrue}, deg); !ok {
		t.Fatal("degraded identity must pass a require-only rule (fail-open auth)")
	}

	// Authorization is still enforced: a scope-gated or identity-gated
	// rule must fail closed for a degraded id (it holds no scopes and a
	// non-allowlisted name).
	if ok, _ := a.Authorize(&RuleAuthConfig{Scopes: []string{"admin"}}, deg); ok {
		t.Fatal("degraded identity must NOT satisfy a scope-gated rule (no privilege escalation during outage)")
	}
	if ok, _ := a.Authorize(&RuleAuthConfig{Identities: []string{"someone-else"}}, deg); ok {
		t.Fatal("degraded identity must NOT satisfy an identity-allowlist rule")
	}

	// Control: a genuinely anonymous caller is still denied under
	// defaultRequire.
	if ok, _ := a.Authorize(nil, nil); ok {
		t.Fatal("anonymous must still be denied under defaultRequire (fail-closed)")
	}
	// Control: a verified identity missing the required scope is denied.
	plain := &Identity{Name: "u", Method: "api-key", Scopes: []string{"read"}}
	if ok, _ := a.Authorize(&RuleAuthConfig{Scopes: []string{"admin"}}, plain); ok {
		t.Fatal("non-degraded identity missing the required scope must be denied")
	}
}
