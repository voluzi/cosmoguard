package cosmoguard

import (
	"net/http"
	"testing"
)

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

// TestStripCredentialQuery is the regression test for the query-string
// API-key leak: when an api-key method authenticates from ?api_key=...,
// StripCredentialQuery must remove that param from the outbound URL so
// the credential is not forwarded upstream. Non-credential params are
// preserved.
func TestStripCredentialQuery(t *testing.T) {
	cfg := &AuthConfig{
		Enable: true,
		Methods: []AuthMethodConfig{{
			Type:       "api-key",
			Header:     "x-api-key",
			QueryParam: "api_key",
		}},
		Identities: []IdentityConfig{{Name: "c", APIKey: "secret", Scopes: []string{"read"}}},
	}
	a, err := NewAuthenticator(cfg, nil)
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	r, _ := http.NewRequest(http.MethodGet, "http://x/cosmos/foo?api_key=secret&height=42", nil)
	a.StripCredentialQuery(r)

	q := r.URL.Query()
	if q.Has("api_key") {
		t.Fatalf("api_key must be stripped from outbound URL, got %q", r.URL.RawQuery)
	}
	if q.Get("height") != "42" {
		t.Fatalf("non-credential param must be preserved, got %q", r.URL.RawQuery)
	}

	// No-queryParam config → no-op (and never panics on empty query).
	cfg2 := &AuthConfig{Enable: true, Methods: []AuthMethodConfig{{Type: "api-key", Header: "x-api-key"}},
		Identities: []IdentityConfig{{Name: "c", APIKey: "secret"}}}
	a2, _ := NewAuthenticator(cfg2, nil)
	t.Cleanup(func() { _ = a2.Close() })
	r2, _ := http.NewRequest(http.MethodGet, "http://x/foo?api_key=secret", nil)
	a2.StripCredentialQuery(r2)
	if r2.URL.Query().Get("api_key") != "secret" {
		t.Fatal("a method without queryParam must not strip ?api_key")
	}
}
