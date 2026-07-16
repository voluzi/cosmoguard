package testharness_test

import (
	"net/http"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

func ptrBool(b bool) *bool { return &b }

// TestI_AuthRequiredNoCredential — a rule with auth.require:true returns
// 401 when the client presents no credential, and the upstream is never
// hit.
func TestI_AuthRequiredNoCredential(t *testing.T) {
	cfg := authBaseConfig()
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth: &cosmoguard.RuleAuthConfig{
			Require: ptrBool(true),
		},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{"v":1}`),
	)

	resp := h.GET(t, h.LCDURL+"/private")
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/private"), 0,
		"upstream must not see an unauthenticated request to a require:true rule")
}

// TestI_AuthRequiredValidKey — same rule, valid API key in Authorization
// header (Bearer form) → 200 OK, upstream called.
func TestI_AuthRequiredValidKey(t *testing.T) {
	cfg := authBaseConfig()
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth: &cosmoguard.RuleAuthConfig{
			Require: ptrBool(true),
		},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{"v":1}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer test-key-1")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/private"), 1)
}

// TestI_AuthStripsCredentialHeader — the Authorization header MUST be
// removed before forwarding upstream. A Cosmos node should see exactly
// the request a direct caller would have sent.
func TestI_AuthStripsCredentialHeader(t *testing.T) {
	cfg := authBaseConfig()
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth: &cosmoguard.RuleAuthConfig{
			Require: ptrBool(true),
		},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer test-key-1")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	// Inspect what the fake upstream actually received.
	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 1)
	assert.Equal(t, calls[0].Headers.Get("Authorization"), "",
		"Authorization header must be stripped before upstream sees the request")
}

// TestI_AuthScopeEnforcement — rule requires `write` scope; an identity
// holding only `read` is denied; an identity with both passes.
func TestI_AuthScopeEnforcement(t *testing.T) {
	cfg := authBaseConfig()
	cfg.Auth.Identities = []cosmoguard.IdentityConfig{
		{Name: "reader", APIKey: "read-key", Scopes: []string{"read"}},
		{Name: "writer", APIKey: "write-key", Scopes: []string{"read", "write"}},
	}
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/sensitive"},
		Methods:  []string{http.MethodPost},
		Auth: &cosmoguard.RuleAuthConfig{
			Require: ptrBool(true),
			Scopes:  []string{"write"},
		},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodPost, "/sensitive", `{}`),
	)

	// Reader → denied (missing scope).
	req, _ := http.NewRequest(http.MethodPost, h.LCDURL+"/sensitive", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	r := h.Do(t, req)
	assert.Equal(t, r.StatusCode, http.StatusUnauthorized)

	// Writer → allowed.
	req2, _ := http.NewRequest(http.MethodPost, h.LCDURL+"/sensitive", nil)
	req2.Header.Set("Authorization", "Bearer write-key")
	r2 := h.Do(t, req2)
	assert.Equal(t, r2.StatusCode, http.StatusOK)
}

// authBaseConfig is the shared Config builder for I-* tests: two known
// identities, api-key auth method via Authorization header.
func authBaseConfig() *cosmoguard.Config {
	return &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		Auth: cosmoguard.AuthConfig{
			Enable: true,
			Methods: []cosmoguard.AuthMethodConfig{
				{Type: "api-key", Header: "Authorization"},
			},
			Identities: []cosmoguard.IdentityConfig{
				{Name: "test-1", APIKey: "test-key-1", Scopes: []string{"read"}},
			},
		},
		LCD: cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
}
