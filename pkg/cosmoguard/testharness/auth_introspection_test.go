package testharness_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestI_Introspection_Active: an RFC 7662 introspection response with
// active:true authenticates the request. The response is cached so the
// second request doesn't re-hit the issuer.
func TestI_Introspection_Active(t *testing.T) {
	var hits atomic.Int64
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		// Basic auth from the client (RFC 7662 §2.1)
		user, _, _ := r.BasicAuth()
		_ = user
		_ = r.ParseForm()
		token := r.FormValue("token")
		if token != "good-token" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"active":false}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"active":true,"sub":"alice","scope":"read write"}`))
	}))
	defer issuer.Close()

	cfg := authBaseConfig()
	cfg.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{
				Type:                  "introspection",
				Header:                "Authorization",
				IntrospectionEndpoint: issuer.URL,
				ClientID:              "cosmoguard",
				ClientSecret:          "secret",
				IdentityField:         "sub",
				ScopesField:           "scope",
				CacheTTL:              5 * time.Second,
				Timeout:               2 * time.Second,
				FailureMode:           "fail-closed",
			},
		},
	}
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: ptrBool(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer good-token")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, hits.Load(), int64(1))

	// Repeat — cached, issuer not hit again.
	resp2 := h.Do(t, req)
	assert.Equal(t, resp2.StatusCode, http.StatusOK)
	assert.Equal(t, hits.Load(), int64(1), "introspection result should have been cached")

	// Upstream gets no Authorization header.
	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 2)
	for i, c := range calls {
		assert.Equal(t, c.Headers.Get("Authorization"), "",
			"call %d: Authorization must be stripped before upstream", i)
	}
}

// TestI_Introspection_Inactive: active:false → 401.
func TestI_Introspection_Inactive(t *testing.T) {
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"active":false}`))
	}))
	defer issuer.Close()

	cfg := authBaseConfig()
	cfg.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{Type: "introspection", IntrospectionEndpoint: issuer.URL, FailureMode: "fail-closed"},
		},
	}
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: ptrBool(true)},
	}}

	h := testharness.New(t, testharness.WithConfig(cfg))
	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer expired-or-revoked")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
}
