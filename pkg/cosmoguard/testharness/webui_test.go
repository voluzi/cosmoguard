package testharness_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestJ_WebUI: the embedded dashboard serves HTML at /admin/, the JSON
// API exposes upstreams + rules + identities, and credentials never
// leak through the identities endpoint.
func TestJ_WebUI(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache: cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{
			Enable: true,
			// Dashboard is off by default in v4.0.0 to avoid leaking
			// upstream targets / identity scopes on a metrics port
			// that's often cluster-wide reachable. Opt in here.
			WebUI: cosmoguard.WebUIConfig{Enable: true},
		},
		Auth: cosmoguard.AuthConfig{
			Enable: true,
			Methods: []cosmoguard.AuthMethodConfig{
				{Type: "api-key", Header: "Authorization"},
			},
			Identities: []cosmoguard.IdentityConfig{
				{Name: "prod-app", APIKey: "REDACTED-SECRET-VALUE", Scopes: []string{"read", "broadcast"}},
			},
		},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100,
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/cosmos/bank/v1beta1/balances/*"},
					Methods:  []string{http.MethodGet},
					Cache:    &cosmoguard.RuleCache{Enable: true, TTL: 10 * time.Second},
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
		testharness.WithMetricsEnabled(),
	)
	base := fmt.Sprintf("http://127.0.0.1:%d", h.CosmoGuard.MetricsPort())

	// SPA HTML lands at /admin/.
	r := h.GET(t, base+"/admin/")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	// Loose title check: pin the brand without demanding the exact
	// suffix, so a UI polish that appends " · dashboard" doesn't
	// flag a false positive on a routing change.
	assert.Assert(t, strings.Contains(string(r.Body), "<title>CosmoGuard"),
		"index.html should contain a CosmoGuard <title>")

	// Upstreams API returns the configured node list with health.
	r = h.GET(t, base+"/admin/api/v1/upstreams")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var upDoc struct {
		Upstreams []struct {
			Name    string `json:"name"`
			Healthy bool   `json:"healthy"`
		} `json:"upstreams"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &upDoc))
	assert.Assert(t, len(upDoc.Upstreams) >= 1)

	// Rules API exposes the configured LCD rule.
	r = h.GET(t, base+"/admin/api/v1/rules")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var rulesDoc struct {
		Rules []struct {
			Section      string `json:"section"`
			Action       string `json:"action"`
			MatchSummary string `json:"match_summary"`
			Cache        string `json:"cache"`
		} `json:"rules"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &rulesDoc))
	found := false
	for _, rl := range rulesDoc.Rules {
		if rl.Section == "lcd" && rl.Action == "allow" {
			found = true
			assert.Assert(t, strings.Contains(rl.MatchSummary, "/cosmos/bank/v1beta1/balances/*"),
				"match summary should contain configured path: %s", rl.MatchSummary)
			assert.Assert(t, strings.Contains(rl.Cache, "ttl="),
				"cache summary should mention ttl: %s", rl.Cache)
		}
	}
	assert.Assert(t, found, "expected to find the LCD rule in /admin/api/v1/rules")

	// Identities API exposes the name + scopes but NEVER the credential.
	r = h.GET(t, base+"/admin/api/v1/identities")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	body := string(r.Body)
	assert.Assert(t, strings.Contains(body, "prod-app"))
	assert.Assert(t, strings.Contains(body, "broadcast"))
	assert.Assert(t, !strings.Contains(body, "REDACTED-SECRET-VALUE"),
		"identities endpoint MUST NOT leak the apiKey credential")
}
