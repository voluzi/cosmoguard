package testharness_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestDashboard_StandaloneListener exercises the standalone dashboard
// listener on its own port: HTML lands at /, JSON API at
// /api/v1/{info,upstreams,rules,identities}, and credentials never
// leak through the identities endpoint. Mirrors the existing
// TestJ_WebUI assertions but for the new mount point.
func TestDashboard_StandaloneListener(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache: cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
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
		testharness.WithDashboardEnabled("", ""),
	)
	assert.Assert(t, h.DashboardURL != "", "dashboard URL should be populated when enabled")

	// HTML lands at the root.
	r := h.GET(t, h.DashboardURL+"/")
	assert.Equal(t, r.StatusCode, http.StatusOK,
		"standalone dashboard should serve HTML at /")
	body := string(r.Body)
	assert.Assert(t, strings.Contains(body, "<title>CosmoGuard"),
		"index.html should contain a CosmoGuard <title>")
	// Polish assertion: dashboard markup includes the brand block.
	assert.Assert(t, strings.Contains(body, "CosmoGuard"),
		"dashboard HTML should reference CosmoGuard")

	// /api/v1/info exposes the same info the metrics-port handler does
	// (version, started_at, upstreams). Crucial for the dashboard to
	// avoid cross-origin fetches when the metrics port is gated.
	r = h.GET(t, h.DashboardURL+"/api/v1/info")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var info struct {
		Version       string `json:"version"`
		StartedAt     string `json:"started_at"`
		UptimeSeconds int    `json:"uptime_seconds"`
		Upstreams     struct {
			Count   int `json:"count"`
			Healthy int `json:"healthy"`
		} `json:"upstreams"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &info))
	assert.Assert(t, info.StartedAt != "", "info.started_at should be populated")
	assert.Assert(t, info.Upstreams.Count >= 1, "expected at least one upstream")

	// /api/v1/upstreams returns the configured node list.
	r = h.GET(t, h.DashboardURL+"/api/v1/upstreams")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var upDoc struct {
		Upstreams []struct {
			Name    string `json:"name"`
			Healthy bool   `json:"healthy"`
		} `json:"upstreams"`
	}
	assert.NilError(t, json.Unmarshal(r.Body, &upDoc))
	assert.Assert(t, len(upDoc.Upstreams) >= 1)

	// /api/v1/rules exposes compiled rules.
	r = h.GET(t, h.DashboardURL+"/api/v1/rules")
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
		}
	}
	assert.Assert(t, found, "expected to find the LCD rule in /api/v1/rules")

	// Identities API: scopes visible, secret never leaks.
	r = h.GET(t, h.DashboardURL+"/api/v1/identities")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	idBody := string(r.Body)
	assert.Assert(t, strings.Contains(idBody, "prod-app"))
	assert.Assert(t, strings.Contains(idBody, "broadcast"))
	assert.Assert(t, !strings.Contains(idBody, "REDACTED-SECRET-VALUE"),
		"identities endpoint MUST NOT leak the apiKey credential")
}

// TestDashboard_BasicAuth verifies the optional basic-auth gate
// challenges unauthenticated requests and accepts the configured
// credentials. Constant-time compare lives in basicAuthGate; this
// is the integration-level smoke that it's wired up.
func TestDashboard_BasicAuth(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("ops", "let-me-in"),
	)

	// No creds → 401 with WWW-Authenticate.
	r := h.GET(t, h.DashboardURL+"/api/v1/info")
	assert.Equal(t, r.StatusCode, http.StatusUnauthorized,
		"missing creds should yield 401")
	assert.Assert(t, strings.HasPrefix(r.Header.Get("WWW-Authenticate"), "Basic"),
		"401 should carry a Basic WWW-Authenticate challenge")

	// Wrong creds → 401.
	req, err := http.NewRequest(http.MethodGet, h.DashboardURL+"/api/v1/info", nil)
	assert.NilError(t, err)
	req.SetBasicAuth("ops", "wrong")
	r = h.Do(t, req)
	assert.Equal(t, r.StatusCode, http.StatusUnauthorized,
		"wrong creds should yield 401")

	// Correct creds → 200 JSON.
	req, err = http.NewRequest(http.MethodGet, h.DashboardURL+"/api/v1/info", nil)
	assert.NilError(t, err)
	req.SetBasicAuth("ops", "let-me-in")
	r = h.Do(t, req)
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Assert(t, strings.Contains(r.Header.Get("Content-Type"), "application/json"))
}

// TestDashboard_DisabledByDefault_InHarness pins the contract that
// the test harness default-disables the dashboard (to avoid port
// collisions across parallel tests). A regression that flipped the
// harness to default-on would surface here as a `DashboardURL != ""`
// failure even though the test never called WithDashboardEnabled.
func TestDashboard_DisabledByDefault_InHarness(t *testing.T) {
	h := testharness.New(t)
	assert.Equal(t, h.DashboardURL, "",
		"harness default must NOT enable the dashboard (parallel tests would collide)")
	assert.Equal(t, h.CosmoGuard.DashboardPort(), 0,
		"DashboardPort() should be 0 when the dashboard is disabled")
}

// TestDashboard_BasicAuth_LengthMismatch pins the fix for the
// audit finding that subtle.ConstantTimeCompare on a different-
// length input returns 0 immediately, leaking the configured
// credential length via timing. Functionally: both a much-shorter
// and a much-longer wrong password must produce the same 401 with
// the same WWW-Authenticate header — i.e. the auth path doesn't
// gain extra information from the length of the supplied value.
// This is a behavior-level pin (not a timing measurement); the
// timing-safety itself comes from hashing both sides to fixed-size
// digests before the compare.
func TestDashboard_BasicAuth_LengthMismatch(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("ops", "correct-horse-battery-staple"),
	)

	for _, tc := range []struct {
		name           string
		user, password string
	}{
		{"empty pass", "ops", ""},
		{"short pass", "ops", "x"},
		{"long pass", "ops", strings.Repeat("z", 4096)},
		{"empty user", "", "correct-horse-battery-staple"},
		{"long user", strings.Repeat("u", 4096), "correct-horse-battery-staple"},
		{"both wrong", "nope", "nope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, h.DashboardURL+"/api/v1/info", nil)
			assert.NilError(t, err)
			req.SetBasicAuth(tc.user, tc.password)
			r := h.Do(t, req)
			assert.Equal(t, r.StatusCode, http.StatusUnauthorized,
				"all wrong-cred shapes should return 401, got %d", r.StatusCode)
			assert.Assert(t, strings.HasPrefix(r.Header.Get("WWW-Authenticate"), "Basic"),
				"401 should carry a Basic WWW-Authenticate challenge")
		})
	}
}

// TestDashboard_UnknownAPIPathReturns404 pins the fix for the
// catch-all-static fallthrough: previously a typo like
// /api/v1/info/anything fell through to the static handler mounted
// at "/" and returned index.html with 200, silently masking the
// bad path. The dashboard now answers JSON 404 for unknown
// /api/v1/* segments.
func TestDashboard_UnknownAPIPathReturns404(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("", ""),
	)
	for _, p := range []string{
		"/api/v1/info/anything",
		"/api/v1/info/",
		"/api/v1/upstreams/bogus",
		"/api/v1/rules/extra/path",
		"/api/v1/identities/nope",
		"/api/v1/unknown",
	} {
		t.Run(p, func(t *testing.T) {
			r := h.GET(t, h.DashboardURL+p)
			assert.Equal(t, r.StatusCode, http.StatusNotFound,
				"unknown api path %q should be 404, got %d (body: %s)",
				p, r.StatusCode, string(r.Body))
			assert.Assert(t, strings.Contains(r.Header.Get("Content-Type"), "application/json"),
				"404 should be JSON-typed, got %q", r.Header.Get("Content-Type"))
		})
	}
}

// TestDashboard_JSONSecurityHeaders pins X-Content-Type-Options:
// nosniff and Cache-Control: no-store on every JSON endpoint.
// nosniff stops a browser from MIME-sniffing JSON into HTML (e.g.
// off an attacker-controlled identity name). no-store keeps live
// state out of intermediary caches and the browser bfcache.
func TestDashboard_JSONSecurityHeaders(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("", ""),
	)
	for _, p := range []string{
		"/api/v1/info",
		"/api/v1/upstreams",
		"/api/v1/rules",
		"/api/v1/identities",
	} {
		t.Run(p, func(t *testing.T) {
			r := h.GET(t, h.DashboardURL+p)
			assert.Equal(t, r.StatusCode, http.StatusOK)
			assert.Equal(t, r.Header.Get("X-Content-Type-Options"), "nosniff",
				"missing nosniff on %s", p)
			assert.Equal(t, r.Header.Get("Cache-Control"), "no-store",
				"missing no-store on %s", p)
		})
	}
}

// TestDashboard_HTMLClickjackingHeaders pins the static-shell
// hardening: an attacker page must not be able to iframe the
// dashboard (and coerce clicks against an already-authenticated
// operator), and the browser must not load remote-origin scripts
// into the dashboard origin. X-Frame-Options: DENY + the CSP
// frame-ancestors 'none' belt-and-braces this for old + modern
// browsers; the default-src 'self' confines fetches.
func TestDashboard_HTMLClickjackingHeaders(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("", ""),
	)
	r := h.GET(t, h.DashboardURL+"/")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Equal(t, r.Header.Get("X-Frame-Options"), "DENY",
		"static HTML must DENY framing")
	csp := r.Header.Get("Content-Security-Policy")
	assert.Assert(t, strings.Contains(csp, "frame-ancestors 'none'"),
		"CSP must include frame-ancestors 'none', got %q", csp)
	assert.Assert(t, strings.Contains(csp, "default-src 'self'"),
		"CSP must confine fetches to 'self', got %q", csp)
	assert.Equal(t, r.Header.Get("Referrer-Policy"), "no-referrer",
		"static HTML must suppress Referer leaks")
	assert.Equal(t, r.Header.Get("X-Content-Type-Options"), "nosniff",
		"static HTML must carry nosniff")
}

// TestDashboard_PortCollisionRejected pins the validateListenerPorts
// guard: an operator who points the dashboard at the same port as
// metrics (or rpc, or lcd, …) gets a startup error instead of a
// confusing "address in use" deep in Run.
func TestDashboard_PortCollisionRejected(t *testing.T) {
	dashEnable := true
	cfg := &cosmoguard.Config{
		Host:      "127.0.0.1",
		RpcPort:   17777,
		LcdPort:   17776,
		GrpcPort:  17775,
		Metrics:   cosmoguard.MetricsConfig{Enable: true, Port: 17777},
		Dashboard: cosmoguard.DashboardConfig{Enable: &dashEnable, Port: 17777},
		LCD:       cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
		Nodes: []cosmoguard.NodeConfig{
			{Host: "127.0.0.1", LcdPort: 1317, RpcPort: 26657, GrpcPort: 9090},
		},
	}
	_, err := cosmoguard.New(cfg)
	assert.Assert(t, err != nil, "duplicate port across listeners must be rejected")
	assert.Assert(t, strings.Contains(err.Error(), "port collision"),
		"error should name the collision: %v", err)
}
