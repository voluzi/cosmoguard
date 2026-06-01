package testharness_test

import (
	"net/http"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestJ_CORS_PreflightAllowed: an OPTIONS preflight from an allowed
// origin returns 204 with the appropriate Access-Control-Allow-* headers
// and is NOT forwarded to upstream.
func TestJ_CORS_PreflightAllowed(t *testing.T) {
	cfg := corsBaseConfig()
	cfg.CORS = cosmoguard.CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders: []string{"Authorization", "Content-Type"},
		MaxAge:         600 * time.Second,
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/x", `{}`),
	)

	req, _ := http.NewRequest(http.MethodOptions, h.LCDURL+"/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization")

	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusNoContent)
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Origin"), "https://app.example.com")
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET, POST, OPTIONS")
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Headers"), "Authorization, Content-Type")
	assert.Equal(t, resp.Header.Get("Access-Control-Max-Age"), "600")

	// Preflight MUST NOT have reached upstream.
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodOptions, "/x"), 0)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/x"), 0)
}

// TestJ_CORS_PreflightDenied: a preflight from a non-allowlisted Origin
// returns 403 without CORS headers.
func TestJ_CORS_PreflightDenied(t *testing.T) {
	cfg := corsBaseConfig()
	cfg.CORS = cosmoguard.CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://app.example.com"},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))

	req, _ := http.NewRequest(http.MethodOptions, h.LCDURL+"/x", nil)
	req.Header.Set("Origin", "https://evil.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusForbidden)
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Origin"), "",
		"denied preflight must not leak Allow-Origin")
}

// TestJ_CORS_StripsUpstreamHeaders: when upstream returns its own
// Access-Control-Allow-Origin: * (Cosmos node default), cosmoguard
// strips it and replaces with its own policy.
func TestJ_CORS_StripsUpstreamHeaders(t *testing.T) {
	cfg := corsBaseConfig()
	cfg.CORS = cosmoguard.CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://app.example.com"},
	}

	h := testharness.New(t, testharness.WithConfig(cfg))
	h.Upstream.LCD.SetResponse(http.MethodGet, "/x", testharness.FakeResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":                "application/json",
			"Access-Control-Allow-Origin": "*", // upstream's default — must be stripped
		},
		Body: []byte(`{}`),
	})

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Origin"), "https://app.example.com",
		"upstream's '*' must be replaced with cosmoguard's specific origin")
}

// TestJ_CORS_DisallowedOriginNoHeaders: a plain GET from a non-
// allowlisted Origin gets the response body fine (CORS is about JS
// access, not authorization) but with no Access-Control-Allow-* headers,
// so the browser will block the JS from reading it.
func TestJ_CORS_DisallowedOriginNoHeaders(t *testing.T) {
	cfg := corsBaseConfig()
	cfg.CORS = cosmoguard.CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://app.example.com"},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/x", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/x", nil)
	req.Header.Set("Origin", "https://evil.com")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, resp.Header.Get("Access-Control-Allow-Origin"), "")
}

func corsBaseConfig() *cosmoguard.Config {
	return &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: false},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
}
