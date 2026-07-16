package testharness

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
)

// boolPtr returns a pointer to b, for the *bool config fields
// (Metrics.Enable, RpcConfig.WebSocketEnabled).
func boolPtr(b bool) *bool { return &b }

// Option customizes a Harness before it boots.
type Option interface{ apply(*Harness) }

type funcOption func(*Harness)

func (f funcOption) apply(h *Harness) { f(h) }

// WithConfig overrides the default "allow everything" config with a custom
// in-memory Config. Use this from tests that build their config
// programmatically.
//
// Node/listen addresses set in the supplied config are ignored — the harness
// always assigns ephemeral ports and points cosmoguard at the fake upstream.
func WithConfig(cfg *cosmoguard.Config) Option {
	return funcOption(func(h *Harness) { h.cfg = cfg })
}

// WithConfigYAML parses the given YAML string as a Config. Convenient for
// tests that prefer the same shape an operator would write.
func WithConfigYAML(t *testing.T, src string) Option {
	t.Helper()
	var cfg cosmoguard.Config
	if err := yaml.Unmarshal([]byte(src), &cfg); err != nil {
		t.Fatalf("harness: parse config YAML: %v", err)
	}
	return WithConfig(&cfg)
}

// WithLCDResponse pre-loads a canned response on the fake upstream's LCD
// endpoint. Method is the HTTP method ("GET", "POST", ...); path is the URL
// path (matched exactly; query strings are ignored). body is served as the
// response body with status 200 and Content-Type: application/json.
//
// Use the FakeUpstream's fluent API for fuller control (status, headers,
// delay, dynamic per-request behavior).
func WithLCDResponse(method, path, body string) Option {
	return funcOption(func(h *Harness) {
		h.Upstream.LCD.SetResponse(method, path, FakeResponse{
			StatusCode: 200,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       []byte(body),
		})
	})
}

// WithRPCResponse is the RPC equivalent of WithLCDResponse for non-JSON-RPC
// endpoints (e.g. Tendermint's GET /status).
func WithRPCResponse(method, path, body string) Option {
	return funcOption(func(h *Harness) {
		h.Upstream.RPC.SetResponse(method, path, FakeResponse{
			StatusCode: 200,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       []byte(body),
		})
	})
}

// WithJSONRPCMethod registers a handler for one JSON-RPC method on the fake
// RPC upstream. The handler receives the parsed request and returns the
// result payload (or an error to send back as a JSON-RPC error).
func WithJSONRPCMethod(method string, h JSONRPCHandler) Option {
	return funcOption(func(harness *Harness) {
		harness.Upstream.RPC.SetJSONRPCHandler(method, h)
	})
}

// WithEVMEnabled flips on EVM proxies in the default config. Tests that
// pass their own Config via WithConfig must set EnableEvm there.
func WithEVMEnabled() Option {
	return funcOption(func(h *Harness) {
		if h.cfg == nil {
			h.cfg = defaultHarnessConfig()
		}
		h.cfg.EnableEvm = true
	})
}

// WithMetricsEnabled flips on the Prometheus endpoint. Each harness picks a
// fresh port so multiple instances can coexist.
func WithMetricsEnabled() Option {
	return funcOption(func(h *Harness) {
		if h.cfg == nil {
			h.cfg = defaultHarnessConfig()
		}
		h.cfg.Metrics.Enable = boolPtr(true)
	})
}

// WithDashboardEnabled flips on the standalone read-only dashboard
// (disabled in the harness default to keep parallel tests from
// racing for the default port). Each harness picks a fresh
// dashboard port so multiple instances can coexist. Optional basic-
// auth credentials may be passed; when both are empty the dashboard
// is reachable without auth (matching the production default).
func WithDashboardEnabled(basicAuthUser, basicAuthPassword string) Option {
	return funcOption(func(h *Harness) {
		if h.cfg == nil {
			h.cfg = defaultHarnessConfig()
		}
		on := true
		h.cfg.Dashboard.Enable = &on
		h.cfg.Dashboard.BasicAuthUser = basicAuthUser
		h.cfg.Dashboard.BasicAuthPassword = basicAuthPassword
	})
}
