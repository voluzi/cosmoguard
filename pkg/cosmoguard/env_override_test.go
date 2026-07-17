package cosmoguard

import (
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

// freshConfig returns a Config populated by PrepareConfig with no
// YAML and no env overrides — i.e. all-defaults plus the v3→v4
// promotion. Tests then assert that with a single COSMOGUARD_* env
// var set, the corresponding field is overridden while everything
// else stays at its default.
func freshConfig(t *testing.T) *Config {
	t.Helper()
	cfg := &Config{}
	assert.NilError(t, PrepareConfig(cfg))
	return cfg
}

func TestApplyEnvOverrides_Listener(t *testing.T) {
	t.Setenv("COSMOGUARD_HOST", "127.0.0.99")
	t.Setenv("COSMOGUARD_RPC_PORT", "27017")
	t.Setenv("COSMOGUARD_LCD_PORT", "11337")
	t.Setenv("COSMOGUARD_GRPC_PORT", "19191")
	t.Setenv("COSMOGUARD_ENABLE_EVM", "true")
	t.Setenv("COSMOGUARD_EVM_RPC_PORT", "18000")
	t.Setenv("COSMOGUARD_EVM_RPC_WS_PORT", "18001")
	t.Setenv("COSMOGUARD_METRICS_ENABLE", "false")
	t.Setenv("COSMOGUARD_METRICS_PORT", "9091")

	cfg := freshConfig(t)
	assert.Equal(t, cfg.Host, "127.0.0.99")
	assert.Equal(t, cfg.RpcPort, 27017)
	assert.Equal(t, cfg.LcdPort, 11337)
	assert.Equal(t, cfg.GrpcPort, 19191)
	assert.Equal(t, cfg.EnableEvm, true)
	assert.Equal(t, cfg.EvmRpcPort, 18000)
	assert.Equal(t, cfg.EvmRpcWsPort, 18001)
	assert.Equal(t, cfg.Metrics.IsEnabled(), false)
	assert.Equal(t, cfg.Metrics.Port, 9091)
}

func TestApplyEnvOverrides_Node(t *testing.T) {
	t.Setenv("COSMOGUARD_NODE_NAME", "primary")
	t.Setenv("COSMOGUARD_NODE_HOST", "node.example.com")
	t.Setenv("COSMOGUARD_NODE_TLS", "true")
	t.Setenv("COSMOGUARD_NODE_RPC_PORT", "26658")
	t.Setenv("COSMOGUARD_NODE_LCD_PORT", "1318")
	t.Setenv("COSMOGUARD_NODE_GRPC_PORT", "9091")
	t.Setenv("COSMOGUARD_NODE_EVM_RPC_PORT", "8546")
	t.Setenv("COSMOGUARD_NODE_EVM_RPC_WS_PORT", "8547")

	cfg := freshConfig(t)
	n := cfg.Nodes[0]
	assert.Equal(t, n.Name, "primary")
	assert.Equal(t, n.Host, "node.example.com")
	assert.Equal(t, n.TLS, true)
	assert.Equal(t, n.RpcPort, 26658)
	assert.Equal(t, n.LcdPort, 1318)
	assert.Equal(t, n.GrpcPort, 9091)
	assert.Equal(t, n.EvmRpcPort, 8546)
	assert.Equal(t, n.EvmRpcWsPort, 8547)
}

func TestApplyEnvOverrides_NodeURLs(t *testing.T) {
	t.Setenv("COSMOGUARD_NODE_RPC_URL", "https://rpc.example.com")
	t.Setenv("COSMOGUARD_NODE_LCD_URL", "https://lcd.example.com")
	t.Setenv("COSMOGUARD_NODE_GRPC_URL", "grpcs://grpc.example.com")
	t.Setenv("COSMOGUARD_NODE_EVM_RPC_URL", "https://evm-rpc.example.com")
	t.Setenv("COSMOGUARD_NODE_EVM_RPC_WS_URL", "wss://evm-rpc-ws.example.com")

	cfg := freshConfig(t)
	n := cfg.Nodes[0]
	assert.Equal(t, n.RpcURL, "https://rpc.example.com")
	assert.Equal(t, n.LcdURL, "https://lcd.example.com")
	assert.Equal(t, n.GrpcURL, "grpcs://grpc.example.com")
	assert.Equal(t, n.EvmRpcURL, "https://evm-rpc.example.com")
	assert.Equal(t, n.EvmRpcWsURL, "wss://evm-rpc-ws.example.com")
}

func TestApplyEnvOverrides_DiscoveryCreatedOnDemand(t *testing.T) {
	// Operator points cosmopilot at a K8s headless service via env
	// only; the YAML has no `discovery:` block. Auto-create one
	// with type=dns and refreshInterval=15s defaults so the node
	// passes validateNodeDiscovery.
	t.Setenv("COSMOGUARD_DISCOVERY_HOST", "myservice.ns.svc.cluster.local")

	cfg := freshConfig(t)
	n := cfg.Nodes[0]
	assert.Assert(t, n.Discovery != nil, "discovery block should be created when COSMOGUARD_DISCOVERY_HOST is set")
	assert.Equal(t, n.Discovery.Host, "myservice.ns.svc.cluster.local")
	assert.Equal(t, n.Discovery.Type, "dns")
	assert.Equal(t, n.Discovery.RefreshInterval, 15*time.Second)
}

func TestApplyEnvOverrides_DiscoveryAllFields(t *testing.T) {
	t.Setenv("COSMOGUARD_DISCOVERY_HOST", "headless.svc.local")
	t.Setenv("COSMOGUARD_DISCOVERY_TYPE", "dns")
	t.Setenv("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL", "30s")

	cfg := freshConfig(t)
	d := cfg.Nodes[0].Discovery
	assert.Assert(t, d != nil)
	assert.Equal(t, d.Host, "headless.svc.local")
	assert.Equal(t, d.Type, "dns")
	assert.Equal(t, d.RefreshInterval, 30*time.Second)
}

func TestApplyEnvOverrides_DiscoveryRefreshOnly_NoHost(t *testing.T) {
	// Setting only TYPE/REFRESH creates an empty-host Discovery
	// which validateNodeDiscovery must reject — otherwise a typo
	// in cosmopilot would silently produce a non-resolving config.
	t.Setenv("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL", "10s")

	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "discovery without host should fail validation")
	assert.Assert(t, strings.Contains(err.Error(), "discovery.host"),
		"error should mention missing discovery.host, got %q", err.Error())
}

func TestApplyEnvOverrides_DiscoveryConflictWithStaticHost(t *testing.T) {
	// Discovery + an env-set static Host on the same node is
	// ambiguous — discovered IPs would silently overwrite the
	// static value. validateNodeDiscovery rejects the combination.
	t.Setenv("COSMOGUARD_DISCOVERY_HOST", "svc.local")
	t.Setenv("COSMOGUARD_NODE_HOST", "10.0.0.1")

	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "discovery + static host should fail")
	// Pin the specific validateNodeDiscovery message rather than the
	// loose word "discovery" — otherwise a future error in the
	// auto-create path that happens to contain "discovery" would
	// satisfy this test for the wrong reason.
	assert.Assert(t, strings.Contains(err.Error(), "discovered IPs supply the host"),
		"error should describe the static-host conflict, got %q", err.Error())
}

func TestApplyEnvOverrides_BadInteger(t *testing.T) {
	t.Setenv("COSMOGUARD_RPC_PORT", "not-a-number")

	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_RPC_PORT"),
		"error should mention the offending var, got %q", err.Error())
}

func TestApplyEnvOverrides_BadBoolean(t *testing.T) {
	t.Setenv("COSMOGUARD_NODE_TLS", "definitely")

	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_NODE_TLS"),
		"error should mention the offending var, got %q", err.Error())
}

func TestApplyEnvOverrides_BadRefreshInterval(t *testing.T) {
	t.Setenv("COSMOGUARD_DISCOVERY_HOST", "svc.local")
	t.Setenv("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL", "notaduration")

	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_DISCOVERY_REFRESH_INTERVAL"),
		"error should mention the offending var, got %q", err.Error())
}

func TestApplyEnvOverrides_EmptyTreatedAsUnset(t *testing.T) {
	// Empty value means "not provided" — matches EnvInterpolate
	// semantics so an unfilled Helm template (`--set host=`)
	// doesn't silently zero out the configured value.
	t.Setenv("COSMOGUARD_NODE_HOST", "")
	t.Setenv("COSMOGUARD_NODE_NAME", "")

	cfg := freshConfig(t)
	// Defaults should survive: host stays at 127.0.0.1, name is
	// auto-assigned to "node-0".
	assert.Equal(t, cfg.Nodes[0].Host, "127.0.0.1")
	assert.Equal(t, cfg.Nodes[0].Name, "node-0")
}

func TestApplyEnvOverrides_EmptySetTreatedAsUnset(t *testing.T) {
	// Sanity check: with every env var set to the empty string,
	// PrepareConfig produces the same shape as the all-defaults
	// case. t.Setenv("", "") still SETS the var (to empty) — the
	// contract verified here is that lookupNonEmptyEnv treats
	// set-but-empty identically to unset, so an unfilled Helm
	// template doesn't silently zero out the configured value.
	for _, name := range []string{
		"COSMOGUARD_HOST", "COSMOGUARD_RPC_PORT", "COSMOGUARD_LCD_PORT",
		"COSMOGUARD_GRPC_PORT", "COSMOGUARD_ENABLE_EVM",
		"COSMOGUARD_EVM_RPC_PORT", "COSMOGUARD_EVM_RPC_WS_PORT",
		"COSMOGUARD_METRICS_ENABLE", "COSMOGUARD_METRICS_PORT",
		"COSMOGUARD_NODE_NAME", "COSMOGUARD_NODE_HOST", "COSMOGUARD_NODE_TLS",
		"COSMOGUARD_NODE_RPC_PORT", "COSMOGUARD_NODE_LCD_PORT",
		"COSMOGUARD_NODE_GRPC_PORT", "COSMOGUARD_NODE_EVM_RPC_PORT",
		"COSMOGUARD_NODE_EVM_RPC_WS_PORT",
		"COSMOGUARD_NODE_RPC_URL", "COSMOGUARD_NODE_LCD_URL",
		"COSMOGUARD_NODE_GRPC_URL", "COSMOGUARD_NODE_EVM_RPC_URL",
		"COSMOGUARD_NODE_EVM_RPC_WS_URL",
		"COSMOGUARD_DISCOVERY_HOST", "COSMOGUARD_DISCOVERY_TYPE",
		"COSMOGUARD_DISCOVERY_REFRESH_INTERVAL",
	} {
		t.Setenv(name, "")
	}

	cfg := freshConfig(t)
	// All defaults intact.
	assert.Equal(t, cfg.Host, "0.0.0.0")
	assert.Equal(t, cfg.RpcPort, 16657)
	assert.Equal(t, cfg.Nodes[0].Host, "127.0.0.1")
	assert.Equal(t, cfg.Nodes[0].TLS, false)
	assert.Equal(t, cfg.Nodes[0].Name, "node-0")
	assert.Assert(t, cfg.Nodes[0].Discovery == nil)
}

func TestApplyEnvOverrides_EnvBeatsYAML(t *testing.T) {
	// YAML provides a host; env overrides it. The order
	// guarantee (env > YAML > defaults) is the entire point of
	// the layer — verify directly.
	t.Setenv("COSMOGUARD_NODE_HOST", "fromenv.example.com")

	cfg := &Config{
		Node: NodeConfig{Host: "fromyaml.example.com"},
	}
	assert.NilError(t, PrepareConfig(cfg))
	assert.Equal(t, cfg.Nodes[0].Host, "fromenv.example.com")
}

func TestApplyEnvOverrides_NameAutoAssignedAfterEnv(t *testing.T) {
	// With no name in YAML and no COSMOGUARD_NODE_NAME, the
	// per-node loop assigns "node-0". With COSMOGUARD_NODE_NAME
	// set, the env value must survive the auto-assignment.
	t.Setenv("COSMOGUARD_NODE_NAME", "edge-pod")

	cfg := freshConfig(t)
	assert.Equal(t, cfg.Nodes[0].Name, "edge-pod")
}

func TestApplyEnvOverrides_PortRangeRejected(t *testing.T) {
	cases := []struct {
		envVal  string
		wantErr string
	}{
		{"-1", "out of range"},
		{"0", "out of range"},
		{"65536", "out of range"},
		{"99999", "out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.envVal, func(t *testing.T) {
			t.Setenv("COSMOGUARD_RPC_PORT", tc.envVal)
			cfg := &Config{}
			err := PrepareConfig(cfg)
			assert.Assert(t, err != nil, "port %q should be rejected", tc.envVal)
			assert.Assert(t, strings.Contains(err.Error(), tc.wantErr),
				"error %q should contain %q", err.Error(), tc.wantErr)
			assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_RPC_PORT"),
				"error %q should name the env var", err.Error())
		})
	}
}

func TestApplyEnvOverrides_BoolVocabulary(t *testing.T) {
	// Operators set TLS via Helm/YAML idioms — yes/no/on/off must
	// work, not just true/false. Each tested twice (case variants)
	// so a future case-fold regression surfaces directly.
	trueVals := []string{"true", "TRUE", "1", "yes", "YES", "on", "ON", "t", "y", "Y"}
	for _, v := range trueVals {
		t.Run("true/"+v, func(t *testing.T) {
			t.Setenv("COSMOGUARD_NODE_TLS", v)
			cfg := freshConfig(t)
			assert.Equal(t, cfg.Nodes[0].TLS, true, "value %q should parse to true", v)
		})
	}
	falseVals := []string{"false", "FALSE", "0", "no", "NO", "off", "OFF", "f", "n", "N"}
	for _, v := range falseVals {
		t.Run("false/"+v, func(t *testing.T) {
			t.Setenv("COSMOGUARD_NODE_TLS", v)
			cfg := freshConfig(t)
			assert.Equal(t, cfg.Nodes[0].TLS, false, "value %q should parse to false", v)
		})
	}
}

func TestApplyEnvOverrides_StringTrimsWhitespace(t *testing.T) {
	// A YAML/Helm template that produces `COSMOGUARD_NODE_HOST=" 10.0.0.1 "`
	// (e.g. preserved indentation around an interpolated value) should
	// resolve to "10.0.0.1", not "no such host" at dial time.
	t.Setenv("COSMOGUARD_NODE_HOST", "  10.0.0.1  ")
	cfg := freshConfig(t)
	assert.Equal(t, cfg.Nodes[0].Host, "10.0.0.1")
}

func TestApplyEnvOverrides_BoolTrimsWhitespace(t *testing.T) {
	// Same whitespace tolerance as envStr: `" true "` from a
	// padded template must still parse. strconv.ParseBool itself
	// does NOT trim, so the helper must do it explicitly.
	for _, v := range []string{" true ", "\ttrue\n", "  yes  ", "\n1\r"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("COSMOGUARD_NODE_TLS", v)
			cfg := freshConfig(t)
			assert.Equal(t, cfg.Nodes[0].TLS, true, "value %q should parse as true", v)
		})
	}
}

func TestApplyEnvOverrides_ClearsV3Node(t *testing.T) {
	// PrepareConfig promotes the v3 singular `node:` into Nodes[0]
	// then zeros out cfg.Node. The zero is defensive: any code
	// that yaml-marshals the post-PrepareConfig config back out
	// (e.g. an admin dashboard exporting the live config) gets a
	// clean v4 shape with no stale `node:` section.
	cfg := &Config{Node: NodeConfig{Host: "10.0.0.1", RpcPort: 26658}}
	assert.NilError(t, PrepareConfig(cfg))
	assert.Equal(t, cfg.Nodes[0].Host, "10.0.0.1")
	assert.Equal(t, cfg.Node.Host, "")
	assert.Equal(t, cfg.Node.RpcPort, 0)
}

func TestApplyEnvOverrides_IPv6HostAccepted(t *testing.T) {
	// An IPv6 literal in COSMOGUARD_NODE_HOST is accepted by the
	// env layer as a raw string — downstream pool code is
	// responsible for bracketing on use (`net.JoinHostPort` does
	// this automatically). This test pins the documented
	// behavior so a future "reject brackets" hardening would be a
	// deliberate decision, not a silent regression.
	for _, host := range []string{"::1", "[::1]", "fd00::1"} {
		t.Run(host, func(t *testing.T) {
			t.Setenv("COSMOGUARD_NODE_HOST", host)
			cfg := freshConfig(t)
			assert.Equal(t, cfg.Nodes[0].Host, host)
		})
	}
}

func TestApplyEnvOverrides_PrepareConfigIdempotent(t *testing.T) {
	// PrepareConfig's godoc claims idempotency. With env overrides
	// in the loop, two consecutive PrepareConfig calls on the
	// same struct must produce the same shape — neither
	// duplicating nodes (a buggy re-promotion would append
	// another Nodes[0]) nor losing env-applied values.
	t.Setenv("COSMOGUARD_NODE_HOST", "env.example.com")
	cfg := &Config{}
	assert.NilError(t, PrepareConfig(cfg))
	assert.NilError(t, PrepareConfig(cfg))
	assert.Equal(t, len(cfg.Nodes), 1)
	assert.Equal(t, cfg.Nodes[0].Host, "env.example.com")
}

func TestApplyEnvOverrides_RefreshIntervalNonPositive(t *testing.T) {
	for _, v := range []string{"0s", "-5s"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("COSMOGUARD_DISCOVERY_HOST", "svc.local")
			t.Setenv("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL", v)
			cfg := &Config{}
			err := PrepareConfig(cfg)
			assert.Assert(t, err != nil, "refresh interval %q should be rejected", v)
			assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_DISCOVERY_REFRESH_INTERVAL"),
				"error should name the env var, got %q", err.Error())
		})
	}
}

func TestApplyEnvOverrides_PrecedenceTable(t *testing.T) {
	// Each row: a YAML config with a non-default value + an env
	// override → env must win. The original NodeHost test only
	// covers one field; this table catches a precedence regression
	// in any of the scalar paths.
	type row struct {
		name   string
		setEnv func(t *testing.T)
		yaml   *Config
		check  func(t *testing.T, cfg *Config)
	}
	rows := []row{
		{
			name:   "RpcPort",
			setEnv: func(t *testing.T) { t.Setenv("COSMOGUARD_RPC_PORT", "20000") },
			yaml:   &Config{RpcPort: 30000},
			check:  func(t *testing.T, cfg *Config) { assert.Equal(t, cfg.RpcPort, 20000) },
		},
		{
			name:   "MetricsEnable",
			setEnv: func(t *testing.T) { t.Setenv("COSMOGUARD_METRICS_ENABLE", "false") },
			yaml:   &Config{Metrics: MetricsConfig{Enable: func() *bool { b := true; return &b }()}},
			check:  func(t *testing.T, cfg *Config) { assert.Equal(t, cfg.Metrics.IsEnabled(), false) },
		},
		{
			name:   "MetricsPort",
			setEnv: func(t *testing.T) { t.Setenv("COSMOGUARD_METRICS_PORT", "9888") },
			yaml:   &Config{Metrics: MetricsConfig{Port: 7777}},
			check:  func(t *testing.T, cfg *Config) { assert.Equal(t, cfg.Metrics.Port, 9888) },
		},
		{
			name:   "NodeRpcURL",
			setEnv: func(t *testing.T) { t.Setenv("COSMOGUARD_NODE_RPC_URL", "https://env.example.com") },
			yaml:   &Config{Nodes: []NodeConfig{{RpcURL: "https://yaml.example.com"}}},
			check:  func(t *testing.T, cfg *Config) { assert.Equal(t, cfg.Nodes[0].RpcURL, "https://env.example.com") },
		},
		{
			name:   "DiscoveryHost",
			setEnv: func(t *testing.T) { t.Setenv("COSMOGUARD_DISCOVERY_HOST", "env.svc.local") },
			yaml: &Config{Nodes: []NodeConfig{{
				Discovery: &DiscoveryConfig{Host: "yaml.svc.local", Type: "dns"},
			}}},
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, cfg.Nodes[0].Discovery.Host, "env.svc.local")
			},
		},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			r.setEnv(t)
			assert.NilError(t, PrepareConfig(r.yaml))
			r.check(t, r.yaml)
		})
	}
}

func TestApplyEnvOverrides_URLAndPortCoexist(t *testing.T) {
	// When both a per-service URL and a port are set on the same
	// service, both fields are populated — downstream pools key off
	// URL (per NodeConfig docs) and ignore the port. This test
	// pins the documented behavior so a future "reject both" check
	// would be a deliberate decision, not an accident.
	t.Setenv("COSMOGUARD_NODE_RPC_URL", "https://rpc.example.com")
	t.Setenv("COSMOGUARD_NODE_RPC_PORT", "26659")
	cfg := freshConfig(t)
	assert.Equal(t, cfg.Nodes[0].RpcURL, "https://rpc.example.com")
	assert.Equal(t, cfg.Nodes[0].RpcPort, 26659)
}

// TestApplyEnvOverrides_Dashboard exercises the new
// COSMOGUARD_DASHBOARD_* family. Enable is *bool so an explicit
// "false" must survive PrepareConfig — that's the same
// creasty/defaults footgun pinned by
// TestHealthcheckEnableDisable_RespectsExplicitFalse.
func TestApplyEnvOverrides_Dashboard(t *testing.T) {
	t.Setenv("COSMOGUARD_DASHBOARD_ENABLE", "false")
	t.Setenv("COSMOGUARD_DASHBOARD_PORT", "20001")
	t.Setenv("COSMOGUARD_DASHBOARD_AUTH_USER", "ops")
	t.Setenv("COSMOGUARD_DASHBOARD_AUTH_PASSWORD", "let-me-in")
	cfg := freshConfig(t)
	assert.Assert(t, cfg.Dashboard.Enable != nil, "Enable pointer must be set when env var is provided")
	assert.Equal(t, *cfg.Dashboard.Enable, false)
	assert.Equal(t, cfg.Dashboard.IsEnabled(), false)
	assert.Equal(t, cfg.Dashboard.Port, 20001)
	assert.Equal(t, cfg.Dashboard.BasicAuthUser, "ops")
	assert.Equal(t, cfg.Dashboard.BasicAuthPassword, "let-me-in")
}

// TestApplyEnvOverrides_DashboardEnableTrue covers the
// opt-in-to-enabled direction (operator who left Enable nil in YAML
// then turns it on via env). With *bool the env override must
// allocate a fresh pointer, not leave it nil.
func TestApplyEnvOverrides_DashboardEnableTrue(t *testing.T) {
	t.Setenv("COSMOGUARD_DASHBOARD_ENABLE", "true")
	cfg := freshConfig(t)
	assert.Assert(t, cfg.Dashboard.Enable != nil)
	assert.Equal(t, *cfg.Dashboard.Enable, true)
	assert.Equal(t, cfg.Dashboard.IsEnabled(), true)
}

// TestApplyEnvOverrides_DashboardBoolVocabulary mirrors
// TestApplyEnvOverrides_BoolVocabulary for the *bool helper. A
// regression in envBoolPtr would surface here even if envBool stays
// healthy — they share envBoolFromValue, but the wrapper itself
// (pointer allocation, nil-vs-value) needs its own coverage.
func TestApplyEnvOverrides_DashboardBoolVocabulary(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want bool
	}{
		{"true", true}, {"false", false},
		{"yes", true}, {"no", false},
		{"on", true}, {"off", false},
		{"y", true}, {"n", false},
		{"1", true}, {"0", false},
		{"YES", true}, {"NO", false},
		{" true ", true},
	} {
		t.Run(tc.v, func(t *testing.T) {
			t.Setenv("COSMOGUARD_DASHBOARD_ENABLE", tc.v)
			cfg := freshConfig(t)
			assert.Assert(t, cfg.Dashboard.Enable != nil)
			assert.Equal(t, *cfg.Dashboard.Enable, tc.want)
		})
	}
}

// TestApplyEnvOverrides_DashboardBadBool pins that a malformed
// COSMOGUARD_DASHBOARD_ENABLE fails startup instead of silently
// falling back to the YAML/default — same loud-failure semantics
// the other bool/port env vars enforce.
func TestApplyEnvOverrides_DashboardBadBool(t *testing.T) {
	t.Setenv("COSMOGUARD_DASHBOARD_ENABLE", "maybe")
	cfg := &Config{}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "COSMOGUARD_DASHBOARD_ENABLE"),
		"error should mention the env var: %v", err)
}

// TestApplyEnvOverrides_DashboardEmptyAsUnset confirms the
// empty-as-unset rule applies to the *bool helper. An empty
// templated Helm var (`--set dashboard.enable=`) must leave the
// existing config value (nil) intact, not flip to false.
func TestApplyEnvOverrides_DashboardEmptyAsUnset(t *testing.T) {
	t.Setenv("COSMOGUARD_DASHBOARD_ENABLE", "")
	cfg := freshConfig(t)
	assert.Assert(t, cfg.Dashboard.Enable == nil,
		"empty env var must NOT overwrite a nil *bool — got %v", cfg.Dashboard.Enable)
	assert.Equal(t, cfg.Dashboard.IsEnabled(), false,
		"nil Enable is secure-by-default OFF")
}

// TestApplyEnvOverrides_DashboardPortCollisionRejected wires together
// the env override and the validateListenerPorts guard: setting the
// dashboard port to one already taken by metrics yields a startup
// error with both field names called out.
func TestApplyEnvOverrides_DashboardPortCollisionRejected(t *testing.T) {
	t.Setenv("COSMOGUARD_METRICS_PORT", "29000")
	t.Setenv("COSMOGUARD_DASHBOARD_PORT", "29000")
	on := true
	cfg := &Config{Dashboard: DashboardConfig{Enable: &on}}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "port collision"),
		"error should mention port collision: %v", err)
}

// TestDashboardIsEnabled_DefaultsOff pins the secure-by-default
// posture: the dashboard exposes operator state with no built-in
// auth on a host-bound port, so it must be off unless the operator
// explicitly opts in.
func TestDashboardIsEnabled_DefaultsOff(t *testing.T) {
	on := true
	cfg := &Config{Dashboard: DashboardConfig{Enable: &on}}
	assert.NilError(t, PrepareConfig(cfg))
	assert.Assert(t, cfg.Dashboard.IsEnabled(),
		"explicit enable:true must remain true after PrepareConfig")

	cfg2 := &Config{}
	assert.NilError(t, PrepareConfig(cfg2))
	assert.Assert(t, !cfg2.Dashboard.IsEnabled(),
		"nil Enable must default OFF (secure-by-default)")
}

// TestValidateListenerPorts_ZeroPortRejected pins the defense-in-
// depth guard: validateListenerPorts must reject port==0 for any
// enabled listener. In normal config flow `defaults.Set` fills the
// struct-tag default (19999) so port:0 isn't reachable, and
// envPort rejects 0 at the env-override layer too. This test
// invokes the validator directly to pin the guard against future
// refactors that might remove the default tag or change the env
// layer.
func TestValidateListenerPorts_ZeroPortRejected(t *testing.T) {
	on := true
	metricsOff := false
	cfg := &Config{
		RpcPort: 1, LcdPort: 2, GrpcPort: 3,
		// Metrics defaults to enabled (nil *bool); disable it here so this
		// bare Config (no PrepareConfig, so metrics.port stays 0) isolates
		// the dashboard.port check under test.
		Metrics:   MetricsConfig{Enable: &metricsOff},
		Dashboard: DashboardConfig{Enable: &on, Port: 0},
	}
	err := validateListenerPorts(cfg)
	assert.Assert(t, err != nil, "dashboard.port:0 with enable:true must fail")
	assert.Assert(t, strings.Contains(err.Error(), "dashboard.port"),
		"error should name the offending field: %v", err)
	assert.Assert(t, strings.Contains(err.Error(), "out of range"),
		"error should be the out-of-range message: %v", err)

	// port==0 on a *disabled* dashboard is fine — the listener
	// is never bound, so the value is irrelevant.
	off := false
	cfg2 := &Config{
		RpcPort: 1, LcdPort: 2, GrpcPort: 3,
		Metrics:   MetricsConfig{Enable: &metricsOff},
		Dashboard: DashboardConfig{Enable: &off, Port: 0},
	}
	assert.NilError(t, validateListenerPorts(cfg2),
		"port:0 on a disabled dashboard should pass — the listener is never bound")
}

// TestValidateListenerPorts_OutOfRangeRejected pins the symmetric
// upper bound (>65535) so a typo in a manually-built Config can't
// slip past the validator and surface as a confusing net.Listen
// error.
func TestValidateListenerPorts_OutOfRangeRejected(t *testing.T) {
	on := true
	metricsOff := false
	cfg := &Config{
		RpcPort: 1, LcdPort: 2, GrpcPort: 3,
		Metrics:   MetricsConfig{Enable: &metricsOff},
		Dashboard: DashboardConfig{Enable: &on, Port: 99999},
	}
	err := validateListenerPorts(cfg)
	assert.Assert(t, err != nil)
	assert.Assert(t, strings.Contains(err.Error(), "out of range"),
		"error should be the out-of-range message: %v", err)
}

// TestValidateDashboardAuth_HalfConfiguredRejected pins the fix for
// the empty-password bypass: when an operator sets BasicAuthUser
// but forgets BasicAuthPassword (or vice versa), startup must fail
// loudly rather than silently accepting "" as the password and
// letting anyone who knows the username log in. Covers both the
// standalone DashboardConfig and the legacy WebUIConfig on the
// metrics port — neither entry point may slip past.
func TestValidateDashboardAuth_HalfConfiguredRejected(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cfg         *Config
		wantContain string
	}{
		{
			name: "dashboard user without password",
			cfg: &Config{
				Dashboard: DashboardConfig{BasicAuthUser: "ops", BasicAuthPassword: ""},
			},
			wantContain: "dashboard.basicAuthUser set without basicAuthPassword",
		},
		{
			name: "dashboard password without user",
			cfg: &Config{
				Dashboard: DashboardConfig{BasicAuthUser: "", BasicAuthPassword: "secret"},
			},
			wantContain: "dashboard.basicAuthPassword set without basicAuthUser",
		},
		{
			name: "webUI user without password",
			cfg: &Config{
				Metrics: MetricsConfig{
					WebUI: WebUIConfig{BasicAuthUser: "ops", BasicAuthPassword: ""},
				},
			},
			wantContain: "metrics.webUI.basicAuthUser set without basicAuthPassword",
		},
		{
			name: "webUI password without user",
			cfg: &Config{
				Metrics: MetricsConfig{
					WebUI: WebUIConfig{BasicAuthUser: "", BasicAuthPassword: "secret"},
				},
			},
			wantContain: "metrics.webUI.basicAuthPassword set without basicAuthUser",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDashboardAuth(tc.cfg)
			assert.Assert(t, err != nil, "expected error for half-configured auth")
			assert.Assert(t, strings.Contains(err.Error(), tc.wantContain),
				"want %q, got: %v", tc.wantContain, err)
		})
	}
}

// TestValidateDashboardAuth_BothEmptyAllowed pins the intentional
// "open dashboard" shape: leaving both fields empty is documented
// as the "behind a trusted network / authenticating ingress" mode
// and must NOT regress into a startup error.
func TestValidateDashboardAuth_BothEmptyAllowed(t *testing.T) {
	cfg := &Config{
		Dashboard: DashboardConfig{BasicAuthUser: "", BasicAuthPassword: ""},
		Metrics:   MetricsConfig{WebUI: WebUIConfig{BasicAuthUser: "", BasicAuthPassword: ""}},
	}
	assert.NilError(t, validateDashboardAuth(cfg),
		"both empty must remain allowed — that's the documented open-dashboard shape")
}

// TestValidateDashboardAuth_BothSetAllowed pins the happy path —
// the validator must not interfere when both fields are populated.
func TestValidateDashboardAuth_BothSetAllowed(t *testing.T) {
	cfg := &Config{
		Dashboard: DashboardConfig{BasicAuthUser: "ops", BasicAuthPassword: "secret"},
		Metrics:   MetricsConfig{WebUI: WebUIConfig{BasicAuthUser: "ops", BasicAuthPassword: "secret"}},
	}
	assert.NilError(t, validateDashboardAuth(cfg))
}

// TestPrepareConfig_DashboardAuthHalfConfiguredRejected pins that
// the validator is actually wired into PrepareConfig — a unit test
// for validateDashboardAuth alone could silently pass while the
// validator was dropped from the funnel. Reaching the failure via
// PrepareConfig is the integration-level pin.
func TestPrepareConfig_DashboardAuthHalfConfiguredRejected(t *testing.T) {
	off := false
	cfg := &Config{
		Host:     "127.0.0.1",
		RpcPort:  17801,
		LcdPort:  17802,
		GrpcPort: 17803,
		// Disable the dashboard so we don't have to allocate a
		// non-colliding port — the validator runs regardless of
		// whether the dashboard is enabled, since the misconfig
		// shape is dangerous even on the legacy /admin mount.
		Dashboard: DashboardConfig{Enable: &off, BasicAuthUser: "ops"},
		LCD:       LcdConfig{Default: RuleActionAllow},
		RPC: RpcConfig{
			Default: RuleActionAllow,
			JsonRpc: JsonRpcConfig{Default: RuleActionAllow},
		},
		GRPC: GrpcConfig{Default: RuleActionAllow},
		Nodes: []NodeConfig{
			{Host: "127.0.0.1", LcdPort: 1317, RpcPort: 26657, GrpcPort: 9090},
		},
	}
	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "PrepareConfig must reject half-configured dashboard auth")
	assert.Assert(t, strings.Contains(err.Error(), "basicAuthUser set without basicAuthPassword"),
		"error should name the misconfig: %v", err)
}
