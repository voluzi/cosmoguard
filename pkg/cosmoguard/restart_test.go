package cosmoguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type restartCase struct{ name, previous, next, reason string }

func restartCases() []restartCase {
	const cache = "cache topology change requires a process restart: one or more cache.* fields changed (cluster topology, global ttl, key salt)"
	const auth = "auth config change requires a process restart"
	const nodes = "nodes (upstream topology) change requires a process restart"
	const cors = "cors config change requires a process restart"
	const server = "server config change (timeouts / maxRequestBody / wsReadLimit / websocketLimits / wsAllowedOrigins) requires a process restart"
	const dashboard = "dashboard config change (enable / port / basicAuth / clusterHistoryRestore) requires a process restart"
	const metrics = "metrics config change (enable / port / webUI) requires a process restart"
	const websocket = "websocket config change (webSocketEnabled / webSocketConnections) requires a process restart"
	const grpc = "grpc message size change (maxRecvMsgSize / maxSendMsgSize) requires a process restart"
	cluster := "cache:\n  cluster:\n    bindAddr: 10.0.0.1\n    encryptionKey: " + testClusterKey + "\n    discovery: {mode: static, static: {peers: [10.0.0.2]}}\n"
	return []restartCase{
		{"cache ttl", "", "cache: {ttl: 6s}", cache},
		{"cache key", "", "cache: {key: salt}", cache},
		{"cache coalesce pointer", "", "cache: {coalesce: true}", cache},
		{"cache stale window", "", "cache: {staleWhileRevalidate: 1s}", cache},
		{"cache http timeout", "", "cache: {httpForegroundFetchTimeout: 6m}", cache},
		{"cache grpc timeout", "", "cache: {grpcForegroundFetchTimeout: 6m}", cache},
		{"cache memory pointer", "", "cache: {memory: {maxItems: 0}}", cache},
		{"cache memory bytes", "", "cache: {memory: {maxBytes: 1}}", cache},
		{"cache distributed bytes", "", "cache: {memory: {distributedMaxBytesPerNode: 1}}", cache},
		{"cache reserve", "", "cache: {memory: {reserveFraction: 0.1}}", cache},
		{"cluster added", "", cluster, "cache topology change requires a process restart: cache.cluster block was added (embedded ↔ networked toggle requires restart)"},
		{"cluster removed", cluster, "", "cache topology change requires a process restart: cache.cluster block was removed (embedded ↔ networked toggle requires restart)"},
		{"cluster topology", cluster, strings.Replace(cluster, "10.0.0.1", "10.0.0.3", 1), cache},
		{"cluster slice order", strings.Replace(cluster, "[10.0.0.2]", "[10.0.0.2, 10.0.0.3]", 1), strings.Replace(cluster, "[10.0.0.2]", "[10.0.0.3, 10.0.0.2]", 1), cache},
		{"enable evm", "", "enableEvm: true", "enableEvm change requires a process restart (running=false, new=true)"},
		{"disable evm", "enableEvm: true", "", "enableEvm change requires a process restart (running=true, new=false)"},
		{"auth enable", "", "auth: {enable: true}", auth},
		{"auth default", "", "auth: {defaultRequire: true}", auth},
		{"auth methods", "", "auth: {methods: [{type: api-key, header: x-key}]}", auth},
		{"auth identities", "", "auth: {identities: [{name: a, apiKey: secret}]}", auth},
		{"auth anonymous", "", "auth: {anonymous: {name: guest}}", auth},
		{"auth replay", "", "auth: {replayProtection: {}}", auth},
		{"auth file declaration", "", "auth: {identitiesFile: /does-not-exist}", auth},
		{"auth nil empty", "", "auth: {methods: []}", auth},
		{"auth scope order", "auth: {identities: [{name: a, scopes: [a, b]}]}", "auth: {identities: [{name: a, scopes: [b, a]}]}", auth},
		{"auth validity", "auth: {identities: [{name: a, validUntil: 2030-01-01T00:00:00Z}]}", "auth: {identities: [{name: a, validUntil: 2031-01-01T00:00:00Z}]}", auth},
		{"auth validity offset", "auth: {identities: [{name: a, validUntil: 2030-01-01T00:00:00Z}]}", "auth: {identities: [{name: a, validUntil: '2030-01-01T01:00:00+01:00'}]}", auth},
		{"auth validity zero offset", "auth: {anonymous: {name: a, validUntil: 2030-01-01T00:00:00Z}}", "auth: {anonymous: {name: a, validUntil: '2030-01-01T00:00:00+00:00'}}", auth},
		{"nodes host", "", "nodes: [{host: other}]", nodes},
		{"nodes order", "nodes: [{host: a}, {host: b}]", "nodes: [{host: b}, {host: a}]", nodes},
		{"nodes discovery", "", "nodes: [{discovery: {host: never-resolve.invalid}}]", nodes},
		{"nodes pointer", "", "nodes: [{healthcheck: {}}]", nodes},
		{"cors enabled", "", "cors: {enable: true}", cors},
		{"cors credentials", "", "cors: {credentials: true}", cors},
		{"cors max age", "", "cors: {maxAge: 10s}", cors},
		{"cors origins", "", "cors: {allowedOrigins: [https://example.com]}", cors},
		{"cors methods", "", "cors: {allowedMethods: [GET]}", cors},
		{"cors headers", "", "cors: {allowedHeaders: [X-Key]}", cors},
		{"cors exposed", "", "cors: {exposeHeaders: [X-Key]}", cors},
		{"cors slice order", "cors: {allowedOrigins: [https://a, https://b]}", "cors: {allowedOrigins: [https://b, https://a]}", cors},
		{"read header timeout", "", "server: {readHeaderTimeout: 11s}", server},
		{"read timeout", "", "server: {readTimeout: 31s}", server},
		{"write timeout", "", "server: {writeTimeout: 1s}", server},
		{"idle timeout", "", "server: {idleTimeout: 61s}", server},
		{"body limit", "", "server: {maxRequestBody: 0}", server},
		{"ws read limit", "", "server: {wsReadLimit: 0}", server},
		{"ws client limit", "", "server: {websocketLimits: {maxSubscriptionsPerClient: 0}}", server},
		{"ws identity limit", "", "server: {websocketLimits: {maxSubscriptionsPerIdentity: 0}}", server},
		{"ws upstream limit", "", "server: {websocketLimits: {maxSubscriptionsPerUpstreamConnection: 0}}", server},
		{"ws ip limit", "", "server: {websocketLimits: {maxConnectionsPerIP: 0}}", server},
		{"ws origins", "", "server: {wsAllowedOrigins: ['*']}", server},
		{"ws origins order", "server: {wsAllowedOrigins: [https://a, https://b]}", "server: {wsAllowedOrigins: [https://b, https://a]}", server},
		{"dashboard enable", "", "dashboard: {enable: true}", dashboard},
		{"dashboard port", "", "dashboard: {port: 20000}", dashboard},
		{"dashboard auth", "", "dashboard: {basicAuthUser: user, basicAuthPassword: secret}", dashboard},
		{"dashboard restore", "", "dashboard: {clusterHistoryRestore: true}", dashboard},
		{"metrics enable", "", "metrics: {enable: false}", metrics},
		{"metrics port", "", "metrics: {port: 9002}", metrics},
		{"metrics webui", "", "metrics: {webUI: {enable: true}}", metrics},
		{"metrics auth", "", "metrics: {webUI: {basicAuthUser: user, basicAuthPassword: secret}}", metrics},
		{"rpc websocket enable", "", "rpc: {webSocketEnabled: false}", websocket},
		{"rpc websocket count", "", "rpc: {webSocketConnections: 41}", websocket},
		{"evm websocket count", "", "evm: {ws: {webSocketConnections: 41}}", websocket},
		{"grpc receive", "", "grpc: {maxRecvMsgSize: 1000}", grpc},
		{"grpc send", "", "grpc: {maxSendMsgSize: 1000}", grpc},
		{"cache precedence", "", "cache: {ttl: 6s}\nenableEvm: true\nauth: {enable: true}", cache},
		{"evm precedence", "", "enableEvm: true\nauth: {enable: true}", "enableEvm change requires a process restart (running=false, new=true)"},
		{"auth precedence", "", "auth: {enable: true}\nnodes: [{host: other}]", auth},
		{"nodes precedence", "", "nodes: [{host: other}]\ncors: {enable: true}", nodes},
		{"cors precedence", "", "cors: {enable: true}\nserver: {maxRequestBody: 0}", cors},
		{"server precedence", "", "server: {maxRequestBody: 0}\ndashboard: {enable: true}", server},
		{"dashboard precedence", "", "dashboard: {enable: true}\nmetrics: {enable: false}", dashboard},
		{"metrics precedence", "", "metrics: {enable: false}\nrpc: {webSocketEnabled: false}", metrics},
		{"websocket precedence", "", "rpc: {webSocketEnabled: false}\ngrpc: {maxRecvMsgSize: 1000}", websocket},
		{"unchanged", "", "", ""},
		{"effective server defaults", "", "server: {maxRequestBody: 5242880, wsReadLimit: 1048576, websocketLimits: {maxSubscriptionsPerClient: 32, maxSubscriptionsPerIdentity: 128, maxSubscriptionsPerUpstreamConnection: 10, maxConnectionsPerIP: 16}}", ""},
		{"metrics effective enable", "", "metrics: {enable: true}", ""},
		{"rpc effective enable", "", "rpc: {webSocketEnabled: true}", ""},
		{"dashboard nil false", "", "dashboard: {enable: false, clusterHistoryRestore: false}", ""},
		{"empty origin slices", "", "cors: {allowedOrigins: [], allowedMethods: [], allowedHeaders: [], exposeHeaders: []}\nserver: {wsAllowedOrigins: []}", ""},
		{"enabled cors unchanged", "cors: {enable: true, allowedOrigins: [https://example.com]}", "cors: {enable: true, allowedOrigins: [https://example.com]}", ""},
		{"v3 v4 equivalent", "node: {host: upstream}", "nodes: [{host: upstream}]", ""},
		{"trusted proxies", "", "server: {trustedProxies: [10.0.0.0/8]}", ""},
		{"request logging", "", "dashboard: {requestLog: {enable: true, maxEntries: 10}}", ""},
		{"section defaults", "", "rpc: {default: allow, jsonrpc: {default: allow}}\ngrpc: {default: allow}\nevm: {rpc: {default: allow}, ws: {default: allow}}", ""},
		{"lcd rules", "lcd: {rules: [{paths: [/old], action: allow}]}", "lcd: {rules: [{paths: [/new], action: deny}]}", ""},
		{"rpc http rules", "", "rpc: {rules: [{paths: [/new], action: deny}]}", ""},
		{"rpc json rules", "", "rpc: {jsonrpc: {rules: [{methods: [block], params: {height: 10, prove: true}, action: deny}]}}", ""},
		{"grpc rules", "", "grpc: {rules: [{methods: [cosmos.bank.v1beta1.Query/Balance], action: deny}]}", ""},
		{"evm rules", "enableEvm: true", "enableEvm: true\nevm: {rpc: {rules: [{methods: [eth_chainId], action: deny}], httpRules: [{paths: [/new], action: deny}]}, ws: {rules: [{methods: [eth_subscribe], action: deny}]}}", ""},
		{"untracked listener", "", "rpcPort: 16658", ""},
	}
}

func parseRestartConfig(t *testing.T, raw string) *Config {
	t.Helper()
	cfg, err := ParseConfig([]byte(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func fingerprint(t *testing.T, cfg *Config) string {
	t.Helper()
	value, err := RestartFingerprint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(value, "v1:") || len(value) != 67 {
		t.Fatalf("invalid fingerprint %q", value)
	}
	return value
}

func TestRestartFingerprintGolden(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	// On failure, review encoding/default changes for compatibility. Preserve the
	// digest unless a deliberate encoding-version or module-version change
	// warrants updating this golden.
	const want = "v1:59ff0238c15b8e91f18976662d94fd00100a07c11da6a7750ec879128a618904"
	if got := fingerprint(t, parseRestartConfig(t, "{}\n")); got != want {
		t.Fatalf("minimal config fingerprint = %q; want %q", got, want)
	}
}

func TestRestartFingerprintPolicy(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	for _, tc := range restartCases() {
		t.Run(tc.name, func(t *testing.T) {
			previous, next := parseRestartConfig(t, tc.previous), parseRestartConfig(t, tc.next)
			required, reason := RequiresRestart(previous, next)
			if required != (tc.reason != "") || reason != tc.reason {
				t.Fatalf("restart = %v, %q; want %q", required, reason, tc.reason)
			}
			if changed := fingerprint(t, previous) != fingerprint(t, next); changed != required {
				t.Fatalf("fingerprint changed = %v; restart required = %v", changed, required)
			}
			reversed, _ := RequiresRestart(next, previous)
			if reversed != required {
				t.Fatal("restart decision must be symmetric")
			}
			if again := fingerprint(t, parseRestartConfig(t, tc.next)); again != fingerprint(t, next) {
				t.Fatal("repeated parsing must be deterministic")
			}
		})
	}
}

func TestTryReloadMatchesRestartPolicy(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "COSMOGUARD_") {
			t.Setenv(name, "")
		}
	}
	prevTrusted := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(prevTrusted) })
	for _, tc := range restartCases() {
		t.Run(tc.name, func(t *testing.T) {
			previousRaw, nextRaw := tc.previous, tc.next
			if tc.name != "lcd rules" {
				previousRaw += "\nlcd: {rules: [{paths: [/old], action: allow}]}\n"
				nextRaw += "\nlcd: {rules: [{paths: [/new], action: deny}]}\n"
			}
			previous, next := parseRestartConfig(t, previousRaw), parseRestartConfig(t, nextRaw)
			required, reason := RequiresRestart(previous, next)
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(nextRaw), 0600); err != nil {
				t.Fatal(err)
			}
			cg := &CosmoGuard{cfg: previous, cfgFile: path, origNodes: previous.Nodes, dashboard: newDashboardObservability(),
				lcdProxy: &HttpProxy{}, rpcProxy: &HttpProxy{}, grpcProxy: &GrpcProxy{}, jsonRpcHandler: &JsonRpcHandler{},
				evmRpcProxy: &HttpProxy{}, evmJsonRpcHandler: &JsonRpcHandler{}, evmJsonRpcWsHandler: &JsonRpcHandler{},
			}
			cg.applyRulesLocked()
			cg.tryReload()
			status := cg.dashboard.lastReload
			if status == nil || status.Success == required || status.Error != reason {
				t.Fatalf("reload = %+v; restart = %v, %q", status, required, reason)
			}
			want := "/new"
			if required {
				want = "/old"
			}
			if got := cg.cfg.LCD.Rules[0].Paths[0]; got != want {
				t.Fatalf("active rule path = %q; want %q", got, want)
			}
			if got := cg.lcdProxy.rules[0].Paths[0]; got != want {
				t.Fatalf("applied rule path = %q; want %q", got, want)
			}
		})
	}
}

func TestTryReloadIgnoresDiscoveryExpansion(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	prevTrusted := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(prevTrusted) })
	declaration := "nodes: [{discovery: {host: never-resolve.invalid}}]\n"
	cfg := parseRestartConfig(t, declaration)
	originalNodes := cfg.Nodes
	cfg.Nodes = []NodeConfig{{Host: "10.0.0.1"}, {Host: "10.0.0.2"}}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(declaration+"lcd: {rules: [{paths: [/new], action: deny}]}"), 0600); err != nil {
		t.Fatal(err)
	}
	cg := &CosmoGuard{cfg: cfg, cfgFile: path, origNodes: originalNodes, dashboard: newDashboardObservability(),
		lcdProxy: &HttpProxy{}, rpcProxy: &HttpProxy{}, grpcProxy: &GrpcProxy{}, jsonRpcHandler: &JsonRpcHandler{}}
	cg.tryReload()
	if status := cg.dashboard.lastReload; status == nil || !status.Success {
		t.Fatalf("declarative topology unchanged: %+v", status)
	}
	if cg.cfg.LCD.Rules[0].Paths[0] != "/new" {
		t.Fatal("rule change not applied")
	}
}

func TestRestartAPIsRejectNil(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	if value, err := RestartFingerprint(nil); err == nil || value != "" {
		t.Fatalf("nil config = %q, %v", value, err)
	}
	cfg := parseRestartConfig(t, "{}\n")
	for _, tc := range []struct {
		name           string
		previous, next *Config
	}{
		{"previous nil", nil, cfg},
		{"next nil", cfg, nil},
		{"both nil", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if required, reason := RequiresRestart(tc.previous, tc.next); !required || reason == "" {
				t.Fatalf("nil config comparison = %v, %q; want rejection with a reason", required, reason)
			}
		})
	}
}

func TestRestartFingerprintPreservesEnvironmentBytes(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	configs := make([]*Config, 0, 3)
	for _, host := range []string{"\xff", "\xfe", "\ufffd"} {
		cfg, err := ParseConfig(nil, envLookup(map[string]string{"COSMOGUARD_NODE_HOST": host}))
		if err != nil {
			t.Fatal(err)
		}
		configs = append(configs, cfg)
	}
	for i := 0; i < len(configs); i++ {
		for j := i + 1; j < len(configs); j++ {
			required, reason := RequiresRestart(configs[i], configs[j])
			if !required || reason != "nodes (upstream topology) change requires a process restart" {
				t.Fatalf("different host bytes require restart: %v, %q", required, reason)
			}
			if fingerprint(t, configs[i]) == fingerprint(t, configs[j]) {
				t.Fatalf("different environment bytes collided: %q and %q", configs[i].Nodes[0].Host, configs[j].Nodes[0].Host)
			}
		}
	}
}
