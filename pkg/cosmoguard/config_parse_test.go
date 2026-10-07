package cosmoguard

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Keep config/matcher allocation graphs out of the existing process-wide heap
// growth tests. The child runs the same assertions in the compiled test binary.
func restartTestProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("TEST_COSMOGUARD_RESTART_CHILD") == "1" {
		return false
	}
	timeout := 2 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		t.Fatal("isolated test deadline exceeded")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout="+timeout.String())
	cmd.Env = append(os.Environ(), "TEST_COSMOGUARD_RESTART_CHILD=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated test: %v\n%s", err, output)
	}
	return true
}

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) { value, ok := values[name]; return value, ok }
}

func TestRestartProcessDeadline(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	if _, ok := t.Deadline(); !ok {
		t.Fatal("isolated test has no deadline")
	}
}

func TestParseConfigExplicitEnvironment(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	t.Setenv("COSMOGUARD_NODE_HOST", "process-host")
	t.Setenv("COSMOGUARD_METRICS_ENABLE", "invalid-process-value")
	t.Setenv("CONFIG_NODE", "process-interpolation")
	beforeEnv := os.Environ()
	previousTrust := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(previousTrust) })
	if err := SetTrustedProxies([]string{"10.0.0.0/8"}); err != nil {
		t.Fatal(err)
	}
	raw := []byte("node: {host: '${CONFIG_NODE:-yaml-host}'}\nserver: {trustedProxies: [192.168.0.0/16]}\n")
	cfg, err := ParseConfig(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Nodes[0].Host != "yaml-host" || !cfg.Metrics.IsEnabled() {
		t.Fatalf("nil lookup used process environment: %+v", cfg.Nodes[0])
	}
	cfg, err = ParseConfig(raw, envLookup(map[string]string{
		"CONFIG_NODE": "interpolated-host", "COSMOGUARD_NODE_HOST": " override-host ",
		"COSMOGUARD_METRICS_ENABLE": "false", "COSMOGUARD_METRICS_PORT": "9002",
		"COSMOGUARD_DASHBOARD_AUTH_USER": "user", "COSMOGUARD_DASHBOARD_AUTH_PASSWORD": "secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Nodes[0].Host != "override-host" || cfg.Metrics.IsEnabled() || cfg.Metrics.Port != 9002 || cfg.Dashboard.BasicAuthUser != "user" || cfg.Dashboard.BasicAuthPassword != "secret" {
		t.Fatal("explicit overrides not applied after interpolation/defaults")
	}
	direct := parseRestartConfig(t, "nodes: [{host: override-host}]\nmetrics: {enable: false, port: 9002}\ndashboard: {basicAuthUser: user, basicAuthPassword: secret}\n")
	if required, reason := RequiresRestart(direct, cfg); required {
		t.Fatalf("overrides differ from direct YAML: %s", reason)
	}
	if fingerprint(t, direct) != fingerprint(t, cfg) {
		t.Fatal("effective overrides must have the same fingerprint as YAML")
	}
	_, _ = RequiresRestart(direct, cfg)
	_ = fingerprint(t, cfg)
	req := httptest.NewRequest("GET", "http://guard/", nil)
	req.RemoteAddr = "10.1.2.3:1234"
	req.Header.Set("X-Real-Ip", "203.0.113.1")
	if GetSourceIP(req) != "203.0.113.1" {
		t.Fatal("ParseConfig replaced the trusted-proxy allowlist")
	}
	req.RemoteAddr = "192.168.1.1:1234"
	if GetSourceIP(req) != "192.168.1.1" {
		t.Fatal("ParseConfig published its own trusted-proxy allowlist")
	}
	if !reflect.DeepEqual(beforeEnv, os.Environ()) {
		t.Fatal("APIs changed process environment")
	}
	if string(raw) != "node: {host: '${CONFIG_NODE:-yaml-host}'}\nserver: {trustedProxies: [192.168.0.0/16]}\n" {
		t.Fatal("ParseConfig mutated input")
	}
}

func TestParseConfigValidation(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	cases := []struct{ name, raw, key, value, message string }{
		{"malformed", "nodes: [", "", "", "unmarshal"},
		{"unknown", "unknown: true", "", "", "field unknown"},
		{"trailing", "{}\n---\n{}", "", "", "only one YAML document"},
		{"removed", "cache: {backend: redis}", "", "", "removed in v4"},
		{"both node forms", "node: {rpcPort: 26658}\nnodes: [{}]", "", "", "not both"},
		{"http glob", "lcd: {rules: [{paths: ['[bad'], action: allow}]}", "", "", "lcd:"},
		{"json glob", "rpc: {jsonrpc: {rules: [{methods: ['[bad'], action: allow}]}}", "", "", "rpc.jsonrpc:"},
		{"grpc glob", "grpc: {rules: [{methods: ['[bad'], action: allow}]}", "", "", "grpc:"},
		{"rule action", "lcd: {rules: [{action: invalid}]}", "", "", "is invalid"},
		{"cors glob", "cors: {enable: true, allowedOrigins: ['[bad']}", "", "", "cors:"},
		{"trusted proxy", "server: {trustedProxies: [bad-cidr]}", "", "", "trustedProxies:"},
		{"missing interpolation", "node: {host: '${ABSENT:?provide host}'}", "", "", "provide host"},
		{"integer override", "", "COSMOGUARD_RPC_PORT", "bad", "COSMOGUARD_RPC_PORT:"},
		{"port override", "", "COSMOGUARD_GRPC_PORT", "65536", "out of range"},
		{"boolean override", "", "COSMOGUARD_ENABLE_EVM", "bad", "invalid boolean"},
		{"duration override", "", "COSMOGUARD_DISCOVERY_REFRESH_INTERVAL", "0s", "must be > 0"},
		{"url override", "", "COSMOGUARD_NODE_RPC_URL", "ftp://host", "not supported"},
		{"cluster override", "", "COSMOGUARD_CLUSTER_ENABLE", "true", "bindAddr"},
	}
	previousTrust := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(previousTrust) })
	if err := SetTrustedProxies([]string{"10.0.0.0/8"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(tc.raw), envLookup(map[string]string{tc.key: tc.value}))
			if cfg != nil || err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("ParseConfig = %v, %v; want %q", cfg, err, tc.message)
			}
			if !remotePeerTrusted("10.1.2.3:1234") {
				t.Fatal("failed parsing changed trust list")
			}
		})
	}
}

func TestParseConfigNormalizesRules(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	raw := `lcd:
  rules:
    - priority: 20
      action: allow
      paths: [/v1/*]
      methods: [GET]
    - priority: 10
      action: deny
      paths: [/private]
`
	cfg := parseRestartConfig(t, raw)
	if cfg.RPC.JsonRpc.MaxBatchSize == nil || *cfg.RPC.JsonRpc.MaxBatchSize != 100 || cfg.Nodes[0].Name != "node-0" {
		t.Fatal("special defaults or upstream naming missing")
	}
	if cfg.LCD.Rules[0].Priority != 10 {
		t.Fatal("rules not sorted")
	}
	req := httptest.NewRequest("GET", "http://guard/v1/test", nil)
	if !cfg.LCD.Rules[1].Matches(req) {
		t.Fatal("v3 matcher not compiled")
	}
	modern := parseRestartConfig(t, "lcd: {rules: [{priority: 20, action: allow, match: {paths: [/v1/*], methods: [GET]}}]}")
	if !modern.LCD.Rules[0].Matches(req) {
		t.Fatal("v4 matcher not compiled")
	}
	if required, reason := RequiresRestart(cfg, modern); required {
		t.Fatalf("equivalent rule forms affect restart: %s", reason)
	}
}

func TestLegacyLoadersPublishTrustedProxies(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	previousTrust := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(previousTrust) })
	t.Setenv("CONFIG_NODE", "legacy-host")
	t.Setenv("COSMOGUARD_METRICS_ENABLE", "false")
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := "node: {host: '${CONFIG_NODE}'}\nserver: {trustedProxies: [10.0.0.0/8]}\n"
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfigFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Nodes[0].Host != "legacy-host" || cfg.Metrics.IsEnabled() || !remotePeerTrusted("10.1.2.3:1234") {
		t.Fatal("file loader lost environment or trust publication")
	}
	cfg = &Config{Server: ServerConfig{TrustedProxies: []string{"192.168.0.0/16"}}}
	if err := PrepareConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Metrics.IsEnabled() || !remotePeerTrusted("192.168.1.1:1234") || remotePeerTrusted("10.1.2.3:1234") {
		t.Fatal("programmatic loader lost environment or trust publication")
	}
	if err := os.WriteFile(path, []byte("node: {host: '${MISSING_CONFIG_VALUE}'}"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MISSING_CONFIG_VALUE", "")
	if _, err := ReadConfigFromFile(path); err == nil || !strings.Contains(err.Error(), "error interpolating env vars in "+path) {
		t.Fatalf("filename context lost: %v", err)
	}
}

func TestRestartFingerprintAcrossProcesses(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	raw := "auth: {identities: [{name: a, validUntil: 2030-01-01T00:00:00Z}], anonymous: {name: guest, validUntil: '2030-01-01T00:00:00+00:00'}}\nrpc: {jsonrpc: {rules: [{methods: [block], params: {height: 10, prove: true}, action: allow}]}}"
	if path := os.Getenv("COSMOGUARD_TEST_FINGERPRINT_OUTPUT"); path != "" {
		if err := os.WriteFile(path, []byte(fingerprint(t, parseRestartConfig(t, raw))), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, timezone := range []string{"UTC", "Europe/Lisbon"} {
		path := filepath.Join(t.TempDir(), "fingerprint")
		cmd := exec.Command(os.Args[0], "-test.run=^TestRestartFingerprintAcrossProcesses$")
		cmd.Env = append(os.Environ(), "COSMOGUARD_TEST_FINGERPRINT_OUTPUT="+path, "TZ="+timezone)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child process: %v\n%s", err, output)
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != fingerprint(t, parseRestartConfig(t, raw)) {
			t.Fatalf("fingerprint differs in another process with TZ=%s", timezone)
		}
	}
}
