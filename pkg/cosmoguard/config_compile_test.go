package cosmoguard

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

// reloadTestPorts returns five ephemeral ports for use in a test YAML so
// concurrent or sequential tests don't collide on the default ports.
func reloadTestPorts(t *testing.T) (lcd, rpc, grpc, evmRpc, evmRpcWs int) {
	t.Helper()
	for _, p := range []*int{&lcd, &rpc, &grpc, &evmRpc, &evmRpcWs} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NilError(t, err)
		*p = l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
	}
	return
}

// TestPrepareConfig_RejectsInvalidHttpGlob proves PrepareConfig returns an
// error (does NOT panic) when a path is a malformed glob. Pre-B2 this would
// have panicked via glob.MustCompile and taken the process down at reload.
func TestPrepareConfig_RejectsInvalidHttpGlob(t *testing.T) {
	cfg := &Config{
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{{
				Priority: 100,
				Action:   RuleActionAllow,
				Paths:    []string{"[unclosed-bracket"}, // invalid glob
			}},
		},
	}

	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "expected error for malformed path glob")
	assert.Assert(t, strings.Contains(err.Error(), "[unclosed-bracket"),
		"error should mention the offending value: %v", err)
}

func TestPrepareConfig_RejectsInvalidJsonRpcGlob(t *testing.T) {
	cfg := &Config{
		RPC: RpcConfig{
			Default: RuleActionAllow,
			JsonRpc: JsonRpcConfig{
				Default: RuleActionAllow,
				Rules: []*JsonRpcRule{{
					Priority: 100,
					Action:   RuleActionAllow,
					Methods:  []string{"[bad-method-glob"},
				}},
			},
		},
	}

	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "expected error for malformed method glob")
}

func TestPrepareConfig_RejectsInvalidGrpcGlob(t *testing.T) {
	cfg := &Config{
		GRPC: GrpcConfig{
			Default: RuleActionAllow,
			Rules: []*GrpcRule{{
				Priority: 100,
				Action:   RuleActionAllow,
				Methods:  []string{"[bad-grpc-glob"},
			}},
		},
	}

	err := PrepareConfig(cfg)
	assert.Assert(t, err != nil, "expected error for malformed grpc glob")
}

// TestPrepareConfig_Idempotent re-runs PrepareConfig on the same config and
// verifies it still succeeds and produces the same number of compiled globs.
func TestPrepareConfig_Idempotent(t *testing.T) {
	cfg := &Config{
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{{
				Priority: 100,
				Action:   RuleActionAllow,
				Paths:    []string{"/a", "/b", "/c"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"height": "*"},
			}},
		},
	}

	assert.NilError(t, PrepareConfig(cfg))
	assert.Equal(t, len(cfg.LCD.Rules[0].PathGlobs), 3)
	assert.Equal(t, len(cfg.LCD.Rules[0].QueryGlobs), 1)

	assert.NilError(t, PrepareConfig(cfg))
	assert.Equal(t, len(cfg.LCD.Rules[0].PathGlobs), 3)
	assert.Equal(t, len(cfg.LCD.Rules[0].QueryGlobs), 1)
}

// portYAMLHeader returns the YAML preamble that binds cosmoguard to free
// ports — needed because the package-level reload tests boot real listeners
// and would otherwise collide with each other on the default static ports.
func portYAMLHeader(t *testing.T) string {
	lcd, rpc, grpc, evmRpc, evmRpcWs := reloadTestPorts(t)
	return fmt.Sprintf(`
host: 127.0.0.1
lcdPort: %d
rpcPort: %d
grpcPort: %d
evmRpcPort: %d
evmRpcWsPort: %d
metrics:
  enable: false
`, lcd, rpc, grpc, evmRpc, evmRpcWs)
}

// TestValidateOnly_RejectsBadConfig is the unit-level companion to the
// `cosmoguard --validate` CLI subcommand: the same ReadConfigFromFile
// call the CLI uses must fail loudly on malformed input so operators see
// the problem at pre-deploy time, not at first request.
func TestValidateOnly_RejectsBadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "bad.yaml")

	badYAML := `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: ["[unclosed-glob"]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(badYAML), 0644))

	_, err := ReadConfigFromFile(cfgPath)
	assert.Assert(t, err != nil, "validate path must reject malformed config")
	assert.Assert(t, strings.Contains(err.Error(), "[unclosed-glob"),
		"error should name the offending value: %v", err)
}

// TestValidateOnly_AcceptsValidConfig: positive case — clean config
// produces no error.
func TestValidateOnly_AcceptsValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "ok.yaml")

	yaml := `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/cosmos/bank/v1beta1/params]
      methods: [GET]
      cache:
        enable: true
        ttl: 5s
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(yaml), 0644))

	cfg, err := ReadConfigFromFile(cfgPath)
	assert.NilError(t, err)
	assert.Assert(t, cfg != nil)
	assert.Equal(t, len(cfg.LCD.Rules), 1)
}

// TestTryReload_BadConfigPreservesPrevious is the integration story for
// hot-reload resilience: when the new config fails to load, the in-memory
// cfg pointer must keep pointing at the last-known-good ruleset.
func TestTryReload_BadConfigPreservesPrevious(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")

	goodYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/cosmos/bank/v1beta1/balances]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(goodYAML), 0644))

	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	// Capture the *Config pointer. tryReload's only assignment to cg.cfg is
	// `f.cfg = newCfg` AFTER a successful PrepareConfig. A failed reload
	// must leave cg.cfg pointing at the exact same Config object — pointer
	// identity is the strongest assertion possible.
	originalCfgPtr := cg.cfg
	originalRules := cg.cfg.LCD.Rules
	assert.Equal(t, len(originalRules), 1)
	originalPath := originalRules[0].Paths[0]

	// Stomp the file with a config that fails to compile (invalid glob).
	badYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: ["[invalid-glob"]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(badYAML), 0644))

	cg.tryReload()

	// Pointer identity preserved: cg.cfg was never reassigned.
	assert.Equal(t, cg.cfg, originalCfgPtr,
		"failed reload must not replace the *Config pointer")
	// Rules still in place.
	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], originalPath,
		"failed reload must not mutate the running ruleset")
}

// TestTryReload_GoodConfigReplacesPrevious is the positive case: a valid
// reload swaps the ruleset.
func TestTryReload_GoodConfigReplacesPrevious(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")

	originalYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/v1]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(originalYAML), 0644))

	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], "/v1")

	newYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 200
      action: allow
      paths: [/v2]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(newYAML), 0644))

	cg.tryReload()

	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], "/v2")
	assert.Equal(t, cg.cfg.LCD.Rules[0].Priority, 200)
}

// TestTryReload_AcceptsRuleOnlyChange — a reload that touches only
// rules (leaves the cache section identical) must succeed. Without
// this guard the cache-topology check could over-reject and break
// the common "edit a rule, save, see it apply" workflow.
func TestTryReload_AcceptsRuleOnlyChange(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")

	originalYAML := portYAMLHeader(t) + `
cache:
  key: x
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/v1]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(originalYAML), 0644))

	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	newYAML := portYAMLHeader(t) + `
cache:
  key: x
lcd:
  default: allow
  rules:
    - priority: 200
      action: allow
      paths: [/v2]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(newYAML), 0644))
	cg.tryReload()

	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], "/v2",
		"rule-only reload must take effect even with the cache-topology guard in place")
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, true,
		"rule-only reload must record success, not be misclassified as a topology change")
}

// TestWatchConfigFile_IgnoresSiblingFileEvents proves the directory watcher
// is filtered by filename. Dropping the config in a busy directory (the
// classic case: `/tmp`) must not turn every neighbouring temp file into
// a reload — see the storm produced by the unfiltered version, where a
// single startup against `/tmp` produced tens of thousands of reloads
// per second from unrelated editor / shell churn.
func TestWatchConfigFile_IgnoresSiblingFileEvents(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")

	originalYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/v1]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(originalYAML), 0644))

	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	go func() { _ = cg.WatchConfigFile() }()
	// Let the watcher install before we generate events.
	time.Sleep(100 * time.Millisecond)

	// currentCfg reads cg.cfg under the same mutex tryReload writes
	// under. Plain reads would race against the watcher goroutine.
	currentCfg := func() *Config {
		cg.configMutex.Lock()
		defer cg.configMutex.Unlock()
		return cg.cfg
	}

	originalCfgPtr := currentCfg()

	// Churn the directory with sibling files. None of these are the
	// config; the watcher must not react to any of them.
	for i := 0; i < 5; i++ {
		sibling := filepath.Join(tmpDir, fmt.Sprintf("noise-%d.tmp", i))
		assert.NilError(t, os.WriteFile(sibling, []byte("x"), 0644))
		assert.NilError(t, os.Remove(sibling))
	}
	time.Sleep(150 * time.Millisecond)

	assert.Equal(t, currentCfg(), originalCfgPtr,
		"sibling-file churn must not trigger a config reload")

	// Now actually change the config file. The watcher must pick that up.
	newYAML := portYAMLHeader(t) + `
lcd:
  default: allow
  rules:
    - priority: 200
      action: allow
      paths: [/v2]
      methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(newYAML), 0644))

	deadline := time.Now().Add(2 * time.Second)
	var swapped *Config
	for time.Now().Before(deadline) {
		swapped = currentCfg()
		if swapped != originalCfgPtr {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Assert(t, swapped != originalCfgPtr,
		"writing the config file should trigger a reload")
	assert.Equal(t, swapped.LCD.Rules[0].Paths[0], "/v2")
}

// TestTryReload_RejectsEnableEvmToggle is the regression test for the
// hot-reload EVM-toggle panic: an instance started with enableEvm:false
// never builds the EVM proxies, so accepting a reload that flips it on
// would make applyRulesLocked dereference nil EVM servers and crash the
// watcher. The reload must be rejected (cfg unchanged, recorded failed),
// not panic.
func TestTryReload_RejectsEnableEvmToggle(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")

	// One header → stable ports, so the only delta is the EVM flip.
	header := portYAMLHeader(t)
	base := header + `
lcd:
  default: allow
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(base), 0644))

	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })
	assert.Equal(t, cg.cfg.EnableEvm, false)
	originalCfgPtr := cg.cfg

	flipped := header + `
enableEvm: true
lcd:
  default: allow
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(flipped), 0644))

	cg.tryReload() // must not panic

	assert.Equal(t, cg.cfg, originalCfgPtr,
		"enableEvm flip must be rejected: cfg pointer unchanged")
	assert.Equal(t, cg.cfg.EnableEvm, false)
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, false,
		"enableEvm toggle reload must be recorded as failed")
}

// authReloadYAML builds a config with an auth block + one LCD rule path,
// so the auth-immutability tests can vary auth vs rules independently.
func authReloadYAML(header, identities, rulePath string) string {
	return header + `
auth:
  enable: true
  methods:
    - type: api-key
      header: x-api-key
  identities:
` + identities + `
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [` + rulePath + `]
      methods: [GET]
`
}

// TestTryReload_RejectsAuthChange is the regression test for the auth
// hot-reload gap: the Authenticator is built once at startup, so a reload
// that changes the global auth block (e.g. adds/removes an identity) must
// be rejected — accepting it would report success while still
// authenticating against the old credentials.
func TestTryReload_RejectsAuthChange(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")
	header := portYAMLHeader(t)

	oneID := "    - name: a\n      apiKey: key-a\n      scopes: [read]\n"
	twoIDs := oneID + "    - name: b\n      apiKey: key-b\n      scopes: [read]\n"

	assert.NilError(t, os.WriteFile(cfgPath, []byte(authReloadYAML(header, oneID, "/v1")), 0644))
	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })
	originalCfgPtr := cg.cfg

	// Add a second identity → auth block changed → reject.
	assert.NilError(t, os.WriteFile(cfgPath, []byte(authReloadYAML(header, twoIDs, "/v1")), 0644))
	cg.tryReload()

	assert.Equal(t, cg.cfg, originalCfgPtr, "auth change must be rejected: cfg pointer unchanged")
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, false, "auth change reload must be recorded as failed")
}

// TestTryReload_AcceptsRuleChangeWithAuthUnchanged guards against the
// auth-immutability check over-rejecting: when the auth block is
// byte-for-byte unchanged, a rule-only edit must still hot-reload.
func TestTryReload_AcceptsRuleChangeWithAuthUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")
	header := portYAMLHeader(t)
	oneID := "    - name: a\n      apiKey: key-a\n      scopes: [read]\n"

	assert.NilError(t, os.WriteFile(cfgPath, []byte(authReloadYAML(header, oneID, "/v1")), 0644))
	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	// Same auth, different rule path → must be accepted.
	assert.NilError(t, os.WriteFile(cfgPath, []byte(authReloadYAML(header, oneID, "/v2")), 0644))
	cg.tryReload()

	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], "/v2",
		"rule-only reload must apply even with an (unchanged) auth block present")
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, true,
		"unchanged auth must not trip the auth-immutability rejection")
}

// TestTryReload_RejectsNodesChange is the regression test for the
// upstream-topology hot-reload gap: pools are built once at startup from
// cfg.Nodes, so a reload that changes nodes: must be rejected
// (restart-required) rather than reported successful while traffic keeps
// going to the old upstream set.
func TestTryReload_RejectsNodesChange(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")
	header := portYAMLHeader(t)

	base := header + `
nodes:
  - name: a
    host: node-a
    rpcPort: 26657
lcd:
  default: allow
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(base), 0644))
	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })
	originalCfgPtr := cg.cfg

	// Change the upstream host → topology change → reject.
	changed := header + `
nodes:
  - name: a
    host: node-b
    rpcPort: 26657
lcd:
  default: allow
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(changed), 0644))
	cg.tryReload()

	assert.Equal(t, cg.cfg, originalCfgPtr, "nodes change must be rejected: cfg pointer unchanged")
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, false, "nodes change reload must be recorded as failed")
}

// TestTryReload_AcceptsRuleChangeWithNodesUnchanged guards the nodes:
// immutability check against over-rejecting: a rule-only edit (nodes:
// byte-for-byte unchanged) must still hot-reload.
func TestTryReload_AcceptsRuleChangeWithNodesUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "cosmoguard.yaml")
	header := portYAMLHeader(t)
	node := `
nodes:
  - name: a
    host: node-a
    rpcPort: 26657
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(header+node+"lcd:\n  default: allow\n  rules:\n    - priority: 100\n      action: allow\n      paths: [/v1]\n      methods: [GET]\n"), 0644))
	cg, err := NewFromFile(cfgPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })

	assert.NilError(t, os.WriteFile(cfgPath, []byte(header+node+"lcd:\n  default: allow\n  rules:\n    - priority: 200\n      action: allow\n      paths: [/v2]\n      methods: [GET]\n"), 0644))
	cg.tryReload()

	assert.Equal(t, cg.cfg.LCD.Rules[0].Paths[0], "/v2", "rule-only reload must apply with an unchanged nodes block")
	status := cg.dashboard.lastReload
	assert.Assert(t, status != nil)
	assert.Equal(t, status.Success, true, "unchanged nodes must not trip the topology rejection")
}
