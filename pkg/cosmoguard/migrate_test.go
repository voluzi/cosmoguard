package cosmoguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/assert"
)

func TestDetectV3Syntax_FlatRules(t *testing.T) {
	cfg := &Config{
		Node: NodeConfig{Host: "1.2.3.4", LcdPort: 1317},
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{
				{Priority: 100, Action: RuleActionAllow, Paths: []string{"/x"}},
				{Priority: 200, Action: RuleActionAllow, Match: &MatchTree{Path: "/y"}},
			},
		},
	}
	// Simulate the PrepareConfig stage that promotes v3 Node → Nodes.
	cfg.Nodes = []NodeConfig{cfg.Node}

	d := DetectV3Syntax(cfg)
	assert.Equal(t, d.SingularNode, true)
	assert.Equal(t, d.FlatRuleCount, 1)
	assert.Equal(t, d.FlatRulesBySection["lcd"], 1)
	assert.Equal(t, d.HasAny(), true)
}

func TestDetectV3Syntax_PureV4(t *testing.T) {
	cfg := &Config{
		Nodes: []NodeConfig{{Host: "1.2.3.4", LcdPort: 1317}},
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{
				{Priority: 100, Action: RuleActionAllow, Match: &MatchTree{Path: "/x"}},
			},
		},
	}
	d := DetectV3Syntax(cfg)
	assert.Equal(t, d.HasAny(), false)
}

// TestMigrateV3Config: a v3 file is rewritten in v4 form; the original
// is preserved at .v3.bak; the rewritten file parses cleanly.
func TestMigrateV3Config(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "v3.yaml")

	v3YAML := `
host: 0.0.0.0
node:
  host: 10.0.0.1
  lcdPort: 1317
  rpcPort: 26657
  grpcPort: 9090
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      paths: [/cosmos/bank/v1beta1/balances]
      methods: [GET]
      query:
        height: "*"
rpc:
  default: allow
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(v3YAML), 0o644))

	n, err := MigrateV3Config(cfgPath)
	assert.NilError(t, err)
	assert.Assert(t, n > 0, "expected migration to apply at least 1 change")

	// Backup exists with the original content.
	bak, err := os.ReadFile(cfgPath + ".v3.bak")
	assert.NilError(t, err)
	assert.Equal(t, string(bak), v3YAML)

	// Rewritten file:
	migrated, err := os.ReadFile(cfgPath)
	assert.NilError(t, err)
	out := string(migrated)
	// v4 shape signals:
	assert.Assert(t, strings.Contains(out, "nodes:"), "migrated config should use `nodes:`")
	assert.Assert(t, strings.Contains(out, "match:"), "migrated config should wrap rules in `match:`")
	// v3 shape should be gone:
	assert.Assert(t, !strings.Contains(out, "\nnode:\n"), "migrated config should not have top-level `node:`: %s", out)

	// Rewritten file parses cleanly and matches semantics.
	cfg, err := ReadConfigFromFile(cfgPath)
	assert.NilError(t, err)
	assert.Equal(t, len(cfg.Nodes), 1)
	assert.Equal(t, cfg.Nodes[0].Host, "10.0.0.1")
	assert.Equal(t, len(cfg.LCD.Rules), 1)
	assert.Equal(t, cfg.LCD.Rules[0].Match.Paths[0], "/cosmos/bank/v1beta1/balances")
}

// TestMigrateV3Config_AlreadyV4: a v4 file produces no changes and no
// backup is written.
func TestMigrateV3Config_AlreadyV4(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "v4.yaml")

	v4YAML := `
nodes:
  - host: 10.0.0.1
    lcdPort: 1317
    rpcPort: 26657
lcd:
  default: allow
  rules:
    - priority: 100
      action: allow
      match:
        paths: [/cosmos/bank/v1beta1/balances]
        methods: [GET]
`
	assert.NilError(t, os.WriteFile(cfgPath, []byte(v4YAML), 0o644))

	n, err := MigrateV3Config(cfgPath)
	assert.NilError(t, err)
	assert.Equal(t, n, 0)
	// No backup written.
	_, err = os.Stat(cfgPath + ".v3.bak")
	assert.Assert(t, os.IsNotExist(err))
}
