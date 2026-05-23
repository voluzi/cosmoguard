package cosmoguard

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// V3Detection enumerates the v3-syntax features found in a config. Empty
// when the config is already v4-shaped.
type V3Detection struct {
	// SingularNode is true when the operator used `node:` instead of
	// `nodes:`. v3 compat path; safe to migrate.
	SingularNode bool

	// FlatRuleCount is the number of HTTP rules that still use the v3
	// flat syntax (paths/methods/query at the rule top level instead of
	// inside a `match:` block). Each one will be desugared on load by
	// Compile; migrating them to v4 is purely cosmetic but makes future
	// schema changes safer.
	FlatRuleCount int

	// FlatRulesBySection groups counts by which section of the config
	// they live in, for the log message.
	FlatRulesBySection map[string]int
}

// HasAny reports whether anything v3-shaped was found.
func (d V3Detection) HasAny() bool {
	return d.SingularNode || d.FlatRuleCount > 0
}

// DetectV3Syntax scans a parsed-but-not-yet-Compiled config for v3-shaped
// pieces. Compile mutates rules in place (it merges v3 fields into a
// match tree), so detection must run on a fresh parse. The CLI flow:
//
//	raw := ReadConfigFromFile(path)        // ← compile already ran here
//	det := DetectV3Syntax(reparseRawYAML)  // ← re-parse to detect
//
// In practice the simplest path is to detect at MigrateV3Config time,
// which re-reads the file.
func DetectV3Syntax(cfg *Config) V3Detection {
	d := V3Detection{FlatRulesBySection: map[string]int{}}
	// Singular node detection: cfg.Node is the v3 field. The detector
	// gets called on a RAW YAML parse from NewFromFile (no PrepareConfig
	// has promoted Node → Nodes yet), so v3 looks like Node populated +
	// Nodes empty. The previous condition required `len(Nodes) == 1`
	// which never holds on a raw parse, so the operator warning never
	// fired in production. Detect off Node directly: a non-empty Host
	// in the singular field is unambiguous v3 syntax regardless of
	// whether Nodes is set in parallel (the latter is a separate error
	// caught by PrepareConfig).
	if cfg.Node.Host != "" {
		d.SingularNode = true
	}

	count := func(section string, rules []*HttpRule) {
		for _, r := range rules {
			if usesV3FlatSyntax(r) {
				d.FlatRuleCount++
				d.FlatRulesBySection[section]++
			}
		}
	}
	count("lcd", cfg.LCD.Rules)
	count("rpc", cfg.RPC.Rules)
	count("evm.rpc.httpRules", cfg.EVM.RPC.HttpRules)
	return d
}

func usesV3FlatSyntax(r *HttpRule) bool {
	if r == nil {
		return false
	}
	if len(r.Paths) > 0 || len(r.Methods) > 0 || len(r.Query) > 0 {
		return true
	}
	return false
}

// LogV3Detection emits a structured warning describing what was found
// and recommending --migrate-config. Called from NewFromFile on every
// load so operators see the message in their startup logs.
func LogV3Detection(d V3Detection, configPath string) {
	if !d.HasAny() {
		return
	}
	fields := Fields{
		"config": configPath,
	}
	if d.SingularNode {
		fields["v3.node"] = "singular node: detected (use `nodes:` list in v4)"
	}
	if d.FlatRuleCount > 0 {
		// Compact rendering of the per-section counts for the log.
		parts := make([]string, 0, len(d.FlatRulesBySection))
		secs := make([]string, 0, len(d.FlatRulesBySection))
		for k := range d.FlatRulesBySection {
			secs = append(secs, k)
		}
		sort.Strings(secs)
		for _, s := range secs {
			parts = append(parts, fmt.Sprintf("%s:%d", s, d.FlatRulesBySection[s]))
		}
		fields["v3.flat_rules"] = strings.Join(parts, ",")
	}
	log.WithFields(fields).Warn(
		"v3 config syntax detected; consider running with --migrate-config " +
			"to rewrite the file in v4 form (functionally equivalent, just clearer)",
	)
}

// MigrateV3Config rewrites configPath into the v4 form. The original
// file is backed up to <configPath>.v3.bak (overwriting any previous
// backup). Returns the number of changes applied; 0 means the config
// was already v4-shaped.
//
// Migration is purely cosmetic — v3 syntax keeps working forever via
// the desugaring path in Compile. The point is operator clarity for
// new readers and to reduce ambiguity in future schema evolutions.
func MigrateV3Config(configPath string) (int, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return 0, fmt.Errorf("read config: %w", err)
	}
	// Env interpolation happens at READ time, but for migration we
	// preserve the original ${VAR} references — operators don't want
	// their secrets baked into the YAML on disk. Re-parse the raw bytes
	// WITHOUT interpolation so the migration output round-trips var
	// references unchanged.
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return 0, fmt.Errorf("parse config: %w", err)
	}

	changes := 0

	// Promote singular `node:` into `nodes:` list.
	if cfg.Node.Host != "" && len(cfg.Nodes) == 0 {
		// Copy Node into the list. Don't clear Node itself yet — yaml
		// marshaling with `omitempty` will drop the zero-valued struct
		// after we zero it out.
		cfg.Nodes = []NodeConfig{cfg.Node}
		cfg.Node = NodeConfig{}
		changes++
	}

	// Convert flat-syntax rules.
	convert := func(rules []*HttpRule) {
		for _, r := range rules {
			if !usesV3FlatSyntax(r) {
				continue
			}
			if r.Match == nil {
				r.Match = &MatchTree{}
			}
			r.Match.Paths = append(r.Match.Paths, r.Paths...)
			r.Match.Methods = append(r.Match.Methods, r.Methods...)
			if len(r.Query) > 0 {
				if r.Match.Query == nil {
					r.Match.Query = map[string]string{}
				}
				for k, v := range r.Query {
					r.Match.Query[k] = v
				}
			}
			r.Paths = nil
			r.Methods = nil
			r.Query = nil
			changes++
		}
	}
	convert(cfg.LCD.Rules)
	convert(cfg.RPC.Rules)
	convert(cfg.EVM.RPC.HttpRules)

	if changes == 0 {
		return 0, nil
	}

	// Back up the original.
	backupPath := configPath + ".v3.bak"
	if err := os.WriteFile(backupPath, raw, 0o600); err != nil {
		return 0, fmt.Errorf("write backup %s: %w", backupPath, err)
	}

	// Render the migrated config. Use yaml.v3's indent for readability;
	// the YAML library preserves omitempty so cleared v3 fields drop out.
	var buf strings.Builder
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&cfg); err != nil {
		return 0, fmt.Errorf("encode v4 config: %w", err)
	}
	enc.Close()

	header := fmt.Sprintf("# cosmoguard config — migrated v3 → v4 by `cosmoguard --migrate-config`\n# original preserved at %s (timestamp %s)\n",
		filepath.Base(backupPath), time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(configPath, []byte(header+buf.String()), 0o600); err != nil {
		return 0, fmt.Errorf("write migrated config: %w", err)
	}
	return changes, nil
}
