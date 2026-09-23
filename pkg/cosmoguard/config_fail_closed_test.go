package cosmoguard

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readTestConfig(t *testing.T, content string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return ReadConfigFromFile(path)
}

func TestReadConfigFromFileRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name, content, want string
	}{
		{"top level", "lcdPor: 11317\n", "lcdPor"},
		{"auth option", "auth:\n  enabled: true\n", "enabled"},
		{"rule option", "lcd:\n  rules:\n    - action: allow\n      rateLimt:\n        rate: 1/s\n", "rateLimt"},
		{"rate option", "lcd:\n  rules:\n    - action: allow\n      rateLimit:\n        rate: 1/s\n        scpoe: per-ip\n", "scpoe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readTestConfig(t, tt.content)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected unknown field %q, got %v", tt.want, err)
			}
		})
	}
}

func TestReadConfigFromFileRejectsTrailingDocument(t *testing.T) {
	for _, content := range []string{
		"lcd:\n  default: allow\n---\nlcd:\n  default: deny\n",
		"lcd:\n  default: allow\n---\n",
	} {
		_, err := readTestConfig(t, content)
		if err == nil || !strings.Contains(err.Error(), "document") {
			t.Fatalf("expected trailing document error, got %v", err)
		}
	}
}

func TestReadConfigFromFilePreservesFlexibleValues(t *testing.T) {
	cfg, err := readTestConfig(t, `
lcd:
  rules:
    - action: allow
      query:
        custom-height: "*"
rpc:
  jsonrpc:
    rules:
      - action: deny
        params:
          custom-height: 10
`)
	if err != nil {
		t.Fatal(err)
	}
	params, ok := cfg.RPC.JsonRpc.Rules[0].Params.(map[string]any)
	if cfg.LCD.Rules[0].Query["custom-height"] != "*" || !ok || params["custom-height"] != float64(10) {
		t.Fatalf("flexible match fields were lost: %+v", cfg)
	}
}

func TestReadConfigFromFilePreservesEmptyAndRemovedKeyBehavior(t *testing.T) {
	for _, content := range []string{"", "# no settings\n"} {
		cfg, err := readTestConfig(t, content)
		if err != nil || cfg.LCD.Default != RuleActionDeny {
			t.Fatalf("empty config should use defaults: cfg=%+v err=%v", cfg, err)
		}
	}
	_, err := readTestConfig(t, "cache:\n  redis: {}\n")
	if err == nil || !strings.Contains(err.Error(), "removed in v4") {
		t.Fatalf("expected removed-key migration guidance, got %v", err)
	}
}

func TestRuleCompileRejectsInvalidActions(t *testing.T) {
	compilers := []struct {
		name    string
		compile func(RuleAction) error
	}{
		{"http", func(action RuleAction) error { return (&HttpRule{Priority: 17, Action: action}).Compile() }},
		{"jsonrpc", func(action RuleAction) error { return (&JsonRpcRule{Priority: 17, Action: action}).Compile() }},
		{"grpc", func(action RuleAction) error { return (&GrpcRule{Priority: 17, Action: action}).Compile() }},
	}
	for _, compiler := range compilers {
		for _, action := range []RuleAction{"", "Allow", "allow-all"} {
			t.Run(compiler.name+"/"+string(action), func(t *testing.T) {
				err := compiler.compile(action)
				if err == nil || !strings.Contains(err.Error(), compiler.name+" rule (priority 17)") || !strings.Contains(err.Error(), "action") {
					t.Fatalf("expected contextual invalid action error, got %v", err)
				}
			})
		}
		for _, action := range []RuleAction{RuleActionAllow, RuleActionDeny} {
			if err := compiler.compile(action); err != nil {
				t.Fatalf("valid %s action %q: %v", compiler.name, action, err)
			}
		}
	}
}

func TestPrepareConfigRejectsInvalidDefaults(t *testing.T) {
	tests := []struct {
		name string
		set  func(*Config)
	}{
		{"lcd", func(c *Config) { c.LCD.Default = "permit" }},
		{"rpc", func(c *Config) { c.RPC.Default = "permit" }},
		{"rpc.jsonrpc", func(c *Config) { c.RPC.JsonRpc.Default = "permit" }},
		{"grpc", func(c *Config) { c.GRPC.Default = "permit" }},
		{"evm.rpc", func(c *Config) { c.EVM.RPC.Default = "permit" }},
		{"evm.ws", func(c *Config) { c.EVM.WS.Default = "permit" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			tt.set(cfg)
			err := PrepareConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.name+".default") {
				t.Fatalf("expected invalid default for %s, got %v", tt.name, err)
			}
		})
	}
}

func TestRuleCompileRejectsInvalidRates(t *testing.T) {
	compilers := []struct {
		name    string
		compile func(*RateLimitConfig) error
	}{
		{"http", func(rate *RateLimitConfig) error {
			return (&HttpRule{Priority: 17, Action: RuleActionAllow, RateLimit: rate}).Compile()
		}},
		{"jsonrpc", func(rate *RateLimitConfig) error {
			return (&JsonRpcRule{Priority: 17, Action: RuleActionAllow, RateLimit: rate}).Compile()
		}},
		{"grpc", func(rate *RateLimitConfig) error {
			return (&GrpcRule{Priority: 17, Action: RuleActionAllow, RateLimit: rate}).Compile()
		}},
	}
	for _, compiler := range compilers {
		for _, rate := range []struct {
			name  string
			value float64
		}{
			{"missing", 0}, {"negative", -1}, {"nan", math.NaN()},
			{"positive infinity", math.Inf(1)}, {"negative infinity", math.Inf(-1)},
		} {
			t.Run(compiler.name+"/"+rate.name, func(t *testing.T) {
				err := compiler.compile(&RateLimitConfig{Rate: Rate{PerSecond: rate.value}})
				if err == nil || !strings.Contains(err.Error(), compiler.name+" rule (priority 17)") || !strings.Contains(err.Error(), "rateLimit.rate") {
					t.Fatalf("expected contextual invalid rate error, got %v", err)
				}
			})
		}
		if err := compiler.compile(&RateLimitConfig{Rate: Rate{PerSecond: 1}}); err != nil {
			t.Fatalf("valid %s rate: %v", compiler.name, err)
		}
	}
}

func TestReadConfigFromFileRejectsInvalidRate(t *testing.T) {
	for _, value := range []string{"", "0", "-1"} {
		t.Run(value, func(t *testing.T) {
			content := "lcd:\n  rules:\n    - priority: 17\n      action: allow\n      rateLimit: {}\n"
			if value != "" {
				content = "lcd:\n  rules:\n    - priority: 17\n      action: allow\n      rateLimit:\n        rate: " + value + "\n"
			}
			_, err := readTestConfig(t, content)
			if err == nil || !strings.Contains(err.Error(), "rateLimit.rate") {
				t.Fatalf("expected invalid YAML rate error, got %v", err)
			}
		})
	}
}

func TestReadConfigFromFileRejectsNullRules(t *testing.T) {
	for _, tt := range []struct {
		name, yaml, want string
	}{
		{"http singleton", "lcd:\n  rules: [null]\n", "http rules[0]"},
		{"http mixed", "rpc:\n  rules:\n    - action: allow\n    -\n", "http rules[1]"},
		{"jsonrpc singleton", "rpc:\n  jsonrpc:\n    rules: [null]\n", "jsonrpc rules[0]"},
		{"jsonrpc mixed", "evm:\n  ws:\n    rules:\n      - action: allow\n      - null\n", "jsonrpc rules[1]"},
		{"grpc singleton", "grpc:\n  rules: [null]\n", "grpc rules[0]"},
		{"grpc mixed", "grpc:\n  rules:\n    - action: allow\n    -\n", "grpc rules[1]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != nil {
					t.Errorf("null rule panicked: %v", got)
				}
			}()
			_, err := readTestConfig(t, tt.yaml)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %s error, got %v", tt.want, err)
			}
		})
	}
}

func TestRuleCompileRejectsNegativeBurst(t *testing.T) {
	for _, tt := range []struct {
		name    string
		compile func(int) error
	}{
		{"http", func(burst int) error {
			return (&HttpRule{Priority: 17, Action: RuleActionAllow, RateLimit: &RateLimitConfig{Rate: Rate{PerSecond: 10}, Burst: burst}}).Compile()
		}},
		{"jsonrpc", func(burst int) error {
			return (&JsonRpcRule{Priority: 17, Action: RuleActionAllow, RateLimit: &RateLimitConfig{Rate: Rate{PerSecond: 10}, Burst: burst}}).Compile()
		}},
		{"grpc", func(burst int) error {
			return (&GrpcRule{Priority: 17, Action: RuleActionAllow, RateLimit: &RateLimitConfig{Rate: Rate{PerSecond: 10}, Burst: burst}}).Compile()
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.compile(-4)
			if err == nil || !strings.Contains(err.Error(), tt.name+" rule (priority 17)") || !strings.Contains(err.Error(), "rateLimit.burst") {
				t.Fatalf("expected contextual negative burst error, got %v", err)
			}
			if err := tt.compile(0); err != nil {
				t.Fatalf("omitted burst should use default: %v", err)
			}
		})
	}
}

func TestReadConfigFromFileRejectsNegativeBurst(t *testing.T) {
	_, err := readTestConfig(t, "lcd:\n  rules:\n    - priority: 17\n      action: allow\n      rateLimit:\n        rate: 10\n        burst: -4\n")
	if err == nil || !strings.Contains(err.Error(), "http rule (priority 17)") || !strings.Contains(err.Error(), "rateLimit.burst") {
		t.Fatalf("expected contextual negative burst error, got %v", err)
	}
}
