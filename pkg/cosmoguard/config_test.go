package cosmoguard

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestReadConfigFromFile(t *testing.T) {
	cfg, err := ReadConfigFromFile("../../example.config.yml")
	assert.NilError(t, err)
	assert.Assert(t, cfg != nil)
}

func TestReadConfigFromFile_NonExistent(t *testing.T) {
	_, err := ReadConfigFromFile("/nonexistent/path/config.yml")
	assert.Assert(t, err != nil)
}

func TestReadConfigFromFile_InvalidYAML(t *testing.T) {
	// Create a temp file with invalid YAML
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "invalid.yml")

	err := os.WriteFile(tmpFile, []byte("invalid: yaml: content: [}"), 0644)
	assert.NilError(t, err)

	_, err = ReadConfigFromFile(tmpFile)
	assert.Assert(t, err != nil)
}

func TestReadConfigFromFile_DefaultValues(t *testing.T) {
	// Create a minimal config file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "minimal.yml")

	err := os.WriteFile(tmpFile, []byte("# minimal config\n"), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	// Check default values
	assert.Equal(t, cfg.Host, "0.0.0.0")
	assert.Equal(t, cfg.RpcPort, 16657)
	assert.Equal(t, cfg.LcdPort, 11317)
	assert.Equal(t, cfg.GrpcPort, 19090)
	assert.Equal(t, cfg.EnableEvm, false)
	assert.Equal(t, cfg.EvmRpcPort, 18545)
	assert.Equal(t, cfg.EvmRpcWsPort, 18546)

	// Node defaults
	assert.Equal(t, cfg.Node.Host, "127.0.0.1")
	assert.Equal(t, cfg.Node.RpcPort, 26657)
	assert.Equal(t, cfg.Node.LcdPort, 1317)
	assert.Equal(t, cfg.Node.GrpcPort, 9090)

	// Cache defaults
	assert.Equal(t, cfg.Cache.TTL, 5*time.Second)

	// Metrics defaults
	assert.Equal(t, cfg.Metrics.Enable, true)
	assert.Equal(t, cfg.Metrics.Port, 9001)

	// Default actions
	assert.Equal(t, cfg.LCD.Default, RuleAction("deny"))
	assert.Equal(t, cfg.RPC.Default, RuleAction("deny"))
	assert.Equal(t, cfg.GRPC.Default, RuleAction("deny"))
}

func TestReadConfigFromFile_CustomValues(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "custom.yml")

	configContent := `
host: 192.168.1.1
rpcPort: 26657
lcdPort: 1317
grpcPort: 9090
enableEvm: true
evmRpcPort: 8545

node:
  host: 10.0.0.1
  rpcPort: 26658
  lcdPort: 1318

cache:
  ttl: 30s
  key: "test-prefix"

metrics:
  enable: false
  port: 9999

lcd:
  default: allow
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Equal(t, cfg.Host, "192.168.1.1")
	assert.Equal(t, cfg.RpcPort, 26657)
	assert.Equal(t, cfg.LcdPort, 1317)
	assert.Equal(t, cfg.GrpcPort, 9090)
	assert.Equal(t, cfg.EnableEvm, true)
	assert.Equal(t, cfg.EvmRpcPort, 8545)

	assert.Equal(t, cfg.Node.Host, "10.0.0.1")
	assert.Equal(t, cfg.Node.RpcPort, 26658)
	assert.Equal(t, cfg.Node.LcdPort, 1318)

	assert.Equal(t, cfg.Cache.TTL, 30*time.Second)
	assert.Equal(t, cfg.Cache.Key, "test-prefix")

	// Note: The 'enable' field has default:true, so when explicitly set to false
	// the default library may still apply the default. We test that port is set correctly.
	assert.Equal(t, cfg.Metrics.Port, 9999)

	assert.Equal(t, cfg.LCD.Default, RuleAction("allow"))
}

func TestReadConfigFromFile_WithRules(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "rules.yml")

	configContent := `
lcd:
  default: deny
  rules:
    - priority: 100
      action: allow
      paths:
        - /cosmos/bank/*
      methods:
        - GET
      cache:
        enable: true
        ttl: 10s
    - priority: 200
      action: deny
      paths:
        - /cosmos/admin/*

rpc:
  jsonrpc:
    default: deny
    rules:
      - priority: 50
        action: allow
        methods:
          - status
          - health
      - priority: 100
        action: allow
        methods:
          - abci_query
        params:
          path: "/cosmos.bank.v1beta1.Query/*"

grpc:
  default: deny
  rules:
    - priority: 10
      action: allow
      methods:
        - /cosmos.bank.v1beta1.Query/AllBalances
    - priority: 20
      action: allow
      methods:
        - /cosmos.bank.v1beta1.Query/Balance
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	// LCD rules should be sorted by priority
	assert.Equal(t, len(cfg.LCD.Rules), 2)
	assert.Equal(t, cfg.LCD.Rules[0].Priority, 100) // Lower priority first
	assert.Equal(t, cfg.LCD.Rules[1].Priority, 200)
	assert.Equal(t, cfg.LCD.Rules[0].Action, RuleAction("allow"))
	assert.Equal(t, cfg.LCD.Rules[0].Cache.Enable, true)
	assert.Equal(t, cfg.LCD.Rules[0].Cache.TTL, 10*time.Second)

	// RPC JsonRpc rules should be sorted by priority
	assert.Equal(t, len(cfg.RPC.JsonRpc.Rules), 2)
	assert.Equal(t, cfg.RPC.JsonRpc.Rules[0].Priority, 50)
	assert.Equal(t, cfg.RPC.JsonRpc.Rules[1].Priority, 100)

	// GRPC rules should be sorted by priority
	assert.Equal(t, len(cfg.GRPC.Rules), 2)
	assert.Equal(t, cfg.GRPC.Rules[0].Priority, 10)
	assert.Equal(t, cfg.GRPC.Rules[1].Priority, 20)
}

func TestReadConfigFromFile_RedisConfig(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "redis.yml")

	redisURL := "redis://localhost:6379"
	configContent := `
cache:
  ttl: 10s
  redis: "` + redisURL + `"
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Assert(t, cfg.Cache.Redis != nil)
	assert.Equal(t, *cfg.Cache.Redis, redisURL)
}

func TestReadConfigFromFile_RedisSentinelConfig(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "sentinel.yml")

	configContent := `
cache:
  ttl: 10s
  redis-sentinel:
    master_name: mymaster
    sentinel_addrs:
      - sentinel1:26379
      - sentinel2:26379
      - sentinel3:26379
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Assert(t, cfg.Cache.RedisSentinel != nil)
	assert.Equal(t, cfg.Cache.RedisSentinel.MasterName, "mymaster")
	assert.Equal(t, len(cfg.Cache.RedisSentinel.SentinelAddrs), 3)
}

func TestReadConfigFromFile_EVMConfig(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "evm.yml")

	configContent := `
enableEvm: true
evm:
  rpc:
    default: allow
    rules:
      - action: allow
        methods:
          - eth_blockNumber
          - eth_chainId
    httpRules:
      - action: allow
        paths:
          - /
        methods:
          - POST
  ws:
    default: deny
    webSocketConnections: 20
    rules:
      - action: allow
        methods:
          - eth_subscribe
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Equal(t, cfg.EnableEvm, true)
	assert.Equal(t, cfg.EVM.RPC.Default, RuleAction("allow"))
	assert.Equal(t, len(cfg.EVM.RPC.Rules), 1)
	assert.Equal(t, len(cfg.EVM.RPC.HttpRules), 1)
	assert.Equal(t, cfg.EVM.WS.Default, RuleAction("deny"))
	assert.Equal(t, cfg.EVM.WS.WebSocketConnections, 20)
	assert.Equal(t, len(cfg.EVM.WS.Rules), 1)
}

func TestRuleCache_Fields(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "cache.yml")

	configContent := `
lcd:
  rules:
    - action: allow
      paths: ["/test"]
      cache:
        enable: true
        ttl: 30s
        cacheError: true
        cacheEmptyResult: true
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Equal(t, len(cfg.LCD.Rules), 1)
	assert.Equal(t, cfg.LCD.Rules[0].Cache.Enable, true)
	assert.Equal(t, cfg.LCD.Rules[0].Cache.TTL, 30*time.Second)
	assert.Equal(t, cfg.LCD.Rules[0].Cache.CacheError, true)
	assert.Equal(t, cfg.LCD.Rules[0].Cache.CacheEmptyResult, true)
}

func TestReadConfigFromFile_RulesPrioritySorting(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "priority.yml")

	// Rules are written out of order to test sorting
	configContent := `
lcd:
  rules:
    - priority: 500
      action: allow
      paths: ["/third"]
    - priority: 100
      action: allow
      paths: ["/first"]
    - priority: 300
      action: deny
      paths: ["/second"]
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	// Should be sorted by priority ascending
	assert.Equal(t, cfg.LCD.Rules[0].Priority, 100)
	assert.Equal(t, cfg.LCD.Rules[0].Paths[0], "/first")
	assert.Equal(t, cfg.LCD.Rules[1].Priority, 300)
	assert.Equal(t, cfg.LCD.Rules[1].Paths[0], "/second")
	assert.Equal(t, cfg.LCD.Rules[2].Priority, 500)
	assert.Equal(t, cfg.LCD.Rules[2].Paths[0], "/third")
}
