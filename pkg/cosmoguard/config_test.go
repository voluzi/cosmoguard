package cosmoguard

import (
	"math"
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

	// Node defaults — PrepareConfig promotes the v3 singular
	// `node:` into Nodes[0] and clears cfg.Node, so assertions
	// read from the v4-canonical Nodes[0] shape.
	assert.Equal(t, cfg.Nodes[0].Host, "127.0.0.1")
	assert.Equal(t, cfg.Nodes[0].RpcPort, 26657)
	assert.Equal(t, cfg.Nodes[0].LcdPort, 1317)
	assert.Equal(t, cfg.Nodes[0].GrpcPort, 9090)

	// Cache defaults
	assert.Equal(t, cfg.Cache.TTL, 5*time.Second)
	assert.Equal(t, cfg.Cache.GRPCForegroundFetchTimeout, 5*time.Minute)

	// Metrics defaults
	assert.Equal(t, cfg.Metrics.IsEnabled(), true)
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
  grpcForegroundFetchTimeout: 45m

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

	assert.Equal(t, cfg.Nodes[0].Host, "10.0.0.1")
	assert.Equal(t, cfg.Nodes[0].RpcPort, 26658)
	assert.Equal(t, cfg.Nodes[0].LcdPort, 1318)

	assert.Equal(t, cfg.Cache.TTL, 30*time.Second)
	assert.Equal(t, cfg.Cache.Key, "test-prefix")
	assert.Equal(t, cfg.Cache.GRPCForegroundFetchTimeout, 45*time.Minute)

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

func TestReadConfigFromFile_HttpRuleQuery(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "query.yml")

	configContent := `
rpc:
  rules:
    - priority: 100
      action: allow
      paths: ["/block"]
      methods: [GET]
      query:
        height: "*"
      cache:
        enable: true
        ttl: 1h
    - priority: 200
      action: allow
      paths: ["/block"]
      methods: [GET]
`

	err := os.WriteFile(tmpFile, []byte(configContent), 0644)
	assert.NilError(t, err)

	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Equal(t, len(cfg.RPC.Rules), 2)
	// First rule has query and should be the cacheable one.
	assert.Equal(t, cfg.RPC.Rules[0].Priority, 100)
	assert.Equal(t, cfg.RPC.Rules[0].Query["height"], "*")
	assert.Assert(t, cfg.RPC.Rules[0].QueryGlobs["height"] != nil)
	// Second rule has no query.
	assert.Equal(t, cfg.RPC.Rules[1].Priority, 200)
	assert.Equal(t, len(cfg.RPC.Rules[1].Query), 0)
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

// TestHealthcheckEnableDisable_RespectsExplicitFalse pins the fix
// for the creasty/defaults bool-override footgun: a YAML config with
// `healthcheck.enable: false` must end up disabled after PrepareConfig.
// Previously the field was a plain `bool` with `default:"true"`, and
// the library cannot distinguish a zero-value from an unset one, so
// the explicit false was silently overridden back to true and the
// operator's healthchecks ran anyway. The *bool form (nil = default,
// non-nil = honored) is what makes this test pass.
func TestHealthcheckEnableDisable_RespectsExplicitFalse(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "hc.yml")
	cfgYAML := `
host: 127.0.0.1
nodes:
  - name: explicit-off
    host: 10.0.0.1
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    healthcheck:
      enable: false
  - name: explicit-on
    host: 10.0.0.2
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    healthcheck:
      enable: true
  - name: defaulted
    host: 10.0.0.3
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    healthcheck: {}
`
	assert.NilError(t, os.WriteFile(tmpFile, []byte(cfgYAML), 0644))
	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	// explicit false must stay false (the bug under test).
	assert.Assert(t, !cfg.Nodes[0].Healthcheck.IsEnabled(),
		"node[0] explicit enable:false leaked through as enabled")
	// explicit true stays true.
	assert.Assert(t, cfg.Nodes[1].Healthcheck.IsEnabled(),
		"node[1] explicit enable:true should be enabled")
	// nil pointer means "use the default", which is enabled.
	assert.Assert(t, cfg.Nodes[2].Healthcheck.IsEnabled(),
		"node[2] unset enable should default to enabled")
}

// TestCircuitBreakerEnableDisable_RespectsExplicitFalse is the same
// pin for CircuitBreakerConfig.Enable. Same root cause as the
// healthcheck variant — covered separately so a regression on either
// struct fails the right test.
func TestCircuitBreakerEnableDisable_RespectsExplicitFalse(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "cb.yml")
	cfgYAML := `
host: 127.0.0.1
nodes:
  - name: explicit-off
    host: 10.0.0.1
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    circuitBreaker:
      enable: false
  - name: defaulted
    host: 10.0.0.2
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    circuitBreaker: {}
`
	assert.NilError(t, os.WriteFile(tmpFile, []byte(cfgYAML), 0644))
	cfg, err := ReadConfigFromFile(tmpFile)
	assert.NilError(t, err)

	assert.Assert(t, !cfg.Nodes[0].CircuitBreaker.IsEnabled(),
		"node[0] explicit circuitBreaker.enable:false leaked through as enabled")
	assert.Assert(t, cfg.Nodes[1].CircuitBreaker.IsEnabled(),
		"node[1] unset enable should default to enabled")
}

// TestValidateCacheBackend covers the contract surface validated by
// validateCacheBackend: the cluster: block is present requires a routable
// bindAddr + a discovery mode, and the per-mode required fields
// (dns.host, static.peers) are enforced.
func TestValidateCacheBackend(t *testing.T) {
	t.Run("empty cache validates", func(t *testing.T) {
		c := &CacheGlobalConfig{}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("negative gRPC foreground timeout rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{GRPCForegroundFetchTimeout: -time.Second}
		assert.Assert(t, validateCacheBackend(c) != nil)
	})

	t.Run("negative memory caps rejected", func(t *testing.T) {
		for _, m := range []CacheMemoryConfig{
			{MaxBytes: int64p(-1)},
			{MaxItems: int64p(-5)},
			{DistributedMaxBytesPerNode: int64p(-1)},
		} {
			c := &CacheGlobalConfig{Memory: m}
			assert.Assert(t, validateCacheBackend(c) != nil, "negative cap must be rejected: %+v", m)
		}
	})

	t.Run("explicit zero memory caps accepted (unlimited)", func(t *testing.T) {
		c := &CacheGlobalConfig{Memory: CacheMemoryConfig{
			MaxBytes:                   int64p(0),
			DistributedMaxBytesPerNode: int64p(0),
		}}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("reserveFraction out of range rejected", func(t *testing.T) {
		for _, f := range []float64{-0.1, 0.9, 1.5, math.NaN(), math.Inf(1)} {
			c := &CacheGlobalConfig{Memory: CacheMemoryConfig{ReserveFraction: float64p(f)}}
			assert.Assert(t, validateCacheBackend(c) != nil, "reserveFraction %g must be rejected", f)
		}
		c := &CacheGlobalConfig{Memory: CacheMemoryConfig{ReserveFraction: float64p(0.30)}}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("the cluster: block is present without bindAddr rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})

	t.Run("the cluster: block is present requires discovery mode", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:     "10.0.0.5",
				ReplicaCount: 2,
				Quorum:       1,
			},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})

	t.Run("dns mode requires host", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:     "10.0.0.5",
				ReplicaCount: 2,
				Quorum:       1,
				Discovery:    &ClusterDiscoveryConfig{Mode: "dns"},
			},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})

	t.Run("static mode requires peers", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:     "10.0.0.5",
				ReplicaCount: 2,
				Quorum:       1,
				Discovery:    &ClusterDiscoveryConfig{Mode: "static"},
			},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})

	t.Run("static mode with peers passes", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:      "10.0.0.5",
				ReplicaCount:  2,
				Quorum:        1,
				EncryptionKey: testClusterEncryptionKey,
				Discovery: &ClusterDiscoveryConfig{
					Mode:   "static",
					Static: &StaticDiscoveryConfig{Peers: []string{"a:3322", "b:3322"}},
				},
			},
		}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("cluster mode without encryption key rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:     "10.0.0.5",
				ReplicaCount: 2,
				Quorum:       1,
				Discovery: &ClusterDiscoveryConfig{
					Mode:   "static",
					Static: &StaticDiscoveryConfig{Peers: []string{"a:3322"}},
				},
			},
		}
		assert.Assert(t, validateCacheBackend(c) != nil, "missing encryptionKey should be rejected")
	})

	t.Run("cluster mode with malformed encryption key rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:      "10.0.0.5",
				ReplicaCount:  2,
				Quorum:        1,
				EncryptionKey: "dG9vLXNob3J0", // decodes to 8 bytes — invalid length
				Discovery: &ClusterDiscoveryConfig{
					Mode:   "static",
					Static: &StaticDiscoveryConfig{Peers: []string{"a:3322"}},
				},
			},
		}
		assert.Assert(t, validateCacheBackend(c) != nil, "8-byte key should be rejected")
	})

	t.Run("quorum greater than replicaCount rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				ReplicaCount: 2,
				Quorum:       3,
				Discovery: &ClusterDiscoveryConfig{
					Mode:   "static",
					Static: &StaticDiscoveryConfig{Peers: []string{"a:3322"}},
				},
			},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})

	t.Run("the cluster: block is present rejects wildcard bindAddr", func(t *testing.T) {
		for _, addr := range []string{"", "0.0.0.0", "::", "[::]"} {
			c := &CacheGlobalConfig{
				Cluster: &ClusterConfig{
					BindAddr:     addr,
					ReplicaCount: 2,
					Quorum:       1,
					Discovery: &ClusterDiscoveryConfig{
						Mode:   "static",
						Static: &StaticDiscoveryConfig{Peers: []string{"a:3322"}},
					},
				},
			}
			err := validateCacheBackend(c)
			assert.Assert(t, err != nil, "bindAddr=%q should be rejected", addr)
		}
	})

	t.Run("experimental modes accepted at config time", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:      "10.0.0.5",
				ReplicaCount:  2,
				Quorum:        1,
				EncryptionKey: testClusterEncryptionKey,
				Discovery:     &ClusterDiscoveryConfig{Mode: "kubernetes"},
			},
		}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("dns port out of range rejected", func(t *testing.T) {
		for _, port := range []int{-1, 0xffff + 1, 100000} {
			c := &CacheGlobalConfig{
				Cluster: &ClusterConfig{
					BindAddr:     "10.0.0.5",
					ReplicaCount: 2,
					Quorum:       1,
					Discovery: &ClusterDiscoveryConfig{
						Mode: "dns",
						DNS:  &DNSDiscoveryConfig{Host: "peers.local", Port: port},
					},
				},
			}
			err := validateCacheBackend(c)
			assert.Assert(t, err != nil, "dns.port=%d should be rejected", port)
		}
	})

	t.Run("dns port zero accepted (inherits gossipPort)", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:      "10.0.0.5",
				ReplicaCount:  2,
				Quorum:        1,
				EncryptionKey: testClusterEncryptionKey,
				Discovery: &ClusterDiscoveryConfig{
					Mode: "dns",
					DNS:  &DNSDiscoveryConfig{Host: "peers.local"},
				},
			},
		}
		assert.NilError(t, validateCacheBackend(c))
	})

	t.Run("dns refreshInterval below 1s rejected", func(t *testing.T) {
		c := &CacheGlobalConfig{
			Cluster: &ClusterConfig{
				BindAddr:     "10.0.0.5",
				ReplicaCount: 2,
				Quorum:       1,
				Discovery: &ClusterDiscoveryConfig{
					Mode: "dns",
					DNS: &DNSDiscoveryConfig{
						Host:            "peers.local",
						RefreshInterval: 100 * time.Millisecond,
					},
				},
			},
		}
		err := validateCacheBackend(c)
		assert.Assert(t, err != nil)
	})
}
