package cosmoguard

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

const (
	cacheHit  = "hit"
	cacheMiss = "miss"
	// cacheError is emitted when the cache backend itself failed (Redis
	// timeout, connection refused, etc.) — distinct from a clean
	// "key not found" miss. Without this, a Redis outage looks
	// identical to cold-cache traffic and hit-rate alerts never fire.
	cacheError = "error"
)

type Config struct {
	Host         string            `yaml:"host,omitempty" default:"0.0.0.0"`
	RpcPort      int               `yaml:"rpcPort,omitempty" default:"16657"`
	LcdPort      int               `yaml:"lcdPort,omitempty" default:"11317"`
	GrpcPort     int               `yaml:"grpcPort,omitempty" default:"19090"`
	EnableEvm    bool              `yaml:"enableEvm,omitempty" default:"false"`
	EvmRpcPort   int               `yaml:"evmRpcPort,omitempty" default:"18545"`
	EvmRpcWsPort int               `yaml:"evmRpcWsPort,omitempty" default:"18546"`
	Node         NodeConfig        `yaml:"node,omitempty"`  // v3 singular — kept for compat
	Nodes        []NodeConfig      `yaml:"nodes,omitempty"` // v4 list (preferred)
	Upstream     UpstreamConfig    `yaml:"upstream,omitempty"`
	Cache        CacheGlobalConfig `yaml:"cache,omitempty"`
	Server       ServerConfig      `yaml:"server,omitempty"`
	Auth         AuthConfig        `yaml:"auth,omitempty"`
	CORS         CORSConfig        `yaml:"cors,omitempty"`
	LCD          LcdConfig         `yaml:"lcd,omitempty"`
	RPC          RpcConfig         `yaml:"rpc,omitempty"`
	GRPC         GrpcConfig        `yaml:"grpc,omitempty"`
	EVM          EvmConfig         `yaml:"evm,omitempty"`
	Metrics      MetricsConfig     `yaml:"metrics,omitempty"`
	Dashboard    DashboardConfig   `yaml:"dashboard,omitempty"`
	Tracing      TracingConfig     `yaml:"tracing,omitempty"`
}

// ServerConfig holds HTTP/WS server hardening knobs that apply to every HTTP
// proxy cosmoguard exposes (LCD, RPC, EVM RPC, EVM RPC WS). Defaults are
// chosen to be safely above what a Cosmos node tolerates for its own clients,
// so cosmoguard never becomes the bottleneck for legitimate traffic.
type ServerConfig struct {
	// ReadHeaderTimeout caps how long a client may take to send the HTTP
	// request header. Mitigates slowloris-style attacks.
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout,omitempty" default:"10s"`
	// ReadTimeout caps the total time to read the entire request (headers
	// + body). Set higher than ReadHeaderTimeout to allow legitimate large
	// tx broadcasts. 0 means no limit.
	ReadTimeout time.Duration `yaml:"readTimeout,omitempty" default:"30s"`
	// WriteTimeout caps how long the server may take to write a response.
	// Default 0 = NO LIMIT, matching v3 behaviour: blockchain endpoints can
	// legitimately stream very large / slow responses (/block_results,
	// /genesis, big eth_getLogs) that a fixed 30s deadline would truncate
	// mid-body with no error to the client. Operators exposing cosmoguard to
	// untrusted clients should set an explicit ceiling.
	WriteTimeout time.Duration `yaml:"writeTimeout,omitempty"`
	// IdleTimeout caps how long a keep-alive connection sits idle before the
	// server closes it.
	IdleTimeout time.Duration `yaml:"idleTimeout,omitempty" default:"60s"`
	// MaxRequestBody bounds the request body cosmoguard will read into memory.
	// Requests exceeding this return 413 Request Entity Too Large. Read via
	// EffectiveMaxRequestBody(): nil (unset) → 5 MiB default (accommodates a
	// wasm MsgStoreCode broadcast; the rc.1 default of 1 MiB rejected those),
	// explicit 0 → no limit. It is a *int64 (not a plain int64 with a default
	// tag) so an explicit `maxRequestBody: 0` survives defaults.Set instead of
	// being silently reset to the default.
	MaxRequestBody *int64 `yaml:"maxRequestBody,omitempty"`
	// WSReadLimit bounds individual WebSocket frames cosmoguard will read.
	// Read via EffectiveWSReadLimit(): nil (unset) → 1 MiB default (generous
	// for any legitimate Cosmos/EVM frame incl. a large eth_sendRawTransaction;
	// the rc.1 default of 64 KiB closed bigger frames), explicit 0 → no limit.
	// *int64 for the same explicit-zero-survives-defaults reason as above.
	WSReadLimit *int64 `yaml:"wsReadLimit,omitempty"`
	// WSAllowedOrigins is the allowlist applied to the Origin header on
	// incoming WebSocket upgrade requests. Same-origin requests (Origin host
	// matches Host header) are always permitted. Requests with no Origin
	// header (non-browser clients) are always permitted. For cross-origin
	// browser clients, the Origin must match at least one entry in this
	// list. Entries support glob patterns ("https://*.example.com").
	//
	// An empty list (the v4 default) denies all cross-origin WS upgrades —
	// a deliberate breaking change from v3. Use ["*"] to restore the v3
	// behavior of accepting any Origin.
	WSAllowedOrigins []string `yaml:"wsAllowedOrigins,omitempty"`

	// TrustedProxies is the CIDR allowlist of immediate upstream peers
	// whose X-Real-Ip / X-Forwarded-For headers cosmoguard will honor
	// for per-IP rate limiting, audit logging, and the dashboard's
	// SourceIP rule predicates. When empty (the default), every
	// proxy-supplied header is ignored and the source IP comes from
	// r.RemoteAddr — secure-by-default: a misconfigured or naked
	// deployment can't be tricked into rate-limiting the wrong
	// client by anyone who simply sends `X-Real-Ip: ...`.
	//
	// Set this to the CIDR(s) of your front-end LB / ingress (e.g.
	// ["10.0.0.0/8", "127.0.0.1/32"]) once you've verified that LB
	// rewrites the forwarded headers. After that, X-Real-Ip /
	// X-Forwarded-For are honored ONLY when the immediate connection
	// peer matches one of the allowed CIDRs.
	TrustedProxies []string `yaml:"trustedProxies,omitempty"`
}

const (
	defaultMaxRequestBody int64 = 5 << 20 // 5 MiB
	defaultServerWSRead   int64 = 1 << 20 // 1 MiB
)

// EffectiveMaxRequestBody resolves the request-body cap: nil → the 5 MiB
// default, explicit value (including 0 = no limit) → that value.
func (s *ServerConfig) EffectiveMaxRequestBody() int64 {
	if s.MaxRequestBody == nil {
		return defaultMaxRequestBody
	}
	return *s.MaxRequestBody
}

// EffectiveWSReadLimit resolves the WS frame cap: nil → the 1 MiB default,
// explicit value (including 0 = no limit) → that value.
func (s *ServerConfig) EffectiveWSReadLimit() int64 {
	if s.WSReadLimit == nil {
		return defaultServerWSRead
	}
	return *s.WSReadLimit
}

type NodeConfig struct {
	// Name is the human-readable identifier used in metrics / logs.
	// Optional — defaults to "node-<index>" when omitted.
	Name string `yaml:"name,omitempty"`

	Host         string `yaml:"host,omitempty" default:"127.0.0.1"`
	RpcPort      int    `yaml:"rpcPort,omitempty" default:"26657"`
	LcdPort      int    `yaml:"lcdPort,omitempty" default:"1317"`
	GrpcPort     int    `yaml:"grpcPort,omitempty" default:"9090"`
	EvmRpcPort   int    `yaml:"evmRpcPort,omitempty" default:"8545"`
	EvmRpcWsPort int    `yaml:"evmRpcWsPort,omitempty" default:"8546"`

	// TLS, when true, switches the default upstream scheme from
	// http/ws to https/wss and dials gRPC with system-trust TLS
	// credentials. Per-service URL overrides (RpcURL, LcdURL, …)
	// take precedence over this flag.
	TLS bool `yaml:"tls,omitempty"`

	// Per-service URL overrides. When set, the full URL (scheme +
	// host + optional port) replaces the constructed
	// scheme://host:port for that service — useful when each
	// upstream service is fronted by its own hostname on the edge
	// (e.g. https://lcd.example.com, https://rpc.example.com on
	// port 443).
	//
	// Schemes:
	//   LCD, RPC, EVM-RPC: http or https
	//   EVM-RPC-WS:        ws, wss, http (→ws) or https (→wss)
	//   gRPC:              grpc/http (plaintext) or grpcs/https/tls
	RpcURL      string `yaml:"rpcURL,omitempty"`
	LcdURL      string `yaml:"lcdURL,omitempty"`
	GrpcURL     string `yaml:"grpcURL,omitempty"`
	EvmRpcURL   string `yaml:"evmRpcURL,omitempty"`
	EvmRpcWsURL string `yaml:"evmRpcWsURL,omitempty"`

	// Weight is the relative share of traffic this node gets under
	// weighted round-robin. Defaults to 1 (equal share). Reserved for
	// the picker upgrade in the next E slice.
	Weight int `yaml:"weight,omitempty" default:"1"`

	// Healthcheck, when set, configures active liveness probing for this
	// upstream. Probes run in a per-upstream goroutine; an upstream
	// removed from the picker stops receiving new requests but in-flight
	// requests are not interrupted.
	Healthcheck *NodeHealthcheckConfig `yaml:"healthcheck,omitempty"`

	// CircuitBreaker, when set, observes the success/failure outcome of
	// actual proxied requests. After ConsecutiveFailures back-to-back
	// failures, the upstream is removed from the picker for CooldownPeriod;
	// after that, one request is allowed through to test recovery
	// (half-open). Healthchecks and circuit breakers are complementary —
	// healthchecks catch "upstream is down", circuit breakers catch
	// "upstream is responding but failing requests".
	CircuitBreaker *CircuitBreakerConfig `yaml:"circuitBreaker,omitempty"`

	// Discovery, when set, makes this entry a TEMPLATE: instead of one
	// concrete upstream, every IP returned by resolving Discovery.Host
	// becomes a synthetic NodeConfig with this template's ports +
	// TLS + healthcheck + circuit-breaker settings. Used to follow a
	// Kubernetes headless Service: each pod IP behind the headless
	// service becomes its own first-class upstream in cosmoguard's
	// LCD/RPC/gRPC pools, so per-upstream load balancing, circuit
	// breaking, and least-conn picking apply per pod rather than
	// being short-circuited by a single Service VIP.
	//
	// `host` and the per-service URL overrides MUST be empty when
	// Discovery is set — the discovered IP becomes the upstream's
	// host, and overrides wouldn't make sense per-pod.
	Discovery *DiscoveryConfig `yaml:"discovery,omitempty"`
}

// DiscoveryConfig describes how a NodeConfig template expands into
// many concrete upstreams via DNS resolution. Today only `type: dns`
// is supported — it issues an A/AAAA lookup against the configured
// host every RefreshInterval and reconciles the resulting IP set
// against each pool. New pods appear as new upstreams; pods that
// disappear from DNS are removed from each pool (and their
// healthcheck goroutines stopped).
//
// DNS is what Kubernetes headless Services already expose, so this
// works out-of-the-box for clusterDNS topology — no API access
// required and no in-cluster RBAC needed.
type DiscoveryConfig struct {
	// Type selects the discovery mechanism. "" or "dns" mean DNS.
	// Reserved for future expansion (e.g. "kubernetes-endpoints" via
	// the K8s API directly, which would react instantly to pod
	// changes instead of waiting for the DNS TTL).
	Type string `yaml:"type,omitempty" default:"dns"`
	// Host is the DNS name to resolve. For a Kubernetes headless
	// Service this is typically
	// `<svc-name>.<namespace>.svc.cluster.local` (or any name your
	// cluster's DNS routes through endpoints).
	Host string `yaml:"host,omitempty"`
	// RefreshInterval governs how often discovery re-resolves the
	// host. Short intervals react faster to pod churn at the cost of
	// more DNS lookups. The default (15s) matches a typical
	// kube-dns TTL — finer-grained polling buys nothing when the
	// underlying TTL caps freshness anyway.
	RefreshInterval time.Duration `yaml:"refreshInterval,omitempty" default:"15s"`
}

// CircuitBreakerConfig configures consecutive-failure circuit breaking for
// a single upstream. Failures = upstream returned 5xx OR the transport
// itself failed (connection refused, timeout).
type CircuitBreakerConfig struct {
	// Enable is a *bool so YAML `enable: false` survives defaults.Set.
	// creasty/defaults can't distinguish a zero-value `bool` from an
	// unset one, so `default:"true"` on a plain bool would silently
	// re-enable a config the operator explicitly disabled. nil means
	// "use the default (enabled)"; an explicit pointer to false stays
	// false. Read through CircuitBreakerEnabled() so callers don't
	// have to nil-check.
	Enable *bool `yaml:"enable,omitempty"`
	// ConsecutiveFailures is the number of back-to-back failures that
	// trip the breaker. A single success resets the counter.
	ConsecutiveFailures int `yaml:"consecutiveFailures,omitempty" default:"5"`
	// CooldownPeriod is how long the circuit stays open before allowing
	// a half-open probe.
	CooldownPeriod time.Duration `yaml:"cooldownPeriod,omitempty" default:"30s"`
}

// UpstreamConfig configures pool-level behavior shared across nodes.
type UpstreamConfig struct {
	// Strategy is the picker algorithm:
	//   weighted-round-robin  — default; respects node.weight
	//   round-robin           — ignores weight, equal share
	//   least-conn            — pick the upstream with the fewest
	//                           in-flight requests; ties broken
	//                           round-robin
	//   primary-failover     — always pick the lowest-priority node
	//                          unless it's unhealthy
	Strategy string `yaml:"strategy,omitempty" default:"weighted-round-robin"`

	// Retries configures connect-failure retries onto the next upstream.
	// Applied only to idempotent methods (GET, HEAD, OPTIONS — and
	// idempotent JSON-RPC methods). Defaults: max=2 retries.
	Retries RetryConfig `yaml:"retries,omitempty"`
}

// RetryConfig — failover semantics for transient failures.
type RetryConfig struct {
	Max int `yaml:"max,omitempty" default:"2"`
}

// NodeHealthcheckConfig configures the active health probe for a single
// upstream. Cosmos nodes expose /status (Tendermint RPC) and /node_info
// (LCD); the default is /status with HTTP GET.
type NodeHealthcheckConfig struct {
	// Enable is a *bool so YAML `enable: false` survives defaults.Set.
	// See CircuitBreakerConfig.Enable for the full rationale — same
	// creasty/defaults limitation, same workaround. Read through
	// HealthcheckEnabled() so callers don't have to nil-check.
	Enable *bool `yaml:"enable,omitempty"`
	// Path is the URL path GET'd by the probe. Defaults to /status.
	Path string `yaml:"path,omitempty" default:"/status"`
	// Service selects which upstream port to probe: "rpc" (default),
	// "lcd", or "grpc". gRPC support is reserved for Phase F.
	Service string `yaml:"service,omitempty" default:"rpc"`
	// Interval between probes.
	Interval time.Duration `yaml:"interval,omitempty" default:"5s"`
	// Timeout per probe.
	Timeout time.Duration `yaml:"timeout,omitempty" default:"2s"`
	// UnhealthyAfter is the number of consecutive failed probes before
	// the upstream is removed from the picker.
	UnhealthyAfter int `yaml:"unhealthyAfter,omitempty" default:"3"`
	// HealthyAfter is the number of consecutive successful probes
	// before a previously-unhealthy upstream is added back to the picker.
	HealthyAfter int `yaml:"healthyAfter,omitempty" default:"2"`
}

// IsEnabled reports whether the healthcheck should run. nil means
// "use the default (enabled)"; an explicit `enable: false` in YAML
// stays false. Safe to call on a nil receiver — returns false so
// `n.Healthcheck.IsEnabled()` works without a separate nil check.
func (h *NodeHealthcheckConfig) IsEnabled() bool {
	if h == nil {
		return false
	}
	return h.Enable == nil || *h.Enable
}

// IsEnabled reports whether the circuit breaker is active. Same
// nil-default-enabled idiom as NodeHealthcheckConfig.IsEnabled.
func (c *CircuitBreakerConfig) IsEnabled() bool {
	if c == nil {
		return false
	}
	return c.Enable == nil || *c.Enable
}

type CacheGlobalConfig struct {
	// Cache + rate-limiter backend: olric (L2) fronted by an
	// in-process L1 (see pkg/cache/tiered.go).
	TTL time.Duration `yaml:"ttl,omitempty" default:"5s"`
	Key string        `yaml:"key"`
	// Cluster controls the embedded olric daemon. When nil, the daemon
	// runs in embedded-only mode on loopback ephemeral ports (no external
	// surface, no peers, no replication). When set with Enable: true, the
	// daemon binds the configured BindAddr:BindPort and joins peers
	// advertised by the configured Discovery mode.
	Cluster *ClusterConfig `yaml:"cluster,omitempty"`
}

// ClusterConfig configures the olric daemon's networking and replication
// behaviour. The presence of this struct in CacheGlobalConfig (i.e.
// the operator wrote a `cluster:` block) IS the cluster-mode toggle.
// Omit the block to run the daemon in embedded-only mode (loopback,
// ephemeral ports, no gossip) — the default for single-pod installs
// and the no-config test path.
type ClusterConfig struct {
	// BindAddr is the interface olric binds its inter-node RESP socket
	// to AND the advertise address that becomes this pod's unique member
	// Name in the cluster. MUST be a routable per-pod address: wildcard
	// values (empty, 0.0.0.0, ::) are rejected because every pod would
	// otherwise share the same Name and collide on memberlist join. In
	// Kubernetes wire this from the downward API (e.g. "${POD_IP}");
	// on bare metal use the host's primary IP.
	BindAddr string `yaml:"bindAddr,omitempty"`

	// BindPort is the TCP port for olric's internal RESP socket
	// (node-to-node data replication, rebalancing, scan). Matches olric's
	// upstream default. Must not collide with other cosmoguard listeners.
	BindPort int `yaml:"bindPort,omitempty" default:"3320"`

	// GossipPort is the memberlist port (TCP + UDP on the same port).
	// Operators frequently block UDP at the firewall by reflex; both
	// protocols on this port must be open between pods. Default 3322,
	// matching olric's upstream default. Documented in CONFIG.md.
	GossipPort int `yaml:"gossipPort,omitempty" default:"3322"`

	// PeerApiPort is the HTTP port the dashboard fan-out aggregator
	// listens on. 0 means "BindPort + 1" — chosen so a single explicit
	// BindPort sets up all three ports predictably. Restricted to the
	// memberlist member set at the network layer; never internet-facing.
	PeerApiPort int `yaml:"peerApiPort,omitempty"`

	// ReplicaCount is the olric replication factor. Default 2 (one
	// primary + one replica). 3 is the textbook split-brain-proof
	// configuration; 2 is the cost-sensitive choice with documented
	// trade-offs in CONFIG.md.
	ReplicaCount int `yaml:"replicaCount,omitempty" default:"2"`

	// Quorum is the minimum number of live members required for writes
	// to succeed. Default 1 (any single live member can accept writes).
	// Set to 2 when ReplicaCount=3 to get strict quorum semantics.
	Quorum int `yaml:"quorum,omitempty" default:"1"`

	// Discovery selects how peer addresses are obtained. Required when
	// Enable is true — validation rejects unset Discovery with a clear
	// error rather than silently falling back to a default that would
	// pick the wrong mode in some environments.
	Discovery *ClusterDiscoveryConfig `yaml:"discovery,omitempty"`

	// EncryptionKey is a base64-encoded 16, 24, or 32-byte key that
	// enables memberlist gossip encryption + authentication (AES-128/192/
	// 256-GCM). REQUIRED in cluster mode: without it the gossip and RESP
	// data ports carry no encryption and no peer authentication, so any
	// host that can reach them can join the cluster and read/write the
	// shared DMaps — rate-limit buckets, cached responses, and the JWT
	// replay set. Generate one with e.g. `head -c32 /dev/urandom | base64`
	// and give every pod in the cluster the SAME value (wire it from a
	// Kubernetes Secret / env var). Node discovery must still restrict the
	// peer ports at the network layer (NetworkPolicy) — this is
	// defence-in-depth, not a substitute.
	EncryptionKey string `yaml:"encryptionKey,omitempty"`
}

// ClusterDiscoveryConfig selects and configures the peer-discovery mode.
type ClusterDiscoveryConfig struct {
	// Mode is one of "dns", "static", "kubernetes", "mdns". Required —
	// startup fails fast if unset when the cluster: block is present.
	Mode string `yaml:"mode,omitempty"`

	// DNS — recommended for Kubernetes (headless service) and any
	// environment with a service registry that fronts pods via DNS.
	DNS *DNSDiscoveryConfig `yaml:"dns,omitempty"`

	// Static — explicit peer list. Useful for fixed 2/3-node clusters
	// outside Kubernetes (e.g. VMs).
	Static *StaticDiscoveryConfig `yaml:"static,omitempty"`

	// K8s — direct Kubernetes API access. Experimental; use DNS on
	// Kubernetes unless this is promoted.
	K8s *K8sDiscoveryConfig `yaml:"kubernetes,omitempty"`

	// MDNS — multicast DNS for LAN auto-discovery. Experimental.
	MDNS *MDNSDiscoveryConfig `yaml:"mdns,omitempty"`
}

// DNSDiscoveryConfig configures DNS-based peer discovery. Issues A/AAAA
// lookups against Host every RefreshInterval; the returned IPs are joined
// with cluster.gossipPort to form peer addresses for memberlist.
type DNSDiscoveryConfig struct {
	// Host is the DNS name to resolve. For a Kubernetes headless service
	// this is typically `<svc>.<ns>.svc.cluster.local`.
	Host string `yaml:"host,omitempty"`
	// Port to use when forming the peer address. 0 → cluster.GossipPort.
	// Operators rarely need to override this; included for the rare
	// case where the gossip port differs across replicas.
	Port int `yaml:"port,omitempty"`
	// RefreshInterval governs how often DNS is re-queried. Cluster
	// joins react fast enough at 10s; shorter intervals waste lookups.
	RefreshInterval time.Duration `yaml:"refreshInterval,omitempty" default:"10s"`
}

// StaticDiscoveryConfig provides a fixed peer list.
type StaticDiscoveryConfig struct {
	// Peers is the list of host:port pairs (or bare hosts; in that case
	// cluster.gossipPort is appended). Self-references are filtered out
	// at runtime so the same config can be deployed to every pod.
	Peers []string `yaml:"peers,omitempty"`
}

// K8sDiscoveryConfig — Kubernetes API discovery (experimental).
type K8sDiscoveryConfig struct {
	Namespace     string `yaml:"namespace,omitempty"`
	LabelSelector string `yaml:"labelSelector,omitempty"`
}

// MDNSDiscoveryConfig — multicast DNS (experimental).
type MDNSDiscoveryConfig struct {
	// ServiceTag is the mDNS service identifier the daemon registers
	// and queries. Defaults to "cosmoguard" so every cosmoguard on the
	// LAN finds every other automatically.
	ServiceTag string `yaml:"serviceTag,omitempty" default:"cosmoguard"`
}

type RuleCache struct {
	Enable           bool          `yaml:"enable,omitempty"`
	TTL              time.Duration `yaml:"ttl,omitempty"`
	CacheError       bool          `yaml:"cacheError,omitempty"`
	CacheEmptyResult bool          `yaml:"cacheEmptyResult,omitempty"`
	// PreserveHeaders is the list of upstream response header names that
	// should be replayed verbatim on a cache hit, in addition to the
	// always-preserved set (Content-Type, Content-Encoding, Cache-Control,
	// ETag, Vary). Header names are matched case-insensitively. Hop-by-hop
	// headers cannot be added to this list — they are always stripped.
	PreserveHeaders []string `yaml:"preserveHeaders,omitempty"`

	// KeyMode controls how the cache key is computed for gRPC rules.
	// "raw" (default): hash the request payload bytes verbatim — matches
	// only when two clients serialize their protobuf identically.
	// "method-only": ignore the payload entirely; one cache entry per
	// gRPC method per rule. Useful for parameter-less queries
	// (cosmos.bank.v1beta1.Query/Params, /node_info, etc.) where
	// every caller would get the same answer anyway. Misuse on a
	// query that DOES take parameters will silently return the
	// first response to every caller — operators must scope rules
	// carefully when they enable method-only.
	//
	// The HTTP cache always hashes the full request (path + query +
	// body); KeyMode is honored on gRPC only.
	KeyMode string `yaml:"keyMode,omitempty"`

	// KeyMetadata lists inbound gRPC metadata keys whose values are folded
	// into the cache key, so requests that differ only by such metadata are
	// cached separately. This matters because response-affecting metadata
	// (notably `x-cosmos-block-height`, which selects the state height a
	// Cosmos query runs against) IS forwarded upstream and changes the
	// response — but was previously absent from the key, so a query at one
	// height served another height's cached response for the whole TTL.
	//
	// A nil (unset) list defaults to the response-affecting keys in
	// defaultGrpcCacheKeyMetadata (currently x-cosmos-block-height). Set an
	// explicit empty list ([]) to opt out entirely (e.g. a method-only rule
	// on a genuinely height-independent query). Keys are matched
	// lowercase, as gRPC canonicalizes metadata keys to lowercase.
	KeyMetadata []string `yaml:"keyMetadata,omitempty"`
}

// defaultGrpcCacheKeyMetadata is the response-affecting gRPC metadata folded
// into cache keys when a rule doesn't configure KeyMetadata explicitly.
var defaultGrpcCacheKeyMetadata = []string{"x-cosmos-block-height"}

// EffectiveKeyMetadata returns the metadata keys to fold into the gRPC cache
// key: the configured list when non-nil (including an explicit empty list,
// which opts out), or the safe default otherwise.
func (c *RuleCache) EffectiveKeyMetadata() []string {
	if c.KeyMetadata != nil {
		return c.KeyMetadata
	}
	return defaultGrpcCacheKeyMetadata
}

type LcdConfig struct {
	Default RuleAction  `yaml:"default,omitempty" default:"deny"`
	Rules   []*HttpRule `yaml:"rules,omitempty"`
}

type RpcConfig struct {
	Default RuleAction    `yaml:"default,omitempty" default:"deny"`
	Rules   []*HttpRule   `yaml:"rules,omitempty"`
	JsonRpc JsonRpcConfig `yaml:"jsonrpc,omitempty"`
	// WebSocketEnabled is *bool so an explicit `webSocketEnabled: false`
	// survives defaults.Set (a plain bool with default:"true" would be
	// silently re-enabled — creasty/defaults can't distinguish false from
	// unset). Read via WebSocketIsEnabled(). nil → default (enabled).
	WebSocketEnabled     *bool `yaml:"webSocketEnabled,omitempty"`
	WebSocketConnections int   `yaml:"webSocketConnections,omitempty" default:"10"`
}

// WebSocketIsEnabled reports whether the JSON-RPC WebSocket proxy is on.
// nil-default-enabled idiom.
func (c *RpcConfig) WebSocketIsEnabled() bool {
	return c.WebSocketEnabled == nil || *c.WebSocketEnabled
}

type JsonRpcConfig struct {
	Default RuleAction     `yaml:"default,omitempty" default:"deny"`
	Rules   []*JsonRpcRule `yaml:"rules,omitempty"`
	// MaxBatchSize caps the number of requests a single JSON-RPC batch may
	// contain. Oversized batches are rejected with HTTP 413 before any rule
	// is evaluated — protects against amplification attacks where one TCP
	// connection fans out into thousands of upstream calls. 0 disables the
	// cap (legacy v3 behavior; not recommended). 100 matches what Cosmos
	// nodes accept by default.
	//
	// Pointer-typed so the operator can distinguish "unset → use the
	// default of 100" from "explicitly disable the cap (set 0)". With a
	// plain int + default tag, creasty/defaults would overwrite an
	// explicit 0 with 100, silently re-enabling the cap.
	MaxBatchSize *int `yaml:"maxBatchSize,omitempty"`
}

type GrpcConfig struct {
	Default RuleAction  `yaml:"default,omitempty" default:"deny"`
	Rules   []*GrpcRule `yaml:"rules,omitempty"`
	// Protosets is the list of binary FileDescriptorSet files
	// (produced by `protoc --descriptor_set_out=foo.protoset`).
	// Loaded at startup and used by gRPC rules with
	// `cache.keyMode: canonical` to decode + re-encode request
	// payloads deterministically before hashing — collapses byte-
	// level differences in protobuf serialization across clients.
	// Empty list disables canonicalization (the keyMode silently
	// degrades to "raw" for methods not in any protoset).
	Protosets []string `yaml:"protosets,omitempty"`
}

type MetricsConfig struct {
	// Enable is *bool (not a plain bool with default:"true") so an explicit
	// `metrics: {enable: false}` survives defaults.Set — creasty/defaults
	// can't tell a zero-value false from unset, so default:"true" on a plain
	// bool silently re-enabled metrics an operator had explicitly disabled.
	// Read via IsEnabled(). nil → default (enabled).
	Enable *bool `yaml:"enable,omitempty"`
	Port   int   `yaml:"port,omitempty" default:"9001"`
	// WebUI controls the /admin/ dashboard mounted on the metrics
	// port. Default is false: cosmoguard's metrics port is commonly
	// exposed inside a cluster (Prometheus scrape), so an
	// unauthenticated dashboard there leaks upstream targets and
	// identity scopes to anyone with cluster access. Operators who
	// want the dashboard must opt in explicitly.
	WebUI WebUIConfig `yaml:"webUI,omitempty"`
}

// IsEnabled reports whether metrics are on. nil-default-enabled idiom, same
// as NodeHealthcheckConfig.IsEnabled.
func (m *MetricsConfig) IsEnabled() bool {
	if m == nil {
		return false
	}
	return m.Enable == nil || *m.Enable
}

// WebUIConfig configures the optional read-only dashboard.
type WebUIConfig struct {
	Enable bool `yaml:"enable,omitempty"`
	// BasicAuthUser / BasicAuthPassword, when both set, gate the
	// dashboard with HTTP Basic. nil/empty disables the gate — only
	// safe when the metrics port is reachable only from a trusted
	// network. Recommended even for in-cluster deployments.
	BasicAuthUser     string `yaml:"basicAuthUser,omitempty"`
	BasicAuthPassword string `yaml:"basicAuthPassword,omitempty"`
}

// DashboardConfig configures the standalone read-only dashboard,
// served on its own port (separate from metrics so cluster operators
// can expose /metrics to Prometheus without leaking dashboard
// content). On by default — cosmopilot-style orchestrators want a
// turnkey UI without explicit opt-in.
//
// Open by default is deliberate: most cosmoguard deployments live
// behind a private network or an ingress that adds its own auth.
// Operators who want belt-and-suspenders gating can set
// BasicAuthUser + BasicAuthPassword.
type DashboardConfig struct {
	// Enable is *bool so an explicit `enable: false` in YAML doesn't
	// get silently overwritten by the default-true via creasty/defaults.
	// Same idiom as NodeHealthcheckConfig / CircuitBreakerConfig.
	Enable *bool `yaml:"enable,omitempty"`
	// Port is the listener port for the dashboard. Chosen high enough
	// to clear the common Cosmos / metrics port range so the default
	// install doesn't conflict with a colocated node or scraping setup.
	// Caveat: 19999 is also Netdata's default — operators colocating
	// cosmoguard with Netdata must override via `dashboard.port` or
	// `COSMOGUARD_DASHBOARD_PORT`. The collision surfaces as a clean
	// startup error from validateListenerPorts (or net.Listen if a
	// non-cosmoguard process holds the port), not as silent
	// confusion.
	Port int `yaml:"port,omitempty" default:"19999"`
	// BasicAuthUser / BasicAuthPassword optionally gate the dashboard
	// with HTTP Basic. nil/empty leaves the dashboard open — fine
	// behind a trusted network or an authenticating ingress, risky on
	// the public internet.
	BasicAuthUser     string `yaml:"basicAuthUser,omitempty"`
	BasicAuthPassword string `yaml:"basicAuthPassword,omitempty"`
	// RequestLog is an optional bounded ring of recent requests
	// rendered as the "Live traffic" dashboard panel. Off by default:
	// turning it on exposes path + identity + source-ip of every
	// captured request through the dashboard, which is a partial
	// audit-log surface — restrict the dashboard with BasicAuth or
	// IP allowlists before enabling.
	RequestLog *RequestLogConfig `yaml:"requestLog,omitempty"`
}

// RequestLogConfig configures the time-windowed live-traffic log.
// Only metadata is captured (method, path, status, latency, cache
// state, rule tag, identity name, source ip, upstream) — never
// headers, never request or response bodies.
type RequestLogConfig struct {
	Enable bool `yaml:"enable,omitempty"`
	// MaxAge is the time window: entries older than now()-maxAge
	// are evicted on the next push. Default 30 minutes.
	MaxAge time.Duration `yaml:"maxAge,omitempty" default:"30m"`
	// MaxEntries is a hard total-entry cap across all sections as
	// the OOM backstop: a 100k-rps burst can't pile up beyond this
	// even within the time window. Default 10000 (≈2 MiB at ~200 B
	// per entry).
	MaxEntries int `yaml:"maxEntries,omitempty" default:"10000"`
	// IncludeSuccess captures 2xx outcomes (default true).
	IncludeSuccess *bool `yaml:"includeSuccess,omitempty"`
	// IncludeDenied captures denied / 4xx / 5xx outcomes (default
	// true). Toggle off to avoid double-rendering with the existing
	// Recent-denials panel.
	IncludeDenied *bool `yaml:"includeDenied,omitempty"`
}

func (c *RequestLogConfig) IsEnabled() bool { return c != nil && c.Enable }
func (c *RequestLogConfig) IncludeSuccessOrDefault() bool {
	return c == nil || c.IncludeSuccess == nil || *c.IncludeSuccess
}
func (c *RequestLogConfig) IncludeDeniedOrDefault() bool {
	return c == nil || c.IncludeDenied == nil || *c.IncludeDenied
}
func (c *RequestLogConfig) MaxEntriesOrDefault() int {
	if c == nil || c.MaxEntries <= 0 {
		return 10000
	}
	return c.MaxEntries
}
func (c *RequestLogConfig) MaxAgeOrDefault() time.Duration {
	if c == nil || c.MaxAge <= 0 {
		return 30 * time.Minute
	}
	return c.MaxAge
}

// IsEnabled reports whether the standalone dashboard should run.
// The dashboard is OFF by default: it exposes operator state
// (rules, identities, recent denials, optionally request paths) on
// a port bound to config.host (defaults to 0.0.0.0) with no built-in
// auth. Opt in explicitly via `dashboard.enable: true`; the Helm
// chart sets this for you and pairs it with the optional
// `ingressDashboard` / `gateway.dashboard` Ingress that the chart
// recommends gating with BasicAuth or an IP allowlist.
func (d *DashboardConfig) IsEnabled() bool {
	if d == nil {
		return false
	}
	return d.Enable != nil && *d.Enable
}

type EvmConfig struct {
	RPC EvmRpcConfig   `yaml:"rpc,omitempty"`
	WS  EvmRpcWsConfig `yaml:"ws,omitempty"`
}

type EvmRpcConfig struct {
	Default   RuleAction     `yaml:"default,omitempty" default:"deny"`
	Rules     []*JsonRpcRule `yaml:"rules,omitempty"`
	HttpRules []*HttpRule    `yaml:"httpRules,omitempty"`
}

type EvmRpcWsConfig struct {
	Default              RuleAction     `yaml:"default,omitempty" default:"deny"`
	Rules                []*JsonRpcRule `yaml:"rules,omitempty"`
	WebSocketConnections int            `yaml:"webSocketConnections,omitempty" default:"10"`
}

// ReadConfigFromFile parses YAML at path and returns a fully prepared Config
// (env vars interpolated, defaults applied, rules sorted by priority, globs
// compiled). The returned config is suitable to pass to New() directly.
//
// Env interpolation runs BEFORE YAML parse so that variable references appear
// inside any string-shaped field — including unquoted scalars. Supported
// forms: ${VAR}, ${VAR:-default}, ${VAR:?message}. See EnvInterpolate.
func ReadConfigFromFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}
	interpolated, err := EnvInterpolate(string(raw))
	if err != nil {
		return nil, fmt.Errorf("error interpolating env vars in %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte(interpolated), &cfg); err != nil {
		return nil, fmt.Errorf("error in config file unmarshal: %v", err)
	}
	// yaml.Unmarshal silently drops keys with no matching struct field, so a
	// v3 config's removed Redis backend would be ignored and each replica
	// would fall back to an ISOLATED embedded cache instead of the shared one
	// the operator intended. Detect the removed keys and fail loudly with a
	// migration pointer rather than degrade silently.
	if err := detectRemovedConfigKeys([]byte(interpolated)); err != nil {
		return nil, err
	}
	if err := PrepareConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// detectRemovedConfigKeys fails when a config still contains keys that v4
// removed, so the operator gets a clear migration message instead of a
// silently-ignored setting.
func detectRemovedConfigKeys(raw []byte) error {
	var probe struct {
		Cache struct {
			Redis         *yaml.Node `yaml:"redis"`
			RedisSentinel *yaml.Node `yaml:"redis-sentinel"`
		} `yaml:"cache"`
	}
	// Ignore unmarshal errors here — the real unmarshal already validated
	// the document; this probe only inspects specific keys.
	_ = yaml.Unmarshal(raw, &probe)
	if probe.Cache.Redis != nil || probe.Cache.RedisSentinel != nil {
		return fmt.Errorf("cache.redis / cache.redis-sentinel were removed in v4: the distributed cache/rate-limiter now uses the embedded olric cluster — migrate to a cache.cluster block (see CONFIG.md). Remove the redis keys to start")
	}
	return nil
}

// PrepareConfig finalizes a parsed-or-constructed Config: applies struct-tag
// defaults, sorts each rule list by priority, then compiles globs. Returns
// an error if any rule's globs are malformed; the caller can then reject the
// config (or, on hot-reload, preserve the previous one).
//
// Order matters: defaults must run BEFORE compile so any default-populated
// path/method values become valid glob inputs. Both `ReadConfigFromFile` and
// `cosmoguard.New(*Config)` funnel through this helper so programmatic and
// YAML-sourced configs end up identical.
//
// Idempotent — Compile() reinitializes the glob fields on each call.
func PrepareConfig(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	// Capture whether the operator supplied a v3 `node:` block BEFORE
	// defaults.Set runs. The previous "explicitly set" heuristic
	// (`Host != "127.0.0.1"`) couldn't distinguish operator intent
	// from a defaults-filled host, so a config that set both `node:`
	// (with rpcPort but the default host) and `nodes:` would silently
	// lose the rpcPort instead of failing fast. Comparing to the zero
	// value here is unambiguous — defaults.Set hasn't filled anything
	// yet.
	nodeSet := cfg.Node != (NodeConfig{})
	if err := defaults.Set(cfg); err != nil {
		return fmt.Errorf("applying config defaults: %w", err)
	}
	// MaxBatchSize is a *int so we can distinguish "unset" from
	// "explicitly 0 (disable cap)". defaults.Set can't fill *int
	// from a struct tag, so apply the default of 100 here when the
	// operator left the field nil. An explicit 0 in YAML survives
	// PrepareConfig untouched.
	if cfg.RPC.JsonRpc.MaxBatchSize == nil {
		d := 100
		cfg.RPC.JsonRpc.MaxBatchSize = &d
	}
	// v3 compat: if the operator only set the singular `node:` block,
	// promote it into Nodes so the rest of the code path can assume a
	// list. The v3 shape is "one node, the one in `node:`". A config
	// that provides both `node:` and `nodes:` is an error — operators
	// should pick one. nodeSet was captured before defaults.Set ran so
	// we don't conflate a defaults-filled Node with operator intent.
	if nodeSet && len(cfg.Nodes) > 0 {
		return fmt.Errorf("config: set either `node:` (v3) or `nodes:` (v4), not both")
	}
	if len(cfg.Nodes) == 0 {
		cfg.Nodes = []NodeConfig{cfg.Node}
	}
	// Clear the v3 singular Node now that its content lives in
	// Nodes[0] (or was confirmed empty). Subsequent passes — env
	// overrides, validation, idempotent re-runs of PrepareConfig —
	// must read from cfg.Nodes only; leaving cfg.Node populated
	// risks a future caller reading the stale pre-promotion shape.
	cfg.Node = NodeConfig{}
	// Env overrides layer on top of YAML + defaults so external
	// orchestrators (cosmopilot, Helm, plain `docker run -e …`) can
	// inject the per-deployment plumbing without rewriting the YAML.
	// Must run AFTER v3→v4 promotion (so cfg.Nodes[0] exists) and
	// BEFORE per-node validation (so a bad env value fails startup
	// the same way a bad YAML value would).
	if err := applyEnvOverrides(cfg); err != nil {
		return err
	}
	// Each upstream gets a stable name. Auto-assign when omitted.
	for i := range cfg.Nodes {
		if cfg.Nodes[i].Name == "" {
			cfg.Nodes[i].Name = fmt.Sprintf("node-%d", i)
		}
		if err := validateNodeURLs(&cfg.Nodes[i]); err != nil {
			return fmt.Errorf("nodes[%d] (%s): %w", i, cfg.Nodes[i].Name, err)
		}
		if err := validateNodeDiscovery(&cfg.Nodes[i]); err != nil {
			return fmt.Errorf("nodes[%d] (%s): %w", i, cfg.Nodes[i].Name, err)
		}
		if err := validateNodeHealthcheck(&cfg.Nodes[i]); err != nil {
			return fmt.Errorf("nodes[%d] (%s): %w", i, cfg.Nodes[i].Name, err)
		}
	}
	if err := sortAndCompileHttp(cfg.LCD.Rules); err != nil {
		return fmt.Errorf("lcd: %w", err)
	}
	if err := sortAndCompileHttp(cfg.RPC.Rules); err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	if err := sortAndCompileJsonRpc(cfg.RPC.JsonRpc.Rules); err != nil {
		return fmt.Errorf("rpc.jsonrpc: %w", err)
	}
	if err := sortAndCompileGrpc(cfg.GRPC.Rules); err != nil {
		return fmt.Errorf("grpc: %w", err)
	}
	if err := sortAndCompileJsonRpc(cfg.EVM.RPC.Rules); err != nil {
		return fmt.Errorf("evm.rpc: %w", err)
	}
	if err := sortAndCompileHttp(cfg.EVM.RPC.HttpRules); err != nil {
		return fmt.Errorf("evm.rpc.httpRules: %w", err)
	}
	if err := sortAndCompileJsonRpc(cfg.EVM.WS.Rules); err != nil {
		return fmt.Errorf("evm.ws: %w", err)
	}
	if err := cfg.CORS.Compile(); err != nil {
		return fmt.Errorf("cors: %w", err)
	}
	if err := validateUpstreamStrategy(cfg.Upstream.Strategy); err != nil {
		return err
	}
	if err := validateTracing(&cfg.Tracing); err != nil {
		return err
	}
	if err := validateListenerPorts(cfg); err != nil {
		return err
	}
	if err := validateDashboardAuth(cfg); err != nil {
		return err
	}
	if err := validateAuthEndpoints(&cfg.Auth); err != nil {
		return err
	}
	if err := validateServerLimits(&cfg.Server); err != nil {
		return err
	}
	if err := validateCacheBackend(&cfg.Cache); err != nil {
		return err
	}
	// Publish the trusted-proxies CIDR list so GetSourceIP /
	// rate-limit / audit code can honor forwarded headers ONLY when
	// the immediate peer is on the allowlist. Empty list → secure-
	// by-default (every header is ignored). Validation lives inside
	// SetTrustedProxies so a typo in a CIDR fails startup loudly
	// rather than being silently treated as "trust nothing".
	if err := SetTrustedProxies(cfg.Server.TrustedProxies); err != nil {
		return err
	}
	return nil
}

// validateDashboardAuth rejects half-configured basic auth where a
// username is set but the password is empty. Without this guard the
// gate would gladly accept the empty string as the password — an
// attacker who guesses or learns the username could authenticate
// just by sending no password at all. Applies to both the standalone
// DashboardConfig and the legacy WebUIConfig (which mounts under
// /admin/ on the metrics port) so neither entry point is left exposed.
//
// The intentional "open dashboard" case (both empty) is still allowed:
// it's the documented "behind a trusted network / authenticating
// ingress" mode. We only reject the mismatched shape where the
// operator clearly meant to enable auth but missed half of it.
func validateDashboardAuth(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	if cfg.Dashboard.BasicAuthUser != "" && cfg.Dashboard.BasicAuthPassword == "" {
		return fmt.Errorf("dashboard.basicAuthUser set without basicAuthPassword (set both or neither)")
	}
	if cfg.Dashboard.BasicAuthUser == "" && cfg.Dashboard.BasicAuthPassword != "" {
		return fmt.Errorf("dashboard.basicAuthPassword set without basicAuthUser (set both or neither)")
	}
	if cfg.Metrics.WebUI.BasicAuthUser != "" && cfg.Metrics.WebUI.BasicAuthPassword == "" {
		return fmt.Errorf("metrics.webUI.basicAuthUser set without basicAuthPassword (set both or neither)")
	}
	if cfg.Metrics.WebUI.BasicAuthUser == "" && cfg.Metrics.WebUI.BasicAuthPassword != "" {
		return fmt.Errorf("metrics.webUI.basicAuthPassword set without basicAuthUser (set both or neither)")
	}
	return nil
}

// validateListenerPorts catches double-bind and zero-port
// misconfigurations at startup. Every listener binds on cfg.Host;
// two listeners that share the same port would race for the bind
// and the second one would either fail with a confusing EADDRINUSE
// deep in Run, or — worse on some platforms — silently steal
// traffic from the first. Skips ports for disabled features
// (metrics off, EVM off, dashboard off) so an operator who turned
// them off doesn't get a spurious "port shared" error against a
// port that's never actually bound.
//
// Port 0 is rejected for any *enabled* listener: net.Listen on :0
// binds an arbitrary ephemeral port, which silently breaks the
// accessor (MetricsPort/DashboardPort report the configured 0,
// not the actual bound port), breaks Prometheus scrape configs,
// and makes collision detection meaningless across two listeners
// each set to 0. If an operator wants to turn off a listener they
// should use the explicit Enable flag, not port:0.
func validateListenerPorts(cfg *Config) error {
	type entry struct {
		field string
		port  int
	}
	entries := []entry{
		{"rpcPort", cfg.RpcPort},
		{"lcdPort", cfg.LcdPort},
		{"grpcPort", cfg.GrpcPort},
	}
	if cfg.EnableEvm {
		entries = append(entries,
			entry{"evmRpcPort", cfg.EvmRpcPort},
			entry{"evmRpcWsPort", cfg.EvmRpcWsPort},
		)
	}
	if cfg.Metrics.IsEnabled() {
		entries = append(entries, entry{"metrics.port", cfg.Metrics.Port})
	}
	if cfg.Dashboard.IsEnabled() {
		entries = append(entries, entry{"dashboard.port", cfg.Dashboard.Port})
	}
	if cfg.Cache.Cluster != nil {
		cl := cfg.Cache.Cluster
		entries = append(entries,
			entry{"cache.cluster.bindPort", cl.BindPort},
			entry{"cache.cluster.gossipPort", cl.GossipPort},
		)
		// PeerApiPort defaults to BindPort+1 when 0; collision detection
		// has to see the resolved value, so resolve here as well.
		peerPort := cl.PeerApiPort
		if peerPort == 0 {
			peerPort = cl.BindPort + 1
		}
		entries = append(entries, entry{"cache.cluster.peerApiPort", peerPort})
	}
	seen := make(map[int]string, len(entries))
	for _, e := range entries {
		if e.port <= 0 || e.port > 65535 {
			return fmt.Errorf("%s: port %d out of range (want 1..65535)", e.field, e.port)
		}
		if other, ok := seen[e.port]; ok {
			return fmt.Errorf("listener port collision: %s and %s both bind port %d",
				other, e.field, e.port)
		}
		seen[e.port] = e.field
	}
	return nil
}

// validateCacheBackend enforces the cluster-mode contract when the
// operator supplied a cache.cluster block:
//   - bindAddr must be routable per-pod (olric derives its memberlist
//     Name from BindAddr:BindPort and pods with wildcard binds would
//     clash on join);
//   - a discovery mode must be set explicitly (no environment-agnostic
//     default that wouldn't silently fail somewhere).
func validateCacheBackend(c *CacheGlobalConfig) error {
	if c.Cluster != nil {
		// A wildcard bindAddr (empty, 0.0.0.0, ::) is incompatible with
		// clustering: olric's MemberlistConfig.Name is derived from
		// BindAddr:BindPort and must be unique per peer. Two pods that
		// both bind 0.0.0.0 would share the same Name and clash on join,
		// and the dashboard fan-out (which parses Name back into a host)
		// would target an unroutable address. Force an explicit advertise
		// address — the K8s example wires this from the downward API.
		switch c.Cluster.BindAddr {
		case "", "0.0.0.0", "::", "[::]":
			return fmt.Errorf("cache.cluster.bindAddr: a routable address is required when the cluster: block is present (got %q); set it to the pod/host IP, e.g. \"${POD_IP}\" via the Kubernetes downward API", c.Cluster.BindAddr)
		}
		if c.Cluster.Discovery == nil || c.Cluster.Discovery.Mode == "" {
			return fmt.Errorf("the cluster: block is present requires cache.cluster.discovery.mode to be set")
		}
		switch c.Cluster.Discovery.Mode {
		case "dns":
			if c.Cluster.Discovery.DNS == nil || c.Cluster.Discovery.DNS.Host == "" {
				return fmt.Errorf("cache.cluster.discovery.mode=dns requires cache.cluster.discovery.dns.host")
			}
			// Port=0 is "use cluster.gossipPort" (resolved at runtime). Any
			// explicit value must be a real TCP port — without this check
			// a typo like 70000 or -1 would propagate into net.JoinHostPort
			// and surface as cryptic memberlist join errors instead.
			if p := c.Cluster.Discovery.DNS.Port; p != 0 && (p < 1 || p > 65535) {
				return fmt.Errorf("cache.cluster.discovery.dns.port: out of range (got %d, want 1..65535 or 0 to inherit cluster.gossipPort)", p)
			}
			// RefreshInterval=0 falls back to 10s at runtime. Anything
			// explicit below 1s is almost certainly a units mistake — the
			// operator typed `100` thinking milliseconds but yaml parses
			// bare numbers as nanoseconds, so we'd silently hammer DNS at
			// the maximum rate the resolver allowed. Refuse fast.
			if d := c.Cluster.Discovery.DNS.RefreshInterval; d != 0 && d < time.Second {
				return fmt.Errorf("cache.cluster.discovery.dns.refreshInterval: %s is below the 1s minimum; if you meant milliseconds, use the duration suffix (e.g. \"500ms\" is rejected; \"5s\" is the sensible floor)", d)
			}
		case "static":
			if c.Cluster.Discovery.Static == nil || len(c.Cluster.Discovery.Static.Peers) == 0 {
				return fmt.Errorf("cache.cluster.discovery.mode=static requires cache.cluster.discovery.static.peers")
			}
		case "kubernetes", "mdns":
			// Experimental modes — accepted at config-load time but
			// blocked at runtime by cluster_discovery.go until Step 7
			// finishes manual validation.
		default:
			return fmt.Errorf("cache.cluster.discovery.mode: unknown value %q (want dns / static / kubernetes / mdns)", c.Cluster.Discovery.Mode)
		}

		if c.Cluster.ReplicaCount < 1 {
			return fmt.Errorf("cache.cluster.replicaCount: must be >= 1 (got %d)", c.Cluster.ReplicaCount)
		}
		if c.Cluster.Quorum < 1 || c.Cluster.Quorum > c.Cluster.ReplicaCount {
			return fmt.Errorf("cache.cluster.quorum: must be in [1, replicaCount=%d] (got %d)", c.Cluster.ReplicaCount, c.Cluster.Quorum)
		}
		// Require gossip encryption/auth in cluster mode. Without it the
		// peer ports are unauthenticated and unencrypted — any host that
		// reaches them can join and read/write the shared DMaps (rate-limit
		// buckets, cache, JWT replay set).
		if _, err := DecodeClusterEncryptionKey(c.Cluster.EncryptionKey); err != nil {
			return fmt.Errorf("cache.cluster.encryptionKey: %w", err)
		}
	}

	return nil
}

// DecodeClusterEncryptionKey decodes and validates the base64 cluster
// encryption key. memberlist accepts 16, 24, or 32-byte keys (AES-128/192/
// 256). An empty key is rejected in cluster mode (this is only called from
// the cluster branch of validateCacheBackend and from the runtime).
func DecodeClusterEncryptionKey(b64 string) ([]byte, error) {
	if b64 == "" {
		return nil, fmt.Errorf("required in cluster mode: set a base64-encoded 16/24/32-byte key (e.g. `head -c32 /dev/urandom | base64`), the SAME value on every pod")
	}
	key, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("not valid base64: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("decoded key is %d bytes; must be 16, 24, or 32 (AES-128/192/256)", len(key))
	}
}

// validateUpstreamStrategy returns an error for unknown strategy
// values so a typo (e.g. "lest-conn") fails startup loudly instead
// of silently degrading to weighted-round-robin and leaving the
// operator wondering why their config didn't take.
func validateUpstreamStrategy(s string) error {
	switch s {
	case "", "weighted-round-robin", "round-robin", "least-conn", "primary-failover":
		return nil
	default:
		return fmt.Errorf("upstream.strategy: unknown value %q (want weighted-round-robin / round-robin / least-conn / primary-failover)", s)
	}
}

// validateServerLimits rejects a negative maxRequestBody / wsReadLimit. Those
// fields are *int64 where nil means "use the default" and an explicit 0 means
// "no limit"; a negative value (e.g. a `-1` typo) is neither, and would slip
// past the `> 0` cap check at use sites and silently remove the cap. Fail
// startup with a clear message instead.
func validateServerLimits(s *ServerConfig) error {
	if s.MaxRequestBody != nil && *s.MaxRequestBody < 0 {
		return fmt.Errorf("server.maxRequestBody must be >= 0 (0 means no limit); got %d", *s.MaxRequestBody)
	}
	if s.WSReadLimit != nil && *s.WSReadLimit < 0 {
		return fmt.Errorf("server.wsReadLimit must be >= 0 (0 means no limit); got %d", *s.WSReadLimit)
	}
	return nil
}

// validateAuthEndpoints requires the URLs cosmoguard fetches keys/decisions
// from (JWKS, OAuth introspection, external validator) to use https, unless
// they point at loopback. Over plaintext http an on-path attacker can serve
// a forged JWKS (its own RSA key under the expected kid) or a forged
// introspection/validator response and mint a fully-trusted identity — a
// complete authentication bypass. Node upstream URLs are validated
// separately (validateNodeURLs); auth endpoints previously had no scheme
// check at all.
func validateAuthEndpoints(a *AuthConfig) error {
	// When auth is disabled, NewAuthenticator builds no methods and never
	// fetches any endpoint, so a staged/dev http:// JWKS or validator URL in
	// a disabled block is inert — don't fail startup on it.
	if a == nil || !a.Enable {
		return nil
	}
	for i := range a.Methods {
		m := &a.Methods[i]
		for _, e := range []struct{ field, value string }{
			{"jwksUrl", m.JwksURL},
			{"introspectionEndpoint", m.IntrospectionEndpoint},
			{"endpoint", m.Endpoint},
		} {
			if err := requireSecureAuthURL(e.field, e.value); err != nil {
				return fmt.Errorf("auth.methods[%d]: %w", i, err)
			}
		}
	}
	return nil
}

// requireSecureAuthURL enforces https on an auth endpoint, allowing http
// only for loopback (local dev / sidecar on the same host).
func requireSecureAuthURL(field, value string) error {
	if value == "" {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%s: invalid URL %q: %w", field, value, err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopbackHost(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("%s: refusing plaintext http for a non-loopback auth endpoint %q — use https (an on-path attacker could forge the response and bypass authentication)", field, value)
	default:
		return fmt.Errorf("%s: scheme %q not supported (want https)", field, u.Scheme)
	}
}

// isLoopbackHost reports whether host is localhost or a loopback IP.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// validateNodeURLs rejects malformed or scheme-incompatible per-service
// URL overrides up-front so config errors fail startup with a clear
// `field: scheme "X" not supported` message instead of surfacing as a
// dial-time error on the first inbound request. Driven by a per-service
// table so the validation set here matches what each upstream pool
// actually accepts.
func validateNodeURLs(n *NodeConfig) error {
	type entry struct {
		field   string
		value   string
		schemes []string // accepted scheme set; empty means parse-only
	}
	for _, e := range []entry{
		{"rpcURL", n.RpcURL, []string{"http", "https"}},
		{"lcdURL", n.LcdURL, []string{"http", "https"}},
		{"grpcURL", n.GrpcURL, []string{"grpc", "grpcs", "http", "https", "tls"}},
		{"evmRpcURL", n.EvmRpcURL, []string{"http", "https"}},
		{"evmRpcWsURL", n.EvmRpcWsURL, []string{"http", "https", "ws", "wss"}},
	} {
		if e.value == "" {
			continue
		}
		u, err := parseUpstreamOverride(e.value)
		if err != nil {
			return fmt.Errorf("%s: %w", e.field, err)
		}
		ok := false
		for _, s := range e.schemes {
			if u.Scheme == s {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("%s: scheme %q not supported (want one of %v)", e.field, u.Scheme, e.schemes)
		}
	}
	return nil
}

// validateNodeDiscovery enforces the invariants of a discovery-mode
// NodeConfig: it acts as a TEMPLATE that expands into many concrete
// upstreams at runtime, so a static Host or per-service URL override
// on the same node is ambiguous and rejected up-front. The
// discovery host itself must be non-empty (otherwise there's
// nothing to resolve), and we constrain the type to the values the
// code path actually understands so a typo ("dnss") fails startup
// instead of silently degrading.
func validateNodeDiscovery(n *NodeConfig) error {
	if n.Discovery == nil {
		return nil
	}
	d := n.Discovery
	if d.Host == "" {
		return fmt.Errorf("discovery.host: required when discovery: is set")
	}
	switch d.Type {
	case "", "dns":
		// ok
	default:
		return fmt.Errorf("discovery.type: unknown value %q (only \"dns\" is supported today)", d.Type)
	}
	if d.RefreshInterval < 0 {
		return fmt.Errorf("discovery.refreshInterval: must be >= 0 (got %s)", d.RefreshInterval)
	}
	// Per-service URL overrides don't make sense per discovered pod —
	// each pod gets its own IP, but overrides are full URLs and can't
	// be templated. Reject the combination explicitly.
	if n.RpcURL != "" || n.LcdURL != "" || n.GrpcURL != "" || n.EvmRpcURL != "" || n.EvmRpcWsURL != "" {
		return fmt.Errorf("discovery: per-service URL overrides (rpcURL/lcdURL/grpcURL/evmRpcURL/evmRpcWsURL) are not supported on a discovery template")
	}
	// A static Host on the same entry is a sign the operator mixed
	// the two shapes — the discovered IP becomes Host, so the static
	// value would be silently overwritten. The default
	// "127.0.0.1" survives because defaults.Set populated it; only
	// reject explicit non-default values.
	if n.Host != "" && n.Host != "127.0.0.1" {
		return fmt.Errorf("discovery: static host (%q) cannot be combined with discovery: — discovered IPs supply the host", n.Host)
	}
	return nil
}

// validateNodeHealthcheck rejects an unknown Healthcheck.Service
// value at config-load time rather than letting it surface deep in
// buildHttpUpstream as "unknown upstream service \"grpc\"" — far
// from the YAML field that caused it. Reserved values that the
// healthcheck loop can actually probe are: lcd, rpc, evm_rpc,
// evm_rpc_ws. Empty defaults to whatever the proxy's own service
// is. "grpc" is documented as future work and explicitly rejected
// so an operator who set it knows.
func validateNodeHealthcheck(n *NodeConfig) error {
	if n == nil || !n.Healthcheck.IsEnabled() {
		return nil
	}
	switch n.Healthcheck.Service {
	case "", "lcd", "rpc", "evm_rpc", "evm_rpc_ws":
		return nil
	default:
		return fmt.Errorf("healthcheck.service: unknown value %q (want one of: lcd, rpc, evm_rpc, evm_rpc_ws)", n.Healthcheck.Service)
	}
}

// validateTracing rejects invalid TracingConfig values up-front
// rather than at the first span emit (where the error would be
// observed as silent data loss).
func validateTracing(t *TracingConfig) error {
	if t == nil || !t.Enable {
		return nil
	}
	switch t.Protocol {
	case "", "grpc", "http":
	default:
		return fmt.Errorf("tracing.protocol: unknown value %q (want grpc or http)", t.Protocol)
	}
	if t.SampleRate < 0 || t.SampleRate > 1 {
		return fmt.Errorf("tracing.sampleRate: %g out of range [0.0, 1.0]", t.SampleRate)
	}
	return nil
}

func sortAndCompileHttp(rules []*HttpRule) error {
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Priority < rules[j].Priority
	})
	for _, r := range rules {
		if err := r.Compile(); err != nil {
			return err
		}
	}
	return nil
}

func sortAndCompileJsonRpc(rules []*JsonRpcRule) error {
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Priority < rules[j].Priority
	})
	for _, r := range rules {
		if err := r.Compile(); err != nil {
			return err
		}
	}
	return nil
}

func sortAndCompileGrpc(rules []*GrpcRule) error {
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Priority < rules[j].Priority
	})
	for _, r := range rules {
		if err := r.Compile(); err != nil {
			return err
		}
	}
	return nil
}
