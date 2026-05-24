package cosmoguard

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

type CosmoGuard struct {
	cfgFile     string
	cfg         *Config
	configMutex sync.Mutex
	startedAt   time.Time

	// runDone is closed by Shutdown so the no-config-file branch of
	// Run() (which would otherwise block on a bare `select{}`) wakes
	// up and returns. Without this, callers that do
	// `go f.Run(); ...; f.Shutdown()` leak the Run goroutine for the
	// life of the process — visible in tests as race-detector noise
	// and "shutdown complete" lying about clean termination.
	// sync.Once guards the close so a second Shutdown call is safe.
	runDone     chan struct{}
	runDoneOnce sync.Once

	auth *Authenticator

	lcdProxy       *HttpProxy
	rpcProxy       *HttpProxy
	grpcProxy      *GrpcProxy
	jsonRpcHandler *JsonRpcHandler

	evmRpcProxy         *HttpProxy
	evmRpcWsProxy       *HttpProxy
	evmJsonRpcHandler   *JsonRpcHandler
	evmJsonRpcWsHandler *JsonRpcHandler

	// Metrics server, populated when Metrics.Enable is true. Per-instance so
	// multiple CosmoGuards in one process don't clash on http.DefaultServeMux.
	metricsServer *http.Server

	// Standalone dashboard server, populated when Dashboard.IsEnabled
	// is true. Bound on its own port so the metrics endpoint can be
	// exposed cluster-wide to Prometheus without leaking dashboard
	// content. nil when the dashboard is disabled — Run / Shutdown
	// guard on that.
	dashboardServer *http.Server

	// peerApiServer is the internal HTTP listener every cluster pod
	// runs so its peers can read THIS pod's local /api/v1/<resource>
	// state for fan-out aggregation. nil in embedded-only (single-
	// instance) mode. Restricted by network — see peerMembershipGate
	// in dashboard_cluster.go — and never auth-gated, never internet-
	// facing.
	peerApiServer *http.Server

	// tracingShutdown is the OpenTelemetry tracer-provider shutdown
	// hook returned by SetupTracing. Always non-nil (no-op when
	// tracing is disabled) so Shutdown can call it unconditionally.
	tracingShutdown func(context.Context) error

	// configWatcher is the fsnotify watcher driving WatchConfigFile.
	// Captured on CosmoGuard so Shutdown can close it and let the
	// watcher goroutine exit cleanly — without this, the goroutine
	// blocks on watcher.Events forever and leaks across test runs.
	// Atomic so Shutdown can read it from one goroutine while
	// WatchConfigFile assigns it from another.
	configWatcher atomic.Pointer[fsnotify.Watcher]

	// discovery is the DNS reconciler that follows pod-IP churn for
	// every NodeConfig with a Discovery block. nil when no template
	// is configured (the all-static-nodes case). Started in Run,
	// stopped in Shutdown.
	discovery *Discoverer

	// dashboard holds the in-memory observability surfaces the
	// read-only dashboard renders (unmatched-rule counters, recent
	// denials, etc.). Allocated once in New(); proxies receive a
	// pointer via SetDashboard so every instrumentation site is
	// nil-safe. Not persisted across restarts — same model as the
	// rate limiter.
	dashboard *dashboardObservability

	// requestLog is the optional per-section ring buffer of recent
	// request metadata (the dashboard's Live-traffic panel). Always
	// non-nil; Record is a no-op when the operator hasn't enabled it.
	requestLog *requestLog

	// origNodes is the upstream node config as written by the operator,
	// captured at New() BEFORE expandDiscoveryNodes rewrites cfg.Nodes
	// in place with resolved IPs. tryReload compares the freshly-read
	// config's Nodes against this (not the expanded f.cfg.Nodes) so a
	// discovery-based deployment isn't falsely flagged as a topology
	// change on every reload.
	origNodes []NodeConfig

	// cluster owns the in-process olric daemon. Always non-nil after
	// a successful New(): embedded-only mode in the single-instance
	// default config, networked in the cluster: block is present. The
	// embedded client is plumbed into every proxy so the v4 default
	// rate limiter (and, in later steps, cache + observability
	// replication) share state with peers.
	cluster *clusterRuntime

	// metricsHistory is the bounded ring buffer of recent
	// MetricsSnapshot records (5 min @ 5 s poll cadence). Sampled by
	// the observability replicator's ticker and exposed via the
	// step-5 /api/v1/metrics/history endpoint so the dashboard's
	// time-series charts hydrate immediately on connect.
	metricsHistory *metricsHistory

	// obsReplicator periodically writes (dashboard + metricsHistory)
	// to the olric DMap so a single-pod restart restores its prior
	// state from a peer's replica. nil when no olric client is
	// available (legacy test paths).
	obsReplicator *observabilityReplicator
}

// nodeRpcAddrs returns the WS backend URLs ("ws://host:port" or
// "wss://host:port") for every node — fed into the JSON-RPC handler's
// WS broker pool. Honors per-node TLS flag and RpcURL overrides.
func nodeRpcAddrs(nodes []NodeConfig) ([]string, error) {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		b, err := nodeWSBackend(n, serviceRPC)
		if err != nil {
			return nil, fmt.Errorf("node %s rpc backend: %w", n.Name, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// nodeEvmWsAddrs returns WS backend URLs for the EVM RPC WS service on
// every node.
func nodeEvmWsAddrs(nodes []NodeConfig) ([]string, error) {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		b, err := nodeWSBackend(n, serviceEVMRPCWS)
		if err != nil {
			return nil, fmt.Errorf("node %s evm-rpc-ws backend: %w", n.Name, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// NewFromFile reads a YAML config from the given path and constructs a
// CosmoGuard. Calling Run() on the returned instance also starts a config
// file watcher that hot-reloads rules on change.
//
// On load, NewFromFile re-parses the file (no env interpolation) to detect
// v3-shaped syntax — singular `node:` block, flat path/method/query on
// rules — and logs a warning suggesting --migrate-config when found. The
// detected v3 syntax keeps working forever; the warning is cosmetic.
func NewFromFile(path string) (*CosmoGuard, error) {
	slog.Info("loading config file", "file", path)
	cfg, err := ReadConfigFromFile(path)
	if err != nil {
		return nil, err
	}
	// v3 detection: re-parse the raw file (no env interp, no PrepareConfig)
	// so we can see the original shape. Errors are ignored — if we couldn't
	// parse it twice, ReadConfigFromFile would have already failed above.
	if raw, rerr := os.ReadFile(path); rerr == nil {
		var rawCfg Config
		if uerr := yaml.Unmarshal(raw, &rawCfg); uerr == nil {
			LogV3Detection(DetectV3Syntax(&rawCfg), path)
		}
	}
	cg, err := New(cfg)
	if err != nil {
		return nil, err
	}
	cg.cfgFile = path
	return cg, nil
}

// New constructs a CosmoGuard from an already-parsed Config. Unlike
// NewFromFile, the returned instance has no associated config file and will
// not hot-reload — useful for tests and programmatic embedding.
//
// The config is run through PrepareConfig (defaults + sort + compile) so
// programmatic callers get the same finalized shape YAML callers do.
// PrepareConfig is safe to invoke on an already-prepared config, so the
// NewFromFile path (which prepared during load) incurs no extra work.
func New(cfg *Config) (*CosmoGuard, error) {
	if err := PrepareConfig(cfg); err != nil {
		return nil, err
	}
	// Snapshot the operator-written node config before expansion so a
	// hot reload can detect a real topology change without tripping on
	// discovery's in-place rewrite of cfg.Nodes (see CosmoGuard.origNodes
	// and tryReload).
	origNodes := append([]NodeConfig(nil), cfg.Nodes...)
	// Expand any discovery templates BEFORE building pools so the
	// constructors see concrete IPs. Templates whose DNS lookup
	// returns zero records at boot survive — they're tracked in the
	// returned slice and will be filled in by the runtime reconciler.
	// A template lookup error (DNS unreachable) also survives boot:
	// we don't want a transient cluster-DNS hiccup to fail startup
	// for an autoscaled deployment.
	templates, err := expandDiscoveryNodes(cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("error expanding discovery templates: %w", err)
	}
	if len(cfg.Nodes) == 0 {
		// Distinguish "config has nothing" (programmer error,
		// must fix the YAML) from "every template resolved to
		// zero IPs at boot" (transient — the pool just isn't
		// ready yet). Both fail boot today because the proxy
		// constructors require at least one node, but the
		// operator-facing message should make the difference
		// obvious.
		if len(templates) == 0 {
			return nil, fmt.Errorf("no upstream nodes configured: cfg.Nodes is empty and no discovery templates are set")
		}
		hosts := make([]string, 0, len(templates))
		for _, t := range templates {
			hosts = append(hosts, t.Template.Discovery.Host)
		}
		return nil, fmt.Errorf("no upstream nodes after discovery expansion: %d template(s) resolved to zero IPs at boot (hosts: %v) and no static nodes were configured as fallback; either add a static node or wait for the headless service's pods to become ready", len(templates), hosts)
	}
	cosmoGuard := &CosmoGuard{
		cfg:            cfg,
		origNodes:      origNodes,
		startedAt:      time.Now(),
		runDone:        make(chan struct{}),
		dashboard:      newDashboardObservability(),
		requestLog:     newRequestLog(cfg.Dashboard.RequestLog),
		metricsHistory: newMetricsHistory(defaultMetricsHistoryCap),
	}
	if len(templates) > 0 {
		cosmoGuard.discovery = NewDiscoverer(log, templates, nil)
		cosmoGuard.discovery.SetDashboard(cosmoGuard.dashboard)
	}

	// success defaults to false; any error return below triggers the
	// rollback so background goroutines we already started (tracing
	// exporter, JWKS auto-refresher inside the authenticator) get
	// torn down before we return nil to the caller. Without this,
	// every failing test (or production constructor that bails late)
	// leaks goroutines for the lifetime of the process.
	success := false
	defer func() {
		if success {
			return
		}
		if cosmoGuard.auth != nil {
			_ = cosmoGuard.auth.Close()
		}
		if cosmoGuard.tracingShutdown != nil {
			_ = cosmoGuard.tracingShutdown(context.Background())
		}
		if cosmoGuard.obsReplicator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = cosmoGuard.obsReplicator.Close(ctx)
			cancel()
		}
		if cosmoGuard.cluster != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = cosmoGuard.cluster.Close(ctx)
			cancel()
		}
	}()

	// Set up tracing once. SetupTracing installs the W3C propagator
	// regardless of cfg.Tracing.Enable so traceparent headers flow
	// through cosmoguard even when local export is off.
	shutdown, err := SetupTracing(context.Background(), &cfg.Tracing)
	if err != nil {
		return nil, fmt.Errorf("error setting up tracing: %w", err)
	}
	cosmoGuard.tracingShutdown = shutdown

	// Spin up the in-process olric daemon. In the zero-config default it
	// runs embedded-only (loopback, ephemeral ports, no gossip); when
	// the cluster: block is present it joins the configured peers. The
	// embedded client is threaded into every proxy so the v4 default
	// rate limiter — and the cache + observability replication —
	// share state across pods. Failure to start is fatal: downstream
	// consumers assume the runtime is up.
	cluster, err := newClusterRuntime(clusterRuntimeOptions{
		Cluster: cfg.Cache.Cluster,
	})
	if err != nil {
		return nil, fmt.Errorf("error setting up cluster runtime: %w", err)
	}
	cosmoGuard.cluster = cluster

	// Build the shared Authenticator AFTER the cluster runtime so the
	// JWT replay store gets the live olric client and shares its
	// seen-jti set across replicas. Calling NewAuthenticator earlier
	// would pass a nil client and silently fall back to a per-pod
	// memory store — a JWT could then be replayed once per replica.
	authn, err := NewAuthenticator(&cfg.Auth, cosmoGuard.cluster.Client())
	if err != nil {
		return nil, fmt.Errorf("error setting up authenticator: %w", err)
	}
	cosmoGuard.auth = authn

	// Build the observability replicator now that the olric client
	// exists. Replicator construction is best-effort: in legacy embed
	// paths the client may be nil, in which case obsReplicator stays
	// nil and the dashboard runs without restart survival (still
	// fully functional, just loses counters on restart). The Start +
	// Close calls below are nil-safe so the rest of the code stays
	// branchless.
	cosmoGuard.obsReplicator, err = newObservabilityReplicator(
		cosmoGuard.cluster.Client(),
		cosmoGuard.dashboard,
		cosmoGuard.metricsHistory,
		func() (*MetricsSnapshot, error) {
			return gatherMetricsSnapshot(prometheus.DefaultGatherer)
		},
		"", // empty → falls through to os.Hostname()
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up observability replicator: %w", err)
	}

	// Setup gRPC proxy
	cosmoGuard.grpcProxy, err = NewGrpcProxy("grpc",
		fmt.Sprintf("%s:%d", cosmoGuard.cfg.Host, cosmoGuard.cfg.GrpcPort),
		cosmoGuard.cfg.Nodes, &cosmoGuard.cfg.Upstream,
		cosmoGuard.cfg.GRPC.Protosets,
		WithCacheConfig[GrpcProxyOptions](&cosmoGuard.cfg.Cache),
		WithOlricClient[GrpcProxyOptions](cosmoGuard.cluster.Client()),
		WithMetricsEnabled[GrpcProxyOptions](cosmoGuard.cfg.Metrics.Enable),
		WithAuthenticator[GrpcProxyOptions](cosmoGuard.auth),
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up grpc cosmoguard proxy: %w", err)
	}

	// Setup LCD proxy
	cosmoGuard.lcdProxy, err = NewHttpProxy("lcd",
		fmt.Sprintf("%s:%d", cosmoGuard.cfg.Host, cosmoGuard.cfg.LcdPort),
		cosmoGuard.cfg.Nodes, "lcd",
		WithCacheConfig[HttpProxyOptions](&cosmoGuard.cfg.Cache),
		WithOlricClient[HttpProxyOptions](cosmoGuard.cluster.Client()),
		WithServerConfig[HttpProxyOptions](&cosmoGuard.cfg.Server),
		WithAuthenticator[HttpProxyOptions](cosmoGuard.auth),
		WithCORSConfig[HttpProxyOptions](&cosmoGuard.cfg.CORS),
		WithUpstreamConfig[HttpProxyOptions](&cosmoGuard.cfg.Upstream),
		WithMetricsEnabled[HttpProxyOptions](cosmoGuard.cfg.Metrics.Enable),
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up lcd cosmoguard proxy: %w", err)
	}

	// Setup JSONRPC handler for RPC proxy
	rpcWSBackends, err := nodeRpcAddrs(cosmoGuard.cfg.Nodes)
	if err != nil {
		return nil, fmt.Errorf("error resolving rpc ws backends: %w", err)
	}
	cosmoGuard.jsonRpcHandler, err = NewJsonRpcHandler("jsonrpc",
		WithCacheConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Cache),
		WithOlricClient[JsonRpcHandlerOptions](cosmoGuard.cluster.Client()),
		WithWebSocketEnabled[JsonRpcHandlerOptions](cosmoGuard.cfg.RPC.WebSocketEnabled),
		WithWebSocketBackends[JsonRpcHandlerOptions](rpcWSBackends),
		WithWebSocketConnections[JsonRpcHandlerOptions](cosmoGuard.cfg.RPC.WebSocketConnections),
		WithMetricsEnabled[JsonRpcHandlerOptions](cosmoGuard.cfg.Metrics.Enable),
		WithServerConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Server),
		WithMaxBatchSize[JsonRpcHandlerOptions](*cosmoGuard.cfg.RPC.JsonRpc.MaxBatchSize),
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up jsonrpc handler: %w", err)
	}

	// Setup RPC proxy
	cosmoGuard.rpcProxy, err = NewHttpProxy("rpc",
		fmt.Sprintf("%s:%d", cosmoGuard.cfg.Host, cosmoGuard.cfg.RpcPort),
		cosmoGuard.cfg.Nodes, "rpc",
		WithCacheConfig[HttpProxyOptions](&cosmoGuard.cfg.Cache),
		WithOlricClient[HttpProxyOptions](cosmoGuard.cluster.Client()),
		WithServerConfig[HttpProxyOptions](&cosmoGuard.cfg.Server),
		WithAuthenticator[HttpProxyOptions](cosmoGuard.auth),
		WithCORSConfig[HttpProxyOptions](&cosmoGuard.cfg.CORS),
		WithUpstreamConfig[HttpProxyOptions](&cosmoGuard.cfg.Upstream),
		WithMetricsEnabled[HttpProxyOptions](cosmoGuard.cfg.Metrics.Enable),
		WithEndpointHandler[HttpProxyOptions]([]Endpoint{
			{
				Path:   "/",
				Method: "POST",
			},
			{
				Path:   defaultWebsocketPath,
				Method: "GET",
			},
		}, cosmoGuard.jsonRpcHandler),
	)
	if err != nil {
		return nil, fmt.Errorf("error setting up rpc cosmoguard proxy: %w", err)
	}

	if cosmoGuard.cfg.EnableEvm {
		// Setup JSONRPC handler for EVM RPC proxy
		cosmoGuard.evmJsonRpcHandler, err = NewJsonRpcHandler("evm_jsonrpc",
			WithCacheConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Cache),
			WithOlricClient[JsonRpcHandlerOptions](cosmoGuard.cluster.Client()),
			WithWebSocketEnabled[JsonRpcHandlerOptions](false),
			WithMetricsEnabled[JsonRpcHandlerOptions](cosmoGuard.cfg.Metrics.Enable),
			WithServerConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Server),
			// EVM RPC JsonRpc config reuses the RPC JsonRpc batch cap unless
			// it has its own (EvmRpcConfig is currently flat; revisit in
			// Phase C when the schema gets per-protocol overrides).
			WithMaxBatchSize[JsonRpcHandlerOptions](*cosmoGuard.cfg.RPC.JsonRpc.MaxBatchSize),
		)
		if err != nil {
			return nil, fmt.Errorf("error setting up jsonrpc handler for evm-rpc: %w", err)
		}

		cosmoGuard.evmRpcProxy, err = NewHttpProxy("evm_rpc",
			fmt.Sprintf("%s:%d", cosmoGuard.cfg.Host, cosmoGuard.cfg.EvmRpcPort),
			cosmoGuard.cfg.Nodes, "evm_rpc",
			WithCacheConfig[HttpProxyOptions](&cosmoGuard.cfg.Cache),
			WithOlricClient[HttpProxyOptions](cosmoGuard.cluster.Client()),
			WithServerConfig[HttpProxyOptions](&cosmoGuard.cfg.Server),
			WithAuthenticator[HttpProxyOptions](cosmoGuard.auth),
			WithCORSConfig[HttpProxyOptions](&cosmoGuard.cfg.CORS),
			WithUpstreamConfig[HttpProxyOptions](&cosmoGuard.cfg.Upstream),
			WithMetricsEnabled[HttpProxyOptions](cosmoGuard.cfg.Metrics.Enable),
			WithEndpointHandler[HttpProxyOptions]([]Endpoint{
				{
					Path:   "/",
					Method: "POST",
				},
			}, cosmoGuard.evmJsonRpcHandler),
		)
		if err != nil {
			return nil, fmt.Errorf("error setting up evm-rpc cosmoguard proxy: %w", err)
		}

		// Setup JSONRPC handler for EVM RPC WS proxy
		evmWSBackends, err := nodeEvmWsAddrs(cosmoGuard.cfg.Nodes)
		if err != nil {
			return nil, fmt.Errorf("error resolving evm-rpc-ws backends: %w", err)
		}
		cosmoGuard.evmJsonRpcWsHandler, err = NewJsonRpcHandler("evm_jsonrpc_ws",
			WithCacheConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Cache),
			WithOlricClient[JsonRpcHandlerOptions](cosmoGuard.cluster.Client()),
			WithWebSocketEnabled[JsonRpcHandlerOptions](true),
			WithWebSocketConnections[JsonRpcHandlerOptions](cosmoGuard.cfg.EVM.WS.WebSocketConnections),
			WithWebSocketBackends[JsonRpcHandlerOptions](evmWSBackends),
			WithWebSocketPath[JsonRpcHandlerOptions]("/"),
			WithUpstreamManager[JsonRpcHandlerOptions](EthUpstreamConnManager),
			WithMetricsEnabled[JsonRpcHandlerOptions](cosmoGuard.cfg.Metrics.Enable),
			WithServerConfig[JsonRpcHandlerOptions](&cosmoGuard.cfg.Server),
		)
		if err != nil {
			return nil, fmt.Errorf("error setting up jsonrpc handler for evm-rpc: %w", err)
		}

		cosmoGuard.evmRpcWsProxy, err = NewHttpProxy("evm_rpc_ws",
			fmt.Sprintf("%s:%d", cosmoGuard.cfg.Host, cosmoGuard.cfg.EvmRpcWsPort),
			cosmoGuard.cfg.Nodes, "evm_rpc_ws",
			WithCacheConfig[HttpProxyOptions](&cosmoGuard.cfg.Cache),
			WithOlricClient[HttpProxyOptions](cosmoGuard.cluster.Client()),
			WithServerConfig[HttpProxyOptions](&cosmoGuard.cfg.Server),
			WithAuthenticator[HttpProxyOptions](cosmoGuard.auth),
			WithCORSConfig[HttpProxyOptions](&cosmoGuard.cfg.CORS),
			WithUpstreamConfig[HttpProxyOptions](&cosmoGuard.cfg.Upstream),
			WithMetricsEnabled[HttpProxyOptions](cosmoGuard.cfg.Metrics.Enable),
			WithEndpointHandler[HttpProxyOptions]([]Endpoint{
				{
					Path:   "/",
					Method: "GET",
				},
			}, cosmoGuard.evmJsonRpcWsHandler),
		)
		if err != nil {
			return nil, fmt.Errorf("error setting up evm-rpc-ws cosmoguard proxy: %w", err)
		}
	}

	// Wire the dashboard observability sink into every proxy /
	// handler so unmatched + deny events flow into the same buffers
	// served by /api/v1/unmatched and /api/v1/denied. Sections match
	// the names listRules uses so the dashboard joins them cleanly.
	// The JsonRpcHandler.SetDashboard call propagates to its embedded
	// WS proxy, so we only need to call it on the handler.
	if cosmoGuard.lcdProxy != nil {
		cosmoGuard.lcdProxy.SetDashboard("lcd", cosmoGuard.dashboard)
	}
	if cosmoGuard.rpcProxy != nil {
		cosmoGuard.rpcProxy.SetDashboard("rpc", cosmoGuard.dashboard)
	}
	if cosmoGuard.jsonRpcHandler != nil {
		cosmoGuard.jsonRpcHandler.SetDashboard("rpc.jsonrpc", cosmoGuard.dashboard)
	}
	if cosmoGuard.grpcProxy != nil {
		cosmoGuard.grpcProxy.SetDashboard("grpc", cosmoGuard.dashboard)
	}
	if cosmoGuard.evmRpcProxy != nil {
		cosmoGuard.evmRpcProxy.SetDashboard("evm.rpc.httpRules", cosmoGuard.dashboard)
	}
	if cosmoGuard.evmJsonRpcHandler != nil {
		cosmoGuard.evmJsonRpcHandler.SetDashboard("evm.rpc", cosmoGuard.dashboard)
	}
	if cosmoGuard.evmRpcWsProxy != nil {
		cosmoGuard.evmRpcWsProxy.SetDashboard("evm.ws", cosmoGuard.dashboard)
	}
	if cosmoGuard.evmJsonRpcWsHandler != nil {
		cosmoGuard.evmJsonRpcWsHandler.SetDashboard("evm.ws", cosmoGuard.dashboard)
	}

	// JSON-RPC handlers get the Authenticator so per-rule auth gates
	// on JsonRpcRule.Auth can evaluate against the resolved identity.
	// HttpProxy already gets it via WithAuthenticator at construction.
	if cosmoGuard.jsonRpcHandler != nil {
		cosmoGuard.jsonRpcHandler.SetAuthenticator(cosmoGuard.auth)
	}
	if cosmoGuard.evmJsonRpcHandler != nil {
		cosmoGuard.evmJsonRpcHandler.SetAuthenticator(cosmoGuard.auth)
	}
	if cosmoGuard.evmJsonRpcWsHandler != nil {
		cosmoGuard.evmJsonRpcWsHandler.SetAuthenticator(cosmoGuard.auth)
	}

	// Wire the request-log ring into every HTTP proxy. The HTTP layer
	// fronts LCD, RPC, EVM-RPC, EVM-WS, and JSON-RPC (POST /), so a
	// single Record per HTTP completion covers all of those.
	if cosmoGuard.lcdProxy != nil {
		cosmoGuard.lcdProxy.SetRequestLog(cosmoGuard.requestLog)
	}
	if cosmoGuard.rpcProxy != nil {
		cosmoGuard.rpcProxy.SetRequestLog(cosmoGuard.requestLog)
	}
	if cosmoGuard.evmRpcProxy != nil {
		cosmoGuard.evmRpcProxy.SetRequestLog(cosmoGuard.requestLog)
	}
	if cosmoGuard.evmRpcWsProxy != nil {
		cosmoGuard.evmRpcWsProxy.SetRequestLog(cosmoGuard.requestLog)
	}
	// WS frames + connect/close lifecycle land in the same feed. The
	// handlers forward to their embedded WS proxy; the HTTP-fronted
	// POST / path is already covered by the HttpProxy wiring above.
	if cosmoGuard.jsonRpcHandler != nil {
		cosmoGuard.jsonRpcHandler.SetRequestLog(cosmoGuard.requestLog)
	}
	if cosmoGuard.evmJsonRpcWsHandler != nil {
		cosmoGuard.evmJsonRpcWsHandler.SetRequestLog(cosmoGuard.requestLog)
	}

	// Register the now-built pools with the DNS reconciler so it can
	// add/remove upstreams as pod IPs churn. WS pools aren't included
	// — their fixed-size connection set with pinned subscriptions
	// can't be runtime-resized safely; pod-IP changes there require
	// a cosmoguard restart and the operator is expected to roll
	// cosmoguard alongside the upstream Deployment.
	if cosmoGuard.discovery != nil {
		if cosmoGuard.lcdProxy != nil {
			cosmoGuard.discovery.RegisterHttpPool(cosmoGuard.lcdProxy.pool)
		}
		if cosmoGuard.rpcProxy != nil {
			cosmoGuard.discovery.RegisterHttpPool(cosmoGuard.rpcProxy.pool)
		}
		if cosmoGuard.evmRpcProxy != nil {
			cosmoGuard.discovery.RegisterHttpPool(cosmoGuard.evmRpcProxy.pool)
		}
		if cosmoGuard.evmRpcWsProxy != nil {
			cosmoGuard.discovery.RegisterHttpPool(cosmoGuard.evmRpcWsProxy.pool)
		}
		if cosmoGuard.grpcProxy != nil {
			cosmoGuard.discovery.RegisterGrpcPool(cosmoGuard.grpcProxy.pool)
		}
	}

	// Construct (but don't yet start) the metrics + ops server. MUST REMAIN
	// SYNCHRONOUS — Shutdown reads f.metricsServer without a lock, relying on
	// the happens-before chain: New returns → caller obtains *CosmoGuard →
	// caller may call Shutdown. If anyone moves this assignment into a
	// goroutine launched from New the race detector will catch it.
	//
	// Each instance gets its own mux so multiple CosmoGuards in one process
	// (e.g. tests) don't clash on http.DefaultServeMux. The mux serves:
	//   * /metrics   — Prometheus exposition
	//   * /healthz   — liveness probe (200 if the process is running)
	//   * /readyz    — readiness probe; 200 iff every served upstream pool
	//                  has at least one healthy member. /info's
	//                  upstreams.healthy mirrors the same gate so the
	//                  dashboard never says "3/3 healthy" while readiness
	//                  is failing.
	//
	// Gated on Metrics.Enable so operators who explicitly disable the
	// metrics endpoint don't get an unexpected port bound. Kubernetes
	// deployments wanting health probes therefore need metrics enabled
	// (the default).
	if cfg.Metrics.Enable {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		// Opt-in pprof on the metrics port. Gated by COSMOGUARD_PPROF
		// because /debug/pprof leaks symbol info and the heap profile
		// is CPU-heavy; restrict via NetworkPolicy when enabled.
		if os.Getenv("COSMOGUARD_PPROF") != "" {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		}
		// readOnly wraps a handler to reject non-GET/HEAD methods
		// with 405. Without this, POST /healthz returned 200 — which
		// fuzzers, ops tooling, and a small subset of Kubernetes
		// probe clients treat as a contract violation. RFC says
		// servers MUST reject unsupported methods on a known route.
		readOnly := func(h http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet && r.Method != http.MethodHead {
					w.Header().Set("Allow", "GET, HEAD")
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				h(w, r)
			}
		}
		mux.HandleFunc("/healthz", readOnly(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		mux.HandleFunc("/info", readOnly(func(w http.ResponseWriter, _ *http.Request) {
			// Report the live upstream surface, not the configured node
			// count. Discovery (and any future scaling source) mutates
			// pool membership under the hood — using len(cfg.Nodes)
			// would freeze on the boot-time configured set and miss
			// every post-boot pod-IP shift. Dedupe by upstream name so
			// a node configured into multiple service pools (LCD + RPC
			// + ...) isn't double-counted; matches countHealthyUpstreams'
			// contract for the "healthy" field.
			info := map[string]any{
				"version":        Version,
				"commit":         CommitHash,
				"started_at":     cosmoGuard.startedAt.UTC().Format(time.RFC3339),
				"uptime_seconds": int(time.Since(cosmoGuard.startedAt).Seconds()),
				"upstreams": map[string]any{
					"count":   cosmoGuard.countUpstreams(),
					"healthy": cosmoGuard.countHealthyUpstreams(),
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = stdjson.NewEncoder(w).Encode(info)
		}))
		mux.HandleFunc("/readyz", readOnly(func(w http.ResponseWriter, _ *http.Request) {
			// Ready iff every CONFIGURED upstream pool has at least one
			// healthy member. Single-node pools with no healthcheck are
			// always reported healthy by the pool, so this only goes red
			// in multi-upstream deployments where every upstream's probes
			// have failed UnhealthyAfter times. Covers gRPC + EVM in
			// addition to LCD + RPC so a gRPC-only or EVM-only
			// deployment can't lie its way to ready.
			if !cosmoGuard.allPoolsReady() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("no healthy upstreams"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
		}))
		// Read-only dashboard at /admin/ plus its JSON API at
		// /admin/api/v1/*. Off by default — the metrics port is
		// commonly exposed cluster-wide to Prometheus, so an
		// unauthenticated dashboard there would leak upstream
		// targets + identity scopes. Operators opt in via
		// metrics.webUI.enable; BasicAuth gating is recommended.
		if cfg.Metrics.WebUI.Enable {
			installWebUI(mux, cosmoGuard, &cfg.Metrics.WebUI)
		}
		cosmoGuard.metricsServer = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", cfg.Host, cfg.Metrics.Port),
			Handler: mux,
		}
	}

	// Standalone read-only dashboard listener. Independent of
	// Metrics.Enable so an operator who turns metrics off can still
	// run the dashboard, and vice versa. Construction here keeps the
	// happens-before chain that Shutdown relies on (see metricsServer
	// comment above): every server field assigned before New returns.
	cosmoGuard.dashboardServer = installDashboardServer(cosmoGuard, &cfg.Dashboard, cfg.Host)
	// Peer-API listener — present only in cluster mode. The fan-out
	// aggregators on the public dashboard read from this listener on
	// every cluster peer. Building it AFTER the cluster runtime + the
	// dashboard observability surfaces means the handlers see a
	// fully-wired CosmoGuard from the first request.
	cosmoGuard.peerApiServer = installPeerAPIServer(cosmoGuard)

	success = true
	return cosmoGuard, nil
}

func (f *CosmoGuard) Run() error {
	f.applyRules()

	// Start observability replication AFTER the dashboard surfaces
	// are wired but BEFORE any traffic starts flowing. Restore-from-
	// peer happens here: a pod restart picks up its prior counters
	// (denied, unmatched, history) from the DMap before the first
	// request lands, so the dashboard isn't briefly empty during
	// startup.
	if f.obsReplicator != nil {
		startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := f.obsReplicator.Start(startCtx); err != nil {
			slog.Warn("observability replication: start failed (continuing without restart survival)", "error", err)
		}
		cancel()
	}

	// Start DNS discovery now that proxies (and their pools) are
	// constructed and have hosting the initial set of upstreams. The
	// reconciler diffs against the current pool membership, so this
	// must run AFTER the initial pools are populated by NewHttp/Grpc
	// UpstreamPool, AND BEFORE the proxies start serving — a discovery
	// add that lands mid-request would be valid but harder to reason
	// about. In practice the proxy.Run() goroutines start immediately
	// below, so the race is benign even if it occurred.
	if f.discovery != nil {
		f.discovery.Start()
	}

	if f.metricsServer != nil {
		go func(srv *http.Server) {
			slog.Info("starting metrics + ops server (/metrics, /healthz, /readyz)", "address", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("error starting metrics server", "error", err)
			}
		}(f.metricsServer)
	}

	if f.dashboardServer != nil {
		go func(srv *http.Server) {
			slog.Info("starting read-only dashboard", "address", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("error starting dashboard server", "error", err)
			}
		}(f.dashboardServer)
	}

	if f.peerApiServer != nil {
		go func(srv *http.Server) {
			slog.Info("starting cluster peer api", "address", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("error starting peer api server", "error", err)
			}
		}(f.peerApiServer)
	}

	go func() {
		if err := f.rpcProxy.Run(); err != nil {
			slog.Error("rpc proxy returned error", "error", err)
		}
	}()

	go func() {
		if err := f.grpcProxy.Run(); err != nil {
			slog.Error("grpc proxy returned error", "error", err)
		}
	}()

	go func() {
		if err := f.lcdProxy.Run(); err != nil {
			slog.Error("lcd proxy returned error", "error", err)
		}
	}()

	if f.cfg.EnableEvm {
		go func() {
			if err := f.evmRpcProxy.Run(); err != nil {
				slog.Error("evm-rpc proxy returned error", "error", err)
			}
		}()

		go func() {
			if err := f.evmRpcWsProxy.Run(); err != nil {
				slog.Error("evm-rpc-ws proxy returned error", "error", err)
			}
		}()
	}

	// Block on config file watcher when bound to a file. Instances
	// constructed via New() (no file) block on runDone so Shutdown
	// can wake the goroutine cleanly — previously this was a bare
	// `select {}` and the Run goroutine leaked across Shutdown.
	if f.cfgFile == "" {
		<-f.runDone
		return nil
	}
	return f.WatchConfigFile()
}

// WatchConfigFile blocks the goroutine that calls it, watching cfgFile for
// changes and re-applying rules atomically. A bad reload (malformed YAML,
// invalid glob, missing required env var) is logged and the previous config
// stays in effect. Transient fsnotify errors are logged and the watcher
// keeps running — one inotify hiccup never permanently disables hot reload.
func (f *CosmoGuard) WatchConfigFile() error {
	if f.cfgFile == "" {
		return fmt.Errorf("cosmoguard has no associated config file")
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	f.configWatcher.Store(watcher)

	if err := watcher.Add(filepath.Dir(f.cfgFile)); err != nil {
		return err
	}
	// Watching the directory (rather than the file) lets us follow editors
	// that atomic-save via rename — but it also means every unrelated event
	// in the directory lands on our channel. Without filtering, dropping
	// the config in a busy directory like /tmp turns into a reload storm
	// (one per neighbouring temp file). Match on the cleaned target path.
	target := filepath.Clean(f.cfgFile)
	// Debounce window: a single save typically emits multiple events
	// (Write+Chmod on plain saves, Create+Rename+Remove on atomic-save
	// editors like vim/VS Code). Without coalescing them, the first
	// event triggers a meaningful tryReload that records the real
	// delta on the dashboard, and the second event triggers a no-op
	// tryReload that overwrites the dashboard with "no change" — the
	// operator just saved a TTL edit and the panel still shows zero
	// delta. 200ms is short enough that the operator doesn't perceive
	// a lag and long enough to absorb every editor save pattern we've
	// seen in practice.
	const debounceWindow = 200 * time.Millisecond
	var (
		debounceTimer *time.Timer
		debounceCh    <-chan time.Time
	)
	for {
		select {
		case ev, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("config watcher events channel closed")
			}
			if filepath.Clean(ev.Name) != target {
				continue
			}
			// (Re)arm the debounce timer instead of reloading
			// inline. Each event that arrives within the window
			// pushes the firing time out, so a burst of N events
			// from one save collapses into one reload.
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.NewTimer(debounceWindow)
			debounceCh = debounceTimer.C
		case <-debounceCh:
			debounceCh = nil
			f.tryReload()
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("config watcher errors channel closed")
			}
			// fsnotify can emit transient errors (e.g. EINTR) that don't
			// invalidate the watch. Log and keep watching rather than
			// permanently disabling hot reload.
			slog.Warn("config watcher transient error; continuing", "error", err)
		}
	}
}

// tryReload re-reads and re-applies the config file. On failure the previous
// in-memory config is preserved and the error is logged — the running proxy
// keeps serving with the last-known-good ruleset.
//
// The swap and the applyRules call run under one mutex acquisition so two
// concurrent reloads cannot interleave (R1 swap, R2 swap, R2 apply, R1 apply
// would otherwise leave proxies with the older config). fsnotify delivers
// events one at a time so this is mostly belt-and-braces, but the cost is
// negligible.
func (f *CosmoGuard) tryReload() {
	slog.Info("reloading config file", "file", f.cfgFile)
	newCfg, err := ReadConfigFromFile(f.cfgFile)
	if err != nil {
		slog.Error("config reload failed; keeping previous config", "error", err)
		// Surface the failure on the dashboard so an operator
		// staring at the pill can see "last reload errored" instead
		// of the older success rendering forever.
		f.dashboard.RecordReload(false, err.Error(), nil)
		return
	}

	f.configMutex.Lock()
	defer f.configMutex.Unlock()
	// Reject reloads that try to change runtime-immutable cache/cluster
	// topology. Proxies, the rate-limiter pool, the olric runtime and the
	// peer-API listener are all wired ONCE at startup against the original
	// CacheGlobalConfig pointer; reloading silently here would accept the
	// edit, show a green pill on the dashboard, and keep serving traffic
	// from the OLD backend — exactly the "thought I changed it but nothing
	// happened" trap. Surfacing the rejection through RecordReload puts the
	// constraint in front of the operator instead of buried in logs.
	if msg, ok := cacheTopologyChange(f.cfg, newCfg); !ok {
		err := fmt.Errorf("cache topology change requires a process restart: %s", msg)
		slog.Warn("config reload rejected", "error", err)
		f.dashboard.RecordReload(false, err.Error(), nil)
		return
	}
	// enableEvm is wired ONCE at startup: when it's false the EVM
	// proxies/handlers are never constructed (left nil). Accepting a
	// flip here would make applyRulesLocked's EVM branch dereference
	// those nil servers and panic the watcher goroutine. Treat it as
	// runtime-immutable, same as cache topology — reject and surface it.
	if f.cfg.EnableEvm != newCfg.EnableEvm {
		err := fmt.Errorf("enableEvm change requires a process restart (running=%v, new=%v)", f.cfg.EnableEvm, newCfg.EnableEvm)
		slog.Warn("config reload rejected", "error", err)
		f.dashboard.RecordReload(false, err.Error(), nil)
		return
	}
	// The Authenticator (identities, methods, defaultRequire, replay
	// store, JWKS refreshers) is built ONCE at startup and wired into
	// every proxy/handler. A reload swaps f.cfg but does not rebuild it,
	// so accepting auth edits here would report success while still
	// authenticating against the OLD identities/keys — a silent key-
	// rotation / revocation failure. Treat the global auth block as
	// runtime-immutable and reject the change (per-rule `auth:` on
	// individual rules still hot-reloads, since it lives in the rules).
	if !reflect.DeepEqual(f.cfg.Auth, newCfg.Auth) {
		err := fmt.Errorf("auth config change requires a process restart")
		slog.Warn("config reload rejected", "error", err)
		f.dashboard.RecordReload(false, err.Error(), nil)
		return
	}
	// Upstream topology (nodes:) is wired ONCE at startup: the LCD/RPC/
	// gRPC/EVM pools are built from cfg.Nodes in New() and a reload only
	// calls SetRules — it never rebuilds pools. Accepting a nodes: edit
	// would report success while traffic kept flowing to the OLD upstream
	// set. Reject as restart-required (runtime pod-IP churn for a node is
	// handled separately by the DNS discovery reconciler, which mutates
	// pools in place — that path is unaffected by this guard because it
	// doesn't go through config reload).
	if !reflect.DeepEqual(f.origNodes, newCfg.Nodes) {
		err := fmt.Errorf("nodes (upstream topology) change requires a process restart")
		slog.Warn("config reload rejected", "error", err)
		f.dashboard.RecordReload(false, err.Error(), nil)
		return
	}
	before := f.ruleFingerprintsLocked()
	f.cfg = newCfg
	f.applyRulesLocked()
	// Pick up dashboard.requestLog edits without a pod restart.
	// requestLog's state pointer is swapped atomically — the next
	// Record sees the new enable / maxAge / maxEntries values.
	if f.requestLog != nil {
		f.requestLog.ApplyConfig(f.cfg.Dashboard.RequestLog)
	}
	after := f.ruleFingerprintsLocked()
	f.dashboard.RecordReload(true, "", reloadDelta(before, after))
}

// cacheTopologyChange reports whether the cache section between two configs
// is identical for runtime purposes. Returns (description, false) when a
// runtime-immutable field differs so tryReload can refuse the reload and
// surface a clear reason; returns ("", true) when the cache sections match.
//
// The cache section is treated as a single immutable bundle because:
//
//   - cache.backend selects the whole rate-limiter + cache implementation;
//     swapping it mid-flight would orphan all in-flight buckets and reroute
//     subsequent traffic against a different distributed-state surface.
//   - cache.cluster controls the embedded olric daemon's bind address, ports,
//     replication factor, and discovery — all bound at startup by
//     clusterRuntime and unchangeable without recreating the daemon.
//   - cache.redis / redis-sentinel feed connection pools constructed once
//     at cosmoguard.New; per-rule limiters built during reload reuse the
//     proxies' captured *CacheGlobalConfig pointer, so a URL change here
//     never reaches the connector.
//   - global cache.ttl + cache.key are baked into per-rule cache compilation
//     at startup; reload sees the live rules' already-resolved values.
//
// reflect.DeepEqual on the whole CacheGlobalConfig is the simplest
// expression of "literally nothing under cache: may change" — and because
// both old and new come through PrepareConfig + defaults.Set, defaulted
// fields compare equal even when only one side spelled them out in YAML.
func cacheTopologyChange(oldCfg, newCfg *Config) (string, bool) {
	if oldCfg == nil || newCfg == nil {
		return "", true
	}
	if reflect.DeepEqual(oldCfg.Cache, newCfg.Cache) {
		return "", true
	}
	// Name the embedded↔networked flip specifically so the dashboard
	// row points the operator at the right knob; other cache field
	// differences collapse to a generic message.
	oldCluster := oldCfg.Cache.Cluster != nil
	newCluster := newCfg.Cache.Cluster != nil
	if oldCluster != newCluster {
		direction := "added"
		if oldCluster {
			direction = "removed"
		}
		return fmt.Sprintf("cache.cluster block was %s (embedded ↔ networked toggle requires restart)", direction), false
	}
	return "one or more cache.* fields changed (cluster topology, global ttl, key salt)", false
}

// ruleFingerprintsLocked returns the current per-section list of rule
// fingerprints. Caller must hold f.configMutex. Used by tryReload to
// capture the before/after deltas surfaced through
// /api/v1/reload-status; the fingerprint already encodes priority +
// action + match shape + cache/rate-limit/auth config (see rules.go
// httpRuleFingerprint and friends), so an in-place TTL edit shifts
// the fingerprint and shows up as a Modified row even though the
// rule count is unchanged.
//
// Returns the slice rather than a count so reloadDelta can compute
// added/removed/modified via a multiset diff. Sections with no rules
// still appear with an empty slice so a section that goes N→0 (or
// 0→N) is visible in the delta.
func (f *CosmoGuard) ruleFingerprintsLocked() map[string][]uint64 {
	out := map[string][]uint64{}
	if f.cfg == nil {
		return out
	}
	out["lcd"] = httpRuleFingerprints(f.cfg.LCD.Rules)
	out["rpc"] = httpRuleFingerprints(f.cfg.RPC.Rules)
	out["rpc.jsonrpc"] = jsonRpcRuleFingerprints(f.cfg.RPC.JsonRpc.Rules)
	out["grpc"] = grpcRuleFingerprints(f.cfg.GRPC.Rules)
	if f.cfg.EnableEvm {
		out["evm.rpc"] = jsonRpcRuleFingerprints(f.cfg.EVM.RPC.Rules)
		out["evm.rpc.httpRules"] = httpRuleFingerprints(f.cfg.EVM.RPC.HttpRules)
		out["evm.ws"] = jsonRpcRuleFingerprints(f.cfg.EVM.WS.Rules)
	}
	return out
}

func httpRuleFingerprints(rs []*HttpRule) []uint64 {
	out := make([]uint64, 0, len(rs))
	for _, r := range rs {
		if r != nil {
			out = append(out, r.Fingerprint)
		}
	}
	return out
}

func jsonRpcRuleFingerprints(rs []*JsonRpcRule) []uint64 {
	out := make([]uint64, 0, len(rs))
	for _, r := range rs {
		if r != nil {
			out = append(out, r.Fingerprint)
		}
	}
	return out
}

func grpcRuleFingerprints(rs []*GrpcRule) []uint64 {
	out := make([]uint64, 0, len(rs))
	for _, r := range rs {
		if r != nil {
			out = append(out, r.Fingerprint)
		}
	}
	return out
}

// reloadDelta zips two per-section fingerprint maps into the
// ReloadSection shape the dashboard renders. Added/Removed/Modified
// are derived from a multiset diff: fingerprints that appear in
// `after` but not `before` (and vice-versa) feed the raw add/remove
// counts; we treat min(added_fp, removed_fp) as in-place
// modifications so a one-rule TTL edit shows up as Modified=1
// instead of "+1 added, -1 removed". The unmatched remainder stays
// as pure additions or removals.
//
// Sections present in either map appear in the output, so a section
// that goes N→0 (or 0→N) still shows up.
func reloadDelta(before, after map[string][]uint64) map[string]ReloadSection {
	out := map[string]ReloadSection{}
	for k, b := range before {
		out[k] = sectionDelta(b, after[k])
	}
	for k, a := range after {
		if _, ok := out[k]; !ok {
			out[k] = sectionDelta(nil, a)
		}
	}
	return out
}

func sectionDelta(before, after []uint64) ReloadSection {
	sec := ReloadSection{Before: len(before), After: len(after)}
	// Multiset diff: count occurrences on each side, then for every
	// fingerprint compute the per-side surplus. The sum of surpluses
	// on each side is the raw added/removed count; the pairwise
	// minimum tags in-place modifications.
	counts := map[uint64]int{}
	for _, fp := range before {
		counts[fp]--
	}
	for _, fp := range after {
		counts[fp]++
	}
	addedRaw, removedRaw := 0, 0
	for _, c := range counts {
		if c > 0 {
			addedRaw += c
		} else if c < 0 {
			removedRaw += -c
		}
	}
	modified := addedRaw
	if removedRaw < modified {
		modified = removedRaw
	}
	sec.Modified = modified
	sec.Added = addedRaw - modified
	sec.Removed = removedRaw - modified
	return sec
}

// applyRules acquires configMutex and pushes the current cfg's rules into
// each proxy. Use applyRulesLocked when the caller already holds the mutex
// (as tryReload does to keep swap + apply atomic).
func (f *CosmoGuard) applyRules() {
	f.configMutex.Lock()
	defer f.configMutex.Unlock()
	f.applyRulesLocked()
}

func (f *CosmoGuard) applyRulesLocked() {
	slog.Info("applying cosmoguard rules")

	// Rules for LCD
	slog.Debug("applying LCD cosmoguard rules", "default", f.cfg.LCD.Default)
	f.lcdProxy.SetRules(f.cfg.LCD.Rules, f.cfg.LCD.Default)

	// Rules for gRPC
	slog.Debug("applying gRPC cosmoguard rules", "default", f.cfg.GRPC.Default)
	f.grpcProxy.SetRules(f.cfg.GRPC.Rules, f.cfg.GRPC.Default)

	// Rules for RPC (and jsonrpc)
	slog.Debug("applying RPC cosmoguard rules", "default", f.cfg.RPC.Default)
	f.rpcProxy.SetRules(f.cfg.RPC.Rules, f.cfg.RPC.Default)
	slog.Debug("applying JSONRPC cosmoguard rules", "default", f.cfg.RPC.JsonRpc.Default)
	f.jsonRpcHandler.SetRules(f.cfg.RPC.JsonRpc.Rules, f.cfg.RPC.JsonRpc.Default)

	if f.cfg.EnableEvm {
		// Rules for EVM RPC
		slog.Debug("applying EVM-RPC cosmoguard rules", "default", f.cfg.EVM.RPC.Default)
		f.evmRpcProxy.SetRules(f.cfg.EVM.RPC.HttpRules, f.cfg.EVM.RPC.Default)
		f.evmJsonRpcHandler.SetRules(f.cfg.EVM.RPC.Rules, f.cfg.EVM.RPC.Default)

		// Rules for EVM RPC WS
		slog.Debug("applying EVM-RPC-WS cosmoguard rules", "default", f.cfg.EVM.WS.Default)
		f.evmJsonRpcWsHandler.SetRules(f.cfg.EVM.WS.Rules, f.cfg.EVM.WS.Default)
	}
}

// countHealthyUpstreams returns how many nodes are healthy on EVERY
// served surface (LCD AND RPC), matching /readyz's gate.
//
// The previous shape did a UNION (healthy in LCD *or* RPC), which
// disagreed with /readyz's INTERSECTION (healthy in LCD *and* RPC):
// a node with healthy LCD but failed RPC counted as "healthy" in
// /info but flipped /readyz to 503. Operators staring at the
// dashboard saw "3/3 healthy" while Kubernetes took the pod out of
// rotation. Now /info and /readyz can never disagree.
//
// Dedupe by upstream name handles the case where a node is
// configured into both pools (the common shape).
func (f *CosmoGuard) countHealthyUpstreams() int {
	// Per-name tracking of "this upstream appears in pool X and is OK
	// there". A node counts as healthy iff every pool it appears in
	// reports OK; pools the node isn't configured into don't gate it.
	// Covers LCD + RPC + gRPC + EVM-RPC + EVM-RPC-WS so a deployment
	// that only serves gRPC isn't silently reported as 0/0.
	type surfaces struct {
		surfaces int // bitmask of pools this name appears in
		ok       int // bitmask of pools where it's currently healthy
	}
	state := map[string]*surfaces{}
	addHTTP := func(p *HttpProxy, bit int) {
		if p == nil || p.pool == nil {
			return
		}
		for _, u := range p.pool.Upstreams() {
			s := state[u.Name]
			if s == nil {
				s = &surfaces{}
				state[u.Name] = s
			}
			s.surfaces |= bit
			if u.healthy.Load() {
				s.ok |= bit
			}
		}
	}
	addGRPC := func(p *GrpcProxy, bit int) {
		if p == nil || p.pool == nil {
			return
		}
		for _, u := range p.pool.Upstreams() {
			s := state[u.Name]
			if s == nil {
				s = &surfaces{}
				state[u.Name] = s
			}
			s.surfaces |= bit
			if u.Healthy() {
				s.ok |= bit
			}
		}
	}
	const (
		bitLCD = 1 << iota
		bitRPC
		bitGRPC
		bitEVM
		bitEVMWS
	)
	addHTTP(f.lcdProxy, bitLCD)
	addHTTP(f.rpcProxy, bitRPC)
	addGRPC(f.grpcProxy, bitGRPC)
	addHTTP(f.evmRpcProxy, bitEVM)
	addHTTP(f.evmRpcWsProxy, bitEVMWS)
	n := 0
	for _, s := range state {
		if s.ok == s.surfaces {
			n++
		}
	}
	return n
}

// countUpstreams returns the live distinct-upstream count across all
// served pools (LCD + RPC + gRPC + EVM-RPC + EVM-RPC-WS) — the count
// denominator that pairs with countHealthyUpstreams. Reads the pools'
// live snapshots so discovery-driven add/remove is reflected
// immediately on /info. Dedupe by upstream name keeps a node
// configured into multiple service pools from being counted more
// than once.
func (f *CosmoGuard) countUpstreams() int {
	seen := map[string]struct{}{}
	addHTTP := func(p *HttpProxy) {
		if p == nil || p.pool == nil {
			return
		}
		for _, u := range p.pool.Upstreams() {
			seen[u.Name] = struct{}{}
		}
	}
	addHTTP(f.lcdProxy)
	addHTTP(f.rpcProxy)
	addHTTP(f.evmRpcProxy)
	addHTTP(f.evmRpcWsProxy)
	if f.grpcProxy != nil && f.grpcProxy.pool != nil {
		for _, u := range f.grpcProxy.pool.Upstreams() {
			seen[u.Name] = struct{}{}
		}
	}
	return len(seen)
}

// allPoolsReady reports whether every CONFIGURED pool has at least
// one healthy member. A deployment that only ever wires the gRPC
// proxy must still gate /readyz on gRPC's pool — the previous shape
// only checked LCD + RPC and returned 200 for an all-unhealthy
// gRPC-only deployment.
func (f *CosmoGuard) allPoolsReady() bool {
	check := func(ok bool) bool { return ok }
	if f.lcdProxy != nil && f.lcdProxy.pool != nil {
		if !check(f.lcdProxy.pool.AnyHealthy()) {
			return false
		}
	}
	if f.rpcProxy != nil && f.rpcProxy.pool != nil {
		if !check(f.rpcProxy.pool.AnyHealthy()) {
			return false
		}
	}
	if f.grpcProxy != nil && f.grpcProxy.pool != nil {
		if !check(f.grpcProxy.pool.AnyHealthy()) {
			return false
		}
	}
	if f.evmRpcProxy != nil && f.evmRpcProxy.pool != nil {
		if !check(f.evmRpcProxy.pool.AnyHealthy()) {
			return false
		}
	}
	if f.evmRpcWsProxy != nil && f.evmRpcWsProxy.pool != nil {
		if !check(f.evmRpcWsProxy.pool.AnyHealthy()) {
			return false
		}
	}
	return true
}

// LcdPool returns the LCD proxy's upstream pool (for test inspection of
// healthcheck state). nil before construction.
func (f *CosmoGuard) LcdPool() *HttpUpstreamPool {
	if f.lcdProxy == nil {
		return nil
	}
	return f.lcdProxy.pool
}

// RpcPool returns the RPC proxy's upstream pool.
func (f *CosmoGuard) RpcPool() *HttpUpstreamPool {
	if f.rpcProxy == nil {
		return nil
	}
	return f.rpcProxy.pool
}

// MetricsPort returns the port the metrics + ops (/healthz, /readyz) server
// listens on. Returns 0 when metrics are disabled. Routes through
// snapshotConfig so a concurrent tryReload swap doesn't race the
// f.cfg pointer read.
func (f *CosmoGuard) MetricsPort() int {
	cfg := f.snapshotConfig()
	if cfg == nil {
		return 0
	}
	if !cfg.Metrics.Enable {
		return 0
	}
	return cfg.Metrics.Port
}

// DashboardPort returns the port the read-only dashboard listens on.
// Returns 0 when the dashboard is disabled. snapshotConfig protects
// the read from a concurrent reload pointer swap.
func (f *CosmoGuard) DashboardPort() int {
	cfg := f.snapshotConfig()
	if cfg == nil {
		return 0
	}
	if !cfg.Dashboard.IsEnabled() {
		return 0
	}
	return cfg.Dashboard.Port
}

// snapshotConfig returns the current config pointer captured under
// configMutex. The dashboard handlers read cfg fields (rules,
// identities) while tryReload swaps the pointer; without this
// snapshot, a request landing during a reload races on the slice
// header read of (e.g.) cfg.LCD.Rules. The returned pointer is
// stable for the caller's lifetime — subsequent reloads swap the
// CosmoGuard.cfg pointer, but the snapshot still points at the
// previous *Config until the caller drops it. That's the same
// snapshot semantics applyRulesLocked relies on.
func (f *CosmoGuard) snapshotConfig() *Config {
	f.configMutex.Lock()
	defer f.configMutex.Unlock()
	return f.cfg
}

// Shutdown stops every proxy and the metrics server, releasing all listeners.
// Drains in-flight requests via each proxy's Shutdown(ctx), force-closes on
// deadline expiry, then tears down the upstream pools, caches, rate
// limiters, auth, and tracing exporter in dependency order. The signal
// handler in cmd/cosmoguard wires Shutdown into SIGTERM/SIGINT with a
// configurable grace window.
//
// Safe to call multiple times. Returns the first non-nil error encountered.
func (f *CosmoGuard) Shutdown(ctx context.Context) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Wake the Run() goroutine for cfg-file-less instances (which
	// would otherwise park on `<-runDone` forever) and let
	// double-Shutdown stay safe via the sync.Once.
	if f.runDone != nil {
		f.runDoneOnce.Do(func() { close(f.runDone) })
	}
	// Close the config watcher (if WatchConfigFile is running). Closing
	// the watcher closes its event channel, which lets the WatchConfigFile
	// goroutine exit instead of blocking forever on the next receive.
	// Swap to nil so a second Shutdown call (documented safe) doesn't
	// double-close the underlying inotify FD.
	if w := f.configWatcher.Swap(nil); w != nil {
		record(w.Close())
	}
	// Stop the DNS reconciler before tearing down proxies so a
	// reconcile in flight doesn't try to call AddUpstream on a pool
	// that's about to close its conns.
	if f.discovery != nil {
		f.discovery.Stop()
	}
	if f.metricsServer != nil {
		record(f.metricsServer.Shutdown(ctx))
	}
	if f.dashboardServer != nil {
		record(f.dashboardServer.Shutdown(ctx))
	}
	if f.peerApiServer != nil {
		record(f.peerApiServer.Shutdown(ctx))
	}
	if f.lcdProxy != nil {
		record(f.lcdProxy.Shutdown(ctx))
	}
	if f.rpcProxy != nil {
		record(f.rpcProxy.Shutdown(ctx))
	}
	if f.grpcProxy != nil {
		record(f.grpcProxy.Shutdown(ctx))
	}
	if f.evmRpcProxy != nil {
		record(f.evmRpcProxy.Shutdown(ctx))
	}
	if f.evmRpcWsProxy != nil {
		record(f.evmRpcWsProxy.Shutdown(ctx))
	}
	// JSON-RPC handlers own their own cache (separate from the HTTP proxy
	// cache); reap their goroutines too.
	if f.jsonRpcHandler != nil {
		record(f.jsonRpcHandler.Shutdown())
	}
	if f.evmJsonRpcHandler != nil {
		record(f.evmJsonRpcHandler.Shutdown())
	}
	if f.evmJsonRpcWsHandler != nil {
		record(f.evmJsonRpcWsHandler.Shutdown())
	}
	// Replay store (if configured) owns its own goroutine / Redis pool.
	if f.auth != nil {
		record(f.auth.Close())
	}
	// Flush in-flight spans + close the OTLP exporter. No-op when
	// tracing was never set up. Use a fresh 5s deadline rather than
	// the caller's ctx: by the time we reach this line the proxy
	// drain has typically consumed most or all of the operator's
	// shutdownGrace, and a cancelled flush silently drops buffered
	// spans — exactly the symptom operators chase under "shutdown
	// lost the last spans I needed".
	if f.tracingShutdown != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record(f.tracingShutdown(flushCtx))
		cancel()
	}
	// Flush the final observability snapshot to the DMap BEFORE we
	// tear down the olric runtime — otherwise the last 30s of
	// counters (often the most interesting ones during a planned
	// rollout) would be lost. Use a fresh 5s deadline rather than
	// the caller's ctx for the same reason the tracing flush above
	// does: by the time we reach this line the proxy drain has
	// typically consumed most of the operator's shutdownGrace, and
	// a cancelled flush silently drops the snapshot — exactly the
	// data peers need to seed the restarting pod's dashboard.
	if f.obsReplicator != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record(f.obsReplicator.Close(flushCtx))
		cancel()
	}
	// Stop the in-process olric daemon last: every other consumer
	// (proxies, JSON-RPC handlers, auth) had a chance to drain
	// in-flight work before we yank the embedded client out from
	// under them. Same fresh-deadline reasoning as the obs flush and
	// tracing exporter above: a cancelled ctx makes olric.Shutdown
	// return immediately and skip the polite memberlist leave, so
	// peers wait the full failure-detector timeout before redirecting
	// traffic away from this pod. 5s is well past LeaveTimeout=500ms,
	// so the cap doesn't cost a healthy shutdown anything.
	if f.cluster != nil {
		clusterCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record(f.cluster.Close(clusterCtx))
		cancel()
	}
	return firstErr
}
