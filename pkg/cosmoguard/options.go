package cosmoguard

import "github.com/olric-data/olric"

type SharedOptions struct {
	CacheConfig    *CacheGlobalConfig
	ServerConfig   *ServerConfig
	Authenticator  *Authenticator
	MetricsEnabled bool
	// CacheBudget is the per-instance memory budget for this proxy's
	// response cache, already divided across the enabled caches by New().
	// Zero-value means unbounded (the test/programmatic path).
	CacheBudget CacheBudget
	// OlricClient is the in-process olric handle used by the v4
	// cluster-shared rate limiter (cache.backend=olric, the default).
	// nil when no clusterRuntime is wired (tests, programmatic
	// embedders) — NewRateLimiter falls back to in-memory in that case.
	OlricClient *olric.EmbeddedClient
}

func DefaultSharedOptions() *SharedOptions {
	return &SharedOptions{
		MetricsEnabled: true,
	}
}

type HttpProxyOptions struct {
	*SharedOptions
	EndpointHandlers []*httpProxyEndpointHandler
	CORSConfig       *CORSConfig
	UpstreamConfig   *UpstreamConfig
}

func DefaultHttpProxyOptions() *HttpProxyOptions {
	cfg := &HttpProxyOptions{
		EndpointHandlers: make([]*httpProxyEndpointHandler, 0),
	}
	cfg.SharedOptions = DefaultSharedOptions()
	return cfg
}

type GrpcProxyOptions struct {
	*SharedOptions
}

func DefaultGrpcProxyOptions() *GrpcProxyOptions {
	cfg := &GrpcProxyOptions{}
	cfg.SharedOptions = DefaultSharedOptions()
	return cfg
}

type JsonRpcHandlerOptions struct {
	*SharedOptions
	CORSConfig *CORSConfig
	// WebsocketBackend is the single-backend form (v3 compat). Used only
	// when WebsocketBackends is empty.
	WebsocketBackend string
	// WebsocketBackends is the v4 multi-upstream form. When non-empty,
	// the broker spreads its connection pool across all listed backends
	// in even round-robin shares.
	WebsocketBackends    []string
	WebsocketEnabled     bool
	WebsocketConnections int
	WebsocketPath        string
	UpstreamConstructor  UpstreamConnManagerConstructor
	MaxBatchSize         int
}

func DefaultJsonRpcHandlerOptions() *JsonRpcHandlerOptions {
	cfg := &JsonRpcHandlerOptions{
		WebsocketBackend:     "localhost:26657",
		WebsocketEnabled:     true,
		WebsocketConnections: 10,
		WebsocketPath:        defaultWebsocketPath,
		UpstreamConstructor:  CosmosUpstreamConnManager,
	}
	cfg.SharedOptions = DefaultSharedOptions()
	return cfg
}

type Option[T any] func(*T)

func WithCacheConfig[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions | GrpcProxyOptions](c *CacheGlobalConfig) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.CacheConfig = c
		case *HttpProxyOptions:
			x.CacheConfig = c
		case *JsonRpcHandlerOptions:
			x.CacheConfig = c
		case *GrpcProxyOptions:
			x.CacheConfig = c
		default:
			panic("unexpected use")
		}
	}
}

// WithCacheBudget threads the per-instance response-cache memory budget
// (issue #15) into a proxy. New() resolves the total budget from the pod's
// memory limit and divides it across the enabled caches before passing each
// share here. The zero value leaves the cache unbounded (test path).
func WithCacheBudget[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions | GrpcProxyOptions](b CacheBudget) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.CacheBudget = b
		case *HttpProxyOptions:
			x.CacheBudget = b
		case *JsonRpcHandlerOptions:
			x.CacheBudget = b
		case *GrpcProxyOptions:
			x.CacheBudget = b
		default:
			panic("unexpected use")
		}
	}
}

// WithOlricClient threads the cluster runtime's in-process olric handle
// into proxies that build rate limiters. nil disables the cluster-shared
// limiter — useful for tests that don't spin up clusterRuntime.
func WithOlricClient[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions | GrpcProxyOptions](c *olric.EmbeddedClient) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.OlricClient = c
		case *HttpProxyOptions:
			x.OlricClient = c
		case *JsonRpcHandlerOptions:
			x.OlricClient = c
		case *GrpcProxyOptions:
			x.OlricClient = c
		default:
			panic("unexpected use")
		}
	}
}

func WithMetricsEnabled[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions | GrpcProxyOptions](b bool) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.MetricsEnabled = b
		case *HttpProxyOptions:
			x.MetricsEnabled = b
		case *JsonRpcHandlerOptions:
			x.MetricsEnabled = b
		case *GrpcProxyOptions:
			x.MetricsEnabled = b
		default:
			panic("unexpected use")
		}
	}
}

// WithServerConfig threads HTTP/WS server hardening knobs (timeouts, body
// caps) into proxies that bind a listener. nil leaves the proxy with Go's
// stdlib defaults.
func WithServerConfig[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions](c *ServerConfig) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.ServerConfig = c
		case *HttpProxyOptions:
			x.ServerConfig = c
		case *JsonRpcHandlerOptions:
			x.ServerConfig = c
		default:
			panic("unexpected use")
		}
	}
}

// WithAuthenticator threads a shared *Authenticator through proxies so all
// of them resolve identities and strip credential headers consistently.
// nil leaves auth disabled.
func WithAuthenticator[T SharedOptions | HttpProxyOptions | JsonRpcHandlerOptions | GrpcProxyOptions](a *Authenticator) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *SharedOptions:
			x.Authenticator = a
		case *HttpProxyOptions:
			x.Authenticator = a
		case *JsonRpcHandlerOptions:
			x.Authenticator = a
		case *GrpcProxyOptions:
			x.Authenticator = a
		default:
			panic("unexpected use")
		}
	}
}

// WithCORSConfig threads the CORS policy into HTTP-facing handlers. nil leaves
// Cosmoguard's CORS layer disabled.
func WithCORSConfig[T HttpProxyOptions | JsonRpcHandlerOptions](c *CORSConfig) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *HttpProxyOptions:
			x.CORSConfig = c
		case *JsonRpcHandlerOptions:
			x.CORSConfig = c
		default:
			panic("unexpected use")
		}
	}
}

// WithUpstreamConfig threads pool-level upstream behavior (picker strategy
// and retry budget) into a HttpProxy. nil leaves both at their defaults
// (weighted-round-robin, 2 retries).
func WithUpstreamConfig[T HttpProxyOptions](c *UpstreamConfig) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *HttpProxyOptions:
			x.UpstreamConfig = c
		default:
			panic("unexpected use")
		}
	}
}

func WithEndpointHandler[T HttpProxyOptions](endpoints []Endpoint, handler EndpointHandler) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *HttpProxyOptions:
			x.EndpointHandlers = append(x.EndpointHandlers, &httpProxyEndpointHandler{
				Endpoints: endpoints,
				Handler:   handler,
			})
		default:
			panic("unexpected use")
		}
	}
}

func WithWebSocketBackend[T JsonRpcHandlerOptions](backend string) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.WebsocketBackend = backend
		default:
			panic("unexpected use")
		}
	}
}

// WithWebSocketBackends supplies the multi-upstream WS backend list. When
// set, the broker spreads its connection pool evenly across all backends;
// each long-lived client connection sticks to the upstream it landed on.
func WithWebSocketBackends[T JsonRpcHandlerOptions](backends []string) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.WebsocketBackends = backends
		default:
			panic("unexpected use")
		}
	}
}

func WithWebSocketEnabled[T JsonRpcHandlerOptions](enabled bool) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.WebsocketEnabled = enabled
		default:
			panic("unexpected use")
		}
	}
}

func WithWebSocketConnections[T JsonRpcHandlerOptions](v int) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.WebsocketConnections = v
		default:
			panic("unexpected use")
		}
	}
}

func WithWebSocketPath[T JsonRpcHandlerOptions](path string) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.WebsocketPath = path
		default:
			panic("unexpected use")
		}
	}
}

func WithUpstreamManager[T JsonRpcHandlerOptions](constructor UpstreamConnManagerConstructor) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.UpstreamConstructor = constructor
		default:
			panic("unexpected use")
		}
	}
}

// WithMaxBatchSize caps the number of requests a JSON-RPC batch may contain.
// 0 disables the cap.
func WithMaxBatchSize[T JsonRpcHandlerOptions](n int) Option[T] {
	return func(opts *T) {
		switch x := any(opts).(type) {
		case *JsonRpcHandlerOptions:
			x.MaxBatchSize = n
		default:
			panic("unexpected use")
		}
	}
}
