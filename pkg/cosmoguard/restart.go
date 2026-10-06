package cosmoguard

import (
	"crypto/sha256"
	stdjson "encoding/json"
	"fmt"
	"reflect"
	"time"
)

// restartConfig contains only startup-captured declarations. Effective values
// and empty-slice normalization preserve the reload policy's equivalence classes.
type restartConfig struct {
	Cache     CacheGlobalConfig
	EnableEvm bool
	Auth      restartAuth
	Nodes     []NodeConfig
	CORS      restartCORS
	Server    restartServer
	Dashboard restartDashboard
	Metrics   restartMetrics
	WebSocket restartWebSocket
	GRPC      restartGRPC
}

type restartAuth struct {
	Declarations        AuthConfig
	ValidUntil          []*restartTimestamp
	AnonymousValidUntil *restartTimestamp
}

type restartTimestamp struct {
	Seconds             int64
	Nanoseconds, Offset int
	UTC                 bool
}

// YAML timestamps declare an instant, an offset and UTC versus numeric-zone
// spelling. Local's cached timezone tables are process state, not declarations.
func timestampRestartProjection(value *time.Time) *restartTimestamp {
	if value == nil {
		return nil
	}
	_, offset := value.Zone()
	return &restartTimestamp{value.Unix(), value.Nanosecond(), offset, value.Location() == time.UTC}
}

func authRestartProjection(cfg AuthConfig) restartAuth {
	projection := restartAuth{Declarations: cfg}
	if cfg.Identities != nil {
		projection.Declarations.Identities = make([]IdentityConfig, len(cfg.Identities))
		copy(projection.Declarations.Identities, cfg.Identities)
		projection.ValidUntil = make([]*restartTimestamp, len(cfg.Identities))
		for i, identity := range cfg.Identities {
			projection.ValidUntil[i] = timestampRestartProjection(identity.ValidUntil)
			projection.Declarations.Identities[i].ValidUntil = nil
		}
	}
	if cfg.Anonymous != nil {
		anonymous := *cfg.Anonymous
		projection.AnonymousValidUntil = timestampRestartProjection(anonymous.ValidUntil)
		anonymous.ValidUntil = nil
		projection.Declarations.Anonymous = &anonymous
	}
	return projection
}

type restartCORS struct {
	Enable, Credentials                                           bool
	MaxAge                                                        time.Duration
	AllowedOrigins, AllowedMethods, AllowedHeaders, ExposeHeaders []string
}

type restartServer struct {
	ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	MaxRequestBody, WSReadLimit                               int64
	WebSocketLimits                                           WebSocketLimits
	WSAllowedOrigins                                          []string
}

type restartDashboard struct {
	Enable                           bool
	Port                             int
	BasicAuthUser, BasicAuthPassword string
	ClusterHistoryRestore            bool
}

type restartMetrics struct {
	Enable bool
	Port   int
	WebUI  WebUIConfig
}

type restartWebSocket struct {
	Enable                         bool
	RPCConnections, EVMConnections int
}

type restartGRPC struct{ MaxRecvMsgSize, MaxSendMsgSize int }

func restartProjection(cfg *Config) restartConfig {
	cors := restartCORS{
		Enable: cfg.CORS.Enable, Credentials: cfg.CORS.Credentials, MaxAge: cfg.CORS.MaxAge,
		AllowedOrigins: restartStrings(cfg.CORS.AllowedOrigins),
		AllowedMethods: restartStrings(cfg.CORS.AllowedMethods),
		AllowedHeaders: restartStrings(cfg.CORS.AllowedHeaders),
		ExposeHeaders:  restartStrings(cfg.CORS.ExposeHeaders),
	}
	return restartConfig{
		Cache: cfg.Cache, EnableEvm: cfg.EnableEvm, Auth: authRestartProjection(cfg.Auth), Nodes: cfg.Nodes, CORS: cors,
		Server:    serverRestartProjection(&cfg.Server),
		Dashboard: dashboardRestartProjection(&cfg.Dashboard),
		Metrics:   restartMetrics{cfg.Metrics.IsEnabled(), cfg.Metrics.Port, cfg.Metrics.WebUI},
		WebSocket: restartWebSocket{cfg.RPC.WebSocketIsEnabled(), cfg.RPC.WebSocketConnections, cfg.EVM.WS.WebSocketConnections},
		GRPC:      restartGRPC{cfg.GRPC.MaxRecvMsgSize, cfg.GRPC.MaxSendMsgSize},
	}
}

func restartStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}

func serverRestartProjection(cfg *ServerConfig) restartServer {
	return restartServer{cfg.ReadHeaderTimeout, cfg.ReadTimeout, cfg.WriteTimeout, cfg.IdleTimeout,
		cfg.EffectiveMaxRequestBody(), cfg.EffectiveWSReadLimit(), cfg.EffectiveWebSocketLimits(), restartStrings(cfg.WSAllowedOrigins)}
}

func dashboardRestartProjection(cfg *DashboardConfig) restartDashboard {
	return restartDashboard{cfg.Enable != nil && *cfg.Enable, cfg.Port, cfg.BasicAuthUser, cfg.BasicAuthPassword, cfg.ClusterHistoryRestoreEnabled()}
}

// RequiresRestart compares non-nil prepared declarative configs, before runtime
// DNS expansion. It does not mutate them. The reason is the first reload rejection
// message, or empty when hot reload is permitted.
func RequiresRestart(previous, next *Config) (bool, string) {
	old, new := restartProjection(previous), restartProjection(next)
	if !reflect.DeepEqual(old.Cache, new.Cache) {
		detail := "one or more cache.* fields changed (cluster topology, global ttl, key salt)"
		if (old.Cache.Cluster == nil) != (new.Cache.Cluster == nil) {
			direction := "added"
			if old.Cache.Cluster != nil {
				direction = "removed"
			}
			detail = fmt.Sprintf("cache.cluster block was %s (embedded ↔ networked toggle requires restart)", direction)
		}
		return true, "cache topology change requires a process restart: " + detail
	}
	if old.EnableEvm != new.EnableEvm {
		return true, fmt.Sprintf("enableEvm change requires a process restart (running=%v, new=%v)", old.EnableEvm, new.EnableEvm)
	}
	checks := []struct {
		old, new any
		reason   string
	}{
		{old.Auth, new.Auth, "auth config change requires a process restart"},
		{old.Nodes, new.Nodes, "nodes (upstream topology) change requires a process restart"},
		{old.CORS, new.CORS, "cors config change requires a process restart"},
		{old.Server, new.Server, "server config change (timeouts / maxRequestBody / wsReadLimit / websocketLimits / wsAllowedOrigins) requires a process restart"},
		{old.Dashboard, new.Dashboard, "dashboard config change (enable / port / basicAuth / clusterHistoryRestore) requires a process restart"},
		{old.Metrics, new.Metrics, "metrics config change (enable / port / webUI) requires a process restart"},
		{old.WebSocket, new.WebSocket, "websocket config change (webSocketEnabled / webSocketConnections) requires a process restart"},
		{old.GRPC, new.GRPC, "grpc message size change (maxRecvMsgSize / maxSendMsgSize) requires a process restart"},
	}
	for _, check := range checks {
		if !reflect.DeepEqual(check.old, check.new) {
			return true, check.reason
		}
	}
	return false, ""
}

// RestartFingerprint returns a versioned SHA-256 digest of the same declarations
// RequiresRestart compares. cfg must be a non-nil prepared declarative config,
// before runtime DNS expansion. No environment or runtime services are accessed.
// Fingerprints are comparable only when produced by the same module version.
func RestartFingerprint(cfg *Config) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("restart fingerprint: nil config")
	}
	value, err := restartEncoding(reflect.ValueOf(restartProjection(cfg)))
	if err != nil {
		return "", fmt.Errorf("restart fingerprint: %w", err)
	}
	raw, err := stdjson.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("restart fingerprint: %w", err)
	}
	return fmt.Sprintf("v1:%x", sha256.Sum256(raw)), nil
}

// Structural encoding preserves nil versus empty slices and arbitrary string
// bytes. Pointer addresses and marshaler methods never enter the hash.
func restartEncoding(value reflect.Value) (any, error) {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return nil, nil
		}
		return restartEncoding(value.Elem())
	case reflect.Struct, reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return nil, nil
		}
		length := value.Len
		field := value.Index
		if value.Kind() == reflect.Struct {
			length = value.NumField
			field = value.Field
		}
		items := make([]any, length())
		for i := range items {
			item, err := restartEncoding(field(i))
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	case reflect.String:
		// JSON strings replace invalid UTF-8; env overrides can contain arbitrary bytes.
		return []byte(value.String()), nil
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint(), nil
	case reflect.Float32, reflect.Float64:
		f := value.Float()
		if f == 0 {
			return float64(0), nil
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unsupported restart value type %s", value.Type())
	}
}
