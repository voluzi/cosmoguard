package cosmoguard

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// applyEnvOverrides overlays selected COSMOGUARD_* env vars onto an
// already-parsed Config. The goal is to let an external operator —
// cosmopilot, a Helm chart, plain `docker run -e` — inject the
// per-deployment plumbing (target host, ports, K8s discovery service
// name, listener ports) without rewriting the YAML the user owns
// (rules, cache, auth).
//
// Precedence is env > YAML > struct-tag default. The override step
// runs inside PrepareConfig after defaults.Set and after the v3→v4
// node promotion, but BEFORE per-node validation — so a malformed env
// value (bad URL scheme, conflicting discovery + static host, …)
// fails startup the same way a bad YAML value would.
//
// Node-scope overrides target cfg.Nodes[0]. Multi-node deployments
// must continue to use YAML — env vars don't compose well with a
// list. An empty env var is treated as "not set" (matching
// EnvInterpolate's empty-as-unset semantics) so an unintentionally-
// empty templated var (e.g. `helm install --set host=`) does not
// silently zero out an existing config value.
//
// Supported variables:
//
//	Listener side (cosmoguard's own bind):
//	  COSMOGUARD_HOST                     cfg.Host
//	  COSMOGUARD_RPC_PORT                 cfg.RpcPort
//	  COSMOGUARD_LCD_PORT                 cfg.LcdPort
//	  COSMOGUARD_GRPC_PORT                cfg.GrpcPort
//	  COSMOGUARD_ENABLE_EVM               cfg.EnableEvm
//	  COSMOGUARD_EVM_RPC_PORT             cfg.EvmRpcPort
//	  COSMOGUARD_EVM_RPC_WS_PORT          cfg.EvmRpcWsPort
//	  COSMOGUARD_METRICS_ENABLE           cfg.Metrics.Enable
//	  COSMOGUARD_METRICS_PORT             cfg.Metrics.Port
//	  COSMOGUARD_DASHBOARD_ENABLE         cfg.Dashboard.Enable
//	  COSMOGUARD_DASHBOARD_PORT           cfg.Dashboard.Port
//	  COSMOGUARD_DASHBOARD_AUTH_USER      cfg.Dashboard.BasicAuthUser
//	  COSMOGUARD_DASHBOARD_AUTH_PASSWORD  cfg.Dashboard.BasicAuthPassword
//
//	Upstream node (applied to cfg.Nodes[0]):
//	  COSMOGUARD_NODE_NAME                cfg.Nodes[0].Name
//	  COSMOGUARD_NODE_HOST                cfg.Nodes[0].Host
//	  COSMOGUARD_NODE_TLS                 cfg.Nodes[0].TLS
//	  COSMOGUARD_NODE_RPC_PORT            cfg.Nodes[0].RpcPort
//	  COSMOGUARD_NODE_LCD_PORT            cfg.Nodes[0].LcdPort
//	  COSMOGUARD_NODE_GRPC_PORT           cfg.Nodes[0].GrpcPort
//	  COSMOGUARD_NODE_EVM_RPC_PORT        cfg.Nodes[0].EvmRpcPort
//	  COSMOGUARD_NODE_EVM_RPC_WS_PORT     cfg.Nodes[0].EvmRpcWsPort
//	  COSMOGUARD_NODE_RPC_URL             cfg.Nodes[0].RpcURL
//	  COSMOGUARD_NODE_LCD_URL             cfg.Nodes[0].LcdURL
//	  COSMOGUARD_NODE_GRPC_URL            cfg.Nodes[0].GrpcURL
//	  COSMOGUARD_NODE_EVM_RPC_URL         cfg.Nodes[0].EvmRpcURL
//	  COSMOGUARD_NODE_EVM_RPC_WS_URL      cfg.Nodes[0].EvmRpcWsURL
//
//	Discovery (cosmopilot's main use case — point at a K8s headless
//	service and let cosmoguard expand pod IPs into upstreams):
//	  COSMOGUARD_DISCOVERY_HOST              cfg.Nodes[0].Discovery.Host
//	  COSMOGUARD_DISCOVERY_TYPE              cfg.Nodes[0].Discovery.Type
//	  COSMOGUARD_DISCOVERY_REFRESH_INTERVAL  cfg.Nodes[0].Discovery.RefreshInterval
//
// Setting any of the three COSMOGUARD_DISCOVERY_* vars on a node
// without an existing Discovery block creates one (with type=dns,
// refreshInterval=15s) so a minimal YAML can be converted into a
// discovery-driven deployment purely via env. To combine discovery
// with env vars, the YAML must not set a non-default `node.host` —
// validateNodeDiscovery rejects that combination because the
// discovered IPs would silently overwrite the static value.
func applyEnvOverrides(cfg *Config) error {
	// Listener side. Port fields go through envPort (1..65535 range
	// check) so a typo (`PORT=265570`) fails at startup with a clear
	// error pointing at the offending env var, instead of a confusing
	// "listen on port -65535" message from net.Listen later.
	envStr("COSMOGUARD_HOST", &cfg.Host)
	if err := envPort("COSMOGUARD_RPC_PORT", &cfg.RpcPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_LCD_PORT", &cfg.LcdPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_GRPC_PORT", &cfg.GrpcPort); err != nil {
		return err
	}
	if err := envBool("COSMOGUARD_ENABLE_EVM", &cfg.EnableEvm); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_EVM_RPC_PORT", &cfg.EvmRpcPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_EVM_RPC_WS_PORT", &cfg.EvmRpcWsPort); err != nil {
		return err
	}
	if err := envBool("COSMOGUARD_METRICS_ENABLE", &cfg.Metrics.Enable); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_METRICS_PORT", &cfg.Metrics.Port); err != nil {
		return err
	}

	// Dashboard: standalone read-only UI. Enable is *bool so the
	// override can distinguish unset / true / false the same way
	// YAML can (Dashboard.IsEnabled treats nil as default-enabled).
	if err := envBoolPtr("COSMOGUARD_DASHBOARD_ENABLE", &cfg.Dashboard.Enable); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_DASHBOARD_PORT", &cfg.Dashboard.Port); err != nil {
		return err
	}
	envStr("COSMOGUARD_DASHBOARD_AUTH_USER", &cfg.Dashboard.BasicAuthUser)
	envStr("COSMOGUARD_DASHBOARD_AUTH_PASSWORD", &cfg.Dashboard.BasicAuthPassword)

	// Node-side overrides target Nodes[0]. PrepareConfig guarantees at
	// least one entry exists (via the v3→v4 promotion) by the time
	// this function runs, but guard defensively so a future refactor
	// of PrepareConfig that calls applyEnvOverrides out of order
	// doesn't panic-deref a zero-length slice.
	if len(cfg.Nodes) == 0 {
		return nil
	}
	n := &cfg.Nodes[0]
	envStr("COSMOGUARD_NODE_NAME", &n.Name)
	envStr("COSMOGUARD_NODE_HOST", &n.Host)
	if err := envBool("COSMOGUARD_NODE_TLS", &n.TLS); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_NODE_RPC_PORT", &n.RpcPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_NODE_LCD_PORT", &n.LcdPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_NODE_GRPC_PORT", &n.GrpcPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_NODE_EVM_RPC_PORT", &n.EvmRpcPort); err != nil {
		return err
	}
	if err := envPort("COSMOGUARD_NODE_EVM_RPC_WS_PORT", &n.EvmRpcWsPort); err != nil {
		return err
	}
	envStr("COSMOGUARD_NODE_RPC_URL", &n.RpcURL)
	envStr("COSMOGUARD_NODE_LCD_URL", &n.LcdURL)
	envStr("COSMOGUARD_NODE_GRPC_URL", &n.GrpcURL)
	envStr("COSMOGUARD_NODE_EVM_RPC_URL", &n.EvmRpcURL)
	envStr("COSMOGUARD_NODE_EVM_RPC_WS_URL", &n.EvmRpcWsURL)

	// Discovery: any of the three vars create the Discovery block on
	// demand. Mirror the YAML defaults (type=dns, refreshInterval=15s)
	// so a node configured purely via env passes validateNodeDiscovery.
	dHost, dHostSet := lookupNonEmptyEnv("COSMOGUARD_DISCOVERY_HOST")
	dType, dTypeSet := lookupNonEmptyEnv("COSMOGUARD_DISCOVERY_TYPE")
	dRefresh, dRefreshSet := lookupNonEmptyEnv("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL")
	if dHostSet || dTypeSet || dRefreshSet {
		if n.Discovery == nil {
			n.Discovery = &DiscoveryConfig{
				Type:            "dns",
				RefreshInterval: 15 * time.Second,
			}
		}
		if dHostSet {
			n.Discovery.Host = dHost
		}
		if dTypeSet {
			n.Discovery.Type = dType
		}
		if dRefreshSet {
			d, err := time.ParseDuration(dRefresh)
			if err != nil {
				return fmt.Errorf("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL: invalid duration %q: %w", dRefresh, err)
			}
			if d <= 0 {
				// Zero or negative would degenerate into a tight DNS
				// refresh loop. validateNodeDiscovery only rejects < 0,
				// so guard at the env-override layer where the typo is
				// more likely to originate.
				return fmt.Errorf("COSMOGUARD_DISCOVERY_REFRESH_INTERVAL: got %s, must be > 0", d)
			}
			n.Discovery.RefreshInterval = d
		}
	}
	return nil
}

// envStr assigns *dst from the env var named name when it is set
// to a non-empty value. Surrounding whitespace is trimmed — a
// stray `COSMOGUARD_NODE_HOST=" 10.0.0.1"` from a misquoted YAML
// template would otherwise propagate to dial time as a confusing
// "no such host" error. Skipped when the var is unset or empty
// per the empty-as-unset rule documented on lookupNonEmptyEnv.
func envStr(name string, dst *string) {
	if v, ok := lookupNonEmptyEnv(name); ok {
		*dst = strings.TrimSpace(v)
	}
}

// envPort assigns *dst from the env var named name parsed as a
// TCP/UDP port (1..65535). A malformed or out-of-range value is a
// startup-fatal error pointing at the offending env var, never a
// silent fall-through — port misconfiguration would otherwise
// surface as a confusing net.Listen error far from the cause.
func envPort(name string, dst *int) error {
	v, ok := lookupNonEmptyEnv(name)
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("%s: invalid integer %q: %w", name, v, err)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("%s: port %d out of range (want 1..65535)", name, n)
	}
	*dst = n
	return nil
}

// envBool assigns *dst from the env var named name. Accepts the
// strconv.ParseBool vocabulary (1/0/t/f/true/false, any case) plus
// the YAML / Helm chart idioms `yes/no/on/off` that operators
// reasonably expect to work — those would otherwise be rejected
// and produce a startup failure for an obviously-correct value.
// Other values fail with an error naming the env var, never
// silently keeping the YAML/default.
func envBool(name string, dst *bool) error {
	v, ok := lookupNonEmptyEnv(name)
	if !ok {
		return nil
	}
	return envBoolFromValue(name, v, dst)
}

// envBoolPtr is the *bool variant of envBool, used for config fields
// where the YAML can distinguish "unset" (nil) from "explicitly
// false" — typically the IsEnabled idiom that lets a default-true
// flag accept `enable: false` from YAML without the creasty/defaults
// override footgun. An unset env var leaves *dst untouched (it stays
// nil if it was nil); a set env var allocates a *bool with the
// parsed value. Accepts the same vocabulary as envBool.
func envBoolPtr(name string, dst **bool) error {
	v, ok := lookupNonEmptyEnv(name)
	if !ok {
		return nil
	}
	var b bool
	// Reuse envBool's parsing by funneling through a temp var; this
	// keeps the bool vocabulary (yes/no/on/off/y/n + ParseBool) in a
	// single place so the two helpers can't drift.
	if err := envBoolFromValue(name, v, &b); err != nil {
		return err
	}
	*dst = &b
	return nil
}

// envBoolFromValue is the parse-only core shared by envBool and
// envBoolPtr. Kept package-private so tests exercise envBool /
// envBoolPtr (the public-shape callers) instead of the helper.
func envBoolFromValue(name, v string, dst *bool) error {
	trimmed := strings.TrimSpace(v)
	switch strings.ToLower(trimmed) {
	case "yes", "y", "on":
		*dst = true
		return nil
	case "no", "n", "off":
		*dst = false
		return nil
	}
	b, err := strconv.ParseBool(trimmed)
	if err != nil {
		return fmt.Errorf("%s: invalid boolean %q (want 1/0, true/false, yes/no, on/off, y/n): %w", name, v, err)
	}
	*dst = b
	return nil
}

// lookupNonEmptyEnv reports the value of the env var only when it is
// both set and non-empty. Matches EnvInterpolate's empty-as-unset
// rule so a templated-but-unfilled var (e.g. `helm --set host=`)
// doesn't silently overwrite a configured value with the empty
// string.
func lookupNonEmptyEnv(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
