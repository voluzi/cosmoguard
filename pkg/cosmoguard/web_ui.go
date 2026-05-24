package cosmoguard

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	stdjson "encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//go:embed web
var webFS embed.FS

// installWebUI registers the read-only dashboard routes on mux. The
// HTML + CSS + JS are baked into the binary via go:embed; the
// dashboard fetches the JSON API and renders client-side. There's
// no build step — no framework, no npm, no bundler — so the dashboard
// never breaks because a node_modules pinning got stale.
//
// All routes are gated by the same wrapper: when basicAuthUser is
// non-empty, the wrapper requires HTTP Basic with constant-time
// compare; when it's empty, the routes are open (operator's call).
//
// This is the legacy "co-mounted on the metrics port" entry. The
// canonical mount in v4+ is the standalone dashboard listener
// installed by installDashboardServer.
func installWebUI(mux *http.ServeMux, cg *CosmoGuard, cfg *WebUIConfig) {
	user, pass := "", ""
	if cfg != nil {
		user, pass = cfg.BasicAuthUser, cfg.BasicAuthPassword
	}
	installDashboardRoutes(mux, cg, user, pass, "/admin")
}

// installDashboardRoutes mounts the dashboard UI + JSON API under the
// given urlPrefix on mux. urlPrefix is either "" (canonical
// standalone listener, routes at "/" and "/api/v1/...") or "/admin"
// (legacy co-mounted on the metrics port; preserved so operators
// running the old config don't get a 404). Trailing slash on
// urlPrefix is normalized away.
//
// Each handler runs through basicAuthGate so a single auth config
// covers both the static HTML and the JSON endpoints. The gate is
// a passthrough when user is empty — same opt-in shape as the
// legacy WebUIConfig.
//
// Cluster-aware /api/v1/cluster/* routes land on this listener too —
// see installClusterRoutes. The internal peer-API server set up in
// installPeerAPIServer reuses installLocalAPIRoutes (not this
// function) so it doesn't serve the static UI or re-expose the
// cluster fan-out endpoints — only the leaf /api/v1/<resource>
// handlers that fan-out reads from.
func installDashboardRoutes(mux *http.ServeMux, cg *CosmoGuard, basicAuthUser, basicAuthPassword, urlPrefix string) {
	urlPrefix = strings.TrimRight(urlPrefix, "/")
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		return
	}
	gate := basicAuthGate(basicAuthUser, basicAuthPassword)

	// Static UI. When urlPrefix == "" we mount at "/" (the file
	// server takes care of "/" → "/index.html"); when urlPrefix is
	// "/admin" we mount at "/admin/" and strip the prefix so the
	// embedded FS doesn't see it.
	staticPath := urlPrefix + "/"
	staticHandler := http.FileServer(http.FS(sub))
	if urlPrefix != "" {
		staticHandler = http.StripPrefix(urlPrefix+"/", staticHandler)
	}
	mux.Handle(staticPath, gate(htmlSecurityHeaders(staticHandler)))

	apiBase := urlPrefix + "/api/v1"
	installLocalAPIRoutes(mux, cg, gate, apiBase)
	// Cluster fan-out endpoints only land on the public dashboard so
	// peer-API listeners (which serve the LOCAL-only data fan-out
	// reads from) can't recursively call themselves. The peer port is
	// resolved per-request inside installClusterRoutes — informational
	// only: cacheTopologyChange rejects any cache.cluster.* mutation on
	// hot-reload (including peerApiPort), so the resolved value is
	// stable for the life of the process. The per-request read is kept
	// for defensive symmetry with the rest of the config plumbing.
	installClusterRoutes(mux, cg, gate, apiBase)
}

// installLocalAPIRoutes mounts only the /api/v1/<resource> handlers
// that return THIS pod's local state. Shared between the public
// dashboard (where each route is auth-gated and composed with static
// UI + cluster fan-outs) and the internal peer-API listener (where
// auth is by network — see peerMembershipGate in dashboard_cluster.go
// — and no static / cluster routes are mounted).
func installLocalAPIRoutes(mux *http.ServeMux, cg *CosmoGuard, gate func(http.Handler) http.Handler, apiBase string) {
	mux.Handle(apiBase+"/upstreams", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listUpstreams(cg))
	})))
	mux.Handle(apiBase+"/rules", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listRules(cg))
	})))
	mux.Handle(apiBase+"/identities", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listIdentities(cg))
	})))
	mux.Handle(apiBase+"/info", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, dashboardInfo(cg))
	})))
	// /metrics surfaces an aggregated snapshot of every request-time
	// histogram + the per-upstream healthy gauge. The dashboard
	// client polls it on the same 5s cadence as the other endpoints
	// and computes rates / percentiles client-side by differencing
	// snapshots. Stateless on the server — no time-series buffer to
	// manage across reloads. Errors from Gather() bubble out as a
	// 500 so the dashboard's error banner fires instead of silently
	// rendering stale data.
	mux.Handle(apiBase+"/metrics", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		snap, err := gatherMetricsSnapshot(prometheus.DefaultGatherer)
		if err != nil {
			http.Error(w, "metrics gather: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, snap)
	})))
	// /metrics/history serves the bounded ring buffer of recent
	// snapshots the observability replicator samples on a 5 s cadence
	// (60 entries = 5 min). The dashboard hydrates its time-series
	// charts from this on connect so panels are populated immediately
	// after a pod restart instead of starting empty and back-filling
	// one tick at a time. Entries are ordered oldest-first so the
	// client can append live polls onto the end without resorting.
	mux.Handle(apiBase+"/metrics/history", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listMetricsHistory(cg))
	})))
	// /unmatched and /denied surface the bounded in-memory
	// observability buffers fed by the proxies' fallthrough +
	// deny paths. Same auth gate as the rest of the dashboard
	// API — the data is operator-private (source IPs, paths).
	mux.Handle(apiBase+"/unmatched", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listUnmatched(cg))
	})))
	mux.Handle(apiBase+"/denied", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listDenied(cg))
	})))
	// /cache-cardinality, /reload-status, /discovery-log feed the
	// diagnostics tab. Same auth gate; all three render small bounded
	// in-memory state populated by the cache-write, hot-reload, and
	// DNS-reconcile sites.
	mux.Handle(apiBase+"/cache-cardinality", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listCardinality(cg))
	})))
	mux.Handle(apiBase+"/reload-status", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listReloadStatus(cg))
	})))
	// /requests/recent returns the bounded ring of recent request
	// metadata (only when dashboard.requestLog.enable is true; off by
	// default — see RequestLogConfig). Path + identity + source-ip
	// are operator-private; same gate as the rest of the dashboard.
	mux.Handle(apiBase+"/requests/recent", gate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, listRecentRequests(cg, r))
	})))
	mux.Handle(apiBase+"/discovery-log", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listDiscoveryLog(cg))
	})))
	// /websocket surfaces the live WS connection + subscription view
	// (connected clients, fan-out subscriptions, upstream pool health).
	// Read from live process state; same operator-private gate.
	mux.Handle(apiBase+"/websocket", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listWebSocket(cg))
	})))
	// Catch unknown /api/v1/* paths with a JSON 404. Without this,
	// ServeMux falls through to the static file handler mounted at
	// "/" (or "/admin/") which then returns the index.html bundle
	// with 200 for e.g. /api/v1/info/anything — silently masking the
	// typo and confusing dashboard / curl callers. The trailing
	// slash in the pattern makes it match the entire subtree.
	mux.Handle(apiBase+"/", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unknown api path"}`))
	})))
}

// installDashboardServer builds the standalone read-only dashboard
// HTTP server on cfg.Port. Returned server is unstarted — the caller
// (cosmoguard.New) wires Run/Shutdown. nil when the dashboard is
// disabled, so the caller can skip the goroutine without a separate
// flag check.
func installDashboardServer(cg *CosmoGuard, cfg *DashboardConfig, host string) *http.Server {
	if cfg == nil || !cfg.IsEnabled() {
		return nil
	}
	mux := http.NewServeMux()
	installDashboardRoutes(mux, cg, cfg.BasicAuthUser, cfg.BasicAuthPassword, "")
	// Timeouts defend against slowloris-style resource exhaustion on
	// the standalone listener: without them a single attacker dripping
	// bytes over an open TCP connection pins a goroutine indefinitely.
	// The dashboard is on-by-default and may be reachable from
	// untrusted networks (operators who forget to gate it behind
	// ingress); the timeouts cap that exposure at the cost of a hard
	// limit on slow clients — fine because every endpoint here is
	// either tiny JSON or static assets, none of which legitimately
	// take tens of seconds. Values mirror the production-safe defaults
	// from the Go HTTP server docs.
	return &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// dashboardInfo is the JSON payload the standalone dashboard polls
// for the header chrome (version, uptime, healthy upstreams). Mirrors
// the /info handler on the metrics port so the dashboard works on
// either mount point without cross-origin fetches.
//
// The cluster sub-object lets the UI decide whether to surface the
// "this pod / cluster" toggle: peer_count includes self, so a single-
// instance deployment reports 1 and the UI hides the toggle. enabled
// reflects ClusterConfig.Enable so the UI never tries the /cluster/*
// endpoints in embedded-only mode (they would return empty envelopes
// and confuse the renderers).
func dashboardInfo(cg *CosmoGuard) map[string]any {
	enabled := false
	peerCount := 0
	if cfg := cg.snapshotConfig(); cfg != nil && cfg.Cache.Cluster != nil {
		enabled = true
	}
	if enabled {
		if members := clusterMembers(context.Background(), cg); len(members) > 0 {
			peerCount = len(members)
		}
	}
	return map[string]any{
		"version":        Version,
		"commit":         CommitHash,
		"started_at":     cg.startedAt.UTC().Format(time.RFC3339),
		"uptime_seconds": int(time.Since(cg.startedAt).Seconds()),
		"upstreams": map[string]any{
			"count":   cg.countUpstreams(),
			"healthy": cg.countHealthyUpstreams(),
		},
		"cluster": map[string]any{
			"enabled":    enabled,
			"peer_count": peerCount,
		},
	}
}

// basicAuthGate returns the middleware to wrap each dashboard route.
// When user is empty, returns a passthrough — operators behind a
// trusted network (the common K8s sidecar / ingress-fronted case)
// can leave the dashboard ungated.
//
// Constant-time check is built on SHA-256 digests rather than a raw
// `subtle.ConstantTimeCompare` of the request strings, for two
// reasons:
//
//  1. `ConstantTimeCompare` returns 0 immediately when the two
//     byte slices have different lengths, leaking the configured
//     username and password lengths via response time. Hashing
//     normalizes both sides to a fixed 32-byte digest before the
//     compare.
//  2. The naive `compare(u) && compare(p)` form short-circuits when
//     the username compare fails, so an attacker can distinguish
//     "wrong user" from "wrong password" by timing and probe the
//     username separately. Here both compares always run and their
//     results are ANDed into a single int that we check at the end.
func basicAuthGate(user, password string) func(http.Handler) http.Handler {
	if user == "" {
		return func(h http.Handler) http.Handler { return h }
	}
	wantUHash := sha256.Sum256([]byte(user))
	wantPHash := sha256.Sum256([]byte(password))
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			gotUHash := sha256.Sum256([]byte(u))
			gotPHash := sha256.Sum256([]byte(p))
			okU := subtle.ConstantTimeCompare(gotUHash[:], wantUHash[:])
			okP := subtle.ConstantTimeCompare(gotPHash[:], wantPHash[:])
			if !ok || (okU&okP) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="cosmoguard"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

// htmlSecurityHeaders pins the dashboard shell's clickjacking and
// content-injection defenses:
//
//   - X-Frame-Options: DENY  — and frame-ancestors 'none' below for
//     browsers that honor CSP — so an attacker page can't iframe the
//     dashboard and coerce clicks against an already-authenticated
//     operator session.
//   - Content-Security-Policy with default-src 'self' confines fetches
//     to the dashboard origin (the UI only ever calls its own
//     /api/v1/*). The 'unsafe-inline' allowance on style-src and
//     script-src is unavoidable today because index.html keeps its
//     CSS and JS inline as a no-build-step design choice; the policy
//     still blocks remote-origin script injection, which is the
//     practical XSS vector.
//   - Referrer-Policy: no-referrer keeps the dashboard URL (which may
//     embed an operator's hostname/port in shareable links) out of
//     outbound requests.
//
// Applied only to the static handler; the JSON endpoints already
// carry nosniff + no-store via writeJSON.
func htmlSecurityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		// index.html has no inline <script> or event handlers — only
		// a couple of inline `style="..."` attributes on a SVG path
		// and a header div. Drop 'unsafe-inline' from script-src so
		// the practical XSS vector (a reflected/stored payload that
		// renders as an inline script) is blocked even if a future
		// regression slips one in. style-src keeps 'unsafe-inline'
		// for the inline attrs in index.html. img-src adds data: so
		// the SVG favicon embedded as a data URL still loads.
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; "+
				"style-src 'self' 'unsafe-inline'; "+
				"script-src 'self'; "+
				"img-src 'self' data:; "+
				"frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		// Cache-Control on static dashboard assets: the bundle ships
		// embedded in the binary so any change requires a redeploy.
		// `no-cache, must-revalidate` lets the browser keep the asset
		// in its disk cache but forces a conditional GET so a redeploy
		// is picked up on the next page load instead of waiting for
		// the default heuristic freshness window.
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	// nosniff: the dashboard JSON endpoints are read-only metadata,
	// but a misbehaving browser that sniffed HTML out of a JSON
	// payload (or out of an attacker-controlled identity name) could
	// be coerced into rendering it as HTML in the dashboard origin.
	// Pinning the type defends against that and matches the OWASP
	// secure-headers baseline.
	// no-store: dashboard payloads include live state (upstream
	// health, uptime, rule list) that goes stale immediately, and
	// some of it (identity names, scopes) is sensitive enough that
	// we don't want intermediaries or the browser back/forward cache
	// retaining copies past the operator's session.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	// Log encode failures: a partially-written response is invisible to
	// the operator otherwise (the dashboard renders an empty section
	// with no signal that the backend choked on the payload). Most
	// failures here are client disconnects, which are uninteresting,
	// but a genuine encoder bug (unsupported type, cyclic struct) needs
	// to be findable. Warn-level keeps the disconnect noise low while
	// still surfacing real failures.
	if err := stdjson.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("dashboard json encode failed", "error", err)
	}
}

// upstreamView is the JSON shape the dashboard expects.
type upstreamView struct {
	Name        string `json:"name"`
	Target      string `json:"target"`
	Weight      int    `json:"weight"`
	Healthy     bool   `json:"healthy"`
	CircuitOpen bool   `json:"circuit_open"`
}

func listUpstreams(cg *CosmoGuard) map[string]any {
	out := []upstreamView{}
	add := func(pool *HttpUpstreamPool) {
		if pool == nil {
			return
		}
		for _, u := range pool.Upstreams() {
			out = append(out, upstreamView{
				Name:        u.Name,
				Target:      u.Target.String(),
				Weight:      u.Weight,
				Healthy:     u.healthy.Load(),
				CircuitOpen: u.CircuitOpen(),
			})
		}
	}
	if cg.lcdProxy != nil {
		add(cg.lcdProxy.pool)
	}
	// RPC pool is the same upstream set as LCD with different port —
	// listing both would duplicate. The dashboard shows the LCD pool
	// (which has identical health from the same node).
	return map[string]any{"upstreams": out}
}

// ruleView is a flat, dashboard-friendly summary of one compiled rule.
//
// Tag + RuleID let operators correlate an alert from the
// `rule_id` Prometheus label back to the dashboard. The HTTP / JSON-
// RPC histograms label every observation with the operator-supplied
// `tag:` field, falling back to `r-<fingerprint>` when the tag is
// empty. Without those fields here, an alert firing on
// `rule_id="r-deadbeef"` was a dead-end lookup — operators had to
// recompute the fingerprint by hand.
type ruleView struct {
	Section      string `json:"section"`
	Priority     int    `json:"priority"`
	Action       string `json:"action"`
	Tag          string `json:"tag,omitempty"`
	RuleID       string `json:"rule_id"`
	MatchSummary string `json:"match_summary"`
	Cache        string `json:"cache,omitempty"`
	RateLimit    string `json:"rate_limit,omitempty"`
	Auth         string `json:"auth,omitempty"`
}

func listRules(cg *CosmoGuard) map[string]any {
	out := []ruleView{}
	// Snapshot under configMutex so a concurrent tryReload swap
	// can't race the slice-header reads below. Same pattern as
	// applyRulesLocked.
	cfg := cg.snapshotConfig()
	if cfg == nil {
		return map[string]any{"rules": out}
	}
	for _, r := range cfg.LCD.Rules {
		out = append(out, httpRuleView("lcd", r))
	}
	for _, r := range cfg.RPC.Rules {
		out = append(out, httpRuleView("rpc", r))
	}
	for _, r := range cfg.RPC.JsonRpc.Rules {
		out = append(out, jsonRpcRuleView("rpc.jsonrpc", r))
	}
	for _, r := range cfg.GRPC.Rules {
		out = append(out, grpcRuleView("grpc", r))
	}
	for _, r := range cfg.EVM.RPC.Rules {
		out = append(out, jsonRpcRuleView("evm.rpc", r))
	}
	for _, r := range cfg.EVM.RPC.HttpRules {
		out = append(out, httpRuleView("evm.rpc.httpRules", r))
	}
	for _, r := range cfg.EVM.WS.Rules {
		out = append(out, jsonRpcRuleView("evm.ws", r))
	}
	return map[string]any{"rules": out}
}

func httpRuleView(section string, r *HttpRule) ruleView {
	return ruleView{
		Section:      section,
		Priority:     r.Priority,
		Action:       string(r.Action),
		Tag:          r.Tag,
		RuleID:       ruleTagOrFingerprint(r.Tag, r.Fingerprint),
		MatchSummary: summarizeHttpMatch(r),
		Cache:        summarizeCache(r.Cache),
		RateLimit:    summarizeRateLimit(r.RateLimit),
		Auth:         summarizeAuth(r.Auth),
	}
}

func jsonRpcRuleView(section string, r *JsonRpcRule) ruleView {
	parts := []string{}
	if len(r.Methods) > 0 {
		parts = append(parts, fmt.Sprintf("methods=%v", r.Methods))
	}
	if r.Params != nil {
		parts = append(parts, "params=set")
	}
	return ruleView{
		Section:      section,
		Priority:     r.Priority,
		Action:       string(r.Action),
		Tag:          r.Tag,
		RuleID:       ruleTagOrFingerprint(r.Tag, r.Fingerprint),
		MatchSummary: joinNonEmpty(parts),
		Cache:        summarizeCache(r.Cache),
	}
}

func grpcRuleView(section string, r *GrpcRule) ruleView {
	return ruleView{
		Section:      section,
		Priority:     r.Priority,
		Action:       string(r.Action),
		Tag:          r.Tag,
		RuleID:       ruleTagOrFingerprint(r.Tag, r.Fingerprint),
		MatchSummary: fmt.Sprintf("methods=%v", r.Methods),
		Cache:        summarizeCache(r.Cache),
	}
}

func summarizeHttpMatch(r *HttpRule) string {
	parts := []string{}
	if r.Match != nil {
		m := r.Match
		if m.Path != "" {
			parts = append(parts, "path="+m.Path)
		}
		if len(m.Paths) > 0 {
			parts = append(parts, fmt.Sprintf("paths=%v", m.Paths))
		}
		if m.Method != "" {
			parts = append(parts, "method="+m.Method)
		}
		if len(m.Methods) > 0 {
			parts = append(parts, fmt.Sprintf("methods=%v", m.Methods))
		}
		if len(m.Query) > 0 {
			parts = append(parts, "query=set")
		}
		if len(m.Header) > 0 {
			parts = append(parts, "header=set")
		}
		if m.SourceIP != "" {
			parts = append(parts, "sourceIP="+m.SourceIP)
		}
		if len(m.All)+len(m.Any)+len(m.None) > 0 {
			parts = append(parts, fmt.Sprintf("combinator(all=%d,any=%d,none=%d)",
				len(m.All), len(m.Any), len(m.None)))
		}
	}
	if len(r.Paths) > 0 {
		parts = append(parts, fmt.Sprintf("paths=%v", r.Paths))
	}
	if len(r.Methods) > 0 {
		parts = append(parts, fmt.Sprintf("methods=%v", r.Methods))
	}
	if len(r.Query) > 0 {
		parts = append(parts, "query=set")
	}
	return joinNonEmpty(parts)
}

func summarizeCache(c *RuleCache) string {
	if c == nil || !c.Enable {
		return ""
	}
	return fmt.Sprintf("ttl=%s cacheError=%v", c.TTL, c.CacheError)
}

func summarizeRateLimit(rl *RateLimitConfig) string {
	if rl == nil {
		return ""
	}
	scope := rl.Scope
	if scope == "" {
		scope = RateLimitScopePerIP
	}
	return fmt.Sprintf("rate=%s burst=%d scope=%s", rl.Rate.Spec, rl.Burst, scope)
}

func summarizeAuth(a *RuleAuthConfig) string {
	if a == nil {
		return ""
	}
	parts := []string{}
	if a.Require != nil && *a.Require {
		parts = append(parts, "require=true")
	}
	if len(a.Scopes) > 0 {
		parts = append(parts, fmt.Sprintf("scopes=%v", a.Scopes))
	}
	if len(a.Identities) > 0 {
		parts = append(parts, fmt.Sprintf("identities=%v", a.Identities))
	}
	return joinNonEmpty(parts)
}

// joinNonEmpty joins parts with a single space, skipping any empty
// element. The name promised "non-empty" so a future caller can
// safely pass partially-populated slices without producing
// double-space artifacts in the match summary.
func joinNonEmpty(parts []string) string {
	out := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if out != "" {
			out += " "
		}
		out += p
	}
	return out
}

type identityView struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes,omitempty"`
}

func listIdentities(cg *CosmoGuard) map[string]any {
	out := []identityView{}
	// Snapshot under configMutex; see listRules for the same race
	// concern with tryReload's pointer swap.
	cfg := cg.snapshotConfig()
	if cfg != nil {
		for _, i := range cfg.Auth.Identities {
			// Credentials (APIKey) are NEVER exposed by the dashboard —
			// read-only is one thing, leaking secrets is another.
			out = append(out, identityView{Name: i.Name, Scopes: i.Scopes})
		}
	}
	return map[string]any{"identities": out}
}
