package cosmoguard

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olric-data/olric"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/voluzi/cosmoguard/pkg/cache"
	"github.com/voluzi/cosmoguard/pkg/util"
)

type EndpointHandler interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request, next func(w http.ResponseWriter, r *http.Request))
	Start(logger *Entry) error
}

type httpProxyEndpointHandler struct {
	Endpoints []Endpoint
	Handler   EndpointHandler
}

type Endpoint struct {
	Path   string
	Method string
}

type HttpProxy struct {
	defaultAction    RuleAction
	rules            []*HttpRule
	server           *http.Server
	pool             *HttpUpstreamPool
	cache            cache.Cache[string, CachedResponse]
	endpointHandlers []*httpProxyEndpointHandler
	rulesMutex       sync.RWMutex
	// setRulesMu serializes SetRules so two concurrent reloads can't
	// interleave their snapshot-then-commit and end up with a
	// p.limiters that points at a limiter the OTHER call already
	// Close()'d. Today tryReload holds configMutex around the call
	// chain, masking the race in production — but SetRules is
	// exported and any direct caller (tests, future hot-reload paths)
	// trips it. The reload path is rare; the lock cost is fine.
	setRulesMu       sync.Mutex
	log              *Entry
	responseTimeHist *prometheus.HistogramVec
	// maxRequestBody is the cap applied to inbound request bodies. 0 leaves
	// the body unbounded (legacy behavior, used only when no ServerConfig was
	// threaded in).
	maxRequestBody int64
	// cacheConfig is the global cache config (Redis URL etc). Used when
	// building rate-limit buckets so HPA-scaled replicas can share state.
	cacheConfig *CacheGlobalConfig
	// olricClient is the embedded olric handle used to build the
	// cluster-wide rate limiter (backend=olric, the v4 default). nil
	// in tests that bypass cosmoguard.New — the limiter constructor
	// falls back to in-memory in that case.
	olricClient *olric.EmbeddedClient
	// limiters maps rule fingerprint to RateLimiter. Rebuilt every SetRules;
	// stale limiters are Close()'d.
	limiters map[uint64]RateLimiter
	// proxyName is the cosmoguard-internal namespace for limiter Redis keys.
	proxyName string
	// auth is the per-proxy authenticator. nil when auth.enable is false
	// (every request gets the anonymous identity, no gating applied).
	auth *Authenticator
	// cors, when non-nil and enabled, controls cross-origin responses.
	cors *CORSConfig
	// cgDashboard is the optional observability sink that records
	// unmatched-rule fallthroughs and denied requests for the
	// dashboard. nil when the dashboard surface isn't wired (e.g.
	// programmatic embedders that bypass cosmoguard.New). All
	// Record* calls are nil-safe.
	cgDashboard *dashboardObservability
	// cgRequestLog captures recent-request metadata for the dashboard's
	// Live-traffic panel. nil-safe; the proxy calls Record on every
	// completed request and the ring drops the entry when the operator
	// hasn't enabled it.
	cgRequestLog *requestLog
	// section is the dashboard section name passed alongside every
	// recorded event ("lcd", "rpc", "evm.rpc.httpRules"). Set by
	// SetDashboard; empty until wired so RecordUnmatched/Deny no-op.
	section string
}

// SetDashboard wires the observability sink and section name into
// the proxy. Called once from cosmoguard.New after the proxy is
// constructed; nil-safe so tests that bypass dashboard wiring can
// skip it.
func (p *HttpProxy) SetDashboard(section string, d *dashboardObservability) {
	p.section = section
	p.cgDashboard = d
}

// SetRequestLog wires the recent-requests ring. Nil-safe.
func (p *HttpProxy) SetRequestLog(rl *requestLog) { p.cgRequestLog = rl }

// CachedResponse captures what was returned by the upstream so a cache hit
// can replay it faithfully — same status, same headers, same body. Pre-B5
// the cached form was just (Data, StatusCode) and the hit path forced
// Content-Type to application/json, breaking compatibility for endpoints
// that return text/plain, application/grpc-web, etc.
type CachedResponse struct {
	Data       []byte
	StatusCode int
	// Headers is the subset of upstream response headers preserved across
	// cache hits. We retain Content-Type, Content-Encoding, and any header
	// explicitly listed in the rule's cache.preserveHeaders. Hop-by-hop
	// headers (Connection, Transfer-Encoding, Keep-Alive, etc.) are never
	// cached — they're per-connection.
	Headers map[string]string
	// StoredAt is when the entry was written. Used at hit time to compute
	// the downstream Age header (RFC 7234 §5.1) — without this, replays
	// inherit the upstream's stale Age value and downstream caches think
	// the response is fresh for longer than it actually is.
	StoredAt time.Time
	// UpstreamAge is the value of the upstream's Age header (in seconds)
	// at store time, 0 if absent. Added to (now - StoredAt) to get the
	// downstream Age — preserves the chain of how long the response has
	// been in flight across any upstream cache layers.
	UpstreamAge int
}

// httpCacheWriteTimeout bounds how long a detached cache-write context
// stays alive. The response has already been produced and is just as
// cacheable as one whose client stuck around; a slow/wedged cache
// backend should not be allowed to leak goroutines indefinitely.
const httpCacheWriteTimeout = 5 * time.Second

// NewHttpProxy constructs an HTTP proxy fronting a pool of upstream nodes
// for a given service ("lcd", "rpc", "evm_rpc", "evm_rpc_ws"). Round-robin
// picks among healthy upstreams; healthchecks (if configured per node)
// flip nodes in and out of the pool automatically.
//
// Single-node pools are a fast path with zero per-request pool overhead
// — behaves identically to the v3 single-upstream setup.
func NewHttpProxy(name, localAddr string, nodes []NodeConfig, service string, opts ...Option[HttpProxyOptions]) (*HttpProxy, error) {
	cfg := DefaultHttpProxyOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	srv := &http.Server{Addr: localAddr}
	if sc := cfg.ServerConfig; sc != nil {
		srv.ReadHeaderTimeout = sc.ReadHeaderTimeout
		srv.ReadTimeout = sc.ReadTimeout
		srv.WriteTimeout = sc.WriteTimeout
		srv.IdleTimeout = sc.IdleTimeout
	}
	proxy := HttpProxy{
		log:              log.WithField("proxy", name),
		server:           srv,
		endpointHandlers: cfg.EndpointHandlers,
		cacheConfig:      cfg.CacheConfig,
		olricClient:      cfg.OlricClient,
		proxyName:        name,
		limiters:         map[uint64]RateLimiter{},
		auth:             cfg.Authenticator,
		cors:             cfg.CORSConfig,
	}
	if cfg.ServerConfig != nil {
		proxy.maxRequestBody = cfg.ServerConfig.EffectiveMaxRequestBody()
	}

	// Per-request request rewrite: stripped credential headers, anything
	// else cosmoguard wants to sanitize before upstream sees it. Applied
	// inside every per-upstream Director by the pool.
	rewriteDirector := func(r *http.Request) {
		if proxy.auth != nil {
			proxy.auth.StripCredentialHeaders(r.Header)
			proxy.auth.StripCredentialQuery(r)
		}
	}

	var poolOpts []HttpUpstreamPoolOption
	if cfg.UpstreamConfig != nil {
		poolOpts = append(poolOpts,
			WithUpstreamStrategy(cfg.UpstreamConfig.Strategy),
			WithUpstreamRetries(cfg.UpstreamConfig.Retries.Max),
		)
	}
	pool, err := NewHttpUpstreamPool(nodes, service, rewriteDirector, proxy.log, poolOpts...)
	if err != nil {
		return nil, err
	}
	proxy.pool = pool

	// Per-upstream ModifyResponse + ErrorHandler. Both serve two
	// purposes: (1) CORS application on successful responses, (2)
	// circuit-breaker outcome recording on every request.
	//
	// Define the hooks as pool-level functions so the pool can also
	// install them on upstreams added at runtime by DNS discovery —
	// without this, dynamically-discovered upstreams would still
	// proxy requests but would skip CORS post-processing and never
	// feed the circuit breaker, a footgun that only shows up in
	// production with autoscaled headless services.
	modifyResponse := func(u *HttpUpstream, resp *http.Response) error {
		ok := resp.StatusCode < 500
		u.RecordOutcome(ok)
		if proxy.cors != nil && proxy.cors.Enable {
			origin := ""
			if resp.Request != nil {
				origin = resp.Request.Header.Get("Origin")
			}
			proxy.cors.ApplyToResponse(resp.Header, origin)
		}
		return nil
	}
	// ErrorHandler runs when the transport itself fails (conn
	// refused, timeout, etc.). Always counts as a failure. When the
	// request is in retry mode (context carries a *retryState), we
	// record the failure flag and skip the 502 so the pool can hand
	// the request to the next upstream with a clean writer.
	errorHandler := func(u *HttpUpstream, w http.ResponseWriter, r *http.Request, err error) {
		u.RecordOutcome(false)
		if rs := retryStateFromCtx(r.Context()); rs != nil {
			rs.failed = true
			return
		}
		// Apply CORS headers on the 502 path. Without this, a browser
		// firing a cross-origin request that hits a transport failure
		// sees a "CORS error" in devtools that masks the real upstream
		// failure — same headers are present on the success path via
		// modifyResponse, so omitting them here makes errors uniquely
		// hard to debug from the client side.
		if proxy.cors != nil && proxy.cors.Enable {
			proxy.cors.ApplyToResponse(w.Header(), r.Header.Get("Origin"))
		}
		// Default behavior of httputil.ReverseProxy.
		w.WriteHeader(http.StatusBadGateway)
	}
	pool.SetProxyHooks(modifyResponse, errorHandler)
	for _, u := range pool.Upstreams() {
		u := u // closure capture
		u.proxy.ModifyResponse = func(resp *http.Response) error {
			return modifyResponse(u, resp)
		}
		u.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			errorHandler(u, w, r, err)
		}
	}
	proxy.server.Handler = &proxy

	// Setup cache
	var cacheOptions []cache.Option
	if cfg.CacheConfig != nil {
		cacheOptions = append(cacheOptions, cache.DefaultTTL(cfg.CacheConfig.TTL))
	}

	proxy.cache, err = newResponseCache[string, CachedResponse](cfg.CacheConfig, cfg.OlricClient, name, cacheOptions...)
	if err != nil {
		return nil, err
	}

	if cfg.MetricsEnabled {
		proxy.responseTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: name,
			Name:      "request_duration_seconds",
			Help:      "Histogram of response time for handler in seconds",
			Buckets:   responseTimeBuckets,
		}, []string{"method", "status_code", "cache", "action", "rule_id", "upstream"})
	}

	return &proxy, nil
}

func (p *HttpProxy) Run() error {
	if p.responseTimeHist != nil {
		// Use Register instead of MustRegister to handle
		// re-registration gracefully across hot-reloads and
		// (importantly) across parallel tests in the same process.
		// On AlreadyRegisteredError, REPLACE our private vec with
		// the already-registered one so observations land where
		// the gatherer will see them — otherwise the second
		// CosmoGuard in a process observes into a private vec that
		// /metrics never reports, silently dropping every metric.
		if err := prometheus.Register(p.responseTimeHist); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
					p.responseTimeHist = existing
				}
			}
		}
	}

	for _, eh := range p.endpointHandlers {
		if err := eh.Handler.Start(p.log); err != nil {
			return err
		}
	}

	// Start active healthchecks on any upstream that configured them.
	// No-op for single-node pools with no healthcheck — zero overhead.
	if p.pool != nil {
		p.pool.StartHealthchecks()
	}

	p.log.WithField("address", p.server.Addr).Infof("starting http proxy")
	err := p.server.ListenAndServe()
	if err == http.ErrServerClosed {
		// Clean shutdown via Shutdown(); Run() returns nil so callers don't
		// treat the orderly close as a fatal error.
		return nil
	}
	return err
}

// Shutdown closes the listener and drains in-flight requests with the given
// context's deadline, then closes the cache backend and every rate limiter
// (releasing TTL-expiry goroutines and Redis connection pools). If the
// drain hits the deadline, force-closes active conns BEFORE tearing down
// the pool / cache / limiters so handlers still running on those conns
// don't see freed state. Returns the first non-nil error.
func (p *HttpProxy) Shutdown(ctx context.Context) error {
	err := p.server.Shutdown(ctx)
	// If the graceful drain hit the deadline, force-close active
	// connections BEFORE we tear down the upstream pool / cache /
	// limiters below — otherwise in-flight handlers run against a
	// freed pool or a closed Redis client and either panic or
	// return nonsense to the still-connected caller.
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		p.log.Warn("http proxy shutdown deadline exceeded; force-closing in-flight conns")
		_ = p.server.Close()
	}
	if p.pool != nil {
		p.pool.Shutdown()
	}
	if p.cache != nil {
		if cerr := p.cache.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	p.rulesMutex.Lock()
	limiters := p.limiters
	p.limiters = nil
	p.rulesMutex.Unlock()
	for _, l := range limiters {
		if cerr := l.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (p *HttpProxy) SetRules(rules []*HttpRule, defaultAction RuleAction) {
	// Serialize SetRules across the whole snapshot → build → commit
	// → close sequence. Without this, two concurrent SetRules calls
	// (A reads `existing`; B reads `existing`; A commits its
	// newLimiters; B commits its newLimiters built on the SAME stale
	// snapshot) can leave p.limiters pointing at a RateLimiter that
	// the OTHER call already Close()'d — a subsequent request then
	// uses a limiter whose Redis conn is gone.
	//
	// rulesMutex stays as the RWMutex for the hot request path; this
	// is a separate mutex so the long build phase doesn't block
	// ServeHTTP readers.
	p.setRulesMu.Lock()
	defer p.setRulesMu.Unlock()

	// Build a new fingerprint→limiter map for the new ruleset. Any limiter
	// from the previous map that doesn't have a matching rule in the new
	// set is Close()'d so its Redis connection / timers are released.
	//
	// Snapshot the existing limiters under RLock so the commit below
	// observes a consistent view even though we hold setRulesMu.
	p.rulesMutex.RLock()
	existing := p.limiters
	p.rulesMutex.RUnlock()

	newLimiters := map[uint64]RateLimiter{}
	for _, r := range rules {
		if r.RateLimit == nil {
			continue
		}
		// Reuse the previous limiter if the rule fingerprint didn't change.
		if l, ok := existing[r.Fingerprint]; ok {
			newLimiters[r.Fingerprint] = l
			continue
		}
		// Each rule's bucket pool gets its own keyspace under the proxy
		// name so multiple proxies (lcd, rpc, etc.) don't share buckets.
		keyspace := p.proxyName + ":rl:" + strconv.FormatUint(r.Fingerprint, 16)
		l, err := NewRateLimiter(*r.RateLimit, p.olricClient, keyspace)
		if err != nil {
			if sentinel := limiterForFailedInit(r.RateLimit, err); sentinel != nil {
				p.log.WithError(err).WithField("rule_priority", r.Priority).
					Error("rate limiter init failed; fail-closed rule will DENY")
				newLimiters[r.Fingerprint] = sentinel
			} else {
				p.log.WithError(err).WithField("rule_priority", r.Priority).
					Error("rate limiter init failed; fail-open rule will run without limit")
			}
			continue
		}
		newLimiters[r.Fingerprint] = l
	}

	p.rulesMutex.Lock()
	old := p.limiters
	p.rules = rules
	p.defaultAction = defaultAction
	p.limiters = newLimiters
	p.rulesMutex.Unlock()

	// Close limiters that didn't survive the swap (outside the mutex).
	for fp, l := range old {
		if _, kept := newLimiters[fp]; !kept {
			_ = l.Close()
		}
	}
}

func (p *HttpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer recoverHTTP(p.log, w, r)

	// Request-ID injection: every request gets an X-Request-Id (preserved
	// if already set by an upstream LB; freshly generated otherwise). The
	// ID is on r.Context so handlers + the reverse-proxy Director see it,
	// AND on w.Header so the client gets the same value back.
	r, _ = WithRequestID(r, w)

	// Tracing: extract any incoming W3C traceparent and start a server
	// span. End-of-request is in the deferred span.End below. When
	// tracing is disabled the global tracer is a no-op — zero overhead
	// per request beyond a single function call.
	ctx, span := StartHTTPSpan(r, p.proxyName)
	defer span.End()
	r = r.WithContext(ctx)

	// Attach per-request stats so the pool can record which upstream
	// served the request and the rule-match path can record the rule
	// tag — both end up as Prometheus label values.
	ctx, _ = WithRequestStats(r.Context())
	r = r.WithContext(ctx)

	// CORS preflight: handled by cosmoguard, never forwarded upstream.
	// Returns 204 with allow-* headers on a permitted Origin, 403
	// otherwise. Plain OPTIONS (no Access-Control-Request-Method) is NOT
	// a preflight and falls through to normal proxy behavior.
	if p.cors != nil && p.cors.HandlePreflight(w, r) {
		return
	}

	// Cap request body up-front. Two layers:
	//   1. Content-Length pre-check: a client-supplied hint, trusted only as
	//      a fast-reject path. Lying low still hits the backstop below; lying
	//      high gets a 413 on a request that, had it been honest, could have
	//      been allowed — acceptable tradeoff for the common case where
	//      Cosmos clients set Content-Length accurately on JSON requests.
	//   2. MaxBytesReader backstop: covers chunked transfers (no
	//      Content-Length) and clients that lied low. When its limit is hit
	//      mid-stream, the reverse proxy surfaces a connection-level error
	//      instead of a clean 413 — uglier client experience, but the cap is
	//      still enforced and memory is bounded.
	if p.maxRequestBody > 0 {
		if r.ContentLength > p.maxRequestBody {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestBody)
		}
	}

	start := time.Now()

	// Snapshot the per-rule state under RLock so the long-running
	// upstream call doesn't keep the lock held against SetRules. The
	// chain runs against the snapshot; any rule reload between
	// snapshot and forward affects only subsequent requests.
	p.rulesMutex.RLock()
	rulesSnap := p.rules
	defaultActionSnap := p.defaultAction
	limitersSnap := p.limiters
	p.rulesMutex.RUnlock()

	req := newHTTPRequest(r)
	var matchedRule *HttpRule
	// Run auth + HTTP-rule + rate-limit gates BEFORE endpoint handler
	// dispatch. Skipping this for endpoint-handled paths (JSON-RPC
	// POST /, WS GET /websocket) lets unauthenticated requests reach
	// the JSON-RPC/WS handler, which only does its own protocol-level
	// allow/deny — bypassing the HTTP-level auth gate entirely.
	decision := p.gateChainSnap(rulesSnap, limitersSnap, &matchedRule)(req)
	// Middlewares enrich the context; pick up the latest copy.
	r = req.r

	// Endpoint handlers (JSON-RPC POST /, EVM-WS GET /websocket) run once
	// the gate has passed — for the no-rule-matched case AND for an
	// explicit ALLOW rule on the endpoint path. The latter is the
	// documented way to opt the connection out of HTTP-level auth (e.g.
	// `POST /` with auth.require:false) while delegating per-method
	// decisions to the JSON-RPC rules; routing it to the generic HTTP
	// allow path instead would bypass JSON-RPC method rules, per-method
	// auth/rate limits, and caching. A DENY rule (or a Stopped gate)
	// short-circuits before dispatch.
	if !decision.Stop && (matchedRule == nil || matchedRule.Action == RuleActionAllow) {
		for _, handler := range p.endpointHandlers {
			for _, e := range handler.Endpoints {
				if e.Method == r.Method && e.Path == r.URL.Path {
					handler.Handler.ServeHTTP(w, r, p.pool.ServeHTTP)
					return
				}
			}
		}
	}
	if decision.Stop {
		ruleTag := ""
		if matchedRule != nil {
			ruleTag = ruleTagOrFingerprint(matchedRule.Tag, matchedRule.Fingerprint)
		}
		switch decision.HTTPStatus {
		case http.StatusUnauthorized:
			p.cgDashboard.RecordDeny(DenyRecord{
				Section:  p.section,
				Reason:   "auth",
				SourceIP: GetSourceIP(r),
				Method:   r.Method,
				Path:     r.URL.Path,
				RuleTag:  ruleTag,
			})
			p.unauthorized(w, r, decision.Reason, start)
		case http.StatusTooManyRequests:
			p.cgDashboard.RecordDeny(DenyRecord{
				Section:  p.section,
				Reason:   "rate_limit",
				SourceIP: GetSourceIP(r),
				Method:   r.Method,
				Path:     r.URL.Path,
				RuleTag:  ruleTag,
			})
			p.tooManyRequests(w, r, time.Duration(decision.RetryAfter)*time.Second, start)
		default:
			http.Error(w, decision.Reason, decision.HTTPStatus)
		}
		return
	}

	// Chain passed all gates. Apply the matched rule's action; fall
	// through to default action when no rule matched.
	if matchedRule != nil {
		switch matchedRule.Action {
		case RuleActionAllow:
			p.allow(w, r, matchedRule, start)
		case RuleActionDeny:
			p.cgDashboard.RecordDeny(DenyRecord{
				Section:  p.section,
				Reason:   "rule",
				SourceIP: GetSourceIP(r),
				Method:   r.Method,
				Path:     r.URL.Path,
				RuleTag:  ruleTagOrFingerprint(matchedRule.Tag, matchedRule.Fingerprint),
			})
			p.deny(w, r, start)
		default:
			p.log.Errorf("unrecognized rule action %q", matchedRule.Action)
		}
		return
	}
	// Default-action path: chain didn't match any rule. The default
	// tag was already set by the match middleware when no rule fired.
	// Record the unmatched (method, path) tuple for the dashboard so
	// operators running `default: allow` can see what's slipping
	// through their rule set.
	p.cgDashboard.RecordUnmatched(p.section, r.Method, r.URL.Path)
	if defaultActionSnap == RuleActionAllow {
		p.allow(w, r, nil, start)
	} else {
		p.cgDashboard.RecordDeny(DenyRecord{
			Section:  p.section,
			Reason:   "default",
			SourceIP: GetSourceIP(r),
			Method:   r.Method,
			Path:     r.URL.Path,
		})
		p.deny(w, r, start)
	}
}

// gateChainSnap builds the per-request middleware chain that runs the
// auth + rate-limit gates and finds the matching rule. Takes
// snapshots of (rules, limiters) so the caller doesn't have to hold
// the rules RLock for the duration of the request. matched is an
// out-parameter the match middleware fills in so ServeHTTP can apply
// the rule's allow/deny action afterwards; it stays nil when no rule
// matched (default-action path).
func (p *HttpProxy) gateChainSnap(rules []*HttpRule, limiters map[uint64]RateLimiter, matched **HttpRule) Next {
	limFor := func(fp uint64) RateLimiter {
		if l, ok := limiters[fp]; ok {
			return l
		}
		return nil
	}
	return Chain(
		MWPanicRecovery(p.log),
		MWIdentityResolve(p.auth, httpRequestFrom),
		p.mwMatchSnap(rules, matched),
		MWAuthGate(p.auth, ruleAuthFrom(matched)),
		MWRateLimit(rateLimitConfigFrom(matched), limFor, httpRequestFrom, p.log),
	)
}

// mwMatchSnap is the HTTP-specific match middleware operating on a
// pre-snapshotted rule slice. Walks the slice for the first rule
// whose Matches() returns true, captures it in *matched, sets the
// request's RuleTag (consumed by recordOutcome as the `rule_id`
// metric label), and Continues. When no rule matches, sets RuleTag
// to "default" and Continues so the default-action path runs.
func (p *HttpProxy) mwMatchSnap(rules []*HttpRule, matched **HttpRule) Middleware {
	return func(req Request, next Next) Decision {
		hr := httpRequestFrom(req)
		if hr == nil {
			return next(req)
		}
		for _, rule := range rules {
			if rule.Matches(hr) {
				*matched = rule
				req.SetRuleTag(ruleTagOrFingerprint(rule.Tag, rule.Fingerprint))
				if stats := RequestStatsFromCtx(req.Context()); stats != nil {
					stats.RuleTag = req.RuleTag()
				}
				return next(req)
			}
		}
		req.SetRuleTag("default")
		if stats := RequestStatsFromCtx(req.Context()); stats != nil {
			stats.RuleTag = "default"
		}
		return next(req)
	}
}

// httpRequestFrom returns the underlying *http.Request from a
// pipeline Request when the protocol is HTTP. Used by the middlewares
// that need direct access to headers / IP-source plumbing.
func httpRequestFrom(req Request) *http.Request {
	if hr, ok := req.(*httpRequest); ok {
		return hr.HTTPRequest()
	}
	return nil
}

// ruleAuthFrom returns the matched rule's RuleAuthConfig (or nil)
// so MWAuthGate can decide whether to enforce.
func ruleAuthFrom(matched **HttpRule) func(Request) *RuleAuthConfig {
	return func(Request) *RuleAuthConfig {
		if matched == nil || *matched == nil {
			return nil
		}
		return (*matched).Auth
	}
}

// rateLimitConfigFrom yields the matched rule's RateLimitConfig +
// fingerprint, or nil + 0 when no rate limit applies (no rule
// matched, deny action, or no rateLimit configured).
func rateLimitConfigFrom(matched **HttpRule) func(Request) (*RateLimitConfig, uint64) {
	return func(Request) (*RateLimitConfig, uint64) {
		if matched == nil || *matched == nil {
			return nil, 0
		}
		r := *matched
		if r.Action != RuleActionAllow || r.RateLimit == nil {
			return nil, 0
		}
		return r.RateLimit, r.Fingerprint
	}
}

func (p *HttpProxy) allow(w http.ResponseWriter, r *http.Request, rule *HttpRule, startTime time.Time) {
	// Local name is ruleCache (not cache) so the package alias `cache`
	// stays in scope for errors.Is(..., cache.ErrNotFound) below. Was a
	// silent shadow before the Has-then-Get refactor — fine while no
	// reference to the package was needed inside this function.
	var ruleCache *RuleCache
	var fingerprint uint64
	var ruleTag string
	if rule != nil {
		ruleCache = rule.Cache
		fingerprint = rule.Fingerprint
		ruleTag = ruleTagOrFingerprint(rule.Tag, rule.Fingerprint)
	}
	if ruleCache != nil && ruleCache.Enable {
		// Caching is active for this rule. We need the body twice (once to
		// compute the cache hash, once to forward upstream on a miss), so
		// wrap it with ReusableReader. This is the ONLY path that pays the
		// double-buffering cost. A drain error here is the MaxBytesReader
		// cap tripping (oversized chunked / mismatched Content-Length) —
		// reject with 413 rather than caching/forwarding a truncated body.
		rr, rerr := ReusableReader(r.Body)
		if rerr != nil {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = rr
		hash, err := p.getRequestHash(r, fingerprint)
		if err != nil {
			// We could not get the hash, but we can still serve the request
			p.log.Errorf("error getting hash of request: %v", err)
		} else {
			// Single round-trip lookup. ErrNotFound is a miss (cold
			// path, no logging); anything else is a backend failure
			// that we surface as cache=error so operators alerting
			// on hit-rate see the regression instead of having an
			// outage masquerade as cold-cache traffic. The previous
			// Has-then-Get shape doubled every cache-hit cost on
			// the olric backend (Has() = Get() internally), so
			// remote-partition hits paid two RTTs per lookup.
			res, lookupErr := p.cache.Get(r.Context(), hash)
			if lookupErr == nil {
				p.cacheHit(w, r, res, startTime)
				return
			}
			if !errors.Is(lookupErr, cache.ErrNotFound) {
				p.log.Errorf("error getting cached value: %v", lookupErr)
				ww := WrapStatusOnly(w)
				p.pool.ServeHTTP(ww, r)
				p.recordOutcome(r, ww.GetStatusCode(), cacheError, RuleActionAllow, startTime, "request allowed (cache backend error)")
				return
			}
			p.cacheMiss(w, r, hash, ruleCache, ruleTag, startTime)
			return
		}

	}
	// Fast path: no cache for this rule. Use the lightweight status-only
	// writer — no body buffer, no header snapshot. Bytes flow straight
	// from upstream to client.
	ww := WrapStatusOnly(w)
	p.pool.ServeHTTP(ww, r)
	p.recordOutcome(r, ww.GetStatusCode(), cacheMiss, RuleActionAllow, startTime, "request allowed")
}

// getRequestHash produces a cache key. ruleFingerprint is mixed in so two
// rules that happen to match the same request never share cache entries
// (preventing the pre-B5 cross-rule poisoning bug with cacheError /
// cacheEmptyResult mismatches). When called from the default-action path,
// pass 0 — the per-rule namespace simply collapses to "default".
func (p *HttpProxy) getRequestHash(req *http.Request, ruleFingerprint uint64) (string, error) {
	b, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	// Normalize query string (sorted keys) so semantically-equivalent
	// requests share a cache entry regardless of param order.
	canonical := req.URL.Path
	if q := req.URL.Query().Encode(); q != "" {
		canonical += "?" + q
	}
	// Fold the client's acceptable content-coding set into the key. Upstreams
	// commonly content-negotiate on it (Content-Encoding + Vary: Accept-
	// Encoding); without this, a compressed response cached for one client
	// would be replayed verbatim to a client that can't decode that coding.
	// We key on the FULL set of acceptable codings (not a gzip/br/identity
	// bucket) so a client accepting e.g. `gzip, zstd` never shares an entry
	// with a gzip-only client — the upstream might return zstd, which the
	// gzip-only client couldn't accept. Responses that Vary on anything
	// besides Accept-Encoding are refused caching (see cacheableByVary).
	return util.XXHash64Hex(
		strconv.FormatUint(ruleFingerprint, 16) + "\x00" +
			req.Method + "\x00" + canonical + "\x00" +
			acceptEncodingKey(req.Header.Get("Accept-Encoding")) + "\x00" +
			string(b),
	), nil
}

// acceptEncodingKey returns a canonical, order-independent representation of
// the codings a client will accept (those with q > 0), so two clients share a
// cache entry only when their acceptable-coding sets are identical. It parses
// `q` values (RFC 9110 §12.5.3), so `gzip;q=0` is excluded. `*` (any coding)
// is kept as its own token — a `*` client and a `gzip` client must not share
// an entry because the upstream may return a coding only one of them accepts.
// `identity` is always implicitly acceptable unless explicitly excluded.
func acceptEncodingKey(ae string) string {
	accepted := map[string]struct{}{}
	excluded := map[string]struct{}{}
	for _, part := range strings.Split(ae, ",") {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		coding := token
		q := 1.0
		if semi := strings.IndexByte(token, ';'); semi >= 0 {
			coding = strings.TrimSpace(token[:semi])
			for _, param := range strings.Split(token[semi+1:], ";") {
				param = strings.TrimSpace(param)
				if v, ok := strings.CutPrefix(strings.ToLower(param), "q="); ok {
					if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
						q = f
					}
				}
			}
		}
		coding = strings.ToLower(coding)
		if q <= 0 {
			excluded[coding] = struct{}{}
		} else {
			accepted[coding] = struct{}{}
		}
	}
	// identity is implicitly acceptable unless it — or `*` — is explicitly
	// excluded (RFC 9110 §12.5.3).
	_, identityExcluded := excluded["identity"]
	_, starExcluded := excluded["*"]
	if !identityExcluded && !starExcluded {
		accepted["identity"] = struct{}{}
	}
	tokens := make([]string, 0, len(accepted)+len(excluded))
	for c := range accepted {
		tokens = append(tokens, c)
	}
	// When the client accepts a wildcard, an explicit `coding;q=0` exclusion
	// is meaningful (it carves the coding OUT of "anything") and MUST stay in
	// the key — otherwise `*, gzip;q=0` would collide with plain `*` and a
	// gzip response chosen for the wildcard client could later be served to a
	// client that forbids gzip. Without a wildcard, an excluded coding is
	// equivalent to simply not listing it, so it's dropped for better dedup.
	if _, wildcard := accepted["*"]; wildcard {
		for c := range excluded {
			if c == "*" {
				continue
			}
			tokens = append(tokens, "!"+c)
		}
	}
	sort.Strings(tokens)
	return strings.Join(tokens, ",")
}

// cacheHit serves an already-fetched cached response. The caller does the
// single Get up front (so it can distinguish ErrNotFound from a backend
// error and route accordingly) and hands the value in — avoids the
// extra lookup the old shape paid on every hit.
func (p *HttpProxy) cacheHit(w http.ResponseWriter, r *http.Request, res CachedResponse, startTime time.Time) {
	// Replay every preserved header from the cached response before writing
	// status + body. Cache marker is added LAST so it can't be overridden by
	// a cached upstream header named "Cache". No Content-Type fallback —
	// the miss path doesn't synthesize one either, so the hit path doing so
	// would create a subtle hit-vs-miss asymmetry. Cosmos endpoints always
	// send Content-Type; if upstream genuinely didn't, the cached response
	// matches that.
	for k, v := range res.Headers {
		w.Header().Set(k, v)
	}
	// Compute and emit Age per RFC 7234 §5.1. Downstream caches and
	// clients use Age to bound their own freshness windows; without
	// this, replays look freshly synthesized and downstream caches
	// double-count the TTL. StoredAt is zero for cache entries written
	// before this field existed (forward-compat with on-disk Redis
	// entries from older builds) — skip emission in that case rather
	// than emit a wildly-wrong Age computed from the epoch.
	if !res.StoredAt.IsZero() {
		age := int(time.Since(res.StoredAt).Seconds()) + res.UpstreamAge
		if age < 0 {
			age = 0
		}
		w.Header().Set("Age", strconv.Itoa(age))
	}
	// Cache state is exposed via the `X-Cosmoguard-Cache` header
	// (cosmoguard-namespaced, Set semantics on both hit and miss so
	// it behaves symmetrically). The original `Cache` header name
	// collided with a legitimate response header — replacing it on
	// hit and appending on miss produced inconsistent, non-upstream
	// headers. The new namespaced form makes it clear this is
	// cosmoguard-supplied and operators can drop it with a CORS /
	// header-strip rule if byte-identical responses are required.
	w.Header().Set(cacheStateHeader, cacheHit)
	// Apply CORS to the cache-hit response. The miss path goes through
	// the reverse-proxy ModifyResponse hook; the hit path bypasses that
	// hook entirely, so apply here.
	if p.cors != nil {
		p.cors.ApplyToResponse(w.Header(), r.Header.Get("Origin"))
	}
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(res.Data)
	p.recordOutcome(r, res.StatusCode, cacheHit, RuleActionAllow, startTime, "request allowed")
}

// cacheStateHeader is the response header cosmoguard adds to indicate
// cache state. Both hit and miss responses carry it so downstream
// dashboards can distinguish, AND so the header set is symmetric
// (the hit path used to do Set, miss used to do Add — clients saw
// different shapes depending on rule cache state).
const cacheStateHeader = "X-Cosmoguard-Cache"

func (p *HttpProxy) cacheMiss(w http.ResponseWriter, r *http.Request, requestHash string, cache *RuleCache, ruleTag string, startTime time.Time) {
	w.Header().Set(cacheStateHeader, cacheMiss)

	ww := WrapResponseWriter(w)
	p.pool.ServeHTTP(ww, r)
	p.recordOutcome(r, ww.GetStatusCode(), cacheMiss, RuleActionAllow, startTime, "request allowed")

	if ww.GetStatusCode() <= 0 {
		return
	}

	b, err := ww.GetWrittenBytes()
	if err != nil {
		p.log.Errorf("error loading upstream response: %v\n response not cached", err)
		return
	}

	p.log.WithFields(map[string]interface{}{
		"error":         ww.GetStatusCode() != http.StatusOK,
		"cache-enabled": cache.Enable,
		"cache-ttl":     cache.TTL.String(),
		"cache-error":   cache.CacheError,
	}).Debug("got response from upstream")

	status := ww.GetStatusCode()
	// 5xx is NEVER cached. The cacheError flag was documented as
	// covering 4xx (operator wants to memoize a 404 / 401), but the
	// old guard `status != 200` lumped 502/503/504 in too — a
	// transient upstream outage stuck around for the full TTL even
	// after the upstream recovered. Restrict cacheError to client
	// errors only.
	if status >= 500 {
		return
	}
	if status != http.StatusOK && !cache.CacheError {
		return
	}
	// Respect upstream Cache-Control: no-store / private / max-age=0
	// — a node that explicitly told us not to cache (personalised
	// tx-history, error pages, etc.) must not have its response
	// stashed. Same intent as RFC 7234 §5.2.2.
	if !cacheableByUpstream(ww.GetCommittedHeaders()) {
		return
	}
	// Refuse to cache a response whose Vary header names request headers we
	// don't fold into the cache key. We DO key on Accept-Encoding (see
	// getRequestHash), so Vary: Accept-Encoding is fine; anything else
	// (e.g. Vary: Authorization, Accept-Language, *) would let one client's
	// content-negotiated response be served to a client that sent different
	// headers.
	if !cacheableByVary(ww.GetCommittedHeaders()) {
		return
	}

	// Parse the upstream's Age header (if any) at store time so cache
	// hits can compute a correct downstream Age (per RFC 7234 §5.1):
	// our Age = upstream's_Age_at_store_time + (now - StoredAt).
	upstreamAge := 0
	if v := ww.GetCommittedHeaders().Get("Age"); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			upstreamAge = n
		}
	}
	// Detached context for the cache write: r.Context() is canceled when
	// the client disconnects, which aborts a Set that would otherwise
	// have succeeded. The response was already produced and is just as
	// cacheable. Short timeout bounds a wedged backend.
	writeCtx, cancel := context.WithTimeout(context.Background(), httpCacheWriteTimeout)
	err = p.cache.Set(writeCtx, requestHash, CachedResponse{
		Data:        b,
		StatusCode:  status,
		Headers:     pickCacheableHeaders(ww.GetCommittedHeaders(), cache.PreserveHeaders),
		StoredAt:    time.Now().UTC(),
		UpstreamAge: upstreamAge,
	}, cache.TTL)
	cancel()
	if err != nil {
		p.log.Errorf("error setting cache value: %v", err)
		return
	}
	// Record the cache-key cardinality for this (section, rule) pair
	// so the dashboard can surface a rule that's blowing up Redis with
	// per-request unique keys (e.g. accidentally including a query
	// string nonce in the hash derivation). Path-plus-query so query
	// variants register as distinct keys — the operator's signal is
	// precisely "is this rule writing one entry per unique query?".
	p.cgDashboard.RecordCardinality(p.section, ruleTag, r.Method+" "+p.redactedRequestURI(r))
}

// redactCredentialQuery replaces the value of any credential-carrying query
// parameter (from an api-key method in queryParam mode) with "REDACTED", so
// the raw key never lands in the dashboard's request log or cardinality
// samples. Returns the input unchanged when nothing needs redacting.
func (p *HttpProxy) redactCredentialQuery(rawQuery string) string {
	if rawQuery == "" || p.auth == nil {
		return rawQuery
	}
	names := p.auth.CredentialQueryParams()
	if len(names) == 0 {
		return rawQuery
	}
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		// Unparseable query: fall back to dropping it entirely rather than
		// risk logging a credential we couldn't parse out.
		return "[redacted]"
	}
	changed := false
	for _, name := range names {
		if q.Has(name) {
			q.Set(name, "REDACTED")
			changed = true
		}
	}
	if !changed {
		return rawQuery
	}
	return q.Encode()
}

// redactedRequestURI rebuilds path?query with credential query params
// redacted, for cardinality sampling.
func (p *HttpProxy) redactedRequestURI(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return r.URL.RequestURI()
	}
	redacted := p.redactCredentialQuery(r.URL.RawQuery)
	if redacted == "" {
		return r.URL.Path
	}
	return r.URL.Path + "?" + redacted
}

// cacheableByUpstream reports whether the upstream's Cache-Control
// header (if any) permits caching the response. Returns true when
// the header is absent or carries no anti-cache directive — the
// permissive default that matches what cosmoguard did pre-fix.
//
// Honored directives (RFC 7234 §5.2.2):
//   - no-store: don't cache at all.
//   - no-cache: must revalidate before reuse; cosmoguard has no
//     conditional-GET path so treat as not-cacheable.
//   - private: response is for a single user; not cacheable by a shared
//     proxy like cosmoguard.
//   - s-maxage=0: explicit "no shared cache" — overrides max-age for
//     shared caches, so check this before falling back to max-age.
//   - max-age=0: caller wants a freshness window of zero. Only honored
//     when s-maxage is absent (s-maxage wins for shared caches).
func cacheableByUpstream(h http.Header) bool {
	cc := h.Get("Cache-Control")
	if cc == "" {
		return true
	}
	var hasSMaxAge, sMaxAgeZero, maxAgeZero bool
	for _, raw := range strings.Split(cc, ",") {
		d := strings.ToLower(strings.TrimSpace(raw))
		switch d {
		case "no-store", "no-cache", "private":
			return false
		}
		if v, ok := strings.CutPrefix(d, "s-maxage="); ok {
			hasSMaxAge = true
			if strings.TrimSpace(v) == "0" {
				sMaxAgeZero = true
			}
		}
		if v, ok := strings.CutPrefix(d, "max-age="); ok {
			if strings.TrimSpace(v) == "0" {
				maxAgeZero = true
			}
		}
	}
	if sMaxAgeZero {
		return false
	}
	if !hasSMaxAge && maxAgeZero {
		return false
	}
	return true
}

// unauthorized writes a 401 when a rule's auth gate denies the request.
// reason is logged but NOT exposed in the response body — operators can
// debug from logs, attackers can't probe scope details.
func (p *HttpProxy) unauthorized(w http.ResponseWriter, r *http.Request, reason string, startTime time.Time) {
	WriteError(w, http.StatusUnauthorized, "unauthorized")
	p.recordOutcome(r, http.StatusUnauthorized, cacheMiss, RuleActionDeny, startTime,
		"request denied by auth gate", "reason", reason)
}

// tooManyRequests writes a 429 with a Retry-After header. retryAfter is
// rounded up to whole seconds since that's what the spec requires —
// truncating would let the client retry before the limiter resets and
// trip again. Minimum of 1s so the header is never `Retry-After: 0`.
func (p *HttpProxy) tooManyRequests(w http.ResponseWriter, r *http.Request, retryAfter time.Duration, startTime time.Time) {
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	WriteError(w, http.StatusTooManyRequests, "rate limit exceeded")
	p.recordOutcome(r, http.StatusTooManyRequests, cacheMiss, RuleActionDeny, startTime,
		"request rate-limited", "retry-after", retryAfter.String())
}

// recordOutcome emits the standardized per-request log line + Prometheus
// observation. Centralizes what was duplicated across allow / cacheHit /
// cacheMiss / deny / unauthorized / tooManyRequests pre-D2. Optional extra
// fields are appended to the log line (key/value pairs).
//
// action is "allow" or "deny"; cacheState is "hit" or "miss"; status is
// the HTTP status code that was (or will be) written.
//
// rule_id + upstream labels come from RequestStats on the context. They
// fall back to "default" / "" when the request short-circuited before a
// rule matched or an upstream was picked.
func (p *HttpProxy) recordOutcome(r *http.Request, status int, cacheState, action string, startTime time.Time, msg string, extras ...any) {
	duration := time.Since(startTime)
	stats := RequestStatsFromCtx(r.Context())
	ruleID, upstream := "default", ""
	if stats != nil {
		if stats.RuleTag != "" {
			ruleID = stats.RuleTag
		}
		upstream = stats.Upstream
	}
	if p.responseTimeHist != nil {
		p.responseTimeHist.WithLabelValues(
			r.Method,
			strconv.Itoa(status),
			cacheState,
			action,
			ruleID,
			upstream).Observe(duration.Seconds())
	}
	// Mark the inbound span as failed for deny / 4xx / 5xx outcomes
	// so deny+error spans don't render as green successes in
	// Jaeger / Tempo. Attribute the HTTP status code per OTEL
	// semconv so backends can filter and aggregate by status class.
	markSpanOutcome(r.Context(), status, action)
	fields := Fields{
		"path":       r.URL.Path,
		"method":     r.Method,
		"status":     status,
		"cache":      cacheState,
		"duration":   duration,
		"source":     GetSourceIP(r),
		"user-agent": r.UserAgent(),
		"rule_id":    ruleID,
		"upstream":   upstream,
	}
	for i := 0; i+1 < len(extras); i += 2 {
		if k, ok := extras[i].(string); ok {
			fields[k] = extras[i+1]
		}
	}
	p.log.WithFields(fields).Info(msg)

	// Live-traffic ring (no-op when the operator hasn't enabled it).
	if p.cgRequestLog != nil {
		identity := ""
		if stats != nil {
			identity = stats.IdentityName
		}
		p.cgRequestLog.Record(RequestLogEntry{
			Section:    p.section,
			Method:     r.Method,
			Path:       r.URL.Path,
			Query:      p.redactCredentialQuery(r.URL.RawQuery),
			Status:     status,
			LatencyMs:  duration.Milliseconds(),
			CacheState: cacheState,
			Action:     action,
			RuleTag:    ruleID,
			Identity:   identity,
			SourceIP:   GetSourceIP(r),
			Upstream:   upstream,
		})
	}
}

// ruleTagOrFingerprint returns the operator-supplied rule tag if set,
// or a stable fingerprint-derived fallback string otherwise. Bounded
// cardinality either way: tags come from config, fingerprints from
// hashed rule contents (one value per rule).
func ruleTagOrFingerprint(tag string, fingerprint uint64) string {
	if tag != "" {
		return tag
	}
	return "r-" + strconv.FormatUint(fingerprint, 16)
}

func (p *HttpProxy) deny(w http.ResponseWriter, r *http.Request, startTime time.Time) {
	WriteError(w, http.StatusUnauthorized, "unauthorized")
	p.recordOutcome(r, http.StatusUnauthorized, cacheMiss, RuleActionDeny, startTime, "request denied")
}
