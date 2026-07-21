package cosmoguard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HttpUpstream is one logical upstream node, scoped to a single service
// (LCD or RPC). Holds the per-upstream ReverseProxy + healthcheck +
// circuit-breaker state.
type HttpUpstream struct {
	Name      string
	Target    *url.URL
	Weight    int // for weighted round-robin (default 1)
	proxy     *httputil.ReverseProxy
	healthy   atomic.Bool
	hcConfig  *NodeHealthcheckConfig
	probeAddr string // full URL for the healthcheck GET

	// rolling counters for healthcheck state transitions
	failCount    atomic.Int32
	successCount atomic.Int32

	// Circuit breaker state — independent of healthcheck so the two can
	// coexist (a healthcheck-up but request-failing upstream gets tripped
	// by the breaker; a breaker-half-open upstream that fails its probe
	// re-opens without affecting the healthcheck-driven `healthy` bit).
	cbConfig         *CircuitBreakerConfig
	cbOpen           atomic.Bool
	cbConsecFails    atomic.Int32
	cbOpenedAtUnixMs atomic.Int64 // 0 when closed

	// inFlight counts active requests against this upstream. Incremented
	// before dispatch, decremented after response. Used by the
	// least-conn picker.
	inFlight atomic.Int32

	// stop is closed by RemoveUpstream to signal this upstream's
	// healthcheck goroutine to exit. Always non-nil — constructor-
	// seeded upstreams need it too because discovery-expanded nodes
	// (which arrive via the constructor) ARE subject to RemoveUpstream
	// once the reconciler observes them drop out of DNS. Leaving it
	// nil there leaks the probe goroutine, which keeps marking the
	// dead IP unhealthy via setUpstreamHealthy(..., false) — and that
	// call re-publishes the gauge series that RemoveUpstream just
	// deleted, surfacing as stale `cosmoguard_upstream_healthy=0`
	// rows on /metrics for pods that no longer exist.
	stop chan struct{}
	// done is closed by the healthcheck goroutine when it exits.
	// RemoveUpstream waits on it (after close(stop)) so the goroutine
	// has fully drained — including any in-flight probe call to
	// setUpstreamHealthy — before we deleteUpstreamHealthy. Without
	// this wait, an in-flight probe completing on the same instant
	// as RemoveUpstream races the delete and re-publishes the series.
	done chan struct{}
	// probeStarted is set true the instant a healthcheck goroutine
	// is spawned for this upstream (either by StartHealthchecks at
	// boot, or by AddUpstream post-StartHealthchecks). RemoveUpstream
	// gates the done-wait on this flag — without it, removing an
	// upstream that has hcConfig set but whose probe goroutine was
	// never started (pre-StartHealthchecks add+remove, or after
	// Shutdown drained the goroutine pool) burns the full 5s wait
	// on a `done` channel nobody will ever close.
	probeStarted atomic.Bool
}

// CircuitOpen reports whether the upstream is currently in the open
// state. Used by Pick to exclude it from the rotation.
//
// State machine:
//
//	closed     — cbOpen=false, cbOpenedAtUnixMs=0. All requests admitted.
//	open       — cbOpen=true,  cbOpenedAtUnixMs>0. All requests rejected
//	             until cooldown elapses.
//	half-open  — cbOpen=true,  cbOpenedAtUnixMs=0. EXACTLY ONE probe
//	             request is admitted (won the CAS); concurrent callers
//	             still see open and stay rejected. The probe's outcome
//	             via RecordOutcome closes the breaker (on success) or
//	             re-trips it with a fresh openedAt (on failure).
//
// Previously the cooldown-elapsed branch cleared cbOpen for EVERY
// caller that observed cooldown was up, so a burst of N concurrent
// requests after recovery all hit the still-dead upstream
// simultaneously. The CAS gate below ensures one probe at a time.
// This is READ-ONLY: it never consumes the half-open probe token, so
// healthySet can call it for every upstream without side effects. The
// token is consumed in consumeHalfOpenProbe, called only for the upstream
// Pick actually returns — otherwise an upstream whose token was consumed
// by the healthySet scan but that the picker didn't select would never get
// an RPC (and its RecordOutcome), wedging it open forever.
func (u *HttpUpstream) CircuitOpen() bool {
	if !u.cbOpen.Load() {
		return false
	}
	if u.cbConfig == nil {
		return true
	}
	openedAt := u.cbOpenedAtUnixMs.Load()
	if openedAt == 0 {
		// A probe is already in flight; everyone else stays rejected
		// until its outcome lands.
		return true
	}
	// Rejected until cooldown elapses; after that the upstream is
	// probe-eligible (no token consumed here — read-only).
	return time.Now().UnixMilli()-openedAt < u.cbConfig.CooldownPeriod.Milliseconds()
}

// consumeHalfOpenProbe attempts to consume the single half-open probe token,
// returning true when the caller MAY dispatch to this upstream. See the gRPC
// GrpcUpstream.consumeHalfOpenProbe for the full state table — closed → true;
// open+cooldown-elapsed+won-CAS → true; open+probe-in-flight or CAS-lost →
// false; still-tripped → false. Pick/pickNotTried call it only for the
// upstream they intend to return and reselect/reject on false, so a consumed
// token is always paired with exactly one request.
func (u *HttpUpstream) consumeHalfOpenProbe() bool {
	if !u.cbOpen.Load() || u.cbConfig == nil {
		return true
	}
	openedAt := u.cbOpenedAtUnixMs.Load()
	if openedAt == 0 {
		return false
	}
	if time.Now().UnixMilli()-openedAt < u.cbConfig.CooldownPeriod.Milliseconds() {
		return false
	}
	if u.cbOpenedAtUnixMs.CompareAndSwap(openedAt, 0) {
		u.cbConsecFails.Store(0)
		return true
	}
	return false
}

// firstClosed returns the first upstream in set (other than skip) whose
// breaker is fully closed, or nil.
func (p *HttpUpstreamPool) firstClosed(set []*HttpUpstream, skip *HttpUpstream) *HttpUpstream {
	for _, u := range set {
		if u != skip && !u.cbOpen.Load() {
			return u
		}
	}
	return nil
}

// RecordOutcome is called after each proxied request completes. ok=true
// for 2xx/3xx; ok=false for 5xx or transport errors. Trips the breaker
// when ConsecutiveFailures back-to-back failures land, and drives the
// half-open → closed / re-open transitions.
func (u *HttpUpstream) RecordOutcome(ok bool) {
	if !u.cbConfig.IsEnabled() {
		return
	}
	// Detect half-open state: cbOpen=true AND cbOpenedAtUnixMs=0.
	// On a half-open outcome, success closes the breaker; failure
	// re-trips it with a fresh cooldown timer.
	if u.cbOpen.Load() && u.cbOpenedAtUnixMs.Load() == 0 {
		if ok {
			u.cbOpen.Store(false)
			u.cbConsecFails.Store(0)
			return
		}
		u.cbOpenedAtUnixMs.Store(time.Now().UnixMilli())
		return
	}
	if ok {
		u.cbConsecFails.Store(0)
		return
	}
	fails := u.cbConsecFails.Add(1)
	if !u.cbOpen.Load() && int(fails) >= u.cbConfig.ConsecutiveFailures {
		u.cbOpen.Store(true)
		u.cbOpenedAtUnixMs.Store(time.Now().UnixMilli())
	}
}

// HttpUpstreamPool is a pool of HttpUpstreams that picks one per request
// via round-robin among the currently-healthy set. When zero are healthy,
// falls back to round-robin among ALL upstreams (better to try than 503
// the request — the LB in front is the last word on availability).
//
// Single-upstream pools (len(upstreams) == 1) skip all the pool logic
// and behave identically to v3 — zero overhead for the common case.
type HttpUpstreamPool struct {
	name string // pool label for /metrics gauges; matches the proxy service ("lcd", "rpc", "evm_rpc", "evm_rpc_ws")

	// upstreams is the current pool member set. Stored as an atomic
	// pointer so the hot Pick path reads lock-free (a single atomic
	// load returns an immutable slice); writers (Add/RemoveUpstream
	// from discovery, plus the initial seed in the constructor)
	// serialize on writeMu and publish a new slice via Store. Slices
	// are never mutated in place — that's the contract that keeps
	// readers safe without copying.
	upstreams atomic.Pointer[[]*HttpUpstream]
	writeMu   sync.Mutex

	// service is the upstream service this pool serves ("lcd", "rpc",
	// "evm_rpc"). Stored separately from name (= the metrics label)
	// because both happen to coincide today but the buildHttpUpstream
	// helper expects the service identifier. Keeping them distinct
	// avoids a future split-naming bug.
	service string
	// rewriteDirector is the per-request Director hook from the proxy
	// constructor; remembered so AddUpstream can build a new upstream
	// with the same per-rule behavior (header strip etc.) without
	// requiring the discoverer to re-supply it.
	rewriteDirector func(*http.Request)
	// modifyResponse / errorHandler are the per-upstream proxy hooks
	// the HttpProxy installs on every upstream (CORS application,
	// circuit-breaker outcome recording). Remembered so AddUpstream
	// can install them on a new upstream without the discoverer
	// needing visibility into HttpProxy internals.
	modifyResponse func(*HttpUpstream, *http.Response) error
	errorHandler   func(*HttpUpstream, http.ResponseWriter, *http.Request, error)

	idx      atomic.Uint32 // round-robin cursor
	strategy string        // weighted-round-robin | round-robin | least-conn | primary-failover
	maxRetry int           // retry on next upstream on transport failure
	log      *Entry

	// healthcheck lifecycle. hcMu protects hcCancel against concurrent
	// StartHealthchecks / Shutdown calls (Shutdown can race against
	// itself in particular under integration-test teardown).
	hcMu     sync.Mutex
	hcCtx    context.Context // valid only while hcCancel != nil; consumed by per-upstream goroutines started after StartHealthchecks
	hcCancel context.CancelFunc
	hcWG     sync.WaitGroup
	// hcSpawning is incremented under hcMu by any call that is about to
	// emit hcWG.Add(1) calls (StartHealthchecks runs a whole spawn loop;
	// AddUpstream emits at most one). Shutdown waits on it before
	// hcWG.Wait() so the WaitGroup Add-happens-before-Wait contract
	// holds even when Shutdown observes hcCancel=set while another
	// goroutine is between "I saw hcCancel != nil under hcMu" and
	// "I called hcWG.Add(1)". Without this barrier, the Wait can
	// return with counter=0 while a probe goroutine is still about
	// to be spawned, leaking it past Shutdown.
	hcSpawning sync.WaitGroup
	// hcShutdown is set true under hcMu by Shutdown. StartHealthchecks
	// and AddUpstream check it under hcMu and bail before spawning any
	// probe goroutines. Closes the production-real "Shutdown wins"
	// race: when Shutdown runs before StartHealthchecks has acquired
	// hcMu (the SIGTERM-during-fast-startup scenario), Shutdown observes
	// hcCancel == nil and returns immediately. Without this flag, the
	// late StartHealthchecks would then proceed, set hcCancel, and
	// spawn probes that nobody will ever cancel — leaking goroutines
	// past pool teardown. Pool is single-shot: once shut down it stays
	// shut down, matching the actual production lifecycle (boot →
	// serve → SIGTERM → exit). Restart requires a fresh pool.
	hcShutdown bool
}

// upstreamsSnapshot returns the current upstreams slice. The result is
// immutable for the duration of the caller's use — writers always
// publish a new slice via atomic Store, never mutate in place.
// Returns nil before the constructor has populated the pool; callers
// in steady state never see nil.
func (p *HttpUpstreamPool) upstreamsSnapshot() []*HttpUpstream {
	if ptr := p.upstreams.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// storeUpstreams publishes a new upstreams slice. Caller must hold
// writeMu so two concurrent writers don't lose each other's updates.
func (p *HttpUpstreamPool) storeUpstreams(s []*HttpUpstream) {
	p.upstreams.Store(&s)
}

// NewHttpUpstreamPool builds the pool from a list of NodeConfigs for a
// given service. service is "lcd" or "rpc" — picks the right port.
// rewriteDirector lets the caller add per-rule behavior (header strip,
// CORS strip) on top of the per-upstream Director.
func NewHttpUpstreamPool(
	nodes []NodeConfig,
	service string,
	rewriteDirector func(*http.Request),
	logger *Entry,
	opts ...HttpUpstreamPoolOption,
) (*HttpUpstreamPool, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("upstream pool: no nodes configured")
	}
	pool := &HttpUpstreamPool{
		name:            service,
		service:         service,
		rewriteDirector: rewriteDirector,
		log:             logger,
		strategy:        "weighted-round-robin",
	}
	for _, opt := range opts {
		opt(pool)
	}
	registerSharedMetrics()
	initial := make([]*HttpUpstream, 0, len(nodes))
	for _, n := range nodes {
		u, err := buildHttpUpstream(n, service, rewriteDirector)
		if err != nil {
			return nil, err
		}
		// Optimistic ONLY when no healthcheck is configured (there's no way
		// to confirm, so assume usable). When a healthcheck IS configured,
		// start unhealthy so /readyz gates the pod out of the load balancer
		// until the first probe confirms the upstream is actually reachable —
		// otherwise a fresh/scaled-up pod is marked Ready and 502s a burst of
		// traffic before its upstream connection is warm.
		initialHealthy := u.hcConfig == nil
		u.healthy.Store(initialHealthy)
		if !initialHealthy {
			// Gated on a healthcheck: preload the counter so the first probe
			// success (not HealthyAfter of them) clears the cold-start gate.
			u.successCount.Store(coldStartSuccessSeed(u.hcConfig))
		}
		// Seed the gauge so operators see every configured upstream in
		// /metrics from boot — even those that never receive traffic.
		setUpstreamHealthy(pool.name, u.Name, initialHealthy)
		initial = append(initial, u)
	}
	pool.storeUpstreams(initial)
	return pool, nil
}

// SetProxyHooks registers the modify-response and error-handler
// closures the HttpProxy installs on every per-upstream
// httputil.ReverseProxy. The pool needs visibility into these hooks
// so AddUpstream (driven by DNS discovery) can install them on
// dynamically added upstreams — otherwise a discovered upstream
// would proxy requests but would skip CORS post-processing and
// never feed the circuit breaker, a subtle observability /
// reliability gap that only surfaces in production with autoscaled
// headless services.
//
// Called exactly once from NewHttpProxy. Must be called BEFORE any
// AddUpstream invocation.
func (p *HttpUpstreamPool) SetProxyHooks(
	modifyResponse func(*HttpUpstream, *http.Response) error,
	errorHandler func(*HttpUpstream, http.ResponseWriter, *http.Request, error),
) {
	p.modifyResponse = modifyResponse
	p.errorHandler = errorHandler
}

// AddUpstream constructs an upstream from the supplied NodeConfig and
// publishes it into the pool atomically. Idempotent — re-adding an
// upstream that shares a Name with an existing member is a no-op
// (logged at debug). Picker callers see the new upstream on their
// very next Pick().
//
// When called after StartHealthchecks, the new upstream's healthcheck
// goroutine is launched immediately under the pool's healthcheck
// context, so the new upstream gets the same active-probe coverage as
// constructor-seeded ones. RemoveUpstream closes the per-upstream
// stop channel to drain the goroutine.
func (p *HttpUpstreamPool) AddUpstream(n NodeConfig) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	current := p.upstreamsSnapshot()
	for _, u := range current {
		if u.Name == n.Name {
			return nil // already in pool — discovery loop re-asserted the same IP
		}
	}
	u, err := buildHttpUpstream(n, p.service, p.rewriteDirector)
	if err != nil {
		return err
	}
	// Install the proxy hooks (CORS apply / circuit-breaker outcome /
	// retry-aware error handling) the proxy constructor installed on
	// the constructor-seeded upstreams. Without these the dynamically
	// added upstream would still serve requests, but it'd silently
	// skip CORS post-processing and never record outcomes into the
	// circuit breaker — a discoverable footgun in the metric stream.
	if p.modifyResponse != nil {
		u.proxy.ModifyResponse = func(resp *http.Response) error {
			return p.modifyResponse(u, resp)
		}
	}
	if p.errorHandler != nil {
		u.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			p.errorHandler(u, w, r, err)
		}
	}
	// Same gate as the constructor: a healthcheck-backed upstream added at
	// runtime (DNS discovery) starts unhealthy until its first probe confirms
	// reachability, so /readyz doesn't flip green on an unproven endpoint.
	addHealthy := u.hcConfig == nil
	u.healthy.Store(addHealthy)
	if !addHealthy {
		u.successCount.Store(coldStartSuccessSeed(u.hcConfig))
	}
	setUpstreamHealthy(p.name, u.Name, addHealthy)

	next := make([]*HttpUpstream, 0, len(current)+1)
	next = append(next, current...)
	next = append(next, u)
	p.storeUpstreams(next)

	// If healthchecks are already running, start one for the new
	// upstream under the per-upstream stop channel that buildHttpUpstream
	// already attached. RemoveUpstream can then drain just this
	// goroutine without canceling the pool-wide hcCtx (which would
	// tear down every other healthcheck too).
	//
	// hcWG.Add(1) is done UNDER hcMu (sequenced with Shutdown's hcCancel
	// observation) so Shutdown cannot observe hcCancel != nil and call
	// hcWG.Wait() while this Add(1) is still pending. The `go runHealthcheck`
	// happens outside hcMu so we don't hold the lock across the spawn.
	p.hcMu.Lock()
	// Bail if the pool has already been shut down — spawning a probe
	// goroutine now would leak it (Shutdown's cancel is gone). hcShutdown
	// is set under hcMu in Shutdown so we observe it consistently here.
	hcRunning := p.hcCancel != nil && !p.hcShutdown
	hcCtx := p.hcCtx
	spawn := hcRunning && u.hcConfig != nil
	if spawn {
		p.hcWG.Add(1)
	}
	p.hcMu.Unlock()
	if spawn {
		u.probeStarted.Store(true)
		go p.runHealthcheck(hcCtx, u)
	}
	return nil
}

// RemoveUpstream drops the named upstream from the pool. The picker
// stops returning it on the next call; in-flight requests already
// dispatched to that upstream complete naturally (we never abort).
// The upstream's healthcheck goroutine (if any) is signalled to
// exit by closing its stop channel; setUpstreamHealthy(false) flips
// the /metrics gauge so dashboards reflect the removal immediately.
// Idempotent — removing an upstream that isn't in the pool is a no-op.
func (p *HttpUpstreamPool) RemoveUpstream(name string) {
	// Phase 1: snapshot + swap under writeMu so the picker sees the
	// removal atomically. We deliberately do NOT hold writeMu across
	// the done-wait below: a probe stuck on its own hcConfig.Timeout
	// would otherwise pin the lock for seconds, blocking any
	// concurrent AddUpstream / RemoveUpstream — and a DNS-driven
	// reconciler that churns several upstreams per tick would
	// head-of-line-block on the first stuck one.
	p.writeMu.Lock()
	current := p.upstreamsSnapshot()
	idx := -1
	for i, u := range current {
		if u.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		p.writeMu.Unlock()
		return
	}
	removed := current[idx]
	next := make([]*HttpUpstream, 0, len(current)-1)
	next = append(next, current[:idx]...)
	next = append(next, current[idx+1:]...)
	p.storeUpstreams(next)
	p.writeMu.Unlock()

	// Phase 2: stop the per-upstream healthcheck goroutine. The
	// stop channel is always non-nil (buildHttpUpstream pre-creates
	// it), so this works for constructor-seeded discovery upstreams
	// as well as AddUpstream ones.
	close(removed.stop)
	// Wait for any in-flight probe to finish so its setUpstreamHealthy
	// call lands before our deleteUpstreamHealthy below. Without this
	// wait, a probe mid-HTTP-call would publish the series AFTER our
	// delete, leaving a stale `cosmoguard_upstream_healthy=0` row on
	// /metrics. probeStarted gates the wait: hcConfig may be set
	// without a goroutine actually running (pre-StartHealthchecks
	// add+remove, or post-Shutdown removal), and waiting on a `done`
	// channel nobody will ever close would burn the full timeout for
	// no reason. Cap the wait so a probe stuck on its hcConfig.Timeout
	// can't pin the caller indefinitely.
	if removed.probeStarted.Load() {
		select {
		case <-removed.done:
		case <-time.After(5 * time.Second):
		}
	}
	// Drop the gauge from /metrics so the dashboard doesn't keep
	// reporting a non-existent upstream as healthy. WithLabelValues
	// publishes the new value (0) for the soon-to-be-deleted series
	// in the same expression — but the immediate DeleteLabelValues
	// is what actually removes the time series. The Set(0) is
	// defensive in case another goroutine raced the gauge read.
	setUpstreamHealthy(p.name, name, false)
	deleteUpstreamHealthy(p.name, name)
}

func buildHttpUpstream(n NodeConfig, service string, rewriteDirector func(*http.Request)) (*HttpUpstream, error) {
	target, err := upstreamHTTPURL(n, service)
	if err != nil {
		return nil, err
	}
	// Pre-compute the Host header value: strip an explicit port when it
	// matches the scheme's default (80 for http, 443 for https) because
	// some TLS edges (Cloudflare, certain Caddy setups) return 421
	// Misdirected Request when the Host header carries `:443` against a
	// cert issued for the bare hostname. For non-default ports the
	// suffix must stay so the upstream node still routes correctly.
	hostHeader := hostHeaderForTarget(target)
	rp := httputil.NewSingleHostReverseProxy(target)
	origDirector := rp.Director
	rp.Director = func(r *http.Request) {
		origDirector(r)
		// httputil.NewSingleHostReverseProxy preserves the inbound Host
		// header by default — wrong when the upstream is a TLS edge
		// gateway / nginx that vhost-routes on Host (e.g. user's
		// lcd.<chain>.voluzi.com pattern: the gateway 404s anything
		// addressed to "127.0.0.1:xxxx"). Resetting r.Host to the
		// upstream's host puts the right vhost on the wire. For local
		// bare-node deployments the Host header is harmless metadata,
		// so this is safe across all topologies.
		//
		// X-Forwarded-Host preserves what the inbound client put on
		// the Host header so upstream / downstream services that
		// trust forwarded headers still see the original.
		//
		// ALWAYS overwrite — cosmoguard is the authoritative source
		// of these values for traffic it terminates. The previous
		// "only set when absent" logic let a malicious client preset
		// X-Forwarded-Host / X-Forwarded-Proto to spoof upstream
		// apps that auto-route, redirect, or scope cookies on these
		// headers (an internal "admin" host could be impersonated by
		// any internet client supplying the header). Standard
		// reverse-proxy hardening.
		if r.Host != "" {
			r.Header.Set("X-Forwarded-Host", r.Host)
		}
		proto := "http"
		if r.TLS != nil {
			proto = "https"
		}
		r.Header.Set("X-Forwarded-Proto", proto)
		r.Host = hostHeader
		if rewriteDirector != nil {
			rewriteDirector(r)
		}
		// Propagate the active span as W3C traceparent so the upstream
		// node (and anything downstream of it) joins the same trace.
		// No-op when tracing is disabled — the global propagator is
		// still installed in that case (SetupTracing), so we always
		// inject either the current span or whatever traceparent the
		// inbound request carried.
		InjectHTTPHeaders(r.Context(), r.Header)
	}
	w := n.Weight
	if w < 1 {
		w = 1
	}
	u := &HttpUpstream{
		Name:   n.Name,
		Target: target,
		Weight: w,
		proxy:  rp,
		// Pre-create stop so RemoveUpstream can drain this upstream's
		// healthcheck goroutine regardless of whether the upstream
		// arrived via the constructor or via AddUpstream. See the
		// field comment for the gauge-leak this prevents.
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if n.Healthcheck.IsEnabled() {
		u.hcConfig = n.Healthcheck
		// Honor Healthcheck.Service so a single probe (e.g. always RPC
		// /status) gives one source of truth for "is this node alive"
		// across the LCD / RPC / EVM pools. Default after config-prep
		// is "rpc", so out-of-the-box every pool probes the RPC URL —
		// matching the documented intent of the field. When the service
		// matches the proxy's own service, or no service is set, the
		// existing target URL is reused.
		probeTarget := target
		if hs := n.Healthcheck.Service; hs != "" && hs != service {
			pt, err := upstreamHTTPURL(n, hs)
			if err != nil {
				return nil, fmt.Errorf("healthcheck.service=%q: %w", hs, err)
			}
			probeTarget = pt
		}
		// Shared with the gRPC pool builder so both normalize the
		// operator-supplied path identically (leading slash, query split).
		u.probeAddr = healthcheckProbeURL(probeTarget.Scheme, probeTarget.Host, n.Healthcheck.Path)
	}
	if n.CircuitBreaker.IsEnabled() {
		u.cbConfig = n.CircuitBreaker
	}
	return u, nil
}

// hostHeaderForTarget returns the value to set on the outbound request's
// Host header for a given upstream URL. When the URL's port matches the
// scheme's default (80 for http, 443 for https) the port suffix is
// stripped — Cloudflare and some Caddy configurations 421-reject
// requests whose Host header carries `:443` against a cert issued for
// the bare hostname. Non-default ports stay so direct ip:port
// deployments (the v3 default) keep routing.
//
// IPv6 hosts are returned with brackets restored (`[::1]` rather than
// the bare `::1` that url.URL.Hostname returns) so the result is a
// valid HTTP/1.1 Host header per RFC 7230 § 5.4. Scheme comparison is
// case-insensitive because RFC 3986 § 3.1 declares schemes case-
// insensitive on the wire — url.Parse doesn't normalize.
func hostHeaderForTarget(target *url.URL) string {
	port := target.Port()
	if port == "" {
		return target.Host
	}
	scheme := strings.ToLower(target.Scheme)
	defaultMatched := (scheme == "https" && port == "443") || (scheme == "http" && port == "80")
	if !defaultMatched {
		return target.Host
	}
	host := target.Hostname()
	if strings.Contains(host, ":") {
		// IPv6 literal — Hostname() unwraps the brackets; restore them
		// for the wire format.
		return "[" + host + "]"
	}
	return host
}

// HttpUpstreamPoolOption configures a pool at construction time.
type HttpUpstreamPoolOption func(*HttpUpstreamPool)

// WithUpstreamStrategy selects the picker algorithm. Valid values:
// "weighted-round-robin" (default), "round-robin", "least-conn",
// "primary-failover". Unknown values fall back to weighted-round-robin.
func WithUpstreamStrategy(s string) HttpUpstreamPoolOption {
	return func(p *HttpUpstreamPool) {
		switch s {
		case "weighted-round-robin", "round-robin", "least-conn", "primary-failover":
			p.strategy = s
		}
	}
}

// WithUpstreamRetries sets the number of next-upstream retries on a
// transport-level failure (connect refused, timeout, etc.). 0 disables
// retries (the legacy behavior).
func WithUpstreamRetries(n int) HttpUpstreamPoolOption {
	return func(p *HttpUpstreamPool) {
		p.maxRetry = n
	}
}

// WithPoolName overrides the pool's metrics label (the `pool=...` tag on
// cosmoguard_upstream_healthy). Defaults to the `service` arg of
// NewHttpUpstreamPool. Tests use this to avoid colliding with other
// pools that share the same service identifier — assigning the label
// AFTER construction would leak a "service-named" gauge series for the
// seed upstream because the constructor already published one. Pass
// the option so the constructor seeds the metric with the right label
// from the start.
func WithPoolName(name string) HttpUpstreamPoolOption {
	return func(p *HttpUpstreamPool) {
		if name != "" {
			p.name = name
		}
	}
}

// Pick returns the next upstream to use for a request, dispatching to the
// configured strategy:
//
//   - "weighted-round-robin" (default) — rotation slots proportional to
//     node.weight.
//   - "round-robin" — equal share regardless of weight.
//   - "least-conn" — pick the upstream with the lowest current in-flight
//     count among healthy. Ties broken by round-robin cursor.
//   - "primary-failover" — always pick the FIRST healthy node in the
//     configured order (priority by position). Falls over to the next
//     when the primary is down.
//
// All strategies fall back to "any upstream" when zero are healthy —
// better to try than to drop the request entirely. The LB in front is
// the last word on availability.
func (p *HttpUpstreamPool) Pick() *HttpUpstream {
	current := p.upstreamsSnapshot()
	if len(current) == 0 {
		// Empty pool — every node was removed (DNS discovery
		// returned zero IPs, or RemoveUpstream drained the last
		// constructor-seeded entry). Returning nil lets the
		// caller serve a 503 instead of dividing by zero deep
		// in pickRR's modulo or indexing tied[0] in pickLeastConn.
		return nil
	}
	if len(current) == 1 {
		// Fast path: single-upstream pool. No mutex, no atomics, no
		// health logic — behaves identically to a v3 single reverse-
		// proxy setup. The common case.
		return current[0]
	}
	// Thread the SAME snapshot through healthySet + picker. Previously
	// healthySet took an independent second snapshot, so a concurrent
	// RemoveUpstream draining the last upstreams between here and there
	// yielded an empty set and the pickers divided by zero / indexed [0].
	healthy := p.healthySet(current)
	picked := p.pickFromSet(healthy)
	if picked != nil && !picked.consumeHalfOpenProbe() {
		// Lost the half-open probe token (concurrent probe) — prefer a
		// fully-closed upstream so we don't double-probe a recovering node.
		if alt := p.firstClosed(healthy, picked); alt != nil {
			return alt
		}
	}
	return picked
}

// pickFromSet dispatches to the configured strategy picker.
func (p *HttpUpstreamPool) pickFromSet(set []*HttpUpstream) *HttpUpstream {
	switch p.strategy {
	case "round-robin":
		return p.pickRR(set)
	case "least-conn":
		return p.pickLeastConn(set)
	case "primary-failover":
		return p.pickPrimary(set)
	default:
		return p.pickWeightedRR(set)
	}
}

// healthySet returns upstreams currently in the picker (healthy AND
// circuit-closed). When zero qualify, returns all upstreams so we can
// still try the request.
func (p *HttpUpstreamPool) healthySet(current []*HttpUpstream) []*HttpUpstream {
	out := current[:0:0]
	for _, u := range current {
		if u.healthy.Load() && !u.CircuitOpen() {
			out = append(out, u)
		}
	}
	if len(out) == 0 {
		return current
	}
	return out
}

func (p *HttpUpstreamPool) pickRR(set []*HttpUpstream) *HttpUpstream {
	if len(set) == 0 {
		return nil
	}
	// Use uint32 arithmetic end-to-end so the % stays non-negative when
	// idx wraps past 2^32. The previous `int(i)-1` form went negative on
	// the wrap boundary (i=0 → -1 → set[-1] panic). After ~4B requests
	// is rare but not impossible on a long-lived high-RPS deployment.
	i := p.idx.Add(1) - 1
	return set[i%uint32(len(set))]
}

func (p *HttpUpstreamPool) pickWeightedRR(set []*HttpUpstream) *HttpUpstream {
	if len(set) == 0 {
		return nil
	}
	totalWeight := 0
	for _, u := range set {
		totalWeight += u.Weight
	}
	if totalWeight <= 0 {
		return p.pickRR(set)
	}
	// Same wrap-safety as pickRR — keep the modulo on the unsigned form.
	pos := int((p.idx.Add(1) - 1) % uint32(totalWeight))
	for _, u := range set {
		if pos < u.Weight {
			return u
		}
		pos -= u.Weight
	}
	return set[len(set)-1] // unreachable
}

func (p *HttpUpstreamPool) pickLeastConn(set []*HttpUpstream) *HttpUpstream {
	if len(set) == 0 {
		return nil
	}
	// Find min in-flight; collect all upstreams at that minimum; round-
	// robin within the tie set so ties don't perpetually slam one node.
	minF := int32(-1)
	for _, u := range set {
		f := u.inFlight.Load()
		if minF < 0 || f < minF {
			minF = f
		}
	}
	tied := set[:0:0]
	for _, u := range set {
		if u.inFlight.Load() == minF {
			tied = append(tied, u)
		}
	}
	// inFlight can shift between the two passes, emptying tied; fall back
	// to the full (non-empty) set rather than panicking in pickRR.
	if len(tied) == 0 {
		return p.pickRR(set)
	}
	if len(tied) == 1 {
		return tied[0]
	}
	return p.pickRR(tied)
}

func (p *HttpUpstreamPool) pickPrimary(set []*HttpUpstream) *HttpUpstream {
	// "Primary" is the first node in the configured order that's still
	// in the healthy set. Configured order is determined by position in
	// the nodes: list — the first node is primary, second is the
	// immediate failover, and so on.
	if len(set) == 0 {
		return nil
	}
	for _, u := range p.upstreamsSnapshot() {
		for _, h := range set {
			if u == h {
				return u
			}
		}
	}
	return set[0]
}

// retryStateKey is a context key. When present, the installed
// per-upstream ErrorHandler (see http_proxy.go) stores the transport error
// here and skips writing a 502 — so the retry loop can hand the response
// to the next upstream cleanly. Absent: behave like a normal proxy.
type retryStateKeyT struct{}

var retryStateKey = retryStateKeyT{}

type retryState struct {
	failed bool
}

// retryStateFromCtx returns the retry state attached to ctx, or nil when
// retries are disabled for this request.
func retryStateFromCtx(ctx context.Context) *retryState {
	if v := ctx.Value(retryStateKey); v != nil {
		return v.(*retryState)
	}
	return nil
}

// requestStatsKey is a context key carrying the picked upstream's name
// (and a slot for future stat enrichment) back to the calling proxy so
// metrics can label observations with `upstream`.
type requestStatsKeyT struct{}

var requestStatsKey = requestStatsKeyT{}

// RequestStats is the per-request metadata that proxy + pool fill in
// for the metrics layer to read back. Both fields are bounded (rule
// tags come from config, upstream names from config) so they're safe
// as Prometheus labels.
type RequestStats struct {
	// Upstream is the name of the node the pool dispatched to. Empty
	// when no upstream was selected (e.g. deny short-circuit).
	Upstream string
	// RuleTag is the operator-supplied tag of the rule that matched.
	// "default" when no rule matched and the default-action path
	// served the request.
	RuleTag string
	// IdentityName is the resolved identity (api-key alias, JWT
	// subject). Empty for anonymous / unauthenticated requests. Used
	// by the request-log ring; not exported as a Prometheus label.
	IdentityName string
}

// RequestStatsFromCtx returns the *RequestStats attached to ctx, or nil
// when the request didn't go through a pool that filled it (single-
// upstream fast paths still set this so the label is populated).
func RequestStatsFromCtx(ctx context.Context) *RequestStats {
	if v := ctx.Value(requestStatsKey); v != nil {
		return v.(*RequestStats)
	}
	return nil
}

// WithRequestStats returns ctx with a fresh *RequestStats attached. The
// proxy must call this before invoking pool.ServeHTTP so the pool has a
// slot to fill in.
func WithRequestStats(ctx context.Context) (context.Context, *RequestStats) {
	stats := &RequestStats{}
	return context.WithValue(ctx, requestStatsKey, stats), stats
}

// ServeHTTP dispatches the request through the picker, tracks in-flight
// load, and on a connect-time failure with an idempotent method retries
// the next upstream up to maxRetry times. Once the upstream has begun
// writing a response back to the client, retries are no longer safe and
// the failure is surfaced.
//
// Idempotency rules:
//   - GET, HEAD, OPTIONS — always idempotent
//   - POST/PUT/DELETE/PATCH — non-idempotent; never retried
//   - JSON-RPC over POST is non-idempotent at the transport level because
//     a method like broadcast_tx_sync mutates state. Per-method retry
//     belongs in the JSON-RPC handler, not here.
func (p *HttpUpstreamPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	stats := RequestStatsFromCtx(r.Context())
	// Exactly one real upstream fetch per ServeHTTP call (internal retries
	// collapse to one logical fetch). Cache hits and coalesced single-flight
	// waiters never reach this method, so this counts only genuine upstream
	// calls. Deferred so the labels reflect the upstream ultimately dispatched
	// to; no-ops when no upstream was selected.
	if stats != nil {
		defer func() { recordUpstreamRequest(p.name, stats.Upstream, stats.RuleTag) }()
	}
	current := p.upstreamsSnapshot()
	// Fast path — single upstream, no retry/load logic.
	if len(current) == 1 {
		u := current[0]
		if stats != nil {
			stats.Upstream = u.Name
		}
		u.inFlight.Add(1)
		defer u.inFlight.Add(-1)
		u.proxy.ServeHTTP(w, r)
		return
	}

	canRetry := p.maxRetry > 0 && isIdempotentTransport(r.Method)

	attempts := 1
	if canRetry {
		attempts = p.maxRetry + 1
	}

	// Without retries, just dispatch through the picker once.
	if !canRetry {
		u := p.Pick()
		if u == nil {
			// Drained pool (no upstreams left). Surface 503 so
			// the LB ahead of us can fail over instead of having
			// the proxy panic on a nil dereference.
			http.Error(w, "no upstream available", http.StatusServiceUnavailable)
			return
		}
		if stats != nil {
			stats.Upstream = u.Name
		}
		u.inFlight.Add(1)
		defer u.inFlight.Add(-1)
		u.proxy.ServeHTTP(w, r)
		return
	}

	tried := make(map[*HttpUpstream]struct{}, attempts)
	state := &retryState{}
	rCtx := context.WithValue(r.Context(), retryStateKey, state)
	rWithCtx := r.WithContext(rCtx)

	for i := 0; i < attempts; i++ {
		u := p.pickNotTried(tried)
		if u == nil {
			// No untried upstream left — exhausted.
			break
		}
		tried[u] = struct{}{}
		if stats != nil {
			stats.Upstream = u.Name
		}

		// Reset for this attempt; install a retry-aware writer that
		// remembers whether any byte already went to the client.
		state.failed = false
		rw := &retryAwareWriter{ResponseWriter: w}

		u.inFlight.Add(1)
		u.proxy.ServeHTTP(rw, rWithCtx)
		u.inFlight.Add(-1)

		// Success path (no transport failure recorded) or response
		// already committed — return.
		if !state.failed || rw.wroteHeader {
			return
		}
		if p.log != nil {
			p.log.WithFields(Fields{
				"upstream": u.Name,
				"target":   u.Target.String(),
				"attempt":  i + 1,
			}).Debug("retrying transport failure on next upstream")
		}
	}
	// Exhausted retries — surface 502.
	w.WriteHeader(http.StatusBadGateway)
}

// pickNotTried returns an upstream that hasn't been tried yet. Honors
// healthy + circuit gating like Pick() does. Returns nil when every
// upstream in the pool has been tried (or there are none).
func (p *HttpUpstreamPool) pickNotTried(tried map[*HttpUpstream]struct{}) *HttpUpstream {
	current := p.upstreamsSnapshot()
	if len(tried) >= len(current) {
		return nil
	}
	// First try the configured strategy among untried upstreams.
	healthy := p.healthySet(current)
	candidates := healthy[:0:0]
	for _, u := range healthy {
		if _, seen := tried[u]; !seen {
			candidates = append(candidates, u)
		}
	}
	if len(candidates) > 0 {
		picked := p.pickFromSet(candidates)
		// Retry picks must consume the half-open probe token too, exactly
		// like Pick — otherwise a retryable request routed here after
		// cooldown never transitions the breaker to half-open (openedAt
		// stays non-zero), so RecordOutcome can't resolve it and the
		// breaker stays open while still serving traffic. On a lost token,
		// prefer a fully-closed untried candidate.
		if picked != nil && !picked.consumeHalfOpenProbe() {
			if alt := p.firstClosed(candidates, picked); alt != nil {
				return alt
			}
		}
		return picked
	}
	// Fall back to any untried upstream — health bit may be stale.
	for _, u := range current {
		if _, seen := tried[u]; !seen {
			return u
		}
	}
	return nil
}

// retryAwareWriter wraps an http.ResponseWriter to record whether any
// upstream byte (header or body) has been written. Once true, the request
// is no longer eligible for retry because the client has already received
// data.
type retryAwareWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *retryAwareWriter) WriteHeader(code int) {
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *retryAwareWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher so SSE / chunked responses can stream
// through the wrapper. Marks the response as committed.
func (w *retryAwareWriter) Flush() {
	w.wroteHeader = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ReadFrom delegates to the underlying writer's io.ReaderFrom when
// available — preserves the zero-copy fast path Go's net/http uses
// for the response body. Without this, wrapping the writer disables
// the optimization (a measurable hit on large LCD responses).
// Counts as committed since any byte handed off here goes to the
// client.
func (w *retryAwareWriter) ReadFrom(src io.Reader) (int64, error) {
	w.wroteHeader = true
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(w.ResponseWriter, src)
}

// isIdempotentTransport reports whether an HTTP method is safe to replay
// at the transport level. Only the methods that the RFC marks idempotent
// AND that Cosmos nodes don't use for state changes qualify.
func isIdempotentTransport(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

// StartHealthchecks spawns one goroutine per upstream that has a
// healthcheck configured. Stops on Shutdown.
//
// Idempotent within a single pool lifetime: calling twice without an
// intervening Shutdown is a no-op (second call sees hcCancel != nil and
// returns). Calling AFTER Shutdown is also a no-op — the pool is
// single-shot (see Shutdown for the rationale) and StartHealthchecks
// observes the hcShutdown latch and bails. Restart requires a fresh
// pool.
func (p *HttpUpstreamPool) StartHealthchecks() {
	p.hcMu.Lock()
	// Pool is single-shot: once Shutdown ran, refuse to start. This closes
	// the SIGTERM-during-fast-startup race where Shutdown takes hcMu before
	// StartHealthchecks does, sees hcCancel == nil, and returns; a late
	// StartHealthchecks then proceeds, sets hcCancel, and spawns probes
	// with no cancellation hook — they would leak past pool teardown.
	if p.hcShutdown {
		p.hcMu.Unlock()
		return
	}
	if p.hcCancel != nil {
		p.hcMu.Unlock()
		return // already running
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.hcCtx = ctx
	p.hcCancel = cancel
	// Reserve a slot on hcSpawning *under hcMu* so Shutdown — which
	// also observes hcCancel under hcMu — sees a non-zero counter the
	// instant it could trigger a Wait(). Without this, Shutdown's
	// hcWG.Wait can return with counter=0 while we're still about to
	// call hcWG.Add(1) per upstream below.
	p.hcSpawning.Add(1)
	p.hcMu.Unlock()
	// LIFO defer ordering: this hcSpawning.Done() is declared FIRST, so
	// it fires LAST — after the function body completes AND after the
	// `defer p.writeMu.Unlock()` declared below has run. By the time
	// Done lands, every per-upstream hcWG.Add(1) emitted by the spawn
	// loop has already been called, so Shutdown's hcSpawning.Wait then
	// hcWG.Wait observes a settled counter. Reordering this defer past
	// the writeMu one would silently break the Add-before-Wait contract,
	// so the declaration site must stay above the spawn loop.
	defer p.hcSpawning.Done()

	// Hold writeMu across the spawn loop so a concurrent RemoveUpstream
	// can't observe probeStarted=false on an upstream we're about to
	// spawn a goroutine for. Without this, the interleave is:
	//   - we snapshot upstreams here, the snapshot contains "foo"
	//   - reconciler calls RemoveUpstream("foo"): Phase 1 swaps it out,
	//     Phase 2 sees probeStarted=false and skips the done-wait,
	//     then deleteUpstreamHealthy clears the series
	//   - we then Store(true) + go runHealthcheck for foo, whose
	//     initial p.probe re-publishes the gauge via setUpstreamHealthy
	// Holding writeMu (same lock RemoveUpstream takes for Phase 1)
	// linearizes the two paths: either Remove observes our Store(true)
	// and waits for done, or the spawn loop sees foo already gone from
	// the snapshot. StartHealthchecks is called once per pool lifetime,
	// so the extra serialization is negligible.
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	for _, u := range p.upstreamsSnapshot() {
		if u.hcConfig == nil {
			continue
		}
		p.hcWG.Add(1)
		u.probeStarted.Store(true)
		go p.runHealthcheck(ctx, u)
	}
}

// Shutdown stops the healthcheck goroutines and marks the pool as
// permanently shut down. Safe to call multiple times AND safe under
// concurrent calls — the swap on hcCancel is done under hcMu, then
// Wait runs outside the lock so a slow healthcheck doesn't block a
// parallel Shutdown caller.
//
// Pool is single-shot: once Shutdown has been called, any future
// StartHealthchecks or AddUpstream will refuse to spawn new probe
// goroutines. This is the operative production contract — the only
// realistic lifecycle is boot → serve → SIGTERM → exit, and a pool
// that could be revived after Shutdown would have to plumb a new
// hcCancel into every still-running probe (it can't — they hold the
// old context). Refusing the restart is the only race-free option.
//
// Ordering matters: hcShutdown is set BEFORE observing hcCancel under
// the same hcMu hold. That's what makes the latch race-free — any
// late StartHealthchecks acquiring hcMu after this critical section
// sees hcShutdown=true and bails, regardless of whether hcCancel was
// nil (Shutdown-before-Start case) or not (normal teardown).
func (p *HttpUpstreamPool) Shutdown() {
	p.hcMu.Lock()
	// Latch the pool shut: any future StartHealthchecks / AddUpstream
	// will read this under hcMu and bail before touching hcCancel,
	// closing the "Shutdown took the lock first" interleave that would
	// otherwise leak a late-spawned probe goroutine.
	p.hcShutdown = true
	cancel := p.hcCancel
	p.hcCancel = nil
	p.hcCtx = nil
	p.hcMu.Unlock()
	if cancel == nil {
		// Either no healthchecks ever started (hcShutdown latched first),
		// or a parallel Shutdown already captured the cancel. In the
		// parallel-Shutdown case the sibling caller is still inside its
		// hcSpawning/hcWG drain; we mirror its Wait calls so every
		// Shutdown caller has the same post-condition (no probe
		// goroutines outlive any Shutdown return). Wait is a no-op when
		// the counter is already 0.
		p.hcSpawning.Wait()
		p.hcWG.Wait()
		return
	}
	cancel()
	// Drain any in-flight StartHealthchecks/AddUpstream spawn paths so
	// every hcWG.Add(1) they will emit has landed before we Wait. Without
	// this barrier, a Shutdown that sees hcCancel != nil (set by a
	// StartHealthchecks that hasn't yet reached its hcWG.Add(1) calls)
	// would observe counter=0 and return prematurely while a probe
	// goroutine is still about to be spawned — the classic
	// sync.WaitGroup "Add must happen-before Wait when counter is 0"
	// hazard.
	p.hcSpawning.Wait()
	p.hcWG.Wait()
}

func (p *HttpUpstreamPool) runHealthcheck(ctx context.Context, u *HttpUpstream) {
	defer p.hcWG.Done()
	// Signal RemoveUpstream that we've finished — including any
	// in-flight probe whose setUpstreamHealthy call would otherwise
	// race the gauge delete. close(u.done) is the last thing this
	// goroutine does so the wait in RemoveUpstream is tight.
	defer close(u.done)
	// Bail before the initial probe if we were spawned after the pool
	// was already shut down (Shutdown's cancel()) or this upstream was
	// already removed (close(u.stop)). The initial probe would otherwise
	// run with a cancelled context, fail, and flip the gauge to
	// unhealthy for a series that should be gone — exactly the
	// gauge-leak symptom this whole subsystem is trying to prevent.
	select {
	case <-ctx.Done():
		return
	case <-u.stop:
		return
	default:
	}
	client := &http.Client{Timeout: u.hcConfig.Timeout}
	ticker := time.NewTicker(u.hcConfig.Interval)
	defer ticker.Stop()

	// One immediate probe so the initial state isn't a guess.
	p.probe(ctx, u, client)

	for {
		select {
		case <-ctx.Done():
			return
		case <-u.stop:
			// Per-upstream stop signal — RemoveUpstream closes this
			// so we exit without draining the entire pool. nil chan
			// is permanently non-ready, so the constructor-seeded
			// (no per-upstream stop) path falls through correctly.
			return
		case <-ticker.C:
			p.probe(ctx, u, client)
		}
	}
}

func (p *HttpUpstreamPool) probe(ctx context.Context, u *HttpUpstream, client *http.Client) {
	probeCtx, cancel := context.WithTimeout(ctx, u.hcConfig.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, u.probeAddr, nil)
	if err != nil {
		if !probeAborted(ctx, u.stop) {
			p.recordProbeResult(u, false, err)
		}
		return
	}
	resp, err := client.Do(req)
	// Suppress the probe verdict if the pool was shut down (parent ctx
	// cancelled) or this upstream was removed (u.stop closed) while
	// the request was in flight. Publishing it via recordProbeResult
	// would re-flip the gauge series after RemoveUpstream's
	// deleteUpstreamHealthy / Shutdown's drain — exactly the
	// gauge-leak hazard the rest of this subsystem fights.
	if probeAborted(ctx, u.stop) {
		if err == nil {
			resp.Body.Close()
		}
		return
	}
	if err != nil {
		p.recordProbeResult(u, false, err)
		return
	}
	resp.Body.Close()
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	p.recordProbeResult(u, ok, nil)
}

// probeAborted reports whether the parent healthcheck context has been
// cancelled (pool Shutdown) or the per-upstream stop channel has been
// closed (RemoveUpstream). Probes that complete after either signal
// must not publish their verdict — the metric series for this upstream
// has either been cleaned up or is about to be.
//
// Both pool types share the same lifecycle signals (ctx + per-upstream
// stop channel); the helper takes the channel directly so the gRPC
// pool can call it without type-specific shims.
func probeAborted(ctx context.Context, stop <-chan struct{}) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// coldStartSuccessSeed returns the value to preload an upstream's success
// counter with when it starts gated-unhealthy (a healthcheck is configured but
// hasn't run yet). Seeding it to HealthyAfter-1 means the FIRST successful
// probe clears the cold-start readiness gate, rather than waiting for
// HealthyAfter probes — HealthyAfter is flap protection for RUNTIME recovery
// (a probe failure resets the counter to 0), not a cold-start delay.
func coldStartSuccessSeed(hc *NodeHealthcheckConfig) int32 {
	if hc == nil || hc.HealthyAfter <= 1 {
		return 0
	}
	return int32(hc.HealthyAfter - 1)
}

// recordProbeResult applies the consecutive-success / consecutive-failure
// thresholds so transient blips don't flap the upstream in/out of the
// picker.
func (p *HttpUpstreamPool) recordProbeResult(u *HttpUpstream, ok bool, err error) {
	if ok {
		u.failCount.Store(0)
		s := u.successCount.Add(1)
		if !u.healthy.Load() && int(s) >= u.hcConfig.HealthyAfter {
			u.healthy.Store(true)
			setUpstreamHealthy(p.name, u.Name, true)
			p.log.WithFields(Fields{
				"upstream": u.Name,
				"target":   u.Target.String(),
			}).Info("upstream healthy")
		}
		return
	}
	u.successCount.Store(0)
	f := u.failCount.Add(1)
	if u.healthy.Load() && int(f) >= u.hcConfig.UnhealthyAfter {
		u.healthy.Store(false)
		setUpstreamHealthy(p.name, u.Name, false)
		fields := Fields{
			"upstream": u.Name,
			"target":   u.Target.String(),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		p.log.WithFields(fields).Warn("upstream marked unhealthy")
	}
}

// AnyHealthy reports whether at least one upstream is in the healthy set.
// Used by /readyz to gate readiness on actual upstream availability.
func (p *HttpUpstreamPool) AnyHealthy() bool {
	for _, u := range p.upstreamsSnapshot() {
		if u.healthy.Load() {
			return true
		}
	}
	return false
}

// Upstreams returns a snapshot of the current upstreams. The returned
// slice is immutable for the caller's use (writers always publish a
// new slice) — callers may iterate without holding any lock and may
// retain the slice header indefinitely. Exposed for test inspection
// and the /info handler / web UI.
func (p *HttpUpstreamPool) Upstreams() []*HttpUpstream {
	return p.upstreamsSnapshot()
}
