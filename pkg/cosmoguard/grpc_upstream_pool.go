package cosmoguard

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// GrpcUpstream is a single upstream gRPC node — name, dial target, weight,
// the dialled ClientConn, and per-upstream load / circuit-breaker state.
//
// Health is sourced from two signals: an active HTTP probe against the
// node's configured Healthcheck.Service URL (so an upstream whose host
// fails DNS or whose node process is down is detected even before any
// RPC is attempted — closing the cold-start bleed window where lazy
// grpc.Dial leaves a broken peer in connectivity.Idle until the first
// RPC tries it), AND grpc.ClientConn.GetState(). Both must look good
// for Pick() to route to this upstream.
type GrpcUpstream struct {
	Name   string
	Target string
	Weight int

	conn *grpc.ClientConn

	// inFlight tracks active RPCs against this upstream. Incremented by
	// the proxy before dispatch, decremented after.
	inFlight atomic.Int32

	// Active healthcheck — populated when n.Healthcheck.IsEnabled()
	// returns true (i.e. Enable is nil-default-enabled or *Enable is
	// true). nil means "no active probing, rely on conn state only"
	// (the old behaviour, kept for back-compat with single-node configs).
	hcConfig     *NodeHealthcheckConfig
	probeAddr    string
	healthy      atomic.Bool
	successCount atomic.Int32
	failCount    atomic.Int32

	// Circuit breaker — same semantics as the HTTP upstream's.
	cbConfig         *CircuitBreakerConfig
	cbOpen           atomic.Bool
	cbConsecFails    atomic.Int32
	cbOpenedAtUnixMs atomic.Int64

	// stop is closed by RemoveUpstream to signal this upstream's
	// healthcheck goroutine to exit. Always non-nil — buildGrpcUpstream
	// pre-creates it so discovery-expanded upstreams (which arrive via
	// the pool constructor) drain correctly on later removal. See
	// HttpUpstream.stop for the same idiom and the gauge-leak rationale.
	stop chan struct{}
	// done is closed by the healthcheck goroutine when it exits.
	// RemoveUpstream waits on it after close(stop) so an in-flight
	// probe's setUpstreamHealthy call lands before deleteUpstreamHealthy
	// — same race-avoidance contract as HttpUpstream.done.
	done chan struct{}
	// probeStarted gates RemoveUpstream's done-wait — see
	// HttpUpstream.probeStarted for the full rationale (avoid
	// blocking for the full 5s timeout on upstreams whose probe
	// goroutine was never spawned).
	probeStarted atomic.Bool
}

// CircuitOpen reports (READ-ONLY) whether the breaker currently blocks
// routing. It never mutates breaker state, so healthySet / status readers
// can call it for every upstream without side effects.
//
// The three-state machine:
//
//	closed     — cbOpen=false, cbOpenedAtUnixMs=0. All RPCs admitted.
//	open       — cbOpen=true,  cbOpenedAtUnixMs>0. Rejected until cooldown
//	             elapses, after which the upstream becomes probe-eligible.
//	half-open  — cbOpen=true,  cbOpenedAtUnixMs=0. A probe RPC is in flight
//	             (its token was consumed by consumeHalfOpenProbe); further
//	             callers stay rejected until RecordOutcome resolves it.
//
// CRITICAL: consuming the single half-open probe token is deliberately NOT
// done here. It used to be — CircuitOpen did the CAS to openedAt=0 as a
// side effect of the healthySet scan. But healthySet calls this for EVERY
// upstream, so an upstream whose token was consumed by the scan but that
// the picker then DIDN'T select never received an RPC, RecordOutcome never
// ran, and openedAt stayed 0 forever → the upstream was wedged out of
// rotation permanently. The token is now consumed in consumeHalfOpenProbe,
// called ONLY for the upstream Pick actually returns, guaranteeing every
// consumed token is paired with an RPC (and its RecordOutcome).
func (u *GrpcUpstream) CircuitOpen() bool {
	if !u.cbOpen.Load() {
		return false
	}
	if u.cbConfig == nil {
		return true
	}
	openedAt := u.cbOpenedAtUnixMs.Load()
	if openedAt == 0 {
		// A probe is already in flight.
		return true
	}
	// Rejected until cooldown elapses; after that the upstream is
	// probe-eligible (read-only — no token consumed here).
	return time.Now().UnixMilli()-openedAt < u.cbConfig.CooldownPeriod.Milliseconds()
}

// consumeHalfOpenProbe attempts to consume the single half-open probe token.
// It returns true when the caller MAY dispatch to this upstream:
//   - closed breaker → true (the common case, no token involved);
//   - open + cooldown elapsed + this caller won the CAS → true (it is THE
//     probe, so its RPC + RecordOutcome resolves the breaker);
//   - open + cooldown elapsed + this caller LOST the CAS to a concurrent
//     probe → false (another probe is already in flight; dispatching here
//     would send a second, defeating the single-probe guarantee);
//   - open + cooldown not elapsed → false (still tripped).
//
// Pick/pickNotTried call this only for the upstream they intend to return and
// reselect/reject on false, so a consumed token is always paired with exactly
// one RPC and the upstream can never be wedged.
func (u *GrpcUpstream) consumeHalfOpenProbe() bool {
	if !u.cbOpen.Load() || u.cbConfig == nil {
		return true
	}
	openedAt := u.cbOpenedAtUnixMs.Load()
	if openedAt == 0 {
		// A probe is already in flight — reject.
		return false
	}
	if time.Now().UnixMilli()-openedAt < u.cbConfig.CooldownPeriod.Milliseconds() {
		return false
	}
	// Cooldown elapsed: exactly one caller wins the CAS and becomes the
	// probe; losers must NOT dispatch. Reset consecFails so a single failure
	// on the probe re-trips the breaker.
	if u.cbOpenedAtUnixMs.CompareAndSwap(openedAt, 0) {
		u.cbConsecFails.Store(0)
		return true
	}
	return false
}

// grpcNeutral reports whether a gRPC error is pure client-side noise (the
// caller went away or gave up before the upstream's health could be
// observed) that must not move the breaker in either direction.
func grpcNeutral(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// grpcClientCaused reports whether a gRPC error is an application-level
// rejection (invalid arguments, not-found, permission/auth, ...) rather
// than an upstream health problem. The upstream received the request and
// responded — mirroring the HTTP breaker's `<500` handling, that counts
// as a successful breaker outcome, not a failure or a no-op. Otherwise a
// client hammering a cacheable method with requests the node legitimately
// rejects (InvalidArgument/NotFound) would leave a healthy upstream's
// consecutive-failure count un-reset, or a half-open probe answered with
// NotFound would never close the breaker.
func grpcClientCaused(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented:
		return true
	default:
		// Unavailable, Internal, ResourceExhausted, DataLoss, Aborted,
		// Unknown — genuine upstream/transport health signals.
		return false
	}
}

// RecordOutcomeErr records an RPC result classified by gRPC status code,
// so only genuine upstream/transport failures move the breaker toward
// open. A true local cancellation (grpcNeutral) is treated as neutral —
// EXCEPT that a neutral outcome on the half-open probe RPC re-arms the
// cooldown (rather than leaving the token consumed with no resolution,
// which would wedge the upstream open forever). An application-level
// rejection (grpcClientCaused) is recorded as a breaker success, since the
// upstream demonstrably responded.
func (u *GrpcUpstream) RecordOutcomeErr(err error) {
	if err == nil {
		u.RecordOutcome(true)
		return
	}
	if u.cbConfig == nil || !u.cbConfig.IsEnabled() {
		return
	}
	if grpcNeutral(err) {
		if u.cbOpen.Load() && u.cbOpenedAtUnixMs.Load() == 0 {
			u.cbOpenedAtUnixMs.Store(time.Now().UnixMilli())
		}
		return
	}
	if grpcClientCaused(err) {
		u.RecordOutcome(true)
		return
	}
	u.RecordOutcome(false)
}

// RecordOutcome is called after every RPC. ok=true on success, false on
// any error. Drives the open ↔ closed / half-open ↔ closed transitions
// so a successful probe RPC closes the breaker (instead of needing
// ConsecutiveFailures consecutive successes) and a failed probe
// re-trips it with a fresh cooldown timer.
func (u *GrpcUpstream) RecordOutcome(ok bool) {
	// Guard against nil cbConfig BEFORE calling IsEnabled. Most
	// upstreams have CircuitBreaker disabled at config time and end up
	// with a nil cbConfig (buildGrpcUpstream only sets it when
	// IsEnabled returned true). Relying on a nil-receiver IsEnabled
	// implementation is brittle — guard here explicitly so a future
	// CircuitBreakerConfig method-set change can't introduce a nil
	// deref on a hot path.
	if u.cbConfig == nil || !u.cbConfig.IsEnabled() {
		return
	}
	// Half-open detection: cbOpen=true AND openedAt=0 means a probe
	// is in flight. Success closes the breaker; failure re-trips it
	// with a fresh cooldown.
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

// Healthy reports whether the upstream is usable for new RPCs. When an
// active healthcheck is configured, the probe verdict has the final say:
// a probe that fails (DNS / TCP / 5xx) flips healthy=false even while
// grpc.ClientConn is still in Idle waiting for the first RPC — this is
// the cold-start bleed the active probe is here to close. When no
// healthcheck is configured, the ClientConn state remains the sole
// signal. Idle, Connecting, and Ready are accepted so a backup upstream
// recovering from a quick blip stays in rotation instead of seeing
// every RPC stall while it warms up.
func (u *GrpcUpstream) Healthy() bool {
	if u.conn == nil {
		return false
	}
	if u.hcConfig != nil && !u.healthy.Load() {
		return false
	}
	s := u.conn.GetState()
	return s == connectivity.Ready || s == connectivity.Idle || s == connectivity.Connecting
}

// GrpcUpstreamPool holds a list of GrpcUpstreams + picker state. Single-
// upstream pools are a fast path — same shape as HttpUpstreamPool.
type GrpcUpstreamPool struct {
	name string // pool label for /metrics gauges; always "grpc" today

	// upstreams is the current pool member set. Stored as an atomic
	// pointer so the hot Pick path reads lock-free (a single atomic
	// load returns an immutable slice); writers (Add/RemoveUpstream
	// from discovery, plus the initial seed in the constructor)
	// serialize on writeMu and publish a new slice via Store. Slices
	// are never mutated in place — that's the contract that keeps
	// readers safe without copying.
	upstreams atomic.Pointer[[]*GrpcUpstream]
	writeMu   sync.Mutex

	idx      atomic.Uint32
	strategy string
	log      *Entry

	closeOnce sync.Once

	// Active-healthcheck lifecycle, mirroring HttpUpstreamPool. hcMu
	// protects hcCancel/hcCtx against concurrent StartHealthchecks /
	// Close / AddUpstream calls.
	hcMu     sync.Mutex
	hcCtx    context.Context
	hcCancel context.CancelFunc
	hcWG     sync.WaitGroup
	// hcSpawning is the same Add-before-Wait barrier the HTTP pool
	// uses — see HttpUpstreamPool.hcSpawning for the full rationale.
	hcSpawning sync.WaitGroup
	// hcShutdown latches the pool as permanently shut down. Set under
	// hcMu in Close. StartHealthchecks and AddUpstream check it under
	// hcMu and refuse to spawn new probe goroutines once set. Closes
	// the production-real "Shutdown wins" race — see
	// HttpUpstreamPool.hcShutdown for the full rationale. Particularly
	// important on this pool because Close is wrapped in sync.Once, so
	// a second Close cannot recover from a leaked late spawn.
	hcShutdown bool
}

// upstreamsSnapshot returns the current upstreams slice. Immutable for
// the duration of the caller's use — writers always publish a new slice
// via Store, never mutate in place. Returns nil before the constructor
// has populated the pool; callers in steady state never see nil.
func (p *GrpcUpstreamPool) upstreamsSnapshot() []*GrpcUpstream {
	if ptr := p.upstreams.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// storeUpstreams publishes a new upstreams slice. Caller must hold
// writeMu so two concurrent writers don't lose each other's updates.
func (p *GrpcUpstreamPool) storeUpstreams(s []*GrpcUpstream) {
	p.upstreams.Store(&s)
}

// NewGrpcUpstreamPool dials one grpc.ClientConn per node and returns the
// pool ready to pick. Caller must Close() when done. name is the pool
// label exposed on the cosmoguard_upstream_healthy gauge.
func NewGrpcUpstreamPool(name string, nodes []NodeConfig, logger *Entry, opts ...GrpcUpstreamPoolOption) (*GrpcUpstreamPool, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("grpc upstream pool: no nodes configured")
	}
	pool := &GrpcUpstreamPool{name: name, log: logger, strategy: "weighted-round-robin"}
	for _, opt := range opts {
		opt(pool)
	}
	registerSharedMetrics()
	initial := make([]*GrpcUpstream, 0, len(nodes))
	for _, n := range nodes {
		u, err := buildGrpcUpstream(n)
		if err != nil {
			// Roll back any conns already dialled.
			for _, prev := range initial {
				_ = prev.conn.Close()
			}
			return nil, fmt.Errorf("grpc upstream %s: %w", n.Name, err)
		}
		// Optimistic ONLY when no healthcheck is configured. With a
		// healthcheck, start unhealthy so /readyz gates the pod out of the
		// load balancer until the first probe confirms reachability — matches
		// HttpUpstream and stops a fresh/scaled-up pod from erroring traffic
		// before its upstream is proven usable.
		initialHealthy := u.hcConfig == nil
		u.healthy.Store(initialHealthy)
		// Seed the gauge with that verdict so operators see every configured
		// upstream in /metrics from boot.
		setUpstreamHealthy(pool.name, u.Name, initialHealthy)
		initial = append(initial, u)
	}
	pool.storeUpstreams(initial)
	return pool, nil
}

// buildGrpcUpstream dials the ClientConn and assembles a GrpcUpstream
// from a NodeConfig. Shared between the constructor and AddUpstream so
// dynamically-discovered upstreams get the same TLS, healthcheck, and
// circuit-breaker treatment as constructor-seeded ones.
func buildGrpcUpstream(n NodeConfig) (*GrpcUpstream, error) {
	w := n.Weight
	if w < 1 {
		w = 1
	}
	target, creds, err := upstreamGRPCTarget(n)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	u := &GrpcUpstream{
		Name:   n.Name,
		Target: target,
		Weight: w,
		conn:   conn,
		// Pre-create stop so RemoveUpstream drains correctly whether
		// the upstream was constructor-seeded or added later. See the
		// field comment on stop.
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if n.Healthcheck.IsEnabled() {
		u.hcConfig = n.Healthcheck
		// gRPC has no HTTP healthcheck endpoint of its own, so the
		// probe always rides the HTTP service URL the operator
		// already configured (defaults to rpc:/status). One source
		// of truth for "is this node alive" across LCD / RPC / gRPC.
		hs := n.Healthcheck.Service
		if hs == "" {
			hs = "rpc"
		}
		probeTarget, err := upstreamHTTPURL(n, hs)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("healthcheck.service=%q: %w", hs, err)
		}
		// Normalize via the shared helper (NOT string concatenation) so a
		// path without a leading slash ("status") doesn't yield
		// "http://host:portstatus" and fail every probe.
		u.probeAddr = healthcheckProbeURL(probeTarget.Scheme, probeTarget.Host, n.Healthcheck.Path)
	}
	if n.CircuitBreaker.IsEnabled() {
		u.cbConfig = n.CircuitBreaker
	}
	return u, nil
}

// AddUpstream dials and publishes a new upstream into the pool. Idempotent
// on Name — re-adding an existing upstream is a no-op. When called after
// StartHealthchecks, the new upstream's healthcheck goroutine is started
// immediately so it gets the same active-probe coverage as constructor-
// seeded ones.
func (p *GrpcUpstreamPool) AddUpstream(n NodeConfig) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	current := p.upstreamsSnapshot()
	for _, u := range current {
		if u.Name == n.Name {
			return nil // already in pool
		}
	}
	u, err := buildGrpcUpstream(n)
	if err != nil {
		return err
	}
	addHealthy := u.hcConfig == nil
	u.healthy.Store(addHealthy)
	setUpstreamHealthy(p.name, u.Name, addHealthy)

	next := make([]*GrpcUpstream, 0, len(current)+1)
	next = append(next, current...)
	next = append(next, u)
	p.storeUpstreams(next)

	// If healthchecks are already running, start one for the new upstream
	// under the same pool-level context. The per-upstream stop channel
	// is pre-created by buildGrpcUpstream so RemoveUpstream can drain
	// just this goroutine.
	//
	// hcWG.Add(1) lives inside the hcMu critical section — same Add-
	// before-Wait pattern as HttpUpstreamPool.AddUpstream; see the
	// hcSpawning comment on the pool struct for the race this guards.
	p.hcMu.Lock()
	// Bail if the pool has already been shut down — spawning a probe
	// goroutine now would leak it (Close's cancel is gone, and Close is
	// guarded by sync.Once so it cannot be re-invoked to clean up).
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

// RemoveUpstream drops the named upstream from the pool and tears down
// its ClientConn. The picker swap happens immediately so no new RPCs
// can be routed to this upstream; the actual ClientConn.Close is then
// deferred to a background goroutine that waits for inFlight to drain
// (bounded by a short deadline). This avoids cancelling RPCs that the
// picker already returned this upstream for but that haven't yet
// reached the gRPC client — a real race during DNS-driven removals
// where a pod IP just rotated out. The upstream's healthcheck
// goroutine (if any) is signalled to exit by closing its stop channel.
// Idempotent.
func (p *GrpcUpstreamPool) RemoveUpstream(name string) {
	// Phase 1: snapshot+swap under writeMu, then unlock. We do NOT hold
	// the lock across the done-wait below — a 5s wait under writeMu
	// would serialize the discovery reconciler behind a single slow
	// probe drain and cause head-of-line blocking for every other
	// AddUpstream/RemoveUpstream on this pool.
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
	next := make([]*GrpcUpstream, 0, len(current)-1)
	next = append(next, current[:idx]...)
	next = append(next, current[idx+1:]...)
	p.storeUpstreams(next)
	p.writeMu.Unlock()

	// Phase 2: signal stop + drain probe outside the lock. Concurrent
	// adds/removes for OTHER upstreams can proceed during this wait.
	//
	// buildGrpcUpstream always creates stop, so a constructor-seeded
	// upstream drains the same way an AddUpstream one does. Without
	// this close, the leaked probe goroutine would keep calling
	// setUpstreamHealthy(..., false) and re-publish the gauge series
	// we delete a few lines down.
	close(removed.stop)
	// Wait for an in-flight probe to drain so its setUpstreamHealthy
	// call lands before our deleteUpstreamHealthy below; same race
	// fix as the HTTP pool. We gate on probeStarted (set just before
	// `go p.runHealthcheck`) rather than hcConfig != nil — without
	// this, a pre-StartHealthchecks Remove would burn the full 5s on
	// a done channel nobody is going to close.
	if removed.probeStarted.Load() {
		select {
		case <-removed.done:
		case <-time.After(5 * time.Second):
		}
	}
	// Defer the Close so in-flight RPCs handed out by Pick() before the
	// snapshot swap have a window to actually issue against the conn.
	// The deadline keeps us from leaking goroutines if a caller pins
	// inFlight indefinitely (e.g. a stuck stream); 5s is comfortably
	// longer than any reasonable unary RPC and short enough that
	// repeated pod churn doesn't pile up zombie connections.
	if removed.conn != nil {
		go func(u *GrpcUpstream) {
			// Pick() now reserves the in-flight lease atomically with
			// selection (Pick returns an upstream only after Add(1)), so
			// there is no Pick→dispatch window where inFlight is 0 while an
			// RPC is inbound. The snapshot swap above stops NEW picks, and
			// this drain waits out the leases already handed out — no grace
			// heuristic needed. The 5s deadline bounds a stuck stream so
			// repeated pod churn can't pile up zombie connections.
			deadline := time.Now().Add(5 * time.Second)
			for u.inFlight.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
			}
			_ = u.conn.Close()
		}(removed)
	}
	setUpstreamHealthy(p.name, name, false)
	deleteUpstreamHealthy(p.name, name)
}

// Upstreams returns a snapshot of the current upstreams. Immutable for
// the caller's use. Exposed for /info, the web UI, and tests.
func (p *GrpcUpstreamPool) Upstreams() []*GrpcUpstream {
	return p.upstreamsSnapshot()
}

// StartHealthchecks spawns one goroutine per upstream that has a
// healthcheck configured. Stops on Close.
//
// Idempotent within a single pool lifetime, and a post-Close call is
// also a no-op — the pool is single-shot (see HttpUpstreamPool.Shutdown
// for the rationale; particularly relevant here because Close is
// wrapped in sync.Once and cannot be re-invoked to mop up a leaked
// spawn). Restart requires a fresh pool.
func (p *GrpcUpstreamPool) StartHealthchecks() {
	p.hcMu.Lock()
	// Pool is single-shot: once Close ran, refuse to start so a
	// SIGTERM-during-fast-startup race can't leak probe goroutines
	// past pool teardown. See HttpUpstreamPool.StartHealthchecks for
	// the full interleave this closes.
	if p.hcShutdown {
		p.hcMu.Unlock()
		return
	}
	if p.hcCancel != nil {
		p.hcMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.hcCtx = ctx
	p.hcCancel = cancel
	// Reserve a slot on hcSpawning before releasing hcMu so a concurrent
	// stopHealthchecks (called from Close) sees the counter set the
	// instant it could call hcWG.Wait. See HttpUpstreamPool.hcSpawning.
	p.hcSpawning.Add(1)
	p.hcMu.Unlock()
	// LIFO defer ordering: this hcSpawning.Done is declared FIRST so it
	// fires LAST — after `defer p.writeMu.Unlock()` declared below.
	// stopHealthchecks's hcSpawning.Wait therefore blocks until every
	// per-upstream hcWG.Add(1) has landed. See HttpUpstreamPool's
	// StartHealthchecks for the same comment.
	defer p.hcSpawning.Done()

	// Hold writeMu across the spawn loop so a concurrent RemoveUpstream
	// can't observe probeStarted=false on an upstream whose goroutine
	// we're about to start — see HttpUpstreamPool.StartHealthchecks for
	// the full interleave this guards against.
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

// stopHealthchecks cancels the probe goroutines, marks the pool as
// permanently shut down, and waits for the goroutines to drain. Called
// from Close so the pool's lifecycle is single-method.
//
// Setting hcShutdown under hcMu before observing hcCancel closes the
// "Shutdown wins" race: a StartHealthchecks that hasn't yet taken hcMu
// when this runs will, after hcMu unlock here, observe hcShutdown=true
// and bail without spawning probes. Without the flag, that late
// StartHealthchecks would set a fresh hcCancel after our cancel-and-
// wait completed, spawning probes nobody would ever cancel.
//
// On gRPC this matters more than on the HTTP pool because Close is
// wrapped in sync.Once — a second Close call cannot recover from
// leaked late-spawned probes.
func (p *GrpcUpstreamPool) stopHealthchecks() {
	p.hcMu.Lock()
	p.hcShutdown = true
	cancel := p.hcCancel
	p.hcCancel = nil
	p.hcCtx = nil
	p.hcMu.Unlock()
	if cancel == nil {
		// Defensive only: stopHealthchecks is invoked exclusively via
		// Close, which is wrapped in sync.Once, so concurrent /
		// repeated invocations don't occur in production. The Wait
		// calls here are kept symmetric with the HTTP pool's Shutdown
		// and act as a no-op when counters are 0 — the cost is
		// negligible and they protect against a future caller wiring
		// stopHealthchecks into a path that bypasses Close.
		p.hcSpawning.Wait()
		p.hcWG.Wait()
		return
	}
	cancel()
	// Wait for any in-flight spawn loops (StartHealthchecks) to land
	// their hcWG.Add(1) calls before we Wait on hcWG. See
	// HttpUpstreamPool.Shutdown for the full rationale.
	p.hcSpawning.Wait()
	p.hcWG.Wait()
}

func (p *GrpcUpstreamPool) runHealthcheck(ctx context.Context, u *GrpcUpstream) {
	defer p.hcWG.Done()
	// Signal RemoveUpstream that we've finished; see the HTTP pool's
	// equivalent for the race this guards against.
	defer close(u.done)
	// Suppress the initial probe if the pool was shut down or this
	// upstream was already removed before we got scheduled. See
	// HttpUpstreamPool.runHealthcheck for the same defensive check.
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

	// One immediate probe so the initial verdict isn't a guess — the
	// reason this whole probe loop exists is to flip the optimistic
	// default off before any RPCs leak to a broken peer.
	p.probe(ctx, u, client)

	for {
		select {
		case <-ctx.Done():
			return
		case <-u.stop:
			// Per-upstream stop signal — RemoveUpstream closes this so
			// we exit without draining the entire pool. nil chan is
			// permanently non-ready, so the constructor-seeded path
			// (no per-upstream stop) falls through correctly.
			return
		case <-ticker.C:
			p.probe(ctx, u, client)
		}
	}
}

func (p *GrpcUpstreamPool) probe(ctx context.Context, u *GrpcUpstream, client *http.Client) {
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
	// Suppress the verdict if the pool was shut down or this upstream
	// was removed mid-probe — see HttpUpstreamPool.probe / probeAborted
	// for the gauge-leak hazard this guards.
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

// recordProbeResult applies consecutive-success / consecutive-failure
// thresholds so transient blips don't flap the upstream in/out of the
// picker — same rules as HttpUpstreamPool.recordProbeResult.
func (p *GrpcUpstreamPool) recordProbeResult(u *GrpcUpstream, ok bool, err error) {
	if ok {
		u.failCount.Store(0)
		s := u.successCount.Add(1)
		if !u.healthy.Load() && int(s) >= u.hcConfig.HealthyAfter {
			u.healthy.Store(true)
			setUpstreamHealthy(p.name, u.Name, true)
			p.log.WithFields(Fields{
				"upstream": u.Name,
				"target":   u.Target,
			}).Info("grpc upstream healthy")
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
			"target":   u.Target,
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		p.log.WithFields(fields).Warn("grpc upstream marked unhealthy")
	}
}

// Close stops the healthcheck goroutines and shuts down every dialed
// ClientConn. Safe to call multiple times.
func (p *GrpcUpstreamPool) Close() {
	p.closeOnce.Do(func() {
		p.stopHealthchecks()
		for _, u := range p.upstreamsSnapshot() {
			_ = u.conn.Close()
		}
	})
}

// GrpcUpstreamPoolOption — pool config knob.
type GrpcUpstreamPoolOption func(*GrpcUpstreamPool)

// WithGrpcUpstreamStrategy selects the picker for gRPC.
func WithGrpcUpstreamStrategy(s string) GrpcUpstreamPoolOption {
	return func(p *GrpcUpstreamPool) {
		switch s {
		case "weighted-round-robin", "round-robin", "least-conn", "primary-failover":
			p.strategy = s
		}
	}
}

// Pick returns the next upstream to serve the RPC. Same strategy menu as
// HttpUpstreamPool. Falls back to "any upstream" when none qualify.
// Returns nil when the pool has been drained (e.g. DNS discovery
// returned zero IPs and every constructor-seeded upstream was
// removed); callers must check for nil and surface a gRPC
// Unavailable status instead of dereferencing.
func (p *GrpcUpstreamPool) Pick() *GrpcUpstream {
	// Take ONE snapshot and thread it through healthySet + the picker.
	// Previously healthySet took an independent second snapshot, so a
	// concurrent RemoveUpstream that drained the last upstream between
	// Pick's len check and healthySet's own load produced an empty set and
	// the pickers panicked with an integer divide-by-zero / index-out-of-
	// range. With a single snapshot the set is stable for this call.
	current := p.upstreamsSnapshot()
	if len(current) == 0 {
		return nil
	}
	// Single-upstream pools still go through healthySet so the
	// breaker / probe verdict is honoured. healthySet falls back to
	// `current` when everything is unhealthy, which preserves the
	// "return the only thing in the pool as a last resort" property
	// the previous short-circuit had — without skipping the breaker
	// check on a sole upstream whose breaker is open.
	healthy := p.healthySet(current)
	picked := p.pickFromSet(healthy)
	if picked != nil && !picked.consumeHalfOpenProbe() {
		// We selected a half-open-eligible upstream but lost the probe
		// token to a concurrent caller (or its cooldown wasn't actually
		// elapsed). Dispatching anyway would send a second probe, defeating
		// the single-probe guarantee — instead prefer a fully-closed
		// upstream. Keep `picked` only if none exists (better to
		// double-probe a recovering node than fail the request outright).
		if alt := p.firstClosed(healthy, picked); alt != nil {
			picked = alt
		}
	}
	if picked != nil {
		// Reserve an in-flight LEASE atomically with selection. The caller
		// MUST release it with inFlight.Add(-1) exactly once. This closes
		// the RemoveUpstream close-race: there is no longer a gap between
		// Pick returning an upstream and its in-flight count rising, so a
		// concurrent RemoveUpstream's drain-wait always observes the lease
		// and won't Close() the conn out from under the RPC.
		picked.inFlight.Add(1)
	}
	return picked
}

// pickFromSet dispatches to the configured strategy picker.
func (p *GrpcUpstreamPool) pickFromSet(set []*GrpcUpstream) *GrpcUpstream {
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

// firstClosed returns the first upstream in set (other than skip) whose
// breaker is fully closed (not open at all), or nil.
func (p *GrpcUpstreamPool) firstClosed(set []*GrpcUpstream, skip *GrpcUpstream) *GrpcUpstream {
	for _, u := range set {
		if u != skip && !u.cbOpen.Load() {
			return u
		}
	}
	return nil
}

func (p *GrpcUpstreamPool) healthySet(current []*GrpcUpstream) []*GrpcUpstream {
	out := current[:0:0]
	for _, u := range current {
		if u.Healthy() && !u.CircuitOpen() {
			out = append(out, u)
		}
	}
	if len(out) == 0 {
		return current
	}
	return out
}

func (p *GrpcUpstreamPool) pickRR(set []*GrpcUpstream) *GrpcUpstream {
	// Defensive: never modulo by zero. Callers pass the healthy set, which
	// is normally non-empty, but a concurrent inFlight change can empty the
	// tie set in pickLeastConn between its two passes.
	if len(set) == 0 {
		return nil
	}
	// Do the -1 in uint32 space so the wrap-around at MaxUint32 stays
	// non-negative. The previous shape `int(i)-1` produced a negative
	// int after the uint32 wrap (int(0) - 1 == -1), and `-1 % positive`
	// in Go preserves the dividend's sign — so the next Pick after
	// 4.3 billion calls panicked on a negative slice index. Subtracting
	// in uint32 wraps cleanly (0 - 1 == MaxUint32), and the int cast
	// of a uint32 is always non-negative on 64-bit Go.
	i := int(p.idx.Add(1) - 1)
	return set[i%len(set)]
}

func (p *GrpcUpstreamPool) pickWeightedRR(set []*GrpcUpstream) *GrpcUpstream {
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
	// uint32-space subtraction; see pickRR for the wrap-around rationale.
	pos := int(p.idx.Add(1)-1) % totalWeight
	for _, u := range set {
		if pos < u.Weight {
			return u
		}
		pos -= u.Weight
	}
	return set[len(set)-1]
}

func (p *GrpcUpstreamPool) pickLeastConn(set []*GrpcUpstream) *GrpcUpstream {
	if len(set) == 0 {
		return nil
	}
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
	// inFlight can change between the two passes above, leaving tied empty;
	// fall back to the full (non-empty) set rather than panicking in pickRR.
	if len(tied) == 0 {
		return p.pickRR(set)
	}
	if len(tied) == 1 {
		return tied[0]
	}
	return p.pickRR(tied)
}

func (p *GrpcUpstreamPool) pickPrimary(set []*GrpcUpstream) *GrpcUpstream {
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

// AnyHealthy reports whether at least one upstream is in the
// healthy set. Used by /readyz to gate readiness on actual upstream
// availability. Routes through Healthy() (Idle / Connecting / Ready
// all count) instead of the stricter connectivity.Ready-only check
// the previous shape used — matches what the picker actually
// considers usable, and avoids a chicken-and-egg where /readyz
// returns 503 while a freshly-dialed conn is still in Idle state.
func (p *GrpcUpstreamPool) AnyHealthy() bool {
	for _, u := range p.upstreamsSnapshot() {
		if u.Healthy() {
			return true
		}
	}
	return false
}
