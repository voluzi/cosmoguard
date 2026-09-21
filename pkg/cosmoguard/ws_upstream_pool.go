package cosmoguard

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// ErrNoHealthyUpstream is returned by UpstreamPool.getConnection (and
// propagated by MakeRequest / Subscribe) when every conn in the pool
// reports IsHealthy() == false. Exposed as a sentinel so callers and
// tests can match it with errors.Is rather than substring matching
// the error text.
var ErrNoHealthyUpstream = errors.New("ws upstream pool: no healthy connection available")

type UpstreamPool struct {
	conn    []UpstreamConnManager
	targets []url.URL // per-conn backend, aligned with conn; read-only after construction
	connIdx uint32
	log     *Entry
	IdGen   *util.UniqueID

	subscriptionConn        map[string]UpstreamConnManager
	subscriptionID          map[string]string
	subscriptionParam       map[string]string
	subscriptionLease       map[string]*wsReservationLease
	canonicalReservations   map[string]*wsCanonicalReservation
	pendingCreates          map[string]*wsPoolCreate
	pendingUnsubscribe      map[string]struct{}
	migrating               map[string]struct{}
	drainingParams          map[string]map[*wsReservationLease]struct{}
	subMux                  sync.Mutex
	maxSubscriptionsPerConn int

	// subCount maps each conn to its own atomic counter of pinned
	// subscriptions. The MAP itself is built once at NewUpstreamPool
	// and never mutated afterward — the conn slice doesn't grow or
	// shrink — so the picker reads it lock-free even while
	// Subscribe / Unsubscribe / MigrateUnhealthy increment or
	// decrement the individual atomics under subMux. We can't read
	// subscriptionConn for this directly because subMux is held by
	// Subscribe across the getConnection call (a deadlock), so the
	// counter shim sidesteps the lock hierarchy.
	//
	// Tracks SUBSCRIPTIONS only, not one-shot MakeRequest traffic —
	// MakeRequest doesn't pin anything on a conn, so a conn saturated
	// with one-shot RPCs still looks "idle" to subscription routing.
	// That's by design: clumping subscriptions hurts head-of-line
	// latency, clumping one-shots is harmless because each completes
	// independently.
	subCount map[UpstreamConnManager]*atomic.Int64

	onSubscriptionMessage  func(*JsonRpcMsg)
	afterJoinPendingCreate func()
}

type wsReservationLease struct {
	pool *UpstreamPool
	conn UpstreamConnManager
	once sync.Once
}

type wsMigrationHandleOwner interface {
	reservationForHandle(string) (string, bool)
	localUnsubscribePreservingHandle(string) <-chan error
}

type wsCanonicalReservation struct {
	id          string
	settled     chan struct{}
	settleOnce  sync.Once
	releaseOnce sync.Once
}

func newWSCanonicalReservation(id string) *wsCanonicalReservation {
	return &wsCanonicalReservation{id: id, settled: make(chan struct{})}
}

func (r *wsCanonicalReservation) markSettled() {
	if r != nil {
		r.settleOnce.Do(func() { close(r.settled) })
	}
}

func (r *wsCanonicalReservation) releaseWhenSettled(idGen *util.UniqueID) {
	if r == nil || idGen == nil {
		return
	}
	r.releaseOnce.Do(func() {
		release := func() { idGen.Release(r.id) }
		select {
		case <-r.settled:
			release()
		default:
			go func() {
				<-r.settled
				release()
			}()
		}
	})
}

func (l *wsReservationLease) release() {
	if l == nil {
		return
	}
	l.pool.subMux.Lock()
	l.releaseLocked()
	l.pool.subMux.Unlock()
}

func (l *wsReservationLease) releaseLocked() {
	l.once.Do(func() { l.pool.addSubCount(l.conn, -1) })
}

type wsPoolCreate struct {
	done chan struct{}
	id   string
	err  error
}

// SetMaxSubscriptionsPerConnection configures the per-connection admission
// cap before the pool begins serving traffic. Zero disables the cap.
func (p *UpstreamPool) SetMaxSubscriptionsPerConnection(limit int) {
	p.subMux.Lock()
	p.maxSubscriptionsPerConn = limit
	p.subMux.Unlock()
}

// NewUpstreamPool builds the WS connection pool. `backends` is a list
// of scheme-bearing URL strings ("ws://host:port" or "wss://host:port"
// — no path); `path` is the WS handshake path the pool appends ("/" or
// "/websocket"); `n` is the total connection budget. Connections are
// spread evenly across backends via i % len(backends). With a single
// backend this collapses to the v3 behavior — every connection targets
// the same upstream.
//
// gorilla/websocket's Dialer auto-selects TLS for wss:// URLs using
// system trust roots, so plain ws / wss URLs work without any further
// dialer configuration.
//
// Panics if backends is empty (the JsonRpcHandler guarantees non-empty
// before constructing) or if any entry isn't a parseable URL with a
// ws/wss scheme. The latter is a programmer error from
// nodeWSBackend — config-time validation catches operator typos
// upstream of this constructor.
func NewUpstreamPool(backends []string, path string, n int, onMessage func(*JsonRpcMsg), upstreamConstructor UpstreamConnManagerConstructor) *UpstreamPool {
	if len(backends) == 0 {
		panic("ws upstream pool: no backends configured")
	}
	// n < 1 makes the picker's `% len(p.conn)` divide by zero and
	// the pool useless besides — catch it at construction with the
	// same loudness as the no-backends case so a misconfigured
	// `webSocketConnections: 0` fails startup rather than panicking
	// on the first inbound subscription.
	if n < 1 {
		panic(fmt.Sprintf("ws upstream pool: connection budget n=%d must be >= 1", n))
	}
	parsed := make([]url.URL, len(backends))
	for i, b := range backends {
		u, err := url.Parse(b)
		if err != nil {
			panic(fmt.Sprintf("ws upstream pool: invalid backend %q: %v", b, err))
		}
		switch u.Scheme {
		case "ws", "wss":
		default:
			panic(fmt.Sprintf("ws upstream pool: backend %q must use ws:// or wss:// scheme", b))
		}
		if u.Host == "" {
			panic(fmt.Sprintf("ws upstream pool: backend %q missing host", b))
		}
		parsed[i] = url.URL{Scheme: u.Scheme, Host: u.Host}
	}
	pool := &UpstreamPool{
		conn:                  make([]UpstreamConnManager, n),
		targets:               make([]url.URL, n),
		subscriptionConn:      make(map[string]UpstreamConnManager),
		subscriptionID:        make(map[string]string),
		subscriptionParam:     make(map[string]string),
		subscriptionLease:     make(map[string]*wsReservationLease),
		canonicalReservations: make(map[string]*wsCanonicalReservation),
		pendingCreates:        make(map[string]*wsPoolCreate),
		pendingUnsubscribe:    make(map[string]struct{}),
		migrating:             make(map[string]struct{}),
		drainingParams:        make(map[string]map[*wsReservationLease]struct{}),
		subCount:              make(map[UpstreamConnManager]*atomic.Int64, n),
		onSubscriptionMessage: onMessage,
		IdGen:                 &util.UniqueID{},
	}

	for i := 0; i < n; i++ {
		base := parsed[i%len(parsed)]
		backendURL := url.URL{
			Scheme: base.Scheme,
			Host:   base.Host,
			Path:   path,
		}
		pool.conn[i] = upstreamConstructor(backendURL, pool.IdGen, pool.onSubscriptionMessage)
		pool.targets[i] = backendURL
		// One atomic counter per conn. The map is frozen after this
		// loop; the picker reads it lock-free.
		pool.subCount[pool.conn[i]] = new(atomic.Int64)
	}

	return pool
}

// addSubCount nudges the per-conn subscription counter by delta.
// Nil-safe on both p.subCount (so tests can construct a bare
// UpstreamPool literal without the counter map) and on the per-conn
// entry (so a conn that's never been seen by NewUpstreamPool's
// init loop simply doesn't influence the picker — the counter is
// a heuristic, not a correctness contract).
func (p *UpstreamPool) addSubCount(c UpstreamConnManager, delta int64) {
	if p.subCount == nil {
		return
	}
	if ctr, ok := p.subCount[c]; ok {
		ctr.Add(delta)
	}
}

// Stop signals every conn manager in the pool to terminate its Run
// loop and close its WS client. Safe to call multiple times.
func (p *UpstreamPool) Stop() {
	for _, c := range p.conn {
		if c != nil {
			c.Stop()
		}
	}
}

func (p *UpstreamPool) Start(log *Entry) error {
	p.log = log
	for i, conn := range p.conn {
		go func(id int, c UpstreamConnManager) {
			if err := c.Run(p.log.WithField("upstream-id", id)); err != nil {
				p.log.Errorf("error on upstream connection: %v", err)
			}
		}(i, conn)
	}
	return nil
}

// getConnection picks an upstream conn for the next request or
// subscription. The picker is health-aware and load-aware:
//
//  1. Skip conns whose UpstreamConnManager reports IsHealthy() ==
//     false. Without this, a conn whose WS socket has dropped and
//     is in reconnect backoff would still be handed out and the
//     caller's MakeRequest / Subscribe would fail on a dead socket.
//  2. Among healthy conns, prefer the one with the fewest pinned
//     subscriptions (read lock-free from subCount). Without this,
//     subscriptions clump on whichever conn happens to be next in
//     the round-robin rotation, head-of-line latency on the loaded
//     conn grows, and one conn going unhealthy migrates a
//     disproportionate share of traffic.
//  3. Rotate the starting offset via the atomic connIdx so ties
//     (e.g. all conns currently at 0 subs) spread across the pool
//     instead of stampeding the first slot.
//
// Lock-free hot path: every read is either an atomic load
// (IsHealthy / subCount) or a read from a slice / map that's
// immutable after NewUpstreamPool, so the picker doesn't acquire
// subMux even though Subscribe calls it while holding subMux.
//
// When every conn is unhealthy, returns an explicit error instead
// of silently handing back a dead conn — the caller sees "no
// healthy upstream" with a single grep instead of debugging a
// generic "websocket: connection closed" deep in MakeRequest.
func (p *UpstreamPool) getConnection() (UpstreamConnManager, error) {
	n := len(p.conn)
	// Modulo the uint32 first, THEN convert to int — otherwise on a
	// 32-bit platform after ~4B picks the wrap from 0xFFFFFFFF to 0
	// produces a negative int and Go's `%` preserves the sign,
	// flipping `start` to a negative slice index. The construction
	// guarantee n >= 1 means the uint32 modulo is safe.
	start := int((atomic.AddUint32(&p.connIdx, 1) - 1) % uint32(n))

	var best UpstreamConnManager
	var bestCount int64
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		c := p.conn[idx]
		if !c.IsHealthy() {
			continue
		}
		// Counter is nil-safe: a UpstreamPool literal that
		// bypassed NewUpstreamPool's init loop (test fixtures,
		// future direct constructors) simply contributes "0
		// subscriptions" for the affected conn — the picker keeps
		// working, just without the load-aware tier for that conn.
		var cnt int64
		if p.subCount != nil {
			if ctr, ok := p.subCount[c]; ok {
				cnt = ctr.Load()
			}
		}
		if best == nil || cnt < bestCount {
			best = c
			bestCount = cnt
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, ErrNoHealthyUpstream
}

func (p *UpstreamPool) MakeRequest(msg *JsonRpcMsg) (*JsonRpcMsg, error) {
	conn, err := p.getConnection()
	if err != nil {
		return nil, err
	}
	return conn.MakeRequest(msg)
}

func (p *UpstreamPool) Subscribe(param string) (string, error) {
	// Short critical section: check + pick. We do NOT hold subMux
	// across conn.Subscribe (a network round-trip) — that would
	// stall every other Subscribe/Unsubscribe call AND deadlock
	// against onUpstreamMessage in some edge cases.
	p.subMux.Lock()
	p.ensureSubscriptionMapsLocked()
	if len(p.drainingParams[param]) > 0 {
		limit := p.maxSubscriptionsPerConn
		p.subMux.Unlock()
		return "", pendingUnsubscribeError(limit)
	}
	if id, ok := p.subscriptionID[param]; ok {
		_, pending := p.pendingUnsubscribe[id]
		limit := p.maxSubscriptionsPerConn
		p.subMux.Unlock()
		if pending {
			return "", pendingUnsubscribeError(limit)
		}
		return id, nil
	}
	if pending := p.pendingCreates[param]; pending != nil {
		p.subMux.Unlock()
		if p.afterJoinPendingCreate != nil {
			p.afterJoinPendingCreate()
		}
		<-pending.done
		return pending.id, pending.err
	}
	conn, lease, err := p.reserveSubscriptionConnectionLocked(nil)
	if err != nil {
		p.subMux.Unlock()
		return "", err
	}
	pending := &wsPoolCreate{done: make(chan struct{})}
	p.pendingCreates[param] = pending
	p.subMux.Unlock()

	id, err := conn.Subscribe(param)
	if err != nil {
		if isUncertainWSUpstreamOutcome(err) {
			settled := uncertainWSUpstreamOutcomeSettlement(err)
			p.subMux.Lock()
			p.addDrainingLeaseLocked(param, lease)
			p.subMux.Unlock()
			err = uncertainWSUpstreamOutcomeWithSettlement(err, p.releaseDrainingLease(param, lease, settled))
		} else {
			lease.release()
		}
		p.completeCreate(param, pending, "", err)
		return "", err
	}

	p.subMux.Lock()
	if existing, ok := p.subscriptionID[param]; ok {
		_, removalPending := p.pendingUnsubscribe[existing]
		limit := p.maxSubscriptionsPerConn
		p.subMux.Unlock()
		p.retireUncommitted(param, conn, lease)
		p.completeCreate(param, pending, existing, nil)
		if removalPending {
			return "", pendingUnsubscribeError(limit)
		}
		return existing, nil
	}
	p.subscriptionParam[id] = param
	p.subscriptionID[param] = id
	p.subscriptionConn[id] = conn
	p.subscriptionLease[id] = lease
	p.subMux.Unlock()
	p.completeCreate(param, pending, id, nil)
	return id, nil
}

func (p *UpstreamPool) ensureSubscriptionMapsLocked() {
	if p.subscriptionConn == nil {
		p.subscriptionConn = make(map[string]UpstreamConnManager)
	}
	if p.subscriptionID == nil {
		p.subscriptionID = make(map[string]string)
	}
	if p.subscriptionParam == nil {
		p.subscriptionParam = make(map[string]string)
	}
	if p.subscriptionLease == nil {
		p.subscriptionLease = make(map[string]*wsReservationLease)
	}
	if p.canonicalReservations == nil {
		p.canonicalReservations = make(map[string]*wsCanonicalReservation)
	}
	if p.pendingCreates == nil {
		p.pendingCreates = make(map[string]*wsPoolCreate)
	}
	if p.pendingUnsubscribe == nil {
		p.pendingUnsubscribe = make(map[string]struct{})
	}
	if p.migrating == nil {
		p.migrating = make(map[string]struct{})
	}
	if p.drainingParams == nil {
		p.drainingParams = make(map[string]map[*wsReservationLease]struct{})
	}
}

func (p *UpstreamPool) completeCreate(param string, pending *wsPoolCreate, id string, err error) {
	p.subMux.Lock()
	pending.id, pending.err = id, err
	if p.pendingCreates[param] == pending {
		delete(p.pendingCreates, param)
	}
	close(pending.done)
	p.subMux.Unlock()
}

func (p *UpstreamPool) addDrainingLeaseLocked(param string, lease *wsReservationLease) {
	drains := p.drainingParams[param]
	if drains == nil {
		drains = make(map[*wsReservationLease]struct{})
		p.drainingParams[param] = drains
	}
	drains[lease] = struct{}{}
}

func (p *UpstreamPool) removeDrainingLeaseLocked(param string, lease *wsReservationLease) {
	drains := p.drainingParams[param]
	delete(drains, lease)
	if len(drains) == 0 {
		delete(p.drainingParams, param)
	}
}

func (p *UpstreamPool) releaseDrainingLease(param string, lease *wsReservationLease, settled <-chan struct{}) <-chan struct{} {
	return p.releaseDrainingLeaseAndReservation(param, lease, nil, settled)
}

func (p *UpstreamPool) releaseDrainingLeaseAndReservation(param string, lease *wsReservationLease, reservation *wsCanonicalReservation, settled <-chan struct{}) <-chan struct{} {
	if settled == nil {
		return nil
	}
	done := make(chan struct{})
	release := func() {
		p.subMux.Lock()
		p.removeDrainingLeaseLocked(param, lease)
		lease.releaseLocked()
		p.subMux.Unlock()
		reservation.releaseWhenSettled(p.IdGen)
		close(done)
	}
	select {
	case <-settled:
		release()
		return done
	default:
	}
	go func() {
		<-settled
		release()
	}()
	return done
}

func (p *UpstreamPool) drainFailedMigration(oldID, param string, source UpstreamConnManager, lease *wsReservationLease, settled <-chan struct{}) {
	p.subMux.Lock()
	tracked := p.subscriptionConn[oldID] == source
	if tracked {
		p.addDrainingLeaseLocked(param, lease)
	}
	p.subMux.Unlock()
	if settled == nil {
		return
	}
	release := func() {
		p.subMux.Lock()
		p.removeDrainingLeaseLocked(param, lease)
		if p.subscriptionConn[oldID] == source {
			delete(p.migrating, oldID)
		}
		lease.releaseLocked()
		p.subMux.Unlock()
	}
	select {
	case <-settled:
		release()
	default:
		go func() {
			<-settled
			release()
		}()
	}
}

func (p *UpstreamPool) retireUncommitted(param string, conn UpstreamConnManager, lease *wsReservationLease) {
	settled := conn.LocalUnsubscribe(param)
	if settled == nil {
		lease.release()
		return
	}
	go func() {
		if err, ok := <-settled; !ok || err == nil {
			lease.release()
		}
	}()
}

func pendingUnsubscribeError(limit int) error {
	if limit > 0 {
		return &WSResourceExhaustedError{Scope: wsLimitScopeUpstreamConnection, Limit: limit}
	}
	return errors.New("subscription removal is pending")
}

// reserveSubscriptionConnectionLocked chooses a healthy connection with
// available subscription capacity and increments its count before the caller
// performs network I/O. The caller must hold subMux.
func (p *UpstreamPool) reserveSubscriptionConnectionLocked(skip UpstreamConnManager) (UpstreamConnManager, *wsReservationLease, error) {
	n := len(p.conn)
	start := int((atomic.AddUint32(&p.connIdx, 1) - 1) % uint32(n))
	var best UpstreamConnManager
	var bestCount int64
	healthy := false
	for i := 0; i < n; i++ {
		c := p.conn[(start+i)%n]
		if c == skip || !c.IsHealthy() {
			continue
		}
		healthy = true
		var count int64
		if counter, ok := p.subCount[c]; ok {
			count = counter.Load()
		}
		if p.maxSubscriptionsPerConn > 0 && count >= int64(p.maxSubscriptionsPerConn) {
			continue
		}
		if best == nil || count < bestCount {
			best = c
			bestCount = count
		}
	}
	if best != nil {
		p.addSubCount(best, 1)
		return best, &wsReservationLease{pool: p, conn: best}, nil
	}
	if healthy {
		return nil, nil, &WSResourceExhaustedError{
			Scope: wsLimitScopeUpstreamConnection,
			Limit: p.maxSubscriptionsPerConn,
		}
	}
	return nil, nil, ErrNoHealthyUpstream
}

func (p *UpstreamPool) Unsubscribe(subID string) error {
	// Snapshot under lock, do the network call outside it, then
	// commit deletion atomically.
	p.subMux.Lock()
	p.ensureSubscriptionMapsLocked()
	conn, ok := p.subscriptionConn[subID]
	if !ok {
		p.subMux.Unlock()
		return fmt.Errorf("connection for subscription not found")
	}
	if _, pending := p.pendingUnsubscribe[subID]; pending {
		limit := p.maxSubscriptionsPerConn
		p.subMux.Unlock()
		return pendingUnsubscribeError(limit)
	}
	if p.pendingUnsubscribe == nil {
		p.pendingUnsubscribe = make(map[string]struct{})
	}
	p.pendingUnsubscribe[subID] = struct{}{}
	lease := p.leaseForRouteLocked(subID, conn)
	canonicalReservation := p.canonicalReservations[subID]
	param := p.subscriptionParam[subID]
	p.subMux.Unlock()

	// A definite failure preserves the existing subscription maps and
	// clears the pending-removal marker so callers may retry. An uncertain
	// outcome keeps capacity charged until the exact socket settles.
	if err := conn.Unsubscribe(subID); err != nil {
		if isUncertainWSUpstreamOutcome(err) {
			settled := uncertainWSUpstreamOutcomeSettlement(err)
			p.subMux.Lock()
			if p.subscriptionConn[subID] == conn {
				p.removeRouteLocked(subID, param)
				p.addDrainingLeaseLocked(param, lease)
			}
			p.subMux.Unlock()
			err = uncertainWSUpstreamOutcomeWithSettlement(err, p.releaseDrainingLeaseAndReservation(param, lease, canonicalReservation, settled))
		} else {
			p.subMux.Lock()
			delete(p.pendingUnsubscribe, subID)
			p.subMux.Unlock()
		}
		return err
	}

	p.subMux.Lock()
	p.removeRouteLocked(subID, param)
	lease.releaseLocked()
	p.subMux.Unlock()
	canonicalReservation.releaseWhenSettled(p.IdGen)
	return nil
}

func (p *UpstreamPool) leaseForRouteLocked(id string, conn UpstreamConnManager) *wsReservationLease {
	if lease := p.subscriptionLease[id]; lease != nil {
		return lease
	}
	lease := &wsReservationLease{pool: p, conn: conn}
	p.subscriptionLease[id] = lease
	return lease
}

func (p *UpstreamPool) removeRouteLocked(id, param string) {
	delete(p.subscriptionConn, id)
	delete(p.subscriptionParam, id)
	delete(p.subscriptionLease, id)
	delete(p.canonicalReservations, id)
	delete(p.pendingUnsubscribe, id)
	delete(p.migrating, id)
	if p.subscriptionID[param] == id {
		delete(p.subscriptionID, param)
	}
}

// SubscriptionMigration is one (oldID, newID, param) tuple emitted by
// MigrateUnhealthy when an active subscription gets moved from an
// unhealthy upstream to a healthy one.
type SubscriptionMigration struct {
	OldID string
	NewID string
	Param string
}

// MigrateUnhealthy walks the pool's pinned subscriptions; for each
// whose UpstreamConnManager is currently unhealthy, attempts to
// re-subscribe on the first healthy alternative connection. Returns a
// list of (oldID, newID, param) tuples so the broker can update its
// own subscription manager.
//
// Subscriptions stay on their original conn when no healthy
// alternative is available — better than dropping them; the existing
// per-conn auto-reconnect may still recover.
//
// We never hold subMux across conn.Subscribe (network call). The
// algorithm is: snapshot candidates under lock, do the network calls
// outside, commit successful migrations under lock.
func (p *UpstreamPool) MigrateUnhealthy() []SubscriptionMigration {
	type pending struct {
		oldID       string
		conn        UpstreamConnManager
		alt         UpstreamConnManager
		param       string
		sourceLease *wsReservationLease
		destLease   *wsReservationLease
		canonical   *wsCanonicalReservation
		reservation string
	}
	p.subMux.Lock()
	p.ensureSubscriptionMapsLocked()
	if len(p.subscriptionConn) == 0 {
		p.subMux.Unlock()
		return nil
	}
	candidates := make([]pending, 0)
	for oldID, conn := range p.subscriptionConn {
		if _, removing := p.pendingUnsubscribe[oldID]; removing {
			continue
		}
		if _, moving := p.migrating[oldID]; moving {
			continue
		}
		if conn.IsHealthy() {
			continue
		}
		param, ok := p.subscriptionParam[oldID]
		if !ok {
			continue
		}
		alt, destLease, err := p.reserveSubscriptionConnectionLocked(conn)
		if err != nil {
			continue
		}
		p.migrating[oldID] = struct{}{}
		canonical := p.canonicalReservations[oldID]
		reservation := ""
		if canonical == nil {
			if owner, ok := conn.(wsMigrationHandleOwner); ok {
				reservation, _ = owner.reservationForHandle(oldID)
			}
		}
		candidates = append(candidates, pending{
			oldID: oldID, conn: conn, alt: alt, param: param,
			sourceLease: p.leaseForRouteLocked(oldID, conn), destLease: destLease,
			canonical: canonical, reservation: reservation,
		})
	}
	p.subMux.Unlock()

	var migrated []SubscriptionMigration
	for _, c := range candidates {
		newID, err := c.alt.Subscribe(c.param)
		if err != nil {
			if isUncertainWSUpstreamOutcome(err) {
				p.drainFailedMigration(c.oldID, c.param, c.conn, c.destLease, uncertainWSUpstreamOutcomeSettlement(err))
			} else {
				c.destLease.release()
				p.subMux.Lock()
				if p.subscriptionConn[c.oldID] == c.conn {
					delete(p.migrating, c.oldID)
				}
				p.subMux.Unlock()
			}
			if p.log != nil {
				p.log.WithFields(Fields{
					"oldID": c.oldID,
					"param": c.param,
					"error": err.Error(),
				}).Warn("ws subscription migration: re-subscribe failed")
			}
			continue
		}
		// Commit under lock. A concurrent client unsubscribe could
		// have removed oldID — in that case undo the new subscribe so
		// we don't leak it on the alt.
		//
		// We do NOT need to additionally check that subscriptionID[param]
		// still maps to oldID: pool.Subscribe early-returns the existing
		// id when subscriptionID[param] is already populated (line
		// ~219), so a racer cannot create a SECOND entry for the same
		// param while oldID still lives. Either Unsubscribe ran (and
		// stillPinned catches it) or it didn't (and the param mapping
		// still points at oldID).
		p.subMux.Lock()
		current := p.subscriptionConn[c.oldID]
		_, removing := p.pendingUnsubscribe[c.oldID]
		_, moving := p.migrating[c.oldID]
		if current != c.conn || removing || !moving || p.subscriptionLease[c.oldID] != c.sourceLease {
			p.subMux.Unlock()
			p.retireUncommitted(c.param, c.alt, c.destLease)
			continue
		}
		canonical := c.canonical
		preserveSourceHandle := false
		if canonical == nil && c.reservation != "" {
			canonical = newWSCanonicalReservation(c.reservation)
			preserveSourceHandle = true
		}
		p.removeRouteLocked(c.oldID, c.param)
		p.subscriptionConn[newID] = c.alt
		p.subscriptionParam[newID] = c.param
		p.subscriptionID[c.param] = newID
		p.subscriptionLease[newID] = c.destLease
		if canonical != nil {
			p.canonicalReservations[newID] = canonical
		}
		p.subMux.Unlock()

		// A reconnect can recreate the source subscription while the alternate
		// subscribe is in flight, so its slot stays reserved until cleanup proves
		// that duplicate cannot remain.
		var cleanup <-chan error
		if preserveSourceHandle {
			cleanup = c.conn.(wsMigrationHandleOwner).localUnsubscribePreservingHandle(c.param)
		} else {
			cleanup = c.conn.LocalUnsubscribe(c.param)
		}
		if cleanup == nil {
			c.sourceLease.release()
			if preserveSourceHandle {
				canonical.markSettled()
			}
		} else {
			go func(lease *wsReservationLease, settled <-chan error, canonical *wsCanonicalReservation, settleCanonical bool) {
				if err, ok := <-settled; !ok || err == nil {
					lease.release()
					if settleCanonical {
						canonical.markSettled()
					}
				}
			}(c.sourceLease, cleanup, canonical, preserveSourceHandle)
		}

		if p.log != nil {
			p.log.WithFields(Fields{
				"oldID": c.oldID,
				"newID": newID,
				"param": c.param,
			}).Info("ws subscription migrated to healthy upstream")
		}
		migrated = append(migrated, SubscriptionMigration{
			OldID: c.oldID, NewID: newID, Param: c.param,
		})
	}
	return migrated
}

// ConnStat is one upstream connection's dashboard view: its backend
// target, current health, and the number of subscriptions pinned to it.
type ConnStat struct {
	Target        string `json:"target"`
	Healthy       bool   `json:"healthy"`
	Subscriptions int    `json:"subscriptions"`
}

// ConnStats returns a per-connection snapshot for the dashboard WS
// panel. Read-only: targets is immutable after construction, subCount
// loads are atomic, IsHealthy is the conn's own atomic flag.
func (p *UpstreamPool) ConnStats() []ConnStat {
	out := make([]ConnStat, 0, len(p.conn))
	for i, c := range p.conn {
		var cnt int64
		if ctr, ok := p.subCount[c]; ok {
			cnt = ctr.Load()
		}
		out = append(out, ConnStat{
			Target:        p.targets[i].Host,
			Healthy:       c.IsHealthy(),
			Subscriptions: int(cnt),
		})
	}
	return out
}

// SubscriptionTarget returns the backend host an upstream subscription
// id is currently pinned to, or "" when the id is unknown.
func (p *UpstreamPool) SubscriptionTarget(id string) string {
	p.subMux.Lock()
	defer p.subMux.Unlock()
	c, ok := p.subscriptionConn[id]
	if !ok {
		return ""
	}
	for i, candidate := range p.conn {
		if candidate == c {
			return p.targets[i].Host
		}
	}
	return ""
}
