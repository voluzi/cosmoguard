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

	subscriptionConn  map[string]UpstreamConnManager
	subscriptionID    map[string]string
	subscriptionParam map[string]string
	subMux            sync.Mutex

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

	onSubscriptionMessage func(*JsonRpcMsg)
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
	if id, ok := p.subscriptionID[param]; ok {
		p.subMux.Unlock()
		return id, nil
	}
	conn, err := p.getConnection()
	if err != nil {
		p.subMux.Unlock()
		return "", err
	}
	p.subMux.Unlock()

	id, err := conn.Subscribe(param)
	if err != nil {
		return "", err
	}

	// Commit under lock. A concurrent Subscribe for the same param
	// could have raced in here; if so, prefer the existing entry and
	// unsubscribe ours so we don't double-subscribe upstream.
	p.subMux.Lock()
	if existing, ok := p.subscriptionID[param]; ok {
		p.subMux.Unlock()
		// No counter undo: we only increment inside the
		// commit branch below, never on the race-loser path,
		// so the inc/dec stays balanced (zero on each side).
		_ = conn.Unsubscribe(id)
		return existing, nil
	}
	p.subscriptionParam[id] = param
	p.subscriptionID[param] = id
	p.subscriptionConn[id] = conn
	// Bump the per-conn subscription counter in lockstep with the
	// subscriptionConn write so the picker's lock-free read can't
	// observe a committed subscription whose counter hasn't been
	// updated yet (or vice versa). The atomic itself is lock-free,
	// but we're inside subMux here for ordering.
	p.addSubCount(conn, 1)
	p.subMux.Unlock()
	return id, nil
}

func (p *UpstreamPool) Unsubscribe(subID string) error {
	// Snapshot under lock, do the network call outside it, then
	// commit deletion atomically.
	p.subMux.Lock()
	conn, ok := p.subscriptionConn[subID]
	if !ok {
		p.subMux.Unlock()
		return fmt.Errorf("connection for subscription not found")
	}
	p.subMux.Unlock()

	// If the upstream Unsubscribe RPC fails we bail without
	// committing the cleanup — pre-existing behavior preserved so
	// the broker's retry semantics don't change. Note: the per-conn
	// counter has identical drift here (we never reached
	// addSubCount), matching the maps. A fail-then-retry that
	// eventually succeeds is balanced; a fail-then-give-up leaks
	// both the map entry AND the counter slot until pool restart —
	// same blast radius as before the counter existed.
	if err := conn.Unsubscribe(subID); err != nil {
		return err
	}

	p.subMux.Lock()
	delete(p.subscriptionConn, subID)
	delete(p.subscriptionID, p.subscriptionParam[subID])
	delete(p.subscriptionParam, subID)
	// Decrement the per-conn counter in lockstep with the
	// subscriptionConn delete; same ordering rationale as Subscribe.
	p.addSubCount(conn, -1)
	p.subMux.Unlock()
	return nil
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
		oldID string
		conn  UpstreamConnManager
		alt   UpstreamConnManager
		param string
	}
	p.subMux.Lock()
	if len(p.subscriptionConn) == 0 {
		p.subMux.Unlock()
		return nil
	}
	candidates := make([]pending, 0)
	for oldID, conn := range p.subscriptionConn {
		if conn.IsHealthy() {
			continue
		}
		param, ok := p.subscriptionParam[oldID]
		if !ok {
			continue
		}
		alt := p.firstHealthyOther(conn)
		if alt == nil {
			continue
		}
		candidates = append(candidates, pending{oldID: oldID, conn: conn, alt: alt, param: param})
	}
	p.subMux.Unlock()

	var migrated []SubscriptionMigration
	for _, c := range candidates {
		newID, err := c.alt.Subscribe(c.param)
		if err != nil {
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
		if _, stillPinned := p.subscriptionConn[c.oldID]; !stillPinned {
			p.subMux.Unlock()
			_ = c.alt.Unsubscribe(newID)
			continue
		}
		delete(p.subscriptionConn, c.oldID)
		delete(p.subscriptionParam, c.oldID)
		p.subscriptionConn[newID] = c.alt
		p.subscriptionParam[newID] = c.param
		p.subscriptionID[c.param] = newID
		// Move the subscription's tally from the unhealthy conn to
		// the alt so the picker keeps an accurate load view across
		// the migration. Both atomics are bumped under subMux so a
		// concurrent picker can't see the subscription counted twice
		// or not at all.
		p.addSubCount(c.conn, -1)
		p.addSubCount(c.alt, 1)
		p.subMux.Unlock()

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

// firstHealthyOther returns the healthiest, least-loaded alternative
// UpstreamConnManager for migrating away from `skip`, or nil when no
// alternative is available. Same selection policy as getConnection
// (skip-unhealthy, prefer-fewest-subs) so migrations don't pile every
// orphaned subscription onto whichever conn happens to be earliest
// in the slice — a pool-wide outage that recovers one conn at a time
// would otherwise clump the whole backlog on the first survivor,
// defeating the load-aware promise of the picker.
func (p *UpstreamPool) firstHealthyOther(skip UpstreamConnManager) UpstreamConnManager {
	var best UpstreamConnManager
	var bestCount int64
	for _, c := range p.conn {
		if c == skip || !c.IsHealthy() {
			continue
		}
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
	return best
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
