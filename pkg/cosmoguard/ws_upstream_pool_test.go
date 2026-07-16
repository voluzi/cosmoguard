package cosmoguard

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// fakeInstanceSeq makes every fakeUpstreamConn instance unique across
// the whole pool — two conns pointing at the same backend host must
// still produce distinct subscription ids, otherwise the pool's
// subscriptionConn map collides on commit and the picker tests can't
// keep score. Package-level so the seq doesn't restart per pool.
var fakeInstanceSeq atomic.Uint64

// fakeUpstreamConn records the URL it was constructed with so the
// distribution-across-backends invariant of NewUpstreamPool can be
// verified without dialling real WS sockets. healthy is an atomic
// switch the picker tests flip at runtime to model conns going in
// and out of reconnect backoff; instance + subIDSeq combine into
// globally-unique Subscribe ids so the pool's bookkeeping never
// sees collisions across conns.
type fakeUpstreamConn struct {
	target   url.URL
	instance uint64
	healthy  atomic.Bool
	subIDSeq atomic.Uint64
}

func (f *fakeUpstreamConn) Run(_ *Entry) error                           { return nil }
func (f *fakeUpstreamConn) MakeRequest(*JsonRpcMsg) (*JsonRpcMsg, error) { return nil, nil }
func (f *fakeUpstreamConn) HasSubscription(string) bool                  { return false }
func (f *fakeUpstreamConn) Subscribe(string) (string, error) {
	return fmt.Sprintf("%s/i%d#%d", f.target.Host, f.instance, f.subIDSeq.Add(1)), nil
}
func (f *fakeUpstreamConn) Unsubscribe(string) error { return nil }
func (f *fakeUpstreamConn) LocalUnsubscribe(string)  {}
func (f *fakeUpstreamConn) IsHealthy() bool          { return f.healthy.Load() }
func (f *fakeUpstreamConn) Stop()                    {}

func fakeConstructor(u url.URL, _ *util.UniqueID, _ func(*JsonRpcMsg)) UpstreamConnManager {
	f := &fakeUpstreamConn{target: u, instance: fakeInstanceSeq.Add(1)}
	f.healthy.Store(true)
	return f
}

// TestWSPoolDistributesAcrossBackends: 6 connections across 3 backends
// should produce 2 connections per backend (i % len pattern).
func TestWSPoolDistributesAcrossBackends(t *testing.T) {
	backends := []string{"ws://a:1", "ws://b:2", "ws://c:3"}
	pool := NewUpstreamPool(backends, "/websocket", 6, func(*JsonRpcMsg) {}, fakeConstructor)

	hits := map[string]int{}
	for _, c := range pool.conn {
		f := c.(*fakeUpstreamConn)
		hits[f.target.Host]++
	}
	wantHosts := []string{"a:1", "b:2", "c:3"}
	for _, h := range wantHosts {
		if hits[h] != 2 {
			t.Fatalf("backend %s: got %d connections, want 2 (got hits=%v)", h, hits[h], hits)
		}
	}
}

// TestWSPoolSingleBackendCompat: with one backend, all N connections
// target it (preserves v3 behavior).
func TestWSPoolSingleBackendCompat(t *testing.T) {
	pool := NewUpstreamPool([]string{"ws://only:1"}, "/", 5, func(*JsonRpcMsg) {}, fakeConstructor)
	for i, c := range pool.conn {
		f := c.(*fakeUpstreamConn)
		if f.target.Host != "only:1" {
			t.Fatalf("conn %d: host=%s want only:1", i, f.target.Host)
		}
		if f.target.Scheme != "ws" {
			t.Fatalf("conn %d: scheme=%s want ws", i, f.target.Scheme)
		}
	}
}

// TestWSPoolPropagatesTLSScheme: a wss:// backend must yield wss:// per-
// conn URLs (so gorilla/websocket auto-switches to TLS at Dial time).
func TestWSPoolPropagatesTLSScheme(t *testing.T) {
	pool := NewUpstreamPool([]string{"wss://rpc.example.com"}, "/websocket", 3, func(*JsonRpcMsg) {}, fakeConstructor)
	for i, c := range pool.conn {
		f := c.(*fakeUpstreamConn)
		if f.target.Scheme != "wss" {
			t.Fatalf("conn %d: scheme=%s want wss", i, f.target.Scheme)
		}
		if f.target.Host != "rpc.example.com" {
			t.Fatalf("conn %d: host=%s want rpc.example.com", i, f.target.Host)
		}
		if f.target.Path != "/websocket" {
			t.Fatalf("conn %d: path=%s want /websocket", i, f.target.Path)
		}
	}
}

// TestWSPoolRejectsBadScheme: backends must use ws:// or wss:// — anything
// else is a programmer error (config-time validation catches operator
// typos upstream of NewUpstreamPool).
func TestWSPoolRejectsBadScheme(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on http:// backend")
		}
	}()
	_ = NewUpstreamPool([]string{"http://oops:1"}, "/", 1, func(*JsonRpcMsg) {}, fakeConstructor)
}

// TestWSPoolRejectsZeroBudget pins the n>=1 guard on NewUpstreamPool.
// A misconfigured `webSocketConnections: 0` would otherwise reach
// the picker as `% 0` and panic with a divide-by-zero on the first
// inbound subscription instead of failing loudly at startup.
func TestWSPoolRejectsZeroBudget(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on n=0")
		}
	}()
	_ = NewUpstreamPool([]string{"ws://a:1"}, "/", 0, func(*JsonRpcMsg) {}, fakeConstructor)
}

// TestWSPoolPicker_SkipsUnhealthy pins the IsHealthy() gate on
// getConnection: a conn whose WS socket has dropped (and is in
// reconnect backoff) must NEVER be handed out, even if it's next
// in the round-robin rotation. Without the gate, the caller's
// MakeRequest fails on a dead socket.
func TestWSPoolPicker_SkipsUnhealthy(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
		"/", 3, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	// Mark "a:1" unhealthy; the picker must keep returning b/c.
	pool.conn[0].(*fakeUpstreamConn).healthy.Store(false)

	for i := 0; i < 30; i++ {
		c, err := pool.getConnection()
		if err != nil {
			t.Fatalf("iter %d: unexpected error: %v", i, err)
		}
		host := c.(*fakeUpstreamConn).target.Host
		if host == "a:1" {
			t.Fatalf("iter %d: picker returned unhealthy conn %s", i, host)
		}
	}
}

// TestWSPoolPicker_PrefersLeastLoaded pins the load-aware tier of
// the picker: among healthy conns, the one with the fewest pinned
// subscriptions wins. Without this, subscriptions clump on whichever
// conn happens to be next in the round-robin rotation.
func TestWSPoolPicker_PrefersLeastLoaded(t *testing.T) {
	t.Run("ignores most-loaded", func(t *testing.T) {
		pool := NewUpstreamPool(
			[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
			"/", 3, func(*JsonRpcMsg) {}, fakeConstructor,
		)
		// Pre-load b:2 with 5 fake subscriptions; the picker should
		// route the next pick to either a:1 or c:3 (both at 0).
		pool.subCount[pool.conn[1]].Store(5)

		hits := map[string]int{}
		for i := 0; i < 60; i++ {
			c, err := pool.getConnection()
			if err != nil {
				t.Fatalf("iter %d: unexpected error: %v", i, err)
			}
			hits[c.(*fakeUpstreamConn).target.Host]++
		}
		if hits["b:2"] != 0 {
			t.Fatalf("loaded conn b:2 should never be picked while a/c are idler, got hits=%v", hits)
		}
		if hits["a:1"] == 0 || hits["c:3"] == 0 {
			t.Fatalf("expected picks to spread across a:1 and c:3, got hits=%v", hits)
		}
	})

	t.Run("picks unique lowest among healthy", func(t *testing.T) {
		// All three conns healthy but asymmetrically loaded:
		// a=1, b=2, c=3. The picker MUST pick a every time —
		// the rotation tiebreaker only kicks in when counts
		// are equal, and here they're not. Without the
		// load-aware tier this test fails because picks land
		// round-robin across b and c too.
		pool := NewUpstreamPool(
			[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
			"/", 3, func(*JsonRpcMsg) {}, fakeConstructor,
		)
		pool.subCount[pool.conn[0]].Store(1)
		pool.subCount[pool.conn[1]].Store(2)
		pool.subCount[pool.conn[2]].Store(3)

		for i := 0; i < 30; i++ {
			c, err := pool.getConnection()
			if err != nil {
				t.Fatalf("iter %d: unexpected error: %v", i, err)
			}
			host := c.(*fakeUpstreamConn).target.Host
			if host != "a:1" {
				t.Fatalf("iter %d: expected always a:1 (lowest load), got %s", i, host)
			}
		}
	})
}

// TestWSPoolPicker_RoundRobinTiebreaker pins the rotation behavior
// when every healthy conn is at the same sub-count: the atomic
// connIdx offset must spread picks instead of stampeding the first
// slot in iteration order. Without the rotation, "least loaded"
// would deterministically return p.conn[0] until something else
// nudged the count.
func TestWSPoolPicker_RoundRobinTiebreaker(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
		"/", 3, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	hits := map[string]int{}
	for i := 0; i < 90; i++ {
		c, err := pool.getConnection()
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		hits[c.(*fakeUpstreamConn).target.Host]++
	}
	// Every host should appear roughly 30 times. Allow generous
	// slack — the rotation is exact, but a future change to the
	// hash / index math shouldn't be required to land on
	// 30/30/30.
	for _, h := range []string{"a:1", "b:2", "c:3"} {
		if hits[h] < 20 || hits[h] > 40 {
			t.Fatalf("host %s got %d picks; want ~30 (hits=%v)", h, hits[h], hits)
		}
	}
}

// TestWSPoolPicker_AllUnhealthyError pins the explicit error path
// when every conn is in reconnect backoff. The caller (MakeRequest /
// Subscribe) propagates the error so the operator sees "no healthy
// upstream" in their logs with one grep instead of debugging a
// generic "websocket: connection closed" buried in conn.MakeRequest.
// The error is the ErrNoHealthyUpstream sentinel so callers can
// match it with errors.Is.
func TestWSPoolPicker_AllUnhealthyError(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2"},
		"/", 2, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	for _, c := range pool.conn {
		c.(*fakeUpstreamConn).healthy.Store(false)
	}

	_, err := pool.getConnection()
	if err == nil {
		t.Fatal("expected an error when every conn is unhealthy")
	}
	if !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("error should be ErrNoHealthyUpstream, got: %v", err)
	}

	// And MakeRequest propagates the same error verbatim — the
	// caller never gets handed a nil conn that would panic.
	_, err = pool.MakeRequest(&JsonRpcMsg{})
	if err == nil {
		t.Fatal("MakeRequest should propagate the picker's no-healthy error")
	}
	if !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("MakeRequest should propagate ErrNoHealthyUpstream, got: %v", err)
	}
}

// TestWSPoolPicker_SingleConnUnhealthy pins the n=1 fast path: a
// single conn going unhealthy must still produce ErrNoHealthyUpstream
// rather than dividing by zero in the modulo math or skipping the
// health check entirely. Guards against a future "n==1 short
// circuit" optimization that bypasses IsHealthy.
func TestWSPoolPicker_SingleConnUnhealthy(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://only:1"},
		"/", 1, func(*JsonRpcMsg) {}, fakeConstructor,
	)

	// Healthy first: picker returns the lone conn.
	c, err := pool.getConnection()
	if err != nil || c == nil {
		t.Fatalf("healthy single conn should be returned, got conn=%v err=%v", c, err)
	}

	// Then unhealthy: picker returns ErrNoHealthyUpstream, no panic.
	pool.conn[0].(*fakeUpstreamConn).healthy.Store(false)
	_, err = pool.getConnection()
	if !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("single unhealthy conn should yield ErrNoHealthyUpstream, got: %v", err)
	}
}

// TestWSPoolPicker_CountersTrackSubscribeUnsubscribe pins the
// invariant that the per-conn subCount stays in sync with the
// subscriptionConn map: every Subscribe bumps the conn's counter,
// every Unsubscribe drops it back. A regression that forgot the
// inc would show up here as the picker still seeing a "fully
// loaded" conn after every subscription was released — defeating
// the entire load-aware path.
func TestWSPoolPicker_CountersTrackSubscribeUnsubscribe(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1"},
		"/", 2, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	// Each Subscribe call routes to a healthy conn (both initially
	// at 0). Issue 4 distinct subscriptions; the picker should
	// spread them across the 2 conns.
	ids := []string{}
	for i := 0; i < 4; i++ {
		id, err := pool.Subscribe(fmt.Sprintf("param-%d", i))
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	total := pool.subCount[pool.conn[0]].Load() + pool.subCount[pool.conn[1]].Load()
	if total != 4 {
		t.Fatalf("after 4 Subscribes the counters should sum to 4, got %d (c0=%d c1=%d)",
			total, pool.subCount[pool.conn[0]].Load(), pool.subCount[pool.conn[1]].Load())
	}

	// And each conn should hold exactly 2 — the picker prefers
	// least-loaded so the spread is even by construction.
	if got := pool.subCount[pool.conn[0]].Load(); got != 2 {
		t.Fatalf("conn 0 should hold 2 subs, got %d", got)
	}
	if got := pool.subCount[pool.conn[1]].Load(); got != 2 {
		t.Fatalf("conn 1 should hold 2 subs, got %d", got)
	}

	// Unsubscribe everything; counters return to zero.
	for _, id := range ids {
		if err := pool.Unsubscribe(id); err != nil {
			t.Fatalf("Unsubscribe %s: %v", id, err)
		}
	}
	for i, c := range pool.conn {
		if got := pool.subCount[c].Load(); got != 0 {
			t.Fatalf("conn %d counter should be 0 after Unsubscribe-all, got %d", i, got)
		}
	}
}

// TestWSPoolPicker_ConcurrentSubscribeAndPick pins concurrency
// safety: many goroutines hammering Subscribe / Unsubscribe /
// getConnection at the same time must not race on subCount,
// subscriptionConn, or the IsHealthy / counter reads inside the
// picker. Run under -race; a regression that drops the addSubCount
// wrap, or that lets getConnection acquire subMux (deadlock), or
// that writes the subCount map outside the constructor (data race
// on the map header) all fail here.
func TestWSPoolPicker_ConcurrentSubscribeAndPick(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
		"/", 6, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Subscribers: each goroutine pins a fresh param and releases
	// it on a tight loop. The pool's bookkeeping must stay consistent.
	subscribers := 8
	wg.Add(subscribers)
	for i := 0; i < subscribers; i++ {
		go func(id int) {
			defer wg.Done()
			seq := 0
			for !stop.Load() {
				param := fmt.Sprintf("g%d-p%d", id, seq)
				subID, err := pool.Subscribe(param)
				if err != nil {
					continue
				}
				_ = pool.Unsubscribe(subID)
				seq++
			}
		}(i)
	}

	// Pickers: hammer getConnection from a separate goroutine
	// pool to make sure the lock-free read path doesn't race the
	// inc/dec writes under subMux.
	pickers := 8
	wg.Add(pickers)
	for i := 0; i < pickers; i++ {
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_, _ = pool.getConnection()
			}
		}()
	}

	// Health-flipper: a single goroutine toggles healthiness on
	// one conn so the picker exercises both branches concurrently
	// with the subscribe / pick storm.
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := false
		for !stop.Load() {
			pool.conn[0].(*fakeUpstreamConn).healthy.Store(flip)
			flip = !flip
		}
	}()

	time.Sleep(100 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// Sanity: after the storm settles, every counter should be
	// non-negative. A regression that dropped an inc or
	// double-decremented would leave a negative count.
	for i, c := range pool.conn {
		if got := pool.subCount[c].Load(); got < 0 {
			t.Fatalf("conn %d counter went negative under load: %d", i, got)
		}
	}
}

// TestWSPoolPicker_MigrationPrefersLeastLoadedAlt pins the load-aware
// migration path: when a dead conn's subscriptions need to land on a
// survivor, the migrator must pick the least-loaded healthy alt, not
// just the first one in the slice. Without this fix, a pool-wide
// outage that recovers one conn at a time would pile every orphaned
// sub onto the first survivor — defeating the same clumping the
// request-side picker prevents.
func TestWSPoolPicker_MigrationPrefersLeastLoadedAlt(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2", "ws://c:3"},
		"/", 3, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	// Pre-seed: a is the future-dead conn carrying our victim
	// subscription; b is already heavily loaded; c is idle. The
	// migrator must route to c, not b (the slice-first survivor).
	pool.subscriptionConn["victim-1"] = pool.conn[0]
	pool.subscriptionParam["victim-1"] = "tm.event=NewBlock"
	pool.subscriptionID["tm.event=NewBlock"] = "victim-1"
	pool.subCount[pool.conn[0]].Store(1) // a (dying) had 1
	pool.subCount[pool.conn[1]].Store(9) // b is heavily loaded
	pool.subCount[pool.conn[2]].Store(0) // c is idle

	pool.conn[0].(*fakeUpstreamConn).healthy.Store(false)

	migrations := pool.MigrateUnhealthy()
	if len(migrations) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migrations))
	}
	if pool.subscriptionConn[migrations[0].NewID] != pool.conn[2] {
		t.Fatalf("migrator should have routed to the least-loaded alt c:3, landed on %v",
			pool.subscriptionConn[migrations[0].NewID].(*fakeUpstreamConn).target.Host)
	}
	// And the counters should reflect the move.
	if got := pool.subCount[pool.conn[0]].Load(); got != 0 {
		t.Fatalf("dead conn counter should decrement to 0, got %d", got)
	}
	if got := pool.subCount[pool.conn[2]].Load(); got != 1 {
		t.Fatalf("survivor counter should increment to 1, got %d", got)
	}
}

// TestWSPoolPicker_CountersTrackMigration pins the migration leg:
// when MigrateUnhealthy moves a subscription off a dead conn onto
// a healthy alternative, both counters must update atomically so
// the picker's next decision sees the correct post-migration load.
func TestWSPoolPicker_CountersTrackMigration(t *testing.T) {
	pool := NewUpstreamPool(
		[]string{"ws://a:1", "ws://b:2"},
		"/", 2, func(*JsonRpcMsg) {}, fakeConstructor,
	)
	// Pin all subscriptions to conn 0 by marking conn 1 unhealthy
	// before any Subscribe — the picker will then only pick conn 0.
	pool.conn[1].(*fakeUpstreamConn).healthy.Store(false)
	for i := 0; i < 3; i++ {
		if _, err := pool.Subscribe(fmt.Sprintf("param-%d", i)); err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
	}
	if got := pool.subCount[pool.conn[0]].Load(); got != 3 {
		t.Fatalf("pre-migration: conn 0 should hold 3 subs, got %d", got)
	}

	// Now flip the health: conn 0 dies, conn 1 recovers. The
	// migrator must move all 3 subscriptions onto conn 1 and
	// fix the counters accordingly.
	pool.conn[0].(*fakeUpstreamConn).healthy.Store(false)
	pool.conn[1].(*fakeUpstreamConn).healthy.Store(true)

	migrated := pool.MigrateUnhealthy()
	if len(migrated) != 3 {
		t.Fatalf("expected 3 migrations, got %d", len(migrated))
	}
	if got := pool.subCount[pool.conn[0]].Load(); got != 0 {
		t.Fatalf("post-migration: conn 0 counter should be 0, got %d", got)
	}
	if got := pool.subCount[pool.conn[1]].Load(); got != 3 {
		t.Fatalf("post-migration: conn 1 counter should be 3, got %d", got)
	}
}
