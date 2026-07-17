package cosmoguard

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// fakeMigratingConn implements UpstreamConnManager with operator-
// controlled health + an in-memory subscription log. Built per-test so
// each migration scenario is isolated.
type fakeMigratingConn struct {
	healthy    atomic.Bool
	subCalls   atomic.Int32
	subscribed map[string]string // id → param
	failOnce   atomic.Bool       // if true, the next Subscribe returns an error and clears the flag
	idGen      *util.UniqueID
	name       string
}

func (f *fakeMigratingConn) Run(_ *Entry) error                           { return nil }
func (f *fakeMigratingConn) MakeRequest(*JsonRpcMsg) (*JsonRpcMsg, error) { return nil, nil }
func (f *fakeMigratingConn) IsHealthy() bool                              { return f.healthy.Load() }
func (f *fakeMigratingConn) HasSubscription(param string) bool {
	for _, p := range f.subscribed {
		if p == param {
			return true
		}
	}
	return false
}
func (f *fakeMigratingConn) Subscribe(param string) (string, error) {
	if f.failOnce.Load() {
		f.failOnce.Store(false)
		return "", errors.New("subscribe failed")
	}
	f.subCalls.Add(1)
	id := f.idGen.ID()
	if f.subscribed == nil {
		f.subscribed = map[string]string{}
	}
	f.subscribed[id] = param
	return id, nil
}
func (f *fakeMigratingConn) Unsubscribe(id string) error {
	delete(f.subscribed, id)
	return nil
}
func (f *fakeMigratingConn) LocalUnsubscribe(param string) {
	for id, p := range f.subscribed {
		if p == param {
			delete(f.subscribed, id)
			return
		}
	}
}
func (f *fakeMigratingConn) Stop() {}

// TestMigrateUnhealthy_MovesSubscriptionToSurvivor: dead = pinned conn;
// healthy = alternate. After MigrateUnhealthy, the pool maps point at
// the survivor and the survivor has a fresh subscribe call.
func TestMigrateUnhealthy_MovesSubscriptionToSurvivor(t *testing.T) {
	idGen := &util.UniqueID{}
	dead := &fakeMigratingConn{idGen: idGen, name: "dead"}
	alive := &fakeMigratingConn{idGen: idGen, name: "alive"}
	dead.healthy.Store(false)
	alive.healthy.Store(true)

	pool := &UpstreamPool{
		conn:              []UpstreamConnManager{dead, alive},
		subscriptionConn:  map[string]UpstreamConnManager{},
		subscriptionID:    map[string]string{},
		subscriptionParam: map[string]string{},
	}
	// Pretend the dead conn was holding a subscription for "tm.event=NewBlock".
	pool.subscriptionConn["old-1"] = dead
	pool.subscriptionParam["old-1"] = "tm.event=NewBlock"
	pool.subscriptionID["tm.event=NewBlock"] = "old-1"

	migrations := pool.MigrateUnhealthy()
	if len(migrations) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migrations))
	}
	m := migrations[0]
	if m.OldID != "old-1" || m.Param != "tm.event=NewBlock" || m.NewID == "old-1" {
		t.Fatalf("unexpected migration tuple: %+v", m)
	}
	if pool.subscriptionConn[m.NewID] != alive {
		t.Fatalf("pool didn't rewire subscriptionConn to alive: %+v", pool.subscriptionConn)
	}
	if pool.subscriptionParam[m.NewID] != "tm.event=NewBlock" {
		t.Fatal("pool didn't rewire subscriptionParam")
	}
	if pool.subscriptionID["tm.event=NewBlock"] != m.NewID {
		t.Fatal("pool didn't rewire subscriptionID")
	}
	if _, stillThere := pool.subscriptionConn["old-1"]; stillThere {
		t.Fatal("pool still references the old id")
	}
	if alive.subCalls.Load() != 1 {
		t.Fatalf("alive should have received 1 Subscribe call; got %d", alive.subCalls.Load())
	}
}

// TestMigrateUnhealthy_NoSurvivorKeepsSubscription: both upstreams
// unhealthy → no migration, no pool mutation. The subscription stays
// pinned so the existing per-conn auto-reconnect can still recover.
func TestMigrateUnhealthy_NoSurvivorKeepsSubscription(t *testing.T) {
	idGen := &util.UniqueID{}
	a := &fakeMigratingConn{idGen: idGen, name: "a"}
	b := &fakeMigratingConn{idGen: idGen, name: "b"}
	// Both unhealthy.
	a.healthy.Store(false)
	b.healthy.Store(false)

	pool := &UpstreamPool{
		conn:              []UpstreamConnManager{a, b},
		subscriptionConn:  map[string]UpstreamConnManager{"sub-x": a},
		subscriptionParam: map[string]string{"sub-x": "param-x"},
		subscriptionID:    map[string]string{"param-x": "sub-x"},
	}

	if got := pool.MigrateUnhealthy(); len(got) != 0 {
		t.Fatalf("expected no migrations, got %d", len(got))
	}
	if pool.subscriptionConn["sub-x"] != a {
		t.Fatal("subscription should remain pinned when there's no survivor")
	}
}

// TestSubscriptionManager_MigrateKeepsCanonicalID: after a migration the
// client-facing (canonical) id stays stable — only the upstream id moves.
// This is the invariant that stops EVM clients from silently dropping
// events (they match notifications on params.subscription == canonical id)
// and that keeps eth_unsubscribe(canonicalID) resolving.
func TestSubscriptionManager_MigrateKeepsCanonicalID(t *testing.T) {
	sm := NewSubscriptionManager()
	sm.AddSubscription("p", "old")
	client := &JsonRpcWsClient{} // empty struct is fine — used as a map key here
	sm.SubscribeClient("old", client, "client-tag")

	if !sm.Migrate("old", "new") {
		t.Fatal("migrate should succeed")
	}

	// Canonical id (client-facing) is UNCHANGED.
	if id, _ := sm.GetSubscriptionID("p"); id != "old" {
		t.Fatalf("canonical id should stay 'old', got %s", id)
	}
	if p, _ := sm.GetSubscriptionParam("old"); p != "p" {
		t.Fatalf("canonical id lost its param mapping: %s", p)
	}
	// The current upstream id resolves forward from the canonical id...
	if up, ok := sm.UpstreamID("old"); !ok || up != "new" {
		t.Fatalf("UpstreamID(old) = %q, %v; want new,true", up, ok)
	}
	// ...and an inbound notification carrying the NEW upstream id routes
	// back to the canonical id.
	if canon, ok := sm.CanonicalID("new"); !ok || canon != "old" {
		t.Fatalf("CanonicalID(new) = %q, %v; want old,true", canon, ok)
	}
	// Client set is preserved under the canonical id (never moved).
	clients := sm.GetSubscriptionClients("old")
	if clients[client] != "client-tag" {
		t.Fatalf("client set not preserved: %+v", clients)
	}

	// A second migration chains correctly off the current upstream id.
	if !sm.Migrate("new", "newer") {
		t.Fatal("second migrate should succeed")
	}
	if canon, _ := sm.CanonicalID("newer"); canon != "old" {
		t.Fatalf("CanonicalID(newer) = %q; want old", canon)
	}
	if up, _ := sm.UpstreamID("old"); up != "newer" {
		t.Fatalf("UpstreamID(old) = %q; want newer", up)
	}
	// The stale upstream id no longer resolves.
	if _, ok := sm.CanonicalID("new"); ok {
		t.Fatal("stale upstream id 'new' should no longer translate")
	}
}

func TestSubscriptionManager_MigrateMissingOldReturnsFalse(t *testing.T) {
	sm := NewSubscriptionManager()
	if sm.Migrate("nope", "new") {
		t.Fatal("expected false for unknown upstream id")
	}
}

// TestSubscriptionManager_RemoveClearsTranslation ensures RemoveSubscription
// tears down the canonical↔upstream translation entries too (no leak).
func TestSubscriptionManager_RemoveClearsTranslation(t *testing.T) {
	sm := NewSubscriptionManager()
	sm.AddSubscription("p", "old")
	sm.Migrate("old", "new")
	sm.RemoveSubscription("old")
	if _, ok := sm.CanonicalID("new"); ok {
		t.Fatal("upstream translation should be gone after remove")
	}
	if _, ok := sm.UpstreamID("old"); ok {
		t.Fatal("canonical translation should be gone after remove")
	}
}
