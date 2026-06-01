package cosmoguard

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLookup implements LookupFunc with a swappable answer. Tests
// mutate the answer to drive the reconciler through different DNS
// outcomes without touching the system resolver.
type fakeLookup struct {
	mu  sync.Mutex
	ips []string
	err error
	// calls tracks how many times the lookup ran — used by tests
	// that need to confirm the ticker fired without sleeping on
	// the wall clock.
	calls atomic.Int32
}

func (f *fakeLookup) lookup(_ context.Context, _ string) ([]string, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make([]string, len(f.ips))
	copy(out, f.ips)
	return out, nil
}

func (f *fakeLookup) set(ips []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ips = ips
	f.err = err
}

// TestExpandDiscoveryNodes_Empty: no Discovery templates — Nodes
// passes through unchanged and templates slice is empty.
func TestExpandDiscoveryNodes_Empty(t *testing.T) {
	cfg := &Config{Nodes: []NodeConfig{{Name: "a", Host: "10.0.0.1"}}}
	tmpls, err := expandDiscoveryNodes(cfg, func(context.Context, string) ([]string, error) {
		t.Fatal("lookup should not be called when no template is set")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tmpls) != 0 {
		t.Fatalf("expected 0 templates, got %d", len(tmpls))
	}
	if len(cfg.Nodes) != 1 || cfg.Nodes[0].Name != "a" {
		t.Fatalf("static node should pass through, got %+v", cfg.Nodes)
	}
}

// TestExpandDiscoveryNodes_Expands: a template resolves to 3 IPs and
// produces 3 concrete nodes with stable name/host.
func TestExpandDiscoveryNodes_Expands(t *testing.T) {
	tmpl := NodeConfig{
		Name: "validators",
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "validators.default.svc.cluster.local",
			RefreshInterval: 15 * time.Second,
		},
		RpcPort: 26657,
		LcdPort: 1317,
	}
	cfg := &Config{Nodes: []NodeConfig{tmpl}}
	lookup := func(_ context.Context, host string) ([]string, error) {
		if host != tmpl.Discovery.Host {
			t.Fatalf("unexpected host: %s", host)
		}
		return []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, nil
	}
	tmpls, err := expandDiscoveryNodes(cfg, lookup)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tmpls) != 1 {
		t.Fatalf("expected 1 template, got %d", len(tmpls))
	}
	if len(cfg.Nodes) != 3 {
		t.Fatalf("expected 3 expanded nodes, got %d", len(cfg.Nodes))
	}
	// Verify expansion order is sorted (so cosmoguard's pool ordering
	// stays deterministic across boots).
	wantNames := []string{"validators-10.0.0.1", "validators-10.0.0.2", "validators-10.0.0.3"}
	for i, n := range cfg.Nodes {
		if n.Name != wantNames[i] {
			t.Fatalf("Nodes[%d].Name=%q want %q", i, n.Name, wantNames[i])
		}
		if n.Host == "" || n.Discovery != nil {
			t.Fatalf("Nodes[%d] should have Host set and Discovery nil; got %+v", i, n)
		}
		if n.RpcPort != 26657 || n.LcdPort != 1317 {
			t.Fatalf("Nodes[%d] template ports not carried: %+v", i, n)
		}
	}
}

// TestExpandDiscoveryNodes_ZeroIPs: a template resolving to zero IPs
// is NOT a hard error — the runtime reconciler is here to absorb a
// not-yet-ready headless service. The template is captured for
// later reconcile; cfg.Nodes is left empty.
func TestExpandDiscoveryNodes_ZeroIPs(t *testing.T) {
	cfg := &Config{Nodes: []NodeConfig{{
		Name:      "pending",
		Discovery: &DiscoveryConfig{Type: "dns", Host: "pending.svc"},
	}}}
	tmpls, err := expandDiscoveryNodes(cfg, func(context.Context, string) ([]string, error) { return nil, nil })
	if err != nil {
		t.Fatalf("zero-IP lookup should not error: %v", err)
	}
	if len(tmpls) != 1 {
		t.Fatalf("zero-IP template should still be tracked, got %d templates", len(tmpls))
	}
	if len(cfg.Nodes) != 0 {
		t.Fatalf("expected 0 expanded nodes, got %d", len(cfg.Nodes))
	}
}

// TestExpandDiscoveryNodes_LookupError: a DNS lookup error is
// soft-failed — the template is recorded for later retry by the
// reconciler. Boot still succeeds (operators don't want a transient
// DNS hiccup to crash-loop their proxy).
func TestExpandDiscoveryNodes_LookupError(t *testing.T) {
	cfg := &Config{Nodes: []NodeConfig{{
		Name:      "broken",
		Discovery: &DiscoveryConfig{Type: "dns", Host: "broken.svc"},
	}}}
	tmpls, err := expandDiscoveryNodes(cfg, func(context.Context, string) ([]string, error) {
		return nil, errors.New("dns server unreachable")
	})
	if err != nil {
		t.Fatalf("lookup error should be soft-failed at boot: %v", err)
	}
	if len(tmpls) != 1 {
		t.Fatalf("failing template should still be tracked, got %d templates", len(tmpls))
	}
}

// TestExpandDiscoveryNodes_MixStaticAndTemplate: a static node and a
// template coexist. Static passes through; template expands to its
// resolved IPs and joins the same Nodes slice.
func TestExpandDiscoveryNodes_MixStaticAndTemplate(t *testing.T) {
	cfg := &Config{Nodes: []NodeConfig{
		{Name: "static", Host: "10.1.0.1"},
		{Name: "dyn", Discovery: &DiscoveryConfig{Type: "dns", Host: "dyn.svc"}},
	}}
	tmpls, err := expandDiscoveryNodes(cfg, func(context.Context, string) ([]string, error) {
		return []string{"10.2.0.1", "10.2.0.2"}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tmpls) != 1 {
		t.Fatalf("expected 1 template, got %d", len(tmpls))
	}
	if len(cfg.Nodes) != 3 {
		t.Fatalf("expected 3 nodes (1 static + 2 expanded), got %d", len(cfg.Nodes))
	}
	if cfg.Nodes[0].Name != "static" {
		t.Fatalf("static should appear before expanded entries, got %q", cfg.Nodes[0].Name)
	}
}

// TestDiscoverer_ReconcilesAddAndRemove builds two static HTTP pools
// (no constructor-time DNS) plus a Discoverer with a single template
// and drives it through:
//   - first reconcile: adds two upstreams
//   - second reconcile: removes one, adds one
//
// Then verifies both pools converge.
func TestDiscoverer_ReconcilesAddAndRemove(t *testing.T) {
	// Two HTTP pools, both empty except for one seed upstream so
	// the constructor doesn't reject them. The seed is named in a
	// way that doesn't collide with discovery's synthetic names.
	seed := newTestUpstream("seed", 1)
	poolA := newTestHTTPPool("round-robin", 0, seed)
	poolA.name = "lcd"
	poolA.service = "lcd"

	poolB := newTestHTTPPool("round-robin", 0, newTestUpstream("seed", 1))
	poolB.name = "rpc"
	poolB.service = "rpc"

	tmpl := NodeConfig{
		Name:    "pods",
		RpcPort: 26657,
		LcdPort: 1317,
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "pods.svc",
			RefreshInterval: 10 * time.Millisecond,
		},
	}

	fl := &fakeLookup{}
	fl.set([]string{"10.0.0.1", "10.0.0.2"}, nil)

	d := NewDiscoverer(log, []DiscoveryTemplate{{Template: tmpl, SeedIPs: map[string]struct{}{}}}, fl.lookup)
	d.RegisterHttpPool(poolA)
	d.RegisterHttpPool(poolB)
	d.Start()
	t.Cleanup(d.Stop)

	if !waitFor(t, 1*time.Second, func() bool {
		return upstreamHasName(poolA, "pods-10.0.0.1") &&
			upstreamHasName(poolA, "pods-10.0.0.2") &&
			upstreamHasName(poolB, "pods-10.0.0.1") &&
			upstreamHasName(poolB, "pods-10.0.0.2")
	}) {
		t.Fatalf("expected both pools to converge to {seed, pods-10.0.0.1, pods-10.0.0.2}, got A=%v B=%v",
			upstreamNames(poolA), upstreamNames(poolB))
	}

	// Now drop 10.0.0.1 and add 10.0.0.3.
	fl.set([]string{"10.0.0.2", "10.0.0.3"}, nil)

	if !waitFor(t, 1*time.Second, func() bool {
		return !upstreamHasName(poolA, "pods-10.0.0.1") &&
			upstreamHasName(poolA, "pods-10.0.0.3") &&
			!upstreamHasName(poolB, "pods-10.0.0.1") &&
			upstreamHasName(poolB, "pods-10.0.0.3")
	}) {
		t.Fatalf("reconcile didn't converge after IP shift; A=%v B=%v",
			upstreamNames(poolA), upstreamNames(poolB))
	}
}

// TestDiscoverer_LookupErrorKeepsState: when a reconcile lookup
// fails, the existing pool membership must be preserved (an
// intermittent DNS error must not silently empty the pool).
func TestDiscoverer_LookupErrorKeepsState(t *testing.T) {
	pool := newTestHTTPPool("round-robin", 0, newTestUpstream("seed", 1))
	pool.name = "lcd"
	pool.service = "lcd"

	tmpl := NodeConfig{
		Name:    "pods",
		LcdPort: 1317,
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "pods.svc",
			RefreshInterval: 10 * time.Millisecond,
		},
	}

	fl := &fakeLookup{}
	fl.set([]string{"10.0.0.1"}, nil)
	// Empty SeedIPs: the test pool wasn't preseeded with the
	// template's IPs at construction (unlike production), so the
	// first reconcile is expected to ADD the upstream.
	d := NewDiscoverer(log, []DiscoveryTemplate{{
		Template: tmpl,
		SeedIPs:  map[string]struct{}{},
	}}, fl.lookup)
	d.RegisterHttpPool(pool)
	d.Start()
	t.Cleanup(d.Stop)

	if !waitFor(t, 1*time.Second, func() bool {
		return upstreamHasName(pool, "pods-10.0.0.1")
	}) {
		t.Fatalf("initial add didn't land: %v", upstreamNames(pool))
	}

	// Flip the lookup to error. The reconciler must NOT remove the
	// existing upstream.
	fl.set(nil, errors.New("dns timeout"))

	// Wait for at least one more lookup tick to have fired.
	target := fl.calls.Load() + 3
	if !waitFor(t, 1*time.Second, func() bool {
		return fl.calls.Load() >= target
	}) {
		t.Fatalf("lookup didn't run again under error path")
	}
	if !upstreamHasName(pool, "pods-10.0.0.1") {
		t.Fatalf("upstream removed under DNS error; pool=%v", upstreamNames(pool))
	}
}

// TestDiscoverer_StopIsIdempotent: Stop on a never-started Discoverer
// is a no-op; Stop twice on a started one drains exactly once.
func TestDiscoverer_StopIsIdempotent(t *testing.T) {
	d := NewDiscoverer(log, nil, nil)
	d.Stop() // never started — should not deadlock
	d.Stop()

	tmpl := NodeConfig{
		Name: "x",
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "x.svc",
			RefreshInterval: 10 * time.Millisecond,
		},
	}
	d2 := NewDiscoverer(log, []DiscoveryTemplate{{
		Template: tmpl,
		SeedIPs:  map[string]struct{}{},
	}}, func(context.Context, string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	})
	d2.Start()
	d2.Stop()
	d2.Stop() // double-stop must be safe
}

// TestDiscoverer_SeededFromExpansion: verify that an IP populated into
// SeedIPs by expandDiscoveryNodes is NOT re-added on the first
// reconcile — the seed must take precedence over a fresh lookup that
// happens to return the same IP. This protects against duplicate
// "discovery added upstream" logs on every cosmoguard restart.
func TestDiscoverer_SeededFromExpansion(t *testing.T) {
	pool := newTestHTTPPool("round-robin", 0, newTestUpstream("seed", 1))
	pool.name = "lcd"
	pool.service = "lcd"

	tmpl := NodeConfig{
		Name:    "pods",
		LcdPort: 1317,
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "pods.svc",
			RefreshInterval: 10 * time.Millisecond,
		},
	}

	fl := &fakeLookup{}
	fl.set([]string{"10.0.0.1"}, nil)

	d := NewDiscoverer(log, []DiscoveryTemplate{{
		Template: tmpl,
		// SeedIPs mimics what expandDiscoveryNodes would have done at
		// boot — 10.0.0.1 was already installed in the pool then.
		SeedIPs: map[string]struct{}{"10.0.0.1": {}},
	}}, fl.lookup)
	d.RegisterHttpPool(pool)
	d.Start()
	t.Cleanup(d.Stop)

	// Wait for one full tick.
	target := fl.calls.Load() + 2
	if !waitFor(t, 1*time.Second, func() bool {
		return fl.calls.Load() >= target
	}) {
		t.Fatalf("lookup didn't tick")
	}
	// Pool should NOT have grown — the IP was seeded into current.
	if upstreamHasName(pool, "pods-10.0.0.1") {
		t.Fatalf("seeded IP was treated as new and added to pool: %v", upstreamNames(pool))
	}
}

// TestDiscoverer_GrpcOnlyRegistration: a Discoverer with only a gRPC
// pool registered (no HTTP pools) must still reconcile correctly.
// Regression for the original implementation, which seeded `current`
// by walking httpPools[0] and silently mis-tracked state for any
// template that only fed a gRPC pool.
func TestDiscoverer_GrpcOnlyRegistration(t *testing.T) {
	grpcPool := newTestGrpcPool("round-robin")
	grpcPool.name = "grpc"

	tmpl := NodeConfig{
		Name:     "pods",
		GrpcPort: 9090,
		Discovery: &DiscoveryConfig{
			Type:            "dns",
			Host:            "pods.svc",
			RefreshInterval: 10 * time.Millisecond,
		},
	}

	fl := &fakeLookup{}
	fl.set([]string{"10.0.0.1"}, nil)

	d := NewDiscoverer(log, []DiscoveryTemplate{{
		Template: tmpl,
		SeedIPs:  map[string]struct{}{},
	}}, fl.lookup)
	d.RegisterGrpcPool(grpcPool)
	d.Start()
	t.Cleanup(d.Stop)

	if !waitFor(t, 1*time.Second, func() bool {
		for _, u := range grpcPool.Upstreams() {
			if u.Name == "pods-10.0.0.1" {
				return true
			}
		}
		return false
	}) {
		names := []string{}
		for _, u := range grpcPool.Upstreams() {
			names = append(names, u.Name)
		}
		t.Fatalf("gRPC-only reconcile didn't add upstream: pool=%v", names)
	}

	// Shift the IP and verify the previous one is removed.
	fl.set([]string{"10.0.0.2"}, nil)
	if !waitFor(t, 1*time.Second, func() bool {
		hasNew, hasOld := false, false
		for _, u := range grpcPool.Upstreams() {
			if u.Name == "pods-10.0.0.2" {
				hasNew = true
			}
			if u.Name == "pods-10.0.0.1" {
				hasOld = true
			}
		}
		return hasNew && !hasOld
	}) {
		names := []string{}
		for _, u := range grpcPool.Upstreams() {
			names = append(names, u.Name)
		}
		t.Fatalf("gRPC-only reconcile didn't converge after shift: pool=%v", names)
	}
}

// TestHttpUpstreamPool_AddRemove exercises the new dynamic API
// directly — verifies Pick sees added upstreams, RemoveUpstream
// drops them, and idempotency holds.
func TestHttpUpstreamPool_AddRemove(t *testing.T) {
	seed := newTestUpstream("seed", 1)
	pool := newTestHTTPPool("round-robin", 0, seed)
	pool.name = "lcd"
	pool.service = "lcd"

	n := NodeConfig{Name: "added", Host: "10.0.0.10", LcdPort: 1317}
	if err := pool.AddUpstream(n); err != nil {
		t.Fatalf("AddUpstream: %v", err)
	}
	if !upstreamHasName(pool, "added") {
		t.Fatalf("AddUpstream didn't publish; pool=%v", upstreamNames(pool))
	}
	// Idempotency: re-add same name is a no-op.
	if err := pool.AddUpstream(n); err != nil {
		t.Fatalf("re-AddUpstream: %v", err)
	}
	if got := len(pool.Upstreams()); got != 2 {
		t.Fatalf("expected 2 upstreams after idempotent re-add, got %d", got)
	}

	pool.RemoveUpstream("added")
	if upstreamHasName(pool, "added") {
		t.Fatalf("RemoveUpstream didn't drop; pool=%v", upstreamNames(pool))
	}
	// Remove of unknown name is a no-op.
	pool.RemoveUpstream("not-there")
	if got := len(pool.Upstreams()); got != 1 {
		t.Fatalf("expected 1 upstream after stray remove, got %d", got)
	}
}

// --- small helpers ---

func upstreamNames(p *HttpUpstreamPool) []string {
	ups := p.Upstreams()
	out := make([]string, 0, len(ups))
	for _, u := range ups {
		out = append(out, u.Name)
	}
	sort.Strings(out)
	return out
}

func upstreamHasName(p *HttpUpstreamPool, name string) bool {
	for _, u := range p.Upstreams() {
		if u.Name == name {
			return true
		}
	}
	return false
}

// waitFor polls cond every 10ms up to timeout. Returns true if cond
// became true, false on timeout. Used to drive the reconciler tests
// without sleeping for the full ticker interval.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
