package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeL2 is a programmable expiryAwareCache used only by the tiered
// tests so we can exercise corner cases (negative remaining-TTL,
// missing TTL, L2-only errors) without needing a live olric.
type fakeL2[K comparable, V any] struct {
	store    map[K]fakeEntry[V]
	getCalls atomic.Int32
	closed   bool
}

type fakeEntry[V any] struct {
	value    V
	expiryMs int64
}

func newFakeL2[K comparable, V any]() *fakeL2[K, V] {
	return &fakeL2[K, V]{store: map[K]fakeEntry[V]{}}
}

func (f *fakeL2[K, V]) Set(_ context.Context, key K, value V, ttl time.Duration) error {
	var expiryMs int64
	if ttl > 0 {
		expiryMs = time.Now().Add(ttl).UnixMilli()
	}
	f.store[key] = fakeEntry[V]{value: value, expiryMs: expiryMs}
	return nil
}

func (f *fakeL2[K, V]) Get(ctx context.Context, key K) (V, error) {
	v, _, err := f.GetWithExpiry(ctx, key)
	return v, err
}

func (f *fakeL2[K, V]) GetWithExpiry(_ context.Context, key K) (V, int64, error) {
	f.getCalls.Add(1)
	var zero V
	e, ok := f.store[key]
	if !ok {
		return zero, 0, ErrNotFound
	}
	return e.value, e.expiryMs, nil
}

func (f *fakeL2[K, V]) Has(ctx context.Context, key K) (bool, error) {
	_, err := f.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (f *fakeL2[K, V]) Close() error { f.closed = true; return nil }

// setExpiryAt forces an entry's stored deadline (mostly to test the
// already-expired-but-not-yet-reaped case).
func (f *fakeL2[K, V]) setExpiryAt(key K, value V, expiryMs int64) {
	f.store[key] = fakeEntry[V]{value: value, expiryMs: expiryMs}
}

func newTieredForTest[K comparable, V any](t *testing.T) (*TieredCache[K, V], *fakeL2[K, V]) {
	t.Helper()
	l1, err := NewMemoryCache[K, V]("test", DefaultTTL(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	l2 := newFakeL2[K, V]()
	tc, err := NewTieredCache[K, V](l1, l2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tc.Close() })
	return tc, l2
}

// TestTieredSecondGetServedFromL1 — the central reason this exists.
// Once a key is fetched (L2 hit), subsequent reads must come from L1
// without touching L2. We measure that via fakeL2's call counter.
func TestTieredSecondGetServedFromL1(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	if err := tc.Set(ctx, "k", "v1", time.Minute); err != nil {
		t.Fatal(err)
	}
	// Set drives one L2 write; getCalls should stay at 0.
	if got := l2.getCalls.Load(); got != 0 {
		t.Fatalf("after Set: L2 GetWithExpiry call count = %d, want 0", got)
	}

	for i := 0; i < 5; i++ {
		v, err := tc.Get(ctx, "k")
		if err != nil || v != "v1" {
			t.Fatalf("iter %d: Get = (%q, %v), want (\"v1\", nil)", i, v, err)
		}
	}
	// Set populated L1, so none of the 5 Gets should have hit L2.
	if got := l2.getCalls.Load(); got != 0 {
		t.Errorf("L1 should have served every Get after Set, L2 GetWithExpiry call count = %d", got)
	}
}

// TestTieredL1MissPopulatesFromL2 — a value written by another pod
// lives only in L2 from this pod's perspective. The first Get must
// fall through to L2 (one call) and the next Get must hit L1 (no
// additional call).
func TestTieredL1MissPopulatesFromL2(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	// Write directly into L2 (simulates a write from another pod).
	_ = l2.Set(ctx, "k", "v-from-peer", time.Minute)
	if got := l2.getCalls.Load(); got != 0 {
		t.Fatalf("baseline: L2 GetWithExpiry = %d, want 0", got)
	}

	v, err := tc.Get(ctx, "k")
	if err != nil || v != "v-from-peer" {
		t.Fatalf("first Get = (%q, %v), want (\"v-from-peer\", nil)", v, err)
	}
	if got := l2.getCalls.Load(); got != 1 {
		t.Errorf("first Get should make exactly 1 L2 call, got %d", got)
	}

	// Second Get must be served from L1 — call count stays at 1.
	v, err = tc.Get(ctx, "k")
	if err != nil || v != "v-from-peer" {
		t.Fatalf("second Get = (%q, %v), want (\"v-from-peer\", nil)", v, err)
	}
	if got := l2.getCalls.Load(); got != 1 {
		t.Errorf("second Get hit L2 (call count = %d), L1 should have served it", got)
	}
}

// TestTieredL1HonoursL2Expiry — the key correctness property. L1
// entries must expire at the same wall-clock instant as L2 entries.
// We simulate by writing a value to L2 with a short remaining
// deadline, fetching it (populates L1), then sleeping past the
// deadline and asserting L1 has expired (next Get hits L2 again).
func TestTieredL1HonoursL2Expiry(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	// Simulate a peer-written entry with 50 ms remaining.
	l2.setExpiryAt("k", "soon-gone", time.Now().Add(50*time.Millisecond).UnixMilli())

	v, err := tc.Get(ctx, "k")
	if err != nil || v != "soon-gone" {
		t.Fatalf("initial Get = (%q, %v)", v, err)
	}
	startCalls := l2.getCalls.Load()

	// Repeat Gets within the window stay on L1.
	v, _ = tc.Get(ctx, "k")
	if v != "soon-gone" {
		t.Fatalf("within-window Get = %q, want soon-gone", v)
	}
	if got := l2.getCalls.Load(); got != startCalls {
		t.Errorf("within-window Get hit L2 unexpectedly (%d new calls)", got-startCalls)
	}

	// Sleep past the wall-clock deadline.
	time.Sleep(80 * time.Millisecond)

	// L1 must have expired now. Drop the L2 entry too so a successful
	// Get can only mean L1 served stale data — which is the bug we're
	// guarding against. We expect ErrNotFound.
	delete(l2.store, "k")
	if _, err := tc.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after expiry: Get err = %v, want ErrNotFound (L1 served stale data)", err)
	}
}

// TestTieredL1SkipsExpiredL2Entry — if L2 returns an entry whose
// stored deadline is in the past (clock skew, racy reaper, etc.),
// TieredCache must NOT cache that value in L1 — otherwise the L1
// would pin a dead value until ttlcache's default TTL.
func TestTieredL1SkipsExpiredL2Entry(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	// Put an L2 entry with a deadline already in the past.
	l2.setExpiryAt("k", "stale", time.Now().Add(-5*time.Second).UnixMilli())

	v, err := tc.Get(ctx, "k")
	if err != nil || v != "stale" {
		t.Fatalf("Get returned (%q, %v) — should return the L2 value verbatim", v, err)
	}
	// L1 must NOT have cached this; remove it from L2 and re-Get must miss.
	delete(l2.store, "k")
	if _, err := tc.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("L1 cached an already-expired L2 entry: Get err = %v, want ErrNotFound", err)
	}
}

// TestTieredSetWritesBothLayers — write goes to L2 first, then L1.
// We verify L2 has the value (via direct inspection) and that an
// immediate Get is served from L1 (no L2 call needed).
func TestTieredSetWritesBothLayers(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	if err := tc.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatal(err)
	}
	if e, ok := l2.store["k"]; !ok || e.value != "v" {
		t.Errorf("L2 missing entry after Set: %+v", l2.store)
	}
	beforeCalls := l2.getCalls.Load()
	if v, err := tc.Get(ctx, "k"); err != nil || v != "v" {
		t.Fatalf("Get = (%q, %v)", v, err)
	}
	if got := l2.getCalls.Load(); got != beforeCalls {
		t.Errorf("Get after Set hit L2 (%d call) — L1 should have served it", got-beforeCalls)
	}
}

// TestTieredHasChecksBothLayers — Has true on L1 hit, then on L2
// hit, then false when neither has it.
func TestTieredHasChecksBothLayers(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	// Neither layer has it.
	if ok, _ := tc.Has(ctx, "k"); ok {
		t.Errorf("Has on missing key returned true")
	}

	// L2-only.
	_ = l2.Set(ctx, "k", "v", time.Minute)
	if ok, _ := tc.Has(ctx, "k"); !ok {
		t.Errorf("Has should find an L2-only entry")
	}

	// L1 + L2.
	_ = tc.Set(ctx, "k2", "v2", time.Minute)
	if ok, _ := tc.Has(ctx, "k2"); !ok {
		t.Errorf("Has should find an entry written via tc.Set")
	}
}

// TestTieredInvalidateLocal — InvalidateLocal must drop L1 contents
// without touching L2. After invalidation, the next Get must consult
// L2 again.
func TestTieredInvalidateLocal(t *testing.T) {
	tc, l2 := newTieredForTest[string, string](t)
	ctx := context.Background()

	_ = tc.Set(ctx, "k", "v", time.Minute)
	// Warm the L1 (Set already populates it but be explicit).
	_, _ = tc.Get(ctx, "k")
	beforeCalls := l2.getCalls.Load()

	tc.InvalidateLocal()

	if v, err := tc.Get(ctx, "k"); err != nil || v != "v" {
		t.Fatalf("after InvalidateLocal: Get = (%q, %v)", v, err)
	}
	if got := l2.getCalls.Load(); got != beforeCalls+1 {
		t.Errorf("expected exactly 1 new L2 call after InvalidateLocal, got %d", got-beforeCalls)
	}
}

// TestTieredCloseCallsBoth — Close cascades to L1 + L2.
func TestTieredCloseCallsBoth(t *testing.T) {
	l1, _ := NewMemoryCache[string, string]("t", DefaultTTL(time.Minute))
	l2 := newFakeL2[string, string]()
	tc, err := NewTieredCache[string, string](l1, l2)
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.Close(); err != nil {
		t.Errorf("Close err = %v", err)
	}
	if !l2.closed {
		t.Errorf("L2 was not closed via TieredCache.Close")
	}
}

// TestTieredRequiresBothLayers — defensive: nil L1 or L2 must error
// at construction, not panic at first Get.
func TestTieredRequiresBothLayers(t *testing.T) {
	if _, err := NewTieredCache[string, string](nil, newFakeL2[string, string]()); err == nil {
		t.Errorf("NewTieredCache(nil, l2) should error")
	}
	l1, _ := NewMemoryCache[string, string]("t", DefaultTTL(time.Minute))
	if _, err := NewTieredCache[string, string](l1, nil); err == nil {
		t.Errorf("NewTieredCache(l1, nil) should error")
	}
}
