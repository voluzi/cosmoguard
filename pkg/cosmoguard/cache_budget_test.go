package cosmoguard

import "testing"

// withMemoryLimit swaps the injectable limit provider for the duration of a
// test so budget resolution can be exercised deterministically without
// reading the host's cgroup.
func withMemoryLimit(t *testing.T, limit uint64, ok bool) {
	t.Helper()
	prev := memoryLimitProvider
	memoryLimitProvider = func() (uint64, bool) { return limit, ok }
	t.Cleanup(func() { memoryLimitProvider = prev })
}

func int64p(v int64) *int64       { return &v }
func float64p(v float64) *float64 { return &v }

// TestResolveBudget_ScalingReserve checks the auto model across pod sizes:
// reserve = max(128MiB, 0.20*L); budget = L-reserve; L1=40%, L2=60%. The
// critical invariant (issue #15 / advisor): the two tiers that share the pod
// heap plus the reserve must never exceed the detected limit.
func TestResolveBudget_ScalingReserve(t *testing.T) {
	const MiB = 1 << 20
	cases := []struct {
		name  string
		limit uint64
	}{
		{"256MiB", 256 * MiB},
		{"512MiB", 512 * MiB},
		{"1GiB", 1024 * MiB},
		{"4GiB", 4096 * MiB},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withMemoryLimit(t, tc.limit, true)
			b := (&CacheGlobalConfig{}).ResolveBudget()

			if b.L1MaxBytes == 0 || b.L2MaxBytesPerNode == 0 {
				t.Fatalf("auto budget must be bounded, got L1=%d L2=%d", b.L1MaxBytes, b.L2MaxBytesPerNode)
			}
			// L2 gets the larger share (replicas live there too).
			if b.L2MaxBytesPerNode <= b.L1MaxBytes {
				t.Errorf("expected L2 (%d) > L1 (%d)", b.L2MaxBytesPerNode, b.L1MaxBytes)
			}
			// Coherence: both tiers + a runtime reserve must fit under the
			// limit. The reserve here mirrors the resolver's floor/fraction.
			reserve := uint64(float64(tc.limit) * defaultReserveFraction)
			if reserve < reserveFloorBytes {
				reserve = reserveFloorBytes
			}
			if sum := b.L1MaxBytes + b.L2MaxBytesPerNode + reserve; sum > tc.limit {
				t.Errorf("L1+L2+reserve (%d) exceeds limit (%d)", sum, tc.limit)
			}
		})
	}
}

// TestResolveBudget_Fallback: with no detectable limit, each tier falls back
// to the fixed 128 MiB cap (never unlimited).
func TestResolveBudget_Fallback(t *testing.T) {
	withMemoryLimit(t, 0, false)
	b := (&CacheGlobalConfig{}).ResolveBudget()
	if b.L1MaxBytes != fallbackTierBytes || b.L2MaxBytesPerNode != fallbackTierBytes {
		t.Fatalf("fallback should be %d per tier, got L1=%d L2=%d", fallbackTierBytes, b.L1MaxBytes, b.L2MaxBytesPerNode)
	}
}

// TestResolveBudget_ExplicitOverrides: an explicit byte value wins over the
// auto-derivation, and an explicit 0 means "no limit".
func TestResolveBudget_ExplicitOverrides(t *testing.T) {
	withMemoryLimit(t, 1<<30, true) // 1 GiB detected, should be ignored where overridden

	t.Run("explicit positive wins", func(t *testing.T) {
		cfg := &CacheGlobalConfig{Memory: CacheMemoryConfig{
			MaxBytes:                   int64p(10 << 20),
			DistributedMaxBytesPerNode: int64p(20 << 20),
			MaxItems:                   int64p(1000),
		}}
		b := cfg.ResolveBudget()
		if b.L1MaxBytes != 10<<20 || b.L2MaxBytesPerNode != 20<<20 || b.L1MaxItems != 1000 {
			t.Fatalf("explicit values not honored: %+v", b)
		}
	})

	t.Run("explicit zero means unlimited", func(t *testing.T) {
		cfg := &CacheGlobalConfig{Memory: CacheMemoryConfig{
			MaxBytes:                   int64p(0),
			DistributedMaxBytesPerNode: int64p(0),
		}}
		b := cfg.ResolveBudget()
		if b.L1MaxBytes != 0 || b.L2MaxBytesPerNode != 0 {
			t.Fatalf("explicit 0 should be unlimited, got L1=%d L2=%d", b.L1MaxBytes, b.L2MaxBytesPerNode)
		}
	})
}

// TestResolveBudget_MisconfiguredReserveFractionFloored: a reserveFraction so
// large that the budget would compute to ~0 must NOT resolve to 0 (unlimited)
// — it floors to minAutoTierBytes so the OOM guard still holds.
func TestResolveBudget_MisconfiguredReserveFractionFloored(t *testing.T) {
	withMemoryLimit(t, 200*(1<<20), true) // 200 MiB
	cfg := &CacheGlobalConfig{Memory: CacheMemoryConfig{ReserveFraction: float64p(0.89)}}
	b := cfg.ResolveBudget()
	if b.L1MaxBytes < minAutoTierBytes || b.L2MaxBytesPerNode < minAutoTierBytes {
		t.Fatalf("tiers must floor to %d, got L1=%d L2=%d", minAutoTierBytes, b.L1MaxBytes, b.L2MaxBytesPerNode)
	}
}

// TestPerCache_DividesAcrossInstances: the total budget is split across the
// caches that share the heap, and unlimited (0) stays unlimited.
func TestPerCache_DividesAcrossInstances(t *testing.T) {
	total := CacheBudget{L1MaxBytes: 800, L1MaxItems: 80, L2MaxBytesPerNode: 1200}
	got := total.PerCache(4)
	if got.L1MaxBytes != 200 || got.L1MaxItems != 20 || got.L2MaxBytesPerNode != 300 {
		t.Fatalf("unexpected per-cache split: %+v", got)
	}

	unlimited := CacheBudget{}.PerCache(8)
	if unlimited.L1MaxBytes != 0 || unlimited.L2MaxBytesPerNode != 0 {
		t.Fatalf("unlimited must stay unlimited, got %+v", unlimited)
	}

	if n0 := (CacheBudget{L1MaxBytes: 100}).PerCache(0); n0.L1MaxBytes != 100 {
		t.Fatalf("n<1 should be treated as 1, got %d", n0.L1MaxBytes)
	}
}

// TestCountResponseCaches tracks the proxy construction in New().
func TestCountResponseCaches(t *testing.T) {
	if n := countResponseCaches(&Config{EnableEvm: false}); n != 4 {
		t.Errorf("non-evm should build 4 caches, got %d", n)
	}
	if n := countResponseCaches(&Config{EnableEvm: true}); n != 8 {
		t.Errorf("evm should build 8 caches, got %d", n)
	}
}
