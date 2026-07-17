package cosmoguard

import (
	"testing"

	"github.com/olric-data/olric/config"
)

// TestApplyL2EvictionConfig_ExemptsSecurityDMaps is the security-critical
// regression for issue #15's L2 wiring: response-cache DMaps must inherit LRU
// eviction, but the rate-limit buckets/locks, JWT replay set, and
// observability snapshots must NEVER be evicted (that would reset a rate
// limiter or let a JWT be replayed). They are pinned to NONE via Custom.
func TestApplyL2EvictionConfig_ExemptsSecurityDMaps(t *testing.T) {
	dmaps := &config.DMaps{}
	applyL2EvictionConfig(dmaps, 256<<20, 1)

	// Global default: LRU with the configured cap, so any response-cache
	// DMap (name = cache.Key + proxy) inherits eviction.
	if dmaps.EvictionPolicy != config.LRUEviction {
		t.Fatalf("global eviction policy = %q, want LRU", dmaps.EvictionPolicy)
	}
	if dmaps.MaxInuse != 256<<20 {
		t.Fatalf("global MaxInuse = %d, want %d", dmaps.MaxInuse, 256<<20)
	}

	// Every non-cache DMap must be explicitly exempt.
	for _, name := range evictionExemptDMaps {
		cs, ok := dmaps.Custom[name]
		if !ok {
			t.Fatalf("DMap %q missing from Custom exemptions", name)
		}
		if cs.EvictionPolicy != config.EvictionPolicy("NONE") {
			t.Errorf("DMap %q eviction policy = %q, want NONE", name, cs.EvictionPolicy)
		}
	}
}

// TestApplyL2EvictionConfig_Disabled: a zero cap leaves eviction off entirely
// (unlimited L2), matching the operator opt-out.
func TestApplyL2EvictionConfig_Disabled(t *testing.T) {
	dmaps := &config.DMaps{}
	applyL2EvictionConfig(dmaps, 0, 1)
	if dmaps.EvictionPolicy == config.LRUEviction {
		t.Fatal("zero cap must not enable LRU eviction")
	}
	if len(dmaps.Custom) != 0 {
		t.Fatalf("zero cap must not add Custom entries, got %d", len(dmaps.Custom))
	}
}

// TestApplyL2EvictionConfig_ReplicaFactorDividesCap: since backup writes
// bypass olric's LRU cap, MaxInuse is divided by the replica factor so a
// node's actual (primary + replica) footprint stays within the budget.
func TestApplyL2EvictionConfig_ReplicaFactorDividesCap(t *testing.T) {
	dmaps := &config.DMaps{}
	applyL2EvictionConfig(dmaps, 300<<20, 3)
	if dmaps.MaxInuse != 100<<20 {
		t.Fatalf("MaxInuse with replicaFactor 3 = %d, want %d", dmaps.MaxInuse, 100<<20)
	}
}

// TestEvictionExemptDMaps_CoversKnownNonCacheDMaps guards against a new
// non-cache DMap being added without an eviction exemption. The known
// non-cache DMap names are asserted present.
func TestEvictionExemptDMaps_CoversKnownNonCacheDMaps(t *testing.T) {
	want := map[string]bool{
		rateLimitDMap:      false,
		rateLimitLocksDMap: false,
		replayJTIDMap:      false,
		replicationDMap:    false,
	}
	for _, name := range evictionExemptDMaps {
		if _, known := want[name]; known {
			want[name] = true
		}
	}
	for name, covered := range want {
		if !covered {
			t.Errorf("non-cache DMap %q is not in evictionExemptDMaps", name)
		}
	}

	// Reverse guard: every exempt entry must be a KNOWN non-cache DMap. This
	// catches a response-cache DMap being accidentally added to the exempt
	// list (which would silently disable its eviction and reintroduce OOM).
	for _, name := range evictionExemptDMaps {
		if _, known := want[name]; !known {
			t.Errorf("evictionExemptDMaps contains unexpected DMap %q — a cache DMap must never be exempt from eviction", name)
		}
	}
}
