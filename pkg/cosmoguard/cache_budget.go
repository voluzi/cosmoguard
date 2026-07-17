package cosmoguard

const (
	// reserveFloorBytes is the minimum memory held back from the cache
	// budget in auto mode — covers the Go runtime baseline and request
	// buffers on small pods where a percentage would be too little.
	reserveFloorBytes uint64 = 128 << 20 // 128 MiB
	// defaultReserveFraction is the fraction of the detected limit held
	// back when the operator doesn't set cache.memory.reserveFraction.
	defaultReserveFraction = 0.20
	// l1ShareOfBudget / l2ShareOfBudget split the cache budget between the
	// in-process L1 and the olric L2. L2 gets the larger share because it
	// also stores replicas of peers' partitions.
	l1ShareOfBudget = 0.40
	l2ShareOfBudget = 0.60
	// fallbackTierBytes is the per-tier cap used when no cgroup memory
	// limit can be detected (bare metal / unconstrained host).
	fallbackTierBytes uint64 = 128 << 20 // 128 MiB
	// minAutoTierBytes floors each auto-derived tier so a misconfigured
	// reserveFraction (reserve ≈ limit → budget ≈ 0) can never resolve to
	// 0, which would mean "unlimited" and reintroduce the OOM it guards.
	minAutoTierBytes uint64 = 16 << 20 // 16 MiB
)

// CacheBudget is the resolved, byte-level memory budget for the two cache
// tiers. A value of 0 for a byte field means "no limit" (an explicit
// operator opt-out); auto-derived values are always non-zero.
type CacheBudget struct {
	// L1MaxBytes caps the in-process cache by approximate payload bytes.
	L1MaxBytes uint64
	// L1MaxItems optionally caps the in-process entry count (0 = no cap).
	L1MaxItems uint64
	// L2MaxBytesPerNode caps the olric L2's per-node in-use bytes.
	L2MaxBytesPerNode uint64
}

// PerCache divides a total budget across n response-cache instances that
// share the pod heap (one L1 ttlcache + one olric DMap per enabled proxy),
// so their combined footprint stays within the total. A 0 (unlimited) field
// stays 0. n < 1 is treated as 1.
func (b CacheBudget) PerCache(n int) CacheBudget {
	if n < 1 {
		n = 1
	}
	div := func(v uint64) uint64 {
		if v == 0 {
			return 0
		}
		if share := v / uint64(n); share > 0 {
			return share
		}
		return 1
	}
	return CacheBudget{
		L1MaxBytes:        div(b.L1MaxBytes),
		L1MaxItems:        div(b.L1MaxItems),
		L2MaxBytesPerNode: div(b.L2MaxBytesPerNode),
	}
}

// countResponseCaches returns how many response-cache instances New() will
// build, so the shared pod budget can be split evenly. Always-on services
// (grpc, lcd, rpc HTTP + its jsonrpc handler) contribute 4; enabling EVM
// adds its rpc/ws HTTP proxies and jsonrpc handlers (4 more). This must
// track the proxy construction in New().
func countResponseCaches(cfg *Config) int {
	const baseCaches = 4 // grpc, lcd, jsonrpc, rpc
	if cfg.EnableEvm {
		return baseCaches + 4 // evm_jsonrpc, evm_rpc, evm_jsonrpc_ws, evm_rpc_ws
	}
	return baseCaches
}

// ResolveBudget turns the (possibly sparse) CacheMemoryConfig into concrete
// per-tier byte caps. Explicit config wins per tier; otherwise the tier is
// auto-derived from the detected cgroup memory limit via the scaling-reserve
// model documented on CacheMemoryConfig. The memory limit is read through
// memoryLimitProvider, which tests override to exercise this deterministically.
func (c *CacheGlobalConfig) ResolveBudget() CacheBudget {
	m := c.Memory

	var items uint64
	if m.MaxItems != nil && *m.MaxItems > 0 {
		items = uint64(*m.MaxItems)
	}

	autoL1, autoL2 := fallbackTierBytes, fallbackTierBytes
	if limit, ok := memoryLimitProvider(); ok {
		reserveFraction := defaultReserveFraction
		if m.ReserveFraction != nil {
			reserveFraction = *m.ReserveFraction
		}
		reserve := uint64(float64(limit) * reserveFraction)
		if reserve < reserveFloorBytes {
			reserve = reserveFloorBytes
		}
		var budget uint64
		if limit > reserve {
			budget = limit - reserve
		}
		autoL1 = floorBytes(uint64(float64(budget)*l1ShareOfBudget), minAutoTierBytes)
		autoL2 = floorBytes(uint64(float64(budget)*l2ShareOfBudget), minAutoTierBytes)
	}

	return CacheBudget{
		L1MaxBytes:        resolveTierBytes(m.MaxBytes, autoL1),
		L1MaxItems:        items,
		L2MaxBytesPerNode: resolveTierBytes(m.DistributedMaxBytesPerNode, autoL2),
	}
}

// resolveTierBytes applies the per-tier override rule: nil → the auto value;
// an explicit value (including 0 = no limit) → that value clamped to >= 0.
func resolveTierBytes(override *int64, auto uint64) uint64 {
	if override == nil {
		return auto
	}
	if *override <= 0 {
		return 0
	}
	return uint64(*override)
}

func floorBytes(v, floor uint64) uint64 {
	if v < floor {
		return floor
	}
	return v
}
