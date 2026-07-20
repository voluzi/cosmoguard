package cosmoguard

import "time"

const maxDuration = time.Duration(1<<63 - 1)

func saturatingDurationAdd(a, b time.Duration) time.Duration {
	if b > 0 && a > maxDuration-b {
		return maxDuration
	}
	return a + b
}

// cacheFreshness classifies a cached entry relative to its logical TTL and the
// stale-while-revalidate window. It is the single policy shared by the HTTP,
// JSON-RPC, and gRPC cache paths so freshness is decided identically.
type cacheFreshness int

const (
	freshEntry   cacheFreshness = iota // within logical TTL — serve as a normal hit
	staleEntry                         // past TTL, within stale window — serve + refresh
	expiredEntry                       // past TTL+stale, or unknown age — treat as a miss
)

// classifyFreshness decides how a cached entry written at storedAt should be
// treated at now, given its logical ttl and the stale window.
//
// A zero storedAt — an entry written before the StoredAt field existed, or one
// primed without it — is treated as FRESH: such entries were stored under a
// physical TTL equal to their logical TTL (no stale window), so the backend
// evicts them exactly at expiry and they are genuinely fresh while present.
// Treating them as expired instead would make every pre-upgrade entry
// revalidate at once on deploy — the very stampede this change prevents.
func classifyFreshness(storedAt, now time.Time, ttl, stale time.Duration) cacheFreshness {
	if storedAt.IsZero() || ttl <= 0 {
		return freshEntry
	}
	age := now.Sub(storedAt)
	switch {
	case age < ttl:
		return freshEntry
	case age < saturatingDurationAdd(ttl, stale):
		return staleEntry
	default:
		return expiredEntry
	}
}

// physicalTTL is the TTL an entry is actually stored under so a stale entry
// physically outlives its logical ttl: ttl+stale. When stale is 0 it collapses
// to ttl — today's behavior, where the backend deletes exactly at the logical
// TTL and classifyFreshness never observes a stale entry.
func physicalTTL(ttl, stale time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	return saturatingDurationAdd(ttl, stale)
}

// resolveCoalesce resolves the effective single-flight setting for a rule: its
// explicit value if set, else the global default, else ON. Mirrors how an
// unset per-rule ttl falls through to the global cache.ttl.
func resolveCoalesce(rule *RuleCache, global *bool) bool {
	if rule != nil && rule.Coalesce != nil {
		return *rule.Coalesce
	}
	return global == nil || *global
}

// resolveStaleWindow resolves the effective stale window: the rule's value if
// non-zero, else the global default (which may itself be 0 = off).
func resolveStaleWindow(rule *RuleCache, global time.Duration) time.Duration {
	if rule != nil && rule.StaleWhileRevalidate != 0 {
		return rule.StaleWhileRevalidate
	}
	return global
}

// cfgCoalesce / cfgStaleWindow / cfgTTL read the cluster-wide cache defaults
// nil-safely (the global config is nil in tests that bypass cosmoguard.New).
func cfgCoalesce(c *CacheGlobalConfig) *bool {
	if c == nil {
		return nil
	}
	return c.Coalesce
}

func cfgStaleWindow(c *CacheGlobalConfig) time.Duration {
	if c == nil {
		return 0
	}
	return c.StaleWhileRevalidate
}

func cfgTTL(c *CacheGlobalConfig) time.Duration {
	if c == nil {
		return 0
	}
	return c.TTL
}

// effectiveTTL is the rule's ttl, or the global default when the rule leaves it
// unset — the same fallback the store uses, and the freshness horizon SWR
// extends.
func effectiveTTL(rule *RuleCache, global *CacheGlobalConfig) time.Duration {
	if rule != nil && rule.TTL > 0 {
		return rule.TTL
	}
	return cfgTTL(global)
}

// nowOrDefault returns now() when set, else time.Now — so struct-literal
// proxies in tests that don't set a clock still work.
func nowOrDefault(now func() time.Time) time.Time {
	if now != nil {
		return now()
	}
	return time.Now()
}
