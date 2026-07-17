package cosmoguard

import (
	"github.com/olric-data/olric"

	"github.com/voluzi/cosmoguard/pkg/cache"
)

// newResponseCache builds the response cache for a proxy / handler:
// an olric L2 fronted by an in-process L1, namespaced by
// cacheCfg.Key+name so multiple cosmoguard fleets sharing one olric
// don't cross-contaminate. Falls back to a per-pod MemoryCache when
// olricClient is nil (test paths without a cluster runtime).
func newResponseCache[K comparable, V any](
	cacheCfg *CacheGlobalConfig,
	olricClient *olric.EmbeddedClient,
	name string,
	budget CacheBudget,
	opts ...cache.Option,
) (cache.Cache[K, V], error) {
	// Apply the per-instance L1 byte/item caps resolved at startup so the
	// in-process cache can't grow unbounded and OOM the pod (issue #15).
	// L2 (olric) eviction is configured separately on the daemon.
	if budget.L1MaxBytes > 0 || budget.L1MaxItems > 0 {
		opts = append(opts,
			cache.MaxCost(budget.L1MaxBytes),
			cache.MaxItems(budget.L1MaxItems),
			cache.OnEvict(func() { recordCacheEviction(name) }),
		)
	}

	if olricClient == nil {
		return cache.NewMemoryCache[K, V](name, opts...)
	}

	namespace := name
	if cacheCfg != nil {
		namespace = cacheCfg.Key + name
	}

	l2, err := cache.NewOlricCache[K, V](olricClient, namespace, opts...)
	if err != nil {
		return nil, err
	}
	l1, err := cache.NewMemoryCache[K, V](name, opts...)
	if err != nil {
		// Degrade to L2-only rather than failing the proxy.
		return l2, nil
	}
	return cache.NewTieredCache[K, V](l1, l2)
}
