package cosmoguard

import (
	"time"

	"github.com/voluzi/olric"

	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

const l2OperationBudget = 100 * time.Millisecond
const l2OperationCapacity = 128

func newResponseOperations(workBytes uint64) cache.Option {
	return cache.RecoveringOperations(l2OperationCapacity, l2OperationBudget, workBytes,
		func(outcome string) { recordBackendOperationFailure("l2", outcome) },
		func(reason string) { l2WriteSkips.WithLabelValues(reason).Inc() },
		func(unavailable bool) { recordBackendUnavailable("l2", unavailable) })
}

type ownedResponseCache[K comparable, V any] struct {
	cache.Cache[K, V]
	operations cache.Option
}

func (c *ownedResponseCache[K, V]) Close() error {
	c.operations.CloseOperations()
	return c.Cache.Close()
}

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
	operations cache.Option,
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

	ownsOperations := operations == nil
	if ownsOperations {
		operations = newResponseOperations(responseWorkBytes())
	}
	opts = append(opts, operations)
	var response cache.Cache[K, V]
	success := false
	defer func() {
		if ownsOperations && !success {
			operations.CloseOperations()
		}
	}()
	l2, err := cache.NewOlricCache[K, V](olricClient, namespace, opts...)
	if err != nil {
		return nil, err
	}
	l1, err := cache.NewMemoryCache[K, V](name, opts...)
	if err != nil {
		// Degrade to L2-only rather than failing the proxy.
		response = l2
	} else {
		response, err = cache.NewTieredCache[K, V](l1, l2)
		if err != nil {
			return nil, err
		}
	}
	success = true
	if ownsOperations {
		return &ownedResponseCache[K, V]{response, operations}, nil
	}
	return response, nil
}
