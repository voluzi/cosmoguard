package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type namespacedKey[K comparable] struct {
	Namespace string
	Key       K
}

func (k namespacedKey[K]) String() string {
	return fmt.Sprintf("[%s]%v", k.Namespace, k.Key)
}

type MemoryCache[K comparable, V any] struct {
	Namespace string
	cache     *ttlcache.Cache[namespacedKey[K], V]
}

func NewMemoryCache[K comparable, V any](namespace string, opts ...Option) (Cache[K, V], error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	cacheOptions := []ttlcache.Option[namespacedKey[K], V]{
		ttlcache.WithDisableTouchOnHit[namespacedKey[K], V](),
		ttlcache.WithTTL[namespacedKey[K], V](options.TTL),
	}
	if options.MaxCostBytes > 0 {
		cacheOptions = append(cacheOptions,
			ttlcache.WithMaxCost[namespacedKey[K], V](
				options.MaxCostBytes,
				func(item ttlcache.CostItem[namespacedKey[K], V]) uint64 {
					return costOf(item.Value)
				},
			),
		)
	}
	if options.MaxItems > 0 {
		cacheOptions = append(cacheOptions,
			ttlcache.WithCapacity[namespacedKey[K], V](options.MaxItems),
		)
	}
	c := MemoryCache[K, V]{
		Namespace: namespace,
		cache:     ttlcache.New[namespacedKey[K], V](cacheOptions...),
	}
	if options.OnEvict != nil {
		c.cache.OnEviction(func(_ context.Context, reason ttlcache.EvictionReason, _ *ttlcache.Item[namespacedKey[K], V]) {
			// Only capacity/cost evictions signal memory pressure. TTL
			// expiry is normal churn (every entry expires at ~5s) and would
			// otherwise drown the "cap is undersized" signal the caller is
			// after.
			if reason == ttlcache.EvictionReasonMaxCostExceeded || reason == ttlcache.EvictionReasonCapacityReached {
				options.OnEvict()
			}
		})
	}
	go c.cache.Start()
	return c, nil
}

// CacheCoster lets a cached value report its approximate in-memory size
// in bytes, so byte-cost eviction accounts for it accurately. Payload
// types held in the cache (e.g. cached HTTP / JSON-RPC responses)
// implement this; []byte is handled directly by costOf.
type CacheCoster interface {
	CacheCost() uint64
}

// fallbackCostBytes is charged for values that are neither []byte nor a
// CacheCoster. A non-zero estimate keeps the byte budget meaningful for
// unknown types instead of letting them count as free.
const fallbackCostBytes uint64 = 1024

// costOf estimates a cached value's in-memory footprint in bytes for
// byte-cost eviction. []byte is measured directly; anything implementing
// CacheCoster reports its own size; everything else falls back to a
// fixed conservative estimate so unknown types still count toward the
// budget.
func costOf(v any) uint64 {
	switch t := v.(type) {
	case []byte:
		return uint64(len(t))
	case CacheCoster:
		return t.CacheCost()
	default:
		return fallbackCostBytes
	}
}

// Metrics returns the underlying ttlcache metrics (insertions, evictions,
// hits, misses). Used to surface cache_evictions_total so operators can
// tell when a byte/item cap is undersized.
func (c MemoryCache[K, V]) Metrics() ttlcache.Metrics {
	return c.cache.Metrics()
}

func (c MemoryCache[K, V]) key(key K) namespacedKey[K] {
	return namespacedKey[K]{
		Namespace: c.Namespace,
		Key:       key,
	}
}

func (c MemoryCache[K, V]) Set(_ context.Context, key K, value V, ttl time.Duration) error {
	c.cache.Set(c.key(key), value, ttl)
	return nil
}

func (c MemoryCache[K, V]) Get(_ context.Context, key K) (V, error) {
	item := c.cache.Get(c.key(key))
	if item != nil {
		return item.Value(), nil
	}
	var result V
	return result, ErrNotFound
}

func (c MemoryCache[K, V]) Has(_ context.Context, key K) (bool, error) {
	return c.cache.Has(c.key(key)), nil
}

// Close stops the ttlcache background expiration goroutine. Idempotent —
// ttlcache.Cache.Stop() handles repeated calls safely.
func (c MemoryCache[K, V]) Close() error {
	c.cache.Stop()
	return nil
}

// Reset drops every entry without stopping the background expiry
// goroutine. Used by TieredCache.InvalidateLocal on hot-reload so the
// L1 fast path can't serve a value whose underlying rule has changed.
// The TTL goroutine keeps running; the next write resumes normally.
func (c MemoryCache[K, V]) Reset() {
	c.cache.DeleteAll()
}
