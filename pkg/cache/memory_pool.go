package cache

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// sharedEntryOverheadBytes allows for the larger key and interface value beyond
// the legacy entry estimate. This is estimated cost, not an exact heap bound.
const sharedEntryOverheadBytes uint64 = 64

// A distinct immutable owner isolates adapters even with identical namespaces.
type memoryOwner struct {
	namespace string
	onEvict   func()
}

type sharedMemoryKey struct {
	owner  *memoryOwner
	number uint64
	text   string
}

// MemoryPool owns a global L1 LRU and its expiry cleaner. Adapters close only
// their own entries; the runtime must close the pool when all consumers stop.
// Pooled adapters support string and uint64 keys; other keys require legacy caches.
type MemoryPool struct {
	mu          sync.Mutex
	cache       *ttlcache.Cache[sharedMemoryKey, any]
	closed      bool
	startDone   chan struct{}
	closeOnce   sync.Once
	observeOnce sync.Once
}

// NewMemoryPool uses total estimated bytes and entry count. Each zero limit
// independently means unlimited. Pool limits override adapter MaxCost/MaxItems.
func NewMemoryPool(maxBytes, maxItems uint64) *MemoryPool {
	opts := []ttlcache.Option[sharedMemoryKey, any]{ttlcache.WithDisableTouchOnHit[sharedMemoryKey, any]()}
	if maxBytes > 0 {
		opts = append(opts, ttlcache.WithMaxCost[sharedMemoryKey, any](maxBytes, func(item ttlcache.CostItem[sharedMemoryKey, any]) uint64 {
			return entryOverheadBytes + sharedEntryOverheadBytes + costOf(item.Value)
		}))
	}
	if maxItems > 0 {
		opts = append(opts, ttlcache.WithCapacity[sharedMemoryKey, any](maxItems))
	}
	p := &MemoryPool{cache: ttlcache.New[sharedMemoryKey, any](opts...), startDone: make(chan struct{})}

	go func() { defer close(p.startDone); p.cache.Start() }()
	return p
}

func (p *MemoryPool) observeEvictions() {
	p.observeOnce.Do(func() {
		p.cache.OnEviction(func(_ context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[sharedMemoryKey, any]) {
			if reason == ttlcache.EvictionReasonMaxCostExceeded || reason == ttlcache.EvictionReasonCapacityReached {
				if fn := item.Key().owner.onEvict; fn != nil {
					fn()
				}
			}
		})
	})
}

// Close prevents later writes, removes entries and joins the cleaner once.
func (p *MemoryPool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cache.DeleteAll()
		p.mu.Unlock()
		for {
			p.cache.Stop()
			select {
			case <-p.startDone:
				return
			default:
				runtime.Gosched()
			}
		}
	})
	return nil
}

type pooledMemoryCache[K comparable, V any] struct {
	pool   *MemoryPool
	owner  *memoryOwner
	ttl    time.Duration
	closed bool // guarded by pool.mu; reads use only the backing cache
}

func newPooledMemoryCache[K comparable, V any](namespace string, o *Options) (Cache[K, V], error) {
	var key K
	switch any(key).(type) {
	case string, uint64:
		if o.OnEvict != nil {
			o.memoryPool.observeEvictions()
		}
		return &pooledMemoryCache[K, V]{pool: o.memoryPool, owner: &memoryOwner{namespace: namespace, onEvict: o.OnEvict}, ttl: o.TTL}, nil
	default:
		return nil, errMemoryPoolKeyType
	}
}

func (c *pooledMemoryCache[K, V]) key(key K) sharedMemoryKey {
	if text, ok := any(key).(string); ok {
		return sharedMemoryKey{owner: c.owner, text: text}
	}
	return sharedMemoryKey{owner: c.owner, number: any(key).(uint64)}
}

var errMemoryPoolKeyType = errors.New("shared memory pool requires string or uint64 keys")

var errMemoryPoolClosed = errors.New("memory cache pool or adapter closed")

func (c *pooledMemoryCache[K, V]) Set(_ context.Context, key K, value V, ttl time.Duration) error {
	c.pool.mu.Lock()
	defer c.pool.mu.Unlock()
	if c.closed || c.pool.closed {
		return errMemoryPoolClosed
	}
	if ttl == ttlcache.DefaultTTL || (ttl == ttlcache.PreviousOrDefaultTTL && !c.pool.cache.Has(c.key(key))) {
		ttl = c.ttl
	}
	c.pool.cache.Set(c.key(key), value, ttl)
	return nil
}
func (c *pooledMemoryCache[K, V]) Get(_ context.Context, key K) (V, error) {
	if item := c.pool.cache.Get(c.key(key)); item != nil {
		value := item.Value()
		if value == nil {
			var zero V
			return zero, nil
		}
		if value, ok := value.(V); ok {
			return value, nil
		}
	}
	var zero V
	return zero, ErrNotFound
}
func (c *pooledMemoryCache[K, V]) Has(_ context.Context, key K) (bool, error) {
	return c.pool.cache.Has(c.key(key)), nil
}
func (c *pooledMemoryCache[K, V]) resetLocked() {
	// Keys omits expired entries; remove those first to release closed owners.
	c.pool.cache.DeleteExpired()
	for _, key := range c.pool.cache.Keys() {
		if key.owner == c.owner {
			c.pool.cache.Delete(key)
		}
	}
}
func (c *pooledMemoryCache[K, V]) Reset() {
	c.pool.mu.Lock()
	defer c.pool.mu.Unlock()
	if !c.closed && !c.pool.closed {
		c.resetLocked()
	}
}
func (c *pooledMemoryCache[K, V]) Close() error {
	c.pool.mu.Lock()
	defer c.pool.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.resetLocked()
	}
	return nil
}
