package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

func pooled[K comparable, V any](t *testing.T, p *MemoryPool, namespace string, opts ...Option) Cache[K, V] {
	t.Helper()
	opts = append(opts, WithMemoryPool(p))
	c, err := NewMemoryCache[K, V](namespace, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func TestMemoryPoolGlobalLRUAndIsolation(t *testing.T) {
	p := NewMemoryPool(0, 3)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	var evicted [2]atomic.Int64
	a := pooled[string, []byte](t, p, "same", OnEvict(func() { evicted[0].Add(1) }))
	b := pooled[string, int](t, p, "same", OnEvict(func() { evicted[1].Add(1) }))
	c := pooled[uint64, string](t, p, "third")
	require.NoError(t, a.Set(t.Context(), "key", []byte("a"), time.Hour))
	require.NoError(t, b.Set(t.Context(), "key", 2, time.Hour))
	require.NoError(t, c.Set(t.Context(), 1, "c", time.Hour))
	v, err := a.Get(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("a"), v)
	require.NoError(t, a.Set(t.Context(), "new", []byte("new"), time.Hour))
	_, err = b.Get(t.Context(), "key")
	require.ErrorIs(t, err, ErrNotFound)
	require.Eventually(t, func() bool { return evicted[1].Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, evicted[0].Load())
	require.NoError(t, a.Close())
	v2, err := c.Get(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, "c", v2)
	require.Error(t, a.Set(t.Context(), "closed", nil, time.Hour))
	require.NoError(t, p.Close())
	require.Error(t, c.Set(t.Context(), 2, "closed", time.Hour))
}

func TestMemoryPoolCostAndLifecycle(t *testing.T) {
	p := NewMemoryPool(4096, 0)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	caches := make([]Cache[string, []byte], 8)
	for i := range caches {
		caches[i] = pooled[string, []byte](t, p, fmt.Sprint(i), MaxCost(1), MaxItems(1))
	}
	// Pooled limits override legacy limits; one owner can use the whole total.
	for i := range 8 {
		require.NoError(t, caches[0].Set(t.Context(), fmt.Sprint(i), make([]byte, 128), time.Hour))
	}
	for i := range 8 {
		_, err := caches[0].Get(t.Context(), fmt.Sprint(i))
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	for n, c := range caches {
		wg.Go(func() {
			for i := range 50 {
				require.NoError(t, c.Set(t.Context(), fmt.Sprint(i%10), make([]byte, n*32+i%2*256), time.Hour))
			}
		})
	}
	wg.Wait()
	var charge uint64
	for _, key := range p.cache.Keys() {
		v := p.cache.Get(key).Value()
		charge += 264 + uint64(len(v.([]byte)))
	}
	require.LessOrEqual(t, charge, uint64(4096))
	require.NoError(t, caches[0].Set(t.Context(), "oversized", make([]byte, 5000), time.Hour))
	_, err := caches[0].Get(t.Context(), "oversized")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, caches[1].Set(t.Context(), "live", nil, time.Hour))
	caches[0].(interface{ Reset() }).Reset()
	_, err = caches[1].Get(t.Context(), "live")
	require.NoError(t, err)
	require.NoError(t, p.Close())
	require.Empty(t, p.cache.Keys())
}

func TestMemoryPoolTTLAndEarlyClose(t *testing.T) {
	for range 20 {
		p := NewMemoryPool(0, 0)
		require.NoError(t, p.Close())
		require.NoError(t, p.Close())
	}
	p := NewMemoryPool(0, 0)
	defer p.Close()
	a := pooled[string, []byte](t, p, "expiring", DefaultTTL(500*time.Millisecond))
	b := pooled[uint64, []byte](t, p, "no-expiry", DefaultTTL(ttlcache.NoTTL))
	require.NoError(t, a.Set(t.Context(), "key", nil, ttlcache.DefaultTTL))
	require.NoError(t, b.Set(t.Context(), 1, nil, ttlcache.DefaultTTL))
	time.Sleep(10 * time.Millisecond)
	_, err := a.Get(t.Context(), "key")
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	_, err = a.Get(t.Context(), "key")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.Get(t.Context(), 1)
	require.NoError(t, err)
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
}

func TestMemoryPoolNilAndTinyValues(t *testing.T) {
	p := NewMemoryPool(264*3, 0)
	defer p.Close()
	c := pooled[string, any](t, p, "nil")
	require.NoError(t, c.Set(t.Context(), "nil", nil, time.Hour))
	// Unknown values, including nil interfaces, still pay the fallback charge.
	_, err := c.Get(t.Context(), "nil")
	require.ErrorIs(t, err, ErrNotFound)
	unlimited := NewMemoryPool(0, 0)
	defer unlimited.Close()
	c = pooled[string, any](t, unlimited, "nil")
	require.NoError(t, c.Set(t.Context(), "nil", nil, time.Hour))
	value, err := c.Get(t.Context(), "nil")
	require.NoError(t, err)
	require.Nil(t, value)
	tiny := pooled[uint64, []byte](t, p, "tiny")
	for i := uint64(0); i < 10; i++ {
		require.NoError(t, tiny.Set(t.Context(), i, nil, time.Hour))
	}
	require.Len(t, p.cache.Keys(), 3)
}

func TestMemoryPoolTieredHitSkipsL2(t *testing.T) {
	p := NewMemoryPool(0, 0)
	defer p.Close()
	l1 := pooled[uint64, []byte](t, p, "tiered")
	l2 := newFakeL2[uint64, []byte]()
	c, err := NewTieredCache[uint64, []byte](l1, l2)
	require.NoError(t, err)
	require.NoError(t, c.Set(t.Context(), 1000, []byte("cached"), time.Hour))
	for range 100 {
		value, err := c.Get(t.Context(), 1000)
		require.NoError(t, err)
		require.Equal(t, []byte("cached"), value)
	}
	require.Zero(t, l2.getCalls.Load())
}

func TestMemoryPoolConcurrentResetAndClose(t *testing.T) {
	p := NewMemoryPool(4096, 0)
	defer p.Close()
	a := pooled[string, []byte](t, p, "closing")
	b := pooled[string, []byte](t, p, "sibling")
	require.NoError(t, b.Set(t.Context(), "live", nil, time.Hour))
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				err := a.Set(t.Context(), "key", nil, time.Hour)
				if err != nil {
					require.ErrorIs(t, err, errMemoryPoolClosed)
				}
				_, _ = a.Get(t.Context(), "key")
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			a.(interface{ Reset() }).Reset()
		}
		require.NoError(t, a.Close())
	})
	wg.Wait()
	require.Error(t, a.Set(t.Context(), "late", nil, time.Hour))
	_, err := b.Get(t.Context(), "live")
	require.NoError(t, err)
	require.NoError(t, p.Close())
	require.Error(t, b.Set(t.Context(), "late", nil, time.Hour))
}

func TestMemoryPoolKeyTypesDoNotChangeLegacyCaches(t *testing.T) {
	p := NewMemoryPool(0, 0)
	defer p.Close()
	_, err := NewMemoryCache[struct{ ID int }, []byte]("unsupported", WithMemoryPool(p))
	require.ErrorIs(t, err, errMemoryPoolKeyType)
	c, err := NewMemoryCache[struct{ ID int }, []byte]("legacy")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Set(t.Context(), struct{ ID int }{1}, []byte("legacy"), time.Hour))
	v, err := c.Get(t.Context(), struct{ ID int }{1})
	require.NoError(t, err)
	require.Equal(t, []byte("legacy"), v)
}
