package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type sizedValue struct{ n uint64 }

func (s sizedValue) CacheCost() uint64 { return s.n }

func TestCostOf(t *testing.T) {
	assert.Equal(t, uint64(3), costOf([]byte("abc")))
	assert.Equal(t, uint64(42), costOf(sizedValue{n: 42}))
	// Unknown type falls back to a fixed non-zero estimate so it still
	// counts toward the byte budget.
	assert.Equal(t, fallbackCostBytes, costOf(struct{ x int }{x: 1}))
}

// TestMemoryCache_ByteCostEviction: with a byte cap, inserting past the cap
// evicts least-recently-used entries; recent keys survive and the OnEvict
// hook fires for each eviction.
func TestMemoryCache_ByteCostEviction(t *testing.T) {
	var evictions atomic.Int64
	// Cap at 100 bytes; each entry costs 40 → at most 2 fit.
	c, err := NewMemoryCache[string, []byte](
		DefaultNamespace,
		MaxCost(100),
		OnEvict(func() { evictions.Add(1) }),
	)
	assert.NoError(t, err)

	val := make([]byte, 40)
	for _, k := range []string{"a", "b", "c", "d"} {
		assert.NoError(t, c.Set(context.TODO(), k, val, 0))
	}

	// The two most-recently-set keys must still be present.
	_, err = c.Get(context.TODO(), "d")
	assert.NoError(t, err)
	_, err = c.Get(context.TODO(), "c")
	assert.NoError(t, err)

	// The earliest keys were evicted to honor the cap.
	_, err = c.Get(context.TODO(), "a")
	assert.ErrorIs(t, err, ErrNotFound)

	// OnEviction runs on a separate goroutine (ttlcache contract), so poll
	// briefly rather than reading synchronously.
	assert.Eventually(t, func() bool { return evictions.Load() > 0 }, time.Second, 5*time.Millisecond,
		"OnEvict should have fired for capacity evictions")
}

// TestMemoryCache_TTLExpiryNotCountedAsEviction: OnEvict is a capacity-
// pressure signal, so ordinary TTL expiry must NOT fire it — otherwise the
// evictions metric would climb at the insertion rate in steady state and be
// useless for spotting an undersized cap.
func TestMemoryCache_TTLExpiryNotCountedAsEviction(t *testing.T) {
	var evictions atomic.Int64
	// Generous byte cap so nothing is evicted for capacity; short TTL so the
	// single entry expires quickly.
	c, err := NewMemoryCache[string, []byte](
		DefaultNamespace,
		MaxCost(1<<20),
		OnEvict(func() { evictions.Add(1) }),
	)
	assert.NoError(t, err)

	assert.NoError(t, c.Set(context.TODO(), "k", []byte("v"), 20*time.Millisecond))
	// Wait past the TTL and confirm the entry is gone (expired).
	assert.Eventually(t, func() bool {
		_, err := c.Get(context.TODO(), "k")
		return err == ErrNotFound
	}, time.Second, 5*time.Millisecond)

	// Give any (erroneous) async eviction callback time to run, then assert
	// the counter stayed at zero — expiry is not a capacity eviction.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(0), evictions.Load(), "TTL expiry must not count as a capacity eviction")
}

// TestMemoryCache_ItemCapEviction: MaxItems bounds the entry count.
func TestMemoryCache_ItemCapEviction(t *testing.T) {
	c, err := NewMemoryCache[string, []byte](DefaultNamespace, MaxItems(2))
	assert.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		assert.NoError(t, c.Set(context.TODO(), k, []byte("x"), 0))
	}
	_, err = c.Get(context.TODO(), "a")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = c.Get(context.TODO(), "c")
	assert.NoError(t, err)
}

// TestMemoryCache_UnboundedByDefault: without caps, nothing is evicted for
// capacity reasons (preserves pre-issue-#15 behavior).
func TestMemoryCache_UnboundedByDefault(t *testing.T) {
	c, err := NewMemoryCache[string, []byte](DefaultNamespace)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		assert.NoError(t, c.Set(context.TODO(), string(rune(i)), []byte("payload"), 0))
	}
	_, err = c.Get(context.TODO(), string(rune(0)))
	assert.NoError(t, err)
}
