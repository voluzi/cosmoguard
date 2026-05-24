package cache

import (
	"context"
	"errors"
	"time"
)

// TieredCache layers an in-process L1 in front of an expiry-aware L2,
// so repeat reads of a key skip the L2 network hop.
//
// L1 entries inherit L2's absolute wall-clock expiry deadline (read
// via GetWithExpiry), so an entry expires at the same instant on
// every pod — no staleness window opened by the L1 indirection.
type TieredCache[K comparable, V any] struct {
	l1 Cache[K, V]
	l2 expiryAwareCache[K, V]
}

// expiryAwareCache is the L2 surface TieredCache needs: a Cache plus
// a Get variant that returns the entry's absolute Unix-ms deadline.
type expiryAwareCache[K comparable, V any] interface {
	Cache[K, V]
	// GetWithExpiry returns the value and the absolute wall-clock
	// expiry deadline in Unix milliseconds (0 when no TTL is set).
	GetWithExpiry(ctx context.Context, key K) (V, int64, error)
}

// NewTieredCache wires an L1 in front of an expiry-aware L2.
// Close cascades to both.
func NewTieredCache[K comparable, V any](
	l1 Cache[K, V],
	l2 expiryAwareCache[K, V],
) (*TieredCache[K, V], error) {
	if l1 == nil || l2 == nil {
		return nil, errors.New("tiered cache: L1 and L2 must both be non-nil")
	}
	return &TieredCache[K, V]{l1: l1, l2: l2}, nil
}

func (c *TieredCache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	// L2 is the source of truth; surface its error.
	if err := c.l2.Set(ctx, key, value, ttl); err != nil {
		return err
	}
	// L1 is best-effort: on failure the next Get repopulates it.
	_ = c.l1.Set(ctx, key, value, ttl)
	return nil
}

func (c *TieredCache[K, V]) Get(ctx context.Context, key K) (V, error) {
	if v, err := c.l1.Get(ctx, key); err == nil {
		return v, nil
	}
	v, expiryMs, err := c.l2.GetWithExpiry(ctx, key)
	if err != nil {
		return v, err
	}
	// Size L1 to L2's remaining wall-clock TTL, not "now + global TTL"
	// (which would extend L1 past L2 by the entry's existing age in L2).
	// Skip the Set when the entry is already past its deadline — pinning
	// it in L1 with ttlcache's default TTL would resurrect expired data.
	if expiryMs > 0 {
		if remaining := time.Until(time.UnixMilli(expiryMs)); remaining > 0 {
			_ = c.l1.Set(ctx, key, v, remaining)
		}
	}
	return v, nil
}

func (c *TieredCache[K, V]) Has(ctx context.Context, key K) (bool, error) {
	if ok, err := c.l1.Has(ctx, key); err == nil && ok {
		return true, nil
	}
	return c.l2.Has(ctx, key)
}

// InvalidateLocal drops every entry from L1 without touching L2.
// Used by the hot-reload path; no-op when the L1 backend doesn't
// expose Reset (so a future stand-in can't break reload).
func (c *TieredCache[K, V]) InvalidateLocal() {
	if r, ok := c.l1.(interface{ Reset() }); ok {
		r.Reset()
	}
}

func (c *TieredCache[K, V]) Close() error {
	// Try both even if L1 errors; otherwise an L1 close failure leaks
	// the L2 client.
	err1 := c.l1.Close()
	err2 := c.l2.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
