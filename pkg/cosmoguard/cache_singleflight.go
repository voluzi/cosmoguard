package cosmoguard

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
)

// coalescer deduplicates concurrent work for the same key using single-flight,
// so N concurrent cache misses collapse to one upstream fetch whose result is
// shared with every waiter. It also drives fire-and-forget background refresh
// for stale-while-revalidate, running at most one refresh per key at a time.
//
// Coalescing is per-proxy and therefore per-pod: across a cluster the shared
// olric L2 dedups the stored value and serves subsequent reads cluster-wide, so
// per-pod single-flight already collapses an expiry stampede from N-per-pod to
// ~1-per-pod. A cluster-wide lock is intentionally out of scope.
//
// The zero value is ready to use.
type coalescer[V any] struct {
	g        singleflight.Group
	inflight sync.Map // key(string) -> struct{}; guards background refreshes
}

// do runs fn under single-flight for key: the first caller executes fn and
// concurrent callers for the same key block until it returns and share the
// result. Each caller honors ITS OWN ctx — a waiter whose client disconnects
// returns promptly with ctx.Err() rather than being pinned to a slow leader.
// fn should use a detached context so a leader disconnect doesn't abort the
// shared fetch for the remaining waiters.
func (c *coalescer[V]) do(ctx context.Context, key string, fn func() (V, error)) (V, error) {
	ch := c.g.DoChan(key, func() (any, error) { return fn() })
	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case res := <-ch:
		v, _ := res.Val.(V)
		return v, res.Err
	}
}

// refresh runs fn in the background for key, at most once per key at a time —
// a refresh requested while one is already running for that key is dropped.
// Used by stale-while-revalidate so the stale-serving request never blocks.
// It shares the same single-flight group as do, so a background refresh and a
// concurrent foreground miss for the same key collapse into one fetch.
func (c *coalescer[V]) refresh(key string, fn func() (V, error)) {
	if _, busy := c.inflight.LoadOrStore(key, struct{}{}); busy {
		return
	}
	go func() {
		defer c.inflight.Delete(key)
		_, _, _ = c.g.Do(key, func() (any, error) { return fn() })
	}()
}
