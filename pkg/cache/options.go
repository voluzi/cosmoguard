package cache

import (
	"time"

	"github.com/voluzi/cosmoguard/v5/internal/boundedcall"
)

const (
	defaultCacheTTL = 5 * time.Second
)

func defaultOptions() *Options {
	return &Options{
		TTL: defaultCacheTTL,
	}
}

type Options struct {
	TTL           time.Duration
	operationGate *boundedcall.Gate
	// MaxCostBytes caps the in-memory (L1) working set by approximate
	// payload cost in bytes, evicting least-recently-used entries above
	// the cap. 0 means unbounded. Each entry is charged a flat per-entry
	// overhead (entryOverheadBytes) plus its value's own size (costOf) —
	// see memory.go — so a flood of tiny values is bounded by real heap use.
	MaxCostBytes uint64
	// MaxItems caps the L1 entry count, evicting LRU entries above the
	// cap. 0 means unbounded. A secondary guard alongside MaxCostBytes.
	MaxItems uint64
	// OnEvict, when set, is called once per entry evicted due to a capacity
	// or byte-cost limit (NOT for ordinary TTL expiry). Lets the caller
	// surface a capacity-pressure metric without the cache package depending
	// on a metrics library. nil = no-op.
	OnEvict func()
}

type Option func(*Options)

func DefaultTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.TTL = ttl
	}
}

// MaxCost bounds the in-memory cache by approximate payload bytes (LRU
// eviction above the cap). 0 leaves it unbounded.
func MaxCost(bytes uint64) Option {
	return func(o *Options) {
		o.MaxCostBytes = bytes
	}
}

// MaxItems bounds the in-memory cache by entry count (LRU eviction above
// the cap). 0 leaves it unbounded.
func MaxItems(n uint64) Option {
	return func(o *Options) {
		o.MaxItems = n
	}
}

// OnEvict registers a callback invoked once per in-memory entry evicted due
// to a capacity/byte-cost limit (not TTL expiry).
func OnEvict(fn func()) Option {
	return func(o *Options) {
		o.OnEvict = fn
	}
}

// BoundedOperations limits olric caller waiting and outstanding operations.
// Reusing the option shares one admission pool across cache instances.
// Memory caches ignore it; expired operations may still finish in olric.
func BoundedOperations(capacity int, budget time.Duration, onFailure func(string)) Option {
	gate := boundedcall.New(capacity, budget, onFailure)
	return func(o *Options) { o.operationGate = gate }
}
