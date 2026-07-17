package cache

import "time"

const (
	defaultCacheTTL = 5 * time.Second
)

func defaultOptions() *Options {
	return &Options{
		TTL: defaultCacheTTL,
	}
}

type Options struct {
	TTL time.Duration
	// MaxCostBytes caps the in-memory (L1) working set by approximate
	// payload cost in bytes, evicting least-recently-used entries above
	// the cap. 0 means unbounded. The cost of an entry is computed by
	// costOf (see memory.go).
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
