package cache

import (
	"time"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/internal/bytebudget"
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
	TTL            time.Duration
	operationGate  *boundedcall.Gate
	operationBytes *bytebudget.Budget
	onSkip         func(string)
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
func BoundedOperations(capacity int, budget time.Duration, maxBytes uint64, onFailure func(string), onSkip func(string)) Option {
	return boundedOperations(boundedcall.New(capacity, budget, onFailure), maxBytes, onSkip)
}

func boundedOperations(gate *boundedcall.Gate, maxBytes uint64, onSkip func(string)) Option {
	bytes := bytebudget.New(maxBytes)
	return func(o *Options) { o.operationGate = gate; o.operationBytes = bytes; o.onSkip = onSkip }
}

// OperationBytes reports reservations for a shared response admission option.
func (opt Option) OperationBytes() (reserved, capacity uint64) {
	if opt == nil {
		return 0, 0
	}
	o := defaultOptions()
	opt(o)
	if o.operationBytes == nil {
		return 0, 0
	}
	s := o.operationBytes.Snapshot()
	return s.Reserved, s.Limit
}

// CloseOperations stops new admission; admitted workers retain their leases.
func (opt Option) CloseOperations() {
	if opt != nil {
		o := defaultOptions()
		opt(o)
		o.operationGate.Close()
	}
}

// RecoveringOperations skips repeated backend waits during an outage.
func RecoveringOperations(capacity int, budget time.Duration, maxBytes uint64, onFailure func(string), onSkip func(string), onUnavailable func(bool)) Option {
	return boundedOperations(boundedcall.NewRecovering(capacity, budget, onFailure, onUnavailable), maxBytes, onSkip)
}
