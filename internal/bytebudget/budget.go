package bytebudget

import (
	"math"
	"sync"
)

type Snapshot struct{ Limit, Reserved, Rejected uint64 }
type Budget struct {
	mu                        sync.Mutex
	limit, reserved, rejected uint64
}
type Lease struct {
	b *Budget
	n uint64
}

func New(limit uint64) *Budget { return &Budget{limit: limit} }
func (b *Budget) TryAcquire(n uint64) (*Lease, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > math.MaxUint64-b.reserved || (b.limit != 0 && (n > b.limit || b.reserved > b.limit-n)) {
		b.rejected++
		return nil, false
	}
	b.reserved += n
	return &Lease{b: b, n: n}, true
}
func (l *Lease) ShrinkTo(n uint64) {
	l.b.mu.Lock()
	defer l.b.mu.Unlock()
	if n < l.n {
		l.b.reserved -= l.n - n
		l.n = n
	}
}
func (l *Lease) Release() {
	if l != nil {
		l.ShrinkTo(0)
	}
}
func (b *Budget) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{b.limit, b.reserved, b.rejected}
}
