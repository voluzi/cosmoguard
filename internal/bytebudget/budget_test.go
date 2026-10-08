package bytebudget

import (
	"math"
	"sync"
	"testing"
)

func TestLeaseAccounting(t *testing.T) {
	b := New(100)
	l, ok := b.TryAcquire(80)
	if !ok {
		t.Fatal("admission")
	}
	if _, ok := b.TryAcquire(21); ok {
		t.Fatal("exceeded cap")
	}
	l.ShrinkTo(40)
	l.ShrinkTo(60)
	if b.Snapshot().Reserved != 40 {
		t.Fatal(b.Snapshot())
	}
	l.Release()
	l.Release()
	if b.Snapshot().Reserved != 0 || b.Snapshot().Rejected != 1 {
		t.Fatal(b.Snapshot())
	}
}
func TestConcurrentReservations(t *testing.T) {
	b := New(100)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 100 {
				l, ok := b.TryAcquire(10)
				if ok {
					if b.Snapshot().Reserved > 100 {
						t.Error("cap")
					}
					l.Release()
				}
			}
		})
	}
	wg.Wait()
	if b.Snapshot().Reserved != 0 {
		t.Fatal("leak")
	}
}
func TestUnlimitedOverflow(t *testing.T) {
	b := New(0)
	l, ok := b.TryAcquire(math.MaxUint64)
	if !ok {
		t.Fatal("unlimited")
	}
	if _, ok := b.TryAcquire(1); ok {
		t.Fatal("overflow")
	}
	l.Release()
}
