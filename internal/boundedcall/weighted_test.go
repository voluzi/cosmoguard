package boundedcall

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/v6/internal/bytebudget"
)

func TestWeightedGateTimeoutRetainsLease(t *testing.T) {
	g := New(2, 20*time.Millisecond, nil)
	b := bytebudget.New(10)
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	result := make(chan error, 1)
	go func() {
		_, err := DoWeighted(t.Context(), g, b, 10, func(context.Context, *bytebudget.Lease) ([]byte, error) {
			close(entered)
			<-release
			return make([]byte, 1<<20), nil
		})
		result <- err
	}()
	<-entered
	if err := <-result; !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	if b.Snapshot().Reserved != 10 {
		t.Fatal("early release")
	}
	called := false
	_, err := DoWeighted(t.Context(), g, b, 1, func(context.Context, *bytebudget.Lease) (int, error) { called = true; return 1, nil })
	if !errors.Is(err, ErrRejected) || called {
		t.Fatal("admitted abandoned permit")
	}
	unblock()
	deadline := time.Now().Add(time.Second)
	for b.Snapshot().Reserved != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.Snapshot().Reserved != 0 {
		t.Fatal("leak")
	}
}
func TestWeightedGateSlotAndBytesAtomicCleanup(t *testing.T) {
	b := bytebudget.New(10)
	g := New(1, time.Second, nil)
	_, err := DoWeighted(t.Context(), g, b, 11, func(context.Context, *bytebudget.Lease) (int, error) { t.Fatal("entered"); return 0, nil })
	if !errors.Is(err, ErrRejected) {
		t.Fatal(err)
	}
	v, err := DoWeighted(t.Context(), g, b, 10, func(_ context.Context, l *bytebudget.Lease) (int, error) { l.ShrinkTo(1); return 42, nil })
	if err != nil || v != 42 || b.Snapshot().Reserved != 0 {
		t.Fatal(v, err, b.Snapshot())
	}
}
func TestWeightedGatePanicReleases(t *testing.T) {
	b := bytebudget.New(10)
	_, err := DoWeighted(t.Context(), New(1, time.Second, nil), b, 10, func(context.Context, *bytebudget.Lease) (int, error) { panic("test") })
	if err == nil || b.Snapshot().Reserved != 0 {
		t.Fatal(err, b.Snapshot())
	}
}
