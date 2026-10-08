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

func TestWeightedGateRetainsCompletedResultUntilHandoff(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(map[bool]string{false: "deliver", true: "cancel"}[abandon], func(t *testing.T) {
			g := New(1, time.Second, nil)
			b := bytebudget.New(1 << 20)
			lease, ok := b.TryAcquire(1 << 20)
			if !ok {
				t.Fatal("initial reservation")
			}
			g.slots <- struct{}{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			returned := make(chan struct{})
			done := make(chan callResult[[]byte])
			workerDone := make(chan struct{})
			go runWorker(ctx, g.slots, lease, func(context.Context, *bytebudget.Lease) ([]byte, error) {
				value := make([]byte, 1<<20)
				value[0] = 42
				close(returned)
				return value, nil
			}, done, workerDone, nil)
			<-returned
			// A descheduled receiver has not yet accepted the completed result.
			until := time.NewTimer(20 * time.Millisecond)
			defer until.Stop()
			held := true
			for held {
				if b.Snapshot().Reserved != 1<<20 {
					t.Error("released bytes while result was awaiting delivery")
					break
				}
				select {
				case <-until.C:
					held = false
				case <-time.After(time.Millisecond):
				}
			}
			_, err := DoWeighted(t.Context(), g, nil, 0, func(context.Context, *bytebudget.Lease) (int, error) {
				t.Error("released slot while result was awaiting delivery")
				return 0, nil
			})
			if !errors.Is(err, ErrRejected) {
				t.Errorf("admitted second worker: %v", err)
			}
			if abandon {
				cancel()
			} else {
				res := <-done
				if res.err != nil || len(res.value) != 1<<20 || res.value[0] != 42 {
					t.Error("lost delivered value", res.err)
				}
			}
			select {
			case <-workerDone:
			case <-time.After(time.Second):
				t.Fatal("worker did not finish handoff")
			}
			if b.Snapshot().Reserved != 0 {
				t.Error("retained result lease after handoff")
			}
			value, err := Do(t.Context(), g, func(context.Context) (int, error) { return 7, nil })
			if err != nil || value != 7 {
				t.Fatal("slot not reusable", err)
			}
		})
	}
}
