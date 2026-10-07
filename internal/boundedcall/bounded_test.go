package boundedcall

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBoundedCallCapacityAndRecovery(t *testing.T) {
	var active, peak, calls atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var timedOut, rejected atomic.Int32
	gate := New(2, 20*time.Millisecond, func(outcome string) {
		switch outcome {
		case "timeout":
			timedOut.Add(1)
		case "rejected":
			rejected.Add(1)
		default:
			t.Errorf("unbounded outcome %q", outcome)
		}
	})
	op := func(context.Context) (int, error) {
		calls.Add(1)
		n := active.Add(1)
		for p := peak.Load(); n > p; p = peak.Load() {
			if peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer active.Add(-1)
		<-release
		return 42, nil
	}

	for range 2 {
		done := make(chan error, 1)
		go func() { _, err := Do(t.Context(), gate, op); done <- err }()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(5 * time.Second):
			t.Fatal("caller did not stop waiting")
		}
	}
	for range 20 {
		_, err := Do(t.Context(), gate, op)
		require.ErrorIs(t, err, ErrRejected)
	}
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(2), peak.Load())
	require.Equal(t, int32(2), timedOut.Load())
	require.Equal(t, int32(20), rejected.Load())
	unblock()
	require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
	got, err := Do(t.Context(), gate, func(context.Context) (int, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, 7, got)
}

func TestBoundedCallCancellationRetainsCapacity(t *testing.T) {
	gate := New(1, time.Second, nil)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := Do(ctx, gate, func(context.Context) (int, error) { close(started); <-release; return 1, nil })
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("caller did not stop waiting")
	}
	_, err := Do(t.Context(), gate, func(context.Context) (int, error) {
		t.Error("capacity was released before backend return")
		return 0, nil
	})
	require.ErrorIs(t, err, ErrRejected)
}

func TestBoundedCallPanicReleasesCapacity(t *testing.T) {
	gate := New(1, time.Second, nil)
	_, err := Do(t.Context(), gate, func(context.Context) (int, error) { panic("broken backend") })
	require.ErrorContains(t, err, "broken backend")
	got, err := Do(t.Context(), gate, func(context.Context) (int, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, 7, got)
}
