package boundedcall

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
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
	var observed atomic.Int32
	gate := New(1, time.Second, func(string) { observed.Add(1) })
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
	require.Zero(t, observed.Load(), "caller cancellation is not a backend timeout")
	_, err := Do(t.Context(), gate, func(context.Context) (int, error) {
		t.Error("capacity was released before backend return")
		return 0, nil
	})
	require.ErrorIs(t, err, ErrRejected)
}

func TestBoundedCallPanicReleasesCapacity(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	gate := New(1, time.Second, nil)
	_, err := Do(t.Context(), gate, func(context.Context) (int, error) { panic("broken backend") })
	require.ErrorContains(t, err, "broken backend")
	require.Equal(t, 1, strings.Count(logs.String(), "level=ERROR"))
	require.Contains(t, logs.String(), "TestBoundedCallPanicReleasesCapacity")
	require.Contains(t, logs.String(), "stack=")
	got, err := Do(t.Context(), gate, func(context.Context) (int, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, 7, got)
}

func TestBoundedCallWaitsForCapacityAndRecovers(t *testing.T) {
	var timeouts, rejected atomic.Int32
	gate := NewWaiting(1, 20*time.Millisecond, func(outcome string) {
		if outcome == "timeout" {
			timeouts.Add(1)
		} else {
			rejected.Add(1)
		}
	})
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := Do(ctx, gate, func(context.Context) (int, error) { close(started); <-release; return 1, nil })
		done <- err
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	_, err := Do(t.Context(), gate, func(context.Context) (int, error) { t.Error("a waiting caller must not start a worker"); return 0, nil })
	require.ErrorIs(t, err, ErrTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), timeouts.Load())
	require.Zero(t, rejected.Load())
	unblock()
	got, err := Do(t.Context(), gate, func(context.Context) (int, error) { return 7, nil })
	require.NoError(t, err)
	require.Equal(t, 7, got)
}

func TestBoundedCallLateAdmissionGetsFullOperationBudget(t *testing.T) {
	gate := NewWaiting(1, time.Second, nil)
	release, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := Do(ctx, gate, func(context.Context) (int, error) { close(started); <-release; return 1, nil })
		done <- err
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	releasedAt := make(chan time.Time, 1)
	timer := time.AfterFunc(10*time.Millisecond, func() { releasedAt <- time.Now(); unblock() })
	defer timer.Stop()
	deadline, err := Do(t.Context(), gate, func(opCtx context.Context) (time.Time, error) { deadline, _ := opCtx.Deadline(); return deadline, nil })
	require.NoError(t, err)
	require.False(t, deadline.Before((<-releasedAt).Add(time.Second)), "operation budget must start after admission")
}

func TestBoundedCallCallerDeadlineIsNotBackendTimeout(t *testing.T) {
	for _, admission := range []bool{false, true} {
		t.Run(map[bool]string{false: "operation", true: "admission"}[admission], func(t *testing.T) {
			var observed atomic.Int32
			gate := NewWaiting(1, time.Second, func(string) { observed.Add(1) })
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			parkedCtx, cancelParked := context.WithCancel(t.Context())
			defer cancelParked()
			parked := make(chan error, 1)
			op := func(context.Context) (int, error) { close(started); <-release; return 1, nil }
			if admission {
				go func() { _, err := Do(parkedCtx, gate, op); parked <- err }()
				<-started
				cancelParked()
				require.ErrorIs(t, <-parked, context.Canceled)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			_, err := Do(ctx, gate, op)
			require.Equal(t, ctx.Err(), err, "caller errors must be returned without a backend wrapper")
			require.NotErrorIs(t, err, ErrTimeout)
			require.Zero(t, observed.Load(), "caller deadlines are not backend timeouts")
		})
	}
}

type cancelOnResultContext struct {
	context.Context
	cancel context.CancelFunc
	ready  atomic.Bool
}

func (c *cancelOnResultContext) Err() error {
	if c.ready.Load() {
		c.cancel()
	}
	return c.Context.Err()
}

func TestBoundedCallCanceledWorkerResultIsDiscarded(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		parent, cancel := context.WithCancel(t.Context())
		ctx := &cancelOnResultContext{Context: parent, cancel: cancel}
		gate := New(1, time.Second, nil)
		if waiting {
			gate = NewWaiting(1, time.Second, nil)
		}
		value, err := Do(ctx, gate, func(context.Context) (int, error) {
			ctx.ready.Store(true)
			return 42, nil
		})
		cancel()
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, value)
	}
}
