package boundedcall

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/bytebudget"
)

func TestOutageGateFailsFastAndRetainsReservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(8, 20*time.Millisecond, nil, unavailable.Store)
		budget := bytebudget.New(80)
		release := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var calls atomic.Int32
		op := func(context.Context, *bytebudget.Lease) (int, error) {
			calls.Add(1)
			<-release
			return 42, nil
		}
		for range 3 {
			_, err := DoWeighted(t.Context(), g, budget, 10, op)
			require.ErrorIs(t, err, ErrTimeout)
		}
		require.True(t, unavailable.Load())
		for range 20 {
			_, err := DoWeighted(t.Context(), g, budget, 10, op)
			require.ErrorIs(t, err, ErrUnavailable)
		}
		require.Equal(t, int32(3), calls.Load())
		require.Equal(t, uint64(30), budget.Snapshot().Reserved)
		time.Sleep(time.Second)
		_, err := DoWeighted(t.Context(), g, budget, 10, op)
		require.ErrorIs(t, err, ErrTimeout)
		time.Sleep(2 * time.Second)
		value, err := DoWeighted(t.Context(), g, budget, 10, func(context.Context, *bytebudget.Lease) (int, error) {
			calls.Add(1)
			require.Equal(t, uint64(50), budget.Snapshot().Reserved, "old workers retain reservations")
			return 7, nil
		})
		require.NoError(t, err, "a resolved probe may be replaced after cooldown even if its worker is stuck")
		require.Equal(t, 7, value)
		require.Equal(t, int32(5), calls.Load())
		require.Equal(t, uint64(40), budget.Snapshot().Reserved)
		require.False(t, unavailable.Load())
		unblock()
		synctest.Wait()
		require.False(t, unavailable.Load(), "obsolete workers cannot change recovered state")
		require.Zero(t, budget.Snapshot().Reserved)
	})
}

func TestOutageGateRequiresConsecutiveTimeouts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewRecovering(8, time.Second, nil, nil)
		timeout := func(context.Context) (int, error) { return 0, ErrTimeout }
		for range 2 {
			_, err := Do(t.Context(), g, timeout)
			require.ErrorIs(t, err, ErrTimeout)
		}
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 42, nil })
		require.NoError(t, err)
		for range 2 {
			_, err := Do(t.Context(), g, timeout)
			require.ErrorIs(t, err, ErrTimeout)
		}
		other := errors.New("backend answered with an error")
		_, err = Do(t.Context(), g, func(context.Context) (int, error) { return 0, other })
		require.ErrorIs(t, err, other)
		for range 3 {
			_, err := Do(t.Context(), g, timeout)
			require.ErrorIs(t, err, ErrTimeout)
		}
		_, err = Do(t.Context(), g, timeout)
		require.ErrorIs(t, err, ErrUnavailable)
	})
}

func TestOutageGateExcludesCallerAndAdmissionFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(1, time.Second, nil, unavailable.Store)
		budget := bytebudget.New(10)
		release, started := make(chan struct{}), make(chan struct{})
		defer close(release)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := DoWeighted(ctx, g, budget, 10, func(context.Context, *bytebudget.Lease) (int, error) {
				close(started)
				<-release
				return 1, nil
			})
			done <- err
		}()
		<-started
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		for range 4 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { t.Error("capacity admitted"); return 0, nil })
			require.ErrorIs(t, err, ErrRejected)
		}
		require.False(t, unavailable.Load())
	})
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(4, time.Second, nil, unavailable.Store)
		budget := bytebudget.New(10)
		for range 4 {
			_, err := DoWeighted(t.Context(), g, budget, 11, func(context.Context, *bytebudget.Lease) (int, error) { t.Error("bytes admitted"); return 0, nil })
			require.ErrorIs(t, err, ErrRejected)
			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			_, err = Do(ctx, g, func(ctx context.Context) (int, error) { <-ctx.Done(); return 0, ctx.Err() })
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotErrorIs(t, err, ErrTimeout)
			cancel()
			synctest.Wait()
		}
		require.False(t, unavailable.Load())
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 7, nil })
		require.NoError(t, err)
	})
}

func TestOutageGateIgnoresPreOutageSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(8, time.Second, nil, unavailable.Store)
		started, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { close(started); <-release; return 1, nil })
			done <- err
		}()
		<-started
		for range 3 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 0, ErrTimeout })
			require.ErrorIs(t, err, ErrTimeout)
		}
		close(release)
		require.NoError(t, <-done)
		require.True(t, unavailable.Load())
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { t.Error("outage admitted"); return 0, nil })
		require.ErrorIs(t, err, ErrUnavailable)
	})
}

func TestOutageGateCloseCannotBeReversedByProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(8, time.Second, nil, unavailable.Store)
		for range 3 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 0, ErrTimeout })
			require.ErrorIs(t, err, ErrTimeout)
		}
		require.True(t, unavailable.Load())
		time.Sleep(time.Second)
		started, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { close(started); <-release; return 1, nil })
			done <- err
		}()
		<-started
		g.Close()
		require.False(t, unavailable.Load())
		close(release)
		require.NoError(t, <-done)
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { t.Error("closed gate admitted"); return 0, nil })
		require.ErrorIs(t, err, ErrRejected)
	})
}

func TestWaitingGateKeepsPerRequestChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewWaiting(8, 20*time.Millisecond, nil)
		calls := 0
		for range 4 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { calls++; return 0, ErrTimeout })
			require.ErrorIs(t, err, ErrTimeout)
		}
		require.Equal(t, 4, calls)
	})
}

func TestOutageGateAdmitsOneConcurrentProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewRecovering(32, time.Second, nil, nil)
		for range 3 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 0, ErrTimeout })
			require.ErrorIs(t, err, ErrTimeout)
		}
		time.Sleep(time.Second)
		release := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var calls atomic.Int32
		results := make(chan error, 20)
		for range 20 {
			go func() {
				_, err := Do(t.Context(), g, func(context.Context) (int, error) { calls.Add(1); <-release; return 42, nil })
				results <- err
			}()
		}
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load())
		for range 19 {
			require.ErrorIs(t, <-results, ErrUnavailable)
		}
		unblock()
		require.NoError(t, <-results)
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 7, nil })
		require.NoError(t, err)
	})
}

func TestOutageReplacementProbeRespectsWorkerCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := NewRecovering(4, 20*time.Millisecond, nil, nil)
		release := make(chan struct{})
		defer close(release)
		var calls atomic.Int32
		work := func(context.Context) (int, error) { calls.Add(1); <-release; return 42, nil }
		for range 3 {
			_, err := Do(t.Context(), g, work)
			require.ErrorIs(t, err, ErrTimeout)
		}
		time.Sleep(time.Second)
		_, err := Do(t.Context(), g, work)
		require.ErrorIs(t, err, ErrTimeout)
		time.Sleep(time.Second)
		_, err = Do(t.Context(), g, work)
		require.ErrorIs(t, err, ErrRejected)
		require.Equal(t, int32(4), calls.Load(), "replacement probes cannot exceed worker capacity")
	})
}

func TestOutageProbeNonTimeoutErrorClosesGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var unavailable atomic.Bool
		g := NewRecovering(8, time.Second, nil, unavailable.Store)
		for range 3 {
			_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 0, ErrTimeout })
			require.ErrorIs(t, err, ErrTimeout)
		}
		require.True(t, unavailable.Load())
		time.Sleep(time.Second)
		backendError := errors.New("write quorum not reached")
		_, err := Do(t.Context(), g, func(context.Context) (int, error) { return 0, backendError })
		require.ErrorIs(t, err, backendError)
		require.False(t, unavailable.Load(), "an on-time backend error proves recovery")
		value, err := Do(t.Context(), g, func(context.Context) (int, error) { return 42, nil })
		require.NoError(t, err)
		require.Equal(t, 42, value)
	})
}
