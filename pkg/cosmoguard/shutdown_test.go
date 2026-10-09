package cosmoguard

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDrainShutdownBoundsUncooperativeCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		release := make(chan struct{})
		defer close(release)
		var calls atomic.Int32
		deadlines := make(chan time.Time, 1)
		f := &CosmoGuard{tracingShutdown: func(ctx context.Context) error {
			calls.Add(1)
			deadline, _ := ctx.Deadline()
			deadlines <- deadline
			<-release
			return nil
		}}
		done := make(chan error, 1)
		go func() { done <- f.DrainAndShutdown(t.Context()) }()
		time.Sleep(8 * time.Second)
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		default:
			t.Fatal("cleanup exceeded its absolute phase budget")
		}
		require.Equal(t, start.Add(7*time.Second), <-deadlines)
		require.ErrorIs(t, f.Shutdown(t.Context()), context.DeadlineExceeded)
		require.Equal(t, int32(1), calls.Load(), "late cleanup stays owned and is never duplicated")
	})
}

func TestImmediateShutdownInterruptsDrainHold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		f := &CosmoGuard{tracingShutdown: func(context.Context) error { calls.Add(1); return nil }}
		start := time.Now()
		done := make(chan error, 1)
		go func() { done <- f.DrainAndShutdown(t.Context()) }()
		synctest.Wait()
		time.Sleep(time.Second)
		require.NoError(t, f.Shutdown(t.Context()))
		require.NoError(t, <-done)
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, int32(1), calls.Load())
	})
}

func TestDrainShutdownHonorsShortCallerBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		f := &CosmoGuard{tracingShutdown: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		require.ErrorIs(t, f.DrainAndShutdown(ctx), context.DeadlineExceeded)
		require.LessOrEqual(t, time.Since(start), 2*time.Second)
	})
}

func TestRepeatedShutdownHonorsItsCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &CosmoGuard{tracingShutdown: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
		done := make(chan error, 1)
		go func() { done <- f.DrainAndShutdown(t.Context()) }()
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		start := time.Now()
		require.ErrorIs(t, f.Shutdown(ctx), context.DeadlineExceeded)
		require.LessOrEqual(t, time.Since(start), 10*time.Millisecond)
		require.ErrorIs(t, <-done, context.DeadlineExceeded)
	})
}

func TestShutdownRetainsClusterUntilDependentWriteFinishes(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	var logs rateLimitLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	members, err := cr.Client().Members(t.Context())
	require.NoError(t, err)
	require.Len(t, members, 1)
	address := members[0].Name
	listening := func() bool {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}
	require.True(t, listening())
	synctest.Test(t, func(t *testing.T) {
		release, unblock := boundedTestRelease(t)
		defer unblock()
		dm := &stalledDMap{release: release, stage: "put"}
		rep, err := newObservabilityReplicator(cr.Client(), newDashboardObservability(), newMetricsHistory(10), nil, "shutdown-owner", true)
		require.NoError(t, err)
		rep.dm = dm
		f := &CosmoGuard{cluster: cr, obsReplicator: rep}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, f.Shutdown(ctx), context.DeadlineExceeded)
		synctest.Wait()
		require.Equal(t, int32(1), dm.puts.Load())
		require.True(t, listening(), "Olric stopped while a dependent write was unfinished")
		unblock()
		synctest.Wait()
	})
	require.Eventually(t, func() bool { return !listening() }, 5*time.Second, 10*time.Millisecond,
		"late cleanup did not stop the retained daemon")
	require.Contains(t, logs.String(), "late olric leave failed")
	require.Contains(t, logs.String(), context.DeadlineExceeded.Error())
}
