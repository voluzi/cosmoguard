package cosmoguard

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/olric"
)

type outageLimiter struct {
	RateLimiter
	err   error
	calls atomic.Int32
}

func (l *outageLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	l.calls.Add(1)
	return false, time.Second, l.err
}

func TestLimiterOutageUsesLocalDecisionsAndRecoversOnDenial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 4}
		local, err := NewRateLimiter(cfg, nil, "outage")
		require.NoError(t, err)
		defer local.Close()
		backend := &outageLimiter{err: olric.ErrOperationTimeout}
		l := &boundedRateLimiter{RateLimiter: backend, local: local, operationGate: limiterOperations}
		beforeAllowed := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_unavailable", "allowed"))
		beforeDenied := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_unavailable", "denied"))
		beforeState := testutil.ToFloat64(backendUnavailableGates.WithLabelValues("limiter"))
		for range 3 {
			allowed, _, err := l.Allow(t.Context(), "key")
			require.NoError(t, err)
			require.True(t, allowed)
		}
		require.Equal(t, beforeState+1, testutil.ToFloat64(backendUnavailableGates.WithLabelValues("limiter")))
		for i := 0; i < 3; i++ {
			allowed, _, err := l.Allow(t.Context(), "key")
			require.NoError(t, err)
			require.Equal(t, i == 0, allowed)
		}
		require.Equal(t, int32(3), backend.calls.Load())
		require.Equal(t, beforeAllowed+1, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_unavailable", "allowed")))
		require.Equal(t, beforeDenied+2, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_unavailable", "denied")))
		time.Sleep(time.Second)
		backend.err = nil
		allowed, retry, err := l.Allow(t.Context(), "key")
		require.NoError(t, err)
		require.False(t, allowed)
		require.Equal(t, time.Second, retry)
		require.Equal(t, beforeState, testutil.ToFloat64(backendUnavailableGates.WithLabelValues("limiter")))
		require.Equal(t, int32(4), backend.calls.Load(), "clustered denial is a healthy response, not local fallback")
		_, _, err = l.Allow(t.Context(), "other-key")
		require.NoError(t, err)
		require.Equal(t, int32(5), backend.calls.Load())
	})
}

type outageReplayDMap struct {
	olric.DMap
	fail  bool
	calls atomic.Int32
}

func (d *outageReplayDMap) Put(ctx context.Context, _ string, _ any, _ ...olric.PutOption) error {
	d.calls.Add(1)
	if d.fail {
		<-ctx.Done()
		return ctx.Err()
	}
	return olric.ErrKeyFound
}
func TestReplayChecksEveryRequestAfterBackendTimeouts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dm := &outageReplayDMap{fail: true}
		store := &olricReplayStore{dm: dm, operationGate: replayOperations}
		for range 4 {
			seen, err := store.SeenOrStore(t.Context(), "same-token", time.Minute)
			require.False(t, seen)
			require.ErrorIs(t, err, boundedcall.ErrTimeout)
			synctest.Wait()
		}
		require.Equal(t, int32(4), dm.calls.Load())
		dm.fail = false
		seen, err := store.SeenOrStore(t.Context(), "same-token", time.Minute)
		require.NoError(t, err)
		require.True(t, seen, "the recovered backend rejects an existing token immediately")
		require.Equal(t, int32(5), dm.calls.Load())
	})
}
