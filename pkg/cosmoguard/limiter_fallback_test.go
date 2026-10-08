package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
)

func TestLimiterFallbackEnforcesPerReplicaAndRecovers(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	release, unblock := boundedTestRelease(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 3}
	type result struct {
		allowed bool
		err     error
	}
	for replica := range 2 {
		limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), fmt.Sprint(replica))
		require.NoError(t, err)
		limiter.(*boundedRateLimiter).operationGate = boundedcall.New(limiterOperationCapacity, limiterOperationBudget, func(outcome string) { recordBackendOperationFailure("limiter", outcome) })
		dm := &stalledDMap{release: release, stage: "get"}
		limiter.(*boundedRateLimiter).RateLimiter = &olricRateLimiter{dm: dm, locks: dm, rate: cfg.Rate.PerSecond, burst: float64(cfg.Burst), refillExp: time.Minute}
		done := make(chan result, 12)
		for range 12 {
			go func() { allowed, _, err := limiter.Allow(t.Context(), "same-key"); done <- result{allowed, err} }()
		}
		allowed := 0
		for range 12 {
			select {
			case res := <-done:
				require.NoError(t, res.err)
				if res.allowed {
					allowed++
				}
			case <-time.After(5 * time.Second):
				t.Fatal("stalled limiter blocked caller")
			}
		}
		require.Equal(t, 3, allowed, "each replica must enforce its own burst during fallback")
		t.Cleanup(func() {
			unblock()
			require.Eventually(t, func() bool { return dm.unlocks.Load() >= 12 }, 5*time.Second, time.Millisecond)
		})
		if replica == 1 {
			unblock()
			require.Eventually(t, func() bool { return dm.unlocks.Load() >= 12 }, 5*time.Second, time.Millisecond)
			ok, _, err := limiter.Allow(t.Context(), "same-key")
			require.NoError(t, err)
			require.True(t, ok, "recovered shared backend must be used even though local bucket is exhausted")
		}
	}
}

func TestLimiterBackendErrorFallsBackRegardlessOfDeprecatedMode(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	for _, mode := range []string{"", "fail-open", "fail-closed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 2, FailureMode: mode}
			limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "errors/"+mode)
			require.NoError(t, err)
			limiter.(*boundedRateLimiter).RateLimiter = failingRateLimiter{err: errors.New("backend unavailable")}
			beforeAllowed := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "allowed"))
			beforeDenied := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "denied"))
			for i := range 4 {
				allowed, _, err := limiter.Allow(t.Context(), "key")
				require.NoError(t, err)
				require.Equal(t, i < 2, allowed)
			}
			require.Equal(t, beforeAllowed+2, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "allowed")))
			require.Equal(t, beforeDenied+2, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "denied")))
		})
	}
}

// Keep the configured fallback capacity independent of the shared attempt pool.
func TestLimiterFallbackCapacityDecision(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	release, unblock := boundedTestRelease(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1}
	limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "capacity-fallback")
	require.NoError(t, err)
	limiter.(*boundedRateLimiter).operationGate = boundedcall.New(limiterOperationCapacity, limiterOperationBudget, func(outcome string) { recordBackendOperationFailure("limiter", outcome) })
	dm := &stalledDMap{release: release, stage: "get"}
	limiter.(*boundedRateLimiter).RateLimiter = &olricRateLimiter{dm: dm, locks: dm, rate: cfg.Rate.PerSecond, burst: 1, refillExp: time.Minute}
	var wg sync.WaitGroup
	for range limiterOperationCapacity {
		wg.Go(func() { _, _, err := limiter.Allow(context.Background(), "parked"); require.NoError(t, err) })
	}
	require.Eventually(t, func() bool { return dm.gets.Load() == limiterOperationCapacity }, 5*time.Second, time.Millisecond)
	before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "rejected"))
	beforeAllowed := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("capacity", "allowed"))
	beforeDenied := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("capacity", "denied"))
	for i := range 2 {
		ok, _, err := limiter.Allow(t.Context(), "extra")
		require.NoError(t, err)
		require.Equal(t, i == 0, ok)
	}
	require.Equal(t, before+2, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "rejected")))
	require.Equal(t, beforeAllowed+1, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("capacity", "allowed")))
	require.Equal(t, beforeDenied+1, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("capacity", "denied")))
	unblock()
	wg.Wait()
	require.Eventually(t, func() bool { return dm.unlocks.Load() == limiterOperationCapacity }, 5*time.Second, time.Millisecond)
}
