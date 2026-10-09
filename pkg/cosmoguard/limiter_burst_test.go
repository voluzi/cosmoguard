package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

type trackedLimiter struct {
	RateLimiter
	active atomic.Int32
}

func (l *trackedLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	l.active.Add(1)
	defer l.active.Add(-1)
	return l.RateLimiter.Allow(ctx, key)
}

// Local fallback must preserve admission for distinct keys even when a loaded
// runner exceeds the shared attempt budget.
func TestClusterLimiterHealthyBurstAdmission(t *testing.T) {
	a, b := newTwoNodeClusterForTest(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 5}, Burst: 5}
	for _, tc := range []struct {
		n        int
		distinct bool
	}{{100, true}, {400, true}, {200, false}, {30, false}} {
		name := fmt.Sprintf("%d/distinct=%t", tc.n, tc.distinct)
		t.Run(name, func(t *testing.T) {
			for _, bounded := range []bool{false, true} {
				keyspace := fmt.Sprintf("burst/%s/%t", name, bounded)
				var limiters []RateLimiter
				var trackers []*trackedLimiter
				for _, cr := range []*clusterRuntime{a, b} {
					raw, err := newOlricRateLimiter(cr.Client(), cfg, keyspace)
					require.NoError(t, err)
					tracker := &trackedLimiter{RateLimiter: raw}
					trackers = append(trackers, tracker)
					var limiter RateLimiter = tracker
					if bounded {
						local, err := NewRateLimiter(cfg, nil, keyspace)
						require.NoError(t, err)
						limiter = &boundedRateLimiter{RateLimiter: tracker, local: local, operationGate: a.limiterOperations}
					}
					limiters = append(limiters, limiter)
				}
				var allowed, denied, timedOut, rejected, other atomic.Int32
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range tc.n {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						key := "shared"
						if tc.distinct {
							key = fmt.Sprint(i)
						}
						ok, _, err := limiters[0].Allow(t.Context(), key)
						switch {
						case errors.Is(err, boundedcall.ErrRejected):
							rejected.Add(1)
						case errors.Is(err, context.DeadlineExceeded):
							timedOut.Add(1)
						case err != nil:
							other.Add(1)
						case ok:
							allowed.Add(1)
						default:
							denied.Add(1)
						}
					}()
				}
				begin := time.Now()
				close(start)
				wg.Wait()
				elapsed := time.Since(begin)
				t.Logf("bounded=%t N=%d distinct=%t allowed=%d denied=%d timeout=%d rejected=%d other=%d duration=%s", bounded, tc.n, tc.distinct, allowed.Load(), denied.Load(), timedOut.Load(), rejected.Load(), other.Load(), elapsed)
				require.Eventually(t, func() bool { return trackers[0].active.Load()+trackers[1].active.Load() == 0 }, 5*time.Second, time.Millisecond)
				if bounded {
					require.Zero(t, rejected.Load())
					require.Zero(t, timedOut.Load())
					require.Zero(t, other.Load())
					if tc.distinct {
						require.Equal(t, int32(tc.n), allowed.Load())
					} else {
						// Native timestamps are rounded to milliseconds; include that
						// rounding in the refill ceiling rather than another run's count.
						nativeCeiling := int32(cfg.Burst) + int32(math.Floor(cfg.Rate.PerSecond*(elapsed+time.Millisecond).Seconds()))
						require.LessOrEqual(t, allowed.Load(), nativeCeiling+int32(cfg.Burst), "same-key admission is bounded by native ceiling plus one local burst")
					}
				}
			}
		})
	}
	t.Run("L2 healthy burst", func(t *testing.T) {
		responseCache, err := newResponseCache[string, []byte](&CacheGlobalConfig{Cluster: &ClusterConfig{}}, a.Client(), "healthy-burst", CacheBudget{}, a.ResponseOperations())
		require.NoError(t, err)
		defer responseCache.Close()
		var rejected, other atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 400 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := responseCache.Get(t.Context(), fmt.Sprint(i))
				switch {
				case errors.Is(err, boundedcall.ErrRejected):
					rejected.Add(1)
				case errors.Is(err, cache.ErrNotFound), errors.Is(err, context.DeadlineExceeded):
				default:
					other.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		t.Logf("L2 capacity rejections: %d", rejected.Load())
		require.Zero(t, other.Load())
	})
}

func TestClusterLimiterContentionDeniesWithinBudget(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	limiter, err := newRuleRateLimiter(RateLimitConfig{Rate: Rate{PerSecond: 5}, Burst: 5}, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "contention")
	require.NoError(t, err)
	raw := limiter.(*boundedRateLimiter).RateLimiter.(*olricRateLimiter)
	lock, err := raw.locks.LockWithTimeout(t.Context(), "contention:key", 2*time.Second, time.Second)
	require.NoError(t, err)
	defer lock.Unlock(context.Background())
	allowed, retry, err := limiter.Allow(t.Context(), "key")
	require.NoError(t, err, "ordinary lock contention must not enter the backend failure policy")
	require.False(t, allowed)
	require.Zero(t, retry)
	require.NoError(t, lock.Unlock(t.Context()))
	allowed, _, err = limiter.Allow(t.Context(), "key")
	require.NoError(t, err)
	require.True(t, allowed)
}
