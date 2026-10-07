package cosmoguard

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type cancellationIgnoringLimiter struct {
	RateLimiter
	started chan struct{}
	release <-chan struct{}
}

func (l *cancellationIgnoringLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	close(l.started)
	<-l.release
	return false, 0, context.Canceled
}
func TestLimiterCallerCancellationDoesNotUseFallback(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1}
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "during"}[before], func(t *testing.T) {
			limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "cancel")
			require.NoError(t, err)
			bounded := limiter.(*boundedRateLimiter)
			release, unblock := boundedTestRelease(t)
			backend := &cancellationIgnoringLimiter{RateLimiter: bounded.RateLimiter, started: make(chan struct{}), release: release}
			bounded.RateLimiter = backend
			beforeAllowed := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "allowed"))
			beforeDenied := testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "denied"))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if before {
				cancel()
			}
			done := make(chan error, 1)
			go func() { _, _, err := limiter.Allow(ctx, "key"); done <- err }()
			if !before {
				select {
				case <-backend.started:
				case <-time.After(5 * time.Second):
					t.Fatal("attempt did not start")
				}
				cancel()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled caller waited for backend")
			}
			require.Equal(t, beforeAllowed, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "allowed")))
			require.Equal(t, beforeDenied, testutil.ToFloat64(limiterFallbackCounter.WithLabelValues("backend_error", "denied")))
			allowed, _, err := bounded.local.Allow(t.Context(), "key")
			require.NoError(t, err)
			require.True(t, allowed, "cancellation must leave the local burst untouched")
			unblock()
			require.NoError(t, limiter.Close())
		})
	}
}
