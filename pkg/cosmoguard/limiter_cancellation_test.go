package cosmoguard

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestLimiterCallerContextErrorsAreNotProtocolDenials(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "deadline"}[deadline], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			} else {
				cancel()
			}
			defer cancel()
			cfg := &RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1}
			var logs bytes.Buffer
			logger := newEntry(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Run("http", func(t *testing.T) {
				var forwarded atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
				defer up.Close()
				p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: up.URL}})
				rule := &HttpRule{Action: RuleActionAllow, RateLimit: cfg}
				require.NoError(t, rule.Compile())
				p.SetRules([]*HttpRule{rule}, RuleActionDeny)
				p.log = logger
				p.cgDashboard = newDashboardObservability()
				p.responseTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_cancelled_http_duration"}, []string{"method", "status", "cache", "action", "rule_id", "upstream"})
				rec := httptest.NewRecorder()
				rec.Code = 0
				p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil).WithContext(ctx))
				require.Zero(t, rec.Code, "a cancelled client gets no response, including no 429")
				require.Empty(t, rec.Body.String())
				require.Zero(t, forwarded.Load(), "cancelled requests must not be forwarded")
				require.Empty(t, p.cgDashboard.denied.Snapshot())
				require.Zero(t, testutil.CollectAndCount(p.responseTimeHist), "cancellation must not record a rate-limit denial")
				require.Empty(t, logs.String(), "cancellation is not logged as a limiter failure or denial")
			})
			logs.Reset()
			t.Run("grpc", func(t *testing.T) {
				grpcRule := &GrpcRule{Action: RuleActionAllow, Methods: []string{"/svc/M"}, RateLimit: cfg}
				require.NoError(t, grpcRule.Compile())
				gp := &GrpcProxy{log: logger, cgDashboard: newDashboardObservability(), rules: []*GrpcRule{grpcRule}}
				_, err := gp.enforcePolicy(ctx, "/svc/M")
				want := codes.Canceled
				if deadline {
					want = codes.DeadlineExceeded
				}
				require.Equal(t, want, status.Code(err))
				require.Empty(t, gp.cgDashboard.denied.Snapshot())
				require.Empty(t, logs.String())
			})
			logs.Reset()
			t.Run("jsonrpc", func(t *testing.T) {
				rpcRule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"m"}, RateLimit: cfg}
				require.NoError(t, rpcRule.Compile())
				h := &JsonRpcHandler{log: logger, cgDashboard: newDashboardObservability()}
				r := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
				ok, _, _ := h.jsonRpcPolicyVerdict(r, &JsonRpcMsg{Method: "m"}, rpcRule, nil)
				require.False(t, ok, "a cancelled call is not forwarded")
				require.Empty(t, h.cgDashboard.denied.Snapshot())
				require.Empty(t, logs.String())
			})
		})
	}
}
