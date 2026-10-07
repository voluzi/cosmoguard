package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olric-data/olric"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/voluzi/cosmoguard/v5/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v5/pkg/cache"
)

type stalledDMap struct {
	olric.DMap
	release                    <-chan struct{}
	stage                      string
	gets, puts, locks, unlocks atomic.Int32
}

func (d *stalledDMap) Get(context.Context, string) (*olric.GetResponse, error) {
	d.gets.Add(1)
	if d.stage == "get" {
		<-d.release
	}
	return nil, olric.ErrKeyNotFound
}
func (d *stalledDMap) Put(context.Context, string, any, ...olric.PutOption) error {
	d.puts.Add(1)
	if d.stage == "put" {
		<-d.release
	}
	return nil
}
func (d *stalledDMap) LockWithTimeout(context.Context, string, time.Duration, time.Duration) (olric.LockContext, error) {
	d.locks.Add(1)
	if d.stage == "lock" {
		<-d.release
	}
	return stalledLock{d}, nil
}

type stalledLock struct{ dm *stalledDMap }

func (l stalledLock) Unlock(context.Context) error {
	l.dm.unlocks.Add(1)
	if l.dm.stage == "unlock" {
		<-l.dm.release
	}
	return nil
}
func (stalledLock) Lease(context.Context, time.Duration) error { return nil }

type stalledCacheClient struct{ dm olric.DMap }

func (c stalledCacheClient) NewDMap(string, ...olric.DMapOption) (olric.DMap, error) {
	return c.dm, nil
}

func boundedTestRelease(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	safety := time.AfterFunc(700*time.Millisecond, unblock)
	t.Cleanup(func() { safety.Stop(); unblock() })
	return release, unblock
}

func TestHTTPL2TimeoutFallsBackAndPreservesL1(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	dm := &stalledDMap{release: release, stage: "get"}
	l2, err := cache.NewOlricCache[string, CachedResponse](stalledCacheClient{dm}, "http", boundedL2Operations)
	require.NoError(t, err)
	l1, err := cache.NewMemoryCache[string, CachedResponse]("http")
	require.NoError(t, err)
	tiered, err := cache.NewTieredCache[string, CachedResponse](l1, l2)
	require.NoError(t, err)
	var forwarded atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded.Add(1)
		_, _ = w.Write([]byte("healthy upstream"))
	}))
	t.Cleanup(up.Close)
	p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: up.URL}})
	require.NoError(t, p.cache.Close())
	p.cache = tiered
	rule := &HttpRule{Action: RuleActionAllow, Cache: &RuleCache{Enable: true, TTL: time.Second}}
	require.NoError(t, rule.Compile())
	p.SetRules([]*HttpRule{rule}, RuleActionDeny)
	before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "timeout"))
	start := time.Now()
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cached", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "healthy upstream", rec.Body.String())
	require.Less(t, time.Since(start), 400*time.Millisecond)
	require.Equal(t, int32(1), dm.gets.Load())
	require.Equal(t, int32(1), forwarded.Load())
	require.Equal(t, before+1, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "timeout")))
	req := httptest.NewRequest(http.MethodGet, "/cached", nil)
	hash, err := p.getRequestHash(req, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	require.NoError(t, l1.Set(t.Context(), hash, CachedResponse{StatusCode: http.StatusOK, Data: []byte("L1"), StoredAt: time.Now()}, time.Second))
	rec = httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	require.Equal(t, "L1", rec.Body.String())
	require.Equal(t, int32(1), dm.gets.Load(), "L1 must bypass the stalled backend")
	require.Equal(t, int32(1), forwarded.Load())
	unblock()
}

func TestLimiterWholeAttemptTimeoutUsesFailureMode(t *testing.T) {
	for _, stage := range []string{"lock", "get", "put", "unlock"} {
		for _, mode := range []string{"fail-open", "fail-closed"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				release, unblock := boundedTestRelease(t)
				dm := &stalledDMap{release: release, stage: stage}
				limiter := &boundedRateLimiter{RateLimiter: &olricRateLimiter{dm: dm, locks: dm, rate: 1, burst: 1, refillExp: time.Minute}}
				var forwarded atomic.Int32
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { forwarded.Add(1); _, _ = w.Write([]byte("ok")) }))
				t.Cleanup(up.Close)
				p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: up.URL}})
				rule := &HttpRule{Action: RuleActionAllow, RateLimit: &RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 1, FailureMode: mode}}
				require.NoError(t, rule.Compile())
				p.SetRules([]*HttpRule{rule}, RuleActionDeny)
				p.limiters[rule.Fingerprint] = limiter
				before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "timeout"))
				start := time.Now()
				rec := httptest.NewRecorder()
				p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
				require.Less(t, time.Since(start), 500*time.Millisecond)
				want := http.StatusOK
				if mode == "fail-closed" {
					want = http.StatusTooManyRequests
				}
				require.Equal(t, want, rec.Code)
				require.Equal(t, before+1, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "timeout")))
				if mode == "fail-closed" {
					require.Zero(t, forwarded.Load())
				} else {
					require.Equal(t, int32(1), forwarded.Load())
				}
				unblock()
				require.Eventually(t, func() bool { return dm.unlocks.Load() == 1 }, time.Second, time.Millisecond)
				require.Equal(t, int32(1), dm.locks.Load())
				require.Equal(t, int32(1), dm.gets.Load())
				require.Equal(t, int32(1), dm.puts.Load(), "one attempt; a late write is not retried")
			})
		}
	}
}

func TestClusterBoundsDoNotAffectLocalBackends(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 10}, Burst: 2}
	for _, client := range []*olric.EmbeddedClient{nil, cr.Client()} {
		limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{}, client, "local")
		require.NoError(t, err)
		_, bounded := limiter.(*boundedRateLimiter)
		require.False(t, bounded)
		ok, _, err := limiter.Allow(t.Context(), "key")
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, limiter.Close())
	}
	clustered, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "cluster")
	require.NoError(t, err)
	require.IsType(t, &boundedRateLimiter{}, clustered)
	require.NoError(t, clustered.Close())
}

func TestBackendCapacityIsolationAndLocalCacheBypass(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	dm := &stalledDMap{release: release, stage: "get"}
	l2, err := cache.NewOlricCache[string, []byte](stalledCacheClient{dm}, "saturated", boundedL2Operations)
	require.NoError(t, err)
	results := make(chan error, l2OperationCapacity)
	for range l2OperationCapacity {
		go func() { _, err := l2.Get(t.Context(), "key"); results <- err }()
	}
	require.Eventually(t, func() bool { return dm.gets.Load() == l2OperationCapacity }, 500*time.Millisecond, time.Millisecond)
	for range l2OperationCapacity {
		require.ErrorIs(t, <-results, context.DeadlineExceeded)
	}
	before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "rejected"))
	start := time.Now()
	_, err = l2.Get(t.Context(), "extra")
	require.ErrorIs(t, err, boundedcall.ErrRejected)
	require.Less(t, time.Since(start), 50*time.Millisecond)
	require.Equal(t, before+1, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "rejected")))
	require.Equal(t, int32(l2OperationCapacity), dm.gets.Load())
	cr := newEmbeddedClusterRuntimeForTest(t)
	for _, client := range []*olric.EmbeddedClient{nil, cr.Client()} {
		local, err := newResponseCache[string, []byte](&CacheGlobalConfig{}, client, "unbounded-local", CacheBudget{})
		require.NoError(t, err)
		require.NoError(t, local.Set(t.Context(), "key", []byte("value"), time.Second))
		got, err := local.Get(t.Context(), "key")
		require.NoError(t, err)
		require.Equal(t, []byte("value"), got)
		require.NoError(t, local.Close())
	}
	clusteredCache, err := newResponseCache[string, []byte](&CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "bounded-cluster", CacheBudget{})
	require.NoError(t, err)
	defer clusteredCache.Close()
	_, err = clusteredCache.Get(t.Context(), "key")
	require.ErrorIs(t, err, boundedcall.ErrRejected, "factory must bound clustered L2")
	healthy, err := newRuleRateLimiter(RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 1}, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "isolated")
	require.NoError(t, err)
	defer healthy.Close()
	ok, _, err := healthy.Allow(t.Context(), "key")
	require.NoError(t, err, "cache saturation must not consume limiter admission")
	require.True(t, ok)
	stalled := &stalledDMap{release: release, stage: "get"}
	limiter := &boundedRateLimiter{RateLimiter: &olricRateLimiter{dm: stalled, locks: stalled, rate: 1, burst: 1, refillExp: time.Minute}}
	limiterResults := make(chan error, limiterOperationCapacity)
	for range limiterOperationCapacity {
		go func() { _, _, err := limiter.Allow(t.Context(), "key"); limiterResults <- err }()
	}
	require.Eventually(t, func() bool { return stalled.gets.Load() == limiterOperationCapacity }, 500*time.Millisecond, time.Millisecond)
	for range limiterOperationCapacity {
		require.ErrorIs(t, <-limiterResults, context.DeadlineExceeded)
	}
	before = testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "rejected"))
	start = time.Now()
	_, _, err = limiter.Allow(t.Context(), "extra")
	require.ErrorIs(t, err, boundedcall.ErrRejected)
	require.Less(t, time.Since(start), 50*time.Millisecond)
	require.Equal(t, before+1, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "rejected")))
	require.Equal(t, int32(limiterOperationCapacity), stalled.gets.Load())
	unblock()
	require.Eventually(t, func() bool { _, err := l2.Get(t.Context(), "recovered"); return err == cache.ErrNotFound }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { ok, _, err := limiter.Allow(t.Context(), "recovered"); return err == nil && ok }, time.Second, time.Millisecond)
}
