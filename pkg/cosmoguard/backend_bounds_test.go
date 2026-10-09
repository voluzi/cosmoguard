package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

type stalledDMap struct {
	olric.DMap
	release                    <-chan struct{}
	stage                      string
	gets, puts, locks, unlocks atomic.Int32
}

func (d *stalledDMap) Get(context.Context, string) (*olric.GetResponse, error) {
	d.gets.Add(1)
	if d.stage == "get" || d.stage == "all" {
		<-d.release
	}
	return nil, olric.ErrKeyNotFound
}
func (d *stalledDMap) Put(context.Context, string, any, ...olric.PutOption) error {
	d.puts.Add(1)
	if d.stage == "put" || d.stage == "all" {
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

// Proxy tests inject through the existing cache interface. The real olric
// adapter's cancellation and admission paths are tested in pkg/cache.
type stalledResponseCache[V any] struct {
	cache.Cache[string, V]
	dm   *stalledDMap
	gate *boundedcall.Gate
}

func newStalledResponseCache[V any](dm *stalledDMap) *stalledResponseCache[V] {
	return &stalledResponseCache[V]{dm: dm, gate: boundedcall.New(l2OperationCapacity, l2OperationBudget, func(outcome string) { recordBackendOperationFailure("l2", outcome) })}
}
func (c *stalledResponseCache[V]) Get(ctx context.Context, key string) (V, error) {
	var zero V
	_, err := boundedcall.Do(ctx, c.gate, func(ctx context.Context) (*olric.GetResponse, error) { return c.dm.Get(ctx, key) })
	if errors.Is(err, olric.ErrKeyNotFound) {
		err = cache.ErrNotFound
	}
	return zero, err
}
func (c *stalledResponseCache[V]) GetWithExpiry(ctx context.Context, key string) (V, int64, error) {
	v, err := c.Get(ctx, key)
	return v, 0, err
}
func (c *stalledResponseCache[V]) Set(ctx context.Context, key string, v V, ttl time.Duration) error {
	_, err := boundedcall.Do(ctx, c.gate, func(ctx context.Context) (struct{}, error) { return struct{}{}, c.dm.Put(ctx, key, v, olric.EX(ttl)) })
	return err
}
func (c *stalledResponseCache[V]) Close() error { return nil }

func boundedTestRelease(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return release, unblock
}

func TestHTTPL2TimeoutFallsBackAndPreservesL1(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	dm := &stalledDMap{release: release, stage: "all"}
	l2 := newStalledResponseCache[CachedResponse](dm)
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
	rec := serveBoundedTestRequest(t, p, httptest.NewRequest(http.MethodGet, "/cached", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "healthy upstream", rec.Body.String())
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, int32(2), dm.gets.Load())
	require.Equal(t, int32(1), forwarded.Load())
	req := httptest.NewRequest(http.MethodGet, "/cached", nil)
	hash, err := p.getRequestHash(req, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, err := l1.Get(t.Context(), hash); return err == nil }, 5*time.Second, time.Millisecond)
	rec = serveBoundedTestRequest(t, p, req)
	require.Equal(t, "healthy upstream", rec.Body.String())
	require.Equal(t, int32(2), dm.gets.Load(), "L1 must bypass the stalled backend")
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "timeout")) == before+3
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), forwarded.Load())
	unblock()
}

func TestLimiterWholeAttemptTimeoutUsesLocalFallback(t *testing.T) {
	for _, stage := range []string{"lock", "get", "put", "unlock"} {
		t.Run(stage, func(t *testing.T) {
			release, unblock := boundedTestRelease(t)
			dm := &stalledDMap{release: release, stage: stage}
			cfg := RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1, FailureMode: "fail-closed"}
			local, err := NewRateLimiter(cfg, nil, "fallback")
			require.NoError(t, err)
			limiter := &boundedRateLimiter{RateLimiter: &olricRateLimiter{dm: dm, locks: dm, rate: cfg.Rate.PerSecond, burst: 1, refillExp: time.Minute}, local: local, operationGate: boundedcall.New(limiterOperationCapacity, limiterOperationBudget, func(outcome string) { recordBackendOperationFailure("limiter", outcome) })}
			var forwarded atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { forwarded.Add(1); _, _ = w.Write([]byte("ok")) }))
			t.Cleanup(up.Close)
			p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: up.URL}})
			rule := &HttpRule{Action: RuleActionAllow, RateLimit: &cfg}
			require.NoError(t, rule.Compile())
			p.SetRules([]*HttpRule{rule}, RuleActionDeny)
			p.limiters[rule.Fingerprint] = limiter
			before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "timeout"))
			done := make(chan int, 2)
			for range 2 {
				go func() {
					rec := httptest.NewRecorder()
					p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
					done <- rec.Code
				}()
			}
			statuses := map[int]int{}
			for range 2 {
				select {
				case code := <-done:
					statuses[code]++
				case <-time.After(5 * time.Second):
					t.Fatal("limiter caller did not stop waiting")
				}
			}
			require.Equal(t, map[int]int{http.StatusOK: 1, http.StatusTooManyRequests: 1}, statuses)
			require.Equal(t, int32(1), forwarded.Load())
			require.Equal(t, before+2, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("limiter", "timeout")))
			unblock()
			require.Eventually(t, func() bool { return dm.unlocks.Load() == 2 }, 5*time.Second, time.Millisecond)
			require.Equal(t, int32(2), dm.locks.Load())
			require.Equal(t, int32(2), dm.gets.Load())
			require.Equal(t, int32(2), dm.puts.Load(), "late writes are not retried")
		})
	}
}

func TestClusterBoundsDoNotAffectLocalBackends(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	cfg := RateLimitConfig{Rate: Rate{PerSecond: 10}, Burst: 2}
	for _, client := range []*olric.EmbeddedClient{nil, cr.Client()} {
		limiter, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{}, client, "local")
		require.NoError(t, err)
		_, bounded := limiter.(*boundedRateLimiter)
		require.True(t, bounded)
		require.Nil(t, limiter.(*boundedRateLimiter).operationGate)
		ok, _, err := limiter.Allow(t.Context(), "key")
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, limiter.Close())
	}
	clustered, err := newRuleRateLimiter(cfg, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "cluster")
	require.NoError(t, err)
	require.IsType(t, &boundedRateLimiter{}, clustered)
	require.NoError(t, clustered.Close())
	authCfg := &AuthConfig{Enable: true, ReplayProtection: &ReplayProtectionConfig{Enable: true}}
	for _, networked := range []bool{false, true} {
		auth, err := newAuthenticator(authCfg, cr.Client(), networked)
		require.NoError(t, err)
		store := auth.Replay().(*olricReplayStore)
		if networked {
			require.NotNil(t, store.operationGate)
		} else {
			require.Nil(t, store.operationGate)
		}
		require.NoError(t, auth.Close())
	}
}

func TestBackendCapacityIsolationAndLocalCacheBypass(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	dm := &stalledDMap{release: release, stage: "get"}
	l2 := newStalledResponseCache[[]byte](dm)
	results := make(chan error, l2OperationCapacity)
	for range l2OperationCapacity {
		go func() { _, err := l2.Get(t.Context(), "key"); results <- err }()
	}
	require.Eventually(t, func() bool { return dm.gets.Load() == l2OperationCapacity }, 5*time.Second, time.Millisecond)
	for range l2OperationCapacity {
		require.ErrorIs(t, <-results, context.DeadlineExceeded)
	}
	before := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "rejected"))
	start := time.Now()
	_, err := l2.Get(t.Context(), "extra")
	require.ErrorIs(t, err, boundedcall.ErrRejected)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, before+1, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("l2", "rejected")))
	require.Equal(t, int32(l2OperationCapacity), dm.gets.Load())
	cr := newEmbeddedClusterRuntimeForTest(t)
	for _, client := range []*olric.EmbeddedClient{nil, cr.Client()} {
		local, err := newResponseCache[string, []byte](&CacheGlobalConfig{}, client, "unbounded-local", CacheBudget{}, nil)
		require.NoError(t, err)
		require.NoError(t, local.Set(t.Context(), "key", []byte("value"), time.Second))
		got, err := local.Get(t.Context(), "key")
		require.NoError(t, err)
		require.Equal(t, []byte("value"), got)
		require.NoError(t, local.Close())
	}
	clusteredCache, err := newResponseCache[string, []byte](&CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "bounded-cluster", CacheBudget{}, nil)
	require.NoError(t, err)
	defer clusteredCache.Close()
	_, err = clusteredCache.Get(t.Context(), "key")
	require.ErrorIs(t, err, cache.ErrNotFound, "independent real cache remains healthy")
	healthy, err := newRuleRateLimiter(RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 1}, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "isolated")
	require.NoError(t, err)
	defer healthy.Close()
	ok, _, err := healthy.Allow(t.Context(), "key")
	require.NoError(t, err, "cache saturation must not consume limiter admission")
	require.True(t, ok)
	unblock()
	require.Eventually(t, func() bool { _, err := l2.Get(t.Context(), "recovered"); return err == cache.ErrNotFound }, time.Second, time.Millisecond)
}

func serveBoundedTestRequest(t *testing.T, p *HttpProxy, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { rec := httptest.NewRecorder(); p.ServeHTTP(rec, r); done <- rec }()
	select {
	case rec := <-done:
		return rec
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish while backend remained blocked")
		return nil
	}
}

type rejectedResponseCache struct {
	cache.Cache[string, CachedResponse]
}

func (c rejectedResponseCache) Get(context.Context, string) (CachedResponse, error) {
	return CachedResponse{}, boundedcall.ErrRejected
}

func TestHTTPBoundedLookupFailureStillCoalesces(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	defer unblock()
	entered := make(chan struct{}, 2)
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte("healthy upstream"))
	})
	p.cache = rejectedResponseCache{p.cache}
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})
	done := make(chan *httptest.ResponseRecorder, 2)
	go func() { done <- doGet(p, rule) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first fetch did not reach upstream")
	}
	enrolled := make(chan struct{})
	ctx := &coalescerWaiterContext{Context: t.Context(), enrolled: enrolled}
	go func() {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/status", nil).WithContext(ctx)
		p.allow(rec, r, rule, time.Now())
		done <- rec
	}()
	select {
	case <-enrolled:
	case <-time.After(5 * time.Second):
		t.Fatal("second request did not enter coalescer")
	}
	unblock()
	for range 2 {
		select {
		case rec := <-done:
			require.Equal(t, "healthy upstream", rec.Body.String())
		case <-time.After(5 * time.Second):
			t.Fatal("coalesced request did not finish")
		}
	}
	require.Equal(t, int32(1), hits.Load())
}

func TestResponseCacheEntryLimits(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered=%t", clustered), func(t *testing.T) {
			var cr *clusterRuntime
			if clustered {
				cr, _ = newTwoNodeClusterForTest(t)
			} else {
				cr = newEmbeddedClusterRuntimeForTest(t)
			}
			cfg := &CacheGlobalConfig{}
			if clustered {
				cfg.Cluster = &ClusterConfig{}
			}
			responses, err := newResponseCache[string, CachedResponse](cfg, cr.Client(), "entry-limits", CacheBudget{}, nil)
			require.NoError(t, err)
			defer responses.Close()
			for _, size := range []int{256 << 10, 257 << 10, 1023 << 10, 1<<20 + 1} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					key := fmt.Sprint(size)
					response := CachedResponse{Data: make([]byte, size)}
					err := responses.Set(t.Context(), key, response, time.Minute)
					if size > 1<<20 {
						require.ErrorIs(t, err, olric.ErrEntryTooLarge)
						got, getErr := responses.Get(t.Context(), key)
						require.NoError(t, getErr)
						require.Equal(t, response.Data, got.Data)
					} else {
						require.NoError(t, err, "both cache modes must preserve the native entry limit")
						got, err := responses.Get(t.Context(), key)
						require.NoError(t, err)
						require.Equal(t, response.Data, got.Data)
					}
				})
			}
		})
	}
}
