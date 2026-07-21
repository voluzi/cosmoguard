package cosmoguard

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClassifyFreshness(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	ttl := 10 * time.Second
	stale := 60 * time.Second
	cases := []struct {
		name     string
		storedAt time.Time
		now      time.Time
		ttl      time.Duration
		stale    time.Duration
		want     cacheFreshness
	}{
		{"fresh", base, base.Add(5 * time.Second), ttl, stale, freshEntry},
		{"just-fresh", base, base.Add(ttl - time.Nanosecond), ttl, stale, freshEntry},
		{"stale-start", base, base.Add(ttl), ttl, stale, staleEntry},
		{"stale-mid", base, base.Add(ttl + 30*time.Second), ttl, stale, staleEntry},
		{"expired", base, base.Add(ttl + stale), ttl, stale, expiredEntry},
		{"no-stale-window-past-ttl", base, base.Add(ttl + time.Second), ttl, 0, expiredEntry},
		{"zero-storedAt-is-fresh", time.Time{}, base, ttl, stale, freshEntry},
		{"zero-ttl-is-fresh", base, base.Add(time.Hour), 0, stale, freshEntry},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, classifyFreshness(c.storedAt, c.now, c.ttl, c.stale))
		})
	}
}

func TestResolveCoalesceAndStaleWindow(t *testing.T) {
	tr, fa := true, false
	// coalesce: rule wins, else global, else default-ON.
	require.True(t, resolveCoalesce(nil, nil))                        // default on
	require.True(t, resolveCoalesce(&RuleCache{}, nil))               // rule unset -> default on
	require.False(t, resolveCoalesce(&RuleCache{Coalesce: &fa}, nil)) // rule explicit off
	require.False(t, resolveCoalesce(&RuleCache{}, &fa))              // inherit global off
	require.True(t, resolveCoalesce(&RuleCache{Coalesce: &tr}, &fa))  // rule overrides global
	require.False(t, resolveCoalesce(&RuleCache{Coalesce: &fa}, &tr)) // rule overrides global

	// stale window: rule wins if non-zero, else global.
	require.Equal(t, time.Duration(0), resolveStaleWindow(nil, 0))
	require.Equal(t, 30*time.Second, resolveStaleWindow(&RuleCache{}, 30*time.Second))                                     // inherit global
	require.Equal(t, 5*time.Second, resolveStaleWindow(&RuleCache{StaleWhileRevalidate: 5 * time.Second}, 30*time.Second)) // rule overrides
}

func TestPhysicalTTL(t *testing.T) {
	require.Equal(t, 5*time.Second, physicalTTL(5*time.Second, 0))
	require.Equal(t, 65*time.Second, physicalTTL(5*time.Second, 60*time.Second))
	require.Equal(t, time.Duration(0), physicalTTL(0, 60*time.Second)) // no ttl -> unchanged
}

// newCacheTestProxy builds an HttpProxy backed by an in-memory cache and a
// single fake upstream whose hit count is returned. bodyPrefix lets the
// upstream return a distinguishable, deterministic body.
func newCacheTestProxy(t *testing.T, upstreamDelay time.Duration, handler http.HandlerFunc) (*HttpProxy, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if upstreamDelay > 0 {
			time.Sleep(upstreamDelay)
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	target, _ := url.Parse(srv.URL)
	u := &HttpUpstream{Name: "up", Target: target, proxy: httputil.NewSingleHostReverseProxy(target)}
	u.healthy.Store(true)
	pool := newTestHTTPPool("weighted-round-robin", 0, u)

	c, err := newResponseCache[string, CachedResponse](nil, nil, "test", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	p := &HttpProxy{
		log:   log.WithField("test", "cache-coalesce"),
		pool:  pool,
		cache: c,
		now:   time.Now,
	}
	return p, &hits
}

func cacheRule(t *testing.T, c *RuleCache) *HttpRule {
	t.Helper()
	r := &HttpRule{Priority: 1, Action: RuleActionAllow, Paths: []string{"/status"}, Methods: []string{"GET"}, Cache: c}
	require.NoError(t, r.Compile())
	return r
}

func doGet(p *HttpProxy, rule *HttpRule) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	p.allow(rec, req, rule, time.Now())
	return rec
}

// TestHTTPCoalesce_NConcurrentMissesOneUpstreamCall is the headline proof:
// 50 concurrent cold requests for the same key collapse to ONE upstream call.
func TestHTTPCoalesce_NConcurrentMissesOneUpstreamCall(t *testing.T) {
	p, hits := newCacheTestProxy(t, 60*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute}) // coalesce defaults on

	const n = 50
	var wg sync.WaitGroup
	bodies := make([]string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			rec := doGet(p, rule)
			bodies[i] = rec.Body.String()
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(1), hits.Load(), "N concurrent misses must collapse to one upstream call")
	for i := 1; i < n; i++ {
		require.Equal(t, bodies[0], bodies[i], "all coalesced waiters get the same body")
	}
	require.Equal(t, `{"ok":true}`, bodies[0])
}

// TestHTTPCoalesceDisabled_MultipleUpstreamCalls verifies coalesce:false lets
// concurrent misses each hit the upstream.
func TestHTTPCoalesceDisabled_MultipleUpstreamCalls(t *testing.T) {
	off := false
	p, hits := newCacheTestProxy(t, 40*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); doGet(p, rule) }()
	}
	wg.Wait()
	require.Greater(t, hits.Load(), int32(1), "with coalescing off, concurrent misses each hit upstream")
}

// TestHTTPServeStale_ServesStaleAndRefreshesOnce verifies stale-while-
// revalidate: a request in the stale window is served instantly from cache
// (X-Cosmoguard-Cache: stale) while exactly one background refresh runs.
func TestHTTPServeStale_ServesStaleAndRefreshesOnce(t *testing.T) {
	var version atomic.Int32
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"v":%d}`, version.Add(1))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: 5 * time.Second, StaleWhileRevalidate: 60 * time.Second})

	base := time.Unix(2_000_000, 0)
	p.now = func() time.Time { return base }

	// Prime the cache (miss → stores at base with physical TTL 65s).
	rec := doGet(p, rule)
	require.Equal(t, cacheMiss, rec.Header().Get(cacheStateHeader))
	require.Equal(t, `{"v":1}`, rec.Body.String())
	require.Equal(t, int32(1), hits.Load())

	// Advance into the stale window (age 6s, ttl 5s, stale 60s → stale).
	p.now = func() time.Time { return base.Add(6 * time.Second) }

	rec = doGet(p, rule)
	require.Equal(t, cacheStale, rec.Header().Get(cacheStateHeader), "served stale")
	require.Equal(t, `{"v":1}`, rec.Body.String(), "stale response is the previously cached body")

	// Exactly one background refresh fires.
	require.Eventually(t, func() bool { return hits.Load() == 2 }, 2*time.Second, 5*time.Millisecond,
		"stale serve must trigger exactly one background refresh")
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(2), hits.Load(), "no extra refreshes")

	// After the refresh (stored at base+6s), a read at the same time is fresh.
	rec = doGet(p, rule)
	require.Equal(t, cacheHit, rec.Header().Get(cacheStateHeader))
	require.Equal(t, `{"v":2}`, rec.Body.String(), "refresh updated the cached value")
	require.Equal(t, int32(2), hits.Load(), "fresh hit does not reach upstream")
}

type blockingCachedResponseWriter struct {
	header       http.Header
	writeStarted chan struct{}
	releaseWrite <-chan struct{}
	startedOnce  sync.Once
}

func (w *blockingCachedResponseWriter) Header() http.Header { return w.header }
func (*blockingCachedResponseWriter) WriteHeader(int)       {}
func (w *blockingCachedResponseWriter) Write(p []byte) (int, error) {
	w.startedOnce.Do(func() { close(w.writeStarted) })
	<-w.releaseWrite
	return len(p), nil
}

func TestHTTPServeStaleStartsRefreshBeforeClientWriteCompletes(t *testing.T) {
	refreshStarted := make(chan struct{})
	var refreshOnce sync.Once
	p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		refreshOnce.Do(func() { close(refreshStarted) })
		_, _ = w.Write([]byte("fresh"))
	})

	releaseWrite := make(chan struct{})
	w := &blockingCachedResponseWriter{
		header:       make(http.Header),
		writeStarted: make(chan struct{}),
		releaseWrite: releaseWrite,
	}
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	done := make(chan struct{})
	go func() {
		p.serveStale(w, req, CachedResponse{
			StatusCode: http.StatusOK,
			Data:       []byte("stale"),
			StoredAt:   time.Now().Add(-time.Minute),
		}, "key", &RuleCache{Enable: true, TTL: time.Minute}, "rule", time.Now())
		close(done)
	}()

	<-w.writeStarted
	refreshBeforeWriteCompleted := false
	select {
	case <-refreshStarted:
		refreshBeforeWriteCompleted = true
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseWrite)
	<-done
	if !refreshBeforeWriteCompleted {
		select {
		case <-refreshStarted:
		case <-time.After(time.Second):
			t.Fatal("background refresh never started")
		}
		t.Fatal("background refresh started only after the stale body write completed")
	}
}

func TestJSONRPCStaleStartsRefreshBeforeClientWriteCompletes(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(*JsonRpcHandler, http.ResponseWriter, *http.Request, func(http.ResponseWriter, *http.Request), uint64, *JsonRpcRule, *JsonRpcMsg)
	}{
		{
			name: "direct cache hit",
			invoke: func(h *JsonRpcHandler, w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request), _ uint64, _ *JsonRpcRule, request *JsonRpcMsg) {
				h.handleHttpSingle(request, w, r, next, time.Now())
			},
		},
		{
			name: "coalesced stale replay",
			invoke: func(h *JsonRpcHandler, w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request), hash uint64, rule *JsonRpcRule, request *JsonRpcMsg) {
				h.serveSingleMiss(w, r, next, hash, rule.Cache, "rule", request, time.Now())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
				Cache:   &RuleCache{Enable: true, TTL: time.Minute, StaleWhileRevalidate: time.Minute},
			}
			h := newJSONCacheHandler(t, rule)
			base := time.Unix(2_000_000, 0)
			h.now = func() time.Time { return base }
			request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
			hash := request.HashWithRule(rule.Fingerprint)
			require.NoError(t, h.cache.Set(context.Background(), hash, &JsonRpcMsg{
				Version:  "2.0",
				ID:       1,
				Result:   []byte(`"stale"`),
				StoredAt: base.Add(-90 * time.Second),
			}, time.Hour))

			refreshStarted := make(chan struct{})
			var refreshOnce sync.Once
			next := func(w http.ResponseWriter, _ *http.Request) {
				refreshOnce.Do(func() { close(refreshStarted) })
				_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"fresh"}`))
			}
			releaseWrite := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseWrite) }) })
			w := &blockingCachedResponseWriter{
				header:       make(http.Header),
				writeStarted: make(chan struct{}),
				releaseWrite: releaseWrite,
			}
			req, _ := jsonRequestContext()
			done := make(chan struct{})
			go func() {
				tt.invoke(h, w, req, next, hash, rule, request)
				close(done)
			}()

			<-w.writeStarted
			refreshBeforeWriteCompleted := false
			select {
			case <-refreshStarted:
				refreshBeforeWriteCompleted = true
			case <-time.After(250 * time.Millisecond):
			}
			releaseOnce.Do(func() { close(releaseWrite) })
			<-done
			if !refreshBeforeWriteCompleted {
				select {
				case <-refreshStarted:
				case <-time.After(time.Second):
					t.Fatal("background refresh never started")
				}
				t.Fatal("background refresh started only after the stale body write completed")
			}
		})
	}
}

// TestJSONRPCSingleCoalesce_OneUpstreamCall proves the JSON-RPC single-request
// path coalesces concurrent misses for the same method into one upstream call.
func TestJSONRPCSingleCoalesce_OneUpstreamCall(t *testing.T) {
	c, err := newResponseCache[uint64, *JsonRpcMsg](nil, nil, "t", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-single"),
		cache:         c,
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		now:           time.Now,
		defaultAction: RuleActionDeny,
	}
	jrule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute}, // coalesce defaults on
	}
	require.NoError(t, jrule.Compile())
	h.rules = []*JsonRpcRule{jrule}

	var upstreamCalls atomic.Int32
	next := func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		time.Sleep(60 * time.Millisecond)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	const n = 40
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			req := &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "status"}
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			h.handleHttpSingle(req, w, r, next, time.Now())
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), upstreamCalls.Load(), "concurrent JSON-RPC single misses must collapse to one upstream call")
}
