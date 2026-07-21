package cosmoguard

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cachepkg "github.com/voluzi/cosmoguard/pkg/cache"
)

func TestFreshnessDurationAdditionSaturates(t *testing.T) {
	maxDuration := time.Duration(1<<63 - 1)
	ttl := maxDuration - 10*time.Nanosecond
	stale := 20 * time.Nanosecond
	storedAt := time.Unix(0, 0)

	require.Equal(t, maxDuration, physicalTTL(ttl, stale))
	require.Equal(t, staleEntry, classifyFreshness(storedAt, storedAt.Add(ttl), ttl, stale))
}

func TestCoalescerForegroundMissDoesNotJoinBackgroundRefresh(t *testing.T) {
	var c coalescer[int]
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	t.Cleanup(release)

	c.refresh("key", func() (int, error) {
		close(refreshStarted)
		<-releaseRefresh
		return 1, nil
	})
	<-refreshStarted

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := c.do(ctx, "key", func() (int, error) { return 2, nil })
	require.NoError(t, err)
	require.Equal(t, 2, got)
	release()
}

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func cacheRequest(p *HttpProxy, rule *HttpRule, headers http.Header) (*httptest.ResponseRecorder, *RequestStats) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	ctx, stats := WithRequestStats(req.Context())
	req = req.WithContext(ctx)
	if requestID := req.Header.Get("X-Request-Id"); requestID != "" {
		rec.Header().Set("X-Request-Id", requestID)
	}
	p.allow(rec, req, rule, time.Now())
	return rec, stats
}

func TestHTTPCoalesceDoesNotSharePrivateResponses(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.Header().Set("Cache-Control", "private")
		_, _ = w.Write([]byte(r.Header.Get("Authorization")))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	type result struct {
		rec *httptest.ResponseRecorder
	}
	first := make(chan result, 1)
	second := make(chan result, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, http.Header{"Authorization": {"Bearer alice"}})
		first <- result{rec: rec}
	}()
	<-started
	go func() {
		rec, _ := cacheRequest(p, rule, http.Header{"Authorization": {"Bearer bob"}})
		second <- result{rec: rec}
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.Equal(t, "Bearer alice", (<-first).rec.Body.String())
	require.Equal(t, "Bearer bob", (<-second).rec.Body.String())
	require.Equal(t, int32(2), hits.Load(), "private responses must be fetched per request")
}

func TestHTTPCoalescedWaitersDoNotReceiveLeaderHeaders(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.Header().Set("Set-Cookie", "session="+r.Header.Get("X-User"))
		w.Header().Set("X-Request-Id", r.Header.Get("X-Request-Id"))
		_, _ = w.Write([]byte("shared"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, http.Header{
			"X-Request-Id": {"request-a"},
			"X-User":       {"alice"},
		})
		first <- rec
	}()
	<-started
	go func() {
		rec, _ := cacheRequest(p, rule, http.Header{
			"X-Request-Id": {"request-b"},
			"X-User":       {"bob"},
		})
		second <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	recA := <-first
	recB := <-second
	require.Equal(t, int32(1), hits.Load())
	require.Equal(t, "shared", recA.Body.String())
	require.Equal(t, "shared", recB.Body.String())
	for _, value := range recA.Header().Values("X-Request-Id") {
		require.Equal(t, "request-a", value)
	}
	for _, value := range recB.Header().Values("X-Request-Id") {
		require.Equal(t, "request-b", value)
	}
	require.NotContains(t, recA.Header().Values("Set-Cookie"), "session=bob")
	require.NotContains(t, recB.Header().Values("Set-Cookie"), "session=alice")
}

func TestHTTPCoalescedWaitersKeepUpstreamStats(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		_, _ = w.Write([]byte("ok"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first := make(chan *RequestStats, 1)
	second := make(chan *RequestStats, 1)
	go func() {
		_, stats := cacheRequest(p, rule, nil)
		first <- stats
	}()
	<-started
	go func() {
		_, stats := cacheRequest(p, rule, nil)
		second <- stats
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.Equal(t, "up", (<-first).Upstream)
	require.Equal(t, "up", (<-second).Upstream)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPForegroundMissUsesConfiguredProxyDeadline(t *testing.T) {
	target, err := url.Parse("http://upstream.invalid")
	require.NoError(t, err)
	type deadlineObservation struct {
		remaining time.Duration
		ok        bool
	}
	observedDeadline := make(chan deadlineObservation, 1)
	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	reverseProxy.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		remaining := time.Duration(0)
		if ok {
			remaining = time.Until(deadline)
		}
		observedDeadline <- deadlineObservation{remaining: remaining, ok: ok}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("bounded")),
			Request:    r,
		}, nil
	})
	upstream := &HttpUpstream{Name: "up", Target: target, proxy: reverseProxy}
	upstream.healthy.Store(true)

	cache, err := newResponseCache[string, CachedResponse](nil, nil, "deadline-test", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	p := &HttpProxy{
		log:   log.WithField("test", "deadline"),
		pool:  newTestHTTPPool("weighted-round-robin", 0, upstream),
		cache: cache,
		now:   time.Now,
	}
	configuredTimeout := 45 * time.Minute
	p.cacheConfig = &CacheGlobalConfig{HTTPForegroundFetchTimeout: configuredTimeout}
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	ctx, cancel := context.WithTimeout(req.Context(), time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	p.allow(rec, req, rule, time.Now())

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "bounded", rec.Body.String())
	observed := <-observedDeadline
	require.True(t, observed.ok)
	require.Greater(t, observed.remaining, 44*time.Minute)
	require.LessOrEqual(t, observed.remaining, configuredTimeout)
}

type blockingResponseCache struct {
	setStarted chan struct{}
	releaseSet chan struct{}
	closeOnce  sync.Once
}

type capturingResponseCache struct {
	set chan CachedResponse
}

func (c *capturingResponseCache) Set(_ context.Context, _ string, value CachedResponse, _ time.Duration) error {
	c.set <- value
	return nil
}

func (*capturingResponseCache) Get(context.Context, string) (CachedResponse, error) {
	return CachedResponse{}, cachepkg.ErrNotFound
}

func (*capturingResponseCache) Has(context.Context, string) (bool, error) { return false, nil }
func (*capturingResponseCache) Close() error                              { return nil }

func TestHTTPAsyncPersistenceUsesFetchCompletionTime(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	capturing := &capturingResponseCache{set: make(chan CachedResponse, 1)}
	p.cache = capturing
	fetchedAt := time.Unix(2_000_000, 0)
	advancedAt := fetchedAt.Add(time.Minute)
	var clock atomic.Int64
	clock.Store(fetchedAt.UnixNano())
	p.now = func() time.Time { return time.Unix(0, clock.Load()) }

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	out, err := p.fetchAndStore(req, "key", &RuleCache{Enable: true, TTL: time.Minute}, "rule", &responseOwner{}, true)
	require.NoError(t, err)
	require.Equal(t, "ok", string(out.Body))
	clock.Store(advancedAt.UnixNano())
	runtime.Gosched()

	select {
	case stored := <-capturing.set:
		require.Equal(t, fetchedAt.UTC(), stored.StoredAt)
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not run")
	}
}

type stagedResponseCache struct {
	mu               sync.Mutex
	value            *CachedResponse
	getCalls         atomic.Int32
	secondGetStarted chan struct{}
	releaseSecondGet chan struct{}
	setStarted       chan struct{}
	releaseSet       chan struct{}
	setFinished      chan struct{}
	releaseSetOnce   sync.Once
	releaseGetOnce   sync.Once
}

func newStagedResponseCache() *stagedResponseCache {
	return &stagedResponseCache{
		secondGetStarted: make(chan struct{}),
		releaseSecondGet: make(chan struct{}),
		setStarted:       make(chan struct{}),
		releaseSet:       make(chan struct{}),
		setFinished:      make(chan struct{}),
	}
}

func (c *stagedResponseCache) Set(_ context.Context, _ string, value CachedResponse, _ time.Duration) error {
	close(c.setStarted)
	<-c.releaseSet
	c.mu.Lock()
	copy := value
	c.value = &copy
	c.mu.Unlock()
	close(c.setFinished)
	return nil
}

func (c *stagedResponseCache) Get(context.Context, string) (CachedResponse, error) {
	c.mu.Lock()
	if c.value != nil {
		value := *c.value
		c.mu.Unlock()
		return value, nil
	}
	c.mu.Unlock()
	if c.getCalls.Add(1) == 3 {
		close(c.secondGetStarted)
		<-c.releaseSecondGet
	}
	return CachedResponse{}, cachepkg.ErrNotFound
}

func (c *stagedResponseCache) Has(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	return err == nil, nil
}

func (c *stagedResponseCache) Close() error {
	c.releaseSetOnce.Do(func() { close(c.releaseSet) })
	c.releaseGetOnce.Do(func() { close(c.releaseSecondGet) })
	return nil
}

func newBlockingResponseCache() *blockingResponseCache {
	return &blockingResponseCache{
		setStarted: make(chan struct{}, 1),
		releaseSet: make(chan struct{}),
	}
}

func (c *blockingResponseCache) Set(context.Context, string, CachedResponse, time.Duration) error {
	select {
	case c.setStarted <- struct{}{}:
	default:
	}
	<-c.releaseSet
	return nil
}

func (*blockingResponseCache) Get(context.Context, string) (CachedResponse, error) {
	return CachedResponse{}, cachepkg.ErrNotFound
}

func (*blockingResponseCache) Has(context.Context, string) (bool, error) { return false, nil }

func (c *blockingResponseCache) Close() error {
	c.closeOnce.Do(func() { close(c.releaseSet) })
	return nil
}

func TestHTTPMissResponseDoesNotWaitForCachePersistence(t *testing.T) {
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	blocking := newBlockingResponseCache()
	t.Cleanup(func() { _ = blocking.Close() })
	p.cache = blocking
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		first <- rec
	}()
	select {
	case <-blocking.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}

	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		second <- rec
	}()

	select {
	case rec := <-first:
		require.Equal(t, "ok", rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("leader response waited for cache persistence")
	}
	select {
	case rec := <-second:
		require.Equal(t, "ok", rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("coalesced waiter response waited for cache persistence")
	}
	require.Equal(t, int32(1), hits.Load(), "pending persistence must not open a second upstream miss")
	_ = blocking.Close()
}

func TestHTTPPendingResponseExpiresBeforePersistenceCompletes(t *testing.T) {
	var responses atomic.Int32
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("response-" + strconv.Itoa(int(responses.Add(1)))))
	})
	blocking := newBlockingResponseCache()
	t.Cleanup(func() { _ = blocking.Close() })
	p.cache = blocking
	base := time.Unix(2_000_000, 0)
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	p.now = func() time.Time { return time.Unix(0, clock.Load()) }
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Second})

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		first <- rec
	}()
	select {
	case <-blocking.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}
	require.Equal(t, "response-1", (<-first).Body.String())

	clock.Store(base.Add(2 * time.Second).UnixNano())
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		second <- rec
	}()
	select {
	case rec := <-second:
		require.Equal(t, "response-2", rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expired pending response did not refetch")
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPPendingPersistenceCannotOpenDuplicateMiss(t *testing.T) {
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	staged := newStagedResponseCache()
	t.Cleanup(func() { _ = staged.Close() })
	p.cache = staged
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		first <- rec
	}()
	select {
	case <-staged.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}

	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		second <- rec
	}()
	select {
	case <-staged.secondGetStarted:
	case <-time.After(time.Second):
		t.Fatal("second cache lookup did not start")
	}
	staged.releaseSetOnce.Do(func() { close(staged.releaseSet) })
	select {
	case <-staged.setFinished:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not finish")
	}
	time.Sleep(20 * time.Millisecond)
	staged.releaseGetOnce.Do(func() { close(staged.releaseSecondGet) })

	require.Equal(t, "ok", (<-first).Body.String())
	select {
	case rec := <-second:
		require.Equal(t, "ok", rec.Body.String())
	case <-time.After(time.Second):
		t.Fatal("second response did not complete")
	}
	require.Equal(t, int32(1), hits.Load(), "a completed pending write must not open a second upstream miss")
}

func TestHTTPStreamingMissDoesNotWaitForCachePersistence(t *testing.T) {
	p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	blocking := newBlockingResponseCache()
	t.Cleanup(func() { _ = blocking.Close() })
	p.cache = blocking
	off := false
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})

	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec, _ := cacheRequest(p, rule, nil)
		response <- rec
	}()
	select {
	case <-blocking.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}
	select {
	case rec := <-response:
		require.Equal(t, "ok", rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("streaming response waited for cache persistence")
	}
	_ = blocking.Close()
}

func TestHTTPCoalesceDisabledStreamsMisses(t *testing.T) {
	firstChunk := make(chan struct{}, 1)
	release := make(chan struct{})
	p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("first"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		firstChunk <- struct{}{}
		<-release
		_, _ = w.Write([]byte("second"))
	})
	off := false
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.allow(w, r, rule, time.Now())
	}))
	t.Cleanup(proxyServer.Close)

	response := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.Get(proxyServer.URL + "/status")
		if err != nil {
			errCh <- err
			return
		}
		response <- resp
	}()
	<-firstChunk

	var resp *http.Response
	select {
	case err := <-errCh:
		t.Fatal(err)
	case resp = <-response:
	case <-time.After(250 * time.Millisecond):
		close(release)
		t.Fatal("headers were not streamed while coalescing was disabled")
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	buf := make([]byte, len("first"))
	_, err := io.ReadFull(resp.Body, buf)
	require.NoError(t, err)
	require.Equal(t, "first", string(buf))
	close(release)
	rest, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "second", string(rest))
	require.Equal(t, cacheMiss, resp.Header.Get(cacheStateHeader))
}

func newJSONCacheHandler(t *testing.T, rule *JsonRpcRule) *JsonRpcHandler {
	t.Helper()
	cache, err := newResponseCache[uint64, *JsonRpcMsg](nil, nil, "json-review", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, rule.Compile())
	return &JsonRpcHandler{
		log:           log.WithField("test", "json-review"),
		cache:         cache,
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		now:           time.Now,
		defaultAction: RuleActionDeny,
		rules:         []*JsonRpcRule{rule},
	}
}

func jsonRequestContext() (*http.Request, *RequestStats) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, stats := WithRequestStats(req.Context())
	return req.WithContext(ctx), stats
}

func TestJSONRPCStaleResponseCarriesCacheHeader(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: 5 * time.Second, StaleWhileRevalidate: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	cors := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://app.example"}}
	require.NoError(t, cors.Compile())
	h.cors = cors
	base := time.Unix(2_000_000, 0)
	h.now = func() time.Time { return base }
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	hash := request.HashWithRule(rule.Fingerprint)
	require.NoError(t, h.cache.Set(context.Background(), hash, &JsonRpcMsg{
		Version:  "2.0",
		ID:       1,
		Result:   []byte(`"stale"`),
		StoredAt: base.Add(-6 * time.Second),
	}, time.Hour))

	rec := httptest.NewRecorder()
	req, _ := jsonRequestContext()
	req.Header.Set("Origin", "https://app.example")
	h.handleHttpSingle(request, rec, req, func(http.ResponseWriter, *http.Request) {}, time.Now())

	require.Equal(t, cacheStale, rec.Header().Get(cacheStateHeader))
	require.Equal(t, "https://app.example", rec.Header().Get("Access-Control-Allow-Origin"))
}

func TestJSONRPCNotificationDoesNotCoalesceWithCall(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var upstreamCalls atomic.Int32
	next := func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.Header.Get("X-Request-Kind") == "notification" {
			started <- struct{}{}
			<-release
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":7,"result":"ok"}`))
	}

	notificationDone := make(chan struct{}, 1)
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("X-Request-Kind", "notification")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: "status"}, rec, r, next, time.Now())
		notificationDone <- struct{}{}
	}()
	<-started

	callDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("X-Request-Kind", "call")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 7, Method: "status"}, rec, r, next, time.Now())
		callDone <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-notificationDone
	rec := <-callDone

	require.Equal(t, int32(2), upstreamCalls.Load())
	require.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":"ok"}`, rec.Body.String())
}

func TestJSONRPCCoalescedMissPreservesStatusAndHeaders(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute, CacheError: true, CacheEmptyResult: true},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	next := func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.Header().Set("Retry-After", "3")
		w.Header().Set("X-Upstream", "node-a")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":9,"error":{"code":-32000,"message":"busy"}}`))
	}

	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 9, Method: "status"}, rec, req, next, time.Now())
		first <- rec
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 10, Method: "status"}, rec, req, next, time.Now())
		second <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	recA := <-first
	recB := <-second
	for id, rec := range map[int]*httptest.ResponseRecorder{9: recA, 10: recB} {
		require.Equal(t, http.StatusTooManyRequests, rec.Code)
		require.Equal(t, "3", rec.Header().Get("Retry-After"))
		require.JSONEq(t, `{"jsonrpc":"2.0","id":`+strconv.Itoa(id)+`,"error":{"code":-32000,"message":"busy"}}`, rec.Body.String())
	}
	require.Equal(t, "node-a", recA.Header().Get("X-Upstream"))
	require.Empty(t, recB.Header().Get("X-Upstream"))
}

func TestJSONRPCCoalescedMissPreservesNonJSONTransportFailure(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{name: "bad gateway", status: http.StatusBadGateway, contentType: "text/html", body: "<h1>bad gateway</h1>"},
		{name: "rate limited", status: http.StatusTooManyRequests, contentType: "text/plain", body: "slow down"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
				Cache:   &RuleCache{Enable: true, TTL: time.Minute},
			}
			h := newJSONCacheHandler(t, rule)
			next := func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tt.contentType)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}

			rec := httptest.NewRecorder()
			req, _ := jsonRequestContext()
			h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())

			require.Equal(t, tt.status, rec.Code)
			require.Equal(t, tt.contentType, rec.Header().Get("Content-Type"))
			require.Equal(t, tt.body, rec.Body.String())
		})
	}
}

func TestJSONRPCCoalescedWaiterRefetchesUncacheableError(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var upstreamCalls atomic.Int32
	next := func(w http.ResponseWriter, r *http.Request) {
		if upstreamCalls.Add(1) == 1 {
			started <- struct{}{}
			<-release
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + r.Header.Get("X-Test-ID") + `,"error":{"code":-32000,"message":"` + r.Header.Get("X-User") + `"}}`))
	}

	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		req.Header.Set("X-Test-ID", "1")
		req.Header.Set("X-User", "alice")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())
		first <- rec
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		req.Header.Set("X-Test-ID", "2")
		req.Header.Set("X-User", "bob")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, req, next, time.Now())
		second <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"alice"}}`, (<-first).Body.String())
	require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"bob"}}`, (<-second).Body.String())
	require.Equal(t, int32(2), upstreamCalls.Load())
}

func TestJSONRPCCoalescedMissOverridesUpstreamCacheMarker(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	next := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(cacheStateHeader, cacheHit)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	rec := httptest.NewRecorder()
	req, _ := jsonRequestContext()
	h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())

	require.Equal(t, cacheMiss, rec.Header().Get(cacheStateHeader))
}

func TestJSONRPCRewrittenResponsesDropRepresentationValidators(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	next := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"upstream-wire"`)
		w.Header().Set("Last-Modified", "Sun, 20 Jul 2026 20:00:00 GMT")
		w.Header().Set("Content-MD5", "upstream-digest")
		started <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0", "id":1, "result":"ok"}`))
	}

	ownerResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())
		ownerResult <- rec
	}()
	<-started
	waiterResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 999, Method: "status"}, rec, req, next, time.Now())
		waiterResult <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	owner := <-ownerResult
	rec := <-waiterResult

	require.Equal(t, `"upstream-wire"`, owner.Header().Get("ETag"))
	require.Empty(t, rec.Header().Get("ETag"))
	require.Empty(t, rec.Header().Get("Last-Modified"))
	require.Empty(t, rec.Header().Get("Content-MD5"))
	require.JSONEq(t, `{"jsonrpc":"2.0","id":999,"result":"ok"}`, rec.Body.String())
}

func TestJSONRPCCoalescedWaitersKeepUpstreamStats(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	next := func(w http.ResponseWriter, r *http.Request) {
		if stats := RequestStatsFromCtx(r.Context()); stats != nil {
			stats.Upstream = "node-a"
		}
		started <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	first := make(chan *RequestStats, 1)
	second := make(chan *RequestStats, 1)
	go func() {
		rec := httptest.NewRecorder()
		r, stats := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, r, next, time.Now())
		first <- stats
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		r, stats := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, r, next, time.Now())
		second <- stats
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.Equal(t, "node-a", (<-first).Upstream)
	require.Equal(t, "node-a", (<-second).Upstream)
}

func TestJSONRPCCoalescedWaitersDoNotReceiveLeaderHeaders(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	next := func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.Header().Set("Set-Cookie", "session="+r.Header.Get("X-User"))
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("X-User", "alice")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, r, next, time.Now())
		first <- rec
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("X-User", "bob")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, r, next, time.Now())
		second <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	recA := <-first
	recB := <-second
	require.NotContains(t, recA.Header().Values("Set-Cookie"), "session=bob")
	require.NotContains(t, recB.Header().Values("Set-Cookie"), "session=alice")
}

func TestJSONRPCCoalescedWaitersReapplyCORS(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	cors := &CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://a.example", "https://b.example"},
	}
	require.NoError(t, cors.Compile())
	h.cors = cors
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	next := func(w http.ResponseWriter, r *http.Request) {
		cors.ApplyToResponse(w.Header(), r.Header.Get("Origin"))
		started <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	first := make(chan *httptest.ResponseRecorder, 1)
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("Origin", "https://a.example")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, r, next, time.Now())
		first <- rec
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		r, _ := jsonRequestContext()
		r.Header.Set("Origin", "https://b.example")
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, r, next, time.Now())
		second <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.Equal(t, "https://a.example", (<-first).Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", (<-second).Header().Get("Access-Control-Allow-Origin"))
}

func TestJSONRPCForegroundMissUsesConfiguredProxyDeadline(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	type deadlineObservation struct {
		remaining time.Duration
		ok        bool
	}
	observedDeadline := make(chan deadlineObservation, 1)
	next := func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		remaining := time.Duration(0)
		if ok {
			remaining = time.Until(deadline)
		}
		observedDeadline <- deadlineObservation{remaining: remaining, ok: ok}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"bounded"}`))
	}
	configuredTimeout := 45 * time.Minute
	h.cacheConfig = &CacheGlobalConfig{HTTPForegroundFetchTimeout: configuredTimeout}

	rec := httptest.NewRecorder()
	req, _ := jsonRequestContext()
	ctx, cancel := context.WithTimeout(req.Context(), time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())

	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"bounded"}`, rec.Body.String())
	observed := <-observedDeadline
	require.True(t, observed.ok)
	require.Greater(t, observed.remaining, 44*time.Minute)
	require.LessOrEqual(t, observed.remaining, configuredTimeout)
}

func TestHTTPBackgroundRefreshUsesIndependentRequestStats(t *testing.T) {
	observedStats := make(chan *RequestStats, 1)
	target, err := url.Parse("http://upstream.invalid")
	require.NoError(t, err)
	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	reverseProxy.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		observedStats <- RequestStatsFromCtx(r.Context())
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("fresh")),
			Request:    r,
		}, nil
	})
	upstream := &HttpUpstream{Name: "up", Target: target, proxy: reverseProxy}
	upstream.healthy.Store(true)
	responseCache, err := newResponseCache[string, CachedResponse](nil, nil, "stats-test", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = responseCache.Close() })
	p := &HttpProxy{
		log:   log.WithField("test", "refresh-stats"),
		pool:  newTestHTTPPool("weighted-round-robin", 0, upstream),
		cache: responseCache,
		now:   time.Now,
	}
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	ctx, originalStats := WithRequestStats(req.Context())
	req = req.WithContext(ctx)

	_, err = p.backgroundRefreshFn(req, "key", &RuleCache{Enable: true, TTL: time.Minute}, "rule")()
	require.NoError(t, err)
	refreshStats := <-observedStats
	require.NotNil(t, refreshStats)
	require.NotSame(t, originalStats, refreshStats)
	require.Equal(t, "up", refreshStats.Upstream)
	require.Empty(t, originalStats.Upstream)
}

func TestHTTPBackgroundRefreshDropsClientPreconditions(t *testing.T) {
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range"} {
			if r.Header.Get(name) != "" {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		_, _ = w.Write([]byte("fresh"))
	})
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("If-Match", `"old"`)
	req.Header.Set("If-None-Match", `"old"`)
	req.Header.Set("If-Modified-Since", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
	req.Header.Set("If-Unmodified-Since", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("If-Range", `"old"`)

	out, err := p.backgroundRefreshFn(req, "key", &RuleCache{Enable: true, TTL: time.Minute}, "rule")()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, out.StatusCode)
	require.Equal(t, []byte("fresh"), out.Body)
	require.Equal(t, int32(1), hits.Load())

	cached, err := p.cache.Get(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("fresh"), cached.Data)
}

func TestJSONRPCBackgroundRefreshDropsClientPreconditions(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	next := func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range"} {
			if r.Header.Get(name) != "" {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"fresh"}`))
	}
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	hash := request.HashWithRule(rule.Fingerprint)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"status"}`))
	ctx, _ := WithRequestStats(req.Context())
	req = req.WithContext(ctx)
	req.Header.Set("If-Match", `"old"`)
	req.Header.Set("If-None-Match", `"old"`)
	req.Header.Set("If-Modified-Since", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
	req.Header.Set("If-Unmodified-Since", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("If-Range", `"old"`)

	out, err := h.singleBackgroundRefreshFn(req, next, hash, rule.Cache, "rule", request.Method)()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, out.StatusCode)
	require.NotNil(t, out.Message)

	cached, err := h.cache.Get(context.Background(), hash)
	require.NoError(t, err)
	require.JSONEq(t, `"fresh"`, string(cached.Result))
}

func TestJSONRPCBackgroundRefreshUsesIndependentRequestStats(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	observedStats := make(chan *RequestStats, 1)
	next := func(w http.ResponseWriter, r *http.Request) {
		stats := RequestStatsFromCtx(r.Context())
		stats.Upstream = "json-up"
		observedStats <- stats
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"fresh"}`))
	}
	req, originalStats := jsonRequestContext()
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}

	_, err := h.singleBackgroundRefreshFn(req, next, request.HashWithRule(rule.Fingerprint), rule.Cache, "rule", request.Method)()
	require.NoError(t, err)
	refreshStats := <-observedStats
	require.NotNil(t, refreshStats)
	require.NotSame(t, originalStats, refreshStats)
	require.Equal(t, "json-up", refreshStats.Upstream)
	require.Empty(t, originalStats.Upstream)
}

type deadlineObservingJSONCache struct {
	deadline chan bool
}

func (c *deadlineObservingJSONCache) Set(ctx context.Context, _ uint64, _ *JsonRpcMsg, _ time.Duration) error {
	_, ok := ctx.Deadline()
	c.deadline <- ok
	return nil
}

func (*deadlineObservingJSONCache) Get(context.Context, uint64) (*JsonRpcMsg, error) {
	return nil, cachepkg.ErrNotFound
}

func (*deadlineObservingJSONCache) Has(context.Context, uint64) (bool, error) { return false, nil }
func (*deadlineObservingJSONCache) Close() error                              { return nil }

func TestJSONRPCCachePersistenceHasDetachedDeadline(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	observing := &deadlineObservingJSONCache{deadline: make(chan bool, 1)}
	h.cache = observing
	next := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	rec := httptest.NewRecorder()
	req, _ := jsonRequestContext()
	h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())

	select {
	case hasDeadline := <-observing.deadline:
		require.True(t, hasDeadline)
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not run")
	}
}

type blockingJSONCache struct {
	setStarted chan struct{}
	releaseSet chan struct{}
	closeOnce  sync.Once
}

func newBlockingJSONCache() *blockingJSONCache {
	return &blockingJSONCache{setStarted: make(chan struct{}, 4), releaseSet: make(chan struct{})}
}

func (c *blockingJSONCache) Set(context.Context, uint64, *JsonRpcMsg, time.Duration) error {
	select {
	case c.setStarted <- struct{}{}:
	default:
	}
	<-c.releaseSet
	return nil
}

func (*blockingJSONCache) Get(context.Context, uint64) (*JsonRpcMsg, error) {
	return nil, cachepkg.ErrNotFound
}

func (*blockingJSONCache) Has(context.Context, uint64) (bool, error) { return false, nil }

func (c *blockingJSONCache) Close() error {
	c.closeOnce.Do(func() { close(c.releaseSet) })
	return nil
}

func TestJSONRPCMissResponseDoesNotWaitForCachePersistence(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	blocking := newBlockingJSONCache()
	t.Cleanup(func() { _ = blocking.Close() })
	h.cache = blocking
	next := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())
		response <- rec
	}()
	select {
	case <-blocking.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}
	select {
	case rec := <-response:
		require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`, rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("JSON-RPC response waited for cache persistence")
	}
	_ = blocking.Close()
}

func TestJSONRPCPendingResponseExpiresBeforePersistenceCompletes(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Second},
	}
	h := newJSONCacheHandler(t, rule)
	blocking := newBlockingJSONCache()
	t.Cleanup(func() { _ = blocking.Close() })
	h.cache = blocking
	base := time.Unix(2_000_000, 0)
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	h.now = func() time.Time { return time.Unix(0, clock.Load()) }
	var upstreamCalls atomic.Int32
	next := func(w http.ResponseWriter, _ *http.Request) {
		call := strconv.Itoa(int(upstreamCalls.Add(1)))
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + call + `,"result":"response-` + call + `"}`))
	}

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())
		first <- rec
	}()
	select {
	case <-blocking.setStarted:
	case <-time.After(time.Second):
		t.Fatal("cache persistence did not start")
	}
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"response-1"}`, (<-first).Body.String())

	clock.Store(base.Add(2 * time.Second).UnixNano())
	second := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, req, next, time.Now())
		second <- rec
	}()
	select {
	case rec := <-second:
		require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":"response-2"}`, rec.Body.String())
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expired pending JSON-RPC response did not refetch")
	}
	require.Equal(t, int32(2), upstreamCalls.Load())
}
