package cosmoguard

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newHardeningProxy(t *testing.T, nodes []NodeConfig, opts ...Option[HttpProxyOptions]) *HttpProxy {
	t.Helper()
	p, err := NewHttpProxy(t.Name(), "", nodes, serviceLCD, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	})
	return p
}

// deadUpstreamURL returns the URL of a listener that has been closed, so
// dialing it fails with a transport error.
func deadUpstreamURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func retryUpstreamConfig() *UpstreamConfig {
	return &UpstreamConfig{Strategy: "round-robin", Retries: RetryConfig{Max: 2}}
}

func TestHTTPBreakerIgnoresNonGatewayServerErrors(t *testing.T) {
	var status atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(upstream.Close)

	p := newHardeningProxy(t, []NodeConfig{{
		Name:           "up",
		LcdURL:         upstream.URL,
		CircuitBreaker: &CircuitBreakerConfig{ConsecutiveFailures: 2, CooldownPeriod: time.Minute},
	}})
	u := p.pool.Upstreams()[0]
	send := func(code int) {
		status.Store(int32(code))
		rec := httptest.NewRecorder()
		p.pool.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/block?height=999999999", nil))
		require.Equal(t, code, rec.Code)
	}

	for _, code := range []int{http.StatusInternalServerError, http.StatusNotImplemented, http.StatusInternalServerError, http.StatusInternalServerError} {
		send(code)
	}
	require.False(t, u.cbOpen.Load(), "client-inducible 5xx must not trip the breaker")

	send(http.StatusServiceUnavailable)
	send(http.StatusGatewayTimeout)
	require.True(t, u.cbOpen.Load(), "consecutive 503/504 must trip the breaker")
}

func TestHTTPUpstreamPoolUsesDedicatedTransport(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)

	p := newHardeningProxy(t, []NodeConfig{{Name: "a", LcdURL: upstream.URL}},
		WithServerConfig[HttpProxyOptions](&ServerConfig{WriteTimeout: 7 * time.Second}))
	require.NoError(t, p.pool.AddUpstream(NodeConfig{Name: "b", LcdURL: upstream.URL}))

	ups := p.pool.Upstreams()
	require.Len(t, ups, 2)
	for _, u := range ups {
		transport, ok := u.proxy.Transport.(*http.Transport)
		require.True(t, ok, "upstream %s must have a dedicated *http.Transport, got %T", u.Name, u.proxy.Transport)
		require.NotSame(t, http.DefaultTransport, transport)
		require.Same(t, p.pool.transport, transport)
		require.Equal(t, upstreamIdleConnsPerHost, transport.MaxIdleConnsPerHost)
		require.Equal(t, 7*time.Second, transport.ResponseHeaderTimeout)
	}
}

func TestHTTPUpstreamResponseHeaderTimeoutFollowsWriteTimeout(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		upstream.Close()
	})

	p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: upstream.URL}},
		WithServerConfig[HttpProxyOptions](&ServerConfig{WriteTimeout: 100 * time.Millisecond}))

	done := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.pool.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
		done <- rec.Code
	}()
	select {
	case code := <-done:
		require.Equal(t, http.StatusBadGateway, code)
	case <-time.After(5 * time.Second):
		t.Fatal("a header-stalling upstream must be abandoned after the write timeout")
	}
}

func TestHTTPCachedResponseKeepsRetryAfter(t *testing.T) {
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("slow down"))
	})
	off := false
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, CacheError: true, Coalesce: &off})
	key, err := p.getRequestHash(httptest.NewRequest(http.MethodGet, "/status", nil), rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)

	first := doGet(p, rule)
	require.Equal(t, http.StatusTooManyRequests, first.Code)
	require.Eventually(t, func() bool {
		_, err := p.cache.Get(context.Background(), key)
		return err == nil
	}, 2*time.Second, 5*time.Millisecond)

	second := doGet(p, rule)
	require.Equal(t, int32(1), hits.Load(), "second request must be a cache hit")
	require.Equal(t, cacheHit, second.Header().Get(cacheStateHeader))
	require.Equal(t, http.StatusTooManyRequests, second.Code)
	require.Equal(t, "7", second.Header().Get("Retry-After"))
}

func TestHTTPCacheKeyIgnoresCredentialQuery(t *testing.T) {
	a, err := NewAuthenticator(&AuthConfig{
		Enable:     true,
		Methods:    []AuthMethodConfig{{Type: "api-key", Header: "x-api-key", QueryParam: "api_key"}},
		Identities: []IdentityConfig{{Name: "a", APIKey: "key-a"}, {Name: "b", APIKey: "key-b"}},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	p := &HttpProxy{auth: a}
	metadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	hash := func(target string) string {
		t.Helper()
		got, err := p.getRequestHash(httptest.NewRequest(http.MethodGet, target, nil), 42, metadata)
		require.NoError(t, err)
		return got
	}

	require.Equal(t, hash("/cosmos/foo?height=1"), hash("/cosmos/foo?api_key=key-a&height=1"))
	require.Equal(t, hash("/cosmos/foo?api_key=key-a&height=1"), hash("/cosmos/foo?height=1&api_key=key-b"))
	require.NotEqual(t, hash("/cosmos/foo?api_key=key-a&height=1"), hash("/cosmos/foo?api_key=key-a&height=2"))
}

func TestHTTPRetryReplaysRequestBody(t *testing.T) {
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	}))
	t.Cleanup(alive.Close)

	p := newHardeningProxy(t,
		[]NodeConfig{{Name: "dead", LcdURL: deadUpstreamURL(t)}, {Name: "alive", LcdURL: alive.URL}},
		WithCORSConfig[HttpProxyOptions](compiledTestCORS(t)),
		WithUpstreamConfig[HttpProxyOptions](retryUpstreamConfig()))
	p.SetRules(nil, RuleActionAllow)
	front := httptest.NewServer(p)
	t.Cleanup(front.Close)

	// Round-robin over two upstreams: across four requests the dead one is
	// tried first at least once, which is the attempt that consumes the body.
	for i := 0; i < 4; i++ {
		req, err := http.NewRequest(http.MethodGet, front.URL+"/query", strings.NewReader(`{"q":1}`))
		require.NoError(t, err)
		req.Header.Set("Origin", "https://a.example")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "request %d: %s", i, body)
		require.Equal(t, `{"q":1}`, string(body))
		require.Equal(t, []string{"https://a.example"}, resp.Header.Values("Access-Control-Allow-Origin"), "request %d", i)
	}
}

func TestHTTPRetryOversizedChunkedBodyIs413WithCORS(t *testing.T) {
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an oversized body must not reach the upstream")
	}))
	t.Cleanup(alive.Close)
	limit := int64(4)
	p := newHardeningProxy(t,
		[]NodeConfig{{Name: "a", LcdURL: alive.URL}, {Name: "b", LcdURL: alive.URL}},
		WithCORSConfig[HttpProxyOptions](compiledTestCORS(t)),
		WithServerConfig[HttpProxyOptions](&ServerConfig{MaxRequestBody: &limit}),
		WithUpstreamConfig[HttpProxyOptions](retryUpstreamConfig()))
	p.SetRules(nil, RuleActionAllow)

	req := httptest.NewRequest(http.MethodGet, "/query", strings.NewReader("0123456789"))
	req.ContentLength = -1 // chunked: only the MaxBytesReader backstop sees the size
	req.Header.Set("Origin", "https://a.example")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Equal(t, []string{"https://a.example"}, rec.Header().Values("Access-Control-Allow-Origin"))
}

func TestHTTPSingleUpstreamTransportErrorCarriesCORS(t *testing.T) {
	p := newHardeningProxy(t, []NodeConfig{{Name: "dead", LcdURL: deadUpstreamURL(t)}},
		WithCORSConfig[HttpProxyOptions](compiledTestCORS(t)))
	p.SetRules(nil, RuleActionAllow)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Origin", "https://a.example")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, []string{"https://a.example"}, rec.Header().Values("Access-Control-Allow-Origin"))
	require.Equal(t, []string{"Origin"}, rec.Header().Values("Vary"))
}

func TestHTTPProxyGeneratedResponsesCarryCORS(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(upstream.Close)
	limit := int64(4)
	p := newHardeningProxy(t, []NodeConfig{{Name: "up", LcdURL: upstream.URL}},
		WithCORSConfig[HttpProxyOptions](compiledTestCORS(t)),
		WithServerConfig[HttpProxyOptions](&ServerConfig{MaxRequestBody: &limit}))
	deny := &HttpRule{Priority: 1, Action: RuleActionDeny, Paths: []string{"/denied"}, Methods: []string{"GET"}}
	require.NoError(t, deny.Compile())
	p.SetRules([]*HttpRule{deny}, RuleActionAllow)

	serve := func(method, target, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Origin", "https://a.example")
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		return rec
	}

	denied := serve(http.MethodGet, "/denied", "")
	require.Equal(t, http.StatusUnauthorized, denied.Code)
	require.Equal(t, []string{"https://a.example"}, denied.Header().Values("Access-Control-Allow-Origin"))

	tooLarge := serve(http.MethodPost, "/anything", "0123456789")
	require.Equal(t, http.StatusRequestEntityTooLarge, tooLarge.Code)
	require.Equal(t, []string{"https://a.example"}, tooLarge.Header().Values("Access-Control-Allow-Origin"))

	proxied := serve(http.MethodGet, "/status", "")
	require.Equal(t, http.StatusOK, proxied.Code)
	require.Equal(t, []string{"https://a.example"}, proxied.Header().Values("Access-Control-Allow-Origin"),
		"a proxied response must carry exactly one ACAO")
	require.Equal(t, []string{"Origin"}, proxied.Header().Values("Vary"))
}

func TestJSONRPCPassThroughCarriesSingleCORSOrigin(t *testing.T) {
	cors := compiledTestCORS(t)
	p, _ := newRealHookCacheProxy(t, cors, func(w http.ResponseWriter, r *http.Request) {
		writeJSONRPCUpstreamResult(t, w, r)
	})
	h := &JsonRpcHandler{
		log:           p.log,
		cgDashboard:   newDashboardObservability(),
		now:           time.Now,
		defaultAction: RuleActionAllow,
		cors:          cors,
		limiters:      map[uint64]RateLimiter{},
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"status"}`))
	req.Header.Set("Origin", "https://a.example")
	rec := httptest.NewRecorder()

	h.handleHttp(rec, req, p.pool.ServeHTTP, time.Now())

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"https://a.example"}, rec.Header().Values("Access-Control-Allow-Origin"))
	require.Equal(t, []string{"Origin"}, rec.Header().Values("Vary"))
}

func TestHTTPPoolExhaustedRetriesCarryCORS(t *testing.T) {
	cors := compiledTestCORS(t)
	p := newHardeningProxy(t,
		[]NodeConfig{{Name: "a", LcdURL: deadUpstreamURL(t)}, {Name: "b", LcdURL: deadUpstreamURL(t)}},
		WithCORSConfig[HttpProxyOptions](cors),
		WithUpstreamConfig[HttpProxyOptions](retryUpstreamConfig()))
	p.SetRules(nil, RuleActionAllow)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Origin", "https://b.example")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, []string{"https://b.example"}, rec.Header().Values("Access-Control-Allow-Origin"))
}

// countingParams counts how often the JSON-RPC params are marshaled, which
// is what computing a cache key costs.
type countingParams struct{ n *atomic.Int32 }

func (c countingParams) MarshalJSON() ([]byte, error) {
	c.n.Add(1)
	return []byte(`[]`), nil
}

func TestJSONRPCCacheKeyOnlyForCacheableMatch(t *testing.T) {
	var rules []*JsonRpcRule
	for _, method := range []string{"a", "b", "c"} {
		rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{method}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
		require.NoError(t, rule.Compile())
		rules = append(rules, rule)
	}
	var marshals atomic.Int32
	request := func() *JsonRpcMsg {
		return &JsonRpcMsg{Version: "2.0", ID: 1, Method: "unmatched", Params: countingParams{&marshals}}
	}

	t.Run("http", func(t *testing.T) {
		marshals.Store(0)
		h := &JsonRpcHandler{
			log:           log.WithField("test", t.Name()),
			cgDashboard:   newDashboardObservability(),
			now:           time.Now,
			defaultAction: RuleActionDeny,
			rules:         rules,
			limiters:      map[uint64]RateLimiter{},
		}
		h.handleHttpSingle(request(), httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil),
			func(http.ResponseWriter, *http.Request) { t.Fatal("unmatched request must not be forwarded") }, time.Now())
		require.Zero(t, marshals.Load())
	})

	t.Run("websocket", func(t *testing.T) {
		marshals.Store(0)
		proxy := &JsonRpcWebSocketProxy{log: log.WithField("test", t.Name()), cgDashboard: newDashboardObservability()}
		proxy.SetRules(rules, RuleActionDeny, nil)
		client, _ := newWSCacheClient(t)
		require.NoError(t, proxy.handleRequest(client, request(), "127.0.0.1", nil))
		require.Zero(t, marshals.Load())
	})
}

func TestMetricsServerTimeouts(t *testing.T) {
	srv := newMetricsServer("127.0.0.1:0", http.NotFoundHandler(), false)
	require.Equal(t, 10*time.Second, srv.ReadHeaderTimeout)
	require.Equal(t, 30*time.Second, srv.ReadTimeout)
	require.Equal(t, 30*time.Second, srv.WriteTimeout)
	require.Equal(t, 120*time.Second, srv.IdleTimeout)

	withPprof := newMetricsServer("127.0.0.1:0", http.NotFoundHandler(), true)
	require.Equal(t, 10*time.Second, withPprof.ReadHeaderTimeout)
	require.Zero(t, withPprof.WriteTimeout, "pprof's default 30s profile must not exceed the write timeout")
}

func TestHTTPPoolWithoutTransportServesAddedUpstream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(upstream.Close)
	pool := newTestHTTPPool("round-robin", 0)
	pool.service = serviceLCD
	require.NoError(t, pool.AddUpstream(NodeConfig{Name: "up", LcdURL: upstream.URL}))

	rec := httptest.NewRecorder()
	pool.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTPRetryLoopSharesHeaderTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	release := make(chan struct{})
	var hits atomic.Int32
	stall := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	a := httptest.NewServer(stall)
	b := httptest.NewServer(stall)
	t.Cleanup(func() {
		close(release)
		a.Close()
		b.Close()
	})

	p := newHardeningProxy(t,
		[]NodeConfig{{Name: "a", LcdURL: a.URL}, {Name: "b", LcdURL: b.URL}},
		WithServerConfig[HttpProxyOptions](&ServerConfig{WriteTimeout: timeout}),
		WithUpstreamConfig[HttpProxyOptions](retryUpstreamConfig()))

	start := time.Now()
	rec := httptest.NewRecorder()
	p.pool.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	elapsed := time.Since(start)

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Less(t, elapsed, 2*timeout-timeout/4,
		"both stalled upstreams must share one header-timeout budget, took %s", elapsed)
}
