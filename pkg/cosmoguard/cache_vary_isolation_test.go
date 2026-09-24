package cosmoguard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newRealHookCacheProxy(t *testing.T, cors *CORSConfig, handler http.HandlerFunc) (*HttpProxy, *atomic.Int32) {
	t.Helper()

	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(upstream.Close)

	p, err := NewHttpProxy(
		"vary-isolation-test",
		"",
		[]NodeConfig{{Name: "up", LcdURL: upstream.URL}},
		"lcd",
		WithCORSConfig[HttpProxyOptions](cors),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	})
	return p, &hits
}

func compiledTestCORS(t *testing.T) *CORSConfig {
	t.Helper()
	cors := &CORSConfig{
		Enable:         true,
		AllowedOrigins: []string{"https://a.example", "https://b.example"},
	}
	require.NoError(t, cors.Compile())
	return cors
}

func writeJSONRPCUpstreamResult(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read upstream request: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	single, batch, err := ParseJsonRpcMessage(body)
	if err != nil {
		t.Errorf("parse upstream request: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var responseBody []byte
	if single != nil {
		responseBody, err = WithResult(single, "ok").Marshal()
	} else {
		responses := make(JsonRpcMsgs, 0, len(batch))
		for _, request := range batch {
			if request.ID != nil {
				responses = append(responses, WithResult(request, "ok"))
			}
		}
		responseBody, err = responses.Marshal()
	}
	if err != nil {
		t.Errorf("marshal upstream response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(responseBody); err != nil {
		t.Errorf("write upstream response: %v", err)
	}
}

func TestHTTPUpstreamVaryOriginIsolatesSequentialRequests(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Origin")
		_, _ = fmt.Fprint(w, r.Header.Get("Origin"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	second, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})

	require.Equal(t, "https://a.example", first.Body.String())
	require.Equal(t, "https://b.example", second.Body.String())
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPStreamingUpstreamVaryOriginIsolatesSequentialRequests(t *testing.T) {
	off := false
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Origin")
		_, _ = fmt.Fprint(w, r.Header.Get("Origin"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})

	first, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	second, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})

	require.Equal(t, "https://a.example", first.Body.String())
	require.Equal(t, "https://b.example", second.Body.String())
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPStreamingCORSAddedVaryOriginRemainsCacheableThroughRealHook(t *testing.T) {
	off := false
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("shared"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})

	first, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	keyRequest := httptest.NewRequest(http.MethodGet, "/status", nil)
	keyRequest.Header.Set("Origin", "https://a.example")
	key, err := p.getRequestHash(keyRequest, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stored, cacheErr := p.cache.Has(t.Context(), key)
		return cacheErr == nil && stored
	}, time.Second, time.Millisecond)
	second, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})

	require.Equal(t, "shared", first.Body.String())
	require.Equal(t, "shared", second.Body.String())
	require.Equal(t, "https://a.example", first.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", second.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, int32(1), hits.Load())
}

func TestHTTPUpstreamVaryOriginIsNotSharedByCoalescedCallers(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.Header().Set("Vary", "Origin")
		_, _ = fmt.Fprint(w, r.Header.Get("Origin"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	origins := []string{"https://a.example", "https://b.example"}
	bodies := make([]string, len(origins))
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(origins))
	for i, origin := range origins {
		go func() {
			defer wg.Done()
			<-start
			response, _ := cacheRequest(p, rule, http.Header{"Origin": {origin}})
			bodies[i] = response.Body.String()
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, origins, bodies)
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPBackgroundRefreshDoesNotStoreUpstreamVaryOrigin(t *testing.T) {
	p, _ := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Origin")
		_, _ = fmt.Fprint(w, r.Header.Get("Origin"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Origin", "https://a.example")

	response, err := p.backgroundRefreshFn(req, "background-vary", rule.Cache, "rule")()
	require.NoError(t, err)
	require.False(t, response.Shareable)
	_, err = p.cache.Get(t.Context(), "background-vary")
	require.Error(t, err)
}

func TestHTTPCORSAddedVaryOriginRemainsCacheableThroughRealHook(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("shared"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	first, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	second, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})

	require.Equal(t, "shared", first.Body.String())
	require.Equal(t, "shared", second.Body.String())
	require.Equal(t, "https://a.example", first.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", second.Header().Get("Access-Control-Allow-Origin"))
	require.True(t, corsVaryHasOrigin(first.Header()))
	require.True(t, corsVaryHasOrigin(second.Header()))
	require.Equal(t, int32(1), hits.Load())
}

func jsonTenantRequest(tenant string, id int) *http.Request {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"status"}`, id)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Tenant", tenant)
	ctx, _ := WithRequestStats(req.Context())
	return req.WithContext(ctx)
}

func jsonTenantUpstream(delay time.Duration, hits *atomic.Int32) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Vary", "X-Tenant")
		id := 1
		if r.Header.Get("X-Tenant") == "bob" {
			id = 2
		}
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%q}`, id, r.Header.Get("X-Tenant"))
	}
}

func TestJSONRPCUpstreamVaryIsolatesSequentialRequests(t *testing.T) {
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	var hits atomic.Int32
	next := jsonTenantUpstream(0, &hits)

	for i, tenant := range []string{"alice", "bob"} {
		response := httptest.NewRecorder()
		request := &JsonRpcMsg{Version: "2.0", ID: i + 1, Method: "status"}
		h.handleHttpSingle(request, response, jsonTenantRequest(tenant, i+1), next, time.Now())
		require.Contains(t, response.Body.String(), tenant)
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestJSONRPCNonCoalescedUpstreamVaryPreventsStorage(t *testing.T) {
	off := false
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off}}
	h := newJSONCacheHandler(t, rule)
	capturing := &capturingJSONCache{set: make(chan capturedJSONStore, 1)}
	h.cache = capturing
	var hits atomic.Int32
	next := jsonTenantUpstream(0, &hits)

	response := httptest.NewRecorder()
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	h.handleHttpSingle(request, response, jsonTenantRequest("alice", 1), next, time.Now())

	require.Contains(t, response.Body.String(), "alice")
	require.Equal(t, int32(1), hits.Load())
	assertNoJSONStore(t, capturing)
}

func TestJSONRPCNonCoalescedCORSAddedVaryStoresAndHitsThroughRealHook(t *testing.T) {
	off := false
	cors := compiledTestCORS(t)
	p, hits := newRealHookCacheProxy(t, cors, func(w http.ResponseWriter, r *http.Request) {
		writeJSONRPCUpstreamResult(t, w, r)
	})
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off}}
	h := newJSONCacheHandler(t, rule)
	h.cors = cors
	next := p.pool.ServeHTTP

	firstRequest := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	firstHTTP := jsonTenantRequest("", 1)
	firstHTTP.Header.Set("Origin", "https://a.example")
	first := httptest.NewRecorder()
	h.handleHttpSingle(firstRequest, first, firstHTTP, next, time.Now())

	hash := jsonRPCHTTPCacheKey(firstRequest, rule.Fingerprint, firstHTTP)
	require.Eventually(t, func() bool {
		_, err := h.cache.Get(t.Context(), hash)
		return err == nil
	}, time.Second, time.Millisecond)

	secondRequest := &JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}
	secondHTTP := jsonTenantRequest("", 2)
	secondHTTP.Header.Set("Origin", "https://b.example")
	second := httptest.NewRecorder()
	h.handleHttpSingle(secondRequest, second, secondHTTP, next, time.Now())

	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`, first.Body.String())
	require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":"ok"}`, second.Body.String())
	require.Equal(t, "https://a.example", first.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", second.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, cacheHit, second.Header().Get(cacheStateHeader))
	require.Equal(t, int32(1), hits.Load())
}

func TestJSONRPCUpstreamVaryIsNotSharedByCoalescedCallers(t *testing.T) {
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	var hits atomic.Int32
	next := jsonTenantUpstream(60*time.Millisecond, &hits)

	tenants := []string{"alice", "bob"}
	bodies := make([]string, len(tenants))
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(tenants))
	for i, tenant := range tenants {
		go func() {
			defer wg.Done()
			<-start
			response := httptest.NewRecorder()
			request := &JsonRpcMsg{Version: "2.0", ID: i + 1, Method: "status"}
			h.handleHttpSingle(request, response, jsonTenantRequest(tenant, i+1), next, time.Now())
			bodies[i] = response.Body.String()
		}()
	}
	close(start)
	wg.Wait()

	for i, tenant := range tenants {
		require.Contains(t, bodies[i], tenant)
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestJSONRPCBatchUpstreamVaryPreventsStorage(t *testing.T) {
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	var hits atomic.Int32
	next := func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Add("Vary", "X-Tenant")
		_, _ = fmt.Fprintf(w, `[{"jsonrpc":"2.0","id":1,"result":%q}]`, r.Header.Get("X-Tenant"))
	}

	for _, tenant := range []string{"alice", "bob"} {
		response := httptest.NewRecorder()
		h.handleHttpBatch(
			JsonRpcMsgs{{Version: "2.0", ID: 1, Method: "status"}},
			response,
			jsonTenantRequest(tenant, 1),
			next,
			time.Now(),
		)
		require.Contains(t, response.Body.String(), tenant)
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestJSONRPCBatchCORSAddedVaryStoresAndHitsThroughRealHook(t *testing.T) {
	cors := compiledTestCORS(t)
	p, hits := newRealHookCacheProxy(t, cors, func(w http.ResponseWriter, r *http.Request) {
		writeJSONRPCUpstreamResult(t, w, r)
	})
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	h.cors = cors
	next := p.pool.ServeHTTP

	responses := make([]*httptest.ResponseRecorder, 0, 2)
	for i, origin := range []string{"https://a.example", "https://b.example"} {
		id := i + 1
		response := httptest.NewRecorder()
		httpRequest := jsonTenantRequest("", id)
		httpRequest.Header.Set("Origin", origin)
		h.handleHttpBatch(
			JsonRpcMsgs{{Version: "2.0", ID: id, Method: "status"}},
			response,
			httpRequest,
			next,
			time.Now(),
		)
		responses = append(responses, response)
	}

	require.JSONEq(t, `[{"jsonrpc":"2.0","id":1,"result":"ok"}]`, responses[0].Body.String())
	require.JSONEq(t, `[{"jsonrpc":"2.0","id":2,"result":"ok"}]`, responses[1].Body.String())
	require.Equal(t, "https://a.example", responses[0].Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", responses[1].Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, int32(1), hits.Load())
}

func TestJSONRPCBackgroundRefreshDoesNotStoreUpstreamVary(t *testing.T) {
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	capturing := &capturingJSONCache{set: make(chan capturedJSONStore, 1)}
	h.cache = capturing
	next := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Vary", "Accept-Encoding")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	response, err := h.singleBackgroundRefreshFn(jsonTenantRequest("alice", 1), next, 42, rule.Cache, "rule", "status")()
	require.NoError(t, err)
	require.False(t, response.Shareable)
	assertNoJSONStore(t, capturing)
}

func TestJSONRPCBackgroundRefreshCORSAddedVaryRemainsShareableThroughRealHook(t *testing.T) {
	cors := compiledTestCORS(t)
	p, hits := newRealHookCacheProxy(t, cors, func(w http.ResponseWriter, r *http.Request) {
		writeJSONRPCUpstreamResult(t, w, r)
	})
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	h.cors = cors
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	httpRequest := jsonTenantRequest("", 1)
	httpRequest.Header.Set("Origin", "https://a.example")
	hash := request.HashWithRule(rule.Fingerprint)

	response, err := h.singleBackgroundRefreshFn(httpRequest, p.pool.ServeHTTP, hash, rule.Cache, "rule", request.Method)()
	require.NoError(t, err)
	require.True(t, response.Shareable)
	require.Equal(t, "https://a.example", response.Headers.Get("Access-Control-Allow-Origin"))
	require.True(t, corsVaryHasOrigin(response.Headers))
	cached, err := h.cache.Get(t.Context(), hash)
	require.NoError(t, err)
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`, string(mustMarshalJSONRPC(t, cached)))
	require.Equal(t, int32(1), hits.Load())
}

func mustMarshalJSONRPC(t *testing.T, response *JsonRpcMsg) []byte {
	t.Helper()
	body, err := response.Marshal()
	require.NoError(t, err)
	return body
}

// cometBFTWildcardCORS mimics a CometBFT or Cosmos SDK node with cors_allowed_origins = ["*"]: it
// sends `Vary: Origin` on every response and `Access-Control-Allow-Origin: *` when the request
// carries an Origin, while the body is the same for every client.
func cometBFTWildcardCORS(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Origin")
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}
	_, _ = w.Write([]byte("status"))
}

// Many clients from many origins must be served from the cache: curl-like clients share one entry
// and all browsers share another, whatever their origin, with the node's CORS header replayed.
func TestHTTPWildcardCORSUpstreamIsSharedAcrossOrigins(t *testing.T) {
	for name, coalesce := range map[string]bool{"coalesced": true, "streaming": false} {
		t.Run(name, func(t *testing.T) {
			p, hits := newRealHookCacheProxy(t, nil, cometBFTWildcardCORS)
			rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce})
			waitStored := func(origin string) {
				req := httptest.NewRequest(http.MethodGet, "/status", nil)
				if origin != "" {
					req.Header.Set("Origin", origin)
				}
				key, err := p.getRequestHash(req, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					stored, err := p.cache.Has(t.Context(), key)
					return err == nil && stored
				}, time.Second, time.Millisecond)
			}

			first, _ := cacheRequest(p, rule, nil)
			require.Empty(t, first.Header().Get("Access-Control-Allow-Origin"))
			waitStored("")
			for range 3 {
				response, _ := cacheRequest(p, rule, nil)
				require.Equal(t, "status", response.Body.String())
				require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
			}
			require.Equal(t, int32(1), hits.Load(), "clients without an Origin share one entry")

			origins := []string{"https://a.example", "https://b.example", "https://c.example", "https://d.example"}
			_, _ = cacheRequest(p, rule, http.Header{"Origin": {origins[0]}})
			waitStored(origins[0])
			for _, origin := range origins {
				response, _ := cacheRequest(p, rule, http.Header{"Origin": {origin}})
				require.Equal(t, "status", response.Body.String())
				require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"), origin)
			}
			require.Equal(t, int32(2), hits.Load(), "every origin shares one entry")
		})
	}
}

// An upstream that answers each origin differently (echoing it back) is not shared across origins.
func TestHTTPOriginSpecificCORSUpstreamIsNotShared(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		_, _ = w.Write([]byte("status"))
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	a, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	b, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})
	require.Equal(t, "https://a.example", a.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "https://b.example", b.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, int32(2), hits.Load())
}

// When cosmoguard owns CORS it replaces the node's headers on cache hits with its own policy.
func TestHTTPWildcardCORSUpstreamBehindCosmoguardCORS(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, compiledTestCORS(t), cometBFTWildcardCORS)
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	a, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://a.example"}})
	require.Eventually(t, func() bool {
		b, _ := cacheRequest(p, rule, http.Header{"Origin": {"https://b.example"}})
		return b.Header().Get(cacheStateHeader) == cacheHit && b.Header().Get("Access-Control-Allow-Origin") == "https://b.example"
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, "https://a.example", a.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, int32(1), hits.Load())
}

func TestHTTPCacheableByVary(t *testing.T) {
	wildcard := http.Header{"Vary": {"Origin"}, "Access-Control-Allow-Origin": {"*"}}
	require.True(t, httpCacheableByVary(wildcard, true))
	require.False(t, httpCacheableByVary(wildcard, false), "a wildcard answer to a request without an Origin is unexpected")
	require.True(t, httpCacheableByVary(http.Header{"Vary": {"Origin"}}, false))
	require.False(t, httpCacheableByVary(http.Header{"Vary": {"Origin"}}, true), "no CORS answer to an origin may be origin-specific")
	require.False(t, httpCacheableByVary(http.Header{"Vary": {"Origin"}, "Access-Control-Allow-Origin": {"https://a.example"}}, true))
	require.True(t, httpCacheableByVary(http.Header{"Vary": {"Accept-Encoding, Origin"}}, false))
	require.False(t, httpCacheableByVary(http.Header{"Vary": {"Origin, X-Tenant"}}, false))
	require.False(t, httpCacheableByVary(http.Header{"Vary": {"*"}}, false))
}

func TestPickCacheableHeadersStoresOnlyAWildcardACAO(t *testing.T) {
	wildcard := pickCacheableHeaders(http.Header{"Access-Control-Allow-Origin": {"*"}, "Access-Control-Expose-Headers": {"X-Custom"}}, nil)
	require.Equal(t, "*", wildcard["Access-Control-Allow-Origin"])
	require.Equal(t, "X-Custom", wildcard["Access-Control-Expose-Headers"])

	specific := pickCacheableHeaders(http.Header{"Access-Control-Allow-Origin": {"https://a.example"}, "Access-Control-Expose-Headers": {"X-Custom"}}, nil)
	require.NotContains(t, specific, "Access-Control-Allow-Origin")
	require.NotContains(t, specific, "Access-Control-Expose-Headers")
}

// jsonRPCWildcardCORSUpstream answers JSON-RPC like a CometBFT node with cors_allowed_origins = ["*"].
func jsonRPCWildcardCORSUpstream(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		if r.Header.Get("Origin") != "" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		writeJSONRPCUpstreamResult(t, w, r)
	}
}

func jsonRPCStatusRequest(id int, origin string) (*JsonRpcMsg, *http.Request) {
	httpRequest := jsonTenantRequest("", id)
	if origin != "" {
		httpRequest.Header.Set("Origin", origin)
	}
	return &JsonRpcMsg{Version: "2.0", ID: id, Method: "status"}, httpRequest
}

// JSON-RPC over HTTP to a wildcard-CORS node is shared across origins: plain clients share one entry,
// every browser origin shares another, and browser hits carry the node's wildcard CORS header.
func TestJSONRPCWildcardCORSUpstreamIsSharedAcrossOrigins(t *testing.T) {
	for name, coalesce := range map[string]bool{"coalesced": true, "non-coalesced": false} {
		t.Run(name, func(t *testing.T) {
			p, hits := newRealHookCacheProxy(t, nil, jsonRPCWildcardCORSUpstream(t))
			rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce}}
			h := newJSONCacheHandler(t, rule)
			next := p.pool.ServeHTTP
			call := func(id int, origin string) *httptest.ResponseRecorder {
				request, httpRequest := jsonRPCStatusRequest(id, origin)
				response := httptest.NewRecorder()
				h.handleHttpSingle(request, response, httpRequest, next, time.Now())
				require.JSONEq(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"ok"}`, id), response.Body.String())
				return response
			}
			waitStored := func(origin string) {
				request, httpRequest := jsonRPCStatusRequest(1, origin)
				key := jsonRPCHTTPCacheKey(request, rule.Fingerprint, httpRequest)
				require.Eventually(t, func() bool {
					_, err := h.cache.Get(t.Context(), key)
					return err == nil
				}, time.Second, time.Millisecond)
			}

			call(1, "")
			waitStored("")
			for id := 2; id <= 4; id++ {
				response := call(id, "")
				require.Equal(t, cacheHit, response.Header().Get(cacheStateHeader))
				require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
			}
			require.Equal(t, int32(1), hits.Load(), "clients without an Origin share one entry")

			call(5, "https://a.example")
			waitStored("https://a.example")
			for id, origin := range []string{"https://a.example", "https://b.example", "https://c.example"} {
				response := call(10+id, origin)
				require.Equal(t, cacheHit, response.Header().Get(cacheStateHeader), origin)
				require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"), origin)
			}
			require.Equal(t, int32(2), hits.Load(), "every origin shares one entry")
		})
	}
}

// A JSON-RPC upstream that answers each origin differently is not shared across origins.
func TestJSONRPCOriginSpecificCORSUpstreamIsNotShared(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		writeJSONRPCUpstreamResult(t, w, r)
	})
	off := false
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off}}
	h := newJSONCacheHandler(t, rule)
	for id, origin := range []string{"https://a.example", "https://b.example"} {
		request, httpRequest := jsonRPCStatusRequest(id+1, origin)
		response := httptest.NewRecorder()
		h.handleHttpSingle(request, response, httpRequest, p.pool.ServeHTTP, time.Now())
		require.Equal(t, origin, response.Header().Get("Access-Control-Allow-Origin"))
	}
	require.Equal(t, int32(2), hits.Load())
}

// A batch from a browser stores wildcard-CORS entries that later single browser requests hit.
func TestJSONRPCBatchWildcardCORSEntryServesBrowserSingles(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, nil, jsonRPCWildcardCORSUpstream(t))
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	_, batchHTTP := jsonRPCStatusRequest(1, "https://a.example")
	h.handleHttpBatch(JsonRpcMsgs{{Version: "2.0", ID: 1, Method: "status"}}, httptest.NewRecorder(), batchHTTP, p.pool.ServeHTTP, time.Now())
	require.Equal(t, int32(1), hits.Load())

	request, httpRequest := jsonRPCStatusRequest(2, "https://b.example")
	response := httptest.NewRecorder()
	h.handleHttpSingle(request, response, httpRequest, p.pool.ServeHTTP, time.Now())
	require.Equal(t, cacheHit, response.Header().Get(cacheStateHeader))
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, int32(1), hits.Load())
}

// The bucket without an Origin keeps the plain key the WebSocket path uses, so an entry primed over
// WebSocket serves plain HTTP clients, and never browsers, which have no CORS answer for it.
func TestJSONRPCWebSocketEntryServesOnlyRequestsWithoutOrigin(t *testing.T) {
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)
	request, plain := jsonRPCStatusRequest(1, "")
	require.NoError(t, h.cache.Set(t.Context(), request.HashWithRule(rule.Fingerprint), &JsonRpcMsg{Version: "2.0", ID: 1, Result: []byte(`"ws"`), StoredAt: time.Now()}, time.Minute))
	require.Equal(t, request.HashWithRule(rule.Fingerprint), jsonRPCHTTPCacheKey(request, rule.Fingerprint, plain))

	_, browser := jsonRPCStatusRequest(1, "https://a.example")
	require.NotEqual(t, request.HashWithRule(rule.Fingerprint), jsonRPCHTTPCacheKey(request, rule.Fingerprint, browser))
}

func TestJSONRPCCacheableByVary(t *testing.T) {
	require.True(t, jsonRPCCacheableByVary(http.Header{"Vary": {"Origin"}, "Access-Control-Allow-Origin": {"*"}}, true))
	require.True(t, jsonRPCCacheableByVary(http.Header{"Vary": {"Origin"}}, false))
	require.False(t, jsonRPCCacheableByVary(http.Header{"Vary": {"Origin"}, "Access-Control-Allow-Origin": {"https://a.example"}}, true))
	require.False(t, jsonRPCCacheableByVary(http.Header{"Vary": {"Accept-Encoding"}}, false), "JSON-RPC entries are re-marshalled")
	require.False(t, jsonRPCCacheableByVary(http.Header{"Vary": {"Origin, X-Tenant"}}, false))
}

// Browser callers coalesced onto one upstream fetch all receive the wildcard CORS answer.
func TestJSONRPCCoalescedBrowserWaitersGetWildcardCORS(t *testing.T) {
	release := make(chan struct{})
	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		<-release
		jsonRPCWildcardCORSUpstream(t)(w, r)
	})
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	h := newJSONCacheHandler(t, rule)

	origins := []string{"https://a.example", "https://b.example", "https://c.example"}
	responses := make([]*httptest.ResponseRecorder, len(origins))
	var wg sync.WaitGroup
	for i, origin := range origins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, httpRequest := jsonRPCStatusRequest(i+1, origin)
			responses[i] = httptest.NewRecorder()
			h.handleHttpSingle(request, responses[i], httpRequest, p.pool.ServeHTTP, time.Now())
		}()
	}
	require.Eventually(t, func() bool { return hits.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let the other callers join the in-flight fetch
	close(release)
	wg.Wait()

	require.Equal(t, int32(1), hits.Load())
	for i, response := range responses {
		require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"), origins[i])
	}
}

// When cosmoguard owns CORS its policy decides: an origin it does not allow gets no wildcard from
// the cached node answer.
func TestJSONRPCWildcardCORSEntryDefersToCosmoguardCORS(t *testing.T) {
	p, _ := newRealHookCacheProxy(t, nil, jsonRPCWildcardCORSUpstream(t))
	off := false
	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"status"}, Cache: &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off}}
	h := newJSONCacheHandler(t, rule)
	h.cors = compiledTestCORS(t)

	first, firstHTTP := jsonRPCStatusRequest(1, "https://a.example")
	h.handleHttpSingle(first, httptest.NewRecorder(), firstHTTP, p.pool.ServeHTTP, time.Now())
	key := jsonRPCHTTPCacheKey(first, rule.Fingerprint, firstHTTP)
	require.Eventually(t, func() bool {
		_, err := h.cache.Get(t.Context(), key)
		return err == nil
	}, time.Second, time.Millisecond)

	request, httpRequest := jsonRPCStatusRequest(2, "https://not-allowed.example")
	response := httptest.NewRecorder()
	h.handleHttpSingle(request, response, httpRequest, p.pool.ServeHTTP, time.Now())
	require.Equal(t, cacheHit, response.Header().Get(cacheStateHeader))
	require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
}
