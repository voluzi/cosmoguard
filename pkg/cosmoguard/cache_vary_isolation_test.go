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

	hash := firstRequest.HashWithRule(rule.Fingerprint)
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
