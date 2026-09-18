package cosmoguard

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func informationalCacheRule(t *testing.T, cache *RuleCache) *HttpRule {
	t.Helper()
	rule := &HttpRule{
		Priority: 1,
		Action:   RuleActionAllow,
		Paths:    []string{"/status"},
		Methods:  []string{http.MethodPost},
		Cache:    cache,
	}
	require.NoError(t, rule.Compile())
	return rule
}

func informationalPOSTRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"request":"same"}`))
	require.NoError(t, err)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Accept-Encoding", "identity")
	return req
}

func doInformationalPOST(t *testing.T, client *http.Client, url string) (*http.Response, string) {
	t.Helper()
	resp, err := client.Do(informationalPOSTRequest(t, url))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, string(body)
}

func informationalCacheKey(t *testing.T, p *HttpProxy, rule *HttpRule, url string) string {
	t.Helper()
	req := informationalPOSTRequest(t, url)
	key, err := p.getRequestHash(req, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	return key
}

func TestHTTPFinalUncacheableHeaderAfterInformationalResponse(t *testing.T) {
	for _, coalesce := range []bool{true, false} {
		for _, directive := range []string{"no-store", "private"} {
			t.Run(fmt.Sprintf("coalesce=%t/%s", coalesce, directive), func(t *testing.T) {
				var hits atomic.Int32
				p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
					hit := hits.Add(1)
					w.Header().Set("X-Upstream-Phase", "informational")
					w.WriteHeader(http.StatusContinue)
					w.Header().Del("X-Upstream-Phase")
					w.Header().Set("Cache-Control", directive)
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"hit":%d}`, hit)
				})
				rule := informationalCacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce})
				proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					p.allow(w, r, rule, time.Now())
				}))
				t.Cleanup(proxyServer.Close)
				url := proxyServer.URL + "/status"
				key := informationalCacheKey(t, p, rule, url)

				first, firstBody := doInformationalPOST(t, proxyServer.Client(), url)
				require.Equal(t, directive, first.Header.Get("Cache-Control"))
				require.Empty(t, first.Header.Get("X-Upstream-Phase"))
				require.Never(t, func() bool {
					stored, err := p.cache.Has(t.Context(), key)
					return err == nil && stored
				}, 100*time.Millisecond, 5*time.Millisecond)

				second, secondBody := doInformationalPOST(t, proxyServer.Client(), url)
				require.Equal(t, directive, second.Header.Get("Cache-Control"))
				require.NotEqual(t, firstBody, secondBody)
				require.Equal(t, int32(2), hits.Load())
			})
		}
	}
}

func TestHTTPCacheableFinalHeadersAfterInformationalResponse(t *testing.T) {
	for _, coalesce := range []bool{true, false} {
		t.Run(fmt.Sprintf("coalesce=%t", coalesce), func(t *testing.T) {
			var hits atomic.Int32
			p, _ := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
				hit := hits.Add(1)
				w.Header().Set("X-Upstream-Phase", "informational")
				w.WriteHeader(http.StatusContinue)
				w.Header().Del("X-Upstream-Phase")
				w.Header().Set("Cache-Control", "public, max-age=60")
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Final-Response", fmt.Sprintf("hit-%d", hit))
				_, _ = fmt.Fprintf(w, `{"hit":%d}`, hit)
			})
			rule := informationalCacheRule(t, &RuleCache{
				Enable:          true,
				TTL:             time.Minute,
				Coalesce:        &coalesce,
				PreserveHeaders: []string{"X-Final-Response"},
			})
			proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p.allow(w, r, rule, time.Now())
			}))
			t.Cleanup(proxyServer.Close)
			url := proxyServer.URL + "/status"
			key := informationalCacheKey(t, p, rule, url)

			first, firstBody := doInformationalPOST(t, proxyServer.Client(), url)
			require.Eventually(t, func() bool {
				stored, err := p.cache.Has(t.Context(), key)
				return err == nil && stored
			}, time.Second, 5*time.Millisecond)
			second, secondBody := doInformationalPOST(t, proxyServer.Client(), url)

			for _, response := range []*http.Response{first, second} {
				require.Equal(t, "public, max-age=60", response.Header.Get("Cache-Control"))
				require.Equal(t, "application/json", response.Header.Get("Content-Type"))
				require.Equal(t, "hit-1", response.Header.Get("X-Final-Response"))
				require.Empty(t, response.Header.Get("X-Upstream-Phase"))
			}
			require.Equal(t, firstBody, secondBody)
			require.Equal(t, int32(1), hits.Load())
		})
	}
}

func TestJSONRPCFinalUncacheableHeaderAfterInformationalResponse(t *testing.T) {
	for _, coalesce := range []bool{true, false} {
		for _, directive := range []string{"no-store", "private"} {
			t.Run(fmt.Sprintf("coalesce=%t/%s", coalesce, directive), func(t *testing.T) {
				rule := &JsonRpcRule{
					Action:  RuleActionAllow,
					Methods: []string{"status"},
					Cache:   &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce},
				}
				h := newJSONCacheHandler(t, rule)
				capturing := &capturingJSONCache{set: make(chan capturedJSONStore, 1)}
				h.cache = capturing
				request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
				req, _ := jsonRequestContext()
				next := func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusContinue)
					w.Header().Set("Cache-Control", directive)
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
				}

				if coalesce {
					response, err := h.fetchSingle(req, next, request.HashWithRule(rule.Fingerprint), rule.Cache, "rule", request.Method, &jsonRpcResponseOwner{}, false)
					require.NoError(t, err)
					require.False(t, response.Shareable)
				} else {
					h.getSingleUpstreamResponse(newInformationalResponseRecorder(), req, next, request.HashWithRule(rule.Fingerprint), rule.Cache, "rule", request.Method)
				}
				assertNoJSONStore(t, capturing)
			})
		}
	}
}

func TestJSONRPCCacheableFinalResponseAfterInformationalResponse(t *testing.T) {
	for _, coalesce := range []bool{true, false} {
		t.Run(fmt.Sprintf("coalesce=%t", coalesce), func(t *testing.T) {
			rule := &JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
				Cache:   &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce},
			}
			h := newJSONCacheHandler(t, rule)
			request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
			hash := request.HashWithRule(rule.Fingerprint)
			var hits atomic.Int32
			next := func(w http.ResponseWriter, _ *http.Request) {
				hit := hits.Add(1)
				w.WriteHeader(http.StatusContinue)
				w.Header().Set("Cache-Control", "public, max-age=60")
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"hit-%d"}`, hit)
			}
			req, _ := jsonRequestContext()

			if coalesce {
				response, err := h.fetchSingle(req, next, hash, rule.Cache, "rule", request.Method, &jsonRpcResponseOwner{}, false)
				require.NoError(t, err)
				require.True(t, response.Shareable)
			} else {
				h.getSingleUpstreamResponse(newInformationalResponseRecorder(), req, next, hash, rule.Cache, "rule", request.Method)
			}
			require.Eventually(t, func() bool {
				_, err := h.cache.Get(t.Context(), hash)
				return err == nil
			}, time.Second, 5*time.Millisecond)

			replay := httptest.NewRecorder()
			replayRequest := &JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}
			replayHTTP, _ := jsonRequestContext()
			h.handleHttpSingle(replayRequest, replay, replayHTTP, func(http.ResponseWriter, *http.Request) {
				t.Fatal("cache replay reached upstream")
			}, time.Now())

			require.Equal(t, cacheHit, replay.Header().Get(cacheStateHeader))
			require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":"hit-1"}`, replay.Body.String())
			require.Equal(t, int32(1), hits.Load())
		})
	}
}
