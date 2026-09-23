package cosmoguard

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHTTPResponseIntegrityAdmission(t *testing.T) {
	p := &HttpProxy{}
	for _, tc := range []struct {
		status     int
		cacheError bool
		want       bool
	}{
		{http.StatusOK, false, true},
		{http.StatusOK, true, true},
		{http.StatusPartialContent, false, false},
		{http.StatusPartialContent, true, false},
		{http.StatusNotModified, false, false},
		{http.StatusNotModified, true, false},
		{http.StatusNotFound, false, false},
		{http.StatusNotFound, true, true},
		{http.StatusInternalServerError, false, false},
		{http.StatusInternalServerError, true, false},
	} {
		t.Run(http.StatusText(tc.status)+"/cacheError="+map[bool]string{false: "false", true: "true"}[tc.cacheError], func(t *testing.T) {
			require.Equal(t, tc.want, p.shouldStore(tc.status, nil, &RuleCache{CacheError: tc.cacheError}))
		})
	}
}

func TestHTTPResponseIntegrityIncompleteResponsesAreNotCached(t *testing.T) {
	for _, tc := range []struct {
		name       string
		headerName string
		headerVal  string
		status     int
		body       string
	}{
		{"range", "Range", "bytes=0-2", http.StatusPartialContent, "abc"},
		{"conditional", "If-None-Match", `"old"`, http.StatusNotModified, ""},
	} {
		for _, coalesce := range []bool{false, true} {
			t.Run(tc.name+"/coalesce="+map[bool]string{false: "false", true: "true"}[coalesce], func(t *testing.T) {
				p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get(tc.headerName) != "" {
						if tc.status == http.StatusPartialContent {
							w.Header().Set("Content-Range", "bytes 0-2/8")
						} else {
							w.Header().Set("ETag", `"old"`)
						}
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
						return
					}
					_, _ = io.WriteString(w, "complete")
				})
				rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce, CacheError: true})
				first, _ := cacheRequest(p, rule, http.Header{tc.headerName: {tc.headerVal}})
				require.Equal(t, tc.status, first.Code)
				require.Equal(t, tc.body, first.Body.String())
				if tc.status == http.StatusPartialContent {
					require.Equal(t, "bytes 0-2/8", first.Header().Get("Content-Range"))
				} else {
					require.Equal(t, `"old"`, first.Header().Get("ETag"))
				}
				key, err := p.getRequestHash(httptest.NewRequest(http.MethodGet, "/status", nil), rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
				require.NoError(t, err)
				_, staged := p.pendingMisses.Load(key)
				require.False(t, staged)
				second, _ := cacheRequest(p, rule, nil)
				require.Equal(t, http.StatusOK, second.Code)
				require.Equal(t, "complete", second.Body.String())
				require.Equal(t, int32(2), hits.Load())
				waitForHTTPTestCacheEntry(t, p, rule, nil)
				third, _ := cacheRequest(p, rule, nil)
				require.Equal(t, "complete", third.Body.String())
				require.Equal(t, int32(2), hits.Load())
			})
		}
	}
}

func TestHTTPResponseIntegrityIncompleteResponseIsNotShared(t *testing.T) {
	for _, tc := range []struct {
		name       string
		headerName string
		headerVal  string
		status     int
	}{
		{"range", "Range", "bytes=0-2", http.StatusPartialContent},
		{"conditional", "If-None-Match", `"old"`, http.StatusNotModified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(tc.headerName) != "" {
					close(started)
					<-release
					w.WriteHeader(tc.status)
					if tc.status == http.StatusPartialContent {
						_, _ = io.WriteString(w, "abc")
					}
					return
				}
				_, _ = io.WriteString(w, "complete")
			})
			rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, CacheError: true})
			owner := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				res, _ := cacheRequest(p, rule, http.Header{tc.headerName: {tc.headerVal}})
				owner <- res
			}()
			<-started
			observed := make(chan struct{})
			waiter := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/status", nil)
				r = r.WithContext(&observedDoneContext{Context: t.Context(), observed: observed})
				p.allow(w, r, rule, time.Now())
				waiter <- w
			}()
			<-observed
			close(release)
			require.Equal(t, tc.status, (<-owner).Code)
			got := <-waiter
			require.Equal(t, http.StatusOK, got.Code)
			require.Equal(t, "complete", got.Body.String())
			require.Equal(t, int32(2), hits.Load())
		})
	}
}

func TestHTTPResponseIntegrityAbortedUpstream(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cache    bool
		coalesce bool
	}{
		{"direct", false, false},
		{"streaming miss", true, false},
		{"coalesced miss", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var healthy atomic.Bool
			p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
				if healthy.Load() {
					_, _ = io.WriteString(w, "complete")
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "partial")
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			})
			if tc.cache {
				rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &tc.coalesce})
				p.SetRules([]*HttpRule{rule}, RuleActionDeny)
			} else {
				p.SetRules(nil, RuleActionAllow)
			}
			server := httptest.NewServer(p)
			t.Cleanup(server.Close)
			client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
			t.Cleanup(client.CloseIdleConnections)
			res, err := client.Get(server.URL + "/status")
			if tc.coalesce {
				require.Error(t, err)
				require.Nil(t, res)
			} else if err != nil {
				require.Nil(t, res)
			} else {
				defer res.Body.Close()
				body, readErr := io.ReadAll(res.Body)
				require.Equal(t, "partial", string(body))
				require.Error(t, readErr)
			}
			if tc.cache {
				rule := p.rules[0]
				probeReq := httptest.NewRequest(http.MethodGet, server.URL+"/status", nil)
				probeReq.Header.Set("Accept-Encoding", "gzip")
				key, hashErr := p.getRequestHash(probeReq, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
				require.NoError(t, hashErr)
				_, staged := p.pendingMisses.Load(key)
				require.False(t, staged)
				stored, cacheErr := p.cache.Has(t.Context(), key)
				require.NoError(t, cacheErr)
				require.False(t, stored)
			}
			healthy.Store(true)
			res, err = client.Get(server.URL + "/status")
			require.NoError(t, err)
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Equal(t, "complete", string(body))
			require.Equal(t, int32(2), hits.Load())
			if tc.cache {
				probeReq := httptest.NewRequest(http.MethodGet, server.URL+"/status", nil)
				probeReq.Header.Set("Accept-Encoding", "gzip")
				waitForHTTPTestCacheRequest(t, p, p.rules[0], probeReq)
			}
		})
	}
}

func TestHTTPResponseIntegrityAbortedRefresh(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Second, StaleWhileRevalidate: time.Minute})
	request := httptest.NewRequest(http.MethodGet, "/status", nil)
	key, err := p.getRequestHash(request, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	stale := CachedResponse{StatusCode: http.StatusOK, Data: []byte("stale"), StoredAt: time.Now().Add(-2 * time.Second)}
	require.NoError(t, p.cache.Set(t.Context(), key, stale, time.Minute))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.sf.refresh(key, p.backgroundRefreshFn(r, key, rule.Cache, "test"))
		_, _ = io.WriteString(w, "stale")
	}))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 2 * time.Second}
	res, err := client.Get(server.URL + "/status")
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Body.Close() })
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "stale", string(body))
	require.Eventually(t, func() bool {
		_, active := p.sf.inflight.Load(key)
		return !active && hits.Load() == 1
	}, time.Second, time.Millisecond)
	cached, err := p.cache.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, stale.Data, cached.Data)
	require.Equal(t, stale.StoredAt.Unix(), cached.StoredAt.Unix())
	_, staged := p.pendingMisses.Load(key)
	require.False(t, staged)
	require.NotContains(t, logs.String(), "panic in background cache refresh")
}
