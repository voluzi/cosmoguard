package cosmoguard

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitForHTTPTestCacheEntry(t *testing.T, p *HttpProxy, rule *HttpRule, headers http.Header) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	key, err := p.getRequestHash(req, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stored, cacheErr := p.cache.Has(t.Context(), key)
		return cacheErr == nil && stored
	}, time.Second, time.Millisecond)
}

func TestHTTPDefaultKeyMetadataIsolatesSequentialHeights(t *testing.T) {
	tests := []struct {
		name       string
		headerName string
		coalesce   bool
	}{
		{name: "cosmos header coalesced", headerName: "X-Cosmos-Block-Height", coalesce: true},
		{name: "gateway metadata header coalesced", headerName: "Grpc-Metadata-X-Cosmos-Block-Height", coalesce: true},
		{name: "cosmos header streaming", headerName: "X-Cosmos-Block-Height", coalesce: false},
		{name: "gateway metadata header streaming", headerName: "Grpc-Metadata-X-Cosmos-Block-Height", coalesce: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
				height := r.Header.Get(tt.headerName)
				if height == "" {
					height = "absent"
				}
				_, _ = fmt.Fprint(w, height)
			})
			coalesce := tt.coalesce
			rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &coalesce})
			header := func(height string) http.Header {
				if height == "" {
					return nil
				}
				return http.Header{tt.headerName: {height}}
			}

			for _, height := range []string{"100", "200", ""} {
				response, _ := cacheRequest(p, rule, header(height))
				want := height
				if want == "" {
					want = "absent"
				}
				require.Equal(t, want, response.Body.String())
				require.Equal(t, cacheMiss, response.Header().Get(cacheStateHeader))
				waitForHTTPTestCacheEntry(t, p, rule, header(height))
			}

			for _, height := range []string{"100", "200", ""} {
				response, _ := cacheRequest(p, rule, header(height))
				want := height
				if want == "" {
					want = "absent"
				}
				require.Equal(t, want, response.Body.String())
				require.Equal(t, cacheHit, response.Header().Get(cacheStateHeader))
			}
			require.Equal(t, int32(3), hits.Load())
		})
	}
}

func TestHTTPDefaultKeyMetadataSeparatesConcurrentColdMisses(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }

	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		height := r.Header.Get("X-Cosmos-Block-Height")
		started <- height
		<-release
		_, _ = fmt.Fprint(w, height)
	})
	// Register after the proxy so blocked upstream calls are released before
	// the proxy's shutdown cleanup waits for them.
	t.Cleanup(releaseUpstream)
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})

	type result struct {
		height string
		body   string
	}
	results := make(chan result, 2)
	request := func(height string) {
		response, _ := cacheRequest(p, rule, http.Header{"X-Cosmos-Block-Height": {height}})
		results <- result{height: height, body: response.Body.String()}
	}
	go request("100")
	go request("200")

	seen := make(map[string]bool, 2)
	for len(seen) < 2 {
		select {
		case height := <-started:
			seen[height] = true
		case <-time.After(500 * time.Millisecond):
			require.FailNow(t, "different-height upstream calls did not start independently", "started heights: %v", seen)
		}
	}
	releaseUpstream()

	for range 2 {
		select {
		case got := <-results:
			require.Equal(t, got.height, got.body)
		case <-time.After(time.Second):
			require.FailNow(t, "cache request did not complete after releasing upstream")
		}
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPKeyMetadataConfigurationControlsCacheIdentity(t *testing.T) {
	tests := []struct {
		name       string
		cache      *RuleCache
		requests   []http.Header
		wantBodies []string
		wantHits   int32
	}{
		{
			name:  "explicit empty list opts out",
			cache: &RuleCache{Enable: true, TTL: time.Minute, KeyMetadata: []string{}},
			requests: []http.Header{
				{"X-Cosmos-Block-Height": {"100"}, "X-Response-Version": {"v1"}},
				{"X-Cosmos-Block-Height": {"200"}},
			},
			wantBodies: []string{"height=100 version=v1", "height=100 version=v1"},
			wantHits:   1,
		},
		{
			name:  "custom list replaces height defaults",
			cache: &RuleCache{Enable: true, TTL: time.Minute, KeyMetadata: []string{"X-Response-Version"}},
			requests: []http.Header{
				{"X-Cosmos-Block-Height": {"100"}, "X-Response-Version": {"v1"}},
				{"X-Cosmos-Block-Height": {"200"}, "X-Response-Version": {"v1"}},
				{"X-Cosmos-Block-Height": {"200"}, "X-Response-Version": {"v2"}},
			},
			wantBodies: []string{
				"height=100 version=v1",
				"height=100 version=v1",
				"height=200 version=v2",
			},
			wantHits: 2,
		},
		{
			name: "custom list can retain both height aliases",
			cache: &RuleCache{Enable: true, TTL: time.Minute, KeyMetadata: []string{
				"X-Cosmos-Block-Height",
				"Grpc-Metadata-X-Cosmos-Block-Height",
				"X-Response-Version",
			}},
			requests: []http.Header{
				{
					"X-Cosmos-Block-Height":               {"100"},
					"Grpc-Metadata-X-Cosmos-Block-Height": {"100"},
					"X-Response-Version":                  {"v1"},
				},
				{
					"X-Cosmos-Block-Height":               {"100"},
					"Grpc-Metadata-X-Cosmos-Block-Height": {"100"},
					"X-Response-Version":                  {"v2"},
				},
			},
			wantBodies: []string{"height=100 version=v1", "height=100 version=v2"},
			wantHits:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, "height=%s version=%s",
					r.Header.Get("X-Cosmos-Block-Height"),
					r.Header.Get("X-Response-Version"),
				)
			})
			rule := cacheRule(t, tt.cache)
			require.Len(t, tt.requests, len(tt.wantBodies))
			for i, headers := range tt.requests {
				response, _ := cacheRequest(p, rule, headers)
				require.Equal(t, tt.wantBodies[i], response.Body.String())
			}
			require.Equal(t, tt.wantHits, hits.Load())
		})
	}
}

func TestHTTPKeyMetadataHostUsesRequestAuthority(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, r.Host)
	})
	upstreams := *p.pool.upstreams.Load()
	director := upstreams[0].proxy.Director
	upstreams[0].proxy.Director = func(req *http.Request) {
		host := req.Host
		director(req)
		req.Host = host
	}
	rule := cacheRule(t, &RuleCache{
		Enable:      true,
		TTL:         time.Minute,
		KeyMetadata: []string{"hOsT"},
	})
	request := func(host string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		req.Host = host
		p.allow(recorder, req, rule, time.Now())
		return recorder
	}

	first := request("alpha.example")
	second := request("beta.example")
	keyRequest := httptest.NewRequest(http.MethodGet, "/status", nil)
	keyRequest.Host = "alpha.example"
	key, err := p.getRequestHash(keyRequest, rule.Fingerprint, rule.Cache.EffectiveHTTPKeyMetadata())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stored, cacheErr := p.cache.Has(t.Context(), key)
		return cacheErr == nil && stored
	}, time.Second, time.Millisecond)
	repeat := request("alpha.example")

	require.Equal(t, "alpha.example", first.Body.String())
	require.Equal(t, "beta.example", second.Body.String())
	require.Equal(t, "alpha.example", repeat.Body.String())
	require.Equal(t, cacheMiss, first.Header().Get(cacheStateHeader))
	require.Equal(t, cacheMiss, second.Header().Get(cacheStateHeader))
	require.Equal(t, cacheHit, repeat.Header().Get(cacheStateHeader))
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPKeyMetadataPolicyChangeUsesNewNamespace(t *testing.T) {
	p, hits := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, r.Header.Get("X-Response-Version"))
	})
	versionRule := cacheRule(t, &RuleCache{
		Enable:      true,
		TTL:         time.Minute,
		KeyMetadata: []string{"X-Response-Version"},
	})
	otherRule := cacheRule(t, &RuleCache{
		Enable:      true,
		TTL:         time.Minute,
		KeyMetadata: []string{"X-Other-Header"},
	})

	first, _ := cacheRequest(p, versionRule, http.Header{"X-Response-Version": {"v1"}})
	second, _ := cacheRequest(p, otherRule, nil)

	require.Equal(t, "v1", first.Body.String())
	require.Empty(t, second.Body.String())
	require.Equal(t, int32(2), hits.Load())
}

func TestHTTPDefaultKeyMetadataIsolatesStaleRefreshes(t *testing.T) {
	type heightState struct {
		mu       sync.Mutex
		versions map[string]int
	}
	state := heightState{versions: make(map[string]int)}
	refreshStarted := make(chan string, 2)
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }

	p, _ := newRealHookCacheProxy(t, nil, func(w http.ResponseWriter, r *http.Request) {
		height := r.Header.Get("X-Cosmos-Block-Height")
		state.mu.Lock()
		state.versions[height]++
		version := state.versions[height]
		state.mu.Unlock()
		if version > 1 && (height == "100" || height == "200") {
			refreshStarted <- height
			<-releaseRefresh
		}
		_, _ = fmt.Fprintf(w, "%s:v%d", height, version)
	})
	t.Cleanup(release)
	rule := cacheRule(t, &RuleCache{
		Enable:               true,
		TTL:                  5 * time.Second,
		StaleWhileRevalidate: time.Minute,
	})
	base := time.Unix(2_000_000, 0)
	var nowNanos atomic.Int64
	nowNanos.Store(base.UnixNano())
	p.now = func() time.Time { return time.Unix(0, nowNanos.Load()) }
	header := func(height string) http.Header {
		return http.Header{"X-Cosmos-Block-Height": {height}}
	}

	for _, height := range []string{"100", "200"} {
		response, _ := cacheRequest(p, rule, header(height))
		require.Equal(t, height+":v1", response.Body.String())
		require.Equal(t, cacheMiss, response.Header().Get(cacheStateHeader))
	}
	nowNanos.Store(base.Add(6 * time.Second).UnixNano())

	for _, height := range []string{"100", "200"} {
		response, _ := cacheRequest(p, rule, header(height))
		require.Equal(t, height+":v1", response.Body.String())
		require.Equal(t, cacheStale, response.Header().Get(cacheStateHeader))
	}
	seen := make(map[string]bool, 2)
	for len(seen) < 2 {
		select {
		case height := <-refreshStarted:
			seen[height] = true
		case <-time.After(time.Second):
			require.FailNow(t, "height refreshes did not start independently", "started heights: %v", seen)
		}
	}
	for _, height := range []string{"100", "200"} {
		response, _ := cacheRequest(p, rule, header(height))
		require.Equal(t, height+":v1", response.Body.String())
		require.Equal(t, cacheStale, response.Header().Get(cacheStateHeader))
	}
	state.mu.Lock()
	require.Equal(t, map[string]int{"100": 2, "200": 2}, state.versions)
	state.mu.Unlock()
	unprimed, _ := cacheRequest(p, rule, header("300"))
	require.Equal(t, "300:v1", unprimed.Body.String())
	require.Equal(t, cacheMiss, unprimed.Header().Get(cacheStateHeader))

	release()
	for _, height := range []string{"100", "200"} {
		require.Eventually(t, func() bool {
			response, _ := cacheRequest(p, rule, header(height))
			return response.Header().Get(cacheStateHeader) == cacheHit && response.Body.String() == height+":v2"
		}, time.Second, time.Millisecond)
	}
}
