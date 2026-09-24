package compat

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"
)

// fakeHeightNode answers with the height it was asked for, as a pinned
// query whose result changes every block does. It also states that height.
func fakeHeightNode(t *testing.T, sameEveryHeight bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("x-cosmos-block-height")
		w.Header().Set("Grpc-Metadata-X-Cosmos-Block-Height", h)
		if sameEveryHeight {
			h = "const"
		}
		fmt.Fprintf(w, `{"pool":"%s"}`, h)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeCache fronts upstream with a cache of the given TTL. keyOnHeight
// false is the defect the probe exists to catch.
func fakeCache(t *testing.T, upstream string, keyOnHeight bool, ttl time.Duration) *httptest.Server {
	t.Helper()
	type entry struct {
		body   []byte
		stored time.Time
	}
	var mu sync.Mutex
	cache := map[string]entry{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		if keyOnHeight {
			key += "|" + r.Header.Get("x-cosmos-block-height")
		}
		mu.Lock()
		e, hit := cache[key]
		mu.Unlock()
		if hit && time.Since(e.stored) < ttl {
			_, _ = w.Write(e.body) // a hit states no height, like cosmoguard's
			return
		}
		req, _ := http.NewRequest(http.MethodGet, upstream+r.URL.Path, nil)
		req.Header.Set("x-cosmos-block-height", r.Header.Get("x-cosmos-block-height"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		mu.Lock()
		cache[key] = entry{body, time.Now()}
		mu.Unlock()
		w.Header().Set("Grpc-Metadata-X-Cosmos-Block-Height", resp.Header.Get("Grpc-Metadata-X-Cosmos-Block-Height"))
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func runCrossHeight(t *testing.T, node, guard string) Result {
	t.Helper()
	h := newHTTPDoer(5 * time.Second)
	// The round delay outlasts the cache TTL, as with make compat.
	task := crossHeightTask("probe", 100, 40*time.Millisecond, func(ctx context.Context, toGuard bool, hh int64) Response {
		base := node
		if toGuard {
			base = guard
		}
		return h.do(ctx, http.MethodGet, base+"/pool", nil, map[string]string{"x-cosmos-block-height": strconv.FormatInt(hh, 10)})
	})
	return task(t.Context())
}

func TestCrossHeightCatchesHeightBlindCache(t *testing.T) {
	node := fakeHeightNode(t, false)

	res := runCrossHeight(t, node.URL, fakeCache(t, node.URL, true, 20*time.Millisecond).URL)
	assert.Equal(t, res.Class, Identical, res.Detail)

	res = runCrossHeight(t, node.URL, fakeCache(t, node.URL, false, 20*time.Millisecond).URL)
	assert.Equal(t, res.Class, Differs, res.Detail)
	assert.Assert(t, res.Detail != "", "the difference is explained")
}

func TestCrossHeightSkipsHeightStableAnswers(t *testing.T) {
	node := fakeHeightNode(t, true)
	res := runCrossHeight(t, node.URL, fakeCache(t, node.URL, false, 20*time.Millisecond).URL)
	assert.Equal(t, res.Class, Skipped, res.Detail)
}
