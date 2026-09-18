package cosmoguard

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testResponseCaptureLimit = 32 << 20

type boundedCountingResponseWriter struct {
	header http.Header
	status int
	count  int64
	prefix bytes.Buffer
}

func newBoundedCountingResponseWriter() *boundedCountingResponseWriter {
	return &boundedCountingResponseWriter{header: make(http.Header)}
}

func (w *boundedCountingResponseWriter) Header() http.Header { return w.header }

func (w *boundedCountingResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *boundedCountingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.prefix.Len() < 4096 {
		remaining := 4096 - w.prefix.Len()
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = w.prefix.Write(p[:remaining])
	}
	w.count += int64(len(p))
	return len(p), nil
}

func writeRepeated(t *testing.T, w io.Writer, value byte, count int) {
	t.Helper()
	chunk := bytes.Repeat([]byte{value}, 64<<10)
	for count > 0 {
		n := min(count, len(chunk))
		written, err := w.Write(chunk[:n])
		require.NoError(t, err)
		require.Equal(t, n, written)
		count -= n
	}
}

type failingResponseWriter struct {
	header http.Header
	err    error
}

func (w *failingResponseWriter) Header() http.Header { return w.header }
func (*failingResponseWriter) WriteHeader(int)       {}
func (w *failingResponseWriter) Write(p []byte) (int, error) {
	return len(p) / 2, w.err
}

type shortThenSuccessResponseWriter struct {
	header http.Header
	calls  int
}

func (w *shortThenSuccessResponseWriter) Header() http.Header { return w.header }
func (*shortThenSuccessResponseWriter) WriteHeader(int)       {}
func (w *shortThenSuccessResponseWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return len(p) / 2, nil
	}
	return len(p), nil
}

type switchableShortResponseWriter struct {
	header http.Header
	short  bool
}

func (w *switchableShortResponseWriter) Header() http.Header { return w.header }
func (*switchableShortResponseWriter) WriteHeader(int)       {}
func (w *switchableShortResponseWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) / 2, nil
	}
	return len(p), nil
}

type coalescerWaiterContext struct {
	context.Context
	enrolled chan struct{}
	once     sync.Once
}

func (c *coalescerWaiterContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.enrolled) })
	return c.Context.Done()
}

func TestResponseWriterWrapperResponseCapture(t *testing.T) {
	t.Run("overflow keeps streaming and releases capture", func(t *testing.T) {
		downstream := newBoundedCountingResponseWriter()
		wrapper := WrapResponseWriter(downstream)

		writeRepeated(t, wrapper, 'x', testResponseCaptureLimit+1)
		captured, err := wrapper.GetWrittenBytes()

		require.ErrorIs(t, err, errResponseCaptureTooLarge)
		require.Nil(t, captured)
		require.Equal(t, int64(testResponseCaptureLimit+1), downstream.count)
		require.Zero(t, wrapper.buf.Len())
		require.Zero(t, wrapper.buf.Cap())
	})

	t.Run("exact limit transfers owned storage", func(t *testing.T) {
		downstream := newBoundedCountingResponseWriter()
		wrapper := WrapResponseWriter(downstream)
		writeRepeated(t, wrapper, 'a', testResponseCaptureLimit)
		before := wrapper.buf.Bytes()

		captured, err := wrapper.GetWrittenBytes()

		require.NoError(t, err)
		require.Len(t, captured, testResponseCaptureLimit)
		require.Same(t, &before[0], &captured[0])
		require.Zero(t, wrapper.buf.Len())
		require.Zero(t, wrapper.buf.Cap())
		_, err = wrapper.Write([]byte("later"))
		require.NoError(t, err)
		require.Equal(t, byte('a'), captured[0])
		require.Equal(t, byte('a'), captured[len(captured)-1])
	})

	t.Run("downstream failure invalidates capture", func(t *testing.T) {
		downstreamErr := errors.New("downstream write failed")
		wrapper := WrapResponseWriter(&failingResponseWriter{header: make(http.Header), err: downstreamErr})

		n, err := wrapper.Write([]byte("response"))
		require.Equal(t, 4, n)
		require.ErrorIs(t, err, downstreamErr)
		captured, captureErr := wrapper.GetWrittenBytes()
		require.ErrorIs(t, captureErr, downstreamErr)
		require.Nil(t, captured)
	})

	t.Run("nil-error short write permanently invalidates capture", func(t *testing.T) {
		downstream := &shortThenSuccessResponseWriter{header: make(http.Header)}
		wrapper := WrapResponseWriter(downstream)

		n, err := wrapper.Write([]byte("response"))
		require.Equal(t, 4, n)
		require.ErrorIs(t, err, io.ErrShortWrite)
		captured, captureErr := wrapper.GetWrittenBytes()
		require.ErrorIs(t, captureErr, io.ErrShortWrite)
		require.Nil(t, captured)
		require.Zero(t, wrapper.buf.Len())
		require.Zero(t, wrapper.buf.Cap())

		n, err = wrapper.Write([]byte("later"))
		require.Equal(t, len("later"), n)
		require.NoError(t, err)
		captured, captureErr = wrapper.GetWrittenBytes()
		require.ErrorIs(t, captureErr, io.ErrShortWrite)
		require.Nil(t, captured)
		require.Zero(t, wrapper.buf.Len())
		require.Zero(t, wrapper.buf.Cap())
	})

	t.Run("overflowing write still reports downstream short write", func(t *testing.T) {
		downstream := &switchableShortResponseWriter{header: make(http.Header), short: true}
		wrapper := newResponseWriterWrapper(downstream, 4)

		n, err := wrapper.Write([]byte("response"))
		require.Equal(t, 4, n)
		require.ErrorIs(t, err, io.ErrShortWrite)
		captured, captureErr := wrapper.GetWrittenBytes()
		require.ErrorIs(t, captureErr, errResponseCaptureTooLarge)
		require.Nil(t, captured)
	})

	t.Run("write after capture transfer reports downstream short write", func(t *testing.T) {
		downstream := &switchableShortResponseWriter{header: make(http.Header)}
		wrapper := WrapResponseWriter(downstream)
		_, err := wrapper.Write([]byte("captured"))
		require.NoError(t, err)
		captured, captureErr := wrapper.GetWrittenBytes()
		require.NoError(t, captureErr)
		require.Equal(t, "captured", string(captured))

		downstream.short = true
		n, err := wrapper.Write([]byte("response"))
		require.Equal(t, 4, n)
		require.ErrorIs(t, err, io.ErrShortWrite)
	})
}

func TestResponseWriterWrapperCaptureBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		writes     []string
		want       string
		overflowed bool
	}{
		{name: "empty", writes: []string{""}, want: ""},
		{name: "exact limit", writes: []string{"123", "4567", "890"}, want: "1234567890"},
		{name: "limit plus one", writes: []string{"12345", "67890", "x"}, overflowed: true},
		{name: "single oversized write", writes: []string{"12345678901"}, overflowed: true},
		{name: "writes continue after overflow", writes: []string{"1234567890", "x", "later"}, overflowed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			downstream := newBoundedCountingResponseWriter()
			wrapper := newResponseWriterWrapper(downstream, 10)
			for _, value := range tt.writes {
				n, err := wrapper.Write([]byte(value))
				require.NoError(t, err)
				require.Equal(t, len(value), n)
				require.LessOrEqual(t, wrapper.buf.Len(), 10)
				require.LessOrEqual(t, wrapper.buf.Cap(), 10)
			}

			captured, err := wrapper.GetWrittenBytes()
			if tt.overflowed {
				require.ErrorIs(t, err, errResponseCaptureTooLarge)
				require.Nil(t, captured)
				require.Zero(t, wrapper.buf.Cap())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, string(captured))
			}
			var wantDownstream int64
			for _, value := range tt.writes {
				wantDownstream += int64(len(value))
			}
			require.Equal(t, wantDownstream, downstream.count)
		})
	}
}

func TestResponseWriterWrapperConcurrentCaptureBounds(t *testing.T) {
	const limit = 1 << 20
	for i := range 8 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			downstream := newBoundedCountingResponseWriter()
			wrapper := newResponseWriterWrapper(downstream, limit)

			writeRepeated(t, wrapper, byte('a'+i), 3*limit)
			captured, err := wrapper.GetWrittenBytes()

			require.ErrorIs(t, err, errResponseCaptureTooLarge)
			require.Nil(t, captured)
			require.Zero(t, wrapper.buf.Cap())
			require.Equal(t, int64(3*limit), downstream.count)
		})
	}
}

func TestHTTPResponseCaptureStreamingOverflow(t *testing.T) {
	for _, tt := range []struct {
		name         string
		cacheControl string
	}{
		{name: "otherwise cacheable"},
		{name: "upstream no-store", cacheControl: "no-store"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			off := false
			p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Upstream", "streamed")
				if tt.cacheControl != "" {
					w.Header().Set("Cache-Control", tt.cacheControl)
				}
				w.WriteHeader(http.StatusCreated)
				writeRepeated(t, w, 'h', testResponseCaptureLimit+1)
			})
			capturing := &capturingResponseCache{set: make(chan capturedResponseStore, 1)}
			p.cache = capturing
			rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off})
			w := newBoundedCountingResponseWriter()
			req := httptest.NewRequest(http.MethodGet, "/status", nil)

			p.allow(w, req, rule, time.Now())

			require.Equal(t, http.StatusCreated, w.status)
			require.Equal(t, int64(testResponseCaptureLimit+1), w.count)
			require.Equal(t, "streamed", w.Header().Get("X-Upstream"))
			require.Equal(t, cacheMiss, w.Header().Get(cacheStateHeader))
			require.Equal(t, int32(1), hits.Load())
			select {
			case <-capturing.set:
				t.Fatal("overflowed streaming response must not be cached")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

func TestHTTPResponseCaptureCoalescedOverflow(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(started) })
		<-release
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Encoding", "secret")
		w.Header().Set("Set-Cookie", "private=1")
		w.Header().Set("X-Upstream-Private", "secret")
		writeRepeated(t, w, 'h', testResponseCaptureLimit+1)
	})
	capturing := &capturingResponseCache{set: make(chan capturedResponseStore, 1)}
	p.cache = capturing
	p.cors = &CORSConfig{Enable: true, AllowedOrigins: []string{"https://a.example", "https://b.example"}}
	require.NoError(t, p.cors.Compile())
	rule := cacheRule(t, &RuleCache{Enable: true, TTL: time.Minute})
	type result struct {
		origin string
		w      *boundedCountingResponseWriter
	}
	results := make(chan result, 2)
	run := func(origin string, enrolled chan struct{}) {
		w := newBoundedCountingResponseWriter()
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		req.Header.Set("Origin", origin)
		if enrolled != nil {
			req = req.WithContext(&coalescerWaiterContext{Context: req.Context(), enrolled: enrolled})
		}
		p.allow(w, req, rule, time.Now())
		results <- result{origin: origin, w: w}
	}
	go run("https://a.example", nil)
	<-started
	waiterEnrolled := make(chan struct{})
	go run("https://b.example", waiterEnrolled)
	<-waiterEnrolled
	releaseOnce.Do(func() { close(release) })

	for range 2 {
		got := <-results
		require.Equal(t, http.StatusBadGateway, got.w.status)
		require.Equal(t, "bad gateway\n", got.w.prefix.String())
		require.Equal(t, int64(len("bad gateway\n")), got.w.count)
		require.Equal(t, cacheMiss, got.w.Header().Get(cacheStateHeader))
		require.Equal(t, got.origin, got.w.Header().Get("Access-Control-Allow-Origin"))
		require.Empty(t, got.w.Header().Get("Content-Encoding"))
		require.Empty(t, got.w.Header().Get("Set-Cookie"))
		require.Empty(t, got.w.Header().Get("X-Upstream-Private"))
	}
	require.Equal(t, int32(1), hits.Load())
	select {
	case <-capturing.set:
		t.Fatal("overflowed coalesced response must not be cached")
	case <-time.After(100 * time.Millisecond):
	}
}

func writeLargeJSONRPCResponse(t *testing.T, w http.ResponseWriter, id int) int64 {
	t.Helper()
	prefix := []byte(`{"jsonrpc":"2.0","id":`)
	prefix = append(prefix, byte('0'+id))
	prefix = append(prefix, []byte(`,"result":"`)...)
	suffix := []byte(`"}`)
	_, err := w.Write(prefix)
	require.NoError(t, err)
	writeRepeated(t, w, 'j', testResponseCaptureLimit)
	_, err = w.Write(suffix)
	require.NoError(t, err)
	return int64(len(prefix) + testResponseCaptureLimit + len(suffix))
}

func TestJSONRPCResponseCaptureStreamingOverflow(t *testing.T) {
	for _, tt := range []struct {
		name         string
		cacheControl string
	}{
		{name: "otherwise cacheable"},
		{name: "upstream no-store", cacheControl: "no-store"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			off := false
			rule := &JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
				Cache:   &RuleCache{Enable: true, TTL: time.Minute, Coalesce: &off},
			}
			h := newJSONCacheHandler(t, rule)
			capturing := &capturingJSONCache{set: make(chan capturedJSONStore, 1)}
			h.cache = capturing
			request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
			req, _ := jsonRequestContext()
			w := newBoundedCountingResponseWriter()
			var wantBytes int64

			h.getSingleUpstreamResponse(w, req, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Upstream", "streamed")
				if tt.cacheControl != "" {
					w.Header().Set("Cache-Control", tt.cacheControl)
				}
				w.WriteHeader(http.StatusAccepted)
				wantBytes = writeLargeJSONRPCResponse(t, w, 1)
			}, request.HashWithRule(rule.Fingerprint), rule.Cache, "rule", request.Method)

			require.Equal(t, http.StatusAccepted, w.status)
			require.Equal(t, wantBytes, w.count)
			require.Equal(t, "streamed", w.Header().Get("X-Upstream"))
			require.Equal(t, cacheMiss, w.Header().Get(cacheStateHeader))
			select {
			case <-capturing.set:
				t.Fatal("overflowed JSON-RPC stream must not be cached")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

func TestJSONRPCResponseCaptureCoalescedOverflow(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	h.cors = &CORSConfig{Enable: true, AllowedOrigins: []string{"https://a.example", "https://b.example"}}
	require.NoError(t, h.cors.Compile())
	capturing := &capturingJSONCache{set: make(chan capturedJSONStore, 1)}
	h.cache = capturing
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var calls atomic.Int32
	next := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		w.Header().Set("Content-Encoding", "secret")
		w.Header().Set("Set-Cookie", "private=1")
		w.Header().Set("X-Upstream-Private", "secret")
		writeLargeJSONRPCResponse(t, w, 1)
	}

	type result struct {
		id     int
		origin string
		w      *boundedCountingResponseWriter
	}
	results := make(chan result, 2)
	run := func(id int, origin string, enrolled chan struct{}) {
		request := &JsonRpcMsg{Version: "2.0", ID: id, Method: "status"}
		req, _ := jsonRequestContext()
		req.Header.Set("Origin", origin)
		if enrolled != nil {
			req = req.WithContext(&coalescerWaiterContext{Context: req.Context(), enrolled: enrolled})
		}
		w := newBoundedCountingResponseWriter()
		h.serveSingleMiss(w, req, next, request.HashWithRule(rule.Fingerprint), rule.Cache, "rule", request, time.Now())
		results <- result{id: id, origin: origin, w: w}
	}
	go run(1, "https://a.example", nil)
	<-started
	waiterEnrolled := make(chan struct{})
	go run(2, "https://b.example", waiterEnrolled)
	<-waiterEnrolled
	releaseOnce.Do(func() { close(release) })

	for range 2 {
		got := <-results
		require.Equal(t, http.StatusOK, got.w.status)
		require.Equal(t, cacheMiss, got.w.Header().Get(cacheStateHeader))
		require.Equal(t, got.origin, got.w.Header().Get("Access-Control-Allow-Origin"))
		require.Empty(t, got.w.Header().Get("Content-Encoding"))
		require.Empty(t, got.w.Header().Get("Set-Cookie"))
		require.Empty(t, got.w.Header().Get("X-Upstream-Private"))
		var response struct {
			ID    int `json:"id"`
			Error struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		require.NoError(t, stdjson.Unmarshal(got.w.prefix.Bytes(), &response))
		require.Equal(t, got.id, response.ID)
		require.Equal(t, -32603, response.Error.Code)
		require.Equal(t, "upstream error", response.Error.Message)
	}
	require.Equal(t, int32(1), calls.Load())
	select {
	case <-capturing.set:
		t.Fatal("overflowed coalesced JSON-RPC response must not be cached")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHTTPResponseCaptureRefreshOverflowPreservesStale(t *testing.T) {
	var oversized atomic.Bool
	oversized.Store(true)
	p, hits := newCacheTestProxy(t, 0, func(w http.ResponseWriter, _ *http.Request) {
		if oversized.Load() {
			writeRepeated(t, w, 'r', testResponseCaptureLimit+1)
			return
		}
		_, _ = w.Write([]byte("fresh"))
	})
	base := time.Unix(3_000_000, 0)
	p.now = func() time.Time { return base }
	ruleCache := &RuleCache{Enable: true, TTL: time.Minute, StaleWhileRevalidate: time.Minute}
	key := "refresh-overflow"
	stale := CachedResponse{StatusCode: http.StatusOK, Data: []byte("stale"), StoredAt: base.Add(-90 * time.Second)}
	require.NoError(t, p.cache.Set(t.Context(), key, stale, time.Hour))
	req := httptest.NewRequest(http.MethodGet, "/status", nil)

	p.sf.refresh(key, p.backgroundRefreshFn(req, key, ruleCache, "rule"))
	require.Eventually(t, func() bool {
		_, inflight := p.sf.inflight.Load(key)
		return hits.Load() == 1 && !inflight
	}, 3*time.Second, 5*time.Millisecond)
	stored, err := p.cache.Get(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, stale.Data, stored.Data)
	require.Equal(t, stale.StoredAt, stored.StoredAt)

	oversized.Store(false)
	p.sf.refresh(key, p.backgroundRefreshFn(req, key, ruleCache, "rule"))
	require.Eventually(t, func() bool {
		stored, getErr := p.cache.Get(context.Background(), key)
		return getErr == nil && hits.Load() == 2 && string(stored.Data) == "fresh"
	}, 3*time.Second, 5*time.Millisecond)
}

func TestJSONRPCResponseCaptureRefreshOverflowPreservesStale(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute, StaleWhileRevalidate: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	base := time.Unix(4_000_000, 0)
	h.now = func() time.Time { return base }
	request := &JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}
	hash := request.HashWithRule(rule.Fingerprint)
	key := strconv.FormatUint(hash, 16)
	stale := &JsonRpcMsg{Version: "2.0", ID: 1, Result: []byte(`"stale"`), StoredAt: base.Add(-90 * time.Second)}
	require.NoError(t, h.cache.Set(t.Context(), hash, stale, time.Hour))
	var oversized atomic.Bool
	oversized.Store(true)
	var calls atomic.Int32
	next := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if oversized.Load() {
			writeLargeJSONRPCResponse(t, w, 1)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"fresh"}`))
	}
	req, _ := jsonRequestContext()

	h.sf.refresh(key, h.singleBackgroundRefreshFn(req, next, hash, rule.Cache, "rule", request.Method))
	require.Eventually(t, func() bool {
		_, inflight := h.sf.inflight.Load(key)
		return calls.Load() == 1 && !inflight
	}, 3*time.Second, 5*time.Millisecond)
	stored, err := h.cache.Get(t.Context(), hash)
	require.NoError(t, err)
	require.Equal(t, stale.Result, stored.Result)
	require.Equal(t, stale.StoredAt, stored.StoredAt)

	oversized.Store(false)
	h.sf.refresh(key, h.singleBackgroundRefreshFn(req, next, hash, rule.Cache, "rule", request.Method))
	require.Eventually(t, func() bool {
		stored, getErr := h.cache.Get(context.Background(), hash)
		return getErr == nil && calls.Load() == 2 && string(stored.Result) == `"fresh"`
	}, 3*time.Second, 5*time.Millisecond)
}
