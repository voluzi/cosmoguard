package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cachepkg "github.com/voluzi/cosmoguard/pkg/cache"
	"github.com/voluzi/cosmoguard/pkg/util"
)

type batchIDProbeCache struct {
	gets atomic.Int32
	sets atomic.Int32
}

func (c *batchIDProbeCache) Get(context.Context, uint64) (*JsonRpcMsg, error) {
	c.gets.Add(1)
	return nil, cachepkg.ErrNotFound
}

func (c *batchIDProbeCache) Set(context.Context, uint64, *JsonRpcMsg, time.Duration) error {
	c.sets.Add(1)
	return nil
}

func (*batchIDProbeCache) Has(context.Context, uint64) (bool, error) { return false, nil }
func (*batchIDProbeCache) Close() error                              { return nil }

type batchIDProbeLimiter struct{ calls atomic.Int32 }

func (l *batchIDProbeLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	l.calls.Add(1)
	return true, 0, nil
}

func (*batchIDProbeLimiter) Close() error { return nil }

func parseBatchIDs(t *testing.T, body string) JsonRpcMsgs {
	t.Helper()
	single, batch, err := ParseJsonRpcMessage([]byte(body))
	require.NoError(t, err)
	require.Nil(t, single)
	require.NotNil(t, batch)
	return batch
}

func TestHandleHTTPBatchRejectsDuplicateIDsBeforeWork(t *testing.T) {
	cors := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://app.example"}}
	require.NoError(t, cors.Compile())

	tests := []struct {
		name string
		body string
	}{
		{name: "numeric", body: `[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":1,"method":"b"}]`},
		{name: "string", body: `[{"jsonrpc":"2.0","id":"same","method":"a"},{"jsonrpc":"2.0","id":"same","method":"b"}]`},
		{name: "decoded equivalent strings", body: `[{"jsonrpc":"2.0","id":"a","method":"a"},{"jsonrpc":"2.0","id":"\u0061","method":"b"}]`},
		{name: "explicit null", body: `[{"jsonrpc":"2.0","id":null,"method":"a"},{"jsonrpc":"2.0","id":null,"method":"b"}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probeCache := &batchIDProbeCache{}
			probeLimiter := &batchIDProbeLimiter{}
			rule := &JsonRpcRule{
				Action:    RuleActionAllow,
				Methods:   []string{"a", "b"},
				Cache:     &RuleCache{Enable: true, TTL: time.Minute},
				RateLimit: &RateLimitConfig{},
			}
			require.NoError(t, rule.Compile())
			h := &JsonRpcHandler{
				log:           log.WithField("test", t.Name()),
				cache:         probeCache,
				cgDashboard:   newDashboardObservability(),
				section:       "rpc.jsonrpc",
				now:           time.Now,
				defaultAction: RuleActionDeny,
				rules:         []*JsonRpcRule{rule},
				limiters:      map[uint64]RateLimiter{rule.Fingerprint: probeLimiter},
				cors:          cors,
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Header.Set("Origin", "https://app.example")
			upstreamCalls := 0

			h.handleHttpBatch(parseBatchIDs(t, tt.body), recorder, request, func(http.ResponseWriter, *http.Request) {
				upstreamCalls++
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
			require.Equal(t, "https://app.example", recorder.Header().Get("Access-Control-Allow-Origin"))
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())
			require.Zero(t, upstreamCalls)
			require.Zero(t, probeCache.gets.Load())
			require.Zero(t, probeCache.sets.Load())
			require.Zero(t, probeLimiter.calls.Load())
		})
	}
}

func TestHandleHTTPPreservesOversizedBatchPrecedence(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("test", t.Name()),
		cgDashboard:   newDashboardObservability(),
		defaultAction: RuleActionAllow,
		maxBatchSize:  1,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":1,"method":"b"}]`,
	))

	h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized duplicate batch reached upstream")
	}, time.Now())

	require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)
}

func TestJsonRpcResponsesSetCorrelatesOnlyUnambiguousIDs(t *testing.T) {
	requests := parseBatchIDs(t, `[
		{"jsonrpc":"2.0","id":1,"method":"numeric"},
		{"jsonrpc":"2.0","id":"1","method":"string"},
		{"jsonrpc":"2.0","id":null,"method":"null"},
		{"jsonrpc":"2.0","method":"notification"}
	]`)
	responses := parseBatchIDs(t, `[
		{"jsonrpc":"2.0","id":null,"result":"null"},
		{"jsonrpc":"2.0","id":"1","result":"string"},
		{"jsonrpc":"2.0","id":1,"result":"numeric"}
	]`)
	correlated := JsonRpcResponses{}
	for _, request := range requests {
		correlated.AddPending(request)
	}

	correlated.Set(requests, responses)

	require.JSONEq(t, `"numeric"`, string(correlated.Find(requests[0]).Response.Result))
	require.JSONEq(t, `"string"`, string(correlated.Find(requests[1]).Response.Result))
	require.JSONEq(t, `"null"`, string(correlated.Find(requests[2]).Response.Result))
	require.Nil(t, correlated.Find(requests[3]).Response)
}

func TestJsonRpcResponsesSetLeavesDuplicateRequestIDsPending(t *testing.T) {
	tests := []struct {
		name       string
		firstID    any
		secondID   any
		responseID any
	}{
		{name: "numeric", firstID: int(7), secondID: float64(7), responseID: int64(7)},
		{name: "string", firstID: "same", secondID: "same", responseID: "same"},
		{name: "explicit null", firstID: explicitNullID, secondID: explicitNullID, responseID: explicitNullID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := JsonRpcMsgs{
				{Version: "2.0", ID: tt.firstID, Method: "first"},
				{Version: "2.0", ID: tt.secondID, Method: "second"},
				{Version: "2.0", ID: "unique", Method: "unique"},
			}
			correlated := JsonRpcResponses{}
			for _, request := range requests {
				correlated.AddPending(request)
			}

			correlated.Set(requests, JsonRpcMsgs{
				{Version: "2.0", ID: tt.responseID, Result: []byte(`"ambiguous"`)},
				{Version: "2.0", ID: "unique", Result: []byte(`"resolved"`)},
			})

			require.Nil(t, correlated.Find(requests[0]).Response)
			require.Nil(t, correlated.Find(requests[1]).Response)
			require.JSONEq(t, `"resolved"`, string(correlated.Find(requests[2]).Response.Result))

			correlated.FillUnansweredCalls()
			require.Equal(t, -32603, correlated.Find(requests[0]).Response.Error.Code)
			require.Equal(t, -32603, correlated.Find(requests[1]).Response.Error.Code)
			require.JSONEq(t, `"resolved"`, string(correlated.Find(requests[2]).Response.Result))
		})
	}
}

func TestJsonRpcResponsesSetLeavesMalformedResponsesPending(t *testing.T) {
	requests := JsonRpcMsgs{
		{Version: "2.0", ID: 1, Method: "duplicate"},
		{Version: "2.0", ID: 2, Method: "missing"},
		{Version: "2.0", ID: 3, Method: "resolved"},
	}
	correlated := JsonRpcResponses{}
	for _, request := range requests {
		correlated.AddPending(request)
	}
	preserved := &JsonRpcMsg{Version: "2.0", ID: 3, Result: []byte(`"preserved"`)}
	correlated.Find(requests[2]).Response = preserved

	correlated.Set(requests, JsonRpcMsgs{
		{Version: "2.0", ID: int64(1), Result: []byte(`"first"`)},
		{Version: "2.0", ID: float64(1), Result: []byte(`"second"`)},
		nil,
		{Version: "2.0", Result: []byte(`"absent-id"`)},
		{Version: "2.0", ID: 99, Result: []byte(`"unknown-id"`)},
		{Version: "2.0", ID: 3, Result: []byte(`"replacement"`)},
	})

	require.Nil(t, correlated.Find(requests[0]).Response)
	require.Nil(t, correlated.Find(requests[1]).Response)
	require.Same(t, preserved, correlated.Find(requests[2]).Response)
	correlated.FillUnansweredCalls()
	require.Equal(t, -32603, correlated.Find(requests[0]).Response.Error.Code)
	require.Equal(t, -32603, correlated.Find(requests[1]).Response.Error.Code)
}

func TestMalformedUpstreamBatchIDsDoNotPoisonRequestCaches(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		secondCache bool
	}{
		{
			name:     "duplicate",
			response: `[{"jsonrpc":"2.0","id":1,"result":"first"},{"jsonrpc":"2.0","id":1,"result":"poison"}]`,
		},
		{
			name:        "missing",
			response:    `[{"jsonrpc":"2.0","result":"poison"},{"jsonrpc":"2.0","id":2,"result":"second"}]`,
			secondCache: true,
		},
		{
			name:        "mismatched",
			response:    `[{"jsonrpc":"2.0","id":99,"result":"poison"},{"jsonrpc":"2.0","id":2,"result":"second"}]`,
			secondCache: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"first", "second"},
				Cache:   &RuleCache{Enable: true, TTL: time.Minute, CacheEmptyResult: true},
			}
			h := newJSONCacheHandler(t, rule)
			requests := JsonRpcMsgs{
				{Version: "2.0", ID: 1, Method: "first"},
				{Version: "2.0", ID: 2, Method: "second"},
			}
			recorder := httptest.NewRecorder()

			h.handleHttpBatch(requests, recorder, httptest.NewRequest(http.MethodPost, "/", nil), func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.response))
			}, time.Now())

			_, err := h.cache.Get(context.Background(), requests[0].HashWithRule(rule.Fingerprint))
			require.ErrorIs(t, err, cachepkg.ErrNotFound)
			second, err := h.cache.Get(context.Background(), requests[1].HashWithRule(rule.Fingerprint))
			if !tt.secondCache {
				require.ErrorIs(t, err, cachepkg.ErrNotFound)
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, `"second"`, string(second.Result))
		})
	}
}

func TestNormalizeJsonRpcIDTreatsIntegralRepresentationsEqually(t *testing.T) {
	tests := []struct {
		name string
		id   any
		want int64
	}{
		{name: "int", id: int(7), want: 7},
		{name: "int8", id: int8(7), want: 7},
		{name: "int16", id: int16(7), want: 7},
		{name: "int32", id: int32(7), want: 7},
		{name: "int64", id: int64(7), want: 7},
		{name: "uint", id: uint(7), want: 7},
		{name: "uint8", id: uint8(7), want: 7},
		{name: "uint16", id: uint16(7), want: 7},
		{name: "uint32", id: uint32(7), want: 7},
		{name: "uint64", id: uint64(7), want: 7},
		{name: "uintptr", id: uintptr(7), want: 7},
		{name: "negative int", id: int(-7), want: -7},
		{name: "negative int8", id: int8(-7), want: -7},
		{name: "negative int16", id: int16(-7), want: -7},
		{name: "negative int32", id: int32(-7), want: -7},
		{name: "negative int64", id: int64(-7), want: -7},
		{name: "integral float64", id: float64(7), want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeJsonRpcID(tt.id))
		})
	}
	require.NotEqual(t, normalizeJsonRpcID(7), normalizeJsonRpcID("7"))
}

func TestNormalizeJsonRpcIDPreservesLargeUnsignedValues(t *testing.T) {
	high := uint64(1) << 63
	require.Equal(t, high, normalizeJsonRpcID(high))
	require.NotEqual(t, normalizeJsonRpcID(high), normalizeJsonRpcID(int64(-1<<63)))
	if strconv.IntSize == 64 {
		require.Equal(t, high, normalizeJsonRpcID(uint(high)))
	}
}

func TestIntegralIDKindsShareDuplicateAndCorrelationKeys(t *testing.T) {
	tests := []struct {
		name string
		id   any
	}{
		{name: "int", id: int(7)},
		{name: "int8", id: int8(7)},
		{name: "int16", id: int16(7)},
		{name: "int32", id: int32(7)},
		{name: "uint", id: uint(7)},
		{name: "uint8", id: uint8(7)},
		{name: "uint16", id: uint16(7)},
		{name: "uint32", id: uint32(7)},
		{name: "uint64", id: uint64(7)},
		{name: "uintptr", id: uintptr(7)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := JsonRpcMsgs{
				{Version: "2.0", ID: int64(7), Method: "first"},
				{Version: "2.0", ID: tt.id, Method: "second"},
			}
			require.True(t, hasDuplicateJsonRpcIDs(requests))

			correlated := JsonRpcResponses{}
			correlated.AddPending(requests[0])
			correlated.AddPending(requests[1])
			correlated.Set(requests, JsonRpcMsgs{
				{Version: "2.0", ID: int64(7), Result: []byte(`"ambiguous"`)},
			})

			require.Nil(t, correlated.Find(requests[0]).Response)
			require.Nil(t, correlated.Find(requests[1]).Response)
		})
	}
}

func TestRejectedDuplicateBatchDoesNotDisruptSingleMissCoalescing(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	h.handleHttpBatch(
		JsonRpcMsgs{{Version: "2.0", ID: 1, Method: "status"}, {Version: "2.0", ID: 1, Method: "status"}},
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/", nil),
		func(http.ResponseWriter, *http.Request) { t.Fatal("rejected batch reached upstream") },
		time.Now(),
	)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	secondUpstream := make(chan struct{}, 1)
	var upstreamCalls atomic.Int32
	next := func(w http.ResponseWriter, _ *http.Request) {
		if upstreamCalls.Add(1) == 1 {
			started <- struct{}{}
			<-release
		} else {
			secondUpstream <- struct{}{}
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
	}

	results := make(chan *httptest.ResponseRecorder, 2)
	invoke := func(id int) {
		recorder := httptest.NewRecorder()
		h.handleHttpSingle(
			&JsonRpcMsg{Version: "2.0", ID: id, Method: "status"},
			recorder,
			httptest.NewRequest(http.MethodPost, "/", nil),
			next,
			time.Now(),
		)
		results <- recorder
	}
	go invoke(1)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first single miss did not reach upstream")
	}
	go invoke(2)
	select {
	case <-secondUpstream:
		t.Fatal("concurrent single miss was not coalesced")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	for range 2 {
		recorder := <-results
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Contains(t, recorder.Body.String(), `"result":"ok"`)
	}
	require.Equal(t, int32(1), upstreamCalls.Load())
}

func TestRejectedHTTPBatchCannotPoisonCacheReadByWebSocket(t *testing.T) {
	responseCache, err := newResponseCache[uint64, *JsonRpcMsg](nil, nil, t.Name(), CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, responseCache.Close()) })
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status", "health"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	require.NoError(t, rule.Compile())
	h := &JsonRpcHandler{
		log:           log.WithField("test", t.Name()),
		cache:         responseCache,
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		now:           time.Now,
		defaultAction: RuleActionDeny,
		rules:         []*JsonRpcRule{rule},
	}
	h.handleHttpBatch(
		JsonRpcMsgs{{Version: "2.0", ID: 1, Method: "status"}, {Version: "2.0", ID: 1, Method: "health"}},
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/", nil),
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`[{"jsonrpc":"2.0","id":1,"result":"status"},{"jsonrpc":"2.0","id":1,"result":"health-poison"}]`))
		},
		time.Now(),
	)

	upstream := &wsCacheUpstream{makeResponse: func(request *JsonRpcMsg, _ int32) (*JsonRpcMsg, error) {
		return WithResult(request, "ws-fresh"), nil
	}}
	constructor := func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return upstream }
	proxy, err := NewJsonRpcWebSocketProxy(
		t.Name(), []string{"ws://upstream.invalid"}, "/websocket", 1, constructor, responseCache, false, nil,
	)
	require.NoError(t, err)
	proxy.log = log.WithField("test", t.Name())
	proxy.cgDashboard = newDashboardObservability()
	proxy.section = "rpc.jsonrpc"
	proxy.SetRules([]*JsonRpcRule{rule}, RuleActionDeny, nil)
	client, peer := newWSCacheClient(t)

	require.NoError(t, proxy.handleRequest(client, &JsonRpcMsg{Version: "2.0", ID: 99, Method: "status"}, "127.0.0.1", nil))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	var response JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&response))
	require.Equal(t, 99, response.ID)
	require.JSONEq(t, `"ws-fresh"`, string(response.Result))
	require.Equal(t, int32(1), upstream.calls.Load())
}
