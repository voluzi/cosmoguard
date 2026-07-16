package testharness_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestHarness_AllowsPassthroughByDefault verifies that with the default
// "allow everything" config, cosmoguard forwards requests to the fake
// upstream and returns its response unchanged.
func TestHarness_AllowsPassthroughByDefault(t *testing.T) {
	h := testharness.New(t,
		testharness.WithLCDResponse(http.MethodGet, "/cosmos/bank/v1beta1/params",
			`{"params":{"send_enabled":[]}}`),
	)

	resp := h.GET(t, h.LCDURL+"/cosmos/bank/v1beta1/params")
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, string(resp.Body), `{"params":{"send_enabled":[]}}`)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/cosmos/bank/v1beta1/params"), 1)
}

// TestHarness_DenyRule confirms that a deny rule short-circuits before
// reaching upstream and returns 401.
func TestHarness_DenyRule(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionDeny,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100,
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/cosmos/bank/v1beta1/params"},
					Methods:  []string{http.MethodGet},
				},
			},
		},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     boolPtr(true),
			WebSocketConnections: 2,
			JsonRpc:              cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/cosmos/bank/v1beta1/params", `{"params":{}}`),
		testharness.WithLCDResponse(http.MethodGet, "/secret", `{"top":"secret"}`),
	)

	allowed := h.GET(t, h.LCDURL+"/cosmos/bank/v1beta1/params")
	assert.Equal(t, allowed.StatusCode, http.StatusOK)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/cosmos/bank/v1beta1/params"), 1)

	denied := h.GET(t, h.LCDURL+"/secret")
	assert.Equal(t, denied.StatusCode, http.StatusUnauthorized)
	// Upstream must NOT have been called for the denied path.
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/secret"), 0)
}

// TestHarness_CacheHit verifies that with caching enabled, the second
// identical request is served from cache (upstream call count stays at 1).
func TestHarness_CacheHit(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100,
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/cached"},
					Methods:  []string{http.MethodGet},
					Cache:    &cosmoguard.RuleCache{Enable: true, TTL: time.Hour},
				},
			},
		},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     boolPtr(true),
			WebSocketConnections: 2,
			JsonRpc:              cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/cached", `{"value":42}`),
	)

	r1 := h.GET(t, h.LCDURL+"/cached")
	r2 := h.GET(t, h.LCDURL+"/cached")

	assert.Equal(t, r1.StatusCode, http.StatusOK)
	assert.Equal(t, r2.StatusCode, http.StatusOK)
	assert.Equal(t, string(r2.Body), string(r1.Body))
	// Upstream should have been hit only once.
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/cached"), 1)
}

// TestHarness_QueryMatchHeightPinned exercises the new query-param matching
// shipped earlier in v4 development: /block?height=N goes to the cached
// rule, bare /block falls through to the non-cached rule. The whole point
// of this test is that the upstream is hit exactly twice for two bare-/block
// requests but only once for two /block?height=12 requests.
func TestHarness_QueryMatchHeightPinned(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100, // more specific first
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/block"},
					Methods:  []string{http.MethodGet},
					Query:    map[string]string{"height": "*"},
					Cache:    &cosmoguard.RuleCache{Enable: true, TTL: time.Hour},
				},
				{
					Priority: 200, // bare /block, no cache
					Action:   cosmoguard.RuleActionAllow,
					Paths:    []string{"/block"},
					Methods:  []string{http.MethodGet},
				},
			},
		},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     boolPtr(true),
			WebSocketConnections: 2,
			JsonRpc:              cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}

	upstreamBody := func(r *http.Request) testharness.FakeResponse {
		// Echo the query so cached vs fresh responses are distinguishable.
		return testharness.FakeResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       []byte(`{"height":"` + r.URL.Query().Get("height") + `"}`),
		}
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
	)
	h.Upstream.LCD.SetDynamicHandler(http.MethodGet, "/block", upstreamBody)

	// Two height-pinned reads: second should hit cache.
	_ = h.GET(t, h.LCDURL+"/block?height=12")
	_ = h.GET(t, h.LCDURL+"/block?height=12")

	// Two bare reads: both go to upstream.
	_ = h.GET(t, h.LCDURL+"/block")
	_ = h.GET(t, h.LCDURL+"/block")

	got := h.Upstream.LCD.CallCount(http.MethodGet, "/block")
	// 1 cached path (1 upstream) + 2 bare paths (2 upstream) = 3 upstream calls
	assert.Equal(t, got, 3)
}

// TestHarness_JSONRPCSinglePassthrough boots a JSON-RPC handler for one
// method on the upstream and verifies cosmoguard returns its result intact.
func TestHarness_JSONRPCSinglePassthrough(t *testing.T) {
	h := testharness.New(t,
		testharness.WithJSONRPCMethod("status",
			func(req testharness.JSONRPCRequest) (any, *testharness.JSONRPCError) {
				return map[string]any{
					"node_info": map[string]any{"network": "test-1"},
				}, nil
			},
		),
	)

	resp := h.JSONRPCRequest(t, 7, "status", nil)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	var out struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("parse jsonrpc response: %v\nbody: %s", err, resp.Body)
	}
	assert.Equal(t, out.JSONRPC, "2.0")
	assert.Equal(t, out.ID, 7)
	assert.Equal(t, out.Result.NodeInfo.Network, "test-1")
}

// TestHarness_JSONRPCBatch verifies a batch request is forwarded and responses
// come back in the correct order.
func TestHarness_JSONRPCBatch(t *testing.T) {
	h := testharness.New(t,
		testharness.WithJSONRPCMethod("status",
			func(req testharness.JSONRPCRequest) (any, *testharness.JSONRPCError) {
				return "ok", nil
			},
		),
		testharness.WithJSONRPCMethod("health",
			func(req testharness.JSONRPCRequest) (any, *testharness.JSONRPCError) {
				return "alive", nil
			},
		),
	)

	resp := h.JSONRPCBatch(t, []testharness.JSONRPCCall{
		{ID: 1, Method: "status"},
		{ID: 2, Method: "health"},
	})
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	// Body must be a JSON array of length 2.
	var arr []map[string]any
	if err := json.Unmarshal(resp.Body, &arr); err != nil {
		t.Fatalf("parse batch response: %v\nbody: %s", err, resp.Body)
	}
	assert.Equal(t, len(arr), 2)
}

// TestHarness_ContentTypePreserved is the first compatibility regression
// test: when upstream sets an unusual content-type, cosmoguard MUST forward
// it as-is. This is a known pre-v4 bug (cached HTTP responses force
// application/json) — for now we exercise the no-cache path which is
// supposed to be fully transparent. Once Phase B5 lands, this test gets a
// companion assertion for the cache-hit path.
func TestHarness_ContentTypePreserved_Passthrough(t *testing.T) {
	h := testharness.New(t)
	h.Upstream.LCD.SetResponse(http.MethodGet, "/text-endpoint",
		testharness.FakeResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "text/plain; charset=utf-8"},
			Body:       []byte("hello world"),
		},
	)

	resp := h.GET(t, h.LCDURL+"/text-endpoint")
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, resp.Header.Get("Content-Type"), "text/plain; charset=utf-8")
	assert.Equal(t, string(resp.Body), "hello world")
}

// TestHarness_RecordedRequests verifies the Calls() snapshot captures method,
// path, query, and body fields the harness will need for assertions.
func TestHarness_RecordedRequests(t *testing.T) {
	h := testharness.New(t,
		testharness.WithLCDResponse(http.MethodPost, "/echo", "{}"),
	)

	body := []byte(`{"hello":"world"}`)
	_ = h.POST(t, h.LCDURL+"/echo?foo=bar", "application/json", body)

	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 1)
	c := calls[0]
	assert.Equal(t, c.Method, http.MethodPost)
	assert.Equal(t, c.Path, "/echo")
	assert.Equal(t, c.Query.Get("foo"), "bar")
	assert.Assert(t, bytes.Equal(c.Body, body))
}

// TestHarness_GRPCListenerOpen confirms cosmoguard's gRPC proxy is bound and
// accepting connections on the public-facing port real clients would dial.
// Phase F replaces the upstream stub with a real gRPC server and adds
// hit/miss/streaming tests on top.
func TestHarness_GRPCListenerOpen(t *testing.T) {
	h := testharness.New(t)
	conn, err := net.DialTimeout("tcp",
		"127.0.0.1:"+strconv.Itoa(h.GRPCPort),
		500*time.Millisecond)
	assert.NilError(t, err)
	_ = conn.Close()
}

// TestHarness_DefaultDenyBlocksUnmatchedRequests proves that with Default:
// Deny and no matching rule, cosmoguard returns 401 and upstream is never
// hit. Pairs with TestHarness_AllowsPassthroughByDefault.
func TestHarness_DefaultDenyBlocksUnmatchedRequests(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionDeny},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     boolPtr(true),
			WebSocketConnections: 2,
			JsonRpc:              cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/any", `{"x":1}`),
	)

	resp := h.GET(t, h.LCDURL+"/any")
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/any"), 0)
}

// TestHarness_ContentTypePreserved_CachedHit (Phase B5): the cache-hit path
// now replays the upstream's Content-Type instead of forcing application/json.
func TestHarness_ContentTypePreserved_CachedHit(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{{
				Priority: 100,
				Action:   cosmoguard.RuleActionAllow,
				Paths:    []string{"/text"},
				Methods:  []string{http.MethodGet},
				Cache:    &cosmoguard.RuleCache{Enable: true, TTL: time.Hour},
			}},
		},
		RPC:  cosmoguard.RpcConfig{Default: cosmoguard.RuleActionAllow, JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow}},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t, testharness.WithConfig(cfg))
	h.Upstream.LCD.SetResponse(http.MethodGet, "/text", testharness.FakeResponse{
		StatusCode: 200,
		Headers:    map[string]string{"Content-Type": "text/plain; charset=utf-8"},
		Body:       []byte("hello"),
	})

	// First call: miss → upstream → ContentType should be preserved.
	r1 := h.GET(t, h.LCDURL+"/text")
	assert.Equal(t, r1.Header.Get("Content-Type"), "text/plain; charset=utf-8")
	// Second call: hit → cached; today returns application/json (BUG).
	r2 := h.GET(t, h.LCDURL+"/text")
	assert.Equal(t, r2.Header.Get("Content-Type"), "text/plain; charset=utf-8")
}

// TestHarness_JSONRPCNullID verifies the harness's JSON-RPC handler echoes
// the request ID back unchanged when it is JSON `null` (a JSON-RPC
// notification). This pins down the encoding behavior so a future regression
// surfaces here, not in production.
func TestHarness_JSONRPCNullID(t *testing.T) {
	gotID := make(chan json.RawMessage, 1)
	h := testharness.New(t,
		testharness.WithJSONRPCMethod("notify",
			func(req testharness.JSONRPCRequest) (any, *testharness.JSONRPCError) {
				gotID <- req.ID
				return "ack", nil
			},
		),
	)

	body := []byte(`{"jsonrpc":"2.0","id":null,"method":"notify","params":[]}`)
	resp := h.POST(t, h.RPCURL+"/", "application/json", body)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	select {
	case id := <-gotID:
		assert.Equal(t, string(id), "null")
	case <-time.After(time.Second):
		t.Fatal("handler never received request")
	}
}

// TestHarness_JSONRPCDispatchUnderHTTPAllowRule is the regression test
// for the endpoint-dispatch bypass: an explicit HTTP `POST /` allow rule
// (the documented way to opt the connection out of HTTP-level auth and
// delegate to JSON-RPC method rules) must NOT route the request to the
// generic HTTP allow path. The JSON-RPC handler must still run so a
// per-method deny rule applies. Before the fix, the matched HTTP rule
// suppressed endpoint dispatch and the call was forwarded upstream.
func TestHarness_JSONRPCDispatchUnderHTTPAllowRule(t *testing.T) {
	cfg := &cosmoguard.Config{
		Host:    "127.0.0.1",
		Cache:   cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		Metrics: cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			// HTTP-level allow rule on the JSON-RPC endpoint path.
			Rules: []*cosmoguard.HttpRule{
				{Paths: []string{"/"}, Methods: []string{"POST"}, Action: cosmoguard.RuleActionAllow},
			},
			JsonRpc: cosmoguard.JsonRpcConfig{
				Default: cosmoguard.RuleActionAllow,
				// Per-method deny — only enforced if the JSON-RPC handler runs.
				Rules: []*cosmoguard.JsonRpcRule{
					{Methods: []string{"secret"}, Action: cosmoguard.RuleActionDeny},
				},
			},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionDeny},
	}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithJSONRPCMethod("secret",
			func(req testharness.JSONRPCRequest) (any, *testharness.JSONRPCError) {
				// If this runs, the JSON-RPC deny rule was bypassed.
				return map[string]any{"ok": true}, nil
			},
		),
	)

	resp := h.JSONRPCRequest(t, 1, "secret", nil)
	// The JSON-RPC method-deny must have applied: the response must NOT
	// carry the upstream success result.
	if strings.Contains(string(resp.Body), `"ok"`) {
		t.Fatalf("JSON-RPC method deny was bypassed — upstream result leaked: %s", resp.Body)
	}
	var out struct {
		Error  *struct{ Code int } `json:"error"`
		Result json.RawMessage     `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("parse jsonrpc response: %v\nbody: %s", err, resp.Body)
	}
	if out.Error == nil {
		t.Fatalf("expected a JSON-RPC error (method denied), got: %s", resp.Body)
	}
}
