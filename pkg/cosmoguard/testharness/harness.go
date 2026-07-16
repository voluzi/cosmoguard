// Package testharness boots an in-process fake Cosmos upstream and a real
// cosmoguard instance in front of it, exposing helpers that make end-to-end
// tests of the proxy pipeline straightforward.
//
// # Quickstart
//
//	h := testharness.New(t,
//		testharness.WithLCDResponse("GET", "/cosmos/bank/v1beta1/params", `{"params":{}}`),
//	)
//	resp := h.GET(t, h.LCDURL+"/cosmos/bank/v1beta1/params")
//	// h.Upstream.LCD.CallCount("GET", "/cosmos/bank/v1beta1/params") == 1
//
// The harness picks ephemeral ports automatically and routes through real
// cosmoguard code paths, so any compatibility regression surfaces as a test
// failure.
//
// # JSON-RPC routing semantics
//
// When the fake RPC upstream receives a POST whose body parses as JSON-RPC,
// it looks up the requested method in the handler map registered via
// WithJSONRPCMethod / SetJSONRPCHandler.
//
//   - Single request to a registered method: JSON-RPC response.
//   - Single request to an unregistered method: falls through to static/
//     dynamic/default handlers on the same path.
//   - Batch request where every method is registered: JSON-RPC array response.
//   - Batch request where any method is unregistered: ALL fall through to
//     static/dynamic/default handlers — the batch is treated as one HTTP
//     request, not split per-method.
//
// This all-or-nothing batch behavior differs from a real Cosmos node, which
// returns per-call errors for unknown methods in a batch. The harness errs on
// the side of failing loudly: an unrouted batch produces a 404 from the
// default handler, which is easy to catch in a test, rather than a partially-
// successful response that hides the missing registration.
//
// # Compatibility-test fidelity
//
// The fake upstream is built on net/http/httptest, so chunked transfer,
// header preservation, status codes, and content negotiation all go through
// Go's real HTTP server pipeline. Two non-fidelities to be aware of:
//
//   - writeFake (the canned-response writer) does NOT default the
//     Content-Type header. If a test wants one, it must set it. This lets
//     compatibility tests catch regressions where cosmoguard adds a header
//     upstream never sent.
//   - The gRPC stub accepts TCP connections and closes them silently. Phase F
//     replaces this with a real gRPC server.
package testharness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
)

func init() {
	// Quieter slog during tests — Error+ only by default. Individual
	// tests can re-enable via slog.SetDefault.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}

// Harness is the running test environment: a fake upstream + a real cosmoguard
// instance + ready-to-use client URLs.
type Harness struct {
	t *testing.T

	// Fake upstream serving LCD (REST), RPC HTTP and JSON-RPC, and WS subscriptions.
	Upstream *FakeUpstream

	// The cosmoguard instance under test.
	CosmoGuard *cosmoguard.CosmoGuard

	// URLs of cosmoguard's listening endpoints. Use these in tests.
	LCDURL string
	RPCURL string
	// WSURL points at the JSON-RPC websocket served by the RPC proxy.
	WSURL    string
	EvmRPC   string
	EvmRPCWS string

	// GRPCPort is the TCP port cosmoguard's gRPC proxy listens on. There's
	// no URL form because gRPC clients dial host:port directly.
	GRPCPort int

	// DashboardURL points at the standalone read-only dashboard,
	// when enabled via WithDashboardEnabled. Empty string when off.
	DashboardURL string

	cfg     *cosmoguard.Config
	stopMu  sync.Mutex
	stopped bool
}

// New constructs a Harness, applies options, and starts both the fake
// upstream and cosmoguard. The harness registers a cleanup function on t,
// so tests don't normally need to call Stop() themselves.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()
	h := &Harness{
		t:        t,
		Upstream: NewFakeUpstream(t),
	}

	for _, opt := range opts {
		opt.apply(h)
	}

	if h.cfg == nil {
		h.cfg = defaultHarnessConfig()
	}

	// Ports are chosen via freePort (bind :0, read the port, close).
	// That's inherently racy: between the close and cosmoguard.New
	// re-binding, a parallel test or the OS can grab the same port,
	// surfacing as "bind: address already in use". Retry the whole
	// port-assign + New with fresh ports a few times so the suite is
	// robust under -parallel load instead of flaking. Declared here so
	// the post-New URL builders can read the final values.
	var lcdPort, rpcPort, grpcPort, evmRpcPort, evmRpcWsPort int
	var cg *cosmoguard.CosmoGuard
	for attempt := 0; ; attempt++ {
		lcdPort = freePort(t)
		rpcPort = freePort(t)
		grpcPort = freePort(t)
		evmRpcPort = freePort(t)
		evmRpcWsPort = freePort(t)

		h.cfg.Host = "127.0.0.1"
		h.cfg.LcdPort = lcdPort
		h.cfg.RpcPort = rpcPort
		h.cfg.GrpcPort = grpcPort
		h.cfg.EvmRpcPort = evmRpcPort
		h.cfg.EvmRpcWsPort = evmRpcWsPort
		// IMPORTANT: writes to h.cfg.Node MUST precede cosmoguard.New
		// below — PrepareConfig promotes Node into Nodes[0] and then
		// zeros out Node, so a post-New write here would land on the
		// zeroed singular field and be silently lost. Re-applied each
		// retry iteration because the prior New zeroed Node.
		//
		// Skip the singular-Node defaults entirely when the test
		// pre-populated cfg.Nodes via WithConfig: the multi-upstream
		// tests provide their own pool and PrepareConfig now rejects
		// configs that set both `node:` (v3) and `nodes:` (v4).
		if len(h.cfg.Nodes) == 0 {
			h.cfg.Node.Host = h.Upstream.Host()
			h.cfg.Node.LcdPort = h.Upstream.LCDPort()
			h.cfg.Node.RpcPort = h.Upstream.RPCPort()
			h.cfg.Node.GrpcPort = h.Upstream.GRPCPort()
			h.cfg.Node.EvmRpcPort = h.Upstream.RPCPort()   // share with RPC for now
			h.cfg.Node.EvmRpcWsPort = h.Upstream.RPCPort() // ditto
		}

		if h.cfg.Metrics.IsEnabled() {
			h.cfg.Metrics.Port = freePort(t)
		}
		if h.cfg.Dashboard.IsEnabled() {
			h.cfg.Dashboard.Port = freePort(t)
		}

		var nerr error
		cg, nerr = cosmoguard.New(h.cfg)
		if nerr == nil {
			break
		}
		// Retry only the racy "port got taken between freePort and bind"
		// case; anything else is a real config/setup error.
		if attempt < 4 && strings.Contains(nerr.Error(), "address already in use") {
			continue
		}
		t.Fatalf("harness: cosmoguard.New: %v", nerr)
	}
	h.CosmoGuard = cg

	h.LCDURL = fmt.Sprintf("http://127.0.0.1:%d", lcdPort)
	h.RPCURL = fmt.Sprintf("http://127.0.0.1:%d", rpcPort)
	h.WSURL = fmt.Sprintf("ws://127.0.0.1:%d/websocket", rpcPort)
	h.GRPCPort = grpcPort
	if h.cfg.EnableEvm {
		h.EvmRPC = fmt.Sprintf("http://127.0.0.1:%d", evmRpcPort)
		h.EvmRPCWS = fmt.Sprintf("ws://127.0.0.1:%d", evmRpcWsPort)
	}
	if h.cfg.Dashboard.IsEnabled() {
		h.DashboardURL = fmt.Sprintf("http://127.0.0.1:%d", h.cfg.Dashboard.Port)
	}

	// cosmoguard.Run() blocks; launch on a goroutine and wait for listeners.
	go func() {
		if err := h.CosmoGuard.Run(); err != nil {
			slog.Debug("harness: cosmoguard.Run returned", "error", err)
		}
	}()

	if err := h.waitReady(2 * time.Second); err != nil {
		t.Fatalf("harness: cosmoguard never came up: %v", err)
	}

	t.Cleanup(h.Stop)
	return h
}

// Stop tears down the harness: shuts down the cosmoguard instance (closing
// every listener) and stops the fake upstream. Safe to call multiple times.
//
// Cosmoguard is shut down BEFORE the fake upstream so any in-flight requests
// see cosmoguard close their connection cleanly instead of receiving
// ECONNREFUSED from a vanished upstream. Phase J ships fully graceful drain
// with in-flight request waits; this minimal Shutdown is enough to prevent FD
// leaks between tests.
func (h *Harness) Stop() {
	h.stopMu.Lock()
	defer h.stopMu.Unlock()
	if h.stopped {
		return
	}
	h.stopped = true

	if h.CosmoGuard != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := h.CosmoGuard.Shutdown(ctx); err != nil {
			h.t.Logf("harness: cosmoguard shutdown returned: %v", err)
		}
		cancel()
	}
	h.Upstream.Stop()
}

func (h *Harness) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ports := []int{h.cfg.LcdPort, h.cfg.RpcPort, h.cfg.GrpcPort}
	if h.cfg.Metrics.IsEnabled() {
		ports = append(ports, h.cfg.Metrics.Port)
	}
	if h.cfg.Dashboard.IsEnabled() {
		ports = append(ports, h.cfg.Dashboard.Port)
	}
	for _, p := range ports {
		if !waitPortListen(p, deadline) {
			return fmt.Errorf("port %d never opened", p)
		}
	}
	return nil
}

func waitPortListen(port int, deadline time.Time) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// ---------- Client helpers ----------

// HTTPClient returns a configured *http.Client suitable for talking to the
// harness. Reasonable timeouts; no automatic redirect following.
func (h *Harness) HTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Response captures the parts of an HTTP response a test typically wants to
// assert on, after the body has been fully read.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// GET issues a GET against the given absolute URL.
func (h *Harness) GET(t *testing.T, fullURL string) *Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fullURL, nil)
	if err != nil {
		t.Fatalf("harness: build GET %s: %v", fullURL, err)
	}
	return h.Do(t, req)
}

// POST issues a POST with the given body and content-type.
func (h *Harness) POST(t *testing.T, fullURL, contentType string, body []byte) *Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, fullURL, strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("harness: build POST %s: %v", fullURL, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return h.Do(t, req)
}

// Do executes a fully-constructed request, draining and closing the body.
func (h *Harness) Do(t *testing.T, req *http.Request) *Response {
	t.Helper()
	resp, err := h.HTTPClient().Do(req)
	if err != nil {
		t.Fatalf("harness: %s %s: %v", req.Method, req.URL.String(), err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("harness: read response body: %v", err)
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       b,
	}
}

// ---------- Convenience JSON-RPC helpers ----------

// JSONRPCCall describes a single call inside a batch.
type JSONRPCCall struct {
	ID     any
	Method string
	Params any
}

// JSONRPCRequest issues one JSON-RPC POST to the configured RPC port.
func (h *Harness) JSONRPCRequest(t *testing.T, id any, method string, params any) *Response {
	t.Helper()
	if id == nil {
		id = time.Now().UnixNano()
	}
	body := mustMarshal(t, map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	return h.POST(t, h.RPCURL+"/", "application/json", body)
}

// JSONRPCBatch issues a batched JSON-RPC POST. Order is preserved; if a call
// has no ID, its index is used.
func (h *Harness) JSONRPCBatch(t *testing.T, calls []JSONRPCCall) *Response {
	t.Helper()
	batch := make([]map[string]any, len(calls))
	for i, c := range calls {
		id := c.ID
		if id == nil {
			id = i + 1
		}
		batch[i] = map[string]any{
			"jsonrpc": "2.0",
			"id":      id,
			"method":  c.Method,
			"params":  c.Params,
		}
	}
	return h.POST(t, h.RPCURL+"/", "application/json", mustMarshal(t, batch))
}

// ---------- internals ----------

// freePort returns an ephemeral TCP port by binding-then-closing. There is a
// vanishingly small TOCTOU window between Close() and cosmoguard's bind; in
// practice this hasn't been observed but tests that depend on absolute port
// uniqueness should be aware.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("harness: free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func defaultHarnessConfig() *cosmoguard.Config {
	// Dashboard is on-by-default in production but off in the test
	// harness: parallel harnesses would otherwise race for the default
	// Dashboard.Port (19999) and one would fail to bind. Tests that
	// want the dashboard plug a custom Config that allocates a fresh
	// port and sets Enable explicitly.
	dashboardOff := false
	return &cosmoguard.Config{
		Host:      "127.0.0.1",
		EnableEvm: false,
		Cache: cosmoguard.CacheGlobalConfig{
			TTL: 5 * time.Second,
		},
		Metrics:   cosmoguard.MetricsConfig{Enable: boolPtr(false)},
		Dashboard: cosmoguard.DashboardConfig{Enable: &dashboardOff},
		LCD:       cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     boolPtr(true),
			WebSocketConnections: 2,
			JsonRpc: cosmoguard.JsonRpcConfig{
				Default: cosmoguard.RuleActionAllow,
			},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("harness: marshal: %v", err)
	}
	return b
}
