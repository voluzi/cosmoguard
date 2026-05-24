package testharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// FakeUpstream is the bundle of mock servers cosmoguard talks to in tests.
// Today this covers HTTP for LCD and RPC (including JSON-RPC and a stub for
// WebSocket); gRPC will be added in Phase F.
type FakeUpstream struct {
	LCD  *FakeHTTPServer
	RPC  *FakeHTTPServer
	GRPC *FakeGRPCServer // currently a stub (listener that accepts and drops)

	t *testing.T
}

// NewFakeUpstream constructs and starts a fake upstream. It binds httptest
// servers on ephemeral ports; coordinates are accessible via Host/LCDPort/
// RPCPort/GRPCPort.
func NewFakeUpstream(t *testing.T) *FakeUpstream {
	t.Helper()
	return &FakeUpstream{
		t:    t,
		LCD:  newFakeHTTPServer(t, "lcd"),
		RPC:  newFakeHTTPServer(t, "rpc"),
		GRPC: newFakeGRPCServer(t),
	}
}

// Stop tears down all upstream listeners.
func (u *FakeUpstream) Stop() {
	u.LCD.Stop()
	u.RPC.Stop()
	u.GRPC.Stop()
}

// Host returns the host (127.0.0.1) cosmoguard should dial.
func (u *FakeUpstream) Host() string { return "127.0.0.1" }

// LCDPort returns the ephemeral port serving LCD requests.
func (u *FakeUpstream) LCDPort() int { return u.LCD.port }

// RPCPort returns the ephemeral port serving RPC + JSON-RPC requests.
func (u *FakeUpstream) RPCPort() int { return u.RPC.port }

// GRPCPort returns the ephemeral port stub-serving gRPC. Connections to it
// are accepted and immediately closed; tests that exercise gRPC should
// override this.
func (u *FakeUpstream) GRPCPort() int { return u.GRPC.port }

// FakeResponse is a canned response a fake server returns for a given request.
type FakeResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
	// Delay, if non-zero, sleeps before writing the response — useful for
	// timeout and cancellation tests.
	Delay time.Duration
}

// DynamicHandler lets a test produce a per-request response. The returned
// FakeResponse is sent unchanged.
type DynamicHandler func(r *http.Request) FakeResponse

// JSONRPCHandler handles one JSON-RPC method. Return either result or err
// (set the unused one to nil).
type JSONRPCHandler func(req JSONRPCRequest) (result any, err *JSONRPCError)

// JSONRPCRequest is what a JSONRPCHandler receives.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// JSONRPCError mirrors the JSON-RPC 2.0 error shape.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// FakeHTTPServer is an httptest.Server backed by a configurable response
// table. Lookups are by "METHOD path" (the path is taken from r.URL.Path —
// query strings do not affect routing). The server records every request
// it receives, so tests can assert on call counts and arguments.
type FakeHTTPServer struct {
	t         *testing.T
	name      string
	srv       *httptest.Server
	port      int
	mu        sync.RWMutex
	static    map[string]FakeResponse   // key: "METHOD path"
	dynamic   map[string]DynamicHandler // key: "METHOD path"
	jsonrpc   map[string]JSONRPCHandler // key: method name
	defaultFn DynamicHandler            // fallback for unmatched paths
	calls     []RecordedRequest         // history (append-only under mu)
	callIdx   map[string]int            // "METHOD path" → count
}

// RecordedRequest captures everything we know about a received request after
// the body has been drained.
type RecordedRequest struct {
	Method  string
	Path    string
	Query   url.Values
	Headers http.Header
	Body    []byte
	At      time.Time
}

func newFakeHTTPServer(t *testing.T, name string) *FakeHTTPServer {
	t.Helper()
	s := &FakeHTTPServer{
		t:       t,
		name:    name,
		static:  map[string]FakeResponse{},
		dynamic: map[string]DynamicHandler{},
		jsonrpc: map[string]JSONRPCHandler{},
		callIdx: map[string]int{},
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	u, err := url.Parse(s.srv.URL)
	if err != nil {
		t.Fatalf("fakeupstream: parse server URL: %v", err)
	}
	_, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("fakeupstream: split host/port: %v", err)
	}
	if _, err := fmt.Sscanf(portStr, "%d", &s.port); err != nil {
		t.Fatalf("fakeupstream: parse port: %v", err)
	}
	return s
}

// SetResponse registers a canned response for an exact "method+path" pair.
// Re-registering replaces the previous entry.
func (s *FakeHTTPServer) SetResponse(method, path string, r FakeResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.static[routeKey(method, path)] = r
}

// SetDynamicHandler registers a per-request handler. Takes precedence over
// any static response with the same key.
func (s *FakeHTTPServer) SetDynamicHandler(method, path string, h DynamicHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamic[routeKey(method, path)] = h
}

// SetDefaultHandler installs a fallback used when no other handler matches.
// If unset, unmatched requests receive 404 with a JSON error body.
func (s *FakeHTTPServer) SetDefaultHandler(h DynamicHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultFn = h
}

// SetJSONRPCHandler registers a handler for one JSON-RPC method. When a
// POST arrives whose body parses as a JSON-RPC request (single or batch)
// and the requested method has a registered handler, the server responds
// with the JSON-RPC shape. Unrouted methods fall through to static/dynamic
// lookup or the default.
func (s *FakeHTTPServer) SetJSONRPCHandler(method string, h JSONRPCHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jsonrpc[method] = h
}

// CallCount returns the number of times a particular METHOD+path has been hit.
func (s *FakeHTTPServer) CallCount(method, path string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.callIdx[routeKey(method, path)]
}

// Calls returns a snapshot of every recorded request.
func (s *FakeHTTPServer) Calls() []RecordedRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RecordedRequest, len(s.calls))
	copy(out, s.calls)
	return out
}

// Reset clears all recorded requests and call counters. Registered handlers
// stay in place.
func (s *FakeHTTPServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
	s.callIdx = map[string]int{}
}

// Stop shuts the underlying httptest.Server down.
func (s *FakeHTTPServer) Stop() {
	if s.srv != nil {
		s.srv.Close()
	}
}

func (s *FakeHTTPServer) handle(w http.ResponseWriter, r *http.Request) {
	// Drain the body once so it's available to recorded-request consumers and
	// JSON-RPC parsing alike. After this point r.Body is exhausted.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "fakeupstream: read body: "+err.Error(), http.StatusInternalServerError)
		return
	}
	r.Body.Close()

	key := routeKey(r.Method, r.URL.Path)

	s.mu.Lock()
	s.callIdx[key]++
	s.calls = append(s.calls, RecordedRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Query:   r.URL.Query(),
		Headers: r.Header.Clone(),
		Body:    append([]byte(nil), body...),
		At:      time.Now(),
	})
	// Snapshot handlers under the lock; release before invoking them so they
	// can register further responses if needed.
	static, hasStatic := s.static[key]
	dyn, hasDyn := s.dynamic[key]
	jsonHandlers := make(map[string]JSONRPCHandler, len(s.jsonrpc))
	for k, v := range s.jsonrpc {
		jsonHandlers[k] = v
	}
	defaultFn := s.defaultFn
	s.mu.Unlock()

	// JSON-RPC routing — only triggers on POST with a JSON body and at least
	// one registered method.
	if r.Method == http.MethodPost && len(jsonHandlers) > 0 && looksLikeJSON(body) {
		if handled := s.handleJSONRPC(w, body, jsonHandlers); handled {
			return
		}
	}

	if hasDyn {
		writeFake(w, dyn(cloneRequestWithBody(r, body)))
		return
	}
	if hasStatic {
		writeFake(w, static)
		return
	}
	if defaultFn != nil {
		writeFake(w, defaultFn(cloneRequestWithBody(r, body)))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"error":"fakeupstream(%s): no handler for %s %s"}`, s.name, r.Method, r.URL.Path)))
}

// handleJSONRPC parses the body as one or many JSON-RPC requests and dispatches
// each through the registered handler map. Returns true if it produced a
// response. Unknown methods fall through (return false) so static/dynamic
// handlers on the same path still apply.
func (s *FakeHTTPServer) handleJSONRPC(w http.ResponseWriter, body []byte, handlers map[string]JSONRPCHandler) bool {
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "[") {
		var batch []JSONRPCRequest
		if err := json.Unmarshal(body, &batch); err != nil {
			return false
		}
		// If ANY method in the batch is unhandled, fall through. This keeps
		// the routing predictable: a JSON-RPC batch is either fully ours or
		// fully handled by static/dynamic/default.
		for _, req := range batch {
			if _, ok := handlers[req.Method]; !ok {
				return false
			}
		}
		resps := make([]map[string]any, len(batch))
		for i, req := range batch {
			resps[i] = invokeJSONRPC(handlers[req.Method], req)
		}
		writeJSON(w, http.StatusOK, resps)
		return true
	}

	var single JSONRPCRequest
	if err := json.Unmarshal(body, &single); err != nil {
		return false
	}
	if _, ok := handlers[single.Method]; !ok {
		return false
	}
	writeJSON(w, http.StatusOK, invokeJSONRPC(handlers[single.Method], single))
	return true
}

func invokeJSONRPC(h JSONRPCHandler, req JSONRPCRequest) map[string]any {
	result, errResp := h(req)
	out := map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
	}
	if errResp != nil {
		out["error"] = errResp
	} else {
		out["result"] = result
	}
	return out
}

// writeFake sends the FakeResponse to the wire. Headers are written in
// alphabetical order for deterministic golden-test output. NOTE: no
// Content-Type default is applied — if the test wants one, it must set it.
// This lets compatibility tests catch any case where cosmoguard adds a
// header upstream never sent.
func writeFake(w http.ResponseWriter, r FakeResponse) {
	if r.Delay > 0 {
		time.Sleep(r.Delay)
	}
	keys := make([]string, 0, len(r.Headers))
	for k := range r.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w.Header().Set(k, r.Headers[k])
	}
	status := r.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(r.Body) > 0 {
		_, _ = w.Write(r.Body)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	b, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "fakeupstream: marshal: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// cloneRequestWithBody returns the same *http.Request with its body replaced
// by a fresh reader so DynamicHandlers can read it.
func cloneRequestWithBody(r *http.Request, body []byte) *http.Request {
	clone := r.Clone(r.Context())
	clone.Body = io.NopCloser(bytes.NewReader(body))
	return clone
}

func looksLikeJSON(body []byte) bool {
	body = bytes.TrimLeft(body, " \t\n\r")
	if len(body) == 0 {
		return false
	}
	return body[0] == '{' || body[0] == '['
}

func routeKey(method, path string) string { return method + " " + path }

// ---------- gRPC stub ----------

// FakeGRPCServer is a placeholder that just opens a TCP listener and accepts
// connections, closing them silently. Phase F replaces this with a real gRPC
// server that supports caching, streaming, and rule-driven behavior.
type FakeGRPCServer struct {
	listener net.Listener
	port     int
	stopCh   chan struct{}
}

func newFakeGRPCServer(t *testing.T) *FakeGRPCServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakeupstream: grpc listen: %v", err)
	}
	g := &FakeGRPCServer{
		listener: l,
		port:     l.Addr().(*net.TCPAddr).Port,
		stopCh:   make(chan struct{}),
	}
	go g.acceptLoop()
	return g
}

func (g *FakeGRPCServer) acceptLoop() {
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			select {
			case <-g.stopCh:
				return
			default:
			}
			return
		}
		_ = conn.Close()
	}
}

// Stop closes the listener.
func (g *FakeGRPCServer) Stop() {
	select {
	case <-g.stopCh:
		return
	default:
	}
	close(g.stopCh)
	if g.listener != nil {
		_ = g.listener.Close()
	}
}
