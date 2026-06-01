package cosmoguard

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestJsonRpcResponses_NotificationBatchNotDropped is the regression
// test for the notification-batch 502: a batch mixing a call (with id)
// and a notification (no id) gets one upstream response, and that must
// NOT be treated as a count mismatch. The call's response is preserved;
// the notification produces no output; UnansweredCalls reports zero.
func TestJsonRpcResponses_NotificationBatchNotDropped(t *testing.T) {
	var l JsonRpcResponses
	call := &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "a"}
	notif := &JsonRpcMsg{Version: "2.0", Method: "b"} // no id → notification
	l.AddPending(call)
	l.AddPending(notif)

	// A compliant upstream answers only the call.
	resp := &JsonRpcMsg{Version: "2.0", ID: float64(1)}
	l.Set(JsonRpcMsgs{call, notif}, JsonRpcMsgs{resp})

	if n := l.UnansweredCalls(); n != 0 {
		t.Fatalf("notification batch wrongly flagged as mismatch: UnansweredCalls=%d, want 0", n)
	}
	if final := l.GetFinal(); len(final) != 1 {
		t.Fatalf("expected exactly the call's response, got %d", len(final))
	}
}

// TestJsonRpcResponses_DroppedCallFlagged confirms the guard still fires
// when a real id-bearing call goes unanswered (the case the original
// count check protected against).
func TestJsonRpcResponses_DroppedCallFlagged(t *testing.T) {
	var l JsonRpcResponses
	c1 := &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "a"}
	c2 := &JsonRpcMsg{Version: "2.0", ID: float64(2), Method: "b"}
	l.AddPending(c1)
	l.AddPending(c2)

	// Upstream answered only c1 — c2 is dropped.
	l.Set(JsonRpcMsgs{c1, c2}, JsonRpcMsgs{&JsonRpcMsg{Version: "2.0", ID: float64(1)}})

	if n := l.UnansweredCalls(); n != 1 {
		t.Fatalf("dropped call must be flagged: UnansweredCalls=%d, want 1", n)
	}
}

// TestHandleHttpBatch_PerRuleAuthDeniesItems is the regression test for
// the JSON-RPC batch policy bypass: a method matching an allow rule that
// carries auth scopes must be denied per-item inside a batch, exactly
// as it is for a single request — and a denied item must never be
// forwarded upstream. Before the fix, the batch loop skipped the
// per-rule auth + rate-limit gate entirely.
func TestHandleHttpBatch_PerRuleAuthDeniesItems(t *testing.T) {
	h := &JsonRpcHandler{
		log:         log.WithField("t", "jsonrpc-batch"),
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
	}
	// Allow rule matching every method (empty Methods → Match true),
	// gated on the "admin" scope no caller in this test holds.
	h.rules = []*JsonRpcRule{{
		Tag:    "balance",
		Action: RuleActionAllow,
		Auth:   &RuleAuthConfig{Scopes: []string{"admin"}},
	}}
	h.defaultAction = RuleActionDeny

	batch := JsonRpcMsgs{
		{Version: "2.0", ID: float64(1), Method: "balance"},
		{Version: "2.0", ID: float64(2), Method: "balance"},
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil) // anonymous: no identity on ctx
	next := func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream must not be called: every batch item is policy-denied")
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response is not a JSON array: %v (body=%s)", err, w.Body.String())
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 batch responses, got %d (%s)", len(arr), w.Body.String())
	}
	for i, item := range arr {
		errObj, ok := item["error"].(map[string]any)
		if !ok {
			t.Fatalf("item %d: expected a JSON-RPC error (denied), got %v", i, item)
		}
		if code, _ := errObj["code"].(float64); int(code) != -32001 {
			t.Fatalf("item %d: expected -32001 auth deny, got code %v", i, errObj["code"])
		}
		if _, hasResult := item["result"]; hasResult {
			t.Fatalf("item %d: denied item must not carry a result", i)
		}
	}
}

// TestHandleHttpBatch_DeniedNotificationGetsNoResponse pins JSON-RPC
// 2.0 §4.1 on the deny path: a notification (no id) that is policy-denied
// inside a batch must produce NO response object, while an id-bearing
// call denied in the same batch still gets its error.
func TestHandleHttpBatch_DeniedNotificationGetsNoResponse(t *testing.T) {
	h := &JsonRpcHandler{
		log:         log.WithField("t", "jsonrpc-batch"),
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
	}
	h.rules = []*JsonRpcRule{{
		Tag:    "balance",
		Action: RuleActionAllow,
		Auth:   &RuleAuthConfig{Scopes: []string{"admin"}},
	}}
	h.defaultAction = RuleActionDeny

	batch := JsonRpcMsgs{
		{Version: "2.0", ID: float64(1), Method: "balance"}, // call → error
		{Version: "2.0", Method: "balance"},                 // notification → nothing
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	next := func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream must not be called: every batch item is policy-denied")
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response is not a JSON array: %v (body=%s)", err, w.Body.String())
	}
	// Only the id-bearing call gets a response; the notification gets none.
	if len(arr) != 1 {
		t.Fatalf("expected exactly 1 response (call only, notification suppressed), got %d (%s)", len(arr), w.Body.String())
	}
	if id, _ := arr[0]["id"].(float64); id != 1 {
		t.Fatalf("the single response must be for the id=1 call, got id=%v", arr[0]["id"])
	}
}

// TestJsonRpcResponses_FillUnansweredCalls confirms unanswered id-bearing
// calls become per-item JSON-RPC errors while notifications stay absent
// and already-answered items (cache hits) are preserved.
func TestJsonRpcResponses_FillUnansweredCalls(t *testing.T) {
	var l JsonRpcResponses
	call := &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "a"}
	notif := &JsonRpcMsg{Version: "2.0", Method: "b"}
	hit := &JsonRpcMsg{Version: "2.0", ID: float64(2), Method: "c"}
	l.AddPending(call)
	l.AddPending(notif)
	l.AddResponse(hit, &JsonRpcMsg{Version: "2.0", ID: float64(2)}) // already answered (cache hit)

	l.FillUnansweredCalls()

	final := l.GetFinal()
	if len(final) != 2 {
		t.Fatalf("expected 2 responses (errored call + preserved hit), got %d", len(final))
	}
	var sawCallErr, sawHit bool
	for _, m := range final {
		switch m.ID {
		case int64(1), float64(1):
			if m.Error == nil || m.Error.Code != -32603 {
				t.Fatalf("unanswered call must carry a -32603 error, got %+v", m.Error)
			}
			sawCallErr = true
		case int64(2), float64(2):
			sawHit = true
		}
	}
	if !sawCallErr || !sawHit {
		t.Fatalf("expected both the errored call and the preserved hit; callErr=%v hit=%v", sawCallErr, sawHit)
	}
}

// TestHandleHttpBatch_UpstreamErrorReturnsPerItemErrors is the regression
// test for the P1: when the upstream call fails entirely, the batch must
// NOT return a non-JSON 500 — every id-bearing call comes back as a
// per-item JSON-RPC error in a valid 200 array. (The mixed answered/
// omitted correlation is covered at the unit level by
// TestJsonRpcResponses_FillUnansweredCalls.)
func TestHandleHttpBatch_UpstreamErrorReturnsPerItemErrors(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		defaultAction: RuleActionAllow,
	}
	batch := JsonRpcMsgs{
		{Version: "2.0", ID: float64(1), Method: "a"},
		{Version: "2.0", ID: float64(2), Method: "b"},
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	// Upstream returns a non-JSON body → getResponsesFromUpstream errors.
	next := func(uw http.ResponseWriter, _ *http.Request) {
		uw.WriteHeader(http.StatusBadGateway)
		_, _ = uw.Write([]byte("upstream exploded"))
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	if w.Code != http.StatusOK {
		t.Fatalf("expected a valid 200 batch array, got %d (%s)", w.Code, w.Body.String())
	}
	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response must be a JSON array, got %q: %v", w.Body.String(), err)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 per-item errors, got %d (%s)", len(arr), w.Body.String())
	}
	for _, m := range arr {
		e, ok := m["error"].(map[string]any)
		if !ok || int(e["code"].(float64)) != -32603 {
			t.Fatalf("each item must be a -32603 error, got %v", m)
		}
	}
}

// TestHandleHttpBatch_AllNotificationsEmptyBody confirms a batch that
// yields no responses (all notifications) returns 200 with an EMPTY body,
// not "[]", per JSON-RPC 2.0 §6.
func TestHandleHttpBatch_AllNotificationsEmptyBody(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		defaultAction: RuleActionDeny, // notifications denied → no response, no upstream
	}
	batch := JsonRpcMsgs{
		{Version: "2.0", Method: "a"},
		{Version: "2.0", Method: "b"},
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	next := func(http.ResponseWriter, *http.Request) {
		t.Fatal("no upstream call expected for an all-notification batch")
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if body := w.Body.String(); body != "" {
		t.Fatalf("notification-only batch must return an empty body, got %q", body)
	}
}

// TestHandleHttpBatch_CacheDisabledNotServed is the regression test for
// the cache.enable gap: a rule with a cache: block but enable:false must
// forward to upstream, never serve a pre-existing cached entry. The
// fake cache below would return a hit if consulted; the test asserts the
// upstream is hit instead.
func TestHandleHttpBatch_CacheDisabledNotServed(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		cache:         alwaysHitCache{},
		defaultAction: RuleActionDeny,
	}
	// Allow rule WITH a cache block but caching disabled.
	h.rules = []*JsonRpcRule{{
		Tag:    "balance",
		Action: RuleActionAllow,
		Cache:  &RuleCache{Enable: false, TTL: time.Minute},
	}}

	batch := JsonRpcMsgs{{Version: "2.0", ID: float64(1), Method: "balance"}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	upstreamHit := false
	next := func(uw http.ResponseWriter, _ *http.Request) {
		upstreamHit = true
		_, _ = uw.Write([]byte(`[{"jsonrpc":"2.0","id":1,"result":"from-upstream"}]`))
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	if !upstreamHit {
		t.Fatal("cache.enable:false must forward to upstream, not serve the cache")
	}
}

// alwaysHitCache is a cache.Cache that returns a canned hit for any key —
// used to prove a disabled rule never consults the cache.
type alwaysHitCache struct{}

func (alwaysHitCache) Get(_ context.Context, _ uint64) (*JsonRpcMsg, error) {
	return &JsonRpcMsg{Version: "2.0", Result: []byte(`"from-cache"`)}, nil
}
func (alwaysHitCache) Set(context.Context, uint64, *JsonRpcMsg, time.Duration) error { return nil }
func (alwaysHitCache) Has(context.Context, uint64) (bool, error)                     { return true, nil }
func (alwaysHitCache) Close() error                                                  { return nil }

// TestJsonRpcPolicyVerdict_DefaultRequireNoAuthBlock confirms a matched
// JSON-RPC rule without an auth: block still honours the global
// auth.defaultRequire (anonymous denied with -32001), closing the
// per-method bypass when the fronting HTTP rule opted out with
// require:false.
func TestJsonRpcPolicyVerdict_DefaultRequireNoAuthBlock(t *testing.T) {
	h := &JsonRpcHandler{
		log:         log.WithField("t", "jsonrpc"),
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
	}
	rule := &JsonRpcRule{Action: RuleActionAllow}       // no auth block
	r := httptest.NewRequest(http.MethodPost, "/", nil) // anonymous (no identity on ctx)

	ok, code, _ := h.jsonRpcPolicyVerdict(r, &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "m"}, rule, nil)
	if ok || code != -32001 {
		t.Fatalf("rule without auth block must honour defaultRequire: ok=%v code=%d (want deny -32001)", ok, code)
	}
}

// recordingCache counts Set calls so a test can prove a malformed
// upstream response is not written to the cache.
type recordingCache struct{ sets int }

func (c *recordingCache) Get(context.Context, uint64) (*JsonRpcMsg, error) { return nil, nil }
func (c *recordingCache) Set(context.Context, uint64, *JsonRpcMsg, time.Duration) error {
	c.sets++
	return nil
}
func (c *recordingCache) Has(context.Context, uint64) (bool, error) { return false, nil }
func (c *recordingCache) Close() error                              { return nil }

// TestGetSingleUpstreamResponse_SkipsCacheOnParseError is the regression
// test for caching a malformed upstream reply: when the upstream returns
// non-JSON (e.g. an HTML 502) for a cacheable request that allows
// empty-result caching, the response must NOT be cached (it would poison
// the cache with a synthetic invalid reply until TTL). The client still
// receives the raw bytes via the response tee.
func TestGetSingleUpstreamResponse_SkipsCacheOnParseError(t *testing.T) {
	rec := &recordingCache{}
	h := &JsonRpcHandler{
		log:         log.WithField("t", "jsonrpc"),
		cache:       rec,
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	const body = "<html>502 Bad Gateway</html>"
	next := func(uw http.ResponseWriter, _ *http.Request) { _, _ = uw.Write([]byte(body)) }

	h.getSingleUpstreamResponse(w, r, next, 123,
		&RuleCache{Enable: true, CacheEmptyResult: true, TTL: time.Minute}, "tag", "method")

	if rec.sets != 0 {
		t.Fatalf("malformed upstream response must not be cached, got %d Set calls", rec.sets)
	}
	if w.Body.String() != body {
		t.Fatalf("client must still receive the raw upstream bytes, got %q", w.Body.String())
	}
}

// TestJsonRpcDefaultAuthVerdict pins the no-match default gate: an
// unmatched method must clear auth.defaultRequire — anonymous denied with
// -32001, a resolved identity allowed.
func TestJsonRpcDefaultAuthVerdict(t *testing.T) {
	h := &JsonRpcHandler{
		log:         log.WithField("t", "jsonrpc"),
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
	}
	// Anonymous → denied.
	if ok, code, _ := h.jsonRpcDefaultAuthVerdict(httptest.NewRequest(http.MethodPost, "/", nil), &JsonRpcMsg{Method: "m"}); ok || code != -32001 {
		t.Fatalf("anonymous unmatched must be denied: ok=%v code=%d", ok, code)
	}
	// Resolved identity → allowed.
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r = r.WithContext(context.WithValue(r.Context(), identityCtxKey{}, &Identity{Name: "x", Method: "api-key", Scopes: []string{"read"}}))
	if ok, _, _ := h.jsonRpcDefaultAuthVerdict(r, &JsonRpcMsg{Method: "m"}); !ok {
		t.Fatal("resolved identity must pass the default gate")
	}
}

// TestHandleHttpBatch_UnmatchedDefaultAllowHonoursDefaultRequire is the
// regression test for the no-match default-allow bypass: an unmatched
// method on rpc.jsonrpc.default:allow must still be denied for an
// anonymous caller under defaultRequire, not forwarded upstream.
func TestHandleHttpBatch_UnmatchedDefaultAllowHonoursDefaultRequire(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		auth:          authenticatorWithDefaultRequire(t),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		defaultAction: RuleActionAllow,
	}
	// No rules → every item is unmatched → default-allow branch.
	batch := JsonRpcMsgs{{Version: "2.0", ID: float64(1), Method: "anything"}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil) // anonymous
	next := func(http.ResponseWriter, *http.Request) {
		t.Fatal("unmatched anonymous under defaultRequire must be denied, not forwarded")
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response not a JSON array: %v (%s)", err, w.Body.String())
	}
	if len(arr) != 1 {
		t.Fatalf("expected 1 response, got %d (%s)", len(arr), w.Body.String())
	}
	e, ok := arr[0]["error"].(map[string]any)
	if !ok || int(e["code"].(float64)) != -32001 {
		t.Fatalf("unmatched anonymous must get -32001 auth deny, got %v", arr[0])
	}
}

// TestHandleHttpBatch_NotificationCacheHitNoResponse is the regression
// test for the notification cache-hit leak: the cache key ignores id, so
// a prior id-bearing call can prime an entry; a later notification (no
// id) with the same method/params hits that entry but must still get NO
// response (JSON-RPC 2.0 §4.1).
func TestHandleHttpBatch_NotificationCacheHitNoResponse(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		cache:         alwaysHitCache{}, // every lookup is a hit
		defaultAction: RuleActionDeny,
	}
	h.rules = []*JsonRpcRule{{
		Tag:    "q",
		Action: RuleActionAllow,
		Cache:  &RuleCache{Enable: true, TTL: time.Minute},
	}}

	batch := JsonRpcMsgs{
		{Version: "2.0", ID: float64(1), Method: "q"}, // call → cached result
		{Version: "2.0", Method: "q"},                 // notification → no response
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	next := func(http.ResponseWriter, *http.Request) {
		t.Fatal("cache hit must not reach upstream")
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response not a JSON array: %v (%s)", err, w.Body.String())
	}
	if len(arr) != 1 {
		t.Fatalf("expected exactly 1 response (call only; notification suppressed), got %d (%s)", len(arr), w.Body.String())
	}
	if id, _ := arr[0]["id"].(float64); id != 1 {
		t.Fatalf("the single response must be the id=1 call, got id=%v", arr[0]["id"])
	}
}

// TestHandleHttpSingle_NotificationCacheHitNoResponse mirrors the above
// for the single-request path: a notification cache hit writes no body.
func TestHandleHttpSingle_NotificationCacheHitNoResponse(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-single"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		cache:         alwaysHitCache{},
		defaultAction: RuleActionDeny,
	}
	h.rules = []*JsonRpcRule{{
		Tag:    "q",
		Action: RuleActionAllow,
		Cache:  &RuleCache{Enable: true, TTL: time.Minute},
	}}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	next := func(http.ResponseWriter, *http.Request) { t.Fatal("cache hit must not reach upstream") }

	// Notification: no id.
	h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: "q"}, w, r, next, time.Now())

	if w.Body.Len() != 0 {
		t.Fatalf("notification cache hit must write no response body, got %q", w.Body.String())
	}
}

// TestHandleHttpSingle_DeniedNotificationNoResponse pins §4.1 on the
// single-request deny paths: a denied notification (no id) writes no
// response body, across rule-deny, per-rule auth deny, and default-deny.
func TestHandleHttpSingle_DeniedNotificationNoResponse(t *testing.T) {
	mk := func() *JsonRpcHandler {
		return &JsonRpcHandler{
			log:         log.WithField("t", "jsonrpc-single"),
			cgDashboard: newDashboardObservability(),
			section:     "rpc.jsonrpc",
		}
	}

	t.Run("rule deny", func(t *testing.T) {
		h := mk()
		h.rules = []*JsonRpcRule{{Tag: "q", Action: RuleActionDeny}}
		h.defaultAction = RuleActionAllow
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: "q"}, w, r,
			func(http.ResponseWriter, *http.Request) {}, time.Now())
		if w.Body.Len() != 0 {
			t.Fatalf("rule-denied notification must write no body, got %q", w.Body.String())
		}
	})

	t.Run("default deny", func(t *testing.T) {
		h := mk()
		h.defaultAction = RuleActionDeny // no rules → unmatched → default deny
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: "q"}, w, r,
			func(http.ResponseWriter, *http.Request) {}, time.Now())
		if w.Body.Len() != 0 {
			t.Fatalf("default-denied notification must write no body, got %q", w.Body.String())
		}
	})

	t.Run("per-rule auth deny", func(t *testing.T) {
		h := mk()
		h.auth = authenticatorWithDefaultRequire(t)
		h.rules = []*JsonRpcRule{{Tag: "q", Action: RuleActionAllow, Auth: &RuleAuthConfig{Scopes: []string{"admin"}}}}
		h.defaultAction = RuleActionDeny
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/", nil) // anonymous
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: "q"}, w, r,
			func(http.ResponseWriter, *http.Request) {}, time.Now())
		if w.Body.Len() != 0 {
			t.Fatalf("auth-denied notification must write no body, got %q", w.Body.String())
		}
	})
}

// TestHandleHttpBatch_OversizedUpstreamResponseRejected is the regression
// test for the batch response-cap-before-buffering bug: a rogue upstream
// returning more than the cap must be rejected via the capped writer
// (which stops buffering at the limit) rather than fully buffering then
// length-checking. We assert the batch fails with 502 rather than
// returning the giant body.
func TestHandleHttpBatch_OversizedUpstreamResponseRejected(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("t", "jsonrpc-batch"),
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
		defaultAction: RuleActionAllow,
	}
	batch := JsonRpcMsgs{{Version: "2.0", ID: float64(1), Method: "a"}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	// Upstream streams way over the 32 MiB cap in chunks; the capped
	// writer must stop buffering and the batch must fail.
	chunk := make([]byte, 1<<20) // 1 MiB
	next := func(uw http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 40; i++ { // 40 MiB > 32 MiB cap
			_, _ = uw.Write(chunk)
		}
	}

	h.handleHttpBatch(batch, w, r, next, time.Now())

	// The oversized response is rejected by the capped writer →
	// getResponsesFromUpstream errors → the id-bearing call comes back as
	// a per-item -32603 error in a valid 200 array (not the giant body).
	if w.Code != http.StatusOK {
		t.Fatalf("expected a valid 200 batch array, got %d", w.Code)
	}
	if w.Body.Len() > 1<<20 {
		t.Fatalf("oversized upstream body must NOT be returned to the client (got %d bytes)", w.Body.Len())
	}
	var arr []map[string]any
	if err := stdjson.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("batch response must be a JSON array: %v", err)
	}
	if len(arr) != 1 {
		t.Fatalf("expected 1 per-item error, got %d", len(arr))
	}
	if e, ok := arr[0]["error"].(map[string]any); !ok || int(e["code"].(float64)) != -32603 {
		t.Fatalf("oversized upstream → id call must be a -32603 error, got %v", arr[0])
	}
}

// TestCappedResponseWriter pins the writer: under-limit buffers fully,
// over-limit flags overflow and truncates at the cap.
func TestCappedResponseWriter(t *testing.T) {
	w := newCappedResponseWriter(10)
	n, _ := w.Write([]byte("hello")) // 5 ≤ 10
	if n != 5 || w.overflowed {
		t.Fatalf("under-limit write: n=%d overflow=%v", n, w.overflowed)
	}
	n, _ = w.Write([]byte("worldXXXX")) // pushes past 10
	if n != 9 || !w.overflowed {
		t.Fatalf("over-limit write must report full len + overflow: n=%d overflow=%v", n, w.overflowed)
	}
	if w.buf.Len() != 10 {
		t.Fatalf("buffer must be capped at 10, got %d", w.buf.Len())
	}
}
