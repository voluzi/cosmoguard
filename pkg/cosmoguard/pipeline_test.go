package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMWAuthGate_NilRuleAuthEnforcesDefaultRequire is the regression
// test for the default-require bypass: a matched rule that omits an
// `auth:` block (ruleAuth → nil) must still fall back to the global
// auth.defaultRequire policy. An anonymous caller is denied; a resolved
// identity passes.
func TestMWAuthGate_NilRuleAuthEnforcesDefaultRequire(t *testing.T) {
	a := authenticatorWithDefaultRequire(t) // defaultRequire = true
	nilRuleAuth := func(Request) *RuleAuthConfig { return nil }
	allow := func(Request) Decision { return Decision{} }

	// Anonymous (no identity) + nil rule auth → must be denied 401.
	anon := newHTTPRequest(httptest.NewRequest(http.MethodGet, "http://x/", nil))
	dec := MWAuthGate(a, nilRuleAuth)(anon, allow)
	if !dec.Stop || dec.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("anonymous + defaultRequire: expected 401 stop, got %+v", dec)
	}

	// A resolved (non-anonymous) identity + nil rule auth → allowed.
	authed := newHTTPRequest(httptest.NewRequest(http.MethodGet, "http://x/", nil))
	authed.SetIdentity(&Identity{Name: "test-client", Method: "api-key", Scopes: []string{"read"}})
	dec = MWAuthGate(a, nilRuleAuth)(authed, allow)
	if dec.Stop {
		t.Fatalf("authenticated + nil rule auth: expected pass, got %+v", dec)
	}
}

// TestChain_OrderAndShortCircuit: middlewares run outer-to-inner;
// a Stop decision halts the chain at the level it fires from.
func TestChain_OrderAndShortCircuit(t *testing.T) {
	var trace []string

	logging := func(name string) Middleware {
		return func(req Request, next Next) Decision {
			trace = append(trace, name+":pre")
			d := next(req)
			trace = append(trace, name+":post")
			return d
		}
	}
	deny := func(req Request, _ Next) Decision {
		trace = append(trace, "deny")
		return Decision{Stop: true, Action: "deny", Reason: "blocked"}
	}

	chain := Chain(logging("a"), logging("b"), deny, logging("c"))
	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	d := chain(newHTTPRequest(r))

	if !d.Stop || d.Reason != "blocked" {
		t.Fatalf("decision not propagated: %+v", d)
	}
	want := []string{"a:pre", "b:pre", "deny", "b:post", "a:post"}
	if len(trace) != len(want) {
		t.Fatalf("trace length: got %v want %v", trace, want)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Fatalf("trace[%d]: got %q want %q (full: %v)", i, trace[i], want[i], trace)
		}
	}
}

// TestMWPanicRecovery_HandlesPanic: a panic in a downstream middleware
// surfaces as a 500 Decision, not a goroutine crash.
func TestMWPanicRecovery_HandlesPanic(t *testing.T) {
	chain := Chain(
		MWPanicRecovery(nil),
		func(Request, Next) Decision { panic("kaboom") },
	)
	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	d := chain(newHTTPRequest(r))
	if !d.Stop || d.HTTPStatus != 500 {
		t.Fatalf("expected 500 stop; got %+v", d)
	}
}

// TestMWIdentityResolve_ReplaySurfaces401: an ErrReplay return from
// Resolve must produce a 401 with the dedicated reason.
func TestMWIdentityResolve_ReplaySurfaces401(t *testing.T) {
	a := &Authenticator{
		methods: []AuthMethod{fakeAuthMethod{err: ErrReplay}},
	}
	httpFrom := func(req Request) *http.Request {
		hr, _ := req.(*httpRequest)
		return hr.r
	}
	chain := Chain(MWIdentityResolve(a, httpFrom))
	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	d := chain(newHTTPRequest(r))
	if !d.Stop || d.HTTPStatus != 401 || d.Reason != "token replayed" {
		t.Fatalf("expected replay 401; got %+v", d)
	}
}

// TestProtocolAdaptersExposeOperationName: each protocol adapter must
// return the right OperationName so log lines / metric labels stay
// stable across protocols.
func TestProtocolAdaptersExposeOperationName(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://x/cosmos/bank/v1beta1/params", nil)
	if got := newHTTPRequest(r).OperationName(); got != "/cosmos/bank/v1beta1/params" {
		t.Fatalf("http operation: %s", got)
	}
	jrm := &JsonRpcMsg{Method: "broadcast_tx_sync"}
	if got := newJsonRpcRequest(r, jrm).OperationName(); got != "broadcast_tx_sync" {
		t.Fatalf("jsonrpc operation: %s", got)
	}
	if got := newWSRequest(r, jrm).OperationName(); got != "broadcast_tx_sync" {
		t.Fatalf("ws operation: %s", got)
	}
	if got := newGrpcRequest(r.Context(), "/cosmos.bank.v1beta1.Query/Balance", "1.2.3.4:5678").OperationName(); got != "/cosmos.bank.v1beta1.Query/Balance" {
		t.Fatalf("grpc operation: %s", got)
	}
}

// fakeAuthMethod returns the configured (id, err) for any request.
type fakeAuthMethod struct {
	id  *Identity
	err error
}

func (f fakeAuthMethod) Resolve(_ *http.Request) (*Identity, error) { return f.id, f.err }
func (f fakeAuthMethod) HeadersToStrip() []string                   { return nil }
