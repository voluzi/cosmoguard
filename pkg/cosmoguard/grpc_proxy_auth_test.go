package cosmoguard

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gotest.tools/assert"
)

// fakeServerTransportStream carries the RPC method so
// grpc.MethodFromServerStream can resolve it inside the handler.
type fakeServerTransportStream struct{ method string }

func (f *fakeServerTransportStream) Method() string               { return f.method }
func (f *fakeServerTransportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerTransportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerTransportStream) SetTrailer(metadata.MD) error { return nil }

// gateTestServerStream is a grpc.ServerStream for driving
// cachingStreamHandler in tests. recvCalled / sendCalled flag whether
// the handler touched the request/response frames, so a test can prove
// the policy gate ran BEFORE any frame I/O.
type gateTestServerStream struct {
	ctx        context.Context
	recvCalled bool
	sendCalled bool
}

func (s *gateTestServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *gateTestServerStream) SendHeader(metadata.MD) error { return nil }
func (s *gateTestServerStream) SetTrailer(metadata.MD)       {}
func (s *gateTestServerStream) Context() context.Context     { return s.ctx }
func (s *gateTestServerStream) SendMsg(any) error            { s.sendCalled = true; return nil }
func (s *gateTestServerStream) RecvMsg(any) error            { s.recvCalled = true; return nil }

// authProxyForTest builds a minimal GrpcProxy that just exposes
// auth + dashboard hooks. The cache + upstream pool are skipped
// because Handle deny-paths return before touching them.
func authProxyForTest(t *testing.T, auth *Authenticator) *GrpcProxy {
	t.Helper()
	return &GrpcProxy{
		log:           log.WithField("test", "grpc-auth"),
		defaultAction: RuleActionAllow,
		cgDashboard:   newDashboardObservability(),
		section:       "grpc",
		auth:          auth,
	}
}

// authenticatorWithDefaultRequire builds an Authenticator whose
// default policy requires an identity but accepts one specific
// api key. Anonymous callers are denied.
func authenticatorWithDefaultRequire(t *testing.T) *Authenticator {
	t.Helper()
	require := true
	cfg := &AuthConfig{
		Enable:         true,
		DefaultRequire: require,
		Methods: []AuthMethodConfig{{
			Type:   "api-key",
			Header: "x-api-key",
		}},
		Identities: []IdentityConfig{{
			Name:   "test-client",
			APIKey: "secret",
			Scopes: []string{"read"},
		}},
	}
	a, err := NewAuthenticator(cfg, nil)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// TestGrpcAuth_DeniesAnonymousByDefault — without an api-key header
// the global default-require policy rejects the call with
// Unauthenticated and emits a deny record.
func TestGrpcAuth_DeniesAnonymousByDefault(t *testing.T) {
	a := authenticatorWithDefaultRequire(t)
	p := authProxyForTest(t, a)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	_, _, err := p.Handle(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	assert.Assert(t, err != nil, "anonymous caller must be denied")
	assert.Equal(t, status.Code(err), codes.Unauthenticated, "deny must surface as Unauthenticated")

	denied := p.cgDashboard.denied.Snapshot()
	assert.Equal(t, len(denied), 1, "deny record must land in the dashboard ring")
	assert.Equal(t, denied[0].Reason, "auth")
}

// TestGrpcAuth_PassesValidApiKey — when the caller presents the
// configured api key the gate allows through. The downstream pool
// is nil in this fixture, so Handle returns a no-upstream error
// AFTER the auth gate passes — that's the signal we want.
func TestGrpcAuth_PassesValidApiKey(t *testing.T) {
	a := authenticatorWithDefaultRequire(t)
	p := authProxyForTest(t, a)
	// SetRules — empty list with defaultAction=allow makes the
	// post-auth path take the "no rule matched, default allow"
	// branch, which then hits p.pool.Pick(). pool == nil → nil
	// dereference, so we guard by setting a sentinel pool.
	p.pool = &GrpcUpstreamPool{} // empty pool: Pick returns nil

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{
		"x-api-key": []string{"secret"},
	})
	_, _, err := p.Handle(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	// Auth passed (no Unauthenticated); the no-upstream Unavailable
	// surfaces from later in Handle.
	assert.Assert(t, err != nil, "no upstream pool → expected an Unavailable error")
	assert.Equal(t, status.Code(err), codes.Unavailable, "expected Unavailable (auth passed, no upstream)")
}

// TestGrpcAuth_InvalidApiKeyDenies — a credential that doesn't
// match any identity is denied as a forged-credential per
// MWIdentityResolve semantics: ErrInvalidCredential → Unauthenticated.
func TestGrpcAuth_InvalidApiKeyDenies(t *testing.T) {
	a := authenticatorWithDefaultRequire(t)
	p := authProxyForTest(t, a)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{
		"x-api-key": []string{"not-a-real-key"},
	})
	_, _, err := p.Handle(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	assert.Assert(t, err != nil)
	assert.Equal(t, status.Code(err), codes.Unauthenticated)
}

// TestGrpcCache_GateRunsOnCacheablePath is the regression test for the
// auth/rate-limit bypass on cacheable gRPC methods: cachingStreamHandler
// must run the policy gate (Handle) before any cache lookup or upstream
// forward. With default-require auth and no credentials, an anonymous
// caller hitting a cacheable allow rule must be denied with
// Unauthenticated — and the handler must not even read the request frame.
func TestGrpcCache_GateRunsOnCacheablePath(t *testing.T) {
	a := authenticatorWithDefaultRequire(t)
	p := authProxyForTest(t, a)
	// A cacheable allow rule matching every method (empty Methods →
	// Match returns true). No Compile needed.
	p.rules = []*GrpcRule{{
		Action: RuleActionAllow,
		Cache:  &RuleCache{Enable: true},
	}}

	const method = "/cosmos.bank.v1beta1.Query/Balance"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{}) // no x-api-key
	ctx = grpc.NewContextWithServerTransportStream(ctx, &fakeServerTransportStream{method: method})
	stream := &gateTestServerStream{ctx: ctx}

	transparentCalled := false
	transparent := func(any, grpc.ServerStream) error { transparentCalled = true; return nil }

	err := cachingStreamHandler(p, transparent)(nil, stream)
	assert.Assert(t, err != nil, "anonymous caller on a cacheable method must be denied")
	assert.Equal(t, status.Code(err), codes.Unauthenticated, "deny must surface as Unauthenticated")
	assert.Assert(t, !stream.recvCalled, "gate must run before the request frame is read")
	assert.Assert(t, !stream.sendCalled, "no response/cache frame may be sent on a denied call")
	assert.Assert(t, !transparentCalled, "cacheable path must not fall through to the transparent forwarder")

	denied := p.cgDashboard.denied.Snapshot()
	assert.Equal(t, len(denied), 1, "deny must be recorded")
	assert.Equal(t, denied[0].Reason, "auth")
}

// TestGrpcRateLimitKey_Compound is the regression test for the missing
// compound scope on the gRPC/JSON-RPC rate-limit key: `compound` must
// key on identity+IP (so one identity across IPs gets independent
// budgets), and the IP component must ignore the ephemeral port.
func TestGrpcRateLimitKey_Compound(t *testing.T) {
	const fp = uint64(0xabc)

	// Same identity, different IPs → distinct buckets.
	k1 := grpcRateLimitKey(RateLimitScopeCompound, fp, "10.0.0.1:5555", "alice")
	k2 := grpcRateLimitKey(RateLimitScopeCompound, fp, "10.0.0.2:6666", "alice")
	assert.Assert(t, k1 != k2, "compound: same identity from different IPs must get distinct buckets")

	// Same identity+IP, different ports → same bucket (port stripped).
	k3 := grpcRateLimitKey(RateLimitScopeCompound, fp, "10.0.0.1:9999", "alice")
	assert.Equal(t, k1, k3, "compound: same identity+IP must share a bucket regardless of port")

	// Anonymous compound falls back to per-IP.
	anon := grpcRateLimitKey(RateLimitScopeCompound, fp, "10.0.0.1:1", "")
	perIP := grpcRateLimitKey(RateLimitScopePerIP, fp, "10.0.0.1:2", "")
	assert.Equal(t, anon, perIP, "anonymous compound must fall back to per-IP")
}

// TestSynthRequestFromGrpcMD_CopiesHeaders verifies the helper
// converts gRPC metadata pairs verbatim into http.Header so
// Authenticator.Resolve can match what the api-key / JWT methods
// look for.
func TestSynthRequestFromGrpcMD_CopiesHeaders(t *testing.T) {
	md := metadata.MD{
		"x-api-key":     []string{"abc"},
		"authorization": []string{"Bearer xyz"},
	}
	r := synthRequestFromGrpcMD(md)
	assert.Equal(t, r.Header.Get("X-Api-Key"), "abc")
	assert.Equal(t, r.Header.Get("Authorization"), "Bearer xyz")
}

// TestGrpcAuth_RuleOptOutMakesMethodPublic is the regression test for the
// gRPC ordering bug: with defaultRequire:true, a rule that sets
// auth.require:false must make its method reachable by an anonymous
// caller. Previously the global gate rejected anonymous before any rule
// matched, so the opt-out was unreachable.
func TestGrpcAuth_RuleOptOutMakesMethodPublic(t *testing.T) {
	a := authenticatorWithDefaultRequire(t) // defaultRequire = true
	p := authProxyForTest(t, a)
	p.pool = &GrpcUpstreamPool{} // empty pool: Pick returns nil
	reqFalse := false
	// Matches all methods (no Methods), opts out of auth.
	p.rules = []*GrpcRule{{Action: RuleActionAllow, Auth: &RuleAuthConfig{Require: &reqFalse}}}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{}) // anonymous
	_, _, err := p.Handle(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	// Opt-out lets the anonymous caller past the auth gate; the empty
	// pool then surfaces Unavailable. The key point: NOT Unauthenticated.
	assert.Equal(t, status.Code(err), codes.Unavailable,
		"auth.require:false must let an anonymous caller through (then no-upstream Unavailable)")
}

// TestGrpcAuth_RuleWithoutAuthBlockHonoursDefaultRequire confirms a
// matched gRPC rule that omits an auth block still applies the global
// defaultRequire (anonymous denied), not silently served.
func TestGrpcAuth_RuleWithoutAuthBlockHonoursDefaultRequire(t *testing.T) {
	a := authenticatorWithDefaultRequire(t)
	p := authProxyForTest(t, a)
	p.pool = &GrpcUpstreamPool{}
	p.rules = []*GrpcRule{{Action: RuleActionAllow}} // matches all, no auth block

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{}) // anonymous
	_, _, err := p.Handle(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	assert.Equal(t, status.Code(err), codes.Unauthenticated,
		"a rule without an auth block must still honour defaultRequire")
}

// TestGrpcEnforcePolicy_StripsCredentialMetadata is the regression test
// for the credential-leak bug: the outgoing context handed to the
// upstream must NOT carry the auth credential metadata (API key / JWT),
// while non-credential metadata is preserved.
func TestGrpcEnforcePolicy_StripsCredentialMetadata(t *testing.T) {
	a := authenticatorWithDefaultRequire(t) // api-key in x-api-key
	p := authProxyForTest(t, a)             // defaultAction: allow

	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{
		"x-api-key": []string{"secret"}, // valid credential
		"x-keep":    []string{"v"},      // ordinary metadata
	})
	outCtx, err := p.enforcePolicy(ctx, "/cosmos.bank.v1beta1.Query/Balance")
	assert.NilError(t, err)

	out, ok := metadata.FromOutgoingContext(outCtx)
	assert.Assert(t, ok, "enforcePolicy must produce an outgoing context")
	assert.Equal(t, len(out.Get("x-api-key")), 0, "credential metadata must be stripped before upstream")
	assert.Equal(t, len(out.Get("x-keep")), 1, "non-credential metadata must be preserved")
	assert.Equal(t, out.Get("x-keep")[0], "v")
}

// TestGrpcEnforcePolicy_DoesNotPick is the regression test for the
// double-Pick bug: enforcePolicy must not advance the upstream picker
// (so a cacheable method's policy check / cache hit consumes no
// selection), while Handle picks exactly once.
func TestGrpcEnforcePolicy_DoesNotPick(t *testing.T) {
	p := authProxyForTest(t, nil) // no auth → straight to rule/default
	p.pool = newTestGrpcPool("round-robin", newTestGrpcUpstream("a", 1), newTestGrpcUpstream("b", 1))

	before := p.pool.idx.Load()
	for i := 0; i < 5; i++ {
		if _, err := p.enforcePolicy(context.Background(), "/svc/M"); err != nil {
			t.Fatalf("enforcePolicy err: %v", err)
		}
	}
	if got := p.pool.idx.Load(); got != before {
		t.Fatalf("enforcePolicy must not pick an upstream: idx advanced %d → %d", before, got)
	}

	// Handle (transparent director) picks exactly once.
	if _, _, err := p.Handle(context.Background(), "/svc/M"); err != nil {
		t.Fatalf("Handle err: %v", err)
	}
	if got := p.pool.idx.Load(); got != before+1 {
		t.Fatalf("Handle must pick exactly once: idx %d → %d", before, got)
	}
}

// TestGrpcHandle_StashesPickedUpstream is the regression test for
// transparent gRPC load/circuit accounting: Handle must stash the picked
// upstream on the returned context so rawTransparentHandler can bump
// inFlight and RecordOutcome on the SAME upstream it dialled. Without
// this, transparent (non-cached) traffic never affects least-conn or the
// circuit breaker.
func TestGrpcHandle_StashesPickedUpstream(t *testing.T) {
	p := authProxyForTest(t, nil) // no auth → straight to default-allow
	up := newTestGrpcUpstream("a", 1)
	p.pool = newTestGrpcPool("round-robin", up)

	outCtx, conn, err := p.Handle(context.Background(), "/svc/M")
	if err != nil {
		t.Fatalf("Handle err: %v", err)
	}
	if conn != up.conn {
		t.Fatal("Handle must return the picked upstream's conn")
	}
	if got := upstreamFromCtx(outCtx); got != up {
		t.Fatalf("Handle must stash the picked upstream on the context, got %v", got)
	}
}
