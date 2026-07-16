package cosmoguard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

// TestFailClosedValidatorReturnsAuthUnavailable is the cubic #16 regression:
// a fail-closed external validator whose backend is down must return
// ErrAuthUnavailable (which both transports DENY on) — not a generic error
// that gets treated as anonymous / fail-open.
func TestFailClosedValidatorReturnsAuthUnavailable(t *testing.T) {
	// Point at a closed port so validateUpstream errors.
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	endpoint := down.URL
	down.Close() // now unreachable

	m, err := buildExternalValidatorMethod(AuthMethodConfig{
		Type:        "external-validator",
		Endpoint:    endpoint,
		Header:      "Authorization",
		Timeout:     200 * time.Millisecond,
		FailureMode: "fail-closed",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer sometoken")

	_, resolveErr := m.Resolve(r)
	if !errors.Is(resolveErr, ErrAuthUnavailable) {
		t.Fatalf("fail-closed validator outage should return ErrAuthUnavailable, got %v", resolveErr)
	}
}

// TestConsumeHalfOpenProbe_SingleWinner is the cubic #10/#12 regression: only
// one concurrent caller may win the half-open probe token; losers get false
// and must not dispatch.
func TestConsumeHalfOpenProbe_SingleWinner(t *testing.T) {
	u := &GrpcUpstream{cbConfig: enabledBreaker(3, 10*time.Millisecond)}
	u.cbOpen.Store(true)
	u.cbOpenedAtUnixMs.Store(time.Now().Add(-time.Second).UnixMilli()) // cooldown elapsed

	// First caller wins; the token is now consumed (openedAt == 0).
	if !u.consumeHalfOpenProbe() {
		t.Fatal("first caller should win the probe token")
	}
	// Every subsequent caller must lose until RecordOutcome resolves it.
	for i := 0; i < 3; i++ {
		if u.consumeHalfOpenProbe() {
			t.Fatalf("caller %d must NOT win a second probe while one is in flight", i)
		}
	}
	// A closed breaker always admits.
	u2 := &GrpcUpstream{}
	if !u2.consumeHalfOpenProbe() {
		t.Fatal("closed breaker must admit")
	}
}

// TestPickReservesLease is the cubic #9 regression: gRPC Pick reserves the
// in-flight lease atomically with selection, so there is no Pick→dispatch gap.
func TestPickReservesLease(t *testing.T) {
	a := newTestGrpcUpstream("a", 1)
	pool := newTestGrpcPool("weighted-round-robin", a)
	got := pool.Pick()
	if got != a {
		t.Fatal("expected a")
	}
	if a.inFlight.Load() != 1 {
		t.Fatalf("Pick must reserve an in-flight lease; inFlight=%d want 1", a.inFlight.Load())
	}
}

// TestCorsDeclChanged is the codex #25 regression: comparing CORS configs must
// ignore the compiled closure so an unchanged cors block doesn't block reloads.
func TestCorsDeclChanged(t *testing.T) {
	a := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://x"}}
	b := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://x"}}
	// Compile populates the closure fields on both — a whole-struct DeepEqual
	// would now report them unequal.
	if err := a.Compile(); err != nil {
		t.Fatal(err)
	}
	if err := b.Compile(); err != nil {
		t.Fatal(err)
	}
	if corsDeclChanged(a, b) {
		t.Fatal("identical declarative CORS must compare equal despite compiled closures")
	}
	c := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://y"}}
	if !corsDeclChanged(a, c) {
		t.Fatal("a real origin change must be detected")
	}
	// nil vs [] declarative fields are behaviour-neutral → equal.
	d := &CORSConfig{Enable: false}
	e := &CORSConfig{Enable: false, AllowedOrigins: []string{}}
	if corsDeclChanged(d, e) {
		t.Fatal("nil vs empty origin list must compare equal")
	}
}

// TestGrpcCacheKeyMetadata_ValueBoundaries is the cubic #2 regression: distinct
// multi-valued metadata must not collide (["a,b"] != ["a","b"]).
func TestGrpcCacheKeyMetadata_ValueBoundaries(t *testing.T) {
	keys := []string{"x-h"}
	ctxSingle := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-h", "a,b"))
	ctxDouble := metadata.NewIncomingContext(context.Background(),
		metadata.MD{"x-h": []string{"a", "b"}})
	if grpcCacheKeyMetaPart(ctxSingle, keys) == grpcCacheKeyMetaPart(ctxDouble, keys) {
		t.Fatal(`["a,b"] and ["a","b"] must produce different metadata parts`)
	}
}

// TestEffectiveServerLimits is the codex #30 regression: an explicit 0 means
// "no limit" and must survive (nil → default).
func TestEffectiveServerLimits(t *testing.T) {
	zero := int64(0)
	s := &ServerConfig{MaxRequestBody: &zero, WSReadLimit: &zero}
	if s.EffectiveMaxRequestBody() != 0 {
		t.Fatal("explicit maxRequestBody:0 must stay 0 (no limit)")
	}
	if s.EffectiveWSReadLimit() != 0 {
		t.Fatal("explicit wsReadLimit:0 must stay 0 (no limit)")
	}
	unset := &ServerConfig{}
	if unset.EffectiveMaxRequestBody() != defaultMaxRequestBody {
		t.Fatalf("unset maxRequestBody must default to %d", defaultMaxRequestBody)
	}
	if unset.EffectiveWSReadLimit() != defaultServerWSRead {
		t.Fatalf("unset wsReadLimit must default to %d", defaultServerWSRead)
	}
}

// TestAcceptEncodingKey_QValues is the codex #28 regression: gzip;q=0
// (explicitly forbidden) must NOT appear in the acceptable set.
func TestAcceptEncodingKey_QValues(t *testing.T) {
	if got := acceptEncodingKey("gzip;q=0, br"); strings.Contains(got, "gzip") {
		t.Fatalf("gzip;q=0 must be excluded, got %q", got)
	}
	if got := acceptEncodingKey("gzip;q=0"); got != "identity;q=1" {
		t.Fatalf("gzip;q=0 with nothing else acceptable must be identity;q=1, got %q", got)
	}
	if got := acceptEncodingKey("gzip;q=0.5, br;q=1"); !strings.Contains(got, "gzip") || !strings.Contains(got, "br") {
		t.Fatalf("gzip and br (q>0) should both be present, got %q", got)
	}
	// identity;q=0 excludes the implicit identity coding.
	if got := acceptEncodingKey("gzip, identity;q=0"); strings.Contains(got, "identity") {
		t.Fatalf("identity;q=0 must exclude identity, got %q", got)
	}
	// Opposite rankings of the same coding set must produce different keys
	// (a q-honouring upstream could pick different encodings).
	if acceptEncodingKey("gzip;q=1, br;q=0.5") == acceptEncodingKey("gzip;q=0.5, br;q=1") {
		t.Fatal("opposite gzip/br rankings must produce different keys")
	}
}

// TestValidateServerLimits_RejectsNegative is the cubic ws_proxy.go:109
// follow-up: a negative maxRequestBody/wsReadLimit must be rejected (it would
// otherwise slip past the `> 0` cap check and remove the limit).
func TestValidateServerLimits_RejectsNegative(t *testing.T) {
	neg := int64(-1)
	if err := validateServerLimits(&ServerConfig{WSReadLimit: &neg}); err == nil {
		t.Fatal("negative wsReadLimit must be rejected")
	}
	if err := validateServerLimits(&ServerConfig{MaxRequestBody: &neg}); err == nil {
		t.Fatal("negative maxRequestBody must be rejected")
	}
	zero := int64(0)
	if err := validateServerLimits(&ServerConfig{WSReadLimit: &zero, MaxRequestBody: &zero}); err != nil {
		t.Fatalf("explicit 0 (no limit) must be accepted, got %v", err)
	}
	if err := validateServerLimits(&ServerConfig{}); err != nil {
		t.Fatalf("unset limits must be accepted, got %v", err)
	}
}

// TestServerReload_EffectiveLimitsNeutral is the cubic cosmoguard.go:1033
// follow-up: spelling a limit as its default explicitly must NOT be seen as a
// change (so a rules-only reload isn't rejected).
func TestServerReload_EffectiveLimitsNeutral(t *testing.T) {
	defBody := defaultMaxRequestBody
	defWS := defaultServerWSRead
	unset := &ServerConfig{}
	explicit := &ServerConfig{MaxRequestBody: &defBody, WSReadLimit: &defWS}
	if serverRuntimeImmutableChanged(unset, explicit) {
		t.Fatal("spelling the default explicitly must be behaviour-neutral")
	}
	other := int64(defBody + 1)
	changed := &ServerConfig{MaxRequestBody: &other}
	if !serverRuntimeImmutableChanged(unset, changed) {
		t.Fatal("a real limit change must be detected")
	}
}

// TestDetectRemovedConfigKeys_Backend is the codex config.go:769 follow-up:
// a removed cache.backend must fail loudly.
func TestDetectRemovedConfigKeys_Backend(t *testing.T) {
	if err := detectRemovedConfigKeys([]byte("cache:\n  backend: redis\n")); err == nil {
		t.Fatal("cache.backend must be rejected as a removed key")
	}
	if err := detectRemovedConfigKeys([]byte("cache:\n  redis: redis://x\n")); err == nil {
		t.Fatal("cache.redis must still be rejected")
	}
	if err := detectRemovedConfigKeys([]byte("cache:\n  ttl: 5s\n")); err != nil {
		t.Fatalf("a valid v4 cache block must pass, got %v", err)
	}
}

// TestAcceptEncodingKey_WildcardExclusion is the codex http_proxy.go:806
// follow-up: `*, gzip;q=0` must NOT collide with plain `*`.
func TestAcceptEncodingKey_WildcardExclusion(t *testing.T) {
	if acceptEncodingKey("*, gzip;q=0") == acceptEncodingKey("*") {
		t.Fatal("*, gzip;q=0 must differ from * (it forbids gzip)")
	}
	// Without a wildcard, an exclusion is equivalent to omission (better dedup).
	if acceptEncodingKey("br, gzip;q=0") != acceptEncodingKey("br") {
		t.Fatal("non-wildcard exclusion should dedup with plain omission")
	}
}

// TestLimiterForFailedInit is the codex pipeline_middleware.go:173 follow-up:
// a fail-closed rule whose limiter can't be built gets a failing sentinel
// (which denies), a fail-open rule gets nil (runs unlimited).
func TestLimiterForFailedInit(t *testing.T) {
	closed := &RateLimitConfig{FailureMode: "fail-closed"}
	l := limiterForFailedInit(closed, errTest)
	if l == nil {
		t.Fatal("fail-closed rule must get a sentinel limiter")
	}
	if allowed, _, err := l.Allow(context.Background(), "k"); allowed || err == nil {
		t.Fatalf("sentinel must deny with an error; allowed=%v err=%v", allowed, err)
	}
	open := &RateLimitConfig{} // default fail-open
	if limiterForFailedInit(open, errTest) != nil {
		t.Fatal("fail-open rule must get nil (runs unlimited)")
	}
}

var errTest = errors.New("boom")
