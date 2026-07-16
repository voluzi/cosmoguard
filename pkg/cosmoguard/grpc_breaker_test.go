package cosmoguard

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func enabledBreaker(consec int, cooldown time.Duration) *CircuitBreakerConfig {
	on := true
	return &CircuitBreakerConfig{Enable: &on, ConsecutiveFailures: consec, CooldownPeriod: cooldown}
}

// TestCircuitOpen_ReadOnlyDoesNotConsumeProbe is the G2 regression: the
// healthySet scan calls CircuitOpen() for every upstream, so it MUST NOT
// consume the single half-open probe token — otherwise an upstream the
// picker doesn't select is wedged open forever.
func TestCircuitOpen_ReadOnlyDoesNotConsumeProbe(t *testing.T) {
	u := &GrpcUpstream{cbConfig: enabledBreaker(3, 10*time.Millisecond)}
	// Trip the breaker.
	u.cbOpen.Store(true)
	u.cbOpenedAtUnixMs.Store(time.Now().Add(-time.Second).UnixMilli()) // cooldown already elapsed

	// Repeated read-only checks (as healthySet does across upstreams) must
	// keep reporting probe-eligible (open==false → routable) without
	// consuming the token: openedAt stays non-zero.
	for i := 0; i < 5; i++ {
		if u.CircuitOpen() {
			t.Fatalf("iteration %d: cooldown elapsed, upstream should be probe-eligible (CircuitOpen=false)", i)
		}
		if u.cbOpenedAtUnixMs.Load() == 0 {
			t.Fatalf("iteration %d: read-only CircuitOpen consumed the probe token", i)
		}
	}

	// Only consumeHalfOpenProbe (called for the SELECTED upstream) takes
	// the token; afterwards the breaker rejects further callers until the
	// outcome lands.
	u.consumeHalfOpenProbe()
	if u.cbOpenedAtUnixMs.Load() != 0 {
		t.Fatal("consumeHalfOpenProbe should have taken the token (openedAt=0)")
	}
	if !u.CircuitOpen() {
		t.Fatal("with a probe in flight (openedAt=0) further callers must be rejected")
	}

	// A successful outcome closes the breaker (no wedge).
	u.RecordOutcomeErr(nil)
	if u.CircuitOpen() {
		t.Fatal("successful probe should have closed the breaker")
	}
}

// TestRecordOutcomeErr_ClientErrorsDoNotTrip is the G4 regression: client-
// caused / application-level gRPC statuses must not open the breaker.
func TestRecordOutcomeErr_ClientErrorsDoNotTrip(t *testing.T) {
	clientCaused := []codes.Code{
		codes.Canceled, codes.DeadlineExceeded, codes.InvalidArgument,
		codes.NotFound, codes.PermissionDenied, codes.Unauthenticated,
	}
	for _, code := range clientCaused {
		u := &GrpcUpstream{cbConfig: enabledBreaker(3, time.Minute)}
		for i := 0; i < 10; i++ {
			u.RecordOutcomeErr(status.Error(code, "client"))
		}
		if u.cbOpen.Load() {
			t.Fatalf("code %v: breaker tripped on client-caused errors", code)
		}
	}

	// Genuine upstream failures DO trip it.
	u := &GrpcUpstream{cbConfig: enabledBreaker(3, time.Minute)}
	for i := 0; i < 3; i++ {
		u.RecordOutcomeErr(status.Error(codes.Unavailable, "down"))
	}
	if !u.cbOpen.Load() {
		t.Fatal("breaker should trip after ConsecutiveFailures Unavailable errors")
	}
}

// TestRecordOutcomeErr_ApplicationRejectionCountsAsSuccess mirrors the HTTP
// breaker's `<500` handling: an application-level gRPC status (the upstream
// responded, it just rejected the request) must reset consecFails and close
// a half-open breaker, not merely leave it untouched.
func TestRecordOutcomeErr_ApplicationRejectionCountsAsSuccess(t *testing.T) {
	// Resets consecFails so it takes ConsecutiveFailures fresh failures to trip.
	u := &GrpcUpstream{cbConfig: enabledBreaker(3, time.Minute)}
	u.RecordOutcomeErr(status.Error(codes.Unavailable, "down"))
	u.RecordOutcomeErr(status.Error(codes.Unavailable, "down"))
	u.RecordOutcomeErr(status.Error(codes.NotFound, "not found")) // should reset
	if u.cbConsecFails.Load() != 0 {
		t.Fatalf("application rejection should reset consecFails, got %d", u.cbConsecFails.Load())
	}
	u.RecordOutcomeErr(status.Error(codes.Unavailable, "down"))
	u.RecordOutcomeErr(status.Error(codes.Unavailable, "down"))
	if u.cbOpen.Load() {
		t.Fatal("breaker should not have tripped: only 2 consecutive failures since the reset")
	}

	// Closes a half-open probe.
	probe := &GrpcUpstream{cbConfig: enabledBreaker(3, 10*time.Millisecond)}
	probe.cbOpen.Store(true)
	probe.cbOpenedAtUnixMs.Store(time.Now().Add(-time.Second).UnixMilli())
	probe.consumeHalfOpenProbe()
	probe.RecordOutcomeErr(status.Error(codes.InvalidArgument, "bad request"))
	if probe.CircuitOpen() {
		t.Fatal("application rejection on the half-open probe should close the breaker")
	}
}

// TestRecordOutcomeErr_ClientErrorReArmsHalfOpen: a neutral (client-caused)
// outcome on the probe RPC must re-arm the cooldown rather than leaving the
// consumed token unresolved (which would wedge the upstream).
func TestRecordOutcomeErr_ClientErrorReArmsHalfOpen(t *testing.T) {
	u := &GrpcUpstream{cbConfig: enabledBreaker(3, 10*time.Millisecond)}
	u.cbOpen.Store(true)
	u.cbOpenedAtUnixMs.Store(time.Now().Add(-time.Second).UnixMilli())
	u.consumeHalfOpenProbe() // openedAt -> 0 (probe in flight)

	// Probe RPC returns a client cancel: not an upstream signal, but must
	// re-arm so the upstream is probed again instead of staying wedged.
	u.RecordOutcomeErr(status.Error(codes.Canceled, "client gone"))
	if u.cbOpenedAtUnixMs.Load() == 0 {
		t.Fatal("client-caused probe outcome should re-arm the cooldown (openedAt != 0)")
	}
	if !u.cbOpen.Load() {
		t.Fatal("breaker should remain open after a neutral probe outcome")
	}
}

// TestPick_EmptyPoolReturnsNil guards the G1 panic path: a drained pool
// must yield nil, never divide-by-zero in a picker.
func TestPick_EmptyPoolReturnsNil(t *testing.T) {
	p := &GrpcUpstreamPool{strategy: "least-conn"}
	p.storeUpstreams([]*GrpcUpstream{})
	if got := p.Pick(); got != nil {
		t.Fatalf("Pick on empty pool = %v; want nil", got)
	}
	_ = context.Background()
}
