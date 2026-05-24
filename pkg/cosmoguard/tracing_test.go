package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TestPropagator_AlwaysInstalled: even when tracing is disabled, the
// W3C propagator is installed so cosmoguard passes existing
// traceparent headers through to upstream rather than swallowing them.
func TestPropagator_AlwaysInstalled(t *testing.T) {
	// Force a fresh propagator-less state.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	_, err := SetupTracing(context.Background(), &TracingConfig{Enable: false})
	if err != nil {
		t.Fatalf("setup tracing (disabled): %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	r.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), httpHeaderCarrier(r.Header))
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("propagator did not extract a valid span context")
	}
	if want := "0af7651916cd43dd8448eb211c80319c"; sc.TraceID().String() != want {
		t.Fatalf("trace id mismatch: got %s want %s", sc.TraceID().String(), want)
	}
}

// TestInjectHTTPHeaders_PreservesIncoming: when the inbound request
// already has a traceparent, extracting + re-injecting keeps a valid
// header on the outbound request.
func TestInjectHTTPHeaders_PreservesIncoming(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	r.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	ctx, span := StartHTTPSpan(r, "test")
	defer span.End()

	out := httptest.NewRequest(http.MethodGet, "http://upstream/", nil)
	InjectHTTPHeaders(ctx, out.Header)

	tp := out.Header.Get("traceparent")
	if tp == "" {
		t.Fatal("expected traceparent on the outbound request")
	}
}
