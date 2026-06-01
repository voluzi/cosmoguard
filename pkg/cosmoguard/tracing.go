package cosmoguard

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// tracingMu serializes SetupTracing calls and protects the
// active-provider pointer below.
var tracingMu sync.Mutex

// activeTracingShutdown holds the previous installation's shutdown
// hook. SetupTracing called a second time (e.g. tests, hot reload)
// runs this first to reap the prior TracerProvider's batch-span-
// processor goroutine before installing a fresh one.
var activeTracingShutdown func(context.Context) error

// TracingConfig configures OpenTelemetry tracing. Disabled by default;
// when enabled, cosmoguard emits one span per request and propagates
// W3C traceparent / tracestate headers upstream so the Cosmos node's
// logs / downstream services join the same trace.
//
//	tracing:
//	  enable: true
//	  endpoint: otel-collector:4317
//	  protocol: grpc        # grpc | http
//	  serviceName: cosmoguard
//	  sampleRate: 1.0       # 0.0 - 1.0; default 1.0
type TracingConfig struct {
	Enable      bool    `yaml:"enable,omitempty"`
	Endpoint    string  `yaml:"endpoint,omitempty"`
	Protocol    string  `yaml:"protocol,omitempty"`
	ServiceName string  `yaml:"serviceName,omitempty"`
	SampleRate  float64 `yaml:"sampleRate,omitempty"`
}

// tracerName is the instrumentation library identifier; appears as
// `otel.library.name` on every span cosmoguard emits.
const tracerName = "github.com/voluzi/cosmoguard"

// SetupTracing wires the global tracer provider when cfg.Enable is true.
// Returns a shutdown func the caller invokes during process teardown.
//
// On any setup failure we log + fall back to a no-op tracer rather than
// panicking — tracing is observability, not load-bearing.
func SetupTracing(ctx context.Context, cfg *TracingConfig) (func(context.Context) error, error) {
	tracingMu.Lock()
	defer tracingMu.Unlock()

	// Reap any previously-installed TracerProvider before swapping in
	// a new one. SetupTracing is called once per CosmoGuard.New, but
	// tests construct multiple harnesses and hot-reload paths may
	// re-enter — without this each call leaked the batch-span-
	// processor goroutine of the prior provider.
	if activeTracingShutdown != nil {
		_ = activeTracingShutdown(ctx)
		activeTracingShutdown = nil
	}

	if cfg == nil || !cfg.Enable {
		// Install the W3C propagator anyway so cosmoguard at minimum
		// passes existing traceparent headers through to upstream.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		// Reset the global tracer provider to a real no-op so spans
		// emitted after a Setup(enable=true) → Setup(enable=false)
		// transition don't hit a closed exporter. The previous code
		// re-installed otel.GetTracerProvider() which was the
		// just-shutdown provider — literally a no-op assignment.
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		noop := func(context.Context) error { return nil }
		activeTracingShutdown = noop
		return noop, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "cosmoguard"
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	// 0 disables sampling (no spans emitted); operator intent matches
	// docs' "0.0–1.0" range. Negative values clamp to 0; >1 to 1.
	sample := cfg.SampleRate
	if sample < 0 {
		sample = 0
	}
	if sample > 1.0 {
		sample = 1.0
	}

	var exp *otlptrace.Exporter
	var err error
	switch cfg.Protocol {
	case "http":
		exp, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithInsecure(),
		)
	default: // "grpc" or empty
		exp, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("tracing: build OTLP exporter: %w", err)
	}

	hostName, _ := os.Hostname()
	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.HostName(hostName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sample)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	slog.Info("tracing enabled",
		"endpoint", endpoint,
		"protocol", cfg.Protocol,
		"service", serviceName,
		"sample", sample,
	)

	activeTracingShutdown = tp.Shutdown
	return tp.Shutdown, nil
}

// httpHeaderCarrier adapts an http.Header to propagation.TextMapCarrier.
// Used by both the inbound extract (cosmoguard reads the client's
// traceparent) and the outbound inject (cosmoguard adds traceparent to
// the upstream request).
type httpHeaderCarrier http.Header

func (c httpHeaderCarrier) Get(key string) string { return http.Header(c).Get(key) }
func (c httpHeaderCarrier) Set(key, value string) { http.Header(c).Set(key, value) }
func (c httpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// StartHTTPSpan extracts traceparent from the inbound request and
// starts a server-kind span. The returned context replaces r.Context();
// callers must r.WithContext(ctx) before invoking handlers downstream
// so child spans nest correctly.
//
// Span name follows OTEL HTTP semconv: "{METHOD} {route}". We don't
// know the route at cosmoguard's layer, but using the full path as
// the span name explodes the tracing backend's name index when REST
// endpoints embed identifiers (every `/cosmos/bank/v1beta1/balances/<addr>`
// produces a unique name). Truncate to the first two path segments
// for the span NAME and keep the full path on `http.target` as an
// attribute — same routing fidelity, bounded cardinality.
func StartHTTPSpan(r *http.Request, proxyName string) (context.Context, trace.Span) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), httpHeaderCarrier(r.Header))
	tr := otel.GetTracerProvider().Tracer(tracerName)
	ctx, span := tr.Start(ctx, fmt.Sprintf("%s %s", r.Method, boundedSpanPath(r.URL.Path)),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
			attribute.String("cosmoguard.proxy", proxyName),
		),
	)
	return ctx, span
}

// boundedSpanPath returns the first two non-empty path segments of p
// (joined as "/a/b"), or the whole path if there are fewer. Bounds
// span-name cardinality so e.g.
// /cosmos/bank/v1beta1/balances/cosmos1abc... folds to /cosmos/bank.
// Empty / root path keeps the literal "/" so the span name stays
// non-empty and the operator can still see "GET /" vs "POST /".
func boundedSpanPath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	// strip leading "/" then take the first two segments
	trim := p
	if trim[0] == '/' {
		trim = trim[1:]
	}
	count := 0
	for i := 0; i < len(trim); i++ {
		if trim[i] == '/' {
			count++
			if count == 2 {
				return "/" + trim[:i]
			}
		}
	}
	return "/" + trim
}

// InjectHTTPHeaders writes the current span context into headers as
// W3C traceparent / tracestate. Called on the upstream-bound request
// inside the reverse-proxy Director.
func InjectHTTPHeaders(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, httpHeaderCarrier(h))
}

// markSpanOutcome stamps the span carried on ctx with the HTTP status
// code and rule action, and flips it to Error status for deny / 4xx /
// 5xx outcomes. Without this every span renders as a green success in
// the tracing backend regardless of what cosmoguard actually returned
// to the client — operators chasing a 401 storm or a 502 spike have
// no signal in the trace timeline. No-op when ctx carries no span
// (tracing disabled or non-traced call site).
func markSpanOutcome(ctx context.Context, status int, action string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attribute.Int("http.status_code", status),
		attribute.String("cosmoguard.action", action),
	)
	if status >= 400 || action == string(RuleActionDeny) {
		reason := fmt.Sprintf("status=%d action=%s", status, action)
		span.SetStatus(otelcodes.Error, reason)
	}
}
