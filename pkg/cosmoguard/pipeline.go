package cosmoguard

import (
	"context"
)

// Phase D's unified pipeline. Each protocol handler (HTTP, JSON-RPC,
// WS, gRPC) wraps its incoming request in a Request that satisfies
// this interface and runs it through a Chain of Middleware. The
// per-protocol entry points stay thin: parse the wire format → build
// a Request → run the chain → write the protocol-specific response.
//
// Middleware is composable: any layer can short-circuit by returning
// a non-Continue Decision, or pass control to `next` for the rest of
// the chain. The standard chain is roughly:
//
//   PanicRecovery → RequestID → Tracing → IdentityResolve →
//   RuleMatch → AuthGate → RateLimitGate → CacheLookup → Forward
//
// Not every middleware applies to every protocol. WS, for example,
// has no HTTP-style cache; its chain stops at the rate-limit / auth
// gate and the broker call replaces "Forward". The middleware
// machinery itself doesn't care — middlewares that aren't relevant
// to a protocol are simply omitted from its chain.

// Request is the protocol-agnostic view of a single inbound operation.
// HTTP/JSON-RPC/WS/gRPC each have their own concrete implementations
// that adapt the wire-level request into this shape.
type Request interface {
	// Context returns the request's context — carrying request-id,
	// trace span, RequestStats, and any per-request cancellation.
	Context() context.Context

	// WithContext returns a copy of this Request with ctx replacing
	// its current context. Middlewares use this to enrich the
	// context (e.g. attach the resolved identity) before passing it
	// down the chain.
	WithContext(ctx context.Context) Request

	// Protocol identifies which proxy created this Request: "http",
	// "jsonrpc", "ws", "grpc". Lets middleware code branch when the
	// gate behavior differs across protocols.
	Protocol() string

	// OperationName is the protocol-native method name. For HTTP this
	// is the path; for JSON-RPC it's the method ("status",
	// "broadcast_tx_sync"); for gRPC it's "/pkg.Svc/Method". Used by
	// rule matchers, span names, and log lines.
	OperationName() string

	// SourceIP returns the inferred client IP. Backed by the same
	// X-Forwarded-For / X-Real-IP / RemoteAddr ladder used elsewhere.
	SourceIP() string

	// RuleTag is the operator-supplied tag of the matched rule, used
	// as the `rule_id` metric label. Empty until a RuleMatch
	// middleware sets it.
	RuleTag() string
	SetRuleTag(string)

	// Identity is the resolved authenticated identity for the
	// request, or nil for anonymous. Populated by IdentityResolve.
	Identity() *Identity
	SetIdentity(*Identity)
}

// Decision is the outcome a Middleware returns. Continue means "I did
// my job, hand it to the next middleware". A non-Continue Decision
// short-circuits — the caller writes the protocol-specific response
// and stops walking the chain.
//
// Continue is the zero value so the common case (`return Decision{}`)
// is cheap to type.
type Decision struct {
	// Stop, when true, means the chain should not call the next
	// middleware. The Reason / RetryAfter fields are set by the
	// originating middleware and consumed by the per-protocol
	// adapter to write the right response.
	Stop bool

	// Action is "allow" or "deny" — the cosmoguard label value.
	// Empty when the middleware doesn't take a position (e.g.
	// PanicRecovery on the happy path).
	Action string

	// Reason is a short human-readable string used in audit logs and
	// optionally in the response body when the protocol supports it.
	Reason string

	// HTTPStatus is the suggested HTTP status code; ignored by
	// non-HTTP adapters. 0 means "the adapter picks a default".
	HTTPStatus int

	// RetryAfter is the duration to surface as a Retry-After header
	// (or equivalent) when Stop=true for rate limiting.
	RetryAfter int
}

// Continue is the convenience return value for middlewares that have
// nothing to say.
var Continue = Decision{}

// Next is the type passed to each Middleware so it can invoke the rest
// of the chain.
type Next func(Request) Decision

// Middleware is the chain's building block. The standard signature
// pattern — `func(req Request, next Next) Decision` — lets each
// middleware decide whether to short-circuit (`return Decision{Stop:
// true, ...}`) or delegate (`return next(req)`).
type Middleware func(Request, Next) Decision

// Chain composes multiple Middleware into a single Next. The first
// middleware is the outermost (runs first); the last is innermost.
// PanicRecovery should be first in the slice so it can recover panics
// from every layer below.
func Chain(mws ...Middleware) Next {
	// Inner-to-outer composition: start with a terminal that just
	// returns Continue, then wrap each middleware around it in
	// reverse order.
	final := Next(func(Request) Decision { return Continue })
	for i := len(mws) - 1; i >= 0; i-- {
		mw := mws[i]
		nxt := final
		final = func(r Request) Decision { return mw(r, nxt) }
	}
	return final
}
