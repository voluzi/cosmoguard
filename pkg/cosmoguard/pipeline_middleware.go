package cosmoguard

import (
	"context"
	"errors"
	"math"
	"net/http"
	"runtime/debug"
)

// MWPanicRecovery installs panic recovery as the outermost layer. A
// recovered panic logs the stack and returns Decision{Stop: true,
// Action: "deny", HTTPStatus: 500} so the adapter can write a 500.
//
// Distinct from the per-protocol `recoverHTTP` / `recoverStream`
// helpers: those guard the raw protocol entry; this guards the
// middleware chain itself. Both apply.
func MWPanicRecovery(logger *Entry) Middleware {
	return func(req Request, next Next) (decision Decision) {
		defer func() {
			if rec := recover(); rec != nil {
				if logger != nil {
					logger.WithFields(Fields{
						"panic":     rec,
						"protocol":  req.Protocol(),
						"operation": req.OperationName(),
						"stack":     string(debug.Stack()),
					}).Error("panic inside middleware chain")
				}
				decision = Decision{Stop: true, Action: "deny", HTTPStatus: http.StatusInternalServerError, Reason: "internal error"}
			}
		}()
		return next(req)
	}
}

// MWIdentityResolve calls auth.Resolve once and stashes the result on
// the Request (and the RequestStats on the context, so downstream
// metric labels see it). nil-safe: when auth is disabled or anonymous,
// the request continues with identity = nil. ErrReplay short-circuits.
func MWIdentityResolve(auth *Authenticator, httpReq func(Request) *http.Request) Middleware {
	return func(req Request, next Next) Decision {
		if auth == nil {
			return next(req)
		}
		// Identity-resolve takes an *http.Request; the adapter
		// provides one (HTTP and JSON-RPC HTTP carry one natively;
		// WS/gRPC adapters synthesize a minimal one). When the
		// adapter returns nil, identity-resolve degrades to no-op.
		hr := httpReq(req)
		if hr == nil {
			return next(req)
		}
		id, err := auth.Resolve(hr)
		if err != nil {
			// errors.Is so a future wrapped error (e.g. fmt.Errorf("...: %w", ErrReplay))
			// still trips replay denial — the previous `err == ErrReplay`
			// shape would have silently downgraded to fail-open on any
			// wrapping. Security-critical path: prefer the contract that
			// holds under wrapping.
			if errors.Is(err, ErrReplay) {
				return Decision{Stop: true, Action: "deny", HTTPStatus: http.StatusUnauthorized, Reason: "token replayed"}
			}
			// ErrInvalidCredential: the request presented a credential
			// that failed verification (forged/expired JWT, validator
			// said no). Deny even when the rule wouldn't otherwise
			// require auth — anonymising a request that actively
			// presented a bad credential would let the next configured
			// method silently accept the same header value, defeating
			// the chain's first verifier.
			if errors.Is(err, ErrInvalidCredential) {
				return Decision{Stop: true, Action: "deny", HTTPStatus: http.StatusUnauthorized, Reason: "invalid credential"}
			}
			// Other resolve errors (transient backend failures, etc.):
			// treat as anonymous (fail-open), matching the pre-pipeline
			// behaviour. The rule's auth gate will deny if it requires
			// an identity.
			return next(req)
		}
		req.SetIdentity(id)
		if id != nil {
			if stats := RequestStatsFromCtx(req.Context()); stats != nil {
				stats.IdentityName = id.Name
			}
			// Stash the full Identity (not just the name) on the
			// http.Request context so JSON-RPC dispatch — which runs
			// after the gate chain finishes and the pipeline Request
			// goes out of scope — can read the resolved scopes to
			// enforce per-rule auth gates.
			if hr := httpReq(req); hr != nil {
				*hr = *hr.WithContext(context.WithValue(hr.Context(), identityCtxKey{}, id))
			}
		}
		return next(req)
	}
}

// identityCtxKey carries the resolved *Identity on an
// http.Request.Context() for downstream handlers (JSON-RPC, gRPC)
// that run after MWIdentityResolve sets it.
type identityCtxKey struct{}

// MWAuthGate checks a rule's auth requirement against the resolved
// identity. The per-call accessor returns the matched rule's
// RuleAuthConfig (rules are protocol-specific so this can't be a
// global), or nil when the matched rule omits an `auth:` block or no
// rule matched.
//
// A nil rule-auth does NOT skip the gate: it falls back to the global
// auth.defaultRequire policy via Authorize(nil, id). Returning early on
// nil used to let an unauthenticated request reach an allowed route
// whenever the rule omitted `auth:`, defeating fail-closed
// defaultRequire unless every rule repeated `auth.require: true`. When
// defaultRequire is unset, Authorize(nil, id) is a pass-through, so the
// no-auth-configured deployment is unaffected.
func MWAuthGate(auth *Authenticator, ruleAuth func(Request) *RuleAuthConfig) Middleware {
	return func(req Request, next Next) Decision {
		if auth == nil {
			return next(req)
		}
		ok, reason := auth.Authorize(ruleAuth(req), req.Identity())
		if !ok {
			return Decision{Stop: true, Action: "deny", HTTPStatus: http.StatusUnauthorized, Reason: reason}
		}
		return next(req)
	}
}

// MWRateLimit applies a rule's rate limiter when one is configured.
// The per-call accessors yield the rule's RateLimitConfig + bucket
// fingerprint; ratelimitFor maps fingerprint to the (already-built)
// RateLimiter. nil-safe at every step.
//
// On denial, surfaces RetryAfter in whole seconds (rounded up per
// RFC 7231 — truncating a 1.5s wait to 1 would let the client retry
// early and trip the limiter again). Minimum of 1s so the header is
// never `Retry-After: 0`. On limiter error, fails open — same policy
// the pre-pipeline code used so a Redis hiccup doesn't take down all
// requests.
func MWRateLimit(
	rateConfigFor func(Request) (*RateLimitConfig, uint64),
	limiterFor func(uint64) RateLimiter,
	httpReqFor func(Request) *http.Request,
	logger *Entry,
) Middleware {
	return func(req Request, next Next) Decision {
		cfg, fp := rateConfigFor(req)
		if cfg == nil {
			return next(req)
		}
		limiter := limiterFor(fp)
		if limiter == nil {
			return next(req)
		}
		hr := httpReqFor(req)
		if hr == nil {
			return next(req)
		}
		idName := ""
		if req.Identity() != nil {
			idName = req.Identity().Name
		}
		key := rateLimitKey(cfg.Scope, fp, hr, idName)
		allowed, retry, err := limiter.Allow(req.Context(), key)
		if err != nil {
			if cfg.FailClosed() {
				if logger != nil {
					logger.WithError(err).Warn("rate limiter error; failing closed (denying)")
				}
				return Decision{
					Stop:       true,
					Action:     "deny",
					HTTPStatus: http.StatusTooManyRequests,
					Reason:     "rate limiter unavailable",
					RetryAfter: 1,
				}
			}
			if logger != nil {
				logger.WithError(err).Warn("rate limiter error; failing open")
			}
			return next(req)
		}
		if !allowed {
			ra := int(math.Ceil(retry.Seconds()))
			if ra < 1 {
				ra = 1
			}
			return Decision{
				Stop:       true,
				Action:     "deny",
				HTTPStatus: http.StatusTooManyRequests,
				Reason:     "rate limit exceeded",
				RetryAfter: ra,
			}
		}
		return next(req)
	}
}
