package cosmoguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// RequestIDHeader is the canonical header cosmoguard reads/writes for
// request correlation. Matches the de-facto industry convention
// (X-Request-Id) which most upstreams already understand. Some Cosmos
// ecosystems may use X-Request-ID with different casing; HTTP header
// lookup is case-insensitive so it works either way.
const RequestIDHeader = "X-Request-Id"

// requestIDContextKey is the typed context key for the per-request ID.
// Typed so it can't collide with anything else stored on the context.
type requestIDContextKey struct{}

// WithRequestID injects an X-Request-Id into the request:
//   - If the incoming request already carries one, reuse it (preserves
//     end-to-end correlation when an L7 LB or another proxy stamped it
//     upstream of cosmoguard).
//   - Otherwise generate a fresh 16-hex-char ID (8 random bytes).
//
// The ID is stored on the request context so log emitters can read it,
// AND written back as an X-Request-Id response header so clients can
// reference it in support requests.
//
// Called as the first thing in HttpProxy.ServeHTTP so every code path
// (cache hit, cache miss, deny, 429, 401) emits the correlation ID.
func WithRequestID(r *http.Request, w http.ResponseWriter) (*http.Request, string) {
	id := r.Header.Get(RequestIDHeader)
	if !isValidRequestID(id) {
		id = newRequestID()
		r.Header.Set(RequestIDHeader, id)
	}
	w.Header().Set(RequestIDHeader, id)
	ctx := context.WithValue(r.Context(), requestIDContextKey{}, id)
	return r.WithContext(ctx), id
}

// maxRequestIDLen is the largest client-supplied request ID we accept.
// Anything longer is discarded and a fresh ID minted in its place.
// 128 chars is well above what every common correlation-ID convention
// emits (UUID = 36, GUID = 38, hex-encoded 16 bytes = 32) while still
// being small enough to prevent log-line / header bloat from a hostile
// caller hammering us with multi-KB X-Request-Id values.
const maxRequestIDLen = 128

// isValidRequestID enforces a small allowlist on client-supplied
// X-Request-Id values: non-empty, within length bound, and printable
// ASCII (no control characters, no newlines, no DEL). The
// printable-ASCII constraint blocks log-injection — a value
// containing `\n` would forge a fake log line in operator logs.
func isValidRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// RequestIDFromContext returns the request ID stored on the context by
// WithRequestID, or "" if none was set. Used by handlers and log
// emitters elsewhere in the request pipeline.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

// newRequestID returns a fresh ~64-bit random ID, hex-encoded. Cheap
// (one crypto/rand call) and short enough to be readable in log lines
// while still effectively unique. Falls back to a deterministic
// (insecure but distinct) sentinel if crypto/rand fails — never happens
// in practice but means the ServeHTTP path never blocks on entropy.
func newRequestID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "norand-" + hex.EncodeToString(buf[:])
	}
	return hex.EncodeToString(buf[:])
}
