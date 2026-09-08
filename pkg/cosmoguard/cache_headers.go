package cosmoguard

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// alwaysPreservedHeaders is the set of upstream response headers always
// retained in cosmoguard's cache so a hit replays the same response shape
// the upstream would have produced. Operators can extend this set per-rule
// via cache.preserveHeaders.
//
// Comparison is case-insensitive (HTTP header names are per RFC 7230).
var alwaysPreservedHeaders = []string{
	"Content-Type",
	"Content-Encoding",
	"Cache-Control",
	"ETag",
	"Vary",
	// Last-Modified is the other half of conditional-GET (paired
	// with ETag). Upstreams that emit it expect clients to send
	// If-Modified-Since on the next request and produce a 304; if
	// cosmoguard strips it across a cache hit, every replay starts
	// the cache window fresh and clients lose the 304 path.
	"Last-Modified",
	// Note: Age is intentionally NOT preserved. We capture the upstream
	// Age at store time into CachedResponse.UpstreamAge and emit a
	// freshly-computed Age on every cache hit (RFC 7234 §5.1):
	// downstream_Age = UpstreamAge + (now - StoredAt). Pass-through
	// would re-emit the stale upstream value on every replay, leaving
	// downstream caches with no signal that the response actually aged
	// while sitting in cosmoguard's cache.
}

func cacheControlRequiresRevalidation(value string) bool {
	for _, raw := range strings.Split(value, ",") {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "must-revalidate", "proxy-revalidate":
			return true
		}
	}
	return false
}

func upstreamHTTPStaleWindow(headers http.Header, configured time.Duration) time.Duration {
	if cacheControlRequiresRevalidation(strings.Join(headers.Values("Cache-Control"), ",")) {
		return 0
	}
	return configured
}

func cachedHTTPStaleWindow(response CachedResponse, configured time.Duration) time.Duration {
	if cacheControlRequiresRevalidation(response.Headers[http.CanonicalHeaderKey("Cache-Control")]) {
		return 0
	}
	return configured
}

// hopByHopHeaders are RFC 7230 section 6.1 hop-by-hop headers that must
// never be cached or forwarded — they belong to a specific TCP/TLS hop,
// not to the response payload. Always stripped, even if an operator
// explicitly lists them in cache.preserveHeaders.
var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

type cacheKeyVaryPolicy uint8

const (
	jsonRPCCacheKeyVary cacheKeyVaryPolicy = iota
	httpCacheKeyVary
)

func (p cacheKeyVaryPolicy) includes(field string) bool {
	return p == httpCacheKeyVary && strings.EqualFold(field, "Accept-Encoding")
}

// cacheableByVary reports whether every upstream Vary dimension is represented
// in the selected cache key. An invalid policy permits no dimensions.
func cacheableByVary(upstream http.Header, policy cacheKeyVaryPolicy) bool {
	// An upstream can legally send Vary across MULTIPLE header lines; Get
	// returns only the first, so a response with `Vary: Accept-Encoding`
	// followed by `Vary: Authorization` would otherwise slip through. Walk
	// every value.
	for _, vary := range upstream.Values("Vary") {
		for _, field := range strings.Split(vary, ",") {
			f := strings.ToLower(strings.TrimSpace(field))
			if f == "" {
				continue
			}
			// "*" means the response varies on unspecified request
			// characteristics — never cacheable (RFC 7234 §4.1).
			if f == "*" {
				return false
			}
			if policy.includes(f) {
				continue
			}
			return false
		}
	}
	return true
}

type upstreamVaryCaptureContextKey struct{}

type upstreamVaryCapture struct {
	values   []string
	observed bool
}

func withUpstreamVaryCapture(r *http.Request) (*http.Request, *upstreamVaryCapture) {
	capture := &upstreamVaryCapture{}
	ctx := context.WithValue(r.Context(), upstreamVaryCaptureContextKey{}, capture)
	return r.WithContext(ctx), capture
}

// recordUpstreamVary must run before response middleware mutates Vary.
func recordUpstreamVary(resp *http.Response) {
	if resp == nil || resp.Request == nil {
		return
	}
	capture, ok := resp.Request.Context().Value(upstreamVaryCaptureContextKey{}).(*upstreamVaryCapture)
	if !ok {
		return
	}
	capture.values = append(capture.values[:0], resp.Header.Values("Vary")...)
	capture.observed = true
}

// cacheAdmissionHeaders substitutes the raw upstream Vary when the response
// hook observed it. If the hook did not run, the committed headers remain the
// conservative source of truth.
func cacheAdmissionHeaders(committed http.Header, capture *upstreamVaryCapture) http.Header {
	if capture == nil || !capture.observed {
		return committed
	}
	headers := committed.Clone()
	headers.Del("Vary")
	for _, value := range capture.values {
		headers.Add("Vary", value)
	}
	return headers
}

// pickCacheableHeaders returns a flat map of header-name → value containing
// just the headers we want to store in the cache. Names are canonicalized
// via http.CanonicalHeaderKey so case mismatches across runs don't fragment
// cache entries.
//
// Multi-valued headers are collapsed into a comma-separated single value
// (matching net/http's wire format). Cosmos node responses don't rely on
// multi-value semantics for any of the preserved headers, but the
// collapsing is documented in case operators add custom headers.
func pickCacheableHeaders(upstream http.Header, extra []string) map[string]string {
	if upstream == nil {
		return nil
	}
	allow := make(map[string]bool, len(alwaysPreservedHeaders)+len(extra))
	for _, h := range alwaysPreservedHeaders {
		allow[http.CanonicalHeaderKey(h)] = true
	}
	for _, h := range extra {
		c := http.CanonicalHeaderKey(h)
		if isHopByHop(c) {
			continue
		}
		allow[c] = true
	}

	out := make(map[string]string, len(allow))
	for name, vals := range upstream {
		c := http.CanonicalHeaderKey(name)
		if !allow[c] || len(vals) == 0 {
			continue
		}
		out[c] = strings.Join(vals, ", ")
	}
	return out
}

var sharedResponseHeaders = map[string]struct{}{
	"Cache-Control": {},
	"Content-Type":  {},
	"Retry-After":   {},
}

var rewrittenRepresentationHeaders = map[string]struct{}{
	"Content-Digest": {},
	"Content-Length": {},
	"Content-Md5":    {},
	"Digest":         {},
	"Etag":           {},
	"Last-Modified":  {},
	"Repr-Digest":    {},
}

func pickSharedResponseHeaders(upstream http.Header) http.Header {
	if upstream == nil {
		return nil
	}
	out := make(http.Header)
	for name, values := range upstream {
		canonical := http.CanonicalHeaderKey(name)
		if _, ok := sharedResponseHeaders[canonical]; !ok {
			continue
		}
		out[canonical] = append([]string(nil), values...)
	}
	return out
}

func stripRewrittenRepresentationHeaders(headers http.Header) {
	for name := range rewrittenRepresentationHeaders {
		headers.Del(name)
	}
}

func isHopByHop(canonicalName string) bool {
	for _, h := range hopByHopHeaders {
		if http.CanonicalHeaderKey(h) == canonicalName {
			return true
		}
	}
	return false
}
