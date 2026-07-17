package cosmoguard

import (
	"net/http"
	"strings"
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

// cacheableByVary reports whether a response is safe to cache given its
// Vary header. cosmoguard keys the cache on Accept-Encoding (see
// getRequestHash) but on no other request header, so a response that Vary's
// on anything else — or on "*" — cannot be safely reused across clients and
// must not be cached. Absent/empty Vary is cacheable.
//
// Origin is a special case: when corsOwned is true (cosmoguard's CORS layer
// is enabled) the Origin dimension belongs to cosmoguard, not the cached
// content. Access-Control-Allow-Origin is never stored (it's not in
// alwaysPreservedHeaders) and is re-derived per request by
// CORSConfig.ApplyToResponse on every cache hit, so a Vary: Origin response
// — whose body is identical across origins — is safe to cache. With CORS
// off we stay conservative and treat Origin like any other varying header.
func cacheableByVary(upstream http.Header, corsOwned bool) bool {
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
			// Accept-Encoding is folded into the cache key, so it's safe.
			if f == "accept-encoding" {
				continue
			}
			// Origin is safe only when cosmoguard owns the CORS surface.
			if f == "origin" && corsOwned {
				continue
			}
			// Any other varying header (Authorization, Cookie,
			// Accept-Language, …) reflects real per-client content
			// variance the cache can't key on — refuse.
			return false
		}
	}
	return true
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

func isHopByHop(canonicalName string) bool {
	for _, h := range hopByHopHeaders {
		if http.CanonicalHeaderKey(h) == canonicalName {
			return true
		}
	}
	return false
}
