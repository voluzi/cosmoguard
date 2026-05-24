package cosmoguard

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/gobwas/glob"
)

// compileOriginAllowlist turns a list of origin patterns into a function
// suitable for websocket.Upgrader.CheckOrigin. Each pattern may be:
//
//   - "*"                         — match any Origin
//   - "https://example.com"       — exact match (scheme + host)
//   - "https://*.example.com"     — glob match (single subdomain label)
//   - "*://example.com"           — any scheme, exact host
//
// The returned function permits the upgrade if ANY of the following holds:
//
//  1. The request has no Origin header. Non-browser clients (curl, native
//     code, server-to-server gRPC bridges) don't send one and are always
//     allowed.
//  2. The Origin's host matches the request's Host header (same-origin).
//  3. The Origin matches at least one allowlist entry.
//
// On any URL-parse error the upgrade is denied — a malformed Origin is the
// caller's problem, not ours. Comparisons are case-insensitive (host names
// are per RFC 3986).
//
// Globs use `/` and `.` as separators so `*` cannot cross hostname-label
// boundaries or path slashes. That stops the classic `https://*.example.com`
// bypass where an attacker hosts a file at `https://evil.com/x.example.com`.
//
// Returns an error if any pattern fails to compile, so callers can surface
// config mistakes at load time instead of permitting every connection.
func compileOriginAllowlist(patterns []string) (func(*http.Request) bool, error) {
	type entry struct {
		exact string
		glob  glob.Glob
	}

	wildcardAll := false
	entries := make([]entry, 0, len(patterns))
	for _, p := range patterns {
		if p == "*" {
			wildcardAll = true
			continue
		}
		pl := strings.ToLower(p)
		if strings.ContainsAny(pl, "*?[") {
			// `/` and `.` as separators: `*` stops at slash and at label
			// boundaries, so `https://*.example.com` matches subdomains
			// only, never `https://evil.com/x.example.com`.
			g, err := compileGlob(pl, '/', '.')
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry{glob: g})
		} else {
			entries = append(entries, entry{exact: pl})
		}
	}

	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil || u.Host == "" {
			return false
		}
		// Same-origin: Origin host equals request Host header.
		// Hosts are case-insensitive per RFC 3986.
		if strings.EqualFold(u.Host, r.Host) {
			return true
		}
		if wildcardAll {
			return true
		}
		originLower := strings.ToLower(origin)
		for _, e := range entries {
			if e.exact != "" && e.exact == originLower {
				return true
			}
			if e.glob != nil && e.glob.Match(originLower) {
				return true
			}
		}
		return false
	}, nil
}
