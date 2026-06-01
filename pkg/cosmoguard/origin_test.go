package cosmoguard

import (
	"net/http"
	"testing"

	"gotest.tools/assert"
)

func TestCompileOriginAllowlist(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		origin   string
		host     string
		want     bool
	}{
		{
			name:     "no origin header is always allowed (non-browser)",
			patterns: []string{},
			origin:   "",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "same-origin always allowed",
			patterns: []string{},
			origin:   "http://cosmoguard.local",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "same-origin with port matched",
			patterns: []string{},
			origin:   "http://cosmoguard.local:16657",
			host:     "cosmoguard.local:16657",
			want:     true,
		},
		{
			name:     "cross-origin denied when allowlist empty",
			patterns: []string{},
			origin:   "https://evil.example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "wildcard allows any",
			patterns: []string{"*"},
			origin:   "https://anything.example.com",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "exact match",
			patterns: []string{"https://app.example.com"},
			origin:   "https://app.example.com",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "exact match miss",
			patterns: []string{"https://app.example.com"},
			origin:   "https://other.example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "glob subdomain",
			patterns: []string{"https://*.example.com"},
			origin:   "https://preview-42.example.com",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "glob subdomain does NOT match parent",
			patterns: []string{"https://*.example.com"},
			origin:   "https://example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "glob denies different domain",
			patterns: []string{"https://*.example.com"},
			origin:   "https://evil.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "multiple patterns, second matches",
			patterns: []string{"https://app.example.com", "https://*.preview.example.com"},
			origin:   "https://feature-x.preview.example.com",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "scheme-different denied",
			patterns: []string{"https://app.example.com"},
			origin:   "http://app.example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "malformed origin denied",
			patterns: []string{"*"},
			origin:   "not a url",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			// "null" Origin (sandboxed iframes, file://, data: URIs) must
			// never match. url.Parse("null") returns Host="" → denied.
			name:     "null origin denied even with wildcard",
			patterns: []string{"*"},
			origin:   "null",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			name:     "ipv6 same-origin",
			patterns: []string{},
			origin:   "https://[::1]:8443",
			host:     "[::1]:8443",
			want:     true,
		},
		{
			// Hosts are case-insensitive per RFC 3986: an UPPERCASE Origin
			// must match a lowercase allowlist entry.
			name:     "mixed-case origin matches lowercase pattern",
			patterns: []string{"https://app.example.com"},
			origin:   "https://APP.EXAMPLE.COM",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			// Critical security test: gobwas/glob's default-greedy `*` would
			// let an attacker bypass an "*.example.com" allowlist by hosting
			// a path that contains "example.com". We pin `/` and `.` as
			// separators in compileOriginAllowlist to block this.
			name:     "path-in-host bypass denied",
			patterns: []string{"https://*.example.com"},
			origin:   "https://evil.com/x.example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			// Scheme-less Origin (just "example.com") parses with empty
			// Host → denied. Browsers don't send this shape, but pinned.
			name:     "scheme-less origin denied",
			patterns: []string{"*"},
			origin:   "example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
		{
			// Bracket glob syntax pinned: gobwas supports it. App
			// generations often expose preview deploys at numbered
			// hostnames; this lets operators allowlist them tightly.
			name:     "bracket glob matches numbered host",
			patterns: []string{"https://app-[0-9].example.com"},
			origin:   "https://app-3.example.com",
			host:     "cosmoguard.local",
			want:     true,
		},
		{
			name:     "bracket glob rejects out-of-range",
			patterns: []string{"https://app-[0-9].example.com"},
			origin:   "https://app-x.example.com",
			host:     "cosmoguard.local",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check, err := compileOriginAllowlist(tt.patterns)
			assert.NilError(t, err)

			req, _ := http.NewRequest(http.MethodGet, "http://"+tt.host+"/ws", nil)
			req.Host = tt.host
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			got := check(req)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestCompileOriginAllowlist_RejectsBadGlob(t *testing.T) {
	_, err := compileOriginAllowlist([]string{"[unclosed"})
	assert.Assert(t, err != nil, "expected compile error for malformed glob pattern")
}
