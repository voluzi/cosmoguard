package cosmoguard

import (
	"net/http"
	"testing"
)

// TestCacheableByVary is the #14 regression: a response that Vary's on a
// request header other than Accept-Encoding (which we DO key on) must not be
// cached, or one client's content-negotiated response would be served to a
// client that sent different headers.
func TestCacheableByVary(t *testing.T) {
	cases := []struct {
		name   string
		vary   string
		policy cacheKeyVaryPolicy
		want   bool
	}{
		{"http absent", "", httpCacheKeyVary, true},
		{"http accept encoding", "Accept-Encoding", httpCacheKeyVary, true},
		{"http accept encoding case insensitive", "accept-encoding", httpCacheKeyVary, true},
		{"http repeated accept encoding", "Accept-Encoding, Accept-Encoding", httpCacheKeyVary, true},
		{"http accept language", "Accept-Language", httpCacheKeyVary, false},
		{"http authorization", "Authorization", httpCacheKeyVary, false},
		{"http mixed supported and unsupported", "Accept-Encoding, Accept-Language", httpCacheKeyVary, false},
		{"http wildcard", "*", httpCacheKeyVary, false},
		{"http origin", "Origin", httpCacheKeyVary, false},
		{"json rpc absent", "", jsonRPCCacheKeyVary, true},
		{"json rpc accept encoding", "Accept-Encoding", jsonRPCCacheKeyVary, false},
		{"json rpc origin", "Origin", jsonRPCCacheKeyVary, false},
		{"json rpc wildcard", "*", jsonRPCCacheKeyVary, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.vary != "" {
				h.Set("Vary", tc.vary)
			}
			if got := cacheableByVary(h, tc.policy); got != tc.want {
				t.Errorf("cacheableByVary(Vary: %q, policy=%d) = %v, want %v", tc.vary, tc.policy, got, tc.want)
			}
		})
	}
}

// TestCacheableByVaryMultiLine covers Vary spread across multiple header lines
// (an upstream + cosmoguard's addVary can produce this).
func TestCacheableByVaryMultiLine(t *testing.T) {
	h := http.Header{}
	h.Add("Vary", "Accept-Encoding")
	h.Add("Vary", "Origin")
	if cacheableByVary(h, httpCacheKeyVary) {
		t.Error("Origin must remain uncacheable when it appears on a second line")
	}
	bad := http.Header{}
	bad.Add("Vary", "Accept-Encoding")
	bad.Add("Vary", "Authorization")
	if cacheableByVary(bad, httpCacheKeyVary) {
		t.Error("Authorization must never be cacheable")
	}
}

func TestCacheAdmissionHeadersUsesRawUpstreamVary(t *testing.T) {
	committed := http.Header{"Vary": {"Origin"}}

	observedEmpty := &upstreamVaryCapture{observed: true}
	if got := cacheAdmissionHeaders(committed, observedEmpty); len(got.Values("Vary")) != 0 {
		t.Fatalf("synthetic Vary must be removed from admission headers, got %q", got.Values("Vary"))
	}
	if got := committed.Values("Vary"); len(got) != 1 || got[0] != "Origin" {
		t.Fatalf("committed client-facing headers were mutated: %q", got)
	}

	raw := &upstreamVaryCapture{observed: true, values: []string{"Accept-Encoding", "Origin"}}
	if got := cacheAdmissionHeaders(committed, raw).Values("Vary"); len(got) != 2 || got[0] != "Accept-Encoding" || got[1] != "Origin" {
		t.Fatalf("raw upstream Vary lines were not restored: %q", got)
	}

	unobserved := &upstreamVaryCapture{}
	if got := cacheAdmissionHeaders(committed, unobserved).Values("Vary"); len(got) != 1 || got[0] != "Origin" {
		t.Fatalf("unobserved capture must conservatively keep committed Vary: %q", got)
	}
}

// TestPickCacheableHeadersExcludesACAO is the safety linchpin: the CORS
// Allow-Origin header must never be stored, so a cached body can't leak one
// origin's ACAO to another (it's re-derived per hit by ApplyToResponse).
func TestPickCacheableHeadersExcludesACAO(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Vary", "Origin")
	h.Set("Access-Control-Allow-Origin", "https://app.example.com")
	h.Set("Access-Control-Allow-Credentials", "true")
	got := pickCacheableHeaders(h, nil)
	if _, ok := got["Access-Control-Allow-Origin"]; ok {
		t.Error("Access-Control-Allow-Origin must NOT be stored in the cache")
	}
	if _, ok := got["Access-Control-Allow-Credentials"]; ok {
		t.Error("Access-Control-Allow-Credentials must NOT be stored in the cache")
	}
	if got["Content-Type"] != "application/json" {
		t.Errorf("Content-Type must be preserved, got %q", got["Content-Type"])
	}
	if got["Vary"] != "Origin" {
		t.Errorf("Vary must be preserved, got %q", got["Vary"])
	}
}

// TestAcceptEncodingKey: clients with different acceptable-coding sets must
// get different keys; identical sets (any order) share.
func TestAcceptEncodingKey(t *testing.T) {
	if acceptEncodingKey("") != "identity;q=1" {
		t.Fatalf("empty Accept-Encoding should be identity;q=1, got %q", acceptEncodingKey(""))
	}
	// Order-independent.
	if acceptEncodingKey("gzip, br") != acceptEncodingKey("br, gzip") {
		t.Fatal("acceptable-coding set must be order-independent")
	}
	// A gzip-only client and a gzip+zstd client must NOT share an entry —
	// the upstream might return zstd, which the gzip-only client can't take.
	if acceptEncodingKey("gzip") == acceptEncodingKey("gzip, zstd") {
		t.Fatal("gzip vs gzip+zstd must produce different keys")
	}
	// gzip-capable and identity-only clients differ.
	if acceptEncodingKey("gzip") == acceptEncodingKey("identity") {
		t.Fatal("gzip and identity-only must differ")
	}
}
