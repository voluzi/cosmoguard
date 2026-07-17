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
		vary      string
		corsOwned bool
		want      bool
	}{
		{"", false, true},
		{"Accept-Encoding", false, true},
		{"accept-encoding", false, true},
		{"Accept-Encoding, Accept-Encoding", false, true},
		{"Accept-Language", false, false},
		{"Authorization", false, false},
		{"Accept-Encoding, Accept-Language", false, false},
		{"*", false, false},
		// Origin is cacheable only when cosmoguard owns CORS — ACAO is
		// re-derived per hit and never stored.
		{"Origin", false, false},
		{"Origin", true, true},
		{"origin", true, true},
		{"Accept-Encoding, Origin", true, true},
		// Real per-client variance stays uncacheable even under CORS ownership.
		{"Authorization", true, false},
		{"Cookie", true, false},
		{"Origin, Authorization", true, false},
		{"*", true, false},
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.vary != "" {
			h.Set("Vary", tc.vary)
		}
		if got := cacheableByVary(h, tc.corsOwned); got != tc.want {
			t.Errorf("cacheableByVary(Vary: %q, corsOwned=%v) = %v, want %v", tc.vary, tc.corsOwned, got, tc.want)
		}
	}
}

// TestCacheableByVaryMultiLine covers Vary spread across multiple header lines
// (an upstream + cosmoguard's addVary can produce this).
func TestCacheableByVaryMultiLine(t *testing.T) {
	h := http.Header{}
	h.Add("Vary", "Accept-Encoding")
	h.Add("Vary", "Origin")
	if !cacheableByVary(h, true) {
		t.Error("Accept-Encoding + Origin across two lines must be cacheable when CORS owned")
	}
	if cacheableByVary(h, false) {
		t.Error("Origin must NOT be cacheable when CORS is not owned")
	}
	bad := http.Header{}
	bad.Add("Vary", "Accept-Encoding")
	bad.Add("Vary", "Authorization")
	if cacheableByVary(bad, true) {
		t.Error("Authorization must never be cacheable, even under CORS ownership")
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
