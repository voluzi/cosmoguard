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
		vary string
		want bool
	}{
		{"", true},
		{"Accept-Encoding", true},
		{"accept-encoding", true},
		{"Accept-Encoding, Accept-Encoding", true},
		{"Accept-Language", false},
		{"Authorization", false},
		{"Accept-Encoding, Accept-Language", false},
		{"*", false},
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.vary != "" {
			h.Set("Vary", tc.vary)
		}
		if got := cacheableByVary(h); got != tc.want {
			t.Errorf("cacheableByVary(Vary: %q) = %v, want %v", tc.vary, got, tc.want)
		}
	}
}

// TestAcceptEncodingKey: clients with different acceptable-coding sets must
// get different keys; identical sets (any order) share.
func TestAcceptEncodingKey(t *testing.T) {
	if acceptEncodingKey("") != "identity" {
		t.Fatalf("empty Accept-Encoding should be identity, got %q", acceptEncodingKey(""))
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
