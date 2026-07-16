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

// TestNormalizeAcceptEncoding: gzip-capable and non-gzip clients land in
// different buckets so they don't share a cache entry.
func TestNormalizeAcceptEncoding(t *testing.T) {
	if normalizeAcceptEncoding("gzip, deflate, br") != "gzip" {
		t.Fatal("gzip should win when offered")
	}
	if normalizeAcceptEncoding("") != "identity" {
		t.Fatal("empty Accept-Encoding should be identity")
	}
	if normalizeAcceptEncoding("gzip") == normalizeAcceptEncoding("identity") {
		t.Fatal("gzip and identity must map to different buckets")
	}
}
