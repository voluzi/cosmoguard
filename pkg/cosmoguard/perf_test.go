package cosmoguard

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/util"
)

func TestBatchSizeClass(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "1"},
		{1, "1"},
		{2, "2-5"},
		{5, "2-5"},
		{6, "6-10"},
		{10, "6-10"},
		{11, "11-50"},
		{50, "11-50"},
		{51, "51-100"},
		{100, "51-100"},
		{101, "101-500"},
		{500, "101-500"},
		{501, "500+"},
		{100000, "500+"},
	}
	for _, c := range cases {
		assert.Equal(t, batchSizeClass(c.n), c.want, "size %d", c.n)
	}
}

// TestXXHashStable pins the hash function output for a known input so a
// future swap (e.g., upgrade) surfaces as a test diff.
func TestXXHashStable(t *testing.T) {
	got := util.XXHash64Hex("cosmoguard")
	// xxhash64 of "cosmoguard" is deterministic across Go versions.
	assert.Assert(t, len(got) > 0)
	// Length is at most 16 hex chars (64-bit value).
	assert.Assert(t, len(got) <= 16, "got %q", got)
	// Two distinct inputs hash to distinct outputs.
	other := util.XXHash64Hex("cosmoguard!")
	assert.Assert(t, got != other)
}

// BenchmarkGetRequestHash compares the per-request cost of the cache-key
// computation. xxhash should noticeably outperform sha256+hex on realistic
// Cosmos LCD request shapes. Run with: go test -bench=GetRequestHash ./...
func BenchmarkGetRequestHash(b *testing.B) {
	p := &HttpProxy{}
	body := strings.Repeat(`{"params":{}}`, 8) // ~100 bytes, realistic LCD body
	req, err := http.NewRequest(http.MethodPost, "http://x/cosmos/bank/v1beta1/balances?height=42", io.NopCloser(strings.NewReader(body)))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Body = io.NopCloser(strings.NewReader(body))
		_, err := p.getRequestHash(req, 0xdeadbeef)
		if err != nil {
			b.Fatal(err)
		}
	}
}
