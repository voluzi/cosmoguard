package cosmoguard

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"gotest.tools/assert"
)

func TestHttpProxy_getRequestHash_NormalizesQueryOrder(t *testing.T) {
	p := &HttpProxy{}

	req1, err := http.NewRequest(http.MethodGet, "http://x/block?a=1&b=2", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)
	req2, err := http.NewRequest(http.MethodGet, "http://x/block?b=2&a=1", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)

	keyMetadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	h1, err := p.getRequestHash(req1, 0, keyMetadata)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0, keyMetadata)
	assert.NilError(t, err)

	assert.Equal(t, h1, h2)
}

func TestHttpProxy_getRequestHash_DifferentQueryValues(t *testing.T) {
	p := &HttpProxy{}

	req1, err := http.NewRequest(http.MethodGet, "http://x/block?height=1", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)
	req2, err := http.NewRequest(http.MethodGet, "http://x/block?height=2", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)

	keyMetadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	h1, err := p.getRequestHash(req1, 0, keyMetadata)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0, keyMetadata)
	assert.NilError(t, err)

	assert.Assert(t, h1 != h2)
}

// TestHttpProxy_getRequestHash_FingerprintNamespacesEntries verifies that
// two rules with different fingerprints produce different cache keys for
// the same request — closing the pre-B5 cross-rule poisoning gap.
func TestHttpProxy_getRequestHash_FingerprintNamespacesEntries(t *testing.T) {
	p := &HttpProxy{}

	req, err := http.NewRequest(http.MethodGet, "http://x/block", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)

	keyMetadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	h1, err := p.getRequestHash(req, 1234, keyMetadata)
	assert.NilError(t, err)
	// Body was consumed; reset for the second call.
	req.Body = io.NopCloser(strings.NewReader(""))
	h2, err := p.getRequestHash(req, 5678, keyMetadata)
	assert.NilError(t, err)

	assert.Assert(t, h1 != h2, "different rule fingerprints must produce different hashes")
}

func TestHttpProxy_getRequestHash_BodyAffectsHash(t *testing.T) {
	p := &HttpProxy{}

	req1, err := http.NewRequest(http.MethodPost, "http://x/", io.NopCloser(strings.NewReader("payload-a")))
	assert.NilError(t, err)
	req2, err := http.NewRequest(http.MethodPost, "http://x/", io.NopCloser(strings.NewReader("payload-b")))
	assert.NilError(t, err)

	keyMetadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	h1, err := p.getRequestHash(req1, 0, keyMetadata)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0, keyMetadata)
	assert.NilError(t, err)

	assert.Assert(t, h1 != h2)
}

func TestHttpProxy_getRequestHash_KeyMetadata(t *testing.T) {
	p := &HttpProxy{}
	hash := func(headers http.Header, body string, keyMetadata []string) string {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://x/block?a=1", strings.NewReader(body))
		assert.NilError(t, err)
		for name, values := range headers {
			for _, value := range values {
				req.Header.Add(name, value)
			}
		}
		got, err := p.getRequestHash(req, 42, keyMetadata)
		assert.NilError(t, err)
		return got
	}
	defaults := (&RuleCache{}).EffectiveHTTPKeyMetadata()

	different := []struct {
		name        string
		left, right http.Header
		leftBody    string
		rightBody   string
		keyMetadata []string
	}{
		{
			name: "cosmos height",
			left: http.Header{"X-Cosmos-Block-Height": {"100"}}, right: http.Header{"X-Cosmos-Block-Height": {"200"}},
		},
		{
			name: "gateway height",
			left: http.Header{"Grpc-Metadata-X-Cosmos-Block-Height": {"100"}}, right: http.Header{"Grpc-Metadata-X-Cosmos-Block-Height": {"200"}},
		},
		{
			name: "absent versus present empty",
			left: nil, right: http.Header{"X-Cosmos-Block-Height": {""}},
		},
		{
			name: "second value",
			left: http.Header{"X-Cosmos-Block-Height": {"100", "200"}}, right: http.Header{"X-Cosmos-Block-Height": {"100", "300"}},
		},
		{
			name: "value order",
			left: http.Header{"X-Cosmos-Block-Height": {"100", "200"}}, right: http.Header{"X-Cosmos-Block-Height": {"200", "100"}},
		},
		{
			name: "value boundaries",
			left: http.Header{"X-Cosmos-Block-Height": {"100", "200"}}, right: http.Header{"X-Cosmos-Block-Height": {"100,200"}},
		},
		{
			name:  "both aliases are independent",
			left:  http.Header{"X-Cosmos-Block-Height": {"100"}, "Grpc-Metadata-X-Cosmos-Block-Height": {"200"}},
			right: http.Header{"X-Cosmos-Block-Height": {"100"}, "Grpc-Metadata-X-Cosmos-Block-Height": {"300"}},
		},
		{
			name: "aliases are not equivalent",
			left: http.Header{"X-Cosmos-Block-Height": {"100"}}, right: http.Header{"Grpc-Metadata-X-Cosmos-Block-Height": {"100"}},
		},
		{
			name: "height text is not normalized numerically",
			left: http.Header{"X-Cosmos-Block-Height": {"100"}}, right: http.Header{"X-Cosmos-Block-Height": {"0100"}},
		},
		{
			name: "metadata remains framed from body",
			left: http.Header{"X-Cosmos-Block-Height": {"100\x00body=[\"x\"]"}}, leftBody: "tail",
			right: http.Header{"X-Cosmos-Block-Height": {"100"}}, rightBody: "body=[\"x\"]\x00tail",
		},
	}
	for _, tt := range different {
		t.Run(tt.name, func(t *testing.T) {
			keys := tt.keyMetadata
			if keys == nil {
				keys = defaults
			}
			assert.Assert(t, hash(tt.left, tt.leftBody, keys) != hash(tt.right, tt.rightBody, keys))
		})
	}

	equivalentKeys := (&RuleCache{KeyMetadata: []string{
		"GRPC-METADATA-X-COSMOS-BLOCK-HEIGHT",
		"x-cosmos-block-height",
		"X-Cosmos-Block-Height",
	}}).EffectiveHTTPKeyMetadata()
	headers := http.Header{
		"x-cosmos-block-height":               {"100", "200"},
		"grpc-metadata-x-cosmos-block-height": {"300"},
	}
	first := hash(headers, "body", defaults)
	second := hash(headers, "body", equivalentKeys)
	assert.Equal(t, first, second)
	assert.Equal(t, second, hash(headers, "body", equivalentKeys))
	assert.Equal(t,
		hash(http.Header{"x-cosmos-block-height": {"100"}}, "body", defaults),
		hash(http.Header{"X-COSMOS-BLOCK-HEIGHT": {"100"}}, "body", defaults),
	)
}
