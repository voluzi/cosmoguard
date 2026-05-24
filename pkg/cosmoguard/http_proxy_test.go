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

	h1, err := p.getRequestHash(req1, 0)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0)
	assert.NilError(t, err)

	assert.Equal(t, h1, h2)
}

func TestHttpProxy_getRequestHash_DifferentQueryValues(t *testing.T) {
	p := &HttpProxy{}

	req1, err := http.NewRequest(http.MethodGet, "http://x/block?height=1", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)
	req2, err := http.NewRequest(http.MethodGet, "http://x/block?height=2", io.NopCloser(strings.NewReader("")))
	assert.NilError(t, err)

	h1, err := p.getRequestHash(req1, 0)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0)
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

	h1, err := p.getRequestHash(req, 1234)
	assert.NilError(t, err)
	// Body was consumed; reset for the second call.
	req.Body = io.NopCloser(strings.NewReader(""))
	h2, err := p.getRequestHash(req, 5678)
	assert.NilError(t, err)

	assert.Assert(t, h1 != h2, "different rule fingerprints must produce different hashes")
}

func TestHttpProxy_getRequestHash_BodyAffectsHash(t *testing.T) {
	p := &HttpProxy{}

	req1, err := http.NewRequest(http.MethodPost, "http://x/", io.NopCloser(strings.NewReader("payload-a")))
	assert.NilError(t, err)
	req2, err := http.NewRequest(http.MethodPost, "http://x/", io.NopCloser(strings.NewReader("payload-b")))
	assert.NilError(t, err)

	h1, err := p.getRequestHash(req1, 0)
	assert.NilError(t, err)
	h2, err := p.getRequestHash(req2, 0)
	assert.NilError(t, err)

	assert.Assert(t, h1 != h2)
}
