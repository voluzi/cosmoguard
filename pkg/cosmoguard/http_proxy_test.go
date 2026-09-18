package cosmoguard

import (
	"io"
	"net/http"
	"net/http/httptest"
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

func TestHttpProxy_getRequestHash_TargetIdentity(t *testing.T) {
	p := &HttpProxy{}
	defaultMetadata := (&RuleCache{}).EffectiveHTTPKeyMetadata()
	request := func(rawURL string) *http.Request {
		t.Helper()
		return httptest.NewRequest(http.MethodGet, rawURL, nil)
	}
	hash := func(req *http.Request, keyMetadata []string) string {
		t.Helper()
		got, err := p.getRequestHash(req, 42, keyMetadata)
		assert.NilError(t, err)
		return got
	}

	tests := []struct {
		name        string
		left        func() *http.Request
		right       func() *http.Request
		keyMetadata []string
		wantEqual   bool
	}{
		{
			name:  "authority with default metadata",
			left:  func() *http.Request { return request("http://alpha.example/status") },
			right: func() *http.Request { return request("http://beta.example/status") },
		},
		{
			name:        "authority with explicit empty metadata",
			left:        func() *http.Request { return request("http://alpha.example/status") },
			right:       func() *http.Request { return request("http://beta.example/status") },
			keyMetadata: []string{},
		},
		{
			name:        "authority with custom metadata",
			left:        func() *http.Request { return request("http://alpha.example/status") },
			right:       func() *http.Request { return request("http://beta.example/status") },
			keyMetadata: []string{"X-Response-Version"},
		},
		{
			name:  "authority port",
			left:  func() *http.Request { return request("http://alpha.example:80/status") },
			right: func() *http.Request { return request("http://alpha.example:8080/status") },
		},
		{
			name: "header map host is not request authority",
			left: func() *http.Request {
				req := request("http://alpha.example/status")
				req.Header["Host"] = []string{"header-a.example"}
				return req
			},
			right: func() *http.Request {
				req := request("http://alpha.example/status")
				req.Header["Host"] = []string{"header-b.example"}
				return req
			},
			wantEqual: true,
		},
		{
			name:  "escaped slash",
			left:  func() *http.Request { return request("http://alpha.example/a%2Fb") },
			right: func() *http.Request { return request("http://alpha.example/a/b") },
		},
		{
			name:  "escape spelling",
			left:  func() *http.Request { return request("http://alpha.example/a%2Fb") },
			right: func() *http.Request { return request("http://alpha.example/a%2fb") },
		},
		{
			name:      "query key order",
			left:      func() *http.Request { return request("http://alpha.example/a%2Fb?b=2&a=1") },
			right:     func() *http.Request { return request("http://alpha.example/a%2Fb?a=1&b=2") },
			wantEqual: true,
		},
		{
			name:  "duplicate query value order",
			left:  func() *http.Request { return request("http://alpha.example/status?x=1&x=2") },
			right: func() *http.Request { return request("http://alpha.example/status?x=2&x=1") },
		},
		{
			name: "invalid raw path falls back to path encoding",
			left: func() *http.Request {
				req := request("http://alpha.example/a/b")
				req.URL.RawPath = "/invalid%zz"
				return req
			},
			right:     func() *http.Request { return request("http://alpha.example/a/b") },
			wantEqual: true,
		},
		{
			name: "opaque target",
			left: func() *http.Request {
				req := request("http://alpha.example/same")
				req.URL.Opaque = "/one"
				return req
			},
			right: func() *http.Request {
				req := request("http://alpha.example/same")
				req.URL.Opaque = "/two"
				return req
			},
		},
		{
			name: "opaque and hierarchical targets",
			left: func() *http.Request {
				req := request("http://alpha.example/a")
				req.URL.Opaque = "/a"
				return req
			},
			right: func() *http.Request { return request("http://alpha.example/a") },
		},
		{
			name: "force query",
			left: func() *http.Request {
				req := request("http://alpha.example/status")
				req.URL.ForceQuery = true
				return req
			},
			right: func() *http.Request { return request("http://alpha.example/status") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyMetadata := tt.keyMetadata
			if keyMetadata == nil {
				keyMetadata = defaultMetadata
			}
			left := hash(tt.left(), keyMetadata)
			right := hash(tt.right(), keyMetadata)
			if tt.wantEqual {
				assert.Equal(t, left, right)
				return
			}
			assert.Assert(t, left != right)
		})
	}
}

func TestHttpProxy_getRequestHash_DoesNotMutateURL(t *testing.T) {
	p := &HttpProxy{}
	req := httptest.NewRequest(http.MethodGet, "http://alpha.example/a%2Fb?b=2&a=1", nil)
	req.URL.ForceQuery = true
	before := *req.URL

	_, err := p.getRequestHash(req, 42, []string{})

	assert.NilError(t, err)
	assert.DeepEqual(t, *req.URL, before)
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

func TestHttpProxy_getRequestHash_SynthesizedForwardingMetadata(t *testing.T) {
	p := &HttpProxy{}
	hash := func(req *http.Request, key string) string {
		t.Helper()
		got, err := p.getRequestHash(req, 42, (&RuleCache{KeyMetadata: []string{key}}).EffectiveHTTPKeyMetadata())
		assert.NilError(t, err)
		return got
	}
	request := func(rawURL, forwardedHost, forwardedProto string) *http.Request {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, rawURL, nil)
		if forwardedHost != "" {
			req.Header.Set("X-Forwarded-Host", forwardedHost)
		}
		if forwardedProto != "" {
			req.Header.Set("X-Forwarded-Proto", forwardedProto)
		}
		return req
	}

	t.Run("forwarded host uses request authority", func(t *testing.T) {
		assert.Assert(t,
			hash(request("http://alpha.example/status", "spoofed.example", ""), "X-Forwarded-Host") !=
				hash(request("http://beta.example/status", "spoofed.example", ""), "X-Forwarded-Host"),
		)
	})
	t.Run("forwarded host ignores spoofed value", func(t *testing.T) {
		assert.Equal(t,
			hash(request("http://alpha.example/status", "spoofed-a.example", ""), "X-Forwarded-Host"),
			hash(request("http://alpha.example/status", "spoofed-b.example", ""), "X-Forwarded-Host"),
		)
	})
	t.Run("forwarded host retains inbound value without authority", func(t *testing.T) {
		left := request("http://alpha.example/status", "forwarded-a.example", "")
		left.Host = ""
		right := request("http://alpha.example/status", "forwarded-b.example", "")
		right.Host = ""
		assert.Assert(t,
			hash(left, "X-Forwarded-Host") != hash(right, "X-Forwarded-Host"),
		)
	})
	t.Run("forwarded proto uses transport", func(t *testing.T) {
		assert.Assert(t,
			hash(request("http://alpha.example/status", "", "spoofed"), "X-Forwarded-Proto") !=
				hash(request("https://alpha.example/status", "", "spoofed"), "X-Forwarded-Proto"),
		)
	})
	t.Run("forwarded proto ignores spoofed value", func(t *testing.T) {
		assert.Equal(t,
			hash(request("http://alpha.example/status", "", "spoofed-a"), "X-Forwarded-Proto"),
			hash(request("http://alpha.example/status", "", "spoofed-b"), "X-Forwarded-Proto"),
		)
	})
}
