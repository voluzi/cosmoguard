package cosmoguard

import (
	"net"
	"net/http"
	"net/url"
	"testing"

	"gotest.tools/assert"
)

func TestMatchTree_AllAnyNone(t *testing.T) {
	mk := func(method, path string, headers map[string]string) *http.Request {
		r := &http.Request{
			Method: method,
			URL:    &url.URL{Path: path},
			Header: http.Header{},
		}
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return r
	}

	tests := []struct {
		name    string
		tree    MatchTree
		req     *http.Request
		wantHit bool
	}{
		{
			name: "all: every child must match",
			tree: MatchTree{
				All: []MatchTree{
					{Path: "/block"},
					{Method: "GET"},
				},
			},
			req:     mk("GET", "/block", nil),
			wantHit: true,
		},
		{
			name: "all: one child fails → no match",
			tree: MatchTree{
				All: []MatchTree{
					{Path: "/block"},
					{Method: "POST"},
				},
			},
			req:     mk("GET", "/block", nil),
			wantHit: false,
		},
		{
			name: "any: at least one child matches",
			tree: MatchTree{
				Any: []MatchTree{
					{Method: "GET"},
					{Method: "HEAD"},
				},
			},
			req:     mk("HEAD", "/x", nil),
			wantHit: true,
		},
		{
			name: "any: none match → fail",
			tree: MatchTree{
				Any: []MatchTree{
					{Method: "GET"},
					{Method: "HEAD"},
				},
			},
			req:     mk("DELETE", "/x", nil),
			wantHit: false,
		},
		{
			name: "none: child matches → fail",
			tree: MatchTree{
				None: []MatchTree{
					{Header: map[string]string{"X-Debug": "present"}},
				},
			},
			req:     mk("GET", "/x", map[string]string{"X-Debug": "1"}),
			wantHit: false,
		},
		{
			name: "none: no child matches → success",
			tree: MatchTree{
				None: []MatchTree{
					{Header: map[string]string{"X-Debug": "present"}},
				},
			},
			req:     mk("GET", "/x", nil),
			wantHit: true,
		},
		{
			name: "header present",
			tree: MatchTree{
				Header: map[string]string{"Authorization": "present"},
			},
			req:     mk("GET", "/x", map[string]string{"Authorization": "Bearer abc"}),
			wantHit: true,
		},
		{
			name: "header absent (key not set)",
			tree: MatchTree{
				Header: map[string]string{"X-Debug": "absent"},
			},
			req:     mk("GET", "/x", nil),
			wantHit: true,
		},
		{
			name: "header glob match",
			tree: MatchTree{
				Header: map[string]string{"Authorization": "Bearer *"},
			},
			req:     mk("GET", "/x", map[string]string{"Authorization": "Bearer xyz"}),
			wantHit: true,
		},
		{
			name: "header exact mismatch",
			tree: MatchTree{
				Header: map[string]string{"X-Tenant": "prod"},
			},
			req:     mk("GET", "/x", map[string]string{"X-Tenant": "staging"}),
			wantHit: false,
		},
		{
			name: "combined all + none",
			tree: MatchTree{
				All: []MatchTree{
					{Path: "/block"},
					{Query: map[string]string{"height": "present"}},
				},
				None: []MatchTree{
					{Header: map[string]string{"X-Maintenance": "present"}},
				},
			},
			req: &http.Request{
				Method: "GET",
				URL:    &url.URL{Path: "/block", RawQuery: "height=42"},
				Header: http.Header{},
			},
			wantHit: true,
		},
		{
			name: "empty tree matches everything",
			tree: MatchTree{},
			req:  mk("GET", "/anything", nil),
			// Empty leaf with no atoms or children → c.isLeafAsserted=false →
			// all/any/none all skip → returns true. Matches the documented
			// semantics: an empty tree is the "anything" matcher used by
			// default-action rules.
			wantHit: true,
		},
		{
			name: "query glob",
			tree: MatchTree{
				Query: map[string]string{"height": "[0-9]*"},
			},
			req: &http.Request{
				Method: "GET",
				URL:    &url.URL{Path: "/block", RawQuery: "height=123"},
				Header: http.Header{},
			},
			wantHit: true,
		},
		{
			name: "query absent",
			tree: MatchTree{
				Query: map[string]string{"height": "absent"},
			},
			req: &http.Request{
				Method: "GET",
				URL:    &url.URL{Path: "/block"},
				Header: http.Header{},
			},
			wantHit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NilError(t, tt.tree.Compile())
			got := tt.tree.MatchRequest(tt.req, nil)
			assert.Equal(t, got, tt.wantHit)
		})
	}
}

func TestMatchTree_SourceIP_CIDR(t *testing.T) {
	tree := &MatchTree{
		SourceIP: "10.0.0.0/8",
	}
	assert.NilError(t, tree.Compile())

	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}

	assert.Equal(t, tree.MatchRequest(r, net.ParseIP("10.1.2.3")), true)
	assert.Equal(t, tree.MatchRequest(r, net.ParseIP("192.168.1.1")), false)
	// No source IP at all → can't satisfy a SourceIP constraint.
	assert.Equal(t, tree.MatchRequest(r, nil), false)
}

// TestMatchTree_SourceIP_Exact covers the single-IP branch (no slash).
func TestMatchTree_SourceIP_Exact(t *testing.T) {
	tree := &MatchTree{SourceIP: "10.0.0.5"}
	assert.NilError(t, tree.Compile())

	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
	assert.Equal(t, tree.MatchRequest(r, net.ParseIP("10.0.0.5")), true)
	assert.Equal(t, tree.MatchRequest(r, net.ParseIP("10.0.0.6")), false)
}

// TestMatchTree_NestedCombinators exercises multi-level recursion: an
// `any` whose children are themselves `all` blocks.
func TestMatchTree_NestedCombinators(t *testing.T) {
	tree := &MatchTree{
		Any: []MatchTree{
			{All: []MatchTree{
				{Path: "/block"},
				{Methods: []string{"GET"}},
			}},
			{All: []MatchTree{
				{Path: "/commit"},
				{Methods: []string{"POST"}},
			}},
		},
	}
	assert.NilError(t, tree.Compile())

	hitA := &http.Request{Method: "GET", URL: &url.URL{Path: "/block"}, Header: http.Header{}}
	hitB := &http.Request{Method: "POST", URL: &url.URL{Path: "/commit"}, Header: http.Header{}}
	miss := &http.Request{Method: "GET", URL: &url.URL{Path: "/commit"}, Header: http.Header{}} // wrong method for /commit

	assert.Equal(t, tree.MatchRequest(hitA, nil), true)
	assert.Equal(t, tree.MatchRequest(hitB, nil), true)
	assert.Equal(t, tree.MatchRequest(miss, nil), false)
}

// TestMatchTree_HeaderCaseInsensitive verifies that header name matching
// is case-insensitive both at config time and at request time.
func TestMatchTree_HeaderCaseInsensitive(t *testing.T) {
	tree := &MatchTree{
		Header: map[string]string{"x-debug": "present"}, // lowercase in config
	}
	assert.NilError(t, tree.Compile())

	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
	r.Header.Set("X-Debug", "1") // mixed-case header on the wire

	assert.Equal(t, tree.MatchRequest(r, nil), true)
}

// TestHttpRule_V3Compat_LiteralPresent pins the v3 backward-compat
// semantics: a v3 config with `query: {x: "present"}` keeps treating
// "present" as a literal exact-match value, NOT a presence keyword.
// Operators who want presence checks must use the v4 `match.query` shape.
func TestHttpRule_V3Compat_LiteralPresent(t *testing.T) {
	// v3 flat syntax — should preserve old behavior (literal exact match).
	v3 := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Paths:    []string{"/x"},
		Query:    map[string]string{"flag": "present"},
	}
	assert.NilError(t, v3.Compile())

	// Request where flag exists but value is something else → must NOT
	// match because v3 semantics required exact-value match.
	r1 := &http.Request{
		Method: "GET",
		URL:    &url.URL{Path: "/x", RawQuery: "flag=enabled"},
		Header: http.Header{},
	}
	assert.Equal(t, v3.Matches(r1), false,
		"v3 query value 'present' must NOT be promoted to presence keyword")

	// Request with the literal value "present" → must match.
	r2 := &http.Request{
		Method: "GET",
		URL:    &url.URL{Path: "/x", RawQuery: "flag=present"},
		Header: http.Header{},
	}
	assert.Equal(t, v3.Matches(r2), true)
}

// TestHttpRule_V3Compat_QueryFingerprint locks down the broader fingerprint
// stability claim for the query case (the original V3Desugaring test only
// covered paths+methods).
func TestHttpRule_V3Compat_QueryFingerprint(t *testing.T) {
	v3 := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Paths:    []string{"/x"},
		Query:    map[string]string{"height": "*"},
	}
	v4 := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Match: &MatchTree{
			Paths: []string{"/x"},
			Query: map[string]string{"height": "*"},
		},
	}
	assert.NilError(t, v3.Compile())
	assert.NilError(t, v4.Compile())
	assert.Equal(t, v3.Fingerprint, v4.Fingerprint,
		"v3 query field must produce the same fingerprint as the equivalent v4 match.query")
}

func TestMatchTree_Invalid(t *testing.T) {
	bad := &MatchTree{Paths: []string{"[unclosed"}}
	err := bad.Compile()
	assert.Assert(t, err != nil, "expected compile error for malformed path glob")

	badIP := &MatchTree{SourceIP: "not-an-ip"}
	err = badIP.Compile()
	assert.Assert(t, err != nil, "expected compile error for invalid IP")
}

// TestHttpRule_V3Desugaring proves that v3 flat-syntax fields land in the
// effective matcher and behave identically to the v4 explicit form.
func TestHttpRule_V3Desugaring(t *testing.T) {
	v3 := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Paths:    []string{"/block"},
		Methods:  []string{"GET"},
	}
	v4 := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Match: &MatchTree{
			Paths:   []string{"/block"},
			Methods: []string{"GET"},
		},
	}
	assert.NilError(t, v3.Compile())
	assert.NilError(t, v4.Compile())

	// Same fingerprint → same cache namespace whether the operator wrote
	// the v3 form or the v4 form.
	assert.Equal(t, v3.Fingerprint, v4.Fingerprint,
		"v3 flat syntax must produce the same fingerprint as the equivalent v4 match tree")

	// Both match the same request the same way.
	good := &http.Request{Method: "GET", URL: &url.URL{Path: "/block"}, Header: http.Header{}}
	bad := &http.Request{Method: "POST", URL: &url.URL{Path: "/block"}, Header: http.Header{}}

	assert.Equal(t, v3.Matches(good), true)
	assert.Equal(t, v4.Matches(good), true)
	assert.Equal(t, v3.Matches(bad), false)
	assert.Equal(t, v4.Matches(bad), false)
}

// TestHttpRule_MixedV3AndV4Merge verifies that a rule with BOTH v3 flat
// fields and a v4 Match tree combines them at the top level (implicit
// "all of: v4 tree atoms AND v3 atoms").
func TestHttpRule_MixedV3AndV4Merge(t *testing.T) {
	r := &HttpRule{
		Priority: 100,
		Action:   RuleActionAllow,
		Paths:    []string{"/block"}, // v3
		Match: &MatchTree{
			Methods: []string{"GET"}, // v4
		},
	}
	assert.NilError(t, r.Compile())

	good := &http.Request{Method: "GET", URL: &url.URL{Path: "/block"}, Header: http.Header{}}
	wrongPath := &http.Request{Method: "GET", URL: &url.URL{Path: "/other"}, Header: http.Header{}}
	wrongMethod := &http.Request{Method: "POST", URL: &url.URL{Path: "/block"}, Header: http.Header{}}

	assert.Equal(t, r.Matches(good), true)
	assert.Equal(t, r.Matches(wrongPath), false)
	assert.Equal(t, r.Matches(wrongMethod), false)
}
