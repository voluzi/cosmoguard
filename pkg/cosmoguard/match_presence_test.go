package cosmoguard

import (
	"net/http"
	"net/url"
	"testing"

	"gotest.tools/assert"
)

// TestMatchTree_PresentEmptyValue is the #21 regression: a present-but-empty
// query key (`?debug` / `?debug=`) or header (`X-Debug:`) must satisfy a
// `present` predicate and fail an `absent` one. Presence is key existence,
// not value non-emptiness.
func TestMatchTree_PresentEmptyValue(t *testing.T) {
	t.Run("query present matches empty value", func(t *testing.T) {
		tree := &MatchTree{Query: map[string]string{"debug": "present"}}
		assert.NilError(t, tree.Compile())
		for _, raw := range []string{"debug", "debug="} {
			r := &http.Request{Method: "GET", URL: &url.URL{Path: "/", RawQuery: raw}, Header: http.Header{}}
			assert.Equal(t, tree.MatchRequest(r, nil), true, "query %q should be present", raw)
		}
		// Truly absent → no match.
		r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
		assert.Equal(t, tree.MatchRequest(r, nil), false)
	})

	t.Run("query absent does not match empty value", func(t *testing.T) {
		tree := &MatchTree{Query: map[string]string{"debug": "absent"}}
		assert.NilError(t, tree.Compile())
		r := &http.Request{Method: "GET", URL: &url.URL{Path: "/", RawQuery: "debug="}, Header: http.Header{}}
		assert.Equal(t, tree.MatchRequest(r, nil), false, "empty-valued key is present, so absent must fail")
	})

	t.Run("header present matches empty value", func(t *testing.T) {
		tree := &MatchTree{Header: map[string]string{"x-debug": "present"}}
		assert.NilError(t, tree.Compile())
		r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
		r.Header["X-Debug"] = []string{""} // present but empty
		assert.Equal(t, tree.MatchRequest(r, nil), true)
	})

	t.Run("header absent does not match empty value", func(t *testing.T) {
		tree := &MatchTree{Header: map[string]string{"x-debug": "absent"}}
		assert.NilError(t, tree.Compile())
		r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
		r.Header["X-Debug"] = []string{""}
		assert.Equal(t, tree.MatchRequest(r, nil), false)
	})
}

// TestIsTruthy is the #18 regression: string acceptance uses a truthy
// allowlist, so a stringly-typed backend returning "False"/"no"/"off" for an
// invalid credential is NOT accepted.
func TestIsTruthy(t *testing.T) {
	truthy := []any{true, "true", "TRUE", "1", "yes", "on", "active", "valid", 1.0, []any{"x"}}
	for _, v := range truthy {
		if !isTruthy(v) {
			t.Errorf("isTruthy(%#v) = false, want true", v)
		}
	}
	falsy := []any{false, "false", "False", "FALSE", "no", "off", "inactive", "0", "", "null", nil, 0.0}
	for _, v := range falsy {
		if isTruthy(v) {
			t.Errorf("isTruthy(%#v) = true, want false", v)
		}
	}
}
