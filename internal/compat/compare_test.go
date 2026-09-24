package compat

import (
	"errors"
	"strings"
	"testing"

	"gotest.tools/assert"
)

func ok(body string) Response { return Response{Status: 200, Body: []byte(body)} }

func TestClassify(t *testing.T) {
	cases := []struct {
		name     string
		direct   Response
		proxies  []Response
		volatile bool
		want     Class
		detail   string
	}{
		{"identical", ok(`{"a":1}`), []Response{ok(`{"a":1}`), ok(`{"a":1}`)}, false, Identical, ""},
		{"error answers compared too", Response{Status: 404, Body: []byte("nope")}, []Response{{Status: 404, Body: []byte("nope")}}, false, Identical, ""},
		{"node down", Response{Err: errors.New("timeout")}, []Response{ok(`{}`)}, false, Failed, "node: timeout"},
		{"cosmoguard down", ok(`{}`), []Response{{Err: errors.New("refused")}}, false, Differs, "cosmoguard: refused"},
		{"node gateway error", Response{Status: 504, Body: []byte("<html>")}, []Response{ok(`{}`)}, false, Failed, "node: gateway status 504"},
		{"cosmoguard gateway error", ok(`{}`), []Response{{Status: 504}}, false, Differs, "status node=200 cosmoguard=504"},
		{"denied", ok(`{}`), []Response{{Status: 403, Denied: true}}, false, Denied, "refused with 403"},
		{"node rate-limited", Response{Status: 429, Throttled: true}, []Response{ok(`{}`)}, false, Failed, "rate-limited"},
		{"content type", Response{Status: 200, Body: []byte("{}"), ContentType: "application/json"}, []Response{{Status: 200, Body: []byte("{}"), ContentType: "text/plain"}}, false, Differs, "Content-Type"},
		{"both deny", Response{Status: 403, Denied: true}, []Response{{Status: 403, Denied: true}}, false, Identical, ""},
		{"status", ok(`{}`), []Response{{Status: 502}}, false, Differs, "status node=200 cosmoguard=502"},
		{"cached answer differs", ok(`{"a":1}`), []Response{ok(`{"a":1}`), ok(`{"a":2}`)}, false, Differs, "call 2: at $.a"},
		{"volatile same shape", ok(`{"h":"5"}`), []Response{ok(`{"h":"9"}`)}, true, Identical, ""},
		{"volatile new key", ok(`{"h":"5"}`), []Response{ok(`{"h":"5","x":1}`)}, true, Differs, "keys on one side only: x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, detail := Classify(tc.direct, tc.proxies, tc.volatile)
			assert.Equal(t, got, tc.want, detail)
			assert.Assert(t, strings.Contains(detail, tc.detail), "detail %q lacks %q", detail, tc.detail)
		})
	}
}

func TestSameHeight(t *testing.T) {
	assert.Assert(t, SameHeight(Response{Height: 5}, Response{}, Response{Height: 5}))
	assert.Assert(t, !SameHeight(Response{Height: 5}, Response{Height: 6}))
	assert.Assert(t, SameHeight(Response{}, Response{}))
}

func TestBodyDiff(t *testing.T) {
	assert.Equal(t, BodyDiff([]byte(`{"a":[1,2]}`), []byte(`{"a":[1,3]}`)), "at $.a[1]: node=2 cosmoguard=3")
	assert.Equal(t, BodyDiff([]byte(`{"a":1}`), []byte(`{"a":1,"b":2}`)), "at $.b: only cosmoguard has it")
	assert.Equal(t, BodyDiff([]byte(`{"a":1,"b":2}`), []byte(`{"b":2,"a":1}`)), "same JSON value, different bytes (formatting or key order)")
	// Large integers must not be rounded into equality.
	assert.Assert(t, strings.HasPrefix(BodyDiff([]byte(`{"n":12345678901234567890}`), []byte(`{"n":12345678901234567891}`)), "at $.n"))
	assert.Assert(t, strings.HasPrefix(BodyDiff([]byte("abcX"), []byte("abcY")), "bodies differ at byte 3"))
}

func TestShapeDiff(t *testing.T) {
	assert.Equal(t, ShapeDiff([]byte(`{"a":[{"x":1}]}`), []byte(`{"a":[{"x":2},{"x":3}]}`)), "")
	assert.Equal(t, ShapeDiff([]byte(`{"a":[]}`), []byte(`{"a":[{"x":2}]}`)), "")
	assert.Equal(t, ShapeDiff([]byte(`{"a":"1"}`), []byte(`{"a":1}`)), "at $.a: node has string, cosmoguard number")
	assert.Equal(t, ShapeDiff([]byte(`{"a":[{"x":1}]}`), []byte(`{"a":[{"y":1}]}`)), "at $.a[0]: keys on one side only: x, y")
	assert.Assert(t, ShapeDiff([]byte("x"), []byte("y")) != "")
}
