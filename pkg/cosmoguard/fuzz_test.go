package cosmoguard

import "testing"

// FuzzEnvInterpolate runs random byte strings through the env-var
// interpolator. The contract: never panic, always return either the
// substituted string (no error) or a descriptive error. Any panic the
// fuzzer surfaces is a real bug we want to fix.
func FuzzEnvInterpolate(f *testing.F) {
	seeds := []string{
		"",
		"plain",
		"${FOO}",
		"${FOO:-default}",
		"${FOO:?missing message}",
		"${}",
		"${FOO",
		"$",
		"${{nested}}",
		"a ${FOO} b ${BAR:-x} c",
		"https://${HOST:-example.com}:${PORT:-443}/path",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = EnvInterpolate(s)
	})
}

// FuzzParseJsonRpcMessage feeds arbitrary bytes to the JSON-RPC parser.
// Contract: never panic. A real malformed message must surface as a
// non-nil error from ParseJsonRpcMessage, not a runtime panic on a nil
// dereference / index out of range / type assertion.
func FuzzParseJsonRpcMessage(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"status","params":{}}`),
		[]byte(`[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]`),
		[]byte(`{"jsonrpc":"2.0","id":null,"method":"x"}`),
		[]byte(`{"jsonrpc":"2.0","id":"abc","method":"x"}`),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte(`null`),
		[]byte(``),
		[]byte(`{"id":}`),
		[]byte(`{"params":[1,2,{"nested":[{"x":[null]}]}]}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ParseJsonRpcMessage(data)
	})
}

// FuzzHttpRuleCompile asserts that Compile never panics on arbitrary
// path/method/query input — even with adversarial glob syntax. Failure
// mode is a non-nil error from Compile; a panic is a bug.
func FuzzHttpRuleCompile(f *testing.F) {
	seeds := []struct {
		path   string
		method string
		query  string
	}{
		{"/a/b/c", "GET", "height"},
		{"/cosmos/*", "POST", "*"},
		{"/x?", "DELETE", "[a-z]"},
		{"", "", ""},
		{"[unclosed", "GET", "?"},
		{"**", "GET", "**"},
		{"/[a-]", "GET", "[a-]"},
	}
	for _, s := range seeds {
		f.Add(s.path, s.method, s.query)
	}

	f.Fuzz(func(t *testing.T, path, method, query string) {
		r := &HttpRule{
			Priority: 1000,
			Action:   RuleActionAllow,
			Paths:    []string{path},
			Methods:  []string{method},
			Query:    map[string]string{"k": query},
		}
		_ = r.Compile()
	})
}

// FuzzCompileOriginAllowlist drives the WS origin matcher with arbitrary
// patterns. Same contract: error or success, never panic.
func FuzzCompileOriginAllowlist(f *testing.F) {
	seeds := []string{
		"*",
		"https://example.com",
		"https://*.example.com",
		"*://*.example.com",
		"https://app-[0-9].example.com",
		"",
		"[unclosed",
		"null",
		"data:text/plain,hello",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, pattern string) {
		_, _ = compileOriginAllowlist([]string{pattern})
	})
}
