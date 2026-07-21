package cosmoguard

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestHttpRule_Match(t *testing.T) {
	table := []struct {
		Rule        HttpRule
		ExpectMatch bool
		Request     *http.Request
	}{
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/path/to/a", "/path/to/b"},
				Methods:  []string{"GET", "POST"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/path/to/a",
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/path/to/a", "path/to/b"},
				Methods:  []string{"POST"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/path/to/a",
				},
			},
			ExpectMatch: false,
		},
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/path/to/*"},
				Methods:  []string{"GET"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/path/to/a",
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/path/to/*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/path/to/a",
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/path/to/*/test"},
				Methods:  []string{"GET"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/path/to/a/test",
				},
			},
			ExpectMatch: true,
		},
		// Query: rule requires height to be present and any value.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/block"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"height": "*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/block",
					RawQuery: "height=12345",
				},
			},
			ExpectMatch: true,
		},
		// Query: required key absent -> no match.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/block"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"height": "*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/block",
				},
			},
			ExpectMatch: false,
		},
		// Query: glob value matches a specific shape.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/block"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"height": "1234*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/block",
					RawQuery: "height=12345",
				},
			},
			ExpectMatch: true,
		},
		// Query: glob value does not match.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/block"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"height": "1234*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/block",
					RawQuery: "height=99999",
				},
			},
			ExpectMatch: false,
		},
		// Query: extra request keys are ignored.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/abci_query"},
				Methods:  []string{"GET"},
				Query:    map[string]string{"path": "/cosmos.bank*"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/abci_query",
					RawQuery: "path=/cosmos.bank.v1beta1.Query/AllBalances&data=0xff&height=1",
				},
			},
			ExpectMatch: true,
		},
		// Query: empty Query map preserves path-only matching.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/block"},
				Methods:  []string{"GET"},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/block",
					RawQuery: "height=12345",
				},
			},
			ExpectMatch: true,
		},
		// Query: multiple required keys, all must match.
		{
			Rule: HttpRule{
				Priority: 1000,
				Action:   RuleActionAllow,
				Paths:    []string{"/abci_query"},
				Methods:  []string{"GET"},
				Query: map[string]string{
					"path":   "/cosmos.bank*",
					"height": "*",
				},
			},
			Request: &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path:     "/abci_query",
					RawQuery: "path=/cosmos.bank.v1beta1.Query/AllBalances",
				},
			},
			ExpectMatch: false,
		},
	}

	for _, test := range table {
		assert.NilError(t, test.Rule.Compile())
		assert.Equal(t, test.Rule.Matches(test.Request), test.ExpectMatch)
	}
}

// TestHttpRule_Fingerprint pins down the contract for the per-rule cache
// namespace introduced in Phase B5: (1) Compile populates Fingerprint to a
// non-zero value, (2) two rules with the same matching+cache shape produce
// the same Fingerprint, (3) rules that differ in cacheError (or any other
// fingerprint input) get different Fingerprints. Phase B5's cross-rule
// cache poisoning fix depends on this contract.
func TestHttpRule_Fingerprint(t *testing.T) {
	mkRule := func(cacheErr bool) *HttpRule {
		return &HttpRule{
			Priority: 100,
			Action:   RuleActionAllow,
			Paths:    []string{"/x"},
			Methods:  []string{"GET"},
			Cache: &RuleCache{
				CacheError: cacheErr,
			},
		}
	}

	a := mkRule(false)
	assert.NilError(t, a.Compile())
	assert.Assert(t, a.Fingerprint != 0, "Compile must populate Fingerprint")

	// Same shape → same fingerprint.
	a2 := mkRule(false)
	assert.NilError(t, a2.Compile())
	assert.Equal(t, a.Fingerprint, a2.Fingerprint,
		"rules with identical shape must share a fingerprint")

	// Different cacheError → different fingerprint (the poisoning-prevention case).
	b := mkRule(true)
	assert.NilError(t, b.Compile())
	assert.Assert(t, a.Fingerprint != b.Fingerprint,
		"rules differing only in cacheError must have different fingerprints")
}

func TestRuleCacheDisableStaleWhileRevalidateValidationAndFingerprint(t *testing.T) {
	mkRule := func(cache *RuleCache) *HttpRule {
		return &HttpRule{
			Priority: 100,
			Action:   RuleActionAllow,
			Paths:    []string{"/x"},
			Methods:  []string{"GET"},
			Cache:    cache,
		}
	}

	inherited := mkRule(&RuleCache{})
	disabled := mkRule(&RuleCache{DisableStaleWhileRevalidate: true})
	assert.NilError(t, inherited.Compile())
	assert.NilError(t, disabled.Compile())
	assert.Assert(t, inherited.Fingerprint != disabled.Fingerprint)

	conflicting := mkRule(&RuleCache{
		StaleWhileRevalidate:        time.Minute,
		DisableStaleWhileRevalidate: true,
	})
	assert.ErrorContains(t, conflicting.Compile(), "cannot set both")
}

func TestJsonRpcRule_Match(t *testing.T) {
	table := []struct {
		Rule        JsonRpcRule
		ExpectMatch bool
		Request     *JsonRpcMsg
	}{
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "status",
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "status",
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"status"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
			},
			ExpectMatch: false,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"abci_*"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params:  make(map[string]interface{}),
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"abci_*"},
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/*",
				},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/AllBalances",
					"test": 1,
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"abci_*"},
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/*",
					"test": 1,
				},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/AllBalances",
					"test": 1,
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"abci_*"},
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/*",
					"test": 1,
				},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/AllBalances",
					"test": 2,
				},
			},
			ExpectMatch: false,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{},
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/*",
				},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/AllBalances",
				},
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"abci_*"},
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/*",
				},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "abci_query",
				Params: map[string]interface{}{
					"test": 1,
				},
			},
			ExpectMatch: false,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"eth_getBlockByNumber"},
				Params:  []interface{}{"latest"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "eth_getBlockByNumber",
				Params:  []interface{}{"latest", false},
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"eth_getBlockByNumber"},
				Params:  []interface{}{"latest"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "eth_getBlockByNumber",
				Params:  []interface{}{"latest", true},
			},
			ExpectMatch: true,
		},
		{
			Rule: JsonRpcRule{
				Action:  RuleActionAllow,
				Methods: []string{"eth_getBlockByNumber"},
				Params:  []interface{}{"latest"},
			},
			Request: &JsonRpcMsg{
				ID:      1234,
				Version: "2.0",
				Method:  "eth_getBlockByNumber",
				Params:  []interface{}{"0x49376"},
			},
			ExpectMatch: false,
		},
	}

	for _, test := range table {
		assert.NilError(t, test.Rule.Compile())
		assert.Equal(t, test.Rule.Match(test.Request), test.ExpectMatch)
	}
}
