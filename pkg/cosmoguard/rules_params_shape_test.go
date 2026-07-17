package cosmoguard

import "testing"

// TestJsonRpcRule_ParamsShapeMismatchDoesNotBypass is the #5 regression: a
// rule that filters by one params shape must NOT match a request that sends
// params in the other shape. Previously the mismatched branch returned true
// unconditionally, so a params-scoped allow/deny rule was bypassed by simply
// flipping the JSON shape (array ⇄ object).
func TestJsonRpcRule_ParamsShapeMismatchDoesNotBypass(t *testing.T) {
	// Rule filters eth_call by a positional (slice) param value.
	sliceRule := &JsonRpcRule{
		Methods: []string{"eth_call"},
		Params:  []interface{}{"0xdeadbeef"},
	}
	if err := sliceRule.Compile(); err != nil {
		t.Fatal(err)
	}
	if !sliceRule.ParamsSlice || sliceRule.ParamsMap {
		t.Fatalf("expected a slice-params rule, got ParamsSlice=%v ParamsMap=%v", sliceRule.ParamsSlice, sliceRule.ParamsMap)
	}

	// Matching positional request → matches.
	if !sliceRule.Match(&JsonRpcMsg{Method: "eth_call", Params: []interface{}{"0xdeadbeef"}}) {
		t.Fatal("slice rule should match the matching positional request")
	}
	// Same method but map-shaped params → the slice filter cannot be
	// satisfied, so it must NOT match (no bypass).
	if sliceRule.Match(&JsonRpcMsg{Method: "eth_call", Params: map[string]interface{}{"to": "0xdeadbeef"}}) {
		t.Fatal("slice-params rule must not be bypassed by map-shaped params")
	}

	// Mirror: rule filters by object (map) params.
	mapRule := &JsonRpcRule{
		Methods: []string{"eth_call"},
		Params:  map[string]interface{}{"to": "0xdeadbeef"},
	}
	if err := mapRule.Compile(); err != nil {
		t.Fatal(err)
	}
	if !mapRule.ParamsMap || mapRule.ParamsSlice {
		t.Fatalf("expected a map-params rule, got ParamsMap=%v ParamsSlice=%v", mapRule.ParamsMap, mapRule.ParamsSlice)
	}
	if !mapRule.Match(&JsonRpcMsg{Method: "eth_call", Params: map[string]interface{}{"to": "0xdeadbeef"}}) {
		t.Fatal("map rule should match the matching object request")
	}
	if mapRule.Match(&JsonRpcMsg{Method: "eth_call", Params: []interface{}{"0xdeadbeef"}}) {
		t.Fatal("map-params rule must not be bypassed by array-shaped params")
	}

	// A rule with NO params constraint still matches either shape.
	anyRule := &JsonRpcRule{Methods: []string{"eth_call"}}
	if err := anyRule.Compile(); err != nil {
		t.Fatal(err)
	}
	if !anyRule.Match(&JsonRpcMsg{Method: "eth_call", Params: []interface{}{"x"}}) ||
		!anyRule.Match(&JsonRpcMsg{Method: "eth_call", Params: map[string]interface{}{"a": "b"}}) {
		t.Fatal("params-less rule should match any params shape")
	}
}
