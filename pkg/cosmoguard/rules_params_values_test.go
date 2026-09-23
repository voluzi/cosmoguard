package cosmoguard

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxExactJSONInteger = int64(9007199254740991)

func parseSingleJSONRPCRequest(t *testing.T, body string) *JsonRpcMsg {
	t.Helper()

	request, batch, err := ParseJsonRpcMessage([]byte(body))
	require.NoError(t, err)
	require.Nil(t, batch)
	require.NotNil(t, request)
	return request
}

func TestJsonRpcRuleNumericParamsMatchEquivalentJSONValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ruleParams any
		request    string
	}{
		{
			name:       "map integer",
			ruleParams: map[string]any{"height": 10},
			request:    `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":10}}`,
		},
		{
			name:       "map negative integer",
			ruleParams: map[string]any{"height": -10},
			request:    `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":-1e1}}`,
		},
		{
			name:       "map fraction",
			ruleParams: map[string]any{"ratio": 1.25},
			request:    `{"jsonrpc":"2.0","id":1,"method":"block","params":{"ratio":1.25}}`,
		},
		{
			name:       "map float32 fraction",
			ruleParams: map[string]any{"ratio": float32(0.1)},
			request:    `{"jsonrpc":"2.0","id":1,"method":"block","params":{"ratio":0.1}}`,
		},
		{
			name:       "slice unsigned integer",
			ruleParams: []any{uint32(10)},
			request:    `{"jsonrpc":"2.0","id":1,"method":"block","params":[10.0]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"block"}, Params: tt.ruleParams}
			require.NoError(t, rule.Compile())

			request := parseSingleJSONRPCRequest(t, tt.request)
			assert.True(t, rule.Match(request))
		})
	}
}

func TestJsonRpcRuleScalarParamsPreserveFlatMatchingSemantics(t *testing.T) {
	t.Parallel()

	t.Run("map subset with booleans null and glob", func(t *testing.T) {
		t.Parallel()

		rule := &JsonRpcRule{
			Action:  RuleActionAllow,
			Methods: []string{"query"},
			Params: map[string]any{
				"enabled": true,
				"cursor":  nil,
				"path":    "/cosmos/*",
			},
		}
		require.NoError(t, rule.Compile())

		matching := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"query","params":{"enabled":true,"cursor":null,"path":"/cosmos/bank","extra":42}}`)
		assert.True(t, rule.Match(matching))

		missingNull := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"query","params":{"enabled":true,"path":"/cosmos/bank"}}`)
		assert.False(t, rule.Match(missingNull), "explicit null must not match an absent key")

		wrongBool := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"query","params":{"enabled":false,"cursor":null,"path":"/cosmos/bank"}}`)
		assert.False(t, rule.Match(wrongBool))
	})

	t.Run("slice prefix", func(t *testing.T) {
		t.Parallel()

		rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"query"}, Params: []any{10, true, nil}}
		require.NoError(t, rule.Compile())

		matching := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"query","params":[10,true,null,"extra"]}`)
		assert.True(t, rule.Match(matching))

		tooShort := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"query","params":[10,true]}`)
		assert.False(t, rule.Match(tooShort))
	})
}

func TestJsonRpcRuleMatchesProgrammaticIntegerRequestParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ruleParams    any
		requestParams any
	}{
		{
			name:          "map",
			ruleParams:    map[string]any{"height": float64(10)},
			requestParams: map[string]any{"height": int64(10)},
		},
		{
			name:          "slice",
			ruleParams:    []any{float64(-10)},
			requestParams: []any{int8(-10)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"block"}, Params: tt.ruleParams}
			require.NoError(t, rule.Compile())
			assert.True(t, rule.Match(&JsonRpcMsg{Method: "block", Params: tt.requestParams}))
		})
	}
}

func TestJsonRpcRuleCompileRejectsUnsupportedParamValues(t *testing.T) {
	t.Parallel()

	type unsupportedScalar struct{ Value string }

	tests := []struct {
		name       string
		params     any
		wantDetail string
	}{
		{name: "nested map", params: map[string]any{"filter": map[string]any{"address": "0x1"}}, wantDetail: `params["filter"]`},
		{name: "nested slice", params: []any{[]any{"nested"}}, wantDetail: "params[0]"},
		{name: "timestamp", params: map[string]any{"at": time.Unix(0, 0)}, wantDetail: `params["at"]`},
		{name: "unsupported scalar", params: []any{unsupportedScalar{Value: "x"}}, wantDetail: "params[0]"},
		{name: "integer above exact range", params: map[string]any{"height": maxExactJSONInteger + 1}, wantDetail: `params["height"]`},
		{name: "integer below exact range", params: []any{-maxExactJSONInteger - 1}, wantDetail: "params[0]"},
		{name: "unsigned integer above exact range", params: []any{uint64(maxExactJSONInteger) + 1}, wantDetail: "params[0]"},
		{name: "floating point integer above exact range", params: map[string]any{"height": float64(maxExactJSONInteger + 1)}, wantDetail: `params["height"]`},
		{name: "floating point integer below exact range", params: []any{float64(-maxExactJSONInteger - 1)}, wantDetail: "params[0]"},
		{name: "positive infinity", params: map[string]any{"ratio": math.Inf(1)}, wantDetail: `params["ratio"]`},
		{name: "negative infinity", params: map[string]any{"ratio": math.Inf(-1)}, wantDetail: `params["ratio"]`},
		{name: "not a number", params: []any{math.NaN()}, wantDetail: "params[0]"},
		{name: "top level string", params: "height", wantDetail: "params"},
		{name: "top level scalar", params: 10, wantDetail: "params"},
		{name: "typed map", params: map[string]int{"height": 10}, wantDetail: "params"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := &JsonRpcRule{Priority: 17, Action: RuleActionAllow, Params: tt.params}
			err := rule.Compile()
			require.Error(t, err)
			assert.ErrorContains(t, err, "jsonrpc rule (priority 17)")
			assert.ErrorContains(t, err, tt.wantDetail)
		})
	}
}

func TestJsonRpcRuleCompileAcceptsExactIntegerBoundaries(t *testing.T) {
	t.Parallel()

	rule := &JsonRpcRule{Action: RuleActionAllow, Params: []any{maxExactJSONInteger, -maxExactJSONInteger, uint64(maxExactJSONInteger)}}
	require.NoError(t, rule.Compile())

	assert.True(t, rule.Match(&JsonRpcMsg{Params: []any{
		float64(maxExactJSONInteger),
		float64(-maxExactJSONInteger),
		float64(maxExactJSONInteger),
	}}))
}

func TestJsonRpcRuleCompileDoesNotMutateCallerParams(t *testing.T) {
	t.Parallel()

	mapInput := map[string]any{"height": 10}
	mapRule := &JsonRpcRule{Action: RuleActionAllow, Params: mapInput}
	require.NoError(t, mapRule.Compile())
	require.IsType(t, int(0), mapInput["height"])
	require.IsType(t, float64(0), mapRule.Params.(map[string]any)["height"])

	mapInput["height"] = 20
	assert.True(t, mapRule.Match(&JsonRpcMsg{Params: map[string]any{"height": 10}}))
	assert.False(t, mapRule.Match(&JsonRpcMsg{Params: map[string]any{"height": 20}}))

	sliceInput := []any{int32(10)}
	sliceRule := &JsonRpcRule{Action: RuleActionAllow, Params: sliceInput}
	require.NoError(t, sliceRule.Compile())
	require.IsType(t, int32(0), sliceInput[0])
	require.IsType(t, float64(0), sliceRule.Params.([]any)[0])

	sliceInput[0] = int32(20)
	assert.True(t, sliceRule.Match(&JsonRpcMsg{Params: []any{10}}))
	assert.False(t, sliceRule.Match(&JsonRpcMsg{Params: []any{20}}))
}

func TestJsonRpcRuleCompilePublishesStateOnlyAfterSuccessfulValidation(t *testing.T) {
	t.Parallel()

	rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"block"}, Params: map[string]any{"height": 10}}
	require.NoError(t, rule.Compile())
	originalFingerprint := rule.Fingerprint

	rule.Params = map[string]any{"filter": map[string]any{"address": "0x1"}}
	require.Error(t, rule.Compile())
	assert.Equal(t, originalFingerprint, rule.Fingerprint)
	assert.True(t, rule.Match(&JsonRpcMsg{Method: "block", Params: map[string]any{"height": 10}}))
}

func TestJsonRpcRuleCompileClearsStaleParamMatcherState(t *testing.T) {
	t.Parallel()

	rule := &JsonRpcRule{Action: RuleActionAllow, Params: map[string]any{"path": "/cosmos/*"}}
	require.NoError(t, rule.Compile())
	require.Contains(t, rule.ParamsGlobs, "path")

	rule.Params = []any{10}
	require.NoError(t, rule.Compile())
	assert.Empty(t, rule.ParamsGlobs)
	assert.False(t, rule.ParamsMap)
	assert.True(t, rule.ParamsSlice)
	assert.False(t, rule.Match(&JsonRpcMsg{Params: map[string]any{"path": "/cosmos/bank"}}))
	assert.True(t, rule.Match(&JsonRpcMsg{Params: []any{10}}))
}

func TestJsonRpcRuleMatchRejectsNestedRequestValuesWithoutPanicking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ruleParams    any
		requestParams any
	}{
		{
			name:          "map",
			ruleParams:    map[string]any{"height": 10},
			requestParams: map[string]any{"height": map[string]any{"nested": true}},
		},
		{
			name:          "slice",
			ruleParams:    []any{10},
			requestParams: []any{[]any{10}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule := &JsonRpcRule{Action: RuleActionAllow, Params: tt.ruleParams}
			require.NoError(t, rule.Compile())

			assert.NotPanics(t, func() {
				assert.False(t, rule.Match(&JsonRpcMsg{Params: tt.requestParams}))
			})
		})
	}
}

func TestJsonRpcRuleNormalizedParamsProduceEquivalentFingerprints(t *testing.T) {
	t.Parallel()

	rules := []*JsonRpcRule{
		{Action: RuleActionDeny, Methods: []string{"block"}, Params: map[string]any{"height": 10, "finalized": true}},
		{Action: RuleActionDeny, Methods: []string{"block"}, Params: map[string]any{"finalized": true, "height": uint16(10)}},
		{Action: RuleActionDeny, Methods: []string{"block"}, Params: map[string]any{"height": float64(10), "finalized": true}},
	}

	for _, rule := range rules {
		require.NoError(t, rule.Compile())
	}
	assert.Equal(t, rules[0].Fingerprint, rules[1].Fingerprint)
	assert.Equal(t, rules[0].Fingerprint, rules[2].Fingerprint)
}

func TestJsonRpcHandlerNumericDenyRuleOverridesDefaultAllow(t *testing.T) {
	t.Parallel()

	rule := &JsonRpcRule{
		Action:  RuleActionDeny,
		Methods: []string{"block"},
		Params:  map[string]any{"height": 10},
	}
	require.NoError(t, rule.Compile())

	handler := &JsonRpcHandler{
		log:           log.WithField("test", "numeric-deny"),
		rules:         []*JsonRpcRule{rule},
		defaultAction: RuleActionAllow,
		cgDashboard:   newDashboardObservability(),
		section:       "rpc.jsonrpc",
	}
	request := parseSingleJSONRPCRequest(t, `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":10}}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	upstreamCalled := false
	next := func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalled = true
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"forwarded"}`))
	}

	handler.handleHttpSingle(request, w, r, next, time.Now())

	assert.False(t, upstreamCalled, "matching deny rule must stop the request before upstream")
	require.NotNil(t, w.Result())
	assert.Contains(t, w.Body.String(), `"code":401`)
}
