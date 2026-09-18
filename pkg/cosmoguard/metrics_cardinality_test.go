package cosmoguard

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type cardinalityDenyLimiter struct{}

func (cardinalityDenyLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return false, time.Second, nil
}

func (cardinalityDenyLimiter) Close() error { return nil }

func TestJSONRPCMetricMethodLabelsAreConfigurationBounded(t *testing.T) {
	hist := newJSONRPCMetricHistogram("cardinality_jsonrpc")
	h := &JsonRpcHandler{
		log:              log.WithField("test", "metrics-cardinality"),
		defaultAction:    RuleActionDeny,
		cgDashboard:      newDashboardObservability(),
		section:          "rpc.jsonrpc",
		responseTimeHist: hist,
	}

	for i := 0; i < 200; i++ {
		recordJSONRPCMetric(t, h, fmt.Sprintf("client_method_%d", i))
	}

	methods := gatheredMethods(t, hist)
	require.Len(t, methods, 1)
	require.Equal(t, map[string]uint64{"other": 200}, methods)

	snap := gatherHistogramSnapshot(t, hist)
	payload := &replicationPayload{History: []MetricsSnapshot{*snap}}
	blob, err := marshalReplicationPayload(payload)
	require.NoError(t, err)
	var restored replicationPayload
	require.NoError(t, msgpack.Unmarshal(blob, &restored))
	require.Equal(t, map[string]uint64{"other": 200}, restored.History[0].Protocols["cardinality_jsonrpc"].ByMethod)
}

func TestJSONRPCMetricMethodLabelsUseMatchedRuleMethods(t *testing.T) {
	hist := newJSONRPCMetricHistogram("configured_jsonrpc")
	rules := []*JsonRpcRule{
		{Tag: "eth-rule", Action: RuleActionDeny, Methods: []string{"eth_*", "eth_blockNumber"}},
		{Tag: "catch-all", Action: RuleActionDeny},
	}
	for _, rule := range rules {
		require.NoError(t, rule.Compile())
	}
	h := &JsonRpcHandler{
		log:              log.WithField("test", "metrics-cardinality"),
		rules:            rules,
		defaultAction:    RuleActionDeny,
		cgDashboard:      newDashboardObservability(),
		section:          "rpc.jsonrpc",
		responseTimeHist: hist,
	}

	recordJSONRPCMetric(t, h, "eth_blockNumber")
	recordJSONRPCMetric(t, h, "eth_getBalance")
	recordJSONRPCMetric(t, h, "net_version")

	require.Equal(t, map[string]uint64{
		"eth_blockNumber": 1,
		"eth_*":           1,
		"other":           1,
	}, gatheredMethods(t, hist))
}

func TestJSONRPCMetricMethodLabelsCoverMatchedPolicyDenials(t *testing.T) {
	hist := newJSONRPCMetricHistogram("policy_jsonrpc")
	authRule := &JsonRpcRule{
		Tag:     "auth-rule",
		Action:  RuleActionAllow,
		Methods: []string{"private_*"},
		Auth:    &RuleAuthConfig{Scopes: []string{"admin"}},
	}
	rateRule := &JsonRpcRule{
		Tag:       "rate-rule",
		Action:    RuleActionAllow,
		Methods:   []string{"limited_*"},
		RateLimit: &RateLimitConfig{Scope: RateLimitScopePerIP},
	}
	for _, rule := range []*JsonRpcRule{authRule, rateRule} {
		require.NoError(t, rule.Compile())
	}
	h := &JsonRpcHandler{
		log:              log.WithField("test", "metrics-cardinality"),
		rules:            []*JsonRpcRule{authRule, rateRule},
		defaultAction:    RuleActionDeny,
		cgDashboard:      newDashboardObservability(),
		section:          "rpc.jsonrpc",
		auth:             authenticatorWithDefaultRequire(t),
		limiters:         map[uint64]RateLimiter{rateRule.Fingerprint: cardinalityDenyLimiter{}},
		responseTimeHist: hist,
	}

	recordJSONRPCMetric(t, h, "private_read")
	recordJSONRPCMetricWithIdentity(t, h, "limited_once", &Identity{Name: "test-client", Method: "api-key", Scopes: []string{"read"}})

	snap := gatherHistogramSnapshot(t, hist).Protocols["policy_jsonrpc"]
	require.Equal(t, map[string]uint64{"private_*": 1, "limited_*": 1}, snap.ByMethod)
	require.Equal(t, map[string]uint64{"auth-rule": 1, "rate-rule": 1}, snap.ByRule)
	require.True(t, hasDenyReason(h.cgDashboard, "private_read", "auth"))
	require.True(t, hasDenyReason(h.cgDashboard, "limited_once", "rate_limit"))
}

func TestJSONRPCMetricMethodDoesNotComeFromRuleTag(t *testing.T) {
	hist := newJSONRPCMetricHistogram("rule_tag_jsonrpc")
	h := &JsonRpcHandler{
		log:              log.WithField("test", "metrics-cardinality"),
		responseTimeHist: hist,
	}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, stats := WithRequestStats(req.Context())
	stats.RuleTag = "configured-looking-method"
	req = req.WithContext(ctx)
	h.recordSingle(req, &JsonRpcMsg{Method: "client-controlled"}, cacheMiss, string(RuleActionDeny), time.Now(), "denied")

	require.Equal(t, map[string]uint64{"other": 1}, gatheredMethods(t, hist))
}

func TestWebSocketMetricMethodLabelsAreConfigurationBounded(t *testing.T) {
	hist := newJSONRPCMetricHistogram("websocket_cardinality")
	p := &JsonRpcWebSocketProxy{
		log:              log.WithField("test", "metrics-cardinality"),
		defaultAction:    RuleActionDeny,
		cgDashboard:      newDashboardObservability(),
		section:          "rpc.jsonrpc",
		responseTimeHist: hist,
	}
	p.SetRequestLog(enabledLog(t, 300))

	for i := 0; i < 200; i++ {
		require.NoError(t, p.handleRequest(nil, &JsonRpcMsg{Method: fmt.Sprintf("client_ws_method_%d", i)}, "127.0.0.1", nil))
	}
	methods := gatheredMethods(t, hist)
	require.Len(t, methods, 1)
	require.Equal(t, map[string]uint64{"other": 200}, methods)
	require.True(t, requestLogHasMethod(p.cgRequestLog, "client_ws_method_199"))
}

func TestWebSocketMetricMethodLabelsCoverMatchesAndPolicyDenials(t *testing.T) {
	hist := newJSONRPCMetricHistogram("websocket_policy")
	exactGlobRule := &JsonRpcRule{Tag: "eth-rule", Action: RuleActionDeny, Methods: []string{"eth_*", "eth_blockNumber"}}
	catchAllRule := &JsonRpcRule{Tag: "catch-all", Action: RuleActionDeny}
	authRule := &JsonRpcRule{Tag: "auth-rule", Action: RuleActionAllow, Methods: []string{"private_*"}, Auth: &RuleAuthConfig{Scopes: []string{"admin"}}}
	rateRule := &JsonRpcRule{Tag: "rate-rule", Action: RuleActionAllow, Methods: []string{"limited_*"}, RateLimit: &RateLimitConfig{Scope: RateLimitScopePerIP}}
	for _, rule := range []*JsonRpcRule{exactGlobRule, catchAllRule, authRule, rateRule} {
		require.NoError(t, rule.Compile())
	}
	p := &JsonRpcWebSocketProxy{
		log:              log.WithField("test", "metrics-cardinality"),
		rules:            []*JsonRpcRule{exactGlobRule, authRule, rateRule, catchAllRule},
		defaultAction:    RuleActionDeny,
		cgDashboard:      newDashboardObservability(),
		section:          "rpc.jsonrpc",
		auth:             authenticatorWithDefaultRequire(t),
		limiters:         map[uint64]RateLimiter{rateRule.Fingerprint: cardinalityDenyLimiter{}},
		responseTimeHist: hist,
	}

	for _, method := range []string{"eth_blockNumber", "eth_getBalance", "private_read", "net_version"} {
		require.NoError(t, p.handleRequest(nil, &JsonRpcMsg{Method: method}, "127.0.0.1", nil))
	}
	require.NoError(t, p.handleRequest(nil, &JsonRpcMsg{Method: "limited_once"}, "127.0.0.1",
		&Identity{Name: "test-client", Method: "api-key", Scopes: []string{"read"}}))

	snap := gatherHistogramSnapshot(t, hist).Protocols["websocket_policy"]
	require.Equal(t, map[string]uint64{
		"eth_blockNumber": 1,
		"eth_*":           1,
		"private_*":       1,
		"limited_*":       1,
		"other":           1,
	}, snap.ByMethod)
	require.Equal(t, uint64(1), snap.ByRule["auth-rule"])
	require.Equal(t, uint64(1), snap.ByRule["rate-rule"])
	require.True(t, hasDenyReason(p.cgDashboard, "private_read", "auth"))
	require.True(t, hasDenyReason(p.cgDashboard, "limited_once", "rate_limit"))
}

func TestHTTPMetricMethodLabelsAllowlistStandardVerbs(t *testing.T) {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cardinality_http",
		Name:      "request_duration_seconds",
	}, []string{"method", "status_code", "cache", "action", "rule_id", "upstream"})
	p := &HttpProxy{
		log:              log.WithField("test", "metrics-cardinality"),
		section:          "lcd",
		responseTimeHist: hist,
		cgRequestLog:     enabledLog(t, 300),
	}
	standard := []string{
		http.MethodConnect,
		http.MethodDelete,
		http.MethodGet,
		http.MethodHead,
		http.MethodOptions,
		http.MethodPatch,
		http.MethodPost,
		http.MethodPut,
		http.MethodTrace,
	}
	for _, method := range standard {
		req := httptest.NewRequest(method, "/", nil)
		p.recordOutcome(req, http.StatusOK, cacheMiss, string(RuleActionAllow), time.Now(), "allowed")
	}
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(fmt.Sprintf("CUSTOM%d", i), "/", nil)
		p.recordOutcome(req, http.StatusOK, cacheMiss, string(RuleActionAllow), time.Now(), "allowed")
	}

	want := map[string]uint64{"OTHER": 200}
	for _, method := range standard {
		want[method] = 1
	}
	methods := gatheredMethods(t, hist)
	require.Len(t, methods, len(want))
	require.Equal(t, want, methods)
	require.True(t, requestLogHasMethod(p.cgRequestLog, "CUSTOM199"))
}

func newJSONRPCMetricHistogram(namespace string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "request_duration_seconds",
	}, []string{"method", "cache", "action", "rule_id", "upstream"})
}

func recordJSONRPCMetric(t *testing.T, h *JsonRpcHandler, method string) {
	t.Helper()
	recordJSONRPCMetricWithIdentity(t, h, method, nil)
}

func recordJSONRPCMetricWithIdentity(t *testing.T, h *JsonRpcHandler, method string, identity *Identity) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx, _ := WithRequestStats(req.Context())
	if identity != nil {
		ctx = context.WithValue(ctx, identityCtxKey{}, identity)
	}
	req = req.WithContext(ctx)
	h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", Method: method}, httptest.NewRecorder(), req,
		func(http.ResponseWriter, *http.Request) { t.Fatal("request must not reach upstream") }, time.Now())
}

func gatheredMethods(t *testing.T, hist *prometheus.HistogramVec) map[string]uint64 {
	t.Helper()
	snap := gatherHistogramSnapshot(t, hist)
	require.Len(t, snap.Protocols, 1)
	for _, protocol := range snap.Protocols {
		return protocol.ByMethod
	}
	return nil
}

func gatherHistogramSnapshot(t *testing.T, hist *prometheus.HistogramVec) *MetricsSnapshot {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(hist)
	snap, err := gatherMetricsSnapshot(reg)
	require.NoError(t, err)
	return snap
}

func requestLogHasMethod(requests *requestLog, method string) bool {
	for _, entry := range requests.Snapshot(nil, 0) {
		if entry.Method == method {
			return true
		}
	}
	return false
}

func hasDenyReason(dashboard *dashboardObservability, method, reason string) bool {
	for _, deny := range dashboard.denied.Snapshot() {
		if deny.Method == method && deny.Reason == reason {
			return true
		}
	}
	return false
}
