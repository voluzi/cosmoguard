package testharness_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestMetricsEndpoint_Shape drives some real traffic through the
// proxy, sleeps long enough for the histograms to absorb the
// observations, then asserts /api/v1/metrics returns a populated
// JSON snapshot the dashboard can chart from. Pins the contract
// the client polls — protocol slugs, histogram count > 0, by_cache
// populated, upstream gauge entries.
func TestMetricsEndpoint_Shape(t *testing.T) {
	cfg := &cosmoguard.Config{
		Cache: cosmoguard.CacheGlobalConfig{TTL: 5 * time.Second},
		LCD: cosmoguard.LcdConfig{
			Default: cosmoguard.RuleActionAllow,
			Rules: []*cosmoguard.HttpRule{
				{
					Priority: 100, Action: cosmoguard.RuleActionAllow,
					Paths: []string{"/cosmos/bank/v1beta1/params"},
					Cache: &cosmoguard.RuleCache{Enable: true, TTL: 10 * time.Second},
				},
			},
		},
		RPC: cosmoguard.RpcConfig{
			Default: cosmoguard.RuleActionAllow,
			JsonRpc: cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithDashboardEnabled("", ""),
	)
	// Seed the fake LCD upstream with a canned response so the
	// proxied call actually 200s (the FakeHTTPServer 404s for
	// unconfigured paths). Without this the histogram still
	// fires, but the by_status map would only carry "404" and
	// the test would be measuring deny-path observations
	// instead of the happy-path it claims to cover.
	h.Upstream.LCD.SetResponse(http.MethodGet, "/cosmos/bank/v1beta1/params",
		testharness.FakeResponse{
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       []byte(`{"params":{"send_enabled":[],"default_send_enabled":true}}`),
		})

	// Drive 5 LCD requests to populate the lcd histogram (after
	// the first miss + cache write, the next 4 should hit the
	// cache and add to by_cache["hit"]).
	for i := 0; i < 5; i++ {
		r := h.GET(t, h.LCDURL+"/cosmos/bank/v1beta1/params")
		assert.Equal(t, r.StatusCode, http.StatusOK,
			"LCD request %d should succeed before metrics check", i)
	}

	// Tiny pause — observations are synchronous on Observe() but
	// the registry's read snapshot is published lazily by
	// client_golang. 25ms is generous.
	time.Sleep(25 * time.Millisecond)

	r := h.GET(t, h.DashboardURL+"/api/v1/metrics")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	var snap cosmoguard.MetricsSnapshot
	assert.NilError(t, json.Unmarshal(r.Body, &snap))

	assert.Assert(t, snap.TimestampMs > 0, "timestamp must be populated")

	lcd, ok := snap.Protocols["lcd"]
	assert.Assert(t, ok, "expected lcd in protocols: %v", snap.Protocols)
	assert.Assert(t, lcd.Histogram.Count >= 5,
		"lcd histogram should have absorbed >=5 observations, got %d", lcd.Histogram.Count)
	assert.Assert(t, len(lcd.ByCache) > 0,
		"by_cache should be populated; got %v", lcd.ByCache)
	assert.Assert(t, lcd.ByAction["allow"] >= 5,
		"by_action[allow] should be >=5; got %d", lcd.ByAction["allow"])
	assert.Assert(t, len(snap.Upstreams) >= 1,
		"upstreams gauge should expose at least one entry; got %v", snap.Upstreams)

	// Buckets must come back sorted + cumulative; the client
	// derives percentiles from this ordering.
	for i := 1; i < len(lcd.Histogram.Buckets); i++ {
		assert.Assert(t, lcd.Histogram.Buckets[i-1].Le < lcd.Histogram.Buckets[i].Le,
			"buckets not strictly ascending by Le: %+v", lcd.Histogram.Buckets)
		assert.Assert(t, lcd.Histogram.Buckets[i-1].Count <= lcd.Histogram.Buckets[i].Count,
			"buckets not cumulative: %+v", lcd.Histogram.Buckets)
	}
}

// TestMetricsEndpoint_Headers pins the security-header set on the
// new endpoint matches the rest of /api/v1/* — nosniff + no-store +
// JSON Content-Type. Without this an operator could intermediary-
// cache the dashboard's live metrics and stare at stale data.
func TestMetricsEndpoint_Headers(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("", ""),
	)
	r := h.GET(t, h.DashboardURL+"/api/v1/metrics")
	assert.Equal(t, r.StatusCode, http.StatusOK)
	assert.Assert(t, strings.Contains(r.Header.Get("Content-Type"), "application/json"),
		"Content-Type should be application/json; got %q", r.Header.Get("Content-Type"))
	assert.Equal(t, r.Header.Get("X-Content-Type-Options"), "nosniff")
	assert.Equal(t, r.Header.Get("Cache-Control"), "no-store")
}

// TestMetricsEndpoint_AuthRequired pins that the basic-auth gate
// wraps the new endpoint identically to the rest of /api/v1/*. An
// operator who configured a dashboard password must not see the
// metrics endpoint as a hole around the auth wrapper.
func TestMetricsEndpoint_AuthRequired(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("ops", "let-me-in"),
	)
	r := h.GET(t, h.DashboardURL+"/api/v1/metrics")
	assert.Equal(t, r.StatusCode, http.StatusUnauthorized,
		"unauthenticated /api/v1/metrics should 401")
	assert.Assert(t, strings.HasPrefix(r.Header.Get("WWW-Authenticate"), "Basic"))

	// With correct credentials → 200 JSON.
	req, err := http.NewRequest(http.MethodGet, h.DashboardURL+"/api/v1/metrics", nil)
	assert.NilError(t, err)
	req.SetBasicAuth("ops", "let-me-in")
	r = h.Do(t, req)
	assert.Equal(t, r.StatusCode, http.StatusOK)
}

// TestDashboard_CatchAll404_StillWorks regression-pins that the new
// metrics route doesn't accidentally shadow the catch-all 404. A
// regression that re-ordered the handler registrations (mux puts
// the longest pattern first, but a future split that broke that
// invariant would silently return 200 with the metrics body for
// /api/v1/metrics/anything).
func TestDashboard_CatchAll404_StillWorks(t *testing.T) {
	h := testharness.New(t,
		testharness.WithDashboardEnabled("", ""),
	)
	for _, p := range []string{
		"/api/v1/metrics/extra",
		"/api/v1/metrics/",
		"/api/v1/unknownpath",
	} {
		t.Run(p, func(t *testing.T) {
			r := h.GET(t, h.DashboardURL+p)
			assert.Equal(t, r.StatusCode, http.StatusNotFound,
				"%s should still 404 after adding the metrics endpoint", p)
		})
	}
}
