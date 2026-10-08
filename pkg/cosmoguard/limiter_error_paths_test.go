package cosmoguard

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEmbeddedLimiterFailureEnforcesEveryProtocol(t *testing.T) {
	for _, protocol := range []string{"http", "grpc", "jsonrpc", "websocket"} {
		for _, failure := range []string{"missing", "allow", "constructor"} {
			t.Run(protocol+"/"+failure, func(t *testing.T) {
				cfg := &RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1, Scope: RateLimitScopeGlobal, FailureMode: "fail-closed"}
				var limiter RateLimiter
				if failure == "allow" {
					limiter = failingRateLimiter{err: errors.New("embedded backend unavailable")}
				}
				request := httptest.NewRequest(http.MethodGet, "/status", nil)
				var decide func() bool
				switch protocol {
				case "http":
					rule := &HttpRule{Action: RuleActionAllow, RateLimit: cfg}
					require.NoError(t, rule.Compile())
					if failure == "constructor" {
						limiter = limiterForFailedInit(cfg, errors.New("embedded constructor unavailable"))
					}
					mw := MWRateLimit(func(Request) (*RateLimitConfig, uint64) { return cfg, rule.Fingerprint }, func(uint64) RateLimiter { return limiter }, httpRequestFrom, nil)
					decide = func() bool {
						d := mw(newHTTPRequest(request), func(Request) Decision { return Decision{} })
						if d.Stop {
							require.Equal(t, http.StatusTooManyRequests, d.HTTPStatus)
						}
						return !d.Stop
					}
				case "grpc":
					rule := &GrpcRule{Action: RuleActionAllow, Methods: []string{"/svc/M"}, RateLimit: cfg}
					require.NoError(t, rule.Compile())
					if failure == "constructor" {
						limiter = limiterForFailedInit(cfg, errors.New("embedded constructor unavailable"))
					}
					p := &GrpcProxy{log: log.WithField("test", t.Name()), cgDashboard: newDashboardObservability(), rules: []*GrpcRule{rule}, limiters: map[uint64]RateLimiter{rule.Fingerprint: limiter}}
					decide = func() bool {
						_, err := p.enforcePolicy(t.Context(), "/svc/M")
						if err != nil {
							require.Equal(t, codes.ResourceExhausted, status.Code(err))
						}
						return err == nil
					}
				case "jsonrpc", "websocket":
					rule := &JsonRpcRule{Action: RuleActionAllow, Methods: []string{"m"}, RateLimit: cfg}
					require.NoError(t, rule.Compile())
					if failure == "constructor" {
						limiter = limiterForFailedInit(cfg, errors.New("embedded constructor unavailable"))
					}
					limits := map[uint64]RateLimiter{rule.Fingerprint: limiter}
					msg := &JsonRpcMsg{Method: "m"}
					if protocol == "jsonrpc" {
						h := &JsonRpcHandler{log: log.WithField("test", t.Name()), cgDashboard: newDashboardObservability()}
						decide = func() bool {
							ok, code, _ := h.jsonRpcPolicyVerdict(request, msg, rule, limits)
							if !ok {
								require.Equal(t, -32005, code)
							}
							return ok
						}
					} else {
						p := &JsonRpcWebSocketProxy{log: log.WithField("test", t.Name()), cgDashboard: newDashboardObservability()}
						decide = func() bool {
							ok, code, _ := p.policyVerdict(msg, rule, nil, "127.0.0.1", limits)
							if !ok {
								require.Equal(t, -32005, code)
							}
							return ok
						}
					}
				}
				require.True(t, decide(), "first fallback request fits the burst")
				require.False(t, decide(), "a limiter failure must not bypass the bucket")
			})
		}
	}
}
