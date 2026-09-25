package cosmoguard

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/v5/pkg/util"
)

// wsDenyLimiter is a RateLimiter that always denies, for proving the WS
// per-rule rate-limit gate fires.
type wsDenyLimiter struct{}

func (wsDenyLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return false, time.Second, nil
}
func (wsDenyLimiter) Close() error { return nil }

// TestWSPolicyVerdict_EnforcesPerRuleAuthAndRate is the regression test
// for the WebSocket JSON-RPC policy bypass: per-rule auth scopes and
// rate limits must be enforced on WS frames, mirroring the HTTP single
// and batch paths. Before the fix, handleRequest only checked
// match/action/cache, so any upgraded client bypassed these gates.
func TestWSPolicyVerdict_EnforcesPerRuleAuthAndRate(t *testing.T) {
	p := &JsonRpcWebSocketProxy{
		auth:        authenticatorWithDefaultRequire(t),
		cgDashboard: newDashboardObservability(),
		section:     "rpc.jsonrpc",
		log:         log.WithField("t", "ws"),
	}
	req := &JsonRpcMsg{Method: "subscribe", ID: float64(1)}

	// Rule gated on the "admin" scope.
	authRule := &JsonRpcRule{Tag: "sub", Action: RuleActionAllow, Auth: &RuleAuthConfig{Scopes: []string{"admin"}}}

	// Anonymous → denied with -32001 (missing scope).
	ok, code, _ := p.policyVerdict(req, authRule, nil, "1.2.3.4", nil)
	if ok || code != -32001 {
		t.Fatalf("anonymous + admin-scope rule: ok=%v code=%d, want deny -32001", ok, code)
	}

	// Identity holding the scope → allowed.
	admin := &Identity{Name: "admin-1", Method: "api-key", Scopes: []string{"admin"}}
	if ok, _, _ := p.policyVerdict(req, authRule, admin, "1.2.3.4", nil); !ok {
		t.Fatalf("admin identity: expected allow")
	}

	// Per-rule rate limit denies → -32005, regardless of identity.
	rateRule := &JsonRpcRule{Tag: "rl", Action: RuleActionAllow, RateLimit: &RateLimitConfig{Scope: RateLimitScopePerIP}}
	limiters := map[uint64]RateLimiter{rateRule.Fingerprint: wsDenyLimiter{}}
	if ok, code, _ := p.policyVerdict(req, rateRule, admin, "1.2.3.4", limiters); ok || code != -32005 {
		t.Fatalf("rate-limited rule: ok=%v code=%d, want deny -32005", ok, code)
	}
}

func TestWSAnonymousIdentityDoesNotConsumeIdentityQuota(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rules []*JsonRpcRule
	}{
		{name: "default allow"},
		{name: "rule allow", rules: []*JsonRpcRule{{Action: RuleActionAllow, Methods: []string{methodSubscribeCosmos}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, rule := range tc.rules {
				if err := rule.Compile(); err != nil {
					t.Fatal(err)
				}
			}
			upstream := newLimitingUpstream()
			broker := NewBroker([]string{"ws://upstream.test"}, "/", 1,
				func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return upstream })
			broker.log = log.WithField("test", t.Name())
			broker.setAdmissionController(newWSAdmissionController(WebSocketLimits{
				MaxSubscriptionsPerClient:   2,
				MaxSubscriptionsPerIdentity: 1,
			}))
			proxy := &JsonRpcWebSocketProxy{
				broker:        broker,
				admission:     broker.admission,
				rules:         tc.rules,
				defaultAction: RuleActionAllow,
				cgDashboard:   newDashboardObservability(),
				section:       "rpc.jsonrpc",
				path:          "/websocket",
				log:           log.WithField("test", t.Name()),
			}
			unbucketed := []*Identity{
				{Name: "guest", Method: "anonymous"},
				{Name: "degraded", Method: "external-validator-degraded", Degraded: true},
			}
			for identityIndex, identity := range unbucketed {
				for i := 0; i < 2; i++ {
					client, peer := newWSCacheClient(t)
					request := &JsonRpcMsg{
						Version: jsonRpcVersion,
						ID:      identityIndex*10 + i + 1,
						Method:  methodSubscribeCosmos,
						Params:  []any{identity.Name + "-" + string(rune('a'+i))},
					}
					if err := proxy.handleRequest(client, request, "192.0.2.1", identity); err != nil {
						t.Fatal(err)
					}
					var response JsonRpcMsg
					if err := peer.ReadJSON(&response); err != nil {
						t.Fatal(err)
					}
					if response.Error != nil {
						t.Fatalf("%s subscription %d rejected: %v", identity.Method, i+1, response.Error)
					}
				}
				if got := broker.admission.identitySubs[identity.Name]; got != 0 {
					t.Fatalf("%s identity quota usage = %d, want 0", identity.Method, got)
				}
			}

			authenticated := &Identity{Name: "alice", Method: "api-key"}
			clientA, peerA := newWSCacheClient(t)
			clientB, peerB := newWSCacheClient(t)
			requests := []*JsonRpcMsg{
				{Version: jsonRpcVersion, ID: 11, Method: methodSubscribeCosmos, Params: []any{"auth-a"}},
				{Version: jsonRpcVersion, ID: 12, Method: methodSubscribeCosmos, Params: []any{"auth-b"}},
			}
			for i, pair := range []struct {
				client *JsonRpcWsClient
				peer   *websocket.Conn
			}{{clientA, peerA}, {clientB, peerB}} {
				if err := proxy.handleRequest(pair.client, requests[i], "192.0.2.2", authenticated); err != nil {
					t.Fatal(err)
				}
				var response JsonRpcMsg
				if err := pair.peer.ReadJSON(&response); err != nil {
					t.Fatal(err)
				}
				if i == 0 && response.Error != nil {
					t.Fatalf("first authenticated subscription rejected: %v", response.Error)
				}
				if i == 1 {
					if response.Error == nil || response.Error.Code != -32005 {
						t.Fatalf("second authenticated response = %+v, want identity exhaustion", response.Error)
					}
					data, ok := response.Error.Data.(map[string]any)
					if !ok || data["scope"] != wsLimitScopeIdentity {
						t.Fatalf("second authenticated response data = %#v, want identity scope", response.Error.Data)
					}
				}
			}
		})
	}
}
