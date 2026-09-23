package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPUnknownRuleActionDenies(t *testing.T) {
	p := &HttpProxy{
		log:           log.WithField("test", t.Name()),
		section:       "lcd",
		rules:         []*HttpRule{{Action: "permit", Tag: "invalid"}},
		defaultAction: RuleActionAllow,
		cgDashboard:   newDashboardObservability(),
		cgRequestLog:  enabledLog(t, 10),
	}
	w := httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/status", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("unknown action returned %d, want 403", w.Code)
	}
	denied := p.cgDashboard.denied.Snapshot()
	if len(denied) != 1 || denied[0].Reason != "rule" || denied[0].RuleTag != "invalid" {
		t.Fatalf("unknown action must record a rule denial: %+v", denied)
	}
	entries := p.cgRequestLog.Snapshot([]string{"lcd"}, 10)
	if len(entries) != 1 || entries[0].Status != http.StatusForbidden || entries[0].Action != string(RuleActionDeny) {
		t.Fatalf("unknown action must record a denied HTTP outcome: %+v", entries)
	}
}

func TestJSONRPCSingleUnknownRuleActionDenies(t *testing.T) {
	for _, tt := range []struct {
		name string
		id   any
	}{
		{"call", float64(1)},
		{"notification", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hist := newJSONRPCMetricHistogram("unknown_action_single")
			h := &JsonRpcHandler{
				log:              log.WithField("test", t.Name()),
				section:          "rpc.jsonrpc",
				rules:            []*JsonRpcRule{{Action: "permit", Tag: "invalid"}},
				defaultAction:    RuleActionAllow,
				cgDashboard:      newDashboardObservability(),
				responseTimeHist: hist,
			}
			called := false
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			ctx, _ := WithRequestStats(req.Context())
			req = req.WithContext(ctx)
			h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: tt.id, Method: "status"}, w,
				req,
				func(http.ResponseWriter, *http.Request) { called = true }, time.Now())
			if called {
				t.Fatal("unknown action reached upstream")
			}
			if tt.id == nil {
				if w.Body.Len() != 0 {
					t.Fatalf("notification received response: %s", w.Body.String())
				}
			} else if !strings.Contains(w.Body.String(), `"code":401`) {
				t.Fatalf("expected unauthorized JSON-RPC response, got %s", w.Body.String())
			}
			denied := h.cgDashboard.denied.Snapshot()
			if len(denied) != 1 || denied[0].Reason != "rule" || denied[0].RuleTag != "invalid" {
				t.Fatalf("unknown action must record a rule denial: %+v", denied)
			}
			snap := gatherHistogramSnapshot(t, hist).Protocols["unknown_action_single"]
			if snap.ByAction[string(RuleActionDeny)] != 1 || snap.ByRule["invalid"] != 1 {
				t.Fatalf("unknown action must record a denied JSON-RPC outcome: %+v", snap)
			}
		})
	}
}

func TestJSONRPCBatchUnknownRuleActionDenies(t *testing.T) {
	h := &JsonRpcHandler{
		log:           log.WithField("test", t.Name()),
		section:       "rpc.jsonrpc",
		rules:         []*JsonRpcRule{{Action: "permit", Tag: "invalid"}},
		defaultAction: RuleActionAllow,
		cgDashboard:   newDashboardObservability(),
	}
	called := false
	w := httptest.NewRecorder()
	h.handleHttpBatch(JsonRpcMsgs{
		{Version: "2.0", ID: float64(1), Method: "status"},
		{Version: "2.0", Method: "status"},
	}, w, httptest.NewRequest(http.MethodPost, "/", nil),
		func(http.ResponseWriter, *http.Request) { called = true }, time.Now())
	if called {
		t.Fatal("unknown action reached upstream")
	}
	if strings.Count(w.Body.String(), `"code":401`) != 1 || strings.Count(w.Body.String(), `"id":1`) != 1 {
		t.Fatalf("expected one unauthorized call response and no notification response, got %s", w.Body.String())
	}
	denied := h.cgDashboard.denied.Snapshot()
	if len(denied) != 2 || denied[0].Reason != "rule" || denied[1].Reason != "rule" {
		t.Fatalf("unknown actions must record rule denials: %+v", denied)
	}
}

func TestWebSocketUnknownRuleActionDenies(t *testing.T) {
	p := &JsonRpcWebSocketProxy{
		log:           log.WithField("test", t.Name()),
		section:       "rpc.jsonrpc",
		rules:         []*JsonRpcRule{{Action: "permit", Tag: "invalid"}},
		defaultAction: RuleActionAllow,
		cgDashboard:   newDashboardObservability(),
		cgRequestLog:  enabledLog(t, 10),
	}
	client, peer := newWSCacheClient(t)
	if err := p.handleRequest(client, &JsonRpcMsg{Version: "2.0", ID: float64(1), Method: "status"}, "192.0.2.1", nil); err != nil {
		t.Fatal(err)
	}
	var response JsonRpcMsg
	if err := peer.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized JSON-RPC response, got %+v", response)
	}
	if err := p.handleRequest(nil, &JsonRpcMsg{Version: "2.0", Method: "status"}, "192.0.2.1", nil); err != nil {
		t.Fatal(err)
	}
	denied := p.cgDashboard.denied.Snapshot()
	if len(denied) != 2 || denied[0].Reason != "rule" || denied[1].Reason != "rule" {
		t.Fatalf("unknown actions must record rule denials: %+v", denied)
	}
	entries := p.cgRequestLog.Snapshot([]string{"rpc.jsonrpc"}, 10)
	if len(entries) != 2 || entries[0].Status != http.StatusUnauthorized || entries[0].Action != string(RuleActionDeny) {
		t.Fatalf("unknown actions must record denied WebSocket outcomes: %+v", entries)
	}
}

func TestGRPCUnknownRuleActionDenies(t *testing.T) {
	p := authProxyForTest(t, nil)
	p.rules = []*GrpcRule{{Action: "permit", Tag: "invalid"}}
	_, err := p.enforcePolicy(context.Background(), "/svc/M")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unknown action returned %v, want PermissionDenied", err)
	}
	denied := p.cgDashboard.denied.Snapshot()
	if len(denied) != 1 || denied[0].Reason != "rule" || denied[0].RuleTag != "invalid" {
		t.Fatalf("unknown action must record a rule denial: %+v", denied)
	}
}
