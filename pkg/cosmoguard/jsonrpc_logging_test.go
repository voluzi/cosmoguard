package cosmoguard

import (
	"bytes"
	stdjson "encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJSONRPCOutcomeLogsOmitParams(t *testing.T) {
	const marker = "private-parameter-marker"
	const source = "192.0.2.42"
	const method = "eth_sendRawTransaction"
	const id = "request-7"
	const ruleID = "rule-a"
	const upstream = "upstream-a"

	paramsCases := []struct {
		name   string
		params any
		large  bool
	}{
		{name: "nested object", params: map[string]any{"nested": map[string]any{"secret": marker}}},
		{name: "large array", params: []any{marker, "0x" + strings.Repeat("ab", 32*1024)}, large: true},
		{name: "nil", params: nil},
	}
	levels := []struct {
		name  string
		level slog.Level
	}{
		{name: "info", level: slog.LevelInfo},
		{name: "debug", level: slog.LevelDebug},
	}
	recorders := []struct {
		name         string
		cache        string
		msg          string
		wantDuration bool
		wantUpstream bool
		record       func(*Entry, *http.Request, *JsonRpcMsg)
	}{
		{
			name: "single", cache: cacheHit, msg: "single outcome", wantDuration: true, wantUpstream: true,
			record: func(logger *Entry, r *http.Request, request *JsonRpcMsg) {
				h := &JsonRpcHandler{log: logger}
				h.recordSingle(r, request, cacheHit, string(RuleActionAllow), time.Now(), "single outcome")
			},
		},
		{
			name: "batch", cache: "n/a", msg: "batch outcome", wantUpstream: true,
			record: func(logger *Entry, r *http.Request, request *JsonRpcMsg) {
				h := &JsonRpcHandler{log: logger}
				h.recordBatchItem(r, request, "", "batch outcome")
			},
		},
		{
			name: "websocket", cache: cacheMiss, msg: "websocket outcome", wantDuration: true,
			record: func(logger *Entry, _ *http.Request, request *JsonRpcMsg) {
				p := &JsonRpcWebSocketProxy{log: logger}
				p.recordOutcome(request, source, cacheMiss, string(RuleActionAllow), &JsonRpcRule{Tag: ruleID}, time.Now(), "websocket outcome")
			},
		},
	}

	for _, recorder := range recorders {
		for _, level := range levels {
			for _, paramsCase := range paramsCases {
				t.Run(recorder.name+"/"+level.name+"/"+paramsCase.name, func(t *testing.T) {
					var output bytes.Buffer
					logger := newEntry(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: level.level})))
					r := httptest.NewRequest(http.MethodPost, "http://example.com/", nil)
					r.RemoteAddr = source + ":12345"
					ctx, stats := WithRequestStats(r.Context())
					stats.RuleTag = ruleID
					stats.Upstream = upstream
					r = r.WithContext(ctx)
					request := &JsonRpcMsg{ID: id, Method: method, Params: paramsCase.params}
					before, err := stdjson.Marshal(request.Params)
					require.NoError(t, err)

					recorder.record(logger, r, request)

					raw := output.Bytes()
					var event map[string]any
					decoder := stdjson.NewDecoder(bytes.NewReader(raw))
					require.NoError(t, decoder.Decode(&event))
					var extra map[string]any
					require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
					require.Equal(t, "INFO", event["level"])
					require.Equal(t, recorder.msg, event["msg"])
					require.Equal(t, id, event["id"])
					require.Equal(t, method, event["method"])
					require.Equal(t, recorder.cache, event["cache"])
					require.Equal(t, source, event["source"])
					require.Equal(t, ruleID, event["rule_id"])
					if recorder.wantUpstream {
						require.Equal(t, upstream, event["upstream"])
					}
					if recorder.wantDuration {
						if _, ok := event["duration"]; !ok {
							t.Error("duration field is missing")
						}
					}
					if _, ok := event["params"]; ok {
						t.Error("params field must be absent from outcome log")
					}
					if bytes.Contains(raw, []byte(marker)) {
						t.Error("parameter marker appeared in outcome log")
					}
					if paramsCase.large && len(raw) > 2*1024 {
						t.Errorf("large parameter produced %d log bytes; want at most 2048", len(raw))
					}
					after, err := stdjson.Marshal(request.Params)
					require.NoError(t, err)
					if !bytes.Equal(before, after) {
						t.Error("recording changed request parameters")
					}
				})
			}
		}
	}
}
