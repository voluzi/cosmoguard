package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func methodOfLength(n int) string { return strings.Repeat("m", n) }

func TestParseJsonRpcMessageRejectsOversizedMethod(t *testing.T) {
	t.Run("accepts a method at the limit", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcMessage([]byte(
			`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes) + `"}`))
		require.NoError(t, err)
		require.Nil(t, batch)
		require.Len(t, msg.Method, maxJsonRpcMethodBytes)
	})

	t.Run("rejects one byte over", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcMessage([]byte(
			`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`))
		require.ErrorIs(t, err, ErrInvalidRequest)
		require.Nil(t, msg)
		require.Nil(t, batch)
	})

	t.Run("rejects the whole batch when one member is oversized", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcMessage([]byte(
			`[{"jsonrpc":"2.0","id":1,"method":"status"},` +
				`{"jsonrpc":"2.0","id":2,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}]`))
		require.ErrorIs(t, err, ErrInvalidRequest)
		require.Nil(t, msg)
		require.Nil(t, batch)
	})

	t.Run("leaves upstream responses alone", func(t *testing.T) {
		// Responses carry no method, and the parser is shared with the
		// upstream read path — a large result must still parse.
		msg, _, err := ParseJsonRpcMessage([]byte(
			`{"jsonrpc":"2.0","id":1,"result":"` + strings.Repeat("r", 1<<20) + `"}`))
		require.NoError(t, err)
		require.Len(t, msg.Result, (1<<20)+2) // quoted
	})
}

// TestHandleHTTPRejectsOversizedMethodBeforeRuleMatching proves the
// rejection happens in the parser, before rule matching — so an
// oversized method never reaches the observability sinks at all.
func TestHandleHTTPRejectsOversizedMethodBeforeRuleMatching(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "single",
			body: `{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`,
		},
		{
			name: "batch",
			body: `[{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnvelopeTestHandler(t)
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()

			h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
				t.Fatal("oversized method reached upstream")
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())

			h.cgDashboard.unmatchedMu.Lock()
			defer h.cgDashboard.unmatchedMu.Unlock()
			require.Empty(t, h.cgDashboard.unmatched, "rejected request must not reach the unmatched counter")
		})
	}
}

func TestReceiveMsgRejectsOversizedMethodAsInvalidRequest(t *testing.T) {
	client, peer := newWSCacheClient(t)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte(
		`{"jsonrpc":"2.0","id":1,"method":"`+methodOfLength(maxJsonRpcMethodBytes+1)+`"}`,
	)))

	msg, err := client.ReceiveMsg()

	require.Nil(t, msg)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.NotErrorIs(t, err, ErrBadMessage)
}
