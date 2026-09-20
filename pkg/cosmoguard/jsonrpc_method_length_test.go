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

func TestParseJsonRpcRequestRejectsOversizedMethod(t *testing.T) {
	t.Run("accepts a method at the limit", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcRequest([]byte(
			`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes) + `"}`))
		require.NoError(t, err)
		require.Nil(t, batch)
		require.Len(t, msg.Method, maxJsonRpcMethodBytes)
	})

	t.Run("rejects one byte over", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcRequest([]byte(
			`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`))
		require.ErrorIs(t, err, ErrInvalidRequest)
		require.Nil(t, msg)
		require.Nil(t, batch)
	})

	t.Run("rejects the whole batch when one member is oversized", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcRequest([]byte(
			`[{"jsonrpc":"2.0","id":1,"method":"status"},` +
				`{"jsonrpc":"2.0","id":2,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}]`))
		require.ErrorIs(t, err, ErrInvalidRequest)
		require.Nil(t, msg)
		require.Nil(t, batch)
	})

	t.Run("keeps a valid batch intact", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcRequest([]byte(
			`[{"jsonrpc":"2.0","id":1,"method":"status"},{"jsonrpc":"2.0","id":2,"method":"health"}]`))
		require.NoError(t, err)
		require.Nil(t, msg)
		require.Len(t, batch, 2)
		require.Equal(t, "health", batch[1].Method)
	})
}

// TestParseJsonRpcMessageKeepsUpstreamMethodsUnbounded pins the split:
// the length cap is a policy on client requests. An upstream is
// configured by the operator, and dropping its notification would cost
// a subscriber an event it is entitled to.
func TestParseJsonRpcMessageKeepsUpstreamMethodsUnbounded(t *testing.T) {
	t.Run("upstream notification with a long method", func(t *testing.T) {
		long := methodOfLength(maxJsonRpcMethodBytes + 1)
		msg, _, err := ParseJsonRpcMessage([]byte(`{"jsonrpc":"2.0","method":"` + long + `","params":{}}`))
		require.NoError(t, err)
		require.Equal(t, long, msg.Method)
	})

	t.Run("upstream response with a large result", func(t *testing.T) {
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

func TestReceiveRequestRejectsOversizedMethodAsInvalidRequest(t *testing.T) {
	client, peer := newWSCacheClient(t)
	frame := []byte(`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, frame))

	msg, err := client.ReceiveRequest()

	require.Nil(t, msg)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.NotErrorIs(t, err, ErrBadMessage)

	// The same frame read as upstream traffic is delivered, not dropped.
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, frame))
	upstream, err := client.ReceiveMsg()
	require.NoError(t, err)
	require.Len(t, upstream.Method, maxJsonRpcMethodBytes+1)
}
