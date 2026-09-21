package cosmoguard

import (
	"io"
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
		require.Nil(t, batch)
		// The message comes back so the caller can tell a request from
		// a notification and answer only the former (§4.1). What
		// matters is that an id is present, not which numeric type the
		// decoder produced for it.
		require.NotNil(t, msg)
		require.NotNil(t, msg.ID)
	})

	t.Run("keeps nothing back when the payload never parsed", func(t *testing.T) {
		msg, batch, err := ParseJsonRpcRequest([]byte(`{"jsonrpc":`))
		require.Error(t, err)
		require.Nil(t, msg, "a half-decoded id must not reach the caller")
		require.Nil(t, batch)
	})

	t.Run("hands a batch back intact for per-member checking", func(t *testing.T) {
		// §6 answers a batch member by member, so the parser must not
		// discard a whole array over one member; handleHttpBatch
		// rejects the offending call on its own.
		msg, batch, err := ParseJsonRpcRequest([]byte(
			`[{"jsonrpc":"2.0","id":1,"method":"status"},` +
				`{"jsonrpc":"2.0","id":2,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}]`))
		require.NoError(t, err)
		require.Nil(t, msg)
		require.Len(t, batch, 2)
		require.Len(t, batch[1].Method, maxJsonRpcMethodBytes+1)
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
		want string
	}{
		{
			name: "single",
			body: `{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`,
			// The id was read, so §5.1's null is not the right answer.
			want: `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":1}`,
		},
		{
			name: "batch",
			body: `[{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}]`,
			want: `[{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":1}]`,
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
			require.JSONEq(t, tt.want, recorder.Body.String())

			h.cgDashboard.unmatchedMu.Lock()
			defer h.cgDashboard.unmatchedMu.Unlock()
			require.Empty(t, h.cgDashboard.unmatched, "rejected request must not reach the unmatched counter")
		})
	}
}

// TestOversizedNotificationGetsNoResponse pins §4.1 for the length
// policy: a notification carries no id and gets no reply however it is
// rejected, the same way the deny and auth paths already treat one.
func TestOversizedNotificationGetsNoResponse(t *testing.T) {
	notification := `{"jsonrpc":"2.0","method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`

	t.Run("http stays silent", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(notification))
		recorder := httptest.NewRecorder()

		h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
			t.Fatal("oversized notification reached upstream")
		}, time.Now())

		require.Empty(t, recorder.Body.String(), "a notification must not be answered")
	})

	t.Run("http still answers a request", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
			t.Fatal("oversized request reached upstream")
		}, time.Now())

		require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":1}`, recorder.Body.String())
	})

	t.Run("websocket hands the frame back so the caller can stay silent", func(t *testing.T) {
		client, peer := newWSCacheClient(t)
		require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte(notification)))

		msg, err := client.ReceiveRequest()

		require.ErrorIs(t, err, ErrInvalidRequest)
		require.NotNil(t, msg, "the rejected frame must come back so §4.1 can be honoured")
		require.Nil(t, msg.ID)
	})
}

// TestOversizedBatchMemberIsRejectedAlone pins §6: a well-formed batch
// is answered member by member, so one oversized method costs only its
// own call — siblings are forwarded and answered, and a notification
// keeps the silence it would have had on its own.
func TestOversizedBatchMemberIsRejectedAlone(t *testing.T) {
	oversized := methodOfLength(maxJsonRpcMethodBytes + 1)

	t.Run("sibling is forwarded and answered", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `[{"jsonrpc":"2.0","id":1,"method":"status"},` +
			`{"jsonrpc":"2.0","id":2,"method":"` + oversized + `"}]`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		var forwarded []byte
		h.handleHttp(recorder, request, func(w http.ResponseWriter, r *http.Request) {
			var err error
			forwarded, err = io.ReadAll(r.Body)
			require.NoError(t, err)
			_, _ = w.Write([]byte(`[{"jsonrpc":"2.0","id":1,"result":"ok"}]`))
		}, time.Now())

		require.JSONEq(t, `[{"jsonrpc":"2.0","id":1,"result":"ok"},`+
			`{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":2}]`,
			recorder.Body.String())
		require.NotContains(t, string(forwarded), oversized, "the rejected member must not reach upstream")
		require.Contains(t, string(forwarded), "status")

		// The valid sibling legitimately lands in the unmatched
		// counter; the rejected member must not, because it is turned
		// away before rule matching.
		for _, r := range unmatchedComponents(h.cgDashboard, h.section) {
			require.NotContains(t, r.value, oversized[:64], "a rejected member reached the unmatched counter")
		}
	})

	t.Run("oversized notification beside a request costs it nothing", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `[{"jsonrpc":"2.0","id":1,"method":"status"},` +
			`{"jsonrpc":"2.0","method":"` + oversized + `"}]`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		h.handleHttp(recorder, request, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[{"jsonrpc":"2.0","id":1,"result":"ok"}]`))
		}, time.Now())

		require.JSONEq(t, `[{"jsonrpc":"2.0","id":1,"result":"ok"}]`, recorder.Body.String())
	})

	t.Run("a batch that answers nothing returns nothing", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `[{"jsonrpc":"2.0","method":"status"},{"jsonrpc":"2.0","method":"` + oversized + `"}]`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		forwardedCalls := 0
		h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
			forwardedCalls++
		}, time.Now())

		require.Equal(t, http.StatusOK, recorder.Code)
		require.Empty(t, recorder.Body.String(), "§6: no responses means no array")
		require.Equal(t, 1, forwardedCalls, "the valid notification is still forwarded")
	})

	t.Run("lone oversized notification never reaches upstream", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `[{"jsonrpc":"2.0","method":"` + oversized + `"}]`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
			t.Fatal("oversized notification reached upstream")
		}, time.Now())

		require.Equal(t, http.StatusOK, recorder.Code)
		require.Empty(t, recorder.Body.String())
	})

	t.Run("duplicate ids still fail the whole batch first", func(t *testing.T) {
		h := newEnvelopeTestHandler(t)
		body := `[{"jsonrpc":"2.0","id":1,"method":"` + oversized + `"},` +
			`{"jsonrpc":"2.0","id":1,"method":"status"}]`
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		recorder := httptest.NewRecorder()

		h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
			t.Fatal("duplicate-id batch reached upstream")
		}, time.Now())

		require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`,
			recorder.Body.String())
	})
}

func TestReceiveRequestRejectsOversizedMethodAsInvalidRequest(t *testing.T) {
	client, peer := newWSCacheClient(t)
	frame := []byte(`{"jsonrpc":"2.0","id":1,"method":"` + methodOfLength(maxJsonRpcMethodBytes+1) + `"}`)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, frame))

	msg, err := client.ReceiveRequest()

	require.ErrorIs(t, err, ErrInvalidRequest)
	require.NotErrorIs(t, err, ErrBadMessage)
	require.NotNil(t, msg, "the rejected frame comes back so §4.1 can be honoured")

	// The same frame read as upstream traffic is delivered, not dropped.
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, frame))
	upstream, err := client.ReceiveMsg()
	require.NoError(t, err)
	require.Len(t, upstream.Method, maxJsonRpcMethodBytes+1)
}
