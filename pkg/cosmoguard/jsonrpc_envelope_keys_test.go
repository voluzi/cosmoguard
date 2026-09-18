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

func TestParseJsonRpcMessageRejectsAmbiguousReservedEnvelopeKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "jsonrpc case variant", body: `{"jsonrpc":"2.0","JsonRpc":"1.0","id":1,"method":"status"}`},
		{name: "id case variant", body: `{"jsonrpc":"2.0","id":1,"ID":2,"method":"status"}`},
		{name: "method case variant", body: `{"jsonrpc":"2.0","id":1,"method":"status","Method":"unsafe"}`},
		{name: "params case variant", body: `{"jsonrpc":"2.0","id":1,"method":"status","params":[],"Params":["unsafe"]}`},
		{name: "duplicate jsonrpc", body: `{"jsonrpc":"2.0","jsonrpc":"1.0","id":1,"method":"status"}`},
		{name: "duplicate id", body: `{"jsonrpc":"2.0","id":1,"id":2,"method":"status"}`},
		{name: "duplicate method", body: `{"jsonrpc":"2.0","id":1,"method":"status","method":"unsafe"}`},
		{name: "duplicate params", body: `{"jsonrpc":"2.0","id":1,"method":"status","params":[],"params":["unsafe"]}`},
		{name: "escaped duplicate method", body: `{"jsonrpc":"2.0","id":1,"method":"status","m\u0065thod":"unsafe"}`},
		{name: "unicode fold variant", body: `{"j\u017fonrpc":"2.0","id":1,"method":"status"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			single, batch, err := ParseJsonRpcMessage([]byte(tt.body))

			require.ErrorIs(t, err, ErrInvalidRequest)
			require.Nil(t, single)
			require.Nil(t, batch)
		})
	}
}

func TestParseJsonRpcMessageAllowsUnambiguousEnvelopes(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantBatch bool
	}{
		{
			name: "nested reserved names",
			body: `{"jsonrpc":"2.0","id":1,"method":"status","params":{"Method":"nested","method":"nested-again"}}`,
		},
		{
			name: "extension fields",
			body: `{"jsonrpc":"2.0","id":1,"method":"status","trace":"first","Trace":"second","trace":"third"}`,
		},
		{
			name: "response",
			body: `{"jsonrpc":"2.0","id":1,"result":{"Method":"nested"}}`,
		},
		{
			name:      "batch",
			body:      `[{"jsonrpc":"2.0","id":1,"method":"status"},{"jsonrpc":"2.0","id":2,"method":"health"}]`,
			wantBatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			single, batch, err := ParseJsonRpcMessage([]byte(tt.body))

			require.NoError(t, err)
			if tt.wantBatch {
				require.Nil(t, single)
				require.NotNil(t, batch)
				return
			}
			require.NotNil(t, single)
			require.Nil(t, batch)
		})
	}
}

func TestParseJsonRpcMessageKeepsMalformedJSONAsParseError(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "truncated object", body: `{"jsonrpc":`},
		{name: "ambiguous with leading zero", body: `{"jsonrpc":"2.0","method":"status","Method":"other","extension":-01}`},
		{name: "ambiguous with leading decimal point", body: `{"jsonrpc":"2.0","method":"status","Method":"other","extension":-.1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			single, batch, err := ParseJsonRpcMessage([]byte(tt.body))

			require.Error(t, err)
			require.NotErrorIs(t, err, ErrInvalidRequest)
			require.Nil(t, batch)
			require.NotNil(t, single)
		})
	}
}

func TestHandleHTTPRejectsAmbiguousEnvelopeBeforeUpstream(t *testing.T) {
	h := newEnvelopeTestHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"allowed","Method":"unsafe"}`,
	))
	recorder := httptest.NewRecorder()
	upstreamCalls := 0

	h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
		upstreamCalls++
	}, time.Now())

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())
	require.Zero(t, upstreamCalls)
}

func TestHandleHTTPRejectsAmbiguousBatchEnvelopeBeforeUpstream(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "case variant",
			body: `[{"jsonrpc":"2.0","id":1,"method":"allowed","Method":"unsafe"}]`,
		},
		{
			name: "duplicate",
			body: `[{"jsonrpc":"2.0","id":1,"method":"allowed","method":"unsafe"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnvelopeTestHandler(t)
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()
			upstreamCalls := 0

			h.handleHttp(recorder, request, func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls++
				_, _ = w.Write([]byte(`[{"jsonrpc":"2.0","id":1,"result":"ok"}]`))
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())
			require.Zero(t, upstreamCalls)
		})
	}
}

func TestHandleHTTPRejectsNonObjectBatchItemsBeforeUpstream(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "null",
			body: `[null,{"jsonrpc":"2.0","id":1,"method":"allowed"}]`,
		},
		{
			name: "scalar",
			body: `[42,{"jsonrpc":"2.0","id":1,"method":"allowed"}]`,
		},
		{
			name: "array",
			body: `[[{"jsonrpc":"2.0","id":2,"method":"nested"}],{"jsonrpc":"2.0","id":1,"method":"allowed"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnvelopeTestHandler(t)
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()
			upstreamCalls := 0

			h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
				upstreamCalls++
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())
			require.Zero(t, upstreamCalls)
		})
	}
}

func TestValidateJsonRpcEnvelopeKeysDoesNotCopyLargeValues(t *testing.T) {
	largeValue := strings.Repeat("x", 256<<10)
	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "unescaped string",
			body: []byte(`{"jsonrpc":"2.0","id":1,"method":"status","params":"` + largeValue + `"}`),
		},
		{
			name: "escaped string",
			body: []byte(`{"jsonrpc":"2.0","id":1,"method":"status","params":"` + largeValue + `\n"}`),
		},
		{
			name: "nested escaped string",
			body: []byte(`{"jsonrpc":"2.0","id":1,"method":"status","params":{"nested":"` + largeValue + `\n"}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := validateJsonRpcEnvelopeKeys(tt.body); err != nil {
						b.Fatal(err)
					}
				}
			})

			require.Less(t, result.AllocedBytesPerOp(), int64(len(tt.body)/16))
		})
	}
}

func TestHandleHTTPClassifiesValidJSONDecodeFailuresAsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "numeric method", body: `{"jsonrpc":"2.0","id":1,"method":42}`},
		{name: "object id", body: `{"jsonrpc":"2.0","id":{"nested":1},"method":"status"}`},
		{name: "array id", body: `{"jsonrpc":"2.0","id":[1,2],"method":"status"}`},
		{name: "batch object id", body: `[{"jsonrpc":"2.0","id":{"nested":1},"method":"status"}]`},
		{name: "batch array id", body: `[{"jsonrpc":"2.0","id":[1,2],"method":"status"}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			single, batch, err := ParseJsonRpcMessage([]byte(tt.body))
			require.ErrorIs(t, err, ErrInvalidRequest)
			require.Nil(t, single)
			require.Nil(t, batch)

			h := newEnvelopeTestHandler(t)
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()
			upstreamCalls := 0

			h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
				upstreamCalls++
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":null}`, recorder.Body.String())
			require.Zero(t, upstreamCalls)
		})
	}
}

func TestHandleHTTPForwardsUnambiguousRequestByteForByte(t *testing.T) {
	h := newEnvelopeTestHandler(t)
	body := " \n{\"jsonrpc\":\"2.0\", \"id\":1, \"method\":\"status\", \"extension\":true}\n"
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	var forwarded []byte

	h.handleHttp(recorder, request, func(_ http.ResponseWriter, r *http.Request) {
		var err error
		forwarded, err = io.ReadAll(r.Body)
		require.NoError(t, err)
	}, time.Now())

	require.Equal(t, []byte(body), forwarded)
}

func TestHandleHTTPKeepsMalformedJSONAsParseError(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "truncated object", body: `{"jsonrpc":`},
		{name: "trailing JSON", body: `{"jsonrpc":"2.0","id":1,"method":"status"}{}`},
		{name: "ambiguous with leading zero", body: `{"jsonrpc":"2.0","method":"status","Method":"other","extension":-01}`},
		{name: "ambiguous with leading decimal point", body: `{"jsonrpc":"2.0","method":"status","Method":"other","extension":-.1}`},
		{name: "batch with leading zero", body: `[{"jsonrpc":"2.0","id":1,"method":"status","extension":-01}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnvelopeTestHandler(t)
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()

			h.handleHttp(recorder, request, func(http.ResponseWriter, *http.Request) {
				t.Fatal("malformed request reached upstream")
			}, time.Now())

			require.Equal(t, http.StatusOK, recorder.Code)
			require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}`, recorder.Body.String())
		})
	}
}

func TestReceiveMsgPreservesInvalidEnvelopeClassification(t *testing.T) {
	client, peer := newWSCacheClient(t)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte(
		`{"jsonrpc":"2.0","id":1,"method":"allowed","Method":"unsafe"}`,
	)))

	msg, err := client.ReceiveMsg()

	require.Nil(t, msg)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.NotErrorIs(t, err, ErrBadMessage)
	require.Equal(t, -32600, InvalidRequestResponse().Error.Code)
}

func TestReceiveMsgKeepsMalformedJSONAsBadMessage(t *testing.T) {
	client, peer := newWSCacheClient(t)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":`)))

	msg, err := client.ReceiveMsg()

	require.Nil(t, msg)
	require.ErrorIs(t, err, ErrBadMessage)
	require.NotErrorIs(t, err, ErrInvalidRequest)
}

func newEnvelopeTestHandler(t *testing.T) *JsonRpcHandler {
	t.Helper()
	return &JsonRpcHandler{
		log:           log.WithField("test", t.Name()),
		cgDashboard:   newDashboardObservability(),
		defaultAction: RuleActionAllow,
	}
}
