package cosmoguard

import (
	"net/http"
	"testing"

	"gotest.tools/assert"
)

func TestJsonRpcMsg_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expectedID interface{}
		wantErr    bool
	}{
		{
			name:       "integer ID",
			input:      `{"jsonrpc":"2.0","id":123,"method":"status"}`,
			expectedID: 123,
			wantErr:    false,
		},
		{
			name:       "string ID",
			input:      `{"jsonrpc":"2.0","id":"abc-123","method":"status"}`,
			expectedID: "abc-123",
			wantErr:    false,
		},
		{
			name:       "null ID",
			input:      `{"jsonrpc":"2.0","method":"status"}`,
			expectedID: nil,
			wantErr:    false,
		},
		{
			name:       "zero ID",
			input:      `{"jsonrpc":"2.0","id":0,"method":"status"}`,
			expectedID: 0,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msg JsonRpcMsg
			err := json.Unmarshal([]byte(tt.input), &msg)

			if tt.wantErr {
				assert.Assert(t, err != nil, "expected error but got nil")
				return
			}

			assert.NilError(t, err)
			assert.Equal(t, msg.ID, tt.expectedID)
		})
	}
}

// TestJsonRpcMsg_ResultByteFidelity proves that parsing a response and
// then re-marshalling it preserves the exact byte order of object keys
// inside `result`. The bug this guards against: with `Result interface{}`
// (plain), jsoniter decodes nested objects as map[string]interface{}, then
// remarshals with alphabetically-sorted keys — the cached replay no longer
// matches the upstream's natural order, breaking the project's
// byte-identical Cosmos-compatibility invariant.
func TestJsonRpcMsg_ResultByteFidelity(t *testing.T) {
	// Keys deliberately ordered "z, a, m" — a sorting-aware decoder would
	// emit "a, m, z" on round-trip.
	in := `{"jsonrpc":"2.0","id":1,"result":{"z":1,"a":2,"m":{"y":1,"b":2}}}`

	var msg JsonRpcMsg
	assert.NilError(t, json.Unmarshal([]byte(in), &msg))

	out, err := msg.Marshal()
	assert.NilError(t, err)

	// The slice of `result` in the round-trip output must match the input.
	const want = `"result":{"z":1,"a":2,"m":{"y":1,"b":2}}`
	if !contains(string(out), want) {
		t.Fatalf("result bytes not preserved\n  in:  %s\n  out: %s", in, string(out))
	}
}

// TestJsonRpcMsg_WireSuffixRoundTrip proves Marshal appends the wire
// suffix (typically "\n" from Cosmos/EVM JSON-RPC servers) so cache-hit
// replays are byte-identical to the original upstream payload, not
// just the parsed JSON. Without this, the trailing newline that
// upstreams emit drops on every cache hit and breaks the byte-
// identical compatibility invariant on the wire framing.
func TestJsonRpcMsg_WireSuffixRoundTrip(t *testing.T) {
	msg := &JsonRpcMsg{
		Version:    "2.0",
		ID:         1,
		Result:     []byte(`"0x1af4"`),
		WireSuffix: []byte("\n"),
	}
	out, err := msg.Marshal()
	assert.NilError(t, err)
	want := `{"jsonrpc":"2.0","id":1,"result":"0x1af4"}` + "\n"
	assert.Equal(t, string(out), want)

	// CloneWithID must carry the suffix forward, otherwise the cache's
	// "replace upstream id with client id" rewrite would silently drop
	// the trailing wire bytes on every hit.
	clone := msg.CloneWithID("abc")
	cloneOut, err := clone.Marshal()
	assert.NilError(t, err)
	assert.Equal(t, string(cloneOut), `{"jsonrpc":"2.0","id":"abc","result":"0x1af4"}`+"\n")

	// A nil/empty WireSuffix must produce the bare JSON object — the
	// pre-change behaviour for cosmoguard-generated responses
	// (UnauthorizedResponse, EmptyResult) that never came from an
	// upstream wire payload.
	bare := &JsonRpcMsg{Version: "2.0", ID: 1, Result: []byte(`"0x1af4"`)}
	bareOut, err := bare.Marshal()
	assert.NilError(t, err)
	assert.Equal(t, string(bareOut), `{"jsonrpc":"2.0","id":1,"result":"0x1af4"}`)
}

// TestTrailingWhitespace covers the helper that detects the upstream's
// trailing wire bytes before they get attached to the cached message.
func TestTrailingWhitespace(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"{}\n", "\n"},
		{"{}\r\n", "\r\n"},
		{"{} \t\n", " \t\n"},
		{"{}", ""},
		{"", ""},
	}
	for _, c := range cases {
		got := string(trailingWhitespace([]byte(c.in)))
		assert.Equal(t, got, c.want)
	}
}

// TestJsonRpcMsg_ResultNullRoundTrips proves an explicit `"result":null`
// is preserved verbatim across a parse/marshal round-trip. JSON-RPC 2.0
// requires a successful response to carry either `result` or `error`;
// dropping `result:null` on the wire breaks compliance and breaks
// upstream-byte-identical replays for endpoints like eth_getBlockByHash
// that return null for unknown hashes. IsEmptyResult() must still
// classify it as empty so the CacheEmptyResult gate keeps working.
func TestJsonRpcMsg_ResultNullRoundTrips(t *testing.T) {
	const in = `{"jsonrpc":"2.0","id":1,"result":null}`
	var msg JsonRpcMsg
	assert.NilError(t, json.Unmarshal([]byte(in), &msg))
	assert.Assert(t, msg.Result != nil, "result:null must NOT collapse to nil Result")
	assert.Assert(t, msg.IsEmptyResult(), "result:null must still register as empty")

	out, err := msg.Marshal()
	assert.NilError(t, err)
	if !contains(string(out), `"result":null`) {
		t.Fatalf("result:null lost on round-trip\n  in:  %s\n  out: %s", in, string(out))
	}

	// An absent result field still parses to nil Result (no result
	// key on the wire) and IsEmptyResult agrees.
	var absent JsonRpcMsg
	assert.NilError(t, json.Unmarshal([]byte(`{"jsonrpc":"2.0","id":1}`), &absent))
	assert.Assert(t, absent.Result == nil, "absent result must parse to nil Result")
	assert.Assert(t, absent.IsEmptyResult(), "absent result must register as empty")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestJsonRpcMsg_Marshal(t *testing.T) {
	msg := &JsonRpcMsg{
		Version: "2.0",
		ID:      1,
		Method:  "status",
		Params:  map[string]interface{}{"key": "value"},
	}

	b, err := msg.Marshal()
	assert.NilError(t, err)
	assert.Assert(t, len(b) > 0)

	// Unmarshal back and verify
	var parsed JsonRpcMsg
	err = json.Unmarshal(b, &parsed)
	assert.NilError(t, err)
	assert.Equal(t, parsed.Method, "status")
}

func TestJsonRpcMsg_Clone(t *testing.T) {
	original := &JsonRpcMsg{
		Version: "2.0",
		ID:      1,
		Method:  "status",
		Params:  map[string]interface{}{"key": "value"},
		Result:  []byte(`"success"`),
		Error: &JsonRpcError{
			Code:    100,
			Message: "error",
		},
	}

	clone := original.Clone()

	assert.Equal(t, clone.Version, original.Version)
	assert.Equal(t, clone.ID, original.ID)
	assert.Equal(t, clone.Method, original.Method)
	assert.Equal(t, string(clone.Result), string(original.Result))
	assert.Equal(t, clone.Error.Code, original.Error.Code)

	// Verify it's a different object
	clone.ID = 999
	assert.Assert(t, original.ID != clone.ID)
}

func TestJsonRpcMsg_CloneWithID(t *testing.T) {
	original := &JsonRpcMsg{
		Version: "2.0",
		ID:      1,
		Method:  "status",
	}

	clone := original.CloneWithID("new-id")

	assert.Equal(t, clone.ID, "new-id")
	assert.Equal(t, clone.Method, original.Method)
	assert.Equal(t, original.ID, 1) // Original unchanged
}

func TestJsonRpcError_Clone(t *testing.T) {
	t.Run("clone non-nil error", func(t *testing.T) {
		original := &JsonRpcError{
			Code:    500,
			Message: "internal error",
			Data:    "details",
		}

		clone := original.Clone()

		assert.Equal(t, clone.Code, original.Code)
		assert.Equal(t, clone.Message, original.Message)
		assert.Equal(t, clone.Data, original.Data)
	})

	t.Run("clone nil error", func(t *testing.T) {
		var original *JsonRpcError
		clone := original.Clone()
		assert.Assert(t, clone == nil)
	})
}

func TestParseJsonRpcMessage(t *testing.T) {
	t.Run("parse single message", func(t *testing.T) {
		input := `{"jsonrpc":"2.0","id":1,"method":"status"}`

		single, batch, err := ParseJsonRpcMessage([]byte(input))

		assert.NilError(t, err)
		assert.Assert(t, single != nil)
		assert.Assert(t, batch == nil)
		assert.Equal(t, single.Method, "status")
	})

	t.Run("parse batch message", func(t *testing.T) {
		input := `[{"jsonrpc":"2.0","id":1,"method":"status"},{"jsonrpc":"2.0","id":2,"method":"health"}]`

		single, batch, err := ParseJsonRpcMessage([]byte(input))

		assert.NilError(t, err)
		assert.Assert(t, single == nil)
		assert.Assert(t, batch != nil)
		assert.Equal(t, len(batch), 2)
		assert.Equal(t, batch[0].Method, "status")
		assert.Equal(t, batch[1].Method, "health")
	})

	t.Run("parse empty batch", func(t *testing.T) {
		input := `[]`

		single, batch, err := ParseJsonRpcMessage([]byte(input))

		assert.NilError(t, err)
		assert.Assert(t, single == nil)
		assert.Equal(t, len(batch), 0)
	})
}

func TestUnauthorizedResponse(t *testing.T) {
	req := &JsonRpcMsg{
		Version: "2.0",
		ID:      123,
		Method:  "restricted_method",
	}

	resp := UnauthorizedResponse(req)

	assert.Equal(t, resp.Version, "2.0")
	assert.Equal(t, resp.ID, 123)
	assert.Assert(t, resp.Error != nil)
	assert.Equal(t, resp.Error.Code, http.StatusUnauthorized)
	assert.Equal(t, resp.Error.Message, "unauthorized access")
}

func TestEmptyResult(t *testing.T) {
	req := &JsonRpcMsg{
		Version: "2.0",
		ID:      456,
	}

	resp := EmptyResult(req)

	assert.Equal(t, resp.Version, "2.0")
	assert.Equal(t, resp.ID, 456)
	assert.Assert(t, resp.Result != nil)
}

func TestWithResult(t *testing.T) {
	req := &JsonRpcMsg{
		Version: "2.0",
		ID:      789,
	}

	result := map[string]string{"status": "ok"}
	resp := WithResult(req, result)

	assert.Equal(t, resp.Version, "2.0")
	assert.Equal(t, resp.ID, 789)
	assert.Assert(t, resp.Result != nil)
	// Result is now stored as marshalled bytes (RawMessage); verify the
	// value round-trips back to the original map.
	var parsed map[string]string
	assert.NilError(t, json.Unmarshal(resp.Result, &parsed))
	assert.Equal(t, parsed["status"], "ok")
}

func TestErrorResponse(t *testing.T) {
	req := &JsonRpcMsg{
		Version: "2.0",
		ID:      111,
	}

	resp := ErrorResponse(req, 500, "internal error", "extra data")

	assert.Equal(t, resp.Version, "2.0")
	assert.Equal(t, resp.ID, 111)
	assert.Assert(t, resp.Error != nil)
	assert.Equal(t, resp.Error.Code, 500)
	assert.Equal(t, resp.Error.Message, "internal error")
	assert.Equal(t, resp.Error.Data, "extra data")
}

func TestJsonRpcResponses_Operations(t *testing.T) {
	t.Run("AddPending and GetPendingRequests", func(t *testing.T) {
		responses := &JsonRpcResponses{}

		req1 := &JsonRpcMsg{ID: 1, Method: "method1"}
		req2 := &JsonRpcMsg{ID: 2, Method: "method2"}

		responses.AddPending(req1)
		responses.AddPending(req2)

		pending := responses.GetPendingRequests()
		assert.Equal(t, len(pending), 2)
	})

	t.Run("AddResponse and GetFinal", func(t *testing.T) {
		responses := &JsonRpcResponses{}

		req := &JsonRpcMsg{ID: 1, Method: "method1"}
		resp := &JsonRpcMsg{ID: 1, Result: []byte(`"success"`)}

		responses.AddResponse(req, resp)

		final := responses.GetFinal()
		assert.Equal(t, len(final), 1)
		assert.Equal(t, string(final[0].Result), `"success"`)
	})

	t.Run("Deny", func(t *testing.T) {
		responses := &JsonRpcResponses{}

		req := &JsonRpcMsg{ID: 1, Method: "restricted"}
		responses.Deny(req)

		final := responses.GetFinal()
		assert.Equal(t, len(final), 1)
		assert.Assert(t, final[0].Error != nil)
		assert.Equal(t, final[0].Error.Code, http.StatusUnauthorized)
	})

	t.Run("Find", func(t *testing.T) {
		responses := &JsonRpcResponses{}

		req1 := &JsonRpcMsg{ID: 1, Method: "method1"}
		req2 := &JsonRpcMsg{ID: 2, Method: "method2"}

		responses.AddPending(req1)
		responses.AddPending(req2)

		found := responses.Find(req1)
		assert.Assert(t, found != nil)
		assert.Equal(t, found.Request, req1)

		notFound := responses.Find(&JsonRpcMsg{ID: 999})
		assert.Assert(t, notFound == nil)
	})

	t.Run("Set", func(t *testing.T) {
		responses := &JsonRpcResponses{}

		req1 := &JsonRpcMsg{ID: 1, Method: "method1"}
		req2 := &JsonRpcMsg{ID: 2, Method: "method2"}

		responses.AddPending(req1)
		responses.AddPending(req2)

		resp1 := &JsonRpcMsg{ID: 1, Result: []byte(`"result1"`)}
		resp2 := &JsonRpcMsg{ID: 2, Result: []byte(`"result2"`)}

		responses.Set(JsonRpcMsgs{req1, req2}, JsonRpcMsgs{resp1, resp2})

		final := responses.GetFinal()
		assert.Equal(t, len(final), 2)
	})
}

func TestJsonRpcMsgs_Marshal(t *testing.T) {
	msgs := JsonRpcMsgs{
		{Version: "2.0", ID: 1, Method: "status"},
		{Version: "2.0", ID: 2, Method: "health"},
	}

	b, err := msgs.Marshal()
	assert.NilError(t, err)
	assert.Assert(t, len(b) > 0)

	// Should start with [ for array
	assert.Assert(t, b[0] == '[')
}
