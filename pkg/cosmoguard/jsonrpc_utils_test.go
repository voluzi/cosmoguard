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
		Result:  "success",
		Error: &JsonRpcError{
			Code:    100,
			Message: "error",
		},
	}

	clone := original.Clone()

	assert.Equal(t, clone.Version, original.Version)
	assert.Equal(t, clone.ID, original.ID)
	assert.Equal(t, clone.Method, original.Method)
	assert.Equal(t, clone.Result, original.Result)
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

func TestJsonRpcMsg_Hash(t *testing.T) {
	msg1 := &JsonRpcMsg{
		Method: "status",
		Params: map[string]interface{}{"key": "value"},
	}

	msg2 := &JsonRpcMsg{
		Method: "status",
		Params: map[string]interface{}{"key": "value"},
	}

	msg3 := &JsonRpcMsg{
		Method: "status",
		Params: map[string]interface{}{"key": "different"},
	}

	// Same content should produce same hash
	assert.Equal(t, msg1.Hash(), msg2.Hash())

	// Different content should produce different hash
	assert.Assert(t, msg1.Hash() != msg3.Hash())
}

func TestJsonRpcMsg_MaybeGetPath(t *testing.T) {
	tests := []struct {
		name     string
		msg      *JsonRpcMsg
		expected string
	}{
		{
			name: "with path in params",
			msg: &JsonRpcMsg{
				Params: map[string]interface{}{
					"path": "/cosmos.bank.v1beta1.Query/AllBalances",
				},
			},
			expected: "/cosmos.bank.v1beta1.Query/AllBalances",
		},
		{
			name: "without path in params",
			msg: &JsonRpcMsg{
				Params: map[string]interface{}{
					"other": "value",
				},
			},
			expected: "",
		},
		{
			name: "params is not a map",
			msg: &JsonRpcMsg{
				Params: []interface{}{"a", "b"},
			},
			expected: "",
		},
		{
			name: "nil params",
			msg: &JsonRpcMsg{
				Params: nil,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.msg.MaybeGetPath()
			assert.Equal(t, result, tt.expected)
		})
	}
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
	// Check the map content
	resultMap := resp.Result.(map[string]string)
	assert.Equal(t, resultMap["status"], "ok")
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
		resp := &JsonRpcMsg{ID: 1, Result: "success"}

		responses.AddResponse(req, resp)

		final := responses.GetFinal()
		assert.Equal(t, len(final), 1)
		assert.Equal(t, final[0].Result, "success")
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

		resp1 := &JsonRpcMsg{ID: 1, Result: "result1"}
		resp2 := &JsonRpcMsg{ID: 2, Result: "result2"}

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
