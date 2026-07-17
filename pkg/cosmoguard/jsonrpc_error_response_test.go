package cosmoguard

import (
	"strings"
	"testing"
)

// TestErrorResponsesEmitNullID is the cubic #18 regression: parse/invalid-
// request errors must serialize `"id":null` (JSON-RPC 2.0 §5.1), not drop the
// id field via omitempty.
func TestErrorResponsesEmitNullID(t *testing.T) {
	pe, err := ParseErrorResponse().Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pe), `"id":null`) {
		t.Fatalf("parse error must carry \"id\":null, got %s", pe)
	}
	if !strings.Contains(string(pe), `-32700`) {
		t.Fatalf("parse error must be -32700, got %s", pe)
	}

	ir, err := InvalidRequestResponse().Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ir), `"id":null`) {
		t.Fatalf("invalid request must carry \"id\":null, got %s", ir)
	}
	if !strings.Contains(string(ir), `-32600`) {
		t.Fatalf("invalid request must be -32600, got %s", ir)
	}

	// A normal notification response path must still omit a nil id (the
	// explicit-null sentinel is only used by the error builders).
	n, _ := (&JsonRpcMsg{Version: "2.0", Method: "x"}).Marshal()
	if strings.Contains(string(n), `"id"`) {
		t.Fatalf("a bare nil id should still be omitted, got %s", n)
	}
}
