package cosmoguard

import (
	"strings"
	"testing"
)

// TestJsonRpcMsgCacheCost_ChargesIdAndStructuredError ensures the L1 byte
// budget accounts for caller-controlled interface{} fields (issue #15): a
// large string ID and a large structured error.data must be charged by their
// real size, not a tiny fixed amount, or a client could grow the cache past
// cache.memory.maxBytes while being under-charged.
func TestJsonRpcMsgCacheCost_ChargesIdAndStructuredError(t *testing.T) {
	bigID := strings.Repeat("x", 4096)
	small := &JsonRpcMsg{Method: "status"}
	withBigID := &JsonRpcMsg{Method: "status", ID: bigID}
	if withBigID.CacheCost() < small.CacheCost()+4000 {
		t.Fatalf("large string ID not charged: small=%d withBigID=%d", small.CacheCost(), withBigID.CacheCost())
	}

	// Structured error.data (decoded map/slice) must be measured, not charged
	// the flat scalar amount.
	bigData := map[string]any{"trace": strings.Repeat("y", 4096)}
	withStructuredErr := &JsonRpcMsg{
		Method: "broadcast_tx",
		Error:  &JsonRpcError{Code: -1, Message: "failed", Data: bigData},
	}
	if withStructuredErr.CacheCost() < 4000 {
		t.Fatalf("structured error.data not charged by size: cost=%d", withStructuredErr.CacheCost())
	}

	// Scalar values stay cheap (no marshal blow-up).
	scalarID := &JsonRpcMsg{Method: "status", ID: 12345}
	if scalarID.CacheCost() > small.CacheCost()+64 {
		t.Fatalf("scalar ID over-charged: %d", scalarID.CacheCost())
	}
}
