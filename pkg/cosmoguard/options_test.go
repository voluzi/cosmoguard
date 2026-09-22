package cosmoguard

import "testing"

func TestDefaultJsonRpcHandlerOptionsWebsocketConnections(t *testing.T) {
	if got := DefaultJsonRpcHandlerOptions().WebsocketConnections; got != 40 {
		t.Fatalf("WebsocketConnections = %d, want 40", got)
	}
}
