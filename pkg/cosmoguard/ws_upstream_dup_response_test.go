package cosmoguard

import (
	"net/url"
	"testing"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// TestOnUpstreamMessage_DuplicateResponseNoPanic asserts that a second
// upstream response frame carrying the same JSON-RPC id does NOT panic with
// "send on closed channel". A misbehaving/malicious upstream can emit
// duplicate responses; before the fix the second frame re-loaded the closed
// response channel and crashed the whole process (the Run goroutine has no
// recover of its own).
func TestOnUpstreamMessage_DuplicateResponseNoPanic(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:0/")
	idGen := &util.UniqueID{}

	t.Run("eth", func(t *testing.T) {
		mgr := EthUpstreamConnManager(*u, idGen, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)
		mgr.log = log.WithField("test", "eth-dup")

		respChan := make(chan *JsonRpcMsg, 1)
		mgr.respMap["42"] = respChan

		// First response: delivered + channel closed + entry removed.
		mgr.onUpstreamMessage(&JsonRpcMsg{Version: "2.0", ID: "42"})
		if _, ok := <-respChan; !ok {
			t.Fatal("expected first response on the channel")
		}
		// Second, duplicate response with the same id must NOT panic.
		mgr.onUpstreamMessage(&JsonRpcMsg{Version: "2.0", ID: "42"})
	})

	t.Run("cosmos", func(t *testing.T) {
		mgr := CosmosUpstreamConnManager(*u, idGen, func(*JsonRpcMsg) {}).(*UpstreamConnManagerCosmos)
		mgr.log = log.WithField("test", "cosmos-dup")

		respChan := make(chan *JsonRpcMsg, 1)
		mgr.respMap["42"] = respChan

		mgr.onUpstreamMessage(&JsonRpcMsg{Version: "2.0", ID: "42"})
		if _, ok := <-respChan; !ok {
			t.Fatal("expected first response on the channel")
		}
		mgr.onUpstreamMessage(&JsonRpcMsg{Version: "2.0", ID: "42"})
	})
}
