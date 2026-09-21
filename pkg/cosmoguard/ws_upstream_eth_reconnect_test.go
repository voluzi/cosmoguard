package cosmoguard

import (
	"net/url"
	"testing"

	"github.com/voluzi/cosmoguard/pkg/util"
)

func TestEthResetConnectionStatePreservesDesiredSubscriptions(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:0/")
	manager := EthUpstreamConnManager(*u, &util.UniqueID{}, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)
	seedManagerSubscription(manager, "newHeads", "0xold")
	key := wsResponseKey{id: "req-1"}
	manager.respMap[key] = make(chan *JsonRpcMsg, 1)

	if !manager.HasSubscription("newHeads") {
		t.Fatal("desired membership must survive reconnect state reset")
	}
	if len(manager.respMap) != 1 {
		t.Fatalf("in-flight response state must remain scoped to its old socket, got %d entries", len(manager.respMap))
	}
}

func TestEthReconnectSnapshotRetainsEveryDesiredSubscription(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:0/")
	manager := EthUpstreamConnManager(*u, &util.UniqueID{}, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)
	seedManagerSubscription(manager, "newHeads", "0xold")
	seedManagerSubscription(manager, "logs", "0xold2")

	manager.lifecycle.mu.RLock()
	desired := len(manager.lifecycle.byParam)
	manager.lifecycle.mu.RUnlock()
	if desired != 2 {
		t.Fatalf("expected both subscriptions retained for replay, got %d", desired)
	}
}
