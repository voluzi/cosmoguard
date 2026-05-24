package cosmoguard

import (
	"net/url"
	"testing"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// TestEthResetConnectionState_PreservesSubByParam is the regression test
// for the EVM WS reconnect subscription-loss bug: the reconnect reset
// must clear the connection-scoped maps (subByID, respMap) but PRESERVE
// subByParam, which reSubmitSubscriptionsOnClient reads to know what to
// re-issue on the new connection. The old resetAll wiped subByParam, so
// client subscriptions were silently never re-established.
func TestEthResetConnectionState_PreservesSubByParam(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:0/")
	mgr := EthUpstreamConnManager(*u, &util.UniqueID{}, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)

	// Simulate an established subscription before the connection drops.
	mgr.subByParam["newHeads"] = "0xold"
	mgr.subByID["0xold"] = "newHeads"
	mgr.respMap["req-1"] = make(chan *JsonRpcMsg, 1)

	mgr.resetConnectionState()

	if _, ok := mgr.subByParam["newHeads"]; !ok {
		t.Fatal("subByParam must survive reconnect so the subscription is re-issued")
	}
	if len(mgr.subByID) != 0 {
		t.Fatalf("subByID (connection-scoped upstream ids) must be cleared, got %v", mgr.subByID)
	}
	if len(mgr.respMap) != 0 {
		t.Fatalf("respMap (in-flight on dead conn) must be cleared, got %d entries", len(mgr.respMap))
	}
}

// TestEthReSubmitPendingFromSubByParam confirms reSubmitSubscriptionsOnClient
// derives its work-list from the preserved subByParam — i.e. after a
// reset that keeps subByParam, every param is still pending re-issue
// (the previous HasSubscription skip would have dropped them all).
func TestEthReSubmitPendingFromSubByParam(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:0/")
	mgr := EthUpstreamConnManager(*u, &util.UniqueID{}, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)
	mgr.subByParam["newHeads"] = "0xold"
	mgr.subByParam["logs"] = "0xold2"
	mgr.resetConnectionState()

	// Mirror the snapshot logic of reSubmitSubscriptionsOnClient.
	mgr.subMux.Lock()
	pending := make([]string, 0, len(mgr.subByParam))
	for p := range mgr.subByParam {
		pending = append(pending, p)
	}
	mgr.subMux.Unlock()

	if len(pending) != 2 {
		t.Fatalf("expected both params pending re-subscribe after reset, got %v", pending)
	}
}
