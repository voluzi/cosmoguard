package cosmoguard

import (
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/pkg/util"
)

// The upstream writes the first event right behind the subscribe
// acknowledgement. The client must get the acknowledgement, then the event.
func TestWSFirstEventAfterSubscribeIsDelivered(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			for range 20 {
				upClient, upPeer := newWSCacheClient(t)
				var manager UpstreamConnManager
				proxy, err := NewJsonRpcWebSocketProxy(t.Name(), []string{"ws://upstream.test"}, "/websocket", 1,
					func(_ url.URL, ids *util.UniqueID, onMessage func(*JsonRpcMsg)) UpstreamConnManager {
						manager = protocol.constructor(url.URL{}, ids, onMessage)
						return manager
					}, nil, false, nil)
				require.NoError(t, err)
				proxy.log = log.WithField("test", t.Name())
				proxy.broker.log = proxy.log
				proxy.cgDashboard = newDashboardObservability()
				proxy.SetRules(nil, RuleActionAllow, nil)
				setManagerClient(manager, upClient)
				setManagerLog(manager, proxy.log)
				go func() {
					for {
						msg, err := upClient.ReceiveMsg()
						if err != nil {
							return
						}
						dispatchManagerMessageFromClient(manager, upClient, msg)
					}
				}()
				go func() {
					var req JsonRpcMsg
					if err := upPeer.ReadJSON(&req); err != nil {
						return
					}
					ack, _ := protocol.subscribeSuccess(&req, "x").Marshal()
					event := []byte(`{"jsonrpc":"2.0","id":"` + req.ID.(string) + `","result":{"query":"q","data":{}}}`)
					if protocol.evm {
						event = []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"subscription-x","result":{}}}`)
					}
					_ = upPeer.WriteMessage(websocket.TextMessage, ack)
					_ = upPeer.WriteMessage(websocket.TextMessage, event)
				}()

				client, peer := newWSCacheClient(t)
				method := methodSubscribeCosmos
				if protocol.evm {
					method = methodSubscribeEth
				}
				require.NoError(t, proxy.handleRequest(client, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: 1, Method: method, Params: []any{"q"},
				}, "192.0.2.1", nil))

				require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
				var ack, event JsonRpcMsg
				require.NoError(t, peer.ReadJSON(&ack))
				require.EqualValues(t, 1, ack.ID, "acknowledgement must come first")
				require.Nil(t, ack.Error)
				require.NoError(t, peer.ReadJSON(&event), "first event was dropped")
				if protocol.evm {
					var handle string
					require.NoError(t, json.Unmarshal(ack.Result, &handle))
					require.Equal(t, "eth_subscription", event.Method)
					require.Equal(t, handle, event.Params.(map[string]any)["subscription"])
				} else {
					require.JSONEq(t, `{}`, string(ack.Result), "acknowledgement must come first")
					require.EqualValues(t, 1, event.ID)
					var result map[string]any
					require.NoError(t, json.Unmarshal(event.Result, &result))
					require.Equal(t, "q", result["query"])
				}
			}
		})
	}
}

func TestBrokerHoldsJoinedClientEventsUntilAcknowledged(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	first, _ := newWSCacheClient(t)
	joiner, joinerPeer := newWSCacheClient(t)
	_, err := broker.HandleSubscription(first, &JsonRpcMsg{Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"}})
	require.NoError(t, err)
	id, _ := broker.sm.GetSubscriptionID("q")

	res, delivered, err := broker.handleSubscription(joiner, &JsonRpcMsg{Version: jsonRpcVersion, ID: 2, Method: methodSubscribeCosmos, Params: []any{"q"}})
	require.NoError(t, err)
	broker.onSubscriptionMessage(&JsonRpcMsg{Version: jsonRpcVersion, ID: id, Result: []byte(`{"query":"q"}`)})
	require.NoError(t, joinerPeer.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err = joinerPeer.ReadMessage()
	require.Error(t, err, "event reached the joining client before its acknowledgement")

	joiner2, joiner2Peer := newWSCacheClient(t)
	res, delivered, err = broker.handleSubscription(joiner2, &JsonRpcMsg{Version: jsonRpcVersion, ID: 3, Method: methodSubscribeCosmos, Params: []any{"q"}})
	require.NoError(t, err)
	broker.onSubscriptionMessage(&JsonRpcMsg{Version: jsonRpcVersion, ID: id, Result: []byte(`{"query":"q"}`)})
	require.NoError(t, joiner2.SendMsg(res))
	delivered()
	require.NoError(t, joiner2Peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	var ack, event JsonRpcMsg
	require.NoError(t, joiner2Peer.ReadJSON(&ack))
	require.JSONEq(t, `{}`, string(ack.Result))
	require.NoError(t, joiner2Peer.ReadJSON(&event))
	require.EqualValues(t, 3, event.ID)
}

func TestBrokerHeldEventsOverflowClosesClient(t *testing.T) {
	upstream := newLimitingUpstream()
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
	broker.log = log.WithField("test", t.Name())
	client, _ := newWSCacheClient(t)
	_, _, err := broker.handleSubscription(client, &JsonRpcMsg{Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"q"}})
	require.NoError(t, err)
	id, _ := broker.sm.GetSubscriptionID("q")
	for range wsNotificationQueueMessages + 1 {
		broker.onSubscriptionMessage(&JsonRpcMsg{Version: jsonRpcVersion, ID: id, Result: []byte(`{"query":"q"}`)})
	}
	require.Eventually(t, client.IsClosed, 2*time.Second, 5*time.Millisecond)
}

// A subscribe the upstream rejects after the broker registered it must
// leave nothing behind.
func TestBrokerFailedCreatedSubscribeUnwinds(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			broker, _, backend := newRealManagerBroker(t, protocol, WebSocketLimits{MaxSubscriptionsPerClient: 1})
			client, _ := newWSCacheClient(t)
			method := methodSubscribeCosmos
			if protocol.evm {
				method = methodSubscribeEth
			}
			call := startBrokerCall(broker, client, &JsonRpcMsg{Version: jsonRpcVersion, ID: 1, Method: method, Params: []any{"q"}}, "alice")
			request := mustRecv(t, backend.requests, "upstream subscribe")
			// Registered before the subscribe went out.
			require.Equal(t, 1, broker.ClientSubCount(client))
			backend.responses <- providerRejectedResponse(request)
			result := mustRecv(t, call, "subscribe")
			require.NoError(t, result.err)
			require.NotNil(t, result.response.Error)

			require.Equal(t, 0, broker.ClientSubCount(client))
			_, exists := broker.sm.GetSubscriptionID("q")
			require.False(t, exists)
			broker.membershipMu.Lock()
			require.Empty(t, broker.requests)
			require.Empty(t, broker.pending)
			broker.membershipMu.Unlock()
			require.NoError(t, broker.admission.reserveSubscription(client, "alice"), "admission must be released")
		})
	}
}

func TestCosmosLateSubscribeAcknowledgementIsNotAnEvent(t *testing.T) {
	var forwarded []*JsonRpcMsg
	upClient, _ := newWSCacheClient(t)
	manager := CosmosUpstreamConnManager(url.URL{}, &util.UniqueID{}, func(msg *JsonRpcMsg) { forwarded = append(forwarded, msg) }).(*UpstreamConnManagerCosmos)
	manager.log = log.WithField("test", t.Name())
	setManagerClient(manager, upClient)
	lifecycle := manager.subscriptionLifecycle()
	lifecycle.mu.Lock()
	record := &wsSubscriptionRecord{param: "q", handle: "7", state: wsSubscriptionCreating, desired: true, settled: make(chan struct{})}
	lifecycle.byParam["q"] = record
	lifecycle.bindEarlyLocked(record, upClient, "7")
	lifecycle.mu.Unlock()

	manager.onUpstreamMessage(upClient, &JsonRpcMsg{Version: jsonRpcVersion, ID: "7", Result: []byte(` { } `)})
	require.Empty(t, forwarded)
	manager.onUpstreamMessage(upClient, &JsonRpcMsg{Version: jsonRpcVersion, ID: "7", Result: []byte(`{"query":"q"}`)})
	require.Len(t, forwarded, 1)
}
