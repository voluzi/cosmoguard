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
					require.EqualValues(t, 1, event.ID)
					require.NotEmpty(t, event.Result)
				}
			}
		})
	}
}
