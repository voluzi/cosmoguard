package cosmoguard

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWSForwardingFailureRepliesWithRPCError(t *testing.T) {
	coalesce := true
	cases := []struct {
		name  string
		setup func(*JsonRpcWebSocketProxy, *JsonRpcRule)
	}{
		{"default allow", func(p *JsonRpcWebSocketProxy, _ *JsonRpcRule) {
			p.SetRules(nil, RuleActionAllow, nil)
		}},
		{"rule allow", func(p *JsonRpcWebSocketProxy, rule *JsonRpcRule) {
			rule.Cache = nil
			p.SetRules([]*JsonRpcRule{rule}, RuleActionDeny, nil)
		}},
		{"coalesced cache miss", func(*JsonRpcWebSocketProxy, *JsonRpcRule) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy, rule, upstream := newWSCacheProxy(t, &coalesce, 0)
			tc.setup(proxy, rule)
			var failing atomic.Bool
			failing.Store(true)
			upstream.makeResponse = func(request *JsonRpcMsg, _ int32) (*JsonRpcMsg, error) {
				if failing.Load() {
					return nil, errors.New("dial ws://10.0.0.7:26657: connection refused")
				}
				return WithResult(request, "ok"), nil
			}

			server := httptest.NewServer(http.HandlerFunc(proxy.HandleConnection))
			t.Cleanup(server.Close)
			peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Close() })

			// The notification must get no reply, so the first frame read is
			// the answer to id 7.
			require.NoError(t, peer.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "status"}))
			require.NoError(t, peer.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 7, "method": "status"}))
			require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
			var reply JsonRpcMsg
			require.NoError(t, peer.ReadJSON(&reply))
			require.EqualValues(t, 7, reply.ID)
			require.NotNil(t, reply.Error)
			require.Equal(t, -32603, reply.Error.Code)
			require.NotContains(t, reply.Error.Message, "10.0.0.7")

			failing.Store(false)
			require.NoError(t, peer.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 8, "method": "status"}))
			reply = JsonRpcMsg{}
			require.NoError(t, peer.ReadJSON(&reply))
			require.EqualValues(t, 8, reply.ID)
			require.Nil(t, reply.Error)
		})
	}
}

func TestWSUndeliverableForwardErrorClosesClient(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 0)
	proxy.SetRules(nil, RuleActionAllow, nil)
	upstream.makeResponse = func(*JsonRpcMsg, int32) (*JsonRpcMsg, error) {
		return nil, errors.New("upstream unavailable")
	}
	server := httptest.NewServer(http.HandlerFunc(proxy.HandleConnection))
	t.Cleanup(server.Close)
	peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	var client *JsonRpcWsClient
	require.Eventually(t, func() bool {
		proxy.connsMu.Lock()
		defer proxy.connsMu.Unlock()
		for c := range proxy.conns {
			client = c
		}
		return client != nil
	}, 2*time.Second, 10*time.Millisecond)
	tcp, ok := client.conn.NetConn().(*net.TCPConn)
	require.True(t, ok)
	// Reads still work, every write fails.
	require.NoError(t, tcp.CloseWrite())

	require.NoError(t, peer.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "status"}))
	require.Eventually(t, client.IsClosed, 2*time.Second, 10*time.Millisecond)
}
