package compat

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"gotest.tools/assert"
)

// fakeWS acks a subscribe (or refuses it) and then sends one NewBlock
// event per height, with the given proposer so payloads can differ.
func fakeWS(t *testing.T, refuse bool, proposer string, heights ...int) string {
	t.Helper()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
		if refuse {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":401,"message":"unauthorized access"}}`))
			return
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
		for _, h := range heights {
			ev := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{"block":{"header":{"height":"%d","proposer_address":"%s"}}}}}}`, h, proposer)
			if c.WriteMessage(websocket.TextMessage, []byte(ev)) != nil {
				return
			}
		}
		_, _, _ = c.ReadMessage() // hold the connection open until the client leaves
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestCompareSub(t *testing.T) {
	sub := cometNewBlock
	sub.path = ""
	ctx := context.Background()

	t.Run("same block, same payload", func(t *testing.T) {
		// The sides start at different heights; the common one is compared.
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10, 11, 12), fakeWS(t, false, "P", 11, 12))
		assert.Equal(t, res.Class, Identical, res.Detail)
		assert.Equal(t, res.Detail, "block 11")
	})
	t.Run("same block, different payload", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10), fakeWS(t, false, "Q", 10))
		assert.Equal(t, res.Class, Differs)
		assert.Assert(t, strings.Contains(res.Detail, "proposer_address"), res.Detail)
	})
	t.Run("node refuses", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, true, "P"), fakeWS(t, false, "P", 10))
		assert.Equal(t, res.Class, Failed)
		assert.Assert(t, strings.Contains(res.Detail, "unauthorized access"), res.Detail)
	})
	t.Run("cosmoguard refuses", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10), fakeWS(t, true, "P"))
		assert.Equal(t, res.Class, Differs)
		assert.Assert(t, strings.Contains(res.Detail, "cosmoguard refused"), res.Detail)
	})
}

func TestEvmNewHeadsEvent(t *testing.T) {
	key, payload, ok := evmNewHeads.event(map[string]any{
		"method": "eth_subscription",
		"params": map[string]any{"subscription": "0x1", "result": map[string]any{"number": "0xa"}},
	})
	assert.Assert(t, ok)
	assert.Equal(t, key, "0xa")
	assert.DeepEqual(t, payload, map[string]any{"number": "0xa"})
	_, _, ok = evmNewHeads.event(map[string]any{"id": 1, "result": "0x1"})
	assert.Assert(t, !ok, "the subscribe ack is not an event")
}
