package compat

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10, 11, 12), fakeWS(t, false, "P", 11, 12), 5*time.Second)
		assert.Equal(t, res.Class, Identical, res.Detail)
		assert.Equal(t, res.Detail, "block 11")
	})
	t.Run("same block, different payload", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10), fakeWS(t, false, "Q", 10), 5*time.Second)
		assert.Equal(t, res.Class, Differs)
		assert.Assert(t, strings.Contains(res.Detail, "proposer_address"), res.Detail)
	})
	t.Run("node refuses", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, true, "P"), fakeWS(t, false, "P", 10), 5*time.Second)
		assert.Equal(t, res.Class, Failed)
		assert.Assert(t, strings.Contains(res.Detail, "unauthorized access"), res.Detail)
	})
	t.Run("cosmoguard refuses", func(t *testing.T) {
		res := compareSub(ctx, sub, fakeWS(t, false, "P", 10), fakeWS(t, true, "P"), 5*time.Second)
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

func TestWsCompareSkipsEVMWithoutWebSocket(t *testing.T) {
	node := fakeWS(t, false, "P", 10)
	o := Options{
		Node:  Endpoints{RPC: strings.Replace(node, "ws", "http", 1), EVM: "http://evm"},
		Guard: Endpoints{RPC: strings.Replace(node, "ws", "http", 1), EVM: "http://evm"},
	}
	res := wsCompare(t.Context(), o)
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[1].Class, Skipped)
	assert.Equal(t, res[1].Name, evmNewHeads.name)
	assert.Equal(t, res[1].Detail, noEVM)

	// A chain without EVM reports the subscription as skipped too.
	o.Node.EVM, o.Guard.EVM = "", ""
	res = wsCompare(t.Context(), o)
	assert.Equal(t, res[1].Class, Skipped)
}

// fakeWSFrames acks a subscribe, then sends frames verbatim and holds the
// connection open.
func fakeWSFrames(t *testing.T, frames ...string) string {
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
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
		for _, f := range frames {
			if c.WriteMessage(websocket.TextMessage, []byte(f)) != nil {
				return
			}
		}
		_, _, _ = c.ReadMessage()
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func newBlockFrame(height int, events string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{"block":{"header":{"height":"%d"}}}},"events":%s}}`, height, events)
}

func TestCompareSubComparesEventAttributes(t *testing.T) {
	sub := cometNewBlock
	sub.path = ""
	node := fakeWSFrames(t, newBlockFrame(10, `{"tm.event":["NewBlock"],"mint.amount":["5"]}`))
	guard := fakeWSFrames(t, newBlockFrame(10, `{"tm.event":["NewBlock"]}`))
	res := compareSub(t.Context(), sub, node, guard, 5*time.Second)
	assert.Equal(t, res.Class, Differs, res.Detail)
	assert.Assert(t, strings.Contains(res.Detail, "events"), res.Detail)
}

func TestCompareSubInterrupted(t *testing.T) {
	sub := cometNewBlock
	sub.path = ""
	// The node delivers a block, cosmoguard not yet, when the run stops.
	node := fakeWSFrames(t, newBlockFrame(10, `{}`))
	guard := fakeWSFrames(t)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	res := compareSub(ctx, sub, node, guard, 5*time.Second)
	assert.Equal(t, res.Class, Unstable, res.Detail)
	assert.Equal(t, res.Detail, "interrupted before a verdict")
}

func TestCompareSubHandshakeTimeout(t *testing.T) {
	// A server that accepts the connection but never completes the
	// WebSocket handshake.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	start := time.Now()
	res := compareSub(t.Context(), cometNewBlock, "ws://"+l.Addr().String(), "ws://"+l.Addr().String(), 100*time.Millisecond)
	assert.Equal(t, res.Class, Failed, res.Detail)
	assert.Assert(t, time.Since(start) < 5*time.Second, "--timeout bounds the handshake, not the %s event window", wsWindow)
}
