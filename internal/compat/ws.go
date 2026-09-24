package compat

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// wsWindow bounds how long a subscription comparison waits for both sides
// to deliver an event for the same block.
const wsWindow = 45 * time.Second

// wsSub describes one subscription and how to read the block key and the
// comparable payload out of its event frames.
type wsSub struct {
	name      string
	path      string
	subscribe map[string]any
	// event returns the block key and payload of an event frame, and false
	// for anything else (acks, keepalives).
	event func(frame map[string]any) (string, any, bool)
}

var cometNewBlock = wsSub{
	name:      "subscribe tm.event='NewBlock'",
	path:      "/websocket",
	subscribe: map[string]any{"jsonrpc": "2.0", "id": 1, "method": "subscribe", "params": map[string]any{"query": "tm.event='NewBlock'"}},
	event: func(f map[string]any) (string, any, bool) {
		result, _ := f["result"].(map[string]any)
		data, _ := result["data"].(map[string]any)
		value, _ := data["value"].(map[string]any)
		block, _ := value["block"].(map[string]any)
		header, _ := block["header"].(map[string]any)
		height, ok := header["height"].(string)
		if !ok {
			return "", nil, false
		}
		return height, data, true
	},
}

var evmNewHeads = wsSub{
	name:      "eth_subscribe newHeads",
	subscribe: map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": []any{"newHeads"}},
	event: func(f map[string]any) (string, any, bool) {
		if f["method"] != "eth_subscription" {
			return "", nil, false
		}
		params, _ := f["params"].(map[string]any)
		head, _ := params["result"].(map[string]any)
		num, ok := head["number"].(string)
		if !ok {
			return "", nil, false
		}
		return num, head, true
	},
}

func wsCompare(ctx context.Context, o Options) []Result {
	results := []Result{compareSub(ctx, cometNewBlock, wsURL(o.Node.RPC), wsURL(o.Guard.RPC))}
	switch {
	case o.Node.EVMWS != "" && o.Guard.EVMWS != "":
		results = append(results, compareSub(ctx, evmNewHeads, wsURL(o.Node.EVMWS), wsURL(o.Guard.EVMWS)))
	case o.Node.EVM != "":
		results = append(results, Result{Protocol: ProtoWS, Name: evmNewHeads.name, Class: Skipped, Detail: "no EVM WebSocket URL for both sides"})
	}
	return results
}

func wsURL(base string) string {
	switch {
	case strings.HasPrefix(base, "https://"):
		return "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		return "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base
}

type wsEvent struct {
	key     string
	payload any
	// err is set when the server answered the subscribe with an error.
	err string
}

// compareSub subscribes on both sides and compares the first block both
// deliver. Subscription ids are not compared, since cosmoguard may assign
// its own.
func compareSub(ctx context.Context, s wsSub, node, guard string) Result {
	res := Result{Protocol: ProtoWS, Name: s.name}
	ctx, cancel := context.WithTimeout(ctx, wsWindow)
	defer cancel()

	nodeCh, err := subscribe(ctx, node+s.path, s)
	if err != nil {
		res.Class, res.Detail = Failed, "node: "+err.Error()
		return res
	}
	guardCh, err := subscribe(ctx, guard+s.path, s)
	if err != nil {
		res.Class, res.Detail = Differs, "cosmoguard: "+err.Error()
		return res
	}
	seen := [2]map[string]any{{}, {}}
	for {
		var ev wsEvent
		var side int
		var ok bool
		select {
		case <-ctx.Done():
			res.Class = Unstable
			if len(seen[0]) > 0 && len(seen[1]) == 0 {
				res.Class = Differs
			}
			res.Detail = fmt.Sprintf("no block delivered by both within %s (node %d events, cosmoguard %d)", wsWindow, len(seen[0]), len(seen[1]))
			return res
		case ev, ok = <-nodeCh:
			side = 0
		case ev, ok = <-guardCh:
			side = 1
		}
		if !ok && ctx.Err() != nil {
			// The window closed the connections; report the timeout
			// on the next turn rather than a closed subscription.
			if side == 0 {
				nodeCh = nil
			} else {
				guardCh = nil
			}
			continue
		}
		if !ok || ev.err != "" {
			why := "closed the subscription"
			if ev.err != "" {
				why = "refused the subscription: " + ev.err
			}
			if side == 0 {
				res.Class, res.Detail = Failed, "node "+why
			} else {
				res.Class, res.Detail = Differs, "cosmoguard "+why
			}
			return res
		}
		seen[side][ev.key] = ev.payload
		other, both := seen[1-side][ev.key]
		if !both {
			continue
		}
		a, b := ev.payload, other
		if side == 1 {
			a, b = other, ev.payload
		}
		if d := jsonDiff("$", a, b); d != "" {
			res.Class, res.Detail = Differs, "block "+ev.key+": "+d
			return res
		}
		res.Class, res.Detail = Identical, "block "+ev.key
		return res
	}
}

// subscribe dials url, sends the subscription and streams its events
// until ctx ends or the connection closes.
func subscribe(ctx context.Context, url string, s wsSub) (<-chan wsEvent, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	if err := conn.WriteJSON(s.subscribe); err != nil {
		conn.Close()
		return nil, err
	}
	ch := make(chan wsEvent, 16)
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	go func() {
		defer close(ch)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			v, ok := decodeJSON(msg)
			if !ok {
				continue
			}
			frame, _ := v.(map[string]any)
			ev := wsEvent{}
			if e, isErr := frame["error"].(map[string]any); isErr {
				ev.err = fmt.Sprint(e["message"])
			} else if ev.key, ev.payload, ok = s.event(frame); !ok {
				continue
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
