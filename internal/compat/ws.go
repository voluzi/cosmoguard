package compat

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// wsWindow bounds how long a subscription comparison waits for both sides
// to deliver an event for the same block. Tests shorten it.
var wsWindow = 45 * time.Second

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
		// The whole result: data plus the query and event attributes
		// clients filter on.
		return height, result, true
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
	results := []Result{compareSub(ctx, cometNewBlock, wsURL(o.Node.RPC), wsURL(o.Guard.RPC), o.Timeout)}
	if o.Node.EVMWS != "" && o.Guard.EVMWS != "" {
		results = append(results, compareSub(ctx, evmNewHeads, wsURL(o.Node.EVMWS), wsURL(o.Guard.EVMWS), o.Timeout))
	} else {
		results = append(results, Result{Protocol: ProtoWS, Name: evmNewHeads.name, Class: Skipped, Detail: noEVM})
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
	// refusal is the JSON-RPC error object the server answered the
	// subscribe with.
	refusal map[string]any
	// ack marks the server's acceptance of the subscribe.
	ack bool
}

// compareSub subscribes on both sides and compares the first block both
// deliver. Subscription ids are not compared, since cosmoguard may assign
// its own. timeout bounds each handshake; wsWindow bounds the wait for a
// common block.
func compareSub(ctx context.Context, s wsSub, node, guard string, timeout time.Duration) Result {
	res := Result{Protocol: ProtoWS, Name: s.name}
	run := ctx
	ctx, cancel := context.WithTimeout(ctx, wsWindow)
	defer cancel()

	nodeCh, err := subscribe(ctx, node+s.path, s, timeout)
	if err != nil {
		res.Class, res.Detail = Failed, "node: "+err.Error()
		return res
	}
	guardCh, err := subscribe(ctx, guard+s.path, s, timeout)
	if err != nil {
		res.Class, res.Detail = Differs, "cosmoguard: "+err.Error()
		if ctx.Err() != nil {
			res.Class, res.Detail = Unstable, "interrupted before a verdict"
		}
		return res
	}
	seen := [2]map[string]any{{}, {}}
	// A side has answered the subscribe once it acked, sent an event or
	// refused. After a refusal the other side's answer is awaited, so the
	// verdict does not depend on which arrives first.
	var refused [2]map[string]any
	var accepted [2]bool
	// A matching block is only a verdict once both sides also returned
	// the subscribe's own response (its id is not compared).
	var acked [2]bool
	var matched string
	chans := [2]<-chan wsEvent{nodeCh, guardCh}
	for {
		if class, detail, done := refusalVerdict(refused, accepted); done {
			res.Class, res.Detail = class, detail
			return res
		}
		var ev wsEvent
		var side int
		var ok bool
		select {
		case <-ctx.Done():
			if run.Err() != nil {
				res.Class, res.Detail = Unstable, "interrupted before a verdict"
				return res
			}
			switch {
			case matched != "" && !acked[0]:
				res.Class, res.Detail = Failed, "the node never acknowledged the subscribe"
			case matched != "":
				res.Class, res.Detail = Differs, "cosmoguard delivered "+matched+" but never acknowledged the subscribe"
			case refused[0] != nil:
				res.Class, res.Detail = Failed, "node refused the subscription: "+short(refused[0])
			case refused[1] != nil:
				res.Class, res.Detail = Failed, fmt.Sprintf("the node did not answer the subscribe within %s; cosmoguard refused it: %s", wsWindow, short(refused[1]))
			default:
				res.Class = Unstable
				if len(seen[0]) > 0 && len(seen[1]) == 0 {
					res.Class = Differs
				}
				res.Detail = fmt.Sprintf("no block delivered by both within %s (node %d events, cosmoguard %d)", wsWindow, len(seen[0]), len(seen[1]))
			}
			return res
		case ev, ok = <-chans[0]:
			side = 0
		case ev, ok = <-chans[1]:
			side = 1
		}
		if !ok && (ctx.Err() != nil || refused[side] != nil) {
			// The window closed the connections, or the server closed
			// after refusing; the verdict comes from what was seen.
			chans[side] = nil
			continue
		}
		if !ok {
			if side == 0 {
				res.Class, res.Detail = Failed, "node closed the subscription"
			} else {
				res.Class, res.Detail = Differs, "cosmoguard closed the subscription"
			}
			return res
		}
		switch {
		case ev.refusal != nil:
			refused[side] = ev.refusal
			continue
		case ev.ack:
			accepted[side], acked[side] = true, true
			if matched != "" && acked[0] && acked[1] {
				res.Class, res.Detail = Identical, matched
				return res
			}
			continue
		}
		accepted[side] = true
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
		matched = "block " + ev.key
		if acked[0] && acked[1] {
			res.Class, res.Detail = Identical, matched
			return res
		}
	}
}

// refusalVerdict decides a subscription once a refusal and the other
// side's answer are both known: both refusing compares the JSON-RPC error
// objects, only the node refusing means the node failed, only cosmoguard
// refusing is a difference.
func refusalVerdict(refused [2]map[string]any, accepted [2]bool) (Class, string, bool) {
	switch {
	case refused[0] != nil && refused[1] != nil:
		if d := errorDiff(refused[0], refused[1]); d != "" {
			return Differs, "both refused the subscription, with different errors: " + d, true
		}
		return Identical, "both refused the subscription: " + short(refused[0]), true
	case refused[0] != nil && accepted[1]:
		return Failed, "node refused the subscription: " + short(refused[0]), true
	case refused[1] != nil && accepted[0]:
		return Differs, "cosmoguard refused the subscription the node accepted: " + short(refused[1]), true
	}
	return "", "", false
}

// errorDiff lists the JSON-RPC error fields (code, message, data) that
// differ between the node's and cosmoguard's error objects.
func errorDiff(node, guard map[string]any) string {
	var diffs []string
	for _, k := range []string{"code", "message", "data"} {
		if jsonDiff("$", node[k], guard[k]) != "" {
			diffs = append(diffs, fmt.Sprintf("%s node=%s cosmoguard=%s", k, short(node[k]), short(guard[k])))
		}
	}
	return strings.Join(diffs, "; ")
}

// subscribe dials url, sends the subscription and streams its events
// until ctx ends or the connection closes.
func subscribe(ctx context.Context, url string, s wsSub, timeout time.Duration) (<-chan wsEvent, error) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(dctx, url, nil)
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
				ev.refusal = e
			} else if ev.key, ev.payload, ok = s.event(frame); !ok {
				// The subscribe's own answer is an ack; anything else is
				// ignored.
				_, hasResult := frame["result"]
				if !hasResult || fmt.Sprint(frame["id"]) != fmt.Sprint(s.subscribe["id"]) {
					continue
				}
				ev.ack = true
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
