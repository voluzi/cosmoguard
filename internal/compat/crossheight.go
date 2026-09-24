package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

// ProtoCrossHeight names the cross-height probes in results.
const ProtoCrossHeight = "cross-height"

// crossHeightTask asks cosmoguard for the same query at the pinned height
// and at the height below, comparing each with the node at that height.
// Every other comparison uses one height, so a cosmoguard cache keyed
// without the height would pass them all. Here each round asks cosmoguard
// for the pinned height first, so a height-blind cache answers the request
// for the height below from that entry.
//
// send issues the query to one side at one height. The endpoint must give
// different answers at the two heights, or the probe proves nothing and is
// skipped.
func crossHeightTask(name string, height int64, delay time.Duration, send func(ctx context.Context, guard bool, h int64) Response) task {
	return func(ctx context.Context) Result {
		res := Result{Protocol: ProtoCrossHeight, Name: name}
		below := height - 1
		atPin, atBelow := send(ctx, false, height), send(ctx, false, below)
		if atPin.Err == nil && atBelow.Err == nil {
			if c, _ := Classify(atPin, []Response{atBelow}, false); c == Identical {
				res.Class, res.Detail = Skipped, fmt.Sprintf("the node answers the same at heights %d and %d, so the probe proves nothing", height, below)
				return res
			}
		}
		for _, h := range []int64{height, below} {
			class, detail := settle(ctx, comparison{
				fetch: func() (Response, Response, Response, Response) {
					if h != height {
						send(ctx, true, height)
					}
					return send(ctx, false, h), send(ctx, true, h), send(ctx, false, h), send(ctx, true, h)
				},
				pinned: h,
				delay:  delay,
			})
			if class != Identical {
				res.Class, res.Detail = class, fmt.Sprintf("at height %d: %s", h, detail)
				return res
			}
		}
		res.Class, res.Detail = Identical, fmt.Sprintf("heights %d and %d", height, below)
		return res
	}
}

// The cross-height probes use a query whose answer changes every block:
// the community pool grows with each block's fees and inflation.
const (
	crossHeightLCD  = "/cosmos/distribution/v1beta1/community_pool"
	crossHeightGRPC = "/cosmos.distribution.v1beta1.Query/CommunityPool"
)

func crossHeightTasks(h *httpDoer, o Options, height int64, node, guard *grpc.ClientConn) []task {
	var tasks []task
	if o.enabled(ProtoLCD) {
		tasks = append(tasks, crossHeightTask("LCD "+crossHeightLCD, height, o.RoundDelay, func(ctx context.Context, toGuard bool, hh int64) Response {
			base := o.Node.LCD
			if toGuard {
				base = o.Guard.LCD
			}
			return h.do(ctx, http.MethodGet, base+crossHeightLCD, nil, map[string]string{"x-cosmos-block-height": strconv.FormatInt(hh, 10)})
		}))
	}
	if o.enabled(ProtoGRPC) && node != nil && guard != nil {
		tasks = append(tasks, crossHeightTask("gRPC "+crossHeightGRPC, height, o.RoundDelay, func(ctx context.Context, toGuard bool, hh int64) Response {
			conn := node
			if toGuard {
				conn = guard
			}
			cctx, cancel := context.WithTimeout(ctx, o.Timeout)
			defer cancel()
			return invoke(cctx, conn, crossHeightGRPC, nil, hh)
		}))
	}
	if o.enabled(ProtoRPC) {
		tasks = append(tasks, crossHeightTask("RPC abci_query "+crossHeightGRPC, height, o.RoundDelay, func(ctx context.Context, toGuard bool, hh int64) Response {
			base := o.Node.RPC
			if toGuard {
				base = o.Guard.RPC
			}
			body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "abci_query",
				"params": map[string]any{"path": crossHeightGRPC, "height": strconv.FormatInt(hh, 10)}})
			return h.do(ctx, http.MethodPost, base, body, nil)
		}))
	}
	return tasks
}
