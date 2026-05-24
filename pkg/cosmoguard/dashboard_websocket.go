package cosmoguard

// dashboard_websocket.go exposes the live WebSocket surface the
// dashboard's WebSockets tab renders: connected clients, the
// subscriptions they fan out from, and the upstream connection pool
// backing them. All data is read at snapshot time from live process
// state (the WS proxy's connection registry + the broker's
// subscription manager and upstream pool) — nothing is persisted and
// the per-frame hot path is untouched.

// WSConnInfo is one connected client's dashboard row.
type WSConnInfo struct {
	SourceIP      string `json:"source_ip"`
	Identity      string `json:"identity,omitempty"`
	ConnectedMs   int64  `json:"connected_ms"`
	Subscriptions int    `json:"subscriptions"`
}

// WSSubInfo is one upstream subscription with its client fan-out width
// and the backend it is pinned to.
type WSSubInfo struct {
	Param       string `json:"param"`
	Subscribers int    `json:"subscribers"`
	Upstream    string `json:"upstream,omitempty"`
}

// WSSectionStats is the per-section (rpc.jsonrpc, evm.ws) live view.
// The headline counters sit at the top so the UI can render stat cards
// without walking the detail slices; Conns/Subs/Upstreams carry the
// per-row detail for the tables.
type WSSectionStats struct {
	Section               string       `json:"section"`
	Path                  string       `json:"path"`
	Connections           int          `json:"connections"`
	ClientSubscriptions   int          `json:"client_subscriptions"`
	UpstreamSubscriptions int          `json:"upstream_subscriptions"`
	UpstreamConnsHealthy  int          `json:"upstream_conns_healthy"`
	UpstreamConnsTotal    int          `json:"upstream_conns_total"`
	Conns                 []WSConnInfo `json:"conns"`
	Subs                  []WSSubInfo  `json:"subs"`
	Upstreams             []ConnStat   `json:"upstreams"`
}

// listWebSocket is the JSON payload for GET /api/v1/websocket — one
// WSSectionStats per WS-enabled handler (Tendermint RPC /websocket and
// EVM WS). enabled is true when at least one section has WS wired, so
// the panel can render a "no WebSocket endpoints" hint otherwise.
func listWebSocket(cg *CosmoGuard) map[string]any {
	sections := []WSSectionStats{}
	add := func(h *JsonRpcHandler) {
		if h == nil {
			return
		}
		if wp := h.WSProxy(); wp != nil {
			sections = append(sections, wp.StatsSnapshot())
		}
	}
	if cg != nil {
		add(cg.jsonRpcHandler)
		add(cg.evmJsonRpcWsHandler)
	}
	return map[string]any{"sections": sections, "enabled": len(sections) > 0}
}
