package compat

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// rpcParam is one CometBFT parameter in both of its encodings: the URI
// form (strings quoted, bytes as 0x-hex) and the JSON-RPC form (bytes as
// base64).
type rpcParam struct {
	name string
	uri  string
	json any
}

type rpcCall struct {
	method   string
	params   []rpcParam
	volatile bool
	// skip says why the call cannot be built (a live value is missing).
	skip string
}

// cometCalls lists every read-only CometBFT RPC method. Writes
// (broadcast_*, check_tx, unsafe_*) are left out, and genesis is covered
// by genesis_chunked because a full genesis can be hundreds of MB.
func cometCalls(height int64, blockHash, txHash string) []rpcCall {
	h := strconv.FormatInt(height, 10)
	num := func(name, v string) rpcParam { return rpcParam{name, v, v} }
	str := func(name, v string) rpcParam { return rpcParam{name, strconv.Quote(v), v} }
	byt := func(name, hexv string) rpcParam {
		raw, _ := hex.DecodeString(hexv)
		return rpcParam{name, "0x" + hexv, base64.StdEncoding.EncodeToString(raw)}
	}
	missing := func(name, v string) string {
		if v == "" {
			return "no live " + name
		}
		return ""
	}
	return []rpcCall{
		{method: "health"},
		{method: "status", volatile: true},
		{method: "net_info", volatile: true},
		{method: "abci_info", volatile: true},
		{method: "num_unconfirmed_txs", volatile: true},
		{method: "unconfirmed_txs", params: []rpcParam{num("limit", "1")}, volatile: true},
		{method: "consensus_state", volatile: true},
		{method: "dump_consensus_state", volatile: true},
		{method: "genesis_chunked", params: []rpcParam{num("chunk", "0")}},
		{method: "blockchain", params: []rpcParam{num("minHeight", strconv.FormatInt(height-2, 10)), num("maxHeight", h)}},
		{method: "block", params: []rpcParam{num("height", h)}},
		{method: "block_results", params: []rpcParam{num("height", h)}},
		{method: "commit", params: []rpcParam{num("height", h)}},
		{method: "header", params: []rpcParam{num("height", h)}},
		{method: "validators", params: []rpcParam{num("height", h), num("page", "1"), num("per_page", "30")}},
		{method: "consensus_params", params: []rpcParam{num("height", h)}},
		{method: "block_by_hash", params: []rpcParam{byt("hash", blockHash)}, skip: missing("block hash", blockHash)},
		{method: "header_by_hash", params: []rpcParam{byt("hash", blockHash)}, skip: missing("block hash", blockHash)},
		{method: "tx", params: []rpcParam{byt("hash", txHash)}, skip: missing("tx hash", txHash)},
		{method: "tx_search", params: []rpcParam{str("query", "tx.height="+h), num("per_page", "5")}},
		{method: "block_search", params: []rpcParam{str("query", "block.height="+h)}},
		{method: "abci_query", params: []rpcParam{str("path", "/cosmos.bank.v1beta1.Query/Params"), num("height", h)}},
	}
}

func (c rpcCall) uri() string {
	q := make([]string, 0, len(c.params))
	for _, p := range c.params {
		q = append(q, p.name+"="+url.QueryEscape(p.uri))
	}
	s := "/" + c.method
	if len(q) > 0 {
		s += "?" + strings.Join(q, "&")
	}
	return s
}

func (c rpcCall) body(id int) map[string]any {
	params := map[string]any{}
	for _, p := range c.params {
		params[p.name] = p.json
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "method": c.method, "params": params}
}

func cometTasks(ctx context.Context, h *httpDoer, o Options, p Params, height int64) []task {
	var blk struct {
		BlockID struct {
			Hash string `json:"hash"`
		} `json:"block_id"`
	}
	blockHash := ""
	if err := h.postJSONRPC(ctx, o.Node.RPC, "block", map[string]any{"height": strconv.FormatInt(height, 10)}, &blk); err == nil {
		blockHash = blk.BlockID.Hash
	}
	calls := cometCalls(height, blockHash, p["tx_hash"])

	post := func(ctx context.Context, base string, body []byte) Response {
		return h.do(ctx, http.MethodPost, base, body, nil)
	}
	var tasks []task
	for _, c := range calls {
		tasks = append(tasks,
			httpPairTask(ProtoRPC, "GET /"+c.method, c.skip, c.volatile, func(ctx context.Context, base string) Response {
				return h.do(ctx, http.MethodGet, base+c.uri(), nil, nil)
			}, o.Node.RPC, o.Guard.RPC),
			httpPairTask(ProtoRPC, "POST "+c.method, c.skip, c.volatile, func(ctx context.Context, base string) Response {
				b, _ := json.Marshal(c.body(1))
				return post(ctx, base, b)
			}, o.Node.RPC, o.Guard.RPC),
		)
	}

	// One batch of pinned calls, to cover batch handling.
	batch := []map[string]any{}
	for i, c := range calls {
		switch c.method {
		case "block", "validators", "abci_query":
			batch = append(batch, c.body(i+1))
		}
	}
	tasks = append(tasks, httpPairTask(ProtoRPC, "POST batch(block,validators,abci_query)", "", false, func(ctx context.Context, base string) Response {
		b, _ := json.Marshal(batch)
		return post(ctx, base, b)
	}, o.Node.RPC, o.Guard.RPC))
	return tasks
}

// httpPairTask compares node and cosmoguard answers (see settle) to the
// request built by send.
func httpPairTask(proto, name, skip string, volatile bool, send func(context.Context, string) Response, node, guard string) task {
	return func(ctx context.Context) Result {
		res := Result{Protocol: proto, Name: name}
		if skip != "" {
			res.Class, res.Detail = Skipped, skip
			return res
		}
		fetch := func() (Response, Response, Response, Response) {
			return send(ctx, node), send(ctx, guard), send(ctx, node), send(ctx, guard)
		}
		res.Class, res.Detail = settle(fetch, volatile, func(r Response) Response { return r })
		return res
	}
}
