package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type evmCall struct {
	method   string
	params   []any
	volatile bool
	skip     string
}

// evmChain holds live values for EVM calls, read at the pinned block.
type evmChain struct {
	block     string // pinned block number, hex
	blockHash string
	addr      string
	txHash    string
	txBlock   string
	txBlockID string
}

const zeroAddress = "0x0000000000000000000000000000000000000000"

// evmCalls lists the read-only eth_/net_/web3_ methods. Sending, signing
// and filter-installing methods are left out.
func evmCalls(c evmChain) []evmCall {
	missing := func(name, v string) string {
		if v == "" {
			return "no live " + name
		}
		return ""
	}
	b := c.block
	return []evmCall{
		// clientVersion, accounts and coinbase describe the answering node.
		{method: "web3_clientVersion", volatile: true},
		{method: "web3_sha3", params: []any{"0x68656c6c6f"}},
		{method: "net_version"},
		{method: "net_listening"},
		{method: "net_peerCount", volatile: true},
		{method: "eth_chainId"},
		{method: "eth_protocolVersion"},
		{method: "eth_accounts", volatile: true},
		{method: "eth_mining"},
		{method: "eth_hashrate"},
		{method: "eth_coinbase", volatile: true},
		{method: "eth_blockNumber", volatile: true},
		{method: "eth_syncing", volatile: true},
		{method: "eth_gasPrice", volatile: true},
		{method: "eth_maxPriorityFeePerGas", volatile: true},
		{method: "eth_feeHistory", params: []any{"0x4", b, []int{25, 75}}},
		{method: "eth_getBlockByNumber", params: []any{b, false}},
		{method: "eth_getBlockByNumber", params: []any{b, true}},
		{method: "eth_getBlockByHash", params: []any{c.blockHash, false}, skip: missing("block hash", c.blockHash)},
		{method: "eth_getBlockTransactionCountByNumber", params: []any{b}},
		{method: "eth_getBlockTransactionCountByHash", params: []any{c.blockHash}, skip: missing("block hash", c.blockHash)},
		{method: "eth_getUncleCountByBlockNumber", params: []any{b}},
		{method: "eth_getUncleByBlockNumberAndIndex", params: []any{b, "0x0"}},
		{method: "eth_getUncleCountByBlockHash", params: []any{c.blockHash}, skip: missing("block hash", c.blockHash)},
		{method: "eth_getUncleByBlockHashAndIndex", params: []any{c.blockHash, "0x0"}, skip: missing("block hash", c.blockHash)},
		{method: "eth_getBlockReceipts", params: []any{b}},
		{method: "eth_getLogs", params: []any{map[string]any{"fromBlock": b, "toBlock": b}}},
		{method: "eth_getBalance", params: []any{c.addr, b}},
		{method: "eth_getTransactionCount", params: []any{c.addr, b}},
		{method: "eth_getCode", params: []any{c.addr, b}},
		{method: "eth_getStorageAt", params: []any{c.addr, "0x0", b}},
		{method: "eth_getProof", params: []any{c.addr, []string{}, b}},
		{method: "eth_call", params: []any{map[string]any{"to": c.addr, "data": "0x"}, b}},
		{method: "eth_estimateGas", params: []any{map[string]any{"from": c.addr, "to": c.addr, "value": "0x0"}, b}},
		{method: "eth_getTransactionByHash", params: []any{c.txHash}, skip: missing("transaction", c.txHash)},
		{method: "eth_getTransactionReceipt", params: []any{c.txHash}, skip: missing("transaction", c.txHash)},
		{method: "eth_getTransactionByBlockNumberAndIndex", params: []any{c.txBlock, "0x0"}, skip: missing("transaction", c.txHash)},
		{method: "eth_getTransactionByBlockHashAndIndex", params: []any{c.txBlockID, "0x0"}, skip: missing("transaction", c.txHash)},
	}
}

func (c evmCall) name() string {
	if len(c.params) == 0 {
		return c.method
	}
	b, _ := json.Marshal(c.params)
	if len(b) > 70 {
		b = append(b[:67], "..."...)
	}
	return c.method + string(b)
}

// discoverEVM reads the pinned block and the nearest block at or below it
// that carries a transaction, for hash and address parameters.
func discoverEVM(ctx context.Context, h *httpDoer, url string, height int64) evmChain {
	c := evmChain{block: fmt.Sprintf("0x%x", height), addr: zeroAddress}
	var pinned struct {
		Hash string `json:"hash"`
	}
	if err := h.postJSONRPC(ctx, url, "eth_getBlockByNumber", []any{c.block, false}, &pinned); err == nil {
		c.blockHash = pinned.Hash
	}
	for hh := height; hh > height-50 && hh > 0; hh-- {
		num := fmt.Sprintf("0x%x", hh)
		var blk struct {
			Hash         string `json:"hash"`
			Transactions []struct {
				Hash string `json:"hash"`
				From string `json:"from"`
			} `json:"transactions"`
		}
		if err := h.postJSONRPC(ctx, url, "eth_getBlockByNumber", []any{num, true}, &blk); err != nil {
			break
		}
		if len(blk.Transactions) > 0 {
			tx := blk.Transactions[0]
			c.txHash, c.txBlock, c.txBlockID = tx.Hash, num, blk.Hash
			if tx.From != "" {
				c.addr = tx.From
			}
			break
		}
	}
	return c
}

func evmTasks(ctx context.Context, h *httpDoer, o Options, height int64) []task {
	calls := evmCalls(discoverEVM(ctx, h, o.Node.EVM, height))
	post := func(body any) func(context.Context, string) Response {
		b, _ := json.Marshal(body)
		return func(ctx context.Context, base string) Response {
			return h.do(ctx, http.MethodPost, base, b, nil)
		}
	}
	var tasks []task
	var batch []any
	for i, c := range calls {
		body := map[string]any{"jsonrpc": "2.0", "id": i + 1, "method": c.method, "params": c.params}
		if c.params == nil {
			body["params"] = []any{}
		}
		tasks = append(tasks, httpPairTask(ProtoEVM, c.name(), c.skip, c.volatile, post(body), o.Node.EVM, o.Guard.EVM, o.RoundDelay))
		if c.method == "eth_chainId" || c.method == "eth_getBalance" || (c.method == "eth_getBlockByNumber" && c.params[1] == false) {
			batch = append(batch, body)
		}
	}
	tasks = append(tasks, httpPairTask(ProtoEVM, "batch(eth_chainId,eth_getBlockByNumber,eth_getBalance)", "", false, post(batch), o.Node.EVM, o.Guard.EVM, o.RoundDelay))
	return tasks
}
