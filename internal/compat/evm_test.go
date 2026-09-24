package compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gotest.tools/assert"
)

// fakeEVM serves eth_getBlockByNumber: block 0x64 has one transaction,
// every other block none. Hash-only requests get transaction hashes as
// strings, as real nodes send them.
func fakeEVM(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Params []any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		num, full := req.Params[0].(string), req.Params[1].(bool)
		var txs any = []any{}
		if num == "0x64" {
			if full {
				txs = []any{map[string]any{"hash": "0xtx", "from": "0xfrom"}}
			} else {
				txs = []any{"0xtx"}
			}
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%s}`, mustJSON(map[string]any{"hash": "0xhash" + num, "transactions": txs}))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestDiscoverEVM(t *testing.T) {
	url := fakeEVM(t)
	h := newHTTPDoer(5 * time.Second)

	// The pinned block itself carries a transaction (hashes as strings).
	c := discoverEVM(t.Context(), h, url, 100)
	assert.Equal(t, c.block, "0x64")
	assert.Equal(t, c.blockHash, "0xhash0x64")
	assert.Equal(t, c.txHash, "0xtx")
	assert.Equal(t, c.addr, "0xfrom")

	// Walking back from an empty block finds the transaction below it.
	c = discoverEVM(t.Context(), h, url, 103)
	assert.Equal(t, c.blockHash, "0xhash0x67")
	assert.Equal(t, c.txBlock, "0x64")
	assert.Equal(t, c.txBlockID, "0xhash0x64")
}

func TestEvmCallName(t *testing.T) {
	assert.Equal(t, evmCall{method: "eth_chainId"}.name(), "eth_chainId")
	assert.Equal(t, evmCall{method: "eth_getBalance", params: []any{"0xa", "0x1"}}.name(), `eth_getBalance["0xa","0x1"]`)
}

// TestEvmCallsReadOnly guards the safety of running against public nodes:
// nothing that sends, signs or installs server-side state.
func TestEvmCallsReadOnly(t *testing.T) {
	// Every method here was checked to read state only. Adding one to
	// evmCalls means checking it and adding it here.
	readOnly := map[string]bool{
		"web3_clientVersion": true, "web3_sha3": true, "eth_getUncleCountByBlockHash": true, "eth_getUncleByBlockHashAndIndex": true, "net_version": true, "net_listening": true, "net_peerCount": true,
		"eth_chainId": true, "eth_protocolVersion": true, "eth_accounts": true, "eth_mining": true,
		"eth_hashrate": true, "eth_coinbase": true, "eth_blockNumber": true, "eth_syncing": true,
		"eth_gasPrice": true, "eth_maxPriorityFeePerGas": true, "eth_feeHistory": true,
		"eth_getBlockByNumber": true, "eth_getBlockByHash": true,
		"eth_getBlockTransactionCountByNumber": true, "eth_getBlockTransactionCountByHash": true,
		"eth_getUncleCountByBlockNumber": true, "eth_getUncleByBlockNumberAndIndex": true,
		"eth_getBlockReceipts": true, "eth_getLogs": true, "eth_getBalance": true,
		"eth_getTransactionCount": true, "eth_getCode": true, "eth_getStorageAt": true,
		"eth_getProof": true, "eth_call": true, "eth_estimateGas": true,
		"eth_getTransactionByHash": true, "eth_getTransactionReceipt": true,
		"eth_getTransactionByBlockNumberAndIndex": true, "eth_getTransactionByBlockHashAndIndex": true,
	}
	for _, c := range evmCalls(evmChain{block: "0x1", txHash: "0xt", blockHash: "0xb"}) {
		assert.Assert(t, readOnly[c.method], "%s is not on the verified read-only list", c.method)
	}
}
