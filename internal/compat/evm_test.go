package compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
	for _, c := range evmCalls(evmChain{block: "0x1", txHash: "0xt", blockHash: "0xb"}) {
		for _, bad := range []string{"eth_send", "eth_sign", "personal_", "eth_newFilter", "eth_newBlockFilter", "eth_newPendingTransactionFilter", "eth_uninstallFilter", "debug_", "admin_", "miner_"} {
			assert.Assert(t, !strings.HasPrefix(c.method, bad), c.method)
		}
	}
}
