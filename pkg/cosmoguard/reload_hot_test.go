package cosmoguard

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newHotReloadTestGuard(t *testing.T, raw string) *CosmoGuard {
	t.Helper()
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "COSMOGUARD_") {
			t.Setenv(name, "")
		}
	}
	trusted := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(trusted) })
	cfg := parseRestartConfig(t, raw)
	cg := &CosmoGuard{cfg: cfg, cfgFile: filepath.Join(t.TempDir(), "config.yaml"), origNodes: cfg.Nodes,
		dashboard: newDashboardObservability(), lcdProxy: &HttpProxy{}, rpcProxy: &HttpProxy{}, grpcProxy: &GrpcProxy{},
		jsonRpcHandler: newEnvelopeTestHandler(t), evmRpcProxy: &HttpProxy{},
		evmJsonRpcHandler: newEnvelopeTestHandler(t), evmJsonRpcWsHandler: newEnvelopeTestHandler(t),
	}
	for _, handler := range []*JsonRpcHandler{cg.jsonRpcHandler, cg.evmJsonRpcHandler, cg.evmJsonRpcWsHandler} {
		handler.maxBatchSize = *cfg.RPC.JsonRpc.MaxBatchSize
	}
	cg.applyRulesLocked()
	return cg
}

func reloadTestFile(t *testing.T, cg *CosmoGuard, raw string) {
	t.Helper()
	if err := os.WriteFile(cg.cfgFile, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	cg.tryReload()
}

func reloadBatchRequest(handler *JsonRpcHandler, size int) (*httptest.ResponseRecorder, bool) {
	requests := make([]map[string]any, size)
	responses := make([]map[string]any, size)
	for i := range requests {
		requests[i] = map[string]any{"jsonrpc": "2.0", "id": i + 1, "method": "call"}
		responses[i] = map[string]any{"jsonrpc": "2.0", "id": i + 1, "result": "ok"}
	}
	raw, _ := stdjson.Marshal(requests)
	response, _ := stdjson.Marshal(responses)
	recorder := httptest.NewRecorder()
	forwarded := false
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(raw))), func(w http.ResponseWriter, _ *http.Request) {
		forwarded = true
		_, _ = w.Write(response)
	})
	return recorder, forwarded
}

func assertReloadBatch(t *testing.T, handler *JsonRpcHandler, size, status int) {
	t.Helper()
	response, forwarded := reloadBatchRequest(handler, size)
	if response.Code != status || forwarded != (status == http.StatusOK) {
		t.Fatalf("batch size %d: status=%d forwarded=%v; want status=%d", size, response.Code, forwarded, status)
	}
	if status == http.StatusOK {
		var replies []struct {
			Result string `json:"result"`
		}
		if err := stdjson.Unmarshal(response.Body.Bytes(), &replies); err != nil || len(replies) != size {
			t.Fatalf("batch replies = %s, error = %v", response.Body.String(), err)
		}
		for _, reply := range replies {
			if reply.Result != "ok" {
				t.Fatalf("batch reply = %+v; want upstream result", reply)
			}
		}
	}
}

func TestTryReloadJSONRPCBatchLimit(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	config := func(limit string) string {
		return fmt.Sprintf("enableEvm: true\nrpc: {jsonrpc: {default: allow%s}}\nevm: {rpc: {default: allow}, ws: {default: allow}}", limit)
	}
	cg := newHotReloadTestGuard(t, config(", maxBatchSize: 2"))
	handlers := map[string]*JsonRpcHandler{"cosmos rpc": cg.jsonRpcHandler, "evm rpc": cg.evmJsonRpcHandler, "evm ws HTTP": cg.evmJsonRpcWsHandler}
	for _, handler := range handlers {
		assertReloadBatch(t, handler, 2, http.StatusOK)
	}
	for _, tc := range []struct {
		name, limit string
		size, code  int
	}{
		{"lowered", ", maxBatchSize: 1", 2, http.StatusRequestEntityTooLarge},
		{"disabled", ", maxBatchSize: 0", 3, http.StatusOK},
		{"default restored", "", 101, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reloadTestFile(t, cg, config(tc.limit))
			if status := cg.dashboard.lastReload; status == nil || !status.Success {
				t.Fatalf("reload rejected: %+v", status)
			}
			for name, handler := range handlers {
				t.Run(name, func(t *testing.T) {
					assertReloadBatch(t, handler, tc.size, tc.code)
					assertReloadBatch(t, handler, 1, http.StatusOK)
				})
			}
		})
	}
	var readers sync.WaitGroup
	for _, handler := range handlers {
		readers.Go(func() {
			for i := 0; i < 100; i++ {
				response, forwarded := reloadBatchRequest(handler, 1)
				if response.Code != http.StatusOK || !forwarded {
					t.Errorf("concurrent in-limit batch: status=%d forwarded=%v", response.Code, forwarded)
					return
				}
			}
		})
	}
	for i := 0; i < 10; i++ {
		reloadTestFile(t, cg, config(fmt.Sprintf(", maxBatchSize: %d", 1+i%2)))
	}
	readers.Wait()
}
