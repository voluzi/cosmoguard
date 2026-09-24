package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/internal/compat"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
)

func TestOverlay(t *testing.T) {
	defaults := compat.Endpoints{LCD: "https://lcd", RPC: "https://rpc", GRPC: "https://grpc"}
	got := overlay(defaults, compat.Endpoints{GRPC: "http://localhost:19090"})
	assert.DeepEqual(t, got, compat.Endpoints{LCD: "https://lcd", RPC: "https://rpc", GRPC: "http://localhost:19090"})
}

func TestTrimSlashes(t *testing.T) {
	got := trimSlashes(compat.Endpoints{LCD: "https://lcd/", RPC: "https://rpc//", GRPC: "http://g:1"})
	assert.DeepEqual(t, got, compat.Endpoints{LCD: "https://lcd", RPC: "https://rpc", GRPC: "http://g:1"})
}

func TestRequiredURLs(t *testing.T) {
	only := func(ps ...string) func(string) bool {
		return func(p string) bool {
			for _, q := range ps {
				if p == q {
					return true
				}
			}
			return false
		}
	}
	all := func(string) bool { return true }
	full := compat.Endpoints{LCD: "l", RPC: "r", GRPC: "g"}

	assert.Assert(t, requiredURLs(full, all, true) == nil)
	assert.DeepEqual(t, requiredURLs(compat.Endpoints{}, all, false), []string{"--guard-lcd", "--guard-rpc", "--guard-grpc"})
	// An RPC-only run needs no gRPC on either side and no guard LCD.
	assert.Assert(t, requiredURLs(compat.Endpoints{LCD: "l", RPC: "r"}, only(compat.ProtoRPC), true) == nil)
	assert.Assert(t, requiredURLs(compat.Endpoints{RPC: "r"}, only(compat.ProtoRPC), false) == nil)
	// LCD discovery needs the node's gRPC, not cosmoguard's.
	assert.DeepEqual(t, requiredURLs(compat.Endpoints{LCD: "l", RPC: "r"}, only(compat.ProtoLCD), true), []string{"--node-grpc"})
	assert.Assert(t, requiredURLs(compat.Endpoints{LCD: "l"}, only(compat.ProtoLCD), false) == nil)
}

func TestWithoutOverrides(t *testing.T) {
	got := withoutOverrides([]string{"PATH=/bin", "COSMOGUARD_NODE_RPC_URL=http://x", "HOME=/h"})
	assert.DeepEqual(t, got, []string{"PATH=/bin", "HOME=/h"})
}

func TestDefaultNode(t *testing.T) {
	// Without flags the node is a port-forward on the standard ports.
	assert.DeepEqual(t, overlay(defaultNode, compat.Endpoints{}), compat.Endpoints{
		LCD:   "http://localhost:1317",
		RPC:   "http://localhost:26657",
		GRPC:  "http://localhost:9090",
		EVM:   "http://localhost:8545",
		EVMWS: "ws://localhost:8546",
	})
	// Each --node-* flag replaces only its own default.
	got := overlay(defaultNode, compat.Endpoints{RPC: "http://localhost:36657"})
	assert.Equal(t, got.RPC, "http://localhost:36657")
	assert.Equal(t, got.LCD, defaultNode.LCD)
	assert.Equal(t, got.GRPC, defaultNode.GRPC)
}

func TestResolveEVM(t *testing.T) {
	refused := &net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	timeout := errors.New("i/o timeout")
	const url = "http://localhost:8545"

	got, err := resolveEVM(url, false, nil)
	assert.NilError(t, err)
	assert.Equal(t, got, url, "a reachable default is compared")

	got, err = resolveEVM(url, false, refused)
	assert.NilError(t, err, "a chain without EVM is not a failure")
	assert.Equal(t, got, "", "a refused default is skipped")

	_, err = resolveEVM(url, true, refused)
	assert.ErrorContains(t, err, "unreachable", "an explicit endpoint must be reachable")

	got, err = resolveEVM(url, false, timeout)
	assert.NilError(t, err)
	assert.Equal(t, got, url, "only a refusal means no EVM; other failures are reported by the comparison")
}

func TestReachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	addr := l.Addr().String()
	assert.NilError(t, reachable("http://"+addr))
	l.Close()
	err = reachable("ws://" + addr)
	assert.Assert(t, errors.Is(err, syscall.ECONNREFUSED), "a closed port is refused: %v", err)
}

// TestSpawnConfigAcceptsWebSocketUpstreams renders the spawn config for an
// EVM node given ws:// or wss:// WebSocket URLs, and builds a cosmoguard
// from it: cosmoguard takes WebSocket upstreams as http(s) URLs only.
func TestSpawnConfigAcceptsWebSocketUpstreams(t *testing.T) {
	for _, ws := range []string{"ws://localhost:8546", "wss://evm-ws.example.com", ""} {
		t.Run(ws, func(t *testing.T) {
			node := compat.Endpoints{
				LCD: "http://localhost:1317", RPC: "http://localhost:26657", GRPC: "http://localhost:9090",
				EVM: "https://evm.example.com", EVMWS: ws,
			}
			ports, err := freePorts(6)
			assert.NilError(t, err)
			path := filepath.Join(t.TempDir(), "cosmoguard.yaml")
			f, err := os.Create(path)
			assert.NilError(t, err)
			assert.NilError(t, writeSpawnConfig(f, node, ports))
			assert.NilError(t, f.Close())

			cfg, err := cosmoguard.ReadConfigFromFile(path)
			assert.NilError(t, err)
			for _, n := range cfg.Nodes {
				assert.Assert(t, strings.HasPrefix(n.EvmRpcWsURL, "http://") || strings.HasPrefix(n.EvmRpcWsURL, "https://"), n.EvmRpcWsURL)
			}
			cg, err := cosmoguard.New(cfg)
			assert.NilError(t, err, "cosmoguard must accept the rendered config")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = cg.Shutdown(ctx)
		})
	}
}

func TestHTTPScheme(t *testing.T) {
	assert.Equal(t, httpScheme("ws://h:8546"), "http://h:8546")
	assert.Equal(t, httpScheme("wss://h"), "https://h")
	assert.Equal(t, httpScheme("https://h"), "https://h")
}
