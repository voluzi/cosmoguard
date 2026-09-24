package main

import (
	"testing"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/internal/compat"
)

func TestOverlay(t *testing.T) {
	preset := compat.Endpoints{LCD: "https://lcd", RPC: "https://rpc", GRPC: "https://grpc"}
	got := overlay(preset, compat.Endpoints{GRPC: "http://localhost:19090"})
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
