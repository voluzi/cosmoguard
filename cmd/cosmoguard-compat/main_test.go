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
