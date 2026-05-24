package testharness_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestK_GoldenReplay verifies the Phase K compatibility-replay
// infrastructure end-to-end:
//  1. Build a synthetic fixture file (mimicking what
//     scripts/record-golden.sh produces).
//  2. Load it via LoadGoldenFixtures.
//  3. Seed the harness's fake upstream from the fixture.
//  4. Issue a request through cosmoguard and assert CompareResponse
//     returns "" (byte-identical match).
//
// Once the user records real fixtures against live nodes, the
// TestCompatibility matrix will follow this exact pattern.
func TestK_GoldenReplay(t *testing.T) {
	dir := t.TempDir()
	chain := "synthetic"
	chainDir := filepath.Join(dir, chain)
	assert.NilError(t, os.MkdirAll(chainDir, 0o755))

	// Manifest
	mf := map[string]any{
		"chain":          chain,
		"recorded_at":    "2026-01-01T00:00:00Z",
		"lcd":            "https://lcd.example.com",
		"rpc":            "https://rpc.example.com",
		"ignore_headers": []string{"Date", "X-Request-Id"},
	}
	mfBytes, _ := json.Marshal(mf)
	assert.NilError(t, os.WriteFile(filepath.Join(chainDir, "manifest.json"), mfBytes, 0o644))

	// One fixture.
	body := []byte(`{"params":{}}`)
	fx := map[string]any{
		"request": map[string]any{
			"method": "GET",
			"path":   "/cosmos/staking/v1beta1/params",
		},
		"response": map[string]any{
			"status":      200,
			"headers":     map[string]string{"Content-Type": "application/json"},
			"body_base64": base64.StdEncoding.EncodeToString(body),
		},
	}
	fxBytes, _ := json.Marshal(fx)
	assert.NilError(t, os.WriteFile(filepath.Join(chainDir, "get_cosmos_staking_v1beta1_params.json"), fxBytes, 0o644))

	// One RPC-service fixture, exercising the per-service routing in
	// SeedFromFixtures. The fake RPC server must answer the same body
	// when cosmoguard relays through the RPC listener.
	rpcBody := []byte(`{"abci_info":{"data":"x"}}`)
	rpcFx := map[string]any{
		"request": map[string]any{
			"method":  "GET",
			"path":    "/abci_info",
			"service": "rpc",
		},
		"response": map[string]any{
			"status":      200,
			"headers":     map[string]string{"Content-Type": "application/json"},
			"body_base64": base64.StdEncoding.EncodeToString(rpcBody),
		},
	}
	rpcFxBytes, _ := json.Marshal(rpcFx)
	assert.NilError(t, os.WriteFile(filepath.Join(chainDir, "get_abci_info.json"), rpcFxBytes, 0o644))

	manifest, fixtures := testharness.LoadGoldenFixtures(t, chainDir)
	assert.Equal(t, manifest.Chain, "synthetic")
	assert.Equal(t, len(fixtures), 2)

	// Seed harness and replay through cosmoguard.
	h := testharness.New(t)
	h.SeedFromFixtures(fixtures)

	// Replay each fixture through the service-appropriate cosmoguard
	// listener. The seeding side already routed to LCD vs RPC fakes by
	// service; the replay side must use the matching listener URL so
	// the round-trip exercises the right proxy code path.
	for _, f := range fixtures {
		var url string
		switch f.Request.Service {
		case "rpc":
			url = h.RPCURL + f.Request.Path
		case "evm_rpc":
			url = h.EvmRPC + f.Request.Path
		default: // "" / "lcd"
			url = h.LCDURL + f.Request.Path
		}
		resp := h.GET(t, url)
		diff := testharness.CompareResponse(f, resp, manifest.IgnoreHeaders)
		assert.Equal(t, diff, "",
			"cosmoguard-relayed response should match the golden fixture (service=%q path=%q): %s",
			f.Request.Service, f.Request.Path, diff)
	}
}

// TestK_GoldenReplay_JSONRPCCollision exercises the SeedFromFixtures
// bucketing path: two fixtures share the same (service, method, path)
// — both are POSTs to the RPC root — but carry different JSON-RPC
// methods in the body. Without per-body dispatch, the second fixture
// would silently overwrite the first via SetResponse and the replay
// would see one wrong response. The test asserts each request gets
// the response matching its specific JSON-RPC method, and that no
// fixture is overwritten silently.
func TestK_GoldenReplay_JSONRPCCollision(t *testing.T) {
	mkFixture := func(reqBody, respBody []byte) testharness.GoldenFixture {
		var f testharness.GoldenFixture
		f.Request.Method = "POST"
		f.Request.Path = "/"
		f.Request.Service = "rpc"
		f.Request.ContentType = "application/json"
		f.Request.BodyBase64 = base64.StdEncoding.EncodeToString(reqBody)
		f.Response.Status = 200
		f.Response.Headers = map[string]string{"Content-Type": "application/json"}
		f.Response.BodyBase64 = base64.StdEncoding.EncodeToString(respBody)
		return f
	}

	abciReq := []byte(`{"jsonrpc":"2.0","id":1,"method":"abci_info","params":[]}`)
	abciResp := []byte(`{"jsonrpc":"2.0","id":1,"result":{"response":{"data":"abci"}}}`)
	healthReq := []byte(`{"jsonrpc":"2.0","id":1,"method":"health","params":[]}`)
	healthResp := []byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)

	fixtures := []testharness.GoldenFixture{
		mkFixture(abciReq, abciResp),
		mkFixture(healthReq, healthResp),
	}

	h := testharness.New(t)
	h.SeedFromFixtures(fixtures)

	// Each request must round-trip to its own response, not the other one.
	for _, f := range fixtures {
		reqBody, _ := base64.StdEncoding.DecodeString(f.Request.BodyBase64)
		resp := h.POST(t, h.RPCURL+f.Request.Path, "application/json", reqBody)
		diff := testharness.CompareResponse(f, resp, nil)
		assert.Equal(t, diff, "",
			"each colliding fixture must replay to its own response (req=%q): %s",
			string(reqBody), diff)
	}
	// Belt-and-suspenders: every fixture's POST should have hit the
	// fake. A regression that returned the wrong response from a cached
	// handler would still increment CallCount, but if SeedFromFixtures
	// silently dropped one fixture entirely, the second POST would land
	// on the catch-all 404 and we'd see an unexpected miss.
	assert.Equal(t, h.Upstream.RPC.CallCount("POST", "/"), len(fixtures),
		"every colliding-fixture POST should reach the RPC fake")
}

// TestK_GoldenReplay_SameMethodDifferentParams covers the demotion
// path in SeedFromFixtures: two fixtures share the same JSON-RPC
// method ("eth_call") but differ in params. The seeder must remove
// both from byMethod (since neither uniquely owns "eth_call") and
// route via byte-exact body match. A regression that kept colliding
// methods in byMethod would silently return the second fixture for
// any request matching "eth_call", regardless of params.
func TestK_GoldenReplay_SameMethodDifferentParams(t *testing.T) {
	mkFixture := func(reqBody, respBody []byte) testharness.GoldenFixture {
		var f testharness.GoldenFixture
		f.Request.Method = "POST"
		f.Request.Path = "/"
		f.Request.Service = "rpc"
		f.Request.ContentType = "application/json"
		f.Request.BodyBase64 = base64.StdEncoding.EncodeToString(reqBody)
		f.Response.Status = 200
		f.Response.Headers = map[string]string{"Content-Type": "application/json"}
		f.Response.BodyBase64 = base64.StdEncoding.EncodeToString(respBody)
		return f
	}

	reqA := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":["aaa"]}`)
	respA := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xAAA"}`)
	reqB := []byte(`{"jsonrpc":"2.0","id":2,"method":"eth_call","params":["bbb"]}`)
	respB := []byte(`{"jsonrpc":"2.0","id":2,"result":"0xBBB"}`)

	fixtures := []testharness.GoldenFixture{mkFixture(reqA, respA), mkFixture(reqB, respB)}

	h := testharness.New(t)
	h.SeedFromFixtures(fixtures)

	// Request A must come back as A, not B (which would happen if a
	// regression left byMethod["eth_call"]=respB shadowing the dispatch).
	respGotA := h.POST(t, h.RPCURL+"/", "application/json", reqA)
	diffA := testharness.CompareResponse(fixtures[0], respGotA, nil)
	assert.Equal(t, diffA, "", "same-method-different-params: A must return A: %s", diffA)

	respGotB := h.POST(t, h.RPCURL+"/", "application/json", reqB)
	diffB := testharness.CompareResponse(fixtures[1], respGotB, nil)
	assert.Equal(t, diffB, "", "same-method-different-params: B must return B: %s", diffB)

	// A novel eth_call with params we never recorded must NOT
	// resolve to either fixture (no silent shadowing). It hits the
	// no-match fallback (404 with the documented error body). We
	// assert on the body text, not just the 404, because a future
	// regression that returns 404 from a different code path (e.g.
	// "method not allowed" added at SetDynamicHandler) would still
	// satisfy a status-only check.
	reqUnknown := []byte(`{"jsonrpc":"2.0","id":3,"method":"eth_call","params":["ccc"]}`)
	respGotU := h.POST(t, h.RPCURL+"/", "application/json", reqUnknown)
	assert.Equal(t, respGotU.StatusCode, 404,
		"unrecorded params must miss both fixtures, got status %d", respGotU.StatusCode)
	assert.Assert(t, strings.Contains(string(respGotU.Body), "no fixture matched"),
		"unrecorded params must hit the documented no-match fallback, got body %q",
		string(respGotU.Body))
}
