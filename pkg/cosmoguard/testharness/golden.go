package testharness

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// GoldenFixture is one captured request+response from a real Cosmos node.
// Recorded by scripts/record-golden.sh (legacy local-node fixtures) or
// scripts/record-golden-remote.sh (live-node fixtures captured for
// TestK_GoldenLiveCompatibility). Replayed by the compatibility test
// suite to assert cosmoguard preserves byte-identical behavior.
//
// Service is "lcd" / "rpc" / "evm_rpc". Empty defaults to "lcd" for
// backwards compatibility with pre-live fixtures. BodyBase64 +
// ContentType on the request are populated only for non-GET fixtures
// (JSON-RPC POSTs, etc.) — empty for the GET-only legacy shape, which
// makes this struct backwards-compatible with the existing fixture
// JSONs.
type GoldenFixture struct {
	Request struct {
		Method      string `json:"method"`
		Path        string `json:"path"`
		Service     string `json:"service,omitempty"`
		ContentType string `json:"content_type,omitempty"`
		BodyBase64  string `json:"body_base64,omitempty"`
	} `json:"request"`
	Response struct {
		Status     int               `json:"status"`
		Headers    map[string]string `json:"headers"`
		BodyBase64 string            `json:"body_base64"`
	} `json:"response"`
}

// GoldenManifest is the per-chain summary written by record-golden.sh
// (legacy local-node) or record-golden-remote.sh (live-node). EvmRPC
// is empty for local-node fixtures or chains without EVM support.
type GoldenManifest struct {
	Chain         string   `json:"chain"`
	RecordedAt    string   `json:"recorded_at"`
	LCD           string   `json:"lcd"`
	RPC           string   `json:"rpc"`
	EvmRPC        string   `json:"evm_rpc,omitempty"`
	IgnoreHeaders []string `json:"ignore_headers"`
}

// LoadGoldenFixtures walks a directory of recorded fixtures and returns
// them paired with the chain's manifest. The directory layout is the
// one record-golden.sh produces:
//
//	testdata/golden/<chain>/manifest.json
//	testdata/golden/<chain>/<request-slug>.json
func LoadGoldenFixtures(t *testing.T, dir string) (GoldenManifest, []GoldenFixture) {
	t.Helper()
	manifestPath := filepath.Join(dir, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("golden: read manifest %s: %v", manifestPath, err)
	}
	var manifest GoldenManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("golden: parse manifest: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("golden: read dir %s: %v", dir, err)
	}
	var fixtures []GoldenFixture
	for _, e := range entries {
		if e.IsDir() || e.Name() == "manifest.json" || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("golden: read fixture %s: %v", e.Name(), err)
		}
		var f GoldenFixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("golden: parse %s: %v", e.Name(), err)
		}
		fixtures = append(fixtures, f)
	}
	// Stable order for deterministic test runs.
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].Request.Path < fixtures[j].Request.Path
	})
	return manifest, fixtures
}

// SeedFromFixtures pre-loads each fixture's response onto the harness's
// fake upstream so that a request hitting cosmoguard with the same
// method+path gets the recorded response back. Routing follows the
// fixture's Service field:
//
//	"" or "lcd"  → LCD fake (back-compat with the GET-only legacy shape)
//	"rpc"        → RPC fake
//	"evm_rpc"    → RPC fake (the harness shares the RPC server for EVM)
//
// Unknown service values fall back to LCD with a no-op (silent) so an
// older fixture file with a new service label can still be loaded
// without panicking; tests that care can assert on CallCount per
// server.
//
// When multiple fixtures share the same (server, method, path) — which
// is common for JSON-RPC POSTs to the root path with different methods,
// or for the harness's RPC fake which doubles as the EVM-RPC server —
// the seeder installs a DynamicHandler that dispatches by JSON-RPC
// method (read from the body) and falls back to exact request-body
// match. Without this bucketing, two fixtures with the same
// method+path would silently overwrite each other via SetResponse and
// the second-loaded fixture would shadow the first; the replay loop
// would then see one wrong response without any signal that something
// was lost.
//
// Use this together with a harness built with default:allow + no rules
// to assert that cosmoguard preserves byte-for-byte response shape
// when it's not gating anything — the compatibility-with-direct-node
// invariant.
func (h *Harness) SeedFromFixtures(fixtures []GoldenFixture) {
	h.t.Helper()
	type bucketKey struct {
		srv    *FakeHTTPServer
		method string
		path   string
	}
	buckets := map[bucketKey][]GoldenFixture{}
	for _, f := range fixtures {
		var srv *FakeHTTPServer
		switch f.Request.Service {
		case "rpc", "evm_rpc":
			srv = h.Upstream.RPC
		default: // "" / "lcd" / unknown
			srv = h.Upstream.LCD
		}
		k := bucketKey{srv: srv, method: f.Request.Method, path: f.Request.Path}
		buckets[k] = append(buckets[k], f)
	}
	// Pre-validate every bucket before installing any handler. Map
	// iteration order is non-deterministic in Go; if we Fatalf'd in the
	// middle of the install loop, callers would see a partially-seeded
	// harness whose state depended on map walk order. The two-pass
	// shape also means the install loop below is total — every
	// reachable bucket gets a handler — without belt-and-suspenders
	// re-validation at install time.
	for k, fxs := range buckets {
		if len(fxs) == 1 {
			continue
		}
		emptyCount := 0
		bodyCount := map[string]int{}
		for _, f := range fxs {
			reqBody, _ := base64.StdEncoding.DecodeString(f.Request.BodyBase64)
			if len(reqBody) == 0 {
				emptyCount++
				continue
			}
			bodyCount[string(reqBody)]++
		}
		if emptyCount > 1 {
			h.t.Fatalf("golden: %d fixtures for %s %s have empty request bodies — ambiguous, cannot seed",
				emptyCount, k.method, k.path)
		}
		for body, n := range bodyCount {
			if n > 1 {
				h.t.Fatalf("golden: %d fixtures for %s %s share request body %q — duplicate capture, cannot seed",
					n, k.method, k.path, body)
			}
		}
	}
	// Install handlers.
	for k, fxs := range buckets {
		if len(fxs) == 1 {
			f := fxs[0]
			body, _ := base64.StdEncoding.DecodeString(f.Response.BodyBase64)
			k.srv.SetResponse(f.Request.Method, f.Request.Path, FakeResponse{
				StatusCode: f.Response.Status,
				Headers:    f.Response.Headers,
				Body:       body,
			})
			continue
		}
		// Multiple fixtures share the same (server, method, path). Build
		// dispatch tables:
		//   - byMethod[m]  → response for a JSON-RPC method that appears
		//                    in exactly one fixture (single owner)
		//   - byBody[req]  → byte-exact-body lookup; primary matcher.
		//                    Covers identical method + different
		//                    params/id, and any non-JSON-RPC body.
		//   - emptyBody    → at most one fixture with an empty request
		//                    body (validated above)
		// Methods that appear in more than one fixture are *not* kept
		// in byMethod — they would collide on second insert and the
		// second-loaded fixture would silently win. Demote them: every
		// colliding fixture is reachable only via byte-exact body match.
		type entry struct {
			reqBody []byte
			method  string
			resp    FakeResponse
		}
		entries := make([]entry, 0, len(fxs))
		methodCount := map[string]int{}
		for _, f := range fxs {
			respBody, _ := base64.StdEncoding.DecodeString(f.Response.BodyBase64)
			reqBody, _ := base64.StdEncoding.DecodeString(f.Request.BodyBase64)
			method := extractJSONRPCMethod(reqBody)
			if method != "" {
				methodCount[method]++
			}
			entries = append(entries, entry{
				reqBody: reqBody,
				method:  method,
				resp: FakeResponse{
					StatusCode: f.Response.Status,
					Headers:    f.Response.Headers,
					Body:       respBody,
				},
			})
		}
		byMethod := map[string]FakeResponse{}
		byBody := map[string]FakeResponse{}
		var emptyBody FakeResponse
		haveEmpty := false
		for _, e := range entries {
			switch {
			case len(e.reqBody) == 0:
				emptyBody = e.resp
				haveEmpty = true
			case e.method != "" && methodCount[e.method] == 1:
				byMethod[e.method] = e.resp
				byBody[string(e.reqBody)] = e.resp
			default:
				// Either non-JSON-RPC body, OR a JSON-RPC method that
				// appears in multiple fixtures. Body-exact is the only
				// safe matcher.
				byBody[string(e.reqBody)] = e.resp
			}
		}
		k.srv.SetDynamicHandler(k.method, k.path, func(r *http.Request) FakeResponse {
			b, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			// Byte-exact body match wins — never returns the wrong
			// fixture when the caller sent exactly what was recorded.
			if resp, ok := byBody[string(b)]; ok {
				return resp
			}
			// JSON-RPC method fallback only when a method has a single
			// owning fixture (collisions were demoted out of byMethod
			// above).
			if m := extractJSONRPCMethod(b); m != "" {
				if resp, ok := byMethod[m]; ok {
					return resp
				}
			}
			if len(b) == 0 && haveEmpty {
				return emptyBody
			}
			return FakeResponse{
				StatusCode: http.StatusNotFound,
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       []byte(`{"error":"no fixture matched request body"}`),
			}
		})
	}
}

// extractJSONRPCMethod returns the "method" field of a JSON-RPC 2.0
// request body, or "" if the body is not a JSON-RPC 2.0 request. The
// "jsonrpc" field is required by the spec — a body that merely has a
// "method" key (e.g. some application-level JSON shape that happens
// to use the same field name) does not get bucketed for method-level
// dispatch in SeedFromFixtures. Best-effort: returns "" on any JSON
// error so a non-RPC body falls through to byte-exact body matching.
func extractJSONRPCMethod(body []byte) string {
	var req struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	if req.JSONRPC == "" {
		return ""
	}
	return req.Method
}

// CompareResponse returns "" if the response matches the fixture
// (modulo the ignored headers), or a diagnostic string describing the
// first mismatch found.
func CompareResponse(fixture GoldenFixture, got *Response, ignoreHeaders []string) string {
	if got.StatusCode != fixture.Response.Status {
		return fmt.Sprintf("status: got %d want %d", got.StatusCode, fixture.Response.Status)
	}
	wantBody, _ := base64.StdEncoding.DecodeString(fixture.Response.BodyBase64)
	if string(got.Body) != string(wantBody) {
		return fmt.Sprintf("body mismatch:\n  got:  %q\n  want: %q", got.Body, wantBody)
	}
	ignored := map[string]bool{}
	for _, h := range ignoreHeaders {
		ignored[http.CanonicalHeaderKey(h)] = true
	}
	// Check each header from the fixture is present (and unchanged) on
	// the live response. Extra headers on the live side are tolerated —
	// cosmoguard legitimately adds X-Request-Id, Cache: miss, etc.
	for k, v := range fixture.Response.Headers {
		canon := http.CanonicalHeaderKey(k)
		if ignored[canon] {
			continue
		}
		gotV := got.Header.Get(canon)
		if gotV != v {
			return fmt.Sprintf("header %q: got %q want %q", canon, gotV, v)
		}
	}
	return ""
}
