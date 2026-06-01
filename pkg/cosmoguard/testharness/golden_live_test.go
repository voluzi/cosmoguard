package testharness

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
)

// TestK_GoldenLiveCompatibility verifies the project-wide 100%-Cosmos-
// compatibility invariant against the user's real Nibiru + Allora
// deployments: every request that works against the bare node MUST
// produce a byte-identical response (modulo documented non-determinism
// and a known header allowlist) when routed through cosmoguard.
//
// The test is opt-in — default CI stays hermetic. Enable with:
//
//	COSMOGUARD_GOLDEN_LIVE=1 go test ./pkg/cosmoguard/testharness/ -run TestK_GoldenLiveCompatibility -v
//
// Strategy: for each curated endpoint, hit the live node directly AND
// through an in-process cosmoguard pointed at the same live node (via
// the new per-service URL overrides). Both fetches run within ~1s so
// the node's mutable state (height, block time) doesn't drift.
// Endpoints known to embed timestamps/heights get their non-
// deterministic fields canonicalized out before comparison.
func TestK_GoldenLiveCompatibility(t *testing.T) {
	if os.Getenv("COSMOGUARD_GOLDEN_LIVE") != "1" {
		t.Skip("set COSMOGUARD_GOLDEN_LIVE=1 to run against the live Nibiru/Allora nodes")
	}

	// Quieter slog during live test runs — only surface errors so the
	// test output is the assertion log, not proxy chatter.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	cases := []liveChainCase{
		{
			Name: "nibiru",
			Node: cosmoguard.NodeConfig{
				Name:        "nibiru",
				LcdURL:      "https://lcd.nibiru.voluzi.com",
				RpcURL:      "https://rpc.nibiru.voluzi.com",
				GrpcURL:     "https://grpc.nibiru.voluzi.com",
				EvmRpcURL:   "https://evm-rpc.nibiru.voluzi.com",
				EvmRpcWsURL: "https://evm-rpc-ws.nibiru.voluzi.com",
			},
			EnableEVM: true,
		},
		{
			Name: "allora",
			Node: cosmoguard.NodeConfig{
				Name:    "allora",
				LcdURL:  "https://lcd.allora.voluzi.com",
				RpcURL:  "https://rpc.allora.voluzi.com",
				GrpcURL: "https://grpc.allora.voluzi.com",
			},
			EnableEVM: false,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			runLiveChainCase(t, c)
		})
	}
}

type liveChainCase struct {
	Name      string
	Node      cosmoguard.NodeConfig
	EnableEVM bool
}

// runLiveChainCase boots a cosmoguard pointed at the chain's live
// endpoints, then walks LCD + RPC + JSON-RPC + EVM endpoints asserting
// direct == via-proxy.
func runLiveChainCase(t *testing.T, c liveChainCase) {
	t.Helper()

	// Pre-flight: the upstream must answer /status. If the user's node
	// is down we get a clear early failure instead of a stew of 502s.
	if err := preflight(c.Node.RpcURL + "/status"); err != nil {
		t.Skipf("preflight %s: %v", c.Node.RpcURL, err)
	}

	cg, urls := bootCosmoguardForLive(t, c)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = cg.Shutdown(ctx)
		cancel()
	}()

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			// System trust roots; same as cosmoguard's outbound transport.
			TLSClientConfig: &tls.Config{},
		},
	}

	// --- LCD GETs ---
	lcdEndpoints := []liveEndpoint{
		{Path: "/cosmos/base/tendermint/v1beta1/node_info", Stable: true},
		{Path: "/cosmos/bank/v1beta1/params", Stable: true},
		{Path: "/cosmos/staking/v1beta1/params", Stable: true},
		{Path: "/cosmos/auth/v1beta1/params", Stable: true},
		{Path: "/cosmos/slashing/v1beta1/params", Stable: true},
	}
	for _, e := range lcdEndpoints {
		runLiveGetCase(t, httpClient, "LCD "+e.Path, c.Node.LcdURL+e.Path, urls.LCD+e.Path, e)
	}

	// --- RPC GETs (Tendermint over HTTP) ---
	rpcEndpoints := []liveEndpoint{
		{Path: "/health", Stable: true},
		// /status embeds the live block height + timestamps → compare
		// only deterministic substructure.
		{Path: "/status", Stable: false, JSONIgnore: []string{
			"result.sync_info.latest_block_height",
			"result.sync_info.latest_block_hash",
			"result.sync_info.latest_app_hash",
			"result.sync_info.latest_block_time",
			"result.sync_info.earliest_block_time",
			"result.sync_info.catching_up",
			"id",
		}},
		{Path: "/abci_info", Stable: false, JSONIgnore: []string{
			"result.response.last_block_height",
			"result.response.last_block_app_hash",
			"id",
		}},
	}
	for _, e := range rpcEndpoints {
		runLiveGetCase(t, httpClient, "RPC "+e.Path, c.Node.RpcURL+e.Path, urls.RPC+e.Path, e)
	}

	// --- JSON-RPC POSTs (Tendermint over JSON-RPC) ---
	jsonRPCCases := []liveJSONRPCCase{
		{
			Name: "abci_info",
			Body: `{"jsonrpc":"2.0","id":1,"method":"abci_info","params":[]}`,
			Endpoint: liveEndpoint{JSONIgnore: []string{
				"result.response.last_block_height",
				"result.response.last_block_app_hash",
				"id",
			}},
		},
	}
	for _, jc := range jsonRPCCases {
		runLivePostCase(t, httpClient, "JSON-RPC "+jc.Name,
			c.Node.RpcURL, urls.RPC, []byte(jc.Body),
			"application/json", jc.Endpoint)
	}

	// --- EVM JSON-RPC (Nibiru only) ---
	if c.EnableEVM {
		evmCases := []liveJSONRPCCase{
			{Name: "eth_chainId",
				Body:     `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`,
				Endpoint: liveEndpoint{Stable: true, JSONIgnore: []string{"id"}}},
			{Name: "net_version",
				Body:     `{"jsonrpc":"2.0","id":1,"method":"net_version","params":[]}`,
				Endpoint: liveEndpoint{Stable: true, JSONIgnore: []string{"id"}}},
			{Name: "eth_blockNumber",
				Body:     `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
				Endpoint: liveEndpoint{JSONIgnore: []string{"result", "id"}}},
		}
		for _, ec := range evmCases {
			runLivePostCase(t, httpClient, "EVM "+ec.Name,
				c.Node.EvmRpcURL, urls.EVM, []byte(ec.Body),
				"application/json", ec.Endpoint)
		}
	}

	// --- gRPC ---
	runLiveGRPCCase(t, c.Node, urls.GRPC)
}

// liveEndpoint configures one comparison case.
type liveEndpoint struct {
	Path string
	// Stable=true asserts strict byte-identical body. Stable=false
	// requires the test to fall back to JSON-shape comparison with
	// the JSONIgnore key set masked out.
	Stable bool
	// JSONIgnore is a list of dotted JSON paths whose values are
	// blanked before structural comparison (only consulted when
	// Stable=false). "result.sync_info.latest_block_height" zeros
	// that nested field, etc.
	JSONIgnore []string
}

type liveJSONRPCCase struct {
	Name     string
	Body     string
	Endpoint liveEndpoint
}

// runLiveGetCase fetches the same path direct + through cosmoguard
// (concurrently, to minimize mutable-state drift) and asserts the
// responses match per the endpoint's stability profile.
func runLiveGetCase(t *testing.T, client *http.Client, label, directURL, proxyURL string, e liveEndpoint) {
	t.Helper()
	direct, proxy := fetchTwice(t, client, label, "GET", directURL, proxyURL, "", nil)
	assertLiveMatch(t, label, direct, proxy, e)
}

func runLivePostCase(t *testing.T, client *http.Client, label, directURL, proxyURL string, body []byte, contentType string, e liveEndpoint) {
	t.Helper()
	direct, proxy := fetchTwice(t, client, label, "POST", directURL, proxyURL, contentType, body)
	assertLiveMatch(t, label, direct, proxy, e)
}

// fetchTwice issues the request to both targets in parallel and
// returns (direct, proxy) responses. The two requests fire ~together
// so block-time / height drift between them is bounded to network
// latency.
func fetchTwice(t *testing.T, client *http.Client, label, method, directURL, proxyURL, contentType string, body []byte) (*liveResp, *liveResp) {
	t.Helper()
	type result struct {
		r   *liveResp
		err error
	}
	directCh := make(chan result, 1)
	proxyCh := make(chan result, 1)
	go func() {
		r, err := doLive(client, method, directURL, contentType, body)
		directCh <- result{r, err}
	}()
	go func() {
		r, err := doLive(client, method, proxyURL, contentType, body)
		proxyCh <- result{r, err}
	}()
	d := <-directCh
	p := <-proxyCh
	if d.err != nil {
		t.Fatalf("%s: direct fetch %s: %v", label, directURL, d.err)
	}
	if p.err != nil {
		t.Fatalf("%s: proxy fetch %s: %v", label, proxyURL, p.err)
	}
	return d.r, p.r
}

type liveResp struct {
	Status  int
	Headers http.Header
	Body    []byte
}

func doLive(client *http.Client, method, target, contentType string, body []byte) (*liveResp, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, target, bodyReader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &liveResp{Status: resp.StatusCode, Headers: resp.Header, Body: b}, nil
}

// assertLiveMatch compares the two responses by status + body shape.
// Header comparison is intentionally narrow: only Content-Type is
// asserted to match (cosmoguard owns Date, X-Request-Id,
// X-Cosmoguard-Cache and may legitimately add headers the bare node
// never set).
func assertLiveMatch(t *testing.T, label string, direct, proxy *liveResp, e liveEndpoint) {
	t.Helper()
	if direct.Status != proxy.Status {
		t.Errorf("%s: status mismatch direct=%d proxy=%d", label, direct.Status, proxy.Status)
		return
	}
	if got, want := proxy.Headers.Get("Content-Type"), direct.Headers.Get("Content-Type"); got != want {
		t.Errorf("%s: content-type mismatch direct=%q proxy=%q", label, want, got)
	}
	// Content-Length parity catches re-serialization (e.g. cosmoguard
	// accidentally buffers + re-encodes a chunked response into a
	// known-length one or vice versa). Only enforce when BOTH sides
	// advertise one — neither one being set is fine.
	if cl1, cl2 := direct.Headers.Get("Content-Length"), proxy.Headers.Get("Content-Length"); cl1 != "" && cl2 != "" && cl1 != cl2 {
		t.Errorf("%s: content-length mismatch direct=%s proxy=%s", label, cl1, cl2)
	}
	if e.Stable {
		if !bytes.Equal(direct.Body, proxy.Body) {
			t.Errorf("%s: body mismatch (stable endpoint)\n  direct: %s\n  proxy:  %s",
				label, summarize(direct.Body), summarize(proxy.Body))
		}
		return
	}
	// Non-stable: structural compare with the ignore set masked out.
	directJSON, err := unmarshalRelaxed(direct.Body)
	if err != nil {
		t.Errorf("%s: direct body not JSON: %v (body=%s)", label, err, summarize(direct.Body))
		return
	}
	proxyJSON, err := unmarshalRelaxed(proxy.Body)
	if err != nil {
		t.Errorf("%s: proxy body not JSON: %v (body=%s)", label, err, summarize(proxy.Body))
		return
	}
	maskJSONPaths(directJSON, e.JSONIgnore)
	maskJSONPaths(proxyJSON, e.JSONIgnore)
	if !reflect.DeepEqual(directJSON, proxyJSON) {
		dbytes, _ := json.MarshalIndent(directJSON, "", "  ")
		pbytes, _ := json.MarshalIndent(proxyJSON, "", "  ")
		t.Errorf("%s: structural body mismatch\n  direct: %s\n  proxy:  %s",
			label, dbytes, pbytes)
	}
}

func summarize(b []byte) string {
	const max = 240
	s := string(b)
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

func unmarshalRelaxed(b []byte) (any, error) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// maskJSONPaths sets each dotted path in v to nil so deep-equal can
// ignore the value. Missing intermediates are silently skipped.
func maskJSONPaths(v any, paths []string) {
	for _, p := range paths {
		maskOne(v, strings.Split(p, "."))
	}
}

func maskOne(v any, segs []string) {
	if len(segs) == 0 || v == nil {
		return
	}
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	if len(segs) == 1 {
		if _, exists := m[segs[0]]; exists {
			m[segs[0]] = nil
		}
		return
	}
	next, exists := m[segs[0]]
	if !exists {
		return
	}
	maskOne(next, segs[1:])
}

// liveProxyURLs collects the loopback URLs the live test routes through.
type liveProxyURLs struct {
	LCD  string
	RPC  string
	EVM  string
	GRPC string // host:port for gRPC dial
}

// bootCosmoguardForLive boots an in-process cosmoguard configured to
// forward to the live node over TLS. Allow-all default with no rules
// — the test asserts compatibility, not gating.
func bootCosmoguardForLive(t *testing.T, c liveChainCase) (*cosmoguard.CosmoGuard, liveProxyURLs) {
	t.Helper()
	lcdPort := freePort(t)
	rpcPort := freePort(t)
	grpcPort := freePort(t)
	evmRpcPort := 0
	if c.EnableEVM {
		evmRpcPort = freePort(t)
	}
	cfg := &cosmoguard.Config{
		Host:         "127.0.0.1",
		LcdPort:      lcdPort,
		RpcPort:      rpcPort,
		GrpcPort:     grpcPort,
		EnableEvm:    c.EnableEVM,
		EvmRpcPort:   evmRpcPort,
		EvmRpcWsPort: freePort(t),
		Nodes:        []cosmoguard.NodeConfig{c.Node},
		Metrics:      cosmoguard.MetricsConfig{Enable: false},
		LCD:          cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC: cosmoguard.RpcConfig{
			Default:              cosmoguard.RuleActionAllow,
			WebSocketEnabled:     false, // we don't exercise WS in this test
			WebSocketConnections: 1,
			JsonRpc:              cosmoguard.JsonRpcConfig{Default: cosmoguard.RuleActionAllow},
		},
		GRPC: cosmoguard.GrpcConfig{Default: cosmoguard.RuleActionAllow},
	}
	if c.EnableEVM {
		cfg.EVM = cosmoguard.EvmConfig{
			RPC: cosmoguard.EvmRpcConfig{Default: cosmoguard.RuleActionAllow},
			WS:  cosmoguard.EvmRpcWsConfig{Default: cosmoguard.RuleActionAllow, WebSocketConnections: 1},
		}
	}
	cg, err := cosmoguard.New(cfg)
	if err != nil {
		t.Fatalf("live: cosmoguard.New: %v", err)
	}
	// Capture cg.Run() error so a config-time or listener-bind failure
	// surfaces with a clear message instead of cascading into "port
	// never opened". We never block on this channel — the goroutine
	// closes it on exit; either a port comes up and we move on, or
	// waitPortListen times out and we report a Run() error if one
	// landed.
	runErrCh := make(chan error, 1)
	go func() {
		defer close(runErrCh)
		if err := cg.Run(); err != nil {
			runErrCh <- err
		}
	}()
	// Wait for listeners.
	deadline := time.Now().Add(5 * time.Second)
	ports := []int{lcdPort, rpcPort, grpcPort}
	if c.EnableEVM {
		ports = append(ports, evmRpcPort)
	}
	for _, p := range ports {
		if !waitPortListen(p, deadline) {
			select {
			case rerr := <-runErrCh:
				if rerr != nil {
					t.Fatalf("live: cosmoguard.Run failed: %v", rerr)
				}
			default:
			}
			t.Fatalf("live: cosmoguard port %d never opened", p)
		}
	}
	urls := liveProxyURLs{
		LCD:  fmt.Sprintf("http://127.0.0.1:%d", lcdPort),
		RPC:  fmt.Sprintf("http://127.0.0.1:%d", rpcPort),
		GRPC: fmt.Sprintf("127.0.0.1:%d", grpcPort),
	}
	if c.EnableEVM {
		urls.EVM = fmt.Sprintf("http://127.0.0.1:%d", evmRpcPort)
	}
	return cg, urls
}

func preflight(target string) error {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(target)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("upstream %s returned %d", target, resp.StatusCode)
	}
	return nil
}

// --- gRPC live check ---

// runLiveGRPCCase dials the live gRPC endpoint directly AND through
// cosmoguard's gRPC proxy and asserts that both invocations of
// cosmos.bank.v1beta1.Query/Params return identical payloads. This is
// the gRPC analog of the HTTP byte-identical assertion. Uses the
// rawCodec via a generic Invoke so we never have to compile the proto
// descriptors into the test binary.
func runLiveGRPCCase(t *testing.T, n cosmoguard.NodeConfig, proxyAddr string) {
	t.Helper()
	const method = "/cosmos.bank.v1beta1.Query/Params"

	// Direct dial: parse the GrpcURL ourselves so we use the same
	// scheme→credentials mapping cosmoguard does internally.
	directTarget, directCreds, err := liveGrpcTarget(n.GrpcURL)
	if err != nil {
		t.Fatalf("grpc direct target: %v", err)
	}
	directConn, err := grpc.NewClient(directTarget,
		grpc.WithTransportCredentials(directCreds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawBytesCodec{})),
	)
	if err != nil {
		t.Fatalf("grpc dial direct %s: %v", directTarget, err)
	}
	defer directConn.Close()

	proxyConn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawBytesCodec{})),
	)
	if err != nil {
		t.Fatalf("grpc dial proxy %s: %v", proxyAddr, err)
	}
	defer proxyConn.Close()

	// Empty QueryParamsRequest — its protobuf encoding is zero bytes.
	req := rawBytes(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx)

	var directOut rawBytes
	if err := directConn.Invoke(ctx, method, req, &directOut); err != nil {
		t.Fatalf("grpc direct invoke: %v", err)
	}
	var proxyOut rawBytes
	if err := proxyConn.Invoke(ctx, method, req, &proxyOut); err != nil {
		t.Fatalf("grpc proxy invoke: %v", err)
	}
	if !bytes.Equal(directOut, proxyOut) {
		t.Errorf("gRPC %s: response bytes differ\n  direct len=%d %x\n  proxy  len=%d %x",
			method, len(directOut), directOut[:min(len(directOut), 64)],
			len(proxyOut), proxyOut[:min(len(proxyOut), 64)])
	}
}

// liveGrpcTarget mirrors upstreamGRPCTarget in the main package
// (which we can't import from the test of the testharness because of
// the public/internal split) — kept tiny on purpose.
func liveGrpcTarget(raw string) (string, credentials.TransportCredentials, error) {
	if raw == "" {
		return "", nil, fmt.Errorf("empty GrpcURL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", nil, err
	}
	host := u.Host
	useTLS := false
	switch u.Scheme {
	case "https", "grpcs", "tls":
		useTLS = true
	case "http", "grpc":
		useTLS = false
	default:
		return "", nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
		if useTLS {
			host = net.JoinHostPort(host, "443")
		} else {
			host = net.JoinHostPort(host, "80")
		}
	}
	if useTLS {
		return host, credentials.NewTLS(&tls.Config{ServerName: u.Hostname()}), nil
	}
	return host, insecure.NewCredentials(), nil
}

// rawBytes + rawBytesCodec let us call any gRPC method without
// generated stubs. The codec returns the wire payload byte-for-byte —
// perfect for a "did cosmoguard alter anything?" check.
type rawBytes []byte

func (r *rawBytes) Reset()         { *r = (*r)[:0] }
func (r *rawBytes) String() string { return fmt.Sprintf("rawBytes(len=%d)", len(*r)) }
func (r *rawBytes) ProtoMessage()  {}

type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v any) ([]byte, error) {
	if b, ok := v.(rawBytes); ok {
		return []byte(b), nil
	}
	if b, ok := v.(*rawBytes); ok {
		return []byte(*b), nil
	}
	return nil, fmt.Errorf("rawBytesCodec.Marshal: unsupported %T", v)
}

func (rawBytesCodec) Unmarshal(data []byte, v any) error {
	if b, ok := v.(*rawBytes); ok {
		*b = append((*b)[:0], data...)
		return nil
	}
	return fmt.Errorf("rawBytesCodec.Unmarshal: unsupported %T", v)
}

func (rawBytesCodec) Name() string { return "raw_bytes_compat" }
