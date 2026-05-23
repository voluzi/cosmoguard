package cosmoguard

import (
	"net/url"
	"strings"
	"testing"
)

// TestUpstreamHTTPURL_TLSFlag: with n.TLS=true and no override, the
// built URL should use https with the configured port.
func TestUpstreamHTTPURL_TLSFlag(t *testing.T) {
	n := NodeConfig{Host: "node.example.com", LcdPort: 1317, TLS: true}
	u, err := upstreamHTTPURL(n, serviceLCD)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if u.Scheme != "https" {
		t.Fatalf("scheme=%s want https", u.Scheme)
	}
	if u.Host != "node.example.com:1317" {
		t.Fatalf("host=%s want node.example.com:1317", u.Host)
	}
}

// TestUpstreamHTTPURL_OverrideWinsOverTLSFlag: an explicit lcdURL takes
// precedence even when n.TLS is set, and its path is stripped so the
// proxy preserves the inbound request path.
func TestUpstreamHTTPURL_OverrideWinsOverTLSFlag(t *testing.T) {
	n := NodeConfig{
		Host: "ignored", LcdPort: 1317, TLS: true,
		LcdURL: "https://lcd.nibiru.voluzi.com/somepath",
	}
	u, err := upstreamHTTPURL(n, serviceLCD)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if u.Scheme != "https" {
		t.Fatalf("scheme=%s want https", u.Scheme)
	}
	if u.Host != "lcd.nibiru.voluzi.com" {
		t.Fatalf("host=%s want lcd.nibiru.voluzi.com", u.Host)
	}
	if u.Path != "" {
		t.Fatalf("path=%s want empty (path is preserved from inbound request)", u.Path)
	}
}

func TestUpstreamHTTPURL_BadSchemeRejected(t *testing.T) {
	n := NodeConfig{LcdURL: "ws://nope.example.com"}
	_, err := upstreamHTTPURL(n, serviceLCD)
	if err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Fatalf("expected scheme error; got %v", err)
	}
}

// TestNodeWSURL_HTTPSMapsToWSS: when an operator points their RPC at
// https://… (so all five overrides can share one hostname), the WS
// helper maps the scheme to wss automatically.
func TestNodeWSURL_HTTPSMapsToWSS(t *testing.T) {
	n := NodeConfig{RpcURL: "https://rpc.example.com"}
	u, err := nodeWSURL(n, serviceRPC)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if u.Scheme != "wss" {
		t.Fatalf("scheme=%s want wss", u.Scheme)
	}
}

// TestNodeWSBackend_ProducesStringURL: the helper must emit a string
// suitable to hand straight to NewUpstreamPool (scheme + host, no
// trailing slash).
func TestNodeWSBackend_ProducesStringURL(t *testing.T) {
	n := NodeConfig{Host: "10.0.0.1", RpcPort: 26657, TLS: false}
	got, err := nodeWSBackend(n, serviceRPC)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "ws://10.0.0.1:26657" {
		t.Fatalf("backend=%s want ws://10.0.0.1:26657", got)
	}
}

func TestNodeWSBackend_TLS(t *testing.T) {
	n := NodeConfig{Host: "node.example.com", EvmRpcWsPort: 8546, TLS: true}
	got, err := nodeWSBackend(n, serviceEVMRPCWS)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "wss://node.example.com:8546" {
		t.Fatalf("backend=%s want wss://node.example.com:8546", got)
	}
}

// TestUpstreamGRPCTarget_HTTPSOverrideUsesTLS: an https://… GrpcURL
// must dial with TLS credentials and default to port 443 when none
// is in the host.
func TestUpstreamGRPCTarget_HTTPSOverrideUsesTLS(t *testing.T) {
	n := NodeConfig{GrpcURL: "https://grpc.nibiru.voluzi.com"}
	target, creds, err := upstreamGRPCTarget(n)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target != "grpc.nibiru.voluzi.com:443" {
		t.Fatalf("target=%s want grpc.nibiru.voluzi.com:443", target)
	}
	if creds == nil || creds.Info().SecurityProtocol != "tls" {
		t.Fatalf("expected TLS credentials, got %+v", creds)
	}
}

func TestUpstreamGRPCTarget_PlaintextFallback(t *testing.T) {
	n := NodeConfig{Host: "10.0.0.1", GrpcPort: 9090}
	target, creds, err := upstreamGRPCTarget(n)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if target != "10.0.0.1:9090" {
		t.Fatalf("target=%s want 10.0.0.1:9090", target)
	}
	if creds == nil {
		t.Fatal("expected insecure credentials, got nil")
	}
	if creds.Info().SecurityProtocol == "tls" {
		t.Fatalf("expected plaintext credentials, got TLS")
	}
}

func TestUpstreamGRPCTarget_TLSFlagWithoutOverride(t *testing.T) {
	n := NodeConfig{Host: "grpc.example.com", GrpcPort: 443, TLS: true}
	_, creds, err := upstreamGRPCTarget(n)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if creds == nil || creds.Info().SecurityProtocol != "tls" {
		t.Fatalf("expected TLS credentials, got %+v", creds)
	}
}

// TestPrepareConfig_RejectsBadOverrideURL: a malformed override should
// fail PrepareConfig with a clear field name in the error.
func TestPrepareConfig_RejectsBadOverrideURL(t *testing.T) {
	cfg := &Config{
		Nodes: []NodeConfig{{Name: "n0", RpcURL: "://bad"}},
	}
	err := PrepareConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "rpcURL") {
		t.Fatalf("expected rpcURL error; got %v", err)
	}
}

// TestParseUpstreamOverride_RejectsUserinfo: an override with embedded
// credentials is silently dropped by the rest of the URL-building
// pipeline (we only carry Scheme + Host on the output). Rather than
// shipping auth into the void, reject at config load so the operator
// sees an immediate, named error and knows to use the auth: stanza.
func TestParseUpstreamOverride_RejectsUserinfo(t *testing.T) {
	cfg := &Config{
		Nodes: []NodeConfig{{Name: "n0", LcdURL: "https://alice:secret@lcd.example.com"}},
	}
	err := PrepareConfig(cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("expected `userinfo` in error message; got %v", err)
	}
}

// TestHostHeaderForTarget_StripsDefaultPort: the Host header on the
// outbound request must drop :443 / :80 suffixes to keep TLS edge
// gateways from 421-rejecting requests whose Host doesn't match the
// cert SAN literally. Non-default ports stay, so direct ip:port
// deployments still route correctly.
func TestHostHeaderForTarget_StripsDefaultPort(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"https://lcd.nibiru.voluzi.com:443", "lcd.nibiru.voluzi.com"},
		{"http://lcd.example:80", "lcd.example"},
		{"https://lcd.example", "lcd.example"},
		{"http://10.0.0.1:1317", "10.0.0.1:1317"},
		{"https://10.0.0.1:8443", "10.0.0.1:8443"},
		{"http://node:26657", "node:26657"},
		// Scheme comparison is case-insensitive per RFC 3986 §3.1; the
		// switch must accept uppercase HTTPS / HTTP without leaving
		// the :443 / :80 suffix on the Host header.
		{"HTTPS://lcd.example:443", "lcd.example"},
		{"HTTP://lcd.example:80", "lcd.example"},
		// IPv6 literals: when the port is the default, the bracketed
		// form must be preserved on the Host header (RFC 7230 §5.4).
		{"https://[2001:db8::1]:443", "[2001:db8::1]"},
		{"http://[::1]:80", "[::1]"},
		// IPv6 with non-default port keeps Host verbatim (already
		// bracketed by url.URL.Host).
		{"https://[2001:db8::1]:8443", "[2001:db8::1]:8443"},
	}
	for _, tc := range cases {
		u, err := url.Parse(tc.raw)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", tc.raw, err)
		}
		if got := hostHeaderForTarget(u); got != tc.want {
			t.Errorf("hostHeaderForTarget(%q)=%q want %q", tc.raw, got, tc.want)
		}
	}
}

// TestPrepareConfig_RejectsIncompatibleScheme: per-service scheme is
// enforced at config load — a `lcdURL: ws://…` should fail with a
// scheme-named error before the LCD pool ever sees it (otherwise the
// operator would only learn at first inbound request).
func TestPrepareConfig_RejectsIncompatibleScheme(t *testing.T) {
	cases := []struct {
		name   string
		node   NodeConfig
		needle string
	}{
		{
			name:   "lcdURL refuses ws",
			node:   NodeConfig{Name: "n0", LcdURL: "ws://lcd.example"},
			needle: "lcdURL",
		},
		{
			name:   "evmRpcWsURL accepts ws/wss/http/https only",
			node:   NodeConfig{Name: "n0", EvmRpcWsURL: "grpcs://ws.example"},
			needle: "evmRpcWsURL",
		},
		{
			name:   "grpcURL refuses ws",
			node:   NodeConfig{Name: "n0", GrpcURL: "ws://grpc.example"},
			needle: "grpcURL",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Nodes: []NodeConfig{tc.node}}
			err := PrepareConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.needle) {
				t.Fatalf("expected %q in error; got %v", tc.needle, err)
			}
			if !strings.Contains(err.Error(), "scheme") {
				t.Fatalf("expected `scheme` in error message; got %v", err)
			}
		})
	}
}
