package cosmoguard

import (
	"bufio"
	"crypto/tls"
	stdjson "encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type upstreamWireRequest struct {
	Method, URL, Host, Protocol, SourceIP, Body string
	Header, Trailer                             http.Header
	ContentLength                               int64
	TransferEncoding                            []string
}

func TestUpstreamRequestWireContract(t *testing.T) {
	previous := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(previous) })
	require.NoError(t, SetTrustedProxies([]string{"10.0.0.0/8"}))
	expected := map[string]upstreamWireRequest{}
	data, err := os.ReadFile("testdata/upstream_request_wire.json")
	require.NoError(t, err)
	require.NoError(t, stdjson.Unmarshal(data, &expected))
	cases := []struct {
		name, method, target, body, remote, headers, upstreamPath string
		parsed, override                                          bool
	}{
		{name: "get", method: "GET", target: "/status"},
		{name: "incoming https", method: "GET", target: "/status"},
		{name: "non-address peer", method: "GET", target: "/status", remote: "unknown", headers: "X-Forwarded-For: 203.0.113.7\r\n"},
		{name: "nil forwarding", method: "GET", target: "/status"},
		{name: "post", method: "POST", target: "/submit?x=1", body: "hello", headers: "Content-Type: text/plain\r\nX-Multi: one\r\nX-Multi: two\r\n"},
		{name: "untrusted forwarding", method: "GET", target: "/status", remote: "192.0.2.9:1234", headers: "X-Forwarded-For: 203.0.113.7, 198.51.100.3\r\nX-Forwarded-For: 10.0.0.1\r\nX-Forwarded-Host: spoofed.example\r\nX-Forwarded-Proto: https\r\nForwarded: for=203.0.113.7;proto=https\r\nVia: 1.1 edge\r\nX-Real-Ip: 203.0.113.8\r\n"},
		{name: "trusted forwarding", method: "GET", target: "/status", remote: "10.0.0.2:1234", headers: "X-Forwarded-For: 203.0.113.7, 198.51.100.3\r\nX-Forwarded-For: 10.0.0.1\r\nForwarded: for=203.0.113.7;proto=https\r\nVia: 1.1 edge\r\n"},
		{name: "override host and escaped path", method: "GET", target: "/a%2Fb?q=%2f+%20&x=1&x=2", override: true, upstreamPath: "/base%2Fpath?fixed=%2f"},
		{name: "upgrade", method: "GET", target: "/websocket", headers: "Connection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nTe: gzip, trailers\r\n"},
		{name: "hop headers", method: "GET", target: "/status", headers: "Connection: keep-alive, X-Remove\r\nX-Remove: private\r\nKeep-Alive: timeout=5\r\nProxy-Connection: keep-alive\r\nProxy-Authenticate: Basic\r\nProxy-Authorization: Basic Zm9vOmJhcg==\r\nTe: gzip, trailers\r\nTrailer: X-Foo\r\nUpgrade: unused\r\n"},
		{name: "forwarding named in connection", method: "GET", target: "/status", headers: "Connection: X-Forwarded-For, X-Forwarded-Host, X-Forwarded-Proto, Forwarded, Via\r\nX-Forwarded-For: 203.0.113.7\r\nForwarded: for=203.0.113.7\r\nVia: 1.1 edge\r\n"},
		{name: "odd query", method: "GET", target: "/status?bad=%zz&semi=a;b&ok=%2f+%20&dup=1&dup=2&empty="},
		{name: "parsed valid query", method: "GET", target: "/status?ok=%2f+%20&dup=1&dup=2", parsed: true},
		{name: "parsed odd query", method: "GET", target: "/status?bad=%zz&semi=a;b&ok=%2f+%20&dup=1&dup=2&empty=", parsed: true},
		{name: "http 1.0", method: "GET", target: "/status", headers: "Connection: close\r\n"},
		{name: "trailers", method: "POST", target: "/submit", body: "5\r\nhello\r\n0\r\nX-Checksum: done\r\n\r\n", headers: "Transfer-Encoding: chunked\r\nTrailer: X-Checksum\r\nTe: trailers\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan upstreamWireRequest, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, readErr := io.ReadAll(r.Body)
				if readErr != nil {
					t.Error(readErr)
				}
				received <- upstreamWireRequest{Method: r.Method, URL: r.URL.String(), Host: r.Host, Protocol: r.Proto, Header: r.Header.Clone(), Trailer: r.Trailer.Clone(), Body: string(body), ContentLength: r.ContentLength, TransferEncoding: r.TransferEncoding}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer upstream.Close()
			target, err := url.Parse(upstream.URL)
			require.NoError(t, err)
			host, portText, err := net.SplitHostPort(target.Host)
			require.NoError(t, err)
			port, err := strconv.Atoi(portText)
			require.NoError(t, err)
			node := NodeConfig{Name: "wire", Host: host, RpcPort: port}
			if tc.override {
				node.RpcURL = upstream.URL + tc.upstreamPath
			}
			u, err := buildHttpUpstream(node, serviceRPC, nil)
			require.NoError(t, err)
			protocol := "HTTP/1.1"
			if tc.name == "http 1.0" {
				protocol = "HTTP/1.0"
			}
			headers := tc.headers
			if tc.body != "" && tc.name != "trailers" {
				headers += "Content-Length: " + strconv.Itoa(len(tc.body)) + "\r\n"
			}
			req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(tc.method + " " + tc.target + " " + protocol + "\r\nHost: rpc.public.example:8080\r\n" + headers + "\r\n" + tc.body)))
			require.NoError(t, err)
			req.RemoteAddr = tc.remote
			if req.RemoteAddr == "" {
				req.RemoteAddr = "192.0.2.1:1234"
			}
			if tc.parsed {
				_ = req.ParseForm()
			}
			if tc.name == "incoming https" {
				req.TLS = &tls.ConnectionState{}
			}
			if tc.name == "nil forwarding" {
				req.Header["X-Forwarded-For"] = nil
			}
			source := GetSourceIP(req)
			response := httptest.NewRecorder()
			u.proxy.ServeHTTP(response, req.WithContext(t.Context()))
			require.Equal(t, http.StatusNoContent, response.Code)
			got := <-received
			if got.Host == target.Host {
				got.Host = "upstream.example:port"
			}
			got.SourceIP = source
			require.Equal(t, expected[tc.name], got)
		})
	}
	require.Len(t, expected, len(cases))
}

func TestUpstreamDropsUnparseableQueryParameters(t *testing.T) {
	auth, err := NewAuthenticator(&AuthConfig{Enable: true,
		Methods:    []AuthMethodConfig{{Type: "api-key", QueryParam: "api_key"}},
		Identities: []IdentityConfig{{Name: "client", APIKey: "secret"}},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = auth.Close() })
	for _, tc := range []struct {
		name, query, want string
		strip             bool
	}{
		{name: "bad escape", query: "height=42&uninspected=%zz&keep=ok", want: "height=42&keep=ok"},
		{name: "semicolon", query: "height=42&uninspected=one;admin=true&keep=ok", want: "height=42&keep=ok"},
		{name: "credential and bad escape", query: "height=42&api_key=secret&uninspected=%zz", want: "height=42", strip: true},
		{name: "credential and semicolon", query: "height=42&api_key=secret&uninspected=one;admin=true", want: "height=42", strip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received <- r.URL.RawQuery
				w.WriteHeader(http.StatusNoContent)
			}))
			defer upstream.Close()
			target := upstream.URL
			var hook func(*http.Request)
			if tc.strip {
				hook = auth.StripCredentialQuery
			}
			u, err := buildHttpUpstream(NodeConfig{Name: "query", RpcURL: target}, serviceRPC, hook)
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodGet, "/probe?"+tc.query, nil)
			require.False(t, request.URL.Query().Has("uninspected"))
			response := httptest.NewRecorder()
			u.proxy.ServeHTTP(response, request.WithContext(t.Context()))
			require.Equal(t, http.StatusNoContent, response.Code)
			require.Equal(t, tc.want, <-received)
		})
	}
}
