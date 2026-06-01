package cosmoguard

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// service identifiers used by upstreamHTTPURL / nodeWSURL /
// upstreamGRPCTarget. Centralized so a typo at a call site is a compile
// error rather than a silent fall-through to "default".
const (
	serviceLCD       = "lcd"
	serviceRPC       = "rpc"
	serviceEVMRPC    = "evm_rpc"
	serviceEVMRPCWS  = "evm_rpc_ws"
	serviceGRPC      = "grpc"
	defaultHTTPSPort = 443
	defaultHTTPPort  = 80
)

// parseUpstreamOverride parses an operator-supplied URL override (one of
// NodeConfig.RpcURL / LcdURL / GrpcURL / EvmRpcURL / EvmRpcWsURL),
// requiring scheme + host. Per-service scheme validity (https vs grpcs
// etc.) is enforced by the service-specific helpers — this layer only
// rejects unparseable / structurally-invalid URLs.
func parseUpstreamOverride(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("URL %q missing scheme (need e.g. https://...)", raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("URL %q missing host", raw)
	}
	// userinfo (user[:password]@host) is silently dropped by the rest of
	// the upstream-building pipeline — we only carry Scheme + Host on
	// the returned URL. Better to reject at config load than to leak a
	// surprise "auth headers don't reach upstream" debug session.
	if u.User != nil {
		return nil, fmt.Errorf("URL %q must not include userinfo; configure auth via the auth: stanza instead", raw)
	}
	// Normalize the scheme to lower-case once so every downstream
	// switch / comparison is case-insensitive without each call site
	// having to remember. RFC 3986 § 3.1 declares schemes case-
	// insensitive; url.Parse doesn't normalize.
	u.Scheme = strings.ToLower(u.Scheme)
	return u, nil
}

// upstreamHTTPURL returns the upstream URL for an HTTP-shaped service
// (lcd / rpc / evm_rpc). Honors per-service URL overrides first, then
// healthcheckProbeURL builds the GET URL for an active healthcheck from
// a probe target (scheme+host) and the operator-supplied path. The path
// is normalized through url.URL so a missing leading "/" (e.g. "status"
// instead of "/status") doesn't produce "http://hoststatus", and a path
// carrying a query string is split into RawQuery instead of being pasted
// as a literal path substring. An empty path resolves to the upstream's
// root. Shared by the HTTP and gRPC pool builders so both normalize
// identically (the gRPC builder previously string-concatenated the path
// and broke probes for unslashed paths).
func healthcheckProbeURL(scheme, host, path string) string {
	u := url.URL{Scheme: scheme, Host: host}
	if path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		if i := strings.IndexByte(path, '?'); i >= 0 {
			u.Path = path[:i]
			u.RawQuery = path[i+1:]
		} else {
			u.Path = path
		}
	}
	return u.String()
}

// the node's TLS flag, then defaults to plaintext http.
//
// The returned URL has Scheme + Host populated; Path is left empty
// because httputil.NewSingleHostReverseProxy preserves the inbound
// path on the outgoing request.
func upstreamHTTPURL(n NodeConfig, service string) (*url.URL, error) {
	if override := nodeServiceOverride(n, service); override != "" {
		u, err := parseUpstreamOverride(override)
		if err != nil {
			return nil, err
		}
		switch u.Scheme {
		case "http", "https":
			// ok
		default:
			return nil, fmt.Errorf("upstream %s: %s scheme %q not supported (want http or https)", service, service, u.Scheme)
		}
		// Strip path/query — the proxy preserves the inbound request's
		// path verbatim. Leaving a path on the upstream target would
		// either double-prefix or get silently dropped by
		// httputil.NewSingleHostReverseProxy, depending on Go version.
		out := &url.URL{Scheme: u.Scheme, Host: u.Host}
		return out, nil
	}
	scheme := "http"
	if n.TLS {
		scheme = "https"
	}
	port, err := httpServicePort(n, service)
	if err != nil {
		return nil, err
	}
	// net.JoinHostPort brackets IPv6 literals (fd00::1 → [fd00::1]:port);
	// a bare fmt "%s:%d" would emit an unparseable host for AAAA-discovered
	// or IPv6-configured upstreams.
	return url.Parse(fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(n.Host, fmt.Sprintf("%d", port))))
}

// nodeWSURL returns the WebSocket upstream URL for a WS-shaped service
// (rpc — Tendermint /websocket — or evm_rpc_ws). Returns a URL whose
// Scheme is "ws" or "wss"; the WS pool appends the path.
//
// HTTP-style overrides (http/https) are mapped to ws/wss so an operator
// can point all five service URLs at the same edge hostname.
func nodeWSURL(n NodeConfig, service string) (*url.URL, error) {
	if override := nodeServiceOverride(n, service); override != "" {
		u, err := parseUpstreamOverride(override)
		if err != nil {
			return nil, err
		}
		switch u.Scheme {
		case "ws", "wss":
			// keep as-is
		case "http":
			u.Scheme = "ws"
		case "https":
			u.Scheme = "wss"
		default:
			return nil, fmt.Errorf("upstream %s: scheme %q not supported (want ws/wss or http/https)", service, u.Scheme)
		}
		out := &url.URL{Scheme: u.Scheme, Host: u.Host}
		return out, nil
	}
	scheme := "ws"
	if n.TLS {
		scheme = "wss"
	}
	port, err := wsServicePort(n, service)
	if err != nil {
		return nil, err
	}
	// net.JoinHostPort brackets IPv6 literals (fd00::1 → [fd00::1]:port);
	// a bare fmt "%s:%d" would emit an unparseable host for AAAA-discovered
	// or IPv6-configured upstreams.
	return url.Parse(fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(n.Host, fmt.Sprintf("%d", port))))
}

// upstreamGRPCTarget returns the gRPC dial target ("host:port") plus the
// TLS credentials to use. Honors GrpcURL when set:
//   - grpcs / https / tls schemes ⇒ TLS, default port 443
//   - grpc / http schemes        ⇒ plaintext, default port 80
//
// When no override is set, falls back to Host:GrpcPort with creds
// chosen by the node's TLS flag.
func upstreamGRPCTarget(n NodeConfig) (target string, creds credentials.TransportCredentials, err error) {
	if n.GrpcURL != "" {
		u, perr := parseUpstreamOverride(n.GrpcURL)
		if perr != nil {
			return "", nil, perr
		}
		useTLS := false
		switch u.Scheme {
		case "grpcs", "https", "tls":
			useTLS = true
		case "grpc", "http":
			useTLS = false
		default:
			return "", nil, fmt.Errorf("upstream grpc: scheme %q not supported (want grpcs/https/tls or grpc/http)", u.Scheme)
		}
		host := u.Host
		if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
			// No explicit port → infer from scheme. Use u.Hostname()
			// (unbracketed) so JoinHostPort can correctly re-bracket
			// IPv6 literals — passing u.Host directly would produce
			// "[[::1]]:443" for an IPv6 override.
			if useTLS {
				host = net.JoinHostPort(u.Hostname(), fmt.Sprintf("%d", defaultHTTPSPort))
			} else {
				host = net.JoinHostPort(u.Hostname(), fmt.Sprintf("%d", defaultHTTPPort))
			}
		}
		if useTLS {
			// ServerName is the hostname portion of the URL — needed for
			// SNI + certificate verification when the dial target is an
			// IP or includes a port suffix.
			serverName := u.Hostname()
			return host, credentials.NewTLS(&tls.Config{ServerName: serverName}), nil
		}
		return host, insecure.NewCredentials(), nil
	}
	target = fmt.Sprintf("%s:%d", n.Host, n.GrpcPort)
	if n.TLS {
		return target, credentials.NewTLS(&tls.Config{ServerName: n.Host}), nil
	}
	return target, insecure.NewCredentials(), nil
}

// nodeServiceOverride returns the operator-supplied URL override for a
// given service, or "" when none is configured.
func nodeServiceOverride(n NodeConfig, service string) string {
	switch service {
	case serviceLCD:
		return n.LcdURL
	case serviceRPC:
		return n.RpcURL
	case serviceEVMRPC:
		return n.EvmRpcURL
	case serviceEVMRPCWS:
		return n.EvmRpcWsURL
	case serviceGRPC:
		return n.GrpcURL
	}
	return ""
}

func httpServicePort(n NodeConfig, service string) (int, error) {
	switch service {
	case serviceLCD:
		return n.LcdPort, nil
	case serviceRPC:
		return n.RpcPort, nil
	case serviceEVMRPC:
		return n.EvmRpcPort, nil
	case serviceEVMRPCWS:
		return n.EvmRpcWsPort, nil
	default:
		return 0, fmt.Errorf("unknown upstream service %q", service)
	}
}

func wsServicePort(n NodeConfig, service string) (int, error) {
	switch service {
	case serviceRPC:
		return n.RpcPort, nil
	case serviceEVMRPCWS:
		return n.EvmRpcWsPort, nil
	default:
		return 0, fmt.Errorf("unknown ws upstream service %q", service)
	}
}

// nodeWSBackend returns the WS upstream URL as a string (without path)
// suitable for the WS pool's backend list. Convenience wrapper around
// nodeWSURL for the caller in cosmoguard.go. nodeWSURL only populates
// Scheme + Host on the returned URL so the output is always
// "{scheme}://{host}" with no trailing path.
func nodeWSBackend(n NodeConfig, service string) (string, error) {
	u, err := nodeWSURL(n, service)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
