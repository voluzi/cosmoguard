package cosmoguard

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
)

// trustedProxies is the live CIDR allowlist of immediate connection
// peers whose X-Real-Ip / X-Forwarded-For headers GetSourceIP will
// honor. Empty (the default) means "trust no one", and the source
// IP comes from r.RemoteAddr only — secure-by-default.
//
// atomic.Pointer so SetTrustedProxies can swap the list at runtime
// (config reload) without locking the hot GetSourceIP path. The
// pointed-to slice is immutable after publication.
var trustedProxies atomic.Pointer[[]*net.IPNet]

// SetTrustedProxies parses and publishes the CIDR allowlist for
// trusted forwarded-header sources. Pass nil or empty to clear
// (default-deny). Returns an error on any malformed CIDR so a typo
// fails startup loudly rather than silently degrading to
// "trust nothing".
//
// PROCESS-GLOBAL. Two CosmoGuard instances in the same process
// share the same allowlist — the second New() call's
// PrepareConfig overwrites the first's trustedProxies. In
// practice cosmoguard runs as a single binary so this is fine,
// but tests that spin up multiple CosmoGuards must be aware
// (the testharness pattern uses 0.0.0.0/0 for permissiveness).
func SetTrustedProxies(cidrs []string) error {
	if len(cidrs) == 0 {
		trustedProxies.Store(nil)
		return nil
	}
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		// Accept bare IPs too — they're equivalent to /32 (v4) or
		// /128 (v6), and operators frequently write the LB IP without
		// thinking about masks.
		if !strings.Contains(c, "/") {
			if ip := net.ParseIP(c); ip != nil {
				if ip.To4() != nil {
					c += "/32"
				} else {
					c += "/128"
				}
			}
		}
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return fmt.Errorf("trustedProxies: invalid CIDR %q: %w", c, err)
		}
		nets = append(nets, n)
	}
	trustedProxies.Store(&nets)
	return nil
}

// snapshotTrustedProxies returns the current live trusted-proxy list
// pointer; restoreTrustedProxies puts it back. Used by the config
// reloader to undo PrepareConfig's SetTrustedProxies side effect when a
// reload is ultimately rejected — otherwise a rejected config's trust
// list would leak into live source-IP decisions.
func snapshotTrustedProxies() *[]*net.IPNet { return trustedProxies.Load() }

func restoreTrustedProxies(prev *[]*net.IPNet) { trustedProxies.Store(prev) }

// remotePeerTrusted reports whether r.RemoteAddr falls inside the
// configured trustedProxies allowlist. Empty allowlist → always
// false (default-deny).
func remotePeerTrusted(remoteAddr string) bool {
	nets := trustedProxies.Load()
	if nets == nil || len(*nets) == 0 {
		return false
	}
	host := remoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range *nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

type reusableReader struct {
	io.Reader
	readBuf *bytes.Buffer
	backBuf *bytes.Buffer
}

func ReusableReader(r io.ReadCloser) (io.ReadCloser, error) {
	readBuf := bytes.Buffer{}
	// Surface the drain error (notably http.MaxBytesReader's
	// MaxBytesError) instead of swallowing it: a request that exceeds
	// server.maxRequestBody without an honest Content-Length is only
	// caught here on the buffering paths (cacheable HTTP rules,
	// JSON-RPC). Returning the error lets callers reject with 413
	// instead of parsing/forwarding the truncated body.
	_, err := readBuf.ReadFrom(r)
	_ = r.Close()
	backBuf := bytes.Buffer{}

	return reusableReader{
		io.TeeReader(&readBuf, &backBuf),
		&readBuf,
		&backBuf,
	}, err
}

func (r reusableReader) Close() error {
	return nil
}

func (r reusableReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.reset()
	}
	return n, err
}

func (r reusableReader) reset() {
	io.Copy(r.readBuf, r.backBuf)
}

type ResponseWriterWrapper struct {
	http.ResponseWriter
	buf              *bytes.Buffer
	multi            io.Writer
	statusCode       int
	committedHeaders http.Header // snapshot taken at WriteHeader / first Write
}

func WrapResponseWriter(w http.ResponseWriter) *ResponseWriterWrapper {
	buffer := &bytes.Buffer{}
	multi := io.MultiWriter(buffer, w)
	return &ResponseWriterWrapper{
		ResponseWriter: w,
		buf:            buffer,
		multi:          multi,
	}
}

func (w *ResponseWriterWrapper) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		// http.ResponseWriter implicitly writes a 200 on first Write; mirror
		// that semantics here so the snapshot captures the headers the
		// client actually sees.
		w.statusCode = http.StatusOK
		w.snapshotHeaders()
	}
	return w.multi.Write(p)
}

func (w *ResponseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.snapshotHeaders()
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *ResponseWriterWrapper) snapshotHeaders() {
	if w.committedHeaders == nil {
		w.committedHeaders = w.ResponseWriter.Header().Clone()
	}
}

func (w *ResponseWriterWrapper) GetStatusCode() int {
	return w.statusCode
}

func (w *ResponseWriterWrapper) GetWrittenBytes() ([]byte, error) {
	return io.ReadAll(w.buf)
}

// GetCommittedHeaders returns the header set as it was the moment the
// response was committed to the client. nil if the wrapper was never
// written to. Cosmoguard's cache layer uses this snapshot so the cache
// hit path replays the same headers the upstream sent.
func (w *ResponseWriterWrapper) GetCommittedHeaders() http.Header {
	return w.committedHeaders
}

// StatusOnlyWriter is the minimum wrapper needed by the fast path
// (non-cached rules). It captures the status code for metrics/logging
// but does NOT buffer the response body — bytes flow straight through
// to the underlying writer, no allocation overhead. Use this when you
// know the response will not need to be cached or replayed.
type StatusOnlyWriter struct {
	http.ResponseWriter
	statusCode int
}

// WrapStatusOnly returns a writer that only intercepts the status code.
func WrapStatusOnly(w http.ResponseWriter) *StatusOnlyWriter {
	return &StatusOnlyWriter{ResponseWriter: w}
}

// Write captures the implicit 200 the stdlib emits on first Write.
func (w *StatusOnlyWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

// WriteHeader records the explicit status code and forwards.
func (w *StatusOnlyWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// GetStatusCode returns the most-recently-observed response status.
func (w *StatusOnlyWriter) GetStatusCode() int { return w.statusCode }

// GetSourceIP returns the best-effort client IP, in priority order:
// X-Real-Ip → X-Forwarded-For (FIRST hop only) → RemoteAddr. The
// forwarded-header path is ONLY taken when r.RemoteAddr (the
// immediate connection peer) matches the configured trustedProxies
// allowlist; otherwise the headers are ignored and RemoteAddr is
// returned directly.
//
// Default-deny semantics: a deployment that didn't set
// server.trustedProxies treats every client-supplied X-Real-Ip /
// X-Forwarded-For as untrustworthy, so a naked cosmoguard hit
// directly from the internet can't be tricked into rate-limiting
// the wrong client (or spoofing the audit trail) by anyone who
// simply sends `X-Real-Ip: 1.2.3.4`. To honor forwarded headers,
// the operator sets server.trustedProxies to the CIDR of the LB
// that actually rewrites them.
//
// X-Forwarded-For is a comma-separated chain: "client-ip, lb-ip, ...".
// The leftmost entry is the original client; subsequent entries are
// intermediate proxies. Returning the first hop matches the spec and is
// what every rate-limit / per-IP feature actually wants.
func GetSourceIP(r *http.Request) string {
	if remotePeerTrusted(r.RemoteAddr) {
		if v := r.Header.Get("X-Real-Ip"); v != "" {
			return stripPort(strings.TrimSpace(v))
		}
		if v := r.Header.Get("X-Forwarded-For"); v != "" {
			// First hop only — strip everything after the first comma.
			if i := strings.IndexByte(v, ','); i >= 0 {
				return stripPort(strings.TrimSpace(v[:i]))
			}
			return stripPort(strings.TrimSpace(v))
		}
	}
	// Host-only: r.RemoteAddr carries the ephemeral source port, which
	// would give every TCP connection from the same client a distinct
	// per-IP rate-limit bucket and break sourceIP/CIDR matching unless
	// every caller remembered to strip it. Return the canonical IP so
	// policy decisions are stable; the port is not useful for any
	// per-client policy. stripPort is idempotent, so callers that still
	// wrap this in stripPort() are unaffected.
	return stripPort(r.RemoteAddr)
}

func WriteData(w http.ResponseWriter, code int, data []byte, headers ...string) {
	if len(headers)%2 == 0 {
		for i := 0; i < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
	}
	w.WriteHeader(code)
	w.Write(data)
}

func WriteError(w http.ResponseWriter, code int, msg string, headers ...string) {
	if len(headers)%2 == 0 {
		for i := 0; i < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
	}
	w.WriteHeader(code)
	w.Write([]byte(msg))
}
