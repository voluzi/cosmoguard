package cosmoguard

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gotest.tools/assert"
)

func TestReusableReader(t *testing.T) {
	t.Run("read content multiple times", func(t *testing.T) {
		original := "hello world"
		reader := ReusableReader(io.NopCloser(strings.NewReader(original)))

		// First read
		content1, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, string(content1), original)

		// Second read (should return same content)
		content2, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, string(content2), original)

		// Third read
		content3, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, string(content3), original)
	})

	t.Run("close returns nil", func(t *testing.T) {
		reader := ReusableReader(io.NopCloser(strings.NewReader("test")))
		err := reader.Close()
		assert.NilError(t, err)
	})

	t.Run("empty content", func(t *testing.T) {
		reader := ReusableReader(io.NopCloser(strings.NewReader("")))

		content, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, string(content), "")
	})

	t.Run("large content", func(t *testing.T) {
		original := strings.Repeat("abcdefghij", 10000) // 100KB
		reader := ReusableReader(io.NopCloser(strings.NewReader(original)))

		content1, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, len(content1), len(original))

		content2, err := io.ReadAll(reader)
		assert.NilError(t, err)
		assert.Equal(t, len(content2), len(original))
	})
}

func TestResponseWriterWrapper(t *testing.T) {
	t.Run("write and get status code", func(t *testing.T) {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)

		wrapper.WriteHeader(http.StatusOK)
		n, err := wrapper.Write([]byte("hello"))

		assert.NilError(t, err)
		assert.Equal(t, n, 5)
		assert.Equal(t, wrapper.GetStatusCode(), http.StatusOK)
	})

	t.Run("default status code on write", func(t *testing.T) {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)

		// Write without explicit WriteHeader
		wrapper.Write([]byte("hello"))

		assert.Equal(t, wrapper.GetStatusCode(), http.StatusOK)
	})

	t.Run("get written bytes", func(t *testing.T) {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)

		wrapper.Write([]byte("hello"))
		wrapper.Write([]byte(" world"))

		bytes, err := wrapper.GetWrittenBytes()
		assert.NilError(t, err)
		assert.Equal(t, string(bytes), "hello world")
	})

	t.Run("writes to underlying response writer", func(t *testing.T) {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)

		wrapper.WriteHeader(http.StatusCreated)
		wrapper.Write([]byte("created"))

		assert.Equal(t, rec.Code, http.StatusCreated)
		assert.Equal(t, rec.Body.String(), "created")
	})

	t.Run("status code not set returns zero", func(t *testing.T) {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)

		// Don't write or set header
		assert.Equal(t, wrapper.GetStatusCode(), 0)
	})
}

// withTrustAll temporarily configures the package-level
// trustedProxies allowlist to "trust everyone" for the duration of
// the calling test, then restores the previous allowlist. Tests that
// need the legacy "every forwarded header is honored" semantics call
// this once at the top.
func withTrustAll(t *testing.T) {
	t.Helper()
	prev := trustedProxies.Load()
	if err := SetTrustedProxies([]string{"0.0.0.0/0", "::/0"}); err != nil {
		t.Fatalf("withTrustAll: %v", err)
	}
	t.Cleanup(func() { trustedProxies.Store(prev) })
}

func TestGetSourceIP(t *testing.T) {
	// Legacy header-trusting behavior is gated behind the trusted-
	// proxies allowlist; tests written before that gate existed
	// assume "trust everyone".
	withTrustAll(t)

	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expected   string
	}{
		{
			name:       "X-Real-Ip header",
			headers:    map[string]string{"X-Real-Ip": "1.2.3.4"},
			remoteAddr: "5.6.7.8:1234",
			expected:   "1.2.3.4",
		},
		{
			name:       "X-Forwarded-For header",
			headers:    map[string]string{"X-Forwarded-For": "1.2.3.4"},
			remoteAddr: "5.6.7.8:1234",
			expected:   "1.2.3.4",
		},
		{
			name:       "X-Real-Ip takes precedence",
			headers:    map[string]string{"X-Real-Ip": "1.1.1.1", "X-Forwarded-For": "2.2.2.2"},
			remoteAddr: "5.6.7.8:1234",
			expected:   "1.1.1.1",
		},
		{
			name:       "fallback to RemoteAddr",
			headers:    map[string]string{},
			remoteAddr: "5.6.7.8:1234",
			expected:   "5.6.7.8:1234",
		},
		{
			name:       "empty headers fallback",
			headers:    map[string]string{"X-Real-Ip": "", "X-Forwarded-For": ""},
			remoteAddr: "192.168.1.1:8080",
			expected:   "192.168.1.1:8080",
		},
		{
			// X-Forwarded-For chain: "client, lb-1, lb-2" — return just
			// the leftmost (original client) per the spec.
			name:       "X-Forwarded-For chain",
			headers:    map[string]string{"X-Forwarded-For": "1.2.3.4, 10.0.0.1, 10.0.0.2"},
			remoteAddr: "5.6.7.8:1234",
			expected:   "1.2.3.4",
		},
		{
			// X-Forwarded-For chain with no spaces around commas.
			name:       "X-Forwarded-For chain no spaces",
			headers:    map[string]string{"X-Forwarded-For": "1.2.3.4,10.0.0.1"},
			remoteAddr: "5.6.7.8:1234",
			expected:   "1.2.3.4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result := GetSourceIP(req)
			assert.Equal(t, result, tt.expected)
		})
	}
}

// TestGetSourceIP_DefaultDeny pins the secure-by-default behavior:
// when no trustedProxies CIDRs are configured (the v4 default), the
// X-Real-Ip / X-Forwarded-For headers MUST be ignored and the source
// IP comes from r.RemoteAddr. A naked cosmoguard hit directly from
// the internet must not be tricked into rate-limiting (or auditing)
// the wrong client by anyone who sends `X-Real-Ip: 1.2.3.4`.
func TestGetSourceIP_DefaultDeny(t *testing.T) {
	// Ensure the global is empty for this test even if a prior test
	// left it populated.
	prev := trustedProxies.Load()
	trustedProxies.Store(nil)
	t.Cleanup(func() { trustedProxies.Store(prev) })

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "5.6.7.8:1234"
	req.Header.Set("X-Real-Ip", "1.2.3.4")
	req.Header.Set("X-Forwarded-For", "9.9.9.9")

	if got := GetSourceIP(req); got != "5.6.7.8:1234" {
		t.Fatalf("default-deny must ignore client-supplied headers, got %q", got)
	}
}

// TestGetSourceIP_TrustedPeerHonored exercises the gate's positive
// path: when the immediate peer matches the configured allowlist,
// X-Real-Ip / X-Forwarded-For are honored; a peer outside the
// allowlist is treated as untrusted and the headers are ignored.
func TestGetSourceIP_TrustedPeerHonored(t *testing.T) {
	prev := trustedProxies.Load()
	if err := SetTrustedProxies([]string{"10.0.0.0/8"}); err != nil {
		t.Fatalf("SetTrustedProxies: %v", err)
	}
	t.Cleanup(func() { trustedProxies.Store(prev) })

	// Trusted peer: headers are honored.
	r1 := httptest.NewRequest(http.MethodGet, "/", nil)
	r1.RemoteAddr = "10.1.2.3:55555"
	r1.Header.Set("X-Real-Ip", "1.2.3.4")
	if got := GetSourceIP(r1); got != "1.2.3.4" {
		t.Fatalf("trusted peer should honor X-Real-Ip, got %q", got)
	}

	// Untrusted peer: header is ignored, RemoteAddr is returned.
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.RemoteAddr = "8.8.8.8:55555"
	r2.Header.Set("X-Real-Ip", "1.2.3.4")
	if got := GetSourceIP(r2); got != "8.8.8.8:55555" {
		t.Fatalf("untrusted peer must ignore X-Real-Ip, got %q", got)
	}
}

// TestSetTrustedProxies_BareIPAccepted pins the operator-friendly
// shortcut: a bare IP without a "/mask" is auto-expanded to /32
// (v4) or /128 (v6). Operators often list LB IPs directly and
// shouldn't get a confusing "invalid CIDR" startup error.
func TestSetTrustedProxies_BareIPAccepted(t *testing.T) {
	prev := trustedProxies.Load()
	t.Cleanup(func() { trustedProxies.Store(prev) })

	if err := SetTrustedProxies([]string{"10.0.0.1", "2001:db8::1"}); err != nil {
		t.Fatalf("bare IPs should parse: %v", err)
	}
	if !remotePeerTrusted("10.0.0.1:9999") {
		t.Fatal("10.0.0.1 should be trusted")
	}
	if remotePeerTrusted("10.0.0.2:9999") {
		t.Fatal("10.0.0.2 must NOT be trusted (only /32 allowed)")
	}
}

// TestSetTrustedProxies_BadCIDRRejected pins startup failure on
// a malformed CIDR. Without this, a typo would silently degrade to
// "trust nothing" and operators would chase a missing-IP bug.
func TestSetTrustedProxies_BadCIDRRejected(t *testing.T) {
	prev := trustedProxies.Load()
	t.Cleanup(func() { trustedProxies.Store(prev) })

	err := SetTrustedProxies([]string{"10.0.0.0/8", "not-a-cidr"})
	if err == nil {
		t.Fatal("expected an error on malformed CIDR")
	}
}

func TestWriteData(t *testing.T) {
	t.Run("write data with headers", func(t *testing.T) {
		rec := httptest.NewRecorder()

		WriteData(rec, http.StatusOK, []byte("test data"),
			"Content-Type", "application/json",
			"X-Custom", "value")

		assert.Equal(t, rec.Code, http.StatusOK)
		assert.Equal(t, rec.Body.String(), "test data")
		assert.Equal(t, rec.Header().Get("Content-Type"), "application/json")
		assert.Equal(t, rec.Header().Get("X-Custom"), "value")
	})

	t.Run("write data without headers", func(t *testing.T) {
		rec := httptest.NewRecorder()

		WriteData(rec, http.StatusCreated, []byte("created"))

		assert.Equal(t, rec.Code, http.StatusCreated)
		assert.Equal(t, rec.Body.String(), "created")
	})

	t.Run("odd number of headers (ignored)", func(t *testing.T) {
		rec := httptest.NewRecorder()

		// Odd number of header args - should be ignored
		WriteData(rec, http.StatusOK, []byte("data"), "Content-Type")

		assert.Equal(t, rec.Code, http.StatusOK)
		assert.Equal(t, rec.Header().Get("Content-Type"), "")
	})
}

func TestWriteError(t *testing.T) {
	t.Run("write error with headers", func(t *testing.T) {
		rec := httptest.NewRecorder()

		WriteError(rec, http.StatusBadRequest, "bad request",
			"Content-Type", "text/plain")

		assert.Equal(t, rec.Code, http.StatusBadRequest)
		assert.Equal(t, rec.Body.String(), "bad request")
		assert.Equal(t, rec.Header().Get("Content-Type"), "text/plain")
	})

	t.Run("write error without headers", func(t *testing.T) {
		rec := httptest.NewRecorder()

		WriteError(rec, http.StatusInternalServerError, "internal error")

		assert.Equal(t, rec.Code, http.StatusInternalServerError)
		assert.Equal(t, rec.Body.String(), "internal error")
	})
}

func TestReusableReader_PartialReads(t *testing.T) {
	original := "hello world this is a test"
	reader := ReusableReader(io.NopCloser(strings.NewReader(original)))

	// Read in chunks
	buf := make([]byte, 5)

	// First chunk
	n, err := reader.Read(buf)
	assert.NilError(t, err)
	assert.Equal(t, n, 5)
	assert.Equal(t, string(buf), "hello")

	// Read remaining
	remaining, err := io.ReadAll(reader)
	assert.NilError(t, err)
	assert.Equal(t, string(remaining), " world this is a test")

	// Now read again from start
	full, err := io.ReadAll(reader)
	assert.NilError(t, err)
	assert.Equal(t, string(full), original)
}

func TestResponseWriterWrapper_HeaderAccess(t *testing.T) {
	rec := httptest.NewRecorder()
	wrapper := WrapResponseWriter(rec)

	// Set header through wrapper
	wrapper.Header().Set("X-Test", "value")

	assert.Equal(t, wrapper.Header().Get("X-Test"), "value")
	assert.Equal(t, rec.Header().Get("X-Test"), "value")
}

func BenchmarkReusableReader(b *testing.B) {
	data := bytes.Repeat([]byte("test data "), 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := ReusableReader(io.NopCloser(bytes.NewReader(data)))
		io.ReadAll(reader)
		io.ReadAll(reader) // Second read
	}
}

func BenchmarkResponseWriterWrapper(b *testing.B) {
	data := []byte("response data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		wrapper := WrapResponseWriter(rec)
		wrapper.WriteHeader(http.StatusOK)
		wrapper.Write(data)
		wrapper.GetWrittenBytes()
	}
}
