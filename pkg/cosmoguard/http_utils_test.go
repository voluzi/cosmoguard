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

func TestGetSourceIP(t *testing.T) {
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
