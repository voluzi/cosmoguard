package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type abortCountingWriter struct {
	headerCalls   int
	writeCalls    int
	abortOnHeader bool
}

func (w *abortCountingWriter) Header() http.Header { return make(http.Header) }
func (w *abortCountingWriter) WriteHeader(int) {
	w.headerCalls++
	if w.abortOnHeader {
		panic(http.ErrAbortHandler)
	}
}
func (w *abortCountingWriter) Write(b []byte) (int, error) {
	w.writeCalls++
	return len(b), nil
}

func TestRecoverHTTPPreservesAbortHandler(t *testing.T) {
	for _, tc := range []struct {
		name   string
		write  bool
		nested bool
	}{
		{"before write", false, false},
		{"after write", true, false},
		{"nested recovery", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := newEntry(slog.New(slog.NewTextHandler(&logs, nil)))
			w := &abortCountingWriter{}
			r := httptest.NewRequest(http.MethodGet, "/status", nil)
			var got any
			func() {
				defer func() { got = recover() }()
				defer recoverHTTP(logger, w, r)
				if tc.nested {
					defer recoverHTTP(logger, w, r)
				}
				if tc.write {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("partial"))
				}
				panic(http.ErrAbortHandler)
			}()
			require.True(t, got == http.ErrAbortHandler, "got panic %v", got)
			want := 0
			if tc.write {
				want = 1
			}
			require.Equal(t, want, w.headerCalls)
			require.Equal(t, want, w.writeCalls)
			require.Empty(t, logs.String())
		})
	}
	t.Run("late writer abort", func(t *testing.T) {
		w := &abortCountingWriter{abortOnHeader: true}
		r := httptest.NewRequest(http.MethodGet, "/status", nil)
		var got any
		func() {
			defer func() { got = recover() }()
			defer recoverHTTP(nil, w, r)
			panic("ordinary bug")
		}()
		require.True(t, got == http.ErrAbortHandler, "got panic %v", got)
		require.Equal(t, 1, w.headerCalls)
	})
}

// TestRecoverHTTP_PanicProducesFiveHundred: a handler that panics
// before writing should result in a 500.
func TestRecoverHTTP_Panics500BeforeWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://x/y", nil)

	func() {
		defer recoverHTTP(nil, rec, req)
		panic("kaboom")
	}()

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

// TestRecoverHTTP_PanicAfterWriteHeadersOK: once status has been
// written, the recover must not panic further; the goroutine returns
// cleanly.
func TestRecoverHTTP_PanicAfterWriteHeadersOK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://x/y", nil)

	func() {
		defer recoverHTTP(nil, rec, req)
		rec.WriteHeader(200)
		panic("late kaboom")
	}()

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// fakeServerStream is the minimum implementation of grpc.ServerStream
// for the recover test.
type fakeServerStream struct{}

func (fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (fakeServerStream) SetTrailer(metadata.MD)       {}
func (fakeServerStream) Context() context.Context     { return context.Background() }
func (fakeServerStream) SendMsg(any) error            { return nil }
func (fakeServerStream) RecvMsg(any) error            { return nil }

func TestRecoverStream_PanicReturnsInternal(t *testing.T) {
	handler := recoverStream(nil, func(any, grpc.ServerStream) error {
		panic("kaboom")
	})
	err := handler(nil, fakeServerStream{})
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v", st.Code())
	}
}

func TestRecoverStream_NormalErrorPasses(t *testing.T) {
	want := errors.New("expected")
	handler := recoverStream(nil, func(any, grpc.ServerStream) error {
		return want
	})
	got := handler(nil, fakeServerStream{})
	if !errors.Is(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
