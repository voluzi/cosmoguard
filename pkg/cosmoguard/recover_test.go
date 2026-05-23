package cosmoguard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
