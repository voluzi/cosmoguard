package cosmoguard

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestShutdownDrainsFiniteGRPCWhileHTTPAndStreamAreStuck(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	port := startGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		var req rawFrame
		if err := s.RecvMsg(&req); err != nil {
			return err
		}
		entered <- string(req.Payload)
		if string(req.Payload) == "finite" {
			select {
			case <-release:
				return s.SendMsg(&rawFrame{Payload: []byte("finished")})
			case <-s.Context().Done():
				return s.Context().Err()
			}
		}
		<-s.Context().Done()
		return s.Context().Err()
	})
	p, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port}, nil)
	finite, stream := make(chan error, 1), make(chan error, 1)
	go func() {
		var reply rawFrame
		err := conn.Invoke(t.Context(), grpcCacheTestMethod, &rawFrame{Payload: []byte("finite")}, &reply)
		if err == nil && string(reply.Payload) != "finished" {
			err = io.ErrUnexpectedEOF
		}
		finite <- err
	}()
	go func() {
		stream <- conn.Invoke(t.Context(), grpcCacheTestMethod, &rawFrame{Payload: []byte("stream")}, &rawFrame{})
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("RPC did not reach upstream")
		}
	}
	httpEntered := make(chan struct{})
	httpRelease := make(chan struct{})
	defer close(httpRelease)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(httpEntered)
		select {
		case <-httpRelease:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		r, err := http.Get(srv.URL)
		if err == nil {
			_ = r.Body.Close()
		}
	}()
	select {
	case <-httpEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not reach the handler")
	}
	f := &CosmoGuard{lcdProxy: &HttpProxy{server: srv.Config, log: log.WithField("test", t.Name())}, grpcProxy: p}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.Shutdown(ctx) }()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", p.listener.Addr().String(), 20*time.Millisecond)
		if c != nil {
			_ = c.Close()
		}
		return err != nil
	}, 150*time.Millisecond, time.Millisecond, "gRPC must stop accepting before the earlier HTTP drain finishes")
	unblock()
	select {
	case err := <-finite:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("finite RPC was not drained")
	}
	select {
	case err := <-stream:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("infinite stream did not end at the deadline")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown exceeded the caller budget")
	}
	select {
	case <-httpDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP connection was not forced closed")
	}
}
