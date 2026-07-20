package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	cosmoguardcache "github.com/voluzi/cosmoguard/pkg/cache"
)

const grpcCacheTestMethod = "/cosmoguard.test.Cache/Query"

type grpcCacheTestStream struct {
	ctx      context.Context
	request  []byte
	mu       sync.Mutex
	header   metadata.MD
	response []byte
}

func newGRPCCacheTestStream(request []byte) *grpcCacheTestStream {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	ctx = grpc.NewContextWithServerTransportStream(ctx, &fakeServerTransportStream{method: grpcCacheTestMethod})
	return &grpcCacheTestStream{ctx: ctx, request: append([]byte(nil), request...)}
}

func (s *grpcCacheTestStream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *grpcCacheTestStream) SendHeader(md metadata.MD) error { return s.SetHeader(md) }
func (s *grpcCacheTestStream) SetTrailer(metadata.MD)          {}
func (s *grpcCacheTestStream) Context() context.Context        { return s.ctx }

func (s *grpcCacheTestStream) SendMsg(msg any) error {
	frame, ok := msg.(*rawFrame)
	if !ok {
		return fmt.Errorf("unexpected response type %T", msg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.response = append([]byte(nil), frame.Payload...)
	return nil
}

func (s *grpcCacheTestStream) RecvMsg(msg any) error {
	frame, ok := msg.(*rawFrame)
	if !ok {
		return fmt.Errorf("unexpected request type %T", msg)
	}
	frame.Payload = append([]byte(nil), s.request...)
	return nil
}

func (s *grpcCacheTestStream) result() (string, []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := s.header.Get(grpcCacheMetaKey)
	state := ""
	if len(values) > 0 {
		state = values[0]
	}
	return state, append([]byte(nil), s.response...)
}

type grpcCacheReadBarrier struct {
	cosmoguardcache.Cache[string, grpcCachedResponse]
	want int
	mu   sync.Mutex
	read int
	done chan struct{}
}

func newGRPCCacheReadBarrier(inner cosmoguardcache.Cache[string, grpcCachedResponse], want int) *grpcCacheReadBarrier {
	return &grpcCacheReadBarrier{Cache: inner, want: want, done: make(chan struct{})}
}

func (c *grpcCacheReadBarrier) Get(ctx context.Context, key string) (grpcCachedResponse, error) {
	value, err := c.Cache.Get(ctx, key)
	c.mu.Lock()
	c.read++
	if c.read == c.want {
		close(c.done)
	}
	done := c.done
	c.mu.Unlock()
	select {
	case <-done:
		return value, err
	case <-ctx.Done():
		return grpcCachedResponse{}, ctx.Err()
	}
}

func newRawGRPCTestUpstream(t *testing.T, handler grpc.StreamHandler) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(handler),
	)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		server.Stop()
		require.NoError(t, listener.Close())
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve raw gRPC test upstream: %v", err)
		}
	})
	return conn
}

func newGRPCCacheTestProxy(t *testing.T, conn *grpc.ClientConn, cacheRule *RuleCache) (*GrpcProxy, *GrpcRule) {
	t.Helper()
	rule := &GrpcRule{
		Priority: 1,
		Action:   RuleActionAllow,
		Methods:  []string{grpcCacheTestMethod},
		Cache:    cacheRule,
	}
	require.NoError(t, rule.Compile())

	responseCache, err := newResponseCache[string, grpcCachedResponse](nil, nil, t.Name(), CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, responseCache.Close()) })

	upstream := newTestGrpcUpstream("raw", 1)
	upstream.conn = conn
	return &GrpcProxy{
		defaultAction: RuleActionDeny,
		rules:         []*GrpcRule{rule},
		pool:          newTestGrpcPool("weighted-round-robin", upstream),
		log:           log.WithField("test", t.Name()),
		grpcCache:     responseCache,
		now:           time.Now,
		cgDashboard:   newDashboardObservability(),
		section:       "grpc",
	}, rule
}

func grpcCacheTestHandler(p *GrpcProxy) grpc.StreamHandler {
	return cachingStreamHandler(p, func(any, grpc.ServerStream) error {
		return errors.New("cacheable test method reached transparent handler")
	})
}

func TestGRPCCacheConcurrentMissesCoalesceOneUpstreamInvoke(t *testing.T) {
	const waiters = 16
	request := []byte("same-request")
	payload := []byte("coalesced-response")
	var invokes atomic.Int32
	upstreamStarted := make(chan struct{})
	releaseUpstream := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }
	t.Cleanup(release)

	conn := newRawGRPCTestUpstream(t, func(_ any, stream grpc.ServerStream) error {
		var frame rawFrame
		if err := stream.RecvMsg(&frame); err != nil {
			return err
		}
		invokes.Add(1)
		startedOnce.Do(func() { close(upstreamStarted) })
		<-releaseUpstream
		return stream.SendMsg(&rawFrame{Payload: payload})
	})
	p, _ := newGRPCCacheTestProxy(t, conn, &RuleCache{Enable: true, TTL: time.Minute})
	readBarrier := newGRPCCacheReadBarrier(p.grpcCache, waiters)
	p.grpcCache = readBarrier
	handler := grpcCacheTestHandler(p)

	streams := make([]*grpcCacheTestStream, waiters)
	errs := make(chan error, waiters)
	for i := range streams {
		streams[i] = newGRPCCacheTestStream(request)
		go func(stream *grpcCacheTestStream) { errs <- handler(nil, stream) }(streams[i])
	}

	select {
	case <-readBarrier.done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent requests did not all reach the cold cache")
	}
	select {
	case <-upstreamStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("coalesced upstream invoke did not start")
	}
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(1), invokes.Load(), "cold requests must share one in-flight Invoke")
	release()

	for range waiters {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), invokes.Load())
	for _, stream := range streams {
		cacheState, response := stream.result()
		require.Equal(t, cacheMiss, cacheState)
		require.Equal(t, payload, response)
	}
}

func TestGRPCCacheStaleResponseRefreshesOnceInBackground(t *testing.T) {
	request := []byte("request")
	stalePayload := []byte("version-1")
	refreshedPayload := []byte("version-2")
	var invokes atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	t.Cleanup(release)

	conn := newRawGRPCTestUpstream(t, func(_ any, stream grpc.ServerStream) error {
		var frame rawFrame
		if err := stream.RecvMsg(&frame); err != nil {
			return err
		}
		switch invokes.Add(1) {
		case 1:
			return stream.SendMsg(&rawFrame{Payload: stalePayload})
		case 2:
			close(refreshStarted)
			<-releaseRefresh
			return stream.SendMsg(&rawFrame{Payload: refreshedPayload})
		default:
			return fmt.Errorf("unexpected extra upstream Invoke")
		}
	})
	p, rule := newGRPCCacheTestProxy(t, conn, &RuleCache{
		Enable:               true,
		TTL:                  time.Second,
		StaleWhileRevalidate: time.Minute,
	})
	handler := grpcCacheTestHandler(p)
	base := time.Now().UTC()
	p.now = func() time.Time { return base }

	prime := newGRPCCacheTestStream(request)
	require.NoError(t, handler(nil, prime))
	cacheState, response := prime.result()
	require.Equal(t, cacheMiss, cacheState)
	require.Equal(t, stalePayload, response)
	require.Equal(t, int32(1), invokes.Load())

	p.now = func() time.Time { return base.Add(2 * time.Second) }
	stale := newGRPCCacheTestStream(request)
	staleDone := make(chan error, 1)
	go func() { staleDone <- handler(nil, stale) }()
	select {
	case err := <-staleDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stale response waited for the background refresh")
	}
	cacheState, response = stale.result()
	require.Equal(t, cacheStale, cacheState)
	require.Equal(t, stalePayload, response)

	select {
	case <-refreshStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not start")
	}
	for range 8 {
		stream := newGRPCCacheTestStream(request)
		require.NoError(t, handler(nil, stream))
		cacheState, response = stream.result()
		require.Equal(t, cacheStale, cacheState)
		require.Equal(t, stalePayload, response)
	}
	require.Equal(t, int32(2), invokes.Load(), "stale readers must share one background refresh")

	release()
	metaPart := grpcCacheKeyMetaPart(
		metadata.NewIncomingContext(context.Background(), metadata.MD{}),
		rule.Cache.EffectiveKeyMetadata(),
	)
	key := grpcCacheKey(rule.Fingerprint, grpcCacheTestMethod, request, rule.Cache.KeyMode, p.canonical, metaPart)
	require.Eventually(t, func() bool {
		cached, err := p.grpcCache.Get(context.Background(), key)
		return err == nil && string(cached.Payload) == string(refreshedPayload)
	}, 2*time.Second, 5*time.Millisecond)

	hit := newGRPCCacheTestStream(request)
	require.NoError(t, handler(nil, hit))
	cacheState, response = hit.result()
	require.Equal(t, cacheHit, cacheState)
	require.Equal(t, refreshedPayload, response)
	require.Equal(t, int32(2), invokes.Load())
}
