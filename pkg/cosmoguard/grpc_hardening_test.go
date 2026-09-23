package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// startGRPCTestUpstream serves handler on a loopback TCP port, with message
// limits high enough that only the proxy's limits are under test.
func startGRPCTestUpstream(t *testing.T, handler grpc.StreamHandler) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(handler),
		grpc.MaxRecvMsgSize(64<<20),
	)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return lis.Addr().(*net.TCPAddr).Port
}

// startGRPCTestProxy runs a real GrpcProxy in front of the upstream on port
// and returns it with a client connection to its listener.
func startGRPCTestProxy(t *testing.T, node NodeConfig, rules []*GrpcRule, opts ...Option[GrpcProxyOptions]) (*GrpcProxy, *grpc.ClientConn) {
	t.Helper()
	node.Host = "127.0.0.1"
	if node.Name == "" {
		node.Name = "n0"
	}
	for _, r := range rules {
		require.NoError(t, r.Compile())
	}
	p, err := NewGrpcProxy("grpc", "127.0.0.1:0", []NodeConfig{node}, nil, nil, opts...)
	require.NoError(t, err)
	p.SetDashboard("grpc", newDashboardObservability())
	p.SetRules(rules, RuleActionAllow)
	go func() { _ = p.Run() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})
	conn, err := grpc.NewClient(p.listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}), grpc.MaxCallRecvMsgSize(64<<20)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return p, conn
}

func echoSizedUpstream(size int) grpc.StreamHandler {
	return func(_ any, s grpc.ServerStream) error {
		var req rawFrame
		if err := s.RecvMsg(&req); err != nil {
			return err
		}
		return s.SendMsg(&rawFrame{Payload: bytes.Repeat([]byte{'a'}, size)})
	}
}

func TestGRPCProxyRelaysMessagesAboveGrpcGoDefault(t *testing.T) {
	const size = 9 << 20
	port := startGRPCTestUpstream(t, echoSizedUpstream(size))
	for name, cache := range map[string]*RuleCache{
		"transparent": nil,
		"cached":      {Enable: true, TTL: time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			rule := &GrpcRule{Priority: 1, Action: RuleActionAllow, Methods: []string{grpcCacheTestMethod}, Cache: cache}
			_, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port}, []*GrpcRule{rule})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var resp rawFrame
			req := &rawFrame{Payload: bytes.Repeat([]byte{'b'}, size)}
			require.NoError(t, conn.Invoke(ctx, grpcCacheTestMethod, req, &resp))
			require.Len(t, resp.Payload, size)
		})
	}
}

func TestGRPCProxyEnforcesConfiguredMessageLimits(t *testing.T) {
	port := startGRPCTestUpstream(t, echoSizedUpstream(2<<20))
	_, conn := startGRPCTestProxy(t,
		NodeConfig{GrpcPort: port, CircuitBreaker: enabledBreaker(1, time.Minute)},
		nil, WithGrpcMessageLimits(1<<20, 1<<20))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: bytes.Repeat([]byte{'b'}, 2<<20)}, &rawFrame{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "request over maxRecvMsgSize: %v", err)

	err = conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: []byte{1}}, &rawFrame{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "response over maxSendMsgSize: %v", err)
}

// A cache hit never passes the upstream receive cap, so the listener's own
// send cap is what bounds it.
func TestGRPCProxyCapsCachedResponseSize(t *testing.T) {
	port := startGRPCTestUpstream(t, echoSizedUpstream(1))
	rule := &GrpcRule{Priority: 1, Action: RuleActionAllow, Methods: []string{grpcCacheTestMethod}, Cache: &RuleCache{Enable: true, TTL: time.Minute}}
	p, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port}, []*GrpcRule{rule}, WithGrpcMessageLimits(1<<20, 1<<20))
	request := []byte{1}
	metaPart := grpcCacheKeyMetaPart(metadata.NewIncomingContext(context.Background(), metadata.MD{}), rule.Cache.EffectiveKeyMetadata())
	key := grpcCacheKey(rule.Fingerprint, grpcCacheTestMethod, request, rule.Cache.KeyMode, p.canonical, metaPart)
	p.storeNewestGRPCResponse(key, grpcCachedResponse{Payload: bytes.Repeat([]byte{'a'}, 2<<20), StoredAt: time.Now().UTC()}, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: request}, &rawFrame{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
}

// The proxy's own receive cap rejects a response the upstream delivered in
// full; that must not open the upstream's breaker.
func TestGRPCLocalReceiveCapDoesNotOpenBreaker(t *testing.T) {
	port := startGRPCTestUpstream(t, echoSizedUpstream(2<<20))
	p, conn := startGRPCTestProxy(t,
		NodeConfig{GrpcPort: port, CircuitBreaker: enabledBreaker(1, time.Minute)},
		nil, WithGrpcMessageLimits(1<<20, 1<<20))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: []byte{1}}, &rawFrame{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	up := p.pool.upstreamsSnapshot()[0]
	require.Eventually(t, func() bool { return up.inFlight.Load() == 0 }, 2*time.Second, 5*time.Millisecond)
	require.False(t, up.CircuitOpen())
}

func TestGRPCTransportFailureStillOpensBreaker(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	p, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port, CircuitBreaker: enabledBreaker(1, time.Minute)}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: []byte{1}}, &rawFrame{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	up := p.pool.upstreamsSnapshot()[0]
	require.Eventually(t, up.CircuitOpen, 2*time.Second, 5*time.Millisecond)
}

func TestGRPCInvalidClientMetadataDoesNotOpenBreaker(t *testing.T) {
	for name, md := range map[string]metadata.MD{
		"non-printable value": metadata.Pairs("x-trace", "café"),
		"illegal key":         {"x+foo": []string{"1"}},
	} {
		t.Run(name, func(t *testing.T) {
			var invokes atomic.Int32
			conn := newRawGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
				invokes.Add(1)
				var req rawFrame
				if err := s.RecvMsg(&req); err != nil {
					return err
				}
				return s.SendMsg(&rawFrame{Payload: []byte{1}})
			})
			p, _ := newGRPCCacheTestProxy(t, conn, &RuleCache{Enable: true, TTL: time.Minute})
			up := p.pool.upstreamsSnapshot()[0]
			up.cbConfig = enabledBreaker(1, time.Minute)

			s := newGRPCCacheTestStream([]byte{1})
			s.ctx = grpc.NewContextWithServerTransportStream(
				metadata.NewIncomingContext(context.Background(), md),
				&fakeServerTransportStream{method: grpcCacheTestMethod})
			err := grpcCacheTestHandler(p)(nil, s)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
			require.False(t, up.CircuitOpen())
			require.Zero(t, invokes.Load())
		})
	}
}

func TestValidateOutgoingMetadata(t *testing.T) {
	require.NoError(t, validateOutgoingMetadata(metadata.MD{
		":authority":            {"node"},
		"x-cosmos-block-height": {"12"},
		"trace-bin":             {"\x00\xff"},
	}))
	require.Error(t, validateOutgoingMetadata(metadata.MD{"": {"v"}}))
	require.Error(t, validateOutgoingMetadata(metadata.MD{"x-a": {"line\nbreak"}}))
}

// inboundFailingStream fails its RecvMsg once the forwarder is already
// delivering an upstream message, whose delivery then fails with a
// non-status error — the path where the forwarder synthesizes Internal.
type inboundFailingStream struct {
	*grpcCacheTestStream
	sending chan struct{}
}

func (s *inboundFailingStream) RecvMsg(any) error {
	<-s.sending
	return errors.New("inbound reset")
}

func (s *inboundFailingStream) SendMsg(any) error {
	close(s.sending)
	time.Sleep(50 * time.Millisecond)
	return errors.New("inbound write failed")
}

func TestGRPCTransparentInboundFailureDoesNotOpenBreaker(t *testing.T) {
	port := startGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		if err := s.SendMsg(&rawFrame{Payload: []byte{1}}); err != nil {
			return err
		}
		<-s.Context().Done()
		return s.Context().Err()
	})
	up, err := buildGrpcUpstream(NodeConfig{Name: "raw", Host: "127.0.0.1", GrpcPort: port}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = up.conn.Close() })
	conn := up.conn
	up.cbConfig = enabledBreaker(1, time.Minute)
	director := func(ctx context.Context, _ string) (context.Context, *grpc.ClientConn, error) {
		out := metadata.NewOutgoingContext(ctx, metadata.MD{})
		return context.WithValue(out, grpcUpstreamCtxKey{}, up), conn, nil
	}
	s := &inboundFailingStream{grpcCacheTestStream: newGRPCCacheTestStream(nil), sending: make(chan struct{})}
	err = rawTransparentHandler(director)(nil, s)
	require.Equal(t, codes.Internal, status.Code(err), "%v", err)
	require.False(t, up.CircuitOpen())
}

// Upstream response headers reach the client even when no message follows
// them, on success and on error.
func TestGRPCTransparentForwardsHeadersWithoutMessages(t *testing.T) {
	port := startGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		if err := s.SendHeader(metadata.Pairs("x-cosmos-block-height", "12345")); err != nil {
			return err
		}
		var req rawFrame
		if err := s.RecvMsg(&req); err != nil {
			return err
		}
		if string(req.Payload) == "fail" {
			return status.Error(codes.NotFound, "missing")
		}
		return nil
	})
	_, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var header metadata.MD
	err := conn.Invoke(ctx, grpcCacheTestMethod, &rawFrame{Payload: []byte("fail")}, &rawFrame{}, grpc.Header(&header))
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, []string{"12345"}, header.Get("x-cosmos-block-height"))

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, grpcCacheTestMethod)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&rawFrame{Payload: []byte("ok")}))
	require.NoError(t, stream.CloseSend())
	require.ErrorIs(t, stream.RecvMsg(&rawFrame{}), io.EOF)
	header, err = stream.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"12345"}, header.Get("x-cosmos-block-height"))
}

func TestGRPCListenerCapsConcurrentStreams(t *testing.T) {
	var open atomic.Int32
	release := make(chan struct{})
	port := startGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		open.Add(1)
		<-release
		return nil
	})
	_, conn := startGRPCTestProxy(t, NodeConfig{GrpcPort: port}, nil)
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	for range grpcMaxConcurrentStreams {
		stream, err := conn.NewStream(ctx, desc, grpcCacheTestMethod)
		require.NoError(t, err)
		require.NoError(t, stream.SendMsg(&rawFrame{Payload: []byte{1}}))
	}
	require.Eventually(t, func() bool { return open.Load() == grpcMaxConcurrentStreams }, 10*time.Second, 10*time.Millisecond)

	extraCtx, extraCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer extraCancel()
	stream, err := conn.NewStream(extraCtx, desc, grpcCacheTestMethod)
	if err == nil {
		_ = stream.SendMsg(&rawFrame{Payload: []byte{1}})
		err = stream.RecvMsg(&rawFrame{})
	}
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
	require.Equal(t, int32(grpcMaxConcurrentStreams), open.Load())
}

func TestUpstreamGRPCTarget_IPv6(t *testing.T) {
	target, _, err := upstreamGRPCTarget(NodeConfig{Host: "fd00::1", GrpcPort: 9090})
	require.NoError(t, err)
	require.Equal(t, "[fd00::1]:9090", target)
}

func TestPrepareConfig_GrpcMessageLimits(t *testing.T) {
	cfg := &Config{}
	require.NoError(t, PrepareConfig(cfg))
	require.Equal(t, 10<<20, cfg.GRPC.MaxRecvMsgSize)
	require.Equal(t, 1<<31-1, cfg.GRPC.MaxSendMsgSize)

	require.ErrorContains(t, PrepareConfig(&Config{GRPC: GrpcConfig{MaxRecvMsgSize: -1}}), "grpc.maxRecvMsgSize")
	require.ErrorContains(t, PrepareConfig(&Config{GRPC: GrpcConfig{MaxSendMsgSize: -1}}), "grpc.maxSendMsgSize")
}

func TestTryReload_RejectsGrpcMessageLimitChange(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "cosmoguard.yaml")
	header := portYAMLHeader(t)
	require.NoError(t, os.WriteFile(cfgPath, []byte(header+"\ngrpc:\n  default: allow\n"), 0644))
	cg, err := NewFromFile(cfgPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })
	original := cg.cfg

	require.NoError(t, os.WriteFile(cfgPath, []byte(header+"\ngrpc:\n  default: allow\n  maxRecvMsgSize: 1048576\n"), 0644))
	cg.tryReload()

	require.Same(t, original, cg.cfg)
	require.NotNil(t, cg.dashboard.lastReload)
	require.False(t, cg.dashboard.lastReload.Success)
}

// Upstream response metadata stays paired with the payload across miss, hit,
// stale and refresh, with the cache marker added alongside it.
func TestGRPCCachePreservesUpstreamMetadata(t *testing.T) {
	var height atomic.Int32
	conn := newRawGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		var req rawFrame
		if err := s.RecvMsg(&req); err != nil {
			return err
		}
		h := strconv.Itoa(int(height.Add(1)))
		if err := s.SetHeader(metadata.Pairs("x-cosmos-block-height", h)); err != nil {
			return err
		}
		s.SetTrailer(metadata.Pairs("app-trailer", "t"+h))
		return s.SendMsg(&rawFrame{Payload: []byte("payload-" + h)})
	})
	p, _ := newGRPCCacheTestProxy(t, conn, &RuleCache{Enable: true, TTL: time.Second, StaleWhileRevalidate: time.Minute})
	handler := grpcCacheTestHandler(p)
	base := time.Now().UTC()
	var offset atomic.Int64 // the background refresh reads p.now concurrently
	p.now = func() time.Time { return base.Add(time.Duration(offset.Load())) }

	expect := func(state, h string) {
		t.Helper()
		s := newGRPCCacheTestStream([]byte("req"))
		require.NoError(t, handler(nil, s))
		gotState, payload := s.result()
		header, trailer := s.metadata()
		require.Equal(t, state, gotState)
		require.Equal(t, "payload-"+h, string(payload))
		require.Equal(t, []string{h}, header.Get("x-cosmos-block-height"))
		require.Equal(t, []string{"t" + h}, trailer.Get("app-trailer"))
	}
	expect(cacheMiss, "1")
	expect(cacheHit, "1")

	offset.Store(int64(2 * time.Second))
	expect(cacheStale, "1")
	require.Eventually(t, func() bool { return height.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
	offset.Store(int64(2500 * time.Millisecond))
	require.Eventually(t, func() bool {
		s := newGRPCCacheTestStream([]byte("req"))
		require.NoError(t, handler(nil, s))
		state, _ := s.result()
		return state == cacheHit
	}, 2*time.Second, 5*time.Millisecond)
	expect(cacheHit, "2")
}

func TestGRPCCacheMissForwardsUpstreamErrorMetadata(t *testing.T) {
	conn := newRawGRPCTestUpstream(t, func(_ any, s grpc.ServerStream) error {
		var req rawFrame
		if err := s.RecvMsg(&req); err != nil {
			return err
		}
		if err := s.SendHeader(metadata.Pairs("x-cosmos-block-height", "9")); err != nil {
			return err
		}
		s.SetTrailer(metadata.Pairs("app-trailer", "why"))
		return status.Error(codes.NotFound, "missing")
	})
	p, _ := newGRPCCacheTestProxy(t, conn, &RuleCache{Enable: true, TTL: time.Minute})
	s := newGRPCCacheTestStream([]byte("req"))
	err := grpcCacheTestHandler(p)(nil, s)
	require.Equal(t, codes.NotFound, status.Code(err))
	header, trailer := s.metadata()
	require.Equal(t, []string{"9"}, header.Get("x-cosmos-block-height"))
	require.Equal(t, []string{"why"}, trailer.Get("app-trailer"))
}

// Shared backends (olric / redis) msgpack-encode entries; the metadata must
// survive the round trip.
func TestGRPCCachedResponseMetadataRoundTrip(t *testing.T) {
	in := grpcCachedResponse{
		Payload:  []byte("p"),
		StoredAt: time.Now().UTC().Truncate(time.Millisecond),
		Header:   metadata.Pairs("x-cosmos-block-height", "5"),
		Trailer:  metadata.Pairs("app-trailer", "t"),
	}
	data, err := msgpack.Marshal(in)
	require.NoError(t, err)
	var out grpcCachedResponse
	require.NoError(t, msgpack.Unmarshal(data, &out))
	require.Equal(t, in.Header, out.Header)
	require.Equal(t, in.Trailer, out.Trailer)
}
