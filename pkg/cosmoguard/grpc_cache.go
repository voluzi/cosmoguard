package cosmoguard

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// spannedServerStream lets us replace the gRPC server stream's context
// with one that carries an active OpenTelemetry span. gRPC owns the
// real ctx on ServerStream, so the only way to propagate spanCtx into
// the director (Handle) and the transparent forwarder is to wrap.
type spannedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s spannedServerStream) Context() context.Context { return s.ctx }

// grpcCacheWriteTimeout bounds how long a cache write can hang after
// the inbound RPC has already returned a response to the client. We
// intentionally use a detached context for Set so a client that closes
// its stream immediately doesn't abort the write — the response was
// already produced and is just as cacheable as one whose client stuck
// around. A short timeout prevents a slow/wedged cache backend from
// leaking goroutines.
const grpcCacheWriteTimeout = 5 * time.Second

// grpcRefreshTimeout bounds a coalesced / background upstream Invoke so a wedged
// upstream can't pin a refresh goroutine forever.
const grpcRefreshTimeout = 30 * time.Second

// grpcForegroundFetchTimeout bounds detached foreground flights without
// coupling them to any one caller's deadline. It is deliberately longer than
// the background refresh budget so slow but legitimate unary calls can finish.
const grpcForegroundFetchTimeout = 5 * time.Minute

func configuredGRPCForegroundFetchTimeout(cacheConfig *CacheGlobalConfig) time.Duration {
	if cacheConfig != nil && cacheConfig.GRPCForegroundFetchTimeout > 0 {
		return cacheConfig.GRPCForegroundFetchTimeout
	}
	return grpcForegroundFetchTimeout
}

// grpcCacheMetaKey is the response-header metadata cosmoguard adds to indicate
// cache state (hit / miss / stale) — the gRPC analogue of the HTTP
// X-Cosmoguard-Cache header. gRPC had no cache signal before; this gives
// clients and dashboards parity.
const grpcCacheMetaKey = "x-cosmoguard-cache"

// grpcCachedResponse wraps a cached gRPC response payload with the time it was
// stored, so the freshness / stale-while-revalidate policy can be applied at
// read time (the bare []byte value had no timestamp). Old []byte-schema entries
// fail to decode into this struct and surface as a miss — a safe self-heal.
type grpcCachedResponse struct {
	Payload   []byte    `msgpack:"payload"`
	StoredAt  time.Time `msgpack:"stored_at,omitempty"`
	shareable bool
	owner     *grpcResponseOwner
}

type grpcResponseOwner [1]byte

// CacheCost implements the L1 cost accounting (cache.CacheCoster). 64 bytes of
// slack covers the struct header + StoredAt + map/entry bookkeeping.
func (g grpcCachedResponse) CacheCost() uint64 { return uint64(len(g.Payload)) + 64 }

// rawCodec is the codec cosmoguard installs on the gRPC server + per-call
// on the upstream client. Hands raw protobuf bytes to handlers without
// deserializing — the proxy doesn't need to know any .proto definitions.
//
// This is the same technique mwitkow's proxy.Codec uses, but with an
// EXPORTED frame type so cosmoguard's caching handler can read .Payload
// directly. Registered via grpc.ForceServerCodec / grpc.ForceCodec.
type rawCodec struct{}

func (rawCodec) Name() string { return "cosmoguard-raw" }

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*rawFrame)
	if !ok {
		return nil, fmt.Errorf("cosmoguard rawCodec: cannot marshal %T", v)
	}
	return f.Payload, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*rawFrame)
	if !ok {
		return fmt.Errorf("cosmoguard rawCodec: cannot unmarshal into %T", v)
	}
	f.Payload = data
	return nil
}

// rawFrame is the message type our codec exchanges. Both sides of the
// proxy use it — the inbound stream receives `*rawFrame` for the
// request bytes, the outbound Invoke fills another `*rawFrame` with the
// response bytes.
type rawFrame struct {
	Payload []byte
}

// Reset / String / ProtoMessage implementations to satisfy gRPC's
// expectation that the type "looks like" a proto message. The codec
// never actually calls these — they exist so reflection at the gRPC
// layer doesn't panic.
func (f *rawFrame) Reset()         { f.Payload = nil }
func (f *rawFrame) String() string { return "<rawFrame>" }
func (f *rawFrame) ProtoMessage()  {}

func init() {
	// Register the codec with gRPC's global registry so it's nameable.
	encoding.RegisterCodec(rawCodec{})
}

// cachingStreamHandler is the StreamHandler we install via
// grpc.UnknownServiceHandler. Its decision tree per request:
//
//  1. Look up the inbound method against the rules. If no matching
//     allow rule has Cache.Enable=true, fall through to the standard
//     transparent forwarder (mwitkow). Streaming methods naturally
//     end up on this path because no operator should mark a stream
//     cacheable.
//  2. For cacheable methods: read ONE request frame, hash it, check
//     the cache. On hit, write the cached frame to the server stream
//     and return. On miss, invoke the upstream via grpc.Invoke with
//     our codec, cache the response payload, write it to the server
//     stream.
//
// The handler runs in a freshly-spawned goroutine per RPC, so the
// stateful reads from the stream are fine to do inline.
func cachingStreamHandler(
	p *GrpcProxy,
	transparent grpc.StreamHandler,
) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(stream)
		if !ok {
			return transparent(srv, stream)
		}

		// Start the per-RPC span here so its lifetime covers the FULL
		// stream — cache lookup, upstream Invoke, transparent forward,
		// and all bidi pumping. Previously Handle (the director) opened
		// the span and ended it on return, which closed the span before
		// the actual RPC bytes flowed and rendered the trace as a
		// zero-duration span. The director still annotates this span
		// for deny / no-upstream outcomes via trace.SpanFromContext.
		tracer := otel.GetTracerProvider().Tracer(tracerName)
		spanCtx, span := tracer.Start(stream.Context(), "grpc "+method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", method),
			),
		)
		defer span.End()
		stream = spannedServerStream{ServerStream: stream, ctx: spanCtx}

		// Reflection MUST keep working when the rules allow it. We
		// don't try to cache reflection responses.
		rule := p.findMatchingAllowRule(method)
		if rule == nil || rule.Cache == nil || !rule.Cache.Enable {
			return transparent(srv, stream)
		}

		// Cacheable methods MUST still pass the full policy gate before
		// we touch the cache. enforcePolicy runs auth + per-rule auth +
		// rate-limit (re-matching rules in priority order, so a
		// higher-priority deny is enforced even though findMatchingAllowRule
		// only scanned for an allow) and returns the upstream context with
		// credentials stripped. Crucially it does NOT pick an upstream —
		// so a cache hit burns no round-robin selection, and on a miss we
		// Pick exactly once below.
		outCtx, gateErr := p.enforcePolicy(stream.Context(), method)
		if gateErr != nil {
			return gateErr
		}

		// Read the request frame. If the method is actually streaming,
		// the operator misconfigured the cache; we still proceed (read
		// the first message, treat it as the whole request). Document
		// the assumption.
		var req rawFrame
		if err := stream.RecvMsg(&req); err != nil {
			return err
		}

		metaPart := grpcCacheKeyMetaPart(stream.Context(), rule.Cache.EffectiveKeyMetadata())
		key := grpcCacheKey(rule.Fingerprint, method, req.Payload, rule.Cache.KeyMode, p.canonical, metaPart)

		// Background refreshes keep the credential-stripped metadata but
		// must not inherit the triggering client's cancellation.
		md, _ := metadata.FromOutgoingContext(outCtx)

		if cached, err := p.grpcCache.Get(stream.Context(), key); err == nil {
			effTTL := effectiveTTL(rule.Cache, p.cacheConfig)
			stale := resolveStaleWindow(rule.Cache, cfgStaleWindow(p.cacheConfig))
			switch classifyFreshness(cached.StoredAt, nowOrDefault(p.now), effTTL, stale) {
			case freshEntry:
				_ = stream.SetHeader(metadata.Pairs(grpcCacheMetaKey, cacheHit))
				return stream.SendMsg(&rawFrame{Payload: cached.Payload})
			case staleEntry:
				// Serve the stale payload immediately and refresh in the
				// background (coalesced by key) so the client never waits.
				_ = stream.SetHeader(metadata.Pairs(grpcCacheMetaKey, cacheStale))
				p.sf.refresh(key, p.grpcRefreshFn(md, method, req.Payload, key, rule))
				return stream.SendMsg(&rawFrame{Payload: cached.Payload})
				// expiredEntry falls through to a miss.
			}
		}

		// Miss: fetch upstream, coalescing concurrent misses for the same key
		// into ONE Invoke when coalescing is enabled (the default). Coalesced
		// waiters honor their own stream deadline; the shared fetch runs under
		// a detached context so one client's disconnect can't abort it.
		var (
			out      grpcCachedResponse
			fetchErr error
		)
		if resolveCoalesce(rule.Cache, cfgCoalesce(p.cacheConfig)) {
			owner := &grpcResponseOwner{}
			out, fetchErr = p.sf.do(stream.Context(), key, p.grpcForegroundFetchFn(outCtx, method, req.Payload, key, rule, owner))
			if fetchErr != nil && stream.Context().Err() != nil {
				return fetchErr
			}
			if out.owner != owner && (!out.shareable || fetchErr != nil) {
				out, fetchErr = p.grpcFetchAndStore(outCtx, method, req.Payload, key, rule)
			}
		} else {
			out, fetchErr = p.grpcFetchAndStore(outCtx, method, req.Payload, key, rule)
		}
		if fetchErr != nil {
			return fetchErr
		}
		_ = stream.SetHeader(metadata.Pairs(grpcCacheMetaKey, cacheMiss))
		return stream.SendMsg(&rawFrame{Payload: out.Payload})
	}
}

// grpcForegroundFetchFn detaches the shared upstream call from any one
// waiter's cancellation or deadline while preserving outgoing metadata.
func (p *GrpcProxy) grpcForegroundFetchFn(outCtx context.Context, method string, reqPayload []byte, key string, rule *GrpcRule, owner *grpcResponseOwner) func() (grpcCachedResponse, error) {
	return func() (grpcCachedResponse, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(outCtx), configuredGRPCForegroundFetchTimeout(p.cacheConfig))
		defer cancel()
		out, err := p.grpcFetchAndStore(ctx, method, reqPayload, key, rule)
		out.owner = owner
		return out, err
	}
}

// grpcRefreshFn bounds detached stale-while-revalidate work so a wedged
// upstream cannot pin a background goroutine indefinitely.
func (p *GrpcProxy) grpcRefreshFn(md metadata.MD, method string, reqPayload []byte, key string, rule *GrpcRule) func() (grpcCachedResponse, error) {
	return func() (grpcCachedResponse, error) {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), grpcRefreshTimeout)
		defer cancel()
		return p.grpcFetchAndStore(ctx, method, reqPayload, key, rule)
	}
}

// grpcFetchAndStore performs ONE upstream Invoke and stores the cacheable
// response (with StoredAt and a physical TTL extended by the stale window). It
// Picks its own upstream so coalesced waiters don't each burn a selection.
// Empty payloads are never cached (an empty frame is indistinguishable from an
// upstream-sent-nothing error and would poison the entry until TTL).
func (p *GrpcProxy) grpcFetchAndStore(fetchCtx context.Context, method string, reqPayload []byte, key string, rule *GrpcRule) (grpcCachedResponse, error) {
	upstream := p.pool.Pick()
	if upstream == nil {
		return grpcCachedResponse{}, status.Error(codes.Unavailable, "no upstream available")
	}
	var resp rawFrame
	// Pick already reserved the in-flight lease; release it after Invoke.
	invokeErr := upstream.conn.Invoke(fetchCtx, method, &rawFrame{Payload: reqPayload}, &resp, grpc.ForceCodec(rawCodec{}))
	upstream.inFlight.Add(-1)
	// Classify by status code so client-caused / application-level errors
	// (Canceled, DeadlineExceeded, InvalidArgument, NotFound…) don't trip the
	// breaker on a healthy upstream.
	upstream.RecordOutcomeErr(invokeErr)
	if invokeErr != nil {
		return grpcCachedResponse{}, invokeErr
	}
	out := grpcCachedResponse{
		Payload:   resp.Payload,
		StoredAt:  nowOrDefault(p.now).UTC(),
		shareable: len(resp.Payload) > 0,
	}
	if len(resp.Payload) > 0 {
		effTTL := effectiveTTL(rule.Cache, p.cacheConfig)
		stale := resolveStaleWindow(rule.Cache, cfgStaleWindow(p.cacheConfig))
		writeCtx, cancel := context.WithTimeout(context.Background(), grpcCacheWriteTimeout)
		_ = p.grpcCache.Set(writeCtx, key, out, physicalTTL(effTTL, stale))
		cancel()
	}
	return out, nil
}

// findMatchingAllowRule returns the first allow rule matching the method,
// or nil. Walks rules in priority order under the read lock.
func (p *GrpcProxy) findMatchingAllowRule(method string) *GrpcRule {
	p.rulesMutex.RLock()
	defer p.rulesMutex.RUnlock()
	for _, r := range p.rules {
		if r.Action == RuleActionAllow && r.Match(method) {
			return r
		}
	}
	return nil
}

// grpcCacheKey computes the per-rule namespaced cache key. xxhash for
// the payload + rule fingerprint as a prefix matches the HTTP layer's
// pattern from B5. keyMode selects how the payload contributes:
//
//   - "" / "raw": payload bytes go in verbatim. Cache hits only when
//     two clients serialize identically.
//   - "method-only": payload excluded entirely; one entry per
//     (rule, method). Safe only for parameter-less queries.
//   - "canonical": payload decoded against the operator-supplied
//     protoset (canonical registry) and re-encoded deterministically
//     before hashing. Cache hits across clients that emit the same
//     logical message regardless of byte-level differences.
//     Methods missing from the registry silently degrade to "raw".
//
// metaPart carries the response-affecting request metadata (e.g. block
// height) so requests differing only by that metadata don't collide. It is
// folded into EVERY mode — including method-only, where the payload is
// ignored but the height still selects a different response.
func grpcCacheKey(fingerprint uint64, method string, payload []byte, keyMode string, canonical *CanonicalRegistry, metaPart string) string {
	switch keyMode {
	case "method-only":
		return util.XXHash64Hex(
			fmt.Sprintf("%x\x00%s\x00%s", fingerprint, method, metaPart),
		)
	case "canonical":
		payload = canonical.Canonicalize(method, payload)
	}
	return util.XXHash64Hex(
		fmt.Sprintf("%x\x00%s\x00", fingerprint, method) + string(payload) + "\x00" + metaPart,
	)
}

// grpcCacheKeyMetaPart builds the deterministic metadata contribution to a
// gRPC cache key from the inbound context. keys are the rule's effective
// KeyMetadata allowlist; they are sorted so ordering can't change the key,
// and multi-valued metadata is joined so all values participate.
func grpcCacheKeyMetaPart(ctx context.Context, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	var b strings.Builder
	for _, k := range sorted {
		// gRPC canonicalizes metadata keys to lowercase.
		vals := md.Get(strings.ToLower(k))
		b.WriteString(strings.ToLower(k))
		b.WriteByte('=')
		// Quote each value so value boundaries are unambiguous: a single
		// value containing a separator (e.g. ["a,b"]) must NOT serialize
		// identically to two values (["a","b"]) — otherwise two requests
		// with different forwarded metadata could share a cache entry.
		// %q escapes embedded quotes, so the encoding is injective.
		fmt.Fprintf(&b, "%q", vals)
		b.WriteByte(';')
	}
	return b.String()
}

// Compile-time interface check.
var _ grpc.StreamHandler = (func(any, grpc.ServerStream) error)(nil)

// rawStreamDirector is the StreamDirector signature: given an inbound
// method, return an outgoing context + the upstream ClientConn to
// forward to. Mirrors mwitkow/grpc-proxy's StreamDirector so
// GrpcProxy.Handle can satisfy both shapes.
type rawStreamDirector func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error)

// rawTransparentHandler is the transparent forwarder cosmoguard uses
// in place of grpcproxy.TransparentHandler. The mwitkow handler uses
// *emptypb.Empty as its message buffer, which is incompatible with
// our ForceServerCodec(rawCodec) install — rawCodec deliberately only
// accepts *rawFrame so the caching layer's frame contents are
// statically guaranteed. Roll our own forwarder around *rawFrame and
// the codec passes payloads through unmodified.
//
// Semantics match mwitkow's TransparentHandler:
//   - Bidirectional streaming pump (both sides may send/receive).
//   - On client-to-server EOF, half-close the upstream and keep
//     pumping server-to-client until that side returns.
//   - On any non-EOF error from server-to-client, cancel the upstream
//     and return Internal to the caller.
//   - On any error (including EOF) from client-to-server, copy the
//     upstream trailer to the inbound stream before returning.
func rawTransparentHandler(director rawStreamDirector) grpc.StreamHandler {
	streamDesc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	return func(_ any, serverStream grpc.ServerStream) (retErr error) {
		method, ok := grpc.MethodFromServerStream(serverStream)
		if !ok {
			return status.Errorf(codes.Internal, "rawTransparentHandler: missing method on server stream")
		}
		outCtx, backendConn, err := director(serverStream.Context(), method)
		if err != nil {
			return err
		}

		// Account in-flight load + record the call outcome on the SAME
		// upstream the director picked, so transparent (non-cached) gRPC
		// traffic drives least-conn selection and the circuit breaker —
		// previously only the cache-miss Invoke path did. nil-safe when
		// no upstream was stashed (e.g. a non-GrpcProxy director in tests).
		if up := upstreamFromCtx(outCtx); up != nil {
			// Pick (in the director) already reserved the in-flight lease;
			// release it when the stream finishes.
			defer func() {
				up.inFlight.Add(-1)
				// Classify by status code: a client cancel/deadline or an
				// app-level status the node returns (InvalidArgument,
				// NotFound…) must not open the breaker on a healthy upstream.
				up.RecordOutcomeErr(retErr)
			}()
		}

		clientCtx, clientCancel := context.WithCancel(outCtx)
		defer clientCancel()

		clientStream, err := grpc.NewClientStream(clientCtx, streamDesc, backendConn, method)
		if err != nil {
			return err
		}

		s2cErrCh := rawForwardServerToClient(serverStream, clientStream)
		c2sErrCh := rawForwardClientToServer(clientStream, serverStream)
		// One side closes first; either is fine. We then handle the
		// other so the half-close / trailer propagation invariants
		// hold.
		for i := 0; i < 2; i++ {
			select {
			case s2cErr := <-s2cErrCh:
				if s2cErr == io.EOF {
					// Client done sending. Half-close the upstream and
					// keep pumping responses through.
					_ = clientStream.CloseSend()
					continue
				}
				// Inbound s2c failed mid-stream (non-EOF). Cancel the
				// upstream so its pump exits, drain c2sErrCh so the
				// goroutine doesn't leak, and surface the trailer from
				// whatever the upstream managed to emit before being
				// torn down — same invariant the EOF path holds. When
				// the drained c2s side carries a real upstream status
				// (e.g. the upstream returned a typed error before the
				// inbound pump tripped), prefer that over synthesizing
				// Internal so cosmoguard preserves the 100%-Cosmos-
				// node compatibility invariant for error codes.
				clientCancel()
				c2sErr := <-c2sErrCh
				serverStream.SetTrailer(clientStream.Trailer())
				if c2sErr != nil && c2sErr != io.EOF {
					if _, ok := status.FromError(c2sErr); ok {
						return c2sErr
					}
				}
				return status.Errorf(codes.Internal, "raw proxy s2c: %v", s2cErr)
			case c2sErr := <-c2sErrCh:
				serverStream.SetTrailer(clientStream.Trailer())
				if c2sErr != io.EOF {
					return c2sErr
				}
				return nil
			}
		}
		return status.Errorf(codes.Internal, "rawTransparentHandler: unreachable")
	}
}

// rawForwardServerToClient pumps inbound (client → cosmoguard) frames
// to the upstream client stream. Each iteration reads ONE frame, hands
// it across, releases the buffer for the next read. Uses a fresh
// rawFrame per iteration so a slow upstream send can't get back-
// referenced by the next recv.
//
// A panic inside the pump (out-of-bounds, nil deref in a malformed
// codec call, etc.) is recovered locally so the panic surfaces as a
// channel error instead of taking down the whole gRPC server process —
// matches recoverStream's invariant for the outer handler.
func rawForwardServerToClient(src grpc.ServerStream, dst grpc.ClientStream) chan error {
	ret := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Non-blocking send: the normal-path goroutine may have
				// already pushed an err onto ret before the panic
				// landed. ret is buffered 1, so a second blocking send
				// would deadlock the goroutine forever.
				select {
				case ret <- status.Errorf(codes.Internal, "raw proxy s2c panic: %v", r):
				default:
				}
			}
		}()
		for {
			f := &rawFrame{}
			if err := src.RecvMsg(f); err != nil {
				ret <- err
				return
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}

// rawForwardClientToServer pumps upstream (cosmoguard → client) frames
// to the inbound server stream. On the first iteration we also copy
// the upstream's response headers before flushing the first message
// — same hack mwitkow uses, since gRPC requires headers be sent
// before any body frame on the server stream.
func rawForwardClientToServer(src grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Non-blocking send: see rawForwardServerToClient for
				// rationale (same race; same deadlock risk).
				select {
				case ret <- status.Errorf(codes.Internal, "raw proxy c2s panic: %v", r):
				default:
				}
			}
		}()
		first := true
		for {
			f := &rawFrame{}
			if err := src.RecvMsg(f); err != nil {
				ret <- err
				return
			}
			if first {
				first = false
				md, err := src.Header()
				if err != nil {
					ret <- err
					return
				}
				if err := dst.SendHeader(md); err != nil {
					ret <- err
					return
				}
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}

// _ = cache is used to avoid an "imported and not used" if a future
// refactor drops the explicit reference. The pkg/cache import is held
// here for the gRPC cache integration to use.
var _ = struct{}{}
