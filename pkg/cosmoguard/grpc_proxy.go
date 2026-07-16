package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/olric-data/olric"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	cosmoguardcache "github.com/voluzi/cosmoguard/pkg/cache"
)

// grpcRateLimitKey derives the bucket key for a gRPC rate-limit
// scope. Same shape as rateLimitKey for HTTP: fingerprint-prefixed
// so different rules don't share buckets, and per-identity falls
// back to per-ip when no identity is resolved.
func grpcRateLimitKey(scope RateLimitScope, fingerprint uint64, source, identity string) string {
	prefix := strconv.FormatUint(fingerprint, 16)
	// Normalise the source to a bare IP (gRPC passes peer host:port,
	// JSON-RPC passes an already-bare IP) so per-ip / compound buckets
	// key on the client IP rather than the ephemeral connection port.
	ip := stripPort(source)
	switch scope {
	case RateLimitScopeGlobal:
		return prefix + ":global"
	case RateLimitScopePerIdentity:
		if identity != "" {
			return prefix + ":id:" + identity
		}
		return prefix + ":ip:" + ip
	case RateLimitScopeCompound:
		// identity+IP together: one identity across many IPs gets
		// independent budgets; anonymous callers fall back to per-IP.
		// Mirrors rateLimitKey so gRPC + JSON-RPC honour compound.
		if identity != "" {
			return prefix + ":compound:" + identity + "|" + ip
		}
		return prefix + ":ip:" + ip
	default: // per-ip
		return prefix + ":ip:" + ip
	}
}

// synthRequestFromGrpcMD wraps inbound gRPC metadata as an
// *http.Request so Authenticator.Resolve (which reads http.Header)
// can extract credentials from gRPC clients unchanged. The synthetic
// request carries only the headers — no body, no path — because the
// configured AuthMethods only inspect headers.
func synthRequestFromGrpcMD(md metadata.MD) *http.Request {
	// URL must be non-nil: an api-key AuthMethod configured in queryParam
	// mode falls back to r.URL.Query() when the header form is absent
	// (always the case over gRPC, where credentials only arrive as
	// metadata). A nil URL there panics the handler goroutine on every
	// keyless RPC. An empty URL yields an empty query, so the method simply
	// resolves no credential.
	r := &http.Request{URL: &url.URL{}, Header: make(http.Header, len(md))}
	for k, vs := range md {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	return r
}

type GrpcProxy struct {
	defaultAction RuleAction
	rules         []*GrpcRule
	listener      net.Listener
	server        *grpc.Server
	pool          *GrpcUpstreamPool
	rulesMutex    sync.RWMutex
	log           *Entry
	// grpcCache holds cached response bytes keyed by per-rule fingerprint
	// + method + request bytes. Populated when any rule has cache.enable
	// configured; nil otherwise (no overhead).
	grpcCache cosmoguardcache.Cache[string, []byte]
	// canonical holds protobuf method descriptors loaded from operator-
	// supplied protoset files. Used by rules with cache.keyMode:
	// canonical to decode + re-encode request payloads deterministically
	// before hashing. nil when no protosets are configured.
	canonical *CanonicalRegistry
	// cgDashboard is the optional observability sink for unmatched +
	// deny events. nil-safe.
	cgDashboard *dashboardObservability
	// section is the dashboard section name ("grpc").
	section string
	// auth runs the same identity-resolve + Authorize pipeline the HTTP
	// proxies use, with credentials lifted from the inbound gRPC
	// metadata. nil → no auth gate (legacy embedder paths only).
	auth *Authenticator
	// limiters maps a rule's Fingerprint to its token-bucket. Built
	// in SetRules from each rule's RateLimit; read under rulesMutex.
	limiters    map[uint64]RateLimiter
	olricClient *olric.EmbeddedClient
	proxyName   string
	setRulesMu  sync.Mutex
}

// SetDashboard wires the dashboard observability sink and section
// name. Called by cosmoguard.New after construction.
func (p *GrpcProxy) SetDashboard(section string, d *dashboardObservability) {
	p.section = section
	p.cgDashboard = d
}

// SetAuthenticator wires the Authenticator. Nil-safe; setting nil
// disables the auth gate.
func (p *GrpcProxy) SetAuthenticator(a *Authenticator) { p.auth = a }

// NewGrpcProxy constructs a gRPC proxy fronting a pool of upstream nodes.
// Each node is dialled lazily by grpc.Dial — connection state per upstream
// drives the picker's healthy set.
//
// Single-node pools take the fast path with no pool overhead.
//
// protosets is the optional list of binary FileDescriptorSet files used
// for canonical-key gRPC cache lookups; pass nil/empty to disable.
func NewGrpcProxy(name, localAddr string, nodes []NodeConfig, upstreamCfg *UpstreamConfig, protosets []string, opts ...Option[GrpcProxyOptions]) (*GrpcProxy, error) {
	cfg := DefaultGrpcProxyOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	lis, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, err
	}

	proxy := GrpcProxy{
		log:      log.WithField("proxy", name),
		listener: lis,
	}

	if len(protosets) > 0 {
		reg, err := LoadCanonicalRegistry(protosets)
		if err != nil {
			_ = lis.Close()
			return nil, fmt.Errorf("grpc canonical registry: %w", err)
		}
		proxy.canonical = reg
		proxy.log.WithField("methods", reg.MethodCount()).Info("loaded protobuf descriptors for canonical gRPC cache")
	}

	var poolOpts []GrpcUpstreamPoolOption
	if upstreamCfg != nil {
		poolOpts = append(poolOpts, WithGrpcUpstreamStrategy(upstreamCfg.Strategy))
	}
	pool, err := NewGrpcUpstreamPool(name, nodes, proxy.log, poolOpts...)
	if err != nil {
		return nil, err
	}
	proxy.pool = pool
	proxy.auth = cfg.Authenticator
	proxy.olricClient = cfg.OlricClient
	proxy.proxyName = name

	// Build a cache for gRPC responses. Dispatch (olric / redis / memory)
	// goes through newResponseCache so cached gRPC responses share the
	// same backend the HTTP / JSON-RPC caches do — the v4 default is
	// cluster-shared via the embedded olric DMap, which keeps the cache
	// hit-rate stable across HPA-driven scaling and rolling restarts.
	var cacheOptions []cosmoguardcache.Option
	if cfg.CacheConfig != nil {
		cacheOptions = append(cacheOptions, cosmoguardcache.DefaultTTL(cfg.CacheConfig.TTL))
	}
	gcache, err := newResponseCache[string, []byte](cfg.CacheConfig, cfg.OlricClient, name, cacheOptions...)
	if err != nil {
		return nil, err
	}
	proxy.grpcCache = gcache

	// rawTransparentHandler replaces mwitkow/grpc-proxy's TransparentHandler
	// because mwitkow's forwarder buffers messages in *emptypb.Empty,
	// which is incompatible with our ForceServerCodec(rawCodec)
	// install — rawCodec only accepts *rawFrame so caching-path frames
	// are statically guaranteed. See rawTransparentHandler in
	// grpc_cache.go for the bidi pump semantics.
	transparent := rawTransparentHandler(proxy.Handle)
	// UnknownServiceHandler bypasses gRPC's standard interceptor chain,
	// so panics inside cachingStreamHandler / TransparentHandler would
	// otherwise propagate up to the gRPC server's default recover with
	// no cosmoguard-side log. Wrap the handler in our own recover so
	// the panic is logged with stack + method context, and the client
	// gets a clean Internal error instead of a stream reset.
	handler := recoverStream(proxy.log, cachingStreamHandler(&proxy, transparent))
	server := grpc.NewServer(
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(handler),
	)
	proxy.server = server

	return &proxy, nil
}

func (p *GrpcProxy) Run() error {
	// Active probing closes the cold-start window where lazy grpc.Dial
	// leaves a broken peer in connectivity.Idle until the first RPC
	// tries it — without this, ~30% of RPCs to a 9-broken-of-10 pool
	// land on a broken peer in the first ~30s. No-op for nodes with
	// no healthcheck configured.
	if p.pool != nil {
		p.pool.StartHealthchecks()
	}
	p.log.WithField("address", p.listener.Addr().String()).Info("starting grpc proxy")
	return p.server.Serve(p.listener)
}

// Shutdown stops the gRPC server, closing the listener and waiting for
// in-flight RPCs to complete (gRPC's GracefulStop), then closes every
// upstream client connection. Honors the caller's deadline: a hung
// streaming RPC can otherwise pin GracefulStop forever, defeating the
// shutdownGrace the operator picked.
func (p *GrpcProxy) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		// graceful drain completed within the deadline.
	case <-ctx.Done():
		// Deadline expired before in-flight streams drained.
		// Hard-kill so the rest of Shutdown can proceed; otherwise
		// pool.Close races GracefulStop and downstream RPCs hang
		// on a closed client conn.
		p.server.Stop()
		<-done
	}
	if p.pool != nil {
		p.pool.Close()
	}
	if p.grpcCache != nil {
		_ = p.grpcCache.Close()
	}
	return nil
}

func (p *GrpcProxy) SetRules(rules []*GrpcRule, defaultAction RuleAction) {
	// setRulesMu serialises concurrent SetRules so the limiter rebuild
	// doesn't race with itself across hot-reloads. See HttpProxy.SetRules
	// for the same idiom.
	p.setRulesMu.Lock()
	defer p.setRulesMu.Unlock()

	p.rulesMutex.RLock()
	existing := p.limiters
	p.rulesMutex.RUnlock()

	newLimiters := map[uint64]RateLimiter{}
	for _, r := range rules {
		if r.RateLimit == nil {
			continue
		}
		if l, ok := existing[r.Fingerprint]; ok {
			newLimiters[r.Fingerprint] = l
			continue
		}
		keyspace := p.proxyName + ":rl:" + strconv.FormatUint(r.Fingerprint, 16)
		l, err := NewRateLimiter(*r.RateLimit, p.olricClient, keyspace)
		if err != nil {
			p.log.WithError(err).WithField("rule_priority", r.Priority).
				Error("rate limiter init failed; rule will run without limit")
			continue
		}
		newLimiters[r.Fingerprint] = l
	}

	p.rulesMutex.Lock()
	old := p.limiters
	p.rules = rules
	p.defaultAction = defaultAction
	p.limiters = newLimiters
	p.rulesMutex.Unlock()

	for fp, l := range old {
		if _, kept := newLimiters[fp]; !kept {
			_ = l.Close()
		}
	}
}

// enforcePolicy runs the auth + per-rule auth + rate-limit + rule-action
// decision for method and returns the outgoing context (inbound metadata
// MINUS the stripped credentials) when the call is allowed, or a gRPC
// status error when it is denied. It deliberately does NOT select an
// upstream — picking is the caller's job (Handle for the transparent
// forwarder; the cache handler only on a miss). Conflating the two made
// a cacheable method's policy check burn a round-robin Pick (skewing
// load to one backend) and forwarded credentials upstream.
func (p *GrpcProxy) enforcePolicy(ctx context.Context, method string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	// The per-RPC span is started by cachingStreamHandler so its lifetime
	// covers the full RPC; here we just annotate it for deny outcomes.
	span := trace.SpanFromContext(ctx)
	markErrSpan := func(reason string) { span.SetStatus(otelcodes.Error, reason) }

	// outMD is the metadata forwarded upstream. Credentials are removed
	// from it after a successful Resolve (below) so API keys / JWTs never
	// reach the upstream node — the gRPC equivalent of the HTTP layer's
	// StripCredentialHeaders.
	outMD := md.Copy()

	var source string
	if pr, ok := peer.FromContext(ctx); ok {
		source = pr.Addr.String()
	}

	// gRPC reflection is gated like any other method — operators who
	// need it must explicitly allow /grpc.reflection.* via a rule.

	var id *Identity
	if p.auth != nil {
		synthReq := synthRequestFromGrpcMD(md)
		var err error
		id, err = p.auth.Resolve(synthReq)
		if err != nil {
			if errors.Is(err, ErrReplay) || errors.Is(err, ErrInvalidCredential) {
				p.cgDashboard.RecordDeny(DenyRecord{
					Section: p.section, Reason: "auth",
					SourceIP: source, Method: method,
				})
				markErrSpan("auth: " + err.Error())
				return ctx, status.Error(codes.Unauthenticated, err.Error())
			}
			// Other resolve errors (transient auth-backend failures, JWKS
			// fetch hiccups): treat as anonymous (fail-open) so the SAME
			// underlying error yields the SAME allow/deny across HTTP and
			// gRPC — the HTTP MWIdentityResolve path does exactly this. The
			// per-rule auth gate below still denies if the rule requires an
			// identity, and a fail-closed auth method surfaces its decision
			// as ErrInvalidCredential (handled above), not as a transient
			// error. Previously gRPC hard-denied here while HTTP admitted
			// anonymously — a transient blip took gRPC down but not HTTP.
			p.log.WithError(err).Warn("grpc auth resolve error; treating request as anonymous")
			id = nil
		}
		// Strip the configured credential metadata (gRPC keys are
		// lower-cased) so it is never forwarded upstream.
		for _, name := range p.auth.CredentialHeaderNames() {
			delete(outMD, strings.ToLower(name))
		}
		// NOTE: the global auth.defaultRequire gate is applied per-rule
		// (and on the no-match default path) below, NOT here — matching
		// the HTTP ordering so a rule with auth.require:false can opt a
		// public method out. A forged/replayed credential is still
		// rejected above regardless of any rule.
	}
	outCtx := metadata.NewOutgoingContext(ctx, outMD)

	p.rulesMutex.RLock()
	defer p.rulesMutex.RUnlock()
	for _, rule := range p.rules {
		if !rule.Match(method) {
			continue
		}
		// Per-rule auth, applied for every matched rule when auth is
		// enabled. rule.Auth may be nil → Authorize falls back to the
		// global auth.defaultRequire, so a rule can opt out
		// (auth.require:false) or tighten (scopes/identities).
		if p.auth != nil {
			if ok, reason := p.auth.Authorize(rule.Auth, id); !ok {
				p.cgDashboard.RecordDeny(DenyRecord{
					Section: p.section, Reason: "auth",
					SourceIP: source, Method: method,
					RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
				})
				markErrSpan("auth: " + reason)
				return ctx, status.Error(codes.Unauthenticated, reason)
			}
		}
		// Per-rule rate-limit: token bucket keyed by scope.
		if l, ok := p.limiters[rule.Fingerprint]; ok && l != nil {
			idName := ""
			if id != nil {
				idName = id.Name
			}
			key := grpcRateLimitKey(rule.RateLimit.Scope, rule.Fingerprint, source, idName)
			allowed, retryAfter, rlErr := l.Allow(ctx, key)
			if rlErr != nil {
				if rule.RateLimit.FailClosed() {
					p.log.WithError(rlErr).Warn("grpc rate limiter error; failing closed (denying)")
					p.cgDashboard.RecordDeny(DenyRecord{
						Section: p.section, Reason: "rate_limit",
						SourceIP: source, Method: method,
						RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
					})
					markErrSpan("rate limiter unavailable")
					return ctx, status.Error(codes.ResourceExhausted, "rate limiter unavailable")
				}
				// Fail-open (default) on limiter transport error so an olric
				// blip doesn't take down traffic.
				p.log.WithError(rlErr).Warn("grpc rate limiter error; allowing")
			} else if !allowed {
				p.cgDashboard.RecordDeny(DenyRecord{
					Section: p.section, Reason: "rate_limit",
					SourceIP: source, Method: method,
					RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
				})
				markErrSpan("rate limited")
				st := status.New(codes.ResourceExhausted, "rate limit exceeded")
				if retryAfter > 0 {
					_, _ = st.WithDetails() // placeholder if we ever want RetryInfo proto
				}
				return ctx, st.Err()
			}
		}
		switch rule.Action {
		case RuleActionAllow:
			p.log.WithFields(map[string]interface{}{
				"method": method, "source": source, "action": "allow",
			}).Info("request allowed")
			return outCtx, nil

		case RuleActionDeny:
			p.log.WithFields(map[string]interface{}{
				"method": method, "source": source, "action": "deny",
			}).Info("request denied")
			p.cgDashboard.RecordDeny(DenyRecord{
				Section:  p.section,
				Reason:   "rule",
				SourceIP: source,
				Method:   method,
				RuleTag:  ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
			})
			markErrSpan("denied by rule")
			return ctx, status.Errorf(codes.Unavailable, "Unauthorized")

		default:
			log.Errorf("unrecognized rule action %q", rule.Action)
		}
	}

	// No rule matched — record (method, "") under the gRPC section.
	p.cgDashboard.RecordUnmatched(p.section, method, "")
	// Apply the global auth.defaultRequire to unmatched methods (no rule
	// opted them out). Authorize(nil, id) is a pass-through when
	// defaultRequire is off.
	if p.auth != nil {
		if ok, reason := p.auth.Authorize(nil, id); !ok {
			p.cgDashboard.RecordDeny(DenyRecord{
				Section: p.section, Reason: "auth",
				SourceIP: source, Method: method,
			})
			markErrSpan("auth: " + reason)
			return ctx, status.Error(codes.Unauthenticated, reason)
		}
	}
	if p.defaultAction == RuleActionAllow {
		p.log.WithFields(map[string]interface{}{
			"method": method, "source": source, "action": "allow",
		}).Info("request allowed")
		return outCtx, nil
	}
	markErrSpan("denied by default action")
	p.log.WithFields(map[string]interface{}{
		"method": method, "source": source, "action": "deny",
	}).Info("request denied")
	p.cgDashboard.RecordDeny(DenyRecord{
		Section:  p.section,
		Reason:   "default",
		SourceIP: source,
		Method:   method,
	})
	return ctx, status.Errorf(codes.Unavailable, "Unauthorized")
}

// grpcUpstreamCtxKey carries the picked *GrpcUpstream from Handle to the
// transparent forwarder so the forwarder can account in-flight load and
// record the call outcome on the SAME upstream it dialled. The director
// signature can only return a *grpc.ClientConn, so the upstream travels
// on the context instead.
type grpcUpstreamCtxKey struct{}

// upstreamFromCtx recovers the picked upstream stashed by Handle, or nil.
func upstreamFromCtx(ctx context.Context) *GrpcUpstream {
	up, _ := ctx.Value(grpcUpstreamCtxKey{}).(*GrpcUpstream)
	return up
}

// Handle is the StreamDirector for the transparent forwarder: enforce
// policy, then select an upstream. The cache path calls enforcePolicy
// directly and picks only on a miss, so it never burns a pick on a hit.
func (p *GrpcProxy) Handle(ctx context.Context, method string) (context.Context, *grpc.ClientConn, error) {
	outCtx, err := p.enforcePolicy(ctx, method)
	if err != nil {
		return ctx, nil, err
	}
	up := p.pool.Pick()
	if up == nil {
		trace.SpanFromContext(ctx).SetStatus(otelcodes.Error, "no upstream available")
		return ctx, nil, status.Error(codes.Unavailable, "no upstream available")
	}
	// Stash the picked upstream so rawTransparentHandler can bump
	// inFlight (for least-conn) and RecordOutcome (for the circuit
	// breaker) — otherwise transparent gRPC traffic never affects load
	// or circuit state and only the cache-miss Invoke path does.
	outCtx = context.WithValue(outCtx, grpcUpstreamCtxKey{}, up)
	return outCtx, up.conn, nil
}
