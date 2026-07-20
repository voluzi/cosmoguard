package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/voluzi/cosmoguard/pkg/cache"
)

const (
	defaultWebsocketPath = "/websocket"

	// defaultWSReadLimit bounds a single inbound client WS frame when no
	// ServerConfig is threaded through the constructor. 1 MiB is generous
	// for any legitimate Cosmos/EVM frame (including a large
	// eth_sendRawTransaction) while still capping worst-case heap from a
	// single oversized frame. Deployments that need a different value set
	// server.wsReadLimit; 0 there means "no explicit limit".
	defaultWSReadLimit int64 = 1 << 20
)

type JsonRpcWebSocketProxy struct {
	broker *Broker
	cache  cache.Cache[uint64, *JsonRpcMsg]
	// cacheConfig resolves the cluster-wide stale-while-revalidate / ttl
	// defaults a rule inherits; now is swappable for deterministic tests.
	// Both set by the parent JsonRpcHandler. nil/unset falls back safely.
	cacheConfig      *CacheGlobalConfig
	now              func() time.Time
	wsBackends       []string
	upgrader         *websocket.Upgrader
	rules            []*JsonRpcRule
	defaultAction    RuleAction
	rulesMutex       sync.RWMutex
	log              *Entry
	responseTimeHist *prometheus.HistogramVec
	// wsReadLimit caps the size of any single inbound WebSocket message.
	// 0 means "use gorilla's default" (legacy behavior).
	wsReadLimit int64
	// cgDashboard is the optional observability sink, propagated from
	// the parent JsonRpcHandler via SetDashboard. nil-safe.
	cgDashboard *dashboardObservability
	// section is the dashboard section name ("evm.ws", "rpc.jsonrpc").
	section string
	// path is the WS handshake path ("/websocket", "/"). Stored so the
	// request-log entries carry the same path the client connected on.
	path string
	// auth is the shared Authenticator. Currently retained for symmetry
	// with the HTTP / gRPC sides — per-rule WS auth enforcement (using
	// JsonRpcRule.Auth) is queued for a follow-up.
	auth *Authenticator
	// cgRequestLog captures per-frame + connect/close metadata for the
	// dashboard's Live-traffic feed. nil-safe.
	cgRequestLog *requestLog

	// limiters maps a rule's Fingerprint to its token-bucket, forwarded
	// from JsonRpcHandler.SetRules so per-rule rate limits apply to WS
	// frames the same way they do on the HTTP single/batch paths. Read
	// under rulesMutex.
	limiters map[uint64]RateLimiter

	// conns is the live registry of connected clients, keyed by client
	// pointer. Written once per connection lifecycle (register on
	// upgrade, deregister on close) — never on the per-frame hot path.
	connsMu sync.Mutex
	conns   map[*JsonRpcWsClient]*wsConnInfo
}

// wsConnInfo is the per-connection metadata the WebSockets panel renders.
type wsConnInfo struct {
	sourceIP    string
	identity    string
	connectedAt time.Time
}

// SetRequestLog wires the Live-traffic request log. Nil-safe.
func (p *JsonRpcWebSocketProxy) SetRequestLog(rl *requestLog) { p.cgRequestLog = rl }

// SetDashboard wires the dashboard observability sink and section
// name. Called by JsonRpcHandler.SetDashboard.
func (p *JsonRpcWebSocketProxy) SetDashboard(section string, d *dashboardObservability) {
	p.section = section
	p.cgDashboard = d
}

// SetAuthenticator wires the Authenticator. Nil-safe.
func (p *JsonRpcWebSocketProxy) SetAuthenticator(a *Authenticator) { p.auth = a }

func NewJsonRpcWebSocketProxy(name string, backends []string, path string, connections int, upstreamConstructor UpstreamConnManagerConstructor,
	cache cache.Cache[uint64, *JsonRpcMsg], metricsEnabled bool, serverCfg *ServerConfig) (*JsonRpcWebSocketProxy, error) {
	proxy := &JsonRpcWebSocketProxy{
		broker:     NewBroker(backends, path, connections, upstreamConstructor),
		wsBackends: backends,
		upgrader:   &websocket.Upgrader{},
		cache:      cache,
		path:       path,
		conns:      map[*JsonRpcWsClient]*wsConnInfo{},
	}
	var allowed []string
	if serverCfg != nil {
		// EffectiveWSReadLimit honors an explicit 0 as "no limit" (per the
		// field doc) while defaulting nil to a bounded 1 MiB — a `*int64`
		// so an explicit 0 survives defaults.Set. The previous `<= 0 →
		// 64 KiB` override silently clamped an explicit 0 AND contradicted
		// the doc, closing connections that sent legitimately large frames
		// (e.g. a big eth_sendRawTransaction).
		proxy.wsReadLimit = serverCfg.EffectiveWSReadLimit()
		allowed = serverCfg.WSAllowedOrigins
	} else {
		// No ServerConfig threaded through (programmatic embedders, tests):
		// apply a bounded default so a client can't pin large heap with one
		// oversized frame.
		proxy.wsReadLimit = defaultWSReadLimit
	}

	check, err := compileOriginAllowlist(allowed)
	if err != nil {
		return nil, fmt.Errorf("ws %s: compile allowed origins: %w", name, err)
	}
	proxy.upgrader.CheckOrigin = check

	if metricsEnabled {
		// Mirrors the JSON-RPC HTTP histogram label set: method + cache
		// + cosmoguard (action) + rule_id (operator-supplied tag) +
		// upstream (which backend served the WS frame). All bounded.
		proxy.responseTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: fmt.Sprintf("websocket_%s", name),
			Name:      "request_duration_seconds",
			Help:      "Histogram of response time for handler in seconds",
			Buckets:   responseTimeBuckets,
		}, []string{"method", "cache", "action", "rule_id", "upstream"})
	}

	return proxy, nil
}

func (p *JsonRpcWebSocketProxy) Run(log *Entry) error {
	p.log = log.WithField("type", "websocket")
	if p.responseTimeHist != nil {
		// Same swap-on-AlreadyRegistered fix as HttpProxy.Run /
		// JsonRpcHandler.Start so a second WS proxy in the same
		// process observes into the registered vec instead of its
		// own private one (which /metrics never gathers from).
		if err := prometheus.Register(p.responseTimeHist); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
					p.responseTimeHist = existing
				}
			}
		}
	}
	return p.broker.Start(p.log)
}

func (p *JsonRpcWebSocketProxy) SetRules(rules []*JsonRpcRule, defaultAction RuleAction, limiters map[uint64]RateLimiter) {
	p.rulesMutex.Lock()
	defer p.rulesMutex.Unlock()

	p.rules = rules
	p.defaultAction = defaultAction
	p.limiters = limiters
}

// policyVerdict runs the matched rule's per-rule auth + rate-limit
// checks against the connection's resolved identity, recording any deny.
// Writes nothing — handleRequest emits the JSON-RPC error frame. Mirrors
// the HTTP path's jsonRpcPolicyVerdict so WS frames can't bypass per-rule
// scopes/identities or rate limits that the HTTP single/batch paths
// enforce. The global auth.defaultRequire gate already ran on the WS
// upgrade request (HttpProxy gate chain), so this covers per-rule policy.
func (p *JsonRpcWebSocketProxy) policyVerdict(request *JsonRpcMsg, rule *JsonRpcRule, identity *Identity, source string, limiters map[uint64]RateLimiter) (ok bool, code int, reason string) {
	// Always Authorize when auth is enabled (rule.Auth may be nil →
	// Authorize applies the global auth.defaultRequire); a nil-guard
	// would let a WS rule without an auth block bypass defaultRequire.
	if p.auth != nil {
		if authOK, why := p.auth.Authorize(rule.Auth, identity); !authOK {
			p.cgDashboard.RecordDeny(DenyRecord{
				Section: p.section, Reason: "auth",
				SourceIP: source, Method: request.Method,
				RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
			})
			return false, -32001, why
		}
	}
	if l, found := limiters[rule.Fingerprint]; found && l != nil {
		idName := ""
		if identity != nil {
			idName = identity.Name
		}
		key := grpcRateLimitKey(rule.RateLimit.Scope, rule.Fingerprint, source, idName)
		allowed, _, rlErr := l.Allow(context.Background(), key)
		if rlErr != nil {
			if rule.RateLimit.FailClosed() {
				p.log.WithError(rlErr).Warn("ws rate limiter error; failing closed (denying)")
				p.cgDashboard.RecordDeny(DenyRecord{
					Section: p.section, Reason: "rate_limit",
					SourceIP: source, Method: request.Method,
					RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
				})
				return false, -32005, "rate limiter unavailable"
			}
			p.log.WithError(rlErr).Warn("ws rate limiter error; allowing")
		} else if !allowed {
			p.cgDashboard.RecordDeny(DenyRecord{
				Section: p.section, Reason: "rate_limit",
				SourceIP: source, Method: request.Method,
				RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
			})
			return false, -32005, "rate limit exceeded"
		}
	}
	return true, 0, ""
}

func (p *JsonRpcWebSocketProxy) HandleConnection(w http.ResponseWriter, r *http.Request) {
	// Pre-upgrade panic recovery — once we've upgraded, the WS-loop
	// defer below kicks in.
	defer recoverHTTP(p.log, w, r)

	// Tracing: a span covering the entire WS-connection lifetime. Spans
	// per-frame would be too noisy; this is sufficient for join with the
	// rest of the request graph.
	_, span := StartHTTPSpan(r, "ws")
	defer span.End()

	conn, err := p.upgrader.Upgrade(w, r, nil)
	if err != nil {
		p.log.Errorf("error upgrading connection to websocket: %v", err)
		return
	}
	if p.wsReadLimit > 0 {
		conn.SetReadLimit(p.wsReadLimit)
	}

	client := NewJsonRpcWsClient(conn)
	defer client.Close()
	// Post-upgrade panic recovery — no response writer to reply on, but
	// the connection closes cleanly on goroutine return.
	defer recoverWS(p.log, r.RemoteAddr)

	source := GetSourceIP(r)
	identity := ""
	// idObj is the connection-level identity resolved by the HTTP gate
	// chain during the WS upgrade. The same identity applies to every
	// frame on this connection; per-rule auth/rate-limit checks below
	// evaluate against it.
	var idObj *Identity
	if id, ok := r.Context().Value(identityCtxKey{}).(*Identity); ok && id != nil {
		identity = id.Name
		idObj = id
	}
	connectedAt := time.Now()
	p.registerConn(client, source, identity, connectedAt)
	p.recordLifecycle("CONNECT", source, identity, http.StatusSwitchingProtocols, 0)
	defer func() {
		p.deregisterConn(client)
		p.recordLifecycle("CLOSE", source, identity, http.StatusOK, time.Since(connectedAt))
	}()

	for {
		req, err := client.ReceiveMsg()
		if err != nil {
			// A malformed / unsupported frame on an otherwise healthy
			// connection must NOT tear down the client and drop all its
			// subscriptions. Reply with the appropriate JSON-RPC error
			// (id: null) and keep reading — invalid JSON → -32700 Parse
			// Error, valid-JSON-but-not-a-single-request (e.g. a batch) →
			// -32600 Invalid Request. If the write fails the socket really
			// is dead, so fall through to close.
			if errors.Is(err, ErrBadMessage) || errors.Is(err, ErrInvalidRequest) {
				p.log.Warnf("bad message from client: %v", err)
				resp := ParseErrorResponse()
				if errors.Is(err, ErrInvalidRequest) {
					resp = InvalidRequestResponse()
				}
				if sendErr := client.SendMsg(resp); sendErr == nil {
					continue
				}
				client.Close()
				break
			}
			if errors.Is(err, ErrClosed) {
				p.log.Warnf("client disconnected")
				client.Close()
				break
			}
			// Any other read error means the conn is no longer
			// usable — gorilla doesn't recover from read failures
			// and ReceiveMsg has already closed the underlying
			// conn. Break out of the loop instead of continuing,
			// which would hot-loop on the now-dead conn and leak
			// this goroutine for the life of the process.
			p.log.Errorf("error reading message: %v", err)
			client.Close()
			break
		}

		if err := p.handleRequest(client, req, source, idObj); err != nil {
			p.log.Errorf("error handling request: %v", err)
			continue
		}
	}
}

// registerConn adds a freshly-upgraded client to the live registry.
func (p *JsonRpcWebSocketProxy) registerConn(c *JsonRpcWsClient, sourceIP, identity string, at time.Time) {
	p.connsMu.Lock()
	p.conns[c] = &wsConnInfo{sourceIP: sourceIP, identity: identity, connectedAt: at}
	p.connsMu.Unlock()
}

// deregisterConn drops a client from the live registry on disconnect.
func (p *JsonRpcWebSocketProxy) deregisterConn(c *JsonRpcWsClient) {
	p.connsMu.Lock()
	delete(p.conns, c)
	p.connsMu.Unlock()
}

// recordLifecycle pushes a CONNECT / CLOSE row into the Live-traffic
// feed so the connection lifecycle shows up alongside per-frame rows.
// latency carries the connection lifetime on CLOSE (zero on CONNECT).
func (p *JsonRpcWebSocketProxy) recordLifecycle(event, sourceIP, identity string, status int, latency time.Duration) {
	if p.cgRequestLog == nil {
		return
	}
	p.cgRequestLog.Record(RequestLogEntry{
		Section:   p.section,
		Method:    event,
		Path:      p.path,
		Status:    status,
		LatencyMs: latency.Milliseconds(),
		Action:    "ws",
		Identity:  identity,
		SourceIP:  sourceIP,
	})
}

// StatsSnapshot returns this section's live WebSocket view for the
// dashboard panel. Read-only: the registry lock is held only to copy
// pointers, and the broker accessors take their own short locks.
func (p *JsonRpcWebSocketProxy) StatsSnapshot() WSSectionStats {
	p.connsMu.Lock()
	clients := make([]*JsonRpcWsClient, 0, len(p.conns))
	conns := make([]WSConnInfo, 0, len(p.conns))
	for c, info := range p.conns {
		clients = append(clients, c)
		conns = append(conns, WSConnInfo{
			SourceIP:    info.sourceIP,
			Identity:    info.identity,
			ConnectedMs: info.connectedAt.UnixMilli(),
		})
	}
	p.connsMu.Unlock()

	clientSubs := 0
	for i, c := range clients {
		n := p.broker.ClientSubCount(c)
		conns[i].Subscriptions = n
		clientSubs += n
	}

	subs := p.broker.SubStats()
	if subs == nil {
		subs = []WSSubInfo{}
	}
	ups := p.broker.UpstreamStats()
	if ups == nil {
		ups = []ConnStat{}
	}
	healthy := 0
	for _, u := range ups {
		if u.Healthy {
			healthy++
		}
	}
	return WSSectionStats{
		Section:               p.section,
		Path:                  p.path,
		Connections:           len(conns),
		ClientSubscriptions:   clientSubs,
		UpstreamSubscriptions: len(subs),
		UpstreamConnsHealthy:  healthy,
		UpstreamConnsTotal:    len(ups),
		Conns:                 conns,
		Subs:                  subs,
		Upstreams:             ups,
	}
}

// recordOutcome emits the standardized per-WS-request log + Prometheus
// observation. Mirrors HttpProxy.recordOutcome / JsonRpcHandler.recordSingle
// for WS subscribers — same label set, different histogram.
//
// ruleID and upstream complete the Phase H label set; upstream is "" on
// the WS path because the broker's connection pool picks per-frame and
// that picking lives inside ws_upstream_pool. Operators who need a
// per-upstream label on WS metrics can wire it up via the broker's
// onSubscriptionMessage path in a future slice.
func (p *JsonRpcWebSocketProxy) recordOutcome(request *JsonRpcMsg, source, cacheState, action, ruleID string, startTime time.Time, msg string) {
	duration := time.Since(startTime)
	if ruleID == "" {
		ruleID = "default"
	}
	p.log.WithFields(Fields{
		"id":       request.ID,
		"method":   request.Method,
		"params":   request.Params,
		"cache":    cacheState,
		"duration": duration,
		"source":   source,
		"rule_id":  ruleID,
	}).Info(msg)
	if p.responseTimeHist != nil {
		p.responseTimeHist.WithLabelValues(
			request.Method,
			cacheState,
			action,
			ruleID,
			"", // upstream — see doc above
		).Observe(duration.Seconds())
	}
	// Live-traffic feed: one row per WS frame. Status is synthesized
	// from the action so the request log's success/denied filters work
	// the same as on the HTTP path (deny → 401, otherwise 200).
	if p.cgRequestLog != nil {
		status := http.StatusOK
		if action == string(RuleActionDeny) {
			status = http.StatusUnauthorized
		}
		p.cgRequestLog.Record(RequestLogEntry{
			Section:    p.section,
			Method:     request.Method,
			Path:       p.path,
			Status:     status,
			LatencyMs:  duration.Milliseconds(),
			CacheState: cacheState,
			Action:     action,
			RuleTag:    ruleID,
			SourceIP:   source,
		})
	}
}

func (p *JsonRpcWebSocketProxy) handleRequest(client *JsonRpcWsClient, request *JsonRpcMsg, source string, identity *Identity) error {
	// Snapshot rules under a short RLock instead of holding it
	// across the broker call (network I/O). Mirrors the http_proxy
	// pattern; SetRules' Lock no longer stalls on in-flight WS frames.
	p.rulesMutex.RLock()
	rulesSnap := p.rules
	defaultActionSnap := p.defaultAction
	limitersSnap := p.limiters
	p.rulesMutex.RUnlock()

	startTime := time.Now()

	for _, rule := range rulesSnap {
		// Per-rule cache namespace: see HashWithRule for the cross-
		// rule cache poisoning rationale.
		hash := request.HashWithRule(rule.Fingerprint)
		match := rule.Match(request)
		if match {
			ruleID := ruleTagOrFingerprint(rule.Tag, rule.Fingerprint)
			// Per-rule auth + rate-limit gate, mirroring the HTTP
			// single/batch paths. Without this a WS client could
			// bypass scopes/identities or rate limits frame-by-frame.
			// On deny, reply with the JSON-RPC error and stop (the
			// frame is neither cached nor forwarded).
			if vok, code, reason := p.policyVerdict(request, rule, identity, source, limitersSnap); !vok {
				// Notifications (no id) get no response, even on deny (§4.1).
				if request.ID != nil {
					if err := client.SendMsg(ErrorResponse(request, code, reason, nil)); err != nil {
						return err
					}
				}
				p.recordOutcome(request, source, cacheMiss, RuleActionDeny, ruleID, startTime, "request denied (policy)")
				return nil
			}
			switch rule.Action {
			case RuleActionAllow:
				// Subscription methods MUST NOT round-trip the cache.
				// Caching a subscribe response replays a subscription
				// id that belongs to a different client's broker-side
				// subscription, so the new client never receives any
				// pushes. Caching an unsubscribe response replays a
				// success the new client did not earn. The footgun
				// surfaces only if an operator marks a subscription
				// rule cacheable — defensible runtime guard rather
				// than a config-time reject because rule.matches() is
				// pattern-based and may straddle subscription and
				// non-subscription methods.
				cacheable := rule.Cache != nil && rule.Cache.Enable && !hasSubscriptionMethod(request)
				if cacheable {
					// Single round-trip lookup: ErrNotFound is the miss
					// signal, any other error is a backend failure that
					// we log and fall through on. The previous shape
					// (Has then Get) doubled every cache-hit cost on
					// the olric backend, whose Has() implementation
					// runs a full Get() internally — so a remote-
					// partition hit paid two RTTs to find one entry.
					res, err := p.cache.Get(context.Background(), hash)
					// Only a FRESH entry is served from cache. A stale entry
					// (stored under an extended physical TTL for HTTP-path
					// serve-stale) is revalidated inline here — the WS broker
					// already coalesces identical subscriptions, so there is no
					// stampede to serve-stale around.
					if err == nil && classifyFreshness(res.StoredAt, nowOrDefault(p.now),
						effectiveTTL(rule.Cache, p.cacheConfig),
						resolveStaleWindow(rule.Cache, cfgStaleWindow(p.cacheConfig))) == freshEntry {
						// Notifications (no id) get no response, even on a
						// cache hit — the key ignores id, so a prior call
						// could have primed this entry (JSON-RPC 2.0 §4.1).
						if request.ID != nil {
							if err = client.SendMsg(res.CloneWithID(request.ID)); err != nil {
								return err
							}
						}
						p.recordOutcome(request, source, cacheHit, RuleActionAllow, ruleID, startTime, "request allowed")
						return nil
					}
					if err != nil && !errors.Is(err, cache.ErrNotFound) {
						p.log.Errorf("error getting cached value: %v", err)
					}
				}

				var res *JsonRpcMsg
				var err error
				if hasSubscriptionMethod(request) {
					res, err = p.broker.HandleSubscription(client, request)
					if err != nil {
						return err
					}
				} else {
					res, err = p.broker.HandleRequest(request)
					if err != nil {
						return err
					}
				}

				// Notifications (no id) get no response frame, even though
				// the upstream round trip happened (JSON-RPC 2.0 §4.1).
				// Subscription frames always carry an id, so they're
				// unaffected. Nothing is cached for a notification either —
				// the cached entry would later be replayed to a real call.
				if request.ID == nil {
					p.recordOutcome(request, source, cacheMiss, RuleActionAllow, ruleID, startTime, "request allowed")
					return nil
				}

				if err = client.SendMsg(res); err != nil {
					return err
				}
				p.recordOutcome(request, source, cacheMiss, RuleActionAllow, ruleID, startTime, "request allowed")

				if !cacheable {
					return nil
				}

				if res.Error != nil && !rule.Cache.CacheError {
					return nil
				}
				if res.IsEmptyResult() && !rule.Cache.CacheEmptyResult {
					return nil
				}
				res.StoredAt = nowOrDefault(p.now).UTC()
				physTTL := physicalTTL(effectiveTTL(rule.Cache, p.cacheConfig), resolveStaleWindow(rule.Cache, cfgStaleWindow(p.cacheConfig)))
				if err = p.cache.Set(context.Background(), hash, res, physTTL); err != nil {
					return fmt.Errorf("error storing in cache: %v", err)
				}
				p.cgDashboard.RecordCardinality(p.section, ruleID, request.Method)
				return nil

			case RuleActionDeny:
				p.cgDashboard.RecordDeny(DenyRecord{
					Section:  p.section,
					Reason:   "rule",
					SourceIP: source,
					Method:   request.Method,
					RuleTag:  ruleID,
				})
				// A denied notification still gets no response (§4.1).
				if request.ID != nil {
					if err := client.SendMsg(UnauthorizedResponse(request)); err != nil {
						return err
					}
				}
				p.recordOutcome(request, source, cacheMiss, RuleActionDeny, ruleID, startTime, "request denied")
				return nil

			default:
				p.log.Errorf("unrecognized rule action %q", rule.Action)
			}
		}
	}

	// No rule matched — note the unmatched method, then apply default.
	p.cgDashboard.RecordUnmatched(p.section, request.Method, "")
	if defaultActionSnap == RuleActionAllow {
		// Even on default-allow, an unmatched method must clear the
		// global auth.defaultRequire gate (no rule opted it out) —
		// mirrors the HTTP single/batch no-match paths.
		if p.auth != nil {
			if ok, reason := p.auth.Authorize(nil, identity); !ok {
				p.cgDashboard.RecordDeny(DenyRecord{
					Section: p.section, Reason: "auth",
					SourceIP: source, Method: request.Method,
				})
				// Notification (no id) → no response frame, even on deny (§4.1).
				if request.ID != nil {
					if err := client.SendMsg(ErrorResponse(request, -32001, reason, nil)); err != nil {
						return err
					}
				}
				p.recordOutcome(request, source, cacheMiss, RuleActionDeny, "default", startTime, "request denied (auth)")
				return nil
			}
		}
		var res *JsonRpcMsg
		var err error
		if hasSubscriptionMethod(request) {
			res, err = p.broker.HandleSubscription(client, request)
			if err != nil {
				return err
			}
		} else {
			res, err = p.broker.HandleRequest(request)
			if err != nil {
				return err
			}
		}

		// Notification (no id) → no response frame even on default-allow
		// (JSON-RPC 2.0 §4.1). Subscriptions always carry an id.
		if request.ID != nil {
			if err = client.SendMsg(res); err != nil {
				return err
			}
		}

	} else {
		p.cgDashboard.RecordDeny(DenyRecord{
			Section:  p.section,
			Reason:   "default",
			SourceIP: source,
			Method:   request.Method,
		})
		// A denied notification still gets no response (§4.1).
		if request.ID != nil {
			if err := client.SendMsg(UnauthorizedResponse(request)); err != nil {
				return err
			}
		}
	}

	p.recordOutcome(request, source, cacheMiss, string(defaultActionSnap), "default", startTime,
		fmt.Sprintf("request %s", defaultActionSnap))
	return nil
}
