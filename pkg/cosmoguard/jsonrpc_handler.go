package cosmoguard

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/olric-data/olric"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/voluzi/cosmoguard/pkg/cache"
)

type JsonRpcHandler struct {
	cache            cache.Cache[uint64, *JsonRpcMsg]
	defaultAction    RuleAction
	wsProxy          *JsonRpcWebSocketProxy
	wsPath           string
	rules            []*JsonRpcRule
	rulesMutex       sync.RWMutex // Mutex to block readers when rules are being updated
	log              *Entry
	responseTimeHist *prometheus.HistogramVec
	batchResTimeHist *prometheus.HistogramVec
	// maxBatchSize caps the number of requests in a single JSON-RPC batch.
	// 0 disables the cap.
	maxBatchSize int
	// cgDashboard is the optional observability sink for unmatched
	// + deny events. nil when not wired by cosmoguard.New; all
	// Record* calls are nil-safe.
	cgDashboard *dashboardObservability
	// section is the dashboard section name ("rpc.jsonrpc", "evm.rpc").
	// Empty when SetDashboard hasn't been called.
	section string
	// auth runs per-rule auth checks at match time. The HTTP-level
	// gate has already resolved an identity by the time JSON-RPC
	// dispatch runs; per-rule enforcement reads it from req.Context
	// via RequestStats.
	auth *Authenticator
	// limiters maps a rule's Fingerprint to its token-bucket. Built
	// in SetRules; nil-safe lookup in handleHttpSingle.
	limiters    map[uint64]RateLimiter
	olricClient *olric.EmbeddedClient
	proxyName   string
	setRulesMu  sync.Mutex
}

// SetAuthenticator wires the Authenticator used for per-rule auth
// checks. Nil-safe.
func (h *JsonRpcHandler) SetAuthenticator(a *Authenticator) {
	h.auth = a
	if h.wsProxy != nil {
		h.wsProxy.SetAuthenticator(a)
	}
}

// SetRequestLog forwards the Live-traffic request log to the embedded
// WS proxy. The HTTP (POST /) path is logged by the fronting HttpProxy,
// so only the WS proxy needs wiring here. Nil-safe.
func (h *JsonRpcHandler) SetRequestLog(rl *requestLog) {
	if h.wsProxy != nil {
		h.wsProxy.SetRequestLog(rl)
	}
}

// WSProxy returns the embedded WebSocket proxy, or nil when WS is
// disabled for this handler. Used by the dashboard WS panel.
func (h *JsonRpcHandler) WSProxy() *JsonRpcWebSocketProxy { return h.wsProxy }

// SetDashboard wires the dashboard observability sink and section
// name. Also propagates them to the embedded WS proxy when present
// so WS-side unmatched/deny events feed the same buffers.
func (h *JsonRpcHandler) SetDashboard(section string, d *dashboardObservability) {
	h.section = section
	h.cgDashboard = d
	if h.wsProxy != nil {
		h.wsProxy.SetDashboard(section, d)
	}
}

func NewJsonRpcHandler(name string, opts ...Option[JsonRpcHandlerOptions]) (*JsonRpcHandler, error) {
	cfg := DefaultJsonRpcHandlerOptions()
	for _, opt := range opts {
		opt(cfg)
	}
	handler := &JsonRpcHandler{
		wsPath:      cfg.WebsocketPath,
		auth:        cfg.Authenticator,
		olricClient: cfg.OlricClient,
		proxyName:   name,
	}

	// Setup cache
	var cacheOptions []cache.Option
	if cfg.CacheConfig != nil {
		cacheOptions = append(cacheOptions, cache.DefaultTTL(cfg.CacheConfig.TTL))
	}

	var err error
	handler.cache, err = newResponseCache[uint64, *JsonRpcMsg](cfg.CacheConfig, cfg.OlricClient, name, cacheOptions...)
	if err != nil {
		return nil, err
	}

	if cfg.WebsocketEnabled {
		// v4 multi-upstream form takes precedence; fall back to the
		// singular field for v3 compat.
		backends := cfg.WebsocketBackends
		if len(backends) == 0 {
			backends = []string{cfg.WebsocketBackend}
		}
		handler.wsProxy, err = NewJsonRpcWebSocketProxy(
			name,
			backends,
			cfg.WebsocketPath,
			cfg.WebsocketConnections,
			cfg.UpstreamConstructor,
			handler.cache,
			cfg.MetricsEnabled,
			cfg.ServerConfig,
		)
		if err != nil {
			return nil, err
		}
	}

	handler.maxBatchSize = cfg.MaxBatchSize

	if cfg.MetricsEnabled {
		// G5 dropped the unbounded `path` label sourced from JSON-RPC
		// params; Phase H re-introduces `rule_id` (operator-supplied
		// tag) and `upstream` (selected pool node) as bounded labels.
		handler.responseTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: name,
			Name:      "request_duration_seconds",
			Help:      "Histogram of response time for handler in seconds",
			Buckets:   responseTimeBuckets,
		}, []string{"method", "cache", "action", "rule_id", "upstream"})
		// Batch histogram pre-G used per-batch integer labels (requests,
		// allowed, denied, cache_hits, cache_misses), each generating one
		// Prometheus series per distinct value. Replaced with bucketed
		// size_class labels and a closed action label.
		handler.batchResTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: fmt.Sprintf("%s_batch", name),
			Name:      "request_duration_seconds",
			Help:      "Histogram of response time for handler in seconds",
			Buckets:   responseTimeBuckets,
		}, []string{"size_class"})
	}

	return handler, nil
}

// recordSingle emits the standardized log + Prometheus observation for one
// JSON-RPC single-request outcome. Centralizes what was duplicated across
// every allow/deny/cache-hit/cache-miss path of handleHttpSingle pre-D2.
func (h *JsonRpcHandler) recordSingle(r *http.Request, request *JsonRpcMsg, cacheState, action string, startTime time.Time, msg string) {
	duration := time.Since(startTime)
	ruleID, upstream := statsLabels(r)
	if h.responseTimeHist != nil {
		h.responseTimeHist.WithLabelValues(
			request.Method,
			cacheState,
			action,
			ruleID,
			upstream).Observe(duration.Seconds())
	}
	// Stamp the active span with the rule action. Deny outcomes
	// (which return JSON-RPC 200 with an embedded error) would
	// otherwise render as green in the tracing backend; status=200
	// here is the HTTP framing, not the rpc outcome.
	status := http.StatusOK
	if action == string(RuleActionDeny) {
		status = http.StatusUnauthorized
	}
	markSpanOutcome(r.Context(), status, action)
	h.log.WithFields(Fields{
		"id":       request.ID,
		"method":   request.Method,
		"params":   request.Params,
		"cache":    cacheState,
		"duration": duration,
		"source":   GetSourceIP(r),
		"rule_id":  ruleID,
		"upstream": upstream,
	}).Info(msg)
}

// recordBatchItem emits the per-item log line inside a JSON-RPC batch.
// No Prometheus observation per-item — the histogram fires once for the
// whole batch via batchResTimeHist.
//
// cacheState normalizes the "" passed by short-circuit (deny / default)
// paths to "n/a" so log queries grouping on the field have exactly the
// three documented states ("hit", "miss", "n/a") and never an empty
// bucket the operator has to special-case.
func (h *JsonRpcHandler) recordBatchItem(r *http.Request, req *JsonRpcMsg, cacheState, msg string) {
	ruleID, upstream := statsLabels(r)
	if cacheState == "" {
		cacheState = "n/a"
	}
	h.log.WithFields(Fields{
		"id":       req.ID,
		"method":   req.Method,
		"params":   req.Params,
		"cache":    cacheState,
		"source":   GetSourceIP(r),
		"rule_id":  ruleID,
		"upstream": upstream,
	}).Info(msg)
}

// statsLabels reads RequestStats from the request's context and returns
// (rule_id, upstream) suitable for Prometheus labels. rule_id defaults
// to "default" when no rule matched; upstream defaults to "" when the
// request short-circuited before the pool was reached.
func statsLabels(r *http.Request) (string, string) {
	stats := RequestStatsFromCtx(r.Context())
	if stats == nil {
		return "default", ""
	}
	ruleID := stats.RuleTag
	if ruleID == "" {
		ruleID = "default"
	}
	return ruleID, stats.Upstream
}

// batchSizeClass maps a JSON-RPC batch length into one of a small closed
// set of strings, so the Prometheus label produced from it has bounded
// cardinality (~7 distinct values, regardless of traffic shape).
func batchSizeClass(n int) string {
	switch {
	case n <= 1:
		return "1"
	case n <= 5:
		return "2-5"
	case n <= 10:
		return "6-10"
	case n <= 50:
		return "11-50"
	case n <= 100:
		return "51-100"
	case n <= 500:
		return "101-500"
	default:
		return "500+"
	}
}

// Shutdown closes the handler's cache backend + stops the broker's
// subscription migrator. Used by CosmoGuard.Shutdown to reap goroutines
// and release Redis pools. Idempotent.
func (h *JsonRpcHandler) Shutdown() error {
	if h.wsProxy != nil && h.wsProxy.broker != nil {
		h.wsProxy.broker.Stop()
	}
	if h.cache != nil {
		return h.cache.Close()
	}
	return nil
}

func (h *JsonRpcHandler) Start(logger *Entry) error {
	h.log = logger.WithField("handler", "jsonrpc")

	// On AlreadyRegisteredError, swap the private vec for the
	// existing collector so observations land where the gatherer
	// sees them — same fix pattern as HttpProxy.Run. Without it, a
	// second JsonRpcHandler in the same process (hot-reload, two
	// CosmoGuards in a test) observes into a dead vec and the
	// dashboard's /api/v1/metrics never sees the data.
	if h.responseTimeHist != nil {
		if err := prometheus.Register(h.responseTimeHist); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
					h.responseTimeHist = existing
				}
			}
		}
	}
	if h.batchResTimeHist != nil {
		if err := prometheus.Register(h.batchResTimeHist); err != nil {
			if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
					h.batchResTimeHist = existing
				}
			}
		}
	}

	if h.wsProxy != nil {
		go func() {
			if err := h.wsProxy.Run(h.log); err != nil {
				h.log.Errorf("error on websocket proxy: %v", err)
			}
		}()
	}
	return nil
}

func (h *JsonRpcHandler) SetRules(rules []*JsonRpcRule, defaultAction RuleAction) {
	// Serialise SetRules so concurrent reloads can't race on the
	// limiter rebuild and leak closed limiters. Same idiom as
	// HttpProxy.SetRules.
	h.setRulesMu.Lock()
	defer h.setRulesMu.Unlock()

	h.rulesMutex.RLock()
	existing := h.limiters
	h.rulesMutex.RUnlock()

	newLimiters := map[uint64]RateLimiter{}
	for _, r := range rules {
		if r.RateLimit == nil {
			continue
		}
		if l, ok := existing[r.Fingerprint]; ok {
			newLimiters[r.Fingerprint] = l
			continue
		}
		keyspace := h.proxyName + ":rl:" + strconv.FormatUint(r.Fingerprint, 16)
		l, err := NewRateLimiter(*r.RateLimit, h.olricClient, keyspace)
		if err != nil {
			h.log.WithError(err).WithField("rule_priority", r.Priority).
				Error("rate limiter init failed; rule will run without limit")
			continue
		}
		newLimiters[r.Fingerprint] = l
	}

	h.rulesMutex.Lock()
	old := h.limiters
	h.rules = rules
	h.defaultAction = defaultAction
	h.limiters = newLimiters
	h.rulesMutex.Unlock()

	if h.wsProxy != nil {
		h.wsProxy.SetRules(rules, defaultAction, newLimiters)
	}

	for fp, l := range old {
		if _, kept := newLimiters[fp]; !kept {
			_ = l.Close()
		}
	}
}

func (h *JsonRpcHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request)) {
	defer recoverHTTP(h.log, w, r)
	// Wrap in a child span — HttpProxy.ServeHTTP started the parent
	// before dispatching here. When tracing is disabled this is a no-op.
	ctx, span := StartHTTPSpan(r, "jsonrpc")
	defer span.End()
	r = r.WithContext(ctx)
	start := time.Now()

	if r.Method == http.MethodPost && r.URL.Path == "/" {
		h.log.Debug("serving http jsonrpc request")
		h.handleHttp(w, r, next, start)
		return
	}

	if h.wsProxy != nil && r.URL.Path == h.wsPath && r.Method == http.MethodGet {
		h.log.Debug("handling jsonrpc websocket connection")
		h.wsProxy.HandleConnection(w, r)
		return
	}

	h.log.WithFields(map[string]interface{}{
		"method": r.Method,
		"path":   r.URL.Path,
	}).Errorf("unexpected request")
	WriteError(w, http.StatusBadRequest, "unexpected request")
}

func (h *JsonRpcHandler) handleHttp(w http.ResponseWriter, r *http.Request,
	next func(http.ResponseWriter, *http.Request), startTime time.Time) {
	// The body was wrapped in http.MaxBytesReader by HttpProxy.ServeHTTP
	// before this handler runs. ReusableReader drains it now; a drain
	// error is the body-size cap tripping (oversized chunked / mismatched
	// Content-Length), which must be rejected with 413 rather than
	// parsing the truncated payload.
	rr, rerr := ReusableReader(r.Body)
	if rerr != nil {
		WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = rr

	b, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "bad request")
		return
	}

	req, requests, parseErr := ParseJsonRpcMessage(b)
	if parseErr != nil {
		// Per JSON-RPC 2.0 §5.1, a parse failure responds with id=null,
		// code -32700 Parse error (when the payload was unparseable as
		// JSON) or -32600 Invalid Request (when it parsed as JSON but
		// not as a JSON-RPC message). Previously a parse failure on a
		// non-array payload still produced a defaulted request struct
		// and silently went on to handleHttpSingle.
		code := -32700
		msg := "Parse error"
		if req != nil || requests != nil {
			code = -32600
			msg = "Invalid Request"
		}
		errResp := ErrorResponse(&JsonRpcMsg{ID: nil}, code, msg, nil)
		body, mErr := errResp.Marshal()
		if mErr != nil {
			WriteError(w, http.StatusBadRequest, "bad request")
			return
		}
		WriteData(w, http.StatusOK, body, "Content-Type", "application/json")
		return
	}
	if req != nil {
		h.handleHttpSingle(req, w, r, next, startTime)
		return
	}
	// Per JSON-RPC 2.0 §6.7, an empty array MUST be rejected with a
	// single Invalid Request error (id=null). The previous fall-through
	// happily handed `[]` to handleHttpBatch which silently produced
	// `[]` back — spec-violating, and a confusing no-op for clients
	// debugging a malformed batch generator.
	if len(requests) == 0 {
		errResp := ErrorResponse(&JsonRpcMsg{ID: nil}, -32600, "Invalid Request", nil)
		body, mErr := errResp.Marshal()
		if mErr != nil {
			h.log.Errorf("error marshalling empty-batch error response: %v", mErr)
			WriteError(w, http.StatusBadRequest, "bad request")
			return
		}
		WriteData(w, http.StatusOK, body, "Content-Type", "application/json")
		return
	}
	// Reject oversized batches before any rule evaluation. Batch
	// amplification is one of the cheapest ways to turn a single TCP
	// connection into a flood of upstream calls.
	if h.maxBatchSize > 0 && len(requests) > h.maxBatchSize {
		h.log.WithFields(map[string]interface{}{
			"batch_size":     len(requests),
			"max_batch_size": h.maxBatchSize,
			"source":         GetSourceIP(r),
		}).Warn("jsonrpc batch exceeds maxBatchSize")
		WriteError(w, http.StatusRequestEntityTooLarge,
			"jsonrpc batch size exceeds configured maximum")
		return
	}
	h.handleHttpBatch(requests, w, r, next, startTime)
}

// jsonRpcPolicyVerdict runs the matched rule's per-rule auth +
// rate-limit checks against the resolved identity and records any deny
// on the dashboard. It writes NO response — callers emit it in their
// own shape (the single path as a standalone HTTP body, the batch path
// as one element of the response array). Returning ok=true lets the
// caller proceed; ok=false carries the JSON-RPC error code (-32001 auth
// / -32005 rate) + human reason. Shared between the single and batch
// paths so the batch path cannot drift from the single path's policy —
// the drift that let a denied call slip through inside a batch.
//
// Identity is pulled from the HTTP request context — the gate chain in
// HttpProxy.ServeHTTP resolved it once for the connection.
func (h *JsonRpcHandler) jsonRpcPolicyVerdict(r *http.Request, request *JsonRpcMsg,
	rule *JsonRpcRule, limiters map[uint64]RateLimiter) (ok bool, code int, reason string) {
	var id *Identity
	if hr, found := r.Context().Value(identityCtxKey{}).(*Identity); found {
		id = hr
	}
	// Always Authorize when auth is enabled, even if the rule omits an
	// auth: block — Authorize(nil, id) applies the global
	// auth.defaultRequire. Guarding on rule.Auth != nil would let a
	// JSON-RPC method rule without an auth block be served anonymously
	// under defaultRequire (when the fronting HTTP rule opted out with
	// require:false). Authorize(nil, id) is a pass-through when
	// defaultRequire is off, so no-auth deployments are unaffected.
	if h.auth != nil {
		if authOK, why := h.auth.Authorize(rule.Auth, id); !authOK {
			h.cgDashboard.RecordDeny(DenyRecord{
				Section: h.section, Reason: "auth",
				SourceIP: GetSourceIP(r), Method: request.Method,
				RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
			})
			return false, -32001, why
		}
	}
	if l, found := limiters[rule.Fingerprint]; found && l != nil {
		idName := ""
		if id != nil {
			idName = id.Name
		}
		key := grpcRateLimitKey(rule.RateLimit.Scope, rule.Fingerprint, GetSourceIP(r), idName)
		allowed, _, rlErr := l.Allow(r.Context(), key)
		if rlErr != nil {
			h.log.WithError(rlErr).Warn("jsonrpc rate limiter error; allowing")
		} else if !allowed {
			h.cgDashboard.RecordDeny(DenyRecord{
				Section: h.section, Reason: "rate_limit",
				SourceIP: GetSourceIP(r), Method: request.Method,
				RuleTag: ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
			})
			return false, -32005, "rate limit exceeded"
		}
	}
	return true, 0, ""
}

// jsonRpcDefaultAuthVerdict applies the global auth.defaultRequire to an
// unmatched method (no JsonRpcRule covered it) on a default-allow path.
// Without this, when the fronting HTTP endpoint opts out of auth
// (POST / with require:false) an anonymous caller could invoke any
// method not named by a rule under rpc.jsonrpc.default:allow. Mirrors the
// gRPC no-match default gate. Returns ok + the JSON-RPC deny code/reason;
// pass-through when auth is disabled or defaultRequire is off. Records
// the deny.
func (h *JsonRpcHandler) jsonRpcDefaultAuthVerdict(r *http.Request, request *JsonRpcMsg) (ok bool, code int, reason string) {
	if h.auth == nil {
		return true, 0, ""
	}
	var id *Identity
	if hr, found := r.Context().Value(identityCtxKey{}).(*Identity); found {
		id = hr
	}
	if authOK, why := h.auth.Authorize(nil, id); !authOK {
		h.cgDashboard.RecordDeny(DenyRecord{
			Section: h.section, Reason: "auth",
			SourceIP: GetSourceIP(r), Method: request.Method,
		})
		return false, -32001, why
	}
	return true, 0, ""
}

// enforceJsonRpcRulePolicy is the single-request wrapper around
// jsonRpcPolicyVerdict: it writes the deny response + records the
// outcome and returns false when blocked, true to proceed.
func (h *JsonRpcHandler) enforceJsonRpcRulePolicy(w http.ResponseWriter, r *http.Request,
	request *JsonRpcMsg, rule *JsonRpcRule, limiters map[uint64]RateLimiter, startTime time.Time) bool {
	ok, code, reason := h.jsonRpcPolicyVerdict(r, request, rule, limiters)
	if ok {
		return true
	}
	status := http.StatusUnauthorized
	logMsg := "request denied (auth)"
	if code == -32005 {
		status = http.StatusTooManyRequests
		logMsg = "request denied (rate)"
	}
	// Notifications (no id) get no response, even on deny (JSON-RPC 2.0
	// §4.1) — matches the batch and WS deny paths.
	if request.ID != nil {
		body, _ := ErrorResponse(request, code, reason, nil).Marshal()
		WriteData(w, status, body, "Content-Type", "application/json")
	}
	h.recordSingle(r, request, "", RuleActionDeny, startTime, logMsg)
	return false
}

func (h *JsonRpcHandler) handleHttpSingle(request *JsonRpcMsg, w http.ResponseWriter, r *http.Request,
	next func(http.ResponseWriter, *http.Request), startTime time.Time) {
	// Snapshot rules + defaultAction under a short RLock so we don't
	// hold the reader lock across the upstream call below. Keeping
	// the RLock for the full request would block every config reload
	// (SetRules takes the write lock and waits for readers to drain)
	// for as long as the slowest upstream takes to answer — and
	// because tryReload acquires configMutex before calling SetRules,
	// any other consumer of configMutex (dashboard snapshotConfig,
	// MetricsPort, etc.) would stall on the same slow upstream.
	// Same fix pattern as handleHttpBatch / ws_proxy.
	h.rulesMutex.RLock()
	rulesSnap := h.rules
	defaultActionSnap := h.defaultAction
	limitersSnap := h.limiters
	h.rulesMutex.RUnlock()

	for _, rule := range rulesSnap {
		// Per-rule cache namespace: mix the rule fingerprint into the
		// key so two cacheable rules matching the same JSON-RPC method
		// don't share entries (different TTLs / cacheError flags would
		// otherwise be silently ignored — whichever rule populated the
		// cache first won).
		hash := request.HashWithRule(rule.Fingerprint)
		match := rule.Match(request)
		if match {
			// Per-rule auth + rate-limit run BEFORE the action switch
			// so an explicit allow rule can still be gated by scopes /
			// rate. Identity was resolved by the HTTP-level gate and
			// is on the RequestStats; for anonymous traffic id is nil
			// and Authorize falls through to its default-require check.
			if !h.enforceJsonRpcRulePolicy(w, r, request, rule, limitersSnap, startTime) {
				return
			}
			if stats := RequestStatsFromCtx(r.Context()); stats != nil {
				stats.RuleTag = ruleTagOrFingerprint(rule.Tag, rule.Fingerprint)
			}
			switch rule.Action {
			case RuleActionAllow:
				// Subscription methods MUST NOT round-trip the cache —
				// see the long-form rationale in ws_proxy.go. HTTP
				// subscribe is degenerate (Tendermint / EVM nodes reject
				// it), but cacheError + a wildcard rule could still
				// archive the upstream error response, wasting space at
				// best and hiding a future server upgrade at worst.
				// Match the WS guard so an operator who toggles
				// cache.enable on a `method: "*"` rule cannot footgun
				// either transport.
				cacheable := rule.Cache != nil && rule.Cache.Enable && !hasSubscriptionMethod(request)
				if cacheable {
					// Single round-trip lookup; ErrNotFound = miss,
					// other errors are backend failures that we log
					// and fall through on. Mirrors the ws_proxy
					// rewrite — olric's Has() is a Get() under the
					// hood, so the prior Has-then-Get pattern paid
					// double the RTT on every hit.
					res, err := h.cache.Get(r.Context(), hash)
					if err == nil {
						// Notifications (no id) get no response, even on a
						// cache hit — the key ignores id, so a prior call
						// could have primed this entry (JSON-RPC 2.0 §4.1).
						if request.ID != nil {
							h.writeSingleResponse(w, res.CloneWithID(request.ID))
						}
						h.recordSingle(r, request, cacheHit, RuleActionAllow, startTime, "request allowed")
						return
					}
					if !errors.Is(err, cache.ErrNotFound) {
						h.log.Errorf("error retrieving from cache: %v", err)
					}
					h.getSingleUpstreamResponse(w, r, next, hash, rule.Cache,
						ruleTagOrFingerprint(rule.Tag, rule.Fingerprint), request.Method)
					h.recordSingle(r, request, cacheMiss, RuleActionAllow, startTime, "request allowed")
					return
				}
				next(w, r)
				h.recordSingle(r, request, cacheMiss, RuleActionAllow, startTime, "request allowed")
				return

			case RuleActionDeny:
				h.cgDashboard.RecordDeny(DenyRecord{
					Section:  h.section,
					Reason:   "rule",
					SourceIP: GetSourceIP(r),
					Method:   request.Method,
					RuleTag:  ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
				})
				// Notifications (no id) get no response, even on deny (§4.1).
				if request.ID != nil {
					h.writeSingleResponse(w, UnauthorizedResponse(request))
				}
				h.recordSingle(r, request, cacheMiss, RuleActionDeny, startTime, "request denied")
				return

			default:
				h.log.Errorf("unrecognized rule action %q", rule.Action)
			}
		}
	}

	if stats := RequestStatsFromCtx(r.Context()); stats != nil && stats.RuleTag == "" {
		stats.RuleTag = "default"
	}
	// No rule matched — record the (method, "") tuple so the
	// dashboard's unmatched panel surfaces it for the operator.
	h.cgDashboard.RecordUnmatched(h.section, request.Method, "")
	if defaultActionSnap == RuleActionAllow {
		// Even on default-allow, an unmatched method must clear the
		// global auth.defaultRequire gate (no rule opted it out).
		if ok, code, reason := h.jsonRpcDefaultAuthVerdict(r, request); !ok {
			// Notification (no id) → no response, even on deny (§4.1).
			if request.ID != nil {
				body, _ := ErrorResponse(request, code, reason, nil).Marshal()
				WriteData(w, http.StatusUnauthorized, body, "Content-Type", "application/json")
			}
			h.recordSingle(r, request, "", RuleActionDeny, startTime, "request denied (auth)")
			return
		}
		next(w, r)
		h.recordSingle(r, request, cacheMiss, RuleActionAllow, startTime, "request allowed")
	} else {
		h.cgDashboard.RecordDeny(DenyRecord{
			Section:  h.section,
			Reason:   "default",
			SourceIP: GetSourceIP(r),
			Method:   request.Method,
		})
		// Notifications (no id) get no response, even on deny (§4.1).
		if request.ID != nil {
			h.writeSingleResponse(w, UnauthorizedResponse(request))
		}
		h.recordSingle(r, request, cacheMiss, RuleActionDeny, startTime, "request denied")
	}
}

func (h *JsonRpcHandler) getSingleUpstreamResponse(w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request), hash uint64, cache *RuleCache, ruleTag, method string) {
	ww := WrapResponseWriter(w)
	next(ww, r)

	b, err := ww.GetWrittenBytes()
	if err != nil {
		h.log.Errorf("error getting data from upstream response: %v", err)
		return
	}

	res, _, perr := ParseJsonRpcMessage(b)
	if perr != nil || res == nil {
		// Upstream returned a body that isn't a valid JSON-RPC message
		// (e.g. an HTML 502 / plain-text error during an outage). The
		// client already received the raw bytes via the response tee;
		// do NOT cache — storing a zero-value message would replay a
		// synthetic invalid JSON-RPC response from cache until the TTL
		// expires, even after the upstream recovers.
		if perr != nil {
			h.log.Warnf("upstream returned unparseable JSON-RPC; skipping cache: %v", perr)
		}
		return
	}

	// Capture any trailing whitespace (typically "\n" from Cosmos /
	// EVM JSON-RPC servers) so cache-hit replays remain byte-identical
	// to the original upstream wire payload, not just the parsed JSON.
	if suffix := trailingWhitespace(b); len(suffix) > 0 {
		res.WireSuffix = suffix
	}

	h.log.WithFields(map[string]interface{}{
		"error":              res.Error != nil,
		"empty-result":       res.IsEmptyResult(),
		"cache-enabled":      cache.Enable,
		"cache-ttl":          cache.TTL.String(),
		"cache-error":        cache.CacheError,
		"cache-empty-result": cache.CacheEmptyResult,
	}).Debug("got response from upstream")

	if res.Error != nil && !cache.CacheError {
		return
	}
	if res.IsEmptyResult() && !cache.CacheEmptyResult {
		return
	}

	if err = h.cache.Set(r.Context(), hash, res, cache.TTL); err != nil {
		h.log.Errorf("error setting cache value: %v", err)
		return
	}
	h.cgDashboard.RecordCardinality(h.section, ruleTag, method)
}

func (h *JsonRpcHandler) writeSingleResponse(w http.ResponseWriter, res *JsonRpcMsg) {
	b, err := res.Marshal()
	if err != nil {
		h.log.Errorf("error marshalling response from cache: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// set the proper content type before writing
	w.Header().Set("Content-Type", "application/json")

	w.Write(b)
}

func (h *JsonRpcHandler) handleHttpBatch(requests JsonRpcMsgs, w http.ResponseWriter, r *http.Request,
	next func(http.ResponseWriter, *http.Request), startTime time.Time) {
	responses := JsonRpcResponses{}

	var cacheHits, cacheMisses, allowed, denied int
	requestIDs := make([]interface{}, len(requests))

	// Snapshot rules + defaultAction under a short RLock so the long
	// cache-lookup + upstream call below don't hold a reader lock
	// against SetRules. Same anti-pattern fix as ws_proxy /
	// http_proxy.
	h.rulesMutex.RLock()
	rulesSnap := h.rules
	defaultActionSnap := h.defaultAction
	limitersSnap := h.limiters
	h.rulesMutex.RUnlock()

RequestsLoop:
	for i, req := range requests {
		requestIDs[i] = req.ID
		if len(rulesSnap) == 0 {
			cacheMisses++
			// No rules at all — every batch item is unmatched.
			h.cgDashboard.RecordUnmatched(h.section, req.Method, "")
			if defaultActionSnap == RuleActionAllow {
				if ok, code, reason := h.jsonRpcDefaultAuthVerdict(r, req); !ok {
					denied++
					if req.ID != nil {
						responses.AddResponse(req, ErrorResponse(req, code, reason, nil))
					}
					h.recordBatchItem(r, req, "", "request in batch denied (auth)")
					continue RequestsLoop
				}
				responses.AddPending(req)
				allowed++
				h.recordBatchItem(r, req, cacheMiss, "request in batch allowed")
			} else {
				h.cgDashboard.RecordDeny(DenyRecord{
					Section:  h.section,
					Reason:   "default",
					SourceIP: GetSourceIP(r),
					Method:   req.Method,
				})
				// Notifications (no id) get no response, even on denial
				// (JSON-RPC 2.0 §4.1).
				if req.ID != nil {
					responses.Deny(req)
				}
				denied++
				h.recordBatchItem(r, req, "", "request in batch denied")
			}
			continue RequestsLoop
		}
		for _, rule := range rulesSnap {
			match := rule.Match(req)
			if match {
				// Per-rule auth + rate-limit gate, identical to the
				// single-request path (jsonRpcPolicyVerdict). Without
				// this, a method matching an allow rule that carries
				// auth scopes/identities or a rateLimit could be
				// invoked — or served from cache — inside a batch by a
				// caller that would be denied as a single request.
				if vok, code, reason := h.jsonRpcPolicyVerdict(r, req, rule, limitersSnap); !vok {
					denied++
					// Notifications (no id) get NO response, even on
					// denial, per JSON-RPC 2.0 §4.1. Emit the error only
					// for id-bearing calls.
					if req.ID != nil {
						responses.AddResponse(req, ErrorResponse(req, code, reason, nil))
					}
					h.recordBatchItem(r, req, "", "request in batch denied (policy)")
					continue RequestsLoop
				}
				switch rule.Action {
				case RuleActionAllow:
					allowed++

					// Forward without touching the cache when the rule
					// doesn't enable caching (no cache: block, or
					// cache.enable: false — an operator must be able to
					// turn caching off without deleting the block) OR the
					// method is a subscription (a cached subscribe /
					// unsubscribe response would replay an upstream
					// subscription id / success token into a different
					// client's request). AddPending hits the upstream and
					// stores nothing.
					if rule.Cache == nil || !rule.Cache.Enable || hasSubscriptionMethod(req) {
						responses.AddPending(req)
						cacheMisses++
						h.recordBatchItem(r, req, cacheMiss, "request in batch allowed")
						continue RequestsLoop
					}

					// Per-rule cache namespace; see HashWithRule.
					hash := req.HashWithRule(rule.Fingerprint)
					ruleTag := ruleTagOrFingerprint(rule.Tag, rule.Fingerprint)
					// Single round-trip lookup; ErrNotFound = miss
					// (fall through to upstream), other errors are
					// backend failures (also fall through, but log).
					// Olric's Has() runs a Get() internally, so the
					// previous Has-then-Get shape doubled the per-
					// hit RTT for remote-partition entries.
					res, err := h.cache.Get(r.Context(), hash)
					if err == nil {
						cacheHits++
						// Notifications (no id) get no response, even on a
						// cache hit. The cache key ignores id, so a prior
						// id-bearing call with the same method/params can
						// have primed this entry — appending it here would
						// reply to a notification (JSON-RPC 2.0 §4.1).
						if req.ID != nil {
							responses.AddResponse(req, res)
						}
						h.recordBatchItem(r, req, cacheHit, "request in batch allowed")
						continue RequestsLoop
					}
					if !errors.Is(err, cache.ErrNotFound) {
						h.log.Errorf("error loading response from cache: %v", err)
					}

					cacheMisses++
					responses.AddPendingWithCacheConfig(req, hash, rule.Cache, ruleTag)
					h.recordBatchItem(r, req, cacheMiss, "request in batch allowed")
					continue RequestsLoop

				case RuleActionDeny:
					denied++
					h.cgDashboard.RecordDeny(DenyRecord{
						Section:  h.section,
						Reason:   "rule",
						SourceIP: GetSourceIP(r),
						Method:   req.Method,
						RuleTag:  ruleTagOrFingerprint(rule.Tag, rule.Fingerprint),
					})
					h.recordBatchItem(r, req, "", "request in batch denied")
					// Notification (no id) → no response, even on denial
					// (JSON-RPC 2.0 §4.1).
					if req.ID != nil {
						responses.Deny(req)
					}
					continue RequestsLoop

				default:
					h.log.Errorf("unrecognized rule action %q", rule.Action)
				}
				break
			}
		}
		// No rule matched this batch item — record it under the
		// section's unmatched counter.
		h.cgDashboard.RecordUnmatched(h.section, req.Method, "")
		if defaultActionSnap == RuleActionAllow {
			if ok, code, reason := h.jsonRpcDefaultAuthVerdict(r, req); !ok {
				denied++
				if req.ID != nil {
					responses.AddResponse(req, ErrorResponse(req, code, reason, nil))
				}
				h.recordBatchItem(r, req, "", "request in batch denied (auth)")
				continue RequestsLoop
			}
			responses.AddPending(req)
			allowed++
			h.recordBatchItem(r, req, "", "request in batch allowed")
		} else {
			h.cgDashboard.RecordDeny(DenyRecord{
				Section:  h.section,
				Reason:   "default",
				SourceIP: GetSourceIP(r),
				Method:   req.Method,
			})
			// Notifications (no id) get no response, even on denial
			// (JSON-RPC 2.0 §4.1).
			if req.ID != nil {
				responses.Deny(req)
			}
			denied++
			h.recordBatchItem(r, req, "", "request in batch denied")
		}
	}

	// send pending requests to upstream and grab the response
	pendingRequests := responses.GetPendingRequests()
	if len(pendingRequests) > 0 {
		h.log.Debug("getting from upstream")
		upstreamResponses, err := h.getResponsesFromUpstream(r, pendingRequests, next)
		if err != nil {
			// Don't fail the whole batch with a non-JSON 500: that breaks
			// batch semantics and discards cache hits already resolved
			// above. Leave upstreamResponses unset; FillUnansweredCalls
			// below turns each still-pending call into a per-item
			// JSON-RPC error while answered/cached items are preserved.
			h.log.Errorf("error getting responses from upstream: %v", err)
		} else {
			// Correlate by id (Set) — per JSON-RPC 2.0 batch responses
			// match by id, not position; notifications get none.
			responses.Set(pendingRequests, upstreamResponses)
			if err = responses.StoreInCache(h.cache, func(ruleTag, method string) {
				h.cgDashboard.RecordCardinality(h.section, ruleTag, method)
			}); err != nil {
				h.log.Errorf("error caching responses: %v", err)
			}
		}
		// Any id-bearing call still without a response — upstream errored
		// or omitted it — becomes a per-item JSON-RPC error so the batch
		// stays a valid JSON array and answered calls / cache hits are
		// preserved (replaces the old whole-batch 500/502). Notifications
		// (no id) are left out per JSON-RPC 2.0 §4.1.
		responses.FillUnansweredCalls()
	}

	// Per JSON-RPC 2.0 §6, a batch that yields no Response objects (e.g.
	// all notifications) MUST return nothing rather than an empty array —
	// reply 200 with an empty body.
	final := responses.GetFinal()
	if len(final) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	b, err := final.Marshal()
	if err != nil {
		h.log.Errorf("error marshalling response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	WriteData(w, http.StatusOK, b, "Content-Type", "application/json")

	duration := time.Since(startTime)
	if h.batchResTimeHist != nil {
		h.batchResTimeHist.WithLabelValues(
			batchSizeClass(len(requests)),
		).Observe(duration.Seconds())
	}

	h.log.WithFields(map[string]interface{}{
		"requests":     len(requests),
		"requests_id":  requestIDs,
		"allowed":      allowed,
		"denied":       denied,
		"cache_hits":   cacheHits,
		"cache_misses": cacheMisses,
		"duration":     duration,
		"source":       GetSourceIP(r),
	}).Info("processed batch of requests")
}

func (h *JsonRpcHandler) getResponsesFromUpstream(httpRequest *http.Request, requests JsonRpcMsgs, next func(http.ResponseWriter, *http.Request)) (JsonRpcMsgs, error) {
	b, err := requests.Marshal()
	if err != nil {
		return nil, fmt.Errorf("error marshalling requests to upstream: %v", err)
	}
	req := httpRequest.Clone(httpRequest.Context())
	req.Body = io.NopCloser(bytes.NewReader(b))
	req.ContentLength = int64(len(b))

	// Cap how much of the upstream response we buffer — without this a
	// pathological / malicious upstream returning gigabytes to a single
	// batch OOMs the proxy. Use a capped writer that stops buffering once
	// the limit is exceeded, so the cap is enforced DURING next() rather
	// than after the whole body is already in memory (a bare
	// httptest.ResponseRecorder buffers everything first). 32 MiB is
	// generous for any legitimate Cosmos / EVM batch response. Mirrors
	// the inbound http.MaxBytesReader guard on the response side.
	const maxUpstreamBatchResponse = 32 << 20
	w := newCappedResponseWriter(maxUpstreamBatchResponse)
	next(w, req)
	if w.overflowed {
		return nil, fmt.Errorf("upstream batch response exceeded %d bytes (cap)", maxUpstreamBatchResponse)
	}
	b = w.buf.Bytes()
	single, responses, _ := ParseJsonRpcMessage(b)
	if len(responses) == 0 && single != nil {
		responses = JsonRpcMsgs{single}
	}
	return responses, nil
}

// cappedResponseWriter is an http.ResponseWriter that buffers the body up
// to a byte cap and then stops, flagging overflow. Used for the JSON-RPC
// batch upstream call so a rogue upstream returning an enormous body
// can't grow an unbounded in-memory buffer (the OOM a bare
// httptest.ResponseRecorder would allow): once the cap is hit we stop
// copying bytes and report overflow to the caller, which fails the batch.
type cappedResponseWriter struct {
	header     http.Header
	buf        bytes.Buffer
	limit      int
	written    int
	overflowed bool
}

func newCappedResponseWriter(limit int) *cappedResponseWriter {
	return &cappedResponseWriter{header: make(http.Header), limit: limit}
}

func (w *cappedResponseWriter) Header() http.Header { return w.header }

func (w *cappedResponseWriter) WriteHeader(int) {}

func (w *cappedResponseWriter) Write(p []byte) (int, error) {
	if w.overflowed {
		// Pretend success so the upstream handler keeps draining its
		// source instead of erroring; we've already decided to fail the
		// batch and won't use any further bytes.
		return len(p), nil
	}
	remaining := w.limit - w.written
	if len(p) > remaining {
		w.buf.Write(p[:remaining])
		w.written += remaining
		w.overflowed = true
		return len(p), nil
	}
	n, err := w.buf.Write(p)
	w.written += n
	return n, err
}
