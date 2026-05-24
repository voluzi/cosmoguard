package cosmoguard

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gobwas/glob"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// fingerprint helpers — write small primitives into the running hash with
// length-prefixing where ambiguity is possible, so adjacent fields can't
// merge into the same byte sequence.

func writeFingerprintInt(h hash.Hash64, v int) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	_, _ = h.Write(buf[:])
}

func writeFingerprintBool(h hash.Hash64, v bool) {
	if v {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
}

func writeFingerprintStr(h hash.Hash64, s string) {
	writeFingerprintInt(h, len(s))
	_, _ = h.Write([]byte(s))
}

func writeFingerprintStrSlice(h hash.Hash64, ss []string) {
	// Sort first so two slices with the same elements in different orders
	// hash identically. The matching semantics treat them as equivalent
	// (any-of), so the cache namespace should too.
	cp := append([]string(nil), ss...)
	sort.Strings(cp)
	writeFingerprintInt(h, len(cp))
	for _, s := range cp {
		writeFingerprintStr(h, s)
	}
}

func writeFingerprintStrMap(h hash.Hash64, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	writeFingerprintInt(h, len(keys))
	for _, k := range keys {
		writeFingerprintStr(h, k)
		writeFingerprintStr(h, m[k])
	}
}

type RuleAction string

const (
	RuleActionAllow = "allow"
	RuleActionDeny  = "deny"
)

type HttpRule struct {
	Priority int        `yaml:"priority,omitempty" default:"1000"`
	Action   RuleAction `yaml:"action"`

	// Tag is the operator-supplied label used in the `rule_id`
	// Prometheus label. Bounded by config so cardinality stays safe;
	// when empty, falls back to `default` (or `unmatched` if no rule
	// matched). Pick short, stable identifiers ("lcd-balance",
	// "broadcast-tx") — they show up in dashboards.
	Tag string `yaml:"tag,omitempty"`

	// v4 expressive matcher. If non-nil, the user-supplied tree is preserved
	// verbatim and combined with the v3 flat fields below at Compile time.
	Match *MatchTree `yaml:"match,omitempty"`

	// v3 flat-syntax fields. Still parsed for backward compat; desugared
	// into the effective matcher at Compile time. New configs should prefer
	// `match:`.
	Paths   []string          `yaml:"paths,omitempty"`
	Methods []string          `yaml:"methods,omitempty"`
	Query   map[string]string `yaml:"query,omitempty"`

	Cache *RuleCache `yaml:"cache,omitempty"`

	// RateLimit, when set, throttles matching requests. Bucket state lives
	// in Redis when cache.redis is configured (so HPA-scaled replicas share
	// the budget); falls back to in-process buckets otherwise. The actual
	// RateLimiter implementation is owned by the proxy (one per rule by
	// fingerprint) so we don't mutate the rule struct after Compile.
	RateLimit *RateLimitConfig `yaml:"rateLimit,omitempty"`

	// Auth gates the rule on the resolved Identity. Combined with the
	// global auth.defaultRequire flag to decide whether unauthenticated
	// requests reach this rule.
	Auth *RuleAuthConfig `yaml:"auth,omitempty"`

	// effectiveMatch is the actual matcher used by Matches. Built by Compile
	// from a fresh shallow-copy of r.Match plus the v3 flat fields, so
	// repeated Compile calls produce identical state (idempotent).
	//
	// PUBLICATION CONTRACT: effectiveMatch is set during Compile (config-load
	// time) and read concurrently by Matches (per-request, no lock). A rule
	// becomes safely visible to handlers only via SetRules under
	// rulesMutex; that synchronization point provides the happens-before
	// edge. Do NOT call Compile again on a rule that's already in service —
	// the race detector would catch the resulting torn read.
	effectiveMatch *MatchTree           `yaml:"-"`
	PathGlobs      []glob.Glob          `yaml:"-"`
	QueryGlobs     map[string]glob.Glob `yaml:"-"`

	// Fingerprint: stable hash of the rule's matching criteria + cache shape,
	// computed at Compile() time. Mixed into the cache key so two rules that
	// happen to match the same request never share cache entries.
	Fingerprint uint64 `yaml:"-"`
}

func (r *HttpRule) String() string {
	return fmt.Sprintf("%d - %s - %v - %v", r.Priority, r.Action, r.Methods, r.Paths)
}

// Compile validates and materializes the matcher state on the rule, and
// computes a stable Fingerprint used to namespace cache entries per rule.
// Returns an error if any user-supplied glob, IP/CIDR, or sub-tree is
// malformed.
//
// Compile builds/finalizes the effective MatchTree:
//   - If r.Match is nil, an empty tree is created and the v3 flat fields
//     (Paths/Methods/Query) are folded into it as leaf atoms.
//   - If r.Match is non-nil, the v3 flat fields are merged in as additional
//     leaf atoms on the top-level tree (treated as implicit `all`). Mixing
//     is allowed; new configs should prefer the explicit tree form.
//
// The legacy PathGlobs / QueryGlobs slices are still populated so any
// dependent code in this package keeps working until the unified pipeline
// (Phase D) retires them.
//
// Always reinitializes state so calling Compile twice produces a clean
// result.
func (r *HttpRule) Compile() error {
	if err := r.RateLimit.validate(); err != nil {
		return err
	}
	// Build a fresh effective match tree on every Compile call so two
	// compiles in a row produce identical state (idempotent).
	em := &MatchTree{}
	if r.Match != nil {
		// Shallow copy of the user-supplied tree's top-level fields. We
		// don't deep-clone children because we don't mutate them.
		em.All = r.Match.All
		em.Any = r.Match.Any
		em.None = r.Match.None
		em.Method = r.Match.Method
		em.Methods = append([]string(nil), r.Match.Methods...)
		em.Path = r.Match.Path
		em.Paths = append([]string(nil), r.Match.Paths...)
		em.SourceIP = r.Match.SourceIP
		if r.Match.Query != nil {
			em.Query = map[string]string{}
			for k, v := range r.Match.Query {
				em.Query[k] = v
			}
		}
		if r.Match.Header != nil {
			em.Header = map[string]string{}
			for k, v := range r.Match.Header {
				em.Header[k] = v
			}
		}
	}
	// Merge v3 flat fields.
	if len(r.Paths) > 0 {
		em.Paths = append(em.Paths, r.Paths...)
	}
	if len(r.Methods) > 0 {
		em.Methods = append(em.Methods, r.Methods...)
	}
	// Track whether the Query map ended up holding v3-originated values.
	// If it did AND the user-supplied v4 tree had no Query of its own, the
	// legacy semantics (treat "present"/"absent" as ordinary globs) apply.
	// If the user supplied a v4 query block too, we conservatively switch
	// to v4 keyword semantics across the merged map — they explicitly
	// opted in.
	v4QueryFromUser := r.Match != nil && len(r.Match.Query) > 0
	if len(r.Query) > 0 {
		if em.Query == nil {
			em.Query = map[string]string{}
		}
		for k, v := range r.Query {
			em.Query[k] = v
		}
		if !v4QueryFromUser {
			em.legacyV3Query = true
		}
	}
	if err := em.Compile(); err != nil {
		return fmt.Errorf("http rule (priority %d): %w", r.Priority, err)
	}
	r.effectiveMatch = em

	// Legacy globs still populated for any pre-D code path (and for tests
	// that assert on PathGlobs / QueryGlobs length).
	r.PathGlobs = nil
	if len(em.Paths) > 0 {
		r.PathGlobs = make([]glob.Glob, 0, len(em.Paths))
		for _, p := range em.Paths {
			g, _ := glob.Compile(p, '/') // already validated above
			r.PathGlobs = append(r.PathGlobs, g)
		}
	}
	r.QueryGlobs = nil
	if len(em.Query) > 0 {
		r.QueryGlobs = make(map[string]glob.Glob, len(em.Query))
		for k, v := range em.Query {
			if v == "present" || v == "absent" {
				g, _ := glob.Compile("*")
				r.QueryGlobs[k] = g
				continue
			}
			g, _ := glob.Compile(v)
			r.QueryGlobs[k] = g
		}
	}

	r.Fingerprint = httpRuleFingerprint(r)
	return nil
}

// httpRuleFingerprint produces a stable 64-bit hash of an HttpRule's matching
// criteria + cache shape. Stable across runs (no random salts) and order-
// independent within a single rule (paths/methods/query keys are sorted
// before hashing). Different rules → different fingerprints with extremely
// high probability.
//
// After Compile runs, all matching state lives in r.Match — so the
// fingerprint hashes ONLY the match tree (which already absorbed any v3
// flat-syntax fields) plus priority/action/cache. This keeps the
// fingerprint invariant under v3→v4 syntactic-sugar conversion: an
// operator who migrates `paths: [/x]` to `match: { path: /x }` gets the
// same cache key.
func httpRuleFingerprint(r *HttpRule) uint64 {
	h := fnv.New64a()
	writeFingerprintInt(h, r.Priority)
	writeFingerprintStr(h, string(r.Action))
	if r.effectiveMatch != nil {
		writeFingerprintStr(h, "match:")
		r.effectiveMatch.writeFingerprint(h)
	}
	if r.Cache != nil {
		writeFingerprintStr(h, "cache:")
		writeFingerprintBool(h, r.Cache.Enable)
		// Hash the duration as raw nanoseconds (an int64) rather than its
		// rendered form. time.Duration.String() is stable today but
		// "rendered representation" is the wrong contract to depend on.
		writeFingerprintInt(h, int(r.Cache.TTL))
		writeFingerprintBool(h, r.Cache.CacheError)
		writeFingerprintBool(h, r.Cache.CacheEmptyResult)
		writeFingerprintStrSlice(h, r.Cache.PreserveHeaders)
	}
	if r.RateLimit != nil {
		writeFingerprintStr(h, "rl:")
		writeFingerprintStr(h, strconv.FormatFloat(r.RateLimit.Rate.PerSecond, 'f', -1, 64))
		writeFingerprintInt(h, r.RateLimit.Burst)
		writeFingerprintStr(h, string(r.RateLimit.Scope))
	}
	return h.Sum64()
}

// Matches reports whether the request satisfies this rule. (Named "Matches"
// rather than the conventional "Match" because the v4 schema also has a
// Match *MatchTree field on the rule struct, and Go doesn't allow a field
// and method to share a name.)
func (r *HttpRule) Matches(req *http.Request) bool {
	if r.effectiveMatch != nil {
		sourceIP := net.ParseIP(stripPort(GetSourceIP(req)))
		return r.effectiveMatch.MatchRequest(req, sourceIP)
	}
	// Fallback to legacy glob fields. Reached only when Compile has not been
	// called yet — practically never, but kept for safety.
	if len(r.Methods) > 0 && !util.SliceContainsStringIgnoreCase(r.Methods, req.Method) {
		return false
	}
	if len(r.Paths) > 0 {
		matched := false
		for _, g := range r.PathGlobs {
			if g.Match(req.URL.Path) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(r.QueryGlobs) > 0 {
		q := req.URL.Query()
		for k, g := range r.QueryGlobs {
			vals, ok := q[k]
			if !ok || len(vals) == 0 {
				return false
			}
			if !g.Match(vals[0]) {
				return false
			}
		}
	}
	return true
}

// stripPort accepts "host:port" or just "host" and returns the host portion.
// Used when GetSourceIP returns a RemoteAddr-style string with a port suffix
// that net.ParseIP would reject.
func stripPort(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		// IPv6 addresses contain colons too; be sure the host is bracketed.
		if strings.HasPrefix(s, "[") {
			if j := strings.Index(s, "]"); j > 0 {
				return s[1:j]
			}
		}
		// Heuristic: if more than one colon, this is bare IPv6 (no port).
		if strings.Count(s, ":") > 1 {
			return s
		}
		return s[:i]
	}
	return s
}

type JsonRpcRule struct {
	Priority int        `yaml:"priority,omitempty" default:"1000"`
	Action   RuleAction `yaml:"action"`
	// Tag — operator-supplied label for the `rule_id` Prometheus
	// label. See HttpRule.Tag.
	Tag     string      `yaml:"tag,omitempty"`
	Methods []string    `yaml:"methods,omitempty"`
	Params  interface{} `yaml:"params,omitempty"`
	Cache   *RuleCache  `yaml:"cache,omitempty"`
	// Auth gates the rule on the resolved Identity (same semantics
	// as HttpRule.Auth). nil → fall back to the global defaultRequire.
	Auth *RuleAuthConfig `yaml:"auth,omitempty"`
	// RateLimit throttles matching requests with a per-rule token
	// bucket (same semantics as HttpRule.RateLimit).
	RateLimit *RateLimitConfig `yaml:"rateLimit,omitempty"`

	MethodGlobs []glob.Glob          `yaml:"-"`
	ParamsGlobs map[string]glob.Glob `yaml:"-"`
	ParamsMap   bool                 `yaml:"-"`
	ParamsSlice bool                 `yaml:"-"`

	// Fingerprint is a 64-bit stable hash of the rule's matching
	// criteria + action — used as the fallback `rule_id` metric label
	// when Tag is empty. Computed at Compile().
	Fingerprint uint64 `yaml:"-"`
}

func (r *JsonRpcRule) String() string {
	return fmt.Sprintf("%d - %s - %v - %v", r.Priority, r.Action, r.Methods, r.Params)
}

// Compile validates and materializes glob fields. Reinitializes everything
// on each call so a previously-compiled rule whose Params changed doesn't
// retain stale entries.
func (r *JsonRpcRule) Compile() error {
	if err := r.RateLimit.validate(); err != nil {
		return err
	}
	r.MethodGlobs = nil
	r.ParamsGlobs = map[string]glob.Glob{}
	r.ParamsMap = false
	r.ParamsSlice = false

	if len(r.Methods) > 0 {
		r.MethodGlobs = make([]glob.Glob, 0, len(r.Methods))
		for _, p := range r.Methods {
			g, err := glob.Compile(p)
			if err != nil {
				return fmt.Errorf("jsonrpc rule (priority %d) method %q: %w", r.Priority, p, err)
			}
			r.MethodGlobs = append(r.MethodGlobs, g)
		}
	}
	if r.Params != nil {
		if paramsMap, ok := r.Params.(map[string]interface{}); ok {
			r.ParamsMap = len(paramsMap) > 0
			for key, v := range paramsMap {
				if value, ok := v.(string); ok {
					g, err := glob.Compile(value, '/')
					if err != nil {
						return fmt.Errorf("jsonrpc rule (priority %d) params[%q]=%q: %w", r.Priority, key, value, err)
					}
					r.ParamsGlobs[key] = g
				}
			}
		}
		if paramsSlice, ok := r.Params.([]interface{}); ok {
			r.ParamsSlice = len(paramsSlice) > 0
			for i, v := range paramsSlice {
				if value, ok := v.(string); ok {
					g, err := glob.Compile(value, '/')
					if err != nil {
						return fmt.Errorf("jsonrpc rule (priority %d) params[%d]=%q: %w", r.Priority, i, value, err)
					}
					r.ParamsGlobs[strconv.Itoa(i)] = g
				}
			}
		}
	}
	r.Fingerprint = jsonRpcRuleFingerprint(r)
	return nil
}

// jsonRpcRuleFingerprint produces a stable 64-bit hash of the rule's
// matching surface — same role as HttpRule.Fingerprint. Used as the
// fallback `rule_id` metric label when Tag is empty AND mixed into
// rate-limit / cache keys so two semantically distinct rules never
// share a namespace.
//
// MUST be deterministic across processes (same YAML on a different
// HPA replica must produce the same fingerprint). The previous
// implementation used fmt.Sprintf("%v", r.Params) which, for
// map[string]interface{}, depends on Go's random map iteration —
// every pod's Redis-backed cache namespace differed, collapsing the
// effective shared cache to per-pod. Switch to json.Marshal which
// emits string-keyed maps in alphabetical key order.
func jsonRpcRuleFingerprint(r *JsonRpcRule) uint64 {
	h := fnv.New64a()
	writeFingerprintStr(h, string(r.Action))
	writeFingerprintStrSlice(h, r.Methods)
	writeFingerprintInt(h, r.Priority)
	if r.Params != nil {
		b, err := json.Marshal(r.Params)
		if err == nil {
			writeFingerprintStr(h, string(b))
		} else {
			// Malformed params shouldn't reach here (Compile would
			// have rejected the rule) but fall back to %v so we
			// don't silently produce a hash of zero bytes.
			writeFingerprintStr(h, fmt.Sprintf("%v", r.Params))
		}
	}
	if r.RateLimit != nil {
		writeFingerprintStr(h, "rl:")
		writeFingerprintStr(h, strconv.FormatFloat(r.RateLimit.Rate.PerSecond, 'f', -1, 64))
		writeFingerprintInt(h, r.RateLimit.Burst)
		writeFingerprintStr(h, string(r.RateLimit.Scope))
	}
	if r.Auth != nil {
		writeFingerprintStr(h, "auth:")
		if r.Auth.Require != nil {
			writeFingerprintBool(h, *r.Auth.Require)
		}
		writeFingerprintStrSlice(h, r.Auth.Scopes)
		writeFingerprintStrSlice(h, r.Auth.Identities)
	}
	return h.Sum64()
}

func (r *JsonRpcRule) Match(req *JsonRpcMsg) bool {
	// Check if method matches (if no methods configured on rule, we accept any)
	methodMatch := len(r.Methods) == 0
	for _, g := range r.MethodGlobs {
		if g.Match(req.Method) {
			methodMatch = true
		}
	}
	if !methodMatch {
		return false
	}

	// If we dont have any params configured on rule, its a match
	if !(r.ParamsMap || r.ParamsSlice) {
		return true
	}

	switch requestParams := req.Params.(type) {
	case map[string]interface{}:
		if !r.ParamsMap {
			return true
		}
		paramsMap, ok := r.Params.(map[string]interface{})
		if !ok {
			log.Errorf("jsonrpc: request params not a map: %v", requestParams)
			return false
		}
		for key, v := range paramsMap {
			if g, ok := r.ParamsGlobs[key]; ok {
				reqV, exists := requestParams[key]
				if !exists {
					return false
				}
				str, ok := reqV.(string)
				if !ok {
					return false
				}
				if !g.Match(str) {
					return false
				}
			} else {
				if v != requestParams[key] {
					return false
				}
			}
		}
		return true
	case []interface{}:
		if !r.ParamsSlice {
			return true
		}
		paramsSlice, ok := r.Params.([]interface{})
		if !ok {
			log.Errorf("jsonrpc: request params not a slice: %v", requestParams)
			return false
		}
		for i, v := range paramsSlice {
			// Guard against a rule whose params slice is longer than
			// the request's: without this, both branches below
			// dereference requestParams[i] out of bounds and panic
			// (recovered by recoverHTTP but the request still 500s).
			// A rule that asks for more positional params than the
			// request supplied is, by construction, not a match.
			if i >= len(requestParams) {
				return false
			}
			if g, ok := r.ParamsGlobs[strconv.Itoa(i)]; ok {
				str, ok := requestParams[i].(string)
				if !ok {
					return false
				}
				if !g.Match(str) {
					return false
				}
			} else {
				if v != requestParams[i] {
					return false
				}
			}
		}
		return true
	default:
		log.Warnf("unsupported params type: %T\n", requestParams)
		return false
	}
}

type GrpcRule struct {
	Priority int        `yaml:"priority,omitempty" default:"1000"`
	Action   RuleAction `yaml:"action"`
	// Tag — operator-supplied label for the `rule_id` Prometheus
	// label. See HttpRule.Tag.
	Tag         string      `yaml:"tag,omitempty"`
	Methods     []string    `yaml:"methods,omitempty"`
	Cache       *RuleCache  `yaml:"cache,omitempty"`
	MethodGlobs []glob.Glob `yaml:"-"`
	// Auth gates the rule on the resolved Identity (same semantics
	// as HttpRule.Auth). nil → fall back to the global defaultRequire.
	Auth *RuleAuthConfig `yaml:"auth,omitempty"`
	// RateLimit throttles matching requests with a per-rule token
	// bucket (same semantics as HttpRule.RateLimit).
	RateLimit *RateLimitConfig `yaml:"rateLimit,omitempty"`

	// Fingerprint is mixed into the gRPC cache key to namespace per-rule
	// cache entries — same role as HttpRule.Fingerprint.
	Fingerprint uint64 `yaml:"-"`
}

func (r *GrpcRule) String() string {
	return fmt.Sprintf("%d - %s - %v", r.Priority, r.Action, r.Methods)
}

// Compile validates and materializes glob fields. Reinitializes on each call.
// Also computes Fingerprint for per-rule cache-key namespacing.
func (r *GrpcRule) Compile() error {
	if err := r.RateLimit.validate(); err != nil {
		return err
	}
	r.MethodGlobs = nil
	if len(r.Methods) > 0 {
		r.MethodGlobs = make([]glob.Glob, 0, len(r.Methods))
		for _, p := range r.Methods {
			g, err := glob.Compile(p, '/')
			if err != nil {
				return fmt.Errorf("grpc rule (priority %d) method %q: %w", r.Priority, p, err)
			}
			r.MethodGlobs = append(r.MethodGlobs, g)
		}
	}
	// Reject unknown KeyMode values up-front. Previously a typo like
	// "cannonical" or "method_only" fell through grpcCacheKey's
	// switch default and hashed the raw payload — the operator never
	// saw an error and silently lost canonicalization.
	if r.Cache != nil {
		switch r.Cache.KeyMode {
		case "", "raw", "method-only", "canonical":
			// ok
		default:
			return fmt.Errorf("grpc rule (priority %d) cache.keyMode: unknown value %q (want one of: raw, method-only, canonical)", r.Priority, r.Cache.KeyMode)
		}
	}
	r.Fingerprint = grpcRuleFingerprint(r)
	return nil
}

func grpcRuleFingerprint(r *GrpcRule) uint64 {
	h := fnv.New64a()
	writeFingerprintInt(h, r.Priority)
	writeFingerprintStr(h, string(r.Action))
	writeFingerprintStrSlice(h, r.Methods)
	if r.Cache != nil {
		writeFingerprintBool(h, r.Cache.Enable)
		writeFingerprintInt(h, int(r.Cache.TTL))
		writeFingerprintBool(h, r.Cache.CacheError)
	}
	if r.RateLimit != nil {
		writeFingerprintStr(h, "rl:")
		writeFingerprintStr(h, strconv.FormatFloat(r.RateLimit.Rate.PerSecond, 'f', -1, 64))
		writeFingerprintInt(h, r.RateLimit.Burst)
		writeFingerprintStr(h, string(r.RateLimit.Scope))
	}
	if r.Auth != nil {
		writeFingerprintStr(h, "auth:")
		if r.Auth.Require != nil {
			writeFingerprintBool(h, *r.Auth.Require)
		}
		writeFingerprintStrSlice(h, r.Auth.Scopes)
		writeFingerprintStrSlice(h, r.Auth.Identities)
	}
	return h.Sum64()
}

func (r *GrpcRule) Match(method string) bool {
	if len(r.Methods) == 0 {
		return true
	}
	for _, g := range r.MethodGlobs {
		if g.Match(method) {
			return true
		}
	}
	return false
}
