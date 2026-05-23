package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olric-data/olric"
	"golang.org/x/time/rate"
)

// RateLimitScope determines whose request rate a single token bucket counts.
type RateLimitScope string

const (
	// RateLimitScopeGlobal counts every matching request against a single
	// bucket. Useful for "no more than N req/s through this rule, period".
	RateLimitScopeGlobal RateLimitScope = "global"
	// RateLimitScopePerIP gives each source IP its own bucket. The most
	// common shape — caps any one client without affecting others.
	RateLimitScopePerIP RateLimitScope = "per-ip"
	// RateLimitScopePerIdentity gives each authenticated identity its
	// own bucket. Falls back to per-ip when the request is anonymous so
	// unauthenticated traffic is still capped per-source.
	RateLimitScopePerIdentity RateLimitScope = "per-identity"
	// RateLimitScopeCompound buckets requests by (identity, IP)
	// together. Useful when you want to cap each client's traffic but
	// also prevent a single authenticated identity from being abused
	// from many IPs. On anonymous traffic it degrades to per-IP.
	RateLimitScopeCompound RateLimitScope = "compound"
)

// RateLimitConfig is the YAML-facing shape attached to a rule (or, in the
// future, the global section).
type RateLimitConfig struct {
	// Rate is the steady-state request rate the bucket refills at,
	// expressed as either a number (req/sec) or a duration (one req per
	// Duration). YAML examples:
	//
	//   rate: 100             # 100 req/sec
	//   rate: 100/s
	//   rate: 30/min
	//   rate: 1/5s
	//
	// Parsed by ParseRate (yaml.UnmarshalYAML handles it).
	Rate Rate `yaml:"rate"`
	// Burst is the maximum number of tokens the bucket can hold. Defaults
	// to max(Rate, 1) when omitted.
	Burst int `yaml:"burst,omitempty"`
	// Scope determines whose request rate the bucket counts.
	Scope RateLimitScope `yaml:"scope,omitempty" default:"per-ip"`
}

// Rate carries a tokens-per-second value and the original spec string so
// fingerprints and error messages can reference what the operator wrote.
type Rate struct {
	PerSecond float64
	Spec      string
}

// UnmarshalYAML accepts either a number ("100") or a fraction
// ("100/s", "30/min", "1/5s") for the rate value.
func (r *Rate) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		// Try as a plain number.
		var n float64
		if err2 := unmarshal(&n); err2 != nil {
			// Report err2 (the number-parse failure) — err is the
			// string-parse failure we already moved past, and
			// reporting it would mislead the operator. %w preserves
			// the chain so errors.As can recover the underlying
			// yaml.TypeError.
			return fmt.Errorf("rate must be a number or string: %w", err2)
		}
		r.PerSecond = n
		r.Spec = strconv.FormatFloat(n, 'f', -1, 64)
		return nil
	}
	parsed, err := ParseRate(s)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// ParseRate accepts forms like "100", "100/s", "30/min", "1/5s".
func ParseRate(spec string) (Rate, error) {
	if spec == "" {
		return Rate{}, errors.New("empty rate")
	}
	s := strings.TrimSpace(spec)
	// Plain number → req/s.
	if !strings.Contains(s, "/") {
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Rate{}, fmt.Errorf("rate %q: %w", spec, err)
		}
		return Rate{PerSecond: n, Spec: spec}, nil
	}
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return Rate{}, fmt.Errorf("rate %q: expected NUMBER/UNIT", spec)
	}
	count, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return Rate{}, fmt.Errorf("rate %q: %w", spec, err)
	}
	unitStr := strings.TrimSpace(parts[1])
	// Common shorthand: "s", "sec", "second", "m", "min", "minute", "h", "hour"
	var per time.Duration
	switch unitStr {
	case "s", "sec", "second":
		per = time.Second
	case "m", "min", "minute":
		per = time.Minute
	case "h", "hour":
		per = time.Hour
	default:
		// Try Go duration syntax like "5s", "250ms".
		d, derr := time.ParseDuration(unitStr)
		if derr != nil {
			return Rate{}, fmt.Errorf("rate %q: unrecognised unit %q", spec, unitStr)
		}
		per = d
	}
	if per <= 0 {
		return Rate{}, fmt.Errorf("rate %q: non-positive period", spec)
	}
	return Rate{
		PerSecond: count / per.Seconds(),
		Spec:      spec,
	}, nil
}

// RateLimiter is the abstraction every cosmoguard subsystem uses to decide
// whether to admit a request. Implementations: in-memory token buckets
// (single-replica), Redis-backed sliding window (HPA-multi-replica).
type RateLimiter interface {
	// Allow reports whether one unit of work for the given bucket key may
	// proceed right now. Returns the bucket-specific retry-after duration
	// when denied (zero on allow).
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
	// Close releases any resources (timers, Redis pool, etc.).
	Close() error
}

// NewRateLimiter returns an olric-backed token bucket when an olric
// client is supplied — pods share one budget per key. Falls back to
// a per-pod in-process bucket only when olricClient is nil (test
// paths without a cluster runtime). A silent fallback in production
// would multiply the operator's quota by replicaCount.
func NewRateLimiter(cfg RateLimitConfig, olricClient *olric.EmbeddedClient, keyspace string) (RateLimiter, error) {
	// Reject NaN / Inf / negative explicitly — `<= 0` lets NaN slip
	// through (it compares false to every operator), which would
	// produce a rate.Limit(NaN) bucket that never refills, or feed
	// math.Inf into the refillExp expression below where it
	// silently wraps into a negative time.Duration once cast to
	// int64. Both surface as "operator's rule limiter just stopped
	// working" with no log message.
	if math.IsNaN(cfg.Rate.PerSecond) || math.IsInf(cfg.Rate.PerSecond, 0) {
		return nil, fmt.Errorf("rate limit: rate must be a finite number (got %v)", cfg.Rate.PerSecond)
	}
	if cfg.Rate.PerSecond <= 0 {
		return nil, fmt.Errorf("rate limit: non-positive rate")
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = int(cfg.Rate.PerSecond)
		if burst < 1 {
			burst = 1
		}
	}

	if olricClient != nil {
		return newOlricRateLimiter(olricClient, cfg, keyspace)
	}
	// Per-pod in-process fallback — only the test path that
	// doesn't construct a cluster runtime gets here. Documented in
	// the doc-comment above so a programmatic embedder doesn't
	// believe their multi-replica setup is enforcing a cluster-
	// wide quota by accident.
	return &memoryRateLimiter{
		buckets: map[string]*rate.Limiter{},
		rate:    rate.Limit(cfg.Rate.PerSecond),
		burst:   burst,
	}, nil
}

// memoryRateLimiter is the single-replica in-process limiter. One token
// bucket per key (most commonly per source IP). Buckets stay in the map
// indefinitely; for cosmoguard's bounded distinct-key cardinality this is
// fine. If unbounded distinct IPs ever become a concern, a TTL cleanup
// loop can be added.
type memoryRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*rate.Limiter
	rate    rate.Limit
	burst   int
}

func (l *memoryRateLimiter) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		b = rate.NewLimiter(l.rate, l.burst)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	if b.Allow() {
		return true, 0, nil
	}
	// Compute the time until the next token is available.
	reservation := b.Reserve()
	delay := reservation.Delay()
	reservation.Cancel() // we're not actually consuming this token
	return false, delay, nil
}

func (l *memoryRateLimiter) Close() error { return nil }

// ---------- Scope key derivation ----------

// rateLimitKey returns the bucket key for a given scope + request +
// rule fingerprint + (optional) resolved identity. The fingerprint is
// included so two different rules with the same scope+key don't share
// buckets. The identity is "" when the request is anonymous; the
// per-identity and compound scopes degrade to per-ip in that case.
func rateLimitKey(scope RateLimitScope, ruleFingerprint uint64, r *http.Request, identity string) string {
	prefix := strconv.FormatUint(ruleFingerprint, 16)
	ip := stripPort(GetSourceIP(r))
	switch scope {
	case RateLimitScopeGlobal:
		return prefix + ":global"
	case RateLimitScopePerIP:
		return prefix + ":ip:" + ip
	case RateLimitScopePerIdentity:
		if identity != "" {
			return prefix + ":id:" + identity
		}
		return prefix + ":ip:" + ip
	case RateLimitScopeCompound:
		if identity != "" {
			return prefix + ":compound:" + identity + "|" + ip
		}
		return prefix + ":ip:" + ip
	default:
		return prefix + ":global"
	}
}

// _ unused import guard, kept for clarity that fnv is reserved for future
// per-key compression if cardinality ever becomes a Redis-memory issue.
var _ = fnv.New64a
