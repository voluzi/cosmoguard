package cosmoguard

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// externalCacheCapacity bounds the in-memory validation cache so a
// malicious or buggy client minting unique credentials can't grow it
// without limit. ttlcache evicts the oldest entries (LRU-ish) when
// the cap is hit, so genuine traffic still gets cached. Sized large
// enough to comfortably hold every active key for typical operator
// deployments (low thousands) but small enough that the worst case
// is bounded memory rather than an OOM.
const externalCacheCapacity = 10_000

// externalValidatorMethod resolves identities by calling out to an external
// HTTP validation service — the shape covers Allora-style developer
// portals, OpenAI-style key-validation endpoints, and any internal
// "is this token valid?" service.
//
// Sketch:
//
//	auth.methods:
//	  - type: external-validator
//	    endpoint: https://api.allora.network/v1/keys/validate
//	    validatorMethod: GET
//	    header: Authorization              # incoming header carrying the token
//	    forwardHeader: Authorization       # outgoing header on the validator request
//	    responseValid: data.valid          # JSON dot-path, must be truthy
//	    responseIdentity: data.userId
//	    responseScopes: data.scopes
//	    cacheTTL: 60s
//	    timeout: 2s
//	    failureMode: fail-closed
//
// Validation results are TTL-cached so the validator isn't on the hot
// path. Cache lookups use a sha-of-credential key so credentials are
// never echoed into cache keyspace logs.
type externalValidatorMethod struct {
	endpoint       string
	method         string
	inHeader       string
	forwardHeader  string
	responseValid  []string // dot-path split
	responseID     []string
	responseScopes []string
	cacheTTL       time.Duration
	timeout        time.Duration
	failOpen       bool
	stripHdrs      []string

	// cache is a bounded TTL cache keyed on the hashed credential.
	// Capacity is fixed at externalCacheCapacity so a flood of unique
	// credentials can't grow it without limit (the previous sync.Map
	// had no upper bound — a hostile or buggy caller could OOM
	// cosmoguard by minting fresh tokens). ttlcache handles both
	// TTL-based and capacity-based eviction in one structure.
	cache      *ttlcache.Cache[string, *Identity]
	httpClient *http.Client
}

func buildExternalValidatorMethod(cfg AuthMethodConfig) (*externalValidatorMethod, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("external-validator method requires `endpoint`")
	}
	method := strings.ToUpper(cfg.ValidatorMethod)
	switch method {
	case "":
		method = http.MethodGet
	case http.MethodGet, http.MethodPost:
		// OK
	default:
		return nil, fmt.Errorf("external-validator method: unsupported HTTP method %q (GET/POST only)", method)
	}
	hdr := cfg.Header
	if hdr == "" {
		hdr = "Authorization"
	}
	forwardHdr := cfg.ForwardHeader
	if forwardHdr == "" {
		forwardHdr = hdr
	}
	respValid := cfg.ResponseValid
	if respValid == "" {
		respValid = "valid"
	}
	respID := cfg.ResponseIdentity
	if respID == "" {
		respID = "userId"
	}
	ttl := cfg.CacheTTL
	if ttl == 0 {
		ttl = 60 * time.Second
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	failOpen := cfg.FailureMode != "fail-closed" // default fail-open

	cache := ttlcache.New[string, *Identity](
		ttlcache.WithTTL[string, *Identity](ttl),
		ttlcache.WithCapacity[string, *Identity](externalCacheCapacity),
		ttlcache.WithDisableTouchOnHit[string, *Identity](),
	)
	go cache.Start()

	return &externalValidatorMethod{
		endpoint:       cfg.Endpoint,
		method:         method,
		inHeader:       hdr,
		forwardHeader:  forwardHdr,
		responseValid:  strings.Split(respValid, "."),
		responseID:     strings.Split(respID, "."),
		responseScopes: splitNonEmpty(cfg.ResponseScopes, "."),
		cacheTTL:       ttl,
		timeout:        timeout,
		failOpen:       failOpen,
		stripHdrs:      []string{hdr},
		cache:          cache,
		httpClient:     &http.Client{Timeout: timeout},
	}, nil
}

// Close stops the ttlcache background expiration goroutine. Wired up
// via Authenticator.Close so the goroutine doesn't leak across reload
// cycles. Idempotent — ttlcache.Cache.Stop handles repeated calls.
func (m *externalValidatorMethod) Close() {
	if m.cache != nil {
		m.cache.Stop()
	}
}

func splitNonEmpty(s, sep string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, sep)
}

func (m *externalValidatorMethod) Resolve(r *http.Request) (*Identity, error) {
	raw := strings.TrimSpace(r.Header.Get(m.inHeader))
	if raw == "" {
		return nil, nil
	}

	// Cache key = the credential itself, hashed so logs/metrics never see
	// the cleartext. The cache is per-process; HPA-scaled replicas keep
	// independent caches today. Phase D's shared Redis pool can be wired
	// in here in a follow-up.
	cacheKey := credentialCacheKey(raw)

	if item := m.cache.Get(cacheKey); item != nil {
		return item.Value(), nil
	}

	id, err := m.validateUpstream(r.Context(), raw)
	if err != nil {
		if m.failOpen {
			// Fail-open: the validator is unreachable and the operator
			// chose availability over strict verification. Return a
			// degraded identity the auth gate honours unconditionally
			// (see Authorize) rather than nil — nil reads as "anonymous"
			// and the gate would deny every protected route, turning
			// fail-open into fail-closed. Not cached, so the next request
			// retries the validator.
			return degradedIdentity("external-validator"), nil
		}
		// Fail-closed: the validator is unreachable and the operator chose
		// strict verification. Surface ErrAuthUnavailable (not a generic
		// error) so BOTH the HTTP and gRPC paths deny — a generic error is
		// treated as anonymous (fail-open) upstream, which would admit the
		// request on a public / auth.require:false rule.
		return nil, fmt.Errorf("%w: %v", ErrAuthUnavailable, err)
	}

	// Cache positive results for the configured TTL. Negative results
	// (validator returned 4xx → id == nil) are cached briefly so an
	// outage that briefly flipped a key to invalid doesn't cement the
	// "anonymous" decision for the full cacheTTL. Matches the
	// introspection method's 5s clamp — long enough to absorb a burst
	// of probes against the same bad credential, short enough that
	// legitimate clients recover quickly after a revoke-and-replace.
	ttl := m.cacheTTL
	if id == nil && ttl > 5*time.Second {
		ttl = 5 * time.Second
	}
	m.cache.Set(cacheKey, id, ttl)
	return id, nil
}

func (m *externalValidatorMethod) HeadersToStrip() []string { return m.stripHdrs }

// validateUpstream calls the validator and parses its response.
func (m *externalValidatorMethod) validateUpstream(ctx context.Context, credential string) (*Identity, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	var body io.Reader
	if m.method == http.MethodPost {
		// Minimal POST shape: pass the credential as a JSON object. Most
		// portals accept this; richer shapes (templated body) can follow
		// when an operator actually needs them.
		b, _ := stdjson.Marshal(map[string]string{"apiKey": credential})
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, m.method, m.endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(m.forwardHeader, credential)
	if m.method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("external validator: %w", err)
	}
	defer resp.Body.Close()

	// Cap+1 + length check so an oversized validator response surfaces
	// as a clear error rather than silently truncating into invalid
	// JSON. 1 MiB is generous for any sensible external-validator
	// payload (typically a few KB of identity claims).
	const maxExternalValidatorBody = 1 << 20
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxExternalValidatorBody+1))
	if err != nil {
		return nil, fmt.Errorf("external validator: read response: %w", err)
	}
	if int64(len(respBytes)) > maxExternalValidatorBody {
		return nil, fmt.Errorf("external validator: response exceeded %d-byte cap", maxExternalValidatorBody)
	}
	// 4xx and 5xx are split: 4xx is a clear "this credential isn't
	// valid", treated as anonymous fall-through. 5xx is a validator-
	// side error and honors the operator's failureMode (fail-open vs
	// fail-closed). Without this split, an outage at the validator
	// silently rejected every credential — equivalent to fail-closed
	// regardless of config.
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("external validator: status %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	var doc map[string]any
	if err := stdjson.Unmarshal(respBytes, &doc); err != nil {
		return nil, fmt.Errorf("external validator: parse response: %w", err)
	}
	if !isTruthy(lookupPath(doc, m.responseValid)) {
		return nil, nil
	}
	name, _ := lookupPath(doc, m.responseID).(string)
	if name == "" {
		// Validator said valid but didn't name the identity. Treat as
		// anonymous-authenticated for now; operators can fix their
		// response shape if they want named identities.
		name = "external"
	}

	id := &Identity{Name: name, Method: "external-validator"}
	if len(m.responseScopes) > 0 {
		id.Scopes = extractScopes(lookupPath(doc, m.responseScopes))
	}
	return id, nil
}

// lookupPath walks a nested map[string]any by dot-path components.
// Returns nil if any segment is missing or not a map.
func lookupPath(doc map[string]any, path []string) any {
	var cur any = doc
	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[seg]
	}
	return cur
}

// isTruthy interprets a JSON-derived value as the operator likely intended.
// This gates an auth ACCEPT decision (external-validator responseValid /
// introspection `active`), so for STRING values it uses an explicit truthy
// allowlist rather than a blocklist: the previous form only rejected "",
// "false", and "0", so a stringly-typed backend returning "False", "no",
// "off", or "inactive" for an invalid credential was accepted. Booleans pass
// through; non-zero numbers are true; empty containers/nil are false.
func isTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "1", "yes", "y", "on", "active", "valid":
			return true
		default:
			return false
		}
	case float64:
		return x != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return false
}

// credentialCacheKey hashes the credential so cache entries don't expose
// the cleartext. A non-cryptographic 64-bit hash is fine here — we're
// not protecting against pre-image attacks, just avoiding leaking the
// key into in-memory data structures that might be exposed via
// diagnostic endpoints.
func credentialCacheKey(cred string) string {
	return "ext:" + fnv64Hex(cred)
}

// fnv64Hex computes the FNV-1a 64-bit hash of s as a hex string. Same
// hash family the rule fingerprint uses (rules.go); kept local here
// so this file doesn't import pkg/util (cyclic).
func fnv64Hex(s string) string {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return fmt.Sprintf("%x", h)
}
