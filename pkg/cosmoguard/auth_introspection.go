package cosmoguard

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// introspectionMethod implements RFC 7662 OAuth 2.0 Token Introspection.
// The flow:
//  1. Client sends `Authorization: Bearer <token>`.
//  2. Cosmoguard POSTs the token to the issuer's introspection endpoint
//     as application/x-www-form-urlencoded with `token=<token>`.
//  3. Issuer responds with JSON containing at minimum `{"active": true|false}`,
//     and (when active) any of `sub`, `scope`, `username`, etc.
//  4. Cosmoguard caches the result for cacheTTL so the issuer isn't on
//     the per-request hot path.
//
// Differs from `external-validator` only in protocol shape — introspection
// is the standardized form; external-validator is the freeform escape
// hatch for portals that don't implement RFC 7662 (Allora, OpenAI-style
// dashboards, etc.).
type introspectionMethod struct {
	endpoint      string
	header        string
	identityField string
	scopesField   string
	clientID      string
	clientSecret  string
	cacheTTL      time.Duration
	timeout       time.Duration
	failOpen      bool
	stripHdrs     []string

	// cache is a bounded TTL cache — capacity-capped at
	// externalCacheCapacity so a flood of unique tokens can't grow it
	// unbounded. Was an unbounded sync.Map.
	cache      *ttlcache.Cache[string, *Identity]
	httpClient *http.Client
}

func buildIntrospectionMethod(cfg AuthMethodConfig) (*introspectionMethod, error) {
	if cfg.IntrospectionEndpoint == "" {
		return nil, errors.New("introspection method requires `introspectionEndpoint`")
	}
	hdr := cfg.Header
	if hdr == "" {
		hdr = "Authorization"
	}
	idField := cfg.IdentityField
	if idField == "" {
		idField = "sub"
	}
	scopesField := cfg.ScopesField
	if scopesField == "" {
		scopesField = "scope"
	}
	ttl := cfg.CacheTTL
	if ttl == 0 {
		ttl = 60 * time.Second
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	failOpen := cfg.FailureMode != "fail-closed"

	cache := ttlcache.New[string, *Identity](
		ttlcache.WithTTL[string, *Identity](ttl),
		ttlcache.WithCapacity[string, *Identity](externalCacheCapacity),
		ttlcache.WithDisableTouchOnHit[string, *Identity](),
	)
	go cache.Start()

	return &introspectionMethod{
		endpoint:      cfg.IntrospectionEndpoint,
		header:        hdr,
		identityField: idField,
		scopesField:   scopesField,
		clientID:      cfg.ClientID,
		clientSecret:  cfg.ClientSecret,
		cacheTTL:      ttl,
		timeout:       timeout,
		failOpen:      failOpen,
		stripHdrs:     []string{hdr},
		cache:         cache,
		httpClient:    &http.Client{Timeout: timeout},
	}, nil
}

// Close stops the ttlcache background expiration goroutine. Idempotent.
func (m *introspectionMethod) Close() {
	if m.cache != nil {
		m.cache.Stop()
	}
}

func (m *introspectionMethod) Resolve(r *http.Request) (*Identity, error) {
	raw := strings.TrimSpace(r.Header.Get(m.header))
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "Bearer ") {
		raw = strings.TrimSpace(raw[len("Bearer "):])
	}
	if raw == "" {
		return nil, nil
	}

	cacheKey := "intro:" + fnv64Hex(raw)
	if item := m.cache.Get(cacheKey); item != nil {
		return item.Value(), nil
	}

	id, err := m.introspect(r, raw)
	if err != nil {
		if m.failOpen {
			return nil, nil
		}
		return nil, err
	}
	// Negative results (token invalid → id == nil) get a shorter TTL
	// so an IdP that briefly returned active=false during a key
	// rotation doesn't cement the "anonymous" decision for the full
	// cacheTTL — legitimate clients would otherwise be denied for up
	// to a minute after revoke-and-replace. 5s is plenty to absorb a
	// burst of probes against the same token without subjecting the
	// IdP to a thundering herd when a real recovery happens.
	ttl := m.cacheTTL
	if id == nil && ttl > 5*time.Second {
		ttl = 5 * time.Second
	}
	m.cache.Set(cacheKey, id, ttl)
	return id, nil
}

func (m *introspectionMethod) HeadersToStrip() []string { return m.stripHdrs }

// introspect makes the RFC 7662 POST and parses the response.
func (m *introspectionMethod) introspect(r *http.Request, token string) (*Identity, error) {
	form := url.Values{"token": {token}}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, m.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	// RFC 7662 §2.1: introspection clients authenticate to the endpoint.
	// Most issuers accept HTTP Basic with client_id:client_secret.
	if m.clientID != "" {
		req.SetBasicAuth(m.clientID, m.clientSecret)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("introspection: %w", err)
	}
	defer resp.Body.Close()

	// Cap+1 + length check so an oversized response is rejected
	// loudly instead of silently truncated into invalid JSON — the
	// latter looked indistinguishable from "introspection endpoint
	// returned garbage" in operator logs. 1 MiB is far above any
	// legitimate RFC 7662 introspection response.
	const maxIntrospectionBody = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxIntrospectionBody+1))
	if err != nil {
		return nil, fmt.Errorf("introspection: read response: %w", err)
	}
	if int64(len(body)) > maxIntrospectionBody {
		return nil, fmt.Errorf("introspection: response exceeded %d-byte cap", maxIntrospectionBody)
	}
	// Status handling per RFC 7662: the only response shape that
	// expresses a verdict on the *user's* token is 200 OK with a JSON
	// body carrying `active`. Everything else is an introspection-side
	// signal:
	//   - 5xx: server fault, surface as error so failureMode applies.
	//   - 401/403: cosmoguard's *own* client credentials (clientID /
	//     clientSecret) were rejected, or the introspection client
	//     isn't authorised — nothing to do with the end-user token.
	//   - 429: introspection endpoint is rate-limiting cosmoguard.
	//     Silently caching "anonymous" would deny every legitimate
	//     user for the rate-limit window; surface so failureMode and
	//     operator metrics catch it.
	// Other 4xx (400 malformed, 404 not-found, etc.) still mean the
	// token isn't usable and are treated as not-authenticated — the
	// next configured method gets a chance.
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("introspection: status %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusUnauthorized ||
		resp.StatusCode == http.StatusForbidden ||
		resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("introspection: status %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	var doc map[string]any
	if err := stdjson.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("introspection: parse response: %w", err)
	}
	// RFC 7662 §2.2 says `active` is a boolean, but IdPs in the wild
	// emit `"true"` (string) or `1` (number). A strict `.(bool)`
	// assertion silently treated those as inactive — every token from
	// a non-conforming IdP became anonymous regardless of validity.
	// Reuse the JSON truthiness helper from auth_external.go.
	if !isTruthy(doc["active"]) {
		return nil, nil
	}
	name, _ := doc[m.identityField].(string)
	if name == "" {
		name = "introspected"
	}
	id := &Identity{Name: name, Method: "introspection"}
	if v, ok := doc[m.scopesField]; ok {
		id.Scopes = extractScopes(v)
	}
	return id, nil
}
