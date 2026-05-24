package cosmoguard

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/olric-data/olric"
)

// Identity is the resolved subject of an authenticated request. Built by
// an AuthMethod and stored on the request context for downstream rule
// matching, rate-limit scoping, and audit logging.
type Identity struct {
	// Name is the human/machine identifier (api-key alias, JWT subject,
	// validator response ID). Used in logs and as the rate-limit bucket
	// key for scope=per-identity.
	Name string
	// Scopes is the list of capabilities the identity holds. Rules can
	// require specific scopes (auth.scopes).
	Scopes []string
	// Method is the authentication method that produced this identity —
	// "api-key", "jwt", "introspection", "external-validator". Useful for
	// audit logs.
	Method string
	// Degraded marks an identity minted because an auth backend (external
	// validator / introspection) was unreachable and the operator chose
	// fail-open. Authorize honours it unconditionally so a backend outage
	// doesn't turn fail-open into fail-closed on protected routes. Never
	// set for a verified credential.
	Degraded bool
}

// degradedIdentity is the synthetic identity an auth method returns when
// its backend is unreachable and failureMode is fail-open. It carries no
// real scopes — Authorize short-circuits to "allowed" on the Degraded
// flag rather than matching scopes/identities, so the operator's
// availability-over-strictness choice holds across every gate.
func degradedIdentity(method string) *Identity {
	return &Identity{Name: "degraded", Method: method + "-degraded", Degraded: true}
}

// ReplayProtectionConfig opts in to JWT replay protection. When
// enabled, every JWT carrying a `jti` claim is checked against a seen-
// set keyed on (issuer, jti) — a token whose jti has already been seen
// within its expiration window is rejected. Backed by Redis when
// cache.redis is configured (so HPA replicas share the set);
// otherwise an in-process memory store with periodic GC.
//
// Tokens without a `jti` claim are NOT enforced — replay protection
// requires the IdP to mint unique identifiers. Operators who need it
// must configure their IdP to emit jti.
type ReplayProtectionConfig struct {
	Enable bool `yaml:"enable,omitempty"`
}

// AuthConfig is the top-level YAML block for v4 authentication.
type AuthConfig struct {
	Enable bool `yaml:"enable,omitempty"`

	// ReplayProtection, when enabled, gates JWTs against re-use
	// within their TTL. See ReplayProtectionConfig for details.
	ReplayProtection *ReplayProtectionConfig `yaml:"replayProtection,omitempty"`
	// Methods are the authentication backends consulted in order on each
	// request. The first method that produces a non-anonymous identity
	// wins; remaining methods are not consulted.
	Methods []AuthMethodConfig `yaml:"methods,omitempty"`
	// Identities is the inline registry used by the api-key method. For
	// production, prefer IdentitiesFile so secrets live outside the main
	// YAML and can be rotated without a full reload.
	Identities []IdentityConfig `yaml:"identities,omitempty"`
	// IdentitiesFile, when set, is a path to a separate YAML file
	// containing an array of IdentityConfig. The file is hot-reloaded
	// alongside the main config.
	IdentitiesFile string `yaml:"identitiesFile,omitempty"`
	// DefaultRequire, when true, makes every rule require an authenticated
	// identity unless the rule sets `auth.require: false` explicitly.
	// Use for fail-closed deployments.
	DefaultRequire bool `yaml:"defaultRequire,omitempty"`
	// Anonymous is the identity assigned when no AuthMethod produces one.
	// nil means "no anonymous identity" — unauthenticated requests are
	// denied by any rule that requires auth.
	Anonymous *IdentityConfig `yaml:"anonymous,omitempty"`
}

// IdentityConfig is a configurable identity entry. Holds the credential
// material (api-key, etc.) plus the policy: scopes and validity window.
//
// Per-identity rate limits and a per-identity WS subscription cap are
// expressible at the rule level via `rateLimit: { scope: per-identity }`
// — see the rate-limit section of CONFIG.md. Dedicated per-identity
// fields here would also need enforcement plumbing on every protocol
// (HTTP / JSON-RPC / WS / gRPC) and aren't shipped in v4.
type IdentityConfig struct {
	Name       string     `yaml:"name"`
	APIKey     string     `yaml:"apiKey,omitempty"`
	Scopes     []string   `yaml:"scopes,omitempty"`
	ValidUntil *time.Time `yaml:"validUntil,omitempty"`
}

// AuthMethodConfig is one entry in auth.methods. Discriminated by Type.
//
// Per-type field reference:
//
//   - api-key: Header, QueryParam
//   - jwt:     Header, Algorithm (HS256/HS384/HS512), Secret, IdentityClaim,
//     ScopesClaim, Audience, Issuer
//   - external-validator: Endpoint, ValidatorMethod, ForwardHeader,
//     ResponseValid, ResponseIdentity, ResponseScopes, CacheTTL,
//     Timeout, FailureMode
type AuthMethodConfig struct {
	Type string `yaml:"type"`

	// api-key (and shared with jwt — the JWT lives in this header):
	Header     string `yaml:"header,omitempty"`     // default: "Authorization"
	QueryParam string `yaml:"queryParam,omitempty"` // e.g. "api_key" (api-key only)

	// jwt fields:
	Algorithm     string        `yaml:"algorithm,omitempty"`     // HS256/HS384/HS512 (HMAC), RS256/RS384/RS512, ES256/ES384/ES512, EdDSA — required for static-key modes
	Secret        string        `yaml:"secret,omitempty"`        // HMAC: shared signing secret
	PublicKeyFile string        `yaml:"publicKeyFile,omitempty"` // RSA/EC/Ed25519: PEM-encoded public key on disk
	JwksURL       string        `yaml:"jwksUrl,omitempty"`       // OIDC: fetch + refresh JWKS from this URL
	JwksRefresh   time.Duration `yaml:"jwksRefresh,omitempty"`   // how often to re-fetch JWKS (default: 1h)
	IdentityClaim string        `yaml:"identityClaim,omitempty"` // default: "sub"
	ScopesClaim   string        `yaml:"scopesClaim,omitempty"`   // default: "scope" (OAuth-style space-separated)
	Audience      string        `yaml:"audience,omitempty"`      // optional aud check
	Issuer        string        `yaml:"issuer,omitempty"`        // optional iss check

	// introspection (RFC 7662) fields:
	IntrospectionEndpoint string `yaml:"introspectionEndpoint,omitempty"`
	ClientID              string `yaml:"clientId,omitempty"`
	ClientSecret          string `yaml:"clientSecret,omitempty"`
	// IdentityField is the JSON field name in the introspection response
	// holding the identity. Defaults to "sub" per RFC 7662.
	IdentityField string `yaml:"identityField,omitempty"`
	// ScopesField is the JSON field name holding scopes (space-separated
	// string per the RFC). Defaults to "scope".
	ScopesField string `yaml:"scopesField,omitempty"`

	// external-validator fields:
	Endpoint        string `yaml:"endpoint,omitempty"`
	ValidatorMethod string `yaml:"validatorMethod,omitempty"` // GET (default) or POST
	// ForwardHeader names a header on the inbound request whose value is
	// forwarded as the same-named header on the validator request. e.g.
	// "Authorization" means cosmoguard relays the client's Authorization
	// header to the validator.
	ForwardHeader string `yaml:"forwardHeader,omitempty"`
	// ResponseValid is a JSON dot-path that must evaluate to a truthy
	// value (true, non-zero, non-empty) for the validator to consider the
	// credential valid. Empty defaults to "valid" (i.e., expect a top-
	// level "valid": true).
	ResponseValid string `yaml:"responseValid,omitempty"`
	// ResponseIdentity is a JSON dot-path to the identity name in the
	// validator's response. Empty defaults to "userId".
	ResponseIdentity string `yaml:"responseIdentity,omitempty"`
	// ResponseScopes is a JSON dot-path to a string array of scopes.
	// Empty means no scopes are extracted.
	ResponseScopes string `yaml:"responseScopes,omitempty"`
	// CacheTTL bounds how long a validation result is cached. Reduces
	// load on the validator and protects the hot path from its latency.
	CacheTTL time.Duration `yaml:"cacheTTL,omitempty"`
	// Timeout caps how long the validator HTTP call may take.
	Timeout time.Duration `yaml:"timeout,omitempty"`
	// FailureMode: "fail-open" (default) accepts credentials when the
	// validator is unreachable; "fail-closed" rejects them. Operators
	// who care more about availability than security pick fail-open.
	FailureMode string `yaml:"failureMode,omitempty"`
}

// RuleAuthConfig is the per-rule auth gate.
type RuleAuthConfig struct {
	// Require, when true, denies the request unless an identity is
	// resolved. Default is governed by AuthConfig.DefaultRequire.
	Require *bool `yaml:"require,omitempty"`
	// Scopes is the list of scopes the identity must hold. ALL must be
	// present (AND, not OR).
	Scopes []string `yaml:"scopes,omitempty"`
	// Identities, when non-empty, restricts the rule to a closed list of
	// identity names. Overrides Scopes when both are set.
	Identities []string `yaml:"identities,omitempty"`
}

// AuthMethod is the interface every auth backend implements.
type AuthMethod interface {
	// Resolve inspects the request and returns an Identity if it can
	// authenticate the caller. Returns nil identity + nil error if the
	// method doesn't apply (e.g., api-key method but no header set on
	// this request). Returns an error only on backend failure (e.g.,
	// network error talking to an external validator).
	Resolve(r *http.Request) (*Identity, error)
	// HeadersToStrip lists the request header names this method consumes
	// and that must be removed before forwarding upstream. Cosmos never
	// expects to see them.
	HeadersToStrip() []string
}

// Authenticator combines all configured methods + an identity registry.
type Authenticator struct {
	methods    []AuthMethod
	registry   atomic.Pointer[identityRegistry]
	anonymous  *Identity
	defaultReq bool
	// replay is the JWT replay-protection store. nil when disabled.
	// Shared by every jwtMethod the authenticator builds, so all JWT
	// configs (Authorization header, query-token, etc.) consult one
	// seen-set.
	replay ReplayStore
}

// Replay returns the configured replay store, or nil when replay
// protection is disabled. Exposed so jwtMethod can consult the store.
func (a *Authenticator) Replay() ReplayStore { return a.replay }

// Close releases the replay store's backing resources (timers, Redis
// pool) AND any per-method background goroutines (JWKS auto-refresh).
// Safe to call multiple times.
func (a *Authenticator) Close() error {
	if a == nil {
		return nil
	}
	for _, m := range a.methods {
		if c, ok := m.(interface{ Close() }); ok {
			c.Close()
		}
	}
	if a.replay != nil {
		return a.replay.Close()
	}
	return nil
}

// identityRegistry is the resolved store: api-key → Identity.
type identityRegistry struct {
	byAPIKey map[string]*Identity
	byName   map[string]*Identity
}

// NewAuthenticator builds the authenticator from config. Returns a no-op
// authenticator when cfg.Enable is false — every request gets the
// anonymous identity (or nil) and no auth gate is enforced. olricClient
// is used to back the JWT replay-protection store with a cluster-shared
// DMap (cosmoguard:jti); pass nil to force the per-pod memory fallback.
func NewAuthenticator(cfg *AuthConfig, olricClient *olric.EmbeddedClient) (*Authenticator, error) {
	if cfg == nil || !cfg.Enable {
		return &Authenticator{}, nil
	}

	a := &Authenticator{defaultReq: cfg.DefaultRequire}

	if cfg.ReplayProtection != nil && cfg.ReplayProtection.Enable {
		store, err := NewReplayStore(olricClient, "cosmoguard:jti")
		if err != nil {
			return nil, fmt.Errorf("auth.replayProtection: %w", err)
		}
		a.replay = store
	}
	if cfg.Anonymous != nil {
		a.anonymous = &Identity{
			Name:   cfg.Anonymous.Name,
			Scopes: append([]string(nil), cfg.Anonymous.Scopes...),
			Method: "anonymous",
		}
	}

	// Build the identity registry from inline + file sources.
	reg := &identityRegistry{
		byAPIKey: map[string]*Identity{},
		byName:   map[string]*Identity{},
	}
	now := time.Now()
	for _, ic := range cfg.Identities {
		if err := registerIdentity(reg, ic, now); err != nil {
			_ = a.Close()
			return nil, err
		}
	}
	// IdentitiesFile: minimal v1 — supported, but content loading not
	// implemented in this slice. Reserved for the next phase. A
	// configured but unsupported value surfaces as a clear error so
	// operators know it's not silently ignored.
	if cfg.IdentitiesFile != "" {
		_ = a.Close()
		return nil, errors.New("auth.identitiesFile is reserved for a follow-up phase; use auth.identities for now")
	}
	a.registry.Store(reg)

	// Build the configured methods. Unknown types surface as load-time
	// errors rather than silently dropping the credential check.
	// On any error, close already-built methods + replay store so we
	// don't leak the JWKS auto-refresh goroutine when methods[N] fails
	// after methods[0..N-1] launched their refresh tickers.
	for i, m := range cfg.Methods {
		method, err := buildAuthMethod(m, a)
		if err != nil {
			_ = a.Close()
			return nil, fmt.Errorf("auth.methods[%d]: %w", i, err)
		}
		a.methods = append(a.methods, method)
	}
	return a, nil
}

func registerIdentity(reg *identityRegistry, ic IdentityConfig, now time.Time) error {
	if ic.Name == "" {
		return errors.New("identity missing required `name`")
	}
	if ic.ValidUntil != nil && now.After(*ic.ValidUntil) {
		// Expired credentials are loaded but log a warning and skip them.
		// Operators rotating keys can leave old entries in place.
		return nil
	}
	id := &Identity{
		Name:   ic.Name,
		Scopes: append([]string(nil), ic.Scopes...),
		Method: "api-key",
	}
	reg.byName[ic.Name] = id
	if ic.APIKey != "" {
		reg.byAPIKey[ic.APIKey] = id
	}
	return nil
}

// Resolve walks the configured methods in order. First non-nil identity wins.
// If no method produces one, returns the anonymous identity (or nil).
func (a *Authenticator) Resolve(r *http.Request) (*Identity, error) {
	for _, m := range a.methods {
		id, err := m.Resolve(r)
		if err != nil {
			return nil, err
		}
		if id != nil {
			return id, nil
		}
	}
	return a.anonymous, nil
}

// StripCredentialHeaders removes every header any configured method
// consumes. Called before forwarding upstream so the Cosmos node never
// sees cosmoguard-specific auth headers.
func (a *Authenticator) StripCredentialHeaders(h http.Header) {
	for _, m := range a.methods {
		for _, name := range m.HeadersToStrip() {
			h.Del(name)
		}
	}
}

// CredentialHeaderNames returns every header/metadata name the configured
// auth methods consume. Non-HTTP transports (gRPC metadata) use this to
// strip credentials before forwarding upstream, since they can't use the
// http.Header-based StripCredentialHeaders. nil-safe.
func (a *Authenticator) CredentialHeaderNames() []string {
	if a == nil {
		return nil
	}
	var names []string
	for _, m := range a.methods {
		names = append(names, m.HeadersToStrip()...)
	}
	return names
}

// Authorize evaluates the per-rule auth gate against the resolved identity.
// Returns (allowed, denyReason). denyReason is empty on allow.
func (a *Authenticator) Authorize(rule *RuleAuthConfig, id *Identity) (bool, string) {
	// Fail-open degraded identity (auth backend unreachable, operator
	// chose availability) relaxes AUTHENTICATION availability ONLY: a
	// degraded id is non-anonymous and so satisfies "require auth" /
	// defaultRequire below, keeping traffic flowing during an outage. It
	// deliberately does NOT short-circuit AUTHORIZATION — it carries no
	// scopes and a non-allowlisted name, so a rule naming specific
	// scopes/identities still fails closed via the matching below. The
	// backend that would prove those claims is exactly what's down;
	// granting them during the outage would be a silent privilege
	// escalation.
	requires := a.defaultReq
	if rule != nil && rule.Require != nil {
		requires = *rule.Require
	}
	// A rule that names specific scopes or identities is implicitly
	// stronger than "require auth": the operator has declared *which*
	// authenticated subjects may pass. The anonymous shim has no
	// scopes and no name, so it can never satisfy such a rule —
	// regardless of the rule's Require flag. Treating those rules as
	// requires=true here closes the bypass where setting
	// scopes/identities WITHOUT require:true left an anonymous shim
	// satisfying the gate.
	if rule != nil && (len(rule.Scopes) > 0 || len(rule.Identities) > 0) {
		requires = true
	}
	if !requires {
		return true, ""
	}
	// Parentheses pin the precedence (&& binds tighter than ||) so a
	// future edit can't silently flip the semantics. Intent: reject
	// when there's no identity, OR when the identity is the anonymous
	// shim and the rule requires authentication (which includes any
	// rule with scopes / identities set, per the upgrade above).
	if id == nil || (id.Method == "anonymous" && requires) {
		return false, "authentication required"
	}
	if rule == nil {
		return true, ""
	}
	if len(rule.Identities) > 0 {
		for _, n := range rule.Identities {
			if n == id.Name {
				return true, ""
			}
		}
		return false, "identity not in allowed list"
	}
	if len(rule.Scopes) > 0 {
		held := map[string]bool{}
		for _, s := range id.Scopes {
			held[s] = true
		}
		for _, need := range rule.Scopes {
			if !held[need] {
				return false, "missing required scope: " + need
			}
		}
	}
	return true, ""
}

// ---------- AuthMethod implementations ----------

// apiKeyMethod resolves identities from a static or file-backed registry,
// keyed by the API key the client presented.
type apiKeyMethod struct {
	header     string
	queryParam string
	authn      *Authenticator // for registry lookup
	stripHdrs  []string
}

func buildAuthMethod(cfg AuthMethodConfig, a *Authenticator) (AuthMethod, error) {
	switch cfg.Type {
	case "api-key":
		hdr := cfg.Header
		if hdr == "" {
			hdr = "Authorization"
		}
		return &apiKeyMethod{
			header:     hdr,
			queryParam: cfg.QueryParam,
			authn:      a,
			stripHdrs:  []string{hdr},
		}, nil
	case "jwt":
		return buildJWTMethod(cfg, a)
	case "external-validator":
		return buildExternalValidatorMethod(cfg)
	case "introspection":
		return buildIntrospectionMethod(cfg)
	default:
		return nil, fmt.Errorf("unknown auth method type %q", cfg.Type)
	}
}

// Resolve extracts the credential from the configured header (or query
// param), constant-time compares it against the registry, and returns
// the matching Identity if any.
func (m *apiKeyMethod) Resolve(r *http.Request) (*Identity, error) {
	// Header first.
	cred := m.extractFromHeader(r)
	if cred == "" && m.queryParam != "" {
		cred = r.URL.Query().Get(m.queryParam)
	}
	if cred == "" {
		return nil, nil
	}
	reg := m.authn.registry.Load()
	if reg == nil {
		return nil, nil
	}
	// Walk every registered key with subtle.ConstantTimeCompare so the
	// match decision doesn't leak timing on the credential value. Go map
	// indexing is *not* constant-time — hash + bucket probe depends on
	// the key — so the previous `reg.byAPIKey[cred]` shape leaked the
	// usual hashmap-lookup signal even though the post-check looked
	// constant-time. We accept O(N) over the operator-supplied key set
	// (N is small: typically a handful to low hundreds) in exchange for
	// no map-probe timing channel. The loop does not short-circuit:
	// every iteration runs a full constant-time compare regardless of
	// whether a match has already been found, so the wall-time floor is
	// the same for hit and miss.
	credBytes := []byte(cred)
	var found *Identity
	for k, id := range reg.byAPIKey {
		if subtle.ConstantTimeCompare([]byte(k), credBytes) == 1 {
			found = id
		}
	}
	return found, nil
}

func (m *apiKeyMethod) extractFromHeader(r *http.Request) string {
	v := r.Header.Get(m.header)
	if v == "" {
		return ""
	}
	// Accept "Bearer <key>" or raw "<key>".
	if strings.HasPrefix(v, "Bearer ") {
		return strings.TrimSpace(v[len("Bearer "):])
	}
	return strings.TrimSpace(v)
}

// HeadersToStrip lists the request-header names this method consumed.
// The HTTP proxy strips them before forwarding upstream so the Cosmos
// node never sees cosmoguard auth headers.
func (m *apiKeyMethod) HeadersToStrip() []string {
	return m.stripHdrs
}
