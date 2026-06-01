package cosmoguard

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// jwtMethod validates a signed JWT carried in a configured header
// (default Authorization, "Bearer <token>" prefix accepted).
//
// Supports three signing-key families:
//   - HMAC (HS256/HS384/HS512) with a shared `secret:`. The BFF pattern
//     where a small backend mints short-lived tokens with a key cosmoguard
//     also holds. Zero per-request network calls.
//   - Public-key (RS256/RS384/RS512, ES256/ES384/ES512, EdDSA) with a
//     PEM-encoded `publicKeyFile:`. Works with any OIDC-shaped IdP
//     where you can download the verification key once.
//   - JWKS-URL: auto-fetched + auto-refreshed JSON Web Key Set. The
//     standard OIDC flow — point cosmoguard at the IdP's
//     /.well-known/jwks.json and key rotation Just Works. Token's `kid`
//     header is looked up in the live JWKS to pick the verification
//     key. Auth0, Clerk, Cognito, Keycloak, Okta, Firebase Auth, etc.
type jwtMethod struct {
	header        string
	keyFunc       jwt.Keyfunc
	validMethods  []string
	identityClaim string
	scopesClaim   string
	audience      string
	issuer        string
	// replay, when non-nil, is consulted on every successfully-verified
	// JWT carrying a `jti` claim. A repeat jti within the token's TTL
	// is rejected.
	replay ReplayStore
	// jwksCancel ends the keyfunc auto-refresh goroutine. nil for
	// non-JWKS modes. Called from Authenticator.Close on shutdown so
	// the goroutine doesn't outlive the Authenticator.
	jwksCancel context.CancelFunc
}

// Close releases the JWKS refresh goroutine (no-op for static-key
// modes). Called via Authenticator.Close.
func (m *jwtMethod) Close() {
	if m.jwksCancel != nil {
		m.jwksCancel()
		m.jwksCancel = nil
	}
}

func buildJWTMethod(cfg AuthMethodConfig, a *Authenticator) (*jwtMethod, error) {
	hdr := cfg.Header
	if hdr == "" {
		hdr = "Authorization"
	}
	idClaim := cfg.IdentityClaim
	if idClaim == "" {
		idClaim = "sub"
	}
	scopesClaim := cfg.ScopesClaim
	if scopesClaim == "" {
		scopesClaim = "scope"
	}

	var (
		keyFunc      jwt.Keyfunc
		validMethods []string
		jwksCancel   context.CancelFunc
		err          error
	)
	if cfg.JwksURL != "" {
		// JWKS mode: keyfunc handles the fetch, refresh, and per-token
		// kid lookup. Don't pin a single algorithm — accept any alg the
		// JWKS itself advertises (the keyfunc enforces "kid+alg pair
		// must exist in the JWKS"). Operators who want to restrict to a
		// specific algorithm can set Algorithm; if set, only that
		// algorithm passes jwt.Parse's WithValidMethods.
		keyFunc, jwksCancel, err = buildJWKSKeyFunc(cfg.JwksURL, cfg.JwksRefresh)
		if err != nil {
			return nil, err
		}
		if cfg.Algorithm != "" {
			validMethods = []string{cfg.Algorithm}
		} else {
			// Common OIDC IdPs use RS256 + ES256. Allowlisting both
			// (and the other size variants) covers ~all real deployments
			// while still constraining what an attacker can swap into
			// the alg header.
			validMethods = []string{
				"RS256", "RS384", "RS512",
				"PS256", "PS384", "PS512",
				"ES256", "ES384", "ES512",
				"EdDSA",
			}
		}
	} else {
		alg := cfg.Algorithm
		if alg == "" {
			alg = "HS256"
		}
		keyFunc, validMethods, err = buildJWTKeyFunc(alg, cfg.Secret, cfg.PublicKeyFile)
		if err != nil {
			return nil, err
		}
	}

	m := &jwtMethod{
		header:        hdr,
		keyFunc:       keyFunc,
		validMethods:  validMethods,
		identityClaim: idClaim,
		scopesClaim:   scopesClaim,
		audience:      cfg.Audience,
		issuer:        cfg.Issuer,
		jwksCancel:    jwksCancel,
	}
	if a != nil {
		m.replay = a.Replay()
	}
	return m, nil
}

// buildJWKSKeyFunc constructs a keyfunc that fetches the JWKS at url,
// refreshes it on the configured interval, and resolves token.kid →
// key at parse time. Returns the keyfunc + a cancel func tied to the
// background refresh goroutine — the cancel must be invoked on auth
// teardown so the goroutine doesn't outlive the Authenticator (e.g.
// across a hot-reload that rebuilds auth).
//
// The initial fetch is bounded by jwksInitialFetchTimeout so a
// transiently-unreachable IdP doesn't hang cosmoguard's startup
// indefinitely. keyfunc.NewDefaultOverrideCtx blocks until the first
// fetch completes (or fails), and its ctx parameter governs only the
// refresh loop — there's no library-level knob for the initial fetch.
// We honour the timeout by running the call in a goroutine and racing
// it against time.After; on timeout we cancel the refresh ctx (which
// also signals any in-flight HTTP request the library may have spun up)
// and return an error, so the operator sees a clear startup failure
// instead of an indefinite hang.
func buildJWKSKeyFunc(url string, refresh time.Duration) (jwt.Keyfunc, context.CancelFunc, error) {
	if refresh <= 0 {
		refresh = time.Hour
	}
	refreshCtx, cancelRefresh := context.WithCancel(context.Background())

	type result struct {
		k   keyfunc.Keyfunc
		err error
	}
	done := make(chan result, 1)
	go func() {
		k, err := keyfunc.NewDefaultOverrideCtx(refreshCtx, []string{url}, keyfunc.Override{
			RefreshInterval: refresh,
		})
		done <- result{k, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			cancelRefresh()
			return nil, nil, fmt.Errorf("jwt method: load JWKS from %s: %w", url, r.err)
		}
		return r.k.Keyfunc, cancelRefresh, nil
	case <-time.After(jwksInitialFetchTimeout):
		cancelRefresh()
		return nil, nil, fmt.Errorf("jwt method: load JWKS from %s: initial fetch timed out after %s", url, jwksInitialFetchTimeout)
	}
}

// jwksInitialFetchTimeout caps how long the JWKS initial fetch is
// allowed to block startup. 10s matches the v4 HTTP server's
// ReadHeaderTimeout — a remote OIDC IdP that can't answer within that
// is unlikely to recover quickly, so failing startup is the right
// call for the operator to notice.
const jwksInitialFetchTimeout = 10 * time.Second

// buildJWTKeyFunc inspects the chosen algorithm + key material and
// returns a keyfunc plus the list of valid signing methods to pass to
// jwt.Parse (defense in depth — even if an attacker swaps `alg` in the
// header, the parser refuses to use a method not on this list).
func buildJWTKeyFunc(alg, secret, publicKeyFile string) (jwt.Keyfunc, []string, error) {
	// Reject configs that supply key material for both HMAC (secret)
	// AND asymmetric (publicKeyFile) — the active algorithm picks one
	// silently, so an operator who pasted both gets the one their
	// `alg` happens to use, no warning. Failing startup makes the
	// mistake obvious.
	if secret != "" && publicKeyFile != "" {
		return nil, nil, errors.New("jwt method: set either `secret` (HMAC) or `publicKeyFile` (asymmetric), not both")
	}
	switch alg {
	case "HS256", "HS384", "HS512":
		if secret == "" {
			return nil, nil, errors.New("jwt method: HMAC algorithm requires `secret`")
		}
		key := []byte(secret)
		return func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
			}
			return key, nil
		}, []string{alg}, nil

	case "RS256", "RS384", "RS512", "PS256", "PS384", "PS512":
		key, err := loadPublicKey(publicKeyFile, "RSA")
		if err != nil {
			return nil, nil, err
		}
		rsaKey, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, nil, fmt.Errorf("jwt method: %s requires an RSA public key", alg)
		}
		return func(t *jwt.Token) (any, error) {
			switch t.Method.(type) {
			case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
				// OK
			default:
				return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
			}
			return rsaKey, nil
		}, []string{alg}, nil

	case "ES256", "ES384", "ES512":
		key, err := loadPublicKey(publicKeyFile, "EC")
		if err != nil {
			return nil, nil, err
		}
		ecKey, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return nil, nil, fmt.Errorf("jwt method: %s requires an ECDSA public key", alg)
		}
		return func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodECDSA); !ok {
				return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
			}
			return ecKey, nil
		}, []string{alg}, nil

	case "EdDSA":
		key, err := loadPublicKey(publicKeyFile, "Ed25519")
		if err != nil {
			return nil, nil, err
		}
		edKey, ok := key.(ed25519.PublicKey)
		if !ok {
			return nil, nil, errors.New("jwt method: EdDSA requires an Ed25519 public key")
		}
		return func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodEd25519); !ok {
				return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
			}
			return edKey, nil
		}, []string{alg}, nil

	default:
		return nil, nil, fmt.Errorf("jwt method: unsupported algorithm %q", alg)
	}
}

// loadPublicKey reads a PEM-encoded public key from disk and returns
// the parsed key. The expectedKind argument is used only for error
// messages; the actual type assertion happens in the caller.
func loadPublicKey(path, expectedKind string) (any, error) {
	if path == "" {
		return nil, fmt.Errorf("jwt method: %s algorithms require `publicKeyFile`", expectedKind)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("jwt method: read %s: %w", path, err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("jwt method: no PEM block in %s", path)
	}
	// PKIX is the standard "BEGIN PUBLIC KEY" envelope from OpenSSL and
	// most key-gen tools (covers RSA, EC, Ed25519).
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		// Fall back to PKCS1 for older "BEGIN RSA PUBLIC KEY" RSA keys.
		if rsaKey, rsaErr := x509.ParsePKCS1PublicKey(block.Bytes); rsaErr == nil {
			return rsaKey, nil
		}
		return nil, fmt.Errorf("jwt method: parse PEM in %s: %w", path, err)
	}
	return key, nil
}

func (m *jwtMethod) Resolve(r *http.Request) (*Identity, error) {
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

	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(m.validMethods),
		jwt.WithExpirationRequired(),
		// Clock-skew leeway: 30s tolerance on exp/nbf/iat. Without
		// this, a token minted ~milliseconds before the verifier's
		// clock (very common with multiple replicas + NTP drift) is
		// rejected for nbf, surfacing as a sporadic 401 storm at IdP
		// rollover time. 30s matches Auth0/Okta/Keycloak defaults.
		jwt.WithLeeway(30 * time.Second),
	}
	if m.audience != "" {
		parserOpts = append(parserOpts, jwt.WithAudience(m.audience))
	}
	if m.issuer != "" {
		parserOpts = append(parserOpts, jwt.WithIssuer(m.issuer))
	}

	token, err := jwt.Parse(raw, m.keyFunc, parserOpts...)
	if err != nil || !token.Valid {
		// A header was presented and parsed as a JWT shape but failed
		// verification (bad signature, expired, wrong aud/iss, missing
		// exp). Returning (nil, nil) here would let the next configured
		// auth method see the SAME header — a forged JWT could then be
		// accepted by a downstream api-key/external-validator that reads
		// the same Authorization header. Always short-circuit with
		// ErrInvalidCredential so the chain stops and the request is
		// denied at the resolve layer.
		//
		// Audit-log the failure so a forged / expired token isn't
		// invisible to operators. Warn level (not error) — bad
		// tokens at internet-facing endpoints are routine; we just
		// want them in the trail. We log a CATEGORY rather than the
		// raw error so claim values (which jwt/v5 can embed in
		// messages — e.g. expired-exp errors include the timestamp)
		// don't leak into log indices.
		slog.Warn("jwt auth failed",
			"reason", classifyJWTError(err),
			"source", GetSourceIP(r),
			"header", m.header)
		return nil, ErrInvalidCredential
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, nil
	}

	name, _ := claims[m.identityClaim].(string)
	if name == "" {
		// No usable identity claim; treat as auth failure.
		return nil, nil
	}

	// Replay protection: when enabled AND the token carries a jti
	// claim, check the seen-set keyed on (issuer, jti). A repeat
	// within the token's TTL is rejected; a first-time jti is stored
	// for the remaining TTL.
	//
	// Tokens without a jti are NOT enforced — replay protection
	// requires the IdP to mint unique identifiers. Falling back to
	// fail-open here avoids silently breaking deployments whose IdP
	// doesn't emit jti.
	//
	// JSON allows jti as any JSON-primitive; jwt/v5 decodes claims
	// into jwt.MapClaims which preserves the original type. We
	// coerce non-string forms via fmt.Sprintf so an attacker can't
	// bypass replay protection by sending `{"jti": 1}` and getting
	// silently skipped by a strict string type-assert.
	if m.replay != nil {
		if jti := extractJTI(claims["jti"]); jti != "" {
			ttl := jwtRemainingTTL(claims)
			if ttl > 0 {
				// Namespace the replay key on the token's own iss
				// claim, not the configured m.issuer. When operators
				// leave issuer unset (m.issuer == ""), the previous
				// shape collapsed every token into the bare
				// "|<jti>" namespace — so a jti minted by IdP A
				// collided with the same jti from IdP B, and either
				// a stolen jti from one issuer or sloppy reuse from
				// another caused false-positive replay rejections.
				// When jwt.WithIssuer IS set, jwt.Parse has already
				// enforced iss == m.issuer above, so this is
				// equivalent in that path.
				iss, _ := claims["iss"].(string)
				key := iss + "|" + jti
				seen, err := m.replay.SeenOrStore(r.Context(), key, ttl)
				if err != nil {
					// Replay-store error: do NOT silently strip the
					// identity (that's an auth bypass during a Redis
					// outage). Fall through with the verified
					// identity AND log loudly so operators see the
					// dependency failing in their audit pipeline.
					// Operators who prefer fail-closed should fail
					// the replay store loudly; redis-sentinel
					// covers planned outages.
					slog.Warn("replay store error; admitting verified JWT without replay check",
						"error", err.Error(), "issuer", iss)
				} else if seen {
					return nil, ErrReplay
				}
			}
		}
	}

	return &Identity{
		Name:   name,
		Scopes: extractScopes(claims[m.scopesClaim]),
		Method: "jwt",
	}, nil
}

// extractJTI normalizes a JWT `jti` claim value into a non-empty
// string suitable for the replay-store key. JSON permits jti to be
// any primitive (string / number / bool) — RFC 7519 §4.1.7 says it
// MUST be a case-sensitive string, but IdPs in the wild emit numeric
// values. A strict `.(string)` assert on those silently bypasses
// replay protection, so we accept whatever protobuf type the JSON
// decoder produced and stringify it with %v. Returns "" only for
// nil — empty jti is treated as "no jti".
func extractJTI(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// jwtRemainingTTL computes time-until-expiry from the `exp` claim.
// Returns 0 when exp is missing or in the past. Used as the TTL for
// the replay-store entry so we don't hold seen-set keys longer than
// the token would be valid anyway.
func jwtRemainingTTL(claims jwt.MapClaims) time.Duration {
	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil {
		return 0
	}
	d := time.Until(exp.Time)
	if d <= 0 {
		return 0
	}
	return d
}

// HeadersToStrip removes the JWT-carrying header before forwarding to
// upstream. Same reasoning as api-key: Cosmos nodes never expect to see
// cosmoguard auth headers.
func (m *jwtMethod) HeadersToStrip() []string {
	return []string{m.header}
}

// classifyJWTError maps a jwt/v5 verification error to a short
// category string suitable for logs / metrics labels. We deliberately
// don't echo err.Error() because jwt/v5 includes claim values in some
// error messages (e.g. expired-exp includes the timestamp), and we'd
// rather not let an attacker influence log content via a crafted
// claim. Categories are stable strings — operators can alert on
// them.
func classifyJWTError(err error) string {
	if err == nil {
		return "invalid"
	}
	switch {
	case errors.Is(err, jwt.ErrTokenMalformed):
		return "malformed"
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return "signature_invalid"
	case errors.Is(err, jwt.ErrTokenExpired):
		return "expired"
	case errors.Is(err, jwt.ErrTokenNotValidYet):
		return "not_yet_valid"
	case errors.Is(err, jwt.ErrTokenRequiredClaimMissing):
		return "claim_missing"
	case errors.Is(err, jwt.ErrTokenInvalidAudience):
		return "audience_invalid"
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return "issuer_invalid"
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		return "unverifiable"
	default:
		return "invalid"
	}
}

// extractScopes turns a claim value into a []string. Accepts either a
// space-separated string ("read write") — the OAuth 2.0 standard — or a
// JSON array. Anything else returns nil.
func extractScopes(v any) []string {
	switch s := v.(type) {
	case string:
		if s == "" {
			return nil
		}
		return strings.Fields(s)
	case []any:
		out := make([]string, 0, len(s))
		for _, x := range s {
			if str, ok := x.(string); ok && str != "" {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}
