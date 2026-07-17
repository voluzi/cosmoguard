package testharness_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestI_JWT_ValidToken: a request bearing a valid HMAC-signed JWT
// (issued in-test) is accepted; the upstream call sees the request with
// the Authorization header stripped.
func TestI_JWT_ValidToken(t *testing.T) {
	const secret = "test-jwt-secret"

	cfg := authJWTConfig(secret)
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{"v":1}`),
	)

	tok := mintJWT(t, secret, jwt.MapClaims{
		"sub":   "test-user",
		"scope": "read write",
		"exp":   time.Now().Add(5 * time.Minute).Unix(),
	})

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 1)
	assert.Equal(t, calls[0].Headers.Get("Authorization"), "",
		"JWT-carrying header must be stripped before upstream")
}

// TestI_JWT_ExpiredToken: an expired JWT is rejected.
func TestI_JWT_ExpiredToken(t *testing.T) {
	const secret = "test-jwt-secret"

	cfg := authJWTConfig(secret)
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	tok := mintJWT(t, secret, jwt.MapClaims{
		"sub": "test-user",
		"exp": time.Now().Add(-1 * time.Minute).Unix(), // expired
	})
	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/private"), 0)
}

// TestI_JWT_BadSignature: a JWT signed with the wrong secret is rejected.
func TestI_JWT_BadSignature(t *testing.T) {
	cfg := authJWTConfig("real-secret")
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	tok := mintJWT(t, "wrong-secret", jwt.MapClaims{
		"sub": "test-user",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
}

// TestI_ExternalValidator_Allow: cosmoguard forwards the credential to
// a fake validator that returns "valid": true. The credential reaches
// the validator but is stripped from the upstream-bound request.
func TestI_ExternalValidator_Allow(t *testing.T) {
	var validatorHits atomic.Int64
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validatorHits.Add(1)
		if r.Header.Get("Authorization") != "Bearer good-token" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"valid":false}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true,"userId":"alice","scopes":["read","write"]}`))
	}))
	defer validator.Close()

	cfg := authExternalConfig(validator.URL)
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer good-token")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.Equal(t, validatorHits.Load(), int64(1), "validator should have been called once")

	// Second identical request: cached, validator NOT called again.
	resp2 := h.Do(t, req)
	assert.Equal(t, resp2.StatusCode, http.StatusOK)
	assert.Equal(t, validatorHits.Load(), int64(1), "cached validation should skip the validator")

	// Upstream sees neither call carrying Authorization.
	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 2)
	for i, c := range calls {
		assert.Equal(t, c.Headers.Get("Authorization"), "",
			"call %d: Authorization must be stripped before upstream", i)
	}
}

// TestI_ExternalValidator_Reject: validator returns valid:false → 401,
// upstream never called.
func TestI_ExternalValidator_Reject(t *testing.T) {
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":false}`))
	}))
	defer validator.Close()

	cfg := authExternalConfig(validator.URL)
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusUnauthorized)
	assert.Equal(t, h.Upstream.LCD.CallCount(http.MethodGet, "/private"), 0)
}

// ---------- helpers ----------

// TestI_JWT_RSA: a JWT signed with RS256 verifies cleanly against the
// matching PEM public key on disk. The "OIDC-shaped IdP" pattern —
// Auth0, Clerk, Cognito, Keycloak, etc. — where the IdP holds the
// private key and operators pre-download the public key.
func TestI_JWT_RSA(t *testing.T) {
	// Generate an RSA keypair in-test.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NilError(t, err)

	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	assert.NilError(t, err)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	pubFile := filepath.Join(t.TempDir(), "jwt.pub.pem")
	assert.NilError(t, os.WriteFile(pubFile, pubPEM, 0o600))

	cfg := authBaseConfig()
	cfg.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{
				Type:          "jwt",
				Algorithm:     "RS256",
				PublicKeyFile: pubFile,
				IdentityClaim: "sub",
			},
		},
	}
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "test-user",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	signed, err := tok.SignedString(priv)
	assert.NilError(t, err)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	// Wrong key: a token signed with a different keypair MUST be rejected.
	otherPriv, _ := rsa.GenerateKey(rand.Reader, 2048)
	badTok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "attacker",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	badSigned, _ := badTok.SignedString(otherPriv)

	badReq, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	badReq.Header.Set("Authorization", "Bearer "+badSigned)
	badResp := h.Do(t, badReq)
	assert.Equal(t, badResp.StatusCode, http.StatusUnauthorized)
}

// TestI_JWT_JWKS: cosmoguard fetches a JWKS from a fake OIDC IdP on
// startup, looks up the signing key by token.kid, and verifies. Token
// signed with a key not present in the JWKS is rejected.
func TestI_JWT_JWKS(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NilError(t, err)

	// Build a JWKS document containing the public half of `priv`.
	// JWKS RSA: { "kty":"RSA", "alg":"RS256", "kid":"<id>",
	//             "n": base64url(modulus), "e": base64url(exponent) }
	kid := "test-key-1"
	jwks := buildSingleKeyJWKS(t, &priv.PublicKey, kid, "RS256")

	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks)
	}))
	defer jwksServer.Close()

	cfg := authBaseConfig()
	cfg.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{
				Type:          "jwt",
				JwksURL:       jwksServer.URL,
				IdentityClaim: "sub",
			},
		},
	}
	cfg.LCD.Rules = []*cosmoguard.HttpRule{{
		Priority: 100,
		Action:   cosmoguard.RuleActionAllow,
		Paths:    []string{"/private"},
		Methods:  []string{http.MethodGet},
		Auth:     &cosmoguard.RuleAuthConfig{Require: boolPtr(true)},
	}}

	h := testharness.New(t,
		testharness.WithConfig(cfg),
		testharness.WithLCDResponse(http.MethodGet, "/private", `{}`),
	)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "alice",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	assert.NilError(t, err)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	resp := h.Do(t, req)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	// A token signed with a different (unpublished) key MUST be rejected.
	otherPriv, _ := rsa.GenerateKey(rand.Reader, 2048)
	badTok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "attacker",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	badTok.Header["kid"] = kid
	badSigned, _ := badTok.SignedString(otherPriv)

	badReq, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/private", nil)
	badReq.Header.Set("Authorization", "Bearer "+badSigned)
	badResp := h.Do(t, badReq)
	assert.Equal(t, badResp.StatusCode, http.StatusUnauthorized)
}

// buildSingleKeyJWKS renders a JWKS document containing exactly one RSA
// public key — what real OIDC providers serve from /.well-known/jwks.json.
func buildSingleKeyJWKS(t *testing.T, pub *rsa.PublicKey, kid, alg string) []byte {
	t.Helper()
	// Exponent serialization: smallest big-endian byte sequence.
	expBytes := []byte{byte(pub.E >> 24), byte(pub.E >> 16), byte(pub.E >> 8), byte(pub.E)}
	for len(expBytes) > 1 && expBytes[0] == 0 {
		expBytes = expBytes[1:]
	}
	doc := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "RSA",
				"alg": alg,
				"use": "sig",
				"kid": kid,
				"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(expBytes),
			},
		},
	}
	b, err := json.Marshal(doc)
	assert.NilError(t, err)
	return b
}

func mintJWT(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	assert.NilError(t, err)
	return s
}

func authJWTConfig(secret string) *cosmoguard.Config {
	c := authBaseConfig()
	c.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{
				Type:          "jwt",
				Header:        "Authorization",
				Algorithm:     "HS256",
				Secret:        secret,
				IdentityClaim: "sub",
				ScopesClaim:   "scope",
			},
		},
	}
	return c
}

func authExternalConfig(endpoint string) *cosmoguard.Config {
	c := authBaseConfig()
	c.Auth = cosmoguard.AuthConfig{
		Enable: true,
		Methods: []cosmoguard.AuthMethodConfig{
			{
				Type:             "external-validator",
				Endpoint:         endpoint,
				ValidatorMethod:  "GET",
				Header:           "Authorization",
				ForwardHeader:    "Authorization",
				ResponseValid:    "valid",
				ResponseIdentity: "userId",
				ResponseScopes:   "scopes",
				CacheTTL:         5 * time.Second,
				Timeout:          1 * time.Second,
				FailureMode:      "fail-closed",
			},
		},
	}
	return c
}
