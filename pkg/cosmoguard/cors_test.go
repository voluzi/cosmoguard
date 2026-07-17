package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func corsVaryHasOrigin(h http.Header) bool {
	for _, v := range h.Values("Vary") {
		for _, tok := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), "Origin") {
				return true
			}
		}
	}
	return false
}

// TestCORSApplyToResponseVary verifies that when cosmoguard owns CORS it emits
// Vary: Origin on every response (matching rs/cors, the library CometBFT uses)
// and re-derives Access-Control-Allow-Origin from the current request. This is
// what makes a Vary: Origin response safe to cache: ACAO is never stored and is
// regenerated per hit, and the always-present Vary protects any shared cache
// between the client and cosmoguard.
func TestCORSApplyToResponseVary(t *testing.T) {
	mustCompile := func(c *CORSConfig) *CORSConfig {
		t.Helper()
		if err := c.Compile(); err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return c
	}

	t.Run("wildcard emits ACAO:* and Vary: Origin, strips upstream ACAO", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, AllowedOrigins: []string{"*"}})
		h := http.Header{}
		h.Set("Access-Control-Allow-Origin", "https://upstream.example") // upstream artifact
		h.Set("Vary", "Origin")
		c.ApplyToResponse(h, "https://app.example.com")
		if got := h.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Fatalf("ACAO = %q, want *", got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("Vary: Origin must be present, got %q", h.Values("Vary"))
		}
	})

	t.Run("specific origin echoes origin and keeps Vary: Origin", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, AllowedOrigins: []string{"https://app.example.com"}})
		h := http.Header{}
		h.Set("Vary", "Origin")
		c.ApplyToResponse(h, "https://app.example.com")
		if got := h.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Fatalf("ACAO = %q, want the request origin", got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("Vary: Origin must be present, got %q", h.Values("Vary"))
		}
	})

	t.Run("credentialed echoes origin, sets ACAC, keeps Vary: Origin", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, Credentials: true, AllowedOrigins: []string{"https://app.example.com"}})
		h := http.Header{}
		c.ApplyToResponse(h, "https://app.example.com")
		if got := h.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Fatalf("ACAO = %q, want the request origin", got)
		}
		if got := h.Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Fatalf("ACAC = %q, want true", got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("Vary: Origin must be present, got %q", h.Values("Vary"))
		}
	})

	t.Run("no-Origin request: no ACAO but Vary: Origin still advertised", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, AllowedOrigins: []string{"*"}})
		h := http.Header{}
		c.ApplyToResponse(h, "")
		if got := h.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("no-Origin request must not get ACAO, got %q", got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("Vary: Origin must be advertised even without an Origin, got %q", h.Values("Vary"))
		}
	})

	t.Run("disallowed origin: no ACAO but Vary: Origin still advertised", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, AllowedOrigins: []string{"https://app.example.com"}})
		h := http.Header{}
		c.ApplyToResponse(h, "https://evil.example")
		if got := h.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("disallowed origin must not get ACAO, got %q", got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("Vary: Origin must be advertised for a disallowed origin, got %q", h.Values("Vary"))
		}
	})

	t.Run("preserves other Vary tokens, no Origin duplication", func(t *testing.T) {
		c := mustCompile(&CORSConfig{Enable: true, AllowedOrigins: []string{"https://app.example.com"}})
		h := http.Header{}
		h.Add("Vary", "Accept-Encoding")
		h.Add("Vary", "Origin") // upstream already sent Origin on a second line
		c.ApplyToResponse(h, "https://app.example.com")
		joined := strings.ToLower(strings.Join(h.Values("Vary"), ","))
		if !strings.Contains(joined, "accept-encoding") {
			t.Fatalf("Accept-Encoding must be preserved, got %q", h.Values("Vary"))
		}
		// Origin must appear exactly once (addVary is idempotent).
		n := strings.Count(joined, "origin")
		if n != 1 {
			t.Fatalf("Origin should appear exactly once, got %d in %q", n, h.Values("Vary"))
		}
	})

	t.Run("disabled: leaves upstream headers untouched", func(t *testing.T) {
		c := &CORSConfig{Enable: false}
		h := http.Header{}
		h.Set("Access-Control-Allow-Origin", "https://upstream.example")
		h.Set("Vary", "Origin")
		c.ApplyToResponse(h, "https://app.example.com")
		if got := h.Get("Access-Control-Allow-Origin"); got != "https://upstream.example" {
			t.Fatalf("disabled CORS must not touch ACAO, got %q", got)
		}
	})
}

// TestCacheHitReDerivesACAOPerOrigin is the load-bearing safety check for
// caching Vary: Origin responses: a SINGLE cached body — stored WITHOUT
// Access-Control-Allow-Origin — is replayed to two different allowed origins,
// and each gets ITS OWN ACAO re-derived on the hit path. This proves cosmoguard
// never serves one origin's CORS grant to another from a shared cache entry.
func TestCacheHitReDerivesACAOPerOrigin(t *testing.T) {
	cors := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://a.example", "https://b.example"}}
	if err := cors.Compile(); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	p := &HttpProxy{log: log.WithField("test", "cache-hit-cors"), cors: cors}
	// Exactly what the miss path stores: body + preserved headers, and
	// crucially NO Access-Control-Allow-Origin.
	res := CachedResponse{
		Data:       []byte(`{"result":"status"}`),
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json", "Vary": "Origin"},
		StoredAt:   time.Now().UTC(),
	}
	for _, origin := range []string{"https://a.example", "https://b.example"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		req.Header.Set("Origin", origin)
		p.cacheHit(rec, req, res, time.Now())
		h := rec.Result().Header
		if got := h.Get("X-Cosmoguard-Cache"); got != "hit" {
			t.Fatalf("origin %s: X-Cosmoguard-Cache = %q, want hit", origin, got)
		}
		if got := h.Get("Access-Control-Allow-Origin"); got != origin {
			t.Fatalf("origin %s: ACAO = %q, want the request origin (re-derived per hit)", origin, got)
		}
		if !corsVaryHasOrigin(h) {
			t.Fatalf("origin %s: Vary: Origin must be present on hit, got %q", origin, h.Values("Vary"))
		}
		if body := rec.Body.String(); body != `{"result":"status"}` {
			t.Fatalf("origin %s: body = %q, want the shared cached body", origin, body)
		}
	}
}
