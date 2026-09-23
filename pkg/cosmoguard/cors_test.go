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
// and re-derives Access-Control-Allow-Origin from the current request. The
// synthetic Vary protects shared caches between clients and cosmoguard; raw
// upstream Vary is evaluated separately for cosmoguard's own cache admission.
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

// TestCacheHitReDerivesACAOPerOrigin is the load-bearing safety check for a
// cached response carrying cosmoguard's synthetic Vary: Origin. The body is
// stored without Access-Control-Allow-Origin and each hit derives its own ACAO.
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

func TestCORSStripFromResponse(t *testing.T) {
	c := &CORSConfig{Enable: true, AllowedOrigins: []string{"https://a.example"}}
	if err := c.Compile(); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	tests := []struct {
		name string
		vary []string
		want []string
	}{
		{name: "origin only", vary: []string{"Origin"}, want: nil},
		{name: "multi-token value", vary: []string{"Accept-Encoding, Origin"}, want: []string{"Accept-Encoding"}},
		{name: "origin first", vary: []string{"origin, Accept-Encoding"}, want: []string{"Accept-Encoding"}},
		{name: "several lines", vary: []string{"Origin", "Accept-Encoding"}, want: []string{"Accept-Encoding"}},
		{name: "no origin", vary: []string{"Accept-Encoding"}, want: []string{"Accept-Encoding"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			h.Set("Access-Control-Allow-Origin", "https://a.example")
			h.Set("Access-Control-Allow-Credentials", "true")
			for _, v := range tt.vary {
				h.Add("Vary", v)
			}
			c.StripFromResponse(h)
			if got := h.Values("Access-Control-Allow-Origin"); len(got) != 0 {
				t.Fatalf("ACAO must be stripped, got %q", got)
			}
			if got := h.Values("Access-Control-Allow-Credentials"); len(got) != 0 {
				t.Fatalf("ACAC must be stripped, got %q", got)
			}
			got := h.Values("Vary")
			if len(got) != len(tt.want) {
				t.Fatalf("Vary = %q, want %q", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("Vary = %q, want %q", got, tt.want)
				}
			}
		})
	}

	t.Run("disabled", func(t *testing.T) {
		h := http.Header{}
		h.Set("Access-Control-Allow-Origin", "https://upstream.example")
		h.Set("Vary", "Origin")
		(&CORSConfig{Enable: false}).StripFromResponse(h)
		if h.Get("Access-Control-Allow-Origin") == "" || h.Get("Vary") == "" {
			t.Fatal("disabled CORS must not touch the headers")
		}
	})
}
