package cosmoguard

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CORSConfig is the v4 cross-origin policy. When Enable is true, cosmoguard
// fully owns CORS — every upstream CORS header is stripped and replaced
// with cosmoguard's own. This protects against the v3 behavior where a
// node returning `Access-Control-Allow-Origin: *` made cosmoguard's gating
// trivially bypassable from any browser.
//
// Preflight (OPTIONS) requests are handled by cosmoguard directly and
// never forwarded upstream — they're protocol overhead, not application
// traffic.
type CORSConfig struct {
	Enable bool `yaml:"enable,omitempty"`

	// AllowedOrigins lists the origins permitted to make cross-origin
	// requests. Wildcard "*" allows any (combined with Credentials=true,
	// the spec demotes the response to "*"-disallowed; cosmoguard echoes
	// the request Origin in that case to keep credentialed requests
	// working).
	AllowedOrigins []string `yaml:"allowedOrigins,omitempty"`

	// AllowedMethods is the closed set of HTTP methods clients may invoke.
	// Defaults to GET, POST, OPTIONS when omitted.
	AllowedMethods []string `yaml:"allowedMethods,omitempty"`

	// AllowedHeaders names request headers the browser may send on a
	// cross-origin call. Cosmoguard adds Authorization here when auth is
	// enabled so credentialed requests work without operator
	// configuration.
	AllowedHeaders []string `yaml:"allowedHeaders,omitempty"`

	// ExposeHeaders names response headers JavaScript may read.
	ExposeHeaders []string `yaml:"exposeHeaders,omitempty"`

	// MaxAge bounds preflight cache lifetime (Access-Control-Max-Age).
	MaxAge time.Duration `yaml:"maxAge,omitempty"`

	// Credentials toggles Access-Control-Allow-Credentials: true. When
	// true, AllowedOrigins MUST NOT contain "*" per the CORS spec; the
	// special case described above kicks in.
	Credentials bool `yaml:"credentials,omitempty"`

	// compiled state
	originCheck func(origin string) bool `yaml:"-"`
	methodList  string                   `yaml:"-"`
	headerList  string                   `yaml:"-"`
	exposeList  string                   `yaml:"-"`
	maxAgeStr   string                   `yaml:"-"`
}

// Compile validates and pre-computes header values used by Apply. Returns
// an error if AllowedOrigins contains a malformed glob.
func (c *CORSConfig) Compile() error {
	if !c.Enable {
		return nil
	}
	// Reject the universal-credentialed-cross-origin shape up-front.
	// The CORS spec forbids `Access-Control-Allow-Origin: *` together
	// with `Access-Control-Allow-Credentials: true` — browsers reject
	// the response — and writeCORSHeadersToMap below works around that
	// by echoing the request Origin verbatim. The combination of
	// "echo any origin" + "credentials allowed" lets *any* attacker
	// origin issue credentialed requests against cosmoguard and read
	// the response, defeating cookie / Authorization scoping. Fail
	// startup loudly so a misconfigured deployment never ships.
	if c.Credentials {
		for _, o := range c.AllowedOrigins {
			if o == "*" {
				return fmt.Errorf("cors: credentials=true is incompatible with allowedOrigins=\"*\"; specify explicit origins (or origin globs)")
			}
		}
	}
	check, err := compileOriginAllowlist(c.AllowedOrigins)
	if err != nil {
		return err
	}
	c.originCheck = func(origin string) bool {
		// reuse the WS origin allowlist's pure-string form by faking the
		// minimal http.Request shape the check expects.
		req := &http.Request{Header: http.Header{}}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		// A request with no Origin is "always allowed" by the WS check;
		// for CORS we only run this function when Origin is set, so
		// that special case is irrelevant here.
		return check(req)
	}
	methods := c.AllowedMethods
	if len(methods) == 0 {
		methods = []string{http.MethodGet, http.MethodPost, http.MethodOptions}
	}
	c.methodList = strings.Join(methods, ", ")
	if len(c.AllowedHeaders) > 0 {
		c.headerList = strings.Join(c.AllowedHeaders, ", ")
	}
	if len(c.ExposeHeaders) > 0 {
		c.exposeList = strings.Join(c.ExposeHeaders, ", ")
	}
	if c.MaxAge > 0 {
		// Access-Control-Max-Age is a whole-seconds integer. Truncating
		// via int(MaxAge.Seconds()) emits "0" for sub-second
		// configurations (e.g. `maxAge: 500ms`), which the browser
		// reads as "do not cache the preflight" — effectively
		// disabling the feature. Round UP to the next second so the
		// header always carries a useful value.
		secs := c.MaxAge / time.Second
		if c.MaxAge%time.Second > 0 {
			secs++
		}
		c.maxAgeStr = strconv.FormatInt(int64(secs), 10)
	} else if c.MaxAge < 0 {
		return fmt.Errorf("cors: maxAge must be >= 0 (got %s)", c.MaxAge)
	}
	return nil
}

// HandlePreflight responds to an OPTIONS preflight directly. Returns true
// if the request was a preflight and was handled (the caller should NOT
// continue processing). Returns false for non-preflight OPTIONS or when
// the request method isn't OPTIONS.
//
// Per the spec, a preflight is OPTIONS + Access-Control-Request-Method.
// Plain OPTIONS requests (e.g., for endpoint discovery) fall through.
func (c *CORSConfig) HandlePreflight(w http.ResponseWriter, r *http.Request) bool {
	if !c.Enable || r.Method != http.MethodOptions {
		return false
	}
	if r.Header.Get("Access-Control-Request-Method") == "" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" || c.originCheck == nil || !c.originCheck(origin) {
		// Origin not allowed (or absent on a preflight, which is
		// non-standard) — return 403. Do NOT echo CORS headers.
		w.WriteHeader(http.StatusForbidden)
		return true
	}
	c.writeCORSHeaders(w, origin)
	w.Header().Set("Access-Control-Allow-Methods", c.methodList)
	if c.headerList != "" {
		w.Header().Set("Access-Control-Allow-Headers", c.headerList)
	} else if reqHdrs := r.Header.Get("Access-Control-Request-Headers"); reqHdrs != "" {
		// Echo the requested headers when the operator didn't restrict
		// them. Less safe than an explicit allowlist, but matches typical
		// developer expectation.
		w.Header().Set("Access-Control-Allow-Headers", reqHdrs)
	}
	if c.maxAgeStr != "" {
		w.Header().Set("Access-Control-Max-Age", c.maxAgeStr)
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

// ApplyToResponse strips upstream CORS headers from `headers` and replaces
// them with cosmoguard's policy. Called from the cache-hit and cache-
// miss paths so cosmoguard, not the upstream, owns the CORS surface.
func (c *CORSConfig) ApplyToResponse(headers http.Header, requestOrigin string) {
	if !c.Enable {
		return
	}
	// Strip whatever upstream sent.
	for _, h := range []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Credentials",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
		"Access-Control-Expose-Headers",
		"Access-Control-Max-Age",
	} {
		headers.Del(h)
	}
	// Advertise Vary: Origin unconditionally whenever cosmoguard owns CORS —
	// matching rs/cors (the library CometBFT uses), which sets it on every
	// actual request regardless of policy, including wildcard and no-Origin
	// requests. This keeps cosmoguard byte-identical to a real node and stops
	// any shared cache BETWEEN a client and cosmoguard from replaying one
	// origin's response (or an ACAO-less one) to another. cosmoguard's OWN
	// cache is exempt via cacheableByVary's corsOwned path, since ACAO is
	// never stored and is re-derived here on every hit. addVary is idempotent,
	// so an upstream that already sent Vary: Origin isn't duplicated.
	addVary(headers, "Origin")
	// If the request had no Origin (non-browser client), don't add CORS
	// headers — they'd be meaningless.
	if requestOrigin == "" {
		return
	}
	if c.originCheck == nil || !c.originCheck(requestOrigin) {
		return
	}
	c.writeCORSHeadersToMap(headers, requestOrigin)
	if c.exposeList != "" {
		headers.Set("Access-Control-Expose-Headers", c.exposeList)
	}
}

func (c *CORSConfig) writeCORSHeaders(w http.ResponseWriter, origin string) {
	c.writeCORSHeadersToMap(w.Header(), origin)
}

func (c *CORSConfig) writeCORSHeadersToMap(h http.Header, origin string) {
	// Per spec: with Credentials=true the value must be a specific origin,
	// never "*". Even when AllowedOrigins is ["*"], echo the request
	// Origin so credentialed requests work.
	if c.Credentials {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Credentials", "true")
		// Vary on Origin so caches don't conflate responses for different
		// callers. Add (not Set) — upstream may already have emitted
		// Vary: Accept-Encoding or similar; Set would clobber those and
		// silently break caching of compressed responses.
		addVary(h, "Origin")
		return
	}
	// Non-credentialed: "*" is allowed per spec.
	for _, o := range c.AllowedOrigins {
		if o == "*" {
			h.Set("Access-Control-Allow-Origin", "*")
			return
		}
	}
	h.Set("Access-Control-Allow-Origin", origin)
	addVary(h, "Origin")
}

// addVary appends value to the Vary header without duplicating an
// existing entry. Vary is a comma-delimited list per RFC 7231 §7.1.4,
// so a naïve http.Header.Add would emit two separate Vary lines —
// technically valid but confuses some intermediaries. Walking the
// existing values keeps the header single-line and idempotent across
// repeat invocations (e.g. middleware ordering that runs CORS twice).
func addVary(h http.Header, value string) {
	for _, existing := range h.Values("Vary") {
		for _, token := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(token), value) {
				return
			}
		}
	}
	h.Add("Vary", value)
}
