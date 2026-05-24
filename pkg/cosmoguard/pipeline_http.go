package cosmoguard

import (
	"context"
	"net/http"
)

// httpRequest adapts a net/http.Request to the cosmoguard Request
// interface for the unified pipeline. Owned by the HTTP proxy's
// ServeHTTP, it carries the *http.Request + mutable per-request state
// (rule tag, identity) that middlewares fill in as the chain runs.
//
// Read-only fields (operation, source IP) are computed lazily on
// first access — keeps the adapter constructor cheap on every
// request.
type httpRequest struct {
	r        *http.Request
	identity *Identity
	ruleTag  string
}

func newHTTPRequest(r *http.Request) *httpRequest {
	return &httpRequest{r: r}
}

func (h *httpRequest) Context() context.Context { return h.r.Context() }
func (h *httpRequest) WithContext(ctx context.Context) Request {
	out := *h
	out.r = h.r.WithContext(ctx)
	return &out
}
func (h *httpRequest) Protocol() string      { return "http" }
func (h *httpRequest) OperationName() string { return h.r.URL.Path }
func (h *httpRequest) SourceIP() string      { return GetSourceIP(h.r) }

func (h *httpRequest) RuleTag() string     { return h.ruleTag }
func (h *httpRequest) SetRuleTag(s string) { h.ruleTag = s }

func (h *httpRequest) Identity() *Identity     { return h.identity }
func (h *httpRequest) SetIdentity(i *Identity) { h.identity = i }

// HTTPRequest returns the underlying net/http.Request. Used by
// HTTP-specific middlewares (rate-limit key derivation, auth header
// extraction) that need the raw request.
func (h *httpRequest) HTTPRequest() *http.Request { return h.r }
