package cosmoguard

import (
	"context"
	"net/http"
)

// jsonRpcRequest adapts a single JSON-RPC message to the pipeline
// Request interface. The httpReq is the carrier HTTP request (the
// JSON-RPC handler runs over HTTP), so HTTP-specific middlewares
// (identity-resolve via headers, rate-limit-key derivation) keep
// working transparently.
type jsonRpcRequest struct {
	httpReq  *http.Request
	msg      *JsonRpcMsg
	identity *Identity
	ruleTag  string
}

func newJsonRpcRequest(httpReq *http.Request, msg *JsonRpcMsg) *jsonRpcRequest {
	return &jsonRpcRequest{httpReq: httpReq, msg: msg}
}

func (j *jsonRpcRequest) Context() context.Context { return j.httpReq.Context() }
func (j *jsonRpcRequest) WithContext(ctx context.Context) Request {
	out := *j
	out.httpReq = j.httpReq.WithContext(ctx)
	return &out
}
func (j *jsonRpcRequest) Protocol() string           { return "jsonrpc" }
func (j *jsonRpcRequest) OperationName() string      { return j.msg.Method }
func (j *jsonRpcRequest) SourceIP() string           { return GetSourceIP(j.httpReq) }
func (j *jsonRpcRequest) RuleTag() string            { return j.ruleTag }
func (j *jsonRpcRequest) SetRuleTag(s string)        { j.ruleTag = s }
func (j *jsonRpcRequest) Identity() *Identity        { return j.identity }
func (j *jsonRpcRequest) SetIdentity(i *Identity)    { j.identity = i }
func (j *jsonRpcRequest) HTTPRequest() *http.Request { return j.httpReq }

// wsRequest adapts a WS frame's JSON-RPC message. The carrier
// http.Request is the original WS upgrade — useful for source IP
// derivation but generally not used for header-based auth (clients
// don't typically attach Authorization to WS frames). Kept for
// pipeline uniformity.
type wsRequest struct {
	httpReq  *http.Request
	msg      *JsonRpcMsg
	identity *Identity
	ruleTag  string
}

func newWSRequest(httpReq *http.Request, msg *JsonRpcMsg) *wsRequest {
	return &wsRequest{httpReq: httpReq, msg: msg}
}

func (w *wsRequest) Context() context.Context { return w.httpReq.Context() }
func (w *wsRequest) WithContext(ctx context.Context) Request {
	out := *w
	out.httpReq = w.httpReq.WithContext(ctx)
	return &out
}
func (w *wsRequest) Protocol() string           { return "ws" }
func (w *wsRequest) OperationName() string      { return w.msg.Method }
func (w *wsRequest) SourceIP() string           { return GetSourceIP(w.httpReq) }
func (w *wsRequest) RuleTag() string            { return w.ruleTag }
func (w *wsRequest) SetRuleTag(s string)        { w.ruleTag = s }
func (w *wsRequest) Identity() *Identity        { return w.identity }
func (w *wsRequest) SetIdentity(i *Identity)    { w.identity = i }
func (w *wsRequest) HTTPRequest() *http.Request { return w.httpReq }

// grpcRequest adapts an inbound gRPC unary call. method is the full
// gRPC path ("/pkg.Svc/Method"); peer is the resolved peer.Addr (source
// IP). No carrier *http.Request — gRPC has its own metadata model — so
// HTTP-specific middlewares (header auth, HTTP-shaped rate-limit key)
// degrade to no-ops; the chain still runs PanicRecovery + future
// gRPC-aware middlewares.
type grpcRequest struct {
	ctx      context.Context
	method   string
	peer     string
	identity *Identity
	ruleTag  string
}

func newGrpcRequest(ctx context.Context, method, peer string) *grpcRequest {
	return &grpcRequest{ctx: ctx, method: method, peer: peer}
}

func (g *grpcRequest) Context() context.Context { return g.ctx }
func (g *grpcRequest) WithContext(ctx context.Context) Request {
	out := *g
	out.ctx = ctx
	return &out
}
func (g *grpcRequest) Protocol() string        { return "grpc" }
func (g *grpcRequest) OperationName() string   { return g.method }
func (g *grpcRequest) SourceIP() string        { return g.peer }
func (g *grpcRequest) RuleTag() string         { return g.ruleTag }
func (g *grpcRequest) SetRuleTag(s string)     { g.ruleTag = s }
func (g *grpcRequest) Identity() *Identity     { return g.identity }
func (g *grpcRequest) SetIdentity(i *Identity) { g.identity = i }
