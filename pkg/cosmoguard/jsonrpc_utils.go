package cosmoguard

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/voluzi/cosmoguard/pkg/cache"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type JsonRpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *JsonRpcError) Clone() *JsonRpcError {
	if e == nil {
		return nil
	}
	return &JsonRpcError{
		Code:    e.Code,
		Message: e.Message,
		Data:    e.Data,
	}
}

type JsonRpcMsg struct {
	Version string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Method  string      `json:"method,omitempty"`
	Params  interface{} `json:"params,omitempty"`
	// Result is held as RawMessage so the upstream's exact byte order is
	// preserved end-to-end. Decoding into a generic map and re-encoding
	// re-sorts object keys (alphabetic) and breaks cosmoguard's byte-
	// identical compatibility guarantee on cache hits. Use the
	// `EmptyResult` / `WithResult` helpers below for cosmoguard-generated
	// responses (they marshal arbitrary Go values into RawMessage).
	Result jsoniter.RawMessage `json:"result,omitempty"`
	Error  *JsonRpcError       `json:"error,omitempty"`

	// WireSuffix preserves any trailing wire bytes the upstream emitted
	// after the JSON object (typically "\n" from Cosmos RPC servers).
	// Marshal appends these verbatim so cache-hit replays remain byte-
	// identical to the original upstream payload — the byte-fidelity
	// invariant the project keeps for the JSON-RPC inner Result also
	// applies to the outer envelope's wire framing. Excluded from JSON
	// itself (json:"-") and carried through Redis-backed caches via
	// msgpack.
	WireSuffix []byte `json:"-" msgpack:"wire_suffix,omitempty"`

	// StoredAt is when this response was written to the cache. Like
	// WireSuffix it is cache-only (never serialized to a client) and
	// carried through the msgpack-backed L2. The freshness /
	// stale-while-revalidate policy reads it at lookup time. Zero for
	// entries written before this field existed → treated as fresh (see
	// classifyFreshness): those entries have no stale window, so the
	// backend TTL still evicts them correctly.
	StoredAt time.Time `json:"-" msgpack:"stored_at,omitempty"`
}

// jsonRpcMsgOverheadBytes is a flat per-entry allowance covering the
// struct, the Version/Method strings, and the interface{} boxes that the
// byte-length fields below don't capture. Conservative on purpose so the
// L1 byte budget over-counts rather than under-counts.
const jsonRpcMsgOverheadBytes uint64 = 128

// CacheCost reports this message's approximate in-memory footprint in
// bytes so the L1 byte-cost eviction (cache.MaxCost) accounts for it. The
// large, variable parts are Result and WireSuffix (raw bytes); Method, the
// ID, and the Error (message + data) are added, plus a fixed overhead for
// the envelope. ID and Error.Data are interface{} and can carry
// caller-controlled strings, so they must be charged — otherwise a client
// sending many distinct large IDs could grow the cache past its budget
// while being charged near-zero.
func (j *JsonRpcMsg) CacheCost() uint64 {
	cost := jsonRpcMsgOverheadBytes
	cost += uint64(len(j.Result))
	cost += uint64(len(j.WireSuffix))
	cost += uint64(len(j.Method))
	cost += approxInterfaceCost(j.ID)
	if j.Error != nil {
		cost += uint64(len(j.Error.Message))
		cost += approxInterfaceCost(j.Error.Data)
	}
	return cost
}

// approxInterfaceCost estimates the byte footprint of an arbitrary JSON-RPC
// interface{} field (ID, Error.Data). The common shapes — nil, string, raw
// bytes — are measured directly with no allocation. A structured value
// (map/slice from a decoded `error.data` object, or a rare structured ID) is
// serialized to measure its true size: charging a tiny fixed amount would let
// a client cache large structured error payloads while accounting for almost
// nothing, defeating the byte budget. On a marshal error we fall back to a
// conservative fixed estimate.
func approxInterfaceCost(v interface{}) uint64 {
	switch t := v.(type) {
	case nil:
		return 0
	case string:
		return uint64(len(t))
	case []byte:
		return uint64(len(t))
	case jsoniter.RawMessage:
		return uint64(len(t))
	case bool, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return 16 // scalar — small and bounded, no need to marshal
	default:
		if b, err := json.Marshal(t); err == nil {
			return uint64(len(b))
		}
		return 256 // conservative fallback when the value can't be marshaled
	}
}

// rawOrNull is a custom unmarshal target that distinguishes an absent
// JSON field, an explicit `null`, and any other value. jsoniter's
// RawMessage unmarshaller collapses `null` and "absent" into the same
// empty slice (unlike encoding/json), so callers use this shim when
// wire-level field presence affects JSON-RPC semantics.
type rawOrNull struct {
	raw    []byte
	isNull bool
}

func (r *rawOrNull) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		r.isNull = true
		return nil
	}
	r.raw = b
	return nil
}

func (j *JsonRpcMsg) UnmarshalJSON(b []byte) error {
	type msg JsonRpcMsg

	var dest = struct {
		*msg
		ID     rawOrNull `json:"id,omitempty"`
		Result rawOrNull `json:"result,omitempty"`
	}{
		msg: (*msg)(j),
	}

	if err := json.Unmarshal(b, &dest); err != nil {
		return err
	}

	switch {
	case dest.ID.isNull:
		j.ID = explicitNullID
	case len(dest.ID.raw) >= 1:
		i, err := strconv.Atoi(string(dest.ID.raw))
		if err == nil {
			j.ID = i
		} else {
			j.ID, err = strconv.Unquote(string(dest.ID.raw))
			if err != nil {
				return err
			}
		}
	default:
		j.ID = nil
	}

	// Preserve `"result":null` as the literal four bytes so a marshal
	// round-trip stays JSON-RPC 2.0 compliant — the spec requires a
	// successful response to carry either `result` or `error`, and
	// dropping the field entirely (omitempty on nil RawMessage) silently
	// breaks that for cosmos/EVM upstreams that legitimately return
	// `"result":null` (e.g. eth_getBlockByHash for an unknown hash).
	// An absent `result` field still parses to nil, so callers must
	// distinguish "absent" from "explicitly null" via IsEmptyResult.
	switch {
	case dest.Result.isNull:
		j.Result = jsoniter.RawMessage("null")
	case len(dest.Result.raw) > 0:
		j.Result = dest.Result.raw
	default:
		j.Result = nil
	}

	return nil
}

// IsEmptyResult reports whether this message carries no meaningful
// result — either the field is absent or it's the literal JSON `null`.
// Cache gates that opt into "cache empty results only" must check this
// helper, not the raw `Result == nil`, so that `"result":null` upstream
// responses round-trip byte-identically while still being recognized
// as empty by the cache pump.
func (j *JsonRpcMsg) IsEmptyResult() bool {
	return len(j.Result) == 0 || bytes.Equal(j.Result, []byte("null"))
}

func (j *JsonRpcMsg) Clone() *JsonRpcMsg {
	return &JsonRpcMsg{
		Version:    j.Version,
		ID:         j.ID,
		Method:     j.Method,
		Params:     j.Params,
		Result:     j.Result,
		Error:      j.Error.Clone(),
		WireSuffix: j.WireSuffix,
		StoredAt:   j.StoredAt,
	}
}

func (j *JsonRpcMsg) CloneWithID(id interface{}) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version:    j.Version,
		ID:         id,
		Method:     j.Method,
		Params:     j.Params,
		Result:     j.Result,
		Error:      j.Error.Clone(),
		WireSuffix: j.WireSuffix,
		StoredAt:   j.StoredAt,
	}
}

// HashWithRule mixes a rule fingerprint into the cache key so two
// cacheable rules matching the same JSON-RPC method don't share the
// same cache namespace. Without this, a request matched by rule A
// (TTL=5s, cacheError=false) and rule B (TTL=1h, cacheError=true)
// at the same path collided: whichever rule populated the cache
// first won — the other rule's TTL / error-caching policy was
// silently ignored. The HTTP layer's getRequestHash has had this
// rule-fingerprint mix for a while; the JSON-RPC layer never did.
func (j *JsonRpcMsg) HashWithRule(ruleFingerprint uint64) uint64 {
	b, err := json.Marshal(j.Params)
	if err != nil {
		return 0
	}
	buf := make([]byte, 0, 8+len(j.Method)+1+len(b))
	// 8 bytes of fingerprint first, then method + NUL + params.
	for i := 0; i < 8; i++ {
		buf = append(buf, byte(ruleFingerprint>>uint(8*i)))
	}
	buf = append(buf, j.Method...)
	buf = append(buf, 0)
	buf = append(buf, b...)
	return fnv1a.HashBytes64(buf)
}

func (j *JsonRpcMsg) Marshal() ([]byte, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	if len(j.WireSuffix) > 0 {
		b = append(b, j.WireSuffix...)
	}
	return b, nil
}

type JsonRpcMsgs []*JsonRpcMsg

func (j JsonRpcMsgs) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

// trailingWhitespace returns the run of whitespace bytes at the end of
// b — newlines, tabs, carriage returns, spaces. Used by the JSON-RPC
// cache pump to preserve the upstream's trailing wire framing on
// cache-hit replays (Cosmos / EVM JSON-RPC servers commonly send a
// single "\n" after the JSON object). A nil/empty return means the
// upstream emitted no trailing whitespace; callers can skip the
// WireSuffix assignment in that case.
func trailingWhitespace(b []byte) []byte {
	i := len(b)
	for i > 0 {
		c := b[i-1]
		if c != '\n' && c != '\r' && c != '\t' && c != ' ' {
			break
		}
		i--
	}
	if i == len(b) {
		return nil
	}
	out := make([]byte, len(b)-i)
	copy(out, b[i:])
	return out
}

func ParseJsonRpcMessage(b []byte) (*JsonRpcMsg, JsonRpcMsgs, error) {
	if bytes.HasPrefix(b, []byte{'['}) {
		var msg JsonRpcMsgs
		err := json.Unmarshal(b, &msg)
		return nil, msg, err
	}
	var msg JsonRpcMsg
	err := json.Unmarshal(b, &msg)
	return &msg, nil, err
}

func UnauthorizedResponse(req *JsonRpcMsg) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Error: &JsonRpcError{
			Code:    http.StatusUnauthorized,
			Message: "unauthorized access",
		},
		ID: req.ID,
	}
}

// explicitNullID is an ID sentinel that marshals to the literal JSON `null`.
// The JsonRpcMsg.ID field carries `omitempty`, so a bare `nil` id is dropped
// from the wire entirely — but JSON-RPC 2.0 §5.1 requires `"id":null` (not
// absent) on parse / invalid-request errors where no request id is known.
// A non-nil interface holding this value defeats omitempty and emits `null`.
type explicitNullIDType struct{}

func (explicitNullIDType) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

var explicitNullID interface{} = explicitNullIDType{}

// ParseErrorResponse builds a JSON-RPC 2.0 parse error (-32700) with an
// explicit `"id":null` (§5.1) — used when an inbound frame isn't valid JSON.
func ParseErrorResponse() *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Error: &JsonRpcError{
			Code:    -32700,
			Message: "Parse error",
		},
		ID: explicitNullID,
	}
}

// InvalidRequestResponse builds a JSON-RPC 2.0 invalid-request error
// (-32600) with an explicit `"id":null`. Used when a frame is valid JSON but
// not a supported single request (e.g. a batch array, which cosmoguard's WS
// path does not support).
func InvalidRequestResponse() *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Error: &JsonRpcError{
			Code:    -32600,
			Message: "Invalid Request",
		},
		ID: explicitNullID,
	}
}

func EmptyResult(req *JsonRpcMsg) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Result:  jsoniter.RawMessage("{}"),
		ID:      req.ID,
	}
}

// WithResult builds a cosmoguard-generated response, marshalling the
// provided value into RawMessage bytes. Pass already-marshalled bytes
// as `jsoniter.RawMessage(...)` if you need exact-byte control.
func WithResult(req *JsonRpcMsg, result interface{}) *JsonRpcMsg {
	var raw jsoniter.RawMessage
	switch v := result.(type) {
	case jsoniter.RawMessage:
		raw = v
	case nil:
		raw = nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			// A marshalling failure here would silently produce a
			// response with no result. Surface as a Go panic — this
			// only fires for in-process callers (subscribe/unsubscribe
			// responses with bool / string), all of which marshal
			// trivially.
			panic(fmt.Sprintf("WithResult: marshal %T: %v", result, err))
		}
		raw = b
	}
	return &JsonRpcMsg{
		Version: "2.0",
		Result:  raw,
		ID:      req.ID,
	}
}

func ErrorResponse(req *JsonRpcMsg, code int, message string, data interface{}) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Error: &JsonRpcError{
			Code:    code,
			Message: message,
			Data:    data,
		},
		ID: req.ID,
	}
}

type JsonRpcResponse struct {
	Request  *JsonRpcMsg
	Response *JsonRpcMsg
	Cache    *RuleCache
	CacheKey uint64
	// RuleTag is the matched rule's operator tag (or fingerprint
	// fallback). Used by StoreInCache to bump the per-rule cache-
	// cardinality counter on the dashboard.
	RuleTag string
}

type JsonRpcResponses []*JsonRpcResponse

func (l *JsonRpcResponses) GetPendingRequests() JsonRpcMsgs {
	requests := make(JsonRpcMsgs, 0)
	for _, r := range *l {
		if r.Response == nil {
			requests = append(requests, r.Request)
		}
	}
	return requests
}

// UnansweredCalls counts request slots that carry an `id` (i.e. calls,
// not notifications) but still have no response. After Set has
// correlated upstream responses by id, a non-zero count means a real
// call was left unanswered and would be silently dropped from the
// client payload — the caller should fail the batch loudly. Notifications
// (no `id`) legitimately have no response and are not counted.
func (l *JsonRpcResponses) UnansweredCalls() int {
	n := 0
	for _, r := range *l {
		if r.Response == nil && r.Request != nil && r.Request.ID != nil {
			n++
		}
	}
	return n
}

// FillUnansweredCalls replaces every still-pending id-bearing call (a
// slot with no Response) with a JSON-RPC internal error. Used after Set
// (or an upstream failure) so a batch where the upstream errored or
// omitted some responses still returns a valid array — answered calls
// and cache hits are preserved. Notifications (no id) are left untouched
// (they get no response per JSON-RPC 2.0 §4.1).
func (l *JsonRpcResponses) FillUnansweredCalls() {
	for _, r := range *l {
		if r.Response == nil && r.Request != nil && r.Request.ID != nil {
			r.Response = ErrorResponse(r.Request, -32603, "upstream error", nil)
		}
	}
}

func (l *JsonRpcResponses) GetFinal() JsonRpcMsgs {
	responses := make(JsonRpcMsgs, 0)
	for _, r := range *l {
		if r.Response != nil {
			responses = append(responses, r.Response)
		}
	}
	return responses
}

func (l *JsonRpcResponses) AddPending(request *JsonRpcMsg) {
	res := &JsonRpcResponse{
		Request: request,
	}
	*l = append(*l, res)
}

func (l *JsonRpcResponses) AddResponse(request, response *JsonRpcMsg) {
	res := &JsonRpcResponse{
		Request: request,
	}
	res.Response = response.CloneWithID(request.ID)
	*l = append(*l, res)
}

func (l *JsonRpcResponses) AddPendingWithCacheConfig(request *JsonRpcMsg, cacheKey uint64, cacheCfg *RuleCache, ruleTag string) {
	res := &JsonRpcResponse{
		Request:  request,
		Cache:    cacheCfg,
		CacheKey: cacheKey,
		RuleTag:  ruleTag,
	}
	*l = append(*l, res)
}

// Set correlates upstream batch responses to the pending request
// slots by JSON-RPC `id`, NOT by position. Per JSON-RPC 2.0 §6 the
// server MAY return batch responses in any order; the client must
// match by id. Positional correlation (the previous shape) silently
// attributed each response to the wrong request whenever a Cosmos /
// EVM node legally re-ordered — wrong tx hashes, wrong balances,
// data corruption that's invisible to monitoring.
//
// `requests` and `responses` are still both passed because:
//   - falling back to positional when ids are unique-but-missing
//     would be too lenient; we just leave the slot pending
//     (UnauthorizedResponse will surface as the visible error);
//   - duplicate ids in `responses` (malformed upstream) win to
//     last-write — explicit instead of "whichever map iteration
//     order picked"; a future change can warn.
func (l *JsonRpcResponses) Set(requests, responses JsonRpcMsgs) {
	byID := make(map[any]*JsonRpcMsg, len(responses))
	for _, resp := range responses {
		if resp == nil {
			continue
		}
		byID[normalizeJsonRpcID(resp.ID)] = resp
	}
	for i, req := range requests {
		res := l.Find(req)
		if res == nil {
			continue
		}
		if req == nil || req.ID == nil {
			continue
		}
		if matched, ok := byID[normalizeJsonRpcID(req.ID)]; ok {
			res.Response = matched
			continue
		}
		// Fallback: when ids didn't match (notification batch, or
		// upstream omitted id) keep the positional pairing so an
		// upstream that DOES respond in order still works. Only
		// applied when both sides are the same length.
		if len(requests) == len(responses) && i < len(responses) {
			res.Response = responses[i]
		}
	}
}

// normalizeJsonRpcID turns the per-spec polymorphic id (string, number,
// or null) into a comparable map key. Numbers are normalized to int64
// where possible, so an upstream that returns the id as a float doesn't
// fail to match a request that supplied an int (Go's json default
// unmarshal types every number as float64).
func normalizeJsonRpcID(id any) any {
	switch v := id.(type) {
	case nil:
		return nil
	case float64:
		// Preserve integer-valued floats as int64 so a request id of
		// 7 matches a response id of 7.0 (or vice versa).
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	default:
		return v
	}
}

func (l *JsonRpcResponses) Find(request *JsonRpcMsg) *JsonRpcResponse {
	for _, r := range *l {
		if r.Request == request {
			return r
		}
	}
	return nil
}

func (l *JsonRpcResponses) Deny(request *JsonRpcMsg) {
	res := &JsonRpcResponse{
		Request:  request,
		Response: UnauthorizedResponse(request),
		Cache:    nil,
		CacheKey: 0,
	}
	*l = append(*l, res)
}

// StoreInCache writes the cached responses to the backend and, when
// onWrite is non-nil, invokes it once per successful write so the
// caller can bump the per-rule cache-cardinality counter (the WS / HTTP
// single-request paths do this inline; batch fans out through here).
// onWrite receives the rule tag and the original JSON-RPC method.
func (l *JsonRpcResponses) StoreInCache(cache cache.Cache[uint64, *JsonRpcMsg], now time.Time, global *CacheGlobalConfig, upstreamHeaders http.Header, onWrite func(ruleTag, method string)) error {
	// A batch shares one HTTP response, so its Cache-Control governs every
	// message in it. Anti-cache directives (no-store/no-cache/private/zero
	// max-age) forbid storage: entries here are written under a stale-extended
	// TTL for single-path / WS serve-stale, so a stored no-cache reply would be
	// served stale without the revalidation it requires. Mirror the HTTP path.
	if !cacheableByUpstream(upstreamHeaders) {
		return nil
	}
	for _, r := range *l {
		if r.Response != nil && r.Cache != nil && r.Cache.Enable {
			if r.Response.Error != nil && !r.Cache.CacheError {
				continue
			}
			if r.Response.IsEmptyResult() && !r.Cache.CacheEmptyResult {
				continue
			}

			// Stamp StoredAt and store under a physical TTL extended by the
			// stale window (StoredAt is json:"-" so it never leaks to the
			// client) so single-path / WS reads can serve this entry stale
			// while it revalidates. When SWR is off the physical TTL is just
			// the logical TTL.
			r.Response.StoredAt = now
			stale := upstreamHTTPStaleWindow(upstreamHeaders, resolveStaleWindow(r.Cache, cfgStaleWindow(global)))
			ttl := physicalTTL(effectiveTTL(r.Cache, global), stale)
			if err := cache.Set(context.Background(), r.CacheKey, r.Response, ttl); err != nil {
				return err
			}
			if onWrite != nil && r.Request != nil {
				onWrite(r.RuleTag, r.Request.Method)
			}
		}
	}
	return nil
}
