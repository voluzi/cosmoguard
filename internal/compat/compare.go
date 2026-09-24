package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Response is one side's answer. Err is set only when no answer arrived
// (transport failure or timeout); an error status is still an answer and
// is compared like any other.
type Response struct {
	Status int
	Body   []byte
	Err    error
	// Height is the block height the answer was served at, when the
	// protocol reports it; 0 means unknown.
	Height int64
	// Denied marks a refusal status (HTTP 401/403, gRPC Unauthenticated/
	// PermissionDenied).
	Denied bool
	// ContentType is the HTTP Content-Type; empty for gRPC.
	ContentType string
	// Throttled marks a rate-limit answer that survived the retries.
	Throttled bool
	// Unrenderable says why a volatile answer could not be rendered for
	// shape comparison.
	Unrenderable string
	// Oversized marks a body cut at maxBody, which cannot be compared.
	Oversized bool
}

// Classify compares the node's answer with one or more cosmoguard answers
// to the same request (the second one is normally served from cache).
// volatile endpoints cannot be pinned to a height, so only the JSON shape
// of their bodies is compared.
func Classify(direct Response, proxies []Response, volatile bool) (Class, string) {
	if direct.Err != nil {
		return Failed, "node: " + direct.Err.Error()
	}
	if direct.Unrenderable != "" {
		return Skipped, direct.Unrenderable
	}
	for _, r := range append([]Response{direct}, proxies...) {
		if r.Oversized {
			return Skipped, fmt.Sprintf("an answer is larger than %d MiB, so it was not compared", maxBody>>20)
		}
	}
	if gatewayStatus(direct.Status) {
		// The node's own edge failed; there is no node answer to compare.
		return Failed, fmt.Sprintf("node: gateway status %d", direct.Status)
	}
	if direct.Throttled {
		return Failed, "node: still rate-limited after retries"
	}
	for i, p := range proxies {
		call := ""
		if len(proxies) > 1 {
			call = fmt.Sprintf("call %d: ", i+1)
		}
		if p.Err != nil {
			return Differs, call + "cosmoguard: " + p.Err.Error()
		}
		if p.Denied && !direct.Denied {
			return Denied, fmt.Sprintf("%scosmoguard refused with %d", call, p.Status)
		}
		if p.Status != direct.Status {
			return Differs, fmt.Sprintf("%sstatus node=%d cosmoguard=%d", call, direct.Status, p.Status)
		}
		if p.ContentType != direct.ContentType {
			return Differs, fmt.Sprintf("%sContent-Type node=%q cosmoguard=%q", call, direct.ContentType, p.ContentType)
		}
		if volatile {
			if d := ShapeDiff(direct.Body, p.Body); d != "" {
				return Differs, call + "shape: " + d
			}
			continue
		}
		if !bytes.Equal(direct.Body, p.Body) {
			return Differs, call + BodyDiff(direct.Body, p.Body)
		}
	}
	return Identical, ""
}

// gatewayStatus reports an HTTP status a proxy in front of a node sends
// when the node did not answer. A node answers errors with 4xx or 500.
func gatewayStatus(status int) bool {
	return status == 502 || status == 503 || status == 504
}

// SameHeight reports whether every answer that carries a height carries
// the same one. Answers without one are left out: cosmoguard's HTTP cache
// does not replay the height header unless a rule preserves it.
func SameHeight(rs ...Response) bool {
	var h int64
	for _, r := range rs {
		if r.Height == 0 {
			continue
		}
		if h != 0 && r.Height != h {
			return false
		}
		h = r.Height
	}
	return true
}

func decodeJSON(b []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	// Anything but whitespace after the value, even a stray ']', is not
	// one JSON document.
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return v, true
}

// BodyDiff describes where two different bodies first diverge: the JSON
// path when both parse as JSON, otherwise the byte offset.
func BodyDiff(node, guard []byte) string {
	a, okA := decodeJSON(node)
	b, okB := decodeJSON(guard)
	if okA && okB {
		if d := jsonDiff("$", a, b); d != "" {
			return d
		}
		return "same JSON value, different bytes (formatting or key order)"
	}
	i := 0
	for i < len(node) && i < len(guard) && node[i] == guard[i] {
		i++
	}
	return fmt.Sprintf("bodies differ at byte %d (node %d bytes, cosmoguard %d bytes): node=%q cosmoguard=%q",
		i, len(node), len(guard), excerpt(node, i), excerpt(guard, i))
}

func excerpt(b []byte, at int) string {
	end := min(at+40, len(b))
	return string(b[at:end])
}

func short(v any) string {
	b, _ := json.Marshal(v)
	s := string(b)
	if len(s) > 80 {
		s = s[:77] + "..."
	}
	return s
}

func jsonDiff(path string, a, b any) string {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok {
			return fmt.Sprintf("at %s: node=%s cosmoguard=%s", path, short(a), short(b))
		}
		for _, k := range unionKeys(av, bv) {
			x, inA := av[k]
			y, inB := bv[k]
			switch {
			case !inA:
				return fmt.Sprintf("at %s.%s: only cosmoguard has it", path, k)
			case !inB:
				return fmt.Sprintf("at %s.%s: only the node has it", path, k)
			}
			if d := jsonDiff(path+"."+k, x, y); d != "" {
				return d
			}
		}
		return ""
	case []any:
		bv, ok := b.([]any)
		if !ok {
			return fmt.Sprintf("at %s: node=%s cosmoguard=%s", path, short(a), short(b))
		}
		if len(av) != len(bv) {
			return fmt.Sprintf("at %s: node has %d items, cosmoguard %d", path, len(av), len(bv))
		}
		for i := range av {
			if d := jsonDiff(fmt.Sprintf("%s[%d]", path, i), av[i], bv[i]); d != "" {
				return d
			}
		}
		return ""
	default:
		if a != b {
			return fmt.Sprintf("at %s: node=%s cosmoguard=%s", path, short(a), short(b))
		}
		return ""
	}
}

// ShapeDiff compares only the structure of two JSON bodies: the same keys
// at every level and the same value kinds. Arrays are compared by their
// first element, since their length may legitimately differ. Non-JSON
// bodies fall back to byte equality.
func ShapeDiff(node, guard []byte) string {
	a, okA := decodeJSON(node)
	b, okB := decodeJSON(guard)
	if !okA || !okB {
		if bytes.Equal(node, guard) {
			return ""
		}
		return "not JSON on both sides: " + BodyDiff(node, guard)
	}
	return shapeDiff("$", a, b)
}

func kind(v any) string {
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case json.Number:
		return "number"
	case bool:
		return "bool"
	default:
		return "null"
	}
}

func shapeDiff(path string, a, b any) string {
	if ka, kb := kind(a), kind(b); ka != kb {
		return fmt.Sprintf("at %s: node has %s, cosmoguard %s", path, ka, kb)
	}
	switch av := a.(type) {
	case map[string]any:
		bv := b.(map[string]any)
		var missing []string
		for _, k := range unionKeys(av, bv) {
			_, inA := av[k]
			_, inB := bv[k]
			if inA != inB {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			return fmt.Sprintf("at %s: keys on one side only: %s", path, strings.Join(missing, ", "))
		}
		for _, k := range unionKeys(av, bv) {
			if d := shapeDiff(path+"."+k, av[k], bv[k]); d != "" {
				return d
			}
		}
	case []any:
		bv := b.([]any)
		if len(av) > 0 && len(bv) > 0 {
			return shapeDiff(path+"[0]", av[0], bv[0])
		}
	}
	return ""
}

func unionKeys(a, b map[string]any) []string {
	seen := make(map[string]bool, len(a)+len(b))
	var keys []string
	for _, m := range []map[string]any{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}
