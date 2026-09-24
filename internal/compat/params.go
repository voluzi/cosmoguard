package compat

import (
	"net/url"
	"strings"
)

// Params holds live values for request fields. Keys are either request
// field names (as in Cosmos SDK query requests and google.api.http
// templates) or the canonical keys aliases point at, so one discovered
// account address serves every field that takes one.
type Params map[string]string

// aliases maps request field names to the canonical key the value is
// discovered under.
var aliases = map[string]string{
	"address":               "account",
	"account":               "account",
	"account_address":       "account",
	"address_string":        "account",
	"delegator_addr":        "account",
	"delegator_address":     "account",
	"owner":                 "account",
	"granter":               "account",
	"grantee":               "account",
	"voter":                 "account",
	"depositor":             "account",
	"validator_addr":        "validator",
	"validator_address":     "validator",
	"validator_src_address": "validator",
	"validator_dst_address": "validator",
	"src_validator_addr":    "validator",
	"dst_validator_addr":    "validator",
	"operator_address":      "validator",
	"cons_address":          "consensus",
	"consensus_address":     "consensus",
	"denom":                 "denom",
	"proposal_id":           "proposal_id",
	"height":                "height",
	"block_height":          "height",
	"hash":                  "tx_hash",
	"channel_id":            "channel_id",
	"port_id":               "port_id",
	"connection_id":         "connection_id",
	"client_id":             "client_id",
}

// Lookup returns the value for a request field, trying the full (possibly
// dotted) name and then its last segment, each first as a key of its own
// and then through aliases.
func (p Params) Lookup(field string) (string, bool) {
	names := []string{field}
	if i := strings.LastIndexByte(field, '.'); i >= 0 {
		names = append(names, field[i+1:])
	}
	for _, n := range names {
		if v := p[n]; v != "" {
			return v, true
		}
		if key, ok := aliases[n]; ok {
			if v := p[key]; v != "" {
				return v, true
			}
		}
	}
	return "", false
}

// FillPath substitutes the {field} and {field=pattern} variables of a
// google.api.http path template. It returns the missing field names when
// a variable has no live value.
func FillPath(tmpl string, p Params) (string, []string) {
	var out strings.Builder
	var missing []string
	for {
		open := strings.IndexByte(tmpl, '{')
		if open < 0 {
			out.WriteString(tmpl)
			break
		}
		end := strings.IndexByte(tmpl[open:], '}')
		if end < 0 {
			out.WriteString(tmpl)
			break
		}
		out.WriteString(tmpl[:open])
		field, pattern, _ := strings.Cut(tmpl[open+1:open+end], "=")
		v, ok := p.Lookup(field)
		switch {
		case !ok:
			missing = append(missing, field)
		case strings.Contains(pattern, "**") || strings.Contains(pattern, "/"):
			// Multi-segment variables ({denom=**}, {name=a/*/b/*}) keep
			// their slashes; each segment is escaped on its own.
			segs := strings.Split(v, "/")
			for i, s := range segs {
				segs[i] = url.PathEscape(s)
			}
			out.WriteString(strings.Join(segs, "/"))
		default:
			out.WriteString(url.PathEscape(v))
		}
		tmpl = tmpl[open+end+1:]
	}
	return out.String(), missing
}
