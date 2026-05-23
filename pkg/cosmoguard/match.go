package cosmoguard

import (
	"fmt"
	"hash"
	"hash/fnv"
	"net"
	"net/http"
	"strings"

	"github.com/gobwas/glob"
)

// MatchTree is the v4 rule-matching primitive. A tree node is either a
// combinator (all/any/none of its children) or a leaf with one or more
// atomic predicates. A MatchTree is permitted to have BOTH child
// combinators AND leaf atoms; in that case ALL of them must hold (the leaf
// atoms are an implicit `all`).
//
// YAML shape:
//
//	match:
//	  all:
//	    - path: /block
//	    - query.height: "*"
//	  none:
//	    - header.x-debug: present
//	  any:
//	    - method: GET
//	    - method: POST
//
// Atom values support globs (gobwas/glob, with `/` and `.` as separators
// so wildcards cannot cross path or label boundaries). The special values
// `present` and `absent` test for header/query existence.
type MatchTree struct {
	All  []MatchTree `yaml:"all,omitempty"`
	Any  []MatchTree `yaml:"any,omitempty"`
	None []MatchTree `yaml:"none,omitempty"`

	// Leaf atoms. Empty = not asserted.
	Method   string            `yaml:"method,omitempty"`
	Methods  []string          `yaml:"methods,omitempty"`
	Path     string            `yaml:"path,omitempty"`
	Paths    []string          `yaml:"paths,omitempty"`
	Query    map[string]string `yaml:"query,omitempty"`
	Header   map[string]string `yaml:"header,omitempty"`
	SourceIP string            `yaml:"sourceIP,omitempty"`

	// Compiled state — populated by Compile, consumed by Match.
	compiled *compiledMatch `yaml:"-"`

	// legacyV3Query, when true, signals that this tree's Query map came
	// from a v3 flat `query:` field, NOT a v4 `match.query:` block. v3
	// values were always treated as globs (so the literal string "present"
	// in a v3 config meant "match the literal value 'present'"). v4
	// promotes "present"/"absent" to presence-check keywords. To keep
	// existing v3 configs behaving identically, we suppress the keyword
	// interpretation when the field arrived through the legacy path.
	legacyV3Query bool `yaml:"-"`
}

// compiledMatch is the parallel structure built by MatchTree.Compile. Read
// during Match. Separated from the YAML-facing struct so Match has no
// per-call allocations or string comparisons.
type compiledMatch struct {
	all  []*compiledMatch
	any  []*compiledMatch
	none []*compiledMatch

	methodSet      map[string]bool
	pathGlobs      []glob.Glob
	queryAtoms     map[string]predicate
	headerAtoms    map[string]predicate
	sourceIPNet    *net.IPNet
	sourceIPExact  net.IP
	isLeafAsserted bool // true if any leaf atom was actually configured
}

// predicate evaluates a single value-against-pattern atom. `present`/`absent`
// are encoded as predicates that ignore the value entirely.
type predicate struct {
	mode  predicateMode
	g     glob.Glob
	exact string
}

type predicateMode uint8

const (
	predicateGlob    predicateMode = iota // pattern must match the value
	predicatePresent                      // key must be present (any value)
	predicateAbsent                       // key must be absent
	predicateExact                        // exact equality
)

// Compile validates atoms, builds child compileds, and stores a ready-to-run
// compiledMatch on the tree. Idempotent.
func (m *MatchTree) Compile() error {
	c := &compiledMatch{}

	for i := range m.All {
		if err := m.All[i].Compile(); err != nil {
			return fmt.Errorf("all[%d]: %w", i, err)
		}
		c.all = append(c.all, m.All[i].compiled)
	}
	for i := range m.Any {
		if err := m.Any[i].Compile(); err != nil {
			return fmt.Errorf("any[%d]: %w", i, err)
		}
		c.any = append(c.any, m.Any[i].compiled)
	}
	for i := range m.None {
		if err := m.None[i].Compile(); err != nil {
			return fmt.Errorf("none[%d]: %w", i, err)
		}
		c.none = append(c.none, m.None[i].compiled)
	}

	// Leaf atoms.
	if m.Method != "" || len(m.Methods) > 0 {
		c.methodSet = map[string]bool{}
		if m.Method != "" {
			c.methodSet[strings.ToUpper(m.Method)] = true
		}
		for _, mm := range m.Methods {
			c.methodSet[strings.ToUpper(mm)] = true
		}
		c.isLeafAsserted = true
	}
	if m.Path != "" || len(m.Paths) > 0 {
		paths := m.Paths
		if m.Path != "" {
			paths = append([]string{m.Path}, paths...)
		}
		for _, p := range paths {
			g, err := glob.Compile(p, '/')
			if err != nil {
				return fmt.Errorf("path %q: %w", p, err)
			}
			c.pathGlobs = append(c.pathGlobs, g)
		}
		c.isLeafAsserted = true
	}
	if len(m.Query) > 0 {
		c.queryAtoms = map[string]predicate{}
		for k, v := range m.Query {
			p, err := compilePredicate(v, m.legacyV3Query)
			if err != nil {
				return fmt.Errorf("query[%q]: %w", k, err)
			}
			c.queryAtoms[k] = p
		}
		c.isLeafAsserted = true
	}
	if len(m.Header) > 0 {
		c.headerAtoms = map[string]predicate{}
		for k, v := range m.Header {
			p, err := compilePredicate(v, false)
			if err != nil {
				return fmt.Errorf("header[%q]: %w", k, err)
			}
			// Canonicalize header names so lookup matches Go's http.Header.
			c.headerAtoms[http.CanonicalHeaderKey(k)] = p
		}
		c.isLeafAsserted = true
	}
	if m.SourceIP != "" {
		// Accept either a single IP or a CIDR.
		if strings.Contains(m.SourceIP, "/") {
			_, ipnet, err := net.ParseCIDR(m.SourceIP)
			if err != nil {
				return fmt.Errorf("sourceIP %q: %w", m.SourceIP, err)
			}
			c.sourceIPNet = ipnet
		} else {
			ip := net.ParseIP(m.SourceIP)
			if ip == nil {
				return fmt.Errorf("sourceIP %q: invalid IP", m.SourceIP)
			}
			c.sourceIPExact = ip
		}
		c.isLeafAsserted = true
	}

	m.compiled = c
	return nil
}

// compilePredicate parses one atom value.
//
// Supported forms (v4 semantics):
//
//   - "present"             → predicatePresent
//   - "absent"              → predicateAbsent
//   - "<exact-string>"      → predicateExact (no glob chars)
//   - "<glob-pattern>"      → predicateGlob (any of *, ?, [)
//
// When legacyV3 is true (the v3 `query:` flat-syntax path), "present" and
// "absent" are NOT promoted to keywords — they're treated as ordinary
// glob/exact values so existing v3 configs keep their old semantics.
// Operators who want presence checks must opt in to the v4 `match.query`
// shape.
func compilePredicate(v string, legacyV3 bool) (predicate, error) {
	if !legacyV3 {
		switch v {
		case "present":
			return predicate{mode: predicatePresent}, nil
		case "absent":
			return predicate{mode: predicateAbsent}, nil
		}
	}
	if strings.ContainsAny(v, "*?[") {
		g, err := glob.Compile(v)
		if err != nil {
			return predicate{}, err
		}
		return predicate{mode: predicateGlob, g: g}, nil
	}
	return predicate{mode: predicateExact, exact: v}, nil
}

// IsEmpty reports whether the tree asserts anything at all. An empty tree
// matches every request — useful for "default action" rules that just need
// an action and no constraints.
func (m *MatchTree) IsEmpty() bool {
	if m == nil {
		return true
	}
	if m.compiled == nil {
		return false // not compiled yet; assume non-empty
	}
	c := m.compiled
	return !c.isLeafAsserted && len(c.all) == 0 && len(c.any) == 0 && len(c.none) == 0
}

// MatchRequest evaluates the compiled tree against an HTTP request. The
// rule matches if and only if every All child matches, at least one Any
// child matches (or Any is empty), no None child matches, AND every leaf
// atom holds.
func (m *MatchTree) MatchRequest(r *http.Request, sourceIP net.IP) bool {
	if m == nil || m.compiled == nil {
		return true
	}
	return m.compiled.matchRequest(r, sourceIP)
}

func (c *compiledMatch) matchRequest(r *http.Request, sourceIP net.IP) bool {
	for _, child := range c.all {
		if !child.matchRequest(r, sourceIP) {
			return false
		}
	}
	if len(c.any) > 0 {
		hit := false
		for _, child := range c.any {
			if child.matchRequest(r, sourceIP) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	for _, child := range c.none {
		if child.matchRequest(r, sourceIP) {
			return false
		}
	}
	if !c.isLeafAsserted {
		return true
	}
	if c.methodSet != nil && !c.methodSet[strings.ToUpper(r.Method)] {
		return false
	}
	if len(c.pathGlobs) > 0 {
		hit := false
		for _, g := range c.pathGlobs {
			if g.Match(r.URL.Path) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(c.queryAtoms) > 0 {
		q := r.URL.Query()
		for k, p := range c.queryAtoms {
			vals, has := q[k]
			val := ""
			if has && len(vals) > 0 {
				val = vals[0]
			}
			if !p.eval(val, has && len(vals) > 0 && val != "") {
				return false
			}
		}
	}
	if len(c.headerAtoms) > 0 {
		for k, p := range c.headerAtoms {
			val := r.Header.Get(k)
			has := val != ""
			if !p.eval(val, has) {
				return false
			}
		}
	}
	if c.sourceIPNet != nil {
		if sourceIP == nil || !c.sourceIPNet.Contains(sourceIP) {
			return false
		}
	} else if c.sourceIPExact != nil {
		if sourceIP == nil || !sourceIP.Equal(c.sourceIPExact) {
			return false
		}
	}
	return true
}

// eval runs the predicate against a (value, presence) pair.
func (p predicate) eval(value string, present bool) bool {
	switch p.mode {
	case predicatePresent:
		return present
	case predicateAbsent:
		return !present
	case predicateExact:
		return present && value == p.exact
	case predicateGlob:
		if !present {
			return false
		}
		return p.g.Match(value)
	}
	return false
}

// Fingerprint computes a stable 64-bit hash of the tree shape, mixed into
// HttpRule.Fingerprint so per-rule cache namespaces stay correct when rules
// migrate from v3 flat syntax to v4 match trees.
func (m *MatchTree) Fingerprint() uint64 {
	h := fnv.New64a()
	m.writeFingerprint(h)
	return h.Sum64()
}

func (m *MatchTree) writeFingerprint(h hash.Hash64) {
	if m == nil {
		writeFingerprintStr(h, "<nil>")
		return
	}
	writeFingerprintStr(h, "all:")
	writeFingerprintInt(h, len(m.All))
	for i := range m.All {
		m.All[i].writeFingerprint(h)
	}
	writeFingerprintStr(h, "any:")
	writeFingerprintInt(h, len(m.Any))
	for i := range m.Any {
		m.Any[i].writeFingerprint(h)
	}
	writeFingerprintStr(h, "none:")
	writeFingerprintInt(h, len(m.None))
	for i := range m.None {
		m.None[i].writeFingerprint(h)
	}
	writeFingerprintStr(h, "method:")
	writeFingerprintStr(h, m.Method)
	writeFingerprintStrSlice(h, m.Methods)
	writeFingerprintStr(h, "path:")
	writeFingerprintStr(h, m.Path)
	writeFingerprintStrSlice(h, m.Paths)
	writeFingerprintStr(h, "query:")
	writeFingerprintStrMap(h, m.Query)
	writeFingerprintStr(h, "header:")
	writeFingerprintStrMap(h, m.Header)
	writeFingerprintStr(h, "sourceIP:")
	writeFingerprintStr(h, m.SourceIP)
}
