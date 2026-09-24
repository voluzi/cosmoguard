// Package compat compares a Cosmos node's responses with the responses
// cosmoguard gives for the same requests. It backs cmd/cosmoguard-compat.
package compat

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
)

// Class is the verdict for one endpoint.
type Class string

const (
	// Identical means every cosmoguard response matched the node's.
	Identical Class = "identical"
	// Differs means cosmoguard answered, but not as the node did.
	Differs Class = "differs"
	// Denied means cosmoguard refused a request the node answered.
	Denied Class = "denied"
	// Failed means the node itself did not answer, so nothing was compared.
	Failed Class = "failed"
	// Unstable means the endpoint cannot be pinned to one height and the
	// two sides kept answering at different heights.
	Unstable Class = "unstable"
	// Skipped means the request could not be built (e.g. a path parameter
	// with no live value).
	Skipped Class = "skipped"
)

var classOrder = []Class{Identical, Differs, Denied, Failed, Unstable, Skipped}

// Result is the verdict for one endpoint.
type Result struct {
	Protocol string `json:"protocol"`
	Name     string `json:"name"`
	Class    Class  `json:"class"`
	Detail   string `json:"detail,omitempty"`
}

// Report collects results; it is safe for concurrent use.
type Report struct {
	Chain   string   `json:"chain"`
	Height  int64    `json:"height"`
	Results []Result `json:"results"`

	mu sync.Mutex
}

// Add records one result.
func (r *Report) Add(res Result) {
	r.mu.Lock()
	r.Results = append(r.Results, res)
	r.mu.Unlock()
}

// HasDifferences reports whether any endpoint differs.
func (r *Report) HasDifferences() bool {
	for _, res := range r.Results {
		if res.Class == Differs {
			return true
		}
	}
	return false
}

func (r *Report) sorted() []Result {
	out := append([]Result(nil), r.Results...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Protocol != out[j].Protocol {
			return out[i].Protocol < out[j].Protocol
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// WriteSummary prints per-protocol counts, then every result that is not
// identical or skipped. Skipped results are counted; their reasons are in
// the JSON report.
func (r *Report) WriteSummary(w io.Writer) {
	results := r.sorted()
	counts := map[string]map[Class]int{}
	var protocols []string
	for _, res := range results {
		if counts[res.Protocol] == nil {
			counts[res.Protocol] = map[Class]int{}
			protocols = append(protocols, res.Protocol)
		}
		counts[res.Protocol][res.Class]++
	}
	fmt.Fprintf(w, "chain %s, pinned height %d\n\n", r.Chain, r.Height)
	fmt.Fprintf(w, "%-8s", "")
	for _, c := range classOrder {
		fmt.Fprintf(w, "%10s", c)
	}
	fmt.Fprintln(w)
	for _, p := range protocols {
		fmt.Fprintf(w, "%-8s", p)
		for _, c := range classOrder {
			fmt.Fprintf(w, "%10d", counts[p][c])
		}
		fmt.Fprintln(w)
	}
	for _, c := range []Class{Differs, Denied, Failed, Unstable} {
		var lines []string
		for _, res := range results {
			if res.Class == c {
				lines = append(lines, fmt.Sprintf("  %s %s: %s", res.Protocol, res.Name, oneLine(res.Detail)))
			}
		}
		if len(lines) > 0 {
			fmt.Fprintf(w, "\n%s (%d):\n%s\n", strings.ToUpper(string(c)), len(lines), strings.Join(lines, "\n"))
		}
	}
}

// oneLine keeps a summary entry to one short line; the JSON report has
// the full detail.
func oneLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 200 {
		s = s[:197] + "..."
	}
	return s
}

// WriteJSON writes the full report, including skip reasons, to path.
func (r *Report) WriteJSON(path string) error {
	out := struct {
		Chain   string   `json:"chain"`
		Height  int64    `json:"height"`
		Results []Result `json:"results"`
	}{r.Chain, r.Height, r.sorted()}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
