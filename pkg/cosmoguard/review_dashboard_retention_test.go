package cosmoguard

import (
	"bufio"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// oversizedValue is the adversarial input every retention test feeds:
// a 1 MiB string, the scale a single request can reach under the
// default 5 MiB body cap.
func oversizedValue(fill string) string { return strings.Repeat(fill, 1<<20) }

// unmatchedComponents returns the client-controlled halves of every
// retained unmatched key, undoing the (method, path) encoding.
func unmatchedComponents(d *dashboardObservability, section string) []string {
	d.unmatchedMu.Lock()
	c := d.unmatched[section]
	d.unmatchedMu.Unlock()
	var out []string
	for _, e := range c.Snapshot() {
		method, path := splitUnmatchedKey(e.Key)
		out = append(out, method)
		if path != "" {
			out = append(out, path)
		}
	}
	return out
}

// cardinalityComponents returns the client-controlled request keys of
// every retained cardinality entry; the rule tag half is operator
// configuration and is deliberately left out.
func cardinalityComponents(d *dashboardObservability, section string) []string {
	d.cardinalityMu.Lock()
	c := d.cardinality[section]
	d.cardinalityMu.Unlock()
	var out []string
	for _, e := range c.Snapshot() {
		_, requestKey := splitCardinalityKey(e.Key)
		out = append(out, requestKey)
	}
	return out
}

func deniedComponents(d *dashboardObservability) []string {
	var out []string
	for _, rec := range d.denied.Snapshot() {
		out = append(out, rec.SourceIP, rec.Method, rec.Path)
	}
	return out
}

// TestReviewDashboardRetainsUnboundedMethodStrings is the regression
// test from issue #60. Its parser half is adapted to the parse-time
// rejection the same issue asks for: a 1 MiB method is no longer a
// valid request at all.
func TestReviewDashboardRetainsUnboundedMethodStrings(t *testing.T) {
	huge := oversizedValue("a")
	msg, batch, err := ParseJsonRpcMessage([]byte(`{"jsonrpc":"2.0","id":1,"method":"` + huge + `"}`))
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.Nil(t, msg)
	require.Nil(t, batch)

	d := newDashboardObservability()
	for i := 0; i < 64; i++ {
		d.RecordUnmatched("rpc.jsonrpc", huge+strconv.Itoa(i), "")
	}
	d.unmatchedMu.Lock()
	c := d.unmatched["rpc.jsonrpc"]
	d.unmatchedMu.Unlock()
	total := 0
	for _, e := range c.Snapshot() {
		total += len(e.Key)
	}
	t.Logf("retained key bytes after 64 unmatched records: %d", total)
	require.Less(t, total, 1<<20, "unmatched keys must be truncated before retention")
}

// TestDashboardRetentionBoundsEverySink is the class-level assertion:
// every sink that retains a client-controlled string bounds it. A new
// sink added without a bound fails here as soon as it is listed, which
// is the review question this test exists to ask.
func TestDashboardRetentionBoundsEverySink(t *testing.T) {
	huge := oversizedValue("z")

	tests := []struct {
		name string
		feed func(t *testing.T) []string
	}{
		{
			name: "unmatched",
			feed: func(t *testing.T) []string {
				d := newDashboardObservability()
				d.RecordUnmatched("lcd", huge, huge)
				return unmatchedComponents(d, "lcd")
			},
		},
		{
			name: "denied",
			feed: func(t *testing.T) []string {
				d := newDashboardObservability()
				d.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", SourceIP: huge, Method: huge, Path: huge})
				return deniedComponents(d)
			},
		},
		{
			name: "cardinality",
			feed: func(t *testing.T) []string {
				d := newDashboardObservability()
				d.RecordCardinality("lcd", "rule-1", huge)
				return cardinalityComponents(d, "lcd")
			},
		},
		{
			name: "request log",
			feed: func(t *testing.T) []string {
				rl := enabledLog(t, 100)
				rl.Record(RequestLogEntry{
					Section: "lcd", Status: 200,
					Method: huge, Path: huge, Query: huge, SourceIP: huge,
				})
				var out []string
				for _, e := range rl.Snapshot(nil, 10) {
					out = append(out, e.Method, e.Path, e.Query, e.SourceIP)
				}
				return out
			},
		},
		{
			name: "restore from peer snapshot",
			feed: func(t *testing.T) []string {
				blob, err := msgpack.Marshal(&observabilitySnapshot{
					Unmatched:   map[string][]topNEntrySnap{"lcd": {{Key: unmatchedKey(huge, huge), Count: 3}}},
					Cardinality: map[string][]topNEntrySnap{"lcd": {{Key: cardinalityKey("rule-1", huge), Count: 3}}},
					Denied:      []DenyRecord{{Section: "lcd", SourceIP: huge, Method: huge, Path: huge}},
				})
				require.NoError(t, err)

				d := newDashboardObservability()
				require.NoError(t, d.Restore(blob))
				out := unmatchedComponents(d, "lcd")
				out = append(out, cardinalityComponents(d, "lcd")...)
				return append(out, deniedComponents(d)...)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retained := tt.feed(t)
			require.NotEmpty(t, retained, "sink retained nothing — the test no longer exercises it")
			total := 0
			for _, s := range retained {
				require.LessOrEqual(t, len(s), maxRetainedPathBytes, "retained string is unbounded: %d bytes", len(s))
				require.Contains(t, s, retainedTruncationMarker, "truncation must be visible to the operator")
				require.True(t, utf8.ValidString(s), "truncation split a rune")
				total += len(s)
			}
			t.Logf("retained bytes: %d", total)
			require.Less(t, total, 1<<20)
		})
	}
}

// TestRetainedStringsDoNotAliasTheRequestBuffer covers what a length
// bound alone cannot. net/http hands out Method, Path and RawQuery as
// slices of the single buffer holding the whole request line, so a
// short value retained as-is keeps the entire request alive — a
// three-byte "GET" pinning a 512 KiB query string bounds nothing.
func TestRetainedStringsDoNotAliasTheRequestBuffer(t *testing.T) {
	raw := "GET /?q=" + strings.Repeat("x", 512<<10) + " HTTP/1.1\r\nHost: example.com\r\n\r\n"
	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
	require.NoError(t, err)

	// The request line is one allocation; every field sliced out of it
	// points somewhere inside that range.
	base := uintptr(unsafe.Pointer(unsafe.StringData(r.Method)))
	pinsRequestLine := func(s string) bool {
		if s == "" {
			return false
		}
		p := uintptr(unsafe.Pointer(unsafe.StringData(s)))
		return p >= base && p < base+uintptr(len(raw))
	}

	// Precondition: the fields really are slices of that one buffer, so
	// what follows tests the fix rather than net/http's internals.
	require.True(t, pinsRequestLine(r.RequestURI) && pinsRequestLine(r.URL.Path),
		"net/http no longer slices the request line — rewrite this test, don't delete it")

	d := newDashboardObservability()
	d.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", SourceIP: r.Method, Method: r.Method, Path: r.URL.Path})
	d.RecordUnmatched("lcd", r.Method, r.URL.Path)
	d.RecordCardinality("lcd", "rule-1", r.URL.Path)

	rl := enabledLog(t, 10)
	rl.Record(RequestLogEntry{
		Section: "lcd", Status: 200,
		Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, SourceIP: r.Method,
	})

	retained := append(deniedComponents(d), unmatchedComponents(d, "lcd")...)
	retained = append(retained, cardinalityComponents(d, "lcd")...)
	for _, e := range rl.Snapshot(nil, 10) {
		retained = append(retained, e.Method, e.Path, e.Query, e.SourceIP)
	}

	require.NotEmpty(t, retained)
	for _, s := range retained {
		require.False(t, pinsRequestLine(s),
			"a %d-byte retained value still pins the %d-byte request line", len(s), len(raw))
	}
}

// TestRetentionStringFieldsAreClassified pins the string fields of the
// two retained records. Every one is either client-controlled (bounded
// at the sink) or operator-supplied / authenticated (kept intact); a
// new field fails this test until somebody decides which it is.
func TestRetentionStringFieldsAreClassified(t *testing.T) {
	stringFields := func(v any) []string {
		rt := reflect.TypeOf(v)
		var out []string
		for i := 0; i < rt.NumField(); i++ {
			if rt.Field(i).Type.Kind() == reflect.String {
				out = append(out, rt.Field(i).Name)
			}
		}
		return out
	}

	tests := []struct {
		name             string
		value            any
		clientControlled []string
		trusted          []string
	}{
		{
			name:             "DenyRecord",
			value:            DenyRecord{},
			clientControlled: []string{"SourceIP", "Method", "Path"},
			trusted:          []string{"Section", "Reason", "RuleTag"},
		},
		{
			name:             "RequestLogEntry",
			value:            RequestLogEntry{},
			clientControlled: []string{"Method", "Path", "Query", "SourceIP"},
			// Identity comes from a validated JWT claim or a configured
			// API-key alias, so it is not an unauthenticated vector.
			trusted: []string{"Section", "CacheState", "Action", "RuleTag", "Identity", "Upstream"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classified := append(append([]string{}, tt.clientControlled...), tt.trusted...)
			require.ElementsMatch(t, classified, stringFields(tt.value),
				"a new string field must be classified as client-controlled (bound it) or trusted")
		})
	}
}

// TestRecordCardinalityKeepsDistinctLongKeysDistinct guards the reason
// the counter exists: it measures how many distinct cache keys a rule
// writes. Plain truncation would fold every long key into one entry and
// report the key explosion as a single key.
func TestRecordCardinalityKeepsDistinctLongKeysDistinct(t *testing.T) {
	prefix := strings.Repeat("p", 1024)
	suffix := strings.Repeat("s", 1024)
	d := newDashboardObservability()
	for i := 0; i < 50; i++ {
		d.RecordCardinality("lcd", "rule-1", prefix+strconv.Itoa(i)+suffix)
	}
	// The same key again must not count twice.
	d.RecordCardinality("lcd", "rule-1", prefix+"0"+suffix)

	d.cardinalityMu.Lock()
	c := d.cardinality["lcd"]
	d.cardinalityMu.Unlock()
	require.Len(t, c.Snapshot(), 50)

	for _, s := range cardinalityComponents(d, "lcd") {
		require.Contains(t, s, retainedTruncationMarker)
	}
}

func TestBoundRetained(t *testing.T) {
	t.Run("passes through at the limit", func(t *testing.T) {
		exact := strings.Repeat("a", maxRetainedPathBytes)
		require.Equal(t, exact, boundRetained(exact, maxRetainedPathBytes))
	})

	t.Run("bounds and marks anything longer", func(t *testing.T) {
		got := boundRetained(strings.Repeat("a", maxRetainedPathBytes+1), maxRetainedPathBytes)
		require.LessOrEqual(t, len(got), maxRetainedPathBytes)
		require.True(t, strings.HasSuffix(got, retainedTruncationMarker))
	})

	t.Run("never splits a rune", func(t *testing.T) {
		// The cut has to land mid-rune for the back-off to do anything:
		// with a 3-byte rune, only a limit whose cut is not a multiple
		// of 3 exercises it — the other two residues would pass even
		// with the back-off deleted.
		for _, max := range []int{maxRetainedMethodBytes, maxRetainedMethodBytes + 1, maxRetainedMethodBytes + 2} {
			got := boundRetained(strings.Repeat("€", 1<<10), max)
			require.True(t, utf8.ValidString(got), "limit %d cut a rune in half", max)
			require.LessOrEqual(t, len(got), max)
			require.True(t, strings.HasSuffix(got, retainedTruncationMarker))
		}
	})

	t.Run("never exceeds a limit too small for the marker", func(t *testing.T) {
		require.Equal(t, retainedTruncationMarker, boundRetained("abcdef", len(retainedTruncationMarker)))
		require.Empty(t, boundRetained("abcdef", len(retainedTruncationMarker)-1))
	})

	t.Run("is idempotent", func(t *testing.T) {
		once := boundRetained(strings.Repeat("a", 1<<20), maxRetainedMethodBytes)
		require.Equal(t, once, boundRetained(once, maxRetainedMethodBytes))
	})
}

// TestRestoreReappliesBounds covers the mixed-version rollout: a peer
// still running the previous release sends unbounded keys, and a blob
// written by this release must survive a round trip untouched.
func TestRestoreReappliesBounds(t *testing.T) {
	huge := oversizedValue("q")

	t.Run("bounds an old-format blob", func(t *testing.T) {
		blob, err := msgpack.Marshal(&observabilitySnapshot{
			Unmatched: map[string][]topNEntrySnap{"lcd": {{Key: unmatchedKey("GET", huge), Count: 7}}},
		})
		require.NoError(t, err)

		d := newDashboardObservability()
		require.NoError(t, d.Restore(blob))

		d.unmatchedMu.Lock()
		entries := d.unmatched["lcd"].Snapshot()
		d.unmatchedMu.Unlock()
		require.Len(t, entries, 1)
		require.EqualValues(t, 7, entries[0].Count, "counts must survive the rewrite")
		method, path := splitUnmatchedKey(entries[0].Key)
		require.Equal(t, "GET", method)
		require.LessOrEqual(t, len(path), maxRetainedPathBytes)
	})

	t.Run("leaves an already-bounded blob byte-identical", func(t *testing.T) {
		src := newDashboardObservability()
		src.RecordUnmatched("lcd", "GET", "/cosmos/bank/v1beta1/balances")
		src.RecordCardinality("lcd", "rule-1", "GET /status?height=1")
		src.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", SourceIP: "10.0.0.1", Method: "GET", Path: "/denied"})
		blob, err := src.Snapshot()
		require.NoError(t, err)

		dst := newDashboardObservability()
		require.NoError(t, dst.Restore(blob))
		round, err := dst.Snapshot()
		require.NoError(t, err)
		require.Equal(t, blob, round)
	})
}
