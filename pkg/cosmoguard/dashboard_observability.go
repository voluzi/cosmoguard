package cosmoguard

import (
	"container/list"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// dashboard_observability.go holds the in-memory observability surfaces
// the read-only dashboard renders alongside its existing live state.
// Two primitives drive everything:
//
//   - TopNCounter is a bounded LRU of string-keyed counters used to
//     track unmatched endpoints per section (and, in commit B, per-
//     rule cache key cardinality).
//   - RingBuffer[T] is a fixed-size FIFO used for the denied tail
//     (and, in commit B, the discovery refresh log).
//
// Both bound their memory at construction so an attacker driving
// arbitrary keys can't blow up the process. Nothing is persisted
// across restarts — same in-memory model as the existing rate
// limiter. The dashboard listener is auth-gated so the data here
// (source IPs, paths, methods) is not exposed publicly.

// ReloadStatus is the JSON shape returned by /api/v1/reload-status —
// the outcome of the most recent hot-reload attempt. Sections maps
// each rule section name (lcd, rpc, …) to its rule-count delta.
type ReloadStatus struct {
	TimestampMs int64                    `json:"timestamp_ms"`
	Success     bool                     `json:"success"`
	Error       string                   `json:"error,omitempty"`
	Sections    map[string]ReloadSection `json:"sections"`
}

// ReloadSection is the per-section delta surfaced inside ReloadStatus.
//
// Before/After are the rule counts on each side — they catch additions
// and removals at a glance. Added/Removed/Modified are derived from a
// multiset diff of per-rule fingerprints (see reloadDelta + rules.go
// httpRuleFingerprint and friends), so an in-place edit — say, bumping
// a cache TTL on an existing rule — still registers as Modified=1 even
// though Before == After. Without that field a TTL bump would look
// identical to a no-op reload on the dashboard, which is the exact
// confusion this struct's earlier count-only shape produced.
type ReloadSection struct {
	Before   int `json:"before"`
	After    int `json:"after"`
	Added    int `json:"added,omitempty"`
	Removed  int `json:"removed,omitempty"`
	Modified int `json:"modified,omitempty"`
}

// DiscoveryEvent is one entry in the discovery refresh log. Type is
// one of "add", "remove", "error", "tick" — "tick" carries the full
// resolved set on each refresh, the others mark deltas.
type DiscoveryEvent struct {
	TimestampMs int64    `json:"timestamp_ms"`
	Template    string   `json:"template"`
	Type        string   `json:"type"`
	Upstream    string   `json:"upstream,omitempty"`
	IP          string   `json:"ip,omitempty"`
	Resolved    []string `json:"resolved,omitempty"`
	Error       string   `json:"error,omitempty"`
}

// DenyRecord is one entry in the recent-denials ring buffer. Source
// IP is full (matches the existing per-request log lines); operators
// correlate against their own logs which already carry full IPs.
type DenyRecord struct {
	TimestampMs int64  `json:"timestamp_ms"`
	Section     string `json:"section"`
	Reason      string `json:"reason"` // "rule", "default", "rate_limit", "auth"
	SourceIP    string `json:"source_ip"`
	Method      string `json:"method"`         // HTTP verb, JSON-RPC method, gRPC method
	Path        string `json:"path,omitempty"` // empty for jsonrpc/grpc/ws
	RuleTag     string `json:"rule_tag,omitempty"`
}

// UnmatchedEntry is the JSON shape returned by /api/v1/unmatched for
// each (section, method, path) tuple that fell through to the default
// action.
type UnmatchedEntry struct {
	Section   string `json:"section"`
	Method    string `json:"method"`
	Path      string `json:"path,omitempty"`
	Count     uint64 `json:"count"`
	FirstSeen int64  `json:"first_seen_ms"`
	LastSeen  int64  `json:"last_seen_ms"`
}

// TopNEntry is the result of TopNCounter.Snapshot — the (key, count,
// timestamps) tuple for one tracked key.
type TopNEntry struct {
	Key       string
	Count     uint64
	FirstSeen time.Time
	LastSeen  time.Time
}

// TopNCounter is a bounded LRU of string-keyed counters. New keys
// evict the least-recently-observed entry when the counter is at
// capacity. Observe + Snapshot are O(1) amortized; thread-safe.
//
// The point isn't to track every key ever seen — it's to keep the top
// active keys with rough counts. An operator looking at the dashboard
// wants to see "which paths are being hit that no rule matched?", not
// a complete historical log.
type TopNCounter struct {
	mu    sync.Mutex
	cap   int
	items map[string]*tncItem
	// lru.Front() is most-recently-observed; lru.Back() is the next
	// eviction candidate. Element.Value points back to the tncItem so
	// MoveToFront doesn't require a key lookup.
	lru *list.List
}

type tncItem struct {
	key       string
	count     uint64
	firstSeen time.Time
	lastSeen  time.Time
	elem      *list.Element
}

// NewTopNCounter returns a TopNCounter that holds at most cap distinct
// keys. cap <= 0 is normalized to 1 — a counter with no capacity is
// useless but should never panic on Observe.
func NewTopNCounter(cap int) *TopNCounter {
	if cap <= 0 {
		cap = 1
	}
	return &TopNCounter{
		cap:   cap,
		items: make(map[string]*tncItem, cap),
		lru:   list.New(),
	}
}

// Observe increments the counter for key, refreshing its LRU position.
// Inserts at the front when key is new; evicts the LRU back when over
// capacity. Nil-safe so instrumentation callers can dereference a
// possibly-nil counter without guarding.
func (c *TopNCounter) Observe(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if it, ok := c.items[key]; ok {
		it.count++
		it.lastSeen = now
		c.lru.MoveToFront(it.elem)
		return
	}
	if len(c.items) >= c.cap {
		oldest := c.lru.Back()
		if oldest != nil {
			oldKey := oldest.Value.(*tncItem).key
			c.lru.Remove(oldest)
			delete(c.items, oldKey)
		}
	}
	it := &tncItem{key: key, count: 1, firstSeen: now, lastSeen: now}
	it.elem = c.lru.PushFront(it)
	c.items[key] = it
}

// Snapshot returns all tracked entries sorted by count descending.
// Callers MUST treat the returned slice as read-only — the counter
// will reuse internal allocations on subsequent Observe calls but
// returns a fresh slice each call so callers can iterate without
// holding the lock.
func (c *TopNCounter) Snapshot() []TopNEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	out := make([]TopNEntry, 0, len(c.items))
	for _, it := range c.items {
		out = append(out, TopNEntry{
			Key:       it.key,
			Count:     it.count,
			FirstSeen: it.firstSeen,
			LastSeen:  it.lastSeen,
		})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out
}

// RingBuffer is a fixed-size FIFO of arbitrary records. Push is O(1);
// Snapshot returns a copy newest-first. Bounded memory means an
// attacker hammering the deny path can't grow the buffer beyond cap
// entries.
type RingBuffer[T any] struct {
	mu   sync.Mutex
	cap  int
	head int // next write index
	full bool
	buf  []T
}

// NewRingBuffer returns a ring buffer that holds at most cap entries.
// cap <= 0 is normalized to 1.
func NewRingBuffer[T any](cap int) *RingBuffer[T] {
	if cap <= 0 {
		cap = 1
	}
	return &RingBuffer[T]{cap: cap, buf: make([]T, cap)}
}

// Push appends v, overwriting the oldest entry once the buffer is
// full. Nil-safe.
func (r *RingBuffer[T]) Push(v T) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.head] = v
	r.head = (r.head + 1) % r.cap
	if r.head == 0 {
		r.full = true
	}
}

// Snapshot returns a copy of every entry in the buffer, ordered
// newest-first.
func (r *RingBuffer[T]) Snapshot() []T {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	if r.full {
		n = r.cap
	} else {
		n = r.head
	}
	out := make([]T, 0, n)
	for i := 0; i < n; i++ {
		idx := (r.head - 1 - i + r.cap) % r.cap
		out = append(out, r.buf[idx])
	}
	return out
}

// dashboardObservability bundles the per-CosmoGuard observability
// state passed to every proxy. Nil-safe at every layer so callers
// from a future codepath that skips dashboard wiring can't panic.
type dashboardObservability struct {
	unmatchedMu sync.Mutex
	unmatched   map[string]*TopNCounter

	denied *RingBuffer[DenyRecord]

	// cardinality tracks the distinct cache keys written per
	// (section, rule_tag). The point isn't to count every entry
	// ever — it's to spot a single rule blowing up Redis with a
	// misconfigured key derivation.
	cardinalityMu sync.Mutex
	cardinality   map[string]*TopNCounter

	// lastReload is the outcome of the most recent hot-reload
	// attempt. Set on every tryReload (success or failure) so the
	// dashboard pill can render "amber: last reload Nh ago" or
	// "red: reload errored".
	reloadMu   sync.Mutex
	lastReload *ReloadStatus

	// discoveryLog is the bounded refresh log for the DNS / K8s
	// discovery reconciler. Newest-first via RingBuffer.Snapshot.
	discoveryLog *RingBuffer[DiscoveryEvent]
}

// Bounded sizes for the in-memory observability state. TopNCounter
// holds the 256 most-recently-observed unmatched (method, path)
// tuples per section; RingBuffer keeps the last 64 deny records.
// Both are intentionally small — the operator workflow is "look at
// what's hot, fix it" not "audit every event ever".
const (
	defaultUnmatchedCap   = 256
	defaultDeniedCap      = 64
	defaultCardinalityCap = 4096
	defaultDiscoveryCap   = 256
)

func newDashboardObservability() *dashboardObservability {
	return &dashboardObservability{
		unmatched:    make(map[string]*TopNCounter),
		denied:       NewRingBuffer[DenyRecord](defaultDeniedCap),
		cardinality:  make(map[string]*TopNCounter),
		discoveryLog: NewRingBuffer[DiscoveryEvent](defaultDiscoveryCap),
	}
}

// RecordUnmatched bumps the (method, path) counter for the named
// section. Sections are passed from cosmoguard's own proxies (not
// from request data) so the underlying map can lazy-allocate per
// section without unbounded growth. Path is permitted to be empty
// (JSON-RPC / gRPC / WS protocols don't carry one).
func (d *dashboardObservability) RecordUnmatched(section, method, path string) {
	if d == nil {
		return
	}
	d.unmatchedMu.Lock()
	c, ok := d.unmatched[section]
	if !ok {
		c = NewTopNCounter(defaultUnmatchedCap)
		d.unmatched[section] = c
	}
	d.unmatchedMu.Unlock()
	c.Observe(unmatchedKey(method, path))
}

// RecordDeny pushes a deny record into the ring buffer. TimestampMs
// is filled in when zero.
func (d *dashboardObservability) RecordDeny(rec DenyRecord) {
	if d == nil {
		return
	}
	if rec.TimestampMs == 0 {
		rec.TimestampMs = time.Now().UnixMilli()
	}
	d.denied.Push(rec)
}

// RecordCardinality bumps the distinct-keys counter for the
// (section, ruleTag) pair. requestKey is the path / JSON-RPC method /
// gRPC method that's about to be inserted into the cache — what we
// actually care about is the distinct count, not the contents, but
// keeping the key around lets the dashboard surface a sample of the
// hot keys when the count crosses a threshold.
func (d *dashboardObservability) RecordCardinality(section, ruleTag, requestKey string) {
	if d == nil {
		return
	}
	d.cardinalityMu.Lock()
	c, ok := d.cardinality[section]
	if !ok {
		c = NewTopNCounter(defaultCardinalityCap)
		d.cardinality[section] = c
	}
	d.cardinalityMu.Unlock()
	c.Observe(cardinalityKey(ruleTag, requestKey))
}

// RecordReload stamps the latest hot-reload outcome on the
// observability sink. Sections is the before/after rule-count delta
// per section; err is the empty string on success.
func (d *dashboardObservability) RecordReload(success bool, errStr string, sections map[string]ReloadSection) {
	if d == nil {
		return
	}
	d.reloadMu.Lock()
	d.lastReload = &ReloadStatus{
		TimestampMs: time.Now().UnixMilli(),
		Success:     success,
		Error:       errStr,
		Sections:    sections,
	}
	d.reloadMu.Unlock()
}

// RecordDiscovery appends a discovery refresh event to the ring
// buffer. TimestampMs is filled in when zero.
func (d *dashboardObservability) RecordDiscovery(ev DiscoveryEvent) {
	if d == nil {
		return
	}
	if ev.TimestampMs == 0 {
		ev.TimestampMs = time.Now().UnixMilli()
	}
	d.discoveryLog.Push(ev)
}

// cardinalityKey encodes the (rule_tag, request_key) tuple as a
// single string for the TopNCounter map. "::" is the separator
// because gRPC method paths contain "/" and JSON-RPC method names
// contain ".", so neither collides with the rule tag.
func cardinalityKey(ruleTag, requestKey string) string {
	if ruleTag == "" {
		ruleTag = "default"
	}
	return ruleTag + "::" + requestKey
}

func splitCardinalityKey(key string) (ruleTag, requestKey string) {
	if i := strings.Index(key, "::"); i >= 0 {
		return key[:i], key[i+2:]
	}
	return key, ""
}

// unmatchedKey encodes the (method, path) tuple as a single string
// for the TopNCounter map. Space-separated keeps the encoding
// reversible by splitUnmatchedKey, and method tokens never contain
// spaces.
func unmatchedKey(method, path string) string {
	if path == "" {
		return method
	}
	return method + " " + path
}

func splitUnmatchedKey(key string) (method, path string) {
	if i := strings.IndexByte(key, ' '); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}

// listUnmatched is the JSON payload for GET /api/v1/unmatched —
// a section → []UnmatchedEntry map, sorted by count descending
// within each section.
func listUnmatched(cg *CosmoGuard) map[string]any {
	out := map[string][]UnmatchedEntry{}
	if cg == nil || cg.dashboard == nil {
		return map[string]any{"sections": out}
	}
	cg.dashboard.unmatchedMu.Lock()
	sections := make([]string, 0, len(cg.dashboard.unmatched))
	counters := make([]*TopNCounter, 0, len(cg.dashboard.unmatched))
	for s, c := range cg.dashboard.unmatched {
		sections = append(sections, s)
		counters = append(counters, c)
	}
	cg.dashboard.unmatchedMu.Unlock()
	for i, section := range sections {
		entries := counters[i].Snapshot()
		list := make([]UnmatchedEntry, 0, len(entries))
		for _, e := range entries {
			method, path := splitUnmatchedKey(e.Key)
			list = append(list, UnmatchedEntry{
				Section:   section,
				Method:    method,
				Path:      path,
				Count:     e.Count,
				FirstSeen: e.FirstSeen.UnixMilli(),
				LastSeen:  e.LastSeen.UnixMilli(),
			})
		}
		out[section] = list
	}
	return map[string]any{"sections": out}
}

// listDenied is the JSON payload for GET /api/v1/denied — the
// recent-denials ring buffer, newest first.
func listDenied(cg *CosmoGuard) map[string]any {
	if cg == nil || cg.dashboard == nil {
		return map[string]any{"denied": []DenyRecord{}}
	}
	records := cg.dashboard.denied.Snapshot()
	if records == nil {
		records = []DenyRecord{}
	}
	return map[string]any{"denied": records}
}

// CardinalityRule is one (rule_tag, distinct_keys, hot_samples)
// tuple inside the cache-cardinality endpoint response. Samples
// holds up to a handful of the hottest request keys so the
// operator can spot a misconfigured key derivation at a glance.
type CardinalityRule struct {
	RuleTag      string   `json:"rule_tag"`
	DistinctKeys int      `json:"distinct_keys"`
	HotSamples   []string `json:"hot_samples,omitempty"`
}

// maxCardinalityHotSamples caps the per-rule sample list in the
// cache-cardinality endpoint. Three is enough for "look, this rule
// is hashing the bearer token into the key" without flooding the
// payload when 4096 distinct keys are in flight.
const maxCardinalityHotSamples = 3

// listCardinality is the JSON payload for GET /api/v1/cache-cardinality —
// a section → rules map where each rule lists its distinct-keys
// count plus a sample of the hottest keys.
func listCardinality(cg *CosmoGuard) map[string]any {
	out := map[string][]CardinalityRule{}
	if cg == nil || cg.dashboard == nil {
		return map[string]any{"sections": out}
	}
	cg.dashboard.cardinalityMu.Lock()
	sections := make([]string, 0, len(cg.dashboard.cardinality))
	counters := make([]*TopNCounter, 0, len(cg.dashboard.cardinality))
	for s, c := range cg.dashboard.cardinality {
		sections = append(sections, s)
		counters = append(counters, c)
	}
	cg.dashboard.cardinalityMu.Unlock()

	for i, section := range sections {
		entries := counters[i].Snapshot()
		// Group entries by rule_tag prefix so the operator sees one
		// row per rule rather than one row per distinct key.
		byRule := map[string]*CardinalityRule{}
		order := []string{}
		for _, e := range entries {
			ruleTag, reqKey := splitCardinalityKey(e.Key)
			cr, ok := byRule[ruleTag]
			if !ok {
				cr = &CardinalityRule{RuleTag: ruleTag}
				byRule[ruleTag] = cr
				order = append(order, ruleTag)
			}
			cr.DistinctKeys++
			if len(cr.HotSamples) < maxCardinalityHotSamples {
				cr.HotSamples = append(cr.HotSamples, reqKey)
			}
		}
		rows := make([]CardinalityRule, 0, len(order))
		for _, rt := range order {
			rows = append(rows, *byRule[rt])
		}
		out[section] = rows
	}
	return map[string]any{"sections": out}
}

// listReloadStatus is the JSON payload for GET /api/v1/reload-status —
// the most recent hot-reload outcome, or a zero-valued ReloadStatus
// when no reload has happened yet (so the dashboard pill can render
// "no reloads yet" instead of erroring on a null).
func listReloadStatus(cg *CosmoGuard) map[string]any {
	if cg == nil || cg.dashboard == nil {
		return map[string]any{"reload": ReloadStatus{Sections: map[string]ReloadSection{}}}
	}
	cg.dashboard.reloadMu.Lock()
	defer cg.dashboard.reloadMu.Unlock()
	if cg.dashboard.lastReload == nil {
		return map[string]any{"reload": ReloadStatus{Sections: map[string]ReloadSection{}}}
	}
	// Return a copy so the caller can't mutate our held state via
	// the json encoder.
	out := *cg.dashboard.lastReload
	return map[string]any{"reload": out}
}

// observabilitySnapshot is the msgpack-serialisable shape used by the
// replicator (DMap "observability", key=pod_id). Mirrors the runtime
// dashboardObservability state but in a form that survives a process
// restart: counters are flattened to slices, ring buffers to slices,
// pointers to values. Tags are short (1-3 chars) to keep the blob
// tight — pods rewrite their key every 30s under steady state.
//
// Forward compatibility: msgpack ignores unknown fields, so adding new
// observability surfaces won't break a mid-rolling-restart cluster
// where some peers are still on the previous version.
type observabilitySnapshot struct {
	Unmatched    map[string][]topNEntrySnap `msgpack:"u"`
	Cardinality  map[string][]topNEntrySnap `msgpack:"c"`
	Denied       []DenyRecord               `msgpack:"d"`
	DiscoveryLog []DiscoveryEvent           `msgpack:"g"`
	LastReload   *ReloadStatus              `msgpack:"r,omitempty"`
}

type topNEntrySnap struct {
	Key         string `msgpack:"k"`
	Count       uint64 `msgpack:"n"`
	FirstSeenMs int64  `msgpack:"f"`
	LastSeenMs  int64  `msgpack:"l"`
}

// Snapshot serialises the in-memory observability state to a msgpack
// blob. Safe to call from any goroutine — each surface acquires its
// own mutex briefly. The returned blob is appended-to by the
// replicator's outer wrapper (it adds the metrics-history slice
// alongside) before being written to the DMap.
func (d *dashboardObservability) Snapshot() ([]byte, error) {
	if d == nil {
		return nil, nil
	}
	snap := observabilitySnapshot{
		Unmatched:   map[string][]topNEntrySnap{},
		Cardinality: map[string][]topNEntrySnap{},
	}

	d.unmatchedMu.Lock()
	for section, counter := range d.unmatched {
		entries := counter.Snapshot()
		out := make([]topNEntrySnap, 0, len(entries))
		for _, e := range entries {
			out = append(out, topNEntrySnap{
				Key:         e.Key,
				Count:       e.Count,
				FirstSeenMs: e.FirstSeen.UnixMilli(),
				LastSeenMs:  e.LastSeen.UnixMilli(),
			})
		}
		snap.Unmatched[section] = out
	}
	d.unmatchedMu.Unlock()

	d.cardinalityMu.Lock()
	for section, counter := range d.cardinality {
		entries := counter.Snapshot()
		out := make([]topNEntrySnap, 0, len(entries))
		for _, e := range entries {
			out = append(out, topNEntrySnap{
				Key:         e.Key,
				Count:       e.Count,
				FirstSeenMs: e.FirstSeen.UnixMilli(),
				LastSeenMs:  e.LastSeen.UnixMilli(),
			})
		}
		snap.Cardinality[section] = out
	}
	d.cardinalityMu.Unlock()

	snap.Denied = d.denied.Snapshot()
	if snap.Denied == nil {
		snap.Denied = []DenyRecord{}
	}
	// RingBuffer.Snapshot is newest-first; flip to oldest-first so a
	// Restore can simply Push in order and end up with the original
	// LRU ordering.
	reverseInPlace(snap.Denied)

	snap.DiscoveryLog = d.discoveryLog.Snapshot()
	if snap.DiscoveryLog == nil {
		snap.DiscoveryLog = []DiscoveryEvent{}
	}
	reverseInPlace(snap.DiscoveryLog)

	d.reloadMu.Lock()
	if d.lastReload != nil {
		cp := *d.lastReload
		snap.LastReload = &cp
	}
	d.reloadMu.Unlock()

	return msgpack.Marshal(&snap)
}

// Restore replaces the in-memory state from a blob produced by
// Snapshot. Unknown / future fields are silently ignored. A corrupt
// or empty blob leaves the receiver untouched and returns nil — the
// pod simply boots with cold counters in that case rather than
// failing startup.
func (d *dashboardObservability) Restore(blob []byte) error {
	if d == nil || len(blob) == 0 {
		return nil
	}
	var snap observabilitySnapshot
	if err := msgpack.Unmarshal(blob, &snap); err != nil {
		// Don't fail startup over a corrupt snapshot — operators
		// running mixed versions during rollout shouldn't see
		// crashloops. Cold counters are recoverable; a crash is not.
		return nil
	}

	d.unmatchedMu.Lock()
	for section, entries := range snap.Unmatched {
		c, ok := d.unmatched[section]
		if !ok {
			c = NewTopNCounter(defaultUnmatchedCap)
			d.unmatched[section] = c
		}
		restoreTopN(c, entries)
	}
	d.unmatchedMu.Unlock()

	d.cardinalityMu.Lock()
	for section, entries := range snap.Cardinality {
		c, ok := d.cardinality[section]
		if !ok {
			c = NewTopNCounter(defaultCardinalityCap)
			d.cardinality[section] = c
		}
		restoreTopN(c, entries)
	}
	d.cardinalityMu.Unlock()

	for _, rec := range snap.Denied {
		d.denied.Push(rec)
	}
	for _, ev := range snap.DiscoveryLog {
		d.discoveryLog.Push(ev)
	}

	if snap.LastReload != nil {
		d.reloadMu.Lock()
		cp := *snap.LastReload
		d.lastReload = &cp
		d.reloadMu.Unlock()
	}
	return nil
}

// restoreTopN re-hydrates a TopNCounter from a slice of snapshot
// entries. Counts and timestamps are restored directly — the LRU order
// follows insertion order, which is the same order Snapshot emits
// (count-desc). For the dashboard's purposes that's close enough to
// the original LRU; what matters is that the counts and per-key first/
// last-seen timestamps survive the restart, not the exact eviction
// ordering.
func restoreTopN(c *TopNCounter, entries []topNEntrySnap) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range entries {
		if _, exists := c.items[e.Key]; exists {
			continue
		}
		if len(c.items) >= c.cap {
			oldest := c.lru.Back()
			if oldest != nil {
				oldKey := oldest.Value.(*tncItem).key
				c.lru.Remove(oldest)
				delete(c.items, oldKey)
			}
		}
		it := &tncItem{
			key:       e.Key,
			count:     e.Count,
			firstSeen: time.UnixMilli(e.FirstSeenMs),
			lastSeen:  time.UnixMilli(e.LastSeenMs),
		}
		it.elem = c.lru.PushBack(it) // restored entries are older than live ones
		c.items[e.Key] = it
	}
}

// reverseInPlace flips a slice. The ring-buffer snapshot emits
// newest-first but Restore wants oldest-first so Push lands records
// in chronological order (and the resulting in-memory ring still has
// newest entries at the head).
func reverseInPlace[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// listDiscoveryLog is the JSON payload for GET /api/v1/discovery-log —
// the bounded refresh-log ring buffer, newest first.
func listDiscoveryLog(cg *CosmoGuard) map[string]any {
	if cg == nil || cg.dashboard == nil {
		return map[string]any{"events": []DiscoveryEvent{}}
	}
	events := cg.dashboard.discoveryLog.Snapshot()
	if events == nil {
		events = []DiscoveryEvent{}
	}
	return map[string]any{"events": events}
}
