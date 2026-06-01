package cosmoguard

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RequestLogEntry is the metadata captured per request — never bodies,
// never headers. Fields are JSON-tagged to match the dashboard payload.
type RequestLogEntry struct {
	TimestampMs int64  `json:"timestamp_ms"`
	Section     string `json:"section"`
	Method      string `json:"method"`
	Path        string `json:"path,omitempty"`
	Query       string `json:"query,omitempty"`
	Status      int    `json:"status"`
	LatencyMs   int64  `json:"latency_ms"`
	CacheState  string `json:"cache_state,omitempty"`
	Action      string `json:"action,omitempty"`
	RuleTag     string `json:"rule_tag,omitempty"`
	Identity    string `json:"identity,omitempty"`
	SourceIP    string `json:"source_ip,omitempty"`
	Upstream    string `json:"upstream,omitempty"`
}

// requestLogState is the immutable snapshot of the operator-supplied
// RequestLogConfig that Record reads on every push. Swapped atomically
// by ApplyConfig so hot-reload of dashboard.requestLog takes effect on
// the next request — no pod restart required.
type requestLogState struct {
	enable          bool
	includeSuccess  bool
	includeDenied   bool
	maxAge          time.Duration
	maxEntriesTotal int // global hard cap across all sections
}

// requestLog stores per-section time-ordered slices of recent
// request metadata. Eviction is time-windowed (maxAge) with a hard
// total-entry cap as the OOM backstop. Push is O(k) where k is the
// number of expired entries at the head of the section's slice;
// under steady state k ≈ rate-of-aging which is small.
type requestLog struct {
	state atomic.Pointer[requestLogState]

	mu      sync.Mutex
	entries map[string][]RequestLogEntry // per-section, oldest-first
}

func newRequestLog(cfg *RequestLogConfig) *requestLog {
	l := &requestLog{entries: map[string][]RequestLogEntry{}}
	l.ApplyConfig(cfg)
	return l
}

// ApplyConfig swaps the in-effect config atomically. Safe to call from
// any goroutine and from the hot-reload path. Going from enabled to
// disabled keeps the buffered entries on disk — Snapshot will return
// nothing while disabled, and re-enabling without restart picks up
// where we left off (subject to the maxAge window).
func (l *requestLog) ApplyConfig(cfg *RequestLogConfig) {
	if l == nil {
		return
	}
	s := &requestLogState{
		enable:          cfg.IsEnabled(),
		includeSuccess:  cfg.IncludeSuccessOrDefault(),
		includeDenied:   cfg.IncludeDeniedOrDefault(),
		maxAge:          cfg.MaxAgeOrDefault(),
		maxEntriesTotal: cfg.MaxEntriesOrDefault(),
	}
	l.state.Store(s)
}

// Record captures one request. Drops it silently when:
//   - the log is disabled,
//   - the outcome class isn't selected by the operator's filters,
//   - the receiver is nil (programmatic-embedder path).
func (l *requestLog) Record(e RequestLogEntry) {
	if l == nil {
		return
	}
	s := l.state.Load()
	if s == nil || !s.enable {
		return
	}
	denied := e.Status == 0 || e.Status >= 400 || e.Action == "deny"
	if denied && !s.includeDenied {
		return
	}
	if !denied && !s.includeSuccess {
		return
	}
	if e.TimestampMs == 0 {
		e.TimestampMs = time.Now().UnixMilli()
	}

	cutoffMs := e.TimestampMs - s.maxAge.Milliseconds()

	l.mu.Lock()
	defer l.mu.Unlock()

	list := l.entries[e.Section]
	// Drop entries that fell out of the time window. Slice is
	// oldest-first so a linear scan from index 0 finds the cutoff
	// in O(k) where k is the number of expired entries.
	drop := 0
	for drop < len(list) && list[drop].TimestampMs < cutoffMs {
		drop++
	}
	if drop > 0 {
		list = list[drop:]
	}
	list = append(list, e)
	l.entries[e.Section] = list

	// Global hard cap. Only walks when we'd otherwise risk
	// unbounded growth under a sustained burst that overflows
	// maxAge × steady-state rate. Drops the oldest across all
	// sections by timestamp.
	if s.maxEntriesTotal > 0 {
		l.trimToTotalLocked(s.maxEntriesTotal)
	}
}

func (l *requestLog) trimToTotalLocked(maxTotal int) {
	total := 0
	for _, list := range l.entries {
		total += len(list)
	}
	if total <= maxTotal {
		return
	}
	// Find the oldest timestamp across all sections and drop one
	// from there until we're under the cap. Cheaper than sorting
	// everything when the overflow is tiny (the common case).
	for total > maxTotal {
		oldestSection := ""
		oldestTs := int64(1<<63 - 1)
		for section, list := range l.entries {
			if len(list) == 0 {
				continue
			}
			if list[0].TimestampMs < oldestTs {
				oldestTs = list[0].TimestampMs
				oldestSection = section
			}
		}
		if oldestSection == "" {
			return
		}
		l.entries[oldestSection] = l.entries[oldestSection][1:]
		total--
	}
}

// Snapshot returns entries within the configured time window, merged
// newest-first across the requested sections (or all sections when
// sections is empty), capped at limit. limit<=0 means "no extra cap"
// — the time window is the only bound.
func (l *requestLog) Snapshot(sections []string, limit int) []RequestLogEntry {
	if l == nil {
		return nil
	}
	s := l.state.Load()
	if s == nil || !s.enable {
		return nil
	}
	cutoffMs := time.Now().Add(-s.maxAge).UnixMilli()

	want := map[string]bool{}
	for _, sec := range sections {
		want[sec] = true
	}

	l.mu.Lock()
	merged := make([]RequestLogEntry, 0)
	for section, list := range l.entries {
		if len(want) > 0 && !want[section] {
			continue
		}
		// Drop expired entries from the front. Bounded by the time
		// window so the work is amortised across pushes.
		for _, e := range list {
			if e.TimestampMs >= cutoffMs {
				merged = append(merged, e)
			}
		}
	}
	l.mu.Unlock()

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].TimestampMs > merged[j].TimestampMs
	})
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// listRecentRequests is the /api/v1/requests/recent handler payload.
// Query params: section=lcd,rpc (default: all), limit=N (default:
// 0 = window-only). Returns an empty envelope when the log is
// disabled so the dashboard's panel can render a "not enabled" hint
// without a 404.
func listRecentRequests(cg *CosmoGuard, r *http.Request) map[string]any {
	out := map[string]any{"requests": []RequestLogEntry{}, "enabled": false}
	if cg == nil || cg.requestLog == nil {
		return out
	}
	s := cg.requestLog.state.Load()
	if s == nil || !s.enable {
		return out
	}
	out["enabled"] = true
	var sections []string
	if q := r.URL.Query().Get("section"); q != "" {
		for _, p := range strings.Split(q, ",") {
			if p = strings.TrimSpace(p); p != "" {
				sections = append(sections, p)
			}
		}
	}
	limit := 0
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil {
			limit = n
		}
	}
	out["requests"] = cg.requestLog.Snapshot(sections, limit)
	return out
}
