package cosmoguard

import (
	"sync"
)

// metricsHistory is a bounded ring buffer of recent MetricsSnapshot
// records. The dashboard's time-series charts hydrate from this buffer
// on connect — without it, opening the dashboard a few seconds after a
// pod restart produces empty charts until enough live polls accumulate.
//
// Capacity is sized for the dashboard's default 5-second poll cadence:
// 60 entries × 5 s = 5 minutes of recent history per pod. The buffer
// is replicated through observabilityReplicator (DMap "observability",
// key=pod_id) so a restarting pod restores its own history from a
// peer's replica.
//
// Push is mutex-only and O(1). Snapshot returns a fresh slice ordered
// oldest-first (so the dashboard can render charts left-to-right
// without reversing).
type metricsHistory struct {
	mu   sync.Mutex
	cap  int
	head int
	full bool
	buf  []MetricsSnapshot
}

const defaultMetricsHistoryCap = 60 // 5 min @ 5 s poll

// newMetricsHistory returns a ring buffer holding at most cap entries.
// cap <= 0 normalizes to defaultMetricsHistoryCap.
func newMetricsHistory(cap int) *metricsHistory {
	if cap <= 0 {
		cap = defaultMetricsHistoryCap
	}
	return &metricsHistory{cap: cap, buf: make([]MetricsSnapshot, cap)}
}

// Push appends snap to the buffer, overwriting the oldest entry when
// full. Nil-safe so wiring sites can dereference an optional history
// without guarding.
func (h *metricsHistory) Push(snap MetricsSnapshot) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.buf[h.head] = snap
	h.head = (h.head + 1) % h.cap
	if h.head == 0 {
		h.full = true
	}
}

// Snapshot returns a copy of every entry in the buffer, ordered
// oldest-first. Returns nil receiver yields nil slice.
func (h *metricsHistory) Snapshot() []MetricsSnapshot {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var n int
	if h.full {
		n = h.cap
	} else {
		n = h.head
	}
	out := make([]MetricsSnapshot, 0, n)
	for i := 0; i < n; i++ {
		idx := (h.head - n + i + h.cap) % h.cap
		out = append(out, h.buf[idx])
	}
	return out
}

// listMetricsHistory is the JSON payload for GET /api/v1/metrics/history.
// Empty when the buffer has no samples yet (the first 5 s after boot, or
// when the replicator's gather hook is not wired). Returns a stable
// envelope so the client doesn't need to special-case empty bodies.
func listMetricsHistory(cg *CosmoGuard) map[string]any {
	if cg == nil || cg.metricsHistory == nil {
		return map[string]any{"history": []MetricsSnapshot{}}
	}
	snaps := cg.metricsHistory.Snapshot()
	if snaps == nil {
		snaps = []MetricsSnapshot{}
	}
	return map[string]any{"history": snaps}
}

// Restore replaces the buffer contents with snaps (oldest-first).
// Older-than-capacity entries are dropped — the buffer keeps the last
// cap snapshots. Used by the replicator to hydrate from a peer's DMap
// blob on startup.
func (h *metricsHistory) Restore(snaps []MetricsSnapshot) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// Reset state.
	h.head = 0
	h.full = false
	for i := range h.buf {
		h.buf[i] = MetricsSnapshot{}
	}
	// Trim incoming to capacity, keeping the most recent entries.
	if len(snaps) > h.cap {
		snaps = snaps[len(snaps)-h.cap:]
	}
	for _, s := range snaps {
		h.buf[h.head] = s
		h.head = (h.head + 1) % h.cap
		if h.head == 0 {
			h.full = true
		}
	}
}
