package cosmoguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetricsHistory_RingWrap drives the buffer past capacity to
// exercise the wrap path: the oldest entry must be evicted and
// Snapshot must return the remaining cap-many entries oldest-first.
// Without this the ring's head/full bookkeeping could silently
// reorder samples and the dashboard's left-to-right charts would
// render a jumbled timeline.
func TestMetricsHistory_RingWrap(t *testing.T) {
	h := newMetricsHistory(3)
	h.Push(MetricsSnapshot{TimestampMs: 1})
	h.Push(MetricsSnapshot{TimestampMs: 2})
	h.Push(MetricsSnapshot{TimestampMs: 3})
	h.Push(MetricsSnapshot{TimestampMs: 4}) // evicts 1
	h.Push(MetricsSnapshot{TimestampMs: 5}) // evicts 2

	snaps := h.Snapshot()
	require.Len(t, snaps, 3)
	require.Equal(t, int64(3), snaps[0].TimestampMs)
	require.Equal(t, int64(4), snaps[1].TimestampMs)
	require.Equal(t, int64(5), snaps[2].TimestampMs)
}

// TestMetricsHistory_PartialFill confirms a buffer with fewer than
// cap entries returns just those entries (not zero-padded). Mirrors
// the cold-start condition the dashboard hits before 5 minutes have
// elapsed.
func TestMetricsHistory_PartialFill(t *testing.T) {
	h := newMetricsHistory(5)
	h.Push(MetricsSnapshot{TimestampMs: 10})
	h.Push(MetricsSnapshot{TimestampMs: 20})

	snaps := h.Snapshot()
	require.Len(t, snaps, 2)
	require.Equal(t, int64(10), snaps[0].TimestampMs)
	require.Equal(t, int64(20), snaps[1].TimestampMs)
}

// TestMetricsHistory_RestoreTrimsToCap protects against a peer that
// somehow held more entries than this pod's local capacity (e.g.
// capacity changed between versions). Restore must keep the most
// recent cap entries and drop the rest — never overflow the buffer.
func TestMetricsHistory_RestoreTrimsToCap(t *testing.T) {
	h := newMetricsHistory(3)
	in := []MetricsSnapshot{
		{TimestampMs: 1},
		{TimestampMs: 2},
		{TimestampMs: 3},
		{TimestampMs: 4},
		{TimestampMs: 5},
	}
	h.Restore(in)

	snaps := h.Snapshot()
	require.Len(t, snaps, 3)
	require.Equal(t, int64(3), snaps[0].TimestampMs)
	require.Equal(t, int64(4), snaps[1].TimestampMs)
	require.Equal(t, int64(5), snaps[2].TimestampMs)
}

// TestMetricsHistory_NilSafe — the buffer is wired through optional
// pointers (a CosmoGuard built without observability replication
// keeps it nil). Every method must be nil-safe so call sites don't
// have to guard.
func TestMetricsHistory_NilSafe(t *testing.T) {
	var h *metricsHistory
	require.NotPanics(t, func() {
		h.Push(MetricsSnapshot{TimestampMs: 1})
		h.Restore([]MetricsSnapshot{{TimestampMs: 1}})
	})
	require.Nil(t, h.Snapshot())
}

// TestListMetricsHistory_EmptyEnvelope is the contract for the
// /api/v1/metrics/history endpoint when nothing has been sampled
// yet: a stable {"history": []} envelope so the client's
// hydration loop iterates over an empty array instead of a JSON
// null.
func TestListMetricsHistory_EmptyEnvelope(t *testing.T) {
	out := listMetricsHistory(nil)
	got, ok := out["history"].([]MetricsSnapshot)
	require.True(t, ok)
	require.Empty(t, got)

	cg := &CosmoGuard{} // metricsHistory nil
	out = listMetricsHistory(cg)
	got, ok = out["history"].([]MetricsSnapshot)
	require.True(t, ok)
	require.Empty(t, got)
}

// TestListMetricsHistory_ReturnsBufferContents — populated buffer
// surfaces oldest-first into the JSON envelope so the dashboard
// can append live polls onto the tail.
func TestListMetricsHistory_ReturnsBufferContents(t *testing.T) {
	cg := &CosmoGuard{metricsHistory: newMetricsHistory(5)}
	cg.metricsHistory.Push(MetricsSnapshot{TimestampMs: 100})
	cg.metricsHistory.Push(MetricsSnapshot{TimestampMs: 200})

	out := listMetricsHistory(cg)
	got, ok := out["history"].([]MetricsSnapshot)
	require.True(t, ok)
	require.Len(t, got, 2)
	require.Equal(t, int64(100), got[0].TimestampMs)
	require.Equal(t, int64(200), got[1].TimestampMs)
}
