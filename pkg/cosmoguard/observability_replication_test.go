package cosmoguard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestObservabilityReplicator_RoundTrip is the single-node sanity
// check: write counters into a dashboard, flush them through the
// replicator, then build a *fresh* dashboard pointing at the same DMap
// key and verify Restore picks the values back up. Same pattern the
// step-4 "pod A restarts and recovers from B's replica" test below
// uses, just collapsed to one olric runtime so the test is fast and
// the failure mode is unambiguous when something breaks.
func TestObservabilityReplicator_RoundTrip(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)

	// First incarnation: write counters and metrics history, then
	// trigger a manual flush.
	dashA := newDashboardObservability()
	histA := newMetricsHistory(10)
	dashA.RecordUnmatched("lcd", "GET", "/cosmos/bank/v1beta1/balances/foo")
	dashA.RecordUnmatched("lcd", "GET", "/cosmos/bank/v1beta1/balances/foo")
	dashA.RecordUnmatched("lcd", "POST", "/txs")
	dashA.RecordDeny(DenyRecord{
		Section: "rpc",
		Reason:  "rate_limit",
		Method:  "broadcast_tx",
	})
	histA.Push(MetricsSnapshot{TimestampMs: 100})
	histA.Push(MetricsSnapshot{TimestampMs: 200})

	rA, err := newObservabilityReplicator(cr.Client(), dashA, histA, nil, "pod-A")
	require.NoError(t, err)
	require.NotNil(t, rA)
	require.NoError(t, rA.flush(context.Background()))

	// Second incarnation (same pod_id): empty surfaces, restore from
	// the DMap.
	dashB := newDashboardObservability()
	histB := newMetricsHistory(10)
	rB, err := newObservabilityReplicator(cr.Client(), dashB, histB, nil, "pod-A")
	require.NoError(t, err)
	require.NoError(t, rB.restore(context.Background()))

	// Unmatched counters survived (count, key both preserved).
	unmatched := listUnmatched(&CosmoGuard{dashboard: dashB})
	sections := unmatched["sections"].(map[string][]UnmatchedEntry)
	require.Len(t, sections["lcd"], 2)
	// Ordered by count desc.
	require.Equal(t, "GET", sections["lcd"][0].Method)
	require.Equal(t, uint64(2), sections["lcd"][0].Count)
	require.Equal(t, "POST", sections["lcd"][1].Method)
	require.Equal(t, uint64(1), sections["lcd"][1].Count)

	// Denied ring buffer survived.
	denied := dashB.denied.Snapshot()
	require.Len(t, denied, 1)
	require.Equal(t, "rate_limit", denied[0].Reason)

	// Metrics history survived in original order.
	snaps := histB.Snapshot()
	require.Len(t, snaps, 2)
	require.Equal(t, int64(100), snaps[0].TimestampMs)
	require.Equal(t, int64(200), snaps[1].TimestampMs)
}

// TestObservabilityReplicator_PodRestartAcrossCluster is the step-4
// contract test: pod A's snapshot is replicated to peer B via olric
// RF=2, then A "restarts" (we create a fresh replicator pointed at A's
// pod_id) and recovers its previous counters by reading the replica
// from B. Reproduces the exact rolling-restart scenario the v4
// dashboard depends on.
func TestObservabilityReplicator_PodRestartAcrossCluster(t *testing.T) {
	a, b := newTwoNodeClusterForTest(t)

	// Pod A: drive observable traffic, snapshot to the DMap.
	dashA := newDashboardObservability()
	histA := newMetricsHistory(5)
	for i := 0; i < 3; i++ {
		dashA.RecordUnmatched("grpc", "/cosmos.bank.v1beta1.Query/Balance", "")
	}
	dashA.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", Method: "GET"})
	histA.Push(MetricsSnapshot{TimestampMs: 42})

	rA, err := newObservabilityReplicator(a.Client(), dashA, histA, nil, "cosmoguard-0")
	require.NoError(t, err)
	require.NoError(t, rA.flush(context.Background()))

	// Sanity: pod B can read pod A's key directly through its own
	// olric client (replication has reached the peer's partition
	// owner / replica).
	requireKeyVisibleOnPeer(t, b, "cosmoguard-0")

	// Pod A "restarts": new dashboard, new history, same pod_id. The
	// replicator's restore must repopulate both from the cluster.
	dashARestarted := newDashboardObservability()
	histARestarted := newMetricsHistory(5)
	// Crucially: use pod B's client to simulate the partition having
	// drifted off the local node — the restored data must come back
	// over the cluster, not from local memory.
	rRestarted, err := newObservabilityReplicator(b.Client(), dashARestarted, histARestarted, nil, "cosmoguard-0")
	require.NoError(t, err)
	require.NoError(t, rRestarted.restore(context.Background()))

	unmatched := dashARestarted.unmatched["grpc"].Snapshot()
	require.Len(t, unmatched, 1)
	require.Equal(t, uint64(3), unmatched[0].Count)

	denied := dashARestarted.denied.Snapshot()
	require.Len(t, denied, 1)
	require.Equal(t, "rule", denied[0].Reason)

	snaps := histARestarted.Snapshot()
	require.Len(t, snaps, 1)
	require.Equal(t, int64(42), snaps[0].TimestampMs)
}

// requireKeyVisibleOnPeer asserts the observability snapshot written
// under podID is fetchable through peer's own olric client. Wraps
// Eventually so a barely-late replication tick (or a routing-table
// refresh race on a freshly-formed cluster) isn't a flake. Mirrors
// the timing used by TestClusterRuntimeTwoNodeStaticDiscovery.
func requireKeyVisibleOnPeer(t *testing.T, peer *clusterRuntime, podID string) {
	t.Helper()
	dm, err := peer.Client().NewDMap(replicationDMap)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := dm.Get(context.Background(), replicationKeyPrefix+podID)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "snapshot for %q never propagated to peer", podID)
}

// TestObservabilityReplicator_TickerLifecycle ensures Start launches a
// background flush goroutine and Close drains it cleanly. Without
// this guard a Close() that didn't actually stop the goroutine would
// leak across every test that constructs a CosmoGuard.
func TestObservabilityReplicator_TickerLifecycle(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	dash := newDashboardObservability()
	hist := newMetricsHistory(10)

	r, err := newObservabilityReplicator(cr.Client(), dash, hist, nil, "pod-X")
	require.NoError(t, err)
	// Crank the cadence down so a single tick actually fires before
	// Close — the default 30s is way too slow for a unit test.
	r.interval = 50 * time.Millisecond
	r.metricsInterval = 50 * time.Millisecond

	require.NoError(t, r.Start(context.Background()))
	// Wait for at least one tick to fire so we know the goroutine
	// actually ran (not just started).
	time.Sleep(150 * time.Millisecond)

	require.NoError(t, r.Close(context.Background()))
	// Double-close is a no-op, not a panic.
	require.NoError(t, r.Close(context.Background()))
}

// TestObservabilityReplicator_DoubleStartDoesNotRehydrate guards the
// startOnce invariant. A defensive double-Start (supervisor restart,
// test re-init, future retry path) must NOT re-hydrate the DMap
// snapshot on top of live state, because Restore semantics for the
// denied ring + metricsHistory are destructive: the ring would
// double-push the snapshot's records (displacing live denials) and
// metricsHistory.Restore resets the buffer back to the snapshot's
// contents, throwing away samples collected between the two Start
// calls.
func TestObservabilityReplicator_DoubleStartDoesNotRehydrate(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)

	// Seed a snapshot under pod-X so a subsequent restore would have
	// data to pull. The snapshot contains one deny record.
	seedDash := newDashboardObservability()
	seedDash.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", Method: "GET"})
	seedHist := newMetricsHistory(5)
	seedHist.Push(MetricsSnapshot{TimestampMs: 1})
	seeder, err := newObservabilityReplicator(cr.Client(), seedDash, seedHist, nil, "pod-X")
	require.NoError(t, err)
	require.NoError(t, seeder.flush(context.Background()))

	// Fresh replicator on the same pod_id. First Start hydrates from
	// the seeded snapshot.
	dash := newDashboardObservability()
	hist := newMetricsHistory(5)
	r, err := newObservabilityReplicator(cr.Client(), dash, hist, nil, "pod-X")
	require.NoError(t, err)
	// Slow the ticker right down so the periodic flush goroutine
	// cannot interfere with the rehydrate assertion.
	r.interval = time.Hour
	r.metricsInterval = time.Hour
	require.NoError(t, r.Start(context.Background()))

	// Live activity AFTER the first Start: one extra deny + one extra
	// metrics sample. These must survive the second Start call.
	dash.RecordDeny(DenyRecord{Section: "lcd", Reason: "rule", Method: "POST"})
	hist.Push(MetricsSnapshot{TimestampMs: 999})

	// Second Start — must be a no-op, NOT a re-hydrate.
	require.NoError(t, r.Start(context.Background()))

	denied := dash.denied.Snapshot()
	require.Len(t, denied, 2, "live POST deny + restored GET deny — second Start must not re-push the restored record")
	// Order is newest-first: live POST first, restored GET second.
	require.Equal(t, "POST", denied[0].Method)
	require.Equal(t, "GET", denied[1].Method)

	snaps := hist.Snapshot()
	require.Len(t, snaps, 2, "restored sample + live sample — second Start must not reset the buffer")
	require.Equal(t, int64(1), snaps[0].TimestampMs)
	require.Equal(t, int64(999), snaps[1].TimestampMs)

	require.NoError(t, r.Close(context.Background()))
}

// TestObservabilityReplicator_NilOlricClient — the replicator
// constructor returns nil when no olric client is available (the
// embedded-test path that doesn't spin up clusterRuntime). Verify
// Start/Close are nil-safe so the caller can branchlessly invoke
// them.
func TestObservabilityReplicator_NilOlricClient(t *testing.T) {
	r, err := newObservabilityReplicator(nil, nil, nil, nil, "pod")
	require.NoError(t, err)
	require.Nil(t, r)
	require.NoError(t, r.Start(context.Background()))
	require.NoError(t, r.Close(context.Background()))
}
