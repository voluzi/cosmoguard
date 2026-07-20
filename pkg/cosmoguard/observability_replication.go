package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/olric-data/olric"
	"github.com/vmihailenco/msgpack/v5"
)

// observability_replication.go keeps the per-pod dashboard surfaces +
// metrics-history ring buffer alive across a rolling restart. The
// model is intentionally simple — no consensus, no leader, no shared
// truth across pods:
//
//   - Every pod periodically writes its OWN observability blob to a
//     shared olric DMap under its pod_id.
//   - Olric replicates that DMap entry to RF=2 peers, so a single
//     pod restart can recover its previous state from any surviving
//     peer.
//   - On startup, the pod reads ITS OWN key. If a peer has the
//     replica, the pod boots with the prior counters, denied tail,
//     metrics history, etc. — exactly what an operator scrolling
//     through panels expects after `kubectl rollout restart`.
//
// Trade-off (documented for the v4 reviewer): full-cluster restart
// loses everything. This is by design — we're not running a database,
// we're keeping dashboards from blinking. Single-pod-restart survival
// covers ~all real-world rolling-update events.
//
// Hot-path discipline: ZERO request-path code calls into the
// replicator. Observability writes are mutex-only operations on
// in-process counters; the replicator only reads those counters on
// its own ticker and pushes a single DMap.Put per pod per interval.
//
// Two independent responsibilities live here, gated separately:
//
//   - The metrics-history sampler (sampleMetrics → metricsHistory.Push)
//     is a purely in-process, bounded ring feed for the live dashboard
//     time-series panels (/metrics/history). It ALWAYS runs — it has no
//     olric involvement and no leak.
//   - The cross-pod DMap flush + restore (the restart-restore feature)
//     is the ONLY part gated behind dashboard.clusterHistoryRestore.
//     Its 30s replicated large-value Put is what grows olric's
//     log-structured store unbounded → OOM, so it is off by default and
//     only meaningful in cluster mode (peers to restore from).
//
// The `replicate` field carries that gate: false → sample only, never
// touch the DMap.

// replicationDMap is the DMap name where each pod stores its
// observability blob. Stable across versions — peers in mid-rollout
// must converge on the same DMap or replication breaks.
const replicationDMap = "observability"

// replicationKeyPrefix is the prefix inside the DMap. Keeps the
// observability blobs from colliding with any future global keys
// (e.g. cluster-wide config snapshots) inside the same DMap.
const replicationKeyPrefix = "obs:"

// replicationInterval is the cadence at which each pod rewrites its
// snapshot. 30s balances "data isn't more than 30s stale on restart"
// against "we're not constantly serialising counters and writing to
// the partition owner". One Put per pod per 30s is negligible even at
// 100 pods.
const replicationInterval = 30 * time.Second

// replicationTTL is the per-key TTL on the snapshot blob. Long enough
// to survive a slow rolling restart (well beyond the time a pod is
// down) but short enough that a permanently-removed pod's data ages
// out instead of accumulating in the DMap. 24h is conservative on
// both sides.
const replicationTTL = 24 * time.Hour

// replicationPayload is the on-the-wire shape stored in the DMap. The
// outer envelope keeps observability and metrics-history separate so
// a future surface (e.g. per-rule sampling counters) can be added
// without breaking older peers — msgpack ignores unknown fields.
type replicationPayload struct {
	// Observability is the result of dashboardObservability.Snapshot().
	// Stored as raw bytes so we don't double-marshal the inner blob.
	Observability []byte `msgpack:"o"`
	// History is the metrics-history ring buffer, oldest-first.
	History []MetricsSnapshot `msgpack:"h"`
	// WrittenMs is the unix-millis timestamp at which the pod wrote
	// this blob — purely informational so operators can sanity-check
	// "is this snapshot stale?" in the panel.
	WrittenMs int64 `msgpack:"w"`
}

// observabilityReplicator periodically snapshots the local dashboard
// observability + metrics-history into the DMap, and reads back its
// own previous snapshot on startup. One per CosmoGuard instance.
type observabilityReplicator struct {
	dm      olric.DMap
	dash    *dashboardObservability
	history *metricsHistory
	// gather samples a MetricsSnapshot on every metricsInterval tick.
	// Decoupled from prometheus.DefaultGatherer so tests can drive
	// deterministic snapshots without registering live metrics.
	gather func() (*MetricsSnapshot, error)

	// replicate gates the cross-pod DMap flush + restore. When false the
	// replicator only samples metrics history (in-process, no leak) and
	// never writes to or reads from the olric DMap.
	replicate bool

	podID           string
	interval        time.Duration
	metricsInterval time.Duration

	stop      chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once
	wg        sync.WaitGroup

	// oversizedWarned gates the "snapshot exceeds entry cap" warning to the
	// transition edge, so a persistent extreme-cardinality condition logs
	// once (and logs recovery once) instead of every flush tick. flush runs
	// serially on the replication ticker, so no lock is needed.
	oversizedWarned bool
}

// newObservabilityReplicator wires up a replicator. The olric client
// is the same in-process embedded handle the cache + rate limiter use.
// Returns nil if olricClient is nil (tests / embedded-without-cluster
// paths) — replication is best-effort, not a hard dependency.
//
// replicate controls ONLY the cross-pod DMap flush + restore; the
// metrics-history sampler runs regardless (it feeds the live dashboard
// time-series panels and is leak-free). Pass false to sample-only.
func newObservabilityReplicator(
	olricClient *olric.EmbeddedClient,
	dash *dashboardObservability,
	history *metricsHistory,
	gather func() (*MetricsSnapshot, error),
	podID string,
	replicate bool,
) (*observabilityReplicator, error) {
	if olricClient == nil {
		return nil, nil
	}
	// The DMap handle is only needed for the flush/restore path. When
	// replication is off we still construct the replicator (for the
	// sampler) but leave dm nil so no DMap machinery is touched.
	var dm olric.DMap
	if replicate {
		var err error
		dm, err = olricClient.NewDMap(replicationDMap)
		if err != nil {
			return nil, fmt.Errorf("observability replicator: dmap: %w", err)
		}
	}
	if podID == "" {
		// Pod identity fallback chain: hostname is what K8s gives a
		// StatefulSet pod (cosmoguard-0, cosmoguard-1, …). Deployments
		// get an ephemeral hostname per pod replacement — we document
		// the consequence (per-pod history doesn't survive Deployment
		// pod replacement) in CONFIG.md, but the code still works.
		host, _ := os.Hostname()
		podID = host
		if podID == "" {
			podID = "cosmoguard"
		}
	}
	return &observabilityReplicator{
		dm:              dm,
		dash:            dash,
		history:         history,
		gather:          gather,
		replicate:       replicate,
		podID:           podID,
		interval:        replicationInterval,
		metricsInterval: 5 * time.Second, // matches dashboard poll cadence (5min @ 60 entries)
		stop:            make(chan struct{}),
	}, nil
}

// Start restores any prior snapshot from the DMap (peers hold the
// replica via olric's RF=2) and launches the periodic flush goroutine.
// Returns nil on a cold start (no prior key) — that's the expected
// path for the very first pod in a fresh cluster.
//
// Idempotent: calling Start on a replicator that's already running is
// a no-op. Safe to call exactly once per instance lifecycle.
func (r *observabilityReplicator) Start(ctx context.Context) error {
	if r == nil {
		return nil
	}
	// Restore lives inside the startOnce guard. Without it, a defensive
	// double-Start (supervisor restart, test harness re-init, a future
	// retry path) would re-hydrate the DMap snapshot on top of live
	// state — re-pushing the restored denied records onto the ring (now
	// displacing whatever live denials accumulated since first Start)
	// and resetting metricsHistory's accumulated samples back to the
	// snapshot's contents. TopNCounter restoreTopN skips existing keys
	// so unmatched/cardinality survive a double-hydrate, but the ring
	// buffers do not — so we just gate the whole thing.
	//
	// Restore best-effort: a missing key is the cold-start path; any
	// other error is logged but doesn't fail Start — the dashboard
	// still works without restored history, just with cold counters.
	r.startOnce.Do(func() {
		// Restore only when cross-pod replication is on — with it off there
		// is no DMap snapshot to read (dm is nil) and nothing to restore.
		if r.replicate {
			if err := r.restore(ctx); err != nil {
				slog.Warn("observability replication: restore failed (continuing with cold state)", "error", err, "pod_id", r.podID)
			}
		}
		r.wg.Add(1)
		go r.run()
	})
	return nil
}

// Close stops the periodic flush, performs one final snapshot write
// (so the freshest counters land in the DMap before this pod exits),
// and returns when the goroutine has drained. The context bounds the
// final flush — exceeding it leaves the last 30s of data unwritten
// rather than blocking shutdown.
func (r *observabilityReplicator) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	// stopOnce protects close(r.stop) so concurrent Close callers (the
	// test harness fan-out, a shutdown handler, a panic-recover fallback)
	// can't race the check-then-close pattern into a "close of closed
	// channel" panic.
	alreadyStopped := true
	r.stopOnce.Do(func() {
		alreadyStopped = false
		close(r.stop)
	})
	if alreadyStopped {
		return nil
	}
	r.wg.Wait()
	return r.flush(ctx)
}

// PodID returns the stable identifier this replicator writes under.
// Exposed for tests + the peers/cluster dashboard endpoint added in
// Step 6 (so the UI can label each replica by the same ID the DMap
// snapshot is keyed on).
func (r *observabilityReplicator) PodID() string {
	if r == nil {
		return ""
	}
	return r.podID
}

// run is the metrics-history sampler and (when replication is on) the
// periodic DMap flush. The metrics ticker always fires; the flush ticker
// is only wired when r.replicate is true — otherwise flushC stays nil and
// its case blocks forever, so a sample-only replicator never touches the
// DMap. One goroutine, at most two timers, regardless.
func (r *observabilityReplicator) run() {
	defer r.wg.Done()
	metricsT := time.NewTicker(r.metricsInterval)
	defer metricsT.Stop()

	var flushC <-chan time.Time
	if r.replicate {
		flushT := time.NewTicker(r.interval)
		defer flushT.Stop()
		flushC = flushT.C
	}
	for {
		select {
		case <-r.stop:
			return
		case <-metricsT.C:
			r.sampleMetrics()
		case <-flushC:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := r.flush(ctx); err != nil {
				slog.Warn("observability replication: flush failed", "error", err, "pod_id", r.podID)
			}
			cancel()
		}
	}
}

// sampleMetrics drives one observation into the history ring. Errors
// are logged at debug because a transient prometheus.Gather() failure
// is not actionable — the next tick will succeed.
func (r *observabilityReplicator) sampleMetrics() {
	if r.gather == nil || r.history == nil {
		return
	}
	snap, err := r.gather()
	if err != nil {
		slog.Debug("observability replication: metrics gather failed", "error", err)
		return
	}
	if snap == nil {
		return
	}
	r.history.Push(*snap)
}

// flush serialises the current state and writes it to the DMap. Best-
// effort: a partition unreachable in mid-failover is logged but the
// next interval tries again.
func (r *observabilityReplicator) flush(ctx context.Context) error {
	// !replicate ⇒ dm is nil; skip all DMap work (also covers the final
	// flush from Close on a sample-only replicator).
	if r == nil || !r.replicate || r.dm == nil {
		return nil
	}
	obsBlob, err := r.dash.Snapshot()
	if err != nil {
		return fmt.Errorf("observability snapshot: %w", err)
	}
	payload := replicationPayload{
		Observability: obsBlob,
		History:       r.history.Snapshot(),
		WrittenMs:     time.Now().UnixMilli(),
	}
	blob, err := marshalReplicationPayload(&payload)
	if err != nil {
		return fmt.Errorf("observability replication: marshal: %w", err)
	}
	// If the blob is still over the entry cap after trimming history, the
	// base observability snapshot alone is too large (extreme cardinality).
	// Skip the write rather than let olric reject it with ErrEntryTooLarge on
	// every tick — restart-restore is best-effort, so a logged skip is the
	// right failure mode. Bounding the snapshot's internal cardinality is a
	// separate observability change, not this cache-memory fix.
	if len(blob) > maxReplicationBlobBytes {
		// Log only on the transition into the oversized state so a persistent
		// condition doesn't emit a warning every tick (~2880/day/pod).
		if !r.oversizedWarned {
			slog.Warn("observability replication: snapshot exceeds entry cap even after trimming history; skipping restore writes until it shrinks",
				"bytes", len(blob), "cap", maxReplicationBlobBytes)
			r.oversizedWarned = true
		}
		return nil
	}
	if r.oversizedWarned {
		slog.Info("observability replication: snapshot back under entry cap; resuming restore writes")
		r.oversizedWarned = false
	}
	if err := r.dm.Put(ctx, r.key(), blob, olric.EX(replicationTTL)); err != nil {
		return fmt.Errorf("observability replication: put: %w", err)
	}
	return nil
}

// maxReplicationBlobBytes bounds the marshalled replication payload so its
// olric Put can't exceed the per-fragment entry cap (olricTableSizeBytes) and
// fail with ErrEntryTooLarge — which, unlike the response caches, has no
// uncached fallback and would drop dashboard restart-restore entirely. Left a
// margin below the table size for olric's own per-entry framing.
const maxReplicationBlobBytes = int(olricTableSizeBytes) - (16 << 10) // 240 KiB

// marshalReplicationPayload marshals the payload, trimming the metrics
// History oldest-first until the blob fits under maxReplicationBlobBytes.
// History is the unbounded, high-cardinality-sensitive part; the base
// observability snapshot is kept even if it alone is large (a too-large Put
// then surfaces as the normal flush error rather than being silently lost).
func marshalReplicationPayload(payload *replicationPayload) ([]byte, error) {
	blob, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if len(blob) <= maxReplicationBlobBytes || len(payload.History) == 0 {
		return blob, nil
	}
	// Binary search would be overkill; drop the oldest history entries in
	// halving steps until it fits or history is exhausted.
	trimmed := payload.History
	for len(trimmed) > 0 && len(blob) > maxReplicationBlobBytes {
		drop := len(trimmed)/2 + 1
		trimmed = trimmed[drop:]
		p := *payload
		p.History = trimmed
		if blob, err = msgpack.Marshal(&p); err != nil {
			return nil, err
		}
	}
	if dropped := len(payload.History) - len(trimmed); dropped > 0 {
		slog.Warn("observability replication: trimmed history to fit entry cap",
			"dropped", dropped, "kept", len(trimmed), "bytes", len(blob), "cap", maxReplicationBlobBytes)
	}
	return blob, nil
}

// restore reads this pod's own previous snapshot from the DMap and
// hydrates both surfaces. Missing key is the cold-start path and
// returns nil.
func (r *observabilityReplicator) restore(ctx context.Context) error {
	if r == nil || r.dm == nil {
		return nil
	}
	resp, err := r.dm.Get(ctx, r.key())
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("get: %w", err)
	}
	blob, err := resp.Byte()
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	var payload replicationPayload
	if err := msgpack.Unmarshal(blob, &payload); err != nil {
		// Treat corrupt snapshots as "cold start" — operators
		// rolling between versions shouldn't be punished for an
		// older blob format we can no longer decode.
		slog.Warn("observability replication: corrupt snapshot, ignoring", "pod_id", r.podID)
		return nil
	}
	if err := r.dash.Restore(payload.Observability); err != nil {
		return fmt.Errorf("dashboard restore: %w", err)
	}
	if r.history != nil {
		r.history.Restore(payload.History)
	}
	slog.Info("observability replication: restored state from peer replica",
		"pod_id", r.podID, "history_entries", len(payload.History), "written_ms", payload.WrittenMs)
	return nil
}

// key is the full DMap key for this pod's snapshot.
func (r *observabilityReplicator) key() string {
	return replicationKeyPrefix + r.podID
}
