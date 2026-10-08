# Upgrading to v6

v6 replaces native per-fragment Olric tables with one charged response slab pool
per runtime and a separate security pool. Response namespaces, cache keys, native
entry/transfer formats, 271 clustered partitions, replica default and quorums stay
the same. The clustered limiter algorithm, per-replica local fallback and parsed,
deprecated, ignored `rateLimit.failureMode` stay the same. Auth failure modes are
unchanged. No new operator configuration is required.

## Breaking changes for Go consumers

1. Import `github.com/voluzi/cosmoguard/v6` and its subpackages. This release is
   v6.0.0; the old `/v5` module path does not select the new implementation.
2. `cache.BoundedOperations` now takes five arguments: count capacity, total
   caller duration, byte capacity, failure callback and write-skip callback.
   Share the returned option across all response caches owned by one runtime.
   Call `CloseOperations` when that owner closes. Zero byte capacity explicitly
   means unlimited; production runtime wiring uses its automatically derived G.
3. Tiered `Set` writes L1 first and returns every L2 failure. A returned error
   therefore does not imply the value is absent from L1. Capacity, oversized
   entries, encoding/backend failures and timed-out/skipped writes preserve L1.
   Response handlers still serve the upstream result.
4. Standalone response caches now use the shared 128-slot/100ms byte gate and
   slab engine too. Standalone limiter and replay behavior retain their existing
   admission policy. Timed-out workers keep their reservations until they exit;
   closing an owner stops new admission without pretending blocked work ended.
5. Olric storage statistics describe charged shared backing: `Allocated` includes
   slab/tree/descriptor and fragment allowances, and `Inuse` includes rounded live
   records and fragment metadata. Shared backing and slab counts are apportioned across registered
   fragments in constant time, including empty fragments; one fragment receives
   division remainders so totals remain exact. Native garbage ratios are not
   comparable. Use pool
   gauges for capacity decisions.
6. The embedded Olric dependency is now **the voluzi fork**, module
   `github.com/voluzi/olric`, pinned at
   `v0.7.5-0.20261008123445-16baa29c1b3e`. Consumers that interact with Olric Go
   types must update their Olric imports and requirement as well. There is **no
   replace directive**, including for downstream builds. The public module
   downloads through the default Go proxy. The fork contains the approved engine
   selection/module rename and membership-snapshot/shutdown race fixes; its wire
   protocol is unchanged.
7. Enforce the native envelope before cache insertion: keys are ≤255 bytes and
   `29 + len(key) + len(encoded value) < 1MiB`. This includes native framing, so
   a 1MiB payload does not fit. Generic encoding stops before exceeding that
   bound. Larger upstream results can still populate L1.

8. `New` and `NewFromFile` start the enabled metrics/ops listener during
   construction. Call `Shutdown` even without calling `Run`. `NewContext` and
   `NewFromFileContext` allow cancellation while startup waits for routing.
   `/healthz` answers during bootstrap; `/readyz` stays unavailable until the
   proxies serve and their upstream pools are healthy.

For example, migrate an existing response owner to:

```go
operations := cache.BoundedOperations(128, 100*time.Millisecond, 16<<20, onFailure, onSkip)
defer operations.CloseOperations()
// Pass operations to every NewOlricCache owned by this response runtime.
```

`onFailure` retains the existing timeout/rejected outcomes. `onSkip` receives
`inflight`, `storage_capacity`, `entry_size`, `backend`, or `encode`.
`operations.OperationBytes()` returns current and total byte reservations.
An opaque Olric write-quorum error is `backend`; a failed backup with successful
quorum can increment storage rejection without producing a caller skip.

## Rolling replacement and rollback

Keep the same cluster encryption key, DMap/key namespaces, 271 partition count,
replica factor and quorums across versions. Replace one member at a time, wait
for membership and ownership/data-transfer convergence, and keep at least two
healthy connected members for RF2 redundant-data cases. Do not restart the whole
cluster or purge caches as an upgrade workaround. Verify actual replica values
before testing abrupt owner loss: v0.7.4 does not eagerly create backups for
pre-join writes, and quorum one does not guarantee every backup accepted a write.

A slow coordinator can spend longer than 45s scanning old owners before pushing
routing. New joiners remain healthy and not-ready while the advertised cluster
has quorum and its coordinator answers authenticated PINGs. Startup aborts after
45s without that reachability evidence; discovery and daemon start retain their
45s budget. SIGTERM cancels construction and shuts down Olric with its graceful
leave broadcast. This prevents bootstrap waits from exhausting the startup probe;
it does not change an old coordinator's scan or cancel its in-flight replica RPCs.

The native wire codecs are tested in both directions against the real default
engine, including loopback migration, post-join replication, graceful departure,
rollback and disconnect/retry. The **published v5.1.0-image rollout remains a
coordinator release gate**, not a result inferred from those tests. Run
[the release procedure](bounded-l2-release-tests.md) with immutable actual image
digests and archive its evidence before treating rolling compatibility as proven.
The same procedure checks reverse rollback to the published old image.

Old v5 members still have a lazy native 1MiB table for each populated primary or
backup fragment. Four/eight fully populated response namespaces alone can cost
1,084/2,168MiB on one clustered node. Give old participants enough memory for the
full-cache compatibility matrix (4Gi for eight maps is a starting point, measure
peaks), and separately test a moderate rollout at their real 250Mi limits. v6's
pool cannot fix old pods' memory or GC before replacement.

Responses omitted under capacity may miss and refetch; any returned response
must remain byte-correct. Security imports propagate failures rather than
acknowledging omitted records. RF2/quorum1 retains Olric's existing partition,
failover and lock semantics; it is not consensus. Native RF2 lock renewal can
replicate an empty backup value, an inherited limitation recorded in the procedure.
The existing limiter uses fixed-timeout locks and local fallback.

## Memory and operational limits

See [CONFIG.md](../CONFIG.md#memory-budget) for the budget table, standalone gate,
pool metrics and capacity behavior. GOMEMLIMIT stays at 90%; chart limits stay
1Gi with their existing requests/CPU defaults. A 500m/500Mi requests=limits
reference and 200m/250Mi lower compatibility profile require the stated workload
and release evidence. Local unthrottled engine/L1 diagnostics recorded no OOM
but breached the 95% peak criterion in some cases. Whole-guard two-hour soaks,
15-minute expiry/idle gates and published-image compatibility are pending.

Response backing and controlled local/codec copies are bounded. Security
cardinality, fallback identities, application-owned bodies/encoder internals,
pre-admission RESP/outer transfer buffers, slow remote writers and returned export
buffers remain outside those caps. Arbitrary workloads can still exceed a cgroup
limit. No new misconfiguration validation or fallback machinery is introduced.
