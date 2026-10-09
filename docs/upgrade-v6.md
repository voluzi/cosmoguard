# Upgrading to v6

v6 replaces native per-fragment Olric tables with one charged response slab pool
per runtime and a separate security pool. Response namespaces, cache keys, native
entry/transfer formats, 271 clustered partitions, replica default and quorums stay
the same. The clustered limiter algorithm, per-replica local fallback and parsed,
deprecated, ignored `rateLimit.failureMode` stay the same. Auth failure modes are
unchanged. No new operator configuration is required.

## Compatible shared budgets for v6.1

The shared-budget candidate retains module `/v6` and the pinned Olric/ttlcache
versions. Runtime L1 adapters share one global LRU, total estimated bytes and the
total entry-count guard. `cache.NewMemoryPool` and `cache.WithMemoryPool` are
additive Go APIs; callers opting in receive a typed adapter, while existing
constructors without that option retain their concrete `MemoryCache` behaviour.
Pooled adapters support the production `string` and `uint64` key types; selecting
a pool with another key type returns an error. Legacy caches support all
comparable keys. Close adapters independently and close the pool owner after
consumers stop.
`CacheBudget.PerCache` and direct `WithCacheBudget` retain their public semantics.

Every response DMap uses the full node L2 budget before RF division for its soft
LRU thresholds. The hard response backing cap is unchanged. Finite response
fragments can reclaim at most 32 local victims on capacity pressure, including
backup and imported responses. Failed growth preserves its target, but other
victims may already be gone. Empty incoming fragments cannot reclaim sibling
occupancy; fragmentation and the work limit can still reject writes. Security
storage and policy are unchanged. Local victims do not imply durable replica
retention; surviving older valid response copies can still be read.

Rolling v6.0.0/v6.1 members keep the same 271 partitions, DMaps, keys, native wire
and transfer formats, encryption, RF and quorums. Local thresholds need no
consensus handshake or cache purge. Old members keep smaller thresholds and can
still delete copies on new members through native LRU. Replace one member at a
time, wait for ownership/transfer convergence and exercise both coordinators.
Roll back to the actual released v6.0.0 image without flushing. Verify original
bytes/deadlines, direct backup reads and security sentinels. Loopback tests do not
replace the published-image rolling/rollback release gate.

The historical measurements below describe v6.0.0 and its predecessors, not the
shared-budget candidate. Fuller caches can increase heap, GC and RSS despite the
same numeric caps. Release v6.1.0 only after the shared-budget performance and
memory matrix in the [release procedure](bounded-l2-release-tests.md) passes.

### Candidate hot-path diagnostics

Local paired benchmarks used warmed 1,024-key caches, alternating legacy/shared
order and three samples per case. The repeated single-protocol timing test
(`-count=10`, 100ms samples) measured median string hits at 419.5ns legacy versus
445.0ns shared, and uint64 hits at 409.5ns versus 390.5ns in the final run. An
earlier run measured 652.0ns versus 509.5ns and 587.5ns versus 513.0ns, illustrating
host variation. Both paths allocated zero bytes per hit.

The broader 1/4-CPU, 1/4/8-protocol matrix showed substantial timing variation
and a parallel regression. At four CPUs, four-protocol parallel hits measured
237ns versus 673ns for string keys and 249ns versus 656ns for uint64 keys;
eight-protocol hits measured 386ns versus 968ns and 451ns versus 979ns.
Mixed readers/writers also regressed. All cases retained zero per-hit allocations.
These are local diagnostics, not dedicated-runner or deployment measurements.
The common ttlcache item/metrics locks serialize cross-protocol work; optimizing
key representation removes extra hashing overhead but does not remove that
contention. The performance gate remains open: do not release this candidate
until dedicated-runner comparisons and the reference hot workload pass, or
revisit the reservation design if they confirm the regression.

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
   Olric-backed proxy caches also get this default when a Go consumer omits
   `WithL2Operations`. `SharedOptions` contains private runtime admission state;
   use `DefaultSharedOptions` or keyed literals instead of positional literals.
5. Olric storage statistics describe charged shared backing: `Allocated` includes
   slab/tree/descriptor, grown hash indexes and fragment allowances, and `Inuse` includes rounded live
   records and fragment metadata. Shared backing and slab counts are apportioned across registered
   fragments in constant time, including empty fragments; one fragment receives
   division remainders so totals remain exact. Native garbage ratios are not
   comparable. Use pool
   gauges for capacity decisions.
6. The embedded Olric dependency is now **the voluzi fork**, module
   `github.com/voluzi/olric`, pinned at
   `v0.7.5-0.20261009015321-b44a5f871051`. Consumers that interact with Olric Go
   types must update their Olric imports and requirement as well. There is **no
   replace directive**, including for downstream builds. The public module
   downloads through the default Go proxy. The fork changes listed below retain
   the native wire protocol.
7. Enforce the native envelope before cache insertion: keys are ≤255 bytes and
   `29 + len(key) + len(encoded value) < 1MiB`. This includes native framing, so
   a 1MiB payload does not fit. Generic encoding stops before exceeding that
   bound. Larger upstream results can still populate L1.

8. `New` and `NewFromFile` start the enabled metrics/ops listener during
   construction. Call `Shutdown` even without calling `Run`. `NewContext` and
   `NewFromFileContext` allow cancellation while startup waits for routing.
   `/healthz` answers during bootstrap; `/readyz` stays unavailable until the
   proxies serve and their upstream pools are healthy.

9. SIGTERM now fails readiness while serving for five seconds, then closes traffic and operations
   listeners concurrently. `DrainAndShutdown` implements that path; `Shutdown`
   is immediate and interrupts a hold. Shutdown runs once, uses a fixed 29s total
   capped by the caller's deadline, and can report incomplete cleanup rather than
   extending beyond it. Cleanup workers retain ownership until they actually exit.

10. Building or importing the module requires Go 1.26.9 or later. The release
    image uses Go 1.27.2; these patch versions include the HTTP/2 fix for
    GO-2026-6617, alongside `golang.org/x/net` v0.60.0.

## HTTP query behavior

The HTTP proxy now drops query parameters that Go cannot parse, including invalid
percent escapes and entire segments containing an unescaped semicolon. For example,
`?height=42&hidden=%zz&mode=read;admin=true` is forwarded as `?height=42`.
This deliberately changes v5.1.0's raw-query forwarding: upstreams no longer receive
parameters that CosmoGuard's parsed-query rules and authentication cannot inspect.
When sanitation is needed, valid parameters are re-encoded; valid raw queries keep
their encoding. Configured credential query parameters are still stripped before
forwarding.

## Embedded Olric fork

The fork starts from upstream v0.7.4 and includes these commits, in order:

| Commit | Change |
|---|---|
| `eea0f7f` | Honor a configured per-DMap engine instead of silently selecting the default engine. |
| `e1103a0` | Rename the module to `github.com/voluzi/olric`, allowing downstream consumers to use the fork without replace directives. |
| `ed93708` | Protect membership reads while constructing a Stats snapshot. |
| `16baa29` | Stop routing callbacks before shutdown waits, preventing concurrent shutdown work from racing the wait. |
| `cbe50ad` | Run ownership-length RPCs with bounded parallelism and apply their results in the original order, preserving pruning decisions while reducing serial scan delay. |
| `980fe50` | Process membership changes and close departed client pools while routing work waits; coalesce routing notifications into the existing worker. |
| `b3cbf68` | Lock and check fragment retirement before storage access, retry stale lookups, and prevent a queued janitor from removing a recreated fragment. Preserve already-read values when shutdown interrupts the idle check. |
| `d8d805a` | Finish compaction when a fragment is retired and skip retired storage during eviction, preventing a janitor collision from stopping all later expiry sweeps. |
| `1feb3c1` | Test parallel pruning over 271 partitions with shared published owner backing; assert identical serial results and no mutation under the race detector. No production change. |
| `d784449` | Derive the member snapshot from synchronous memberlist join/update/leave callbacks under the native node lock, instead of dereferencing mutable Node metadata returned by Members(). Preserve live-member selection and birthdate ordering while fixing metadata races during routing scans. |
| `154beba` | Compare cached membership with native live-member names and transmitted identities: the local member immediately after Start, same-name rejoin at a new gossip address/ID, metadata updates during reads, and a member declared dead without Leave. No production change. |
| `5b4d8db` | Test that one compaction pass continues past a retired fragment. No production change. |
| `a106ad3` | Remove a retired fragment from its partition after successful Close even if Destroy returns an error; test error propagation and a real DMap write/recreate. |
| `b44a5f8` | Unmap a retired fragment even when Close fails and cancels its context; test both Close/Destroy errors and a real Put/Get on the recreated fragment. |

Ownership scans have 16 workers. Active RPC concurrency to each peer also shares
Olric's client pool, whose default size is `10 × GOMAXPROCS`; at GOMAXPROCS=1 a
scan against one peer therefore cannot have 16 active RPCs. Cache and replica RPCs
use the same per-peer pool.

The callback snapshot includes suspect members just as native Members() does;
death and graceful leave both remove a member. Snapshot updates finish before
event enqueueing, so routing reads do not wait for the asynchronous event loop.
Tests compare native immutable names and the complete identities from transmitted
metadata; they avoid reading mutable native Node.Meta outside its private lock.

Public integration regressions exercise acknowledged Put/Get/Delete and
Destroy/recreate with both the default and custom engines. The fork changes do
not add operator settings or alter partition count, replica defaults, transfer
formats or the clustered limiter algorithm.

For example, migrate an existing response owner to:

```go
operations := cache.RecoveringOperations(128, 100*time.Millisecond, 16<<20, onFailure, onSkip, onUnavailable)
defer operations.CloseOperations()
// Pass operations to every NewOlricCache owned by this response runtime.
```

`BoundedOperations` keeps per-request checks. `RecoveringOperations` adds outage
suppression; `onUnavailable(bool)` observes transitions. `onFailure` receives
`timeout`, `rejected`, or `unavailable`. `onSkip` receives `unavailable`, `inflight`, `storage_capacity`, `entry_size`, `backend`, or `encode`.
`operations.OperationBytes()` returns current byte reservations and the configured capacity.
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
45s budget. Total waiting is capped at ten minutes from each runtime constructor's start, even
with a reachable coordinator; failure to receive a usable routing table then
exits with an explicit error so the kubelet can retry the join. SIGTERM cancels
construction and shuts down Olric with its graceful leave broadcast. This prevents bootstrap waits from exhausting the startup probe;
it does not change an old coordinator's scan or cancel its in-flight replica RPCs.

Two on-prem upgrades from published v5.1.0 used in-cluster load, three replicas
at 200m/250Mi, and roughly 6,000–8,500 requests/s before replacement. Both had
**zero container restarts**:

- Run A had about 1.5 minutes at 1,400–3,200 requests/s, 11 failed requests out
  of roughly 1.3 million, and a few requests taking 14–18 seconds.
- Run B had about 45 seconds at 16–500 requests/s and a later 15-second slice
  at 38 requests/s. Otherwise, two ready replicas served roughly 2,800–3,200
  requests/s for about two minutes while the second replaced pod waited
  2 minutes 20 seconds for the v5.1.0 coordinator's routing table. There were
  31 failed requests out of roughly 0.9 million, with some taking 12–19 seconds.

This degradation occurs while v5.1.0 pods are still cluster members. They have
no bounded waits and stall during membership changes, as in a measured v5.1.0
rolling restart with five restarts per replaced pod and about six minutes of
degradation. It is a one-time cost of leaving v5.x; **upgrade in a low-traffic
window**. These two runs establish the observed cost, not a bound on all rollouts.
A v6-to-v6 rolling restart of 13947aa measured zero restarts, zero errors and no
throughput collapse, sustaining 4,916–8,588 requests/s throughout.

The response and clustered-limiter gates suppress backend calls after three
consecutive executed-operation timeouts. One second later, one real request probes
recovery. Successes and recognized domain outcomes (such as a cache miss, capacity skip,
entry size/encoding rejection or limiter denial) close the state. Transport,
quorum and unknown errors leave a probe open for the next cooldown. A timed-out probe may be replaced
after cooldown; unfinished workers retain their slot and byte reservations.
L2 skips to L1/upstream and the limiter uses its existing per-replica fallback.
The limiter algorithm and deprecated ignored failureMode are unchanged. This
per-gate state may also divert healthy partitions during an outage. Replay keeps
its per-request bounded NX check, with no outage state, so healthy partitions can
still reject replayed tokens. Monitor `cosmoguard_backend_unavailable_gates` and
the `unavailable` backend failures / L2 skips and `backend_unavailable` limiter
fallback reasons. No operator settings are added.

Termination requires no operator probe or lifecycle change: `/readyz` becomes 503
at the signal, while health, metrics and traffic remain live for five seconds.
At +5s traffic and operations listeners stop concurrently; HTTP and finite gRPC work drain until +24s.
Stuck gRPC streams are forced closed at that deadline. WebSocket close code 1001
is best effort under a bounded write; clients reconnect and resubscribe. Cleanup
has at most 2s, capped at +26s; Olric graceful leave has at most 3s, capped at +29s.
The fixed 29s absolute total fits the operator's 30s termination grace with margin
and no preStop hook. Shorter caller budgets can curtail phases. The chart's 5s
preStop plus the binary's 29s fits its 40s grace. Failed startup skips the hold.
Local signal tests alone cannot prove an ingress converges within five seconds;
the on-prem v6 rolling restart above observed zero errors with in-cluster load.

The native wire codecs are tested in both directions against the real default
engine, including loopback migration, post-join replication, graceful departure,
rollback and disconnect/retry. The two published-v5.1.0 forward upgrades above
provide on-prem evidence for that direction. Run
[the release procedure](bounded-l2-release-tests.md) with immutable actual image
digests and archive its evidence. Reverse rollback to the published old image
remains unverified; the local native-engine matrix is not that image test.

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

## Response cache corrections and hit-rate interpretation

Slab Range now supplies Olric's LRU sampler with the oldest entries, so a recently
touched key survives eviction before an untouched older key. Hash indexes grow
with fragment cardinality, charging both arrays during growth; failure preserves
existing keys. Range batches resume from a validated next entry instead of
rescanning earlier entries; callbacks can still delete entries safely. Scan
cursors follow the hash index instead of access generations,
so scanning and reading unchanged keys terminates. Scans concurrent with writes
remain best effort, as with native storage.

HTTP/gRPC response wrappers bound their encoded size before byte admission.
Unknown generic values retain the conservative 8MiB charge. Both response and
security pools admit two codec charges. Byte-cache reads still copy at the adapter
boundary because the real default engine can return a slice into its table.

The 1.2% hit-header result at 200m does not prove that 98.8% of requests reached the
upstream. In a local closed-loop 30-client reproduction over 1,500 keys, 200m/250Mi
produced 1.25% hit markers while avoiding 96.65% of real upstream fetches. Sharing
an in-flight or pending response retains the inherited `miss` marker. Globally
interleaving the keys instead produced 92.45% hit markers and 92.73% upstream
savings. These loopback probes include the generator and fake upstream in the
same CPU-limited container; their rps are diagnostics, not deployment benchmarks.
Use `cosmoguard_upstream_requests_total` to validate the real savings on-prem.
Response timing and cache marker semantics are unchanged.

## Memory and operational limits

See [CONFIG.md](../CONFIG.md#memory-budget) for the budget table, standalone gate,
pool metrics and capacity behavior. GOMEMLIMIT stays at 90%; chart limits stay
1Gi with their existing requests/CPU defaults. A 500m/500Mi requests=limits
reference and 200m/250Mi lower compatibility profile require the stated workload
and release evidence. Local unthrottled engine/L1 diagnostics recorded no OOM
but breached the 95% peak criterion in some cases. On-prem validation of 13947aa
measured 22–28MiB heap / about 60MiB RSS at 250Mi and 25–33MiB heap / about
66MiB RSS at 500Mi, zero idle GC, and the response
pool returning to zero after TTL. Whole-guard long soaks, the Linux CI architecture
matrix and published-image reverse rollback remain unverified.

Response backing and controlled local/codec copies are bounded. Security
cardinality, fallback identities, application-owned bodies/encoder internals,
pre-admission RESP/outer transfer buffers, slow remote writers and returned export
buffers remain outside those caps. Arbitrary workloads can still exceed a cgroup
limit. No new misconfiguration validation or fallback machinery is introduced.
