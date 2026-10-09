# Bounded L2 release gates

Run these procedures on the coordinator's disposable on-prem environment. The
implementer does not contact Kubernetes. A successful runner exit means its
traffic assertions completed; it does **not** certify the entire release matrix.
Archive the completed matrix and sign off each criterion below before releasing.

Build the candidate image with the release workflow's Go 1.27.2 toolchain. Build
the fixture image from the same checkout under an explicit tag:

```sh
export TOOLS_TAG='your-registry/bounded-l2-tools:release-candidate'
docker build -f scripts/bounded-l2-tools.Dockerfile -t "$TOOLS_TAG" .
```

Publish that tag through the coordinator's authorized image workflow, then set
`TOOLS_IMAGE` to its resolved immutable digest before running any procedure.
Record both candidate and tools digests. The tools image contains a deterministic
HTTP/JSON-RPC,
WebSocket and reflecting gRPC upstream and the traffic driver. It is never a
substitute for the published v5.1.0 image or the actual v6 candidate.

Prerequisites: Python 3, Helm, kubectl, mikefarah/yq v4, crane, an existing test
namespace, and permission to create disposable resources there and read pod
metrics. Run from an otherwise clean checkout. Every command requires an explicit
context and namespace. The runner inventories pre-existing resources, uses unique
names, records created UIDs, and deletes only those identities in its finally
handler. If cleanup reports errors, finish removal of the listed **matching UIDs**;
never delete the namespace or use a label-wide deletion over unrelated objects.
Verify before/after inventories. Record container/network/volume inventories and
remove only task-created resources if using Docker instead.

First measure saturation at each profile with the same protocol/size distribution,
TTL, upstream latency and observability setting. Set `PROBE_RPS` to 50% of that
measured sustainable rate. Keep a healthy, under-cap control phase with that fixed
offered rate; retain its absolute throughput, p99 and hit/skip ratios. The runner
rejects an omitted rate so a CPU-starved unthrottled diagnostic cannot silently
become the healthy control.

```sh
export TOOLS_IMAGE='your-registry/bounded-l2-tools@sha256:...'
export RPS_250=... # half of measured saturation at 200m/250Mi
export RPS_500=... # half of measured saturation at 500m/500Mi
export RPS_1GI=... # half of measured saturation at 1 CPU/1Gi
PROBE_RPS="$RPS_250" scripts/test-bounded-l2-cluster.sh soak --context "$TEST_CONTEXT" --namespace "$TEST_NAMESPACE" --new-image "$V6_DIGEST" --cpu 200m --memory 250Mi --requests-equal-limits --duration 2h --output "$RESULTS/250"
PROBE_RPS="$RPS_500" scripts/test-bounded-l2-cluster.sh soak --context "$TEST_CONTEXT" --namespace "$TEST_NAMESPACE" --new-image "$V6_DIGEST" --cpu 500m --memory 500Mi --requests-equal-limits --duration 2h --output "$RESULTS/500"
PROBE_RPS="$RPS_1GI" scripts/test-bounded-l2-cluster.sh soak --context "$TEST_CONTEXT" --namespace "$TEST_NAMESPACE" --new-image "$V6_DIGEST" --cpu 1 --memory 1Gi --requests-equal-limits --duration 2h --output "$RESULTS/1gi"
PROBE_RPS="$RPS_250" scripts/test-bounded-l2-cluster.sh mixed-version --context "$TEST_CONTEXT" --namespace "$TEST_NAMESPACE" --old-image "$V5_DIGEST" --new-image "$V6_DIGEST" --output "$RESULTS/mixed"
```

The default soak runs **two hours per 4/8-DMap scenario**, plus 15 minutes idle
each. It cycles 1→2→4→8→4→2 members at RF2/quorum1, uses fixed 1KiB, 16KiB and
256KiB phases, and switches per-rule TTL from 10s to 1h halfway through via config
hot reload. Global cache settings stay fixed. Requests and limits are equal.
The driver checks deterministic response bytes, uses 10,000 subject names and
fresh jti with 60s expiry, and checks a six-hour replay sentinel and an LCD
limiter sentinel across membership changes. The LCD sentinel has rate 1/6h,
burst one, and carries no jti so limiter checks cannot be masked by replay denial.
Fixture seed is 42. The driver reads its targets and optional per-target sizes
on each request, uses a common HTTP Host/gRPC authority so cache keys remain
shared across pod addresses, rotates the destination within each protocol,
and reserves 10% of traffic for repeated hot keys. It paces requests without
a waiter queue and reports success,
errors and latency percentiles on a five-second ticker. Sentinel checks can delay
subsequent reports during faults; use the recorded timestamps for actual intervals.
Every periodic sentinel assertion must complete with its expected status on each
ready target; a timeout or transport error fails the run. A 30-second interval
without any successful traffic also fails, checked on the reporting ticker and
at completion. This permits brief replacement pauses without masking an outage
in a later phase. gRPC connections are reused.

Traffic remains active through every required phase and its dwell. The runner adds
up to the 600s rollout timeout per phase to the requested traffic duration; this
buffer may extend the load after the last dwell. A driver exit before completion,
even with status zero, fails the run.

The old image is resolved against the registry's published
`ghcr.io/voluzi/cosmoguard:5.1.0` manifest and must report 5.1.0 from `--version`.
The runner records image configs including OCI revision labels. A local rebuild
with similar imports is rejected. Mixed-version starts two old members, adds a
new ordinal, replaces old members individually, and rolls back individually
under continuous real guard API traffic. Old members receive 4Gi initially;
v6 members receive the selected profile limit. Use `--dmaps 4` or `--dmaps 8`
to isolate a case, and repeat at moderate load with old nodes at their actual
250Mi limits. The low-memory acceptance criteria apply to v6 nodes.

## Local release regressions

CI runs the mixed native/custom loopback matrix once on each architecture. Before
release, run `make test.mixed-engine MIXED_ENGINE_COUNT=10` with Go 1.26.9; this
keeps ten race-detector repetitions as a release gate without multiplying every
CI run's cluster setup and convergence waits. `make test.bounded-l2` also retains
ten race repetitions for storage, admission and response cache tests.

## Additional mandatory cells

Complete these cells separately; the runner records them as pending. Do not
shorten the principal two-hour scenarios to make room for them.

| Cell | Procedure and evidence |
| --- | --- |
| Empty/sparse | Capture empty process baseline. Seed exactly one small record per partition per actual response namespace, using `xxhash(DMapName+key) % 271`. The diagnostic `l2probe` does this with the production namespace prefixes; for actual guards, record the cache-key hashes produced by the request fixtures and verify partition coverage through native routing/stats. Record empty and sparse cgroup/heap data. |
| Full and mixed values | Repeat fixed-size runs with `--size 1024`, `16384`, `262144`, and `921600`, and a mixed run (`--size 0`). Each fixed-size phase must reach its resolved response cap and write at least 10× that cap. Use distinct bodies and overwrite churn; never share a single payload slice between callers. Probe native encoded size `29 + len(key) + len(value) < 1MiB` on both sides of the boundary. Record skipped writes and cross-pod L2 hits. |
| Security cardinality | Verify 10,000 **simultaneously live** subject buckets and jti through native stats and expiry deadlines, not just 10,000 names over the entire run. Increase offered rate for a separately labelled admission interval if needed to fit 10,000 jti in 60s. Keep long-lived bucket, lock-token and jti sentinels through transfers. Give the limiter sentinel no replay claim. Inspect every returned identity and deny outcome. |
| Expiry/idle | Run TTL10s until full, retain persistent records in all four security DMaps, stop traffic, wait TTL+60s for response sweep, then 15 minutes idle. The principal TTL1h phase's old entries will not expire merely because new rules use TTL10s. Check response records and unused slabs release, security bytes remain intact, and no forced GC occurs. |
| RF and observability | Repeat RF1 (`--replica-factor 1`) and RF4 (`--replica-factor 4`) at supported member counts. Retain configured RF2 in the ordinary single-member case. Repeat one profile with `--restore-history`. Record actual security allocation and identity counts; these are workload limits, not production caps. |
| Fan-in | 128 distinct near-envelope callers, cancellation-ignoring workers, simultaneous replica fan-in, multiple joins/leaves, and deliberate response byte saturation. Hold one peer's transport, then release it. Confirm caller p99 wait ≤150ms in non-overloaded control, retained charges while workers remain blocked, and eventual return of G and codec gauges to zero. |
| Transfers/faults | Select owners from the native routing table. Verify each backup's actual value with `dm.getentry <dmap> <key> RC` before abrupt owner termination. Exercise graceful leave and abrupt termination separately. Pause/disconnect RESP transfer before sending, mid-frame, and after receiver import but before sender ACK; retry, verify source retained on failure and no corrupt destination. Race TTL expiry with each direction. Restore the exact node/network rule in a finally/trap handler before resource cleanup. Use only task-created namespaces/workloads/links. |
| Rollback/security | During published-image rolling replacement and reverse rollback, verify healthy connected limiter budgets, unexpired jti rejection, and a valid lock token's release after migration through guard/native APIs. Cache misses are allowed after response eviction; returned bytes must match expected hashes. RF1 cannot survive loss of its only copy. RF2/quorum1 is not consensus. |

There are eight configured response namespaces with EVM enabled. The existing
EVM WebSocket HTTP wrapper does not publish HTTP cache rules, so ordinary HTTP
fixtures cannot fill that eighth L1 merely by enabling EVM. Seed and measure all
eight storage namespaces separately and record which real proxy caches are
active. The driver targets the seven available proxy cache paths with EVM on.
Do not present storage-only coverage as eight fully populated API caches.
This task does not change that inherited routing behavior.

Olric v0.7.4 also replicates a TTL-only lock renewal as an empty backup value;
a subsequent quorum read can select it and invalidate the token. This reproduces
with the real default engine. The limiter uses its existing fixed lock timeout,
not renewal. The local RF2 tests cover acquisition, contention, token-safe unlock
and replay NX/XX/TTL; RF1 covers renewal. No fork change for renewal is included.
Do not claim stronger lease-renewal or partition-failure semantics in the rollout.

## Raw measurement contract and acceptance

The runner writes environment, seed, UID inventories, rendered config, immutable
image metadata, raw pod status/restarts, logs, traffic JSONL and each pod's full
Prometheus scrape in serial batches, waiting five seconds after each batch.
Batch timestamps are retained in JSONL and scrape filenames; actual intervals
include the pod scrapes and grow with their count/latency. Preserve sampling
errors; permissions or missing samples invalidate the affected interval. Logs must remain available
before deleting a faulted pod. Use a coordinator-owned host/CRI sampler to match
each recorded pod/container UID to its cgroup and capture, every five seconds:

- `memory.current`, `memory.peak`, `memory.events`, `memory.stat`, `cpu.stat`;
- process RSS/CPU and HeapAlloc, HeapInuse, HeapSys, Sys−HeapReleased, next_gc;
- automatic GC cycles/rate, GC CPU and GC-limiter activation, pool bytes/keys,
  rejections/drops, G and codec reservations, L1/L2 hits, limiter fallback and
  replay errors, success/p50/p95/p99 and restart count.

The v6 scrape adds `cosmoguard_gc_cpu_seconds_total` and
`cosmoguard_gc_limiter_last_enabled_cycle` alongside the standard Go heap/GC
observations. It does not expose cgroup peak/events. Record those through the
host sampler, or mark them missing and keep the gate pending. Old native nodes
require separate GC CPU/limiter sampling; do not invent values for absent metrics. For diagnostic processes,
`l2probe` writes the complete ledger directly without forcing collection. Its
initial empty snapshot establishes CPU and GC baselines and omits their rates;
subsequent snapshots report rates over the preceding measurement interval. Its
engine/L1 run is not a substitute for the actual guard soak. Enable `GODEBUG=gctrace=1`
only for a separately recorded diagnostic if tracing is needed; do not alter
GOMEMLIMIT or use forced collections to make an idle interval pass.

Fail acceptance for any charged response allocation above cap, G/codec excess or
leak, ratcheting index/descriptor allocation, OOM/restart/panic/corrupt response,
lost acknowledged healthy-transfer security state, or eviction of security by
response pressure. Peak v6 cgroup memory must be **<95%** of its limit. After
TTL+60s and five further idle minutes, automatic GC must average **<1/s** over
60s, CPU must be **<20m above empty-security baseline**, naturally observed live
heap must be below GOMEMLIMIT, and cgroup current must settle without ratcheting.
At the fixed healthy offered rate, full-cache throughput must be **≥80%** of
control and p99 **≤2×** control. Report absolute rate/latency and skip/hit ratios;
discarding every L2 write does not pass. Following faults, shared limiter use
and cross-pod hits must recover without restart. Existing 1s limiter/250ms
contention and replay timing contracts remain the deterministic test gates.

Investigate a 95% breach; OOM-free is insufficient. If failure implicates
pre-admission transport allocations, obtain approval for a concrete Olric/redcon
byte-admission patch before changing the dependency further. The approved fork
contains engine selection/module rename, two race fixes, bounded parallel ownership
scans, membership progress and fragment retirement fixes; see the commit table in
[the v6 upgrade guide](upgrade-v6.md).


## Replacement outage and termination evidence

For each replacement, capture the signal time, readiness transition, last new
connection, traffic drain completion, and Olric leave. v6 holds readiness at 503
while traffic/operations listeners serve for 5s, then stops those listeners
concurrently. Traffic is capped at signal +24s, consumer cleanup at +26s, and
Olric leave at +29s. The operator's 30s grace requires no preStop hook. These
changes do not alter termination behavior in an old v5.1.0 container.

Capture `cosmoguard_backend_unavailable_gates`, `unavailable` operation failures
and L2 skips, and `backend_unavailable` limiter fallbacks. Response and limiter
gates suppress calls after three executed-operation timeouts, then allow one
foreground recovery probe after 1s. Replay keeps its per-request bounded NX check.
Compare 20s survivor slices with that run's own healthy control, including the
clustered limiter; after the initial timeout wave require at least 80% of control
and no repeated multi-second p95 slices. Check that shared limiter decisions and
cross-pod L2 hits resume after recovery. Do not count higher fallback throughput
alone as proof that cross-pod cache sharing recovered.

During slow bootstrap, `/healthz` must answer while `/readyz` stays 503. Preserve
old and new coordinator logs, pod events and rendered probes/lifecycle settings.
Published-image mixed rollout, real ingress convergence and the full soak remain
coordinator gates; loopback and diagnostic containers do not certify them.
