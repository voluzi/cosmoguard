# Configuration

This is the cosmoguard v4 configuration reference. Every setting is shown with its YAML key, default, and a short description of behavior. v3-shaped configs continue to work — run `cosmoguard migrate-config` to rewrite them in v4 form.

Table of contents:

- [Top-level settings](#top-level-settings)
- [Server hardening (`server:`)](#server-hardening)
- [Upstream nodes (`nodes:`)](#upstream-nodes)
- [Cache (`cache:`)](#cache)
- [Authentication (`auth:`)](#authentication)
- [CORS (`cors:`)](#cors)
- [LCD / RPC / gRPC / EVM sections](#protocol-sections)
- [Rules](#rules)
  - [HTTP rule (LCD, RPC, EVM RPC HTTP)](#http-rule)
  - [JSON-RPC rule (Cosmos RPC, EVM RPC, EVM WS)](#json-rpc-rule)
  - [gRPC rule](#grpc-rule)
- [Env-var interpolation](#env-var-interpolation)
- [Migration from v3](#migration-from-v3)

---

## Top-level settings

| Key | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Address cosmoguard binds to. |
| `lcdPort` | `11317` | LCD (REST) listen port. |
| `rpcPort` | `16657` | Tendermint RPC listen port. |
| `grpcPort` | `19090` | gRPC listen port. |
| `enableEvm` | `false` | When true, EVM RPC + WS proxies are also started. |
| `evmRpcPort` | `18545` | EVM JSON-RPC listen port. |
| `evmRpcWsPort` | `18546` | EVM WebSocket listen port. |
| `metrics.enable` | `true` | Expose `/metrics`, `/healthz`, `/readyz` on `metrics.port`. |
| `metrics.port` | `9001` | Metrics + health-probe port. |

The enabled metrics listener starts during construction. `/healthz` stays healthy
while Olric awaits routing; `/readyz` stays unavailable until the proxies serve
and every configured upstream pool is healthy. The joiner keeps waiting while
its advertised cluster has quorum and its coordinator answers authenticated
PINGs, and aborts after 45s without that evidence. Discovery and daemon start
retain their 45s startup budget. SIGTERM cancels startup and closes the daemon.

`/metrics` exposes `cosmoguard_config_reloads_total{outcome="..."}`, a process-wide
counter that counts each config reload attempt once, excluding initial startup:

| `outcome` | Meaning |
|---|---|
| `applied` | The config was accepted and its hot-reloadable settings applied. |
| `restart_required` | A startup-captured setting changed; the previous config remains active. |
| `invalid` | Reading, parsing or validating the file failed; the previous config remains active. |

---

## Server hardening

The `server:` block tunes HTTP/WS server timeouts and body caps. Defaults are conservatively safe for any Cosmos node.

```yaml
server:
  readHeaderTimeout: 10s    # cap on time to read HTTP headers (slowloris defense)
  readTimeout: 30s          # cap on total request-read time
  writeTimeout: 0            # cap on response write time, also the upstream response-header wait; 0 = no limit (default)
  idleTimeout: 60s          # keep-alive idle timeout
  maxRequestBody: 5242880   # bytes; requests exceeding this return 413 (0 = no limit; a GET body is then buffered unbounded when upstream retries are on)
  wsReadLimit: 1048576      # max bytes per inbound WebSocket frame (0 = no limit)
  websocketLimits:          # process-local; explicit 0 disables one limit
    maxSubscriptionsPerClient: 32
    maxSubscriptionsPerIdentity: 128
    maxSubscriptionsPerUpstreamConnection: 10
    maxConnectionsPerIP: 16
  wsAllowedOrigins:         # cross-origin WS upgrade allowlist
    - https://app.example.com
    - https://*.preview.example.com
  trustedProxies:           # proxy CIDRs allowed to contribute client IPs
    - 10.0.0.0/8
```

**Breaking from v3:** `wsAllowedOrigins` defaults to empty — cross-origin WS upgrades are denied unless explicitly allowed. To restore v3 behavior, set `wsAllowedOrigins: ["*"]`.

`trustedProxies` must contain only load balancers and ingress proxies under your control. CosmoGuard walks `X-Forwarded-For` from right to left across those trusted hops and selects the first untrusted address as the client, so prefixes supplied by the client are ignored. `X-Real-IP` is used only when no `X-Forwarded-For` header is present; a proxy relying on it must overwrite any client-supplied value. Leave the list empty when CosmoGuard is exposed directly, and never use `0.0.0.0/0` or `::/0` in production.

WebSocket admission limits are local to each CosmoGuard process, not distributed across replicas. The client limit applies to one downstream socket, the identity limit uses the authenticated `Identity.Name` and is shared across the RPC and EVM WebSocket endpoints, and the upstream limit applies to each upstream connection. Anonymous clients do not consume identity quota, but remain subject to the client and source-IP limits. A connection rejected by the source-IP limit receives HTTP 429; an established connection rejected while subscribing receives JSON-RPC `-32005`.

By default, each enabled protocol pool has 40 upstream WebSocket connections with 10 distinct upstream subscriptions per connection, for 400 nominal slots per RPC or EVM pool. The 40 connections are shared across that protocol's configured backends, not allocated per backend. This is local admission capacity with healthy upstreams that accept every connection and subscription, not a guarantee imposed on an upstream service. Downstream quotas still apply before that capacity: for example, 100 distinct subscriptions under one authenticated identity require at least four clients because each client is limited to 32, while the identity is limited to 128. Joining an already deduplicated upstream subscription still consumes downstream client and identity quota. Existing explicit smaller values remain unchanged, and changing these startup-captured settings requires a process restart.

**Default changes since v4.0.0-rc.1** (all restore v3-compatible behaviour that the rc.1 defaults broke):
- `writeTimeout` now defaults to **0 (no limit)**. A fixed deadline truncated large/slow streamed responses (`/block_results`, `/genesis`, big `eth_getLogs`) mid-body. Set an explicit ceiling if exposing cosmoguard to untrusted clients.
- `maxRequestBody` default raised from 1 MiB to **5 MiB** so large payloads (e.g. a wasm `MsgStoreCode` broadcast) aren't rejected with 413.
- `wsReadLimit` default raised from 64 KiB to **1 MiB**, and an explicit `0` now means "no limit" (as documented) instead of being silently forced to 64 KiB. Large frames (e.g. a big `eth_sendRawTransaction`) are no longer dropped.

**Hot-reload:** Changes to `server:` timeouts / body caps / WebSocket limits, `cors:`, and dashboard `enable`/`port`/`basicAuth` **reject** a reload with a "requires a process restart" message. `dashboard.requestLog`, `server.trustedProxies`, `rpc.jsonrpc.maxBatchSize` and `grpc.protosets` hot-reload.

A change covered by the restart policy rejects the **entire** reload: rules in that same file
update stay unchanged too. Global cache, EVM enablement, authentication, upstream
nodes, CORS, server limits/timeouts, dashboard startup settings, metrics/WebUI,
WebSocket pool settings and gRPC message sizes require restart. Rules and section
defaults, trusted proxies, dashboard request logging, JSON-RPC batch limits and
gRPC protosets remain hot-reloadable.

`host`, the protocol listener ports (`lcdPort`, `rpcPort`, `grpcPort`,
`evmRpcPort`, `evmRpcWsPort`), `upstream.*` and `tracing` are captured at startup
but are outside this restart policy. Changes to these settings alone are accepted
on reload without being applied; restart the process to apply them.

`rpc.jsonrpc.maxBatchSize` is shared by the Cosmos RPC and EVM RPC HTTP batch
paths. `0` disables the cap; omission restores the default of 100. WebSocket
frames accept individual JSON-RPC requests, not batches.

### Go configuration comparison API

Import `github.com/voluzi/cosmoguard/v6/pkg/cosmoguard`:

```go
func ParseConfig(raw []byte, lookupEnv func(string) (string, bool)) (*Config, error)
func RequiresRestart(previous, next *Config) (bool, string)
func RestartFingerprint(cfg *Config) (string, error)
```

`ParseConfig` parses strict single-document YAML, applies defaults and normalization,
validates settings and compiles rules. The supplied lookup controls both `${VAR}`
interpolation and `COSMOGUARD_*` overrides; nil means every variable is unset. It
returns a fresh config without publishing trusted proxies or initializing runtime
services. The existing `ReadConfigFromFile` and `PrepareConfig` retain their process
environment and trusted-proxy publication behavior.

Pass non-nil prepared **declarative** configs, before runtime DNS expansion, to
`RequiresRestart` and `RestartFingerprint`. Neither mutates configs or loads
configuration. `RequiresRestart` returns `(false, "")` for a permitted hot reload;
otherwise it returns true and the exact first rejection message used by the binary.
Both APIs reject nil: `RequiresRestart` returns true with a reason, and
`RestartFingerprint` returns an error.
`RestartFingerprint` returns `v1:<64 lowercase hexadecimal SHA-256 digits>` or an
error. Equal fingerprints identify the same restart policy
values; rule-only edits do not change them. Comparison and fingerprinting share the
binary's private restart projection, including effective limits, ordered slices
and meaningful nil/pointer distinctions. Authentication timestamps retain their
declared instant, offset and UTC distinction; process-local timezone caches are
excluded. The digest's encoding is versioned; consumers
should persist the entire string.
Fingerprints are comparable only when produced by the same module version.
The unsalted digest covers configured API keys, JWT and client secrets, dashboard
passwords and cluster encryption keys; key it (for example with HMAC and your own
secret) before storing it somewhere less protected than those secrets.

These APIs do not resolve DNS, open listeners, start the cache cluster or configure
tracing. Importing the existing package still brings its proxy/cache/telemetry
dependencies and existing dependency initializers; it is not a lightweight config
package.

---

## Upstream nodes

The `nodes:` list defines one or more upstream Cosmos nodes cosmoguard fronts. With multiple nodes, requests are round-robin'd among the healthy set; unhealthy nodes are evicted from the picker automatically.

```yaml
nodes:
  - name: validator-1               # optional; defaults to "node-0", "node-1", ...
    host: 10.0.0.1
    lcdPort: 1317
    rpcPort: 26657
    grpcPort: 9090
    evmRpcPort: 8545                # only when enableEvm: true
    evmRpcWsPort: 8546
    weight: 1                       # relative share under weighted-round-robin (the default strategy)
    healthcheck:
      enable: true
      path: /status                 # URL path probed (e.g. /status, /node_info)
      service: rpc                  # which upstream port to probe (rpc | lcd)
      interval: 5s
      timeout: 2s
      unhealthyAfter: 3             # consecutive failures before eviction
      healthyAfter: 2               # consecutive successes before re-entry
  - name: validator-2
    host: 10.0.0.2
    # ...
```

`/readyz` returns 503 when zero upstreams are healthy across LCD + RPC pools. Single-node configs with no `healthcheck:` block stay reported-healthy.

**v3 compat:** the singular `node:` block continues to work and is auto-promoted into a 1-element `nodes:` list at load time. Mixing `node:` and `nodes:` is rejected.

### DNS discovery (Kubernetes headless Services)

For autoscaled deployments where pods come and go, a static IP list isn't workable — cosmoguard needs to follow the live pod set. A `nodes:` entry with a `discovery:` block acts as a TEMPLATE: every IP returned by resolving `discovery.host` becomes its own first-class upstream in cosmoguard's LCD, RPC, gRPC, and EVM-RPC pools. A reconcile goroutine re-resolves the host every `refreshInterval` and atomically adds new pods / removes gone ones from each pool.

```yaml
nodes:
  - name: validators                  # used as the upstream name prefix
    rpcPort: 26657
    lcdPort: 1317
    grpcPort: 9090
    discovery:
      type: dns                       # only value supported today
      host: validators.cosmos.svc.cluster.local
      refreshInterval: 15s            # default; matches typical kube-dns TTL
    healthcheck:
      enable: true
      path: /status
      service: rpc
```

Each resolved pod IP becomes an upstream named `<template-name>-<ip>` (e.g. `validators-10.0.0.1`), so `/metrics` labels are stable per pod and the `cosmoguard_upstream_healthy{upstream=...}` gauge gains/loses series as pods come and go.

Per-pod first-class upstreams unlock the per-upstream features (healthcheck eviction, circuit breakers, least-conn picking) that a single ClusterIP Service VIP otherwise collapses: HTTP/2 (gRPC) and persistent WebSocket connections both pin to a single pod when fronted by a VIP. Point `discovery.host` at a **headless** Service so cluster DNS returns one A record per ready endpoint.

**Constraints when `discovery:` is set:**
- `host:` and the per-service URL overrides (`rpcURL`, `lcdURL`, etc.) must be empty — each discovered pod gets its own IP, and per-pod URL overrides aren't expressible.
- A lookup that returns zero records at boot is NOT an error — the reconciler will pick pods up as they appear. A lookup error (DNS unreachable) is also soft-failed at boot.
- Static `nodes:` entries can coexist with discovery templates in the same list.
- WebSocket pools (RPC subs, EVM WS) take the boot-time resolved set only — pod-IP churn there requires a cosmoguard restart in this slice.

---

## Cache

The `cache:` block configures the cache, rate-limiter, and (optional) cluster runtime. v4 has a single backend: an embedded olric daemon fronted by an in-process L1 cache. Single-pod runs it on loopback; including a `cluster:` block flips it into networked mode with peer gossip and replicated DMaps.

```yaml
cache:
  ttl: 5s                                   # global default; rules can override
  key: "cosmoguard-prod"                    # optional key prefix
  coalesce: true                            # global default for single-flight (on); rules can override
  staleWhileRevalidate: 0s                  # global default stale window (0 = off); rules can override
  httpForegroundFetchTimeout: 5m            # detached coalesced HTTP miss safety bound
  grpcForegroundFetchTimeout: 5m            # detached coalesced gRPC miss safety bound
  # memory: …                               # cache memory budget (see below)
  # cluster: …                              # presence enables networked cluster mode
```

`coalesce` and `staleWhileRevalidate` are cluster-wide **defaults** that each rule inherits unless it sets its own — exactly like `ttl`. Both are covered in [Cache features](#cache-features) below. Changing the global `cache:` block requires a process restart (like `cache.ttl`); per-rule overrides hot-reload.

`httpForegroundFetchTimeout` and `grpcForegroundFetchTimeout` bound the detached upstream calls shared by coalesced HTTP and gRPC misses. Both default to `5m`, are independent of any one caller's deadline, and can be increased for cacheable endpoints or unary methods that legitimately take longer. Each caller still stops waiting under its own request context.

A `hit` cache marker means the request used an already stored response. A `miss`
can still share an in-flight fetch or its pending response while the asynchronous
store finishes; it does not imply another upstream request. This distinction is
especially visible under CPU throttling when concurrent clients walk the same
keys in bursts. Use `cosmoguard_upstream_requests_total` to count real upstream
fetches alongside the hit/miss counters. Foreground response stores use detached
goroutines, with no bounded queue or deliberate queue-drop policy; L1 fills before
L2 admission, and `cosmoguard_l2_write_skips_total` records L2 write skips.

### Memory budget

Response L2 storage uses a shared byte-budgeted slab pool in the embedded Olric
runtime. L1 uses LRU with approximate object/payload costs. A separate security
pool holds rate-limit buckets, locks, JWT replay and observability records; response
pressure never evicts those records. The security pool has no finite storage cap.

The automatic budget still comes from the pod's cgroup v1/v2 memory limit:

```
reserve = min(0.50 × limit, max(128 MiB, 0.20 × limit))
budget  = limit − reserve
L1 = 40% of budget, L2 = 60% of budget
```

L1's budget is divided evenly among enabled response caches. L2's **unsplit node
budget** limits charged response backing and metadata across every response DMap,
primary, replica, previous-owner and imported copy. Its cap is neither multiplied
nor divided by the replica factor. Olric's per-DMap `MaxInuse`/`MaxKeys` remain
soft eviction thresholds, with the existing per-map and replica division. The
slab engine supplies the oldest entries to Olric's LRU sampler. Allocation checks
enforce the shared hard cap when LRU cannot make room. A capacity rejection preserves an existing record on failed growth.

Slabs contain 2MiB backing, a charged 32KiB buddy tree and descriptor/index allowance;
records include fixed headers and size-class rounding. Fragment hash indexes grow
with cardinality, mixing the hash independently of the partition assignment. Both
index arrays are charged during growth; a growth that cannot fit rejects the new
write while preserving existing records. Buddy fragmentation
can leave allocated backing at the cap while live inuse bytes are low: free blocks
may not fit the requested size class, and partly used slabs retain their backing. Empty fragments
have a small metadata charge rather than a 1MiB table. Olric compaction visits
every primary and backup fragment once per second, holding the fragment lock
so expiry cannot invalidate an in-progress conditional write or LRU selection. Expired records release
blocks and fully unused slabs release backing and descriptors for natural GC.
Security records expire only according to their existing TTL policies.
Clustered deployments retain **271 partitions** and separate protocol DMaps;
standalone deployments retain 16 partitions. Cross-pod response sharing and the
default replica factor remain unchanged.

`GOMEMLIMIT` remains **90% of the cgroup limit**, and `GOMAXPROCS` follows the CPU
quota. GOMEMLIMIT is a soft GC target, not a process RSS limit. Neither L1's
approximate costs nor the storage cap bounds the whole process. Without a detected
limit, each tier falls back to 128MiB. The automatic response-work allowance G is
`min(64MiB, roundUpToMiB(limit/16))`, or 16MiB without a detected limit.

| Pod limit | Total L1 | Response storage cap | G | GOMEMLIMIT |
| --- | ---: | ---: | ---: | ---: |
| 250Mi | 50Mi | 75Mi | 16Mi | 225Mi |
| 500Mi | 148.8Mi | 223.2Mi | 32Mi | 450Mi |
| 1Gi | 327.68Mi | 491.52Mi | 64Mi | 921.6Mi |
| No detected limit | 128Mi | 128Mi | 16Mi | Not automatically derived |

Both standalone and clustered response adapters share one **128-slot**, byte-
budgeted gate per runtime, with a **100ms total caller budget**. Admission never
queues workers. Known byte values reserve eight times their encoded native entry
size rounded to 4KiB. HTTP and gRPC response wrappers report an encoded-size
upper bound and reserve against that bound before encoding; the encoder enforces
it. Other generic writes reserve 8MiB. A read initially reserves
2MiB plus 4KiB for the native entry, decoded payload and metadata, then shrinks
to twice its encoded size rounded to 4KiB plus 4KiB before decoding. Generic encoding
uses a writer that refuses growth past the native envelope and shrinks the
reservation once size is known. At 250Mi/500Mi/1Gi, G admits 7/15/31
unknown-size reads concurrently. Small reads can reach the 128-slot count cap
once their sizes are known; even near-envelope reads keep total reservations
within 16MiB/32MiB/64MiB. Read decoding runs within the admitted worker.
A timed-out or cancelled caller discards a late result, but the actual worker
retains its slot and byte lease until result delivery or discard. Detached work can still finish a
late write. Limiter and replay work use their independent existing count gates.

Response and security codecs each have a separate 8MiB scratch allowance from
the runtime reserve, admitting two concurrent 4MiB transfer charges. Codec admission is immediate and retryable. A response
import processes every record and can acknowledge omitted capacity-rejected
responses, counted individually; a security callback failure propagates and must
not acknowledge a dropped security record. Export retries retain source data
until acknowledgement. Returned export buffers outlive encoding admission.

Tiered `Set` fills L1 first and returns any L2 failure to Go callers. Response
handlers serve the upstream result and retain that local entry after capacity,
byte-admission, oversize, encoding or backend failure. A native entry must have
`29 + len(key) + len(encoded value) < 1MiB`, with a key no longer than 255 bytes;
large upstream responses can still fit L1. Keys and entry framing reduce the
maximum value payload. Native entry and transfer wire formats remain unchanged.

Per-fragment Olric statistics divide shared slab backing evenly among registered
fragments, assigning division remainders once so aggregate allocation stays exact.
This takes constant time and includes empty registered fragments. `Allocated`
includes backing and charged metadata; `Inuse` includes live size-class blocks and
fragment metadata. Pool gauges provide the node-level cap view. `NumTables` is
the same shared-slab apportionment, not native table count; native table garbage ratios are
not comparable. L1 accounting remains approximate and depends on object shape.

The hard bound does not cover security cardinality, local limiter identities,
application-owned request/response bodies, generic encoder/decoder internals, accepted
connections/pipelined RESP frames, outer fragment decode before Import,
remote response writers/slow readers, or returned export buffers. The cgroup is
still the process limit. See [v6 upgrades](docs/upgrade-v6.md) and the
[release test procedure](docs/bounded-l2-release-tests.md) for measured profiles
and release gates. The local diagnostic breached the 95% peak criterion in some
cases; 250Mi and 500Mi whole-guard acceptance remain pending on-prem evidence.

During a mixed rollout, **old v5 nodes retain native table allocation**: populating
all 271 partitions in four/eight response DMaps costs at least 1,084/2,168MiB of
table backing on one node before replicas, L1 and runtime allocations. Provide
old nodes sufficient memory for full-cache compatibility tests. The new engine
cannot improve an old node's allocator before that node is replaced.

Override any of it explicitly:

```yaml
cache:
  memory:
    maxBytes: 134217728                 # L1 object-cost budget (bytes). unset → auto; 0 → no limit
    maxItems: 0                         # optional L1 entry-count guard. 0 → no limit
    distributedMaxBytesPerNode: 0       # absolute L2 (olric) per-node cap. unset → auto; 0 → no limit
    reserveFraction: 0.20               # auto-mode reserve fraction; must be in [0, 0.9)
```

> **Upgrade note (v4.0.0):** deployments were previously unbounded. After upgrading, entries beyond the budget are LRU-evicted (more upstream traffic under extreme cardinality, never incorrect results), and `GOMEMLIMIT`/`GOMAXPROCS` are set from the container limits. Set `maxBytes: 0` and `distributedMaxBytesPerNode: 0` to restore the old unbounded behavior. Watch `cosmoguard_cache_evictions_total` (which counts budget evictions only, not TTL expiry) — a rising rate means the cap is undersized for your workload.

### Migration from v3

`cache.backend`, `cache.redis`, and `cache.redis-sentinel` were removed in v4. The embedded olric runtime + L1 cache covers every workload — see `bench/RESULTS.md` for the comparison numbers. A config that still contains `cache.redis` or `cache.redis-sentinel` now **fails startup** with a migration error (rather than silently ignoring the field and running an isolated per-pod cache). Remove those keys and, for multi-replica deployments, add a `cluster:` block.

### Cluster mode

Including a `cache.cluster` block in the config turns the embedded olric daemon into a real cluster. Replicas form a memberlist gossip ring and partition the cache + rate-limiter keyspace. Single binary, single daemon, no external dependencies.

Startup starts the operations listener and `/healthz` before waiting for a usable
Olric routing table. `/readyz` returns 503 during construction, then applies the
existing upstream readiness checks. Discovery and daemon start have a fixed 45s
budget. A joiner may continue waiting beyond 45s while the advertised cluster has
quorum and its coordinator answers authenticated PINGs; without that evidence
startup fails. Bootstrap does not wait for all migration to finish. SIGTERM
cancels construction and cleans up immediately, including Olric's graceful leave.
Keep discovery of unready peers enabled. The chart allows 60s for the startup
probe (30 failures at two-second intervals), which checks `/healthz`. Health
already answers 200 while bootstrap waits; readiness carries that wait and keeps
the joiner out of service until its routing table is usable.

SIGTERM immediately changes `/readyz` to 503 while `/healthz`, metrics, information
and application traffic continue serving for a fixed **five seconds**. This lets
endpoints and ingress converge without a preStop hook. After the hold, all
traffic and operations listeners stop concurrently. Admitted HTTP requests and finite gRPC calls drain
until at most **24 seconds from the signal**; gRPC streams are forcibly stopped
at that deadline. WebSocket clients receive a best-effort **1001 (going away)**
close and their connections/notification queues are closed. Clients must reconnect
and resubscribe; streams are not transferred to another replica.

Consumer/telemetry cleanup gets up to **two seconds**, capped at signal +26s,
then Olric gets up to **three seconds** for graceful leave, capped at signal +29s.
All phases share that absolute deadline and shorter caller budgets can curtail
any phase. Uncooperative cleanup remains owned but cannot extend process shutdown.
The binary cannot infer the pod grace period; its fixed **29s total** fits the
operator's 30s grace with one second of margin and no preStop hook. The chart's
existing external 5s preStop and 40s grace remain compatible (at most 34s total).
Failed startup and immediate `Shutdown` skip the propagation hold;
`DrainAndShutdown` is the signal path. Five seconds cannot guarantee convergence
of an unhealthy ingress/control plane; correlate termination with endpoint and
proxy errors when checking a rollout.

Response-cache L2 reads, existence checks and writes share one 128-slot byte gate
per runtime, with a 100ms caller budget. L1 hits bypass it. Admission does not queue
workers. Timeout, capacity rejection and backend outage use the cache-miss path;
coalescing and upstream response handling remain unchanged. Tiered writes fill L1
first, including when L2 rejects capacity, admission, size, encoding or backend
work. Large native entries can still fit L1. Late operations retain their slots
and byte reservations until the actual worker exits and can finish a late write.

After three consecutive executed-operation timeouts, the L2 gate skips backend
calls with `ErrUnavailable`. After one second, the next real request is its single
recovery probe. An on-time healthy reply closes the outage state, including a
cache miss or storage-capacity rejection. Failed probes wait another second;
once a probe caller has resolved, the next cooldown permits a replacement even
if its worker remains stuck. Old workers retain their slot/byte charges, so
replacement probes cannot exceed admission capacities. Caller cancellation/deadlines and
admission/capacity rejection do not open the state. Old in-flight results cannot
close it. There are no background probes or configuration settings. Three
timeouts filter isolated delays; the one-second cooldown bounds recovery traffic.
The state applies across that gate, so a slow partition can temporarily send other
partitions to L1/upstream. Separate limiter and replay pools remain independent.

Clustered limiter attempts retain v5.1.0's token-bucket algorithm and 250ms
lock-contention deadline. A separate **1s** caller-wait budget covers the whole
lock/read/write/unlock attempt, with **2,048** outstanding slots per process.
At capacity, no attempt is queued: the request immediately uses a local bucket.
The 1s budget leaves 750ms beyond the contention deadline for other work; it
was validated with two RF2/quorum1 loopback members and bursts up to 6,000 calls.
A completed contention timeout still denies without falling back. An attempt
that times out or returns a backend error uses the same local bucket path.

Each rule owns one existing in-memory limiter for fallback, with the same rate,
burst, scope, and key. It holds at most 100,000 buckets, with a 10-minute idle
TTL and capacity eviction; eviction can reset an evicted key's burst. Cache and
replay saturation cannot consume limiter slots. There are no retries of uncertain
shared writes. The clustered limiter gate uses the same three-timeout,
one-second, single-request-probe outage state as L2. During an outage requests
immediately use the existing per-replica bucket. A healthy clustered allowance,
denial or contention result closes it. Token-bucket math, locking and the 250ms
contention deadline are unchanged. A failed limiter constructor uses
local buckets and is retried on rule reload.

While falling back, limits are per replica: across N replicas a client can
receive up to N times the configured rate if only local buckets are deciding.
Under sustained saturation, shared and local decisions can coexist: the aggregate
ceiling is the shared rate plus one local rate per replica, or (N + 1) times the
configured rate. Around transitions, the client can
also receive the local burst in addition to tokens already granted by the shared
bucket. Late shared operations can consume tokens after a local decision, making
later shared decisions stricter. The local limiter retains its existing bounded
bucket-storage behavior. A client keeping one replica's limiter pool saturated
(thousands of concurrent calls on one key) pushes its other rules to per-replica
decisions; this costs at most the documented aggregate bound and does not deny a
key merely because the pool is full.

`rateLimit.failureMode` is **deprecated and ignored**, but remains parsed and
validated so existing configurations load and reload. On primary limiter failure,
the per-replica limiter decides in every deployment mode. v6 retains this key. Startup logs one warning naming affected rules; an accepted
reload warns once for rules that introduce the key. The key is absent from rule
fingerprints and the restart projection, so changing it does not alter bucket identity
or require a restart. Auth-method `failureMode` settings are unchanged.

Clustered JWT replay checks have 512 slots. At capacity, they wait up to
100ms for admission without spawning a backend worker, then get a separate
full 100ms Put budget: worst-case caller wait is 200ms. Timeout uses the existing replay-store error policy: admit the verified
identity and log a warning that replay protection was unavailable. This policy
is unchanged; the wait is bounded. Replay admission shares neither main pool.
Replay deliberately has no outage suppression: every request still attempts its
own bounded atomic NX check. Skipping checks during a gate-wide outage would
admit tokens that an available partition could still reject.

Non-clustered deployments use the same response storage and byte gate. They add
no limiter or replay admission pools, retain the embedded limiter's 250ms
contention deadline, and share the 45s startup default. The healthy limiter
algorithm is unchanged. Embedded limiter errors, failed constructors, or a missing primary
limiter use the same per-rule local fallback; a rate-limited rule never bypasses
its limit because the primary is unavailable. Caller cancellation observed before
local fallback begins returns the context error without consuming a fallback token or counting a fallback.

Underlying olric calls may ignore cancellation, so each slot stays occupied until
the actual call returns. Late writes or token consumption are possible after the
client stops waiting. Per-pod write locks can be released before timed-out Puts
finish; an older write can overwrite a newer one, but the embedded stored-at
timestamp still determines freshness. The limiter's existing 2s lease does not
guarantee mutual exclusion for a critical section stalled beyond that lease.
Bounded workers recover backend panics into errors, log the stack once at error
level, and release their slots.

`cosmoguard_backend_operation_failures_total` counts abandoned waits and capacity
rejections. Its labels are `backend` (`l2`, `limiter`, or `replay`) and `outcome`
(`timeout`, `rejected` or `unavailable`) for L2 and limiter, plus replay
`timeout`: seven live combinations. Replay waits for capacity and never emits `rejected`. It does not count cache misses,
ordinary contention denials, backend panics, or caller context cancellation/deadlines.
Gate-budget expiry or an executed Olric operation-timeout reply counts as a backend timeout. Bounded cache failures, local L2 admission/size/encoding skips and all limiter
fallback decisions log at debug level. L2 write skips with `reason="backend"`
and other cache errors remain errors.
JWT replay failures retain their warning for the existing security audit path.

Storage metrics use only `pool="response"` or `pool="security"`:
`cosmoguard_l2_storage_allocated_bytes`, `cosmoguard_l2_storage_inuse_bytes`,
`cosmoguard_l2_storage_entries`, `cosmoguard_l2_storage_capacity_bytes`,
`cosmoguard_l2_codec_bytes` and `cosmoguard_l2_codec_capacity_bytes`. Capacity zero
means unlimited. `cosmoguard_l2_operation_bytes` and
`cosmoguard_l2_operation_capacity_bytes` report G, including detached workers.
Gauges aggregate active runtimes; closed runtimes release their collector references.
`cosmoguard_l2_last_compaction_timestamp_seconds{pool}` reports the latest completed
storage expiry sweep in an active runtime. Zero means no sweep has completed;
an unchanged value while fragments remain populated indicates stalled expiry.
Empty pools have no fragment to sweep, so their timestamps can remain unchanged.
`cosmoguard_gc_cpu_seconds_total` and `cosmoguard_gc_limiter_last_enabled_cycle`
provide process runtime observations for the soak ledger, without labels.

`cosmoguard_l2_storage_rejections_total{path="fork"|"put"|"put_raw"}` counts receiving
response capacity rejections; `cosmoguard_l2_import_dropped_entries_total` counts
capacity omissions acknowledged during response transfer.
`cosmoguard_l2_write_skips_total{reason}` uses `inflight`, `unavailable`, `storage_capacity`,
`entry_size`, `backend`, or `encode`. The adapter restores the unique capacity message after remote RESP forwarding
so it remains `storage_capacity`; Olric's opaque write-quorum failure stays `backend`. A quorum
success with a rejected backup increments receiving storage rejection only.
Counters are process cumulative and never label keys, tenants or DMap names.
`cosmoguard_cache_evictions_total` retains its L1 budget-eviction meaning.

`cosmoguard_rate_limit_local_fallback_total` records local decisions with eight
combinations: `reason` (`timeout`, `capacity`, `backend_error`, `backend_unavailable`) and `outcome`
(`allowed`, `denied`). Healthy bursts can also enter local fallback at capacity
or when the attempt exceeds 1s. Alert on
`cosmoguard_rate_limit_local_fallback_total{reason="capacity"}` to detect limiter
pool saturation. Replay retains its existing error policy when
either 100ms budget expires.
`cosmoguard_backend_unavailable_gates{backend}` counts outage gates, including a
probe in progress, with fixed labels `l2`, `limiter`, and `replay` (always zero).
Recovery requires a real request: on an idle pod this gauge can remain open even
when the backend has recovered. It records the last observed outage state, not
an active health check. Closed runtimes decrement their L2 state. No new YAML
settings are required.

The three pools retain at most 2,688 backend workers: 128 L2, 2,048 limiter,
and 512 replay. Parked L2 writes retain at most 1 MiB of encoded payload per
slot (including spare encoder capacity), or **128 MiB** across the L2 pool.
Runtime stacks, contexts and result channels add overhead. This is not a hard
total-process memory cap: olric's embedded Put can retain two additional value
copies, making three payload copies alone up to **384 MiB**, before keys,
transport/serialization overhead, active upstream captures, the configured cache
working set and per-rule local fallback buckets (up to 100,000 each).

Cross-pod replication of the dashboard observability snapshot (so a restarting pod restores its counters + metrics history from a peer) is **off by default** and opt-in via `dashboard.clusterHistoryRestore: true` — see [Dashboard restart-restore](#dashboard-restart-restore-off-by-default) below. The live cluster dashboard (peer HTTP fan-out) and Prometheus `/metrics` do **not** depend on it.

```yaml
cache:
  cluster:
    bindAddr: "${POD_IP}"   # routable per-pod IP — wildcard (0.0.0.0, ::) is rejected
    bindPort: 3320          # TCP — olric internal RESP socket
    gossipPort: 3322        # TCP + UDP — both required (operators block UDP by reflex)
    peerApiPort: 0          # 0 → bindPort + 1, used for the cluster dashboard fan-out
    replicaCount: 2         # RF=2 — primary + one replica per partition
    quorum: 1               # 2 for split-brain-proof mode with replicaCount=3
    encryptionKey: "${CLUSTER_KEY}"  # REQUIRED — base64 16/24/32-byte gossip encryption key
    discovery:
      mode: dns             # required; see below
      dns:
        host: cosmoguard-peers.cosmoguard.svc.cluster.local
        refreshInterval: 10s
```

**`encryptionKey` is required in cluster mode.** It enables memberlist gossip encryption + peer authentication (AES-128/192/256-GCM), password authentication on olric's RESP data port, and a derived HMAC key for dashboard peer-API requests. Peer-API signatures use `X-Cosmoguard-Peer-Timestamp` and `X-Cosmoguard-Peer-Signature` and cover the timestamp, method, authority, path, and raw query. They are accepted for 30 seconds before or after the receiver's clock to tolerate ordinary pod clock skew, so a signed request can be replayed within that window. The HMAC authenticates requests but does **not** encrypt the peer API, so NetworkPolicy remains its confidentiality boundary. Generate the shared key with `head -c32 /dev/urandom | base64` and give **every pod the same value** from a Kubernetes Secret.

Peer fan-out has no unsigned compatibility mode. During a rolling upgrade between versions that do and do not sign peer requests, cluster dashboard panels can show partial data until every pod runs the same version; the public dashboard and proxy traffic continue to operate normally.

#### Discovery modes

Four modes ship. There is **no default** — `discovery.mode` must be set explicitly when a `cluster:` block is present.

| Mode | Status | Notes |
| --- | --- | --- |
| `dns` | supported | Resolves a headless service / SRV record. The recommended mode in Kubernetes and any environment with a service registry. |
| `static` | supported | Explicit peer list. Useful for fixed-topology bare-metal deploys and integration tests. |
| `kubernetes` | experimental | Native K8s API discovery via olric's plugin. Built on `.so` plugins upstream — prefer `dns` unless you need label-selector discovery. |
| `mdns` | experimental | Zero-config LAN discovery. Useful for local dev clusters; not recommended for production. |

```yaml
discovery:
  mode: static
  static:
    peers:
      - cosmoguard-0.cosmoguard-peers:3322
      - cosmoguard-1.cosmoguard-peers:3322
      - cosmoguard-2.cosmoguard-peers:3322
```

Bare hosts (no `:port`) are accepted — `cluster.gossipPort` is appended at runtime — and self-references whose host matches `cluster.bindAddr` are filtered out, so the same config can be deployed to every pod when peers are listed by IP. If peers are listed by hostname (e.g. `cosmoguard-0.cosmoguard-peers:3322`) and `bindAddr` is an IP, the filter cannot match and each pod will see itself once in its peer list — olric tolerates that harmlessly (no self-join, no crash).

#### Operational notes

- **TCP + UDP on `gossipPort`** — memberlist gossips over both. Operators who block UDP by reflex break cluster joins; open both protocols on the same port (`bindPort` is olric's data-replication socket and only needs TCP).
- **Three ports per pod, not two** — `bindPort` (3320, TCP), `gossipPort` (3322, TCP + UDP) and `peerApiPort` (defaults to `bindPort + 1` → 3321, TCP) all need to be reachable pod-to-pod. The peer-API listener is what the dashboard fan-out aggregator calls on its siblings; default-deny `NetworkPolicy` setups must allow it explicitly. It requires a valid key-derived HMAC and either a source IP in the current memberlist roster or a loopback source. Keep it pod-network-only because the HMAC authenticates but does not encrypt its HTTP traffic.
- **RF=2 default** — every partition has one primary + one replica. Survives a single-pod restart cleanly. Survives a single-pod permanent loss with re-balancing.
- **2 vs 3 replicas** — both supported.
  - **3 replicas** *(recommended)*: RF=2, quorum=2, textbook no-split-brain configuration.
  - **2 replicas**: cost-sensitive deploys. Set `replicaCount: 2` and accept a documented trade-off: a brief network partition where the two pods disagree about token-bucket state can double-bill rate-limited callers for the duration of the partition. The cache and observability subsystems are unaffected.
- **No persistence** — olric runs in-memory only. Single-pod restart survives via DMap replication (the rejoining pod streams its data back from a peer). **Full-cluster restart loses cache + rate-limit state** (and observability state, when `dashboard.clusterHistoryRestore` is enabled), which is the explicit v4 design trade-off.
- **Pod identity matters for observability survival** — *only when `dashboard.clusterHistoryRestore` is enabled* (off by default). The dashboard observability snapshot is then keyed by hostname: StatefulSet gives stable identities (`cosmoguard-0` stays `cosmoguard-0`) so a restarting pod finds its own previous snapshot on a peer. Deployment ephemeral identities work but new-pod replacement loses that pod's observability rollover — cache and rate-limit are unaffected.

#### Dashboard restart-restore (off by default)

`dashboard.clusterHistoryRestore` gates cross-pod replication of the dashboard observability snapshot — the mechanism that lets a pod **restore** its counters + metrics history from a peer after a rolling restart. It is **off by default**, and you should leave it off unless you specifically need that history to survive a restart.

```yaml
dashboard:
  enable: true
  clusterHistoryRestore: false   # default; set true to opt into restart-restore
```

**What the flag does and does not affect.** It gates *only* the cross-pod restart-restore. The live dashboard time-series panels (`/api/v1/metrics/history`, and the cluster `/api/v1/cluster/metrics/history` fan-out) are fed by an in-process sampler that runs **regardless** of this flag, so they populate normally on a healthy pod either way — only their *survival across a restart* is gated. The live cluster dashboard, peer fan-out, and Prometheus `/metrics` are likewise unaffected.

**Why it defaults off:** when enabled, each pod rewrites a large observability blob to a replicated (RF2) olric DMap every 30s. olric's log-structured kvstore accumulates storage tables from that frequent large-value overwrite without bound, so under real cluster traffic — where the blob grows as request cardinality climbs — the pod heap grows until it is **OOMKilled**. When disabled, a rolling restart simply starts each pod's dashboard counters cold — no functional loss beyond the lost history.

**Requires cluster mode.** Restart-restore reads a peer's replica, so the flag only takes effect when a `cache.cluster` block is present. In a single-pod / embedded deployment (no cluster block) there are no peers to restore from, so the flag is ignored; cosmoguard logs a warning if it is set without cluster mode. Note the gate is the cluster *config*, not live peer count: a cluster that is configured but currently **solo or degraded** (no reachable peers) still performs the 30s DMap writes — and still incurs the memory growth above — with nothing to restore. Only enable it on a healthy multi-pod cluster. Changing the flag on a running pod is rejected on hot-reload as **requires a process restart** — it is wired once at startup.

#### Kubernetes example — StatefulSet + headless Service

The recommended shape. Headless `Service` gives every pod a DNS record `cosmoguard-N.cosmoguard-peers.namespace.svc.cluster.local`; `StatefulSet` pins stable identities so observability survives rolling restarts; downward-API `POD_IP` lets each pod advertise the right bind address. No `volumeClaimTemplates` — there is no on-disk persistence layer.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cosmoguard-peers
spec:
  clusterIP: None          # headless — required for DNS discovery
  selector:
    app: cosmoguard
  ports:
  - name: olric-data
    port: 3320
    protocol: TCP          # olric internal RESP socket (data replication)
  - name: gossip-tcp
    port: 3322
    protocol: TCP
  - name: gossip-udp
    port: 3322
    protocol: UDP          # memberlist needs UDP too — don't forget
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cosmoguard
spec:
  serviceName: cosmoguard-peers
  replicas: 3
  selector:
    matchLabels:
      app: cosmoguard
  template:
    metadata:
      labels:
        app: cosmoguard
    spec:
      containers:
      - name: cosmoguard
        image: ghcr.io/voluzi/cosmoguard:v4
        env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        ports:
        - name: olric-data
          containerPort: 3320
          protocol: TCP
        - name: peer-api
          containerPort: 3321        # bindPort + 1 — dashboard fan-out
          protocol: TCP
        - name: gossip-tcp
          containerPort: 3322
          protocol: TCP
        - name: gossip-udp
          containerPort: 3322
          protocol: UDP
```

Pair with:

```yaml
cache:
  cluster:
    bindAddr: "${POD_IP}"
    bindPort: 3320
    gossipPort: 3322
    replicaCount: 2
    encryptionKey: "${CLUSTER_KEY}"
    discovery:
      mode: dns
      dns:
        host: cosmoguard-peers.cosmoguard.svc.cluster.local
        refreshInterval: 10s
```

Deployments (instead of StatefulSet) are supported too — the only caveat is that per-pod observability snapshots are lost on pod replacement because new pods get new hostnames. Cache and rate-limit data is unaffected.

---

## Authentication

The `auth:` block gates rules on a resolved Identity. Four auth methods ship in v4.0:

```yaml
auth:
  enable: true
  defaultRequire: false              # when true, every rule requires auth unless opted out
  methods:
    - type: api-key
      header: Authorization          # accepts "Bearer <key>" or raw "<key>"
      queryParam: api_key            # optional fallback to ?api_key=...
    - type: jwt
      header: Authorization
      algorithm: HS256               # HMAC (HS256/HS384/HS512), RSA (RS256/RS384/RS512), ECDSA (ES256/ES384/ES512), EdDSA — set to one for strict pinning or omit when using JWKS
      secret: "${JWT_SECRET}"        # HMAC only; for RSA/ECDSA/EdDSA use publicKey or jwksURL
      identityClaim: sub             # default: sub
      scopesClaim: scope             # default: scope (OAuth-style space-separated)
      audience: cosmoguard-prod      # optional aud check
      issuer: https://auth.example.com   # optional iss check
      # jwksURL: https://auth.example.com/.well-known/jwks.json  # auto-refresh; multi-kid + RS/ES/EdDSA supported
    - type: external-validator
      endpoint: https://api.allora.network/v1/keys/validate
      validatorMethod: GET           # GET (default) or POST
      header: Authorization          # incoming credential header
      forwardHeader: Authorization   # same name to validator (default)
      responseValid: data.valid      # JSON dot-path; must evaluate truthy
      responseIdentity: data.userId
      responseScopes: data.scopes
      cacheTTL: 60s
      timeout: 2s
      failureMode: fail-closed       # or fail-open (default)

  identities:                        # inline registry (api-key)
    - name: prod-webapp
      apiKey: "${PROD_API_KEY}"
      scopes: [read, broadcast]
      validUntil: 2026-01-01T00:00:00Z

  anonymous:                         # the identity used for unauthenticated requests
    name: anonymous
    scopes: []

  replayProtection:                  # optional JWT replay protection
    enable: true                     # rejects re-used jti within token TTL
```

Per-identity rate limits are expressed at the **rule** level via `rateLimit: { rate: 10/s, scope: per-identity }` (see the Rate limiting section). That covers the most common "give this api key its own quota" pattern without growing a separate enforcement surface — and the rule layer is the one the proxy already runs on every request.

When `auth.replayProtection.enable` is true and a verified JWT carries a `jti` claim, cosmoguard checks a seen-set keyed on `(issuer, jti)`. A repeat within the token's expiration window is rejected with **HTTP 401** and `reason=token replayed` in the audit log. Tokens without `jti` are not enforced — replay protection requires the IdP to mint unique identifiers. The store is backed by olric when cluster mode is on (so replicas share the seen-set); otherwise in-process with a periodic-GC sweep.

Credential-carrying headers are **always stripped** before forwarding upstream — a Cosmos node never sees cosmoguard's auth headers. The strip set is derived from the configured methods (you don't list it manually). An api-key passed via `queryParam` is likewise redacted from the dashboard's request log and cardinality samples.

**Auth endpoints must use https.** `jwksUrl`, `introspectionEndpoint`, and the external-validator `endpoint` are rejected at startup unless they use `https` (or point at loopback for local dev). Over plaintext http an on-path attacker could serve a forged JWKS or validator response and mint a fully-trusted identity — a complete authentication bypass.

Per-rule auth gate:

```yaml
rules:
  - action: allow
    match:
      paths: [/cosmos/tx/v1beta1/txs]
      methods: [POST]
    auth:
      require: true
      scopes: [broadcast]            # ALL of these scopes must be held
      identities: [prod-webapp]      # OR (overrides scopes): closed allowlist
```

---

## CORS

The `cors:` block lets cosmoguard own cross-origin policy completely — upstream's CORS headers are stripped and replaced with cosmoguard's.

```yaml
cors:
  enable: true
  allowedOrigins:
    - https://app.example.com
    - https://*.preview.example.com  # glob; `*` doesn't cross slashes or label boundaries
  allowedMethods: [GET, POST, OPTIONS]
  allowedHeaders: [Authorization, Content-Type]
  exposeHeaders: [X-Request-Id]
  maxAge: 1h
  credentials: false                  # set true to allow cookies/auth; "*" disallowed in that mode
```

Preflight (`OPTIONS` + `Access-Control-Request-Method`) is handled directly by cosmoguard — never forwarded upstream.

---

## Tracing

Optional OpenTelemetry tracing. Off by default; when enabled, one span per request is emitted to an OTLP collector. Even when off, cosmoguard installs the W3C propagator so existing `traceparent` headers flow through to the upstream untouched.

```yaml
tracing:
  enable: true
  endpoint: otel-collector:4317     # OTLP receiver address
  protocol: grpc                    # grpc (default) | http
  serviceName: cosmoguard           # appears as service.name on every span
  sampleRate: 1.0                   # 0.0–1.0 (TraceIDRatioBased); default 1.0
```

Span shape:

- **HTTP / JSON-RPC HTTP**: server-kind span named `"{METHOD} {path}"`, attributes `http.method`, `http.target`, `cosmoguard.proxy`.
- **JSON-RPC WebSocket**: one span per connection lifetime.
- **gRPC**: server-kind span named `"grpc {method}"`, attributes `rpc.system=grpc`, `rpc.method`.

The reverse-proxy Director injects the active span context as `traceparent` on every outbound upstream request, so the Cosmos node's logs / downstream services join the same trace.

---

## Protocol sections

Each protocol gets its own block with a `default:` action (`allow` or `deny`) and an ordered `rules:` list:

```yaml
lcd:
  default: deny
  rules: [ ... ]                     # HTTP rules

rpc:
  default: deny
  webSocketEnabled: true
  webSocketConnections: 40           # total WS conns across all backends; spread evenly
  rules: [ ... ]                     # HTTP rules
  jsonrpc:
    default: deny
    maxBatchSize: 100                # cap on JSON-RPC batch size; 413 on excess
    rules: [ ... ]                   # JSON-RPC rules

grpc:
  default: deny
  maxRecvMsgSize: 10485760           # bytes; largest request message accepted from clients
  maxSendMsgSize: 2147483647         # bytes; largest response message relayed to clients
  rules: [ ... ]                     # gRPC rules

evm:                                 # only when enableEvm: true
  rpc:
    default: deny
    rules: [ ... ]                   # JSON-RPC rules
    httpRules: [ ... ]               # HTTP rules
  ws:
    default: deny
    webSocketConnections: 40
    rules: [ ... ]                   # JSON-RPC rules
```

---

## Rules

All rules share `priority`, `action`, and an optional `match:` block. Lower priority numbers match first; first-match-wins.
Each rule must set `action` to exactly `allow` or `deny`. Section `default` values accept the same two actions. Unknown configuration keys and additional YAML documents are rejected during validation.

### HTTP rule

| Field | Default | Description |
|---|---|---|
| `priority` | `1000` | Lower = higher precedence. |
| `action` | — | `allow` or `deny`. |
| `match` | empty | v4 expressive matcher. Empty = matches every request. |
| `paths` / `methods` / `query` | empty | v3 flat syntax. Auto-desugars into `match:`. |
| `cache` | nil | Cache settings (see below). |
| `rateLimit` | nil | Throttling settings (see below). |
| `auth` | nil | Auth gate (see [Authentication](#authentication)). |

#### Expressive matcher

The `match:` block builds a tree of combinators and atoms:

```yaml
match:
  all:                               # AND: every child must match
    - path: /block                   # single-value atom
    - paths: [/block, /commit]       # multi-value atom (any of)
    - query:                         # presence-check (key must exist)
        height: present
  any:                               # OR: at least one child must match
    - method: GET
    - method: HEAD
  none:                              # NOT: no child may match
    - header:
        x-debug: present
  # Leaf atoms at this level are an implicit `all`:
  sourceIP: 10.0.0.0/8                # CIDR or single IP
  header:                            # glob match on header value
    authorization: "Bearer *"
```

Multi-value atoms exist for `paths` and `methods` and behave as "any of": the atom matches if the request value equals (or globs to) any list entry. The singular `path`/`method` forms remain for single-value rules.

Atom values support these forms:
- `"present"` — the key must be set on the request (any non-empty value).
- `"absent"` — the key must NOT be set.
- `"<glob>"` — globs use `*`, `?`, `[...]`; `*` does not cross path separators.
- `"<literal>"` — exact equality.

> **v3 note:** values `present` and `absent` are reserved keywords ONLY in v4 `match:` blocks. In v3 flat `query:` they remain literal exact-match strings — your old config behaves identically.

#### Cache

```yaml
cache:
  enable: true
  ttl: 1h                            # 0 means use global cache.ttl
  cacheError: false                  # cache non-2xx responses?
  cacheEmptyResult: false            # cache JSON-RPC results that came back null?
  coalesce: true                     # single-flight concurrent misses (0/unset = inherit global, default on)
  staleWhileRevalidate: 30s          # serve stale up to this long while refreshing (0/unset = inherit global)
  disableStaleWhileRevalidate: false # set true to opt this rule out of an inherited global stale window
  preserveHeaders:                   # additional headers to replay on hit
    - Content-Encoding               # (Content-Type / Cache-Control / ETag / Vary
    - X-Custom-Header                #  are always preserved)
  keyMetadata:                       # request headers folded into HTTP cache keys
    - x-cosmos-block-height
    - grpc-metadata-x-cosmos-block-height
```

Cache keys are namespaced per rule fingerprint — two rules matching the same request never share entries.

##### Cache features

Both `coalesce` and `staleWhileRevalidate` are per-rule overrides of the global `cache.*` defaults, inherited when unset (an unset/`0` value falls through to the global, exactly as `ttl: 0` inherits `cache.ttl`).

- **`coalesce` (single-flight)** — on by default. When a cacheable key is a hard miss, only ONE request fetches upstream; concurrent requests for the same key wait and share that result. This collapses the thundering herd that hits the upstream every time a hot key expires. Set `coalesce: false` to disable per rule (each concurrent miss then fetches independently — the previous behaviour).
- **`staleWhileRevalidate` (serve-stale)** — off by default (`0`). When set, an entry that has passed its `ttl` but is still within the window is served **immediately** (with `X-Cosmoguard-Cache: stale`, or `x-cosmoguard-cache: stale` metadata for gRPC) while ONE background request refreshes it — so the client never waits on the upstream and the entry stays warm. Requires a positive freshness `ttl` (the rule's `ttl`, or the global default) to extend. Set `disableStaleWhileRevalidate: true` on a rule that must opt out of a positive global window; it cannot be combined with a positive per-rule window.
- **`keyMetadata` (response-affecting request metadata)** — applies to HTTP-family and gRPC rules. HTTP-family rules default to `x-cosmos-block-height` and `grpc-metadata-x-cosmos-block-height`; gRPC defaults to `x-cosmos-block-height`. A non-empty list replaces the protocol default, so include the defaults explicitly when adding custom dimensions. `keyMetadata: []` opts out. HTTP names are case-insensitive, and all values participate in their received order with value boundaries preserved. `Host` uses the request authority. `X-Forwarded-Host` also uses a non-empty request authority, but retains the inbound header when the authority is empty; `X-Forwarded-Proto` uses `http` or `https` according to the inbound transport. These derived values match what CosmoGuard sends upstream and ignore overwritten request headers. The two HTTP height aliases remain independent dimensions because gateways can interpret them differently. JSON-RPC message caches and WebSocket caches do not use this setting.

Scope and caveats:
- Coalescing applies to HTTP-family rules (LCD, RPC-HTTP, EVM-RPC-HTTP), gRPC rules, and JSON-RPC **single** requests over HTTP or WebSocket. gRPC responses also carry `x-cosmoguard-cache` response metadata (`hit`/`miss`/`stale`) alongside the upstream's own response metadata and trailers (such as `x-cosmos-block-height`), which are cached with the payload.
- Serve-stale SWR applies to HTTP-family rules, gRPC, and JSON-RPC single requests over HTTP. WebSocket single requests coalesce stale revalidation but wait for the refreshed response instead of serving stale data.
- HTTP responses carrying `Cache-Control: must-revalidate` or `proxy-revalidate` are never served stale, even when SWR is enabled.
- **JSON-RPC batch** items are freshness-aware (a stale entry is revalidated as part of the aggregated batch call) but are not individually coalesced or served stale — a batch already collapses its misses into a single upstream call, so there is nothing to coalesce.
- Coalescing is **per-pod** (in-process). Across a cluster the shared olric L2 dedups the stored value and serves subsequent reads cluster-wide, so per-pod single-flight already cuts an expiry stampede from N-per-pod to ~1-per-pod.
- A coalesced miss buffers the upstream response fully before replying (it can't stream to N waiters); for cacheable responses this only shifts first-byte latency. Set `coalesce: false` on a rule if you need streaming on the miss path.
- HTTP-family and single-request JSON-RPC response capture is limited to 32 MiB per active response. With `coalesce: false`, larger responses continue streaming in full but are not cached. A coalesced oversized miss returns HTTP 502 for HTTP-family rules or JSON-RPC `-32603` (`upstream error`) for single JSON-RPC requests; it is never fetched a second time. An oversized background refresh leaves the existing stale entry unchanged within its current stale window.
- The 32 MiB bound limits retained capture for each active response, not total process memory. N concurrent distinct cache keys may retain up to N × 32 MiB, in addition to transport, parser, and cache overhead; configured cache-tier budgets do not include these active response buffers. Bytes streamed beyond a response's limit are drained without increasing its retained capture.
- HTTP cache admission remains conservative: an upstream `Vary` field other than `Accept-Encoding` or `Origin` prevents storage even when that field is listed in `keyMetadata`. Adding a request header to the key does not make a response with that `Vary` value cacheable.
- `Vary: Origin` from a node with CORS enabled (CometBFT and Cosmos SDK send it on every response) does not prevent caching when the answer is the same for every origin: `Access-Control-Allow-Origin: *` for a request with an `Origin`, no CORS headers for one without. Such a response is shared by all origins; the key only separates requests with and without an `Origin`, and the wildcard CORS header is replayed on hits. The same applies to JSON-RPC over HTTP (single and batch requests); JSON-RPC requests without an `Origin` share their entries with the WebSocket path. A response whose CORS answer depends on the origin is not cached.

#### Rate limit

```yaml
rateLimit:
  rate: 100/s                        # or "100", "30/min", "1/5s", "250/250ms"
  burst: 200                         # max bucket capacity; defaults to rate
  scope: per-ip                      # per-ip (default) | global | per-identity | compound
  failureMode: fail-open             # deprecated and ignored; retained in v6
```

With a `cache.cluster` block present, rate-limit buckets are sharded across replicas through olric so the configured rate is a true cluster-wide budget. In single-pod / embedded olric mode the rate is enforced per pod.

`rateLimit.failureMode` is deprecated and ignored, and remains accepted in v6. Existing `fail-open` and `fail-closed` values remain valid.
On backend failure, the per-replica limiter decides in all modes across HTTP,
JSON-RPC, WebSocket, and gRPC; see [cluster mode](#cluster-mode) for the bounds
and transition behavior. Auth-method `failureMode` is unaffected.
When `rateLimit` is set on a rule, `rate` is required and must be a finite positive number. `burst` must be non-negative; `0` uses the default capacity.

#### Examples

```yaml
rules:
  # Height-pinned reads are immutable — cache aggressively.
  - priority: 100
    action: allow
    match:
      all:
        - paths: [/block, /commit, /block_results]
        - query:
            height: "[0-9]*"
    cache: { enable: true, ttl: 1h }

  # Same paths without `height` always return the chain tip — never cache.
  - priority: 200
    action: allow
    match:
      paths: [/block, /commit, /block_results]
      methods: [GET]

  # Tx broadcast: rate-limit per identity, require write scope.
  - priority: 100
    action: allow
    match:
      paths: [/cosmos/tx/v1beta1/txs]
      methods: [POST]
    auth:
      require: true
      scopes: [broadcast]
    rateLimit:
      rate: 10/s
      scope: per-identity

  # Internal-only path: deny if any cross-origin Origin header is present.
  - priority: 50
    action: deny
    match:
      all:
        - path: /internal/*
        - header:
            origin: present
```

### JSON-RPC rule

| Field | Default | Description |
|---|---|---|
| `priority` | `1000` | |
| `action` | — | `allow` or `deny`. |
| `methods` | empty | JSON-RPC method names (globs supported). Empty = all. |
| `params` | empty | Flat object subset or positional-array prefix of scalar predicates. |
| `cache` | nil | Same shape as HTTP. |

```yaml
rules:
  - action: allow
    methods: [subscribe, unsubscribe, unsubscribe_all]

  - action: allow
    methods: [abci_query]
    params:
      path: /cosmos.bank.v1beta1.Query/AllBalances
    cache:
      enable: true
      ttl: 2s
```

`params` accepts only flat scalar values: strings, booleans, null, finite
numbers, and integers from `-9007199254740991` through
`9007199254740991`. Strings use glob matching. Other scalars use exact
matching after numeric normalization, so YAML `10` matches JSON `10`, `10.0`,
or `1e1`. In object form every configured key must be present (including keys
configured as null), while additional request keys are allowed. In array form
the configured values match a prefix, so additional trailing request values
are allowed. Nested objects/arrays, timestamps, non-finite numbers, unsupported
top-level shapes, and integers outside the exact range reject the config at
startup or reload.

A reload re-applies the rules to live WebSocket subscriptions, matching each
against its original subscribe request. A subscription the new rules deny is
unsubscribed. A Cosmos client receives CometBFT's cancellation error under its
subscribe id. `eth_subscribe` has no cancellation message, so an EVM client's
connection is closed, together with its other subscriptions. The rule action
and per-rule `auth` (against the identity the connection authenticated with)
are re-applied; `rateLimit` is not, since a live subscription makes no further
requests. Revocations appear in the dashboard's denials.

### gRPC rule

| Field | Default | Description |
|---|---|---|
| `priority` | `1000` | |
| `action` | — | `allow` or `deny`. |
| `methods` | empty | Fully-qualified gRPC methods (globs supported). |
| `cache.enable` | `false` | Cache unary responses keyed by (rule fingerprint, method, payload). |
| `cache.ttl` | `5s` | Per-rule TTL. |
| `cache.keyMode` | `raw` | `raw` hashes payload bytes verbatim; `method-only` excludes payload (parameter-less queries only); `canonical` decodes payload against operator-supplied protoset descriptors and re-encodes deterministically before hashing — cache hits across clients with different serialization (field order, default-vs-absent). |
| `cache.keyMetadata` | `x-cosmos-block-height` | Metadata keys folded into the cache key; see [Cache features](#cache-features). |

`grpc.maxRecvMsgSize` and `grpc.maxSendMsgSize` default to the Cosmos SDK node's own gRPC limits (10 MiB in, 2 GiB − 1 out), so the proxy relays every message the node serves. Unset or `0` keeps the default, negative values are rejected, and changing them requires a process restart. The gRPC listener also caps each client connection at 1000 concurrent streams (further streams queue) and pings idle clients every 2 minutes.

For `keyMode: canonical`, set `grpc.protosets:` at the top level. Each path is a binary `FileDescriptorSet` produced by `protoc --descriptor_set_out=foo.protoset -I path/to/protos path/to/protos/**/*.proto`. Methods absent from the loaded protosets silently degrade to `raw`.

Changing the protoset path list hot-reloads the registry. Restart-policy rejection
is checked before reading protosets and takes precedence over file errors. The
new files are fully loaded and validated before config, limits or rules are
changed; a load failure
rejects the whole reload as `invalid` and preserves the previous config and
registry. An unchanged list does not reopen files, keeping unrelated rule reloads
independent of descriptor-file access. Protoset order is significant: reordering
the list reloads the registry, so keep the order identical across replicas.
To load an edited bundle, change its path
(for example, use a versioned filename). Clearing the list disables
canonicalization. Entries cached under the previous descriptors can be served
until their TTL expires.

```yaml
grpc:
  protosets:
    - /etc/cosmoguard/cosmos-sdk.protoset
    - /etc/cosmoguard/ibc-go.protoset
  default: deny
  rules:
    - action: allow
      methods:
        - /cosmos.bank.v1beta1.Query/AllBalances
        - /cosmos.bank.v1beta1.Query/Balance
      cache:
        enable: true
        ttl: 10s
        keyMode: canonical          # client-agnostic cache key
    - action: allow
      methods: ["/cosmos.bank.v1beta1.Query/Params"]
      cache:
        enable: true
        ttl: 60s
        keyMode: method-only        # no parameters; one entry per method
```

gRPC retains its `x-cosmos-block-height` default. Override it only when a method has other response-affecting metadata, or use an explicit empty list on a genuinely height-independent method:

```yaml
      cache:
        enable: true
        ttl: 5s
        keyMetadata: ["x-cosmos-block-height"]   # default; requests at different heights cache separately
```

---

## Env-var interpolation

Any string in the YAML can reference an env var. Useful for secrets:

```yaml
auth:
  identities:
    - name: prod
      apiKey: "${PROD_API_KEY}"            # required; load fails if unset
    - name: dev
      apiKey: "${DEV_API_KEY:-dev-fallback}"   # default when unset
    - name: ci
      apiKey: "${CI_API_KEY:?set this in CI env}"  # custom error message
```

Empty (`VAR=""`) is treated as unset. To pass an empty value deliberately, use `${VAR:-}`.

---

## Migration from v3

Run `cosmoguard validate --config /path/to/cosmoguard.yaml` to check that a v3 config parses cleanly under v4.

Run `cosmoguard migrate-config --config /path/to/cosmoguard.yaml` to rewrite the file in v4 form. The original is preserved at `<path>.v3.bak`. Migration is purely cosmetic — v3 syntax keeps working forever.

**Behavioral changes worth a quick read:**

1. **WS cross-origin upgrades are denied by default.** Set `server.wsAllowedOrigins: ["*"]` for v3 behavior.
2. **CORS preflight responses come from cosmoguard.** If you depend on upstream's `Access-Control-Allow-Origin: *`, enable `cors:` explicitly.
3. **Cache hits now replay upstream's `Content-Type`** instead of forcing `application/json`. Endpoints that returned `text/plain` etc. are no longer mis-labeled on hits.
4. **JSON-RPC `path` Prometheus label removed.** Replaced with bounded `size_class` label on the batch histogram. Dashboards may need a refresh.
5. **gRPC reflection is no longer force-allowed.** Add an explicit rule if you need it.
6. **Every `rateLimit` block must set a finite positive `rate`.** Incomplete blocks previously passed config validation but failed when the runtime created the limiter; with the default `fail-open` mode, that silently left the matching rule unlimited. Add a `rate` (for example, `rate: 10/s`) or remove the block before upgrading.

For the full list of changes since v3, see `git log main..v4`.
