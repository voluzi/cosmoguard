# Configuration

This is the cosmoguard v4 configuration reference. Every setting is shown with its YAML key, default, and a short description of behavior. v3-shaped configs continue to work — run `cosmoguard --migrate-config` to rewrite them in v4 form.

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

---

## Server hardening

The `server:` block tunes HTTP/WS server timeouts and body caps. Defaults are conservatively safe for any Cosmos node.

```yaml
server:
  readHeaderTimeout: 10s    # cap on time to read HTTP headers (slowloris defense)
  readTimeout: 30s          # cap on total request-read time
  writeTimeout: 0            # cap on response write time; 0 = no limit (default)
  idleTimeout: 60s          # keep-alive idle timeout
  maxRequestBody: 5242880   # bytes; requests exceeding this return 413 (0 = no limit)
  wsReadLimit: 1048576      # max bytes per inbound WebSocket frame (0 = no limit)
  wsAllowedOrigins:         # cross-origin WS upgrade allowlist
    - https://app.example.com
    - https://*.preview.example.com
```

**Breaking from v3:** `wsAllowedOrigins` defaults to empty — cross-origin WS upgrades are denied unless explicitly allowed. To restore v3 behavior, set `wsAllowedOrigins: ["*"]`.

**Default changes since v4.0.0-rc.1** (all restore v3-compatible behaviour that the rc.1 defaults broke):
- `writeTimeout` now defaults to **0 (no limit)**. A fixed deadline truncated large/slow streamed responses (`/block_results`, `/genesis`, big `eth_getLogs`) mid-body. Set an explicit ceiling if exposing cosmoguard to untrusted clients.
- `maxRequestBody` default raised from 1 MiB to **5 MiB** so large payloads (e.g. a wasm `MsgStoreCode` broadcast) aren't rejected with 413.
- `wsReadLimit` default raised from 64 KiB to **1 MiB**, and an explicit `0` now means "no limit" (as documented) instead of being silently forced to 64 KiB. Large frames (e.g. a big `eth_sendRawTransaction`) are no longer dropped.

**Hot-reload:** `server:` timeouts / body caps, `cors:`, and dashboard `enable`/`port`/`basicAuth` are captured at startup and now **reject** a reload that changes them (with a clear "requires a process restart" message) instead of silently accepting a change that never takes effect. `dashboard.requestLog` and `server.trustedProxies` still hot-reload.

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
    weight: 1                       # reserved for weighted-RR; ignored today
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
  # memory: …                               # cache memory budget (see below)
  # cluster: …                              # presence enables networked cluster mode
```

### Memory budget

The cache is bounded so a high-cardinality query load (e.g. `/tx`, `/block_search`, `/tx_search` — one entry per distinct hash / query) can't grow the working set until the pod is OOMKilled. Both cache tiers that share the pod heap are capped: the in-process **L1** (LRU by approximate payload bytes) and the olric **L2** (per-node LRU). The rate-limiter, JWT replay set, and observability DMaps are **never** evicted.

By default the budget is **derived automatically from the pod's memory limit** (read from the cgroup, v1/v2), so it scales with the pod without any configuration:

```
reserve = max(128 MiB, 0.20 × limit)   # runtime + buffers + cache overhead
budget  = limit − reserve              # total cache footprint
L1 = 40% of budget,  L2 = 60% of budget   (L2 also holds replicas)
```

The total is then split evenly across the response caches that run in the pod (one per proxy). The **caps are the primary OOM defence**. In parallel, `GOMEMLIMIT` is set to **90% of the limit** and `GOMAXPROCS` to the CPU quota — a secondary GC backstop for transient/non-cache allocations (it can only reclaim freeable memory, not the live cache set). When no cgroup limit is detectable (bare metal), each tier falls back to a fixed **128 MiB**.

**The L2 (olric) cap is approximate by construction** — unlike the exact L1 byte bound. olric evicts by *sampled* LRU and cannot hold a partition below one entry, so each cache DMap has a floor of roughly `ownedPartitions × maxEntrySize`, and the aggregate L2 floor is `N_dmaps × ownedPartitions × 256 KiB`. The per-node cap is enforced by two complementary bounds — `MaxInuse` (bytes) and `MaxKeys` (count, derived from the byte budget) — so a flood of tiny entries can't exhaust heap in per-key index structures before the byte counter trips.

- **Embedded/single-pod** mode uses a reduced partition count so the floor fits the L2 budget of the smallest supported pod (~128 MiB with all proxies enabled).
- **Clustered** mode keeps olric's default partition count (271) because the count is fixed at cluster formation and every peer must agree on it, and a lower count would hurt key distribution as the cluster scales. The trade-off: a *small* cluster (e.g. 2 nodes, `replicaCount: 2`) has a large per-node floor (each node owns nearly every partition as primary or replica), so **a small cluster on small pods can exceed the resolved L2 budget.** Size clustered pods with headroom above `N_dmaps × ownedPartitions × 256 KiB`, or run more/larger nodes.

Size the pod so its L2 budget clears the floor, or raise the memory limit. Entries larger than the 256 KiB fragment `tableSize` are served uncached rather than stored. Clustered deployments also divide the per-node cap by `replicaCount`, since each node holds replica copies that olric's primary-write cap doesn't govern. The L1 byte cap and `GOMEMLIMIT` are unaffected by any of this.

Override any of it explicitly:

```yaml
cache:
  memory:
    maxBytes: 134217728                 # absolute L1 cap (bytes). unset → auto; 0 → no limit
    maxItems: 0                         # optional L1 entry-count guard. 0 → no limit
    distributedMaxBytesPerNode: 0       # absolute L2 (olric) per-node cap. unset → auto; 0 → no limit
    reserveFraction: 0.20               # auto-mode reserve fraction; must be in [0, 0.9)
```

> **Upgrade note (v4.0.0):** deployments were previously unbounded. After upgrading, entries beyond the budget are LRU-evicted (more upstream traffic under extreme cardinality, never incorrect results), and `GOMEMLIMIT`/`GOMAXPROCS` are set from the container limits. Set `maxBytes: 0` and `distributedMaxBytesPerNode: 0` to restore the old unbounded behavior. Watch `cosmoguard_cache_evictions_total` (which counts budget evictions only, not TTL expiry) — a rising rate means the cap is undersized for your workload.

### Migration from v3

`cache.backend`, `cache.redis`, and `cache.redis-sentinel` were removed in v4. The embedded olric runtime + L1 cache covers every workload — see `bench/RESULTS.md` for the comparison numbers. A config that still contains `cache.redis` or `cache.redis-sentinel` now **fails startup** with a migration error (rather than silently ignoring the field and running an isolated per-pod cache). Remove those keys and, for multi-replica deployments, add a `cluster:` block.

### Cluster mode

Including a `cache.cluster` block in the config turns the embedded olric daemon into a real cluster. Replicas form a memberlist gossip ring and partition the cache + rate-limiter keyspace. Single binary, single daemon, no external dependencies.

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

**`encryptionKey` is required in cluster mode.** It enables memberlist gossip encryption + peer authentication (AES-128/192/256-GCM) AND password authentication on olric's RESP data port, so both the gossip plane and the data plane require the shared secret. Without it, any host that can reach the peer ports could join the cluster and read/write the shared DMaps (rate-limit buckets, cache, JWT replay set). Generate one with `head -c32 /dev/urandom | base64` and give **every pod the same value** (from a Kubernetes Secret). Still restrict the peer ports at the network layer (NetworkPolicy) — the key is defence-in-depth, not a substitute.

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
- **Three ports per pod, not two** — `bindPort` (3320, TCP), `gossipPort` (3322, TCP + UDP) and `peerApiPort` (defaults to `bindPort + 1` → 3321, TCP) all need to be reachable pod-to-pod. The peer-API listener is what the dashboard fan-out aggregator calls on its siblings; default-deny `NetworkPolicy` setups must allow it explicitly. Restricted by an IP allowlist built from the current memberlist roster, so it is never internet-facing even when exposed to the pod network.
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

**Requires cluster mode.** Restart-restore reads a peer's replica, so the flag only has an effect when a `cache.cluster` block is present. In a single-pod / embedded deployment there are no peers to restore from, so the flag is ignored (it would otherwise reintroduce the memory growth above with nothing to restore); cosmoguard logs a warning if it is set without cluster mode. Changing the flag on a running pod is rejected on hot-reload as **requires a process restart** — it is wired once at startup.

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

Per-identity rate limits are expressed at the **rule** level via `rateLimit: { scope: per-identity }` (see the Rate limiting section). That covers the most common "give this api key its own quota" pattern without growing a separate enforcement surface — and the rule layer is the one the proxy already runs on every request.

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
  webSocketConnections: 10           # total WS conns across all backends; spread evenly
  rules: [ ... ]                     # HTTP rules
  jsonrpc:
    default: deny
    maxBatchSize: 100                # cap on JSON-RPC batch size; 413 on excess
    rules: [ ... ]                   # JSON-RPC rules

grpc:
  default: deny
  rules: [ ... ]                     # gRPC rules

evm:                                 # only when enableEvm: true
  rpc:
    default: deny
    rules: [ ... ]                   # JSON-RPC rules
    httpRules: [ ... ]               # HTTP rules
  ws:
    default: deny
    webSocketConnections: 10
    rules: [ ... ]                   # JSON-RPC rules
```

---

## Rules

All rules share `priority`, `action`, and an optional `match:` block. Lower priority numbers match first; first-match-wins.

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
    - query.height: present          # presence-check (key must exist)
  any:                               # OR: at least one child must match
    - method: GET
    - method: HEAD
  none:                              # NOT: no child may match
    - header.x-debug: present
  # Leaf atoms at this level are an implicit `all`:
  sourceIP: 10.0.0.0/8                # CIDR or single IP
  header.authorization: "Bearer *"   # glob match on header value
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
  preserveHeaders:                   # additional headers to replay on hit
    - Content-Encoding               # (Content-Type / Cache-Control / ETag / Vary
    - X-Custom-Header                #  are always preserved)
```

Cache keys are namespaced per rule fingerprint — two rules matching the same request never share entries.

#### Rate limit

```yaml
rateLimit:
  rate: 100/s                        # or "100", "30/min", "1/5s", "250/250ms"
  burst: 200                         # max bucket capacity; defaults to rate
  scope: per-ip                      # per-ip (default) | global | per-identity | compound
  failureMode: fail-open             # fail-open (default) | fail-closed
```

With a `cache.cluster` block present, rate-limit buckets are sharded across replicas through olric so the configured rate is a true cluster-wide budget. In single-pod / embedded olric mode the rate is enforced per pod.

`failureMode` controls behaviour when the limiter backend errors (e.g. olric loses quorum). `fail-open` (default) admits the request so a coordination hiccup doesn't 429 all traffic; `fail-closed` denies it so a backend outage can't silently disable rate limiting cluster-wide. Applies uniformly across HTTP, JSON-RPC, WebSocket, and gRPC.

#### Examples

```yaml
rules:
  # Height-pinned reads are immutable — cache aggressively.
  - priority: 100
    action: allow
    match:
      all:
        - paths: [/block, /commit, /block_results]
        - query.height: "[0-9]*"
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
        - header.origin: present
```

### JSON-RPC rule

| Field | Default | Description |
|---|---|---|
| `priority` | `1000` | |
| `action` | — | `allow` or `deny`. |
| `methods` | empty | JSON-RPC method names (globs supported). Empty = all. |
| `params` | empty | Dict (or array index) of param name → glob. |
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

### gRPC rule

| Field | Default | Description |
|---|---|---|
| `priority` | `1000` | |
| `action` | — | `allow` or `deny`. |
| `methods` | empty | Fully-qualified gRPC methods (globs supported). |
| `cache.enable` | `false` | Cache unary responses keyed by (rule fingerprint, method, payload). |
| `cache.ttl` | `5s` | Per-rule TTL. |
| `cache.keyMode` | `raw` | `raw` hashes payload bytes verbatim; `method-only` excludes payload (parameter-less queries only); `canonical` decodes payload against operator-supplied protoset descriptors and re-encodes deterministically before hashing — cache hits across clients with different serialization (field order, default-vs-absent). |

For `keyMode: canonical`, set `grpc.protosets:` at the top level. Each path is a binary `FileDescriptorSet` produced by `protoc --descriptor_set_out=foo.protoset -I path/to/protos path/to/protos/**/*.proto`. Methods absent from the loaded protosets silently degrade to `raw`.

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

**`keyMetadata` (gRPC).** Response-affecting request metadata is folded into the cache key so requests that differ only by such metadata cache separately. It defaults to `["x-cosmos-block-height"]` — the metadata that selects the state height a Cosmos query runs against — so a query at one height never serves another height's cached response. Set an explicit empty list (`keyMetadata: []`) to opt out on a genuinely height-independent method:

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

Run `cosmoguard --validate --config /path/to/cosmoguard.yaml` to check that a v3 config parses cleanly under v4.

Run `cosmoguard --migrate-config --config /path/to/cosmoguard.yaml` to rewrite the file in v4 form. The original is preserved at `<path>.v3.bak`. Migration is purely cosmetic — v3 syntax keeps working forever.

**Behavioral changes worth a quick read:**

1. **WS cross-origin upgrades are denied by default.** Set `server.wsAllowedOrigins: ["*"]` for v3 behavior.
2. **CORS preflight responses come from cosmoguard.** If you depend on upstream's `Access-Control-Allow-Origin: *`, enable `cors:` explicitly.
3. **Cache hits now replay upstream's `Content-Type`** instead of forcing `application/json`. Endpoints that returned `text/plain` etc. are no longer mis-labeled on hits.
4. **JSON-RPC `path` Prometheus label removed.** Replaced with bounded `size_class` label on the batch histogram. Dashboards may need a refresh.
5. **gRPC reflection is no longer force-allowed.** Add an explicit rule if you need it.

For the full list of changes since v3, see `git log main..v4`.
