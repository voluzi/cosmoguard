# CosmoGuard

[![Test](https://github.com/voluzi/cosmoguard/actions/workflows/test.yml/badge.svg)](https://github.com/voluzi/cosmoguard/actions/workflows/test.yml)
[![GoReleaser](https://github.com/voluzi/cosmoguard/actions/workflows/goreleaser.yml/badge.svg)](https://github.com/voluzi/cosmoguard/actions/workflows/goreleaser.yml)
[![Docker Builds](https://github.com/voluzi/cosmoguard/actions/workflows/docker.yml/badge.svg)](https://github.com/voluzi/cosmoguard/actions/workflows/docker.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/voluzi/cosmoguard/blob/main/LICENSE.md)

CosmoGuard is a security-focused proxy + cache + auth gateway in front of
Cosmos-SDK nodes. It gates access at the API-endpoint level (not just by
port), caches deterministic responses, throttles abusive traffic, fans
out to multiple upstream nodes with active healthchecks, and authenticates
clients via API keys / JWT / external validators.

The current major version is **v4**. See the
[release notes](https://github.com/voluzi/cosmoguard/releases) for what
changed from v3; existing v3 configs continue to work — run
`cosmoguard migrate-config` to rewrite them in v4 form when ready.

## Highlights

- **Endpoint-level access control** across every Cosmos surface:
  Tendermint RPC (HTTP + JSON-RPC + WebSocket), Cosmos LCD/REST, gRPC
  (Query), and EVM RPC + WS.
- **Expressive rule schema** (v4): combine `all`/`any`/`none` predicates
  over method, path, query, headers, source IP/CIDR. Globs supported on
  every value.
- **Response caching** with per-rule namespacing, content-type
  preservation, configurable header allowlists. Backed by an embedded
  olric distributed cache with an in-process L1 — single binary,
  no external dependency, shared automatically across replicas when
  cluster mode is on.
- **Rate limiting** with `per-ip`, `global`, and (post-auth) `per-
  identity` scopes. Buckets are sharded across replicas through the
  same olric runtime in cluster mode, so configured rates stay correct
  under HPA without an external store.
- **Authentication**: api-key, JWT (HMAC + RSA/ECDSA/Ed25519), RFC 7662
  token introspection, and an external-validator method for
  developer-portal style credential checks. Credential headers are
  always stripped before forwarding upstream.
- **CORS** owned by cosmoguard, not the upstream — preflight handled
  directly; upstream's CORS headers are stripped and replaced.
- **Multi-upstream nodes** with active healthchecks, weighted round-
  robin, and per-upstream circuit breakers. `/readyz` reflects pool
  health.
- **Hardened defaults**: HTTP timeouts, request body caps, JSON-RPC
  batch size caps, WS origin allowlist, atomic-fail-safe config reload,
  no `glob.MustCompile` panics.
- **Production polish**: graceful shutdown on SIGTERM, `cosmoguard
  validate` for CI gates, `cosmoguard migrate-config` for v3→v4 rewrites,
  `/healthz` / `/readyz` / `/info` / `/metrics` for k8s probes and
  Prometheus.
- **Hot-reload** of config rules without dropping in-flight requests;
  WebSocket subscriptions the new rules deny are revoked.
- **Live observability dashboard**: a read-only UI with per-protocol
  traffic, cache hit rates, rule/identity views, recent denials,
  unmatched endpoints, a live request feed, and a WebSocket
  connection/subscription panel. In cluster mode it fans out across
  peers for a single cluster-wide view. OpenTelemetry tracing and
  Prometheus metrics round out the surface.

## Installation

### Install script (Linux / macOS)

The quickest way to grab the latest release binary. The trailing `!`
tells the installer (jpillora/installer) to move the binary into your
`PATH` rather than just dropping it in the current directory:

```bash
curl -s https://get.voluzi.com/cosmoguard! | bash
```

### Prerequisites
- Go 1.25+ (for building from source).

### Docker

```bash
docker run -it --name cosmoguard \
  -v /path/to/cosmoguard.yaml:/etc/cosmoguard/cosmoguard.yaml \
  ghcr.io/voluzi/cosmoguard \
  --config /etc/cosmoguard/cosmoguard.yaml
```

### Helm (k8s)

Each release publishes the chart as `oci://ghcr.io/voluzi/helm/cosmoguard`,
versioned with cosmoguard itself (chart X.Y.Z deploys image X.Y.Z):

```bash
kubectl create secret generic cosmoguard-cluster-key \
  --from-literal=encryptionKey="$(head -c32 /dev/urandom | base64)"

helm upgrade --install cosmoguard oci://ghcr.io/voluzi/helm/cosmoguard \
  --set config.nodes[0].host=cosmos-node.default.svc \
  --set cluster.existingEncryptionKeySecret=cosmoguard-cluster-key
```

Create the shared key Secret once; do not regenerate it on each deploy. The
Secret and Helm release must use the same namespace (both commands above use
`default`).

Installing from a checkout (`./helm/cosmoguard`) deploys the floating
`latest` image, because the chart's version fields are only stamped at
release. Pin the image for anything but local testing, e.g.
`--set image.tag=4.0.3` (no `v` prefix).

See `helm/cosmoguard/README.md` for cluster-mode + HPA setup.

### Build from source

```bash
git clone https://github.com/voluzi/cosmoguard.git
cd cosmoguard
make install
```

## Quick start

Minimal config — allow `/status` on Tendermint RPC and cache for 10s:

```yaml
nodes:
  - host: 127.0.0.1
    rpcPort: 26657
    lcdPort: 1317
    grpcPort: 9090

cache:
  ttl: 10s

rpc:
  rules:
    - action: allow
      match:
        path: /status
        methods: [GET]
      cache:
        enable: true

  jsonrpc:
    rules:
      - action: allow
        methods: [status]
        cache:
          enable: true
```

A fuller example with multi-upstream, auth, CORS, and rate limiting is
in [`example.config.yml`](./example.config.yml).

See [CONFIG.md](./CONFIG.md) for the complete reference.

## Validate + run

```sh
# Pre-deploy / CI check: parse + compile the config without binding ports.
cosmoguard validate --config /etc/cosmoguard/cosmoguard.yaml

# Rewrite a v3 config in v4 form (the original is backed up to .v3.bak).
cosmoguard migrate-config --config /etc/cosmoguard/cosmoguard.yaml

# Run cosmoguard.
cosmoguard --config /etc/cosmoguard/cosmoguard.yaml
```

## Compatibility

CosmoGuard is designed as a drop-in stand-in for a direct Cosmos node
connection. Any request that worked against a bare Tendermint/Cosmos/
EVM node returns a byte-identical response through cosmoguard when
allowed. There are six intentional v4 behavioral changes to review
before upgrading (default-deny on WS cross-origin, cosmoguard-owned
CORS, content-type fidelity on cache hits, Prometheus label cleanup,
no force-allowed gRPC reflection, and mandatory positive rates in
configured rate-limit blocks).

A compatibility test suite recordable via `scripts/record-golden.sh`
captures live-node responses and replays them through cosmoguard,
asserting byte-identical relay.

### Checking a live node

`cmd/cosmoguard-compat` calls every read endpoint it can find on a node,
both directly and through cosmoguard, and reports where the answers
differ. It finds the endpoints itself: gRPC query methods through server
reflection (only `*.Query` / `*.QueryService` services and the SDK's
known read-only services are called; others are listed as skipped), the LCD routes
annotated on them, every read-only CometBFT RPC
method (URI and JSON-RPC forms, plus a batch), a fixed set of read-only
EVM JSON-RPC methods, and the NewBlock / newHeads WebSocket subscriptions.
Queries that take a height are pinned to one, so their answers compare
byte for byte; answers about the latest state or the answering node
(status, latest block, node info, gas price and the like) are compared by
JSON shape only, and a WebSocket event is compared as a JSON value for the
same block. Each cosmoguard answer is fetched twice so cached answers are
checked too. The status, body and Content-Type are compared; other
response headers are not. A few `cross-height` probes also ask for the
same query at two heights, so a cache that ignores the requested height
is caught.

The node side must be a raw node, not one already behind cosmoguard; a
chain's public endpoints usually are, and comparing against them tests
cosmoguard against itself. By default the tool expects the node on
localhost at the standard ports, so with a Cosmopilot node forward them
first:

```sh
kubectl port-forward svc/<chainnode> 1317 26657 9090   # add 8545 8546 for an EVM chain
make compat
```

`make compat` builds cosmoguard, starts it in front of the node, compares
and stops it. A chain without EVM leaves ports 8545 and 8546 closed; its
EVM checks are reported as skipped. `--node-lcd`, `--node-rpc`,
`--node-grpc` (`http://` for plaintext, `https://` for TLS), `--node-evm`
and `--node-evm-ws` override each default, and an EVM URL given this way
must be reachable. To check a cosmoguard you already run:

```sh
go run ./cmd/cosmoguard-compat \
  --guard-lcd http://cosmoguard:11317 --guard-rpc http://cosmoguard:16657 \
  --guard-grpc http://cosmoguard:19090 --report compat.json
```

Each endpoint is reported as `identical`, `differs` (cosmoguard answered
differently), `denied` (cosmoguard refused a request the node answered),
`failed` (the node itself did not answer), `unstable` or `skipped` (a path
parameter has no live value; `--param name=value` supplies chain-specific
ones). An endpoint is `unstable` when the node disagrees with itself
between two calls, or answers at a different height despite the pin. A
node behind a load balancer can do both, so a differing endpoint is
compared again, up to three rounds, and only reported as `differs` when no
round matched. Rounds and height retries are `--round-delay` apart (3s
with `make compat`, whose cosmoguard caches for 2s), so each reaches the
node rather than cosmoguard's cache; set it above your cache TTL when
checking a running deployment.

The run exits 1 when any endpoint differs, when nothing could be compared,
or, with `make compat` (whose config allows everything), when any request
was denied. With `make compat` the cosmoguard log is kept after a failing
run and its path is printed.

The tool needs network access and is not part of `make test`.

## License

Unless a file notes otherwise, it falls under the
[MIT License](./LICENSE.md).
