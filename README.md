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
`cosmoguard --migrate-config` to rewrite them in v4 form when ready.

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
  --validate` for CI gates, `--migrate-config` for v3→v4 rewrites,
  `/healthz` / `/readyz` / `/info` / `/metrics` for k8s probes and
  Prometheus.
- **Hot-reload** of config rules without dropping in-flight requests.
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

The repo ships a Helm chart at `helm/cosmoguard/`:

```bash
helm upgrade --install cosmoguard ./helm/cosmoguard \
  --set config.nodes[0].host=cosmos-node.default.svc
```

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
cosmoguard --config /etc/cosmoguard/cosmoguard.yaml --validate

# Rewrite a v3 config in v4 form (the original is backed up to .v3.bak).
cosmoguard --config /etc/cosmoguard/cosmoguard.yaml --migrate-config

# Run cosmoguard.
cosmoguard --config /etc/cosmoguard/cosmoguard.yaml
```

## Compatibility

CosmoGuard is designed as a drop-in stand-in for a direct Cosmos node
connection. Any request that worked against a bare Tendermint/Cosmos/
EVM node returns a byte-identical response through cosmoguard when
allowed. There are five intentional v4 behavioral changes to review
before upgrading (default-deny on WS cross-origin, cosmoguard-owned
CORS, content-type fidelity on cache hits, Prometheus label cleanup,
no force-allowed gRPC reflection).

A compatibility test suite recordable via `scripts/record-golden.sh`
captures live-node responses and replays them through cosmoguard,
asserting byte-identical relay.

## License

Unless a file notes otherwise, it falls under the
[MIT License](./LICENSE.md).
