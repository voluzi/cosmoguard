# cosmoguard Helm chart

Deploys [cosmoguard](https://github.com/voluzi/cosmoguard) — a security
firewall, cache, and auth gateway in front of Cosmos-SDK nodes.

## TL;DR

```sh
helm upgrade --install cosmoguard oci://ghcr.io/voluzi/helm/cosmoguard \
  --set config.nodes[0].host=cosmos-node.default.svc
```

## Values

See `values.yaml` for the full reference. Highlights:

- `kind:` — `StatefulSet` (default) or `Deployment`. `StatefulSet` is the
  default because cluster mode is on by default and benefits from stable pod
  identities: stable pod DNS for gossip, and per-pod observability snapshots
  (dashboard panels keyed by `pod_id`) that survive rolling restarts.
  `Deployment` keeps cache + rate-limit data through olric replication too,
  but loses per-pod observability when pods are replaced. (Gentle,
  olric-friendly scale-down comes from `autoscaling.behavior`, not the
  StatefulSet, which uses `podManagementPolicy: Parallel`.)
- `config:` — the cosmoguard YAML, rendered into a ConfigMap mounted at
  `/etc/cosmoguard/cosmoguard.yaml`. See [CONFIG.md] for the schema.
- `existingConfigMap:` / `existingSecret:` — bring your own ConfigMap and
  / or env-var Secret (e.g. ArgoCD-managed config, External Secrets).
  When set, `config:` is ignored / Secret entries are loaded into the
  container env for `${VAR}` interpolation in the cosmoguard YAML.
- `autoscaling.enabled: true` — turns on a HorizontalPodAutoscaler.
  Scale safely when `config.cache.cluster.enable: true` (the chart
  default); without it, rate-limit budgets are per-replica and HPA
  multiplies them.
- `serviceMonitor.enabled: true` — when prometheus-operator is in the
  cluster, scrapes `/metrics` on the cosmoguard service.
- `podDisruptionBudget.enabled: true` — caps voluntary disruptions so
  the cluster keeps quorum during node drains.

## Cluster mode

Cosmoguard ships with an always-embedded olric runtime, and the chart
defaults `config.cache.cluster.enable: true` with `discovery.mode: dns`
and a rendered headless peer Service — so single-replica installs
already exercise the full cluster code path and scaling to N is a
one-line `replicaCount` change.

What the chart wires up automatically when cluster mode is on:

- injects `POD_IP` via the downward API and defaults `bindAddr` to
  `${POD_IP}` (cosmoguard rejects wildcard `bindAddr` in cluster mode
  because every pod would otherwise share the same memberlist Name);
- opens `bindPort` / `gossipPort` (TCP **and** UDP) / `peerApiPort` on
  both the workload and a separate headless peer Service;
- auto-fills `cache.cluster.discovery.dns.host` to the headless
  service's in-cluster FQDN (override by setting an explicit host).

Recommended HA shape for production:

```yaml
kind: StatefulSet
replicaCount: 3
config:
  cache:
    cluster:
      replicaCount: 2   # olric replication factor
      quorum: 2

podDisruptionBudget:
  enabled: true
  minAvailable: 2
```

To opt out of cluster mode entirely (back to v3-style single-binary
behaviour with no peers, no extra ports):

```yaml
config:
  cache:
    cluster:
      enable: false
```

> memberlist gossip needs **both TCP and UDP** on the gossip port. Many
> default NetworkPolicies and firewalls block UDP by reflex; the cluster
> will silently fail to converge if you do.

## External exposure

Two mutually-exclusive routing modes are supported. Disabling all the
options below means the cosmoguard Service is reachable only in-cluster
(use `kubectl port-forward` for local access).

### Ingress (networking.k8s.io/v1)

Three separate Ingress objects, by design:

- `ingress` — LCD / RPC / EVM (HTTP/1.1, WebSocket upgrades transparent).
  Per-endpoint hostnames so each protocol can live on its own subdomain.
- `ingressGrpc` — gRPC. Separate because nginx-ingress (and most
  others) can't mix gRPC and HTTP/1.1 on the same Ingress object; the
  default annotations set `nginx.ingress.kubernetes.io/backend-protocol:
  GRPC`.
- `ingressDashboard` — the operator-only dashboard. Separate so it can
  carry auth annotations (`auth-type: basic`, IP allowlists) that you
  don't want on the public proxy. Refuses to render when
  `config.dashboard.enable: false`.

### Gateway API (gateway.networking.k8s.io/v1)

Mutually exclusive with the three `ingress*` blocks. HTTPRoute for
LCD/RPC/EVM/dashboard, GRPCRoute for gRPC. `gateway.parentRefs` applies
to every endpoint unless an endpoint sets its own.

## Health probes

Liveness uses `/healthz`; readiness uses `/readyz`. Both live on the
metrics port (default 9001). `/readyz` returns 503 when zero upstreams
are healthy across the LCD + RPC pools — k8s will then stop sending
traffic to that pod.

## Customizing the cosmoguard config

The simplest path is to inline your full config under `values.yaml`'s
`config:` key. The chart renders it verbatim into a ConfigMap.

For complex configs with many env-var-interpolated secrets, use
`existingConfigMap:` + `existingSecret:` so secrets live in your Secret
manager (External Secrets / Sealed Secrets / Vault sidecar) rather than
Helm values.

[CONFIG.md]: ../../CONFIG.md
