# cosmoguard Helm chart

Deploys [cosmoguard](https://github.com/voluzi/cosmoguard) — a security
firewall, cache, and auth gateway in front of Cosmos-SDK nodes.

## TL;DR

```sh
kubectl create secret generic cosmoguard-cluster-key \
  --from-literal=encryptionKey="$(head -c32 /dev/urandom | base64)"

helm upgrade --install cosmoguard oci://ghcr.io/voluzi/helm/cosmoguard \
  --set config.nodes[0].host=cosmos-node.default.svc \
  --set cluster.existingEncryptionKeySecret=cosmoguard-cluster-key
```

Create the Secret once and keep it for the lifetime of the cluster. The Secret
must be in the same namespace as the Helm release; both commands above use the
`default` namespace.

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
  `existingConfigMap` replaces the rendered file, while Secret entries are
  loaded into the container env for `${VAR}` interpolation. The chart still
  reads `config.cache.cluster` to render peer ports, discovery resources, and
  cluster enablement, so keep those values consistent with the external file.
- `autoscaling.enabled: true` — turns on a HorizontalPodAutoscaler.
  Scale safely when `config.cache.cluster.enable: true` (the chart
  default); without it, rate-limit budgets are per-replica and HPA
  multiplies them.
- `serviceMonitor.enabled: true` — when prometheus-operator is in the
  cluster, scrapes `/metrics` on the `<fullname>-internal` Service.
- `podDisruptionBudget.enabled: true` — caps voluntary disruptions so
  the cluster keeps quorum during node drains. Set at most one of
  `minAvailable` / `maxUnavailable`; with neither the PDB uses
  `minAvailable: 1`.

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

The shared cluster encryption key also derives the HMAC key used on the
HTTP peer API. Signatures authenticate bodyless dashboard fan-out GETs
within a 30-second clock-skew/replay window; they do not encrypt payloads,
so enable `networkPolicy.enabled: true` or provide an equivalent enforcing
policy as the confidentiality boundary. There is no unsigned fallback, and
mixed-version rolling upgrades can show partial cluster-dashboard data until
all replicas run the signing version.

### Stable cluster key

Cluster mode fails closed unless a usable key source is declared. The
recommended path for Helm templates, dry runs, Argo CD, and other client-side
or GitOps rendering is a pre-created Secret:

```sh
kubectl create secret generic cosmoguard-cluster-key \
  --namespace cosmoguard \
  --from-literal=encryptionKey="$(head -c32 /dev/urandom | base64)"

helm upgrade --install cosmoguard oci://ghcr.io/voluzi/helm/cosmoguard \
  --namespace cosmoguard \
  --set cluster.existingEncryptionKeySecret=cosmoguard-cluster-key
```

Provision this Secret once rather than regenerating it during deployment. Its
`encryptionKey` field contains a base64-encoded 16, 24, or 32-byte key, and it
must be in the release namespace. The chart declares the reference but does
not inspect an externally managed Secret during rendering.

For a server-side Helm install, `cluster.generateEncryptionKey=true` opts into
the chart-managed `<fullname>-cluster-key` Secret. The chart uses `lookup` to
reuse that Secret and marks it `helm.sh/resource-policy: keep`. Do not use this
option with `helm template`, client-side dry runs, or Argo CD: those renderers
cannot look up the existing Secret and produce a new random key each time.
Flux helm-controller performs actual Helm installs, so server-side lookup is
available there.

Inline `cluster.encryptionKey` and
`config.cache.cluster.encryptionKey` remain supported when the chart renders
the ConfigMap, but the value is then visible in that ConfigMap.

For either a chart-rendered or external ConfigMap, `existingSecret` can supply
the interpolation environment and must contain a `CLUSTER_ENCRYPTION_KEY`
field. `env.CLUSTER_ENCRYPTION_KEY` is also accepted, but exposes the key in
Helm values and the rendered workload manifest. Both environment-backed
sources take precedence over `cluster.generateEncryptionKey=true`; an explicit
`env.CLUSTER_ENCRYPTION_KEY` also takes precedence over
`cluster.existingEncryptionKeySecret` so the workload never receives duplicate
environment entries.

### External ConfigMap key wiring

When `existingConfigMap` is set, its `cosmoguard.yaml` must consume the standard
environment variable:

```yaml
cache:
  cluster:
    encryptionKey: "${CLUSTER_ENCRYPTION_KEY}"
```

Declare one of these sources so the workload receives that variable:

- `cluster.existingEncryptionKeySecret`: the Secret must have an
  `encryptionKey` field; the chart creates the explicit environment reference.
- `existingSecret`: the Secret must have a `CLUSTER_ENCRYPTION_KEY` field and
  is loaded through `envFrom`.
- `env.CLUSTER_ENCRYPTION_KEY`: accepted for explicit wiring, but exposes the
  key in Helm values and rendered workload manifests.

The chart validates this declared wiring, not the contents of external
ConfigMaps or Secrets. Inline key values and `cluster.generateEncryptionKey`
only affect chart-rendered configuration and cannot supply an external
ConfigMap.

### Upgrading existing releases

An existing release that used the generated `<fullname>-cluster-key` Secret
must keep those key bytes. Set `cluster.existingEncryptionKeySecret` to that
same Secret name for deterministic rendering, or explicitly retain
`cluster.generateEncryptionKey=true` for server-side Helm operation. Do not
create a replacement key for a running cluster.

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

The metrics and dashboard listeners are never published on the main
Service. They live on a separate ClusterIP Service, `<fullname>-internal`,
so `service.type: LoadBalancer` exposes only the proxy ports. With
`networkPolicy.enabled`, the metrics port stays open even when
`networkPolicy.proxyIngress` restricts the proxy listeners; narrow its
sources with `networkPolicy.metricsFrom`.

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

## Upgrading to 2.0.0

Chart 2.0.0 changes these defaults; the pods restart on upgrade.

- Metrics and dashboard ports move from `<fullname>` to the ClusterIP
  Service `<fullname>-internal`. The bundled ServiceMonitor, dashboard
  Ingress and dashboard HTTPRoute follow automatically; repoint any scrape
  job, runbook or route that used `<fullname>:9001` or `:19999`.
- Pods no longer mount a ServiceAccount token. Set
  `serviceAccount.automountToken: true` if a sidecar (e.g. a Vault agent)
  needs it.
- Pods run with `seccompProfile: RuntimeDefault`.
- With `networkPolicy.enabled` and `networkPolicy.proxyIngress` set, the
  metrics port is now allowed from any source unless
  `networkPolicy.metricsFrom` narrows it.
- `podDisruptionBudget.minAvailable` is no longer set in `values.yaml`, so
  `maxUnavailable` can be set on its own. With neither set the PDB still
  uses `minAvailable: 1`.

## Customizing the cosmoguard config

The simplest path is to inline your full config under `values.yaml`'s
`config:` key. The chart renders it verbatim into a ConfigMap.

For complex configs with many env-var-interpolated secrets, use
`existingConfigMap:` + `existingSecret:` so secrets live in your Secret
manager (External Secrets / Sealed Secrets / Vault sidecar) rather than
Helm values.

[CONFIG.md]: ../../CONFIG.md
