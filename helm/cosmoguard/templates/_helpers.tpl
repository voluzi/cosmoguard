{{/*
Expand the name of the chart.
*/}}
{{- define "cosmoguard.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cosmoguard.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "cosmoguard.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "cosmoguard.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "cosmoguard.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cosmoguard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name
*/}}
{{- define "cosmoguard.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cosmoguard.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
ConfigMap name — either operator-supplied or templated.
*/}}
{{- define "cosmoguard.configMapName" -}}
{{- if .Values.existingConfigMap -}}
{{- .Values.existingConfigMap -}}
{{- else -}}
{{- include "cosmoguard.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Image with tag fallback to .Chart.AppVersion.
*/}}
{{- define "cosmoguard.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{ printf "%s:%s" .Values.image.repository $tag }}
{{- end -}}

{{/*
exposureValidation refuses to install the chart when both routing
modes (Ingress + Gateway API) are enabled at once. Each external-
exposure template includes this so the failure fires no matter which
manifest triggers rendering first.
*/}}
{{- define "cosmoguard.exposureValidation" -}}
{{- if and .Values.ingress.enabled .Values.gateway.enabled -}}
{{- fail "cosmoguard: ingress.enabled and gateway.enabled are mutually exclusive — pick one routing mode" -}}
{{- end -}}
{{- if and .Values.ingressGrpc.enabled .Values.gateway.enabled -}}
{{- fail "cosmoguard: ingressGrpc.enabled and gateway.enabled are mutually exclusive — use gateway.grpc for Gateway API" -}}
{{- end -}}
{{- if and .Values.ingressDashboard.enabled .Values.gateway.enabled -}}
{{- fail "cosmoguard: ingressDashboard.enabled and gateway.enabled are mutually exclusive — use gateway.dashboard for Gateway API" -}}
{{- end -}}
{{- end -}}

{{/*
clusterConfig returns the (possibly empty) cluster config dict found at
.Values.config.cache.cluster. The cosmoguard YAML schema nests cluster
under `cache.cluster` (the embedded olric daemon is owned by the cache
subsystem); this helper centralises the path lookup so the rest of the
templates don't repeat dig() chains.
*/}}
{{- define "cosmoguard.clusterConfig" -}}
{{- $cache := dig "cache" (dict) .Values.config -}}
{{- toYaml (dig "cluster" (dict) $cache) -}}
{{- end -}}

{{/*
clusterEnabled is the single source of truth for "is cluster mode on?".
Returns the string "true" when config.cache.cluster.enable is truthy,
otherwise empty.
*/}}
{{- define "cosmoguard.clusterEnabled" -}}
{{- $cache := dig "cache" (dict) .Values.config -}}
{{- $c := dig "cluster" (dict) $cache -}}
{{- if dig "enable" false $c -}}true{{- end -}}
{{- end -}}

{{/*
peerServiceName — name of the headless service for memberlist gossip.
Defaults to "<fullName>-peers". Operators can override via
peerService.nameOverride for existing NetworkPolicy selectors.
*/}}
{{- define "cosmoguard.peerServiceName" -}}
{{- if .Values.peerService.nameOverride -}}
{{- .Values.peerService.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-peers" (include "cosmoguard.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
clusterValidation refuses to install the chart with internally inconsistent
cluster-mode wiring. Two-replica clusters need quorum=1 (we don't enforce
that here because the binary's own validation is clearer); the chart-side
checks below catch misconfigurations the binary CAN'T see (workload kind
mismatch, peerService disabled without cluster.enable=false).
*/}}
{{- define "cosmoguard.clusterValidation" -}}
{{- $kind := default "Deployment" .Values.kind -}}
{{- if not (or (eq $kind "Deployment") (eq $kind "StatefulSet")) -}}
{{- fail (printf "cosmoguard: kind must be Deployment or StatefulSet (got %q)" $kind) -}}
{{- end -}}
{{- if eq (include "cosmoguard.clusterEnabled" .) "true" -}}
{{- $cache := dig "cache" (dict) .Values.config -}}
{{- $c := dig "cluster" (dict) $cache -}}
{{- $mode := dig "discovery" "mode" "" $c -}}
{{- if eq $mode "" -}}
{{- fail "cosmoguard: config.cache.cluster.enable=true requires config.cache.cluster.discovery.mode (use \"dns\" with the chart's headless peer service in Kubernetes)" -}}
{{- end -}}
{{- $cluster := .Values.cluster | default (dict) -}}
{{/* Skip the key check when the config is an externally-managed ConfigMap —
     the chart doesn't render cosmoguard.yaml then, so it can't see the key
     (the operator supplies it in their own ConfigMap/Secret wiring). */}}
{{- if and (not .Values.existingConfigMap) (not (include "cosmoguard.clusterInlineKey" .)) (not $cluster.existingEncryptionKeySecret) (ne (include "cosmoguard.shouldGenerateKey" .) "true") -}}
{{- fail "cosmoguard: cluster mode requires an encryption key — set cluster.existingEncryptionKeySecret (recommended: a pre-created Secret with an `encryptionKey` field), set cluster.encryptionKey, or enable cluster.generateEncryptionKey (the chart mints a Secret once and reuses it via lookup). generateEncryptionKey is unsafe under client-side / GitOps rendering, which cannot lookup the existing Secret and would silently partition the cluster across syncs — supply existingEncryptionKeySecret there. Generate one manually with: kubectl create secret generic cosmoguard-cluster --from-literal=encryptionKey=$(head -c32 /dev/urandom | base64)" -}}
{{- end -}}
{{- if eq $mode "static" -}}
{{- $peers := dig "discovery" "static" "peers" (list) $c -}}
{{- if eq (len $peers) 0 -}}
{{- fail "cosmoguard: config.cache.cluster.discovery.mode=static requires config.cache.cluster.discovery.static.peers to be non-empty" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
discoveryHost returns the effective DNS discovery host. When cluster mode
is on with discovery.mode=dns AND discovery.dns.host is empty, defaults
to the in-cluster FQDN of the headless peer service. The configmap uses
this so operators don't have to hand-type the service name into the
config block.
*/}}
{{- define "cosmoguard.discoveryHost" -}}
{{- $cache := dig "cache" (dict) .Values.config -}}
{{- $cluster := dig "cluster" (dict) $cache -}}
{{- $host := dig "discovery" "dns" "host" "" $cluster -}}
{{- if $host -}}
{{- $host -}}
{{- else -}}
{{- printf "%s.%s.svc.cluster.local" (include "cosmoguard.peerServiceName" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
podIdentityEnv emits the env entries injected on every workload pod:
  - POD_IP via downward API when cluster mode is on. cosmoguard's
    config supports ${VAR} interpolation; defaulting bindAddr to
    "${POD_IP}" in values.yaml leans on this so the chart Just Works
    without operator-supplied wiring.
*/}}
{{- define "cosmoguard.podIdentityEnv" -}}
{{- if eq (include "cosmoguard.clusterEnabled" .) "true" }}
- name: POD_IP
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- $keySecret := include "cosmoguard.clusterKeySecretName" . -}}
{{- if $keySecret }}
- name: CLUSTER_ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $keySecret }}
      key: encryptionKey
{{- end }}
{{- end }}
{{- end -}}

{{/*
clusterInlineKey — the encryption key the operator supplied INLINE, from
either config.cache.cluster.encryptionKey or the top-level cluster.encryptionKey.
Empty when neither is set (chart generates a Secret / uses an existing one).
When non-empty the key is rendered directly into the config and NO env
reference / generated Secret is used.
*/}}
{{- define "cosmoguard.clusterInlineKey" -}}
{{- $fromConfig := dig "cache" "cluster" "encryptionKey" "" .Values.config -}}
{{- $fromTop := (.Values.cluster | default dict).encryptionKey | default "" -}}
{{- $fromConfig | default $fromTop -}}
{{- end -}}

{{/*
shouldGenerateKey — "true" when the chart must mint a cluster encryption key
Secret itself: cluster mode is on, the config is chart-rendered (not an
externally-managed ConfigMap), the operator supplied neither an inline key
nor an existing Secret, and cluster.generateEncryptionKey is enabled.
*/}}
{{- define "cosmoguard.shouldGenerateKey" -}}
{{- $cluster := .Values.cluster | default (dict) -}}
{{- if and (eq (include "cosmoguard.clusterEnabled" .) "true") (not .Values.existingConfigMap) (not (include "cosmoguard.clusterInlineKey" .)) (not $cluster.existingEncryptionKeySecret) (dig "generateEncryptionKey" false $cluster) -}}
true
{{- end -}}
{{- end -}}

{{/*
generatedKeySecretName — name of the Secret the chart creates for a
self-generated cluster encryption key.
*/}}
{{- define "cosmoguard.generatedKeySecretName" -}}
{{- printf "%s-cluster-key" (include "cosmoguard.fullname" .) -}}
{{- end -}}

{{/*
clusterKeySecretName — name of the Secret to wire CLUSTER_ENCRYPTION_KEY
from, or empty. Empty when the key is inlined (rendered straight into the
config) or when cluster mode is off. Precedence: inline key (no env) >
existingEncryptionKeySecret > chart-generated Secret.
*/}}
{{- define "cosmoguard.clusterKeySecretName" -}}
{{- $cluster := .Values.cluster | default (dict) -}}
{{- if include "cosmoguard.clusterInlineKey" . -}}
{{- else if $cluster.existingEncryptionKeySecret -}}
{{- $cluster.existingEncryptionKeySecret -}}
{{- else if eq (include "cosmoguard.shouldGenerateKey" .) "true" -}}
{{- include "cosmoguard.generatedKeySecretName" . -}}
{{- end -}}
{{- end -}}

{{/*
clusterContainerPorts emits the container ports needed for cluster mode.
gossip carries TCP AND UDP on the same port — both protocols are required
for memberlist; operators frequently block UDP by reflex.
*/}}
{{- define "cosmoguard.clusterContainerPorts" -}}
{{- if eq (include "cosmoguard.clusterEnabled" .) "true" -}}
{{- $cache := dig "cache" (dict) .Values.config }}
{{- $c := dig "cluster" (dict) $cache }}
{{- $bind := dig "bindPort" 3320 $c }}
{{- $gossip := dig "gossipPort" 3322 $c }}
{{- $peerApi := dig "peerApiPort" 0 $c }}
{{- if eq (int $peerApi) 0 }}{{ $peerApi = (add (int $bind) 1) }}{{ end }}
- name: cluster-rpc
  containerPort: {{ $bind }}
  protocol: TCP
- name: gossip-tcp
  containerPort: {{ $gossip }}
  protocol: TCP
- name: gossip-udp
  containerPort: {{ $gossip }}
  protocol: UDP
- name: peer-api
  containerPort: {{ $peerApi }}
  protocol: TCP
{{- end -}}
{{- end -}}

{{/*
gatewayParentRefs picks per-route parentRefs when set, otherwise falls
back to the chart-wide gateway.parentRefs. Returns nothing when both
are empty — caller must guard against that (a hostnames-set route
without parentRefs is invalid and we want a clear render-time error).
Args: a dict with `local` (the route-level parentRefs) and `global`
(the chart-wide default).
*/}}
{{- define "cosmoguard.gatewayParentRefs" -}}
{{- if gt (len .local) 0 -}}
{{- toYaml .local -}}
{{- else -}}
{{- toYaml .global -}}
{{- end -}}
{{- end -}}
