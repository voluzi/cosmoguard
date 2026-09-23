#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${ROOT_DIR}/helm/cosmoguard"
HELM_BIN="${HELM_BIN:-helm}"
YQ_BIN="${YQ_BIN:-yq}"
FULLNAME="hardening-test"
failures=0

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

if ! command -v "${HELM_BIN}" >/dev/null 2>&1; then
  echo "missing Helm command: ${HELM_BIN}" >&2
  exit 2
fi
if ! command -v "${YQ_BIN}" >/dev/null 2>&1; then
  echo "missing yq command: ${YQ_BIN}" >&2
  exit 2
fi

render() {
  local output=$1
  shift
  "${HELM_BIN}" template hardening-test "${CHART_DIR}" \
    --namespace default \
    --set-string "fullnameOverride=${FULLNAME}" \
    --set-string cluster.existingEncryptionKeySecret=hardening-test-key \
    "$@" >"${output}"
}

# check NAME SELECTOR PREDICATE [helm args...] renders the chart, requires
# SELECTOR to match exactly one document, and PREDICATE to be true on it.
check() {
  local name=$1
  local selector=$2
  local predicate=$3
  shift 3
  local output="${tmp_dir}/${name// /-}.yaml"
  if render "${output}" "$@" &&
    "${YQ_BIN}" ea -e "[${selector}] | ((length == 1) and (.[0] | (${predicate})))" "${output}" >/dev/null 2>&1; then
    printf 'ok - %s\n' "${name}"
  else
    printf 'not ok - %s\n' "${name}" >&2
    failures=$((failures + 1))
  fi
}

for kind in Deployment StatefulSet; do
  check "${kind}: RuntimeDefault seccomp and no service account token" \
    'select(.kind == "'"${kind}"'")' '
    (.spec.template.spec.securityContext.seccompProfile.type == "RuntimeDefault") and
    (.spec.template.spec.automountServiceAccountToken == false)
  ' --set-string "kind=${kind}"
done

check "public Service carries no operator ports" \
  'select(.kind == "Service" and .metadata.name == "hardening-test")' '
  [.spec.ports[].name] | ((contains(["metrics"]) or contains(["dashboard"])) | not)
' --set service.type=LoadBalancer --set config.dashboard.enable=true

check "internal ClusterIP Service carries metrics and dashboard" \
  'select(.kind == "Service" and .metadata.name == "hardening-test-internal")' '
  (.spec.type == "ClusterIP") and ([.spec.ports[].name] | contains(["metrics", "dashboard"]))
' --set service.type=LoadBalancer --set config.dashboard.enable=true

check "dashboard Ingress routes to the internal Service" \
  'select(.kind == "Ingress")' '
  .spec.rules[0].http.paths[0].backend.service.name == "hardening-test-internal"
' --set config.dashboard.enable=true --set ingressDashboard.enabled=true \
  --set 'ingressDashboard.hosts[0]=dashboard.example.com'

check "dashboard HTTPRoute routes to the internal Service" \
  'select(.kind == "HTTPRoute")' '
  .spec.rules[0].backendRefs[0].name == "hardening-test-internal"
' --set config.dashboard.enable=true --set gateway.enabled=true \
  --set 'gateway.dashboard.hostnames[0]=dashboard.example.com' \
  --set-json 'gateway.parentRefs=[{"name":"gw"}]'

check "NetworkPolicy keeps the metrics rule when proxyIngress is set" \
  'select(.kind == "NetworkPolicy")' '
  [.spec.ingress[] | select(.ports[]?.port == 9001)] | length == 1
' --set networkPolicy.enabled=true \
  --set-json 'networkPolicy.proxyIngress=[{"from":[{"podSelector":{}}]}]'

check "NetworkPolicy metrics rule honours metricsFrom" \
  'select(.kind == "NetworkPolicy")' '
  [.spec.ingress[] | select(.ports[]?.port == 9001)] | ((length == 1) and
  (.[0].from[0].namespaceSelector.matchLabels["kubernetes.io/metadata.name"] == "monitoring"))
' --set networkPolicy.enabled=true \
  --set-json 'networkPolicy.metricsFrom=[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"monitoring"}}}]'

check "PDB accepts maxUnavailable alone" \
  'select(.kind == "PodDisruptionBudget")' '
  (.spec.maxUnavailable == 1) and (.spec | has("minAvailable") | not)
' --set podDisruptionBudget.enabled=true --set podDisruptionBudget.maxUnavailable=1

check "PDB defaults to minAvailable 1" \
  'select(.kind == "PodDisruptionBudget")' '.spec.minAvailable == 1' --set podDisruptionBudget.enabled=true

if ((failures > 0)); then
  printf '%d hardening render regression case(s) failed\n' "${failures}" >&2
  exit 1
fi

echo "all hardening render regression cases passed"
