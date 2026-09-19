#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${ROOT_DIR}/helm/cosmoguard"
HELM_BIN="${HELM_BIN:-helm}"
YQ_BIN="${YQ_BIN:-yq}"
FULLNAME="config-reload-test"
EXTERNAL_CONFIG="external-cosmoguard-config"
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
  local kind=$2
  shift 2
  "${HELM_BIN}" template config-reload-test "${CHART_DIR}" \
    --namespace default \
    --set-string "fullnameOverride=${FULLNAME}" \
    --set-string "kind=${kind}" \
    --set-string cluster.existingEncryptionKeySecret=config-reload-test-key \
    "$@" >"${output}"
}

assert_reload_contract() {
  local file=$1
  local kind=$2
  local config_map=$3
  local workload="${tmp_dir}/${kind}-${config_map}-workload.yaml"
  WORKLOAD_KIND="${kind}" "${YQ_BIN}" '
    select(.kind == strenv(WORKLOAD_KIND) and .metadata.name == "config-reload-test") |
    .
  ' "${file}" >"${workload}" || return 1
  CONFIG_MAP="${config_map}" "${YQ_BIN}" -e '
    (.spec.template.spec.containers[] | select(.name == "cosmoguard")) as $container |
    ($container.volumeMounts[] | select(.name == "config")) as $mount |
    (.spec.template.spec.volumes[] | select(.name == "config")) as $volume |
    select($mount.mountPath == "/etc/cosmoguard") |
    select($mount.readOnly == true) |
    select($mount.subPath == null) |
    select($mount.subPathExpr == null) |
    select($container.args | contains(["--config=/etc/cosmoguard/cosmoguard.yaml"])) |
    select($volume.configMap.name == strenv(CONFIG_MAP)) |
    select(((.spec.template.metadata.annotations // {}) | has("checksum/config")) | not) |
    true
  ' "${workload}" >/dev/null
}

run_case() {
  local name=$1
  local kind=$2
  local config_map=$3
  shift 3
  local output="${tmp_dir}/${kind}-${name}.yaml"
  if render "${output}" "${kind}" "$@" && assert_reload_contract "${output}" "${kind}" "${config_map}"; then
    printf 'ok - %s: %s\n' "${kind}" "${name}"
  else
    printf 'not ok - %s: %s\n' "${kind}" "${name}" >&2
    failures=$((failures + 1))
  fi
}

for kind in Deployment StatefulSet; do
  run_case managed "${kind}" "${FULLNAME}"
  run_case external "${kind}" "${EXTERNAL_CONFIG}" \
    --set-string "existingConfigMap=${EXTERNAL_CONFIG}"
done

if ((failures > 0)); then
  printf '%d config-reload render regression case(s) failed\n' "${failures}" >&2
  exit 1
fi

echo "all config-reload render regression cases passed"
