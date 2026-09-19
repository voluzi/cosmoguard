#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${ROOT_DIR}/helm/cosmoguard"
HELM_BIN="${HELM_BIN:-helm}"
YQ_BIN="${YQ_BIN:-yq}"
FIXTURE_KEY="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
SECOND_FIXTURE_KEY="ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA="
FULLNAME="cluster-key-test"
GENERATED_SECRET="${FULLNAME}-cluster-key"
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
  "${HELM_BIN}" template cluster-key-test "${CHART_DIR}" \
    --namespace default \
    --set-string "fullnameOverride=${FULLNAME}" \
    --set-string "kind=${kind}" \
    "$@" >"${output}" 2>"${tmp_dir}/helm.stderr"
}

run_case() {
  local name=$1
  shift
  if "$@"; then
    printf 'ok - %s\n' "${name}"
  else
    printf 'not ok - %s\n' "${name}" >&2
    failures=$((failures + 1))
  fi
}

expect_render_failure() {
  local kind=$1
  local diagnostic=$2
  shift 2
  local output="${tmp_dir}/unexpected.yaml"
  if render "${output}" "${kind}" "$@"; then
    echo "render unexpectedly succeeded" >&2
    return 1
  fi
  if ! grep -Fq "${diagnostic}" "${tmp_dir}/helm.stderr"; then
    echo "render failed without expected diagnostic: ${diagnostic}" >&2
    sed -n '1,20p' "${tmp_dir}/helm.stderr" >&2
    return 1
  fi
}

render_pair() {
  local prefix=$1
  local kind=$2
  shift 2
  render "${tmp_dir}/${prefix}-a.yaml" "${kind}" "$@" &&
    render "${tmp_dir}/${prefix}-b.yaml" "${kind}" "$@"
}

assert_deterministic() {
  local prefix=$1
  cmp -s "${tmp_dir}/${prefix}-a.yaml" "${tmp_dir}/${prefix}-b.yaml"
}

resource_count() {
  local file=$1
  local kind=$2
  local name=$3
  RESOURCE_KIND="${kind}" RESOURCE_NAME="${name}" \
    "${YQ_BIN}" -r \
    'select(.kind == strenv(RESOURCE_KIND) and .metadata.name == strenv(RESOURCE_NAME)) | .metadata.name' \
    "${file}" | awk 'NF { count++ } END { print count + 0 }'
}

assert_resource_absent() {
  local file=$1
  local kind=$2
  local name=$3
  local count
  count="$(resource_count "${file}" "${kind}" "${name}")" || return 1
  test "${count}" -eq 0
}

assert_resource_present_once() {
  local file=$1
  local kind=$2
  local name=$3
  test "$(resource_count "${file}" "${kind}" "${name}")" -eq 1
}

key_env_count() {
  local file=$1
  local kind=$2
  WORKLOAD_KIND="${kind}" "${YQ_BIN}" -r '
    select(.kind == strenv(WORKLOAD_KIND)) |
    .spec.template.spec.containers[] |
    select(.name == "cosmoguard") |
    .env[]? |
    select(.name == "CLUSTER_ENCRYPTION_KEY") |
    .name
  ' "${file}" | awk 'NF { count++ } END { print count + 0 }'
}

assert_secret_key_env() {
  local file=$1
  local kind=$2
  local secret_name=$3
  test "$(key_env_count "${file}" "${kind}")" -eq 1 || return 1
  test "$(WORKLOAD_KIND="${kind}" SECRET_NAME="${secret_name}" \
    "${YQ_BIN}" -r '
      select(.kind == strenv(WORKLOAD_KIND)) |
      .spec.template.spec.containers[] |
      select(.name == "cosmoguard") |
      .env[]? |
      select(
        .name == "CLUSTER_ENCRYPTION_KEY" and
        .valueFrom.secretKeyRef.name == strenv(SECRET_NAME) and
        .valueFrom.secretKeyRef.key == "encryptionKey"
      ) |
      .name
    ' "${file}" | awk 'NF { count++ } END { print count + 0 }')" -eq 1
}

assert_plain_key_env() {
  local file=$1
  local kind=$2
  local value=$3
  test "$(key_env_count "${file}" "${kind}")" -eq 1 || return 1
  test "$(WORKLOAD_KIND="${kind}" KEY_VALUE="${value}" \
    "${YQ_BIN}" -r '
      select(.kind == strenv(WORKLOAD_KIND)) |
      .spec.template.spec.containers[] |
      select(.name == "cosmoguard") |
      .env[]? |
      select(.name == "CLUSTER_ENCRYPTION_KEY" and .value == strenv(KEY_VALUE)) |
      .name
    ' "${file}" | awk 'NF { count++ } END { print count + 0 }')" -eq 1
}

assert_no_key_env() {
  local file=$1
  local kind=$2
  local count
  count="$(key_env_count "${file}" "${kind}")" || return 1
  test "${count}" -eq 0
}

assert_env_from_secret() {
  local file=$1
  local kind=$2
  local secret_name=$3
  test "$(WORKLOAD_KIND="${kind}" SECRET_NAME="${secret_name}" \
    "${YQ_BIN}" -r '
      select(.kind == strenv(WORKLOAD_KIND)) |
      .spec.template.spec.containers[] |
      select(.name == "cosmoguard") |
      .envFrom[]? |
      select(.secretRef.name == strenv(SECRET_NAME)) |
      .secretRef.name
    ' "${file}" | awk 'NF { count++ } END { print count + 0 }')" -eq 1
}

assert_config_map_mount() {
  local file=$1
  local kind=$2
  local config_name=$3
  test "$(WORKLOAD_KIND="${kind}" CONFIG_NAME="${config_name}" \
    "${YQ_BIN}" -r '
      select(.kind == strenv(WORKLOAD_KIND)) |
      .spec.template.spec.volumes[]? |
      select(.name == "config" and .configMap.name == strenv(CONFIG_NAME)) |
      .configMap.name
    ' "${file}" | awk 'NF { count++ } END { print count + 0 }')" -eq 1
}

assert_rendered_config_key() {
  local file=$1
  local value=$2
  KEY_VALUE="${value}" "${YQ_BIN}" -e '
    select(.kind == "ConfigMap" and .metadata.name == "cluster-key-test") |
    .data["cosmoguard.yaml"] | from_yaml |
    .cache.cluster.encryptionKey == strenv(KEY_VALUE)
  ' "${file}" >/dev/null
}

assert_generated_key_is_32_bytes() {
  local file=$1
  local decoded="${tmp_dir}/decoded-key"
  "${YQ_BIN}" -r '
    select(.kind == "Secret" and .metadata.name == "cluster-key-test-cluster-key") |
    .data.encryptionKey
  ' "${file}" | openssl base64 -d -A >"${decoded}" 2>/dev/null || return 1
  test "$(wc -c <"${decoded}" | tr -d ' ')" -eq 44 || return 1
  openssl base64 -d -A <"${decoded}" >"${decoded}.raw" 2>/dev/null || return 1
  test "$(wc -c <"${decoded}.raw" | tr -d ' ')" -eq 32
}

case_default_fails() {
  expect_render_failure "$1" "cluster mode requires an encryption key"
}

case_external_missing_fails() {
  local kind=$1
  shift
  expect_render_failure "${kind}" "existingConfigMap requires CLUSTER_ENCRYPTION_KEY wiring" \
    --set-string "existingConfigMap=${EXTERNAL_CONFIG}" "$@"
}

case_dedicated_secret() {
  local kind=$1
  local prefix="${kind}-dedicated"
  render_pair "${prefix}" "${kind}" \
    --set-string cluster.existingEncryptionKeySecret=shared-cluster-key &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_secret_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" shared-cluster-key &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" '${CLUSTER_ENCRYPTION_KEY}'
}

case_external_dedicated_secret() {
  local kind=$1
  local prefix="${kind}-external-dedicated"
  shift
  render_pair "${prefix}" "${kind}" \
    --set-string "existingConfigMap=${EXTERNAL_CONFIG}" \
    --set-string cluster.existingEncryptionKeySecret=shared-cluster-key "$@" &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" ConfigMap "${FULLNAME}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_config_map_mount "${tmp_dir}/${prefix}-a.yaml" "${kind}" "${EXTERNAL_CONFIG}" &&
    assert_secret_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" shared-cluster-key
}

case_external_existing_secret() {
  local kind=$1
  local output="${tmp_dir}/${kind}-external-existing-secret.yaml"
  render "${output}" "${kind}" \
    --set-string "existingConfigMap=${EXTERNAL_CONFIG}" \
    --set-string existingSecret=app-environment &&
    assert_env_from_secret "${output}" "${kind}" app-environment &&
    assert_no_key_env "${output}" "${kind}" &&
    assert_resource_absent "${output}" Secret "${GENERATED_SECRET}"
}

case_external_plain_env() {
  local kind=$1
  local output="${tmp_dir}/${kind}-external-plain-env.yaml"
  render "${output}" "${kind}" \
    --set-string "existingConfigMap=${EXTERNAL_CONFIG}" \
    --set-string "env.CLUSTER_ENCRYPTION_KEY=${FIXTURE_KEY}" &&
    assert_plain_key_env "${output}" "${kind}" "${FIXTURE_KEY}" &&
    assert_resource_absent "${output}" Secret "${GENERATED_SECRET}"
}

case_managed_existing_secret() {
  local kind=$1
  local output="${tmp_dir}/${kind}-managed-existing-secret.yaml"
  render "${output}" "${kind}" --set-string existingSecret=app-environment &&
    assert_rendered_config_key "${output}" '${CLUSTER_ENCRYPTION_KEY}' &&
    assert_env_from_secret "${output}" "${kind}" app-environment &&
    assert_no_key_env "${output}" "${kind}" &&
    assert_resource_absent "${output}" Secret "${GENERATED_SECRET}"
}

case_managed_plain_env() {
  local kind=$1
  local output="${tmp_dir}/${kind}-managed-plain-env.yaml"
  render "${output}" "${kind}" \
    --set-string "env.CLUSTER_ENCRYPTION_KEY=${FIXTURE_KEY}" &&
    assert_rendered_config_key "${output}" '${CLUSTER_ENCRYPTION_KEY}' &&
    assert_plain_key_env "${output}" "${kind}" "${FIXTURE_KEY}" &&
    assert_resource_absent "${output}" Secret "${GENERATED_SECRET}"
}

case_inline_key() {
  local kind=$1
  local source=$2
  local prefix="${kind}-inline-${source//./-}"
  render_pair "${prefix}" "${kind}" --set-string "${source}=${FIXTURE_KEY}" &&
    assert_deterministic "${prefix}" &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" "${FIXTURE_KEY}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_no_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}"
}

case_inline_precedence() {
  local kind=$1
  local output="${tmp_dir}/${kind}-inline-precedence.yaml"
  render "${output}" "${kind}" \
    --set-string "cluster.encryptionKey=${FIXTURE_KEY}" \
    --set-string "config.cache.cluster.encryptionKey=${SECOND_FIXTURE_KEY}" &&
    assert_rendered_config_key "${output}" "${SECOND_FIXTURE_KEY}"
}

case_generated_key() {
  local kind=$1
  local first="${tmp_dir}/${kind}-generated-a.yaml"
  local second="${tmp_dir}/${kind}-generated-b.yaml"
  render "${first}" "${kind}" --set cluster.generateEncryptionKey=true &&
    render "${second}" "${kind}" --set cluster.generateEncryptionKey=true &&
    assert_resource_present_once "${first}" Secret "${GENERATED_SECRET}" &&
    assert_secret_key_env "${first}" "${kind}" "${GENERATED_SECRET}" &&
    assert_generated_key_is_32_bytes "${first}" &&
    assert_generated_key_is_32_bytes "${second}"
}

case_generation_with_secret() {
  local kind=$1
  local prefix="${kind}-generate-secret"
  render_pair "${prefix}" "${kind}" \
    --set cluster.generateEncryptionKey=true \
    --set-string cluster.existingEncryptionKeySecret=shared-cluster-key &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_secret_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" shared-cluster-key
}

case_generation_with_inline() {
  local kind=$1
  local prefix="${kind}-generate-inline"
  render_pair "${prefix}" "${kind}" \
    --set cluster.generateEncryptionKey=true \
    --set-string "cluster.encryptionKey=${FIXTURE_KEY}" &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_no_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" "${FIXTURE_KEY}"
}

case_generation_with_existing_secret() {
  local kind=$1
  local prefix="${kind}-generate-existing-secret"
  render_pair "${prefix}" "${kind}" \
    --set cluster.generateEncryptionKey=true \
    --set-string existingSecret=app-environment &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" '${CLUSTER_ENCRYPTION_KEY}' &&
    assert_env_from_secret "${tmp_dir}/${prefix}-a.yaml" "${kind}" app-environment &&
    assert_no_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}"
}

case_generation_with_plain_env() {
  local kind=$1
  local prefix="${kind}-generate-plain-env"
  render_pair "${prefix}" "${kind}" \
    --set cluster.generateEncryptionKey=true \
    --set-string "env.CLUSTER_ENCRYPTION_KEY=${FIXTURE_KEY}" &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" '${CLUSTER_ENCRYPTION_KEY}' &&
    assert_plain_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" "${FIXTURE_KEY}"
}

case_plain_env_with_dedicated_secret() {
  local kind=$1
  local prefix="${kind}-plain-env-dedicated-secret"
  render_pair "${prefix}" "${kind}" \
    --set-string cluster.existingEncryptionKeySecret=shared-cluster-key \
    --set-string "env.CLUSTER_ENCRYPTION_KEY=${FIXTURE_KEY}" &&
    assert_deterministic "${prefix}" &&
    assert_resource_absent "${tmp_dir}/${prefix}-a.yaml" Secret "${GENERATED_SECRET}" &&
    assert_rendered_config_key "${tmp_dir}/${prefix}-a.yaml" '${CLUSTER_ENCRYPTION_KEY}' &&
    assert_plain_key_env "${tmp_dir}/${prefix}-a.yaml" "${kind}" "${FIXTURE_KEY}"
}

case_cluster_disabled() {
  local kind=$1
  local external=${2:-}
  local output="${tmp_dir}/${kind}-disabled-${external:-managed}.yaml"
  local args=(--set config.cache.cluster.enable=false)
  if [[ -n "${external}" ]]; then
    args+=(--set-string "existingConfigMap=${EXTERNAL_CONFIG}")
  fi
  render "${output}" "${kind}" "${args[@]}" &&
    assert_resource_absent "${output}" Secret "${GENERATED_SECRET}" &&
    assert_no_key_env "${output}" "${kind}"
}

for kind in StatefulSet Deployment; do
  run_case "${kind}: defaults fail closed" case_default_fails "${kind}"
  run_case "${kind}: external ConfigMap without key wiring fails" case_external_missing_fails "${kind}"
  run_case "${kind}: external ConfigMap cannot generate a key" case_external_missing_fails "${kind}" \
    --set cluster.generateEncryptionKey=true
  run_case "${kind}: top-level inline key cannot reach external ConfigMap" case_external_missing_fails "${kind}" \
    --set-string "cluster.encryptionKey=${FIXTURE_KEY}"
  run_case "${kind}: config inline key cannot reach external ConfigMap" case_external_missing_fails "${kind}" \
    --set-string "config.cache.cluster.encryptionKey=${FIXTURE_KEY}"
  run_case "${kind}: dedicated Secret is deterministic" case_dedicated_secret "${kind}"
  run_case "${kind}: external ConfigMap plus dedicated Secret" case_external_dedicated_secret "${kind}"
  run_case "${kind}: ignored external inline key does not suppress Secret wiring" \
    case_external_dedicated_secret "${kind}" --set-string "cluster.encryptionKey=${FIXTURE_KEY}"
  run_case "${kind}: external ConfigMap plus existingSecret" case_external_existing_secret "${kind}"
  run_case "${kind}: external ConfigMap plus explicit environment key" case_external_plain_env "${kind}"
  run_case "${kind}: managed ConfigMap plus existingSecret" case_managed_existing_secret "${kind}"
  run_case "${kind}: managed ConfigMap plus explicit environment key" case_managed_plain_env "${kind}"
  run_case "${kind}: top-level inline key remains supported" case_inline_key "${kind}" cluster.encryptionKey
  run_case "${kind}: config inline key remains supported" case_inline_key "${kind}" config.cache.cluster.encryptionKey
  run_case "${kind}: config inline key keeps precedence" case_inline_precedence "${kind}"
  run_case "${kind}: explicit generation creates a 32-byte key" case_generated_key "${kind}"
  run_case "${kind}: dedicated Secret wins over generation" case_generation_with_secret "${kind}"
  run_case "${kind}: inline key wins over generation" case_generation_with_inline "${kind}"
  run_case "${kind}: existingSecret wins over generation" case_generation_with_existing_secret "${kind}"
  run_case "${kind}: explicit environment key wins over generation" case_generation_with_plain_env "${kind}"
  run_case "${kind}: explicit environment key wins over dedicated Secret" case_plain_env_with_dedicated_secret "${kind}"
  run_case "${kind}: disabled cluster needs no key" case_cluster_disabled "${kind}"
  run_case "${kind}: disabled cluster accepts external ConfigMap" case_cluster_disabled "${kind}" external
done

if ((failures > 0)); then
  printf '%d cluster-key regression case(s) failed\n' "${failures}" >&2
  exit 1
fi

echo "all cluster-key regression cases passed"
