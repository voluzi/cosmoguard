#!/usr/bin/env bash
# record-golden.sh — capture request/response pairs from a real Cosmos node
# for use as cosmoguard compatibility fixtures.
#
# Usage:
#   scripts/record-golden.sh <chain-name> <lcd-url> [rpc-url]
#
# Example:
#   scripts/record-golden.sh cosmoshub https://lcd.cosmoshub-4.example.com
#   scripts/record-golden.sh osmosis    https://lcd.osmosis.zone https://rpc.osmosis.zone
#
# Output: testdata/golden/<chain-name>/<endpoint>.json — one file per
# captured response. Each file contains a JSON envelope:
#   {
#     "request":  { "method":"GET", "path":"...", "headers":{...} },
#     "response": { "status":200, "headers":{...}, "body":"...base64..." }
#   }
#
# Cosmoguard's TestCompatibility suite replays these fixtures both
# direct and through a cosmoguard with default:allow / no rules, and
# asserts byte-identical responses (modulo a documented header
# allowlist like Date).
#
# This script reads from a real node so you can run it once before each
# release tag.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<EOF
usage: $0 <chain-name> <lcd-url> [rpc-url]
example: $0 cosmoshub https://lcd.cosmoshub-4.example.com
EOF
  exit 1
fi

CHAIN="$1"
LCD="${2%/}"
RPC="${3:-}"
OUT_DIR="testdata/golden/${CHAIN}"
mkdir -p "$OUT_DIR"

# Curated list of LCD endpoints worth pinning. Add more as needed —
# every chain's deployed apps reach a different shape.
LCD_ENDPOINTS=(
  "/cosmos/base/tendermint/v1beta1/node_info"
  "/cosmos/base/tendermint/v1beta1/syncing"
  "/cosmos/base/tendermint/v1beta1/blocks/latest"
  "/cosmos/staking/v1beta1/params"
  "/cosmos/staking/v1beta1/pool"
  "/cosmos/staking/v1beta1/validators"
  "/cosmos/distribution/v1beta1/community_pool"
  "/cosmos/mint/v1beta1/inflation"
  "/cosmos/mint/v1beta1/annual_provisions"
  "/cosmos/auth/v1beta1/params"
  "/cosmos/bank/v1beta1/params"
  "/cosmos/gov/v1beta1/params/voting"
  "/cosmos/gov/v1beta1/params/tallying"
  "/cosmos/slashing/v1beta1/params"
)

# Curated list of RPC endpoints (Tendermint).
RPC_ENDPOINTS=(
  "/status"
  "/abci_info"
  "/net_info"
  "/genesis"
)

# A header allowlist whose values cosmoguard's compat test ignores —
# these legitimately differ between the recorded snapshot and a live
# replay (Date, request-specific IDs, etc.). The list is written into
# the fixture set so the replay framework knows what to skip.
HEADERS_IGNORE=(
  "Date"
  "X-Request-Id"
  "Server"
  "X-Powered-By"
)

slugify() {
  # Convert "/cosmos/staking/v1beta1/params" into "cosmos_staking_v1beta1_params".
  printf '%s' "$1" | sed -e 's:^/::' -e 's:/:_:g'
}

capture_one() {
  local method="$1" base="$2" path="$3"
  local out="$OUT_DIR/$(slugify "${method,,}_${path}").json"
  echo "→ $method $base$path"
  # `-i` includes the response status line + headers; we parse them out.
  local raw status_line headers_block body status
  raw=$(curl -sS -i -X "$method" "$base$path") || {
    echo "  (failed, skipping)"
    return 0
  }
  status_line=$(printf '%s\n' "$raw" | head -1)
  status=$(printf '%s' "$status_line" | awk '{print $2}')
  # Split at the FIRST blank line only — subsequent blank lines in
  # the body must be preserved. The previous shape stripped every
  # blank line in the body because the `/^\r?$/` pattern fired
  # unconditionally on every empty line, not just the header/body
  # separator. That corrupted fixtures meant for byte-identical
  # comparison (multi-line JSON pretty-printed responses include
  # blank lines in some encodings).
  headers_block=$(printf '%s\n' "$raw" | awk 'p==0 && /^\r?$/{p=1; next} p==0{print}')
  body=$(printf '%s\n' "$raw" | awk 'p==0 && /^\r?$/{p=1; next} p==1{print}')
  # Render headers as a JSON object. Each "Name: value" line.
  # Split on the FIRST ": " only — header values may themselves
  # contain ": " (e.g. Link: <url>; rel: next) and the previous
  # awk -F': ' shape would have truncated everything after the
  # second occurrence by printing only $2.
  local headers_json
  headers_json=$(printf '%s\n' "$headers_block" | awk '
    NR>1 {
      gsub(/\r/,"")
      idx = index($0, ": ")
      if (idx > 0) {
        name = substr($0, 1, idx-1)
        value = substr($0, idx+2)
        gsub(/\\/, "\\\\", value); gsub(/"/, "\\\"", value)
        printf "%s\"%s\":\"%s\"", sep, name, value
        sep=","
      }
    }
    END{print ""}'
  )
  # Body base64 so binary safe.
  local body_b64
  body_b64=$(printf '%s' "$body" | base64 | tr -d '\n')

  cat >"$out" <<EOF
{
  "request": {
    "method": "$method",
    "path": "$path"
  },
  "response": {
    "status": ${status:-0},
    "headers": { $headers_json },
    "body_base64": "$body_b64"
  }
}
EOF
}

echo "Recording LCD endpoints from $LCD into $OUT_DIR"
for p in "${LCD_ENDPOINTS[@]}"; do
  capture_one GET "$LCD" "$p"
done

if [[ -n "$RPC" ]]; then
  RPC="${RPC%/}"
  echo "Recording RPC endpoints from $RPC"
  for p in "${RPC_ENDPOINTS[@]}"; do
    capture_one GET "$RPC" "$p"
  done
fi

# Write a manifest summarizing what was captured + the headers
# allowlist. The replay framework reads this.
{
  printf '{\n  "chain":"%s",\n  "recorded_at":"%s",\n  "lcd":"%s",\n  "rpc":"%s",\n  "ignore_headers":[' \
    "$CHAIN" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LCD" "$RPC"
  sep=""
  for h in "${HEADERS_IGNORE[@]}"; do
    printf '%s"%s"' "$sep" "$h"; sep=","
  done
  printf ']\n}\n'
} >"$OUT_DIR/manifest.json"

echo "Done. Fixtures at $OUT_DIR"
