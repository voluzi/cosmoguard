#!/usr/bin/env bash
# record-golden-remote.sh — capture request/response pairs from a live
# Cosmos node fronted by a TLS-terminating edge. Companion to
# record-golden.sh (which targets a local plaintext node).
#
# Usage:
#   scripts/record-golden-remote.sh --chain nibiru --lcd https://lcd.nibiru.voluzi.com \
#     --rpc https://rpc.nibiru.voluzi.com --evm-rpc https://evm-rpc.nibiru.voluzi.com
#
#   scripts/record-golden-remote.sh --chain allora --lcd https://lcd.allora.voluzi.com \
#     --rpc https://rpc.allora.voluzi.com
#
# Output: pkg/cosmoguard/testharness/testdata/golden/<chain>-live/<slug>.json
#         pkg/cosmoguard/testharness/testdata/golden/<chain>-live/manifest.json
#
# The "-live" suffix distinguishes these from the local-fake-upstream
# fixtures record-golden.sh produces. TestK_GoldenLiveCompatibility
# reads from the -live directory.
#
# All fixtures are written with the same JSON shape as record-golden.sh
# plus optional request.body_base64 / request.content_type for non-GET
# captures (JSON-RPC POSTs).

set -euo pipefail

# Track temp files so a Ctrl-C / SIGTERM during curl or python parsing
# doesn't leak files into /tmp. capture() appends to this array; the
# EXIT trap fires for both normal exit and signal-interrupt.
TMP_FILES=()
cleanup() {
  if (( ${#TMP_FILES[@]} > 0 )); then
    rm -f "${TMP_FILES[@]}"
  fi
}
trap cleanup EXIT INT TERM

CHAIN=""
LCD=""
RPC=""
EVM_RPC=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --chain)    CHAIN="$2"; shift 2 ;;
    --lcd)      LCD="${2%/}"; shift 2 ;;
    --rpc)      RPC="${2%/}"; shift 2 ;;
    --evm-rpc)  EVM_RPC="${2%/}"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "unknown flag: $1" >&2
      exit 2 ;;
  esac
done

if [[ -z "$CHAIN" || -z "$LCD" || -z "$RPC" ]]; then
  echo "usage: $0 --chain <name> --lcd <https-url> --rpc <https-url> [--evm-rpc <https-url>]" >&2
  exit 2
fi

OUT_DIR="pkg/cosmoguard/testharness/testdata/golden/${CHAIN}-live"
mkdir -p "$OUT_DIR"

# Curated endpoints — kept small and deterministic so a recording made
# moments before a test replay is unlikely to drift. Anything
# height-sensitive (latest block, status's block height) is recorded
# but the replay treats `/status` specially in case the timestamp
# changes between record and replay (see TestK_GoldenLiveCompatibility).
LCD_ENDPOINTS=(
  "/cosmos/base/tendermint/v1beta1/node_info"
  "/cosmos/bank/v1beta1/params"
  "/cosmos/staking/v1beta1/params"
  "/cosmos/auth/v1beta1/params"
  "/cosmos/slashing/v1beta1/params"
)

RPC_GET_ENDPOINTS=(
  "/abci_info"
  "/health"
  "/genesis_chunked?chunk=0"
)

# JSON-RPC POSTs to the RPC root. Same methods exercised over both
# transports (REST GET on Tendermint RPC and JSON-RPC POST) so the
# replay catches any divergence in how cosmoguard handles them.
RPC_JSONRPC_PAYLOADS=(
  '{"jsonrpc":"2.0","id":1,"method":"abci_info","params":[]}'
  '{"jsonrpc":"2.0","id":1,"method":"health","params":[]}'
)

EVM_JSONRPC_PAYLOADS=(
  '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
  '{"jsonrpc":"2.0","id":1,"method":"net_version","params":[]}'
)

# Headers cosmoguard or the edge can legitimately differ on between
# record and replay. The replay framework reads this list from the
# manifest and skips comparison for any header that matches.
HEADERS_IGNORE=(
  "Date"
  "X-Request-Id"
  "X-Cosmoguard-Cache"
  "Server"
  "X-Powered-By"
  "Cf-Ray"
  "Cf-Cache-Status"
  "Age"
  "Via"
  "Strict-Transport-Security"
  "Alt-Svc"
  "Set-Cookie"
  "Vary"
  "Access-Control-Allow-Origin"
  "Access-Control-Allow-Methods"
  "Access-Control-Allow-Headers"
  "Access-Control-Expose-Headers"
  "Access-Control-Max-Age"
)

slugify() {
  # "/cosmos/staking/v1beta1/params"           -> "cosmos_staking_v1beta1_params"
  # "/genesis_chunked?chunk=0"                 -> "genesis_chunked_qs_chunk_0"
  # "POST /eth_chainId" with body becomes the caller's responsibility.
  printf '%s' "$1" \
    | sed -e 's:^/::' \
          -e 's:?:_qs_:g' \
          -e 's:=:_:g' \
          -e 's:&:_:g' \
          -e 's:/:_:g'
}

# lower "<s>" — portable, locale-stable lowercase. Avoids ${var,,}
# (bash 4 only; macOS ships bash 3.2) and forces LC_ALL=C so that
# tr's [:upper:] class is the ASCII A-Z range regardless of the user's
# locale. Without LC_ALL=C, a Turkish locale maps "I" to dotless "ı",
# which silently breaks slug filenames.
lower() { printf '%s' "$1" | LC_ALL=C tr '[:upper:]' '[:lower:]'; }

capture() {
  local method="$1" base="$2" path="$3" body="${4:-}" content_type="${5:-}" slug_hint="${6:-}"
  local method_lc out
  method_lc=$(lower "$method")
  if [[ -n "$slug_hint" ]]; then
    out="$OUT_DIR/$(slugify "${method_lc}_${slug_hint}").json"
  else
    out="$OUT_DIR/$(slugify "${method_lc}_${path}").json"
  fi
  echo "  $method $base$path"

  # Service label maps the request back to which cosmoguard listener
  # the replay should target — the LCD proxy on port LcdPort vs the
  # RPC proxy on RpcPort, etc.
  local service=""
  case "$base" in
    "$LCD")     service="lcd" ;;
    "$RPC")     service="rpc" ;;
    "$EVM_RPC") service="evm_rpc" ;;
  esac

  # Capture raw HTTP bytes to a temp file. Going via a file (rather
  # than bash `raw=$(curl ...)`) is essential to preserve trailing
  # newlines in the body — command substitution always strips them,
  # which would make every captured body off-by-one against an upstream
  # that terminates its JSON with "\n".
  local raw_file
  raw_file=$(mktemp -t cosmoguard-golden.XXXXXX) || {
    echo "    (failed to create tempfile, skipping)"
    return 0
  }
  # Registered for cleanup on EXIT/INT/TERM. The function also rm -f's
  # at the end on the happy path; the trap handles the abort paths.
  TMP_FILES+=("$raw_file")
  if [[ -n "$body" ]]; then
    if ! curl -sS -i -X "$method" -H "Content-Type: ${content_type:-application/json}" \
         --data-raw "$body" "$base$path" >"$raw_file" 2>/dev/null; then
      echo "    (failed, skipping)"
      return 0
    fi
  else
    if ! curl -sS -i -X "$method" "$base$path" >"$raw_file" 2>/dev/null; then
      echo "    (failed, skipping)"
      return 0
    fi
  fi

  # Parse the response and emit the fixture JSON with python3. Reasons
  # for using python here instead of awk:
  #   - byte-faithful: open(..., "rb").read() returns the exact bytes
  #     curl wrote, including any trailing newline. awk's line-oriented
  #     input model can't tell whether a body ended with "\n", and the
  #     `$(...)`-then-base64 path strips trailing newlines anyway, so
  #     awk-based extraction is guaranteed off-by-one on any body that
  #     ends with a newline.
  #   - safe header tolerance: RFC 7230 §3.2 says field-value SP is
  #     optional after ":". awk's strict ": " split silently drops any
  #     header that doesn't have a space; partition + strip handles both.
  #   - safe JSON emission: json.dump escapes everything correctly, so
  #     a hostile header value containing dquotes/backslashes/control
  #     chars cannot corrupt the output file.
  #   - 1xx vs 101 carve-out: 1xx Informational (100, 102, 103…) is
  #     followed by another HTTP response, but 101 Switching Protocols
  #     hands the connection to a non-HTTP protocol and must be treated
  #     as the final response.
  # Wrap python3 in `if !` so its non-zero exit is captured by the
  # if-condition rather than tripping `set -e` and aborting the whole
  # recording session. Under set -e a bare `cmd <<EOF ... EOF` would
  # abort the function on python failure and the "skipping" branch
  # below would be dead code.
  if ! METHOD="$method" PATH_J="$path" SERVICE="$service" \
       CONTENT_TYPE="${content_type:-}" REQ_BODY="$body" \
       python3 - "$raw_file" >"$out" <<'PYEOF'
import base64
import json
import os
import re
import sys

raw = open(sys.argv[1], "rb").read()

pos = 0
skip = 0
status = 0
head = b""
body_start = len(raw)
while True:
    end = raw.find(b"\r\n\r\n", pos)
    sep = 4
    if end < 0:
        end = raw.find(b"\n\n", pos)
        sep = 2
    if end < 0:
        # No header terminator found. Treat what we have as headers
        # and an empty body so the caller still gets a status if we
        # parsed one.
        head = raw[pos:]
        break
    head = raw[pos:end]
    first_line = head.split(b"\n", 1)[0].rstrip(b"\r").decode("latin-1")
    m = re.match(r"HTTP/\S+\s+(\d+)", first_line)
    if not m:
        print(
            "    (warning: could not parse status from response head: %s)" % first_line,
            file=sys.stderr,
        )
        status = 0
        body_start = end + sep
        break
    status = int(m.group(1))
    # 1xx Informational responses (100 Continue, 102 Processing,
    # 103 Early Hints…) are followed by the actual response. 101
    # Switching Protocols is terminal — what comes next is no longer
    # HTTP.
    if 100 <= status < 200 and status != 101:
        pos = end + sep
        skip += 1
        if skip > 5:
            print(
                "    (too many 1xx informational responses, skipping)",
                file=sys.stderr,
            )
            sys.exit(1)
        continue
    body_start = end + sep
    break

body = raw[body_start:]

headers = {}
for line in head.split(b"\n")[1:]:
    line = line.rstrip(b"\r")
    if not line or b":" not in line:
        continue
    k, _, v = line.partition(b":")
    # RFC 7230 §3.2: the SP after ":" is optional. strip() handles both
    # "Foo: bar" and "Foo:bar"; without this, an awk split on ": " would
    # silently drop the latter.
    name = k.decode("latin-1").strip()
    value = v.decode("latin-1").strip()
    if name:
        headers[name] = value

out = {
    "request": {
        "method": os.environ.get("METHOD", ""),
        "path": os.environ.get("PATH_J", ""),
        "service": os.environ.get("SERVICE", ""),
        "content_type": os.environ.get("CONTENT_TYPE", ""),
        "body_base64": base64.b64encode(
            os.environ.get("REQ_BODY", "").encode("utf-8")
        ).decode("ascii"),
    },
    "response": {
        "status": status,
        "headers": headers,
        "body_base64": base64.b64encode(body).decode("ascii"),
    },
}
if status == 0:
    # A response we couldn't pull a status out of. Writing a fixture
    # with status:0 would make the replay test assert against the
    # wrong invariant — bail loudly instead.
    print(
        "    (warning: no parseable status; skipping fixture)",
        file=sys.stderr,
    )
    sys.exit(2)

json.dump(out, sys.stdout, indent=2)
sys.stdout.write("\n")
PYEOF
  then
    rm -f "$out"
    echo "    (failed to parse response, skipping)"
    return 0
  fi
  rm -f "$raw_file"
}

echo "Recording LCD endpoints from $LCD into $OUT_DIR"
for p in "${LCD_ENDPOINTS[@]}"; do
  capture GET "$LCD" "$p"
done

echo "Recording RPC GET endpoints from $RPC"
for p in "${RPC_GET_ENDPOINTS[@]}"; do
  capture GET "$RPC" "$p"
done

echo "Recording RPC JSON-RPC POSTs to $RPC/"
i=0
for payload in "${RPC_JSONRPC_PAYLOADS[@]}"; do
  method_name=$(printf '%s' "$payload" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  capture POST "$RPC" "/" "$payload" "application/json" "jsonrpc_${method_name}"
  i=$((i+1))
done

if [[ -n "$EVM_RPC" ]]; then
  echo "Recording EVM JSON-RPC POSTs to $EVM_RPC/"
  for payload in "${EVM_JSONRPC_PAYLOADS[@]}"; do
    method_name=$(printf '%s' "$payload" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    capture POST "$EVM_RPC" "/" "$payload" "application/json" "evmjsonrpc_${method_name}"
  done
fi

{
  printf '{\n  "chain":"%s-live",\n  "recorded_at":"%s",\n  "lcd":"%s",\n  "rpc":"%s",\n  "evm_rpc":"%s",\n  "ignore_headers":[' \
    "$CHAIN" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LCD" "$RPC" "$EVM_RPC"
  sep=""
  for h in "${HEADERS_IGNORE[@]}"; do
    printf '%s"%s"' "$sep" "$h"; sep=","
  done
  printf ']\n}\n'
} >"$OUT_DIR/manifest.json"

echo "Done. Fixtures at $OUT_DIR"
