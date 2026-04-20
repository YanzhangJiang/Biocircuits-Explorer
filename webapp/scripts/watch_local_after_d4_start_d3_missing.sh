#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
D4_PID_FILE="${D4_PID_FILE:-$ROOT_DIR/webapp/atlas_store/complex_growth_d4_local_v2/run.pid}"
D3_RUN_SCRIPT="${D3_RUN_SCRIPT:-$ROOT_DIR/webapp/scripts/run_d3_complex_growth_missing_local_v2.sh}"
D3_RUN_LOG="${D3_RUN_LOG:-$ROOT_DIR/webapp/atlas_store/complex_growth_d3_missing_local_v2/run.log}"
POLL_SECONDS="${POLL_SECONDS:-30}"

mkdir -p "$(dirname "$D3_RUN_LOG")"

if [[ ! -f "$D4_PID_FILE" ]]; then
  echo "Missing d4 pid file: $D4_PID_FILE" >&2
  exit 1
fi

d4_pid="$(tr -d '[:space:]' < "$D4_PID_FILE")"
if [[ -z "$d4_pid" ]]; then
  echo "Empty d4 pid file: $D4_PID_FILE" >&2
  exit 1
fi

echo "[watch] waiting for d4 pid $d4_pid to exit"
while kill -0 "$d4_pid" >/dev/null 2>&1; do
  sleep "$POLL_SECONDS"
done

echo "[watch] d4 finished; starting d3 missing-part run"
RUN_SCAN=1 "$D3_RUN_SCRIPT" >>"$D3_RUN_LOG" 2>&1
echo "[watch] d3 missing-part run completed"
