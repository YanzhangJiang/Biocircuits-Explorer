#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/Users/yanzhang/git/Biocircuits-Explorer"
WEBAPP="$REPO_ROOT/webapp"
RUNNER="$WEBAPP/scripts/run_complex_growth_campaign.py"

WAIT_FOR_PID=""
EXTRA_ARGS=()

while (($# > 0)); do
  case "$1" in
    --wait-pid)
      WAIT_FOR_PID="${2:?missing pid for --wait-pid}"
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

wait_for_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  while kill -0 "$pid" >/dev/null 2>&1; do
    sleep 60
  done
}

wait_for_pid "$WAIT_FOR_PID"

JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-18}" \
python3 "$RUNNER" "${EXTRA_ARGS[@]}"
