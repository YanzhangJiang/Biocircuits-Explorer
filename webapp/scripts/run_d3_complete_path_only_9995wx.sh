#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${WEBAPP_DIR}/.." && pwd)"
RUNNER="${WEBAPP_DIR}/scripts/run_degree_complete_local_v2.py"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

RUN_ROOT="${RUN_ROOT:-${WEBAPP_DIR}/atlas_store/by_degree/d3_9995wx_streaming}"
PHASE="${PHASE:-run}"
JULIA_THREADS="${JULIA_THREADS:-192}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-192}"
FLUSH_NETWORK_COUNT="${FLUSH_NETWORK_COUNT:-8}"

OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

TOTAL_THREADS="$(nproc --all 2>/dev/null || getconf _NPROCESSORS_ONLN || echo unknown)"
THREADS_PER_CORE="$(lscpu 2>/dev/null | awk -F: '/^Thread\\(s\\) per core:/ {gsub(/ /, "", $2); print $2; exit}')"
PHYSICAL_CORES="$(lscpu 2>/dev/null | awk -F: '/^CPU\\(s\\):/ {gsub(/ /, "", $2); cpus=$2} /^Thread\\(s\\) per core:/ {gsub(/ /, "", $2); tpc=$2} END {if (cpus && tpc) print int(cpus / tpc)}')"
MODEL_NAME="$(lscpu 2>/dev/null | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')"

export JULIA_THREADS
export JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-${JULIA_THREADS}}"
export NETWORK_PARALLELISM
export OPENBLAS_NUM_THREADS
export OMP_NUM_THREADS
export MKL_NUM_THREADS

echo "== d3 path-only 9995WX streaming run =="
echo "REPO_ROOT=$REPO_ROOT"
echo "WEBAPP_DIR=$WEBAPP_DIR"
echo "RUN_ROOT=$RUN_ROOT"
echo "PHASE=$PHASE"
echo "JULIA_BIN=$JULIA_BIN"
echo "MODEL_NAME=${MODEL_NAME:-unknown}"
echo "TOTAL_THREADS=${TOTAL_THREADS}"
echo "PHYSICAL_CORES=${PHYSICAL_CORES:-unknown}"
echo "THREADS_PER_CORE=${THREADS_PER_CORE:-unknown}"
echo "JULIA_NUM_THREADS=$JULIA_NUM_THREADS"
echo "NETWORK_PARALLELISM=$NETWORK_PARALLELISM"
echo "FLUSH_NETWORK_COUNT=$FLUSH_NETWORK_COUNT"
echo "OPENBLAS_NUM_THREADS=$OPENBLAS_NUM_THREADS"
echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
echo "MKL_NUM_THREADS=$MKL_NUM_THREADS"
echo

python3 "$RUNNER" \
  --degree 3 \
  --phase "$PHASE" \
  --run-root "$RUN_ROOT" \
  --scan-mode streaming \
  --network-parallelism "$NETWORK_PARALLELISM" \
  --julia-threads "$JULIA_NUM_THREADS" \
  --flush-network-count "$FLUSH_NETWORK_COUNT" \
  --julia-bin "$JULIA_BIN"
