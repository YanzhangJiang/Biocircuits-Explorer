#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ATLAS_STORE="${REPO_ROOT}/webapp/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-${ATLAS_STORE}/specs}"

JULIA_BIN="${JULIA_BIN:-/root/.local/julia/julia-1.12.5/bin/julia}"
JULIA_EXTRA_ARGS="${JULIA_EXTRA_ARGS:---compiled-modules=existing --pkgimages=existing}"
JULIA_THREADS="${JULIA_THREADS:-120}"
JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-120}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-64}"
CHUNK_SIZE="${CHUNK_SIZE:-64}"

RAW_OUTPUT_DIR="${RAW_OUTPUT_DIR:-/mnt/complex_growth_recovery_raw}"
V2_OUTPUT_DIR="${V2_OUTPUT_DIR:-/mnt/complex_growth_recovery_v2}"

export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export JULIA_NUM_THREADS
export ATLAS_SQLITE_LIGHTWEIGHT_PERSIST="${ATLAS_SQLITE_LIGHTWEIGHT_PERSIST:-1}"

mkdir -p "${RAW_OUTPUT_DIR}" "${V2_OUTPUT_DIR}"

# Current recovery target: recompute the non-patch chunk complements for d=2 and d=3.
D2_NONPATCH_CHUNKS="${D2_NONPATCH_CHUNKS:-1,2,4,5,6,7,8,9,10,11,13,22,24,25,26}"
D3_NONPATCH_CHUNKS="${D3_NONPATCH_CHUNKS:-5,23,24,28,31,56}"

run_degree() {
  local degree="$1"
  local chunk_indices="$2"

  local spec_path="${ATLAS_SPECS_DIR}/report_d${degree}_complex_growth_scan.json"
  local spec_run_path="${RAW_OUTPUT_DIR}/report_d${degree}_complex_growth_nonpatch.spec.json"
  local summary_path="${RAW_OUTPUT_DIR}/report_d${degree}_complex_growth_nonpatch.summary.json"
  local sqlite_path="${RAW_OUTPUT_DIR}/report_d${degree}_complex_growth_nonpatch.sqlite"
  local v2_path="${V2_OUTPUT_DIR}/report_d${degree}_complex_growth_nonpatch_v2.sqlite"
  local v2_stats_path="${V2_OUTPUT_DIR}/report_d${degree}_complex_growth_nonpatch_v2.stats.json"

  rm -f "${sqlite_path}" "${sqlite_path}-shm" "${sqlite_path}-wal" "${summary_path}" "${spec_run_path}"
  rm -f "${v2_path}" "${v2_path}-shm" "${v2_path}-wal" "${v2_stats_path}"

  SPEC_IN="${spec_path}" SPEC_OUT="${spec_run_path}" NETWORK_PARALLELISM_OVERRIDE="${NETWORK_PARALLELISM}" CHUNK_SIZE_OVERRIDE="${CHUNK_SIZE}" python3 - <<'PY'
import json, os
from pathlib import Path

spec_in = Path(os.environ["SPEC_IN"])
spec_out = Path(os.environ["SPEC_OUT"])
data = json.loads(spec_in.read_text())
data["network_parallelism"] = int(os.environ["NETWORK_PARALLELISM_OVERRIDE"])
data["chunk_size"] = int(os.environ["CHUNK_SIZE_OVERRIDE"])
spec_out.write_text(json.dumps(data, indent=2))
PY

  echo "[run] d=${degree} non-patch chunks=${chunk_indices}"

  ATLAS_SQLITE_PATH="${sqlite_path}" \
  ATLAS_SOURCE_LABEL="report_d${degree}_complex_growth_nonpatch" \
  ATLAS_CHUNK_INDICES="${chunk_indices}" \
  "${JULIA_BIN}" ${JULIA_EXTRA_ARGS} --project="${REPO_ROOT}/webapp" \
    "${REPO_ROOT}/webapp/scripts/run_atlas_scan_chunked.jl" \
    "${spec_run_path}" \
    "${summary_path}"

  echo "[migrate] d=${degree} -> v2"
  python3 "${REPO_ROOT}/webapp/scripts/migrate_atlas_sqlite_v2_lossless.py" \
    --src-db "${sqlite_path}" \
    --dst-db "${v2_path}" | tee "${v2_stats_path}"

  rm -f "${sqlite_path}" "${sqlite_path}-shm" "${sqlite_path}-wal"
}

echo "[config] REPO_ROOT=${REPO_ROOT}"
echo "[config] JULIA_BIN=${JULIA_BIN}"
echo "[config] JULIA_THREADS=${JULIA_THREADS}"
echo "[config] JULIA_NUM_THREADS=${JULIA_NUM_THREADS}"
echo "[config] NETWORK_PARALLELISM=${NETWORK_PARALLELISM}"
echo "[config] CHUNK_SIZE=${CHUNK_SIZE}"
echo "[config] RAW_OUTPUT_DIR=${RAW_OUTPUT_DIR}"
echo "[config] V2_OUTPUT_DIR=${V2_OUTPUT_DIR}"

run_degree 2 "${D2_NONPATCH_CHUNKS}"
run_degree 3 "${D3_NONPATCH_CHUNKS}"

echo "[complete] d2/d3 non-patch recovery finished."
