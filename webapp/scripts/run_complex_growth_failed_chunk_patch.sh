#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ATLAS_STORE="${REPO_ROOT}/webapp/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-${ATLAS_STORE}/specs}"
ATLAS_SUMMARIES_DIR="${ATLAS_SUMMARIES_DIR:-${ATLAS_STORE}/summaries}"

JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_EXTRA_ARGS="${JULIA_EXTRA_ARGS:-}"
TOTAL_CPUS="${TOTAL_CPUS:-$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 16)}"
JULIA_THREADS="${JULIA_THREADS:-$(( TOTAL_CPUS > 8 ? (TOTAL_CPUS * 3) / 4 : TOTAL_CPUS ))}"
JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-${JULIA_THREADS}}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-$(( JULIA_THREADS > 4 ? JULIA_THREADS / 4 : 1 ))}"
CHUNK_SIZE="${CHUNK_SIZE:-64}"
OUTPUT_DIR="${OUTPUT_DIR:-${ATLAS_STORE}/complex_growth_failed_patch}"
RUN_DEGREES="${RUN_DEGREES:-2,3,4}"
RUN_TESTS="${RUN_TESTS:-0}"

export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export JULIA_NUM_THREADS
export ATLAS_SQLITE_LIGHTWEIGHT_PERSIST="${ATLAS_SQLITE_LIGHTWEIGHT_PERSIST:-1}"

JULIA_CMD=("${JULIA_BIN}")
if [[ -n "${JULIA_EXTRA_ARGS}" ]]; then
  # shellcheck disable=SC2206
  JULIA_EXTRA_ARGS_ARR=(${JULIA_EXTRA_ARGS})
  JULIA_CMD+=("${JULIA_EXTRA_ARGS_ARR[@]}")
fi

mkdir -p "${OUTPUT_DIR}"

if [[ "${RUN_TESTS}" == "1" ]]; then
  echo "[smoke] Running webapp test suite before patch run..."
  "${JULIA_CMD[@]}" --project="${REPO_ROOT}/webapp" "${REPO_ROOT}/webapp/test/runtests.jl"
fi

chunk_indices_for_degree() {
  local degree="$1"
  DEGREE="${degree}" ATLAS_SUMMARIES_DIR="${ATLAS_SUMMARIES_DIR}" python3 - <<'PY'
import json, os
from pathlib import Path

degree = os.environ["DEGREE"]
summary_dir = Path(os.environ.get("ATLAS_SUMMARIES_DIR", ""))
path = summary_dir / f"report_d{degree}_complex_growth_scan.final.summary.json"
if not path.exists():
    path = Path.cwd() / "webapp" / "atlas_store" / "summaries" / f"report_d{degree}_complex_growth_scan.final.summary.json"
data = json.loads(path.read_text())
failed = []
for ch in data.get("chunks", []):
    atlas_summary = ch.get("atlas_summary") or {}
    if (atlas_summary.get("failed_network_count", 0) or 0) > 0 or (atlas_summary.get("failed_slice_count", 0) or 0) > 0:
        failed.append(str(int(ch["chunk_index"])))
print(",".join(failed))
PY
}

echo "[config] REPO_ROOT=${REPO_ROOT}"
echo "[config] OUTPUT_DIR=${OUTPUT_DIR}"
echo "[config] JULIA_BIN=${JULIA_BIN}"
echo "[config] JULIA_EXTRA_ARGS=${JULIA_EXTRA_ARGS}"
echo "[config] TOTAL_CPUS=${TOTAL_CPUS}"
echo "[config] JULIA_THREADS=${JULIA_THREADS}"
echo "[config] JULIA_NUM_THREADS=${JULIA_NUM_THREADS}"
echo "[config] NETWORK_PARALLELISM=${NETWORK_PARALLELISM}"
echo "[config] CHUNK_SIZE=${CHUNK_SIZE}"
echo "[config] RUN_DEGREES=${RUN_DEGREES}"

IFS=',' read -r -a degrees <<< "${RUN_DEGREES}"

for degree in "${degrees[@]}"; do
  degree="$(echo "${degree}" | xargs)"
  [[ -z "${degree}" ]] && continue

  SPEC_PATH="${ATLAS_SPECS_DIR}/report_d${degree}_complex_growth_scan.json"
  SUMMARY_PATH="${OUTPUT_DIR}/report_d${degree}_complex_growth_failed_patch.summary.json"
  SQLITE_PATH="${OUTPUT_DIR}/report_d${degree}_complex_growth_failed_patch.sqlite"
  DEGREE_NETWORK_PARALLELISM_VAR="NETWORK_PARALLELISM_D${degree}"
  DEGREE_NETWORK_PARALLELISM="${!DEGREE_NETWORK_PARALLELISM_VAR:-${NETWORK_PARALLELISM}}"
  CHUNK_INDICES="$(chunk_indices_for_degree "${degree}")"

  if [[ -z "${CHUNK_INDICES}" ]]; then
    echo "[skip] d=${degree}: no failed chunks found in prior summary."
    continue
  fi

  SPEC_RUN_PATH="${SPEC_PATH}"
  if [[ "${DEGREE_NETWORK_PARALLELISM}" != "${NETWORK_PARALLELISM}" ]]; then
    SPEC_RUN_PATH="${OUTPUT_DIR}/report_d${degree}_complex_growth_failed_patch.spec.json"
    DEGREE="${degree}" \
    SPEC_IN="${SPEC_PATH}" \
    SPEC_OUT="${SPEC_RUN_PATH}" \
    DEGREE_NETWORK_PARALLELISM="${DEGREE_NETWORK_PARALLELISM}" \
    CHUNK_SIZE_OVERRIDE="${CHUNK_SIZE}" \
    python3 - <<'PY'
import json, os
from pathlib import Path

spec_in = Path(os.environ["SPEC_IN"])
spec_out = Path(os.environ["SPEC_OUT"])
data = json.loads(spec_in.read_text())
data["network_parallelism"] = int(os.environ["DEGREE_NETWORK_PARALLELISM"])
data["chunk_size"] = int(os.environ["CHUNK_SIZE_OVERRIDE"])
spec_out.write_text(json.dumps(data, indent=2))
PY
  fi

  echo "[run] d=${degree}"
  echo "[run] failed chunk indices=${CHUNK_INDICES}"
  echo "[run] network_parallelism=${DEGREE_NETWORK_PARALLELISM}"
  echo "[run] sqlite=${SQLITE_PATH}"

  ATLAS_SQLITE_PATH="${SQLITE_PATH}" \
  ATLAS_SOURCE_LABEL="report_d${degree}_complex_growth_failed_patch" \
  ATLAS_CHUNK_INDICES="${CHUNK_INDICES}" \
  "${JULIA_CMD[@]}" --project="${REPO_ROOT}/webapp" \
    "${REPO_ROOT}/webapp/scripts/run_atlas_scan_chunked.jl" \
    "${SPEC_RUN_PATH}" \
    "${SUMMARY_PATH}"

  echo "[done] d=${degree} summary -> ${SUMMARY_PATH}"
done

echo "[complete] failed-chunk patch run finished."
