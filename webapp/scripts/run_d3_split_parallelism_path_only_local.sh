#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${WEBAPP_DIR}/.." && pwd)"
RUNNER="${WEBAPP_DIR}/scripts/run_degree_complete_local_v2.py"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  LOCAL_JULIA="$(find "${HOME}/.local/julia" -maxdepth 3 -type f -name julia 2>/dev/null | sort | tail -n 1 || true)"
  if [[ -n "$LOCAL_JULIA" ]]; then
    JULIA_BIN="$LOCAL_JULIA"
  fi
fi
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

DEGREE="${DEGREE:-3}"
if [[ "$DEGREE" != "3" ]]; then
  echo "This wrapper is only intended for d=3." >&2
  exit 1
fi

RUN_ROOT="${RUN_ROOT:-${WEBAPP_DIR}/atlas_store/by_degree_logqk_m6_p6/d3}"
SCAN_MODE="${SCAN_MODE:-streaming}"
PHASE="${PHASE:-run}"
LOGQK_MIN="${LOGQK_MIN:--6}"
LOGQK_MAX="${LOGQK_MAX:-6}"

NON_COMPLEX_FAMILIES="${NON_COMPLEX_FAMILIES:-orthant,higher_order,homomer,homomer4plus}"
COMPLEX_GROWTH_FAMILIES="${COMPLEX_GROWTH_FAMILIES:-complex_growth}"
ALL_FAMILIES="${ALL_FAMILIES:-${NON_COMPLEX_FAMILIES},${COMPLEX_GROWTH_FAMILIES}}"

NON_COMPLEX_THREADS="${NON_COMPLEX_THREADS:-128}"
NON_COMPLEX_PARALLELISM="${NON_COMPLEX_PARALLELISM:-128}"
NON_COMPLEX_FLUSH_NETWORK_COUNT="${NON_COMPLEX_FLUSH_NETWORK_COUNT:-8}"

COMPLEX_GROWTH_THREADS="${COMPLEX_GROWTH_THREADS:-16}"
COMPLEX_GROWTH_PARALLELISM="${COMPLEX_GROWTH_PARALLELISM:-16}"
COMPLEX_GROWTH_FLUSH_NETWORK_COUNT="${COMPLEX_GROWTH_FLUSH_NETWORK_COUNT:-8}"

RESET_RUN_ROOT="${RESET_RUN_ROOT:-0}"
BACKUP_LABEL="${BACKUP_LABEL:-corrupt_backup}"

OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

RUN_META_PATH="${RUN_ROOT}/meta/report_d3_split_parallelism.run.meta.json"
PATH_ONLY_DB="${RUN_ROOT}/path_only/report_d3_complete_path_only.sqlite"
STARTED_AT="$(date --iso-8601=seconds)"
CURRENT_STAGE="bootstrap"
BACKUP_RUN_ROOT=""

export OPENBLAS_NUM_THREADS
export OMP_NUM_THREADS
export MKL_NUM_THREADS
export JULIA_BIN
export RUN_ROOT
export RUN_META_PATH
export PATH_ONLY_DB
export SCAN_MODE
export LOGQK_MIN
export LOGQK_MAX
export NON_COMPLEX_FAMILIES
export COMPLEX_GROWTH_FAMILIES
export ALL_FAMILIES
export NON_COMPLEX_THREADS
export NON_COMPLEX_PARALLELISM
export NON_COMPLEX_FLUSH_NETWORK_COUNT
export COMPLEX_GROWTH_THREADS
export COMPLEX_GROWTH_PARALLELISM
export COMPLEX_GROWTH_FLUSH_NETWORK_COUNT
export STARTED_AT
export RESET_RUN_ROOT

write_run_meta() {
  local status="$1"
  local stage="$2"
  local finished_at="${3:-}"
  META_STATUS="$status" META_STAGE="$stage" META_FINISHED_AT="$finished_at" python3 - "$RUN_META_PATH" <<'PY'
import json
import os
from pathlib import Path

run_root = Path(os.environ["RUN_ROOT"])
meta_path = Path(os.environ["RUN_META_PATH"])
meta_path.parent.mkdir(parents=True, exist_ok=True)
payload = {}
if meta_path.exists():
    payload = json.loads(meta_path.read_text(encoding="utf-8"))

families = [item for item in os.environ["ALL_FAMILIES"].split(",") if item]
summary_dir = run_root / "meta" / "summaries"
family_results = []
for family in families:
    summary_path = summary_dir / f"report_d3_{family}_complete_local.run.summary.json"
    entry = {
        "family": family,
        "summary_path": str(summary_path),
        "exists": summary_path.exists(),
    }
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        entry.update(
            {
                "status": summary.get("status"),
                "finished_at": summary.get("finished_at"),
                "completed_network_count": summary.get("completed_network_count"),
                "total_network_count": summary.get("total_network_count"),
                "completed_chunk_count": summary.get("completed_chunk_count"),
                "total_chunk_count": summary.get("total_chunk_count"),
                "path_record_count": summary.get("path_record_count"),
                "behavior_slice_count": summary.get("behavior_slice_count"),
                "failed_network_count": summary.get("failed_network_count"),
            }
        )
    family_results.append(entry)

payload.update(
    {
        "degree": 3,
        "run_root": os.environ["RUN_ROOT"],
        "path_only_db": os.environ["PATH_ONLY_DB"],
        "logqk_min": float(os.environ["LOGQK_MIN"]),
        "logqk_max": float(os.environ["LOGQK_MAX"]),
        "scan_mode": os.environ["SCAN_MODE"],
        "family_parallelism": {
            "orthant": int(os.environ["NON_COMPLEX_PARALLELISM"]),
            "higher_order": int(os.environ["NON_COMPLEX_PARALLELISM"]),
            "homomer": int(os.environ["NON_COMPLEX_PARALLELISM"]),
            "homomer4plus": int(os.environ["NON_COMPLEX_PARALLELISM"]),
            "complex_growth": int(os.environ["COMPLEX_GROWTH_PARALLELISM"]),
        },
        "family_threads": {
            "orthant": int(os.environ["NON_COMPLEX_THREADS"]),
            "higher_order": int(os.environ["NON_COMPLEX_THREADS"]),
            "homomer": int(os.environ["NON_COMPLEX_THREADS"]),
            "homomer4plus": int(os.environ["NON_COMPLEX_THREADS"]),
            "complex_growth": int(os.environ["COMPLEX_GROWTH_THREADS"]),
        },
        "non_complex_families": [item for item in os.environ["NON_COMPLEX_FAMILIES"].split(",") if item],
        "complex_growth_families": [item for item in os.environ["COMPLEX_GROWTH_FAMILIES"].split(",") if item],
        "non_complex_threads": int(os.environ["NON_COMPLEX_THREADS"]),
        "complex_growth_threads": int(os.environ["COMPLEX_GROWTH_THREADS"]),
        "non_complex_parallelism": int(os.environ["NON_COMPLEX_PARALLELISM"]),
        "complex_growth_parallelism": int(os.environ["COMPLEX_GROWTH_PARALLELISM"]),
        "non_complex_flush_network_count": int(os.environ["NON_COMPLEX_FLUSH_NETWORK_COUNT"]),
        "complex_growth_flush_network_count": int(os.environ["COMPLEX_GROWTH_FLUSH_NETWORK_COUNT"]),
        "started_at": os.environ["STARTED_AT"],
        "status": os.environ["META_STATUS"],
        "stage": os.environ["META_STAGE"],
        "reset_run_root": os.environ["RESET_RUN_ROOT"] == "1",
        "julia_bin": os.environ["JULIA_BIN"],
        "family_results": family_results,
    }
)

backup_run_root = os.environ.get("BACKUP_RUN_ROOT", "").strip()
if backup_run_root:
    payload["backup_run_root"] = backup_run_root
elif "backup_run_root" in payload:
    payload.pop("backup_run_root", None)

finished_at = os.environ.get("META_FINISHED_AT", "").strip()
if finished_at:
    payload["finished_at"] = finished_at
elif payload.get("status") != "completed":
    payload.pop("finished_at", None)

meta_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
}

on_error() {
  local exit_code="$?"
  write_run_meta "failed" "$CURRENT_STAGE"
  exit "$exit_code"
}
trap on_error ERR

if [[ "$RESET_RUN_ROOT" == "1" && -e "$RUN_ROOT" ]]; then
  backup_stamp="$(date +%Y%m%d_%H%M%S)"
  BACKUP_RUN_ROOT="${RUN_ROOT}.${BACKUP_LABEL}_${backup_stamp}"
  mv "$RUN_ROOT" "$BACKUP_RUN_ROOT"
fi

mkdir -p "$RUN_ROOT"

echo "== d3 split-parallelism path-only rerun =="
echo "REPO_ROOT=$REPO_ROOT"
echo "WEBAPP_DIR=$WEBAPP_DIR"
echo "RUN_ROOT=$RUN_ROOT"
echo "RUN_META_PATH=$RUN_META_PATH"
echo "PATH_ONLY_DB=$PATH_ONLY_DB"
echo "RESET_RUN_ROOT=$RESET_RUN_ROOT"
if [[ -n "$BACKUP_RUN_ROOT" ]]; then
  echo "BACKUP_RUN_ROOT=$BACKUP_RUN_ROOT"
fi
echo "JULIA_BIN=$JULIA_BIN"
echo "SCAN_MODE=$SCAN_MODE"
echo "PHASE=$PHASE"
echo "LOGQK_MIN=$LOGQK_MIN"
echo "LOGQK_MAX=$LOGQK_MAX"
echo "NON_COMPLEX_FAMILIES=$NON_COMPLEX_FAMILIES"
echo "NON_COMPLEX_THREADS=$NON_COMPLEX_THREADS"
echo "NON_COMPLEX_PARALLELISM=$NON_COMPLEX_PARALLELISM"
echo "NON_COMPLEX_FLUSH_NETWORK_COUNT=$NON_COMPLEX_FLUSH_NETWORK_COUNT"
echo "COMPLEX_GROWTH_FAMILIES=$COMPLEX_GROWTH_FAMILIES"
echo "COMPLEX_GROWTH_THREADS=$COMPLEX_GROWTH_THREADS"
echo "COMPLEX_GROWTH_PARALLELISM=$COMPLEX_GROWTH_PARALLELISM"
echo "COMPLEX_GROWTH_FLUSH_NETWORK_COUNT=$COMPLEX_GROWTH_FLUSH_NETWORK_COUNT"
echo "OPENBLAS_NUM_THREADS=$OPENBLAS_NUM_THREADS"
echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
echo "MKL_NUM_THREADS=$MKL_NUM_THREADS"
echo

write_run_meta "running" "$CURRENT_STAGE"

CURRENT_STAGE="non_complex"
write_run_meta "running" "$CURRENT_STAGE"
python3 "$RUNNER" \
  --degree "$DEGREE" \
  --phase "$PHASE" \
  --run-root "$RUN_ROOT" \
  --scan-mode "$SCAN_MODE" \
  --network-parallelism "$NON_COMPLEX_PARALLELISM" \
  --julia-threads "$NON_COMPLEX_THREADS" \
  --flush-network-count "$NON_COMPLEX_FLUSH_NETWORK_COUNT" \
  --logqk-min "$LOGQK_MIN" \
  --logqk-max "$LOGQK_MAX" \
  --families "$NON_COMPLEX_FAMILIES" \
  --julia-bin "$JULIA_BIN"

CURRENT_STAGE="complex_growth"
write_run_meta "running" "$CURRENT_STAGE"
python3 "$RUNNER" \
  --degree "$DEGREE" \
  --phase "$PHASE" \
  --run-root "$RUN_ROOT" \
  --scan-mode "$SCAN_MODE" \
  --network-parallelism "$COMPLEX_GROWTH_PARALLELISM" \
  --julia-threads "$COMPLEX_GROWTH_THREADS" \
  --flush-network-count "$COMPLEX_GROWTH_FLUSH_NETWORK_COUNT" \
  --logqk-min "$LOGQK_MIN" \
  --logqk-max "$LOGQK_MAX" \
  --families "$COMPLEX_GROWTH_FAMILIES" \
  --julia-bin "$JULIA_BIN"

CURRENT_STAGE="completed"
write_run_meta "completed" "$CURRENT_STAGE" "$(date --iso-8601=seconds)"
trap - ERR

echo
echo "Completed d3 split-parallelism rerun."
