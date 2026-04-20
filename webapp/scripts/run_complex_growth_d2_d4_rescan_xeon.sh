#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$WEBAPP/.." && pwd)"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$WEBAPP/atlas_store/specs}"
ATLAS_LIBRARIES_DIR="${ATLAS_LIBRARIES_DIR:-$WEBAPP/atlas_store/libraries}"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

# Aggressive defaults for a dedicated 128 vCPU Xeon host. We still keep
# network_parallelism below the thread count so each active network can use a
# few Julia threads internally, but this profile is intended to drive the
# machine much harder than the conservative laptop/workstation preset.
JULIA_THREADS="${JULIA_THREADS:-96}"
OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-24}"
CHUNK_SIZE="${CHUNK_SIZE:-64}"

SQLITE_PATH="${SQLITE_PATH:-$ATLAS_LIBRARIES_DIR/atlas_extended_assembly_campaign.sqlite}"
OUTPUT_DIR="${OUTPUT_DIR:-$WEBAPP/atlas_store/rescan_complex_growth_d2_d4}"
RUN_TESTS="${RUN_TESTS:-1}"
REFRESH_REPORTS="${REFRESH_REPORTS:-1}"

mkdir -p "$OUTPUT_DIR"

if [[ ! -f "$SQLITE_PATH" ]]; then
  echo "Expected existing sqlite at: $SQLITE_PATH" >&2
  echo "Copy the previous atlas_extended_assembly_campaign.sqlite into $ATLAS_LIBRARIES_DIR first." >&2
  exit 1
fi

export JULIA_NUM_THREADS="$JULIA_THREADS"
export OPENBLAS_NUM_THREADS
export OMP_NUM_THREADS
export MKL_NUM_THREADS
export ATLAS_SQLITE_PATH="$SQLITE_PATH"

echo "== Environment =="
echo "REPO_ROOT=$REPO_ROOT"
echo "WEBAPP=$WEBAPP"
echo "JULIA_BIN=$JULIA_BIN"
echo "JULIA_NUM_THREADS=$JULIA_NUM_THREADS"
echo "NETWORK_PARALLELISM=$NETWORK_PARALLELISM"
echo "CHUNK_SIZE=$CHUNK_SIZE"
echo "OPENBLAS_NUM_THREADS=$OPENBLAS_NUM_THREADS"
echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
echo "MKL_NUM_THREADS=$MKL_NUM_THREADS"
echo "SQLITE_PATH=$SQLITE_PATH"
echo "OUTPUT_DIR=$OUTPUT_DIR"
echo

run_scan() {
  local degree="$1"
  local spec_path="$ATLAS_SPECS_DIR/report_d${degree}_complex_growth_scan.json"
  local patched_spec_path="$OUTPUT_DIR/report_d${degree}_complex_growth_rescan.spec.json"
  local summary_path="$OUTPUT_DIR/report_d${degree}_complex_growth_rescan.summary.json"

  if [[ ! -f "$spec_path" ]]; then
    echo "Missing spec: $spec_path" >&2
    exit 1
  fi

  python3 - "$spec_path" "$patched_spec_path" "$NETWORK_PARALLELISM" "$CHUNK_SIZE" <<'PY'
import json, sys
src, dst, np_raw, chunk_raw = sys.argv[1:]
raw = json.loads(open(src, "r", encoding="utf-8").read())
raw["network_parallelism"] = int(np_raw)
raw["chunk_size"] = int(chunk_raw)
raw["persist_sqlite"] = True
raw["skip_existing"] = True
with open(dst, "w", encoding="utf-8") as fh:
    json.dump(raw, fh, indent=2)
    fh.write("\n")
PY

  echo "== Running complex_growth d=${degree} =="
  "$JULIA_BIN" --project="$WEBAPP" "$WEBAPP/scripts/run_atlas_scan_chunked.jl" \
    "$patched_spec_path" \
    "$summary_path"
  echo "Completed d=${degree}; summary -> $summary_path"
  echo
}

if [[ "$RUN_TESTS" == "1" ]]; then
  echo "== Running webapp test suite =="
  "$JULIA_BIN" --project="$WEBAPP" -e 'include("webapp/test/runtests.jl")'
  echo
fi

run_scan 2
run_scan 3
run_scan 4

if [[ "$REFRESH_REPORTS" == "1" ]]; then
  echo "== Refreshing reports =="
  python3 "$REPO_ROOT/doc/generate_extended_assembly_campaign_report.py"
  python3 "$REPO_ROOT/doc/discover_extended_assembly_examples.py"
  echo
fi

echo "All requested complex-growth rescans completed."
