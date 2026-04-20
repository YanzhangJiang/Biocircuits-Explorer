#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$WEBAPP_DIR/.." && pwd)"
ATLAS_STORE_DIR="$WEBAPP_DIR/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$ATLAS_STORE_DIR/specs}"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

SPEC_SRC="${SPEC_SRC:-$ATLAS_SPECS_DIR/report_d4_complex_growth_scan.json}"
RUN_ROOT="${RUN_ROOT:-$ATLAS_STORE_DIR/complex_growth_d4_local_v2}"
RAW_DIR="${RAW_DIR:-$RUN_ROOT/raw}"
OUT_DIR="${OUT_DIR:-$RUN_ROOT/v2}"

RUN_SCAN="${RUN_SCAN:-0}"
RUN_MIGRATE="${RUN_MIGRATE:-1}"
ATLAS_SQLITE_LIGHTWEIGHT_PERSIST="${ATLAS_SQLITE_LIGHTWEIGHT_PERSIST:-0}"

JULIA_THREADS="${JULIA_THREADS:-16}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-16}"
CHUNK_MULTIPLIER="${CHUNK_MULTIPLIER:-8}"
CHUNK_SIZE="${CHUNK_SIZE:-$(( NETWORK_PARALLELISM * CHUNK_MULTIPLIER ))}"

OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

SPEC_OUT="$RUN_ROOT/report_d4_complex_growth_local.spec.json"
SUMMARY_OUT="$RAW_DIR/report_d4_complex_growth_local.summary.json"
SQLITE_RAW="$RAW_DIR/report_d4_complex_growth_local.sqlite"
SQLITE_V2="$OUT_DIR/report_d4_complex_growth_local_v2.sqlite"
STATS_V2="$OUT_DIR/report_d4_complex_growth_local_v2.stats.json"

mkdir -p "$RUN_ROOT" "$RAW_DIR" "$OUT_DIR"

export JULIA_NUM_THREADS="$JULIA_THREADS"
export OPENBLAS_NUM_THREADS
export OMP_NUM_THREADS
export MKL_NUM_THREADS
export ATLAS_SQLITE_LIGHTWEIGHT_PERSIST

python3 - "$SPEC_SRC" "$SPEC_OUT" "$JULIA_THREADS" "$NETWORK_PARALLELISM" "$CHUNK_SIZE" "$SQLITE_RAW" <<'PY'
import json
import sys

julia_threads_raw, parallelism_raw, chunk_raw = None, None, None
src, dst, julia_threads_raw, parallelism_raw, chunk_raw, sqlite_path = sys.argv[1:]
julia_threads = int(julia_threads_raw)
parallelism = int(parallelism_raw)
chunk_size = int(chunk_raw)

with open(src, "r", encoding="utf-8") as fh:
    raw = json.load(fh)

raw["network_parallelism"] = parallelism
raw["chunk_size"] = chunk_size
raw["persist_sqlite"] = True
raw["skip_existing"] = False
raw["sqlite_path"] = sqlite_path
raw["source_label"] = "report_d4_complex_growth_local"
raw.setdefault("source_metadata", {})
raw["source_metadata"].update(
    {
        "prepared_by": "run_d4_complex_growth_local_v2.sh",
        "julia_threads": julia_threads,
        "network_parallelism": parallelism,
        "chunk_multiplier": chunk_size // parallelism if parallelism else None,
        "chunk_size": chunk_size,
    }
)

with open(dst, "w", encoding="utf-8") as fh:
    json.dump(raw, fh, indent=2)
    fh.write("\n")
PY

echo "== Prepared Local d4 Complex-Growth Run =="
echo "REPO_ROOT=$REPO_ROOT"
echo "WEBAPP_DIR=$WEBAPP_DIR"
echo "JULIA_BIN=$JULIA_BIN"
echo "SPEC_SRC=$SPEC_SRC"
echo "SPEC_OUT=$SPEC_OUT"
echo "SUMMARY_OUT=$SUMMARY_OUT"
echo "SQLITE_RAW=$SQLITE_RAW"
echo "SQLITE_V2=$SQLITE_V2"
echo "STATS_V2=$STATS_V2"
echo "JULIA_NUM_THREADS=$JULIA_NUM_THREADS"
echo "NETWORK_PARALLELISM=$NETWORK_PARALLELISM"
echo "CHUNK_MULTIPLIER=$CHUNK_MULTIPLIER"
echo "CHUNK_SIZE=$CHUNK_SIZE"
echo "ATLAS_SQLITE_LIGHTWEIGHT_PERSIST=$ATLAS_SQLITE_LIGHTWEIGHT_PERSIST"
echo

if [[ "$RUN_SCAN" != "1" ]]; then
  echo "Preparation only. To run later:"
  echo "  RUN_SCAN=1 \"$0\""
  exit 0
fi

echo "== Running d4 complex-growth atlas scan =="
"$JULIA_BIN" --project="$WEBAPP_DIR" \
  "$WEBAPP_DIR/scripts/run_atlas_scan_chunked.jl" \
  "$SPEC_OUT" \
  "$SUMMARY_OUT"

if [[ "$RUN_MIGRATE" == "1" ]]; then
  echo
  echo "== Migrating raw sqlite to v2 lossless format =="
  python3 "$WEBAPP_DIR/scripts/migrate_atlas_sqlite_v2_lossless.py" \
    --src-db "$SQLITE_RAW" \
    --dst-db "$SQLITE_V2" \
    > "$STATS_V2"
fi

echo
echo "Completed local d4 complex-growth run."
