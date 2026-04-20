#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$WEBAPP_DIR/.." && pwd)"
ATLAS_STORE_DIR="$WEBAPP_DIR/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$ATLAS_STORE_DIR/specs}"

JULIA_BIN="${JULIA_BIN:-${JULIA:-$(command -v julia || true)}}"
if [[ -z "$JULIA_BIN" ]]; then
  echo "Unable to find Julia. Set JULIA_BIN or JULIA first." >&2
  exit 1
fi

SPEC_SRC="${SPEC_SRC:-$ATLAS_SPECS_DIR/report_d3_complex_growth_scan.json}"
RUN_ROOT="${RUN_ROOT:-$ATLAS_STORE_DIR/complex_growth_d3_missing_local_v2}"
RAW_DIR="${RAW_DIR:-$RUN_ROOT/raw}"
OUT_DIR="${OUT_DIR:-$RUN_ROOT/v2}"

RUN_SCAN="${RUN_SCAN:-0}"
RUN_MIGRATE="${RUN_MIGRATE:-1}"
ATLAS_SQLITE_LIGHTWEIGHT_PERSIST="${ATLAS_SQLITE_LIGHTWEIGHT_PERSIST:-0}"

JULIA_THREADS="${JULIA_THREADS:-16}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-6}"
CHUNK_MULTIPLIER="${CHUNK_MULTIPLIER:-8}"
REPACKED_CHUNK_SIZE="${REPACKED_CHUNK_SIZE:-$(( NETWORK_PARALLELISM * CHUNK_MULTIPLIER ))}"
ORIGINAL_CHUNK_INDICES="${ORIGINAL_CHUNK_INDICES:-18,19,20,21,22,25,26,27,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,57,58,59,60,61,62}"

OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

SPEC_TMP="$RUN_ROOT/report_d3_complex_growth_missing_local.spec.json"
SUMMARY_RAW="$RAW_DIR/report_d3_complex_growth_missing_local.summary.json"
SQLITE_RAW="$RAW_DIR/report_d3_complex_growth_missing_local.sqlite"
SQLITE_V2="$OUT_DIR/report_d3_complex_growth_missing_local_v2.sqlite"
STATS_V2="$OUT_DIR/report_d3_complex_growth_missing_local_v2.stats.json"

mkdir -p "$RUN_ROOT" "$RAW_DIR" "$OUT_DIR"

export JULIA_NUM_THREADS="$JULIA_THREADS"
export OPENBLAS_NUM_THREADS
export OMP_NUM_THREADS
export MKL_NUM_THREADS
export ATLAS_SQLITE_LIGHTWEIGHT_PERSIST

"$JULIA_BIN" --project="$WEBAPP_DIR" \
  "$WEBAPP_DIR/scripts/build_repacked_missing_d3_spec.jl" \
  "$SPEC_SRC" \
  "$SPEC_TMP" \
  "$ORIGINAL_CHUNK_INDICES" \
  "$REPACKED_CHUNK_SIZE" \
  "$NETWORK_PARALLELISM" \
  "$SQLITE_RAW" \
  >/dev/null

echo "== Prepared Local d3 Missing Complex-Growth Run =="
echo "ROOT_DIR=$ROOT_DIR"
echo "WEBAPP_DIR=$WEBAPP_DIR"
echo "JULIA_BIN=$JULIA_BIN"
echo "SPEC_SRC=$SPEC_SRC"
echo "SPEC_TMP=$SPEC_TMP"
echo "SUMMARY_RAW=$SUMMARY_RAW"
echo "SQLITE_RAW=$SQLITE_RAW"
echo "SQLITE_V2=$SQLITE_V2"
echo "STATS_V2=$STATS_V2"
echo "JULIA_NUM_THREADS=$JULIA_NUM_THREADS"
echo "NETWORK_PARALLELISM=$NETWORK_PARALLELISM"
echo "CHUNK_MULTIPLIER=$CHUNK_MULTIPLIER"
echo "REPACKED_CHUNK_SIZE=$REPACKED_CHUNK_SIZE"
echo "ORIGINAL_CHUNK_INDICES=$ORIGINAL_CHUNK_INDICES"
echo "ATLAS_SQLITE_LIGHTWEIGHT_PERSIST=$ATLAS_SQLITE_LIGHTWEIGHT_PERSIST"
echo

if [[ "$RUN_SCAN" != "1" ]]; then
  echo "Preparation only. To run later:"
  echo "  RUN_SCAN=1 \"$0\""
  exit 0
fi

echo "== Running d3 complex-growth missing-part atlas scan =="
"$JULIA_BIN" --project="$WEBAPP_DIR" \
  "$WEBAPP_DIR/scripts/run_atlas_scan_chunked.jl" \
  "$SPEC_TMP" \
  "$SUMMARY_RAW"

if [[ "$RUN_MIGRATE" == "1" ]]; then
  echo
  echo "== Migrating raw sqlite to v2 lossless format =="
  python3 "$WEBAPP_DIR/scripts/migrate_atlas_sqlite_v2_lossless.py" \
    --src-db "$SQLITE_RAW" \
    --dst-db "$SQLITE_V2" \
    > "$STATS_V2"
fi

echo
echo "Completed local d3 complex-growth missing-part run."
