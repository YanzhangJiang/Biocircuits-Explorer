#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ATLAS_STORE_DIR="$ROOT_DIR/webapp/atlas_store"
ATLAS_SPECS_DIR="${ATLAS_SPECS_DIR:-$ATLAS_STORE_DIR/specs}"
RAW_DIR="${RAW_DIR:-/dev/shm/complex_growth_d3_missing_patch_raw}"
OUT_DIR="${OUT_DIR:-$ATLAS_STORE_DIR/complex_growth_missing_patch_v2}"
SPEC_SRC="${SPEC_SRC:-$ATLAS_SPECS_DIR/report_d3_complex_growth_scan.json}"
SKIP_REPACK="${SKIP_REPACK:-0}"

JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_THREADS="${JULIA_THREADS:-120}"
NETWORK_PARALLELISM="${NETWORK_PARALLELISM:-120}"
ORIGINAL_CHUNK_INDICES="${ORIGINAL_CHUNK_INDICES:-18,19,20,21,22,25,26,27,29,30,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,57,58,59,60,61,62}"
REPACKED_CHUNK_SIZE="${REPACKED_CHUNK_SIZE:-120}"

mkdir -p "$RAW_DIR" "$OUT_DIR"

SPEC_TMP="$RAW_DIR/report_d3_complex_growth_missing_patch.spec.json"
SUMMARY_RAW="$RAW_DIR/report_d3_complex_growth_missing_patch.summary.json"
SQLITE_RAW="$RAW_DIR/report_d3_complex_growth_missing_patch.sqlite"
OUT_V2="$OUT_DIR/report_d3_complex_growth_missing_patch_v2.sqlite"
OUT_STATS="$OUT_DIR/report_d3_complex_growth_missing_patch_v2.stats.json"

export JULIA_NUM_THREADS="$JULIA_THREADS"
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

if [[ "$SKIP_REPACK" == "1" ]]; then
  cp "$SPEC_SRC" "$SPEC_TMP"
else
  "$JULIA_BIN" --project="$ROOT_DIR/webapp" \
    "$ROOT_DIR/webapp/scripts/build_repacked_missing_d3_spec.jl" \
    "$SPEC_SRC" \
    "$SPEC_TMP" \
    "$ORIGINAL_CHUNK_INDICES" \
    "$REPACKED_CHUNK_SIZE" \
    "$NETWORK_PARALLELISM" \
    "$SQLITE_RAW"
fi

echo "[run] spec=$SPEC_TMP"
echo "[run] original_chunk_indices=$ORIGINAL_CHUNK_INDICES"
echo "[run] repacked_chunk_size=$REPACKED_CHUNK_SIZE"
echo "[run] julia_threads=$JULIA_THREADS"
echo "[run] network_parallelism=$NETWORK_PARALLELISM"
echo "[run] raw_sqlite=$SQLITE_RAW"

"$JULIA_BIN" --project="$ROOT_DIR/webapp" \
  "$ROOT_DIR/webapp/scripts/run_atlas_scan_chunked.jl" \
  "$SPEC_TMP" \
  "$SUMMARY_RAW"

python3 "$ROOT_DIR/webapp/scripts/migrate_atlas_sqlite_v2_lossless.py" \
  --input "$SQLITE_RAW" \
  --output "$OUT_V2" \
  --stats-output "$OUT_STATS"

rm -f "$SQLITE_RAW" "$SQLITE_RAW-wal" "$SQLITE_RAW-shm"

echo "[done] summary=$SUMMARY_RAW"
echo "[done] v2=$OUT_V2"
echo "[done] stats=$OUT_STATS"
