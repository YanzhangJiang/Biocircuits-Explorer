#!/usr/bin/env bash
# Merge independent atlas shard SQLite files produced by atlas_build_shard.sbatch.
# Run after the shard array succeeds.
#
# Submit example:
#   sbatch --dependency=afterok:$AID \
#          --export=ALL,BNC_ROOT=$PWD,SHARD_COUNT=8 \
#          slurm/atlas_merge_shards.sbatch

#SBATCH --job-name=bne-atlas-merge
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=12:00:00
#SBATCH --output=logs/atlas_merge_%j.out
#SBATCH --error=logs/atlas_merge_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"

SHARD_COUNT="${SHARD_COUNT:-8}"
FINAL_ATLAS_PATH="${ATLAS_PATH:-$BNC_ROOT/atlas_full/atlas.sqlite}"
FINAL_SUMMARY_PATH="${FINAL_ATLAS_PATH%.sqlite}.merge.summary.json"
JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_PROJECT_DIR="${JULIA_PROJECT_DIR:-webapp_hpc}"

if ! command -v module >/dev/null 2>&1; then
  source /etc/profile.d/modules.sh 2>/dev/null || true
fi
module load julia/1.10.9

export BNC_NO_PROGRESS=1
export BNC_HEADLESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_NUM_PRECOMPILE_TASKS="${JULIA_NUM_PRECOMPILE_TASKS:-4}"

cd "$BNC_ROOT"
case "$PWD" in
  /storage/*|/storage2/*) ;;
  *) echo "Refusing to run compute from non-storage BNC_ROOT=$PWD" >&2; exit 2 ;;
esac

if [ -s "$FINAL_ATLAS_PATH" ] && [ "${OVERWRITE_ATLAS:-0}" != "1" ]; then
  echo "Refusing to overwrite existing atlas: $FINAL_ATLAS_PATH" >&2
  echo "Set OVERWRITE_ATLAS=1 to replace it." >&2
  exit 3
fi

SHARD_ROOT="$BNC_ROOT/atlas_full/shards_n${SHARD_COUNT}"
INPUTS=()
for idx in $(seq 0 $((SHARD_COUNT - 1))); do
  shard="$SHARD_ROOT/atlas_shard_$(printf "%03d" "$idx").sqlite"
  [ -s "$shard" ] || { echo "Missing shard sqlite: $shard" >&2; exit 4; }
  INPUTS+=("$shard")
done

mkdir -p "$(dirname "$FINAL_ATLAS_PATH")"
RUN_ATLAS_PATH="$FINAL_ATLAS_PATH"
RUN_SUMMARY_PATH="$FINAL_SUMMARY_PATH"
CACHE_DIR=""
cleanup() {
  if [ -n "$CACHE_DIR" ] && [ -d "$CACHE_DIR" ]; then
    rm -rf "$CACHE_DIR"
  fi
}
trap cleanup EXIT

if [ -d /data ]; then
  CACHE_DIR="/data/${SLURM_JOB_ID:-manual}_bne_atlasmerge"
  mkdir -p "$CACHE_DIR"
  RUN_ATLAS_PATH="$CACHE_DIR/atlas.sqlite"
  RUN_SUMMARY_PATH="$CACHE_DIR/atlas.merge.summary.json"
  echo "Merging atlas shards on local compute-node SSD: $RUN_ATLAS_PATH"
fi

"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -t "$JULIA_NUM_THREADS" \
  webapp/scripts/merge_atlas_sqlite_shards.jl "$RUN_ATLAS_PATH" "${INPUTS[@]}" "$RUN_SUMMARY_PATH"

if [ "$RUN_ATLAS_PATH" != "$FINAL_ATLAS_PATH" ]; then
  cp -p "$RUN_ATLAS_PATH" "$FINAL_ATLAS_PATH"
  cp -p "$RUN_SUMMARY_PATH" "$FINAL_SUMMARY_PATH"
fi
