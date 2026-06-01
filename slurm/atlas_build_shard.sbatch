#!/usr/bin/env bash
# Optional scale-out atlas build for the d<=3, mu<=5 assembly atlas.
#
# This is for using multiple compute nodes safely. Each array task writes its own
# shard SQLite under atlas_full/shards_n${SHARD_COUNT}/, so no two jobs write the
# same SQLite file. Merge shards later with slurm/atlas_merge_shards.sbatch.
#
# Submit example:
#   SHARDS=8
#   sbatch --array=0-$((SHARDS-1))%4 \
#          --export=ALL,BNC_ROOT=$PWD,SHARD_COUNT=$SHARDS \
#          slurm/atlas_build_shard.sbatch

#SBATCH --job-name=bne-atlas-shard
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
#SBATCH --cpus-per-task=32
#SBATCH --mem=180G
#SBATCH --time=48:00:00
#SBATCH --output=logs/atlas_shard_%A_%a.out
#SBATCH --error=logs/atlas_shard_%A_%a.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"

SPEC="${SPEC:-atlas_specs/atlas_full_v0.spec.json}"
SHARD_COUNT="${SHARD_COUNT:-${SLURM_ARRAY_TASK_COUNT:-8}}"
SHARD_INDEX0="${SLURM_ARRAY_TASK_ID:-0}"
SHARD_INDEX=$((SHARD_INDEX0 + 1))
SHARD_MODE="${SHARD_MODE:-stride}"
JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_PROJECT_DIR="${JULIA_PROJECT_DIR:-webapp_hpc}"

if ! command -v module >/dev/null 2>&1; then
  source /etc/profile.d/modules.sh 2>/dev/null || true
fi
module load julia/1.10.9

export BNC_NO_PROGRESS=1
export BNC_HEADLESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_NUM_PRECOMPILE_TASKS="${JULIA_NUM_PRECOMPILE_TASKS:-8}"

cd "$BNC_ROOT"
case "$PWD" in
  /storage/*|/storage2/*) ;;
  *) echo "Refusing to run compute from non-storage BNC_ROOT=$PWD" >&2; exit 2 ;;
esac

SHARD_ROOT="$BNC_ROOT/atlas_full/shards_n${SHARD_COUNT}"
FINAL_SHARD_PATH="$SHARD_ROOT/atlas_shard_$(printf "%03d" "$SHARD_INDEX0").sqlite"
FINAL_SUMMARY_PATH="${FINAL_SHARD_PATH%.sqlite}.summary.json"
RUN_SHARD_PATH="$FINAL_SHARD_PATH"
RUN_SUMMARY_PATH="$FINAL_SUMMARY_PATH"
CACHE_DIR=""

cleanup() {
  if [ -n "$CACHE_DIR" ] && [ -d "$CACHE_DIR" ]; then
    rm -rf "$CACHE_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$SHARD_ROOT"
if [ -d /data ]; then
  CACHE_DIR="/data/${SLURM_JOB_ID:-manual}_${SHARD_INDEX0}_bne_atlasshard"
  mkdir -p "$CACHE_DIR"
  RUN_SHARD_PATH="$CACHE_DIR/atlas_shard.sqlite"
  RUN_SUMMARY_PATH="$CACHE_DIR/atlas_shard.summary.json"
  [ -s "$FINAL_SHARD_PATH" ] && cp -p "$FINAL_SHARD_PATH" "$RUN_SHARD_PATH"
  [ -s "$FINAL_SUMMARY_PATH" ] && cp -p "$FINAL_SUMMARY_PATH" "$RUN_SUMMARY_PATH"
  echo "Building atlas shard on local compute-node SSD: $RUN_SHARD_PATH"
else
  echo "Building atlas shard directly at: $RUN_SHARD_PATH"
fi

export ATLAS_SQLITE_PATH="$RUN_SHARD_PATH"
export ATLAS_SHARD_COUNT="$SHARD_COUNT"
export ATLAS_SHARD_INDEX="$SHARD_INDEX"
export ATLAS_SHARD_MODE="$SHARD_MODE"

"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -t "$JULIA_NUM_THREADS" \
  webapp/scripts/run_atlas_scan_chunked.jl "$SPEC" "$RUN_SUMMARY_PATH"

if [ "$RUN_SHARD_PATH" != "$FINAL_SHARD_PATH" ]; then
  cp -p "$RUN_SHARD_PATH" "$FINAL_SHARD_PATH"
  cp -p "$RUN_SUMMARY_PATH" "$FINAL_SUMMARY_PATH"
fi
