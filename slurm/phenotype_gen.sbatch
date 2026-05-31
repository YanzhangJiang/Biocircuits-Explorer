#!/usr/bin/env bash
# Phase-2 dataset generation: embarrassingly-parallel phenotyper sweep over
# atlas behavior_slices, one array task per shard -> training shards + diagnostic
# scalar cache. Run AFTER the baseline, when building the learned-layer dataset.
#
# Submit (keep --array end = NUM_SHARDS-1):
#   N=32
#   mkdir -p logs datasets/latent-atlas-v0
#   sbatch --array=0-$((N-1))%16 \
#          --export=ALL,BNC_ROOT=$PWD,ATLAS_PATH=/path/to/atlas.sqlite,NUM_SHARDS=$N \
#          slurm/phenotype_gen.sbatch
#
# Runs:  webapp/scripts/gen_phenotype_shards.jl   (PENDING — Phase-2 deliverable)

#SBATCH --job-name=bne-phenogen
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=08:00:00
#SBATCH --array=0-31%16                    # end must equal NUM_SHARDS-1
#SBATCH --output=logs/phenogen_%A_%a.out
#SBATCH --error=logs/phenogen_%A_%a.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
: "${ATLAS_PATH:?set ATLAS_PATH}"
NUM_SHARDS="${NUM_SHARDS:-${SLURM_ARRAY_TASK_COUNT:-32}}"
JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_PROJECT_DIR="${JULIA_PROJECT_DIR:-webapp_hpc}"
if ! command -v module >/dev/null 2>&1; then
  source /etc/profile.d/modules.sh 2>/dev/null || true
fi
module load julia/1.10.9

export BNC_NO_PROGRESS=1
export BNC_HEADLESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

cd "$BNC_ROOT"
case "$PWD" in
  /storage/*|/storage2/*) ;;
  *) echo "Refusing to run compute from non-storage BNC_ROOT=$PWD" >&2; exit 2 ;;
esac

OUT_STORAGE="$BNC_ROOT/datasets/latent-atlas-v0"
WORK_ATLAS="$ATLAS_PATH"
OUT_RUN="$OUT_STORAGE"
CACHE_DIR=""
cleanup() {
  if [ -n "$CACHE_DIR" ] && [ -d "$CACHE_DIR" ]; then
    rm -rf "$CACHE_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$OUT_STORAGE"
if [ -d /data ]; then
  CACHE_DIR="/data/${SLURM_JOB_ID:-manual}_${SLURM_ARRAY_TASK_ID:-0}_bne_phenogen"
  mkdir -p "$CACHE_DIR/out"
  sleep $(( (${SLURM_ARRAY_TASK_ID:-0} % 16) * 5 ))
  echo "Copying atlas to compute-node SSD cache: $CACHE_DIR/atlas.sqlite"
  if cp -p "$ATLAS_PATH" "$CACHE_DIR/atlas.sqlite"; then
    WORK_ATLAS="$CACHE_DIR/atlas.sqlite"
    OUT_RUN="$CACHE_DIR/out"
  else
    echo "Atlas copy to /data failed; falling back to shared storage reads." >&2
    rm -rf "$CACHE_DIR"
    CACHE_DIR=""
    OUT_RUN="$OUT_STORAGE"
  fi
fi
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -t "$JULIA_NUM_THREADS" \
  webapp/scripts/gen_phenotype_shards.jl \
    --atlas       "$WORK_ATLAS" \
    --num-shards  "$NUM_SHARDS" \
    --shard-index "${SLURM_ARRAY_TASK_ID:-0}" \
    --out         "$OUT_RUN"

if [ "$OUT_RUN" != "$OUT_STORAGE" ]; then
  cp -p "$OUT_RUN/shard_${SLURM_ARRAY_TASK_ID:-0}.jsonl" "$OUT_STORAGE/"
  cp -p "$OUT_RUN/shard_${SLURM_ARRAY_TASK_ID:-0}.meta.json" "$OUT_STORAGE/"
fi
