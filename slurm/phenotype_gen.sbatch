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
#SBATCH --partition=__PARTITION__          # TODO
#SBATCH --account=__ACCOUNT__              # TODO or delete
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=08:00:00
#SBATCH --array=0-31%16                    # TODO: end must equal NUM_SHARDS-1
#SBATCH --output=logs/phenogen_%A_%a.out
#SBATCH --error=logs/phenogen_%A_%a.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
: "${ATLAS_PATH:?set ATLAS_PATH}"
NUM_SHARDS="${NUM_SHARDS:-${SLURM_ARRAY_TASK_COUNT:-32}}"
JULIA_BIN="${JULIA_BIN:-julia}"
# module load julia/1.12                   # TODO

export BNC_NO_PROGRESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

cd "$BNC_ROOT"
OUT="datasets/latent-atlas-v0"
mkdir -p "$OUT"
"$JULIA_BIN" --project=webapp -t "$JULIA_NUM_THREADS" \
  webapp/scripts/gen_phenotype_shards.jl \
    --atlas       "$ATLAS_PATH" \
    --num-shards  "$NUM_SHARDS" \
    --shard-index "${SLURM_ARRAY_TASK_ID:-0}" \
    --out         "$OUT"
