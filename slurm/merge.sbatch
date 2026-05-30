#!/usr/bin/env bash
# Phase-2 post-processing: merge phenotype shards + build leakage-aware splits.
# Submit with a dependency on the array job:
#   AID=$(sbatch --parsable --array=0-$((N-1)) ... slurm/phenotype_gen.sbatch)
#   sbatch --dependency=afterok:$AID \
#          --export=ALL,BNC_ROOT=$PWD,NUM_SHARDS=$N slurm/merge.sbatch
#
# Runs:  webapp/scripts/merge_phenotype_shards.jl + gen_splits.jl  (PENDING)

#SBATCH --job-name=bne-merge
#SBATCH --partition=__PARTITION__          # TODO
#SBATCH --account=__ACCOUNT__              # TODO or delete
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=logs/merge_%j.out
#SBATCH --error=logs/merge_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
JULIA_BIN="${JULIA_BIN:-julia}"
# module load julia/1.12                   # TODO

cd "$BNC_ROOT"
OUT="datasets/latent-atlas-v0"
"$JULIA_BIN" --project=webapp webapp/scripts/merge_phenotype_shards.jl --dir "$OUT" ${ATLAS_PATH:+--atlas "$ATLAS_PATH"}
"$JULIA_BIN" --project=webapp webapp/scripts/gen_splits.jl \
    --dataset "$OUT" --out "$OUT/splits"
