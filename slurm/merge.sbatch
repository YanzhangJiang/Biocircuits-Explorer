#!/usr/bin/env bash
# Phase-2 post-processing: merge phenotype shards + build leakage-aware splits.
# Submit with a dependency on the array job:
#   AID=$(sbatch --parsable --array=0-$((N-1)) ... slurm/phenotype_gen.sbatch)
#   sbatch --dependency=afterok:$AID \
#          --export=ALL,BNC_ROOT=$PWD,NUM_SHARDS=$N,ATLAS_PATH=$PWD/atlas_full/atlas.sqlite slurm/merge.sbatch
#
# Runs:  webapp/scripts/merge_phenotype_shards.jl + gen_splits.jl  (PENDING)

#SBATCH --job-name=bne-merge
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=logs/merge_%j.out
#SBATCH --error=logs/merge_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
JULIA_BIN="${JULIA_BIN:-julia}"
JULIA_PROJECT_DIR="${JULIA_PROJECT_DIR:-webapp_hpc}"
if ! command -v module >/dev/null 2>&1; then
  source /etc/profile.d/modules.sh 2>/dev/null || true
fi
module load julia/1.10.9
export BNC_HEADLESS=1

cd "$BNC_ROOT"
case "$PWD" in
  /storage/*|/storage2/*) ;;
  *) echo "Refusing to run compute from non-storage BNC_ROOT=$PWD" >&2; exit 2 ;;
esac

OUT="$BNC_ROOT/datasets/latent-atlas-v0"
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" webapp/scripts/merge_phenotype_shards.jl --dir "$OUT" ${ATLAS_PATH:+--atlas "$ATLAS_PATH"}
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" webapp/scripts/gen_splits.jl \
    --dataset "$OUT" --out "$OUT/splits"
