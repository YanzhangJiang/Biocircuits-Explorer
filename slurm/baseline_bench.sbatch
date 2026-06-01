#!/usr/bin/env bash
# FIRST cluster job (Phase-1 deterministic baseline): current SQL/motif atlas
# query + phenotyper rerank -> verified pass@k report. No ML. This number is the
# go/no-go baseline the learned layer must later beat (Phase-3 gate).
#
# Submit from $BNC_ROOT on /storage:
#          mkdir -p logs benchmarks/reports
#          sbatch --export=ALL,BNC_ROOT=$PWD slurm/baseline_bench.sbatch
#
# Runs:  webapp/scripts/run_benchmark.jl   (PENDING — next deliverable)

#SBATCH --job-name=bne-baseline
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=08:00:00
#SBATCH --output=logs/baseline_%j.out
#SBATCH --error=logs/baseline_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
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
mkdir -p benchmarks/reports
# Default candidate source = curated seed pool (no atlas needed). To use a full
# NON-path_only atlas instead, append:  --atlas "$ATLAS_PATH"
# (the current path_only exports have empty network_entries and yield nothing).
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -t "$JULIA_NUM_THREADS" \
  webapp/scripts/run_benchmark.jl \
    --tasks benchmarks/tasks \
    --seeds benchmarks/seed_networks.json \
    --K 64 \
    --out   "benchmarks/reports/baseline_${SLURM_JOB_ID:-local}.json"
