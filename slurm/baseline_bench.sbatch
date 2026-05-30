#!/usr/bin/env bash
# FIRST cluster job (Phase-1 deterministic baseline): current SQL/motif atlas
# query + phenotyper rerank -> verified pass@k report. No ML. This number is the
# go/no-go baseline the learned layer must later beat (Phase-3 gate).
#
# Submit:  mkdir -p logs benchmarks/reports
#          sbatch --export=ALL,BNC_ROOT=$PWD,ATLAS_PATH=/path/to/atlas.sqlite slurm/baseline_bench.sbatch
#
# Runs:  webapp/scripts/run_benchmark.jl   (PENDING — next deliverable)

#SBATCH --job-name=bne-baseline
#SBATCH --partition=__PARTITION__          # TODO: e.g. cpu / batch
#SBATCH --account=__ACCOUNT__              # TODO: fill, or delete this line
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=logs/baseline_%j.out
#SBATCH --error=logs/baseline_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
JULIA_BIN="${JULIA_BIN:-julia}"
# module load julia/1.12                   # TODO: uncomment/adjust for your cluster

export BNC_NO_PROGRESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

cd "$BNC_ROOT"
mkdir -p benchmarks/reports
# Default candidate source = curated seed pool (no atlas needed). To use a full
# NON-path_only atlas instead, append:  --atlas "$ATLAS_PATH"
# (the current path_only exports have empty network_entries and yield nothing).
"$JULIA_BIN" --project=webapp -t "$JULIA_NUM_THREADS" \
  webapp/scripts/run_benchmark.jl \
    --tasks benchmarks/tasks \
    --seeds benchmarks/seed_networks.json \
    --K 64 \
    --out   "benchmarks/reports/baseline_${SLURM_JOB_ID:-local}.json"
