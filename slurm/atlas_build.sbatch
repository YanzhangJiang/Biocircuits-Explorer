#!/usr/bin/env bash
# Phase-2 step 1 (source B): build a FULL non-path_only atlas (ROP evidence +
# motif/exact labels + volumes) over the MVP binding family. Heavy, multi-threaded,
# one job. Output: a single atlas sqlite at $ATLAS_PATH.
#
#   mkdir -p logs atlas_full
#   sbatch --export=ALL,BNC_ROOT=$PWD,ATLAS_PATH=$PWD/atlas_full/atlas.sqlite \
#          slurm/atlas_build.sbatch
#
# Scope/cost knob: edit atlas_specs/atlas_full_v0.spec.json (base_species_counts,
# max_reactions). Runs: webapp/scripts/run_atlas_scan.jl <spec>.

#SBATCH --job-name=bne-atlasbuild
#SBATCH --partition=__PARTITION__        # TODO
#SBATCH --account=__ACCOUNT__            # TODO or delete
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=logs/atlasbuild_%j.out
#SBATCH --error=logs/atlasbuild_%j.err

set -euo pipefail
: "${BNC_ROOT:?set BNC_ROOT}"
: "${ATLAS_PATH:?set ATLAS_PATH (output atlas sqlite, e.g. \$PWD/atlas_full/atlas.sqlite)}"
SPEC="${SPEC:-atlas_specs/atlas_full_v0.spec.json}"
JULIA_BIN="${JULIA_BIN:-julia}"
# module load julia/1.12                 # TODO

export BNC_NO_PROGRESS=1
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export ATLAS_SQLITE_PATH="$ATLAS_PATH"   # run_atlas_scan overrides the spec's sqlite_path with this

cd "$BNC_ROOT"
mkdir -p "$(dirname "$ATLAS_PATH")"
"$JULIA_BIN" --project=webapp -t "$JULIA_NUM_THREADS" \
  webapp/scripts/run_atlas_scan.jl "$SPEC" "${ATLAS_PATH%.sqlite}.summary.json"
