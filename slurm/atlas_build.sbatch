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
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
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

FINAL_ATLAS_PATH="$ATLAS_PATH"
FINAL_SUMMARY_PATH="${FINAL_ATLAS_PATH%.sqlite}.summary.json"
RUN_ATLAS_PATH="$FINAL_ATLAS_PATH"
RUN_SUMMARY_PATH="$FINAL_SUMMARY_PATH"
CACHE_DIR=""
cleanup() {
  if [ -n "$CACHE_DIR" ] && [ -d "$CACHE_DIR" ]; then
    rm -rf "$CACHE_DIR"
  fi
}
trap cleanup EXIT

mkdir -p "$(dirname "$FINAL_ATLAS_PATH")"
if [ ! -s "$FINAL_ATLAS_PATH" ] && [ -d /data ]; then
  CACHE_DIR="/data/${SLURM_JOB_ID:-manual}_bne_atlasbuild"
  mkdir -p "$CACHE_DIR"
  RUN_ATLAS_PATH="$CACHE_DIR/atlas.sqlite"
  RUN_SUMMARY_PATH="$CACHE_DIR/atlas.summary.json"
  echo "Building atlas on local compute-node SSD: $RUN_ATLAS_PATH"
else
  echo "Building atlas directly at: $RUN_ATLAS_PATH"
fi

export ATLAS_SQLITE_PATH="$RUN_ATLAS_PATH"   # run_atlas_scan overrides the spec's sqlite_path with this
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -t "$JULIA_NUM_THREADS" \
  webapp/scripts/run_atlas_scan.jl "$SPEC" "$RUN_SUMMARY_PATH"

if [ "$RUN_ATLAS_PATH" != "$FINAL_ATLAS_PATH" ]; then
  echo "Copying atlas back to shared storage: $FINAL_ATLAS_PATH"
  cp -p "$RUN_ATLAS_PATH" "$FINAL_ATLAS_PATH"
  cp -p "$RUN_SUMMARY_PATH" "$FINAL_SUMMARY_PATH"
fi
