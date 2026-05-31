#!/usr/bin/env bash
# One-time (per cluster / per code change) Julia environment setup.
# Instantiates the webapp project, which dev-depends on ../Bnc_julia by path —
# so the repo must be transferred with webapp/ and Bnc_julia/ kept as siblings.
#
#   BNC_ROOT=/storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer bash slurm/env_setup.sh
set -euo pipefail

: "${BNC_ROOT:?set BNC_ROOT to the repo root (the dir containing webapp/ and Bnc_julia/)}"
JULIA_PROJECT_DIR="${JULIA_PROJECT_DIR:-webapp_hpc}"
if ! command -v module >/dev/null 2>&1; then
  # Non-interactive shells on some clusters need this before `module load`.
  source /etc/profile.d/modules.sh 2>/dev/null || true
fi
module load julia/1.10.9
JULIA_BIN="${JULIA_BIN:-julia}"

# This runs on the login node as software setup, so keep it within the documented
# login-node limit of 2 CPU cores.
export JULIA_NUM_THREADS="${JULIA_NUM_THREADS:-2}"
export JULIA_NUM_PRECOMPILE_TASKS="${JULIA_NUM_PRECOMPILE_TASKS:-2}"
export BNC_HEADLESS="${BNC_HEADLESS:-1}"

cd "$BNC_ROOT"
echo "Julia: $("$JULIA_BIN" --version)   (need >= 1.10; cluster module is julia/1.10.9)"
"$JULIA_BIN" --project="$JULIA_PROJECT_DIR" -e '
  using Pkg
  Pkg.develop(Pkg.PackageSpec(path=joinpath(pwd(), "Bnc_julia_headless")))
  Pkg.instantiate()
  if get(ENV, "BNC_PRECOMPILE", "") in ("1", "true", "yes", "on")
      Pkg.precompile()
      println("HPC headless project instantiated + precompiled OK")
  else
      println("HPC headless project instantiated OK (precompile deferred to compute node)")
  end
'
