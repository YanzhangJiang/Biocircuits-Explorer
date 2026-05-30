#!/usr/bin/env bash
# One-time (per cluster / per code change) Julia environment setup.
# Instantiates the webapp project, which dev-depends on ../Bnc_julia by path —
# so the repo must be transferred with webapp/ and Bnc_julia/ kept as siblings.
#
#   BNC_ROOT=/path/to/Biocircuits-Explorer bash slurm/env_setup.sh
set -euo pipefail

: "${BNC_ROOT:?set BNC_ROOT to the repo root (the dir containing webapp/ and Bnc_julia/)}"
JULIA_BIN="${JULIA_BIN:-julia}"   # or `module load julia/1.12` then leave default

cd "$BNC_ROOT"
echo "Julia: $("$JULIA_BIN" --version)   (need >= 1.10; Manifest built with 1.12.5)"
"$JULIA_BIN" --project=webapp -e '
  using Pkg
  Pkg.instantiate()
  Pkg.precompile()
  println("webapp project instantiated + precompiled OK")
'
