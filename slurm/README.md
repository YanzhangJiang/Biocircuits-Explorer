# SLURM job scripts — Latent Atlas pipeline

Cluster jobs for the Biocircuits Explorer Latent Atlas roadmap. The macOS dev
box is not for heavy compute; these submit the work to the SLURM cluster.

All cluster-specific fields are **placeholders** — fill them once (see below).
The scripts pass repo/atlas locations via environment variables so nothing is
hard-coded to one machine.

## 0. Prerequisites

- **Transfer the repo** keeping `webapp/` and `Bnc_julia/` as **siblings** — the
  webapp Julia project dev-depends on `../Bnc_julia` by path.
- **Julia ≥ 1.10** (the committed `Manifest` was built with **1.12.5**; match the
  1.12.x line to avoid a re-resolve). Provide it via `module load julia/...` or by
  exporting `JULIA_BIN=/path/to/julia`.
- **Atlas SQLite** reachable on the cluster filesystem; pass its path as `ATLAS_PATH`.

## 1. Fill the placeholders (once)

In each `*.sbatch` replace:

| Placeholder | Meaning |
|---|---|
| `__PARTITION__` | CPU partition name (e.g. `cpu`, `batch`) |
| `__GPU_PARTITION__` | GPU partition (only `train.sbatch`) |
| `__ACCOUNT__` | Allocation/account, or delete the `#SBATCH --account` line |
| `# module load julia/1.12` | uncomment/adjust, **or** set `JULIA_BIN` instead |

Resource asks (`--cpus-per-task`, `--mem`, `--time`, `--array`) are sane defaults —
tune to your queue.

## 2. One-time environment setup

```bash
export BNC_ROOT=$PWD            # repo root (contains webapp/ and Bnc_julia/)
# module load julia/1.12        # or: export JULIA_BIN=/path/to/julia
bash slurm/env_setup.sh
mkdir -p logs                   # SLURM needs the --output dir to exist
```

## 3. Submit order

**First job — Phase-1 deterministic baseline** (the agreed starting point):

```bash
mkdir -p logs benchmarks/reports
sbatch --export=ALL,BNC_ROOT=$PWD slurm/baseline_bench.sbatch
```

(Uses the curated seed candidate pool — no atlas needed. The current atlas
exports are `path_only` and carry no reconstructable networks.)

**Later — Phase-2 (source B): build full atlas → phenotype → merge.**

```bash
A=$PWD/atlas_full/atlas.sqlite
# 1) build a FULL (non-path_only) atlas over the MVP scope (heavy, one job)
mkdir -p logs atlas_full datasets/latent-atlas-v0
BID=$(sbatch --parsable --export=ALL,BNC_ROOT=$PWD,ATLAS_PATH=$A slurm/atlas_build.sbatch)

# 2) phenotype its behavior_slices (array), gated on the build
N=32
AID=$(sbatch --parsable --dependency=afterok:$BID --array=0-$((N-1))%16 \
        --export=ALL,BNC_ROOT=$PWD,ATLAS_PATH=$A,NUM_SHARDS=$N slurm/phenotype_gen.sbatch)

# 3) merge shards + leakage-aware splits
sbatch --dependency=afterok:$AID \
       --export=ALL,BNC_ROOT=$PWD,NUM_SHARDS=$N,ATLAS_PATH=$A slurm/merge.sbatch
```

Scope/size of the atlas is the knob in `atlas_specs/atlas_full_v0.spec.json`
(`base_species_counts`, `max_reactions`). `sqlite_persist_mode=full` +
`compute_volume=true` are what make it evidence-rich (vs the old `path_only`).

**Much later — Phase 3+ GPU training:** `slurm/train.sbatch` is a placeholder and
intentionally `exit 1`s until a dataset + training entry point exist.

## 4. Script status

| sbatch | Julia/py script it runs | status |
|---|---|---|
| `baseline_bench.sbatch` | `webapp/scripts/run_benchmark.jl` | ✅ ready (seed source) |
| `atlas_build.sbatch` | `webapp/scripts/run_atlas_scan.jl` (full spec) | ✅ ready |
| `phenotype_gen.sbatch` | `webapp/scripts/gen_phenotype_shards.jl` | ✅ ready |
| `merge.sbatch` | `merge_phenotype_shards.jl` + `gen_splits.jl` | ✅ ready |
| `train.sbatch` | `ml/training/train.py` | ⏳ placeholder (Phase 3+) |

The phenotyper core they depend on — `webapp/src/latent_atlas/phenotype_pipeline.jl` —
is **done and smoke-tested** (`webapp/test/test_phenotype_pipeline.jl`).
