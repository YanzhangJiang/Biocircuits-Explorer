# SLURM job scripts — Latent Atlas pipeline

Cluster jobs for the Biocircuits Explorer Latent Atlas roadmap. The macOS dev
box is not for heavy compute; these submit the work to the SLURM cluster.

These scripts are configured for Westlake HPC after a read-only login check on
2026-05-31:

- login node tested: `login04.cluster.com`
- work root: `/storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer`
- CPU partitions: `intel-sc3,amd-ep2`
- QOS: `normal`
- Julia module: `julia/1.10.9`
- Julia project: `webapp_hpc` (headless backend dependencies only)

The compute scripts refuse to run if `BNC_ROOT` is not under `/storage` or
`/storage2`, so the atlas and dataset are not accidentally generated in `$HOME`.

## Status & to-do (as of 2026-05-30)

**Nothing has run on the cluster yet.** No `atlas_full/`, no `datasets/`, no
`benchmarks/reports/baseline_cluster.json`. The only artifact is a stale local
`baseline_local.json` (v0.2.0, predates the v0.3.0 phenotyper + the regime-index
bug fix).

**Outstanding cluster compute (in dependency order):**

1. **Phase-2 (decision B) — the full-atlas pipeline** *(the real outstanding work)*:
   `atlas_build.sbatch` (heavy) → `phenotype_gen.sbatch` (array) → `merge.sbatch`.
   Only ever validated locally on a 1-network atlas; no real corpus exists.
2. **Phase-3+ GPU training** (`train.sbatch`): placeholder (`exit 1`s), not started,
   and **contingent** — per the 2026-05-30 strategic reframe it is pursued only if a
   *measured* failure of exact search justifies the learned layer.

(`baseline_bench.sbatch` is ready but does **not** need the cluster — the 11-network
seed pool runs locally in minutes; the current v0.3.0 local result is ~10/20.)

**Pre-submit checklist (do BEFORE any `sbatch`):**

- [ ] Commit / transfer the current **v0.3.0** code — otherwise the cluster
      runs old code (the regime bug, old `family_holdout`, old phenotyper all return).
- [ ] Transfer only the git-tracked/lightweight working tree. Do **not** copy local
      generated stores such as `webapp/atlas_store/` (147G locally), `node_modules/`,
      or previous SQLite outputs.
- [ ] Run `env_setup.sh` (re-instantiate). It loads `julia/1.10.9` and caps login-node
      setup to 2 Julia threads / 2 precompile tasks. It uses `webapp_hpc`, whose
      `BindingAndCatalysis` source skips Makie/GraphMakie/ImageFiltering on the
      cluster. `Pkg.precompile()` is intentionally deferred unless
      `BNC_PRECOMPILE=1` is set.
- [ ] Regenerate + commit `baseline_local.json` at v0.3.0 (current one is stale).
- [ ] Harness hardening still owed before the atlas numbers are trustworthy:
      a **Π sensitivity sweep** and a **non-degenerate candidate pool**
      (pass@1==pass@5==pass@20 today only because the seed pool is tiny).

**Reframe note:** the near-term *purpose* of the Phase-2 atlas rebuild is a
**non-degenerate baseline corpus** (so the go/no-go gates can fire), **not** ML
training data yet. Phases 3–6 are contingent; see doc Appendix D.

## 0. Prerequisites

- **Transfer the repo** keeping `webapp/` and `Bnc_julia/` as **siblings** — the
  webapp Julia project dev-depends on `../Bnc_julia` by path.
- **Julia 1.10.x on this cluster.** Use `module load julia/1.10.9`; Julia will use
  the lightweight `webapp_hpc` project by default. Do not use the full `webapp`
  project for cluster setup unless GUI/plotting dependencies are actually needed.
- **Atlas SQLite** reachable on the cluster filesystem; pass its path as `ATLAS_PATH`.
- **Shared storage output.** Put the repo under
  `/storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer`; atlas output goes to
  `$BNC_ROOT/atlas_full/atlas.sqlite`, phenotype shards and dataset to
  `$BNC_ROOT/datasets/latent-atlas-v0`, logs to `$BNC_ROOT/logs`.
- **Compute-node SSD cache.** `atlas_build.sbatch` builds on `/data` and copies the
  final SQLite/summary back to `/storage` when possible; `phenotype_gen.sbatch` copies
  the atlas to `/data` per array task and copies only shard outputs back. This avoids
  high random I/O on shared storage while keeping exported results persistent.

## 1. Cluster configuration

The CPU sbatch files are already set to:

```bash
#SBATCH --partition=intel-sc3,amd-ep2
#SBATCH -q normal
module load julia/1.10.9
export BNC_HEADLESS=1
```

No `#SBATCH --account` line is used; this matches the local cluster examples.
`train.sbatch` intentionally remains a GPU placeholder and should not be submitted.

## 2. One-time environment setup

```bash
cd /storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer
export BNC_ROOT=$PWD
bash slurm/env_setup.sh
mkdir -p logs                   # SLURM needs the --output dir to exist
```

## 3. Submit order

**First job — Phase-1 deterministic baseline** (the agreed starting point):

```bash
cd /storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer
export BNC_ROOT=$PWD
mkdir -p logs benchmarks/reports
sbatch --export=ALL,BNC_ROOT=$PWD slurm/baseline_bench.sbatch
```

(Uses the curated seed candidate pool — no atlas needed. The current atlas
exports are `path_only` and carry no reconstructable networks.)

**Later — Phase-2 (source B): build full atlas → phenotype → merge.**

```bash
cd /storage/xiaofangzhouLab/jiangyanzhang/Biocircuits-Explorer
export BNC_ROOT=$PWD
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

## 5. Headless HPC Project

The full local Julia environment pulls in the visualization stack used by the
interactive app (`Makie`, `GraphMakie`, image filtering, media-related artifacts).
That is too heavy for login-node setup and unnecessary for atlas generation.

For cluster jobs, use:

```bash
julia --project=webapp_hpc ...
```

`webapp_hpc/Project.toml` points `BindingAndCatalysis` at
`Bnc_julia_headless/`, whose wrapper sets `BNC_HEADLESS=1` before loading the
shared source. In headless mode `Bnc_julia/src/initialize.jl` skips plot imports
and `visualize.jl`; numerical/ROP/atlas code remains the same. The normal local
`webapp` project is unchanged for frontend and plotting workflows.
