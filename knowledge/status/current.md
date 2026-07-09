# Current verified snapshot

- Snapshot date: 2026-07-10
- Runtime baseline revision inspected: `f9c65a5`
- Current-tree contract inventory: derived by `python3 scripts/verify_repository.py --check`
- Application version and configured toolchains: [generated from their owners](../generated/reference.md#versions-and-configured-toolchains)
- Knowledge status: P2 architecture and P3 paper provenance passed; P4 executable drift gate established
- Scope of this page: repository state and evidence routing, not manuscript claims

## What the baseline establishes

The product has one Julia backend, a browser workspace, a Python conversational
design layer, a macOS host, local/cloud job execution, atlas tooling, and a
standalone Python periodic-table search layer. The exact ownership boundaries are
listed in `knowledge/catalogs/modules.yaml`.

The maintenance branch contains regression-backed changes for these previously
fragile boundaries:

- required baseline tests are active rather than silently accepted as expected
  failures (`webapp/test/runtests.jl`);
- inverse-design summaries retain the full atlas behavior identity
  (`webapp/test/inverse_config_contract.jl`);
- `d=1` single-base homomer candidates are searched and covered by tracked
  Python and Julia contracts (`tests/test_periodic_table.py` and
  `webapp/test/d1_atlas_contract.jl`);
- the equilibrium constructor fails closed on invalid matrix dimensions and the
  SBML bridge preserves executable identifiers separately from display names
  (`Bnc_julia/test/runtests.jl`, `webapp/test/runtests.jl`, and
  `webapp/test/reaction-species.test.mjs`);
- the web project resolves the local engine with repository-relative package
  paths (`webapp/Project.toml` and manifests);
- asynchronous local cancellation is cooperative, terminal job states are
  monotonic, and cloud calls do not execute while the shared job lock is held
  (`webapp/test/jobs_cancellation_contract.jl` and
  `webapp/test/cooperative_cancel_checkpoints_contract.jl`);
- the Reader's optional numerical dependency and no-fabrication boundary are
  exercised in CI (`webapp/scripts/reader/test_reader_nofabrication.py`);
- the degenerate-polyhedron cleanup guard is exercised by the `d=1` bounded and
  unbounded atlas contract (`webapp/test/d1_atlas_contract.jl`).

These bullets say the behavior and its regression evidence exist at the inspected
revision. The parent maintenance task owns the final, cross-platform phase gate.

## Interface state

- The canonical HTTP surface and legacy sunset are projected from executable
  route metadata into the [generated contract reference](../generated/reference.md#api-routes).
  Bare `/api/*` routes remain compatibility aliases and return a deprecation
  header until that declared sunset.
- Liveness, readiness, and Prometheus endpoints exist at `/health`, `/ready`, and
  `/metrics`. Source and tests: `webapp/src/BiocircuitsExplorerBackend.jl` and
  `webapp/test/runtests.jl`.
- Current strict schema identities and coverage are cataloged in
  `knowledge/catalogs/contracts.yaml`; do not infer conformance merely because a
  schema file exists.
- Atlas/Reader output is a prior in the Design Agent. A candidate shown as a
  verified answer requires current engine evidence; see
  `knowledge/contracts/scientific-evidence.md`.

## Known unknowns and gaps

1. The embedded periodic-table files contain conflicting slice and network
   population observations. The exact bytes, counts, and source labels are owned
   once by `knowledge/catalogs/artifacts.yaml`; the canonical publication count
   is **unknown** until a versioned release reconciles them.
2. The standalone paper repository now locks its committed consumer snapshot and
   records the newer working candidate separately. Their relationship to the
   embedded distinct-network population remains **unknown**; do not infer it by
   arithmetic. See the paper-baseline and paper-working-observation catalog
   entries.
3. Several JSON Schemas are hand-authored and do not all have instance-level CI
   coverage. `knowledge/contracts/schemas.md` records coverage per schema.
4. Runtime atlas datasets are optional and not tracked in this checkout. A
   missing `datasets/*/manifest.json` means the generic artifact validator cannot
   prove a dataset is reproducibility-pinned.
5. A paper-side artifact lock, claim ledger, figure map, CI verifier, and strict
   release gate exist at paper commit `bbe91d958df29a98b6462cbdf319002e7fdac8e6`.
   CI passes the committed snapshot; release intentionally remains blocked by
   unresolved producer lineage, claim, count-semantics, and figure-generator
   gaps.
6. Legacy `doc/`, `docs/`, `wiki/`, and copied paper prose contain stale or
   host-specific
   statements. They are historical unless a current owner explicitly re-verifies
   them.

## Repository roles

- Biocircuits-Explorer: executable computation, product contracts, and artifact
  production.
- paper_rop_periodic_table: manuscript wording, figures, and publication
  reproducibility authority.
- bmac-weekly-reports: dated weekly reports and feedback history only.

The similarly named `paper_rop_periodic_table/` directory inside this repository
is an embedded snapshot, not the authority of the standalone manuscript
repository. See `knowledge/research/repositories.md` before synchronizing data.

## Verification entry points

Run commands from the repository root:

```bash
cd webapp && npm ci && npm run lint && npm run test:js && npm run test:py
python3 -m unittest discover -s tests -p 'test_*.py' -v
julia --project=webapp webapp/test/runtests.jl
julia --project=webapp Bnc_julia/test/runtests.jl
julia --project=webapp webapp/test/test_phenotype_pipeline.jl
python3 -m pip install -r scripts/requirements-verify.txt
python3 scripts/verify_repository.py --check
python3 webapp/scripts/reader/test_reader_nofabrication.py
```

CI is the executable owner of the standard subset: `.github/workflows/ci.yml`.
This list is a routing aid; a successful narrow command does not prove an
unrelated phase complete.

## Current knowledge gate

The deterministic contract reference and unified drift checker now cover routes,
schema identities, version owners, links, catalog paths, public-safety rules,
and declared artifact fixtures. The check snapshots the Git-visible worktree and
fails on validator side effects. Keep the paper release gate red until its
recorded provenance gaps are resolved; CI success is not permission to promote
the working candidate.
