# Current verified snapshot

- Snapshot date: 2026-07-10
- Code revision inspected: `f9c65a5`
- Application version in `VERSION`: `0.1.0`
- Knowledge status: P2 architecture passed; P3 paper-provenance CI gate established
- Scope of this page: repository state and evidence routing, not manuscript claims

## What is present at this revision

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

- Canonical HTTP surface: `/api/v1`; bare `/api/*` routes are compatibility
  aliases and return a deprecation header. The declared legacy sunset is
  2027-05-25. Source: `webapp/src/routing.jl`.
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
julia --project=webapp webapp/scripts/gen_schemas.jl
git diff --exit-code -- schemas/
python3 webapp/scripts/validate_artifacts.py
python3 webapp/scripts/reader/test_reader_nofabrication.py
```

CI is the executable owner of the standard subset: `.github/workflows/ci.yml`.
This list is a routing aid; a successful narrow command does not prove an
unrelated phase complete.

## Next knowledge gate

Add deterministic generated references and a unified drift checker for routes,
schema identities, versions, links, catalog paths, and public-safety rules. Keep
the paper release gate red until its recorded provenance gaps are actually
resolved; CI success is not permission to promote the working candidate.
