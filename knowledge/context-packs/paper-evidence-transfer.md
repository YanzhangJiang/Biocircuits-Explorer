# Context pack: paper evidence transfer

- Status: active
- Explorer runtime verified against: `f9c65a5c7650d1a047e63b0fe69bf821b9303eef`
- Explorer knowledge baseline: `0242f17cd669a18f8da5e46ce431b36b79316a72`
- Paper provenance revision: `bbe91d958df29a98b6462cbdf319002e7fdac8e6`
- Branch labels: `codex/fractal-maintainability`, `codex/paper-provenance`
- Primary module IDs: `periodic-table-research`, `atlas`
- Primary contract IDs: `scientific-evidence`, `periodic-table-result`

## Objective

Move a computed Explorer result into manuscript wording only through an explicit
release identity, paper artifact lock, claim entry, figure mapping, and passing
release gate. Keep a newer local candidate visibly separate until that chain is
complete.

## Non-goals

- Do not make the embedded `paper_rop_periodic_table/` snapshot the manuscript
  authority.
- Do not bulk-copy a dirty paper worktree, figures, or aggregate files into a
  release.
- Do not turn a matching filename, headline count, or CI pass into producer
  lineage or a publication claim.

## Read order

1. `PROJECT_SUMMARY.md`
2. `knowledge/status/current.md`
3. `knowledge/research/repositories.md`
4. `knowledge/modules/periodic-table-research.md`
5. `knowledge/contracts/scientific-evidence.md`
6. `knowledge/catalogs/artifacts.yaml`
7. In the standalone paper repository: `provenance/README.md`, then the lock,
   claim ledger, figure map, and dated observations.

## Source and evidence map

| Question | Canonical location | Focus |
|---|---|---|
| What Explorer bytes exist now? | `knowledge/catalogs/artifacts.yaml` | embedded snapshot hashes, population labels, and external paper routes |
| What wording strength is allowed? | `knowledge/contracts/scientific-evidence.md` | prior, computation, enumeration, and theorem boundaries |
| What paper baseline is locked? | `paper_rop_periodic_table:provenance/explorer-artifact-lock.json` | exact consumer bytes and unresolved producer lineage |
| Which manuscript claims may use it? | `paper_rop_periodic_table:provenance/claims.json` | assumptions and allowed/forbidden wording |
| Which figures are reproducible? | `paper_rop_periodic_table:provenance/figures.json` | output identity and generator status |
| What newer material is unpromoted? | `paper_rop_periodic_table:provenance/observations/2026-07-10-working-tree.json` | candidate deltas and semantic conflicts |

Repository-qualified paper locations are logical cross-repository references,
not filesystem paths.

## Invariants

- Explorer produces a release; the standalone paper consumes it. Neither an
  embedded copy nor a weekly report reverses that ownership.
- The current paper lock is a byte-verified consumer snapshot. Its Explorer
  producer revision and full-atlas lineage remain unresolved.
- The working candidate is an observation, not an active artifact. Its aggregate
  postprocessing and manuscript population statement are not yet reconciled.
- The `n_cells` field currently mixes two cell definitions in paper-side
  aggregates. Do not compare it across classes or promote it without a schema
  decision and regenerated artifacts.
- Existence witnesses, bounded absence, analytic theorems, and sampled volume
  annotations retain different evidence strengths.

## Path scope

May change:

- Explorer release manifests and their generators/verifiers
- `knowledge/catalogs/artifacts.yaml` and this routing pack
- standalone paper `provenance/` records and provenance CI

Must not change merely to make a gate green:

- Explorer's embedded `paper_rop_periodic_table/` snapshot
- unrelated files in either user's dirty working tree
- private weekly reports or historical records
- manuscript wording, figures, or data without the owning claim/artifact update

## Verification

Run in the standalone paper repository:

```bash
python3 provenance/verify.py --mode ci
python3 provenance/verify.py --mode release
```

Expected current result: CI passes the committed snapshot; release fails with
explicit blockers. After an intentional promotion, both must pass and the
candidate observation must record its disposition.

## Current checkpoint

- Completed: committed paper lock, ten-claim ledger, nine-figure map, dated
  candidate observation, standard-library verifier, CI workflow, and independent
  mutation testing.
- In progress: no candidate promotion.
- Next safe action: produce a versioned Explorer artifact release with its exact
  engine revision, configs, source hashes, run records, and verifier output.

## Unknowns and risks

- Producer lineage for the committed paper baseline is unresolved.
- The candidate data changes network/slice populations while current manuscript
  population wording remains stale.
- Paper and Explorer postprocessors use incompatible `n_cells` semantics under
  the same field name.
- Several committed figures lack a locked generator or use a different generated
  filename.
- The manuscript parser token total differs from the locked corpus/notebook.

## Handoff rule

Before stopping, record both repository revisions, dirty paths, the exact
verifier modes run, release blockers, and the next owner action. A green CI mode
does not mean the release gate or scientific claim is complete.
