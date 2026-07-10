# Periodic-table Research

`module_id: periodic-table-research`

## Purpose

Define and execute a bounded, reproducible search over reaction-order property
cells indexed by declared structural limits. The module canonicalizes candidate
networks, delegates numerical evaluation to Julia, and writes conclusion-only
cell, witness, and certificate records with profile and code provenance.

## Non-goals

- Bounded candidate generation is a witness-discovery frontier, not an
  exhaustive negative proof for non-trivial cells.
- This public repository does not store manuscript text, paper-side datasets,
  figure exports, or workstation-specific research snapshots.
- A precomputed aggregate, notebook output, retrieval result, or proxy score is
  not automatically a theorem, minimality certificate, or verified claim.

## Owner paths

- Definitions and records: `src/periodic_table/`
- Search, scheduling, reproduction, verification, and reporting:
  `scripts/periodic_table/`
- Executable engine semantics: `Bnc_julia/src/rop/` and related Bnc math core
- Focused smoke notebooks: `notebooks/periodic_table/`
- Generated-run location: `results/periodic_table/runs/`

## Inputs

- Declared `d` and `mu` ranges, property IDs, profile, candidate and pair bounds,
  reaction limit, batch size, parallelism, timeout, and Julia executable.
- Canonical binding-reaction grammar and profile definitions.
- The Julia engine for property evaluation of generated candidate batches.

## Outputs

- Per-run `config.json`, compressed cell results, witnesses, certificates,
  summaries, checkpoints, events, and heartbeat state.
- Reproduction commands and content-derived witness/certificate identifiers.
- A report only at the evidence strength recorded by each cell status; unknown
  cells remain unknown.

## Contract sources

- Profile, property IDs, sign semantics, status vocabulary, and profile hash:
  `src/periodic_table/complete_definition.py`
- Binding grammar and candidate canonicalization:
  `src/periodic_table/complex_generator.py`,
  `src/periodic_table/network_generator.py`, and
  `src/periodic_table/candidate_search.py`
- Record fields and atomic IO: `src/periodic_table/result_schema.py`
- Identity and inheritance: `src/periodic_table/witness_codec.py` and
  `src/periodic_table/inheritance.py`
- Cross-record verifier: `scripts/periodic_table/verify_results.py`
- Numerical property evaluation: `scripts/periodic_table/run_julia_property_batch.jl`
  backed by `Bnc_julia`.

## Tests

`tests/test_periodic_table.py` covers three-state sign handling, singular tokens,
program summaries, complex/reaction grammar, permutation-invariant
canonicalization, bounded candidate uniqueness, the single-base homomer
frontier, multi-base constraints, witness inheritance, and dry-run output shape.
`Bnc_julia/test/runtests.jl` supplies the numerical golden regression boundary.

## CI

`.github/workflows/ci.yml` runs the root periodic-table Python contracts and the
Bnc Julia golden suite. It does not run the full bounded search, reproduce a
stored witness, verify a non-dry generated run, execute notebooks, or build a
manuscript.

## Invariants

- Finite response signs are three-state `-1/0/+1`; zero is retained during
  run-length compression and singular values use separate tokens.
- Candidate identity is invariant to base-species permutation under the declared
  grammar.
- Single-base cells may include homomer assembly when the structural limit
  permits it; multi-base candidates keep the configured base-use requirement.
- Bounded search may establish existence through a reproduced witness but cannot
  claim non-existence merely because it found none.
- Positive statuses reference a stored witness; `NO_COMPLETE` references a
  stored certificate; `UNKNOWN` carries no negative certificate.
- Every run records the profile hash, current `HEAD` commit, and a best-effort
  dirty flag. The current dirty detector does not see staged-only changes, so
  that metadata is not proof of a clean source tree.

## Known gaps

- Full search execution, resume/checkpoint recovery, Julia batch failures, and
  witness reproduction are not part of CI.
- `code_commit.dirty` detects unstaged and untracked changes but not staged-only
  changes, and the result verifier does not independently validate commit
  consistency.
- Only a subset of declared properties has executable witness oracles; the
  runner must preserve unsupported properties as unknown.
- Untracked paper-side material cannot be cited or verified from this checkout.
- Notebook and report claims require an external claim ledger that maps wording
  to dataset hash, code revision, verifier, and figure generator.

## Change protocol

1. Change grammar, property semantics, status meaning, or storage policy only
   with a profile-version/hash decision and updated Python contracts.
2. Preserve the distinction between bounded witness discovery, inherited
   existence, trivial negative certificates, unknown cells, and theorem-grade
   statements in code and prose.
3. Reproduce witnesses and run `verify_results.py` before exporting a run; keep
   config, records, logs, and revision together.
4. Transfer evidence to a manuscript workflow only through a reviewed,
   versioned, hashed artifact release and claim ledger; never copy paper-side
   material back into this public repository.

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: periodic definitions, generators, schemas, runner,
  verifier, root tests, Bnc boundary, and CI workflow wiring.
- Boundary: smoke and contract coverage only; no full search, notebook result,
  dataset claim, or manuscript conclusion was promoted to verified.
