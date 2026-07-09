---
title: Data provenance and evidence boundaries
status: verified
verified_against: f9c65a5
---

# Data provenance and evidence boundaries

A file becomes trustworthy here through a chain, not through its filename:
versioned input → canonical identity → named algorithm/config → result artifact
→ verifier → downstream claim. Missing links must be reported as missing; a
plausible notebook result or copied JSON is not a substitute.

## Evidence ladder

| Level | What it can establish | Required evidence |
|---|---|---|
| Contracted input | what network, target, or policy was requested | accepted schema version and normalized payload |
| Computed result | what one named implementation returned | input/config identity, algorithm version, warnings, result payload |
| Reusable corpus | what a persisted atlas snapshot contains | corpus/schema version, canonical object IDs, merge/source metadata |
| Verified research artifact | what a run or extract supports under declared scope | code revision, profile/config, source artifact identity, verifier output |
| Paper claim | what may be stated publicly | claim strength and assumptions linked to the exact verified artifacts |

An existence witness and a negative/completeness certificate are not
interchangeable. Likewise, an atlas volume or sampled dose-response can rank or
support a candidate only at the strength declared by the producing code.

## Versioned inputs

- [`NetworkIR`](../../webapp/src/ir.jl) is the model contract
  (`bne-ir/v1.0.0`). Its hash excludes provenance metadata and uses canonical model
  content, including a topology identity that is stable under supported
  reaction order and base-species relabeling.
- [`DesignSpec`](../../webapp/src/ir.jl) separates goal, constraints,
  objectives, and policies. Its legacy bridge feeds the existing inverse-design
  pipeline without creating a second source of truth.
- [`DesignabilitySpec`](../../schemas/designability-spec.schema.json) is the
  recommendation target contract. Runtime normalization and support auditing
  live in [`designability.jl`](../../webapp/src/designability.jl).
- SBML is an interchange input/output, not the internal identity. The bridge in
  [`sbml.jl`](../../webapp/src/sbml.jl) preserves executable identifiers and
  human display names separately where the supported subset permits it.

Generated IR schemas are derived from Julia structs by
[`gen_schemas.jl`](../../webapp/scripts/gen_schemas.jl). CI compares the
in-memory deterministic render to committed bytes without rewriting them, then
the unified repository gate checks schema identity, ownership, artifacts, and
the generated reference. Hand-authored schemas remain separate contracts and
therefore need explicit tests when their producers change.

## Canonical identity

[`canonicalization.jl`](../../webapp/src/canonicalization.jl) is the single
owner for canonical JSON hashes and supported binding-network topology identity.
It sorts object keys, normalizes integral numeric forms, rejects non-finite
numbers for hashing, and is shared by IR, atlas, result artifacts, and compiled
inverse queries.

Do not introduce another “stable hash” implementation in a consumer. If two
objects should compare equal, first define that equality in the canonical owner
and cover it with cross-construction tests.

## Runtime result envelopes

[`result_artifact.jl`](../../webapp/src/result_artifact.jl) defines
`bne-result/v1.0.0`. It records:

- result kind;
- source network or spec hashes when they can be parsed;
- algorithm name and application version;
- canonical config hash;
- warnings and creation time.

The async job dispatcher attaches this envelope before persisting a result.
Some synchronous endpoints attach it individually. Therefore, consumers must
inspect the actual response contract rather than assume every historical or
flat response already has a complete envelope. The schema is
[`result-artifact.schema.json`](../../schemas/result-artifact.schema.json).

## Atlas provenance

The in-memory atlas/library contract is owned by
[`atlas.jl`](../../webapp/src/atlas.jl); the current library payload identifies
its atlas and library schema versions and carries network entries, graph and
behavior slices, regime/transition records, family buckets, optional path
records, source manifests, merge events, negative certificates, and query audit
records.

[`atlas_sqlite.jl`](../../webapp/src/atlas_sqlite.jl) persists a normalized
queryable projection plus a library snapshot. It uses an explicit SQLite schema
version, transactions, lock retry handling, and canonical slice/program keys.
The behavior configuration fields that influence identity must survive summary
and witness-materialization clones; that invariant is covered by
[`inverse_config_contract.jl`](../../webapp/test/inverse_config_contract.jl).

An atlas hit proves only what its stored evidence and declared build profile
prove. Proxy volume, partial/high-nullity records, skipped slices, and failed
network builds must remain distinguishable from complete exact records.

## Research and paper-facing artifacts

There are two research pipelines in this repository:

- [`src/periodic_table/`](../../src/periodic_table/) and
  [`scripts/periodic_table/`](../../scripts/periodic_table/) define run configs,
  cell statuses, witnesses, certificates, atomic JSONL output, and a verifier;
- [`paper_rop_periodic_table/`](../../paper_rop_periodic_table/) contains a
  reduced atlas extract, extraction code, and offline notebooks used for
  paper-facing analysis.

Treat these as downstream of the engine and atlas. Historical README prose,
embedded counts, workstation instructions, and copied result JSON can drift.
Before reusing a conclusion, verify the current artifact itself, its generator,
its source identity, and the code revision. The conclusion-only verifier
[`verify_results.py`](../../scripts/periodic_table/verify_results.py) checks run
shape, profile hashes, witness/certificate references, and optional witness
reproduction; it does not retroactively establish provenance for files outside
that run format.

The general artifact validator
[`validate_artifacts.py`](../../webapp/scripts/validate_artifacts.py) currently
covers declared dataset manifests and behavior benchmark specs. It is not a
universal repository artifact validator. A green invocation cannot be cited as
validation of an atlas database, designability screen, periodic-table extract,
or notebook unless that artifact is explicitly included.

## Provenance-preserving change protocol

1. Identify the canonical producer and the exact schema/version owner.
2. Add or update characterization tests before changing identity or evidence
   semantics.
3. Regenerate derived schemas or artifacts with the checked-in generator; do
   not edit generated output to hide drift.
4. Record input hashes, config/profile, code revision, warnings, and verifier
   command in the produced artifact or adjacent manifest.
5. Compare semantic content, not timestamps or path names alone.
6. Keep partial, sampled, proxy, existence, and negative-certificate evidence
   labeled separately.
7. Update downstream notebooks or prose only after the artifact verifier passes.

Unknown provenance is a result: mark the artifact historical or unverified and
rebuild it. Do not infer lineage from matching filenames or approximate counts.
