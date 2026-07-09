---
module: atlas
status: verified
verified_against: f9c65a5
---

# Behavior atlas and inverse design

## Purpose

Enumerate supported binding networks, derive input-change/output behavior
slices from engine paths, persist and merge reusable corpora, query those
corpora by behavior and structural constraints, and refine selected inverse
design candidates.

## Non-goals

- Extending the engine grammar silently beyond the selected search profile.
- Treating an absent atlas match as a universal impossibility proof.
- Treating summary buckets or volume proxies as materialized exact witnesses.
- Hiding failed, partial, skipped, singular, or higher-nullity analysis states.

## Owner paths

- Enumeration, build, merge, query, and compatibility orchestration:
  [`webapp/src/atlas.jl`](../../webapp/src/atlas.jl)
- Compiled query, screening, lazy witnesses, and refinement:
  [`webapp/src/inverse_design.jl`](../../webapp/src/inverse_design.jl)
- SQLite persistence/query projection:
  [`webapp/src/atlas_sqlite.jl`](../../webapp/src/atlas_sqlite.jl)
- Behavior-program identity:
  [`webapp/src/behavior_program_codec.jl`](../../webapp/src/behavior_program_codec.jl)
- Engine path primitives: [`Bnc_julia/src/SISO.jl`](../../Bnc_julia/src/SISO.jl), [`Bnc_julia/src/rop/`](../../Bnc_julia/src/rop/)
- Build/merge/query entry scripts: [`webapp/scripts/`](../../webapp/scripts/)

## Inputs

- Explicit network specs or an `AtlasEnumerationSpec` under an
  `AtlasSearchProfile`.
- `AtlasBehaviorConfig`, input/change expansion, output symbols, and optional
  existing corpus or SQLite location.
- `AtlasQuerySpec` or structured goal fields, plus optional inverse-design and
  refinement policies.
- Supported legacy atlas/library payloads carrying their schema markers.

## Outputs

- Atlas/library objects containing canonical network entries, graph and
  behavior slices, regime/transition records, family buckets, optional witness
  paths, summaries, manifests, and audit stores.
- SQLite snapshots and normalized query tables.
- Query results with compiled-query identity, candidates, witnesses and
  evidence/audit records.
- Inverse-design responses with build plan/delta, library summary, query result,
  optional refinement, and selected best design.

## Contract sources

- Public configuration fields and atlas/library schema markers:
  [`atlas.jl`](../../webapp/src/atlas.jl)
- Canonical topology and content hashes:
  [`canonicalization.jl`](../../webapp/src/canonicalization.jl)
- Compiled query grammar and policy versions:
  [`inverse_design.jl`](../../webapp/src/inverse_design.jl)
- SQLite schema and persistence modes:
  [`atlas_sqlite.jl`](../../webapp/src/atlas_sqlite.jl)
- API handlers: [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)

## Tests

- Atlas, query, SQLite, enumeration, path, singularity, canonicalization, and
  inverse-design testsets in
  [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl).
- [`webapp/test/inverse_config_contract.jl`](../../webapp/test/inverse_config_contract.jl):
  behavior identity survives summary/materialization config derivation.
- [`webapp/test/d1_atlas_contract.jl`](../../webapp/test/d1_atlas_contract.jl):
  single-base homomer atlas construction for bounded and unbounded domains.
- [`webapp/test/cooperative_cancel_checkpoints_contract.jl`](../../webapp/test/cooperative_cancel_checkpoints_contract.jl):
  build/query/inverse workflows observe cancellation.
- [`webapp/scripts/test_query_path_ids.py`](../../webapp/scripts/test_query_path_ids.py)
  and atlas migration/audit scripts provide targeted offline checks, but are not
  all part of the default CI entrypoint.

## CI

The Julia test job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
runs the main backend suite, which includes the dedicated atlas contracts, and
checks generated IR schemas. It does not rebuild the large research atlas or
execute every workstation migration/audit script.

## Invariants

- Canonical network identity is stable across supported reaction ordering and
  base-species relabeling; deduplication and merge keys use that identity.
- A behavior slice identity includes network, change/input, output, and all
  behavior configuration fields that affect classification or program identity.
- Deriving summary or materialization configs changes only the requested fields;
  quantization, program identity, and support semantics are preserved.
- SISO/change graph access goes through engine accessors, and stored graph roles
  are computed only on the active source-to-sink subgraph.
- Higher-nullity output order is represented explicitly as unidentified, never
  coerced into a scalar reaction order.
- Summary buckets and path witnesses retain distinct materialization/evidence
  states. Robustness requiring volume cannot pass from a missing volume.
- Hard negative records are scoped by query hash and profile/compiler/policy
  versions; they are not reusable across incompatible semantics.
- SQLite writes use transactions and close statements/cursors; merges preserve
  source manifests and events.

## Known gaps

- The full atlas/library JSON shape has code-level version markers but no single
  hand-authored JSON Schema equivalent to `NetworkIR` or `DesignabilitySpec`.
- Large atlas construction and migration parity depend on workflows outside the
  default CI corpus.
- The supported inverse-design profile is deliberately narrower than every
  capability present in the mathematical engine; unsupported chemistry is
  rejected rather than generalized.
- Atlas absence is relative to its declared enumeration/profile and build
  completeness, not a universal theorem.

## Change protocol

1. Declare whether the change affects enumeration grammar, canonical identity,
   behavior identity, persistence schema, query semantics, or evidence strength.
2. Add a small build/query fixture and a persistence round trip; add migration
   and version handling before changing stored schema.
3. Run the main Julia suite, dedicated config/d1 contracts, relevant migration
   checks, and a clean schema-drift check.
4. Rebuild or explicitly invalidate downstream atlas/research artifacts when an
   identity or classifier field changes.
5. Record profile, compiler, policy, and data-source versions in any new
   persisted negative or witness evidence.

See [data provenance](../architecture/data-provenance.md) and
[engine card](engine-rop.md).

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: atlas/inverse-design source, SQLite persistence,
  identity codecs, d1/config/cancellation contracts, and CI workflow wiring.
- Boundary: small contract corpora were verified; no large research atlas,
  migration campaign, or universal absence claim was treated as current proof.
