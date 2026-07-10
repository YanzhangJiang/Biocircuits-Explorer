---
module: atlas
status: verified
verified_against: 1177a3d
---

# Behavior atlas and inverse design

## Purpose

The Atlas answers “what did this bounded corpus contain?” It does not answer
“what is possible in every network?” The synchronous API handles small,
already-bounded builds and lookups; enumeration, persistence, lazy evidence
materialization, and refinement run as jobs so an HTTP request cannot start
unbounded work.

The module enumerates supported binding networks, derives input-change/output
behavior slices from engine paths, persists and merges reusable corpora, queries
those corpora by behavior and structural constraints, and can refine selected
inverse-design candidates.

## Non-goals

- Extending the engine grammar silently beyond the selected search profile.
- Treating an absent atlas match as a universal impossibility proof.
- Treating summary buckets or volume proxies as materialized exact witnesses.
- Hiding failed, partial, skipped, singular, or higher-nullity analysis states.
- Treating a raw server filesystem path as a multi-tenant data capability.

## Owner paths

- Enumeration, build, merge, query, and compatibility orchestration:
  [`atlas.jl`](../../webapp/src/atlas.jl)
- Compiled query, screening, lazy witnesses, and refinement:
  [`inverse_design.jl`](../../webapp/src/inverse_design.jl)
- Synchronous request budgets:
  [`atlas_build_budget.jl`](../../webapp/src/atlas_build_budget.jl),
  [`atlas_corpus_budget.jl`](../../webapp/src/atlas_corpus_budget.jl), and
  [`atlas_query_budget.jl`](../../webapp/src/atlas_query_budget.jl)
- SQLite persistence/query projection:
  [`atlas_sqlite.jl`](../../webapp/src/atlas_sqlite.jl)
- HTTP handlers and path policy:
  [`service_handlers.jl`](../../webapp/src/service_handlers.jl) and
  [`request_support.jl`](../../webapp/src/request_support.jl)
- Behavior-program identity:
  [`behavior_program_codec.jl`](../../webapp/src/behavior_program_codec.jl)
- Engine path primitives: [`SISO.jl`](../../Bnc_julia/src/SISO.jl) and
  [`Bnc_julia/src/rop/`](../../Bnc_julia/src/rop/)

## Inputs

- Explicit network specs or, in jobs/offline workflows, an
  `AtlasEnumerationSpec` under an `AtlasSearchProfile`.
- `AtlasBehaviorConfig`, input/change expansion, output symbols, and an optional
  existing in-memory corpus or operator-managed SQLite location.
- `AtlasQuerySpec` or structured goal fields, plus optional inverse-design and
  refinement policies.
- Supported legacy atlas/library payloads carrying their schema markers.

## Outputs

- Atlas/library objects containing canonical network entries, graph and behavior
  slices, regime/transition records, family buckets, optional witness paths,
  summaries, manifests, and audit stores.
- SQLite snapshots and normalized query tables.
- Query results with compiled-query identity, candidates, witnesses, and
  evidence/audit records.
- Inverse-design responses with build plan/delta, library summary, query result,
  optional refinement, and selected best design.

## Synchronous work contract

Synchronous requests are fail-fast and bounded. Static build estimates are
checked before expensive enumeration or analysis; SISO/change-path allocation
limits are enforced incrementally during materialization and stop before the
next path or node would exceed the ceiling. The exact Atlas build limits are:

| Quantity | Limit |
|---|---:|
| Explicit networks | 8 |
| Reactions per model | 5 |
| Model dimension | 24 |
| Selectors per network | 24 |
| Active change dimensions | 4 |
| Change-expansion pre-limit candidates | 2,000 |
| Planned behavior slices | 512 |
| Combined regime candidates | 100,000 |
| Regime candidates × planned slices | 10,000,000 |

Each explicit model is built only far enough to derive its regime-candidate
product. A small reaction count therefore cannot hide an exponential candidate
set. The request is rejected before Atlas analysis when any per-model,
aggregate, slice, or product limit is exceeded.

Fresh `enumeration` is jobs-only. Synchronous explicit builds stay within the
checked-in `binding_small_v0` profile; expanded profile bounds, higher-order or
homomeric template search, `compute_volume=true`,
`include_path_records=true`, and a `path_scope` other than `feasible` are also
jobs-only.

## Query and corpus contract

An in-memory or SQLite-backed synchronous query is bounded by all of the
following:

| Quantity | Limit |
|---|---:|
| Returned results | 100 |
| Networks | 100 |
| Behavior slices | 100 |
| Indexed records across corpus tables | 10,000 |
| SQLite database plus WAL | 128 MiB |
| Combined label/symbol items | 64 |
| Combined predicates | 64 |
| One required path sequence | 16 predicates |
| Query structural complexity | 128 |
| Query JSON | 16 KiB and 256 structural value nodes |
| One numeric token | 64 components |
| One query string/token | 1,024 bytes |
| Slices × structural complexity | 256,000 |
| Slices × query bytes | 32 MiB |

`limit` must be between 1 and 100; zero or negative unbounded semantics are not
available synchronously. Queries requesting witnesses or robustness that may
trigger lazy materialization run as jobs. Enabled inverse refinement is
jobs-only, and a synchronous inverse response requires
`inverse_design.return_library=false`.

## SQLite boundary

Direct Julia/offline SQLite APIs remain filesystem APIs. HTTP is narrower:

- `sqlite_path` is disabled by default;
- production backend opt-in should set both
  `BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=1` and a trusted
  `BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT`; an empty root setting falls back to
  the built-in Atlas store, and every path remains confined beneath the active
  root;
- the UI independently requires
  `window.BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=true` before it will send
  a path;
- containment rejects the store directory itself, directories, parent escapes,
  outside paths, and escapes through existing symlinks;
- synchronous HTTP SQLite access is read-only query access to an existing,
  bounded database and never initializes or migrates it;
- SQLite-backed build, library build, merge, inverse design, and persistence are
  jobs-only.

Both opt-ins are deliberate: the browser cannot enable the backend, and the
backend flag does not make the stock UI disclose a path. Even with containment,
this is an operator-only feature for a trusted, authenticated single-store
deployment. It does not authorize one tenant to name another tenant's file.

SQLite writers use `BEGIN IMMEDIATE` transactions; nested operations use
savepoints. Full-snapshot read–merge–save and full-snapshot skip-event updates
stay in one transaction. A process-local keyed lock serializes those
read-modify-write operations for the same path while allowing different shards
to progress independently. Append/lightweight writes and different processes
are not coordinated by that Julia lock: their safety and waiting still come
from SQLite WAL, busy timeout, and retry behavior.

## Refinement and numerical validity

Refinement scans track solver validity at every sampled point. A non-converged
or non-finite point produces `partial=true`,
`refinement_status="partial_solver_failure"`, and a losing finite score. Such a
trial may be retained for diagnostics but cannot become `best_candidate`.

The final selected design comes from refinement only when reranking was enabled
and a valid refined candidate exists. If `rerank_by_refinement=false`, or every
refined candidate is partial/invalid, selection falls back to the original query
ranking. A gap in a curve is not evidence for a motif, dynamic range, or regime
transition.

## Invariants

- Canonical network identity is stable across supported reaction ordering and
  base-species relabeling only within the exact canonicalization boundary of
  seven free species. Larger NetworkIR/support identities use a deterministic
  positional fallback; relabeled equivalents may not deduplicate. Direct exact
  Atlas canonicalization rejects the unsupported size.
- A behavior slice identity includes network, change/input, output, and all
  behavior configuration fields that affect classification or program identity.
- Deriving summary or materialization configs changes only requested fields;
  quantization, program identity, and support semantics are preserved.
- SISO/change graph access goes through engine accessors, and stored graph roles
  are computed only on the active source-to-sink subgraph.
- Higher-nullity output order is represented explicitly as unidentified, never
  coerced into a scalar reaction order.
- Summary buckets and path witnesses retain distinct materialization/evidence
  states. Robustness requiring volume cannot pass from a missing volume.
- Hard negative records are scoped by query hash and profile/compiler/policy
  versions; they are not reusable across incompatible semantics.
- An Atlas miss is relative to its grammar, profile, corpus, and build
  completeness. It is not a universal theorem.

## Contract sources and tests

- Public configuration fields and atlas/library schema markers:
  [`atlas.jl`](../../webapp/src/atlas.jl)
- Canonical topology and content hashes:
  [`canonicalization.jl`](../../webapp/src/canonicalization.jl)
- Compiled query grammar and policy versions:
  [`inverse_design.jl`](../../webapp/src/inverse_design.jl)
- SQLite schema and persistence modes:
  [`atlas_sqlite.jl`](../../webapp/src/atlas_sqlite.jl)
- Atlas, query, SQLite, enumeration, path, singularity, canonicalization, and
  inverse-design testsets in
  [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl)
- Budget, capacity, invalid-refinement, and concurrent SQLite contracts:
  [`concurrency_and_budget_contract.jl`](../../webapp/test/concurrency_and_budget_contract.jl)
- HTTP path containment and default-deny contracts:
  [`input_validation_contract.jl`](../../webapp/test/input_validation_contract.jl)
- Browser double-opt-in contract:
  [`atlas-sqlite-policy.test.mjs`](../../webapp/test/atlas-sqlite-policy.test.mjs)

The main Julia CI suite includes the small contract corpora. It does not rebuild
the large research atlas or execute every workstation migration/audit script.

## Known gaps

- The full atlas/library JSON shape has code-level version markers but no single
  hand-authored JSON Schema equivalent to `NetworkIR` or
  `DesignabilitySpec`.
- The process-wide synchronous gate has two slots but no queue, fairness,
  deadline, or cancellation.
- SQLite keyed writer locking is process-local; multi-process coordination is
  delegated to SQLite.
- Static work estimates bound request shape; they are not wall-clock deadlines.
- Large atlas construction and migration parity depend on workflows outside the
  default CI corpus.
- The supported inverse-design profile is deliberately narrower than every
  capability present in the mathematical engine.

## Change protocol

1. Declare whether the change affects enumeration grammar, canonical identity,
   behavior identity, persistence schema, query semantics, budget, or evidence
   strength.
2. Add a small build/query fixture and a persistence round trip; add migration
   and version handling before changing stored schema.
3. Run the main Julia suite, budget/concurrency contracts, dedicated config/d1
   contracts, relevant migration checks, and a clean schema-drift check.
4. Rebuild or explicitly invalidate downstream atlas/research artifacts when an
   identity or classifier field changes.
5. Record profile, compiler, policy, data-source, validity, and partial status in
   any new persisted negative or witness evidence.

See [data provenance](../architecture/data-provenance.md),
[scientific evidence](../contracts/scientific-evidence.md), and
[engine card](engine-rop.md).

## Verified against

- Current source commit: `1177a3d`.
- Historical baseline: the Atlas identity/evidence review at `f9c65a5` remains
  historical evidence. It does not cover the current synchronous budgets,
  SQLite writer/HTTP policy, >7 identity fallback, or refinement validity rules.
- Boundary: small contract corpora and concurrency fixtures were verified; no
  large research Atlas or universal absence claim is treated as current proof.
