---
title: Schema contract and coverage
status: verified
verified_against: b91cf41
---

# Schema contract and coverage

A schema tells a client which fields may appear. It does not prove that a
computation converged, that every producer was validated, or that an old stored
file has current meaning. Shape and evidence are separate contracts.

The exact schema path, `$id`, identity field, version, contract owner, and
declared coverage are generated in the
[contract reference](../generated/reference.md#json-schemas). Values there come
from the schema and catalogs, so this page does not copy the full inventory.

## What coverage statements mean

The contract catalog distinguishes four kinds of evidence:

- generated drift checks show that a derived file matches its runtime source;
- runtime contract tests show that selected positive and negative inputs behave
  as promised;
- tracked-instance validation shows that committed examples satisfy a schema;
- semantic tests protect meaning that JSON Schema alone cannot express, such as
  keeping proxy candidates separate from verified recommendations and rejecting
  incomplete numerical evidence.

“Partial,” “conditional,” or “not established” means `unknown`, not “known
invalid.” Upgrade such a claim only after adding an identified producer fixture
and a validator that actually loads it.

## Multi-input reaction-order field v1

`bne-ro-field/v1.0.0` is the strict additive interchange contract for a
reaction-order response over an ordered multi-input log domain. The top-level
discriminator separates three artifacts:

- `sampled_grid` carries output and reaction-order tensors plus one validity flag
  per evaluated domain point;
- `exact_cell_complex` v1 carries canonical counter-clockwise 2D cell vertices,
  areas, ordered affine labels, facet endpoints/incidence/line geometry, explicit
  singular strata, and gaps; an optional H-representation may be retained but is
  not the canonical 2D interchange geometry; and
- `directional_path` carries a declared one-dimensional pullback through a field,
  including both the full local matrix and the path-direction product.

Input axis order, log basis, dimensionless reference scales, closed bounds, fixed
background, output order, and output/input component order are explicit identity
fields. Coverage records eligible, evaluated, valid, invalid, omitted, and stored
populations together with work/storage budgets. A truncated or incompletely stored
artifact is `partial` even when all retained entries are numerically valid. Invalid
numeric entries are null gaps, not zeros.

JSON Schema cannot compare an arbitrary-rank shape with flattened array lengths or
prove that cell incidence is closed. The focused contract test therefore also
checks axis/output/component links, tensor ranks and lengths, cell/facet references,
population arithmetic, and partial-state consistency. Small tracked examples cover
all three representations:
[`sampled_grid`](../../tests/fixtures/ro_field/sampled-grid.json),
[`exact_cell_complex`](../../tests/fixtures/ro_field/exact-cell-complex.json), and
[`directional_path`](../../tests/fixtures/ro_field/directional-path.json).

Coincident source regimes are deduplicated by geometry without discarding their
distinct affine labels. A cell is `set_valued` exactly when it retains more than
one such label. Any singular stratum or set-valued cell makes the artifact
`partial`, contributes invalid coverage, and forces `no_positive_claim`; complete
enumeration alone cannot turn lower-dimensional or ambiguous evidence into a
single-valued field.

The companion request identity is `bne-ro-field-request/v1.0.0`. Exactly one model
source is allowed: a complete inline NetworkIR, a canonical `network_ir_hash`, or
an in-memory `session_id`. Sampled requests require ordered Cartesian coordinates;
exact requests prohibit sampling and are restricted to two conserved-total axes.
Both branches carry finite work/payload/deadline bounds. Exact construction exposes
the five builder limits (`max_candidate_regimes`, `max_cells`,
`max_singular_strata`, `max_pair_checks`, and `max_facets`) with synchronous hard
caps. The portable schema can express `inline`, `chunked`, or
`artifact_reference` output storage. Schema validity is not a capability promise:
a synchronous implementation may reject the two non-inline modes with a structured
response rather than silently changing storage semantics. Tracked request examples
exercise inline NetworkIR, hash, and session sources in
[`tests/fixtures/ro_field_request`](../../tests/fixtures/ro_field_request).

The schemas and fixtures establish interchange structure only. Their bounded
runtime owners add separate evidence: `sample_reaction_order_field` demonstrates
sampled tensors through three inputs; `build_ro_cell_complex` constructs exact
fixed-background 2D geometry; `POST /api/v1/ro_field` publishes inline sampled or
exact fields; SQLite 0.4 stores verified field documents and eligible RPB2
identities; SQLite 0.5 adds normalized, artifact-recomputed regular-cell
signature indexes; and the experimental `/ro-field-demo.html` page renders 2D results or
requires an explicit slice for higher rank. The synchronous runtime rejects
`chunked` and `artifact_reference` before computation, so their presence in the
portable request schema is not a storage-capability claim.

The versioned regular-cell signature and explicit finite-corpus Atlas are local
Julia contracts, not additional interchange-schema representations. They index at
most eight caller-declared exact 2D records, exclude declared lower-dimensional
strata, classify ambiguous/gapped fields as unknown, and scope zero-match results
to the declared demo corpus or evaluated subset. They do not enumerate network
space.

No Workspace/native document integration is claimed. The synchronous endpoint
still rejects `chunked` and `artifact_reference`, but a separate local
`compute_ro_field` Job protocol now realizes deterministic Cartesian plans as
content-addressed work units, chunks, checkpoints, and complete manifests.
Resume creates a new child Job with immutable parent/checkpoint lineage; result
reads revalidate the nested artifacts. Strict server-generated 2D slices reuse
verified Cartesian source points without interpolation. These local runtime
contracts do not add a new representation to RO Field v1, provide a remote
executor, or authorize a complete Atlas campaign. Existing SISO RPB1 payloads
retain their sequence identity; they are not migrated or re-labelled by these
schemas. The mathematical and resource decision is recorded in
[`0005-multi-input-reaction-order-field`](../decisions/0005-multi-input-reaction-order-field.md).

An additional sparse-v2 runtime protocol is deliberately disjoint from that
Cartesian Job and from the RO Field v1 interchange Schema. Its request/spec,
plan, result, batch, state, point-chunk, checkpoint, terminal, and manifest
identities use v2 discriminators and the `ro-field-sparse-v2` local namespace;
none is a fourth `bne-ro-field/v1.0.0` representation. The strict owner is
[`ro_field_sparse_jobs.jl`](../../webapp/src/ro_field_sparse_jobs.jl), backed by
the engine state machine in
[`ro_sparse_sampler.jl`](../../Bnc_julia/src/rop/ro_sparse_sampler.jl). One
adaptive multi-index batch is one work unit, and each checkpoint binds the
prior state, prepared batch, ordered point-result chunk, and next state.

Sparse-v2 remains `local_async` and single-process. Submission performs bounded
record/control-artifact admission; the child worker performs one metered
authoritative forward replay of the named parent checkpoint before reuse, and
result publication/reads replay the complete plan-transition-terminal chain
once. Unreferenced CAS files are not committed
evidence. The plan transitively binds every artifact to a server-derived
numerical-execution policy: module-load-frozen active lock, engine source, and
Web source tree, explicit homotopy/Tsit5 tolerances and work caps, in-RHS
cancellation, strict Float64 closed-cell regime membership, and bounded replay
work. Resume copy shares that artifact-chain meter; bounded plan/model
reconstruction occurs before it. A runtime-lock mismatch
fails before a
child can evaluate or extend the parent. The focused runtime contract is
[`ro_field_sparse_job_contract.jl`](../../webapp/test/ro_field_sparse_job_contract.jl).
This path supplies no shared object store, remote executor, multi-process work
stealing/recovery, or artifact garbage collection, and it does not make strict
Cartesian slices consumers of adaptive data.

The phased theory, chunk/job, sparse-sampling, exact-3D, uncertainty, dynamics,
and campaign gates are recorded in
[`0006-multi-input-ro-field-research-roadmap`](../decisions/0006-multi-input-ro-field-research-roadmap.md).

The working tree also has a strict runtime identity for
`bne-ro-field-differential-analysis/v1.0.0`. It is derived from a complete inline
sampled v1 field, stores its own source and analysis hashes, and keeps
finite-grid consistency and cross-curvature policy wording separate from the
source field's evidence class. Its hand-authored analysis and request Schemas are
[`ro-field-differential-analysis.schema.json`](../../schemas/ro-field-differential-analysis.schema.json)
and
[`ro-field-differential-request.schema.json`](../../schemas/ro-field-differential-request.schema.json).
The tracked analytic fixture is loaded by a semantic Python contract, while the
Julia owner tests multidimensional tensor, gap, policy, tamper, and HTTP behavior.
These schemas are separate contracts and must not be inferred from the base
RO-field Schema.

## Workspace document v2

The complete browser/native document identity is `bne-workspace/v2.0.0` and is
owned by `schemas/workspace.schema.json`. Unlike the earlier partial schemas,
this document schema covers canvas state, node identity/type/position/data,
typed connections, the optional Design Agent conversation, and supported
extension fields.

Version 1 documents are input to a deterministic migrator, not an alternate
current schema. The migrator expands the six merged legacy workflow nodes into
typed config/result nodes, resolves all restored endpoints through the strict
port registry, drops cross-family or otherwise invalid wires with diagnostics,
removes runtime/session fields, and marks restored derived results historical.
The browser and Swift decoder consume shared v1/v2/future fixtures. Unknown
future versions fail before replacing the current workspace.

Schema validity does not make a restored result current. The separate workflow
lifecycle contract controls whether a result may flow downstream; see
[`workflow-execution.md`](workflow-execution.md).

## Release-candidate external evidence

`bne-release-candidate-evidence/v1.0.0` is an operator-completed record for one
immutable candidate across Registry, Compose/TLS, AWS, Slurm, and macOS. The
tracked JSON is deliberately a `not_run` template. It is not an execution log
and does not establish any external result.

A lane can be `passed` only with a named authorized environment, at least one
timestamped command observation whose redacted output is SHA-256-addressed, and
a completed rollback observation or a non-empty reason why rollback is not
applicable. `overall_status=passed` additionally requires a clean 40-character
source commit, configuration hash, OCI digest, macOS artifact hash, and all five
lanes passed. These structural checks prevent accidental promotion of a local
or mocked check; an operator must still review whether each observation proves
the lane named in the runbook.

See the prepared-but-not-executed
[`release-candidate-external-verification`](../runbooks/release-candidate-external-verification.md)
runbook. Evidence files remain outside the tracked template and must be
redacted before hashing.

## Current Design Screen contract

The current response identity is `bne-design-screen/v0.3.0`. Version 0.3 makes
synchronous evaluation coverage visible:

- `eligible_count` is the number of deduplicated catalogue records eligible
  before the card budget is applied;
- `evaluated_count` is the number of records actually evaluated;
- `screened_count` remains the compatibility count and equals
  `evaluated_count`;
- `truncated` is true exactly when `eligible_count > evaluated_count`;
- the same three coverage fields are repeated in `screen_summary` for display
  consumers.

This is not a pagination promise. A truncated response says that the synchronous
screen stopped at its bounded prefix; unevaluated records are neither failures
nor recommendations. The synchronous request cap is 64 screened cards and 8
exact placements. A requested budget above those values is a 422 budget failure,
not silent clipping.

### Migration from v0.2

Version 0.2 consumers that read only `screened_count` can continue to display the
evaluated prefix, but they cannot tell whether candidates were omitted. Current
consumers must require the v0.3 identity and use `eligible_count`,
`evaluated_count`, and `truncated` together. The browser renderer keeps a v0.2
display fallback for historical artifacts; that fallback does not change the
current producer schema.

`screened_candidates` remain proxy-only. `verified_recommendations` remain the
only recommendation list. The transitional `recommended` alias must equal the
verified list, never the screened prefix.

## Fixed-topology ROP shape contract

The request identity is `bne-rop-shape-optimize-request/v1.0.0`; the result
identity is `bne-rop-shape-optimization/v1.0.0`. Both schemas are hand-authored
Draft 2020-12 contracts with runtime instance tests. The only public endpoint is
`POST /api/v1/rop_shape_optimize`; there is no bare `/api` alias.

The request is reference-relative and fixed-topology. It includes the complete
network or complete legacy input for Julia canonicalization, the expected hash
when known, exact finite-window design target, pinned reference, typed edit,
optimization policy, finite work budget, and mandatory stored replay. Unknown
fields, stale hashes, non-finite values, and unsupported replay shapes are
rejected.

The result schema does not collapse all numbers into one evidence claim. It
keeps:

- eligible/evaluated path and cell populations plus truncation;
- the closed geometric effect limit and selected interior realization;
- parameter-only margin with basis, dimension, equality rank, and basis matrix;
- active compiled rows and right-hand-side shadow-price semantics;
- per-cell and union directional intervals in the original direction units;
- a complete sampled replay request, validity record, curve, and metrics; and
- request, result, network, spec, reference, and artifact hashes.

Schema validity alone does not prove a global optimum or a replay pass. Runtime
semantic tests enforce that global labels require untruncated coverage, that the
parameter margin uses the equality-feasible background subspace, and that a
finite-shape pass requires complete valid replay. See the
[module card](../modules/rop-shape-optimization.md) and
[evidence decision](../decisions/0002-rop-shape-margin-and-evidence.md).

## Asynchronous job result commit manifest

The working-tree identity `bne-job-result-manifest/v1.0.0` is a deliberately
small commit marker for asynchronous results. A worker validates the sibling
result-artifact metadata and submitted config identity, serializes the result
once, publishes `result.json`, then publishes `result-manifest.json`, and only
then reports worker success. The manifest records the job and result kind,
algorithm/config identity, exact byte length, JSON media type, top-level payload
count, and SHA-256 of the serialized result.

For new AWS job records, the broker downloads only the manifest (bounded to 64
KiB) and compares it with the canonical record plus `HeadObject` length, content
type, and worker-written SHA-256 object metadata. It does not download or
materialize the potentially large Atlas result during status polling. Only
records created before the protocol field existed use the legacy inline result
validator. A broker that reads a persisted unknown future protocol keeps it
retryable because that version mismatch is not evidence that the result itself
is invalid. A worker asked to execute an unsupported protocol instead fails the
worker job. Rolling deployments therefore update the worker image/job definition
before enabling a broker that submits the new protocol.

This contract proves the mocked local publication/verification protocol, not a
live S3 transfer or AWS Batch run.

## Numerical validity fields

Several scan responses are handler contracts rather than members of a single
hand-authored schema family. Their additive validity fields still have fixed
meaning:

| Response kind | Validity fields | Meaning |
|---|---|---|
| 1D scan, placer curve, qK-space ROP cloud | `valid`, `partial` | one Boolean per sample; `partial` is true when any sample is invalid |
| 2D scan, atlas landscape, FRET heatmap | `validity_grid`, `partial` | one Boolean per cell; `partial` is true when any cell is invalid |
| Placement threshold/program verification | `verification_validity`, `verification_partial` | only complete valid verification may pass |
| Inverse refinement | `refinement_status`, `valid_sample_count`, `sample_count`, `partial` | partial numerical scans are not eligible for best-candidate selection |

Non-converged or non-finite samples are invalid gaps, not zero-valued data. JSON
serialization may encode non-finite numeric placeholders safely, but a consumer
must use the validity mask and must not fill a gap to manufacture a pass, shape,
or ranking score.

## Ownership rules

1. `NetworkIR` and `DesignSpec` fields and version constants are owned by Julia
   source. Change the structs/parser and regenerate their schemas; do not make a
   generated JSON file the primary edit.
2. A hand-authored schema changes with its runtime producer or consumer and a
   contract test in the same change.
3. Each schema has one `$id`, one identity-field `const`, one contract owner, and
   one artifact-catalog owner. The schema owns its version value; catalogs point
   to it instead of copying it.
4. A schema family and a concrete version are different. The NetworkIR parser's
   family-level acceptance is forward-compatibility behavior, not proof that an
   unknown future payload has the intended meaning. The DesignabilitySpec
   normalizer instead requires its exact current version.
5. `additionalProperties: true` is an extension policy, not permission for a
   client to rely on arbitrary fields forever.
6. JSON Schema `$id` values identify schemas. Repository links remain relative,
   so private checkout locations never enter the public contract.

## Identity boundary schemas cannot express

Canonical topology relabeling is exact only for binding networks with at most
seven free species. Above seven, `NetworkIR` and inverse-support identities use a
deterministic positional content hash instead of factorial relabeling. The
payload remains schema-valid, but two payloads that differ only by species
renaming are not promised to deduplicate. Direct exact atlas canonicalization
rejects that unsupported size. Even within seven, the full NetworkIR content
hash retains declared content beyond the topology code, so graph isomorphism
alone is not a promise of equal full hashes. This is an identity algorithm
boundary, not a JSON Schema error.

## Required change sequence

For a schema-bearing interface:

1. change the runtime owner;
2. add or update positive and negative contract fixtures;
3. update or regenerate the schema;
4. run producer and consumer tests;
5. regenerate the reference with `python3 scripts/verify_repository.py --write`;
6. run the read-only gate with `python3 scripts/verify_repository.py --check`;
7. document migration and sunset behavior when compatibility is not additive.

`webapp/scripts/validate_artifacts.py` implements only the draft-07 subset used
by its configured artifact families. A pass must not be generalized to schemas
or instances the script did not load.

## Verified against

- Workspace v2 and release-candidate evidence contracts: implementation
  revision `b91cf41`; JavaScript/Swift shared-fixture checks, Draft 2020-12
  instance tests, and the local repository gate passed on 2026-07-15. The
  release-candidate template remains `not_run` external evidence.
- Shape-optimization schema extension: committed integration revision
  `f2ca13c`; official Draft 2020-12 instance checks and the repository gate
  passed locally on 2026-07-15.
- Earlier implementation anchor: `1177a3d`.
- Historical baseline: schema ownership and drift rules were audited at
  `f9c65a5`; that audit predates Design Screen v0.3 and the explicit numerical
  validity fields above.
