# 0005: Represent multi-input reaction order as a field, not a longer sequence

- Status: accepted-working-tree
- Date: 2026-07-17
- Verified against: `9b05d02` plus the scoped `codex/multi-input-ro-field`
  working tree and the commands in [Verification](#verification)
- Owners: `engine-rop`, `backend-runtime`, `atlas`
- Supersedes: none
- Superseded by: none

## Problem in plain language

With one swept input, the project can describe a response as an ordered sequence
of scalar reaction orders along that input. With two or more independently swept
inputs there is no single natural traversal order. Reaching the same point by
changing input A before input B or B before A can cross different regime
boundaries, even though both traversals belong to one response surface.

The project therefore needs a durable identity for a reaction-order response over
an input domain, plus bounded sampled and path views. Merely changing the current
sequence from a vector to a matrix would lose axis meaning, domain geometry,
validity gaps, path dependence, and whether computation or storage stopped early.

## Context and observed constraints

- [`Bnc_julia/src/SISO.jl`](../../Bnc_julia/src/SISO.jl) selects one changed
  q/K coordinate, orders regimes along a one-dimensional graph, and extracts one
  scalar entry of the regime gain matrix for each path position.
- [`Bnc_julia/src/rop/rop_change_paths.jl`](../../Bnc_julia/src/rop/rop_change_paths.jl)
  can attach several signed reaction-order components to each vertex, but its
  result is still a one-dimensional regime path. It is not a partition of the
  complete multi-input domain.
- [`webapp/src/parameter_scan_handlers.jl`](../../webapp/src/parameter_scan_handlers.jl)
  already returns bounded two-dimensional output and validity grids. It does not
  yet return a versioned multi-input reaction-order field.
- [`webapp/src/behavior_program_codec.jl`](../../webapp/src/behavior_program_codec.jl)
  defines RPB1 identity for a sequence of fixed-dimension states. Existing RPB1
  bytes and SISO behavior identities must not acquire a new surface meaning.
- Grid storage grows as the product of per-axis sample counts. Exact regime-cell
  arrangements can also grow combinatorially. A valid contract must describe a
  bounded prefix without turning omitted work into negative scientific evidence.
- A failed or non-finite sample is an invalid gap under
  [`scientific-evidence.md`](../contracts/scientific-evidence.md), not a numeric
  zero and not support for a behavior claim.

## Decision

### The local quantity is a reaction-order matrix

For ordered log-input coordinates

```text
u = (log(q_1 / q_1_ref), ..., log(q_k / q_k_ref))
```

and ordered log outputs `z`, the local multi-input reaction order is

```text
R(u) = dz / du
```

with shape `output_count × input_count`. A MISO result is therefore a row vector;
a MIMO result is a matrix. A log basis and a positive reference scale are recorded
for every dimensional coordinate so that the log arguments are dimensionless.

The v1 domain is a closed, axis-aligned box in these log coordinates. Canonical
input-axis order, output order, the output/input component product, bounds, log
basis, reference scales, and every fixed background q/K/control coordinate are
part of field identity. Presentation code may transpose axes only while retaining
the canonical order and provenance.

### The canonical asymptotic object is a labelled cell complex

The complete asymptotic representation is `exact_cell_complex`: a set of input-
domain cells labelled by output affine offsets and reaction-order matrices, with
facets, incidence, domain boundaries, and explicit singular, higher-nullity,
unknown, or truncated gaps. Cell and facet order are serialized for deterministic
hashing; geometric identity is not inferred from JSON object order.

`exact` means exact over the declared asymptotic model, domain, fixed background,
construction policy, and completely evaluated cell population. It does not mean
experimental truth, exact finite-equilibrium shape, global chemistry coverage, or
arbitrary-precision coefficients. The coefficient encoding is recorded.

### A sampled tensor is a separate numerical artifact

`sampled_grid` stores a bounded numerical evaluation. For domain shape
`[n_1, ..., n_k]`, `m` outputs, and `k` inputs, its declared shapes are:

```text
output_shape         = [n_1, ..., n_k, m]
reaction_order_shape = [n_1, ..., n_k, m, k]
validity shape       = [n_1, ..., n_k]
```

The artifact records axis coordinates and flatten order. Numeric entries belonging
to an invalid sample are `null`; consumers must not coerce them to zero, interpolate
them into a verified region, score them, or use them to seed a subsequent solve.
An adaptive sparse point set may use the same artifact family, but it must declare
that sampling scheme instead of pretending to be a Cartesian tensor.

### A sequence exists only after choosing a path

For a declared path `gamma(s)`, `directional_path` records the field pullback and
the directional order

```text
dz/ds = R(gamma(s)) * dgamma/ds.
```

The path, parameterization, input coordinates, full reaction-order matrix, and
directional result remain in the artifact. Two paths through the same field may
produce different sequences. Neither sequence becomes the identity of the full
field unless a future, separately versioned equivalence rule proves that choice is
safe.

### Representation and evidence layers do not collapse

The interchange identity is `bne-ro-field/v1.0.0`, owned by
[`schemas/ro-field.schema.json`](../../schemas/ro-field.schema.json). It has three
discriminated representations:

| Representation | Evidence class | Positive claim boundary |
|---|---|---|
| `sampled_grid` | `sampled_numerical` | Finite valid samples under one solver and sampling policy |
| `exact_cell_complex` | `exact_polyhedral` | Declared asymptotic cell population and fixed background only |
| `directional_path` | `directional_derived` | One declared path through a parent field or numerical evaluation |

Every artifact records `partial`, validity policy, eligible/evaluated/valid/
invalid/omitted counts, enumeration completeness, storage completeness, budgets,
and a structured truncation reason. `partial=false` requires complete enumeration,
zero invalid and omitted items, complete storage, and a complete-over-declared-
population claim. A truncated result is partial even when every retained item is
valid. Cancellation, time limits, work limits, and storage limits leave omitted
regions unknown.

Chunked data uses content-addressed artifact references. A manifest that lists all
intended chunks but does not prove complete storage remains partial. Presence of a
schema-valid artifact does not prove that a producer used the engine correctly.

### Relationships outside JSON Schema are fail-closed semantic checks

Draft 2020-12 validates structure and representation discrimination but cannot
express every rank and identity relationship for arbitrary `k` and `m`. A producer
and any trust-boundary consumer must additionally check at least:

- `axis_order` exactly equals the serialized axis-ID sequence;
- `output_order` exactly equals the serialized output-ID sequence;
- `component_order` is the ordered output/input Cartesian product;
- all lower bounds are strictly below upper bounds and swept coordinates do not
  also appear in fixed background;
- tensor shapes, flattened lengths, coordinate counts, and validity lengths agree;
- cell/facet orders and incidence references are closed over their populations;
- regular cell matrix shape is `m × k`; and
- population counts and `partial` agree with invalid, omitted, truncated, and
  incomplete-storage state.

The focused schema tests contain one small example for every representation and
execute these semantic checks. The bounded Julia producer and SQLite trust
boundary repeat the relationships they consume; JSON Schema acceptance alone is
still not runtime or scientific evidence.

### Existing one-dimensional identities remain frozen

SISO RPB1 and current Atlas path records keep their present meaning and bytes. A
new field may expose a one-axis `directional_path` compatible with an old SISO
result, but compatibility must be demonstrated by a parity test; no stored RPB1
payload is relabelled as an RO Field.

Current multi-axis `ChangePaths` can be retained as a legacy path bundle. It cannot
be promoted to `exact_cell_complex` without recomputing domain cells, boundaries,
incidence, fixed-background identity, and coverage.

## Alternatives considered

### Make a dense high-dimensional tensor the canonical object

This is simple for a plotting client, but behavior identity would then depend on
grid resolution and flattening. Exact regime boundaries and adjacency would be
lost, and storage would grow as `n^k`. It remains a useful sampled representation,
not the asymptotic identity.

### Store all monotone path sequences

This reuses current path machinery, but a continuum of paths exists and distinct
paths can encode the same surface. Mixed-direction boundaries and higher-codimension
intersections are not captured by a list of selected chains.

### Extend RPB1 state dimension in place

RPB1 already permits vector-valued states, but it still identifies an ordered
sequence. Assigning a domain/surface meaning to the same family would silently
change Atlas hashes, query behavior, and old files. A new schema family makes the
migration explicit and reversible.

## Consequences

### Benefits

- The engine, API, Atlas, UI, and designability work can share one mathematical
  vocabulary without confusing a response surface with one traversal.
- Axis transposition, tensor layout, fixed background, validity, and truncation
  are no longer implicit.
- Small examples can prove the capability while full high-dimensional campaigns
  remain explicitly uncomputed and unstored.
- Existing SISO results and RPB1 identities remain stable.

### Costs and risks

- Exact cell construction needs new geometry and incidence algorithms.
- Runtime producers and consumers need semantic validation in addition to JSON
  Schema validation.
- Dense storage remains impractical beyond small demonstrations; chunked and
  sparse/adaptive forms need resource budgets and artifact lifecycle support.
- `exact_cell_complex` may be misunderstood as finite nonlinear or experimental
  exactness unless evidence labels are kept visible.
- Axis canonicalization and field equivalence beyond exact declared ordering are
  intentionally deferred rather than guessed in v1.

## Remaining theory and engineering gaps

The field representation closes the first semantic gap, but it is not yet a
general multi-input theory.

- The derivative is a **partial derivative in a declared coordinate chart**:
  every non-swept q/K coordinate is held at the recorded background. Correlated
  experimental controls, dependent conserved totals, and alternative charts need
  an explicit full-rank coordinate map before their entries can be compared.
- For a smooth, single-valued equilibrium output, each output row of `R(u)` is
  locally conservative: mixed partials must agree wherever they exist. The exact
  affine cells satisfy this locally, while a sampled grid currently has solver
  validity but no interval error or discrete-curl certificate. A sequence of
  regimes may differ between paths even though a true equilibrium potential has
  the same endpoint; genuine hysteresis would require a dynamical, multistable
  model outside this contract.
- A reaction-order matrix is only first-order local information. Synergy,
  curvature, switching sharpness, and finite-window gate behavior require mixed
  second derivatives or independently sampled output surfaces. P4 signatures are
  deliberately coarse retrieval features, not complete field equivalence.
- Singular and higher-nullity strata can be set-valued. This migration preserves
  them as unknown evidence; it does not provide a selection theorem, a
  stratified-differential calculus, or a robustness measure near those strata.
- Exact arrangements beyond two inputs need a higher-dimensional incidence
  representation and algorithms whose worst-case cell count is combinatorial.
  No feasible dense-storage promise follows from proving one small 3D sampled
  tensor or one exact 2D complex.
- The equilibrium/asymptotic model still needs experimental calibration,
  uncertainty propagation, and identifiability analysis before a field can be
  interpreted as biological evidence.

The principal engineering gap is therefore controlled sparsity rather than a
larger dense array: asynchronous jobs, adaptive sampling with error indicators,
content-addressed chunks, server-generated slices, resumable construction,
distributed aggregation, and an independently authorized Atlas campaign. Until
those owners exist, the hard synchronous caps and demonstration corpus are part
of the scientific meaning, not temporary UI defaults.

## Implemented bounded migration

The migration is additive and deliberately demonstration-scale:

- P0 owns strict artifact/request schemas, fixtures, semantic validators, and the
  invalid-is-gap evidence policy.
- P1 adds an engine sampled tensor for one to four ordered input axes and outputs.
  Tests include a true `2 × 2 × 2` three-input field; the synchronous producer
  admits at most 4,096 Cartesian points.
- P2 adds a fixed-background exact two-total cell complex with canonical cells,
  facets, incidence, affine labels, singular strata, ambiguity, typed limits, and
  point classification. `exact` retains the Float64/asymptotic boundary above.
- P3 adds the v1-only `POST /api/v1/ro_field` inline producer, a standalone
  `/ro-field-demo.html` viewer, RPB2 `exact_cell_complex_v1` identity, and SQLite
  0.4 artifact storage. RPB2 is intentionally narrower than the interchange
  schema: only a complete, gap-free, non-singular, single-valued exact complex is
  eligible. Sampled and diagnostic exact artifacts retain their field/data hashes
  without being mislabeled as RPB2.
- P4 adds `bne-ro-field-signature/v1.0.0` over regular 2D cell interiors, an
  explicit corpus Atlas of at most eight caller-supplied records, bounded queries,
  and an append-only SQLite 0.5 migration for normalized signature features.
  Stored signatures are rebuilt from their referenced exact artifact before they
  enter a query index. Singular lower-dimensional strata are
  explicitly excluded and counted. Gaps, ambiguity, or set-valued cells yield
  `unknown`, never a selected first label.

The synchronous request hard boundaries are four axes, four outputs, 4,096 sampled
points, 4 MiB inline data, and 30 seconds. Exact requests are two conserved-total
axes with at most 4,096 candidate regimes, 128 cells, 128 singular strata, 8,192
pair checks, and 512 facets. `geometry_tolerance` is capped at `1e-6` and at
`1e-6` of the shortest engine-coordinate side; it may control clipping and
deduplication but cannot relax the independent coverage certificate. Before an
exact artifact is published, the runtime reconstructs every cell edge from its
facets, allowing T-junction segmentation but rejecting missing, duplicate,
overlapping, extra, or incidence-inconsistent segments and discontinuous regular
affine labels. Non-inline request modes remain portable schema values but return
a structured capability rejection before computation. There is no asynchronous
field job, chunk uploader, adaptive sparse sampler, exact higher-dimensional
complex, global topology enumeration, or full Atlas campaign in this migration.

No Workspace document, native-client document, RPB1 byte, or stored SISO path is
rewritten. Rollback must retain read-only handling for any persisted v1 field/RPB2
rows or explicitly report them unsupported; it must never reinterpret them as
RPB1.

## Verification

From the repository root:

```text
python3 tests/test_ro_field_schema.py
python3 tests/test_ro_field_request_schema.py
julia --project=webapp --startup-file=no -e 'using Test, BindingAndCatalysis; include("Bnc_julia/test/multi_input_ro_field_contract.jl")'
julia --project=webapp --startup-file=no -e 'using Test, BindingAndCatalysis; include("Bnc_julia/test/ro_cell_complex_contract.jl")'
julia --project=webapp --startup-file=no webapp/test/ro_field_api_contract.jl
julia --project=webapp --startup-file=no webapp/test/ro_field_identity_sqlite_contract.jl
julia --project=webapp --startup-file=no webapp/test/ro_field_behavior_contract.jl
julia --project=webapp --startup-file=no webapp/test/ro_field_atlas_contract.jl
julia --project=webapp --startup-file=no webapp/test/ro_field_signature_sqlite_contract.jl
node webapp/test/ro-field-render.test.mjs
python3 scripts/verify_repository.py --check
```

The focused contracts validate strict fixtures, request/runtime caps, 2D and 3D
sampled tensors, exact geometry and references, invalid-as-null gaps, API errors,
RPB1/RPB2 separation, SQLite identities, regular-cell signatures, finite-corpus
query wording, and the renderer's no-projection boundary. They do not establish a
complete Atlas, high-dimensional exact construction, practical performance at
large rank, experimental validity, or an external deployment.

## Follow-ups

- [x] P1 (`engine-rop`, `backend-runtime`): bounded arbitrary-rank sampled tensor
  with 2D/3D demonstrations, numerical validity gaps, axis covariance, and hard
  work limits.
- [x] P2 (`engine-rop`): exact fixed-background 2D cells, facets, coverage,
  singular strata, ambiguity, provenance-preserving labels, and point queries.
- [x] P3 (`backend-runtime`, `atlas`, `web-workspace`): inline v1 API, RPB2,
  migration-safe SQLite artifact storage, and a fail-closed standalone viewer.
  Jobs, chunked storage, and native Workspace integration remain deferred.
- [x] P4 (`atlas`): versioned regular-cell invariants, an explicit eight-record
  demo Atlas, normalized signature persistence, and queries whose zero result is
  scoped to the declared/evaluated finite corpus.
- [x] P5 (`engine-rop`, `backend-runtime`): affine input charts, sampled
  finite-grid integrability and curvature diagnostics, an explicit synergy
  policy, regular-extension continuity certification, and intact MIMO Clarke
  generators. See [decision 0006](0006-multi-input-ro-field-research-roadmap.md).
- [ ] Add asynchronous jobs and content-addressed chunk manifests before raising
  the 4 MiB inline boundary.
- [ ] Add adaptive sparse sampling and explicit server-generated 2D slices for
  fields whose input rank is greater than two.
- [ ] Generalize exact geometry beyond two conserved-total axes only after a
  representation, incidence algorithm, and storage budget are independently
  contracted.
- [ ] Run a separately authorized corpus campaign before making any network-space
  prevalence, absence, minimality, or impossibility claim.
