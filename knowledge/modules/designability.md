---
module: designability
status: implemented-working-tree
verified_against: working-tree-2026-07-11
---

# Designability specification and screening

## Purpose

A promising catalogue match is not the same as a design that passed the
declared checks. This module preserves that difference: it can first identify
candidate topologies, then promote only candidates with complete exact or
clearly labeled sampled evidence.

`screened_candidates` answer “what should we inspect?”
`verified_recommendations` answer “what passed the supported constraints?”

## Non-goals

- Presenting proxy catalogue scores as proof.
- Ignoring a hard clause because the active solver cannot enforce it.
- Certifying temporal dynamics with an equilibrium model.
- Claiming exactness for sampled dose-response shape or dynamic range.
- Treating an unevaluated candidate in a truncated screen as a failure.
- Filling a non-converged numerical gap to make a placement pass.

## Owner paths

- Spec normalization, audit, catalogue screening, evidence grading, and
  response: [`designability.jl`](../../webapp/src/designability.jl)
- Exact feasible-region and sampled-forward calculations:
  [`designability_feasible_regions.jl`](../../webapp/src/designability_feasible_regions.jl)
- Fixed-topology shape optimization is a separate downstream owner; see
  [`rop-shape-optimization.md`](rop-shape-optimization.md).
- Request schema:
  [`designability-spec.schema.json`](../../schemas/designability-spec.schema.json)
- Response schema:
  [`designability-screen.schema.json`](../../schemas/designability-screen.schema.json)
- Placement verification:
  [`parameter_placement.jl`](../../webapp/src/parameter_placement.jl) and
  [`parameter_level.jl`](../../webapp/src/parameter_level.jl)
- Backend handler assembly:
  [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
- Frontend design-spec and target nodes:
  [`design-target.js`](../../webapp/public/js/node-types/design-target.js)

## Inputs

- `DesignabilitySpec` with source, target, optional constraints, candidate
  budget, ranking policy, and audit policy.
- A legacy sign/label/exact target only through the explicit legacy target
  wrapper.
- Structured reaction-order behavior programs with input/output symbols and an
  optional finite input window.
- Declared parameter, network, robustness, transition, dynamic-range, output
  feature, and shape clauses from the supported schema.

## Outputs

- A normalized spec plus JSON-pointer constraint audit.
- `minimal_certificates` for qualitative catalogue realizability.
- `screened_candidates`, always downgraded to proxy-only evidence.
- `verified_recommendations` containing exact feasible-region evidence and,
  when requested and supported, labeled sampled-forward evidence.
- For a pinned exact finite-window recommendation, a hash-bound fixed-topology
  reference and an `optimization_handoff_template` whose edit is intentionally
  unset until a user or Agent supplies typed intent.
- A screen summary that distinguishes verified, screened-only, blocked, and
  not-designable outcomes.

## Design Screen v0.3 coverage

The current response identity is `bne-design-screen/v0.3.0`. Version 0.3 makes
the bounded synchronous prefix explicit:

| Field | Meaning |
|---|---|
| `eligible_count` | deduplicated catalogue records eligible before the synchronous evaluation limit |
| `evaluated_count` | records actually evaluated in this request |
| `screened_count` | compatibility alias for the evaluated count |
| `truncated` | true when `eligible_count > evaluated_count` |

The same coverage values appear in `screen_summary`. They describe work done,
not scientific success. `n_matches` and recommendation lists keep their own
meaning.

The synchronous limits are 64 for `max_screened`,
`max_verified_recommendations`/`max_recommended`, and `max_near_misses`, and 8
for `max_exact_placements`. A request above a maximum returns the standard 422
`sync_budget_exceeded` payload. The implementation does not accept a large
budget and silently pretend every eligible candidate was evaluated.

Within the explicit `max_exact_placements` prefix, a candidate whose SISO
regime paths exceed the Web materialization limit remains a screened proxy and
is skipped for exact promotion. That candidate-local limit does not abort later
bounded candidates; it also does not turn the skipped record into a failure or
verified recommendation.

Finite-window witness choices have a separate per-candidate limit of 256 exact
cells. The solver first computes the complete Cartesian-product count with
overflow-safe integer arithmetic, before it creates any witness tuple,
augmented matrix, polyhedron, or LP. Products are iterated lazily only after the
whole declared population fits. An over-limit candidate remains unevaluated and
cannot receive an exact certificate; later candidates in the bounded prefix may
still be evaluated.

A v0.2-only renderer may display `screened_count` for a historical artifact,
but it cannot infer truncation. Current consumers require v0.3 and display
eligible, evaluated, and truncation together.

## Parameter margin and shape-optimization handoff

Finite-window cells now name two different radii. The
`parameter_chebyshev_radius` fixes the selected witness inputs and measures
only the equality-feasible background `log10(q,K)` subspace. The
`augmented_chebyshev_radius` measures the joint parameter/witness space and is
diagnostic only. The compatibility `chebyshev_radius` and tunable-volume lower
bound use the parameter-only value and its affine dimension. A zero-dimensional
parameter subspace has margin zero.

An exact finite-window card may advertise that its pinned topology is ready for
shape optimization. This handoff is not itself an optimizer result and is not a
new member of `verified_recommendations`. The downstream v1 optimizer must
match the network, reference, spec, and hashes, and must report its own path/cell
coverage and finite replay evidence. The screen's parameter-only
`min_chebyshev_radius` remains in the screen spec/audit, while the generated
optimizer handoff carries the same positive floor only as
`optimization.minimum_parameter_margin`; downstream clients cannot lower it.
Full semantics are owned by the
[shape-optimization module](rop-shape-optimization.md) and
[decision 0002](../decisions/0002-rop-shape-margin-and-evidence.md).

## Complete-evidence rule

Sampled-forward evidence is usable only when every required numerical point
converged and produced a finite value. A failed solve or `NaN` is an invalid gap,
not zero response.

- Dose-response and placement curves carry a `valid` vector and `partial`.
- Threshold and realized-program checks carry `verification_validity` and
  `verification_partial`.
- A transition is detected only between two valid neighboring points.
- A partial realized-program curve is not segmented into a passing program.
- Dynamic-range, output-feature, and shape checks reject invalid or floor-limited
  samples.
- Parameter-level solving fails closed when the equilibrium output is not finite
  and converged.

Partial curves may be returned for diagnosis, but they cannot produce a verified
card, satisfy a hard constraint, or win a ranking. This rule is stronger than
“some valid points exist.” Complete validity is required over the evidence used
by the clause.

## Invariants

- Only `bne-designability/v1.0.0` is accepted by the structured normalizer.
- Unknown or malformed nested fields produce explicit audit items; target forms
  that would shadow or bypass one another are rejected or blocked.
- Any unsupported hard clause empties verified recommendations. Soft unsupported
  clauses may remain in the audit but never become evidence.
- Screened catalogue cards have `pass=false`, `proxy_only` evidence, and no
  inherited exact parameter claims.
- Exact recommendations require a nonempty SISO ROP feasible region intersecting
  the effective declared bounds and passing requested exact thresholds.
- Exact finite-window recommendation requires the complete witness-cell
  population to fit the 256-cell preflight. A solved prefix is never promoted
  as an exact union.
- Finite-window programs use augmented path polyhedra with explicit input
  witnesses. Transition order and spacing are enforced only when their audit
  records say `enforced_exact`.
- Dynamic range, output features, and shape checks are labeled
  sampled-forward; invalid or floor-limited samples cannot pass.
- Temporal-dynamics clauses are unsupported by the equilibrium backend and are
  never silently approximated.
- Ranking applies only to verified fields whose support was audited; proxy,
  unevaluated, or partial values cannot win a verified ranking.
- `recommended` is a transitional alias for `verified_recommendations`, never
  for `screened_candidates`.
- Exact verified cards carry an executable `/api/v1/placer_curve` handoff with
  the selected reaction rules, candidate-specific I/O, Kd values, and totals.
  Proxy cards never carry this request. A consumer must still execute the
  handoff and require a complete finite curve before displaying a fresh result.
- Only exact finite-window cards with a complete pinned reference may carry an
  `/api/v1/rop_shape_optimize` template. The template cannot carry a guessed
  edit or imply that optimization/replay has already passed.

## Contract sources and tests

- Input version and allowed shapes:
  [`designability-spec.schema.json`](../../schemas/designability-spec.schema.json)
- Runtime semantic support and fail-closed audit:
  [`designability.jl`](../../webapp/src/designability.jl)
- Exact SISO ROP polyhedra, finite-window witnesses, and forward scans:
  [`designability_feasible_regions.jl`](../../webapp/src/designability_feasible_regions.jl)
- Output evidence vocabulary and v0.3 counts:
  [`designability-screen.schema.json`](../../schemas/designability-screen.schema.json)
- [`designability_spec_contract.jl`](../../webapp/test/designability_spec_contract.jl):
  schema normalization, unknown keys, ambiguous targets, support audit, hard vs
  soft clauses, and endpoint behavior.
- [`designability_cell_budget_contract.jl`](../../webapp/test/designability_cell_budget_contract.jl):
  lazy witness products, overflow-safe counts, pre-materialization rejection,
  and Design Screen wiring of the fixed exact-cell limit.
- [`design_screen_contract.jl`](../../webapp/test/design_screen_contract.jl):
  v0.3 coverage counts, verified/proxy separation, exact parameter placement,
  finite windows, transitions, robustness, sampled features/shapes, invalid
  samples, and fail-closed cases.
- [`design-target-render.test.mjs`](../../webapp/test/design-target-render.test.mjs):
  browser rendering of eligible/evaluated/truncated coverage and verified
  evidence.
- [`rop_shape_api_contract.jl`](../../webapp/test/rop_shape_api_contract.jl) and
  [`design_screen_schema_instance_contract.jl`](../../webapp/test/design_screen_schema_instance_contract.jl):
  hash-bound optimization handoff, explicit radius fields, and schema-instance
  compatibility.

Both dedicated Julia contracts run at the start of the main backend suite.
JavaScript and Python jobs exercise frontend and agent consumers. The
hand-authored designability schemas are not generated from Julia structs, so
semantic changes must update schema, runtime, tests, and renderer together.

## Known gaps

- Temporal dynamics has a schema/audit surface but no active solver.
- Qualitative legacy sign/label targets can establish catalogue/minimal
  realizability, but not an exact feasible-region recommendation.
- Finite-window dynamic range, output features, and shapes use sampled forward
  checks rather than exact guarantees over the whole interval.
- Candidate discovery is bounded by the checked-in design index before exact
  placement; an absent candidate is relative to that catalogue and target
  lowering.
- `truncated=true` reports bounded evaluation but is not a continuation-token or
  pagination contract.
- The optimization handoff is fixed-topology only and does not make the
  catalogue complete. Nonlinear shape optimization beyond the downstream
  sampled replay is not owned by Design Screen.

## Change protocol

1. Add a clause to the JSON Schema, normalizer, audit vocabulary, solver or
   verifier, response schema, and UI/agent handoff as one contract change.
2. Decide and test its evidence level: unsupported, exact, or sampled-forward.
   Never default a new clause to “supported.”
3. Add success, malformed, unsupported-hard, unsupported-soft, invalid/partial,
   and no-evidence cases to the dedicated Julia contracts.
4. Run the full Julia suite plus frontend and Python agent contracts; regenerate
   example artifacts that embed the normalized spec or response.
5. Bump schema/evidence vocabulary when existing consumers could misinterpret
   the changed meaning.

See [data provenance](../architecture/data-provenance.md),
[scientific evidence](../contracts/scientific-evidence.md), and
[atlas card](atlas.md).

## Verified against

- Current implementation: uncommitted working tree on 2026-07-11; exact local
  commands are recorded in the goal completion report. No remote CI run is
  claimed.
- Last committed implementation anchor: `1177a3d`.
- Historical baseline: the proxy/verified and exact/sampled evidence audit at
  `f9c65a5` remains historical context. It predates v0.3 coverage counts,
  synchronous card budgets, and complete-validity placement behavior.
- Boundary: exact and sampled evidence retain their declared scopes; no temporal
  dynamics or catalogue-completeness guarantee is claimed.
