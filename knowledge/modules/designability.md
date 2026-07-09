---
module: designability
status: verified
verified_against: f9c65a5
---

# Designability specification and screening

## Purpose

Translate an explicit design target into auditable supported and unsupported
clauses, find matching catalogue candidates, and promote a candidate to a
verified recommendation only when the declared constraints have exact or
clearly labeled sampled evidence.

The core contrast is between “this topology appears promising” and “this
topology has a parameter region or checked response that satisfies the target.”
`screened_candidates` answers the first question; `verified_recommendations`
answers the second.

## Non-goals

- Presenting proxy catalogue scores as proof.
- Ignoring a hard clause because the active solver cannot enforce it.
- Certifying temporal dynamics with an equilibrium model.
- Claiming exactness for sampled dose-response shape or dynamic range.

## Owner paths

- Spec normalization, audit, catalogue screening, evidence grading, and response:
  [`webapp/src/designability.jl`](../../webapp/src/designability.jl)
- Exact feasible-region and sampled-forward calculations:
  [`webapp/src/designability_feasible_regions.jl`](../../webapp/src/designability_feasible_regions.jl)
- Request schema: [`schemas/designability-spec.schema.json`](../../schemas/designability-spec.schema.json)
- Response schema: [`schemas/designability-screen.schema.json`](../../schemas/designability-screen.schema.json)
- Backend endpoints: [`webapp/src/BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
- Frontend design-spec and target nodes: [`webapp/public/js/node-types/design-target.js`](../../webapp/public/js/node-types/design-target.js)

## Inputs

- `DesignabilitySpec` with source, target, optional constraints, candidate
  budget, ranking policy, and audit policy.
- A legacy sign/label/exact target only through the explicit legacy target
  wrapper.
- Structured reaction-order behavior programs with input/output symbols and
  optional finite input window.
- Declared parameter, network, robustness, transition, dynamic-range, output
  feature, and shape clauses from the supported schema.

## Outputs

- A normalized spec plus JSON-pointer constraint audit.
- `minimal_certificates` for qualitative catalogue realizability.
- `screened_candidates`, always downgraded to proxy-only evidence.
- `verified_recommendations` containing exact feasible-region evidence and,
  when requested and supported, labeled sampled-forward evidence.
- A screen summary that distinguishes verified, screened-only, blocked, and
  not-designable outcomes.

## Contract sources

- Input version and allowed shapes:
  [`designability-spec.schema.json`](../../schemas/designability-spec.schema.json)
- Runtime semantic support and fail-closed audit:
  [`designability.jl`](../../webapp/src/designability.jl)
- Exact SISO ROP polyhedra, finite-window witnesses, and forward scans:
  [`designability_feasible_regions.jl`](../../webapp/src/designability_feasible_regions.jl)
- Output evidence vocabulary:
  [`designability-screen.schema.json`](../../schemas/designability-screen.schema.json)

## Tests

- [`webapp/test/designability_spec_contract.jl`](../../webapp/test/designability_spec_contract.jl):
  schema normalization, unknown keys, ambiguous targets, support audit, hard vs
  soft clauses, and endpoint behavior.
- [`webapp/test/design_screen_contract.jl`](../../webapp/test/design_screen_contract.jl):
  verified/proxy separation, exact parameter placement, finite windows,
  transitions, robustness, sampled features/shapes, and fail-closed cases.
- Both are included at the start of
  [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl).
- [`webapp/test/design-spec-node-contract.test.mjs`](../../webapp/test/design-spec-node-contract.test.mjs)
  and [`webapp/test/design-target-render.test.mjs`](../../webapp/test/design-target-render.test.mjs)
  cover frontend contract assembly/rendering.
- [`webapp/scripts/test_design_agent_contract.py`](../../webapp/scripts/test_design_agent_contract.py)
  covers the agent handoff boundary.

## CI

The main Julia CI suite runs both designability contract files. JavaScript and
Python jobs exercise frontend and agent consumers. The hand-authored
designability schemas are not generated from Julia structs, so semantic changes
must update schema and runtime/tests together.

## Invariants

- Only `bne-designability/v1.0.0` is accepted by the structured normalizer.
- Unknown or malformed nested fields produce explicit audit items; target forms
  that would shadow or bypass each other are rejected or blocked.
- Any unsupported hard clause empties verified recommendations. Soft unsupported
  clauses may remain in the audit but never become evidence.
- Screened catalogue cards have `pass=false`, `proxy_only` evidence, and no
  inherited exact parameter claims.
- Exact recommendations require a nonempty SISO ROP feasible region intersecting
  the effective declared bounds and passing requested exact thresholds.
- Finite-window programs use augmented path polyhedra with explicit input
  witnesses. Transition order and spacing are enforced only when their audit
  records say `enforced_exact`.
- Dynamic range, output features, and shape checks are labeled sampled-forward;
  invalid or floor-limited samples cannot pass.
- Temporal-dynamics clauses are unsupported by the equilibrium backend and are
  never silently approximated.
- Ranking applies only to verified fields whose support was audited; proxy
  values cannot win a verified ranking.

## Known gaps

- Temporal dynamics has a schema/audit surface but no active solver.
- Qualitative legacy sign/label targets can establish catalogue/minimal
  realizability, but not an exact feasible-region recommendation.
- Finite-window dynamic range, output features, and shapes use sampled forward
  checks rather than exact guarantees over the whole interval.
- Candidate discovery is bounded by the checked-in design index before exact
  placement; an absent candidate is relative to that catalogue and target
  lowering.

## Change protocol

1. Add the clause to the JSON Schema, normalizer, audit vocabulary, solver or
   verifier, response schema, and UI/agent handoff as one contract change.
2. Decide and test its evidence level: unsupported, exact, or sampled-forward.
   Never default a new clause to “supported.”
3. Add success, malformed, unsupported-hard, unsupported-soft, and no-evidence
   cases to the dedicated Julia contracts.
4. Run the full Julia suite plus frontend and Python agent contracts; regenerate
   any example artifacts that embed the normalized spec or response.
5. Bump schema/evidence vocabulary when existing consumers could misinterpret
   the changed meaning.

See [data provenance](../architecture/data-provenance.md) and
[atlas card](atlas.md).

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: input/output schemas, runtime normalizer and solvers,
  Julia contracts, frontend/Agent consumers, and CI workflow wiring.
- Boundary: exact and sampled evidence retain their declared scopes; no
  temporal-dynamics or catalogue-completeness guarantee is claimed.
