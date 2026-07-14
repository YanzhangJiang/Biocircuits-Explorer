# 0002: Separate shape limits, parameter margin, and finite replay

- Status: accepted in the 2026-07-11 working tree
- Verified against: `f2ca13c`
- Historical implementation anchor: `1177a3d`
- Owners: `rop-shape-optimization`, `designability`, `backend-runtime`
- Supersedes: implicit use of one finite-window Chebyshev radius for both
  request-location and biochemical-parameter claims
- Superseded by: none

## Problem in plain language

A response can have plenty of room to move its operating points while its
biochemical parameters sit close to failure, or the opposite. One radius over
both kinds of coordinates cannot say which room the design actually has.
Likewise, a linear reaction-order construction can be feasible even when a
finite nonlinear curve does not show the desired visual feature strongly
enough.

The decision is to report three different facts with different owners: the
largest edit allowed by the path geometry, the remaining biochemical parameter
room after selecting the edit, and a fresh finite replay of the returned
parameters. None may silently stand in for another.

## Context

Finite-window designability cells combine background `log10(q,K)` coordinates
with witness input locations. The former choose biochemical parameters; the
latter locate reaction-order changes inside the input window. The earlier
finite-window placement took a Chebyshev center in this augmented space and
then exposed a radius next to background-parameter volume calculations.

A rectangle with parameter width 100 and witness width 1 makes the problem
concrete: the joint Euclidean radius is 0.5, while the parameter-only radius is
50. The narrower witness coordinate must not make a broad biochemical region
look fragile.

The direct shape optimizer also maximizes a linear witness objective on a
closed cell. The mathematical support point can lie on a boundary with no
useful interior parameter room. The implementation therefore needs both the
closed support limit and a nearby selected realization with an explicit minimum
margin.

Finally, reaction-order witness distances are not the same quantities as
sampled nonlinear peak widths or prominence. A fresh solver replay is an
independent verifier, not a way to redefine the geometric optimum after the
fact.

## Decision

### Parameter margin has one coordinate basis

`parameter_chebyshev_radius` is the Euclidean Chebyshev radius in the
equality-feasible background `log10(q,K)` subspace after the selected witness
locations are fixed. Every reusable result also records:

- `parameter_margin_basis`;
- the affine dimension and equality rank; and
- the basis matrix used for the subspace coordinates.

A zero-dimensional parameter subspace has radius zero. It is a unique or fully
equality-pinned parameter point, not an infinite-radius design.

The finite-window compatibility field `chebyshev_radius` carries the
parameter-only value. New code and schemas use the explicit field. The
parameter-volume lower bound uses the parameter affine dimension and this
parameter-only radius.

### Augmented radius is diagnostic only

`augmented_chebyshev_radius` remains the radius in the joint background and
witness-input coordinate space. It may describe joint interior geometry, but it
cannot support a biochemical robustness or tunable-volume claim. Consumers must
not compare it with a parameter-only threshold without an explicit coordinate
conversion and new contract.

### Effect limit and selected realization are distinct

For a typed witness edit, `closure_support_improvement` is the best objective
value on the closed compiled cell union. The global secondary solve may choose
an epsilon-near interior point to maximize parameter margin; its achieved value
is `selected_improvement`. Both are retained.

The lexicographic policy is global across all supplied cells: first find the
best effect, then maximize parameter margin among every cell within the declared
effect tolerance. A cell-local secondary solve followed by primary-only cell
selection is not equivalent and is rejected by contract tests.

### Coverage controls global language

`global_optimal_over_declared_cells` is valid only when all eligible paths and
cells under the declared topology, program, bounds, and construction policy
were evaluated. Truncation changes the status to a best-over-evaluated claim;
omitted cells remain unknown. `require_exhaustive=true` fails instead of
returning that weaker label.

Infeasibility and unboundedness retain the same declared-population scope. No
status is generalized to other topologies, chemistries, parameter bounds, or an
unbounded path construction.

### Replay remains a separate sampled evidence layer

The selected parameters are sent through a complete finite
`POST /api/v1/placer_curve` request. A visual claim requires every requested
sample to be finite and valid plus all declared sampled metrics to pass. Partial
or failed replay leaves the exact path-geometric result intact but withholds a
verified finite-shape claim.

Replay does not feed back into the first production solve. Iterative nonlinear
refinement, if added later, must retain every request, geometry result, replay,
and revision reason as a separate trace.

### Duals keep their actual meaning

An active-row `shadow_price` is the local objective derivative with respect to
that compiled row's right-hand side under the LP solver convention. It is not a
derivative with respect to a biochemical parameter or a finite replay metric.
Degenerate rows may have nonunique or unstable duals, so absence of a reported
dual is not absence of a limiting facet.

## Alternatives considered

### Keep one generic radius

This was rejected because its value depends on whether witness inputs are free,
fixed, or scaled. The same label would change scientific meaning across solver
modes and could enter an invalid parameter-volume calculation.

### Maximize edit only

This was rejected as the default because a support point can have negligible
parameter room. The support limit is still reported, but the selected
realization comes from the declared effect-then-margin policy.

### Let finite replay choose among geometric candidates

This would reproduce the earlier outer candidate screen and make the final
request depend on downstream sampled outcomes. It remains a valid explicitly
labeled benchmark baseline, but it is not the direct optimizer's mathematical
contract.

### Treat witness spans as exact peak features

This was rejected because finite curves can round, suppress, or shift the
asymptotic reaction-order structure. The frozen cat benchmark observes this
failure for the `widen_center` optimum.

## Consequences

### Benefits

- Parameter robustness claims now have a named basis and dimension.
- The joint request/parameter geometry remains available without being
  mislabeled as biochemical room.
- Boundary optima and selected robust realizations can be compared directly.
- Truncation cannot acquire a false global label.
- A replay mismatch becomes usable model evidence instead of a hidden failure.
- The Agent and browser can explain limiting rows without reimplementing the
  numerical compiler.

### Costs and risks

- Existing consumers of generic `chebyshev_radius` must migrate to the explicit
  fields before comparing solver modes.
- The unweighted Euclidean log-coordinate norm is a modeling choice, not a
  calibrated uncertainty distribution.
- Lexicographic optimization solves more LPs than an effect-only pass.
- Dual sensitivity can be fragile at degenerate optima.
- Complete finite replay adds solver cost and remains grid-dependent.

## Verification

The decision is covered by:

```text
julia --project=webapp webapp/test/rop_shape_optimization_contract.jl
julia --project=webapp webapp/test/rop_shape_api_contract.jl
julia --project=webapp webapp/test/design_screen_contract.jl
julia --project=webapp webapp/test/rop_shape_cat_benchmark.jl
```

The core contract includes the 100-by-1 rectangle regression, equality-subspace
and zero-dimensional fixtures, global epsilon selection, direction units,
active-row semantics, and solver statuses. The production-cell contract checks
the same radius distinction through the finite-window engine path. Schema,
Agent, browser, and repository gates protect downstream meanings.

## Follow-ups

- [ ] Define weighted parameter norms only with a versioned uncertainty model.
- [ ] Add dual perturbation diagnostics if active-row sensitivity becomes a
  decision-critical user feature.
- [ ] Add an auditable nonlinear feedback loop for residual replay mismatch;
  do not overwrite failed iterations.
- [ ] Evaluate more fixed topologies before making performance or generality
  claims beyond the frozen cat fixture.
