---
module: rop-shape-optimization
status: implemented-working-tree
verified_against: working-tree-2026-07-11
---

# Fixed-topology ROP shape optimization

## Purpose

The earlier workflow tried three hand-chosen changes to a response curve and
kept the best one that could be built and replayed. This module answers a
stricter question: for one pinned network and one pinned reaction-order
program, how far can the requested shape move before the declared path geometry
or parameter bounds stop it?

The result keeps two facts separate. **Parameter margin** is the remaining room
for biochemical log-parameters after the requested operating points are fixed.
**Replay** is a fresh finite equilibrium scan that checks whether the returned
parameters actually show the requested sampled two-peak phenotype. A geometric
optimum can exist while replay fails; that is a reported result, not a pass.

## Non-goals

- Discovering or changing topology.
- Proving optimality over networks, paths, cells, or bounds omitted by the
  declared population or work budget.
- Treating a reaction-order witness span as an exact nonlinear peak width,
  prominence, or experimental phenotype.
- Treating a failed or partial replay as a verified curve.
- Interpreting LP duals as derivatives with respect to biochemical parameters.
- Certifying biological robustness, dynamics, manufacturability, or an
  unrestricted absence result.

## Owner paths

- Mathematical LP core:
  [`rop_shape_optimization.jl`](../../webapp/src/rop_shape_optimization.jl)
- Fixed-topology request compiler, population construction, result artifact,
  and HTTP handler:
  [`rop_shape_api.jl`](../../webapp/src/rop_shape_api.jl)
- Sampled finite-curve verifier:
  [`rop_shape_replay.jl`](../../webapp/src/rop_shape_replay.jl)
- Finite-window cell construction and parameter/augmented margin separation:
  [`designability_feasible_regions.jl`](../../webapp/src/designability_feasible_regions.jl)
- Design Screen reference handoff:
  [`designability.jl`](../../webapp/src/designability.jl)
- Canonical request and response schemas:
  [`rop-shape-optimize-request.schema.json`](../../schemas/rop-shape-optimize-request.schema.json),
  [`rop-shape-optimization.schema.json`](../../schemas/rop-shape-optimization.schema.json)
- Jobs and synchronous admission:
  [`jobs.jl`](../../webapp/src/jobs.jl),
  [`sync_work_budget.jl`](../../webapp/src/sync_work_budget.jl)
- Agent and browser consumers:
  [`design_agent.py`](../../webapp/scripts/design_agent.py),
  [`engine_client.py`](../../webapp/scripts/engine_client.py),
  [`rop-shape-render.js`](../../webapp/public/js/rop-shape-render.js),
  [`design-target.js`](../../webapp/public/js/node-types/design-target.js),
  [`rop-shape.js`](../../webapp/public/js/node-types/rop-shape.js),
  [`port-types.js`](../../webapp/public/js/port-types.js), and
  [`node-schema.js`](../../webapp/public/js/node-schema.js)

## Inputs and entry points

The only HTTP entry point is `POST /api/v1/rop_shape_optimize`. There is no
bare `/api/rop_shape_optimize` compatibility alias. The same request may run as
the local job kind `rop_shape_optimize` with cooperative cancellation.

The v1 request pins:

- a complete `NetworkIR`, or a complete supported legacy network that Julia
  canonicalizes;
- the expected network hash when it is already known;
- one exact finite-window `DesignabilitySpec` and reaction-order program;
- a reference realization, its hash, operating points, parameters, and path
  identity;
- one typed edit: `broaden`, `separate`, `widen_center`, `translate_group`, or
  `linear_witness`;
- the effect tolerance, minimum parameter margin, finite work budget, and
  mandatory stored replay policy.

Unknown fields, stale hashes, ambiguous radius names, non-finite values,
invalid operating points, and noncanonical evidence endpoints fail closed.
Julia is the only owner of network canonicalization and numerical lowering.

## Solver contract

For every eligible fixed-topology path and regime-witness choice, the compiler
constructs the existing augmented linear cell in background `log10(q,K)`
coordinates and finite-window operating-point coordinates. Typed edits become
linear witness constraints or objectives.

The solve is global epsilon-lexicographic over the supplied cells:

1. maximize the declared edit effect while enforcing the minimum parameter
   margin;
2. retain all cells within `effect_tolerance` of the best effect;
3. among those cells, maximize the conditional parameter-only margin;
4. use effect as the deterministic tie-breaker.

The primary limit is a support value of a closed polyhedron. The selected
realization may be pulled inside that limit to retain the requested margin, so
the response reports `closure_support_improvement` and `selected_improvement`
separately.

The conditional parameter margin fixes the selected operating points and takes
the Euclidean Chebyshev radius only in the equality-feasible background
`log10(q,K)` subspace. Its response carries the basis, affine dimension,
equality rank, and basis matrix. A zero-dimensional parameter subspace has
margin zero by convention. The joint augmented radius is retained only as a
separate diagnostic and cannot support a biochemical parameter-margin claim.

Directional ranges use the caller's raw direction vector and alpha units; the
vector is not normalized. The response retains its L2 norm and the per-cell and
union intervals, including open-ended endpoints.

Active rows retain stable row identity. A reported `shadow_price` is objective
sensitivity to the compiled constraint right-hand side under the solver's sign
convention. It is not a derivative with respect to a biochemical parameter, and
degenerate solutions may omit a stable dual interpretation.

## Coverage and status semantics

Each result names eligible and evaluated path and cell populations, replay
candidate and executed counts, truncation, and truncation reasons.

- `global_optimal_over_declared_cells` is allowed only when the declared
  eligible population was completely evaluated.
- A truncated result is best only over evaluated cells; omitted cells remain
  unknown.
- `require_exhaustive=true` turns budget truncation into a failure rather than a
  weaker success label.
- Infeasible means infeasible over the completely evaluated declared
  population, never over other topologies or bounds.
- Unbounded and unresolved numerical statuses remain distinct from infeasible.

The request accepts at most 2,000 paths. Synchronous HTTP work accepts at most
256 cells and two replays; local job work accepts at most 10,000 cells and 16
replays. The current optimizer replays the selected realization once, but the
larger job cap is reserved in the request contract for bounded extensions.

Before exact population construction, every local job checks a 100,000-entry
upper bound on the Cartesian product of regime candidates. SISO construction
then applies hard materialization bounds of 2,000 paths and 200,000 cumulative
path nodes. These pre-materialization bounds are independent of the user's
`max_paths`/`max_cells`: those fields select coverage only after a finite path
population exists. Crossing a hard bound rejects the job and leaves the omitted
population unknown; it is not an infeasibility conclusion.

## Replay and evidence boundary

The optimizer always builds an executable `POST /api/v1/placer_curve` replay
request for the selected parameters. Replay requires 11--1,000 samples in a
declared window inside `[-20,20]`, stores the curve, and accepts only a complete
finite validity vector. The current verifier checks two sampled peaks,
prominence, separation, half-prominence widths, and the central interval.

HTTP placer calls retain the interactive five-reaction/model/cost policy. ROP
local-job replay selects an explicit asynchronous policy instead: it may replay
a larger model (including six reactions) while retaining the 1,000-sample,
finite-window, regime-candidate, solve-cost, path-population, and cooperative
cancellation boundaries.

Exact path geometry and sampled finite replay remain separate fields and
evidence grades. A replay pass verifies only the declared finite grid and
metric. It does not turn the witness objective into an exact nonlinear feature
objective or an experimental result.

## Design Screen, Agent, and browser boundary

An exact finite-window Design Screen recommendation may carry a pinned
`optimization_handoff_template` with no guessed edit. The Design Target node
emits a separate versioned `ROPShapeReferenceArtifact` only after the card,
finite-window spec, network, reference, hashes, method, and canonical v1 route
agree. Proxy, legacy, ambiguous, or identity-inconsistent cards clear that
output. A Design Screen `min_chebyshev_radius` is preserved as screen evidence
but projected into the handoff's unambiguous
`optimization.minimum_parameter_margin`; it is not copied into the optimizer
spec. The edit-config node may strengthen that floor but fails closed if a
workspace value would weaken it. The edit-config node authors one typed intent
and bounded synchronous policy; a result node rebuilds the request from the
current pinned reference, calls the canonical route, and renders the strict
evidence view.
Saved results are labelled as restored historical artifacts until rerun. The
browser never inserts an optimizer result into `verified_recommendations`.

The Design Agent exposes one allow-listed `optimize_rop_shape` tool. Python
validates and copies the backend evidence; it does not rebuild matrices,
measure peaks, or recalculate geometry. It admits a successful optimization
card only when fixed-topology identity, complete population semantics,
parameter-only basis, result hashes, and the canonical complete replay contract
all agree. Provider language quality remains unverified.

## Tests and benchmark

- [`rop_shape_optimization_contract.jl`](../../webapp/test/rop_shape_optimization_contract.jl):
  LP fixtures, lexicographic union selection, zero-dimensional margin,
  direction units, statuses, active rows, and cancellation.
- [`rop_shape_api_contract.jl`](../../webapp/test/rop_shape_api_contract.jl):
  normalization, typed edits, canonical route, population coverage, artifacts,
  replay, jobs, candidate/path hard bounds, deep cancellation, handoff, budgets,
  and fail-closed cases.
- [`rop_shape_schema_contract.jl`](../../webapp/test/rop_shape_schema_contract.jl):
  official Draft 2020-12 request/response instances and negative cases.
- [`rop_shape_replay_contract.jl`](../../webapp/test/rop_shape_replay_contract.jl):
  complete, partial, malformed, and two-peak metric behavior.
- [`rop_shape_cat_benchmark.jl`](../../webapp/test/rop_shape_cat_benchmark.jl):
  frozen cat-network fixture, replay, population/result lock, and artifact hash.
- Browser and Agent contracts exercise strict consumption without independent
  numerical lowering.
- [`design-target-rop-shape-reference.test.mjs`](../../webapp/test/design-target-rop-shape-reference.test.mjs)
  and [`rop-shape-node-contract.test.mjs`](../../webapp/test/rop-shape-node-contract.test.mjs)
  lock the workspace producer/config/result chain, strict port artifacts,
  fail-closed request construction, and restored-result evidence label.

The tracked benchmark is
[`cat_fixed_topology.json`](../../benchmarks/rop_shape_control/cat_fixed_topology.json)
with result
[`cat_fixed_topology_results.json`](../../benchmarks/rop_shape_control/cat_fixed_topology_results.json).
It compares the former three-candidate screen, cell-wise directional interval
oracle, and direct optimization. The benchmark is illustrative evidence for one
pinned network, not a performance or chemistry-wide completeness claim.

## Known gaps

- Only fixed topology and supported SISO finite-window reaction-order programs
  are implemented.
- The typed objectives are witness-space surrogates. The cat benchmark contains
  one deliberate observed mismatch: the `widen_center` geometric optimum does
  not pass the sampled nonlinear replay.
- No iterative Agent feedback optimizer for residual nonlinear mismatch is
  implemented.
- LP duals can be unstable or nonunique at degeneracy; no perturbation study is
  attached automatically.
- The parameter margin uses an unweighted Euclidean norm in log coordinates;
  class-specific uncertainty weights are future work.
- No live LLM provider, cloud job, Slurm worker, production deployment, or
  browser-to-live-engine optimizer session is established by this module. The
  browser workspace has focused contracts and a local UI smoke, not a released
  cross-process end-to-end result.

## Change protocol

1. Change typed intent, compiler, schemas, Julia core/API tests, Python
   admission, and browser rendering as one contract change.
2. State the coordinate basis and generating layer for every new numeric field.
3. Preserve exact geometry, bounded coverage, selected realization, and sampled
   replay as separate evidence layers.
4. Add both a closed-form LP fixture and a production-cell regression for new
   solver semantics.
5. Re-run the full Julia, JavaScript, Python, schema, and repository gates;
   regenerate benchmark output only through its checked-in command.

## Verified against

- Implementation state: current uncommitted working tree on 2026-07-11. The
  completion report records exact local commands and results; no commit or
  remote CI run is claimed.
- Historical base: `1177a3d` remains the last committed implementation evidence
  anchor in the maintained status page.
- Scope: exact only over the declared fixed-topology path/cell population and
  bounds; finite replay remains sampled and biological/external validation is
  unknown.
