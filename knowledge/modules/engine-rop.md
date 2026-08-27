---
module: engine-rop
status: verified
verified_against: f2ca13c
---

# Engine: binding networks and reaction-order geometry

## Purpose

Turn a valid equilibrium binding network into conserved-coordinate models,
asymptotic regimes, regime graphs, SISO/change paths, reaction-order values,
polyhedra, parameter scans, and equilibrium concentrations. This is the
mathematical engine used by the backend, atlas, and designability solver.

The current runtime distinction is simple: a model may be shared, but a partial
cache build and a failed numerical solve are not valid scientific results.
Regime construction therefore publishes a completion marker under the model's
construction lock, and numerical consumers can request explicit per-sample
validity. The backend adds conservative per-model serialization and Web-only
work caps around these engine contracts.

## Current verified contract

- `find_all_regimes!` holds `_regimes_affine_lock` across its fast-path check and
  the complete regime, graph, affine-cache, and direction build. It sets
  `_regimes_build_complete=true` only at the final commit point; an exception
  clears partial regime state before releasing the lock. An optional
  `cancel_check` reaches candidate enumeration and topology boundaries; a
  cancelled hot-cache read preserves the already-complete shared cache.
- `get_regimes_graph!` always passes through that construction path. A non-null
  `vertices_graph` alone is not treated as proof that concurrent construction
  finished.
- Engine path enumeration accepts optional `max_paths` and
  `max_total_nodes` and raises `PathEnumerationLimitExceeded` before exceeding
  either allocation bound. Its defaults remain `nothing` for direct/offline
  callers.
- The Web adapter supplies 2,000 paths and 200,000 materialized path nodes while
  a synchronous heavy request is active. ROP-shape and Atlas local jobs
  explicitly apply the same values as a hard materialization boundary. Other
  direct/offline engine callers retain the uncapped default unless they opt in.
- Regime and SISO path construction, behavior-family aggregation, and 1D scans
  accept cooperative cancellation callbacks; this includes affine-cache/full
  qK-graph construction, SISO/ChangePaths materialization, and reaction-order
  traversal within a path. Candidate/path loops check at fixed boundaries, and
  1D scans check every requested sample. Default no-cancel calls retain parallel
  reaction-order/affine work; cancellable calls use parent-task checkpoints so
  typed cancellation is not wrapped by threaded task failures.
- One- and two-dimensional scans can use `track_validity=true`. Failed solves
  are marked false and their output samples become `NaN`; the backend exposes
  `valid`/`validity_grid` and `partial` and excludes invalid samples from
  placement/refinement decisions.
- Regime-boundary extraction is a plotting-independent five-point Laplacian
  with replicated edge values. It is loaded in both headless and graphical
  engine modes, so FRET and parameter-scan handlers do not depend on a Makie or
  ImageFiltering include side effect.
- A backend `ModelBundle` lock serializes handlers sharing one mutable engine
  instance. This is intentionally stronger than the engine's verified lock:
  only the regime-build completion path, prewarmed parallel scan reads, and
  covered numerical code have targeted thread-safety evidence.

## Working-tree multi-input RO fields

- [`ro_field.jl`](../../Bnc_julia/src/rop/ro_field.jl) evaluates a bounded
  Cartesian tensor for ordered q/K axes and species outputs. For grid shape
  `[n1,...,nk]` and `m` outputs it returns output shape `[n1,...,nk,m]` and
  reaction-order shape `[n1,...,nk,m,k]`; failed/non-finite points retain a
  false validity entry and `NaN` backing values for the serializer to null.
- [`ro_cell_complex.jl`](../../Bnc_julia/src/rop/ro_cell_complex.jl) constructs
  the complete Float64 polyhedral partition of one declared two-total log box
  at one fixed background. Coincident geometry retains all source regimes and
  all distinct affine labels; singular/lower-dimensional strata and set-valued
  cells remain explicit.
- [`ro_cell_complex_3d.jl`](../../Bnc_julia/src/rop/ro_cell_complex_3d.jl)
  constructs a strict three-dimensional Float64 face lattice by pairwise facet
  common refinement followed by ridge common refinement. It records complete
  cell--facet--ridge--vertex incidence, rank grey zones, domain coverage,
  volume, links, and Euler checks. Before publication it interprets the admitted
  Float64 domain and halfspaces as their bit-exact dyadic rationals and requires
  exact pair dimension/non-overlap, opposing original-H support, support/domain
  plane coverage, and volume equality to agree with the Float64 construction.
  This is not an arbitrary-precision-input, arbitrary-real, or
  arbitrary-dimensional constructor.
- [`ro_stratified_field_3d.jl`](../../Bnc_julia/src/rop/ro_stratified_field_3d.jl)
  rebuilds and validates one publishable explicit-affine D3 complex, interprets
  its Float64 geometry and affine labels as exact dyadic rationals, and checks
  tangential compatibility on a three-point basis of every internal facet,
  induced offset jumps around every dual-graph cotree cycle, and continuity of
  supplied cell potentials. Compatible-gradient existence and continuity of the
  supplied potentials remain separate statuses. The positive result is scoped
  to a complete contractible box and its regular extension; it does not select a
  singular branch or certify a holed domain, `D >= 4`, chemistry extraction, or
  a D3 Clarke query.
- [`ro_coordinate_chart.jl`](../../Bnc_julia/src/rop/ro_coordinate_chart.jl)
  admits a finite, full-column-rank affine map from declared controls to source
  coordinates and applies the chain-rule pullback to complete matrices/tensors.
  Array-valued properties are detached snapshots; an in-memory content seal
  makes lower-level backing-storage mutation fail closed before later public
  use, and the raw constructor independently rechecks admission diagnostics.
- [`ro_nonlinear_coordinate_chart.jl`](../../Bnc_julia/src/rop/ro_nonlinear_coordinate_chart.jl)
  admits a bounded declarative quadratic map from ordered controls to ordered
  source coordinates. The constructor checks only the reference Jacobian;
  every evaluated control point recomputes a numerical SVD admission and rejects
  a fold/non-immersion, rank grey zone, or conditioning grey zone at that point.
  Matrix/tensor pullbacks use the local Jacobian, while the scalar second-order
  path retains both `J' * H_theta * J` and
  `sum_a grad_theta[a] * H_u(theta[a])`. First-order arrays must declare the
  exact source-axis names, unit labels, and evaluated source point, but this
  check cannot establish that the upstream array was labelled honestly. The
  scalar derivative receipt also binds the chart declaration hash and output
  name/unit. Its declared scope is pointwise local immersion and
  `global_injectivity_certified` is always false.
- [`ro_observable_chart.jl`](../../Bnc_julia/src/rop/ro_observable_chart.jl)
  separately admits finite declarative C2 affine or quadratic maps from ordered
  source observables to ordered derived observables. It binds source/output
  order, unit labels, references, a closed finite domain, coefficients, and
  limits, and composes both `R_y = J_phi R_z` and
  `H_y = J_phi H_z + H_phi[R_z,R_z]`. Unit strings are opaque identity labels;
  this layer performs no dimensional algebra or conversion. General ratios,
  log-sums, and other non-polynomial observable maps remain outside its scope.
- [`ro_regular_sheet.jl`](../../Bnc_julia/src/rop/ro_regular_sheet.jl)
  admits square declarative polynomial equilibrium systems in strictly positive
  linear coordinates and treats every Float64 declaration as its exact binary
  rational. It composes the complete affine tube `x=p(u)+delta` into the
  residual polynomial before interval evaluation, differentiates the composed
  polynomial for correlated `F_x` and `F_u+F_x*S` enclosures, proves strict
  parametric Krawczyk inclusion and `beta < 1`, encloses the implicit derivative,
  and can replay a full-dimensional overlap bridge containing both complete
  patch tubes. The positive claim is one root per control and uniqueness inside
  the declared tube, not absence of roots outside it or global branch
  completeness.
- [`ro_regular_root_census.jl`](../../Bnc_julia/src/rop/ro_regular_root_census.jl)
  cumulatively replays a canonical P5r0.1 patch population and exhausts one
  exact-dyadic tensor partition of a declared affine moving state domain. Every
  non-patch cell must exclude zero in at least one exact residual component,
  while every patch tube must map exactly onto one cell, span the complete
  control box, and use the common affine slope. A passed replay proves the
  complete in-domain regular-root count and a complete empty in-domain fold
  set. It neither excludes outside roots nor classifies stability, Hopf events,
  fold-containing domains, global continuation, or hysteresis.
- [`ro_fold_event_census.jl`](../../Bnc_julia/src/rop/ro_fold_event_census.jl)
  covers one declared positive augmented domain for an exactly one-control
  polynomial system with an exact-dyadic tensor partition. Every cell either
  excludes one component of `H=(F,det(F_x))` or proves one strict augmented
  Krawczyk root. A passed replay gives the complete isolated simple-fold set
  inside that domain; it does not attach regular sheets, classify stability, or
  cover multi-control fold sheets.
- [`ro_fold_branch_incidence.jl`](../../Bnc_julia/src/rop/ro_fold_branch_incidence.jl)
  declaratively exchanges one selected state coordinate with the original
  control, covers an isolated event by a central P5r0.1 chart, bridges two local
  half-patches to it, and embeds both half-tubes strictly in original-coordinate
  regular patches. Each event-to-half corridor stays inside the complete fold
  census and excludes every other certified fold enclosure. This proves local
  adjacent-sheet incidence only, not original-control root-count orientation,
  remote component identity, or a global branch graph.
- [`ro_branch_indexed_field.jl`](../../Bnc_julia/src/rop/ro_branch_indexed_field.jl)
  explicitly binds one P5r0 polynomial source as `dx/dt`, cumulatively replays
  every supplied patch and bridge, and turns passed bridges into
  evidence-relative continuation components. Exact row-Gershgorin right bounds
  certify uniform local stability; a positive trace lower bound certifies
  instability; other patches stay unknown. Each patch retains every ordered
  `d log(x_i)/d log(u_j)` column. A multistability witness requires one stable
  patch per component on a common positive box and pairwise strict separation
  of the restricted state tubes, proving only a stable-root lower bound. Stable-
  root population completeness, bifurcation-boundary enclosure, global
  continuation, and true hysteresis are forced false.
- [`ro_spectral_hopf_event_census.jl`](../../Bnc_julia/src/rop/ro_spectral_hopf_event_census.jl)
  requires an explicit `dx/dt` dynamics binding for an exactly one-control
  polynomial system, constructs `P(s)=det(sI-F_x)`, and uses the signed even/odd
  decomposition `P(i*sqrt(z))=E(z)+i*sqrt(z)*O(z)`. It exhausts an exact-dyadic
  tensor cover in `(x,lambda,z)` from `z=0` to strictly beyond an exact uniform
  row-sum spectral bound. A strict augmented Krawczyk root, event-cell
  `det(F_x)` exclusion, and pairwise-separated state/control root projections
  give the complete isolated simple transverse spectral-crossing set inside the
  declared domain. Only a source-bound replayed parent census has authority to
  exclude another frequency; an exported local event hash does not. For this
  P8s1c0 artifact alone, first Lyapunov nondegeneracy, nonlinear Hopf,
  criticality, periodic-orbit incidence, stability completeness, multi-control
  event sheets, and global continuation remain false or unimplemented.
- [`ro_hopf_lyapunov_census.jl`](../../Bnc_julia/src/rop/ro_hopf_lyapunov_census.jl)
  replays the complete P8s1c0 parent before lifting every reported event. It
  derives an outward exact-rational frequency enclosure, validates full complex
  bordered right and Hermitian-adjoint systems after canonical realification,
  separately validates `A^-1 B(q,qbar)` and
  `(2i*omega*I-A)^-1 B(q,q)`, and evaluates the raw second/third state
  derivatives in the fixed Kuznetsov formula. The published coefficient uses
  unit Euclidean right-eigenvector scale and must exclude zero. This proves a
  nondegenerate local Hopf point and center-manifold super/subcriticality for
  every parent event, not the original-control orbit side, an explicit
  periodic-orbit branch, full-state orbit stability, or global continuation.
- [`ro_hopf_periodic_orbit_germ.jl`](../../Bnc_julia/src/rop/ro_hopf_periodic_orbit_germ.jl)
  completely replays the P8s1c0/P8s1c1 authority chain and lifts every
  nondegenerate Hopf event exactly once. With parent equations ordered
  `(F...,E,O)` and variables `(x...,lambda,z)`, strict Krawczyk orientation and
  `det(F_x)` recover the sign of `d Re(mu)/d lambda`; combining it with the
  certified `l1` sign orients the original-control side. The classical Hopf
  theorem then gives one theorem-level local periodic-orbit germ and
  center-manifold radial attraction or repulsion for sufficiently small
  nonzero but unquantified amplitude. This P8s1c2a owner constructs no orbit
  enclosure, validated branch, quantitative amplitude radius, Floquet
  spectrum, full-state stability result, or periodic-orbit population census.
- [`ro_periodic_fourier_identity.jl`](../../Bnc_julia/src/rop/ro_periodic_fourier_identity.jl)
  owns the pre-c2b exact finite-support Fourier residual foundation. It stores
  real trajectories as a conjugate-symmetric full-complex half-spectrum,
  snapshots exact dyadic/rational inputs, and uses bounded direct Laurent
  convolution to evaluate `omega*d_theta(x)-F(x,u)` through every mode
  generated by the declared polynomial source. The audit reports exact
  Galerkin-head, omitted-tail, and full rectangular-complex weighted `l1_nu`
  norms and can certify one nonconstant positive-frequency parameterization
  only when the complete residual vanishes. Source-bound validation, rather
  than the object or its hash, owns authority. This is not an infinite-tail
  radii theorem, a nearby-orbit enclosure or uniqueness proof, a c2b branch,
  quantitative amplitude coverage, Hopf incidence, minimal-period result,
  Floquet result, or population theorem.
- [`ro_field_differential.jl`](../../Bnc_julia/src/rop/ro_field_differential.jl)
  audits sampled elementary faces for circulation, mixed-partial, and
  output-edge consistency; retains raw and symmetrized second-order estimates;
  and applies one explicitly named cross-curvature synergy policy.
- [`ro_stratified_field.jl`](../../Bnc_julia/src/rop/ro_stratified_field.jl)
  certifies a complete continuous regular-cell PWA extension and returns intact
  MIMO Jacobian generators at nonsmooth boundaries. Singular branches are not
  selected or inserted into the regular-limit hull.
- [`ro_singular_selection.jl`](../../Bnc_julia/src/rop/ro_singular_selection.jl)
  keeps physical singular-branch selection separate from the Clarke
  regular-limit layer. A branch can be selected only under a content-identified
  complete candidate-population receipt and matching residual, stability, and
  dynamic-reachability evidence; incomplete, multiple, or gapped candidates
  remain unknown or set-valued. “Complete” here is caller-declared receipt
  consistency: this layer validates submitted counts/content/hashes but does not
  execute the enumerator or recompute referenced analysis/trace artifacts.
- [`ro_field_uncertainty.jl`](../../Bnc_julia/src/rop/ro_field_uncertainty.jl)
  separates parametric, experimental, numerical-gap, and structural
  uncertainty; analyzes local identifiability through a whitened-sensitivity
  SVD; propagates first-order covariance; and summarizes declared replicate or
  typed bounded explicit-coordinate populations without bridging gaps. The
  latter is complete only for its enumerated rows, not for the enclosing
  continuum box.
- [`ro_dynamic_hysteresis.jl`](../../Bnc_julia/src/rop/ro_dynamic_hysteresis.jl)
  separates a finite supplied-protocol loop, an untrusted-callback rate-lag
  candidate, and a conditional equilibrium-branch loop. It can recompute
  declarative polynomial equilibrium residuals and local stability, but this
  supplied-protocol analyzer does not integrate the dynamics or establish
  state-space reachability. Its
  `complete_dynamic_reachability_evidence`,
  `branch_switch_hysteresis_certified`, and
  `qualifies_as_dynamic_hysteresis` flags remain false; static multiple roots and
  self-consistent branch-switch records never upgrade it.
- [`ro_dynamic_trajectory.jl`](../../Bnc_julia/src/rop/ro_dynamic_trajectory.jl)
  separately integrates a deterministic polynomial vector field over one finite
  declared scalar linear ramp. It binds initial state, fixed controls,
  state bounds, an explicit model-time unit with a structured control/time rate
  identity, solver policy, work limits, and strict
  increasing-to-decreasing lineage, and requires primary Tsit5 and tighter
  Vern7 trajectories to agree on the save grid. This is finite model-backed
  numerical trajectory evidence, not a validated error enclosure, independent
  model-residual certificate, branch-switch result, basin/global reachability
  theorem, true hysteresis, or experimental causality.
- [`ro_sparse_sampler.jl`](../../Bnc_julia/src/rop/ro_sparse_sampler.jl)
  implements deterministic nested Clenshaw--Curtis/Smolyak refinement with a
  downward-closed index set. Its v2 surface exposes portable, self-hashed
  plan/state/index-batch/result tokens and pure prepare/commit/finalize
  transitions; one multi-index is one backend work unit, and complete replay is
  the authoritative validator. Invalid points make their whole index
  unresolved and block only its descendant cone. The stopping indicator remains
  finite-policy evidence rather than a continuum error theorem.
- The 2D geometry accepts a positive construction tolerance only up to both an
  absolute `1e-6` ceiling and `1e-6` of the shortest domain side. That tolerance
  may guide clipping and deduplication, but it is not used to relax the final
  coverage claim; publication derives its area/length certificate tolerance
  independently from the declared domain and Float64 resolution.
- The 3D builder retains Float64 incidence construction and grey-zone rank
  checks, then applies the separate dyadic-exact publication gate described
  above. It does not exact-recompute the complete ridge/vertex lattice.
- Geometry and evidence builders preflight typed hard limits with overflow-safe arithmetic and
  expose cooperative cancellation. These are small-model capability contracts,
  not evidence that dense high-rank grids or full exact arrangements are
  computationally practical.

## Working-tree volume-estimator maintenance

- `calc_volume` uses a seed plus global sample index, so a fixed finite sample
  population produces identical integer counts across Julia thread counts and
  scheduling. `time_limit=Inf` removes wall-clock stopping from that claim.
- Logical-worker storage is proportional to workers times the regime count,
  dimensions, and largest constraint row count; it no longer allocates one
  constraint buffer per regime per possible thread slot.
- The legacy regime/path objects can store only one volume value. Consequently,
  only the default unrebased estimator reads or writes that cache. Any explicit
  estimator keyword or rebasing is computed ephemerally, and duplicate selected
  indices are computed once then expanded back into the caller's order.
- Non-finite geometry, samples, confidence transforms, and uniform-box widths
  fail before threaded estimation. Exclusive overlap classification retains the
  full ordered region population even after an earlier region converges.

## Non-goals

- HTTP routing, authentication, job persistence, and UI state.
- Treating catalogue matches, sampled volumes, or paper prose as mathematical
  proof.
- Silently repairing an invalid reaction/conservation matrix.
- Providing temporal-dynamics evidence for the current designability contract.
- Providing cross-process locking, HTTP admission, or Web work policy; those are
  backend responsibilities.
- Claiming a solver-success flag proves analytic correctness or biological
  validity.

## Owner paths

- Public package and core types: [`Bnc_julia/src/BindingAndCatalysis.jl`](../../Bnc_julia/src/BindingAndCatalysis.jl)
- Model construction: [`Bnc_julia/src/initialize.jl`](../../Bnc_julia/src/initialize.jl)
- Regime and graph algorithms: [`Bnc_julia/src/regimes.jl`](../../Bnc_julia/src/regimes.jl), [`Bnc_julia/src/Bnc_regime.jl`](../../Bnc_julia/src/Bnc_regime.jl)
- SISO paths: [`Bnc_julia/src/SISO.jl`](../../Bnc_julia/src/SISO.jl)
- ROP overlay: [`Bnc_julia/src/rop/`](../../Bnc_julia/src/rop/)
- Multi-input sampled, geometric, uncertainty, and dynamic evidence:
  [`Bnc_julia/src/rop/ro_field.jl`](../../Bnc_julia/src/rop/ro_field.jl),
  [`Bnc_julia/src/rop/ro_cell_complex.jl`](../../Bnc_julia/src/rop/ro_cell_complex.jl),
  [`Bnc_julia/src/rop/ro_coordinate_chart.jl`](../../Bnc_julia/src/rop/ro_coordinate_chart.jl),
  [`Bnc_julia/src/rop/ro_nonlinear_coordinate_chart.jl`](../../Bnc_julia/src/rop/ro_nonlinear_coordinate_chart.jl),
  [`Bnc_julia/src/rop/ro_observable_chart.jl`](../../Bnc_julia/src/rop/ro_observable_chart.jl),
  [`Bnc_julia/src/rop/ro_regular_sheet.jl`](../../Bnc_julia/src/rop/ro_regular_sheet.jl),
  [`Bnc_julia/src/rop/ro_regular_root_census.jl`](../../Bnc_julia/src/rop/ro_regular_root_census.jl),
  [`Bnc_julia/src/rop/ro_fold_event_census.jl`](../../Bnc_julia/src/rop/ro_fold_event_census.jl),
  [`Bnc_julia/src/rop/ro_fold_branch_incidence.jl`](../../Bnc_julia/src/rop/ro_fold_branch_incidence.jl),
  [`Bnc_julia/src/rop/ro_branch_indexed_field.jl`](../../Bnc_julia/src/rop/ro_branch_indexed_field.jl),
  [`Bnc_julia/src/rop/ro_spectral_hopf_event_census.jl`](../../Bnc_julia/src/rop/ro_spectral_hopf_event_census.jl),
  [`Bnc_julia/src/rop/ro_hopf_lyapunov_census.jl`](../../Bnc_julia/src/rop/ro_hopf_lyapunov_census.jl),
  [`Bnc_julia/src/rop/ro_field_differential.jl`](../../Bnc_julia/src/rop/ro_field_differential.jl),
  [`Bnc_julia/src/rop/ro_stratified_field.jl`](../../Bnc_julia/src/rop/ro_stratified_field.jl),
  [`Bnc_julia/src/rop/ro_cell_complex_3d.jl`](../../Bnc_julia/src/rop/ro_cell_complex_3d.jl),
  [`Bnc_julia/src/rop/ro_stratified_field_3d.jl`](../../Bnc_julia/src/rop/ro_stratified_field_3d.jl),
  [`Bnc_julia/src/rop/ro_sparse_sampler.jl`](../../Bnc_julia/src/rop/ro_sparse_sampler.jl),
  [`Bnc_julia/src/rop/ro_field_uncertainty.jl`](../../Bnc_julia/src/rop/ro_field_uncertainty.jl),
  [`Bnc_julia/src/rop/ro_singular_selection.jl`](../../Bnc_julia/src/rop/ro_singular_selection.jl),
  [`Bnc_julia/src/rop/ro_dynamic_hysteresis.jl`](../../Bnc_julia/src/rop/ro_dynamic_hysteresis.jl),
  and [`Bnc_julia/src/rop/ro_dynamic_trajectory.jl`](../../Bnc_julia/src/rop/ro_dynamic_trajectory.jl)
- Numerical mapping and scans: [`Bnc_julia/src/qK_x_mapping.jl`](../../Bnc_julia/src/qK_x_mapping.jl), [`Bnc_julia/src/numeric.jl`](../../Bnc_julia/src/numeric.jl)
- Scan validity and warm-start overlay:
  [`Bnc_julia/src/rop/rop_overlay.jl`](../../Bnc_julia/src/rop/rop_overlay.jl)
- Volume estimator: [`Bnc_julia/src/volume_calc.jl`](../../Bnc_julia/src/volume_calc.jl)
- Web-only path adapter and per-bundle runtime boundary:
  [`webapp/src/path_work_budget.jl`](../../webapp/src/path_work_budget.jl),
  [`webapp/src/model_runtime.jl`](../../webapp/src/model_runtime.jl)

## Inputs

- Stoichiometric matrix `N` or conservation matrix `L`, plus optional species,
  total, and binding-constant symbols.
- Optional catalysis matrices for engine APIs that support them.
- A selected total/parameter direction and observed species or linear output.
- Parameter points, ranges, or polyhedral/volume policies.
- Optional path-allocation limits, cooperative cancellation callbacks, and
  optional numerical validity tracking.

## Outputs

- A `Bnc` model and cached regime/graph structures.
- Regime indices, permutations, singularity/nullity flags, affine maps, and
  polyhedral conditions.
- SISO/change paths, exact reaction-order profiles, behavior families, and ROP
  plotting geometry.
- Equilibrium concentrations, scan arrays, and normalized probability/fraction
  estimates represented by `Volume`.
- When requested, path-limit exceptions and scan validity vectors/matrices that
  let the caller distinguish missing evidence from a computed value.

## Contract sources

- Constructor invariants and public model behavior:
  [`initialize.jl`](../../Bnc_julia/src/initialize.jl)
- SISO accessors and path semantics:
  [`SISO.jl`](../../Bnc_julia/src/SISO.jl)
- Regime construction commit/rollback semantics:
  [`regimes.jl`](../../Bnc_julia/src/regimes.jl) and
  [`initialize.jl`](../../Bnc_julia/src/initialize.jl)
- ROP public surface:
  [`rop_exports.jl`](../../Bnc_julia/src/rop/rop_exports.jl)
- Backend conversion from reaction strings:
  [`reaction_parser.jl`](../../webapp/src/reaction_parser.jl)
- Engine-facing API serialization:
  [`model_handlers.jl`](../../webapp/src/model_handlers.jl) and
  [`parameter_scan_handlers.jl`](../../webapp/src/parameter_scan_handlers.jl)

## Tests

- [`Bnc_julia/test/runtests.jl`](../../Bnc_julia/test/runtests.jl): constructor
  rejection, normalized-volume contract, golden characterization networks,
  pre-allocation path limits, scan validity, and concurrent full-regime-graph
  construction.
- [`Bnc_julia/test/multi_input_ro_field_contract.jl`](../../Bnc_julia/test/multi_input_ro_field_contract.jl):
  bounded 2D/3D tensors, axis covariance, analytic parity, invalid gaps, limits,
  and cancellation.
- [`Bnc_julia/test/ro_cell_complex_contract.jl`](../../Bnc_julia/test/ro_cell_complex_contract.jl):
  analytic heterodimer cells/facets/areas, translation and axis covariance,
  coincident labels, ambiguity, point classification, absolute/relative
  geometry-tolerance limits, empty-cell accounting, limits, and cancellation.
- [`Bnc_julia/test/ro_coordinate_chart_contract.jl`](../../Bnc_julia/test/ro_coordinate_chart_contract.jl):
  affine correlation/rotation pullbacks, rank/conditioning rejection, tensor
  parity, detached property snapshots, backing-storage mutation detection,
  shape, finite-value, and overflow boundaries.
- [`Bnc_julia/test/ro_nonlinear_coordinate_chart_contract.jl`](../../Bnc_julia/test/ro_nonlinear_coordinate_chart_contract.jl):
  analytic quadratic values and local Jacobians, zero-Hessian affine parity,
  the complete scalar Hessian chain rule, pointwise fold/rank/conditioning
  rejection, first-order source-axis/point declarations, chart/output-bound
  source-derivative identity, exact Hessian symmetry, content seals/replay,
  forged-result rejection, cumulative/overflow-safe limits, and cancellation.
- [`Bnc_julia/test/ro_observable_chart_contract.jl`](../../Bnc_julia/test/ro_observable_chart_contract.jl):
  affine and quadratic observable values, first-order pullback, both
  second-order chain-rule terms, multi-output row covariance, exact source order
  and unit matching, domain/identity/tamper rejection, limits, and cancellation.
- [`Bnc_julia/test/ro_regular_sheet_contract.jl`](../../Bnc_julia/test/ro_regular_sheet_contract.jl):
  canonical polynomial ownership, exact affine and nonlinear patch fixtures,
  strict Krawczyk/nonsingularity rejection, exact implicit derivatives,
  correlation-preserving full-tube substitution, one- and two-input nonlinear
  translated branches, path-total-derivative cancellation, replay authority,
  complete overlap-tube bridges, cumulative limits, exact-bit preflight, and
  cancellation.
- [`Bnc_julia/test/ro_regular_root_census_contract.jl`](../../Bnc_julia/test/ro_regular_root_census_contract.jl):
  three-sheet and zero-root exhaustive covers, a nonlinear two-input sheet, a
  two-state/two-input tensor cover that uses either residual equation for
  exclusion, exact cell/patch mapping, replay authority, mismatch and unresolved
  rejection, raw-record forgery rejection, bounded resources, and cancellation.
- [`Bnc_julia/test/ro_fold_event_census_contract.jl`](../../Bnc_julia/test/ro_fold_event_census_contract.jl):
  scalar and two-state complete augmented covers, empty and multiple-event
  populations, exact cell-centered translation, simple-fold nondegeneracy,
  cusp/unresolved/missing/duplicate rejection, source identity, authoritative
  replay, raw-record forgery, bounded resources, and cancellation.
- [`Bnc_julia/test/ro_fold_branch_incidence_contract.jl`](../../Bnc_julia/test/ro_fold_branch_incidence_contract.jl):
  declarative one- and two-state coordinate permutation, event containment,
  two complete half-branch bridges, strict original-patch tube inclusion,
  complete-census corridor exclusion, a three-fold intervening-event
  counterexample, source/side/identity/forgery rejection, cumulative budgets,
  and cancellation.
- [`Bnc_julia/test/ro_branch_indexed_field_contract.jl`](../../Bnc_julia/test/ro_branch_indexed_field_contract.jl):
  explicit dynamics identity, replayed bridge components, exact
  stable/unstable/unknown policy fixtures, nonzero ordered two-input state-log
  responses, separated stable-root lower-bound witnesses, forced-false strong
  flags, raw-constructor rejection, overflow-safe budgets, and cancellation.
- [`Bnc_julia/test/ro_spectral_hopf_event_census_contract.jl`](../../Bnc_julia/test/ro_spectral_hopf_event_census_contract.jl):
  signed characteristic-polynomial parity, complete/empty covers, arbitrarily
  small positive frequency, transverse linear crossing with zero first Lyapunov
  coefficient, extra unstable spectrum, tangency, repeated pair, zero--Hopf,
  double-Hopf projection rejection, state/control/frequency boundary roots,
  source/replay/forgery/dimension rejection, cumulative exact-work,
  determinant-leaf/projection-pair/cell/event limits, and early/late
  cancellation.
- [`Bnc_julia/test/ro_hopf_lyapunov_census_contract.jl`](../../Bnc_julia/test/ro_hopf_lyapunov_census_contract.jl):
  analytic radial, quadratic, mixed factorial/sign, quartic-invariance,
  nonnormal `omega=2`, empty, and two-event fixtures; strict Bautin/linear,
  foreign/missing/duplicate/forged-seed rejection; exact extreme-scale square
  roots; parent-first authority; pre-replay resource rejection; cumulative
  work; and immediate/child-stage cancellation.
- [`Bnc_julia/test/ro_hopf_periodic_orbit_germ_contract.jl`](../../Bnc_julia/test/ro_hopf_periodic_orbit_germ_contract.jl):
  all four crossing/`l1` sign combinations, two equal-`l1` events with opposite
  original-control sides in canonical parent order, an extra unstable
  transverse mode, an empty parent population, dense exact determinant signs,
  foreign and self-consistent-hash forgery rejection, forced-false strong
  claims, BigInt population/work preflight, cumulative work, and cancellation
  inside child determinant analysis.
- [`Bnc_julia/test/ro_periodic_fourier_identity_contract.jl`](../../Bnc_julia/test/ro_periodic_fourier_identity_contract.jl):
  exact cosine/sine convention and derivative oracles, independent quadratic/
  cubic/product convolutions, a three-state exact orbit with a forced second
  harmonic, an all-zero Galerkin head with a nonzero omitted `k=2` residual,
  intermediate-support retention, constant and unstable-transverse boundaries,
  separate input/output bandwidths, foreign/source replay, structurally
  consistent residual forgery, forced-false strong claims, exact-operation/
  payload/workspace limits, copy/mutation failure closure, deep convolution
  cancellation, final-publication cancellation, and deterministic retry.
- [`Bnc_julia/test/ro_field_differential_contract.jl`](../../Bnc_julia/test/ro_field_differential_contract.jl):
  analytic non-uniform quadratic fields, discrete inconsistency, invalid gaps,
  unsymmetrized curvature, Hessian spectra, explicit synergy labels, and limits.
- [`Bnc_julia/test/ro_stratified_field_contract.jl`](../../Bnc_julia/test/ro_stratified_field_contract.jl):
  declared Float64/PWA regular-extension closure/continuity, overlap rejection,
  MIMO joint Clarke generators, coupled fixed-direction images, limits, and
  cancellation.
- [`Bnc_julia/test/ro_cell_complex_3d_contract.jl`](../../Bnc_julia/test/ro_cell_complex_3d_contract.jl):
  the analytic `A+B+C<->ABC` fixture, a three-cell T-junction, complete
  cell/facet/ridge/vertex incidence, rank grey zones, coverage, volume, links,
  Euler consistency, tiny dyadic gap/overlap and Float/exact-mismatch rejection,
  scaled supporting planes, exact-bit/resource hard limits, and cancellation.
- [`Bnc_julia/test/ro_stratified_field_3d_contract.jl`](../../Bnc_julia/test/ro_stratified_field_3d_contract.jl):
  exact-dyadic D3 tangential, offset, and dual-cycle checks; multi-output and
  multiway incidence; one-ULP obstructions; ambiguity, identity, limits,
  cancellation, and full certificate reproduction.
- [`Bnc_julia/test/ro_sparse_sampler_contract.jl`](../../Bnc_julia/test/ro_sparse_sampler_contract.jl):
  nested-node identity, downward-closed refinement, deterministic frontier
  ordering, invalid descendant cones, portable v2 transition tokens, exact
  restore/replay, authoritative terminal-result validation, budgets, and
  cancellation.
- [`Bnc_julia/test/ro_field_uncertainty_contract.jl`](../../Bnc_julia/test/ro_field_uncertainty_contract.jl):
  source and population identity, whitened-sensitivity rank, first-order
  covariance, gap-preserving population summaries, synthetic coverage,
  mutation detection, limits, and cancellation.
- [`Bnc_julia/test/ro_singular_selection_contract.jl`](../../Bnc_julia/test/ro_singular_selection_contract.jl):
  declared complete candidate receipts, branch-bound residual/stability/
  reachability evidence, unique/set-valued/unknown outcomes, self-hash
  validation, limits, and cancellation.
- [`Bnc_julia/test/ro_dynamic_hysteresis_contract.jl`](../../Bnc_julia/test/ro_dynamic_hysteresis_contract.jl):
  finite-loop, weak finite-rate-lag candidate, and conditional equilibrium-loop
  classification; closed protocol lineage, exact reverse grids, arbitrary
  switch-record negatives, declarative polynomial recomputation, monostable and
  bistable fixtures, identity, limits, and cancellation. All current strong
  dynamic-reachability and hysteresis flags remain false.
- [`Bnc_julia/test/ro_dynamic_trajectory_contract.jl`](../../Bnc_julia/test/ro_dynamic_trajectory_contract.jl):
  canonical bounded protocol specs, runtime and solver-policy identity,
  monostable and bistable finite integrations, exact forward/reverse lineage,
  predecessor replay, primary/audit disagreement, state bounds, work limits,
  cancellation, and fail-closed strong dynamic flags.
- [`Bnc_julia/test/volume_reproducibility_contract.jl`](../../Bnc_julia/test/volume_reproducibility_contract.jl):
  cross-thread sample-count identity, overlap ordering, extreme inputs,
  duplicate/empty selection, default-cache isolation, and rebased/custom
  estimator behavior.
- [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl): model building,
  graphs, paths, scans, ROP polyhedra, IR bridges, and backend serialization.
- [`webapp/test/d1_atlas_contract.jl`](../../webapp/test/d1_atlas_contract.jl):
  single-base homomer network through engine and atlas.
- [`webapp/test/test_phenotype_pipeline.jl`](../../webapp/test/test_phenotype_pipeline.jl):
  engine-backed phenotype behavior and determinism.
- [`webapp/test/concurrency_and_budget_contract.jl`](../../webapp/test/concurrency_and_budget_contract.jl):
  Web SISO caps, typed 422 translation, spawned-worker context propagation, and
  model-bundle concurrency boundaries.
- [`webapp/test/numerical_validity_contract.jl`](../../webapp/test/numerical_validity_contract.jl):
  validity masks and partial numerical responses across scan/FRET/refinement
  consumers.

## CI

The `test-julia` job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
instantiates the web project with the local `Bnc_julia`, runs the backend suite,
the engine golden suite, and the phenotype suite. The current matrix exercises
the production Julia line declared by that workflow; package compatibility is
broader than the matrix currently proves.

## Invariants

- `N` reaction rows and `L` conservation rows must have full row rank, share the
  same species columns, satisfy `d + r = n` and `N * L' = 0`, and make `[L; N]`
  nonsingular. Invalid input is rejected; reactions are not silently deleted.
- Species, total, and binding-constant symbol counts must match model dimensions.
- A model's cached regimes and graphs must be invalidated when coordinate or
  catalysis changes alter their meaning.
- Binding-regime construction is committed only when
  `_regimes_build_complete=true` under `_regimes_affine_lock`. A partial
  `BindRegimes` or `vertices_graph` value is never a completed fast path, and a
  failed builder clears all partial publications.
- Engine consumers use `get_SISO_graph`, `get_sources`, and `get_sinks` rather
  than depending on obsolete internal fields.
- Path allocation limits are checked during dynamic-programming materialization,
  before appending beyond `max_paths` or `max_total_nodes`. The engine default is
  uncapped; the synchronous Web boundary and ROP-shape/Atlas job policies
  explicitly supply 2,000 and 200,000.
- Cancellation is a resource-control outcome, not a scientific result. A
  cancelled or hard-bound population remains unknown and cannot be reported as
  evidence of infeasibility.
- Parallel 2D scans initialize mutable regime/affine state before row workers
  read the model. A tracked failed solve produces an invalid mask entry and
  `NaN`, resets warm-start state, and cannot be treated as a successful next
  seed.
- Reaction-order values at higher-nullity regimes are not uniquely identified;
  callers must preserve that state instead of inventing a scalar.
- Multi-input axis/output order and the full fixed q/K background are identity
  inputs. Exact 2D cells never choose a first affine label from coincident
  set-valued geometry, and uncovered positive area remains a gap.
- The single-polyhedron `calc_volume` overload returns `Volume`; its mean is a
  normalized Gaussian mass or uniform-box fraction in `[0, 1]`, not geometric
  volume in coordinate units.
- A regime/path single-value cache never aliases a custom seed, tolerance,
  sampler, or rebase configuration. Repeated indices preserve repeated output
  positions without entering the estimator as duplicate exclusive regions.

## Known gaps

- The golden suite is a strong regression baseline, but many values are
  characterization snapshots rather than independent analytic proofs.
- CI currently exercises one Julia release line even though project
  compatibility admits another supported line.
- The package mixes exact/polyhedral and numerical algorithms; evidence strength
  must be named by the caller rather than inferred from the module name.
- Temporal response dynamics are outside the equilibrium designability path.
- Only binding-regime construction has the explicit completion-lock contract.
  Other mutable lazy engine caches are protected conservatively by the
  backend's whole-handler `ModelBundle` lock; finer-grained engine thread safety
  is not established.
- Engine locks and cache state are process-local. They do not coordinate two
  Julia replicas using the same `NetworkIR` content.
- Direct/offline path enumeration is intentionally not capped by the two Web
  adapter values. It can consume large memory unless the caller supplies engine
  limits or partitions the workflow.
- A successful solver retcode is a numerical-policy result, not proof of
  uniqueness, global correctness, scan resolution, or biological relevance.
- The quadratic nonlinear input chart certifies numerical immersion only at the
  reference and at each point actually evaluated. It does not prove global
  injectivity or covering, identify self-overlap away from a local fold, define
  chart-transition/overlap covariance, or propagate chart-calibration
  uncertainty separately from biochemical uncertainty. Its first-order
  source-axis declarations prevent accidental anonymous permutation but do not
  authenticate an upstream producer's labels.
- The observable-chart implementation is finite affine/quadratic only. Unit
  strings bind identity and order but do not perform dimensional algebra or
  conversion; ratio, log-sum, normalized-occupancy, and other general
  non-polynomial observable maps still require a separately versioned contract.
- P5r0 now proves exact-dyadic polynomial regular sheets only inside declared
  positive root tubes. Its overlap bridge transfers branch identity only when
  both entire patch tubes fit inside a separately replayed bridge tube. It does
  not exclude roots outside a tube, enumerate a complete root population,
  enclose folds or Hopf events, extract residuals from arbitrary chemistry, or
  continue a global branch. P5r1 remains design-only: native
  log/transcendental residuals require a direct pinned interval dependency, and
  opaque callbacks remain inadmissible as validated enclosure evidence.
- P8s0 groups only the supplied replayed P5r0 evidence. Its branch components
  are not a complete physical root population, and its separated witness proves
  `at least N` stable roots only on the declared common box. It neither locates
  fold/Hopf boundaries nor certifies global continuation or hysteresis. P5r0.1
  now removes the specific artificial independence between repeated state and
  control occurrences by substituting the complete affine tube before
  differentiation. Ordinary interval wrapping in the resulting `(u,delta)`
  polynomial remains possible, so failure of the strict enclosure gate is still
  unknown rather than evidence of singularity.
- P8s1a upgrades only one bounded population statement: an exhaustive replayed
  tensor cover proves all regular roots and the absence of folds inside its one
  declared affine moving state domain. It requires full-box patches in one
  common slope coordinate and fails closed on every unresolved cell. It does
  not exclude roots outside the domain, classify the complete stable-root
  population, admit a domain containing folds, enclose Hopf events, or prove
  global continuation.
- P8s1b0 exhaustively isolates simple folds only for exactly one control and
  only inside one declared positive augmented domain. P8s1b1 attaches two local
  chart half-branches to original-coordinate regular patches only after the
  full event census excludes every other fold from each covered corridor. It
  does not orient a two-root side in the original control, classify stability,
  identify a remote continuation component, build a global branch graph, cover
  multi-control fold sheets, or establish hysteresis. P8s1c0 now exhaustively
  isolates simple transverse spectral crossings only for exactly one polynomial
  control and only inside one declared state/control domain with a complete
  frequency-squared cover. P8s1c1 now lifts every event only after replaying
  that complete parent and certifies a nonzero unit-scale first Lyapunov
  coefficient with convention-bound center-manifold super/subcriticality.
  P8s1c2a now adds theorem-level germ/event incidence, original-control side,
  and center-manifold radial attraction or repulsion for sufficiently small
  nonzero but unquantified amplitude. It does not construct an explicit orbit
  enclosure or validated quantitative branch, certify Floquet/full-state orbit
  stability or orbit-population completeness, cover multi-control Hopf sheets
  or native residuals, or prove global continuation. P8s1c2b/P8s1c2c and P8s2
  remain design-only. The implemented exact finite-support Fourier residual
  audit is only a pre-c2b identity kernel and does not implement c2b0.
- The chemistry-derived exact constructor still admits only two distinct
  conserved-total axes, binding equilibrium, species outputs, and Float64
  polyhedra. A separate strict three-dimensional constructor accepts explicit
  affine cell specifications and returns a complete Float64 face lattice; it
  does not yet extract those specifications automatically from arbitrary
  chemistry, accept arbitrary-precision inputs, certify arbitrary-real geometry,
  exact-recompute all ridge/vertex incidence, or construct exact incidence for
  `D >= 4`. Its selected dyadic-exact publication predicates apply only to the
  admitted Float64 bit patterns. Higher rank remains sampled/adaptive evidence,
  and dense cost grows as the Cartesian product.
- Exact regular-extension integrability now covers an explicit-affine D3
  complete contractible box through exact tangential, offset, and dual-cycle
  checks. Holed-domain period/cohomology checks, `D >= 4`, and D3
  Clarke/set-valued Jacobian queries remain unimplemented.
- The sparse sampler's hierarchical-surplus stopping rule is deterministic
  finite-policy evidence. It has no uniform continuum error theorem for
  nonsmooth/singular RO fields. Its pure v2 transitions are consumed by the
  local adaptive backend job, but there is no distributed executor, shared
  object store, or cluster recovery contract.
- The Web consumer binds the numerical homotopy policy, runtime/source lock,
  per-point step/RHS caps, in-RHS cancellation, strict Float64 closed-cell
  regime membership, and a metered replay-work cap. Its authoritative path uses
  one forward chain and one cumulative meter through resume artifact copying;
  shallow engine validation is allowed only after that replay or for
  current-process state. Plan/model reconstruction precedes this artifact-chain
  meter. The engine token layer still does not provide a distributed
  lease protocol or a validated sparse continuum error bound.
- Singular selection is conditional on one declared bounded candidate receipt
  and supplied analysis/trace procedures. The current validator does not replay
  the enumerator or load/recompute those analysis/trace contents; it is not a
  complete physical root, stability, or basin-selection theorem.
- Identifiability is local to a declared whitened sensitivity/observation model,
  delta propagation is first-order, and coverage is synthetic or tied to a
  declared finite population. Structural/global identifiability, profile
  likelihood/posterior geometry, nonlinear propagation, and experimental
  calibration are not implemented.
- Conditional equilibrium-loop classification still covers one supplied finite
  forward/reverse protocol cycle and does not itself integrate the vector field.
  A separate model-backed path integrates one finite scalar linear ramp from a
  declared initial state and audits it with a second solver, but it has no
  validated error or independent model-residual enclosure, branch/event
  certificate, basin/global reachability result, zero-rate limit, persistence
  across rates/noise, or experimental-causality claim.
- Exact relabel-invariant hashing is a backend identity policy, not an engine
  theorem. For more than seven free species the backend uses positional content
  hashing, so relabeled equivalents may build separate engine models.
- A finite `time_limit` can stop after a complete batch before every requested
  tolerance is met. The current `Volume` type carries mean and variance but no
  explicit timeout/partial field, so callers that require exact run-to-run
  reproducibility should use `time_limit=Inf` and an explicit tolerance policy.
- Counter-based Gaussian generation is measurably slower than the previous
  thread-local `randn` path; deterministic scheduling is the current tradeoff,
  and future optimization needs an end-to-end benchmark rather than a sampling
  microbenchmark alone.

## Change protocol

1. State whether the change affects construction, regime identity, ROP/path
   semantics, numerical solving, or only presentation.
2. Add a minimal analytic sanity case and a characterization regression for any
   changed mathematical output.
3. Run the engine golden suite and all backend tests that serialize the changed
   object; run phenotype and atlas tests when path or `qK2x` behavior changes.
4. For a new mutable lazy cache, state its publication commit point and add a
   concurrent first-use test before weakening the backend's per-bundle lock.
5. For a numerical consumer, propagate solver status through validity/partial
   output and prove that invalid samples cannot seed, rank, or join a curve.
6. If a spawned Web worker can construct paths, explicitly carry the
   synchronous request context; Julia task-local storage is not inherited by a
   raw `Threads.@spawn`.
7. Bump or record the consuming algorithm/config version if persisted artifact
   meaning changes.
8. Rebuild downstream atlas or research artifacts; never compare them across
   changed semantics by filename alone.

See [system architecture](../architecture/overview.md) and
[data provenance](../architecture/data-provenance.md).

## Verified against

- Current source commit: `f2ca13c`
- Volume-estimator and cache extension: locally verified on 2026-07-15 with
  the focused contract on Julia 1.10 and 1.12 and the complete engine golden
  suite on Julia 1.12.
- Working-tree multi-input extension inspected on 2026-07-18: exact-dyadic D3
  contractible-box integrability, pure sparse-v2 transitions, finite
  model-backed numerical trajectories, the declarative quadratic pointwise
  nonlinear input chart, the finite affine/quadratic observable chart, and the
  exact-dyadic polynomial P5r0.1 patch/bridge owner, the P8s0 branch-indexed
  stable-root-witness field, the bounded P8s1a complete regular-root census,
  the P8s1b0 complete simple-fold event census, the P8s1b1 local
  fold/regular-sheet incidence certificate, the P8s1c0 complete simple
  spectral-Hopf event census, the P8s1c1 complete nondegenerate local-Hopf
  lift, the P8s1c2a complete theorem-level local periodic-orbit germ lift, and
  the pre-c2b exact finite-support Fourier residual identity foundation.
  P5r0.1 passed 106/106, P8s0 passed 76/76, and P8s1a passed 90/90 with
  bounds checking on Julia 1.10.11 and 1.12.6; P8s1b0 passed 78/78 and P8s1b1
  passed 70/70 under the same two versions and bounds policy. P8s1c0 passed
  104/104 with bounds checking on both Julia 1.10.11 and 1.12.6; its direct
  read-only audit found no remaining P0/P1/P2 issue after cumulative-work,
  determinant-leaf, projection-pair, parent-authority, and cancellation fixes.
  P8s1c1 passed 102/102 with bounds checking on the same two Julia versions;
  its direct review found no formula or authority defect, and its reported
  pre-replay resource and local-record structure gaps were fixed before this
  status update. P8s1c2a passed 155/155 with bounds checking on both Julia
  1.10.11 and 1.12.6, and its final direct review found no remaining P0/P1/P2
  after empty-population, augmented-order, determinant-work, deep-cancellation,
  dense-sign, and self-consistent-forgery hardening. The current Julia 1.12
  exact finite-support Fourier identity contract passed 172/172 with bounds
  checking on Julia 1.10.11 and 1.12.6; independent mathematical and
  authority/resource reviews found no remaining P0/P1/P2 after derived-metric,
  bandwidth, allocation-preflight, source-replay, and final-cancellation
  hardening. The current Julia 1.12 cycle passed both complete owner suites
  after pre-c2b exact Fourier integration; the engine suite retained golden
  values 145/145, and the
  standard Web suite retained its one
  pre-existing explicit `@test_broken` and otherwise exited zero.
  The final 135-test nonlinear-chart contract was not rerun on Julia 1.10. P5r1,
  multi-control fold/Hopf sheets, P8s1c2b/P8s1c2c, and native-residual P8s2
  work remain design-only.
  This does not advance the committed source revision or establish any stronger
  boundary excluded above.
- Evidence inspected: engine constructors and public APIs; regime-build
  completion locking and rollback; optional path allocation limits; scan solver
  validity and warm-start reset; backend path/bundle adapters; golden,
  concurrency, numerical-validity, and integration contracts; and CI workflow
  wiring.
- Historical baseline: `f9c65a5` remains the pre-P6 engine and integration
  evidence. It does not establish the current completion marker, Web path caps,
  or validity/partial contract.
- Boundary: no current claim of independent analytic proof, biological
  validation, or support beyond the declared model and solver policies.
