---
module: engine-rop
status: verified
verified_against: 1177a3d
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
- A backend `ModelBundle` lock serializes handlers sharing one mutable engine
  instance. This is intentionally stronger than the engine's verified lock:
  only the regime-build completion path, prewarmed parallel scan reads, and
  covered numerical code have targeted thread-safety evidence.

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

- Current source commit: `1177a3d`
- Volume-estimator and cache extension: current uncommitted working tree on
  2026-07-12, verified with the focused contract on Julia 1.10 and 1.12 and the
  complete engine golden suite on Julia 1.12.
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
