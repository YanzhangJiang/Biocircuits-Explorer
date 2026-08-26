# Project summary

Biocircuits Explorer helps a researcher describe a protein-binding network,
compute how its outputs change, and search for networks that could match a
desired response. The central result is not merely a list of candidates: the
product keeps a retrieved or suggested candidate separate from a result
recomputed by the mathematics engine.

The current implementation evidence anchor is `b91cf41`. Revision `f9c65a5`
remains the historical evidence baseline for the maintained knowledge catalog.
Later documentation
commits may advance the branch without changing which implementation revision
these runtime claims inspected.

## What the current revision establishes

- Local quick start defaults the workspace to `127.0.0.1:8088` and Design Chat
  to `127.0.0.1:8765`.
- Synchronous compute requests are admitted through a two-slot process gate and
  rejected before known unbounded work. Oversized work returns a structured
  `422`; temporary capacity exhaustion returns `429` with `Retry-After`.
- Compiled models are content-addressed and single-flight. Each shared model
  bundle owns its mutable-cache lock, while session aliases and compiled-model
  entries keep separate bounded LRU state.
- Parameter scans, FRET heatmaps, ROP clouds, placement verification, and
  inverse refinement preserve failed-solver points as explicit partial/invalid
  evidence; those points are not plotted or promoted as best designs.
- The browser workspace is a versioned typed workflow runtime. Its exhaustive
  node inventory separates preparation from computation, its serial scheduler
  accepts only structured terminal outcomes, and strict configuration ports
  reject cross-family wires.
- Executable derived results bind freshness to node ownership, graph inputs,
  endpoint, and workspace epoch. Quick Add, Design Target, and Design Agent
  graph construction publish atomically through one undoable GraphPatch.
- Workspace v2 has a complete JSON Schema and shared JavaScript/Swift migration
  fixtures. V1 merged nodes expand deterministically, restored computation is
  historical, nested session/ticket state is stripped, and future versions
  fail before replacing the current document.
- All tracked first-party clients call `/api/v1/*`. Declared bare `/api/*`
  compatibility calls remain available through their sunset and increment a
  bounded Prometheus counter so removal can be based on observed use.
- Raw HTTP `sqlite_path` access is disabled by default. An operator must enable
  it explicitly on both client and server, and the server confines paths to a
  configured store root.
- `webapp/start.sh` is the one explicit unauthenticated Design Chat path. It is
  limited to loopback development and an exact browser origin.
- Outside that explicit development mode, Design Chat launches require an
  exact loopback origin and a bearer token of at least 32 characters. The
  macOS shell creates a new 256-bit token for each helper launch and keeps it
  in memory.
- The main web application CI runs its full owner suite on Julia 1.10 and
  1.12, as does the headless HPC environment. Local checks loaded the
  selected locks on Julia 1.10.11 and 1.12.6; neither result proves a live
  scheduler.
- The Docker workflow is configured to build and start one application image and probe its
  runtime behavior. It does not exercise Compose, Nginx, or TLS.
- The current CI configuration contains no-sign macOS host builds and unit
  tests on `macos-15-intel` and `macos-26`. A local macOS 27 arm64 run passed the
  build-for-testing step and 51/51 unit tests on 2026-07-15, including shared
  Workspace v2 fixture parity; this does not
  prove a remote workflow, UI automation, or a signed/notarized package.
- Local real-Chromium Playwright checks exercise atomic Quick Add/Undo/Redo,
  structured workflow reports, Workspace v2 fail-closed restore, an axe
  serious/critical gate, and one deterministic topology baseline against mocked
  loopback `/api/v1` responses. Zero-warning ESLint is configured. Neither
  configured CI nor this local gate proves a live backend or external service.
- Version and packaging checks keep the application version, packaged
  resources, image labels, and tag rules synchronized and fail closed on
  inconsistent release input.

## Active working-tree research extension

The scoped `codex/multi-input-ro-field` working tree changes reaction order from
a longer scalar sequence into an ordered matrix field in a declared input chart.
It adds bounded sampled fields, exact 2D cell complexes, an explicit-affine
Float64 3D face lattice with dyadic-exact publication predicates, finite-grid
differential/curvature diagnostics, regular-limit and conditional singular
evidence, and an exact-dyadic D3 regular-extension integrability certificate for
complete contractible boxes. P5n additionally admits a bounded declarative
quadratic input chart, rechecks numerical full-column-rank immersion at every
evaluated control point, and retains both terms of the first- and second-order
chain rule. First-order arrays must declare the exact source-axis names, units,
and evaluated source point, although this finite boundary cannot independently
prove that an upstream producer labelled the array honestly. Scalar
second-order receipts additionally bind the chart declaration hash and scalar
output name/unit. P5n never upgrades its pointwise checks to global injectivity.
P5o separately admits finite C2 affine/quadratic observable maps and retains the
source-Hessian and observable-Hessian terms when composing an output jet. Its
unit strings are identity labels only, and general ratio, log-sum, and other
non-polynomial observables are not implemented. P8s0 now binds a P5r0
polynomial equilibrium declaration to explicit vector-field semantics, rebuilds
evidence-relative continuation components from replayed patch bridges, encloses
uniform row-Gershgorin stability and every ordered state log-response column,
and certifies only pairwise-separated stable-root lower-bound witnesses. P8s1a
separately exhausts one exact-dyadic tensor partition of a declared affine
moving state domain: every cell must either exclude zero in one residual
component or coincide exactly with one replayed full-control-box P5r0.1 tube.
A passed census therefore gives the complete in-domain regular-root count and a
complete empty in-domain fold set. P8s1b0 now exhausts an exact-dyadic tensor
partition of one declared positive augmented `(x,lambda)` domain for an exactly
one-control polynomial system. Every cell either excludes one component of
`H=(F,det(F_x))` or contains one strict augmented-Krawczyk root, so a passed
census gives the complete isolated simple-fold set inside that domain. P8s1b1
then exchanges one selected state coordinate with the control declaratively,
attaches the event to two local regular half-branches through replayed P5r0.1
bridges, maps each half-tube strictly inside an original-coordinate regular
patch, and uses the complete event census to exclude every other fold from the
covered corridor. P8s1c0 now binds the polynomial declaration explicitly as
`dx/dt`, writes `P(s)=det(sI-F_x)` as
`E(z)+i*sqrt(z)*O(z)` for `z=omega^2`, and exhausts a state/control/frequency
tensor cover from `z=0` to strictly beyond an exact uniform spectral bound.
Strict augmented Krawczyk roots, event-cell `det(F_x)` exclusion, and complete
state/control projection separation certify every isolated simple transverse
spectral crossing in that declared domain. P8s1c1 now replays that complete
parent, canonically lifts every event through full bordered right/adjoint
eigensystems and separately validated zero/second-harmonic resolvents, and
publishes a unit-scale Kuznetsov first-Lyapunov interval only when it is
strictly separated from zero. This certifies nondegenerate local Hopf points and
center-manifold super/subcriticality for the complete parent event population.
P8s1c2a now replays that complete P8s1c0/P8s1c1 authority chain, recovers the
strict sign of `d Re(mu)/d lambda` from the augmented Krawczyk preconditioner
and `det(F_x)`, and applies the classical nondegenerate Hopf theorem to every
parent event. It publishes a theorem-level local periodic-orbit germ, its
original-control side, and radial attraction or repulsion on the center
manifold for sufficiently small nonzero but unquantified amplitude. It does
not construct a periodic-orbit enclosure or a validated branch.

The pre-c2b foundation now also owns an exact finite-support polynomial
Fourier residual audit. It stores a conjugate-symmetric full-complex
half-spectrum, uses direct exact-rational Laurent convolution without
intermediate truncation, evaluates every mode generated by the declared
polynomial vector field, and separates the retained Galerkin head from the
omitted residual tail. A positive-frequency nonconstant parameterization is
certified as one exact ODE periodic solution only when the complete generated
residual vanishes identically. This exceptional identity is not a Fourier-tail
radii theorem, local branch, quantitative amplitude segment, Hopf-event
incidence, uniqueness, Floquet result, or orbit-population certificate.

The tree also adds a pure
prepare/commit/finalize v2 adaptive sparse state machine, declared-population
uncertainty/identifiability,
conditional equilibrium-branch-loop evidence, finite model-backed numerical
trajectories, content-addressed local Cartesian and adaptive-sparse jobs/resume,
runtime/source-locked sparse solver bounds with in-solve cancellation, explicit
model-time and structured control/time rate identities,
exact-index/no-interpolation Cartesian slices, and campaign
preparation/metadata QC. The current evidence and test boundaries are
tracked in [the current status](knowledge/status/current.md); the theory,
engineering, and authorization gates are in
[decision 0006](knowledge/decisions/0006-multi-input-ro-field-research-roadmap.md).

This working tree does not provide exact incidence or regular-extension
integrability for `D >= 4`, holed-domain cohomology, automatic chemistry
extraction or Clarke queries for the 3D builder, a distributed executor, or
experimental calibration. The nonlinear input chart has no domain-wide
injectivity, chart-overlap/transition, or calibration-uncertainty certificate,
and the observable layer performs no dimensional algebra or unit conversion.
P5r0.1 now implements separately versioned exact-dyadic polynomial regular-
sheet certificates. It symbolically composes each residual with the complete
affine tube `x=p(u)+delta`, differentiates that polynomial to enclose `F_x` and
`F_u+F_x*S` without first separating repeated state/control occurrences, proves
a strict parametric Krawczyk inclusion and `beta < 1`, encloses the implicit
derivative, and can replay a full-dimensional overlap bridge whose tube contains
both patch tubes. This proves one root per control inside the declared positive
tube and branch identity only across a passed bridge; ordinary interval wrapping
inside the remaining `(u,delta)` polynomial can still reject a valid tube, and
roots outside the tube, folds outside admitted patches, and global branch
completeness remain unknown. P5r1 remains design-only for native
log/transcendental residuals through a direct pinned interval dependency. P8s0
does not enumerate all stable roots, locate fold or Hopf
boundaries, continue a branch globally, or establish true hysteresis. Its
multistability result is an existence lower bound on one declared positive
control box; all completeness and global-event flags remain false. P8s1a does
not change that stability claim: its completeness is confined to roots inside
one declared moving state domain, requires every admitted sheet to span the
same control box in one common affine slope coordinate, and rejects the whole
census if interval wrapping leaves any partition cell unresolved. It neither
excludes outside roots nor certifies stability completeness, Hopf events,
fold-containing domains, global continuation, or hysteresis. P8s1b0/P8s1b1
cover only isolated simple folds for exactly one original control and local
adjacent-sheet incidence in one selected chart. They do not identify which
original-control side has two roots, classify stability, identify remote
components, construct a global branch graph, continue fold sheets through
multiple controls, or certify hysteresis. P8s1c0/P8s1c1 cover only exactly one
control and polynomial dynamics, and no local event hash is authority without
source-bound replay of the complete parent census. P8s1c2a orients the
original-control side and certifies only theorem-level germ/event incidence and
center-manifold radial behavior; its `complete` count means every named P8s1c1
event was lifted, not that periodic orbits were enumerated. It provides no
explicit orbit enclosure, validated quantitative branch or amplitude radius,
Floquet spectrum, full-state orbit stability, stability completeness, or
global continuation. P8s1c2b/P8s1c2c, multi-control Hopf event sheets, and
P8s2 remain design-only; the implemented exact finite-support Fourier audit is
only their pre-c2b proof-kernel foundation. The finite trajectory evidence
has no validated error enclosure, certified branch switch, basin/global
reachability result, or true-hysteresis claim. The complete Atlas campaign
remains unrun and requires
separate explicit authorization; preparation counts and content hashes are not
execution or scientific-validity evidence.

A configured CI version is not proof that an external run passed. A static
deployment test is not proof that a cloud service, cluster, signed image, or
installer worked in its real environment.

## Runtime ownership

1. `Bnc_julia/` owns the mathematics engine and golden-value behavior.
2. `webapp/src/` owns the Julia API, interchange bridges, reusable-result
   storage, jobs, and persistence.
3. `webapp/public/` and `frontend-swift/` own the browser and macOS clients.
4. `webapp/scripts/` owns the Python Design Agent, Function-Space Reader, and
   synthesis tools.
5. `webapp_hpc/`, `slurm/`, and `deploy/` own headless runtime, scheduler, and
   deployment mechanics; passing their static checks is not a live rollout.
6. `src/periodic_table/` and `scripts/periodic_table/` own bounded searches,
   not universal negative proofs.

Source, schemas, tests, and versioned artifact manifests outrank prose. The
maintained compression layer is `knowledge/`; ignored `doc/`, `docs/`, and old
developer-wiki material are historical until re-verified. This public
computation repository produces artifacts only; draft manuscripts, private
feedback, and paper-side data are deliberately kept outside it.

## Evidence rules

- Use `/api/v1/*`; bare `/api/*` is a deprecated compatibility surface.
- Never present Reader retrieval, proxy margins, or bounded search absence as a
  proof.
- Preserve terminal Job state, atomic canonical-record publication,
  cooperative cancellation checkpoints, and all behavior-identity fields.
- Keep scientific counts attached to a named population, content hash, source
  revision, and reproduce command.
- If a count, claim, path, version, or external outcome lacks current evidence,
  report it as unknown.

## What remains unverified

The release-candidate evidence Schema, untracked-run template, and operator
runbook are prepared, but none of their external lanes has been executed. The
current checkout therefore does not establish live registry publication/pull,
SBOM/signature verification, live AWS services, live Slurm execution, a signed
and notarized DMG, or the full Compose/Nginx/TLS stack. It also does not resolve
the recorded periodic-table population and producer-lineage conflicts. Scripts
and local contracts do not upgrade an external result from unknown to verified.

For local work: inspect `git status`, read
[the current status](knowledge/status/current.md), choose the relevant module
in [the module catalog](knowledge/catalogs/modules.yaml), and verify its cited
source and tests before changing it. CI workflows and executable tests define
the supported verification commands.
