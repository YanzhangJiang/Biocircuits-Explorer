# Current verified snapshot

- Snapshot date: 2026-07-15
- Component-split revision inspected: `603635d`
- Current implementation revision inspected: `b91cf41`
- Historical knowledge baseline retained: `f9c65a5`
- Current-tree contract inventory:
  `python3 scripts/verify_repository.py --check`
- Configured versions:
  [generated from their owners](../generated/reference.md#versions-and-configured-toolchains)
- Scope: repository runtime and local verification evidence, not manuscript
  claims, a remote CI result, or proof that an external deployment succeeded

The current implementation evidence anchor is `b91cf41`. It retains the fixed-topology ROP
shape-optimization and runtime/job hardening at `f2ca13c`, then adds the typed
browser workflow, Workspace v2, measured API migration, real-browser local
quality gate, and external release-evidence preparation described below.
Revision `1177a3d` remains the earlier bounded-runtime evidence anchor.

## Result in plain language

The backend previously had two failure modes that ordinary input-size checks
could miss. Several requests could update the same cached model or data file at
once, and a small-looking request could expand into far more combinations than
the server could safely finish. Numerical endpoints also did not always keep a
failed calculation visibly separate from a valid plotted value.

At `1177a3d`, each shared runtime object has an explicit serialized update path,
and synchronous requests stop when their real work grows beyond a fixed ceiling.
This ceiling is the **work budget**: it limits the computation that unfolds
inside a request, not only the number of fields in its JSON body. Numerical
responses now carry a **validity marker** so a failed sample remains a gap
instead of looking like a measured zero. Design Screen responses also say how
many candidates were eligible, how many were evaluated, and whether the screen
was truncated.

Revision `603635d` only split the large backend module into focused component
files. It was not intended to change behavior. Revision `1177a3d` then added the
concurrency, work-boundary, validation, and numerical-result changes to those
owners.

## Integrated browser workflow and Workspace v2

The node canvas now has one exhaustive architecture inventory for its 40 node
types. Five roles distinguish sources, configurations, compute, manual gates,
and viewers. Seven parameter/configuration families have exact artifact types;
the former universal `ParamsConfig` type is removed, and the 7-by-7 contract
accepts only the seven same-family pairs. Six merged v1 compute nodes remain
restore-only and migrate to explicit config/result pairs; they are neither
creatable nor scheduler-runnable in Workspace v2.

Every scheduled operation returns `bne-execution-outcome/v1`. The planner
rejects a cycle before executing, then runs the selected connected component in
serial topological order. Failed, blocked, cancelled, stale, missing, or
historical upstream output blocks only its descendants. Result freshness is a
separate axis from scientific evidence: shared lifecycle tickets bind the node
owner, revision, workspace epoch, input fingerprint, and exact endpoint. This
boundary now covers the executable derived families, including Atlas,
Placer, scans, and Vertices, so an obsolete response or delayed renderer cannot
publish as current.

Quick Add, Design Target Build & Tune, and Design Agent graph construction use
one GraphPatch transaction. Planning assigns stable IDs without mutating the
editor; validation/staging/commit either publishes the whole chain as one Undo
item or restores the previous graph, node counter, workspace snapshot, and Undo
depth. Redo retains the planned identities.

Workspace v2 is jointly consumed by the JavaScript migration/loader, the
complete `schemas/workspace.schema.json`, and the Swift decoder. Shared fixtures
prove exact v1-to-v2 structural parity: invalid cross-family wires are dropped,
runtime session/ticket fields are recursively stripped, and persisted current
results restore as historical without changing their evidence grade. A future
document version fails before replacing the active browser or native project.

All tracked first-party clients now use `/api/v1/*`. Declared compatibility
aliases remain through their sunset, while
`bcx_http_legacy_requests_total` measures actual bounded route/method/status
usage; canonical, unknown, and v1-only paths are excluded. This is local
contract evidence, not production traffic evidence.

| Workflow check | Local result | Boundary |
|---|---|---|
| Browser lint and unit contracts | Zero-warning ESLint and the full listed JavaScript suite passed | Source/harness coverage; no live provider or cloud path |
| Real-browser gate | Chromium Playwright passed atomic Quick Add/Undo/Redo, exact structured reports, historical/future Workspace v2 behavior, an axe serious/critical check, and one deterministic topology screenshot with zero captured console/page errors | Static loopback server with mocked canonical API responses; not browser-to-live-Julia evidence |
| Workspace parity | The complete v2 Schema accepted the shared expected fixture; JavaScript migration and the Swift decoder produced the same structure | Shared finite fixtures, not every possible extension document |
| macOS host | Local no-sign build and 51/51 Swift unit tests passed on macOS 27 arm64 | No remote CI, UI automation, packaged-process, signature, notarization, or clean-host claim |

The canonical composition and change rules are in the
[browser workflow execution contract](../contracts/workflow-execution.md); the
[Web](../modules/web-workspace.md) and
[macOS](../modules/macos-host.md) module cards retain the owner/test routing.

## Prepared release-candidate evidence boundary

The repository now contains a fail-closed release-candidate evidence Schema,
template, validation tests, and operator runbook. A passed record must pin one
clean commit, configuration hash, OCI digest, and macOS artifact hash and must
carry hashed, redacted observations plus rollback evidence for Registry,
Compose/TLS, AWS, Slurm, and macOS. This pack is prepared but unexecuted: all
five external outcomes remain unknown until an authorized run records them.

## Working-tree extension: bounded multi-input reaction-order fields

The scoped `codex/multi-input-ro-field` working tree, based on `9b05d02`, adds
the demonstration-scale P0--P4 migration in
[decision 0005](../decisions/0005-multi-input-reaction-order-field.md). This is
current local working-tree evidence dated 2026-07-17; it does not move the
committed implementation anchor `b91cf41` above.

The local multi-input quantity is now a matrix field `R(u) = dz/du`, not a
longer one-dimensional sequence. P0 defines strict request/artifact contracts
and invalid-as-gap semantics. P1 samples one to four ordered input axes and
outputs under a 4,096-point synchronous cap, including a true `2 x 2 x 2`
three-input demonstration. P2 constructs a complete fixed-background exact 2D
Float64/asymptotic cell complex with labelled cells, facets, incidence,
singular strata, ambiguity, and point classification. Its publication boundary
reconstructs complete non-overlapping polygon and facet coverage and does not
let a caller-provided geometry tolerance weaken that certificate.

P3 adds the v1-only `POST /api/v1/ro_field` inline endpoint, standalone strict
viewer, a new RPB2 exact-complex identity, and append-only SQLite 0.4 artifact
storage without changing RPB1. P4 adds regular-cell behavior signatures, an
explicit corpus of at most eight supplied records, bounded corpus-scoped
queries, and SQLite 0.5 normalized signature indexes. Loads rebuild the exact
complex and rerun the classifier before accepting a stored signature.

The following table is the earlier P0--P4 snapshot. It is retained only to
preserve that dated evidence; the current P5--P9 verification is recorded below
and supersedes these numbers as a current total.

| Working-tree check | Local result | Boundary |
|---|---|---|
| Artifact/request schemas | 11/11 and 8/8 focused semantic contracts passed | Finite fixtures, including invalid and partial cases |
| BindingAndCatalysis suite | 385/385 passed; sampled field 38/38, exact 2D complex 82/82, golden values 145/145 | Small tested networks; no large-rank performance claim |
| Full Julia Web suite | 135 testsets: 4,652 passed, 0 failed/errors, and one pre-existing explicit `@test_broken` | Local Julia process, not remote CI |
| RO-field focused backend | API 117/117, identity/storage 44/44, behavior 70/70, finite Atlas 85/85, signature storage 69/69 | Explicit bounded examples and fault injection only |
| Browser and Python consumers | Zero-warning lint; full JavaScript suite passed including RO-field renderer 14/14; Chat API 12/12, Design Agent 44/44, repository Python 130/130 | No live model provider, cloud path, or native Workspace integration |
| Live RO-field page | Two browser-to-local-Julia 3 x 3 sampled requests rendered, including non-uniform coordinates; nine fixed-radius point glyphs, explicit no-interpolation semantics, and zero console warnings/errors | Sampled 2D demonstration only; not an exact-complex, provider, cloud, or performance run |
| Repository gate | 42 maintained files, 18 schemas, and 50 routes regenerated successfully | Local source/catalog/link consistency only |

This migration proves that the code can represent, compute, validate, store,
classify, query, and display small multi-input fields. The P5 working-tree
extension now also admits full-rank affine correlated-input charts; checks
sampled circulation, mixed partials, and output-edge integrals; retains raw and
symmetric curvature; applies one explicitly named finite-window synergy policy;
and certifies/queries a complete declared Float64/PWA 2D regular extension using
intact MIMO Clarke matrix generators. Its directional values are fixed-direction
images of those generators, not tangent-cone Bouligand derivatives. The separate
v1-only differential endpoint hashes
those diagnostics without changing the source field identity. These results are
finite-grid or regular-limit evidence, not continuum, causal, singular-branch,
dynamic, or experimental proof.

The current P5n source adds a separately versioned, bounded declarative
quadratic input chart. It stores ordered source/control names and unit labels,
references, a closed finite domain, a reference Jacobian, and one symmetric
control Hessian per source component. Construction admits only the reference
Jacobian; every evaluated point recomputes numerical rank and conditioning and
rejects a local fold/non-immersion or grey-zone result. First-order matrix/tensor
pullbacks use that local Jacobian. The scalar second-order path retains both
`J' * H_theta * J` and the chart-Hessian term, with source derivatives bound to
the chart declaration hash, scalar output name/unit, exact source point,
component order, and unit labels. First-order arrays must likewise declare the
exact source axis and evaluated point; those caller declarations are checked,
but the finite API cannot independently prove that an upstream producer labelled
the array honestly. This is pointwise local-immersion evidence only: global
injectivity, self-overlap exclusion, chart-transition covariance, and chart-
calibration uncertainty are not implemented.

P5o separately adds finite declarative C2 affine/quadratic observable maps. The
chart binds source/output order, opaque unit labels, references, a closed source
domain, coefficients, limits, and content identity. Jet composition computes
`R_y = J_phi R_z` and retains both `J_phi H_z` and
`H_phi[R_z,R_z]`. This layer performs no dimensional algebra or unit conversion,
and it does not implement general ratio, log-sum, normalized-occupancy, or other
non-polynomial observables.

P5r0.1 now has a separate engine owner and focused contract. A square declarative
polynomial system owns ordered states/controls, unit labels, canonical monomials,
hard limits, and content identity; every admitted Float64 declaration is treated
as its exact binary rational. For an affine predictor plus a remainder box, the
implementation substitutes the complete tube `x=p(u)+delta` into each residual
and combines like terms exactly. Derivatives with respect to `delta` enclose
`F_x`; derivatives with respect to `u` at fixed `delta` enclose the correlated
path quantity `G=F_u+F_x*S`; and `F_u=G-F_x*S` is reconstructed symbolically for
the published partial-Jacobian enclosure. Publication requires strict
coordinate-wise Krawczyk inclusion, an exact full-rank preconditioner,
`beta < 1`, positive control/state tubes, and a complete replay. It also binds
`G` into the v1.1.0 certificate, uses it to enclose `dx/du`, and can prove a
directed parent-to-child branch bridge when a separately certified overlap tube
contains both entire patch tubes.

The positive claim is deliberately local to the declared tube: P5r0 proves one
root for every control and uniqueness inside that tube, but
`roots_outside_declared_tube_excluded` is always false. It does not certify a
global branch population, folds/Hopf events, native log-coordinate residuals,
or chemistry extraction. P5r1 remains design-only and would require a direct
pinned interval dependency for native log/transcendental residuals. Ordinary
solver-success and point-inverse results remain numerical evidence unless they
are independently covered by a passed P5r0 certificate.

P8s0 now consumes those certificates in a separate branch-indexed field owner.
One content-bound dynamics declaration explicitly says that the polynomial
equations are `dx/dt`; time and state-rate units are retained as identity labels,
but no dimensional algebra or physical-model truth is inferred. Every supplied
patch and bridge is replayed under one cumulative exact-operation cap. Replayed
bridges form evidence-relative connected components, not a complete global
branch taxonomy. On each patch, exact interval row-Gershgorin right bounds
certify uniform local asymptotic stability only when every bound is negative;
a positive exact trace lower bound certifies instability, with `trace/n` as a
lower bound on the rightmost real part; all other cases remain
`unknown_stability`. The same patch publishes the ordered state response
enclosure `d log(x_i)/d log(u_j) = (u_j/x_i) dx_i/du_j` for every control axis.

A P8s0 multistability witness selects one uniformly stable patch from each of
at least two replayed components over one common positive control box. Exact
restricted state tubes must be strictly separated for every branch pair, which
proves only that at least that many distinct stable roots exist throughout the
box. It does not prove that the stable-root population is complete, exclude
folds outside the selected tubes, enclose fold/Hopf boundaries, certify global
continuation, or establish true hysteresis; every corresponding strong flag is
forced false. Authority requires complete source replay, not the top-level hash
alone. P5r0.1 closes the previously observed artificial state/control-width
failure for nonlinear translated branches such as
`-(x-u)(x-u-1)(x-u-2)` by using the exact `(u,delta)` composition for the full
tube. It does not eliminate ordinary interval wrapping between terms of that
composed polynomial, and a tube that cannot pass the unchanged strict
Krawczyk/budget gates remains unknown.

P8s1a now adds a separate exact-dyadic complete regular-root census inside one
declared affine moving state domain. It cumulatively replays a canonical
population of full-control-box P5r0.1 patches, maps every complete patch tube
onto exactly one cell of a finite remainder tensor grid in a shared affine slope
coordinate, composes `F(p(u)+delta,u)` once, and exhaustively classifies every
grid cell. A non-patch cell must exclude zero in at least one exact residual
component; an unresolved cell rejects publication. Consequently a passed
certificate proves exactly `N` regular roots for every control in the box,
complete continuation of those `N` sheets across that box, and a complete empty
fold-event set inside the declared moving state domain. Public authority requires
complete replay against the system, patch population, grid, and work limits,
not the top-level content hash.

This is not global branch completion. Roots outside the declared moving state
domain remain unknown; all patches must use the same affine slope and span the
same complete control box; ordinary interval wrapping can reject a valid cover;
and stability is not classified. Stable-root population completeness, Hopf
events, fold-containing domains, global continuation, native
log/transcendental residuals, and true hysteresis remain outside P8s1a.

P8s1b0 now adds a separately versioned complete simple-fold event census for an
exactly one-control polynomial system. It builds the exact augmented polynomial
map `H=(F,det(F_x))`, translates every tensor cell about its exact center, and
requires each cell either to exclude zero in one component of `H` or to prove
one strict augmented Krawczyk root with `beta < 1`. Exhausting the declared
positive `(x,lambda)` domain therefore proves that every singular equilibrium
inside it is one of the reported isolated augmented roots. Nonsingularity of
the augmented Jacobian supplies the corank-one, control-transversality, and
quadratic nondegeneracy conditions for a simple fold. Authority requires full
replay of the system, partition, event seeds, preconditioners, cell decisions,
and cumulative limits; a hash or a failed numerical root search is insufficient.

P8s1b1 attaches one such event to two local regular half-branches without
claiming a global branch graph. It declaratively promotes one selected original
state coordinate to the local continuation control, proves that a central
P5r0.1 chart contains the event, bridges both complete half-patches to that
central chart, and maps each half-tube strictly inside a replayed
original-coordinate regular patch. The covered event-to-half corridors must
stay inside the complete P8s1b0 census domain and be disjoint from the certified
root enclosure of every other fold event. A three-fold adversarial fixture
therefore rejects a purported adjacency when another fold lies between the
selected event and half-patch.

These two contracts remain deliberately local and polynomial. They do not prove
which side of the original control has two roots, stability or stable-root
population completeness, remote component identity, a global branch graph,
fold sheets for two or more controls, Hopf events, native log/transcendental
residuals, global continuation, or true hysteresis.

P8s1c0 now adds `bne-ro-simple-spectral-hopf-event-census/v1.0.0`. It requires
an explicit exactly one-control polynomial dynamics binding, constructs
`P(s)=det(sI-F_x)` with
`P(i*sqrt(z))=E(z)+i*sqrt(z)*O(z)`, and exhausts one exact-dyadic state/control/
frequency-squared tensor cover from `z=0` to strictly beyond an
exact uniform spectral bound. Every reported event passes strict augmented
Krawczyk inclusion, excludes `det(F_x)=0`, has `z>0`, and has a nonsingular
augmented Jacobian; the resulting determinant identity proves an algebraically
simple imaginary pair with nonzero real-part crossing speed, thereby rejecting
a repeated same-frequency pair. Complete pairwise separation of event
state/control projections separately rejects distinct-frequency double-Hopf
points at one equilibrium. Authority belongs to a
source-bound replay of the complete parent census, not to a local event hash.
The focused contract passes 104/104 with bounds checking on Julia 1.10.11 and
1.12.6. A direct read-only audit found no remaining P0/P1/P2 issue after
cumulative-work, determinant-leaf, projection-pair, parent-authority, and
cancellation fixes. By itself this is not a first-Lyapunov, nonlinear-Hopf,
criticality, periodic-orbit, stability-completeness, multi-control-sheet,
native-residual, global-continuation, or hysteresis certificate.

P8s1c1 now adds `bne-ro-complete-nondegenerate-hopf-census/v1.0.0`. It first
preflights all child populations, dimensions, tensor sizes, preconditioners,
fixed exact square-root work, and known cumulative operations, then completely
replays the source-bound P8s1c0 parent. Every parent event must appear exactly
once. Full complex bordered right and Hermitian-adjoint systems, `A` and
second-harmonic resolvents, raw second/third state derivatives, unit-`q`
normalization, and the fixed Kuznetsov formula produce an exact interval `l1`
that must exclude zero. A passed complete lift therefore certifies every parent
event as a nondegenerate local Hopf point with center-manifold supercritical or
subcritical sign. The focused contract passes 102/102 with bounds checking on
Julia 1.10.11 and 1.12.6. Its final direct read-only audit found no remaining
P0/P1/P2 after pre-replay resource, local-record structure, nonnormal-frequency,
two-event canonicalization, and child-stage cancellation hardening. It does not
orient the original-control periodic-orbit side, construct or attach a
validated periodic-orbit branch, certify full-state periodic-orbit stability,
or establish stability completeness, multi-control Hopf sheets, native
residuals, global continuation, or hysteresis.

P8s1c2a now adds
`bne-ro-complete-hopf-periodic-orbit-germ-census/v1.0.0`. It completely
replays P8s1c1 and its P8s1c0 parent, then lifts every parent event exactly
once. With equations ordered `(F...,E,O)` and variables ordered
`(x...,lambda,z)`, the parent bound `norm(I-C*DH,Inf)<1` fixes
`sign(det(DH))=sign(det(C))`. Combining
`det(DH)=det(F_x)*Delta` with the P8s1c0 crossing formula gives

```text
sign(d Re(mu)/d lambda) = -sign(det(C))*sign(det(F_x)),
sign(lambda-lambda_*)   = -sign(l1)*sign(d Re(mu)/d lambda).
```

The classical nondegenerate Hopf theorem therefore supplies a theorem-level
local periodic-orbit germ incident to each event, the original-control side
for sufficiently small nonzero but unquantified amplitude, and radial
attraction (`l1<0`) or repulsion (`l1>0`) on the center manifold. The focused
contract passes 155/155 with bounds checking on Julia 1.10.11 and 1.12.6.
This is not an explicit periodic-orbit enclosure, validated branch,
quantitative amplitude radius, Floquet spectrum, full-state stability result,
periodic-orbit population census, global continuation, or hysteresis proof.
P8s1c2b/P8s1c2c and P8s2 remain design-only.

The pre-c2b proof-kernel foundation now adds
`bne-ro-exact-real-fourier-series/v1.0.0` and
`bne-ro-exact-polynomial-periodic-fourier-residual-audit/v1.0.0`. For one
submitted finite-support trigonometric parameterization, it replays the bound
polynomial dynamics declaration and evaluates
`omega*d_theta(x)-F(x,u)` through every source-generated Laurent mode with
exact `Rational{BigInt}` direct convolution. The versioned rectangular-complex
weighted `l1_nu` receipt reports the retained Galerkin head and every omitted
residual mode separately. A nonconstant positive-frequency parameterization is
one exact ODE periodic solution only when the complete residual is exactly
zero. The focused contract passes 172/172 with bounds checking on Julia
1.10.11 and 1.12.6, including the adversarial case in which every retained
Galerkin equation is zero but mode `k=2` is not. Source-bound replay, derived-
metric reconstruction, separate input/output bandwidth limits, bounded exact
arithmetic/canonical payloads, and cooperative final-publication cancellation
prevent a child self-hash from acting as authority. This layer proves no
infinite-tail/radii enclosure, nearby-orbit uniqueness, periodic-orbit branch,
quantitative amplitude interval, Hopf incidence, minimal period, Floquet or
full-state stability, orbit-population completeness, global continuation, or
hysteresis; it is not P8s1c2b0.

The subsequent P6 working-tree foundation now has two deliberately disjoint
local asynchronous paths. The original sampled `compute_ro_field` path
content-addresses deterministic Cartesian plans, work units, chunks,
checkpoints, and complete manifests. Resume creates a child from the same
owner's terminal failed/cancelled parent and reuses only verified committed
chunks from the same scientific plan. Result reads revalidate the nested
plan/checkpoint/manifest/chunk chain.

The new adaptive v2 path turns the deterministic sparse sampler into a pure
prepare/commit/finalize state machine and then into a separate `local_async`
Job. One multi-index is one backend work unit. Plan, batch, point chunk, state,
checkpoint, terminal result, and dataset manifest are content-addressed; an
uncommitted CAS object left by cancellation cannot enter resume lineage. A
child resumes only from the parent's linearized checkpoint, and final reads
perform one metered authoritative forward replay of every committed transition
plus the plan/terminal/result relationship. The plan also binds the selected
Project/Manifest, engine tree, and `webapp/src/**/*.jl` tree as module-load-
frozen identities, plus Julia and SciML package identities, explicit
homotopy/Tsit5 tolerances, per-point step/RHS caps,
strict Float64 closed-cell membership without best-fit fallback, and a bounded
replay-work model. Resume replay and artifact copying share one cumulative
meter. Plan/model reconstruction precedes that artifact-chain meter. Terminal
result, terminal checkpoint, and manifest bytes are reserved before first
publication, while plan/initial/superseded/orphan objects remain outside a
complete disk-quota claim. The homotopy checks cancellation at every RHS
evaluation; a changed runtime lock cannot extend an older checkpoint. The Web
path admits at most 512 sparse work
units. The v1 Cartesian identity and behavior remain unchanged. Both paths are
local-only and capped research contracts; AWS/Slurm, shared object
storage, remote leases, multi-process work stealing, safe orphan GC, and
cluster recovery remain unimplemented.

Strict slice artifacts now select exactly two free axes from verified 3D or 4D
Cartesian chunk datasets. They reuse source values and gaps with no interpolation
or new evaluation, and default validation requires the plan, manifest, and
chunks together. This proves consistency with the supplied manifest root, not
the authenticity of an untrusted root. The trust anchor must come from the Job
or storage boundary.

P7 adds a Float64 `D=3` face-lattice construction with complete enumerated
cell/facet/ridge/vertex incidence, common refinement, closure, volume, and Euler
checks for explicit affine cell specifications. Its publication gate interprets
the admitted Float64 domain and halfspaces as their bit-exact dyadic rationals
and requires exact pair dimension/non-overlap, opposing support, support/domain
coverage, and volume equality to agree with the Float64 construction. It does
not exact-recompute the complete ridge/vertex lattice and is not an automatic
chemistry extractor, arbitrary-precision-input or arbitrary-real geometry API,
or a `D >= 4` exact-incidence implementation. The existing persisted RPB2
identity remains the 2D contract.

P7b adds an exact regular-extension integrability certificate for that complete
contractible `D=3` box. It interprets admitted Float64 coefficients as exact
dyadic rationals, checks each atomic facet on a three-point affine basis,
reconstructs induced potential-offset jumps, checks every deterministic
dual-graph cotree cycle, and reports supplied affine-potential continuity
separately. It is not a holed-domain period/cohomology certificate, a singular-
branch selection, a `D=3` Clarke query, or a `D >= 4` result.

P8a adds content-bound local identifiability and uncertainty evidence: whitened
sensitivity rank with an explicit numerical grey zone, PSD-factor delta
propagation, declared ensembles or typed bounded explicit coordinate
populations, typed gaps, and computed synthetic coverage. The legacy-named
interval path validates completeness only of the explicitly enumerated finite
coordinate population; it does not certify continuum coverage of the enclosing
box. Its external feature-bound certificate is content-bound and checked
against valid rows, but the engine records that it did not reprove that
certificate. Synthetic coverage is explicitly not
experimental calibration, and none of these artifacts claims global
identifiability, causal validity, or biological validation. A singular-stratum
selector additionally requires a complete declared candidate-population
receipt plus bound stability, reachability, residual, and branch identities; an
incomplete or set-valued population remains unknown. Completeness is currently
caller-declared receipt consistency: counts/content/hashes are checked, but the
enumerator and referenced stability/trace contents are not replayed.

P8b contains a finite supplied-protocol classifier that separates a finite loop,
an untrusted-callback finite-rate-lag candidate, and a conditional equilibrium-
branch loop. Its declarative polynomial branch path recomputes equilibrium
residuals/local stability but does not integrate the ODE.

P8c is a separate finite model-backed numerical trajectory. It integrates one
declarative polynomial vector field from one declared initial state under a
scalar monotone ramp, binds runtime and model/trajectory solver identities,
compares Tsit5 and Vern7, and requires a decreasing child to bind and fully
replay one unlinked increasing predecessor. The vector-field identity now owns
the model time unit, and the protocol binds a structured swept-control/model-
time rate-unit identity before constructing its time grid. Cross-solver agreement is not a
validated error enclosure; model residual evidence is explicitly unknown, and
the trace has no branch/switch certificate. Therefore
`validated_error_enclosure`, `branch_switch_certified`,
`qualifies_as_dynamic_hysteresis`, `global_reachability_certified`, and
`basin_completeness_certified` remain false. Static multiple roots and one
finite trajectory never upgrade to true hysteresis. The older P8b
`complete_dynamic_reachability_evidence` and
`branch_switch_hysteresis_certified` flags likewise remain false.

P9 now prepares deterministic campaign manifests, immutable shard results,
deterministic metadata merge/corpus-lock artifacts, and an independent
identity-map population recount. The enumeration-only reporter names 5,240
supported networks, 41,666
two-dimensional field-plan groups, 69,402 all-rank field-plan groups, and
12,041,474 points in the declared dense `17 x 17` two-dimensional population.
These are preparation counts, not computed fields or successful Atlas records.
Only an at-most-eight-work-unit local demonstration is executable. The complete
campaign has not received separate authorization and has not run. Its corpus
lock proves only the complete declared result-metadata population; QC explicitly
does not recompute addressed field contents or observe external execution.

### Local verification ledger

The current local cycle repeated both complete Julia owner suites after the
P5--P9 implementation, the runtime-identity/replay-budget audit fixes, the
P5r0.1 exact polynomial regular-sheet integration, P8s0 branch-indexed field,
P8s1a complete regular-root census, P8s1b0 complete simple-fold census, and
P8s1b1 local fold/regular-sheet incidence, P8s1c0 complete simple spectral-
event census, P8s1c1 complete nondegenerate local-Hopf lift, and P8s1c2a
theorem-level local periodic-orbit germ integration, plus the pre-c2b exact
finite-support Fourier residual foundation.
All rows are local source/contract evidence, not remote CI, a live cluster, or
an executed complete Atlas campaign.

| Verified check | Local result | Boundary |
|---|---|---|
| BindingAndCatalysis owner suite | Exit 0 after pre-c2b exact Fourier integration; all testsets passed, including affine chart 64/64, P5r0.1 106/106, P8s1a 90/90, P8s1b0 78/78, P8s1b1 70/70, P8s0 76/76, P8s1c0 104/104, P8s1c1 102/102, P8s1c2a 155/155, pre-c2b exact Fourier 172/172, D3 exact integrability 151/151, sparse v2 284/284, P8b 782/782, P8c 151/151, and golden values 145/145; the final headless regime-boundary contract passed 8/8 after its last type-boundary hardening | Julia 1.12.6 local process; finite fixtures and policies |
| Earlier feature audits | P7, P8a, and P8b each ended with P0=0, P1=0, P2=0 for their then-scoped contracts | Review of implemented contracts, not a theorem beyond their declared scope |
| Final runtime/replay audit | Found two P1 issues (load-time identity drift and split resume meters); both were fixed, the focused/full suites rerun, and a final independent review found no new P0/P1. Remaining P2 boundaries are documented: no complete CAS/disk quota; plan/model reconstruction precedes the replay meter; shallow validation is an internal trust path; and distributed runtime identity still needs thread/BLAS/libm/CPU/artifact-ABI fields | Independent code review plus adversarial A/B, cumulative-cap, copy-time tamper, and raw-constructor tests |
| Julia Web/backend owner suite | Exit 0 after pre-c2b exact Fourier integration and the headless regime-boundary repair; 5188 pass, 0 fail/error, and one pre-existing explicit broken test across 5189 assertions; schema drift 57/57, API routes 363/363, backend assembly 199/199, numerical endpoints 25/25, and adaptive sparse Job 141/141 | Standard Web project with headless engine loading on a local Julia 1.12.6 process; no remote provider/cloud execution |
| Phenotyper | 35/35 | Local finite pipeline fixtures |
| Cross-version focused/HPC | P5r0.1 106/106, P8s1a 90/90, P8s1b0 78/78, P8s1b1 70/70, P8s0 76/76, P8s1c0 104/104, P8s1c1 102/102, P8s1c2a 155/155, pre-c2b exact Fourier identity 172/172, sparse engine 284/284, and Web adaptive Job 141/141 passed on Julia 1.10.11; the exact Fourier contract also passed 172/172 on Julia 1.12.6; headless lock selection, instantiate, and package load passed on Julia 1.10.11 and 1.12.6 | Package/lock selection and focused contracts; no live Slurm scheduler |
| Browser JavaScript | Zero-warning lint and complete JavaScript suite passed, including RO-field renderer 14/14 | Node contracts; no browser-to-live-backend rerun in this final cycle |
| Python consumers/contracts | Chat API 12/12, Design Agent 44/44, Reader 15/15, repository Python 134/134 | No live LLM provider or external service |
| Repository gate | Generated then read-only verified: 43 maintained files, 20 schemas, 51 routes | Local catalog/schema/link consistency |

The current increment also has the following independently repeated focused
evidence.

| Current incremental check | Local result | Boundary |
|---|---|---|
| `D=3` exact integrability | 151/151 | Explicit-affine contractible Float64/dyadic fixtures; no holes or `D >= 4` |
| Affine chart admission | 64/64 | Detached public snapshots, content-seal mutation detection, and raw-constructor revalidation; affine charts only |
| Nonlinear input-chart contract | 135/135 on Julia 1.12.6 and included in the exit-0 complete engine owner suite; this final 135-test contract was not rerun on Julia 1.10.11 | Declarative quadratic maps and pointwise numerical immersion only; first-order caller labels are checked but not independently proven; no global injectivity, overlap covariance, or calibration uncertainty |
| Observable-chart contract | 75/75 on Julia 1.10.11 and 1.12.6 | Finite declarative affine/quadratic maps; unit labels only, with no general ratio/log-sum contract |
| P5r0.1 exact polynomial regular sheet | 106/106 on Julia 1.10.11 and 1.12.6 | Exact-dyadic full-tube substitution with one- and two-input nonlinear translated fixtures; uniqueness only inside each declared positive tube, bridge-relative branch identity, no outside-root/global-branch/native-log claim |
| P8s0 branch-indexed polynomial regular field | 76/76 on Julia 1.10.11 and 1.12.6 with bounds checking | Replayed P5r0.1 patches/bridges, conservative exact stability, ordered state log responses, and separated stable-root lower-bound witnesses; no complete population, bifurcation-boundary, global-continuation, or hysteresis claim |
| P8s1a complete regular-root census | 90/90 on Julia 1.10.11 and 1.12.6 with bounds checking | Exhaustive exact-dyadic tensor cover, including nonlinear two-input and two-state/two-input fixtures; complete roots and an empty fold set only inside one declared affine moving domain, with no outside-root, stability-complete, Hopf, fold-containing-domain, global, native-log, or hysteresis claim |
| P8s1b0 complete simple-fold event census | 78/78 on Julia 1.10.11 and 1.12.6 with bounds checking | Exhaustive exact-dyadic `H=(F,det(F_x))` cover for exactly one control; complete isolated simple folds only inside one declared positive augmented domain, with no adjacent-sheet, stability, multi-control event-sheet, native-log, global, or hysteresis claim |
| P8s1b1 local fold/regular-sheet incidence | 70/70 on Julia 1.10.11 and 1.12.6 with bounds checking | Declarative local fold chart, two replayed half-branch bridges, strict original-patch tube containment, and a complete-census corridor excluding intervening folds; no original-control two-root-side, remote-component, stability, global-graph, Hopf, or hysteresis claim |
| P8s1c0 complete simple spectral-event census | 104/104 on Julia 1.10.11 and 1.12.6 with bounds checking; included in the exit-0 complete engine and Web owner suites on Julia 1.12.6 | Exhaustive source-bound `H=(F,E(z),O(z))` cover from zero to beyond a uniform spectral bound for exactly one polynomial dynamics control; complete isolated simple transverse spectral crossings only, with no first-Lyapunov, nonlinear-Hopf, criticality, periodic-orbit, multi-control, native-log, global, or hysteresis claim |
| P8s1c1 complete nondegenerate local-Hopf lift | 102/102 on Julia 1.10.11 and 1.12.6 with bounds checking; included in the exit-0 complete engine and Web owner suites on Julia 1.12.6; final direct read-only audit found no remaining P0/P1/P2 | Complete replayed lift of every P8s1c0 parent event through bordered eigen/adjoint systems, both resolvents, raw `B/C`, and a unit-`q` Kuznetsov `l1` interval excluding zero; center-manifold criticality only, with no original-control side, explicit periodic-orbit incidence, full-state orbit stability, multi-control, native-log, global, or hysteresis claim |
| P8s1c2a theorem-level local periodic-orbit germ lift | 155/155 on Julia 1.10.11 and 1.12.6 with bounds checking; included in the exit-0 complete engine and Web owner suites on Julia 1.12.6; final direct read-only audit found no remaining P0/P1/P2 | Complete c0/c1 replay and one theorem-level germ per nondegenerate Hopf event; strict crossing orientation, original-control side, and center-manifold radial attraction/repulsion for sufficiently small nonzero but unquantified amplitude; no explicit orbit enclosure, validated branch, quantitative amplitude radius, Floquet/full-state stability, orbit-population completeness, multi-control, native-log, global, or hysteresis claim |
| Pre-c2b exact finite-support Fourier residual identity | 172/172 on Julia 1.10.11 and 1.12.6 with bounds checking; included in the exit-0 complete engine and Web owner suites on Julia 1.12.6; independent mathematical and authority/resource audits found no remaining P0/P1/P2 | Complete source-generated exact Laurent residual for one submitted polynomial-ODE parameterization, with Galerkin-head/omitted-tail separation and source-bound replay; no infinite-tail/radii theorem, nearby-orbit enclosure or uniqueness, branch, quantitative amplitude, Hopf incidence, minimal-period, Floquet/full-state stability, population, global, or hysteresis claim |
| Sparse v2 pure transitions | 284/284 on Julia 1.10.11 and 1.12.6 | Finite deterministic policy and portable-token/replay contracts; no continuum error theorem |
| P8b/P8c dynamics | P8b 782/782; P8c 151/151 | Finite deterministic polynomial fixtures; no validated error, branch switch, basin theorem, or true hysteresis |
| Web adaptive sparse Job | 141/141 on Julia 1.10.11 and 1.12.6; legacy Cartesian Job 62/62 | Process-local CAS and lifecycle fixtures; one real heterodimer batch, no shared store or remote executor |

The ordered remaining implementation, independent-QC, and authorization gates
are in
[decision 0006](../decisions/0006-multi-input-ro-field-research-roadmap.md).

## Integrated extension: direct fixed-topology shape control

The previous cat-response prototype asked whether one of three hand-picked
operating-point changes could be built. The integrated revision asks how far one
pinned network can move a typed response edit before its declared path geometry
or parameter bounds stop it. It then chooses a realization with remaining
biochemical parameter room and independently replays the finite curve.

This extension introduces the canonical v1-only
`POST /api/v1/rop_shape_optimize` route, asynchronous local-job support,
typed edits, bounded path/cell enumeration, direct per-cell LPs, explicit
parameter-only margin, active-row/right-hand-side sensitivity, directional
intervals, stored finite replay, Design Screen handoff, an allow-listed Agent
tool, and a fail-closed browser workspace chain. Design Target emits a pinned
reference only for an exact canonical finite-window card; a separate edit-config
node constructs the typed request; a result node calls the v1 route, renders the
evidence layers, and marks restored output as historical until rerun. The three
artifacts use distinct strict port types and are exposed in both Web and macOS
Add Node menus. The Design Screen candidate prior still comes from the tracked
ROP design index (`paper_rop_periodic_table/data/slices.jsonl.gz`, or its explicit
operator override); the optimizer request is self-contained and does not receive
an Atlas SQLite path. The detailed owner and evidence
boundary are in the
[ROP shape-optimization module](../modules/rop-shape-optimization.md) and
[decision 0002](../decisions/0002-rop-shape-margin-and-evidence.md).

The frozen cat benchmark evaluated all 18 eligible paths and all 24 eligible
cells for each of four edits with no truncation. All four direct solves found a
non-grid geometric optimum. Three selected realizations passed complete sampled
finite replay. The `widen_center` realization completed replay but failed its
declared two-peak metric, demonstrating that exact witness geometry is not an
exact nonlinear peak-feature guarantee.

| Integration check | Local result | Boundary |
|---|---|---|
| Full Julia Web suite | No failing tests; the one pre-existing explicit `@test_broken` remains | Local Julia 1.12.6 process, not remote CI |
| Shape core compatibility | 124/124 on Julia 1.12.6 and 124/124 on Julia 1.10.11 | Core LP contract only on 1.10; the full Web suite was not rerun there |
| BindingAndCatalysis suite | All testsets passed; golden-value set 145/145 | Tested models and solver policies only |
| Browser and Python consumers | JS suite passed (including renderer 25/25, reference producer 7/7, shape nodes 12/12); local real-page menu/node/port/intent/fail-closed smoke had zero console errors; Chat API 12/12, Design Agent 39/39, Reader 15/15, and repository Python 103/103 passed | No live LLM provider or browser-to-live-engine optimizer run |
| Frozen cat artifact | Read-only contract 160/160; four geometric optima, three replay passes | One fixed topology and finite replay grid |
| Repository gate | 39 maintained files, 14 schemas, and 49 routes passed regenerated and read-only checks | Local source/catalog consistency only |

These are results for one pinned topology, program, bounds, compiler, and
finite replay grid. They are not evidence of optimality over all networks,
experimental validity, or a live provider/cloud deployment.

## Integrated macOS host maintenance

The integrated revision also hardens the native shell, project
persistence, local helper identity, browser-content boundary, and release
packaging. The configured CI matrix now builds and runs the Swift unit target on
`macos-15-intel` and `macos-26`. On 2026-07-15, a local macOS 27 arm64 run with
Xcode 26.6 passed `build-for-testing` and all unit tests. Browser, Design Chat,
packaged-resource, and release-metadata contracts also passed locally.

This is local evidence retained from revision `f2ca13c` and extended at
`b91cf41`. No remote
CI result, real Cognito flow, packaged helper launch, relocatable Python input,
Developer ID signature, notarization, stapling, Gatekeeper install, or clean-host
qualification is claimed.

The evidence below is local repository evidence. It does not establish that a
remote workflow ran, an image was published, AWS or Slurm accepted a job, or a
signed application was installed.

## Evidence recorded for this snapshot

| Check | Local result through `b91cf41` | Boundary |
|---|---|---|
| Full Julia web suite | Completed with no failing tests; one pre-existing `@test_broken` remains explicitly recorded | Local process only; not evidence of a remote CI run or production traffic |
| `BindingAndCatalysis` suite | All testsets passed; golden-value set 145/145 | Numerical regression coverage is conditional on the tested models and solver policies |
| Browser JavaScript suite | `npm run test:js` passed, including scan-validity, SQLite-policy, and Design Screen rendering contracts | No full browser-to-live-provider Design Agent conversation was exercised |
| Browser quality gate | `npm run lint` passed with zero warnings; local real-Chromium `npm run test:e2e` passed its workflow, v2, axe, and visual contracts | Mocked loopback `/api/v1` endpoints; no remote CI or live Julia/provider/cloud call |
| Native Workspace v2 | Local no-sign build and 51/51 Swift unit tests passed, including shared migration fixtures | No WebView UI automation, packaged-process launch, signing, or clean-host install |
| Release evidence contract | Evidence Schema/template tests passed and the five-lane runbook is prepared | Registry, Compose/TLS, AWS, Slurm, signing/notarization, and rollback lanes were not executed |
| Generated schema check | `julia --project=webapp webapp/scripts/gen_schemas.jl --check` passed | Confirms generated NetworkIR/DesignSpec drift only; hand-authored schemas retain their cataloged coverage levels |
| Deployment contract suite | `python3 tests/test_deployment_contract.py -q` passed, 15/15 | Static and mocked checks do not prove a registry, TLS endpoint, AWS account, or rollback was exercised |

## Verified revision line

| Revision | Result added |
|---|---|
| `d752e15` | Secured loopback startup, Design Chat origin/token handling, native helper launch, and runtime host boundaries |
| `127a2a5` | Added a portable Julia 1.10 lock for the headless HPC environment and removed personal checkout paths |
| `cec0b59` | Synchronized release identity, packaged Design Agent resources, and the split web/HPC CI matrix |
| `01a01be` | Added fail-closed container, image, Compose, TLS, AWS setup, and rollback contracts |
| `603635d` | Moved the backend monolith into focused runtime, model, analysis, placement, scan, geometry, and service files; an assembly contract checks the dependency order and exported surface |
| `1177a3d` | Bounded shared runtime work, serialized mutable state, tightened request validation, preserved numerical failure information, and promoted Design Screen to v0.3 |
| `f2ca13c` | Integrated fixed-topology ROP shape control, durable job ownership, browser lifecycle hardening, deterministic volume contracts, and macOS host maintenance; this is the prior integrated revision |
| `b91cf41` | Added strict typed workflow scheduling, shared result freshness, atomic GraphPatch construction, Workspace v2 JavaScript/Swift parity, measured API aliases, local Chromium/axe/visual gates, and the prepared external release-evidence pack |

## What `1177a3d` establishes

### Shared state and concurrency

- Compiled model bundles have explicit per-bundle ownership, while cache misses
  for the same content hash build once and share the completed result.
- Session aliases and compiled models are bounded state with independent access
  times and least-recently-used eviction behavior.
- Cold regime-graph construction is single-flight, and complete SQLite
  read-modify-write updates are serialized instead of locking only the final
  write.
- The accepted decision record is
  [`0001-shared-runtime-concurrency`](../decisions/0001-shared-runtime-concurrency.md).

### Bounded synchronous work and request handling

- JSON request bodies are limited to 1 MiB at the application boundary and in
  the provided Nginx configuration. Oversized requests return a machine-readable
  413 response.
- Heavy synchronous handlers admit at most a bounded number of concurrent
  requests. Capacity exhaustion returns 429 with retry information; a request
  whose projected or unfolding work exceeds its limit returns 422.
- Model regime candidates, SISO/change paths, atlas builds, corpus reads,
  queries, inverse-design refinement, scans, and geometry sampling now have
  explicit synchronous limits. Larger atlas/library/query/inverse workloads
  belong in the asynchronous job path.
- HTTP SQLite paths are disabled by default. Operator opt-in restricts paths to
  a configured atlas-store root; synchronous writes and persistence remain
  asynchronous-only.

These controls bound one request. They do not prove that every accepted request
will meet a particular latency target on every machine.

### Numerical results and plotting

- One- and two-dimensional scans, atlas landscapes, ROP clouds, and FRET grids
  preserve per-sample success or failure and expose partial-result metadata.
- Non-finite or failed samples are masked before plotting, so the browser keeps
  them as gaps and does not coerce string forms of `NaN` or infinity into data.
- Placement and inverse-design verification do not rank an incomplete numerical
  scan as a successful candidate.

A valid marker means the configured solver produced a finite result for that
sample. It is not experimental validation and does not establish biological
correctness.

### Design Screen v0.3

- `bne-design-screen/v0.3.0` adds `eligible_count`, `evaluated_count`, and
  `truncated` at the top level and in the screen summary.
- The browser displays evaluation coverage and warns when results are not
  exhaustive; it retains a count fallback for v0.2 responses.
- `recommended` remains a compatibility alias for
  `verified_recommendations`. Proxy-only screened candidates are never promoted
  into the verified section.

## Component ownership after `603635d`

`webapp/src/BiocircuitsExplorerBackend.jl` is now an assembly root rather than a
second home for handler implementations. The extracted owners include:

- runtime lifecycle, request support, shared-work limits, and path-work limits;
- model resolution, model handlers, serializers, and numerical computations;
- parameter placement, level solving, parameter scans, and ROP geometry;
- atlas build, corpus, and query budgets; and
- ordinary service handlers.

The exact owner paths and focused tests are listed in
[the module catalog](../catalogs/modules.yaml). The assembly regression test
checks that these files remain included once and in dependency order.

## Earlier verified boundaries retained

- `/api/v1/*` is canonical. Bare `/api/*` routes remain compatibility aliases
  until the sunset projected from executable route metadata into the
  [generated reference](../generated/reference.md#api-routes). All tracked
  first-party clients are canonical; the bounded legacy-request counter informs
  but does not itself authorize alias removal.
- Liveness, readiness, and Prometheus endpoints remain `/health`, `/ready`, and
  `/metrics`.
- The local Design Chat helper remains loopback-only with exact-origin and
  bearer-token checks outside its explicit unauthenticated development mode.
- The Docker workflow and deployment helpers retain their fail-closed version,
  readiness, immutable-reference, AWS-state, and rollback contracts.
- The headless environment retains separate Julia 1.10 and 1.12 dependency
  locks. No real scheduler run is inferred from those files or load checks.
- Atlas and Reader results remain priors in the Design Agent. Fresh engine
  evidence is required before a candidate can be displayed as verified; see
  [the scientific evidence boundary](../contracts/scientific-evidence.md).

## Historical baseline retained

`f9c65a5` remains the baseline revision named by the knowledge manifest and
catalogs. At that inspected revision, the earlier maintenance gate established:

- required Julia tests were active instead of silently treated as expected
  failures;
- inverse-design summaries retained the full reusable-result identity;
- `d=1` single-base homomer candidates were covered by tracked Python and Julia
  checks;
- invalid equilibrium-matrix dimensions failed closed and the SBML bridge kept
  executable identifiers separate from display names;
- local job cancellation was cooperative, terminal states were monotonic, and
  cloud calls did not run while the shared job lock was held;
- the Reader no-fabrication and optional-dependency boundary ran in CI; and
- the degenerate-polyhedron cleanup guard had bounded and unbounded `d=1`
  regression coverage.

Those baseline claims remain historical evidence. Later revisions extend them;
they do not rewrite the catalog's baseline identifier or historical
`current-at-f9c65a5` contract versions.

## Known unknowns and unverified surfaces

1. No remote CI run is claimed for `b91cf41`; the
   evidence table records local commands and configured workflow ownership only.
2. No current evidence establishes publishing to or pulling from a live image
   registry. Image signing, signature verification, and an SBOM release lane
   are also unverified.
3. No live AWS rollout or Slurm run was verified. ECR, Batch, Cognito, S3, IAM,
   quota storage, scheduler behavior, artifact return, and rollback remain
   external unknowns.
4. A signed, notarized, stapled, installed, and Gatekeeper-tested macOS package
   remains unverified.
5. The complete Compose stack with Nginx, a real domain, TLS certificates,
   renewal, readiness admission, and rollback was not run end to end.
6. The deterministic-provider integration contract exercises the production
   Design Agent dispatch against a live local Julia engine through Design Screen
   and Placer. No test contacts a live model provider or proves provider language
   quality, multi-turn convergence, or release-locked retrieval-corpus behavior.
7. Runtime Atlas datasets are optional and not tracked in this checkout. A
   missing dataset manifest means the generic artifact validator cannot prove
   that dataset is reproducibility-pinned.
8. Several hand-authored JSON Schemas still lack instance-level coverage.
   Design Screen v0.3 has direct backend and rendering contracts; that does not
   upgrade unrelated schemas.
9. Paper-side datasets, figures, drafts, and provenance records are deliberately
   untracked. This repository cannot verify manuscript claims or their release
   lineage.
10. Work limits are operational safeguards, not proofs that bounded search is
    scientifically complete. A rejected synchronous request is not evidence
    that the requested circuit or behavior is impossible.

## Verification entry points

Run commands from the repository root:

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
julia --project=Bnc_julia Bnc_julia/test/runtests.jl
(cd webapp && npm run lint && npm run test:js)
(cd webapp && npm run test:e2e && npm run check-i18n-sync)
julia --project=webapp webapp/scripts/gen_schemas.jl --check
python3 tests/test_workspace_schema.py
python3 tests/test_release_candidate_evidence.py
python3 tests/test_deployment_contract.py -q
python3 scripts/verify_repository.py --check
```

Broader release, Python, Reader, HPC, and macOS checks remain separate from
this command list. A successful narrow command does not prove an unrelated
phase complete.

The standard CI owner is
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). The
single-image runtime owner is
[`.github/workflows/docker.yml`](../../.github/workflows/docker.yml). Workflow
configuration is executable ownership of the matrix, not evidence that a
particular remote run passed.

## Current knowledge gate

The generated reference and unified drift checker cover routes, schema
identities, version owners, configured toolchains, links, catalog paths,
public-safety rules, and declared artifact fixtures. The check snapshots the
Git-visible tree and fails on validator side effects. Keep unresolved paper,
release, and external-deployment claims explicitly unknown until recorded
evidence exists.
