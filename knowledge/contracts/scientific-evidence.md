---
title: Scientific evidence contract
status: verified
verified_against: f2ca13c
---

# Scientific evidence contract

The product often answers two questions that sound similar:

1. “Which previously computed candidate should we inspect?”
2. “Did this candidate satisfy the declared target under a named computation?”

The first is retrieval; the second is verification. Mixing them creates polished
but unsupported claims. The same boundary applies inside a numerical run: a
partly computed curve can help diagnose a failure, but it cannot certify the
missing part.

## Evidence classes

### Unknown or unavailable

Use when the engine, corpus, dataset identity, required assumption, or numerical
solve is missing. Return an error, abstention, or `unknown`. Do not substitute a
plausible network, zero, an interpolated value, or a stale number.

### Prior or screened proxy

Examples include atlas seeds, Reader panels, match scores, Reader tiers T1/T2/T3,
shape support, atlas volume annotations, and cards marked `proxy_only` or
`screened_proxy`.

These values can rank work and explain why a candidate was chosen. They do not
show that the candidate was freshly simulated for the current request. The
Design Agent must pass a selected prior through the live compute tool before
presenting it as a verified current-session answer.

### Current computation

A current engine result records the concrete network, inputs, outputs,
parameters or scan policy, algorithm/version, and computed curve, surface,
phenotype, or ROP result. In the conversational layer,
`simulate`/`simulate_2d` create this class of evidence. If the engine reports
offline, the agent must abstain rather than reuse the prior as an answer.

A computation verifies only the declared run. It does not prove robustness over
an unscanned region, global optimality, biological implementability, or a
theorem.

### Fixed-topology shape optimization

An ROP shape result contains several evidence layers that must remain separate:

1. exact path-cell geometry under one pinned topology, finite-window program,
   parameter bounds, compiler version, and evaluated population;
2. the selected biochemical realization and its parameter-only margin in a
   named log-coordinate subspace; and
3. a sampled finite replay of those returned parameters.

`global_optimal_over_declared_cells` is permitted only when every eligible cell
in that declared population was evaluated. A truncated run is best over its
evaluated subset; omitted cells remain unknown. Neither label extends to other
topologies, path builders, bounds, or chemical grammars.

The geometric support limit and selected margin-preserving realization are
different values. A parameter-only radius fixes witness locations and varies
only equality-feasible background `log10(q,K)` coordinates. A joint augmented
radius mixes request-location and parameter slack and cannot be called
biochemical robustness. Active-row shadow prices describe compiled right-hand-
side objective sensitivity, not a parameter derivative.

A finite-shape pass additionally requires the replay's complete Boolean
validity vector and declared sampled metrics. A feasible geometric result with
failed replay remains exact path evidence plus failed finite validation. The
cat benchmark observes this mismatch for one edit, so consumers must not infer
nonlinear peak width or prominence from witness span alone. The accepted
semantics are recorded in
[`0002-rop-shape-margin-and-evidence`](../decisions/0002-rop-shape-margin-and-evidence.md).

### Complete and partial numerical evidence

An equilibrium point is valid only when the solver reports success and the
reported quantity is finite. A non-converged solve, `NaN`, or infinite value is
an invalid gap. It is not a zero and must not be joined to neighboring points to
invent a continuous response.

Current response contracts expose this distinction:

- 1D scans, placer curves, and qK-space ROP clouds return `valid` and `partial`;
- 2D scans, atlas landscapes, and FRET heatmaps return `validity_grid` and
  `partial`;
- placement verification returns `verification_validity` and
  `verification_partial` where applicable;
- inverse refinement records `refinement_status`, valid/sample counts, and
  `partial`.

Partial output may be displayed as a diagnostic with gaps and an explicit
warning. It cannot certify a dose-response shape, threshold, realized behavior
program, feature, robustness clause, or recommendation. It also cannot rerank a
candidate above complete evidence.

Placement passes only when every required verification point is valid. A partial
refinement receives failure status and a losing finite score; it is excluded
from `best_candidate`. Final inverse-design selection uses the original query
ranking whenever `rerank_by_refinement=false` or no valid refined candidate
exists.

### Multi-input reaction-order evidence

A multi-input reaction order is an ordered matrix field in a declared input
chart. Axis order, units/scales, fixed background, output order, chart identity,
and numerical-validity mask are part of the evidence. A correlated-control
pullback verifies the chain rule only for its admitted affine chart; it does not
show that an arbitrary nonlinear experimental control map is invertible.

The P5n working-tree chart adds one strictly bounded nonlinear foundation: a
declarative quadratic `theta(u)` with a closed finite domain and no mapping
callback. Its constructor admits the reference Jacobian, and every later point
evaluation recomputes the local Jacobian and applies the recorded numerical
rank/condition policy. A passed point is therefore a numerically admitted local
immersion only. It is not a domain-wide injectivity, covering, or non-overlap
certificate, and finitely many passing points do not repair that gap. The
first-order pullback uses the local Jacobian. For a scalar output, admissible
second-order evidence must retain the complete formula

```text
H_u z = J_theta(u)' H_theta z J_theta(u)
        + sum_a (partial z / partial theta_a) H_u theta_a.
```

Omitting the second term changes the mathematical artifact and can reverse a
mixed-curvature sign. First-order arrays must declare the exact source component
names/order, unit labels, and source point used by the chart evaluation. These
declarations prevent accidental anonymous permutation or point mismatch, but
the finite API does not independently prove that an upstream producer labelled
the array honestly. Scalar source-derivative receipts additionally bind the
chart declaration SHA-256 and output name/unit, so derivatives from another
chart, output, or point are not interchangeable. P5n does not yet provide chart-
transition maps, overlap covariance, a global self-intersection test, or a
separate chart-calibration uncertainty propagation.

P5o is a separate output-coordinate contract. It currently admits only finite
declarative C2 affine or quadratic maps `y = phi(z)` with ordered source/output
components, references, a closed source domain, symmetric declared Hessians,
and content identity. Its complete composition is

```text
R_y = J_phi(z) R_z
H_u y = J_phi(z) H_u z + H_phi(z)[R_z, R_z].
```

Both terms must remain visible in second-order evidence. Observable unit strings
are opaque identity labels: P5o performs neither dimensional algebra nor unit
conversion, so coefficient compatibility remains a caller assumption. Ratio,
log-sum, normalized-occupancy, and other general non-polynomial observables are
not implemented by relabelling a row or by this quadratic chart.

P5r0.1 now supplies one narrow regular-equilibrium-sheet evidence class for square
declarative polynomial systems in strictly positive linear coordinates. Every
Float64 coefficient, bound, predictor, and preconditioner is interpreted as its
exact binary rational. The complete affine tube `x=p(u)+delta` is substituted
into each residual and like terms are cancelled before interval evaluation.
Differentiation with respect to `delta` supplies `[F_x]`; differentiation with
respect to `u` at fixed `delta` supplies the correlation-preserving enclosure
`[G]` for `G=F_u+F_x*S`; and `F_u=G-F_x*S` is also reconstructed symbolically.
A positive patch requires an exact full-rank preconditioner, strict coordinate-
wise Krawczyk inclusion, and an exact infinity-norm bound `beta < 1`. Its
implicit-derivative enclosure uses

```text
E = I - C [F_x]
Q = -C [G]
||dx/du - S||_infinity <= ||Q||_infinity / (1 - beta).
```

This proves one root for every control and uniqueness inside the declared root
tube; it explicitly does not exclude roots outside that tube. A child inherits a
parent branch identity only through a replayed full-dimensional overlap patch
whose tube contains both entire patch tubes. Neither nearby centers nor point
residual agreement is a branch bridge. Certificate authority requires replay
against the bound polynomial system, not a certificate self-hash alone.

P5r0 does not cover the engine's native log/transcendental residuals, arbitrary
callbacks, global root populations, folds/Hopf events, or global continuation.
P5r1 remains design-only and would require a direct pinned interval dependency.
A successful equilibrium retcode, small point residual, finite inverse, or point
condition number outside a passed P5r0 patch is still not root-tube,
nonsingularity, implicit-derivative, or branch evidence.

P8s0 adds a narrower branch-indexed polynomial evidence class on top of P5r0.
The source must be rebound explicitly as a vector field `dx/dt = F(x,u)`; time
and state-rate unit strings are content identities only, and neither dimensional
algebra nor physical-model truth is certified. Every supplied patch and bridge
is rebuilt under a cumulative exact-operation budget. The undirected connected
components of passed overlap bridges are therefore evidence-relative
continuation components, not an enumeration of every physical branch.

For one replayed patch, let `[J] = [F_x]`. The implemented stability policy uses
the exact row bounds

```text
g_i = upper([J_ii]) + sum_{j != i} max(abs(lower([J_ij])), abs(upper([J_ij]))).
```

If every `g_i < 0`, Gershgorin's theorem puts every eigenvalue strictly in the
left half-plane throughout the tube and the patch is uniformly locally
asymptotically stable. If an exact lower bound on `trace(J)` is positive, at
least one eigenvalue has positive real part and `trace_lower/state_count` is a
lower bound on the rightmost real part. Every other case is
`unknown_stability`; failure of this sufficient policy is never relabelled as
instability. P5r0 nonsingularity excludes a fold only inside its declared tube,
and the stable Gershgorin result excludes a Hopf crossing only inside that
stable tube. It does not locate either boundary.

The associated matrix is specifically the state-coordinate log response

```text
d log(x_i) / d log(u_j) in [u_j] * [1/x_i] * [dx_i/du_j],
```

with every ordered input column retained. It is not automatically an arbitrary
observable response; an admissible output chart must be composed separately.
A multistability witness selects one uniformly stable patch per bridge component
on one common positive control box and requires exact restricted state tubes to
be strictly separated for every selected pair. This proves only a lower bound
on the number of distinct stable roots throughout that box. Stable-root
population completeness, folds outside selected tubes, fold/Hopf boundary
enclosures, global continuation, and true hysteresis are all forced false.
Certificate authority requires replay against the bound polynomial system and
the full supplied patch/bridge population.

P5r0.1 closes the earlier artificial-width failure caused by evaluating repeated
state and control occurrences as independent intervals. In particular, the
translated cubic `-(x-u)(x-u-1)(x-u-2)` and a nonlinear two-input translated
surface retain their exact path cancellations in the focused contract. This is
not a general zero-wrapping theorem: interval evaluation can still overestimate
between distinct terms of the composed `(u,delta)` polynomial. Any such rejected
patch remains unknown and must pass the same strict Krawczyk gate rather than
weakening `beta` or substituting a point Jacobian.

P8s1a adds one bounded population-completeness evidence class. For one common
control box `U`, affine predictor `p(u)`, and finite exact-dyadic tensor
partition `{Delta_k}` of a closed remainder box, complete replay must establish
exactly one of these cases for every cell:

1. one exact interval component of `F(p(u)+delta,u)` excludes zero throughout
   `U x Delta_k`; or
2. exactly one replayed P5r0.1 tube spans `U`, uses the common affine slope, and
   maps exactly onto `Delta_k`.

The closed cells cover the declared moving state domain, every patch root lies
strictly inside its cell, and all other cells exclude a root. A passed census
therefore proves exactly `N` regular roots for every `u in U`, complete
continuation of those `N` sheets across `U`, and a complete empty fold-event set
inside that domain. This is an omission theorem from exhaustive interval
exclusion, not an inference from numerical root search failure. Authority
requires replay of the bound polynomial system, canonical patch population,
grid, every cell decision, and cumulative work limits; content hashes alone are
not scientific certificates.

P8s1a says nothing about roots outside its declared moving domain and fails
closed if any cell remains unresolved. It does not classify stability, admit a
domain containing a fold, enclose Hopf events, certify global continuation,
cover native log/transcendental residuals, or establish hysteresis.

P8s1b0 adds a complete event-population statement with a different domain and
zero set. For an exactly one-control polynomial equilibrium system, it forms

```text
H(x, lambda) = (F(x, lambda), det(F_x(x, lambda)))
```

and exhausts an exact-dyadic tensor partition of one declared positive
augmented domain. Every cell must either exclude zero in one interval component
of `H` or prove exactly one strict interior augmented Krawczyk root with
`beta < 1`; an unresolved cell rejects the whole census. The nonsingular
augmented Jacobian then forces `corank(F_x)=1` and the two ordinary simple-fold
nondegeneracy conditions `w'F_lambda != 0` and `w'F_xx[v,v] != 0`. A passed
census therefore gives the complete isolated simple-fold set inside that
augmented domain. It says nothing about singular equilibria outside the domain,
and it is not a stability, Hopf, branch-incidence, or multi-control fold-sheet
certificate.

P8s1b1 is a local incidence certificate, not a global branch graph. It
declaratively uses one selected original state as a continuation coordinate,
proves the event lies on a central P5r0.1 chart, and connects complete lower and
upper local half-patches to that chart through replayed bridges. Each local
half-tube must map strictly inside one replayed original-coordinate regular
patch. In addition, the whole event-to-half corridor must remain inside the
complete P8s1b0 census domain and be separated from the certified augmented-root
enclosure of every other fold event. This last condition is essential: two
geometrically nearby patches are not adjacent if another fold lies between
them.

The positive P8s1b1 wording is only
`validated_local_two_half_branch_incidence_at_one_simple_fold`. It explicitly
does not identify which side of the original control has two roots, classify
stability, identify remote components, enumerate a global root/branch graph,
continue a fold sheet through additional controls, certify Hopf events, or
establish hysteresis.

P8s1c0 adds a complete **simple spectral-event census** for exactly one control
and one explicitly bound polynomial vector field `dx/dt=F(x,lambda)`. An
equilibrium residual declaration without that dynamics binding has no spectral
meaning and is rejected. With

```text
P(s) = det(sI - F_x) = sum_k a_k s^k,
E(z) = sum_m (-1)^m a_(2m) z^m,
O(z) = sum_m (-1)^m a_(2m+1) z^m,
```

the identity `P(i*sqrt(z))=E(z)+i*sqrt(z)*O(z)` turns a nonzero imaginary pair
into the phase-free square system `H=(F,E,O)=0` in `(x,lambda,z)`. The exact-
dyadic tensor cover starts at `z=0` and ends strictly beyond `R^2`, where an
exact uniform row-sum bound gives `abs(mu) <= R` for every eigenvalue `mu` of
`F_x` throughout the declared state/control domain. Thus every possible
imaginary frequency in that domain lies inside the covered frequency range.
Every cell must component-exclude `H=0` or contain one strict augmented
Krawczyk root. An event cell additionally excludes `det(F_x)=0`, and its root
enclosure lies strictly in `z>0`.

The augmented-Jacobian test has a direct transversality interpretation. Along
the regular equilibrium branch, write

```text
dot(E) = E_lambda - E_x F_x^(-1) F_lambda,
dot(O) = O_lambda - O_x F_x^(-1) F_lambda,
Delta  = dot(E) O_z - E_z dot(O).
```

Then `det(DH)=det(F_x)*Delta`. At a root with `omega=sqrt(z)`,
`P_s(i*omega)=2z*O_z-2i*omega*E_z`; nonsingularity of `DH` therefore makes the
imaginary pair algebraically simple and gives
`d Re(mu)/d lambda = -Delta/(2*(E_z^2+z*O_z^2)) != 0`. Algebraic simplicity
rejects a repeated same-frequency pair. Pairwise strict separation of the
state/control root projections of every reported event separately rejects a
distinct-frequency double-Hopf point. A source-bound replay of the complete
parent census therefore proves the complete isolated simple transverse
spectral-event population inside the declared domain. A local event self-hash
has no independent authority to exclude another frequency at the same
equilibrium.

P8s1c0 alone deliberately stops before a nonlinear Hopf theorem. P8s1c1 adds a
separately versioned **complete nondegenerate local-Hopf lift**. Authority still
starts by replaying the complete source-bound P8s1c0 census; every parent event
must occur exactly once in the child population. Missing, duplicate, foreign,
or degenerate child events reject the whole lift.

For each parent root enclosure, P8s1c1 fixes the Taylor convention

```text
F(x_*+y) = A y + (1/2) B(y,y) + (1/6) C(y,y,y) + O(norm(y)^4),
```

where `B` and `C` are the raw second and third state derivatives. It obtains an
outward exact-rational `omega` enclosure from `z=omega^2`; validates the full
bordered systems for `Aq=i*omega*q` and `A^T p=-i*omega*p` with Hermitian
normalization; and separately validates both required resolvents. With the
Kuznetsov convention,

```text
h11 = A^(-1) B(q,qbar),
h20 = (2i*omega*I-A)^(-1) B(q,q),
G21 = <p, C(q,q,qbar) - 2B(q,h11) + B(qbar,h20)>,
l1  = Re(G21) / (2*omega),
```

the published `l1` is converted to unit Euclidean `q` scale and its exact
interval must be strictly negative or strictly positive. This upgrades every
parent event to a nondegenerate local Hopf point and reports center-manifold
supercriticality (`l1<0`) or subcriticality (`l1>0`). A local P8s1c1 event hash
still has no authority without the complete parent replay.

P8s1c2a adds a separately versioned **complete theorem-level local
periodic-orbit germ lift**. It completely replays P8s1c1 and its P8s1c0 parent,
then requires exactly one germ child for every nondegenerate Hopf event. The
orientation formula fixes parent equations as `(F...,E,O)` and variables as
`(x...,lambda,z)`. If `C` is the exact parent preconditioner, the strict bound
`norm(I-C*DH,Inf)<1` gives a nonsingular homotopy from `I` to `C*DH`, hence
`det(C*DH)>0` and `sign(det(DH))=sign(det(C))`. Therefore

```text
det(DH) = det(F_x)*Delta,
alpha   = d Re(mu)/d lambda
        = -Delta/(2*(E_z^2 + z*O_z^2)),

sign(alpha) = -sign(det(C))*sign(det(F_x)).
```

In the convention-bound radial normal form

```text
r_dot = r*(alpha*(lambda-lambda_*) + l1*r^2 + higher order),
```

the sufficiently small nonzero periodic-orbit germ lies on the side
`sign(lambda-lambda_*)=-sign(l1)*sign(alpha)`. The nondegenerate Hopf theorem
then supports `local_periodic_orbit_germ_exists` and
`theorem_level_hopf_event_incidence_certified`; the latter means the germ tends
to this unique event as amplitude tends to zero, not that a computed orbit
enclosure intersects an event box. `l1<0`/`l1>0` gives radial attraction/
repulsion on the center manifold at onset only.

P8s1c2a supplies no explicit periodic-orbit enclosure, validated quantitative
branch, amplitude radius, Fourier-tail or validated-flow proof, Floquet
spectrum, full-state stability result, periodic-orbit population completeness,
global continuation, or hysteresis. Its complete count refers only to lifting
the named P8s1c1 event population. P8s1c2b must add a desingularized explicit
periodic-orbit branch with a rigorous infinite Fourier tail/radii-polynomial
proof or a separately versioned validated-flow alternative; a finite harmonic-
balance/Galerkin Krawczyk proof is not enough for a true ODE orbit. P8s1c2c must
separately validate Floquet spectra for full-state stability. P5r1 and P8s2
remain required for the corresponding native-residual claims.

The implemented pre-c2b exact Fourier identity layer is a narrower evidence
class, not a partial c2b certificate. For a finite real trigonometric
polynomial written as

```text
x(theta) = sum_k c_k exp(i*k*theta),
c_(-k)   = conjugate(c_k),
```

it binds one polynomial dynamics declaration, constant controls, a strictly
positive angular frequency, and the complete state order. Direct exact-
rational Laurent convolution retains every intermediate mode and evaluates
`omega*d_theta(x)-F(x,u)` through the full support generated by the source;
truncating after any intermediate product is forbidden. With the versioned
rectangular-complex sequence norm

```text
|z|_square = |Re(z)| + |Im(z)|,
||c||_nu   = |c_0|_square
             + 2*sum_(k>=1) nu^k*|c_k|_square,
nu > 1,
```

the artifact reports the retained Galerkin-head residual and every higher
generated residual mode separately. If the complete residual is exactly zero,
the frequency is positive, and at least one nonzero harmonic exists, then
`X(t)=x(omega*t)` is one exact ODE periodic parameterization. This proves that
`2*pi/omega` is a period, not that it is the minimal period.

Every use requires validation by replay against the bound source and dynamics;
the object and its content hash have no independent authority. The constructor
recomputes all residual norms, omitted-mode metadata, and derived flags, while
separate input/output bandwidths, exact-operand/payload/work limits, source-
term preflight, and cooperative cancellation bound the proof-kernel work. A
zero finite Galerkin head with any nonzero omitted mode is explicitly a failed
full ODE identity. Even a passed exact identity supplies no infinite Fourier-
tail/radii theorem, nearby-orbit enclosure or uniqueness, periodic-orbit
branch, quantitative amplitude/control interval, constructive Hopf incidence,
Floquet or full-state stability, orbit-population completeness, global
continuation, or hysteresis. P8s1c2b0 still requires the uniform
desingularized product-space/radii proof defined in the roadmap.

Keep the following positive claims separate:

- `consistent_on_tested_grid` is a finite sampled circulation/mixed-partial/
  output-edge check, not continuum integrability;
- `regular_cell_extension_integrable` is a complete declared Float64 PWA
  regular-cell certificate, not a selected physical singular branch. The D3
  form additionally requires exact-dyadic tangential compatibility, consistent
  induced offsets around every dual-graph cotree cycle, and continuity of the
  supplied potentials on a complete contractible box. Compatible-gradient
  existence and supplied-potential continuity remain separately reported;
- a Clarke query returns the hull of intact incident MIMO Jacobian matrices;
  independently mixing rows from different cells invents unsupported evidence.
  Its current directional values are fixed-direction images of those generators,
  not tangent-cone-selected Bouligand derivatives;
- a singular-branch selection is conditional on its complete declared candidate
  receipt and matching residual, stability, and reachability procedures. In the
  current contract this is caller-declared receipt consistency: the engine
  checks submitted counts/content/hashes but does not run the enumerator or
  recompute the referenced stability analysis or dynamic trace. Zero, one, or
  multiple admitted candidates mean unknown, unique-relative-to-that-population,
  or set-valued respectively;
- a curvature or synergy label is relative to its named chart, scale, finite
  window, threshold, and policy, and is neither a causal nor an experimental
  interaction claim;
- the current 3D face lattice is a complete enumerated Float64 incidence
  construction whose publication gate reinterprets admitted Float64 inputs as
  exact dyadic rationals for pair dimension/non-overlap, support, domain, and
  volume predicates. This does not make it an arbitrary-precision-input or
  arbitrary-real geometry certificate, nor does it exact-recompute all
  ridge/vertex incidence. Its exact regular-extension certificate does not cover
  holed-domain cohomology, `D >= 4`, D3 Clarke queries, or singular-branch
  selection; and
- an adaptive sparse stopping indicator describes the finite deterministic
  refinement policy that ran. The v2 plan/state/index-batch/result tokens make
  prepare/commit/finalize transitions portable and exactly replayable, with one
  multi-index per backend work unit and invalid descendant cones kept
  unresolved. Neither the stopping indicator nor token replay is a uniform
  continuum error bound unless an independent theorem supplies one.

Local identifiability binds a whitened sensitivity/Fisher model, observation
schedule, noise model, numerical rank policy, and source identity. It does not
establish global or individual biochemical identifiability. First-order delta
covariance is a local linear propagation result. Ensemble quantiles, externally
declared certificate-bound feature bounds, structural alternatives,
numerical gaps, and synthetic coverage remain distinct. The legacy-named
interval path checks those bounds against its enumerated valid rows and binds
the external certificate, but does not reprove it or cover the enclosing
continuum. No summary may interpolate across a gap or relabel a synthetic
fixture as experimental calibration.

Dynamic evidence is not an equilibrium RO field. The supplied-protocol analyzer
can recompute equilibria and local Jacobian stability for a declarative
polynomial vector field, but it does not integrate that field. Its closed
forward/reverse lineage, exact paired controls, complete equilibrium evidence,
and paired switch records therefore support only a
`conditional_equilibrium_branch_loop`.

A separate trajectory artifact now integrates a deterministic polynomial vector
field from a declared initial state over one finite scalar linear ramp. It binds
state bounds, fixed controls, a vector-field-owned model-time unit, a
structured swept-control/model-time rate identity, solver/runtime identities and work
limits, requires primary Tsit5 and tighter Vern7 results to agree on the save
grid, and fully replays an increasing-to-decreasing predecessor lineage. This is
`complete_model_backed_finite_protocol_trajectory` evidence when its finite
checks pass. Solver agreement is not a validated error enclosure; the artifact
does not independently certify model residuals, events, branch identity, basin
or global reachability, or a multi-input control-space loop. Its
`validated_error_enclosure`, `branch_switch_certified`,
`qualifies_as_dynamic_hysteresis`, `global_reachability_certified`, and
`basin_completeness_certified` flags remain false. The older P8b
`complete_dynamic_reachability_evidence`,
`branch_switch_hysteresis_certified`, and
`qualifies_as_dynamic_hysteresis` flags likewise remain false. An untrusted
callback supports only a finite-rate-lag candidate, and static multiple roots
remain static evidence. Zero-rate or global hysteresis, cross-rate robustness,
structural stability, and experimental causality remain unproved.

Content-addressed chunks prove bytes and declared point order, not scientific
validity. The local adaptive v2 path additionally binds canonical plan, batch,
point-result, state, checkpoint, terminal-result, and manifest artifacts and
performs one metered authoritative forward replay of every committed transition
before accepting a result or child resume. Its plan binds the selected runtime
lock, engine source, and Web source tree as module-load-frozen identities,
explicit solver tolerances and per-point work caps, in-RHS cancellation, strict
cell containment with no best-fit regime label, and bounded artifact-chain
replay work. Plan/model reconstruction occurs before that replay meter.
A complete checkpoint or manifest still proves coverage only of its finite
plan. A campaign corpus lock and independent metadata recount prove only the
supplied declared result-metadata population unless the addressed field
artifacts are separately loaded and revalidated. Preparation counts are not
execution authority, and no complete local, Slurm, AWS, or other distributed
Atlas campaign may be claimed without its separately authorized execution
evidence.

### Designability recommendation

`screened_candidates` are exploratory. A card belongs in
`verified_recommendations` only when the screen has exact or sampled enforced
evidence and all hard constraints required by that path pass. Preserve both
`evidence_grade` and `certificate_grade`; labels such as `proxy-only`,
`finite-grid sampled`, `exact-window-siso-rop-path`, and
`exact-union-siso-rop` carry different scopes.

The screen's own summary states the invariant: screened candidates are never
proof. In `bne-design-screen/v0.3.0`, `eligible_count` reports the available
catalogue population, `evaluated_count` reports the bounded prefix actually
checked, and `truncated` marks omitted work. An unevaluated candidate is unknown,
not a failed or verified candidate. A compatibility alias such as `recommended`
must equal the verified list, not the screened list.

### Enumerative or theorem evidence

Existence can be supported by a replayable witness within a declared grammar.
Absence, minimality, and completeness require a certificate or theorem whose
assumptions cover the searched space. A bounded candidate search explicitly does
not certify absence for non-trivial cells
(`src/periodic_table/candidate_search.py`).

Never upgrade “not found,” “not present in this atlas,” or “zero sampled volume”
to “impossible.” State the grammar, bounds, engine/config identity, and whether
the result is witness-only, enumeration-relative, or analytic.

## Required provenance for a reusable result

At minimum retain:

- exact input network/spec and content hash;
- input/output selection and units or log-coordinate convention;
- algorithm and application revision;
- classifier, quantization, and support semantics where ROP programs are used;
- parameter bounds, sampling/solver policy, random seed where applicable;
- the validity vector/grid, invalid/non-converged points, and
  cancellation/partial status;
- result artifact identity and creation time;
- the command or API request needed to reproduce the result.

The result artifact envelope supplies part of this list. Missing scientific
assumptions must remain in the result-specific payload or release manifest.

Canonical relabeling is exact only through seven free species. Above that bound,
identity falls back to deterministic positional content. A reusable result for a
larger model must therefore preserve the original symbol ordering; do not infer
that a renamed payload has the same artifact identity.

## Transfer into a manuscript

Software output becomes a publication claim only after the manuscript
repository records a claim entry that pins:

- claim ID and exact wording;
- strength: illustrative, sampled, enumeration-relative, or theorem;
- assumptions and excluded interpretations;
- Explorer revision and artifact release identity;
- dataset hashes and semantically defined counts;
- reproduction command and expected check;
- figure/table consumers;
- allowed and forbidden wording.

The standalone paper repository owns that ledger and its artifact lock. Explorer
owns the computation and release manifest. Weekly reports preserve dated history
but cannot override either owner. See `knowledge/research/repositories.md`.

## Hard wording guards

Do not write:

- “verified” for an atlas/Reader-only result;
- “passed,” “matched,” or “reranked” from a curve or surface with invalid gaps;
- “robust” without the parameter region, metric, and complete validity evidence;
- “volume” without saying whether it is normalized mass, box fraction, a bound,
  or geometric volume;
- “complete,” “minimal,” or “impossible” from bounded search alone;
- “all networks” without the grammar and structural bounds;
- a resolved periodic-table slice/network count while the catalog marks its
  semantics `unknown`.

Prefer scoped wording such as “the current engine run satisfied…,” “the valid
subset was diagnostic but incomplete,” “a witness exists within grammar G and
bounds B,” or “no candidate was found by bounded search S.”

## Regression evidence

- `webapp/scripts/test_design_agent_contract.py` checks the agent/tool boundary.
- `webapp/scripts/reader/test_reader_nofabrication.py` checks unavailable-corpus
  behavior and prevents Reader candidates from being flagged as verified.
- `webapp/test/designability_spec_contract.jl` and
  `webapp/test/design_screen_contract.jl` check proxy/verified separation,
  evidence grades, complete sampled evidence, hard constraints, and certificate
  semantics.
- `webapp/test/concurrency_and_budget_contract.jl` checks invalid-refinement
  selection, synchronous work limits, and machine-readable failures.
- `webapp/test/rop_shape_optimization_contract.jl`,
  `rop_shape_api_contract.jl`, `rop_shape_replay_contract.jl`, and
  `rop_shape_cat_benchmark.jl` check coordinate-basis separation, bounded
  population language, direct LP semantics, complete replay, and the frozen
  surrogate mismatch.
- scan, placement, and frontend validity contracts check that invalid points are
  exposed and rendered as gaps.
- `Bnc_julia/test/ro_coordinate_chart_contract.jl`,
  `ro_nonlinear_coordinate_chart_contract.jl`,
  `ro_observable_chart_contract.jl`,
  `ro_regular_sheet_contract.jl`,
  `ro_branch_indexed_field_contract.jl`,
  `ro_field_differential_contract.jl`, `ro_stratified_field_contract.jl`,
  `ro_stratified_field_3d_contract.jl`,
  `ro_singular_selection_contract.jl`, `ro_field_uncertainty_contract.jl`,
  `ro_dynamic_hysteresis_contract.jl`, `ro_dynamic_trajectory_contract.jl`,
  `ro_sparse_sampler_contract.jl`, and
  `ro_cell_complex_3d_contract.jl` check the separated multi-input mathematical
  evidence classes, identity, gaps, adversarial constructor paths, budgets, and
  cancellation.
- `webapp/test/ro_field_chunks_contract.jl`,
  `ro_field_job_contract.jl`, `ro_field_sparse_job_contract.jl`,
  `ro_field_slices_contract.jl`, and
  `ro_field_campaign_contract.jl` check layered content identity, immutable
  Cartesian and adaptive-sparse resume lineage, complete transition replay,
  exact-index/no-interpolation slice provenance,
  preparation-only authority, merge,
  and metadata-only campaign QC.
- `tests/test_periodic_table.py` checks result statuses and small/trivial witness
  behavior for the standalone periodic-table layer.

Passing these tests enforces important wording/data boundaries; it does not
certify a manuscript claim that has no pinned artifact and claim entry.

## Verified against

- Multi-input working-tree extension: `codex/multi-input-ro-field` based on
  `9b05d02`, locally audited on 2026-07-17, including the declarative quadratic
  pointwise nonlinear input chart, finite affine/quadratic observable chart, and
  exact-dyadic polynomial P5r0 patch/bridge certificates plus the P8s0
  branch-indexed stable-root-witness field, P8s1a complete regular-root census,
  P8s1b0 complete simple-fold census, P8s1b1 local fold incidence, P8s1c0
  complete simple spectral-event census, P8s1c1 complete nondegenerate
  local-Hopf lift, and P8s1c2a complete theorem-level local periodic-orbit germ
  lift. P8s1c0 passed 104/104, P8s1c1 passed 102/102, and P8s1c2a passed
  155/155 with bounds checking on Julia 1.10.11 and 1.12.6; the P8s1c2a final
  direct audit found no remaining P0/P1/P2. Complete engine and standard Web
  owner suites both exited zero after P8s1c2a integration; the Web suite retained
  its one pre-existing explicit `@test_broken`. P5r1, P8s1c2b/P8s1c2c, multi-control
  fold/Hopf sheets, and native-residual P8s2 remain unimplemented designs. This
  is not a committed revision or external campaign result.
- Shape-optimization evidence extension: committed integration revision
  `f2ca13c`, locally verified on 2026-07-15.
- Earlier implementation anchor: `1177a3d`.
- Historical baseline: retrieval-versus-verification and provenance wording was
  audited at `f9c65a5`; that historical evidence predates the explicit
  validity/partial contracts and cannot establish them.
