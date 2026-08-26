# 0006: Advance multi-input RO fields through evidence-separated research phases

- Status: accepted-working-tree
- Date: 2026-07-17
- Verified against: `9b05d02` plus the scoped
  `codex/multi-input-ro-field` working tree
- Owners: `engine-rop`, `backend-runtime`, `atlas`
- Extends: [0005](0005-multi-input-reaction-order-field.md)
- Supersedes: none
- Superseded by: none

## Outcome

The project will treat multi-input reaction order as a family of related but
non-interchangeable artifacts:

1. a field in an explicitly declared input chart;
2. a separately identified differential-analysis artifact;
3. an exact regular-cell extension plus a set-valued singular-limit layer;
4. sampled, sparse, sliced, and chunked numerical views;
5. separate singular-selection, uncertainty, identifiability, and
   dynamical-response artifacts; and
6. a corpus manifest whose population and execution evidence are explicit.

The current `bne-ro-field/v1.0.0`, RPB1, and RPB2 meanings remain frozen.
Research additions do not silently widen those identities. A future field v2 is
allowed only when a new canonical chart or higher-dimensional exact face lattice
cannot be represented without changing v1 semantics.

## Why the gaps must remain separated

An equilibrium field may be a gradient of a single-valued output potential, but
that does not make every sampled tensor integrable, every singular regime
single-valued, or every dynamical system history-independent. Likewise, a
content-addressed chunk proves byte identity, not scientific validity, and a
completed finite corpus proves only facts about its declared population.

The implementation therefore keeps four evidence questions distinct:

- **mathematical consistency**: do derivatives and affine pieces satisfy the
  declared local or finite-grid relationships?
- **numerical validity**: did the solver produce finite, converged values at the
  evaluated coordinates?
- **population completeness**: which points, cells, networks, or work units were
  eligible, evaluated, omitted, or invalid?
- **external execution**: did a separately authorized local, Slurm, AWS, registry,
  or experimental campaign actually run and return its artifacts?

No layer may promote a weaker answer into a stronger one.

## Theory program

### T1. Correlated controls require a coordinate chart

The engine coordinates are ordered source coordinates `theta`. Experimental or
design controls are ordered coordinates `u`. The first admitted chart is affine:

```text
theta = b + A u
```

where `A` has full column rank and passes recorded numerical rank and condition-
number gates. The reaction-order pullback is

```text
R_u = R_theta A.
```

This supports correlated totals, sum/difference controls, and rotated input
coordinates without pretending that dependent controls are independent partial
derivatives. P5n now provides a separately versioned, bounded declarative
quadratic chart that records `theta(u)`, its local Jacobian and source-component
Hessians, a closed finite domain, pointwise immersion scope, and numerical
conditioning. It does not reuse the affine chart version or accept a mapping
callback. The constructor admits only the reference Jacobian, and each evaluated
point reruns the numerical immersion/conditioning gate.

The chart check is numerical admission evidence, not a symbolic proof of rank.
Invalid source slices remain gaps; a tensor pullback must not transform NaNs into
apparently valid derivatives. A first-order array must declare the exact ordered
source-axis names, unit labels, and evaluated source point. This catches an
anonymous permutation or point mismatch but cannot independently authenticate
an upstream producer's labelling. A scalar second-order receipt additionally
binds the chart declaration hash and output name/unit.

The remaining chart theory is global and uncertain. P5n rejects a local
fold/non-immersion or rank/conditioning grey zone at a point, but it has no
domain-wide injectivity/self-overlap proof, chart-overlap transition map, or
overlap-covariance certificate. Calibration uncertainty in the chart must still
propagate separately from biochemical-parameter uncertainty. No global
coordinate claim is allowed until an injectivity or covering certificate spans
the declared domain.

For a nonlinear chart, curvature does not transform by the affine sandwich
alone. For one output `z`, the required chain rule is

```text
H_u z = J_theta(u)' H_theta z J_theta(u)
        + sum_a (partial z / partial theta_a) H_u theta_a.
```

The second term is identically zero only for an affine chart. Omitting it can
change both mixed-curvature magnitude and the sign of a synergy classification.

Input charts do not define the output coordinates. The P5o foundation now types
a finite declarative C2 affine or quadratic observable chart `y = phi(z)`,
including component order, unit labels, references, a closed source domain, and
regularity. At first order it verifies

```text
R_y = J_phi(z) R_z.
```

At second order it retains both the source-input Hessian and the Hessian of
`phi` in the full composition. Unit strings are identity labels only; the
current layer performs no dimensional algebra or conversion. Ratios, log-sums,
normalized occupancies, and other general non-polynomial observables remain
unimplemented and cannot be treated as row renamings of `R_z`.

### T2. Integrability has sampled and exact evidence classes

For each output of a smooth single-valued field, the regular gradient should
satisfy mixed-partial equality. On one elementary sampled `(i,j)` face, the
finite-grid audit evaluates:

```text
closed-loop trapezoid circulation,
d R_i / d u_j - d R_j / d u_i,
observed output-edge change - integrated reaction order.
```

The only positive sampled wording is `consistent_on_tested_grid`. Other outcomes
are `discrete_inconsistent`, `unknown_gap`, or `insufficient_grid`. Even a
gap-free result is not a continuum proof.

For a finite equilibrium model `F(x,u)=0`, an ordinary converged solve is not
enough to define a differentiable RO sheet. P5r must validate a regular sheet by
enclosing the residual, using interval Newton/Krawczyk or equivalent evidence
to prove one root in a declared box, proving an enclosure of `partial F/partial
x` nonsingular, enclosing the implicit derivative, and binding a branch
identity. A finite inverse returned by an unvalidated linear solve near a
singular Jacobian remains a numerical gap, not implicit-function evidence.

The reviewed implementation path splits this gate without weakening it. P5r0
first admits only a declarative multi-control polynomial equilibrium system in
strictly positive linear coordinates. Every admitted Float64 coefficient and
bound is interpreted as its exact dyadic rational value, so a bounded rational
interval/Krawczyk implementation can be replayed without adding a numerical
interval dependency. For an affine root tube
`p(u) = x_hat + S*(u-u_hat)` with remainder box `Delta`, it must publish the
enclosures of `F`, `F_x`, and `F_u`, an exact full-rank preconditioner `C`, the
Krawczyk image, and strict coordinate-wise interior margins. It must also prove
`beta = sup(norm(abs(I-C*[F_x]), Inf)) < 1` and use that contraction to enclose
`dx/du`; point residuals, `cond(F_x)` at one point, or a finite `inv(F_x)` are
not substitutes.

P5r0 proves one regular root inside one declared tube for every control in its
box; it does not prove that roots outside the tube are absent. A child patch may
inherit a branch identity only through a separately replayed overlap bridge
whose tube contains both patch roots and again passes the uniqueness gate.
P5r1 may later add `IntervalArithmetic` as a direct versioned dependency to
cover the engine's native log/transcendental residuals and more general
non-polynomial charts. The package is currently only a transitive manifest
entry and therefore is not an available owner dependency. Neither P5r0 nor
P5r1 may accept an opaque caller callback as validated enclosure evidence.

P5r0.1 is now implemented in a separate owner. It first expands
`F(p(u)+delta,u)` as an exact rational polynomial in controls and remainder
coordinates and combines like terms, so correlations such as `x=u` are not
destroyed by treating predictor states and controls as independent intervals.
Differentiation with respect to `delta` produces `[F_x]`; differentiation with
respect to `u` at fixed `delta` produces `[G]` for `G=F_u+F_x*S`; and the
published `[F_u]` is reconstructed symbolically as `G-F_x*S`. It then publishes
the predictor/full-tube residual enclosures, `[F_x]`, `[F_u]`, `[G]`, `E`, the
Krawczyk image and strict margins, `beta`, and the implicit-derivative enclosure.
A directed overlap bridge reruns the patch proof and proves both entire patch
tubes lie inside its tube. Exact-operand bit length, expansion population,
cumulative interval work, and cancellation are bounded. The classical fixed-box
inclusion/uniqueness gate
follows the Krawczyk theorem summarized in
[Alefeld's verified-computation review](https://na.math.kit.edu/alefeld/download/2009_Verified_Numerical_Computation_for_Nonlinear_Equations.pdf),
while the parameterized use is aligned with modern certified homotopy work
([Duff and Lee, 2024](https://arxiv.org/abs/2402.07053)).

P5r0.1 closes the adversarial translated-branch gap that the earlier natural
full-tube Jacobian form exposed. For
`F(x,u)=-(x-u)(x-u-1)(x-u-2)`, the exact `(u,delta)` polynomial makes the path
total derivative vanish before interval evaluation and admits the regular
moving outer branches under the unchanged strict Krawczyk gate. A nonlinear
two-input fixture likewise recovers the exact predictor slope `[1,2]`. This
removes one specific dependency inflation, not all interval wrapping: distinct
terms of the composed polynomial can still widen an enclosure, expansion work
can hit its hard limit, and any `beta >= 1` result remains rejected/unknown.
Loosening `beta`, shrinking a reported bound after the fact, or accepting a point
Jacobian remains an invalid repair.

P5r1 remains a strict design only. No native log/transcendental enclosure owner
or direct interval dependency exists. Existing solver retcodes, point residuals,
and finite point inverses outside a replayed P5r0 patch remain numerical
evidence, not regular-sheet evidence.

For a complete, single-valued piecewise-affine complex, a stronger regular-cell
certificate requires all of the following:

- complete, non-overlapping coverage of the declared box;
- exactly one finite affine label per full-dimensional cell;
- closed facet incidence and complete cell-boundary coverage; and
- equality of adjacent affine output values on each full internal facet.

For a 2D facet, equality at its two endpoints is sufficient because both sides
are affine. The resulting wording is
`regular_cell_extension_integrable`. It is a certificate for the regular
extension only, not a model of a singular branch.

The working-tree `D=3` certificate now covers a complete contractible box for
the explicit-affine Float64 face lattice. It reinterprets admitted Float64 bits
as exact dyadic rationals, checks adjacent gradients on a three-point affine
basis of every atomic facet, solves the induced potential-offset equations on a
deterministic dual-graph spanning forest, and checks every cotree cycle. It
reports gradient integrability separately from continuity of the affine offsets
supplied by cell labels. This is stronger than the sampled-grid audit, but it is
not a singular-branch model or an arbitrary-real certificate.

`D >= 4`, non-contractible domains, and a `D=3` set-valued Clarke query remain
open. Domains with holes require explicit global periods/cohomology; pairwise
mixed-partial symmetry or a dual graph that ignores domain topology is only
local evidence. Higher-dimensional exact incidence also remains a prerequisite
for applying the same construction beyond `D=3`.

The exact PWA/asymptotic field and the finite equilibrium model are currently
separate evidence families. P7f must connect them for the same chemistry and a
declared asymptotic parameter `epsilon`: on compact sets bounded away from
facets it must give output and Jacobian/RO error bounds with a convergence rate,
while separately identifying boundary layers and exceptional/singular sets.
Agreement on a few fixtures is not a finite-to-asymptotic theorem.

### T3. Curvature and synergy are policy-dependent diagnostics

The raw first-order derivative of the RO field is retained as

```text
J_R[output, component_i, derivative_j] = d R_i / d u_j.
```

It is not silently symmetrized. The symmetric Hessian projection, the discarded
antisymmetric residual, eigenvalues, and the independently computed finite-window
output contrast are all recorded. A mismatch remains visible.

The first synergy convention is explicitly named
`positive_log_cross_curvature`:

- positive mixed output curvature above a threshold:
  `synergistic_under_policy`;
- negative curvature below the negative threshold:
  `antagonistic_under_policy`;
- magnitude within the threshold: `neutral_under_policy`; and
- an invalid cell: `unknown_gap`.

These labels depend on chart, scale, window, output, and threshold. They are not
causal, mechanistic, or experimental interaction claims. Future alternatives
such as multiplicative null models, Bliss/Loewe-style experimental policies, or
information-theoretic interaction must use separately named contracts.

The current second-order layer is finite-difference evidence. A smooth-model
upgrade needs sensitivity equations or automatic differentiation plus solver
error control. A PWA asymptotic field has zero classical curvature inside each
cell and jump/distributional curvature on facets; that measure-valued object
must have its own identity and cannot be fabricated by smoothing across a
singular gap.

### T4. Singular layers use joint-matrix limits, not an arbitrary branch

For a continuous single-valued piecewise-affine regular extension, a boundary
query returns the Clarke generalized-Jacobian hull generated by the complete
incident matrices:

```text
conv{J_1, ..., J_r},   J_l in R^(output_count x input_count).
```

For MIMO output, every full matrix is one coupled generator. Constructing a
separate hull per output and taking their Cartesian product would invent joint
matrices that belong to no incident regular cell. Directional derivatives must
multiply every intact matrix by the same direction for the same reason.
The current `RODirectionalDerivativeGenerators2D` values are fixed-direction
images of those Clarke generators. They are not tangent-cone-selected
Bouligand derivatives and do not prove that every returned generator is
reachable from the queried direction.

This layer is marked `regular_limit_only=true` and
`includes_singular_branch=false`. A separate conditional selector may name one
singular branch only when a typed complete-candidate receipt and branch-bound
residual, stability, and dynamic-reachability evidence all validate under the
same model, chart, stratum, and policy identities. Otherwise its result is
set-valued or unknown. This is a certificate relative to the declared
candidate population and evidence procedures, not a universal law of physical
branch selection; the engine must never choose the first serialized label.
In the current P5s implementation, “complete” is caller-declared population
consistency: the engine checks submitted counts, candidate contents, identities,
and receipt hashes, but does not run or replay the named enumerator and does not
load or recompute the stability analysis or dynamic trace behind their hashes.
Its positive conclusion is therefore conditional on those declarations and
finite evidence statuses.

A model-derived physical selection law remains open. It would need a complete
steady-state/root population or a theorem that bounds omissions, stability
spectra with numerical enclosures, basin/reachability evidence for a declared
dynamics and initial-condition distribution, and a policy for noise-induced or
nonunique selection. The conditional selector does not supply these ingredients
by itself.

P8s is split into three evidence levels. Implemented P8s0 binds a P5r0
polynomial declaration explicitly as `dx/dt`, replays every supplied patch and
bridge, and treats passed bridge connectivity as an evidence-relative branch
graph. It encloses every ordered state-coordinate
`d log(x_i)/d log(u_j)`, uses exact row-Gershgorin bounds for uniform local
stability and positive exact trace for instability, and otherwise stays
unknown. A multistability witness selects one stable patch per replayed
component on a common positive control box and proves only `at least N` distinct
stable roots by pairwise strict tube separation. P5r0 nonsingularity excludes a
fold only inside each selected tube; stable Gershgorin evidence excludes a Hopf
crossing only inside that stable tube. P8s0 does not locate either boundary.

P8s1 is staged rather than represented by one premature completeness flag.
Implemented P8s1a closes one bounded regular-root subproblem. Implemented
P8s1b0/P8s1b1 add a complete isolated simple-fold census for exactly one control
and local adjacent-regular-sheet incidence around each selected event.
Implemented P8s1c0 now adds a complete isolated simple transverse spectral-
event census for exactly one explicitly bound polynomial vector field and one
control. Implemented P8s1c1 now completely lifts those events through a
nonzero first-Lyapunov certificate and center-manifold criticality. Implemented
P8s1c2a now lifts each event to a theorem-level local periodic-orbit germ,
original-control side, and center-manifold radial attraction or repulsion for
sufficiently small nonzero but unquantified amplitude. P8s1c2b is split into
three constructive gates: c2b0 validates one amplitude-pinned Fourier/radii
segment from one replayed event, c2b1 lifts every named parent event, and c2b2
adds general pseudo-arclength segments with strict branch-identity bridges.
P8s1c2c must separately validate Floquet/full-state stability. Multi-control
fold and Hopf sheets remain separately versioned extensions. P8s2 then lifts
those contracts to native
log/transcendental residuals and admissible observable charts after P5r1 and the
relevant chart work exist.
`stable_root_population_complete`, general fold/Hopf-boundary enclosure, and
global continuation remain false. Until P8s2 passes, native engine residuals
remain outside this branch-field evidence. At every level, failure to find
another branch is `unknown` unless the named population certificate proves
otherwise.

P8s1a implements a complete **regular-root census inside one declared affine
moving state domain**. Let `U` be one connected control box,
`p(u)=x_hat+S(u-u_hat)`, and let a finite exact-dyadic tensor grid partition a
closed remainder box `Delta` into cells `Delta_k`. The certificate may publish
completeness only if every cell satisfies exactly one of two replayed cases:

1. exact interval evaluation of at least one component of
   `F(p(u)+delta,u)` excludes zero on `U x Delta_k`; or
2. exactly one P5r0.1 patch has the same complete control box and slope, and its
   entire root tube maps exactly onto `Delta_k` in the global remainder
   coordinates.

Because the closed grid covers `Delta`, the first case excludes every root in
that cell and the second proves exactly one root, strictly interior and with
nonsingular `F_x`, for every `u in U`. Therefore the zero set inside
`p(u)+Delta` consists of exactly `N` regular `C1` sheets, where `N` is the
number of admitted patch cells; the fold-event set inside this declared domain
is complete and empty. Shared cell faces do not double-count a root because a
P5r0.1 root lies strictly inside its cell, while every non-patch cell excludes
zero. This argument is a finite cover theorem, not an inference from failure to
find another numerical solution. Parameterized Krawczyk tracking supplies the
regular-path ingredient ([Duff and Lee, 2024](https://arxiv.org/abs/2402.07053));
the omission result comes separately from exhaustive interval
subdivision/exclusion, consistent with certified real-root isolation workflows
([Ji et al., 2013](https://arxiv.org/abs/1303.5503)).

P8s1a remains bounded in three explicit ways. It says nothing about roots
outside the declared moving state domain; all admitted root sheets must span
the one complete control box with a common affine slope coordinate; and ordinary
interval wrapping may leave a grid cell unresolved. An unresolved cell rejects
the whole census as unknown. It does not establish stable-root population
completeness, because stability may remain unclassified even after every root is
known. `global_continuation_certified`, Hopf-event completeness, and true
hysteresis remain false.

P8s1b now handles isolated folds for exactly one control coordinate by covering
the augmented polynomial system `F(x,u)=0, det(F_x)=0`, including
nondegeneracy, event-box separation, and local incidence with adjacent regular
sheets. P8s1c is split further: P8s1c0 handles the complete simple spectral-
crossing population, P8s1c1 handles first-Lyapunov nondegeneracy and
criticality, P8s1c2a handles theorem-level local germ existence/orientation,
P8s1c2b0 handles one desingularized amplitude-pinned validated segment,
P8s1c2b1 handles the complete named parent-event lift, P8s1c2b2 handles
general pseudo-arclength continuation and strict segment gluing, and P8s1c2c
handles Floquet/full-state stability. Validated Hopf continuation
requires all of this additional structure; it is not a consequence of seeing a
stability-label change ([van den Berg, Lessard and Queirolo,
2021](https://epubs.siam.org/doi/10.1137/20M1343464)).

P8s1b is itself staged. Implemented P8s1b0 isolates the complete simple-fold
event set for an exactly one-control polynomial system; implemented P8s1b1
attaches the two local half-branches to replayed regular-sheet evidence. For
P8s1b0, write the
single control as `lambda` and define the square augmented polynomial map

```text
H(x, lambda) = (F(x, lambda), det(F_x(x, lambda))).
```

Let a finite exact-dyadic tensor grid cover one declared positive event domain
`B` in `(x,lambda)`. Every closed cell must either exclude zero in at least one
exact interval component of `H`, or pass a strict exact Krawczyk proof for one
unique zero of `H` in the cell interior. The preconditioned augmented Jacobian
must satisfy `beta < 1`; an unresolved cell rejects the complete event census.
Because `B` is exhaustively covered, every singular equilibrium in `B` is then
one of the admitted augmented roots.

The regularity of an augmented root is also the fold nondegeneracy gate. If
`D H` is nonsingular at `H=0`, then `F_x` cannot have corank two or more: in that
case `adj(F_x)=0`, every first derivative of `det(F_x)` vanishes, and the last
row of `D H` would be zero. Thus `F_x` has one-dimensional right and left
kernels, spanned by `v` and `w`. Since
`adj(F_x)=gamma v w^T` for nonzero `gamma`, nonsingularity of `D H` is
equivalent to both standard simple-fold conditions

```text
w^T F_lambda != 0,
w^T F_xx[v,v] != 0.
```

This determinant formulation is a finite-polynomial specialization of
validated saddle-node methods that solve a nondegenerate augmented system and
use constructive fixed-point/implicit-function arguments
([Sander and Wanner, 2016](https://epubs.siam.org/doi/10.1137/16M1061011)).
Validated continuation of fold branches requires additional branch-cover
machinery rather than merely locating event points
([Lessard, Sander and Wanner, 2017](https://www.aimsciences.org/article/doi/10.3934/jcd.2017003)).

P8s1b0 therefore certifies a complete set of isolated simple fold points only
inside `B`. By itself it does not prove that the two local half-branches reach
any supplied P5r0.1 patch, identify the side with two roots, classify stability,
or continue through another control dimension. With two or more free controls,
generic fold events form a codimension-one set rather than isolated points;
that still requires a separate validated event-sheet contract.

P8s1b1 supplies the missing local constructive cover without widening P8s1b0's
population claim. For a selected original state coordinate `x_k`, it
declaratively rewrites the polynomial system with `x_k` as the continuation
control and the original `lambda` as one state. A central P5r0.1 patch must
contain the fold-event root enclosure. Complete lower and upper P5r0.1
half-patches lie strictly on opposite `x_k` sides and connect to the central
patch through full-dimensional replayed bridges. Each half-tube is then mapped
back to original coordinates and must lie strictly inside one supplied regular
patch.

Geometric proximity alone is insufficient for the word "adjacent". For each
half, the complete event-to-half corridor in the central tube must remain
strictly inside the P8s1b0 declared augmented domain and must be separated, in
at least one coordinate, from the certified augmented-root enclosure of every
other census event. The complete census then rules out an intervening fold on
that covered local branch corridor. A three-fold adversarial polynomial fixture
tests that omission: selecting the outer event while the middle event lies in
the corridor is rejected.

The resulting scope is local two-half-branch incidence at one isolated simple
fold. It does not orient the two-root side in the original control coordinate,
identify remote continuation components, classify stability, assemble a global
branch graph, or establish hysteresis.

P8s1c0 now implements the first Hopf-related layer without overloading it with
a nonlinear conclusion. It requires the polynomial declaration to be bound
explicitly as the vector field `dx/dt=F(x,lambda)`. For

```text
P(s) = det(sI - F_x) = sum_k a_k s^k,
E(z) = sum_m (-1)^m a_(2m) z^m,
O(z) = sum_m (-1)^m a_(2m+1) z^m,
```

the exact identity `P(i*sqrt(z))=E(z)+i*sqrt(z)*O(z)` permits the phase-free
augmented polynomial system

```text
H(x,lambda,z) = (F(x,lambda), E(x,lambda,z), O(x,lambda,z)).
```

One exact-dyadic tensor grid covers the complete declared positive state/control
domain and a frequency-squared interval beginning exactly at zero. The upper
endpoint must lie strictly beyond `R^2`, where exact interval row sums prove
that every eigenvalue of every admitted `F_x` satisfies `abs(mu)<=R`. Every
cell either excludes zero in one component of `H` or contains one strict
augmented Krawczyk root. An event cell additionally excludes `det(F_x)=0`, and
the enclosed root has `z>0`.

There is no hidden eigenvector phase condition. Along the regular equilibrium
branch define

```text
dot(E) = E_lambda - E_x F_x^(-1) F_lambda,
dot(O) = O_lambda - O_x F_x^(-1) F_lambda,
Delta  = dot(E) O_z - E_z dot(O).
```

The block determinant identity `det(DH)=det(F_x)*Delta` and
`P_s(i*sqrt(z))=2z*O_z-2i*sqrt(z)*E_z` show that a nonsingular augmented root
has one algebraically simple imaginary pair and a nonzero real-part crossing
speed

```text
d Re(mu)/d lambda = -Delta / (2*(E_z^2 + z*O_z^2)).
```

Augmented-Jacobian nonsingularity rules out a repeated same-frequency pair.
Pairwise disjoint state/control projections of all event-root enclosures
separately rule out a distinct-frequency double-Hopf point at the same
equilibrium. Complete source-bound replay of the parent census is essential:
the local event hash alone cannot exclude another frequency. This establishes
only the complete isolated simple transverse **spectral-event** set inside the
declared domain.

P8s1c1 now implements the nonlinear point gate. It fixes a full bordered
right/Hermitian-adjoint normalization, encloses the raw second- and third-order
multilinear vector-field terms and both resolvents, publishes a unit-`q`
Kuznetsov first-Lyapunov interval `l1`, and requires `0 notin l1` for every
event in a completely replayed P8s1c0 parent. This certifies nondegenerate local
Hopf points and convention-bound center-manifold super/subcriticality.

P8s1c2a now implements the theorem-level germ gate. For the fixed equation
order `(F...,E,O)` and variable order `(x...,lambda,z)`, the parent strict
Krawczyk bound implies `det(C*DH)>0`. Hence

```text
sign(d Re(mu)/d lambda) = -sign(det(C))*sign(det(F_x)),
sign(lambda-lambda_*)   = -sign(l1)*sign(d Re(mu)/d lambda).
```

After completely replaying P8s1c0/P8s1c1, every parent event receives exactly
one theorem-level local periodic-orbit germ, its original-control side, and
center-manifold radial attraction or repulsion for sufficiently small nonzero
but unquantified amplitude. This is an asymptotic theorem conclusion, not a
constructed periodic-orbit enclosure or quantitative continuation segment.

P8s1c2b is the next constructive gate. It must implement the desingularized
form `x(theta)=y+a*v(theta)` with equilibrium, phase, amplitude, and
pseudo-arclength conditions; a strict infinite-dimensional Fourier-tail/
radii-polynomial proof (or a separately versioned validated-flow alternative);
quantitative amplitude/control coverage; source-bound incidence to one event in
a completely replayed P8s1c0/P8s1c1/P8s1c2a authority chain;
and resonance, tangency, branch-misattribution, work, replay, and cancellation
fixtures. Finite harmonic balance or a Galerkin Krawczyk proof alone cannot
certify a true ODE orbit. The parameterized Newton--Kantorovich/Fourier
continuation design follows [van den Berg and Queirolo,
2021](https://www.aimsciences.org/article/doi/10.3934/jcd.2021004).

The first incidence segment should not use a free pseudo-arclength coordinate
at the singular endpoint. For `theta in [0,2*pi]`, write the physical period as
`2*pi*tau`, construct the polynomial divided difference symbolically, and use

```text
Fbar(y,a,v,lambda)
    = (F(y+a*v,lambda)-F(y,lambda))/a,
v_theta = tau*Fbar(y,a,v,lambda),
F(y,lambda) = 0,
Re(v[anchor,1]) = 1,
Im(v[anchor,1]) = 0,
a = a_plus*s,                 s in [0,1].
```

The expanded `Fbar` is evaluated directly at `a=0`; interval division by an
amplitude box containing zero is forbidden. The anchor must be the same
P8s1c1 right-eigenvector anchor. This fixes phase and scale. Under the full
complex Fourier convention used here, the positive `k=1` coefficient of the
physical orbit is `a` and its paired real cosine amplitude is `2a`; this factor
of two and the c1 anchor normalization are versioned. The construction avoids
the degeneracy of using `lambda-lambda_* = O(a^2)` as the onset continuation
coordinate.

The proof space is a conjugate-symmetric weighted sequence algebra with the
exact rectangular complex norm

```text
|z|_square = |Re(z)| + |Im(z)|,
||v||_nu   = sum_k |v_k|_square*nu^|k|,   nu > 1.
```

The sequence norm is combined with separately weighted exact scalar blocks for
`y`, `tau`, `a`, and `lambda`; those product weights are part of the proof
identity. Direct convolution retains every intermediate mode; an FFT may generate a
numerical seed but cannot be the proof kernel. For a retained half-bandwidth
`K`, the real finite-core dimension is `2*n*K + 2*n + 3`. The finite residual
must be evaluated through every source-generated mode (up to `d*K` for degree
`d`), while the infinite complement uses an analytic diagonal inverse and
source-derived `Y`, `Z0`, `Z1`, and `Z2` bounds. More precisely, each segment
binds its parameterized equation `H_s`, approximate center `xbar_s`, bounded
injective approximate inverse `A_s`, and fixed-point map
`T_s=I-A_s*H_s`. `Y` bounds `A_s*H_s(xbar_s)` and `Z` bounds `DT_s` on the
complete product-space ball, with separate finite scalar/Fourier blocks and an
analytic infinite-tail block. For a common exact radius
`0 < r <= r_star`, every block must satisfy the strict inequality

```text
p_j(r) = Y_j + (Z0_j + Z1_j - 1)*r + Z2_j*r^2 < 0.
```

These bounds must hold uniformly for every `s` in the segment, not only at its
endpoints. Exact Bernstein bounds or an equivalent outward enclosure should
own the segment-coordinate polynomial. The contraction must also imply
continuous dependence on `s`; unrelated pointwise roots are not a branch. The
artifact must prove the infinite approximate inverse is bounded and injective,
retain conjugate symmetry, keep the complete state dimension, and bind the
finite-core packing, norm, tail inverse, phase, amplitude, and parent
conventions by version.

Constructive onset incidence has its own endpoint gate. At `a=0`, `tau` must
stay positive, `z=tau^-2`, and `(y,lambda,z)` must lie in the selected c0
Krawczyk root enclosure. The first Fourier mode must lie in the corresponding
c1 anchored right-eigenvector enclosure, while the same infinite-dimensional
uniqueness proof excludes unsupported higher endpoint modes and tails. The
c2a germ is selected only through its replayed c0/c1 parent hashes. Box
proximity, equal frequency, or equal `l1` is insufficient. Quantitative
control-side coverage additionally uses the correlation-preserving form
`lambda(a)=lambda_*+a^2*eta(a)` and proves that `eta` has the c2a-predicted
strict sign; subtracting independent lambda intervals and dividing by an
amplitude interval containing zero is invalid.

One cumulative work context starts with the complete c2a replay count. Before
allocation, exact `BigInt` preflight covers `D=2*n*K+2*n+3`, the full `d*K`
generated support, divided-difference monomial expansion, direct convolution
pairs and intermediate support, finite matrices/preconditioners, Bernstein
pieces, canonical bytes, and exact operand bits. Cooperative cancellation
reaches monomial, convolution, finite-core, tail, and segment-bound loops.

The implementation gates are deliberately narrower than the umbrella c2b
name:

- **P8s1c2b0:** one source-bound amplitude-pinned Fourier/radii segment after
  completely replaying the c0/c1/c2a censuses and selecting one c2a germ by its
  replayed parent hashes. It may certify true ODE periodic-orbit tubes
  for `a in (0,a_plus]`, the quantitative amplitude interval, constructive
  incidence at `a=0`, containment of `y+a*v(theta)` and `lambda` in the
  declared positive state/control domain, and uniqueness only inside its
  declared normalized Fourier tube. It must not claim a complete parent-event
  lift.
- **P8s1c2b1:** under one declared and versioned segment policy, exactly one
  source-bound c2b0 child for every event in the completely replayed c2a census.
  The policy fixes admissible `K`, exact `nu`, exact product weights, and
  `a_plus`. Caller order, frequency, `l1`, box proximity, or a child self-hash
  cannot establish parent attribution. Its completeness is only completeness
  of this named parent-event lift.
- **P8s1c2b2:** general pseudo-arclength patches away from `a=0`, with a
  uniformly nondegenerate continuation functional and separately replayed
  common-root bridges. Finite-head or state/control projection overlap is not
  a branch-identity proof.

Before c2b0, the implemented exact finite-support Fourier residual audit now
provides a proof-kernel foundation and adversarial gate. It uses the same
conjugate-symmetric full-complex coefficient convention and exact rectangular
`l1_nu` norm fixed above, performs direct rational Laurent convolution without
intermediate truncation, and recomputes every source-generated Fourier mode.
When the complete residual vanishes as an exact Laurent-polynomial identity,
it proves that one submitted trigonometric
polynomial is a true ODE solution. It still proves no nearby orbit, branch,
amplitude interval, Hopf-event incidence, or uniqueness and therefore is not a
c2b stage. Source-bound replay is the authority, and bounded input/output
bandwidths, exact work, canonical payloads, structural metric reconstruction,
and cooperative cancellation are part of the artifact contract. Its focused
fixture exposes the case where every retained Galerkin equation is zero but the
first omitted mode is nonzero.

P8s1c2c must then validate the periodic variational equation on closed
positive-amplitude subsegments `a in [a_min,a_max]` with `a_min>0`, isolate the
one trivial phase multiplier, and enclose every nontrivial Floquet multiplier
or exponent before publishing full-state stability. A different desingularized
spectral theorem would be required for a uniform claim on a closed segment that
contains onset, where another multiplier approaches one. The validated Floquet normal-
form route is described by [Castelli and Lessard,
2013](https://epubs.siam.org/doi/10.1137/120873960). Passing c2b0 proves only
one declared local tube, c2b1 completes only the named parent-event lift, and
c2b2 extends only the explicitly bridged local coverage. Passing c2c adds
Floquet/full-state stability only on its positive-amplitude subsegments.
Minimal period outside the anchored convention, periodic-orbit population
completeness, and global continuation remain unknown throughout.

### T5. Multistability and hysteresis require a dynamical contract

Different paths through a regime partition do not by themselves prove
hysteresis. Genuine hysteresis requires at least:

- a dynamical model and state variables;
- initial-condition or branch identity;
- a time-dependent input protocol and direction;
- stability and event/branch-switch policy;
- solver tolerances and convergence/termination evidence; and
- a loop or protocol comparison whose history-dependent separation exceeds a
  declared numerical uncertainty.

The dynamical evidence family must distinguish:

- multiple equilibrium roots found at one input;
- locally stable branches;
- protocol-tracked state trajectories;
- switching events; and
- a measured hysteresis loop.

None of these may be stored as an ordinary equilibrium RO field. Failure to find
a second branch in a bounded root search remains `unknown`, unless a complete
certificate applies.

The supplied-root finite-protocol analyzer remains intentionally below a global
hysteresis theorem. Its declarative polynomial path can recompute equilibrium
residuals and local Jacobian stability, but a caller-supplied sequence of stable
roots and self-consistent switch records does not prove that the ODE trajectory
reaches those roots or switches between their basins. Its positive wording is
therefore only `conditional_equilibrium_branch_loop`.

P8c adds a separate finite model-backed numerical trajectory. Starting from one
declared initial state, it integrates a declarative polynomial vector field
under one scalar monotone ramp, records distinct model and trajectory solver
policies, and compares Tsit5 and Vern7 on the declared save grid. A decreasing
child must bind and fully replay one unlinked increasing predecessor. Solver
agreement is numerical cross-check evidence, not a validated error enclosure;
model residual evidence remains unknown, and the artifact contains no branch or
switch certificate. Consequently `complete_dynamic_reachability_evidence`,
`branch_switch_hysteresis_certified`, and
`qualifies_as_dynamic_hysteresis` remain false for the older P8b evidence. For
P8c, `validated_error_enclosure`, `branch_switch_certified`,
`qualifies_as_dynamic_hysteresis`, `global_reachability_certified`, and
`basin_completeness_certified` remain false.

The next dynamic phase requires vector protocols `u(t) in R^D`, event and branch
identity, validated trajectory/residual enclosures, protocol families rather
than one loop, and experimental time/control uncertainty. It must also validate
coefficient dimensions rather than treating equation semantics as a label,
enclose event localization, and distinguish periodic steady response from
transient washout. Basin completeness, quasi-static/zero-rate limits,
stochastic switching, and structural stability remain beyond that finite
trajectory gate.

### T6. Uncertainty and identifiability precede biological interpretation

An uncertainty artifact binds a field request to a typed parameter ensemble or
bounded explicit coordinate population and records the sampled/propagated
population. Completeness of that explicit row population is not continuum
coverage of its enclosing interval box. It must keep three kinds
of uncertainty separate:

- parametric or experimental uncertainty;
- numerical solver error and invalidity; and
- structural/model uncertainty.

At minimum it reports valid replicate counts, quantiles or externally declared
certificate-bound feature bounds, gap probability, and the exact field features
being summarized. The current engine checks those bounds against enumerated
valid rows and binds the certificate but does not reprove it. A credible
interval must not bridge invalid samples by interpolation.

Identifiability first analyzes the local sensitivity or Fisher-information
matrix under a declared observation and noise model. Rank, singular spectrum,
condition number, and practical profile/posterior evidence are separate outputs.
An identifiable combination is not proof that every biochemical parameter is
individually identifiable, and a field feature that is robust in a prior sample
is not automatically experimentally identifiable.

The current local SVD/Fisher and first-order delta layers are therefore only the
first gate. Structural identifiability, profile likelihood or posterior geometry,
multimodality, nonlinear interval propagation, correlated experimental noise,
model discrepancy, and optimal experimental design remain separate future
artifacts. Real calibration also needs raw-data identity, preprocessing,
replicate structure, censoring/missingness, and held-out coverage rather than a
synthetic fixture alone.

P8e must derive or independently replay sensitivities from the bound engine
model; a caller-supplied sensitivity matrix is input evidence, not a verified
model derivative. It must also audit whether invalid solver rows are informative
missingness, because dropping them can bias ranks, intervals, and apparent
coverage.

## Engineering program

### E1. Jobs retain the existing six-state lifecycle

RO-field computation uses the existing states:

```text
queued -> running -> succeeded | failed
queued/running -> cancel_requested -> cancelled | succeeded | failed
```

There is no `paused` or `resumed` state. Resume creates a new job with immutable
lineage:

```text
resume_from = { parent_job_id, checkpoint_sha256 }.
```

Terminal records stay immutable. External calls remain outside `JOBS_LOCK`, and
cooperative cancellation is checked between work units and before publication.

### E2. Content identity is layered

The first data plane uses five distinct identities:

1. a canonical plan hash excluding job ID, URI, timestamps, and executor;
2. a deterministic work-unit identity and point order;
3. a chunk hash over canonical chunk bytes;
4. a checkpoint hash over the plan plus ordered committed work-unit/chunk pairs;
5. a dataset-manifest hash over the plan, ordered chunk hashes, and population
   counts.

Publishing a chunk is atomic. Reusing an existing content hash requires exact
byte equality. A checkpoint may reference only a prefix or an explicitly allowed
completed set from the same plan. Resume schedules only missing work units and
does not recompute committed points.

The initial local vertical uses an inline canonical NetworkIR, explicit point
sets or small Cartesian grids, and chunks of 4--64 points. Every point record is
either a finite valid result or an explicit gap record with a reason. A manifest
is published only after all referenced chunks validate.

### E3. Adaptive sparse sampling is deterministic and evidence-bounded

The first adaptive sampler uses
a nested Smolyak/Clenshaw--Curtis construction with a downward-closed multi-index
set. The plan records:

- chart, domain, output order, and fixed background;
- initial index set and deterministic frontier order;
- error/surplus indicator and norm;
- tie-breaking rule;
- point, level, time, and payload budgets; and
- stopping reason.

Invalid points have no numeric hierarchical surplus. They create an unresolved
region or a refinement-policy decision; they are never replaced by zero. A
stopping indicator is evidence about the tested adaptive policy, not a uniform
continuum error bound unless an independent theorem supplies one.

P6h must add that missing theorem only for a declared function class: mixed
smoothness and anisotropy assumptions, norm and domain, verified derivative or
surplus-tail bounds, and a composition of point-evaluation error with sparse
truncation error. PWA strata must be partitioned and singular/invalid regions
left unresolved. Without verified hypotheses, Smolyak output remains a finite
adaptive approximation even when its last frontier indicator is small.

The implemented v2 state machine separates `prepare`, ordered evaluation, exact
`commit`, and `finalize` into canonical plan/state/batch/result tokens. One
multi-index is one backend work unit; invalidity makes that entire index
unresolved while incomparable branches can continue. Portable tokens have a
plan-bound byte policy plus a versioned fixed-depth restore preflight, and only
validation against the plan plus replayed terminal state is authoritative
result evidence.

The Web integration is a disjoint `local_async` v2 path. It content-addresses
plan, batch, point chunk, state, checkpoint, terminal result, and dataset
manifest records; cancellation may leave an uncommitted orphan CAS object, but
resume can follow only the parent's linearized checkpoint and does not recompute
committed indices. Submission performs bounded lightweight admission, while the
resumed child and final reader perform one authoritative forward-chain replay;
shallow engine checks are used only for state constructed in the current process
or already covered by that replay. Its plan additionally binds
the module-load-frozen selected Project/Manifest, BindingAndCatalysis source
tree, and `webapp/src/**/*.jl` tree plus Julia/SciML package versions, explicit
homotopy/Tsit5 tolerances,
per-point solver-step and RHS-evaluation caps, strict Float64 closed-cell
membership without best-fit fallback, and a replay-work bound over canonical
bytes/scalars, state and scheduler work, transitions, point receipts, payload
scalars, interpolation work, and terminal objects. Cancellation is checked inside every
homotopy RHS evaluation, and the local Web admission cap is 512 sparse work
units. It has no shared object
store, remote lease, distributed worker, or cluster recovery claim.
The replay meter begins after separately bounded plan parsing, NetworkIR/model
reconstruction, and runtime-identity comparison; it is an artifact-chain replay
cap, not an end-to-end CPU or wall-clock budget.

Before this representation can back a shared campaign, P6x must replace the
cumulative state-per-transition layout with an append-only transition/Merkle
chain plus bounded periodic snapshots, or retain an explicitly small immutable
ceiling. The current cumulative state artifacts are safe under the local cap but
grow approximately quadratically in stored history. Terminal result, terminal
checkpoint, and manifest bytes are now reserved before first publication and
revalidated on read. P6x still owns a complete CAS/disk quota: plan, initial and
superseded checkpoints, and orphan objects are outside the current payload
counter and must not be mistaken for a complete job-artifact bound.

### E4. High-dimensional views are explicit slices

A server-generated 2D view declares exactly two free axes and one fixed value
for every other source coordinate. Sparse data requires three distinct
contracts:

- `reused_exact` is allowed only when every requested fixed coordinate and 2D
  node is present in the parent sample population;
- `reevaluated_child_plan` is a new 2D engine computation whose parent is
  lineage only; and
- `interpolated_view` records basis, support nodes, weights, parent terminal
  hash, and conservative gap-contamination rules, and is never labelled as an
  engine evaluation.

Sparse nodes generally do not form a complete Cartesian cross-section.
`projected` is therefore not a sufficient provenance label. Coordinate
projection or nonlinear transformation belongs to the chart contract, and a
global interpolation support that touches any invalid node remains unknown
under the first conservative policy. A live resample may reuse the existing 2D
v1 field contract only as a new evaluation with all lineage retained.

### E5. D=3 uses a Float64 face lattice with a dyadic-exact publication gate

The existing `ROCellComplex2D` and RPB2 remain unchanged. The first credible
higher-dimensional implementation introduces a separate face-lattice model but
opens construction only for `D=3`. It stores cells and faces of every dimension,
with parent/child incidence; CDD objects remain temporary construction state.

Constraints are normalized after mapping the declared box to the unit cube.
Numerical rank uses a low/high SVD threshold with an unresolved grey zone rather
than silently snapping thin geometry. Face dimension must agree between vertex
rank, supporting-hyperplane rank, and polyhedral dimension.

Three-dimensional internal facets are constructed by pairwise common refinement:

```text
Q_ij = P_i intersect P_j.
```

- `dim(Q_ij)=3` is a positive-volume overlap failure;
- `dim(Q_ij)=2` creates one atomic internal facet;
- lower dimensions contribute ridge/vertex closure evidence.

Cell/domain-plane intersections create domain facets. Facet edges then undergo a
second one-dimensional common refinement, so T-junctions become atomic ridges
instead of being lost by maximal-facet matching.

A publishable Float64 consistency certificate requires cell/facet closure,
ridge and vertex link checks, per-side domain coverage, no positive-volume cell
overlap, volume agreement, and global Euler consistency. In addition, the
publication gate interprets the admitted Float64 domain and halfspaces as their
bit-exact dyadic rationals and requires exact pair dimension/non-overlap,
opposing original-H support, support/domain plane coverage, and exact volume
equality to agree with the Float64 construction. Euler numbers alone do not
prove coverage. The evidence wording must state that this is a complete
enumerated Float64/asymptotic incidence construction with selected dyadic-exact
publication predicates, not an arbitrary-precision-input or arbitrary-real
geometry API, a full exact reconstruction of every ridge/vertex incidence, or
finite nonlinear evidence.

The first analytic end-to-end fixture is `A+B+C<->ABC` on `[-2,2]^3` with
`log10(Kd)=0`. It predicts four volume-16 cells, 18 facets, 22 ridges, nine
vertices, and global Euler value one. A separate three-cell T-junction fixture
proves common refinement rather than only the symmetric chemistry case.

`D=4` remains a separate bounded phase, not a loop-bound change. Its face population
can grow combinatorially, and a codimension-one intersection can itself contain
a nontrivial lower-dimensional complex. The next exact-incidence phase must:

1. freeze either Float64-bit-as-dyadic or canonical-rational input semantics and
   a versioned dimension-parametric face/hyperplane identity while retaining the
   current 2D and 3D public identities;
2. recursively common-refine all admitted multiway support-set intersections
   and codimensions, rather than assuming that pairwise maximal facets determine
   all lower faces;
3. let a floating filter decide a publication-critical predicate only when an
   error bound proves rank/sign separation from zero; every unresolved predicate
   requires exact rational fallback;
4. validate the complete chain complex, including every incidence dimension,
   boundary/link homology, and `boundary^2 = 0`, rather than relying on Euler
   values alone;
5. preflight candidate intersections, face population, representation
   conversion, exact-arithmetic bit growth, and incidence-link work before
   materialization; and
6. pass independent rational-oracle fixtures in `D=4` (including a split
   tesseract, non-simple vertices, and nested T-junction analogues) before any
   persisted D4 identity or completeness wording is introduced.

Any exact-bit, candidate-intersection, face-count, or oracle-work overflow
returns bounded/unknown. It cannot publish completeness. Arbitrary `D>4`,
holed-domain periods, and non-manifold complexes remain later evidence classes.

Until those gates pass, high-rank sampled/sparse fields and explicit 2D slices
remain numerical views, not substitutes for an exact face lattice.

### E6. Recovery and distribution use immutable shards

Distributed execution first partitions independent network--field-plan pairs.
One adaptive field plan retains one sequential scheduler and one ordered commit
chain; its multi-indices are not an ordinary concurrent work-stealing queue. A
later point-parallel extension may split one logical multi-index into physical
point shards only if one deterministic ordered reduce publishes the unchanged
logical transition identity.

Workers write content-addressed shard results and never mutate a shared SQLite
database. A shared CAS is a new protocol, not a path substitution. It requires:

- conditional create followed by byte-for-byte read-back verification and a
  tenant/owner access boundary;
- lease tokens with fencing epochs, heartbeat/expiry, cancellation generation,
  and durable coordinator restart reconciliation;
- at-least-once execution whose duplicate results must be byte-identical or be
  quarantined; and
- GC roots for live jobs, terminal manifests, resume lineage, and campaign
  locks, plus a grace period, two-phase deletion, and in-flight-upload safety.

Operational execution status is separate from scientific validity. A worker
receipt uses `completed_with_valid_field`,
`completed_with_numerical_gaps`, `operational_failed`, `cancelled`, or
`retry_exhausted`. Only the first two enter the scientific corpus; any other
status leaves the declared campaign population incomplete. Merge validates the
plan, work-unit identity, point order, chunk hash, counts, duplicate consistency,
and execution receipt before publishing a new manifest. One SQLite database per
shard is allowed; concurrent writers to one corpus database are not the recovery
protocol.

A completed execution receipt must record observed RHS evaluations, solver
steps, wall time, peak bytes, and retry count rather than only their ceilings.
Distributed runtime identity must additionally bind the container/image digest,
thread policy, BLAS/libm implementation, and CPU class. P6g3 should validate one
explicit executor adapter first; passing a local simulator is not evidence for
Slurm or AWS.

### E7. A full Atlas campaign has a separate authority boundary

Infrastructure can prepare a local finite campaign manifest, dry run, merge
validator, and small demonstration corpus. Running the complete campaign against
local high-cost compute, Slurm, AWS, or another external resource requires a
separate explicit authorization.

That authorization is a content-bound record, not a prose string. It names the
`campaign_sha256`, exactly one executor, CPU/wall-clock/storage ceilings, target
CAS, stop rules, validity window, approver and approval time, revocation state,
and the preceding benchmark receipt. The sequence is:

1. P9a freezes the population and plan and runs at most eight benchmark units;
2. P9a2 issues or refuses full-run authority from that measured receipt;
3. P9b executes bounded canary and tranche stages with go/no-go gates;
4. P9c cold-loads every addressed artifact and performs an independent
   population recount; and
5. P9d uses one writer to transactionally build, verify, and atomically activate
   an Atlas snapshot.

The campaign manifest must freeze:

- the finite source-network population or generation grammar and bounds;
- canonical NetworkIR and plan versions;
- chart/domain/background/output policies;
- solver and invalidity policies;
- signature and query versions;
- work-unit partition and expected population counts; and
- code revision, environment locks, and merge command.

Only the completed manifest population can support prevalence or absence claims.
A zero query result means no match in that validated corpus; it is not an
impossibility theorem.

Cold audit reads the actual plan, chunk, checkpoint, terminal, and field bytes;
validates type, runtime lineage, and terminal replay; and records separate
denominators for networks, plans, operationally completed plans, valid fields,
partial/gap fields, samples, and signature records. Operationally missing work
makes the corpus incomplete. Numerical gaps may be counted but cannot be silently
removed from a prevalence denominator. A separately implemented process performs
the recount. Large inventories must be streamed into immutable Merkle-addressed
segments rather than loaded as one JSON population. One single-writer
transaction then builds a temporary Atlas before atomic snapshot activation.

#### Prepared authorization sizes

The current enumerator was run locally in preparation-only mode on 2026-07-17;
it did not construct fields, write an Atlas, or launch an external worker. Under
the existing `complex_growth_binding` grammar with base-species counts 2--4,
one to five reactions, and maximum template/support order five, the current
source enumerates 596 `d=2`, 2,177 `d=3`, and 2,467 `d=4` networks: 5,240 in
total. These counts describe enumerated networks, not successful fields or
slice records. Reproduce the preparation report with
`julia --project=webapp webapp/scripts/report_ro_field_campaign_population.jl`.

For authorization sizing only, one candidate policy takes every unordered
input-axis subset of size two through `d`, treats each reaction product as an
output, and splits outputs into groups of at most four. That produces the
following declared plan populations:

| Candidate scope | Networks | Field-plan groups | Deterministic dense-point population |
|---|---:|---:|---:|
| `d=2`, all 2D axes | 596 | 1,046 | 302,294 at `17 x 17` |
| `d in {2,3}`, all 2D axes | 2,773 | 13,274 | 3,836,186 at `17 x 17` |
| `d in {2,3,4}`, all 2D axes | 5,240 | 41,666 | 12,041,474 at `17 x 17` |
| `d in {2,3,4}`, every 2D--`d` axis subset | 5,240 | 69,402 | unknown until 3D/4D sparse policies and hard stops are frozen |

The plan-group counts include output splitting, so they are not interchangeable
with network counts or axis-subset counts. The last row cannot be converted to
one exact point count before its deterministic sparse initial set, refinement
budget, invalid-region policy, and stopping reasons are frozen. None of these
candidate scopes is authorized merely because its count is now known.

Preparation freezes and hashes the canonical NetworkIR population, every field
plan, the work-unit partition, environment locks, and all scientific policies.
The at-most-eight-unit benchmark then measures point cost, artifact size, gap
rate, observed RHS evaluations, solver steps, wall time, peak bytes, retries,
and cancellation/restart behavior
without extrapolating a prevalence claim. Only that receipt can support the P9a2
authorization decision. An authorized run then proceeds through immutable
canary/tranche work, cold content audit, independent recount, and single-writer
snapshot publication; no stage can be reordered around the authority gate.

The current P9 implementation reaches preparation, local-demo execution,
metadata merge, and independent metadata recount only. Steps 3--5 for a complete
scope require additional executor, shared-storage, artifact-loading, and
external-observation evidence even after authority is granted.

## Delivery sequence and exit gates

| Phase | Deliverable | Exit gate |
|---|---|---|
| P5 | affine charts, sampled integrability/curvature, explicit synergy policy, regular-extension certificate, joint Clarke generators | analytic quadratic and `max(u,v)` MIMO fixtures; gaps stay unknown; source field identity unchanged |
| P5s | conditional singular-branch selection distinct from the regular-limit hull | caller-declared receipt consistency; branch-bound residual/stability/reachability metadata; constructor/hash/budget/cancellation adversaries; no enumerator or analysis replay claim |
| P6a | deterministic plan, content-addressed point chunks, checkpoints, manifests, new-job resume | byte-tamper, order, plan-mismatch, cancellation, and committed-point non-recompute tests |
| P6b | `compute_ro_field` job using the six existing states | local job result/manifest protocol, cancellation race, terminal immutability, restart behavior |
| P6c | adaptive sparse point set | deterministic frontier, invalid-descendant policy, portable identity, budget and cancellation tests |
| P6c2 | canonical sparse pure transitions | bounded canonical plan/state/batch/result tokens; deterministic prepare/commit/finalize; invalid cones; full replay; semantic tamper, budget, cancellation, and Julia 1.10/1.12 checks |
| P6d | exact-index/no-interpolation Cartesian 2D slices from chunked higher-rank data | non-symmetric source-index mapping, full provenance-chain validation, no interpolation, pre-loader budgets |
| P6e | local adaptive sparse Job | v1/v2 identity isolation; one-index work units; all-layer CAS tamper; orphan non-commit; verified child resume; outer manifest; terminal immutability; AWS fail-closed |
| P6x | compact adaptive log and complete CAS/disk quota | append-only/Merkle transition history with bounded snapshots; plan/initial/superseded/orphan accounting and GC roots; old local artifacts remain fail-closed |
| P7 | Float64 face-lattice model with strict `D=3` constructor and dyadic-exact publication gate | analytic chemistry fixture, T-junction, tiny gap/overlap, scaled-support, Float/exact mismatch, resource/cancel, and axis-permutation tests |
| P7b | exact `D=3` regular-extension integrability | three-point facet basis, exact tangential/offset/cotree-cycle checks, supplied-potential separation, ULP/cycle/ambiguity/tamper/resource/cancel negatives |
| P8a | uncertainty and identifiability artifacts | synthetic recoverable/non-identifiable models, coverage/calibration checks, numerical gaps separated |
| P8b | conditional equilibrium-branch/protocol artifacts | bistable conditional-loop fixture, arbitrary-switch and monostable negatives, all true-dynamic flags false, resource and cancellation tests |
| P8c | finite model-backed numerical trajectory | runtime/solver identity, two-solver agreement, strict increasing-to-decreasing lineage, predecessor replay, solver-policy separation, and all strong dynamic flags false |
| P9 | campaign manifest, immutable shard-result format, at-most-eight-work-unit local-demo execution, deterministic metadata merge/corpus lock | complete supplied result-metadata population; artifact-content audit and any complete campaign require separate implementation and authorization |
| P5n | declarative quadratic nonlinear input-chart foundation; global/uncertain chart work remains | pointwise immersion/fold/rank/conditioning admission, complete first-order and scalar Hessian chain rules, required first-order source-axis/unit/point declarations, chart/output/point-bound scalar derivative receipts, analytic sign-reversal fixture, forged-result rejection, cumulative/overflow-safe limits, seal/replay/resource/cancellation negatives; caller labels are not independently authenticated, and there is no global injectivity, overlap transition, or calibration-uncertainty claim |
| P5o | finite declarative C2 affine/quadratic output charts; general non-polynomial maps remain | typed `y=phi(z)` with order, unit labels, references, closed domain, first-order pullback, both second-order Hessian terms, identity/tamper/resource/cancellation fixtures; no dimensional algebra, unit conversion, ratio, or log-sum claim |
| P5r0/P5r0.1 | exact-dyadic polynomial regular equilibrium sheets | implemented declarative square polynomial owner; exact full-tube `x=p(u)+delta` substitution/cancellation; correlated `[F_x]` and `[F_u+F_x*S]`; positive tubes; strict Krawczyk inclusion; exact full-rank `C`; `beta < 1`; implicit-derivative enclosure; replayed overlap bridge; outside roots remain unknown |
| P5r1 | native log/transcendental regular equilibrium sheets | add a direct pinned interval dependency and validated native-residual enclosure; no callback evidence and no implementation yet |
| P6f1 | exact sparse research slices | strict wire schema; complete-node presence proof; parent terminal lineage; gap-preserving browser/export parity |
| P6f2 | re-evaluated and interpolated views | child-plan identity separated from interpolation basis/support/weights and conservative gap contamination |
| P6h | mixed-regularity sparse continuum bounds | declared anisotropic mixed-smoothness class and norm; verified evaluation plus truncation/tail error; partitioned PWA strata; unresolved singular regions |
| P6g1 | shared object CAS | conditional create/read-back, tenant boundary, immutable duplicate/quarantine, and rooted two-phase GC |
| P6g2 | coordinator recovery | lease/fencing/heartbeat/expiry, cancellation generation, durable restart reconciliation, and fault-injection tests |
| P6g3 | campaign-level distributed executor | distribute only independent field plans first; one named real executor adapter; deterministic logical-index reduction; full runtime/observed-work receipts; no shared SQLite writer |
| P7c1/P7c2 | D3 query and chemistry extraction | complete incident-matrix Clarke query plus chemistry-to-explicit-affine extraction with source replay |
| P7d0/P7d1/P7d2 | bounded D4 topology and integrability | canonical face predicates; exact D4 chain complex with independent rational oracle; then D4 regular-extension certificate |
| P7e | holed-domain periods | explicit non-contractible domain representation, cellular cohomology/period generators, and exact obstruction fixtures |
| P7f | finite-equilibrium to PWA/asymptotic bridge | same-chemistry `epsilon` family; compact-away-from-facet output/Jacobian error and rate; boundary-layer and exceptional-set accounting |
| P8s0 | polynomial branch witness field | implemented explicit `dx/dt` binding; cumulative replay of supplied P5r0 patches/bridges; evidence-relative components; exact stable/unstable/unknown policy; all ordered state-log-response columns; pairwise-separated at-least-N stable-root witnesses; all completeness/global/event/hysteresis flags false |
| P8s1a | complete regular-root census in one affine moving domain | implemented exact-dyadic remainder grid and cumulative source replay; every cell is either component-wise residual-excluded or exactly one full-box common-slope P5r0.1 tube; complete in-domain root count and regular sheets; complete empty in-domain fold set; outside roots, stability completeness, fold-containing domains, Hopf, and global continuation remain false |
| P8s1b0 | isolated polynomial fold-event census for exactly one control | implemented exhaustive exact-dyadic `H=(F,det(F_x))` cover; strict augmented Krawczyk roots; corank-one/transversality/quadratic nondegeneracy theorem; separated event enclosures; replay, adversarial scalar/two-state, resource/cancel gates; no adjacent-sheet claim from this artifact alone |
| P8s1b1 | local adjacent-sheet incidence at a certified fold | implemented declarative state/control chart; central event-containing P5r0.1 patch; complete lower/upper half-patch bridges; strict original-regular-patch tube containment; full-census corridors excluding intervening folds; original-control root-count orientation, remote components, stability, and global graph remain false |
| P8s1c0 | complete simple polynomial spectral-event census | implemented explicit `dx/dt` binding; signed even/odd characteristic-polynomial decomposition; exact state/control/frequency-squared cover from zero to beyond a uniform spectral bound; strict augmented Krawczyk roots; nonzero frequency, simple pair, transverse crossing, event-population completeness, and same-equilibrium multi-pair exclusion; no first-Lyapunov or periodic-orbit claim |
| P8s1c1 | nonlinear Hopf and criticality certificate | implemented complete parent replay; canonical full bordered right/Hermitian-adjoint systems; exact-rational frequency enclosure; separately validated zero/second-harmonic resolvents; raw second/third derivatives; unit-`q` Kuznetsov first-Lyapunov interval excluding zero for every event; center-manifold super/subcritical classification; no original-control side or periodic-orbit incidence |
| P8s1c2a | theorem-level local periodic-orbit germ | implemented complete c0/c1 replay; exact augmented-preconditioner determinant orientation; strict original-control side from crossing and `l1` signs; classical-Hopf germ/event incidence and center-manifold radial attraction/repulsion for sufficiently small nonzero but unquantified amplitude; no explicit orbit, amplitude radius, Floquet, full-state, or orbit-population claim |
| pre-P8s1c2b | exact finite-support Fourier residual identity | implemented conjugate-symmetric full-complex series, exact source-generated Laurent residual, Galerkin-head/omitted-tail receipts, source-bound replay, and bounded/cancellable work; certifies at most one submitted exact polynomial-ODE periodic parameterization and is not an infinite-tail/radii, branch, incidence, uniqueness, stability, or population certificate |
| P8s1c2b0 | one amplitude-pinned validated local periodic-orbit segment | one completely replayed c2a event; symbolic divided-difference equation valid at `a=0`; fixed first-harmonic phase/amplitude anchor; weighted Fourier sequence space, analytic tail inverse, uniform source-derived radii-polynomial proof on `a in [0,a_plus]`; constructive event incidence and local uniqueness inside one declared tube only |
| P8s1c2b1 | complete named Hopf-event lift to anchored orbit segments | exactly one canonical c2b0 child per completely replayed c2a parent event; source-bound attribution, cumulative resources, adversarial same-frequency/same-`l1` events; complete parent-event lift only, never periodic-orbit population completeness |
| P8s1c2b2 | longer local periodic-orbit continuation | general pseudo-arclength segments away from onset; uniformly nondegenerate continuation functional; common-space head-plus-tail bridge proofs; quantitative no-gap coverage and canonical branch identity without global continuation |
| P8s1c2c | Floquet and full-state orbit stability | validated periodic variational equation; unique trivial phase multiplier; strict enclosure of all nontrivial multipliers/exponents; stable/unstable/unknown semantics for each certified orbit without orbit-population completeness |
| P8s2 | native-residual and observable branch fields | P5r1-backed native log/transcendental enclosures plus admissible chart/observable composition; same explicit witness-versus-complete population and event semantics |
| P8d | vector protocols and branch reachability | `u(t) in R^D`, dimensional coefficient checks, enclosed events, periodic-steady/washout separation, multi-rate/zero-rate families, branch identity, and finite-witness versus global-theorem language |
| P8e | structural/practical identifiability and calibration | engine-derived/replayed sensitivities, structural oracle, profile/posterior geometry, experimental design, informative-missingness audit, raw-data/replicate identity, and held-out calibration |
| P9a/P9a2 | frozen population, benchmark, and authority decision | measured at-most-eight-unit receipt followed by a content-bound approval/refusal with executor and hard resource/stop limits |
| P9b | staged authorized execution | canary/tranche go/no-go gates; typed operational receipts distinct from scientific gaps; immutable CAS results |
| P9c | cold content audit and independent QC | stream/Merkle-inventory every addressed artifact, reload/replay contents, and independently recount every named denominator |
| P9d | Atlas snapshot publication | single-writer temporary build, transactional validation, and atomic activation only after P9c passes |

The dependency order is explicit:

- P5n and P5o now provide bounded quadratic input- and output-coordinate
  foundations. P5r0 now provides validated finite polynomial regular sheets
  inside declared tubes. P5r1 plus future global chart-overlap/injectivity,
  calibration-uncertainty, and general-observable work are still required for
  native-residual coverage, quantitative curvature, branch-resolved dynamics,
  and calibrated biological interpretation;
- `P6e` forks to `P6f1 -> P6f2` for research views and to
  `P6x -> P6g1 -> P6g2 -> P6g3` for compact storage and distributed recovery;
  `P5r0 -> P6h` is the first polynomial quantitative sparse-error path, while
  P5r1 is additionally required for native residuals and P6f views remain
  exploratory without P6h; `P6f2 + P6g3 -> P9a -> P9a2 -> P9b -> P9c -> P9d`
  then places measured
  authority before execution and content audit before Atlas activation;
- `P7b` independently enables `P7c1`, `P7c2`, and `P7d0`; only
  `P7d0 -> P7d1 -> P7d2` is the D4 exact-incidence/integrability chain, while
  `P7d0` plus an explicit holed-domain representation enables `P7e`, and
  `P7c2 + P5r0 -> P7f` is the first polynomial finite-to-asymptotic bridge,
  with P5r1 additionally required for native residuals; and
- `P5r0 -> P8s0 -> P8s1a` moves from supplied polynomial branch witnesses to a
  complete regular-root population inside one declared affine moving domain;
  `P5r0.1 -> P8s1b0 -> P8s1b1` adds isolated simple-fold events and local
  adjacent-sheet incidence for exactly one control. `P8s0 -> P8s1c0` now adds
  complete simple spectral-event domains for one polynomial dynamics binding;
  `P8s1c0 -> P8s1c1` now supplies nonlinear Hopf nondegeneracy, and
  `P8s1c1 -> P8s1c2a` now supplies theorem-level germ incidence and
  original-control orientation. `P8s1c2a -> P8s1c2b0` is the first explicit
  validated local orbit segment; `P8s1c2b0 -> P8s1c2b1` completes the named
  parent-event lift, and `P8s1c2b1 -> P8s1c2b2` adds general local continuation
  and strict segment bridges. `P8s1c2b2 -> P8s1c2c` is required for
  Floquet/full-state stability, although consumers that need only a validated
  orbit may stop at c2b.
  Multi-control fold/Hopf sheets
  require their own extension before the broader P8s1 event contract is
  complete. `P5r1 + P8s1 -> P8s2` is required before
  native engine residuals receive the same evidence. `P8s0 + P8c -> P8d` can
  support finite-witness vector protocols and branch reachability, while a
  complete-population P8d claim additionally requires the relevant P8s1 stage
  (and P8s2 for native residuals). `P8d -> P8e` remains required
  before branch-resolved experimental or biological claims are admitted.

Each phase first runs its narrow owner contract, then the complete engine or web
owner suite, `git diff --check`, and the repository verification gate. A phase is
not complete because its schema exists or its implementation appears plausible.

## Current working-tree evidence

P5 currently implements and tests:

- a full-column-rank affine input chart and matrix/tensor pullback;
- finite-grid circulation, mixed-partial, and output-edge consistency checks;
- unsymmetrized RO derivatives, symmetric Hessian projection, eigenvalues,
  independent output cross-curvature, and an explicit synergy policy;
- a complete declared Float64/PWA 2D regular-extension consistency certificate;
  and
- classical or joint-matrix Clarke generators with coupled fixed-direction
  images (not tangent-cone Bouligand derivatives).

The backend emits a separately hashed
`bne-ro-field-differential-analysis/v1.0.0` artifact from a complete inline
sampled field through the v1-only
`POST /api/v1/ro_field/differential` endpoint. This is local working-tree
evidence, not a remote deployment result.

P5n now adds `bne-ro-nonlinear-input-chart/v1.0.0`, a content-bound declarative
quadratic `theta(u)` with ordered names/unit labels, reference values, a closed
finite domain, local Jacobian and component Hessians, hard limits, cancellation,
and replay. Construction checks the reference Jacobian; each evaluation checks
the actual local Jacobian and fails on a fold/non-immersion, rank grey zone, or
conditioning grey zone. Its first-order matrix/tensor pullbacks use that local
Jacobian, and its scalar-output Hessian retains both the affine sandwich and the
chart-Hessian term. First-order arrays must declare source point, component
order, and unit labels, although the API does not independently authenticate
their upstream labelling. Scalar source derivatives additionally bind the chart
declaration hash and output name/unit. The declared scope remains pointwise
numerically admitted local immersion, and global injectivity is always false.

P5o separately adds `bne-ro-observable-chart/v1.0.0` for finite declarative C2
affine/quadratic `y=phi(z)` maps. It binds source/output component order, opaque
unit labels, references, a closed source domain, coefficients, limits, and
content identity. The composed jet retains both `J_phi H_z` and
`H_phi[R_z,R_z]`, and requires the supplied source jet to match the chart's
source order and units exactly. It neither performs dimensional algebra/unit
conversion nor implements general ratio, log-sum, normalized-occupancy, or
other non-polynomial maps.

P5r0.1 now adds `bne-ro-polynomial-equilibrium-system/v1.0.0`,
`bne-ro-exact-regular-sheet-patch/v1.1.0`, and
`bne-ro-exact-regular-sheet-bridge/v1.1.0`. The exact-dyadic implementation
binds the declarative polynomial source, performs correlation-preserving
full-tube substitution and differentiation, publishes strict Krawczyk and
`beta < 1` evidence, binds `[F_u+F_x*S]`, encloses the implicit derivative, and
replays overlap-tube containment before a child inherits a parent branch
identity. Focused contracts pass 106/106 on Julia 1.10.11 and 1.12.6, including
one- and two-input nonlinear translated branches. Its positive claim is
uniqueness only inside the declared tube; roots outside it and global branch
completeness remain unknown. P5r1 remains design-only for native
log/transcendental residuals.

P8s0 now adds `bne-ro-polynomial-dynamics-binding/v1.0.0` and
`bne-ro-branch-indexed-regular-field/v1.0.0`. It replays the complete supplied
P5r0 patch/bridge population under cumulative exact-work limits, constructs
evidence-relative bridge components, classifies patch stability through exact
row-Gershgorin/trace sufficient tests, and encloses every ordered state-log
response column. A common-box witness accepts only uniformly stable patches
from distinct components whose exact restricted state tubes are pairwise
strictly separated; its result is an at-least-N stable-root lower bound. The
focused contract passes 76/76 with bounds checking on Julia 1.10.11 and 1.12.6,
and both complete engine and standard Web owner suites pass after integration.
Stable-root completeness, folds outside the selected tubes, fold/Hopf boundary
enclosure, global continuation, and true hysteresis are forced false.

P8s1a now adds `bne-ro-complete-regular-root-census/v1.0.0`. It canonicalizes
and cumulatively replays all supplied P5r0.1 patches, requires their complete
control boxes and affine slopes to match the census coordinate, maps every full
patch tube exactly onto one exact-dyadic tensor cell, composes
`F(p(u)+delta,u)` once, and exhaustively evaluates every cell. A non-patch cell
must exclude zero in one residual component; any unresolved cell rejects the
whole census. A passed result therefore proves the exact in-domain regular-root
count for every control, complete sheet continuation across that one box, and a
complete empty in-domain fold set. Focused contracts pass 90/90 with bounds
checking on Julia 1.10.11 and 1.12.6, including three-sheet, zero-root,
nonlinear two-input, and two-state/two-input tensor-cover fixtures plus replay,
forgery, budget, and cancellation negatives. Both complete engine and standard
Web owner suites pass after integration. Roots outside the declared moving
domain, stable-root population completeness, fold-containing domains, Hopf
events, global continuation, native residuals, and true hysteresis remain
unknown.

P8s1b0 now adds `bne-ro-simple-fold-event-census/v1.0.0`. It constructs the
exact polynomial `H=(F,det(F_x))`, exhausts one declared positive augmented
tensor domain, and accepts each cell only through component exclusion or a
strict augmented Krawczyk root. A passed artifact therefore reports every
isolated simple fold inside that domain. The focused contract passes 78/78 with
bounds checking on Julia 1.10.11 and 1.12.6, including scalar, two-state,
empty-event, cusp rejection, exact-translation, replay/forgery, budget, and
cancellation cases.

P8s1b1 now adds `bne-ro-simple-fold-branch-incidence/v1.0.0`. It builds one
declarative fold chart, proves an event-containing central sheet and two
bridged local half-sheets, embeds the half-tubes in original-coordinate regular
patches, and uses the complete P8s1b0 census to exclude every other fold from
the covered corridors. The focused contract passes 70/70 with bounds checking
on Julia 1.10.11 and 1.12.6, including coordinate permutation, source/forgery,
resource/cancellation, and a three-fold intervening-event counterexample. The
complete engine and standard Web owner suites pass after integration. The
original-control two-root side, stability, remote component identity, global
branch graph, multi-control fold sheets, Hopf events, native residuals, and
true hysteresis remain unknown.

P8s1c0 now adds `bne-ro-simple-spectral-hopf-event-census/v1.0.0`. It requires
one explicit polynomial dynamics binding, builds the signed even/odd
characteristic-polynomial decomposition, and exhausts the exact-dyadic
`(x,lambda,z)` cover from `z=0` to beyond the exact uniform spectral bound.
Every event is a strict augmented Krawczyk root with `det(F_x)` excluded and
`z>0`; augmented-Jacobian nonsingularity proves a simple transverse spectral
crossing and rejects a repeated same-frequency pair, while complete pairwise
state/control projection separation excludes distinct-frequency pairs at the
same equilibrium. The focused contract
passes 104/104 with bounds checking on Julia 1.10.11 and 1.12.6. Its direct
read-only audit found no remaining P0/P1/P2 issue after parent-authority,
cumulative-operation, determinant-leaf, projection-pair, and cancellation
hardening. This remains a spectral-event census only. P8s1c1 now adds the
complete replayed first-Lyapunov lift described above. Its focused
contract passes 102/102 with bounds checking on Julia 1.10.11 and 1.12.6, and
its final direct read-only audit found no remaining P0/P1/P2 after pre-replay
resource, local-record structure, nonnormal `omega=2`, two-event canonicalization,
and child-stage cancellation fixes. Both complete Julia 1.12.6 engine and
standard Web owner suites exit zero after pre-c2b exact Fourier integration;
the Web suite
retains its one pre-existing explicit `@test_broken`. P8s1c2a now
passes 155/155 with bounds checking on Julia 1.10.11 and 1.12.6; its final
direct review found no remaining P0/P1/P2 after augmented-order, empty-
population, determinant-work, dense-sign, source-replay forgery, and deep-
cancellation hardening. This proves only theorem-level germ incidence,
original-control side, and center-manifold radial behavior. The implemented
pre-c2b exact finite-support Fourier identity contract passes 172/172 with
bounds checking on Julia 1.10.11 and 1.12.6; independent mathematical and
authority/resource reviews found no remaining P0/P1/P2 after complete-support,
derived-metric, input/output-bandwidth, source-replay, resource-preflight, and
final-cancellation hardening. It is still not a c2b stage. P8s1c2b0 anchored
Fourier/radii segments, P8s1c2b1 complete parent-event lift, P8s1c2b2 general
local continuation, P8s1c2c Floquet stability, multi-control
Hopf sheets, native residuals, and P8s2 remain design-only.

P5s now adds a separately identified conditional singular selector. It binds a
declared bounded-enumerator identity and caller-declared candidate receipt to
strict residual, stability, and finite-protocol reachability metadata. The
engine checks counts/content/hashes but does not execute the enumerator or
recompute the referenced stability analysis or trace. A unique result is unique
only under that declared population and policy; incomplete, gapped, or multiple
candidates remain unknown or set-valued.

P6a--P6e now provide two deliberately separate local asynchronous paths. The v1
Cartesian path retains layered plan/work-unit/chunk/checkpoint/manifest identity,
child resume on the existing six-state Job lifecycle, and exact-index/no-
interpolation 2D slices from verified 3D/4D Cartesian datasets. The v2 adaptive
path adds canonical pure transitions and a content-addressed local Job in which
one sparse multi-index is one work unit and the final read performs one metered
authoritative forward replay of every transition and nested artifact. The
selected lock plus loaded engine and Web source trees are frozen into runtime
identity, and terminal result/checkpoint/
manifest bytes are reserved before publication. Both paths are process-local;
there is no remote
worker, shared object store, sparse slice/view contract, orphan-GC protocol, or
cluster recovery protocol.

P7 now constructs a strict explicit-affine Float64 `D=3` face lattice with
cell/facet/ridge/vertex incidence and common refinement. Its publication gate
uses exact dyadic predicates for the admitted Float64 inputs as described in
E5; this is not automatic chemistry extraction, arbitrary-precision-input or
arbitrary-real geometry, a full exact ridge/vertex reconstruction, or exact
incidence for `D >= 4`. P7b adds exact regular-extension integrability over the
complete contractible box by checking atomic-facet affine bases, induced offset
jumps, dual cotree cycles, and supplied affine potentials. It does not cover
holed-domain cohomology, singular branches, or a `D=3` Clarke query.

P8a introduces declared-population local uncertainty/identifiability evidence.
P8b introduces finite supplied-protocol and conditional equilibrium-branch-loop
evidence. P8c separately integrates a declarative polynomial model under a
finite scalar ramp with two-solver agreement and strict predecessor replay.
The vector field owns one opaque model-time unit, and the protocol rate identity
is structurally bound as swept-control unit divided by that exact time unit; no
unit conversion is inferred from display strings.
P8c is model-backed trajectory evidence, but it has no validated error enclosure,
branch/switch identity, complete basin reachability, zero-rate limit, or true-
hysteresis certificate. All five listed strong dynamic flags remain false. None of
P8a--P8c supports an experimental, causal, or global claim.

P9 prepares deterministic finite campaign manifests, immutable shard results,
deterministic metadata merge/corpus locks, an independent identity-map recount,
and an enumeration-only authorization-size report. Only the at-most-eight-work-unit
local demo is executable. The complete Atlas campaign has not been authorized
or run, and the current QC deliberately does not recompute addressed field
contents or observe external execution.

## Rejected shortcuts

- Treating correlated inputs as independent axes without a chart.
- Calling a finite-grid curl check a continuum integrability proof.
- Symmetrizing mixed derivatives without retaining the mismatch.
- Calling any positive cross derivative causal biological synergy.
- Selecting the first singular label or combining MIMO output rows independently.
- Calling path-dependent regime traversal or a self-consistent equilibrium-root
  sequence hysteresis without model-backed dynamic reachability.
- Calling a P8s0 separated branch witness a complete multistability map, or
  treating an omitted branch or unenclosed bifurcation as absent.
- Adding `paused` or `resumed` to the established job state machine.
- Resuming by chunk index without a plan hash and content identity.
- Extending 2D edge matching directly to 3D maximal facets.
- Letting workers concurrently mutate one Atlas SQLite database.
- Running or claiming a complete external campaign without separate authority
  and a frozen finite population manifest.
