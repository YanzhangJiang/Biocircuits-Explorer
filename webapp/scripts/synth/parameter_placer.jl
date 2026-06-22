#!/usr/bin/env julia
# =============================================================================
# parameter_placer.jl  --  Constructive "ParameterPlacer" for the ROP engine.
#
# WORKING, VERIFIED prototype. ADDITIVE ONLY: this is the only new file; it
# does not modify, git-add, or commit anything else.
#
# GOAL: given a reaction network and a desired *reaction order* (local log-log
# slope of an output species w.r.t. an input total), CONSTRUCTIVELY produce a
# concrete parameter point (binding constants kd + conserved totals q) that
# realizes that slope. The solve is purely geometric -- it reads the regime
# polytope in log-(q,K) space and asks the engine for one interior point of the
# right cell. There is NO scanning / random sampling in the solve. Sampling is
# used ONLY as a final forward verification of the solved point.
#
# Run:
#   BNC_HEADLESS=1 julia --project=<...>/Bnc_julia <...>/parameter_placer.jl
#
# (BNC_HEADLESS=1 is optional; it just skips loading the Makie visualisation
#  layer of the engine, which the placer never needs.)
# =============================================================================

import Pkg
# Activate the engine project so `using BindingAndCatalysis` resolves the
# pinned Project+Manifest. The caller passes --project, but activate defensively
# in case the script is `include`d from another environment.
const _ENGINE_PROJECT = "<redacted-home>/git/Biocircuits-Explorer/Bnc_julia"
if Base.active_project() === nothing ||
   !occursin("Bnc_julia", Base.active_project())
    try
        Pkg.activate(_ENGINE_PROJECT; io = devnull)
    catch
    end
end

using BindingAndCatalysis
# Polyhedra is a dependency of the engine project (used for the Chebyshev-center
# computation in the physical-Kd-bounded placement). Import the module name into
# this script's scope; this adds no new dependency.
import Polyhedra

# The human-form reaction parser ("A + A <-> AA" -> Bnc).
include("<redacted-home>/git/Biocircuits-Explorer/webapp/src/reaction_parser.jl")
using .ReactionParser: build_model

# -----------------------------------------------------------------------------
# qK layout reminder (from reaction_parser.jl / locate_sym_qK):
#   The engine works in log10-(q,K) space, a vector of length n = d + r:
#       qK[1 : d]       = log10 of the conserved totals  q_sym  (e.g. tA, tB)
#       qK[d+1 : d+r]   = log10 of the binding constants K_sym  (Kd1 .. Kdr)
#   `locate_sym_qK(model, sym)` indexes into [q_sym; K_sym] with that convention.
# -----------------------------------------------------------------------------

"""
    _split_log_qK(model, logqK) -> (kd::Vector{Float64}, totals::Dict{Symbol,Float64})

Split an engine log10-(q,K) vector into linear-space binding constants `kd`
(ordered Kd1..Kdr) and a `Symbol => value` dict of conserved totals.
"""
function _split_log_qK(model, logqK::AbstractVector{<:Real})
    d = model.d
    r = model.r
    @assert length(logqK) == d + r "log-qK vector length $(length(logqK)) != d+r = $(d+r)"
    totals_log = logqK[1:d]
    kd_log     = logqK[d+1:d+r]
    kd = exp10.(Float64.(kd_log))
    qsyms = Symbol.(string.(q_sym(model)))   # q_sym(model) is Vector{Num}; names like :tA
    totals = Dict{Symbol,Float64}()
    for i in 1:d
        totals[qsyms[i]] = exp10(Float64(totals_log[i]))
    end
    return kd, totals
end

"""
    _measure_local_RO(model, logqK, input_idx, output_idx; h=0.02) -> Float64

FORWARD VERIFICATION (the only place sampling is allowed). Given a concrete
operating point `logqK` (full log10-(q,K) vector), measure the *local* reaction
order  d log10(output) / d log10(input_total)  by a small symmetric
finite-difference sweep of the input total coordinate, solving the equilibrium
with the engine's `qK2x` at each perturbed point. `h` is the half-step in
log10-decades.
"""
function _measure_local_RO(model, logqK::AbstractVector{<:Real},
                           input_idx::Int, output_idx::Int; h::Real = 0.02)
    base = collect(Float64.(logqK))
    function logout(delta)
        p = copy(base)
        p[input_idx] += delta
        logx = qK2x(model, p; input_logspace = true, output_logspace = true)
        return logx[output_idx]   # already log10 of the output concentration
    end
    yp = logout(+h)
    ym = logout(-h)
    return (yp - ym) / (2h)
end

"""
    place_for_target_slope(rules, input_sym, output_sym, target_RO; tol=0.05,
                           verify_h=0.02)
        -> NamedTuple

Constructively place a parameter point realizing a target local reaction order
(log-log slope) of `output_sym` w.r.t. the total of `input_sym`.

Pipeline:
 1. build_model + find_all_vertices!.
 2. SISOPaths(input) + get_behavior_families(observe_x = output). For every
    feasible path, read its per-regime reaction-order profile (`exact_profile`,
    aligned 1:1 with `vertex_indices`). Find a regime (vertex `v`) whose RO for
    the output equals `target_RO` within `tol`.
 3. CONSTRUCTIVE solve (polytope-only, no sampling): take vertex `v`'s polytope
    in log-(q,K) space via `get_polyhedron(model, v)` and ask the engine for one
    interior point with `get_one_inner_point`. Split it into kd + totals.
 4. VERIFY (forward, sampling allowed): finite-difference the input total around
    the solved point through `qK2x` and assert the measured slope == target_RO.

Returns a NamedTuple:
  (kd, totals, vertex_idx, predicted_RO, dominance_ordering,
   log_qK, measured_RO, pass, input_idx, output_idx)

Throws if no regime matches the target (infeasible target for this topology).
"""
function place_for_target_slope(rules::Vector{String}, input_sym, output_sym,
                                target_RO::Real; tol::Real = 0.05,
                                verify_h::Real = 0.02,
                                kd_bounds::Union{Nothing,Tuple{<:Real,<:Real}} = nothing)
    # When kd_bounds is given, route to the physical-Kd-bounded solver
    # (defined below). Default (nothing) keeps the original deep-interior path.
    if kd_bounds !== nothing
        return _place_for_target_slope_bounded(rules, input_sym, output_sym, target_RO;
                                               tol = tol, verify_h = verify_h,
                                               kd_bounds = kd_bounds)
    end
    input_sym  = Symbol(input_sym)
    output_sym = Symbol(output_sym)

    # ---- 1. build + enumerate regimes -------------------------------------
    model, species, free_syms, prod_syms = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)

    input_idx  = locate_sym_qK(model, input_sym)            # index into [q;K]
    output_idx = locate_sym_x(model, output_sym)

    # ---- 2. behavior families: per-regime reaction orders -----------------
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso;
        observe_x        = output_sym,
        path_scope       = :feasible,   # only paths with non-empty polytopes
        deduplicate      = false,
        keep_singular    = true,
        keep_nonasymptotic = true,      # keep every regime so RO aligns with vtx
        compute_volume   = false,
    )

    # Scan feasible paths for a regime whose RO == target.
    chosen_v   = nothing
    chosen_ro  = nothing
    best_miss  = Inf
    best_v     = nothing
    best_ro    = nothing
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices
        ros  = pr.exact_profile
        length(vtxs) == length(ros) || continue   # alignment guard
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            miss = abs(ro - target_RO)
            if miss < best_miss
                best_miss = miss; best_v = v; best_ro = ro
            end
            if miss <= tol
                chosen_v = v; chosen_ro = ro
                break
            end
        end
        chosen_v === nothing || break
    end

    if chosen_v === nothing
        error("No regime realizes target_RO=$target_RO (tol=$tol) for output " *
              "$output_sym vs input $input_sym. Closest achievable RO is " *
              "$(best_ro) at vertex $(best_v) (|miss|=$(round(best_miss; digits=4))). " *
              "This target is infeasible for this topology.")
    end

    # ---- 3. CONSTRUCTIVE solve: one interior point of the regime polytope --
    # get_polyhedron(model, v) builds the vertex's cell in log10-(q,K) space
    # (dimension n = d+r). get_one_inner_point returns a single interior point.
    # This is pure polytope geometry -- NO scanning, NO random target-hitting.
    #
    # The cells are asymptotic CONES: the dominant-balance slope is exact only in
    # the deep interior and softens near the facets. We therefore pick a
    # DEEP-interior representative by walking further along the cone rays
    # (`extend`). This is NOT sampling-to-hit-the-target: we never search over
    # cells or read the slope to steer the choice; we deterministically take one
    # well-interior point of the *already-selected* cell, escalating `extend`
    # only to get off the boundary layer. The cell (and thus predicted_RO) is
    # fixed by the polytope geometry in step 2.
    poly  = get_polyhedron(model, chosen_v)
    logqK = collect(Float64.(get_one_inner_point(poly; rand_line = false,
                                                 rand_ray = false, extend = 12)))

    kd, totals = _split_log_qK(model, logqK)
    dominance  = string.(show_dominant_condition(model, chosen_v))

    # ---- 5. VERIFY (forward; sampling allowed here only) ------------------
    measured = _measure_local_RO(model, logqK, input_idx, output_idx; h = verify_h)
    pass = abs(measured - target_RO) <= max(tol, 5 * verify_h)  # FD has O(h^2) error

    return (; kd, totals, vertex_idx = chosen_v, predicted_RO = chosen_ro,
            dominance_ordering = dominance, log_qK = logqK,
            measured_RO = measured, pass, input_idx, output_idx,
            model, species)
end

# =============================================================================
# PHYSICAL-Kd-BOUNDED placement (additive).
#
# THE PROBLEM this solves: `place_for_target_slope` walks deep into the cone
# interior (`extend = 12`) to nail the asymptotic slope exactly. For some
# regimes the cone is an unbounded ray whose interior drives the binding
# constants to physically absurd magnitudes -- e.g. the homo-tetramer RO=2 cell
# returns Kd1 = 6.5e-21. A real biochemical placement must keep every Kd inside
# a physical window, e.g. Kd in [1e-3, 1e3] (log10 in [-3, 3]).
#
# THE FIX: intersect the *already-selected* regime cone with the box
#   lo_log10 <= log10(Kd_i) <= hi_log10   for every binding constant i,
# built as extra linear H-rows, and return the MOST-INTERIOR point of that
# intersection -- the Chebyshev center (largest inscribed ball) when it is
# finite, otherwise a moderate inner point of the bounded region. This is still
# pure polytope geometry: we never scan cells or read the slope to steer the
# choice. Sampling is used ONLY in the final forward verification.
#
# H-REP SIGN CONVENTION (matches `place_threshold` above): get_polyhedron(C,C0)
# builds hrep(-C, C0), i.e. rows mean  -C . x <= C0. So to encode an inequality
#   a . x <= rhs   we set the C-row to  -a  and the C0-entry to  rhs.
# A box bound on coordinate i:
#   z_i <= hi        -> a = +e_i, rhs = hi  -> C-row = -e_i, C0 =  hi
#   z_i >= lo  (-z_i <= -lo) -> a = -e_i, rhs = -lo -> C-row = +e_i, C0 = -lo
# =============================================================================

"""
    _kd_box_rows(model, lo_log10, hi_log10) -> (C::Matrix, C0::Vector)

Build the extra H-rows encoding `lo_log10 <= log10(Kd_i) <= hi_log10` for every
binding constant `i` (the qK coordinates `d+1 .. d+r`). Returns inequality rows
in the `get_polyhedron(C, C0)` convention (rows mean `-C . x <= C0`).
"""
function _kd_box_rows(model, lo_log10::Real, hi_log10::Real)
    d = model.d; r = model.r; n = model.n
    rows  = Vector{Vector{Float64}}()
    rhsv  = Float64[]
    for i in (d+1):(d+r)
        e = zeros(Float64, n); e[i] = 1.0
        push!(rows, -e); push!(rhsv, Float64(hi_log10))   # z_i <= hi
        push!(rows,  e); push!(rhsv, -Float64(lo_log10))  # z_i >= lo
    end
    C  = reduce(vcat, (reshape(c, 1, n) for c in rows))
    C0 = rhsv
    return C, C0
end

"""
    _most_interior_point(poly; extend=3) -> (logqK::Vector, radius_or_nan, mode)

Return the most-interior representative of a (bounded-or-coned) polyhedron.
Tries the Chebyshev center (largest inscribed Euclidean ball, `proper=false`
so it works with the CDD LP backend that has no dual support). If the region is
still unbounded in some direction (infinite Chebyshev radius) or the LP backend
balks, falls back to `get_one_inner_point` with a *moderate* `extend` (NOT the
deep `extend = 12`), keeping the point near the bounded core. `mode` is
`:chebyshev` or `:inner_point`; `radius_or_nan` is the inscribed radius for the
Chebyshev case and `NaN` otherwise.
"""
function _most_interior_point(poly; extend::Real = 3)
    try
        ctr, rad = Polyhedra.hchebyshevcenter(poly; verbose = 0, proper = false)
        if isfinite(rad)
            return collect(Float64.(ctr)), Float64(rad), :chebyshev
        end
    catch
        # LP backend (CDD) cannot solve this center; fall through.
    end
    ip = collect(Float64.(get_one_inner_point(poly; rand_line = false,
                                              rand_ray = false, extend = extend)))
    return ip, NaN, :inner_point
end

"""
    _binding_bound_report(model, logqK, lo, hi; atol=1e-6) -> String

Diagnostic: for a solved point, report which Kd bound(s) are *binding* (the
solution sits on the box face). Returns a human-readable string like
"Kd1 at lower bound (1e-3)" or "none (all Kd strictly interior)".
"""
function _binding_bound_report(model, logqK::AbstractVector{<:Real},
                               lo::Real, hi::Real; atol::Real = 1e-3)
    d = model.d; r = model.r
    hits = String[]
    for j in 1:r
        z = Float64(logqK[d + j])
        if abs(z - lo) <= atol
            push!(hits, "Kd$j at lower bound (1e$(Int(round(lo))))")
        elseif abs(z - hi) <= atol
            push!(hits, "Kd$j at upper bound (1e$(Int(round(hi))))")
        end
    end
    return isempty(hits) ? "none (all Kd strictly interior)" : join(hits, "; ")
end

"""
    place_for_target_slope(rules, input_sym, output_sym, target_RO;
                           tol=0.05, verify_h=0.02,
                           kd_bounds=nothing)
        -> NamedTuple

ADDITIVE OVERLOAD via the new `kd_bounds` keyword.

* `kd_bounds === nothing` (default): unchanged behaviour -- deep-interior point
  (`extend = 12`) of the target regime cone, exact asymptotic slope.

* `kd_bounds = (lo_log10, hi_log10)` (e.g. `(-3.0, 3.0)` = Kd in [1e-3, 1e3]):
  PHYSICAL-Kd-BOUNDED placement. The target regime cone is intersected with the
  box `lo_log10 <= log10(Kd_i) <= hi_log10` (extra linear rows, same H-rep sign
  convention as `place_threshold`), and the returned point is the MOST-INTERIOR
  point of that intersection (Chebyshev center when finite, else a moderate
  inner point) -- NOT the unbounded deep-interior point. The forward-measured
  slope is reported honestly: a bound can pin the point near a facet, softening
  the realized finite-Kd slope away from the exact integer. That softened value
  IS the realistic answer.

  If the target regime ∩ box is EMPTY, returns a NamedTuple with
  `feasible = false` describing which bound binds (the closest feasible regime /
  slope from the unconstrained scan), instead of throwing.

Extra fields when `kd_bounds` is given:
  feasible::Bool, kd_bounds, chebyshev_radius, solve_mode (:chebyshev /
  :inner_point), binding_bound::String, and on infeasibility
  closest_feasible_RO / closest_feasible_vertex.
"""
function _place_for_target_slope_bounded(rules::Vector{String}, input_sym, output_sym,
                                target_RO::Real; tol::Real = 0.05,
                                verify_h::Real = 0.02,
                                kd_bounds::Tuple{<:Real,<:Real})
    lo, hi = Float64(kd_bounds[1]), Float64(kd_bounds[2])
    @assert lo < hi "kd_bounds must be (lo_log10, hi_log10) with lo < hi"

    input_sym  = Symbol(input_sym)
    output_sym = Symbol(output_sym)

    # ---- 1. build + enumerate regimes (same as the base solver) -----------
    model, species, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx  = locate_sym_qK(model, input_sym)
    output_idx = locate_sym_x(model, output_sym)

    # ---- 2. behavior families: pick the regime whose RO == target ---------
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso;
        observe_x = output_sym, path_scope = :feasible, deduplicate = false,
        keep_singular = true, keep_nonasymptotic = true, compute_volume = false)

    chosen_v = nothing; chosen_ro = nothing
    best_miss = Inf; best_v = nothing; best_ro = nothing
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            miss = abs(ro - target_RO)
            if miss < best_miss
                best_miss = miss; best_v = v; best_ro = ro
            end
            if miss <= tol
                chosen_v = v; chosen_ro = ro; break
            end
        end
        chosen_v === nothing || break
    end

    if chosen_v === nothing
        error("No regime realizes target_RO=$target_RO (tol=$tol) for output " *
              "$output_sym vs input $input_sym. Closest achievable RO is " *
              "$(best_ro) at vertex $(best_v) (|miss|=$(round(best_miss; digits=4))). " *
              "This target is infeasible for this topology.")
    end

    # ---- 3. CONSTRUCTIVE bounded solve: regime cone INTERSECT Kd-box ------
    # Build the regime's own (C,C0,nullity); split into equalities/inequalities;
    # append the box inequality rows; build the intersected polyhedron and take
    # its most-interior point. Pure polytope geometry -- no sampling.
    vC, vC0, vnull = get_C_C0_nullity_qK(model, chosen_v)
    vC  = Matrix{Float64}(vC); vC0 = Vector{Float64}(vC0)
    if vnull > 0
        eqC = vC[1:vnull, :];     eqC0 = vC0[1:vnull]
        inC = vC[vnull+1:end, :]; inC0 = vC0[vnull+1:end]
    else
        eqC = zeros(0, model.n);  eqC0 = Float64[]
        inC = vC;                 inC0 = vC0
    end
    boxC, boxC0 = _kd_box_rows(model, lo, hi)

    fullC  = vcat(eqC, inC, boxC)
    fullC0 = vcat(eqC0, inC0, boxC0)
    poly   = get_polyhedron(fullC, fullC0, size(eqC, 1))

    if isempty(poly)
        # INFEASIBLE: report which bound binds + closest feasible regime/slope.
        # Diagnose the binding bound by re-solving WITHOUT the box and seeing
        # which Kd coordinate the cone forces outside [lo,hi].
        unb = collect(Float64.(get_one_inner_point(get_polyhedron(model, chosen_v);
                              rand_line = false, rand_ray = false, extend = 12)))
        d = model.d; r = model.r
        binds = String[]
        for j in 1:r
            z = unb[d + j]
            z < lo && push!(binds, "Kd$j wants 1e$(round(z; digits=2)) < lower 1e$(Int(round(lo)))")
            z > hi && push!(binds, "Kd$j wants 1e$(round(z; digits=2)) > upper 1e$(Int(round(hi)))")
        end
        bind_str = isempty(binds) ? "box too tight in a coupled direction" : join(binds, "; ")
        return (; feasible = false, kd_bounds = (lo, hi),
                vertex_idx = chosen_v, predicted_RO = chosen_ro,
                binding_bound = bind_str,
                closest_feasible_RO = best_ro, closest_feasible_vertex = best_v,
                input_idx, output_idx, model, species,
                reason = "Target regime ∩ Kd-box is empty. $bind_str. " *
                         "Closest feasible RO under these bounds is $(best_ro) " *
                         "at vertex $(best_v).")
    end

    logqK, cheb_r, mode = _most_interior_point(poly; extend = 3)
    kd, totals = _split_log_qK(model, logqK)
    dominance  = string.(show_dominant_condition(model, chosen_v))
    bind_str   = _binding_bound_report(model, logqK, lo, hi)

    # ---- 4. VERIFY forward (sampling allowed only here). The bounded point
    # may sit near a facet, so the measured slope is the HONEST realized
    # finite-Kd slope -- report it, do not force it to the integer.
    measured = _measure_local_RO(model, logqK, input_idx, output_idx; h = verify_h)
    pass = abs(measured - target_RO) <= max(tol, 5 * verify_h)

    return (; feasible = true, kd, totals, vertex_idx = chosen_v,
            predicted_RO = chosen_ro, dominance_ordering = dominance,
            log_qK = logqK, measured_RO = measured, pass,
            kd_bounds = (lo, hi), chebyshev_radius = cheb_r, solve_mode = mode,
            binding_bound = bind_str, input_idx, output_idx, model, species)
end

# =============================================================================
# STRETCH: threshold (breakpoint) placement.
# =============================================================================

"""
    place_threshold(rules, input_sym, output_sym, from_idx, to_idx,
                    target_input_value; tol_dec=0.15, verify_h=0.02)
        -> NamedTuple

Place a parameter point whose `from_idx -> to_idx` regime transition (the
breakpoint of the input sweep, e.g. an AB-saturation knee) sits at
input total = `target_input_value`.

Construction (polytope-only):
  get_interface_qK(model, from, to) -> (dir, c): the regime boundary hyperplane
  in log10-(q,K) space is  dir . logqK + c = 0.  Pin the input coordinate to
  log10(target_input_value); that turns the interface into one extra linear
  equality on the remaining (background) log-coordinates. We feed that equality
  to the engine as an extra constraint (constraint_C / constraint_C0 with
  nullity=1 == equality) intersected with the from-regime, then take one
  interior point of the intersection. The result is a concrete (q,K) whose
  from->to knee is exactly at target_input_value.

Verified by a forward scan of the input total (log-decade tolerance `tol_dec`).
"""
function place_threshold(rules::Vector{String}, input_sym, output_sym,
                         from_idx::Int, to_idx::Int, target_input_value::Real;
                         tol_dec::Real = 0.15, verify_h::Real = 0.02)
    input_sym  = Symbol(input_sym)
    output_sym = Symbol(output_sym)

    model, species, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    # The directed qK-space interface (edge.change_dir_qK) is only populated once
    # the change/SISO graph along the input axis has been built. Build it so
    # get_interface_qK can return the oriented hyperplane.
    SISOPaths(model, input_sym)
    input_idx  = locate_sym_qK(model, input_sym)
    output_idx = locate_sym_x(model, output_sym)
    n = model.n

    # interface hyperplane  dir . logqK + c = 0
    # (get_interface_qK is not exported; reach it through the module.) Fall back
    # to the polytope-intersection form if the oriented edge is unavailable.
    dir, c = try
        BindingAndCatalysis.get_interface_qK(model, from_idx, to_idx)
    catch
        BindingAndCatalysis.get_interface_direct(model, from_idx, to_idx)
    end
    dirv = collect(Float64.(Vector(dir)))
    @assert length(dirv) == n "interface dir length $(length(dirv)) != n=$n"

    logtgt = log10(Float64(target_input_value))

    # Pin the input coordinate AND sit on the interface, as two EQUALITY rows.
    #   (a) e_input . logqK = logtgt   (pin the input total to target)
    #   (b) dir     . logqK = -c       (sit on the from->to interface hyperplane)
    #
    # SIGN CONVENTION: get_polyhedron(C, C0, nullity) builds hrep(-C, C0), i.e.
    # rows mean  -C . x <= C0  with the leading `nullity` rows taken as EQUALITY
    # (-C . x == C0). So to encode an equality  a . x == rhs  we set the row of C
    # to `a` and the entry of C0 to `-rhs`:  -a . x == -rhs  <=>  a . x == rhs.
    #   row (a): a = e_input, rhs = logtgt  ->  C0 = -logtgt
    #   row (b): a = dir,     rhs = -c      ->  C0 = +c
    e_input = zeros(Float64, n); e_input[input_idx] = 1.0
    C  = vcat(reshape(e_input, 1, n), reshape(dirv, 1, n))
    C0 = Float64[-logtgt, c]
    nullity = 2

    # Constructive: intersect the from-regime polytope with these equalities and
    # take an interior point. We build the from-regime's (C,C0) and append.
    vC, vC0, vnull = get_C_C0_nullity_qK(model, from_idx)
    vC  = Matrix{Float64}(vC)
    vC0 = Vector{Float64}(vC0)
    # Stack: equalities first (so they occupy the leading `nullity` rows), then
    # the regime's own inequalities. The regime may itself carry equalities
    # (vnull > 0); fold those into the equality block too.
    if vnull > 0
        eqC  = vcat(C[1:nullity, :], vC[1:vnull, :])
        eqC0 = vcat(C0[1:nullity],   vC0[1:vnull])
        ineqC  = vC[vnull+1:end, :]
        ineqC0 = vC0[vnull+1:end]
    else
        eqC = C[1:nullity, :]; eqC0 = C0[1:nullity]
        ineqC = vC; ineqC0 = vC0
    end
    fullC  = vcat(eqC, ineqC)
    fullC0 = vcat(eqC0, ineqC0)
    fullnull = size(eqC, 1)

    poly = get_polyhedron(fullC, fullC0, fullnull)
    if isempty(poly)
        error("Threshold placement infeasible: pinning input=$target_input_value " *
              "on the $from_idx->$to_idx interface leaves an empty from-regime cell.")
    end
    logqK = collect(Float64.(get_one_inner_point(poly)))
    kd, totals = _split_log_qK(model, logqK)

    # ---- VERIFY by forward scan: locate the breakpoint along the input sweep.
    # Hold the background (everything except the input coord) fixed at the solved
    # point; scan the input total across decades; the breakpoint is where the
    # assigned regime switches from `from_idx` to `to_idx`.
    fixed = copy(logqK); deleteat!(fixed, input_idx)
    onehot = zeros(Float64, length(x_sym(model))); onehot[output_idx] = 1.0
    rng = collect(range(logtgt - 2.0, logtgt + 2.0; length = 401))
    _, _, regimes = scan_parameter_1d(model, input_idx, rng, [onehot], fixed;
        input_logspace = true, output_logspace = true)

    # find first index where regime becomes to_idx coming from from_idx
    bp_log = NaN
    for i in 2:length(regimes)
        if regimes[i] == to_idx && regimes[i-1] == from_idx
            bp_log = (rng[i] + rng[i-1]) / 2
            break
        end
    end
    if isnan(bp_log)
        # fall back: first appearance of to_idx
        j = findfirst(==(to_idx), regimes)
        bp_log = j === nothing ? NaN : rng[j]
    end
    pass = isfinite(bp_log) && abs(bp_log - logtgt) <= tol_dec

    return (; kd, totals, from_idx, to_idx,
            target_input_value = Float64(target_input_value),
            measured_breakpoint = isnan(bp_log) ? NaN : exp10(bp_log),
            measured_breakpoint_log = bp_log, target_log = logtgt,
            log_qK = logqK, pass, input_idx, output_idx)
end

# =============================================================================
# DEMO
# =============================================================================

_fmt(v; d=4) = round.(v; digits=d)
_fmtsci(x; d=3) = string(round(x; sigdigits=d))

function _print_slope_result(label, target, res)
    kdstr = join(["Kd$i=$(_fmtsci(res.kd[i]))" for i in 1:length(res.kd)], ", ")
    totstr = join(["$(k)=$(_fmtsci(v))" for (k,v) in sort(collect(res.totals); by=first)], ", ")
    println("  [$label]  target_RO=$target")
    println("      -> vertex_idx = $(res.vertex_idx),  predicted_RO = $(res.predicted_RO)")
    println("      -> kd      = $kdstr")
    println("      -> totals  = $totstr   (one interior point of the regime cell)")
    println("      -> dominance ordering = $(res.dominance_ordering)")
    println("      -> VERIFIED measured slope = $(round(res.measured_RO; digits=4))   " *
            (res.pass ? "PASS ✓" : "FAIL ✗"))
    println()
end

function _print_bounded_header()
    println("    " * "-"^92)
    println("    " * rpad("target", 18) * rpad("kd_bounds", 14) * rpad("solved kd", 26) *
            rpad("meas.slope", 12) * rpad("feasible?", 11) * "binding bound")
    println("    " * "-"^92)
end

function _print_bounded_row(label, target, kb, res)
    kbstr = "[1e$(Int(kb[1])),1e$(Int(kb[2]))]"
    if !res.feasible
        println("    " * rpad(label, 18) * rpad(kbstr, 14) * rpad("(empty)", 26) *
                rpad("--", 12) * rpad("INFEASIBLE", 11) * res.binding_bound)
        println("        closest feasible: RO=$(res.closest_feasible_RO) at vertex $(res.closest_feasible_vertex)")
        return
    end
    kdstr = join(["Kd$i=$(_fmtsci(res.kd[i]))" for i in 1:length(res.kd)], ",")
    inbox = all(kb[1] - 1e-6 <= log10(k) <= kb[2] + 1e-6 for k in res.kd)
    feasstr = inbox ? "yes(inbox)" : "yes(OOB!)"
    bb = res.binding_bound
    mslope = round(res.measured_RO; digits = 4)
    println("    " * rpad(label, 18) * rpad(kbstr, 14) * rpad(kdstr, 26) *
            rpad(string(mslope), 12) * rpad(feasstr, 11) * bb)
    println("        ($(res.solve_mode), cheb_radius=$(isnan(res.chebyshev_radius) ? "n/a" : round(res.chebyshev_radius;digits=3)), predicted_RO=$(res.predicted_RO))")
end

function run_demo()
    println("="^78)
    println("ParameterPlacer demo  --  constructive (q,K) placement from regime polytopes")
    println("="^78)

    # ---------------- Witness 1: homo-tetramer ----------------
    println("\n### Witness 1: HOMO-TETRAMER   A+A<->AA, AA+AA<->AAAA   (input tA, output AAAA)")
    tet_rules = ["A + A <-> AA", "AA + AA <-> AAAA"]
    for target in (4, 2, 1)
        try
            res = place_for_target_slope(tet_rules, :tA, :AAAA, target; tol = 0.05)
            _print_slope_result("tetramer", target, res)
        catch e
            println("  [tetramer]  target_RO=$target  -> ERROR: ", sprint(showerror, e), "\n")
        end
    end

    # ---------------- Witness 2: heterodimer ----------------
    println("\n### Witness 2: HETERODIMER   A+B<->AB   (input tA, output AB)")
    het_rules = ["A + B <-> AB"]
    for target in (1, 0)
        try
            res = place_for_target_slope(het_rules, :tA, :AB, target; tol = 0.05)
            _print_slope_result("heterodimer", target, res)
        catch e
            println("  [heterodimer]  target_RO=$target  -> ERROR: ", sprint(showerror, e), "\n")
        end
    end

    # ---------------- Witness 3: PHYSICAL-Kd-BOUNDED placement ----------------
    println("\n### Witness 3: PHYSICAL-Kd-BOUNDED placement  (Kd in [1e-3, 1e3], log10 in [-3,3])")
    println("    Demonstrates the fix for the deep-interior Kd blow-up (e.g. RO=2 -> Kd1=6.5e-21).")
    _print_bounded_header()
    kb = (-3.0, 3.0)
    println("\n  -- HOMO-TETRAMER  A+A<->AA, AA+AA<->AAAA   (input tA, output AAAA) --")
    for target in (4, 2, 1)
        try
            res = place_for_target_slope(tet_rules, :tA, :AAAA, target;
                                         tol = 0.05, kd_bounds = kb)
            _print_bounded_row("tetramer RO=$target", target, kb, res)
        catch e
            println("    tetramer RO=$target  -> ERROR: ", sprint(showerror, e))
        end
    end
    println("\n  -- HETERODIMER  A+B<->AB   (input tA, output AB)  [contrast] --")
    for target in (1, 0)
        try
            res = place_for_target_slope(het_rules, :tA, :AB, target;
                                         tol = 0.05, kd_bounds = kb)
            _print_bounded_row("heterodimer RO=$target", target, kb, res)
        catch e
            println("    heterodimer RO=$target  -> ERROR: ", sprint(showerror, e))
        end
    end

    # ---------------- STRETCH: threshold placement on heterodimer ----------------
    println("\n### STRETCH: threshold placement (heterodimer AB-saturation knee)")
    try
        # Build once to discover which vertex pair is the unsaturated->saturated knee.
        m, _, _, _ = build_model(het_rules, [1.0])
        find_all_vertices!(m)
        siso = SISOPaths(m, :tA)
        bf = get_behavior_families(siso; observe_x = :AB, path_scope = :feasible,
            deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
            compute_volume = false)
        # Choose a feasible path that contains an RO=1 (rising) -> RO=0 (saturated)
        # transition; its consecutive vertex pair is the (from,to) interface.
        chosen = nothing
        for pr in bf.path_records
            pr.included || continue
            v = pr.vertex_indices; ro = pr.exact_profile
            length(v) == length(ro) || continue
            for k in 1:length(ro)-1
                if abs(ro[k] - 1) <= 0.05 && abs(ro[k+1] - 0) <= 0.05
                    chosen = (v[k], v[k+1]); break
                end
            end
            chosen === nothing || break
        end
        if chosen === nothing
            println("  No rising->saturated (1->0) interface found; skipping threshold demo.")
        else
            from_idx, to_idx = chosen
            target_tA = 5.0   # put the knee at total-A = 5.0 (linear units)
            println("  Using interface  vertex $from_idx -> $to_idx,  target tA breakpoint = $target_tA")
            tr = place_threshold(het_rules, :tA, :AB, from_idx, to_idx, target_tA; tol_dec = 0.15)
            kdstr = join(["Kd$i=$(_fmtsci(tr.kd[i]))" for i in 1:length(tr.kd)], ", ")
            totstr = join(["$(k)=$(_fmtsci(v))" for (k,v) in sort(collect(tr.totals); by=first)], ", ")
            println("      -> kd     = $kdstr")
            println("      -> totals = $totstr")
            println("      -> target breakpoint tA = $(tr.target_input_value)")
            println("      -> VERIFIED measured breakpoint tA = $(_fmtsci(tr.measured_breakpoint))   " *
                    "(log10 miss = $(round(tr.measured_breakpoint_log - tr.target_log; digits=3) ))   " *
                    (tr.pass ? "PASS ✓" : "FAIL ✗"))
        end
    catch e
        println("  [threshold]  -> ERROR: ", sprint(showerror, e))
    end

    println("\n", "="^78)
    println("Done.")
    println("="^78)
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_demo()
end
