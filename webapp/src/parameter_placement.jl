# ─── ParameterPlacer: constructive (q,K) placement for a target reaction order ───
#
# Inlined from the verified prototype `webapp/scripts/synth/parameter_placer.jl`
# (its `place_for_target_slope(...; kd_bounds=...)` path). The prototype is NOT
# `include`d here on purpose: its top-level `Pkg.activate(Bnc_julia)` would switch
# the running backend's active project. Every engine call below
# (find_all_vertices!, SISOPaths, get_behavior_families, get_polyhedron,
# get_one_inner_point, get_C_C0_nullity_qK, qK2x, q_sym, show_dominant_condition,
# scan_parameter_1d, parse_linear_combination) is already in scope via
# `using BindingAndCatalysis`; `build_model` via `using .ReactionParser`;
# `Polyhedra` via `using Polyhedra`. Pure polytope geometry in the solve;
# sampling is used only for the forward verification and the dose-response curve.

# Split an engine log10-(q,K) vector into linear-space kd (Kd1..Kdr) + totals dict.
function _placer_split_log_qK(model, logqK::AbstractVector{<:Real})
    d = model.d; r = model.r
    @assert length(logqK) == d + r
    kd = exp10.(Float64.(logqK[d+1:d+r]))
    qsyms = Symbol.(string.(q_sym(model)))
    totals = Dict{Symbol, Float64}()
    for i in 1:d
        totals[qsyms[i]] = exp10(Float64(logqK[i]))
    end
    return kd, totals
end

# Forward finite-difference local reaction order at a concrete operating point.
function _placer_measure_local_RO(model, logqK, input_idx::Int, output_idx::Int; h::Real = 0.02)
    base = collect(Float64.(logqK))
    f(delta) = begin
        p = copy(base); p[input_idx] += delta
        qK2x(model, p; input_logspace = true, output_logspace = true)[output_idx]
    end
    return (f(+h) - f(-h)) / (2h)
end

# Box rows encoding lo <= log10(Kd_i) <= hi for every binding constant, in the
# get_polyhedron(C, C0) sign convention (rows mean -C . x <= C0).
function _placer_kd_box_rows(model, lo::Real, hi::Real)
    d = model.d; r = model.r; n = model.n
    rows = Vector{Vector{Float64}}(); rhsv = Float64[]
    for i in (d+1):(d+r)
        e = zeros(Float64, n); e[i] = 1.0
        push!(rows, -e); push!(rhsv, Float64(hi))    # z_i <= hi
        push!(rows,  e); push!(rhsv, -Float64(lo))   # z_i >= lo
    end
    C = reduce(vcat, (reshape(c, 1, n) for c in rows))
    return C, rhsv
end

# Most-interior point of a (bounded-or-coned) polyhedron: Chebyshev center when
# finite, else a moderate inner point.
function _placer_most_interior_point(poly; extend::Real = 3)
    try
        ctr, rad = Polyhedra.hchebyshevcenter(poly; verbose = 0, proper = false)
        if isfinite(rad)
            return collect(Float64.(ctr)), Float64(rad), :chebyshev
        end
    catch
        # LP backend (CDD) cannot solve this center; fall through.
    end
    ip = collect(Float64.(get_one_inner_point(poly; rand_line = false, rand_ray = false, extend = extend)))
    return ip, NaN, :inner_point
end

# Constructive, physical-Kd-bounded placement of a (q,K) point realizing
# `target_RO` (local log-log slope of `output_sym` w.r.t. the total of
# `input_sym`). Returns a NamedTuple; on infeasible (target regime ∩ Kd-box
# empty, or no regime matches) returns feasible=false with closest_RO/vertex.
function placer_place_bounded(rules::Vector{String}, input_sym, output_sym,
                              target_RO::Real; tol::Real = 0.05, verify_h::Real = 0.02,
                              kd_bounds::Tuple{<:Real, <:Real})
    lo, hi = Float64(kd_bounds[1]), Float64(kd_bounds[2])
    lo < hi || error("kd_bounds must be (lo_log10, hi_log10) with lo < hi")
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)

    model, species, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx = locate_sym_qK(model, input_sym)
    input_idx === nothing && error("Unknown input parameter: $input_sym")
    output_idx = locate_sym_x(model, output_sym)
    output_idx === nothing && error("Unknown output species: $output_sym")

    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)

    chosen_v = nothing; chosen_ro = nothing
    best_miss = Inf; best_v = nothing; best_ro = nothing
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            miss = abs(ro - target_RO)
            if miss < best_miss; best_miss = miss; best_v = v; best_ro = ro; end
            if miss <= tol; chosen_v = v; chosen_ro = ro; break; end
        end
        chosen_v === nothing || break
    end

    if chosen_v === nothing
        return (; feasible = false, kd_bounds = (lo, hi),
            reason = "No regime realizes target_RO=$target_RO (tol=$tol) for output " *
                     "$output_sym vs input $input_sym. Closest achievable RO is " *
                     "$best_ro at vertex $best_v. This target is infeasible for this topology.",
            closest_RO = best_ro, closest_vertex = best_v,
            input_idx, output_idx, model)
    end

    vC, vC0, vnull = get_C_C0_nullity_qK(model, chosen_v)
    vC = Matrix{Float64}(vC); vC0 = Vector{Float64}(vC0)
    if vnull > 0
        eqC = vC[1:vnull, :];     eqC0 = vC0[1:vnull]
        inC = vC[vnull+1:end, :]; inC0 = vC0[vnull+1:end]
    else
        eqC = zeros(0, model.n);  eqC0 = Float64[]
        inC = vC;                 inC0 = vC0
    end
    boxC, boxC0 = _placer_kd_box_rows(model, lo, hi)
    poly = get_polyhedron(vcat(eqC, inC, boxC), vcat(eqC0, inC0, boxC0), size(eqC, 1))

    if isempty(poly)
        return (; feasible = false, kd_bounds = (lo, hi),
            reason = "Target regime ∩ Kd-box [1e$lo, 1e$hi] is empty. " *
                     "Closest feasible RO under these bounds is $best_ro at vertex $best_v.",
            closest_RO = best_ro, closest_vertex = best_v,
            input_idx, output_idx, model)
    end

    logqK, cheb_r, mode = _placer_most_interior_point(poly; extend = 3)
    kd, totals = _placer_split_log_qK(model, logqK)
    dominance = string.(show_dominant_condition(model, chosen_v))
    measured = _placer_measure_local_RO(model, logqK, input_idx, output_idx; h = verify_h)
    pass = abs(measured - target_RO) <= max(tol, 5 * verify_h)

    return (; feasible = true, kd, totals, vertex_idx = chosen_v, predicted_RO = chosen_ro,
            dominance_ordering = dominance, log_qK = logqK, measured_RO = measured, pass,
            kd_bounds = (lo, hi), chebyshev_radius = cheb_r, solve_mode = mode,
            input_idx, output_idx, model, species)
end

# Verifying dose-response curve at the solved (kd, totals): build the model with
# the solved kd, set the solved totals as background, sweep the input total, and
# observe the output — reusing the exact `scan_parameter_1d` engine path of
# `handle_parameter_scan_1d` so the result has the `plotParameterScan1D` shape.
function placer_dose_response(rules::Vector{String}, kd::Vector{Float64},
                              totals::Dict{Symbol, Float64}, input_sym::Symbol,
                              output_expr::String; param_min = -6.0, param_max = 6.0,
                              n_points = 200)
    model, _, _, _ = build_model(rules, kd)
    input_idx = locate_sym_qK(model, input_sym)
    coeffs = parse_linear_combination(model, output_expr)
    qsyms = Symbol.(string.(q_sym(model)))
    fixed_qK = zeros(Float64, model.n)
    for i in 1:model.d
        fixed_qK[i] = log10(get(totals, qsyms[i], 1.0))
    end
    for j in 1:model.r
        fixed_qK[model.d + j] = log10(kd[j])
    end
    fixed_params = deleteat!(copy(fixed_qK), input_idx)
    n_pts = clamp(Int(n_points), 10, 1000)
    param_range = collect(range(Float64(param_min), Float64(param_max), length = n_pts))
    param_vals, output_traj, regimes = scan_parameter_1d(
        model, input_idx, param_range, [coeffs], fixed_params;
        input_logspace = true, output_logspace = true)
    return Dict(
        "param_symbol" => string(input_sym),
        "param_values" => param_vals,
        "output_exprs" => [output_expr],
        "output_traj"  => mat2vv(output_traj),
        "regimes"      => regimes,
        "x_sym"        => string.(model.x_sym),
    )
end

# POST /api/place_parameters
# Body: { rules:[...] (or session_id/network/hash), input_sym, output_sym,
#         target_ro, kd_bounds:[lo,hi]|null, tol|null }
function handle_place_parameters(req)
    body = read_json(req)

    # Reactions: explicit `rules` win; otherwise resolve a model bundle and use
    # its rules (session_id / network / network_ir_hash).
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)

    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    target_ro  = Float64(_raw_get(body, :target_ro, 1.0))
    tol        = Float64(_raw_get(body, :tol, 0.05))

    kb_raw = _raw_get(body, :kd_bounds, nothing)
    kd_bounds = if kb_raw === nothing
        (-3.0, 3.0)
    else
        kbv = Float64.(collect(kb_raw))
        length(kbv) == 2 || return error_response("`kd_bounds` must be [lo_log10, hi_log10]"; status = 400)
        (kbv[1], kbv[2])
    end

    res = try
        placer_place_bounded(rules, input_sym, output_sym, target_ro; tol = tol, kd_bounds = kd_bounds)
    catch e
        return error_response("ParameterPlacer failed: $(sprint(showerror, e))"; status = 400)
    end

    if !res.feasible
        return json_response(Dict(
            "error" => res.reason,
            "closest_RO" => json_safe_value(get(res, :closest_RO, nothing)),
            "closest_vertex" => get(res, :closest_vertex, nothing),
        ))
    end

    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end

    return json_response(Dict(
        "kd"                  => kd_vec,
        "totals"              => Dict(string(k) => v for (k, v) in res.totals),
        "vertex_idx"          => res.vertex_idx,
        "predicted_RO"        => json_safe_real(res.predicted_RO),
        "measured_RO"         => json_safe_real(res.measured_RO),
        "pass"                => res.pass,
        "dominance_ordering"  => res.dominance_ordering,
        "kd_bounds"           => collect(res.kd_bounds),
        "solve_mode"          => string(res.solve_mode),
        "dose_response_curve" => curve,
    ))
end

# Achievable reaction-order MENU for a network: the distinct finite RO values the
# output can take across the feasible dominance regimes (the quantized ladder),
# each with a representative regime + its dominance ordering. Enumerated from the
# regimes (no sampling) — mirrors placer_place_bounded's path-record loop.
function placer_menu(rules::Vector{String}, input_sym, output_sym)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)
    seen = Dict{Float64, Int}()
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        for (v, ro) in zip(vtxs, ros)
            isfinite(ro) || continue
            rr = round(Float64(ro), digits = 3)
            haskey(seen, rr) || (seen[rr] = v)
        end
    end
    rungs = sort(collect(keys(seen)))
    regimes = [Dict("ro" => r, "vertex_idx" => seen[r],
                    "dominance" => string.(show_dominant_condition(model, seen[r]))) for r in rungs]
    # Representative (richest) feasible path: the ordered regime sequence + per-regime RO.
    # Its consecutive regimes are the transitions a "threshold" can be placed at.
    best = nothing; best_n = -1
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        nd = length(unique(r for r in ros if isfinite(r)))
        if nd > best_n; best_n = nd; best = (vtxs, ros); end
    end
    path = best === nothing ? Any[] :
        [Dict("vertex_idx" => v, "ro" => (isfinite(r) ? round(Float64(r), digits = 3) : nothing))
         for (v, r) in zip(best[1], best[2])]
    return Dict("ladder" => rungs, "regimes" => regimes, "path" => path)
end

# POST /api/placer_menu — { rules|session, input_sym, output_sym } -> { ladder, regimes }
function handle_placer_menu(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    res = try
        placer_menu(rules, Symbol(_raw_get(body, :input_sym, "")), Symbol(_raw_get(body, :output_sym, "")))
    catch e
        return error_response("ParameterPlacer menu failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(res)
end

# POST /api/placer_curve — { rules|session, input_sym, output_sym, kd:[...], totals:{}|null }
# -> dose-response curve at the given kd (for the live-tuning slider). Reuses placer_dose_response.
function handle_placer_curve(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    _raw_haskey(body, :input_sym)  || return error_response("`input_sym` is required"; status = 400)
    _raw_haskey(body, :output_sym) || return error_response("`output_sym` is required"; status = 400)
    _raw_haskey(body, :kd)         || return error_response("`kd` is required"; status = 400)
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = String(_raw_get(body, :output_sym, ""))
    kd = Float64.(collect(_raw_get(body, :kd, Float64[])))
    totals = Dict{Symbol, Float64}()
    tw = _raw_get(body, :totals, nothing)
    if tw !== nothing
        for (k, v) in pairs(tw); totals[Symbol(k)] = Float64(v); end
    end
    curve = try
        placer_dose_response(rules, kd, totals, input_sym, output_sym)
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(curve)
end

# Place a regime TRANSITION (threshold / EC50 / knee) at a target input dose, by
# pinning the input coordinate AND sitting on the from->to interface hyperplane,
# then taking an interior point of the from-regime cell. Ported from the standalone
# place_threshold (uses BindingAndCatalysis.get_interface_qK). Verifies by forward scan.
function placer_threshold(rules::Vector{String}, input_sym, output_sym,
                          from_idx::Int, to_idx::Int, target_input_value::Real; tol_dec::Real = 0.2)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    SISOPaths(model, input_sym)   # populate oriented qK interfaces along the input axis
    input_idx  = locate_sym_qK(model, input_sym); input_idx === nothing && error("Unknown input: $input_sym")
    output_idx = locate_sym_x(model, output_sym); output_idx === nothing && error("Unknown output: $output_sym")
    n = model.n
    dir, c = try
        BindingAndCatalysis.get_interface_qK(model, from_idx, to_idx)
    catch
        BindingAndCatalysis.get_interface_direct(model, from_idx, to_idx)
    end
    dirv = collect(Float64.(Vector(dir)))
    logtgt = log10(Float64(target_input_value))
    e_input = zeros(Float64, n); e_input[input_idx] = 1.0
    # equality rows (get_polyhedron builds hrep(-C,C0); equality a·x==rhs -> row a, C0 -rhs)
    C  = vcat(reshape(e_input, 1, n), reshape(dirv, 1, n))
    C0 = Float64[-logtgt, c]
    vC, vC0, vnull = get_C_C0_nullity_qK(model, from_idx)
    vC = Matrix{Float64}(vC); vC0 = Vector{Float64}(vC0)
    if vnull > 0
        eqC = vcat(C, vC[1:vnull, :]); eqC0 = vcat(C0, vC0[1:vnull])
        ineqC = vC[vnull+1:end, :];    ineqC0 = vC0[vnull+1:end]
    else
        eqC = C; eqC0 = C0; ineqC = vC; ineqC0 = vC0
    end
    poly = get_polyhedron(vcat(eqC, ineqC), vcat(eqC0, ineqC0), size(eqC, 1))
    isempty(poly) && return (; feasible = false,
        reason = "Threshold infeasible: pinning input=$target_input_value on the " *
                 "$from_idx→$to_idx interface empties the from-regime cell.")
    logqK = collect(Float64.(get_one_inner_point(poly)))
    kd, totals = _placer_split_log_qK(model, logqK)
    # verify: scan the input across decades at the solved background; find the from->to breakpoint
    fixed = copy(logqK); deleteat!(fixed, input_idx)
    onehot = zeros(Float64, length(x_sym(model))); onehot[output_idx] = 1.0
    rng = collect(range(logtgt - 2.0, logtgt + 2.0; length = 401))
    _, _, regimes = scan_parameter_1d(model, input_idx, rng, [onehot], fixed;
        input_logspace = true, output_logspace = true)
    bp_log = NaN
    for i in 2:length(regimes)
        if regimes[i] == to_idx && regimes[i-1] == from_idx
            bp_log = (rng[i] + rng[i-1]) / 2; break
        end
    end
    pass = isfinite(bp_log) && abs(bp_log - logtgt) <= tol_dec
    return (; feasible = true, kd, totals, target_input = Float64(target_input_value),
            measured_breakpoint = isfinite(bp_log) ? 10.0^bp_log : NaN,
            pass, dominance_ordering = string.(show_dominant_condition(model, to_idx)))
end

# POST /api/placer_threshold — { rules|session, input_sym, output_sym, from_idx, to_idx, target_input }
function handle_placer_threshold(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions"; status = 400)
    for k in (:input_sym, :output_sym, :from_idx, :to_idx)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    from_idx   = Int(_raw_get(body, :from_idx, 0))
    to_idx     = Int(_raw_get(body, :to_idx, 0))
    target     = Float64(_raw_get(body, :target_input, 1.0))
    res = try
        placer_threshold(rules, input_sym, output_sym, from_idx, to_idx, target)
    catch e
        return error_response("Threshold placement failed: $(sprint(showerror, e))"; status = 400)
    end
    res.feasible || return json_response(Dict("error" => res.reason))
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "kd"                  => kd_vec,
        "totals"              => Dict(string(k) => v for (k, v) in res.totals),
        "target_input"        => res.target_input,
        "measured_breakpoint" => json_safe_real(res.measured_breakpoint),
        "pass"                => res.pass,
        "dominance_ordering"  => res.dominance_ordering,
        "dose_response_curve" => curve,
    ))
end

# ════════ REALIZE THE WHOLE REGIME PROGRAM (the design target is a program) ════════
# place_parameters lands ONE regime (one slope). A design target is a regime
# PROGRAM — the ordered sequence of slopes the dose-response walks. The network was
# selected because its asymptotic itinerary already matches that program; realizing
# it = solving a Kd ORDERING (the background kd + non-input totals) that lays the
# whole transition sequence on a monotone input-dose ladder, so the swept response
# traverses the program with separated breakpoints. This is the "no-sampling via Kd
# ordering" step: each consecutive regime pair has an interface hyperplane
# dir·logqK = c (get_interface_qK); the breakpoint dose is where it is crossed as the
# input sweeps, which is LINEAR in the background — so placing the k transitions at a
# monotone dose ladder is one linear solve. Forward-verified by sweeping the input.
function _placer_segment_ros(pvals::Vector{Float64}, otraj::Vector{Float64},
                             regimes::Vector{Int})
    # Walk the swept regime labels; for each maximal run, measure the local log-log
    # slope (reaction order) at its interior and record the run + its breakpoint dose.
    segs = Vector{NamedTuple{(:vtx, :ro, :i0, :i1), Tuple{Int, Float64, Int, Int}}}()
    i = 1; N = length(regimes)
    while i <= N
        j = i
        while j < N && regimes[j+1] == regimes[i]; j += 1; end
        # interior central-difference slope (skip the 1-2 endpoints touching the knee)
        a = clamp(i + 1, 1, N); b = clamp(j - 1, 1, N)
        ro = NaN
        if b > a
            num = otraj[b] - otraj[a]; den = pvals[b] - pvals[a]
            ro = den == 0 ? NaN : num / den
        elseif j > i
            num = otraj[j] - otraj[i]; den = pvals[j] - pvals[i]
            ro = den == 0 ? NaN : num / den
        end
        push!(segs, (; vtx = regimes[i], ro = ro, i0 = i, i1 = j))
        i = j + 1
    end
    return segs
end
function _collapse_signs(ros)
    out = Char[]
    for r in ros
        (isfinite(r) && abs(r) > 0.05) || continue
        c = r > 0 ? '+' : '-'
        (isempty(out) || out[end] != c) && push!(out, c)
    end
    return String(out)
end
function placer_realize_program(rules::Vector{String}, input_sym, output_sym;
                                kd_bounds::Tuple{<:Real, <:Real} = (-3.0, 3.0),
                                decade_gap::Real = 2.0)
    input_sym = Symbol(input_sym); output_sym = Symbol(output_sym)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx  = locate_sym_qK(model, input_sym);  input_idx  === nothing && error("Unknown input $input_sym")
    output_idx = locate_sym_x(model, output_sym);  output_idx === nothing && error("Unknown output $output_sym")
    siso = SISOPaths(model, input_sym)
    bf = get_behavior_families(siso; observe_x = output_sym, path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true, compute_volume = false)
    best = nothing; best_n = -1
    for pr in bf.path_records
        pr.included || continue
        vtxs = pr.vertex_indices; ros = pr.exact_profile
        length(vtxs) == length(ros) || continue
        nd = length(unique(r for r in ros if isfinite(r)))
        if nd > best_n; best_n = nd; best = (collect(Int, vtxs), collect(Float64, ros)); end
    end
    best === nothing && error("No feasible regime path for output $output_sym")
    vtxs, ros = best
    m = length(vtxs)
    m >= 2 || error("This output has a single regime — use a single-RO solve, not a program")

    n = model.n
    k = m - 1
    # target breakpoint log10-doses: a monotone ladder centred at 0 (the Kd ordering).
    τ = [ (i - (k + 1) / 2) * Float64(decade_gap) for i in 1:k ]
    bg_idx = [j for j in 1:n if j != input_idx]
    A = zeros(Float64, k, length(bg_idx)); bvec = zeros(Float64, k); ok = trues(k)
    for i in 1:k
        from_i = vtxs[i]; to_i = vtxs[i + 1]
        dir, c = try
            BindingAndCatalysis.get_interface_qK(model, from_i, to_i)
        catch
            try BindingAndCatalysis.get_interface_direct(model, from_i, to_i) catch; (nothing, nothing) end
        end
        if dir === nothing; ok[i] = false; continue; end
        dirv = collect(Float64.(Vector(dir)))
        di = dirv[input_idx]
        if abs(di) < 1e-9; ok[i] = false; continue; end   # this interface isn't moved by the input
        # Normalise the row by the input coefficient so least-squares minimises the
        # actual breakpoint-POSITION error |log10(dose_i) - τ_i| (decades), not the
        # raw hyperplane residual — this is what spreads the transitions on the ladder.
        bvec[i] = Float64(c) / di - τ[i]
        for (col, j) in enumerate(bg_idx); A[i, col] = dirv[j] / di; end
    end
    rows = findall(ok)
    isempty(rows) && error("No placeable interfaces along the program path")
    z = A[rows, :] \ bvec[rows]   # least-squares: monotone-ladder Kd ordering
    logqK = zeros(Float64, n)
    for (col, j) in enumerate(bg_idx); logqK[j] = z[col]; end
    logqK[input_idx] = 0.0
    lo, hi = Float64(kd_bounds[1]), Float64(kd_bounds[2])
    for j in (model.d + 1):(model.d + model.r); logqK[j] = clamp(logqK[j], lo, hi); end
    kd, totals = _placer_split_log_qK(model, logqK)

    # Forward-verify: sweep the input across the full ladder window, read the realized
    # regime sequence + per-segment reaction orders + breakpoint doses.
    span = max((k + 2) * Float64(decade_gap), 6.0)
    rng = collect(range(-span / 2, span / 2; length = 700))
    fixed = copy(logqK); deleteat!(fixed, input_idx)
    coeffs = parse_linear_combination(model, String(output_sym))
    pvals, otrajm, regimes = scan_parameter_1d(model, input_idx, rng, [coeffs], fixed;
        input_logspace = true, output_logspace = true)
    otraj = vec(collect(Float64.(otrajm)))
    regv  = Int.(collect(regimes))
    segs = _placer_segment_ros(pvals, otraj, regv)
    # Keep only segments that occupy a real input interval (≥0.4 decade) — a genuine
    # asymptotic plateau, not a transient sliver between clustered breakpoints.
    WIDTH_MIN = 0.4
    wide = [s for s in segs if (pvals[s.i1] - pvals[s.i0]) >= WIDTH_MIN]
    isempty(wide) && (wide = segs)
    measured_ros = [s.ro for s in wide]
    breakpoints = Float64[]
    for s in wide[2:end]; push!(breakpoints, 10.0 ^ pvals[s.i0]); end

    target_finite = [round(r, digits = 3) for r in ros if isfinite(r)]
    measured_signs = _collapse_signs(measured_ros)
    target_signs   = _collapse_signs(target_finite)
    pass = measured_signs == target_signs && !isempty(target_signs)

    return (; feasible = true, kd, totals, log_qK = logqK,
            target_program = target_finite, measured_program = [round(r, digits = 3) for r in measured_ros if isfinite(r)],
            target_signs, measured_signs, breakpoints, pass,
            dominance_ordering = string.(show_dominant_condition(model, vtxs[end])),
            n_transitions = k, n_placed = length(rows))
end

# POST /api/placer_realize_program — { rules|session, input_sym, output_sym, kd_bounds?:[lo,hi] }
function handle_placer_realize_program(req)
    body = read_json(req)
    rules = if _raw_haskey(body, :rules)
        String.(collect(_raw_get(body, :rules, String[])))
    else
        bundle, err = _resolve_bundle_or_response(body)
        err === nothing || return err
        String.(collect(bundle["rules"]))
    end
    isempty(rules) && return error_response("No reactions: provide `rules` or a resolvable model"; status = 400)
    for k in (:input_sym, :output_sym)
        _raw_haskey(body, k) || return error_response("`$k` is required"; status = 400)
    end
    input_sym  = Symbol(_raw_get(body, :input_sym, ""))
    output_sym = Symbol(_raw_get(body, :output_sym, ""))
    kdb = _raw_get(body, :kd_bounds, nothing)
    kd_bounds = (kdb === nothing) ? (-3.0, 3.0) : (Float64(kdb[1]), Float64(kdb[2]))
    res = try
        placer_realize_program(rules, input_sym, output_sym; kd_bounds = kd_bounds)
    catch e
        return error_response("Program realization failed: $(sprint(showerror, e))"; status = 400)
    end
    kd_vec = collect(Float64.(res.kd))
    curve = try
        placer_dose_response(rules, kd_vec, res.totals, input_sym, String(output_sym))
    catch e
        return error_response("Dose-response curve failed: $(sprint(showerror, e))"; status = 400)
    end
    return json_response(Dict(
        "kd"                 => kd_vec,
        "totals"             => Dict(string(k) => v for (k, v) in res.totals),
        "target_program"     => [json_safe_real(x) for x in res.target_program],
        "measured_program"   => [json_safe_real(x) for x in res.measured_program],
        "target_signs"       => res.target_signs,
        "measured_signs"     => res.measured_signs,
        "breakpoints"        => [json_safe_real(x) for x in res.breakpoints],
        "pass"               => res.pass,
        "n_transitions"      => res.n_transitions,
        "n_placed"           => res.n_placed,
        "dominance_ordering" => res.dominance_ordering,
        "dose_response_curve" => curve,
    ))
end
