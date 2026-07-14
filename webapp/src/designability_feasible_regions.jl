# Exact designability certificates over qK-space regions.

Base.@kwdef struct DesignabilityCellResult
    vertex_idx::Int
    predicted_ro::Float64
    path_idx::Int = 0
    vertex_indices::Vector{Int} = Int[]
    witness_vertex_indices::Vector{Int} = Int[]
    predicted_profile::Vector{Float64} = Float64[]
    qK_symbols::Vector{String} = String[]
    witness_input_log10::Vector{Float64} = Float64[]
    sampled_dynamic_range_log10::Float64 = NaN
    sampled_dynamic_range_fold_change::Float64 = NaN
    sampled_dynamic_range_points::Int = 0
    sampled_dynamic_range_floor_limited::Bool = false
    sampled_output_feature_status::String = ""
    sampled_output_feature_name::String = ""
    sampled_output_feature_operator::String = ""
    sampled_output_feature_target::Float64 = NaN
    sampled_output_feature_value::Float64 = NaN
    sampled_output_feature_range::Vector{Float64} = Float64[]
    sampled_output_feature_log10_range::Float64 = NaN
    sampled_output_feature_fold_change::Float64 = NaN
    sampled_output_feature_sample_points::Int = 0
    sampled_output_feature_floor_limited::Bool = false
    sampled_shape_status::String = ""
    sampled_shape_class::String = ""
    sampled_shape_direction::String = ""
    sampled_shape_peak_index::Int = 0
    sampled_shape_sample_points::Int = 0
    sampled_shape_floor_limited::Bool = false
    sampled_shape_min_prominence_log10::Float64 = NaN
    sampled_shape_prominence_left_log10::Float64 = NaN
    sampled_shape_prominence_right_log10::Float64 = NaN
    chebyshev_radius::Float64
    parameter_chebyshev_radius::Float64 = NaN
    augmented_chebyshev_radius::Float64 = NaN
    parameter_margin_dimension::Int = 0
    parameter_margin_equality_rank::Int = 0
    parameter_margin_basis::String = ""
    log_qK::Vector{Float64}
    kd::Vector{Float64}
    totals::Dict{String, Float64}
    solve_mode::String
end

Base.@kwdef struct FeasibleRegionResult
    feasible::Bool
    certificate_grade::String = "exact-union-siso-rop"
    cells::Vector{DesignabilityCellResult} = DesignabilityCellResult[]
    reason::String = ""
end

function _designability_split_eq_ineq(C, C0, nullity::Integer, n::Integer)
    Cf = Matrix{Float64}(C)
    C0f = Vector{Float64}(C0)
    if nullity > 0
        return (
            Cf[1:nullity, :],
            C0f[1:nullity],
            Cf[(nullity + 1):end, :],
            C0f[(nullity + 1):end],
        )
    end
    return (zeros(0, n), Float64[], Cf, C0f)
end

function _designability_bounds_tuple(raw, fallback)
    raw === nothing && return fallback
    (raw isa AbstractVector || raw isa Tuple) || return fallback
    vals = try
        values = collect(raw)
        length(values) == 2 || return fallback
        any(value -> value isa Bool || !(value isa Real), values) && return fallback
        Float64.(values)
    catch
        return fallback
    end
    vals[1] <= vals[2] || return fallback
    return (vals[1], vals[2])
end

function _designability_qk_box_rows(model, bounds::AbstractDict)
    default_bounds = _designability_bounds_tuple(_raw_get(bounds, :default, nothing), (-Inf, Inf))
    class_bounds = _raw_get(bounds, :by_class, Dict{String, Any}())
    kd_bounds = _designability_bounds_tuple(_raw_get(class_bounds, :kd, nothing), default_bounds)
    total_bounds = _designability_bounds_tuple(_raw_get(class_bounds, :total, nothing), default_bounds)

    rows = Vector{Vector{Float64}}()
    rhsv = Float64[]
    for i in 1:model.n
        lo, hi = i <= model.d ? total_bounds : kd_bounds
        if isfinite(hi)
            e = zeros(Float64, model.n)
            e[i] = 1.0
            push!(rows, -e)
            push!(rhsv, hi)
        end
        if isfinite(lo)
            e = zeros(Float64, model.n)
            e[i] = 1.0
            push!(rows, e)
            push!(rhsv, -lo)
        end
    end
    isempty(rows) && return zeros(0, model.n), Float64[]
    return reduce(vcat, (reshape(c, 1, model.n) for c in rows)), rhsv
end

function _designability_qk_box_rows_for_symbols(model, qk_symbols::AbstractVector, bounds::AbstractDict)
    default_bounds = _designability_bounds_tuple(_raw_get(bounds, :default, nothing), (-Inf, Inf))
    class_bounds = _raw_get(bounds, :by_class, Dict{String, Any}())
    kd_bounds = _designability_bounds_tuple(_raw_get(class_bounds, :kd, nothing), default_bounds)
    total_bounds = _designability_bounds_tuple(_raw_get(class_bounds, :total, nothing), default_bounds)
    total_symbols = Set(Symbol.(string.(q_sym(model))))
    kd_symbols = Set(Symbol.(string.(K_sym(model))))

    symbols = Symbol.(string.(qk_symbols))
    rows = Vector{Vector{Float64}}()
    rhsv = Float64[]
    for (i, sym) in enumerate(symbols)
        lo, hi = if sym in total_symbols
            total_bounds
        elseif sym in kd_symbols
            kd_bounds
        else
            default_bounds
        end
        if isfinite(hi)
            e = zeros(Float64, length(symbols))
            e[i] = 1.0
            push!(rows, -e)
            push!(rhsv, hi)
        end
        if isfinite(lo)
            e = zeros(Float64, length(symbols))
            e[i] = 1.0
            push!(rows, e)
            push!(rhsv, -lo)
        end
    end
    isempty(rows) && return zeros(0, length(symbols)), Float64[]
    return reduce(vcat, (reshape(c, 1, length(symbols)) for c in rows)), rhsv
end

function _designability_split_projected_log_qK(model, qk_symbols::AbstractVector, logqK::AbstractVector{<:Real})
    symbols = Symbol.(string.(qk_symbols))
    values = Dict(sym => Float64(value) for (sym, value) in zip(symbols, logqK))
    kd = Float64[]
    for sym in Symbol.(string.(K_sym(model)))
        push!(kd, haskey(values, sym) ? exp10(values[sym]) : NaN)
    end
    totals = Dict{String, Float64}()
    for sym in Symbol.(string.(q_sym(model)))
        haskey(values, sym) && (totals[String(sym)] = exp10(values[sym]))
    end
    return kd, totals
end

function _designability_collapse_finite_profile(profile)
    out = Float64[]
    for ro in collect(profile)
        isfinite(ro) || continue
        value = round(Float64(ro), digits = 3)
        (isempty(out) || out[end] != value) && push!(out, value)
    end
    return out
end

function _designability_collapse_profile_vertices(profile, vertex_indices, target_profile::AbstractVector{<:Real}; tol::Real = 0.05)
    values = Float64[]
    vertex_choices = Vector{Vector{Int}}()
    for (vertex_idx, ro) in zip(collect(vertex_indices), collect(profile))
        isfinite(ro) || continue
        value = round(Float64(ro), digits = 3)
        if isempty(values) || values[end] != value
            push!(values, value)
            push!(vertex_choices, Int[Int(vertex_idx)])
        else
            push!(vertex_choices[end], Int(vertex_idx))
        end
    end
    target = [round(Float64(ro), digits = 3) for ro in target_profile]
    length(values) == length(target) || return nothing
    all(abs(values[i] - target[i]) <= Float64(tol) for i in eachindex(values)) || return nothing
    return values, vertex_choices
end

function _designability_profile_matches(profile, target_profile::AbstractVector{<:Real}; tol::Real = 0.05)
    vals = _designability_collapse_finite_profile(profile)
    target = [round(Float64(ro), digits = 3) for ro in target_profile]
    length(vals) == length(target) || return false
    return all(abs(vals[i] - target[i]) <= Float64(tol) for i in eachindex(vals))
end

function _designability_full_indices_for_symbols(model, qk_symbols::AbstractVector)
    full_symbols = Symbol.(string.(qK_sym(model)))
    out = Int[]
    for sym in Symbol.(string.(qk_symbols))
        idx = findfirst(==(sym), full_symbols)
        idx === nothing && error("unknown qK symbol in projected path polyhedron: $sym")
        push!(out, Int(idx))
    end
    return out
end

function _designability_projected_indices_for_siso(model, siso, input_idx::Integer)
    full_without_input = deleteat!(collect(1:model.n), Int(input_idx))
    projected = _designability_full_indices_for_symbols(model, qK_sym(siso))
    projected == full_without_input ||
        error("SISO projected qK coordinate order does not match full qK without input coordinate")
    return projected
end

function _designability_push_augmented_vertex_rows!(eq_rows, eq_rhs, in_rows, in_rhs,
                                                    model, vertex_idx::Integer,
                                                    input_idx::Integer,
                                                    projected_full_indices::Vector{Int},
                                                    witness_idx::Integer,
                                                    witness_count::Integer)
    vC, vC0, vnull = get_C_C0_nullity_qK(model, Int(vertex_idx))
    C = Matrix{Float64}(vC)
    C0 = Vector{Float64}(vC0)
    n_bg = length(projected_full_indices)
    n_aug = n_bg + Int(witness_count)
    for row_idx in axes(C, 1)
        row = zeros(Float64, n_aug)
        for (bg_idx, full_idx) in enumerate(projected_full_indices)
            row[bg_idx] = C[row_idx, full_idx]
        end
        row[n_bg + Int(witness_idx)] = C[row_idx, Int(input_idx)]
        if row_idx <= vnull
            push!(eq_rows, row)
            push!(eq_rhs, C0[row_idx])
        else
            push!(in_rows, row)
            push!(in_rhs, C0[row_idx])
        end
    end
    return nothing
end

function _designability_push_augmented_rows!(rows, rhs, baseC, baseC0, n_aug::Integer)
    for row_idx in axes(baseC, 1)
        row = zeros(Float64, n_aug)
        row[1:size(baseC, 2)] .= baseC[row_idx, :]
        push!(rows, row)
        push!(rhs, Float64(baseC0[row_idx]))
    end
    return nothing
end

function _designability_path_projected_rows(siso, path_idx::Integer, n_bg::Integer)
    pC, pC0, pnull = try
        get_C_C0_nullity_qK(siso, Int(path_idx))
    catch
        poly = get_polyhedron(siso, Int(path_idx))
        get_C_C0_nullity(poly)
    end
    return _designability_split_eq_ineq(pC, pC0, pnull, n_bg)
end

function _designability_vertex_choice_products(vertex_choices::Vector{Vector{Int}})
    isempty(vertex_choices) && return ()
    return Iterators.product(vertex_choices...)
end

function _designability_bounded_product_count(
    choice_counts::AbstractVector{<:Integer};
    limit::Integer=typemax(Int),
)
    limit >= 0 || throw(ArgumentError("Designability cell limit must be nonnegative."))
    total = 1
    for raw_count in choice_counts
        raw_count >= 0 || throw(ArgumentError(
            "Designability vertex-choice counts must be nonnegative."))
        count = try
            Int(raw_count)
        catch
            throw(SyncBudgetExceeded(
                "Exact feasible-region cell count exceeds the supported integer range."))
        end
        count == 0 && return 0
        total > Int(limit) ÷ count && throw(SyncBudgetExceeded(
            "Exact feasible-region cell enumeration exceeds the limit of $(limit)."))
        total *= count
    end
    return total
end

function _designability_add_cell_count(
    current::Integer,
    additional::Integer,
    limit::Union{Nothing, Integer},
)
    current >= 0 && additional >= 0 || throw(ArgumentError(
        "Designability cell counts must be nonnegative."))
    effective_limit = limit === nothing ? typemax(Int) : Int(limit)
    current > effective_limit - additional && throw(SyncBudgetExceeded(
        "Exact feasible-region cell enumeration exceeds the limit of $(effective_limit)."))
    return Int(current + additional)
end

function _designability_rows_matrix(rows::Vector{Vector{Float64}}, ncols::Integer)
    isempty(rows) && return zeros(0, Int(ncols))
    return reduce(vcat, (reshape(row, 1, Int(ncols)) for row in rows))
end

function _designability_conditional_parameter_margin(
    eqC::AbstractMatrix,
    eq_rhs::AbstractVector,
    inC::AbstractMatrix,
    in_rhs::AbstractVector,
    parameter_symbols::AbstractVector,
    witness_input_log10::AbstractVector,
)
    n_bg = length(parameter_symbols)
    n_witness = length(witness_input_log10)
    n_witness > 0 || return nothing
    coordinates = vcat(Symbol.(parameter_symbols), [_rop_shape_tau(idx)
                                                     for idx in 0:(n_witness - 1)])
    geometry = ROPShapeOptimization.DesignabilityCellGeometry(
        -Matrix{Float64}(eqC), Vector{Float64}(eq_rhs),
        -Matrix{Float64}(inC), Vector{Float64}(in_rhs);
        coordinates=coordinates,
        parameter_coordinates=Symbol.(parameter_symbols),
        witness_coordinates=coordinates[(n_bg + 1):end],
        equality_row_ids=["legacy_augmented:eq:$idx" for idx in axes(eqC, 1)],
        inequality_row_ids=["legacy_augmented:ineq:$idx" for idx in axes(inC, 1)],
        path_identity="legacy_designability_augmented_cell",
        witness_identity=["legacy_designability:witness:$idx"
                          for idx in 0:(n_witness - 1)],
    )
    fixed_witnesses = ROPShapeOptimization.LinearWitnessConstraint[
        ROPShapeOptimization.LinearWitnessConstraint(
            "conditional_witness:$idx",
            [ROPShapeOptimization.WitnessTerm(_rop_shape_tau(idx), 1.0)],
            :eq,
            Float64(witness_input_log10[idx + 1]),
        ) for idx in 0:(n_witness - 1)
    ]
    objective = ROPShapeOptimization.LinearWitnessObjective(
        "conditional_witness_anchor",
        [ROPShapeOptimization.WitnessTerm(_rop_shape_tau(0), 1.0)];
        sense=:maximize,
    )
    result = ROPShapeOptimization.conditional_parameter_margin(
        geometry, objective, Float64(first(witness_input_log10));
        constraints=fixed_witnesses)
    result.status == ROPShapeOptimization.OPTIMAL || return nothing
    result.solution === nothing && return nothing
    solution = Float64.(result.solution)
    return (
        background_log_qK=solution[1:n_bg],
        parameter_radius=Float64(something(result.parameter_margin)),
        subspace=result.subspace,
        solver_status=result.solver_status,
        message=result.message,
    )
end

function _designability_input_window_tuple(input_window)
    input_window isa AbstractDict || return nothing
    _raw_haskey(input_window, :input_log10) || return nothing
    return _designability_bounds_tuple(_raw_get(input_window, :input_log10, nothing), (-Inf, Inf))
end

function _designability_finite_input_window_tuple(input_window)
    input_window isa AbstractDict || return nothing
    _raw_haskey(input_window, :input_log10) || return nothing
    raw = _raw_get(input_window, :input_log10, nothing)
    (raw isa AbstractVector || raw isa Tuple) || return nothing
    values = collect(raw)
    length(values) == 2 || return nothing
    bounds = Float64[]
    for value in values
        value isa Bool && return nothing
        value isa Real || return nothing
        push!(bounds, Float64(value))
    end
    all(isfinite, bounds) || return nothing
    bounds[1] <= bounds[2] || return nothing
    return (bounds[1], bounds[2])
end

function _designability_window_spacing(input_window)
    input_window isa AbstractDict || return 0.0
    _raw_haskey(input_window, :min_spacing_decades) || return 0.0
    raw = _raw_get(input_window, :min_spacing_decades, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    spacing = Float64(raw)
    isfinite(spacing) && spacing >= 0.0 || return nothing
    return spacing
end

function _designability_operating_points(input_window, witness_count::Integer; input_bounds = nothing,
                                         min_spacing = nothing)
    input_window isa AbstractDict || return nothing
    _raw_haskey(input_window, :operating_points_log10) || return nothing
    raw = _raw_get(input_window, :operating_points_log10, nothing)
    (raw isa AbstractVector || raw isa Tuple) || return nothing
    values = collect(raw)
    length(values) == Int(witness_count) || return nothing
    bounds = input_bounds === nothing ? _designability_finite_input_window_tuple(input_window) : input_bounds
    bounds === nothing && return nothing
    lo, hi = bounds
    isfinite(lo) && isfinite(hi) || return nothing
    spacing = if min_spacing === nothing
        _designability_window_spacing(input_window)
    else
        min_spacing isa Bool && return nothing
        try
            Float64(min_spacing)
        catch
            NaN
        end
    end
    spacing === nothing && return nothing
    isfinite(spacing) && spacing >= 0.0 || return nothing
    points = Float64[]
    for value in values
        value isa Bool && return nothing
        value isa Real || return nothing
        point = Float64(value)
        isfinite(point) || return nothing
        point < lo && return nothing
        point > hi && return nothing
        push!(points, point)
    end
    for idx in 1:(length(points) - 1)
        points[idx + 1] - points[idx] + 1e-9 >= spacing || return nothing
    end
    return points
end

function _designability_dynamic_range_min_log10(dynamic_range)
    dynamic_range isa AbstractDict || return nothing
    _raw_haskey(dynamic_range, :min_fold_change) || return nothing
    raw = _raw_get(dynamic_range, :min_fold_change, 1.0)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    min_fold = Float64(raw)
    isfinite(min_fold) || return nothing
    min_fold < 0.0 && return nothing
    return log10(max(1.0, min_fold))
end

function _designability_sample_points(raw_clause)
    raw_clause isa AbstractDict || return nothing
    _raw_haskey(raw_clause, :sample_points) || return nothing
    raw = _raw_get(raw_clause, :sample_points, nothing)
    raw isa Bool && return nothing
    raw isa Integer || return nothing
    n_points = Int(raw)
    11 <= n_points <= 1001 || return nothing
    return n_points
end

function _designability_dynamic_range_sample_points(dynamic_range)
    return _designability_sample_points(dynamic_range)
end

function _designability_shape_sample_points(shape)
    shape isa AbstractDict || return nothing
    _raw_haskey(shape, :sample_points) || return nothing
    raw = _raw_get(shape, :sample_points, nothing)
    raw isa Bool && return nothing
    n_points = raw isa Integer ? Int(raw) : nothing
    n_points === nothing && return nothing
    11 <= n_points <= 1001 || return nothing
    return n_points
end

function _designability_shape_tolerance_log10(shape)
    shape isa AbstractDict || return nothing
    _raw_haskey(shape, :tolerance_log10) || return nothing
    raw = _raw_get(shape, :tolerance_log10, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    tol = Float64(raw)
    isfinite(tol) && tol >= 0.0 || return nothing
    return tol
end

function _designability_shape_min_prominence_log10(shape)
    shape isa AbstractDict || return nothing
    _raw_haskey(shape, :min_prominence_log10) &&
        _raw_haskey(shape, :min_prominence_decades) &&
        return nothing
    key = _raw_haskey(shape, :min_prominence_log10) ? :min_prominence_log10 :
        (_raw_haskey(shape, :min_prominence_decades) ? :min_prominence_decades : nothing)
    key === nothing && return nothing
    raw = _raw_get(shape, key, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    prominence = Float64(raw)
    isfinite(prominence) && prominence >= 0.0 || return nothing
    return prominence
end

function _designability_shape_class(shape)
    shape isa AbstractDict || return ""
    raw = _raw_get(shape, :class, "")
    return raw isa AbstractString ? String(raw) : ""
end

function _designability_shape_monotonicity(shape)
    shape isa AbstractDict || return ""
    _raw_haskey(shape, :monotonicity) || return ""
    raw = _raw_get(shape, :monotonicity, "")
    return raw isa AbstractString ? String(raw) : ""
end

function _designability_shape_supported(shape)
    shape isa AbstractDict || return false
    _designability_shape_sample_points(shape) !== nothing || return false
    _designability_shape_tolerance_log10(shape) !== nothing || return false
    cls = _designability_shape_class(shape)
    if cls == "monotonic"
        (_raw_haskey(shape, :min_prominence_log10) || _raw_haskey(shape, :min_prominence_decades)) &&
            return false
        return _designability_shape_monotonicity(shape) in ("increasing", "decreasing", "any")
    elseif cls == "bell_shaped"
        _raw_haskey(shape, :min_prominence_log10) &&
            _raw_haskey(shape, :min_prominence_decades) &&
            return false
        return _designability_shape_min_prominence_log10(shape) !== nothing
    end
    return false
end

function _designability_fixed_background_by_symbols(model, qk_symbols::AbstractVector,
                                                    background_logqK::AbstractVector{<:Real},
                                                    input_idx::Integer)
    symbols = Symbol.(string.(qk_symbols))
    values = Dict(sym => Float64(value) for (sym, value) in zip(symbols, background_logqK))
    full_symbols = Symbol.(string.(qK_sym(model)))
    fixed = Float64[]
    for (idx, sym) in enumerate(full_symbols)
        idx == Int(input_idx) && continue
        haskey(values, sym) || return nothing
        value = values[sym]
        isfinite(value) || return nothing
        push!(fixed, value)
    end
    return fixed
end

function _designability_window_sample_points(dynamic_range, output_feature, shape)
    n_points = 0
    dynamic_points = _designability_sample_points(dynamic_range)
    _designability_dynamic_range_min_log10(dynamic_range) !== nothing &&
        dynamic_points !== nothing &&
        (n_points = max(n_points, dynamic_points))
    output_points = _designability_sample_points(output_feature)
    output_feature isa AbstractDict &&
        output_points !== nothing &&
        (n_points = max(n_points, output_points))
    _designability_shape_supported(shape) &&
        (n_points = max(n_points, _designability_shape_sample_points(shape)))
    return n_points
end

function _designability_failed_sample_curve(sample_points::Integer, reason::AbstractString; floor_limited::Bool = false)
    return (;
        valid = false,
        ylog = Float64[],
        ylinear = Float64[],
        log10_range = NaN,
        fold_change = NaN,
        output_min = NaN,
        output_max = NaN,
        sample_points = Int(sample_points),
        floor_limited,
        reason = String(reason),
    )
end

function _designability_sample_window_curve(model, input_idx::Integer, output_idx::Integer,
                                           qk_symbols::AbstractVector,
                                           background_logqK::AbstractVector{<:Real},
                                           window_bounds,
                                           sample_points::Integer)
    lo, hi = window_bounds
    isfinite(lo) && isfinite(hi) ||
        return _designability_failed_sample_curve(0, "sampled output features require finite input_window.input_log10 bounds")
    fixed = _designability_fixed_background_by_symbols(model, qk_symbols, background_logqK, input_idx)
    fixed === nothing &&
        return _designability_failed_sample_curve(0, "background qK symbols do not cover the fixed scan coordinates")
    n_points = clamp(Int(sample_points), 11, 1001)
    param_range = collect(range(Float64(lo), Float64(hi); length = n_points))
    onehot = zeros(Float64, length(x_sym(model)))
    onehot[Int(output_idx)] = 1.0
    try
        _, output_traj, _, valid = scan_parameter_1d(
            model, Int(input_idx), param_range, [onehot], fixed;
            input_logspace = true,
            output_logspace = true,
            track_validity = true,
        )
        ylog = Float64.(vec(output_traj[:, 1]))
        valid_values = valid isa AbstractArray ? valid : [valid]
        all_valid = all(Bool.(valid_values))
        finite_outputs = all(isfinite, ylog)
        floor_limited = finite_outputs && any(y -> y <= -99.999, ylog)
        ylinear = finite_outputs ? exp10.(ylog) : Float64[]
        finite_linear_outputs = !isempty(ylinear) && all(isfinite, ylinear)
        log_range = finite_outputs ? maximum(ylog) - minimum(ylog) : NaN
        fold_change = isfinite(log_range) ? exp10(min(log_range, 308.0)) : NaN
        curve_valid = all_valid && finite_outputs && finite_linear_outputs && !floor_limited
        return (;
            valid = curve_valid,
            ylog,
            ylinear,
            log10_range = Float64(log_range),
            fold_change = Float64(fold_change),
            output_min = finite_linear_outputs ? Float64(minimum(ylinear)) : NaN,
            output_max = finite_linear_outputs ? Float64(maximum(ylinear)) : NaN,
            sample_points = n_points,
            floor_limited,
            reason = curve_valid ? "sampled output curve is finite and valid" : "sampled output curve has invalid, nonfinite, or floor-limited samples",
        )
    catch err
        return _designability_failed_sample_curve(n_points, "sampled output scan failed: $(sprint(showerror, err))")
    end
end

function _designability_evaluate_dynamic_range(sampled_curve, dynamic_range)
    min_log = _designability_dynamic_range_min_log10(dynamic_range)
    min_log === nothing && return nothing
    requested_points = _designability_dynamic_range_sample_points(dynamic_range)
    requested_points === nothing && return (;
        pass = false,
        log10_range = NaN,
        fold_change = NaN,
        sample_points = 0,
        floor_limited = false,
        reason = "dynamic_range sample_points must be an integer from 11 to 1001",
    )
    sampled_curve === nothing && return (;
        pass = false,
        log10_range = NaN,
        fold_change = NaN,
        sample_points = 0,
        floor_limited = false,
        reason = "dynamic_range requires a sampled output curve",
    )
    pass = sampled_curve.valid && sampled_curve.log10_range + 1.0e-9 >= min_log
    return (;
        pass,
        log10_range = Float64(sampled_curve.log10_range),
        fold_change = Float64(sampled_curve.fold_change),
        sample_points = Int(sampled_curve.sample_points),
        floor_limited = Bool(sampled_curve.floor_limited),
        reason = pass ? "sampled dynamic range meets min_fold_change" : "sampled dynamic range failed min_fold_change or scan validity",
    )
end

function _designability_sample_dynamic_range(model, input_idx::Integer, output_idx::Integer,
                                             qk_symbols::AbstractVector,
                                             background_logqK::AbstractVector{<:Real},
                                             window_bounds,
                                             dynamic_range)
    min_log = _designability_dynamic_range_min_log10(dynamic_range)
    min_log === nothing && return nothing
    sample_points = _designability_dynamic_range_sample_points(dynamic_range)
    sample_points === nothing && return nothing
    sampled_curve = _designability_sample_window_curve(
        model, input_idx, output_idx, qk_symbols, background_logqK,
        window_bounds, sample_points,
    )
    return _designability_evaluate_dynamic_range(sampled_curve, dynamic_range)
end

function _designability_output_feature_operator(output_feature)
    raw = _raw_get(output_feature, :operator, "=")
    op = raw isa AbstractString ? String(raw) : ""
    return op
end

function _designability_output_feature_target(output_feature)
    _raw_haskey(output_feature, :value) || return nothing
    raw = _raw_get(output_feature, :value, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    value = Float64(raw)
    isfinite(value) || return nothing
    return value
end

function _designability_nonnegative_float(raw)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    value = Float64(raw)
    isfinite(value) && value >= 0.0 || return nothing
    return value
end

function _designability_output_feature_tolerance(output_feature, target::Real)
    output_feature isa AbstractDict || return nothing
    _raw_haskey(output_feature, :tolerance_log10) || return nothing
    return _designability_nonnegative_float(_raw_get(output_feature, :tolerance_log10, nothing))
end

function _designability_compare_feature_value(measured::Real, op::AbstractString, target::Real, tol::Real)
    m = Float64(measured)
    t = Float64(target)
    isfinite(m) && isfinite(t) || return false
    op == ">=" && return m >= t
    op == "<=" && return m <= t
    op == "=" && return abs(m - t) <= Float64(tol)
    return false
end

function _designability_compare_fold_change_log(log_range::Real, op::AbstractString, target::Real,
                                                tol_log10::Real)
    lr = Float64(log_range)
    t = Float64(target)
    isfinite(lr) && isfinite(t) && t > 0.0 || return false
    target_log = log10(t)
    op == ">=" && return lr + 1.0e-9 >= target_log
    op == "<=" && return lr <= target_log + 1.0e-9
    return op == "=" && abs(lr - target_log) <= max(1.0e-9, Float64(tol_log10))
end

function _designability_unsupported_output_feature(output_feature, sampled_curve, reason::AbstractString)
    feature_raw = output_feature isa AbstractDict ? _raw_get(output_feature, :feature, "") : ""
    feature = feature_raw isa AbstractString ? String(feature_raw) : ""
    op = output_feature isa AbstractDict ? _designability_output_feature_operator(output_feature) : ""
    target = output_feature isa AbstractDict ? _designability_output_feature_target(output_feature) : nothing
    return (;
        pass = false,
        status = "unsupported",
        feature,
        operator = op,
        target = target === nothing ? NaN : Float64(target),
        value = NaN,
        range = Float64[],
        log10_range = sampled_curve === nothing ? NaN : Float64(sampled_curve.log10_range),
        fold_change = sampled_curve === nothing ? NaN : Float64(sampled_curve.fold_change),
        sample_points = sampled_curve === nothing ? 0 : Int(sampled_curve.sample_points),
        floor_limited = sampled_curve === nothing ? false : Bool(sampled_curve.floor_limited),
        reason = String(reason),
    )
end

function _designability_evaluate_output_feature(sampled_curve, output_feature)
    output_feature isa AbstractDict || return nothing
    sampled_curve === nothing && return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "output_feature requires a sampled output curve")
    sample_points = _designability_sample_points(output_feature)
    sample_points === nothing && return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "output_feature sample_points must be an integer from 11 to 1001")
    feature_raw = _raw_get(output_feature, :feature, "")
    feature = feature_raw isa AbstractString ? String(feature_raw) : ""
    op = _designability_output_feature_operator(output_feature)
    target = _designability_output_feature_target(output_feature)
    target === nothing && return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "output_feature requires a finite value")
    feature in ("threshold", "level", "fold_change") || return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "unsupported output_feature feature")
    op in (">=", "<=", "=") || return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "unsupported output_feature operator")
    feature == "fold_change" && target <= 0.0 && return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "fold_change output_feature value must be positive")

    output_range = isfinite(sampled_curve.output_min) && isfinite(sampled_curve.output_max) ?
        Float64[Float64(sampled_curve.output_min), Float64(sampled_curve.output_max)] :
        Float64[]
    measured = if feature == "fold_change"
        Float64(sampled_curve.fold_change)
    elseif op == ">="
        Float64(sampled_curve.output_max)
    elseif op == "<="
        Float64(sampled_curve.output_min)
    else
        if isempty(sampled_curve.ylinear)
            NaN
        else
            distances = abs.(sampled_curve.ylinear .- Float64(target))
            Float64(sampled_curve.ylinear[argmin(distances)])
        end
    end
    tol = _designability_output_feature_tolerance(output_feature, target)
    tol === nothing && return _designability_unsupported_output_feature(
        output_feature, sampled_curve, "output_feature tolerances must be finite nonnegative numbers")
    feature_pass = if feature == "fold_change"
        _designability_compare_fold_change_log(sampled_curve.log10_range, op, target, tol)
    else
        _designability_compare_feature_value(measured, op, target, tol)
    end
    pass = sampled_curve.valid && feature_pass
    return (;
        pass,
        status = pass ? "pass" : "fail",
        feature,
        operator = op,
        target = Float64(target),
        value = measured,
        range = output_range,
        log10_range = Float64(sampled_curve.log10_range),
        fold_change = Float64(sampled_curve.fold_change),
        sample_points = Int(sampled_curve.sample_points),
        floor_limited = Bool(sampled_curve.floor_limited),
        reason = pass ? "sampled output_feature constraint is satisfied" : "sampled output_feature constraint failed",
    )
end

function _designability_monotone_nondec(ylog::AbstractVector{<:Real}, tol::Real)
    length(ylog) <= 1 && return true
    return all(Float64(ylog[i + 1]) - Float64(ylog[i]) >= -Float64(tol) for i in 1:(length(ylog) - 1))
end

function _designability_monotone_noninc(ylog::AbstractVector{<:Real}, tol::Real)
    length(ylog) <= 1 && return true
    return all(Float64(ylog[i + 1]) - Float64(ylog[i]) <= Float64(tol) for i in 1:(length(ylog) - 1))
end

function _designability_unsupported_shape(shape, sampled_curve, reason::AbstractString)
    return (;
        pass = false,
        status = "unsupported",
        class = _designability_shape_class(shape),
        direction = "",
        peak_index = 0,
        sample_points = sampled_curve === nothing ? 0 : Int(sampled_curve.sample_points),
        floor_limited = sampled_curve === nothing ? false : Bool(sampled_curve.floor_limited),
        min_prominence_log10 = NaN,
        prominence_left_log10 = NaN,
        prominence_right_log10 = NaN,
        reason = String(reason),
    )
end

function _designability_failed_shape(shape, sampled_curve, reason::AbstractString;
                                     direction::AbstractString = "",
                                     peak_index::Integer = 0,
                                     min_prominence_log10::Real = NaN,
                                     prominence_left_log10::Real = NaN,
                                     prominence_right_log10::Real = NaN)
    return (;
        pass = false,
        status = "fail",
        class = _designability_shape_class(shape),
        direction = String(direction),
        peak_index = Int(peak_index),
        sample_points = sampled_curve === nothing ? 0 : Int(sampled_curve.sample_points),
        floor_limited = sampled_curve === nothing ? false : Bool(sampled_curve.floor_limited),
        min_prominence_log10 = Float64(min_prominence_log10),
        prominence_left_log10 = Float64(prominence_left_log10),
        prominence_right_log10 = Float64(prominence_right_log10),
        reason = String(reason),
    )
end

function _designability_passed_shape(shape, sampled_curve, reason::AbstractString;
                                     direction::AbstractString = "",
                                     peak_index::Integer = 0,
                                     min_prominence_log10::Real = NaN,
                                     prominence_left_log10::Real = NaN,
                                     prominence_right_log10::Real = NaN)
    return (;
        pass = true,
        status = "pass",
        class = _designability_shape_class(shape),
        direction = String(direction),
        peak_index = Int(peak_index),
        sample_points = Int(sampled_curve.sample_points),
        floor_limited = Bool(sampled_curve.floor_limited),
        min_prominence_log10 = Float64(min_prominence_log10),
        prominence_left_log10 = Float64(prominence_left_log10),
        prominence_right_log10 = Float64(prominence_right_log10),
        reason = String(reason),
    )
end

function _designability_evaluate_shape(sampled_curve, shape)
    shape isa AbstractDict || return nothing
    _designability_shape_supported(shape) || return _designability_unsupported_shape(
        shape, sampled_curve, "unsupported or malformed target shape clause")
    sampled_curve === nothing && return _designability_failed_shape(
        shape, sampled_curve, "shape requires a sampled output curve")
    sampled_curve.valid || return _designability_failed_shape(
        shape, sampled_curve, "sampled output curve is invalid, nonfinite, or floor-limited")

    ylog = Float64.(collect(sampled_curve.ylog))
    isempty(ylog) && return _designability_failed_shape(
        shape, sampled_curve, "shape requires at least one sampled output")
    tol = _designability_shape_tolerance_log10(shape)
    tol === nothing && return _designability_unsupported_shape(
        shape, sampled_curve, "shape tolerance_log10 must be finite and nonnegative")
    cls = _designability_shape_class(shape)
    if cls == "monotonic"
        monotonicity = _designability_shape_monotonicity(shape)
        inc = _designability_monotone_nondec(ylog, tol)
        dec = _designability_monotone_noninc(ylog, tol)
        requested_pass = monotonicity == "increasing" ? inc :
            (monotonicity == "decreasing" ? dec : (inc || dec))
        direction = if monotonicity in ("increasing", "decreasing")
            monotonicity
        elseif inc && dec
            "flat"
        elseif inc
            "increasing"
        elseif dec
            "decreasing"
        else
            ""
        end
        requested_pass || return _designability_failed_shape(
            shape, sampled_curve, "sampled curve does not satisfy monotonic shape"; direction = direction)
        return _designability_passed_shape(
            shape, sampled_curve, "sampled curve satisfies monotonic shape"; direction = direction)
    elseif cls == "bell_shaped"
        min_prominence = _designability_shape_min_prominence_log10(shape)
        min_prominence === nothing && return _designability_unsupported_shape(
            shape, sampled_curve, "bell_shaped target shape requires finite nonnegative min_prominence_log10")
        peak_idx = argmax(ylog)
        peak_log = ylog[peak_idx]
        left_prominence = peak_log - first(ylog)
        right_prominence = peak_log - last(ylog)
        interior_peak = 1 < peak_idx < length(ylog)
        monotone_up = interior_peak && _designability_monotone_nondec(ylog[1:peak_idx], tol)
        monotone_down = interior_peak && _designability_monotone_noninc(ylog[peak_idx:end], tol)
        prominent = left_prominence + 1.0e-9 >= min_prominence &&
            right_prominence + 1.0e-9 >= min_prominence
        pass = interior_peak && prominent && monotone_up && monotone_down
        pass || return _designability_failed_shape(
            shape, sampled_curve, "sampled curve does not satisfy bell_shaped target shape";
            direction = "bell_shaped", peak_index = peak_idx,
            min_prominence_log10 = min_prominence,
            prominence_left_log10 = left_prominence,
            prominence_right_log10 = right_prominence)
        return _designability_passed_shape(
            shape, sampled_curve, "sampled curve satisfies bell_shaped target shape";
            direction = "bell_shaped", peak_index = peak_idx,
            min_prominence_log10 = min_prominence,
            prominence_left_log10 = left_prominence,
            prominence_right_log10 = right_prominence)
    end
    return _designability_unsupported_shape(shape, sampled_curve, "unsupported target shape class")
end

function feasible_region_single_ro(rules::Vector{String}, input_sym, output_sym, target_ro::Real,
                                   parameter_bounds::AbstractDict; tol::Real = 0.05)
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx = locate_sym_qK(model, Symbol(input_sym))
    output_idx = locate_sym_x(model, Symbol(output_sym))
    input_idx === nothing && return FeasibleRegionResult(feasible = false, reason = "unknown input symbol")
    output_idx === nothing && return FeasibleRegionResult(feasible = false, reason = "unknown output symbol")
    input_idx <= model.d || return FeasibleRegionResult(feasible = false, reason = "input symbol must be a total, not a binding constant")

    siso = _bounded_siso_paths(model, Symbol(input_sym))
    bf = get_behavior_families(siso; observe_x = Symbol(output_sym), path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)
    boxC, boxC0 = _designability_qk_box_rows(model, parameter_bounds)
    cells = DesignabilityCellResult[]

    for pr in bf.path_records
        pr.included || continue
        length(pr.vertex_indices) == length(pr.exact_profile) || continue
        for (vertex_idx, ro) in zip(pr.vertex_indices, pr.exact_profile)
            isfinite(ro) || continue
            abs(Float64(ro) - Float64(target_ro)) <= Float64(tol) || continue
            vC, vC0, vnull = get_C_C0_nullity_qK(model, vertex_idx)
            eqC, eqC0, inC, inC0 = _designability_split_eq_ineq(vC, vC0, vnull, model.n)
            poly = get_polyhedron(vcat(eqC, inC, boxC), vcat(eqC0, inC0, boxC0), size(eqC, 1))
            isempty(poly) && continue
            logqK, radius, mode = _placer_most_interior_point(poly; extend = 3)
            kd, totals_sym = _placer_split_log_qK(model, logqK)
            totals = Dict(String(k) => Float64(v) for (k, v) in totals_sym)
            push!(cells, DesignabilityCellResult(
                vertex_idx = Int(vertex_idx),
                predicted_ro = Float64(ro),
                predicted_profile = Float64[Float64(ro)],
                qK_symbols = String.(string.(qK_sym(model))),
                chebyshev_radius = isfinite(radius) ? Float64(radius) : 0.0,
                parameter_chebyshev_radius = isfinite(radius) ? Float64(radius) : 0.0,
                augmented_chebyshev_radius = isfinite(radius) ? Float64(radius) : NaN,
                parameter_margin_dimension = model.n - rank(Matrix{Float64}(eqC)),
                parameter_margin_equality_rank = rank(Matrix{Float64}(eqC)),
                parameter_margin_basis = "log10_qK_euclidean",
                log_qK = Float64.(logqK),
                kd = Float64.(kd),
                totals = totals,
                solve_mode = String(mode),
            ))
        end
    end

    sort!(cells; by = c -> c.chebyshev_radius, rev = true)
    return isempty(cells) ?
        FeasibleRegionResult(feasible = false, reason = "no target RO cell intersects declared qK bounds") :
        FeasibleRegionResult(feasible = true, cells = cells)
end

function feasible_region_reaction_order_program(rules::Vector{String}, input_sym, output_sym,
                                                target_profile::AbstractVector{<:Real},
                                                parameter_bounds::AbstractDict; tol::Real = 0.05,
                                                input_window = nothing,
                                                transition_order = nothing,
                                                dynamic_range = nothing,
                                                output_feature = nothing,
                                                shape = nothing,
                                                max_cells::Union{Nothing, Integer} = nothing)
    target = Float64.(collect(target_profile))
    isempty(target) && error("target reaction-order program must not be empty")
    max_cells !== nothing && Int(max_cells) < 1 && throw(ArgumentError(
        "max_cells must be positive or nothing"))
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    find_all_vertices!(model)
    input_idx = locate_sym_qK(model, Symbol(input_sym))
    output_idx = locate_sym_x(model, Symbol(output_sym))
    input_idx === nothing && return FeasibleRegionResult(
        feasible = false,
        certificate_grade = "exact-union-siso-rop-path",
        reason = "unknown input symbol",
    )
    output_idx === nothing && return FeasibleRegionResult(
        feasible = false,
        certificate_grade = "exact-union-siso-rop-path",
        reason = "unknown output symbol",
    )
    input_idx <= model.d || return FeasibleRegionResult(
        feasible = false,
        certificate_grade = "exact-union-siso-rop-path",
        reason = "input symbol must be a total, not a binding constant",
    )

    siso = _bounded_siso_paths(model, Symbol(input_sym))
    bf = get_behavior_families(siso; observe_x = Symbol(output_sym), path_scope = :feasible,
        deduplicate = false, keep_singular = true, keep_nonasymptotic = true,
        compute_volume = false)
    projected_symbols = qK_sym(siso)
    boxC, boxC0 = _designability_qk_box_rows_for_symbols(model, projected_symbols, parameter_bounds)
    projected_full_indices = _designability_projected_indices_for_siso(model, siso, input_idx)
    window_bounds = _designability_input_window_tuple(input_window)
    window_spacing = _designability_window_spacing(input_window)
    window_spacing === nothing && return FeasibleRegionResult(
        feasible = false,
        certificate_grade = "exact-union-siso-rop-path",
        reason = "invalid input_window.min_spacing_decades",
    )
    operating_points = _designability_operating_points(input_window, length(target))
    operating_points_declared = input_window isa AbstractDict &&
        _raw_haskey(input_window, :operating_points_log10)
    if operating_points_declared && operating_points === nothing
        return FeasibleRegionResult(
            feasible = false,
            certificate_grade = "exact-union-siso-rop-path",
            reason = "invalid finite-window operating_points_log10",
        )
    end
    witness_order = transition_order === nothing ?
        collect(1:length(target)) :
        Int.(collect(transition_order))
    if length(witness_order) != length(target) ||
       any(idx -> idx < 1 || idx > length(target), witness_order) ||
       length(unique(witness_order)) != length(target)
        return FeasibleRegionResult(
            feasible = false,
            certificate_grade = "exact-union-siso-rop-path",
            reason = "invalid transition_order program indices",
        )
    end
    # First identify and count the complete declared cell population without
    # materializing any witness Cartesian product, augmented matrix, polyhedron,
    # or LP. Exact certificates are all-or-nothing: an over-budget candidate is
    # left unevaluated by its caller instead of solving a prefix and calling it
    # exact.
    eligible_paths = NamedTuple[]
    eligible_cell_count = 0
    for pr in bf.path_records
        pr.included || continue
        collapsed = _designability_collapse_profile_vertices(pr.exact_profile, pr.vertex_indices, target; tol = tol)
        collapsed === nothing && continue
        profile, vertex_choices = collapsed
        path_cell_count = window_bounds === nothing ? 1 :
            _designability_bounded_product_count(
                length.(vertex_choices);
                limit=max_cells === nothing ? typemax(Int) : Int(max_cells),
            )
        eligible_cell_count = _designability_add_cell_count(
            eligible_cell_count,
            path_cell_count,
            max_cells,
        )
        push!(eligible_paths, (; pr, profile, vertex_choices))
    end

    cells = DesignabilityCellResult[]
    for entry in eligible_paths
        pr = entry.pr
        profile = entry.profile
        vertex_choices = entry.vertex_choices
        if window_bounds !== nothing
            pEqC, pEqC0, pInC, pInC0 = _designability_path_projected_rows(siso, pr.path_idx, length(projected_symbols))
            n_bg = length(projected_symbols)
            n_witness = length(vertex_choices)
            n_aug = n_bg + n_witness
            for witness_vertices_tuple in _designability_vertex_choice_products(vertex_choices)
                witness_vertices = Int.(collect(witness_vertices_tuple))
                eq_rows = Vector{Vector{Float64}}()
                eq_rhs = Float64[]
                in_rows = Vector{Vector{Float64}}()
                in_rhs = Float64[]
                _designability_push_augmented_rows!(eq_rows, eq_rhs, pEqC, pEqC0, n_aug)
                _designability_push_augmented_rows!(in_rows, in_rhs, pInC, pInC0, n_aug)
                _designability_push_augmented_rows!(in_rows, in_rhs, boxC, boxC0, n_aug)
                lo, hi = window_bounds
                for witness_idx in 1:n_witness
                    _designability_push_augmented_vertex_rows!(
                        eq_rows, eq_rhs, in_rows, in_rhs,
                        model, witness_vertices[witness_idx], input_idx,
                        projected_full_indices, witness_idx, n_witness,
                    )
                    t_col = n_bg + witness_idx
                    if isfinite(hi)
                        row = zeros(Float64, n_aug); row[t_col] = -1.0
                        push!(in_rows, row); push!(in_rhs, hi)
                    end
                    if isfinite(lo)
                        row = zeros(Float64, n_aug); row[t_col] = 1.0
                        push!(in_rows, row); push!(in_rhs, -lo)
                    end
                end
                for order_idx in 1:(length(witness_order) - 1)
                    prev = witness_order[order_idx]
                    next = witness_order[order_idx + 1]
                    row = zeros(Float64, n_aug)
                    row[n_bg + prev] = -1.0
                    row[n_bg + next] = 1.0
                    push!(in_rows, row)
                    push!(in_rhs, -window_spacing)
                end
                if operating_points !== nothing
                    for witness_idx in 1:n_witness
                        row = zeros(Float64, n_aug)
                        row[n_bg + witness_idx] = 1.0
                        push!(eq_rows, row)
                        push!(eq_rhs, -operating_points[witness_idx])
                    end
                end
                eqC = _designability_rows_matrix(eq_rows, n_aug)
                inC = _designability_rows_matrix(in_rows, n_aug)
                poly = get_polyhedron(vcat(eqC, inC), vcat(eq_rhs, in_rhs), size(eqC, 1))
                isempty(poly) && continue
                log_aug, radius, mode = _placer_most_interior_point(poly; extend = 3)
                augmented_radius = isfinite(radius) ? Float64(radius) : NaN
                background_logqK = Float64.(log_aug[1:n_bg])
                witness_log_input = Float64.(log_aug[(n_bg + 1):end])
                conditional_margin = _designability_conditional_parameter_margin(
                    eqC, eq_rhs, inC, in_rhs, projected_symbols, witness_log_input)
                parameter_radius = conditional_margin === nothing ? 0.0 :
                    conditional_margin.parameter_radius
                if conditional_margin !== nothing
                    background_logqK = conditional_margin.background_log_qK
                    mode = :conditional_parameter_chebyshev
                end
                sample_points = _designability_window_sample_points(dynamic_range, output_feature, shape)
                sampled_curve = sample_points > 0 ? _designability_sample_window_curve(
                    model, input_idx, output_idx, projected_symbols, background_logqK,
                    window_bounds, sample_points,
                ) : nothing
                sampled_dynamic = _designability_evaluate_dynamic_range(sampled_curve, dynamic_range)
                sampled_output_feature = _designability_evaluate_output_feature(sampled_curve, output_feature)
                sampled_shape = _designability_evaluate_shape(sampled_curve, shape)
                sampled_dynamic !== nothing && !sampled_dynamic.pass && continue
                sampled_output_feature !== nothing && !sampled_output_feature.pass && continue
                sampled_shape !== nothing && !sampled_shape.pass && continue
                kd, totals = _designability_split_projected_log_qK(model, projected_symbols, background_logqK)
                vertex_indices = Int.(collect(pr.vertex_indices))
                push!(cells, DesignabilityCellResult(
                    vertex_idx = isempty(witness_vertices) ? 0 : first(witness_vertices),
                    predicted_ro = isempty(profile) ? NaN : first(profile),
                    path_idx = Int(pr.path_idx),
                    vertex_indices = vertex_indices,
                    witness_vertex_indices = witness_vertices,
                    predicted_profile = Float64.(profile),
                    qK_symbols = String.(string.(projected_symbols)),
                    witness_input_log10 = witness_log_input,
                    sampled_dynamic_range_log10 = sampled_dynamic === nothing ? NaN : sampled_dynamic.log10_range,
                    sampled_dynamic_range_fold_change = sampled_dynamic === nothing ? NaN : sampled_dynamic.fold_change,
                    sampled_dynamic_range_points = sampled_dynamic === nothing ? 0 : sampled_dynamic.sample_points,
                    sampled_dynamic_range_floor_limited = sampled_dynamic === nothing ? false : sampled_dynamic.floor_limited,
                    sampled_output_feature_status = sampled_output_feature === nothing ? "" : sampled_output_feature.status,
                    sampled_output_feature_name = sampled_output_feature === nothing ? "" : sampled_output_feature.feature,
                    sampled_output_feature_operator = sampled_output_feature === nothing ? "" : sampled_output_feature.operator,
                    sampled_output_feature_target = sampled_output_feature === nothing ? NaN : sampled_output_feature.target,
                    sampled_output_feature_value = sampled_output_feature === nothing ? NaN : sampled_output_feature.value,
                    sampled_output_feature_range = sampled_output_feature === nothing ? Float64[] : Float64.(sampled_output_feature.range),
                    sampled_output_feature_log10_range = sampled_output_feature === nothing ? NaN : sampled_output_feature.log10_range,
                    sampled_output_feature_fold_change = sampled_output_feature === nothing ? NaN : sampled_output_feature.fold_change,
                    sampled_output_feature_sample_points = sampled_output_feature === nothing ? 0 : sampled_output_feature.sample_points,
                    sampled_output_feature_floor_limited = sampled_output_feature === nothing ? false : sampled_output_feature.floor_limited,
                    sampled_shape_status = sampled_shape === nothing ? "" : sampled_shape.status,
                    sampled_shape_class = sampled_shape === nothing ? "" : sampled_shape.class,
                    sampled_shape_direction = sampled_shape === nothing ? "" : sampled_shape.direction,
                    sampled_shape_peak_index = sampled_shape === nothing ? 0 : sampled_shape.peak_index,
                    sampled_shape_sample_points = sampled_shape === nothing ? 0 : sampled_shape.sample_points,
                    sampled_shape_floor_limited = sampled_shape === nothing ? false : sampled_shape.floor_limited,
                    sampled_shape_min_prominence_log10 = sampled_shape === nothing ? NaN : sampled_shape.min_prominence_log10,
                    sampled_shape_prominence_left_log10 = sampled_shape === nothing ? NaN : sampled_shape.prominence_left_log10,
                    sampled_shape_prominence_right_log10 = sampled_shape === nothing ? NaN : sampled_shape.prominence_right_log10,
                    chebyshev_radius = parameter_radius,
                    parameter_chebyshev_radius = parameter_radius,
                    augmented_chebyshev_radius = augmented_radius,
                    parameter_margin_dimension = conditional_margin === nothing ? 0 :
                        conditional_margin.subspace.dimension,
                    parameter_margin_equality_rank = conditional_margin === nothing ? 0 :
                        conditional_margin.subspace.equality_rank,
                    parameter_margin_basis =
                        "equality_feasible_log10_qK_subspace",
                    log_qK = background_logqK,
                    kd = Float64.(kd),
                    totals = totals,
                    solve_mode = String(mode),
                ))
            end
            continue
        end
        pC, pC0, pnull = try
            get_C_C0_nullity_qK(siso, pr.path_idx)
        catch
            poly = get_polyhedron(siso, pr.path_idx)
            get_C_C0_nullity(poly)
        end
        eqC, eqC0, inC, inC0 = _designability_split_eq_ineq(pC, pC0, pnull, length(projected_symbols))
        poly = get_polyhedron(vcat(eqC, inC, boxC), vcat(eqC0, inC0, boxC0), size(eqC, 1))
        isempty(poly) && continue
        logqK, radius, mode = _placer_most_interior_point(poly; extend = 3)
        kd, totals = _designability_split_projected_log_qK(model, projected_symbols, logqK)
        vertex_indices = Int.(collect(pr.vertex_indices))
        push!(cells, DesignabilityCellResult(
            vertex_idx = isempty(vertex_indices) ? 0 : first(vertex_indices),
            predicted_ro = isempty(profile) ? NaN : first(profile),
            path_idx = Int(pr.path_idx),
            vertex_indices = vertex_indices,
            predicted_profile = profile,
            qK_symbols = String.(string.(projected_symbols)),
            chebyshev_radius = isfinite(radius) ? Float64(radius) : 0.0,
            parameter_chebyshev_radius = isfinite(radius) ? Float64(radius) : 0.0,
            augmented_chebyshev_radius = isfinite(radius) ? Float64(radius) : NaN,
            parameter_margin_dimension = length(projected_symbols) -
                rank(Matrix{Float64}(eqC)),
            parameter_margin_equality_rank = rank(Matrix{Float64}(eqC)),
            parameter_margin_basis = "log10_qK_euclidean",
            log_qK = Float64.(logqK),
            kd = Float64.(kd),
            totals = totals,
            solve_mode = String(mode),
        ))
    end

    sort!(cells; by = c -> c.chebyshev_radius, rev = true)
    return isempty(cells) ?
        FeasibleRegionResult(
            feasible = false,
            certificate_grade = "exact-union-siso-rop-path",
            reason = "no target RO path polyhedron intersects declared qK bounds",
        ) :
        FeasibleRegionResult(
            feasible = true,
            certificate_grade = "exact-union-siso-rop-path",
            cells = cells,
        )
end
