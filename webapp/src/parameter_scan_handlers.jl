# ─── New API handlers for parameter scanning ───
function handle_parameter_scan_1d(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

    # Parse parameters
    param_symbol = Symbol(body[:param_symbol])
    param_idx = locate_sym_qK(model, param_symbol)
    param_idx === nothing && return error_response("Unknown parameter: $param_symbol"; status=400)

    param_min = Float64(get(body, :param_min, -6.0))
    param_max = Float64(get(body, :param_max, 6.0))
    n_points = clamp(Int(get(body, :n_points, 200)), 10, 1000)

    # Parse output expressions
    output_exprs_raw = body[:output_exprs]
    # Normalize to array if single string provided
    output_exprs = if output_exprs_raw isa String
        [String(output_exprs_raw)]
    else
        String.(output_exprs_raw)
    end
    isempty(output_exprs) && return error_response("At least one output expression required"; status=400)

    output_coeffs = Vector{Vector{Float64}}()
    for expr in output_exprs
        try
            coeffs = parse_linear_combination(model, expr)
            push!(output_coeffs, coeffs)
        catch e
            return error_response("Invalid expression '$expr': $(sprint(showerror, e))"; status=400)
        end
    end

    # Fixed parameters (full log qK). If omitted, keep imported/session Kd values.
    fixed_qK = try
        fixed_qK_or_default(body, model, Float64.(bundle["kd"]))
    catch e
        return error_response(sprint(showerror, e); status=400)
    end

    # Remove scanned parameter from fixed_qK
    fixed_params = deleteat!(copy(fixed_qK), param_idx)

    # Scan
    param_range = range(param_min, param_max, length=n_points) |> collect
    param_vals, output_traj, regimes = scan_parameter_1d(
        model, param_idx, param_range, output_coeffs, fixed_params;
        input_logspace=true, output_logspace=true
    )

    return json_response(Dict(
        "param_symbol" => string(param_symbol),
        "param_values" => param_vals,
        "output_exprs" => output_exprs,
        "output_traj" => mat2vv(output_traj),
        "regimes" => regimes,
        "x_sym" => string.(model.x_sym),
        "fixed_qK" => collect(fixed_qK),
    ))
end

function handle_parameter_scan_2d(req)
    body = read_json(req)
    bundle, err = _resolve_bundle_or_response(body)
    err === nothing || return err

    model = bundle["model"]

    # Parse parameters
    param1_symbol = Symbol(body[:param1_symbol])
    param2_symbol = Symbol(body[:param2_symbol])
    param1_idx = locate_sym_qK(model, param1_symbol)
    param2_idx = locate_sym_qK(model, param2_symbol)
    param1_idx === nothing && return error_response("Unknown parameter: $param1_symbol"; status=400)
    param2_idx === nothing && return error_response("Unknown parameter: $param2_symbol"; status=400)
    param1_idx == param2_idx && return error_response("Parameters must be different"; status=400)

    param1_min = Float64(get(body, :param1_min, -6.0))
    param1_max = Float64(get(body, :param1_max, 6.0))
    param2_min = Float64(get(body, :param2_min, -6.0))
    param2_max = Float64(get(body, :param2_max, 6.0))
    n_grid = clamp(Int(get(body, :n_grid, 80)), 20, 200)

    # Parse output expression (single output for heatmap)
    output_expr = String(body[:output_expr])
    output_coeffs = try
        parse_linear_combination(model, output_expr)
    catch e
        return error_response("Invalid expression '$output_expr': $(sprint(showerror, e))"; status=400)
    end

    # Fixed parameters (full log qK). If omitted, keep imported/session Kd values.
    fixed_qK = try
        fixed_qK_or_default(body, model, Float64.(bundle["kd"]))
    catch e
        return error_response(sprint(showerror, e); status=400)
    end

    # Remove both scanned parameters (in descending order to avoid index shifts)
    indices_to_remove = sort([param1_idx, param2_idx], rev=true)
    fixed_params = copy(fixed_qK)
    for idx in indices_to_remove
        deleteat!(fixed_params, idx)
    end

    # Scan
    param1_range = range(param1_min, param1_max, length=n_grid) |> collect
    param2_range = range(param2_min, param2_max, length=n_grid) |> collect

    param1_vals, param2_vals, output_grid, regime_grid = scan_parameter_2d(
        model, param1_idx, param2_idx, param1_range, param2_range,
        output_coeffs, fixed_params;
        input_logspace=true, output_logspace=true
    )

    return json_response(Dict(
        "param1_symbol" => string(param1_symbol),
        "param2_symbol" => string(param2_symbol),
        "param1_values" => param1_vals,
        "param2_values" => param2_vals,
        "output_expr" => output_expr,
        "output_grid" => mat2vv(output_grid),
        "regime_grid" => mat2vv(regime_grid),
        "fixed_qK" => collect(fixed_qK),
    ))
end

function _atlas_landscape_model_from_spec(body)
    # Prefer a resolvable model bundle (network / hash / session); fall back to a
    # raw {reactions, kd} build for fully stateless landscape calls.
    if _raw_haskey(body, :network) || _raw_haskey(body, :network_ir_hash) || _raw_haskey(body, :session_id)
        bundle = resolve_model_bundle(body)
        return (
            model = bundle["model"],
            rules = String.(bundle["rules"]),
            kd = Float64.(bundle["kd"]),
        )
    end

    _raw_haskey(body, :reactions) || error("Landscape scan requires `network`, `session_id`, or `reactions`.")
    rules = String.(collect(_raw_get(body, :reactions, String[])))
    isempty(rules) && error("At least one reaction is required for a landscape scan.")
    kd = if _raw_haskey(body, :kd)
        Float64.(collect(_raw_get(body, :kd, Float64[])))
    else
        ones(Float64, length(rules))
    end
    length(kd) == length(rules) || error("Length of `kd` must match `reactions`.")
    any(x -> x <= 0, kd) && error("All Kd values must be positive (> 0).")
    model, _, _, _ = build_model(rules, kd)
    return (model=model, rules=rules, kd=kd)
end

function _atlas_landscape_param_options(model)
    total_syms = string.(q_sym(model))
    all_syms = string.(qK_sym(model))
    extras = [sym for sym in all_syms if sym ∉ total_syms]
    return vcat(total_syms, extras)
end

function _atlas_landscape_pick_params(model, body)
    options = _atlas_landscape_param_options(model)
    length(options) >= 2 || error("2D landscape requires at least two q/K coordinates.")

    preferred = if _raw_haskey(body, :preferred_param_symbols)
        [String(sym) for sym in collect(_raw_get(body, :preferred_param_symbols, String[])) if !isempty(strip(String(sym)))]
    else
        String[]
    end

    selected = String[]
    seen = Set{String}()
    for sym in preferred
        sym in options || continue
        sym in seen && continue
        push!(selected, sym)
        push!(seen, sym)
        length(selected) == 2 && break
    end

    if _raw_haskey(body, :param1_symbol)
        sym = String(_raw_get(body, :param1_symbol, ""))
        sym in options || error("Unknown parameter: $(sym)")
        empty!(selected)
        empty!(seen)
        push!(selected, sym)
        push!(seen, sym)
    end
    if _raw_haskey(body, :param2_symbol)
        sym = String(_raw_get(body, :param2_symbol, ""))
        sym in options || error("Unknown parameter: $(sym)")
        sym in seen && error("Parameters must be different.")
        push!(selected, sym)
        push!(seen, sym)
    end

    for sym in options
        sym in seen && continue
        push!(selected, sym)
        push!(seen, sym)
        length(selected) == 2 && break
    end

    length(selected) >= 2 || error("Could not resolve two parameters for the landscape scan.")
    return selected[1], selected[2], options
end

function atlas_landscape_2d_from_spec(body)
    ctx = _atlas_landscape_model_from_spec(body)
    model = ctx.model

    param1_symbol, param2_symbol, param_symbol_options = _atlas_landscape_pick_params(model, body)
    param1_idx = locate_sym_qK(model, Symbol(param1_symbol))
    param2_idx = locate_sym_qK(model, Symbol(param2_symbol))
    (param1_idx === nothing || param2_idx === nothing) && error("Could not locate requested parameters in the model.")
    param1_idx == param2_idx && error("Parameters must be different.")

    output_symbol_options = string.(model.x_sym)
    default_output = isempty(output_symbol_options) ? "" : output_symbol_options[1]
    output_expr = if _raw_haskey(body, :output_expr)
        String(_raw_get(body, :output_expr, default_output))
    elseif _raw_haskey(body, :output_symbol)
        String(_raw_get(body, :output_symbol, default_output))
    else
        default_output
    end
    isempty(strip(output_expr)) && error("Landscape scan requires an output expression.")
    output_coeffs = parse_linear_combination(model, output_expr)

    param1_min = Float64(_raw_get(body, :param1_min, -4.0))
    param1_max = Float64(_raw_get(body, :param1_max, 4.0))
    param2_min = Float64(_raw_get(body, :param2_min, -4.0))
    param2_max = Float64(_raw_get(body, :param2_max, 4.0))
    param1_max > param1_min || error("param1_max must be greater than param1_min.")
    param2_max > param2_min || error("param2_max must be greater than param2_min.")
    n_grid = clamp(Int(_raw_get(body, :n_grid, 72)), 20, 160)

    fixed_qK = _fixed_qK_or_default_raw(body, model, ctx.kd)

    indices_to_remove = sort([param1_idx, param2_idx], rev=true)
    fixed_params = copy(fixed_qK)
    for idx in indices_to_remove
        deleteat!(fixed_params, idx)
    end

    param1_range = collect(range(param1_min, param1_max, length=n_grid))
    param2_range = collect(range(param2_min, param2_max, length=n_grid))
    param1_vals, param2_vals, output_grid, regime_grid = scan_parameter_2d(
        model,
        param1_idx,
        param2_idx,
        param1_range,
        param2_range,
        output_coeffs,
        fixed_params;
        input_logspace=true,
        output_logspace=true,
    )
    bounds = find_bounds(regime_grid)

    return Dict(
        "param1_symbol" => param1_symbol,
        "param2_symbol" => param2_symbol,
        "param1_values" => param1_vals,
        "param2_values" => param2_vals,
        "param_symbol_options" => param_symbol_options,
        "output_expr" => output_expr,
        "output_symbol_options" => output_symbol_options,
        "output_grid" => mat2vv(output_grid),
        "regime_grid" => mat2vv(regime_grid),
        "bounds" => mat2vv(Float64.(bounds)),
        "fixed_qK" => collect(fixed_qK),
        "q_sym" => string.(q_sym(model)),
        "K_sym" => string.(K_sym(model)),
        "x_sym" => string.(model.x_sym),
        "rules" => ctx.rules,
        "kd" => collect(ctx.kd),
        "n_grid" => n_grid,
    )
end

function handle_atlas_landscape_2d(req)
    body = read_json(req)
    try
        return json_response(atlas_landscape_2d_from_spec(body))
    catch e
        return error_response(sprint(showerror, e); status=400)
    end
end
