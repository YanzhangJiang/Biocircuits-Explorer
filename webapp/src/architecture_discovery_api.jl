function _ad_real(raw, name::AbstractString, default; nonnegative::Bool=false)
    value = isnothing(raw) ? Float64(default) : _request_finite_real(raw, name)
    nonnegative && value < 0 && throw(ArgumentError("$name must be non-negative"))
    return value
end

function _ad_integer(raw, name::AbstractString, default; nonnegative::Bool=false)
    value = isnothing(raw) ? Int(default) : begin
        raw isa Integer && !(raw isa Bool) || throw(ArgumentError("$name must be an integer"))
        Int(raw)
    end
    valid = nonnegative ? value >= 0 : value > 0
    valid || throw(ArgumentError(
        "$name must be $(nonnegative ? "non-negative" : "positive")"))
    return value
end

function _ad_string_list(raw, name::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$name must be an array of strings"))
    values = String[]
    for item in raw
        push!(values, _request_string(item, name))
    end
    isempty(values) && throw(ArgumentError("$name must not be empty"))
    return values
end

function _ad_kd_list(raw, name::AbstractString, reaction_count::Int)
    raw isa AbstractVector || throw(ArgumentError("$name must be an array"))
    values = [_request_finite_real(value, name) for value in raw]
    length(values) == reaction_count || throw(ArgumentError(
        "$name must have one value per reaction"))
    all(>(0), values) || throw(ArgumentError("$name values must be positive"))
    return values
end

function _ad_output_expressions(body)
    raw = _raw_get(body, :output_exprs, nothing)
    raw isa AbstractString && return [_request_string(raw, "output_exprs")]
    return _ad_string_list(raw, "output_exprs")
end

function architecture_discovery_from_spec(body)
    rules = _ad_string_list(_raw_get(body, :reactions, nothing), "reactions")
    initial_kd = if _raw_haskey(body, :initial_kd)
        _ad_kd_list(_raw_get(body, :initial_kd, nothing), "initial_kd", length(rules))
    else
        nothing
    end
    simulation_kd = if _raw_haskey(body, :simulation_kd)
        _ad_kd_list(
            _raw_get(body, :simulation_kd, nothing), "simulation_kd", length(rules))
    else
        nothing
    end
    simulation_noise = _ad_real(
        _raw_get(body, :simulation_noise, nothing), "simulation_noise", 0.0;
        nonnegative=true,
    )

    model_kd = isnothing(initial_kd) ? ones(Float64, length(rules)) : initial_kd
    model, species, free_species, product_species = build_model(rules, model_kd)
    output_exprs = _ad_output_expressions(body)
    output_matrix = zeros(Float64, length(output_exprs), model.n)
    for (index, expression) in enumerate(output_exprs)
        output_matrix[index, :] .= parse_linear_combination(model, expression)
    end

    raw_samples = _raw_get(body, :samples, nothing)
    raw_samples isa AbstractVector || throw(ArgumentError("samples must be an array"))
    isempty(raw_samples) && throw(ArgumentError("samples must not be empty"))
    totals = zeros(Float64, length(raw_samples), model.d)
    targets = zeros(Float64, length(raw_samples), length(output_exprs))
    total_names = string.(model.q_sym)

    for (sample_index, sample) in enumerate(raw_samples)
        sample isa AbstractDict || sample isa JSON3.Object || throw(ArgumentError(
            "samples[$sample_index] must be an object"))
        raw_totals = _raw_get(sample, :totals, nothing)
        raw_totals isa AbstractDict || raw_totals isa JSON3.Object || throw(ArgumentError(
            "samples[$sample_index].totals must be an object"))
        for (total_index, total_name) in enumerate(total_names)
            raw_value = _raw_get(raw_totals, Symbol(total_name), nothing)
            isnothing(raw_value) && throw(ArgumentError(
                "samples[$sample_index].totals is missing $total_name"))
            value = _request_finite_real(raw_value, "samples[$sample_index].totals.$total_name")
            value > 0 || throw(ArgumentError(
                "samples[$sample_index].totals.$total_name must be positive"))
            totals[sample_index, total_index] = value
        end

        if isnothing(simulation_kd)
            raw_target = _raw_get(sample, :target, nothing)
            target_values = if raw_target isa Real && !(raw_target isa Bool)
                [_request_finite_real(raw_target, "samples[$sample_index].target")]
            elseif raw_target isa AbstractVector
                [_request_finite_real(value, "samples[$sample_index].target") for value in raw_target]
            else
                throw(ArgumentError("samples[$sample_index].target must be a number or array"))
            end
            length(target_values) == length(output_exprs) || throw(ArgumentError(
                "samples[$sample_index].target must have one value per output expression"))
            targets[sample_index, :] .= target_values
        end
    end

    if !isnothing(simulation_kd)
        # Demo mode uses the hidden parameters only to create observations.
        # discover_architecture below still starts from its ordinary defaults.
        targets .= architecture_loss_gradient(
            model,
            totals,
            targets,
            output_matrix,
            log10.(simulation_kd),
        ).predictions
        if simulation_noise > 0
            rng = MersenneTwister(20260905)
            targets .*= exp.(simulation_noise .* randn(rng, size(targets)))
        end
    end

    fit = discover_architecture(
        model,
        totals,
        targets,
        output_matrix;
        initial_kd=initial_kd,
        sparsity=_ad_real(_raw_get(body, :lambda, nothing), "lambda", 1e-3; nonnegative=true),
        learning_rate=_ad_real(_raw_get(body, :learning_rate, nothing), "learning_rate", 0.03),
        epochs=_ad_integer(_raw_get(body, :epochs, nothing), "epochs", 250),
        active_threshold=_ad_real(
            _raw_get(body, :active_threshold, nothing), "active_threshold", 0.05;
            nonnegative=true,
        ),
        debias_epochs=_ad_integer(
            _raw_get(body, :debias_epochs, nothing), "debias_epochs", 60;
            nonnegative=true,
        ),
    )

    active_indices = findall(fit.active)
    reaction_fit = [
        Dict(
            "rule" => rules[index],
            "active" => fit.active[index],
            "affinity" => fit.affinity[index],
            "selection_kd" => fit.selection_kd[index],
            "kd" => fit.kd[index],
        )
        for index in eachindex(rules)
    ]

    result = Dict(
        "status" => isempty(active_indices) ? "no_active_reactions" : "ok",
        "rules" => rules[active_indices],
        "kd" => fit.kd[active_indices],
        "reaction_fit" => reaction_fit,
        "predictions" => mat2vv(fit.predictions),
        "targets" => mat2vv(fit.targets),
        "fit_loss" => fit.fit_loss,
        "sparse_fit_loss" => fit.sparse_fit_loss,
        "sparse_objective" => fit.sparse_objective,
        "loss_history" => fit.loss_history,
        "q_sym" => total_names,
        "x_sym" => string.(model.x_sym),
        "output_exprs" => output_exprs,
        "species" => string.(species),
        "free_species" => string.(free_species),
        "product_species" => string.(product_species),
    )
    if !isnothing(simulation_kd)
        result["simulation"] = Dict(
            "kd" => simulation_kd,
            "noise_log_std" => simulation_noise,
        )
    end
    return result
end

function handle_discover_architecture(req)
    body = read_json(req)
    try
        return json_response(architecture_discovery_from_spec(body))
    catch err
        if err isa ArgumentError || err isa DimensionMismatch || err isa ErrorException
            return error_response(sprint(showerror, err); status=400)
        end
        rethrow()
    end
end
