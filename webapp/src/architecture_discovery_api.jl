const MAX_ARCHITECTURE_SAMPLES = 256
const MAX_ARCHITECTURE_EPOCHS = 2000
const MAX_ARCHITECTURE_DEBIAS_EPOCHS = 500

function _ad_real(raw, name::AbstractString, default; nonnegative::Bool=false)
    value = isnothing(raw) ? Float64(default) : _request_finite_real(raw, name)
    nonnegative && value < 0 && throw(ArgumentError("$name must be non-negative"))
    return value
end

function _ad_integer(raw, name::AbstractString, default; nonnegative::Bool=false)
    value = isnothing(raw) ? Int(default) : begin
        raw isa Integer && !(raw isa Bool) || throw(ArgumentError("$name must be an integer"))
        try
            Int(raw)
        catch
            throw(ArgumentError("$name is outside the supported integer range"))
        end
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

# Keep the exact coefficient while producing syntax accepted by the engine's
# linear-expression parser (which does not accept a minus in an exponent).
function _ad_decimal_coefficient(value::Real)
    raw = string(abs(Float64(value)))
    occursin('e', raw) || return raw
    mantissa, exponent = split(raw, 'e')
    parts = split(mantissa, '.')
    digits = join(parts)
    position = length(first(parts)) + parse(Int, exponent)
    position <= 0 && return "0." * repeat("0", -position) * digits
    position >= length(digits) && return digits * repeat("0", position - length(digits))
    return digits[1:position] * "." * digits[position + 1:end]
end

function _ad_projected_expression(coefficients, species)
    terms = String[]
    for (coefficient, name) in zip(coefficients, species)
        iszero(coefficient) && continue
        sign = coefficient < 0 ? "-" : isempty(terms) ? "" : "+"
        push!(terms, sign * _ad_decimal_coefficient(coefficient) * "*" * string(name))
    end
    return isempty(terms) ? "0*" * string(first(species)) : join(terms, " ")
end

"""Replay the actually pruned network; weak inactive interactions are not its evidence."""
function _ad_selected_network(rules, kd, original_model, totals, targets, outputs)
    isempty(rules) && return Dict{String, Any}(
        "status" => "no_active_reactions",
        "reason" => "No reaction survived the affinity threshold.",
        "rules" => String[], "kd" => Float64[],
    )
    try
        model, _, _, _ = build_model(rules, kd)
        original_q = Dict(name => index for (index, name) in enumerate(original_model.q_sym))
        missing_totals = [string(name) for name in model.q_sym if !haskey(original_q, name)]
        isempty(missing_totals) || throw(ArgumentError(
            "Pruning introduces conserved totals without target samples: " * join(missing_totals, ", ")))
        original_x = Dict(name => index for (index, name) in enumerate(original_model.x_sym))
        # Unused bound products disappear with their reactions. An unused free
        # species remains present at its conserved total, however; replacing an
        # observed free species with zero would silently change the objective.
        retained_species = Set(string.(model.x_sym))
        removed_free_readouts = [string(name) for name in original_model.x_sym[1:original_model.d]
            if !(string(name) in retained_species) && any(!iszero, @view(outputs[:, original_x[name]]))]
        isempty(removed_free_readouts) || throw(ArgumentError(
            "Pruning removes observed free species whose concentrations cannot be dropped: " *
            join(removed_free_readouts, ", ")))
        projected_outputs = outputs[:, [original_x[name] for name in model.x_sym]]
        projected_totals = totals[:, [original_q[name] for name in model.q_sym]]
        replay = architecture_loss_gradient(
            model, projected_totals, targets, projected_outputs, log10.(kd))
        all(isfinite, replay.predictions) && isfinite(replay.data_loss) ||
            error("Selected-network replay returned non-finite evidence")
        output_exprs = [_ad_projected_expression(row, model.x_sym)
                        for row in eachrow(projected_outputs)]
        network = network_ir_from_legacy(rules, kd; label="Gradient inverse design")
        return Dict{String, Any}(
            "status" => "ok", "rules" => rules, "kd" => kd,
            "network_ir" => network_ir_to_dict(network),
            "network_ir_hash" => network_ir_hash(network),
            "predictions" => mat2vv(replay.predictions),
            "targets" => mat2vv(targets), "fit_loss" => replay.data_loss,
            "output_exprs" => output_exprs,
            "output_coefficients" => mat2vv(projected_outputs),
            "q_sym" => string.(model.q_sym), "x_sym" => string.(model.x_sym),
            "removed_species" => string.(setdiff(original_model.x_sym, model.x_sym)),
            "prediction_basis" => "selected_network",
            "evidence_tier" => "sampled_equilibrium_replay",
        )
    catch err
        (err isa ArgumentError || err isa DimensionMismatch || err isa ErrorException ||
         err isa LinearAlgebra.SingularException || err isa IRValidationError) || rethrow()
        return Dict{String, Any}(
            "status" => "invalid", "reason" => sprint(showerror, err),
            "rules" => rules, "kd" => kd,
        )
    end
end

function architecture_discovery_from_spec(body)
    rules = _ad_string_list(_raw_get(body, :reactions, nothing), "reactions")
    any(ncodeunits(rule) > MAX_SYNC_EXPRESSION_BYTES for rule in rules) &&
        _sync_budget_exceeded("A candidate reaction exceeds $(MAX_SYNC_EXPRESSION_BYTES) bytes.")
    enforce_sync_rule_budget(rules)
    output_exprs = _ad_output_expressions(body)
    length(output_exprs) <= MAX_SYNC_SCAN_OUTPUTS ||
        _sync_budget_exceeded("output_exprs exceeds the synchronous limit of $(MAX_SYNC_SCAN_OUTPUTS).")
    any(ncodeunits(expr) > MAX_SYNC_EXPRESSION_BYTES for expr in output_exprs) &&
        _sync_budget_exceeded("An output expression exceeds $(MAX_SYNC_EXPRESSION_BYTES) bytes.")
    raw_samples = _raw_get(body, :samples, nothing)
    raw_samples isa AbstractVector || throw(ArgumentError("samples must be an array"))
    isempty(raw_samples) && throw(ArgumentError("samples must not be empty"))
    length(raw_samples) <= MAX_ARCHITECTURE_SAMPLES ||
        _sync_budget_exceeded("samples exceeds the synchronous limit of $(MAX_ARCHITECTURE_SAMPLES).")
    epochs = _ad_integer(_raw_get(body, :epochs, nothing), "epochs", 250)
    debias_epochs = _ad_integer(
        _raw_get(body, :debias_epochs, nothing), "debias_epochs", 60; nonnegative=true)
    epochs <= MAX_ARCHITECTURE_EPOCHS ||
        _sync_budget_exceeded("epochs exceeds the synchronous limit of $(MAX_ARCHITECTURE_EPOCHS).")
    debias_epochs <= MAX_ARCHITECTURE_DEBIAS_EPOCHS ||
        _sync_budget_exceeded("debias_epochs exceeds the synchronous limit of $(MAX_ARCHITECTURE_DEBIAS_EPOCHS).")
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
    # Every gradient evaluation performs an equilibrium and adjoint solve. The
    # second refit is conditional; reserve it before any optimization begins.
    solve_cost = 2 * length(raw_samples) * (epochs + 2 * debias_epochs + 6) * model.n^3
    enforce_sync_cost(solve_cost, MAX_SYNC_SCAN_SOLVE_COST, "Architecture discovery")
    output_matrix = zeros(Float64, length(output_exprs), model.n)
    for (index, expression) in enumerate(output_exprs)
        output_matrix[index, :] .= parse_linear_combination(model, expression)
    end

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
        epochs=epochs,
        active_threshold=_ad_real(
            _raw_get(body, :active_threshold, nothing), "active_threshold", 0.05;
            nonnegative=true,
        ),
        debias_epochs=debias_epochs,
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
        "debias_history" => fit.debias_history,
        "prediction_basis" => "candidate_library_with_weak_inactive_reactions",
        "evidence_tier" => "sampled_equilibrium_fit",
        "selection_scope" => "provided_candidate_reactions",
        "optimality" => "local_gradient_fit",
        "identifiability" => "not_assessed",
        "q_sym" => total_names,
        "x_sym" => string.(model.x_sym),
        "output_exprs" => output_exprs,
        "species" => string.(species),
        "free_species" => string.(free_species),
        "product_species" => string.(product_species),
    )
    result["selected_network"] = _ad_selected_network(
        rules[active_indices], fit.kd[active_indices], model, totals, targets, output_matrix)
    result["work_budget"] = Dict(
        "solve_cost" => solve_cost, "max_solve_cost" => MAX_SYNC_SCAN_SOLVE_COST,
        "sample_count" => length(raw_samples), "epochs" => epochs,
        "debias_epochs" => debias_epochs,
    )
    if !isnothing(simulation_kd)
        result["simulation"] = Dict(
            "kd" => simulation_kd,
            "noise_log_std" => simulation_noise,
        )
    end
    return attach_artifact!(result, "discover_architecture";
        algorithm_name="adjoint_adam_sparse_affinity", config=body)
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
