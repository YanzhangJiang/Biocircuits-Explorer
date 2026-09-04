export architecture_loss_gradient, discover_architecture

function _architecture_output_scales(targets::AbstractMatrix{<:Real})
    sample_count, output_count = size(targets)
    sample_count > 0 || throw(ArgumentError("at least one sample is required"))
    output_count > 0 || throw(ArgumentError("at least one output is required"))
    return [
        let value = sqrt(sum(abs2, @view(targets[:, j])) / sample_count)
            value > 0 ? value : 1.0
        end
        for j in 1:output_count
    ]
end

"""
    architecture_loss_gradient(model, totals, targets, outputs, logkd;
                               scales=nothing, warm_starts=nothing)

Evaluate a static equilibrium dataset and differentiate its normalized squared
error with respect to `log10(Kd)`. `totals` has one row per experiment and one
column per conserved total; `outputs` has one row per observed linear
combination of species.

The gradient is obtained by one adjoint solve per experiment. It does not
differentiate through the nonlinear solver and does not form an inverse.
"""
function architecture_loss_gradient(
    model::Bnc,
    totals::AbstractMatrix{<:Real},
    targets::AbstractMatrix{<:Real},
    outputs::AbstractMatrix{<:Real},
    logkd::AbstractVector{<:Real};
    scales=nothing,
    warm_starts=nothing,
)
    sample_count = size(totals, 1)
    output_count = size(outputs, 1)
    sample_count > 0 || throw(ArgumentError("at least one sample is required"))
    output_count > 0 || throw(ArgumentError("at least one output is required"))
    size(totals, 2) == model.d || throw(DimensionMismatch(
        "totals must have $(model.d) columns"))
    size(targets) == (sample_count, output_count) || throw(DimensionMismatch(
        "targets must have size ($(sample_count), $(output_count))"))
    size(outputs, 2) == model.n || throw(DimensionMismatch(
        "outputs must have $(model.n) columns"))
    length(logkd) == model.r || throw(DimensionMismatch(
        "logkd must have $(model.r) entries"))

    q = Float64.(totals)
    y = Float64.(targets)
    C = Float64.(outputs)
    theta = Float64.(logkd)
    all(isfinite, q) && all(>(0), q) || throw(ArgumentError(
        "all conserved totals must be finite and positive"))
    all(isfinite, y) || throw(ArgumentError("all targets must be finite"))
    all(isfinite, C) || throw(ArgumentError("all output coefficients must be finite"))
    all(isfinite, theta) || throw(ArgumentError("all log10(Kd) values must be finite"))

    output_scales = if isnothing(scales)
        _architecture_output_scales(y)
    else
        Float64.(collect(scales))
    end
    length(output_scales) == output_count || throw(DimensionMismatch(
        "scales must have $(output_count) entries"))
    all(value -> isfinite(value) && value > 0, output_scales) ||
        throw(ArgumentError("all output scales must be finite and positive"))

    starts = if isnothing(warm_starts)
        result = Vector{Any}(undef, sample_count)
        fill!(result, nothing)
        result
    else
        length(warm_starts) == sample_count || throw(DimensionMismatch(
            "warm_starts must have one entry per sample"))
        warm_starts
    end

    predictions = zeros(Float64, sample_count, output_count)
    gradient = zeros(Float64, model.r)
    data_loss = 0.0
    log10_qk = Vector{Float64}(undef, model.n)
    log10_qk[model.d + 1:end] .= theta
    normalization = sample_count * output_count

    for sample in 1:sample_count
        log10_qk[1:model.d] .= log10.(@view q[sample, :])
        status = Ref{Symbol}(:unknown)
        start = starts[sample]
        logx = if isnothing(start)
            qK2x(
                model,
                log10_qk;
                input_logspace=true,
                output_logspace=true,
                status=status,
                abstol=1e-10,
                reltol=1e-9,
            )
        else
            qK2x(
                model,
                log10_qk;
                input_logspace=true,
                output_logspace=true,
                startlogx=start.logx,
                startlogqK=start.logqk,
                status=status,
                abstol=1e-10,
                reltol=1e-9,
            )
        end
        status[] == :success || error("equilibrium solve failed for sample $sample")
        all(isfinite, logx) || error("equilibrium solve returned non-finite values for sample $sample")
        starts[sample] = (logx=Float64.(logx), logqk=copy(log10_qk))

        x = exp10.(logx)
        prediction = C * x
        predictions[sample, :] .= prediction
        residual = prediction .- @view(y[sample, :])
        scaled_residual = residual ./ output_scales
        data_loss += 0.5 * sum(abs2, scaled_residual) / normalization

        # d(loss)/d(log10(x)). The ln(10) factor comes from x = 10^logx.
        eta = residual ./ (output_scales .^ 2) ./ normalization
        rhs = log(10.0) .* x .* (transpose(C) * eta)
        jacobian = ∂logqK_∂logx(model; x=x)
        adjoint = transpose(jacobian) \ rhs
        all(isfinite, adjoint) || error("adjoint solve returned non-finite values for sample $sample")
        gradient .+= @view adjoint[model.d + 1:end]
    end

    return (
        data_loss=data_loss,
        gradient=gradient,
        predictions=predictions,
        scales=output_scales,
        warm_starts=starts,
    )
end

function _architecture_adam(
    model::Bnc,
    totals::Matrix{Float64},
    targets::Matrix{Float64},
    outputs::Matrix{Float64},
    initial_logkd::Vector{Float64};
    scales::Vector{Float64},
    sparsity::Float64,
    learning_rate::Float64,
    epochs::Int,
    trainable::BitVector,
    logkd_bounds::Tuple{Float64, Float64},
)
    theta = copy(initial_logkd)
    first_moment = zeros(Float64, model.r)
    second_moment = zeros(Float64, model.r)
    starts = nothing
    history = NamedTuple[]
    beta1, beta2 = 0.9, 0.999

    for epoch in 1:epochs
        evaluation = architecture_loss_gradient(
            model, totals, targets, outputs, theta;
            scales=scales, warm_starts=starts,
        )
        starts = evaluation.warm_starts
        affinity = exp10.(-theta)
        objective = evaluation.data_loss + sparsity * sum(affinity)
        gradient = evaluation.gradient .- sparsity * log(10.0) .* affinity
        all(isfinite, gradient) || error("architecture gradient became non-finite")

        first_moment .= beta1 .* first_moment .+ (1 - beta1) .* gradient
        second_moment .= beta2 .* second_moment .+ (1 - beta2) .* gradient .^ 2
        first_unbiased = first_moment ./ (1 - beta1^epoch)
        second_unbiased = second_moment ./ (1 - beta2^epoch)

        for reaction in eachindex(theta)
            trainable[reaction] || continue
            theta[reaction] -= learning_rate * first_unbiased[reaction] /
                               (sqrt(second_unbiased[reaction]) + 1e-8)
            theta[reaction] = clamp(theta[reaction], logkd_bounds...)
        end

        if epoch == 1 || epoch == epochs || epoch % 10 == 0
            push!(history, (
                epoch=epoch,
                data_loss=evaluation.data_loss,
                objective=objective,
            ))
        end
    end

    final = architecture_loss_gradient(
        model, totals, targets, outputs, theta;
        scales=scales, warm_starts=starts,
    )
    return (logkd=theta, evaluation=final, history=history)
end

"""
    discover_architecture(model, totals, targets, outputs; kwargs...)

Fit an over-specified equilibrium binding network and select reactions by
penalizing association strength `1 / Kd`. Parameters are represented as
`log10(Kd)`, matching the rest of BindingAndCatalysis.

When `initial_kd` is omitted, a tiny deterministic spread breaks exact
parameter symmetry; explicitly supplied initial values are left unchanged.

After selection, inactive reactions are placed at the weak-binding bound and
the active affinities are refit without the sparsity penalty.
"""
function discover_architecture(
    model::Bnc,
    totals::AbstractMatrix{<:Real},
    targets::AbstractMatrix{<:Real},
    outputs::AbstractMatrix{<:Real};
    initial_kd=nothing,
    sparsity::Real=1e-3,
    learning_rate::Real=0.03,
    epochs::Integer=250,
    active_threshold::Real=0.05,
    debias_epochs::Integer=60,
    logkd_bounds=(-12.0, 12.0),
)
    q = Float64.(totals)
    y = Float64.(targets)
    C = Float64.(outputs)
    kd0 = if isnothing(initial_kd)
        model.r <= 1 ? ones(Float64, model.r) :
            exp10.(range(-0.004, 0.004; length=model.r))
    else
        Float64.(collect(initial_kd))
    end
    length(kd0) == model.r || throw(DimensionMismatch(
        "initial_kd must have $(model.r) entries"))
    all(value -> isfinite(value) && value > 0, kd0) || throw(ArgumentError(
        "initial_kd values must be finite and positive"))

    lambda = Float64(sparsity)
    lr = Float64(learning_rate)
    threshold = Float64(active_threshold)
    epoch_count = Int(epochs)
    refit_count = Int(debias_epochs)
    bounds = (Float64(logkd_bounds[1]), Float64(logkd_bounds[2]))
    isfinite(lambda) && lambda >= 0 || throw(ArgumentError("sparsity must be non-negative"))
    isfinite(lr) && lr > 0 || throw(ArgumentError("learning_rate must be positive"))
    isfinite(threshold) && threshold >= 0 || throw(ArgumentError(
        "active_threshold must be non-negative"))
    epoch_count > 0 || throw(ArgumentError("epochs must be positive"))
    refit_count >= 0 || throw(ArgumentError("debias_epochs must be non-negative"))
    all(isfinite, bounds) && bounds[1] < bounds[2] || throw(ArgumentError(
        "logkd_bounds must be an increasing finite pair"))

    initial_logkd = clamp.(log10.(kd0), bounds...)
    scales = _architecture_output_scales(y)

    sparse_fit = _architecture_adam(
        model, q, y, C, initial_logkd;
        scales=scales,
        sparsity=lambda,
        learning_rate=lr,
        epochs=epoch_count,
        trainable=trues(model.r),
        logkd_bounds=bounds,
    )
    selection_affinity = exp10.(-sparse_fit.logkd)
    active = BitVector(selection_affinity .>= threshold)

    final_logkd = copy(sparse_fit.logkd)
    final_logkd[.!active] .= bounds[2]
    final_fit = if any(active) && refit_count > 0
        _architecture_adam(
            model, q, y, C, final_logkd;
            scales=scales,
            sparsity=0.0,
            learning_rate=lr,
            epochs=refit_count,
            trainable=active,
            logkd_bounds=bounds,
        )
    else
        evaluation = architecture_loss_gradient(
            model, q, y, C, final_logkd; scales=scales)
        (logkd=final_logkd, evaluation=evaluation, history=NamedTuple[])
    end

    return (
        kd=exp10.(final_fit.logkd),
        selection_kd=exp10.(sparse_fit.logkd),
        affinity=selection_affinity,
        active=active,
        predictions=final_fit.evaluation.predictions,
        targets=y,
        output_scales=scales,
        fit_loss=final_fit.evaluation.data_loss,
        sparse_fit_loss=sparse_fit.evaluation.data_loss,
        sparse_objective=sparse_fit.evaluation.data_loss + lambda * sum(selection_affinity),
        loss_history=sparse_fit.history,
        debias_history=final_fit.history,
    )
end
