"""
    ROFieldGridLimitExceeded

Raised before grid-result allocation when a Cartesian reaction-order field
would exceed the caller's declared point budget.
"""
struct ROFieldGridLimitExceeded <: Exception
    requested_shape::Vector{Int}
    max_grid_points::Int
end

function Base.showerror(io::IO, err::ROFieldGridLimitExceeded)
    requested = prod(BigInt.(err.requested_shape))
    print(io,
        "reaction-order field grid shape ", Tuple(err.requested_shape),
        " has ", requested, " points, exceeding max_grid_points=",
        err.max_grid_points)
end

"""
    SampledReactionOrderField

Bounded numerical reaction-order field over an ordered Cartesian product of
log10 `qK` coordinates. Array dimensions follow the engine contract:

- `output_log10`: `grid_shape..., output_count`
- `reaction_orders`: `grid_shape..., output_count, input_count`
- `validity` and `regime_ids`: `grid_shape...`

Invalid points remain `NaN` in both numeric arrays, `false` in `validity`, and
`0` in `regime_ids`.
"""
struct SampledReactionOrderField{
    O<:AbstractArray{Float64},
    R<:AbstractArray{Float64},
    V<:AbstractArray{Bool},
    G<:AbstractArray{Int},
}
    axis_indices::Vector{Int}
    axis_coordinates_log10::Vector{Vector{Float64}}
    output_indices::Vector{Int}
    fixed_logqK::Vector{Float64}
    output_log10::O
    reaction_orders::R
    validity::V
    regime_ids::G
end

@inline function _ro_field_expected_numerical_failure(err)
    return err isa DomainError ||
           err isa OverflowError ||
           err isa LinearAlgebra.SingularException ||
           err isa LinearAlgebra.LAPACKException
end

function _normalize_ro_field_request(
    model::Bnc,
    axis_indices::AbstractVector{<:Integer},
    axis_coordinates::AbstractVector,
    output_indices::AbstractVector{<:Integer},
    fixed_logqK::AbstractVector{<:Real},
    max_grid_points::Integer,
)
    isempty(axis_indices) && throw(ArgumentError(
        "at least one ordered qK axis is required"))
    length(axis_coordinates) == length(axis_indices) || throw(DimensionMismatch(
        "axis_coordinates must contain one coordinate vector per axis index"))
    isempty(output_indices) && throw(ArgumentError(
        "at least one output species is required"))
    max_grid_points > 0 || throw(ArgumentError(
        "max_grid_points must be positive"))
    max_grid_points <= typemax(Int) || throw(ArgumentError(
        "max_grid_points must fit in Int"))

    axes_idx = Int.(axis_indices)
    outputs_idx = Int.(output_indices)
    length(unique(axes_idx)) == length(axes_idx) || throw(ArgumentError(
        "axis_indices must be unique and ordered"))
    length(unique(outputs_idx)) == length(outputs_idx) || throw(ArgumentError(
        "output_indices must be unique and ordered"))
    all(i -> 1 <= i <= model.n, axes_idx) || throw(BoundsError(1:model.n, axes_idx))
    all(i -> 1 <= i <= model.n, outputs_idx) || throw(BoundsError(1:model.n, outputs_idx))

    all(axis -> axis isa AbstractVector && eltype(axis) <: Real, axis_coordinates) ||
        throw(ArgumentError("each axis coordinate collection must be a real vector"))
    grid_shape = Int[length(axis) for axis in axis_coordinates]
    all(>(0), grid_shape) || throw(ArgumentError(
        "each axis must contain at least one coordinate"))

    # The point-count gate deliberately precedes coordinate copies, regime
    # construction, solver work, and every result-array allocation.
    requested_points = prod(BigInt.(grid_shape))
    limit = Int(max_grid_points)
    requested_points <= limit || throw(ROFieldGridLimitExceeded(grid_shape, limit))

    length(fixed_logqK) == model.n || throw(DimensionMismatch(
        "fixed_logqK must contain all $(model.n) q/K coordinates"))
    fixed = Float64.(fixed_logqK)
    all(isfinite, fixed) || throw(ArgumentError(
        "fixed_logqK coordinates must be finite"))

    coordinates = [Float64.(collect(axis)) for axis in axis_coordinates]
    all(axis -> all(isfinite, axis), coordinates) || throw(ArgumentError(
        "axis coordinates must be finite log10 values"))

    return axes_idx, coordinates, outputs_idx, fixed, Tuple(grid_shape)
end

"""
    sample_reaction_order_field(model, axis_indices, axis_coordinates,
        output_indices, fixed_logqK; max_grid_points=100_000,
        cancel_check=_NO_CANCEL_CHECK, solver_kwargs...)
        -> SampledReactionOrderField

Sample the finite-equilibrium reaction-order matrix
`d log(x_outputs) / d log(qK_axes)` on a bounded Cartesian log10 grid.

`axis_indices` defines semantic and array-axis order. `axis_coordinates`
contains one log10 coordinate vector per input axis. `fixed_logqK` is a full
reference `qK` vector; swept coordinates are overwritten and every other
coordinate remains fixed. `output_indices` is an ordered list of species.

The point budget is checked before result allocation or model computation.
The cancellation callback is invoked before regime construction, before every
grid point, and after the final point. A point is valid only when the
equilibrium solve succeeds, all concentrations and requested derivatives are
finite, and a regime identifier is available. Expected point-local numerical
failures remain explicit gaps; cancellation and programming errors propagate.
"""
function sample_reaction_order_field(
    model::Bnc,
    axis_indices::AbstractVector{<:Integer},
    axis_coordinates::AbstractVector,
    output_indices::AbstractVector{<:Integer},
    fixed_logqK::AbstractVector{<:Real};
    max_grid_points::Integer=100_000,
    cancel_check=_NO_CANCEL_CHECK,
    solver_kwargs...,
)
    axes_idx, coordinates, outputs_idx, fixed, grid_shape =
        _normalize_ro_field_request(
            model,
            axis_indices,
            axis_coordinates,
            output_indices,
            fixed_logqK,
            max_grid_points,
        )

    cancel_check()
    find_all_regimes!(model; cancel_check=cancel_check)
    cancel_check()

    output_shape = (grid_shape..., length(outputs_idx))
    reaction_order_shape = (
        grid_shape..., length(outputs_idx), length(axes_idx))
    output_log10 = fill(NaN, output_shape)
    reaction_orders = fill(NaN, reaction_order_shape)
    validity = falses(grid_shape)
    regime_ids = fill(0, grid_shape)

    logqK = copy(fixed)
    status = Ref(:pending)
    for grid_index in CartesianIndices(validity)
        cancel_check()
        point_index = Tuple(grid_index)
        for axis_position in eachindex(axes_idx)
            logqK[axes_idx[axis_position]] =
                coordinates[axis_position][point_index[axis_position]]
        end

        status[] = :pending
        logx = try
            qK2x(
                model,
                logqK;
                input_logspace=true,
                output_logspace=true,
                status=status,
                solver_kwargs...,
            )
        catch err
            _ro_field_expected_numerical_failure(err) || rethrow()
            continue
        end
        status[] === :success || continue
        all(isfinite, logx) || continue

        jacobian = try
            ∂logx_∂logqK(
                model;
                x=logx,
                qK=logqK,
                input_logspace=true,
            )
        catch err
            _ro_field_expected_numerical_failure(err) || rethrow()
            continue
        end
        selected_outputs = logx[outputs_idx]
        selected_reaction_orders = jacobian[outputs_idx, axes_idx]
        all(isfinite, selected_outputs) || continue
        all(isfinite, selected_reaction_orders) || continue

        regime_idx = assign_regime_qK(
            model,
            logqK;
            input_logspace=true,
            return_idx=true,
            membership=:closed_cell,
        )
        regime_idx >= 1 || continue

        @views output_log10[point_index..., :] .= selected_outputs
        @views reaction_orders[point_index..., :, :] .= selected_reaction_orders
        validity[grid_index] = true
        regime_ids[grid_index] = regime_idx
    end
    cancel_check()

    return SampledReactionOrderField(
        axes_idx,
        coordinates,
        outputs_idx,
        fixed,
        output_log10,
        reaction_orders,
        validity,
        regime_ids,
    )
end
