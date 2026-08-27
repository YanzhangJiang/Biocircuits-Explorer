"""Raised before a differential RO-field diagnostic exceeds its work budget."""
struct RODifferentialLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::RODifferentialLimitExceeded)
    print(io, "RO-field differential analysis ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

"""Summary for one ordered pair of input axes in a sampled-grid audit."""
struct ROIntegrabilityPairSummary
    axis_pair::NTuple{2,Int}
    total_face_count::Int
    evaluated_face_count::Int
    invalid_face_count::Int
    violating_face_count::Int
    max_abs_circulation::Union{Nothing,Float64}
    max_abs_mixed_partial_mismatch::Union{Nothing,Float64}
    max_abs_output_edge_residual::Union{Nothing,Float64}
    max_normalized_residual::Union{Nothing,Float64}
    worst_face_base_index::Vector{Int}
    worst_output_index::Union{Nothing,Int}
end

"""
Finite-grid evidence for whether a sampled reaction-order matrix behaves like
the gradient of the sampled output values on every valid elementary 2-face.

`status` is one of `:consistent_on_tested_grid`, `:discrete_inconsistent`,
`:unknown_gap`, or `:insufficient_grid`.  Even the first status is not a
continuum integrability proof: it is scoped to the declared finite grid and
tolerances.
"""
struct RODiscreteIntegrabilityCertificate
    status::Symbol
    complete::Bool
    input_dimension::Int
    output_count::Int
    total_face_count::Int
    evaluated_face_count::Int
    invalid_face_count::Int
    violating_face_count::Int
    absolute_tolerance::Float64
    relative_tolerance::Float64
    pair_summaries::Vector{ROIntegrabilityPairSummary}
end

"""
Cell-centred finite-difference derivatives of a sampled reaction-order field.

`gradient_jacobian[..., output, i, j]` estimates `d R_i / d u_j` without
silently symmetrizing it. `symmetric_hessian` is its symmetric part,
`antisymmetry_residual[..., output]` records the information discarded by that
projection, and `mixed_output_curvature` independently estimates mixed
finite-window curvature from output values. Invalid cells remain NaN gaps.
"""
struct ROFiniteDifferenceCurvature
    status::Symbol
    complete::Bool
    cell_shape::Vector{Int}
    total_cell_count::Int
    evaluated_cell_count::Int
    invalid_cell_count::Int
    validity::BitArray
    gradient_jacobian::Array{Float64}
    symmetric_hessian::Array{Float64}
    mixed_output_curvature::Array{Float64}
    antisymmetry_residual::Array{Float64}
    hessian_eigenvalues::Array{Float64}
end

"""Counts for one output and one unordered input pair under an explicit policy."""
struct ROSynergyPairSummary
    axis_pair::NTuple{2,Int}
    output_index::Int
    evaluated_cell_count::Int
    positive_count::Int
    negative_count::Int
    neutral_count::Int
    unknown_gap_count::Int
end

"""
Finite-window labels under the declared `:positive_log_cross_curvature` policy.

This is a coordinate-, scale-, and window-dependent mathematical convention:
positive mixed output curvature is labelled `:synergistic_under_policy`,
negative curvature `:antagonistic_under_policy`, and a magnitude no larger
than `threshold` `:neutral_under_policy`. It is not a causal or mechanistic
interaction claim. Invalid cells remain `:unknown_gap`; diagonal entries are
`:not_applicable`.
"""
struct ROFiniteWindowSynergy
    policy::Symbol
    threshold::Float64
    classification::Array{Symbol}
    pair_summaries::Vector{ROSynergyPairSummary}
end

@inline function _rod_limit(phase::Symbol, requested::BigInt, limit::Integer)
    limit > 0 || throw(ArgumentError("$phase limit must be positive"))
    limit <= typemax(Int) || throw(ArgumentError("$phase limit must fit in Int"))
    requested <= limit || throw(RODifferentialLimitExceeded(
        phase, requested, Int(limit)))
    return nothing
end

function _rod_sampled_field_dimensions(field::SampledReactionOrderField)
    input_dimension = length(field.axis_indices)
    output_count = length(field.output_indices)
    grid_shape = size(field.validity)
    length(grid_shape) == input_dimension || throw(DimensionMismatch(
        "field validity rank does not match the input-axis count"))
    size(field.regime_ids) == grid_shape || throw(DimensionMismatch(
        "field regime_ids shape does not match validity"))
    size(field.output_log10) == (grid_shape..., output_count) ||
        throw(DimensionMismatch(
            "field output_log10 shape does not match grid and outputs"))
    size(field.reaction_orders) ==
        (grid_shape..., output_count, input_dimension) ||
        throw(DimensionMismatch(
            "field reaction_orders shape does not match grid, outputs, and axes"))
    length(field.axis_coordinates_log10) == input_dimension ||
        throw(DimensionMismatch(
            "field coordinate-vector count does not match input axes"))
    for axis in 1:input_dimension
        coordinates = field.axis_coordinates_log10[axis]
        length(coordinates) == grid_shape[axis] || throw(DimensionMismatch(
            "axis $axis coordinate count does not match the grid shape"))
        all(isfinite, coordinates) || throw(ArgumentError(
            "axis $axis coordinates must be finite"))
        all(coordinates[index] < coordinates[index + 1]
            for index in 1:max(length(coordinates) - 1, 0)) ||
            throw(ArgumentError(
                "axis $axis coordinates must be strictly increasing"))
    end
    return input_dimension, output_count, grid_shape
end

@inline function _rod_point_is_finite(field, point_index, output_count,
                                      input_dimension)
    @inbounds for output in 1:output_count
        isfinite(field.output_log10[point_index..., output]) || return false
        for axis in 1:input_dimension
            isfinite(field.reaction_orders[point_index..., output, axis]) ||
                return false
        end
    end
    return true
end

@inline function _rod_residual_threshold(left::Float64, right::Float64,
                                         absolute_tolerance::Float64,
                                         relative_tolerance::Float64)
    return absolute_tolerance + relative_tolerance *
        max(1.0, abs(left), abs(right))
end

function _rod_face_count(grid_shape, left_axis::Int, right_axis::Int)
    count = BigInt(1)
    for axis in eachindex(grid_shape)
        extent = axis == left_axis || axis == right_axis ?
            max(grid_shape[axis] - 1, 0) : grid_shape[axis]
        count *= BigInt(extent)
    end
    return count
end

"""
    certify_sampled_ro_integrability(field; absolute_tolerance=1e-8,
        relative_tolerance=1e-6, max_faces=100_000,
        max_face_output_evaluations=1_000_000)

Audit all valid elementary oriented 2-faces using three independent finite-grid
signals: trapezoidal closed-loop circulation, mismatch between the two mixed
partial estimates, and output-edge change versus the corresponding reaction-
order line integral. Invalid corners make a face unknown rather than numeric.
"""
function certify_sampled_ro_integrability(
    field::SampledReactionOrderField;
    absolute_tolerance::Real=1e-8,
    relative_tolerance::Real=1e-6,
    max_faces::Integer=100_000,
    max_face_output_evaluations::Integer=1_000_000,
)
    atol = Float64(absolute_tolerance)
    rtol = Float64(relative_tolerance)
    isfinite(atol) && atol > 0 || throw(ArgumentError(
        "absolute_tolerance must be finite and positive"))
    isfinite(rtol) && rtol >= 0 || throw(ArgumentError(
        "relative_tolerance must be finite and nonnegative"))
    input_dimension, output_count, grid_shape =
        _rod_sampled_field_dimensions(field)

    total_faces_big = BigInt(0)
    pair_face_counts = Dict{NTuple{2,Int},BigInt}()
    for left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        pair = (left_axis, right_axis)
        count = _rod_face_count(grid_shape, pair...)
        pair_face_counts[pair] = count
        total_faces_big += count
    end
    _rod_limit(:faces, total_faces_big, max_faces)
    _rod_limit(:face_output_evaluations,
        total_faces_big * BigInt(output_count),
        max_face_output_evaluations)

    pair_summaries = ROIntegrabilityPairSummary[]
    total_evaluated = 0
    total_invalid = 0
    total_violating = 0

    for left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        face_shape = ntuple(axis ->
            axis == left_axis || axis == right_axis ?
                max(grid_shape[axis] - 1, 0) : grid_shape[axis],
            input_dimension)
        evaluated = 0
        invalid = 0
        violating = 0
        max_circulation = nothing
        max_mixed = nothing
        max_edge = nothing
        max_normalized = nothing
        worst_base = Int[]
        worst_output = nothing

        for face_index in CartesianIndices(face_shape)
            base = Tuple(face_index)
            p00 = base
            p10 = ntuple(axis -> base[axis] + (axis == left_axis),
                input_dimension)
            p01 = ntuple(axis -> base[axis] + (axis == right_axis),
                input_dimension)
            p11 = ntuple(axis -> base[axis] +
                (axis == left_axis) + (axis == right_axis),
                input_dimension)
            corners = (p00, p10, p01, p11)
            if !all(point -> field.validity[point...], corners)
                invalid += 1
                continue
            end
            all(point -> _rod_point_is_finite(
                field, point, output_count, input_dimension), corners) ||
                throw(ArgumentError(
                    "a valid sampled-field point contains non-finite data"))

            delta_left = field.axis_coordinates_log10[left_axis][base[left_axis] + 1] -
                field.axis_coordinates_log10[left_axis][base[left_axis]]
            delta_right = field.axis_coordinates_log10[right_axis][base[right_axis] + 1] -
                field.axis_coordinates_log10[right_axis][base[right_axis]]
            evaluated += 1
            face_violates = false

            for output in 1:output_count
                rleft00 = field.reaction_orders[p00..., output, left_axis]
                rleft10 = field.reaction_orders[p10..., output, left_axis]
                rleft01 = field.reaction_orders[p01..., output, left_axis]
                rleft11 = field.reaction_orders[p11..., output, left_axis]
                rright00 = field.reaction_orders[p00..., output, right_axis]
                rright10 = field.reaction_orders[p10..., output, right_axis]
                rright01 = field.reaction_orders[p01..., output, right_axis]
                rright11 = field.reaction_orders[p11..., output, right_axis]

                circulation =
                    delta_left * (rleft00 + rleft10) / 2 +
                    delta_right * (rright10 + rright11) / 2 -
                    delta_left * (rleft01 + rleft11) / 2 -
                    delta_right * (rright00 + rright01) / 2
                dleft_dright =
                    ((rleft01 - rleft00) + (rleft11 - rleft10)) /
                    (2delta_right)
                dright_dleft =
                    ((rright10 - rright00) + (rright11 - rright01)) /
                    (2delta_left)
                mixed_mismatch = dleft_dright - dright_dleft

                z00 = field.output_log10[p00..., output]
                z10 = field.output_log10[p10..., output]
                z01 = field.output_log10[p01..., output]
                z11 = field.output_log10[p11..., output]
                edge_pairs = (
                    (z10 - z00, delta_left * (rleft00 + rleft10) / 2),
                    (z11 - z01, delta_left * (rleft01 + rleft11) / 2),
                    (z01 - z00, delta_right * (rright00 + rright01) / 2),
                    (z11 - z10, delta_right * (rright10 + rright11) / 2),
                )
                edge_residual = maximum(abs(observed - integrated)
                    for (observed, integrated) in edge_pairs)

                mixed_threshold = _rod_residual_threshold(
                    dleft_dright, dright_dleft, atol, rtol)
                circulation_scale = max(delta_left * delta_right, eps(Float64))
                circulation_threshold = mixed_threshold * circulation_scale
                edge_normalized = maximum(
                    abs(observed - integrated) /
                    _rod_residual_threshold(observed, integrated, atol, rtol)
                    for (observed, integrated) in edge_pairs)
                normalized = max(
                    abs(mixed_mismatch) / mixed_threshold,
                    abs(circulation) / circulation_threshold,
                    edge_normalized,
                )

                max_circulation = max(
                    something(max_circulation, 0.0), abs(circulation))
                max_mixed = max(something(max_mixed, 0.0),
                    abs(mixed_mismatch))
                max_edge = max(something(max_edge, 0.0), edge_residual)
                if max_normalized === nothing || normalized > max_normalized
                    max_normalized = normalized
                    worst_base = collect(base)
                    worst_output = output
                end
                normalized <= 1.0 || (face_violates = true)
            end
            face_violates && (violating += 1)
        end

        total = Int(pair_face_counts[(left_axis, right_axis)])
        push!(pair_summaries, ROIntegrabilityPairSummary(
            (left_axis, right_axis), total, evaluated, invalid, violating,
            max_circulation, max_mixed, max_edge, max_normalized,
            worst_base, worst_output,
        ))
        total_evaluated += evaluated
        total_invalid += invalid
        total_violating += violating
    end

    total_faces = Int(total_faces_big)
    complete = total_faces > 0 && total_invalid == 0
    status = if total_faces == 0
        :insufficient_grid
    elseif total_violating > 0
        :discrete_inconsistent
    elseif total_invalid > 0
        :unknown_gap
    else
        :consistent_on_tested_grid
    end
    return RODiscreteIntegrabilityCertificate(
        status,
        complete,
        input_dimension,
        output_count,
        total_faces,
        total_evaluated,
        total_invalid,
        total_violating,
        atol,
        rtol,
        pair_summaries,
    )
end

function _rod_cell_count(grid_shape)
    return prod(BigInt(max(extent - 1, 0)) for extent in grid_shape)
end

@inline function _rod_corner_index(base, mask::Int, dimension::Int)
    return ntuple(axis -> base[axis] + ((mask >> (axis - 1)) & 1),
        dimension)
end

"""
    estimate_sampled_ro_curvature(field; max_cells=100_000,
        max_corner_visits=1_000_000)

Estimate the cell-centred gradient Jacobian, its symmetric Hessian projection,
eigenvalues, and mixed finite-window output curvature on a non-uniform
Cartesian grid. These quantities are coordinate- and window-dependent
diagnostics; they are not causal or mechanism-specific synergy claims.
"""
function estimate_sampled_ro_curvature(
    field::SampledReactionOrderField;
    max_cells::Integer=100_000,
    max_corner_visits::Integer=1_000_000,
)
    input_dimension, output_count, grid_shape =
        _rod_sampled_field_dimensions(field)
    cell_shape = Int[max(extent - 1, 0) for extent in grid_shape]
    total_cells_big = _rod_cell_count(grid_shape)
    corners_per_cell_big = BigInt(2)^input_dimension
    _rod_limit(:curvature_cells, total_cells_big, max_cells)
    _rod_limit(:curvature_corner_visits,
        total_cells_big * corners_per_cell_big,
        max_corner_visits)
    total_cells = Int(total_cells_big)

    tensor_shape = (Tuple(cell_shape)..., output_count,
        input_dimension, input_dimension)
    scalar_shape = (Tuple(cell_shape)..., output_count)
    eigen_shape = (Tuple(cell_shape)..., output_count, input_dimension)
    validity = falses(Tuple(cell_shape))
    gradient_jacobian = fill(NaN, tensor_shape)
    symmetric_hessian = fill(NaN, tensor_shape)
    mixed_output_curvature = fill(NaN, tensor_shape)
    antisymmetry_residual = fill(NaN, scalar_shape)
    hessian_eigenvalues = fill(NaN, eigen_shape)

    total_cells == 0 && return ROFiniteDifferenceCurvature(
        :insufficient_grid, false, cell_shape, 0, 0, 0, validity,
        gradient_jacobian, symmetric_hessian, mixed_output_curvature,
        antisymmetry_residual, hessian_eigenvalues)

    corner_count = Int(corners_per_cell_big)
    evaluated = 0
    invalid = 0
    for cell_index in CartesianIndices(Tuple(cell_shape))
        base = Tuple(cell_index)
        corners = [_rod_corner_index(base, mask, input_dimension)
            for mask in 0:(corner_count - 1)]
        if !all(point -> field.validity[point...], corners)
            invalid += 1
            continue
        end
        all(point -> _rod_point_is_finite(
            field, point, output_count, input_dimension), corners) ||
            throw(ArgumentError(
                "a valid sampled-field point contains non-finite data"))
        deltas = Float64[
            field.axis_coordinates_log10[axis][base[axis] + 1] -
            field.axis_coordinates_log10[axis][base[axis]]
            for axis in 1:input_dimension
        ]

        validity[cell_index] = true
        evaluated += 1
        for output in 1:output_count
            jacobian = Matrix{Float64}(undef,
                input_dimension, input_dimension)
            for component_axis in 1:input_dimension,
                derivative_axis in 1:input_dimension
                edge_sum = 0.0
                edge_count = 0
                for mask in 0:(corner_count - 1)
                    ((mask >> (derivative_axis - 1)) & 1) == 0 || continue
                    lower = _rod_corner_index(base, mask, input_dimension)
                    upper = ntuple(axis -> lower[axis] +
                        (axis == derivative_axis), input_dimension)
                    edge_sum +=
                        field.reaction_orders[upper..., output, component_axis] -
                        field.reaction_orders[lower..., output, component_axis]
                    edge_count += 1
                end
                jacobian[component_axis, derivative_axis] =
                    edge_sum / (edge_count * deltas[derivative_axis])
            end
            symmetric = (jacobian + transpose(jacobian)) / 2
            @views gradient_jacobian[base..., output, :, :] .= jacobian
            @views symmetric_hessian[base..., output, :, :] .= symmetric
            antisymmetry_residual[base..., output] =
                maximum(abs, jacobian - transpose(jacobian); init=0.0)
            @views hessian_eigenvalues[base..., output, :] .=
                eigvals(Symmetric(symmetric))

            for left_axis in 1:max(input_dimension - 1, 0),
                right_axis in (left_axis + 1):input_dimension
                contrast_sum = 0.0
                contrast_count = 0
                for mask in 0:(corner_count - 1)
                    ((mask >> (left_axis - 1)) & 1) == 0 || continue
                    ((mask >> (right_axis - 1)) & 1) == 0 || continue
                    p00 = _rod_corner_index(base, mask, input_dimension)
                    p10 = ntuple(axis -> p00[axis] +
                        (axis == left_axis), input_dimension)
                    p01 = ntuple(axis -> p00[axis] +
                        (axis == right_axis), input_dimension)
                    p11 = ntuple(axis -> p00[axis] +
                        (axis == left_axis) + (axis == right_axis),
                        input_dimension)
                    contrast_sum += (
                        field.output_log10[p11..., output] -
                        field.output_log10[p10..., output] -
                        field.output_log10[p01..., output] +
                        field.output_log10[p00..., output]
                    ) / (deltas[left_axis] * deltas[right_axis])
                    contrast_count += 1
                end
                contrast = contrast_sum / contrast_count
                mixed_output_curvature[
                    base..., output, left_axis, right_axis] = contrast
                mixed_output_curvature[
                    base..., output, right_axis, left_axis] = contrast
            end
        end
    end

    complete = invalid == 0
    status = evaluated == 0 ? :no_valid_cells :
        complete ? :complete : :partial
    return ROFiniteDifferenceCurvature(
        status,
        complete,
        cell_shape,
        total_cells,
        evaluated,
        invalid,
        validity,
        gradient_jacobian,
        symmetric_hessian,
        mixed_output_curvature,
        antisymmetry_residual,
        hessian_eigenvalues,
    )
end

"""
    classify_finite_window_synergy(curvature; threshold=1e-8)

Apply the explicit positive-log-cross-curvature convention to an already
computed finite-difference result. No missing value is coerced to zero and no
continuum, causal, or mechanism-specific claim is made.
"""
function classify_finite_window_synergy(
    curvature::ROFiniteDifferenceCurvature;
    threshold::Real=1e-8,
)
    cutoff = Float64(threshold)
    isfinite(cutoff) && cutoff >= 0 || throw(ArgumentError(
        "synergy threshold must be finite and nonnegative"))
    input_dimension = size(curvature.mixed_output_curvature,
        ndims(curvature.mixed_output_curvature))
    ndims(curvature.mixed_output_curvature) >= 3 || throw(DimensionMismatch(
        "mixed_output_curvature must include output and two input axes"))
    size(curvature.mixed_output_curvature,
        ndims(curvature.mixed_output_curvature) - 1) == input_dimension ||
        throw(DimensionMismatch(
            "mixed_output_curvature input-axis dimensions must agree"))
    output_axis = ndims(curvature.mixed_output_curvature) - 2
    output_count = size(curvature.mixed_output_curvature, output_axis)
    Tuple(size(curvature.validity)) == Tuple(curvature.cell_shape) ||
        throw(DimensionMismatch(
            "curvature validity shape must agree with cell_shape"))
    expected_shape = (Tuple(curvature.cell_shape)..., output_count,
        input_dimension, input_dimension)
    size(curvature.mixed_output_curvature) == expected_shape ||
        throw(DimensionMismatch(
            "mixed_output_curvature shape is inconsistent"))

    classification = fill(:unknown_gap, expected_shape)
    for cell_index in CartesianIndices(Tuple(curvature.cell_shape)),
        output in 1:output_count, axis in 1:input_dimension
        point = Tuple(cell_index)
        classification[point..., output, axis, axis] = :not_applicable
    end

    summaries = ROSynergyPairSummary[]
    for output in 1:output_count,
        left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        positive = 0
        negative = 0
        neutral = 0
        unknown = 0
        evaluated = 0
        for cell_index in CartesianIndices(Tuple(curvature.cell_shape))
            point = Tuple(cell_index)
            label = if !curvature.validity[cell_index]
                unknown += 1
                :unknown_gap
            else
                value = curvature.mixed_output_curvature[
                    point..., output, left_axis, right_axis]
                isfinite(value) || throw(ArgumentError(
                    "a valid curvature cell has non-finite mixed output curvature"))
                evaluated += 1
                if value > cutoff
                    positive += 1
                    :synergistic_under_policy
                elseif value < -cutoff
                    negative += 1
                    :antagonistic_under_policy
                else
                    neutral += 1
                    :neutral_under_policy
                end
            end
            classification[point..., output, left_axis, right_axis] = label
            classification[point..., output, right_axis, left_axis] = label
        end
        push!(summaries, ROSynergyPairSummary(
            (left_axis, right_axis), output, evaluated, positive, negative,
            neutral, unknown))
    end
    return ROFiniteWindowSynergy(
        :positive_log_cross_curvature, cutoff, classification, summaries)
end
