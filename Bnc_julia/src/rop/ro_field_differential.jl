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

function _rod_write_string(io::IO, value::AbstractString)
    bytes = codeunits(value)
    write(io, htol(UInt64(length(bytes))))
    write(io, bytes)
    return nothing
end

function _rod_write_int(io::IO, value::Integer)
    write(io, htol(reinterpret(UInt64, Int64(value))))
    return nothing
end

function _rod_write_float(io::IO, value::Float64)
    write(io, reinterpret(UInt8, [value]))
    return nothing
end

function _rod_write_optional_float(io::IO, value::Union{Nothing,Float64})
    write(io, UInt8(value === nothing ? 0 : 1))
    value === nothing || _rod_write_float(io, value)
    return nothing
end

function _rod_write_float_array(
    io::IO,
    values::Array{Float64};
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_write_int(io, ndims(values))
    for extent in size(values)
        _rod_write_int(io, extent)
    end
    bytes = reinterpret(UInt8, vec(values))
    chunk_hashes = IOBuffer()
    chunk_size = 1 << 20
    for first_byte in 1:chunk_size:length(bytes)
        _rod_cancel(cancel_check)
        last_byte = min(first_byte + chunk_size - 1, length(bytes))
        write(chunk_hashes, SHA.sha256(@view bytes[first_byte:last_byte]))
    end
    _rod_cancel(cancel_check)
    write(io, SHA.sha256(take!(chunk_hashes)))
    return nothing
end

function _rod_write_bit_array(
    io::IO,
    values::BitArray;
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_write_int(io, ndims(values))
    for extent in size(values)
        _rod_write_int(io, extent)
    end
    chunk_hashes = IOBuffer()
    chunk_size = 1 << 20
    linear_values = vec(values)
    for first_value in 1:chunk_size:length(values)
        _rod_cancel(cancel_check)
        last_value = min(first_value + chunk_size - 1, length(values))
        chunk = UInt8.(@view linear_values[first_value:last_value])
        write(chunk_hashes, SHA.sha256(chunk))
    end
    _rod_cancel(cancel_check)
    write(io, SHA.sha256(take!(chunk_hashes)))
    return nothing
end

function _rod_detached_copy(
    values::AbstractArray;
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_cancel(cancel_check)
    admitted = similar(values)
    chunk_size = 1 << 20
    for first_value in 1:chunk_size:length(values)
        _rod_cancel(cancel_check)
        count_value = min(chunk_size, length(values) - first_value + 1)
        copyto!(admitted, first_value, values, first_value, count_value)
    end
    _rod_cancel(cancel_check)
    return admitted
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
    content_sha256::String

    function ROIntegrabilityPairSummary(
        axis_pair::NTuple{2,Int},
        total_face_count::Int,
        evaluated_face_count::Int,
        invalid_face_count::Int,
        violating_face_count::Int,
        max_abs_circulation::Union{Nothing,Float64},
        max_abs_mixed_partial_mismatch::Union{Nothing,Float64},
        max_abs_output_edge_residual::Union{Nothing,Float64},
        max_normalized_residual::Union{Nothing,Float64},
        worst_face_base_index::Vector{Int},
        worst_output_index::Union{Nothing,Int},
        ;
        cancel_check=_NO_CANCEL_CHECK,
    )
        worst = _rod_detached_copy(
            worst_face_base_index; cancel_check)
        _rod_validate_pair_summary_components(
            axis_pair,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            max_abs_circulation,
            max_abs_mixed_partial_mismatch,
            max_abs_output_edge_residual,
            max_normalized_residual,
            worst,
            worst_output_index,
        )
        content_sha256 = _rod_pair_summary_sha256(
            axis_pair,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            max_abs_circulation,
            max_abs_mixed_partial_mismatch,
            max_abs_output_edge_residual,
            max_normalized_residual,
            worst,
            worst_output_index;
            cancel_check,
        )
        return new(
            axis_pair,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            max_abs_circulation,
            max_abs_mixed_partial_mismatch,
            max_abs_output_edge_residual,
            max_normalized_residual,
            worst,
            worst_output_index,
            content_sha256,
        )
    end
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
    content_sha256::String

    function RODiscreteIntegrabilityCertificate(
        status::Symbol,
        complete::Bool,
        input_dimension::Int,
        output_count::Int,
        total_face_count::Int,
        evaluated_face_count::Int,
        invalid_face_count::Int,
        violating_face_count::Int,
        absolute_tolerance::Float64,
        relative_tolerance::Float64,
        pair_summaries::Vector{ROIntegrabilityPairSummary},
        ;
        max_input_dimension::Integer=512,
        max_tensor_elements::Integer=50_000_000,
        max_output_bytes::Integer=256_000_000,
        max_tensor_work::Integer=100_000_000,
        cancel_check=_NO_CANCEL_CHECK,
    )
        _rod_integrability_certificate_preflight(
            input_dimension,
            pair_summaries;
            max_input_dimension,
            max_tensor_elements,
            max_output_bytes,
            max_tensor_work,
            cancel_check,
        )
        summaries = ROIntegrabilityPairSummary[]
        sizehint!(summaries, length(pair_summaries))
        for (position, summary) in enumerate(pair_summaries)
            _rod_periodic_cancel(cancel_check, position)
            push!(summaries, _rod_copy_pair_summary(
                summary; cancel_check))
        end
        _rod_validate_integrability_certificate_components(
            status,
            complete,
            input_dimension,
            output_count,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            absolute_tolerance,
            relative_tolerance,
            summaries;
            cancel_check,
        )
        content_sha256 = _rod_integrability_certificate_sha256(
            status,
            complete,
            input_dimension,
            output_count,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            absolute_tolerance,
            relative_tolerance,
            summaries;
            cancel_check,
        )
        return new(
            status,
            complete,
            input_dimension,
            output_count,
            total_face_count,
            evaluated_face_count,
            invalid_face_count,
            violating_face_count,
            absolute_tolerance,
            relative_tolerance,
            summaries,
            content_sha256,
        )
    end
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
    content_sha256::String

    function ROFiniteDifferenceCurvature(
        status::Symbol,
        complete::Bool,
        cell_shape::Vector{Int},
        total_cell_count::Int,
        evaluated_cell_count::Int,
        invalid_cell_count::Int,
        validity::BitArray,
        gradient_jacobian::Array{Float64},
        symmetric_hessian::Array{Float64},
        mixed_output_curvature::Array{Float64},
        antisymmetry_residual::Array{Float64},
        hessian_eigenvalues::Array{Float64},
        ;
        max_input_dimension::Integer=64,
        max_tensor_elements::Integer=20_000_000,
        max_output_bytes::Integer=256_000_000,
        max_tensor_work::Integer=250_000_000,
        cancel_check=_NO_CANCEL_CHECK,
    )
        _rod_curvature_result_preflight(
            cell_shape,
            validity,
            gradient_jacobian,
            symmetric_hessian,
            mixed_output_curvature,
            antisymmetry_residual,
            hessian_eigenvalues;
            max_input_dimension,
            max_tensor_elements,
            max_output_bytes,
            max_tensor_work,
            cancel_check,
        )
        admitted_cell_shape = _rod_detached_copy(
            cell_shape; cancel_check)
        admitted_validity = _rod_detached_copy(
            validity; cancel_check)
        admitted_gradient = _rod_detached_copy(
            gradient_jacobian; cancel_check)
        admitted_symmetric = _rod_detached_copy(
            symmetric_hessian; cancel_check)
        admitted_mixed = _rod_detached_copy(
            mixed_output_curvature; cancel_check)
        admitted_antisymmetry = _rod_detached_copy(
            antisymmetry_residual; cancel_check)
        admitted_eigenvalues = _rod_detached_copy(
            hessian_eigenvalues; cancel_check)
        _rod_validate_curvature_components(
            status,
            complete,
            admitted_cell_shape,
            total_cell_count,
            evaluated_cell_count,
            invalid_cell_count,
            admitted_validity,
            admitted_gradient,
            admitted_symmetric,
            admitted_mixed,
            admitted_antisymmetry,
            admitted_eigenvalues;
            cancel_check,
        )
        content_sha256 = _rod_curvature_sha256(
            status,
            complete,
            admitted_cell_shape,
            total_cell_count,
            evaluated_cell_count,
            invalid_cell_count,
            admitted_validity,
            admitted_gradient,
            admitted_symmetric,
            admitted_mixed,
            admitted_antisymmetry,
            admitted_eigenvalues;
            cancel_check,
        )
        return new(
            status,
            complete,
            admitted_cell_shape,
            total_cell_count,
            evaluated_cell_count,
            invalid_cell_count,
            admitted_validity,
            admitted_gradient,
            admitted_symmetric,
            admitted_mixed,
            admitted_antisymmetry,
            admitted_eigenvalues,
            content_sha256,
        )
    end
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
    content_sha256::String

    function ROFiniteWindowSynergy(
        policy::Symbol,
        threshold::Float64,
        classification::Array{Symbol},
        pair_summaries::Vector{ROSynergyPairSummary},
        ;
        max_input_dimension::Integer=64,
        max_classification_elements::Integer=10_000_000,
        max_output_bytes::Integer=256_000_000,
        max_tensor_work::Integer=250_000_000,
        cancel_check=_NO_CANCEL_CHECK,
    )
        _rod_synergy_result_preflight(
            classification,
            pair_summaries;
            max_input_dimension,
            max_classification_elements,
            max_output_bytes,
            max_tensor_work,
            cancel_check,
        )
        admitted_classification = _rod_detached_copy(
            classification; cancel_check)
        admitted_summaries = _rod_detached_copy(
            pair_summaries; cancel_check)
        _rod_validate_synergy_components(
            policy,
            threshold,
            admitted_classification,
            admitted_summaries;
            cancel_check,
        )
        content_sha256 = _rod_synergy_sha256(
            policy,
            threshold,
            admitted_classification,
            admitted_summaries;
            cancel_check,
        )
        return new(
            policy,
            threshold,
            admitted_classification,
            admitted_summaries,
            content_sha256,
        )
    end
end

function _rod_integrability_certificate_preflight(
    input_dimension::Int,
    pair_summaries::Vector{ROIntegrabilityPairSummary};
    max_input_dimension::Integer,
    max_tensor_elements::Integer,
    max_output_bytes::Integer,
    max_tensor_work::Integer,
    cancel_check,
)
    _rod_cancel(cancel_check)
    input_dimension > 0 || throw(ArgumentError(
        "integrability certificates require a positive input dimension"))
    _rod_limit(:integrability_result_input_dimension,
        BigInt(input_dimension), max_input_dimension)
    expected_summary_count = BigInt(input_dimension) *
        (BigInt(input_dimension) - 1) ÷ 2
    summary_count = BigInt(length(pair_summaries))
    summary_count == expected_summary_count || throw(ArgumentError(
        "integrability certificate must cover every unordered input pair"))

    nested_index_elements = BigInt(0)
    for (position, summary) in enumerate(pair_summaries)
        _rod_periodic_cancel(cancel_check, position)
        nested_index_elements += BigInt(length(
            getfield(summary, :worst_face_base_index)))
    end
    tensor_elements = summary_count + nested_index_elements
    _rod_limit(:integrability_result_tensor_elements,
        tensor_elements, max_tensor_elements)
    output_bytes =
        8 * summary_count * BigInt(64) +
        2 * nested_index_elements * BigInt(sizeof(Int))
    _rod_limit(:integrability_result_output_bytes,
        output_bytes, max_output_bytes)
    total_work = 16 * summary_count + 8 * nested_index_elements
    _rod_limit(:integrability_result_tensor_work,
        total_work, max_tensor_work)
    _rod_cancel(cancel_check)
    return nothing
end

function _rod_curvature_result_preflight(
    cell_shape::Vector{Int},
    validity::BitArray,
    gradient_jacobian::Array{Float64},
    symmetric_hessian::Array{Float64},
    mixed_output_curvature::Array{Float64},
    antisymmetry_residual::Array{Float64},
    hessian_eigenvalues::Array{Float64};
    max_input_dimension::Integer,
    max_tensor_elements::Integer,
    max_output_bytes::Integer,
    max_tensor_work::Integer,
    cancel_check,
)
    _rod_cancel(cancel_check)
    input_dimension = length(cell_shape)
    input_dimension > 0 || throw(ArgumentError(
        "curvature evidence requires at least one input axis"))
    _rod_limit(:curvature_result_input_dimension,
        BigInt(input_dimension), max_input_dimension)
    all(>=(0), cell_shape) || throw(ArgumentError(
        "curvature cell extents must be nonnegative"))
    ndims(gradient_jacobian) == input_dimension + 3 ||
        throw(DimensionMismatch(
            "curvature tensor rank is inconsistent with cell_shape"))
    output_count = size(gradient_jacobian, input_dimension + 1)
    output_count > 0 || throw(ArgumentError(
        "curvature evidence requires at least one output"))
    tensor_shape = (Tuple(cell_shape)..., output_count,
        input_dimension, input_dimension)
    scalar_shape = (Tuple(cell_shape)..., output_count)
    eigen_shape = (Tuple(cell_shape)..., output_count, input_dimension)
    size(validity) == Tuple(cell_shape) || throw(DimensionMismatch(
        "curvature validity shape must agree with cell_shape"))
    size(gradient_jacobian) == tensor_shape &&
        size(symmetric_hessian) == tensor_shape &&
        size(mixed_output_curvature) == tensor_shape ||
        throw(DimensionMismatch("curvature tensor shapes are inconsistent"))
    size(antisymmetry_residual) == scalar_shape ||
        throw(DimensionMismatch(
            "antisymmetry_residual shape is inconsistent"))
    size(hessian_eigenvalues) == eigen_shape ||
        throw(DimensionMismatch(
            "hessian_eigenvalues shape is inconsistent"))

    float_elements = BigInt(0)
    for values in (
        gradient_jacobian,
        symmetric_hessian,
        mixed_output_curvature,
        antisymmetry_residual,
        hessian_eigenvalues,
    )
        _rod_cancel(cancel_check)
        float_elements += BigInt(length(values))
    end
    validity_elements = BigInt(length(validity))
    tensor_elements = float_elements + validity_elements
    _rod_limit(:curvature_result_tensor_elements,
        tensor_elements, max_tensor_elements)
    bit_storage_bytes = (validity_elements + 7) ÷ 8
    bit_hash_scratch = min(validity_elements, BigInt(1 << 20))
    dimension = BigInt(input_dimension)
    eigen_scratch = (dimension * dimension + dimension) *
        BigInt(sizeof(Float64))
    output_bytes =
        float_elements * BigInt(sizeof(Float64)) +
        bit_storage_bytes +
        BigInt(input_dimension * sizeof(Int)) +
        max(bit_hash_scratch, eigen_scratch)
    _rod_limit(:curvature_result_output_bytes,
        output_bytes, max_output_bytes)
    total_cells = prod(BigInt, cell_shape)
    total_work =
        6 * tensor_elements +
        total_cells * BigInt(output_count) * dimension^3
    _rod_limit(:curvature_result_tensor_work,
        total_work, max_tensor_work)
    _rod_cancel(cancel_check)
    return nothing
end

function _rod_synergy_result_preflight(
    classification::Array{Symbol},
    pair_summaries::Vector{ROSynergyPairSummary};
    max_input_dimension::Integer,
    max_classification_elements::Integer,
    max_output_bytes::Integer,
    max_tensor_work::Integer,
    cancel_check,
)
    _rod_cancel(cancel_check)
    ndims(classification) >= 3 || throw(DimensionMismatch(
        "finite-window synergy labels require output and two input axes"))
    input_dimension = size(classification, ndims(classification))
    size(classification, ndims(classification) - 1) == input_dimension ||
        throw(DimensionMismatch(
            "finite-window synergy input-axis dimensions must agree"))
    input_dimension > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one input axis"))
    _rod_limit(:synergy_result_input_dimension,
        BigInt(input_dimension), max_input_dimension)
    output_axis = ndims(classification) - 2
    output_count = size(classification, output_axis)
    output_count > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one output"))
    cell_shape = ntuple(axis -> size(classification, axis), output_axis - 1)
    total_cells = prod(BigInt, cell_shape)
    classification_elements = BigInt(length(classification))
    _rod_limit(:synergy_result_classification_elements,
        classification_elements, max_classification_elements)
    dimension = BigInt(input_dimension)
    summary_count = BigInt(output_count) *
        (dimension * (dimension - 1) ÷ 2)
    BigInt(length(pair_summaries)) == summary_count ||
        throw(ArgumentError(
            "finite-window synergy summaries must cover every output and axis pair"))
    hash_chunk_bytes = min(classification_elements, BigInt(1 << 20))
    output_bytes =
        classification_elements * BigInt(sizeof(UInt)) +
        8 * summary_count * BigInt(8 * sizeof(Int)) +
        hash_chunk_bytes
    _rod_limit(:synergy_result_output_bytes,
        output_bytes, max_output_bytes)
    total_work =
        4 * classification_elements +
        total_cells * summary_count +
        16 * summary_count
    _rod_limit(:synergy_result_tensor_work,
        total_work, max_tensor_work)
    _rod_cancel(cancel_check)
    return nothing
end

function _rod_validate_pair_summary_components(
    axis_pair::NTuple{2,Int},
    total_face_count::Int,
    evaluated_face_count::Int,
    invalid_face_count::Int,
    violating_face_count::Int,
    max_abs_circulation::Union{Nothing,Float64},
    max_abs_mixed_partial_mismatch::Union{Nothing,Float64},
    max_abs_output_edge_residual::Union{Nothing,Float64},
    max_normalized_residual::Union{Nothing,Float64},
    worst_face_base_index::Vector{Int},
    worst_output_index::Union{Nothing,Int},
)
    0 < axis_pair[1] < axis_pair[2] || throw(ArgumentError(
        "integrability axis_pair must be positive and strictly ordered"))
    total_face_count >= 0 && evaluated_face_count >= 0 &&
        invalid_face_count >= 0 && violating_face_count >= 0 ||
        throw(ArgumentError("integrability face counts must be nonnegative"))
    evaluated_face_count + invalid_face_count == total_face_count ||
        throw(ArgumentError(
            "evaluated and invalid face counts must cover the pair total"))
    violating_face_count <= evaluated_face_count || throw(ArgumentError(
        "violating face count cannot exceed evaluated faces"))
    maxima = (
        max_abs_circulation,
        max_abs_mixed_partial_mismatch,
        max_abs_output_edge_residual,
        max_normalized_residual,
    )
    if evaluated_face_count == 0
        all(isnothing, maxima) || throw(ArgumentError(
            "unevaluated axis pairs cannot publish numeric maxima"))
        isempty(worst_face_base_index) && worst_output_index === nothing ||
            throw(ArgumentError(
                "unevaluated axis pairs cannot publish a worst face"))
    else
        all(value -> value !== nothing && isfinite(value) && value >= 0,
            maxima) || throw(ArgumentError(
                "evaluated axis-pair maxima must be finite and nonnegative"))
        !isempty(worst_face_base_index) && all(>(0), worst_face_base_index) ||
            throw(ArgumentError(
                "evaluated axis pairs require a positive worst-face index"))
        worst_output_index !== nothing && worst_output_index > 0 ||
            throw(ArgumentError(
                "evaluated axis pairs require a positive worst output"))
    end
    return nothing
end

function _rod_pair_summary_sha256(
    axis_pair::NTuple{2,Int},
    total_face_count::Int,
    evaluated_face_count::Int,
    invalid_face_count::Int,
    violating_face_count::Int,
    max_abs_circulation::Union{Nothing,Float64},
    max_abs_mixed_partial_mismatch::Union{Nothing,Float64},
    max_abs_output_edge_residual::Union{Nothing,Float64},
    max_normalized_residual::Union{Nothing,Float64},
    worst_face_base_index::Vector{Int},
    worst_output_index::Union{Nothing,Int},
    ;
    cancel_check=_NO_CANCEL_CHECK,
)
    io = IOBuffer()
    _rod_write_string(io, "bne-ro-integrability-pair-memory/v1")
    for value in axis_pair
        _rod_write_int(io, value)
    end
    for value in (
        total_face_count,
        evaluated_face_count,
        invalid_face_count,
        violating_face_count,
    )
        _rod_write_int(io, value)
    end
    for value in (
        max_abs_circulation,
        max_abs_mixed_partial_mismatch,
        max_abs_output_edge_residual,
        max_normalized_residual,
    )
        _rod_write_optional_float(io, value)
    end
    _rod_write_int(io, length(worst_face_base_index))
    for (position, value) in enumerate(worst_face_base_index)
        _rod_periodic_cancel(cancel_check, position)
        _rod_write_int(io, value)
    end
    write(io, UInt8(worst_output_index === nothing ? 0 : 1))
    worst_output_index === nothing || _rod_write_int(io, worst_output_index)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rod_assert_unchanged(
    summary::ROIntegrabilityPairSummary;
    cancel_check=_NO_CANCEL_CHECK,
)
    actual = _rod_pair_summary_sha256(
        getfield(summary, :axis_pair),
        getfield(summary, :total_face_count),
        getfield(summary, :evaluated_face_count),
        getfield(summary, :invalid_face_count),
        getfield(summary, :violating_face_count),
        getfield(summary, :max_abs_circulation),
        getfield(summary, :max_abs_mixed_partial_mismatch),
        getfield(summary, :max_abs_output_edge_residual),
        getfield(summary, :max_normalized_residual),
        getfield(summary, :worst_face_base_index),
        getfield(summary, :worst_output_index);
        cancel_check,
    )
    actual == getfield(summary, :content_sha256) || throw(ArgumentError(
        "ROIntegrabilityPairSummary backing storage changed after admission"))
    return nothing
end

function Base.getproperty(summary::ROIntegrabilityPairSummary, name::Symbol)
    _rod_assert_unchanged(summary)
    value = getfield(summary, name)
    return name === :worst_face_base_index ? copy(value) : value
end

function _rod_copy_pair_summary(
    summary::ROIntegrabilityPairSummary;
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_assert_unchanged(summary; cancel_check)
    return ROIntegrabilityPairSummary(
        getfield(summary, :axis_pair),
        getfield(summary, :total_face_count),
        getfield(summary, :evaluated_face_count),
        getfield(summary, :invalid_face_count),
        getfield(summary, :violating_face_count),
        getfield(summary, :max_abs_circulation),
        getfield(summary, :max_abs_mixed_partial_mismatch),
        getfield(summary, :max_abs_output_edge_residual),
        getfield(summary, :max_normalized_residual),
        getfield(summary, :worst_face_base_index),
        getfield(summary, :worst_output_index);
        cancel_check,
    )
end

function _rod_validate_integrability_certificate_components(
    status::Symbol,
    complete::Bool,
    input_dimension::Int,
    output_count::Int,
    total_face_count::Int,
    evaluated_face_count::Int,
    invalid_face_count::Int,
    violating_face_count::Int,
    absolute_tolerance::Float64,
    relative_tolerance::Float64,
    pair_summaries::Vector{ROIntegrabilityPairSummary};
    cancel_check=_NO_CANCEL_CHECK,
)
    input_dimension > 0 && output_count > 0 || throw(ArgumentError(
        "integrability certificates require positive input/output counts"))
    isfinite(absolute_tolerance) && absolute_tolerance > 0 ||
        throw(ArgumentError(
            "integrability absolute tolerance must be finite and positive"))
    isfinite(relative_tolerance) && relative_tolerance >= 0 ||
        throw(ArgumentError(
            "integrability relative tolerance must be finite and nonnegative"))
    expected_pair_count = input_dimension * (input_dimension - 1) ÷ 2
    length(pair_summaries) == expected_pair_count || throw(ArgumentError(
        "integrability certificate must cover every unordered input pair"))
    seen_pairs = Set{NTuple{2,Int}}()
    for (position, summary) in enumerate(pair_summaries)
        _rod_periodic_cancel(cancel_check, position)
        _rod_assert_unchanged(summary; cancel_check)
        pair = getfield(summary, :axis_pair)
        pair[2] <= input_dimension || throw(ArgumentError(
            "integrability pair exceeds input_dimension"))
        pair in seen_pairs && throw(ArgumentError(
            "integrability pair summaries must be unique"))
        push!(seen_pairs, pair)
        worst_output = getfield(summary, :worst_output_index)
        worst_output === nothing || worst_output <= output_count ||
            throw(ArgumentError(
                "integrability worst output exceeds output_count"))
        worst_face = getfield(summary, :worst_face_base_index)
        isempty(worst_face) || length(worst_face) == input_dimension ||
            throw(ArgumentError(
                "integrability worst-face index rank is inconsistent"))
    end
    sum(summary -> getfield(summary, :total_face_count), pair_summaries;
        init=0) ==
        total_face_count || throw(ArgumentError(
            "integrability pair totals do not match total_face_count"))
    sum(summary -> getfield(summary, :evaluated_face_count), pair_summaries;
        init=0) ==
        evaluated_face_count || throw(ArgumentError(
            "integrability pair totals do not match evaluated_face_count"))
    sum(summary -> getfield(summary, :invalid_face_count), pair_summaries;
        init=0) ==
        invalid_face_count || throw(ArgumentError(
            "integrability pair totals do not match invalid_face_count"))
    sum(summary -> getfield(summary, :violating_face_count), pair_summaries;
        init=0) ==
        violating_face_count || throw(ArgumentError(
            "integrability pair totals do not match violating_face_count"))
    expected_complete = total_face_count > 0 && invalid_face_count == 0
    complete == expected_complete || throw(ArgumentError(
        "integrability complete flag is inconsistent with face coverage"))
    expected_status = if total_face_count == 0
        :insufficient_grid
    elseif violating_face_count > 0
        :discrete_inconsistent
    elseif invalid_face_count > 0
        :unknown_gap
    else
        :consistent_on_tested_grid
    end
    status === expected_status || throw(ArgumentError(
        "integrability status is inconsistent with face counts"))
    return nothing
end

function _rod_integrability_certificate_sha256(
    status::Symbol,
    complete::Bool,
    input_dimension::Int,
    output_count::Int,
    total_face_count::Int,
    evaluated_face_count::Int,
    invalid_face_count::Int,
    violating_face_count::Int,
    absolute_tolerance::Float64,
    relative_tolerance::Float64,
    pair_summaries::Vector{ROIntegrabilityPairSummary};
    cancel_check=_NO_CANCEL_CHECK,
)
    io = IOBuffer()
    _rod_write_string(io, "bne-ro-integrability-certificate-memory/v1")
    _rod_write_string(io, String(status))
    write(io, UInt8(complete))
    for value in (
        input_dimension,
        output_count,
        total_face_count,
        evaluated_face_count,
        invalid_face_count,
        violating_face_count,
    )
        _rod_write_int(io, value)
    end
    _rod_write_float(io, absolute_tolerance)
    _rod_write_float(io, relative_tolerance)
    _rod_write_int(io, length(pair_summaries))
    for (position, summary) in enumerate(pair_summaries)
        _rod_periodic_cancel(cancel_check, position)
        _rod_assert_unchanged(summary; cancel_check)
        _rod_write_string(io, getfield(summary, :content_sha256))
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rod_assert_unchanged(
    certificate::RODiscreteIntegrabilityCertificate,
)
    actual = _rod_integrability_certificate_sha256(
        getfield(certificate, :status),
        getfield(certificate, :complete),
        getfield(certificate, :input_dimension),
        getfield(certificate, :output_count),
        getfield(certificate, :total_face_count),
        getfield(certificate, :evaluated_face_count),
        getfield(certificate, :invalid_face_count),
        getfield(certificate, :violating_face_count),
        getfield(certificate, :absolute_tolerance),
        getfield(certificate, :relative_tolerance),
        getfield(certificate, :pair_summaries),
    )
    actual == getfield(certificate, :content_sha256) || throw(ArgumentError(
        "RODiscreteIntegrabilityCertificate backing storage changed after admission"))
    return nothing
end

function Base.getproperty(
    certificate::RODiscreteIntegrabilityCertificate,
    name::Symbol,
)
    _rod_assert_unchanged(certificate)
    value = getfield(certificate, name)
    if name === :pair_summaries
        return [_rod_copy_pair_summary(summary) for summary in value]
    end
    return value
end

function _rod_validate_curvature_components(
    status::Symbol,
    complete::Bool,
    cell_shape::Vector{Int},
    total_cell_count::Int,
    evaluated_cell_count::Int,
    invalid_cell_count::Int,
    validity::BitArray,
    gradient_jacobian::Array{Float64},
    symmetric_hessian::Array{Float64},
    mixed_output_curvature::Array{Float64},
    antisymmetry_residual::Array{Float64},
    hessian_eigenvalues::Array{Float64};
    cancel_check=_NO_CANCEL_CHECK,
)
    input_dimension = length(cell_shape)
    input_dimension > 0 || throw(ArgumentError(
        "curvature evidence requires at least one input axis"))
    all(>=(0), cell_shape) || throw(ArgumentError(
        "curvature cell extents must be nonnegative"))
    total_cell_count >= 0 && evaluated_cell_count >= 0 &&
        invalid_cell_count >= 0 || throw(ArgumentError(
            "curvature cell counts must be nonnegative"))
    BigInt(total_cell_count) == prod(BigInt, cell_shape) ||
        throw(ArgumentError(
            "curvature total_cell_count does not match cell_shape"))
    evaluated_cell_count + invalid_cell_count == total_cell_count ||
        throw(ArgumentError(
            "evaluated and invalid curvature cells must cover the total"))
    size(validity) == Tuple(cell_shape) || throw(DimensionMismatch(
        "curvature validity shape must agree with cell_shape"))
    count(validity) == evaluated_cell_count || throw(ArgumentError(
        "curvature validity mask does not match evaluated_cell_count"))

    tensor_rank = input_dimension + 3
    ndims(gradient_jacobian) == tensor_rank || throw(DimensionMismatch(
        "curvature tensors must include cell, output, and two input axes"))
    output_axis = input_dimension + 1
    output_count = size(gradient_jacobian, output_axis)
    output_count > 0 || throw(ArgumentError(
        "curvature evidence requires at least one output"))
    tensor_shape = (Tuple(cell_shape)..., output_count,
        input_dimension, input_dimension)
    scalar_shape = (Tuple(cell_shape)..., output_count)
    eigen_shape = (Tuple(cell_shape)..., output_count, input_dimension)
    size(gradient_jacobian) == tensor_shape || throw(DimensionMismatch(
        "gradient_jacobian shape is inconsistent"))
    size(symmetric_hessian) == tensor_shape || throw(DimensionMismatch(
        "symmetric_hessian shape is inconsistent"))
    size(mixed_output_curvature) == tensor_shape || throw(DimensionMismatch(
        "mixed_output_curvature shape is inconsistent"))
    size(antisymmetry_residual) == scalar_shape || throw(DimensionMismatch(
        "antisymmetry_residual shape is inconsistent"))
    size(hessian_eigenvalues) == eigen_shape || throw(DimensionMismatch(
        "hessian_eigenvalues shape is inconsistent"))

    expected_complete = total_cell_count > 0 && invalid_cell_count == 0
    complete == expected_complete || throw(ArgumentError(
        "curvature complete flag is inconsistent with cell coverage"))
    expected_status = if total_cell_count == 0
        :insufficient_grid
    elseif evaluated_cell_count == 0
        :no_valid_cells
    elseif invalid_cell_count == 0
        :complete
    else
        :partial
    end
    status === expected_status || throw(ArgumentError(
        "curvature status is inconsistent with cell coverage"))

    validation_position = 0
    for cell_index in CartesianIndices(Tuple(cell_shape)),
        output in 1:output_count
        validation_position += 1
        _rod_periodic_cancel(cancel_check, validation_position)
        point = Tuple(cell_index)
        gradient = @view gradient_jacobian[point..., output, :, :]
        symmetric = @view symmetric_hessian[point..., output, :, :]
        mixed = @view mixed_output_curvature[point..., output, :, :]
        eigenvalues = @view hessian_eigenvalues[point..., output, :]
        residual = antisymmetry_residual[point..., output]
        if validity[cell_index]
            all(isfinite, gradient) && all(isfinite, symmetric) &&
                all(isfinite, eigenvalues) && isfinite(residual) &&
                residual >= 0 || throw(ArgumentError(
                    "valid curvature cells must contain finite derivatives"))
            issymmetric(symmetric) || throw(ArgumentError(
                "valid symmetric_hessian slices must be symmetric"))
            issorted(eigenvalues) || throw(ArgumentError(
                "valid Hessian eigenvalues must be ordered"))
            # The eigenvalue vector is evidence derived from this exact
            # admitted Hessian, not an independently trusted caller claim.
            # Replaying the same dense symmetric solver binds ordering and
            # multiplicity as well as the numeric values.
            _rod_cancel(cancel_check)
            expected_eigenvalues = eigvals(Symmetric(Matrix(symmetric)))
            _rod_cancel(cancel_check)
            isequal(collect(eigenvalues), expected_eigenvalues) ||
                throw(ArgumentError(
                    "hessian_eigenvalues do not replay from symmetric_hessian"))
            expected_residual = 0.0
            for component_axis in 1:input_dimension,
                derivative_axis in 1:input_dimension
                validation_position += 1
                _rod_periodic_cancel(cancel_check, validation_position)
                expected_symmetric = (
                    gradient[component_axis, derivative_axis] +
                    gradient[derivative_axis, component_axis]) / 2
                isequal(
                    symmetric[component_axis, derivative_axis],
                    expected_symmetric,
                ) || throw(ArgumentError(
                    "symmetric_hessian does not match gradient_jacobian"))
                expected_residual = max(expected_residual, abs(
                    gradient[component_axis, derivative_axis] -
                    gradient[derivative_axis, component_axis]))
            end
            isequal(residual, expected_residual) || throw(ArgumentError(
                "antisymmetry_residual does not match gradient_jacobian"))
            for left_axis in 1:input_dimension,
                right_axis in 1:input_dimension
                validation_position += 1
                _rod_periodic_cancel(cancel_check, validation_position)
                value = mixed[left_axis, right_axis]
                if left_axis == right_axis
                    isnan(value) || throw(ArgumentError(
                        "mixed-output curvature diagonals must be NaN"))
                else
                    isfinite(value) &&
                        isequal(value, mixed[right_axis, left_axis]) ||
                        throw(ArgumentError(
                            "mixed-output curvature must be finite and symmetric off diagonal"))
                end
            end
        else
            all(isnan, gradient) && all(isnan, symmetric) &&
                all(isnan, mixed) && isnan(residual) &&
                all(isnan, eigenvalues) || throw(ArgumentError(
                    "invalid curvature cells must remain NaN gaps"))
        end
    end
    return nothing
end

function _rod_curvature_sha256(
    status::Symbol,
    complete::Bool,
    cell_shape::Vector{Int},
    total_cell_count::Int,
    evaluated_cell_count::Int,
    invalid_cell_count::Int,
    validity::BitArray,
    gradient_jacobian::Array{Float64},
    symmetric_hessian::Array{Float64},
    mixed_output_curvature::Array{Float64},
    antisymmetry_residual::Array{Float64},
    hessian_eigenvalues::Array{Float64};
    cancel_check=_NO_CANCEL_CHECK,
)
    io = IOBuffer()
    _rod_write_string(io, "bne-ro-finite-difference-curvature-memory/v1")
    _rod_write_string(io, String(status))
    write(io, UInt8(complete))
    _rod_write_int(io, length(cell_shape))
    for extent in cell_shape
        _rod_write_int(io, extent)
    end
    for count_value in (
        total_cell_count,
        evaluated_cell_count,
        invalid_cell_count,
    )
        _rod_write_int(io, count_value)
    end
    _rod_write_bit_array(io, validity; cancel_check)
    for values in (
        gradient_jacobian,
        symmetric_hessian,
        mixed_output_curvature,
        antisymmetry_residual,
        hessian_eigenvalues,
    )
        _rod_write_float_array(io, values; cancel_check)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rod_assert_unchanged(
    curvature::ROFiniteDifferenceCurvature;
    cancel_check=_NO_CANCEL_CHECK,
)
    actual = _rod_curvature_sha256(
        getfield(curvature, :status),
        getfield(curvature, :complete),
        getfield(curvature, :cell_shape),
        getfield(curvature, :total_cell_count),
        getfield(curvature, :evaluated_cell_count),
        getfield(curvature, :invalid_cell_count),
        getfield(curvature, :validity),
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        getfield(curvature, :mixed_output_curvature),
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues);
        cancel_check,
    )
    actual == getfield(curvature, :content_sha256) || throw(ArgumentError(
        "ROFiniteDifferenceCurvature backing storage changed after admission"))
    return nothing
end

function Base.getproperty(curvature::ROFiniteDifferenceCurvature, name::Symbol)
    _rod_assert_unchanged(curvature)
    value = getfield(curvature, name)
    if name === :cell_shape || name === :validity ||
            name === :gradient_jacobian || name === :symmetric_hessian ||
            name === :mixed_output_curvature ||
            name === :antisymmetry_residual ||
            name === :hessian_eigenvalues
        return copy(value)
    end
    return value
end

const _ROD_SYNERGY_LABELS = (
    :not_applicable,
    :unknown_gap,
    :synergistic_under_policy,
    :antagonistic_under_policy,
    :neutral_under_policy,
)

@inline function _rod_synergy_label_code(label::Symbol)
    position = findfirst(==(label), _ROD_SYNERGY_LABELS)
    position === nothing && throw(ArgumentError(
        "unsupported finite-window synergy label"))
    return UInt8(position)
end

function _rod_validate_synergy_components(
    policy::Symbol,
    threshold::Float64,
    classification::Array{Symbol},
    pair_summaries::Vector{ROSynergyPairSummary};
    cancel_check=_NO_CANCEL_CHECK,
)
    policy === :positive_log_cross_curvature || throw(ArgumentError(
        "unsupported finite-window synergy policy"))
    isfinite(threshold) && threshold >= 0 || throw(ArgumentError(
        "finite-window synergy threshold must be finite and nonnegative"))
    ndims(classification) >= 3 || throw(DimensionMismatch(
        "finite-window synergy labels require output and two input axes"))
    input_dimension = size(classification, ndims(classification))
    size(classification, ndims(classification) - 1) == input_dimension ||
        throw(DimensionMismatch(
            "finite-window synergy input-axis dimensions must agree"))
    input_dimension > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one input axis"))
    output_axis = ndims(classification) - 2
    output_count = size(classification, output_axis)
    output_count > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one output"))
    cell_shape = ntuple(axis -> size(classification, axis), output_axis - 1)
    total_cells = prod(Int, cell_shape)

    allowed_off_diagonal = (
        :unknown_gap,
        :synergistic_under_policy,
        :antagonistic_under_policy,
        :neutral_under_policy,
    )
    validation_position = 0
    for cell_index in CartesianIndices(cell_shape), output in 1:output_count,
        left_axis in 1:input_dimension,
        right_axis in 1:input_dimension
        validation_position += 1
        _rod_periodic_cancel(cancel_check, validation_position)
        point = Tuple(cell_index)
        label = classification[
            point..., output, left_axis, right_axis]
        if left_axis == right_axis
            label === :not_applicable || throw(ArgumentError(
                "finite-window synergy diagonals must be not_applicable"))
        else
            label in allowed_off_diagonal || throw(ArgumentError(
                "unsupported off-diagonal finite-window synergy label"))
            label === classification[
                point..., output, right_axis, left_axis] ||
                throw(ArgumentError(
                    "finite-window synergy labels must be symmetric"))
        end
    end

    expected_summary_count = output_count *
        (input_dimension * (input_dimension - 1) ÷ 2)
    length(pair_summaries) == expected_summary_count || throw(ArgumentError(
        "finite-window synergy summaries must cover every output and axis pair"))
    seen = Set{Tuple{Int,NTuple{2,Int}}}()
    for (summary_position, summary) in enumerate(pair_summaries)
        _rod_periodic_cancel(cancel_check, summary_position)
        pair = summary.axis_pair
        0 < pair[1] < pair[2] <= input_dimension || throw(ArgumentError(
            "finite-window synergy axis pairs must be ordered and in range"))
        0 < summary.output_index <= output_count || throw(ArgumentError(
            "finite-window synergy output index is out of range"))
        key = (summary.output_index, pair)
        key in seen && throw(ArgumentError(
            "finite-window synergy summaries must be unique"))
        push!(seen, key)
        counts = (
            summary.evaluated_cell_count,
            summary.positive_count,
            summary.negative_count,
            summary.neutral_count,
            summary.unknown_gap_count,
        )
        all(>=(0), counts) || throw(ArgumentError(
            "finite-window synergy counts must be nonnegative"))
        summary.positive_count + summary.negative_count +
            summary.neutral_count == summary.evaluated_cell_count ||
            throw(ArgumentError(
                "finite-window synergy evaluated count is inconsistent"))
        summary.evaluated_cell_count + summary.unknown_gap_count ==
            total_cells || throw(ArgumentError(
                "finite-window synergy counts must cover every cell"))

        observed = Dict(
            :synergistic_under_policy => 0,
            :antagonistic_under_policy => 0,
            :neutral_under_policy => 0,
            :unknown_gap => 0,
        )
        for (cell_position, cell_index) in
                enumerate(CartesianIndices(cell_shape))
            _rod_periodic_cancel(cancel_check, cell_position)
            point = Tuple(cell_index)
            label = classification[
                point..., summary.output_index, pair[1], pair[2]]
            observed[label] += 1
        end
        observed[:synergistic_under_policy] == summary.positive_count &&
            observed[:antagonistic_under_policy] == summary.negative_count &&
            observed[:neutral_under_policy] == summary.neutral_count &&
            observed[:unknown_gap] == summary.unknown_gap_count ||
            throw(ArgumentError(
                "finite-window synergy summaries do not match labels"))
    end
    return nothing
end

function _rod_synergy_sha256(
    policy::Symbol,
    threshold::Float64,
    classification::Array{Symbol},
    pair_summaries::Vector{ROSynergyPairSummary};
    cancel_check=_NO_CANCEL_CHECK,
)
    io = IOBuffer()
    _rod_write_string(io, "bne-ro-finite-window-synergy-memory/v1")
    _rod_write_string(io, String(policy))
    _rod_write_float(io, threshold)
    _rod_write_int(io, ndims(classification))
    for extent in size(classification)
        _rod_write_int(io, extent)
    end
    label_chunk_hashes = IOBuffer()
    chunk_size = 1 << 20
    for first_label in 1:chunk_size:length(classification)
        _rod_cancel(cancel_check)
        last_label = min(
            first_label + chunk_size - 1, length(classification))
        labels = Vector{UInt8}(undef, last_label - first_label + 1)
        for (chunk_position, source_position) in
                enumerate(first_label:last_label)
            _rod_periodic_cancel(cancel_check, chunk_position)
            labels[chunk_position] = _rod_synergy_label_code(
                classification[source_position])
        end
        write(label_chunk_hashes, SHA.sha256(labels))
    end
    _rod_cancel(cancel_check)
    write(io, SHA.sha256(take!(label_chunk_hashes)))
    _rod_write_int(io, length(pair_summaries))
    for (summary_position, summary) in enumerate(pair_summaries)
        _rod_periodic_cancel(cancel_check, summary_position)
        for axis in summary.axis_pair
            _rod_write_int(io, axis)
        end
        for count_value in (
            summary.output_index,
            summary.evaluated_cell_count,
            summary.positive_count,
            summary.negative_count,
            summary.neutral_count,
            summary.unknown_gap_count,
        )
            _rod_write_int(io, count_value)
        end
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rod_assert_unchanged(
    result::ROFiniteWindowSynergy;
    cancel_check=_NO_CANCEL_CHECK,
)
    actual = _rod_synergy_sha256(
        getfield(result, :policy),
        getfield(result, :threshold),
        getfield(result, :classification),
        getfield(result, :pair_summaries);
        cancel_check,
    )
    actual == getfield(result, :content_sha256) || throw(ArgumentError(
        "ROFiniteWindowSynergy backing storage changed after admission"))
    return nothing
end

function Base.getproperty(result::ROFiniteWindowSynergy, name::Symbol)
    _rod_assert_unchanged(result)
    value = getfield(result, name)
    if name === :classification || name === :pair_summaries
        return copy(value)
    end
    return value
end

@inline function _rod_limit(phase::Symbol, requested::BigInt, limit::Integer)
    limit > 0 || throw(ArgumentError("$phase limit must be positive"))
    limit <= typemax(Int) || throw(ArgumentError("$phase limit must fit in Int"))
    requested <= limit || throw(RODifferentialLimitExceeded(
        phase, requested, Int(limit)))
    return nothing
end

@inline function _rod_cancel(cancel_check)
    cancel_check()
    return nothing
end

@inline function _rod_periodic_cancel(cancel_check, position::Integer)
    (position - 1) % 256 == 0 && _rod_cancel(cancel_check)
    return nothing
end

function _rod_sampled_field_dimensions(
    field::SampledReactionOrderField;
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_cancel(cancel_check)
    input_dimension = length(field.axis_indices)
    output_count = length(field.output_indices)
    input_dimension > 0 || throw(ArgumentError(
        "sampled RO-field differential analysis requires at least one input axis"))
    output_count > 0 || throw(ArgumentError(
        "sampled RO-field differential analysis requires at least one output"))
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
        _rod_cancel(cancel_check)
        coordinates = field.axis_coordinates_log10[axis]
        length(coordinates) == grid_shape[axis] || throw(DimensionMismatch(
            "axis $axis coordinate count does not match the grid shape"))
        for index in eachindex(coordinates)
            _rod_periodic_cancel(cancel_check, index)
            isfinite(coordinates[index]) || throw(ArgumentError(
                "axis $axis coordinates must be finite"))
            if index > firstindex(coordinates)
                coordinates[index - 1] < coordinates[index] ||
                    throw(ArgumentError(
                        "axis $axis coordinates must be strictly increasing"))
            end
        end
    end
    return input_dimension, output_count, grid_shape
end

@inline function _rod_point_is_finite(
    field,
    point_index,
    output_count,
    input_dimension,
    cancel_check,
)
    @inbounds for output in 1:output_count
        _rod_periodic_cancel(cancel_check, output)
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

function _rod_face_count(
    grid_shape,
    left_axis::Int,
    right_axis::Int;
    cancel_check=_NO_CANCEL_CHECK,
)
    count = BigInt(1)
    for axis in eachindex(grid_shape)
        _rod_periodic_cancel(cancel_check, axis)
        extent = axis == left_axis || axis == right_axis ?
            max(grid_shape[axis] - 1, 0) : grid_shape[axis]
        count *= BigInt(extent)
    end
    return count
end

@inline function _rod_axis_pair_count(input_dimension::Int)
    return BigInt(input_dimension) * max(BigInt(input_dimension) - 1, 0) ÷ 2
end

function _rod_integrability_tensor_work(
    total_faces::BigInt,
    axis_pair_count::BigInt,
    input_dimension::Int,
    output_count::Int,
)
    # Per face: four validity reads; for every output, four corners each scan
    # one output plus all RO components, then the diagnostics consume eight RO
    # values and four output values. This intentionally over-counts reused
    # values so the limit is conservative and independent of implementation
    # caching.
    dimension = BigInt(input_dimension)
    # Each pair summary is sealed once and then detached, revalidated, and
    # re-sealed into the published certificate. Count those linear-in-D
    # passes, along with the fixed summary fields, instead of treating result
    # admission as free work.
    pair_axis_work = 6 * axis_pair_count * (dimension + 12)
    face_work = total_faces * (
        4 + BigInt(output_count) * (4 * (dimension + 1) + 12))
    return pair_axis_work + face_work
end

"""
    certify_sampled_ro_integrability(field; absolute_tolerance=1e-8,
        relative_tolerance=1e-6, max_faces=100_000,
        max_face_output_evaluations=1_000_000, max_axis_pairs=100_000,
        max_tensor_work=100_000_000, cancel_check=_NO_CANCEL_CHECK)

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
    max_axis_pairs::Integer=100_000,
    max_tensor_work::Integer=100_000_000,
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_cancel(cancel_check)
    atol = Float64(absolute_tolerance)
    rtol = Float64(relative_tolerance)
    isfinite(atol) && atol > 0 || throw(ArgumentError(
        "absolute_tolerance must be finite and positive"))
    isfinite(rtol) && rtol >= 0 || throw(ArgumentError(
        "relative_tolerance must be finite and nonnegative"))
    input_dimension, output_count, grid_shape =
        _rod_sampled_field_dimensions(field; cancel_check)

    axis_pair_count_big = _rod_axis_pair_count(input_dimension)
    _rod_limit(:axis_pairs, axis_pair_count_big, max_axis_pairs)
    _rod_limit(:integrability_tensor_work,
        _rod_integrability_tensor_work(
            BigInt(0), axis_pair_count_big,
            input_dimension, output_count),
        max_tensor_work)
    total_faces_big = BigInt(0)
    pair_face_counts = Dict{NTuple{2,Int},BigInt}()
    for left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        _rod_cancel(cancel_check)
        pair = (left_axis, right_axis)
        count = _rod_face_count(grid_shape, pair...; cancel_check)
        pair_face_counts[pair] = count
        total_faces_big += count
    end
    _rod_limit(:faces, total_faces_big, max_faces)
    _rod_limit(:face_output_evaluations,
        total_faces_big * BigInt(output_count),
        max_face_output_evaluations)
    _rod_limit(:integrability_tensor_work,
        _rod_integrability_tensor_work(
            total_faces_big, axis_pair_count_big,
            input_dimension, output_count),
        max_tensor_work)
    _rod_cancel(cancel_check)

    pair_summaries = ROIntegrabilityPairSummary[]
    total_evaluated = 0
    total_invalid = 0
    total_violating = 0

    for left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        _rod_cancel(cancel_check)
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
            _rod_cancel(cancel_check)
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
                field, point, output_count, input_dimension, cancel_check),
                corners) ||
                throw(ArgumentError(
                    "a valid sampled-field point contains non-finite data"))

            delta_left = field.axis_coordinates_log10[left_axis][base[left_axis] + 1] -
                field.axis_coordinates_log10[left_axis][base[left_axis]]
            delta_right = field.axis_coordinates_log10[right_axis][base[right_axis] + 1] -
                field.axis_coordinates_log10[right_axis][base[right_axis]]
            evaluated += 1
            face_violates = false

            for output in 1:output_count
                _rod_periodic_cancel(cancel_check, output)
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
    _rod_cancel(cancel_check)

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
        pair_summaries;
        cancel_check,
    )
end

function _rod_cell_count(grid_shape)
    return prod(BigInt(max(extent - 1, 0)) for extent in grid_shape)
end

function _rod_curvature_tensor_elements(
    total_cells::BigInt,
    input_dimension::Int,
    output_count::Int,
)
    dimension = BigInt(input_dimension)
    outputs = BigInt(output_count)
    # Three cell/output/D/D tensors, one cell/output scalar tensor, one
    # cell/output/D eigenvalue tensor, and the cell-validity mask.
    return total_cells * (
        outputs * (3 * dimension * dimension + 1 + dimension) + 1)
end

function _rod_curvature_tensor_work(
    total_cells::BigInt,
    corners_per_cell::BigInt,
    input_dimension::Int,
    output_count::Int,
)
    dimension = BigInt(input_dimension)
    outputs = BigInt(output_count)
    axis_pairs = _rod_axis_pair_count(input_dimension)
    # Conservative scalar-work envelope: finite-value scans, all component x
    # derivative edge differences, every mixed-output contrast, dense Hessian
    # symmetrization/residual work, and the cubic symmetric eigensolve scale.
    per_cell_output =
        corners_per_cell * (dimension + 1) +
        corners_per_cell * dimension * dimension +
        corners_per_cell * axis_pairs +
        3 * dimension * dimension + dimension^3
    evidence_elements_per_cell =
        outputs * (3 * dimension * dimension + 1 + dimension) + 1
    # Published evidence is detached, semantically validated, and hashed.
    # Six scalar passes account for bounded admission operations; the extra
    # cubic term covers authoritative eigenvalue replay from every admitted
    # symmetric Hessian.
    admission_work = 6 * evidence_elements_per_cell +
        outputs * dimension^3
    return total_cells * (
        corners_per_cell + outputs * per_cell_output + admission_work)
end

@inline function _rod_corner_index(base, mask::Int, dimension::Int)
    return ntuple(axis -> base[axis] + ((mask >> (axis - 1)) & 1),
        dimension)
end

"""
    estimate_sampled_ro_curvature(field; max_cells=100_000,
        max_corner_visits=1_000_000, max_tensor_elements=20_000_000,
        max_tensor_work=250_000_000, cancel_check=_NO_CANCEL_CHECK)

Estimate the cell-centred gradient Jacobian, its symmetric Hessian projection,
eigenvalues, and mixed finite-window output curvature on a non-uniform
Cartesian grid. These quantities are coordinate- and window-dependent
diagnostics; they are not causal or mechanism-specific synergy claims.
"""
function estimate_sampled_ro_curvature(
    field::SampledReactionOrderField;
    max_cells::Integer=100_000,
    max_corner_visits::Integer=1_000_000,
    max_tensor_elements::Integer=20_000_000,
    max_tensor_work::Integer=250_000_000,
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_cancel(cancel_check)
    input_dimension, output_count, grid_shape =
        _rod_sampled_field_dimensions(field; cancel_check)
    cell_shape = Int[max(extent - 1, 0) for extent in grid_shape]
    total_cells_big = _rod_cell_count(grid_shape)
    corners_per_cell_big = BigInt(2)^input_dimension
    _rod_limit(:curvature_cells, total_cells_big, max_cells)
    _rod_limit(:curvature_corner_visits,
        total_cells_big * corners_per_cell_big,
        max_corner_visits)
    _rod_limit(:curvature_tensor_elements,
        _rod_curvature_tensor_elements(
            total_cells_big, input_dimension, output_count),
        max_tensor_elements)
    _rod_limit(:curvature_tensor_work,
        _rod_curvature_tensor_work(
            total_cells_big, corners_per_cell_big,
            input_dimension, output_count),
        max_tensor_work)
    total_cells = Int(total_cells_big)
    _rod_cancel(cancel_check)

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
        antisymmetry_residual, hessian_eigenvalues; cancel_check)

    corner_count = Int(corners_per_cell_big)
    evaluated = 0
    invalid = 0
    for cell_index in CartesianIndices(Tuple(cell_shape))
        _rod_cancel(cancel_check)
        base = Tuple(cell_index)
        corners = Vector{typeof(base)}(undef, corner_count)
        for mask in 0:(corner_count - 1)
            _rod_periodic_cancel(cancel_check, mask + 1)
            corners[mask + 1] = _rod_corner_index(
                base, mask, input_dimension)
        end
        cell_is_valid = true
        for (position, point) in enumerate(corners)
            _rod_periodic_cancel(cancel_check, position)
            if !field.validity[point...]
                cell_is_valid = false
                break
            end
        end
        if !cell_is_valid
            invalid += 1
            continue
        end
        all(point -> _rod_point_is_finite(
            field, point, output_count, input_dimension, cancel_check),
            corners) ||
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
            _rod_periodic_cancel(cancel_check, output)
            jacobian = Matrix{Float64}(undef,
                input_dimension, input_dimension)
            edge_visit_count = 0
            for component_axis in 1:input_dimension,
                derivative_axis in 1:input_dimension
                _rod_cancel(cancel_check)
                edge_sum = 0.0
                edge_count = 0
                for mask in 0:(corner_count - 1)
                    ((mask >> (derivative_axis - 1)) & 1) == 0 || continue
                    edge_visit_count += 1
                    _rod_periodic_cancel(cancel_check, edge_visit_count)
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
            _rod_cancel(cancel_check)
            symmetric = (jacobian + transpose(jacobian)) / 2
            _rod_cancel(cancel_check)
            @views gradient_jacobian[base..., output, :, :] .= jacobian
            @views symmetric_hessian[base..., output, :, :] .= symmetric
            antisymmetry_residual[base..., output] =
                maximum(abs, jacobian - transpose(jacobian); init=0.0)
            # LAPACK's dense symmetric eigensolve is one bounded blocking call;
            # checkpoints immediately before and after keep cancellation
            # cooperative at the available library boundary.
            _rod_cancel(cancel_check)
            @views hessian_eigenvalues[base..., output, :] .=
                eigvals(Symmetric(symmetric))
            _rod_cancel(cancel_check)

            contrast_visit_count = 0
            for left_axis in 1:max(input_dimension - 1, 0),
                right_axis in (left_axis + 1):input_dimension
                _rod_cancel(cancel_check)
                contrast_sum = 0.0
                contrast_count = 0
                for mask in 0:(corner_count - 1)
                    ((mask >> (left_axis - 1)) & 1) == 0 || continue
                    ((mask >> (right_axis - 1)) & 1) == 0 || continue
                    contrast_visit_count += 1
                    _rod_periodic_cancel(cancel_check, contrast_visit_count)
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
            _rod_cancel(cancel_check)
        end
    end
    _rod_cancel(cancel_check)

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
        hessian_eigenvalues;
        cancel_check,
    )
end

function _rod_synergy_preflight(
    curvature::ROFiniteDifferenceCurvature;
    max_input_dimension::Integer,
    max_tensor_elements::Integer,
    max_classification_elements::Integer,
    max_output_bytes::Integer,
    max_tensor_work::Integer,
    cancel_check,
)
    _rod_cancel(cancel_check)
    cell_shape = getfield(curvature, :cell_shape)
    input_dimension = length(cell_shape)
    input_dimension > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one input axis"))
    _rod_limit(:synergy_input_dimension,
        BigInt(input_dimension), max_input_dimension)
    all(>=(0), cell_shape) || throw(ArgumentError(
        "finite-window synergy cell extents must be nonnegative"))

    mixed_output_curvature = getfield(
        curvature, :mixed_output_curvature)
    ndims(mixed_output_curvature) == input_dimension + 3 ||
        throw(DimensionMismatch(
            "mixed_output_curvature rank is inconsistent with cell_shape"))
    output_axis = input_dimension + 1
    output_count = size(mixed_output_curvature, output_axis)
    output_count > 0 || throw(ArgumentError(
        "finite-window synergy requires at least one output"))
    expected_shape = (Tuple(cell_shape)..., output_count,
        input_dimension, input_dimension)
    size(mixed_output_curvature) == expected_shape ||
        throw(DimensionMismatch(
            "mixed_output_curvature shape is inconsistent"))

    input_tensor_elements = BigInt(length(
        getfield(curvature, :validity)))
    for values in (
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        mixed_output_curvature,
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues),
    )
        _rod_cancel(cancel_check)
        input_tensor_elements += BigInt(length(values))
    end
    _rod_limit(:synergy_input_tensor_elements,
        input_tensor_elements, max_tensor_elements)

    total_cells = prod(BigInt, cell_shape)
    dimension = BigInt(input_dimension)
    outputs = BigInt(output_count)
    axis_pairs = dimension * (dimension - 1) ÷ 2
    classification_elements =
        total_cells * outputs * dimension * dimension
    classification_elements == BigInt(length(mixed_output_curvature)) ||
        throw(DimensionMismatch(
            "mixed_output_curvature element count is inconsistent"))
    _rod_limit(:synergy_classification_elements,
        classification_elements, max_classification_elements)

    summary_count = outputs * axis_pairs
    symbol_storage_bytes = BigInt(sizeof(UInt))
    summary_storage_bytes = BigInt(8 * sizeof(Int))
    hash_chunk_bytes = min(classification_elements, BigInt(1 << 20))
    # Peak publication memory holds the working output and its detached sealed
    # copy, summary vectors plus validation-set overhead, and one bounded
    # label-hash chunk. The eight-summary factor deliberately over-budgets
    # Julia container metadata rather than counting only isbits payloads.
    output_bytes =
        2 * classification_elements * symbol_storage_bytes +
        8 * summary_count * summary_storage_bytes +
        hash_chunk_bytes
    _rod_limit(:synergy_output_bytes, output_bytes, max_output_bytes)

    input_revalidation_work =
        8 * input_tensor_elements +
        total_cells * outputs * dimension^3
    diagonal_work = total_cells * outputs * dimension
    pair_cell_work = total_cells * summary_count
    publication_work =
        5 * classification_elements +
        diagonal_work +
        8 * pair_cell_work +
        pair_cell_work +
        16 * summary_count
    total_work = input_revalidation_work + publication_work
    _rod_limit(:synergy_tensor_work, total_work, max_tensor_work)
    _rod_cancel(cancel_check)

    return (
        cell_shape=cell_shape,
        input_dimension=input_dimension,
        output_count=output_count,
        expected_shape=expected_shape,
        summary_count=Int(summary_count),
    )
end

"""
    classify_finite_window_synergy(curvature; threshold=1e-8,
        max_input_dimension=64, max_tensor_elements=20_000_000,
        max_classification_elements=10_000_000,
        max_output_bytes=256_000_000, max_tensor_work=250_000_000,
        cancel_check=_NO_CANCEL_CHECK)

Apply the explicit positive-log-cross-curvature convention to an already
computed finite-difference result. No missing value is coerced to zero and no
continuum, causal, or mechanism-specific claim is made. All input replay,
classification allocation, cell/pair work, and result admission are covered
by BigInt preflight limits before any output tensor is allocated.
"""
function classify_finite_window_synergy(
    curvature::ROFiniteDifferenceCurvature;
    threshold::Real=1e-8,
    max_input_dimension::Integer=64,
    max_tensor_elements::Integer=20_000_000,
    max_classification_elements::Integer=10_000_000,
    max_output_bytes::Integer=256_000_000,
    max_tensor_work::Integer=250_000_000,
    cancel_check=_NO_CANCEL_CHECK,
)
    _rod_cancel(cancel_check)
    cutoff = Float64(threshold)
    isfinite(cutoff) && cutoff >= 0 || throw(ArgumentError(
        "synergy threshold must be finite and nonnegative"))
    preflight = _rod_synergy_preflight(
        curvature;
        max_input_dimension,
        max_tensor_elements,
        max_classification_elements,
        max_output_bytes,
        max_tensor_work,
        cancel_check,
    )
    _rod_assert_unchanged(curvature; cancel_check)
    cell_shape = preflight.cell_shape
    validity = getfield(curvature, :validity)
    mixed_output_curvature = getfield(
        curvature, :mixed_output_curvature)
    _rod_validate_curvature_components(
        getfield(curvature, :status),
        getfield(curvature, :complete),
        cell_shape,
        getfield(curvature, :total_cell_count),
        getfield(curvature, :evaluated_cell_count),
        getfield(curvature, :invalid_cell_count),
        validity,
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        mixed_output_curvature,
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues);
        cancel_check,
    )
    input_dimension = preflight.input_dimension
    output_count = preflight.output_count
    expected_shape = preflight.expected_shape

    _rod_cancel(cancel_check)
    classification = fill(:unknown_gap, expected_shape)
    _rod_cancel(cancel_check)
    diagonal_position = 0
    for cell_index in CartesianIndices(Tuple(cell_shape)),
        output in 1:output_count, axis in 1:input_dimension
        diagonal_position += 1
        _rod_periodic_cancel(cancel_check, diagonal_position)
        point = Tuple(cell_index)
        classification[point..., output, axis, axis] = :not_applicable
    end

    summaries = ROSynergyPairSummary[]
    sizehint!(summaries, preflight.summary_count)
    pair_position = 0
    cell_position = 0
    for output in 1:output_count,
        left_axis in 1:max(input_dimension - 1, 0),
        right_axis in (left_axis + 1):input_dimension
        pair_position += 1
        _rod_periodic_cancel(cancel_check, pair_position)
        positive = 0
        negative = 0
        neutral = 0
        unknown = 0
        evaluated = 0
        for cell_index in CartesianIndices(Tuple(cell_shape))
            cell_position += 1
            _rod_periodic_cancel(cancel_check, cell_position)
            point = Tuple(cell_index)
            label = if !validity[cell_index]
                unknown += 1
                :unknown_gap
            else
                value = mixed_output_curvature[
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
    _rod_cancel(cancel_check)
    return ROFiniteWindowSynergy(
        :positive_log_cross_curvature, cutoff, classification, summaries;
        max_input_dimension,
        max_classification_elements,
        max_output_bytes,
        max_tensor_work,
        cancel_check)
end
