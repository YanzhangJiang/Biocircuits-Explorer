using LinearAlgebra
import SHA

"""Raised before affine-chart admission or pullback exceeds a hard limit."""
struct ROAffineInputChartLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, error::ROAffineInputChartLimitExceeded)
    print(io, "affine RO input chart ", error.phase, " requires ",
        error.requested, ", exceeding limit=", error.limit)
end

"""Hard dimensions, allocation, factorization, multiplication, and hash limits."""
struct ROAffineInputChartLimits
    max_source_coordinates::Int
    max_control_coordinates::Int
    max_array_elements::Int
    max_metadata_bytes::Int
    max_operation_scalars::Int
    max_factorization_work::Int
    max_hash_bytes::Int

    function ROAffineInputChartLimits(
        max_source_coordinates::Int,
        max_control_coordinates::Int,
        max_array_elements::Int,
        max_metadata_bytes::Int,
        max_operation_scalars::Int,
        max_factorization_work::Int,
        max_hash_bytes::Int,
    )
        values = (
            max_source_coordinates,
            max_control_coordinates,
            max_array_elements,
            max_metadata_bytes,
            max_operation_scalars,
            max_factorization_work,
            max_hash_bytes,
        )
        all(>(0), values) || throw(ArgumentError(
            "all affine-chart limits must be positive"))
        return new(values...)
    end
end

function ROAffineInputChartLimits(;
    max_source_coordinates::Integer=256,
    max_control_coordinates::Integer=64,
    max_array_elements::Integer=1_048_576,
    max_metadata_bytes::Integer=1_000_000,
    max_operation_scalars::Integer=10_000_000,
    max_factorization_work::Integer=100_000_000,
    max_hash_bytes::Integer=32_000_000,
)
    raw = (
        max_source_coordinates,
        max_control_coordinates,
        max_array_elements,
        max_metadata_bytes,
        max_operation_scalars,
        max_factorization_work,
        max_hash_bytes,
    )
    any(value -> value isa Bool || !(value isa Integer), raw) &&
        throw(ArgumentError("affine-chart limits must be integers, not Bool"))
    all(value -> typemin(Int) <= value <= typemax(Int), raw) ||
        throw(ArgumentError("affine-chart limits must fit in Int"))
    return ROAffineInputChartLimits(Int.(raw)...)
end

@inline function _ro_chart_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "affine-chart work request for $phase must be nonnegative"))
    amount <= limit || throw(ROAffineInputChartLimitExceeded(
        phase, amount, limit))
    return nothing
end

@inline function _ro_chart_cancel(cancel_check)
    cancel_check()
    return nothing
end

function _ro_chart_copy_limits(limits::ROAffineInputChartLimits)
    return ROAffineInputChartLimits(
        limits.max_source_coordinates,
        limits.max_control_coordinates,
        limits.max_array_elements,
        limits.max_metadata_bytes,
        limits.max_operation_scalars,
        limits.max_factorization_work,
        limits.max_hash_bytes,
    )
end

const _RO_CHART_COPY_CHUNK_ELEMENTS = 4_096
const _RO_CHART_HASH_CHUNK_BYTES = 32_768

function _ro_chart_cancellable_copy(
    values::AbstractArray,
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    copied = Array{eltype(values)}(undef, size(values))
    for (position, index) in enumerate(eachindex(values, copied))
        (position - 1) % _RO_CHART_COPY_CHUNK_ELEMENTS == 0 &&
            _ro_chart_cancel(cancel_check)
        copied[index] = values[index]
    end
    _ro_chart_cancel(cancel_check)
    return copied
end

function _ro_chart_encoded_string_bytes(value::AbstractString)
    return BigInt(8) + BigInt(ncodeunits(value))
end

function _ro_chart_encoded_strings_bytes(values)
    total = BigInt(8)
    for value in values
        value isa AbstractString || continue
        total += _ro_chart_encoded_string_bytes(value)
    end
    return total
end

function _ro_chart_content_hash_bytes(
    source_offset_elements::Integer,
    source_jacobian_elements::Integer,
    singular_value_elements::Integer,
    source_axis_ids,
    target_axis_ids,
    axis_identity_scope::Symbol,
)
    header = "bne-ro-affine-input-chart-memory/v2\0"
    return BigInt(ncodeunits(header)) +
        8 + 8 * BigInt(source_offset_elements) +
        16 + 8 * BigInt(source_jacobian_elements) +
        8 + 8 * BigInt(singular_value_elements) +
        8 + 24 +
        _ro_chart_encoded_strings_bytes(source_axis_ids) +
        _ro_chart_encoded_strings_bytes(target_axis_ids) +
        _ro_chart_encoded_string_bytes(String(axis_identity_scope)) +
        7 * 8
end

function _ro_chart_pullback_hash_bytes(
    source_shape,
    result_shape,
    source_axis_ids,
    target_axis_ids,
    axis_identity_scope::Symbol,
    chart_content_sha256::AbstractString,
)
    header = "bne-ro-affine-pullback-result/v2"
    source_elements = BigInt(1)
    for extent in source_shape
        source_elements *= BigInt(extent)
    end
    result_elements = BigInt(1)
    for extent in result_shape
        result_elements *= BigInt(extent)
    end
    return _ro_chart_encoded_string_bytes(header) +
        _ro_chart_encoded_string_bytes(chart_content_sha256) +
        _ro_chart_encoded_string_bytes(String(axis_identity_scope)) +
        _ro_chart_encoded_strings_bytes(source_axis_ids) +
        _ro_chart_encoded_strings_bytes(target_axis_ids) +
        8 + 8 * BigInt(length(source_shape)) + 8 * source_elements +
        8 + 8 * BigInt(length(result_shape)) + 8 * result_elements +
        7 * 8
end

function _ro_chart_hash_array!(context, values::Array{Float64}, cancel_check)
    _ro_chart_hash_update!(
        context,
        reinterpret(UInt8, vec(values)),
        cancel_check,
    )
    return nothing
end

function _ro_chart_hash_limits!(
    context,
    limits::ROAffineInputChartLimits,
    cancel_check,
)
    for value in (
        limits.max_source_coordinates,
        limits.max_control_coordinates,
        limits.max_array_elements,
        limits.max_metadata_bytes,
        limits.max_operation_scalars,
        limits.max_factorization_work,
        limits.max_hash_bytes,
    )
        _ro_chart_hash_uint64!(context, value, cancel_check)
    end
    return nothing
end

function _ro_chart_hash_update!(context, bytes, cancel_check)
    byte_count = length(bytes)
    first = firstindex(bytes)
    offset = 0
    while offset < byte_count
        _ro_chart_cancel(cancel_check)
        count = min(_RO_CHART_HASH_CHUNK_BYTES, byte_count - offset)
        range = (first + offset):(first + offset + count - 1)
        SHA.update!(context, @view bytes[range])
        offset += count
    end
    _ro_chart_cancel(cancel_check)
    return nothing
end

function _ro_chart_hash_uint64!(context, value::Integer, cancel_check)
    encoded = reinterpret(UInt8, [htol(UInt64(value))])
    _ro_chart_hash_update!(context, encoded, cancel_check)
    return nothing
end

function _ro_chart_hash_string!(context, value::AbstractString, cancel_check)
    bytes = codeunits(value)
    _ro_chart_hash_uint64!(context, length(bytes), cancel_check)
    _ro_chart_hash_update!(context, bytes, cancel_check)
    return nothing
end

function _ro_chart_hash_strings!(context, values, cancel_check)
    _ro_chart_hash_uint64!(context, length(values), cancel_check)
    for value in values
        _ro_chart_cancel(cancel_check)
        _ro_chart_hash_string!(context, value, cancel_check)
    end
    return nothing
end

function _ro_chart_array_elements(raw::AbstractArray)
    amount = BigInt(1)
    for extent in size(raw)
        amount *= BigInt(extent)
    end
    return amount
end

function _ro_chart_metadata_bytes(
    source_axis_ids,
    target_axis_ids,
    ;
    expected_source_count::Union{Nothing,Integer}=nothing,
    expected_target_count::Union{Nothing,Integer}=nothing,
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    expected_source_count === nothing ||
        length(source_axis_ids) == expected_source_count || throw(
            DimensionMismatch(
                "source_axis_ids must contain $expected_source_count ordered identifiers"))
    expected_target_count === nothing ||
        length(target_axis_ids) == expected_target_count || throw(
            DimensionMismatch(
                "target_axis_ids must contain $expected_target_count ordered identifiers"))
    bytes = BigInt(0)
    for collection in (source_axis_ids, target_axis_ids), value in collection
        _ro_chart_cancel(cancel_check)
        value isa AbstractString || continue
        bytes += BigInt(ncodeunits(value))
        _ro_chart_limit(:metadata_bytes, bytes, limits.max_metadata_bytes)
    end
    _ro_chart_cancel(cancel_check)
    return bytes
end

function _ro_chart_axis_identity_counts(
    axis_identity_scope::Symbol,
    source_count::Int,
    control_count::Int,
)
    if axis_identity_scope === :dimension_only_numerical_transform
        return 0, 0
    elseif axis_identity_scope === :source_and_target_axis_ids_bound
        return source_count, control_count
    end
    throw(ArgumentError("unsupported affine-chart axis identity scope"))
end

function _ro_chart_preflight_dimensions(
    source_count::Integer,
    control_count::Integer,
    array_elements::Integer,
    factorization_multiplier::Integer,
    limits::ROAffineInputChartLimits,
)
    _ro_chart_limit(:source_coordinates, source_count,
        limits.max_source_coordinates)
    _ro_chart_limit(:control_coordinates, control_count,
        limits.max_control_coordinates)
    _ro_chart_limit(:array_elements, array_elements,
        limits.max_array_elements)
    factorization_work = BigInt(factorization_multiplier) *
        BigInt(source_count) * BigInt(control_count)^2
    _ro_chart_limit(:factorization_work, factorization_work,
        limits.max_factorization_work)
    return nothing
end

"""
    ROAffineInputChart(source_offset, source_jacobian;
        rank_rtol=1e-12, max_condition_number=1e10,
        source_axis_ids=nothing, target_axis_ids=nothing)

A full-column-rank affine chart from declared control coordinates `u` to the
engine's ordered source coordinates `theta`:

```text
theta = source_offset + source_jacobian * u
```

`source_jacobian` therefore has shape `source_count x control_count`. The
constructor rejects a chart before use when its columns are numerically rank
deficient or its 2-norm condition number exceeds `max_condition_number`.
Both decisions are made from Float64 singular values and the recorded
`rank_rtol`; they are numerical admission checks, not symbolic rank proofs.
Array-valued properties are detached snapshots. The admitted backing arrays
are content-hashed in memory so a lower-level mutation is detected before a
subsequent public operation instead of leaving stale rank/conditioning
diagnostics. The raw validated constructor independently rechecks its arrays,
diagnostics, and seal. When both ordered axis-ID vectors are supplied, they are
part of the content seal and identity-bound pullback receipts can be produced.
Omitting both retains the legacy dimension-only numerical transform, which is
explicitly not a complete-evidence result.
"""
struct ROAffineInputChart
    source_offset::Vector{Float64}
    source_jacobian::Matrix{Float64}
    singular_values::Vector{Float64}
    numerical_rank::Int
    condition_number::Float64
    rank_rtol::Float64
    max_condition_number::Float64
    source_axis_ids::Vector{String}
    target_axis_ids::Vector{String}
    axis_identity_scope::Symbol
    limits::ROAffineInputChartLimits
    content_sha256::String

    function ROAffineInputChart(
        source_offset::Vector{Float64},
        source_jacobian::Matrix{Float64},
        singular_values::Vector{Float64},
        numerical_rank::Int,
        condition_number::Float64,
        rank_rtol::Float64,
        max_condition_number::Float64,
        source_axis_ids::Vector{String},
        target_axis_ids::Vector{String},
        axis_identity_scope::Symbol,
        content_sha256::String,
        ::Val{:validated},
        ;
        limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
        cancel_check=() -> nothing,
    )
        _ro_chart_cancel(cancel_check)
        admitted_limits = _ro_chart_copy_limits(limits)
        source_count = length(source_offset)
        control_count = size(source_jacobian, 2)
        array_elements = BigInt(length(source_offset)) +
            BigInt(length(source_jacobian)) +
            BigInt(length(singular_values))
        _ro_chart_preflight_dimensions(
            source_count,
            control_count,
            array_elements,
            1,
            admitted_limits,
        )
        expected_source_ids, expected_target_ids =
            _ro_chart_axis_identity_counts(
                axis_identity_scope, source_count, control_count)
        metadata_bytes = _ro_chart_metadata_bytes(
            source_axis_ids,
            target_axis_ids;
            expected_source_count=expected_source_ids,
            expected_target_count=expected_target_ids,
            limits=admitted_limits,
            cancel_check=cancel_check,
        )
        _ro_chart_limit(:metadata_bytes, metadata_bytes,
            admitted_limits.max_metadata_bytes)
        hash_bytes = _ro_chart_content_hash_bytes(
            length(source_offset),
            length(source_jacobian),
            length(singular_values),
            source_axis_ids,
            target_axis_ids,
            axis_identity_scope,
        )
        _ro_chart_limit(
            :hash_bytes, hash_bytes, admitted_limits.max_hash_bytes)
        _ro_chart_validate_components(
            source_offset,
            source_jacobian,
            singular_values,
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
            source_axis_ids,
            target_axis_ids,
            axis_identity_scope,
            admitted_limits,
            cancel_check,
        )
        content_sha256 == _ro_chart_content_sha256(
            source_offset,
            source_jacobian,
            singular_values,
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
            source_axis_ids,
            target_axis_ids,
            axis_identity_scope,
            limits=admitted_limits,
            cancel_check=cancel_check,
        ) || throw(ArgumentError(
            "ROAffineInputChart content seal does not match admitted values"))
        _ro_chart_cancel(cancel_check)
        return new(
            _ro_chart_cancellable_copy(source_offset, cancel_check),
            _ro_chart_cancellable_copy(source_jacobian, cancel_check),
            _ro_chart_cancellable_copy(singular_values, cancel_check),
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
            _ro_chart_cancellable_copy(source_axis_ids, cancel_check),
            _ro_chart_cancellable_copy(target_axis_ids, cancel_check),
            axis_identity_scope,
            admitted_limits,
            content_sha256,
        )
    end

    # Compatibility for the pre-identity raw constructor. Its seal is still
    # recomputed under the current dimension-only identity scope.
    function ROAffineInputChart(
        source_offset::Vector{Float64},
        source_jacobian::Matrix{Float64},
        singular_values::Vector{Float64},
        numerical_rank::Int,
        condition_number::Float64,
        rank_rtol::Float64,
        max_condition_number::Float64,
        content_sha256::String,
        validated::Val{:validated},
        ;
        limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
        cancel_check=() -> nothing,
    )
        return ROAffineInputChart(
            source_offset,
            source_jacobian,
            singular_values,
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
            String[],
            String[],
            :dimension_only_numerical_transform,
            content_sha256,
            validated;
            limits=limits,
            cancel_check=cancel_check,
        )
    end
end

function _ro_chart_content_sha256(
    source_offset::Vector{Float64},
    source_jacobian::Matrix{Float64},
    singular_values::Vector{Float64},
    numerical_rank::Int,
    condition_number::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    source_axis_ids::Vector{String},
    target_axis_ids::Vector{String},
    axis_identity_scope::Symbol,
    ;
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    work_limits::ROAffineInputChartLimits=limits,
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    array_elements = BigInt(length(source_offset)) +
        BigInt(length(source_jacobian)) + BigInt(length(singular_values))
    expected_source_ids, expected_target_ids = _ro_chart_axis_identity_counts(
        axis_identity_scope, length(source_offset), size(source_jacobian, 2))
    metadata_bytes = _ro_chart_metadata_bytes(
        source_axis_ids,
        target_axis_ids;
        expected_source_count=expected_source_ids,
        expected_target_count=expected_target_ids,
        limits=work_limits,
        cancel_check=cancel_check,
    )
    _ro_chart_limit(:array_elements, array_elements,
        work_limits.max_array_elements)
    _ro_chart_limit(:metadata_bytes, metadata_bytes,
        work_limits.max_metadata_bytes)
    hash_bytes = _ro_chart_content_hash_bytes(
        length(source_offset),
        length(source_jacobian),
        length(singular_values),
        source_axis_ids,
        target_axis_ids,
        axis_identity_scope,
    )
    _ro_chart_limit(:hash_bytes, hash_bytes, work_limits.max_hash_bytes)

    context = SHA.SHA2_256_CTX()
    _ro_chart_hash_update!(context,
        codeunits("bne-ro-affine-input-chart-memory/v2\0"), cancel_check)
    _ro_chart_hash_uint64!(context, length(source_offset), cancel_check)
    _ro_chart_hash_array!(context, source_offset, cancel_check)
    _ro_chart_hash_uint64!(context, size(source_jacobian, 1), cancel_check)
    _ro_chart_hash_uint64!(context, size(source_jacobian, 2), cancel_check)
    _ro_chart_hash_array!(context, source_jacobian, cancel_check)
    _ro_chart_hash_uint64!(context, length(singular_values), cancel_check)
    _ro_chart_hash_array!(context, singular_values, cancel_check)
    _ro_chart_hash_uint64!(context, numerical_rank, cancel_check)
    diagnostics = [condition_number, rank_rtol, max_condition_number]
    _ro_chart_hash_array!(context, diagnostics, cancel_check)
    _ro_chart_hash_strings!(context, source_axis_ids, cancel_check)
    _ro_chart_hash_strings!(context, target_axis_ids, cancel_check)
    _ro_chart_hash_string!(context, String(axis_identity_scope), cancel_check)
    _ro_chart_hash_limits!(context, limits, cancel_check)
    _ro_chart_cancel(cancel_check)
    return bytes2hex(SHA.digest!(context))
end

function _ro_chart_content_sha256(
    source_offset::Vector{Float64},
    source_jacobian::Matrix{Float64},
    singular_values::Vector{Float64},
    numerical_rank::Int,
    condition_number::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    ;
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    work_limits::ROAffineInputChartLimits=limits,
    cancel_check=() -> nothing,
)
    return _ro_chart_content_sha256(
        source_offset,
        source_jacobian,
        singular_values,
        numerical_rank,
        condition_number,
        rank_rtol,
        max_condition_number,
        String[],
        String[],
        :dimension_only_numerical_transform,
        limits=limits,
        work_limits=work_limits,
        cancel_check=cancel_check,
    )
end

function _ro_chart_validate_components(
    source_offset::Vector{Float64},
    source_jacobian::Matrix{Float64},
    singular_values::Vector{Float64},
    numerical_rank::Int,
    condition_number::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    source_axis_ids::Vector{String},
    target_axis_ids::Vector{String},
    axis_identity_scope::Symbol,
    limits::ROAffineInputChartLimits,
    cancel_check,
)
    _ro_chart_cancel(cancel_check)
    source_count = length(source_offset)
    control_count = size(source_jacobian, 2)
    source_count > 0 || throw(ArgumentError(
        "source_offset must contain at least one source coordinate"))
    control_count > 0 || throw(ArgumentError(
        "source_jacobian must contain at least one control-coordinate column"))
    size(source_jacobian, 1) == source_count || throw(DimensionMismatch(
        "source_jacobian row count must equal source_offset length"))
    source_count >= control_count || throw(DimensionMismatch(
        "a full-column-rank source_jacobian requires source_count >= control_count"))
    array_elements = BigInt(length(source_offset)) +
        BigInt(length(source_jacobian)) + BigInt(length(singular_values))
    _ro_chart_preflight_dimensions(
        source_count, control_count, array_elements, 1, limits)
    expected_source_ids, expected_target_ids = _ro_chart_axis_identity_counts(
        axis_identity_scope, source_count, control_count)
    metadata_bytes = _ro_chart_metadata_bytes(
        source_axis_ids,
        target_axis_ids;
        expected_source_count=expected_source_ids,
        expected_target_count=expected_target_ids,
        limits=limits,
        cancel_check=cancel_check,
    )
    _ro_chart_limit(:metadata_bytes, metadata_bytes, limits.max_metadata_bytes)
    _ro_chart_require_finite_array(
        source_offset, "source_offset", cancel_check)
    _ro_chart_require_finite_array(
        source_jacobian, "source_jacobian", cancel_check)
    isfinite(rank_rtol) && 0.0 < rank_rtol < 1.0 || throw(ArgumentError(
        "rank_rtol must lie strictly between zero and one"))
    isfinite(max_condition_number) && max_condition_number >= 1.0 ||
        throw(ArgumentError("max_condition_number must be finite and at least one"))

    _ro_chart_cancel(cancel_check)
    expected_singular_values = Vector{Float64}(svdvals(source_jacobian))
    _ro_chart_cancel(cancel_check)
    length(singular_values) == control_count &&
        all(isfinite, singular_values) &&
        isequal(singular_values, expected_singular_values) ||
        throw(ArgumentError(
            "singular_values do not match the supplied source_jacobian"))
    threshold = rank_rtol * first(expected_singular_values)
    expected_rank = count(
        value -> value > threshold, expected_singular_values)
    numerical_rank == expected_rank == control_count || throw(ArgumentError(
        "source_jacobian is numerically rank deficient or rank diagnostics are stale"))
    expected_condition = first(expected_singular_values) /
        last(expected_singular_values)
    isfinite(expected_condition) &&
        isequal(condition_number, expected_condition) || throw(ArgumentError(
            "condition_number does not match the supplied source_jacobian"))
    condition_number <= max_condition_number || throw(ArgumentError(
        "source_jacobian condition number exceeds max_condition_number"))
    _ro_chart_validate_axis_identity(
        source_axis_ids,
        target_axis_ids,
        axis_identity_scope,
        source_count,
        control_count,
        limits,
        cancel_check,
    )
    _ro_chart_cancel(cancel_check)
    return nothing
end

function _ro_chart_assert_unchanged(
    chart::ROAffineInputChart;
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    _ro_chart_limit(:source_coordinates,
        BigInt(length(getfield(chart, :source_offset))),
        limits.max_source_coordinates)
    _ro_chart_limit(:control_coordinates,
        BigInt(size(getfield(chart, :source_jacobian), 2)),
        limits.max_control_coordinates)
    actual = _ro_chart_content_sha256(
        getfield(chart, :source_offset),
        getfield(chart, :source_jacobian),
        getfield(chart, :singular_values),
        getfield(chart, :numerical_rank),
        getfield(chart, :condition_number),
        getfield(chart, :rank_rtol),
        getfield(chart, :max_condition_number),
        getfield(chart, :source_axis_ids),
        getfield(chart, :target_axis_ids),
        getfield(chart, :axis_identity_scope),
        limits=getfield(chart, :limits),
        work_limits=limits,
        cancel_check=cancel_check,
    )
    actual == getfield(chart, :content_sha256) || throw(ArgumentError(
        "ROAffineInputChart backing storage changed after admission"))
    _ro_chart_cancel(cancel_check)
    return nothing
end

function Base.getproperty(chart::ROAffineInputChart, name::Symbol)
    _ro_chart_assert_unchanged(chart)
    value = getfield(chart, name)
    if name === :source_offset ||
            name === :source_jacobian ||
            name === :singular_values ||
            name === :source_axis_ids ||
            name === :target_axis_ids
        return copy(value)
    end
    return value
end

function _ro_chart_finite_float64_array(
    raw::AbstractArray,
    name::AbstractString;
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    values = Array{Float64}(undef, size(raw))
    for (position, index) in enumerate(eachindex(raw))
        (position - 1) % 256 == 0 && _ro_chart_cancel(cancel_check)
        value = raw[index]
        value isa Real && !(value isa Bool) || throw(ArgumentError(
            "$name must contain only real, non-Boolean values"))
        converted = Float64(value)
        isfinite(converted) ||
            throw(ArgumentError("$name must contain only finite values"))
        values[index] = converted
    end
    _ro_chart_cancel(cancel_check)
    return values
end

function _ro_chart_require_finite_array(
    values::AbstractArray,
    name::AbstractString,
    cancel_check,
)
    for (position, value) in enumerate(values)
        (position - 1) % _RO_CHART_COPY_CHUNK_ELEMENTS == 0 &&
            _ro_chart_cancel(cancel_check)
        isfinite(value) || throw(ArgumentError("$name must contain only finite values"))
    end
    _ro_chart_cancel(cancel_check)
    return nothing
end

function _ro_chart_finite_real(raw, name::AbstractString)
    raw isa Real && !(raw isa Bool) ||
        throw(ArgumentError("$name must be a real, non-Boolean value"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    return value
end

function _ro_chart_write_string(io::IO, value::AbstractString;
                                cancel_check=() -> nothing)
    _ro_chart_cancel(cancel_check)
    bytes = codeunits(value)
    write(io, htol(UInt64(length(bytes))))
    write(io, bytes)
    return nothing
end

function _ro_chart_write_strings(io::IO, values::Vector{String};
                                 cancel_check=() -> nothing)
    _ro_chart_cancel(cancel_check)
    write(io, htol(UInt64(length(values))))
    for value in values
        _ro_chart_cancel(cancel_check)
        _ro_chart_write_string(io, value; cancel_check=cancel_check)
    end
    return nothing
end


function _ro_chart_normalize_axis_ids(
    raw,
    expected::Int,
    name::AbstractString;
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    raw isa AbstractVector || throw(ArgumentError("$name must be a vector"))
    length(raw) == expected || throw(DimensionMismatch(
        "$name must contain $expected ordered identifiers"))
    ids = String[]
    sizehint!(ids, expected)
    metadata_bytes = BigInt(0)
    for (position, raw_id) in enumerate(raw)
        _ro_chart_cancel(cancel_check)
        raw_id isa AbstractString || throw(ArgumentError(
            "$name[$position] must be a string"))
        id = String(raw_id)
        !isempty(id) && strip(id) == id && !occursin('\0', id) ||
            throw(ArgumentError(
                "$name[$position] must be nonempty, trimmed, and NUL-free"))
        ncodeunits(id) <= 256 || throw(ArgumentError(
            "$name[$position] exceeds 256 UTF-8 bytes"))
        metadata_bytes += BigInt(ncodeunits(id))
        _ro_chart_limit(:metadata_bytes, metadata_bytes,
            limits.max_metadata_bytes)
        push!(ids, id)
    end
    allunique(ids) || throw(ArgumentError("$name must be unique"))
    return ids
end

function _ro_chart_validate_axis_identity(
    source_axis_ids::Vector{String},
    target_axis_ids::Vector{String},
    axis_identity_scope::Symbol,
    source_count::Int,
    control_count::Int,
    limits::ROAffineInputChartLimits,
    cancel_check,
)
    _ro_chart_cancel(cancel_check)
    if axis_identity_scope === :dimension_only_numerical_transform
        isempty(source_axis_ids) && isempty(target_axis_ids) ||
            throw(ArgumentError(
                "dimension-only affine charts cannot retain axis identifiers"))
    elseif axis_identity_scope === :source_and_target_axis_ids_bound
        _ro_chart_normalize_axis_ids(
            source_axis_ids, source_count, "source_axis_ids";
            limits=limits, cancel_check=cancel_check) ==
            source_axis_ids || throw(ArgumentError(
                "source_axis_ids are not canonical"))
        _ro_chart_normalize_axis_ids(
            target_axis_ids, control_count, "target_axis_ids";
            limits=limits, cancel_check=cancel_check) ==
            target_axis_ids || throw(ArgumentError(
                "target_axis_ids are not canonical"))
    else
        throw(ArgumentError("unsupported affine-chart axis identity scope"))
    end
    _ro_chart_cancel(cancel_check)
    return nothing
end

function ROAffineInputChart(
    source_offset::AbstractVector,
    source_jacobian::AbstractMatrix;
    rank_rtol::Real=1e-12,
    max_condition_number::Real=1e10,
    source_axis_ids=nothing,
    target_axis_ids=nothing,
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    admitted_limits = _ro_chart_copy_limits(limits)
    source_count = length(source_offset)
    control_count = size(source_jacobian, 2)
    source_count > 0 || throw(ArgumentError(
        "source_offset must contain at least one source coordinate"))
    control_count > 0 || throw(ArgumentError(
        "source_jacobian must contain at least one control-coordinate column"))
    size(source_jacobian, 1) == source_count || throw(DimensionMismatch(
        "source_jacobian row count must equal source_offset length"))
    source_count >= control_count || throw(DimensionMismatch(
        "a full-column-rank source_jacobian requires source_count >= control_count"))
    array_elements = BigInt(length(source_offset)) +
        _ro_chart_array_elements(source_jacobian) + BigInt(control_count)
    # The ordinary constructor performs one SVD for admission diagnostics and
    # the raw sealed constructor independently replays it.
    _ro_chart_preflight_dimensions(
        source_count, control_count, array_elements, 2, admitted_limits)
    if source_axis_ids === nothing && target_axis_ids === nothing
        nothing
    elseif source_axis_ids === nothing || target_axis_ids === nothing
        throw(ArgumentError(
            "source_axis_ids and target_axis_ids must be supplied together"))
    else
        source_axis_ids isa AbstractVector || throw(ArgumentError(
            "source_axis_ids must be a vector"))
        target_axis_ids isa AbstractVector || throw(ArgumentError(
            "target_axis_ids must be a vector"))
        length(source_axis_ids) == source_count || throw(DimensionMismatch(
            "source_axis_ids must contain $source_count ordered identifiers"))
        length(target_axis_ids) == control_count || throw(DimensionMismatch(
            "target_axis_ids must contain $control_count ordered identifiers"))
    end
    identity_scope, admitted_source_axis_ids, admitted_target_axis_ids =
        if source_axis_ids === nothing
            (:dimension_only_numerical_transform, String[], String[])
        else
            (
                :source_and_target_axis_ids_bound,
                _ro_chart_normalize_axis_ids(
                    source_axis_ids, source_count, "source_axis_ids";
                    limits=admitted_limits, cancel_check=cancel_check),
                _ro_chart_normalize_axis_ids(
                    target_axis_ids, control_count, "target_axis_ids";
                    limits=admitted_limits, cancel_check=cancel_check),
            )
        end
    metadata_bytes = _ro_chart_metadata_bytes(
        admitted_source_axis_ids,
        admitted_target_axis_ids;
        expected_source_count=length(admitted_source_axis_ids),
        expected_target_count=length(admitted_target_axis_ids),
        limits=admitted_limits,
        cancel_check=cancel_check,
    )
    _ro_chart_limit(
        :metadata_bytes, metadata_bytes, admitted_limits.max_metadata_bytes)
    hash_bytes = _ro_chart_content_hash_bytes(
        source_count,
        BigInt(source_count) * BigInt(control_count),
        control_count,
        admitted_source_axis_ids,
        admitted_target_axis_ids,
        identity_scope,
    )
    _ro_chart_limit(
        :hash_bytes, 2 * hash_bytes, admitted_limits.max_hash_bytes)

    offset = vec(_ro_chart_finite_float64_array(
        source_offset, "source_offset"; cancel_check=cancel_check))
    jacobian = _ro_chart_finite_float64_array(
        source_jacobian, "source_jacobian"; cancel_check=cancel_check)

    rtol = _ro_chart_finite_real(rank_rtol, "rank_rtol")
    0.0 < rtol < 1.0 || throw(ArgumentError(
        "rank_rtol must lie strictly between zero and one"))
    condition_limit = _ro_chart_finite_real(
        max_condition_number, "max_condition_number")
    condition_limit >= 1.0 || throw(ArgumentError(
        "max_condition_number must be at least one"))

    _ro_chart_cancel(cancel_check)
    singular_values = Vector{Float64}(svdvals(jacobian))
    _ro_chart_cancel(cancel_check)
    length(singular_values) == control_count || error(
        "internal affine-chart SVD dimension mismatch")
    largest = first(singular_values)
    threshold = rtol * largest
    numerical_rank = count(value -> value > threshold, singular_values)
    numerical_rank == control_count || throw(ArgumentError(
        "source_jacobian is numerically rank deficient: rank=$numerical_rank, " *
        "required=$control_count, rank_rtol=$rtol"))

    smallest = last(singular_values)
    condition_number = largest / smallest
    isfinite(condition_number) || throw(ArgumentError(
        "source_jacobian has a non-finite 2-norm condition number"))
    condition_number <= condition_limit || throw(ArgumentError(
        "source_jacobian condition number $condition_number exceeds " *
        "max_condition_number=$condition_limit"))

    expected_source_ids, expected_target_ids = _ro_chart_axis_identity_counts(
        identity_scope, source_count, control_count)
    metadata_bytes = _ro_chart_metadata_bytes(
        admitted_source_axis_ids,
        admitted_target_axis_ids;
        expected_source_count=expected_source_ids,
        expected_target_count=expected_target_ids,
        limits=admitted_limits,
        cancel_check=cancel_check,
    )
    _ro_chart_limit(:metadata_bytes, metadata_bytes,
        admitted_limits.max_metadata_bytes)
    hash_bytes = _ro_chart_content_hash_bytes(
        length(offset),
        length(jacobian),
        length(singular_values),
        admitted_source_axis_ids,
        admitted_target_axis_ids,
        identity_scope,
    )
    _ro_chart_limit(
        :hash_bytes, 2 * hash_bytes, admitted_limits.max_hash_bytes)

    # Store detached arrays so later mutation of constructor inputs cannot
    # alter the admitted chart or its recorded diagnostics.
    stored_offset = _ro_chart_cancellable_copy(offset, cancel_check)
    stored_jacobian = _ro_chart_cancellable_copy(jacobian, cancel_check)
    stored_singular_values =
        _ro_chart_cancellable_copy(singular_values, cancel_check)
    content_sha256 = _ro_chart_content_sha256(
        stored_offset,
        stored_jacobian,
        stored_singular_values,
        numerical_rank,
        condition_number,
        rtol,
        condition_limit,
        admitted_source_axis_ids,
        admitted_target_axis_ids,
        identity_scope,
        limits=admitted_limits,
        cancel_check=cancel_check,
    )
    return ROAffineInputChart(
        stored_offset,
        stored_jacobian,
        stored_singular_values,
        numerical_rank,
        condition_number,
        rtol,
        condition_limit,
        admitted_source_axis_ids,
        admitted_target_axis_ids,
        identity_scope,
        content_sha256,
        Val(:validated);
        limits=admitted_limits,
        cancel_check=cancel_check,
    )
end

"""Number of ordered engine/source coordinates produced by `chart`."""
function ro_source_coordinate_count(chart::ROAffineInputChart)
    _ro_chart_assert_unchanged(chart)
    return length(getfield(chart, :source_offset))
end

"""Number of independent declared controls consumed by `chart`."""
function ro_control_coordinate_count(chart::ROAffineInputChart)
    _ro_chart_assert_unchanged(chart)
    return size(getfield(chart, :source_jacobian), 2)
end

"""
    map_ro_source_coordinates(chart, control_coordinates) -> Vector{Float64}

Map one ordered control-coordinate vector through `theta = b + A*u`.
Non-finite inputs or a non-finite mapped source coordinate fail closed.
"""
function map_ro_source_coordinates(
    chart::ROAffineInputChart,
    control_coordinates::AbstractVector,
    ;
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    source_count = length(getfield(chart, :source_offset))
    control_count = size(getfield(chart, :source_jacobian), 2)
    length(control_coordinates) == control_count || throw(DimensionMismatch(
        "control_coordinates must contain $control_count ordered values"))
    _ro_chart_limit(:array_elements,
        BigInt(length(control_coordinates)) + BigInt(source_count),
        limits.max_array_elements)
    _ro_chart_limit(:operation_scalars,
        BigInt(source_count) * BigInt(control_count),
        limits.max_operation_scalars)
    _ro_chart_assert_unchanged(
        chart; limits=limits, cancel_check=cancel_check)
    controls = vec(_ro_chart_finite_float64_array(
        control_coordinates, "control_coordinates";
        cancel_check=cancel_check))
    _ro_chart_cancel(cancel_check)
    mapped = getfield(chart, :source_offset) +
        getfield(chart, :source_jacobian) * controls
    _ro_chart_cancel(cancel_check)
    all(isfinite, mapped) || throw(OverflowError(
        "affine input-chart mapping produced a non-finite source coordinate"))
    return mapped
end

function _ro_chart_pullback_values(
    chart::ROAffineInputChart,
    source_values::AbstractArray,
    name::AbstractString,
    ;
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    ndims(source_values) >= 1 || throw(DimensionMismatch(
        "$name must have a final source-coordinate axis"))
    source_count = length(getfield(chart, :source_offset))
    size(source_values, ndims(source_values)) == source_count ||
        throw(DimensionMismatch(
            "the final $name axis must have source-coordinate length $source_count"))
    source_elements = _ro_chart_array_elements(source_values)
    leading_count_big = source_elements ÷ BigInt(source_count)
    control_count = size(getfield(chart, :source_jacobian), 2)
    result_elements = leading_count_big * BigInt(control_count)
    _ro_chart_limit(:array_elements,
        source_elements + result_elements,
        limits.max_array_elements)
    _ro_chart_limit(:operation_scalars,
        leading_count_big * BigInt(source_count) * BigInt(control_count),
        limits.max_operation_scalars)
    _ro_chart_assert_unchanged(
        chart; limits=limits, cancel_check=cancel_check)
    values = _ro_chart_finite_float64_array(
        source_values, name; cancel_check=cancel_check)
    leading_count = length(values) ÷ source_count
    flattened = reshape(values, leading_count, source_count)
    _ro_chart_cancel(cancel_check)
    pulled_back = flattened * getfield(chart, :source_jacobian)
    _ro_chart_cancel(cancel_check)
    all(isfinite, pulled_back) || throw(OverflowError(
        "$name pullback produced a non-finite derivative"))
    output_shape = (
        size(values)[1:(ndims(values) - 1)]...,
        control_count,
    )
    return _ro_chart_cancellable_copy(
        reshape(pulled_back, output_shape), cancel_check)
end

function _ro_chart_preflight_receipt_hashes(
    chart::ROAffineInputChart,
    source_shape,
    result_shape,
    source_axis_ids::Vector{String},
    target_axis_ids::Vector{String},
    limits::ROAffineInputChartLimits,
    cancel_check,
)
    _ro_chart_cancel(cancel_check)
    source_count = length(getfield(chart, :source_offset))
    control_count = size(getfield(chart, :source_jacobian), 2)
    chart_source_ids = getfield(chart, :source_axis_ids)
    chart_target_ids = getfield(chart, :target_axis_ids)
    chart_metadata_bytes = _ro_chart_metadata_bytes(
        chart_source_ids,
        chart_target_ids;
        expected_source_count=source_count,
        expected_target_count=control_count,
        limits=limits,
        cancel_check=cancel_check,
    )
    result_metadata_bytes = _ro_chart_metadata_bytes(
        source_axis_ids,
        target_axis_ids;
        expected_source_count=source_count,
        expected_target_count=control_count,
        limits=limits,
        cancel_check=cancel_check,
    ) + BigInt(ncodeunits(getfield(chart, :content_sha256)))
    _ro_chart_limit(
        :metadata_bytes, chart_metadata_bytes, limits.max_metadata_bytes)
    _ro_chart_limit(
        :metadata_bytes, result_metadata_bytes, limits.max_metadata_bytes)
    chart_hash_bytes = _ro_chart_content_hash_bytes(
        length(getfield(chart, :source_offset)),
        length(getfield(chart, :source_jacobian)),
        length(getfield(chart, :singular_values)),
        chart_source_ids,
        chart_target_ids,
        getfield(chart, :axis_identity_scope),
    )
    result_hash_bytes = _ro_chart_pullback_hash_bytes(
        source_shape,
        result_shape,
        source_axis_ids,
        target_axis_ids,
        getfield(chart, :axis_identity_scope),
        getfield(chart, :content_sha256),
    )
    # Receipt construction and validation each hash the chart once directly,
    # once during multiplication replay, and the receipt once.
    cumulative_hash_bytes = 2 * chart_hash_bytes + result_hash_bytes
    _ro_chart_limit(
        :hash_bytes, cumulative_hash_bytes, limits.max_hash_bytes)
    _ro_chart_cancel(cancel_check)
    return cumulative_hash_bytes
end

"""
Identity-bound result of an affine first-order pullback.

The detached source values, transformed values, ordered source/target axis
identifiers, and chart content identity share one content seal. Validation
replays the matrix product against the chart. Only this receipt is an
axis-identity-bound result; the legacy raw-array overload is explicitly a
dimension-only numerical transform.
"""
struct ROAffinePullbackResult{N}
    source_values::Array{Float64,N}
    values::Array{Float64,N}
    source_axis_ids::Vector{String}
    target_axis_ids::Vector{String}
    axis_identity_scope::Symbol
    chart_content_sha256::String
    limits::ROAffineInputChartLimits
    content_sha256::String

    function ROAffinePullbackResult(
        chart::ROAffineInputChart,
        source_values::AbstractArray,
        source_axis_ids,
        ;
        limits::ROAffineInputChartLimits=getfield(chart, :limits),
        cancel_check=() -> nothing,
    )
        _ro_chart_cancel(cancel_check)
        admitted_limits = _ro_chart_copy_limits(limits)
        getfield(chart, :axis_identity_scope) ===
            :source_and_target_axis_ids_bound || throw(ArgumentError(
                "axis-bound pullback requires a chart with source_axis_ids and target_axis_ids"))
        source_count = length(getfield(chart, :source_offset))
        control_count = size(getfield(chart, :source_jacobian), 2)
        admitted_source_ids = _ro_chart_normalize_axis_ids(
            source_axis_ids,
            source_count,
            "source_axis_ids",
            limits=admitted_limits,
            cancel_check=cancel_check,
        )
        admitted_source_ids == getfield(chart, :source_axis_ids) ||
            throw(ArgumentError(
                "source_axis_ids order does not match the affine chart"))
        ndims(source_values) >= 1 || throw(DimensionMismatch(
            "source_values must have a final source-coordinate axis"))
        size(source_values, ndims(source_values)) == source_count || throw(
            DimensionMismatch(
                "the final source_values axis must have source-coordinate length $source_count"))
        source_elements = _ro_chart_array_elements(source_values)
        leading_count = source_elements ÷ BigInt(source_count)
        result_elements = leading_count * BigInt(control_count)
        _ro_chart_limit(:array_elements,
            source_elements + result_elements,
            admitted_limits.max_array_elements)
        _ro_chart_limit(:operation_scalars,
            leading_count * BigInt(source_count) * BigInt(control_count),
            admitted_limits.max_operation_scalars)
        source_shape = size(source_values)
        result_shape = (
            source_shape[1:(length(source_shape) - 1)]...,
            control_count,
        )
        target_ids = getfield(chart, :target_axis_ids)
        _ro_chart_preflight_receipt_hashes(
            chart,
            source_shape,
            result_shape,
            admitted_source_ids,
            target_ids,
            admitted_limits,
            cancel_check,
        )
        _ro_chart_assert_unchanged(
            chart; limits=admitted_limits, cancel_check=cancel_check)
        _ro_chart_cancel(cancel_check)
        admitted_source_values = _ro_chart_finite_float64_array(
            source_values, "source_values"; cancel_check=cancel_check)
        values = _ro_chart_pullback_values(
            chart,
            admitted_source_values,
            "source_values";
            limits=admitted_limits,
            cancel_check=cancel_check,
        )
        target_ids = _ro_chart_cancellable_copy(target_ids, cancel_check)
        identity_scope = getfield(chart, :axis_identity_scope)
        chart_content_sha256 = getfield(chart, :content_sha256)
        content_sha256 = _ro_chart_pullback_result_sha256(
            admitted_source_values,
            values,
            admitted_source_ids,
            target_ids,
            identity_scope,
            chart_content_sha256,
            limits=admitted_limits,
            cancel_check=cancel_check,
        )
        _ro_chart_cancel(cancel_check)
        return new{ndims(values)}(
            admitted_source_values,
            values,
            admitted_source_ids,
            target_ids,
            identity_scope,
            chart_content_sha256,
            admitted_limits,
            content_sha256,
        )
    end
end

function _ro_chart_pullback_result_sha256(
    source_values::Array{Float64},
    values::Array{Float64},
    source_axis_ids::Vector{String},
    target_axis_ids::Vector{String},
    axis_identity_scope::Symbol,
    chart_content_sha256::String,
    ;
    limits::ROAffineInputChartLimits=ROAffineInputChartLimits(),
    work_limits::ROAffineInputChartLimits=limits,
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    array_elements = _ro_chart_array_elements(source_values) +
        _ro_chart_array_elements(values)
    source_count = size(source_values, ndims(source_values))
    control_count = size(values, ndims(values))
    metadata_bytes = _ro_chart_metadata_bytes(
        source_axis_ids,
        target_axis_ids;
        expected_source_count=source_count,
        expected_target_count=control_count,
        limits=work_limits,
        cancel_check=cancel_check,
    ) + BigInt(ncodeunits(chart_content_sha256))
    _ro_chart_limit(:array_elements, array_elements,
        work_limits.max_array_elements)
    _ro_chart_limit(:metadata_bytes, metadata_bytes,
        work_limits.max_metadata_bytes)
    hash_bytes = _ro_chart_pullback_hash_bytes(
        size(source_values),
        size(values),
        source_axis_ids,
        target_axis_ids,
        axis_identity_scope,
        chart_content_sha256,
    )
    _ro_chart_limit(:hash_bytes, hash_bytes, work_limits.max_hash_bytes)

    context = SHA.SHA2_256_CTX()
    _ro_chart_hash_string!(
        context, "bne-ro-affine-pullback-result/v2", cancel_check)
    _ro_chart_hash_string!(context, chart_content_sha256, cancel_check)
    _ro_chart_hash_string!(
        context, String(axis_identity_scope), cancel_check)
    _ro_chart_hash_strings!(context, source_axis_ids, cancel_check)
    _ro_chart_hash_strings!(context, target_axis_ids, cancel_check)
    for array in (source_values, values)
        _ro_chart_hash_uint64!(context, ndims(array), cancel_check)
        for extent in size(array)
            _ro_chart_hash_uint64!(context, extent, cancel_check)
        end
        _ro_chart_hash_array!(context, array, cancel_check)
    end
    _ro_chart_hash_limits!(context, limits, cancel_check)
    _ro_chart_cancel(cancel_check)
    return bytes2hex(SHA.digest!(context))
end

function _ro_chart_assert_unchanged(
    result::ROAffinePullbackResult;
    limits::ROAffineInputChartLimits=getfield(result, :limits),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    expected = _ro_chart_pullback_result_sha256(
        getfield(result, :source_values),
        getfield(result, :values),
        getfield(result, :source_axis_ids),
        getfield(result, :target_axis_ids),
        getfield(result, :axis_identity_scope),
        getfield(result, :chart_content_sha256),
        limits=getfield(result, :limits),
        work_limits=limits,
        cancel_check=cancel_check,
    )
    expected == getfield(result, :content_sha256) || throw(ArgumentError(
        "ROAffinePullbackResult backing storage changed after admission"))
    _ro_chart_cancel(cancel_check)
    return nothing
end

function Base.getproperty(result::ROAffinePullbackResult, name::Symbol)
    _ro_chart_assert_unchanged(result)
    value = getfield(result, name)
    if name === :source_values || name === :values ||
            name === :source_axis_ids || name === :target_axis_ids
        return copy(value)
    end
    return value
end

function _ro_chart_bind_pullback_result(
    chart::ROAffineInputChart,
    source_values::AbstractArray,
    source_axis_ids,
    ;
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    return ROAffinePullbackResult(
        chart,
        source_values,
        source_axis_ids;
        limits=limits,
        cancel_check=cancel_check,
    )
end

"""Re-hash and validate an identity-bound affine pullback against its chart."""
function validate_ro_affine_pullback_result(
    chart::ROAffineInputChart,
    result::ROAffinePullbackResult,
    ;
    limits::ROAffineInputChartLimits=getfield(result, :limits),
    cancel_check=() -> nothing,
)
    _ro_chart_cancel(cancel_check)
    source_values = getfield(result, :source_values)
    values = getfield(result, :values)
    source_count = length(getfield(chart, :source_offset))
    control_count = size(getfield(chart, :source_jacobian), 2)
    source_elements = _ro_chart_array_elements(source_values)
    result_elements = _ro_chart_array_elements(values)
    _ro_chart_limit(
        :array_elements,
        source_elements + result_elements,
        limits.max_array_elements,
    )
    leading_count = source_elements ÷ BigInt(source_count)
    _ro_chart_limit(
        :operation_scalars,
        leading_count * BigInt(source_count) * BigInt(control_count),
        limits.max_operation_scalars,
    )
    _ro_chart_preflight_receipt_hashes(
        chart,
        size(source_values),
        size(values),
        getfield(result, :source_axis_ids),
        getfield(result, :target_axis_ids),
        limits,
        cancel_check,
    )
    _ro_chart_assert_unchanged(
        chart; limits=limits, cancel_check=cancel_check)
    _ro_chart_assert_unchanged(
        result; limits=limits, cancel_check=cancel_check)
    getfield(chart, :axis_identity_scope) ===
        :source_and_target_axis_ids_bound || throw(ArgumentError(
            "pullback receipt validation requires an identity-bound chart"))
    getfield(result, :axis_identity_scope) ===
        getfield(chart, :axis_identity_scope) || throw(ArgumentError(
            "pullback receipt identity scope does not match the chart"))
    getfield(result, :chart_content_sha256) ==
        getfield(chart, :content_sha256) || throw(ArgumentError(
            "pullback receipt chart identity does not match"))
    getfield(result, :source_axis_ids) ==
        getfield(chart, :source_axis_ids) || throw(ArgumentError(
            "pullback receipt source-axis identity does not match the chart"))
    getfield(result, :target_axis_ids) ==
        getfield(chart, :target_axis_ids) || throw(ArgumentError(
            "pullback receipt target-axis identity does not match the chart"))
    replayed = _ro_chart_pullback_values(
        chart,
        getfield(result, :source_values),
        "receipt source_values";
        limits=limits,
        cancel_check=cancel_check,
    )
    isequal(replayed, getfield(result, :values)) || throw(ArgumentError(
        "pullback receipt values do not replay under the bound affine chart"))
    _ro_chart_cancel(cancel_check)
    return result
end

"""
    pullback_ro_matrix(chart, source_matrix) -> Matrix{Float64}

Apply the affine-chain-rule pullback `R_u = R_theta * A` to a matrix whose
columns are in the chart's source-coordinate order. With no keyword this is a
legacy dimension-only numerical transform and returns a `Matrix`. Supplying
`source_axis_ids` requires an identity-bound chart, verifies the exact ordered
source IDs, and returns a sealed `ROAffinePullbackResult` carrying both source
and target IDs. This catches a declared permutation; it cannot independently
authenticate dishonest labels supplied by an upstream producer.
"""
function pullback_ro_matrix(
    chart::ROAffineInputChart,
    source_matrix::AbstractMatrix,
    ;
    source_axis_ids=nothing,
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    source_axis_ids === nothing && return _ro_chart_pullback_values(
        chart,
        source_matrix,
        "source_matrix";
        limits=limits,
        cancel_check=cancel_check,
    )
    return _ro_chart_bind_pullback_result(
        chart,
        source_matrix,
        source_axis_ids;
        limits=limits,
        cancel_check=cancel_check,
    )
end

"""
    pullback_ro_tensor(chart, source_tensor) -> Array{Float64}

Apply `R_u = R_theta * A` independently to every leading-index slice of an
arbitrary-rank tensor. The tensor's last axis must be the ordered source
coordinate axis; all preceding axes and their order are preserved. As with the
matrix entry point, supplying `source_axis_ids` returns the authoritative
identity-bound receipt; omitting them returns only a numerical array.
"""
function pullback_ro_tensor(
    chart::ROAffineInputChart,
    source_tensor::AbstractArray,
    ;
    source_axis_ids=nothing,
    limits::ROAffineInputChartLimits=getfield(chart, :limits),
    cancel_check=() -> nothing,
)
    source_axis_ids === nothing && return _ro_chart_pullback_values(
        chart,
        source_tensor,
        "source_tensor";
        limits=limits,
        cancel_check=cancel_check,
    )
    return _ro_chart_bind_pullback_result(
        chart,
        source_tensor,
        source_axis_ids;
        limits=limits,
        cancel_check=cancel_check,
    )
end
