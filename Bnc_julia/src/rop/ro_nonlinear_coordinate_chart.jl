using LinearAlgebra
import SHA

const RO_NONLINEAR_INPUT_CHART_VERSION =
    "bne-ro-nonlinear-input-chart/v1.0.0"
const RO_NONLINEAR_INPUT_CHART_SCOPE =
    :pointwise_numerically_admitted_local_immersion_only

"""Raised before nonlinear-chart admission or evaluation exceeds a hard limit."""
struct RONonlinearChartLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, error::RONonlinearChartLimitExceeded)
    print(io, "nonlinear RO input chart ", error.phase, " requires ",
        error.requested, ", exceeding limit=", error.limit)
end

"""
Pointwise numerical rejection of a local Jacobian.

`reason` is `:non_immersion`, `:rank_grey_zone`, or
`:condition_number_grey_zone`. These are Float64 admission decisions, not
symbolic rank proofs.
"""
struct RONonlinearChartImmersionRejected <: Exception
    reason::Symbol
    smallest_singular_value::Float64
    rank_threshold::Float64
    condition_number::Float64
    maximum_condition_number::Float64
end

function Base.showerror(io::IO, error::RONonlinearChartImmersionRejected)
    print(io, "nonlinear RO input chart rejected local Jacobian: ",
        error.reason, "; smallest singular value=",
        error.smallest_singular_value, ", rank threshold=",
        error.rank_threshold, ", condition number=",
        error.condition_number, ", maximum condition number=",
        error.maximum_condition_number)
end

"""Hard declaration, metadata, evaluation, and factorization limits."""
struct RONonlinearChartLimits
    max_source_coordinates::Int
    max_control_coordinates::Int
    max_coefficient_scalars::Int
    max_metadata_bytes::Int
    max_operation_scalars::Int
    max_factorization_work::Int

    function RONonlinearChartLimits(
        max_source_coordinates::Int,
        max_control_coordinates::Int,
        max_coefficient_scalars::Int,
        max_metadata_bytes::Int,
        max_operation_scalars::Int,
        max_factorization_work::Int,
    )
        values = (
            max_source_coordinates,
            max_control_coordinates,
            max_coefficient_scalars,
            max_metadata_bytes,
            max_operation_scalars,
            max_factorization_work,
        )
        all(>(0), values) || throw(ArgumentError(
            "all nonlinear-chart limits must be positive"))
        return new(values...)
    end
end

function RONonlinearChartLimits(;
    max_source_coordinates::Integer=64,
    max_control_coordinates::Integer=16,
    max_coefficient_scalars::Integer=1_000_000,
    max_metadata_bytes::Integer=1_000_000,
    max_operation_scalars::Integer=10_000_000,
    max_factorization_work::Integer=100_000_000,
)
    raw = (
        max_source_coordinates,
        max_control_coordinates,
        max_coefficient_scalars,
        max_metadata_bytes,
        max_operation_scalars,
        max_factorization_work,
    )
    any(value -> value isa Bool || !(value isa Integer), raw) &&
        throw(ArgumentError(
            "nonlinear-chart limits must be integers, not Bool"))
    all(value -> typemin(Int) <= value <= typemax(Int), raw) ||
        throw(ArgumentError(
            "nonlinear-chart limits must fit in Int"))
    return RONonlinearChartLimits(Int.(raw)...)
end

function _ronc_copy_limits(limits::RONonlinearChartLimits)
    return RONonlinearChartLimits(
        limits.max_source_coordinates,
        limits.max_control_coordinates,
        limits.max_coefficient_scalars,
        limits.max_metadata_bytes,
        limits.max_operation_scalars,
        limits.max_factorization_work,
    )
end

@inline function _ronc_limit(
    phase::Symbol,
    requested::Integer,
    limit::Int,
)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "nonlinear-chart work request for $phase must be nonnegative"))
    amount <= limit || throw(RONonlinearChartLimitExceeded(
        phase, amount, limit))
    return nothing
end

function _ronc_array_scalar_count(raw::AbstractArray)
    amount = BigInt(1)
    for extent in size(raw)
        extent >= 0 || throw(ArgumentError(
            "array extents must be nonnegative"))
        amount *= BigInt(extent)
    end
    return amount
end

@inline function _ronc_cancel(cancel_check)
    cancel_check()
    return nothing
end

"""
    RONonlinearInputChart(; ...)

A finite declarative quadratic map from ordered controls `u` to ordered engine
source coordinates `theta`. With `du = u - control_reference`, the map is

```text
theta[a] = source_reference[a]
         + source_jacobian_at_reference[a, :] * du
         + 1/2 * du' * source_hessians[a, :, :] * du.
```

The representation stores coefficients, names, units, a closed finite box,
and numerical admission policy; it never accepts a user mapping callback.
Every source-component Hessian is exactly symmetric after Float64 admission.

The constructor admits the reference Jacobian only. Every later evaluation
recomputes the local Jacobian and rejects a fold/non-immersion, numerical-rank
grey zone, or condition-number grey zone at that point. Consequently
`immersion_scope` is pointwise and `global_injectivity_certified` is always
false. Neither a successful construction nor finitely many successful point
evaluations is a global coordinate or injectivity certificate.

Array-valued properties are detached snapshots. A content seal detects
lower-level mutation before any later public operation, and the raw validated
constructor independently recomputes admission diagnostics and both hashes.
"""
struct RONonlinearInputChart
    source_coordinate_names::Vector{String}
    source_coordinate_units::Vector{String}
    control_coordinate_names::Vector{String}
    control_coordinate_units::Vector{String}
    control_reference::Vector{Float64}
    domain_lower::Vector{Float64}
    domain_upper::Vector{Float64}
    source_reference::Vector{Float64}
    source_jacobian_at_reference::Matrix{Float64}
    source_hessians::Array{Float64,3}
    rank_atol::Float64
    rank_rtol::Float64
    max_condition_number::Float64
    reference_singular_values::Vector{Float64}
    reference_numerical_rank::Int
    reference_condition_number::Float64
    immersion_scope::Symbol
    global_injectivity_certified::Bool
    limits::RONonlinearChartLimits
    declaration_sha256::String
    content_sha256::String

    function RONonlinearInputChart(
        source_coordinate_names::Vector{String},
        source_coordinate_units::Vector{String},
        control_coordinate_names::Vector{String},
        control_coordinate_units::Vector{String},
        control_reference::Vector{Float64},
        domain_lower::Vector{Float64},
        domain_upper::Vector{Float64},
        source_reference::Vector{Float64},
        source_jacobian_at_reference::Matrix{Float64},
        source_hessians::Array{Float64,3},
        rank_atol::Float64,
        rank_rtol::Float64,
        max_condition_number::Float64,
        reference_singular_values::Vector{Float64},
        reference_numerical_rank::Int,
        reference_condition_number::Float64,
        immersion_scope::Symbol,
        global_injectivity_certified::Bool,
        limits::RONonlinearChartLimits,
        declaration_sha256::String,
        content_sha256::String,
        ::Val{:validated};
        cancel_check=() -> nothing,
    )
        validated_limits = _ronc_copy_limits(limits)
        _ronc_validate_typed_declaration(
            source_coordinate_names,
            source_coordinate_units,
            control_coordinate_names,
            control_coordinate_units,
            control_reference,
            domain_lower,
            domain_upper,
            source_reference,
            source_jacobian_at_reference,
            source_hessians,
            rank_atol,
            rank_rtol,
            max_condition_number,
            validated_limits;
            cancel_check,
        )
        diagnostics = _ronc_local_jacobian_diagnostics(
            source_jacobian_at_reference,
            rank_atol,
            rank_rtol,
            max_condition_number,
            validated_limits;
            cancel_check,
        )
        isequal(reference_singular_values, diagnostics.singular_values) ||
            throw(ArgumentError(
                "reference singular values do not match the declared Jacobian"))
        reference_numerical_rank == diagnostics.numerical_rank ||
            throw(ArgumentError(
                "reference numerical rank does not match the declared Jacobian"))
        isequal(reference_condition_number, diagnostics.condition_number) ||
            throw(ArgumentError(
                "reference condition number does not match the declared Jacobian"))
        immersion_scope == RO_NONLINEAR_INPUT_CHART_SCOPE || throw(ArgumentError(
            "nonlinear input chart must retain its pointwise local-immersion scope"))
        global_injectivity_certified === false || throw(ArgumentError(
            "this contract cannot certify global injectivity"))

        expected_declaration = _ronc_declaration_sha256(
            source_coordinate_names,
            source_coordinate_units,
            control_coordinate_names,
            control_coordinate_units,
            control_reference,
            domain_lower,
            domain_upper,
            source_reference,
            source_jacobian_at_reference,
            source_hessians,
            rank_atol,
            rank_rtol,
            max_condition_number,
            validated_limits;
            cancel_check,
        )
        declaration_sha256 == expected_declaration || throw(ArgumentError(
            "nonlinear input-chart declaration hash does not match admitted values"))
        expected_content = _ronc_content_sha256(
            expected_declaration,
            reference_singular_values,
            reference_numerical_rank,
            reference_condition_number,
            immersion_scope,
            global_injectivity_certified;
            cancel_check,
        )
        content_sha256 == expected_content || throw(ArgumentError(
            "nonlinear input-chart content seal does not match admitted values"))
        _ronc_cancel(cancel_check)
        return new(
            copy(source_coordinate_names),
            copy(source_coordinate_units),
            copy(control_coordinate_names),
            copy(control_coordinate_units),
            copy(control_reference),
            copy(domain_lower),
            copy(domain_upper),
            copy(source_reference),
            copy(source_jacobian_at_reference),
            copy(source_hessians),
            rank_atol,
            rank_rtol,
            max_condition_number,
            copy(reference_singular_values),
            reference_numerical_rank,
            reference_condition_number,
            immersion_scope,
            false,
            validated_limits,
            declaration_sha256,
            content_sha256,
        )
    end
end

function _ronc_metadata_vector(
    raw,
    expected_count::Int,
    label::AbstractString,
    byte_count::Base.RefValue{BigInt},
    limits::RONonlinearChartLimits;
    unique_names::Bool=false,
    cancel_check=() -> nothing,
)
    raw isa Union{Tuple,AbstractVector} || throw(ArgumentError(
        "$label must be a sized tuple or vector"))
    length(raw) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count entries"))
    result = Vector{String}(undef, expected_count)
    for (position, value) in enumerate(raw)
        _ronc_cancel(cancel_check)
        value isa AbstractString || throw(ArgumentError(
            "$label entries must be strings"))
        byte_count[] += ncodeunits(value)
        _ronc_limit(:metadata_bytes, byte_count[], limits.max_metadata_bytes)
        normalized = String(value)
        _ronc_validate_metadata_string(normalized, "$label[$position]")
        result[position] = normalized
    end
    unique_names && !allunique(result) && throw(ArgumentError(
        "$label entries must be unique"))
    return result
end

function _ronc_validate_metadata_string(
    value::String,
    label::AbstractString,
)
    isempty(value) && throw(ArgumentError("$label must be non-empty"))
    occursin('\0', value) && throw(ArgumentError(
        "$label must not contain NUL"))
    strip(value) == value || throw(ArgumentError(
        "$label must not contain leading or trailing whitespace"))
    return nothing
end

function _ronc_validate_sha256(value::String, label::AbstractString)
    ncodeunits(value) == 64 || throw(ArgumentError(
        "$label must be a 64-character lowercase SHA-256 hex digest"))
    all(byte -> UInt8('0') <= byte <= UInt8('9') ||
            UInt8('a') <= byte <= UInt8('f'), codeunits(value)) ||
        throw(ArgumentError(
            "$label must be lowercase SHA-256 hexadecimal"))
    return nothing
end

function _ronc_finite_dense_array(
    raw::AbstractArray,
    label::AbstractString;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    result = Array{Float64}(undef, size(raw))
    for (position, value) in enumerate(raw)
        (position - 1) % 256 == 0 && _ronc_cancel(cancel_check)
        value isa Real && !(value isa Bool) || throw(ArgumentError(
            "$label must contain real, non-Boolean values"))
        converted = Float64(value)
        isfinite(converted) || throw(ArgumentError(
            "$label must contain only finite values"))
        result[position] = converted == 0.0 ? 0.0 : converted
    end
    _ronc_cancel(cancel_check)
    return result
end

function _ronc_finite_vector(
    raw,
    label::AbstractString;
    cancel_check=() -> nothing,
)
    raw isa AbstractVector || throw(ArgumentError(
        "$label must be an AbstractVector"))
    return Vector{Float64}(_ronc_finite_dense_array(
        raw, label; cancel_check))
end

function _ronc_finite_matrix(
    raw,
    label::AbstractString;
    cancel_check=() -> nothing,
)
    raw isa AbstractMatrix || throw(ArgumentError(
        "$label must be an AbstractMatrix"))
    return Matrix{Float64}(_ronc_finite_dense_array(
        raw, label; cancel_check))
end

function _ronc_finite_hessians(
    raw,
    label::AbstractString;
    cancel_check=() -> nothing,
)
    raw isa AbstractArray && ndims(raw) == 3 || throw(ArgumentError(
        "$label must be a rank-three array"))
    return Array{Float64,3}(_ronc_finite_dense_array(
        raw, label; cancel_check))
end

function _ronc_finite_real(raw, label::AbstractString)
    raw isa Real && !(raw isa Bool) || throw(ArgumentError(
        "$label must be a real, non-Boolean value"))
    result = Float64(raw)
    isfinite(result) || throw(ArgumentError("$label must be finite"))
    return result == 0.0 ? 0.0 : result
end

function _ronc_preflight_declaration(
    source_count::Int,
    control_count::Int,
    limits::RONonlinearChartLimits,
)
    source_count > 0 || throw(ArgumentError(
        "at least one source coordinate is required"))
    control_count > 0 || throw(ArgumentError(
        "at least one control coordinate is required"))
    source_count >= control_count || throw(DimensionMismatch(
        "a local immersion requires source_count >= control_count"))
    _ronc_limit(:source_coordinates, source_count,
        limits.max_source_coordinates)
    _ronc_limit(:control_coordinates, control_count,
        limits.max_control_coordinates)
    coefficient_scalars = BigInt(source_count) +
        BigInt(source_count) * control_count +
        BigInt(source_count) * control_count * control_count
    _ronc_limit(:coefficient_scalars, coefficient_scalars,
        limits.max_coefficient_scalars)
    factorization_work = BigInt(source_count) * control_count * control_count
    _ronc_limit(:factorization_work, factorization_work,
        limits.max_factorization_work)
    return nothing
end

function _ronc_validate_typed_declaration(
    source_coordinate_names::Vector{String},
    source_coordinate_units::Vector{String},
    control_coordinate_names::Vector{String},
    control_coordinate_units::Vector{String},
    control_reference::Vector{Float64},
    domain_lower::Vector{Float64},
    domain_upper::Vector{Float64},
    source_reference::Vector{Float64},
    source_jacobian_at_reference::Matrix{Float64},
    source_hessians::Array{Float64,3},
    rank_atol::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    source_count = length(source_reference)
    control_count = length(control_reference)
    _ronc_preflight_declaration(source_count, control_count, limits)
    length(source_coordinate_names) == source_count ||
        throw(DimensionMismatch(
            "source_coordinate_names length must equal source_count"))
    length(source_coordinate_units) == source_count ||
        throw(DimensionMismatch(
            "source_coordinate_units length must equal source_count"))
    length(control_coordinate_names) == control_count ||
        throw(DimensionMismatch(
            "control_coordinate_names length must equal control_count"))
    length(control_coordinate_units) == control_count ||
        throw(DimensionMismatch(
            "control_coordinate_units length must equal control_count"))
    length(domain_lower) == control_count &&
        length(domain_upper) == control_count || throw(DimensionMismatch(
            "domain bounds must match control_count"))
    size(source_jacobian_at_reference) == (source_count, control_count) ||
        throw(DimensionMismatch(
            "source_jacobian_at_reference must have shape source_count x control_count"))
    size(source_hessians) ==
        (source_count, control_count, control_count) ||
        throw(DimensionMismatch(
            "source_hessians must have shape source_count x control_count x control_count"))

    # The raw validated path performs all O(1) shape/coefficient/factorization
    # preflights above, then charges string byte lengths before inspecting any
    # metadata contents.
    metadata_vectors = (
        source_coordinate_names,
        source_coordinate_units,
        control_coordinate_names,
        control_coordinate_units,
    )
    metadata_labels = (
        "source_coordinate_names",
        "source_coordinate_units",
        "control_coordinate_names",
        "control_coordinate_units",
    )
    metadata_bytes = BigInt(0)
    for values in metadata_vectors, value in values
        metadata_bytes += ncodeunits(value)
        _ronc_limit(:metadata_bytes, metadata_bytes,
            limits.max_metadata_bytes)
    end
    for (values, label) in zip(metadata_vectors, metadata_labels)
        for (position, value) in enumerate(values)
            _ronc_cancel(cancel_check)
            _ronc_validate_metadata_string(value, "$label[$position]")
        end
    end
    allunique(source_coordinate_names) || throw(ArgumentError(
        "source coordinate names must be unique"))
    allunique(control_coordinate_names) || throw(ArgumentError(
        "control coordinate names must be unique"))

    for values in (
        control_reference,
        domain_lower,
        domain_upper,
        source_reference,
        source_jacobian_at_reference,
        source_hessians,
    )
        for value in values
            _ronc_cancel(cancel_check)
            isfinite(value) || throw(ArgumentError(
                "nonlinear-chart numeric declarations must be finite"))
        end
    end
    for control in 1:control_count
        _ronc_cancel(cancel_check)
        domain_lower[control] < domain_upper[control] ||
            throw(ArgumentError(
                "each nonlinear-chart domain interval must have positive width"))
        domain_lower[control] <= control_reference[control] <=
            domain_upper[control] || throw(ArgumentError(
                "control_reference must lie in the closed declared domain"))
    end
    for source in 1:source_count, left in 1:control_count,
            right in 1:control_count
        _ronc_cancel(cancel_check)
        isequal(source_hessians[source, left, right],
            source_hessians[source, right, left]) || throw(ArgumentError(
                "every source-component Hessian must be exactly symmetric"))
    end
    isfinite(rank_atol) && rank_atol >= 0.0 || throw(ArgumentError(
        "rank_atol must be finite and nonnegative"))
    isfinite(rank_rtol) && 0.0 < rank_rtol < 1.0 || throw(ArgumentError(
        "rank_rtol must lie strictly between zero and one"))
    isfinite(max_condition_number) && max_condition_number >= 1.0 ||
        throw(ArgumentError(
            "max_condition_number must be finite and at least one"))
    _ronc_cancel(cancel_check)
    return nothing
end

function _ronc_local_jacobian_diagnostics(
    jacobian::Matrix{Float64},
    rank_atol::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    source_count, control_count = size(jacobian)
    factorization_work = BigInt(source_count) * control_count * control_count
    _ronc_limit(:factorization_work, factorization_work,
        limits.max_factorization_work)
    _ronc_cancel(cancel_check)
    singular_values = Vector{Float64}(svdvals(jacobian))
    length(singular_values) == control_count || error(
        "internal nonlinear-chart SVD dimension mismatch")
    all(isfinite, singular_values) || throw(ArgumentError(
        "local Jacobian SVD produced non-finite singular values"))
    largest = first(singular_values)
    smallest = last(singular_values)
    rank_threshold = max(rank_atol, rank_rtol * largest)
    numerical_rank = count(value -> value > rank_threshold, singular_values)
    if smallest == 0.0
        throw(RONonlinearChartImmersionRejected(
            :non_immersion,
            smallest,
            rank_threshold,
            Inf,
            max_condition_number,
        ))
    elseif numerical_rank != control_count
        throw(RONonlinearChartImmersionRejected(
            :rank_grey_zone,
            smallest,
            rank_threshold,
            largest / smallest,
            max_condition_number,
        ))
    end
    condition_number = largest / smallest
    isfinite(condition_number) || throw(RONonlinearChartImmersionRejected(
        :non_immersion,
        smallest,
        rank_threshold,
        condition_number,
        max_condition_number,
    ))
    condition_number <= max_condition_number ||
        throw(RONonlinearChartImmersionRejected(
            :condition_number_grey_zone,
            smallest,
            rank_threshold,
            condition_number,
            max_condition_number,
        ))
    _ronc_cancel(cancel_check)
    return (
        singular_values=singular_values,
        numerical_rank=numerical_rank,
        condition_number=condition_number,
    )
end

@inline function _ronc_write_uint(io::IO, value::Integer)
    value >= 0 || throw(ArgumentError("hash integer must be nonnegative"))
    write(io, htol(UInt64(value)))
end

@inline function _ronc_write_float(io::IO, value::Float64)
    write(io, htol(reinterpret(UInt64, value)))
end

function _ronc_write_string(
    io::IO,
    value::AbstractString;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    _ronc_write_uint(io, ncodeunits(value))
    write(io, codeunits(value))
end

function _ronc_write_strings(
    io::IO,
    values::Vector{String};
    cancel_check=() -> nothing,
)
    _ronc_write_uint(io, length(values))
    for value in values
        _ronc_write_string(io, value; cancel_check)
    end
end

function _ronc_write_float_array(
    io::IO,
    values::AbstractArray{Float64};
    cancel_check=() -> nothing,
)
    _ronc_write_uint(io, ndims(values))
    for dimension in size(values)
        _ronc_write_uint(io, dimension)
    end
    for (position, value) in enumerate(values)
        (position - 1) % 256 == 0 && _ronc_cancel(cancel_check)
        _ronc_write_float(io, value)
    end
    _ronc_cancel(cancel_check)
end

function _ronc_write_limits(
    io::IO,
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    for value in (
        limits.max_source_coordinates,
        limits.max_control_coordinates,
        limits.max_coefficient_scalars,
        limits.max_metadata_bytes,
        limits.max_operation_scalars,
        limits.max_factorization_work,
    )
        _ronc_cancel(cancel_check)
        _ronc_write_uint(io, value)
    end
end

function _ronc_declaration_sha256(
    source_coordinate_names::Vector{String},
    source_coordinate_units::Vector{String},
    control_coordinate_names::Vector{String},
    control_coordinate_units::Vector{String},
    control_reference::Vector{Float64},
    domain_lower::Vector{Float64},
    domain_upper::Vector{Float64},
    source_reference::Vector{Float64},
    source_jacobian_at_reference::Matrix{Float64},
    source_hessians::Array{Float64,3},
    rank_atol::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    io = IOBuffer()
    _ronc_write_string(io, RO_NONLINEAR_INPUT_CHART_VERSION; cancel_check)
    _ronc_write_strings(io, source_coordinate_names; cancel_check)
    _ronc_write_strings(io, source_coordinate_units; cancel_check)
    _ronc_write_strings(io, control_coordinate_names; cancel_check)
    _ronc_write_strings(io, control_coordinate_units; cancel_check)
    _ronc_write_float_array(io, control_reference; cancel_check)
    _ronc_write_float_array(io, domain_lower; cancel_check)
    _ronc_write_float_array(io, domain_upper; cancel_check)
    _ronc_write_float_array(io, source_reference; cancel_check)
    _ronc_write_float_array(io, source_jacobian_at_reference; cancel_check)
    _ronc_write_float_array(io, source_hessians; cancel_check)
    _ronc_write_float(io, rank_atol)
    _ronc_write_float(io, rank_rtol)
    _ronc_write_float(io, max_condition_number)
    _ronc_write_limits(io, limits; cancel_check)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ronc_content_sha256(
    declaration_sha256::String,
    reference_singular_values::Vector{Float64},
    reference_numerical_rank::Int,
    reference_condition_number::Float64,
    immersion_scope::Symbol,
    global_injectivity_certified::Bool;
    cancel_check=() -> nothing,
)
    io = IOBuffer()
    _ronc_write_string(io,
        "bne-ro-nonlinear-input-chart-memory/v1"; cancel_check)
    _ronc_write_string(io, declaration_sha256; cancel_check)
    _ronc_write_float_array(io, reference_singular_values; cancel_check)
    _ronc_write_uint(io, reference_numerical_rank)
    _ronc_write_float(io, reference_condition_number)
    _ronc_write_string(io, String(immersion_scope); cancel_check)
    write(io, UInt8(global_injectivity_certified ? 1 : 0))
    _ronc_cancel(cancel_check)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ronc_assert_unchanged(
    chart::RONonlinearInputChart;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    declaration = _ronc_declaration_sha256(
        getfield(chart, :source_coordinate_names),
        getfield(chart, :source_coordinate_units),
        getfield(chart, :control_coordinate_names),
        getfield(chart, :control_coordinate_units),
        getfield(chart, :control_reference),
        getfield(chart, :domain_lower),
        getfield(chart, :domain_upper),
        getfield(chart, :source_reference),
        getfield(chart, :source_jacobian_at_reference),
        getfield(chart, :source_hessians),
        getfield(chart, :rank_atol),
        getfield(chart, :rank_rtol),
        getfield(chart, :max_condition_number),
        getfield(chart, :limits);
        cancel_check,
    )
    declaration == getfield(chart, :declaration_sha256) ||
        throw(ArgumentError(
            "RONonlinearInputChart declaration changed after admission"))
    content = _ronc_content_sha256(
        declaration,
        getfield(chart, :reference_singular_values),
        getfield(chart, :reference_numerical_rank),
        getfield(chart, :reference_condition_number),
        getfield(chart, :immersion_scope),
        getfield(chart, :global_injectivity_certified);
        cancel_check,
    )
    content == getfield(chart, :content_sha256) || throw(ArgumentError(
        "RONonlinearInputChart backing storage changed after admission"))
    return nothing
end

function Base.getproperty(chart::RONonlinearInputChart, name::Symbol)
    _ronc_assert_unchanged(chart)
    value = getfield(chart, name)
    if name in (
        :source_coordinate_names,
        :source_coordinate_units,
        :control_coordinate_names,
        :control_coordinate_units,
        :control_reference,
        :domain_lower,
        :domain_upper,
        :source_reference,
        :source_jacobian_at_reference,
        :source_hessians,
        :reference_singular_values,
    )
        return copy(value)
    end
    return value
end

function RONonlinearInputChart(;
    source_coordinate_names,
    source_coordinate_units,
    control_coordinate_names,
    control_coordinate_units,
    control_reference,
    domain_lower,
    domain_upper,
    source_reference,
    source_jacobian_at_reference,
    source_hessians,
    rank_atol::Real=0.0,
    rank_rtol::Real=1e-12,
    max_condition_number::Real=1e10,
    limits::RONonlinearChartLimits=RONonlinearChartLimits(),
    cancel_check=() -> nothing,
)
    admitted_limits = _ronc_copy_limits(limits)
    _ronc_cancel(cancel_check)
    source_reference isa AbstractVector || throw(ArgumentError(
        "source_reference must be an AbstractVector"))
    control_reference isa AbstractVector || throw(ArgumentError(
        "control_reference must be an AbstractVector"))
    source_count = length(source_reference)
    control_count = length(control_reference)
    _ronc_preflight_declaration(source_count, control_count, admitted_limits)
    domain_lower isa AbstractVector || throw(ArgumentError(
        "domain_lower must be an AbstractVector"))
    domain_upper isa AbstractVector || throw(ArgumentError(
        "domain_upper must be an AbstractVector"))
    length(domain_lower) == control_count &&
        length(domain_upper) == control_count || throw(DimensionMismatch(
            "domain bounds must match control_count"))
    source_jacobian_at_reference isa AbstractMatrix || throw(ArgumentError(
        "source_jacobian_at_reference must be an AbstractMatrix"))
    size(source_jacobian_at_reference) == (source_count, control_count) ||
        throw(DimensionMismatch(
            "source_jacobian_at_reference must have shape source_count x control_count"))
    source_hessians isa AbstractArray && ndims(source_hessians) == 3 ||
        throw(ArgumentError("source_hessians must be a rank-three array"))
    size(source_hessians) ==
        (source_count, control_count, control_count) ||
        throw(DimensionMismatch(
            "source_hessians must have shape source_count x control_count x control_count"))

    byte_count = Ref(BigInt(0))
    source_names = _ronc_metadata_vector(
        source_coordinate_names,
        source_count,
        "source_coordinate_names",
        byte_count,
        admitted_limits;
        unique_names=true,
        cancel_check=cancel_check,
    )
    source_units = _ronc_metadata_vector(
        source_coordinate_units,
        source_count,
        "source_coordinate_units",
        byte_count,
        admitted_limits;
        cancel_check=cancel_check,
    )
    control_names = _ronc_metadata_vector(
        control_coordinate_names,
        control_count,
        "control_coordinate_names",
        byte_count,
        admitted_limits;
        unique_names=true,
        cancel_check,
    )
    control_units = _ronc_metadata_vector(
        control_coordinate_units,
        control_count,
        "control_coordinate_units",
        byte_count,
        admitted_limits;
        cancel_check,
    )
    controls0 = _ronc_finite_vector(
        control_reference, "control_reference"; cancel_check)
    lower = _ronc_finite_vector(
        domain_lower, "domain_lower"; cancel_check)
    upper = _ronc_finite_vector(
        domain_upper, "domain_upper"; cancel_check)
    sources0 = _ronc_finite_vector(
        source_reference, "source_reference"; cancel_check)
    jacobian0 = _ronc_finite_matrix(
        source_jacobian_at_reference,
        "source_jacobian_at_reference";
        cancel_check,
    )
    hessians = _ronc_finite_hessians(
        source_hessians, "source_hessians"; cancel_check)
    atol = _ronc_finite_real(rank_atol, "rank_atol")
    rtol = _ronc_finite_real(rank_rtol, "rank_rtol")
    condition_limit = _ronc_finite_real(
        max_condition_number, "max_condition_number")
    _ronc_validate_typed_declaration(
        source_names,
        source_units,
        control_names,
        control_units,
        controls0,
        lower,
        upper,
        sources0,
        jacobian0,
        hessians,
        atol,
        rtol,
        condition_limit,
        admitted_limits;
        cancel_check,
    )
    diagnostics = _ronc_local_jacobian_diagnostics(
        jacobian0,
        atol,
        rtol,
        condition_limit,
        admitted_limits;
        cancel_check,
    )
    declaration_sha256 = _ronc_declaration_sha256(
        source_names,
        source_units,
        control_names,
        control_units,
        controls0,
        lower,
        upper,
        sources0,
        jacobian0,
        hessians,
        atol,
        rtol,
        condition_limit,
        admitted_limits;
        cancel_check,
    )
    content_sha256 = _ronc_content_sha256(
        declaration_sha256,
        diagnostics.singular_values,
        diagnostics.numerical_rank,
        diagnostics.condition_number,
        RO_NONLINEAR_INPUT_CHART_SCOPE,
        false;
        cancel_check,
    )
    return RONonlinearInputChart(
        source_names,
        source_units,
        control_names,
        control_units,
        controls0,
        lower,
        upper,
        sources0,
        jacobian0,
        hessians,
        atol,
        rtol,
        condition_limit,
        diagnostics.singular_values,
        diagnostics.numerical_rank,
        diagnostics.condition_number,
        RO_NONLINEAR_INPUT_CHART_SCOPE,
        false,
        admitted_limits,
        declaration_sha256,
        content_sha256,
        Val(:validated);
        cancel_check,
    )
end

"""
A detached point-evaluation snapshot; not a global-coordinate certificate and
not accepted as a trusted input by another public operation.
"""
struct RONonlinearChartEvaluation
    chart_declaration_sha256::String
    control_coordinates::Vector{Float64}
    source_coordinates::Vector{Float64}
    source_jacobian::Matrix{Float64}
    source_hessians::Array{Float64,3}
    singular_values::Vector{Float64}
    numerical_rank::Int
    condition_number::Float64
    immersion_scope::Symbol
    global_injectivity_certified::Bool

    function RONonlinearChartEvaluation(
        chart_declaration_sha256::String,
        control_coordinates::Vector{Float64},
        source_coordinates::Vector{Float64},
        source_jacobian::Matrix{Float64},
        source_hessians::Array{Float64,3},
        singular_values::Vector{Float64},
        numerical_rank::Int,
        condition_number::Float64,
        immersion_scope::Symbol,
        global_injectivity_certified::Bool,
        ::Val{:validated},
    )
        _ronc_validate_sha256(
            chart_declaration_sha256, "chart_declaration_sha256")
        source_count, control_count = size(source_jacobian)
        length(control_coordinates) == control_count || throw(DimensionMismatch(
            "evaluation controls do not match the Jacobian"))
        length(source_coordinates) == source_count || throw(DimensionMismatch(
            "evaluation sources do not match the Jacobian"))
        size(source_hessians) ==
            (source_count, control_count, control_count) ||
            throw(DimensionMismatch(
                "evaluation Hessians do not match the Jacobian"))
        length(singular_values) == control_count || throw(DimensionMismatch(
            "evaluation singular values do not match the control count"))
        numerical_rank == control_count || throw(ArgumentError(
            "evaluation snapshot must represent an admitted local immersion"))
        all(isfinite, control_coordinates) &&
            all(isfinite, source_coordinates) &&
            all(isfinite, source_jacobian) &&
            all(isfinite, source_hessians) &&
            all(isfinite, singular_values) &&
            isfinite(condition_number) || throw(ArgumentError(
                "evaluation snapshot values must be finite"))
        all(>(0.0), singular_values) &&
            condition_number >= 1.0 || throw(ArgumentError(
                "evaluation snapshot diagnostics must be positive"))
        immersion_scope == RO_NONLINEAR_INPUT_CHART_SCOPE || throw(ArgumentError(
            "evaluation snapshot must retain the pointwise scope"))
        global_injectivity_certified === false || throw(ArgumentError(
            "evaluation snapshot cannot certify global injectivity"))
        return new(
            chart_declaration_sha256,
            copy(control_coordinates),
            copy(source_coordinates),
            copy(source_jacobian),
            copy(source_hessians),
            copy(singular_values),
            numerical_rank,
            condition_number,
            immersion_scope,
            false,
        )
    end
end

function _ronc_evaluation_work(
    source_count::Int,
    control_count::Int,
)
    chart_seal = BigInt(source_count) * control_count * control_count +
        BigInt(source_count) * control_count +
        BigInt(2) * source_count + BigInt(4) * control_count
    map_and_jacobian = BigInt(source_count) * control_count * control_count +
        BigInt(source_count) * control_count + source_count
    return chart_seal + map_and_jacobian
end

function _ronc_evaluation_work(chart::RONonlinearInputChart)
    source_count = length(getfield(chart, :source_reference))
    control_count = length(getfield(chart, :control_reference))
    source_count > 0 && control_count > 0 || throw(ArgumentError(
        "nonlinear chart coordinate counts changed after admission"))
    chart_seal = BigInt(source_count)
    for name in (
        :control_reference,
        :domain_lower,
        :domain_upper,
        :source_reference,
        :source_jacobian_at_reference,
        :source_hessians,
        :reference_singular_values,
    )
        chart_seal += _ronc_array_scalar_count(getfield(chart, name))
    end
    map_and_jacobian = BigInt(source_count) * control_count * control_count +
        BigInt(source_count) * control_count + source_count
    return chart_seal + map_and_jacobian
end

"""
    evaluate_ro_nonlinear_input_chart(chart, controls; cancel_check)

Evaluate the declared quadratic map and its local Jacobian at one point in the
closed domain. The point is returned only after its local Jacobian passes the
recorded numerical immersion and conditioning policy.
"""
function evaluate_ro_nonlinear_input_chart(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    source_count = length(getfield(chart, :source_reference))
    control_count = length(getfield(chart, :control_reference))
    limits = getfield(chart, :limits)
    _ronc_limit(:operation_scalars,
        _ronc_evaluation_work(chart),
        limits.max_operation_scalars)
    _ronc_assert_unchanged(chart; cancel_check)
    length(control_coordinates) == control_count || throw(DimensionMismatch(
        "control_coordinates must contain $control_count ordered values"))
    controls = _ronc_finite_vector(
        control_coordinates, "control_coordinates"; cancel_check)
    for control in 1:control_count
        getfield(chart, :domain_lower)[control] <= controls[control] <=
            getfield(chart, :domain_upper)[control] || throw(DomainError(
                controls[control],
                "control coordinate $control lies outside the closed chart domain",
            ))
    end
    delta = controls - getfield(chart, :control_reference)
    jacobian = copy(getfield(chart, :source_jacobian_at_reference))
    sources = getfield(chart, :source_reference) + jacobian * delta
    hessians = getfield(chart, :source_hessians)
    for source in 1:source_count
        _ronc_cancel(cancel_check)
        component_hessian = @view hessians[source, :, :]
        sources[source] += 0.5 * dot(delta, component_hessian * delta)
        @views jacobian[source, :] .+= component_hessian * delta
    end
    all(isfinite, sources) && all(isfinite, jacobian) || throw(OverflowError(
        "quadratic nonlinear-chart evaluation produced a non-finite value"))
    diagnostics = _ronc_local_jacobian_diagnostics(
        jacobian,
        getfield(chart, :rank_atol),
        getfield(chart, :rank_rtol),
        getfield(chart, :max_condition_number),
        limits;
        cancel_check,
    )
    return RONonlinearChartEvaluation(
        getfield(chart, :declaration_sha256),
        copy(controls),
        copy(sources),
        copy(jacobian),
        copy(hessians),
        copy(diagnostics.singular_values),
        diagnostics.numerical_rank,
        diagnostics.condition_number,
        RO_NONLINEAR_INPUT_CHART_SCOPE,
        false,
        Val(:validated),
    )
end

function map_ro_nonlinear_source_coordinates(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector;
    cancel_check=() -> nothing,
)
    return evaluate_ro_nonlinear_input_chart(
        chart, control_coordinates; cancel_check).source_coordinates
end

function ro_nonlinear_source_jacobian(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector;
    cancel_check=() -> nothing,
)
    return evaluate_ro_nonlinear_input_chart(
        chart, control_coordinates; cancel_check).source_jacobian
end

function _ronc_pullback_values(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector,
    source_values::AbstractArray,
    label::AbstractString;
    source_coordinate_names,
    source_coordinate_units,
    source_coordinates,
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    ndims(source_values) >= 1 || throw(DimensionMismatch(
        "$label must have a final source-coordinate axis"))
    source_count = length(getfield(chart, :source_reference))
    control_count = length(getfield(chart, :control_reference))
    source_count > 0 && control_count > 0 || throw(ArgumentError(
        "nonlinear chart coordinate counts changed after admission"))
    size(source_values, ndims(source_values)) == source_count ||
        throw(DimensionMismatch(
            "the final $label axis must have source-coordinate length $source_count"))
    limits = getfield(chart, :limits)
    input_scalars = _ronc_array_scalar_count(source_values)
    leading_count = input_scalars ÷ source_count
    output_scalars = leading_count * control_count
    work = _ronc_evaluation_work(chart) +
        input_scalars + output_scalars +
        leading_count * source_count * control_count
    _ronc_limit(:operation_scalars, work, limits.max_operation_scalars)
    leading_count <= typemax(Int) || throw(RONonlinearChartLimitExceeded(
        :operation_scalars, leading_count, limits.max_operation_scalars))
    evaluation = evaluate_ro_nonlinear_input_chart(
        chart, control_coordinates; cancel_check)
    source_coordinates isa AbstractVector || throw(ArgumentError(
        "source_coordinates must be an AbstractVector"))
    length(source_coordinates) == source_count || throw(DimensionMismatch(
        "source_coordinates must match the source axis"))
    byte_count = Ref(BigInt(0))
    declared_names = _ronc_metadata_vector(
        source_coordinate_names,
        source_count,
        "source_coordinate_names",
        byte_count,
        limits;
        unique_names=true,
        cancel_check,
    )
    declared_units = _ronc_metadata_vector(
        source_coordinate_units,
        source_count,
        "source_coordinate_units",
        byte_count,
        limits;
        cancel_check,
    )
    declared_point = _ronc_finite_vector(
        source_coordinates, "source_coordinates"; cancel_check)
    declared_names == getfield(chart, :source_coordinate_names) ||
        throw(ArgumentError(
            "$label source-coordinate names/order do not match the chart"))
    declared_units == getfield(chart, :source_coordinate_units) ||
        throw(ArgumentError(
            "$label source-coordinate units do not match the chart"))
    isequal(declared_point, evaluation.source_coordinates) ||
        throw(ArgumentError(
            "$label was declared at a different source-coordinate point"))
    values = _ronc_finite_hyperarray(
        source_values, label; cancel_check)
    flattened = reshape(values, Int(leading_count), source_count)
    pulled_back = flattened * evaluation.source_jacobian
    all(isfinite, pulled_back) || throw(OverflowError(
        "$label nonlinear pullback produced a non-finite derivative"))
    output_shape = (
        size(values)[1:(ndims(values) - 1)]...,
        control_count,
    )
    _ronc_cancel(cancel_check)
    return reshape(pulled_back, output_shape)
end

function _ronc_finite_hyperarray(
    raw::AbstractArray,
    label::AbstractString;
    cancel_check=() -> nothing,
)
    return _ronc_finite_dense_array(raw, label; cancel_check)
end

"""
Apply `R_u = R_theta * J_theta(u)` to a source-coordinate matrix.

The caller must declare the ordered source-axis names, units, and exact source
point. Those declarations are checked against the chart evaluation, but this
finite API does not independently prove that an upstream array was labelled
honestly.
"""
function pullback_ro_nonlinear_matrix(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector,
    source_matrix::AbstractMatrix;
    source_coordinate_names,
    source_coordinate_units,
    source_coordinates,
    cancel_check=() -> nothing,
)
    return Matrix(_ronc_pullback_values(
        chart,
        control_coordinates,
        source_matrix,
        "source_matrix";
        source_coordinate_names,
        source_coordinate_units,
        source_coordinates,
        cancel_check,
    ))
end

"""Apply the nonlinear first-order pullback along an arbitrary final axis."""
function pullback_ro_nonlinear_tensor(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector,
    source_tensor::AbstractArray;
    source_coordinate_names,
    source_coordinate_units,
    source_coordinates,
    cancel_check=() -> nothing,
)
    return _ronc_pullback_values(
        chart,
        control_coordinates,
        source_tensor,
        "source_tensor";
        source_coordinate_names,
        source_coordinate_units,
        source_coordinates,
        cancel_check,
    )
end

"""
Source-coordinate derivatives for one scalar output at one exact ordered
source-coordinate point. Raw gradient/Hessian arrays are not accepted directly
by the nonlinear second-order pullback because their chart, scalar-output
identity, evaluation point, column order, and units could otherwise be silently
relabelled.
"""
struct RONonlinearSourceDerivatives
    chart_declaration_sha256::String
    output_name::String
    output_unit::String
    source_coordinate_names::Vector{String}
    source_coordinate_units::Vector{String}
    source_coordinates::Vector{Float64}
    gradient::Vector{Float64}
    hessian::Matrix{Float64}
    limits::RONonlinearChartLimits
    content_sha256::String

    function RONonlinearSourceDerivatives(
        chart_declaration_sha256::String,
        output_name::String,
        output_unit::String,
        source_coordinate_names::Vector{String},
        source_coordinate_units::Vector{String},
        source_coordinates::Vector{Float64},
        gradient::Vector{Float64},
        hessian::Matrix{Float64},
        limits::RONonlinearChartLimits,
        content_sha256::String,
        ::Val{:validated};
        cancel_check=() -> nothing,
    )
        admitted_limits = _ronc_copy_limits(limits)
        _ronc_validate_source_derivatives_typed(
            chart_declaration_sha256,
            output_name,
            output_unit,
            source_coordinate_names,
            source_coordinate_units,
            source_coordinates,
            gradient,
            hessian,
            admitted_limits;
            cancel_check,
        )
        expected = _ronc_source_derivatives_sha256(
            chart_declaration_sha256,
            output_name,
            output_unit,
            source_coordinate_names,
            source_coordinate_units,
            source_coordinates,
            gradient,
            hessian,
            admitted_limits;
            cancel_check,
        )
        content_sha256 == expected || throw(ArgumentError(
            "source-derivative content seal does not match admitted values"))
        return new(
            chart_declaration_sha256,
            output_name,
            output_unit,
            copy(source_coordinate_names),
            copy(source_coordinate_units),
            copy(source_coordinates),
            copy(gradient),
            copy(hessian),
            admitted_limits,
            content_sha256,
        )
    end
end

function _ronc_validate_source_derivatives_typed(
    chart_declaration_sha256::String,
    output_name::String,
    output_unit::String,
    source_coordinate_names::Vector{String},
    source_coordinate_units::Vector{String},
    source_coordinates::Vector{Float64},
    gradient::Vector{Float64},
    hessian::Matrix{Float64},
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    source_count = length(gradient)
    source_count > 0 || throw(ArgumentError(
        "source derivatives require at least one source coordinate"))
    _ronc_limit(:source_coordinates, source_count,
        limits.max_source_coordinates)
    size(hessian) == (source_count, source_count) ||
        throw(DimensionMismatch(
            "source derivative Hessian must have shape source_count x source_count"))
    length(source_coordinate_names) == source_count ||
        throw(DimensionMismatch(
            "source derivative names must match source_count"))
    length(source_coordinate_units) == source_count ||
        throw(DimensionMismatch(
            "source derivative units must match source_count"))
    length(source_coordinates) == source_count ||
        throw(DimensionMismatch(
            "source derivative evaluation point must match source_count"))
    _ronc_limit(:operation_scalars,
        BigInt(2) * source_count + BigInt(source_count) * source_count,
        limits.max_operation_scalars)

    # Charge bounded byte lengths before metadata content inspection.
    metadata_bytes = BigInt(ncodeunits(chart_declaration_sha256)) +
        ncodeunits(output_name) + ncodeunits(output_unit)
    _ronc_limit(:metadata_bytes, metadata_bytes,
        limits.max_metadata_bytes)
    _ronc_validate_sha256(
        chart_declaration_sha256, "chart_declaration_sha256")
    _ronc_validate_metadata_string(output_name, "output_name")
    _ronc_validate_metadata_string(output_unit, "output_unit")
    for values in (source_coordinate_names, source_coordinate_units),
            value in values
        metadata_bytes += ncodeunits(value)
        _ronc_limit(:metadata_bytes, metadata_bytes,
            limits.max_metadata_bytes)
    end
    for (values, label) in (
        (source_coordinate_names, "source_coordinate_names"),
        (source_coordinate_units, "source_coordinate_units"),
    )
        for (position, value) in enumerate(values)
            _ronc_cancel(cancel_check)
            _ronc_validate_metadata_string(value, "$label[$position]")
        end
    end
    allunique(source_coordinate_names) || throw(ArgumentError(
        "source derivative coordinate names must be unique"))
    for values in (source_coordinates, gradient, hessian), value in values
        _ronc_cancel(cancel_check)
        isfinite(value) || throw(ArgumentError(
            "source derivatives must contain only finite values"))
    end
    for left in 1:source_count, right in 1:source_count
        _ronc_cancel(cancel_check)
        isequal(hessian[left, right], hessian[right, left]) ||
            throw(ArgumentError(
                "source derivative Hessian must be exactly symmetric"))
    end
    _ronc_cancel(cancel_check)
    return nothing
end

function _ronc_source_derivatives_sha256(
    chart_declaration_sha256::String,
    output_name::String,
    output_unit::String,
    source_coordinate_names::Vector{String},
    source_coordinate_units::Vector{String},
    source_coordinates::Vector{Float64},
    gradient::Vector{Float64},
    hessian::Matrix{Float64},
    limits::RONonlinearChartLimits;
    cancel_check=() -> nothing,
)
    io = IOBuffer()
    _ronc_write_string(
        io, "bne-ro-nonlinear-source-derivatives/v1.2.0"; cancel_check)
    _ronc_write_string(io, chart_declaration_sha256; cancel_check)
    _ronc_write_string(io, output_name; cancel_check)
    _ronc_write_string(io, output_unit; cancel_check)
    _ronc_write_strings(io, source_coordinate_names; cancel_check)
    _ronc_write_strings(io, source_coordinate_units; cancel_check)
    _ronc_write_float_array(io, source_coordinates; cancel_check)
    _ronc_write_float_array(io, gradient; cancel_check)
    _ronc_write_float_array(io, hessian; cancel_check)
    _ronc_write_limits(io, limits; cancel_check)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ronc_assert_unchanged(
    derivatives::RONonlinearSourceDerivatives;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    expected = _ronc_source_derivatives_sha256(
        getfield(derivatives, :chart_declaration_sha256),
        getfield(derivatives, :output_name),
        getfield(derivatives, :output_unit),
        getfield(derivatives, :source_coordinate_names),
        getfield(derivatives, :source_coordinate_units),
        getfield(derivatives, :source_coordinates),
        getfield(derivatives, :gradient),
        getfield(derivatives, :hessian),
        getfield(derivatives, :limits);
        cancel_check,
    )
    expected == getfield(derivatives, :content_sha256) ||
        throw(ArgumentError(
            "RONonlinearSourceDerivatives backing storage changed after admission"))
    return nothing
end

function Base.getproperty(
    derivatives::RONonlinearSourceDerivatives,
    name::Symbol,
)
    _ronc_assert_unchanged(derivatives)
    value = getfield(derivatives, name)
    if name in (
        :source_coordinate_names,
        :source_coordinate_units,
        :source_coordinates,
        :gradient,
        :hessian,
    )
        return copy(value)
    end
    return value
end

function RONonlinearSourceDerivatives(;
    chart_declaration_sha256,
    output_name,
    output_unit,
    source_coordinate_names,
    source_coordinate_units,
    source_coordinates,
    gradient,
    hessian,
    limits::RONonlinearChartLimits=RONonlinearChartLimits(),
    cancel_check=() -> nothing,
)
    admitted_limits = _ronc_copy_limits(limits)
    _ronc_cancel(cancel_check)
    chart_declaration_sha256 isa AbstractString || throw(ArgumentError(
        "chart_declaration_sha256 must be a string"))
    output_name isa AbstractString || throw(ArgumentError(
        "output_name must be a string"))
    output_unit isa AbstractString || throw(ArgumentError(
        "output_unit must be a string"))
    gradient isa AbstractVector || throw(ArgumentError(
        "source derivative gradient must be an AbstractVector"))
    source_coordinates isa AbstractVector || throw(ArgumentError(
        "source derivative evaluation point must be an AbstractVector"))
    hessian isa AbstractMatrix || throw(ArgumentError(
        "source derivative Hessian must be an AbstractMatrix"))
    source_count = length(gradient)
    source_count > 0 || throw(ArgumentError(
        "source derivatives require at least one source coordinate"))
    _ronc_limit(:source_coordinates, source_count,
        admitted_limits.max_source_coordinates)
    size(hessian) == (source_count, source_count) ||
        throw(DimensionMismatch(
            "source derivative Hessian must have shape source_count x source_count"))
    length(source_coordinates) == source_count || throw(DimensionMismatch(
        "source derivative evaluation point must match source_count"))
    _ronc_limit(:operation_scalars,
        BigInt(2) * source_count + BigInt(source_count) * source_count,
        admitted_limits.max_operation_scalars)
    identity_metadata_bytes = BigInt(ncodeunits(chart_declaration_sha256)) +
        ncodeunits(output_name) + ncodeunits(output_unit)
    _ronc_limit(:metadata_bytes, identity_metadata_bytes,
        admitted_limits.max_metadata_bytes)
    admitted_chart_sha256 = String(chart_declaration_sha256)
    admitted_output_name = String(output_name)
    admitted_output_unit = String(output_unit)
    _ronc_validate_sha256(
        admitted_chart_sha256, "chart_declaration_sha256")
    _ronc_validate_metadata_string(admitted_output_name, "output_name")
    _ronc_validate_metadata_string(admitted_output_unit, "output_unit")
    byte_count = Ref(identity_metadata_bytes)
    names = _ronc_metadata_vector(
        source_coordinate_names,
        source_count,
        "source_coordinate_names",
        byte_count,
        admitted_limits;
        unique_names=true,
        cancel_check,
    )
    units = _ronc_metadata_vector(
        source_coordinate_units,
        source_count,
        "source_coordinate_units",
        byte_count,
        admitted_limits;
        cancel_check,
    )
    admitted_source_coordinates = _ronc_finite_vector(
        source_coordinates, "source_coordinates"; cancel_check)
    admitted_gradient = _ronc_finite_vector(
        gradient, "gradient"; cancel_check)
    admitted_hessian = _ronc_finite_matrix(
        hessian, "hessian"; cancel_check)
    _ronc_validate_source_derivatives_typed(
        admitted_chart_sha256,
        admitted_output_name,
        admitted_output_unit,
        names,
        units,
        admitted_source_coordinates,
        admitted_gradient,
        admitted_hessian,
        admitted_limits;
        cancel_check,
    )
    content_sha256 = _ronc_source_derivatives_sha256(
        admitted_chart_sha256,
        admitted_output_name,
        admitted_output_unit,
        names,
        units,
        admitted_source_coordinates,
        admitted_gradient,
        admitted_hessian,
        admitted_limits;
        cancel_check,
    )
    return RONonlinearSourceDerivatives(
        admitted_chart_sha256,
        admitted_output_name,
        admitted_output_unit,
        names,
        units,
        admitted_source_coordinates,
        admitted_gradient,
        admitted_hessian,
        admitted_limits,
        content_sha256,
        Val(:validated);
        cancel_check,
    )
end

"""
Detached audit snapshot for one scalar-output second-order pullback.

`affine_sandwich_term` is `J' * H_theta(z) * J` and
`chart_hessian_term` is `sum_a grad_theta(z)[a] * H_u(theta[a])`.
`control_hessian` is their unsuppressed sum.
"""
struct RONonlinearHessianPullback
    chart_declaration_sha256::String
    source_derivatives_sha256::String
    output_name::String
    output_unit::String
    control_coordinates::Vector{Float64}
    source_coordinates::Vector{Float64}
    local_source_jacobian::Matrix{Float64}
    control_gradient::Vector{Float64}
    affine_sandwich_term::Matrix{Float64}
    chart_hessian_term::Matrix{Float64}
    control_hessian::Matrix{Float64}

    function RONonlinearHessianPullback(
        chart_declaration_sha256::String,
        source_derivatives_sha256::String,
        output_name::String,
        output_unit::String,
        control_coordinates::Vector{Float64},
        source_coordinates::Vector{Float64},
        local_source_jacobian::Matrix{Float64},
        control_gradient::Vector{Float64},
        affine_sandwich_term::Matrix{Float64},
        chart_hessian_term::Matrix{Float64},
        control_hessian::Matrix{Float64},
        ::Val{:validated},
    )
        _ronc_validate_sha256(
            chart_declaration_sha256, "chart_declaration_sha256")
        _ronc_validate_sha256(
            source_derivatives_sha256, "source_derivatives_sha256")
        _ronc_validate_metadata_string(output_name, "output_name")
        _ronc_validate_metadata_string(output_unit, "output_unit")
        source_count, control_count = size(local_source_jacobian)
        length(source_coordinates) == source_count || throw(DimensionMismatch(
            "pullback source coordinates do not match the Jacobian"))
        length(control_coordinates) == control_count &&
            length(control_gradient) == control_count ||
            throw(DimensionMismatch(
                "pullback controls do not match the Jacobian"))
        for (label, values) in (
            ("affine_sandwich_term", affine_sandwich_term),
            ("chart_hessian_term", chart_hessian_term),
            ("control_hessian", control_hessian),
        )
            size(values) == (control_count, control_count) ||
                throw(DimensionMismatch(
                    "$label must be control_count x control_count"))
        end
        all(isfinite, control_coordinates) &&
            all(isfinite, source_coordinates) &&
            all(isfinite, local_source_jacobian) &&
            all(isfinite, control_gradient) &&
            all(isfinite, affine_sandwich_term) &&
            all(isfinite, chart_hessian_term) &&
            all(isfinite, control_hessian) || throw(ArgumentError(
                "pullback snapshot values must be finite"))
        isequal(control_hessian,
            affine_sandwich_term + chart_hessian_term) ||
            throw(ArgumentError(
                "control_hessian must retain both chain-rule terms"))
        return new(
            chart_declaration_sha256,
            source_derivatives_sha256,
            output_name,
            output_unit,
            copy(control_coordinates),
            copy(source_coordinates),
            copy(local_source_jacobian),
            copy(control_gradient),
            copy(affine_sandwich_term),
            copy(chart_hessian_term),
            copy(control_hessian),
        )
    end
end

"""
    pullback_ro_nonlinear_hessian(chart, controls, source_derivatives)

Apply the complete scalar-output second-order chain rule

```text
H_u z = J' * H_theta z * J
      + sum_a grad_theta(z)[a] * H_u theta[a].
```

The second term is retained even when it reverses the sign of the affine
sandwich. No causal or chart-invariant synergy interpretation is attached.
"""
function pullback_ro_nonlinear_hessian(
    chart::RONonlinearInputChart,
    control_coordinates::AbstractVector,
    source_derivatives::RONonlinearSourceDerivatives;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    source_count = length(getfield(chart, :source_reference))
    control_count = length(getfield(chart, :control_reference))
    limits = getfield(chart, :limits)
    source_derivative_seal_work = BigInt(0)
    for name in (:source_coordinates, :gradient, :hessian)
        source_derivative_seal_work += _ronc_array_scalar_count(
            getfield(source_derivatives, name))
    end
    pullback_work = BigInt(source_count) * source_count +
        BigInt(source_count) * control_count * control_count +
        BigInt(source_count) * source_count * control_count +
        BigInt(source_count) * control_count
    cumulative_work = _ronc_evaluation_work(chart) +
        source_derivative_seal_work + pullback_work
    _ronc_limit(:operation_scalars, cumulative_work,
        limits.max_operation_scalars)
    evaluation = evaluate_ro_nonlinear_input_chart(
        chart, control_coordinates; cancel_check)
    _ronc_assert_unchanged(source_derivatives; cancel_check)
    getfield(source_derivatives, :chart_declaration_sha256) ==
        getfield(chart, :declaration_sha256) || throw(ArgumentError(
            "source derivatives were declared for a different input chart"))
    getfield(source_derivatives, :source_coordinate_names) ==
        getfield(chart, :source_coordinate_names) || throw(ArgumentError(
            "source derivative coordinate names/order do not match the chart"))
    getfield(source_derivatives, :source_coordinate_units) ==
        getfield(chart, :source_coordinate_units) || throw(ArgumentError(
            "source derivative coordinate units do not match the chart"))
    isequal(
        getfield(source_derivatives, :source_coordinates),
        evaluation.source_coordinates,
    ) || throw(ArgumentError(
        "source derivatives were evaluated at a different source-coordinate point"))
    gradient = getfield(source_derivatives, :gradient)
    hessian = getfield(source_derivatives, :hessian)
    length(gradient) == source_count &&
        size(hessian) == (source_count, source_count) ||
        throw(DimensionMismatch(
            "source derivatives do not match the chart source count"))
    jacobian = evaluation.source_jacobian
    affine_term = transpose(jacobian) * hessian * jacobian
    chart_term = zeros(Float64, control_count, control_count)
    chart_hessians = getfield(chart, :source_hessians)
    for source in 1:source_count
        _ronc_cancel(cancel_check)
        @views chart_term .+= gradient[source] .* chart_hessians[source, :, :]
    end
    control_hessian = affine_term + chart_term
    control_gradient = vec(transpose(gradient) * jacobian)
    all(isfinite, affine_term) && all(isfinite, chart_term) &&
        all(isfinite, control_hessian) && all(isfinite, control_gradient) ||
        throw(OverflowError(
            "nonlinear second-order pullback produced a non-finite derivative"))
    _ronc_cancel(cancel_check)
    return RONonlinearHessianPullback(
        getfield(chart, :declaration_sha256),
        getfield(source_derivatives, :content_sha256),
        getfield(source_derivatives, :output_name),
        getfield(source_derivatives, :output_unit),
        copy(evaluation.control_coordinates),
        copy(evaluation.source_coordinates),
        copy(jacobian),
        copy(control_gradient),
        copy(affine_term),
        copy(chart_term),
        copy(control_hessian),
        Val(:validated),
    )
end

"""
Rebuild the chart from its sealed declarative values and rerun reference-point
admission. Equality of declaration/content hashes is required. This is finite
local replay, not a proof of global injectivity over the domain.
"""
function replay_ro_nonlinear_input_chart(
    chart::RONonlinearInputChart;
    cancel_check=() -> nothing,
)
    _ronc_cancel(cancel_check)
    _ronc_assert_unchanged(chart; cancel_check)
    rebuilt = RONonlinearInputChart(
        source_coordinate_names=getfield(chart, :source_coordinate_names),
        source_coordinate_units=getfield(chart, :source_coordinate_units),
        control_coordinate_names=getfield(chart, :control_coordinate_names),
        control_coordinate_units=getfield(chart, :control_coordinate_units),
        control_reference=getfield(chart, :control_reference),
        domain_lower=getfield(chart, :domain_lower),
        domain_upper=getfield(chart, :domain_upper),
        source_reference=getfield(chart, :source_reference),
        source_jacobian_at_reference=
            getfield(chart, :source_jacobian_at_reference),
        source_hessians=getfield(chart, :source_hessians),
        rank_atol=getfield(chart, :rank_atol),
        rank_rtol=getfield(chart, :rank_rtol),
        max_condition_number=getfield(chart, :max_condition_number),
        limits=getfield(chart, :limits),
        cancel_check=cancel_check,
    )
    getfield(rebuilt, :declaration_sha256) ==
        getfield(chart, :declaration_sha256) || error(
            "nonlinear input-chart replay changed the declaration hash")
    getfield(rebuilt, :content_sha256) == getfield(chart, :content_sha256) ||
        error("nonlinear input-chart replay changed the content hash")
    return rebuilt
end
