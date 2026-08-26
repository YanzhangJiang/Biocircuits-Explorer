using LinearAlgebra
import SHA

"""
    ROAffineInputChart(source_offset, source_jacobian;
        rank_rtol=1e-12, max_condition_number=1e10)

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
diagnostics, and seal.
"""
struct ROAffineInputChart
    source_offset::Vector{Float64}
    source_jacobian::Matrix{Float64}
    singular_values::Vector{Float64}
    numerical_rank::Int
    condition_number::Float64
    rank_rtol::Float64
    max_condition_number::Float64
    content_sha256::String

    function ROAffineInputChart(
        source_offset::Vector{Float64},
        source_jacobian::Matrix{Float64},
        singular_values::Vector{Float64},
        numerical_rank::Int,
        condition_number::Float64,
        rank_rtol::Float64,
        max_condition_number::Float64,
        content_sha256::String,
        ::Val{:validated},
    )
        _ro_chart_validate_components(
            source_offset,
            source_jacobian,
            singular_values,
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
        )
        content_sha256 == _ro_chart_content_sha256(
            source_offset,
            source_jacobian,
            singular_values,
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
        ) || throw(ArgumentError(
            "ROAffineInputChart content seal does not match admitted values"))
        return new(
            copy(source_offset),
            copy(source_jacobian),
            copy(singular_values),
            numerical_rank,
            condition_number,
            rank_rtol,
            max_condition_number,
            content_sha256,
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
)
    io = IOBuffer()
    write(io, codeunits("bne-ro-affine-input-chart-memory/v1\0"))
    write(io, htol(UInt64(length(source_offset))))
    write(io, reinterpret(UInt8, source_offset))
    write(io, htol(UInt64(size(source_jacobian, 1))))
    write(io, htol(UInt64(size(source_jacobian, 2))))
    write(io, reinterpret(UInt8, vec(source_jacobian)))
    write(io, htol(UInt64(length(singular_values))))
    write(io, reinterpret(UInt8, singular_values))
    write(io, htol(UInt64(numerical_rank)))
    write(io, reinterpret(UInt8, [
        condition_number,
        rank_rtol,
        max_condition_number,
    ]))
    return bytes2hex(SHA.sha256(take!(io)))
end

function _ro_chart_validate_components(
    source_offset::Vector{Float64},
    source_jacobian::Matrix{Float64},
    singular_values::Vector{Float64},
    numerical_rank::Int,
    condition_number::Float64,
    rank_rtol::Float64,
    max_condition_number::Float64,
)
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
    all(isfinite, source_offset) && all(isfinite, source_jacobian) ||
        throw(ArgumentError("affine chart arrays must contain only finite values"))
    isfinite(rank_rtol) && 0.0 < rank_rtol < 1.0 || throw(ArgumentError(
        "rank_rtol must lie strictly between zero and one"))
    isfinite(max_condition_number) && max_condition_number >= 1.0 ||
        throw(ArgumentError("max_condition_number must be finite and at least one"))

    expected_singular_values = Vector{Float64}(svdvals(source_jacobian))
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
    return nothing
end

function _ro_chart_assert_unchanged(chart::ROAffineInputChart)
    actual = _ro_chart_content_sha256(
        getfield(chart, :source_offset),
        getfield(chart, :source_jacobian),
        getfield(chart, :singular_values),
        getfield(chart, :numerical_rank),
        getfield(chart, :condition_number),
        getfield(chart, :rank_rtol),
        getfield(chart, :max_condition_number),
    )
    actual == getfield(chart, :content_sha256) || throw(ArgumentError(
        "ROAffineInputChart backing storage changed after admission"))
    return nothing
end

function Base.getproperty(chart::ROAffineInputChart, name::Symbol)
    _ro_chart_assert_unchanged(chart)
    value = getfield(chart, name)
    if name === :source_offset ||
            name === :source_jacobian ||
            name === :singular_values
        return copy(value)
    end
    return value
end

function _ro_chart_finite_float64_array(raw::AbstractArray, name::AbstractString)
    all(value -> value isa Real && !(value isa Bool), raw) ||
        throw(ArgumentError("$name must contain only real, non-Boolean values"))
    values = Float64.(raw)
    all(isfinite, values) || throw(ArgumentError("$name must contain only finite values"))
    return values
end

function _ro_chart_finite_real(raw, name::AbstractString)
    raw isa Real && !(raw isa Bool) ||
        throw(ArgumentError("$name must be a real, non-Boolean value"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    return value
end

function ROAffineInputChart(
    source_offset::AbstractVector,
    source_jacobian::AbstractMatrix;
    rank_rtol::Real=1e-12,
    max_condition_number::Real=1e10,
)
    offset = vec(_ro_chart_finite_float64_array(
        source_offset, "source_offset"))
    jacobian = Matrix(_ro_chart_finite_float64_array(
        source_jacobian, "source_jacobian"))
    source_count = length(offset)
    control_count = size(jacobian, 2)

    source_count > 0 || throw(ArgumentError(
        "source_offset must contain at least one source coordinate"))
    control_count > 0 || throw(ArgumentError(
        "source_jacobian must contain at least one control-coordinate column"))
    size(jacobian, 1) == source_count || throw(DimensionMismatch(
        "source_jacobian row count must equal source_offset length"))
    source_count >= control_count || throw(DimensionMismatch(
        "a full-column-rank source_jacobian requires source_count >= control_count"))

    rtol = _ro_chart_finite_real(rank_rtol, "rank_rtol")
    0.0 < rtol < 1.0 || throw(ArgumentError(
        "rank_rtol must lie strictly between zero and one"))
    condition_limit = _ro_chart_finite_real(
        max_condition_number, "max_condition_number")
    condition_limit >= 1.0 || throw(ArgumentError(
        "max_condition_number must be at least one"))

    singular_values = Vector{Float64}(svdvals(jacobian))
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

    # Store detached arrays so later mutation of constructor inputs cannot
    # alter the admitted chart or its recorded diagnostics.
    stored_offset = copy(offset)
    stored_jacobian = copy(jacobian)
    stored_singular_values = copy(singular_values)
    content_sha256 = _ro_chart_content_sha256(
        stored_offset,
        stored_jacobian,
        stored_singular_values,
        numerical_rank,
        condition_number,
        rtol,
        condition_limit,
    )
    return ROAffineInputChart(
        stored_offset,
        stored_jacobian,
        stored_singular_values,
        numerical_rank,
        condition_number,
        rtol,
        condition_limit,
        content_sha256,
        Val(:validated),
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
)
    _ro_chart_assert_unchanged(chart)
    controls = vec(_ro_chart_finite_float64_array(
        control_coordinates, "control_coordinates"))
    expected = size(getfield(chart, :source_jacobian), 2)
    length(controls) == expected || throw(DimensionMismatch(
        "control_coordinates must contain $expected ordered values"))
    mapped = getfield(chart, :source_offset) +
        getfield(chart, :source_jacobian) * controls
    all(isfinite, mapped) || throw(OverflowError(
        "affine input-chart mapping produced a non-finite source coordinate"))
    return mapped
end

function _ro_chart_pullback_values(
    chart::ROAffineInputChart,
    source_values::AbstractArray,
    name::AbstractString,
)
    _ro_chart_assert_unchanged(chart)
    ndims(source_values) >= 1 || throw(DimensionMismatch(
        "$name must have a final source-coordinate axis"))
    source_count = length(getfield(chart, :source_offset))
    size(source_values, ndims(source_values)) == source_count ||
        throw(DimensionMismatch(
            "the final $name axis must have source-coordinate length $source_count"))
    values = _ro_chart_finite_float64_array(source_values, name)
    leading_count = length(values) ÷ source_count
    flattened = reshape(values, leading_count, source_count)
    pulled_back = flattened * getfield(chart, :source_jacobian)
    all(isfinite, pulled_back) || throw(OverflowError(
        "$name pullback produced a non-finite derivative"))
    output_shape = (
        size(values)[1:(ndims(values) - 1)]...,
        size(getfield(chart, :source_jacobian), 2),
    )
    return reshape(pulled_back, output_shape)
end

"""
    pullback_ro_matrix(chart, source_matrix) -> Matrix{Float64}

Apply the affine-chain-rule pullback `R_u = R_theta * A` to a matrix whose
columns are in the chart's source-coordinate order.
"""
function pullback_ro_matrix(
    chart::ROAffineInputChart,
    source_matrix::AbstractMatrix,
)
    return Matrix(_ro_chart_pullback_values(
        chart, source_matrix, "source_matrix"))
end

"""
    pullback_ro_tensor(chart, source_tensor) -> Array{Float64}

Apply `R_u = R_theta * A` independently to every leading-index slice of an
arbitrary-rank tensor. The tensor's last axis must be the ordered source
coordinate axis; all preceding axes and their order are preserved.
"""
function pullback_ro_tensor(
    chart::ROAffineInputChart,
    source_tensor::AbstractArray,
)
    return _ro_chart_pullback_values(
        chart, source_tensor, "source_tensor")
end
