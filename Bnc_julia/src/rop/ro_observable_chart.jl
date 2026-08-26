import SHA

const RO_OBSERVABLE_CHART_VERSION = "bne-ro-observable-chart/v1.0.0"
const RO_OBSERVABLE_CHART_SCOPE =
    "finite_declarative_affine_or_quadratic_observable_map"

"""Raised before a declarative observable-chart operation exceeds its budget."""
struct ROObservableChartLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROObservableChartLimitExceeded)
    print(io, "RO observable-chart ", err.phase, " requires ",
        err.requested, " work units, exceeding limit=", err.limit)
end

"""
    ROObservableChartLimits(; ...)

Hard pre-allocation limits for one finite declarative observable map and one
second-order composition. A work unit is one declared scalar coefficient or
one scalar multiply-accumulate term in the formulas documented below; it is a
deterministic admission measure, not elapsed time.
"""
struct ROObservableChartLimits
    max_source_components::Int
    max_output_components::Int
    max_coefficients::Int
    max_control_components::Int
    max_chain_rule_terms::Int
    max_metadata_bytes::Int
end

function _roo_positive_int(raw::Integer, name::AbstractString)
    raw > 0 || throw(ArgumentError("$name must be positive"))
    raw <= typemax(Int) || throw(ArgumentError("$name must fit in Int"))
    return Int(raw)
end

function ROObservableChartLimits(;
    max_source_components::Integer=64,
    max_output_components::Integer=64,
    max_coefficients::Integer=1_000_000,
    max_control_components::Integer=64,
    max_chain_rule_terms::Integer=10_000_000,
    max_metadata_bytes::Integer=65_536,
)
    return ROObservableChartLimits(
        _roo_positive_int(max_source_components, "max_source_components"),
        _roo_positive_int(max_output_components, "max_output_components"),
        _roo_positive_int(max_coefficients, "max_coefficients"),
        _roo_positive_int(max_control_components, "max_control_components"),
        _roo_positive_int(max_chain_rule_terms, "max_chain_rule_terms"),
        _roo_positive_int(max_metadata_bytes, "max_metadata_bytes"),
    )
end

function _roo_validate_limits(limits::ROObservableChartLimits)
    for name in fieldnames(ROObservableChartLimits)
        getfield(limits, name) > 0 || throw(ArgumentError(
            "$(name) must be positive"))
    end
    return nothing
end

@inline function _roo_limit(
    phase::Symbol,
    requested::BigInt,
    limit::Int,
)
    requested <= limit || throw(ROObservableChartLimitExceeded(
        phase, requested, limit))
    return nothing
end

function _roo_string_vector(
    raw::AbstractVector,
    name::AbstractString;
    unique_values::Bool,
)
    values = String[]
    sizehint!(values, length(raw))
    for (index, item) in pairs(raw)
        item isa AbstractString || throw(ArgumentError(
            "$name[$index] must be a string"))
        value = String(item)
        isempty(value) && throw(ArgumentError(
            "$name[$index] must not be empty"))
        occursin('\0', value) && throw(ArgumentError(
            "$name[$index] must not contain a NUL byte"))
        strip(value) == value || throw(ArgumentError(
            "$name[$index] must not have leading or trailing whitespace"))
        push!(values, value)
    end
    if unique_values && length(Set(values)) != length(values)
        throw(ArgumentError("$name must contain unique ordered values"))
    end
    return values
end

function _roo_metadata_bytes(named_vectors...)
    requested = BigInt(0)
    for (name, raw) in named_vectors
        for (index, item) in pairs(raw)
            item isa AbstractString || throw(ArgumentError(
                "$name[$index] must be a string"))
            requested += BigInt(ncodeunits(item))
        end
    end
    return requested
end

function _roo_finite_vector(raw::AbstractVector, name::AbstractString)
    all(value -> value isa Real && !(value isa Bool), raw) ||
        throw(ArgumentError(
            "$name must contain only real, non-Boolean values"))
    values = Float64.(raw)
    all(isfinite, values) || throw(ArgumentError(
        "$name must contain only finite values"))
    return Vector{Float64}(values)
end

function _roo_finite_array(raw::AbstractArray, name::AbstractString)
    all(value -> value isa Real && !(value isa Bool), raw) ||
        throw(ArgumentError(
            "$name must contain only real, non-Boolean values"))
    values = Float64.(raw)
    all(isfinite, values) || throw(ArgumentError(
        "$name must contain only finite values"))
    return values
end

function _roo_write_string(io::IO, value::AbstractString)
    bytes = codeunits(value)
    write(io, htol(UInt64(length(bytes))))
    write(io, bytes)
    return nothing
end

function _roo_write_strings(io::IO, values::Vector{String})
    write(io, htol(UInt64(length(values))))
    for value in values
        _roo_write_string(io, value)
    end
    return nothing
end

function _roo_write_float_array(io::IO, values::AbstractArray{Float64})
    write(io, htol(UInt64(ndims(values))))
    for extent in size(values)
        write(io, htol(UInt64(extent)))
    end
    write(io, reinterpret(UInt8, vec(values)))
    return nothing
end

function _roo_content_sha256(
    source_component_order::Vector{String},
    output_component_order::Vector{String},
    source_units::Vector{String},
    output_units::Vector{String},
    source_reference::Vector{Float64},
    output_reference::Vector{Float64},
    domain_lower::Vector{Float64},
    domain_upper::Vector{Float64},
    linear_jacobian::Matrix{Float64},
    quadratic_hessians::Array{Float64,3},
    map_kind::Symbol,
    regularity::Symbol,
    limits::ROObservableChartLimits,
)
    io = IOBuffer()
    _roo_write_string(io, RO_OBSERVABLE_CHART_VERSION)
    _roo_write_string(io, RO_OBSERVABLE_CHART_SCOPE)
    _roo_write_strings(io, source_component_order)
    _roo_write_strings(io, output_component_order)
    _roo_write_strings(io, source_units)
    _roo_write_strings(io, output_units)
    _roo_write_float_array(io, source_reference)
    _roo_write_float_array(io, output_reference)
    _roo_write_float_array(io, domain_lower)
    _roo_write_float_array(io, domain_upper)
    _roo_write_float_array(io, linear_jacobian)
    _roo_write_float_array(io, quadratic_hessians)
    _roo_write_string(io, String(map_kind))
    _roo_write_string(io, String(regularity))
    for name in fieldnames(ROObservableChartLimits)
        write(io, htol(UInt64(getfield(limits, name))))
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

function _roo_validate_components(
    source_component_order::Vector{String},
    output_component_order::Vector{String},
    source_units::Vector{String},
    output_units::Vector{String},
    source_reference::Vector{Float64},
    output_reference::Vector{Float64},
    domain_lower::Vector{Float64},
    domain_upper::Vector{Float64},
    linear_jacobian::Matrix{Float64},
    quadratic_hessians::Array{Float64,3},
    map_kind::Symbol,
    regularity::Symbol,
    limits::ROObservableChartLimits,
)
    _roo_validate_limits(limits)
    source_count = length(source_component_order)
    output_count = length(output_component_order)
    source_count > 0 || throw(ArgumentError(
        "source_component_order must not be empty"))
    output_count > 0 || throw(ArgumentError(
        "output_component_order must not be empty"))
    source_count <= limits.max_source_components ||
        throw(ROObservableChartLimitExceeded(
            :source_components, BigInt(source_count),
            limits.max_source_components))
    output_count <= limits.max_output_components ||
        throw(ROObservableChartLimitExceeded(
            :output_components, BigInt(output_count),
            limits.max_output_components))
    coefficient_count = BigInt(output_count) * BigInt(source_count) +
        BigInt(output_count) * BigInt(source_count)^2
    _roo_limit(:coefficients, coefficient_count, limits.max_coefficients)
    length(source_units) == source_count || throw(DimensionMismatch(
        "source_units must follow source_component_order"))
    length(output_units) == output_count || throw(DimensionMismatch(
        "output_units must follow output_component_order"))
    length(source_reference) == source_count || throw(DimensionMismatch(
        "source_reference must follow source_component_order"))
    length(output_reference) == output_count || throw(DimensionMismatch(
        "output_reference must follow output_component_order"))
    length(domain_lower) == source_count || throw(DimensionMismatch(
        "domain_lower must follow source_component_order"))
    length(domain_upper) == source_count || throw(DimensionMismatch(
        "domain_upper must follow source_component_order"))
    size(linear_jacobian) == (output_count, source_count) ||
        throw(DimensionMismatch(
            "linear_jacobian must have shape output_count x source_count"))
    size(quadratic_hessians) ==
        (output_count, source_count, source_count) ||
        throw(DimensionMismatch(
            "quadratic_hessians must have shape " *
            "output_count x source_count x source_count"))
    metadata_bytes = _roo_metadata_bytes(
        ("source_component_order", source_component_order),
        ("output_component_order", output_component_order),
        ("source_units", source_units),
        ("output_units", output_units),
    )
    _roo_limit(:metadata_bytes, metadata_bytes, limits.max_metadata_bytes)
    length(Set(source_component_order)) == source_count || throw(ArgumentError(
        "source_component_order must contain unique ordered values"))
    length(Set(output_component_order)) == output_count || throw(ArgumentError(
        "output_component_order must contain unique ordered values"))
    for (name, values) in (
        ("source_component_order", source_component_order),
        ("output_component_order", output_component_order),
        ("source_units", source_units),
        ("output_units", output_units),
    )
        all(value -> !isempty(value) && !occursin('\0', value) &&
            strip(value) == value, values) || throw(ArgumentError(
                "$name contains an invalid string"))
    end
    all(isfinite, source_reference) && all(isfinite, output_reference) &&
        all(isfinite, domain_lower) && all(isfinite, domain_upper) &&
        all(isfinite, linear_jacobian) && all(isfinite, quadratic_hessians) ||
        throw(ArgumentError(
            "observable-chart numeric content must be finite"))
    for source in 1:source_count
        domain_lower[source] < domain_upper[source] || throw(ArgumentError(
            "domain_lower[$source] must be strictly below domain_upper[$source]"))
        domain_lower[source] <= source_reference[source] <=
            domain_upper[source] || throw(ArgumentError(
                "source_reference[$source] must lie in the declared domain"))
    end
    for output in 1:output_count, left in 1:source_count,
        right in 1:source_count
        quadratic_hessians[output, left, right] ==
            quadratic_hessians[output, right, left] || throw(ArgumentError(
                "quadratic_hessians[$output, :, :] must be symmetric"))
    end
    map_kind in (:affine, :quadratic) || throw(ArgumentError(
        "map_kind must be :affine or :quadratic"))
    has_quadratic_term = any(value -> !iszero(value), quadratic_hessians)
    if map_kind === :affine
        !has_quadratic_term || throw(ArgumentError(
            "an affine observable map must have zero quadratic Hessians"))
    else
        has_quadratic_term || throw(ArgumentError(
            "a quadratic observable map must declare a nonzero Hessian term"))
    end
    regularity === :C2 || throw(ArgumentError(
        "finite affine/quadratic observable maps must declare regularity=:C2"))
    return nothing
end

"""
    RODeclarativeObservableChart

A finite second-order map from ordered source observables `z` to ordered
derived observables `y`. With `delta = z - source_reference`, the only admitted
map is

```text
y[p] = output_reference[p]
     + sum(a, L[p,a] * delta[a])
     + 1/2 * sum(a,b, Q[p,a,b] * delta[a] * delta[b]).
```

`Q[p,:,:]` is required to be symmetric. The component order, units,
references, closed finite domain, C2 regularity, coefficients, and limits are
all content-bound by `observable_chart_identity`. This type deliberately does
not accept an arbitrary evaluation callback. It also does not require the map
to be invertible: a derived observable may reduce dimension or couple several
source observables. General ratios, log-sums, and other non-polynomial maps are
outside this affine/quadratic contract.

Unit strings are opaque identity labels. This layer neither performs
dimensional algebra nor converts numerical values between units; callers must
supply already compatible coordinates and coefficients. The versioned scope is
`finite_declarative_affine_or_quadratic_observable_map`.

Array-valued properties are detached snapshots. Lower-level mutation of the
admitted backing storage is detected before every public operation. The raw
validated constructor independently repeats all admission checks and verifies
the identity, so it cannot be used to bypass the public constructors.
"""
struct RODeclarativeObservableChart
    source_component_order::Vector{String}
    output_component_order::Vector{String}
    source_units::Vector{String}
    output_units::Vector{String}
    source_reference::Vector{Float64}
    output_reference::Vector{Float64}
    domain_lower::Vector{Float64}
    domain_upper::Vector{Float64}
    linear_jacobian::Matrix{Float64}
    quadratic_hessians::Array{Float64,3}
    map_kind::Symbol
    regularity::Symbol
    limits::ROObservableChartLimits
    observable_chart_identity::String

    function RODeclarativeObservableChart(
        source_component_order::Vector{String},
        output_component_order::Vector{String},
        source_units::Vector{String},
        output_units::Vector{String},
        source_reference::Vector{Float64},
        output_reference::Vector{Float64},
        domain_lower::Vector{Float64},
        domain_upper::Vector{Float64},
        linear_jacobian::Matrix{Float64},
        quadratic_hessians::Array{Float64,3},
        map_kind::Symbol,
        regularity::Symbol,
        limits::ROObservableChartLimits,
        observable_chart_identity::String,
        ::Val{:validated},
    )
        _roo_validate_components(
            source_component_order,
            output_component_order,
            source_units,
            output_units,
            source_reference,
            output_reference,
            domain_lower,
            domain_upper,
            linear_jacobian,
            quadratic_hessians,
            map_kind,
            regularity,
            limits,
        )
        expected = _roo_content_sha256(
            source_component_order,
            output_component_order,
            source_units,
            output_units,
            source_reference,
            output_reference,
            domain_lower,
            domain_upper,
            linear_jacobian,
            quadratic_hessians,
            map_kind,
            regularity,
            limits,
        )
        observable_chart_identity == expected || throw(ArgumentError(
            "observable_chart_identity does not match admitted content"))
        return new(
            copy(source_component_order),
            copy(output_component_order),
            copy(source_units),
            copy(output_units),
            copy(source_reference),
            copy(output_reference),
            copy(domain_lower),
            copy(domain_upper),
            copy(linear_jacobian),
            copy(quadratic_hessians),
            map_kind,
            regularity,
            limits,
            observable_chart_identity,
        )
    end
end

function _roo_assert_unchanged(chart::RODeclarativeObservableChart)
    actual = _roo_content_sha256(
        getfield(chart, :source_component_order),
        getfield(chart, :output_component_order),
        getfield(chart, :source_units),
        getfield(chart, :output_units),
        getfield(chart, :source_reference),
        getfield(chart, :output_reference),
        getfield(chart, :domain_lower),
        getfield(chart, :domain_upper),
        getfield(chart, :linear_jacobian),
        getfield(chart, :quadratic_hessians),
        getfield(chart, :map_kind),
        getfield(chart, :regularity),
        getfield(chart, :limits),
    )
    actual == getfield(chart, :observable_chart_identity) ||
        throw(ArgumentError(
            "RODeclarativeObservableChart backing storage changed after admission"))
    return nothing
end

function Base.getproperty(chart::RODeclarativeObservableChart, name::Symbol)
    _roo_assert_unchanged(chart)
    value = getfield(chart, name)
    if name in (
        :source_component_order,
        :output_component_order,
        :source_units,
        :output_units,
        :source_reference,
        :output_reference,
        :domain_lower,
        :domain_upper,
        :linear_jacobian,
        :quadratic_hessians,
    )
        return copy(value)
    end
    return value
end

function _roo_construct(
    map_kind::Symbol,
    source_component_order::AbstractVector,
    output_component_order::AbstractVector,
    linear_jacobian::AbstractMatrix,
    quadratic_hessians::AbstractArray;
    source_units::AbstractVector,
    output_units::AbstractVector,
    source_reference::AbstractVector,
    output_reference::AbstractVector,
    domain_lower::AbstractVector,
    domain_upper::AbstractVector,
    regularity::Symbol=:C2,
    limits::ROObservableChartLimits=ROObservableChartLimits(),
    cancel_check=() -> nothing,
)
    cancel_check()
    _roo_validate_limits(limits)
    source_count = length(source_component_order)
    output_count = length(output_component_order)
    source_count <= limits.max_source_components ||
        throw(ROObservableChartLimitExceeded(
            :source_components, BigInt(source_count),
            limits.max_source_components))
    output_count <= limits.max_output_components ||
        throw(ROObservableChartLimitExceeded(
            :output_components, BigInt(output_count),
            limits.max_output_components))
    coefficient_count = BigInt(output_count) * BigInt(source_count) +
        BigInt(output_count) * BigInt(source_count)^2
    _roo_limit(:coefficients, coefficient_count, limits.max_coefficients)
    size(linear_jacobian) == (output_count, source_count) ||
        throw(DimensionMismatch(
            "linear_jacobian must have shape output_count x source_count"))
    size(quadratic_hessians) ==
        (output_count, source_count, source_count) ||
        throw(DimensionMismatch(
            "quadratic_hessians must have shape " *
            "output_count x source_count x source_count"))
    length(source_units) == source_count || throw(DimensionMismatch(
        "source_units must follow source_component_order"))
    length(output_units) == output_count || throw(DimensionMismatch(
        "output_units must follow output_component_order"))
    length(source_reference) == source_count || throw(DimensionMismatch(
        "source_reference must follow source_component_order"))
    length(output_reference) == output_count || throw(DimensionMismatch(
        "output_reference must follow output_component_order"))
    length(domain_lower) == source_count || throw(DimensionMismatch(
        "domain_lower must follow source_component_order"))
    length(domain_upper) == source_count || throw(DimensionMismatch(
        "domain_upper must follow source_component_order"))
    metadata_bytes = _roo_metadata_bytes(
        ("source_component_order", source_component_order),
        ("output_component_order", output_component_order),
        ("source_units", source_units),
        ("output_units", output_units),
    )
    _roo_limit(:metadata_bytes, metadata_bytes, limits.max_metadata_bytes)
    cancel_check()

    sources = _roo_string_vector(source_component_order,
        "source_component_order"; unique_values=true)
    outputs = _roo_string_vector(output_component_order,
        "output_component_order"; unique_values=true)
    source_unit_values = _roo_string_vector(source_units,
        "source_units"; unique_values=false)
    output_unit_values = _roo_string_vector(output_units,
        "output_units"; unique_values=false)
    source_ref = _roo_finite_vector(source_reference, "source_reference")
    output_ref = _roo_finite_vector(output_reference, "output_reference")
    lower = _roo_finite_vector(domain_lower, "domain_lower")
    upper = _roo_finite_vector(domain_upper, "domain_upper")
    linear = Matrix(_roo_finite_array(linear_jacobian, "linear_jacobian"))
    quadratic = Array{Float64,3}(_roo_finite_array(
        quadratic_hessians, "quadratic_hessians"))
    cancel_check()

    _roo_validate_components(
        sources,
        outputs,
        source_unit_values,
        output_unit_values,
        source_ref,
        output_ref,
        lower,
        upper,
        linear,
        quadratic,
        map_kind,
        regularity,
        limits,
    )
    identity = _roo_content_sha256(
        sources,
        outputs,
        source_unit_values,
        output_unit_values,
        source_ref,
        output_ref,
        lower,
        upper,
        linear,
        quadratic,
        map_kind,
        regularity,
        limits,
    )
    cancel_check()
    return RODeclarativeObservableChart(
        sources,
        outputs,
        source_unit_values,
        output_unit_values,
        source_ref,
        output_ref,
        lower,
        upper,
        linear,
        quadratic,
        map_kind,
        regularity,
        limits,
        identity,
        Val(:validated),
    )
end

"""Construct a finite C2 affine observable map with an identically zero Hessian."""
function ROAffineObservableChart(
    source_component_order::AbstractVector,
    output_component_order::AbstractVector,
    linear_jacobian::AbstractMatrix;
    source_units::AbstractVector,
    output_units::AbstractVector,
    source_reference::AbstractVector,
    output_reference::AbstractVector,
    domain_lower::AbstractVector,
    domain_upper::AbstractVector,
    regularity::Symbol=:C2,
    limits::ROObservableChartLimits=ROObservableChartLimits(),
    cancel_check=() -> nothing,
)
    cancel_check()
    _roo_validate_limits(limits)
    source_count = length(source_component_order)
    output_count = length(output_component_order)
    source_count <= limits.max_source_components ||
        throw(ROObservableChartLimitExceeded(
            :source_components, BigInt(source_count),
            limits.max_source_components))
    output_count <= limits.max_output_components ||
        throw(ROObservableChartLimitExceeded(
            :output_components, BigInt(output_count),
            limits.max_output_components))
    coefficient_count = BigInt(output_count) * BigInt(source_count) +
        BigInt(output_count) * BigInt(source_count)^2
    _roo_limit(:coefficients, coefficient_count, limits.max_coefficients)
    size(linear_jacobian) == (output_count, source_count) ||
        throw(DimensionMismatch(
            "linear_jacobian must have shape output_count x source_count"))
    quadratic = zeros(Float64, output_count, source_count, source_count)
    return _roo_construct(
        :affine,
        source_component_order,
        output_component_order,
        linear_jacobian,
        quadratic;
        source_units=source_units,
        output_units=output_units,
        source_reference=source_reference,
        output_reference=output_reference,
        domain_lower=domain_lower,
        domain_upper=domain_upper,
        regularity=regularity,
        limits=limits,
        cancel_check=cancel_check,
    )
end

"""Construct a finite C2 quadratic observable map with symmetric Hessians."""
function ROQuadraticObservableChart(
    source_component_order::AbstractVector,
    output_component_order::AbstractVector,
    linear_jacobian::AbstractMatrix,
    quadratic_hessians::AbstractArray;
    source_units::AbstractVector,
    output_units::AbstractVector,
    source_reference::AbstractVector,
    output_reference::AbstractVector,
    domain_lower::AbstractVector,
    domain_upper::AbstractVector,
    regularity::Symbol=:C2,
    limits::ROObservableChartLimits=ROObservableChartLimits(),
    cancel_check=() -> nothing,
)
    return _roo_construct(
        :quadratic,
        source_component_order,
        output_component_order,
        linear_jacobian,
        quadratic_hessians;
        source_units=source_units,
        output_units=output_units,
        source_reference=source_reference,
        output_reference=output_reference,
        domain_lower=domain_lower,
        domain_upper=domain_upper,
        regularity=regularity,
        limits=limits,
        cancel_check=cancel_check,
    )
end

"""Number of ordered source-observable components consumed by `chart`."""
function ro_source_observable_count(chart::RODeclarativeObservableChart)
    _roo_assert_unchanged(chart)
    return length(getfield(chart, :source_component_order))
end

"""Number of ordered derived-observable components produced by `chart`."""
function ro_derived_observable_count(chart::RODeclarativeObservableChart)
    _roo_assert_unchanged(chart)
    return length(getfield(chart, :output_component_order))
end

function _roo_source_delta(
    chart::RODeclarativeObservableChart,
    source_values::AbstractVector,
)
    _roo_assert_unchanged(chart)
    source_count = length(getfield(chart, :source_component_order))
    length(source_values) == source_count || throw(DimensionMismatch(
        "source_values must follow source_component_order"))
    values = _roo_finite_vector(source_values, "source_values")
    lower = getfield(chart, :domain_lower)
    upper = getfield(chart, :domain_upper)
    for source in 1:source_count
        lower[source] <= values[source] <= upper[source] ||
            throw(DomainError(values[source],
                "source_values[$source] lies outside the closed observable domain"))
    end
    delta = values - getfield(chart, :source_reference)
    all(isfinite, delta) || throw(OverflowError(
        "source-reference subtraction produced a non-finite value"))
    return values, delta
end

function _roo_map_work(
    map_kind::Symbol,
    output_count::Int,
    source_count::Int,
)
    linear_terms = BigInt(output_count) * BigInt(source_count)
    map_kind === :affine && return linear_terms
    # Each declared quadratic coefficient contributes once to the value and
    # once to the pointwise observable Jacobian.
    return linear_terms +
        2 * BigInt(output_count) * BigInt(source_count)^2
end

function _roo_value_jacobian(
    chart::RODeclarativeObservableChart,
    source_values::AbstractVector;
    cancel_check=() -> nothing,
)
    cancel_check()
    values, delta = _roo_source_delta(chart, source_values)
    source_count = length(getfield(chart, :source_component_order))
    output_count = length(getfield(chart, :output_component_order))
    limits = getfield(chart, :limits)
    evaluation_work = _roo_map_work(
        getfield(chart, :map_kind), output_count, source_count)
    _roo_limit(:map_evaluation, evaluation_work,
        limits.max_chain_rule_terms)

    output_values = copy(getfield(chart, :output_reference))
    jacobian = copy(getfield(chart, :linear_jacobian))
    linear = getfield(chart, :linear_jacobian)
    quadratic = getfield(chart, :quadratic_hessians)
    for output in 1:output_count
        cancel_check()
        value = output_values[output]
        for source in 1:source_count
            value += linear[output, source] * delta[source]
            if getfield(chart, :map_kind) === :quadratic
                derivative = jacobian[output, source]
                for right in 1:source_count
                    derivative += quadratic[output, source, right] *
                        delta[right]
                    value += 0.5 * quadratic[output, source, right] *
                        delta[source] * delta[right]
                end
                jacobian[output, source] = derivative
            end
        end
        output_values[output] = value
    end
    all(isfinite, output_values) && all(isfinite, jacobian) ||
        throw(OverflowError(
            "observable-chart evaluation produced a non-finite value"))
    cancel_check()
    return values, output_values, jacobian
end

"""
    evaluate_ro_observables(chart, source_values; cancel_check=...)

Evaluate the admitted finite affine/quadratic map at one ordered source value.
The returned metadata binds both component orders, units, and the exact chart
identity; the closed declared source domain is enforced before evaluation.
"""
function evaluate_ro_observables(
    chart::RODeclarativeObservableChart,
    source_values::AbstractVector;
    cancel_check=() -> nothing,
)
    values, output_values, jacobian = _roo_value_jacobian(
        chart, source_values; cancel_check=cancel_check)
    return (
        observable_chart_version=RO_OBSERVABLE_CHART_VERSION,
        observable_chart_scope=RO_OBSERVABLE_CHART_SCOPE,
        observable_chart_identity=getfield(chart, :observable_chart_identity),
        source_component_order=copy(getfield(chart, :source_component_order)),
        output_component_order=copy(getfield(chart, :output_component_order)),
        source_units=copy(getfield(chart, :source_units)),
        output_units=copy(getfield(chart, :output_units)),
        source_values=values,
        output_values=output_values,
        observable_jacobian=jacobian,
        observable_hessians=copy(getfield(chart, :quadratic_hessians)),
        regularity=getfield(chart, :regularity),
        map_kind=getfield(chart, :map_kind),
    )
end

"""
    compose_ro_observable_jet(chart, source_values, source_reaction_orders,
        source_input_hessians; source_component_order, source_units,
        control_component_order, control_units, cancel_check=...)

Compose one complete second-order source jet with `y = phi(z)`. If
`Rz[a,i] = partial z[a] / partial u[i]` and
`Hz[a,i,j] = partial^2 z[a] / partial u[i] partial u[j]`, this computes

```text
Ry[p,i] = sum(a, Jphi[p,a] * Rz[a,i])

Hy[p,i,j] = sum(a, Jphi[p,a] * Hz[a,i,j])
            + sum(a,b, Hphi[p,a,b] * Rz[a,i] * Rz[b,j]).
```

Neither source nor result Hessians are silently symmetrized. The caller must
bind the source jet to the chart by supplying an exact source-component order
and unit match; a row-permuted anonymous array is rejected. Component order,
units, source values, and chart identity accompany the returned arrays.
"""
function compose_ro_observable_jet(
    chart::RODeclarativeObservableChart,
    source_values::AbstractVector,
    source_reaction_orders::AbstractMatrix,
    source_input_hessians::AbstractArray;
    source_component_order::AbstractVector,
    source_units::AbstractVector,
    control_component_order::AbstractVector,
    control_units::AbstractVector,
    cancel_check=() -> nothing,
)
    cancel_check()
    _roo_assert_unchanged(chart)
    source_count = length(getfield(chart, :source_component_order))
    output_count = length(getfield(chart, :output_component_order))
    control_count = length(control_component_order)
    limits = getfield(chart, :limits)
    control_count > 0 || throw(ArgumentError(
        "control_component_order must not be empty"))
    control_count <= limits.max_control_components ||
        throw(ROObservableChartLimitExceeded(
            :control_components, BigInt(control_count),
            limits.max_control_components))
    size(source_reaction_orders) == (source_count, control_count) ||
        throw(DimensionMismatch(
            "source_reaction_orders must have shape source_count x control_count"))
    size(source_input_hessians) ==
        (source_count, control_count, control_count) ||
        throw(DimensionMismatch(
            "source_input_hessians must have shape " *
            "source_count x control_count x control_count"))
    length(source_component_order) == source_count ||
        throw(DimensionMismatch(
            "source_component_order must match the source-jet row count"))
    length(source_units) == source_count || throw(DimensionMismatch(
        "source_units must match the source-jet row count"))
    length(control_units) == control_count || throw(DimensionMismatch(
        "control_units must follow control_component_order"))
    metadata_bytes = _roo_metadata_bytes(
        ("source_component_order", source_component_order),
        ("source_units", source_units),
        ("control_component_order", control_component_order),
        ("control_units", control_units),
    )
    _roo_limit(:jet_metadata_bytes, metadata_bytes,
        limits.max_metadata_bytes)
    chain_work =
        BigInt(output_count) * BigInt(source_count) * BigInt(control_count) +
        BigInt(output_count) * BigInt(source_count) * BigInt(control_count)^2 +
        BigInt(output_count) * BigInt(source_count)^2 * BigInt(control_count)^2
    work = _roo_map_work(
        getfield(chart, :map_kind), output_count, source_count) + chain_work
    _roo_limit(:second_order_composition, work,
        limits.max_chain_rule_terms)
    cancel_check()

    declared_sources = _roo_string_vector(source_component_order,
        "source_component_order"; unique_values=true)
    declared_source_units = _roo_string_vector(source_units,
        "source_units"; unique_values=false)
    declared_sources == getfield(chart, :source_component_order) ||
        throw(ArgumentError(
            "source_component_order does not exactly match the observable chart"))
    declared_source_units == getfield(chart, :source_units) ||
        throw(ArgumentError(
            "source_units do not exactly match the observable chart"))
    controls = _roo_string_vector(control_component_order,
        "control_component_order"; unique_values=true)
    units = _roo_string_vector(control_units,
        "control_units"; unique_values=false)
    source_ro = Matrix(_roo_finite_array(
        source_reaction_orders, "source_reaction_orders"))
    source_hessians = Array{Float64,3}(_roo_finite_array(
        source_input_hessians, "source_input_hessians"))
    values, output_values, observable_jacobian = _roo_value_jacobian(
        chart, source_values; cancel_check=cancel_check)
    observable_hessians = getfield(chart, :quadratic_hessians)
    cancel_check()

    output_ro = Matrix{Float64}(undef, output_count, control_count)
    output_hessians = Array{Float64,3}(
        undef, output_count, control_count, control_count)
    for output in 1:output_count
        cancel_check()
        for left_control in 1:control_count
            cancel_check()
            first_derivative = 0.0
            for source in 1:source_count
                first_derivative += observable_jacobian[output, source] *
                    source_ro[source, left_control]
            end
            output_ro[output, left_control] = first_derivative
            for right_control in 1:control_count
                # One checkpoint bounds the longest uninterrupted contraction
                # to O(source_count^2) scalar terms, even for one output.
                cancel_check()
                second_derivative = 0.0
                for source in 1:source_count
                    second_derivative += observable_jacobian[output, source] *
                        source_hessians[source, left_control, right_control]
                end
                for left_source in 1:source_count,
                    right_source in 1:source_count
                    second_derivative +=
                        observable_hessians[output, left_source, right_source] *
                        source_ro[left_source, left_control] *
                        source_ro[right_source, right_control]
                end
                output_hessians[output, left_control, right_control] =
                    second_derivative
            end
        end
    end
    all(isfinite, output_ro) && all(isfinite, output_hessians) ||
        throw(OverflowError(
            "observable-chain composition produced a non-finite derivative"))
    cancel_check()
    return (
        observable_chart_version=RO_OBSERVABLE_CHART_VERSION,
        observable_chart_scope=RO_OBSERVABLE_CHART_SCOPE,
        observable_chart_identity=getfield(chart, :observable_chart_identity),
        source_component_order=copy(getfield(chart, :source_component_order)),
        output_component_order=copy(getfield(chart, :output_component_order)),
        source_units=copy(getfield(chart, :source_units)),
        output_units=copy(getfield(chart, :output_units)),
        control_component_order=controls,
        control_units=units,
        source_values=values,
        output_values=output_values,
        observable_jacobian=observable_jacobian,
        observable_hessians=copy(observable_hessians),
        reaction_orders=output_ro,
        input_hessians=output_hessians,
        map_kind=getfield(chart, :map_kind),
        regularity=getfield(chart, :regularity),
        chain_rule_work_units=Int(work),
    )
end
