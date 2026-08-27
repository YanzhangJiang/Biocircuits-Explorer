const RO_POLYNOMIAL_EQUILIBRIUM_SYSTEM_VERSION =
    "bne-ro-polynomial-equilibrium-system/v1.0.0"
const RO_REGULAR_SHEET_PATCH_VERSION =
    "bne-ro-exact-regular-sheet-patch/v1.1.0"
const RO_REGULAR_SHEET_BRIDGE_VERSION =
    "bne-ro-exact-regular-sheet-bridge/v1.1.0"
const RO_REGULAR_SHEET_PATCH_SCOPE =
    :exact_dyadic_parametric_krawczyk_unique_root_inside_declared_tube
const RO_REGULAR_SHEET_BRIDGE_SCOPE =
    :exact_dyadic_overlap_bridge_between_two_validated_root_tubes

const _RORSExact = Rational{BigInt}

struct RORegularSheetLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::RORegularSheetLimitExceeded)
    print(io, "RO regular-sheet ", err.phase, " requested ", err.requested,
        ", exceeding limit=", err.limit)
end

struct RORegularSheetCertificationRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::RORegularSheetCertificationRejected)
    print(io, "RO regular-sheet certification rejected (", err.reason,
        "): ", err.detail)
end

"""Hard construction and exact-arithmetic limits for P5r0 artifacts."""
struct RORegularSheetLimits
    max_states::Int
    max_controls::Int
    max_terms_per_equation::Int
    max_total_terms::Int
    max_expanded_terms::Int
    max_total_degree::Int
    max_metadata_bytes::Int
    max_exact_operand_bits::Int
    max_interval_operations::Int

    function RORegularSheetLimits(
        max_states::Int,
        max_controls::Int,
        max_terms_per_equation::Int,
        max_total_terms::Int,
        max_expanded_terms::Int,
        max_total_degree::Int,
        max_metadata_bytes::Int,
        max_exact_operand_bits::Int,
        max_interval_operations::Int,
    )
        for (label, value) in (
            ("max_states", max_states),
            ("max_controls", max_controls),
            ("max_terms_per_equation", max_terms_per_equation),
            ("max_total_terms", max_total_terms),
            ("max_expanded_terms", max_expanded_terms),
            ("max_total_degree", max_total_degree),
            ("max_metadata_bytes", max_metadata_bytes),
            ("max_exact_operand_bits", max_exact_operand_bits),
            ("max_interval_operations", max_interval_operations),
        )
            value > 0 || throw(ArgumentError("$label must be positive"))
        end
        max_terms_per_equation <= max_total_terms || throw(ArgumentError(
            "max_terms_per_equation cannot exceed max_total_terms"))
        max_total_terms <= max_expanded_terms || throw(ArgumentError(
            "max_total_terms cannot exceed max_expanded_terms"))
        max_exact_operand_bits >= 1076 || throw(ArgumentError(
            "max_exact_operand_bits must admit every finite Float64 dyadic"))
        return new(
            max_states,
            max_controls,
            max_terms_per_equation,
            max_total_terms,
            max_expanded_terms,
            max_total_degree,
            max_metadata_bytes,
            max_exact_operand_bits,
            max_interval_operations,
        )
    end
end

function RORegularSheetLimits(;
    max_states::Integer=8,
    max_controls::Integer=8,
    max_terms_per_equation::Integer=128,
    max_total_terms::Integer=512,
    max_expanded_terms::Integer=8_192,
    max_total_degree::Integer=16,
    max_metadata_bytes::Integer=32_768,
    max_exact_operand_bits::Integer=65_536,
    max_interval_operations::Integer=2_000_000,
)
    values = (
        max_states,
        max_controls,
        max_terms_per_equation,
        max_total_terms,
        max_expanded_terms,
        max_total_degree,
        max_metadata_bytes,
        max_exact_operand_bits,
        max_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("regular-sheet limits must fit Int"))
    return RORegularSheetLimits(Int.(values)...)
end

@inline function _rors_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "RO regular-sheet requested work must be nonnegative"))
    amount <= limit || throw(RORegularSheetLimitExceeded(
        phase, amount, limit))
    return nothing
end

@inline function _rors_validate_sha256(value::AbstractString, label::String)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$label must be a lowercase SHA-256 hexadecimal string"))
    return nothing
end

function _rors_validate_metadata(value::String, label::String)
    isempty(value) && throw(ArgumentError("$label must not be empty"))
    value == strip(value) || throw(ArgumentError(
        "$label must not contain leading or trailing whitespace"))
    occursin('\0', value) && throw(ArgumentError(
        "$label must not contain NUL"))
    return nothing
end

@inline function _rors_bit_length(value::_RORSExact)
    numerator_bits = ndigits(abs(numerator(value)); base=2)
    denominator_bits = ndigits(denominator(value); base=2)
    return max(numerator_bits, denominator_bits)
end

@inline function _rors_is_dyadic(value::_RORSExact)
    denominator_value = denominator(value)
    return denominator_value > 0 && ispow2(denominator_value)
end

function _rors_check_exact(
    value::_RORSExact,
    limits::RORegularSheetLimits,
    phase::Symbol,
)
    bits = _rors_bit_length(value)
    _rors_limit(phase, bits, limits.max_exact_operand_bits)
    return value
end

function _rors_exact_float(
    value,
    limits::RORegularSheetLimits,
    label::String,
)
    value isa Float64 || throw(ArgumentError(
        "$label must contain Float64 values; implicit numeric coercion is disabled"))
    isfinite(value) || throw(ArgumentError("$label must be finite"))
    normalized = value == 0.0 ? 0.0 : value
    return _rors_check_exact(
        _RORSExact(normalized), limits, :exact_operand_bits)
end

function _rors_write_token(io::IO, value)
    text = string(value)
    print(io, ncodeunits(text), ':', text, ';')
    return nothing
end

function _rors_write_exact(io::IO, value::_RORSExact)
    _rors_write_token(io, numerator(value))
    _rors_write_token(io, denominator(value))
    return nothing
end

function _rors_write_limits(io::IO, limits::RORegularSheetLimits)
    for value in (
        limits.max_states,
        limits.max_controls,
        limits.max_terms_per_equation,
        limits.max_total_terms,
        limits.max_expanded_terms,
        limits.max_total_degree,
        limits.max_metadata_bytes,
        limits.max_exact_operand_bits,
        limits.max_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

"""
One nonzero Float64-coefficient monomial. The coefficient is interpreted as its
exact binary rational. Exponents are nonnegative and are dimension-checked by
the owning equilibrium system.
"""
struct ROPolynomialTerm
    coefficient::Float64
    state_exponents::Tuple
    control_exponents::Tuple

    function ROPolynomialTerm(
        coefficient::Float64,
        state_exponents::Tuple,
        control_exponents::Tuple,
        ::Val{:validated},
    )
        isfinite(coefficient) || throw(ArgumentError(
            "polynomial coefficient must be finite"))
        coefficient != 0.0 || throw(ArgumentError(
            "zero polynomial terms are not canonical"))
        all(exponent -> exponent isa Int && exponent >= 0,
            state_exponents) || throw(ArgumentError(
                "state exponents must be nonnegative Int values"))
        all(exponent -> exponent isa Int && exponent >= 0,
            control_exponents) || throw(ArgumentError(
                "control exponents must be nonnegative Int values"))
        return new(
            coefficient == 0.0 ? 0.0 : coefficient,
            state_exponents,
            control_exponents,
        )
    end
end

function _rors_exponent_tuple(raw, label::String)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "$label must be an ordered vector or tuple"))
    values = Int[]
    sizehint!(values, length(raw))
    for (position, value) in enumerate(raw)
        value isa Integer || throw(ArgumentError(
            "$label[$position] must be an integer"))
        value >= 0 || throw(ArgumentError(
            "$label[$position] must be nonnegative"))
        value <= typemax(Int) || throw(ArgumentError(
            "$label[$position] exceeds Int"))
        push!(values, Int(value))
    end
    return Tuple(values)
end

function ROPolynomialTerm(
    coefficient::Float64,
    state_exponents,
    control_exponents,
)
    return ROPolynomialTerm(
        coefficient,
        _rors_exponent_tuple(state_exponents, "state_exponents"),
        _rors_exponent_tuple(control_exponents, "control_exponents"),
        Val(:validated),
    )
end

function Base.:(==)(left::ROPolynomialTerm, right::ROPolynomialTerm)
    return isequal(left.coefficient, right.coefficient) &&
        left.state_exponents == right.state_exponents &&
        left.control_exponents == right.control_exponents
end

function _rors_string_tuple(
    raw,
    label::String,
    byte_count::Base.RefValue{BigInt},
    limits::RORegularSheetLimits;
    unique_values::Bool,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "$label must be an ordered vector or tuple"))
    values = String[]
    sizehint!(values, length(raw))
    for (position, value) in enumerate(raw)
        value isa AbstractString || throw(ArgumentError(
            "$label[$position] must be a string"))
        byte_count[] += ncodeunits(value)
        _rors_limit(:metadata_bytes, byte_count[], limits.max_metadata_bytes)
        admitted = String(value)
        _rors_validate_metadata(admitted, "$label[$position]")
        push!(values, admitted)
    end
    unique_values && !allunique(values) && throw(ArgumentError(
        "$label values must be unique"))
    return Tuple(values)
end

function _rors_system_sha256(
    state_names::Tuple,
    state_units::Tuple,
    control_names::Tuple,
    control_units::Tuple,
    equations::Tuple,
    limits::RORegularSheetLimits,
)
    io = IOBuffer()
    _rors_write_token(io, RO_POLYNOMIAL_EQUILIBRIUM_SYSTEM_VERSION)
    for values in (state_names, state_units, control_names, control_units)
        _rors_write_token(io, length(values))
        for value in values
            _rors_write_token(io, value)
        end
    end
    _rors_write_token(io, length(equations))
    for equation in equations
        _rors_write_token(io, length(equation))
        for term in equation
            _rors_write_token(io, bitstring(term.coefficient))
            _rors_write_token(io, length(term.state_exponents))
            for exponent in term.state_exponents
                _rors_write_token(io, exponent)
            end
            _rors_write_token(io, length(term.control_exponents))
            for exponent in term.control_exponents
                _rors_write_token(io, exponent)
            end
        end
    end
    _rors_write_limits(io, limits)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rors_validate_system_fields(
    state_names::Tuple,
    state_units::Tuple,
    control_names::Tuple,
    control_units::Tuple,
    equations::Tuple,
    limits::RORegularSheetLimits,
)
    state_count = length(state_names)
    control_count = length(control_names)
    state_count > 0 || throw(ArgumentError(
        "polynomial equilibrium system requires at least one state"))
    control_count > 0 || throw(ArgumentError(
        "polynomial equilibrium system requires at least one control"))
    _rors_limit(:states, state_count, limits.max_states)
    _rors_limit(:controls, control_count, limits.max_controls)
    length(state_units) == state_count || throw(DimensionMismatch(
        "state_units must follow state_names"))
    length(control_units) == control_count || throw(DimensionMismatch(
        "control_units must follow control_names"))
    length(equations) == state_count || throw(DimensionMismatch(
        "a regular-sheet equilibrium system must be square"))
    allunique(state_names) || throw(ArgumentError(
        "state_names must be unique"))
    allunique(control_names) || throw(ArgumentError(
        "control_names must be unique"))
    for (label, values) in (
        ("state_names", state_names),
        ("state_units", state_units),
        ("control_names", control_names),
        ("control_units", control_units),
    ), (position, value) in enumerate(values)
        value isa String || throw(ArgumentError("$label must contain Strings"))
        _rors_validate_metadata(value, "$label[$position]")
    end
    total_terms = BigInt(0)
    for (equation_index, equation) in enumerate(equations)
        equation isa Tuple || throw(ArgumentError(
            "equation $equation_index must be an immutable admitted tuple"))
        isempty(equation) && throw(ArgumentError(
            "equation $equation_index must contain at least one term"))
        _rors_limit(:terms_per_equation, length(equation),
            limits.max_terms_per_equation)
        total_terms += length(equation)
        _rors_limit(:total_terms, total_terms, limits.max_total_terms)
        previous_key = nothing
        for (term_index, term) in enumerate(equation)
            term isa ROPolynomialTerm || throw(ArgumentError(
                "equation $equation_index term $term_index has the wrong type"))
            length(term.state_exponents) == state_count ||
                throw(DimensionMismatch(
                    "equation $equation_index term $term_index state exponents do not match state_count"))
            length(term.control_exponents) == control_count ||
                throw(DimensionMismatch(
                    "equation $equation_index term $term_index control exponents do not match control_count"))
            degree = sum(BigInt, term.state_exponents) +
                sum(BigInt, term.control_exponents)
            _rors_limit(:total_degree, degree, limits.max_total_degree)
            _rors_exact_float(term.coefficient, limits,
                "equation $equation_index term $term_index coefficient")
            key = (term.state_exponents, term.control_exponents)
            previous_key !== nothing && key == previous_key &&
                throw(ArgumentError(
                    "equation $equation_index contains duplicate monomials"))
            previous_key !== nothing && isless(key, previous_key) &&
                throw(ArgumentError(
                    "equation $equation_index terms are not canonical"))
            previous_key = key
        end
    end
    return nothing
end

"""
A square declarative polynomial equilibrium system `F(x,u)=0`. It has no
callback and owns the exact component order, unit labels, monomial population,
limits, and declaration hash.
"""
struct ROPolynomialEquilibriumSystem
    state_names::Tuple
    state_units::Tuple
    control_names::Tuple
    control_units::Tuple
    equations::Tuple
    limits::RORegularSheetLimits
    declaration_sha256::String

    function ROPolynomialEquilibriumSystem(
        state_names::Tuple,
        state_units::Tuple,
        control_names::Tuple,
        control_units::Tuple,
        equations::Tuple,
        limits::RORegularSheetLimits,
        declaration_sha256::String,
        ::Val{:validated},
    )
        _rors_validate_system_fields(
            state_names,
            state_units,
            control_names,
            control_units,
            equations,
            limits,
        )
        expected = _rors_system_sha256(
            state_names,
            state_units,
            control_names,
            control_units,
            equations,
            limits,
        )
        declaration_sha256 == expected || throw(ArgumentError(
            "polynomial equilibrium-system declaration hash mismatch"))
        return new(
            state_names,
            state_units,
            control_names,
            control_units,
            equations,
            limits,
            declaration_sha256,
        )
    end
end


function ROPolynomialEquilibriumSystem(;
    state_names,
    state_units,
    control_names,
    control_units,
    equations,
    limits::RORegularSheetLimits=RORegularSheetLimits(),
)
    byte_count = Ref(BigInt(0))
    admitted_state_names = _rors_string_tuple(
        state_names, "state_names", byte_count, limits; unique_values=true)
    admitted_state_units = _rors_string_tuple(
        state_units, "state_units", byte_count, limits; unique_values=false)
    admitted_control_names = _rors_string_tuple(
        control_names, "control_names", byte_count, limits; unique_values=true)
    admitted_control_units = _rors_string_tuple(
        control_units, "control_units", byte_count, limits; unique_values=false)
    equations isa AbstractVector || equations isa Tuple || throw(ArgumentError(
        "equations must be an ordered vector or tuple"))
    admitted_equations = Any[]
    sizehint!(admitted_equations, length(equations))
    total_terms = BigInt(0)
    for (equation_index, raw_equation) in enumerate(equations)
        raw_equation isa AbstractVector || raw_equation isa Tuple ||
            throw(ArgumentError(
                "equation $equation_index must be an ordered vector or tuple"))
        _rors_limit(:terms_per_equation, length(raw_equation),
            limits.max_terms_per_equation)
        total_terms += length(raw_equation)
        _rors_limit(:total_terms, total_terms, limits.max_total_terms)
        terms = ROPolynomialTerm[]
        sizehint!(terms, length(raw_equation))
        for (term_index, term) in enumerate(raw_equation)
            term isa ROPolynomialTerm || throw(ArgumentError(
                "equation $equation_index term $term_index must be ROPolynomialTerm"))
            push!(terms, term)
        end
        sort!(terms; by=term ->
            (term.state_exponents, term.control_exponents))
        push!(admitted_equations, Tuple(terms))
    end
    equation_tuple = Tuple(admitted_equations)
    _rors_validate_system_fields(
        admitted_state_names,
        admitted_state_units,
        admitted_control_names,
        admitted_control_units,
        equation_tuple,
        limits,
    )
    declaration_sha256 = _rors_system_sha256(
        admitted_state_names,
        admitted_state_units,
        admitted_control_names,
        admitted_control_units,
        equation_tuple,
        limits,
    )
    return ROPolynomialEquilibriumSystem(
        admitted_state_names,
        admitted_state_units,
        admitted_control_names,
        admitted_control_units,
        equation_tuple,
        limits,
        declaration_sha256,
        Val(:validated),
    )
end

function validate_ro_polynomial_equilibrium_system(
    system::ROPolynomialEquilibriumSystem,
)
    _rors_validate_system_fields(
        system.state_names,
        system.state_units,
        system.control_names,
        system.control_units,
        system.equations,
        system.limits,
    )
    expected = _rors_system_sha256(
        system.state_names,
        system.state_units,
        system.control_names,
        system.control_units,
        system.equations,
        system.limits,
    )
    expected == system.declaration_sha256 || throw(ArgumentError(
        "polynomial equilibrium-system declaration hash mismatch"))
    return true
end

"""An immutable closed exact rational interval."""
struct ROExactInterval
    lower::_RORSExact
    upper::_RORSExact

    function ROExactInterval(
        lower::_RORSExact,
        upper::_RORSExact,
        ::Val{:validated},
    )
        lower <= upper || throw(ArgumentError(
            "exact interval lower endpoint exceeds upper endpoint"))
        return new(lower, upper)
    end
end

function ROExactInterval(lower::Float64, upper::Float64)
    isfinite(lower) && isfinite(upper) || throw(ArgumentError(
        "exact interval Float64 endpoints must be finite"))
    return ROExactInterval(
        _RORSExact(lower == 0.0 ? 0.0 : lower),
        _RORSExact(upper == 0.0 ? 0.0 : upper),
        Val(:validated),
    )
end

Base.in(value::Real, interval::ROExactInterval) =
    interval.lower <= value <= interval.upper

Base.:(==)(left::ROExactInterval, right::ROExactInterval) =
    left.lower == right.lower && left.upper == right.upper
Base.isequal(left::ROExactInterval, right::ROExactInterval) =
    isequal(left.lower, right.lower) && isequal(left.upper, right.upper)

function Base.show(io::IO, interval::ROExactInterval)
    print(io, "ROExactInterval(", interval.lower, ", ", interval.upper, ")")
end

"""Immutable column-major exact rational matrix used in certificate payloads."""
struct ROExactMatrix
    row_count::Int
    column_count::Int
    data::Tuple

    function ROExactMatrix(
        row_count::Int,
        column_count::Int,
        data::Tuple,
        ::Val{:validated},
    )
        row_count > 0 && column_count > 0 || throw(ArgumentError(
            "exact matrix dimensions must be positive"))
        length(data) == row_count * column_count || throw(DimensionMismatch(
            "exact matrix data do not match its dimensions"))
        all(value -> value isa _RORSExact, data) || throw(ArgumentError(
            "exact matrix data must be Rational{BigInt}"))
        return new(row_count, column_count, data)
    end
end

Base.size(matrix::ROExactMatrix) =
    (matrix.row_count, matrix.column_count)
function Base.getindex(matrix::ROExactMatrix, row::Int, column::Int)
    1 <= row <= matrix.row_count &&
        1 <= column <= matrix.column_count ||
        throw(BoundsError(matrix, (row, column)))
    return matrix.data[row + (column - 1) * matrix.row_count]
end

function _rors_exact_matrix_wrapper(matrix::Matrix{_RORSExact})
    return ROExactMatrix(
        size(matrix, 1), size(matrix, 2), Tuple(vec(matrix)), Val(:validated))
end

"""Immutable column-major exact interval matrix used in certificate payloads."""
struct ROExactIntervalMatrix
    row_count::Int
    column_count::Int
    data::Tuple

    function ROExactIntervalMatrix(
        row_count::Int,
        column_count::Int,
        data::Tuple,
        ::Val{:validated},
    )
        row_count > 0 && column_count > 0 || throw(ArgumentError(
            "exact interval-matrix dimensions must be positive"))
        length(data) == row_count * column_count || throw(DimensionMismatch(
            "exact interval-matrix data do not match its dimensions"))
        all(value -> value isa ROExactInterval, data) || throw(ArgumentError(
            "exact interval-matrix data must be ROExactInterval"))
        return new(row_count, column_count, data)
    end
end

Base.size(matrix::ROExactIntervalMatrix) =
    (matrix.row_count, matrix.column_count)
function Base.getindex(matrix::ROExactIntervalMatrix, row::Int, column::Int)
    1 <= row <= matrix.row_count &&
        1 <= column <= matrix.column_count ||
        throw(BoundsError(matrix, (row, column)))
    return matrix.data[row + (column - 1) * matrix.row_count]
end

function _rors_interval_matrix_wrapper(matrix::Matrix{ROExactInterval})
    return ROExactIntervalMatrix(
        size(matrix, 1), size(matrix, 2), Tuple(vec(matrix)), Val(:validated))
end

mutable struct _RORSContext
    limits::RORegularSheetLimits
    operations::BigInt
    cancel_check
end

function _RORSContext(limits::RORegularSheetLimits, cancel_check)
    cancel_check()
    return _RORSContext(limits, BigInt(0), cancel_check)
end

@inline function _rors_tick!(context::_RORSContext, count::Integer=1)
    amount = BigInt(count)
    amount >= 0 || throw(ArgumentError(
        "exact interval operation count must be nonnegative"))
    context.operations += amount
    _rors_limit(:interval_operations, context.operations,
        context.limits.max_interval_operations)
    (context.operations == amount || context.operations % 256 == 0) &&
        context.cancel_check()
    return nothing
end

@inline function _rors_checked(context::_RORSContext, value::_RORSExact)
    return _rors_check_exact(
        value, context.limits, :exact_intermediate_operand_bits)
end

@inline function _rors_preflight_exact_bits(
    context::_RORSContext,
    requested::Integer,
)
    _rors_limit(:exact_intermediate_operand_bits, requested,
        context.limits.max_exact_operand_bits)
    return nothing
end

function _rors_exact_add(
    context::_RORSContext,
    left::_RORSExact,
    right::_RORSExact,
)
    _rors_tick!(context)
    _rors_preflight_exact_bits(
        context, BigInt(_rors_bit_length(left)) + _rors_bit_length(right) + 1)
    return _rors_checked(context, left + right)
end


function _rors_exact_subtract(
    context::_RORSContext,
    left::_RORSExact,
    right::_RORSExact,
)
    return _rors_exact_add(context, left, -right)
end

function _rors_exact_multiply(
    context::_RORSContext,
    left::_RORSExact,
    right::_RORSExact,
)
    _rors_tick!(context)
    (iszero(left) || iszero(right)) && return zero(_RORSExact)
    _rors_preflight_exact_bits(
        context, BigInt(_rors_bit_length(left)) + _rors_bit_length(right))
    return _rors_checked(context, left * right)
end


function _rors_exact_divide(
    context::_RORSContext,
    numerator_value::_RORSExact,
    denominator_value::_RORSExact,
)
    iszero(denominator_value) && throw(DivideError())
    _rors_tick!(context)
    iszero(numerator_value) && return zero(_RORSExact)
    _rors_preflight_exact_bits(
        context,
        BigInt(_rors_bit_length(numerator_value)) +
            _rors_bit_length(denominator_value),
    )
    return _rors_checked(context, numerator_value / denominator_value)
end

function _rors_exact_power(
    context::_RORSContext,
    value::_RORSExact,
    exponent::Int,
)
    exponent >= 0 || throw(ArgumentError("exact power must be nonnegative"))
    _rors_tick!(context, max(exponent, 1))
    (iszero(value) || exponent == 0) || _rors_preflight_exact_bits(
        context, BigInt(_rors_bit_length(value)) * exponent)
    return _rors_checked(context, value^exponent)
end

@inline function _rors_interval(
    context::_RORSContext,
    lower::_RORSExact,
    upper::_RORSExact,
)
    _rors_checked(context, lower)
    _rors_checked(context, upper)
    return ROExactInterval(lower, upper, Val(:validated))
end

@inline _rors_point(context::_RORSContext, value::_RORSExact) =
    _rors_interval(context, value, value)

function _rors_add(
    context::_RORSContext,
    left::ROExactInterval,
    right::ROExactInterval,
)
    return _rors_interval(
        context,
        _rors_exact_add(context, left.lower, right.lower),
        _rors_exact_add(context, left.upper, right.upper),
    )
end

function _rors_negate(context::_RORSContext, value::ROExactInterval)
    _rors_tick!(context)
    return _rors_interval(context, -value.upper, -value.lower)
end

function _rors_subtract(
    context::_RORSContext,
    left::ROExactInterval,
    right::ROExactInterval,
)
    return _rors_add(context, left, _rors_negate(context, right))
end

function _rors_multiply(
    context::_RORSContext,
    left::ROExactInterval,
    right::ROExactInterval,
)
    products = (
        _rors_exact_multiply(context, left.lower, right.lower),
        _rors_exact_multiply(context, left.lower, right.upper),
        _rors_exact_multiply(context, left.upper, right.lower),
        _rors_exact_multiply(context, left.upper, right.upper),
    )
    return _rors_interval(context, minimum(products), maximum(products))
end

function _rors_integer_power(
    context::_RORSContext,
    value::ROExactInterval,
    exponent::Int,
)
    exponent >= 0 || throw(ArgumentError(
        "interval exponent must be nonnegative"))
    exponent == 0 && return _rors_point(context, one(_RORSExact))
    if isodd(exponent)
        return _rors_interval(
            context,
            _rors_exact_power(context, value.lower, exponent),
            _rors_exact_power(context, value.upper, exponent),
        )
    end
    lower_power = _rors_exact_power(context, value.lower, exponent)
    upper_power = _rors_exact_power(context, value.upper, exponent)
    if value.upper < 0
        return _rors_interval(context, upper_power, lower_power)
    elseif value.lower > 0
        return _rors_interval(context, lower_power, upper_power)
    end
    return _rors_interval(
        context,
        zero(_RORSExact),
        max(lower_power, upper_power),
    )
end

@inline _rors_contains_zero(value::ROExactInterval) =
    value.lower <= 0 <= value.upper
@inline _rors_subset(left::ROExactInterval, right::ROExactInterval) =
    right.lower <= left.lower && left.upper <= right.upper
@inline _rors_strict_subset(left::ROExactInterval, right::ROExactInterval) =
    right.lower < left.lower && left.upper < right.upper
@inline _rors_abs_upper(value::ROExactInterval) =
    max(abs(value.lower), abs(value.upper))

function _rors_write_interval(io::IO, value::ROExactInterval)
    _rors_write_exact(io, value.lower)
    _rors_write_exact(io, value.upper)
    return nothing
end

function _rors_write_exact_vector(io::IO, values::Tuple)
    _rors_write_token(io, length(values))
    for value in values
        _rors_write_exact(io, value)
    end
    return nothing
end

function _rors_write_interval_vector(io::IO, values::Tuple)
    _rors_write_token(io, length(values))
    for value in values
        _rors_write_interval(io, value)
    end
    return nothing
end

function _rors_write_exact_matrix(io::IO, matrix::ROExactMatrix)
    _rors_write_token(io, matrix.row_count)
    _rors_write_token(io, matrix.column_count)
    for value in matrix.data
        _rors_write_exact(io, value)
    end
    return nothing
end

function _rors_write_interval_matrix(io::IO, matrix::ROExactIntervalMatrix)
    _rors_write_token(io, matrix.row_count)
    _rors_write_token(io, matrix.column_count)
    for value in matrix.data
        _rors_write_interval(io, value)
    end
    return nothing
end

function _rors_exact_vector(
    raw,
    expected_count::Int,
    label::String,
    context::_RORSContext,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "$label must be an ordered vector or tuple"))
    length(raw) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count values"))
    values = Vector{_RORSExact}(undef, expected_count)
    for index in 1:expected_count
        _rors_tick!(context)
        values[index] = _rors_exact_float(
            raw[index], context.limits, "$label[$index]")
    end
    return values
end

function _rors_exact_matrix(
    raw,
    row_count::Int,
    column_count::Int,
    label::String,
    context::_RORSContext,
)
    raw isa AbstractMatrix || throw(ArgumentError(
        "$label must be an AbstractMatrix"))
    size(raw) == (row_count, column_count) || throw(DimensionMismatch(
        "$label must have shape ($row_count, $column_count)"))
    values = Matrix{_RORSExact}(undef, row_count, column_count)
    for column in 1:column_count, row in 1:row_count
        _rors_tick!(context)
        values[row, column] = _rors_exact_float(
            raw[row, column], context.limits, "$label[$row,$column]")
    end
    return values
end

function _rors_exact_box(
    raw_lower,
    raw_upper,
    count::Int,
    label::String,
    context::_RORSContext,
)
    lower = _rors_exact_vector(
        raw_lower, count, "$(label)_lower", context)
    upper = _rors_exact_vector(
        raw_upper, count, "$(label)_upper", context)
    result = Vector{ROExactInterval}(undef, count)
    for index in 1:count
        lower[index] <= upper[index] || throw(ArgumentError(
            "$label interval $index has lower > upper"))
        result[index] = _rors_interval(context, lower[index], upper[index])
    end
    return result
end

function _rors_exact_rank(
    matrix::Matrix{_RORSExact},
    context::_RORSContext,
)
    rows, columns = size(matrix)
    work = copy(matrix)
    rank = 0
    for column in 1:columns
        context.cancel_check()
        pivot_row = 0
        for row in (rank + 1):rows
            _rors_tick!(context)
            if !iszero(work[row, column])
                pivot_row = row
                break
            end
        end
        iszero(pivot_row) && continue
        rank += 1
        if pivot_row != rank
            for trailing_column in 1:columns
                work[rank, trailing_column], work[pivot_row, trailing_column] =
                    work[pivot_row, trailing_column], work[rank, trailing_column]
            end
        end
        pivot_value = work[rank, column]
        for trailing_column in column:columns
            work[rank, trailing_column] = _rors_exact_divide(
                context, work[rank, trailing_column], pivot_value)
        end
        for row in 1:rows
            row == rank && continue
            factor = work[row, column]
            iszero(factor) && continue
            for trailing_column in column:columns
                product = _rors_exact_multiply(
                    context, factor, work[rank, trailing_column])
                work[row, trailing_column] = _rors_exact_subtract(
                    context, work[row, trailing_column], product)
            end
        end
        rank == min(rows, columns) && break
    end
    return rank
end

function _rors_affine_enclosure(
    state_reference::Vector{_RORSExact},
    control_reference::Vector{_RORSExact},
    slope::Matrix{_RORSExact},
    control_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    state_count, control_count = size(slope)
    result = Vector{ROExactInterval}(undef, state_count)
    for state in 1:state_count
        enclosure = _rors_point(context, state_reference[state])
        for control in 1:control_count
            displacement = _rors_subtract(
                context,
                control_box[control],
                _rors_point(context, control_reference[control]),
            )
            contribution = _rors_multiply(
                context,
                _rors_point(context, slope[state, control]),
                displacement,
            )
            enclosure = _rors_add(context, enclosure, contribution)
        end
        result[state] = enclosure
    end
    return result
end

const _RORSPolynomial = Dict{Tuple,_RORSExact}

@inline function _rors_zero_exponents(control_count::Int)
    return Tuple(fill(0, control_count))
end

function _rors_polynomial_add_term!(
    polynomial::_RORSPolynomial,
    exponents::Tuple,
    coefficient::_RORSExact,
    context::_RORSContext,
)
    iszero(coefficient) && return polynomial
    _rors_tick!(context)
    admitted = _rors_checked(context, coefficient)
    if haskey(polynomial, exponents)
        admitted = _rors_exact_add(
            context, polynomial[exponents], admitted)
        if iszero(admitted)
            delete!(polynomial, exponents)
        else
            polynomial[exponents] = admitted
        end
    else
        polynomial[exponents] = admitted
        _rors_limit(:expanded_polynomial_terms, length(polynomial),
            context.limits.max_expanded_terms)
    end
    return polynomial
end

function _rors_polynomial_constant(
    coefficient::_RORSExact,
    control_count::Int,
    context::_RORSContext,
)
    polynomial = _RORSPolynomial()
    _rors_polynomial_add_term!(
        polynomial,
        _rors_zero_exponents(control_count),
        coefficient,
        context,
    )
    return polynomial
end

function _rors_polynomial_multiply(
    left::_RORSPolynomial,
    right::_RORSPolynomial,
    context::_RORSContext,
)
    (isempty(left) || isempty(right)) && return _RORSPolynomial()
    pair_work = BigInt(length(left)) * length(right)
    remaining = BigInt(context.limits.max_interval_operations) -
        context.operations
    pair_work <= remaining || throw(RORegularSheetLimitExceeded(
        :interval_operations,
        context.operations + pair_work,
        context.limits.max_interval_operations,
    ))
    result = _RORSPolynomial()
    left_keys = sort!(collect(keys(left)))
    right_keys = sort!(collect(keys(right)))
    for left_key in left_keys, right_key in right_keys
        _rors_tick!(context)
        exponents = Tuple(
            left_key[index] + right_key[index] for index in eachindex(left_key))
        total_degree = sum(BigInt, exponents)
        _rors_limit(:expanded_total_degree, total_degree,
            context.limits.max_total_degree)
        coefficient = _rors_exact_multiply(
            context, left[left_key], right[right_key])
        _rors_polynomial_add_term!(
            result, exponents, coefficient, context)
    end
    return result
end

function _rors_polynomial_power(
    base::_RORSPolynomial,
    exponent::Int,
    control_count::Int,
    context::_RORSContext,
)
    exponent >= 0 || throw(ArgumentError(
        "polynomial power must be nonnegative"))
    result = _rors_polynomial_constant(
        one(_RORSExact), control_count, context)
    for _ in 1:exponent
        result = _rors_polynomial_multiply(result, base, context)
    end
    return result
end

function _rors_polynomial_accumulate!(
    target::_RORSPolynomial,
    source::_RORSPolynomial,
    context::_RORSContext,
)
    for exponents in sort!(collect(keys(source)))
        _rors_polynomial_add_term!(
            target, exponents, source[exponents], context)
    end
    return target
end

function _rors_polynomial_scaled_accumulate!(
    target::_RORSPolynomial,
    source::_RORSPolynomial,
    scale::_RORSExact,
    context::_RORSContext,
)
    iszero(scale) && return target
    for exponents in sort!(collect(keys(source)))
        coefficient = _rors_exact_multiply(
            context, scale, source[exponents])
        _rors_polynomial_add_term!(
            target, exponents, coefficient, context)
    end
    return target
end

function _rors_predictor_polynomials(
    state_reference::Vector{_RORSExact},
    control_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    context::_RORSContext,
)
    state_count, control_count = size(predictor_slope)
    result = Vector{_RORSPolynomial}(undef, state_count)
    zero_exponents = _rors_zero_exponents(control_count)
    for state in 1:state_count
        constant = state_reference[state]
        for control in 1:control_count
            product = _rors_exact_multiply(
                context,
                predictor_slope[state, control],
                control_reference[control],
            )
            constant = _rors_exact_subtract(context, constant, product)
        end
        polynomial = _RORSPolynomial()
        _rors_polynomial_add_term!(
            polynomial, zero_exponents, constant, context)
        for control in 1:control_count
            slope = predictor_slope[state, control]
            iszero(slope) && continue
            exponents = collect(zero_exponents)
            exponents[control] = 1
            _rors_polynomial_add_term!(
                polynomial, Tuple(exponents), slope, context)
        end
        result[state] = polynomial
    end
    return result
end

function _rors_affine_tube_polynomials(
    state_reference::Vector{_RORSExact},
    control_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    context::_RORSContext,
)
    state_count, control_count = size(predictor_slope)
    variable_count = control_count + state_count
    result = Vector{_RORSPolynomial}(undef, state_count)
    zero_exponents = _rors_zero_exponents(variable_count)
    for state in 1:state_count
        constant = state_reference[state]
        for control in 1:control_count
            product = _rors_exact_multiply(
                context,
                predictor_slope[state, control],
                control_reference[control],
            )
            constant = _rors_exact_subtract(context, constant, product)
        end
        polynomial = _RORSPolynomial()
        _rors_polynomial_add_term!(
            polynomial, zero_exponents, constant, context)
        for control in 1:control_count
            slope = predictor_slope[state, control]
            iszero(slope) && continue
            exponents = collect(zero_exponents)
            exponents[control] = 1
            _rors_polynomial_add_term!(
                polynomial, Tuple(exponents), slope, context)
        end
        remainder_exponents = collect(zero_exponents)
        remainder_exponents[control_count + state] = 1
        _rors_polynomial_add_term!(
            polynomial,
            Tuple(remainder_exponents),
            one(_RORSExact),
            context,
        )
        result[state] = polynomial
    end
    return result
end

function _rors_compose_equation(
    equation::Tuple,
    state_polynomials::Vector{_RORSPolynomial},
    control_count::Int,
    variable_count::Int,
    context::_RORSContext;
    state_derivative::Int=0,
    control_derivative::Int=0,
)
    state_derivative == 0 || control_derivative == 0 || throw(ArgumentError(
        "one polynomial derivative axis must be selected at a time"))
    0 <= state_derivative <= length(state_polynomials) ||
        throw(BoundsError(state_polynomials, state_derivative))
    0 <= control_derivative <= control_count ||
        throw(BoundsError(1:control_count, control_derivative))
    variable_count >= control_count || throw(ArgumentError(
        "polynomial variable count cannot be smaller than control count"))
    equation_polynomial = _RORSPolynomial()
    zero_exponents = _rors_zero_exponents(variable_count)
    for term in equation
        coefficient = _rors_exact_float(
            term.coefficient, context.limits, "polynomial coefficient")
        if state_derivative > 0
            exponent = term.state_exponents[state_derivative]
            iszero(exponent) && continue
            coefficient = _rors_exact_multiply(
                context, coefficient, _RORSExact(exponent))
        elseif control_derivative > 0
            exponent = term.control_exponents[control_derivative]
            iszero(exponent) && continue
            coefficient = _rors_exact_multiply(
                context, coefficient, _RORSExact(exponent))
        end
        composed = _rors_polynomial_constant(
            coefficient, variable_count, context)
        for state in eachindex(state_polynomials)
            exponent = term.state_exponents[state] -
                (state_derivative == state ? 1 : 0)
            iszero(exponent) && continue
            composed = _rors_polynomial_multiply(
                composed,
                _rors_polynomial_power(
                    state_polynomials[state],
                    exponent,
                    variable_count,
                    context,
                ),
                context,
            )
        end
        control_exponents = collect(zero_exponents)
        for control in 1:control_count
            control_exponents[control] = term.control_exponents[control] -
                (control_derivative == control ? 1 : 0)
        end
        if any(exponent -> !iszero(exponent), control_exponents)
            control_monomial = _RORSPolynomial()
            _rors_polynomial_add_term!(
                control_monomial,
                Tuple(control_exponents),
                one(_RORSExact),
                context,
            )
            composed = _rors_polynomial_multiply(
                composed, control_monomial, context)
        end
        _rors_polynomial_accumulate!(
            equation_polynomial, composed, context)
    end
    return equation_polynomial
end

function _rors_compose_predictor_equation(
    equation::Tuple,
    predictor_polynomials::Vector{_RORSPolynomial},
    control_count::Int,
    context::_RORSContext,
)
    return _rors_compose_equation(
        equation,
        predictor_polynomials,
        control_count,
        control_count,
        context,
    )
end

function _rors_evaluate_polynomial(
    polynomial::_RORSPolynomial,
    variable_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    result = _rors_point(context, zero(_RORSExact))
    for exponents in sort!(collect(keys(polynomial)))
        term_value = _rors_point(context, polynomial[exponents])
        length(exponents) == length(variable_box) || throw(DimensionMismatch(
            "polynomial exponent tuple does not match its evaluation box"))
        for variable in eachindex(variable_box)
            exponent = exponents[variable]
            iszero(exponent) && continue
            term_value = _rors_multiply(
                context,
                term_value,
                _rors_integer_power(
                    context, variable_box[variable], exponent),
            )
        end
        result = _rors_add(context, result, term_value)
    end
    return result
end

function _rors_polynomial_derivative(
    polynomial::_RORSPolynomial,
    variable::Int,
    context::_RORSContext,
)
    result = _RORSPolynomial()
    for exponents in sort!(collect(keys(polynomial)))
        variable in eachindex(exponents) || throw(BoundsError(
            exponents, variable))
        exponent = exponents[variable]
        iszero(exponent) && continue
        derivative_exponents = collect(exponents)
        derivative_exponents[variable] -= 1
        coefficient = _rors_exact_multiply(
            context,
            polynomial[exponents],
            _RORSExact(exponent),
        )
        _rors_polynomial_add_term!(
            result,
            Tuple(derivative_exponents),
            coefficient,
            context,
        )
    end
    return result
end

function _rors_predictor_residual_enclosure(
    system::ROPolynomialEquilibriumSystem,
    state_reference::Vector{_RORSExact},
    control_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    control_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    predictors = _rors_predictor_polynomials(
        state_reference,
        control_reference,
        predictor_slope,
        context,
    )
    residual = Vector{ROExactInterval}(undef, length(system.equations))
    for equation in eachindex(system.equations)
        context.cancel_check()
        composed = _rors_compose_predictor_equation(
            system.equations[equation],
            predictors,
            length(system.control_names),
            context,
        )
        residual[equation] = _rors_evaluate_polynomial(
            composed, control_box, context)
    end
    return residual
end

function _rors_correlated_tube_enclosures(
    system::ROPolynomialEquilibriumSystem,
    state_reference::Vector{_RORSExact},
    control_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    remainder_box::Vector{ROExactInterval},
    control_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    # The exact change of variables x = p(u) + delta keeps every repeated
    # state/control occurrence in one polynomial before interval evaluation.
    # Therefore d/d(delta) is F_x and d/du at fixed delta is F_u + F_x*S.
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    variable_box = vcat(control_box, remainder_box)
    state_polynomials = _rors_affine_tube_polynomials(
        state_reference,
        control_reference,
        predictor_slope,
        context,
    )
    residual = Vector{ROExactInterval}(undef, state_count)
    state_jacobian = Matrix{ROExactInterval}(
        undef, state_count, state_count)
    control_jacobian = Matrix{ROExactInterval}(
        undef, state_count, control_count)
    predictor_total_derivative = Matrix{ROExactInterval}(
        undef, state_count, control_count)
    for equation in 1:state_count
        context.cancel_check()
        composed = _rors_compose_equation(
            system.equations[equation],
            state_polynomials,
            control_count,
            length(variable_box),
            context,
        )
        residual[equation] = _rors_evaluate_polynomial(
            composed, variable_box, context)
        state_derivative_polynomials = Vector{_RORSPolynomial}(
            undef, state_count)
        for state in 1:state_count
            derivative = _rors_polynomial_derivative(
                composed, control_count + state, context)
            state_derivative_polynomials[state] = derivative
            state_jacobian[equation, state] =
                _rors_evaluate_polynomial(
                    derivative, variable_box, context)
        end
        for control in 1:control_count
            total_derivative = _rors_polynomial_derivative(
                composed, control, context)
            predictor_total_derivative[equation, control] =
                _rors_evaluate_polynomial(
                    total_derivative, variable_box, context)
            partial_derivative = _RORSPolynomial()
            _rors_polynomial_accumulate!(
                partial_derivative, total_derivative, context)
            for state in 1:state_count
                _rors_polynomial_scaled_accumulate!(
                    partial_derivative,
                    state_derivative_polynomials[state],
                    -predictor_slope[state, control],
                    context,
                )
            end
            control_jacobian[equation, control] =
                _rors_evaluate_polynomial(
                    partial_derivative, variable_box, context)
        end
    end
    return (
        residual,
        state_jacobian,
        control_jacobian,
        predictor_total_derivative,
    )
end

function _rors_point_interval_matrix_product(
    left::Matrix{_RORSExact},
    right::Matrix{ROExactInterval},
    context::_RORSContext,
)
    left_rows, shared = size(left)
    size(right, 1) == shared || throw(DimensionMismatch(
        "exact point/interval matrix product has incompatible dimensions"))
    right_columns = size(right, 2)
    result = Matrix{ROExactInterval}(undef, left_rows, right_columns)
    for column in 1:right_columns, row in 1:left_rows
        value = _rors_point(context, zero(_RORSExact))
        for inner in 1:shared
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    _rors_point(context, left[row, inner]),
                    right[inner, column],
                ),
            )
        end
        result[row, column] = value
    end
    return result
end

function _rors_interval_point_matrix_product(
    left::Matrix{ROExactInterval},
    right::Matrix{_RORSExact},
    context::_RORSContext,
)
    left_rows, shared = size(left)
    size(right, 1) == shared || throw(DimensionMismatch(
        "exact interval/point matrix product has incompatible dimensions"))
    right_columns = size(right, 2)
    result = Matrix{ROExactInterval}(undef, left_rows, right_columns)
    for column in 1:right_columns, row in 1:left_rows
        value = _rors_point(context, zero(_RORSExact))
        for inner in 1:shared
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    left[row, inner],
                    _rors_point(context, right[inner, column]),
                ),
            )
        end
        result[row, column] = value
    end
    return result
end

function _rors_point_interval_vector_product(
    left::Matrix{_RORSExact},
    right::Vector{ROExactInterval},
    context::_RORSContext,
)
    rows, shared = size(left)
    length(right) == shared || throw(DimensionMismatch(
        "exact point/interval vector product has incompatible dimensions"))
    result = Vector{ROExactInterval}(undef, rows)
    for row in 1:rows
        value = _rors_point(context, zero(_RORSExact))
        for inner in 1:shared
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    _rors_point(context, left[row, inner]),
                    right[inner],
                ),
            )
        end
        result[row] = value
    end
    return result
end

function _rors_interval_vector_product(
    left::Matrix{ROExactInterval},
    right::Vector{ROExactInterval},
    context::_RORSContext,
)
    rows, shared = size(left)
    length(right) == shared || throw(DimensionMismatch(
        "exact interval-vector product has incompatible dimensions"))
    result = Vector{ROExactInterval}(undef, rows)
    for row in 1:rows
        value = _rors_point(context, zero(_RORSExact))
        for inner in 1:shared
            value = _rors_add(
                context,
                value,
                _rors_multiply(context, left[row, inner], right[inner]),
            )
        end
        result[row] = value
    end
    return result
end

function _rors_interval_matrix_add(
    left::Matrix{ROExactInterval},
    right::Matrix{ROExactInterval},
    context::_RORSContext,
)
    size(left) == size(right) || throw(DimensionMismatch(
        "exact interval matrices have incompatible dimensions"))
    result = similar(left)
    for index in eachindex(left)
        result[index] = _rors_add(context, left[index], right[index])
    end
    return result
end

function _rors_error_matrix(
    preconditioned_jacobian::Matrix{ROExactInterval},
    context::_RORSContext,
)
    rows, columns = size(preconditioned_jacobian)
    rows == columns || throw(DimensionMismatch(
        "preconditioned state Jacobian must be square"))
    result = similar(preconditioned_jacobian)
    for column in 1:columns, row in 1:rows
        identity_value = row == column ? one(_RORSExact) : zero(_RORSExact)
        result[row, column] = _rors_subtract(
            context,
            _rors_point(context, identity_value),
            preconditioned_jacobian[row, column],
        )
    end
    return result
end

function _rors_negate_vector(
    values::Vector{ROExactInterval},
    context::_RORSContext,
)
    return [_rors_negate(context, value) for value in values]
end

function _rors_negate_matrix(
    values::Matrix{ROExactInterval},
    context::_RORSContext,
)
    result = similar(values)
    for index in eachindex(values)
        result[index] = _rors_negate(context, values[index])
    end
    return result
end

function _rors_beta(
    error_matrix::Matrix{ROExactInterval},
    context::_RORSContext,
)
    rows, columns = size(error_matrix)
    beta = zero(_RORSExact)
    for row in 1:rows
        row_sum = zero(_RORSExact)
        for column in 1:columns
            row_sum = _rors_exact_add(
                context,
                row_sum,
                _rors_abs_upper(error_matrix[row, column]),
            )
        end
        beta = max(beta, row_sum)
    end
    return beta
end

function _rors_exact_matrix_values(matrix::ROExactMatrix)
    return reshape(collect(matrix.data), matrix.row_count, matrix.column_count)
end

function _rors_interval_matrix_values(matrix::ROExactIntervalMatrix)
    return reshape(collect(matrix.data), matrix.row_count, matrix.column_count)
end

function _rors_branch_identity_sha256(
    system_declaration_sha256::String,
    control_box::Tuple,
    control_reference::Tuple,
    state_reference::Tuple,
    predictor_slope::ROExactMatrix,
    remainder_box::Tuple,
)
    io = IOBuffer()
    _rors_write_token(io, "bne-ro-local-regular-branch-identity/v1.0.0")
    _rors_write_token(io, system_declaration_sha256)
    _rors_write_interval_vector(io, control_box)
    _rors_write_exact_vector(io, control_reference)
    _rors_write_exact_vector(io, state_reference)
    _rors_write_exact_matrix(io, predictor_slope)
    _rors_write_interval_vector(io, remainder_box)
    return bytes2hex(SHA.sha256(take!(io)))
end

function _rors_patch_payload(
    version::String,
    system_declaration_sha256::String,
    limits::RORegularSheetLimits,
    control_box::Tuple,
    control_reference::Tuple,
    state_reference::Tuple,
    predictor_slope::ROExactMatrix,
    remainder_box::Tuple,
    preconditioner::ROExactMatrix,
    predictor_state_enclosure::Tuple,
    tube_state_enclosure::Tuple,
    predictor_residual_enclosure::Tuple,
    tube_residual_enclosure::Tuple,
    state_jacobian_enclosure::ROExactIntervalMatrix,
    control_jacobian_enclosure::ROExactIntervalMatrix,
    predictor_total_derivative_enclosure::ROExactIntervalMatrix,
    error_matrix_enclosure::ROExactIntervalMatrix,
    krawczyk_image::Tuple,
    strict_lower_margins::Tuple,
    strict_upper_margins::Tuple,
    contraction_beta::_RORSExact,
    implicit_derivative_center::ROExactMatrix,
    implicit_derivative_radii::Tuple,
    implicit_derivative_enclosure::ROExactIntervalMatrix,
    exact_operation_count::Int,
    root_exists_for_every_control::Bool,
    unique_inside_declared_tube::Bool,
    roots_outside_declared_tube_excluded::Bool,
    state_jacobian_nonsingular_on_tube::Bool,
    implicit_derivative_enclosed::Bool,
    evidence_scope::Symbol,
    branch_identity_sha256::String,
)
    return (
        version=version,
        system_declaration_sha256=system_declaration_sha256,
        limits=limits,
        control_box=control_box,
        control_reference=control_reference,
        state_reference=state_reference,
        predictor_slope=predictor_slope,
        remainder_box=remainder_box,
        preconditioner=preconditioner,
        predictor_state_enclosure=predictor_state_enclosure,
        tube_state_enclosure=tube_state_enclosure,
        predictor_residual_enclosure=predictor_residual_enclosure,
        tube_residual_enclosure=tube_residual_enclosure,
        state_jacobian_enclosure=state_jacobian_enclosure,
        control_jacobian_enclosure=control_jacobian_enclosure,
        predictor_total_derivative_enclosure=
            predictor_total_derivative_enclosure,
        error_matrix_enclosure=error_matrix_enclosure,
        krawczyk_image=krawczyk_image,
        strict_lower_margins=strict_lower_margins,
        strict_upper_margins=strict_upper_margins,
        contraction_beta=contraction_beta,
        implicit_derivative_center=implicit_derivative_center,
        implicit_derivative_radii=implicit_derivative_radii,
        implicit_derivative_enclosure=implicit_derivative_enclosure,
        exact_operation_count=exact_operation_count,
        root_exists_for_every_control=root_exists_for_every_control,
        unique_inside_declared_tube=unique_inside_declared_tube,
        roots_outside_declared_tube_excluded=
            roots_outside_declared_tube_excluded,
        state_jacobian_nonsingular_on_tube=
            state_jacobian_nonsingular_on_tube,
        implicit_derivative_enclosed=implicit_derivative_enclosed,
        evidence_scope=evidence_scope,
        branch_identity_sha256=branch_identity_sha256,
    )
end

function _rors_patch_sha256(data)
    io = IOBuffer()
    _rors_write_token(io, data.version)
    _rors_write_token(io, data.system_declaration_sha256)
    _rors_write_limits(io, data.limits)
    _rors_write_interval_vector(io, data.control_box)
    _rors_write_exact_vector(io, data.control_reference)
    _rors_write_exact_vector(io, data.state_reference)
    _rors_write_exact_matrix(io, data.predictor_slope)
    _rors_write_interval_vector(io, data.remainder_box)
    _rors_write_exact_matrix(io, data.preconditioner)
    _rors_write_interval_vector(io, data.predictor_state_enclosure)
    _rors_write_interval_vector(io, data.tube_state_enclosure)
    _rors_write_interval_vector(io, data.predictor_residual_enclosure)
    _rors_write_interval_vector(io, data.tube_residual_enclosure)
    _rors_write_interval_matrix(io, data.state_jacobian_enclosure)
    _rors_write_interval_matrix(io, data.control_jacobian_enclosure)
    _rors_write_interval_matrix(
        io, data.predictor_total_derivative_enclosure)
    _rors_write_interval_matrix(io, data.error_matrix_enclosure)
    _rors_write_interval_vector(io, data.krawczyk_image)
    _rors_write_exact_vector(io, data.strict_lower_margins)
    _rors_write_exact_vector(io, data.strict_upper_margins)
    _rors_write_exact(io, data.contraction_beta)
    _rors_write_exact_matrix(io, data.implicit_derivative_center)
    _rors_write_exact_vector(io, data.implicit_derivative_radii)
    _rors_write_interval_matrix(io, data.implicit_derivative_enclosure)
    _rors_write_token(io, data.exact_operation_count)
    _rors_write_token(io, data.root_exists_for_every_control)
    _rors_write_token(io, data.unique_inside_declared_tube)
    _rors_write_token(io, data.roots_outside_declared_tube_excluded)
    _rors_write_token(io, data.state_jacobian_nonsingular_on_tube)
    _rors_write_token(io, data.implicit_derivative_enclosed)
    _rors_write_token(io, data.evidence_scope)
    _rors_write_token(io, data.branch_identity_sha256)
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Exact P5r0 proof for one affine root tube over one positive control box.

For every declared control, this artifact proves one root exists and is unique
inside the declared tube. It deliberately does not exclude roots outside that
tube.
"""
struct RORegularSheetPatchCertificate
    version::String
    system_declaration_sha256::String
    limits::RORegularSheetLimits
    control_box::Tuple
    control_reference::Tuple
    state_reference::Tuple
    predictor_slope::ROExactMatrix
    remainder_box::Tuple
    preconditioner::ROExactMatrix
    predictor_state_enclosure::Tuple
    tube_state_enclosure::Tuple
    predictor_residual_enclosure::Tuple
    tube_residual_enclosure::Tuple
    state_jacobian_enclosure::ROExactIntervalMatrix
    control_jacobian_enclosure::ROExactIntervalMatrix
    predictor_total_derivative_enclosure::ROExactIntervalMatrix
    error_matrix_enclosure::ROExactIntervalMatrix
    krawczyk_image::Tuple
    strict_lower_margins::Tuple
    strict_upper_margins::Tuple
    contraction_beta::_RORSExact
    implicit_derivative_center::ROExactMatrix
    implicit_derivative_radii::Tuple
    implicit_derivative_enclosure::ROExactIntervalMatrix
    exact_operation_count::Int
    root_exists_for_every_control::Bool
    unique_inside_declared_tube::Bool
    roots_outside_declared_tube_excluded::Bool
    state_jacobian_nonsingular_on_tube::Bool
    implicit_derivative_enclosed::Bool
    evidence_scope::Symbol
    branch_identity_sha256::String
    certificate_sha256::String

    function RORegularSheetPatchCertificate(
        version::String,
        system_declaration_sha256::String,
        limits::RORegularSheetLimits,
        control_box::Tuple,
        control_reference::Tuple,
        state_reference::Tuple,
        predictor_slope::ROExactMatrix,
        remainder_box::Tuple,
        preconditioner::ROExactMatrix,
        predictor_state_enclosure::Tuple,
        tube_state_enclosure::Tuple,
        predictor_residual_enclosure::Tuple,
        tube_residual_enclosure::Tuple,
        state_jacobian_enclosure::ROExactIntervalMatrix,
        control_jacobian_enclosure::ROExactIntervalMatrix,
        predictor_total_derivative_enclosure::ROExactIntervalMatrix,
        error_matrix_enclosure::ROExactIntervalMatrix,
        krawczyk_image::Tuple,
        strict_lower_margins::Tuple,
        strict_upper_margins::Tuple,
        contraction_beta::_RORSExact,
        implicit_derivative_center::ROExactMatrix,
        implicit_derivative_radii::Tuple,
        implicit_derivative_enclosure::ROExactIntervalMatrix,
        exact_operation_count::Int,
        root_exists_for_every_control::Bool,
        unique_inside_declared_tube::Bool,
        roots_outside_declared_tube_excluded::Bool,
        state_jacobian_nonsingular_on_tube::Bool,
        implicit_derivative_enclosed::Bool,
        evidence_scope::Symbol,
        branch_identity_sha256::String,
        ::Val{:validated},
    )
        version == RO_REGULAR_SHEET_PATCH_VERSION || throw(ArgumentError(
            "regular-sheet patch version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        _rors_validate_sha256(
            branch_identity_sha256, "branch_identity_sha256")
        evidence_scope == RO_REGULAR_SHEET_PATCH_SCOPE || throw(ArgumentError(
            "regular-sheet patch evidence scope mismatch"))
        root_exists_for_every_control || throw(ArgumentError(
            "regular-sheet patch must retain its existence result"))
        unique_inside_declared_tube || throw(ArgumentError(
            "regular-sheet patch must retain tube-relative uniqueness"))
        roots_outside_declared_tube_excluded && throw(ArgumentError(
            "P5r0 cannot exclude roots outside the declared tube"))
        state_jacobian_nonsingular_on_tube || throw(ArgumentError(
            "regular-sheet patch must retain interval nonsingularity"))
        implicit_derivative_enclosed || throw(ArgumentError(
            "regular-sheet patch must retain its derivative enclosure"))
        0 <= contraction_beta < 1 || throw(ArgumentError(
            "regular-sheet contraction beta must lie in [0,1)"))
        exact_operation_count >= 0 || throw(ArgumentError(
            "exact_operation_count must be nonnegative"))
        predictor_total_derivative_enclosure.row_count ==
            length(state_reference) &&
            predictor_total_derivative_enclosure.column_count ==
                length(control_reference) || throw(DimensionMismatch(
            "predictor total derivative must match equation/control order"))
        _rors_limit(:interval_operations, exact_operation_count,
            limits.max_interval_operations)
        all(>(zero(_RORSExact)), strict_lower_margins) &&
            all(>(zero(_RORSExact)), strict_upper_margins) ||
            throw(ArgumentError(
                "regular-sheet Krawczyk margins must be strictly positive"))
        data = _rors_patch_payload(
            version,
            system_declaration_sha256,
            limits,
            control_box,
            control_reference,
            state_reference,
            predictor_slope,
            remainder_box,
            preconditioner,
            predictor_state_enclosure,
            tube_state_enclosure,
            predictor_residual_enclosure,
            tube_residual_enclosure,
            state_jacobian_enclosure,
            control_jacobian_enclosure,
            predictor_total_derivative_enclosure,
            error_matrix_enclosure,
            krawczyk_image,
            strict_lower_margins,
            strict_upper_margins,
            contraction_beta,
            implicit_derivative_center,
            implicit_derivative_radii,
            implicit_derivative_enclosure,
            exact_operation_count,
            root_exists_for_every_control,
            unique_inside_declared_tube,
            roots_outside_declared_tube_excluded,
            state_jacobian_nonsingular_on_tube,
            implicit_derivative_enclosed,
            evidence_scope,
            branch_identity_sha256,
        )
        certificate_sha256 = _rors_patch_sha256(data)
        return new(
            version,
            system_declaration_sha256,
            limits,
            control_box,
            control_reference,
            state_reference,
            predictor_slope,
            remainder_box,
            preconditioner,
            predictor_state_enclosure,
            tube_state_enclosure,
            predictor_residual_enclosure,
            tube_residual_enclosure,
            state_jacobian_enclosure,
            control_jacobian_enclosure,
            predictor_total_derivative_enclosure,
            error_matrix_enclosure,
            krawczyk_image,
            strict_lower_margins,
            strict_upper_margins,
            contraction_beta,
            implicit_derivative_center,
            implicit_derivative_radii,
            implicit_derivative_enclosure,
            exact_operation_count,
            true,
            true,
            false,
            true,
            true,
            evidence_scope,
            branch_identity_sha256,
            certificate_sha256,
        )
    end
end

function _rors_require_dyadic_inputs(
    control_box::Vector{ROExactInterval},
    control_reference::Vector{_RORSExact},
    state_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    remainder_box::Vector{ROExactInterval},
    preconditioner::Matrix{_RORSExact},
    context::_RORSContext,
)
    for values in (
        control_reference,
        state_reference,
        vec(predictor_slope),
        vec(preconditioner),
    ), value in values
        _rors_tick!(context)
        _rors_is_dyadic(value) || throw(ArgumentError(
            "P5r0 declarations must be exact dyadic rationals"))
        _rors_checked(context, value)
    end
    for box in (control_box, remainder_box), interval in box,
            value in (interval.lower, interval.upper)
        _rors_tick!(context)
        _rors_is_dyadic(value) || throw(ArgumentError(
            "P5r0 interval declarations must be exact dyadic rationals"))
        _rors_checked(context, value)
    end
    return nothing
end

function _rors_certify_patch_exact(
    system::ROPolynomialEquilibriumSystem,
    control_box::Vector{ROExactInterval},
    control_reference::Vector{_RORSExact},
    state_reference::Vector{_RORSExact},
    predictor_slope::Matrix{_RORSExact},
    remainder_box::Vector{ROExactInterval},
    preconditioner::Matrix{_RORSExact},
    context::_RORSContext,
)
    validate_ro_polynomial_equilibrium_system(system)
    start_operations = context.operations
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    length(control_box) == control_count || throw(DimensionMismatch(
        "control_box does not match the system control count"))
    length(control_reference) == control_count || throw(DimensionMismatch(
        "control_reference does not match the system control count"))
    length(state_reference) == state_count || throw(DimensionMismatch(
        "state_reference does not match the system state count"))
    size(predictor_slope) == (state_count, control_count) ||
        throw(DimensionMismatch(
            "predictor_slope must have shape state_count x control_count"))
    length(remainder_box) == state_count || throw(DimensionMismatch(
        "remainder_box does not match the system state count"))
    size(preconditioner) == (state_count, state_count) ||
        throw(DimensionMismatch(
            "preconditioner must have shape state_count x state_count"))
    _rors_require_dyadic_inputs(
        control_box,
        control_reference,
        state_reference,
        predictor_slope,
        remainder_box,
        preconditioner,
        context,
    )
    for control in 1:control_count
        interval = control_box[control]
        interval.lower > 0 || throw(RORegularSheetCertificationRejected(
            :nonpositive_control_box,
            "control coordinate $control is not strictly positive",
        ))
        interval.lower < interval.upper || throw(
            RORegularSheetCertificationRejected(
                :degenerate_control_box,
                "control coordinate $control must have positive width",
            ))
        interval.lower <= control_reference[control] <= interval.upper ||
            throw(RORegularSheetCertificationRejected(
                :control_reference_outside_box,
                "control reference $control lies outside its box",
            ))
    end
    for state in 1:state_count
        interval = remainder_box[state]
        interval.lower < 0 < interval.upper || throw(
            RORegularSheetCertificationRejected(
                :remainder_does_not_contain_zero_interior,
                "state remainder $state must contain zero strictly",
            ))
    end
    _rors_exact_rank(preconditioner, context) == state_count || throw(
        RORegularSheetCertificationRejected(
            :singular_preconditioner,
            "the exact dyadic preconditioner is not full rank",
        ))

    predictor_state = _rors_affine_enclosure(
        state_reference,
        control_reference,
        predictor_slope,
        control_box,
        context,
    )
    tube_state = Vector{ROExactInterval}(undef, state_count)
    for state in 1:state_count
        tube_state[state] = _rors_add(
            context, predictor_state[state], remainder_box[state])
        tube_state[state].lower > 0 || throw(
            RORegularSheetCertificationRejected(
                :nonpositive_state_tube,
                "state tube $state leaves strictly positive linear coordinates",
            ))
    end

    predictor_residual = _rors_predictor_residual_enclosure(
        system,
        state_reference,
        control_reference,
        predictor_slope,
        control_box,
        context,
    )
    tube_residual, state_jacobian, control_jacobian,
        predictor_total_derivative = _rors_correlated_tube_enclosures(
            system,
            state_reference,
            control_reference,
            predictor_slope,
            remainder_box,
            control_box,
            context,
        )
    preconditioned_jacobian = _rors_point_interval_matrix_product(
        preconditioner, state_jacobian, context)
    error_matrix = _rors_error_matrix(preconditioned_jacobian, context)
    beta = _rors_beta(error_matrix, context)
    beta < 1 || throw(RORegularSheetCertificationRejected(
        :contraction_not_proven,
        "exact infinity-norm error bound beta=$beta is not below one",
    ))

    centered_residual = _rors_negate_vector(
        _rors_point_interval_vector_product(
            preconditioner, predictor_residual, context),
        context,
    )
    remainder_contribution = _rors_interval_vector_product(
        error_matrix, remainder_box, context)
    krawczyk_image = Vector{ROExactInterval}(undef, state_count)
    strict_lower_margins = Vector{_RORSExact}(undef, state_count)
    strict_upper_margins = Vector{_RORSExact}(undef, state_count)
    for state in 1:state_count
        krawczyk_image[state] = _rors_add(
            context, centered_residual[state], remainder_contribution[state])
        _rors_strict_subset(krawczyk_image[state], remainder_box[state]) ||
            throw(RORegularSheetCertificationRejected(
                :krawczyk_not_strictly_inside_remainder,
                "Krawczyk coordinate $state is not strictly inside its remainder interval",
            ))
        strict_lower_margins[state] = _rors_exact_subtract(
            context,
            krawczyk_image[state].lower,
            remainder_box[state].lower,
        )
        strict_upper_margins[state] = _rors_exact_subtract(
            context,
            remainder_box[state].upper,
            krawczyk_image[state].upper,
        )
    end
    all(_rors_contains_zero, tube_residual) || throw(
        RORegularSheetCertificationRejected(
            :tube_residual_enclosure_excludes_zero,
            "the correlated full-tube residual enclosure excludes zero",
        ))

    derivative_correction = _rors_negate_matrix(
        _rors_point_interval_matrix_product(
            preconditioner, predictor_total_derivative, context),
        context,
    )
    derivative_radii = Vector{_RORSExact}(undef, control_count)
    derivative_enclosure = Matrix{ROExactInterval}(
        undef, state_count, control_count)
    denominator_value = _rors_exact_subtract(
        context, one(_RORSExact), beta)
    for control in 1:control_count
        correction_bound = zero(_RORSExact)
        for state in 1:state_count
            correction_bound = max(
                correction_bound,
                _rors_abs_upper(derivative_correction[state, control]),
            )
        end
        radius = _rors_exact_divide(
            context, correction_bound, denominator_value)
        derivative_radii[control] = radius
        for state in 1:state_count
            center = predictor_slope[state, control]
            derivative_enclosure[state, control] = _rors_interval(
                context,
                _rors_exact_subtract(context, center, radius),
                _rors_exact_add(context, center, radius),
            )
        end
    end
    context.cancel_check()
    operation_count_big = context.operations - start_operations
    operation_count_big <= typemax(Int) || throw(
        RORegularSheetLimitExceeded(
            :interval_operations,
            operation_count_big,
            context.limits.max_interval_operations,
        ))
    exact_operation_count = Int(operation_count_big)
    control_box_tuple = Tuple(control_box)
    control_reference_tuple = Tuple(control_reference)
    state_reference_tuple = Tuple(state_reference)
    predictor_slope_wrapper = _rors_exact_matrix_wrapper(predictor_slope)
    remainder_box_tuple = Tuple(remainder_box)
    preconditioner_wrapper = _rors_exact_matrix_wrapper(preconditioner)
    branch_identity_sha256 = _rors_branch_identity_sha256(
        system.declaration_sha256,
        control_box_tuple,
        control_reference_tuple,
        state_reference_tuple,
        predictor_slope_wrapper,
        remainder_box_tuple,
    )
    data = _rors_patch_payload(
        RO_REGULAR_SHEET_PATCH_VERSION,
        system.declaration_sha256,
        system.limits,
        control_box_tuple,
        control_reference_tuple,
        state_reference_tuple,
        predictor_slope_wrapper,
        remainder_box_tuple,
        preconditioner_wrapper,
        Tuple(predictor_state),
        Tuple(tube_state),
        Tuple(predictor_residual),
        Tuple(tube_residual),
        _rors_interval_matrix_wrapper(state_jacobian),
        _rors_interval_matrix_wrapper(control_jacobian),
        _rors_interval_matrix_wrapper(predictor_total_derivative),
        _rors_interval_matrix_wrapper(error_matrix),
        Tuple(krawczyk_image),
        Tuple(strict_lower_margins),
        Tuple(strict_upper_margins),
        beta,
        predictor_slope_wrapper,
        Tuple(derivative_radii),
        _rors_interval_matrix_wrapper(derivative_enclosure),
        exact_operation_count,
        true,
        true,
        false,
        true,
        true,
        RO_REGULAR_SHEET_PATCH_SCOPE,
        branch_identity_sha256,
    )
    return RORegularSheetPatchCertificate(
        data.version,
        data.system_declaration_sha256,
        data.limits,
        data.control_box,
        data.control_reference,
        data.state_reference,
        data.predictor_slope,
        data.remainder_box,
        data.preconditioner,
        data.predictor_state_enclosure,
        data.tube_state_enclosure,
        data.predictor_residual_enclosure,
        data.tube_residual_enclosure,
        data.state_jacobian_enclosure,
        data.control_jacobian_enclosure,
        data.predictor_total_derivative_enclosure,
        data.error_matrix_enclosure,
        data.krawczyk_image,
        data.strict_lower_margins,
        data.strict_upper_margins,
        data.contraction_beta,
        data.implicit_derivative_center,
        data.implicit_derivative_radii,
        data.implicit_derivative_enclosure,
        data.exact_operation_count,
        data.root_exists_for_every_control,
        data.unique_inside_declared_tube,
        data.roots_outside_declared_tube_excluded,
        data.state_jacobian_nonsingular_on_tube,
        data.implicit_derivative_enclosed,
        data.evidence_scope,
        data.branch_identity_sha256,
        Val(:validated),
    )
end

"""
    certify_ro_regular_sheet_patch(system; ...)

Certify a parameterized affine root tube by exact interval evaluation of a
declarative polynomial system. Every supplied number must be Float64 and is
reinterpreted as its exact binary rational. No solver result or callback is
accepted as proof input.
"""
function certify_ro_regular_sheet_patch(
    system::ROPolynomialEquilibriumSystem;
    control_lower,
    control_upper,
    control_reference,
    state_reference,
    predictor_slope,
    remainder_lower,
    remainder_upper,
    preconditioner,
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    context = _RORSContext(system.limits, cancel_check)
    control_box = _rors_exact_box(
        control_lower,
        control_upper,
        control_count,
        "control",
        context,
    )
    admitted_control_reference = _rors_exact_vector(
        control_reference,
        control_count,
        "control_reference",
        context,
    )
    admitted_state_reference = _rors_exact_vector(
        state_reference,
        state_count,
        "state_reference",
        context,
    )
    admitted_slope = _rors_exact_matrix(
        predictor_slope,
        state_count,
        control_count,
        "predictor_slope",
        context,
    )
    remainder_box = _rors_exact_box(
        remainder_lower,
        remainder_upper,
        state_count,
        "remainder",
        context,
    )
    admitted_preconditioner = _rors_exact_matrix(
        preconditioner,
        state_count,
        state_count,
        "preconditioner",
        context,
    )
    return _rors_certify_patch_exact(
        system,
        control_box,
        admitted_control_reference,
        admitted_state_reference,
        admitted_slope,
        remainder_box,
        admitted_preconditioner,
        context,
    )
end

function _rors_replay_patch_exact(
    system::ROPolynomialEquilibriumSystem,
    certificate::RORegularSheetPatchCertificate,
    context::_RORSContext,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "regular-sheet patch belongs to a different equilibrium system"))
    certificate.limits == system.limits || throw(ArgumentError(
        "regular-sheet patch limits do not match the equilibrium system"))
    rebuilt = _rors_certify_patch_exact(
        system,
        collect(certificate.control_box),
        collect(certificate.control_reference),
        collect(certificate.state_reference),
        _rors_exact_matrix_values(certificate.predictor_slope),
        collect(certificate.remainder_box),
        _rors_exact_matrix_values(certificate.preconditioner),
        context,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "regular-sheet patch replay changed its certificate hash"))
    return rebuilt
end

function replay_ro_regular_sheet_patch(
    system::ROPolynomialEquilibriumSystem,
    certificate::RORegularSheetPatchCertificate;
    cancel_check=() -> nothing,
)
    context = _RORSContext(system.limits, cancel_check)
    return _rors_replay_patch_exact(system, certificate, context)
end

function validate_ro_regular_sheet_patch(
    system::ROPolynomialEquilibriumSystem,
    certificate::RORegularSheetPatchCertificate;
    cancel_check=() -> nothing,
)
    replay_ro_regular_sheet_patch(
        system, certificate; cancel_check=cancel_check)
    return true
end

function _rors_bridge_sha256(
    system_declaration_sha256::String,
    parent_patch_sha256::String,
    child_patch_sha256::String,
    bridge_patch_sha256::String,
    inherited_branch_identity_sha256::String,
    exact_operation_count::Int,
    parent_tube_contained::Bool,
    child_tube_contained::Bool,
    same_root_on_overlap::Bool,
    evidence_scope::Symbol,
)
    io = IOBuffer()
    _rors_write_token(io, RO_REGULAR_SHEET_BRIDGE_VERSION)
    _rors_write_token(io, system_declaration_sha256)
    _rors_write_token(io, parent_patch_sha256)
    _rors_write_token(io, child_patch_sha256)
    _rors_write_token(io, bridge_patch_sha256)
    _rors_write_token(io, inherited_branch_identity_sha256)
    _rors_write_token(io, exact_operation_count)
    _rors_write_token(io, parent_tube_contained)
    _rors_write_token(io, child_tube_contained)
    _rors_write_token(io, same_root_on_overlap)
    _rors_write_token(io, evidence_scope)
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Directed parent-to-child branch bridge. The bridge reruns a Krawczyk proof on a
full-dimensional overlap and proves that both patch tubes are contained in the
bridge tube there. Hence the parent, child, and bridge unique roots coincide on
the overlap. This is not a global continuation or fold certificate.
"""
struct RORegularSheetBridgeCertificate
    version::String
    system_declaration_sha256::String
    parent_patch_sha256::String
    child_patch_sha256::String
    bridge_patch::RORegularSheetPatchCertificate
    inherited_branch_identity_sha256::String
    exact_operation_count::Int
    parent_tube_contained::Bool
    child_tube_contained::Bool
    same_root_on_overlap::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function RORegularSheetBridgeCertificate(
        version::String,
        system_declaration_sha256::String,
        parent_patch_sha256::String,
        child_patch_sha256::String,
        bridge_patch::RORegularSheetPatchCertificate,
        inherited_branch_identity_sha256::String,
        exact_operation_count::Int,
        parent_tube_contained::Bool,
        child_tube_contained::Bool,
        same_root_on_overlap::Bool,
        evidence_scope::Symbol,
        ::Val{:validated},
    )
        version == RO_REGULAR_SHEET_BRIDGE_VERSION || throw(ArgumentError(
            "regular-sheet bridge version mismatch"))
        for (label, value) in (
            ("system_declaration_sha256", system_declaration_sha256),
            ("parent_patch_sha256", parent_patch_sha256),
            ("child_patch_sha256", child_patch_sha256),
            ("inherited_branch_identity_sha256",
                inherited_branch_identity_sha256),
        )
            _rors_validate_sha256(value, label)
        end
        bridge_patch.system_declaration_sha256 ==
            system_declaration_sha256 || throw(ArgumentError(
                "bridge patch belongs to a different equilibrium system"))
        evidence_scope == RO_REGULAR_SHEET_BRIDGE_SCOPE || throw(ArgumentError(
            "regular-sheet bridge evidence scope mismatch"))
        parent_tube_contained && child_tube_contained &&
            same_root_on_overlap || throw(ArgumentError(
                "regular-sheet bridge must retain both containment proofs and the shared-root conclusion"))
        exact_operation_count >= 0 || throw(ArgumentError(
            "bridge exact_operation_count must be nonnegative"))
        _rors_limit(:interval_operations, exact_operation_count,
            bridge_patch.limits.max_interval_operations)
        certificate_sha256 = _rors_bridge_sha256(
            system_declaration_sha256,
            parent_patch_sha256,
            child_patch_sha256,
            bridge_patch.certificate_sha256,
            inherited_branch_identity_sha256,
            exact_operation_count,
            parent_tube_contained,
            child_tube_contained,
            same_root_on_overlap,
            evidence_scope,
        )
        return new(
            version,
            system_declaration_sha256,
            parent_patch_sha256,
            child_patch_sha256,
            bridge_patch,
            inherited_branch_identity_sha256,
            exact_operation_count,
            true,
            true,
            true,
            evidence_scope,
            certificate_sha256,
        )
    end
end

function _rors_overlap_subset(
    overlap::Vector{ROExactInterval},
    patch_box::Tuple,
)
    length(overlap) == length(patch_box) || return false
    return all(index -> _rors_subset(overlap[index], patch_box[index]),
        eachindex(overlap))
end

function _rors_patch_tube_relative_to_bridge(
    patch::RORegularSheetPatchCertificate,
    bridge_patch::RORegularSheetPatchCertificate,
    overlap::Vector{ROExactInterval},
    context::_RORSContext,
)
    state_count = length(patch.state_reference)
    control_count = length(patch.control_reference)
    patch_slope = _rors_exact_matrix_values(patch.predictor_slope)
    bridge_slope = _rors_exact_matrix_values(bridge_patch.predictor_slope)
    result = Vector{ROExactInterval}(undef, state_count)
    for state in 1:state_count
        constant = _rors_exact_subtract(
            context,
            patch.state_reference[state],
            bridge_patch.state_reference[state],
        )
        for control in 1:control_count
            patch_offset = _rors_exact_multiply(
                context,
                patch_slope[state, control],
                patch.control_reference[control],
            )
            bridge_offset = _rors_exact_multiply(
                context,
                bridge_slope[state, control],
                bridge_patch.control_reference[control],
            )
            constant = _rors_exact_subtract(
                context, constant, patch_offset)
            constant = _rors_exact_add(
                context, constant, bridge_offset)
        end
        difference = _rors_point(context, constant)
        for control in 1:control_count
            coefficient = _rors_exact_subtract(
                context,
                patch_slope[state, control],
                bridge_slope[state, control],
            )
            iszero(coefficient) && continue
            difference = _rors_add(
                context,
                difference,
                _rors_multiply(
                    context,
                    _rors_point(context, coefficient),
                    overlap[control],
                ),
            )
        end
        result[state] = _rors_add(
            context, difference, patch.remainder_box[state])
    end
    return result
end

function _rors_certify_bridge_exact(
    system::ROPolynomialEquilibriumSystem,
    parent::RORegularSheetPatchCertificate,
    child::RORegularSheetPatchCertificate,
    overlap::Vector{ROExactInterval},
    bridge_control_reference::Vector{_RORSExact},
    bridge_state_reference::Vector{_RORSExact},
    bridge_predictor_slope::Matrix{_RORSExact},
    bridge_remainder::Vector{ROExactInterval},
    bridge_preconditioner::Matrix{_RORSExact},
    context::_RORSContext,
)
    start_operations = context.operations
    rebuilt_parent = _rors_replay_patch_exact(system, parent, context)
    rebuilt_child = _rors_replay_patch_exact(system, child, context)
    _rors_overlap_subset(overlap, rebuilt_parent.control_box) || throw(
        RORegularSheetCertificationRejected(
            :overlap_outside_parent_control_box,
            "the bridge control box is not contained in the parent patch",
        ))
    _rors_overlap_subset(overlap, rebuilt_child.control_box) || throw(
        RORegularSheetCertificationRejected(
            :overlap_outside_child_control_box,
            "the bridge control box is not contained in the child patch",
        ))
    bridge_patch = _rors_certify_patch_exact(
        system,
        overlap,
        bridge_control_reference,
        bridge_state_reference,
        bridge_predictor_slope,
        bridge_remainder,
        bridge_preconditioner,
        context,
    )
    parent_relative = _rors_patch_tube_relative_to_bridge(
        rebuilt_parent, bridge_patch, overlap, context)
    child_relative = _rors_patch_tube_relative_to_bridge(
        rebuilt_child, bridge_patch, overlap, context)
    for state in eachindex(parent_relative)
        _rors_subset(parent_relative[state], bridge_patch.remainder_box[state]) ||
            throw(RORegularSheetCertificationRejected(
                :parent_tube_not_contained_in_bridge,
                "parent tube coordinate $state is not enclosed by the bridge tube on the overlap",
            ))
        _rors_subset(child_relative[state], bridge_patch.remainder_box[state]) ||
            throw(RORegularSheetCertificationRejected(
                :child_tube_not_contained_in_bridge,
                "child tube coordinate $state is not enclosed by the bridge tube on the overlap",
            ))
    end
    context.cancel_check()
    operation_count_big = context.operations - start_operations
    operation_count_big <= typemax(Int) || throw(
        RORegularSheetLimitExceeded(
            :interval_operations,
            operation_count_big,
            context.limits.max_interval_operations,
        ))
    operation_count = Int(operation_count_big)
    inherited_branch_identity_sha256 = rebuilt_parent.branch_identity_sha256
    return RORegularSheetBridgeCertificate(
        RO_REGULAR_SHEET_BRIDGE_VERSION,
        system.declaration_sha256,
        rebuilt_parent.certificate_sha256,
        rebuilt_child.certificate_sha256,
        bridge_patch,
        inherited_branch_identity_sha256,
        operation_count,
        true,
        true,
        true,
        RO_REGULAR_SHEET_BRIDGE_SCOPE,
        Val(:validated),
    )
end

"""
    certify_ro_regular_sheet_bridge(system, parent, child; ...)

Prove that the unique roots certified by `parent` and `child` are the same root
on a declared full-dimensional overlap. The bridge tube must contain both
entire patch tubes on that overlap; mere point proximity is insufficient.
"""
function certify_ro_regular_sheet_bridge(
    system::ROPolynomialEquilibriumSystem,
    parent::RORegularSheetPatchCertificate,
    child::RORegularSheetPatchCertificate;
    overlap_lower,
    overlap_upper,
    bridge_control_reference,
    bridge_state_reference,
    bridge_predictor_slope,
    bridge_remainder_lower,
    bridge_remainder_upper,
    bridge_preconditioner,
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    context = _RORSContext(system.limits, cancel_check)
    overlap = _rors_exact_box(
        overlap_lower,
        overlap_upper,
        control_count,
        "overlap",
        context,
    )
    control_reference = _rors_exact_vector(
        bridge_control_reference,
        control_count,
        "bridge_control_reference",
        context,
    )
    state_reference = _rors_exact_vector(
        bridge_state_reference,
        state_count,
        "bridge_state_reference",
        context,
    )
    predictor_slope = _rors_exact_matrix(
        bridge_predictor_slope,
        state_count,
        control_count,
        "bridge_predictor_slope",
        context,
    )
    remainder = _rors_exact_box(
        bridge_remainder_lower,
        bridge_remainder_upper,
        state_count,
        "bridge_remainder",
        context,
    )
    preconditioner = _rors_exact_matrix(
        bridge_preconditioner,
        state_count,
        state_count,
        "bridge_preconditioner",
        context,
    )
    return _rors_certify_bridge_exact(
        system,
        parent,
        child,
        overlap,
        control_reference,
        state_reference,
        predictor_slope,
        remainder,
        preconditioner,
        context,
    )
end

function replay_ro_regular_sheet_bridge(
    system::ROPolynomialEquilibriumSystem,
    parent::RORegularSheetPatchCertificate,
    child::RORegularSheetPatchCertificate,
    bridge::RORegularSheetBridgeCertificate;
    cancel_check=() -> nothing,
)
    bridge.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "regular-sheet bridge belongs to a different equilibrium system"))
    bridge.parent_patch_sha256 == parent.certificate_sha256 ||
        throw(ArgumentError("regular-sheet bridge parent identity mismatch"))
    bridge.child_patch_sha256 == child.certificate_sha256 ||
        throw(ArgumentError("regular-sheet bridge child identity mismatch"))
    context = _RORSContext(system.limits, cancel_check)
    bridge_patch = bridge.bridge_patch
    rebuilt = _rors_certify_bridge_exact(
        system,
        parent,
        child,
        collect(bridge_patch.control_box),
        collect(bridge_patch.control_reference),
        collect(bridge_patch.state_reference),
        _rors_exact_matrix_values(bridge_patch.predictor_slope),
        collect(bridge_patch.remainder_box),
        _rors_exact_matrix_values(bridge_patch.preconditioner),
        context,
    )
    rebuilt.certificate_sha256 == bridge.certificate_sha256 ||
        throw(ArgumentError(
            "regular-sheet bridge replay changed its certificate hash"))
    return rebuilt
end

function validate_ro_regular_sheet_bridge(
    system::ROPolynomialEquilibriumSystem,
    parent::RORegularSheetPatchCertificate,
    child::RORegularSheetPatchCertificate,
    bridge::RORegularSheetBridgeCertificate;
    cancel_check=() -> nothing,
)
    replay_ro_regular_sheet_bridge(
        system, parent, child, bridge; cancel_check=cancel_check)
    return true
end
