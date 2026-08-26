const RO_EXACT_REAL_FOURIER_SERIES_VERSION =
    "bne-ro-exact-real-fourier-series/v1.0.0"
const RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_VERSION =
    "bne-ro-exact-polynomial-periodic-fourier-residual-audit/v1.0.0"
const RO_EXACT_WEIGHTED_L1_FOURIER_NORM_VERSION =
    "conjugate-symmetric-rectangular-complex-weighted-l1/v1.0.0"
const RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_SCOPE =
    :one_exact_finite_support_periodic_parameterization_identity_only

struct ROPeriodicFourierLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROPeriodicFourierLimitExceeded)
    print(io, "periodic Fourier ", err.phase, " requested ",
        err.requested, ", exceeding limit=", err.limit)
end

struct ROPeriodicFourierRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROPeriodicFourierRejected)
    print(io, "periodic Fourier audit rejected (", err.reason,
        "): ", err.detail)
end

"""
Hard admission, convolution, exact-arithmetic, and payload limits.

`max_workspace_entries` bounds each individual dense Laurent-series buffer;
the separately enforced `max_series_entries` bounds each admitted state or
published residual population. `max_exact_operations` counts binary rational
arithmetic in residual construction plus both metric construction and the
inner structural reconstruction; canonical encoding is bounded in bytes
instead of being counted as exact arithmetic.
"""
struct ROPeriodicFourierLimits
    max_states::Int
    max_controls::Int
    max_input_half_bandwidth::Int
    max_output_half_bandwidth::Int
    max_series_entries::Int
    max_source_terms::Int
    max_total_degree::Int
    max_convolution_pairs::Int
    max_workspace_entries::Int
    max_canonical_payload_bytes::Int
    max_exact_operand_bits::Int
    max_exact_operations::Int

    function ROPeriodicFourierLimits(values::Vararg{Int,12})
        for (label, value) in zip((
            "max_states",
            "max_controls",
            "max_input_half_bandwidth",
            "max_output_half_bandwidth",
            "max_series_entries",
            "max_source_terms",
            "max_total_degree",
            "max_convolution_pairs",
            "max_workspace_entries",
            "max_canonical_payload_bytes",
            "max_exact_operand_bits",
            "max_exact_operations",
        ), values)
            value > 0 || throw(ArgumentError("$label must be positive"))
        end
        values[3] <= values[4] || throw(ArgumentError(
            "input half-bandwidth cannot exceed output half-bandwidth"))
        BigInt(values[9]) >= 2 * BigInt(values[4]) + 1 || throw(ArgumentError(
            "workspace limit must admit the configured output bandwidth"))
        values[11] >= 1076 || throw(ArgumentError(
            "max_exact_operand_bits must admit every finite Float64 dyadic"))
        return new(values...)
    end
end

function ROPeriodicFourierLimits(;
    max_states::Integer=16,
    max_controls::Integer=8,
    max_input_half_bandwidth::Integer=128,
    max_output_half_bandwidth::Integer=512,
    max_series_entries::Integer=16_384,
    max_source_terms::Integer=4_096,
    max_total_degree::Integer=32,
    max_convolution_pairs::Integer=20_000_000,
    max_workspace_entries::Integer=1_025,
    max_canonical_payload_bytes::Integer=16_777_216,
    max_exact_operand_bits::Integer=65_536,
    max_exact_operations::Integer=50_000_000,
)
    values = (
        max_states,
        max_controls,
        max_input_half_bandwidth,
        max_output_half_bandwidth,
        max_series_entries,
        max_source_terms,
        max_total_degree,
        max_convolution_pairs,
        max_workspace_entries,
        max_canonical_payload_bytes,
        max_exact_operand_bits,
        max_exact_operations,
    )
    all(value -> value isa Integer && !(value isa Bool), values) ||
        throw(ArgumentError("periodic Fourier limits must be integers"))
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("periodic Fourier limits must fit Int"))
    return ROPeriodicFourierLimits(Int.(values)...)
end

Base.:(==)(left::ROPeriodicFourierLimits,
    right::ROPeriodicFourierLimits) =
    all(index -> getfield(left, index) == getfield(right, index),
        1:fieldcount(ROPeriodicFourierLimits))
Base.isequal(left::ROPeriodicFourierLimits,
    right::ROPeriodicFourierLimits) = left == right

@inline function _ropf_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "periodic Fourier requested work must be nonnegative"))
    amount <= limit || throw(
        ROPeriodicFourierLimitExceeded(phase, amount, limit))
    return nothing
end

function _ropf_token_text_bytes(value)
    if value isa String
        return BigInt(ncodeunits(value))
    elseif value isa Symbol
        return BigInt(ncodeunits(String(value)))
    elseif value isa Bool
        return BigInt(value ? 4 : 5)
    elseif value isa Integer
        digits = ndigits(value; base=10)
        return BigInt(digits + (value < 0 ? 1 : 0))
    end
    throw(ArgumentError(
        "unsupported periodic Fourier canonical token $(typeof(value))"))
end

function _ropf_write_token(
    io::IOBuffer,
    value,
    limits::ROPeriodicFourierLimits,
)
    text_bytes = _ropf_token_text_bytes(value)
    token_bytes = BigInt(ndigits(text_bytes; base=10)) +
        text_bytes + 2
    _ropf_limit(:canonical_payload_bytes,
        BigInt(position(io)) + token_bytes,
        limits.max_canonical_payload_bytes)
    text = string(value)
    ncodeunits(text) == text_bytes || throw(ArgumentError(
        "periodic Fourier canonical token length changed during encoding"))
    print(io, ncodeunits(text), ':', text, ';')
    return nothing
end

function _ropf_write_exact(
    io::IOBuffer,
    value::_RORSExact,
    limits::ROPeriodicFourierLimits,
)
    _ropf_write_token(io, numerator(value), limits)
    _ropf_write_token(io, denominator(value), limits)
    return nothing
end

function _ropf_write_limits(
    io::IOBuffer,
    limits::ROPeriodicFourierLimits,
)
    for value in (
        limits.max_states,
        limits.max_controls,
        limits.max_input_half_bandwidth,
        limits.max_output_half_bandwidth,
        limits.max_series_entries,
        limits.max_source_terms,
        limits.max_total_degree,
        limits.max_convolution_pairs,
        limits.max_workspace_entries,
        limits.max_canonical_payload_bytes,
        limits.max_exact_operand_bits,
        limits.max_exact_operations,
    )
        _ropf_write_token(io, value, limits)
    end
    return nothing
end

@inline function _ropf_check_exact(
    value::_RORSExact,
    limits::ROPeriodicFourierLimits,
)
    bits = max(
        ndigits(numerator(value); base=2),
        ndigits(denominator(value); base=2),
    )
    _ropf_limit(:exact_operand_bits, bits,
        limits.max_exact_operand_bits)
    return value
end

@inline function _ropf_raw_exact_bits(value::Integer)
    return ndigits(value; base=2)
end

@inline function _ropf_raw_exact_bits(value::Rational)
    return max(
        ndigits(numerator(value); base=2),
        ndigits(denominator(value); base=2),
    )
end

function _ropf_exact_value(
    value,
    limits::ROPeriodicFourierLimits,
    label::String,
)
    value isa Bool && throw(ArgumentError("$label must not be Bool"))
    if value isa Integer || value isa Rational
        _ropf_limit(:exact_operand_bits, _ropf_raw_exact_bits(value),
            limits.max_exact_operand_bits)
    end
    exact = if value isa Bool
        throw(ArgumentError("$label must not be Bool"))
    elseif value isa Integer
        BigInt(value) // BigInt(1)
    elseif value isa Rational
        BigInt(numerator(value)) // BigInt(denominator(value))
    elseif value isa Float64
        isfinite(value) || throw(ArgumentError("$label must be finite"))
        _RORSExact(value == 0.0 ? 0.0 : value)
    else
        throw(ArgumentError(
            "$label must be Integer, Rational, or finite Float64"))
    end
    return _ropf_check_exact(exact, limits)
end

function _ropf_series_sha256(
    half_bandwidth::Int,
    real_parts::Tuple,
    imag_parts::Tuple,
    limits::ROPeriodicFourierLimits,
)
    io = IOBuffer()
    _ropf_write_token(io, RO_EXACT_REAL_FOURIER_SERIES_VERSION, limits)
    _ropf_write_token(io, half_bandwidth, limits)
    for values in (real_parts, imag_parts)
        _ropf_write_token(io, length(values), limits)
        for value in values
            _ropf_write_exact(io, value, limits)
        end
    end
    _ropf_write_limits(io, limits)
    return bytes2hex(SHA.sha256(take!(io)))
end

struct _ROPFSeriesValidatedToken end
const _ROPF_SERIES_VALIDATED_TOKEN = _ROPFSeriesValidatedToken()

"""
An exact real `2pi`-periodic scalar Fourier series.

The stored values are the full complex Fourier coefficients `c_k` for
`k=0:M`; negative modes are defined by `c_-k=conj(c_k)`. Thus `cos(theta)`
has `real_parts[2]=1/2`, while `sin(theta)` has
`imag_parts[2]=-1/2`. Trailing zero modes are removed canonically.
"""
struct ROExactRealFourierSeries
    version::String
    half_bandwidth::Int
    real_parts::Tuple
    imag_parts::Tuple
    limits::ROPeriodicFourierLimits
    series_sha256::String

    function ROExactRealFourierSeries(
        ::_ROPFSeriesValidatedToken,
        version::String,
        half_bandwidth::Int,
        real_parts::Tuple,
        imag_parts::Tuple,
        limits::ROPeriodicFourierLimits,
        series_sha256::String,
    )
        version == RO_EXACT_REAL_FOURIER_SERIES_VERSION ||
            throw(ArgumentError("exact Fourier-series version mismatch"))
        half_bandwidth >= 0 || throw(ArgumentError(
            "Fourier half-bandwidth must be nonnegative"))
        length(real_parts) == half_bandwidth + 1 ||
            throw(DimensionMismatch("real half-spectrum length mismatch"))
        length(imag_parts) == half_bandwidth + 1 ||
            throw(DimensionMismatch("imaginary half-spectrum length mismatch"))
        all(value -> value isa _RORSExact, real_parts) ||
            throw(ArgumentError("real half-spectrum must be exact rational"))
        all(value -> value isa _RORSExact, imag_parts) ||
            throw(ArgumentError("imaginary half-spectrum must be exact rational"))
        imag_parts[1] == 0 || throw(ArgumentError(
            "the zero Fourier mode of a real series must be real"))
        half_bandwidth > 0 &&
            real_parts[end] == 0 && imag_parts[end] == 0 &&
            throw(ArgumentError("Fourier series has a trailing zero mode"))
        _ropf_limit(:series_half_bandwidth, half_bandwidth,
            limits.max_output_half_bandwidth)
        _ropf_limit(:series_entries, half_bandwidth + 1,
            limits.max_series_entries)
        for values in (real_parts, imag_parts), value in values
            _ropf_check_exact(value, limits)
        end
        expected = _ropf_series_sha256(
            half_bandwidth, real_parts, imag_parts, limits)
        series_sha256 == expected || throw(ArgumentError(
            "exact Fourier-series hash mismatch"))
        return new(version, half_bandwidth, real_parts, imag_parts,
            limits, series_sha256)
    end
end

Base.:(==)(left::ROExactRealFourierSeries,
    right::ROExactRealFourierSeries) =
    all(index -> getfield(left, index) == getfield(right, index),
        1:fieldcount(ROExactRealFourierSeries))
Base.isequal(left::ROExactRealFourierSeries,
    right::ROExactRealFourierSeries) = left == right

function ROExactRealFourierSeries(
    real_parts,
    imag_parts;
    limits::ROPeriodicFourierLimits=ROPeriodicFourierLimits(),
)
    (real_parts isa AbstractVector || real_parts isa Tuple) ||
        throw(ArgumentError("real_parts must be ordered"))
    (imag_parts isa AbstractVector || imag_parts isa Tuple) ||
        throw(ArgumentError("imag_parts must be ordered"))
    length(real_parts) == length(imag_parts) ||
        throw(DimensionMismatch("real_parts and imag_parts must have equal length"))
    isempty(real_parts) && throw(ArgumentError(
        "a Fourier half-spectrum requires its zero mode"))
    _ropf_limit(:series_entries, length(real_parts),
        limits.max_series_entries)
    admitted_real = [_ropf_exact_value(value, limits,
        "real_parts[$position]")
        for (position, value) in enumerate(real_parts)]
    admitted_imag = [_ropf_exact_value(value, limits,
        "imag_parts[$position]")
        for (position, value) in enumerate(imag_parts)]
    admitted_imag[1] == 0 || throw(ArgumentError(
        "the zero Fourier mode of a real series must be real"))
    last_mode = length(admitted_real)
    while last_mode > 1 && admitted_real[last_mode] == 0 &&
            admitted_imag[last_mode] == 0
        last_mode -= 1
    end
    resize!(admitted_real, last_mode)
    resize!(admitted_imag, last_mode)
    half_bandwidth = last_mode - 1
    _ropf_limit(:series_half_bandwidth, half_bandwidth,
        limits.max_output_half_bandwidth)
    real_tuple = Tuple(admitted_real)
    imag_tuple = Tuple(admitted_imag)
    sha = _ropf_series_sha256(
        half_bandwidth, real_tuple, imag_tuple, limits)
    return ROExactRealFourierSeries(
        _ROPF_SERIES_VALIDATED_TOKEN,
        RO_EXACT_REAL_FOURIER_SERIES_VERSION,
        half_bandwidth,
        real_tuple,
        imag_tuple,
        limits,
        sha,
    )
end

function validate_ro_exact_real_fourier_series(
    series::ROExactRealFourierSeries,
)
    rebuilt = ROExactRealFourierSeries(
        series.real_parts,
        series.imag_parts;
        limits=series.limits,
    )
    rebuilt == series || throw(ArgumentError(
        "exact Fourier series does not reproduce"))
    return true
end

mutable struct _ROPFWorkContext
    limits::ROPeriodicFourierLimits
    exact_operations::BigInt
    convolution_pairs::BigInt
    cancel_check
end

_ROPFWorkContext(limits::ROPeriodicFourierLimits, cancel_check) =
    _ROPFWorkContext(limits, BigInt(0), BigInt(0), cancel_check)

@inline function _ropf_cancel(context::_ROPFWorkContext)
    context.cancel_check()
    return nothing
end

@inline function _ropf_tick!(
    context::_ROPFWorkContext,
    count::Integer=1,
)
    context.exact_operations += count
    _ropf_limit(:exact_operations, context.exact_operations,
        context.limits.max_exact_operations)
    if context.exact_operations % 256 == 0
        _ropf_cancel(context)
    end
    return nothing
end

@inline function _ropf_checked(
    context::_ROPFWorkContext,
    value::_RORSExact,
)
    return _ropf_check_exact(value, context.limits)
end

@inline function _ropf_preflight_exact_bits(
    context::_ROPFWorkContext,
    requested::Integer,
)
    _ropf_limit(:exact_operand_bits, requested,
        context.limits.max_exact_operand_bits)
    return nothing
end

@inline function _ropf_add(
    context::_ROPFWorkContext,
    left::_RORSExact,
    right::_RORSExact,
)
    _ropf_tick!(context)
    _ropf_preflight_exact_bits(
        context,
        BigInt(_ropf_raw_exact_bits(left)) +
            _ropf_raw_exact_bits(right) + 1,
    )
    return _ropf_checked(context, left + right)
end

@inline function _ropf_sub(
    context::_ROPFWorkContext,
    left::_RORSExact,
    right::_RORSExact,
)
    _ropf_tick!(context)
    _ropf_preflight_exact_bits(
        context,
        BigInt(_ropf_raw_exact_bits(left)) +
            _ropf_raw_exact_bits(right) + 1,
    )
    return _ropf_checked(context, left - right)
end

@inline function _ropf_mul(
    context::_ROPFWorkContext,
    left::_RORSExact,
    right::_RORSExact,
)
    _ropf_tick!(context)
    (iszero(left) || iszero(right)) &&
        return BigInt(0) // BigInt(1)
    _ropf_preflight_exact_bits(
        context,
        BigInt(_ropf_raw_exact_bits(left)) +
            _ropf_raw_exact_bits(right),
    )
    return _ropf_checked(context, left * right)
end

struct _ROPFComplex
    re::_RORSExact
    im::_RORSExact
end

const _ROPF_COMPLEX_ZERO = _ROPFComplex(BigInt(0)//BigInt(1),
    BigInt(0)//BigInt(1))
const _ROPF_COMPLEX_ONE = _ROPFComplex(BigInt(1)//BigInt(1),
    BigInt(0)//BigInt(1))

@inline function _ropf_complex_add(
    context::_ROPFWorkContext,
    left::_ROPFComplex,
    right::_ROPFComplex,
)
    return _ROPFComplex(
        _ropf_add(context, left.re, right.re),
        _ropf_add(context, left.im, right.im),
    )
end

@inline function _ropf_complex_sub(
    context::_ROPFWorkContext,
    left::_ROPFComplex,
    right::_ROPFComplex,
)
    return _ROPFComplex(
        _ropf_sub(context, left.re, right.re),
        _ropf_sub(context, left.im, right.im),
    )
end

@inline function _ropf_complex_mul(
    context::_ROPFWorkContext,
    left::_ROPFComplex,
    right::_ROPFComplex,
)
    real_value = _ropf_sub(
        context,
        _ropf_mul(context, left.re, right.re),
        _ropf_mul(context, left.im, right.im),
    )
    imag_value = _ropf_add(
        context,
        _ropf_mul(context, left.re, right.im),
        _ropf_mul(context, left.im, right.re),
    )
    return _ROPFComplex(real_value, imag_value)
end

@inline function _ropf_complex_scale(
    context::_ROPFWorkContext,
    value::_ROPFComplex,
    scalar::_RORSExact,
)
    return _ROPFComplex(
        _ropf_mul(context, value.re, scalar),
        _ropf_mul(context, value.im, scalar),
    )
end

@inline _ropf_complex_iszero(value::_ROPFComplex) =
    value.re == 0 && value.im == 0

struct _ROPFFullSeries
    half_bandwidth::Int
    coefficients::Vector{_ROPFComplex}
end

@inline function _ropf_mode_index(
    series::_ROPFFullSeries,
    mode::Int,
)
    -series.half_bandwidth <= mode <= series.half_bandwidth ||
        throw(BoundsError(series.coefficients, mode))
    return mode + series.half_bandwidth + 1
end

@inline function _ropf_coefficient(
    series::_ROPFFullSeries,
    mode::Int,
)
    return series.coefficients[_ropf_mode_index(series, mode)]
end

function _ropf_full_series(series::ROExactRealFourierSeries)
    bandwidth = series.half_bandwidth
    coefficients = fill(_ROPF_COMPLEX_ZERO, 2 * bandwidth + 1)
    coefficients[bandwidth + 1] =
        _ROPFComplex(series.real_parts[1], series.imag_parts[1])
    for mode in 1:bandwidth
        positive = _ROPFComplex(
            series.real_parts[mode + 1],
            series.imag_parts[mode + 1],
        )
        coefficients[bandwidth + mode + 1] = positive
        coefficients[bandwidth - mode + 1] =
            _ROPFComplex(positive.re, -positive.im)
    end
    return _ROPFFullSeries(bandwidth, coefficients)
end

function _ropf_constant_series(value::_RORSExact)
    return _ROPFFullSeries(0, [_ROPFComplex(value, BigInt(0)//BigInt(1))])
end

function _ropf_add_series(
    context::_ROPFWorkContext,
    left::_ROPFFullSeries,
    right::_ROPFFullSeries,
)
    bandwidth = max(left.half_bandwidth, right.half_bandwidth)
    _ropf_limit(:output_half_bandwidth, bandwidth,
        context.limits.max_output_half_bandwidth)
    _ropf_limit(:workspace_entries, 2 * BigInt(bandwidth) + 1,
        context.limits.max_workspace_entries)
    coefficients = fill(_ROPF_COMPLEX_ZERO, 2 * bandwidth + 1)
    for mode in -bandwidth:bandwidth
        left_value = abs(mode) <= left.half_bandwidth ?
            _ropf_coefficient(left, mode) : _ROPF_COMPLEX_ZERO
        right_value = abs(mode) <= right.half_bandwidth ?
            _ropf_coefficient(right, mode) : _ROPF_COMPLEX_ZERO
        coefficients[mode + bandwidth + 1] =
            _ropf_complex_add(context, left_value, right_value)
    end
    return _ROPFFullSeries(bandwidth, coefficients)
end

function _ropf_sub_series(
    context::_ROPFWorkContext,
    left::_ROPFFullSeries,
    right::_ROPFFullSeries,
)
    bandwidth = max(left.half_bandwidth, right.half_bandwidth)
    _ropf_limit(:output_half_bandwidth, bandwidth,
        context.limits.max_output_half_bandwidth)
    _ropf_limit(:workspace_entries, 2 * BigInt(bandwidth) + 1,
        context.limits.max_workspace_entries)
    coefficients = fill(_ROPF_COMPLEX_ZERO, 2 * bandwidth + 1)
    for mode in -bandwidth:bandwidth
        left_value = abs(mode) <= left.half_bandwidth ?
            _ropf_coefficient(left, mode) : _ROPF_COMPLEX_ZERO
        right_value = abs(mode) <= right.half_bandwidth ?
            _ropf_coefficient(right, mode) : _ROPF_COMPLEX_ZERO
        coefficients[mode + bandwidth + 1] =
            _ropf_complex_sub(context, left_value, right_value)
    end
    return _ROPFFullSeries(bandwidth, coefficients)
end

function _ropf_scale_series(
    context::_ROPFWorkContext,
    series::_ROPFFullSeries,
    scalar::_RORSExact,
)
    coefficients = Vector{_ROPFComplex}(undef,
        length(series.coefficients))
    for index in eachindex(series.coefficients)
        coefficients[index] = _ropf_complex_scale(
            context, series.coefficients[index], scalar)
    end
    return _ROPFFullSeries(series.half_bandwidth, coefficients)
end

function _ropf_convolve(
    context::_ROPFWorkContext,
    left::_ROPFFullSeries,
    right::_ROPFFullSeries,
)
    bandwidth_big = BigInt(left.half_bandwidth) + right.half_bandwidth
    _ropf_limit(:output_half_bandwidth, bandwidth_big,
        context.limits.max_output_half_bandwidth)
    _ropf_limit(:workspace_entries, 2 * bandwidth_big + 1,
        context.limits.max_workspace_entries)
    pair_count = (2 * BigInt(left.half_bandwidth) + 1) *
        (2 * BigInt(right.half_bandwidth) + 1)
    context.convolution_pairs += pair_count
    _ropf_limit(:convolution_pairs, context.convolution_pairs,
        context.limits.max_convolution_pairs)
    bandwidth = Int(bandwidth_big)
    coefficients = fill(_ROPF_COMPLEX_ZERO, 2 * bandwidth + 1)
    processed = 0
    for left_mode in -left.half_bandwidth:left.half_bandwidth
        left_value = _ropf_coefficient(left, left_mode)
        for right_mode in -right.half_bandwidth:right.half_bandwidth
            target_mode = left_mode + right_mode
            target_index = target_mode + bandwidth + 1
            product = _ropf_complex_mul(
                context, left_value, _ropf_coefficient(right, right_mode))
            coefficients[target_index] = _ropf_complex_add(
                context, coefficients[target_index], product)
            processed += 1
            processed % 256 == 0 && _ropf_cancel(context)
        end
    end
    return _ROPFFullSeries(bandwidth, coefficients)
end

function _ropf_derivative(
    context::_ROPFWorkContext,
    series::_ROPFFullSeries,
)
    coefficients = Vector{_ROPFComplex}(undef,
        length(series.coefficients))
    for mode in -series.half_bandwidth:series.half_bandwidth
        value = _ropf_coefficient(series, mode)
        mode_exact = BigInt(mode) // BigInt(1)
        # i*k*(re+i*im) = -k*im + i*k*re.
        coefficients[mode + series.half_bandwidth + 1] = _ROPFComplex(
            _ropf_mul(context, -mode_exact, value.im),
            _ropf_mul(context, mode_exact, value.re),
        )
    end
    return _ROPFFullSeries(series.half_bandwidth, coefficients)
end

function _ropf_public_series(
    series::_ROPFFullSeries,
    limits::ROPeriodicFourierLimits,
)
    last_mode = series.half_bandwidth
    while last_mode > 0 &&
            _ropf_complex_iszero(_ropf_coefficient(series, last_mode))
        last_mode -= 1
    end
    real_parts = Vector{_RORSExact}(undef, last_mode + 1)
    imag_parts = Vector{_RORSExact}(undef, last_mode + 1)
    for mode in 0:last_mode
        value = _ropf_coefficient(series, mode)
        real_parts[mode + 1] = value.re
        imag_parts[mode + 1] = value.im
    end
    return ROExactRealFourierSeries(
        real_parts, imag_parts; limits=limits)
end

function _ropf_source_admission_preflight(
    system::ROPolynomialEquilibriumSystem,
    limits::ROPeriodicFourierLimits,
)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    _ropf_limit(:states, state_count, limits.max_states)
    _ropf_limit(:controls, control_count, limits.max_controls)

    source_terms = sum(
        (BigInt(length(equation)) for equation in system.equations);
        init=BigInt(0),
    )
    _ropf_limit(:source_terms, source_terms, limits.max_source_terms)
    for equation in system.equations, term in equation
        total_degree = sum(BigInt, term.state_exponents) +
            sum(BigInt, term.control_exponents)
        _ropf_limit(:total_degree, total_degree,
            limits.max_total_degree)
    end
    return nothing
end

function _ropf_preflight(
    system::ROPolynomialEquilibriumSystem,
    state_series::Tuple,
    galerkin_half_bandwidth::Int,
    limits::ROPeriodicFourierLimits,
)
    state_count = length(system.state_names)
    control_count = length(system.control_names)
    _ropf_limit(:states, state_count, limits.max_states)
    _ropf_limit(:controls, control_count, limits.max_controls)
    length(state_series) == state_count || throw(DimensionMismatch(
        "state_series must follow the declared state order"))
    input_entries = BigInt(0)
    for (state_index, series) in enumerate(state_series)
        series isa ROExactRealFourierSeries || throw(ArgumentError(
            "state_series[$state_index] has the wrong type"))
        _ropf_limit(:input_half_bandwidth, series.half_bandwidth,
            limits.max_input_half_bandwidth)
        input_entries += series.half_bandwidth + 1
        _ropf_limit(:input_series_entries, input_entries,
            limits.max_series_entries)
    end
    galerkin_half_bandwidth >= 0 || throw(ArgumentError(
        "galerkin_half_bandwidth must be nonnegative"))
    _ropf_limit(:galerkin_half_bandwidth, galerkin_half_bandwidth,
        limits.max_output_half_bandwidth)
    input_half_bandwidth = maximum(
        (series.half_bandwidth for series in state_series);
        init=0,
    )
    galerkin_half_bandwidth >= input_half_bandwidth || throw(
        ROPeriodicFourierRejected(
            :candidate_support_outside_galerkin_head,
            "galerkin_half_bandwidth must retain every submitted state mode",
        ))

    source_terms = BigInt(0)
    planned_pairs = BigInt(0)
    predicted_bandwidths = Int[]
    sizehint!(predicted_bandwidths, state_count)
    for (equation_index, equation) in enumerate(system.equations)
        equation_bandwidth = BigInt(0)
        for term in equation
            source_terms += 1
            _ropf_limit(:source_terms, source_terms,
                limits.max_source_terms)
            total_degree = sum(BigInt, term.state_exponents) +
                sum(BigInt, term.control_exponents)
            _ropf_limit(:total_degree, total_degree,
                limits.max_total_degree)
            current_bandwidth = BigInt(0)
            for state_index in 1:state_count
                series_bandwidth = BigInt(
                    state_series[state_index].half_bandwidth)
                exponent = BigInt(term.state_exponents[state_index])
                if exponent > 0
                    pair_count = (2 * series_bandwidth + 1) * (
                        exponent * (2 * current_bandwidth + 1) +
                        series_bandwidth * exponent * (exponent - 1)
                    )
                    planned_pairs += pair_count
                    _ropf_limit(:convolution_pairs, planned_pairs,
                        limits.max_convolution_pairs)
                    current_bandwidth += exponent * series_bandwidth
                    _ropf_limit(:output_half_bandwidth, current_bandwidth,
                        limits.max_output_half_bandwidth)
                    _ropf_limit(:workspace_entries,
                        2 * current_bandwidth + 1,
                        limits.max_workspace_entries)
                end
            end
            equation_bandwidth = max(
                equation_bandwidth, current_bandwidth)
        end
        state_input_bandwidth = BigInt(
            state_series[equation_index].half_bandwidth)
        residual_bandwidth = max(
            equation_bandwidth, state_input_bandwidth)
        _ropf_limit(:output_half_bandwidth, residual_bandwidth,
            limits.max_output_half_bandwidth)
        _ropf_limit(:workspace_entries, 2 * residual_bandwidth + 1,
            limits.max_workspace_entries)
        push!(predicted_bandwidths, Int(residual_bandwidth))
    end
    predicted_entries = sum(
        (BigInt(value) + 1 for value in predicted_bandwidths);
        init=BigInt(0),
    )
    _ropf_limit(:output_series_entries, predicted_entries,
        limits.max_series_entries)
    return (
        planned_pairs=planned_pairs,
        predicted_bandwidths=Tuple(predicted_bandwidths),
        source_terms=source_terms,
    )
end

function _ropf_exact_pow(
    context::_ROPFWorkContext,
    base::_RORSExact,
    exponent::Int,
)
    exponent >= 0 || throw(ArgumentError(
        "exact powers require a nonnegative exponent"))
    result = BigInt(1) // BigInt(1)
    for _ in 1:exponent
        result = _ropf_mul(context, result, base)
    end
    return result
end

function _ropf_polynomial_component(
    context::_ROPFWorkContext,
    equation::Tuple,
    state_series::Tuple,
    controls::Tuple,
)
    result = _ropf_constant_series(BigInt(0)//BigInt(1))
    for (term_index, term) in enumerate(equation)
        _ropf_cancel(context)
        term_series = _ropf_constant_series(BigInt(1)//BigInt(1))
        for state_index in eachindex(state_series)
            factor = state_series[state_index]
            for _ in 1:term.state_exponents[state_index]
                term_series = _ropf_convolve(
                    context, term_series, factor)
            end
        end
        scalar = _ropf_exact_value(
            term.coefficient,
            context.limits,
            "polynomial coefficient $term_index",
        )
        for control_index in eachindex(controls)
            scalar = _ropf_mul(
                context,
                scalar,
                _ropf_exact_pow(
                    context,
                    controls[control_index],
                    term.control_exponents[control_index],
                ),
            )
        end
        term_series = _ropf_scale_series(context, term_series, scalar)
        result = _ropf_add_series(context, result, term_series)
    end
    return result
end

function _ropf_exact_residuals(
    context::_ROPFWorkContext,
    system::ROPolynomialEquilibriumSystem,
    state_series::Tuple,
    controls::Tuple,
    omega::_RORSExact,
)
    residuals = _ROPFFullSeries[]
    sizehint!(residuals, length(state_series))
    for equation_index in eachindex(system.equations)
        _ropf_cancel(context)
        vector_field = _ropf_polynomial_component(
            context,
            system.equations[equation_index],
            state_series,
            controls,
        )
        time_derivative = _ropf_scale_series(
            context,
            _ropf_derivative(context, state_series[equation_index]),
            omega,
        )
        push!(residuals, _ropf_sub_series(
            context, time_derivative, vector_field))
    end
    return Tuple(residuals)
end

function _ropf_weighted_l1_range(
    context::_ROPFWorkContext,
    series::ROExactRealFourierSeries,
    weight_nu::_RORSExact,
    first_mode::Int,
    last_mode::Int,
)
    first_mode >= 0 || throw(ArgumentError(
        "weighted-norm first mode must be nonnegative"))
    last_mode < first_mode && return BigInt(0)//BigInt(1)
    last_mode = min(last_mode, series.half_bandwidth)
    first_mode > last_mode && return BigInt(0)//BigInt(1)
    result = BigInt(0) // BigInt(1)
    if first_mode == 0
        result = _ropf_add(context, result, abs(series.real_parts[1]))
        first_mode = 1
    end
    for mode in first_mode:last_mode
        rectangle = _ropf_add(
            context,
            abs(series.real_parts[mode + 1]),
            abs(series.imag_parts[mode + 1]),
        )
        weighted = _ropf_mul(
            context,
            _ropf_exact_pow(context, weight_nu, mode),
            rectangle,
        )
        result = _ropf_add(
            context,
            result,
            _ropf_mul(context, BigInt(2)//BigInt(1), weighted),
        )
    end
    return result
end

@inline function _ropf_series_iszero(series::ROExactRealFourierSeries)
    return all(==(0), series.real_parts) && all(==(0), series.imag_parts)
end

function _ropf_nonconstant(state_series::Tuple)
    for series in state_series, mode in 1:series.half_bandwidth
        if series.real_parts[mode + 1] != 0 ||
                series.imag_parts[mode + 1] != 0
            return true
        end
    end
    return false
end

function _ropf_residual_metrics(
    context::_ROPFWorkContext,
    residual_series::Tuple,
    galerkin_half_bandwidth::Int,
    weight_nu::_RORSExact,
)
    head_norms = _RORSExact[]
    tail_norms = _RORSExact[]
    full_norms = _RORSExact[]
    sizehint!(head_norms, length(residual_series))
    sizehint!(tail_norms, length(residual_series))
    sizehint!(full_norms, length(residual_series))
    first_omitted = nothing
    for series in residual_series
        _ropf_cancel(context)
        head = _ropf_weighted_l1_range(
            context, series, weight_nu, 0, galerkin_half_bandwidth)
        tail = _ropf_weighted_l1_range(
            context,
            series,
            weight_nu,
            galerkin_half_bandwidth + 1,
            series.half_bandwidth,
        )
        total = _ropf_add(context, head, tail)
        push!(head_norms, head)
        push!(tail_norms, tail)
        push!(full_norms, total)
        for mode in (galerkin_half_bandwidth + 1):series.half_bandwidth
            if series.real_parts[mode + 1] != 0 ||
                    series.imag_parts[mode + 1] != 0
                first_omitted = first_omitted === nothing ? mode :
                    min(first_omitted, mode)
                break
            end
        end
    end
    return (
        head_norms=Tuple(head_norms),
        tail_norms=Tuple(tail_norms),
        full_norms=Tuple(full_norms),
        first_omitted=first_omitted,
    )
end

function _ropf_write_series(
    io::IOBuffer,
    series::ROExactRealFourierSeries,
    limits::ROPeriodicFourierLimits,
    cancel_check=() -> nothing,
)
    cancel_check()
    _ropf_write_token(io, series.version, limits)
    _ropf_write_token(io, series.half_bandwidth, limits)
    encoded = 0
    for values in (series.real_parts, series.imag_parts)
        _ropf_write_token(io, length(values), limits)
        for value in values
            _ropf_write_exact(io, value, limits)
            encoded += 1
            encoded % 256 == 0 && cancel_check()
        end
    end
    _ropf_write_token(io, series.series_sha256, limits)
    cancel_check()
    return nothing
end

function _ropf_audit_sha256(
    system_declaration_sha256::String,
    dynamics_binding_declaration_sha256::String,
    state_series::Tuple,
    controls::Tuple,
    omega::_RORSExact,
    galerkin_half_bandwidth::Int,
    weight_nu::_RORSExact,
    residual_series::Tuple,
    galerkin_head_residual_norms::Tuple,
    omitted_tail_residual_norms::Tuple,
    weighted_l1_residual_norms::Tuple,
    first_omitted_nonzero_mode::Union{Nothing,Int},
    planned_convolution_pair_count::Int,
    convolution_pair_count::Int,
    analysis_exact_operation_count::Int,
    limits::ROPeriodicFourierLimits,
    flags::Tuple,
    cancel_check=() -> nothing,
)
    cancel_check()
    io = IOBuffer()
    for value in (
        RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_VERSION,
        RO_EXACT_WEIGHTED_L1_FOURIER_NORM_VERSION,
        RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_SCOPE,
        system_declaration_sha256,
        dynamics_binding_declaration_sha256,
    )
        _ropf_write_token(io, value, limits)
    end
    _ropf_write_token(io, length(state_series), limits)
    for series in state_series
        _ropf_write_series(io, series, limits, cancel_check)
    end
    _ropf_write_token(io, length(controls), limits)
    for (position, value) in enumerate(controls)
        _ropf_write_exact(io, value, limits)
        position % 256 == 0 && cancel_check()
    end
    _ropf_write_exact(io, omega, limits)
    _ropf_write_token(io, galerkin_half_bandwidth, limits)
    _ropf_write_exact(io, weight_nu, limits)
    _ropf_write_token(io, length(residual_series), limits)
    for series in residual_series
        _ropf_write_series(io, series, limits, cancel_check)
    end
    for values in (
        galerkin_head_residual_norms,
        omitted_tail_residual_norms,
        weighted_l1_residual_norms,
    )
        _ropf_write_token(io, length(values), limits)
        for (position, value) in enumerate(values)
            _ropf_write_exact(io, value, limits)
            position % 256 == 0 && cancel_check()
        end
    end
    _ropf_write_token(io, first_omitted_nonzero_mode !== nothing, limits)
    first_omitted_nonzero_mode !== nothing &&
        _ropf_write_token(io, first_omitted_nonzero_mode, limits)
    for value in (
        planned_convolution_pair_count,
        convolution_pair_count,
        analysis_exact_operation_count,
    )
        _ropf_write_token(io, value, limits)
    end
    _ropf_write_limits(io, limits)
    for flag in flags
        _ropf_write_token(io, flag, limits)
    end
    cancel_check()
    result = bytes2hex(SHA.sha256(take!(io)))
    cancel_check()
    return result
end

struct _ROPFAuditValidatedToken end
const _ROPF_AUDIT_VALIDATED_TOKEN = _ROPFAuditValidatedToken()

"""
Source-bound exact audit of one finite Fourier parameterization.

The audit evaluates `omega*d_theta(x)-F(x,u)` through every mode generated by
the declared polynomial source. A zero Galerkin head with a nonzero omitted
mode is reported as a failed full ODE identity. If the complete generated
Laurent-polynomial residual is exactly zero, positive frequency and a nonzero
harmonic prove one exact ODE periodic parameterization. This exceptional exact
identity proves no nearby orbit, infinite-tail/radii theorem, local branch,
amplitude interval, Hopf-event incidence, uniqueness, Floquet spectrum, or
population completeness; it is a pre-c2b proof-kernel foundation.

The content hash is not independent authority. Every consumer must call the
source-bound validator before using a claim; validation also fails closed if
low-level mutation has made any nested exact value inconsistent with its hash.
"""
struct ROExactPolynomialPeriodicFourierAudit
    version::String
    weighted_norm_version::String
    evidence_scope::Symbol
    system_declaration_sha256::String
    dynamics_binding_declaration_sha256::String
    state_series::Tuple
    controls::Tuple
    omega::_RORSExact
    galerkin_half_bandwidth::Int
    weight_nu::_RORSExact
    residual_series::Tuple
    galerkin_head_residual_norms::Tuple
    omitted_tail_residual_norms::Tuple
    weighted_l1_residual_norms::Tuple
    first_omitted_nonzero_mode::Union{Nothing,Int}
    planned_convolution_pair_count::Int
    convolution_pair_count::Int
    analysis_exact_operation_count::Int
    limits::ROPeriodicFourierLimits
    real_fourier_symmetry_by_construction::Bool
    positive_angular_frequency::Bool
    nonconstant_parameterization::Bool
    galerkin_head_residual_exactly_zero::Bool
    omitted_residual_tail_exactly_zero::Bool
    full_residual_exactly_zero::Bool
    single_exact_ode_periodic_parameterization_certified::Bool
    explicit_periodic_orbit_enclosure_certified::Bool
    state_positivity_certified::Bool
    minimal_period_certified::Bool
    infinite_fourier_tail_radii_certified::Bool
    local_uniqueness_inside_declared_fourier_tube::Bool
    validated_periodic_orbit_branch_certified::Bool
    quantitative_amplitude_coverage_certified::Bool
    constructive_hopf_event_incidence_certified::Bool
    floquet_spectrum_certified::Bool
    full_state_periodic_orbit_stability_certified::Bool
    periodic_orbit_population_complete::Bool
    stable_periodic_orbit_population_complete::Bool
    multi_control_hopf_sheet_certified::Bool
    native_residuals_certified::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    certificate_sha256::String

    function ROExactPolynomialPeriodicFourierAudit(
        ::_ROPFAuditValidatedToken,
        version::String,
        weighted_norm_version::String,
        evidence_scope::Symbol,
        system_declaration_sha256::String,
        dynamics_binding_declaration_sha256::String,
        state_series::Tuple,
        controls::Tuple,
        omega::_RORSExact,
        galerkin_half_bandwidth::Int,
        weight_nu::_RORSExact,
        residual_series::Tuple,
        galerkin_head_residual_norms::Tuple,
        omitted_tail_residual_norms::Tuple,
        weighted_l1_residual_norms::Tuple,
        first_omitted_nonzero_mode::Union{Nothing,Int},
        planned_convolution_pair_count::Int,
        convolution_pair_count::Int,
        analysis_exact_operation_count::Int,
        limits::ROPeriodicFourierLimits,
        flags::NTuple{23,Bool},
        certificate_sha256::String,
        cancel_check=() -> nothing,
    )
        cancel_check()
        version == RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_VERSION ||
            throw(ArgumentError("periodic Fourier audit version mismatch"))
        weighted_norm_version == RO_EXACT_WEIGHTED_L1_FOURIER_NORM_VERSION ||
            throw(ArgumentError("periodic Fourier norm version mismatch"))
        evidence_scope == RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_SCOPE ||
            throw(ArgumentError("periodic Fourier evidence scope mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        _rors_validate_sha256(dynamics_binding_declaration_sha256,
            "dynamics_binding_declaration_sha256")
        _ropf_check_exact(omega, limits)
        _ropf_check_exact(weight_nu, limits)
        omega > 0 || throw(ArgumentError("omega must be positive"))
        weight_nu > 1 || throw(ArgumentError("weight_nu must exceed one"))
        galerkin_half_bandwidth >= 0 || throw(ArgumentError(
            "galerkin_half_bandwidth must be nonnegative"))
        _ropf_limit(:galerkin_half_bandwidth, galerkin_half_bandwidth,
            limits.max_output_half_bandwidth)
        planned_convolution_pair_count >= 0 || throw(ArgumentError(
            "planned convolution count must be nonnegative"))
        convolution_pair_count == planned_convolution_pair_count ||
            throw(ArgumentError("convolution work differs from its plan"))
        analysis_exact_operation_count >= 0 || throw(ArgumentError(
            "exact operation count must be nonnegative"))
        _ropf_limit(:convolution_pairs, planned_convolution_pair_count,
            limits.max_convolution_pairs)
        _ropf_limit(:exact_operations, analysis_exact_operation_count,
            limits.max_exact_operations)
        length(residual_series) == length(state_series) ||
            throw(DimensionMismatch("residual/state series count mismatch"))
        _ropf_limit(:states, length(state_series), limits.max_states)
        _ropf_limit(:controls, length(controls), limits.max_controls)

        input_entries = BigInt(0)
        for (position, series) in enumerate(state_series)
            series isa ROExactRealFourierSeries || throw(ArgumentError(
                "state_series[$position] has the wrong type"))
            series.limits == limits || throw(ArgumentError(
                "state Fourier-series limits differ from audit limits"))
            validate_ro_exact_real_fourier_series(series)
            _ropf_limit(:input_half_bandwidth, series.half_bandwidth,
                limits.max_input_half_bandwidth)
            series.half_bandwidth <= galerkin_half_bandwidth || throw(
                ROPeriodicFourierRejected(
                    :candidate_support_outside_galerkin_head,
                    "the Galerkin head must retain every submitted state mode",
                ))
            input_entries += series.half_bandwidth + 1
            _ropf_limit(:input_series_entries, input_entries,
                limits.max_series_entries)
        end
        for (position, value) in enumerate(controls)
            value isa _RORSExact || throw(ArgumentError(
                "controls[$position] must be exact rational"))
            _ropf_check_exact(value, limits)
        end

        output_entries = BigInt(0)
        for (position, series) in enumerate(residual_series)
            series isa ROExactRealFourierSeries || throw(ArgumentError(
                "residual_series[$position] has the wrong type"))
            series.limits == limits || throw(ArgumentError(
                "residual Fourier-series limits differ from audit limits"))
            validate_ro_exact_real_fourier_series(series)
            _ropf_limit(:output_half_bandwidth, series.half_bandwidth,
                limits.max_output_half_bandwidth)
            _ropf_limit(:workspace_entries,
                2 * BigInt(series.half_bandwidth) + 1,
                limits.max_workspace_entries)
            output_entries += series.half_bandwidth + 1
            _ropf_limit(:output_series_entries, output_entries,
                limits.max_series_entries)
        end
        for values in (
            galerkin_head_residual_norms,
            omitted_tail_residual_norms,
            weighted_l1_residual_norms,
        )
            length(values) == length(state_series) ||
                throw(DimensionMismatch("residual norm count mismatch"))
            for value in values
                value isa _RORSExact || throw(ArgumentError(
                    "residual norms must be exact rational"))
                value >= 0 || throw(ArgumentError(
                    "residual norms must be nonnegative"))
                _ropf_check_exact(value, limits)
            end
        end
        metric_context = _ROPFWorkContext(limits, cancel_check)
        metrics = _ropf_residual_metrics(
            metric_context,
            residual_series,
            galerkin_half_bandwidth,
            weight_nu,
        )
        galerkin_head_residual_norms == metrics.head_norms ||
            throw(ArgumentError("Galerkin-head residual norms are inconsistent"))
        omitted_tail_residual_norms == metrics.tail_norms ||
            throw(ArgumentError("omitted-tail residual norms are inconsistent"))
        weighted_l1_residual_norms == metrics.full_norms ||
            throw(ArgumentError("full residual norms are inconsistent"))
        first_omitted_nonzero_mode == metrics.first_omitted ||
            throw(ArgumentError("first omitted residual mode is inconsistent"))
        analysis_exact_operation_count >= metric_context.exact_operations ||
            throw(ArgumentError(
                "analysis operation count is smaller than metric reconstruction"))
        nonconstant = _ropf_nonconstant(state_series)
        head_zero = all(==(0), metrics.head_norms)
        tail_zero = all(==(0), metrics.tail_norms)
        expected_derived = (
            true,
            true,
            nonconstant,
            head_zero,
            tail_zero,
            head_zero && tail_zero,
            nonconstant && head_zero && tail_zero,
        )
        flags[1:7] == expected_derived || throw(ArgumentError(
            "periodic Fourier derived flags are inconsistent"))
        all(!, flags[8:end]) || throw(ArgumentError(
            "exact identity audit cannot publish branch or stability claims"))
        expected_hash = _ropf_audit_sha256(
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            state_series,
            controls,
            omega,
            galerkin_half_bandwidth,
            weight_nu,
            residual_series,
            galerkin_head_residual_norms,
            omitted_tail_residual_norms,
            weighted_l1_residual_norms,
            first_omitted_nonzero_mode,
            planned_convolution_pair_count,
            convolution_pair_count,
            analysis_exact_operation_count,
            limits,
            flags,
            cancel_check,
        )
        certificate_sha256 == expected_hash || throw(ArgumentError(
            "periodic Fourier audit hash mismatch"))
        cancel_check()
        artifact = new(
            version,
            weighted_norm_version,
            evidence_scope,
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            state_series,
            controls,
            omega,
            galerkin_half_bandwidth,
            weight_nu,
            residual_series,
            galerkin_head_residual_norms,
            omitted_tail_residual_norms,
            weighted_l1_residual_norms,
            first_omitted_nonzero_mode,
            planned_convolution_pair_count,
            convolution_pair_count,
            analysis_exact_operation_count,
            limits,
            flags...,
            certificate_sha256,
        )
        cancel_check()
        return artifact
    end
end

Base.:(==)(left::ROExactPolynomialPeriodicFourierAudit,
    right::ROExactPolynomialPeriodicFourierAudit) =
    all(index -> getfield(left, index) == getfield(right, index),
        1:fieldcount(ROExactPolynomialPeriodicFourierAudit))
Base.isequal(left::ROExactPolynomialPeriodicFourierAudit,
    right::ROExactPolynomialPeriodicFourierAudit) = left == right

function audit_ro_exact_polynomial_periodic_fourier_residual(
    system::ROPolynomialEquilibriumSystem,
    dynamics::ROPolynomialDynamicsBinding,
    state_series,
    control_values,
    angular_frequency;
    galerkin_half_bandwidth,
    weight_nu=BigInt(2)//BigInt(1),
    limits::ROPeriodicFourierLimits=ROPeriodicFourierLimits(),
    cancel_check=() -> nothing,
)
    (state_series isa AbstractVector || state_series isa Tuple) ||
        throw(ArgumentError("state_series must be ordered"))
    (control_values isa AbstractVector || control_values isa Tuple) ||
        throw(ArgumentError("control_values must be ordered"))
    galerkin_half_bandwidth isa Integer &&
        !(galerkin_half_bandwidth isa Bool) || throw(ArgumentError(
        "galerkin_half_bandwidth must be an integer"))
    typemin(Int) <= galerkin_half_bandwidth <= typemax(Int) ||
        throw(ArgumentError("galerkin_half_bandwidth must fit Int"))
    cutoff = Int(galerkin_half_bandwidth)
    cutoff >= 0 || throw(ArgumentError(
        "galerkin_half_bandwidth must be nonnegative"))
    _ropf_limit(:galerkin_half_bandwidth, cutoff,
        limits.max_output_half_bandwidth)

    state_count = length(system.state_names)
    control_count = length(system.control_names)
    _ropf_limit(:states, state_count, limits.max_states)
    _ropf_limit(:controls, control_count, limits.max_controls)
    length(state_series) == state_count ||
        throw(DimensionMismatch(
            "state_series must follow the declared state order"))
    length(control_values) == control_count ||
        throw(DimensionMismatch(
            "control_values must follow the declared control order"))
    preflight_series = Tuple(state_series)
    preflight_controls = Tuple(control_values)
    total_input_entries = sum(
        (begin
            series isa ROExactRealFourierSeries || throw(ArgumentError(
                "state_series[$position] has the wrong type"))
            _ropf_limit(:input_half_bandwidth, series.half_bandwidth,
                limits.max_input_half_bandwidth)
            series.half_bandwidth <= cutoff || throw(
                ROPeriodicFourierRejected(
                    :candidate_support_outside_galerkin_head,
                    "the Galerkin head must retain every submitted state mode",
                ))
            for values in (series.real_parts, series.imag_parts), value in values
                _ropf_check_exact(value, limits)
            end
            BigInt(series.half_bandwidth) + 1
        end for (position, series) in enumerate(preflight_series));
        init=BigInt(0),
    )
    _ropf_limit(:input_series_entries, total_input_entries,
        limits.max_series_entries)

    _ropf_source_admission_preflight(system, limits)
    work_plan = _ropf_preflight(
        system, preflight_series, cutoff, limits)
    cancel_check()
    validate_ro_polynomial_equilibrium_system(system)
    cancel_check()
    validate_ro_polynomial_dynamics_binding(system, dynamics)
    admitted_series_values = ROExactRealFourierSeries[]
    sizehint!(admitted_series_values, state_count)
    for series in preflight_series
        cancel_check()
        validate_ro_exact_real_fourier_series(series)
        push!(admitted_series_values, ROExactRealFourierSeries(
            series.real_parts,
            series.imag_parts;
            limits=limits,
        ))
    end
    admitted_series = Tuple(admitted_series_values)

    cancel_check()
    admitted_controls = Tuple(
        _ropf_exact_value(value, limits, "control_values[$position]")
        for (position, value) in enumerate(preflight_controls)
    )
    omega = _ropf_exact_value(
        angular_frequency, limits, "angular_frequency")
    omega > 0 || throw(ArgumentError(
        "angular_frequency must be positive"))
    nu = _ropf_exact_value(weight_nu, limits, "weight_nu")
    nu > 1 || throw(ArgumentError("weight_nu must exceed one"))

    cancel_check()
    context = _ROPFWorkContext(limits, cancel_check)
    full_state_series = Tuple(
        _ropf_full_series(series) for series in admitted_series)
    full_residuals = _ropf_exact_residuals(
        context,
        system,
        full_state_series,
        admitted_controls,
        omega,
    )
    context.convolution_pairs == work_plan.planned_pairs || throw(
        ROPeriodicFourierRejected(
            :convolution_plan_mismatch,
            "runtime convolution work differs from static preflight",
        ))
    residual_series = Tuple(
        _ropf_public_series(series, limits) for series in full_residuals)
    published_entries = sum(
        (BigInt(series.half_bandwidth) + 1
            for series in residual_series);
        init=BigInt(0),
    )
    _ropf_limit(:output_series_entries, published_entries,
        limits.max_series_entries)

    metric_operation_start = context.exact_operations
    metrics = _ropf_residual_metrics(
        context, residual_series, cutoff, nu)
    metric_operation_count =
        context.exact_operations - metric_operation_start
    head_tuple = metrics.head_norms
    tail_tuple = metrics.tail_norms
    full_tuple = metrics.full_norms
    first_omitted = metrics.first_omitted
    head_zero = all(==(0), head_tuple)
    tail_zero = all(==(0), tail_tuple)
    full_zero = head_zero && tail_zero
    nonconstant = _ropf_nonconstant(admitted_series)
    single_identity = nonconstant && full_zero
    planned_pairs = Int(work_plan.planned_pairs)
    actual_pairs = Int(context.convolution_pairs)
    cumulative_exact_operations =
        context.exact_operations + metric_operation_count
    _ropf_limit(:exact_operations, cumulative_exact_operations,
        limits.max_exact_operations)
    exact_operations = Int(cumulative_exact_operations)
    flags = (
        true,              # real Fourier symmetry by construction
        true,              # positive angular frequency
        nonconstant,
        head_zero,
        tail_zero,
        full_zero,
        single_identity,
        false,             # explicit orbit enclosure
        false,             # state positivity
        false,             # minimal period
        false,             # infinite-tail/radii proof
        false,             # local tube uniqueness
        false,             # validated branch
        false,             # quantitative amplitude coverage
        false,             # constructive Hopf incidence
        false,             # Floquet spectrum
        false,             # full-state orbit stability
        false,             # periodic-orbit population completeness
        false,             # stable-orbit population completeness
        false,             # multi-control Hopf sheet
        false,             # native residuals
        false,             # global continuation
        false,             # true hysteresis
    )
    certificate_sha256 = _ropf_audit_sha256(
        system.declaration_sha256,
        dynamics.declaration_sha256,
        admitted_series,
        admitted_controls,
        omega,
        cutoff,
        nu,
        residual_series,
        head_tuple,
        tail_tuple,
        full_tuple,
        first_omitted,
        planned_pairs,
        actual_pairs,
        exact_operations,
        limits,
        flags,
        cancel_check,
    )
    cancel_check()
    artifact = ROExactPolynomialPeriodicFourierAudit(
        _ROPF_AUDIT_VALIDATED_TOKEN,
        RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_VERSION,
        RO_EXACT_WEIGHTED_L1_FOURIER_NORM_VERSION,
        RO_EXACT_POLYNOMIAL_PERIODIC_FOURIER_AUDIT_SCOPE,
        system.declaration_sha256,
        dynamics.declaration_sha256,
        admitted_series,
        admitted_controls,
        omega,
        cutoff,
        nu,
        residual_series,
        head_tuple,
        tail_tuple,
        full_tuple,
        first_omitted,
        planned_pairs,
        actual_pairs,
        exact_operations,
        limits,
        flags,
        certificate_sha256,
        cancel_check,
    )
    cancel_check()
    return artifact
end

function replay_ro_exact_polynomial_periodic_fourier_residual(
    system::ROPolynomialEquilibriumSystem,
    dynamics::ROPolynomialDynamicsBinding,
    audit::ROExactPolynomialPeriodicFourierAudit;
    cancel_check=() -> nothing,
)
    audit.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "periodic Fourier audit belongs to a different system"))
    audit.dynamics_binding_declaration_sha256 ==
        dynamics.declaration_sha256 || throw(ArgumentError(
        "periodic Fourier audit belongs to a different dynamics binding"))
    return audit_ro_exact_polynomial_periodic_fourier_residual(
        system,
        dynamics,
        audit.state_series,
        audit.controls,
        audit.omega;
        galerkin_half_bandwidth=audit.galerkin_half_bandwidth,
        weight_nu=audit.weight_nu,
        limits=audit.limits,
        cancel_check=cancel_check,
    )
end

function validate_ro_exact_polynomial_periodic_fourier_residual(
    system::ROPolynomialEquilibriumSystem,
    dynamics::ROPolynomialDynamicsBinding,
    audit::ROExactPolynomialPeriodicFourierAudit;
    cancel_check=() -> nothing,
)
    audit.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "periodic Fourier audit belongs to a different system"))
    audit.dynamics_binding_declaration_sha256 ==
        dynamics.declaration_sha256 || throw(ArgumentError(
        "periodic Fourier audit belongs to a different dynamics binding"))
    rebuilt = replay_ro_exact_polynomial_periodic_fourier_residual(
        system, dynamics, audit; cancel_check=cancel_check)
    rebuilt == audit || throw(ArgumentError(
        "periodic Fourier audit does not reproduce"))
    return true
end
