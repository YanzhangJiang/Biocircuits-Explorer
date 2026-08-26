const RO_HOPF_LYAPUNOV_SEED_VERSION =
    "bne-ro-hopf-lyapunov-seed/v1.0.0"
const RO_FIRST_LYAPUNOV_HOPF_EVENT_VERSION =
    "bne-ro-first-lyapunov-hopf-event/v1.0.0"
const RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_VERSION =
    "bne-ro-complete-nondegenerate-hopf-census/v1.0.0"
const RO_FIRST_LYAPUNOV_FORMULA_VERSION =
    "kuznetsov-first-lyapunov-coefficient/v1.0.0"
const RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION =
    "bordered-right-anchor-hermitian-adjoint-unit-q/v1.0.0"
const RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_SCOPE =
    :complete_nondegenerate_local_hopf_lift_of_replayed_parent_census
const _ROHL_SQRT_BISECTION_STEPS = 128

struct ROHopfLyapunovLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROHopfLyapunovLimitExceeded)
    print(
        io,
        "Hopf-Lyapunov limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct ROHopfLyapunovRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROHopfLyapunovRejected)
    print(
        io,
        "Hopf-Lyapunov certification rejected (",
        err.reason,
        "): ",
        err.detail,
    )
end

@inline function _rohl_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "Hopf-Lyapunov requested work must be nonnegative"))
    amount <= limit || throw(
        ROHopfLyapunovLimitExceeded(phase, amount, limit))
    return nothing
end

"""Hard population, tensor, realification, and exact-operation limits for P8s1c1."""
struct ROHopfLyapunovLimits
    max_events::Int
    max_realified_linear_dimension::Int
    max_derivative_tensor_entries::Int
    max_preconditioner_entries::Int
    max_sqrt_bisection_steps::Int
    max_parent_replay_interval_operations::Int
    max_analysis_interval_operations::Int

    function ROHopfLyapunovLimits(
        max_events::Int,
        max_realified_linear_dimension::Int,
        max_derivative_tensor_entries::Int,
        max_preconditioner_entries::Int,
        max_sqrt_bisection_steps::Int,
        max_parent_replay_interval_operations::Int,
        max_analysis_interval_operations::Int,
    )
        max_events >= 0 || throw(ArgumentError(
            "max_events must be nonnegative"))
        for (label, value) in (
            ("max_realified_linear_dimension",
                max_realified_linear_dimension),
            ("max_derivative_tensor_entries",
                max_derivative_tensor_entries),
            ("max_preconditioner_entries", max_preconditioner_entries),
            ("max_sqrt_bisection_steps", max_sqrt_bisection_steps),
            ("max_parent_replay_interval_operations",
                max_parent_replay_interval_operations),
            ("max_analysis_interval_operations",
                max_analysis_interval_operations),
        )
            value > 0 || throw(ArgumentError("$label must be positive"))
        end
        max_parent_replay_interval_operations <=
            max_analysis_interval_operations || throw(ArgumentError(
            "parent replay operations cannot exceed the total analysis cap"))
        return new(
            max_events,
            max_realified_linear_dimension,
            max_derivative_tensor_entries,
            max_preconditioner_entries,
            max_sqrt_bisection_steps,
            max_parent_replay_interval_operations,
            max_analysis_interval_operations,
        )
    end
end

function ROHopfLyapunovLimits(;
    max_events::Integer=256,
    max_realified_linear_dimension::Integer=18,
    max_derivative_tensor_entries::Integer=8_192,
    max_preconditioner_entries::Integer=200_000,
    max_sqrt_bisection_steps::Integer=_ROHL_SQRT_BISECTION_STEPS,
    max_parent_replay_interval_operations::Integer=2_000_000,
    max_analysis_interval_operations::Integer=6_000_000,
)
    values = (
        max_events,
        max_realified_linear_dimension,
        max_derivative_tensor_entries,
        max_preconditioner_entries,
        max_sqrt_bisection_steps,
        max_parent_replay_interval_operations,
        max_analysis_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("Hopf-Lyapunov limits must fit Int"))
    return ROHopfLyapunovLimits(Int.(values)...)
end

function _rohl_write_limits(io::IO, limits::ROHopfLyapunovLimits)
    for value in (
        limits.max_events,
        limits.max_realified_linear_dimension,
        limits.max_derivative_tensor_entries,
        limits.max_preconditioner_entries,
        limits.max_sqrt_bisection_steps,
        limits.max_parent_replay_interval_operations,
        limits.max_analysis_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

struct _ROHLValidatedToken end
const _ROHL_VALIDATED_TOKEN = _ROHLValidatedToken()

function _rohl_seed_sha256(
    event_certificate_sha256::String,
    state_count::Int,
    anchor_state_index::Int,
    right_bordered_center::Tuple,
    right_bordered_remainder_box::Tuple,
    right_bordered_preconditioner::ROExactMatrix,
    adjoint_bordered_center::Tuple,
    adjoint_bordered_remainder_box::Tuple,
    adjoint_bordered_preconditioner::ROExactMatrix,
    zero_resolvent_center::Tuple,
    zero_resolvent_remainder_box::Tuple,
    zero_resolvent_preconditioner::ROExactMatrix,
    second_harmonic_center::Tuple,
    second_harmonic_remainder_box::Tuple,
    second_harmonic_preconditioner::ROExactMatrix,
)
    io = IOBuffer()
    _rors_write_token(io, RO_HOPF_LYAPUNOV_SEED_VERSION)
    _rors_write_token(io, event_certificate_sha256)
    _rors_write_token(io, state_count)
    _rors_write_token(io, anchor_state_index)
    for (center, box, preconditioner) in (
        (right_bordered_center, right_bordered_remainder_box,
            right_bordered_preconditioner),
        (adjoint_bordered_center, adjoint_bordered_remainder_box,
            adjoint_bordered_preconditioner),
        (zero_resolvent_center, zero_resolvent_remainder_box,
            zero_resolvent_preconditioner),
        (second_harmonic_center, second_harmonic_remainder_box,
            second_harmonic_preconditioner),
    )
        _rors_write_exact_vector(io, center)
        _rors_write_interval_vector(io, box)
        _rors_write_exact_matrix(io, preconditioner)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Exact seed data for the four strict linear Krawczyk proofs used by P8s1c1.

The right and adjoint systems are full complex bordered systems after the
canonical `[real; imaginary]` realification. The seed is bound to one parent
P8s1c0 event hash, but it has no scientific authority without replay of the
complete parent census.
"""
struct ROHopfLyapunovSeed
    version::String
    event_certificate_sha256::String
    state_count::Int
    anchor_state_index::Int
    right_bordered_center::Tuple
    right_bordered_remainder_box::Tuple
    right_bordered_preconditioner::ROExactMatrix
    adjoint_bordered_center::Tuple
    adjoint_bordered_remainder_box::Tuple
    adjoint_bordered_preconditioner::ROExactMatrix
    zero_resolvent_center::Tuple
    zero_resolvent_remainder_box::Tuple
    zero_resolvent_preconditioner::ROExactMatrix
    second_harmonic_center::Tuple
    second_harmonic_remainder_box::Tuple
    second_harmonic_preconditioner::ROExactMatrix
    seed_sha256::String

    function ROHopfLyapunovSeed(
        ::_ROHLValidatedToken,
        version::String,
        event_certificate_sha256::String,
        state_count::Int,
        anchor_state_index::Int,
        right_bordered_center::Tuple,
        right_bordered_remainder_box::Tuple,
        right_bordered_preconditioner::ROExactMatrix,
        adjoint_bordered_center::Tuple,
        adjoint_bordered_remainder_box::Tuple,
        adjoint_bordered_preconditioner::ROExactMatrix,
        zero_resolvent_center::Tuple,
        zero_resolvent_remainder_box::Tuple,
        zero_resolvent_preconditioner::ROExactMatrix,
        second_harmonic_center::Tuple,
        second_harmonic_remainder_box::Tuple,
        second_harmonic_preconditioner::ROExactMatrix,
        seed_sha256::String,
    )
        version == RO_HOPF_LYAPUNOV_SEED_VERSION ||
            throw(ArgumentError("Hopf-Lyapunov seed version mismatch"))
        _rors_validate_sha256(
            event_certificate_sha256, "event_certificate_sha256")
        state_count >= 2 || throw(ArgumentError(
            "a Hopf-Lyapunov seed needs at least two states"))
        1 <= anchor_state_index <= state_count || throw(ArgumentError(
            "anchor_state_index is outside the state order"))
        bordered_dimension = 2 * (state_count + 1)
        expected_dimensions = (
            bordered_dimension,
            bordered_dimension,
            state_count,
            2 * state_count,
        )
        centers = (
            right_bordered_center,
            adjoint_bordered_center,
            zero_resolvent_center,
            second_harmonic_center,
        )
        boxes = (
            right_bordered_remainder_box,
            adjoint_bordered_remainder_box,
            zero_resolvent_remainder_box,
            second_harmonic_remainder_box,
        )
        preconditioners = (
            right_bordered_preconditioner,
            adjoint_bordered_preconditioner,
            zero_resolvent_preconditioner,
            second_harmonic_preconditioner,
        )
        for proof in eachindex(expected_dimensions)
            dimension = expected_dimensions[proof]
            length(centers[proof]) == dimension &&
                length(boxes[proof]) == dimension ||
                throw(DimensionMismatch(
                    "Hopf-Lyapunov seed proof $proof has the wrong vector dimension"))
            all(value -> value isa _RORSExact, centers[proof]) ||
                throw(ArgumentError(
                    "Hopf-Lyapunov centers must be exact"))
            all(value -> value isa ROExactInterval, boxes[proof]) ||
                throw(ArgumentError(
                    "Hopf-Lyapunov remainder boxes must be exact intervals"))
            for interval in boxes[proof]
                interval.lower < 0 < interval.upper &&
                    interval.lower == -interval.upper ||
                    throw(ArgumentError(
                        "Hopf-Lyapunov remainder boxes must be positive-width and symmetric about zero"))
            end
            size(preconditioners[proof]) == (dimension, dimension) ||
                throw(DimensionMismatch(
                    "Hopf-Lyapunov preconditioner $proof has the wrong shape"))
        end
        _rors_validate_sha256(seed_sha256, "seed_sha256")
        expected = _rohl_seed_sha256(
            event_certificate_sha256,
            state_count,
            anchor_state_index,
            right_bordered_center,
            right_bordered_remainder_box,
            right_bordered_preconditioner,
            adjoint_bordered_center,
            adjoint_bordered_remainder_box,
            adjoint_bordered_preconditioner,
            zero_resolvent_center,
            zero_resolvent_remainder_box,
            zero_resolvent_preconditioner,
            second_harmonic_center,
            second_harmonic_remainder_box,
            second_harmonic_preconditioner,
        )
        seed_sha256 == expected || throw(ArgumentError(
            "Hopf-Lyapunov seed hash mismatch"))
        return new(
            version,
            event_certificate_sha256,
            state_count,
            anchor_state_index,
            right_bordered_center,
            right_bordered_remainder_box,
            right_bordered_preconditioner,
            adjoint_bordered_center,
            adjoint_bordered_remainder_box,
            adjoint_bordered_preconditioner,
            zero_resolvent_center,
            zero_resolvent_remainder_box,
            zero_resolvent_preconditioner,
            second_harmonic_center,
            second_harmonic_remainder_box,
            second_harmonic_preconditioner,
            seed_sha256,
        )
    end
end

function _rohl_exact_complex_vector(
    raw,
    expected_count::Int,
    label::String,
    context::_RORSContext;
    append_zero::Bool=false,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "$label must be an ordered vector or tuple"))
    length(raw) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count ComplexF64 values"))
    complex_count = expected_count + (append_zero ? 1 : 0)
    real_parts = Vector{_RORSExact}(undef, complex_count)
    imaginary_parts = Vector{_RORSExact}(undef, complex_count)
    for index in 1:expected_count
        value = raw[index]
        value isa ComplexF64 || throw(ArgumentError(
            "$label[$index] must be ComplexF64; implicit coercion is disabled"))
        real_parts[index] = _rors_exact_float(
            real(value), context.limits, "$label[$index].real")
        imaginary_parts[index] = _rors_exact_float(
            imag(value), context.limits, "$label[$index].imag")
        _rors_tick!(context, 2)
    end
    if append_zero
        real_parts[end] = zero(_RORSExact)
        imaginary_parts[end] = zero(_RORSExact)
        _rors_tick!(context, 2)
    end
    return vcat(real_parts, imaginary_parts)
end

function _rohl_exact_radii(
    raw,
    expected_count::Int,
    label::String,
    context::_RORSContext,
)
    values = if raw isa Float64
        fill(raw, expected_count)
    elseif raw isa AbstractVector || raw isa Tuple
        length(raw) == expected_count || throw(DimensionMismatch(
            "$label must contain $expected_count Float64 radii"))
        collect(raw)
    else
        throw(ArgumentError(
            "$label must be one Float64 radius or an ordered vector"))
    end
    result = Vector{ROExactInterval}(undef, expected_count)
    for index in 1:expected_count
        radius = _rors_exact_float(
            values[index], context.limits, "$label[$index]")
        _rors_tick!(context)
        radius > 0 || throw(ArgumentError(
            "$label[$index] must be strictly positive"))
        result[index] = _rors_interval(context, -radius, radius)
    end
    return result
end

"""
    ROHopfLyapunovSeed(system, event; ...)

Detach and exactify Float64 seed data for one parent spectral event. Complex
vectors are realified in the canonical `[real; imaginary]` order. The bordered
scalar centers are fixed at zero and included automatically.
"""
function ROHopfLyapunovSeed(
    system::ROPolynomialEquilibriumSystem,
    event::ROSimpleSpectralHopfEvent;
    anchor_state_index::Integer,
    right_eigenvector_center,
    right_bordered_remainder_radii,
    right_bordered_preconditioner,
    adjoint_eigenvector_center,
    adjoint_bordered_remainder_radii,
    adjoint_bordered_preconditioner,
    zero_resolvent_center,
    zero_resolvent_remainder_radii,
    zero_resolvent_preconditioner,
    second_harmonic_center,
    second_harmonic_remainder_radii,
    second_harmonic_preconditioner,
)
    validate_ro_polynomial_equilibrium_system(system)
    state_count = length(system.state_names)
    length(event.center) == state_count + 2 || throw(DimensionMismatch(
        "parent spectral event does not match the system state count"))
    anchor_state_index isa Integer &&
        typemin(Int) <= anchor_state_index <= typemax(Int) ||
        throw(ArgumentError("anchor_state_index must fit Int"))
    anchor = Int(anchor_state_index)
    1 <= anchor <= state_count || throw(ArgumentError(
        "anchor_state_index is outside the state order"))
    context = _RORSContext(system.limits, () -> nothing)
    bordered_dimension = 2 * (state_count + 1)
    right_center = _rohl_exact_complex_vector(
        right_eigenvector_center,
        state_count,
        "right_eigenvector_center",
        context;
        append_zero=true,
    )
    adjoint_center = _rohl_exact_complex_vector(
        adjoint_eigenvector_center,
        state_count,
        "adjoint_eigenvector_center",
        context;
        append_zero=true,
    )
    zero_center = _rors_exact_vector(
        zero_resolvent_center,
        state_count,
        "zero_resolvent_center",
        context,
    )
    second_center = _rohl_exact_complex_vector(
        second_harmonic_center,
        state_count,
        "second_harmonic_center",
        context,
    )
    right_box = _rohl_exact_radii(
        right_bordered_remainder_radii,
        bordered_dimension,
        "right_bordered_remainder_radii",
        context,
    )
    adjoint_box = _rohl_exact_radii(
        adjoint_bordered_remainder_radii,
        bordered_dimension,
        "adjoint_bordered_remainder_radii",
        context,
    )
    zero_box = _rohl_exact_radii(
        zero_resolvent_remainder_radii,
        state_count,
        "zero_resolvent_remainder_radii",
        context,
    )
    second_box = _rohl_exact_radii(
        second_harmonic_remainder_radii,
        2 * state_count,
        "second_harmonic_remainder_radii",
        context,
    )
    right_preconditioner = _rors_exact_matrix(
        right_bordered_preconditioner,
        bordered_dimension,
        bordered_dimension,
        "right_bordered_preconditioner",
        context,
    )
    adjoint_preconditioner = _rors_exact_matrix(
        adjoint_bordered_preconditioner,
        bordered_dimension,
        bordered_dimension,
        "adjoint_bordered_preconditioner",
        context,
    )
    zero_preconditioner = _rors_exact_matrix(
        zero_resolvent_preconditioner,
        state_count,
        state_count,
        "zero_resolvent_preconditioner",
        context,
    )
    second_preconditioner = _rors_exact_matrix(
        second_harmonic_preconditioner,
        2 * state_count,
        2 * state_count,
        "second_harmonic_preconditioner",
        context,
    )
    for (label, preconditioner) in (
        ("right bordered", right_preconditioner),
        ("adjoint bordered", adjoint_preconditioner),
        ("zero resolvent", zero_preconditioner),
        ("second harmonic", second_preconditioner),
    )
        _rors_exact_rank(preconditioner, context) ==
            size(preconditioner, 1) || throw(ArgumentError(
            "$label preconditioner must have exact full rank"))
    end
    wrappers = (
        _rors_exact_matrix_wrapper(right_preconditioner),
        _rors_exact_matrix_wrapper(adjoint_preconditioner),
        _rors_exact_matrix_wrapper(zero_preconditioner),
        _rors_exact_matrix_wrapper(second_preconditioner),
    )
    centers = (
        Tuple(right_center),
        Tuple(adjoint_center),
        Tuple(zero_center),
        Tuple(second_center),
    )
    boxes = (
        Tuple(right_box),
        Tuple(adjoint_box),
        Tuple(zero_box),
        Tuple(second_box),
    )
    seed_sha256 = _rohl_seed_sha256(
        event.certificate_sha256,
        state_count,
        anchor,
        centers[1], boxes[1], wrappers[1],
        centers[2], boxes[2], wrappers[2],
        centers[3], boxes[3], wrappers[3],
        centers[4], boxes[4], wrappers[4],
    )
    return ROHopfLyapunovSeed(
        _ROHL_VALIDATED_TOKEN,
        RO_HOPF_LYAPUNOV_SEED_VERSION,
        event.certificate_sha256,
        state_count,
        anchor,
        centers[1], boxes[1], wrappers[1],
        centers[2], boxes[2], wrappers[2],
        centers[3], boxes[3], wrappers[3],
        centers[4], boxes[4], wrappers[4],
        seed_sha256,
    )
end

struct _ROHLComplexInterval
    real::ROExactInterval
    imaginary::ROExactInterval
end

@inline function _rohl_complex_zero(context::_RORSContext)
    zero_interval = _rors_point(context, zero(_RORSExact))
    return _ROHLComplexInterval(zero_interval, zero_interval)
end

@inline function _rohl_complex_real(
    context::_RORSContext,
    value::ROExactInterval,
)
    return _ROHLComplexInterval(
        value, _rors_point(context, zero(_RORSExact)))
end

function _rohl_complex_add(
    context::_RORSContext,
    left::_ROHLComplexInterval,
    right::_ROHLComplexInterval,
)
    return _ROHLComplexInterval(
        _rors_add(context, left.real, right.real),
        _rors_add(context, left.imaginary, right.imaginary),
    )
end

function _rohl_complex_negate(
    context::_RORSContext,
    value::_ROHLComplexInterval,
)
    return _ROHLComplexInterval(
        _rors_negate(context, value.real),
        _rors_negate(context, value.imaginary),
    )
end

function _rohl_complex_subtract(
    context::_RORSContext,
    left::_ROHLComplexInterval,
    right::_ROHLComplexInterval,
)
    return _rohl_complex_add(
        context, left, _rohl_complex_negate(context, right))
end

function _rohl_complex_multiply(
    context::_RORSContext,
    left::_ROHLComplexInterval,
    right::_ROHLComplexInterval,
)
    return _ROHLComplexInterval(
        _rors_subtract(
            context,
            _rors_multiply(context, left.real, right.real),
            _rors_multiply(context, left.imaginary, right.imaginary),
        ),
        _rors_add(
            context,
            _rors_multiply(context, left.real, right.imaginary),
            _rors_multiply(context, left.imaginary, right.real),
        ),
    )
end

function _rohl_complex_real_scale(
    context::_RORSContext,
    scalar::ROExactInterval,
    value::_ROHLComplexInterval,
)
    return _ROHLComplexInterval(
        _rors_multiply(context, scalar, value.real),
        _rors_multiply(context, scalar, value.imaginary),
    )
end

@inline function _rohl_complex_conjugate(
    context::_RORSContext,
    value::_ROHLComplexInterval,
)
    return _ROHLComplexInterval(
        value.real, _rors_negate(context, value.imaginary))
end

function _rohl_realify_matrix(
    matrix::Matrix{_ROHLComplexInterval},
    context::_RORSContext,
)
    rows, columns = size(matrix)
    result = Matrix{ROExactInterval}(undef, 2 * rows, 2 * columns)
    for column in 1:columns, row in 1:rows
        value = matrix[row, column]
        result[row, column] = value.real
        result[row, columns + column] =
            _rors_negate(context, value.imaginary)
        result[rows + row, column] = value.imaginary
        result[rows + row, columns + column] = value.real
    end
    return result
end

function _rohl_realify_vector(values::Vector{_ROHLComplexInterval})
    return vcat(
        [value.real for value in values],
        [value.imaginary for value in values],
    )
end

function _rohl_complex_solution(
    realified::Vector{ROExactInterval},
    complex_count::Int,
)
    length(realified) == 2 * complex_count || throw(DimensionMismatch(
        "realified complex solution has the wrong length"))
    return [
        _ROHLComplexInterval(realified[index],
            realified[complex_count + index])
        for index in 1:complex_count
    ]
end

function _rohl_interval_point_vector_product(
    matrix::Matrix{ROExactInterval},
    vector::Vector{_RORSExact},
    context::_RORSContext,
)
    rows, columns = size(matrix)
    length(vector) == columns || throw(DimensionMismatch(
        "interval/point vector product has incompatible dimensions"))
    result = Vector{ROExactInterval}(undef, rows)
    for row in 1:rows
        value = _rors_point(context, zero(_RORSExact))
        for column in 1:columns
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    matrix[row, column],
                    _rors_point(context, vector[column]),
                ),
            )
        end
        result[row] = value
    end
    return result
end

function _rohl_certify_linear_system(
    label::String,
    matrix::Matrix{ROExactInterval},
    right_hand_side::Vector{ROExactInterval},
    center::Tuple,
    remainder_box::Tuple,
    preconditioner::ROExactMatrix,
    limits::ROHopfLyapunovLimits,
    context::_RORSContext,
)
    dimension = length(center)
    size(matrix) == (dimension, dimension) &&
        length(right_hand_side) == dimension &&
        length(remainder_box) == dimension &&
        size(preconditioner) == (dimension, dimension) ||
        throw(DimensionMismatch("$label proof dimensions do not match"))
    _rohl_limit(
        :realified_linear_dimension,
        dimension,
        limits.max_realified_linear_dimension,
    )
    point_preconditioner = _rors_exact_matrix_values(preconditioner)
    _rors_exact_rank(point_preconditioner, context) == dimension ||
        throw(ArgumentError("$label preconditioner must have exact full rank"))
    center_vector = collect(center)
    remainder_vector = collect(remainder_box)
    residual = _rohl_interval_point_vector_product(
        matrix, center_vector, context)
    for row in 1:dimension
        residual[row] = _rors_subtract(
            context, residual[row], right_hand_side[row])
    end
    preconditioned_matrix = _rors_point_interval_matrix_product(
        point_preconditioner, matrix, context)
    error_matrix = _rors_error_matrix(preconditioned_matrix, context)
    beta = _rors_beta(error_matrix, context)
    beta < 1 || throw(ROHopfLyapunovRejected(
        :linear_contraction_not_proven,
        "$label has beta=$beta, requiring beta < 1",
    ))
    newton_offset = _rors_negate_vector(
        _rors_point_interval_vector_product(
            point_preconditioner, residual, context),
        context,
    )
    error_image = _rors_interval_vector_product(
        error_matrix, remainder_vector, context)
    image = Vector{ROExactInterval}(undef, dimension)
    solution = Vector{ROExactInterval}(undef, dimension)
    for coordinate in 1:dimension
        image[coordinate] = _rors_add(
            context, newton_offset[coordinate], error_image[coordinate])
        _rors_strict_subset(
            image[coordinate], remainder_vector[coordinate]) ||
            throw(ROHopfLyapunovRejected(
                :linear_krawczyk_inclusion_not_proven,
                "$label Krawczyk image is not strictly interior in coordinate $coordinate",
            ))
        solution[coordinate] = _rors_add(
            context,
            _rors_point(context, center_vector[coordinate]),
            image[coordinate],
        )
    end
    return Tuple(image), Tuple(solution), beta
end

function _rohl_sqrt_bound(
    value::_RORSExact,
    return_upper::Bool,
    limits::ROHopfLyapunovLimits,
    context::_RORSContext,
)
    value > 0 || throw(ROHopfLyapunovRejected(
        :nonpositive_frequency_squared,
        "the parent frequency-squared root must stay positive",
    ))
    _rohl_limit(
        :sqrt_bisection_steps,
        _ROHL_SQRT_BISECTION_STEPS,
        limits.max_sqrt_bisection_steps,
    )
    lower = zero(_RORSExact)
    # For positive n/d, n < 2^nbits(n) and d >= 2^(nbits(d)-1), so
    # value < 2^e with e=nbits(n)-nbits(d)+1. Choosing k=ceil(e/2)
    # gives the exact power-of-two bracket sqrt(value) < 2^k, including
    # subnormal-scale values without hundreds of leading zero bisections.
    exponent_bound = ndigits(numerator(value); base=2) -
        ndigits(denominator(value); base=2) + 1
    sqrt_exponent_bound = cld(exponent_bound, 2)
    two = _RORSExact(2)
    magnitude = _rors_exact_power(
        context, two, abs(sqrt_exponent_bound))
    upper = sqrt_exponent_bound >= 0 ? magnitude :
        _rors_exact_divide(context, one(_RORSExact), magnitude)
    _rors_exact_multiply(context, upper, upper) >= value ||
        throw(ROHopfLyapunovRejected(
            :frequency_sqrt_bracket_failed,
            "the exact power-of-two square-root bracket is not an upper bound",
        ))
    for step in 1:_ROHL_SQRT_BISECTION_STEPS
        step % 64 == 0 && context.cancel_check()
        midpoint = _rors_exact_divide(
            context,
            _rors_exact_add(context, lower, upper),
            two,
        )
        midpoint_squared = _rors_exact_multiply(
            context, midpoint, midpoint)
        if midpoint_squared <= value
            lower = midpoint
        else
            upper = midpoint
        end
    end
    return return_upper ? upper : lower
end

function _rohl_frequency_enclosure(
    frequency_squared::ROExactInterval,
    limits::ROHopfLyapunovLimits,
    context::_RORSContext,
)
    lower = _rohl_sqrt_bound(
        frequency_squared.lower, false, limits, context)
    upper = _rohl_sqrt_bound(
        frequency_squared.upper, true, limits, context)
    lower > 0 && lower < upper || throw(ROHopfLyapunovRejected(
        :positive_frequency_enclosure_not_proven,
        "fixed exact bisection did not produce a strict positive frequency enclosure",
    ))
    lower_squared = _rors_exact_multiply(context, lower, lower)
    upper_squared = _rors_exact_multiply(context, upper, upper)
    lower_squared <= frequency_squared.lower &&
        frequency_squared.upper <= upper_squared ||
        throw(ROHopfLyapunovRejected(
            :frequency_enclosure_mismatch,
            "the exact frequency enclosure does not cover the parent squared-frequency root",
        ))
    return _rors_interval(context, lower, upper)
end

function _rohl_event_root_box(
    event::ROSimpleSpectralHopfEvent,
    context::_RORSContext,
)
    result = Vector{ROExactInterval}(undef, length(event.center))
    for variable in eachindex(result)
        result[variable] = _rors_add(
            context,
            _rors_point(context, event.center[variable]),
            event.krawczyk_offset_image[variable],
        )
    end
    return result
end

function _rohl_derivative_polynomials(
    system::ROPolynomialEquilibriumSystem,
    limits::ROHopfLyapunovLimits,
    context::_RORSContext,
)
    state_count = length(system.state_names)
    tensor_entries = BigInt(state_count)^2 +
        BigInt(state_count)^3 + BigInt(state_count)^4
    _rohl_limit(
        :derivative_tensor_entries,
        tensor_entries,
        limits.max_derivative_tensor_entries,
    )
    residual = _rohsc_system_polynomials(system, context)
    first = Matrix{_RORSPolynomial}(undef, state_count, state_count)
    second = Array{_RORSPolynomial}(undef,
        state_count, state_count, state_count)
    third = Array{_RORSPolynomial}(undef,
        state_count, state_count, state_count, state_count)
    for equation in 1:state_count
        context.cancel_check()
        for first_state in 1:state_count
            first_derivative = _rors_polynomial_derivative(
                residual[equation], first_state, context)
            first[equation, first_state] = first_derivative
            for second_state in 1:state_count
                second_derivative = _rors_polynomial_derivative(
                    first_derivative, second_state, context)
                second[equation, first_state, second_state] =
                    second_derivative
                for third_state in 1:state_count
                    third[equation, first_state, second_state,
                        third_state] = _rors_polynomial_derivative(
                        second_derivative, third_state, context)
                end
            end
        end
    end
    return first, second, third
end

function _rohl_evaluate_derivatives(
    first::Matrix{_RORSPolynomial},
    second::Array{_RORSPolynomial,3},
    third::Array{_RORSPolynomial,4},
    root_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    state_count = size(first, 1)
    state_jacobian = Matrix{ROExactInterval}(
        undef, state_count, state_count)
    bilinear = Array{ROExactInterval}(
        undef, state_count, state_count, state_count)
    trilinear = Array{ROExactInterval}(
        undef, state_count, state_count, state_count, state_count)
    for equation in 1:state_count
        context.cancel_check()
        for first_state in 1:state_count
            state_jacobian[equation, first_state] =
                _rors_evaluate_polynomial(
                    first[equation, first_state], root_box, context)
            for second_state in 1:state_count
                bilinear[equation, first_state, second_state] =
                    _rors_evaluate_polynomial(
                        second[equation, first_state, second_state],
                        root_box,
                        context,
                    )
                for third_state in 1:state_count
                    trilinear[equation, first_state, second_state,
                        third_state] = _rors_evaluate_polynomial(
                        third[equation, first_state, second_state,
                            third_state],
                        root_box,
                        context,
                    )
                end
            end
        end
    end
    return state_jacobian, bilinear, trilinear
end

function _rohl_right_bordered_system(
    state_jacobian::Matrix{ROExactInterval},
    frequency::ROExactInterval,
    anchor::Int,
    context::_RORSContext,
)
    state_count = size(state_jacobian, 1)
    complex_dimension = state_count + 1
    matrix = Matrix{_ROHLComplexInterval}(
        undef, complex_dimension, complex_dimension)
    for index in eachindex(matrix)
        matrix[index] = _rohl_complex_zero(context)
    end
    for row in 1:state_count, column in 1:state_count
        imaginary = row == column ?
            _rors_negate(context, frequency) :
            _rors_point(context, zero(_RORSExact))
        matrix[row, column] = _ROHLComplexInterval(
            state_jacobian[row, column], imaginary)
    end
    one_interval = _rors_point(context, one(_RORSExact))
    matrix[anchor, complex_dimension] =
        _rohl_complex_real(context, one_interval)
    matrix[complex_dimension, anchor] =
        _rohl_complex_real(context, one_interval)
    rhs = [_rohl_complex_zero(context) for _ in 1:complex_dimension]
    rhs[end] = _rohl_complex_real(context, one_interval)
    return _rohl_realify_matrix(matrix, context), _rohl_realify_vector(rhs)
end

function _rohl_adjoint_bordered_system(
    state_jacobian::Matrix{ROExactInterval},
    frequency::ROExactInterval,
    right_eigenvector::Vector{_ROHLComplexInterval},
    anchor::Int,
    context::_RORSContext,
)
    state_count = size(state_jacobian, 1)
    length(right_eigenvector) == state_count || throw(DimensionMismatch(
        "right eigenvector does not match the state count"))
    complex_dimension = state_count + 1
    matrix = Matrix{_ROHLComplexInterval}(
        undef, complex_dimension, complex_dimension)
    for index in eachindex(matrix)
        matrix[index] = _rohl_complex_zero(context)
    end
    for row in 1:state_count, column in 1:state_count
        imaginary = row == column ? frequency :
            _rors_point(context, zero(_RORSExact))
        matrix[row, column] = _ROHLComplexInterval(
            state_jacobian[column, row], imaginary)
    end
    one_interval = _rors_point(context, one(_RORSExact))
    matrix[anchor, complex_dimension] =
        _rohl_complex_real(context, one_interval)
    for column in 1:state_count
        matrix[complex_dimension, column] =
            _rohl_complex_conjugate(
                context, right_eigenvector[column])
    end
    rhs = [_rohl_complex_zero(context) for _ in 1:complex_dimension]
    rhs[end] = _rohl_complex_real(context, one_interval)
    return _rohl_realify_matrix(matrix, context), _rohl_realify_vector(rhs)
end

function _rohl_b_q_qbar_real(
    bilinear::Array{ROExactInterval,3},
    q::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    state_count = length(q)
    result = Vector{ROExactInterval}(undef, state_count)
    for equation in 1:state_count
        value = _rors_point(context, zero(_RORSExact))
        for first_state in 1:state_count, second_state in 1:state_count
            product = _rors_add(
                context,
                _rors_multiply(
                    context,
                    q[first_state].real,
                    q[second_state].real,
                ),
                _rors_multiply(
                    context,
                    q[first_state].imaginary,
                    q[second_state].imaginary,
                ),
            )
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    bilinear[equation, first_state, second_state],
                    product,
                ),
            )
        end
        result[equation] = value
    end
    return result
end

function _rohl_apply_bilinear(
    bilinear::Array{ROExactInterval,3},
    left::Vector{_ROHLComplexInterval},
    right::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    state_count = length(left)
    length(right) == state_count || throw(DimensionMismatch(
        "bilinear arguments have different dimensions"))
    result = Vector{_ROHLComplexInterval}(undef, state_count)
    for equation in 1:state_count
        value = _rohl_complex_zero(context)
        for first_state in 1:state_count, second_state in 1:state_count
            product = _rohl_complex_multiply(
                context, left[first_state], right[second_state])
            value = _rohl_complex_add(
                context,
                value,
                _rohl_complex_real_scale(
                    context,
                    bilinear[equation, first_state, second_state],
                    product,
                ),
            )
        end
        result[equation] = value
    end
    return result
end

function _rohl_apply_trilinear(
    trilinear::Array{ROExactInterval,4},
    first::Vector{_ROHLComplexInterval},
    second::Vector{_ROHLComplexInterval},
    third::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    state_count = length(first)
    length(second) == state_count && length(third) == state_count ||
        throw(DimensionMismatch(
            "trilinear arguments have different dimensions"))
    result = Vector{_ROHLComplexInterval}(undef, state_count)
    for equation in 1:state_count
        context.cancel_check()
        value = _rohl_complex_zero(context)
        for first_state in 1:state_count,
                second_state in 1:state_count,
                third_state in 1:state_count
            product = _rohl_complex_multiply(
                context,
                _rohl_complex_multiply(
                    context,
                    first[first_state],
                    second[second_state],
                ),
                third[third_state],
            )
            value = _rohl_complex_add(
                context,
                value,
                _rohl_complex_real_scale(
                    context,
                    trilinear[equation, first_state, second_state,
                        third_state],
                    product,
                ),
            )
        end
        result[equation] = value
    end
    return result
end

function _rohl_second_harmonic_rhs(
    bilinear::Array{ROExactInterval,3},
    q::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    return _rohl_apply_bilinear(bilinear, q, q, context)
end

function _rohl_second_harmonic_matrix(
    state_jacobian::Matrix{ROExactInterval},
    frequency::ROExactInterval,
    context::_RORSContext,
)
    state_count = size(state_jacobian, 1)
    matrix = Matrix{_ROHLComplexInterval}(
        undef, state_count, state_count)
    two = _rors_point(context, _RORSExact(2))
    two_frequency = _rors_multiply(context, two, frequency)
    for row in 1:state_count, column in 1:state_count
        matrix[row, column] = _ROHLComplexInterval(
            _rors_negate(context, state_jacobian[row, column]),
            row == column ? two_frequency :
                _rors_point(context, zero(_RORSExact)),
        )
    end
    return _rohl_realify_matrix(matrix, context)
end

function _rohl_real_matrix_as_interval(
    matrix::Matrix{ROExactInterval},
)
    return matrix
end

function _rohl_real_vector_as_complex(
    values::Vector{ROExactInterval},
    context::_RORSContext,
)
    return [_rohl_complex_real(context, value) for value in values]
end

function _rohl_hermitian_inner_product(
    left::Vector{_ROHLComplexInterval},
    right::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    length(left) == length(right) || throw(DimensionMismatch(
        "Hermitian inner-product arguments have different dimensions"))
    result = _rohl_complex_zero(context)
    for index in eachindex(left)
        result = _rohl_complex_add(
            context,
            result,
            _rohl_complex_multiply(
                context,
                _rohl_complex_conjugate(context, left[index]),
                right[index],
            ),
        )
    end
    return result
end

function _rohl_q_norm_squared(
    q::Vector{_ROHLComplexInterval},
    context::_RORSContext,
)
    result = _rors_point(context, zero(_RORSExact))
    for value in q
        result = _rors_add(
            context,
            result,
            _rors_add(
                context,
                _rors_integer_power(context, value.real, 2),
                _rors_integer_power(context, value.imaginary, 2),
            ),
        )
    end
    return result
end

function _rohl_positive_divide(
    numerator_interval::ROExactInterval,
    denominator_interval::ROExactInterval,
    context::_RORSContext,
)
    denominator_interval.lower > 0 || throw(ROHopfLyapunovRejected(
        :positive_normalization_denominator_not_proven,
        "a first-Lyapunov normalization denominator reaches zero",
    ))
    reciprocal = _rors_interval(
        context,
        _rors_exact_divide(
            context, one(_RORSExact), denominator_interval.upper),
        _rors_exact_divide(
            context, one(_RORSExact), denominator_interval.lower),
    )
    return _rors_multiply(context, numerator_interval, reciprocal)
end

function _rohl_seed_admission_operation_count(seed::ROHopfLyapunovSeed)
    count = BigInt(0)
    for (center, box, preconditioner) in (
        (seed.right_bordered_center,
            seed.right_bordered_remainder_box,
            seed.right_bordered_preconditioner),
        (seed.adjoint_bordered_center,
            seed.adjoint_bordered_remainder_box,
            seed.adjoint_bordered_preconditioner),
        (seed.zero_resolvent_center,
            seed.zero_resolvent_remainder_box,
            seed.zero_resolvent_preconditioner),
        (seed.second_harmonic_center,
            seed.second_harmonic_remainder_box,
            seed.second_harmonic_preconditioner),
    )
        count += length(center) + 2 * length(box) + length(preconditioner.data)
    end
    return count
end

function _rohl_event_sha256(
    parent_event_certificate_sha256::String,
    seed_sha256::String,
    root_enclosure::Tuple,
    frequency_enclosure::ROExactInterval,
    right_solution_enclosure::Tuple,
    adjoint_solution_enclosure::Tuple,
    zero_resolvent_solution_enclosure::Tuple,
    second_harmonic_solution_enclosure::Tuple,
    contraction_betas::Tuple,
    q_norm_squared_enclosure::ROExactInterval,
    adjoint_pairing_real_enclosure::ROExactInterval,
    adjoint_pairing_imaginary_enclosure::ROExactInterval,
    g21_real_enclosure::ROExactInterval,
    g21_imaginary_enclosure::ROExactInterval,
    first_lyapunov_coefficient_enclosure::ROExactInterval,
    center_manifold_criticality::Symbol,
)
    io = IOBuffer()
    for value in (
        RO_FIRST_LYAPUNOV_HOPF_EVENT_VERSION,
        RO_FIRST_LYAPUNOV_FORMULA_VERSION,
        RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION,
        parent_event_certificate_sha256,
        seed_sha256,
    )
        _rors_write_token(io, value)
    end
    _rors_write_interval_vector(io, root_enclosure)
    _rors_write_interval(io, frequency_enclosure)
    for enclosure in (
        right_solution_enclosure,
        adjoint_solution_enclosure,
        zero_resolvent_solution_enclosure,
        second_harmonic_solution_enclosure,
    )
        _rors_write_interval_vector(io, enclosure)
    end
    _rors_write_token(io, length(contraction_betas))
    for beta in contraction_betas
        _rors_write_exact(io, beta)
    end
    for enclosure in (
        q_norm_squared_enclosure,
        adjoint_pairing_real_enclosure,
        adjoint_pairing_imaginary_enclosure,
        g21_real_enclosure,
        g21_imaginary_enclosure,
        first_lyapunov_coefficient_enclosure,
    )
        _rors_write_interval(io, enclosure)
    end
    for value in (
        center_manifold_criticality,
        true,  # parent_complete_census_required_for_authority
        true,  # right_eigenpair_bordered_system_certified
        true,  # hermitian_adjoint_normalization_certified
        true,  # zero_resolvent_certified
        true,  # second_harmonic_resolvent_certified
        true,  # first_lyapunov_coefficient_nonzero_certified
        true,  # nondegenerate_local_hopf_certified
        false, # original_control_side_certified
        false, # periodic_orbit_incidence_certified
        false, # full_state_periodic_orbit_stability_certified
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
One P8s1c1 first-Lyapunov proof attached to a parent P8s1c0 event.

The numerical coefficient uses the unit-Euclidean-norm right-eigenvector
convention. Its sign gives center-manifold criticality. This local record alone
cannot exclude another imaginary pair, orient the original control side, or
attach a machine-validated periodic-orbit branch.
"""
struct ROFirstLyapunovHopfEvent
    version::String
    formula_version::String
    normalization_version::String
    parent_event_certificate_sha256::String
    seed_sha256::String
    root_enclosure::Tuple
    frequency_enclosure::ROExactInterval
    right_solution_enclosure::Tuple
    adjoint_solution_enclosure::Tuple
    zero_resolvent_solution_enclosure::Tuple
    second_harmonic_solution_enclosure::Tuple
    contraction_betas::Tuple
    q_norm_squared_enclosure::ROExactInterval
    adjoint_pairing_real_enclosure::ROExactInterval
    adjoint_pairing_imaginary_enclosure::ROExactInterval
    g21_real_enclosure::ROExactInterval
    g21_imaginary_enclosure::ROExactInterval
    first_lyapunov_coefficient_enclosure::ROExactInterval
    center_manifold_criticality::Symbol
    parent_complete_census_required_for_authority::Bool
    right_eigenpair_bordered_system_certified::Bool
    hermitian_adjoint_normalization_certified::Bool
    zero_resolvent_certified::Bool
    second_harmonic_resolvent_certified::Bool
    first_lyapunov_coefficient_nonzero_certified::Bool
    nondegenerate_local_hopf_certified::Bool
    original_control_side_certified::Bool
    periodic_orbit_incidence_certified::Bool
    full_state_periodic_orbit_stability_certified::Bool
    certificate_sha256::String

    function ROFirstLyapunovHopfEvent(
        ::_ROHLValidatedToken,
        version::String,
        formula_version::String,
        normalization_version::String,
        parent_event_certificate_sha256::String,
        seed_sha256::String,
        root_enclosure::Tuple,
        frequency_enclosure::ROExactInterval,
        right_solution_enclosure::Tuple,
        adjoint_solution_enclosure::Tuple,
        zero_resolvent_solution_enclosure::Tuple,
        second_harmonic_solution_enclosure::Tuple,
        contraction_betas::Tuple,
        q_norm_squared_enclosure::ROExactInterval,
        adjoint_pairing_real_enclosure::ROExactInterval,
        adjoint_pairing_imaginary_enclosure::ROExactInterval,
        g21_real_enclosure::ROExactInterval,
        g21_imaginary_enclosure::ROExactInterval,
        first_lyapunov_coefficient_enclosure::ROExactInterval,
        center_manifold_criticality::Symbol,
        parent_complete_census_required_for_authority::Bool,
        right_eigenpair_bordered_system_certified::Bool,
        hermitian_adjoint_normalization_certified::Bool,
        zero_resolvent_certified::Bool,
        second_harmonic_resolvent_certified::Bool,
        first_lyapunov_coefficient_nonzero_certified::Bool,
        nondegenerate_local_hopf_certified::Bool,
        original_control_side_certified::Bool,
        periodic_orbit_incidence_certified::Bool,
        full_state_periodic_orbit_stability_certified::Bool,
        certificate_sha256::String,
    )
        version == RO_FIRST_LYAPUNOV_HOPF_EVENT_VERSION ||
            throw(ArgumentError("first-Lyapunov event version mismatch"))
        formula_version == RO_FIRST_LYAPUNOV_FORMULA_VERSION ||
            throw(ArgumentError("first-Lyapunov formula version mismatch"))
        normalization_version ==
            RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION ||
            throw(ArgumentError(
                "Hopf eigenvector normalization version mismatch"))
        for (hash, label) in (
            (parent_event_certificate_sha256,
                "parent_event_certificate_sha256"),
            (seed_sha256, "seed_sha256"),
            (certificate_sha256, "certificate_sha256"),
        )
            _rors_validate_sha256(hash, label)
        end
        state_count = length(root_enclosure) - 2
        state_count >= 2 || throw(DimensionMismatch(
            "a first-Lyapunov root enclosure needs at least two states, one control, and frequency squared"))
        all(value -> value isa ROExactInterval, root_enclosure) ||
            throw(ArgumentError(
                "first-Lyapunov root enclosure must contain exact intervals"))
        all(interval -> interval.lower > 0, root_enclosure) ||
            throw(ArgumentError(
                "first-Lyapunov root coordinates and squared frequency must stay positive"))
        expected_solution_lengths = (
            2 * (state_count + 1),
            2 * (state_count + 1),
            state_count,
            2 * state_count,
        )
        solution_enclosures = (
            right_solution_enclosure,
            adjoint_solution_enclosure,
            zero_resolvent_solution_enclosure,
            second_harmonic_solution_enclosure,
        )
        for proof in eachindex(expected_solution_lengths)
            length(solution_enclosures[proof]) ==
                expected_solution_lengths[proof] ||
                throw(DimensionMismatch(
                    "first-Lyapunov proof $proof has the wrong solution dimension"))
            all(value -> value isa ROExactInterval,
                solution_enclosures[proof]) || throw(ArgumentError(
                "first-Lyapunov solution enclosures must contain exact intervals"))
        end
        length(contraction_betas) == 4 &&
            all(beta -> beta isa _RORSExact && 0 <= beta < 1,
                contraction_betas) || throw(ArgumentError(
            "first-Lyapunov proof needs four contraction betas in [0,1)"))
        frequency_enclosure.lower > 0 || throw(ArgumentError(
            "first-Lyapunov frequency enclosure must be positive"))
        frequency_enclosure.lower^2 <= root_enclosure[end].lower &&
            root_enclosure[end].upper <= frequency_enclosure.upper^2 ||
            throw(ArgumentError(
                "frequency enclosure does not cover the squared-frequency root"))
        q_norm_squared_enclosure.lower > 0 || throw(ArgumentError(
            "right-eigenvector norm must be strictly positive"))
        one(_RORSExact) in adjoint_pairing_real_enclosure &&
            _rors_contains_zero(adjoint_pairing_imaginary_enclosure) ||
            throw(ArgumentError(
                "Hermitian adjoint pairing enclosure does not contain one"))
        !_rors_contains_zero(first_lyapunov_coefficient_enclosure) ||
            throw(ArgumentError(
                "first Lyapunov coefficient enclosure reaches zero"))
        expected_criticality =
            first_lyapunov_coefficient_enclosure.upper < 0 ?
            :supercritical : :subcritical
        center_manifold_criticality == expected_criticality ||
            throw(ArgumentError(
                "center-manifold criticality disagrees with the Lyapunov sign"))
        for (value, label) in (
            (parent_complete_census_required_for_authority,
                "parent-census authority boundary"),
            (right_eigenpair_bordered_system_certified,
                "right bordered eigensystem"),
            (hermitian_adjoint_normalization_certified,
                "Hermitian adjoint normalization"),
            (zero_resolvent_certified, "zero resolvent"),
            (second_harmonic_resolvent_certified,
                "second-harmonic resolvent"),
            (first_lyapunov_coefficient_nonzero_certified,
                "nonzero first Lyapunov coefficient"),
            (nondegenerate_local_hopf_certified,
                "nondegenerate local Hopf"),
        )
            value || throw(ArgumentError(
                "first-Lyapunov event lost $label"))
        end
        for (value, label) in (
            (original_control_side_certified, "original-control side"),
            (periodic_orbit_incidence_certified,
                "periodic-orbit incidence"),
            (full_state_periodic_orbit_stability_certified,
                "full-state periodic-orbit stability"),
        )
            value && throw(ArgumentError(
                "P8s1c1 cannot certify $label"))
        end
        expected = _rohl_event_sha256(
            parent_event_certificate_sha256,
            seed_sha256,
            root_enclosure,
            frequency_enclosure,
            right_solution_enclosure,
            adjoint_solution_enclosure,
            zero_resolvent_solution_enclosure,
            second_harmonic_solution_enclosure,
            contraction_betas,
            q_norm_squared_enclosure,
            adjoint_pairing_real_enclosure,
            adjoint_pairing_imaginary_enclosure,
            g21_real_enclosure,
            g21_imaginary_enclosure,
            first_lyapunov_coefficient_enclosure,
            center_manifold_criticality,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "first-Lyapunov event hash mismatch"))
        return new(
            version,
            formula_version,
            normalization_version,
            parent_event_certificate_sha256,
            seed_sha256,
            root_enclosure,
            frequency_enclosure,
            right_solution_enclosure,
            adjoint_solution_enclosure,
            zero_resolvent_solution_enclosure,
            second_harmonic_solution_enclosure,
            contraction_betas,
            q_norm_squared_enclosure,
            adjoint_pairing_real_enclosure,
            adjoint_pairing_imaginary_enclosure,
            g21_real_enclosure,
            g21_imaginary_enclosure,
            first_lyapunov_coefficient_enclosure,
            center_manifold_criticality,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            false,
            false,
            false,
            certificate_sha256,
        )
    end
end

function _rohl_make_event(
    parent_event_certificate_sha256::String,
    seed_sha256::String,
    root_enclosure::Tuple,
    frequency_enclosure::ROExactInterval,
    right_solution_enclosure::Tuple,
    adjoint_solution_enclosure::Tuple,
    zero_resolvent_solution_enclosure::Tuple,
    second_harmonic_solution_enclosure::Tuple,
    contraction_betas::Tuple,
    q_norm_squared_enclosure::ROExactInterval,
    adjoint_pairing::_ROHLComplexInterval,
    g21::_ROHLComplexInterval,
    first_lyapunov_coefficient_enclosure::ROExactInterval,
    center_manifold_criticality::Symbol,
)
    hash = _rohl_event_sha256(
        parent_event_certificate_sha256,
        seed_sha256,
        root_enclosure,
        frequency_enclosure,
        right_solution_enclosure,
        adjoint_solution_enclosure,
        zero_resolvent_solution_enclosure,
        second_harmonic_solution_enclosure,
        contraction_betas,
        q_norm_squared_enclosure,
        adjoint_pairing.real,
        adjoint_pairing.imaginary,
        g21.real,
        g21.imaginary,
        first_lyapunov_coefficient_enclosure,
        center_manifold_criticality,
    )
    return ROFirstLyapunovHopfEvent(
        _ROHL_VALIDATED_TOKEN,
        RO_FIRST_LYAPUNOV_HOPF_EVENT_VERSION,
        RO_FIRST_LYAPUNOV_FORMULA_VERSION,
        RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION,
        parent_event_certificate_sha256,
        seed_sha256,
        root_enclosure,
        frequency_enclosure,
        right_solution_enclosure,
        adjoint_solution_enclosure,
        zero_resolvent_solution_enclosure,
        second_harmonic_solution_enclosure,
        contraction_betas,
        q_norm_squared_enclosure,
        adjoint_pairing.real,
        adjoint_pairing.imaginary,
        g21.real,
        g21.imaginary,
        first_lyapunov_coefficient_enclosure,
        center_manifold_criticality,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        false,
        false,
        false,
        hash,
    )
end

function _rohl_census_sha256(
    system_declaration_sha256::String,
    dynamics_binding_declaration_sha256::String,
    parent_census_sha256::String,
    limits::ROHopfLyapunovLimits,
    seeds::Tuple,
    events::Tuple,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    for value in (
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_VERSION,
        system_declaration_sha256,
        dynamics_binding_declaration_sha256,
        parent_census_sha256,
        RO_FIRST_LYAPUNOV_FORMULA_VERSION,
        RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION,
    )
        _rors_write_token(io, value)
    end
    _rohl_write_limits(io, limits)
    _rors_write_token(io, length(seeds))
    for seed in seeds
        _rors_write_token(io, seed.seed_sha256)
    end
    _rors_write_token(io, length(events))
    for event in events
        _rors_write_token(io, event.certificate_sha256)
    end
    _rors_write_token(io, analysis_interval_operation_count)
    for value in (
        true,  # parent_census_replayed
        true,  # every_parent_event_lifted_exactly_once
        true,  # all_first_lyapunov_coefficients_nonzero_certified
        true,  # nondegenerate_local_hopf_event_set_complete_for_parent
        false, # original_control_sides_certified
        false, # periodic_orbit_incidence_certified
        false, # full_state_periodic_orbit_stability_certified
        false, # stable_root_population_complete
        false, # global_continuation_certified
        false, # true_hysteresis_certified
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Complete P8s1c1 lift of every event in one replayed P8s1c0 parent census.

All parent spectral events have a strict nonzero first-Lyapunov enclosure, so
they are nondegenerate local Hopf bifurcations in the fixed Kuznetsov
convention. Explicit periodic-orbit incidence, original-control side, full
state stability, global continuation, and hysteresis remain uncertified.
"""
struct ROCompleteNondegenerateHopfCensus
    version::String
    system_declaration_sha256::String
    dynamics_binding_declaration_sha256::String
    parent_census_sha256::String
    formula_version::String
    normalization_version::String
    limits::ROHopfLyapunovLimits
    seeds::Tuple
    events::Tuple
    parent_event_count::Int
    nondegenerate_hopf_event_count::Int
    analysis_interval_operation_count::Int
    parent_census_replayed::Bool
    every_parent_event_lifted_exactly_once::Bool
    all_first_lyapunov_coefficients_nonzero_certified::Bool
    nondegenerate_local_hopf_event_set_complete_for_parent::Bool
    original_control_sides_certified::Bool
    periodic_orbit_incidence_certified::Bool
    full_state_periodic_orbit_stability_certified::Bool
    stable_root_population_complete::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROCompleteNondegenerateHopfCensus(
        ::_ROHLValidatedToken,
        version::String,
        system_declaration_sha256::String,
        dynamics_binding_declaration_sha256::String,
        parent_census_sha256::String,
        formula_version::String,
        normalization_version::String,
        limits::ROHopfLyapunovLimits,
        seeds::Tuple,
        events::Tuple,
        parent_event_count::Int,
        nondegenerate_hopf_event_count::Int,
        analysis_interval_operation_count::Int,
        parent_census_replayed::Bool,
        every_parent_event_lifted_exactly_once::Bool,
        all_first_lyapunov_coefficients_nonzero_certified::Bool,
        nondegenerate_local_hopf_event_set_complete_for_parent::Bool,
        original_control_sides_certified::Bool,
        periodic_orbit_incidence_certified::Bool,
        full_state_periodic_orbit_stability_certified::Bool,
        stable_root_population_complete::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_VERSION ||
            throw(ArgumentError(
                "complete nondegenerate-Hopf census version mismatch"))
        formula_version == RO_FIRST_LYAPUNOV_FORMULA_VERSION ||
            throw(ArgumentError("first-Lyapunov formula version mismatch"))
        normalization_version ==
            RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION ||
            throw(ArgumentError(
                "Hopf eigenvector normalization version mismatch"))
        for (hash, label) in (
            (system_declaration_sha256, "system_declaration_sha256"),
            (dynamics_binding_declaration_sha256,
                "dynamics_binding_declaration_sha256"),
            (parent_census_sha256, "parent_census_sha256"),
            (certificate_sha256, "certificate_sha256"),
        )
            _rors_validate_sha256(hash, label)
        end
        evidence_scope == RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_SCOPE ||
            throw(ArgumentError(
                "complete nondegenerate-Hopf evidence scope mismatch"))
        length(seeds) == length(events) == parent_event_count ==
            nondegenerate_hopf_event_count || throw(ArgumentError(
            "nondegenerate-Hopf population counts do not match"))
        _rohl_limit(:events, parent_event_count, limits.max_events)
        all(seed -> seed isa ROHopfLyapunovSeed, seeds) ||
            throw(ArgumentError(
                "nondegenerate-Hopf census contains an invalid seed"))
        all(event -> event isa ROFirstLyapunovHopfEvent, events) ||
            throw(ArgumentError(
                "nondegenerate-Hopf census contains an invalid event"))
        for index in eachindex(events)
            events[index].parent_event_certificate_sha256 ==
                seeds[index].event_certificate_sha256 &&
                events[index].seed_sha256 == seeds[index].seed_sha256 ||
                throw(ArgumentError(
                    "nondegenerate-Hopf event does not match its seed"))
        end
        allunique(seed.event_certificate_sha256 for seed in seeds) ||
            throw(ArgumentError(
                "nondegenerate-Hopf parent events must be unique"))
        for (value, label) in (
            (parent_census_replayed, "parent census replay"),
            (every_parent_event_lifted_exactly_once,
                "complete parent-event lift"),
            (all_first_lyapunov_coefficients_nonzero_certified,
                "nonzero first-Lyapunov population"),
            (nondegenerate_local_hopf_event_set_complete_for_parent,
                "complete nondegenerate-Hopf set"),
        )
            value || throw(ArgumentError(
                "nondegenerate-Hopf census lost $label"))
        end
        for (value, label) in (
            (original_control_sides_certified, "original-control sides"),
            (periodic_orbit_incidence_certified,
                "periodic-orbit incidence"),
            (full_state_periodic_orbit_stability_certified,
                "full-state periodic-orbit stability"),
            (stable_root_population_complete,
                "stable-root population completeness"),
            (global_continuation_certified, "global continuation"),
            (true_hysteresis_certified, "true hysteresis"),
        )
            value && throw(ArgumentError(
                "P8s1c1 cannot certify $label"))
        end
        analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "Hopf-Lyapunov operation count must be nonnegative"))
        _rohl_limit(
            :analysis_interval_operations,
            analysis_interval_operation_count,
            limits.max_analysis_interval_operations,
        )
        expected = _rohl_census_sha256(
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            parent_census_sha256,
            limits,
            seeds,
            events,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "complete nondegenerate-Hopf census hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            parent_census_sha256,
            formula_version,
            normalization_version,
            limits,
            seeds,
            events,
            parent_event_count,
            nondegenerate_hopf_event_count,
            analysis_interval_operation_count,
            true,
            true,
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            evidence_scope,
            certificate_sha256,
        )
    end
end

function _rohl_preconditioner_entry_count(seeds::Tuple)
    return sum((
        BigInt(length(seed.right_bordered_preconditioner.data)) +
        length(seed.adjoint_bordered_preconditioner.data) +
        length(seed.zero_resolvent_preconditioner.data) +
        length(seed.second_harmonic_preconditioner.data)
        for seed in seeds
    ); init=BigInt(0))
end

function _rohl_preflight_inputs(
    system::ROPolynomialEquilibriumSystem,
    parent_census::ROCompleteSimpleSpectralHopfEventCensus,
    seeds::Tuple,
    limits::ROHopfLyapunovLimits,
    cancel_check,
)
    parent_census.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "parent spectral-Hopf census belongs to a different system"))
    parent_count = parent_census.spectral_hopf_event_count
    _rohl_limit(:events, parent_count, limits.max_events)
    length(seeds) == parent_count || throw(ArgumentError(
        "exactly one Hopf-Lyapunov seed is required for every parent event"))
    all(seed -> seed isa ROHopfLyapunovSeed, seeds) ||
        throw(ArgumentError("all Hopf-Lyapunov seeds must be admitted seeds"))
    _rohl_limit(
        :parent_replay_interval_operations,
        parent_census.analysis_interval_operation_count,
        limits.max_parent_replay_interval_operations,
    )
    _rohl_limit(
        :analysis_interval_operations,
        parent_census.analysis_interval_operation_count,
        limits.max_analysis_interval_operations,
    )

    state_count = length(system.state_names)
    bordered_dimension = 2 * (state_count + 1)
    if parent_count > 0
        _rohl_limit(
            :realified_linear_dimension,
            max(bordered_dimension, 2 * state_count),
            limits.max_realified_linear_dimension,
        )
        tensor_entries = BigInt(state_count)^2 +
            BigInt(state_count)^3 + BigInt(state_count)^4
        _rohl_limit(
            :derivative_tensor_entries,
            tensor_entries,
            limits.max_derivative_tensor_entries,
        )
        _rohl_limit(
            :sqrt_bisection_steps,
            _ROHL_SQRT_BISECTION_STEPS,
            limits.max_sqrt_bisection_steps,
        )
    end
    preconditioner_entries = _rohl_preconditioner_entry_count(seeds)
    _rohl_limit(
        :preconditioner_entries,
        preconditioner_entries,
        limits.max_preconditioner_entries,
    )
    admission_operations = sum((
        _rohl_seed_admission_operation_count(seed) for seed in seeds
    ); init=BigInt(0))
    known_operations = BigInt(
        parent_census.analysis_interval_operation_count) +
        admission_operations
    _rohl_limit(
        :analysis_interval_operations,
        known_operations,
        limits.max_analysis_interval_operations,
    )

    seed_by_event = Dict{String,ROHopfLyapunovSeed}()
    for seed in seeds
        cancel_check()
        seed.state_count == state_count || throw(DimensionMismatch(
            "Hopf-Lyapunov seed state count does not match the system"))
        haskey(seed_by_event, seed.event_certificate_sha256) &&
            throw(ArgumentError(
                "duplicate Hopf-Lyapunov seed for one parent event"))
        seed.seed_sha256 == _rohl_seed_sha256(
            seed.event_certificate_sha256,
            seed.state_count,
            seed.anchor_state_index,
            seed.right_bordered_center,
            seed.right_bordered_remainder_box,
            seed.right_bordered_preconditioner,
            seed.adjoint_bordered_center,
            seed.adjoint_bordered_remainder_box,
            seed.adjoint_bordered_preconditioner,
            seed.zero_resolvent_center,
            seed.zero_resolvent_remainder_box,
            seed.zero_resolvent_preconditioner,
            seed.second_harmonic_center,
            seed.second_harmonic_remainder_box,
            seed.second_harmonic_preconditioner,
        ) || throw(ArgumentError("nested Hopf-Lyapunov seed hash mismatch"))
        seed_by_event[seed.event_certificate_sha256] = seed
    end
    canonical_seeds = ROHopfLyapunovSeed[]
    sizehint!(canonical_seeds, parent_count)
    for event in parent_census.events
        seed = get(seed_by_event, event.certificate_sha256, nothing)
        seed === nothing && throw(ArgumentError(
            "a parent spectral-Hopf event has no Hopf-Lyapunov seed"))
        push!(canonical_seeds, seed)
    end
    length(seed_by_event) == parent_count || throw(ArgumentError(
        "a Hopf-Lyapunov seed does not belong to the parent census"))
    return Tuple(canonical_seeds)
end

function _rohl_certify_event(
    event::ROSimpleSpectralHopfEvent,
    seed::ROHopfLyapunovSeed,
    derivative_polynomials,
    limits::ROHopfLyapunovLimits,
    context::_RORSContext,
)
    state_count = seed.state_count
    root_box = _rohl_event_root_box(event, context)
    frequency = _rohl_frequency_enclosure(
        event.frequency_squared_root_enclosure, limits, context)
    state_jacobian, bilinear, trilinear =
        _rohl_evaluate_derivatives(
            derivative_polynomials...,
            root_box,
            context,
        )

    right_matrix, right_rhs = _rohl_right_bordered_system(
        state_jacobian, frequency, seed.anchor_state_index, context)
    _, right_solution_tuple, right_beta = _rohl_certify_linear_system(
        "right bordered eigensystem",
        right_matrix,
        right_rhs,
        seed.right_bordered_center,
        seed.right_bordered_remainder_box,
        seed.right_bordered_preconditioner,
        limits,
        context,
    )
    right_solution = collect(right_solution_tuple)
    q_with_border = _rohl_complex_solution(
        right_solution, state_count + 1)
    q = q_with_border[1:state_count]

    adjoint_matrix, adjoint_rhs = _rohl_adjoint_bordered_system(
        state_jacobian,
        frequency,
        q,
        seed.anchor_state_index,
        context,
    )
    _, adjoint_solution_tuple, adjoint_beta =
        _rohl_certify_linear_system(
            "Hermitian adjoint bordered eigensystem",
            adjoint_matrix,
            adjoint_rhs,
            seed.adjoint_bordered_center,
            seed.adjoint_bordered_remainder_box,
            seed.adjoint_bordered_preconditioner,
            limits,
            context,
        )
    adjoint_solution = collect(adjoint_solution_tuple)
    p_with_border = _rohl_complex_solution(
        adjoint_solution, state_count + 1)
    p = p_with_border[1:state_count]

    zero_rhs = _rohl_b_q_qbar_real(bilinear, q, context)
    _, zero_solution_tuple, zero_beta = _rohl_certify_linear_system(
        "zero-frequency resolvent",
        _rohl_real_matrix_as_interval(state_jacobian),
        zero_rhs,
        seed.zero_resolvent_center,
        seed.zero_resolvent_remainder_box,
        seed.zero_resolvent_preconditioner,
        limits,
        context,
    )
    zero_solution = collect(zero_solution_tuple)

    second_rhs_complex = _rohl_second_harmonic_rhs(
        bilinear, q, context)
    second_matrix = _rohl_second_harmonic_matrix(
        state_jacobian, frequency, context)
    _, second_solution_tuple, second_beta =
        _rohl_certify_linear_system(
            "second-harmonic resolvent",
            second_matrix,
            _rohl_realify_vector(second_rhs_complex),
            seed.second_harmonic_center,
            seed.second_harmonic_remainder_box,
            seed.second_harmonic_preconditioner,
            limits,
            context,
        )
    second_solution = collect(second_solution_tuple)
    h20 = _rohl_complex_solution(second_solution, state_count)

    q_conjugate = [
        _rohl_complex_conjugate(context, value) for value in q
    ]
    c_term = _rohl_apply_trilinear(
        trilinear, q, q, q_conjugate, context)
    h11_complex = _rohl_real_vector_as_complex(zero_solution, context)
    b_q_h11 = _rohl_apply_bilinear(
        bilinear, q, h11_complex, context)
    b_qbar_h20 = _rohl_apply_bilinear(
        bilinear, q_conjugate, h20, context)
    two_interval = _rors_point(context, _RORSExact(2))
    normal_form_vector = Vector{_ROHLComplexInterval}(
        undef, state_count)
    for state in 1:state_count
        normal_form_vector[state] = _rohl_complex_add(
            context,
            _rohl_complex_subtract(
                context,
                c_term[state],
                _rohl_complex_real_scale(
                    context, two_interval, b_q_h11[state]),
            ),
            b_qbar_h20[state],
        )
    end
    pairing = _rohl_hermitian_inner_product(p, q, context)
    one(_RORSExact) in pairing.real &&
        _rors_contains_zero(pairing.imaginary) ||
        throw(ROHopfLyapunovRejected(
            :hermitian_adjoint_normalization_not_enclosed,
            "the certified adjoint/right pairing does not enclose one",
        ))
    g21 = _rohl_hermitian_inner_product(
        p, normal_form_vector, context)
    q_norm_squared = _rohl_q_norm_squared(q, context)
    q_norm_squared.lower > 0 || throw(ROHopfLyapunovRejected(
        :right_eigenvector_norm_not_positive,
        "the right-eigenvector norm enclosure reaches zero",
    ))
    denominator = _rors_multiply(
        context,
        _rors_multiply(context, two_interval, frequency),
        q_norm_squared,
    )
    first_lyapunov = _rohl_positive_divide(
        g21.real, denominator, context)
    !_rors_contains_zero(first_lyapunov) || throw(
        ROHopfLyapunovRejected(
            :first_lyapunov_coefficient_not_separated_from_zero,
            "the first-Lyapunov enclosure $first_lyapunov reaches zero (Bautin/degenerate or unresolved)",
        ))
    criticality = first_lyapunov.upper < 0 ?
        :supercritical : :subcritical
    return _rohl_make_event(
        event.certificate_sha256,
        seed.seed_sha256,
        Tuple(root_box),
        frequency,
        right_solution_tuple,
        adjoint_solution_tuple,
        zero_solution_tuple,
        second_solution_tuple,
        (right_beta, adjoint_beta, zero_beta, second_beta),
        q_norm_squared,
        pairing,
        g21,
        first_lyapunov,
        criticality,
    )
end

function _rohl_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    parent_census::ROCompleteSimpleSpectralHopfEventCensus,
    seeds::Tuple,
    limits::ROHopfLyapunovLimits,
    cancel_check,
)
    validate_ro_polynomial_equilibrium_system(system)
    canonical_seed_tuple = _rohl_preflight_inputs(
        system, parent_census, seeds, limits, cancel_check)
    replayed_parent =
        replay_ro_complete_simple_spectral_hopf_event_census(
            system, parent_census; cancel_check=cancel_check)
    replayed_parent.certificate_sha256 == parent_census.certificate_sha256 ||
        throw(ArgumentError("parent spectral-Hopf replay changed its hash"))

    parent_count = parent_census.spectral_hopf_event_count
    canonical_seeds = collect(canonical_seed_tuple)

    context_limits = _rohsc_regular_limits_with_operation_cap(
        system.limits, limits.max_analysis_interval_operations)
    context = _RORSContext(context_limits, cancel_check)
    context.operations = BigInt(parent_census.analysis_interval_operation_count)
    for seed in canonical_seeds
        admission_count = _rohl_seed_admission_operation_count(seed)
        admission_count <= typemax(Int) || throw(
            ROHopfLyapunovLimitExceeded(
                :analysis_interval_operations,
                admission_count,
                limits.max_analysis_interval_operations,
            ))
        _rors_tick!(context, Int(admission_count))
    end
    derivative_polynomials = parent_count == 0 ? nothing :
        _rohl_derivative_polynomials(system, limits, context)
    events = ROFirstLyapunovHopfEvent[]
    sizehint!(events, parent_count)
    for index in 1:parent_count
        context.cancel_check()
        push!(events, _rohl_certify_event(
            replayed_parent.events[index],
            canonical_seeds[index],
            derivative_polynomials,
            limits,
            context,
        ))
    end
    context.cancel_check()
    context.operations <= typemax(Int) || throw(
        ROHopfLyapunovLimitExceeded(
            :analysis_interval_operations,
            context.operations,
            limits.max_analysis_interval_operations,
        ))
    operation_count = Int(context.operations)
    seed_tuple = Tuple(canonical_seeds)
    event_tuple = Tuple(events)
    hash = _rohl_census_sha256(
        system.declaration_sha256,
        parent_census.dynamics_binding.declaration_sha256,
        parent_census.certificate_sha256,
        limits,
        seed_tuple,
        event_tuple,
        operation_count,
    )
    return ROCompleteNondegenerateHopfCensus(
        _ROHL_VALIDATED_TOKEN,
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_VERSION,
        system.declaration_sha256,
        parent_census.dynamics_binding.declaration_sha256,
        parent_census.certificate_sha256,
        RO_FIRST_LYAPUNOV_FORMULA_VERSION,
        RO_HOPF_EIGENVECTOR_NORMALIZATION_VERSION,
        limits,
        seed_tuple,
        event_tuple,
        parent_count,
        length(events),
        operation_count,
        true,
        true,
        true,
        true,
        false,
        false,
        false,
        false,
        false,
        false,
        RO_COMPLETE_NONDEGENERATE_HOPF_CENSUS_SCOPE,
        hash,
    )
end

"""
    certify_ro_complete_nondegenerate_hopf_census(system, parent, seeds; ...)

Replay a complete P8s1c0 parent census and lift every one of its spectral
events through strict bordered-eigenvector, adjoint, zero-resolvent,
second-harmonic-resolvent, and nonzero first-Lyapunov proofs.
"""
function certify_ro_complete_nondegenerate_hopf_census(
    system::ROPolynomialEquilibriumSystem,
    parent_census::ROCompleteSimpleSpectralHopfEventCensus,
    seeds;
    limits::ROHopfLyapunovLimits=ROHopfLyapunovLimits(),
    cancel_check=() -> nothing,
)
    seeds isa AbstractVector || seeds isa Tuple || throw(ArgumentError(
        "Hopf-Lyapunov seeds must be an ordered vector or tuple"))
    _rohl_limit(:events, length(seeds), limits.max_events)
    length(seeds) == parent_census.spectral_hopf_event_count ||
        throw(ArgumentError(
            "exactly one Hopf-Lyapunov seed is required for every parent event"))
    all(seed -> seed isa ROHopfLyapunovSeed, seeds) ||
        throw(ArgumentError("all Hopf-Lyapunov seeds must be admitted seeds"))
    try
        return _rohl_certify_exact(
            system, parent_census, Tuple(seeds), limits, cancel_check)
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROHopfLyapunovLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
end

function replay_ro_complete_nondegenerate_hopf_census(
    system::ROPolynomialEquilibriumSystem,
    parent_census::ROCompleteSimpleSpectralHopfEventCensus,
    certificate::ROCompleteNondegenerateHopfCensus;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "nondegenerate-Hopf census belongs to a different system"))
    certificate.parent_census_sha256 == parent_census.certificate_sha256 ||
        throw(ArgumentError(
            "nondegenerate-Hopf census belongs to a different parent census"))
    rebuilt = _rohl_certify_exact(
        system,
        parent_census,
        certificate.seeds,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "complete nondegenerate-Hopf census replay changed its hash"))
    return rebuilt
end

function validate_ro_complete_nondegenerate_hopf_census(
    system::ROPolynomialEquilibriumSystem,
    parent_census::ROCompleteSimpleSpectralHopfEventCensus,
    certificate::ROCompleteNondegenerateHopfCensus;
    cancel_check=() -> nothing,
)
    replay_ro_complete_nondegenerate_hopf_census(
        system,
        parent_census,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
