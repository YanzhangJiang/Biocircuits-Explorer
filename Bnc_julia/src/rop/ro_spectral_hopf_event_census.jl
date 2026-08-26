const RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_VERSION =
    "bne-ro-simple-spectral-hopf-event-census/v1.0.0"
const RO_SIMPLE_SPECTRAL_HOPF_EVENT_VERSION =
    "bne-ro-simple-spectral-hopf-event/v1.0.0"
const RO_SPECTRAL_HOPF_EVENT_CENSUS_CELL_VERSION =
    "bne-ro-spectral-hopf-event-census-cell/v1.0.0"
const RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_SCOPE =
    :complete_simple_spectral_hopf_events_inside_declared_domain

struct ROSpectralHopfEventCensusLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function _rohsc_regular_limits_with_operation_cap(
    limits::RORegularSheetLimits,
    operation_cap::Int,
)
    return _rofe_regular_limits_with_operation_cap(limits, operation_cap)
end

function _rohsc_parse_axes(
    raw,
    variable_count::Int,
    limits,
    context::_RORSContext,
)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "event_axis_breaks must be an ordered vector or tuple"))
    length(raw) == variable_count || throw(DimensionMismatch(
        "event_axis_breaks must match the spectral-Hopf variable count"))
    axes = Vector{Tuple}(undef, variable_count)
    cell_count = BigInt(1)
    for variable in 1:variable_count
        context.cancel_check()
        axis = raw[variable]
        axis isa AbstractVector || axis isa Tuple || throw(ArgumentError(
            "each spectral-Hopf axis must be a vector or tuple"))
        length(axis) >= 2 || throw(ArgumentError(
            "each spectral-Hopf axis requires at least two breakpoints"))
        _rohsc_limit(
            :axis_breakpoints_per_variable,
            length(axis),
            limits.max_axis_breakpoints_per_variable,
        )
        admitted = Vector{_RORSExact}(undef, length(axis))
        for index in eachindex(axis)
            _rors_tick!(context)
            admitted[index] = _rors_exact_float(
                axis[index], context.limits, "event_axis_breaks")
        end
        all(index -> admitted[index] < admitted[index + 1],
            1:(length(admitted) - 1)) || throw(ArgumentError(
            "spectral-Hopf axis breakpoints must be strictly increasing"))
        if variable < variable_count
            first(admitted) > 0 || throw(ArgumentError(
                "the declared state/control domain must stay positive"))
        else
            first(admitted) == 0 || throw(ArgumentError(
                "the frequency-squared axis must start exactly at zero"))
        end
        axes[variable] = Tuple(admitted)
        cell_count *= length(admitted) - 1
        _rohsc_limit(:cells, cell_count, limits.max_cells)
    end
    return Tuple(axes)
end

function _rohsc_admit_event_seeds(
    raw_indices,
    raw_preconditioners,
    event_axis_breaks::Tuple,
    limits,
    context::_RORSContext,
)
    raw_indices isa AbstractVector || raw_indices isa Tuple ||
        throw(ArgumentError(
            "event_grid_indices must be an ordered vector or tuple"))
    raw_preconditioners isa AbstractVector || raw_preconditioners isa Tuple ||
        throw(ArgumentError(
            "event_preconditioners must be an ordered vector or tuple"))
    length(raw_indices) == length(raw_preconditioners) ||
        throw(DimensionMismatch(
            "event_preconditioners must follow event_grid_indices"))
    _rohsc_limit(:events, length(raw_indices), limits.max_events)
    variable_count = length(event_axis_breaks)
    pairs = Vector{Tuple{Tuple,Matrix{_RORSExact}}}()
    sizehint!(pairs, length(raw_indices))
    for seed in eachindex(raw_indices)
        context.cancel_check()
        raw_index = raw_indices[seed]
        raw_index isa AbstractVector || raw_index isa Tuple ||
            throw(ArgumentError(
                "each event grid index must be a vector or tuple"))
        length(raw_index) == variable_count || throw(DimensionMismatch(
            "event grid index $seed has the wrong dimension"))
        grid_index = Vector{Int}(undef, variable_count)
        for variable in 1:variable_count
            value = raw_index[variable]
            value isa Integer || throw(ArgumentError(
                "event grid indices must contain integers"))
            1 <= value <= length(event_axis_breaks[variable]) - 1 ||
                throw(ArgumentError(
                    "event grid index $seed is outside the partition"))
            value <= typemax(Int) || throw(ArgumentError(
                "event grid index $seed exceeds Int"))
            grid_index[variable] = Int(value)
        end
        preconditioner = _rors_exact_matrix(
            raw_preconditioners[seed],
            variable_count,
            variable_count,
            "event_preconditioners[$seed]",
            context,
        )
        push!(pairs, (Tuple(grid_index), preconditioner))
    end
    cell_dimensions = Tuple(length(axis) - 1 for axis in event_axis_breaks)
    linear_indices = LinearIndices(cell_dimensions)
    sort!(pairs; by=pair -> linear_indices[CartesianIndex(pair[1])])
    indices = [pair[1] for pair in pairs]
    allunique(indices) || throw(ArgumentError(
        "event_grid_indices must be unique"))
    return Tuple(indices), [pair[2] for pair in pairs]
end

function _rohsc_admission_operation_count(
    event_axis_breaks::Tuple,
    event_preconditioners,
)
    variable_count = length(event_axis_breaks)
    axis_operations = sum(
        (BigInt(length(axis)) for axis in event_axis_breaks);
        init=BigInt(0),
    )
    matrix_operations = BigInt(length(event_preconditioners)) *
        variable_count * variable_count
    return axis_operations + matrix_operations
end

function _rohsc_system_polynomials(
    system::ROPolynomialEquilibriumSystem,
    context::_RORSContext,
)
    state_count = length(system.state_names)
    variable_count = state_count + 2
    result = Vector{_RORSPolynomial}(undef, state_count)
    for equation in 1:state_count
        context.cancel_check()
        polynomial = _RORSPolynomial()
        for term in system.equations[equation]
            coefficient = _rors_exact_float(
                term.coefficient,
                context.limits,
                "polynomial coefficient",
            )
            exponents = Tuple((
                term.state_exponents...,
                term.control_exponents[1],
                0,
            ))
            length(exponents) == variable_count || throw(DimensionMismatch(
                "spectral-Hopf polynomial exponent count mismatch"))
            _rors_polynomial_add_term!(
                polynomial, exponents, coefficient, context)
        end
        result[equation] = polynomial
    end
    return result
end

function _rohsc_state_jacobian_polynomials(
    residual::Vector{_RORSPolynomial},
    state_count::Int,
    context::_RORSContext,
)
    length(residual) == state_count || throw(DimensionMismatch(
        "spectral-Hopf residual count does not match the state count"))
    jacobian = Matrix{_RORSPolynomial}(
        undef, state_count, state_count)
    for equation in 1:state_count, state in 1:state_count
        jacobian[equation, state] = _rors_polynomial_derivative(
            residual[equation], state, context)
    end
    return jacobian
end

function _rohsc_extend_polynomial(
    polynomial::_RORSPolynomial,
    context::_RORSContext,
)
    extended = _RORSPolynomial()
    for exponents in sort!(collect(keys(polynomial)))
        _rors_polynomial_add_term!(
            extended,
            Tuple((exponents..., 0)),
            polynomial[exponents],
            context,
        )
    end
    return extended
end

function _rohsc_characteristic_even_odd(
    state_jacobian::Matrix{_RORSPolynomial},
    variable_count::Int,
    context::_RORSContext,
)
    state_count, column_count = size(state_jacobian)
    state_count == column_count && state_count >= 2 ||
        throw(DimensionMismatch(
            "spectral-Hopf characteristic polynomial needs a square state Jacobian"))
    formal_variable_count = variable_count + 1
    s_polynomial = _RORSPolynomial()
    s_exponents = zeros(Int, formal_variable_count)
    s_exponents[end] = 1
    _rors_polynomial_add_term!(
        s_polynomial,
        Tuple(s_exponents),
        one(_RORSExact),
        context,
    )
    characteristic_matrix = Matrix{_RORSPolynomial}(
        undef, state_count, state_count)
    for row in 1:state_count, column in 1:state_count
        entry = _RORSPolynomial()
        row == column && _rors_polynomial_accumulate!(
            entry, s_polynomial, context)
        extended = _rohsc_extend_polynomial(
            state_jacobian[row, column], context)
        _rors_polynomial_scaled_accumulate!(
            entry, extended, -one(_RORSExact), context)
        characteristic_matrix[row, column] = entry
    end
    characteristic = _rofe_polynomial_determinant(
        characteristic_matrix, context)
    even_polynomial = _RORSPolynomial()
    odd_polynomial = _RORSPolynomial()
    for exponents in sort!(collect(keys(characteristic)))
        length(exponents) == formal_variable_count ||
            throw(DimensionMismatch(
                "characteristic-polynomial exponent count mismatch"))
        s_power = exponents[end]
        z_power = div(s_power, 2)
        reduced_exponents = collect(exponents[1:end-1])
        reduced_exponents[end] += z_power
        sign = isodd(z_power) ? -one(_RORSExact) : one(_RORSExact)
        coefficient = _rors_exact_multiply(
            context, sign, characteristic[exponents])
        target = iseven(s_power) ? even_polynomial : odd_polynomial
        _rors_polynomial_add_term!(
            target, Tuple(reduced_exponents), coefficient, context)
    end
    return even_polynomial, odd_polynomial
end

function _rohsc_augmented_polynomials(
    system::ROPolynomialEquilibriumSystem,
    context::_RORSContext,
)
    residual = _rohsc_system_polynomials(system, context)
    state_count = length(residual)
    variable_count = state_count + 2
    state_jacobian = _rohsc_state_jacobian_polynomials(
        residual, state_count, context)
    state_determinant = _rofe_polynomial_determinant(
        state_jacobian, context)
    even_polynomial, odd_polynomial =
        _rohsc_characteristic_even_odd(
            state_jacobian, variable_count, context)
    augmented = vcat(residual, [even_polynomial, odd_polynomial])
    length(augmented) == variable_count || throw(DimensionMismatch(
        "spectral-Hopf augmented polynomial system is not square"))
    return augmented, state_jacobian, state_determinant
end

function Base.showerror(io::IO, err::ROSpectralHopfEventCensusLimitExceeded)
    print(
        io,
        "spectral-Hopf event census limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct ROSpectralHopfEventCensusRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROSpectralHopfEventCensusRejected)
    print(
        io,
        "spectral-Hopf event census rejected (",
        err.reason,
        "): ",
        err.detail,
    )
end

@inline function _rohsc_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "spectral-Hopf requested work must be nonnegative"))
    amount <= limit || throw(
        ROSpectralHopfEventCensusLimitExceeded(phase, amount, limit))
    return nothing
end

"""Hard tensor-population and exact-operation limits for P8s1c0."""
struct ROSpectralHopfEventCensusLimits
    max_axis_breakpoints_per_variable::Int
    max_cells::Int
    max_events::Int
    max_projection_pairs::Int
    max_interval_operations::Int

    function ROSpectralHopfEventCensusLimits(
        max_axis_breakpoints_per_variable::Int,
        max_cells::Int,
        max_events::Int,
        max_projection_pairs::Int,
        max_interval_operations::Int,
    )
        max_axis_breakpoints_per_variable >= 2 || throw(ArgumentError(
            "max_axis_breakpoints_per_variable must be at least two"))
        max_cells > 0 || throw(ArgumentError(
            "max_cells must be positive"))
        max_events >= 0 || throw(ArgumentError(
            "max_events must be nonnegative"))
        max_projection_pairs >= 0 || throw(ArgumentError(
            "max_projection_pairs must be nonnegative"))
        max_interval_operations > 0 || throw(ArgumentError(
            "max_interval_operations must be positive"))
        return new(
            max_axis_breakpoints_per_variable,
            max_cells,
            max_events,
            max_projection_pairs,
            max_interval_operations,
        )
    end
end

function ROSpectralHopfEventCensusLimits(;
    max_axis_breakpoints_per_variable::Integer=64,
    max_cells::Integer=4_096,
    max_events::Integer=256,
    max_projection_pairs::Integer=32_640,
    max_interval_operations::Integer=2_000_000,
)
    values = (
        max_axis_breakpoints_per_variable,
        max_cells,
        max_events,
        max_projection_pairs,
        max_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError(
            "spectral-Hopf census limits must fit Int"))
    return ROSpectralHopfEventCensusLimits(Int.(values)...)
end

function _rohsc_write_limits(
    io::IO,
    limits::ROSpectralHopfEventCensusLimits,
)
    for value in (
        limits.max_axis_breakpoints_per_variable,
        limits.max_cells,
        limits.max_events,
        limits.max_projection_pairs,
        limits.max_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

struct _ROHSCValidatedToken end
const _ROHSC_VALIDATED_TOKEN = _ROHSCValidatedToken()

function _rohsc_event_sha256(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    center::Tuple,
    preconditioner::ROExactMatrix,
    krawczyk_offset_image::Tuple,
    augmented_residual_enclosure::Tuple,
    augmented_jacobian_enclosure::ROExactIntervalMatrix,
    state_jacobian_determinant_enclosure::ROExactInterval,
    frequency_squared_root_enclosure::ROExactInterval,
    contraction_beta::_RORSExact,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SIMPLE_SPECTRAL_HOPF_EVENT_VERSION)
    _rors_write_token(io, linear_index)
    _rors_write_token(io, length(grid_index))
    for index in grid_index
        _rors_write_token(io, index)
    end
    _rors_write_interval_vector(io, event_box)
    _rors_write_exact_vector(io, center)
    _rors_write_exact_matrix(io, preconditioner)
    _rors_write_interval_vector(io, krawczyk_offset_image)
    _rors_write_interval_vector(io, augmented_residual_enclosure)
    _rors_write_interval_matrix(io, augmented_jacobian_enclosure)
    _rors_write_interval(io, state_jacobian_determinant_enclosure)
    _rors_write_interval(io, frequency_squared_root_enclosure)
    _rors_write_exact(io, contraction_beta)
    for value in (
        true,  # unique_augmented_root_inside_event_box
        true,  # equilibrium_regular_certified
        true,  # nonzero_frequency_certified
        true,  # simple_conjugate_pair_certified
        true,  # real_part_transversality_certified
        false, # no_other_imaginary_pair_at_same_equilibrium_certified
        false, # first_lyapunov_coefficient_nonzero_certified
        false, # nonlinear_hopf_bifurcation_certified
        false, # periodic_orbit_incidence_certified
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
One isolated root of `(F,E,O)` for the even/odd characteristic-polynomial
decomposition `det(i*omega*I-F_x)=E(omega^2)+i*omega*O(omega^2)`.

This is a simple transverse spectral crossing. Absence of another imaginary
pair at the same equilibrium is a parent-census conclusion, not authority of
this local event hash. This is not yet a nonlinear Hopf bifurcation or a
periodic-orbit incidence certificate.
"""
struct ROSimpleSpectralHopfEvent
    version::String
    linear_index::Int
    grid_index::Tuple
    event_box::Tuple
    center::Tuple
    preconditioner::ROExactMatrix
    krawczyk_offset_image::Tuple
    augmented_residual_enclosure::Tuple
    augmented_jacobian_enclosure::ROExactIntervalMatrix
    state_jacobian_determinant_enclosure::ROExactInterval
    frequency_squared_root_enclosure::ROExactInterval
    contraction_beta::_RORSExact
    unique_augmented_root_inside_event_box::Bool
    equilibrium_regular_certified::Bool
    nonzero_frequency_certified::Bool
    simple_conjugate_pair_certified::Bool
    real_part_transversality_certified::Bool
    no_other_imaginary_pair_at_same_equilibrium_certified::Bool
    first_lyapunov_coefficient_nonzero_certified::Bool
    nonlinear_hopf_bifurcation_certified::Bool
    periodic_orbit_incidence_certified::Bool
    certificate_sha256::String

    function ROSimpleSpectralHopfEvent(
        ::_ROHSCValidatedToken,
        version::String,
        linear_index::Int,
        grid_index::Tuple,
        event_box::Tuple,
        center::Tuple,
        preconditioner::ROExactMatrix,
        krawczyk_offset_image::Tuple,
        augmented_residual_enclosure::Tuple,
        augmented_jacobian_enclosure::ROExactIntervalMatrix,
        state_jacobian_determinant_enclosure::ROExactInterval,
        frequency_squared_root_enclosure::ROExactInterval,
        contraction_beta::_RORSExact,
        unique_augmented_root_inside_event_box::Bool,
        equilibrium_regular_certified::Bool,
        nonzero_frequency_certified::Bool,
        simple_conjugate_pair_certified::Bool,
        real_part_transversality_certified::Bool,
        no_other_imaginary_pair_at_same_equilibrium_certified::Bool,
        first_lyapunov_coefficient_nonzero_certified::Bool,
        nonlinear_hopf_bifurcation_certified::Bool,
        periodic_orbit_incidence_certified::Bool,
        certificate_sha256::String,
    )
        version == RO_SIMPLE_SPECTRAL_HOPF_EVENT_VERSION ||
            throw(ArgumentError("spectral-Hopf event version mismatch"))
        linear_index > 0 || throw(ArgumentError(
            "spectral-Hopf event linear index must be positive"))
        variable_count = length(grid_index)
        variable_count >= 4 || throw(ArgumentError(
            "a spectral-Hopf event requires at least two states, one control, and frequency squared"))
        all(index -> index isa Int && index > 0, grid_index) ||
            throw(ArgumentError(
                "spectral-Hopf grid indices must be positive Int values"))
        length(event_box) == variable_count &&
            length(center) == variable_count &&
            length(krawczyk_offset_image) == variable_count &&
            length(augmented_residual_enclosure) == variable_count ||
            throw(DimensionMismatch(
                "spectral-Hopf event dimensions do not match"))
        all(interval -> interval isa ROExactInterval, event_box) &&
            all(interval -> interval isa ROExactInterval,
                krawczyk_offset_image) &&
            all(interval -> interval isa ROExactInterval,
                augmented_residual_enclosure) || throw(ArgumentError(
            "spectral-Hopf event enclosures must be exact intervals"))
        all(value -> value isa _RORSExact, center) || throw(ArgumentError(
            "spectral-Hopf event center must be exact"))
        size(preconditioner) == (variable_count, variable_count) ||
            throw(DimensionMismatch(
                "spectral-Hopf preconditioner has the wrong shape"))
        size(augmented_jacobian_enclosure) ==
            (variable_count, variable_count) || throw(DimensionMismatch(
            "spectral-Hopf augmented Jacobian has the wrong shape"))
        for variable in 1:variable_count
            box = event_box[variable]
            variable < variable_count && box.lower <= 0 &&
                throw(ArgumentError(
                    "state/control spectral-Hopf boxes must be positive"))
            variable == variable_count && box.lower < 0 &&
                throw(ArgumentError(
                    "frequency-squared boxes must be nonnegative"))
            box.lower < box.upper || throw(ArgumentError(
                "spectral-Hopf event boxes must have positive width"))
            center[variable] == (box.lower + box.upper) / 2 ||
                throw(ArgumentError(
                    "spectral-Hopf center is not the exact box midpoint"))
            centered = ROExactInterval(
                box.lower - center[variable],
                box.upper - center[variable],
                Val(:validated),
            )
            _rors_strict_subset(
                krawczyk_offset_image[variable], centered) ||
                throw(ArgumentError(
                    "spectral-Hopf Krawczyk image is not strictly interior"))
        end
        all(_rors_contains_zero, augmented_residual_enclosure) ||
            throw(ArgumentError(
                "spectral-Hopf residual enclosure must contain zero"))
        !_rors_contains_zero(state_jacobian_determinant_enclosure) ||
            throw(ArgumentError(
                "spectral-Hopf event does not exclude a zero state eigenvalue"))
        frequency_squared_root_enclosure.lower > 0 || throw(ArgumentError(
            "spectral-Hopf frequency must be strictly nonzero"))
        _rors_strict_subset(
            frequency_squared_root_enclosure,
            event_box[end],
        ) || throw(ArgumentError(
            "spectral-Hopf frequency root is not strictly inside its cell"))
        0 <= contraction_beta < 1 || throw(ArgumentError(
            "spectral-Hopf contraction beta must lie in [0,1)"))
        for (value, label) in (
            (unique_augmented_root_inside_event_box,
                "augmented-root uniqueness"),
            (equilibrium_regular_certified, "equilibrium regularity"),
            (nonzero_frequency_certified, "nonzero frequency"),
            (simple_conjugate_pair_certified, "simple conjugate pair"),
            (real_part_transversality_certified,
                "real-part transversality"),
        )
            value || throw(ArgumentError(
                "spectral-Hopf event lost $label"))
        end
        no_other_imaginary_pair_at_same_equilibrium_certified &&
            throw(ArgumentError(
                "a local event cannot exclude another parent-census frequency"))
        first_lyapunov_coefficient_nonzero_certified &&
            throw(ArgumentError(
                "P8s1c0 does not certify the first Lyapunov coefficient"))
        nonlinear_hopf_bifurcation_certified && throw(ArgumentError(
            "P8s1c0 is not a nonlinear Hopf-bifurcation certificate"))
        periodic_orbit_incidence_certified && throw(ArgumentError(
            "P8s1c0 does not attach a periodic-orbit branch"))
        _rors_validate_sha256(certificate_sha256, "certificate_sha256")
        expected = _rohsc_event_sha256(
            linear_index,
            grid_index,
            event_box,
            center,
            preconditioner,
            krawczyk_offset_image,
            augmented_residual_enclosure,
            augmented_jacobian_enclosure,
            state_jacobian_determinant_enclosure,
            frequency_squared_root_enclosure,
            contraction_beta,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "spectral-Hopf event hash mismatch"))
        return new(
            version,
            linear_index,
            grid_index,
            event_box,
            center,
            preconditioner,
            krawczyk_offset_image,
            augmented_residual_enclosure,
            augmented_jacobian_enclosure,
            state_jacobian_determinant_enclosure,
            frequency_squared_root_enclosure,
            contraction_beta,
            true,
            true,
            true,
            true,
            true,
            false,
            false,
            false,
            false,
            certificate_sha256,
        )
    end
end

function _rohsc_make_event(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    center::Tuple,
    preconditioner::ROExactMatrix,
    krawczyk_offset_image::Tuple,
    augmented_residual_enclosure::Tuple,
    augmented_jacobian_enclosure::ROExactIntervalMatrix,
    state_jacobian_determinant_enclosure::ROExactInterval,
    frequency_squared_root_enclosure::ROExactInterval,
    contraction_beta::_RORSExact,
)
    hash = _rohsc_event_sha256(
        linear_index,
        grid_index,
        event_box,
        center,
        preconditioner,
        krawczyk_offset_image,
        augmented_residual_enclosure,
        augmented_jacobian_enclosure,
        state_jacobian_determinant_enclosure,
        frequency_squared_root_enclosure,
        contraction_beta,
    )
    return ROSimpleSpectralHopfEvent(
        _ROHSC_VALIDATED_TOKEN,
        RO_SIMPLE_SPECTRAL_HOPF_EVENT_VERSION,
        linear_index,
        grid_index,
        event_box,
        center,
        preconditioner,
        krawczyk_offset_image,
        augmented_residual_enclosure,
        augmented_jacobian_enclosure,
        state_jacobian_determinant_enclosure,
        frequency_squared_root_enclosure,
        contraction_beta,
        true,
        true,
        true,
        true,
        true,
        false,
        false,
        false,
        false,
        hash,
    )
end

function _rohsc_cell_sha256(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    classification::Symbol,
    excluding_augmented_equation_index::Int,
    augmented_residual_enclosure::Tuple,
    event_certificate_sha256::String,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SPECTRAL_HOPF_EVENT_CENSUS_CELL_VERSION)
    _rors_write_token(io, linear_index)
    _rors_write_token(io, length(grid_index))
    for index in grid_index
        _rors_write_token(io, index)
    end
    _rors_write_interval_vector(io, event_box)
    _rors_write_token(io, classification)
    _rors_write_token(io, excluding_augmented_equation_index)
    _rors_write_interval_vector(io, augmented_residual_enclosure)
    _rors_write_token(io, event_certificate_sha256)
    return bytes2hex(SHA.sha256(take!(io)))
end

"""One exhaustive tensor-cell decision for the P8s1c0 augmented system."""
struct ROSpectralHopfEventCensusCell
    version::String
    linear_index::Int
    grid_index::Tuple
    event_box::Tuple
    classification::Symbol
    excluding_augmented_equation_index::Int
    augmented_residual_enclosure::Tuple
    event_certificate_sha256::String
    evidence_sha256::String

    function ROSpectralHopfEventCensusCell(
        ::_ROHSCValidatedToken,
        version::String,
        linear_index::Int,
        grid_index::Tuple,
        event_box::Tuple,
        classification::Symbol,
        excluding_augmented_equation_index::Int,
        augmented_residual_enclosure::Tuple,
        event_certificate_sha256::String,
        evidence_sha256::String,
    )
        version == RO_SPECTRAL_HOPF_EVENT_CENSUS_CELL_VERSION ||
            throw(ArgumentError(
                "spectral-Hopf census cell version mismatch"))
        linear_index > 0 || throw(ArgumentError(
            "spectral-Hopf census cell index must be positive"))
        variable_count = length(grid_index)
        variable_count >= 4 &&
            all(index -> index isa Int && index > 0, grid_index) ||
            throw(ArgumentError(
                "spectral-Hopf grid indices must be positive Int values"))
        length(event_box) == variable_count &&
            length(augmented_residual_enclosure) == variable_count ||
            throw(DimensionMismatch(
                "spectral-Hopf cell dimensions do not match"))
        all(interval -> interval isa ROExactInterval, event_box) &&
            all(interval -> interval isa ROExactInterval,
                augmented_residual_enclosure) || throw(ArgumentError(
            "spectral-Hopf cell enclosures must be exact intervals"))
        classification in (
            :unique_simple_spectral_hopf_event,
            :spectral_hopf_free_by_augmented_residual_exclusion,
        ) || throw(ArgumentError(
            "unknown spectral-Hopf cell classification"))
        if classification == :unique_simple_spectral_hopf_event
            excluding_augmented_equation_index == 0 ||
                throw(ArgumentError(
                    "an event cell cannot name an excluding equation"))
            _rors_validate_sha256(
                event_certificate_sha256,
                "event_certificate_sha256",
            )
            all(_rors_contains_zero, augmented_residual_enclosure) ||
                throw(ArgumentError(
                    "an event-cell residual enclosure must contain zero"))
        else
            1 <= excluding_augmented_equation_index <= variable_count ||
                throw(ArgumentError(
                    "a free cell must name one excluding equation"))
            isempty(event_certificate_sha256) || throw(ArgumentError(
                "a free cell cannot bind an event certificate"))
            !_rors_contains_zero(augmented_residual_enclosure[
                excluding_augmented_equation_index]) ||
                throw(ArgumentError(
                    "the named augmented equation does not exclude zero"))
        end
        _rors_validate_sha256(evidence_sha256, "cell evidence_sha256")
        expected = _rohsc_cell_sha256(
            linear_index,
            grid_index,
            event_box,
            classification,
            excluding_augmented_equation_index,
            augmented_residual_enclosure,
            event_certificate_sha256,
        )
        evidence_sha256 == expected || throw(ArgumentError(
            "spectral-Hopf cell hash mismatch"))
        return new(
            version,
            linear_index,
            grid_index,
            event_box,
            classification,
            excluding_augmented_equation_index,
            augmented_residual_enclosure,
            event_certificate_sha256,
            evidence_sha256,
        )
    end
end

function _rohsc_make_cell(
    linear_index::Int,
    grid_index::Tuple,
    event_box::Tuple,
    classification::Symbol,
    excluding_augmented_equation_index::Int,
    augmented_residual_enclosure::Tuple,
    event_certificate_sha256::String,
)
    hash = _rohsc_cell_sha256(
        linear_index,
        grid_index,
        event_box,
        classification,
        excluding_augmented_equation_index,
        augmented_residual_enclosure,
        event_certificate_sha256,
    )
    return ROSpectralHopfEventCensusCell(
        _ROHSC_VALIDATED_TOKEN,
        RO_SPECTRAL_HOPF_EVENT_CENSUS_CELL_VERSION,
        linear_index,
        grid_index,
        event_box,
        classification,
        excluding_augmented_equation_index,
        augmented_residual_enclosure,
        event_certificate_sha256,
        hash,
    )
end

function _rohsc_census_sha256(
    system_declaration_sha256::String,
    dynamics_binding_declaration_sha256::String,
    limits::ROSpectralHopfEventCensusLimits,
    augmented_variable_names::Tuple,
    augmented_variable_units::Tuple,
    event_axis_breaks::Tuple,
    declared_event_box::Tuple,
    uniform_spectral_radius_upper_bound::_RORSExact,
    frequency_squared_coverage_upper_bound::_RORSExact,
    event_grid_indices::Tuple,
    event_preconditioners::Tuple,
    events::Tuple,
    cells::Tuple,
    partition_cell_count::Int,
    spectral_hopf_event_count::Int,
    spectral_hopf_free_cell_count::Int,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_VERSION)
    _rors_write_token(io, system_declaration_sha256)
    _rors_write_token(io, dynamics_binding_declaration_sha256)
    _rohsc_write_limits(io, limits)
    for values in (augmented_variable_names, augmented_variable_units)
        _rors_write_token(io, length(values))
        for value in values
            _rors_write_token(io, value)
        end
    end
    _rors_write_token(io, length(event_axis_breaks))
    for axis in event_axis_breaks
        _rors_write_exact_vector(io, axis)
    end
    _rors_write_interval_vector(io, declared_event_box)
    _rors_write_exact(io, uniform_spectral_radius_upper_bound)
    _rors_write_exact(io, frequency_squared_coverage_upper_bound)
    _rors_write_token(io, length(event_grid_indices))
    for grid_index in event_grid_indices
        _rors_write_token(io, length(grid_index))
        for index in grid_index
            _rors_write_token(io, index)
        end
    end
    _rors_write_token(io, length(event_preconditioners))
    for preconditioner in event_preconditioners
        _rors_write_exact_matrix(io, preconditioner)
    end
    _rors_write_token(io, length(events))
    for event in events
        _rors_write_token(io, event.certificate_sha256)
    end
    _rors_write_token(io, length(cells))
    for cell in cells
        _rors_write_token(io, cell.evidence_sha256)
    end
    for value in (
        partition_cell_count,
        spectral_hopf_event_count,
        spectral_hopf_free_cell_count,
        analysis_interval_operation_count,
    )
        _rors_write_token(io, value)
    end
    for value in (
        true,  # frequency_domain_complete_for_declared_state_control_box
        true,  # spectral_hopf_event_set_complete_inside_declared_domain
        true,  # all_events_equilibrium_regular
        true,  # all_events_have_one_simple_conjugate_pair
        true,  # all_events_real_part_transverse
        true,  # event_state_control_projections_pairwise_disjoint
        false, # first_lyapunov_coefficients_nonzero_certified
        false, # nonlinear_hopf_bifurcations_certified
        false, # periodic_orbit_incidence_certified
        false, # stable_root_population_complete
        false, # global_continuation_certified
        false, # native_residuals_certified
        false, # true_hysteresis_certified
        RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Complete P8s1c0 census of isolated simple spectral-Hopf events inside one
declared positive state/control box and a frequency-squared axis proven to
cover every eigenvalue by an exact row-sum bound.

The positive conclusion is spectral. First-Lyapunov, periodic-orbit, stability,
global-continuation, and hysteresis claims remain false.
"""
struct ROCompleteSimpleSpectralHopfEventCensus
    version::String
    system_declaration_sha256::String
    dynamics_binding::ROPolynomialDynamicsBinding
    limits::ROSpectralHopfEventCensusLimits
    augmented_variable_names::Tuple
    augmented_variable_units::Tuple
    event_axis_breaks::Tuple
    declared_event_box::Tuple
    uniform_spectral_radius_upper_bound::_RORSExact
    frequency_squared_coverage_upper_bound::_RORSExact
    event_grid_indices::Tuple
    event_preconditioners::Tuple
    events::Tuple
    cells::Tuple
    partition_cell_count::Int
    spectral_hopf_event_count::Int
    spectral_hopf_free_cell_count::Int
    analysis_interval_operation_count::Int
    frequency_domain_complete_for_declared_state_control_box::Bool
    spectral_hopf_event_set_complete_inside_declared_domain::Bool
    all_events_equilibrium_regular::Bool
    all_events_have_one_simple_conjugate_pair::Bool
    all_events_real_part_transverse::Bool
    event_state_control_projections_pairwise_disjoint::Bool
    first_lyapunov_coefficients_nonzero_certified::Bool
    nonlinear_hopf_bifurcations_certified::Bool
    periodic_orbit_incidence_certified::Bool
    stable_root_population_complete::Bool
    global_continuation_certified::Bool
    native_residuals_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROCompleteSimpleSpectralHopfEventCensus(
        ::_ROHSCValidatedToken,
        version::String,
        system_declaration_sha256::String,
        dynamics_binding::ROPolynomialDynamicsBinding,
        limits::ROSpectralHopfEventCensusLimits,
        augmented_variable_names::Tuple,
        augmented_variable_units::Tuple,
        event_axis_breaks::Tuple,
        declared_event_box::Tuple,
        uniform_spectral_radius_upper_bound::_RORSExact,
        frequency_squared_coverage_upper_bound::_RORSExact,
        event_grid_indices::Tuple,
        event_preconditioners::Tuple,
        events::Tuple,
        cells::Tuple,
        partition_cell_count::Int,
        spectral_hopf_event_count::Int,
        spectral_hopf_free_cell_count::Int,
        analysis_interval_operation_count::Int,
        frequency_domain_complete_for_declared_state_control_box::Bool,
        spectral_hopf_event_set_complete_inside_declared_domain::Bool,
        all_events_equilibrium_regular::Bool,
        all_events_have_one_simple_conjugate_pair::Bool,
        all_events_real_part_transverse::Bool,
        event_state_control_projections_pairwise_disjoint::Bool,
        first_lyapunov_coefficients_nonzero_certified::Bool,
        nonlinear_hopf_bifurcations_certified::Bool,
        periodic_orbit_incidence_certified::Bool,
        stable_root_population_complete::Bool,
        global_continuation_certified::Bool,
        native_residuals_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_VERSION ||
            throw(ArgumentError(
                "simple spectral-Hopf census version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        dynamics_binding.system_declaration_sha256 ==
            system_declaration_sha256 || throw(ArgumentError(
            "spectral-Hopf dynamics binding belongs to a different system"))
        _rors_validate_sha256(certificate_sha256, "certificate_sha256")
        evidence_scope == RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_SCOPE ||
            throw(ArgumentError(
                "simple spectral-Hopf evidence scope mismatch"))
        for (value, label) in (
            (frequency_domain_complete_for_declared_state_control_box,
                "frequency-domain completeness"),
            (spectral_hopf_event_set_complete_inside_declared_domain,
                "event-set completeness"),
            (all_events_equilibrium_regular,
                "event equilibrium regularity"),
            (all_events_have_one_simple_conjugate_pair,
                "simple-pair completeness"),
            (all_events_real_part_transverse,
                "real-part transversality"),
            (event_state_control_projections_pairwise_disjoint,
                "event projection separation"),
        )
            value || throw(ArgumentError(
                "spectral-Hopf census lost $label"))
        end
        for (value, label) in (
            (first_lyapunov_coefficients_nonzero_certified,
                "first Lyapunov coefficient"),
            (nonlinear_hopf_bifurcations_certified,
                "nonlinear Hopf bifurcation"),
            (periodic_orbit_incidence_certified,
                "periodic-orbit incidence"),
            (stable_root_population_complete,
                "stable-root population"),
            (global_continuation_certified,
                "global continuation"),
            (native_residuals_certified, "native residuals"),
            (true_hysteresis_certified, "true hysteresis"),
        )
            value && throw(ArgumentError(
                "P8s1c0 cannot certify $label"))
        end
        variable_count = length(augmented_variable_names)
        variable_count >= 4 || throw(ArgumentError(
            "a spectral-Hopf census requires at least four augmented variables"))
        length(augmented_variable_units) == variable_count &&
            length(event_axis_breaks) == variable_count &&
            length(declared_event_box) == variable_count ||
            throw(DimensionMismatch(
                "spectral-Hopf census dimensions do not match"))
        all(value -> value isa String, augmented_variable_names) &&
            all(value -> value isa String, augmented_variable_units) ||
            throw(ArgumentError(
                "spectral-Hopf names and units must be strings"))
        allunique(augmented_variable_names) || throw(ArgumentError(
            "spectral-Hopf augmented variable names must be unique"))
        cell_dimensions = Vector{Int}(undef, variable_count)
        cell_count_big = BigInt(1)
        for variable in 1:variable_count
            axis = event_axis_breaks[variable]
            length(axis) >= 2 || throw(ArgumentError(
                "each spectral-Hopf axis needs at least two breakpoints"))
            _rohsc_limit(
                :axis_breakpoints_per_variable,
                length(axis),
                limits.max_axis_breakpoints_per_variable,
            )
            all(value -> value isa _RORSExact, axis) ||
                throw(ArgumentError(
                    "spectral-Hopf axis breakpoints must be exact"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "spectral-Hopf axis breakpoints must increase"))
            if variable < variable_count
                first(axis) > 0 || throw(ArgumentError(
                    "spectral-Hopf state/control axes must be positive"))
            else
                first(axis) == 0 || throw(ArgumentError(
                    "frequency-squared coverage must start at zero"))
            end
            declared_event_box[variable] == ROExactInterval(
                first(axis), last(axis), Val(:validated)) ||
                throw(ArgumentError(
                    "declared spectral-Hopf bounds do not match their axis"))
            cell_dimensions[variable] = length(axis) - 1
            cell_count_big *= cell_dimensions[variable]
            _rohsc_limit(:cells, cell_count_big, limits.max_cells)
        end
        uniform_spectral_radius_upper_bound >= 0 ||
            throw(ArgumentError(
                "spectral-radius upper bound must be nonnegative"))
        frequency_squared_coverage_upper_bound ==
            uniform_spectral_radius_upper_bound^2 ||
            throw(ArgumentError(
                "frequency coverage bound must be the squared spectral-radius bound"))
        last(event_axis_breaks[end]) >
            frequency_squared_coverage_upper_bound ||
            throw(ArgumentError(
                "frequency-squared axis does not strictly pad its proof bound"))
        partition_cell_count == cell_count_big || throw(ArgumentError(
            "spectral-Hopf partition count mismatch"))
        length(cells) == partition_cell_count || throw(ArgumentError(
            "spectral-Hopf cell population is incomplete"))
        all(cell -> cell isa ROSpectralHopfEventCensusCell, cells) ||
            throw(ArgumentError(
                "spectral-Hopf census contains an invalid cell"))
        all(event -> event isa ROSimpleSpectralHopfEvent, events) ||
            throw(ArgumentError(
                "spectral-Hopf census contains an invalid event"))
        length(events) == spectral_hopf_event_count ==
            length(event_grid_indices) == length(event_preconditioners) ||
            throw(ArgumentError(
                "spectral-Hopf event population counts do not match"))
        spectral_hopf_free_cell_count + spectral_hopf_event_count ==
            partition_cell_count || throw(ArgumentError(
            "spectral-Hopf cell counts do not close"))
        _rohsc_limit(
            :events,
            spectral_hopf_event_count,
            limits.max_events,
        )
        all(matrix -> matrix isa ROExactMatrix, event_preconditioners) ||
            throw(ArgumentError(
                "spectral-Hopf preconditioners must be exact matrices"))
        length(unique(event_grid_indices)) == length(event_grid_indices) ||
            throw(ArgumentError(
                "spectral-Hopf event indices must be unique"))
        event_by_grid = Dict(event.grid_index => event for event in events)
        length(event_by_grid) == length(events) || throw(ArgumentError(
            "spectral-Hopf events occupy duplicate cells"))
        canonical_indices = Tuple[]
        canonical_hashes = String[]
        free_count = 0
        event_count = 0
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(Tuple(cell_dimensions)))
            cell = cells[linear_index]
            grid_index = Tuple(cartesian_index)
            cell.linear_index == linear_index &&
                cell.grid_index == grid_index || throw(ArgumentError(
                "spectral-Hopf cells are not in canonical order"))
            for variable in 1:variable_count
                axis = event_axis_breaks[variable]
                index = grid_index[variable]
                expected_box = ROExactInterval(
                    axis[index], axis[index + 1], Val(:validated))
                cell.event_box[variable] == expected_box ||
                    throw(ArgumentError(
                        "spectral-Hopf cell bounds do not match their axis"))
            end
            cell.evidence_sha256 == _rohsc_cell_sha256(
                cell.linear_index,
                cell.grid_index,
                cell.event_box,
                cell.classification,
                cell.excluding_augmented_equation_index,
                cell.augmented_residual_enclosure,
                cell.event_certificate_sha256,
            ) || throw(ArgumentError(
                "nested spectral-Hopf cell hash mismatch"))
            if cell.classification == :unique_simple_spectral_hopf_event
                event_count += 1
                event = get(event_by_grid, grid_index, nothing)
                event === nothing && throw(ArgumentError(
                    "spectral-Hopf event cell has no event"))
                event.linear_index == linear_index &&
                    event.event_box == cell.event_box &&
                    event.certificate_sha256 ==
                        cell.event_certificate_sha256 ||
                    throw(ArgumentError(
                        "spectral-Hopf event does not match its cell"))
                push!(canonical_indices, grid_index)
                push!(canonical_hashes, event.certificate_sha256)
            else
                free_count += 1
            end
        end
        event_count == spectral_hopf_event_count &&
            free_count == spectral_hopf_free_cell_count ||
            throw(ArgumentError(
                "spectral-Hopf canonical counts do not match"))
        event_grid_indices == Tuple(canonical_indices) ||
            throw(ArgumentError(
                "spectral-Hopf event indices are not canonical"))
        canonical_hashes ==
            [event.certificate_sha256 for event in events] ||
            throw(ArgumentError(
                "spectral-Hopf event order is not canonical"))
        for index in eachindex(events)
            event = events[index]
            preconditioner = event_preconditioners[index]
            event.preconditioner.row_count == preconditioner.row_count &&
                event.preconditioner.column_count ==
                    preconditioner.column_count &&
                event.preconditioner.data == preconditioner.data ||
                throw(ArgumentError(
                "spectral-Hopf event preconditioner does not match its seed"))
            event.certificate_sha256 == _rohsc_event_sha256(
                event.linear_index,
                event.grid_index,
                event.event_box,
                event.center,
                event.preconditioner,
                event.krawczyk_offset_image,
                event.augmented_residual_enclosure,
                event.augmented_jacobian_enclosure,
                event.state_jacobian_determinant_enclosure,
                event.frequency_squared_root_enclosure,
                event.contraction_beta,
            ) || throw(ArgumentError(
                "nested spectral-Hopf event hash mismatch"))
        end
        state_control_count = variable_count - 1
        projection_pair_count =
            BigInt(length(events)) * (length(events) - 1) ÷ 2
        _rohsc_limit(
            :projection_pairs,
            projection_pair_count,
            limits.max_projection_pairs,
        )
        for left_index in eachindex(events)
            left = events[left_index]
            for right_index in eachindex(events)
                right_index > left_index || continue
                right = events[right_index]
                separated = false
                for variable in 1:state_control_count
                    left_root = ROExactInterval(
                        left.center[variable] +
                            left.krawczyk_offset_image[variable].lower,
                        left.center[variable] +
                            left.krawczyk_offset_image[variable].upper,
                        Val(:validated),
                    )
                    right_root = ROExactInterval(
                        right.center[variable] +
                            right.krawczyk_offset_image[variable].lower,
                        right.center[variable] +
                            right.krawczyk_offset_image[variable].upper,
                        Val(:validated),
                    )
                    if left_root.upper < right_root.lower ||
                            right_root.upper < left_root.lower
                        separated = true
                        break
                    end
                end
                separated || throw(ArgumentError(
                    "spectral-Hopf event state/control projections overlap"))
            end
        end
        analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "spectral-Hopf operation count must be nonnegative"))
        _rohsc_limit(
            :interval_operations,
            analysis_interval_operation_count,
            limits.max_interval_operations,
        )
        expected = _rohsc_census_sha256(
            system_declaration_sha256,
            dynamics_binding.declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_box,
            uniform_spectral_radius_upper_bound,
            frequency_squared_coverage_upper_bound,
            event_grid_indices,
            event_preconditioners,
            events,
            cells,
            partition_cell_count,
            spectral_hopf_event_count,
            spectral_hopf_free_cell_count,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "simple spectral-Hopf census hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            dynamics_binding,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_box,
            uniform_spectral_radius_upper_bound,
            frequency_squared_coverage_upper_bound,
            event_grid_indices,
            event_preconditioners,
            events,
            cells,
            partition_cell_count,
            spectral_hopf_event_count,
            spectral_hopf_free_cell_count,
            analysis_interval_operation_count,
            true,
            true,
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
            false,
            evidence_scope,
            certificate_sha256,
        )
    end
end

function _rohsc_uniform_spectral_radius_upper_bound(
    state_jacobian::Matrix{_RORSPolynomial},
    declared_event_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    state_count, column_count = size(state_jacobian)
    state_count == column_count || throw(DimensionMismatch(
        "spectral-radius bound requires a square state Jacobian"))
    radius_bound = zero(_RORSExact)
    for row in 1:state_count
        context.cancel_check()
        row_bound = zero(_RORSExact)
        for column in 1:state_count
            enclosure = _rors_evaluate_polynomial(
                state_jacobian[row, column],
                declared_event_box,
                context,
            )
            row_bound = _rors_exact_add(
                context, row_bound, _rors_abs_upper(enclosure))
        end
        radius_bound = max(radius_bound, row_bound)
    end
    return radius_bound
end

function _rohsc_excluding_equation(
    residual::Vector{ROExactInterval},
)
    length(residual) >= 4 || throw(DimensionMismatch(
        "spectral-Hopf residual must contain states, E, and O"))
    even_equation = length(residual) - 1
    odd_equation = length(residual)
    for equation in (even_equation, odd_equation)
        !_rors_contains_zero(residual[equation]) && return equation
    end
    for equation in 1:(even_equation - 1)
        !_rors_contains_zero(residual[equation]) && return equation
    end
    return nothing
end

function _rohsc_absolute_root_interval(
    event::ROSimpleSpectralHopfEvent,
    variable::Int,
)
    variable in eachindex(event.center) || throw(BoundsError(
        event.center, variable))
    offset = event.krawczyk_offset_image[variable]
    return ROExactInterval(
        event.center[variable] + offset.lower,
        event.center[variable] + offset.upper,
        Val(:validated),
    )
end

@inline function _rohsc_disjoint(
    left::ROExactInterval,
    right::ROExactInterval,
)
    return left.upper < right.lower || right.upper < left.lower
end

function _rohsc_projections_pairwise_disjoint(
    events,
    state_control_count::Int,
    cancel_check,
)
    for left_index in eachindex(events)
        cancel_check()
        left = events[left_index]
        for right_index in eachindex(events)
            right_index > left_index || continue
            cancel_check()
            right = events[right_index]
            any_separated = false
            for variable in 1:state_control_count
                if _rohsc_disjoint(
                    _rohsc_absolute_root_interval(left, variable),
                    _rohsc_absolute_root_interval(right, variable),
                )
                    any_separated = true
                    break
                end
            end
            any_separated || return false, left_index, right_index
        end
    end
    return true, 0, 0
end

function _rohsc_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    dynamics_binding::ROPolynomialDynamicsBinding,
    event_axis_breaks::Tuple,
    event_grid_indices::Tuple,
    event_preconditioners::Vector{Matrix{_RORSExact}},
    limits::ROSpectralHopfEventCensusLimits,
    cancel_check,
    admission_operation_count::BigInt,
)
    validate_ro_polynomial_equilibrium_system(system)
    validate_ro_polynomial_dynamics_binding(system, dynamics_binding)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1c0 requires exactly one control coordinate"))
    state_count = length(system.state_names)
    state_count >= 2 || throw(ArgumentError(
        "P8s1c0 requires at least two state coordinates"))
    variable_count = state_count + 2
    length(event_axis_breaks) == variable_count || throw(DimensionMismatch(
        "event axes do not match states, control, and frequency squared"))
    length(event_grid_indices) == length(event_preconditioners) ||
        throw(DimensionMismatch(
            "event preconditioners do not follow event grid indices"))
    _rohsc_limit(:events, length(event_grid_indices), limits.max_events)
    seed_projection_pair_count =
        BigInt(length(event_grid_indices)) *
        (length(event_grid_indices) - 1) ÷ 2
    _rohsc_limit(
        :projection_pairs,
        seed_projection_pair_count,
        limits.max_projection_pairs,
    )

    context_limits = _rohsc_regular_limits_with_operation_cap(
        system.limits, limits.max_interval_operations)
    context = _RORSContext(context_limits, cancel_check)
    expected_admission_operations = _rohsc_admission_operation_count(
        event_axis_breaks, event_preconditioners)
    admission_operation_count == expected_admission_operations ||
        throw(ArgumentError(
            "spectral-Hopf admission operation count is not canonical"))
    _rohsc_limit(
        :interval_operations,
        admission_operation_count,
        limits.max_interval_operations,
    )
    context.operations = admission_operation_count
    cells = ROSpectralHopfEventCensusCell[]
    events = ROSimpleSpectralHopfEvent[]
    try
        cell_dimensions = Vector{Int}(undef, variable_count)
        cell_count_big = BigInt(1)
        declared_event_box = Vector{ROExactInterval}(
            undef, variable_count)
        for variable in 1:variable_count
            context.cancel_check()
            axis = event_axis_breaks[variable]
            length(axis) >= 2 || throw(ArgumentError(
                "each spectral-Hopf axis needs at least two breakpoints"))
            _rohsc_limit(
                :axis_breakpoints_per_variable,
                length(axis),
                limits.max_axis_breakpoints_per_variable,
            )
            all(value -> value isa _RORSExact, axis) ||
                throw(ArgumentError(
                    "spectral-Hopf axis breakpoints must be exact"))
            all(index -> axis[index] < axis[index + 1],
                1:(length(axis) - 1)) || throw(ArgumentError(
                "spectral-Hopf axis breakpoints must be strictly increasing"))
            if variable < variable_count
                first(axis) > 0 || throw(
                    ROSpectralHopfEventCensusRejected(
                        :nonpositive_declared_state_control_domain,
                        "augmented variable $variable leaves positive coordinates",
                    ))
            else
                first(axis) == 0 || throw(
                    ROSpectralHopfEventCensusRejected(
                        :frequency_domain_does_not_start_at_zero,
                        "frequency-squared coverage must start exactly at zero",
                    ))
            end
            declared_event_box[variable] = _rors_interval(
                context, first(axis), last(axis))
            cell_dimensions[variable] = length(axis) - 1
            cell_count_big *= cell_dimensions[variable]
            _rohsc_limit(:cells, cell_count_big, limits.max_cells)
        end

        seed_by_grid = Dict{Tuple,Matrix{_RORSExact}}()
        for seed in eachindex(event_grid_indices)
            context.cancel_check()
            grid_index = event_grid_indices[seed]
            length(grid_index) == variable_count || throw(DimensionMismatch(
                "event grid index $seed has the wrong dimension"))
            for variable in 1:variable_count
                index = grid_index[variable]
                index isa Int &&
                    1 <= index <= cell_dimensions[variable] ||
                    throw(ArgumentError(
                        "event grid index $seed lies outside the partition"))
            end
            haskey(seed_by_grid, grid_index) && throw(ArgumentError(
                "event grid indices must be unique"))
            preconditioner = event_preconditioners[seed]
            size(preconditioner) == (variable_count, variable_count) ||
                throw(DimensionMismatch(
                    "event preconditioner $seed has the wrong shape"))
            _rors_exact_rank(preconditioner, context) == variable_count ||
                throw(ArgumentError(
                    "event preconditioner $seed must have exact full rank"))
            seed_by_grid[grid_index] = preconditioner
        end

        # Both det(F_x) and det(sI-F_x) currently use exact cofactor
        # expansion. Preflight its leaf population before allocating recursive
        # minors; the ordinary exact-operation meter remains the tighter
        # runtime gate once polynomial arithmetic begins.
        determinant_leaf_bound = BigInt(2) * factorial(BigInt(state_count))
        _rohsc_limit(
            :determinant_permutation_leaves,
            determinant_leaf_bound,
            limits.max_interval_operations,
        )
        augmented_polynomials, state_jacobian, state_determinant =
            _rohsc_augmented_polynomials(system, context)
        radius_bound = _rohsc_uniform_spectral_radius_upper_bound(
            state_jacobian, declared_event_box, context)
        frequency_squared_bound = _rors_exact_multiply(
            context, radius_bound, radius_bound)
        last(event_axis_breaks[end]) > frequency_squared_bound || throw(
            ROSpectralHopfEventCensusRejected(
                :frequency_domain_not_complete,
                "frequency-squared upper endpoint $(last(event_axis_breaks[end])) must strictly exceed the exact spectral bound $frequency_squared_bound",
            ))

        sizehint!(cells, Int(cell_count_big))
        sizehint!(events, length(event_grid_indices))
        for (linear_index, cartesian_index) in enumerate(
            CartesianIndices(Tuple(cell_dimensions)))
            context.cancel_check()
            grid_index = Tuple(cartesian_index)
            event_box, center, centered_box = _rofe_cell_geometry(
                grid_index, event_axis_breaks, context)
            centered_variables = _rofe_centered_variable_polynomials(
                center, context)
            translated = [
                _rofe_translate_polynomial(
                    polynomial, centered_variables, context)
                for polynomial in augmented_polynomials
            ]
            translated_state_determinant = _rofe_translate_polynomial(
                state_determinant, centered_variables, context)
            residual, augmented_jacobian = _rofe_augmented_enclosures(
                translated, centered_box, context)
            preconditioner = get(seed_by_grid, grid_index, nothing)
            if preconditioner === nothing
                excluding_equation = _rohsc_excluding_equation(residual)
                excluding_equation === nothing && throw(
                    ROSpectralHopfEventCensusRejected(
                        :unresolved_event_partition_cell,
                        "partition cell $grid_index neither excludes a spectral-Hopf root nor binds one event seed",
                    ))
                push!(cells, _rohsc_make_cell(
                    linear_index,
                    grid_index,
                    Tuple(event_box),
                    :spectral_hopf_free_by_augmented_residual_exclusion,
                    excluding_equation,
                    Tuple(residual),
                    "",
                ))
                continue
            end

            excluding_equation = _rohsc_excluding_equation(residual)
            excluding_equation === nothing || throw(
                ROSpectralHopfEventCensusRejected(
                    :event_seed_excludes_augmented_root,
                    "event seed $grid_index excludes zero in augmented equation $excluding_equation",
                ))
            state_determinant_enclosure = _rors_evaluate_polynomial(
                translated_state_determinant, centered_box, context)
            !_rors_contains_zero(state_determinant_enclosure) || throw(
                ROSpectralHopfEventCensusRejected(
                    :equilibrium_regularity_not_proven,
                    "event seed $grid_index does not exclude det(F_x)=0 on its event cell",
                ))
            preconditioned_jacobian =
                _rors_point_interval_matrix_product(
                    preconditioner, augmented_jacobian, context)
            error_matrix = _rors_error_matrix(
                preconditioned_jacobian, context)
            beta = _rors_beta(error_matrix, context)
            beta < 1 || throw(ROSpectralHopfEventCensusRejected(
                :augmented_contraction_not_proven,
                "event seed $grid_index has beta=$beta, requiring beta < 1",
            ))
            point_residual = _rofe_point_residual(translated, context)
            preconditioned_residual =
                _rors_point_interval_vector_product(
                    preconditioner, point_residual, context)
            newton_offset = _rors_negate_vector(
                preconditioned_residual, context)
            error_image = _rors_interval_vector_product(
                error_matrix, centered_box, context)
            krawczyk_image = Vector{ROExactInterval}(
                undef, variable_count)
            for variable in 1:variable_count
                krawczyk_image[variable] = _rors_add(
                    context,
                    newton_offset[variable],
                    error_image[variable],
                )
                _rors_strict_subset(
                    krawczyk_image[variable], centered_box[variable]) ||
                    throw(ROSpectralHopfEventCensusRejected(
                        :augmented_krawczyk_inclusion_not_proven,
                        "event seed $grid_index Krawczyk image is not strictly interior in variable $variable",
                    ))
            end
            frequency_squared_root_enclosure = _rors_add(
                context,
                _rors_point(context, center[end]),
                krawczyk_image[end],
            )
            frequency_squared_root_enclosure.lower > 0 || throw(
                ROSpectralHopfEventCensusRejected(
                    :nonzero_frequency_not_proven,
                    "event seed $grid_index does not enclose z=omega^2 strictly above zero",
                ))

            # Write P(s)=det(sI-F_x)=E(-s^2)+s*O(-s^2). At a root of
            # (F,E(z),O(z)) with z>0, det(F_x)!=0 and nonsingular DH,
            # P_s(i*sqrt(z))=2z*O_z-2i*sqrt(z)*E_z cannot vanish, so the
            # conjugate pair is algebraically simple. Eliminating the regular
            # equilibrium branch gives Delta=dot(E)*O_z-E_z*dot(O)!=0 and
            # alpha'=-Delta/(2*(E_z^2+z*O_z^2))!=0. This is a transverse
            # spectral crossing only; no first-Lyapunov claim follows.
            event = _rohsc_make_event(
                linear_index,
                grid_index,
                Tuple(event_box),
                Tuple(center),
                _rors_exact_matrix_wrapper(preconditioner),
                Tuple(krawczyk_image),
                Tuple(residual),
                _rors_interval_matrix_wrapper(augmented_jacobian),
                state_determinant_enclosure,
                frequency_squared_root_enclosure,
                beta,
            )
            push!(events, event)
            push!(cells, _rohsc_make_cell(
                linear_index,
                grid_index,
                Tuple(event_box),
                :unique_simple_spectral_hopf_event,
                0,
                Tuple(residual),
                event.certificate_sha256,
            ))
        end
        length(events) == length(seed_by_grid) || throw(ArgumentError(
            "not every spectral-Hopf event seed entered the partition"))
        projection_pair_count =
            BigInt(length(events)) * (length(events) - 1) ÷ 2
        _rohsc_limit(
            :projection_pairs,
            projection_pair_count,
            limits.max_projection_pairs,
        )
        projections_disjoint, left_index, right_index =
            _rohsc_projections_pairwise_disjoint(
                events, state_count + 1, cancel_check)
        projections_disjoint || throw(
            ROSpectralHopfEventCensusRejected(
                :multiple_imaginary_pairs_same_equilibrium_not_excluded,
                "events $left_index and $right_index have overlapping state/control root enclosures",
            ))

        context.cancel_check()
        context.operations <= typemax(Int) || throw(
            ROSpectralHopfEventCensusLimitExceeded(
                :interval_operations,
                context.operations,
                limits.max_interval_operations,
            ))
        analysis_operations = Int(context.operations)
        event_tuple = Tuple(events)
        cell_tuple = Tuple(cells)
        event_indices = Tuple(event.grid_index for event in events)
        preconditioner_wrappers = Tuple(
            event.preconditioner for event in events)
        free_count = length(cells) - length(events)
        frequency_name = "frequency_squared"
        (frequency_name in system.state_names ||
            frequency_name in system.control_names) && throw(ArgumentError(
            "frequency_squared is reserved by the spectral-Hopf census"))
        augmented_variable_names = Tuple((
            system.state_names...,
            system.control_names[1],
            frequency_name,
        ))
        augmented_variable_units = Tuple((
            system.state_units...,
            system.control_units[1],
            string("(", dynamics_binding.time_unit, ")^-2"),
        ))
        declared_event_tuple = Tuple(declared_event_box)
        certificate_sha256 = _rohsc_census_sha256(
            system.declaration_sha256,
            dynamics_binding.declaration_sha256,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_tuple,
            radius_bound,
            frequency_squared_bound,
            event_indices,
            preconditioner_wrappers,
            event_tuple,
            cell_tuple,
            length(cells),
            length(events),
            free_count,
            analysis_operations,
        )
        return ROCompleteSimpleSpectralHopfEventCensus(
            _ROHSC_VALIDATED_TOKEN,
            RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_VERSION,
            system.declaration_sha256,
            dynamics_binding,
            limits,
            augmented_variable_names,
            augmented_variable_units,
            event_axis_breaks,
            declared_event_tuple,
            radius_bound,
            frequency_squared_bound,
            event_indices,
            preconditioner_wrappers,
            event_tuple,
            cell_tuple,
            length(cells),
            length(events),
            free_count,
            analysis_operations,
            true,
            true,
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
            false,
            RO_SIMPLE_SPECTRAL_HOPF_EVENT_CENSUS_SCOPE,
            certificate_sha256,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROSpectralHopfEventCensusLimitExceeded(
                :interval_operations,
                err.requested,
                limits.max_interval_operations,
            ))
        end
        rethrow()
    end
end

"""
    certify_ro_complete_simple_spectral_hopf_event_census(system, binding; ...)

Exhaustively classify a positive exact-dyadic state/control box and a complete
frequency-squared axis for the phase-free polynomial system `(F,E,O)`. Every
event seed must pass a strict augmented Krawczyk proof, and every other cell
must exclude one augmented equation. The result certifies simple transverse
spectral crossings, not nonlinear Hopf bifurcations.
"""
function certify_ro_complete_simple_spectral_hopf_event_census(
    system::ROPolynomialEquilibriumSystem,
    dynamics_binding::ROPolynomialDynamicsBinding;
    event_axis_breaks,
    event_grid_indices,
    event_preconditioners,
    limits::ROSpectralHopfEventCensusLimits=
        ROSpectralHopfEventCensusLimits(),
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    validate_ro_polynomial_dynamics_binding(system, dynamics_binding)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1c0 requires exactly one control coordinate"))
    state_count = length(system.state_names)
    state_count >= 2 || throw(ArgumentError(
        "P8s1c0 requires at least two state coordinates"))
    variable_count = state_count + 2
    parse_limits = _rohsc_regular_limits_with_operation_cap(
        system.limits, limits.max_interval_operations)
    context = _RORSContext(parse_limits, cancel_check)
    try
        admitted_axes = _rohsc_parse_axes(
            event_axis_breaks,
            variable_count,
            limits,
            context,
        )
        admitted_indices, admitted_preconditioners =
            _rohsc_admit_event_seeds(
                event_grid_indices,
                event_preconditioners,
                admitted_axes,
                limits,
                context,
            )
        return _rohsc_certify_exact(
            system,
            dynamics_binding,
            admitted_axes,
            admitted_indices,
            admitted_preconditioners,
            limits,
            cancel_check,
            context.operations,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROSpectralHopfEventCensusLimitExceeded(
                :interval_operations,
                err.requested,
                limits.max_interval_operations,
            ))
        end
        rethrow()
    end
end

function replay_ro_complete_simple_spectral_hopf_event_census(
    system::ROPolynomialEquilibriumSystem,
    certificate::ROCompleteSimpleSpectralHopfEventCensus;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "spectral-Hopf census belongs to a different polynomial system"))
    preconditioners = Matrix{_RORSExact}[]
    sizehint!(preconditioners, length(certificate.event_preconditioners))
    for preconditioner in certificate.event_preconditioners
        push!(preconditioners, _rors_exact_matrix_values(preconditioner))
    end
    rebuilt = _rohsc_certify_exact(
        system,
        certificate.dynamics_binding,
        certificate.event_axis_breaks,
        certificate.event_grid_indices,
        preconditioners,
        certificate.limits,
        cancel_check,
        _rohsc_admission_operation_count(
            certificate.event_axis_breaks, preconditioners),
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "complete spectral-Hopf event census replay changed its hash"))
    return rebuilt
end

function validate_ro_complete_simple_spectral_hopf_event_census(
    system::ROPolynomialEquilibriumSystem,
    certificate::ROCompleteSimpleSpectralHopfEventCensus;
    cancel_check=() -> nothing,
)
    replay_ro_complete_simple_spectral_hopf_event_census(
        system,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
