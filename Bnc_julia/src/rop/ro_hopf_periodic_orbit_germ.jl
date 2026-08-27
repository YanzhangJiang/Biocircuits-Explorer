const RO_HOPF_PERIODIC_ORBIT_GERM_EVENT_VERSION =
    "bne-ro-hopf-periodic-orbit-germ-event/v1.0.0"
const RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_VERSION =
    "bne-ro-complete-hopf-periodic-orbit-germ-census/v1.0.0"
const RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION =
    "classical-nondegenerate-hopf-local-periodic-orbit-germ/v1.0.0"
# This formula version fixes columns as `(x..., lambda, z)` and equations as
# `(F..., E, O)`. Reordering either side can reverse the determinant sign.
const RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION =
    "even-odd-hopf-crossing-det-sign-x-lambda-z-F-E-O/v1.0.0"
const RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_SCOPE =
    :complete_theorem_level_local_periodic_orbit_germ_lift_of_replayed_nondegenerate_hopf_census

struct ROHopfPeriodicOrbitGermLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROHopfPeriodicOrbitGermLimitExceeded)
    print(
        io,
        "Hopf periodic-orbit germ limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct ROHopfPeriodicOrbitGermRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROHopfPeriodicOrbitGermRejected)
    print(
        io,
        "Hopf periodic-orbit germ certification rejected (",
        err.reason,
        "): ",
        err.detail,
    )
end

@inline function _rohpg_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "Hopf periodic-orbit germ requested work must be nonnegative"))
    amount <= limit || throw(
        ROHopfPeriodicOrbitGermLimitExceeded(phase, amount, limit))
    return nothing
end

"""Hard population, determinant, and cumulative-work limits for P8s1c2a."""
struct ROHopfPeriodicOrbitGermLimits
    max_events::Int
    max_determinant_dimension::Int
    max_preconditioner_entries::Int
    max_parent_replay_interval_operations::Int
    max_analysis_interval_operations::Int

    function ROHopfPeriodicOrbitGermLimits(
        max_events::Int,
        max_determinant_dimension::Int,
        max_preconditioner_entries::Int,
        max_parent_replay_interval_operations::Int,
        max_analysis_interval_operations::Int,
    )
        max_events >= 0 || throw(ArgumentError(
            "max_events must be nonnegative"))
        for (label, value) in (
            ("max_determinant_dimension", max_determinant_dimension),
            ("max_preconditioner_entries", max_preconditioner_entries),
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
            max_determinant_dimension,
            max_preconditioner_entries,
            max_parent_replay_interval_operations,
            max_analysis_interval_operations,
        )
    end
end

function ROHopfPeriodicOrbitGermLimits(;
    max_events::Integer=256,
    max_determinant_dimension::Integer=64,
    max_preconditioner_entries::Integer=1_000_000,
    max_parent_replay_interval_operations::Integer=6_000_000,
    max_analysis_interval_operations::Integer=8_000_000,
)
    values = (
        max_events,
        max_determinant_dimension,
        max_preconditioner_entries,
        max_parent_replay_interval_operations,
        max_analysis_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError(
            "Hopf periodic-orbit germ limits must fit Int"))
    return ROHopfPeriodicOrbitGermLimits(Int.(values)...)
end

function _rohpg_write_limits(
    io::IO,
    limits::ROHopfPeriodicOrbitGermLimits,
)
    for value in (
        limits.max_events,
        limits.max_determinant_dimension,
        limits.max_preconditioner_entries,
        limits.max_parent_replay_interval_operations,
        limits.max_analysis_interval_operations,
    )
        _rors_write_token(io, value)
    end
    return nothing
end

struct _ROHPGValidatedToken end
const _ROHPG_VALIDATED_TOKEN = _ROHPGValidatedToken()

@inline function _rohpg_sign_symbol(sign::Int)
    sign in (-1, 1) || throw(ArgumentError(
        "a certified sign must be -1 or 1"))
    return sign > 0 ? :positive : :negative
end

@inline function _rohpg_control_side(sign::Int)
    sign in (-1, 1) || throw(ArgumentError(
        "a control-side sign must be -1 or 1"))
    return sign > 0 ? :control_above_hopf_event :
        :control_below_hopf_event
end

@inline function _rohpg_center_stability(first_lyapunov_sign::Int)
    first_lyapunov_sign in (-1, 1) || throw(ArgumentError(
        "a first-Lyapunov sign must be -1 or 1"))
    return first_lyapunov_sign < 0 ?
        :radially_attracting_on_center_manifold :
        :radially_repelling_on_center_manifold
end

function _rohpg_event_sha256(
    parent_spectral_event_sha256::String,
    parent_hopf_event_sha256::String,
    original_control_name::String,
    original_control_unit::String,
    preconditioner_determinant_sign::Int,
    state_jacobian_determinant_sign::Int,
    real_part_crossing_speed_sign::Int,
    first_lyapunov_coefficient_sign::Int,
    original_control_side::Symbol,
    center_manifold_radial_stability_at_onset::Symbol,
)
    io = IOBuffer()
    for value in (
        RO_HOPF_PERIODIC_ORBIT_GERM_EVENT_VERSION,
        RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION,
        RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION,
        parent_spectral_event_sha256,
        parent_hopf_event_sha256,
        original_control_name,
        original_control_unit,
        preconditioner_determinant_sign,
        state_jacobian_determinant_sign,
        real_part_crossing_speed_sign,
        first_lyapunov_coefficient_sign,
        original_control_side,
        center_manifold_radial_stability_at_onset,
        true,  # complete parent chain required for authority
        true,  # nondegenerate Hopf hypotheses replayed
        true,  # crossing orientation certified
        true,  # theorem-level local periodic-orbit germ exists
        true,  # theorem-level event incidence
        true,  # original-control side
        true,  # center-manifold radial stability at onset
        false, # explicit periodic-orbit enclosure
        false, # validated periodic-orbit branch
        false, # quantitative amplitude radius
        false, # Floquet spectrum
        false, # full-state orbit stability
        false, # global continuation
        false, # true hysteresis
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
One P8s1c2a theorem-level local periodic-orbit germ.

This record applies the classical nondegenerate Hopf theorem after replaying
the complete P8s1c0/P8s1c1 authority chain. It proves existence, event
incidence, original-control side, and center-manifold radial stability only
for sufficiently small, nonzero, but unquantified amplitude. It contains no
periodic-orbit enclosure or Floquet proof.
"""
struct ROHopfPeriodicOrbitGermEvent
    version::String
    theorem_version::String
    crossing_orientation_formula_version::String
    parent_spectral_event_sha256::String
    parent_hopf_event_sha256::String
    original_control_name::String
    original_control_unit::String
    preconditioner_determinant_sign::Int
    state_jacobian_determinant_sign::Int
    real_part_crossing_speed_sign::Int
    first_lyapunov_coefficient_sign::Int
    original_control_side::Symbol
    center_manifold_radial_stability_at_onset::Symbol
    complete_parent_chain_required_for_authority::Bool
    nondegenerate_hopf_hypotheses_replayed::Bool
    real_part_crossing_orientation_certified::Bool
    local_periodic_orbit_germ_exists::Bool
    theorem_level_hopf_event_incidence_certified::Bool
    original_control_side_certified::Bool
    center_manifold_radial_stability_at_onset_certified::Bool
    explicit_periodic_orbit_enclosure_certified::Bool
    validated_periodic_orbit_branch_certified::Bool
    quantitative_amplitude_radius_certified::Bool
    floquet_spectrum_certified::Bool
    full_state_periodic_orbit_stability_certified::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    certificate_sha256::String

    function ROHopfPeriodicOrbitGermEvent(
        ::_ROHPGValidatedToken,
        version::String,
        theorem_version::String,
        crossing_orientation_formula_version::String,
        parent_spectral_event_sha256::String,
        parent_hopf_event_sha256::String,
        original_control_name::String,
        original_control_unit::String,
        preconditioner_determinant_sign::Int,
        state_jacobian_determinant_sign::Int,
        real_part_crossing_speed_sign::Int,
        first_lyapunov_coefficient_sign::Int,
        original_control_side::Symbol,
        center_manifold_radial_stability_at_onset::Symbol,
        complete_parent_chain_required_for_authority::Bool,
        nondegenerate_hopf_hypotheses_replayed::Bool,
        real_part_crossing_orientation_certified::Bool,
        local_periodic_orbit_germ_exists::Bool,
        theorem_level_hopf_event_incidence_certified::Bool,
        original_control_side_certified::Bool,
        center_manifold_radial_stability_at_onset_certified::Bool,
        explicit_periodic_orbit_enclosure_certified::Bool,
        validated_periodic_orbit_branch_certified::Bool,
        quantitative_amplitude_radius_certified::Bool,
        floquet_spectrum_certified::Bool,
        full_state_periodic_orbit_stability_certified::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        certificate_sha256::String,
    )
        version == RO_HOPF_PERIODIC_ORBIT_GERM_EVENT_VERSION ||
            throw(ArgumentError(
                "Hopf periodic-orbit germ event version mismatch"))
        theorem_version == RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION ||
            throw(ArgumentError(
                "Hopf periodic-orbit germ theorem version mismatch"))
        crossing_orientation_formula_version ==
            RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION ||
            throw(ArgumentError(
                "Hopf crossing-orientation formula version mismatch"))
        _rors_validate_sha256(parent_spectral_event_sha256,
            "parent_spectral_event_sha256")
        _rors_validate_sha256(parent_hopf_event_sha256,
            "parent_hopf_event_sha256")
        isempty(original_control_name) && throw(ArgumentError(
            "original control name must be nonempty"))
        isempty(original_control_unit) && throw(ArgumentError(
            "original control unit must be nonempty"))
        for (sign, label) in (
            (preconditioner_determinant_sign,
                "preconditioner determinant"),
            (state_jacobian_determinant_sign,
                "state-Jacobian determinant"),
            (real_part_crossing_speed_sign,
                "real-part crossing speed"),
            (first_lyapunov_coefficient_sign,
                "first Lyapunov coefficient"),
        )
            sign in (-1, 1) || throw(ArgumentError(
                "$label sign must be -1 or 1"))
        end
        expected_crossing = -preconditioner_determinant_sign *
            state_jacobian_determinant_sign
        real_part_crossing_speed_sign == expected_crossing ||
            throw(ArgumentError(
                "real-part crossing sign disagrees with the determinant formula"))
        expected_side_sign = -first_lyapunov_coefficient_sign *
            real_part_crossing_speed_sign
        original_control_side == _rohpg_control_side(expected_side_sign) ||
            throw(ArgumentError(
                "original-control side disagrees with l1 and crossing signs"))
        center_manifold_radial_stability_at_onset ==
            _rohpg_center_stability(first_lyapunov_coefficient_sign) ||
            throw(ArgumentError(
                "center-manifold radial stability disagrees with l1 sign"))
        for (value, label) in (
            (complete_parent_chain_required_for_authority,
                "complete parent-chain authority"),
            (nondegenerate_hopf_hypotheses_replayed,
                "nondegenerate Hopf hypotheses"),
            (real_part_crossing_orientation_certified,
                "real-part crossing orientation"),
            (local_periodic_orbit_germ_exists,
                "local periodic-orbit germ existence"),
            (theorem_level_hopf_event_incidence_certified,
                "theorem-level Hopf-event incidence"),
            (original_control_side_certified,
                "original-control side"),
            (center_manifold_radial_stability_at_onset_certified,
                "center-manifold radial stability"),
        )
            value || throw(ArgumentError(
                "Hopf periodic-orbit germ lost $label"))
        end
        for (value, label) in (
            (explicit_periodic_orbit_enclosure_certified,
                "explicit periodic-orbit enclosure"),
            (validated_periodic_orbit_branch_certified,
                "validated periodic-orbit branch"),
            (quantitative_amplitude_radius_certified,
                "quantitative amplitude radius"),
            (floquet_spectrum_certified, "Floquet spectrum"),
            (full_state_periodic_orbit_stability_certified,
                "full-state periodic-orbit stability"),
            (global_continuation_certified, "global continuation"),
            (true_hysteresis_certified, "true hysteresis"),
        )
            value && throw(ArgumentError(
                "P8s1c2a cannot certify $label"))
        end
        expected = _rohpg_event_sha256(
            parent_spectral_event_sha256,
            parent_hopf_event_sha256,
            original_control_name,
            original_control_unit,
            preconditioner_determinant_sign,
            state_jacobian_determinant_sign,
            real_part_crossing_speed_sign,
            first_lyapunov_coefficient_sign,
            original_control_side,
            center_manifold_radial_stability_at_onset,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "Hopf periodic-orbit germ event hash mismatch"))
        return new(
            version,
            theorem_version,
            crossing_orientation_formula_version,
            parent_spectral_event_sha256,
            parent_hopf_event_sha256,
            original_control_name,
            original_control_unit,
            preconditioner_determinant_sign,
            state_jacobian_determinant_sign,
            real_part_crossing_speed_sign,
            first_lyapunov_coefficient_sign,
            original_control_side,
            center_manifold_radial_stability_at_onset,
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
            false,
            false,
            false,
            false,
            certificate_sha256,
        )
    end
end

function _rohpg_make_event(
    parent_spectral_event_sha256::String,
    parent_hopf_event_sha256::String,
    original_control_name::String,
    original_control_unit::String,
    preconditioner_determinant_sign::Int,
    state_jacobian_determinant_sign::Int,
    real_part_crossing_speed_sign::Int,
    first_lyapunov_coefficient_sign::Int,
)
    side_sign = -first_lyapunov_coefficient_sign *
        real_part_crossing_speed_sign
    side = _rohpg_control_side(side_sign)
    stability = _rohpg_center_stability(
        first_lyapunov_coefficient_sign)
    hash = _rohpg_event_sha256(
        parent_spectral_event_sha256,
        parent_hopf_event_sha256,
        original_control_name,
        original_control_unit,
        preconditioner_determinant_sign,
        state_jacobian_determinant_sign,
        real_part_crossing_speed_sign,
        first_lyapunov_coefficient_sign,
        side,
        stability,
    )
    return ROHopfPeriodicOrbitGermEvent(
        _ROHPG_VALIDATED_TOKEN,
        RO_HOPF_PERIODIC_ORBIT_GERM_EVENT_VERSION,
        RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION,
        RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION,
        parent_spectral_event_sha256,
        parent_hopf_event_sha256,
        original_control_name,
        original_control_unit,
        preconditioner_determinant_sign,
        state_jacobian_determinant_sign,
        real_part_crossing_speed_sign,
        first_lyapunov_coefficient_sign,
        side,
        stability,
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
        false,
        false,
        false,
        false,
        hash,
    )
end

function _rohpg_census_sha256(
    system_declaration_sha256::String,
    dynamics_binding_declaration_sha256::String,
    spectral_parent_census_sha256::String,
    hopf_parent_census_sha256::String,
    limits::ROHopfPeriodicOrbitGermLimits,
    events::Tuple,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    for value in (
        RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_VERSION,
        RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION,
        RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION,
        system_declaration_sha256,
        dynamics_binding_declaration_sha256,
        spectral_parent_census_sha256,
        hopf_parent_census_sha256,
    )
        _rors_write_token(io, value)
    end
    _rohpg_write_limits(io, limits)
    _rors_write_token(io, length(events))
    for event in events
        _rors_write_token(io, event.certificate_sha256)
    end
    _rors_write_token(io, analysis_interval_operation_count)
    for value in (
        true,  # complete c0/c1 parent chain replayed
        true,  # every parent event lifted exactly once
        true,  # all local germs exist by theorem
        true,  # all theorem-level incidences certified
        true,  # all original-control sides certified
        true,  # all center-manifold onset stability classified
        false, # explicit periodic-orbit enclosures
        false, # validated periodic-orbit branches
        false, # quantitative amplitude radii
        false, # Floquet spectra
        false, # full-state orbit stability
        false, # periodic-orbit population completeness
        false, # stable-orbit population completeness
        false, # global continuation
        false, # true hysteresis
        RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
Complete P8s1c2a theorem-level lift of a replayed P8s1c1 event population.

Completeness refers only to lifting every event in the named parent census. It
does not enumerate all periodic orbits and does not construct any orbit.
"""
struct ROCompleteHopfPeriodicOrbitGermCensus
    version::String
    theorem_version::String
    crossing_orientation_formula_version::String
    system_declaration_sha256::String
    dynamics_binding_declaration_sha256::String
    spectral_parent_census_sha256::String
    hopf_parent_census_sha256::String
    limits::ROHopfPeriodicOrbitGermLimits
    events::Tuple
    parent_event_count::Int
    local_periodic_orbit_germ_count::Int
    analysis_interval_operation_count::Int
    complete_parent_chain_replayed::Bool
    every_parent_event_lifted_exactly_once::Bool
    all_local_periodic_orbit_germs_exist::Bool
    all_theorem_level_hopf_event_incidences_certified::Bool
    all_original_control_sides_certified::Bool
    all_center_manifold_radial_stabilities_at_onset_certified::Bool
    explicit_periodic_orbit_enclosures_certified::Bool
    validated_periodic_orbit_branches_certified::Bool
    quantitative_amplitude_radii_certified::Bool
    floquet_spectra_certified::Bool
    full_state_periodic_orbit_stabilities_certified::Bool
    periodic_orbit_population_complete::Bool
    stable_periodic_orbit_population_complete::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROCompleteHopfPeriodicOrbitGermCensus(
        ::_ROHPGValidatedToken,
        version::String,
        theorem_version::String,
        crossing_orientation_formula_version::String,
        system_declaration_sha256::String,
        dynamics_binding_declaration_sha256::String,
        spectral_parent_census_sha256::String,
        hopf_parent_census_sha256::String,
        limits::ROHopfPeriodicOrbitGermLimits,
        events::Tuple,
        parent_event_count::Int,
        local_periodic_orbit_germ_count::Int,
        analysis_interval_operation_count::Int,
        complete_parent_chain_replayed::Bool,
        every_parent_event_lifted_exactly_once::Bool,
        all_local_periodic_orbit_germs_exist::Bool,
        all_theorem_level_hopf_event_incidences_certified::Bool,
        all_original_control_sides_certified::Bool,
        all_center_manifold_radial_stabilities_at_onset_certified::Bool,
        explicit_periodic_orbit_enclosures_certified::Bool,
        validated_periodic_orbit_branches_certified::Bool,
        quantitative_amplitude_radii_certified::Bool,
        floquet_spectra_certified::Bool,
        full_state_periodic_orbit_stabilities_certified::Bool,
        periodic_orbit_population_complete::Bool,
        stable_periodic_orbit_population_complete::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_VERSION ||
            throw(ArgumentError(
                "complete Hopf periodic-orbit germ census version mismatch"))
        theorem_version == RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION ||
            throw(ArgumentError(
                "Hopf periodic-orbit germ theorem version mismatch"))
        crossing_orientation_formula_version ==
            RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION ||
            throw(ArgumentError(
                "Hopf crossing-orientation formula version mismatch"))
        for (hash, label) in (
            (system_declaration_sha256, "system_declaration_sha256"),
            (dynamics_binding_declaration_sha256,
                "dynamics_binding_declaration_sha256"),
            (spectral_parent_census_sha256,
                "spectral_parent_census_sha256"),
            (hopf_parent_census_sha256, "hopf_parent_census_sha256"),
            (certificate_sha256, "certificate_sha256"),
        )
            _rors_validate_sha256(hash, label)
        end
        evidence_scope ==
            RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_SCOPE ||
            throw(ArgumentError(
                "complete Hopf periodic-orbit germ evidence scope mismatch"))
        length(events) == parent_event_count ==
            local_periodic_orbit_germ_count || throw(ArgumentError(
            "Hopf periodic-orbit germ population counts do not match"))
        _rohpg_limit(:events, parent_event_count, limits.max_events)
        all(event -> event isa ROHopfPeriodicOrbitGermEvent, events) ||
            throw(ArgumentError(
                "Hopf periodic-orbit germ census contains an invalid event"))
        allunique(event.parent_spectral_event_sha256 for event in events) ||
            throw(ArgumentError(
                "Hopf periodic-orbit germ parent events must be unique"))
        for (value, label) in (
            (complete_parent_chain_replayed,
                "complete parent-chain replay"),
            (every_parent_event_lifted_exactly_once,
                "complete parent-event lift"),
            (all_local_periodic_orbit_germs_exist,
                "local periodic-orbit germ existence"),
            (all_theorem_level_hopf_event_incidences_certified,
                "theorem-level Hopf-event incidence population"),
            (all_original_control_sides_certified,
                "original-control side population"),
            (all_center_manifold_radial_stabilities_at_onset_certified,
                "center-manifold radial stability population"),
        )
            value || throw(ArgumentError(
                "complete Hopf periodic-orbit germ census lost $label"))
        end
        for (value, label) in (
            (explicit_periodic_orbit_enclosures_certified,
                "explicit periodic-orbit enclosures"),
            (validated_periodic_orbit_branches_certified,
                "validated periodic-orbit branches"),
            (quantitative_amplitude_radii_certified,
                "quantitative amplitude radii"),
            (floquet_spectra_certified, "Floquet spectra"),
            (full_state_periodic_orbit_stabilities_certified,
                "full-state periodic-orbit stabilities"),
            (periodic_orbit_population_complete,
                "periodic-orbit population completeness"),
            (stable_periodic_orbit_population_complete,
                "stable-periodic-orbit population completeness"),
            (global_continuation_certified, "global continuation"),
            (true_hysteresis_certified, "true hysteresis"),
        )
            value && throw(ArgumentError(
                "P8s1c2a cannot certify $label"))
        end
        analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "Hopf periodic-orbit germ operation count must be nonnegative"))
        _rohpg_limit(
            :analysis_interval_operations,
            analysis_interval_operation_count,
            limits.max_analysis_interval_operations,
        )
        expected = _rohpg_census_sha256(
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            spectral_parent_census_sha256,
            hopf_parent_census_sha256,
            limits,
            events,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "complete Hopf periodic-orbit germ census hash mismatch"))
        return new(
            version,
            theorem_version,
            crossing_orientation_formula_version,
            system_declaration_sha256,
            dynamics_binding_declaration_sha256,
            spectral_parent_census_sha256,
            hopf_parent_census_sha256,
            limits,
            events,
            parent_event_count,
            local_periodic_orbit_germ_count,
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
            false,
            false,
            evidence_scope,
            certificate_sha256,
        )
    end
end

function _rohpg_exact_determinant_sign(
    matrix::ROExactMatrix,
    context::_RORSContext,
)
    rows, columns = size(matrix)
    rows == columns || throw(DimensionMismatch(
        "determinant sign requires a square exact matrix"))
    work = Matrix{_RORSExact}(undef, rows, columns)
    for column in 1:columns, row in 1:rows
        _rors_tick!(context)
        work[row, column] = matrix[row, column]
    end
    sign = 1
    for column in 1:columns
        context.cancel_check()
        pivot_row = 0
        for row in column:rows
            _rors_tick!(context)
            if !iszero(work[row, column])
                pivot_row = row
                break
            end
        end
        iszero(pivot_row) && throw(ROHopfPeriodicOrbitGermRejected(
            :singular_parent_preconditioner,
            "the exact P8s1c0 event preconditioner is singular",
        ))
        if pivot_row != column
            for trailing_column in column:columns
                work[column, trailing_column],
                    work[pivot_row, trailing_column] =
                    work[pivot_row, trailing_column],
                    work[column, trailing_column]
            end
            sign = -sign
        end
        pivot = work[column, column]
        pivot < 0 && (sign = -sign)
        for row in (column + 1):rows
            factor = _rors_exact_divide(
                context, work[row, column], pivot)
            iszero(factor) && continue
            work[row, column] = zero(_RORSExact)
            for trailing_column in (column + 1):columns
                product = _rors_exact_multiply(
                    context, factor, work[column, trailing_column])
                work[row, trailing_column] = _rors_exact_subtract(
                    context, work[row, trailing_column], product)
            end
        end
    end
    return sign
end

@inline function _rohpg_interval_sign(
    interval::ROExactInterval,
    label::String,
)
    interval.lower > 0 && return 1
    interval.upper < 0 && return -1
    throw(ROHopfPeriodicOrbitGermRejected(
        :sign_not_separated_from_zero,
        "$label enclosure reaches zero",
    ))
end

function _rohpg_preflight_inputs(
    system::ROPolynomialEquilibriumSystem,
    spectral_parent::ROCompleteSimpleSpectralHopfEventCensus,
    hopf_parent::ROCompleteNondegenerateHopfCensus,
    limits::ROHopfPeriodicOrbitGermLimits,
)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1c2a requires exactly one original control coordinate"))
    hopf_parent.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "nondegenerate-Hopf parent belongs to a different system"))
    spectral_parent.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "spectral-Hopf parent belongs to a different system"))
    hopf_parent.parent_census_sha256 == spectral_parent.certificate_sha256 ||
        throw(ArgumentError(
            "nondegenerate-Hopf parent does not descend from the supplied spectral parent"))
    hopf_parent.dynamics_binding_declaration_sha256 ==
        spectral_parent.dynamics_binding.declaration_sha256 ||
        throw(ArgumentError(
            "Hopf parent dynamics bindings do not match"))
    expected_augmented_names = Tuple((
        system.state_names...,
        system.control_names[1],
        "frequency_squared",
    ))
    spectral_parent.augmented_variable_names == expected_augmented_names ||
        throw(ArgumentError(
            "spectral-Hopf parent does not use the required (x..., lambda, z) variable order"))
    event_count = hopf_parent.nondegenerate_hopf_event_count
    event_count == hopf_parent.parent_event_count ==
        spectral_parent.spectral_hopf_event_count || throw(ArgumentError(
        "Hopf parent population counts do not match"))
    _rohpg_limit(:events, event_count, limits.max_events)
    if event_count > 0
        determinant_dimension = BigInt(length(system.state_names)) + 2
        _rohpg_limit(
            :determinant_dimension,
            determinant_dimension,
            limits.max_determinant_dimension,
        )
    end
    preconditioner_entries = sum((
        BigInt(length(event.preconditioner.data))
        for event in spectral_parent.events
    ); init=BigInt(0))
    _rohpg_limit(
        :preconditioner_entries,
        preconditioner_entries,
        limits.max_preconditioner_entries,
    )
    parent_replay_operations =
        BigInt(spectral_parent.analysis_interval_operation_count) +
        hopf_parent.analysis_interval_operation_count
    _rohpg_limit(
        :parent_replay_interval_operations,
        parent_replay_operations,
        limits.max_parent_replay_interval_operations,
    )
    _rohpg_limit(
        :analysis_interval_operations,
        parent_replay_operations,
        limits.max_analysis_interval_operations,
    )
    if event_count > 0
        dimension = BigInt(length(system.state_names)) + 2
        minimum_determinant_operations = BigInt(event_count) * (
            dimension^2 + dimension + dimension * (dimension - 1) ÷ 2)
        _rohpg_limit(
            :analysis_interval_operations,
            parent_replay_operations +
                minimum_determinant_operations,
            limits.max_analysis_interval_operations,
        )
    end
    return event_count, Int(parent_replay_operations)
end

function _rohpg_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    spectral_parent::ROCompleteSimpleSpectralHopfEventCensus,
    hopf_parent::ROCompleteNondegenerateHopfCensus,
    limits::ROHopfPeriodicOrbitGermLimits,
    cancel_check,
)
    event_count, parent_replay_operations = _rohpg_preflight_inputs(
        system, spectral_parent, hopf_parent, limits)
    validate_ro_polynomial_equilibrium_system(system)
    # Rational{BigInt} is only shallowly immutable.  Detach the admitted
    # parents before replay so later low-level mutation of their nested GMP
    # integers cannot change either rebuilt parent while this lift runs.
    spectral_replay_source = deepcopy(spectral_parent)
    hopf_replay_source = deepcopy(hopf_parent)
    replayed_spectral =
        replay_ro_complete_simple_spectral_hopf_event_census(
            system,
            spectral_replay_source;
            cancel_check=cancel_check,
        )
    replayed_spectral.certificate_sha256 ==
        spectral_parent.certificate_sha256 || throw(ArgumentError(
        "spectral-Hopf parent replay changed its hash"))
    replayed_hopf = replay_ro_complete_nondegenerate_hopf_census(
        system,
        replayed_spectral,
        hopf_replay_source;
        cancel_check=cancel_check,
    )
    replayed_hopf.certificate_sha256 == hopf_parent.certificate_sha256 ||
        throw(ArgumentError(
            "nondegenerate-Hopf parent replay changed its hash"))

    context_limits = _rohsc_regular_limits_with_operation_cap(
        system.limits, limits.max_analysis_interval_operations)
    context = _RORSContext(context_limits, cancel_check)
    context.operations = BigInt(parent_replay_operations)
    events = ROHopfPeriodicOrbitGermEvent[]
    sizehint!(events, event_count)
    for index in 1:event_count
        context.cancel_check()
        spectral_event = replayed_spectral.events[index]
        hopf_event = replayed_hopf.events[index]
        hopf_event.parent_event_certificate_sha256 ==
            spectral_event.certificate_sha256 || throw(ArgumentError(
            "nondegenerate-Hopf event does not match the spectral parent order"))
        preconditioner_sign = _rohpg_exact_determinant_sign(
            spectral_event.preconditioner, context)
        state_jacobian_sign = _rohpg_interval_sign(
            spectral_event.state_jacobian_determinant_enclosure,
            "state-Jacobian determinant",
        )
        # The parent fixes equations `(F..., E, O)` and variables
        # `(x..., lambda, z)`. Its strict Krawczyk bound gives
        # `||I-C*DH|| < 1`, so the homotopy from I to C*DH is nonsingular and
        # `sign(det(DH)) = sign(det(C))`. The Schur identity
        # `det(DH)=det(F_x)*Delta`, together with
        # `d Re(mu)/d lambda = -Delta/(2*(E_z^2+z*O_z^2))`, yields:
        crossing_sign = -preconditioner_sign * state_jacobian_sign
        first_lyapunov_sign = _rohpg_interval_sign(
            hopf_event.first_lyapunov_coefficient_enclosure,
            "first Lyapunov coefficient",
        )
        push!(events, _rohpg_make_event(
            spectral_event.certificate_sha256,
            hopf_event.certificate_sha256,
            system.control_names[1],
            system.control_units[1],
            preconditioner_sign,
            state_jacobian_sign,
            crossing_sign,
            first_lyapunov_sign,
        ))
    end
    context.cancel_check()
    context.operations <= typemax(Int) || throw(
        ROHopfPeriodicOrbitGermLimitExceeded(
            :analysis_interval_operations,
            context.operations,
            limits.max_analysis_interval_operations,
        ))
    operation_count = Int(context.operations)
    event_tuple = Tuple(events)
    hash = _rohpg_census_sha256(
        system.declaration_sha256,
        replayed_spectral.dynamics_binding.declaration_sha256,
        replayed_spectral.certificate_sha256,
        replayed_hopf.certificate_sha256,
        limits,
        event_tuple,
        operation_count,
    )
    return ROCompleteHopfPeriodicOrbitGermCensus(
        _ROHPG_VALIDATED_TOKEN,
        RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_VERSION,
        RO_HOPF_PERIODIC_ORBIT_GERM_THEOREM_VERSION,
        RO_HOPF_CROSSING_ORIENTATION_FORMULA_VERSION,
        system.declaration_sha256,
        replayed_spectral.dynamics_binding.declaration_sha256,
        replayed_spectral.certificate_sha256,
        replayed_hopf.certificate_sha256,
        limits,
        event_tuple,
        event_count,
        length(events),
        operation_count,
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
        false,
        false,
        RO_COMPLETE_HOPF_PERIODIC_ORBIT_GERM_CENSUS_SCOPE,
        hash,
    )
end

"""
    certify_ro_complete_hopf_periodic_orbit_germ_census(
        system, spectral_parent, hopf_parent; ...)

Replay the complete P8s1c0/P8s1c1 chain and apply the classical Hopf theorem
to every nondegenerate event. The returned P8s1c2a artifact proves only a
theorem-level local orbit germ, its original-control side, and its
center-manifold radial stability at onset.
"""
function certify_ro_complete_hopf_periodic_orbit_germ_census(
    system::ROPolynomialEquilibriumSystem,
    spectral_parent::ROCompleteSimpleSpectralHopfEventCensus,
    hopf_parent::ROCompleteNondegenerateHopfCensus;
    limits::ROHopfPeriodicOrbitGermLimits=
        ROHopfPeriodicOrbitGermLimits(),
    cancel_check=() -> nothing,
)
    try
        return _rohpg_certify_exact(
            system, spectral_parent, hopf_parent, limits, cancel_check)
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROHopfPeriodicOrbitGermLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
end

function replay_ro_complete_hopf_periodic_orbit_germ_census(
    system::ROPolynomialEquilibriumSystem,
    spectral_parent::ROCompleteSimpleSpectralHopfEventCensus,
    hopf_parent::ROCompleteNondegenerateHopfCensus,
    certificate::ROCompleteHopfPeriodicOrbitGermCensus;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "Hopf periodic-orbit germ census belongs to a different system"))
    certificate.spectral_parent_census_sha256 ==
        spectral_parent.certificate_sha256 || throw(ArgumentError(
        "Hopf periodic-orbit germ census belongs to a different spectral parent"))
    certificate.hopf_parent_census_sha256 ==
        hopf_parent.certificate_sha256 || throw(ArgumentError(
        "Hopf periodic-orbit germ census belongs to a different nonlinear-Hopf parent"))
    rebuilt = _rohpg_certify_exact(
        system,
        spectral_parent,
        hopf_parent,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "complete Hopf periodic-orbit germ census replay changed its hash"))
    return rebuilt
end

function validate_ro_complete_hopf_periodic_orbit_germ_census(
    system::ROPolynomialEquilibriumSystem,
    spectral_parent::ROCompleteSimpleSpectralHopfEventCensus,
    hopf_parent::ROCompleteNondegenerateHopfCensus,
    certificate::ROCompleteHopfPeriodicOrbitGermCensus;
    cancel_check=() -> nothing,
)
    replay_ro_complete_hopf_periodic_orbit_germ_census(
        system,
        spectral_parent,
        hopf_parent,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
