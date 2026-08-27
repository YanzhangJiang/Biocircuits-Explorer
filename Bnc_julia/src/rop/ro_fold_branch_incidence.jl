const RO_SIMPLE_FOLD_BRANCH_INCIDENCE_VERSION =
    "bne-ro-simple-fold-branch-incidence/v1.0.0"
const RO_FOLD_HALF_BRANCH_INCIDENCE_VERSION =
    "bne-ro-fold-half-branch-incidence/v1.0.0"
const RO_SIMPLE_FOLD_BRANCH_INCIDENCE_SCOPE =
    :validated_local_two_half_branch_incidence_at_one_simple_fold

struct ROFoldBranchIncidenceLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFoldBranchIncidenceLimitExceeded)
    print(
        io,
        "fold-branch incidence limit exceeded during ",
        err.phase,
        ": requested ",
        err.requested,
        ", limit ",
        err.limit,
    )
end

struct ROFoldBranchIncidenceRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(io::IO, err::ROFoldBranchIncidenceRejected)
    print(io, "fold-branch incidence rejected (", err.reason, "): ",
        err.detail)
end

@inline function _rofi_limit(phase::Symbol, requested, limit::Int)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "fold-branch incidence requested work must be nonnegative"))
    amount <= limit || throw(
        ROFoldBranchIncidenceLimitExceeded(phase, amount, limit))
    return nothing
end

"""Cumulative source-replay and exact-analysis budgets for P8s1b1."""
struct ROFoldBranchIncidenceLimits
    max_source_replay_interval_operations::Int
    max_analysis_interval_operations::Int

    function ROFoldBranchIncidenceLimits(
        max_source_replay_interval_operations::Int,
        max_analysis_interval_operations::Int,
    )
        max_source_replay_interval_operations >= 0 || throw(ArgumentError(
            "max_source_replay_interval_operations must be nonnegative"))
        max_analysis_interval_operations > 0 || throw(ArgumentError(
            "max_analysis_interval_operations must be positive"))
        return new(
            max_source_replay_interval_operations,
            max_analysis_interval_operations,
        )
    end
end

function ROFoldBranchIncidenceLimits(;
    max_source_replay_interval_operations::Integer=50_000_000,
    max_analysis_interval_operations::Integer=5_000_000,
)
    values = (
        max_source_replay_interval_operations,
        max_analysis_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("fold-branch incidence limits must fit Int"))
    return ROFoldBranchIncidenceLimits(Int.(values)...)
end

function _rofi_write_limits(io::IO, limits::ROFoldBranchIncidenceLimits)
    _rors_write_token(io, limits.max_source_replay_interval_operations)
    _rors_write_token(io, limits.max_analysis_interval_operations)
    return nothing
end

struct _ROFIValidatedToken end
const _ROFI_VALIDATED_TOKEN = _ROFIValidatedToken()

"""
    ro_fold_branch_coordinate_system(system, chart_state_index)

Permute an exactly one-control polynomial equilibrium system so the selected
state coordinate becomes the local continuation control. The remaining states,
followed by the original control, become the new square state vector. This is a
declarative monomial permutation, not an opaque residual callback.
"""
function ro_fold_branch_coordinate_system(
    system::ROPolynomialEquilibriumSystem,
    chart_state_index::Integer,
)
    validate_ro_polynomial_equilibrium_system(system)
    length(system.control_names) == 1 || throw(ArgumentError(
        "a fold branch-coordinate system requires exactly one control"))
    state_count = length(system.state_names)
    1 <= chart_state_index <= state_count ||
        throw(BoundsError(system.state_names, chart_state_index))
    chart_state_index <= typemax(Int) ||
        throw(BoundsError(system.state_names, chart_state_index))
    chart_index = Int(chart_state_index)
    retained_states = [
        state for state in 1:state_count if state != chart_index
    ]
    state_names = [system.state_names[state] for state in retained_states]
    state_units = [system.state_units[state] for state in retained_states]
    push!(state_names, system.control_names[1])
    push!(state_units, system.control_units[1])
    equations = Vector{Vector{ROPolynomialTerm}}(undef, state_count)
    for equation in 1:state_count
        terms = ROPolynomialTerm[]
        sizehint!(terms, length(system.equations[equation]))
        for source_term in system.equations[equation]
            state_exponents = Int[
                source_term.state_exponents[state]
                for state in retained_states
            ]
            push!(state_exponents, source_term.control_exponents[1])
            control_exponents = [
                source_term.state_exponents[chart_index],
            ]
            push!(terms, ROPolynomialTerm(
                source_term.coefficient,
                state_exponents,
                control_exponents,
            ))
        end
        equations[equation] = terms
    end
    return ROPolynomialEquilibriumSystem(
        state_names=state_names,
        state_units=state_units,
        control_names=[system.state_names[chart_index]],
        control_units=[system.state_units[chart_index]],
        equations=equations,
        limits=system.limits,
    )
end

function _rofi_half_sha256(
    side::Symbol,
    chart_interval::ROExactInterval,
    local_half_patch_sha256::String,
    local_bridge_sha256::String,
    regular_patch_sha256::String,
    local_state_enclosure::Tuple,
    regular_control_enclosure::ROExactInterval,
    regular_remainder_enclosure::Tuple,
    corridor_augmented_enclosure::Tuple,
)
    io = IOBuffer()
    _rors_write_token(io, RO_FOLD_HALF_BRANCH_INCIDENCE_VERSION)
    _rors_write_token(io, side)
    _rors_write_interval(io, chart_interval)
    for hash in (
        local_half_patch_sha256,
        local_bridge_sha256,
        regular_patch_sha256,
    )
        _rors_write_token(io, hash)
    end
    _rors_write_interval_vector(io, local_state_enclosure)
    _rors_write_interval(io, regular_control_enclosure)
    _rors_write_interval_vector(io, regular_remainder_enclosure)
    _rors_write_interval_vector(io, corridor_augmented_enclosure)
    for value in (
        true, # local_half_strictly_separated_from_event
        true, # local_tube_strictly_inside_regular_patch
        true, # same_equilibrium_branch_on_overlap
        true, # intervening_fold_event_excluded
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""One side of a validated local fold chart attached to one regular patch."""
struct ROFoldHalfBranchIncidence
    version::String
    side::Symbol
    chart_interval::ROExactInterval
    local_half_patch_sha256::String
    local_bridge_sha256::String
    regular_patch_sha256::String
    local_state_enclosure::Tuple
    regular_control_enclosure::ROExactInterval
    regular_remainder_enclosure::Tuple
    corridor_augmented_enclosure::Tuple
    local_half_strictly_separated_from_event::Bool
    local_tube_strictly_inside_regular_patch::Bool
    same_equilibrium_branch_on_overlap::Bool
    intervening_fold_event_excluded::Bool
    evidence_sha256::String

    function ROFoldHalfBranchIncidence(
        ::_ROFIValidatedToken,
        version::String,
        side::Symbol,
        chart_interval::ROExactInterval,
        local_half_patch_sha256::String,
        local_bridge_sha256::String,
        regular_patch_sha256::String,
        local_state_enclosure::Tuple,
        regular_control_enclosure::ROExactInterval,
        regular_remainder_enclosure::Tuple,
        corridor_augmented_enclosure::Tuple,
        local_half_strictly_separated_from_event::Bool,
        local_tube_strictly_inside_regular_patch::Bool,
        same_equilibrium_branch_on_overlap::Bool,
        intervening_fold_event_excluded::Bool,
        evidence_sha256::String,
    )
        version == RO_FOLD_HALF_BRANCH_INCIDENCE_VERSION ||
            throw(ArgumentError(
                "fold half-branch incidence version mismatch"))
        side in (:lower_chart_side, :upper_chart_side) ||
            throw(ArgumentError("unknown fold half-branch side"))
        chart_interval.lower > 0 &&
            chart_interval.lower < chart_interval.upper ||
            throw(ArgumentError(
                "fold half-branch chart interval must be positive and nondegenerate"))
        for (hash, label) in (
            (local_half_patch_sha256, "local_half_patch_sha256"),
            (local_bridge_sha256, "local_bridge_sha256"),
            (regular_patch_sha256, "regular_patch_sha256"),
        )
            _rors_validate_sha256(hash, label)
        end
        !isempty(local_state_enclosure) &&
            length(regular_remainder_enclosure) ==
                length(local_state_enclosure) || throw(DimensionMismatch(
            "fold half-branch state/remainder dimensions do not match"))
        all(interval -> interval isa ROExactInterval,
            local_state_enclosure) &&
            all(interval -> interval isa ROExactInterval,
                regular_remainder_enclosure) &&
            length(corridor_augmented_enclosure) ==
                length(local_state_enclosure) + 1 &&
            all(interval -> interval isa ROExactInterval,
                corridor_augmented_enclosure) || throw(ArgumentError(
            "fold half-branch enclosures must be exact intervals"))
        regular_control_enclosure.lower > 0 &&
            regular_control_enclosure.lower <
                regular_control_enclosure.upper || throw(ArgumentError(
            "fold half-branch regular-control enclosure must be positive and nondegenerate"))
        local_half_strictly_separated_from_event || throw(ArgumentError(
            "fold half-branch is not strictly separated from the event"))
        local_tube_strictly_inside_regular_patch || throw(ArgumentError(
            "local half-branch tube is not strictly inside its regular patch"))
        same_equilibrium_branch_on_overlap || throw(ArgumentError(
            "fold half-branch lost its replayed branch identity"))
        intervening_fold_event_excluded || throw(ArgumentError(
            "an intervening fold event was not excluded"))
        _rors_validate_sha256(evidence_sha256, "evidence_sha256")
        expected = _rofi_half_sha256(
            side,
            chart_interval,
            local_half_patch_sha256,
            local_bridge_sha256,
            regular_patch_sha256,
            local_state_enclosure,
            regular_control_enclosure,
            regular_remainder_enclosure,
            corridor_augmented_enclosure,
        )
        evidence_sha256 == expected || throw(ArgumentError(
            "fold half-branch incidence hash mismatch"))
        return new(
            version,
            side,
            chart_interval,
            local_half_patch_sha256,
            local_bridge_sha256,
            regular_patch_sha256,
            local_state_enclosure,
            regular_control_enclosure,
            regular_remainder_enclosure,
            corridor_augmented_enclosure,
            true,
            true,
            true,
            true,
            evidence_sha256,
        )
    end
end

function _rofi_make_half(
    side::Symbol,
    chart_interval::ROExactInterval,
    local_half_patch_sha256::String,
    local_bridge_sha256::String,
    regular_patch_sha256::String,
    local_state_enclosure::Tuple,
    regular_control_enclosure::ROExactInterval,
    regular_remainder_enclosure::Tuple,
    corridor_augmented_enclosure::Tuple,
)
    hash = _rofi_half_sha256(
        side,
        chart_interval,
        local_half_patch_sha256,
        local_bridge_sha256,
        regular_patch_sha256,
        local_state_enclosure,
        regular_control_enclosure,
        regular_remainder_enclosure,
        corridor_augmented_enclosure,
    )
    return ROFoldHalfBranchIncidence(
        _ROFI_VALIDATED_TOKEN,
        RO_FOLD_HALF_BRANCH_INCIDENCE_VERSION,
        side,
        chart_interval,
        local_half_patch_sha256,
        local_bridge_sha256,
        regular_patch_sha256,
        local_state_enclosure,
        regular_control_enclosure,
        regular_remainder_enclosure,
        corridor_augmented_enclosure,
        true,
        true,
        true,
        true,
        hash,
    )
end

function _rofi_certificate_sha256(
    system_declaration_sha256::String,
    limits::ROFoldBranchIncidenceLimits,
    fold_event_census_sha256::String,
    fold_event_certificate_sha256::String,
    chart_state_index::Int,
    branch_coordinate_system_sha256::String,
    central_local_patch_sha256::String,
    lower_half_local_patch_sha256::String,
    upper_half_local_patch_sha256::String,
    lower_local_bridge_sha256::String,
    upper_local_bridge_sha256::String,
    lower_regular_patch_sha256::String,
    upper_regular_patch_sha256::String,
    event_augmented_root_enclosure::Tuple,
    event_local_remainder_enclosure::Tuple,
    lower_incidence::ROFoldHalfBranchIncidence,
    upper_incidence::ROFoldHalfBranchIncidence,
    source_replay_interval_operation_count::Int,
    analysis_interval_operation_count::Int,
)
    io = IOBuffer()
    _rors_write_token(io, RO_SIMPLE_FOLD_BRANCH_INCIDENCE_VERSION)
    _rors_write_token(io, system_declaration_sha256)
    _rofi_write_limits(io, limits)
    for hash in (
        fold_event_census_sha256,
        fold_event_certificate_sha256,
    )
        _rors_write_token(io, hash)
    end
    _rors_write_token(io, chart_state_index)
    for hash in (
        branch_coordinate_system_sha256,
        central_local_patch_sha256,
        lower_half_local_patch_sha256,
        upper_half_local_patch_sha256,
        lower_local_bridge_sha256,
        upper_local_bridge_sha256,
        lower_regular_patch_sha256,
        upper_regular_patch_sha256,
    )
        _rors_write_token(io, hash)
    end
    _rors_write_interval_vector(io, event_augmented_root_enclosure)
    _rors_write_interval_vector(io, event_local_remainder_enclosure)
    _rors_write_token(io, lower_incidence.evidence_sha256)
    _rors_write_token(io, upper_incidence.evidence_sha256)
    _rors_write_token(io, source_replay_interval_operation_count)
    _rors_write_token(io, analysis_interval_operation_count)
    for value in (
        true,  # fold_event_census_replayed
        true,  # selected_event_simple_fold_certified
        true,  # event_on_local_branch_certified
        true,  # two_local_half_branches_certified
        true,  # adjacent_regular_sheet_incidence_certified
        false, # original_control_two_root_side_certified
        false, # remote_component_identity_certified
        false, # root_population_complete_inside_declared_domain
        false, # stable_root_population_complete
        false, # hopf_event_set_complete
        false, # global_continuation_certified
        false, # true_hysteresis_certified
        RO_SIMPLE_FOLD_BRANCH_INCIDENCE_SCOPE,
    )
        _rors_write_token(io, value)
    end
    return bytes2hex(SHA.sha256(take!(io)))
end

"""
P8s1b1 incidence for exactly one selected simple fold. A central local branch
coordinate patch contains the event; two bridge-connected local half patches
lie on opposite sides; each half tube is strictly contained in one replayed
regular patch in the original `(x, lambda)` coordinates.

This artifact does not identify or compare remote branch components.
"""
struct ROSimpleFoldBranchIncidenceCertificate
    version::String
    system_declaration_sha256::String
    limits::ROFoldBranchIncidenceLimits
    fold_event_census_sha256::String
    fold_event_certificate_sha256::String
    chart_state_index::Int
    branch_coordinate_system_sha256::String
    central_local_patch_sha256::String
    lower_half_local_patch_sha256::String
    upper_half_local_patch_sha256::String
    lower_local_bridge_sha256::String
    upper_local_bridge_sha256::String
    lower_regular_patch_sha256::String
    upper_regular_patch_sha256::String
    event_augmented_root_enclosure::Tuple
    event_local_remainder_enclosure::Tuple
    lower_incidence::ROFoldHalfBranchIncidence
    upper_incidence::ROFoldHalfBranchIncidence
    source_replay_interval_operation_count::Int
    analysis_interval_operation_count::Int
    fold_event_census_replayed::Bool
    selected_event_simple_fold_certified::Bool
    event_on_local_branch_certified::Bool
    two_local_half_branches_certified::Bool
    adjacent_regular_sheet_incidence_certified::Bool
    original_control_two_root_side_certified::Bool
    remote_component_identity_certified::Bool
    root_population_complete_inside_declared_domain::Bool
    stable_root_population_complete::Bool
    hopf_event_set_complete::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROSimpleFoldBranchIncidenceCertificate(
        ::_ROFIValidatedToken,
        version::String,
        system_declaration_sha256::String,
        limits::ROFoldBranchIncidenceLimits,
        fold_event_census_sha256::String,
        fold_event_certificate_sha256::String,
        chart_state_index::Int,
        branch_coordinate_system_sha256::String,
        central_local_patch_sha256::String,
        lower_half_local_patch_sha256::String,
        upper_half_local_patch_sha256::String,
        lower_local_bridge_sha256::String,
        upper_local_bridge_sha256::String,
        lower_regular_patch_sha256::String,
        upper_regular_patch_sha256::String,
        event_augmented_root_enclosure::Tuple,
        event_local_remainder_enclosure::Tuple,
        lower_incidence::ROFoldHalfBranchIncidence,
        upper_incidence::ROFoldHalfBranchIncidence,
        source_replay_interval_operation_count::Int,
        analysis_interval_operation_count::Int,
        fold_event_census_replayed::Bool,
        selected_event_simple_fold_certified::Bool,
        event_on_local_branch_certified::Bool,
        two_local_half_branches_certified::Bool,
        adjacent_regular_sheet_incidence_certified::Bool,
        original_control_two_root_side_certified::Bool,
        remote_component_identity_certified::Bool,
        root_population_complete_inside_declared_domain::Bool,
        stable_root_population_complete::Bool,
        hopf_event_set_complete::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_SIMPLE_FOLD_BRANCH_INCIDENCE_VERSION ||
            throw(ArgumentError(
                "simple-fold branch-incidence version mismatch"))
        evidence_scope == RO_SIMPLE_FOLD_BRANCH_INCIDENCE_SCOPE ||
            throw(ArgumentError(
                "simple-fold branch-incidence evidence scope mismatch"))
        for (hash, label) in (
            (system_declaration_sha256, "system_declaration_sha256"),
            (fold_event_census_sha256, "fold_event_census_sha256"),
            (fold_event_certificate_sha256,
                "fold_event_certificate_sha256"),
            (branch_coordinate_system_sha256,
                "branch_coordinate_system_sha256"),
            (central_local_patch_sha256, "central_local_patch_sha256"),
            (lower_half_local_patch_sha256,
                "lower_half_local_patch_sha256"),
            (upper_half_local_patch_sha256,
                "upper_half_local_patch_sha256"),
            (lower_local_bridge_sha256, "lower_local_bridge_sha256"),
            (upper_local_bridge_sha256, "upper_local_bridge_sha256"),
            (lower_regular_patch_sha256, "lower_regular_patch_sha256"),
            (upper_regular_patch_sha256, "upper_regular_patch_sha256"),
            (certificate_sha256, "certificate_sha256"),
        )
            _rors_validate_sha256(hash, label)
        end
        variable_count = length(event_augmented_root_enclosure)
        variable_count > 1 || throw(ArgumentError(
            "fold incidence requires at least one state and one control"))
        state_count = variable_count - 1
        1 <= chart_state_index <= state_count || throw(BoundsError(
            1:state_count, chart_state_index))
        length(event_local_remainder_enclosure) == state_count ||
            throw(DimensionMismatch(
                "event local remainder has the wrong dimension"))
        all(interval -> interval isa ROExactInterval,
            event_augmented_root_enclosure) &&
            all(interval -> interval isa ROExactInterval,
                event_local_remainder_enclosure) || throw(ArgumentError(
            "fold-incidence event enclosures must be exact intervals"))
        lower_incidence.side == :lower_chart_side &&
            upper_incidence.side == :upper_chart_side ||
            throw(ArgumentError(
                "fold-incidence half branches have the wrong side labels"))
        lower_incidence.chart_interval.upper <
            event_augmented_root_enclosure[chart_state_index].lower ||
            throw(ArgumentError(
                "lower half branch is not strictly below the event enclosure"))
        event_augmented_root_enclosure[chart_state_index].upper <
            upper_incidence.chart_interval.lower || throw(ArgumentError(
            "upper half branch is not strictly above the event enclosure"))
        lower_incidence.local_half_patch_sha256 ==
            lower_half_local_patch_sha256 &&
            upper_incidence.local_half_patch_sha256 ==
                upper_half_local_patch_sha256 || throw(ArgumentError(
            "fold-incidence local half-patch identities do not match"))
        lower_incidence.local_bridge_sha256 ==
            lower_local_bridge_sha256 &&
            upper_incidence.local_bridge_sha256 ==
                upper_local_bridge_sha256 || throw(ArgumentError(
            "fold-incidence bridge identities do not match"))
        lower_incidence.regular_patch_sha256 ==
            lower_regular_patch_sha256 &&
            upper_incidence.regular_patch_sha256 ==
                upper_regular_patch_sha256 || throw(ArgumentError(
            "fold-incidence regular-patch identities do not match"))
        lower_half_local_patch_sha256 != upper_half_local_patch_sha256 ||
            throw(ArgumentError(
                "the two local half patches must be distinct"))
        lower_regular_patch_sha256 != upper_regular_patch_sha256 ||
            throw(ArgumentError(
                "the two incident regular patches must be distinct"))
        for incidence in (lower_incidence, upper_incidence)
            expected_half_hash = _rofi_half_sha256(
                incidence.side,
                incidence.chart_interval,
                incidence.local_half_patch_sha256,
                incidence.local_bridge_sha256,
                incidence.regular_patch_sha256,
                incidence.local_state_enclosure,
                incidence.regular_control_enclosure,
                incidence.regular_remainder_enclosure,
                incidence.corridor_augmented_enclosure,
            )
            incidence.evidence_sha256 == expected_half_hash ||
                throw(ArgumentError(
                    "nested fold half-branch incidence hash mismatch"))
        end
        source_replay_interval_operation_count >= 0 &&
            analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "fold-incidence operation counts must be nonnegative"))
        _rofi_limit(
            :source_replay_interval_operations,
            source_replay_interval_operation_count,
            limits.max_source_replay_interval_operations,
        )
        _rofi_limit(
            :analysis_interval_operations,
            analysis_interval_operation_count,
            limits.max_analysis_interval_operations,
        )
        fold_event_census_replayed || throw(ArgumentError(
            "fold-event census replay evidence was lost"))
        selected_event_simple_fold_certified || throw(ArgumentError(
            "selected event is not certified as a simple fold"))
        event_on_local_branch_certified || throw(ArgumentError(
            "fold event is not certified on the local branch chart"))
        two_local_half_branches_certified || throw(ArgumentError(
            "two local fold half branches were not certified"))
        adjacent_regular_sheet_incidence_certified || throw(ArgumentError(
            "adjacent regular-sheet incidence was not certified"))
        original_control_two_root_side_certified && throw(ArgumentError(
            "P8s1b1 does not orient the original-control two-root side"))
        remote_component_identity_certified && throw(ArgumentError(
            "P8s1b1 cannot identify remote branch components"))
        root_population_complete_inside_declared_domain &&
            throw(ArgumentError(
                "P8s1b1 does not enumerate the complete root population"))
        stable_root_population_complete && throw(ArgumentError(
            "P8s1b1 does not classify the stable-root population"))
        hopf_event_set_complete && throw(ArgumentError(
            "P8s1b1 does not certify Hopf-event completeness"))
        global_continuation_certified && throw(ArgumentError(
            "P8s1b1 is not a global continuation certificate"))
        true_hysteresis_certified && throw(ArgumentError(
            "static fold incidence cannot certify true hysteresis"))
        expected = _rofi_certificate_sha256(
            system_declaration_sha256,
            limits,
            fold_event_census_sha256,
            fold_event_certificate_sha256,
            chart_state_index,
            branch_coordinate_system_sha256,
            central_local_patch_sha256,
            lower_half_local_patch_sha256,
            upper_half_local_patch_sha256,
            lower_local_bridge_sha256,
            upper_local_bridge_sha256,
            lower_regular_patch_sha256,
            upper_regular_patch_sha256,
            event_augmented_root_enclosure,
            event_local_remainder_enclosure,
            lower_incidence,
            upper_incidence,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
        )
        certificate_sha256 == expected || throw(ArgumentError(
            "simple-fold branch-incidence hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            limits,
            fold_event_census_sha256,
            fold_event_certificate_sha256,
            chart_state_index,
            branch_coordinate_system_sha256,
            central_local_patch_sha256,
            lower_half_local_patch_sha256,
            upper_half_local_patch_sha256,
            lower_local_bridge_sha256,
            upper_local_bridge_sha256,
            lower_regular_patch_sha256,
            upper_regular_patch_sha256,
            event_augmented_root_enclosure,
            event_local_remainder_enclosure,
            lower_incidence,
            upper_incidence,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
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

function _rofi_source_operation_count(
    census::ROCompleteSimpleFoldEventCensus,
    central_local::RORegularSheetPatchCertificate,
    lower_local::RORegularSheetPatchCertificate,
    upper_local::RORegularSheetPatchCertificate,
    lower_bridge::RORegularSheetBridgeCertificate,
    upper_bridge::RORegularSheetBridgeCertificate,
    lower_regular::RORegularSheetPatchCertificate,
    upper_regular::RORegularSheetPatchCertificate,
    limits::ROFoldBranchIncidenceLimits,
)
    count = BigInt(census.analysis_interval_operation_count) +
        central_local.exact_operation_count +
        lower_local.exact_operation_count +
        upper_local.exact_operation_count +
        lower_bridge.exact_operation_count +
        upper_bridge.exact_operation_count +
        lower_regular.exact_operation_count +
        upper_regular.exact_operation_count
    _rofi_limit(
        :source_replay_interval_operations,
        count,
        limits.max_source_replay_interval_operations,
    )
    count <= typemax(Int) || throw(ROFoldBranchIncidenceLimitExceeded(
        :source_replay_interval_operations,
        count,
        limits.max_source_replay_interval_operations,
    ))
    return Int(count)
end

function _rofi_replay_fold_census_event(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    cancel_check,
)
    # Certificate payloads contain Rational{BigInt}; detach the shallowly
    # immutable source before replay so the rebuilt census cannot retain GMP
    # integer aliases into the caller-owned object.
    replay_source = deepcopy(census)
    replayed_census = replay_ro_complete_simple_fold_event_census(
        system, replay_source; cancel_check=cancel_check)
    replayed_event_index = findfirst(candidate ->
        candidate.certificate_sha256 == event.certificate_sha256,
        replayed_census.events,
    )
    replayed_event_index === nothing && throw(ArgumentError(
        "selected fold event disappeared during census replay"))
    return replayed_census, replayed_census.events[replayed_event_index]
end

function _rofi_replay_sources(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    lower_local_patch::RORegularSheetPatchCertificate,
    upper_local_patch::RORegularSheetPatchCertificate,
    lower_local_bridge::RORegularSheetBridgeCertificate,
    upper_local_bridge::RORegularSheetBridgeCertificate,
    lower_regular_patch::RORegularSheetPatchCertificate,
    upper_regular_patch::RORegularSheetPatchCertificate,
    chart_state_index::Int,
    limits::ROFoldBranchIncidenceLimits,
    cancel_check,
)
    validate_ro_polynomial_equilibrium_system(system)
    length(system.control_names) == 1 || throw(ArgumentError(
        "P8s1b1 requires exactly one original control coordinate"))
    state_count = length(system.state_names)
    1 <= chart_state_index <= state_count ||
        throw(BoundsError(system.state_names, chart_state_index))
    cancel_check()
    branch_system = ro_fold_branch_coordinate_system(
        system, chart_state_index)
    census.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "fold-event census belongs to another polynomial system"))
    any(candidate -> candidate.certificate_sha256 ==
        event.certificate_sha256, census.events) || throw(ArgumentError(
        "selected fold event is absent from the supplied census"))
    event.simple_fold_certified || throw(ArgumentError(
        "selected event is not certified as a simple fold"))
    for (patch, label) in (
        (central_local_patch, "central local patch"),
        (lower_local_patch, "lower local patch"),
        (upper_local_patch, "upper local patch"),
    )
        patch.system_declaration_sha256 ==
            branch_system.declaration_sha256 || throw(ArgumentError(
            "$label belongs to another branch-coordinate system"))
    end
    for (patch, label) in (
        (lower_regular_patch, "lower regular patch"),
        (upper_regular_patch, "upper regular patch"),
    )
        patch.system_declaration_sha256 == system.declaration_sha256 ||
            throw(ArgumentError("$label belongs to another original system"))
    end
    lower_local_bridge.system_declaration_sha256 ==
        branch_system.declaration_sha256 &&
        upper_local_bridge.system_declaration_sha256 ==
            branch_system.declaration_sha256 || throw(ArgumentError(
        "a local bridge belongs to another branch-coordinate system"))
    lower_local_bridge.parent_patch_sha256 ==
        central_local_patch.certificate_sha256 &&
        lower_local_bridge.child_patch_sha256 ==
            lower_local_patch.certificate_sha256 || throw(ArgumentError(
        "lower local bridge does not bind central -> lower patches"))
    upper_local_bridge.parent_patch_sha256 ==
        central_local_patch.certificate_sha256 &&
        upper_local_bridge.child_patch_sha256 ==
            upper_local_patch.certificate_sha256 || throw(ArgumentError(
        "upper local bridge does not bind central -> upper patches"))
    lower_local_bridge.bridge_patch.control_box ==
        lower_local_patch.control_box || throw(ArgumentError(
        "lower bridge must cover the complete lower local patch control box"))
    upper_local_bridge.bridge_patch.control_box ==
        upper_local_patch.control_box || throw(ArgumentError(
        "upper bridge must cover the complete upper local patch control box"))
    source_operations = _rofi_source_operation_count(
        census,
        central_local_patch,
        lower_local_patch,
        upper_local_patch,
        lower_local_bridge,
        upper_local_bridge,
        lower_regular_patch,
        upper_regular_patch,
        limits,
    )
    cancel_check()
    replayed_census, replayed_event = _rofi_replay_fold_census_event(
        system, census, event, cancel_check)
    central_local_replay_source = deepcopy(central_local_patch)
    lower_local_replay_source = deepcopy(lower_local_patch)
    upper_local_replay_source = deepcopy(upper_local_patch)
    lower_bridge_replay_source = deepcopy(lower_local_bridge)
    upper_bridge_replay_source = deepcopy(upper_local_bridge)
    lower_regular_replay_source = deepcopy(lower_regular_patch)
    upper_regular_replay_source = deepcopy(upper_regular_patch)
    replayed_central_local_patch = replay_ro_regular_sheet_patch(
        branch_system,
        central_local_replay_source;
        cancel_check=cancel_check,
    )
    replayed_lower_local_patch = replay_ro_regular_sheet_patch(
        branch_system,
        lower_local_replay_source;
        cancel_check=cancel_check,
    )
    replayed_upper_local_patch = replay_ro_regular_sheet_patch(
        branch_system,
        upper_local_replay_source;
        cancel_check=cancel_check,
    )
    replayed_lower_local_bridge = replay_ro_regular_sheet_bridge(
        branch_system,
        replayed_central_local_patch,
        replayed_lower_local_patch,
        lower_bridge_replay_source;
        cancel_check=cancel_check,
    )
    replayed_upper_local_bridge = replay_ro_regular_sheet_bridge(
        branch_system,
        replayed_central_local_patch,
        replayed_upper_local_patch,
        upper_bridge_replay_source;
        cancel_check=cancel_check,
    )
    replayed_lower_regular_patch = replay_ro_regular_sheet_patch(
        system, lower_regular_replay_source; cancel_check=cancel_check)
    replayed_upper_regular_patch = replay_ro_regular_sheet_patch(
        system, upper_regular_replay_source; cancel_check=cancel_check)
    return (
        branch_system=branch_system,
        census=replayed_census,
        event=replayed_event,
        central_local_patch=replayed_central_local_patch,
        lower_local_patch=replayed_lower_local_patch,
        upper_local_patch=replayed_upper_local_patch,
        lower_local_bridge=replayed_lower_local_bridge,
        upper_local_bridge=replayed_upper_local_bridge,
        lower_regular_patch=replayed_lower_regular_patch,
        upper_regular_patch=replayed_upper_regular_patch,
        source_operations=source_operations,
    )
end

function _rofi_predictor_interval(
    patch::RORegularSheetPatchCertificate,
    state::Int,
    control_interval::ROExactInterval,
    context::_RORSContext,
)
    slope = patch.predictor_slope[state, 1]
    displacement = _rors_subtract(
        context,
        control_interval,
        _rors_point(context, patch.control_reference[1]),
    )
    return _rors_add(
        context,
        _rors_point(context, patch.state_reference[state]),
        _rors_multiply(
            context,
            _rors_point(context, slope),
            displacement,
        ),
    )
end

function _rofi_event_on_local_branch(
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    chart_state_index::Int,
    context::_RORSContext,
)
    state_count = length(event.event_box) - 1
    length(central_local_patch.state_reference) == state_count ||
        throw(DimensionMismatch(
            "central local patch has the wrong state dimension"))
    event_root = Vector{ROExactInterval}(undef, state_count + 1)
    for variable in 1:(state_count + 1)
        event_root[variable] = _rors_add(
            context,
            _rors_point(context, event.center[variable]),
            event.krawczyk_offset_image[variable],
        )
    end
    chart_enclosure = event_root[chart_state_index]
    _rors_strict_subset(
        chart_enclosure, central_local_patch.control_box[1]) ||
        throw(ROFoldBranchIncidenceRejected(
            :event_chart_coordinate_outside_central_patch,
            "the fold-event chart enclosure is not strictly inside the central local patch",
        ))
    retained_states = [
        state for state in 1:state_count if state != chart_state_index
    ]
    local_state_root = ROExactInterval[
        event_root[state] for state in retained_states
    ]
    push!(local_state_root, event_root[state_count + 1])
    local_remainder = Vector{ROExactInterval}(undef, state_count)
    for state in 1:state_count
        predictor = _rofi_predictor_interval(
            central_local_patch, state, chart_enclosure, context)
        local_remainder[state] = _rors_subtract(
            context, local_state_root[state], predictor)
        _rors_strict_subset(
            local_remainder[state],
            central_local_patch.remainder_box[state],
        ) || throw(ROFoldBranchIncidenceRejected(
            :event_not_inside_central_local_tube,
            "fold-event root enclosure is not strictly inside central local remainder $state",
        ))
    end
    return Tuple(event_root), Tuple(local_remainder)
end

function _rofi_local_affine_data(
    local_patch::RORegularSheetPatchCertificate,
    state::Int,
    context::_RORSContext,
)
    slope = local_patch.predictor_slope[state, 1]
    intercept = _rors_exact_subtract(
        context,
        local_patch.state_reference[state],
        _rors_exact_multiply(
            context, slope, local_patch.control_reference[1]),
    )
    return intercept, slope
end

@inline function _rofi_disjoint(
    left::ROExactInterval,
    right::ROExactInterval,
)
    return left.upper < right.lower || right.upper < left.lower
end

function _rofi_event_root_enclosure(
    event::ROSimpleFoldEvent,
    context::_RORSContext,
)
    return [
        _rors_add(
            context,
            _rors_point(context, event.center[variable]),
            event.krawczyk_offset_image[variable],
        )
        for variable in eachindex(event.center)
    ]
end

function _rofi_fold_free_corridor(
    side::Symbol,
    census::ROCompleteSimpleFoldEventCensus,
    selected_event::ROSimpleFoldEvent,
    event_chart_enclosure::ROExactInterval,
    central_local_patch::RORegularSheetPatchCertificate,
    half_chart_interval::ROExactInterval,
    chart_state_index::Int,
    context::_RORSContext,
)
    corridor_chart = if side == :lower_chart_side
        half_chart_interval.upper < event_chart_enclosure.lower || throw(
            ROFoldBranchIncidenceRejected(
                :lower_half_not_separated_from_event,
                "lower local patch is not strictly below the event enclosure",
            ))
        _rors_interval(
            context,
            half_chart_interval.lower,
            event_chart_enclosure.upper,
        )
    elseif side == :upper_chart_side
        event_chart_enclosure.upper < half_chart_interval.lower || throw(
            ROFoldBranchIncidenceRejected(
                :upper_half_not_separated_from_event,
                "upper local patch is not strictly above the event enclosure",
            ))
        _rors_interval(
            context,
            event_chart_enclosure.lower,
            half_chart_interval.upper,
        )
    else
        throw(ArgumentError("unknown fold-corridor side"))
    end
    corridor_chart.lower < corridor_chart.upper || throw(
        ROFoldBranchIncidenceRejected(
            :degenerate_fold_corridor,
            "$side fold corridor has no positive width",
        ))
    _rors_strict_subset(
        corridor_chart, central_local_patch.control_box[1]) || throw(
        ROFoldBranchIncidenceRejected(
            :fold_corridor_outside_central_patch,
            "$side fold corridor is not strictly inside the central chart patch",
        ))
    state_count = length(census.augmented_variable_names) - 1
    local_state_enclosure = [
        _rors_add(
            context,
            _rofi_predictor_interval(
                central_local_patch, state, corridor_chart, context),
            central_local_patch.remainder_box[state],
        )
        for state in 1:state_count
    ]
    augmented_enclosure = Vector{ROExactInterval}(
        undef, state_count + 1)
    retained_position = 0
    for original_state in 1:state_count
        if original_state == chart_state_index
            augmented_enclosure[original_state] = corridor_chart
        else
            retained_position += 1
            augmented_enclosure[original_state] =
                local_state_enclosure[retained_position]
        end
    end
    augmented_enclosure[state_count + 1] =
        local_state_enclosure[state_count]
    for variable in 1:(state_count + 1)
        _rors_strict_subset(
            augmented_enclosure[variable],
            census.declared_event_box[variable],
        ) || throw(ROFoldBranchIncidenceRejected(
            :fold_corridor_outside_complete_census_domain,
            "$side fold corridor leaves the complete event-census domain in augmented variable $variable",
        ))
    end
    for other_event in census.events
        other_event.certificate_sha256 ==
            selected_event.certificate_sha256 && continue
        other_root = _rofi_event_root_enclosure(other_event, context)
        any(variable -> _rofi_disjoint(
                other_root[variable], augmented_enclosure[variable]),
            1:(state_count + 1)) || throw(
            ROFoldBranchIncidenceRejected(
                :intervening_fold_event_not_excluded,
                "$side corridor is not separated from fold event $(other_event.certificate_sha256)",
            ))
    end
    return Tuple(augmented_enclosure)
end

function _rofi_half_incidence(
    side::Symbol,
    event_chart_enclosure::ROExactInterval,
    local_patch::RORegularSheetPatchCertificate,
    local_bridge::RORegularSheetBridgeCertificate,
    regular_patch::RORegularSheetPatchCertificate,
    corridor_augmented_enclosure::Tuple,
    chart_state_index::Int,
    context::_RORSContext,
)
    state_count = length(regular_patch.state_reference)
    length(local_patch.state_reference) == state_count ||
        throw(DimensionMismatch(
            "local and original fold patches have different state counts"))
    chart_interval = local_patch.control_box[1]
    if side == :lower_chart_side
        chart_interval.upper < event_chart_enclosure.lower || throw(
            ROFoldBranchIncidenceRejected(
                :lower_half_not_separated_from_event,
                "lower local patch is not strictly below the event enclosure",
            ))
    elseif side == :upper_chart_side
        event_chart_enclosure.upper < chart_interval.lower || throw(
            ROFoldBranchIncidenceRejected(
                :upper_half_not_separated_from_event,
                "upper local patch is not strictly above the event enclosure",
            ))
    else
        throw(ArgumentError("unknown local fold half-branch side"))
    end
    local_bridge.parent_tube_contained &&
        local_bridge.child_tube_contained &&
        local_bridge.same_root_on_overlap || throw(ArgumentError(
        "local fold bridge lost its branch-identity conclusions"))

    local_state_enclosure = Vector{ROExactInterval}(undef, state_count)
    local_intercepts = Vector{_RORSExact}(undef, state_count)
    local_slopes = Vector{_RORSExact}(undef, state_count)
    for state in 1:state_count
        intercept, slope = _rofi_local_affine_data(
            local_patch, state, context)
        local_intercepts[state] = intercept
        local_slopes[state] = slope
        affine = _rors_add(
            context,
            _rors_point(context, intercept),
            _rors_multiply(
                context,
                _rors_point(context, slope),
                chart_interval,
            ),
        )
        local_state_enclosure[state] = _rors_add(
            context, affine, local_patch.remainder_box[state])
    end
    lambda_state = state_count
    regular_control_enclosure = local_state_enclosure[lambda_state]
    _rors_strict_subset(
        regular_control_enclosure, regular_patch.control_box[1]) ||
        throw(ROFoldBranchIncidenceRejected(
            :local_control_not_inside_regular_patch,
            "$side local lambda enclosure is not strictly inside its regular patch control box",
        ))

    lambda_intercept = local_intercepts[lambda_state]
    lambda_slope = local_slopes[lambda_state]
    lambda_remainder = local_patch.remainder_box[lambda_state]
    regular_slope = _rors_exact_matrix_values(
        regular_patch.predictor_slope)
    regular_remainder = Vector{ROExactInterval}(undef, state_count)
    retained_position = 0
    for original_state in 1:state_count
        if original_state == chart_state_index
            state_intercept = zero(_RORSExact)
            state_slope = one(_RORSExact)
            state_remainder = _rors_point(context, zero(_RORSExact))
        else
            retained_position += 1
            state_intercept = local_intercepts[retained_position]
            state_slope = local_slopes[retained_position]
            state_remainder = local_patch.remainder_box[retained_position]
        end
        original_predictor_slope = regular_slope[original_state, 1]
        intercept = _rors_exact_subtract(
            context,
            state_intercept,
            regular_patch.state_reference[original_state],
        )
        intercept = _rors_exact_add(
            context,
            intercept,
            _rors_exact_multiply(
                context,
                original_predictor_slope,
                regular_patch.control_reference[1],
            ),
        )
        intercept = _rors_exact_subtract(
            context,
            intercept,
            _rors_exact_multiply(
                context,
                original_predictor_slope,
                lambda_intercept,
            ),
        )
        slope = _rors_exact_subtract(
            context,
            state_slope,
            _rors_exact_multiply(
                context,
                original_predictor_slope,
                lambda_slope,
            ),
        )
        affine_difference = _rors_add(
            context,
            _rors_point(context, intercept),
            _rors_multiply(
                context,
                _rors_point(context, slope),
                chart_interval,
            ),
        )
        remainder_difference = _rors_subtract(
            context,
            state_remainder,
            _rors_multiply(
                context,
                _rors_point(context, original_predictor_slope),
                lambda_remainder,
            ),
        )
        regular_remainder[original_state] = _rors_add(
            context, affine_difference, remainder_difference)
        _rors_strict_subset(
            regular_remainder[original_state],
            regular_patch.remainder_box[original_state],
        ) || throw(ROFoldBranchIncidenceRejected(
            :local_tube_not_inside_regular_patch,
            "$side local tube is not strictly inside regular remainder $original_state",
        ))
    end
    return _rofi_make_half(
        side,
        chart_interval,
        local_patch.certificate_sha256,
        local_bridge.certificate_sha256,
        regular_patch.certificate_sha256,
        Tuple(local_state_enclosure),
        regular_control_enclosure,
        Tuple(regular_remainder),
        corridor_augmented_enclosure,
    )
end

function _rofi_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    lower_local_patch::RORegularSheetPatchCertificate,
    upper_local_patch::RORegularSheetPatchCertificate,
    lower_local_bridge::RORegularSheetBridgeCertificate,
    upper_local_bridge::RORegularSheetBridgeCertificate,
    lower_regular_patch::RORegularSheetPatchCertificate,
    upper_regular_patch::RORegularSheetPatchCertificate,
    chart_state_index::Int,
    limits::ROFoldBranchIncidenceLimits,
    cancel_check,
)
    lower_local_patch.certificate_sha256 !=
        upper_local_patch.certificate_sha256 || throw(
        ROFoldBranchIncidenceRejected(
            :local_half_patches_not_distinct,
            "the two local half patches must be distinct",
        ))
    lower_regular_patch.certificate_sha256 !=
        upper_regular_patch.certificate_sha256 || throw(
        ROFoldBranchIncidenceRejected(
            :regular_patches_not_distinct,
            "the two incident regular patches must be distinct",
        ))
    replayed = _rofi_replay_sources(
        system,
        census,
        event,
        central_local_patch,
        lower_local_patch,
        upper_local_patch,
        lower_local_bridge,
        upper_local_bridge,
        lower_regular_patch,
        upper_regular_patch,
        chart_state_index,
        limits,
        cancel_check,
    )
    context_limits = _rofe_regular_limits_with_operation_cap(
        system.limits, limits.max_analysis_interval_operations)
    context = _RORSContext(context_limits, cancel_check)
    try
        event_root, event_local_remainder =
            _rofi_event_on_local_branch(
                replayed.event,
                replayed.central_local_patch,
                chart_state_index,
                context,
            )
        event_chart_enclosure = event_root[chart_state_index]
        lower_corridor = _rofi_fold_free_corridor(
            :lower_chart_side,
            replayed.census,
            replayed.event,
            event_chart_enclosure,
            replayed.central_local_patch,
            replayed.lower_local_patch.control_box[1],
            chart_state_index,
            context,
        )
        upper_corridor = _rofi_fold_free_corridor(
            :upper_chart_side,
            replayed.census,
            replayed.event,
            event_chart_enclosure,
            replayed.central_local_patch,
            replayed.upper_local_patch.control_box[1],
            chart_state_index,
            context,
        )
        lower_incidence = _rofi_half_incidence(
            :lower_chart_side,
            event_chart_enclosure,
            replayed.lower_local_patch,
            replayed.lower_local_bridge,
            replayed.lower_regular_patch,
            lower_corridor,
            chart_state_index,
            context,
        )
        upper_incidence = _rofi_half_incidence(
            :upper_chart_side,
            event_chart_enclosure,
            replayed.upper_local_patch,
            replayed.upper_local_bridge,
            replayed.upper_regular_patch,
            upper_corridor,
            chart_state_index,
            context,
        )
        lower_incidence.chart_interval.upper <
            upper_incidence.chart_interval.lower || throw(
            ROFoldBranchIncidenceRejected(
                :local_half_intervals_overlap,
                "the two local half-branch intervals are not disjoint",
            ))
        context.cancel_check()
        context.operations <= typemax(Int) || throw(
            ROFoldBranchIncidenceLimitExceeded(
                :analysis_interval_operations,
                context.operations,
                limits.max_analysis_interval_operations,
            ))
        analysis_operations = Int(context.operations)
        certificate_sha256 = _rofi_certificate_sha256(
            system.declaration_sha256,
            limits,
            replayed.census.certificate_sha256,
            replayed.event.certificate_sha256,
            chart_state_index,
            replayed.branch_system.declaration_sha256,
            replayed.central_local_patch.certificate_sha256,
            replayed.lower_local_patch.certificate_sha256,
            replayed.upper_local_patch.certificate_sha256,
            replayed.lower_local_bridge.certificate_sha256,
            replayed.upper_local_bridge.certificate_sha256,
            replayed.lower_regular_patch.certificate_sha256,
            replayed.upper_regular_patch.certificate_sha256,
            event_root,
            event_local_remainder,
            lower_incidence,
            upper_incidence,
            replayed.source_operations,
            analysis_operations,
        )
        return ROSimpleFoldBranchIncidenceCertificate(
            _ROFI_VALIDATED_TOKEN,
            RO_SIMPLE_FOLD_BRANCH_INCIDENCE_VERSION,
            system.declaration_sha256,
            limits,
            replayed.census.certificate_sha256,
            replayed.event.certificate_sha256,
            chart_state_index,
            replayed.branch_system.declaration_sha256,
            replayed.central_local_patch.certificate_sha256,
            replayed.lower_local_patch.certificate_sha256,
            replayed.upper_local_patch.certificate_sha256,
            replayed.lower_local_bridge.certificate_sha256,
            replayed.upper_local_bridge.certificate_sha256,
            replayed.lower_regular_patch.certificate_sha256,
            replayed.upper_regular_patch.certificate_sha256,
            event_root,
            event_local_remainder,
            lower_incidence,
            upper_incidence,
            replayed.source_operations,
            analysis_operations,
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
            RO_SIMPLE_FOLD_BRANCH_INCIDENCE_SCOPE,
            certificate_sha256,
        )
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROFoldBranchIncidenceLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
end

"""
    certify_ro_simple_fold_branch_incidence(...; chart_state_index)

Attach the two sides of one replayed simple fold to two original-coordinate
regular patches through a declarative branch-coordinate system, one central
patch, and two full-half-patch overlap bridges. The result is local incidence
only; it cannot identify remote continuation components.
"""
function certify_ro_simple_fold_branch_incidence(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    lower_local_patch::RORegularSheetPatchCertificate,
    upper_local_patch::RORegularSheetPatchCertificate,
    lower_local_bridge::RORegularSheetBridgeCertificate,
    upper_local_bridge::RORegularSheetBridgeCertificate,
    lower_regular_patch::RORegularSheetPatchCertificate,
    upper_regular_patch::RORegularSheetPatchCertificate;
    chart_state_index::Integer,
    limits::ROFoldBranchIncidenceLimits=ROFoldBranchIncidenceLimits(),
    cancel_check=() -> nothing,
)
    state_count = length(system.state_names)
    1 <= chart_state_index <= state_count ||
        throw(BoundsError(system.state_names, chart_state_index))
    chart_state_index <= typemax(Int) ||
        throw(BoundsError(system.state_names, chart_state_index))
    return _rofi_certify_exact(
        system,
        census,
        event,
        central_local_patch,
        lower_local_patch,
        upper_local_patch,
        lower_local_bridge,
        upper_local_bridge,
        lower_regular_patch,
        upper_regular_patch,
        Int(chart_state_index),
        limits,
        cancel_check,
    )
end

function replay_ro_simple_fold_branch_incidence(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    lower_local_patch::RORegularSheetPatchCertificate,
    upper_local_patch::RORegularSheetPatchCertificate,
    lower_local_bridge::RORegularSheetBridgeCertificate,
    upper_local_bridge::RORegularSheetBridgeCertificate,
    lower_regular_patch::RORegularSheetPatchCertificate,
    upper_regular_patch::RORegularSheetPatchCertificate,
    certificate::ROSimpleFoldBranchIncidenceCertificate;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "fold-branch incidence belongs to another polynomial system"))
    certificate.fold_event_census_sha256 == census.certificate_sha256 ||
        throw(ArgumentError("fold-branch incidence census identity mismatch"))
    certificate.fold_event_certificate_sha256 ==
        event.certificate_sha256 || throw(ArgumentError(
        "fold-branch incidence event identity mismatch"))
    supplied_hashes = (
        central_local_patch.certificate_sha256,
        lower_local_patch.certificate_sha256,
        upper_local_patch.certificate_sha256,
        lower_local_bridge.certificate_sha256,
        upper_local_bridge.certificate_sha256,
        lower_regular_patch.certificate_sha256,
        upper_regular_patch.certificate_sha256,
    )
    certificate_hashes = (
        certificate.central_local_patch_sha256,
        certificate.lower_half_local_patch_sha256,
        certificate.upper_half_local_patch_sha256,
        certificate.lower_local_bridge_sha256,
        certificate.upper_local_bridge_sha256,
        certificate.lower_regular_patch_sha256,
        certificate.upper_regular_patch_sha256,
    )
    supplied_hashes == certificate_hashes || throw(ArgumentError(
        "fold-branch incidence source population mismatch"))
    rebuilt = _rofi_certify_exact(
        system,
        census,
        event,
        central_local_patch,
        lower_local_patch,
        upper_local_patch,
        lower_local_bridge,
        upper_local_bridge,
        lower_regular_patch,
        upper_regular_patch,
        certificate.chart_state_index,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "simple-fold branch-incidence replay changed its hash"))
    return rebuilt
end

function validate_ro_simple_fold_branch_incidence(
    system::ROPolynomialEquilibriumSystem,
    census::ROCompleteSimpleFoldEventCensus,
    event::ROSimpleFoldEvent,
    central_local_patch::RORegularSheetPatchCertificate,
    lower_local_patch::RORegularSheetPatchCertificate,
    upper_local_patch::RORegularSheetPatchCertificate,
    lower_local_bridge::RORegularSheetBridgeCertificate,
    upper_local_bridge::RORegularSheetBridgeCertificate,
    lower_regular_patch::RORegularSheetPatchCertificate,
    upper_regular_patch::RORegularSheetPatchCertificate,
    certificate::ROSimpleFoldBranchIncidenceCertificate;
    cancel_check=() -> nothing,
)
    replay_ro_simple_fold_branch_incidence(
        system,
        census,
        event,
        central_local_patch,
        lower_local_patch,
        upper_local_patch,
        lower_local_bridge,
        upper_local_bridge,
        lower_regular_patch,
        upper_regular_patch,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
