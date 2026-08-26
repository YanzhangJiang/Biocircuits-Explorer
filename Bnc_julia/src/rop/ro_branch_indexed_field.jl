const RO_POLYNOMIAL_DYNAMICS_BINDING_VERSION =
    "bne-ro-polynomial-dynamics-binding/v1.0.0"
const RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION =
    "bne-ro-branch-indexed-regular-field/v1.0.0"
const RO_BRANCH_PATCH_STABILITY_POLICY_VERSION =
    "bne-ro-exact-row-gershgorin-stability/v1.0.0"
const RO_BRANCH_LOG_RESPONSE_POLICY_VERSION =
    "bne-ro-state-log-response-enclosure/v1.0.0"
const RO_BRANCH_INDEXED_REGULAR_FIELD_SCOPE =
    :exact_polynomial_branch_witness_lower_bound_only

const _ROBS_STABLE = :certified_uniformly_locally_asymptotically_stable
const _ROBS_UNSTABLE = :certified_unstable_by_positive_trace
const _ROBS_UNKNOWN = :unknown_stability
const _ROBS_STABILITY_STATUSES =
    (_ROBS_STABLE, _ROBS_UNSTABLE, _ROBS_UNKNOWN)

struct ROBranchIndexedFieldLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROBranchIndexedFieldLimitExceeded)
    print(io, "RO branch-indexed field ", err.phase, " requested ",
        err.requested, ", exceeding limit=", err.limit)
end

struct ROBranchIndexedFieldCertificationRejected <: Exception
    reason::Symbol
    detail::String
end

function Base.showerror(
    io::IO,
    err::ROBranchIndexedFieldCertificationRejected,
)
    print(io, "RO branch-indexed field certification rejected (",
        err.reason, "): ", err.detail)
end

"""Hard population, replay, metadata, and exact-analysis limits for P8s0."""
struct ROBranchIndexedFieldLimits
    max_patches::Int
    max_bridges::Int
    max_branches::Int
    max_witness_boxes::Int
    max_witness_branches::Int
    max_pairwise_separations::Int
    max_metadata_bytes::Int
    max_source_replay_interval_operations::Int
    max_analysis_interval_operations::Int

    function ROBranchIndexedFieldLimits(
        max_patches::Int,
        max_bridges::Int,
        max_branches::Int,
        max_witness_boxes::Int,
        max_witness_branches::Int,
        max_pairwise_separations::Int,
        max_metadata_bytes::Int,
        max_source_replay_interval_operations::Int,
        max_analysis_interval_operations::Int,
    )
        for (label, value) in (
            ("max_patches", max_patches),
            ("max_bridges", max_bridges),
            ("max_branches", max_branches),
            ("max_witness_boxes", max_witness_boxes),
            ("max_witness_branches", max_witness_branches),
            ("max_pairwise_separations", max_pairwise_separations),
            ("max_metadata_bytes", max_metadata_bytes),
            ("max_source_replay_interval_operations",
                max_source_replay_interval_operations),
            ("max_analysis_interval_operations",
                max_analysis_interval_operations),
        )
            value > 0 || throw(ArgumentError("$label must be positive"))
        end
        max_witness_branches <= max_branches || throw(ArgumentError(
            "max_witness_branches cannot exceed max_branches"))
        required_pairwise_separations =
            BigInt(max_witness_branches) *
            BigInt(max_witness_branches - 1) ÷ 2
        BigInt(max_pairwise_separations) >= required_pairwise_separations ||
            throw(ArgumentError(
                "max_pairwise_separations is too small for one maximum-size witness"))
        return new(
            max_patches,
            max_bridges,
            max_branches,
            max_witness_boxes,
            max_witness_branches,
            max_pairwise_separations,
            max_metadata_bytes,
            max_source_replay_interval_operations,
            max_analysis_interval_operations,
        )
    end
end

function ROBranchIndexedFieldLimits(;
    max_patches::Integer=64,
    max_bridges::Integer=128,
    max_branches::Integer=64,
    max_witness_boxes::Integer=64,
    max_witness_branches::Integer=16,
    max_pairwise_separations::Integer=8_192,
    max_metadata_bytes::Integer=32_768,
    max_source_replay_interval_operations::Integer=20_000_000,
    max_analysis_interval_operations::Integer=2_000_000,
)
    values = (
        max_patches,
        max_bridges,
        max_branches,
        max_witness_boxes,
        max_witness_branches,
        max_pairwise_separations,
        max_metadata_bytes,
        max_source_replay_interval_operations,
        max_analysis_interval_operations,
    )
    all(value -> typemin(Int) <= value <= typemax(Int), values) ||
        throw(ArgumentError("branch-indexed field limits must fit Int"))
    return ROBranchIndexedFieldLimits(Int.(values)...)
end

@inline function _robs_limit(
    phase::Symbol,
    requested::Integer,
    limit::Int,
)
    amount = BigInt(requested)
    amount >= 0 || throw(ArgumentError(
        "RO branch-indexed field requested work must be nonnegative"))
    amount <= limit || throw(ROBranchIndexedFieldLimitExceeded(
        phase, amount, limit))
    return nothing
end

@inline function _robs_cancel(cancel_check)
    cancel_check()
    return nothing
end

function _robs_hash(payload)
    return bytes2hex(SHA.sha256(codeunits(JSON3.write(payload))))
end

function _robs_exact_payload(value::_RORSExact)
    return (
        numerator=string(numerator(value)),
        denominator=string(denominator(value)),
    )
end

function _robs_interval_payload(value::ROExactInterval)
    return (
        lower=_robs_exact_payload(value.lower),
        upper=_robs_exact_payload(value.upper),
    )
end

function _robs_interval_vector_payload(values::Tuple)
    return Tuple(_robs_interval_payload(value) for value in values)
end

function _robs_interval_matrix_payload(matrix::ROExactIntervalMatrix)
    return (
        rows=matrix.row_count,
        columns=matrix.column_count,
        column_major=Tuple(
            _robs_interval_payload(value) for value in matrix.data),
    )
end

function _robs_limits_payload(limits::ROBranchIndexedFieldLimits)
    return (
        max_patches=limits.max_patches,
        max_bridges=limits.max_bridges,
        max_branches=limits.max_branches,
        max_witness_boxes=limits.max_witness_boxes,
        max_witness_branches=limits.max_witness_branches,
        max_pairwise_separations=limits.max_pairwise_separations,
        max_metadata_bytes=limits.max_metadata_bytes,
        max_source_replay_interval_operations=
            limits.max_source_replay_interval_operations,
        max_analysis_interval_operations=
            limits.max_analysis_interval_operations,
    )
end

function _robs_string_tuple(
    values,
    label::String,
    expected_count::Int,
    limits::ROBranchIndexedFieldLimits,
)
    values isa AbstractVector || values isa Tuple || throw(ArgumentError(
        "$label must be an ordered vector or tuple"))
    length(values) == expected_count || throw(DimensionMismatch(
        "$label must contain $expected_count entries"))
    byte_count = BigInt(0)
    admitted = String[]
    sizehint!(admitted, expected_count)
    for value in values
        value isa AbstractString || throw(ArgumentError(
            "$label entries must be strings"))
        text = String(value)
        _rors_validate_metadata(text, label)
        byte_count += ncodeunits(text)
        _robs_limit(:metadata_bytes, byte_count, limits.max_metadata_bytes)
        push!(admitted, text)
    end
    return Tuple(admitted)
end

struct _ROBSValidatedToken end
const _ROBS_VALIDATED_TOKEN = _ROBSValidatedToken()

"""
Explicitly binds one P5r0 equilibrium polynomial to vector-field semantics.
Unit labels are retained as identities; the engine does not prove dimensional
algebra or that the declaration reflects the intended physical dynamics.
"""
struct ROPolynomialDynamicsBinding
    version::String
    system_declaration_sha256::String
    time_unit::String
    state_rate_units::Tuple
    dynamics_policy_sha256::String
    equations_are_state_time_derivatives::Bool
    unit_algebra_verified::Bool
    evidence_scope::Symbol
    declaration_sha256::String

    function ROPolynomialDynamicsBinding(
        ::_ROBSValidatedToken,
        version::String,
        system_declaration_sha256::String,
        time_unit::String,
        state_rate_units::Tuple,
        dynamics_policy_sha256::String,
        equations_are_state_time_derivatives::Bool,
        unit_algebra_verified::Bool,
        evidence_scope::Symbol,
        declaration_sha256::String,
    )
        version == RO_POLYNOMIAL_DYNAMICS_BINDING_VERSION ||
            throw(ArgumentError("polynomial dynamics-binding version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        _rors_validate_metadata(time_unit, "time_unit")
        all(unit -> unit isa String && !isempty(unit), state_rate_units) ||
            throw(ArgumentError("state_rate_units must be nonempty strings"))
        _rors_validate_sha256(
            dynamics_policy_sha256, "dynamics_policy_sha256")
        equations_are_state_time_derivatives || throw(ArgumentError(
            "the dynamics binding must explicitly declare F as dx/dt"))
        unit_algebra_verified && throw(ArgumentError(
            "P8s0 retains unit labels but cannot verify dimensional algebra"))
        evidence_scope == :declared_polynomial_vector_field_semantics_only ||
            throw(ArgumentError("polynomial dynamics-binding scope mismatch"))
        payload = _robs_dynamics_binding_payload(
            system_declaration_sha256,
            time_unit,
            state_rate_units,
            dynamics_policy_sha256,
        )
        declaration_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "polynomial dynamics-binding declaration hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            time_unit,
            state_rate_units,
            dynamics_policy_sha256,
            true,
            false,
            evidence_scope,
            declaration_sha256,
        )
    end
end

function _robs_dynamics_binding_payload(
    system_declaration_sha256::String,
    time_unit::String,
    state_rate_units::Tuple,
    dynamics_policy_sha256::String,
)
    return (
        version=RO_POLYNOMIAL_DYNAMICS_BINDING_VERSION,
        system_declaration_sha256=system_declaration_sha256,
        time_unit=time_unit,
        state_rate_units=state_rate_units,
        dynamics_policy_sha256=dynamics_policy_sha256,
        equations_are_state_time_derivatives=true,
        unit_algebra_verified=false,
        evidence_scope="declared_polynomial_vector_field_semantics_only",
    )
end

function ROPolynomialDynamicsBinding(
    system::ROPolynomialEquilibriumSystem;
    time_unit,
    state_rate_units,
    dynamics_policy_sha256,
    limits::ROBranchIndexedFieldLimits=ROBranchIndexedFieldLimits(),
)
    validate_ro_polynomial_equilibrium_system(system)
    time_unit isa AbstractString || throw(ArgumentError(
        "time_unit must be a string"))
    admitted_time_unit = String(time_unit)
    _rors_validate_metadata(admitted_time_unit, "time_unit")
    admitted_rate_units = _robs_string_tuple(
        state_rate_units,
        "state_rate_units",
        length(system.state_names),
        limits,
    )
    policy = String(dynamics_policy_sha256)
    _rors_validate_sha256(policy, "dynamics_policy_sha256")
    metadata_bytes = BigInt(ncodeunits(admitted_time_unit)) + sum(
        (BigInt(ncodeunits(unit)) for unit in admitted_rate_units);
        init=BigInt(0),
    )
    _robs_limit(:metadata_bytes, metadata_bytes, limits.max_metadata_bytes)
    payload = _robs_dynamics_binding_payload(
        system.declaration_sha256,
        admitted_time_unit,
        admitted_rate_units,
        policy,
    )
    return ROPolynomialDynamicsBinding(
        _ROBS_VALIDATED_TOKEN,
        RO_POLYNOMIAL_DYNAMICS_BINDING_VERSION,
        system.declaration_sha256,
        admitted_time_unit,
        admitted_rate_units,
        policy,
        true,
        false,
        :declared_polynomial_vector_field_semantics_only,
        _robs_hash(payload),
    )
end

function validate_ro_polynomial_dynamics_binding(
    system::ROPolynomialEquilibriumSystem,
    binding::ROPolynomialDynamicsBinding;
    limits::ROBranchIndexedFieldLimits=ROBranchIndexedFieldLimits(),
)
    binding.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "polynomial dynamics binding belongs to a different system"))
    rebuilt = ROPolynomialDynamicsBinding(
        system;
        time_unit=binding.time_unit,
        state_rate_units=binding.state_rate_units,
        dynamics_policy_sha256=binding.dynamics_policy_sha256,
        limits=limits,
    )
    rebuilt == binding || throw(ArgumentError(
        "polynomial dynamics binding does not reproduce"))
    return true
end

struct ROBranchPatchEvidence
    patch_certificate_sha256::String
    source_branch_identity_sha256::String
    gershgorin_right_bounds::Tuple
    trace_lower_bound::_RORSExact
    stability_status::Symbol
    stability_margin::Union{Nothing,_RORSExact}
    state_log_response_enclosure::ROExactIntervalMatrix
    fold_excluded_inside_declared_tube::Bool
    hopf_excluded_inside_declared_tube::Bool
    evidence_scope::Symbol
    evidence_sha256::String

    function ROBranchPatchEvidence(
        ::_ROBSValidatedToken,
        patch_certificate_sha256::String,
        source_branch_identity_sha256::String,
        gershgorin_right_bounds::Tuple,
        trace_lower_bound::_RORSExact,
        stability_status::Symbol,
        stability_margin::Union{Nothing,_RORSExact},
        state_log_response_enclosure::ROExactIntervalMatrix,
        fold_excluded_inside_declared_tube::Bool,
        hopf_excluded_inside_declared_tube::Bool,
        evidence_scope::Symbol,
        evidence_sha256::String,
    )
        _rors_validate_sha256(
            patch_certificate_sha256, "patch_certificate_sha256")
        _rors_validate_sha256(
            source_branch_identity_sha256,
            "source_branch_identity_sha256",
        )
        !isempty(gershgorin_right_bounds) &&
            all(value -> value isa _RORSExact, gershgorin_right_bounds) ||
            throw(ArgumentError(
                "Gershgorin right bounds must be exact rational values"))
        state_log_response_enclosure.row_count ==
            length(gershgorin_right_bounds) || throw(DimensionMismatch(
            "log-response rows must match the stability state order"))
        stability_status in _ROBS_STABILITY_STATUSES || throw(ArgumentError(
            "unsupported branch-patch stability status"))
        fold_excluded_inside_declared_tube || throw(ArgumentError(
            "every P5r0 patch must retain its fold exclusion inside the tube"))
        if stability_status == _ROBS_STABLE
            all(<(zero(_RORSExact)), gershgorin_right_bounds) ||
                throw(ArgumentError(
                    "stable patch evidence requires strict negative Gershgorin bounds"))
            stability_margin isa _RORSExact && stability_margin > 0 ||
                throw(ArgumentError(
                    "stable patch evidence requires a positive exact margin"))
            hopf_excluded_inside_declared_tube || throw(ArgumentError(
                "uniformly stable patch evidence must retain Hopf exclusion"))
        elseif stability_status == _ROBS_UNSTABLE
            trace_lower_bound > 0 || throw(ArgumentError(
                "positive-trace instability requires a strict positive trace bound"))
            stability_margin isa _RORSExact && stability_margin > 0 ||
                throw(ArgumentError(
                    "unstable patch evidence requires a positive exact margin"))
            hopf_excluded_inside_declared_tube && throw(ArgumentError(
                "positive trace alone does not exclude a Hopf boundary"))
        else
            stability_margin === nothing || throw(ArgumentError(
                "unknown stability must not publish a margin"))
            hopf_excluded_inside_declared_tube && throw(ArgumentError(
                "unknown stability cannot exclude a Hopf boundary"))
        end
        evidence_scope ==
            :exact_tube_stability_and_state_log_response_enclosure ||
            throw(ArgumentError("branch-patch evidence scope mismatch"))
        payload = _robs_patch_evidence_payload(
            patch_certificate_sha256,
            source_branch_identity_sha256,
            gershgorin_right_bounds,
            trace_lower_bound,
            stability_status,
            stability_margin,
            state_log_response_enclosure,
            fold_excluded_inside_declared_tube,
            hopf_excluded_inside_declared_tube,
        )
        evidence_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "branch-patch evidence hash mismatch"))
        return new(
            patch_certificate_sha256,
            source_branch_identity_sha256,
            gershgorin_right_bounds,
            trace_lower_bound,
            stability_status,
            stability_margin,
            state_log_response_enclosure,
            true,
            hopf_excluded_inside_declared_tube,
            evidence_scope,
            evidence_sha256,
        )
    end
end

function _robs_patch_evidence_payload(
    patch_certificate_sha256::String,
    source_branch_identity_sha256::String,
    gershgorin_right_bounds::Tuple,
    trace_lower_bound::_RORSExact,
    stability_status::Symbol,
    stability_margin,
    state_log_response_enclosure::ROExactIntervalMatrix,
    fold_excluded::Bool,
    hopf_excluded::Bool,
)
    return (
        patch_certificate_sha256=patch_certificate_sha256,
        source_branch_identity_sha256=source_branch_identity_sha256,
        stability_policy_version=RO_BRANCH_PATCH_STABILITY_POLICY_VERSION,
        gershgorin_right_bounds=Tuple(
            _robs_exact_payload(value) for value in
                gershgorin_right_bounds),
        trace_lower_bound=_robs_exact_payload(trace_lower_bound),
        stability_status=String(stability_status),
        stability_margin=stability_margin === nothing ? nothing :
            _robs_exact_payload(stability_margin),
        log_response_policy_version=RO_BRANCH_LOG_RESPONSE_POLICY_VERSION,
        state_log_response_enclosure=
            _robs_interval_matrix_payload(state_log_response_enclosure),
        fold_excluded_inside_declared_tube=fold_excluded,
        hopf_excluded_inside_declared_tube=hopf_excluded,
        evidence_scope=
            "exact_tube_stability_and_state_log_response_enclosure",
    )
end

struct ROBranchComponentEvidence
    branch_index::Int
    branch_identity_sha256::String
    patch_certificate_sha256s::Tuple
    bridge_certificate_sha256s::Tuple
    stable_patch_count::Int
    unstable_patch_count::Int
    unknown_patch_count::Int
    stability_coverage_status::Symbol
    evidence_sha256::String

    function ROBranchComponentEvidence(
        ::_ROBSValidatedToken,
        branch_index::Int,
        branch_identity_sha256::String,
        patch_certificate_sha256s::Tuple,
        bridge_certificate_sha256s::Tuple,
        stable_patch_count::Int,
        unstable_patch_count::Int,
        unknown_patch_count::Int,
        stability_coverage_status::Symbol,
        evidence_sha256::String,
    )
        branch_index > 0 || throw(ArgumentError(
            "branch_index must be positive"))
        _rors_validate_sha256(
            branch_identity_sha256, "branch_identity_sha256")
        !isempty(patch_certificate_sha256s) || throw(ArgumentError(
            "a branch component must contain at least one patch"))
        issorted(patch_certificate_sha256s) &&
            allunique(patch_certificate_sha256s) || throw(ArgumentError(
            "branch patch identities must be sorted and unique"))
        for hash in patch_certificate_sha256s
            hash isa String || throw(ArgumentError(
                "branch patch identities must be strings"))
            _rors_validate_sha256(hash, "branch patch certificate SHA-256")
        end
        issorted(bridge_certificate_sha256s) &&
            allunique(bridge_certificate_sha256s) || throw(ArgumentError(
            "branch bridge identities must be sorted and unique"))
        for hash in bridge_certificate_sha256s
            hash isa String || throw(ArgumentError(
                "branch bridge identities must be strings"))
            _rors_validate_sha256(hash, "branch bridge certificate SHA-256")
        end
        stable_patch_count >= 0 && unstable_patch_count >= 0 &&
            unknown_patch_count >= 0 || throw(ArgumentError(
            "branch stability counts must be nonnegative"))
        counts = BigInt(stable_patch_count) + BigInt(unstable_patch_count) +
            BigInt(unknown_patch_count)
        counts == BigInt(length(patch_certificate_sha256s)) || throw(ArgumentError(
            "branch stability counts must cover every patch"))
        expected_status = BigInt(stable_patch_count) == counts ?
            :all_supplied_patches_certified_stable :
            stable_patch_count > 0 ? :contains_certified_stable_patches :
            :no_certified_stable_patch
        stability_coverage_status == expected_status || throw(ArgumentError(
            "branch stability coverage status does not match its counts"))
        payload = _robs_branch_payload(
            branch_index,
            branch_identity_sha256,
            patch_certificate_sha256s,
            bridge_certificate_sha256s,
            stable_patch_count,
            unstable_patch_count,
            unknown_patch_count,
            stability_coverage_status,
        )
        evidence_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "branch component evidence hash mismatch"))
        return new(
            branch_index,
            branch_identity_sha256,
            patch_certificate_sha256s,
            bridge_certificate_sha256s,
            stable_patch_count,
            unstable_patch_count,
            unknown_patch_count,
            stability_coverage_status,
            evidence_sha256,
        )
    end
end

function _robs_branch_payload(
    branch_index::Int,
    branch_identity_sha256::String,
    patch_certificate_sha256s::Tuple,
    bridge_certificate_sha256s::Tuple,
    stable_patch_count::Int,
    unstable_patch_count::Int,
    unknown_patch_count::Int,
    stability_coverage_status::Symbol,
)
    return (
        branch_index=branch_index,
        branch_identity_sha256=branch_identity_sha256,
        patch_certificate_sha256s=patch_certificate_sha256s,
        bridge_certificate_sha256s=bridge_certificate_sha256s,
        stable_patch_count=stable_patch_count,
        unstable_patch_count=unstable_patch_count,
        unknown_patch_count=unknown_patch_count,
        stability_coverage_status=String(stability_coverage_status),
    )
end

struct ROBranchSeparationEvidence
    left_branch_identity_sha256::String
    right_branch_identity_sha256::String
    separating_state_index::Int
    separating_state_name::String
    strict_separation_margin::_RORSExact
    evidence_sha256::String

    function ROBranchSeparationEvidence(
        ::_ROBSValidatedToken,
        left_branch_identity_sha256::String,
        right_branch_identity_sha256::String,
        separating_state_index::Int,
        separating_state_name::String,
        strict_separation_margin::_RORSExact,
        evidence_sha256::String,
    )
        _rors_validate_sha256(
            left_branch_identity_sha256,
            "left_branch_identity_sha256",
        )
        _rors_validate_sha256(
            right_branch_identity_sha256,
            "right_branch_identity_sha256",
        )
        left_branch_identity_sha256 < right_branch_identity_sha256 ||
            throw(ArgumentError(
                "branch separation endpoints must be canonically ordered"))
        separating_state_index > 0 || throw(ArgumentError(
            "separating_state_index must be positive"))
        _rors_validate_metadata(
            separating_state_name, "separating_state_name")
        strict_separation_margin > 0 || throw(ArgumentError(
            "branch separation margin must be strictly positive"))
        payload = _robs_separation_payload(
            left_branch_identity_sha256,
            right_branch_identity_sha256,
            separating_state_index,
            separating_state_name,
            strict_separation_margin,
        )
        evidence_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "branch separation evidence hash mismatch"))
        return new(
            left_branch_identity_sha256,
            right_branch_identity_sha256,
            separating_state_index,
            separating_state_name,
            strict_separation_margin,
            evidence_sha256,
        )
    end
end


function _robs_separation_payload(
    left_branch_identity_sha256::String,
    right_branch_identity_sha256::String,
    separating_state_index::Int,
    separating_state_name::String,
    strict_separation_margin::_RORSExact,
)
    return (
        left_branch_identity_sha256=left_branch_identity_sha256,
        right_branch_identity_sha256=right_branch_identity_sha256,
        separating_state_index=separating_state_index,
        separating_state_name=separating_state_name,
        strict_separation_margin=
            _robs_exact_payload(strict_separation_margin),
    )
end

struct ROMultistabilityWitnessEvidence
    control_box::Tuple
    selected_patch_certificate_sha256s::Tuple
    selected_branch_identity_sha256s::Tuple
    selected_state_tube_enclosures::Tuple
    pairwise_separations::Tuple
    certified_stable_root_lower_bound::Int
    stable_root_population_complete::Bool
    folds_outside_selected_tubes_excluded::Bool
    bifurcation_boundaries_enclosed::Bool
    evidence_scope::Symbol
    evidence_sha256::String

    function ROMultistabilityWitnessEvidence(
        ::_ROBSValidatedToken,
        control_box::Tuple,
        selected_patch_certificate_sha256s::Tuple,
        selected_branch_identity_sha256s::Tuple,
        selected_state_tube_enclosures::Tuple,
        pairwise_separations::Tuple,
        certified_stable_root_lower_bound::Int,
        stable_root_population_complete::Bool,
        folds_outside_selected_tubes_excluded::Bool,
        bifurcation_boundaries_enclosed::Bool,
        evidence_scope::Symbol,
        evidence_sha256::String,
    )
        !isempty(control_box) &&
            all(interval -> interval isa ROExactInterval &&
                0 < interval.lower < interval.upper, control_box) ||
            throw(ArgumentError(
                "multistability witness controls must be positive exact intervals with positive width"))
        count = length(selected_patch_certificate_sha256s)
        count >= 2 || throw(ArgumentError(
            "a multistability witness requires at least two stable patches"))
        length(selected_branch_identity_sha256s) == count &&
            length(selected_state_tube_enclosures) == count ||
            throw(DimensionMismatch(
                "witness patch, branch, and state-tube populations must match"))
        allunique(selected_patch_certificate_sha256s) &&
            allunique(selected_branch_identity_sha256s) ||
            throw(ArgumentError(
                "witness patches and branch components must be unique"))
        for hash in selected_patch_certificate_sha256s
            hash isa String || throw(ArgumentError(
                "witness patch identities must be strings"))
            _rors_validate_sha256(hash, "witness patch certificate SHA-256")
        end
        for hash in selected_branch_identity_sha256s
            hash isa String || throw(ArgumentError(
                "witness branch identities must be strings"))
            _rors_validate_sha256(hash, "witness branch identity SHA-256")
        end
        issorted(selected_branch_identity_sha256s) || throw(ArgumentError(
            "witness branch identities must be canonically sorted"))
        state_count = length(first(selected_state_tube_enclosures))
        state_count > 0 || throw(ArgumentError(
            "witness state tubes must not be empty"))
        for tube in selected_state_tube_enclosures
            tube isa Tuple && length(tube) == state_count &&
                all(interval -> interval isa ROExactInterval &&
                    interval.lower > 0, tube) || throw(ArgumentError(
                "witness state tubes must be positive exact interval tuples with one common state order"))
        end
        expected_pairs = BigInt(count) * BigInt(count - 1) ÷ 2
        BigInt(length(pairwise_separations)) == expected_pairs || throw(ArgumentError(
            "witness must retain every pairwise branch separation"))
        all(separation -> separation isa ROBranchSeparationEvidence,
            pairwise_separations) || throw(ArgumentError(
            "witness pairwise separations have the wrong type"))
        expected_endpoints = Set(
            (selected_branch_identity_sha256s[left],
                selected_branch_identity_sha256s[right])
            for left in 1:(count - 1)
            for right in (left + 1):count
        )
        observed_endpoints = Set(
            (separation.left_branch_identity_sha256,
                separation.right_branch_identity_sha256)
            for separation in pairwise_separations
        )
        observed_endpoints == expected_endpoints || throw(ArgumentError(
            "witness separations must cover each selected branch pair exactly once"))
        all(separation -> separation.separating_state_index <= state_count,
            pairwise_separations) || throw(ArgumentError(
            "witness separation state index exceeds the retained state order"))
        certified_stable_root_lower_bound == count || throw(ArgumentError(
            "stable-root lower bound must equal the selected branch count"))
        stable_root_population_complete && throw(ArgumentError(
            "P8s0 cannot certify a complete stable-root population"))
        folds_outside_selected_tubes_excluded && throw(ArgumentError(
            "P8s0 cannot exclude folds outside selected root tubes"))
        bifurcation_boundaries_enclosed && throw(ArgumentError(
            "P8s0 does not enclose bifurcation boundaries"))
        evidence_scope ==
            :stable_root_lower_bound_on_declared_control_box ||
            throw(ArgumentError("multistability witness scope mismatch"))
        payload = _robs_witness_payload(
            control_box,
            selected_patch_certificate_sha256s,
            selected_branch_identity_sha256s,
            selected_state_tube_enclosures,
            pairwise_separations,
            certified_stable_root_lower_bound,
        )
        evidence_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "multistability witness evidence hash mismatch"))
        return new(
            control_box,
            selected_patch_certificate_sha256s,
            selected_branch_identity_sha256s,
            selected_state_tube_enclosures,
            pairwise_separations,
            count,
            false,
            false,
            false,
            evidence_scope,
            evidence_sha256,
        )
    end
end

function _robs_witness_payload(
    control_box::Tuple,
    patch_hashes::Tuple,
    branch_hashes::Tuple,
    state_tubes::Tuple,
    separations::Tuple,
    lower_bound::Int,
)
    return (
        control_box=_robs_interval_vector_payload(control_box),
        selected_patch_certificate_sha256s=patch_hashes,
        selected_branch_identity_sha256s=branch_hashes,
        selected_state_tube_enclosures=Tuple(
            _robs_interval_vector_payload(tube) for tube in state_tubes),
        pairwise_separations=Tuple(separation.evidence_sha256
            for separation in separations),
        certified_stable_root_lower_bound=lower_bound,
        stable_root_population_complete=false,
        folds_outside_selected_tubes_excluded=false,
        bifurcation_boundaries_enclosed=false,
        evidence_scope="stable_root_lower_bound_on_declared_control_box",
    )
end

struct ROBranchIndexedRegularField
    version::String
    system_declaration_sha256::String
    dynamics_binding::ROPolynomialDynamicsBinding
    limits::ROBranchIndexedFieldLimits
    patch_certificate_sha256s::Tuple
    bridge_certificate_sha256s::Tuple
    patch_evidence::Tuple
    branches::Tuple
    multistability_witnesses::Tuple
    source_replay_interval_operation_count::Int
    analysis_interval_operation_count::Int
    maximum_witnessed_stable_root_lower_bound::Int
    all_supplied_patches_replayed::Bool
    all_supplied_bridges_replayed::Bool
    stable_root_population_status::Symbol
    stable_root_population_complete::Bool
    fold_boundaries_enclosed::Bool
    hopf_boundaries_enclosed::Bool
    global_continuation_certified::Bool
    true_hysteresis_certified::Bool
    evidence_scope::Symbol
    certificate_sha256::String

    function ROBranchIndexedRegularField(
        ::_ROBSValidatedToken,
        version::String,
        system_declaration_sha256::String,
        dynamics_binding::ROPolynomialDynamicsBinding,
        limits::ROBranchIndexedFieldLimits,
        patch_certificate_sha256s::Tuple,
        bridge_certificate_sha256s::Tuple,
        patch_evidence::Tuple,
        branches::Tuple,
        multistability_witnesses::Tuple,
        source_replay_interval_operation_count::Int,
        analysis_interval_operation_count::Int,
        maximum_witnessed_stable_root_lower_bound::Int,
        all_supplied_patches_replayed::Bool,
        all_supplied_bridges_replayed::Bool,
        stable_root_population_status::Symbol,
        stable_root_population_complete::Bool,
        fold_boundaries_enclosed::Bool,
        hopf_boundaries_enclosed::Bool,
        global_continuation_certified::Bool,
        true_hysteresis_certified::Bool,
        evidence_scope::Symbol,
        certificate_sha256::String,
    )
        version == RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION ||
            throw(ArgumentError(
                "branch-indexed regular-field version mismatch"))
        _rors_validate_sha256(
            system_declaration_sha256, "system_declaration_sha256")
        dynamics_binding.system_declaration_sha256 ==
            system_declaration_sha256 || throw(ArgumentError(
            "branch field dynamics binding belongs to another system"))
        !isempty(patch_certificate_sha256s) || throw(ArgumentError(
            "a branch-indexed regular field requires at least one patch"))
        _robs_limit(:patches, length(patch_certificate_sha256s),
            limits.max_patches)
        _robs_limit(:bridges, length(bridge_certificate_sha256s),
            limits.max_bridges)
        _robs_limit(:branches, length(branches), limits.max_branches)
        _robs_limit(:witness_boxes, length(multistability_witnesses),
            limits.max_witness_boxes)
        issorted(patch_certificate_sha256s) &&
            allunique(patch_certificate_sha256s) || throw(ArgumentError(
            "field patch identities must be sorted and unique"))
        for hash in patch_certificate_sha256s
            hash isa String || throw(ArgumentError(
                "field patch identities must be strings"))
            _rors_validate_sha256(hash, "field patch certificate SHA-256")
        end
        issorted(bridge_certificate_sha256s) &&
            allunique(bridge_certificate_sha256s) || throw(ArgumentError(
            "field bridge identities must be sorted and unique"))
        for hash in bridge_certificate_sha256s
            hash isa String || throw(ArgumentError(
                "field bridge identities must be strings"))
            _rors_validate_sha256(hash, "field bridge certificate SHA-256")
        end
        length(patch_evidence) == length(patch_certificate_sha256s) ||
            throw(DimensionMismatch(
                "field must retain one evidence record per patch"))
        all(evidence -> evidence isa ROBranchPatchEvidence,
            patch_evidence) || throw(ArgumentError(
            "field patch evidence has the wrong type"))
        Tuple(evidence.patch_certificate_sha256
            for evidence in patch_evidence) ==
            patch_certificate_sha256s || throw(ArgumentError(
            "patch evidence order does not match field patch order"))
        !isempty(branches) || throw(ArgumentError(
            "a branch-indexed field must contain at least one branch component"))
        all(branch -> branch isa ROBranchComponentEvidence, branches) ||
            throw(ArgumentError("field branch evidence has the wrong type"))
        Tuple(branch.branch_index for branch in branches) ==
            Tuple(1:length(branches)) || throw(ArgumentError(
                "branch indices must be contiguous and ordered"))
        branch_identities = Tuple(branch.branch_identity_sha256
            for branch in branches)
        issorted(branch_identities) && allunique(branch_identities) ||
            throw(ArgumentError(
                "branch identities must be canonically ordered and unique"))
        component_patch_population = sort!(String[
            hash for branch in branches
            for hash in branch.patch_certificate_sha256s
        ])
        Tuple(component_patch_population) == patch_certificate_sha256s ||
            throw(ArgumentError(
                "branch components must partition the complete patch population"))
        component_bridge_population = sort!(String[
            hash for branch in branches
            for hash in branch.bridge_certificate_sha256s
        ])
        Tuple(component_bridge_population) == bridge_certificate_sha256s ||
            throw(ArgumentError(
                "branch components must partition the complete bridge population"))
        all(witness -> witness isa ROMultistabilityWitnessEvidence,
            multistability_witnesses) || throw(ArgumentError(
            "field multistability witnesses have the wrong type"))
        issorted(Tuple(witness.evidence_sha256
            for witness in multistability_witnesses)) &&
            allunique(witness.evidence_sha256
                for witness in multistability_witnesses) ||
            throw(ArgumentError(
                "field multistability witnesses must be sorted and unique"))
        branch_by_patch = Dict(
            hash => branch.branch_identity_sha256
            for branch in branches
            for hash in branch.patch_certificate_sha256s
        )
        all(witness -> all(index -> get(
            branch_by_patch,
            witness.selected_patch_certificate_sha256s[index],
            nothing,
        ) == witness.selected_branch_identity_sha256s[index],
            eachindex(witness.selected_patch_certificate_sha256s)),
            multistability_witnesses) || throw(ArgumentError(
            "a field witness patch is not bound to its named branch component"))
        source_replay_interval_operation_count >= 0 &&
            analysis_interval_operation_count >= 0 || throw(ArgumentError(
            "branch-field operation counts must be nonnegative"))
        _robs_limit(
            :source_replay_interval_operations,
            source_replay_interval_operation_count,
            limits.max_source_replay_interval_operations,
        )
        _robs_limit(
            :analysis_interval_operations,
            analysis_interval_operation_count,
            limits.max_analysis_interval_operations,
        )
        expected_maximum = isempty(multistability_witnesses) ? 0 :
            maximum(witness.certified_stable_root_lower_bound
                for witness in multistability_witnesses)
        maximum_witnessed_stable_root_lower_bound == expected_maximum ||
            throw(ArgumentError(
                "maximum witnessed stable-root lower bound is inconsistent"))
        all_supplied_patches_replayed && all_supplied_bridges_replayed ||
            throw(ArgumentError(
                "branch field must retain complete replay of supplied sources"))
        stable_root_population_status == :witness_lower_bound_only ||
            throw(ArgumentError(
                "P8s0 stable-root population status must remain witness-only"))
        stable_root_population_complete && throw(ArgumentError(
            "P8s0 cannot certify stable-root population completeness"))
        fold_boundaries_enclosed && throw(ArgumentError(
            "P8s0 does not enclose fold boundaries"))
        hopf_boundaries_enclosed && throw(ArgumentError(
            "P8s0 does not enclose Hopf boundaries"))
        global_continuation_certified && throw(ArgumentError(
            "P8s0 cannot certify global continuation"))
        true_hysteresis_certified && throw(ArgumentError(
            "static branch evidence cannot certify true hysteresis"))
        evidence_scope == RO_BRANCH_INDEXED_REGULAR_FIELD_SCOPE ||
            throw(ArgumentError(
                "branch-indexed regular-field evidence scope mismatch"))
        payload = _robs_field_payload(
            system_declaration_sha256,
            dynamics_binding,
            limits,
            patch_certificate_sha256s,
            bridge_certificate_sha256s,
            patch_evidence,
            branches,
            multistability_witnesses,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
            maximum_witnessed_stable_root_lower_bound,
        )
        certificate_sha256 == _robs_hash(payload) || throw(ArgumentError(
            "branch-indexed regular-field certificate hash mismatch"))
        return new(
            version,
            system_declaration_sha256,
            dynamics_binding,
            limits,
            patch_certificate_sha256s,
            bridge_certificate_sha256s,
            patch_evidence,
            branches,
            multistability_witnesses,
            source_replay_interval_operation_count,
            analysis_interval_operation_count,
            maximum_witnessed_stable_root_lower_bound,
            true,
            true,
            :witness_lower_bound_only,
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

function _robs_field_payload(
    system_declaration_sha256::String,
    dynamics_binding::ROPolynomialDynamicsBinding,
    limits::ROBranchIndexedFieldLimits,
    patch_hashes::Tuple,
    bridge_hashes::Tuple,
    patch_evidence::Tuple,
    branches::Tuple,
    witnesses::Tuple,
    source_operations::Int,
    analysis_operations::Int,
    maximum_lower_bound::Int,
)
    return (
        version=RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION,
        system_declaration_sha256=system_declaration_sha256,
        dynamics_binding_sha256=dynamics_binding.declaration_sha256,
        limits=_robs_limits_payload(limits),
        patch_certificate_sha256s=patch_hashes,
        bridge_certificate_sha256s=bridge_hashes,
        patch_evidence_sha256s=Tuple(
            evidence.evidence_sha256 for evidence in patch_evidence),
        branch_evidence_sha256s=Tuple(
            branch.evidence_sha256 for branch in branches),
        multistability_witness_sha256s=Tuple(
            witness.evidence_sha256 for witness in witnesses),
        source_replay_interval_operation_count=source_operations,
        analysis_interval_operation_count=analysis_operations,
        maximum_witnessed_stable_root_lower_bound=maximum_lower_bound,
        all_supplied_patches_replayed=true,
        all_supplied_bridges_replayed=true,
        stable_root_population_status="witness_lower_bound_only",
        stable_root_population_complete=false,
        fold_boundaries_enclosed=false,
        hopf_boundaries_enclosed=false,
        global_continuation_certified=false,
        true_hysteresis_certified=false,
        evidence_scope=String(RO_BRANCH_INDEXED_REGULAR_FIELD_SCOPE),
    )
end

function _robs_regular_limits_with_operation_cap(
    limits::RORegularSheetLimits,
    operation_cap::Int,
)
    return RORegularSheetLimits(
        limits.max_states,
        limits.max_controls,
        limits.max_terms_per_equation,
        limits.max_total_terms,
        limits.max_expanded_terms,
        limits.max_total_degree,
        limits.max_metadata_bytes,
        limits.max_exact_operand_bits,
        operation_cap,
    )
end

function _robs_replay_sources(
    system::ROPolynomialEquilibriumSystem,
    patches::Vector{RORegularSheetPatchCertificate},
    bridges::Vector{RORegularSheetBridgeCertificate},
    limits::ROBranchIndexedFieldLimits,
    cancel_check,
)
    expected_operations = sum(
        (BigInt(patch.exact_operation_count) for patch in patches);
        init=BigInt(0),
    ) + sum(
        (BigInt(bridge.exact_operation_count) for bridge in bridges);
        init=BigInt(0),
    )
    _robs_limit(
        :source_replay_interval_operations,
        expected_operations,
        limits.max_source_replay_interval_operations,
    )
    context_limits = _robs_regular_limits_with_operation_cap(
        system.limits,
        limits.max_source_replay_interval_operations,
    )
    context = _RORSContext(context_limits, cancel_check)
    rebuilt_patch_by_hash = Dict{String,RORegularSheetPatchCertificate}()
    for patch in patches
        _robs_cancel(cancel_check)
        patch.limits == system.limits || throw(ArgumentError(
            "regular-sheet patch limits do not match the source system"))
        # The exact replay algorithm needs the source limits for certificate
        # identity.  Its arithmetic context may use the stricter cumulative
        # P8s0 operation cap without changing certificate meaning.
        rebuilt = _rors_certify_patch_exact(
            system,
            collect(patch.control_box),
            collect(patch.control_reference),
            collect(patch.state_reference),
            _rors_exact_matrix_values(patch.predictor_slope),
            collect(patch.remainder_box),
            _rors_exact_matrix_values(patch.preconditioner),
            context,
        )
        rebuilt.certificate_sha256 == patch.certificate_sha256 ||
            throw(ArgumentError(
                "regular-sheet patch replay changed its certificate hash"))
        rebuilt_patch_by_hash[patch.certificate_sha256] = rebuilt
    end
    rebuilt_bridges = RORegularSheetBridgeCertificate[]
    for bridge in bridges
        _robs_cancel(cancel_check)
        parent = get(
            rebuilt_patch_by_hash, bridge.parent_patch_sha256, nothing)
        child = get(
            rebuilt_patch_by_hash, bridge.child_patch_sha256, nothing)
        parent === nothing && throw(ArgumentError(
            "bridge parent patch is not in the supplied patch population"))
        child === nothing && throw(ArgumentError(
            "bridge child patch is not in the supplied patch population"))
        bridge.parent_patch_sha256 != bridge.child_patch_sha256 ||
            throw(ArgumentError("self bridges are not branch evidence"))
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
        push!(rebuilt_bridges, rebuilt)
    end
    context.operations == expected_operations || throw(ArgumentError(
        "source replay operation count changed from its certificates"))
    context.operations <= typemax(Int) || throw(
        ROBranchIndexedFieldLimitExceeded(
            :source_replay_interval_operations,
            context.operations,
            limits.max_source_replay_interval_operations,
        ))
    return (
        [rebuilt_patch_by_hash[patch.certificate_sha256]
            for patch in patches],
        rebuilt_bridges,
        Int(context.operations),
    )
end

function _robs_abs_upper(value::ROExactInterval)
    return max(abs(value.lower), abs(value.upper))
end

function _robs_reciprocal_positive(
    context::_RORSContext,
    value::ROExactInterval,
)
    value.lower > 0 || throw(ArgumentError(
        "state/control interval must remain strictly positive"))
    return _rors_interval(
        context,
        _rors_exact_divide(context, one(_RORSExact), value.upper),
        _rors_exact_divide(context, one(_RORSExact), value.lower),
    )
end

function _robs_patch_log_response(
    patch::RORegularSheetPatchCertificate,
    context::_RORSContext,
)
    state_count = length(patch.tube_state_enclosure)
    control_count = length(patch.control_box)
    result = Matrix{ROExactInterval}(undef, state_count, control_count)
    for state in 1:state_count
        inverse_state = _robs_reciprocal_positive(
            context, patch.tube_state_enclosure[state])
        for control in 1:control_count
            scale = _rors_multiply(
                context, patch.control_box[control], inverse_state)
            result[state, control] = _rors_multiply(
                context,
                scale,
                patch.implicit_derivative_enclosure[state, control],
            )
        end
    end
    return _rors_interval_matrix_wrapper(result)
end

function _robs_patch_evidence(
    patch::RORegularSheetPatchCertificate,
    context::_RORSContext,
)
    jacobian = patch.state_jacobian_enclosure
    state_count = jacobian.row_count
    jacobian.column_count == state_count || throw(DimensionMismatch(
        "branch stability requires a square state Jacobian"))
    right_bounds = Vector{_RORSExact}(undef, state_count)
    trace_lower = zero(_RORSExact)
    for row in 1:state_count
        right = jacobian[row, row].upper
        trace_lower = _rors_exact_add(
            context, trace_lower, jacobian[row, row].lower)
        for column in 1:state_count
            row == column && continue
            right = _rors_exact_add(
                context, right, _robs_abs_upper(jacobian[row, column]))
        end
        right_bounds[row] = right
    end
    right_tuple = Tuple(right_bounds)
    status = if all(<(zero(_RORSExact)), right_tuple)
        _ROBS_STABLE
    elseif trace_lower > 0
        _ROBS_UNSTABLE
    else
        _ROBS_UNKNOWN
    end
    # Stable evidence bounds the spectral abscissa above by a negative value.
    # Positive trace implies at least one eigenvalue has real part at least the
    # average trace, so the unstable margin is trace_lower/state_count rather
    # than the (generally too large) trace sum itself.
    margin = status == _ROBS_STABLE ? -maximum(right_tuple) :
        status == _ROBS_UNSTABLE ? _rors_exact_divide(
            context, trace_lower, _RORSExact(state_count)) : nothing
    hopf_excluded = status == _ROBS_STABLE
    response = _robs_patch_log_response(patch, context)
    payload = _robs_patch_evidence_payload(
        patch.certificate_sha256,
        patch.branch_identity_sha256,
        right_tuple,
        trace_lower,
        status,
        margin,
        response,
        true,
        hopf_excluded,
    )
    return ROBranchPatchEvidence(
        _ROBS_VALIDATED_TOKEN,
        patch.certificate_sha256,
        patch.branch_identity_sha256,
        right_tuple,
        trace_lower,
        status,
        margin,
        response,
        true,
        hopf_excluded,
        :exact_tube_stability_and_state_log_response_enclosure,
        _robs_hash(payload),
    )
end

function _robs_find_root!(parents::Vector{Int}, index::Int)
    root = index
    while parents[root] != root
        root = parents[root]
    end
    while parents[index] != index
        next_index = parents[index]
        parents[index] = root
        index = next_index
    end
    return root
end

function _robs_union!(parents::Vector{Int}, left::Int, right::Int)
    left_root = _robs_find_root!(parents, left)
    right_root = _robs_find_root!(parents, right)
    left_root == right_root && return nothing
    if left_root < right_root
        parents[right_root] = left_root
    else
        parents[left_root] = right_root
    end
    return nothing
end

function _robs_build_branches(
    system::ROPolynomialEquilibriumSystem,
    patches::Vector{RORegularSheetPatchCertificate},
    bridges::Vector{RORegularSheetBridgeCertificate},
    patch_evidence::Vector{ROBranchPatchEvidence},
    limits::ROBranchIndexedFieldLimits,
    cancel_check,
)
    patch_index = Dict(
        patch.certificate_sha256 => index
        for (index, patch) in enumerate(patches)
    )
    parents = collect(eachindex(patches))
    seen_endpoint_pairs = Set{Tuple{String,String}}()
    for bridge in bridges
        _robs_cancel(cancel_check)
        left = min(bridge.parent_patch_sha256, bridge.child_patch_sha256)
        right = max(bridge.parent_patch_sha256, bridge.child_patch_sha256)
        pair = (left, right)
        pair in seen_endpoint_pairs && throw(ArgumentError(
            "duplicate bridge endpoint pairs must be canonicalized"))
        push!(seen_endpoint_pairs, pair)
        _robs_union!(
            parents,
            patch_index[bridge.parent_patch_sha256],
            patch_index[bridge.child_patch_sha256],
        )
    end
    component_indices = Dict{Int,Vector{Int}}()
    for index in eachindex(patches)
        root = _robs_find_root!(parents, index)
        push!(get!(component_indices, root, Int[]), index)
    end
    _robs_limit(:branches, length(component_indices), limits.max_branches)
    branch_specs = NamedTuple[]
    for indices in values(component_indices)
        _robs_cancel(cancel_check)
        patch_hashes = Tuple(sort!(
            [patches[index].certificate_sha256 for index in indices]))
        patch_set = Set(patch_hashes)
        bridge_hashes = Tuple(sort!(String[
            bridge.certificate_sha256 for bridge in bridges
            if bridge.parent_patch_sha256 in patch_set &&
                bridge.child_patch_sha256 in patch_set
        ]))
        identity_payload = (
            version=RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION,
            system_declaration_sha256=system.declaration_sha256,
            patch_certificate_sha256s=patch_hashes,
            bridge_certificate_sha256s=bridge_hashes,
            identity_scope="evidence_relative_continuation_component",
        )
        push!(branch_specs, (
            identity=_robs_hash(identity_payload),
            patch_hashes=patch_hashes,
            bridge_hashes=bridge_hashes,
        ))
    end
    sort!(branch_specs; by=spec -> spec.identity)
    evidence_by_patch = Dict(
        evidence.patch_certificate_sha256 => evidence
        for evidence in patch_evidence
    )
    branches = ROBranchComponentEvidence[]
    branch_by_patch = Dict{String,String}()
    for (branch_index, spec) in enumerate(branch_specs)
        statuses = [evidence_by_patch[hash].stability_status
            for hash in spec.patch_hashes]
        stable_count = count(==(_ROBS_STABLE), statuses)
        unstable_count = count(==(_ROBS_UNSTABLE), statuses)
        unknown_count = count(==(_ROBS_UNKNOWN), statuses)
        status = stable_count == length(statuses) ?
            :all_supplied_patches_certified_stable :
            stable_count > 0 ? :contains_certified_stable_patches :
            :no_certified_stable_patch
        payload = _robs_branch_payload(
            branch_index,
            spec.identity,
            spec.patch_hashes,
            spec.bridge_hashes,
            stable_count,
            unstable_count,
            unknown_count,
            status,
        )
        branch = ROBranchComponentEvidence(
            _ROBS_VALIDATED_TOKEN,
            branch_index,
            spec.identity,
            spec.patch_hashes,
            spec.bridge_hashes,
            stable_count,
            unstable_count,
            unknown_count,
            status,
            _robs_hash(payload),
        )
        push!(branches, branch)
        for patch_hash in spec.patch_hashes
            branch_by_patch[patch_hash] = spec.identity
        end
    end
    return branches, branch_by_patch
end

function _robs_patch_state_tube_on_box(
    patch::RORegularSheetPatchCertificate,
    control_box::Vector{ROExactInterval},
    context::_RORSContext,
)
    length(control_box) == length(patch.control_box) ||
        throw(DimensionMismatch(
            "witness control box does not match patch controls"))
    all(index -> _rors_subset(
        control_box[index], patch.control_box[index]),
        eachindex(control_box),
    ) || throw(ROBranchIndexedFieldCertificationRejected(
        :witness_outside_patch_control_box,
        "a selected stable patch does not cover the complete witness box",
    ))
    slope = _rors_exact_matrix_values(patch.predictor_slope)
    result = Vector{ROExactInterval}(undef, length(patch.state_reference))
    for state in eachindex(result)
        value = _rors_point(context, patch.state_reference[state])
        for control in eachindex(control_box)
            offset = _rors_subtract(
                context,
                control_box[control],
                _rors_point(context, patch.control_reference[control]),
            )
            value = _rors_add(
                context,
                value,
                _rors_multiply(
                    context,
                    _rors_point(context, slope[state, control]),
                    offset,
                ),
            )
        end
        result[state] = _rors_add(
            context, value, patch.remainder_box[state])
        result[state].lower > 0 || throw(ArgumentError(
            "selected witness state tube left positive coordinates"))
    end
    return Tuple(result)
end

function _robs_pair_separation(
    system::ROPolynomialEquilibriumSystem,
    left_branch::String,
    right_branch::String,
    left_tube::Tuple,
    right_tube::Tuple,
    context::_RORSContext,
)
    left_branch < right_branch || throw(ArgumentError(
        "branch separation inputs must be canonically ordered"))
    length(left_tube) == length(right_tube) ==
        length(system.state_names) || throw(DimensionMismatch(
        "branch state tubes must match the system state order"))
    best_state = 0
    best_margin = zero(_RORSExact)
    for state in eachindex(left_tube)
        margin = if left_tube[state].upper < right_tube[state].lower
            _rors_exact_subtract(
                context,
                right_tube[state].lower,
                left_tube[state].upper,
            )
        elseif right_tube[state].upper < left_tube[state].lower
            _rors_exact_subtract(
                context,
                left_tube[state].lower,
                right_tube[state].upper,
            )
        else
            zero(_RORSExact)
        end
        if margin > best_margin
            best_state = state
            best_margin = margin
        end
    end
    best_state > 0 || throw(ROBranchIndexedFieldCertificationRejected(
        :selected_stable_root_tubes_not_separated,
        "two selected branch tubes are not strictly separated on the witness box",
    ))
    payload = _robs_separation_payload(
        left_branch,
        right_branch,
        best_state,
        system.state_names[best_state],
        best_margin,
    )
    return ROBranchSeparationEvidence(
        _ROBS_VALIDATED_TOKEN,
        left_branch,
        right_branch,
        best_state,
        system.state_names[best_state],
        best_margin,
        _robs_hash(payload),
    )
end

function _robs_parse_witnesses(
    witnesses,
    system::ROPolynomialEquilibriumSystem,
    limits::ROBranchIndexedFieldLimits,
    context::_RORSContext,
)
    witnesses isa AbstractVector || witnesses isa Tuple || throw(ArgumentError(
        "witnesses must be an ordered vector or tuple"))
    _robs_limit(
        :witness_boxes, length(witnesses), limits.max_witness_boxes)
    control_count = length(system.control_names)
    parsed = NamedTuple[]
    for (index, witness) in enumerate(witnesses)
        _robs_cancel(context.cancel_check)
        witness isa NamedTuple && keys(witness) ==
            (:control_lower, :control_upper,
                :patch_certificate_sha256s) || throw(ArgumentError(
            "witness $index must have exactly control_lower, control_upper, and patch_certificate_sha256s"))
        box = _rors_exact_box(
            witness.control_lower,
            witness.control_upper,
            control_count,
            "witness_control",
            context,
        )
        for control in eachindex(box)
            box[control].lower > 0 || throw(ArgumentError(
                "witness controls must be strictly positive"))
            box[control].lower < box[control].upper || throw(ArgumentError(
                "witness controls must have positive width"))
        end
        hashes = witness.patch_certificate_sha256s
        hashes isa AbstractVector || hashes isa Tuple || throw(ArgumentError(
            "witness patch identities must be an ordered vector or tuple"))
        2 <= length(hashes) <= limits.max_witness_branches || throw(
            ROBranchIndexedFieldLimitExceeded(
                :witness_branches,
                length(hashes),
                limits.max_witness_branches,
            ))
        admitted_hashes = String[]
        for hash in hashes
            hash isa AbstractString || throw(ArgumentError(
                "witness patch identities must be strings"))
            text = String(hash)
            _rors_validate_sha256(text, "witness patch certificate SHA-256")
            push!(admitted_hashes, text)
        end
        allunique(admitted_hashes) || throw(ArgumentError(
            "witness patch identities must be unique"))
        push!(parsed, (
            control_box=Tuple(box),
            patch_hashes=Tuple(admitted_hashes),
        ))
    end
    return parsed
end

function _robs_exact_witness_specs(
    witnesses::Tuple,
)
    return NamedTuple[
        (
            control_box=witness.control_box,
            patch_hashes=witness.selected_patch_certificate_sha256s,
        ) for witness in witnesses
    ]
end

function _robs_build_witnesses(
    system::ROPolynomialEquilibriumSystem,
    witness_specs::Vector{<:NamedTuple},
    patch_by_hash::Dict{String,RORegularSheetPatchCertificate},
    patch_evidence_by_hash::Dict{String,ROBranchPatchEvidence},
    branch_by_patch::Dict{String,String},
    limits::ROBranchIndexedFieldLimits,
    context::_RORSContext,
)
    witnesses = ROMultistabilityWitnessEvidence[]
    pair_count = BigInt(0)
    for (witness_index, spec) in enumerate(witness_specs)
        _robs_cancel(context.cancel_check)
        selections = NamedTuple[]
        for patch_hash in spec.patch_hashes
            patch = get(patch_by_hash, patch_hash, nothing)
            patch === nothing && throw(ArgumentError(
                "witness $witness_index names a patch outside the field"))
            evidence = patch_evidence_by_hash[patch_hash]
            evidence.stability_status == _ROBS_STABLE || throw(
                ROBranchIndexedFieldCertificationRejected(
                    :witness_patch_not_uniformly_stable,
                    "witness $witness_index selected a patch without exact uniform stability evidence",
                ))
            branch_identity = branch_by_patch[patch_hash]
            state_tube = _robs_patch_state_tube_on_box(
                patch, collect(spec.control_box), context)
            push!(selections, (
                branch_identity=branch_identity,
                patch_hash=patch_hash,
                state_tube=state_tube,
            ))
        end
        sort!(selections; by=selection ->
            (selection.branch_identity, selection.patch_hash))
        branch_hashes = [selection.branch_identity
            for selection in selections]
        allunique(branch_hashes) || throw(
            ROBranchIndexedFieldCertificationRejected(
                :duplicate_branch_component_in_witness,
                "one witness cannot count two patches from the same replayed branch component",
            ))
        separations = ROBranchSeparationEvidence[]
        for left in 1:(length(selections) - 1)
            for right in (left + 1):length(selections)
                pair_count += 1
                _robs_limit(
                    :pairwise_separations,
                    pair_count,
                    limits.max_pairwise_separations,
                )
                push!(separations, _robs_pair_separation(
                    system,
                    selections[left].branch_identity,
                    selections[right].branch_identity,
                    selections[left].state_tube,
                    selections[right].state_tube,
                    context,
                ))
            end
        end
        patch_hashes = Tuple(selection.patch_hash
            for selection in selections)
        branch_hash_tuple = Tuple(selection.branch_identity
            for selection in selections)
        state_tubes = Tuple(selection.state_tube
            for selection in selections)
        separation_tuple = Tuple(separations)
        payload = _robs_witness_payload(
            spec.control_box,
            patch_hashes,
            branch_hash_tuple,
            state_tubes,
            separation_tuple,
            length(selections),
        )
        push!(witnesses, ROMultistabilityWitnessEvidence(
            _ROBS_VALIDATED_TOKEN,
            spec.control_box,
            patch_hashes,
            branch_hash_tuple,
            state_tubes,
            separation_tuple,
            length(selections),
            false,
            false,
            false,
            :stable_root_lower_bound_on_declared_control_box,
            _robs_hash(payload),
        ))
    end
    sort!(witnesses; by=witness -> witness.evidence_sha256)
    allunique(witness.evidence_sha256 for witness in witnesses) ||
        throw(ArgumentError(
            "duplicate multistability witnesses must be removed"))
    return witnesses
end

function _robs_certify_exact(
    system::ROPolynomialEquilibriumSystem,
    binding::ROPolynomialDynamicsBinding,
    patches::Vector{RORegularSheetPatchCertificate},
    bridges::Vector{RORegularSheetBridgeCertificate},
    witness_specs::Vector{<:NamedTuple},
    limits::ROBranchIndexedFieldLimits,
    cancel_check,
)
    validate_ro_polynomial_dynamics_binding(
        system, binding; limits=limits)
    _robs_limit(:patches, length(patches), limits.max_patches)
    _robs_limit(:bridges, length(bridges), limits.max_bridges)
    !isempty(patches) || throw(ArgumentError(
        "a branch-indexed field requires at least one regular-sheet patch"))
    sort!(patches; by=patch -> patch.certificate_sha256)
    sort!(bridges; by=bridge -> bridge.certificate_sha256)
    patch_hashes = [patch.certificate_sha256 for patch in patches]
    bridge_hashes = [bridge.certificate_sha256 for bridge in bridges]
    allunique(patch_hashes) || throw(ArgumentError(
        "regular-sheet patch population contains duplicate certificates"))
    allunique(bridge_hashes) || throw(ArgumentError(
        "regular-sheet bridge population contains duplicate certificates"))

    rebuilt_patches, rebuilt_bridges, source_operations =
        _robs_replay_sources(
            system, patches, bridges, limits, cancel_check)

    analysis_limits = _robs_regular_limits_with_operation_cap(
        system.limits, limits.max_analysis_interval_operations)
    analysis_context = _RORSContext(analysis_limits, cancel_check)
    patch_evidence, branches, branch_by_patch, witnesses = try
        local evidence_values = ROBranchPatchEvidence[]
        for patch in rebuilt_patches
            push!(evidence_values, _robs_patch_evidence(
                patch, analysis_context))
        end
        local branch_values, branch_mapping = _robs_build_branches(
            system,
            rebuilt_patches,
            rebuilt_bridges,
            evidence_values,
            limits,
            cancel_check,
        )
        local patch_by_hash = Dict(
            patch.certificate_sha256 => patch for patch in rebuilt_patches)
        local patch_evidence_by_hash = Dict(
            evidence.patch_certificate_sha256 => evidence
            for evidence in evidence_values)
        local witness_values = _robs_build_witnesses(
            system,
            witness_specs,
            patch_by_hash,
            patch_evidence_by_hash,
            branch_mapping,
            limits,
            analysis_context,
        )
        (evidence_values, branch_values, branch_mapping, witness_values)
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROBranchIndexedFieldLimitExceeded(
                :analysis_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
    analysis_context.operations <= typemax(Int) || throw(
        ROBranchIndexedFieldLimitExceeded(
            :analysis_interval_operations,
            analysis_context.operations,
            limits.max_analysis_interval_operations,
        ))
    analysis_operations = Int(analysis_context.operations)
    maximum_lower_bound = isempty(witnesses) ? 0 : maximum(
        witness.certified_stable_root_lower_bound for witness in witnesses)
    patch_tuple = Tuple(patch_hashes)
    bridge_tuple = Tuple(bridge_hashes)
    patch_evidence_tuple = Tuple(patch_evidence)
    branch_tuple = Tuple(branches)
    witness_tuple = Tuple(witnesses)
    payload = _robs_field_payload(
        system.declaration_sha256,
        binding,
        limits,
        patch_tuple,
        bridge_tuple,
        patch_evidence_tuple,
        branch_tuple,
        witness_tuple,
        source_operations,
        analysis_operations,
        maximum_lower_bound,
    )
    return ROBranchIndexedRegularField(
        _ROBS_VALIDATED_TOKEN,
        RO_BRANCH_INDEXED_REGULAR_FIELD_VERSION,
        system.declaration_sha256,
        binding,
        limits,
        patch_tuple,
        bridge_tuple,
        patch_evidence_tuple,
        branch_tuple,
        witness_tuple,
        source_operations,
        analysis_operations,
        maximum_lower_bound,
        true,
        true,
        :witness_lower_bound_only,
        false,
        false,
        false,
        false,
        false,
        RO_BRANCH_INDEXED_REGULAR_FIELD_SCOPE,
        _robs_hash(payload),
    )
end

"""
    certify_ro_branch_indexed_regular_field(system, binding, patches, bridges;
        witnesses, limits, cancel_check)

Build evidence-relative continuation components from replayed P5r0 patches and
bridges. A witness explicitly chooses one uniformly stable patch per component;
strict tube separation then proves only a stable-root lower bound on its
declared positive control box.
"""
function certify_ro_branch_indexed_regular_field(
    system::ROPolynomialEquilibriumSystem,
    binding::ROPolynomialDynamicsBinding,
    patches,
    bridges;
    witnesses=NamedTuple[],
    limits::ROBranchIndexedFieldLimits=ROBranchIndexedFieldLimits(),
    cancel_check=() -> nothing,
)
    validate_ro_polynomial_equilibrium_system(system)
    patches isa AbstractVector || patches isa Tuple || throw(ArgumentError(
        "patches must be an ordered vector or tuple"))
    bridges isa AbstractVector || bridges isa Tuple || throw(ArgumentError(
        "bridges must be an ordered vector or tuple"))
    admitted_patches = RORegularSheetPatchCertificate[]
    for patch in patches
        patch isa RORegularSheetPatchCertificate || throw(ArgumentError(
            "patches must contain RORegularSheetPatchCertificate values"))
        push!(admitted_patches, patch)
    end
    admitted_bridges = RORegularSheetBridgeCertificate[]
    for bridge in bridges
        bridge isa RORegularSheetBridgeCertificate || throw(ArgumentError(
            "bridges must contain RORegularSheetBridgeCertificate values"))
        push!(admitted_bridges, bridge)
    end
    parse_limits = _robs_regular_limits_with_operation_cap(
        system.limits, limits.max_analysis_interval_operations)
    parse_context = _RORSContext(parse_limits, cancel_check)
    witness_specs = try
        _robs_parse_witnesses(
            witnesses, system, limits, parse_context)
    catch err
        if err isa RORegularSheetLimitExceeded &&
                err.phase == :interval_operations
            throw(ROBranchIndexedFieldLimitExceeded(
                :witness_admission_interval_operations,
                err.requested,
                limits.max_analysis_interval_operations,
            ))
        end
        rethrow()
    end
    # Strict Float64-to-dyadic admission is a separately bounded parser step;
    # the published analysis count starts from the admitted exact witness box.
    return _robs_certify_exact(
        system,
        binding,
        admitted_patches,
        admitted_bridges,
        witness_specs,
        limits,
        cancel_check,
    )
end

function replay_ro_branch_indexed_regular_field(
    system::ROPolynomialEquilibriumSystem,
    patches,
    bridges,
    certificate::ROBranchIndexedRegularField;
    cancel_check=() -> nothing,
)
    certificate.system_declaration_sha256 == system.declaration_sha256 ||
        throw(ArgumentError(
            "branch-indexed field belongs to a different polynomial system"))
    patches isa AbstractVector || patches isa Tuple || throw(ArgumentError(
        "patches must be an ordered vector or tuple"))
    bridges isa AbstractVector || bridges isa Tuple || throw(ArgumentError(
        "bridges must be an ordered vector or tuple"))
    admitted_patches = RORegularSheetPatchCertificate[patch for patch in patches]
    admitted_bridges = RORegularSheetBridgeCertificate[bridge for bridge in bridges]
    witness_specs = _robs_exact_witness_specs(
        certificate.multistability_witnesses)
    rebuilt = _robs_certify_exact(
        system,
        certificate.dynamics_binding,
        admitted_patches,
        admitted_bridges,
        witness_specs,
        certificate.limits,
        cancel_check,
    )
    rebuilt.certificate_sha256 == certificate.certificate_sha256 ||
        throw(ArgumentError(
            "branch-indexed regular field replay changed its certificate hash"))
    return rebuilt
end

function validate_ro_branch_indexed_regular_field(
    system::ROPolynomialEquilibriumSystem,
    patches,
    bridges,
    certificate::ROBranchIndexedRegularField;
    cancel_check=() -> nothing,
)
    replay_ro_branch_indexed_regular_field(
        system,
        patches,
        bridges,
        certificate;
        cancel_check=cancel_check,
    )
    return true
end
