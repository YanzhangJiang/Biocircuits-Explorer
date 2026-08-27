using LinearAlgebra
import JSON3
import SHA

# Conservative, content-addressed evidence for selecting a singular RO branch.
# A result is conditional on one declared bounded enumerator and one finite
# dynamic protocol.  Nothing here claims a universal physical branch, causality,
# or experimental validation.

const RO_SINGULAR_SELECTION_VERSION =
    "bne-ro-singular-selection-certificate/v2.1.0"
const RO_SINGULAR_SELECTION_POLICY_VERSION =
    "bne-ro-singular-selection-policy/v2.0.0"
const RO_SINGULAR_STABILITY_EVIDENCE_VERSION =
    "bne-ro-singular-stability-evidence/v1.0.0"
const RO_SINGULAR_REACHABILITY_EVIDENCE_VERSION =
    "bne-ro-singular-reachability-evidence/v1.0.0"
const RO_SINGULAR_POPULATION_RECEIPT_VERSION =
    "bne-ro-singular-candidate-population-receipt/v1.0.0"
const RO_SINGULAR_BRANCH_IDENTITY_VERSION =
    "bne-ro-singular-branch-identity/v1.0.0"
const RO_SINGULAR_CANDIDATE_PAYLOAD_VERSION =
    "bne-ro-singular-candidate-payload/v1.0.0"
const RO_SINGULAR_SELECTION_SCOPE =
    :unique_under_declared_complete_dynamic_candidate_policy
const _ROSSEL_STABILITY_SCOPE = :finite_declared_local_stability_analysis
const _ROSSEL_REACHABILITY_SCOPE = :finite_declared_protocol_trace
const _ROSSEL_POPULATION_SCOPE = :declared_bounded_enumerator_receipt

export ROSingularStabilityEvidence, ROSingularReachabilityEvidence
export ROSingularCandidatePopulationReceipt
export ro_singular_branch_identity_sha256
export build_ro_singular_candidate_population_receipt

struct ROSingularSelectionLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, error::ROSingularSelectionLimitExceeded)
    print(io, "singular RO branch selection ", error.phase, " requires ",
        error.requested, ", exceeding limit=", error.limit)
end

struct ROSingularSelectionLimits
    max_inputs::Int
    max_outputs::Int
    max_candidates::Int
    max_matrix_elements::Int
    max_source_regime_ids::Int
    max_gap_reasons::Int
    max_candidate_payload_bytes::Int
    max_identity_bytes::Int
end

function _rossel_positive_limit(raw, name::AbstractString, hard_max::Int)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$name must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$name must fit in Int"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$name must lie in 1:$hard_max"))
    return value
end

function ROSingularSelectionLimits(;
    max_inputs::Integer=64,
    max_outputs::Integer=4_096,
    max_candidates::Integer=4_096,
    max_matrix_elements::Integer=2_000_000,
    max_source_regime_ids::Integer=4_096,
    max_gap_reasons::Integer=128,
    max_candidate_payload_bytes::Integer=4 * 1024 * 1024,
    max_identity_bytes::Integer=64 * 1024 * 1024,
)
    return ROSingularSelectionLimits(
        _rossel_positive_limit(max_inputs, "max_inputs", 4_096),
        _rossel_positive_limit(max_outputs, "max_outputs", 65_536),
        _rossel_positive_limit(max_candidates, "max_candidates", 1_000_000),
        _rossel_positive_limit(
            max_matrix_elements, "max_matrix_elements", 100_000_000),
        _rossel_positive_limit(
            max_source_regime_ids, "max_source_regime_ids", 1_000_000),
        _rossel_positive_limit(max_gap_reasons, "max_gap_reasons", 4_096),
        _rossel_positive_limit(max_candidate_payload_bytes,
            "max_candidate_payload_bytes", 64 * 1024 * 1024),
        _rossel_positive_limit(
            max_identity_bytes, "max_identity_bytes", 1_000_000_000),
    )
end

const _ROSSEL_CERTIFICATE_HARD_LIMITS = ROSingularSelectionLimits(
    max_inputs=4_096,
    max_outputs=65_536,
    max_candidates=1_000_000,
    max_matrix_elements=100_000_000,
    max_source_regime_ids=1_000_000,
    max_gap_reasons=4_096,
    max_candidate_payload_bytes=64 * 1024 * 1024,
    max_identity_bytes=1_000_000_000,
)

@inline function _rossel_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount <= BigInt(limit) || throw(
        ROSingularSelectionLimitExceeded(phase, amount, limit))
    return nothing
end

function _rossel_hash(raw, name::AbstractString)
    raw isa AbstractString || throw(ArgumentError("$name must be a string"))
    value = String(raw)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$name must be a lowercase SHA-256 string"))
    return value
end

function _rossel_id(raw, name::AbstractString)
    (raw isa AbstractString || raw isa Symbol) || throw(ArgumentError(
        "$name must be a string or symbol"))
    value = String(raw)
    isempty(value) && throw(ArgumentError("$name must not be empty"))
    ncodeunits(value) <= 256 || throw(ArgumentError(
        "$name must not exceed 256 UTF-8 bytes"))
    return value
end

function _rossel_ids(raw, name::AbstractString, maximum::Int)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$name must be an ordered collection"))
    1 <= length(raw) <= maximum || throw(ArgumentError(
        "$name must contain 1:$maximum entries"))
    result = String[_rossel_id(value, "$name[]") for value in raw]
    allunique(result) || throw(ArgumentError(
        "$name must contain unique ordered identifiers"))
    return result
end

function _rossel_units(raw, name::AbstractString, expected::Int)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$name must be an ordered collection"))
    length(raw) == expected || throw(DimensionMismatch(
        "$name must match its coordinate order"))
    return String[_rossel_id(value, "$name[]") for value in raw]
end

function _rossel_gaps(raw, name::AbstractString,
                      limits::ROSingularSelectionLimits)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "$name must be an ordered collection"))
    _rossel_limit(:gap_reasons, length(raw), limits.max_gap_reasons)
    gaps = Symbol[]
    for reason in raw
        reason isa Symbol || throw(ArgumentError("$name entries must be symbols"))
        _rossel_id(reason, "$name[]")
        push!(gaps, reason)
    end
    allunique(gaps) || throw(ArgumentError("$name must not contain duplicates"))
    sort!(gaps; by=String)
    return gaps
end

function _rossel_regimes(raw, limits::ROSingularSelectionLimits)
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "source_regime_ids must be a collection"))
    _rossel_limit(:source_regime_ids, length(raw),
        limits.max_source_regime_ids)
    regimes = try
        Int[convert(Int, value) for value in raw]
    catch
        throw(ArgumentError("source_regime_ids must contain Int-compatible values"))
    end
    isempty(regimes) && throw(ArgumentError(
        "source_regime_ids must not be empty"))
    all(>(0), regimes) || throw(ArgumentError(
        "source_regime_ids must be positive"))
    allunique(regimes) || throw(ArgumentError(
        "source_regime_ids must be unique"))
    sort!(regimes)
    return regimes
end

function _rossel_strict_float64(raw, name::AbstractString;
                                nonnegative::Bool=false,
                                positive::Bool=false)
    typeof(raw) === Float64 || throw(ArgumentError(
        "$name must be supplied as Float64; implicit precision conversion is forbidden"))
    value = raw == 0.0 ? 0.0 : raw
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    nonnegative && value < 0.0 && throw(ArgumentError(
        "$name must be nonnegative"))
    positive && value <= 0.0 && throw(ArgumentError(
        "$name must be positive"))
    return value
end

function _rossel_sha_payload(payload, phase::Symbol, limit::Int)
    document = JSON3.write(payload)
    _rossel_limit(phase, ncodeunits(document), limit)
    return bytes2hex(SHA.sha256(codeunits(document)))
end

_rossel_string_bytes(values) = sum(ncodeunits(String(value)) for value in values;
    init=0)

struct _ROSSelValidatedToken end
const _ROSSEL_VALIDATED = _ROSSelValidatedToken()

struct ROSingularSelectionPolicy
    schema_version::String
    policy_sha256::String
    regular_extension_sha256::String
    stratum_sha256::String
    model_sha256::String
    protocol_sha256::String
    branch_enumerator_sha256::String
    enumeration_scope_sha256::String
    stability_policy_sha256::String
    reachability_policy_sha256::String
    input_order::Vector{String}
    input_units::Vector{String}
    output_order::Vector{String}
    output_units::Vector{String}
    residual_norm::Symbol
    residual_scaling::Symbol
    residual_unit::String
    residual_scale::Float64
    residual_evaluator_sha256::String
    residual_absolute_tolerance::Float64
    selection_rule::Symbol
    function ROSingularSelectionPolicy(::_ROSSelValidatedToken, args...)
        return new(args...)
    end
end

function _rossel_policy_payload(policy::ROSingularSelectionPolicy)
    return (
        schema_version=getfield(policy, :schema_version),
        regular_extension_sha256=
            getfield(policy, :regular_extension_sha256),
        stratum_sha256=getfield(policy, :stratum_sha256),
        model_sha256=getfield(policy, :model_sha256),
        protocol_sha256=getfield(policy, :protocol_sha256),
        branch_enumerator_sha256=
            getfield(policy, :branch_enumerator_sha256),
        enumeration_scope_sha256=
            getfield(policy, :enumeration_scope_sha256),
        stability_policy_sha256=
            getfield(policy, :stability_policy_sha256),
        reachability_policy_sha256=
            getfield(policy, :reachability_policy_sha256),
        inputs=(order=getfield(policy, :input_order),
            units=getfield(policy, :input_units)),
        outputs=(order=getfield(policy, :output_order),
            units=getfield(policy, :output_units)),
        residual=(
            norm=String(getfield(policy, :residual_norm)),
            scaling=String(getfield(policy, :residual_scaling)),
            unit=getfield(policy, :residual_unit),
            scale=getfield(policy, :residual_scale),
            evaluator_sha256=getfield(policy, :residual_evaluator_sha256),
            absolute_tolerance=
                getfield(policy, :residual_absolute_tolerance),
        ),
        selection_rule=String(getfield(policy, :selection_rule)),
    )
end

function _rossel_policy_reservation(policy::ROSingularSelectionPolicy)
    strings = Any[
        getfield(policy, :input_order); getfield(policy, :input_units);
        getfield(policy, :output_order); getfield(policy, :output_units);
        getfield(policy, :residual_unit)
    ]
    return BigInt(8_192 + _rossel_string_bytes(strings))
end

function _rossel_validate_policy!(policy::ROSingularSelectionPolicy,
                                  limits::ROSingularSelectionLimits)
    getfield(policy, :schema_version) ==
        RO_SINGULAR_SELECTION_POLICY_VERSION ||
        throw(ArgumentError("unsupported singular selection policy version"))
    for (value, name) in (
        (getfield(policy, :policy_sha256), "policy_sha256"),
        (getfield(policy, :regular_extension_sha256),
            "regular_extension_sha256"),
        (getfield(policy, :stratum_sha256), "stratum_sha256"),
        (getfield(policy, :model_sha256), "model_sha256"),
        (getfield(policy, :protocol_sha256), "protocol_sha256"),
        (getfield(policy, :branch_enumerator_sha256),
            "branch_enumerator_sha256"),
        (getfield(policy, :enumeration_scope_sha256),
            "enumeration_scope_sha256"),
        (getfield(policy, :stability_policy_sha256),
            "stability_policy_sha256"),
        (getfield(policy, :reachability_policy_sha256),
            "reachability_policy_sha256"),
        (getfield(policy, :residual_evaluator_sha256),
            "residual_evaluator_sha256"),
    )
        _rossel_hash(value, name)
    end
    input_order = getfield(policy, :input_order)
    input_units = getfield(policy, :input_units)
    output_order = getfield(policy, :output_order)
    output_units = getfield(policy, :output_units)
    _rossel_ids(input_order, "input_order", limits.max_inputs) ==
        input_order || throw(ArgumentError("input_order is not canonical"))
    _rossel_ids(output_order, "output_order", limits.max_outputs) ==
        output_order || throw(ArgumentError("output_order is not canonical"))
    _rossel_units(input_units, "input_units", length(input_order)) ==
        input_units || throw(
            ArgumentError("input_units are not canonical"))
    _rossel_units(output_units, "output_units", length(output_order)) ==
        output_units || throw(
            ArgumentError("output_units are not canonical"))
    getfield(policy, :residual_norm) in (:linf, :l2) || throw(ArgumentError(
        "unsupported residual_norm"))
    getfield(policy, :residual_scaling) in
        (:absolute, :divide_by_declared_scale) ||
        throw(ArgumentError("unsupported residual_scaling"))
    _rossel_id(getfield(policy, :residual_unit), "residual_unit")
    _rossel_strict_float64(getfield(policy, :residual_scale), "residual_scale";
        positive=true)
    getfield(policy, :residual_scaling) == :absolute &&
        getfield(policy, :residual_scale) != 1.0 &&
        throw(ArgumentError("absolute residual scaling requires residual_scale=1.0"))
    _rossel_strict_float64(getfield(policy, :residual_absolute_tolerance),
        "residual_absolute_tolerance"; nonnegative=true)
    getfield(policy, :selection_rule) ==
        :unique_stable_reachable_complete_candidate ||
        throw(ArgumentError("unsupported singular selection rule"))
    _rossel_limit(:policy_identity_reservation,
        _rossel_policy_reservation(policy), limits.max_identity_bytes)
    expected = _rossel_sha_payload(_rossel_policy_payload(policy),
        :policy_identity_bytes, limits.max_identity_bytes)
    getfield(policy, :policy_sha256) == expected || throw(ArgumentError(
        "singular selection policy self-hash mismatch"))
    return policy
end

function ROSingularSelectionPolicy(;
    regular_extension_sha256,
    stratum_sha256,
    model_sha256,
    protocol_sha256,
    branch_enumerator_sha256,
    enumeration_scope_sha256,
    stability_policy_sha256,
    reachability_policy_sha256,
    input_order,
    input_units,
    output_order,
    output_units,
    residual_norm=:linf,
    residual_scaling=:absolute,
    residual_unit,
    residual_scale=1.0,
    residual_evaluator_sha256,
    residual_absolute_tolerance,
    selection_rule=:unique_stable_reachable_complete_candidate,
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    inputs = _rossel_ids(input_order, "input_order", limits.max_inputs)
    outputs = _rossel_ids(output_order, "output_order", limits.max_outputs)
    input_unit_values = _rossel_units(
        input_units, "input_units", length(inputs))
    output_unit_values = _rossel_units(
        output_units, "output_units", length(outputs))
    norm = Symbol(residual_norm)
    scaling = Symbol(residual_scaling)
    unit = _rossel_id(residual_unit, "residual_unit")
    scale = _rossel_strict_float64(residual_scale, "residual_scale";
        positive=true)
    tolerance = _rossel_strict_float64(residual_absolute_tolerance,
        "residual_absolute_tolerance"; nonnegative=true)
    prototype = ROSingularSelectionPolicy(_ROSSEL_VALIDATED,
        RO_SINGULAR_SELECTION_POLICY_VERSION, repeat("0", 64),
        _rossel_hash(regular_extension_sha256, "regular_extension_sha256"),
        _rossel_hash(stratum_sha256, "stratum_sha256"),
        _rossel_hash(model_sha256, "model_sha256"),
        _rossel_hash(protocol_sha256, "protocol_sha256"),
        _rossel_hash(branch_enumerator_sha256, "branch_enumerator_sha256"),
        _rossel_hash(enumeration_scope_sha256, "enumeration_scope_sha256"),
        _rossel_hash(stability_policy_sha256, "stability_policy_sha256"),
        _rossel_hash(reachability_policy_sha256, "reachability_policy_sha256"),
        inputs, input_unit_values, outputs, output_unit_values,
        norm, scaling, unit, scale,
        _rossel_hash(residual_evaluator_sha256,
            "residual_evaluator_sha256"),
        tolerance, Symbol(selection_rule))
    policy_hash = _rossel_sha_payload(_rossel_policy_payload(prototype),
        :policy_identity_bytes, limits.max_identity_bytes)
    policy = ROSingularSelectionPolicy(_ROSSEL_VALIDATED,
        getfield(prototype, :schema_version), policy_hash,
        getfield(prototype, :regular_extension_sha256),
        getfield(prototype, :stratum_sha256),
        getfield(prototype, :model_sha256),
        getfield(prototype, :protocol_sha256),
        getfield(prototype, :branch_enumerator_sha256),
        getfield(prototype, :enumeration_scope_sha256),
        getfield(prototype, :stability_policy_sha256),
        getfield(prototype, :reachability_policy_sha256),
        copy(getfield(prototype, :input_order)),
        copy(getfield(prototype, :input_units)),
        copy(getfield(prototype, :output_order)),
        copy(getfield(prototype, :output_units)),
        getfield(prototype, :residual_norm),
        getfield(prototype, :residual_scaling),
        getfield(prototype, :residual_unit),
        getfield(prototype, :residual_scale),
        getfield(prototype, :residual_evaluator_sha256),
        getfield(prototype, :residual_absolute_tolerance),
        getfield(prototype, :selection_rule))
    return _rossel_validate_policy!(policy, limits)
end

struct ROSingularStabilityEvidence
    schema_version::String
    evidence_sha256::String
    branch_identity_sha256::String
    model_sha256::String
    stratum_sha256::String
    stability_policy_sha256::String
    status::Symbol
    analysis_sha256::Union{Nothing,String}
    gap_reasons::Vector{Symbol}
    evidence_scope::Symbol
    universal_stability_claimed::Bool
    function ROSingularStabilityEvidence(::_ROSSelValidatedToken, args...)
        return new(args...)
    end
end

function _rossel_stability_payload(evidence::ROSingularStabilityEvidence)
    return (
        schema_version=getfield(evidence, :schema_version),
        branch_identity_sha256=
            getfield(evidence, :branch_identity_sha256),
        model_sha256=getfield(evidence, :model_sha256),
        stratum_sha256=getfield(evidence, :stratum_sha256),
        stability_policy_sha256=
            getfield(evidence, :stability_policy_sha256),
        status=String(getfield(evidence, :status)),
        analysis_sha256=getfield(evidence, :analysis_sha256),
        gap_reasons=String.(getfield(evidence, :gap_reasons)),
        evidence_scope=String(getfield(evidence, :evidence_scope)),
        universal_stability_claimed=
            getfield(evidence, :universal_stability_claimed),
    )
end

function _rossel_validate_stability!(
    evidence::ROSingularStabilityEvidence,
    policy::ROSingularSelectionPolicy,
    branch_identity::String,
    limits::ROSingularSelectionLimits,
)
    getfield(evidence, :schema_version) ==
        RO_SINGULAR_STABILITY_EVIDENCE_VERSION ||
        throw(ArgumentError("unsupported stability evidence version"))
    _rossel_hash(getfield(evidence, :evidence_sha256),
        "stability.evidence_sha256")
    getfield(evidence, :branch_identity_sha256) == branch_identity ||
        throw(ArgumentError(
        "stability evidence is bound to a different branch"))
    getfield(evidence, :model_sha256) == getfield(policy, :model_sha256) &&
        getfield(evidence, :stratum_sha256) ==
            getfield(policy, :stratum_sha256) &&
        getfield(evidence, :stability_policy_sha256) ==
            getfield(policy, :stability_policy_sha256) ||
        throw(ArgumentError("stability evidence does not match selection policy"))
    status = getfield(evidence, :status)
    status in (:locally_stable, :unstable, :unknown_gap) ||
        throw(ArgumentError("unsupported stability status"))
    gap_reasons = getfield(evidence, :gap_reasons)
    gaps = _rossel_gaps(gap_reasons,
        "stability.gap_reasons", limits)
    gaps == gap_reasons || throw(ArgumentError(
        "stability gap reasons are not canonical"))
    analysis_sha256 = getfield(evidence, :analysis_sha256)
    if status == :unknown_gap
        analysis_sha256 === nothing || throw(ArgumentError(
            "unknown stability evidence cannot claim a completed analysis"))
        isempty(gaps) && throw(ArgumentError(
            "unknown stability evidence requires gap reasons"))
    else
        analysis_sha256 === nothing && throw(ArgumentError(
            "known stability status requires an analysis hash"))
        _rossel_hash(analysis_sha256, "stability.analysis_sha256")
        isempty(gaps) || throw(ArgumentError(
            "known stability evidence cannot carry gaps"))
    end
    getfield(evidence, :evidence_scope) == _ROSSEL_STABILITY_SCOPE &&
        !getfield(evidence, :universal_stability_claimed) ||
        throw(ArgumentError(
            "stability evidence exceeds the finite declared scope"))
    expected = _rossel_sha_payload(_rossel_stability_payload(evidence),
        :stability_evidence_bytes, limits.max_candidate_payload_bytes)
    getfield(evidence, :evidence_sha256) == expected || throw(ArgumentError(
        "stability evidence self-hash mismatch"))
    return evidence
end

function ROSingularStabilityEvidence(;
    branch_identity_sha256,
    policy::ROSingularSelectionPolicy,
    status,
    analysis_sha256=nothing,
    gap_reasons=Symbol[],
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    _rossel_validate_policy!(policy, limits)
    branch_hash = _rossel_hash(
        branch_identity_sha256, "branch_identity_sha256")
    gaps = _rossel_gaps(gap_reasons, "stability.gap_reasons", limits)
    analysis = analysis_sha256 === nothing ? nothing :
        _rossel_hash(analysis_sha256, "stability.analysis_sha256")
    prototype = ROSingularStabilityEvidence(_ROSSEL_VALIDATED,
        RO_SINGULAR_STABILITY_EVIDENCE_VERSION, repeat("0", 64),
        branch_hash, getfield(policy, :model_sha256),
        getfield(policy, :stratum_sha256),
        getfield(policy, :stability_policy_sha256), Symbol(status), analysis, gaps,
        _ROSSEL_STABILITY_SCOPE, false)
    evidence_hash = _rossel_sha_payload(_rossel_stability_payload(prototype),
        :stability_evidence_bytes, limits.max_candidate_payload_bytes)
    evidence = ROSingularStabilityEvidence(_ROSSEL_VALIDATED,
        getfield(prototype, :schema_version), evidence_hash,
        getfield(prototype, :branch_identity_sha256),
        getfield(prototype, :model_sha256),
        getfield(prototype, :stratum_sha256),
        getfield(prototype, :stability_policy_sha256),
        getfield(prototype, :status), getfield(prototype, :analysis_sha256),
        copy(getfield(prototype, :gap_reasons)),
        getfield(prototype, :evidence_scope),
        getfield(prototype, :universal_stability_claimed))
    return _rossel_validate_stability!(
        evidence, policy, branch_hash, limits)
end

struct ROSingularReachabilityEvidence
    schema_version::String
    evidence_sha256::String
    branch_identity_sha256::String
    model_sha256::String
    stratum_sha256::String
    protocol_sha256::String
    reachability_policy_sha256::String
    status::Symbol
    dynamic_trace_sha256::Union{Nothing,String}
    gap_reasons::Vector{Symbol}
    evidence_scope::Symbol
    global_reachability_claimed::Bool
    causal_claimed::Bool
    function ROSingularReachabilityEvidence(::_ROSSelValidatedToken, args...)
        return new(args...)
    end
end

function _rossel_reachability_payload(evidence::ROSingularReachabilityEvidence)
    return (
        schema_version=getfield(evidence, :schema_version),
        branch_identity_sha256=
            getfield(evidence, :branch_identity_sha256),
        model_sha256=getfield(evidence, :model_sha256),
        stratum_sha256=getfield(evidence, :stratum_sha256),
        protocol_sha256=getfield(evidence, :protocol_sha256),
        reachability_policy_sha256=
            getfield(evidence, :reachability_policy_sha256),
        status=String(getfield(evidence, :status)),
        dynamic_trace_sha256=getfield(evidence, :dynamic_trace_sha256),
        gap_reasons=String.(getfield(evidence, :gap_reasons)),
        evidence_scope=String(getfield(evidence, :evidence_scope)),
        global_reachability_claimed=
            getfield(evidence, :global_reachability_claimed),
        causal_claimed=getfield(evidence, :causal_claimed),
    )
end

function _rossel_validate_reachability!(
    evidence::ROSingularReachabilityEvidence,
    policy::ROSingularSelectionPolicy,
    branch_identity::String,
    limits::ROSingularSelectionLimits,
)
    getfield(evidence, :schema_version) ==
        RO_SINGULAR_REACHABILITY_EVIDENCE_VERSION ||
        throw(ArgumentError("unsupported reachability evidence version"))
    _rossel_hash(getfield(evidence, :evidence_sha256),
        "reachability.evidence_sha256")
    getfield(evidence, :branch_identity_sha256) == branch_identity ||
        throw(ArgumentError(
        "reachability evidence is bound to a different branch"))
    getfield(evidence, :model_sha256) == getfield(policy, :model_sha256) &&
        getfield(evidence, :stratum_sha256) ==
            getfield(policy, :stratum_sha256) &&
        getfield(evidence, :protocol_sha256) ==
            getfield(policy, :protocol_sha256) &&
        getfield(evidence, :reachability_policy_sha256) ==
            getfield(policy, :reachability_policy_sha256) || throw(ArgumentError(
                "reachability evidence does not match selection policy"))
    status = getfield(evidence, :status)
    status in
        (:reached_under_protocol, :not_reached, :unknown_gap) ||
        throw(ArgumentError("unsupported reachability status"))
    gap_reasons = getfield(evidence, :gap_reasons)
    gaps = _rossel_gaps(gap_reasons,
        "reachability.gap_reasons", limits)
    gaps == gap_reasons || throw(ArgumentError(
        "reachability gap reasons are not canonical"))
    dynamic_trace_sha256 = getfield(evidence, :dynamic_trace_sha256)
    if status == :unknown_gap
        dynamic_trace_sha256 === nothing || throw(ArgumentError(
            "unknown reachability evidence cannot claim a completed trace"))
        isempty(gaps) && throw(ArgumentError(
            "unknown reachability evidence requires gap reasons"))
    else
        dynamic_trace_sha256 === nothing && throw(ArgumentError(
            "known reachability status requires a dynamic trace hash"))
        _rossel_hash(dynamic_trace_sha256,
            "reachability.dynamic_trace_sha256")
        isempty(gaps) || throw(ArgumentError(
            "known reachability evidence cannot carry gaps"))
    end
    getfield(evidence, :evidence_scope) == _ROSSEL_REACHABILITY_SCOPE &&
        !getfield(evidence, :global_reachability_claimed) &&
        !getfield(evidence, :causal_claimed) ||
        throw(ArgumentError(
            "reachability evidence exceeds the finite declared protocol scope"))
    expected = _rossel_sha_payload(_rossel_reachability_payload(evidence),
        :reachability_evidence_bytes, limits.max_candidate_payload_bytes)
    getfield(evidence, :evidence_sha256) == expected || throw(ArgumentError(
        "reachability evidence self-hash mismatch"))
    return evidence
end

function ROSingularReachabilityEvidence(;
    branch_identity_sha256,
    policy::ROSingularSelectionPolicy,
    status,
    dynamic_trace_sha256=nothing,
    gap_reasons=Symbol[],
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    _rossel_validate_policy!(policy, limits)
    branch_hash = _rossel_hash(
        branch_identity_sha256, "branch_identity_sha256")
    gaps = _rossel_gaps(gap_reasons, "reachability.gap_reasons", limits)
    trace = dynamic_trace_sha256 === nothing ? nothing :
        _rossel_hash(dynamic_trace_sha256,
            "reachability.dynamic_trace_sha256")
    prototype = ROSingularReachabilityEvidence(_ROSSEL_VALIDATED,
        RO_SINGULAR_REACHABILITY_EVIDENCE_VERSION, repeat("0", 64),
        branch_hash, getfield(policy, :model_sha256),
        getfield(policy, :stratum_sha256),
        getfield(policy, :protocol_sha256),
        getfield(policy, :reachability_policy_sha256),
        Symbol(status), trace, gaps, _ROSSEL_REACHABILITY_SCOPE,
        false, false)
    evidence_hash = _rossel_sha_payload(
        _rossel_reachability_payload(prototype),
        :reachability_evidence_bytes, limits.max_candidate_payload_bytes)
    evidence = ROSingularReachabilityEvidence(_ROSSEL_VALIDATED,
        getfield(prototype, :schema_version), evidence_hash,
        getfield(prototype, :branch_identity_sha256),
        getfield(prototype, :model_sha256),
        getfield(prototype, :stratum_sha256),
        getfield(prototype, :protocol_sha256),
        getfield(prototype, :reachability_policy_sha256),
        getfield(prototype, :status),
        getfield(prototype, :dynamic_trace_sha256),
        copy(getfield(prototype, :gap_reasons)),
        getfield(prototype, :evidence_scope),
        getfield(prototype, :global_reachability_claimed),
        getfield(prototype, :causal_claimed))
    return _rossel_validate_reachability!(
        evidence, policy, branch_hash, limits)
end

function _rossel_branch_payload(policy::ROSingularSelectionPolicy,
                                branch_id::String, regimes::Vector{Int})
    return (
        schema_version=RO_SINGULAR_BRANCH_IDENTITY_VERSION,
        model_sha256=getfield(policy, :model_sha256),
        stratum_sha256=getfield(policy, :stratum_sha256),
        branch_id=branch_id,
        source_regime_ids=regimes,
    )
end

function ro_singular_branch_identity_sha256(;
    policy::ROSingularSelectionPolicy,
    branch_id,
    source_regime_ids,
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    _rossel_validate_policy!(policy, limits)
    identifier = _rossel_id(branch_id, "branch_id")
    regimes = _rossel_regimes(source_regime_ids, limits)
    reservation = BigInt(2_048 + ncodeunits(identifier)) +
        BigInt(24) * BigInt(length(regimes))
    _rossel_limit(:branch_identity_reservation,
        reservation, limits.max_candidate_payload_bytes)
    return _rossel_sha_payload(
        _rossel_branch_payload(policy, identifier, regimes),
        :branch_identity_bytes, limits.max_candidate_payload_bytes)
end

struct ROSingularBranchCandidate
    schema_version::String
    candidate_payload_sha256::String
    selection_policy_sha256::String
    branch_id::String
    branch_identity_sha256::String
    model_sha256::String
    stratum_sha256::String
    source_regime_ids::Vector{Int}
    jacobian::Union{Nothing,Matrix{Float64}}
    output_at_stratum::Union{Nothing,Vector{Float64}}
    residual_upper_bound::Union{Nothing,Float64}
    numeric_gap_reasons::Vector{Symbol}
    stability_evidence::ROSingularStabilityEvidence
    reachability_evidence::ROSingularReachabilityEvidence
    function ROSingularBranchCandidate(::_ROSSelValidatedToken, args...)
        return new(args...)
    end
end

function _rossel_matrix_payload(matrix)
    matrix === nothing && return nothing
    return [Float64[matrix[row, column] for column in axes(matrix, 2)]
        for row in axes(matrix, 1)]
end

function _rossel_candidate_payload(candidate::ROSingularBranchCandidate)
    stability = getfield(candidate, :stability_evidence)
    reachability = getfield(candidate, :reachability_evidence)
    return (
        schema_version=getfield(candidate, :schema_version),
        selection_policy_sha256=
            getfield(candidate, :selection_policy_sha256),
        branch_id=getfield(candidate, :branch_id),
        branch_identity_sha256=
            getfield(candidate, :branch_identity_sha256),
        model_sha256=getfield(candidate, :model_sha256),
        stratum_sha256=getfield(candidate, :stratum_sha256),
        source_regime_ids=getfield(candidate, :source_regime_ids),
        jacobian=_rossel_matrix_payload(getfield(candidate, :jacobian)),
        output_at_stratum=getfield(candidate, :output_at_stratum),
        residual_upper_bound=getfield(candidate, :residual_upper_bound),
        numeric_gap_reasons=
            String.(getfield(candidate, :numeric_gap_reasons)),
        stability_evidence_sha256=
            getfield(stability, :evidence_sha256),
        reachability_evidence_sha256=
            getfield(reachability, :evidence_sha256),
    )
end

function _rossel_candidate_reservation(candidate::ROSingularBranchCandidate)
    jacobian = getfield(candidate, :jacobian)
    output = getfield(candidate, :output_at_stratum)
    matrix_elements = jacobian === nothing ? 0 : length(jacobian)
    output_elements = output === nothing ? 0 : length(output)
    return BigInt(8_192 + ncodeunits(getfield(candidate, :branch_id))) +
        BigInt(24) * BigInt(length(getfield(candidate, :source_regime_ids))) +
        BigInt(40) * BigInt(matrix_elements + output_elements + 1) +
        BigInt(300) *
            BigInt(length(getfield(candidate, :numeric_gap_reasons)))
end

function _rossel_numeric_matrix(raw, policy, limits)
    raw === nothing && return nothing
    raw isa AbstractMatrix || throw(ArgumentError(
        "candidate jacobian must be a matrix or nothing"))
    size(raw) == (length(getfield(policy, :output_order)),
        length(getfield(policy, :input_order))) ||
        throw(DimensionMismatch(
            "candidate jacobian must match policy output x input order"))
    _rossel_limit(:matrix_elements, length(raw), limits.max_matrix_elements)
    matrix = Matrix{Float64}(raw)
    all(isfinite, matrix) || throw(ArgumentError(
        "candidate jacobian must be finite"))
    matrix[matrix .== 0.0] .= 0.0
    return matrix
end

function _rossel_numeric_output(raw, policy)
    raw === nothing && return nothing
    (raw isa AbstractVector || raw isa Tuple) || throw(ArgumentError(
        "candidate output_at_stratum must be a vector or nothing"))
    length(raw) == length(getfield(policy, :output_order)) ||
        throw(DimensionMismatch(
        "candidate output must match policy output order"))
    output = Float64.(raw)
    all(isfinite, output) || throw(ArgumentError(
        "candidate output_at_stratum must be finite"))
    output[output .== 0.0] .= 0.0
    return output
end

function _rossel_validate_candidate!(
    candidate::ROSingularBranchCandidate,
    policy::ROSingularSelectionPolicy,
    limits::ROSingularSelectionLimits,
)
    getfield(candidate, :schema_version) ==
        RO_SINGULAR_CANDIDATE_PAYLOAD_VERSION ||
        throw(ArgumentError("unsupported singular candidate version"))
    _rossel_hash(getfield(candidate, :candidate_payload_sha256),
        "candidate_payload_sha256")
    getfield(candidate, :selection_policy_sha256) ==
        getfield(policy, :policy_sha256) ||
        throw(ArgumentError("candidate belongs to a different selection policy"))
    branch_id = getfield(candidate, :branch_id)
    identifier = _rossel_id(branch_id, "branch_id")
    identifier == branch_id || throw(ArgumentError(
        "candidate branch_id is not canonical"))
    getfield(candidate, :model_sha256) == getfield(policy, :model_sha256) &&
        getfield(candidate, :stratum_sha256) ==
            getfield(policy, :stratum_sha256) ||
        throw(ArgumentError("candidate model/stratum differs from policy"))
    source_regime_ids = getfield(candidate, :source_regime_ids)
    regimes = _rossel_regimes(source_regime_ids, limits)
    regimes == source_regime_ids || throw(ArgumentError(
        "candidate source regimes are not canonical"))
    expected_branch = _rossel_sha_payload(
        _rossel_branch_payload(policy, identifier, regimes),
        :branch_identity_bytes, limits.max_candidate_payload_bytes)
    getfield(candidate, :branch_identity_sha256) == expected_branch ||
        throw(ArgumentError(
        "candidate branch identity is not derived from branch content"))
    _rossel_validate_stability!(getfield(candidate, :stability_evidence),
        policy, expected_branch, limits)
    _rossel_validate_reachability!(getfield(candidate, :reachability_evidence),
        policy, expected_branch, limits)
    matrix = _rossel_numeric_matrix(getfield(candidate, :jacobian),
        policy, limits)
    output = _rossel_numeric_output(
        getfield(candidate, :output_at_stratum), policy)
    residual_upper_bound = getfield(candidate, :residual_upper_bound)
    residual = residual_upper_bound === nothing ? nothing :
        _rossel_strict_float64(residual_upper_bound,
            "residual_upper_bound"; nonnegative=true)
    numeric_gap_reasons = getfield(candidate, :numeric_gap_reasons)
    gaps = _rossel_gaps(numeric_gap_reasons,
        "numeric_gap_reasons", limits)
    gaps == numeric_gap_reasons || throw(ArgumentError(
        "candidate numeric gap reasons are not canonical"))
    incomplete_numeric = matrix === nothing || output === nothing ||
        residual === nothing
    incomplete_numeric == !isempty(gaps) || throw(ArgumentError(
        "numeric gap reasons must be present exactly for incomplete numeric evidence"))
    _rossel_limit(:candidate_payload_reservation,
        _rossel_candidate_reservation(candidate),
        limits.max_candidate_payload_bytes)
    expected_payload = _rossel_sha_payload(_rossel_candidate_payload(candidate),
        :candidate_payload_bytes, limits.max_candidate_payload_bytes)
    getfield(candidate, :candidate_payload_sha256) == expected_payload ||
        throw(ArgumentError("candidate payload self-hash mismatch"))
    return candidate
end

function ROSingularBranchCandidate(;
    policy::ROSingularSelectionPolicy,
    branch_id,
    source_regime_ids,
    stability_evidence::ROSingularStabilityEvidence,
    reachability_evidence::ROSingularReachabilityEvidence,
    jacobian=nothing,
    output_at_stratum=nothing,
    residual_upper_bound=nothing,
    numeric_gap_reasons=Symbol[],
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    _rossel_validate_policy!(policy, limits)
    identifier = _rossel_id(branch_id, "branch_id")
    regimes = _rossel_regimes(source_regime_ids, limits)
    branch_hash = ro_singular_branch_identity_sha256(
        policy=policy, branch_id=identifier, source_regime_ids=regimes,
        limits=limits)
    _rossel_validate_stability!(
        stability_evidence, policy, branch_hash, limits)
    _rossel_validate_reachability!(
        reachability_evidence, policy, branch_hash, limits)
    matrix = _rossel_numeric_matrix(jacobian, policy, limits)
    output = _rossel_numeric_output(output_at_stratum, policy)
    residual = residual_upper_bound === nothing ? nothing :
        _rossel_strict_float64(residual_upper_bound,
            "residual_upper_bound"; nonnegative=true)
    gaps = _rossel_gaps(numeric_gap_reasons,
        "numeric_gap_reasons", limits)
    incomplete_numeric = matrix === nothing || output === nothing ||
        residual === nothing
    incomplete_numeric == !isempty(gaps) || throw(ArgumentError(
        "numeric gap reasons must be present exactly for incomplete numeric evidence"))
    prototype = ROSingularBranchCandidate(_ROSSEL_VALIDATED,
        RO_SINGULAR_CANDIDATE_PAYLOAD_VERSION, repeat("0", 64),
        getfield(policy, :policy_sha256), identifier, branch_hash,
        getfield(policy, :model_sha256), getfield(policy, :stratum_sha256),
        regimes, matrix, output,
        residual, gaps, stability_evidence, reachability_evidence)
    _rossel_limit(:candidate_payload_reservation,
        _rossel_candidate_reservation(prototype),
        limits.max_candidate_payload_bytes)
    payload_hash = _rossel_sha_payload(_rossel_candidate_payload(prototype),
        :candidate_payload_bytes, limits.max_candidate_payload_bytes)
    candidate = ROSingularBranchCandidate(_ROSSEL_VALIDATED,
        getfield(prototype, :schema_version), payload_hash,
        getfield(prototype, :selection_policy_sha256),
        getfield(prototype, :branch_id),
        getfield(prototype, :branch_identity_sha256),
        getfield(prototype, :model_sha256),
        getfield(prototype, :stratum_sha256),
        copy(getfield(prototype, :source_regime_ids)),
        getfield(prototype, :jacobian) === nothing ? nothing :
            copy(getfield(prototype, :jacobian)),
        getfield(prototype, :output_at_stratum) === nothing ? nothing :
            copy(getfield(prototype, :output_at_stratum)),
        getfield(prototype, :residual_upper_bound),
        copy(getfield(prototype, :numeric_gap_reasons)),
        getfield(prototype, :stability_evidence),
        getfield(prototype, :reachability_evidence))
    return _rossel_validate_candidate!(candidate, policy, limits)
end

struct ROSingularCandidateRoot
    branch_identity_sha256::String
    candidate_payload_sha256::String
    function ROSingularCandidateRoot(branch_identity_sha256,
                                     candidate_payload_sha256,
                                     ::_ROSSelValidatedToken)
        return new(branch_identity_sha256, candidate_payload_sha256)
    end
end

_rossel_root_payload(root::ROSingularCandidateRoot) = (
    branch_identity_sha256=getfield(root, :branch_identity_sha256),
    candidate_payload_sha256=getfield(root, :candidate_payload_sha256),
)

struct ROSingularCandidatePopulationReceipt
    schema_version::String
    receipt_sha256::String
    branch_enumerator_sha256::String
    enumeration_scope_sha256::String
    model_sha256::String
    stratum_sha256::String
    expected_candidate_count::Int
    observed_candidate_count::Int
    complete::Bool
    candidate_roots::Vector{ROSingularCandidateRoot}
    gap_reasons::Vector{Symbol}
    evidence_scope::Symbol
    universal_completeness_claimed::Bool
    function ROSingularCandidatePopulationReceipt(
        ::_ROSSelValidatedToken, args...)
        return new(args...)
    end
end

function _rossel_receipt_payload(receipt::ROSingularCandidatePopulationReceipt)
    return (
        schema_version=getfield(receipt, :schema_version),
        branch_enumerator_sha256=
            getfield(receipt, :branch_enumerator_sha256),
        enumeration_scope_sha256=
            getfield(receipt, :enumeration_scope_sha256),
        model_sha256=getfield(receipt, :model_sha256),
        stratum_sha256=getfield(receipt, :stratum_sha256),
        expected_candidate_count=
            getfield(receipt, :expected_candidate_count),
        observed_candidate_count=
            getfield(receipt, :observed_candidate_count),
        complete=getfield(receipt, :complete),
        candidate_roots=_rossel_root_payload.(
            getfield(receipt, :candidate_roots)),
        gap_reasons=String.(getfield(receipt, :gap_reasons)),
        evidence_scope=String(getfield(receipt, :evidence_scope)),
        universal_completeness_claimed=
            getfield(receipt, :universal_completeness_claimed),
    )
end

function _rossel_assert_policy_unchanged(policy::ROSingularSelectionPolicy)
    expected = _rossel_sha_payload(_rossel_policy_payload(policy),
        :policy_identity_bytes,
        _ROSSEL_CERTIFICATE_HARD_LIMITS.max_identity_bytes)
    expected == getfield(policy, :policy_sha256) || throw(ArgumentError(
        "singular selection policy changed after construction"))
    return nothing
end

function _rossel_assert_stability_unchanged(
    evidence::ROSingularStabilityEvidence,
)
    expected = _rossel_sha_payload(_rossel_stability_payload(evidence),
        :stability_evidence_bytes,
        _ROSSEL_CERTIFICATE_HARD_LIMITS.max_candidate_payload_bytes)
    expected == getfield(evidence, :evidence_sha256) ||
        throw(ArgumentError(
            "singular stability evidence changed after construction"))
    return nothing
end

function _rossel_assert_reachability_unchanged(
    evidence::ROSingularReachabilityEvidence,
)
    expected = _rossel_sha_payload(_rossel_reachability_payload(evidence),
        :reachability_evidence_bytes,
        _ROSSEL_CERTIFICATE_HARD_LIMITS.max_candidate_payload_bytes)
    expected == getfield(evidence, :evidence_sha256) ||
        throw(ArgumentError(
            "singular reachability evidence changed after construction"))
    return nothing
end

function _rossel_assert_candidate_unchanged(
    candidate::ROSingularBranchCandidate,
)
    _rossel_assert_stability_unchanged(
        getfield(candidate, :stability_evidence))
    _rossel_assert_reachability_unchanged(
        getfield(candidate, :reachability_evidence))
    expected = _rossel_sha_payload(_rossel_candidate_payload(candidate),
        :candidate_payload_bytes,
        _ROSSEL_CERTIFICATE_HARD_LIMITS.max_candidate_payload_bytes)
    expected == getfield(candidate, :candidate_payload_sha256) ||
        throw(ArgumentError(
            "singular branch candidate changed after construction"))
    return nothing
end

function _rossel_assert_receipt_unchanged(
    receipt::ROSingularCandidatePopulationReceipt,
)
    expected = _rossel_sha_payload(_rossel_receipt_payload(receipt),
        :population_receipt_bytes,
        _ROSSEL_CERTIFICATE_HARD_LIMITS.max_identity_bytes)
    expected == getfield(receipt, :receipt_sha256) || throw(ArgumentError(
        "singular candidate population receipt changed after construction"))
    return nothing
end

function Base.getproperty(policy::ROSingularSelectionPolicy, name::Symbol)
    _rossel_assert_policy_unchanged(policy)
    value = getfield(policy, name)
    if name in (:input_order, :input_units, :output_order, :output_units)
        return copy(value)
    end
    return value
end


function Base.getproperty(evidence::ROSingularStabilityEvidence, name::Symbol)
    _rossel_assert_stability_unchanged(evidence)
    value = getfield(evidence, name)
    return name == :gap_reasons ? copy(value) : value
end

function Base.getproperty(
    evidence::ROSingularReachabilityEvidence,
    name::Symbol,
)
    _rossel_assert_reachability_unchanged(evidence)
    value = getfield(evidence, name)
    return name == :gap_reasons ? copy(value) : value
end

function Base.getproperty(candidate::ROSingularBranchCandidate, name::Symbol)
    _rossel_assert_candidate_unchanged(candidate)
    value = getfield(candidate, name)
    if name in (:source_regime_ids, :jacobian, :output_at_stratum,
            :numeric_gap_reasons)
        return value === nothing ? nothing : copy(value)
    end
    return value
end

function Base.getproperty(
    receipt::ROSingularCandidatePopulationReceipt,
    name::Symbol,
)
    _rossel_assert_receipt_unchanged(receipt)
    value = getfield(receipt, name)
    if name in (:candidate_roots, :gap_reasons)
        return copy(value)
    end
    return value
end

function _rossel_sorted_candidates(raw_candidates, policy, limits, cancel_check)
    (raw_candidates isa AbstractVector || raw_candidates isa Tuple) ||
        throw(ArgumentError("candidates must be a finite ordered collection"))
    _rossel_limit(:candidates, length(raw_candidates), limits.max_candidates)
    cancel_check()
    candidates = ROSingularBranchCandidate[
        candidate for candidate in raw_candidates]
    branch_ids = String[]
    branch_hashes = String[]
    payload_hashes = String[]
    matrix_elements = BigInt(0)
    for candidate in candidates
        cancel_check()
        _rossel_validate_candidate!(candidate, policy, limits)
        push!(branch_ids, getfield(candidate, :branch_id))
        push!(branch_hashes, getfield(candidate, :branch_identity_sha256))
        push!(payload_hashes, getfield(candidate, :candidate_payload_sha256))
        jacobian = getfield(candidate, :jacobian)
        matrix_elements += jacobian === nothing ? BigInt(0) :
            BigInt(length(jacobian))
        _rossel_limit(:matrix_elements, matrix_elements,
            limits.max_matrix_elements)
    end
    allunique(branch_ids) || throw(ArgumentError(
        "candidate branch_id values must be unique"))
    allunique(branch_hashes) || throw(ArgumentError(
        "candidate branch identities must be unique"))
    allunique(payload_hashes) || throw(ArgumentError(
        "candidate payload identities must be unique"))
    cancel_check()
    sort!(candidates; by=candidate -> (
        getfield(candidate, :branch_identity_sha256),
        getfield(candidate, :candidate_payload_sha256),
        getfield(candidate, :branch_id),
    ))
    cancel_check()
    return candidates
end

function _rossel_receipt_reservation(root_count::Integer, gap_count::Integer)
    return BigInt(8_192) + BigInt(160) * BigInt(root_count) +
        BigInt(300) * BigInt(gap_count)
end

function _rossel_receipt_roots(candidates, cancel_check)
    cancel_check()
    roots = ROSingularCandidateRoot[]
    sizehint!(roots, length(candidates))
    for candidate in candidates
        cancel_check()
        push!(roots, ROSingularCandidateRoot(
            getfield(candidate, :branch_identity_sha256),
            getfield(candidate, :candidate_payload_sha256),
            _ROSSEL_VALIDATED,
        ))
    end
    cancel_check()
    return roots
end

function _rossel_validate_receipt!(
    receipt::ROSingularCandidatePopulationReceipt,
    policy::ROSingularSelectionPolicy,
    candidates,
    limits::ROSingularSelectionLimits,
    cancel_check,
)
    getfield(receipt, :schema_version) ==
        RO_SINGULAR_POPULATION_RECEIPT_VERSION ||
        throw(ArgumentError("unsupported candidate population receipt version"))
    _rossel_hash(getfield(receipt, :receipt_sha256),
        "population_receipt_sha256")
    getfield(receipt, :branch_enumerator_sha256) ==
        getfield(policy, :branch_enumerator_sha256) &&
        getfield(receipt, :enumeration_scope_sha256) ==
            getfield(policy, :enumeration_scope_sha256) &&
        getfield(receipt, :model_sha256) == getfield(policy, :model_sha256) &&
        getfield(receipt, :stratum_sha256) ==
            getfield(policy, :stratum_sha256) || throw(ArgumentError(
            "candidate population receipt does not match selection policy"))
    expected_candidate_count = getfield(receipt, :expected_candidate_count)
    observed_candidate_count = getfield(receipt, :observed_candidate_count)
    candidate_roots = getfield(receipt, :candidate_roots)
    gap_reasons = getfield(receipt, :gap_reasons)
    complete = getfield(receipt, :complete)
    0 <= expected_candidate_count <= limits.max_candidates ||
        throw(ArgumentError("receipt expected candidate count is outside limits"))
    observed_candidate_count == length(candidates) ||
        throw(ArgumentError("receipt observed count differs from candidates"))
    _rossel_limit(:receipt_candidate_roots,
        length(candidate_roots), limits.max_candidates)
    _rossel_limit(:gap_reasons,
        length(gap_reasons), limits.max_gap_reasons)
    _rossel_limit(:population_receipt_reservation,
        _rossel_receipt_reservation(
            length(candidate_roots), length(gap_reasons)),
        limits.max_identity_bytes)
    cancel_check()
    roots = _rossel_receipt_roots(candidates, cancel_check)
    length(candidate_roots) == length(roots) ||
        throw(ArgumentError("receipt candidate roots are incomplete"))
    for index in eachindex(roots)
        cancel_check()
        supplied = candidate_roots[index]
        _rossel_hash(getfield(supplied, :branch_identity_sha256),
            "receipt.candidate_roots[].branch_identity_sha256")
        _rossel_hash(getfield(supplied, :candidate_payload_sha256),
            "receipt.candidate_roots[].candidate_payload_sha256")
        getfield(supplied, :branch_identity_sha256) ==
            getfield(roots[index], :branch_identity_sha256) &&
            getfield(supplied, :candidate_payload_sha256) ==
                getfield(roots[index], :candidate_payload_sha256) ||
                throw(ArgumentError(
                    "receipt candidate roots differ from sorted candidate content"))
    end
    gaps = _rossel_gaps(gap_reasons,
        "population_receipt.gap_reasons", limits)
    gaps == gap_reasons || throw(ArgumentError(
        "population receipt gap reasons are not canonical"))
    if complete
        expected_candidate_count == length(candidates) ||
            throw(ArgumentError(
                "complete receipt expected count must equal candidate count"))
        isempty(gaps) || throw(ArgumentError(
            "complete candidate population receipt cannot carry gaps"))
    else
        expected_candidate_count >= length(candidates) ||
            throw(ArgumentError(
                "incomplete receipt expected count cannot understate observations"))
        isempty(gaps) && throw(ArgumentError(
            "incomplete candidate population receipt requires gap reasons"))
    end
    getfield(receipt, :evidence_scope) == _ROSSEL_POPULATION_SCOPE &&
        !getfield(receipt, :universal_completeness_claimed) ||
        throw(ArgumentError(
            "population receipt exceeds declared bounded enumerator scope"))
    cancel_check()
    expected_hash = _rossel_sha_payload(_rossel_receipt_payload(receipt),
        :population_receipt_bytes, limits.max_identity_bytes)
    getfield(receipt, :receipt_sha256) == expected_hash || throw(ArgumentError(
        "candidate population receipt self-hash mismatch"))
    cancel_check()
    return receipt
end

function build_ro_singular_candidate_population_receipt(
    raw_candidates;
    policy::ROSingularSelectionPolicy,
    expected_candidate_count,
    complete::Bool,
    gap_reasons=Symbol[],
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
    cancel_check=() -> nothing,
)
    cancel_check()
    _rossel_validate_policy!(policy, limits)
    cancel_check()
    candidates = _rossel_sorted_candidates(
        raw_candidates, policy, limits, cancel_check)
    (expected_candidate_count isa Integer &&
        !(expected_candidate_count isa Bool)) || throw(ArgumentError(
            "expected_candidate_count must be an integer"))
    expected = try
        Int(expected_candidate_count)
    catch
        throw(ArgumentError("expected_candidate_count must fit in Int"))
    end
    0 <= expected <= limits.max_candidates || throw(ArgumentError(
        "expected_candidate_count is outside candidate limits"))
    gaps = _rossel_gaps(gap_reasons,
        "population_receipt.gap_reasons", limits)
    if complete
        expected == length(candidates) || throw(ArgumentError(
            "complete receipt expected count must equal candidate count"))
        isempty(gaps) || throw(ArgumentError(
            "complete receipt cannot carry gap reasons"))
    else
        expected >= length(candidates) || throw(ArgumentError(
            "incomplete receipt expected count cannot understate observations"))
        isempty(gaps) && throw(ArgumentError(
            "incomplete receipt requires gap reasons"))
    end
    _rossel_limit(:population_receipt_reservation,
        _rossel_receipt_reservation(length(candidates), length(gaps)),
        limits.max_identity_bytes)
    cancel_check()
    roots = _rossel_receipt_roots(candidates, cancel_check)
    prototype = ROSingularCandidatePopulationReceipt(_ROSSEL_VALIDATED,
        RO_SINGULAR_POPULATION_RECEIPT_VERSION, repeat("0", 64),
        getfield(policy, :branch_enumerator_sha256),
        getfield(policy, :enumeration_scope_sha256),
        getfield(policy, :model_sha256), getfield(policy, :stratum_sha256), expected,
        length(candidates), complete, roots, gaps,
        _ROSSEL_POPULATION_SCOPE, false)
    cancel_check()
    receipt_hash = _rossel_sha_payload(_rossel_receipt_payload(prototype),
        :population_receipt_bytes, limits.max_identity_bytes)
    receipt = ROSingularCandidatePopulationReceipt(_ROSSEL_VALIDATED,
        getfield(prototype, :schema_version), receipt_hash,
        getfield(prototype, :branch_enumerator_sha256),
        getfield(prototype, :enumeration_scope_sha256),
        getfield(prototype, :model_sha256),
        getfield(prototype, :stratum_sha256),
        getfield(prototype, :expected_candidate_count),
        getfield(prototype, :observed_candidate_count),
        getfield(prototype, :complete),
        copy(getfield(prototype, :candidate_roots)),
        copy(getfield(prototype, :gap_reasons)),
        getfield(prototype, :evidence_scope),
        getfield(prototype, :universal_completeness_claimed))
    cancel_check()
    return _rossel_validate_receipt!(
        receipt, policy, candidates, limits, cancel_check)
end

struct ROSingularSelectionCertificate
    schema_version::String
    identity_sha256::String
    selection_authority_sha256::String
    population_receipt_sha256::String
    status::Symbol
    candidate_population_complete::Bool
    candidate_count::Int
    admissible_branch_ids::Vector{String}
    selected_branch_id::Union{Nothing,String}
    selected_branch_identity_sha256::Union{Nothing,String}
    selected_candidate_payload_sha256::Union{Nothing,String}
    selected_jacobian::Union{Nothing,Matrix{Float64}}
    selected_output_at_stratum::Union{Nothing,Vector{Float64}}
    selected_stability_evidence_sha256::Union{Nothing,String}
    selected_stability_analysis_sha256::Union{Nothing,String}
    selected_reachability_evidence_sha256::Union{Nothing,String}
    selected_dynamic_trace_sha256::Union{Nothing,String}
    reason_codes::Vector{Symbol}
    evidence_scope::Symbol
    includes_singular_branch::Bool
    regular_limit_only::Bool
    universal_selection_claimed::Bool
    causal_claimed::Bool
    experimentally_validated::Bool
    function ROSingularSelectionCertificate(::_ROSSelValidatedToken, args...)
        certificate = new(args...)
        _rossel_validate_certificate_state!(
            certificate, _ROSSEL_CERTIFICATE_HARD_LIMITS)
        return certificate
    end
end

function _rossel_certificate_result_snapshot(certificate)
    return (
        population_receipt_sha256=
            getfield(certificate, :population_receipt_sha256),
        status=String(getfield(certificate, :status)),
        candidate_population_complete=
            getfield(certificate, :candidate_population_complete),
        candidate_count=getfield(certificate, :candidate_count),
        admissible_branch_ids=Tuple(
            getfield(certificate, :admissible_branch_ids)),
        selected_branch_id=getfield(certificate, :selected_branch_id),
        selected_branch_identity_sha256=
            getfield(certificate, :selected_branch_identity_sha256),
        selected_candidate_payload_sha256=
            getfield(certificate, :selected_candidate_payload_sha256),
        selected_jacobian=_rossel_matrix_payload(
            getfield(certificate, :selected_jacobian)),
        selected_output_at_stratum=
            getfield(certificate, :selected_output_at_stratum),
        selected_stability_evidence_sha256=
            getfield(certificate, :selected_stability_evidence_sha256),
        selected_stability_analysis_sha256=
            getfield(certificate, :selected_stability_analysis_sha256),
        selected_reachability_evidence_sha256=
            getfield(certificate, :selected_reachability_evidence_sha256),
        selected_dynamic_trace_sha256=
            getfield(certificate, :selected_dynamic_trace_sha256),
        reason_codes=Tuple(String.(getfield(certificate, :reason_codes))),
        evidence_scope=String(getfield(certificate, :evidence_scope)),
        includes_singular_branch=
            getfield(certificate, :includes_singular_branch),
        regular_limit_only=getfield(certificate, :regular_limit_only),
        universal_selection_claimed=
            getfield(certificate, :universal_selection_claimed),
        causal_claimed=getfield(certificate, :causal_claimed),
        experimentally_validated=
            getfield(certificate, :experimentally_validated),
    )
end

function _rossel_certificate_identity_payload(
    selection_authority_sha256,
    certificate,
)
    return (
        schema_version=getfield(certificate, :schema_version),
        selection_authority_sha256=selection_authority_sha256,
        result_snapshot=_rossel_certificate_result_snapshot(certificate),
    )
end

function _rossel_certificate_content_sha256(
    selection_authority_sha256,
    certificate::ROSingularSelectionCertificate,
)
    document = JSON3.write(_rossel_certificate_identity_payload(
        selection_authority_sha256, certificate))
    return bytes2hex(SHA.sha256(codeunits(document)))
end

function _rossel_validate_certificate_state!(
    certificate::ROSingularSelectionCertificate,
    limits::ROSingularSelectionLimits,
)
    getfield(certificate, :schema_version) ==
        RO_SINGULAR_SELECTION_VERSION || throw(ArgumentError(
            "unsupported singular selection certificate version"))
    identity = _rossel_hash(
        getfield(certificate, :identity_sha256), "identity_sha256")
    authority = _rossel_hash(
        getfield(certificate, :selection_authority_sha256),
        "selection_authority_sha256")
    _rossel_hash(getfield(certificate, :population_receipt_sha256),
        "population_receipt_sha256")

    candidate_count = getfield(certificate, :candidate_count)
    0 <= candidate_count <= limits.max_candidates || throw(ArgumentError(
        "certificate candidate_count is outside the configured limit"))
    admissible_ids = getfield(certificate, :admissible_branch_ids)
    length(admissible_ids) <= candidate_count || throw(ArgumentError(
        "certificate admissible branches exceed the candidate count"))
    for branch_id in admissible_ids
        _rossel_id(branch_id, "admissible_branch_ids[]")
    end
    allunique(admissible_ids) &&
        admissible_ids == sort(copy(admissible_ids)) || throw(ArgumentError(
            "certificate admissible branch ids are not canonical"))
    reasons = _rossel_gaps(
        getfield(certificate, :reason_codes),
        "certificate.reason_codes", limits)
    reasons == getfield(certificate, :reason_codes) || throw(ArgumentError(
        "certificate reason codes are not canonical"))

    selected_fields = (
        getfield(certificate, :selected_branch_id),
        getfield(certificate, :selected_branch_identity_sha256),
        getfield(certificate, :selected_candidate_payload_sha256),
        getfield(certificate, :selected_jacobian),
        getfield(certificate, :selected_output_at_stratum),
        getfield(certificate, :selected_stability_evidence_sha256),
        getfield(certificate, :selected_stability_analysis_sha256),
        getfield(certificate, :selected_reachability_evidence_sha256),
        getfield(certificate, :selected_dynamic_trace_sha256),
    )
    selected_presence = map(value -> value !== nothing, selected_fields)
    selected = all(selected_presence)
    selected || all(!, selected_presence) || throw(ArgumentError(
        "certificate selected-branch fields must be all present or all absent"))
    if selected
        branch_id = getfield(certificate, :selected_branch_id)
        _rossel_id(branch_id, "selected_branch_id")
        branch_id in admissible_ids || throw(ArgumentError(
            "selected branch is not in the admissible branch set"))
        for field in (
            :selected_branch_identity_sha256,
            :selected_candidate_payload_sha256,
            :selected_stability_evidence_sha256,
            :selected_stability_analysis_sha256,
            :selected_reachability_evidence_sha256,
            :selected_dynamic_trace_sha256,
        )
            _rossel_hash(getfield(certificate, field), String(field))
        end
        jacobian = getfield(certificate, :selected_jacobian)
        output = getfield(certificate, :selected_output_at_stratum)
        rows, columns = size(jacobian)
        1 <= rows <= limits.max_outputs &&
            1 <= columns <= limits.max_inputs || throw(ArgumentError(
                "selected Jacobian shape is outside the configured limits"))
        _rossel_limit(:certificate_matrix_elements,
            BigInt(rows) * BigInt(columns), limits.max_matrix_elements)
        rows == length(output) || throw(DimensionMismatch(
            "selected output length must equal the Jacobian row count"))
        all(isfinite, jacobian) && all(isfinite, output) ||
            throw(ArgumentError(
                "selected certificate arrays must be finite"))
    end

    getfield(certificate, :evidence_scope) ==
        RO_SINGULAR_SELECTION_SCOPE || throw(ArgumentError(
            "singular selection certificate evidence scope changed"))
    !getfield(certificate, :universal_selection_claimed) &&
        !getfield(certificate, :causal_claimed) &&
        !getfield(certificate, :experimentally_validated) ||
        throw(ArgumentError(
            "singular selection certificate exceeds its evidence scope"))
    includes = getfield(certificate, :includes_singular_branch)
    regular_only = getfield(certificate, :regular_limit_only)
    includes != regular_only || throw(ArgumentError(
        "singular and regular-only result flags are inconsistent"))

    status = getfield(certificate, :status)
    complete = getfield(certificate, :candidate_population_complete)
    if status == :unknown_incomplete_population
        !complete && isempty(admissible_ids) && !selected &&
            :candidate_population_incomplete in reasons &&
            !includes && regular_only || throw(ArgumentError(
                "incomplete-population certificate state is inconsistent"))
    elseif status == :unknown_gap
        complete && candidate_count >= 1 && isempty(admissible_ids) &&
            !selected && :candidate_evidence_gap in reasons &&
            !includes && regular_only || throw(ArgumentError(
                "unknown-gap certificate state is inconsistent"))
    elseif status == :selected_unique_branch_under_declared_policy
        complete && candidate_count >= 1 && length(admissible_ids) == 1 &&
            selected &&
            getfield(certificate, :selected_branch_id) == only(admissible_ids) &&
            isempty(reasons) && includes && !regular_only ||
            throw(ArgumentError(
                "unique-selection certificate state is inconsistent"))
    elseif status == :no_admissible_candidate_in_declared_population
        complete && isempty(admissible_ids) && !selected &&
            reasons == [:no_candidate_satisfied_declared_policy] &&
            !includes && regular_only || throw(ArgumentError(
                "no-admissible-candidate certificate state is inconsistent"))
    elseif status == :set_valued_multiple_admissible_branches
        complete && candidate_count >= 2 && length(admissible_ids) >= 2 &&
            !selected &&
            reasons == [:multiple_candidates_satisfied_declared_policy] &&
            includes && !regular_only || throw(ArgumentError(
                "set-valued certificate state is inconsistent"))
    else
        throw(ArgumentError("unsupported singular selection status"))
    end

    expected = _rossel_sha_payload(
        _rossel_certificate_identity_payload(authority, certificate),
        :selection_identity_bytes, limits.max_identity_bytes)
    identity == expected || throw(ArgumentError(
        "singular selection certificate content or identity is invalid"))
    return certificate
end

function _rossel_assert_certificate_unchanged(
    certificate::ROSingularSelectionCertificate,
)
    _rossel_validate_certificate_state!(
        certificate, _ROSSEL_CERTIFICATE_HARD_LIMITS)
    return nothing
end

"""
    validate_ro_singular_selection_certificate(certificate; limits)

Validate both the content identity and the complete local status/field state
machine of a singular-selection certificate.  This does not replay the
population receipt or the evidence artifacts bound by their hashes.
"""
function validate_ro_singular_selection_certificate(
    certificate::ROSingularSelectionCertificate;
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
)
    _rossel_validate_certificate_state!(certificate, limits)
    return true
end

function Base.getproperty(
    certificate::ROSingularSelectionCertificate,
    name::Symbol,
)
    _rossel_assert_certificate_unchanged(certificate)
    value = getfield(certificate, name)
    if name in (
        :admissible_branch_ids,
        :selected_jacobian,
        :selected_output_at_stratum,
        :reason_codes,
    )
        return value === nothing ? nothing : copy(value)
    end
    return value
end

function _rossel_selection_identity(policy, receipt, limits, cancel_check)
    reservation = BigInt(4_096) +
        BigInt(160) *
            BigInt(length(getfield(receipt, :candidate_roots)))
    _rossel_limit(:selection_identity_reservation,
        reservation, limits.max_identity_bytes)
    cancel_check()
    payload = (
        schema_version=RO_SINGULAR_SELECTION_VERSION,
        policy_sha256=getfield(policy, :policy_sha256),
        population_receipt_sha256=getfield(receipt, :receipt_sha256),
        candidate_roots=_rossel_root_payload.(
            getfield(receipt, :candidate_roots)),
    )
    result = _rossel_sha_payload(payload,
        :selection_identity_bytes, limits.max_identity_bytes)
    cancel_check()
    return result
end

function _rossel_certificate(selection_authority_hash, receipt, status, candidates,
                             admissible_ids, reasons;
                             selected=nothing,
                             includes_singular_branch::Bool=false,
                             regular_limit_only::Bool=true,
                             limits::ROSingularSelectionLimits=
                                 ROSingularSelectionLimits(),
                             cancel_check=() -> nothing)
    _rossel_hash(selection_authority_hash, "selection_authority_sha256")
    fields = (
        schema_version=RO_SINGULAR_SELECTION_VERSION,
        selection_authority_sha256=String(selection_authority_hash),
        population_receipt_sha256=getfield(receipt, :receipt_sha256),
        status=Symbol(status),
        candidate_population_complete=getfield(receipt, :complete),
        candidate_count=length(candidates),
        admissible_branch_ids=copy(admissible_ids),
        selected_branch_id=
            selected === nothing ? nothing : getfield(selected, :branch_id),
        selected_branch_identity_sha256=selected === nothing ? nothing :
            getfield(selected, :branch_identity_sha256),
        selected_candidate_payload_sha256=selected === nothing ? nothing :
            getfield(selected, :candidate_payload_sha256),
        selected_jacobian=
            selected === nothing ? nothing : copy(getfield(selected, :jacobian)),
        selected_output_at_stratum=selected === nothing ? nothing :
            copy(getfield(selected, :output_at_stratum)),
        selected_stability_evidence_sha256=selected === nothing ? nothing :
            getfield(getfield(selected, :stability_evidence), :evidence_sha256),
        selected_stability_analysis_sha256=selected === nothing ? nothing :
            getfield(getfield(selected, :stability_evidence), :analysis_sha256),
        selected_reachability_evidence_sha256=selected === nothing ? nothing :
            getfield(getfield(selected, :reachability_evidence), :evidence_sha256),
        selected_dynamic_trace_sha256=selected === nothing ? nothing :
            getfield(getfield(selected, :reachability_evidence),
                :dynamic_trace_sha256),
        reason_codes=copy(reasons),
        evidence_scope=RO_SINGULAR_SELECTION_SCOPE,
        includes_singular_branch=includes_singular_branch,
        regular_limit_only=regular_limit_only,
        universal_selection_claimed=false,
        causal_claimed=false,
        experimentally_validated=false,
    )
    cancel_check()
    certificate_hash = _rossel_sha_payload(
        _rossel_certificate_identity_payload(
            selection_authority_hash, fields),
        :selection_identity_bytes,
        limits.max_identity_bytes,
    )
    cancel_check()
    certificate = ROSingularSelectionCertificate(_ROSSEL_VALIDATED,
        fields.schema_version, certificate_hash,
        fields.selection_authority_sha256,
        fields.population_receipt_sha256, fields.status,
        fields.candidate_population_complete, fields.candidate_count,
        fields.admissible_branch_ids, fields.selected_branch_id,
        fields.selected_branch_identity_sha256,
        fields.selected_candidate_payload_sha256,
        fields.selected_jacobian, fields.selected_output_at_stratum,
        fields.selected_stability_evidence_sha256,
        fields.selected_stability_analysis_sha256,
        fields.selected_reachability_evidence_sha256,
        fields.selected_dynamic_trace_sha256, fields.reason_codes,
        fields.evidence_scope, fields.includes_singular_branch,
        fields.regular_limit_only, fields.universal_selection_claimed,
        fields.causal_claimed, fields.experimentally_validated)
    _rossel_validate_certificate_state!(certificate, limits)
    return certificate
end

function _rossel_candidate_gaps(candidate, cancel_check)
    stability = getfield(candidate, :stability_evidence)
    reachability = getfield(candidate, :reachability_evidence)
    gaps = unique(Symbol[
        getfield(candidate, :numeric_gap_reasons);
        getfield(stability, :gap_reasons);
        getfield(reachability, :gap_reasons);
    ])
    cancel_check()
    return sort!(gaps; by=String)
end

function certify_ro_singular_branch_selection(
    raw_candidates;
    policy::ROSingularSelectionPolicy,
    population_receipt::ROSingularCandidatePopulationReceipt,
    limits::ROSingularSelectionLimits=ROSingularSelectionLimits(),
    cancel_check=() -> nothing,
)
    cancel_check()
    _rossel_validate_policy!(policy, limits)
    cancel_check()
    candidates = _rossel_sorted_candidates(
        raw_candidates, policy, limits, cancel_check)
    cancel_check()
    _rossel_validate_receipt!(
        population_receipt, policy, candidates, limits, cancel_check)
    identity_hash = _rossel_selection_identity(
        policy, population_receipt, limits, cancel_check)

    if !getfield(population_receipt, :complete)
        cancel_check()
        reasons = sort!(unique(Symbol[
            :candidate_population_incomplete;
            getfield(population_receipt, :gap_reasons);
        ]); by=String)
        return _rossel_certificate(identity_hash, population_receipt,
            :unknown_incomplete_population, candidates, String[], reasons;
            limits, cancel_check)
    end

    cancel_check()
    unknown_candidates = filter(candidates) do candidate
        !isempty(_rossel_candidate_gaps(candidate, cancel_check))
    end
    cancel_check()
    if !isempty(unknown_candidates)
        reasons = Symbol[:candidate_evidence_gap]
        for candidate in unknown_candidates
            cancel_check()
            append!(reasons, _rossel_candidate_gaps(candidate, cancel_check))
        end
        cancel_check()
        sort!(unique!(reasons); by=String)
        cancel_check()
        return _rossel_certificate(identity_hash, population_receipt,
            :unknown_gap, candidates, String[], reasons;
            limits, cancel_check)
    end

    cancel_check()
    admissible = filter(candidates) do candidate
        getfield(candidate, :residual_upper_bound) <=
            getfield(policy, :residual_absolute_tolerance) &&
            getfield(getfield(candidate, :stability_evidence), :status) ==
                :locally_stable &&
            getfield(getfield(candidate, :reachability_evidence), :status) ==
                :reached_under_protocol
    end
    cancel_check()
    admissible_ids = getfield.(admissible, :branch_id)
    cancel_check()
    sort!(admissible_ids)
    cancel_check()
    if length(admissible) == 1
        selected = only(admissible)
        cancel_check()
        return _rossel_certificate(identity_hash, population_receipt,
            :selected_unique_branch_under_declared_policy,
            candidates, admissible_ids, Symbol[];
            selected=selected, includes_singular_branch=true,
            regular_limit_only=false, limits, cancel_check)
    elseif isempty(admissible)
        cancel_check()
        return _rossel_certificate(identity_hash, population_receipt,
            :no_admissible_candidate_in_declared_population,
            candidates, String[],
            [:no_candidate_satisfied_declared_policy]; limits, cancel_check)
    end
    cancel_check()
    return _rossel_certificate(identity_hash, population_receipt,
        :set_valued_multiple_admissible_branches,
        candidates, admissible_ids,
        [:multiple_candidates_satisfied_declared_policy];
        includes_singular_branch=true, regular_limit_only=false,
        limits, cancel_check)
end
