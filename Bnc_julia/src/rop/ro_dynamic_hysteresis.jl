const RO_DYNAMIC_PROTOCOL_TRACE_VERSION =
    "bne-ro-dynamic-protocol-trace/v3.0.0"
const RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION =
    "bne-ro-dynamic-hysteresis-analysis/v3.0.0"
const RO_STATIC_MULTIROOT_EVIDENCE_VERSION =
    "bne-ro-static-multiroot-evidence/v2.0.0"
const RO_DYNAMIC_BRANCH_EVIDENCE_VERSION =
    "bne-ro-dynamic-branch-evidence/v3.0.0"
const RO_POLYNOMIAL_VECTOR_FIELD_VERSION =
    "bne-ro-polynomial-vector-field/v2.0.0"
const RO_DYNAMIC_HYSTERESIS_EVIDENCE_SCOPE =
    :finite_supplied_protocol_and_conditional_equilibrium_evidence_only
const RO_DYNAMIC_HYSTERESIS_ANALYZER_POLICY_SHA256 =
    "sha256:" * bytes2hex(SHA.sha256(codeunits(
        "rodh-v2.1-declarative-polynomial-conditional-equilibrium-loop-" *
        "separate-model-trajectory-policy-residual-and-numerical-agreement-" *
        "dynamic-reachability-gap")))

const _RODH_SOLVER_STATUSES = (
    :success,
    :failure,
    :maxiters,
    :cancelled,
    :exception,
    :unknown,
)
const _RODH_STABILITY_STATUSES = (:stable, :unstable, :unknown)
const _RODH_RESIDUAL_STATUSES = (:verified, :unknown, :failed)
const _RODH_NUMERICAL_AGREEMENT_STATUSES = (:verified, :unknown, :failed)
const _RODH_EVENT_STATUSES = (:verified, :candidate, :unknown, :failed)
const _RODH_EVENT_TYPES = (:branch_switch, :threshold_crossing, :unknown)
const _RODH_EVENT_DETECTION_STATUSES = (:complete, :unknown, :failed)
const _RODH_BRANCH_STRUCTURE_STATUSES = (
    :unique_stable_branch_certified,
    :multiple_stable_branches_certified,
    :unknown,
)
const _RODH_MAX_RATE_RELATIVE_TOLERANCE = 1e-3
const _RODH_MAX_CLOSURE_RELATIVE_TOLERANCE = 1e-6
const _RODH_MAX_STATE_COMPARISON_RELATIVE_TOLERANCE = 1e-6
const _RODH_MAX_SOLVER_ABSOLUTE_TOLERANCE_MULTIPLIER = 1_000.0
const _RODH_MAX_BRANCH_RESIDUAL_TOLERANCE_MULTIPLIER = 10_000.0
const _RODH_MAX_POLYNOMIAL_EXPONENT = 64
const _RODH_MAX_POLYNOMIAL_TOTAL_DEGREE = 128
const _RODH_BRANCH_EVIDENCE_KINDS = (
    :equilibrium_branch,
    :protocol_dynamics,
)

struct RODynamicEvidenceLimits
    max_controls::Int
    max_states::Int
    max_outputs::Int
    max_points_per_trace::Int
    max_events_per_trace::Int
    max_common_comparisons::Int
    max_event_pair_comparisons::Int
    max_analysis_work_units::Int
    max_vector_field_terms::Int
    max_vector_field_exponent_entries::Int
    max_vector_field_work_units::Int
    max_payload_scalars::Int
end

function RODynamicEvidenceLimits(;
    max_controls::Integer=64,
    max_states::Integer=256,
    max_outputs::Integer=256,
    max_points_per_trace::Integer=100_000,
    max_events_per_trace::Integer=10_000,
    max_common_comparisons::Integer=100_000,
    max_event_pair_comparisons::Integer=1_000_000,
    max_analysis_work_units::Integer=2_000_000,
    max_vector_field_terms::Integer=100_000,
    max_vector_field_exponent_entries::Integer=1_000_000,
    max_vector_field_work_units::Integer=10_000_000,
    max_payload_scalars::Integer=10_000_000,
)
    names = (
        "max_controls",
        "max_states",
        "max_outputs",
        "max_points_per_trace",
        "max_events_per_trace",
        "max_common_comparisons",
        "max_event_pair_comparisons",
        "max_analysis_work_units",
        "max_vector_field_terms",
        "max_vector_field_exponent_entries",
        "max_vector_field_work_units",
        "max_payload_scalars",
    )
    raw = (
        max_controls,
        max_states,
        max_outputs,
        max_points_per_trace,
        max_events_per_trace,
        max_common_comparisons,
        max_event_pair_comparisons,
        max_analysis_work_units,
        max_vector_field_terms,
        max_vector_field_exponent_entries,
        max_vector_field_work_units,
        max_payload_scalars,
    )
    converted = Vector{Int}(undef, length(raw))
    for index in eachindex(raw)
        value = raw[index]
        value isa Bool && throw(ArgumentError(
            "$(names[index]) must be an integer"))
        converted[index] = try
            Int(value)
        catch
            throw(ArgumentError("$(names[index]) must fit in Int"))
        end
        converted[index] > 0 || throw(ArgumentError(
            "$(names[index]) must be positive"))
    end
    return RODynamicEvidenceLimits(converted...)
end

@inline function _rodh_strict_float(
    value,
    label::AbstractString;
    finite::Bool=true,
)
    value isa Float64 || throw(ArgumentError(
        "$label must be a strict Float64 value"))
    finite && !isfinite(value) && throw(ArgumentError(
        "$label must be finite"))
    return value
end

@inline function _rodh_strict_nonnegative_float(
    value,
    label::AbstractString,
)
    converted = _rodh_strict_float(value, label)
    converted >= 0.0 && !(converted == 0.0 && signbit(converted)) ||
        throw(ArgumentError(
            "$label must be non-negative and not negative zero"))
    return converted
end

@inline function _rodh_strict_positive_float(
    value,
    label::AbstractString,
)
    converted = _rodh_strict_float(value, label)
    converted > 0.0 || throw(ArgumentError(
        "$label must be positive"))
    return converted
end

@inline function _rodh_cancel(cancel_check)
    cancel_check === nothing || cancel_check()
    return nothing
end

struct RODynamicEvidenceLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::RODynamicEvidenceLimitExceeded)
    print(io, "dynamic RO evidence ", err.phase, " requires ",
        err.requested, ", exceeding limit=", err.limit)
end

@inline function _rodh_limit(
    phase::Symbol,
    requested::Integer,
    limit::Int,
)
    value = BigInt(requested)
    value <= limit || throw(RODynamicEvidenceLimitExceeded(
        phase, value, limit))
    return nothing
end

function _rodh_ordered_count(values, label::AbstractString)
    (values isa AbstractVector || values isa Tuple) || throw(ArgumentError(
        "$label must be an ordered collection"))
    return length(values)
end

function _rodh_vector_count(values, label::AbstractString)
    values isa AbstractVector || throw(ArgumentError(
        "$label must be a vector"))
    return length(values)
end

@inline function _rodh_float(
    value::Real,
    label::AbstractString;
    finite::Bool=true,
)
    value isa Bool && throw(ArgumentError(
        "$label must be a real non-Boolean value"))
    converted = Float64(value)
    finite && !isfinite(converted) && throw(ArgumentError(
        "$label must be finite"))
    return converted == 0.0 ? 0.0 : converted
end

function _rodh_nonempty_string(value, label::AbstractString)
    (value isa AbstractString || value isa Symbol) || throw(ArgumentError(
        "$label must be a string or Symbol"))
    converted = String(value)
    isempty(strip(converted)) && throw(ArgumentError(
        "$label must be non-empty"))
    ncodeunits(converted) <= 256 || throw(ArgumentError(
        "$label must not exceed 256 UTF-8 bytes"))
    return converted
end

@inline function _rodh_rate_unit_display(
    control_unit::AbstractString,
    time_unit::AbstractString,
)
    return String(control_unit) * "/" * String(time_unit)
end

@inline function _rodh_rate_unit_identity(
    control_unit::AbstractString,
    time_unit::AbstractString,
)
    return (
        numerator=(role="swept_control", unit=String(control_unit)),
        denominator=(role="model_time", unit=String(time_unit)),
        display=_rodh_rate_unit_display(control_unit, time_unit),
    )
end

function _rodh_sha256_id(value, label::AbstractString)
    converted = _rodh_nonempty_string(value, label)
    match(r"^sha256:[0-9a-f]{64}$", converted) === nothing &&
        throw(ArgumentError(
            "$label must be a lowercase sha256 content identity"))
    return converted
end

function _rodh_optional_sha256_id(value, label::AbstractString)
    value === nothing && return nothing
    return _rodh_sha256_id(value, label)
end

function _rodh_strings(
    values,
    label::AbstractString;
    unique::Bool=false,
    allow_empty::Bool=false,
)
    _rodh_ordered_count(values, label)
    !allow_empty && isempty(values) && throw(ArgumentError(
        "$label must not be empty"))
    result = Vector{String}(undef, length(values))
    for index in eachindex(values)
        result[index] = _rodh_nonempty_string(
            values[index], "$label entry")
    end
    unique && !allunique(result) && throw(ArgumentError(
        "$label entries must be unique"))
    return result
end

function _rodh_optional_strings(values, label::AbstractString)
    _rodh_ordered_count(values, label)
    result = Vector{Union{Nothing,String}}(undef, length(values))
    for index in eachindex(values)
        value = values[index]
        result[index] = value === nothing ? nothing :
            _rodh_nonempty_string(value, "$label entry")
    end
    return result
end

function _rodh_symbol_vector(values, label::AbstractString, allowed)
    _rodh_ordered_count(values, label)
    result = Vector{Symbol}(undef, length(values))
    for index in eachindex(values)
        value = values[index]
        value isa Symbol || throw(ArgumentError(
            "$label entries must be Symbols"))
        value in allowed || throw(ArgumentError(
            "$label contains unsupported value $value"))
        result[index] = value
    end
    return result
end

function _rodh_finite_vector(values, label::AbstractString)
    _rodh_vector_count(values, label)
    all(value -> value isa Real && !(value isa Bool), values) ||
        throw(ArgumentError(
            "$label must contain real non-Boolean values"))
    result = Vector{Float64}(undef, length(values))
    for index in eachindex(values)
        value = Float64(values[index])
        isfinite(value) || throw(ArgumentError(
            "$label must contain only finite values"))
        result[index] = value == 0.0 ? 0.0 : value
    end
    return result
end

function _rodh_evidence_vector(values, label::AbstractString)
    _rodh_vector_count(values, label)
    all(value -> value isa Float64, values) || throw(ArgumentError(
        "$label must contain Float64 values without implicit conversion"))
    result = Vector{Float64}(undef, length(values))
    for index in eachindex(values)
        value = values[index]
        value < 0.0 && throw(ArgumentError(
            "$label must not contain negative norms"))
        value == 0.0 && signbit(value) && throw(ArgumentError(
            "$label must not encode a norm as negative zero"))
        result[index] = value == 0.0 ? 0.0 : value
    end
    return result
end

function _rodh_evidence_matrix(values, label::AbstractString)
    values isa AbstractMatrix || throw(ArgumentError(
        "$label must be a matrix"))
    all(value -> value isa Real && !(value isa Bool), values) ||
        throw(ArgumentError(
            "$label must contain real non-Boolean values"))
    result = Matrix{Float64}(values)
    for index in eachindex(result)
        result[index] == 0.0 && (result[index] = 0.0)
    end
    return result
end

function _rodh_rate_matches(
    left::Float64,
    right::Float64,
    relative_tolerance::Float64,
    absolute_tolerance::Float64,
)
    left > 0.0 && right > 0.0 || return false
    abs(left - right) <= absolute_tolerance && return true
    return abs(log(left / right)) <= log1p(relative_tolerance)
end

function _rodh_max_abs_delta(left::AbstractVector, right::AbstractVector)
    length(left) == length(right) || return Inf
    isempty(left) && return 0.0
    result = 0.0
    for index in eachindex(left)
        result = max(result, abs(Float64(left[index]) - Float64(right[index])))
    end
    return result
end

struct RODynamicSwitchEvent
    status::Symbol
    event_type::Symbol
    left_index::Int
    right_index::Int
    control_interval::NTuple{2,Float64}
    control_unit::String
    control_uncertainty::Float64
    pre_branch_id::Union{Nothing,String}
    post_branch_id::Union{Nothing,String}
    pre_stability::Symbol
    post_stability::Symbol
    policy_sha256::String
end

function RODynamicSwitchEvent(;
    status,
    event_type,
    left_index::Integer,
    right_index::Integer,
    control_interval,
    control_unit,
    control_uncertainty::Real,
    pre_branch_id=nothing,
    post_branch_id=nothing,
    pre_stability,
    post_stability,
    policy_sha256,
)
    status isa Symbol && status in _RODH_EVENT_STATUSES ||
        throw(ArgumentError("unsupported event status"))
    event_type isa Symbol && event_type in _RODH_EVENT_TYPES ||
        throw(ArgumentError("unsupported event type"))
    left_index isa Bool && throw(ArgumentError(
        "left_index must be an integer"))
    right_index isa Bool && throw(ArgumentError(
        "right_index must be an integer"))
    left = try
        Int(left_index)
    catch
        throw(ArgumentError("left_index must fit in Int"))
    end
    right = try
        Int(right_index)
    catch
        throw(ArgumentError("right_index must fit in Int"))
    end
    left >= 1 || throw(ArgumentError("left_index must be positive"))
    right == left + 1 || throw(ArgumentError(
        "an event must join adjacent sampled rows"))
    _rodh_ordered_count(control_interval, "control_interval") == 2 ||
        throw(DimensionMismatch(
            "control_interval must contain exactly two values"))
    first_control = _rodh_float(
        control_interval[1], "control_interval[1]")
    second_control = _rodh_float(
        control_interval[2], "control_interval[2]")
    lower = min(first_control, second_control)
    upper = max(first_control, second_control)
    upper > lower || throw(ArgumentError(
        "control_interval must have positive span"))
    uncertainty = _rodh_strict_nonnegative_float(
        control_uncertainty, "control_uncertainty")
    0.0 <= uncertainty <= (upper - lower) / 2 ||
        throw(ArgumentError(
            "control_uncertainty must lie within half the event interval"))
    pre_stability isa Symbol &&
        pre_stability in _RODH_STABILITY_STATUSES ||
        throw(ArgumentError("unsupported pre_stability"))
    post_stability isa Symbol &&
        post_stability in _RODH_STABILITY_STATUSES ||
        throw(ArgumentError("unsupported post_stability"))
    pre_branch = pre_branch_id === nothing ? nothing :
        _rodh_nonempty_string(pre_branch_id, "pre_branch_id")
    post_branch = post_branch_id === nothing ? nothing :
        _rodh_nonempty_string(post_branch_id, "post_branch_id")
    if status == :verified
        pre_stability == :stable && post_stability == :stable ||
            throw(ArgumentError(
                "a verified event requires stable pre/post sampled states"))
        if event_type == :branch_switch
            (pre_branch === nothing) == (post_branch === nothing) ||
                throw(ArgumentError(
                    "branch ids must be supplied together or both omitted"))
            if pre_branch !== nothing
                pre_branch != post_branch || throw(ArgumentError(
                    "supplied branch ids must change at a verified switch"))
            end
        end
    end
    return RODynamicSwitchEvent(
        status,
        event_type,
        left,
        right,
        (lower, upper),
        _rodh_nonempty_string(control_unit, "control_unit"),
        uncertainty,
        pre_branch,
        post_branch,
        pre_stability,
        post_stability,
        _rodh_sha256_id(policy_sha256, "event policy_sha256"),
    )
end

function _rodh_normalize_event(event::RODynamicSwitchEvent)
    return RODynamicSwitchEvent(
        status=event.status,
        event_type=event.event_type,
        left_index=event.left_index,
        right_index=event.right_index,
        control_interval=event.control_interval,
        control_unit=event.control_unit,
        control_uncertainty=event.control_uncertainty,
        pre_branch_id=event.pre_branch_id,
        post_branch_id=event.post_branch_id,
        pre_stability=event.pre_stability,
        post_stability=event.post_stability,
        policy_sha256=event.policy_sha256,
    )
end

function _rodh_sort_events!(events, cancel_check)
    sort!(events; by=event -> begin
        _rodh_cancel(cancel_check)
        (
            event.left_index,
            event.right_index,
            String(event.event_type),
            String(event.status),
            something(event.pre_branch_id, ""),
            something(event.post_branch_id, ""),
        )
    end)
    _rodh_cancel(cancel_check)
    return events
end

struct RODynamicProtocolTrace
    schema_version::String
    model_identity::String
    coordinate_chart_id::String
    dynamics_policy_sha256::String
    residual_policy_sha256::String
    protocol_family_sha256::String
    protocol_id::String
    lineage_predecessor_trace_sha256::Union{Nothing,String}
    control_id::String
    control_unit::String
    fixed_control_ids::Vector{String}
    fixed_control_units::Vector{String}
    fixed_control_values::Vector{Float64}
    direction::Symbol
    rate::Float64
    rate_unit::String
    rate_relative_tolerance::Float64
    rate_absolute_tolerance::Float64
    rate_absolute_tolerance_unit::String
    time_unit::String
    initial_condition_id::String
    initial_state::Vector{Float64}
    state_ids::Vector{String}
    state_units::Vector{String}
    output_ids::Vector{String}
    output_units::Vector{String}
    trajectory_solver_method::String
    model_solver_policy_sha256::String
    trajectory_solver_policy_sha256::String
    trajectory_solver_relative_tolerance::Float64
    trajectory_solver_absolute_tolerance::Float64
    stability_method::String
    stability_policy_sha256::String
    branch_policy_sha256::String
    branch_structure_status::Symbol
    event_method::String
    event_policy_sha256::String
    event_detection_status::Symbol
    dynamics_residual_tolerance::Float64
    numerical_agreement_policy_sha256::String
    numerical_agreement_tolerance::Float64
    time::Vector{Float64}
    control::Vector{Float64}
    states::Matrix{Float64}
    outputs::Matrix{Float64}
    solver_status::Vector{Symbol}
    local_stability::Vector{Symbol}
    branch_ids::Vector{Union{Nothing,String}}
    dynamics_residual_norm::Vector{Float64}
    dynamics_residual_status::Vector{Symbol}
    numerical_agreement_norm::Vector{Float64}
    numerical_agreement_status::Vector{Symbol}
    events::Vector{RODynamicSwitchEvent}
    numeric_validity::BitVector
    residual_validity::BitVector
    numerical_agreement_validity::BitVector
    validity::BitVector
    gap_reasons::Vector{Union{Nothing,Symbol}}
end

function RODynamicProtocolTrace(;
    model_identity,
    coordinate_chart_id,
    dynamics_policy_sha256,
    residual_policy_sha256,
    protocol_family_sha256,
    protocol_id,
    lineage_predecessor_trace_sha256=nothing,
    control_id,
    control_unit,
    fixed_control_ids=String[],
    fixed_control_units=String[],
    fixed_control_values=Float64[],
    direction,
    rate::Real,
    rate_unit,
    rate_relative_tolerance::Real=1e-9,
    rate_absolute_tolerance::Real=0.0,
    rate_absolute_tolerance_unit,
    time_unit,
    initial_condition_id,
    initial_state,
    state_ids,
    state_units,
    output_ids,
    output_units,
    trajectory_solver_method,
    model_solver_policy_sha256,
    trajectory_solver_policy_sha256,
    trajectory_solver_relative_tolerance::Real,
    trajectory_solver_absolute_tolerance::Real,
    stability_method,
    stability_policy_sha256,
    branch_policy_sha256,
    branch_structure_status,
    event_method,
    event_policy_sha256,
    event_detection_status,
    dynamics_residual_tolerance::Real,
    numerical_agreement_policy_sha256,
    numerical_agreement_tolerance::Real,
    time,
    control,
    states,
    outputs,
    solver_status,
    local_stability,
    branch_ids,
    dynamics_residual_norm,
    dynamics_residual_status,
    numerical_agreement_norm,
    numerical_agreement_status,
    events=RODynamicSwitchEvent[],
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    # Cancellation is an admission decision.  Check it before even asking a
    # caller-owned collection for its length or first element so an already
    # cancelled request cannot spend work normalizing a large trace.
    _rodh_cancel(cancel_check)
    direction isa Symbol && direction in (:increasing, :decreasing) ||
        throw(ArgumentError(
            "direction must be :increasing or :decreasing"))
    branch_structure_status isa Symbol &&
        branch_structure_status in _RODH_BRANCH_STRUCTURE_STATUSES ||
        throw(ArgumentError("unsupported branch_structure_status"))
    event_detection_status isa Symbol &&
        event_detection_status in _RODH_EVENT_DETECTION_STATUSES ||
        throw(ArgumentError("unsupported event_detection_status"))

    state_count = _rodh_ordered_count(state_ids, "state_ids")
    output_count = _rodh_ordered_count(output_ids, "output_ids")
    state_count > 0 || throw(ArgumentError("state_ids must not be empty"))
    output_count > 0 || throw(ArgumentError("output_ids must not be empty"))
    _rodh_limit(:state_dimensions, state_count, limits.max_states)
    _rodh_limit(:output_dimensions, output_count, limits.max_outputs)
    _rodh_ordered_count(state_units, "state_units") == state_count ||
        throw(DimensionMismatch("state_units must match state_ids"))
    _rodh_ordered_count(output_units, "output_units") == output_count ||
        throw(DimensionMismatch("output_units must match output_ids"))

    fixed_count = _rodh_ordered_count(
        fixed_control_ids, "fixed_control_ids")
    _rodh_ordered_count(
        fixed_control_units, "fixed_control_units") == fixed_count ||
        throw(DimensionMismatch(
            "fixed_control_units must match fixed_control_ids"))
    _rodh_vector_count(
        fixed_control_values, "fixed_control_values") == fixed_count ||
        throw(DimensionMismatch(
            "fixed_control_values must match fixed_control_ids"))
    _rodh_limit(
        :control_dimensions,
        BigInt(1) + fixed_count,
        limits.max_controls,
    )

    point_count = _rodh_vector_count(time, "time")
    point_count >= 2 || throw(ArgumentError(
        "a dynamic protocol trace requires at least two points"))
    _rodh_vector_count(control, "control") == point_count ||
        throw(DimensionMismatch(
            "control must have one value per time point"))
    _rodh_limit(
        :points_per_trace, point_count, limits.max_points_per_trace)
    states isa AbstractMatrix || throw(ArgumentError(
        "states must be a matrix"))
    outputs isa AbstractMatrix || throw(ArgumentError(
        "outputs must be a matrix"))
    size(states) == (point_count, state_count) ||
        throw(DimensionMismatch(
            "states must have point_count x state_count shape"))
    size(outputs) == (point_count, output_count) ||
        throw(DimensionMismatch(
            "outputs must have point_count x output_count shape"))
    _rodh_vector_count(initial_state, "initial_state") == state_count ||
        throw(DimensionMismatch("initial_state must match state_ids"))
    _rodh_ordered_count(solver_status, "solver_status") == point_count ||
        throw(DimensionMismatch(
            "solver_status must have one entry per point"))
    _rodh_ordered_count(local_stability, "local_stability") == point_count ||
        throw(DimensionMismatch(
            "local_stability must have one entry per point"))
    _rodh_ordered_count(branch_ids, "branch_ids") == point_count ||
        throw(DimensionMismatch(
            "branch_ids must have one entry per point"))
    _rodh_vector_count(
        dynamics_residual_norm, "dynamics_residual_norm") == point_count ||
        throw(DimensionMismatch(
            "dynamics_residual_norm must have one entry per point"))
    _rodh_ordered_count(
        dynamics_residual_status,
        "dynamics_residual_status",
    ) == point_count || throw(DimensionMismatch(
        "dynamics_residual_status must have one entry per point"))
    _rodh_vector_count(
        numerical_agreement_norm,
        "numerical_agreement_norm",
    ) == point_count || throw(DimensionMismatch(
        "numerical_agreement_norm must have one entry per point"))
    _rodh_ordered_count(
        numerical_agreement_status,
        "numerical_agreement_status",
    ) == point_count || throw(DimensionMismatch(
        "numerical_agreement_status must have one entry per point"))
    event_count = _rodh_ordered_count(events, "events")
    _rodh_limit(
        :events_per_trace, event_count, limits.max_events_per_trace)
    all(event -> event isa RODynamicSwitchEvent, events) ||
        throw(ArgumentError(
            "events must contain RODynamicSwitchEvent values"))

    # Identity retains both evidence-redacted rows and the exact IEEE-754 bits
    # of every raw diagnostic row.  Count both representations so an identity
    # request cannot silently double the caller's declared scalar budget.
    payload_scalars = BigInt(state_count) + BigInt(fixed_count) +
        BigInt(4) * point_count +
        BigInt(2) * BigInt(point_count) *
            (BigInt(state_count) + BigInt(output_count))
    _rodh_limit(
        :payload_scalars, payload_scalars, limits.max_payload_scalars)

    state_names = _rodh_strings(state_ids, "state_ids"; unique=true)
    output_names = _rodh_strings(output_ids, "output_ids"; unique=true)
    state_unit_values = _rodh_strings(state_units, "state_units")
    output_unit_values = _rodh_strings(output_units, "output_units")
    fixed_ids = _rodh_strings(
        fixed_control_ids, "fixed_control_ids";
        unique=true, allow_empty=true)
    fixed_units = _rodh_strings(
        fixed_control_units, "fixed_control_units"; allow_empty=true)
    fixed_values = _rodh_finite_vector(
        fixed_control_values, "fixed_control_values")
    swept_id = _rodh_nonempty_string(control_id, "control_id")
    control_units = _rodh_nonempty_string(
        control_unit, "control_unit")
    swept_id in fixed_ids && throw(ArgumentError(
        "control_id must not also appear in fixed_control_ids"))

    time_values = _rodh_finite_vector(time, "time")
    control_values = _rodh_finite_vector(control, "control")
    all(time_values[index] < time_values[index + 1]
        for index in 1:(point_count - 1)) || throw(ArgumentError(
            "time must be strictly increasing"))
    if direction == :increasing
        all(control_values[index] < control_values[index + 1]
            for index in 1:(point_count - 1)) || throw(ArgumentError(
                "control must be strictly increasing"))
    else
        all(control_values[index] > control_values[index + 1]
            for index in 1:(point_count - 1)) || throw(ArgumentError(
                "control must be strictly decreasing"))
    end

    rate_value = _rodh_float(rate, "rate")
    rate_value > 0.0 || throw(ArgumentError("rate must be positive"))
    rate_units = _rodh_nonempty_string(rate_unit, "rate_unit")
    rate_rtol = _rodh_strict_nonnegative_float(
        rate_relative_tolerance, "rate_relative_tolerance")
    0.0 <= rate_rtol <= _RODH_MAX_RATE_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "rate_relative_tolerance is outside the supported range"))
    rate_atol = _rodh_strict_nonnegative_float(
        rate_absolute_tolerance, "rate_absolute_tolerance")
    rate_atol <= rate_value * _RODH_MAX_RATE_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "rate_absolute_tolerance must be bounded by the declared " *
            "rate measurement scale"))
    rate_atol_unit = _rodh_nonempty_string(
        rate_absolute_tolerance_unit,
        "rate_absolute_tolerance_unit",
    )
    rate_atol_unit == rate_units || throw(ArgumentError(
        "rate_absolute_tolerance_unit must equal rate_unit"))
    for index in 1:(point_count - 1)
        _rodh_cancel(cancel_check)
        observed_rate = abs(
            (control_values[index + 1] - control_values[index]) /
            (time_values[index + 1] - time_values[index]))
        _rodh_rate_matches(
            observed_rate, rate_value, rate_rtol, rate_atol) ||
            throw(ArgumentError(
                "control/time interval $index is inconsistent with " *
                "the declared constant rate"))
    end

    initial_values = _rodh_finite_vector(initial_state, "initial_state")
    state_values = _rodh_evidence_matrix(states, "states")
    output_values = _rodh_evidence_matrix(outputs, "outputs")
    for state_index in 1:state_count
        state_values[1, state_index] == initial_values[state_index] ||
            throw(ArgumentError(
                "initial_state must exactly equal the first sampled state"))
    end
    solver_values = _rodh_symbol_vector(
        solver_status, "solver_status", _RODH_SOLVER_STATUSES)
    stability_values = _rodh_symbol_vector(
        local_stability, "local_stability", _RODH_STABILITY_STATUSES)
    branch_values = _rodh_optional_strings(branch_ids, "branch_ids")
    residual_values = _rodh_evidence_vector(
        dynamics_residual_norm, "dynamics_residual_norm")
    residual_status_values = _rodh_symbol_vector(
        dynamics_residual_status,
        "dynamics_residual_status",
        _RODH_RESIDUAL_STATUSES,
    )
    residual_tolerance = _rodh_strict_positive_float(
        dynamics_residual_tolerance,
        "dynamics_residual_tolerance",
    )
    agreement_values = _rodh_evidence_vector(
        numerical_agreement_norm, "numerical_agreement_norm")
    agreement_status_values = _rodh_symbol_vector(
        numerical_agreement_status,
        "numerical_agreement_status",
        _RODH_NUMERICAL_AGREEMENT_STATUSES,
    )
    agreement_tolerance = _rodh_strict_positive_float(
        numerical_agreement_tolerance,
        "numerical_agreement_tolerance",
    )

    numeric_validity = falses(point_count)
    residual_validity = falses(point_count)
    numerical_agreement_validity = falses(point_count)
    validity = falses(point_count)
    gap_reasons = Vector{Union{Nothing,Symbol}}(undef, point_count)
    for point in 1:point_count
        _rodh_cancel(cancel_check)
        finite_row = all(isfinite, @view(state_values[point, :])) &&
            all(isfinite, @view(output_values[point, :]))
        numeric_validity[point] =
            solver_values[point] == :success && finite_row
        residual_validity[point] =
            residual_status_values[point] == :verified &&
            isfinite(residual_values[point]) &&
            residual_values[point] <= residual_tolerance
        numerical_agreement_validity[point] =
            agreement_status_values[point] == :verified &&
            isfinite(agreement_values[point]) &&
            agreement_values[point] <= agreement_tolerance
        # Model evidence remains independent of a numerical comparison between
        # two trajectory solvers.  Agreement can support a trajectory artifact,
        # but it can never manufacture a model-residual certificate.
        validity[point] =
            numeric_validity[point] && residual_validity[point]
        gap_reasons[point] = if solver_values[point] != :success
            Symbol("solver_", solver_values[point])
        elseif !finite_row
            :nonfinite
        elseif residual_status_values[point] != :verified
            Symbol("residual_", residual_status_values[point])
        elseif !isfinite(residual_values[point])
            :residual_nonfinite
        elseif residual_values[point] > residual_tolerance
            :dynamics_residual_exceeded
        else
            nothing
        end
    end

    if branch_structure_status == :unique_stable_branch_certified
        all(branch -> branch !== nothing, branch_values) ||
            throw(ArgumentError(
                "a unique-branch certificate requires every branch_id"))
        length(unique(String.(branch_values))) == 1 ||
            throw(ArgumentError(
                "a unique-branch certificate must use one branch_id"))
    elseif branch_structure_status == :multiple_stable_branches_certified
        all(branch -> branch !== nothing, branch_values) ||
            throw(ArgumentError(
                "a multi-branch certificate requires every branch_id"))
        length(unique(String.(branch_values))) >= 2 ||
            throw(ArgumentError(
                "a multi-branch certificate requires at least two branch ids"))
    end

    event_policy = _rodh_sha256_id(
        event_policy_sha256, "event_policy_sha256")
    normalized_events = Vector{RODynamicSwitchEvent}(undef, event_count)
    for event_index in eachindex(events)
        _rodh_cancel(cancel_check)
        event = _rodh_normalize_event(events[event_index])
        event.right_index <= point_count || throw(ArgumentError(
            "event indices exceed the trace point population"))
        event.control_unit == control_units || throw(ArgumentError(
            "event control_unit must equal trace control_unit"))
        event.policy_sha256 == event_policy || throw(ArgumentError(
            "event policy hash must equal trace event_policy_sha256"))
        expected_interval = (
            min(control_values[event.left_index],
                control_values[event.right_index]),
            max(control_values[event.left_index],
                control_values[event.right_index]),
        )
        event.control_interval == expected_interval || throw(ArgumentError(
            "event control_interval must equal its sampled control edge"))
        event.pre_stability == stability_values[event.left_index] &&
            event.post_stability == stability_values[event.right_index] ||
            throw(ArgumentError(
                "event stability must match the sampled endpoints"))
        if event.pre_branch_id !== nothing
            event.pre_branch_id == branch_values[event.left_index] ||
                throw(ArgumentError(
                    "event pre_branch_id must match the sampled branch"))
        end
        if event.post_branch_id !== nothing
            event.post_branch_id == branch_values[event.right_index] ||
                throw(ArgumentError(
                    "event post_branch_id must match the sampled branch"))
        end
        if event.status == :verified
            event_detection_status == :complete || throw(ArgumentError(
                "verified events require complete event detection"))
            validity[event.left_index] && validity[event.right_index] ||
                throw(ArgumentError(
                    "verified events require valid model-residual endpoints"))
        end
        normalized_events[event_index] = event
    end
    event_keys = Set{Tuple}()
    for event in normalized_events
        _rodh_cancel(cancel_check)
        key = (
            event.status,
            event.event_type,
            event.left_index,
            event.right_index,
            event.control_interval,
            event.control_uncertainty,
            event.pre_branch_id,
            event.post_branch_id,
            event.pre_stability,
            event.post_stability,
            event.policy_sha256,
        )
        key in event_keys && throw(ArgumentError(
            "events must not contain duplicate records"))
        push!(event_keys, key)
    end
    _rodh_sort_events!(normalized_events, cancel_check)

    return RODynamicProtocolTrace(
        RO_DYNAMIC_PROTOCOL_TRACE_VERSION,
        _rodh_sha256_id(model_identity, "model_identity"),
        _rodh_sha256_id(coordinate_chart_id, "coordinate_chart_id"),
        _rodh_sha256_id(
            dynamics_policy_sha256, "dynamics_policy_sha256"),
        _rodh_sha256_id(
            residual_policy_sha256, "residual_policy_sha256"),
        _rodh_sha256_id(
            protocol_family_sha256, "protocol_family_sha256"),
        _rodh_nonempty_string(protocol_id, "protocol_id"),
        _rodh_optional_sha256_id(
            lineage_predecessor_trace_sha256,
            "lineage_predecessor_trace_sha256",
        ),
        swept_id,
        control_units,
        copy(fixed_ids),
        copy(fixed_units),
        copy(fixed_values),
        direction,
        rate_value,
        rate_units,
        rate_rtol,
        rate_atol,
        rate_atol_unit,
        _rodh_nonempty_string(time_unit, "time_unit"),
        _rodh_nonempty_string(
            initial_condition_id, "initial_condition_id"),
        copy(initial_values),
        copy(state_names),
        copy(state_unit_values),
        copy(output_names),
        copy(output_unit_values),
        _rodh_nonempty_string(
            trajectory_solver_method, "trajectory_solver_method"),
        _rodh_sha256_id(
            model_solver_policy_sha256, "model_solver_policy_sha256"),
        _rodh_sha256_id(
            trajectory_solver_policy_sha256,
            "trajectory_solver_policy_sha256",
        ),
        begin
            value = _rodh_strict_positive_float(
                trajectory_solver_relative_tolerance,
                "trajectory_solver_relative_tolerance",
            )
            value
        end,
        begin
            value = _rodh_strict_positive_float(
                trajectory_solver_absolute_tolerance,
                "trajectory_solver_absolute_tolerance",
            )
            value
        end,
        _rodh_nonempty_string(stability_method, "stability_method"),
        _rodh_sha256_id(
            stability_policy_sha256, "stability_policy_sha256"),
        _rodh_sha256_id(
            branch_policy_sha256, "branch_policy_sha256"),
        branch_structure_status,
        _rodh_nonempty_string(event_method, "event_method"),
        event_policy,
        event_detection_status,
        residual_tolerance,
        _rodh_sha256_id(
            numerical_agreement_policy_sha256,
            "numerical_agreement_policy_sha256",
        ),
        agreement_tolerance,
        copy(time_values),
        copy(control_values),
        copy(state_values),
        copy(output_values),
        copy(solver_values),
        copy(stability_values),
        copy(branch_values),
        copy(residual_values),
        copy(residual_status_values),
        copy(agreement_values),
        copy(agreement_status_values),
        copy(normalized_events),
        numeric_validity,
        residual_validity,
        numerical_agreement_validity,
        validity,
        gap_reasons,
    )
end

function _rodh_normalize_trace(
    trace::RODynamicProtocolTrace,
    limits::RODynamicEvidenceLimits,
    cancel_check=nothing,
)
    trace.schema_version == RO_DYNAMIC_PROTOCOL_TRACE_VERSION ||
        throw(ArgumentError("unsupported dynamic protocol trace version"))
    return RODynamicProtocolTrace(
        model_identity=trace.model_identity,
        coordinate_chart_id=trace.coordinate_chart_id,
        dynamics_policy_sha256=trace.dynamics_policy_sha256,
        residual_policy_sha256=trace.residual_policy_sha256,
        protocol_family_sha256=trace.protocol_family_sha256,
        protocol_id=trace.protocol_id,
        lineage_predecessor_trace_sha256=
            trace.lineage_predecessor_trace_sha256,
        control_id=trace.control_id,
        control_unit=trace.control_unit,
        fixed_control_ids=trace.fixed_control_ids,
        fixed_control_units=trace.fixed_control_units,
        fixed_control_values=trace.fixed_control_values,
        direction=trace.direction,
        rate=trace.rate,
        rate_unit=trace.rate_unit,
        rate_relative_tolerance=trace.rate_relative_tolerance,
        rate_absolute_tolerance=trace.rate_absolute_tolerance,
        rate_absolute_tolerance_unit=
            trace.rate_absolute_tolerance_unit,
        time_unit=trace.time_unit,
        initial_condition_id=trace.initial_condition_id,
        initial_state=trace.initial_state,
        state_ids=trace.state_ids,
        state_units=trace.state_units,
        output_ids=trace.output_ids,
        output_units=trace.output_units,
        trajectory_solver_method=trace.trajectory_solver_method,
        model_solver_policy_sha256=trace.model_solver_policy_sha256,
        trajectory_solver_policy_sha256=
            trace.trajectory_solver_policy_sha256,
        trajectory_solver_relative_tolerance=
            trace.trajectory_solver_relative_tolerance,
        trajectory_solver_absolute_tolerance=
            trace.trajectory_solver_absolute_tolerance,
        stability_method=trace.stability_method,
        stability_policy_sha256=trace.stability_policy_sha256,
        branch_policy_sha256=trace.branch_policy_sha256,
        branch_structure_status=trace.branch_structure_status,
        event_method=trace.event_method,
        event_policy_sha256=trace.event_policy_sha256,
        event_detection_status=trace.event_detection_status,
        dynamics_residual_tolerance=trace.dynamics_residual_tolerance,
        numerical_agreement_policy_sha256=
            trace.numerical_agreement_policy_sha256,
        numerical_agreement_tolerance=
            trace.numerical_agreement_tolerance,
        time=trace.time,
        control=trace.control,
        states=trace.states,
        outputs=trace.outputs,
        solver_status=trace.solver_status,
        local_stability=trace.local_stability,
        branch_ids=trace.branch_ids,
        dynamics_residual_norm=trace.dynamics_residual_norm,
        dynamics_residual_status=trace.dynamics_residual_status,
        numerical_agreement_norm=trace.numerical_agreement_norm,
        numerical_agreement_status=trace.numerical_agreement_status,
        events=trace.events,
        limits=limits,
        cancel_check=cancel_check,
    )
end

@inline _rodh_payload_float(value::Float64) =
    isfinite(value) ? (value == 0.0 ? 0.0 : value) : nothing

@inline _rodh_payload_float_bits(value::Float64) =
    lpad(string(reinterpret(UInt64, value); base=16), 16, '0')

function _rodh_valid_rows_payload(
    matrix::Matrix{Float64},
    validity::BitVector,
    cancel_check=nothing,
)
    rows = Vector{Any}(undef, size(matrix, 1))
    for row in axes(matrix, 1)
        _rodh_cancel(cancel_check)
        rows[row] = validity[row] ?
            Tuple(_rodh_payload_float(matrix[row, column])
                for column in axes(matrix, 2)) : nothing
    end
    return Tuple(rows)
end

function _rodh_raw_rows_bits_payload(
    matrix::Matrix{Float64},
    cancel_check=nothing,
)
    rows = Vector{Any}(undef, size(matrix, 1))
    for row in axes(matrix, 1)
        _rodh_cancel(cancel_check)
        rows[row] = Tuple(_rodh_payload_float_bits(matrix[row, column])
            for column in axes(matrix, 2))
    end
    return Tuple(rows)
end

function _rodh_event_payload(event::RODynamicSwitchEvent)
    return (
        status=String(event.status),
        event_type=String(event.event_type),
        left_index=event.left_index,
        right_index=event.right_index,
        control_interval=event.control_interval,
        control_unit=event.control_unit,
        control_uncertainty=event.control_uncertainty,
        pre_branch_id=event.pre_branch_id,
        post_branch_id=event.post_branch_id,
        pre_stability=String(event.pre_stability),
        post_stability=String(event.post_stability),
        policy_sha256=event.policy_sha256,
    )
end

function ro_dynamic_protocol_identity_payload(
    raw_trace::RODynamicProtocolTrace;
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    trace = _rodh_normalize_trace(raw_trace, limits, cancel_check)
    return (
        schema_version=trace.schema_version,
        model_identity=trace.model_identity,
        coordinate_chart_id=trace.coordinate_chart_id,
        dynamics_policy_sha256=trace.dynamics_policy_sha256,
        residual_policy_sha256=trace.residual_policy_sha256,
        protocol=(
            family_sha256=trace.protocol_family_sha256,
            protocol_id=trace.protocol_id,
            lineage_predecessor_trace_sha256=
                trace.lineage_predecessor_trace_sha256,
            swept_control=(
                id=trace.control_id,
                unit=trace.control_unit,
                direction=String(trace.direction),
                constant_rate=trace.rate,
                rate_unit=trace.rate_unit,
                rate_relative_tolerance=trace.rate_relative_tolerance,
                rate_absolute_tolerance=trace.rate_absolute_tolerance,
                rate_absolute_tolerance_unit=
                    trace.rate_absolute_tolerance_unit,
            ),
            fixed_controls=(
                order=Tuple(trace.fixed_control_ids),
                units=Tuple(trace.fixed_control_units),
                values=Tuple(trace.fixed_control_values),
            ),
            time_unit=trace.time_unit,
            time=Tuple(trace.time),
            control=Tuple(trace.control),
        ),
        initial_condition=(
            id=trace.initial_condition_id,
            state_order=Tuple(trace.state_ids),
            state_units=Tuple(trace.state_units),
            values=Tuple(trace.initial_state),
        ),
        outputs=(
            order=Tuple(trace.output_ids),
            units=Tuple(trace.output_units),
        ),
        model_solver=(
            policy_sha256=trace.model_solver_policy_sha256,
        ),
        trajectory_solver=(
            method=trace.trajectory_solver_method,
            policy_sha256=trace.trajectory_solver_policy_sha256,
            relative_tolerance=trace.trajectory_solver_relative_tolerance,
            absolute_tolerance=trace.trajectory_solver_absolute_tolerance,
            status=Tuple(String.(trace.solver_status)),
        ),
        stability=(
            method=trace.stability_method,
            policy_sha256=trace.stability_policy_sha256,
            status=Tuple(String.(trace.local_stability)),
        ),
        branches=(
            policy_sha256=trace.branch_policy_sha256,
            structure_status=String(trace.branch_structure_status),
            ids=Tuple(trace.branch_ids),
        ),
        residuals=(
            policy_sha256=trace.residual_policy_sha256,
            tolerance=trace.dynamics_residual_tolerance,
            status=Tuple(String.(trace.dynamics_residual_status)),
            norm=Tuple(_rodh_payload_float(value)
                for value in trace.dynamics_residual_norm),
        ),
        numerical_agreement=(
            policy_sha256=trace.numerical_agreement_policy_sha256,
            tolerance=trace.numerical_agreement_tolerance,
            status=Tuple(String.(trace.numerical_agreement_status)),
            norm=Tuple(_rodh_payload_float(value)
                for value in trace.numerical_agreement_norm),
            validity=Tuple(trace.numerical_agreement_validity),
        ),
        events=(
            method=trace.event_method,
            policy_sha256=trace.event_policy_sha256,
            detection_status=String(trace.event_detection_status),
            records=Tuple(_rodh_event_payload(event)
                for event in trace.events),
        ),
        numeric_validity=Tuple(trace.numeric_validity),
        residual_validity=Tuple(trace.residual_validity),
        validity=Tuple(trace.validity),
        gap_reasons=Tuple(reason === nothing ? nothing : String(reason)
            for reason in trace.gap_reasons),
        states=_rodh_valid_rows_payload(
            trace.states, trace.validity, cancel_check),
        output_values=_rodh_valid_rows_payload(
            trace.outputs, trace.validity, cancel_check),
        raw_state_diagnostics_ieee754_bits=_rodh_raw_rows_bits_payload(
            trace.states, cancel_check),
        raw_output_diagnostics_ieee754_bits=_rodh_raw_rows_bits_payload(
            trace.outputs, cancel_check),
    )
end

function ro_dynamic_protocol_identity_sha256(
    trace::RODynamicProtocolTrace;
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    payload = ro_dynamic_protocol_identity_payload(
        trace; limits=limits, cancel_check=cancel_check)
    return "sha256:" *
        bytes2hex(SHA.sha256(codeunits(JSON3.write(payload))))
end

struct _RODHValidatedToken end
const _RODH_VALIDATED_TOKEN = _RODHValidatedToken()

struct ROPolynomialVectorField
    schema_version::String
    identity_payload::NamedTuple
    identity_sha256::String
    coordinate_chart_id::String
    dynamics_policy_sha256::String
    residual_policy_sha256::String
    solver_policy_sha256::String
    stability_policy_sha256::String
    branch_policy_sha256::String
    event_policy_sha256::String
    state_ids::Tuple
    state_units::Tuple
    control_id::String
    control_unit::String
    time_unit::String
    fixed_control_ids::Tuple
    fixed_control_units::Tuple
    equations::Tuple
    term_count::Int
    exponent_entry_count::Int
    evaluation_work_units::Int
    evidence_scope::Symbol

    function ROPolynomialVectorField(
        ::_RODHValidatedToken,
        args...,
    )
        return new(args...)
    end
end

function ro_polynomial_vector_field(
    ;
    coordinate_chart_id,
    dynamics_policy_sha256,
    residual_policy_sha256,
    solver_policy_sha256,
    stability_policy_sha256,
    branch_policy_sha256,
    event_policy_sha256,
    state_ids,
    state_units,
    control_id,
    control_unit,
    time_unit,
    fixed_control_ids=String[],
    fixed_control_units=String[],
    equations,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    state_count = _rodh_ordered_count(state_ids, "state_ids")
    state_count > 0 || throw(ArgumentError(
        "state_ids must not be empty"))
    _rodh_limit(:state_dimensions, state_count, limits.max_states)
    _rodh_ordered_count(state_units, "state_units") == state_count ||
        throw(DimensionMismatch("state_units must match state_ids"))
    fixed_count = _rodh_ordered_count(
        fixed_control_ids, "fixed_control_ids")
    _rodh_ordered_count(
        fixed_control_units, "fixed_control_units") == fixed_count ||
        throw(DimensionMismatch(
            "fixed_control_units must match fixed_control_ids"))
    _rodh_limit(
        :control_dimensions,
        BigInt(fixed_count) + 1,
        limits.max_controls,
    )
    _rodh_ordered_count(equations, "equations") == state_count ||
        throw(DimensionMismatch(
            "equations must contain one polynomial per ordered state"))

    variable_count = BigInt(state_count) + fixed_count + 1
    term_count = BigInt(0)
    for equation in equations
        _rodh_cancel(cancel_check)
        term_count += _rodh_ordered_count(
            equation, "polynomial equation terms")
    end
    _rodh_limit(
        :vector_field_terms,
        term_count,
        limits.max_vector_field_terms,
    )
    exponent_entry_count = term_count * variable_count
    _rodh_limit(
        :vector_field_exponent_entries,
        exponent_entry_count,
        limits.max_vector_field_exponent_entries,
    )
    payload_scalars = term_count + exponent_entry_count +
        BigInt(2) * (state_count + fixed_count + 1)
    _rodh_limit(
        :vector_field_payload_scalars,
        payload_scalars,
        limits.max_payload_scalars,
    )

    # One stored work unit covers at most one scalar/iteration slot in the
    # interpreter below.  In particular, zero exponents still cost a scan:
    # every term visits every ordered variable, every term visits every state
    # while forming the Jacobian, and every nonzero state derivative scans the
    # complete variable vector again.  Retain total-degree terms as a
    # conservative allowance for integer powers/multiplications.  BigInt keeps
    # the preflight itself overflow-safe before the bounded value is stored.
    state_work = BigInt(state_count)
    evaluation_work = BigInt(2) * variable_count +
        BigInt(2) * (state_work + state_work^2) + state_work
    expected_exponent_count = Int(variable_count)
    for (equation_index, equation) in enumerate(equations)
        for (term_index, term) in enumerate(equation)
            _rodh_cancel(cancel_check)
            term isa NamedTuple &&
                keys(term) == (:coefficient, :exponents) ||
                throw(ArgumentError(
                    "equation $equation_index term $term_index must be " *
                    "a (coefficient, exponents) NamedTuple"))
            coefficient = _rodh_strict_float(
                term.coefficient,
                "polynomial coefficient",
            )
            coefficient != 0.0 || throw(ArgumentError(
                "polynomial coefficients must be nonzero"))
            _rodh_ordered_count(
                term.exponents,
                "polynomial exponent vector",
            ) == expected_exponent_count || throw(DimensionMismatch(
                "each polynomial exponent vector must match the ordered " *
                "state, swept-control, and fixed-control variables"))
            total_degree = 0
            for exponent in term.exponents
                _rodh_cancel(cancel_check)
                exponent isa Int || throw(ArgumentError(
                    "polynomial exponents must be strict Int values"))
                0 <= exponent <= _RODH_MAX_POLYNOMIAL_EXPONENT ||
                    throw(ArgumentError(
                        "polynomial exponent is outside the supported range"))
                total_degree += exponent
            end
            total_degree <= _RODH_MAX_POLYNOMIAL_TOTAL_DEGREE ||
                throw(ArgumentError(
                    "polynomial total degree is outside the supported range"))
            evaluation_work += BigInt(2) + variable_count + total_degree +
                state_count
            for state_index in 1:state_count
                state_exponent = term.exponents[state_index]
                state_exponent == 0 && continue
                evaluation_work += variable_count + 2 +
                    max(total_degree - 1, 0)
            end
        end
    end
    _rodh_limit(
        :vector_field_work_units,
        evaluation_work,
        limits.max_vector_field_work_units,
    )

    state_names = _rodh_strings(state_ids, "state_ids"; unique=true)
    state_unit_values = _rodh_strings(state_units, "state_units")
    swept_id = _rodh_nonempty_string(control_id, "control_id")
    swept_unit = _rodh_nonempty_string(control_unit, "control_unit")
    model_time_unit = _rodh_nonempty_string(time_unit, "time_unit")
    fixed_ids = _rodh_strings(
        fixed_control_ids,
        "fixed_control_ids";
        unique=true,
        allow_empty=true,
    )
    fixed_units = _rodh_strings(
        fixed_control_units,
        "fixed_control_units";
        allow_empty=true,
    )
    swept_id in fixed_ids && throw(ArgumentError(
        "control_id must not also appear in fixed_control_ids"))

    normalized_equations = Vector{Tuple}(undef, state_count)
    for equation_index in 1:state_count
        _rodh_cancel(cancel_check)
        normalized_terms = NamedTuple[]
        seen_exponents = Set{Tuple}()
        for term in equations[equation_index]
            _rodh_cancel(cancel_check)
            exponent_tuple = Tuple(term.exponents)
            exponent_tuple in seen_exponents && throw(ArgumentError(
                "duplicate polynomial monomials must be merged before " *
                "constructing the vector field"))
            push!(seen_exponents, exponent_tuple)
            push!(normalized_terms, (
                coefficient=term.coefficient,
                exponents=exponent_tuple,
            ))
        end
        sort!(normalized_terms; by=term -> begin
            _rodh_cancel(cancel_check)
            term.exponents
        end)
        normalized_equations[equation_index] = Tuple(normalized_terms)
    end

    payload = (
        schema_version=RO_POLYNOMIAL_VECTOR_FIELD_VERSION,
        coordinate_chart_id=_rodh_sha256_id(
            coordinate_chart_id, "coordinate_chart_id"),
        policies=(
            dynamics_sha256=_rodh_sha256_id(
                dynamics_policy_sha256, "dynamics_policy_sha256"),
            residual_sha256=_rodh_sha256_id(
                residual_policy_sha256, "residual_policy_sha256"),
            solver_sha256=_rodh_sha256_id(
                solver_policy_sha256, "solver_policy_sha256"),
            stability_sha256=_rodh_sha256_id(
                stability_policy_sha256, "stability_policy_sha256"),
            branch_sha256=_rodh_sha256_id(
                branch_policy_sha256, "branch_policy_sha256"),
            event_sha256=_rodh_sha256_id(
                event_policy_sha256, "event_policy_sha256"),
        ),
        variables=(
            states=(
                order=Tuple(state_names),
                units=Tuple(state_unit_values),
            ),
            swept_control=(id=swept_id, unit=swept_unit),
            model_time=(
                unit=model_time_unit,
                equation_output_semantics=
                    "state_unit_per_model_time_unit",
            ),
            fixed_controls=(
                order=Tuple(fixed_ids),
                units=Tuple(fixed_units),
            ),
        ),
        equations=Tuple(normalized_equations),
        canonicalization=(
            variable_order="states_then_swept_control_then_fixed_controls",
            duplicate_monomials="rejected_require_premerged_terms",
            maximum_exponent=_RODH_MAX_POLYNOMIAL_EXPONENT,
            maximum_total_degree=_RODH_MAX_POLYNOMIAL_TOTAL_DEGREE,
        ),
        term_count=Int(term_count),
        exponent_entry_count=Int(exponent_entry_count),
        evaluation_work_units=Int(evaluation_work),
        evidence_scope="declarative_polynomial_vector_field_only",
    )
    identity_hash = _rodh_payload_sha256(payload)
    _rodh_cancel(cancel_check)
    return ROPolynomialVectorField(
        _RODH_VALIDATED_TOKEN,
        RO_POLYNOMIAL_VECTOR_FIELD_VERSION,
        payload,
        identity_hash,
        payload.coordinate_chart_id,
        payload.policies.dynamics_sha256,
        payload.policies.residual_sha256,
        payload.policies.solver_sha256,
        payload.policies.stability_sha256,
        payload.policies.branch_sha256,
        payload.policies.event_sha256,
        Tuple(state_names),
        Tuple(state_unit_values),
        swept_id,
        swept_unit,
        model_time_unit,
        Tuple(fixed_ids),
        Tuple(fixed_units),
        Tuple(normalized_equations),
        Int(term_count),
        Int(exponent_entry_count),
        Int(evaluation_work),
        :declarative_polynomial_vector_field_only,
    )
end

function validate_ro_polynomial_vector_field(
    vector_field::ROPolynomialVectorField;
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    vector_field.schema_version == RO_POLYNOMIAL_VECTOR_FIELD_VERSION ||
        throw(ArgumentError(
            "unsupported polynomial vector-field version"))
    _rodh_payload_sha256(vector_field.identity_payload) ==
        vector_field.identity_sha256 || throw(ArgumentError(
            "polynomial vector-field content hash mismatch"))
    recomputed = ro_polynomial_vector_field(
        coordinate_chart_id=vector_field.coordinate_chart_id,
        dynamics_policy_sha256=vector_field.dynamics_policy_sha256,
        residual_policy_sha256=vector_field.residual_policy_sha256,
        solver_policy_sha256=vector_field.solver_policy_sha256,
        stability_policy_sha256=vector_field.stability_policy_sha256,
        branch_policy_sha256=vector_field.branch_policy_sha256,
        event_policy_sha256=vector_field.event_policy_sha256,
        state_ids=vector_field.state_ids,
        state_units=vector_field.state_units,
        control_id=vector_field.control_id,
        control_unit=vector_field.control_unit,
        time_unit=vector_field.time_unit,
        fixed_control_ids=vector_field.fixed_control_ids,
        fixed_control_units=vector_field.fixed_control_units,
        equations=vector_field.equations,
        limits=limits,
        cancel_check=cancel_check,
    )
    for field in fieldnames(ROPolynomialVectorField)
        getfield(vector_field, field) == getfield(recomputed, field) ||
            throw(ArgumentError(
                "polynomial vector field does not reproduce at field $field"))
    end
    return true
end

function _rodh_evaluate_polynomial_vector_field(
    vector_field::ROPolynomialVectorField,
    state,
    control::Float64,
    fixed_controls,
    limits::RODynamicEvidenceLimits,
    cancel_check,
)
    state_count = length(vector_field.state_ids)
    fixed_count = length(vector_field.fixed_control_ids)
    length(state) == state_count || throw(DimensionMismatch(
        "state does not match vector-field state order"))
    length(fixed_controls) == fixed_count || throw(DimensionMismatch(
        "fixed controls do not match vector-field control order"))
    _rodh_limit(
        :vector_field_work_units,
        vector_field.evaluation_work_units,
        limits.max_vector_field_work_units,
    )
    variables = Vector{Float64}(undef, state_count + fixed_count + 1)
    for state_index in 1:state_count
        _rodh_cancel(cancel_check)
        variables[state_index] = state[state_index]
    end
    variables[state_count + 1] = control
    for fixed_index in 1:fixed_count
        _rodh_cancel(cancel_check)
        variables[state_count + 1 + fixed_index] =
            fixed_controls[fixed_index]
    end
    all(isfinite, variables) || throw(ArgumentError(
        "polynomial vector-field inputs must be finite"))

    values = zeros(Float64, state_count)
    jacobian = zeros(Float64, state_count, state_count)
    for equation_index in 1:state_count
        for term in vector_field.equations[equation_index]
            _rodh_cancel(cancel_check)
            monomial = term.coefficient
            for variable_index in eachindex(variables)
                _rodh_cancel(cancel_check)
                exponent = term.exponents[variable_index]
                exponent == 0 ||
                    (monomial *= variables[variable_index]^exponent)
            end
            values[equation_index] += monomial
            for state_index in 1:state_count
                _rodh_cancel(cancel_check)
                exponent = term.exponents[state_index]
                exponent == 0 && continue
                derivative = term.coefficient * exponent
                for variable_index in eachindex(variables)
                    _rodh_cancel(cancel_check)
                    derivative_exponent = term.exponents[variable_index] -
                        (variable_index == state_index ? 1 : 0)
                    derivative_exponent == 0 ||
                        (derivative *=
                            variables[variable_index]^derivative_exponent)
                end
                jacobian[equation_index, state_index] += derivative
            end
        end
    end
    all(isfinite, values) && all(isfinite, jacobian) ||
        throw(ArgumentError(
            "polynomial vector-field evaluation produced a non-finite value"))
    return values, jacobian
end

struct RODynamicBranchEvidence
    schema_version::String
    identity_payload::NamedTuple
    identity_sha256::String
    trace_sha256::String
    evidence_kind::Symbol
    candidate_kind::Symbol
    vector_field_sha256::Union{Nothing,String}
    residual_tolerance::Float64
    stability_margin::Float64
    recomputed_residual_norm::Vector{Union{Nothing,Float64}}
    recomputed_spectral_abscissa::Vector{Union{Nothing,Float64}}
    recomputed_validity::BitVector
    stable::BitVector
    complete::Bool
    all_stable::Bool
    strong_equilibrium_certified::Bool
    status::Symbol
    evidence_scope::Symbol

    function RODynamicBranchEvidence(
        ::_RODHValidatedToken,
        schema_version,
        identity_payload,
        identity_sha256,
        trace_sha256,
        evidence_kind,
        candidate_kind,
        vector_field_sha256,
        residual_tolerance,
        stability_margin,
        recomputed_residual_norm,
        recomputed_spectral_abscissa,
        recomputed_validity,
        stable,
        complete,
        all_stable,
        strong_equilibrium_certified,
        status,
        evidence_scope,
    )
        return new(
            schema_version,
            identity_payload,
            identity_sha256,
            trace_sha256,
            evidence_kind,
            candidate_kind,
            vector_field_sha256,
            residual_tolerance,
            stability_margin,
            recomputed_residual_norm,
            recomputed_spectral_abscissa,
            recomputed_validity,
            stable,
            complete,
            all_stable,
            strong_equilibrium_certified,
            status,
            evidence_scope,
        )
    end
end

@inline function _rodh_payload_sha256(payload)
    return "sha256:" *
        bytes2hex(SHA.sha256(codeunits(JSON3.write(payload))))
end

function _rodh_evaluator_float_vector(value, label::AbstractString)
    value isa AbstractVector || throw(ArgumentError(
        "$label must return a vector"))
    all(entry -> entry isa Float64, value) || throw(ArgumentError(
        "$label must return strict Float64 values"))
    all(isfinite, value) || throw(ArgumentError(
        "$label must return only finite values"))
    return Vector{Float64}(value)
end

function _rodh_evaluator_float_matrix(
    value,
    state_count::Int,
    label::AbstractString,
)
    value isa AbstractMatrix || throw(ArgumentError(
        "$label must return a matrix"))
    size(value) == (state_count, state_count) || throw(DimensionMismatch(
        "$label must return a state_count x state_count matrix"))
    all(entry -> entry isa Float64, value) || throw(ArgumentError(
        "$label must return strict Float64 values"))
    all(isfinite, value) || throw(ArgumentError(
        "$label must return only finite values"))
    return Matrix{Float64}(value)
end

function _rodh_bind_trace_to_vector_field(
    trace::RODynamicProtocolTrace,
    vector_field::ROPolynomialVectorField,
)
    comparisons = (
        (trace.model_identity, vector_field.identity_sha256,
            "model_identity/vector-field root"),
        (trace.coordinate_chart_id, vector_field.coordinate_chart_id,
            "coordinate_chart_id"),
        (trace.dynamics_policy_sha256,
            vector_field.dynamics_policy_sha256,
            "dynamics_policy_sha256"),
        (trace.residual_policy_sha256,
            vector_field.residual_policy_sha256,
            "residual_policy_sha256"),
        (trace.model_solver_policy_sha256,
            vector_field.solver_policy_sha256,
            "model_solver_policy_sha256"),
        (trace.stability_policy_sha256,
            vector_field.stability_policy_sha256,
            "stability_policy_sha256"),
        (trace.branch_policy_sha256,
            vector_field.branch_policy_sha256,
            "branch_policy_sha256"),
        (trace.event_policy_sha256,
            vector_field.event_policy_sha256,
            "event_policy_sha256"),
        (Tuple(trace.state_ids), vector_field.state_ids, "state_ids"),
        (Tuple(trace.state_units), vector_field.state_units, "state_units"),
        (trace.control_id, vector_field.control_id, "control_id"),
        (trace.control_unit, vector_field.control_unit, "control_unit"),
        (trace.time_unit, vector_field.time_unit, "time_unit"),
        (trace.rate_unit,
            _rodh_rate_unit_display(
                vector_field.control_unit, vector_field.time_unit),
            "structured control-per-time rate_unit"),
        (Tuple(trace.fixed_control_ids), vector_field.fixed_control_ids,
            "fixed_control_ids"),
        (Tuple(trace.fixed_control_units), vector_field.fixed_control_units,
            "fixed_control_units"),
    )
    for (trace_value, field_value, label) in comparisons
        trace_value == field_value || throw(ArgumentError(
            "trace $label must exactly match the declarative vector field"))
    end
    return nothing
end

function _rodh_compute_branch_evidence(
    trace::RODynamicProtocolTrace;
    evidence_kind::Symbol,
    candidate_kind::Symbol,
    vector_field_sha256::Union{Nothing,String},
    residual_limit::Float64,
    stability_limit::Float64,
    evaluation_work_per_point::Integer,
    point_evaluator,
    limits::RODynamicEvidenceLimits,
    cancel_check,
)
    trace_hash = ro_dynamic_protocol_identity_sha256(
        trace; limits=limits, cancel_check=cancel_check)
    point_count = length(trace.control)
    state_count = length(trace.state_ids)
    work_units = BigInt(point_count) * (
        BigInt(state_count)^3 + BigInt(evaluation_work_per_point) + 1)
    _rodh_limit(
        :branch_evidence_work_units,
        work_units,
        limits.max_analysis_work_units,
    )
    residual_norm = Vector{Union{Nothing,Float64}}(
        fill(nothing, point_count))
    spectral_abscissa = Vector{Union{Nothing,Float64}}(
        fill(nothing, point_count))
    recomputed_validity = falses(point_count)
    stable = falses(point_count)
    for point in 1:point_count
        _rodh_cancel(cancel_check)
        trace.validity[point] || continue
        residual, jacobian = point_evaluator(point)
        residual = _rodh_evaluator_float_vector(
            residual, "branch evidence residual computation")
        length(residual) == state_count || throw(DimensionMismatch(
            "branch evidence must compute one residual per state"))
        jacobian = _rodh_evaluator_float_matrix(
            jacobian,
            state_count,
            "branch evidence Jacobian computation",
        )
        norm_value = maximum(abs, residual)
        eigenvalues = LinearAlgebra.eigvals(jacobian)
        spectral_value = maximum(real, eigenvalues)
        isfinite(spectral_value) || throw(ArgumentError(
            "branch evidence produced a non-finite Jacobian spectrum"))
        residual_norm[point] = norm_value
        spectral_abscissa[point] = spectral_value
        recomputed_validity[point] = norm_value <= residual_limit
        stable[point] = recomputed_validity[point] &&
            spectral_value <= -stability_limit &&
            trace.local_stability[point] == :stable
    end
    complete = all(trace.validity) && all(recomputed_validity)
    all_stable = complete && all(stable)
    strong_equilibrium_certified =
        evidence_kind == :declarative_polynomial_equilibrium &&
        vector_field_sha256 !== nothing && complete && all_stable
    status = if !all(trace.validity)
        :unknown_trace_gap
    elseif !all(recomputed_validity)
        :recomputed_residual_failure
    elseif !all(stable)
        :recomputed_stability_failure
    elseif strong_equilibrium_certified
        :complete_declarative_equilibrium_evidence
    else
        :complete_untrusted_callback_candidate
    end
    evidence_scope = evidence_kind ==
        :declarative_polynomial_equilibrium ?
        :finite_supplied_declarative_equilibrium_branch_only :
        candidate_kind == :protocol_dynamics ?
            :finite_supplied_untrusted_callback_protocol_dynamics_only :
            :untrusted_callback_equilibrium_candidate_only
    payload = (
        schema_version=RO_DYNAMIC_BRANCH_EVIDENCE_VERSION,
        trace_sha256=trace_hash,
        evidence_kind=String(evidence_kind),
        candidate_kind=String(candidate_kind),
        vector_field_sha256=vector_field_sha256,
        model_identity=trace.model_identity,
        coordinate_chart_id=trace.coordinate_chart_id,
        dynamics_policy_sha256=trace.dynamics_policy_sha256,
        residual_policy_sha256=trace.residual_policy_sha256,
        stability_policy_sha256=trace.stability_policy_sha256,
        branch_policy_sha256=trace.branch_policy_sha256,
        event_policy_sha256=trace.event_policy_sha256,
        residual_tolerance=residual_limit,
        stability_margin=stability_limit,
        recomputed_residual_norm=Tuple(residual_norm),
        recomputed_spectral_abscissa=Tuple(spectral_abscissa),
        recomputed_validity=Tuple(recomputed_validity),
        stable=Tuple(stable),
        complete=complete,
        all_stable=all_stable,
        strong_equilibrium_certified=strong_equilibrium_certified,
        status=String(status),
        evidence_scope=String(evidence_scope),
    )
    return RODynamicBranchEvidence(
        _RODH_VALIDATED_TOKEN,
        RO_DYNAMIC_BRANCH_EVIDENCE_VERSION,
        payload,
        _rodh_payload_sha256(payload),
        trace_hash,
        evidence_kind,
        candidate_kind,
        vector_field_sha256,
        residual_limit,
        stability_limit,
        residual_norm,
        spectral_abscissa,
        recomputed_validity,
        stable,
        complete,
        all_stable,
        strong_equilibrium_certified,
        status,
        evidence_scope,
    )
end

function certify_ro_equilibrium_branch_evidence(
    raw_trace::RODynamicProtocolTrace,
    vector_field::ROPolynomialVectorField;
    residual_tolerance::Float64,
    stability_margin::Float64,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    residual_limit = _rodh_strict_positive_float(
        residual_tolerance, "branch residual_tolerance")
    stability_limit = _rodh_strict_positive_float(
        stability_margin, "branch stability_margin")
    _rodh_cancel(cancel_check)
    validate_ro_polynomial_vector_field(
        vector_field; limits=limits, cancel_check=cancel_check)
    trace = _rodh_normalize_trace(raw_trace, limits, cancel_check)
    _rodh_bind_trace_to_vector_field(trace, vector_field)
    residual_limit <= trace.dynamics_residual_tolerance ||
        throw(ArgumentError(
            "branch residual_tolerance must not exceed the trace residual policy"))
    residual_limit <=
        _RODH_MAX_BRANCH_RESIDUAL_TOLERANCE_MULTIPLIER *
            trace.trajectory_solver_absolute_tolerance || throw(ArgumentError(
                "branch residual_tolerance exceeds the solver-scaled " *
                "strong-evidence bound"))
    point_evaluator = point -> _rodh_evaluate_polynomial_vector_field(
        vector_field,
        @view(trace.states[point, :]),
        trace.control[point],
        trace.fixed_control_values,
        limits,
        cancel_check,
    )
    return _rodh_compute_branch_evidence(
        trace;
        evidence_kind=:declarative_polynomial_equilibrium,
        candidate_kind=:equilibrium_branch,
        vector_field_sha256=vector_field.identity_sha256,
        residual_limit=residual_limit,
        stability_limit=stability_limit,
        evaluation_work_per_point=vector_field.evaluation_work_units,
        point_evaluator=point_evaluator,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function certify_ro_dynamic_branch_evidence(
    raw_trace::RODynamicProtocolTrace;
    evidence_kind::Symbol,
    residual_evaluator,
    jacobian_evaluator,
    residual_tolerance::Float64,
    stability_margin::Float64,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    evidence_kind in _RODH_BRANCH_EVIDENCE_KINDS || throw(ArgumentError(
        "unsupported branch evidence kind"))
    residual_evaluator isa Function || throw(ArgumentError(
        "residual_evaluator must be callable"))
    jacobian_evaluator isa Function || throw(ArgumentError(
        "jacobian_evaluator must be callable"))
    residual_limit = _rodh_strict_positive_float(
        residual_tolerance, "branch residual_tolerance")
    stability_limit = _rodh_strict_positive_float(
        stability_margin, "branch stability_margin")

    _rodh_cancel(cancel_check)
    trace = _rodh_normalize_trace(raw_trace, limits, cancel_check)
    residual_limit <= trace.dynamics_residual_tolerance ||
        throw(ArgumentError(
            "branch residual_tolerance must not exceed the trace residual policy"))
    residual_limit <=
        _RODH_MAX_BRANCH_RESIDUAL_TOLERANCE_MULTIPLIER *
            trace.trajectory_solver_absolute_tolerance || throw(ArgumentError(
                "branch residual_tolerance exceeds the solver-scaled " *
                "strong-evidence bound"))
    point_evaluator = point -> begin
        point_payload = (
            index=point,
            time=trace.time[point],
            control=trace.control[point],
            previous_time=point == 1 ? nothing : trace.time[point - 1],
            previous_control=
                point == 1 ? nothing : trace.control[point - 1],
            control_id=trace.control_id,
            control_unit=trace.control_unit,
            fixed_control_ids=Tuple(trace.fixed_control_ids),
            fixed_control_units=Tuple(trace.fixed_control_units),
            fixed_control_values=Tuple(trace.fixed_control_values),
            state_order=Tuple(trace.state_ids),
            state_units=Tuple(trace.state_units),
            state=Tuple(@view(trace.states[point, :])),
            previous_state=point == 1 ? nothing :
                Tuple(@view(trace.states[point - 1, :])),
        )
        residual = _rodh_evaluator_float_vector(
            residual_evaluator(point_payload),
            "residual_evaluator",
        )
        length(residual) == length(trace.state_ids) || throw(DimensionMismatch(
            "residual_evaluator must return one residual per state"))
        jacobian = _rodh_evaluator_float_matrix(
            jacobian_evaluator(point_payload),
            length(trace.state_ids),
            "jacobian_evaluator",
        )
        return residual, jacobian
    end
    return _rodh_compute_branch_evidence(
        trace;
        evidence_kind=:untrusted_callback,
        candidate_kind=evidence_kind,
        vector_field_sha256=nothing,
        residual_limit=residual_limit,
        stability_limit=stability_limit,
        evaluation_work_per_point=length(trace.state_ids) + 1,
        point_evaluator=point_evaluator,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function _rodh_compare_branch_evidence(
    evidence::RODynamicBranchEvidence,
    recomputed::RODynamicBranchEvidence,
)
    for field in fieldnames(RODynamicBranchEvidence)
        getfield(evidence, field) == getfield(recomputed, field) ||
            throw(ArgumentError(
                "branch evidence does not reproduce at field $field"))
    end
    return true
end

function validate_ro_equilibrium_branch_evidence(
    evidence::RODynamicBranchEvidence,
    trace::RODynamicProtocolTrace,
    vector_field::ROPolynomialVectorField;
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    evidence.schema_version == RO_DYNAMIC_BRANCH_EVIDENCE_VERSION ||
        throw(ArgumentError("unsupported branch evidence version"))
    evidence.evidence_kind == :declarative_polynomial_equilibrium ||
        throw(ArgumentError(
            "equilibrium validator requires declarative polynomial evidence"))
    _rodh_payload_sha256(evidence.identity_payload) ==
        evidence.identity_sha256 || throw(ArgumentError(
            "branch evidence content hash mismatch"))
    recomputed = certify_ro_equilibrium_branch_evidence(
        trace,
        vector_field;
        residual_tolerance=evidence.residual_tolerance,
        stability_margin=evidence.stability_margin,
        limits=limits,
        cancel_check=cancel_check,
    )
    return _rodh_compare_branch_evidence(evidence, recomputed)
end

function validate_ro_dynamic_branch_evidence(
    evidence::RODynamicBranchEvidence,
    trace::RODynamicProtocolTrace;
    residual_evaluator,
    jacobian_evaluator,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    evidence.schema_version == RO_DYNAMIC_BRANCH_EVIDENCE_VERSION ||
        throw(ArgumentError("unsupported branch evidence version"))
    evidence.evidence_kind == :untrusted_callback || throw(ArgumentError(
        "callback validator cannot validate declarative equilibrium evidence"))
    _rodh_payload_sha256(evidence.identity_payload) ==
        evidence.identity_sha256 || throw(ArgumentError(
            "branch evidence content hash mismatch"))
    recomputed = certify_ro_dynamic_branch_evidence(
        trace;
        evidence_kind=evidence.candidate_kind,
        residual_evaluator=residual_evaluator,
        jacobian_evaluator=jacobian_evaluator,
        residual_tolerance=evidence.residual_tolerance,
        stability_margin=evidence.stability_margin,
        limits=limits,
        cancel_check=cancel_check,
    )
    return _rodh_compare_branch_evidence(evidence, recomputed)
end

struct _RODHSignedWindow
    start::Int
    stop::Int
    sign::Int
end

struct RODynamicHysteresisAnalysis
    schema_version::String
    analysis_identity_payload::NamedTuple
    analysis_identity_sha256::String
    forward_trace_sha256::String
    reverse_trace_sha256::String
    forward_branch_evidence_sha256::Union{Nothing,String}
    reverse_branch_evidence_sha256::Union{Nothing,String}
    equilibrium_vector_field_sha256::Union{Nothing,String}
    output_id::String
    status::Symbol
    evidence_class::Symbol
    finite_protocol_loop_detected::Bool
    finite_rate_lag_detected::Bool
    conditional_equilibrium_branch_loop_detected::Bool
    branch_switch_hysteresis_certified::Bool
    qualifies_as_dynamic_hysteresis::Bool
    complete_dynamic_reachability_evidence::Bool
    dynamic_reachability_status::Symbol
    protocol_relationship::Symbol
    closed_state_cycle::Bool
    complete_numeric_traces::Bool
    complete_model_residual_evidence::Bool
    complete_event_evidence::Bool
    complete_recomputed_branch_evidence::Bool
    complete_declarative_equilibrium_evidence::Bool
    exact_common_control_grid::Bool
    exact_common_controls_only::Bool
    interpolation_used::Bool
    matched_rate_pair::Bool
    rate_dependence_status::Symbol
    rate_lag_status::Symbol
    common_control_count::Int
    valid_common_comparison_count::Int
    gap_common_comparison_count::Int
    stable_common_comparison_count::Int
    separated_common_comparison_count::Int
    persistent_separation_window_count::Int
    best_persistent_control_window::Union{Nothing,NTuple{2,Float64}}
    separation_sign::Union{Nothing,Int}
    paired_branch_ids::Union{Nothing,NTuple{2,String}}
    max_stable_branch_separation::Union{Nothing,Float64}
    sampled_signed_loop_area::Union{Nothing,Float64}
    sampled_absolute_loop_area::Union{Nothing,Float64}
    forward_paired_switch_event_index::Union{Nothing,Int}
    reverse_paired_switch_event_index::Union{Nothing,Int}
    separation_threshold::Float64
    jump_threshold::Float64
    minimum_persistence_points::Int
    minimum_persistence_span::Float64
    closure_state_absolute_tolerances::Vector{Float64}
    closure_state_absolute_tolerance_units::Vector{String}
    closure_state_relative_tolerance::Float64
    branch_state_absolute_tolerances::Vector{Float64}
    branch_state_absolute_tolerance_units::Vector{String}
    branch_state_relative_tolerance::Float64
    rate_match_relative_tolerance::Float64
    rate_match_absolute_tolerance::Float64
    rate_match_absolute_tolerance_unit::String
    analysis_policy_sha256::String
    static_multiroot_claimed::Bool
    global_multistability_claimed::Bool
    experimental_causality_claimed::Bool
    evidence_scope::Symbol

    function RODynamicHysteresisAnalysis(
        ::_RODHValidatedToken,
        args...,
    )
        return new(args...)
    end
end

function _rodh_same_protocol_context(
    forward::RODynamicProtocolTrace,
    reverse::RODynamicProtocolTrace,
)
    comparisons = (
        (forward.model_identity, reverse.model_identity, "model_identity"),
        (forward.coordinate_chart_id, reverse.coordinate_chart_id,
            "coordinate_chart_id"),
        (forward.dynamics_policy_sha256,
            reverse.dynamics_policy_sha256,
            "dynamics_policy_sha256"),
        (forward.residual_policy_sha256,
            reverse.residual_policy_sha256,
            "residual_policy_sha256"),
        (forward.protocol_family_sha256,
            reverse.protocol_family_sha256,
            "protocol_family_sha256"),
        (forward.control_id, reverse.control_id, "control_id"),
        (forward.control_unit, reverse.control_unit, "control_unit"),
        (forward.time_unit, reverse.time_unit, "time_unit"),
        (forward.rate_unit, reverse.rate_unit, "rate_unit"),
        (forward.rate_relative_tolerance,
            reverse.rate_relative_tolerance,
            "rate_relative_tolerance"),
        (forward.rate_absolute_tolerance,
            reverse.rate_absolute_tolerance,
            "rate_absolute_tolerance"),
        (forward.rate_absolute_tolerance_unit,
            reverse.rate_absolute_tolerance_unit,
            "rate_absolute_tolerance_unit"),
        (forward.fixed_control_ids, reverse.fixed_control_ids,
            "fixed_control_ids"),
        (forward.fixed_control_units, reverse.fixed_control_units,
            "fixed_control_units"),
        (forward.fixed_control_values, reverse.fixed_control_values,
            "fixed_control_values"),
        (forward.state_ids, reverse.state_ids, "state_ids"),
        (forward.state_units, reverse.state_units, "state_units"),
        (forward.output_ids, reverse.output_ids, "output_ids"),
        (forward.output_units, reverse.output_units, "output_units"),
        (forward.model_solver_policy_sha256,
            reverse.model_solver_policy_sha256,
            "model_solver_policy_sha256"),
        (forward.trajectory_solver_method,
            reverse.trajectory_solver_method,
            "trajectory_solver_method"),
        (forward.trajectory_solver_policy_sha256,
            reverse.trajectory_solver_policy_sha256,
            "trajectory_solver_policy_sha256"),
        (forward.trajectory_solver_relative_tolerance,
            reverse.trajectory_solver_relative_tolerance,
            "trajectory_solver_relative_tolerance"),
        (forward.trajectory_solver_absolute_tolerance,
            reverse.trajectory_solver_absolute_tolerance,
            "trajectory_solver_absolute_tolerance"),
        (forward.stability_method,
            reverse.stability_method,
            "stability_method"),
        (forward.stability_policy_sha256,
            reverse.stability_policy_sha256,
            "stability_policy_sha256"),
        (forward.branch_policy_sha256,
            reverse.branch_policy_sha256,
            "branch_policy_sha256"),
        (forward.event_method, reverse.event_method, "event_method"),
        (forward.event_policy_sha256,
            reverse.event_policy_sha256,
            "event_policy_sha256"),
        (forward.dynamics_residual_tolerance,
            reverse.dynamics_residual_tolerance,
            "dynamics_residual_tolerance"),
        (forward.numerical_agreement_policy_sha256,
            reverse.numerical_agreement_policy_sha256,
            "numerical_agreement_policy_sha256"),
        (forward.numerical_agreement_tolerance,
            reverse.numerical_agreement_tolerance,
            "numerical_agreement_tolerance"),
    )
    for (left, right, label) in comparisons
        left == right || throw(ArgumentError(
            "forward and reverse traces must have identical $label"))
    end
    forward.direction == :increasing || throw(ArgumentError(
        "forward trace must have direction=:increasing"))
    reverse.direction == :decreasing || throw(ArgumentError(
        "reverse trace must have direction=:decreasing"))
    return nothing
end

function _rodh_exact_reverse_grid(
    forward::RODynamicProtocolTrace,
    reverse::RODynamicProtocolTrace,
    cancel_check,
)
    count = length(forward.control)
    length(reverse.control) == count || return false
    for index in 1:count
        _rodh_cancel(cancel_check)
        forward.control[index] ==
            reverse.control[count - index + 1] || return false
    end
    return true
end

function _rodh_push_window!(
    windows::Vector{_RODHSignedWindow},
    start::Int,
    stop::Int,
    sign::Int,
    controls::Vector{Float64},
    minimum_points::Int,
    minimum_span::Float64,
)
    start == 0 && return nothing
    stop - start + 1 >= minimum_points || return nothing
    controls[stop] - controls[start] >= minimum_span || return nothing
    push!(windows, _RODHSignedWindow(start, stop, sign))
    return nothing
end

function _rodh_signed_windows(
    eligible::BitVector,
    differences::Vector{Float64},
    controls::Vector{Float64},
    separation_threshold::Float64,
    minimum_points::Int,
    minimum_span::Float64,
    cancel_check,
)
    windows = _RODHSignedWindow[]
    start = 0
    current_sign = 0
    for index in eachindex(eligible)
        _rodh_cancel(cancel_check)
        separated = eligible[index] &&
            abs(differences[index]) >= separation_threshold
        point_sign = separated ? (differences[index] > 0.0 ? 1 : -1) : 0
        if !separated
            _rodh_push_window!(
                windows,
                start,
                index - 1,
                current_sign,
                controls,
                minimum_points,
                minimum_span,
            )
            start = 0
            current_sign = 0
        elseif start == 0
            start = index
            current_sign = point_sign
        elseif point_sign != current_sign
            _rodh_push_window!(
                windows,
                start,
                index - 1,
                current_sign,
                controls,
                minimum_points,
                minimum_span,
            )
            start = index
            current_sign = point_sign
        end
    end
    _rodh_push_window!(
        windows,
        start,
        length(eligible),
        current_sign,
        controls,
        minimum_points,
        minimum_span,
    )
    return windows
end

function _rodh_best_window(
    windows::Vector{_RODHSignedWindow},
    controls::Vector{Float64},
    cancel_check,
)
    isempty(windows) && return nothing
    best = first(windows)
    best_span = controls[best.stop] - controls[best.start]
    for window in @view(windows[2:end])
        _rodh_cancel(cancel_check)
        span = controls[window.stop] - controls[window.start]
        if span > best_span
            best = window
            best_span = span
        end
    end
    return best
end

@inline function _rodh_state_tolerance(
    left::Float64,
    right::Float64,
    absolute_tolerance::Float64,
    relative_tolerance::Float64,
)
    return absolute_tolerance + relative_tolerance *
        max(abs(left), abs(right))
end

function _rodh_state_rows_close(
    left,
    right,
    absolute_tolerances::Vector{Float64},
    relative_tolerance::Float64,
)
    length(left) == length(right) == length(absolute_tolerances) ||
        return false
    return all(
        abs(Float64(left[index]) - Float64(right[index])) <=
            _rodh_state_tolerance(
                Float64(left[index]),
                Float64(right[index]),
                absolute_tolerances[index],
                relative_tolerance,
            )
        for index in eachindex(absolute_tolerances)
    )
end

@inline function _rodh_state_rows_distinct(
    left,
    right,
    absolute_tolerances::Vector{Float64},
    relative_tolerance::Float64,
)
    return !_rodh_state_rows_close(
        left, right, absolute_tolerances, relative_tolerance)
end

function _rodh_window_has_distinct_recomputed_stable_states(
    window::_RODHSignedWindow,
    forward::RODynamicProtocolTrace,
    reverse::RODynamicProtocolTrace,
    forward_evidence::RODynamicBranchEvidence,
    reverse_evidence::RODynamicBranchEvidence,
    state_absolute_tolerances::Vector{Float64},
    state_relative_tolerance::Float64,
    cancel_check,
)
    point_count = length(forward.control)
    for index in window.start:window.stop
        _rodh_cancel(cancel_check)
        reverse_index = point_count - index + 1
        forward_evidence.stable[index] || return false
        reverse_evidence.stable[reverse_index] || return false
        _rodh_state_rows_distinct(
            @view(forward.states[index, :]),
            @view(reverse.states[reverse_index, :]),
            state_absolute_tolerances,
            state_relative_tolerance,
        ) || return false
    end
    return true
end

function _rodh_event_output_jump(
    trace::RODynamicProtocolTrace,
    event::RODynamicSwitchEvent,
    output_index::Int,
)
    return abs(
        trace.outputs[event.right_index, output_index] -
        trace.outputs[event.left_index, output_index])
end

function _rodh_recomputed_branch_event_coverage(
    trace::RODynamicProtocolTrace,
    evidence::RODynamicBranchEvidence,
    output_index::Int,
    jump_threshold::Float64,
    state_absolute_tolerances::Vector{Float64},
    state_relative_tolerance::Float64,
    cancel_check,
)
    verified_switch_edges = Int[]
    for event in trace.events
        _rodh_cancel(cancel_check)
        event.status == :verified || continue
        event.event_type == :branch_switch || continue
        push!(verified_switch_edges, event.left_index)
    end
    allunique(verified_switch_edges) || return false
    verified_switch_edge_set = Set(verified_switch_edges)
    for left in 1:(length(trace.control) - 1)
        _rodh_cancel(cancel_check)
        numeric_switch = evidence.stable[left] && evidence.stable[left + 1] &&
            abs(trace.outputs[left + 1, output_index] -
                trace.outputs[left, output_index]) >= jump_threshold &&
            _rodh_state_rows_distinct(
                @view(trace.states[left, :]),
                @view(trace.states[left + 1, :]),
                state_absolute_tolerances,
                state_relative_tolerance,
            )
        numeric_switch == (left in verified_switch_edge_set) || return false
    end
    return true
end

@inline function _rodh_event_touches(
    event::RODynamicSwitchEvent,
    control_value::Float64,
)
    return event.control_interval[1] - event.control_uncertainty <=
        control_value <=
        event.control_interval[2] + event.control_uncertainty
end

function _rodh_pair_branch_switch_events(
    windows::Vector{_RODHSignedWindow},
    controls::Vector{Float64},
    forward::RODynamicProtocolTrace,
    reverse::RODynamicProtocolTrace,
    forward_evidence::RODynamicBranchEvidence,
    reverse_evidence::RODynamicBranchEvidence,
    output_index::Int,
    jump_threshold::Float64,
    branch_state_absolute_tolerances::Vector{Float64},
    branch_state_relative_tolerance::Float64,
    closure_state_absolute_tolerances::Vector{Float64},
    closure_state_relative_tolerance::Float64,
    cancel_check,
)
    best_window = nothing
    best_pair = nothing
    best_forward_event = nothing
    best_reverse_event = nothing
    best_span = -Inf
    point_count = length(forward.control)
    for window in windows
        _rodh_cancel(cancel_check)
        _rodh_window_has_distinct_recomputed_stable_states(
            window,
            forward,
            reverse,
            forward_evidence,
            reverse_evidence,
            branch_state_absolute_tolerances,
            branch_state_relative_tolerance,
            cancel_check,
        ) || continue
        upper_boundary = controls[window.stop]
        lower_boundary = controls[window.start]
        for forward_event_index in eachindex(forward.events)
            _rodh_cancel(cancel_check)
            forward_event = forward.events[forward_event_index]
            forward_event.status == :verified || continue
            forward_event.event_type == :branch_switch || continue
            forward_event.left_index == window.stop || continue
            _rodh_event_touches(forward_event, upper_boundary) || continue
            _rodh_event_output_jump(
                forward, forward_event, output_index) >= jump_threshold ||
                continue
            _rodh_state_rows_distinct(
                @view(forward.states[forward_event.left_index, :]),
                @view(forward.states[forward_event.right_index, :]),
                branch_state_absolute_tolerances,
                branch_state_relative_tolerance,
            ) || continue
            reverse_after_forward =
                point_count - forward_event.right_index + 1
            _rodh_state_rows_close(
                @view(forward.states[forward_event.right_index, :]),
                @view(reverse.states[reverse_after_forward, :]),
                closure_state_absolute_tolerances,
                closure_state_relative_tolerance,
            ) || continue
            for reverse_event_index in eachindex(reverse.events)
                _rodh_cancel(cancel_check)
                reverse_event = reverse.events[reverse_event_index]
                reverse_event.status == :verified || continue
                reverse_event.event_type == :branch_switch || continue
                reverse_event.left_index ==
                    point_count - window.start + 1 || continue
                _rodh_event_touches(reverse_event, lower_boundary) || continue
                _rodh_event_output_jump(
                    reverse, reverse_event, output_index) >= jump_threshold ||
                    continue
                _rodh_state_rows_distinct(
                    @view(reverse.states[reverse_event.left_index, :]),
                    @view(reverse.states[reverse_event.right_index, :]),
                    branch_state_absolute_tolerances,
                    branch_state_relative_tolerance,
                ) || continue
                forward_after_reverse =
                    point_count - reverse_event.right_index + 1
                _rodh_state_rows_close(
                    @view(reverse.states[reverse_event.right_index, :]),
                    @view(forward.states[forward_after_reverse, :]),
                    closure_state_absolute_tolerances,
                    closure_state_relative_tolerance,
                ) || continue
                span = upper_boundary - lower_boundary
                if span > best_span
                    best_span = span
                    best_window = window
                    best_pair = forward_event.pre_branch_id === nothing ?
                        nothing : (
                            String(forward_event.pre_branch_id),
                            String(forward_event.post_branch_id),
                        )
                    best_forward_event = forward_event_index
                    best_reverse_event = reverse_event_index
                end
            end
        end
    end
    return best_window, best_pair, best_forward_event, best_reverse_event
end

function _rodh_sampled_loop_areas(
    controls::Vector{Float64},
    differences::Vector{Float64},
    complete_mask::BitVector,
    cancel_check,
)
    length(controls) >= 2 && all(complete_mask) ||
        return nothing, nothing
    signed_area = 0.0
    absolute_area = 0.0
    for index in 1:(length(controls) - 1)
        _rodh_cancel(cancel_check)
        width = controls[index + 1] - controls[index]
        signed_area += width *
            (differences[index] + differences[index + 1]) / 2
        absolute_area += width *
            (abs(differences[index]) + abs(differences[index + 1])) / 2
    end
    return signed_area, absolute_area
end

function _rodh_state_tolerance_policy(
    values,
    units,
    expected_units::Vector{String},
    label::AbstractString,
    maximum_absolute_tolerance::Float64,
)
    values isa AbstractVector || throw(ArgumentError(
        "$label must be a vector"))
    length(values) == length(expected_units) || throw(DimensionMismatch(
        "$label must contain one value per ordered state"))
    all(value -> value isa Float64, values) || throw(ArgumentError(
        "$label must contain strict Float64 values"))
    converted = Vector{Float64}(values)
    for value in converted
        isfinite(value) || throw(ArgumentError(
            "$label must contain finite values"))
        value >= 0.0 && !(value == 0.0 && signbit(value)) ||
            throw(ArgumentError(
                "$label must contain non-negative, non-negative-zero values"))
        value <= maximum_absolute_tolerance || throw(ArgumentError(
            "$label exceeds the solver-scaled strong-evidence bound"))
    end
    unit_values = _rodh_strings(units, "$label units")
    unit_values == expected_units || throw(ArgumentError(
        "$label units must exactly match ordered state_units"))
    return converted, unit_values
end

function analyze_dynamic_hysteresis(
    raw_forward::RODynamicProtocolTrace,
    raw_reverse::RODynamicProtocolTrace;
    output_id,
    separation_threshold,
    jump_threshold,
    minimum_persistence_points::Integer=3,
    minimum_persistence_span,
    closure_state_absolute_tolerances,
    closure_state_absolute_tolerance_units,
    closure_state_relative_tolerance=1e-9,
    branch_state_absolute_tolerances,
    branch_state_absolute_tolerance_units,
    branch_state_relative_tolerance=1e-9,
    forward_branch_evidence::Union{Nothing,RODynamicBranchEvidence}=nothing,
    reverse_branch_evidence::Union{Nothing,RODynamicBranchEvidence}=nothing,
    equilibrium_vector_field::Union{Nothing,ROPolynomialVectorField}=nothing,
    branch_residual_evaluator=nothing,
    branch_jacobian_evaluator=nothing,
    alignment::Symbol=:exact_complete_reverse_grid,
    rate_match_relative_tolerance=1e-9,
    rate_match_absolute_tolerance=0.0,
    rate_match_absolute_tolerance_unit,
    analysis_policy_sha256=
        RO_DYNAMIC_HYSTERESIS_ANALYZER_POLICY_SHA256,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    alignment == :exact_complete_reverse_grid || throw(ArgumentError(
        "v1 requires alignment=:exact_complete_reverse_grid"))
    _rodh_limit(
        :common_control_comparisons,
        max(length(raw_forward.control), length(raw_reverse.control)),
        limits.max_common_comparisons,
    )
    forward = _rodh_normalize_trace(raw_forward, limits, cancel_check)
    reverse = _rodh_normalize_trace(raw_reverse, limits, cancel_check)
    _rodh_same_protocol_context(forward, reverse)
    _rodh_cancel(cancel_check)

    selected_output = _rodh_nonempty_string(output_id, "output_id")
    output_index = findfirst(==(selected_output), forward.output_ids)
    output_index === nothing && throw(ArgumentError(
        "output_id is not present in the ordered output identity"))
    separation = _rodh_strict_positive_float(
        separation_threshold, "separation_threshold")
    jump = _rodh_strict_positive_float(
        jump_threshold, "jump_threshold")
    persistence_span = _rodh_strict_positive_float(
        minimum_persistence_span, "minimum_persistence_span")
    minimum_persistence_points isa Bool && throw(ArgumentError(
        "minimum_persistence_points must be an integer"))
    persistence_points = try
        Int(minimum_persistence_points)
    catch
        throw(ArgumentError(
            "minimum_persistence_points must fit in Int"))
    end
    persistence_points >= 2 || throw(ArgumentError(
        "minimum_persistence_points must be at least two"))

    solver_scaled_tolerance_bound =
        _RODH_MAX_SOLVER_ABSOLUTE_TOLERANCE_MULTIPLIER * max(
            forward.trajectory_solver_absolute_tolerance,
            reverse.trajectory_solver_absolute_tolerance,
        )
    isfinite(solver_scaled_tolerance_bound) || throw(ArgumentError(
        "solver-scaled state-tolerance bound must remain finite"))
    closure_atols, closure_atol_units = _rodh_state_tolerance_policy(
        closure_state_absolute_tolerances,
        closure_state_absolute_tolerance_units,
        forward.state_units,
        "closure_state_absolute_tolerances",
        solver_scaled_tolerance_bound,
    )
    closure_rtol = _rodh_strict_nonnegative_float(
        closure_state_relative_tolerance,
        "closure_state_relative_tolerance",
    )
    0.0 <= closure_rtol <= _RODH_MAX_CLOSURE_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "closure_state_relative_tolerance is outside the strong-evidence range"))
    branch_atols, branch_atol_units = _rodh_state_tolerance_policy(
        branch_state_absolute_tolerances,
        branch_state_absolute_tolerance_units,
        forward.state_units,
        "branch_state_absolute_tolerances",
        solver_scaled_tolerance_bound,
    )
    branch_rtol = _rodh_strict_nonnegative_float(
        branch_state_relative_tolerance,
        "branch_state_relative_tolerance",
    )
    0.0 <= branch_rtol <=
        _RODH_MAX_STATE_COMPARISON_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "branch_state_relative_tolerance is outside the strong-evidence range"))
    rate_match_rtol = _rodh_strict_nonnegative_float(
        rate_match_relative_tolerance,
        "rate_match_relative_tolerance",
    )
    0.0 <= rate_match_rtol <= _RODH_MAX_RATE_RELATIVE_TOLERANCE ||
        throw(ArgumentError(
            "rate_match_relative_tolerance is outside the supported range"))
    rate_match_atol = _rodh_strict_nonnegative_float(
        rate_match_absolute_tolerance,
        "rate_match_absolute_tolerance",
    )
    rate_match_atol_unit = _rodh_nonempty_string(
        rate_match_absolute_tolerance_unit,
        "rate_match_absolute_tolerance_unit",
    )
    rate_match_atol_unit == forward.rate_unit || throw(ArgumentError(
        "rate_match_absolute_tolerance_unit must equal trace rate_unit"))
    rate_match_atol <= min(
        forward.rate_absolute_tolerance,
        reverse.rate_absolute_tolerance,
    ) || throw(ArgumentError(
        "rate_match_absolute_tolerance must not exceed both traces' " *
        "declared measurement resolution"))
    rate_match_atol <= min(forward.rate, reverse.rate) *
        _RODH_MAX_RATE_RELATIVE_TOLERANCE || throw(ArgumentError(
            "rate_match_absolute_tolerance exceeds the supported relative rate scale"))
    analysis_policy = _rodh_sha256_id(
        analysis_policy_sha256, "analysis_policy_sha256")

    forward_hash = ro_dynamic_protocol_identity_sha256(
        forward; limits=limits, cancel_check=cancel_check)
    reverse_hash = ro_dynamic_protocol_identity_sha256(
        reverse; limits=limits, cancel_check=cancel_check)
    one_branch_evidence_missing =
        (forward_branch_evidence === nothing) !=
        (reverse_branch_evidence === nothing)
    one_branch_evidence_missing && throw(ArgumentError(
        "forward and reverse branch evidence must be supplied together"))
    branch_evidence_supplied = forward_branch_evidence !== nothing
    !branch_evidence_supplied && equilibrium_vector_field !== nothing &&
        throw(ArgumentError(
            "equilibrium_vector_field requires paired branch evidence"))
    declarative_revalidation = branch_evidence_supplied &&
        equilibrium_vector_field !== nothing
    callback_revalidation = branch_evidence_supplied &&
        equilibrium_vector_field === nothing
    if declarative_revalidation
        branch_residual_evaluator === nothing &&
            branch_jacobian_evaluator === nothing || throw(ArgumentError(
                "declarative equilibrium revalidation does not accept " *
                "caller callbacks"))
        validate_ro_polynomial_vector_field(
            equilibrium_vector_field;
            limits=limits,
            cancel_check=cancel_check,
        )
    end
    branch_point_evaluation_work = !branch_evidence_supplied ? BigInt(0) :
        declarative_revalidation ?
            BigInt(equilibrium_vector_field.evaluation_work_units) :
            BigInt(length(forward.state_ids) + 1)
    branch_revalidation_work = branch_evidence_supplied ?
        BigInt(length(forward.control) + length(reverse.control)) *
            (BigInt(length(forward.state_ids))^3 +
                branch_point_evaluation_work + 1) : BigInt(0)
    _rodh_limit(
        :analysis_work_units,
        branch_revalidation_work +
            BigInt(max(length(forward.control), length(reverse.control))) * 8 +
            BigInt(length(forward.events) + length(reverse.events)),
        limits.max_analysis_work_units,
    )
    declarative_validation_performed = false
    callback_validation_performed = false
    if declarative_revalidation
        validate_ro_equilibrium_branch_evidence(
            forward_branch_evidence,
            forward,
            equilibrium_vector_field;
            limits=limits,
            cancel_check=cancel_check,
        )
        validate_ro_equilibrium_branch_evidence(
            reverse_branch_evidence,
            reverse,
            equilibrium_vector_field;
            limits=limits,
            cancel_check=cancel_check,
        )
        declarative_validation_performed = true
    elseif callback_revalidation
        branch_residual_evaluator isa Function || throw(ArgumentError(
            "revalidating callback evidence requires residual_evaluator"))
        branch_jacobian_evaluator isa Function || throw(ArgumentError(
            "revalidating callback evidence requires jacobian_evaluator"))
        validate_ro_dynamic_branch_evidence(
            forward_branch_evidence,
            forward;
            residual_evaluator=branch_residual_evaluator,
            jacobian_evaluator=branch_jacobian_evaluator,
            limits=limits,
            cancel_check=cancel_check,
        )
        validate_ro_dynamic_branch_evidence(
            reverse_branch_evidence,
            reverse;
            residual_evaluator=branch_residual_evaluator,
            jacobian_evaluator=branch_jacobian_evaluator,
            limits=limits,
            cancel_check=cancel_check,
        )
        callback_validation_performed = true
    end
    forward_branch_hash = branch_evidence_supplied ?
        forward_branch_evidence.identity_sha256 : nothing
    reverse_branch_hash = branch_evidence_supplied ?
        reverse_branch_evidence.identity_sha256 : nothing
    equilibrium_vector_field_hash = declarative_validation_performed ?
        equilibrium_vector_field.identity_sha256 : nothing
    complete_branch_evidence = branch_evidence_supplied &&
        forward_branch_evidence.complete &&
        reverse_branch_evidence.complete &&
        forward_branch_evidence.all_stable &&
        reverse_branch_evidence.all_stable
    equilibrium_branch_evidence = complete_branch_evidence &&
        declarative_validation_performed &&
        forward_branch_evidence.strong_equilibrium_certified &&
        reverse_branch_evidence.strong_equilibrium_certified &&
        forward_branch_evidence.vector_field_sha256 ==
            equilibrium_vector_field.identity_sha256 &&
        reverse_branch_evidence.vector_field_sha256 ==
            equilibrium_vector_field.identity_sha256
    protocol_dynamic_evidence = complete_branch_evidence &&
        callback_validation_performed &&
        forward_branch_evidence.candidate_kind == :protocol_dynamics &&
        reverse_branch_evidence.candidate_kind == :protocol_dynamics

    numeric_complete =
        all(forward.numeric_validity) && all(reverse.numeric_validity)
    residual_complete =
        all(forward.residual_validity) && all(reverse.residual_validity)
    declared_event_complete =
        forward.event_detection_status == :complete &&
        reverse.event_detection_status == :complete
    recomputed_event_complete = equilibrium_branch_evidence && (
        _rodh_recomputed_branch_event_coverage(
            forward,
            forward_branch_evidence,
            output_index,
            jump,
            branch_atols,
            branch_rtol,
            cancel_check,
        ) && _rodh_recomputed_branch_event_coverage(
            reverse,
            reverse_branch_evidence,
            output_index,
            jump,
            branch_atols,
            branch_rtol,
            cancel_check,
        )
    )
    event_complete = equilibrium_branch_evidence &&
        declared_event_complete && recomputed_event_complete
    exact_grid = _rodh_exact_reverse_grid(
        forward, reverse, cancel_check)
    matched_rate = _rodh_rate_matches(
        forward.rate,
        reverse.rate,
        rate_match_rtol,
        rate_match_atol,
    )
    protocol_relationship =
        reverse.lineage_predecessor_trace_sha256 === nothing ?
            :independent_scans :
        reverse.lineage_predecessor_trace_sha256 == forward_hash ?
            :forward_to_reverse :
            :lineage_mismatch
    closed_state_cycle = _rodh_state_rows_close(
        reverse.initial_state,
        @view(forward.states[end, :]),
        closure_atols,
        closure_rtol,
    ) && _rodh_state_rows_close(
        @view(reverse.states[end, :]),
        forward.initial_state,
        closure_atols,
        closure_rtol,
    )

    controls = exact_grid ? copy(forward.control) : Float64[]
    common_count = length(controls)
    valid_mask = falses(common_count)
    stable_mask = falses(common_count)
    separated_stable_mask = falses(common_count)
    differences = zeros(Float64, common_count)
    for comparison in 1:common_count
        _rodh_cancel(cancel_check)
        reverse_point = common_count - comparison + 1
        valid_mask[comparison] =
            forward.validity[comparison] &&
            reverse.validity[reverse_point]
        valid_mask[comparison] || continue
        stable_mask[comparison] = branch_evidence_supplied &&
            forward_branch_evidence.stable[comparison] &&
            reverse_branch_evidence.stable[reverse_point]
        differences[comparison] =
            forward.outputs[comparison, output_index] -
            reverse.outputs[reverse_point, output_index]
        separated_stable_mask[comparison] =
            stable_mask[comparison] &&
            abs(differences[comparison]) >= separation
    end
    raw_windows = _rodh_signed_windows(
        valid_mask,
        differences,
        controls,
        separation,
        persistence_points,
        persistence_span,
        cancel_check,
    )
    best_window = _rodh_best_window(
        raw_windows, controls, cancel_check)
    event_pair_comparisons = equilibrium_branch_evidence ?
        BigInt(length(raw_windows)) * length(forward.events) *
            length(reverse.events) : BigInt(0)
    _rodh_limit(
        :event_pair_comparisons,
        event_pair_comparisons,
        limits.max_event_pair_comparisons,
    )
    analysis_work_units = branch_revalidation_work +
        BigInt(common_count) * 8 + BigInt(length(raw_windows)) +
        BigInt(length(forward.events) + length(reverse.events)) +
        event_pair_comparisons
    _rodh_limit(
        :analysis_work_units,
        analysis_work_units,
        limits.max_analysis_work_units,
    )
    paired_window, paired_branches, paired_forward_event,
        paired_reverse_event = equilibrium_branch_evidence ?
            _rodh_pair_branch_switch_events(
                raw_windows,
                controls,
                forward,
                reverse,
                forward_branch_evidence,
                reverse_branch_evidence,
                output_index,
                jump,
                branch_atols,
                branch_rtol,
                closure_atols,
                closure_rtol,
                cancel_check,
            ) : (nothing, nothing, nothing, nothing)
    signed_area, absolute_area = _rodh_sampled_loop_areas(
        controls,
        differences,
        valid_mask .& stable_mask,
        cancel_check,
    )
    max_stable_separation = nothing
    for index in eachindex(differences)
        _rodh_cancel(cancel_check)
        stable_mask[index] || continue
        value = abs(differences[index])
        max_stable_separation = max_stable_separation === nothing ?
            value : max(max_stable_separation, value)
    end

    finite_loop_ready = numeric_complete && residual_complete &&
        exact_grid && matched_rate &&
        protocol_relationship == :forward_to_reverse &&
        closed_state_cycle && !isempty(raw_windows)
    conditional_equilibrium_branch_loop = finite_loop_ready &&
        equilibrium_branch_evidence && event_complete &&
        paired_window !== nothing
    complete_dynamic_reachability_evidence = false
    dynamic_reachability_status = :not_implemented
    paired_branch_hysteresis = false
    finite_rate_lag = finite_loop_ready && protocol_dynamic_evidence

    status = if !numeric_complete
        :unknown_numeric_gap
    elseif !residual_complete
        :unknown_model_residual_evidence
    elseif !exact_grid
        :unknown_incomplete_common_control_grid
    elseif !matched_rate
        :incomparable_protocol_rates
    elseif protocol_relationship == :independent_scans
        :independent_scans_not_a_loop
    elseif protocol_relationship == :lineage_mismatch
        :unknown_protocol_lineage
    elseif !closed_state_cycle
        :protocol_cycle_not_closed
    elseif isempty(raw_windows)
        :no_finite_protocol_loop_on_tested_cycle
    elseif equilibrium_branch_evidence && !event_complete
        :unknown_event_evidence
    elseif conditional_equilibrium_branch_loop
        :conditional_equilibrium_branch_loop_on_tested_grid
    elseif finite_rate_lag
        :finite_rate_lag_candidate_from_untrusted_callback
    elseif branch_evidence_supplied && !complete_branch_evidence
        :unknown_recomputed_branch_evidence
    elseif !branch_evidence_supplied
        :finite_protocol_loop_without_recomputed_branch_evidence
    else
        :finite_protocol_loop_without_branch_switch_certificate
    end
    evidence_class = conditional_equilibrium_branch_loop ?
        :conditional_equilibrium_branch_loop :
        finite_rate_lag ? :finite_rate_lag_candidate :
        finite_loop_ready ? :finite_protocol_loop :
        status == :no_finite_protocol_loop_on_tested_cycle ? :no_loop :
        :unknown
    rate_lag_status = finite_rate_lag ?
        :finite_rate_lag_candidate_from_untrusted_callback :
        :not_established
    reported_window = conditional_equilibrium_branch_loop ?
        paired_window : best_window
    best_control_window = reported_window === nothing ? nothing :
        (controls[reported_window.start], controls[reported_window.stop])
    separation_sign = reported_window === nothing ? nothing :
        reported_window.sign

    identity_payload = (
        schema_version=RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION,
        analysis_policy_sha256=analysis_policy,
        forward_trace_sha256=forward_hash,
        reverse_trace_sha256=reverse_hash,
        forward_branch_evidence_sha256=forward_branch_hash,
        reverse_branch_evidence_sha256=reverse_branch_hash,
        equilibrium_vector_field_sha256=equilibrium_vector_field_hash,
        output_id=selected_output,
        alignment=String(alignment),
        separation_threshold=separation,
        jump_threshold=jump,
        minimum_persistence_points=persistence_points,
        minimum_persistence_span=persistence_span,
        closure_state_absolute_tolerances=Tuple(closure_atols),
        closure_state_absolute_tolerance_units=Tuple(closure_atol_units),
        closure_state_relative_tolerance=closure_rtol,
        branch_state_absolute_tolerances=Tuple(branch_atols),
        branch_state_absolute_tolerance_units=Tuple(branch_atol_units),
        branch_state_relative_tolerance=branch_rtol,
        rate_match_relative_tolerance=rate_match_rtol,
        rate_match_absolute_tolerance=rate_match_atol,
        rate_match_absolute_tolerance_unit=rate_match_atol_unit,
        result=(
            status=String(status),
            evidence_class=String(evidence_class),
            finite_protocol_loop_detected=finite_loop_ready,
            finite_rate_lag_detected=finite_rate_lag,
            conditional_equilibrium_branch_loop_detected=
                conditional_equilibrium_branch_loop,
            branch_switch_hysteresis_certified=
                paired_branch_hysteresis,
            qualifies_as_dynamic_hysteresis=paired_branch_hysteresis,
            complete_dynamic_reachability_evidence=
                complete_dynamic_reachability_evidence,
            dynamic_reachability_status=
                String(dynamic_reachability_status),
            protocol_relationship=String(protocol_relationship),
            closed_state_cycle=closed_state_cycle,
            complete_numeric_traces=numeric_complete,
            complete_model_residual_evidence=residual_complete,
            complete_event_evidence=event_complete,
            complete_recomputed_branch_evidence=
                complete_branch_evidence,
            complete_declarative_equilibrium_evidence=
                equilibrium_branch_evidence,
            exact_common_control_grid=exact_grid,
            exact_common_controls_only=exact_grid,
            interpolation_used=false,
            matched_rate_pair=matched_rate,
            rate_dependence_status="not_tested_across_rates",
            rate_lag_status=String(rate_lag_status),
            common_control_count=common_count,
            valid_common_comparison_count=count(valid_mask),
            gap_common_comparison_count=
                common_count - count(valid_mask),
            stable_common_comparison_count=count(stable_mask),
            separated_common_comparison_count=
                count(separated_stable_mask),
            persistent_separation_window_count=length(raw_windows),
            best_persistent_control_window=best_control_window,
            separation_sign=separation_sign,
            paired_branch_ids=paired_branches,
            max_stable_branch_separation=max_stable_separation,
            sampled_signed_loop_area=signed_area,
            sampled_absolute_loop_area=absolute_area,
            forward_paired_switch_event_index=paired_forward_event,
            reverse_paired_switch_event_index=paired_reverse_event,
            static_multiroot_claimed=false,
            global_multistability_claimed=false,
            experimental_causality_claimed=false,
            evidence_scope=String(
                RO_DYNAMIC_HYSTERESIS_EVIDENCE_SCOPE),
        ),
    )
    identity_hash = _rodh_payload_sha256(identity_payload)
    _rodh_cancel(cancel_check)
    return RODynamicHysteresisAnalysis(
        _RODH_VALIDATED_TOKEN,
        RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION,
        identity_payload,
        identity_hash,
        forward_hash,
        reverse_hash,
        forward_branch_hash,
        reverse_branch_hash,
        equilibrium_vector_field_hash,
        selected_output,
        status,
        evidence_class,
        finite_loop_ready,
        finite_rate_lag,
        conditional_equilibrium_branch_loop,
        paired_branch_hysteresis,
        paired_branch_hysteresis,
        complete_dynamic_reachability_evidence,
        dynamic_reachability_status,
        protocol_relationship,
        closed_state_cycle,
        numeric_complete,
        residual_complete,
        event_complete,
        complete_branch_evidence,
        equilibrium_branch_evidence,
        exact_grid,
        exact_grid,
        false,
        matched_rate,
        :not_tested_across_rates,
        rate_lag_status,
        common_count,
        count(valid_mask),
        common_count - count(valid_mask),
        count(stable_mask),
        count(separated_stable_mask),
        length(raw_windows),
        best_control_window,
        separation_sign,
        paired_branches,
        max_stable_separation,
        signed_area,
        absolute_area,
        paired_forward_event,
        paired_reverse_event,
        separation,
        jump,
        persistence_points,
        persistence_span,
        copy(closure_atols),
        copy(closure_atol_units),
        closure_rtol,
        copy(branch_atols),
        copy(branch_atol_units),
        branch_rtol,
        rate_match_rtol,
        rate_match_atol,
        rate_match_atol_unit,
        analysis_policy,
        false,
        false,
        false,
        RO_DYNAMIC_HYSTERESIS_EVIDENCE_SCOPE,
    )
end

function validate_ro_dynamic_hysteresis_analysis(
    analysis::RODynamicHysteresisAnalysis,
    forward::RODynamicProtocolTrace,
    reverse::RODynamicProtocolTrace;
    kwargs...,
)
    analysis.schema_version == RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION ||
        throw(ArgumentError("unsupported dynamic hysteresis analysis version"))
    _rodh_payload_sha256(analysis.analysis_identity_payload) ==
        analysis.analysis_identity_sha256 || throw(ArgumentError(
            "dynamic hysteresis analysis content hash mismatch"))
    recomputed = analyze_dynamic_hysteresis(
        forward,
        reverse;
        kwargs...,
    )
    for field in fieldnames(RODynamicHysteresisAnalysis)
        getfield(analysis, field) == getfield(recomputed, field) ||
            throw(ArgumentError(
                "dynamic hysteresis analysis does not reproduce at field $field"))
    end
    return true
end

struct ROStaticMultirootEvidence
    schema_version::String
    identity_payload::NamedTuple
    identity_sha256::String
    status::Symbol
    root_count::Int
    valid_root_count::Int
    stable_root_count::Int
    distinct_stable_root_pair_count::Int
    qualifies_as_dynamic_hysteresis::Bool
    global_multistability_claimed::Bool
    experimental_causality_claimed::Bool
    evidence_scope::Symbol

    function ROStaticMultirootEvidence(
        ::_RODHValidatedToken,
        args...,
    )
        return new(args...)
    end
end

function analyze_static_multiroot(
    ;
    model_identity,
    coordinate_chart_id,
    control_id,
    control_unit,
    control_value::Real,
    state_ids,
    state_units,
    output_ids,
    output_units,
    states,
    outputs,
    solver_status,
    local_stability,
    distinct_root_threshold=1e-8,
    limits::RODynamicEvidenceLimits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    state_count = _rodh_ordered_count(state_ids, "state_ids")
    output_count = _rodh_ordered_count(output_ids, "output_ids")
    state_count > 0 || throw(ArgumentError("state_ids must not be empty"))
    output_count > 0 || throw(ArgumentError("output_ids must not be empty"))
    _rodh_limit(:state_dimensions, state_count, limits.max_states)
    _rodh_limit(:output_dimensions, output_count, limits.max_outputs)
    _rodh_ordered_count(state_units, "state_units") == state_count ||
        throw(DimensionMismatch("state_units must match state_ids"))
    _rodh_ordered_count(output_units, "output_units") == output_count ||
        throw(DimensionMismatch("output_units must match output_ids"))
    states isa AbstractMatrix || throw(ArgumentError(
        "states must be a matrix"))
    outputs isa AbstractMatrix || throw(ArgumentError(
        "outputs must be a matrix"))
    root_count = size(states, 1)
    root_count >= 1 || throw(ArgumentError(
        "at least one supplied root is required"))
    _rodh_limit(
        :points_per_trace, root_count, limits.max_points_per_trace)
    size(states, 2) == state_count || throw(DimensionMismatch(
        "states columns must match state_ids"))
    size(outputs) == (root_count, output_count) ||
        throw(DimensionMismatch(
            "outputs must have root_count x output_count shape"))
    _rodh_ordered_count(solver_status, "solver_status") == root_count ||
        throw(DimensionMismatch(
            "solver_status must have one entry per root"))
    _rodh_ordered_count(local_stability, "local_stability") == root_count ||
        throw(DimensionMismatch(
            "local_stability must have one entry per root"))
    payload_scalars = BigInt(root_count) *
        (BigInt(state_count) + BigInt(output_count))
    _rodh_limit(
        :payload_scalars, payload_scalars, limits.max_payload_scalars)
    possible_pairs = BigInt(root_count) * BigInt(root_count - 1) ÷ 2
    _rodh_limit(
        :static_root_comparisons,
        possible_pairs,
        limits.max_common_comparisons,
    )
    static_work_units = BigInt(root_count) *
        (BigInt(state_count) + output_count + 1) +
        possible_pairs * state_count
    _rodh_limit(
        :static_root_work_units,
        static_work_units,
        limits.max_analysis_work_units,
    )

    state_names = _rodh_strings(state_ids, "state_ids"; unique=true)
    output_names = _rodh_strings(output_ids, "output_ids"; unique=true)
    state_unit_values = _rodh_strings(state_units, "state_units")
    output_unit_values = _rodh_strings(output_units, "output_units")
    state_values = _rodh_evidence_matrix(states, "states")
    output_values = _rodh_evidence_matrix(outputs, "outputs")
    solver_values = _rodh_symbol_vector(
        solver_status, "solver_status", _RODH_SOLVER_STATUSES)
    stability_values = _rodh_symbol_vector(
        local_stability, "local_stability", _RODH_STABILITY_STATUSES)
    threshold = _rodh_strict_positive_float(
        distinct_root_threshold, "distinct_root_threshold")
    validity = falses(root_count)
    for root in 1:root_count
        _rodh_cancel(cancel_check)
        validity[root] = solver_values[root] == :success &&
            all(isfinite, @view(state_values[root, :])) &&
            all(isfinite, @view(output_values[root, :]))
    end
    stable = validity .& (stability_values .== :stable)
    distinct_pairs = 0
    stable_indices = findall(stable)
    for left_position in 1:max(length(stable_indices) - 1, 0)
        _rodh_cancel(cancel_check)
        left = stable_indices[left_position]
        for right_position in
            (left_position + 1):length(stable_indices)
            _rodh_cancel(cancel_check)
            right = stable_indices[right_position]
            state_distance = 0.0
            for state_index in 1:state_count
                _rodh_cancel(cancel_check)
                state_distance = max(
                    state_distance,
                    abs(state_values[left, state_index] -
                        state_values[right, state_index]),
                )
            end
            state_distance >= threshold && (distinct_pairs += 1)
        end
    end
    status = if !all(validity)
        :unknown_gap
    elseif distinct_pairs > 0
        :multiple_stable_roots_at_tested_control
    elseif any(stable)
        :no_multiple_stable_roots_in_supplied_set
    else
        :insufficient_stability_evidence
    end
    payload = (
        schema_version=RO_STATIC_MULTIROOT_EVIDENCE_VERSION,
        model_identity=_rodh_sha256_id(
            model_identity, "model_identity"),
        coordinate_chart_id=_rodh_sha256_id(
            coordinate_chart_id, "coordinate_chart_id"),
        control=(
            id=_rodh_nonempty_string(control_id, "control_id"),
            unit=_rodh_nonempty_string(control_unit, "control_unit"),
            value=_rodh_float(control_value, "control_value"),
        ),
        states=(
            order=Tuple(state_names),
            units=Tuple(state_unit_values),
            values=_rodh_valid_rows_payload(
                state_values, validity, cancel_check),
        ),
        outputs=(
            order=Tuple(output_names),
            units=Tuple(output_unit_values),
            values=_rodh_valid_rows_payload(
                output_values, validity, cancel_check),
        ),
        solver_status=Tuple(String.(solver_values)),
        local_stability=Tuple(String.(stability_values)),
        validity=Tuple(validity),
        distinct_root_threshold=threshold,
        result=(
            status=String(status),
            root_count=root_count,
            valid_root_count=count(validity),
            stable_root_count=count(stable),
            distinct_stable_root_pair_count=distinct_pairs,
            qualifies_as_dynamic_hysteresis=false,
            global_multistability_claimed=false,
            experimental_causality_claimed=false,
            evidence_scope="finite_supplied_static_root_set_only",
        ),
    )
    payload_hash = _rodh_payload_sha256(payload)
    _rodh_cancel(cancel_check)
    return ROStaticMultirootEvidence(
        _RODH_VALIDATED_TOKEN,
        RO_STATIC_MULTIROOT_EVIDENCE_VERSION,
        payload,
        payload_hash,
        status,
        root_count,
        count(validity),
        count(stable),
        distinct_pairs,
        false,
        false,
        false,
        :finite_supplied_static_root_set_only,
    )
end

function validate_ro_static_multiroot_evidence(
    evidence::ROStaticMultirootEvidence;
    kwargs...,
)
    evidence.schema_version == RO_STATIC_MULTIROOT_EVIDENCE_VERSION ||
        throw(ArgumentError("unsupported static multiroot evidence version"))
    _rodh_payload_sha256(evidence.identity_payload) ==
        evidence.identity_sha256 || throw(ArgumentError(
            "static multiroot evidence content hash mismatch"))
    recomputed = analyze_static_multiroot(; kwargs...)
    for field in fieldnames(ROStaticMultirootEvidence)
        getfield(evidence, field) == getfield(recomputed, field) ||
            throw(ArgumentError(
                "static multiroot evidence does not reproduce at field $field"))
    end
    return true
end
