using Test

module RODynamicHysteresisContractModule
import JSON3
import SHA
import LinearAlgebra
include(joinpath(
    @__DIR__, "..", "src", "rop", "ro_dynamic_hysteresis.jl"))
end

using .RODynamicHysteresisContractModule:
    RO_DYNAMIC_PROTOCOL_TRACE_VERSION,
    RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION,
    RO_STATIC_MULTIROOT_EVIDENCE_VERSION,
    RO_DYNAMIC_BRANCH_EVIDENCE_VERSION,
    RO_POLYNOMIAL_VECTOR_FIELD_VERSION,
    RO_DYNAMIC_HYSTERESIS_EVIDENCE_SCOPE,
    RO_DYNAMIC_HYSTERESIS_ANALYZER_POLICY_SHA256,
    RODynamicEvidenceLimits,
    RODynamicEvidenceLimitExceeded,
    RODynamicSwitchEvent,
    RODynamicProtocolTrace,
    ROPolynomialVectorField,
    RODynamicBranchEvidence,
    RODynamicHysteresisAnalysis,
    ro_dynamic_protocol_identity_payload,
    ro_dynamic_protocol_identity_sha256,
    ro_polynomial_vector_field,
    validate_ro_polynomial_vector_field,
    certify_ro_dynamic_branch_evidence,
    validate_ro_dynamic_branch_evidence,
    certify_ro_equilibrium_branch_evidence,
    validate_ro_equilibrium_branch_evidence,
    validate_ro_dynamic_hysteresis_analysis,
    analyze_dynamic_hysteresis,
    analyze_static_multiroot,
    validate_ro_static_multiroot_evidence

content_hash(label) = "sha256:" *
    bytes2hex(RODynamicHysteresisContractModule.SHA.sha256(
        codeunits(String(label))))

struct RODynamicTraceCancelledBeforeRead <: Exception end

struct RODynamicTraceReadProbe <: AbstractVector{Float64}
    values::Vector{Float64}
    reads::Base.RefValue{Int}
end

Base.size(probe::RODynamicTraceReadProbe) = size(probe.values)
Base.IndexStyle(::Type{RODynamicTraceReadProbe}) = IndexLinear()
function Base.getindex(probe::RODynamicTraceReadProbe, index::Int)
    probe.reads[] += 1
    return probe.values[index]
end

const CHART = content_hash("affine-control-chart-v1")
const DYNAMICS_POLICY = content_hash("analytic-dynamics-policy-v1")
const RESIDUAL_POLICY = content_hash("analytic-residual-policy-v1")
const PROTOCOL_FAMILY = content_hash("triangular-ramp-family-v1")
const SOLVER_POLICY = content_hash("analytic-solver-policy-v1")
const TRAJECTORY_SOLVER_POLICY =
    content_hash("analytic-trajectory-solver-policy-v1")
const NUMERICAL_AGREEMENT_POLICY =
    content_hash("analytic-numerical-agreement-policy-v1")
const STABILITY_POLICY = content_hash("analytic-stability-policy-v1")
const BRANCH_POLICY = content_hash("analytic-branch-policy-v1")
const EVENT_POLICY = content_hash("analytic-event-policy-v1")
const RATE_UNIT = "control-unit/s"

const CUBIC_VECTOR_FIELD = ro_polynomial_vector_field(
    coordinate_chart_id=CHART,
    dynamics_policy_sha256=DYNAMICS_POLICY,
    residual_policy_sha256=RESIDUAL_POLICY,
    solver_policy_sha256=SOLVER_POLICY,
    stability_policy_sha256=STABILITY_POLICY,
    branch_policy_sha256=BRANCH_POLICY,
    event_policy_sha256=EVENT_POLICY,
    state_ids=["x"],
    state_units=["state-unit"],
    control_id="u1",
    control_unit="control-unit",
    fixed_control_ids=["u2"],
    fixed_control_units=["control-unit"],
    time_unit="s",
    equations=[[
        (coefficient=-1.0, exponents=[3, 0, 0]),
        (coefficient=1.0, exponents=[1, 0, 0]),
        (coefficient=1.0, exponents=[0, 1, 0]),
    ]],
)
const LINEAR_VECTOR_FIELD = ro_polynomial_vector_field(
    coordinate_chart_id=CHART,
    dynamics_policy_sha256=DYNAMICS_POLICY,
    residual_policy_sha256=RESIDUAL_POLICY,
    solver_policy_sha256=SOLVER_POLICY,
    stability_policy_sha256=STABILITY_POLICY,
    branch_policy_sha256=BRANCH_POLICY,
    event_policy_sha256=EVENT_POLICY,
    state_ids=["x"],
    state_units=["state-unit"],
    control_id="u1",
    control_unit="control-unit",
    fixed_control_ids=["u2"],
    fixed_control_units=["control-unit"],
    time_unit="s",
    equations=[[
        (coefficient=-2.0, exponents=[1, 0, 0]),
        (coefficient=2.0, exponents=[0, 1, 0]),
    ]],
)
const MODEL_CUBIC = CUBIC_VECTOR_FIELD.identity_sha256
const MODEL_LINEAR = LINEAR_VECTOR_FIELD.identity_sha256

cubic_equilibrium_residual(point) = Float64[
    point.state[1] - point.state[1]^3 + point.control]
cubic_equilibrium_jacobian(point) =
    reshape(Float64[1 - 3point.state[1]^2], 1, 1)

function linear_protocol_residual(point)
    point.index == 1 && return Float64[0.0]
    tau = 0.5
    dt = point.time - point.previous_time
    slope = (point.control - point.previous_control) / dt
    predicted = point.previous_control + slope * (dt - tau) +
        (point.previous_state[1] - point.previous_control + slope * tau) *
            exp(-dt / tau)
    return Float64[point.state[1] - predicted]
end

linear_protocol_jacobian(point) = reshape(Float64[-2.0], 1, 1)
linear_equilibrium_residual(point) =
    Float64[point.state[1] - point.control]
linear_equilibrium_jacobian(point) = reshape(Float64[-2.0], 1, 1)

function branch_evidence_pair(
    forward,
    reverse;
    evidence_kind::Symbol,
    residual_evaluator,
    jacobian_evaluator,
    limits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    forward_evidence = certify_ro_dynamic_branch_evidence(
        forward;
        evidence_kind=evidence_kind,
        residual_evaluator=residual_evaluator,
        jacobian_evaluator=jacobian_evaluator,
        residual_tolerance=1e-8,
        stability_margin=1e-6,
        limits=limits,
        cancel_check=cancel_check,
    )
    reverse_evidence = certify_ro_dynamic_branch_evidence(
        reverse;
        evidence_kind=evidence_kind,
        residual_evaluator=residual_evaluator,
        jacobian_evaluator=jacobian_evaluator,
        residual_tolerance=1e-8,
        stability_margin=1e-6,
        limits=limits,
        cancel_check=cancel_check,
    )
    return forward_evidence, reverse_evidence
end

function equilibrium_evidence_pair(
    forward,
    reverse,
    vector_field;
    limits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
)
    forward_evidence = certify_ro_equilibrium_branch_evidence(
        forward,
        vector_field;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
        limits=limits,
        cancel_check=cancel_check,
    )
    reverse_evidence = certify_ro_equilibrium_branch_evidence(
        reverse,
        vector_field;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
        limits=limits,
        cancel_check=cancel_check,
    )
    return forward_evidence, reverse_evidence
end

function sampled_time(control::Vector{Float64}, rate::Float64)
    result = zeros(Float64, length(control))
    for index in 2:length(control)
        result[index] = result[index - 1] +
            abs(control[index] - control[index - 1]) / rate
    end
    return result
end

function make_event(
    control::Vector{Float64},
    branches,
    left_index::Int,
    right_index::Int;
    status::Symbol=:verified,
    event_type::Symbol=:branch_switch,
    pre_stability::Symbol=:stable,
    post_stability::Symbol=:stable,
    pre_branch_id=branches[left_index],
    post_branch_id=branches[right_index],
    policy_sha256=EVENT_POLICY,
    control_uncertainty=0.0,
)
    return RODynamicSwitchEvent(
        status=status,
        event_type=event_type,
        left_index=left_index,
        right_index=right_index,
        control_interval=(
            control[left_index], control[right_index]),
        control_unit="control-unit",
        control_uncertainty=control_uncertainty,
        pre_branch_id=pre_branch_id,
        post_branch_id=post_branch_id,
        pre_stability=pre_stability,
        post_stability=post_stability,
        policy_sha256=policy_sha256,
    )
end

function make_trace(;
    control,
    direction::Symbol,
    states,
    branch_ids,
    model_identity=MODEL_CUBIC,
    protocol_family_sha256=PROTOCOL_FAMILY,
    protocol_id=direction == :increasing ?
        "increasing-leg" : "decreasing-leg",
    lineage_predecessor_trace_sha256=nothing,
    rate::Float64=1.0,
    rate_relative_tolerance=1e-9,
    rate_absolute_tolerance=0.0,
    initial_state=nothing,
    outputs=nothing,
    solver_status=nothing,
    local_stability=nothing,
    branch_structure_status::Symbol=:unknown,
    dynamics_residual_norm=nothing,
    dynamics_residual_status=nothing,
    model_solver_policy_sha256=SOLVER_POLICY,
    trajectory_solver_policy_sha256=TRAJECTORY_SOLVER_POLICY,
    trajectory_solver_relative_tolerance=1e-10,
    trajectory_solver_absolute_tolerance=1e-12,
    dynamics_residual_tolerance=1e-8,
    numerical_agreement_policy_sha256=NUMERICAL_AGREEMENT_POLICY,
    numerical_agreement_tolerance=1.0,
    numerical_agreement_norm=nothing,
    numerical_agreement_status=nothing,
    event_policy_sha256=EVENT_POLICY,
    event_detection_status::Symbol=:complete,
    events=RODynamicSwitchEvent[],
    limits=RODynamicEvidenceLimits(),
)
    control_values = Float64.(control)
    state_values = Float64.(states)
    point_count = length(control_values)
    output_values = outputs === nothing ?
        copy(state_values) : Float64.(outputs)
    solver_values = solver_status === nothing ?
        fill(:success, point_count) : solver_status
    stability_values = local_stability === nothing ?
        fill(:stable, point_count) : local_stability
    residual_values = dynamics_residual_norm === nothing ?
        zeros(Float64, point_count) :
        collect(dynamics_residual_norm)
    residual_status_values = dynamics_residual_status === nothing ?
        fill(:verified, point_count) : dynamics_residual_status
    agreement_values = numerical_agreement_norm === nothing ?
        fill(NaN, point_count) : collect(numerical_agreement_norm)
    agreement_status_values = numerical_agreement_status === nothing ?
        fill(:unknown, point_count) : numerical_agreement_status
    initial_values = initial_state === nothing ?
        Float64[state_values[1]] : Float64.(initial_state)
    return RODynamicProtocolTrace(
        model_identity=model_identity,
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        protocol_family_sha256=protocol_family_sha256,
        protocol_id=protocol_id,
        lineage_predecessor_trace_sha256=
            lineage_predecessor_trace_sha256,
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        fixed_control_values=[0.25],
        direction=direction,
        rate=rate,
        rate_unit=RATE_UNIT,
        rate_relative_tolerance=rate_relative_tolerance,
        rate_absolute_tolerance=rate_absolute_tolerance,
        rate_absolute_tolerance_unit=RATE_UNIT,
        time_unit="s",
        initial_condition_id=direction == :increasing ?
            "cycle-low-side" : "cycle-high-side",
        initial_state=initial_values,
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        trajectory_solver_method="analytic-fixture",
        model_solver_policy_sha256=model_solver_policy_sha256,
        trajectory_solver_policy_sha256=
            trajectory_solver_policy_sha256,
        trajectory_solver_relative_tolerance=
            trajectory_solver_relative_tolerance,
        trajectory_solver_absolute_tolerance=
            trajectory_solver_absolute_tolerance,
        stability_method="analytic-jacobian-sign",
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        branch_structure_status=branch_structure_status,
        event_method="typed-analytic-event",
        event_policy_sha256=event_policy_sha256,
        event_detection_status=event_detection_status,
        dynamics_residual_tolerance=dynamics_residual_tolerance,
        numerical_agreement_policy_sha256=
            numerical_agreement_policy_sha256,
        numerical_agreement_tolerance=numerical_agreement_tolerance,
        time=sampled_time(control_values, rate),
        control=control_values,
        states=reshape(state_values, :, 1),
        outputs=reshape(output_values, :, 1),
        solver_status=solver_values,
        local_stability=stability_values,
        branch_ids=branch_ids,
        dynamics_residual_norm=residual_values,
        dynamics_residual_status=residual_status_values,
        numerical_agreement_norm=agreement_values,
        numerical_agreement_status=agreement_status_values,
        events=events,
        limits=limits,
    )
end

function copy_trace_with_field(trace, field_name::Symbol, value)
    fields = fieldnames(RODynamicProtocolTrace)
    values = Any[getfield(trace, field) for field in fields]
    values[only(findall(==(field_name), fields))] = value
    return RODynamicProtocolTrace(values...)
end

function stable_cubic_root(control::Float64, branch::Symbol)
    value = branch == :low ? -1.5 : 1.5
    for _ in 1:80
        residual = value^3 - value - control
        value -= residual / (3value^2 - 1)
    end
    @test abs(value^3 - value - control) <= 1e-12
    @test 1 - 3value^2 < 0
    return value
end

function cubic_data()
    increasing_control = Float64[-0.5, -0.25, 0.0, 0.25, 0.5]
    decreasing_control = reverse(increasing_control)
    low = Dict(control => stable_cubic_root(control, :low)
        for control in increasing_control[1:4])
    low[-0.5] = stable_cubic_root(-0.5, :low)
    high = Dict(control => stable_cubic_root(control, :high)
        for control in increasing_control[2:5])
    forward_states = Float64[
        low[-0.5],
        low[-0.25],
        low[0.0],
        low[0.25],
        high[0.5],
    ]
    reverse_states = Float64[
        high[0.5],
        high[0.25],
        high[0.0],
        high[-0.25],
        low[-0.5],
    ]
    forward_branches = Union{Nothing,String}[
        "low", "low", "low", "low", "high"]
    reverse_branches = Union{Nothing,String}[
        "high", "high", "high", "high", "low"]
    forward_residual = abs.(
        forward_states .- forward_states .^ 3 .+
        increasing_control)
    reverse_residual = abs.(
        reverse_states .- reverse_states .^ 3 .+
        decreasing_control)
    return (
        increasing_control=increasing_control,
        decreasing_control=decreasing_control,
        forward_states=forward_states,
        reverse_states=reverse_states,
        forward_branches=forward_branches,
        reverse_branches=reverse_branches,
        forward_residual=forward_residual,
        reverse_residual=reverse_residual,
    )
end

function cubic_pair(;
    forward_rate::Float64=1.0,
    reverse_rate::Float64=1.0,
    reverse_family=PROTOCOL_FAMILY,
    coupled::Bool=true,
    reverse_event_detection_status::Symbol=:complete,
    include_reverse_event::Bool=true,
    forward_residual_status=nothing,
    forward_residual_norm=nothing,
    forward_local_stability=nothing,
    event_policy_sha256=EVENT_POLICY,
)
    data = cubic_data()
    forward_event = make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5;
        policy_sha256=event_policy_sha256,
    )
    forward = make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        rate=forward_rate,
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=forward_residual_norm === nothing ?
            data.forward_residual : forward_residual_norm,
        dynamics_residual_status=forward_residual_status === nothing ?
            fill(:verified, 5) : forward_residual_status,
        local_stability=forward_local_stability,
        event_policy_sha256=event_policy_sha256,
        events=[forward_event],
    )
    reverse_event = make_event(
        data.decreasing_control,
        data.reverse_branches,
        4,
        5;
        policy_sha256=event_policy_sha256,
    )
    reverse = make_trace(
        control=data.decreasing_control,
        direction=:decreasing,
        states=data.reverse_states,
        branch_ids=data.reverse_branches,
        protocol_family_sha256=reverse_family,
        lineage_predecessor_trace_sha256=coupled ?
            ro_dynamic_protocol_identity_sha256(forward) : nothing,
        rate=reverse_rate,
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=data.reverse_residual,
        event_policy_sha256=event_policy_sha256,
        event_detection_status=reverse_event_detection_status,
        events=include_reverse_event ? [reverse_event] :
            RODynamicSwitchEvent[],
    )
    return forward, reverse
end

function analyze_pair(
    forward,
    reverse;
    separation_threshold=1.0,
    jump_threshold=1.0,
    minimum_persistence_points::Int=3,
    minimum_persistence_span=0.5,
    closure_state_absolute_tolerances=
        fill(1e-10, length(forward.state_ids)),
    closure_state_absolute_tolerance_units=copy(forward.state_units),
    closure_state_relative_tolerance=1e-10,
    branch_state_absolute_tolerances=
        fill(1e-10, length(forward.state_ids)),
    branch_state_absolute_tolerance_units=copy(forward.state_units),
    branch_state_relative_tolerance=1e-10,
    rate_match_relative_tolerance=1e-9,
    rate_match_absolute_tolerance=0.0,
    forward_branch_evidence=nothing,
    reverse_branch_evidence=nothing,
    evidence_kind=nothing,
    residual_evaluator=nothing,
    jacobian_evaluator=nothing,
    vector_field=:auto,
    limits=RODynamicEvidenceLimits(),
    cancel_check=nothing,
    kwargs...,
)
    selected_vector_field = vector_field === :auto ?
        (forward.model_identity == MODEL_CUBIC ?
            CUBIC_VECTOR_FIELD : nothing) : vector_field
    selected_kind = evidence_kind === nothing ?
        (forward.model_identity == MODEL_LINEAR ?
            :protocol_dynamics : :equilibrium_branch) : evidence_kind
    selected_residual = residual_evaluator === nothing ?
        (forward.model_identity == MODEL_LINEAR ?
            linear_protocol_residual : cubic_equilibrium_residual) :
        residual_evaluator
    selected_jacobian = jacobian_evaluator === nothing ?
        (forward.model_identity == MODEL_LINEAR ?
            linear_protocol_jacobian : cubic_equilibrium_jacobian) :
        jacobian_evaluator
    if forward_branch_evidence === nothing &&
            reverse_branch_evidence === nothing
        forward_branch_evidence, reverse_branch_evidence =
            selected_vector_field === nothing ? branch_evidence_pair(
                forward,
                reverse;
                evidence_kind=selected_kind,
                residual_evaluator=selected_residual,
                jacobian_evaluator=selected_jacobian,
                limits=limits,
                cancel_check=cancel_check,
            ) : equilibrium_evidence_pair(
                forward,
                reverse,
                selected_vector_field;
                limits=limits,
                cancel_check=cancel_check,
            )
    end
    return analyze_dynamic_hysteresis(
        forward,
        reverse;
        output_id="reporter",
        separation_threshold=separation_threshold,
        jump_threshold=jump_threshold,
        minimum_persistence_points=minimum_persistence_points,
        minimum_persistence_span=minimum_persistence_span,
        closure_state_absolute_tolerances=
            closure_state_absolute_tolerances,
        closure_state_absolute_tolerance_units=
            closure_state_absolute_tolerance_units,
        closure_state_relative_tolerance=
            closure_state_relative_tolerance,
        branch_state_absolute_tolerances=
            branch_state_absolute_tolerances,
        branch_state_absolute_tolerance_units=
            branch_state_absolute_tolerance_units,
        branch_state_relative_tolerance=
            branch_state_relative_tolerance,
        forward_branch_evidence=forward_branch_evidence,
        reverse_branch_evidence=reverse_branch_evidence,
        equilibrium_vector_field=selected_vector_field,
        branch_residual_evaluator=selected_vector_field === nothing ?
            selected_residual : nothing,
        branch_jacobian_evaluator=selected_vector_field === nothing ?
            selected_jacobian : nothing,
        rate_match_relative_tolerance=
            rate_match_relative_tolerance,
        rate_match_absolute_tolerance=
            rate_match_absolute_tolerance,
        rate_match_absolute_tolerance_unit=RATE_UNIT,
        limits=limits,
        cancel_check=cancel_check,
        kwargs...,
    )
end

function validate_pair(
    analysis,
    forward,
    reverse,
    forward_evidence,
    reverse_evidence;
    vector_field=nothing,
    residual_evaluator=nothing,
    jacobian_evaluator=nothing,
    limits=RODynamicEvidenceLimits(),
)
    return validate_ro_dynamic_hysteresis_analysis(
        analysis,
        forward,
        reverse;
        output_id=analysis.output_id,
        separation_threshold=analysis.separation_threshold,
        jump_threshold=analysis.jump_threshold,
        minimum_persistence_points=analysis.minimum_persistence_points,
        minimum_persistence_span=analysis.minimum_persistence_span,
        closure_state_absolute_tolerances=
            analysis.closure_state_absolute_tolerances,
        closure_state_absolute_tolerance_units=
            analysis.closure_state_absolute_tolerance_units,
        closure_state_relative_tolerance=
            analysis.closure_state_relative_tolerance,
        branch_state_absolute_tolerances=
            analysis.branch_state_absolute_tolerances,
        branch_state_absolute_tolerance_units=
            analysis.branch_state_absolute_tolerance_units,
        branch_state_relative_tolerance=
            analysis.branch_state_relative_tolerance,
        forward_branch_evidence=forward_evidence,
        reverse_branch_evidence=reverse_evidence,
        equilibrium_vector_field=vector_field,
        branch_residual_evaluator=vector_field === nothing ?
            residual_evaluator : nothing,
        branch_jacobian_evaluator=vector_field === nothing ?
            jacobian_evaluator : nothing,
        rate_match_relative_tolerance=
            analysis.rate_match_relative_tolerance,
        rate_match_absolute_tolerance=
            analysis.rate_match_absolute_tolerance,
        rate_match_absolute_tolerance_unit=
            analysis.rate_match_absolute_tolerance_unit,
        analysis_policy_sha256=analysis.analysis_policy_sha256,
        limits=limits,
    )
end

function linear_lag_cycle()
    tau = 0.5
    advance(state, control, slope) =
        control + slope * (1 - tau) +
        (state - control + slope * tau) * exp(-1 / tau)
    function cycle_map(initial)
        state = initial
        for control in 0.0:1.0:3.0
            state = advance(state, control, 1.0)
        end
        for control in 4.0:-1.0:1.0
            state = advance(state, control, -1.0)
        end
        return state
    end
    intercept = cycle_map(0.0)
    slope = cycle_map(1.0) - intercept
    initial = intercept / (1 - slope)
    forward_states = Float64[initial]
    state = initial
    for control in 0.0:1.0:3.0
        state = advance(state, control, 1.0)
        push!(forward_states, state)
    end
    reverse_states = Float64[state]
    for control in 4.0:-1.0:1.0
        state = advance(state, control, -1.0)
        push!(reverse_states, state)
    end
    @test abs(state - initial) <= 1e-12
    controls = collect(0.0:1.0:4.0)
    reverse_controls = Base.reverse(controls)
    forward_branches = Union{Nothing,String}[fill("unique", 5)...]
    reverse_branches = Union{Nothing,String}[fill("unique", 5)...]
    forward_event = make_event(
        controls,
        forward_branches,
        2,
        3;
        event_type=:threshold_crossing,
    )
    forward = make_trace(
        control=controls,
        direction=:increasing,
        states=forward_states,
        branch_ids=forward_branches,
        model_identity=MODEL_LINEAR,
        branch_structure_status=
            :unique_stable_branch_certified,
        events=[forward_event],
    )
    reverse_event = make_event(
        reverse_controls,
        reverse_branches,
        2,
        3;
        event_type=:threshold_crossing,
    )
    reverse = make_trace(
        control=reverse_controls,
        direction=:decreasing,
        states=reverse_states,
        branch_ids=reverse_branches,
        model_identity=MODEL_LINEAR,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
        branch_structure_status=
            :unique_stable_branch_certified,
        events=[reverse_event],
    )
    return forward, reverse
end

@testset "trace cancellation precedes caller-owned input reads" begin
    time_reads = Ref(0)
    control_reads = Ref(0)
    time_probe = RODynamicTraceReadProbe(
        Float64[0.0, 1.0], time_reads)
    control_probe = RODynamicTraceReadProbe(
        Float64[0.0, 1.0], control_reads)

    @test_throws RODynamicTraceCancelledBeforeRead RODynamicProtocolTrace(
        model_identity=MODEL_CUBIC,
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        protocol_family_sha256=PROTOCOL_FAMILY,
        protocol_id="cancel-before-read",
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        fixed_control_values=[0.25],
        direction=:increasing,
        rate=1.0,
        rate_unit=RATE_UNIT,
        rate_absolute_tolerance_unit=RATE_UNIT,
        time_unit="s",
        initial_condition_id="cancel-before-read",
        initial_state=[0.0],
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        trajectory_solver_method="not-reached",
        model_solver_policy_sha256=SOLVER_POLICY,
        trajectory_solver_policy_sha256=TRAJECTORY_SOLVER_POLICY,
        trajectory_solver_relative_tolerance=1e-10,
        trajectory_solver_absolute_tolerance=1e-12,
        stability_method="not-reached",
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        branch_structure_status=:unknown,
        event_method="not-reached",
        event_policy_sha256=EVENT_POLICY,
        event_detection_status=:complete,
        dynamics_residual_tolerance=1e-8,
        numerical_agreement_policy_sha256=
            NUMERICAL_AGREEMENT_POLICY,
        numerical_agreement_tolerance=1.0,
        time=time_probe,
        control=control_probe,
        states=zeros(Float64, 2, 1),
        outputs=zeros(Float64, 2, 1),
        solver_status=fill(:success, 2),
        local_stability=fill(:stable, 2),
        branch_ids=Union{Nothing,String}[nothing, nothing],
        dynamics_residual_norm=zeros(Float64, 2),
        dynamics_residual_status=fill(:verified, 2),
        numerical_agreement_norm=fill(NaN, 2),
        numerical_agreement_status=fill(:unknown, 2),
        cancel_check=() -> throw(RODynamicTraceCancelledBeforeRead()),
    )
    @test time_reads[] == 0
    @test control_reads[] == 0
end

@testset "declarative polynomial vector fields are canonical and bounded" begin
    @test CUBIC_VECTOR_FIELD.schema_version ==
        RO_POLYNOMIAL_VECTOR_FIELD_VERSION
    @test CUBIC_VECTOR_FIELD.identity_sha256 == MODEL_CUBIC
    @test CUBIC_VECTOR_FIELD.term_count == 3
    @test CUBIC_VECTOR_FIELD.exponent_entry_count == 9
    @test validate_ro_polynomial_vector_field(CUBIC_VECTOR_FIELD)
    @test CUBIC_VECTOR_FIELD.identity_payload.variables.states.order ==
        ("x",)
    @test CUBIC_VECTOR_FIELD.identity_payload.variables.swept_control.id ==
        "u1"
    @test CUBIC_VECTOR_FIELD.time_unit == "s"
    @test CUBIC_VECTOR_FIELD.identity_payload.variables.model_time == (
        unit="s",
        equation_output_semantics="state_unit_per_model_time_unit",
    )
    @test CUBIC_VECTOR_FIELD.identity_payload.variables.fixed_controls.order ==
        ("u2",)
    @test Tuple(term.exponents
        for term in CUBIC_VECTOR_FIELD.equations[1]) ==
        ((0, 1, 0), (1, 0, 0), (3, 0, 0))

    reordered = ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        time_unit="s",
        equations=[[
            (coefficient=1.0, exponents=[0, 1, 0]),
            (coefficient=1.0, exponents=[1, 0, 0]),
            (coefficient=-1.0, exponents=[3, 0, 0]),
        ]],
    )
    @test reordered.identity_sha256 == CUBIC_VECTOR_FIELD.identity_sha256

    different_time = ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        time_unit="minute",
        equations=CUBIC_VECTOR_FIELD.equations,
    )
    @test different_time.identity_sha256 != CUBIC_VECTOR_FIELD.identity_sha256

    different_model = ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        time_unit="s",
        equations=[[
            (coefficient=-1.0, exponents=[3, 0, 0]),
            (coefficient=1.0, exponents=[1, 0, 0]),
            (coefficient=2.0, exponents=[0, 1, 0]),
        ]],
    )
    @test different_model.identity_sha256 != MODEL_CUBIC
    forward, _ = cubic_pair()
    @test_throws ArgumentError certify_ro_equilibrium_branch_evidence(
        forward,
        different_model;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
    )

    different_policy = ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=content_hash("different-branch-policy"),
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=["u2"],
        fixed_control_units=["control-unit"],
        time_unit="s",
        equations=CUBIC_VECTOR_FIELD.equations,
    )
    @test different_policy.identity_sha256 != MODEL_CUBIC
    @test_throws ArgumentError certify_ro_equilibrium_branch_evidence(
        forward,
        different_policy;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
    )

    positional_values = Any[getfield(CUBIC_VECTOR_FIELD, field)
        for field in fieldnames(ROPolynomialVectorField)]
    @test_throws MethodError ROPolynomialVectorField(positional_values...)
    @test_throws ArgumentError ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        time_unit="s",
        equations=[[
            (coefficient=1.0, exponents=[1, 0]),
            (coefficient=2.0, exponents=[1, 0]),
        ]],
    )
    @test_throws ArgumentError ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        time_unit="s",
        equations=[[(coefficient=Float32(1), exponents=[1, 0])]],
    )
    @test_throws ArgumentError ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        time_unit="s",
        equations=[[(coefficient=1.0, exponents=(true, 0))]],
    )
    @test_throws ArgumentError ro_polynomial_vector_field(
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        time_unit="s",
        equations=[[(coefficient=1.0, exponents=[65, 0])]],
    )

    budget_cases = (
        (:vector_field_terms,
            RODynamicEvidenceLimits(max_vector_field_terms=2)),
        (:vector_field_exponent_entries,
            RODynamicEvidenceLimits(max_vector_field_exponent_entries=8)),
        (:vector_field_work_units,
            RODynamicEvidenceLimits(max_vector_field_work_units=14)),
        (:vector_field_payload_scalars,
            RODynamicEvidenceLimits(max_payload_scalars=17)),
    )
    for (phase, limits) in budget_cases
        error = try
            ro_polynomial_vector_field(
                coordinate_chart_id=CHART,
                dynamics_policy_sha256=DYNAMICS_POLICY,
                residual_policy_sha256=RESIDUAL_POLICY,
                solver_policy_sha256=SOLVER_POLICY,
                stability_policy_sha256=STABILITY_POLICY,
                branch_policy_sha256=BRANCH_POLICY,
                event_policy_sha256=EVENT_POLICY,
                state_ids=["x"],
                state_units=["state-unit"],
                control_id="u1",
                control_unit="control-unit",
                fixed_control_ids=["u2"],
                fixed_control_units=["control-unit"],
                time_unit="s",
                equations=CUBIC_VECTOR_FIELD.equations,
                limits=limits,
            )
            nothing
        catch caught
            caught
        end
        @test error isa RODynamicEvidenceLimitExceeded
        @test error.phase == phase
    end

    fixed_ids = ["fixed_$(index)" for index in 1:20]
    fixed_units = fill("fixed-unit", 20)
    state_term_exponents = zeros(Int, 22)
    state_term_exponents[1] = 1
    fixed_term_exponents = zeros(Int, 22)
    fixed_term_exponents[3] = 1
    wide_spec = (
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        solver_policy_sha256=SOLVER_POLICY,
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        event_policy_sha256=EVENT_POLICY,
        state_ids=["x"],
        state_units=["state-unit"],
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=fixed_ids,
        fixed_control_units=fixed_units,
        time_unit="s",
        equations=[[
            (coefficient=-1.0, exponents=state_term_exponents),
            (coefficient=1.0, exponents=fixed_term_exponents),
        ]],
    )
    wide_field = ro_polynomial_vector_field(; wide_spec...)
    @test wide_field.exponent_entry_count == 44
    @test wide_field.evaluation_work_units == 125

    below_field_work = RODynamicEvidenceLimits(
        max_vector_field_work_units=124)
    field_work_error = try
        ro_polynomial_vector_field(
            ; wide_spec..., limits=below_field_work)
        nothing
    catch caught
        caught
    end
    @test field_work_error isa RODynamicEvidenceLimitExceeded
    @test field_work_error.phase == :vector_field_work_units
    @test field_work_error.requested == 125

    exact_field_work = RODynamicEvidenceLimits(
        max_vector_field_work_units=125)
    boundary_field = ro_polynomial_vector_field(
        ; wide_spec..., limits=exact_field_work)
    @test boundary_field.identity_sha256 == wide_field.identity_sha256
    evaluator_checks = Ref(0)
    polynomial_evaluator = getfield(
        RODynamicHysteresisContractModule,
        :_rodh_evaluate_polynomial_vector_field,
    )
    values, jacobian = polynomial_evaluator(
        boundary_field,
        Float64[1.0],
        0.0,
        vcat(1.0, zeros(Float64, 19)),
        exact_field_work,
        () -> (evaluator_checks[] += 1),
    )
    @test values == Float64[0.0]
    @test jacobian == reshape(Float64[-1.0], 1, 1)
    @test evaluator_checks[] == 91
    @test boundary_field.evaluation_work_units >= evaluator_checks[]

    wide_trace = RODynamicProtocolTrace(
        model_identity=wide_field.identity_sha256,
        coordinate_chart_id=CHART,
        dynamics_policy_sha256=DYNAMICS_POLICY,
        residual_policy_sha256=RESIDUAL_POLICY,
        protocol_family_sha256=PROTOCOL_FAMILY,
        protocol_id="wide-work-boundary",
        control_id="u1",
        control_unit="control-unit",
        fixed_control_ids=fixed_ids,
        fixed_control_units=fixed_units,
        fixed_control_values=vcat(1.0, zeros(Float64, 19)),
        direction=:increasing,
        rate=1.0,
        rate_unit=RATE_UNIT,
        rate_absolute_tolerance_unit=RATE_UNIT,
        time_unit="s",
        initial_condition_id="wide-work-boundary",
        initial_state=[1.0],
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        trajectory_solver_method="analytic-fixture",
        model_solver_policy_sha256=SOLVER_POLICY,
        trajectory_solver_policy_sha256=TRAJECTORY_SOLVER_POLICY,
        trajectory_solver_relative_tolerance=1e-10,
        trajectory_solver_absolute_tolerance=1e-12,
        stability_method="analytic-jacobian-sign",
        stability_policy_sha256=STABILITY_POLICY,
        branch_policy_sha256=BRANCH_POLICY,
        branch_structure_status=:unique_stable_branch_certified,
        event_method="typed-analytic-event",
        event_policy_sha256=EVENT_POLICY,
        event_detection_status=:complete,
        dynamics_residual_tolerance=1e-8,
        numerical_agreement_policy_sha256=
            NUMERICAL_AGREEMENT_POLICY,
        numerical_agreement_tolerance=1.0,
        time=Float64[0.0, 1.0],
        control=Float64[0.0, 1.0],
        states=ones(Float64, 2, 1),
        outputs=ones(Float64, 2, 1),
        solver_status=fill(:success, 2),
        local_stability=fill(:stable, 2),
        branch_ids=Union{Nothing,String}["single", "single"],
        dynamics_residual_norm=zeros(Float64, 2),
        dynamics_residual_status=fill(:verified, 2),
        numerical_agreement_norm=fill(NaN, 2),
        numerical_agreement_status=fill(:unknown, 2),
    )
    below_joint_work = RODynamicEvidenceLimits(
        max_vector_field_work_units=125,
        max_analysis_work_units=253,
    )
    joint_work_error = try
        certify_ro_equilibrium_branch_evidence(
            wide_trace,
            wide_field;
            residual_tolerance=1e-8,
            stability_margin=1e-6,
            limits=below_joint_work,
        )
        nothing
    catch caught
        caught
    end
    @test joint_work_error isa RODynamicEvidenceLimitExceeded
    @test joint_work_error.phase == :branch_evidence_work_units
    @test joint_work_error.requested == 254

    exact_joint_work = RODynamicEvidenceLimits(
        max_vector_field_work_units=125,
        max_analysis_work_units=254,
    )
    boundary_evidence = certify_ro_equilibrium_branch_evidence(
        wide_trace,
        wide_field;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
        limits=exact_joint_work,
    )
    @test boundary_evidence.strong_equilibrium_certified
end

@testset "content identities bind model, policies, history, and analysis config" begin
    forward, reverse = cubic_pair()
    payload = ro_dynamic_protocol_identity_payload(forward)
    @test payload.schema_version == RO_DYNAMIC_PROTOCOL_TRACE_VERSION
    @test payload.model_identity == MODEL_CUBIC
    @test payload.protocol.family_sha256 == PROTOCOL_FAMILY
    @test payload.protocol.swept_control.direction == "increasing"
    @test payload.model_solver.policy_sha256 == SOLVER_POLICY
    @test payload.trajectory_solver.policy_sha256 ==
        TRAJECTORY_SOLVER_POLICY
    @test payload.numerical_agreement.policy_sha256 ==
        NUMERICAL_AGREEMENT_POLICY
    @test payload.branches.ids ==
        ("low", "low", "low", "low", "high")
    @test length(payload.events.records) == 1
    @test payload.events.records[1].event_type == "branch_switch"
    @test startswith(
        ro_dynamic_protocol_identity_sha256(forward), "sha256:")

    analysis = analyze_pair(forward, reverse)
    changed_policy = analyze_pair(
        forward,
        reverse;
        rate_match_relative_tolerance=1e-8,
    )
    @test analysis.analysis_identity_sha256 !=
        changed_policy.analysis_identity_sha256
    @test analysis.analysis_identity_payload.rate_match_relative_tolerance ==
        1e-9
    @test analysis.analysis_policy_sha256 ==
        RO_DYNAMIC_HYSTERESIS_ANALYZER_POLICY_SHA256

    other_forward, _ = cubic_pair(
        event_policy_sha256=content_hash("different-event-policy"))
    @test ro_dynamic_protocol_identity_sha256(other_forward) !=
        ro_dynamic_protocol_identity_sha256(forward)
    @test_throws ArgumentError certify_ro_equilibrium_branch_evidence(
        other_forward,
        CUBIC_VECTOR_FIELD;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
    )

    wrong_model_policy = copy_trace_with_field(
        forward,
        :model_solver_policy_sha256,
        content_hash("wrong-model-solver-policy"),
    )
    @test_throws ArgumentError certify_ro_equilibrium_branch_evidence(
        wrong_model_policy,
        CUBIC_VECTOR_FIELD;
        residual_tolerance=1e-8,
        stability_margin=1e-6,
    )

    wrong_trajectory_policy = copy_trace_with_field(
        reverse,
        :trajectory_solver_policy_sha256,
        content_hash("wrong-trajectory-solver-policy"),
    )
    @test_throws ArgumentError analyze_pair(
        forward, wrong_trajectory_policy)
    wrong_agreement_policy = copy_trace_with_field(
        reverse,
        :numerical_agreement_policy_sha256,
        content_hash("wrong-numerical-agreement-policy"),
    )
    @test_throws ArgumentError analyze_pair(
        forward, wrong_agreement_policy)

    invalid_raw = make_trace(
        control=[0.0, 1.0],
        direction=:increasing,
        states=[0.0, 2.0],
        outputs=[0.0, 20.0],
        solver_status=[:success, :failure],
        branch_ids=Union{Nothing,String}[nothing, nothing],
    )
    invalid_payload = ro_dynamic_protocol_identity_payload(invalid_raw)
    @test invalid_payload.states[2] === nothing
    @test invalid_payload.output_values[2] === nothing
    @test invalid_payload.raw_state_diagnostics_ieee754_bits[2][1] ==
        "4000000000000000"
    @test invalid_payload.raw_output_diagnostics_ieee754_bits[2][1] ==
        "4034000000000000"
    raw_identity = ro_dynamic_protocol_identity_sha256(invalid_raw)
    invalid_raw.states[2, 1] = 3.0
    @test ro_dynamic_protocol_identity_sha256(invalid_raw) != raw_identity
    invalid_raw.states[2, 1] = 2.0
    invalid_raw.outputs[2, 1] = 21.0
    @test ro_dynamic_protocol_identity_sha256(invalid_raw) != raw_identity
    @test_throws ArgumentError make_trace(
        control=[0.0, 1.0],
        direction=:increasing,
        states=[0.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing],
        model_identity="not-content-addressed",
    )
end

@testset "cubic stable roots establish only a conditional branch loop" begin
    forward, reverse = cubic_pair()
    analysis = analyze_pair(forward, reverse)
    @test analysis.schema_version ==
        RO_DYNAMIC_HYSTERESIS_ANALYSIS_VERSION
    @test analysis.status ==
        :conditional_equilibrium_branch_loop_on_tested_grid
    @test analysis.evidence_class ==
        :conditional_equilibrium_branch_loop
    @test analysis.finite_protocol_loop_detected
    @test !analysis.finite_rate_lag_detected
    @test analysis.conditional_equilibrium_branch_loop_detected
    @test !analysis.branch_switch_hysteresis_certified
    @test !analysis.qualifies_as_dynamic_hysteresis
    @test !analysis.complete_dynamic_reachability_evidence
    @test analysis.dynamic_reachability_status == :not_implemented
    @test analysis.protocol_relationship == :forward_to_reverse
    @test analysis.closed_state_cycle
    @test analysis.complete_numeric_traces
    @test analysis.complete_model_residual_evidence
    @test analysis.complete_event_evidence
    @test analysis.exact_common_control_grid
    @test analysis.exact_common_controls_only
    @test !analysis.interpolation_used
    @test analysis.matched_rate_pair
    @test analysis.rate_dependence_status == :not_tested_across_rates
    @test analysis.common_control_count == 5
    @test analysis.persistent_separation_window_count == 1
    @test analysis.best_persistent_control_window == (-0.25, 0.25)
    @test analysis.separation_sign == -1
    @test analysis.paired_branch_ids == ("low", "high")
    @test analysis.forward_paired_switch_event_index == 1
    @test analysis.reverse_paired_switch_event_index == 1
    @test analysis.sampled_absolute_loop_area > 0
    @test !analysis.static_multiroot_claimed
    @test !analysis.global_multistability_claimed
    @test !analysis.experimental_causality_claimed
    @test analysis.evidence_scope ==
        RO_DYNAMIC_HYSTERESIS_EVIDENCE_SCOPE
end

@testset "arbitrary stable-root switch timing is not dynamic reachability" begin
    data = cubic_data()
    forward_event = make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5,
    )
    forward = make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        branch_structure_status=:multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
        events=[forward_event],
    )
    reverse_states = Float64[
        stable_cubic_root(0.5, :high),
        stable_cubic_root(0.25, :high),
        stable_cubic_root(0.0, :high),
        stable_cubic_root(-0.25, :low),
        stable_cubic_root(-0.5, :low),
    ]
    reverse_branches = Union{Nothing,String}[
        "high", "high", "high", "low", "low"]
    reverse_event = make_event(
        data.decreasing_control,
        reverse_branches,
        3,
        4,
    )
    reverse = make_trace(
        control=data.decreasing_control,
        direction=:decreasing,
        states=reverse_states,
        branch_ids=reverse_branches,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
        branch_structure_status=:multiple_stable_branches_certified,
        dynamics_residual_norm=abs.(
            reverse_states .- reverse_states .^ 3 .+
            data.decreasing_control),
        events=[reverse_event],
    )
    analysis = analyze_pair(
        forward,
        reverse;
        minimum_persistence_points=2,
        minimum_persistence_span=0.25,
    )
    @test analysis.complete_declarative_equilibrium_evidence
    @test analysis.complete_event_evidence
    @test analysis.conditional_equilibrium_branch_loop_detected
    @test analysis.status ==
        :conditional_equilibrium_branch_loop_on_tested_grid
    @test !analysis.complete_dynamic_reachability_evidence
    @test !analysis.branch_switch_hysteresis_certified
    @test !analysis.qualifies_as_dynamic_hysteresis
end

@testset "callback single-branch dynamics remains a finite-rate candidate" begin
    forward, reverse = linear_lag_cycle()
    analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.2,
        minimum_persistence_span=2.0,
    )
    @test analysis.status ==
        :finite_rate_lag_candidate_from_untrusted_callback
    @test analysis.evidence_class == :finite_rate_lag_candidate
    @test analysis.finite_protocol_loop_detected
    @test analysis.finite_rate_lag_detected
    @test analysis.rate_lag_status ==
        :finite_rate_lag_candidate_from_untrusted_callback
    @test !analysis.branch_switch_hysteresis_certified
    @test !analysis.qualifies_as_dynamic_hysteresis
    @test analysis.sampled_absolute_loop_area > 0
    @test analysis.forward_paired_switch_event_index === nothing
    @test analysis.reverse_paired_switch_event_index === nothing
end

@testset "forged and uncertain events fail before branch certification" begin
    @test_throws ArgumentError RODynamicSwitchEvent(
        status=:verified,
        event_type=:branch_switch,
        left_index=1,
        right_index=2,
        control_interval=(0.0, 1.0),
        control_unit="control-unit",
        control_uncertainty=0.0,
        pre_branch_id="unique",
        post_branch_id="unique",
        pre_stability=:stable,
        post_stability=:stable,
        policy_sha256=EVENT_POLICY,
    )
    @test_throws ArgumentError RODynamicSwitchEvent(
        status=:verified,
        event_type=:branch_switch,
        left_index=1,
        right_index=2,
        control_interval=(0.0, 1.0),
        control_unit="control-unit",
        control_uncertainty=0.0,
        pre_branch_id="low",
        post_branch_id="high",
        pre_stability=:stable,
        post_stability=:unknown,
        policy_sha256=EVENT_POLICY,
    )

    data = cubic_data()
    forged = make_event(
        data.increasing_control,
        Union{Nothing,String}[
            "forged", "forged", "forged", "forged", "high"],
        4,
        5,
    )
    @test_throws ArgumentError make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
        events=[forged],
    )

    forward, reverse = cubic_pair(
        reverse_event_detection_status=:unknown,
        include_reverse_event=false,
    )
    uncertain = analyze_pair(forward, reverse)
    @test uncertain.status == :unknown_event_evidence
    @test uncertain.finite_protocol_loop_detected
    @test !uncertain.complete_event_evidence
    @test !uncertain.qualifies_as_dynamic_hysteresis

    stability = fill(:stable, 5)
    stability[2] = :unknown
    unstable_forward, unstable_reverse = cubic_pair(
        forward_local_stability=stability,
    )
    unstable = analyze_pair(unstable_forward, unstable_reverse)
    @test unstable.status ==
        :unknown_recomputed_branch_evidence
    @test unstable.finite_protocol_loop_detected
    @test !unstable.branch_switch_hysteresis_certified
    @test !unstable.qualifies_as_dynamic_hysteresis
end

@testset "protocol lineage, initial state, closure, and family are mandatory" begin
    data = cubic_data()
    @test_throws ArgumentError make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        initial_state=[999.0],
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
    )

    independent_forward, independent_reverse = cubic_pair(coupled=false)
    independent = analyze_pair(
        independent_forward, independent_reverse)
    @test independent.status == :independent_scans_not_a_loop
    @test independent.protocol_relationship == :independent_scans
    @test !independent.finite_protocol_loop_detected
    @test !independent.qualifies_as_dynamic_hysteresis

    family_forward, family_reverse = cubic_pair(
        reverse_family=content_hash("unrelated-protocol-family"))
    @test_throws ArgumentError analyze_pair(
        family_forward, family_reverse)
end

@testset "small rates use ratio matching and absolute tolerance has units" begin
    forward, reverse = cubic_pair(
        forward_rate=1e-12,
        reverse_rate=5e-10,
    )
    analysis = analyze_pair(forward, reverse)
    @test analysis.status == :incomparable_protocol_rates
    @test !analysis.matched_rate_pair
    @test !analysis.qualifies_as_dynamic_hysteresis

    @test_throws ArgumentError analyze_dynamic_hysteresis(
        forward,
        reverse;
        output_id="reporter",
        separation_threshold=1.0,
        jump_threshold=1.0,
        minimum_persistence_points=3,
        minimum_persistence_span=0.5,
        closure_state_absolute_tolerances=[1e-10],
        closure_state_absolute_tolerance_units=["state-unit"],
        branch_state_absolute_tolerances=[1e-10],
        branch_state_absolute_tolerance_units=["state-unit"],
        rate_match_relative_tolerance=1e-9,
        rate_match_absolute_tolerance=0.0,
        rate_match_absolute_tolerance_unit="wrong-unit",
    )
end

@testset "exact paired-control grid rejects missing controls without interpolation" begin
    forward = make_trace(
        control=[0.0, 1.0, 2.0, 3.0],
        direction=:increasing,
        states=[0.0, 0.0, 0.0, 1.0],
        branch_ids=Union{Nothing,String}[nothing, nothing, nothing, nothing],
    )
    reverse = make_trace(
        control=[3.0, 2.0, 0.0],
        direction=:decreasing,
        states=[1.0, 1.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing, nothing],
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
    )
    analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.1,
        jump_threshold=0.1,
        minimum_persistence_points=2,
        minimum_persistence_span=1.0,
    )
    @test analysis.status ==
        :unknown_incomplete_common_control_grid
    @test !analysis.exact_common_control_grid
    @test analysis.common_control_count == 0
    @test analysis.sampled_signed_loop_area === nothing
    @test !analysis.qualifies_as_dynamic_hysteresis
    @test_throws ArgumentError analyze_pair(
        forward,
        reverse;
        alignment=:linear_interpolation,
    )
end

@testset "sign-changing separation cannot invent one persistent branch window" begin
    controls = collect(0.0:1.0:4.0)
    forward = make_trace(
        control=controls,
        direction=:increasing,
        states=[0.0, -1.0, 1.0, -1.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing, nothing, nothing, nothing],
    )
    reverse = make_trace(
        control=Base.reverse(controls),
        direction=:decreasing,
        states=[0.0, 1.0, -1.0, 1.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing, nothing, nothing, nothing],
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
    )
    analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=1.0,
        jump_threshold=0.5,
        minimum_persistence_points=3,
        minimum_persistence_span=2.0,
    )
    @test analysis.status ==
        :no_finite_protocol_loop_on_tested_cycle
    @test analysis.persistent_separation_window_count == 0
    @test !analysis.qualifies_as_dynamic_hysteresis
end

@testset "numerical and model-residual gaps remain unknown" begin
    data = cubic_data()
    residual_status = fill(:verified, 5)
    residual_status[2] = :unknown
    residual_norm = copy(data.forward_residual)
    residual_norm[2] = NaN
    forward, reverse = cubic_pair(
        forward_residual_status=residual_status,
        forward_residual_norm=residual_norm,
    )
    @test !forward.residual_validity[2]
    @test forward.gap_reasons[2] == :residual_unknown
    analysis = analyze_pair(forward, reverse)
    @test analysis.status == :unknown_model_residual_evidence
    @test !analysis.complete_model_residual_evidence
    @test !analysis.qualifies_as_dynamic_hysteresis

    failed_status = fill(:success, 5)
    failed_status[2] = :failure
    forward_event = make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5,
    )
    numeric_forward = make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
        solver_status=failed_status,
        events=[forward_event],
    )
    reverse_event = make_event(
        data.decreasing_control,
        data.reverse_branches,
        4,
        5,
    )
    numeric_reverse = make_trace(
        control=data.decreasing_control,
        direction=:decreasing,
        states=data.reverse_states,
        branch_ids=data.reverse_branches,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(numeric_forward),
        branch_structure_status=
            :multiple_stable_branches_certified,
        dynamics_residual_norm=data.reverse_residual,
        events=[reverse_event],
    )
    numeric_gap = analyze_pair(numeric_forward, numeric_reverse)
    @test numeric_gap.status == :unknown_numeric_gap
    @test !numeric_gap.complete_numeric_traces
end

@testset "budgets fail before comparison or event materialization" begin
    forward, reverse = cubic_pair()
    comparison_limits =
        RODynamicEvidenceLimits(max_common_comparisons=4)
    error = try
        analyze_pair(
            forward,
            reverse;
            limits=comparison_limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :common_control_comparisons

    data = cubic_data()
    event = make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5,
    )
    event_limits = RODynamicEvidenceLimits(max_events_per_trace=1)
    error = try
        make_trace(
            control=data.increasing_control,
            direction=:increasing,
            states=data.forward_states,
            branch_ids=data.forward_branches,
            branch_structure_status=
                :multiple_stable_branches_certified,
            dynamics_residual_norm=data.forward_residual,
            events=[event, event],
            limits=event_limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :events_per_trace

    payload_limits = RODynamicEvidenceLimits(max_payload_scalars=10)
    error = try
        make_trace(
            control=data.increasing_control,
            direction=:increasing,
            states=data.forward_states,
            branch_ids=data.forward_branches,
            branch_structure_status=
                :multiple_stable_branches_certified,
            dynamics_residual_norm=data.forward_residual,
            limits=payload_limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :payload_scalars
end

@testset "recomputed branch evidence and result hashes fail closed" begin
    forward, reverse = cubic_pair()
    forward_evidence, reverse_evidence = equilibrium_evidence_pair(
        forward, reverse, CUBIC_VECTOR_FIELD)
    @test forward_evidence.schema_version ==
        RO_DYNAMIC_BRANCH_EVIDENCE_VERSION
    @test forward_evidence.complete
    @test forward_evidence.all_stable
    @test forward_evidence.evidence_kind ==
        :declarative_polynomial_equilibrium
    @test forward_evidence.strong_equilibrium_certified
    branch_positional_values = Any[getfield(forward_evidence, field)
        for field in fieldnames(RODynamicBranchEvidence)]
    @test_throws MethodError RODynamicBranchEvidence(
        branch_positional_values...)
    @test validate_ro_equilibrium_branch_evidence(
        forward_evidence,
        forward,
        CUBIC_VECTOR_FIELD,
    )

    analysis = analyze_pair(
        forward,
        reverse;
        forward_branch_evidence=forward_evidence,
        reverse_branch_evidence=reverse_evidence,
    )
    @test analysis.complete_recomputed_branch_evidence
    @test analysis.complete_declarative_equilibrium_evidence
    @test analysis.equilibrium_vector_field_sha256 ==
        CUBIC_VECTOR_FIELD.identity_sha256
    @test analysis.analysis_identity_payload.result.status ==
        "conditional_equilibrium_branch_loop_on_tested_grid"
    @test validate_pair(
        analysis,
        forward,
        reverse,
        forward_evidence,
        reverse_evidence;
        vector_field=CUBIC_VECTOR_FIELD,
    )

    data = cubic_data()
    unnamed_branches = Union{Nothing,String}[fill(nothing, 5)...]
    unnamed_forward_event = make_event(
        data.increasing_control,
        unnamed_branches,
        4,
        5;
        pre_branch_id=nothing,
        post_branch_id=nothing,
    )
    unnamed_forward = make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=unnamed_branches,
        branch_structure_status=:unknown,
        dynamics_residual_norm=data.forward_residual,
        events=[unnamed_forward_event],
    )
    unnamed_reverse_event = make_event(
        data.decreasing_control,
        unnamed_branches,
        4,
        5;
        pre_branch_id=nothing,
        post_branch_id=nothing,
    )
    unnamed_reverse = make_trace(
        control=data.decreasing_control,
        direction=:decreasing,
        states=data.reverse_states,
        branch_ids=unnamed_branches,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(unnamed_forward),
        branch_structure_status=:unknown,
        dynamics_residual_norm=data.reverse_residual,
        events=[unnamed_reverse_event],
    )
    unnamed_forward_evidence, unnamed_reverse_evidence =
        equilibrium_evidence_pair(
            unnamed_forward,
            unnamed_reverse,
            CUBIC_VECTOR_FIELD,
        )
    unnamed_analysis = analyze_pair(
        unnamed_forward,
        unnamed_reverse;
        forward_branch_evidence=unnamed_forward_evidence,
        reverse_branch_evidence=unnamed_reverse_evidence,
    )
    @test unnamed_analysis.conditional_equilibrium_branch_loop_detected
    @test !unnamed_analysis.qualifies_as_dynamic_hysteresis
    @test unnamed_analysis.paired_branch_ids === nothing

    positional_values = Any[
        getfield(analysis, field)
        for field in fieldnames(RODynamicHysteresisAnalysis)
    ]
    @test_throws MethodError RODynamicHysteresisAnalysis(
        positional_values...)

    forward_evidence.stable[1] = false
    @test_throws ArgumentError validate_ro_equilibrium_branch_evidence(
        forward_evidence,
        forward,
        CUBIC_VECTOR_FIELD,
    )

    fresh_forward_evidence, fresh_reverse_evidence =
        equilibrium_evidence_pair(forward, reverse, CUBIC_VECTOR_FIELD)
    fresh_analysis = analyze_pair(
        forward,
        reverse;
        forward_branch_evidence=fresh_forward_evidence,
        reverse_branch_evidence=fresh_reverse_evidence,
    )
    fresh_analysis.closure_state_absolute_tolerances[1] = 0.0
    @test_throws ArgumentError validate_pair(
        fresh_analysis,
        forward,
        reverse,
        fresh_forward_evidence,
        fresh_reverse_evidence;
        vector_field=CUBIC_VECTOR_FIELD,
    )

    mutable_forward, mutable_reverse = cubic_pair()
    mutable_evidence, _ = equilibrium_evidence_pair(
        mutable_forward, mutable_reverse, CUBIC_VECTOR_FIELD)
    mutable_forward.outputs[2, 1] += 0.25
    @test_throws ArgumentError validate_ro_equilibrium_branch_evidence(
        mutable_evidence,
        mutable_forward,
        CUBIC_VECTOR_FIELD,
    )
end

@testset "single-branch finite-rate lag cannot be relabelled as branch switching" begin
    base_forward, base_reverse = linear_lag_cycle()
    forward_branches = Union{Nothing,String}[
        "A", "A", "A", "A", "B"]
    reverse_branches = Union{Nothing,String}[
        "B", "B", "B", "B", "A"]
    forward_event = make_event(
        base_forward.control, forward_branches, 4, 5)
    forward = make_trace(
        control=base_forward.control,
        direction=:increasing,
        states=vec(base_forward.states),
        outputs=vec(base_forward.outputs),
        branch_ids=forward_branches,
        model_identity=MODEL_LINEAR,
        branch_structure_status=:multiple_stable_branches_certified,
        events=[forward_event],
    )
    reverse_event = make_event(
        base_reverse.control, reverse_branches, 4, 5)
    reverse = make_trace(
        control=base_reverse.control,
        direction=:decreasing,
        states=vec(base_reverse.states),
        outputs=vec(base_reverse.outputs),
        branch_ids=reverse_branches,
        model_identity=MODEL_LINEAR,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
        branch_structure_status=:multiple_stable_branches_certified,
        events=[reverse_event],
    )
    protocol_analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.5,
        minimum_persistence_span=2.0,
    )
    @test protocol_analysis.status ==
        :finite_rate_lag_candidate_from_untrusted_callback
    @test protocol_analysis.evidence_class == :finite_rate_lag_candidate
    @test !protocol_analysis.branch_switch_hysteresis_certified
    @test !protocol_analysis.qualifies_as_dynamic_hysteresis

    forward_equilibrium, reverse_equilibrium = branch_evidence_pair(
        forward,
        reverse;
        evidence_kind=:equilibrium_branch,
        residual_evaluator=linear_equilibrium_residual,
        jacobian_evaluator=linear_equilibrium_jacobian,
    )
    @test !forward_equilibrium.complete
    @test forward_equilibrium.status == :recomputed_residual_failure
    equilibrium_analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.5,
        minimum_persistence_span=2.0,
        forward_branch_evidence=forward_equilibrium,
        reverse_branch_evidence=reverse_equilibrium,
        evidence_kind=:equilibrium_branch,
        residual_evaluator=linear_equilibrium_residual,
        jacobian_evaluator=linear_equilibrium_jacobian,
    )
    @test equilibrium_analysis.status ==
        :unknown_recomputed_branch_evidence
    @test !equilibrium_analysis.qualifies_as_dynamic_hysteresis

    declarative_forward, declarative_reverse = equilibrium_evidence_pair(
        forward, reverse, LINEAR_VECTOR_FIELD)
    @test !declarative_forward.complete
    @test !declarative_forward.strong_equilibrium_certified
    declarative_analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.5,
        minimum_persistence_span=2.0,
        forward_branch_evidence=declarative_forward,
        reverse_branch_evidence=declarative_reverse,
        vector_field=LINEAR_VECTOR_FIELD,
    )
    @test declarative_analysis.status ==
        :unknown_recomputed_branch_evidence
    @test !declarative_analysis.qualifies_as_dynamic_hysteresis

    lying_residual(_point) = Float64[0.0]
    lying_jacobian(_point) = reshape(Float64[-2.0], 1, 1)
    lying_forward, lying_reverse = branch_evidence_pair(
        forward,
        reverse;
        evidence_kind=:equilibrium_branch,
        residual_evaluator=lying_residual,
        jacobian_evaluator=lying_jacobian,
    )
    @test lying_forward.complete
    @test lying_forward.all_stable
    @test lying_forward.evidence_kind == :untrusted_callback
    @test !lying_forward.strong_equilibrium_certified
    lying_analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.5,
        minimum_persistence_span=2.0,
        forward_branch_evidence=lying_forward,
        reverse_branch_evidence=lying_reverse,
        evidence_kind=:equilibrium_branch,
        residual_evaluator=lying_residual,
        jacobian_evaluator=lying_jacobian,
        vector_field=nothing,
    )
    @test lying_analysis.evidence_class == :finite_protocol_loop
    @test !lying_analysis.complete_declarative_equilibrium_evidence
    @test !lying_analysis.branch_switch_hysteresis_certified
    @test !lying_analysis.qualifies_as_dynamic_hysteresis

    @test_throws ArgumentError analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.5,
        minimum_persistence_span=2.0,
        forward_branch_evidence=lying_forward,
        reverse_branch_evidence=lying_reverse,
        vector_field=LINEAR_VECTOR_FIELD,
    )
end

@testset "residual norms reject negative and conversion boundaries" begin
    common = (
        control=[0.0, 1.0],
        direction=:increasing,
        states=[0.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing],
    )
    @test_throws ArgumentError make_trace(
        ; common..., dynamics_residual_norm=Float64[-1.0, 0.0])
    @test_throws ArgumentError make_trace(
        ; common..., dynamics_residual_norm=Float64[-0.0, 0.0])
    @test_throws ArgumentError make_trace(
        ; common..., dynamics_residual_norm=Float64[-Inf, 0.0])
    @test_throws ArgumentError make_trace(
        ; common...,
        dynamics_residual_norm=BigFloat[big"1e-10000", big"0.0"])

    explicit_gap = make_trace(
        ; common...,
        dynamics_residual_norm=Float64[NaN, 0.0],
        dynamics_residual_status=[:unknown, :verified],
    )
    @test !explicit_gap.residual_validity[1]
    @test explicit_gap.gap_reasons[1] == :residual_unknown

    forward, _ = cubic_pair()
    @test_throws ArgumentError certify_ro_dynamic_branch_evidence(
        forward;
        evidence_kind=:equilibrium_branch,
        residual_evaluator=cubic_equilibrium_residual,
        jacobian_evaluator=cubic_equilibrium_jacobian,
        residual_tolerance=1.0,
        stability_margin=1e-6,
    )
end

@testset "unknown stability cannot certify finite-rate lag" begin
    base_forward, base_reverse = linear_lag_cycle()
    unknown_stability = fill(:unknown, 5)
    forward = make_trace(
        control=base_forward.control,
        direction=:increasing,
        states=vec(base_forward.states),
        outputs=vec(base_forward.outputs),
        branch_ids=Union{Nothing,String}[fill("unique", 5)...],
        model_identity=MODEL_LINEAR,
        local_stability=unknown_stability,
        branch_structure_status=:unique_stable_branch_certified,
    )
    reverse = make_trace(
        control=base_reverse.control,
        direction=:decreasing,
        states=vec(base_reverse.states),
        outputs=vec(base_reverse.outputs),
        branch_ids=Union{Nothing,String}[fill("unique", 5)...],
        model_identity=MODEL_LINEAR,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward),
        local_stability=unknown_stability,
        branch_structure_status=:unique_stable_branch_certified,
    )
    analysis = analyze_pair(
        forward,
        reverse;
        separation_threshold=0.5,
        jump_threshold=0.2,
        minimum_persistence_span=2.0,
    )
    @test analysis.status == :unknown_recomputed_branch_evidence
    @test !analysis.finite_rate_lag_detected
    @test analysis.stable_common_comparison_count == 0
end

@testset "strong tolerance policies fail closed" begin
    small_forward, small_reverse = cubic_pair(
        forward_rate=1e-12,
        reverse_rate=5e-10,
    )
    @test_throws ArgumentError analyze_pair(
        small_forward,
        small_reverse;
        rate_match_absolute_tolerance=1e-9,
    )
    @test_throws ArgumentError make_trace(
        control=[0.0, 1.0],
        direction=:increasing,
        states=[0.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing],
        rate=1e-12,
        rate_absolute_tolerance=1e-9,
    )

    forward, reverse = cubic_pair()
    reverse.initial_state[1] += 100.0
    reverse.states[1, 1] += 100.0
    @test_throws ArgumentError analyze_pair(
        forward,
        reverse;
        closure_state_absolute_tolerances=[101.0],
    )
    fresh_forward, fresh_reverse = cubic_pair()
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        closure_state_absolute_tolerance_units=["wrong-state-unit"],
    )

    @test !RODynamicHysteresisContractModule._rodh_state_rows_close(
        Float64[0.0],
        Float64[5e-7],
        Float64[0.0],
        1e-6,
    )
    common = (
        control=[0.0, 1.0],
        direction=:increasing,
        states=[0.0, 0.0],
        branch_ids=Union{Nothing,String}[nothing, nothing],
    )
    @test_throws ArgumentError make_trace(
        ; common..., rate_relative_tolerance=Float32(1e-9))
    @test_throws ArgumentError make_trace(
        ; common..., rate_absolute_tolerance=big"0.0")
    @test_throws ArgumentError make_trace(
        ; common...,
        trajectory_solver_relative_tolerance=Float32(1e-10))
    @test_throws ArgumentError make_trace(
        ; common..., trajectory_solver_absolute_tolerance=true)
    @test_throws ArgumentError make_trace(
        ; common..., dynamics_residual_tolerance=Float32(1e-8))
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        separation_threshold=Float32(1.0),
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        jump_threshold=big"1.0",
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        minimum_persistence_span=true,
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        closure_state_relative_tolerance=Float32(1e-10),
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        branch_state_relative_tolerance=big"1e-10",
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        rate_match_relative_tolerance=Float32(1e-9),
    )
    @test_throws ArgumentError analyze_pair(
        fresh_forward,
        fresh_reverse;
        rate_match_absolute_tolerance=true,
    )
    data = cubic_data()
    @test_throws ArgumentError make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5;
        control_uncertainty=Float32(0.0),
    )
    @test_throws TypeError certify_ro_equilibrium_branch_evidence(
        fresh_forward,
        CUBIC_VECTOR_FIELD;
        residual_tolerance=Float32(1e-8),
        stability_margin=1e-6,
    )
end

@testset "event-pair work is bounded and analysis is cancellable" begin
    data = cubic_data()
    forward_events = [make_event(
        data.increasing_control,
        data.forward_branches,
        4,
        5;
        control_uncertainty=uncertainty,
    ) for uncertainty in (0.0, 0.01, 0.02)]
    reverse_events = [make_event(
        data.decreasing_control,
        data.reverse_branches,
        4,
        5;
        control_uncertainty=uncertainty,
    ) for uncertainty in (0.0, 0.01, 0.02)]
    @test_throws ArgumentError make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        branch_structure_status=:multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
        events=[forward_events[1], forward_events[1]],
    )

    limits = RODynamicEvidenceLimits(max_event_pair_comparisons=8)
    forward = make_trace(
        control=data.increasing_control,
        direction=:increasing,
        states=data.forward_states,
        branch_ids=data.forward_branches,
        branch_structure_status=:multiple_stable_branches_certified,
        dynamics_residual_norm=data.forward_residual,
        events=forward_events,
        limits=limits,
    )
    reverse = make_trace(
        control=data.decreasing_control,
        direction=:decreasing,
        states=data.reverse_states,
        branch_ids=data.reverse_branches,
        lineage_predecessor_trace_sha256=
            ro_dynamic_protocol_identity_sha256(forward; limits=limits),
        branch_structure_status=:multiple_stable_branches_certified,
        dynamics_residual_norm=data.reverse_residual,
        events=reverse_events,
        limits=limits,
    )
    forward_evidence, reverse_evidence = equilibrium_evidence_pair(
        forward,
        reverse,
        CUBIC_VECTOR_FIELD;
        limits=limits,
    )
    error = try
        analyze_pair(
            forward,
            reverse;
            forward_branch_evidence=forward_evidence,
            reverse_branch_evidence=reverse_evidence,
            limits=limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :event_pair_comparisons
    @test error.requested == 9

    cancellable_forward, cancellable_reverse = cubic_pair()
    cancellable_forward_evidence, cancellable_reverse_evidence =
        equilibrium_evidence_pair(
            cancellable_forward,
            cancellable_reverse,
            CUBIC_VECTOR_FIELD,
        )
    cancellation_checks = Ref(0)
    cancel_check = () -> begin
        cancellation_checks[] += 1
        cancellation_checks[] > 3 && throw(InterruptException())
    end
    @test_throws InterruptException analyze_pair(
        cancellable_forward,
        cancellable_reverse;
        forward_branch_evidence=cancellable_forward_evidence,
        reverse_branch_evidence=cancellable_reverse_evidence,
        cancel_check=cancel_check,
    )
    @test cancellation_checks[] > 3
end

@testset "late exact-grid, window, area, and event-sort loops cancel" begin
    forward, reverse = cubic_pair()
    @test_throws InterruptException begin
        RODynamicHysteresisContractModule._rodh_exact_reverse_grid(
            forward,
            reverse,
            () -> throw(InterruptException()),
        )
    end
    windows = RODynamicHysteresisContractModule._RODHSignedWindow[
        RODynamicHysteresisContractModule._RODHSignedWindow(1, 2, 1),
        RODynamicHysteresisContractModule._RODHSignedWindow(2, 4, 1),
    ]
    @test_throws InterruptException begin
        RODynamicHysteresisContractModule._rodh_best_window(
            windows,
            forward.control,
            () -> throw(InterruptException()),
        )
    end
    @test_throws InterruptException begin
        RODynamicHysteresisContractModule._rodh_sampled_loop_areas(
            forward.control,
            zeros(Float64, length(forward.control)),
            trues(length(forward.control)),
            () -> throw(InterruptException()),
        )
    end

    data = cubic_data()
    events = RODynamicSwitchEvent[
        make_event(
            data.increasing_control,
            data.forward_branches,
            4,
            5;
            control_uncertainty=0.01,
        ),
        make_event(
            data.increasing_control,
            data.forward_branches,
            4,
            5;
            control_uncertainty=0.0,
        ),
    ]
    sort_checks = Ref(0)
    @test_throws InterruptException begin
        RODynamicHysteresisContractModule._rodh_sort_events!(
            events,
            () -> begin
                sort_checks[] += 1
                sort_checks[] > 1 && throw(InterruptException())
            end,
        )
    end
    @test sort_checks[] > 1
end

@testset "static multiple roots never become dynamic hysteresis" begin
    static_inputs = (
        model_identity=MODEL_CUBIC,
        coordinate_chart_id=CHART,
        control_id="u1",
        control_unit="control-unit",
        control_value=0.0,
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        states=reshape(Float64[-1.0, 1.0], :, 1),
        outputs=reshape(Float64[-1.0, 1.0], :, 1),
        solver_status=[:success, :success],
        local_stability=[:stable, :stable],
    )
    evidence = analyze_static_multiroot(; static_inputs...)
    @test evidence.schema_version == RO_STATIC_MULTIROOT_EVIDENCE_VERSION
    @test evidence.status == :multiple_stable_roots_at_tested_control
    @test evidence.distinct_stable_root_pair_count == 1
    @test !evidence.qualifies_as_dynamic_hysteresis
    @test !evidence.global_multistability_claimed
    @test !evidence.experimental_causality_claimed
    @test startswith(evidence.identity_sha256, "sha256:")
    @test evidence.identity_payload.result.status ==
        "multiple_stable_roots_at_tested_control"
    @test evidence.identity_payload.result.root_count == 2
    @test !evidence.identity_payload.result.qualifies_as_dynamic_hysteresis
    @test validate_ro_static_multiroot_evidence(
        evidence; static_inputs...)
    @test_throws ArgumentError validate_ro_static_multiroot_evidence(
        evidence;
        static_inputs...,
        distinct_root_threshold=1e-7,
    )
    positional_values = Any[getfield(evidence, field)
        for field in fieldnames(typeof(evidence))]
    @test_throws MethodError typeof(evidence)(positional_values...)
    @test_throws ArgumentError analyze_static_multiroot(
        ; static_inputs..., distinct_root_threshold=Float32(1e-8))
    @test_throws ArgumentError analyze_static_multiroot(
        ; static_inputs..., distinct_root_threshold=big"1e-8")

    limits = RODynamicEvidenceLimits(max_common_comparisons=2)
    error = try
        analyze_static_multiroot(
            model_identity=MODEL_CUBIC,
            coordinate_chart_id=CHART,
            control_id="u1",
            control_unit="control-unit",
            control_value=0.0,
            state_ids=["x"],
            state_units=["state-unit"],
            output_ids=["reporter"],
            output_units=["signal-unit"],
            states=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
            outputs=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
            solver_status=fill(:success, 3),
            local_stability=fill(:stable, 3),
            limits=limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :static_root_comparisons

    work_limits = RODynamicEvidenceLimits(max_analysis_work_units=11)
    error = try
        analyze_static_multiroot(
            model_identity=MODEL_CUBIC,
            coordinate_chart_id=CHART,
            control_id="u1",
            control_unit="control-unit",
            control_value=0.0,
            state_ids=["x"],
            state_units=["state-unit"],
            output_ids=["reporter"],
            output_units=["signal-unit"],
            states=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
            outputs=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
            solver_status=fill(:success, 3),
            local_stability=fill(:stable, 3),
            limits=work_limits,
        )
        nothing
    catch caught
        caught
    end
    @test error isa RODynamicEvidenceLimitExceeded
    @test error.phase == :static_root_work_units

    stable_checks = Ref(0)
    analyze_static_multiroot(
        model_identity=MODEL_CUBIC,
        coordinate_chart_id=CHART,
        control_id="u1",
        control_unit="control-unit",
        control_value=0.0,
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        states=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
        outputs=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
        solver_status=fill(:success, 3),
        local_stability=fill(:stable, 3),
        cancel_check=() -> (stable_checks[] += 1),
    )
    unstable_checks = Ref(0)
    analyze_static_multiroot(
        model_identity=MODEL_CUBIC,
        coordinate_chart_id=CHART,
        control_id="u1",
        control_unit="control-unit",
        control_value=0.0,
        state_ids=["x"],
        state_units=["state-unit"],
        output_ids=["reporter"],
        output_units=["signal-unit"],
        states=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
        outputs=reshape(Float64[-1.0, 0.0, 1.0], :, 1),
        solver_status=fill(:success, 3),
        local_stability=fill(:unstable, 3),
        cancel_check=() -> (unstable_checks[] += 1),
    )
    @test stable_checks[] > unstable_checks[]
end
