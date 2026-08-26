module RODynamicTrajectoryContractModule
using Test
import BindingAndCatalysis
import SHA
const RODT = BindingAndCatalysis

content_hash(label) = "sha256:" *
    bytes2hex(SHA.sha256(codeunits(String(label))))

const CHART = content_hash("trajectory-affine-chart-v1")
const DYNAMICS_POLICY = content_hash("trajectory-dynamics-policy-v1")
const RESIDUAL_POLICY = content_hash("trajectory-residual-policy-v1")
const SOLVER_POLICY = content_hash("trajectory-vector-field-solver-policy-v1")
const STABILITY_POLICY = content_hash("trajectory-stability-policy-v1")
const BRANCH_POLICY = content_hash("trajectory-branch-policy-v1")
const EVENT_POLICY = content_hash("trajectory-event-policy-v1")
const PROTOCOL_FAMILY = content_hash("trajectory-triangular-family-v1")

const LINEAR_FIELD = RODT.ro_polynomial_vector_field(
    coordinate_chart_id=CHART,
    dynamics_policy_sha256=DYNAMICS_POLICY,
    residual_policy_sha256=RESIDUAL_POLICY,
    solver_policy_sha256=SOLVER_POLICY,
    stability_policy_sha256=STABILITY_POLICY,
    branch_policy_sha256=BRANCH_POLICY,
    event_policy_sha256=EVENT_POLICY,
    state_ids=["x"],
    state_units=["state-unit"],
    control_id="u",
    control_unit="control-unit",
    time_unit="s",
    equations=[[
        (coefficient=-2.0, exponents=[1, 0]),
        (coefficient=2.0, exponents=[0, 1]),
    ]],
)

const CUBIC_FIELD = RODT.ro_polynomial_vector_field(
    coordinate_chart_id=CHART,
    dynamics_policy_sha256=DYNAMICS_POLICY,
    residual_policy_sha256=RESIDUAL_POLICY,
    solver_policy_sha256=SOLVER_POLICY,
    stability_policy_sha256=STABILITY_POLICY,
    branch_policy_sha256=BRANCH_POLICY,
    event_policy_sha256=EVENT_POLICY,
    state_ids=["x"],
    state_units=["state-unit"],
    control_id="u",
    control_unit="control-unit",
    time_unit="s",
    equations=[[
        (coefficient=-1.0, exponents=[3, 0]),
        (coefficient=1.0, exponents=[1, 0]),
        (coefficient=1.0, exponents=[0, 1]),
    ]],
)

struct TrajectoryCancelled <: Exception end

struct TrajectoryControlReadProbe <: AbstractVector{Float64}
    values::Vector{Float64}
    reads::Base.RefValue{Int}
end

Base.size(probe::TrajectoryControlReadProbe) = size(probe.values)
Base.IndexStyle(::Type{TrajectoryControlReadProbe}) = IndexLinear()
function Base.getindex(probe::TrajectoryControlReadProbe, index::Int)
    probe.reads[] += 1
    return probe.values[index]
end

function linear_spec(;
    control_grid=Float64[0.0, 0.5, 1.0, 1.5, 2.0],
    direction::Symbol=:increasing,
    initial_state=Float64[0.0],
    lineage_predecessor_evidence_sha256=nothing,
    protocol_id=direction == :increasing ? "linear-forward" : "linear-reverse",
    rate=1.0,
    rate_unit="control-unit/s",
    time_unit="s",
    primary_relative_tolerance=1.0e-8,
    primary_absolute_tolerance=1.0e-10,
    audit_relative_tolerance=1.0e-10,
    audit_absolute_tolerance=1.0e-12,
    agreement_relative_tolerance=1.0e-6,
    agreement_absolute_tolerance=1.0e-8,
    state_lower_bounds=Float64[-10.0],
    state_upper_bounds=Float64[10.0],
    maximum_step=0.05,
    limits=RODT.RODynamicTrajectoryLimits(),
    cancel_check=nothing,
)
    return RODT.ro_dynamic_protocol_spec(
        LINEAR_FIELD;
        protocol_family_sha256=PROTOCOL_FAMILY,
        protocol_id=protocol_id,
        lineage_predecessor_evidence_sha256=
            lineage_predecessor_evidence_sha256,
        direction=direction,
        control_grid=control_grid,
        rate=rate,
        rate_unit=rate_unit,
        time_unit=time_unit,
        initial_condition_id=direction == :increasing ?
            "linear-low" : "linear-high",
        initial_state=initial_state,
        state_lower_bounds=state_lower_bounds,
        state_upper_bounds=state_upper_bounds,
        primary_relative_tolerance=primary_relative_tolerance,
        primary_absolute_tolerance=primary_absolute_tolerance,
        audit_relative_tolerance=audit_relative_tolerance,
        audit_absolute_tolerance=audit_absolute_tolerance,
        maximum_step=maximum_step,
        agreement_relative_tolerance=agreement_relative_tolerance,
        agreement_absolute_tolerance=agreement_absolute_tolerance,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function stable_cubic_root(control::Float64, initial::Float64)
    value = initial
    for _ in 1:100
        residual = value - value^3 + control
        derivative = 1.0 - 3.0value^2
        value -= residual / derivative
    end
    @test abs(value - value^3 + control) <= 1.0e-12
    @test 1.0 - 3.0value^2 < 0.0
    return value
end

function cubic_spec(;
    control_grid=Float64[-0.6, -0.3, 0.0, 0.3, 0.6],
    direction::Symbol=:increasing,
    initial_state=Float64[stable_cubic_root(-0.6, -1.5)],
    lineage_predecessor_evidence_sha256=nothing,
    protocol_id=direction == :increasing ? "cubic-forward" : "cubic-reverse",
    limits=RODT.RODynamicTrajectoryLimits(),
)
    return RODT.ro_dynamic_protocol_spec(
        CUBIC_FIELD;
        protocol_family_sha256=PROTOCOL_FAMILY,
        protocol_id=protocol_id,
        lineage_predecessor_evidence_sha256=
            lineage_predecessor_evidence_sha256,
        direction=direction,
        control_grid=control_grid,
        rate=0.02,
        rate_unit="control-unit/s",
        time_unit="s",
        initial_condition_id=direction == :increasing ?
            "cubic-low" : "cubic-high",
        initial_state=initial_state,
        state_lower_bounds=Float64[-3.0],
        state_upper_bounds=Float64[3.0],
        primary_relative_tolerance=1.0e-8,
        primary_absolute_tolerance=1.0e-10,
        audit_relative_tolerance=1.0e-10,
        audit_absolute_tolerance=1.0e-12,
        maximum_step=0.02,
        agreement_relative_tolerance=1.0e-5,
        agreement_absolute_tolerance=1.0e-7,
        limits=limits,
    )
end

function copy_evidence_with_flag(evidence, field_name::Symbol)
    fields = fieldnames(RODT.ROModelBackedTrajectoryEvidence)
    values = Any[getfield(evidence, field) for field in fields]
    index = only(findall(==(field_name), fields))
    values[index] = true
    return RODT.ROModelBackedTrajectoryEvidence(
        RODT._RODT_VALIDATED, values...)
end

function copy_evidence_with_rehashed_field(
    evidence,
    field_name::Symbol,
    value,
)
    fields = fieldnames(RODT.ROModelBackedTrajectoryEvidence)
    values = Any[getfield(evidence, field) for field in fields]
    values[only(findall(==(field_name), fields))] = value
    candidate = RODT.ROModelBackedTrajectoryEvidence(
        RODT._RODT_VALIDATED, values...)
    payload = RODT._rodt_evidence_payload_from_fields(candidate)
    values[only(findall(==(:identity_payload), fields))] = payload
    values[only(findall(==(:identity_sha256), fields))] =
        RODT._rodh_payload_sha256(payload)
    return RODT.ROModelBackedTrajectoryEvidence(
        RODT._RODT_VALIDATED, values...)
end

function copy_spec_with_runtime_version_identity(spec, runtime_identity)
    runtime_hash = RODT._rodh_payload_sha256(runtime_identity)
    solver_config = merge(
        spec.identity_payload.solver.config,
        (
            runtime_version_identity_sha256=runtime_hash,
            runtime_version_identity=runtime_identity,
        ),
    )
    solver_policy_hash = RODT._rodh_payload_sha256(solver_config)
    identity_payload = merge(
        spec.identity_payload,
        (solver=(
            policy_sha256=solver_policy_hash,
            config=solver_config,
        ),),
    )
    fields = fieldnames(RODT.RODynamicProtocolSpec)
    values = Any[getfield(spec, field) for field in fields]
    values[only(findall(==(:solver_runtime_version_identity), fields))] =
        runtime_identity
    values[only(findall(
        ==(:solver_runtime_version_identity_sha256), fields))] = runtime_hash
    values[only(findall(==(:solver_policy_sha256), fields))] =
        solver_policy_hash
    values[only(findall(==(:identity_payload), fields))] = identity_payload
    values[only(findall(==(:identity_sha256), fields))] =
        RODT._rodh_payload_sha256(identity_payload)
    return RODT.RODynamicProtocolSpec(RODT._RODT_VALIDATED, values...)
end

@testset "dynamic protocol specs are canonical, strict, and bounded" begin
    spec = linear_spec()
    @test spec.schema_version == RODT.RO_DYNAMIC_PROTOCOL_SPEC_VERSION
    @test spec.vector_field_sha256 == LINEAR_FIELD.identity_sha256
    @test spec.primary_solver_id == :ordinarydiffeq_tsit5
    @test spec.audit_solver_id == :ordinarydiffeq_vern7
    @test spec.time_grid == Float64[0.0, 0.5, 1.0, 1.5, 2.0]
    @test spec.time_unit == LINEAR_FIELD.time_unit == "s"
    @test spec.rate_unit == "control-unit/s"
    @test spec.identity_payload.protocol.rate_unit_identity == (
        numerator=(role="swept_control", unit="control-unit"),
        denominator=(role="model_time", unit="s"),
        display="control-unit/s",
    )
    @test spec.output_state_indices == [1]
    @test spec.output_ids == ["x"]
    @test spec.output_units == ["state-unit"]
    @test startswith(spec.identity_sha256, "sha256:")
    @test spec.identity_payload.solver.policy_sha256 ==
        spec.solver_policy_sha256
    runtime_identity = spec.solver_runtime_version_identity
    @test runtime_identity.schema_version ==
        RODT.RO_DYNAMIC_SOLVER_RUNTIME_VERSION_IDENTITY_VERSION
    @test runtime_identity.identity_scope ==
        "runtime_package_and_algorithm_module_versions_only"
    @test runtime_identity.julia.version == string(VERSION)
    @test runtime_identity.ordinarydiffeq.package_name == "OrdinaryDiffEq"
    @test runtime_identity.scimlbase.package_name == "SciMLBase"
    @test runtime_identity.primary_algorithm.solver_id ==
        "ordinarydiffeq_tsit5"
    @test runtime_identity.audit_algorithm.solver_id ==
        "ordinarydiffeq_vern7"
    @test !runtime_identity.external_provenance.
        complete_active_manifest_sha256_embedded
    @test spec.identity_payload.solver.config.runtime_version_identity ==
        runtime_identity
    @test spec.solver_runtime_version_identity_sha256 ==
        RODT._rodh_payload_sha256(runtime_identity)
    @test RODT.validate_ro_dynamic_protocol_spec(spec, LINEAR_FIELD)
    @test linear_spec().identity_sha256 == spec.identity_sha256

    validation_entry_checks = Ref(0)
    @test_throws TrajectoryCancelled RODT.validate_ro_dynamic_protocol_spec(
        spec,
        LINEAR_FIELD;
        cancel_check=() -> begin
            validation_entry_checks[] += 1
            throw(TrajectoryCancelled())
        end,
    )
    @test validation_entry_checks[] == 1

    positional = Any[getfield(spec, field)
        for field in fieldnames(RODT.RODynamicProtocolSpec)]
    @test_throws MethodError RODT.RODynamicProtocolSpec(positional...)
    @test_throws ArgumentError linear_spec(
        control_grid=Float64[0.0, 1.0, 0.5])
    @test_throws ArgumentError linear_spec(
        control_grid=Float32[0.0, 1.0])
    @test_throws ArgumentError linear_spec(
        audit_relative_tolerance=1.0e-7)
    @test_throws ArgumentError linear_spec(
        state_lower_bounds=Float64[0.1])
    @test_throws ArgumentError linear_spec(maximum_step=3.0)
    @test_throws ArgumentError RODT.ro_dynamic_protocol_spec(
        LINEAR_FIELD;
        protocol_family_sha256=PROTOCOL_FAMILY,
        protocol_id="duplicate-output",
        direction=:increasing,
        control_grid=Float64[0.0, 1.0],
        rate=1.0,
        rate_unit="control-unit/s",
        time_unit="s",
        initial_condition_id="duplicate-output",
        initial_state=Float64[0.0],
        output_state_indices=[1, 1],
        state_lower_bounds=Float64[-1.0],
        state_upper_bounds=Float64[1.0],
        maximum_step=0.1,
    )

    point_limits = RODT.RODynamicTrajectoryLimits(max_protocol_points=2)
    error = try
        linear_spec(limits=point_limits)
        nothing
    catch caught
        caught
    end
    @test error isa RODT.RODynamicTrajectoryLimitExceeded
    @test error.phase == :protocol_points

    scalar_limits = RODT.RODynamicTrajectoryLimits(max_state_scalars=2)
    scalar_error = try
        linear_spec(
            control_grid=Float64[0.0, 0.5, 1.0],
            limits=scalar_limits,
        )
        nothing
    catch caught
        caught
    end
    @test scalar_error isa RODT.RODynamicTrajectoryLimitExceeded
    @test scalar_error.phase == :state_scalars

    work_limits = RODT.RODynamicTrajectoryLimits(
        max_total_work_units=1)
    work_error = try
        linear_spec(limits=work_limits)
        nothing
    catch caught
        caught
    end
    @test work_error isa RODT.RODynamicTrajectoryLimitExceeded
    @test work_error.phase == :trajectory_work_units

    reads = Ref(0)
    probe = TrajectoryControlReadProbe(Float64[0.0, 1.0], reads)
    @test_throws TrajectoryCancelled linear_spec(
        control_grid=probe,
        cancel_check=() -> throw(TrajectoryCancelled()),
    )
    @test reads[] == 0

    mutable_spec = linear_spec()
    mutable_spec.control_grid[2] = 0.75
    @test_throws ArgumentError RODT.validate_ro_dynamic_protocol_spec(
        mutable_spec, LINEAR_FIELD)

    foreign_runtime_identity = merge(
        runtime_identity,
        (julia=merge(
            runtime_identity.julia,
            (version="0.0.0-foreign-runtime",),
        ),),
    )
    foreign_spec = copy_spec_with_runtime_version_identity(
        spec, foreign_runtime_identity)
    mismatch = try
        RODT.simulate_ro_model_backed_trajectory(
            foreign_spec, LINEAR_FIELD)
        nothing
    catch caught
        caught
    end
    @test mismatch isa RODT.RODynamicTrajectoryRuntimeIdentityMismatch
    @test mismatch.declared_version_identity_sha256 ==
        foreign_spec.solver_runtime_version_identity_sha256
    @test mismatch.current_version_identity_sha256 ==
        spec.solver_runtime_version_identity_sha256
end

@testset "analytic monostable ramp produces model-backed evidence only" begin
    spec = linear_spec()
    evidence = RODT.simulate_ro_model_backed_trajectory(
        spec, LINEAR_FIELD)
    @test evidence.schema_version ==
        RODT.RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_VERSION
    @test evidence.evidence_scope ==
        RODT.RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE
    @test evidence.status ==
        :complete_model_backed_finite_protocol_trajectory
    @test evidence.complete_model_backed_finite_protocol_trajectory
    @test evidence.complete_save_grid
    @test evidence.all_states_finite
    @test evidence.state_bounds_respected
    @test evidence.primary_retcode == "Success"
    @test evidence.audit_retcode == "Success"
    @test evidence.primary_rhs_evaluations > 0
    @test evidence.audit_rhs_evaluations > 0
    @test evidence.primary_accepted_steps > 0
    @test evidence.audit_accepted_steps > 0
    @test evidence.maximum_scaled_state_discrepancy <= 1.0
    @test evidence.trace.model_solver_policy_sha256 ==
        LINEAR_FIELD.solver_policy_sha256
    @test evidence.trace.trajectory_solver_policy_sha256 ==
        spec.solver_policy_sha256
    @test evidence.trace.model_solver_policy_sha256 !=
        evidence.trace.trajectory_solver_policy_sha256
    @test all(evidence.trace.numeric_validity)
    @test all(evidence.trace.numerical_agreement_validity)
    @test !any(evidence.trace.residual_validity)
    @test !any(evidence.trace.validity)
    @test all(==(:unknown), evidence.trace.dynamics_residual_status)
    @test all(isnan, evidence.trace.dynamics_residual_norm)
    @test all(==(:verified), evidence.trace.numerical_agreement_status)
    @test evidence.trace.event_detection_status == :unknown
    @test all(isnothing, evidence.trace.branch_ids)
    @test !evidence.validated_error_enclosure
    @test !evidence.branch_switch_certified
    @test !evidence.qualifies_as_dynamic_hysteresis
    @test !evidence.global_reachability_certified
    @test !evidence.basin_completeness_certified
    @test !evidence.experimental_causality_claimed
    @test RODT.validate_ro_model_backed_trajectory_evidence(
        evidence, LINEAR_FIELD)

    expected = Float64[
        time - 0.5 + 0.5exp(-2time) for time in spec.time_grid]
    @test maximum(abs.(vec(evidence.trace.states) .- expected)) <= 2.0e-8
    @test evidence.trace.outputs == evidence.trace.states
    replay = RODT.simulate_ro_model_backed_trajectory(
        spec, LINEAR_FIELD)
    @test replay.identity_sha256 == evidence.identity_sha256

    positional = Any[getfield(evidence, field)
        for field in fieldnames(RODT.ROModelBackedTrajectoryEvidence)]
    @test_throws MethodError RODT.ROModelBackedTrajectoryEvidence(
        positional...)
    for field in (
        :validated_error_enclosure,
        :branch_switch_certified,
        :qualifies_as_dynamic_hysteresis,
        :global_reachability_certified,
        :basin_completeness_certified,
        :experimental_causality_claimed,
    )
        forged = copy_evidence_with_flag(evidence, field)
        @test_throws ArgumentError begin
            RODT.validate_ro_model_backed_trajectory_evidence(
                forged, LINEAR_FIELD)
        end
    end

    tampered = RODT.simulate_ro_model_backed_trajectory(
        spec, LINEAR_FIELD)
    tampered.trace.states[2, 1] += 1.0
    @test_throws ArgumentError begin
        RODT.validate_ro_model_backed_trajectory_evidence(
            tampered, LINEAR_FIELD)
    end
end

@testset "forward-to-reverse lineage is exact and reproducible" begin
    forward_spec = linear_spec()
    forward = RODT.simulate_ro_model_backed_trajectory(
        forward_spec, LINEAR_FIELD)
    reverse_spec = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
    )
    reverse_evidence = RODT.simulate_ro_model_backed_trajectory(
        reverse_spec,
        LINEAR_FIELD;
        predecessor_evidence=forward,
    )
    @test reverse_evidence.complete_model_backed_finite_protocol_trajectory
    @test reverse_evidence.predecessor_evidence_sha256 ==
        forward.identity_sha256
    @test reverse_evidence.trace.lineage_predecessor_trace_sha256 ==
        forward.trace_sha256
    @test reverse_evidence.trace.initial_state ==
        vec(copy(@view forward.trace.states[end, :]))
    @test RODT.validate_ro_model_backed_trajectory_evidence(
        reverse_evidence,
        LINEAR_FIELD;
        predecessor_evidence=forward,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        reverse_spec, LINEAR_FIELD)

    wrong_initial = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=Float64[forward.trace.states[end, 1] + 0.1],
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        wrong_initial,
        LINEAR_FIELD;
        predecessor_evidence=forward,
    )

    independent_reverse = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        independent_reverse,
        LINEAR_FIELD;
        predecessor_evidence=forward,
    )

    p8b_analysis = RODT.analyze_dynamic_hysteresis(
        forward.trace,
        reverse_evidence.trace;
        output_id="x",
        separation_threshold=0.1,
        jump_threshold=0.1,
        minimum_persistence_points=2,
        minimum_persistence_span=0.25,
        closure_state_absolute_tolerances=Float64[1.0e-8],
        closure_state_absolute_tolerance_units=["state-unit"],
        branch_state_absolute_tolerances=Float64[1.0e-8],
        branch_state_absolute_tolerance_units=["state-unit"],
        rate_match_absolute_tolerance=0.0,
        rate_match_absolute_tolerance_unit="control-unit/s",
    )
    @test !p8b_analysis.complete_model_residual_evidence

    forged_forward = copy_evidence_with_rehashed_field(
        forward,
        :maximum_absolute_state_discrepancy,
        forward.maximum_absolute_state_discrepancy + 0.25,
    )
    @test RODT._rodt_evidence_payload_from_fields(forged_forward) ==
        forged_forward.identity_payload
    @test RODT._rodh_payload_sha256(forged_forward.identity_payload) ==
        forged_forward.identity_sha256
    forged_child_spec = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=
            forged_forward.identity_sha256,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        forged_child_spec,
        LINEAR_FIELD;
        predecessor_evidence=forged_forward,
    )

    @test_throws ArgumentError linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
        rate_unit="different-control-unit/s",
    )
    @test_throws ArgumentError linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
        time_unit="minute",
    )
    wrong_solver_policy = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
        maximum_step=0.04,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        wrong_solver_policy,
        LINEAR_FIELD;
        predecessor_evidence=forward,
    )

    decreasing_root_spec = linear_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
    )
    decreasing_root = RODT.simulate_ro_model_backed_trajectory(
        decreasing_root_spec, LINEAR_FIELD)
    increasing_child = linear_spec(
        control_grid=reverse(copy(decreasing_root_spec.control_grid)),
        direction=:increasing,
        initial_state=vec(copy(@view decreasing_root.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=decreasing_root.identity_sha256,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        increasing_child,
        LINEAR_FIELD;
        predecessor_evidence=decreasing_root,
    )

    chained_child = linear_spec(
        control_grid=reverse(copy(reverse_spec.control_grid)),
        direction=:increasing,
        initial_state=vec(copy(@view reverse_evidence.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=reverse_evidence.identity_sha256,
    )
    @test_throws ArgumentError RODT.simulate_ro_model_backed_trajectory(
        chained_child,
        LINEAR_FIELD;
        predecessor_evidence=reverse_evidence,
    )
end

@testset "bistable cubic trajectory remains below branch-switch hysteresis" begin
    forward_spec = cubic_spec()
    forward = RODT.simulate_ro_model_backed_trajectory(
        forward_spec, CUBIC_FIELD)
    @test forward.complete_model_backed_finite_protocol_trajectory
    @test forward.trace.states[1, 1] < -1.0
    @test forward.trace.states[end, 1] > 1.0
    @test !forward.branch_switch_certified
    @test !forward.qualifies_as_dynamic_hysteresis

    reverse_spec = cubic_spec(
        control_grid=reverse(copy(forward_spec.control_grid)),
        direction=:decreasing,
        initial_state=vec(copy(@view forward.trace.states[end, :])),
        lineage_predecessor_evidence_sha256=forward.identity_sha256,
    )
    reverse_evidence = RODT.simulate_ro_model_backed_trajectory(
        reverse_spec,
        CUBIC_FIELD;
        predecessor_evidence=forward,
    )
    @test reverse_evidence.complete_model_backed_finite_protocol_trajectory
    @test reverse_evidence.trace.states[1, 1] > 1.0
    @test reverse_evidence.trace.states[end, 1] < -1.0
    @test maximum(abs.(
        vec(forward.trace.states) .-
        reverse(vec(reverse_evidence.trace.states)))) > 1.0
    @test all(isnothing, forward.trace.branch_ids)
    @test all(isnothing, reverse_evidence.trace.branch_ids)
    @test forward.trace.event_detection_status == :unknown
    @test reverse_evidence.trace.event_detection_status == :unknown
    @test !reverse_evidence.validated_error_enclosure
    @test !reverse_evidence.branch_switch_certified
    @test !reverse_evidence.qualifies_as_dynamic_hysteresis
    @test !reverse_evidence.global_reachability_certified
    @test !reverse_evidence.experimental_causality_claimed
    @test RODT.validate_ro_model_backed_trajectory_evidence(
        reverse_evidence,
        CUBIC_FIELD;
        predecessor_evidence=forward,
    )
end

@testset "agreement gaps, runtime budgets, state bounds, and cancel fail closed" begin
    disagreement_spec = linear_spec(
        primary_relative_tolerance=1.0e-5,
        primary_absolute_tolerance=1.0e-7,
        audit_relative_tolerance=1.0e-11,
        audit_absolute_tolerance=1.0e-13,
        agreement_relative_tolerance=1.0e-14,
        agreement_absolute_tolerance=1.0e-14,
    )
    disagreement = RODT.simulate_ro_model_backed_trajectory(
        disagreement_spec, LINEAR_FIELD)
    @test !disagreement.complete_model_backed_finite_protocol_trajectory
    @test disagreement.status == :unknown_primary_audit_disagreement
    @test disagreement.maximum_scaled_state_discrepancy > 1.0
    @test !all(disagreement.trace.numerical_agreement_validity)
    @test !any(disagreement.trace.residual_validity)
    @test all(==(:unknown), disagreement.trace.dynamics_residual_status)
    @test !disagreement.branch_switch_certified
    @test !disagreement.qualifies_as_dynamic_hysteresis

    rhs_limits = RODT.RODynamicTrajectoryLimits(
        max_rhs_evaluations_per_solve=1,
        max_total_work_units=1_000_000,
    )
    rhs_spec = linear_spec(limits=rhs_limits)
    rhs_error = try
        RODT.simulate_ro_model_backed_trajectory(rhs_spec, LINEAR_FIELD)
        nothing
    catch caught
        caught
    end
    @test rhs_error isa RODT.RODynamicTrajectoryLimitExceeded
    @test rhs_error.phase == :rhs_evaluations_per_solve
    @test rhs_error.requested == 2

    step_limits = RODT.RODynamicTrajectoryLimits(
        max_solver_steps_per_solve=10)
    @test isnothing(RODT._rodt_validate_solver_step_counts(
        6, 4, step_limits))
    step_error = try
        RODT._rodt_validate_solver_step_counts(6, 5, step_limits)
        nothing
    catch caught
        caught
    end
    @test step_error isa RODT.RODynamicTrajectoryLimitExceeded
    @test step_error.phase == :solver_step_attempts_per_solve
    @test step_error.requested == 11

    narrow_spec = linear_spec(
        state_lower_bounds=Float64[-0.01],
        state_upper_bounds=Float64[0.01],
    )
    bounds_error = try
        RODT.simulate_ro_model_backed_trajectory(
            narrow_spec, LINEAR_FIELD)
        nothing
    catch caught
        caught
    end
    @test bounds_error isa RODT.RODynamicTrajectoryStateBoundsExceeded
    @test bounds_error.state_index == 1

    cancellation_checks = Ref(0)
    cancel_check = () -> begin
        cancellation_checks[] += 1
        cancellation_checks[] == 200 && throw(TrajectoryCancelled())
    end
    @test_throws TrajectoryCancelled begin
        RODT.simulate_ro_model_backed_trajectory(
            linear_spec(), LINEAR_FIELD; cancel_check=cancel_check)
    end
    @test cancellation_checks[] == 200

    evidence = RODT.simulate_ro_model_backed_trajectory(
        linear_spec(), LINEAR_FIELD)
    validation_entry_checks = Ref(0)
    @test_throws TrajectoryCancelled begin
        RODT.validate_ro_model_backed_trajectory_evidence(
            evidence,
            LINEAR_FIELD;
            cancel_check=() -> begin
                validation_entry_checks[] += 1
                throw(TrajectoryCancelled())
            end,
        )
    end
    @test validation_entry_checks[] == 1
    validation_checks = Ref(0)
    @test_throws TrajectoryCancelled begin
        RODT.validate_ro_model_backed_trajectory_evidence(
            evidence,
            LINEAR_FIELD;
            cancel_check=() -> begin
                validation_checks[] += 1
                validation_checks[] == 5 && throw(TrajectoryCancelled())
            end,
        )
    end
    @test validation_checks[] == 5
end

end # module RODynamicTrajectoryContractModule
