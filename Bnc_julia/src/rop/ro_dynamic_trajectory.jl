import OrdinaryDiffEq as ODE
import SciMLBase

const RO_DYNAMIC_PROTOCOL_SPEC_VERSION =
    "bne-ro-dynamic-protocol-spec/v2.0.0"
const RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_VERSION =
    "bne-ro-model-backed-trajectory-evidence/v2.0.0"
const RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE =
    :finite_deterministic_polynomial_protocol_numerical_trajectory_only
const _RODT_PRIMARY_SOLVER_ID = :ordinarydiffeq_tsit5
const _RODT_AUDIT_SOLVER_ID = :ordinarydiffeq_vern7
const _RODT_MAX_PROTOCOL_DURATION = 1.0e9
const _RODT_RATE_ROUNDOFF_RELATIVE_TOLERANCE = 1.0e-12

struct _RODTValidatedToken end
const _RODT_VALIDATED = _RODTValidatedToken()

struct RODynamicTrajectoryLimits
    max_protocol_points::Int
    max_state_scalars::Int
    max_rhs_evaluations_per_solve::Int
    max_solver_steps_per_solve::Int
    max_total_work_units::Int
    max_payload_scalars::Int

    function RODynamicTrajectoryLimits(::_RODTValidatedToken, args...)
        return new(args...)
    end
end

function _rodt_positive_limit(value, label::AbstractString, hard_max::Int)
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "$label must be an integer"))
    converted = try
        Int(value)
    catch
        throw(ArgumentError("$label must fit in Int"))
    end
    0 < converted <= hard_max || throw(ArgumentError(
        "$label must lie in 1:$hard_max"))
    return converted
end

function RODynamicTrajectoryLimits(;
    max_protocol_points::Integer=10_000,
    max_state_scalars::Integer=2_000_000,
    max_rhs_evaluations_per_solve::Integer=100_000,
    max_solver_steps_per_solve::Integer=100_000,
    max_total_work_units::Integer=50_000_000,
    max_payload_scalars::Integer=10_000_000,
)
    return RODynamicTrajectoryLimits(
        _RODT_VALIDATED,
        _rodt_positive_limit(
            max_protocol_points, "max_protocol_points", 1_000_000),
        _rodt_positive_limit(
            max_state_scalars, "max_state_scalars", 100_000_000),
        _rodt_positive_limit(
            max_rhs_evaluations_per_solve,
            "max_rhs_evaluations_per_solve",
            10_000_000,
        ),
        _rodt_positive_limit(
            max_solver_steps_per_solve,
            "max_solver_steps_per_solve",
            10_000_000,
        ),
        _rodt_positive_limit(
            max_total_work_units,
            "max_total_work_units",
            2_000_000_000,
        ),
        _rodt_positive_limit(
            max_payload_scalars,
            "max_payload_scalars",
            100_000_000,
        ),
    )
end

struct RODynamicTrajectoryLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, error::RODynamicTrajectoryLimitExceeded)
    print(io, "model-backed dynamic trajectory ", error.phase,
        " requires ", error.requested, ", exceeding limit=", error.limit)
end

struct RODynamicTrajectorySolverFailure <: Exception
    solver_id::Symbol
    retcode::String
end

function Base.showerror(io::IO, error::RODynamicTrajectorySolverFailure)
    print(io, "model-backed dynamic trajectory solver ", error.solver_id,
        " did not succeed: ", error.retcode)
end

struct RODynamicTrajectoryStateBoundsExceeded <: Exception
    solver_id::Symbol
    time::Float64
    state_index::Int
    value::Float64
    lower::Float64
    upper::Float64
end

function Base.showerror(io::IO, error::RODynamicTrajectoryStateBoundsExceeded)
    print(io, "model-backed dynamic trajectory solver ", error.solver_id,
        " left declared state bounds at t=", error.time,
        ", state=", error.state_index, ", value=", error.value,
        ", bounds=[", error.lower, ", ", error.upper, "]")
end

@inline function _rodt_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount <= limit || throw(
        RODynamicTrajectoryLimitExceeded(phase, amount, limit))
    return nothing
end

@inline _rodt_zero_canonical(value::Float64) = iszero(value) ? 0.0 : value

function _rodt_strict_finite_vector(raw, label::AbstractString,
    expected::Int, cancel_check)
    raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
        "$label must be an ordered finite collection"))
    length(raw) == expected || throw(DimensionMismatch(
        "$label must have length $expected"))
    values = Vector{Float64}(undef, expected)
    for index in 1:expected
        _rodh_cancel(cancel_check)
        value = raw[index]
        value isa Float64 || throw(ArgumentError(
            "$label must contain strict Float64 values"))
        isfinite(value) || throw(ArgumentError(
            "$label must contain only finite values"))
        values[index] = _rodt_zero_canonical(value)
    end
    return values
end

function _rodt_limits_payload(limits::RODynamicTrajectoryLimits)
    return (
        max_protocol_points=limits.max_protocol_points,
        max_state_scalars=limits.max_state_scalars,
        max_rhs_evaluations_per_solve=
            limits.max_rhs_evaluations_per_solve,
        max_solver_steps_per_solve=limits.max_solver_steps_per_solve,
        max_total_work_units=limits.max_total_work_units,
        max_payload_scalars=limits.max_payload_scalars,
    )
end

struct RODynamicProtocolSpec
    schema_version::String
    identity_payload::NamedTuple
    identity_sha256::String
    vector_field_sha256::String
    protocol_family_sha256::String
    protocol_id::String
    lineage_predecessor_evidence_sha256::Union{Nothing,String}
    direction::Symbol
    control_grid::Vector{Float64}
    time_grid::Vector{Float64}
    rate::Float64
    rate_unit::String
    time_unit::String
    initial_condition_id::String
    initial_state::Vector{Float64}
    fixed_control_values::Vector{Float64}
    output_state_indices::Vector{Int}
    output_ids::Vector{String}
    output_units::Vector{String}
    state_lower_bounds::Vector{Float64}
    state_upper_bounds::Vector{Float64}
    primary_solver_id::Symbol
    audit_solver_id::Symbol
    primary_relative_tolerance::Float64
    primary_absolute_tolerance::Float64
    audit_relative_tolerance::Float64
    audit_absolute_tolerance::Float64
    maximum_step::Float64
    agreement_relative_tolerance::Float64
    agreement_absolute_tolerance::Float64
    solver_policy_sha256::String
    construction_limits::RODynamicTrajectoryLimits
    evidence_scope::Symbol

    function RODynamicProtocolSpec(::_RODTValidatedToken, args...)
        return new(args...)
    end
end

function ro_dynamic_protocol_spec(
    vector_field::ROPolynomialVectorField;
    protocol_family_sha256,
    protocol_id,
    lineage_predecessor_evidence_sha256=nothing,
    direction,
    control_grid,
    rate,
    rate_unit,
    time_unit,
    initial_condition_id,
    initial_state,
    fixed_control_values=Float64[],
    output_state_indices=collect(1:length(vector_field.state_ids)),
    state_lower_bounds,
    state_upper_bounds,
    primary_relative_tolerance=1.0e-8,
    primary_absolute_tolerance=1.0e-10,
    audit_relative_tolerance=1.0e-10,
    audit_absolute_tolerance=1.0e-12,
    maximum_step=0.1,
    agreement_relative_tolerance=1.0e-6,
    agreement_absolute_tolerance=1.0e-8,
    limits::RODynamicTrajectoryLimits=RODynamicTrajectoryLimits(),
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    validate_ro_polynomial_vector_field(
        vector_field; cancel_check=cancel_check)
    direction isa Symbol && direction in (:increasing, :decreasing) ||
        throw(ArgumentError(
            "direction must be :increasing or :decreasing"))

    time_unit_value = _rodh_nonempty_string(time_unit, "time_unit")
    time_unit_value == vector_field.time_unit || throw(ArgumentError(
        "protocol time_unit must equal the vector-field model time unit"))
    rate_unit_value = _rodh_nonempty_string(rate_unit, "rate_unit")
    rate_unit_identity = _rodh_rate_unit_identity(
        vector_field.control_unit, vector_field.time_unit)
    rate_unit_value == rate_unit_identity.display || throw(ArgumentError(
        "protocol rate_unit must be the vector-field control unit divided " *
        "by its model time unit"))

    control_grid isa AbstractVector || control_grid isa Tuple ||
        throw(ArgumentError(
            "control_grid must be an ordered finite collection"))
    point_count = length(control_grid)
    point_count >= 2 || throw(ArgumentError(
        "a dynamic protocol requires at least two control points"))
    _rodt_limit(:protocol_points, point_count, limits.max_protocol_points)
    controls = _rodt_strict_finite_vector(
        control_grid, "control_grid", point_count, cancel_check)
    if direction == :increasing
        all(controls[index] < controls[index + 1]
            for index in 1:(point_count - 1)) || throw(ArgumentError(
            "an increasing protocol requires a strictly increasing grid"))
    else
        all(controls[index] > controls[index + 1]
            for index in 1:(point_count - 1)) || throw(ArgumentError(
            "a decreasing protocol requires a strictly decreasing grid"))
    end

    rate_value = _rodh_strict_positive_float(rate, "protocol rate")
    time_values = Vector{Float64}(undef, point_count)
    time_values[1] = 0.0
    for index in 2:point_count
        _rodh_cancel(cancel_check)
        elapsed = abs(controls[index] - controls[1]) / rate_value
        isfinite(elapsed) && elapsed > time_values[index - 1] ||
            throw(ArgumentError(
                "protocol time grid must remain finite and increasing"))
        elapsed <= _RODT_MAX_PROTOCOL_DURATION || throw(ArgumentError(
            "protocol duration exceeds the supported finite bound"))
        time_values[index] = elapsed
    end

    state_count = length(vector_field.state_ids)
    initial = _rodt_strict_finite_vector(
        initial_state, "initial_state", state_count, cancel_check)
    lower = _rodt_strict_finite_vector(
        state_lower_bounds, "state_lower_bounds", state_count, cancel_check)
    upper = _rodt_strict_finite_vector(
        state_upper_bounds, "state_upper_bounds", state_count, cancel_check)
    all(lower[index] < upper[index] for index in 1:state_count) ||
        throw(ArgumentError(
            "every state lower bound must be below its upper bound"))
    all(lower[index] <= initial[index] <= upper[index]
        for index in 1:state_count) || throw(ArgumentError(
        "initial_state must lie within the declared state bounds"))
    fixed = _rodt_strict_finite_vector(
        fixed_control_values,
        "fixed_control_values",
        length(vector_field.fixed_control_ids),
        cancel_check,
    )

    output_state_indices isa AbstractVector ||
        output_state_indices isa Tuple || throw(ArgumentError(
        "output_state_indices must be an ordered finite collection"))
    isempty(output_state_indices) && throw(ArgumentError(
        "output_state_indices must not be empty"))
    indices = Int[]
    seen_indices = Set{Int}()
    for raw_index in output_state_indices
        _rodh_cancel(cancel_check)
        raw_index isa Integer && !(raw_index isa Bool) ||
            throw(ArgumentError(
                "output_state_indices must contain integers"))
        index = try
            Int(raw_index)
        catch
            throw(ArgumentError(
                "output_state_indices values must fit in Int"))
        end
        1 <= index <= state_count || throw(ArgumentError(
            "output_state_indices must index the ordered states"))
        index in seen_indices && throw(ArgumentError(
            "output_state_indices must not contain duplicates"))
        push!(seen_indices, index)
        push!(indices, index)
    end
    output_ids = String[vector_field.state_ids[index] for index in indices]
    output_units = String[vector_field.state_units[index] for index in indices]

    primary_rtol = _rodh_strict_positive_float(
        primary_relative_tolerance, "primary_relative_tolerance")
    primary_atol = _rodh_strict_positive_float(
        primary_absolute_tolerance, "primary_absolute_tolerance")
    audit_rtol = _rodh_strict_positive_float(
        audit_relative_tolerance, "audit_relative_tolerance")
    audit_atol = _rodh_strict_positive_float(
        audit_absolute_tolerance, "audit_absolute_tolerance")
    audit_rtol < primary_rtol && audit_atol < primary_atol ||
        throw(ArgumentError(
            "audit tolerances must be strictly tighter than primary tolerances"))
    maximum_step_value = _rodh_strict_positive_float(
        maximum_step, "maximum_step")
    maximum_step_value <= last(time_values) || throw(ArgumentError(
        "maximum_step must not exceed the protocol duration"))
    agreement_rtol = _rodh_strict_positive_float(
        agreement_relative_tolerance,
        "agreement_relative_tolerance",
    )
    agreement_atol = _rodh_strict_positive_float(
        agreement_absolute_tolerance,
        "agreement_absolute_tolerance",
    )

    state_scalar_work = BigInt(point_count) * state_count
    _rodt_limit(
        :state_scalars, state_scalar_work, limits.max_state_scalars)
    payload_scalars = BigInt(2) * state_scalar_work +
        BigInt(2) * point_count + BigInt(4) * state_count +
        length(fixed) + length(indices)
    _rodt_limit(
        :payload_scalars, payload_scalars, limits.max_payload_scalars)
    total_work = BigInt(2) * limits.max_rhs_evaluations_per_solve *
        vector_field.evaluation_work_units + BigInt(4) * state_scalar_work
    _rodt_limit(
        :trajectory_work_units, total_work, limits.max_total_work_units)

    solver_payload = (
        primary_solver=String(_RODT_PRIMARY_SOLVER_ID),
        audit_solver=String(_RODT_AUDIT_SOLVER_ID),
        primary_relative_tolerance=primary_rtol,
        primary_absolute_tolerance=primary_atol,
        audit_relative_tolerance=audit_rtol,
        audit_absolute_tolerance=audit_atol,
        maximum_step=maximum_step_value,
        agreement_relative_tolerance=agreement_rtol,
        agreement_absolute_tolerance=agreement_atol,
        numerical_agreement_semantics=
            "primary_audit_scaled_state_discrepancy_on_declared_save_grid",
    )
    solver_policy_hash = _rodh_payload_sha256(solver_payload)
    payload = (
        schema_version=RO_DYNAMIC_PROTOCOL_SPEC_VERSION,
        vector_field_sha256=vector_field.identity_sha256,
        protocol=(
            family_sha256=_rodh_sha256_id(
                protocol_family_sha256, "protocol_family_sha256"),
            id=_rodh_nonempty_string(protocol_id, "protocol_id"),
            lineage_predecessor_evidence_sha256=
                _rodh_optional_sha256_id(
                    lineage_predecessor_evidence_sha256,
                    "lineage_predecessor_evidence_sha256",
                ),
            direction=String(direction),
            control_grid=Tuple(controls),
            time_grid=Tuple(time_values),
            rate=rate_value,
            rate_unit=rate_unit_value,
            rate_unit_identity=rate_unit_identity,
            time_unit=time_unit_value,
        ),
        initial_condition=(
            id=_rodh_nonempty_string(
                initial_condition_id, "initial_condition_id"),
            state=Tuple(initial),
        ),
        fixed_control_values=Tuple(fixed),
        output_state_indices=Tuple(indices),
        output_ids=Tuple(output_ids),
        output_units=Tuple(output_units),
        state_bounds=(lower=Tuple(lower), upper=Tuple(upper)),
        solver=(policy_sha256=solver_policy_hash, config=solver_payload),
        construction_limits=_rodt_limits_payload(limits),
        evidence_scope=String(RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE),
    )
    identity_hash = _rodh_payload_sha256(payload)
    _rodh_cancel(cancel_check)
    return RODynamicProtocolSpec(
        _RODT_VALIDATED,
        RO_DYNAMIC_PROTOCOL_SPEC_VERSION,
        payload,
        identity_hash,
        vector_field.identity_sha256,
        payload.protocol.family_sha256,
        payload.protocol.id,
        payload.protocol.lineage_predecessor_evidence_sha256,
        direction,
        copy(controls),
        copy(time_values),
        rate_value,
        payload.protocol.rate_unit,
        payload.protocol.time_unit,
        payload.initial_condition.id,
        copy(initial),
        copy(fixed),
        copy(indices),
        copy(output_ids),
        copy(output_units),
        copy(lower),
        copy(upper),
        _RODT_PRIMARY_SOLVER_ID,
        _RODT_AUDIT_SOLVER_ID,
        primary_rtol,
        primary_atol,
        audit_rtol,
        audit_atol,
        maximum_step_value,
        agreement_rtol,
        agreement_atol,
        solver_policy_hash,
        limits,
        RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE,
    )
end

function validate_ro_dynamic_protocol_spec(
    spec::RODynamicProtocolSpec,
    vector_field::ROPolynomialVectorField;
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    spec.schema_version == RO_DYNAMIC_PROTOCOL_SPEC_VERSION ||
        throw(ArgumentError("unsupported dynamic protocol spec version"))
    spec.evidence_scope == RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE ||
        throw(ArgumentError("dynamic protocol evidence scope mismatch"))
    recomputed = ro_dynamic_protocol_spec(
        vector_field;
        protocol_family_sha256=spec.protocol_family_sha256,
        protocol_id=spec.protocol_id,
        lineage_predecessor_evidence_sha256=
            spec.lineage_predecessor_evidence_sha256,
        direction=spec.direction,
        control_grid=spec.control_grid,
        rate=spec.rate,
        rate_unit=spec.rate_unit,
        time_unit=spec.time_unit,
        initial_condition_id=spec.initial_condition_id,
        initial_state=spec.initial_state,
        fixed_control_values=spec.fixed_control_values,
        output_state_indices=spec.output_state_indices,
        state_lower_bounds=spec.state_lower_bounds,
        state_upper_bounds=spec.state_upper_bounds,
        primary_relative_tolerance=spec.primary_relative_tolerance,
        primary_absolute_tolerance=spec.primary_absolute_tolerance,
        audit_relative_tolerance=spec.audit_relative_tolerance,
        audit_absolute_tolerance=spec.audit_absolute_tolerance,
        maximum_step=spec.maximum_step,
        agreement_relative_tolerance=spec.agreement_relative_tolerance,
        agreement_absolute_tolerance=spec.agreement_absolute_tolerance,
        limits=spec.construction_limits,
        cancel_check=cancel_check,
    )
    for field in fieldnames(RODynamicProtocolSpec)
        getfield(spec, field) == getfield(recomputed, field) ||
            throw(ArgumentError(
                "dynamic protocol spec does not reproduce at field $field"))
    end
    return true
end

struct _RODTSolverRun
    solver_id::Symbol
    retcode::String
    states::Matrix{Float64}
    rhs_evaluations::Int
    accepted_steps::Int
    rejected_steps::Int
end

function _rodt_validate_solver_step_counts(
    accepted_steps::Int,
    rejected_steps::Int,
    limits::RODynamicTrajectoryLimits,
)
    accepted_steps >= 0 && rejected_steps >= 0 || throw(ArgumentError(
        "trajectory solver step counts must be non-negative"))
    _rodt_limit(
        :solver_step_attempts_per_solve,
        BigInt(accepted_steps) + BigInt(rejected_steps),
        limits.max_solver_steps_per_solve,
    )
    return nothing
end

function _rodt_solver_algorithm(solver_id::Symbol)
    solver_id == _RODT_PRIMARY_SOLVER_ID && return ODE.Tsit5()
    solver_id == _RODT_AUDIT_SOLVER_ID && return ODE.Vern7()
    throw(ArgumentError("unsupported internal trajectory solver"))
end

function _rodt_check_state_bounds!(state, time::Float64,
    spec::RODynamicProtocolSpec, solver_id::Symbol)
    for state_index in eachindex(state)
        value = Float64(state[state_index])
        isfinite(value) || throw(RODynamicTrajectoryStateBoundsExceeded(
            solver_id, time, state_index, value,
            spec.state_lower_bounds[state_index],
            spec.state_upper_bounds[state_index],
        ))
        lower = spec.state_lower_bounds[state_index]
        upper = spec.state_upper_bounds[state_index]
        lower <= value <= upper ||
            throw(RODynamicTrajectoryStateBoundsExceeded(
                solver_id, time, state_index, value, lower, upper))
    end
    return nothing
end

function _rodt_run_solver(
    spec::RODynamicProtocolSpec,
    vector_field::ROPolynomialVectorField,
    solver_id::Symbol,
    relative_tolerance::Float64,
    absolute_tolerance::Float64,
    cancel_check,
)
    limits = spec.construction_limits
    rhs_evaluations = Ref(0)
    direction_sign = spec.direction == :increasing ? 1.0 : -1.0
    start_control = first(spec.control_grid)
    field_limits = RODynamicEvidenceLimits(
        max_vector_field_work_units=max(
            vector_field.evaluation_work_units, 1),
    )
    function rhs!(derivative, state, _parameters, time)
        _rodh_cancel(cancel_check)
        rhs_evaluations[] += 1
        _rodt_limit(
            :rhs_evaluations_per_solve,
            rhs_evaluations[],
            limits.max_rhs_evaluations_per_solve,
        )
        time_value = Float64(time)
        _rodt_check_state_bounds!(state, time_value, spec, solver_id)
        control = start_control + direction_sign * spec.rate * time_value
        isfinite(control) || throw(ArgumentError(
            "internal linear protocol produced a non-finite control"))
        values, _ = _rodh_evaluate_polynomial_vector_field(
            vector_field,
            state,
            control,
            spec.fixed_control_values,
            field_limits,
            cancel_check,
        )
        for state_index in eachindex(values)
            _rodh_cancel(cancel_check)
            derivative[state_index] = values[state_index]
        end
        return nothing
    end

    _rodh_cancel(cancel_check)
    problem = ODE.ODEProblem(
        rhs!, copy(spec.initial_state), (0.0, last(spec.time_grid)))
    solution = ODE.solve(
        problem,
        _rodt_solver_algorithm(solver_id);
        reltol=relative_tolerance,
        abstol=absolute_tolerance,
        dtmax=spec.maximum_step,
        maxiters=limits.max_solver_steps_per_solve,
        saveat=spec.time_grid,
        tstops=spec.time_grid[2:end],
        save_everystep=false,
        dense=false,
    )
    _rodh_cancel(cancel_check)
    SciMLBase.successful_retcode(solution) || throw(
        RODynamicTrajectorySolverFailure(
            solver_id, string(solution.retcode)))
    length(solution.t) == length(spec.time_grid) &&
        all(solution.t[index] == spec.time_grid[index]
            for index in eachindex(spec.time_grid)) || throw(
        RODynamicTrajectorySolverFailure(
            solver_id, "incomplete_or_misaligned_save_grid"))
    accepted_steps = Int(solution.stats.naccept)
    rejected_steps = Int(solution.stats.nreject)
    _rodt_limit(
        :accepted_steps_per_solve,
        accepted_steps,
        limits.max_solver_steps_per_solve,
    )
    _rodt_limit(
        :rejected_steps_per_solve,
        rejected_steps,
        limits.max_solver_steps_per_solve,
    )
    _rodt_validate_solver_step_counts(
        accepted_steps, rejected_steps, limits)

    state_count = length(spec.initial_state)
    states = Matrix{Float64}(
        undef, length(spec.time_grid), state_count)
    for point in eachindex(solution.u)
        _rodh_cancel(cancel_check)
        state = solution.u[point]
        length(state) == state_count || throw(DimensionMismatch(
            "trajectory solver returned an unexpected state dimension"))
        _rodt_check_state_bounds!(
            state, spec.time_grid[point], spec, solver_id)
        for state_index in 1:state_count
            states[point, state_index] = Float64(state[state_index])
        end
    end
    return _RODTSolverRun(
        solver_id,
        string(solution.retcode),
        states,
        rhs_evaluations[],
        accepted_steps,
        rejected_steps,
    )
end

struct ROModelBackedTrajectoryEvidence
    schema_version::String
    identity_payload::NamedTuple
    identity_sha256::String
    spec::RODynamicProtocolSpec
    spec_sha256::String
    vector_field_sha256::String
    predecessor_evidence_sha256::Union{Nothing,String}
    trace::RODynamicProtocolTrace
    trace_sha256::String
    primary_solver_id::Symbol
    audit_solver_id::Symbol
    primary_retcode::String
    audit_retcode::String
    primary_rhs_evaluations::Int
    audit_rhs_evaluations::Int
    primary_accepted_steps::Int
    audit_accepted_steps::Int
    primary_rejected_steps::Int
    audit_rejected_steps::Int
    maximum_absolute_state_discrepancy::Float64
    maximum_scaled_state_discrepancy::Float64
    complete_save_grid::Bool
    all_states_finite::Bool
    state_bounds_respected::Bool
    complete_model_backed_finite_protocol_trajectory::Bool
    status::Symbol
    numerical_agreement_semantics::Symbol
    validated_error_enclosure::Bool
    branch_switch_certified::Bool
    qualifies_as_dynamic_hysteresis::Bool
    global_reachability_certified::Bool
    basin_completeness_certified::Bool
    experimental_causality_claimed::Bool
    evidence_scope::Symbol

    function ROModelBackedTrajectoryEvidence(
        ::_RODTValidatedToken,
        args...,
    )
        return new(args...)
    end
end

function _rodt_evidence_payload_from_fields(
    evidence::ROModelBackedTrajectoryEvidence,
)
    return (
        schema_version=evidence.schema_version,
        spec_sha256=evidence.spec_sha256,
        vector_field_sha256=evidence.vector_field_sha256,
        predecessor_evidence_sha256=
            evidence.predecessor_evidence_sha256,
        trace_sha256=evidence.trace_sha256,
        solver=(
            policy_sha256=evidence.spec.solver_policy_sha256,
            primary=(
                id=String(evidence.primary_solver_id),
                retcode=evidence.primary_retcode,
                rhs_evaluations=evidence.primary_rhs_evaluations,
                accepted_steps=evidence.primary_accepted_steps,
                rejected_steps=evidence.primary_rejected_steps,
            ),
            audit=(
                id=String(evidence.audit_solver_id),
                retcode=evidence.audit_retcode,
                rhs_evaluations=evidence.audit_rhs_evaluations,
                accepted_steps=evidence.audit_accepted_steps,
                rejected_steps=evidence.audit_rejected_steps,
            ),
        ),
        comparison=(
            numerical_agreement_semantics=
                String(evidence.numerical_agreement_semantics),
            maximum_absolute_state_discrepancy=
                evidence.maximum_absolute_state_discrepancy,
            maximum_scaled_state_discrepancy=
                evidence.maximum_scaled_state_discrepancy,
        ),
        result=(
            status=String(evidence.status),
            complete_save_grid=evidence.complete_save_grid,
            all_states_finite=evidence.all_states_finite,
            state_bounds_respected=evidence.state_bounds_respected,
            complete_model_backed_finite_protocol_trajectory=
                evidence.complete_model_backed_finite_protocol_trajectory,
            validated_error_enclosure=evidence.validated_error_enclosure,
            branch_switch_certified=evidence.branch_switch_certified,
            qualifies_as_dynamic_hysteresis=
                evidence.qualifies_as_dynamic_hysteresis,
            global_reachability_certified=
                evidence.global_reachability_certified,
            basin_completeness_certified=
                evidence.basin_completeness_certified,
            experimental_causality_claimed=
                evidence.experimental_causality_claimed,
            evidence_scope=String(evidence.evidence_scope),
        ),
    )
end

function _rodt_predecessor_integrity(
    predecessor::ROModelBackedTrajectoryEvidence,
    vector_field::ROPolynomialVectorField,
    cancel_check,
)
    _rodh_cancel(cancel_check)
    predecessor.spec.direction == :increasing || throw(ArgumentError(
        "v2 predecessor evidence must be an increasing protocol"))
    predecessor.spec.lineage_predecessor_evidence_sha256 === nothing &&
        predecessor.predecessor_evidence_sha256 === nothing ||
        throw(ArgumentError(
            "v2 predecessor evidence must be an unlinked root trajectory"))
    # This is deliberately a complete deterministic replay, not a structural
    # self-hash check.  The unlinked-root precondition above makes the replay
    # non-recursive while rejecting self-consistent forged predecessor fields.
    validate_ro_model_backed_trajectory_evidence(
        predecessor,
        vector_field;
        predecessor_evidence=nothing,
        cancel_check=cancel_check,
    )
    return nothing
end

function _rodt_bind_predecessor(
    spec::RODynamicProtocolSpec,
    predecessor::Union{Nothing,ROModelBackedTrajectoryEvidence},
    vector_field::ROPolynomialVectorField,
    cancel_check,
)
    if spec.lineage_predecessor_evidence_sha256 === nothing
        predecessor === nothing || throw(ArgumentError(
            "an unlinked protocol must not receive predecessor evidence"))
        return nothing
    end
    predecessor === nothing && throw(ArgumentError(
        "a linked protocol requires predecessor evidence"))
    _rodt_predecessor_integrity(
        predecessor, vector_field, cancel_check)
    predecessor.identity_sha256 ==
        spec.lineage_predecessor_evidence_sha256 || throw(ArgumentError(
        "predecessor evidence hash does not match protocol lineage"))
    predecessor.vector_field_sha256 == spec.vector_field_sha256 ||
        throw(ArgumentError(
            "predecessor and child must use the same vector field"))
    predecessor.spec.protocol_family_sha256 ==
        spec.protocol_family_sha256 || throw(ArgumentError(
        "predecessor and child must use the same protocol family"))
    predecessor.spec.direction == :increasing &&
        spec.direction == :decreasing || throw(ArgumentError(
        "v2 lineage permits only one increasing-to-decreasing transition"))
    reverse(predecessor.spec.control_grid) == spec.control_grid ||
        throw(ArgumentError(
            "a forward-to-reverse child requires the exact reverse grid"))
    predecessor.spec.rate == spec.rate || throw(ArgumentError(
        "a forward-to-reverse child requires the same finite rate"))
    predecessor.spec.rate_unit == spec.rate_unit || throw(ArgumentError(
        "predecessor and child rate_unit must match"))
    predecessor.spec.time_unit == spec.time_unit || throw(ArgumentError(
        "predecessor and child time_unit must match"))
    predecessor.spec.solver_policy_sha256 == spec.solver_policy_sha256 ||
        throw(ArgumentError(
            "predecessor and child trajectory solver policies must match"))
    predecessor.spec.fixed_control_values == spec.fixed_control_values ||
        throw(ArgumentError(
            "predecessor and child fixed controls must match"))
    predecessor.spec.output_state_indices == spec.output_state_indices ||
        throw(ArgumentError(
            "predecessor and child output projections must match"))
    predecessor.spec.state_lower_bounds == spec.state_lower_bounds &&
        predecessor.spec.state_upper_bounds == spec.state_upper_bounds ||
        throw(ArgumentError(
            "predecessor and child state bounds must match"))
    final_state = vec(copy(@view predecessor.trace.states[end, :]))
    final_state == spec.initial_state || throw(ArgumentError(
        "child initial_state must exactly equal predecessor final state"))
    return nothing
end

function _rodt_numerical_agreement_policy_sha256(
    spec::RODynamicProtocolSpec,
)
    return _rodh_payload_sha256((
        trajectory_solver_policy_sha256=spec.solver_policy_sha256,
        comparison_semantics=
            "primary_audit_scaled_state_discrepancy_on_declared_save_grid",
        pointwise_threshold=1.0,
    ))
end

function simulate_ro_model_backed_trajectory(
    spec::RODynamicProtocolSpec,
    vector_field::ROPolynomialVectorField;
    predecessor_evidence::Union{
        Nothing,ROModelBackedTrajectoryEvidence}=nothing,
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    validate_ro_dynamic_protocol_spec(
        spec, vector_field; cancel_check=cancel_check)
    spec.vector_field_sha256 == vector_field.identity_sha256 ||
        throw(ArgumentError(
            "protocol spec does not bind the supplied vector field"))
    _rodt_bind_predecessor(
        spec, predecessor_evidence, vector_field, cancel_check)

    primary = _rodt_run_solver(
        spec,
        vector_field,
        spec.primary_solver_id,
        spec.primary_relative_tolerance,
        spec.primary_absolute_tolerance,
        cancel_check,
    )
    _rodh_cancel(cancel_check)
    audit = _rodt_run_solver(
        spec,
        vector_field,
        spec.audit_solver_id,
        spec.audit_relative_tolerance,
        spec.audit_absolute_tolerance,
        cancel_check,
    )

    point_count, state_count = size(primary.states)
    size(audit.states) == (point_count, state_count) ||
        throw(DimensionMismatch(
            "primary and audit trajectory shapes must match"))
    scaled_discrepancies = zeros(Float64, point_count)
    maximum_absolute = 0.0
    maximum_scaled = 0.0
    for point in 1:point_count
        _rodh_cancel(cancel_check)
        point_scaled = 0.0
        for state_index in 1:state_count
            primary_value = primary.states[point, state_index]
            audit_value = audit.states[point, state_index]
            absolute = abs(primary_value - audit_value)
            scale = spec.agreement_absolute_tolerance +
                spec.agreement_relative_tolerance *
                    max(abs(primary_value), abs(audit_value))
            scaled = absolute / scale
            isfinite(absolute) && isfinite(scaled) || throw(ArgumentError(
                "primary/audit trajectory comparison is non-finite"))
            maximum_absolute = max(maximum_absolute, absolute)
            point_scaled = max(point_scaled, scaled)
        end
        scaled_discrepancies[point] = point_scaled
        maximum_scaled = max(maximum_scaled, point_scaled)
    end
    agreement = maximum_scaled <= 1.0
    agreement_status = Symbol[
        value <= 1.0 ? :verified : :failed
        for value in scaled_discrepancies
    ]
    output_count = length(spec.output_state_indices)
    outputs = Matrix{Float64}(undef, point_count, output_count)
    for point in 1:point_count, output in 1:output_count
        _rodh_cancel(cancel_check)
        outputs[point, output] = primary.states[
            point, spec.output_state_indices[output]]
    end
    predecessor_trace_hash = predecessor_evidence === nothing ? nothing :
        predecessor_evidence.trace_sha256
    trace = RODynamicProtocolTrace(
        model_identity=vector_field.identity_sha256,
        coordinate_chart_id=vector_field.coordinate_chart_id,
        dynamics_policy_sha256=vector_field.dynamics_policy_sha256,
        residual_policy_sha256=vector_field.residual_policy_sha256,
        protocol_family_sha256=spec.protocol_family_sha256,
        protocol_id=spec.protocol_id,
        lineage_predecessor_trace_sha256=predecessor_trace_hash,
        control_id=vector_field.control_id,
        control_unit=vector_field.control_unit,
        fixed_control_ids=collect(vector_field.fixed_control_ids),
        fixed_control_units=collect(vector_field.fixed_control_units),
        fixed_control_values=spec.fixed_control_values,
        direction=spec.direction,
        rate=spec.rate,
        rate_unit=spec.rate_unit,
        rate_relative_tolerance=
            _RODT_RATE_ROUNDOFF_RELATIVE_TOLERANCE,
        rate_absolute_tolerance=0.0,
        rate_absolute_tolerance_unit=spec.rate_unit,
        time_unit=spec.time_unit,
        initial_condition_id=spec.initial_condition_id,
        initial_state=spec.initial_state,
        state_ids=collect(vector_field.state_ids),
        state_units=collect(vector_field.state_units),
        output_ids=spec.output_ids,
        output_units=spec.output_units,
        trajectory_solver_method="OrdinaryDiffEq.Tsit5+Vern7-audited",
        model_solver_policy_sha256=vector_field.solver_policy_sha256,
        trajectory_solver_policy_sha256=spec.solver_policy_sha256,
        trajectory_solver_relative_tolerance=
            spec.primary_relative_tolerance,
        trajectory_solver_absolute_tolerance=
            spec.primary_absolute_tolerance,
        stability_method="not_computed_for_dynamic_trajectory",
        stability_policy_sha256=vector_field.stability_policy_sha256,
        branch_policy_sha256=vector_field.branch_policy_sha256,
        branch_structure_status=:unknown,
        event_method="not_computed_for_dynamic_trajectory",
        event_policy_sha256=vector_field.event_policy_sha256,
        event_detection_status=:unknown,
        dynamics_residual_tolerance=1.0,
        numerical_agreement_policy_sha256=
            _rodt_numerical_agreement_policy_sha256(spec),
        numerical_agreement_tolerance=1.0,
        time=spec.time_grid,
        control=spec.control_grid,
        states=primary.states,
        outputs=outputs,
        solver_status=fill(:success, point_count),
        local_stability=fill(:unknown, point_count),
        branch_ids=Union{Nothing,String}[nothing for _ in 1:point_count],
        dynamics_residual_norm=fill(NaN, point_count),
        dynamics_residual_status=fill(:unknown, point_count),
        numerical_agreement_norm=scaled_discrepancies,
        numerical_agreement_status=agreement_status,
        events=RODynamicSwitchEvent[],
        limits=RODynamicEvidenceLimits(
            max_points_per_trace=max(point_count, 2),
            max_payload_scalars=max(
                spec.construction_limits.max_payload_scalars, 1),
        ),
        cancel_check=cancel_check,
    )
    _rodh_bind_trace_to_vector_field(trace, vector_field)
    trace_hash = ro_dynamic_protocol_identity_sha256(
        trace; cancel_check=cancel_check)
    complete_save_grid = all(trace.time .== spec.time_grid) &&
        all(trace.control .== spec.control_grid)
    all_states_finite = all(isfinite, primary.states) &&
        all(isfinite, audit.states)
    state_bounds_respected = true
    complete = agreement && complete_save_grid && all_states_finite &&
        all(trace.numeric_validity) &&
        all(trace.numerical_agreement_validity)
    status = complete ?
        :complete_model_backed_finite_protocol_trajectory :
        :unknown_primary_audit_disagreement
    payload = (
        schema_version=RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_VERSION,
        spec_sha256=spec.identity_sha256,
        vector_field_sha256=vector_field.identity_sha256,
        predecessor_evidence_sha256=
            spec.lineage_predecessor_evidence_sha256,
        trace_sha256=trace_hash,
        solver=(
            policy_sha256=spec.solver_policy_sha256,
            primary=(
                id=String(primary.solver_id),
                retcode=primary.retcode,
                rhs_evaluations=primary.rhs_evaluations,
                accepted_steps=primary.accepted_steps,
                rejected_steps=primary.rejected_steps,
            ),
            audit=(
                id=String(audit.solver_id),
                retcode=audit.retcode,
                rhs_evaluations=audit.rhs_evaluations,
                accepted_steps=audit.accepted_steps,
                rejected_steps=audit.rejected_steps,
            ),
        ),
        comparison=(
            numerical_agreement_semantics=
                "primary_audit_scaled_state_discrepancy_on_declared_save_grid",
            maximum_absolute_state_discrepancy=maximum_absolute,
            maximum_scaled_state_discrepancy=maximum_scaled,
        ),
        result=(
            status=String(status),
            complete_save_grid=complete_save_grid,
            all_states_finite=all_states_finite,
            state_bounds_respected=state_bounds_respected,
            complete_model_backed_finite_protocol_trajectory=complete,
            validated_error_enclosure=false,
            branch_switch_certified=false,
            qualifies_as_dynamic_hysteresis=false,
            global_reachability_certified=false,
            basin_completeness_certified=false,
            experimental_causality_claimed=false,
            evidence_scope=String(
                RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE),
        ),
    )
    identity_hash = _rodh_payload_sha256(payload)
    _rodh_cancel(cancel_check)
    return ROModelBackedTrajectoryEvidence(
        _RODT_VALIDATED,
        RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_VERSION,
        payload,
        identity_hash,
        spec,
        spec.identity_sha256,
        vector_field.identity_sha256,
        spec.lineage_predecessor_evidence_sha256,
        trace,
        trace_hash,
        primary.solver_id,
        audit.solver_id,
        primary.retcode,
        audit.retcode,
        primary.rhs_evaluations,
        audit.rhs_evaluations,
        primary.accepted_steps,
        audit.accepted_steps,
        primary.rejected_steps,
        audit.rejected_steps,
        maximum_absolute,
        maximum_scaled,
        complete_save_grid,
        all_states_finite,
        state_bounds_respected,
        complete,
        status,
        :primary_audit_scaled_state_discrepancy_on_declared_save_grid,
        false,
        false,
        false,
        false,
        false,
        false,
        RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE,
    )
end

function _rodt_reproduction_equal(left, right)
    typeof(left) === typeof(right) || return false
    if left isa AbstractArray
        axes(left) == axes(right) || return false
        return all(_rodt_reproduction_equal(
            left[index], right[index]) for index in eachindex(left))
    elseif left isa NamedTuple
        keys(left) == keys(right) || return false
        return all(_rodt_reproduction_equal(
            getfield(left, key), getfield(right, key)) for key in keys(left))
    elseif left isa Tuple
        length(left) == length(right) || return false
        return all(_rodt_reproduction_equal(
            left[index], right[index]) for index in eachindex(left))
    elseif left isa Union{
        RODynamicProtocolSpec,
        RODynamicProtocolTrace,
        RODynamicTrajectoryLimits,
        RODynamicSwitchEvent,
    }
        return all(_rodt_reproduction_equal(
            getfield(left, field), getfield(right, field))
            for field in fieldnames(typeof(left)))
    end
    return isequal(left, right)
end

function validate_ro_model_backed_trajectory_evidence(
    evidence::ROModelBackedTrajectoryEvidence,
    vector_field::ROPolynomialVectorField;
    predecessor_evidence::Union{
        Nothing,ROModelBackedTrajectoryEvidence}=nothing,
    cancel_check=nothing,
)
    _rodh_cancel(cancel_check)
    evidence.schema_version ==
        RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_VERSION ||
        throw(ArgumentError(
            "unsupported model-backed trajectory evidence version"))
    evidence.evidence_scope ==
        RO_MODEL_BACKED_TRAJECTORY_EVIDENCE_SCOPE || throw(ArgumentError(
        "model-backed trajectory evidence scope mismatch"))
    _rodt_evidence_payload_from_fields(evidence) ==
        evidence.identity_payload || throw(ArgumentError(
        "model-backed trajectory fields do not match its content payload"))
    validate_ro_dynamic_protocol_spec(
        evidence.spec, vector_field; cancel_check=cancel_check)
    evidence.validated_error_enclosure && throw(ArgumentError(
        "trajectory evidence overclaims a validated error enclosure"))
    evidence.branch_switch_certified && throw(ArgumentError(
        "trajectory evidence overclaims branch switching"))
    evidence.qualifies_as_dynamic_hysteresis && throw(ArgumentError(
        "trajectory evidence overclaims dynamic hysteresis"))
    evidence.global_reachability_certified && throw(ArgumentError(
        "trajectory evidence overclaims global reachability"))
    evidence.basin_completeness_certified && throw(ArgumentError(
        "trajectory evidence overclaims basin completeness"))
    evidence.experimental_causality_claimed && throw(ArgumentError(
        "trajectory evidence overclaims experimental causality"))
    recomputed = simulate_ro_model_backed_trajectory(
        evidence.spec,
        vector_field;
        predecessor_evidence=predecessor_evidence,
        cancel_check=cancel_check,
    )
    for field in fieldnames(ROModelBackedTrajectoryEvidence)
        _rodt_reproduction_equal(
            getfield(evidence, field), getfield(recomputed, field)) ||
            throw(ArgumentError(
                "model-backed trajectory evidence does not reproduce at field $field"))
    end
    return true
end
