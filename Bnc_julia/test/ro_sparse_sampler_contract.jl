using Test
using LinearAlgebra
using BindingAndCatalysis
using JSON3

const _ROSS_MODULE = BindingAndCatalysis
if !isdefined(_ROSS_MODULE, :ROSparseSamplingPlan)
    Base.include(_ROSS_MODULE,
        joinpath(@__DIR__, "..", "src", "rop", "ro_sparse_sampler.jl"))
end

struct ROSparseCancelProbe <: Exception end
struct ROSparseOversizedPortableToken <: AbstractString end

Base.ncodeunits(::ROSparseOversizedPortableToken) =
    _ROSS_MODULE._ROSS_V2_MAX_PORTABLE_TOKEN_BYTES + 1

function _ross_plan_1d(; outputs=["f"], tolerance=1e-10)
    return _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="identity-log-control",
        control_ids=["u"],
        source_coordinate_ids=["theta"],
        chart_jacobian=ones(1, 1),
        domain_lower=[-1.0],
        domain_upper=[1.0],
        output_ids=outputs,
        fixed_background=[0.0],
        surplus_tolerance=tolerance,
    )
end

function _ross_plan_2d(; tolerance=1e-10)
    return _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="identity-two-control",
        control_ids=["u", "v"],
        source_coordinate_ids=["theta_u", "theta_v"],
        chart_jacobian=Matrix{Float64}(I, 2, 2),
        domain_lower=[-1.0, -1.0],
        domain_upper=[1.0, 1.0],
        output_ids=["f"],
        fixed_background=zeros(2),
        surplus_tolerance=tolerance,
    )
end

function _ross_plan_v2_1d(;
    outputs=["y"],
    tolerance=1e-10,
    limits=_ROSS_MODULE.ROSparseSamplingLimits(),
)
    return _ROSS_MODULE.ROSparseROChannelPlanV2(
        chart_id="one-input-ro-chart",
        control_ids=["u"],
        source_coordinate_ids=["theta"],
        chart_jacobian=ones(1, 1),
        domain_lower=[-1.0],
        domain_upper=[1.0],
        output_ids=outputs,
        fixed_background=[0.0],
        surplus_tolerance=tolerance,
        limits=limits,
    )
end

function _ross_plan_v2_2d(;
    tolerance=1e-10,
    limits=_ROSS_MODULE.ROSparseSamplingLimits(),
)
    return _ROSS_MODULE.ROSparseROChannelPlanV2(
        chart_id="two-input-ro-chart",
        control_ids=["u", "v"],
        source_coordinate_ids=["theta_u", "theta_v"],
        chart_jacobian=Matrix{Float64}(I, 2, 2),
        domain_lower=[-1.0, -1.0],
        domain_upper=[1.0, 1.0],
        output_ids=["y1", "y2"],
        fixed_background=[0.0, 0.0],
        surplus_tolerance=tolerance,
        limits=limits,
    )
end

function _ross_rehashed_result_status(result, status)
    function rebuild(hash)
        return _ROSS_MODULE.ROSparseSamplingResultV2(
            _ROSS_MODULE._ROSS_V2_TOKEN,
            result.schema_version,
            result.plan_sha256,
            result.sampling_spec_sha256,
            result.terminal_state_sha256,
            status,
            result.stopping_reason,
            result.evidence_scope,
            result.accepted_multi_indices,
            result.active_frontier,
            result.refinement_order,
            result.index_records,
            result.samples,
            result.unresolved_regions,
            result.evaluated_point_count,
            result.valid_point_count,
            result.invalid_point_count,
            result.interpolation_work_consumed,
            result.payload_scalar_count,
            result.backend_work_unit_count,
            result.max_active_indicator,
            hash,
        )
    end
    provisional = rebuild("")
    return rebuild(_ROSS_MODULE._ross_sha256(
        _ROSS_MODULE._ross_v2_result_body(provisional)))
end

function _ross_rehashed_state_pending(state, pending_candidates)
    function rebuild(hash)
        return _ROSS_MODULE.ROSparseAdaptiveStateV2(
            _ROSS_MODULE._ROSS_V2_TOKEN,
            state.schema_version,
            state.plan_sha256,
            state.sampling_spec_sha256,
            state.initial_cursor,
            state.accepted_multi_indices,
            state.refinement_order,
            pending_candidates,
            state.index_records,
            state.samples,
            state.unresolved_regions,
            state.interpolation_work_consumed,
            state.payload_scalar_count,
            state.backend_work_unit_count,
            hash,
        )
    end
    provisional = rebuild("")
    return rebuild(_ROSS_MODULE._ross_sha256(
        _ROSS_MODULE._ross_v2_state_body(provisional)))
end

function _ross_rehashed_batch_cursor_pending(
    batch, initial_cursor_after, pending_candidates_after,
)
    function rebuild(hash)
        return _ROSS_MODULE.ROSparseIndexBatchV2(
            _ROSS_MODULE._ROSS_V2_TOKEN,
            batch.schema_version,
            batch.plan_sha256,
            batch.prior_state_sha256,
            batch.batch_ordinal,
            batch.multi_index,
            batch.refinements_to_commit,
            initial_cursor_after,
            pending_candidates_after,
            batch.requests,
            batch.point_count,
            batch.payload_scalar_count,
            batch.interpolation_work,
            hash,
        )
    end
    provisional = rebuild("")
    return rebuild(_ROSS_MODULE._ross_sha256(
        _ROSS_MODULE._ross_v2_batch_body(provisional)))
end

function _ross_rehashed_result_frontier(result, active_frontier)
    function rebuild(hash)
        return _ROSS_MODULE.ROSparseSamplingResultV2(
            _ROSS_MODULE._ROSS_V2_TOKEN,
            result.schema_version,
            result.plan_sha256,
            result.sampling_spec_sha256,
            result.terminal_state_sha256,
            result.status,
            result.stopping_reason,
            result.evidence_scope,
            result.accepted_multi_indices,
            active_frontier,
            result.refinement_order,
            result.index_records,
            result.samples,
            result.unresolved_regions,
            result.evaluated_point_count,
            result.valid_point_count,
            result.invalid_point_count,
            result.interpolation_work_consumed,
            result.payload_scalar_count,
            result.backend_work_unit_count,
            result.max_active_indicator,
            hash,
        )
    end
    provisional = rebuild("")
    return rebuild(_ROSS_MODULE._ross_sha256(
        _ROSS_MODULE._ross_v2_result_body(provisional)))
end

@testset "nested Clenshaw-Curtis and downward-closed Smolyak algebra" begin
    @test _ROSS_MODULE.nested_clenshaw_curtis_nodes(1) == [0.0]
    @test _ROSS_MODULE.nested_clenshaw_curtis_nodes(2) == [-1.0, 0.0, 1.0]
    @test length.([
        _ROSS_MODULE.nested_clenshaw_curtis_nodes(level) for level in 1:5
    ]) == [1, 3, 5, 9, 17]
    for level in 1:4
        coarse = Set(_ROSS_MODULE.nested_clenshaw_curtis_nodes(level))
        fine = Set(_ROSS_MODULE.nested_clenshaw_curtis_nodes(level + 1))
        @test issubset(coarse, fine)
        increment = Set(
            _ROSS_MODULE.incremental_clenshaw_curtis_nodes(level + 1))
        @test isempty(intersect(coarse, increment))
        @test union(coarse, increment) == fine
    end
    @test_throws ArgumentError _ROSS_MODULE.nested_clenshaw_curtis_nodes(0)
    @test_throws ArgumentError _ROSS_MODULE.nested_clenshaw_curtis_nodes(13)

    downward = Any[[1, 1], [2, 1], [1, 2]]
    @test _ROSS_MODULE.smolyak_is_downward_closed(downward)
    @test _ROSS_MODULE.smolyak_admissible_frontier(
        downward; max_level=3) == Any[[1, 3], [2, 2], [3, 1]]
    @test !_ROSS_MODULE.smolyak_is_downward_closed(
        Any[[1, 1], [2, 2]])
    @test_throws ArgumentError _ROSS_MODULE.smolyak_admissible_frontier(
        Any[[1, 1], [2, 2]]; max_level=3)
    @test_throws ArgumentError _ROSS_MODULE.smolyak_is_downward_closed(Any[])
    @test_throws DimensionMismatch _ROSS_MODULE.smolyak_is_downward_closed(
        Any[[1, 1], [1]])
    @test_throws ArgumentError _ROSS_MODULE.smolyak_is_downward_closed(
        Any[[1], [1]])
end

@testset "analytic polynomial and explicit plan binding" begin
    raw_lower = [-1.0]
    raw_jacobian = ones(1, 1)
    plan = _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="polynomial-chart",
        control_ids=["u"],
        source_coordinate_ids=["theta"],
        chart_jacobian=raw_jacobian,
        domain_lower=raw_lower,
        domain_upper=[1.0],
        output_ids=["quadratic"],
        fixed_background=[3.5],
        surplus_tolerance=1e-10,
    )
    raw_lower[1] = -99.0
    raw_jacobian[1, 1] = 99.0
    @test plan.chart_id == "polynomial-chart"
    @test plan.control_ids == ["u"]
    @test plan.source_coordinate_ids == ["theta"]
    @test plan.domain_lower == [-1.0]
    @test plan.domain_upper == [1.0]
    @test plan.output_ids == ["quadratic"]
    @test plan.fixed_background == [3.5]
    @test plan.chart_jacobian == ones(1, 1)
    @test plan.initial_total_degree == 2
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="blind-policy", control_ids=["u"],
        source_coordinate_ids=["theta"], chart_jacobian=ones(1, 1),
        domain_lower=[-1.0], domain_upper=[1.0], output_ids=["f"],
        fixed_background=[0.0], initial_total_degree=1)

    evaluator = request -> begin
        u = only(request.control_coordinates)
        @test only(request.source_coordinates) ≈ 3.5 + u atol=0.0
        _ROSS_MODULE.ro_sparse_valid([1.0 + 2.0u + 3.0u^2])
    end
    result = _ROSS_MODULE.adaptive_sparse_ro_field(plan, evaluator)
    @test result.status == :complete_finite_policy
    @test result.stopping_reason == :surplus_tolerance_met
    @test result.evidence_scope == :finite_adaptive_policy_only
    @test result.continuum_error_bound === nothing
    @test occursin(r"^[0-9a-f]{64}$", result.sampling_spec_sha256)
    @test occursin(r"^[0-9a-f]{64}$", result.adaptive_plan_sha256)
    @test result.sampling_spec_sha256 ==
        _ROSS_MODULE.ro_sparse_sampling_spec_sha256(plan)
    @test result.evaluated_point_count == 5
    @test result.valid_point_count == 5
    @test result.invalid_point_count == 0
    @test result.accepted_multi_indices == Any[[1], [2], [3]]
    @test result.refinement_order == Any[[1], [2]]
    @test _ROSS_MODULE.smolyak_is_downward_closed(
        result.accepted_multi_indices)
    @test length(unique(sample.request.point_id for sample in result.samples)) == 5
    point_prefix = "ross:$(result.sampling_spec_sha256):"
    @test [sample.request.point_id for sample in result.samples] == [
        point_prefix * "cc-point[cc:1/2]",
        point_prefix * "cc-point[cc:0/1]",
        point_prefix * "cc-point[cc:1/1]",
        point_prefix * "cc-point[cc:1/4]",
        point_prefix * "cc-point[cc:3/4]",
    ]
    for sample in result.samples
        u = only(sample.request.control_coordinates)
        @test only(sample.evaluation.values) ≈ 1.0 + 2.0u + 3.0u^2
        @test sample.surplus !== nothing
    end
    @test result.index_records[end].multi_index == [3]
    @test result.index_records[end].indicator <= 2e-15
    @test result.max_active_indicator <= plan.surplus_tolerance

    changed_domain = _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id=plan.chart_id, control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=plan.chart_jacobian,
        domain_lower=[-2.0], domain_upper=plan.domain_upper,
        output_ids=plan.output_ids, fixed_background=plan.fixed_background,
        surplus_tolerance=plan.surplus_tolerance)
    @test _ROSS_MODULE.ro_sparse_sampling_spec_sha256(changed_domain) !=
        result.sampling_spec_sha256
    signed_zero_plan = _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id=plan.chart_id,
        control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=reshape([-0.0], 1, 1) .+ 1.0,
        domain_lower=[-1.0],
        domain_upper=[1.0],
        output_ids=plan.output_ids,
        fixed_background=[-0.0],
        surplus_tolerance=plan.surplus_tolerance,
    )
    positive_zero_plan = _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id=plan.chart_id,
        control_ids=plan.control_ids,
        source_coordinate_ids=plan.source_coordinate_ids,
        chart_jacobian=ones(1, 1),
        domain_lower=[-1.0],
        domain_upper=[1.0],
        output_ids=plan.output_ids,
        fixed_background=[0.0],
        surplus_tolerance=plan.surplus_tolerance,
    )
    @test !signbit(only(signed_zero_plan.fixed_background))
    @test _ROSS_MODULE.ro_sparse_sampling_spec_sha256(signed_zero_plan) ==
        _ROSS_MODULE.ro_sparse_sampling_spec_sha256(positive_zero_plan)
    tighter_limits = _ROSS_MODULE.ROSparseSamplingLimits(max_points=2_048)
    @test _ROSS_MODULE.ro_sparse_sampling_spec_sha256(plan) ==
        result.sampling_spec_sha256
    @test _ROSS_MODULE.ro_sparse_adaptive_plan_sha256(plan, tighter_limits) !=
        result.adaptive_plan_sha256
end

@testset "detached per-index commit receipts and deterministic replay" begin
    plan = _ross_plan_1d()
    receipts = Any[]
    result = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> _ROSS_MODULE.ro_sparse_valid([
            1.0 + only(request.control_coordinates)^2,
        ]);
        index_commit_callback=receipt -> push!(receipts, receipt),
    )
    @test length(receipts) == length(result.accepted_multi_indices)
    @test getfield.(receipts, :multi_index) ==
        Tuple.(result.accepted_multi_indices)
    @test all(receipt -> receipt.status == "resolved", receipts)
    @test all(receipt -> Tuple(getfield.(receipt.samples, :point_id)) ==
        receipt.point_ids, receipts)

    cached = Dict(
        sample.point_id => sample.values for receipt in receipts
        for sample in receipt.samples
    )
    live_calls = Ref(0)
    replay_receipts = Any[]
    replay = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            values = get(cached, request.point_id, nothing)
            values === nothing && (live_calls[] += 1)
            values === nothing ?
                _ROSS_MODULE.ro_sparse_invalid(:missing_committed_point) :
                _ROSS_MODULE.ro_sparse_valid(collect(values))
        end;
        index_commit_callback=receipt -> push!(replay_receipts, receipt),
    )
    @test live_calls[] == 0
    @test replay_receipts == receipts
    @test replay.accepted_multi_indices == result.accepted_multi_indices
    @test replay.refinement_order == result.refinement_order
    @test replay.stopping_reason == result.stopping_reason

    @test_throws ArgumentError _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> _ROSS_MODULE.ro_sparse_valid([1.0]);
        index_commit_callback=1,
    )
    callback_calls = Ref(0)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> _ROSS_MODULE.ro_sparse_valid([1.0]);
        index_commit_callback=_ -> begin
            callback_calls[] += 1
            throw(ROSparseCancelProbe())
        end,
    )
    @test callback_calls[] == 1
end

@testset "deterministic frontier selection and lexicographic ties" begin
    plan = _ross_plan_2d()
    function run_once()
        seen = String[]
        evaluator = request -> begin
            push!(seen, request.point_id)
            u, v = request.control_coordinates
            _ROSS_MODULE.ro_sparse_valid([1.0 + u^2 + v^2])
        end
        result = _ROSS_MODULE.adaptive_sparse_ro_field(plan, evaluator)
        return result, seen
    end
    first_result, first_seen = run_once()
    second_result, second_seen = run_once()
    @test first_result.status == :complete_finite_policy
    @test first_result.stopping_reason == :surplus_tolerance_met
    @test first_result.accepted_multi_indices == Any[
        [1, 1], [1, 2], [2, 1], [1, 3], [2, 2], [3, 1],
    ]
    @test first_result.refinement_order == Any[
        [1, 1], [1, 2], [2, 1],
    ]
    @test first_result.active_frontier == Any[
        [1, 3], [2, 2], [3, 1],
    ]
    @test first_result.evaluated_point_count == 13
    @test first_seen == second_seen
    @test first_result.accepted_multi_indices ==
        second_result.accepted_multi_indices
    @test first_result.refinement_order == second_result.refinement_order
    @test getfield.(first_result.index_records, :indicator) ==
        getfield.(second_result.index_records, :indicator)
    @test length(first_seen) == length(unique(first_seen))
    @test _ROSS_MODULE.smolyak_is_downward_closed(
        first_result.accepted_multi_indices)
    mixed_record = only(filter(record -> record.multi_index == [2, 2],
        first_result.index_records))
    @test mixed_record.indicator == 0.0
    @test first_result.max_active_indicator <= 3e-15

    interaction_seen = Vector{Vector{Int}}()
    interaction = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            push!(interaction_seen, copy(request.multi_index))
            u, v = request.control_coordinates
            _ROSS_MODULE.ro_sparse_valid([u * v])
        end,
    )
    mixed_interaction = only(filter(
        record -> record.multi_index == [2, 2],
        interaction.index_records))
    @test mixed_interaction.indicator > 0.9
    @test [2, 2] in interaction_seen
    @test interaction.evaluated_point_count > 13
end

@testset "invalid samples create unresolved regions without numeric surplus" begin
    plan = _ross_plan_1d()
    evaluator = request -> begin
        u = only(request.control_coordinates)
        u > 0.5 ?
            _ROSS_MODULE.ro_sparse_invalid(:solver_nonconvergence) :
            _ROSS_MODULE.ro_sparse_valid([u^2])
    end
    result = _ROSS_MODULE.adaptive_sparse_ro_field(plan, evaluator)
    @test result.status == :unknown_gap
    @test result.stopping_reason == :frontier_exhausted_with_unresolved
    @test result.evidence_scope == :finite_adaptive_policy_only
    @test result.continuum_error_bound === nothing
    @test result.evaluated_point_count == 3
    @test result.valid_point_count == 2
    @test result.invalid_point_count == 1
    @test length(result.unresolved_regions) == 1
    unresolved = only(result.unresolved_regions)
    @test unresolved.multi_index == [2]
    @test unresolved.reasons == [:solver_nonconvergence]
    level_two = filter(sample -> sample.request.multi_index == [2],
        result.samples)
    @test length(level_two) == 2
    @test all(sample -> sample.surplus === nothing, level_two)
    invalid = only(filter(sample -> !sample.evaluation.valid, result.samples))
    @test invalid.evaluation.values === nothing
    @test invalid.evaluation.invalid_reason == :solver_nonconvergence
    @test invalid.surplus === nothing
    @test result.index_records[end].indicator === nothing
    @test result.index_records[end].status == :unresolved_gap

    branch_plan = _ross_plan_2d()
    branch_seen = Vector{Vector{Int}}()
    branched = _ROSS_MODULE.adaptive_sparse_ro_field(
        branch_plan,
        request -> begin
            push!(branch_seen, copy(request.multi_index))
            request.multi_index == [2, 1] ?
                _ROSS_MODULE.ro_sparse_invalid(:branch_gap) :
                _ROSS_MODULE.ro_sparse_valid([sum(request.control_coordinates)])
        end,
    )
    @test branched.status == :unknown_gap
    @test branched.stopping_reason in (
        :surplus_tolerance_met_with_unresolved,
        :frontier_exhausted_with_unresolved,
    )
    @test [1, 3] in branch_seen
    @test !([2, 2] in branch_seen)
    @test !([3, 1] in branch_seen)
    @test all(record -> record.indicator === nothing,
        filter(record -> record.multi_index == [2, 1],
            branched.index_records))
end

@testset "malformed input, hard budgets, and cooperative cancellation" begin
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="bad", control_ids=["u", "u"],
        source_coordinate_ids=["theta"], chart_jacobian=ones(1, 2),
        domain_lower=[-1.0, -1.0], domain_upper=[1.0, 1.0],
        output_ids=["f"], fixed_background=[0.0])
    @test_throws DimensionMismatch _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="bad", control_ids=["u", "v"],
        source_coordinate_ids=["theta"], chart_jacobian=ones(1, 2),
        domain_lower=[-1.0, -1.0], domain_upper=[1.0, 1.0],
        output_ids=["f"], fixed_background=[0.0])
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="bad", control_ids=["u", "v"],
        source_coordinate_ids=["a", "b"], chart_jacobian=ones(2, 2),
        domain_lower=[-1.0, -1.0], domain_upper=[1.0, 1.0],
        output_ids=["f"], fixed_background=zeros(2))
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingPlan(
        chart_id="bad", control_ids=["u"],
        source_coordinate_ids=["theta"], chart_jacobian=ones(1, 1),
        domain_lower=[1.0], domain_upper=[1.0], output_ids=["f"],
        fixed_background=[0.0])
    @test_throws ArgumentError _ross_plan_1d(tolerance=NaN)
    @test_throws ArgumentError _ROSS_MODULE.ro_sparse_valid([NaN])
    @test_throws ArgumentError _ROSS_MODULE.ro_sparse_invalid(:none)
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingLimits(
        max_points=0)
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingLimits(
        max_level=13)
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingLimits(
        -1, 1, 1, 1, 1)
    @test_throws ArgumentError _ROSS_MODULE.ROSparseSamplingLimits(
        13, 4_096, 256, 10_000_000, 262_144)

    plan = _ross_plan_1d()
    @test_throws ArgumentError _ROSS_MODULE.adaptive_sparse_ro_field(
        plan, request -> [only(request.control_coordinates)])
    @test_throws DimensionMismatch _ROSS_MODULE.adaptive_sparse_ro_field(
        plan, request -> _ROSS_MODULE.ro_sparse_valid([1.0, 2.0]))
    @test_throws ArgumentError _ROSS_MODULE.adaptive_sparse_ro_field(
        plan, request -> _ROSS_MODULE.ROSparseEvaluation(
            false, [0.0], :solver_nonconvergence))

    point_calls = Ref(0)
    point_limited = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            point_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0])
        end;
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_points=1),
    )
    @test point_limited.status == :partial_budget
    @test point_limited.stopping_reason == :point_budget_exhausted
    @test point_limited.evaluated_point_count == point_calls[] == 1
    @test point_limited.evaluated_point_count <= point_limited.limits.max_points

    work_calls = Ref(0)
    work_limited = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            work_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0])
        end;
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_work=1),
    )
    @test work_limited.stopping_reason == :work_budget_exhausted
    @test work_limited.evaluated_point_count == work_calls[] == 0
    @test work_limited.work_units_consumed <= work_limited.limits.max_work

    two_output_plan = _ross_plan_1d(outputs=["f", "g"])
    payload_calls = Ref(0)
    payload_limited = _ROSS_MODULE.adaptive_sparse_ro_field(
        two_output_plan,
        request -> begin
            payload_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0, 2.0])
        end;
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_payload_scalars=1),
    )
    @test payload_limited.stopping_reason == :payload_budget_exhausted
    @test payload_limited.evaluated_point_count == payload_calls[] == 0
    @test payload_limited.payload_scalar_count <=
        payload_limited.limits.max_payload_scalars

    level_calls = Ref(0)
    level_limited = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            level_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0])
        end;
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_level=1),
    )
    @test level_limited.stopping_reason == :level_budget_exhausted
    @test level_limited.evaluated_point_count == level_calls[] == 1
    @test maximum(maximum, level_limited.accepted_multi_indices) <= 1

    index_limited = _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> _ROSS_MODULE.ro_sparse_valid([1.0]);
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_multi_indices=1),
    )
    @test index_limited.stopping_reason == :multi_index_budget_exhausted
    @test length(index_limited.accepted_multi_indices) == 1

    cancel_checks = Ref(0)
    evaluator_calls = Ref(0)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            evaluator_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0])
        end;
        cancel_check=() -> begin
            cancel_checks[] += 1
            throw(ROSparseCancelProbe())
        end,
    )
    @test cancel_checks[] == 1
    @test evaluator_calls[] == 0

    post_checks = Ref(0)
    post_calls = Ref(0)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.adaptive_sparse_ro_field(
        plan,
        request -> begin
            post_calls[] += 1
            _ROSS_MODULE.ro_sparse_valid([1.0])
        end;
        cancel_check=() -> begin
            post_checks[] += 1
            post_checks[] == 2 && throw(ROSparseCancelProbe())
        end,
    )
    @test post_calls[] == 1
    @test post_checks[] == 2
end

@testset "v2 canonical RO-channel plan and token identity" begin
    raw_outputs = ["y1", "y2"]
    raw_background = [-0.0, 0.0]
    plan = _ROSS_MODULE.ROSparseROChannelPlanV2(
        chart_id="canonical-two-input",
        control_ids=["u", "v"],
        source_coordinate_ids=["theta_u", "theta_v"],
        chart_jacobian=Matrix{Float64}(I, 2, 2),
        domain_lower=[-1.0, -1.0],
        domain_upper=[1.0, 1.0],
        output_ids=raw_outputs,
        fixed_background=raw_background,
    )
    raw_outputs[1] = "mutated"
    raw_background[1] = 7.0
    @test plan.output_ids == ["y1", "y2"]
    @test plan.fixed_background == [0.0, 0.0]
    @test !signbit(first(plan.fixed_background))
    components = _ROSS_MODULE.ro_sparse_ro_channel_order_v2(plan)
    @test getfield.(components, :channel_index) == 1:4
    @test getfield.(components, :output_id) == ["y1", "y1", "y2", "y2"]
    @test getfield.(components, :input_axis_id) == ["u", "v", "u", "v"]
    payload = _ROSS_MODULE.ro_sparse_ro_channel_plan_v2_payload(plan)
    @test payload.sampling_spec.policy.indicator ==
        "ordered_ro_components_linf_hierarchical_surplus"
    @test payload.sampling_spec.policy.channel_order ==
        "output_major_then_input_minor"
    @test payload.sampling_spec.policy.continuum_error_bound === nothing
    @test payload.execution_budget.max_interpolation_work ==
        plan.limits.max_work
    @test payload.portable_token_policy.max_token_bytes ==
        _ROSS_MODULE.ro_sparse_portable_token_byte_limit_v2(plan)
    @test payload.portable_token_policy.enforcement ==
        "all_v2_token_construction_publication_and_restore"
    @test ncodeunits(JSON3.write(payload)) <=
        _ROSS_MODULE.ro_sparse_portable_token_byte_limit_v2(plan)
    restored = _ROSS_MODULE.restore_ro_sparse_ro_channel_plan_v2(
        JSON3.write(payload))
    @test restored.plan_sha256 == plan.plan_sha256
    @test restored.sampling_spec_sha256 == plan.sampling_spec_sha256
    @test _ROSS_MODULE.validate_ro_sparse_ro_channel_plan_v2(restored)
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(" " * JSON3.write(payload))
    too_deep = repeat("[", 65) * "0" * repeat("]", 65)
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(too_deep)

    signed_zero = _ross_plan_v2_1d()
    positive_zero = _ross_plan_v2_1d()
    @test signed_zero.sampling_spec_sha256 ==
        positive_zero.sampling_spec_sha256
    tighter = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_points=2_048))
    @test tighter.sampling_spec_sha256 == positive_zero.sampling_spec_sha256
    @test tighter.plan_sha256 != positive_zero.plan_sha256
    changed_output = _ross_plan_v2_1d(outputs=["different"])
    @test changed_output.sampling_spec_sha256 !=
        positive_zero.sampling_spec_sha256

    stale_hash = merge(payload, (plan_sha256=repeat("0", 64),))
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(stale_hash)
    bad_policy = merge(payload.sampling_spec.policy,
        (indicator="full_output_linf_hierarchical_surplus",))
    bad_spec = merge(payload.sampling_spec, (policy=bad_policy,))
    bad_spec_hash = _ROSS_MODULE._ross_sha256(bad_spec)
    bad_body = (
        schema_version=payload.schema_version,
        sampling_spec=bad_spec,
        execution_budget=payload.execution_budget,
        portable_token_policy=payload.portable_token_policy,
        sampling_spec_sha256=bad_spec_hash,
    )
    rehashed_semantic_tamper = merge(bad_body,
        (plan_sha256=_ROSS_MODULE._ross_sha256(bad_body),))
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(rehashed_semantic_tamper)

    detached_plan = _ross_plan_v2_1d()
    detached_controls = detached_plan.control_ids
    detached_controls[1] = "mutated"
    detached_jacobian = detached_plan.chart_jacobian
    detached_jacobian[1, 1] = 9.0
    @test detached_plan.control_ids == ["u"]
    @test detached_plan.chart_jacobian == ones(1, 1)
    @test _ROSS_MODULE.validate_ro_sparse_ro_channel_plan_v2(detached_plan)
    for field in (
        :control_ids, :source_coordinate_ids, :chart_jacobian,
        :domain_lower, :domain_upper, :output_ids, :fixed_background,
    )
        @test getproperty(detached_plan, field) !==
            getfield(detached_plan, field)
    end

    mutable_plan = _ross_plan_v2_1d()
    getfield(mutable_plan, :domain_lower)[1] = -9.0
    @test_throws ArgumentError mutable_plan.plan_sha256
    @test_throws ArgumentError _ROSS_MODULE.
        validate_ro_sparse_ro_channel_plan_v2(mutable_plan)
    @test_throws MethodError _ROSS_MODULE.ROSparseAdaptiveStateV2()
    @test all(type -> type != Function,
        fieldtypes(_ROSS_MODULE.ROSparseAdaptiveStateV2))
    @test all(type -> type != Function,
        fieldtypes(_ROSS_MODULE.ROSparseIndexBatchV2))
end

@testset "v2 pure transitions survive every batch boundary and pending replay" begin
    plan = _ross_plan_v2_2d(
        tolerance=1e-12,
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_multi_indices=8),
    )
    evaluator = request -> begin
        u, v = request.control_coordinates
        # Exact plan order: (y1,u), (y1,v), (y2,u), (y2,v).
        _ROSS_MODULE.ro_sparse_valid([u * v, u, v, u - v])
    end
    uninterrupted = _ROSS_MODULE.adaptive_sparse_ro_field_v2(plan, evaluator)

    state = _ROSS_MODULE.initialize_ro_sparse_state_v2(plan)
    @test _ROSS_MODULE.validate_ro_sparse_state_v2(plan, state)
    saw_pending_batch = false
    saw_pending_resumed_state = false
    while true
        batch = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(plan, state)
        batch === nothing && break
        batch_json = JSON3.write(
            _ROSS_MODULE.ro_sparse_index_batch_v2_payload(batch))
        restored_batch = _ROSS_MODULE.restore_ro_sparse_index_batch_v2(
            plan, state, batch_json)
        @test _ROSS_MODULE.ro_sparse_index_batch_v2_payload(restored_batch) ==
            _ROSS_MODULE.ro_sparse_index_batch_v2_payload(batch)
        receipts = [_ROSS_MODULE.ro_sparse_ordered_evaluation_v2(
            request, evaluator(request)) for request in restored_batch.requests]
        next_state = _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
            plan, state, restored_batch, receipts)
        state_json = JSON3.write(
            _ROSS_MODULE.ro_sparse_state_v2_payload(next_state))
        resumed = _ROSS_MODULE.restore_ro_sparse_state_v2(plan, state_json)
        @test resumed.state_sha256 == next_state.state_sha256
        @test _ROSS_MODULE.ro_sparse_state_v2_payload(resumed) ==
            _ROSS_MODULE.ro_sparse_state_v2_payload(next_state)
        if !isempty(restored_batch.pending_candidates_after)
            saw_pending_batch = true
            @test !isempty(resumed.pending_candidates)
            direct_next = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(
                plan, next_state)
            resumed_next = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(
                plan, resumed)
            @test _ROSS_MODULE.ro_sparse_index_batch_v2_payload(direct_next) ==
                _ROSS_MODULE.ro_sparse_index_batch_v2_payload(resumed_next)
            @test first(direct_next.multi_index) !=
                first(restored_batch.multi_index) ||
                direct_next.multi_index != restored_batch.multi_index
            saw_pending_resumed_state = true
        end
        state = resumed
    end
    resumed_result = _ROSS_MODULE.finalize_ro_sparse_state_v2(plan, state)
    @test saw_pending_batch
    @test saw_pending_resumed_state
    @test _ROSS_MODULE.ro_sparse_result_v2_payload(resumed_result) ==
        _ROSS_MODULE.ro_sparse_result_v2_payload(uninterrupted)
    @test resumed_result.status == :complete_finite_policy
    @test resumed_result.stopping_reason == :surplus_tolerance_met
    @test resumed_result.backend_work_unit_count == 8
    @test resumed_result.backend_work_unit_count ==
        length(resumed_result.index_records)
    @test resumed_result.evaluated_point_count >
        resumed_result.backend_work_unit_count
    @test resumed_result.interpolation_work_consumed >
        resumed_result.backend_work_unit_count
    @test _ROSS_MODULE.validate_ro_sparse_result_v2(resumed_result)
    @test _ROSS_MODULE.validate_ro_sparse_result_v2(
        plan, state, resumed_result)
    restored_result = _ROSS_MODULE.restore_ro_sparse_result_v2(
        plan, state,
        JSON3.write(_ROSS_MODULE.ro_sparse_result_v2_payload(resumed_result)))
    @test _ROSS_MODULE.ro_sparse_result_v2_payload(restored_result) ==
        _ROSS_MODULE.ro_sparse_result_v2_payload(resumed_result)
    result_body = _ROSS_MODULE._ross_v2_result_body(resumed_result)
    altered_result_body = merge(result_body, (status="unknown_gap",))
    rehashed_result = merge(altered_result_body,
        (result_sha256=_ROSS_MODULE._ross_sha256(altered_result_body),))
    @test_throws ArgumentError _ROSS_MODULE.restore_ro_sparse_result_v2(
        plan, state, rehashed_result)
    typed_rehashed_result = _ross_rehashed_result_status(
        resumed_result, :unknown_gap)
    @test_throws ArgumentError _ROSS_MODULE.
        validate_ro_sparse_result_v2(typed_rehashed_result)
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_result_v2_payload(typed_rehashed_result)
    @test_throws ArgumentError _ROSS_MODULE.validate_ro_sparse_result_v2(
        plan, state, typed_rehashed_result)

    rehashed_bad_frontier = _ross_rehashed_result_frontier(
        resumed_result, [[0]])
    @test_throws ArgumentError _ROSS_MODULE.
        validate_ro_sparse_result_v2(rehashed_bad_frontier)
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_result_v2_payload(rehashed_bad_frontier)

    # Rehashing altered scientific values is insufficient: state restoration
    # replays every committed transition and detects the changed surplus.
    first_batch = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(
        plan, _ROSS_MODULE.initialize_ro_sparse_state_v2(plan))
    first_receipts = [_ROSS_MODULE.ro_sparse_ordered_evaluation_v2(
        request, evaluator(request)) for request in first_batch.requests]
    one_commit = _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, _ROSS_MODULE.initialize_ro_sparse_state_v2(plan),
        first_batch, first_receipts)
    rehashed_bad_state = _ross_rehashed_state_pending(one_commit, [[0]])
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_state_v2_payload(rehashed_bad_state)
    rehashed_bad_batch = _ross_rehashed_batch_cursor_pending(
        first_batch, 0, [[0]])
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_index_batch_v2_payload(rehashed_bad_batch)
    body = _ROSS_MODULE._ross_v2_state_body(one_commit)
    altered_values = collect(first(body.samples).values)
    altered_values[1] += 1.0
    altered_sample = merge(first(body.samples),
        (values=Tuple(altered_values),))
    altered_samples = (altered_sample, Base.tail(body.samples)...)
    altered_body = merge(body, (samples=altered_samples,))
    rehashed_state = merge(altered_body,
        (state_sha256=_ROSS_MODULE._ross_sha256(altered_body),))
    @test_throws ArgumentError _ROSS_MODULE.restore_ro_sparse_state_v2(
        plan, rehashed_state)

    mutable_batch = _ROSS_MODULE.restore_ro_sparse_index_batch_v2(
        plan, _ROSS_MODULE.initialize_ro_sparse_state_v2(plan),
        JSON3.write(_ROSS_MODULE.ro_sparse_index_batch_v2_payload(first_batch)))
    detached_batch_index = mutable_batch.multi_index
    detached_batch_index[1] = 0
    detached_batch_requests = mutable_batch.requests
    detached_batch_requests[1].control_coordinates[1] = 99.0
    @test first(mutable_batch.multi_index) >= 1
    @test mutable_batch.requests[1].control_coordinates[1] != 99.0
    for field in (
        :multi_index, :refinements_to_commit,
        :pending_candidates_after, :requests,
    )
        @test getproperty(mutable_batch, field) !==
            getfield(mutable_batch, field)
    end
    @test _ROSS_MODULE.ro_sparse_index_batch_v2_payload(mutable_batch) ==
        _ROSS_MODULE.ro_sparse_index_batch_v2_payload(first_batch)
    getfield(mutable_batch, :requests)[1].control_coordinates[1] = 99.0
    @test_throws ArgumentError mutable_batch.batch_sha256
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_index_batch_v2_payload(mutable_batch)
    @test_throws ArgumentError _ROSS_MODULE.validate_ro_sparse_index_batch_v2(
        plan, _ROSS_MODULE.initialize_ro_sparse_state_v2(plan), mutable_batch)
    mutable_state_payload = deepcopy(one_commit)
    detached_state_samples = mutable_state_payload.samples
    detached_state_samples[1].evaluation.values[1] += 1.0
    @test mutable_state_payload.samples[1].evaluation.values[1] !=
        detached_state_samples[1].evaluation.values[1]
    for field in (
        :accepted_multi_indices, :refinement_order, :pending_candidates,
        :index_records, :samples, :unresolved_regions,
    )
        @test getproperty(mutable_state_payload, field) !==
            getfield(mutable_state_payload, field)
    end
    @test _ROSS_MODULE.ro_sparse_state_v2_payload(mutable_state_payload) ==
        _ROSS_MODULE.ro_sparse_state_v2_payload(one_commit)
    getfield(mutable_state_payload, :samples)[1].evaluation.values[1] += 1.0
    @test_throws ArgumentError mutable_state_payload.state_sha256
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_state_v2_payload(mutable_state_payload)
    mutable_result_payload = deepcopy(resumed_result)
    detached_result_samples = mutable_result_payload.samples
    detached_result_samples[1].evaluation.values[1] += 1.0
    @test mutable_result_payload.samples[1].evaluation.values[1] !=
        detached_result_samples[1].evaluation.values[1]
    for field in (
        :accepted_multi_indices, :active_frontier, :refinement_order,
        :index_records, :samples, :unresolved_regions,
    )
        @test getproperty(mutable_result_payload, field) !==
            getfield(mutable_result_payload, field)
    end
    @test _ROSS_MODULE.ro_sparse_result_v2_payload(mutable_result_payload) ==
        _ROSS_MODULE.ro_sparse_result_v2_payload(resumed_result)
    getfield(mutable_result_payload, :samples)[1].evaluation.values[1] += 1.0
    @test_throws ArgumentError mutable_result_payload.result_sha256
    @test_throws ArgumentError _ROSS_MODULE.
        ro_sparse_result_v2_payload(mutable_result_payload)
    mutable_prior = _ROSS_MODULE.restore_ro_sparse_state_v2(
        plan, JSON3.write(_ROSS_MODULE.ro_sparse_state_v2_payload(one_commit)))
    next_batch = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(
        plan, mutable_prior)
    getfield(mutable_prior, :accepted_multi_indices)[1][1] += 1
    @test_throws ArgumentError _ROSS_MODULE.validate_ro_sparse_index_batch_v2(
        plan, mutable_prior, next_batch)
end

@testset "v2 invalid cones preserve incomparable branches" begin
    plan = _ross_plan_v2_2d()
    seen = Vector{Vector{Int}}()
    result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        plan,
        request -> begin
            push!(seen, copy(request.multi_index))
            u, v = request.control_coordinates
            if request.multi_index == [2, 1] &&
                    request.normalized_coordinates[1] > 0.0
                _ROSS_MODULE.ro_sparse_invalid(:solver_nonconvergence)
            else
                _ROSS_MODULE.ro_sparse_valid([u, v, u + v, u - v])
            end
        end,
    )
    @test result.status == :unknown_gap
    @test result.stopping_reason in (
        :surplus_tolerance_met_with_unresolved,
        :frontier_exhausted_with_unresolved,
    )
    @test length(result.unresolved_regions) == 1
    @test only(result.unresolved_regions).multi_index == [2, 1]
    @test only(result.unresolved_regions).reasons == [:solver_nonconvergence]
    unresolved_samples = filter(
        sample -> sample.request.multi_index == [2, 1], result.samples)
    @test length(unresolved_samples) == 2
    @test count(sample -> !sample.evaluation.valid, unresolved_samples) == 1
    @test all(sample -> sample.surplus === nothing, unresolved_samples)
    @test [1, 3] in seen
    @test !([2, 2] in seen)
    @test !([3, 1] in seen)
    @test result.backend_work_unit_count == length(result.index_records)
    @test result.interpolation_work_consumed > result.backend_work_unit_count
end

@testset "v2 exact commit receipts, budgets, foreign tokens, and cancel" begin
    plan = _ross_plan_v2_1d()
    state0 = _ROSS_MODULE.initialize_ro_sparse_state_v2(plan)
    batch1 = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(plan, state0)
    receipt1 = [_ROSS_MODULE.ro_sparse_ordered_evaluation_v2(
        request, _ROSS_MODULE.ro_sparse_valid([1.0]))
        for request in batch1.requests]
    state1 = _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state0, batch1, receipt1)
    batch2 = _ROSS_MODULE.prepare_ro_sparse_index_batch_v2(plan, state1)
    receipts2 = [_ROSS_MODULE.ro_sparse_ordered_evaluation_v2(
        request, _ROSS_MODULE.ro_sparse_valid([1.0]))
        for request in batch2.requests]
    @test length(receipts2) == 2
    @test_throws ArgumentError _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state1, batch2, receipts2[1:1])
    @test_throws ArgumentError _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state1, batch2, [receipts2[1], receipts2[1]])
    @test_throws ArgumentError _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state1, batch2, reverse(receipts2))
    foreign = merge(first(receipts2), (point_id="foreign-point",))
    @test_throws ArgumentError _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state1, batch2, [foreign, receipts2[2]])
    state2 = _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state1, batch2, receipts2)
    @test_throws ArgumentError _ROSS_MODULE.commit_ro_sparse_index_batch_v2(
        plan, state2, batch2, receipts2)
    other_plan = _ROSS_MODULE.ROSparseROChannelPlanV2(
        chart_id="other", control_ids=["u"],
        source_coordinate_ids=["theta"], chart_jacobian=ones(1, 1),
        domain_lower=[-2.0], domain_upper=[1.0], output_ids=["y"],
        fixed_background=[0.0])
    @test_throws ArgumentError _ROSS_MODULE.
        prepare_ro_sparse_index_batch_v2(other_plan, state1)

    point_plan = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_points=1))
    point_result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        point_plan, _ -> _ROSS_MODULE.ro_sparse_valid([1.0]))
    @test point_result.stopping_reason == :point_budget_exhausted
    @test point_result.evaluated_point_count == 1
    @test point_result.backend_work_unit_count == 1
    @test point_result.interpolation_work_consumed == 2

    work_plan = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_work=1))
    work_result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        work_plan, _ -> _ROSS_MODULE.ro_sparse_valid([1.0]))
    @test work_result.stopping_reason == :work_budget_exhausted
    @test work_result.interpolation_work_consumed == 0
    @test work_result.backend_work_unit_count == 0

    payload_plan = _ross_plan_v2_1d(
        outputs=["y1", "y2"],
        limits=_ROSS_MODULE.ROSparseSamplingLimits(max_payload_scalars=1))
    payload_result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        payload_plan, _ -> _ROSS_MODULE.ro_sparse_valid([1.0, 2.0]))
    @test payload_result.stopping_reason == :payload_budget_exhausted
    @test payload_result.payload_scalar_count == 0
    @test payload_result.backend_work_unit_count == 0

    index_plan = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_multi_indices=1))
    index_result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        index_plan, _ -> _ROSS_MODULE.ro_sparse_valid([1.0]))
    @test index_result.stopping_reason == :multi_index_budget_exhausted
    @test index_result.backend_work_unit_count == 1

    level_plan = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_level=1))
    level_result = _ROSS_MODULE.adaptive_sparse_ro_field_v2(
        level_plan, _ -> _ROSS_MODULE.ro_sparse_valid([1.0]))
    @test level_result.stopping_reason == :level_budget_exhausted
    @test level_result.backend_work_unit_count == 1

    oversized = ROSparseOversizedPortableToken()
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(oversized)
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_state_v2(plan, oversized)
    @test_throws ArgumentError _ROSS_MODULE.
        restore_ro_sparse_index_batch_v2(plan, state0, oversized)

    cancel = () -> throw(ROSparseCancelProbe())
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        initialize_ro_sparse_state_v2(plan; cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        prepare_ro_sparse_index_batch_v2(plan, state0; cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        commit_ro_sparse_index_batch_v2(
            plan, state0, batch1, receipt1; cancel_check=cancel)
    terminal_plan = _ross_plan_v2_1d(limits=
        _ROSS_MODULE.ROSparseSamplingLimits(max_work=1))
    terminal_state = _ROSS_MODULE.initialize_ro_sparse_state_v2(terminal_plan)
    terminal_result = _ROSS_MODULE.finalize_ro_sparse_state_v2(
        terminal_plan, terminal_state)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        finalize_ro_sparse_state_v2(
            terminal_plan, terminal_state; cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(
            _ROSS_MODULE.ro_sparse_ro_channel_plan_v2_payload(plan);
            cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.restore_ro_sparse_state_v2(
        plan, _ROSS_MODULE.ro_sparse_state_v2_payload(state0);
        cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        restore_ro_sparse_index_batch_v2(
            plan, state0,
            _ROSS_MODULE.ro_sparse_index_batch_v2_payload(batch1);
            cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.restore_ro_sparse_result_v2(
        terminal_plan, terminal_state,
        _ROSS_MODULE.ro_sparse_result_v2_payload(terminal_result);
        cancel_check=cancel)
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        validate_ro_sparse_result_v2(
            terminal_plan, terminal_state, terminal_result;
            cancel_check=cancel)
    scan_checks = Ref(0)
    scan_cancel = () -> begin
        scan_checks[] += 1
        scan_checks[] >= 3 && throw(ROSparseCancelProbe())
    end
    @test_throws ROSparseCancelProbe _ROSS_MODULE.
        restore_ro_sparse_ro_channel_plan_v2(
            repeat(" ", _ROSS_MODULE._ROSS_V2_JSON_SCAN_CANCEL_STRIDE + 1);
            cancel_check=scan_cancel)
    @test scan_checks[] == 3
    @test_throws ArgumentError _ROSS_MODULE.restore_ro_sparse_result_v2(
        terminal_plan, terminal_state, oversized)
    @test state0.backend_work_unit_count == 0
    @test isempty(state0.samples)

    interpolation_level = 9
    interpolation_nodes = _ROSS_MODULE._ross_full_nodes(interpolation_level)
    interpolation_spec = repeat("a", 64)
    interpolation_evaluations = Dict(
        _ROSS_MODULE._ross_point_id(interpolation_spec, (node,)) =>
            _ROSS_MODULE.ro_sparse_valid([node.coordinate])
        for node in interpolation_nodes
    )
    surplus_checks = Ref(0)
    @test_throws ROSparseCancelProbe _ROSS_MODULE._ross_tensor_interpolate(
        (interpolation_level,), [0.125], interpolation_evaluations, 1,
        interpolation_spec,
        () -> begin
            surplus_checks[] += 1
            surplus_checks[] == 3 && throw(ROSparseCancelProbe())
        end,
    )
    @test surplus_checks[] == 3
end
