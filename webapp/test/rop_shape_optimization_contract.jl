using Test

include(joinpath(@__DIR__, "..", "src", "rop_shape_optimization.jl"))
using .ROPShapeOptimization

const MAX_TAU = LinearWitnessObjective(
    "max_tau", [WitnessTerm(:tau, 1.0)]; sense=:maximize)

function box_cell(
    path::AbstractString;
    theta_bounds=(-1.0, 1.0),
    tau_bounds=(0.0, 1.0),
    row_scales=(1.0, 1.0, 1.0, 1.0),
)
    theta_lo, theta_hi = theta_bounds
    tau_lo, tau_hi = tau_bounds
    scales = Float64.(collect(row_scales))
    Aineq = [
         scales[1]  0.0
        -scales[2]  0.0
         0.0        scales[3]
         0.0       -scales[4]
    ]
    bineq = [
        scales[1] * theta_hi,
        -scales[2] * theta_lo,
        scales[3] * tau_hi,
        -scales[4] * tau_lo,
    ]
    return DesignabilityCellGeometry(
        zeros(Float64, 0, 2), Float64[], Aineq, bineq;
        coordinates=[:theta, :tau],
        parameter_coordinates=[:theta],
        witness_coordinates=[:tau],
        equality_row_ids=String[],
        inequality_row_ids=[
            "$path:theta_upper", "$path:theta_lower",
            "$path:tau_upper", "$path:tau_lower",
        ],
        path_identity=path,
        witness_identity=["$path:witness_1"],
    )
end

function two_witness_box_cell(
    path::AbstractString;
    theta_bounds=(-1.0, 1.0),
    left_bounds=(0.0, 1.0),
    right_bounds=(0.0, 1.0),
)
    theta_lo, theta_hi = theta_bounds
    left_lo, left_hi = left_bounds
    right_lo, right_hi = right_bounds
    return DesignabilityCellGeometry(
        zeros(Float64, 0, 3), Float64[],
        [
             1.0  0.0  0.0
            -1.0  0.0  0.0
             0.0  1.0  0.0
             0.0 -1.0  0.0
             0.0  0.0  1.0
             0.0  0.0 -1.0
        ],
        [theta_hi, -theta_lo, left_hi, -left_lo, right_hi, -right_lo];
        coordinates=[:theta, :tau_left, :tau_right],
        parameter_coordinates=[:theta],
        witness_coordinates=[:tau_left, :tau_right],
        equality_row_ids=String[],
        inequality_row_ids=[
            "$path:theta_upper", "$path:theta_lower",
            "$path:left_upper", "$path:left_lower",
            "$path:right_upper", "$path:right_lower",
        ],
        path_identity=path,
        witness_identity=["$path:witness_left", "$path:witness_right"],
    )
end

@testset "ROP shape optimization core" begin
    @testset "compiled geometry and typed witness rows retain identity" begin
        cell = box_cell("typed"; tau_bounds=(0.0, 10.0))
        constraint = LinearWitnessConstraint(
            "request:tau_cap", [WitnessTerm(:tau, 1.0)], :le, 4.0)
        result = optimize_fixed_cell(
            cell, MAX_TAU; constraints=[constraint], effect_tolerance=0.0)

        @test CompiledWitnessCell === DesignabilityCellGeometry
        @test result.status == OPTIMAL
        @test result.path_identity == "typed"
        @test result.witness_identity == ["typed:witness_1"]
        @test result.primary.effect ≈ 4.0 atol=1e-6
        @test result.margin !== nothing
        @test any(row -> row.row_id == "request:tau_cap", result.primary.active_rows)
        @test all(row -> isfinite(row.normalized_residual), result.primary.active_rows)
        cap_row = only(filter(
            row -> row.row_id == "request:tau_cap", result.primary.active_rows))
        @test cap_row.dual ≈ -1.0 atol=1e-6
        @test cap_row.shadow_price ≈ 1.0 atol=1e-6

        min_tau = LinearWitnessObjective(
            "min_tau", [WitnessTerm(:tau, 1.0)]; sense=:minimize)
        floor_constraint = LinearWitnessConstraint(
            "request:tau_floor", [WitnessTerm(:tau, 1.0)], :ge, 2.0)
        minimum = maximize_effect(cell, min_tau; constraints=[floor_constraint])
        @test minimum.status == OPTIMAL
        @test minimum.effect ≈ 2.0 atol=1e-6
        @test any(row -> row.row_id == "request:tau_floor", minimum.active_rows)
        floor_row = only(filter(
            row -> row.row_id == "request:tau_floor", minimum.active_rows))
        # The >= request is compiled as -tau <= -rhs. Its compiled-RHS
        # derivative is -1 even though the original request-RHS derivative is +1.
        @test floor_row.dual ≈ -1.0 atol=1e-6
        @test floor_row.shadow_price ≈ -1.0 atol=1e-6

        scaled_cap = LinearWitnessConstraint(
            "request:scaled_tau_cap", [WitnessTerm(:tau, 7.0)], :le, 28.0)
        scaled_result = maximize_effect(cell, MAX_TAU; constraints=[scaled_cap])
        scaled_row = only(filter(
            row -> row.row_id == "request:scaled_tau_cap",
            scaled_result.active_rows,
        ))
        @test scaled_row.dual ≈ -1 / 7 atol=1e-6
        @test scaled_row.shadow_price ≈ 1 / 7 atol=1e-6

        duplicate_caps = [
            LinearWitnessConstraint(
                "request:duplicate_cap_1", [WitnessTerm(:tau, 1.0)], :le, 4.0),
            LinearWitnessConstraint(
                "request:duplicate_cap_2", [WitnessTerm(:tau, 1.0)], :le, 4.0),
        ]
        degenerate = maximize_effect(cell, MAX_TAU; constraints=duplicate_caps)
        duplicate_rows = filter(
            row -> occursin("duplicate_cap", row.row_id), degenerate.active_rows)
        @test length(duplicate_rows) == 2
        @test all(row -> row.dual === nothing, duplicate_rows)
        @test all(row -> row.shadow_price === nothing, duplicate_rows)
        @test occursin("rank deficient", degenerate.message)
    end

    @testset "directional request intervals retain caller scaling" begin
        cell = box_cell(
            "directional"; theta_bounds=(-1.0, 1.0), tau_bounds=(0.0, 10.0))
        interval = directional_request_interval(cell, [2.0], [2.0])
        @test interval.status == OPTIMAL
        @test interval.lower_status == OPTIMAL
        @test interval.upper_status == OPTIMAL
        @test interval.alpha_min ≈ -1.0 atol=1e-6
        @test interval.alpha_max ≈ 4.0 atol=1e-6
        @test interval.direction == [2.0]
        @test interval.direction_norm == 2.0
        @test interval.reference_witness == [2.0]
        @test interval.witness_coordinates == [:tau]
        @test interval.path_identity == "directional"
        @test something(interval.lower_solution)[2] ≈ 0.0 atol=1e-6
        @test something(interval.upper_solution)[2] ≈ 10.0 atol=1e-6

        constrained = directional_request_interval(
            cell, [2.0], [2.0];
            constraints=[LinearWitnessConstraint(
                "directional:tau_cap", [WitnessTerm(:tau, 1.0)], :le, 8.0)],
        )
        @test constrained.alpha_min ≈ -1.0 atol=1e-6
        @test constrained.alpha_max ≈ 3.0 atol=1e-6

        rescaled = directional_request_interval(cell, [2.0], [4.0])
        @test rescaled.direction == [4.0]
        @test rescaled.direction_norm == 4.0
        @test rescaled.alpha_min ≈ -0.5 atol=1e-6
        @test rescaled.alpha_max ≈ 2.0 atol=1e-6

        one_sided = DesignabilityCellGeometry(
            zeros(Float64, 0, 2), Float64[],
            [-1.0 0.0; 0.0 -1.0], [0.0, 0.0];
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta], witness_coordinates=[:tau],
            equality_row_ids=String[],
            inequality_row_ids=["one_sided:theta_lower", "one_sided:tau_lower"],
            path_identity="one_sided", witness_identity=["one_sided:witness"],
        )
        one_sided_interval = directional_request_interval(
            one_sided, [0.0], [1.0])
        @test one_sided_interval.status == UNBOUNDED
        @test one_sided_interval.lower_status == OPTIMAL
        @test one_sided_interval.upper_status == UNBOUNDED
        @test one_sided_interval.alpha_min ≈ 0.0 atol=1e-6
        @test one_sided_interval.alpha_max === nothing

        coupled = DesignabilityCellGeometry(
            reshape([0.0, 1.0, -1.0], 1, 3), [0.0],
            two_witness_box_cell("directional_infeasible").Aineq,
            two_witness_box_cell("directional_infeasible").bineq;
            coordinates=[:theta, :tau_left, :tau_right],
            parameter_coordinates=[:theta],
            witness_coordinates=[:tau_left, :tau_right],
            equality_row_ids=["directional_infeasible:coupling"],
            inequality_row_ids=[
                "directional_infeasible:theta_upper",
                "directional_infeasible:theta_lower",
                "directional_infeasible:left_upper",
                "directional_infeasible:left_lower",
                "directional_infeasible:right_upper",
                "directional_infeasible:right_lower",
            ],
            path_identity="directional_infeasible",
            witness_identity=["directional_infeasible:left", "directional_infeasible:right"],
        )
        infeasible_interval = directional_request_interval(
            coupled, [0.0, 1.0], [1.0, 1.0])
        @test infeasible_interval.status == INFEASIBLE
        @test infeasible_interval.lower_status == INFEASIBLE
        @test infeasible_interval.upper_status == INFEASIBLE

        @test_throws ArgumentError directional_request_interval(cell, [2.0], [0.0])
        @test_throws ArgumentError directional_request_interval(cell, [2.0], [1.0, 2.0])
        numerical_interval = directional_request_interval(
            cell, [2.0], [1.0];
            optimizer_factory=() -> error("injected directional failure"),
        )
        @test numerical_interval.status == NUMERICAL_ERROR
        @test numerical_interval.lower_status == NUMERICAL_ERROR
        @test numerical_interval.upper_status == NUMERICAL_ERROR
    end

    @testset "balanced max-min objective preserves weakest improvement" begin
        balanced = BalancedMaxMinWitnessObjective(
            "balanced_ears",
            [
                WitnessImprovement(
                    "left", [WitnessTerm(:tau_left, 1.0)], 0.0),
                WitnessImprovement(
                    "right", [WitnessTerm(:tau_right, 1.0)], 1.0),
            ],
        )
        cell = two_witness_box_cell(
            "balanced_cell"; theta_bounds=(-2.0, 2.0),
            left_bounds=(0.0, 3.0), right_bounds=(0.0, 5.0))
        result = optimize_fixed_cell(cell, balanced; effect_tolerance=0.0)
        @test result.status == OPTIMAL
        @test result.primary.effect ≈ 3.0 atol=1e-6
        @test something(result.margin).parameter_margin ≈ 2.0 atol=1e-5
        @test any(
            row -> row.row_id ==
                   "objective:balanced_ears:improvement:left",
            result.primary.active_rows,
        )

        global_balanced = BalancedMaxMinWitnessObjective(
            "global_balanced",
            [
                WitnessImprovement(
                    "left", [WitnessTerm(:tau_left, 1.0)], 0.0),
                WitnessImprovement(
                    "right", [WitnessTerm(:tau_right, 1.0)], 0.0),
            ],
        )
        sharp = two_witness_box_cell(
            "balanced_sharp"; theta_bounds=(0.0, 0.2),
            left_bounds=(0.0, 4.0), right_bounds=(0.0, 4.0))
        robust = two_witness_box_cell(
            "balanced_robust"; theta_bounds=(-5.0, 5.0),
            left_bounds=(0.0, 3.95), right_bounds=(0.0, 3.95))
        relaxed = optimize_cell_union(
            [sharp, robust], global_balanced; effect_tolerance=0.1)
        @test relaxed.status == OPTIMAL
        @test relaxed.global_effect ≈ 4.0 atol=1e-6
        @test relaxed.selected_cell_index == 2
        @test something(something(relaxed.selected).margin).parameter_margin ≈ 5.0 atol=1e-5
        strict = optimize_cell_union(
            [sharp, robust], global_balanced; effect_tolerance=0.01)
        @test strict.status == OPTIMAL
        @test strict.selected_cell_index == 1

        unbounded_balanced_cell = DesignabilityCellGeometry(
            zeros(Float64, 0, 3), Float64[],
            [-1.0 0.0 0.0; 0.0 -1.0 0.0; 0.0 0.0 -1.0],
            [0.0, 0.0, 0.0];
            coordinates=[:theta, :tau_left, :tau_right],
            parameter_coordinates=[:theta],
            witness_coordinates=[:tau_left, :tau_right],
            equality_row_ids=String[],
            inequality_row_ids=[
                "balanced_unbounded:theta_lower",
                "balanced_unbounded:left_lower",
                "balanced_unbounded:right_lower",
            ],
            path_identity="balanced_unbounded",
            witness_identity=["balanced_unbounded:left", "balanced_unbounded:right"],
        )
        @test maximize_effect(
            unbounded_balanced_cell, global_balanced).status == UNBOUNDED
        @test maximize_effect(
            cell, balanced;
            optimizer_factory=() -> error("injected balanced failure"),
        ).status == NUMERICAL_ERROR

        @test_throws ArgumentError BalancedMaxMinWitnessObjective(
            "empty_balanced", WitnessImprovement[])
        @test_throws ArgumentError BalancedMaxMinWitnessObjective(
            "duplicate_balanced",
            [
                WitnessImprovement("same", [WitnessTerm(:tau_left, 1.0)], 0.0),
                WitnessImprovement("same", [WitnessTerm(:tau_right, 1.0)], 0.0),
            ],
        )
    end

    @testset "rectangle separates request flexibility from parameter margin" begin
        # Joint rectangle radius is 0.5, but the conditional parameter fiber is
        # [0, 100], whose Chebyshev radius is 50.
        rectangle = box_cell(
            "rectangle"; theta_bounds=(0.0, 100.0), tau_bounds=(0.0, 1.0))
        result = optimize_fixed_cell(
            rectangle, MAX_TAU; effect_tolerance=1e-7)

        @test result.status == OPTIMAL
        @test result.primary.effect ≈ 1.0 atol=1e-6
        @test result.margin !== nothing
        margin = something(result.margin)
        @test margin.status == OPTIMAL
        @test margin.parameter_margin ≈ 50.0 atol=1e-5
        @test margin.subspace.dimension == 1
        @test margin.subspace.equality_rank == 0
        @test margin.subspace.basis ≈ ones(1, 1) atol=1e-12
        active_ids = Set(row.row_id for row in margin.active_rows)
        @test "rectangle:theta_upper" in active_ids
        @test "rectangle:theta_lower" in active_ids
        @test all(row -> abs(row.normalized_residual) <= 1e-6, margin.active_rows)
    end

    @testset "coupled parameter fiber uses the 0D radius convention" begin
        coupled = DesignabilityCellGeometry(
            reshape([1.0, -1.0], 1, 2), [0.0],
            [1.0 0.0; -1.0 0.0; 0.0 1.0; 0.0 -1.0],
            [1.0, 0.0, 1.0, 0.0];
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta],
            witness_coordinates=[:tau],
            equality_row_ids=["coupled:theta_equals_tau"],
            inequality_row_ids=[
                "coupled:theta_upper", "coupled:theta_lower",
                "coupled:tau_upper", "coupled:tau_lower",
            ],
            path_identity="coupled",
            witness_identity=["coupled:witness_1"],
        )
        result = optimize_fixed_cell(coupled, MAX_TAU; effect_tolerance=1e-7)

        @test result.status == OPTIMAL
        @test result.primary.subspace.dimension == 0
        @test result.primary.subspace.equality_rank == 1
        @test size(result.primary.subspace.basis) == (1, 0)
        @test something(result.margin).parameter_margin == 0.0
        @test occursin("zero-dimensional", something(result.margin).message)

        scaled_coupling = DesignabilityCellGeometry(
            reshape([1.0e-12, -1.0e-12], 1, 2), [0.0],
            coupled.Aineq, coupled.bineq;
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta],
            witness_coordinates=[:tau],
            equality_row_ids=["scaled:theta_equals_tau"],
            inequality_row_ids=[
                "scaled:theta_upper", "scaled:theta_lower",
                "scaled:tau_upper", "scaled:tau_lower",
            ],
            path_identity="scaled_coupling",
            witness_identity=["scaled:witness_1"],
        )
        @test parameter_subspace(scaled_coupling).dimension == 0
        @test parameter_subspace(scaled_coupling).equality_rank == 1

        impossible_positive_margin = maximize_effect(
            coupled, MAX_TAU; minimum_parameter_margin=1e-3)
        @test impossible_positive_margin.status == INFEASIBLE
        @test impossible_positive_margin.effect === nothing
    end

    @testset "disconnected cell union is optimized cell-wise" begin
        left = box_cell(
            "left_component"; theta_bounds=(-1.0, 1.0), tau_bounds=(-3.0, -2.0))
        right = box_cell(
            "right_component"; theta_bounds=(-1.0, 1.0), tau_bounds=(1.0, 3.0))
        result = optimize_cell_union(
            [left, right], MAX_TAU; effect_tolerance=0.0)

        @test result.status == OPTIMAL
        @test result.global_effect ≈ 3.0 atol=1e-6
        @test result.selected_cell_index == 2
        @test something(result.selected).path_identity == "right_component"
        @test length(result.cell_results) == 2
        @test all(item -> item.primary.status == OPTIMAL, result.cell_results)
    end

    @testset "epsilon lexicographic selection is global, not cell-local" begin
        sharp_best = box_cell(
            "sharp_best"; theta_bounds=(0.0, 0.2), tau_bounds=(0.0, 10.0))
        robust_near = box_cell(
            "robust_near"; theta_bounds=(-5.0, 5.0), tau_bounds=(0.0, 9.95))

        relaxed = optimize_cell_union(
            [sharp_best, robust_near], MAX_TAU; effect_tolerance=0.1)
        @test relaxed.status == OPTIMAL
        @test relaxed.global_effect ≈ 10.0 atol=1e-6
        @test relaxed.selected_cell_index == 2
        @test something(something(relaxed.selected).margin).parameter_margin ≈ 5.0 atol=1e-5
        @test something(something(relaxed.selected).margin).effect >= 9.9 - 1e-6

        strict = optimize_cell_union(
            [sharp_best, robust_near], MAX_TAU; effect_tolerance=0.01)
        @test strict.status == OPTIMAL
        @test strict.selected_cell_index == 1
        @test something(something(strict.selected).margin).parameter_margin ≈ 0.1 atol=1e-5
        @test !strict.cell_results[2].selected_for_margin
    end

    @testset "row scaling preserves radius and active residual semantics" begin
        reference = box_cell(
            "scale_reference"; theta_bounds=(0.0, 100.0), tau_bounds=(0.0, 1.0))
        scaled = box_cell(
            "scale_variant"; theta_bounds=(0.0, 100.0), tau_bounds=(0.0, 1.0),
            row_scales=(7.0, 0.2, 3.0, 11.0))
        reference_result = optimize_fixed_cell(
            reference, MAX_TAU; effect_tolerance=1e-7)
        scaled_result = optimize_fixed_cell(
            scaled, MAX_TAU; effect_tolerance=1e-7)
        reference_margin = something(reference_result.margin)
        scaled_margin = something(scaled_result.margin)

        @test reference_margin.parameter_margin ≈ scaled_margin.parameter_margin atol=1e-5
        @test scaled_margin.parameter_margin ≈ 50.0 atol=1e-5
        theta_rows = filter(
            row -> occursin("theta_", row.row_id), scaled_margin.active_rows)
        @test length(theta_rows) == 2
        @test all(row -> abs(row.ball_residual) <= 1e-4, theta_rows)
        @test all(row -> abs(row.normalized_residual) <= 1e-6, theta_rows)
    end

    @testset "closed solver statuses and invalid input" begin
        infeasible_cell = box_cell(
            "infeasible"; theta_bounds=(-1.0, 1.0), tau_bounds=(1.0, 0.0))
        infeasible_result = maximize_effect(infeasible_cell, MAX_TAU)
        @test infeasible_result.status == INFEASIBLE
        @test status_name(infeasible_result.status) == "infeasible"

        unbounded_cell = DesignabilityCellGeometry(
            zeros(Float64, 0, 2), Float64[],
            [-1.0 0.0; 0.0 -1.0], [0.0, 0.0];
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta],
            witness_coordinates=[:tau],
            equality_row_ids=String[],
            inequality_row_ids=["unbounded:theta_lower", "unbounded:tau_lower"],
            path_identity="unbounded",
            witness_identity=["unbounded:witness_1"],
        )
        unbounded_result = maximize_effect(unbounded_cell, MAX_TAU)
        @test unbounded_result.status == UNBOUNDED
        @test status_name(unbounded_result.status) == "unbounded"

        injected_error = maximize_effect(
            box_cell("injected_error"), MAX_TAU;
            optimizer_factory=() -> error("injected optimizer failure"),
        )
        @test injected_error.status == NUMERICAL_ERROR
        @test occursin("injected optimizer failure", injected_error.message)
        @test status_name(injected_error.status) == "numerical_error"
        @test status_name(OPTIMAL) == "optimal"

        @test_throws ArgumentError DesignabilityCellGeometry(
            zeros(Float64, 0, 2), Float64[],
            [NaN 0.0], [1.0];
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta], witness_coordinates=[:tau],
            equality_row_ids=String[], inequality_row_ids=["invalid:nan"],
            path_identity="invalid_nan", witness_identity=["invalid:witness"],
        )
        @test_throws ArgumentError DesignabilityCellGeometry(
            reshape([1.0, 0.0], 1, 2), [0.0],
            reshape([0.0, 1.0], 1, 2), [1.0];
            coordinates=[:theta, :tau],
            parameter_coordinates=[:theta], witness_coordinates=[:tau],
            equality_row_ids=["duplicate"], inequality_row_ids=["duplicate"],
            path_identity="invalid_ids", witness_identity=["invalid:witness"],
        )
        @test_throws ArgumentError LinearWitnessObjective(
            "zero", WitnessTerm[]; sense=:maximize)
        @test_throws ArgumentError LinearWitnessObjective(
            "duplicate_terms", [WitnessTerm(:tau, 1.0), WitnessTerm(:tau, -1.0)];
            sense=:maximize,
        )
        parameter_objective = LinearWitnessObjective(
            "parameter_is_not_witness", [WitnessTerm(:theta, 1.0)]; sense=:maximize)
        @test_throws ArgumentError maximize_effect(
            box_cell("invalid_objective"), parameter_objective)
        colliding_constraint = LinearWitnessConstraint(
            "collision:tau_upper", [WitnessTerm(:tau, 1.0)], :le, 0.5)
        @test_throws ArgumentError maximize_effect(
            box_cell("collision"), MAX_TAU; constraints=[colliding_constraint])
        @test_throws ArgumentError parameter_subspace(
            box_cell("invalid_rank_tolerance"); rank_tolerance=0.0)
    end

    @testset "cell-union work has cooperative cancellation checkpoints" begin
        calls = Ref(0)
        @test_throws ErrorException optimize_cell_union(
            [box_cell("cancelled")], MAX_TAU;
            cancel_check=() -> begin
                calls[] += 1
                error("cancelled at checkpoint")
            end,
        )
        @test calls[] == 1
    end
end
