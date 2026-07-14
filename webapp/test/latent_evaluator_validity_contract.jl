using Test
using BiocircuitsExplorerBackend

if !isdefined(Main, :BehaviorEvaluators)
    include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl"))
end
const LEV = BehaviorEvaluators

@testset "Latent Atlas evaluator numerical validity" begin
    @testset "logic draw requires its four finite valid corners" begin
        grid = Float64[
            0 1 2
            3 4 5
            6 7 8
        ]
        validity = trues(size(grid))
        valid_draw = LEV._logic_draw_from_grid(grid, validity)
        @test valid_draw.valid === true
        @test valid_draw.table == LEV.LOGIC_TABLES["A"]
        @test valid_draw.margin == 4.0

        invalid_corner = copy(validity)
        invalid_corner[1, 1] = false
        rejected_corner = LEV._logic_draw_from_grid(grid, invalid_corner)
        @test rejected_corner.valid === false
        @test rejected_corner.table === nothing
        @test rejected_corner.reason == "invalid_required_corner"

        nonfinite_corner = copy(grid)
        nonfinite_corner[end, end] = NaN
        rejected_nonfinite = LEV._logic_draw_from_grid(nonfinite_corner, validity)
        @test rejected_nonfinite.valid === false
        @test rejected_nonfinite.table === nothing
        @test rejected_nonfinite.reason == "nonfinite_required_corner"

        # Logic consumes only the truth-table corners. An invalid unused interior
        # sample is preserved by the scan mask but does not erase valid corners.
        invalid_interior = copy(validity)
        invalid_interior[2, 2] = false
        @test LEV._logic_draw_from_grid(grid, invalid_interior).valid === true

        partial = LEV._summarize_logic_draws(
            [valid_draw, rejected_corner]; target="A")
        @test partial["requested_draw_count"] == 2
        @test partial["valid_draw_count"] == 1
        @test partial["invalid_draw_count"] == 1
        @test partial["partial"] === true
        @test partial["evidence_status"] == "partial"
        @test partial["realized_gate"] == "A"
        @test partial["truth_table_agreement"] == 1.0
        @test LEV.is_complete_evaluator_evidence(partial) === false

        no_evidence = LEV._summarize_logic_draws([rejected_corner, rejected_nonfinite])
        @test no_evidence["evidence_status"] == "no_evidence"
        @test no_evidence["valid_draw_count"] == 0
        @test no_evidence["realized_table"] === nothing
        @test no_evidence["realized_gate"] === nothing
        @test no_evidence["median_margin_decades"] === nothing
        @test isempty(no_evidence["tables"])
        @test LEV.is_complete_evaluator_evidence(no_evidence) === false
    end

    @testset "analog draw requires the complete finite grid" begin
        axis = [-1.0, 0.0, 1.0]
        grid = Float64[
            0 1 0
            1 5 1
            0 1 0
        ]
        validity = trues(size(grid))
        valid_draw = LEV._analog_draw_from_grid(grid, validity, axis, axis)
        @test valid_draw.valid === true
        @test valid_draw.bump == 1
        @test valid_draw.dynamic_range == 5.0

        # Compatibility check: a fully valid grid retains the legacy formulas.
        axis1_grid = Float64[axis[i] for i in eachindex(axis), _ in eachindex(axis)]
        axis2_grid = Float64[axis[j] for _ in eachindex(axis), j in eachindex(axis)]
        @test valid_draw.ratio_corr ≈
            LEV._corr(vec(axis1_grid .- axis2_grid), vec(grid))
        @test valid_draw.coactivation_corr ≈
            LEV._corr(vec(min.(axis1_grid, axis2_grid)), vec(grid))

        invalid_interior = copy(validity)
        invalid_interior[2, 2] = false
        rejected_interior = LEV._analog_draw_from_grid(
            grid, invalid_interior, axis, axis)
        @test rejected_interior.valid === false
        @test rejected_interior.bump === nothing
        @test rejected_interior.dynamic_range === nothing
        @test rejected_interior.reason == "invalid_required_grid_point"

        all_invalid = falses(size(grid))
        rejected_all = LEV._analog_draw_from_grid(grid, all_invalid, axis, axis)
        @test rejected_all.valid === false

        nonfinite_grid = copy(grid)
        nonfinite_grid[2, 2] = NaN
        rejected_nonfinite = LEV._analog_draw_from_grid(
            nonfinite_grid, validity, axis, axis)
        @test rejected_nonfinite.valid === false
        @test rejected_nonfinite.reason == "nonfinite_required_grid_point"

        partial = LEV._summarize_analog_draws([valid_draw, rejected_interior])
        @test partial["requested_draw_count"] == 2
        @test partial["valid_draw_count"] == 1
        @test partial["invalid_draw_count"] == 1
        @test partial["partial"] === true
        @test partial["evidence_status"] == "partial"
        @test partial["bump_fraction"] == 1.0
        @test partial["median_dynamic_range_decades"] == 5.0
        @test LEV.is_complete_evaluator_evidence(partial) === false

        no_evidence = LEV._summarize_analog_draws([rejected_all, rejected_nonfinite])
        @test no_evidence["evidence_status"] == "no_evidence"
        @test no_evidence["valid_draw_count"] == 0
        @test no_evidence["bump_fraction"] === nothing
        @test no_evidence["median_dynamic_range_decades"] === nothing
        @test no_evidence["median_ratio_corr"] === nothing
        @test no_evidence["median_coactivation_corr"] === nothing
        @test LEV.is_complete_evaluator_evidence(no_evidence) === false
    end

    @testset "contextual evaluator excludes incomplete contexts" begin
        valid_and = (
            valid=true,
            table=LEV.LOGIC_TABLES["AND"],
            margin=1.5,
            reason="valid",
        )
        valid_or = (
            valid=true,
            table=LEV.LOGIC_TABLES["OR"],
            margin=1.0,
            reason="valid",
        )
        invalid = (
            valid=false,
            table=nothing,
            margin=nothing,
            reason="invalid_required_corner",
        )
        complete_and = LEV._summarize_logic_draws([valid_and, valid_and])
        complete_or = LEV._summarize_logic_draws([valid_or, valid_or])
        partial_or = LEV._summarize_logic_draws([valid_or, invalid])
        no_logic_evidence = LEV._summarize_logic_draws([invalid, invalid])

        partial_contexts = LEV._summarize_contextual_results(
            "tC",
            [-2.0, 0.0, 2.0],
            [complete_and, partial_or, no_logic_evidence];
            min_support=0.5,
        )
        @test partial_contexts["requested_context_count"] == 3
        @test partial_contexts["valid_context_count"] == 1
        @test partial_contexts["invalid_context_count"] == 2
        @test partial_contexts["partial"] === true
        @test partial_contexts["evidence_status"] == "partial"
        @test partial_contexts["per_context"][1]["gate"] == "AND"
        @test partial_contexts["per_context"][2]["gate"] === nothing
        @test partial_contexts["per_context"][3]["gate"] === nothing
        @test partial_contexts["distinct_robust_gates"] == ["AND"]
        @test partial_contexts["reprogrammable"] === nothing
        @test LEV.is_complete_evaluator_evidence(
            partial_contexts; unit=:context) === false

        complete_contexts = LEV._summarize_contextual_results(
            "tC", [-2.0, 2.0], [complete_and, complete_or]; min_support=0.5)
        @test complete_contexts["evidence_status"] == "complete"
        @test complete_contexts["partial"] === false
        @test complete_contexts["valid_context_count"] == 2
        @test complete_contexts["reprogrammable"] === true
        @test LEV.is_complete_evaluator_evidence(
            complete_contexts; unit=:context) === true

        calls = Ref(0)
        fake_logic = function (_model, _species, _free; fixed_totals, kwargs...)
            calls[] += 1
            context = only(values(fixed_totals))
            return context < 0 ? complete_and : partial_or
        end
        propagated = LEV.evaluate_contextual(
            nothing,
            nothing,
            nothing;
            input_syms=["tA", "tB"],
            output_sym="C_A_B",
            context_sym="tC",
            context_levels=[-1.0, 1.0],
            K=2,
            logic_evaluator=fake_logic,
        )
        @test calls[] == 2
        @test propagated["evidence_status"] == "partial"
        @test propagated["valid_context_count"] == 1
        @test propagated["per_context"][2]["gate"] === nothing
        @test propagated["reprogrammable"] === nothing
    end

    @testset "real complete scans preserve legacy metric availability" begin
        model, species, free_syms, _ = BiocircuitsExplorerBackend.build_model(
            ["A + B <-> C_A_B"], [1.0])
        logic = LEV.evaluate_logic(
            model,
            species,
            free_syms;
            input_syms=["tA", "tB"],
            output_sym="C_A_B",
            npoints=3,
            K=2,
            kd_lo=0.0,
            kd_hi=0.0,
        )
        @test logic["evidence_status"] == "complete"
        @test logic["requested_draw_count"] == 2
        @test logic["valid_draw_count"] == 2
        @test logic["invalid_draw_count"] == 0
        @test logic["partial"] === false
        @test logic["realized_gate"] isa AbstractString
        @test logic["median_margin_decades"] isa Real
        @test LEV.is_complete_evaluator_evidence(logic) === true

        analog = LEV.evaluate_analog(
            model,
            species,
            free_syms;
            input_syms=["tA", "tB"],
            output_sym="C_A_B",
            npoints=3,
            K=1,
            kd_lo=0.0,
            kd_hi=0.0,
        )
        @test analog["evidence_status"] == "complete"
        @test analog["requested_draw_count"] == 1
        @test analog["valid_draw_count"] == 1
        @test analog["invalid_draw_count"] == 0
        @test analog["partial"] === false
        @test analog["bump_fraction"] isa Real
        @test analog["median_dynamic_range_decades"] isa Real
        @test analog["median_ratio_corr"] isa Real
        @test analog["median_coactivation_corr"] isa Real
        @test LEV.is_complete_evaluator_evidence(analog) === true
    end
end
