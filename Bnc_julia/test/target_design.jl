using LinearAlgebra
using Random

# These fixtures exercise the target-driven engine against finite differences
# and the independent full-species Bnc homotopy solver. They intentionally use
# stepwise Kd values: formation constants alone would miss precursor gradients.
function td_test_target(input_names, output_species; transforms=fill("linear", length(output_species)),
        offsets=zeros(length(output_species)), optimize_offsets=falses(length(output_species)),
        rows=nothing)
    rows === nothing && (rows = [Dict("inputs" => fill(x, length(input_names)),
        "outputs" => fill(0.2, length(output_species)), "weight" => w)
        for (x, w) in zip([0.2, 0.9, 3.0], [1.0, 2.0, 0.7])])
    return Dict{String,Any}(
        "schema_version" => "bne-design-target/v1.0.0", "source" => "data",
        "inputs" => [Dict("name" => name, "min" => 0.1, "max" => 4.0, "scale" => "log") for name in input_names],
        "outputs" => [Dict("name" => "readout_$i", "species" => species,
            "transform" => transforms[i], "offset" => offsets[i],
            "optimize_offset" => optimize_offsets[i]) for (i, species) in enumerate(output_species)],
        "samples" => rows,
    )
end

function td_test_problem(target; chemistry=Dict{String,Any}(), optimization=Dict{String,Any}())
    c = merge(Dict{String,Any}("auxiliary_monomers" => 1, "max_complex_size" => 3,
        "max_reactions" => 20, "allow_homomers" => true), chemistry)
    o = merge(Dict{String,Any}("epochs" => 2, "restarts" => 1, "prune_rounds" => 0), optimization)
    normalized = normalize_design_problem(target, c, o)
    return (network=generate_design_network(normalized.target, normalized.chemistry), normalized...)
end

# Parse the emitted reaction strings rather than the optimizer's parent/path
# arrays, and reconstruct conservation from each species' monomer composition.
function td_test_physical_model(network)
    species = vcat(network.monomers, network.names)
    N = zeros(Int, length(network.rules), length(species))
    for (i, rule) in enumerate(network.rules)
        reactants, product = split(rule, " <-> ")
        for name in split(reactants, " + ")
            N[i, something(findfirst(==(name), species))] += 1
        end
        N[i, something(findfirst(==(product), species))] -= 1
    end
    L = hcat(Matrix{Int}(I, length(network.monomers), length(network.monomers)),
        transpose(Int.(network.compositions)))
    return Bnc(N=N, L=L, x_sym=Symbol.(species),
        q_sym=Symbol.("t" .* network.monomers), K_sym=[Symbol("Kd$i") for i in eachindex(network.rules)])
end

@testset "Target-driven design mathematics and physical selection" begin
    TD = BindingAndCatalysis

    @testset "Search limits report actual evaluated iterations without claiming convergence" begin
        problem = td_test_problem(td_test_target(["X"], ["A"]);
            optimization=Dict("epochs" => 3, "restarts" => 2, "prune_rounds" => 1, "max_rmse" => 0.0))
        result = design_binding_network(problem.target, problem.chemistry, problem.optimization)
        @test !result["target_met"]
        stop = result["termination"]
        @test stop["reason"] == "search_budget_exhausted"
        @test stop["epochs_per_fit"] == 3
        @test stop["initializations"] == 2
        @test stop["pruning_round_limit"] == 1
        @test length(stop["fit_stops"]) == 4
        @test stop["iterations"] == 12
        @test all(fit["reason"] == "iteration_limit" && fit["iterations"] == 3 for fit in stop["fit_stops"])
        @test all(0 <= fit["best_step"] <= fit["iterations"] for fit in stop["fit_stops"])

        # Interrupt an evaluation with the engine's numerical-failure type.
        # The last valid state must be reported, not a completed iteration budget.
        inject = Ref(-1)
        checkpoint = function ()
            inject[] < 0 && return false
            inject[] += 1
            inject[] == 2 && throw(TD.DesignEquilibriumError("test invalid update"))
            return false
        end
        fit = TD._td_fit(problem.network, problem.target, problem.optimization,
            TD._td_initial(problem.network, problem.target, problem.optimization, MersenneTwister(1));
            phase="fit", restart=1, history=Any[], cancelled=checkpoint,
            callback=(state, evaluation) -> (state["step"] == 0 && (inject[] = 0)))
        @test fit.stop_reason == "invalid_equilibrium_update"
        @test fit.iterations == 0
        @test fit.step == 0
        @test isfinite(fit.evaluation.rmse)
    end

    @testset "Every implicit gradient agrees with central differences" begin
        target = td_test_target(["X"], ["A_X2", "A"];
            transforms=["log10", "linear"], offsets=[0.15, -0.2], optimize_offsets=[true, false])
        problem = td_test_problem(target)
        (; network, target, optimization) = problem
        layout = TD._td_layout(network, target, optimization)
        theta = vcat(collect(range(-0.6, 0.8; length=layout.r)), [0.23, 0.15])
        evaluation = TD._td_evaluate(network, target, optimization, theta)
        @test layout.count == layout.r + 2
        @test layout.total_indices == network.auxiliary_indices
        @test layout.offset_indices == [1]
        chained = something(findfirst(==("A_X2"), network.names))
        @test sum(network.path[chained, :]) == 2
        @test count(!iszero, evaluation.gradient[1:layout.r]) == layout.r
        for i in eachindex(theta)
            plus, minus = copy(theta), copy(theta)
            plus[i] += 2e-5; minus[i] -= 2e-5
            finite_difference = (TD._td_evaluate(network, target, optimization, plus; gradients=false).loss -
                TD._td_evaluate(network, target, optimization, minus; gradients=false).loss) / 4e-5
            @test isapprox(evaluation.gradient[i], finite_difference; rtol=2e-5, atol=2e-8)
        end
        @test evaluation.offsets == [0.15, -0.2]
        @test evaluation.totals[network.auxiliary_indices] ≈ [10.0^0.23]
        fixed = merge(optimization, Dict("optimize_totals" => false))
        @test isempty(TD._td_layout(network, target, fixed).total_indices)
        @test TD._td_layout(network, target, fixed).count == layout.r + 1
        warm = TD._td_evaluate(network, target, optimization, theta; warm=evaluation.starts)
        @test warm.predictions ≈ evaluation.predictions
        @test warm.gradient ≈ evaluation.gradient
        @test evaluation.maxmass < 1e-8
        @test evaluation.maxaction < 1e-12

        # Opting a second offset in adds exactly its own residual derivative.
        both = deepcopy(target)
        both["outputs"][2]["optimize_offset"] = true
        both_theta = vcat(theta, -0.2)
        both_eval = TD._td_evaluate(network, both, optimization, both_theta)
        plus, minus = copy(both_theta), copy(both_theta)
        plus[end] += 1e-5; minus[end] -= 1e-5
        slope = (TD._td_evaluate(network, both, optimization, plus).loss -
            TD._td_evaluate(network, both, optimization, minus).loss) / 2e-5
        @test both_eval.predictions ≈ evaluation.predictions
        @test isapprox(both_eval.gradient[end], slope; rtol=1e-7, atol=1e-9)
    end

    @testset "Generated step reactions replay through the native physical model" begin
        problem = td_test_problem(td_test_target(["X"], ["A_X2", "A"];
            transforms=["log10", "linear"], offsets=[0.35, -0.1], optimize_offsets=[true, false]))
        (; network, target, optimization) = problem
        layout = TD._td_layout(network, target, optimization)
        theta = vcat(collect(range(-0.7, 0.9; length=layout.r)), [0.12, 0.35])
        evaluation = TD._td_evaluate(network, target, optimization, theta)
        # Axis scale controls editor sampling only. Supplied samples always
        # represent physical concentrations, independent of that metadata.
        linear_target = deepcopy(target)
        linear_target["inputs"][1]["scale"] = "linear"
        linear_problem = td_test_problem(linear_target)
        linear_evaluation = TD._td_evaluate(linear_problem.network, linear_problem.target,
            linear_problem.optimization, theta)
        @test linear_problem.target["inputs"][1]["scale"] == "linear"
        @test linear_problem.target["samples"] == target["samples"]
        @test linear_evaluation.predictions == evaluation.predictions
        @test linear_evaluation.gradient == evaluation.gradient
        model = td_test_physical_model(network)
        @test iszero(model.N * transpose(model.L))
        @test model.r == layout.r
        for (b, sample) in enumerate(target["samples"])
            totals = copy(evaluation.totals)
            totals[network.input_indices] .= sample["inputs"]
            status = Ref(:not_started)
            concentrations = qK2x(model, vcat(log10.(totals), theta[1:layout.r]);
                input_logspace=true, status=status, reltol=1e-11, abstol=1e-12)
            linear_concentrations = qK2x(model, vcat(totals, exp10.(theta[1:layout.r]));
                input_logspace=false, reltol=1e-11, abstol=1e-12)
            @test status[] == :success
            @test isapprox(linear_concentrations, concentrations; rtol=2e-8, atol=2e-10)
            @test isapprox(model.L * concentrations, totals; rtol=2e-8, atol=1e-10)
            @test isapprox(model.N * log10.(concentrations), theta[1:layout.r]; atol=2e-8)
            for (o, output) in enumerate(target["outputs"])
                value = concentrations[layout.output_indices[o]]
                readout = (output["transform"] == "log10" ? log10(value) : value) + evaluation.offsets[o]
                @test isapprox(readout, evaluation.predictions[b, o]; rtol=2e-8, atol=2e-9)
                linear_value = linear_concentrations[layout.output_indices[o]]
                linear_readout = (output["transform"] == "log10" ? log10(linear_value) : linear_value) + linear_evaluation.offsets[o]
                @test isapprox(linear_readout, linear_evaluation.predictions[b, o]; rtol=2e-8, atol=2e-9)
            end
        end
    end

    @testset "Input and output dimensions are independent" begin
        for (input_names, output_species, expected) in [
                (["X"], ["A", "A_X"], (3, 2)),
                (["X", "Y"], ["A_X_Y"], (3, 1)),
                (["X", "Y", "Z"], ["X_Y", "X_Z", "Y_Z"], (3, 3))]
            problem = td_test_problem(td_test_target(input_names, output_species);
                chemistry=Dict("max_reactions" => 12))
            (; network, target, optimization) = problem
            theta = TD._td_initial(network, target, optimization, MersenneTwister(12))
            evaluation = TD._td_evaluate(network, target, optimization, theta)
            @test size(evaluation.predictions) == expected
            @test length(network.input_indices) == length(input_names)
            @test all(isfinite, evaluation.predictions)
            @test evaluation.maxmass < 1e-8
        end
    end

    @testset "Chemical closure and reaction caps are enforced" begin
        problem = td_test_problem(td_test_target(["X"], ["A_X2"]);
            chemistry=Dict("max_reactions" => 2))
        network = problem.network
        @test network.rules == ["A + X <-> A_X", "A_X + X <-> A_X2"]
        @test length(network.rules) == 2
        @test network.path == [1.0 0.0; 1.0 1.0]
        @test network.available_count > length(network.rules)
        @test_throws ArgumentError td_test_problem(td_test_target(["X"], ["A_X2"]);
            chemistry=Dict("max_reactions" => 1))
        @test_throws ArgumentError td_test_problem(td_test_target(["X"], ["A_X2"]);
            chemistry=Dict("allow_homomers" => false))
        heteromeric = td_test_problem(td_test_target(["X", "Y"], ["A_X_Y"]);
            chemistry=Dict("allow_homomers" => false, "max_reactions" => 4)).network
        @test maximum(heteromeric.compositions) == 1
        @test all(j -> 0 <= heteromeric.parents[j] < j, eachindex(heteromeric.rules))
        @test_throws ArgumentError TD._td_network(["A", "X"], ["X"], [[1, 2]])
        @test_throws ArgumentError td_test_problem(td_test_target(["X"], ["missing_species"]))
    end

    @testset "Pruning removes unused auxiliaries and maps physical parameters" begin
        target = td_test_problem(td_test_target(["X", "Y"], ["A_X", "A"];
            optimize_offsets=[true, false])).target
        network = TD._td_network(["A", "B", "X", "Y"], ["X", "Y"], [[1, 0, 1, 0], [0, 2, 0, 0]])
        pruned = TD._td_prune(network, target, [0.3, 0.001], 1.0)
        @test pruned !== nothing
        @test pruned.monomers == ["A", "X", "Y"]
        @test pruned.rules == ["A + X <-> A_X"]
        @test pruned.monomers[pruned.input_indices] == ["X", "Y"]
        @test all(row["species"] in vcat(pruned.monomers, pruned.names) for row in target["outputs"])
        @test TD._td_prune(pruned, target, [0.3], 1.0) === nothing
        optimization = Dict("optimize_totals" => true)
        mapped = TD._td_initial(pruned, target, optimization, MersenneTwister(2);
            previous=(network, [0.12, 0.81, -0.2, 0.6, 0.15]))
        @test mapped == [0.12, -0.2, 0.15]
        @test size(pruned.compositions) == (1, 3)
    end

    @testset "Optional grammar filters include precursor descendants" begin
        target = td_test_target(["X"], ["A"])
        limited = td_test_problem(target; chemistry=Dict("max_copies" => Dict("X" => 1))).network
        xcolumn = something(findfirst(==("X"), limited.monomers))
        @test maximum(limited.compositions[:, xcolumn]) == 1
        @test "A_X" in limited.names
        @test !("A_X2" in limited.names)
        forbidden = td_test_problem(target; chemistry=Dict("forbidden_complexes" => ["A_X"])).network
        @test !("A_X" in forbidden.names)
        @test !("A_X2" in forbidden.names)
        @test "A2_X" in forbidden.names  # Its legal precursor is A2, not A_X.
        gated = td_test_problem(target; chemistry=Dict("binding_gates" => [
            Dict("monomer" => "X", "requires" => Dict("A" => 1))])).network
        @test !("X2" in gated.names)
        @test !("X3" in gated.names)
        @test "A_X2" in gated.names
        exempt = td_test_problem(target; chemistry=Dict("auxiliary_monomers" => 2,
            "max_reactions" => 40, "binding_gates" => [
                Dict("monomer" => "X", "requires" => Dict("B" => 1), "unless_core_count_at_least" => 2)])).network
        @test !("A_X" in exempt.names)
        @test "A2_X" in exempt.names
        @test "B_X" in exempt.names
        @test_throws ArgumentError td_test_problem(target; chemistry=Dict("max_copies" => Dict("unknown" => 1)))
        @test_throws ArgumentError td_test_problem(target; chemistry=Dict("forbidden_complexes" => ["unknown"]))
        @test_throws ArgumentError td_test_problem(target; chemistry=Dict("binding_gates" => [
            Dict("monomer" => "X", "requires" => Dict("unknown" => 1))]))
        @test_throws ArgumentError td_test_problem(td_test_target(["X"], ["A_X2"]);
            chemistry=Dict("forbidden_complexes" => ["A_X"]))
    end

    @testset "A failed prune and refit never replaces its valid parent" begin
        problem = td_test_problem(td_test_target(["X"], ["A_X"]);
            chemistry=Dict("max_complex_size" => 2),
            optimization=Dict("epochs" => 1, "prune_rounds" => 1, "prune_tolerance" => 0.0,
                "max_rmse" => 1e-10, "optimize_totals" => false, "seed" => 19))
        (; network, target, chemistry, optimization) = problem
        initial = TD._td_initial(network, target, optimization, MersenneTwister(19))
        truth = TD._td_evaluate(network, target, optimization, initial).predictions
        for (i, sample) in enumerate(target["samples"])
            sample["outputs"] = collect(truth[i, :])
        end
        result = design_binding_network(target, chemistry, optimization)
        @test result["target_met"]
        @test length(result["pruning_history"]) == 1
        rejected = only(result["pruning_history"])
        @test !rejected["accepted"]
        @test rejected["after"] < rejected["before"]
        @test rejected["rmse"] > rejected["error_ceiling"]
        @test result["final_reaction_count"] == result["initial_reaction_count"]
        @test result["selected_network"]["kd"] ≈ exp10.(initial)
        @test result["selected_network"]["rmse"] < 1e-12
        @test any(state["phase"] == "prune_refit" for state in result["optimization_history"])
    end

    @testset "Accepted pruning returns the refitted physical network" begin
        result = design_binding_network(td_test_target(["X"], ["A_X"]),
            Dict("auxiliary_monomers" => 2, "max_complex_size" => 2, "allow_homomers" => false),
            Dict("epochs" => 2, "restarts" => 1, "prune_rounds" => 2, "prune_fraction" => 1.0,
                "prune_tolerance" => 1.0, "max_rmse" => 1.0, "optimize_totals" => false))
        @test result["target_met"]
        @test result["initial_reaction_count"] == 3
        @test result["final_reaction_count"] == 1
        @test only(result["pruning_history"])["accepted"]
        selected = result["selected_network"]
        @test selected["rules"] == ["A + X <-> A_X"]
        @test selected["monomers"] == ["A", "X"]
        @test Set(keys(selected["totals"])) == Set(["A"])
        @test selected["prediction_basis"] == "selected_network"
        kd, a = only(selected["kd"]), selected["totals"]["A"]
        for (i, sample) in enumerate(result["target"]["samples"])
            x = only(sample["inputs"])
            # Stable quadratic binding solution independent of either solver.
            complex = 2a*x / (a + x + kd + sqrt((a + x + kd)^2 - 4a*x))
            @test only(selected["predictions"][i]) ≈ complex atol=1e-10
        end
    end

    @testset "Unmet targets and held-out failures remain explicit" begin
        impossible = td_test_target(["X"], ["X"])
        for sample in impossible["samples"]; sample["outputs"] = [-1.0]; end
        chemistry = Dict("auxiliary_monomers" => 0, "max_complex_size" => 2, "allow_homomers" => false)
        options = Dict("epochs" => 1, "restarts" => 1, "prune_rounds" => 2, "max_rmse" => 0.01)
        result = design_binding_network(impossible, chemistry, options)
        @test !result["target_met"]
        @test result["selected_network"]["rmse"] > options["max_rmse"]
        @test result["selected_network"]["rules"] == String[]
        @test any(occursin("not met", warning) for warning in result["warnings"])
        @test result["algorithm"]["global_minimality_claimed"] == false

        identity = td_test_target(["X"], ["X"])
        for sample in identity["samples"]; sample["outputs"] = copy(sample["inputs"]); end
        identity["validation_samples"] = [Dict("inputs" => [2.0], "outputs" => [-1.0], "weight" => 1.0)]
        failed_validation = design_binding_network(identity, chemistry, options)
        @test failed_validation["selected_network"]["rmse"] < 1e-12
        @test failed_validation["validation"]["rmse"] ≈ 3.0
        @test !failed_validation["target_met"]
        @test failed_validation["selected_network"]["physical_audit"]["cold_replay"]
        @test failed_validation["selected_network"]["physical_audit"]["validation_samples"] == 1
        @test failed_validation["selected_network"]["physical_audit"]["training_samples"] == 3
        @test_throws InterruptException design_binding_network(identity, chemistry, options; cancelled=()->true)

        # A good output cannot dilute a failed output below the user tolerance.
        mixed = td_test_target(["X"], ["X", "X"])
        for sample in mixed["samples"]
            x = only(sample["inputs"])
            sample["outputs"] = [x, x + 0.06]
        end
        mixed_result = design_binding_network(mixed, chemistry, merge(options, Dict("max_rmse" => 0.05)))
        @test mixed_result["selected_network"]["rmse"] < 0.05
        @test mixed_result["selected_network"]["per_output_rmse"] ≈ [0.0, 0.06] atol=1e-12
        @test !mixed_result["target_met"]

        # Held-out values may affect selection, but never the fit derivative.
        fit_problem = td_test_problem(td_test_target(["X"], ["A_X"]))
        theta = TD._td_initial(fit_problem.network, fit_problem.target, fit_problem.optimization, MersenneTwister(3))
        baseline = TD._td_evaluate(fit_problem.network, fit_problem.target, fit_problem.optimization, theta)
        with_holdout = deepcopy(fit_problem.target)
        with_holdout["validation_samples"] = [Dict("inputs" => [2.0], "outputs" => [-100.0], "weight" => 1e6)]
        same_fit = TD._td_evaluate(fit_problem.network, with_holdout, fit_problem.optimization, theta)
        @test same_fit.gradient == baseline.gradient
        @test same_fit.loss == baseline.loss
    end
end
