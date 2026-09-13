module ArchitectureDiscoveryAPITests

using Test
using HTTP
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

@testset "Architecture discovery HTTP endpoint" begin
    payload = Dict(
        "reactions" => ["2A <-> AA", "A + B <-> AB", "2B <-> BB"],
        "output_exprs" => ["2*AA + AB", "AB + 2*BB"],
        "samples" => [
            Dict("totals" => Dict("tA" => 0.2, "tB" => 0.4), "target" => [0.01, 0.01]),
            Dict("totals" => Dict("tA" => 1.0, "tB" => 1.0), "target" => [0.2, 0.2]),
        ],
        "epochs" => 2,
        "debias_epochs" => 0,
    )
    response = Backend.router(HTTP.Request(
        "POST",
        "/api/v1/discover_architecture",
        ["Content-Type" => "application/json"],
        JSON3.write(payload),
    ))
    @test response.status == 200
    result = JSON3.read(response.body)
    @test length(result["reaction_fit"]) == 3
    @test length(result["predictions"]) == 2
    @test all(length(row) == 2 for row in result["predictions"])
    @test result["q_sym"] == ["tA", "tB"]
    @test result["output_exprs"] == ["2*AA + AB", "AB + 2*BB"]
    @test result["prediction_basis"] == "candidate_library_with_weak_inactive_reactions"
    @test result["optimality"] == "local_gradient_fit"
    @test result["identifiability"] == "not_assessed"
    @test result["artifact"]["kind"] == "discover_architecture"
    selected = result["selected_network"]
    @test selected["status"] == "ok"
    @test selected["prediction_basis"] == "selected_network"
    @test selected["evidence_tier"] == "sampled_equilibrium_replay"
    @test selected["rules"] == result["rules"]
    @test selected["kd"] == result["kd"]
    selected_ir = Backend.parse_network_ir(Backend._materialize(selected["network_ir"]))
    @test Backend.network_ir_hash(selected_ir) == selected["network_ir_hash"]

    simulated_payload = deepcopy(payload)
    for sample in simulated_payload["samples"]
        delete!(sample, "target")
    end
    simulated_payload["simulation_kd"] = [1e6, 0.8, 1e6]
    simulated_payload["simulation_noise"] = 0.1
    simulated_response = Backend.router(HTTP.Request(
        "POST",
        "/api/v1/discover_architecture",
        ["Content-Type" => "application/json"],
        JSON3.write(simulated_payload),
    ))
    @test simulated_response.status == 200
    simulated = JSON3.read(simulated_response.body)
    @test simulated["simulation"]["kd"] == [1e6, 0.8, 1e6]
    @test simulated["simulation"]["noise_log_std"] == 0.1
    @test all(value > 0 for row in simulated["targets"] for value in row)

    missing_total = deepcopy(payload)
    delete!(missing_total["samples"][1]["totals"], "tB")
    rejected = Backend.router(HTTP.Request(
        "POST",
        "/api/v1/discover_architecture",
        ["Content-Type" => "application/json"],
        JSON3.write(missing_total),
    ))
    @test rejected.status == 400
    @test occursin("missing tB", String(rejected.body))

    @test Backend.router(HTTP.Request(
        "POST", "/api/discover_architecture", [], JSON3.write(payload))).status == 404

    @testset "Bounded synchronous discovery" begin
        for (field, value) in (
            ("epochs", Backend.MAX_ARCHITECTURE_EPOCHS + 1),
            ("debias_epochs", Backend.MAX_ARCHITECTURE_DEBIAS_EPOCHS + 1),
            ("samples", fill(first(payload["samples"]), Backend.MAX_ARCHITECTURE_SAMPLES + 1)),
            ("output_exprs", fill("AB", Backend.MAX_SYNC_SCAN_OUTPUTS + 1)),
            ("output_exprs", [repeat("A", Backend.MAX_SYNC_EXPRESSION_BYTES + 1)]),
            ("reactions", fill("A + B <-> AB", Backend.MAX_SYNC_REACTIONS + 1)),
        )
            over_budget = merge(payload, Dict(field => value))
            response = Backend.router(HTTP.Request(
                "POST", "/api/v1/discover_architecture", [], JSON3.write(over_budget)))
            @test response.status == 422
            @test JSON3.read(response.body)["code"] == "sync_budget_exceeded"
        end
        cost_limited = merge(payload, Dict(
            "samples" => fill(first(payload["samples"]), 256),
            "epochs" => 2000, "debias_epochs" => 500,
        ))
        cost_response = Backend.router(HTTP.Request(
            "POST", "/api/v1/discover_architecture", [], JSON3.write(cost_limited)))
        @test cost_response.status == 422
        @test occursin("work budget", String(cost_response.body))
        @test :handle_discover_architecture in Backend.SYNC_HEAVY_HANDLER_NAMES
    end
end

@testset "Selected network uses a fresh pruned equilibrium replay" begin
    rules = ["2A <-> AA", "A + B <-> AB", "2B <-> BB"]
    model, _, _, _ = Backend.build_model(rules, ones(3))
    outputs = reduce(vcat, [transpose(Backend.parse_linear_combination(model, expression))
                           for expression in ["2*AA + AB", "AB + 2*BB", "AA"]])
    selected = Backend._ad_selected_network(
        [rules[2]], [1.0], model, [1.0 1.0], zeros(1, 3), outputs)
    # A + B <-> AB, tA=tB=Kd=1: AB=(3-sqrt(5))/2. Deleted
    # homodimers must contribute exactly zero, not a large finite Kd.
    expected = (3 - sqrt(5)) / 2
    @test selected["status"] == "ok"
    @test selected["predictions"][1] ≈ [expected, expected, 0.0]
    @test selected["fit_loss"] ≈ expected^2 / 3
    @test Set(selected["removed_species"]) == Set(["AA", "BB"])
    @test selected["output_exprs"] == ["1.0*AB", "1.0*AB", "0*A"]
    pruned_model, _, _, _ = Backend.build_model(selected["rules"], selected["kd"])
    for (expression, coefficients) in zip(selected["output_exprs"], selected["output_coefficients"])
        @test Backend.parse_linear_combination(pruned_model, expression) == coefficients
    end
    @test Backend._ad_decimal_coefficient(1e-10) == "0.00000000010"

    # Removing the first reaction makes AB an independent conserved input;
    # the original tA/tB observations cannot provide its total.
    chain_rules = ["A + B <-> AB", "AB + B <-> AB2"]
    chain, _, _, _ = Backend.build_model(chain_rules, ones(2))
    chain_outputs = reshape(Backend.parse_linear_combination(chain, "AB2"), 1, :)
    invalid = Backend._ad_selected_network(
        [chain_rules[2]], [1.0], chain, [1.0 1.0], zeros(1, 1), chain_outputs)
    @test invalid["status"] == "invalid"
    @test occursin("tAB", invalid["reason"])
    @test !haskey(invalid, "network_ir")
    @test !haskey(invalid, "predictions")
    independent_rules = ["A + B <-> AB", "C + D <-> CD"]
    independent, _, _, _ = Backend.build_model(independent_rules, ones(2))
    free_output = reshape(Backend.parse_linear_combination(independent, "A + CD"), 1, :)
    missing_free = Backend._ad_selected_network(
        [independent_rules[2]], [1.0], independent, [1.0 1.0 1.0 1.0], zeros(1, 1), free_output)
    @test missing_free["status"] == "invalid"
    @test occursin("observed free species", missing_free["reason"])
    @test !haskey(missing_free, "predictions")
    empty_result = Backend._ad_selected_network(
        String[], Float64[], chain, [1.0 1.0], zeros(1, 1), chain_outputs)
    @test empty_result["status"] == "no_active_reactions"
    @test !haskey(empty_result, "network_ir")
end

end # module
