using Test
using HTTP
using JSON3

@testset "Numerical endpoints expose solver validity without corrupting plots" begin
    network = Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
    )
    post(path, body) = router(HTTP.Request(
        "POST", path, ["Content-Type" => "application/json"], JSON3.write(body)))

    rop_response = post("/api/v1/rop_cloud", Dict(
        "network" => network,
        "sampling_mode" => "qk",
        "n_samples" => 100,
        "span" => 2,
    ))
    @test rop_response.status == 200
    rop = JSON3.read(String(rop_response.body))
    @test length(rop["reaction_orders"]) == 100
    @test length(rop["fret_values"]) == 100
    @test length(rop["valid"]) == 100
    @test rop["sample_count"] == 100
    @test rop["valid_sample_count"] == count(==(true), rop["valid"])
    @test rop["partial"] == !all(==(true), rop["valid"])

    fret_response = post("/api/v1/fret_heatmap", Dict(
        "network" => network,
        "n_grid" => 20,
        "logq_min" => -1.0,
        "logq_max" => 1.0,
    ))
    @test fret_response.status == 200
    fret = JSON3.read(String(fret_response.body))
    @test length(fret["fret"]) == 20
    @test all(row -> length(row) == 20, fret["fret"])
    @test length(fret["validity_grid"]) == 20
    @test all(row -> length(row) == 20, fret["validity_grid"])
    validity = collect(Iterators.flatten(fret["validity_grid"]))
    @test fret["partial"] == !all(==(true), validity)

    valid_candidate = Dict(
        "refinement_status" => "ok",
        "best_trial" => Dict("partial" => false),
    )
    invalid_candidate = Dict(
        "refinement_status" => "partial_solver_failure",
        "best_trial" => Dict("partial" => true),
    )
    @test BiocircuitsExplorerBackend._refinement_candidate_is_valid(valid_candidate)
    @test !BiocircuitsExplorerBackend._refinement_candidate_is_valid(invalid_candidate)

    query_result = Dict("results" => Any[Dict("network_id" => "query-first")])
    annotated = Dict(
        "enabled" => true,
        "reranked" => false,
        "best_candidate" => Dict("network_id" => "refined"),
    )
    reranked = merge(annotated, Dict("reranked" => true))
    invalid_refinement = merge(reranked, Dict("best_candidate" => nothing))
    @test BiocircuitsExplorerBackend._select_inverse_best_design(
        annotated, query_result)["selection_source"] == "query"
    @test BiocircuitsExplorerBackend._select_inverse_best_design(
        reranked, query_result)["selection_source"] == "refinement"
    @test BiocircuitsExplorerBackend._select_inverse_best_design(
        invalid_refinement, query_result)["selection_source"] == "query"
end
