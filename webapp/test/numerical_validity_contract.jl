using Test
using HTTP
using JSON3

@testset "Placement pass requires complete numerical verification" begin
    backend = BiocircuitsExplorerBackend
    @test backend._placer_verification_complete(Bool[true, true])
    @test !backend._placer_verification_complete(Bool[true, false, true])
    @test !backend._placer_verification_complete(Bool[])
    @test !backend._placer_verification_complete(Any[true, 1])

    @test backend._placer_verified_pass(true, Bool[true, true])
    @test !backend._placer_verified_pass(true, Bool[true, false])
    @test !backend._placer_verified_pass(false, Bool[true, true])

    complete_curve = Dict("valid" => Bool[true, true], "partial" => false)
    partial_curve = Dict("valid" => Bool[true, false], "partial" => true)
    @test backend._placer_curve_verification(true, complete_curve).pass
    @test !backend._placer_curve_verification(true, partial_curve).pass
    @test backend._placer_curve_verification(true, partial_curve).partial
    @test backend._placer_curve_verification(true, partial_curve).reason == "partial_dose_response"
    @test !backend._placer_curve_verification(false, complete_curve).pass
    @test backend._placer_curve_verification(false, complete_curve).reason == "local_reaction_order_mismatch"
end

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

    placement_response = post("/api/v1/place_parameters", Dict(
        "rules" => Any["A + B <-> AB"],
        "input_sym" => "tA",
        "output_sym" => "AB",
        "target_ro" => 1.0,
        "kd_bounds" => Any[-3.0, 3.0],
    ))
    @test placement_response.status == 200
    placement = JSON3.read(String(placement_response.body))
    @test placement["pass"] === true
    @test placement["local_reaction_order_pass"] === true
    @test placement["verification_partial"] === false
    @test placement["verification_reason"] == "complete"
    @test length(placement["verification_validity"]) ==
        length(placement["dose_response_curve"]["param_values"])
    @test all(==(true), placement["verification_validity"])

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
