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
end

end # module
