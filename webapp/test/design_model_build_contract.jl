using Test
using BiocircuitsExplorerBackend
using BindingAndCatalysis
using HTTP
using JSON3

@testset "Designed equilibrium models load without enumerating regimes" begin
    backend = BiocircuitsExplorerBackend
    post_model(body) = router(HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"], JSON3.write(body)))
    monomers = ["A", "B", "X"]
    rules = ["$(monomers[i]) + $(monomers[j]) <-> C_$(monomers[i])_$(monomers[j])"
             for i in eachindex(monomers) for j in i:length(monomers)]
    network = network_ir_from_legacy(rules, collect(range(0.2, 2.0; length=6)))
    request = Dict("network" => network_ir_to_dict(network), "build_mode" => "design_equilibrium")
    backend.ModelCache._clear_all!()
    response = post_model(request)
    @test response.status == 200
    result = JSON3.read(String(response.body))
    @test result["build_mode"] == "design_equilibrium"
    @test result["r"] == 6
    @test result["d"] == 3
    @test result["kd"] == collect(range(0.2, 2.0; length=6))
    @test result["q_sym"] == ["tA", "tB", "tX"]
    bundle = backend.ModelCache.get_model(String(result["network_ir_hash"]))
    @test bundle["model"].BindRegimes === nothing
    @test bundle["model"].vertices_graph === nothing
    @test iszero(bundle["model"].N * transpose(bundle["model"].L))
    @test post_model(request).status == 200
    @test post_model(Dict("network" => network_ir_to_dict(network))).status == 422
    @test_throws backend.SyncBudgetExceeded backend.build_model_bundle(network)
    for endpoint in ("find_vertices", "build_graph", "parameter_scan_1d")
        # A caller cannot use a permitted equilibrium load or its mode flag to
        # bypass the budget on a subsequent expensive operation.
        response = router(HTTP.Request("POST", "/api/v1/$endpoint",
            ["Content-Type" => "application/json"], JSON3.write(Dict(
                "network_ir_hash" => result["network_ir_hash"],
                "build_mode" => "design_equilibrium"))))
        @test response.status == 422
    end
    @test post_model(merge(request, Dict("build_mode" => "unchecked"))).status == 400
    @test post_model(merge(request, Dict("build_mode" => true))).status == 400

    # The separate build mode also works when even the background regime
    # candidate product would be too large. No such product is materialized.
    many = ["A", "B", "C", "D", "E", "F"]
    many_rules = ["$(many[i]) + $(many[j]) <-> C_$(many[i])_$(many[j])"
                  for i in eachindex(many) for j in i:length(many)]
    larger = network_ir_from_legacy(many_rules, ones(length(many_rules)))
    large_bundle = backend.build_model_bundle(larger; build_mode=:design_equilibrium)
    @test large_bundle["model"].r == 21
    @test large_bundle["model"].BindRegimes === nothing
    @test_throws backend.ModelCandidateBoundExceeded backend.model_candidate_bound(
        large_bundle["model"]; maximum=backend.MAX_JOB_REGIME_CANDIDATES, label="test")
end

@testset "Designed model validation and declared monomer preservation" begin
    backend = BiocircuitsExplorerBackend
    base = network_ir_to_dict(network_ir_from_legacy(["A + B <-> AB"], [0.7]))
    push!(base["species"], Dict("name" => "X", "role" => "free"))
    network = parse_network_ir(base)
    backend.ModelCache._clear_all!()
    ordinary = backend.build_model_bundle(network)
    @test string.(ordinary["free_syms"]) == ["A", "B", "X"]
    @test ordinary["model"].d == 3
    @test ordinary["model"].n == 4
    @test Matrix(ordinary["model"].L)[3, :] == [0, 0, 1, 0]
    @test backend.build_model_bundle(network; build_mode=:design_equilibrium) === ordinary
    backend.ModelCache._clear_all!()
    designed = backend.build_model_bundle(network; build_mode=:design_equilibrium)
    @test Matrix(designed["model"].N) == Matrix(ordinary["model"].N)
    @test Matrix(designed["model"].L) == Matrix(ordinary["model"].L)
    @test backend.build_model_bundle(network) === designed
    status = Ref(:uninitialized)
    concentrations = qK2x(designed["model"], [1.0, 2.0, 3.0, 0.7]; status=status)
    @test status[] == :success
    @test concentrations[3] ≈ 3.0
    expected_dimer = (3.7 - sqrt(3.7^2 - 8.0)) / 2
    @test concentrations[4] ≈ expected_dimer rtol=1e-7

    bad_rules = (
        ["A + B <-> AB", "A + B <-> AB"],
        ["A + C <-> B", "A + B <-> C"],
        ["A <-> B"],
        ["3 A <-> AAA"],
        ["A + B <-> 2 AB"],
        ["A$i + B$i <-> AB$i" for i in 1:4],
        [i == 1 ? "A + A <-> A2" : "A + A$i <-> A$(i + 1)" for i in 1:257],
        [i == 1 ? "A + A <-> A2" : "A$i + A$i <-> A$(i + 1)" for i in 1:7],
    )
    for rules in bad_rules
        invalid = network_ir_from_legacy(rules, ones(length(rules)))
        @test_throws ArgumentError backend.build_model_bundle(invalid; build_mode=:design_equilibrium)
    end
    extra_bound = deepcopy(base)
    extra_bound["species"][end]["role"] = "bound"
    @test_throws ArgumentError backend.build_model_bundle(parse_network_ir(extra_bound))
    too_many = deepcopy(base)
    append!(too_many["species"], [Dict("name" => "Z$i", "role" => "free") for i in 1:25])
    @test_throws backend.SyncBudgetExceeded backend.build_model_bundle(parse_network_ir(too_many))
    @test_throws ArgumentError backend.build_model_bundle(parse_network_ir(too_many);
                                                         build_mode=:design_equilibrium)
end
