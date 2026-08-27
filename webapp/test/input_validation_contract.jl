using Test
using HTTP
using JSON3

@testset "NetworkIR accepts only finite, non-boolean numbers" begin
    @test_throws IRValidationError parse_network_ir(Dict(
        "reactions" => Any[true],
        "kd" => Any[1.0],
    ))
    @test_throws IRValidationError parse_network_ir(Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
        "label" => true,
    ))
    for invalid_kd in (true, NaN, Inf, -Inf)
        @test_throws IRValidationError parse_network_ir(Dict(
            "reactions" => Any["A + B <-> AB"],
            "kd" => Any[invalid_kd],
        ))
    end

    for invalid_kd in (true, NaN, Inf, -Inf)
        @test_throws IRValidationError parse_network_ir(Dict(
            "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
            "reactions" => Any[
                Dict("formula" => "A + B <-> AB", "kd" => invalid_kd),
            ],
        ))
    end

    @test_throws IRValidationError parse_network_ir(Dict(
        "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
        "species" => Any[Dict("name" => "A", "initial_total" => true)],
        "reactions" => Any[Dict("formula" => "A <-> B", "kd" => 1.0)],
    ))
    @test_throws IRValidationError parse_network_ir(Dict(
        "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
        "species" => Any[Dict("name" => "A", "role" => true)],
        "reactions" => Any[Dict("formula" => "A <-> B", "kd" => 1.0)],
    ))
    @test_throws IRValidationError parse_network_ir(Dict(
        "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
        "reactions" => Any[
            Dict("formula" => "A <-> B", "kd" => 1.0, "kind" => Dict()),
        ],
    ))
    @test_throws IRValidationError network_ir_from_legacy(
        ["A + B <-> AB"],
        [Inf],
    )
    @test_throws ErrorException BiocircuitsExplorerBackend.build_model(
        ["A + B <-> AB"],
        [Inf],
    )
    model, _, _, _ = BiocircuitsExplorerBackend.build_model(
        ["A + B <-> AB"],
        [1.0],
    )
    @test_throws ErrorException BiocircuitsExplorerBackend.default_log_qK(
        model,
        Real[true],
    )

    bool_build = router(HTTP.Request(
        "POST",
        "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "reactions" => Any["A + B <-> AB"],
            "kd" => Any[true],
        )),
    ))
    @test bool_build.status == 400
    @test occursin("finite number", String(bool_build.body))

    bad_rule_build = router(HTTP.Request(
        "POST",
        "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("reactions" => Any[true], "kd" => Any[1.0])),
    ))
    @test bad_rule_build.status == 400

    @test_throws IRValidationError parse_design_spec(Dict(
        "ir_schema_version" => DESIGN_SPEC_SCHEMA_VERSION,
        "goal" => Dict(),
        "constraints" => Dict("max_reactions" => true),
    ))
end

@testset "Parameter scans reject non-string output expressions" begin
    network = Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
    )
    post(path, body) = router(HTTP.Request(
        "POST",
        path,
        ["Content-Type" => "application/json"],
        JSON3.write(body),
    ))

    object_1d = post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "output_exprs" => Dict("expression" => "AB"),
    ))
    @test object_1d.status == 400
    @test occursin("output_exprs must be", String(object_1d.body))

    mixed_1d = post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "output_exprs" => Any["AB", 1],
    ))
    @test mixed_1d.status == 400
    @test occursin("output_exprs must be", String(mixed_1d.body))

    object_2d = post("/api/v1/parameter_scan_2d", Dict(
        "network" => network,
        "param1_symbol" => "tA",
        "param2_symbol" => "tB",
        "output_expr" => Dict("expression" => "AB"),
    ))
    @test object_2d.status == 400
    @test occursin("output_expr must be a string", String(object_2d.body))

    nonfinite_coefficient = post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "output_exprs" => Any["1e309*AB"],
    ))
    @test nonfinite_coefficient.status == 400

    bool_fixed = post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "output_exprs" => Any["AB"],
        "fixed_qK" => Any[true, 0.0, 0.0],
    ))
    @test bool_fixed.status == 400
    @test occursin("finite non-boolean", String(bool_fixed.body))

    landscape_bool_kd = post("/api/v1/atlas_landscape_2d", Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[true],
    ))
    @test landscape_bool_kd.status == 400
end

@testset "2D scans echo the resolved canonical model identity" begin
    post(path, body) = router(HTTP.Request(
        "POST",
        path,
        ["Content-Type" => "application/json"],
        JSON3.write(body),
    ))
    first = JSON3.read(String(post("/api/v1/build_model", Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
    )).body))
    second = JSON3.read(String(post("/api/v1/build_model", Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[2.0],
    )).body))
    @test first["network_ir_hash"] != second["network_ir_hash"]

    response = post("/api/v1/parameter_scan_2d", Dict(
        "session_id" => first["session_id"],
        "network_ir_hash" => second["network_ir_hash"],
        "param1_symbol" => "tA",
        "param2_symbol" => "tB",
        "output_expr" => "AB",
        "n_grid" => 20,
    ))
    @test response.status == 200
    payload = JSON3.read(String(response.body))
    @test payload["network_ir_hash"] == second["network_ir_hash"]
end

@testset "Model and placement handlers reject lossy JSON coercions" begin
    network = Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
    )
    rules = Any["A + B <-> AB"]
    post(path, body) = router(HTTP.Request(
        "POST", path, ["Content-Type" => "application/json"], JSON3.write(body)))

    @test post("/api/v1/build_model", merge(
        network, Dict("session_id" => true))).status == 400
    @test post("/api/v1/build_model", merge(
        network, Dict("session_id" => repeat("a", 129)))).status == 400
    @test post("/api/v1/find_vertices", Dict(
        "network_ir_hash" => true,
    )).status == 400
    @test post("/api/v1/find_vertices", Dict(
        "network_ir_hash" => "abc",
    )).status == 400
    @test post("/api/v1/rop_cloud", Dict(
        "network" => network,
        "sampling_mode" => true,
    )).status == 400
    @test post("/api/v1/rop_cloud", Dict(
        "sampling_mode" => "x_space",
        "reactions" => Any[true],
        "n_samples" => 100,
    )).status == 400

    for bad_idx in (1.5, true)
        response = post("/api/v1/vertex_detail", Dict(
            "network" => network,
            "vertex_idx" => bad_idx,
        ))
        @test response.status == 400
    end

    behavior_bool = post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "tA",
        "observe_x" => "AB",
        "deduplicate" => 1,
    ))
    @test behavior_bool.status == 400

    @test post("/api/v1/build_graph", Dict(
        "network" => network,
        "graph_mode" => "unknown",
    )).status == 400
    @test post("/api/v1/build_graph", Dict(
        "network" => network,
        "graph_mode" => "siso",
    )).status == 400
    @test post("/api/v1/build_graph", Dict(
        "network" => network,
        "graph_mode" => "siso",
        "change_qK" => "not_a_coordinate",
    )).status == 400
    @test post("/api/v1/siso_paths", Dict(
        "network" => network,
        "change_qK" => "not_a_coordinate",
    )).status == 400
    @test post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "tA",
    )).status == 400
    @test post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "not_a_coordinate",
        "observe_x" => "AB",
    )).status == 400
    @test post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "tA",
        "observe_x" => "not_a_species",
    )).status == 400
    @test post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "tA",
        "observe_x" => "AB",
        "path_scope" => "unknown",
    )).status == 400
    @test post("/api/v1/behavior_families", Dict(
        "network" => network,
        "change_qK" => "tA",
        "observe_x" => "AB",
        "path_scope" => "robust",
        "compute_volume" => false,
    )).status == 400

    unknown_target = post("/api/v1/rop_cloud", Dict(
        "sampling_mode" => "x_space",
        "reactions" => rules,
        "target_species" => "not_a_species",
        "n_samples" => 100,
    ))
    @test unknown_target.status == 400
    @test occursin("target_species", String(unknown_target.body))

    rop_bool = post("/api/v1/rop_polyhedron", Dict(
        "network" => network,
        "pairs" => Any[
            Dict("x_symbol" => "AB", "qk_symbol" => "tA"),
            Dict("x_symbol" => "AB", "qk_symbol" => "tB"),
        ],
        "add_inner_points" => 1,
    ))
    @test rop_bool.status == 400

    base_place = Dict(
        "rules" => rules,
        "input_sym" => "tA",
        "output_sym" => "AB",
        "target_ro" => 1.0,
    )
    @test post("/api/v1/place_parameters", merge(
        base_place, Dict("tol" => true))).status == 400
    @test post("/api/v1/place_parameters", merge(
        base_place, Dict("kd_bounds" => Any[true, 3.0]))).status == 400

    curve_base = Dict(
        "rules" => rules,
        "input_sym" => "tA",
        "output_sym" => "AB",
    )
    @test post("/api/v1/placer_curve", merge(
        curve_base, Dict("kd" => Any[true]))).status == 400
    @test post("/api/v1/placer_curve", merge(
        curve_base, Dict("kd" => Any[1.0], "totals" => Dict("tB" => true)))).status == 400
    routed_curve = post("/api/v1/placer_curve", merge(
        curve_base, Dict(
            "kd" => Any[1.0],
            "totals" => Dict("tB" => 1.0),
            "param_min" => -2.0,
            "param_max" => 2.0,
            "n_points" => 21,
        )))
    @test routed_curve.status == 200
    routed_curve_body = JSON3.read(String(routed_curve.body))
    @test length(routed_curve_body.param_values) == 21
    @test length(routed_curve_body.valid) == 21
    @test all(routed_curve_body.valid)
    @test routed_curve_body.partial == false

    threshold_base = Dict(
        "rules" => rules,
        "input_sym" => "tA",
        "output_sym" => "AB",
        "from_idx" => 1,
        "to_idx" => 2,
    )
    @test post("/api/v1/placer_threshold", merge(
        threshold_base, Dict("from_idx" => 1.5))).status == 400
    @test post("/api/v1/placer_threshold", merge(
        threshold_base, Dict("target_input" => true))).status == 400

    realize_base = Dict(
        "rules" => rules,
        "input_sym" => "tA",
        "output_sym" => "AB",
    )
    for bad_bounds in (Any[true, 3.0], Any[3.0, -3.0], Any[-3.0])
        @test post("/api/v1/placer_realize_program", merge(
            realize_base, Dict("kd_bounds" => bad_bounds))).status == 400
    end

    level_base = Dict(
        "rules" => rules,
        "input_sym" => "tA",
        "output_sym" => "AB",
        "operating_input" => 1.0,
        "target_level" => 1.0,
        "adjust_sym" => "tB",
        "kd" => Any[1.0],
    )
    @test post("/api/v1/placer_level", merge(
        level_base, Dict("operating_input" => true))).status == 400
    @test post("/api/v1/placer_level", merge(
        level_base, Dict("kd" => Any[true]))).status == 400
end

@testset "Request and Classic UI limits match the 1 MiB/20k contract" begin
    @test BiocircuitsExplorerBackend.Serialization.MAX_JSON_REQUEST_BYTES == 1024 * 1024

    oversized = router(HTTP.Request(
        "POST",
        "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        fill(UInt8('x'), BiocircuitsExplorerBackend.Serialization.MAX_JSON_REQUEST_BYTES + 1),
    ))
    @test oversized.status == 413
    oversized_payload = JSON3.read(String(oversized.body))
    @test oversized_payload["code"] == "request_body_too_large"
    @test oversized_payload["limit_bytes"] == 1024 * 1024
    @test oversized_payload["retryable"] === false

    deeply_nested = Dict{String, Any}("leaf" => true)
    for _ in 1:(BiocircuitsExplorerBackend.Serialization.MAX_JSON_NESTING_DEPTH + 1)
        deeply_nested = Dict{String, Any}("nested" => deeply_nested)
    end
    deep_response = router(HTTP.Request(
        "POST",
        "/api/v1/design_search",
        ["Content-Type" => "application/json"],
        JSON3.write(deeply_nested),
    ))
    @test deep_response.status == 400

    classic_html = read(joinpath(@__DIR__, "..", "public", "classic.html"), String)
    @test occursin(r"id=\"cloud-samples\"[^>]*max=\"20000\"", classic_html)
    @test !occursin(r"id=\"cloud-samples\"[^>]*max=\"100000\"", classic_html)
end

@testset "Session aliases are bounded LRU state" begin
    store = BiocircuitsExplorerBackend.SessionStore
    store._clear_all!()
    first_payload = Dict("model" => 1)
    second_payload = Dict("model" => 2)
    third_payload = Dict("model" => 3)
    store.set_session("first", first_payload; max_sessions=2)
    store.set_session("second", second_payload; max_sessions=2)
    lock(store._LOCK) do
        store._SESSION_LAST_ACCESS["first"] = 0.0
        store._SESSION_LAST_ACCESS["second"] = 1.0
    end
    store.set_session("third", third_payload; max_sessions=2)
    @test store.get_session("first") === nothing
    @test store.get_session("second") === second_payload
    @test store.get_session("third") === third_payload
    @test store.session_count() == 2
    @test store.set_session_if_available("second", second_payload; max_sessions=2)
    @test !store.set_session_if_available("second", first_payload; max_sessions=2)
    store._clear_all!()
end

@testset "HTTP atlas paths stay inside the configured store" begin
    mktempdir() do dir
        store = joinpath(dir, "atlas-store")
        outside = joinpath(dir, "outside")
        mkpath(store)
        mkpath(outside)
        symlink(outside, joinpath(store, "escape"))

        withenv("BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS" => nothing) do
            @test_throws ArgumentError BiocircuitsExplorerBackend._normalize_http_atlas_paths(Dict(
                "sqlite_path" => "safe.sqlite",
            ))
            disabled = router(HTTP.Request(
                "POST",
                "/api/v1/query_atlas",
                ["Content-Type" => "application/json"],
                JSON3.write(Dict(
                    "sqlite_path" => "safe.sqlite",
                    "query" => Dict("limit" => 1),
                )),
            ))
            @test disabled.status == 400
        end

        withenv(
            "BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT" => store,
            "BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS" => "1",
        ) do
            normalized = BiocircuitsExplorerBackend._normalize_http_atlas_paths(Dict(
                "sqlite_path" => "safe.sqlite",
            ))
            @test normalized["sqlite_path"] == joinpath(store, "safe.sqlite")
            @test_throws ArgumentError BiocircuitsExplorerBackend._normalize_http_atlas_paths(Dict(
                "sqlite_path" => joinpath(outside, "outside.sqlite"),
            ))
            @test_throws ArgumentError BiocircuitsExplorerBackend._normalize_http_atlas_paths(Dict(
                "sqlite_path" => joinpath("escape", "outside.sqlite"),
            ))

            outside_service = router(HTTP.Request(
                "POST",
                "/api/v1/build_atlas",
                ["Content-Type" => "application/json"],
                JSON3.write(Dict(
                    "sqlite_path" => joinpath(outside, "outside.sqlite"),
                    "networks" => Any[],
                )),
            ))
            @test outside_service.status == 400

            corpus_bypass = router(HTTP.Request(
                "POST",
                "/api/v1/build_atlas_library",
                ["Content-Type" => "application/json"],
                JSON3.write(Dict(
                    "sqlite_path" => joinpath(outside, "outside.sqlite"),
                    "network_entries" => Any[],
                    "behavior_slices" => Any[],
                    "family_buckets" => Any[],
                )),
            ))
            @test corpus_bypass.status == 400

            outside_job = router(HTTP.Request(
                "POST",
                "/api/v1/jobs",
                ["Content-Type" => "application/json"],
                JSON3.write(Dict(
                    "kind" => "build_atlas",
                    "spec" => Dict(
                        "sqlite_path" => joinpath(outside, "outside.sqlite"),
                        "networks" => Any[],
                    ),
                )),
            ))
            @test outside_job.status == 400
        end
    end
end
