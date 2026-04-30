using Test
using BiocircuitsExplorerBackend
using BindingAndCatalysis
using Logging
using HTTP
using JSON3

const SIMPLE_NETWORK = Dict(
    "label" => "monomer_dimer",
    "reactions" => Any["A + B <-> AB"],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["AB"],
)

const ALT_NETWORK = Dict(
    "label" => "monomer_alt_dimer",
    "reactions" => Any["A + C <-> AC"],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["AC"],
)

const DUAL_INPUT_NETWORK = Dict(
    "label" => "dual_input_dimer",
    "reactions" => Any["A + B <-> AB"],
    "input_symbols" => Any["tA", "tB"],
    "output_symbols" => Any["AB"],
)

const HIGH_NULLITY_NETWORK = Dict(
    "label" => "step_trimer",
    "reactions" => Any[
        "A + B <-> C_A_B",
        "C_A_B + C <-> C_A_B_C",
    ],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["A"],
)

const HOMOMER_MIXED_NETWORK = Dict(
    "label" => "homomer_mixed",
    "reactions" => Any[
        "A + A <-> C_A_A",
        "A + B <-> C_A_B",
    ],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["C_A_A"],
)

const ORTHANT_NETWORK = Dict(
    "label" => "orthant_dimer",
    "reactions" => Any["A + B <-> AB"],
    "change_specs" => Any[
        Dict(
            "kind" => "orthant",
            "qk_symbols" => Any["tA", "tB"],
            "signs" => Any["+", "+"],
        ),
    ],
    "output_symbols" => Any["AB"],
)

const D4_REGRESSION_NETWORK = Dict(
    "label" => "d4_regression",
    "reactions" => Any[
        "A + B <-> C_A_B",
        "A + C <-> C_A_C",
        "A + D <-> C_A_D",
        "B + C <-> C_B_C",
    ],
    "change_specs" => Any[
        Dict(
            "kind" => "orthant",
            "qk_symbols" => Any["tA", "tB"],
            "signs" => Any["+", "+"],
        ),
    ],
    "output_symbols" => Any["A"],
)

const EMPTY_PATH_REGRESSION_NETWORK = Dict(
    "label" => "complex_growth_empty_path_regression",
    "reactions" => Any[
        "A + A <-> C_A_A",
        "A + C_A_A_B <-> C_A_A_A_B",
        "B + C_A_A <-> C_A_A_B",
        "B + C_A_A_A_B <-> C_A_A_A_B_B",
    ],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["C_A_A_A_B_B"],
)

struct ThrowingLogger <: AbstractLogger end

Logging.min_enabled_level(::ThrowingLogger) = Logging.Info
Logging.shouldlog(::ThrowingLogger, level, _module, group, id) = level >= Logging.Info
Logging.catch_exceptions(::ThrowingLogger) = false

function Logging.handle_message(::ThrowingLogger, level, message, _module, group, id, file, line; kwargs...)
    throw(IOError("write", Base.UV_EPIPE))
end

@testset "Compiled Query Hash" begin
    profile = atlas_search_profile_binding_small_v0()
    q1 = Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
        "motif_labels" => Any["x"],
        "limit" => 3,
    )
    q2 = Dict(
        "limit" => 3,
        "motif_labels" => Any["x"],
        "output_symbols" => Any["AB"],
        "input_symbols" => Any["tA"],
    )

    gamma1 = compile_query(q1, profile)
    gamma2 = compile_query(q2, profile)

    @test gamma1["h_Q"] == gamma2["h_Q"]
end

@testset "Buffering Logger Tolerates Broken Console Pipe" begin
    lock(BiocircuitsExplorerBackend.DEBUG_LOG_LOCK) do
        empty!(BiocircuitsExplorerBackend.DEBUG_LOGS)
        BiocircuitsExplorerBackend.DEBUG_LOG_SEQ[] = 0
    end

    logger = BiocircuitsExplorerBackend.BufferingConsoleLogger(ThrowingLogger(), Logging.Info)

    @test Logging.catch_exceptions(logger) == true
    @test_nowarn Logging.handle_message(logger, Logging.Info, "test message", @__MODULE__, :tests, :event, "file", 1)
    @test logger.console_forwarding_enabled[] == false

    lock(BiocircuitsExplorerBackend.DEBUG_LOG_LOCK) do
        @test any(entry -> get(entry, "message", "") == "test message", BiocircuitsExplorerBackend.DEBUG_LOGS)
        @test any(entry -> occursin("Console log forwarding disabled", get(entry, "message", "")), BiocircuitsExplorerBackend.DEBUG_LOGS)
    end
end

@testset "JSON Safe Value Sanitizes Nonfinite Reals" begin
    sanitized = BiocircuitsExplorerBackend.json_safe_value(Dict(
        "finite" => 1.5,
        "pos_inf" => Inf,
        "neg_inf" => -Inf,
        "nan" => NaN,
        "nested" => Any[Inf, Dict(:x => -Inf)],
    ))

    @test sanitized["finite"] == 1.5
    @test sanitized["pos_inf"] == "Inf"
    @test sanitized["neg_inf"] == "-Inf"
    @test sanitized["nan"] == "NaN"
    @test sanitized["nested"][1] == "Inf"
    @test sanitized["nested"][2]["x"] == "-Inf"
end

@testset "Behavior Program Codec Round Trips" begin
    cfg = Dict(
        "ro_quantization_digits" => 3,
        "ro_quantization_scale" => 1000,
        "motif_zero_tol" => 1e-6,
    )

    scalar_profile = Any[0.0, -1.0, 1.0, 0.0]
    vector_profile = Any[Any[0.0, 1.0], Any[-1.0, 1.0], Any[-1.0, 0.0]]
    singular_profile = Any[NaN, Inf, -Inf, 1 / 3]

    for profile in (scalar_profile, vector_profile, singular_profile)
        blob = encode_program_blob(profile, cfg)
        @test startswith(String(blob[1:4]), "RPB1")
        @test decode_program_blob(blob, cfg) == canonical_program_profile(profile, cfg)
        @test behavior_program_hash(blob) == behavior_program_hash(encode_program_blob(profile, cfg))
    end

    @test program_exact_label(scalar_profile, cfg) == "0 -> -1 -> +1 -> 0"
    @test program_exact_label(Any[1 / 3], cfg) == "+0.333"
    features = program_features(vector_profile, cfg)
    @test features["len"] == 3
    @test features["dim"] == 2
    @test features["c_distinct"] == 3.0
end

@testset "Behavior Aggregate SQLite Writer" begin
    mktempdir() do dir
        db_path = joinpath(dir, "behavior_aggregate.sqlite")
        network_id = "[1]+[2]<->[1,2]"
        cfg = BiocircuitsExplorerBackend.atlas_behavior_config_to_dict(AtlasBehaviorConfig(
            path_scope=:feasible,
            min_volume_mean=0.0,
            include_path_records=false,
        ))
        atlas = Dict(
            "atlas_schema_version" => "0.2.0",
            "generated_at" => "test",
            "network_entries" => Any[Dict(
                "network_id" => network_id,
                "canonical_code" => network_id,
                "analysis_status" => "ok",
                "base_species_count" => 2,
                "reaction_count" => 1,
                "total_species_count" => 3,
                "max_support" => 2,
                "support_mass" => 2,
                "source_label" => "codec_test",
                "source_kind" => "explicit",
                "motif_union" => Any["0 -> +"],
                "exact_union" => Any["0.0 -> 1.0"],
                "slice_ids" => Any["slice-1"],
            )],
            "input_graph_slices" => Any[Dict(
                "graph_slice_id" => "graph-1",
                "network_id" => network_id,
                "input_symbol" => "tA",
                "change_signature" => "tA:+",
                "vertex_count" => 2,
                "edge_count" => 1,
                "path_count" => 2,
            )],
            "behavior_slices" => Any[Dict(
                "slice_id" => "slice-1",
                "network_id" => network_id,
                "graph_slice_id" => "graph-1",
                "input_symbol" => "tA",
                "change_signature" => "tA:+",
                "output_symbol" => "AB",
                "analysis_status" => "ok",
                "path_scope" => "feasible",
                "min_volume_mean" => 0.0,
                "total_paths" => 2,
                "feasible_paths" => 2,
                "included_paths" => 2,
                "excluded_paths" => 0,
                "motif_union" => Any["0 -> +"],
                "exact_union" => Any["0.0 -> 1.0"],
                "classifier_config" => cfg,
            )],
            "regime_records" => Any[],
            "transition_records" => Any[],
            "family_buckets" => Any[Dict(
                "bucket_id" => "slice-1::exact::1",
                "slice_id" => "slice-1",
                "family_kind" => "exact",
                "family_idx" => 1,
                "exact_profile" => Any[0.0, 1.0],
                "family_label" => "0.0 -> 1.0",
                "motif_profile" => Any[0, 1],
                "parent_motif" => "0 -> +",
                "path_count" => 2,
                "robust_path_count" => 0,
                "volume_mean" => nothing,
                "representative_path_idx" => 1,
                "representative_vertex_indices" => Any[1, 2],
                "representative_path_length" => 2,
            )],
            "path_records" => Any[],
            "duplicate_inputs" => Any[],
        )

        summary = atlas_sqlite_append_atlas!(db_path, atlas; persist_mode=:behavior_aggregate)
        @test summary["persist_mode"] == "behavior_aggregate"
        @test summary["behavior_program_count"] == 1
        @test summary["slice_program_support_count"] == 1
        @test summary["network_program_support_count"] == 1
        @test summary["witness_path_count"] == 1
        @test summary["path_record_count"] == 0
    end
end

@testset "Parent Watchdog Exit Logic" begin
    @test BiocircuitsExplorerBackend.parent_watchdog_should_exit(nothing, 1) == false
    @test BiocircuitsExplorerBackend.parent_watchdog_should_exit(3210, 3210) == false
    @test BiocircuitsExplorerBackend.parent_watchdog_should_exit(3210, 1) == true
    @test BiocircuitsExplorerBackend.parent_watchdog_should_exit(3210, 9999) == true

    withenv("BIOCIRCUITS_EXPLORER_PARENT_PID" => "4321", "ROP_PARENT_PID" => "") do
        @test BiocircuitsExplorerBackend.configured_parent_pid() == 4321
    end
    withenv("BIOCIRCUITS_EXPLORER_PARENT_PID" => "bad", "ROP_PARENT_PID" => "") do
        @test BiocircuitsExplorerBackend.configured_parent_pid() === nothing
    end
end

@testset "Router Guards Static And API Errors" begin
    traversal = router(HTTP.Request("GET", "/../Project.toml"))
    malformed = router(HTTP.Request("POST", "/api/build_model", ["Content-Type" => "application/json"], "{"))
    missing_field = router(HTTP.Request("POST", "/api/build_model", ["Content-Type" => "application/json"], JSON3.write(Dict(
        "kd" => Any[1.0],
    ))))
    wrong_method = router(HTTP.Request("GET", "/api/build_model"))

    @test traversal.status == 404
    @test malformed.status == 400
    @test occursin("Invalid JSON", String(malformed.body))
    @test missing_field.status == 400
    @test occursin("reactions", String(missing_field.body))
    @test wrong_method.status == 405
    @test occursin("Method not allowed", String(wrong_method.body))
end

@testset "Unsupported Query Scope" begin
    profile = atlas_search_profile_binding_small_v0()
    @test_throws ArgumentError compile_query(Dict("unknown_key" => 1), profile)
    @test_throws ArgumentError compile_query(Dict(
        "required_regimes" => Any[Dict("vertex_idx" => 1)],
    ), profile)
end

@testset "Support Hard Negative" begin
    spec = Dict(
        "networks" => Any[SIMPLE_NETWORK],
        "query" => Dict(
            "max_base_species" => 1,
            "limit" => 5,
        ),
        "inverse_design" => Dict(
            "return_library" => true,
            "return_delta_atlas" => false,
        ),
    )

    result = run_inverse_design_from_spec(spec)
    certs = result["library"]["negative_certificate_store"]

    @test result["query_result"]["result_count"] == 0
    @test length(certs) >= 1
    @test any(cert -> cert["scope"] == "support", certs)
end

@testset "Negative Knowledge Versioning" begin
    library = atlas_library_default()
    versions = Dict(
        "profile_version" => "binding_small_v0",
        "compiler_version" => "gamma_q_v0.1.0",
        "policy_version" => "support_screen_v0.1.0",
    )

    record_negative(
        library,
        "support",
        "sig",
        "hash",
        "soft",
        "budget_exhausted",
        Dict("kind" => "soft_note"),
        versions,
    )
    @test check_negative(library, "support", "sig", "hash", versions) === nothing

    record_negative(
        library,
        "support",
        "sig",
        "hash",
        "hard",
        "exact_support_screen_empty",
        Dict("kind" => "hall_type_separation"),
        versions,
    )
    @test check_negative(library, "support", "sig", "hash", versions) !== nothing
    @test check_negative(library, "support", "sig", "hash", merge(versions, Dict("policy_version" => "support_screen_v9.9.9"))) === nothing
end

@testset "Exact Hall Negative" begin
    hall_network = Dict(
        "label" => "hall_fail",
        "reactions" => Any[
            "A + B <-> AB",
            "A + B <-> P",
            "A + C <-> AC",
        ],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
    )

    spec = Dict(
        "networks" => Any[hall_network],
        "query" => Dict(
            "support_count_spec" => Dict(
                "allowed_species" => Any["AB", "P"],
                "min_counts" => Dict("AB" => 2, "P" => 1),
            ),
            "limit" => 5,
        ),
        "inverse_design" => Dict(
            "return_library" => true,
            "return_delta_atlas" => false,
        ),
    )

    result = run_inverse_design_from_spec(spec)
    certs = result["library"]["negative_certificate_store"]

    @test result["query_result"]["result_count"] == 0
    @test any(cert -> cert["reason"] == "exact_support_screen_empty", certs)
end

@testset "Volume Policy Coercion" begin
    spec = Dict(
        "networks" => Any[SIMPLE_NETWORK],
        "query" => Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
            "require_witness_robust" => true,
            "limit" => 5,
        ),
        "volume_policy" => "proxy",
    )

    result = run_inverse_design_from_spec(spec)
    @test result["policies"]["volume_policy_requested"] == "proxy"
    @test result["policies"]["volume_policy"] == "estimated"
    @test result["policies"]["volume_policy_coercion_reason"] == "exact_volume_semantics_require_estimated_policy"
end

@testset "Summary First Lazy Witness And Refinement" begin
    spec = Dict(
        "networks" => Any[SIMPLE_NETWORK],
        "query" => Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
            "require_witness_feasible" => true,
            "limit" => 5,
        ),
        "refinement" => Dict(
            "enabled" => true,
            "top_k" => 1,
            "trials" => 1,
            "n_points" => 25,
            "include_traces" => false,
        ),
        "inverse_design" => Dict(
            "return_library" => true,
            "return_delta_atlas" => true,
        ),
    )

    result = run_inverse_design_from_spec(spec)
    query_result = result["query_result"]
    library = result["library"]
    delta_atlas = result["delta_atlas"]
    refinement = result["refinement_result"]

    @test query_result["result_count"] == 1
    @test length(delta_atlas["path_records"]) == 0
    @test length(library["materialization_events"]) >= 1
    @test length(library["path_records"]) >= 1
    @test result["best_design"] !== nothing
    @test refinement["enabled"] == true
    @test refinement["best_candidate"] !== nothing
    @test haskey(refinement["best_candidate"]["best_trial"], "seed_source")
end

@testset "Reproducible Query Result" begin
    spec = Dict(
        "networks" => Any[SIMPLE_NETWORK],
        "query" => Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
            "require_witness_feasible" => true,
            "limit" => 5,
        ),
        "refinement" => Dict(
            "enabled" => true,
            "top_k" => 1,
            "trials" => 1,
            "n_points" => 25,
            "include_traces" => false,
        ),
        "inverse_design" => Dict(
            "return_library" => true,
            "return_delta_atlas" => false,
        ),
    )

    first = run_inverse_design_from_spec(spec)
    second = run_inverse_design_from_spec(merge(spec, Dict("library" => first["library"])))

    @test first["compiled_query"]["h_Q"] == second["compiled_query"]["h_Q"]
    @test first["query_result"]["result_count"] == second["query_result"]["result_count"] == 1
    @test first["best_design"]["candidate"]["network_id"] == second["best_design"]["candidate"]["network_id"]
    @test first["best_design"]["candidate"]["slice_id"] == second["best_design"]["candidate"]["slice_id"]
    @test first["best_design"]["candidate"]["refinement_score"] == second["best_design"]["candidate"]["refinement_score"]
end

@testset "SQLite Query Prefilter Roundtrip" begin
    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas.sqlite")
        atlas = build_behavior_atlas_from_spec(Dict(
            "networks" => Any[SIMPLE_NETWORK, ALT_NETWORK],
            "behavior_config" => Dict(
                "include_path_records" => true,
            ),
        ))
        atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="sqlite_prefilter_test")

        query = Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
            "require_witness_feasible" => true,
            "limit" => 5,
        )

        in_memory = query_behavior_atlas(atlas, query)
        via_sqlite = query_behavior_atlas_from_spec(Dict(
            "sqlite_path" => sqlite_path,
            "query" => query,
        ))
        prefiltered = BiocircuitsExplorerBackend.atlas_sqlite_load_query_corpus(sqlite_path, query)

        @test in_memory["result_count"] == 1
        @test via_sqlite["result_count"] == in_memory["result_count"]
        @test via_sqlite["results"][1]["slice_id"] == in_memory["results"][1]["slice_id"]
        @test via_sqlite["results"][1]["network_id"] == in_memory["results"][1]["network_id"]
        @test via_sqlite["results"][1]["best_witness_path"]["path_record_id"] == in_memory["results"][1]["best_witness_path"]["path_record_id"]
        @test prefiltered["sqlite_prefilter"]["candidate_slice_count"] == 1
        @test prefiltered["sqlite_prefilter"]["candidate_network_count"] == 1
        @test length(prefiltered["behavior_slices"]) == 1
        @test length(prefiltered["path_records"]) >= 1
    end
end

@testset "SQLite Append Atlas Reconstructs Library" begin
    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas_append.sqlite")
        atlas = build_behavior_atlas_from_spec(Dict(
            "networks" => Any[SIMPLE_NETWORK, ALT_NETWORK],
            "behavior_config" => Dict(
                "include_path_records" => true,
            ),
        ))

        summary = atlas_sqlite_append_atlas!(sqlite_path, atlas;
            source_label="sqlite_append_test",
            library_label="append_only_library",
        )
        loaded = atlas_sqlite_load_library(sqlite_path)
        query = Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
            "limit" => 5,
        )
        in_memory = query_behavior_atlas(atlas, query)
        via_inverse = run_inverse_design_from_spec(Dict(
            "sqlite_path" => sqlite_path,
            "query" => query,
            "inverse_design" => Dict(
                "build_library_if_missing" => false,
                "return_library" => false,
                "return_delta_atlas" => false,
            ),
        ))

        @test atlas_sqlite_has_library(sqlite_path) == true
        @test summary["atlas_count"] == 1
        @test atlas_sqlite_summary(sqlite_path)["atlas_count"] == 1
        @test loaded["atlas_count"] == 1
        @test loaded["library_label"] == "append_only_library"
        @test loaded["unique_network_count"] == atlas["unique_network_count"]
        @test length(loaded["behavior_slices"]) == length(atlas["behavior_slices"])
        @test via_inverse["build_source_mode"] == "sqlite_library"
        @test via_inverse["query_result"]["result_count"] == in_memory["result_count"] == 1
        @test via_inverse["query_result"]["results"][1]["network_id"] == in_memory["results"][1]["network_id"]
        @test via_inverse["query_result"]["results"][1]["slice_id"] == in_memory["results"][1]["slice_id"]
    end
end

@testset "Prune SQLite Uses Lightweight Runtime Persist" begin
    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas_prune.sqlite")
        db = BiocircuitsExplorerBackend.atlas_sqlite_connect(sqlite_path)
        try
            BiocircuitsExplorerBackend._atlas_sqlite_set_metadata!(db, "prune_only_sqlite", "true")
        finally
            BiocircuitsExplorerBackend.SQLite.close(db)
        end

        atlas = build_behavior_atlas_from_spec(Dict(
            "networks" => Any[SIMPLE_NETWORK, ALT_NETWORK],
            "behavior_config" => Dict(
                "include_path_records" => true,
            ),
        ))

        summary = atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="sqlite_prune_test")
        loaded = atlas_sqlite_load_library(sqlite_path)
        prefiltered = BiocircuitsExplorerBackend.atlas_sqlite_load_query_corpus(sqlite_path, Dict(
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
        ))

        @test summary["behavior_slice_count"] == length(atlas["behavior_slices"])
        @test summary["regime_record_count"] == length(atlas["regime_records"])
        @test summary["family_bucket_count"] == length(atlas["family_buckets"])
        @test summary["transition_record_count"] == 0
        @test summary["path_record_count"] == 0
        @test atlas_sqlite_summary(sqlite_path)["path_record_count"] == 0
        @test length(loaded["transition_records"]) == 0
        @test length(loaded["path_records"]) == 0
        @test length(prefiltered["behavior_slices"]) == 1
        @test prefiltered["behavior_slices"][1]["output_symbol"] == "AB"

        db = BiocircuitsExplorerBackend.atlas_sqlite_connect(sqlite_path)
        try
            @test BiocircuitsExplorerBackend._atlas_sqlite_metadata_text(db, "persist_mode") == "lightweight"
        finally
            BiocircuitsExplorerBackend.SQLite.close(db)
        end
    end
end

@testset "SQLite Helpers Do Not Accumulate Registered Statements" begin
    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas_stmt_lifecycle.sqlite")
        db = BiocircuitsExplorerBackend.atlas_sqlite_connect(sqlite_path)
        try
            @test length(db.stmt_wrappers) == 0

            BiocircuitsExplorerBackend._atlas_sqlite_execute(db, "CREATE TABLE tmp_values (x INTEGER)")
            @test length(db.stmt_wrappers) == 0

            for value in 1:5
                BiocircuitsExplorerBackend._atlas_sqlite_execute(db, "INSERT INTO tmp_values (x) VALUES (?)", (value,))
            end
            @test length(db.stmt_wrappers) == 0

            query = BiocircuitsExplorerBackend._atlas_sqlite_query(db, "SELECT x FROM tmp_values ORDER BY x")
            try
                @test [Int(row[:x]) for row in query] == collect(1:5)
                @test length(db.stmt_wrappers) == 0
            finally
                BiocircuitsExplorerBackend.DBInterface.close!(query)
            end

            @test length(db.stmt_wrappers) == 0
        finally
            BiocircuitsExplorerBackend.SQLite.close(db)
        end
    end
end

@testset "Change Expansion Generates Axis And Orthant Slices" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[DUAL_INPUT_NETWORK],
        "change_expansion" => Dict(
            "mode" => "orthant",
            "max_active_dims" => 2,
            "include_axis_slices" => true,
        ),
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    signatures = sort!([String(slice["change_signature"]) for slice in atlas["behavior_slices"]])
    @test atlas["change_expansion"]["mode"] == "orthant"
    @test length(atlas["behavior_slices"]) == 3
    @test signatures == ["orthant(+tA,+tB)", "tA", "tB"]
end

@testset "Atlas Landscape 2D Scan From Raw Rules" begin
    result = BiocircuitsExplorerBackend.atlas_landscape_2d_from_spec(Dict(
        "reactions" => Any["A + B <-> AB"],
        "output_expr" => "AB",
        "preferred_param_symbols" => Any["tA", "tB"],
        "n_grid" => 24,
    ))

    @test result["param1_symbol"] == "tA"
    @test result["param2_symbol"] == "tB"
    @test result["output_expr"] == "AB"
    @test result["param_symbol_options"] == ["tA", "tB", "Kd1"]
    @test result["output_symbol_options"] == ["A", "B", "AB"]
    @test length(result["param1_values"]) == 24
    @test length(result["param2_values"]) == 24
    @test length(result["output_grid"]) == 24
    @test length(first(result["output_grid"])) == 24
    @test length(result["regime_grid"]) == 24
    @test length(first(result["regime_grid"])) == 24
    @test length(result["bounds"]) == 24
    @test length(first(result["bounds"])) == 24
end

@testset "Higher Nullity Off-Path Vertices Do Not Break Materialization" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[D4_REGRESSION_NETWORK],
        "search_profile" => Dict(
            "name" => "binding_small_v0",
            "slice_mode" => "change",
            "input_mode" => "totals_only",
        ),
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["behavior_slices"]) == 1
    @test atlas["behavior_slices"][1]["analysis_status"] == "ok"
    @test atlas["behavior_slices"][1]["regime_record_count"] > 0
    @test atlas["input_graph_slices"][1]["vertex_count"] < atlas["input_graph_slices"][1]["full_vertex_count"]
end

@testset "Subset Binding Enumeration Supports Higher-Order Templates" begin
    profile = AtlasSearchProfile(
        name="higher_order_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_higher_order_templates=true,
        max_support=3,
        max_reactions=5,
    )
    spec = AtlasEnumerationSpec(
        mode=:subset_binding,
        base_species_counts=[3],
        min_reactions=1,
        max_reactions=1,
        min_template_order=3,
        max_template_order=3,
    )

    networks, summary = enumerate_network_specs(spec; search_profile=profile)
    @test length(networks) == 1
    @test networks[1][:reactions] == ["A + B + C <-> C_A_B_C"]
    @test summary["generated_network_count"] == 1
end

@testset "Homomeric Templates Validate and Build" begin
    profile = AtlasSearchProfile(
        name="homomer_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_homomeric_templates=true,
        max_homomer_order=3,
        max_support=3,
        max_reactions=5,
    )

    dimer_validation = BiocircuitsExplorerBackend.validate_rules_against_profile(["A + A <-> AA"], profile)
    trimer_validation = BiocircuitsExplorerBackend.validate_rules_against_profile(["A + A + A <-> AAA"], profile)

    @test dimer_validation["valid"] == true
    @test trimer_validation["valid"] == true
    @test dimer_validation["metrics"]["max_support"] == 2
    @test trimer_validation["metrics"]["max_support"] == 3
    @test dimer_validation["supports"][:AA] == [:A, :A]
    @test trimer_validation["supports"][:AAA] == [:A, :A, :A]

    atlas = build_behavior_atlas_from_spec(Dict(
        "search_profile" => Dict(
            "name" => "homomer_scan",
            "slice_mode" => "change",
            "input_mode" => "totals_only",
            "allow_homomeric_templates" => true,
            "max_homomer_order" => 3,
        ),
        "networks" => Any[HOMOMER_MIXED_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test length(atlas["behavior_slices"]) == 1
    @test only(atlas["behavior_slices"])["output_symbol"] == "C_A_A"
end

@testset "Pairwise Plus Homomeric Enumeration Includes AA and AAA" begin
    profile = AtlasSearchProfile(
        name="pairwise_plus_homomeric_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_homomeric_templates=true,
        max_homomer_order=3,
        max_support=3,
        max_reactions=5,
    )
    spec = AtlasEnumerationSpec(
        mode=:pairwise_plus_homomeric,
        base_species_counts=[2],
        min_reactions=2,
        max_reactions=2,
        min_template_order=2,
        max_template_order=3,
    )

    networks, summary = enumerate_network_specs(spec; search_profile=profile)
    rendered = Set(Tuple(sort(String.(network[:reactions]))) for network in networks)

    @test ("A + A <-> C_A_A", "B + B <-> C_B_B") in rendered
    @test ("A + A + A <-> C_A_A_A", "B + B <-> C_B_B") in rendered
    @test summary["generated_network_count"] == length(networks)
end

@testset "Pairwise Plus Homomeric Enumeration Supports Tetramer Filter" begin
    profile = AtlasSearchProfile(
        name="pairwise_plus_homomeric_tetramer_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_homomeric_templates=true,
        max_homomer_order=4,
        max_support=4,
        max_reactions=5,
        max_base_species=1,
    )
    spec = AtlasEnumerationSpec(
        mode=:pairwise_plus_homomeric,
        base_species_counts=[1],
        min_reactions=1,
        max_reactions=1,
        min_template_order=2,
        max_template_order=4,
        require_homomeric_template=true,
        require_product_support_at_least=4,
    )

    networks, summary = enumerate_network_specs(spec; search_profile=profile)
    rendered = Set(Tuple(sort(String.(network[:reactions]))) for network in networks)

    @test ("A + A + A + A <-> C_A_A_A_A",) in rendered
    @test !any(any(occursin("C_A_A", rule) && !occursin("C_A_A_A_A", rule) for rule in reaction_set) for reaction_set in rendered)
    @test summary["generated_network_count"] == length(networks) >= 1
end

@testset "Complex-Growth Enumeration Includes AB Plus C To ABC" begin
    profile = AtlasSearchProfile(
        name="complex_growth_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_homomeric_templates=true,
        allow_higher_order_templates=true,
        max_homomer_order=3,
        max_support=3,
        max_reactions=5,
        max_base_species=3,
    )
    spec = AtlasEnumerationSpec(
        mode=:complex_growth_binding,
        base_species_counts=[3],
        min_reactions=2,
        max_reactions=2,
        max_template_order=3,
        require_complex_growth_template=true,
        require_product_support_at_least=3,
    )

    networks, summary = enumerate_network_specs(spec; search_profile=profile)
    rendered = Set(Tuple(sort(String.(network[:reactions]))) for network in networks)

    @test ("A + B <-> C_A_B", "C + C_A_B <-> C_A_B_C") in rendered
    @test summary["generated_network_count"] == length(networks) >= 1
end

@testset "Complex-Growth Enumeration Includes Tetrameric Homomer Growth" begin
    profile = AtlasSearchProfile(
        name="complex_growth_homomer_scan",
        slice_mode=:change,
        input_mode=:totals_only,
        allow_homomeric_templates=true,
        allow_higher_order_templates=true,
        max_homomer_order=4,
        max_support=4,
        max_reactions=5,
        max_base_species=1,
    )
    spec = AtlasEnumerationSpec(
        mode=:complex_growth_binding,
        base_species_counts=[1],
        min_reactions=2,
        max_reactions=2,
        max_template_order=4,
        require_homomeric_template=true,
        require_complex_growth_template=true,
        require_product_support_at_least=4,
    )

    networks, summary = enumerate_network_specs(spec; search_profile=profile)
    rendered = Set(Tuple(sort(String.(network[:reactions]))) for network in networks)

    @test ("A + A <-> C_A_A", "C_A_A + C_A_A <-> C_A_A_A_A") in rendered
    @test summary["generated_network_count"] == length(networks) >= 1
end

@testset "Parallel Network Build Matches Serial Build" begin
    spec = Dict(
        "networks" => Any[SIMPLE_NETWORK, ALT_NETWORK, HIGH_NULLITY_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    )

    serial = build_behavior_atlas_from_spec(spec)
    parallel = build_behavior_atlas_from_spec(merge(spec, Dict("network_parallelism" => 2)))

    @test serial["input_network_count"] == parallel["input_network_count"] == 3
    @test serial["unique_network_count"] == parallel["unique_network_count"]
    @test serial["successful_network_count"] == parallel["successful_network_count"]
    @test serial["failed_network_count"] == parallel["failed_network_count"]
    @test length(serial["duplicate_inputs"]) == length(parallel["duplicate_inputs"]) == 1
    @test sort!([String(entry["network_id"]) for entry in serial["network_entries"]]) ==
          sort!([String(entry["network_id"]) for entry in parallel["network_entries"]])
    @test sort!([String(slice["slice_id"]) for slice in serial["behavior_slices"]]) ==
          sort!([String(slice["slice_id"]) for slice in parallel["behavior_slices"]])
    @test sort!([String(item["duplicate_of_network_id"]) for item in serial["duplicate_inputs"]]) ==
          sort!([String(item["duplicate_of_network_id"]) for item in parallel["duplicate_inputs"]])
    @test parallel["network_parallelism"] == (Threads.nthreads() > 1 ? 2 : 1)
end

@testset "SQLite Query Prefilter Supports Change Signatures" begin
    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas.sqlite")
        atlas = build_behavior_atlas_from_spec(Dict(
            "networks" => Any[ORTHANT_NETWORK],
            "behavior_config" => Dict(
                "include_path_records" => true,
                "compute_volume" => false,
                "min_volume_mean" => 0.0,
            ),
        ))
        atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="sqlite_change_signature_test")

        query = Dict(
            "change_signatures" => Any["orthant(+tA,+tB)"],
            "output_symbols" => Any["AB"],
            "limit" => 5,
        )

        in_memory = query_behavior_atlas(atlas, query)
        via_sqlite = query_behavior_atlas_from_spec(Dict(
            "sqlite_path" => sqlite_path,
            "query" => query,
        ))
        prefiltered = BiocircuitsExplorerBackend.atlas_sqlite_load_query_corpus(sqlite_path, query)

        @test in_memory["result_count"] == 1
        @test via_sqlite["result_count"] == 1
        @test via_sqlite["results"][1]["change_signature"] == "orthant(+tA,+tB)"
        @test prefiltered["sqlite_prefilter"]["candidate_slice_count"] == 1
        @test only(prefiltered["behavior_slices"])["change_signature"] == "orthant(+tA,+tB)"
    end
end

@testset "Previously High Nullity Slices Materialize And Reuse" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[HIGH_NULLITY_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["behavior_slices"]) == 1
    @test length(atlas["regime_records"]) > 0
    @test length(atlas["transition_records"]) > 0
    @test length(atlas["family_buckets"]) > 0

    slice = only(atlas["behavior_slices"])
    network = only(atlas["network_entries"])

    @test slice["analysis_status"] == "ok"
    @test slice["build_state"] == "complete"
    @test slice["partial_result_available"] == false
    @test slice["regime_record_count"] > 0
    @test slice["family_bucket_count"] > 0

    @test network["analysis_status"] == "ok"
    @test network["build_state"] == "complete"
    @test network["failure_classes"] == String[]
    @test network["failed_slice_count"] == 0
    @test network["successful_slice_count"] == 1

    @test length(BiocircuitsExplorerBackend._library_existing_ok_slice_ids(atlas)) == 1
    @test query_behavior_atlas(atlas, Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["A"],
        "limit" => 5,
    ))["result_count"] == 1

    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas.sqlite")
        atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="high_nullity_test")
        @test length(atlas_sqlite_existing_ok_slice_ids(sqlite_path)) == 1

        rerun = build_behavior_atlas_from_spec(Dict(
            "networks" => Any[HIGH_NULLITY_NETWORK],
            "behavior_config" => Dict(
                "compute_volume" => false,
                "include_path_records" => false,
                "min_volume_mean" => 0.0,
            ),
            "sqlite_path" => sqlite_path,
            "skip_existing" => true,
        ))
        @test rerun["skipped_existing_slice_count"] == 1
    end
end

@testset "Nullity-One Vertices Expose H0 While Higher Nullity Stays Guarded" begin
    model, _, _, _ = BiocircuitsExplorerBackend.build_model(["A + B <-> AB"], [1.0])
    find_all_vertices!(model)

    nullity_one_idx = first(filter(i -> get_nullity(model, i) == 1, 1:n_vertices(model)))
    H, H0 = get_H_H0(model, nullity_one_idx)
    exprs = show_expression_x(model, nullity_one_idx; log_space=true)

    @test size(H) == (3, 3)
    @test length(H0) == 3
    @test !isempty(H0)
    @test length(exprs) == 3

    high_model, _, _, _ = BiocircuitsExplorerBackend.build_model(Vector{String}(HIGH_NULLITY_NETWORK["reactions"]), ones(length(HIGH_NULLITY_NETWORK["reactions"])))
    find_all_vertices!(high_model)
    high_nullity_idx = first(filter(i -> get_nullity(high_model, i) > 1, 1:n_vertices(high_model)))
    @test_logs (:error, r"nullity is bigger than 1") isnothing(get_H_H0(high_model, high_nullity_idx))
end

@testset "High-Nullity qK Conditions Render Nonempty And Cleanly" begin
    high_model, _, _, _ = BiocircuitsExplorerBackend.build_model(Vector{String}(HIGH_NULLITY_NETWORK["reactions"]), ones(length(HIGH_NULLITY_NETWORK["reactions"])))
    find_all_vertices!(high_model)
    high_nullity_idx = first(filter(i -> get_nullity(high_model, i) > 1, 1:n_vertices(high_model)))

    cond_log = show_condition_qK(high_model, high_nullity_idx)
    cond_lin = show_condition_qK(high_model, high_nullity_idx; log_space=false)

    @test get_nullity(high_model, high_nullity_idx) == 2
    @test !isempty(cond_log)
    @test !isempty(cond_lin)
    @test all(c -> !occursin(".0", string(c)), cond_lin)
end

@testset "Leading Singular Tokens Deduplicate Without Crashing" begin
    scalar_path = [NaN, NaN, 1.0, 1.0, NaN, 0.0, 0.0]
    @test isequal(BindingAndCatalysis._dedup(scalar_path), [NaN, 1.0, NaN, 0.0])
    @test isequal(BindingAndCatalysis._dedup([NaN, NaN]), [NaN])

    vector_path = [
        [NaN, NaN],
        [NaN, NaN],
        [1.0, 0.0],
        [1.0, 0.0],
        [NaN, NaN],
        [0.0, -1.0],
        [0.0, -1.0],
    ]
    @test isequal(BindingAndCatalysis._dedup(vector_path), [
        [NaN, NaN],
        [1.0, 0.0],
        [NaN, NaN],
        [0.0, -1.0],
    ])
    @test isequal(BindingAndCatalysis._dedup([[NaN, NaN], [NaN, NaN]]), [[NaN, NaN]])
end

@testset "Empty Path Polyhedra Do Not Crash Complex-Growth Materialization" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[EMPTY_PATH_REGRESSION_NETWORK],
        "search_profile" => Dict(
            "mode" => "complex_growth_binding",
            "allow_homomeric_templates" => true,
            "allow_higher_order_templates" => true,
            "require_complex_growth_template" => true,
            "max_support" => 8,
            "input_mode" => "totals_only",
            "max_reactions" => 5,
        ),
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0

    network = only(atlas["network_entries"])
    @test network["analysis_status"] == "ok"
    @test network["build_state"] == "complete"
    @test network["failure_classes"] == String[]
end

@testset "Orthant Change Slice Builds In Graph-Only Mode" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "path_scope" => "all",
            "compute_volume" => false,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["input_graph_slices"]) == 1
    @test length(atlas["behavior_slices"]) == 1
    @test length(atlas["path_records"]) >= 1

    graph_slice = only(atlas["input_graph_slices"])
    slice = only(atlas["behavior_slices"])
    first_path = first(atlas["path_records"])

    @test graph_slice["change_kind"] == "orthant"
    @test graph_slice["input_symbol"] == "+tA,+tB"
    @test graph_slice["graph_config"]["slice_mode"] == "change"
    @test graph_slice["graph_config"]["graph_schema_version"] == "orthant_v0"

    @test slice["analysis_status"] == "ok"
    @test slice["input_symbol"] == "+tA,+tB"
    @test slice["change_kind"] == "orthant"
    @test slice["feasibility_mode"] == "graph_only_unchecked"
    @test any(token -> occursin("(", token), slice["regime_token_union"])

    @test first_path["feasibility_checked"] == false
    @test first(first_path["exact_profile"]) isa AbstractVector
end

@testset "Orthant Change Slice Supports Feasible Filtering" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    first_path = first(atlas["path_records"])
    @test slice["analysis_status"] == "ok"
    @test slice["feasibility_mode"] == "projected_feasible"
    @test slice["feasible_paths"] >= 1
    @test first_path["feasibility_checked"] == true
end

@testset "Orthant Change Slice Supports Volume Filtering" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => true,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    first_path = first(atlas["path_records"])
    @test slice["analysis_status"] == "ok"
    @test slice["feasibility_mode"] == "projected_feasible"
    @test slice["included_paths"] >= 1
    @test first_path["feasibility_checked"] == true
    @test first_path["volume"] !== nothing
end

@testset "Orthant Change Slice Supports Robust Filtering" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "path_scope" => "robust",
            "compute_volume" => true,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    first_path = first(atlas["path_records"])
    @test slice["analysis_status"] == "ok"
    @test slice["included_paths"] >= 1
    @test first_path["volume"] !== nothing
end

@testset "Orthant Witness Materialization And Refinement Degrade Gracefully" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    query = Dict(
        "change_signatures" => Any["orthant(+tA,+tB)"],
        "output_symbols" => Any["AB"],
        "require_witness_feasible" => true,
        "limit" => 5,
    )
    result = query_behavior_atlas(atlas, query)
    gamma_q = compile_query(query, atlas_search_profile_binding_small_v0())
    refinement = BiocircuitsExplorerBackend.inverse_refinement_spec_from_raw(Dict(
        "enabled" => true,
        "top_k" => 1,
        "trials" => 1,
        "n_points" => 21,
        "include_traces" => false,
    ))
    refined = refine_top_k(result, gamma_q, refinement)

    @test result["result_count"] == 1
    @test result["results"][1]["change_signature"] == "orthant(+tA,+tB)"
    @test result["results"][1]["best_witness_path"] !== nothing
    @test refined["evaluated_count"] == 1
    @test refined["results"][1]["change_signature"] == "orthant(+tA,+tB)"
    @test refined["results"][1]["refinement_status"] == "unsupported_multidimensional_refinement"
end

@testset "Refresh Demotes Historical Incomplete Slice" begin
    library = atlas_library_default()
    library["network_entries"] = Dict{String, Any}[
        Dict(
            "network_id" => "demo_network",
            "analysis_status" => "ok",
            "build_state" => "complete",
            "base_species_count" => 2,
            "reaction_count" => 1,
            "max_support" => 2,
            "support_mass" => 1,
            "raw_rules" => Any["A + B <-> AB"],
            "source_label" => "historical_bad",
            "source_kind" => "explicit",
        ),
    ]
    library["behavior_slices"] = Dict{String, Any}[
        Dict(
            "slice_id" => "demo_network::input=tA::output=A::cfg=test",
            "network_id" => "demo_network",
            "graph_slice_id" => "demo_network::graph_input=tA::graphcfg=siso_v0",
            "analysis_status" => "ok",
            "input_symbol" => "tA",
            "output_symbol" => "A",
            "classifier_config" => Dict("path_scope" => "feasible"),
            "total_paths" => 3,
            "feasible_paths" => 3,
            "included_paths" => 3,
            "motif_union" => Any["monotone_activation"],
            "exact_union" => Any["1"],
        ),
    ]

    refreshed = BiocircuitsExplorerBackend._refresh_atlas_library!(library)
    slice = only(refreshed["behavior_slices"])
    network = only(refreshed["network_entries"])

    @test slice["analysis_status"] == "failed"
    @test slice["build_state"] == "partial_failed"
    @test slice["failure_class"] == "incomplete_slice_records"
    @test slice["failure_stage"] == "slice_record_materialization"
    @test slice["partial_result_available"] == true
    @test "missing_regime_records" in slice["integrity_issues"]
    @test "missing_family_buckets" in slice["integrity_issues"]
    @test isempty(BiocircuitsExplorerBackend._library_existing_ok_slice_ids(refreshed))

    @test network["analysis_status"] == "failed"
    @test network["build_state"] == "failed"
    @test network["failed_slice_count"] == 1
    @test network["successful_slice_count"] == 0

    result = query_behavior_atlas(refreshed, Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["A"],
        "limit" => 5,
    ))
    @test result["result_count"] == 0
end
