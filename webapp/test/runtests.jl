using Test
using BiocircuitsExplorerBackend
using Logging

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

const HIGH_NULLITY_NETWORK = Dict(
    "label" => "step_trimer",
    "reactions" => Any[
        "A + B <-> C_A_B",
        "C_A_B + C <-> C_A_B_C",
    ],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["A"],
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

@testset "High Nullity Slices Are Marked And Not Reused" begin
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[HIGH_NULLITY_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 0
    @test atlas["failed_network_count"] == 1
    @test length(atlas["behavior_slices"]) == 1
    @test length(atlas["regime_records"]) == 0
    @test length(atlas["transition_records"]) == 0
    @test length(atlas["family_buckets"]) == 0

    slice = only(atlas["behavior_slices"])
    network = only(atlas["network_entries"])

    @test slice["analysis_status"] == "failed"
    @test slice["build_state"] == "partial_failed"
    @test slice["failure_class"] == "unsupported_high_nullity"
    @test slice["failure_stage"] == "slice_record_materialization"
    @test slice["partial_result_available"] == true
    @test slice["regime_record_count"] == 0
    @test slice["family_bucket_count"] == 0
    @test "missing_regime_records" in slice["integrity_issues"]
    @test "missing_family_buckets" in slice["integrity_issues"]
    @test occursin("atlas_nullity_gt_1", slice["error"])

    @test network["analysis_status"] == "failed"
    @test network["build_state"] == "failed"
    @test network["failure_classes"] == ["unsupported_high_nullity"]
    @test network["failed_slice_count"] == 1
    @test network["successful_slice_count"] == 0

    @test isempty(BiocircuitsExplorerBackend._library_existing_ok_slice_ids(atlas))
    @test query_behavior_atlas(atlas, Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["A"],
        "limit" => 5,
    ))["result_count"] == 0

    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas.sqlite")
        atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="high_nullity_test")
        @test isempty(atlas_sqlite_existing_ok_slice_ids(sqlite_path))

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
        @test rerun["skipped_existing_slice_count"] == 0
    end
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
