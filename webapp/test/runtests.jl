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
