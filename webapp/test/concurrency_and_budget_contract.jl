using Test
using HTTP
using JSON3

@testset "Shared model state has explicit ownership and single-flight" begin
    backend = BiocircuitsExplorerBackend
    MC = backend.ModelCache
    SS = backend.SessionStore

    # Session and model TTLs belong to their own aliases/entries, not to the
    # shared payload Dict.
    SS._clear_all!()
    shared = Dict{String, Any}("payload" => true)
    SS.set_session("active", shared)
    SS.set_session("idle", shared)
    lock(SS._LOCK) do
        SS._SESSION_LAST_ACCESS["active"] = 99.0
        SS._SESSION_LAST_ACCESS["idle"] = 0.0
    end
    evicted_sessions = SS.cleanup_expired_sessions!(ttl=10, now_epoch=100)
    @test Set(evicted_sessions) == Set(["idle"])
    @test SS.get_session("active") === shared
    @test SS.get_session("idle") === nothing

    MC._clear_all!()
    MC.put_model("active", shared)
    MC.put_model("idle", shared)
    lock(MC._LOCK) do
        MC._MODEL_LAST_ACCESS["active"] = 99.0
        MC._MODEL_LAST_ACCESS["idle"] = 0.0
    end
    evicted_models = MC.cleanup_expired_models!(ttl=10, now_epoch=100)
    @test Set(evicted_models) == Set(["idle"])
    @test MC.get_model("active") === shared
    @test MC.get_model("idle") === nothing

    # Concurrent misses for one NetworkIR return the exact same published
    # bundle, rather than split bundles with unrelated locks/caches.
    MC._clear_all!()
    network = parse_network_ir(Dict(
        "reactions" => ["P6A + P6B <-> P6AB"],
        "kd" => [1.0],
    ))
    gate = Base.Event()
    tasks = [Threads.@spawn begin
        wait(gate)
        backend.build_model_bundle(network)
    end for _ in 1:8]
    notify(gate)
    bundles = fetch.(tasks)
    @test all(bundle -> bundle === bundles[1], bundles)
    @test MC.model_count() == 1

    # A request keeps the exact bundle it locked even if cache maintenance
    # evicts that hash before the handler performs its second resolution.
    backend.with_model_bundle_lock(bundles[1]) do
        task_local_storage(backend._REQUEST_MODEL_BUNDLE_TLS_KEY, bundles[1]) do
            MC._clear_models!()
            @test backend.build_model_bundle(network) === bundles[1]
            @test backend.resolve_model_bundle(Dict("network" => network_ir_to_dict(network))) === bundles[1]
        end
    end

    # The per-bundle lock covers the complete operation, not individual Dict
    # lookups. A second task cannot enter while the first is paused inside it.
    entered = Channel{Int}(2)
    release_first = Base.Event()
    first = Threads.@spawn backend.with_model_bundle_lock(bundles[1]) do
        put!(entered, 1)
        wait(release_first)
    end
    @test take!(entered) == 1
    second = Threads.@spawn backend.with_model_bundle_lock(bundles[1]) do
        put!(entered, 2)
    end
    yield()
    @test !isready(entered)
    notify(release_first)
    fetch(first)
    fetch(second)
    @test take!(entered) == 2
end

@testset "Web SISO path limits map engine allocation stops to 422 budgets" begin
    backend = BiocircuitsExplorerBackend
    err = BindingAndCatalysis.PathEnumerationLimitExceeded(:paths, 2_000, 2_001)
    @test_throws backend.SyncBudgetExceeded backend._path_budget_exceeded(err)
    hard_error = try
        backend._path_materialization_hard_bound_exceeded(
            err; label="ROP shape job")
        nothing
    catch caught
        caught
    end
    @test hard_error isa ArgumentError
    @test occursin("materialization hard bound", sprint(showerror, hard_error))
    @test occursin("does not establish scientific infeasibility",
                   sprint(showerror, hard_error))
    @test backend.MAX_WEB_REGIME_PATHS == 2_000
    @test backend.MAX_WEB_MATERIALIZED_PATH_NODES == 200_000
    @test !backend._in_sync_request_context()
    backend.with_sync_work_gate(:handle_siso_paths) do
        @test backend._in_sync_request_context()
        # Julia tasks do not inherit task-local storage. Atlas constructs its
        # change paths before spawning output workers; keep this behavior
        # explicit so future parallelization propagates the policy on purpose.
        @test !fetch(Threads.@spawn backend._in_sync_request_context())
        worker_contexts = backend._run_network_jobs_parallel(
            _ -> backend._in_sync_request_context(), [1, 2], 2)
        @test all(worker_contexts)
        worker_error = try
            backend._run_network_jobs_parallel([1, 2], 2) do _
                backend._sync_budget_exceeded("worker budget probe")
            end
            nothing
        catch err
            err
        end
        @test worker_error isa backend.SyncBudgetExceeded
    end
    @test !backend._in_sync_request_context()
end

@testset "Design index cold load is single-flight" begin
    backend = BiocircuitsExplorerBackend
    @test backend._design_normalize_design_target("sign", "++--+") == ("sign", "+-+")
    @test backend._design_normalize_design_target("exact", [1, 1.0, 0, -1, -1]) ==
          ("exact", [1.0, 0.0, -1.0])
    @test backend._design_normalize_design_target("exact", [-0.0]) == ("exact", [0.0])
    count_before_invalid = backend._DESIGN_INDEX_LOAD_COUNT[]
    @test_throws ErrorException backend._design_normalize_design_target("sign", "+oops")
    @test backend._DESIGN_INDEX_LOAD_COUNT[] == count_before_invalid

    old_path = get(ENV, "BNE_DESIGN_INDEX", nothing)
    old_state = backend._DESIGN_INDEX_STATE[]
    old_count = backend._DESIGN_INDEX_LOAD_COUNT[]

    mktempdir() do dir
        jsonl = joinpath(dir, "slices.jsonl")
        gzip_path = jsonl * ".gz"
        record = Dict(
            "nid" => "[1]+[2]<->[1,2]",
            "inp" => "tA",
            "out" => "A",
            "d" => 2,
            "r" => 1,
            "mu" => 2,
            "exact" => ["1"],
            "base_motifs" => ["monotone_activation"],
        )
        open(jsonl, "w") do io
            write(io, JSON3.write(record), '\n')
        end
        run(pipeline(`gzip -c $jsonl`, stdout=gzip_path))

        try
            ENV["BNE_DESIGN_INDEX"] = gzip_path
            lock(backend._DESIGN_INDEX_LOCK) do
                backend._DESIGN_INDEX_STATE[] = nothing
            end
            gate = Base.Event()
            tasks = [Threads.@spawn begin
                wait(gate)
                backend._load_design_index()
            end for _ in 1:8]
            notify(gate)
            results = fetch.(tasks)
            @test all(result -> result === results[1], results)
            @test length(results[1]) == 1
            @test backend._DESIGN_INDEX_LOAD_COUNT[] == old_count + 1
        finally
            lock(backend._DESIGN_INDEX_LOCK) do
                backend._DESIGN_INDEX_STATE[] = old_state
                backend._DESIGN_INDEX_LOAD_COUNT[] = old_count
            end
            if old_path === nothing
                pop!(ENV, "BNE_DESIGN_INDEX", nothing)
            else
                ENV["BNE_DESIGN_INDEX"] = old_path
            end
        end
    end
end

@testset "Synchronous API work fails closed before expensive compute" begin
    backend = BiocircuitsExplorerBackend
    entered = Channel{Nothing}(2)
    release = Base.Event()
    holders = [Threads.@spawn backend.with_sync_work_gate(:handle_design_screen) do
        put!(entered, nothing)
        wait(release)
    end for _ in 1:2]
    take!(entered)
    take!(entered)
    @test_throws backend.SyncCapacityExceeded backend.with_sync_work_gate(
        () -> nothing, :handle_design_screen)
    capacity_error = try
        backend.with_sync_work_gate(() -> nothing, :handle_design_screen)
        nothing
    catch err
        err
    end
    @test capacity_error isa backend.SyncCapacityExceeded
    @test !occursin("/api/v1/jobs", sprint(showerror, capacity_error))
    capacity_response = router(HTTP.Request(
        "POST",
        "/api/v1/design_screen",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("target_kind" => "sign", "target" => "+")),
    ))
    @test capacity_response.status == 429
    capacity_body = JSON3.read(String(capacity_response.body))
    @test capacity_body["code"] == "sync_capacity_exhausted"
    @test capacity_body["retryable"] === true
    @test capacity_body["retry_after_seconds"] == 1
    @test HTTP.header(capacity_response, "Retry-After") == "1"
    @test occursin("Retry-After",
        HTTP.header(capacity_response, "Access-Control-Expose-Headers"))
    notify(release)
    fetch.(holders)

    @test backend.sync_bounded_int(64.0, "candidate_budget"; max=64) == 64
    @test_throws ArgumentError backend.sync_bounded_int(64.5, "candidate_budget"; max=64)
    @test_throws ArgumentError backend.sync_bounded_int(NaN, "candidate_budget"; max=64)
    @test_throws ArgumentError backend.sync_bounded_int(Inf, "candidate_budget"; max=64)
    @test_throws ArgumentError backend.sync_bounded_int(true, "candidate_budget"; max=64)
    @test_throws backend.SyncBudgetExceeded backend.sync_bounded_int(
        big(typemax(Int)) + 1, "candidate_budget"; max=64)

    oversized_budget = HTTP.Request(
        "POST",
        "/api/v1/design_screen",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "target_kind" => "sign",
            "target" => "+",
            "candidate_budget" => Dict("max_screened" => 65),
        )),
    )
    response = router(oversized_budget)
    @test response.status == 422
    @test occursin("/api/v1/jobs", String(response.body))
    oversized_budget_body = JSON3.read(String(response.body))
    @test oversized_budget_body["code"] == "sync_budget_exceeded"
    @test oversized_budget_body["retryable"] === false

    network = Dict("reactions" => ["A + B <-> AB"], "kd" => [1.0])
    post(path, body) = router(HTTP.Request(
        "POST", path, ["Content-Type" => "application/json"], JSON3.write(body)))

    enumeration_spec = Dict("enumeration" => Dict("mode" => "pairwise_binding"))
    enumerating_requests = (
        "/api/v1/build_atlas" => enumeration_spec,
        "/api/v1/query_atlas" => Dict("query" => Dict(), "atlas_spec" => enumeration_spec),
        "/api/v1/build_atlas_library" => Dict("atlas_spec" => enumeration_spec),
        "/api/v1/merge_atlas_library" => Dict(
            "library" => atlas_library_default(), "atlas_spec" => enumeration_spec),
        "/api/v1/run_inverse_design" => Dict(
            "query" => Dict(), "atlas_spec" => enumeration_spec),
    )
    for (path, body) in enumerating_requests
        rejected = post(path, body)
        @test rejected.status == 422
        @test occursin("/api/v1/jobs", String(rejected.body))
    end

    explicit_networks = Any[
        Dict("label" => "bounded-$idx", "reactions" => ["A$idx + B$idx <-> AB$idx"])
        for idx in 1:(backend.MAX_SYNC_ATLAS_NETWORKS + 1)
    ]
    @test post("/api/v1/build_atlas", Dict("networks" => explicit_networks)).status == 422
    six_rule_network = Dict("reactions" => [
        "A$idx + B$idx <-> AB$idx" for idx in 1:(backend.MAX_SYNC_REACTIONS + 1)
    ])
    @test post("/api/v1/build_atlas", Dict("networks" => Any[six_rule_network])).status == 422
    @test post("/api/v1/build_atlas", Dict(
        "networks" => Any[network],
        "behavior_config" => Dict("compute_volume" => true),
    )).status == 422
    @test post("/api/v1/build_atlas", Dict(
        "networks" => Any[network],
        "behavior_config" => Dict("include_path_records" => true),
    )).status == 422
    @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(Dict(
        "networks" => Any[network],
        "network_parallelism" => true,
    ), :handle_build_atlas)
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(Dict(
        "networks" => Any[network],
        "change_expansion" => Dict("mode" => "orthant", "limit_per_network" => 0),
    ), :handle_build_atlas)
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(Dict(
        "networks" => Any[network],
        "search_profile" => Dict("max_base_species" => 5),
    ), :handle_build_atlas)
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(Dict(
        "networks" => Any[network],
        "search_profile" => Dict("allow_higher_order_templates" => true),
    ), :handle_build_atlas)
    @test_throws backend.SyncBudgetExceeded backend.sync_change_expansion_candidate_bound(
        24, :orthant, 4, true, true)
    axis_model, _, _, _ = backend.ReactionParser.build_model(
        String.(network["reactions"]), Float64.(network["kd"]))
    axis_profile = atlas_search_profile_binding_small_v0()
    axis_expansion = backend.atlas_change_expansion_spec_from_raw(Dict(
        "mode" => "axes_only",
        "include_negative_directions" => true,
        "limit_per_network" => 1,
    ))
    actual_axis_changes = backend._resolve_change_specs(
        Dict("input_symbols" => Any["tA", "tB"]),
        axis_model,
        axis_profile,
        axis_expansion,
    )
    @test length(actual_axis_changes) == 4
    @test backend.sync_change_expansion_candidate_bound(
        2, :axes_only, 1, true, true) == length(actual_axis_changes)
    for invalid_network in (
        merge(network, Dict("input_symbols" => Any["tA", "tA"])),
        merge(network, Dict("output_symbols" => Any["AB", 1])),
        merge(network, Dict("change_specs" => Any[Dict(
            "kind" => "axis",
            "qk_symbols" => Any["tA", "tB"],
            "qk_signs" => Any[1],
        )])),
        merge(network, Dict("change_specs" => Any[Dict(
            "kind" => "orthant",
            "qk_symbols" => ["t$idx" for idx in 1:5],
        )])),
    )
        err = try
            backend.enforce_sync_atlas_request_budget(
                Dict("networks" => Any[invalid_network]), :handle_build_atlas)
            nothing
        catch caught
            caught
        end
        @test err isa ArgumentError || err isa backend.SyncBudgetExceeded
    end

    # A built corpus may itself retain enumeration provenance. Referencing it
    # must not be mistaken for requesting fresh synchronous enumeration.
    referenced_atlas = Dict(
        "network_entries" => Any[],
        "input_graph_slices" => Any[],
        "behavior_slices" => Any[],
        "regime_records" => Any[],
        "transition_records" => Any[],
        "family_buckets" => Any[],
        "path_records" => Any[],
        "enumeration" => Dict("generated_network_count" => 100),
    )
    @test backend.enforce_sync_atlas_request_budget(referenced_atlas) === referenced_atlas
    for reference in (
        Dict("atlas" => referenced_atlas),
        Dict("library" => atlas_library_default()),
        Dict("sqlite_path" => "reference-only.sqlite"),
        Dict("atlas_spec" => referenced_atlas),
    )
        @test backend.enforce_sync_atlas_request_budget(reference) === reference
    end

    mktempdir() do dir
        sqlite_path = joinpath(dir, "bounded.sqlite")
        atlas_sqlite_save_library!(sqlite_path, atlas_library_default())
        mtime_before = stat(sqlite_path).mtime
        sqlite_query = Dict("sqlite_path" => sqlite_path, "query" => Dict("limit" => 1))
        @test backend.enforce_sync_atlas_request_budget(
            sqlite_query, :handle_query_atlas) === sqlite_query
        @test stat(sqlite_path).mtime == mtime_before
        @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
            Dict("sqlite_path" => sqlite_path, "networks" => Any[]),
            :handle_build_atlas,
        )
        @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
            Dict("sqlite_path" => joinpath(dir, "new.sqlite"), "networks" => Any[]),
            :handle_build_atlas,
        )
        @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
            Dict("sqlite_path" => joinpath(dir, "missing.sqlite"),
                 "query" => Dict("limit" => 1)),
            :handle_query_atlas,
        )

        unknown_path = joinpath(dir, "unknown.sqlite")
        unknown_db = backend.SQLite.DB(unknown_path)
        try
            backend.DBInterface.execute(unknown_db, "CREATE TABLE unrelated (id INTEGER)")
        finally
            backend.SQLite.close(unknown_db)
        end
        unknown_size = filesize(unknown_path)
        @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
            Dict("sqlite_path" => unknown_path, "query" => Dict("limit" => 1)),
            :handle_query_atlas,
        )
        @test filesize(unknown_path) == unknown_size
    end

    bounded_query = Dict(
        "atlas" => referenced_atlas,
        "query" => Dict("limit" => backend.MAX_SYNC_ATLAS_QUERY_RESULTS),
    )
    @test backend.enforce_sync_atlas_request_budget(
        bounded_query, :handle_query_atlas) === bounded_query
    for invalid_limit in (true, 1.5, NaN, Inf)
        @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
            Dict("atlas" => referenced_atlas, "query" => Dict("limit" => invalid_limit)),
            :handle_query_atlas,
        )
    end
    for unbounded_limit in (0, -1)
        @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
            Dict("atlas" => referenced_atlas, "query" => Dict("limit" => unbounded_limit)),
            :handle_query_atlas,
        )
    end
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => referenced_atlas,
             "query" => Dict("limit" => backend.MAX_SYNC_ATLAS_QUERY_RESULTS + 1)),
        :handle_query_atlas,
    )
    for malformed_query in (
        Dict("limit" => 1, "required_regimes" => Any[Dict("singular" => 2)]),
        Dict("limit" => 1, "required_regimes" => Any[Dict("nullity" => 1.5)]),
        Dict("limit" => 1, "required_transitions" => Any[
            Dict("from" => Dict("source" => 1), "to" => Dict("sink" => true)),
        ]),
    )
        @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
            Dict("atlas" => referenced_atlas, "query" => malformed_query),
            :handle_query_atlas,
        )
    end
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict(
            "atlas" => referenced_atlas,
            "query" => Dict(
                "limit" => 1,
                "required_regimes" => Any[Dict(
                    "output_order_token" => collect(
                        1:(backend.MAX_SYNC_ATLAS_QUERY_TOKEN_LENGTH + 1)),
                )],
            ),
        ),
        :handle_query_atlas,
    )
    deeply_nested_query = Dict{String, Any}("limit" => 1)
    nested_cursor = deeply_nested_query
    for _ in 1:140
        child = Dict{String, Any}()
        nested_cursor["nested"] = child
        nested_cursor = child
    end
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => referenced_atlas, "query" => deeply_nested_query),
        :handle_query_atlas,
    )
    for malformed_support in (
        Dict("min_counts" => Dict("A" => 1.5)),
        Dict("max_counts" => Dict("A" => true)),
        Dict("min_counts" => Dict("A" => 2), "max_counts" => Dict("A" => 1)),
        Dict("required_species" => "A"),
        Dict("allowed_species" => Any[1]),
    )
        @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
            Dict(
                "atlas" => referenced_atlas,
                "query" => Dict("limit" => 1, "support_count_spec" => malformed_support),
            ),
            :handle_query_atlas,
        )
    end
    @test post("/api/v1/query_atlas", Dict(
        "atlas" => referenced_atlas,
        "query" => Dict(
            "limit" => 1,
            "support_count_spec" => Dict("min_counts" => Dict("A" => 1.5)),
        ),
    )).status == 400
    @test post("/api/v1/run_inverse_design", Dict(
        "library" => atlas_library_default(),
        "query" => Dict(
            "limit" => 1,
            "required_regimes" => Any[Dict("singular" => 2)],
        ),
        "inverse_design" => Dict("return_library" => false),
    )).status == 400
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict(
            "atlas" => referenced_atlas,
            "query" => Dict("limit" => 1, "require_witness_feasible" => true),
        ),
        :handle_query_atlas,
    )
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict(
            "atlas" => referenced_atlas,
            "query" => Dict(
                "limit" => 1,
                "support_count_spec" => Dict(
                    "min_counts" => Dict("S$idx" => 1 for idx in
                        1:backend.MAX_SYNC_ATLAS_QUERY_VALUE_NODES),
                ),
            ),
        ),
        :handle_query_atlas,
    )

    oversized_corpus = merge(referenced_atlas, Dict(
        "behavior_slices" => fill(Dict{String, Any}(),
            backend.MAX_SYNC_ATLAS_QUERY_SLICES + 1),
    ))
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => oversized_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )
    record_heavy_corpus = merge(referenced_atlas, Dict(
        "path_records" => fill(Dict{String, Any}(),
            backend.MAX_SYNC_ATLAS_QUERY_RECORDS + 1),
    ))
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => record_heavy_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )
    malformed_corpus = merge(referenced_atlas, Dict(
        "network_entries" => Any[Dict(
            "network_id" => true,
            "analysis_status" => "ok",
            "raw_rules" => Any["A + B <-> AB"],
        )],
    ))
    @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => malformed_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )
    malformed_status_corpus = merge(referenced_atlas, Dict(
        "network_entries" => Any[Dict("analysis_status" => true)],
    ))
    @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => malformed_status_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )
    overflowing_count_corpus = merge(referenced_atlas, Dict(
        "behavior_slices" => Any[Dict(
            "slice_id" => "overflowing-count",
            "path_count" => 1.0e100,
        )],
    ))
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => overflowing_count_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )
    high_profile_lhs = join(["HP$idx" for idx in 1:19], " + ")
    high_profile_rules = ["$high_profile_lhs <-> HPP$idx" for idx in 1:5]
    high_profile_corpus = merge(referenced_atlas, Dict(
        "network_entries" => Any[Dict(
            "network_id" => "synthetic-high-profile",
            "analysis_status" => "ok",
            "raw_rules" => high_profile_rules,
        )],
    ))
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("atlas" => high_profile_corpus, "query" => Dict("limit" => 1)),
        :handle_query_atlas,
    )

    inverse_base = Dict(
        "library" => atlas_library_default(),
        "query" => Dict("limit" => 10),
        "inverse_design" => Dict("return_library" => false),
    )
    @test backend.enforce_sync_atlas_request_budget(
        inverse_base, :handle_run_inverse_design) === inverse_base
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        merge(inverse_base, Dict("refinement" => Dict("enabled" => true))),
        :handle_run_inverse_design,
    )
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("library" => atlas_library_default(), "query" => Dict("limit" => 10)),
        :handle_run_inverse_design,
    )
    @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
        merge(inverse_base, Dict("inverse_design" => Dict("return_library" => 1))),
        :handle_run_inverse_design,
    )
    for refinement in (
        Dict("enabled" => true, "top_k" => 0),
        Dict("enabled" => true, "top_k" => backend.MAX_SYNC_INVERSE_TOP_K + 1),
        Dict("enabled" => true, "top_k" => true),
        Dict("enabled" => true, "trials" => backend.MAX_SYNC_INVERSE_TRIALS + 1),
        Dict("enabled" => true, "n_points" => backend.MAX_SYNC_INVERSE_POINTS + 1),
        Dict("enabled" => true, "top_k" => backend.MAX_SYNC_INVERSE_TOP_K,
             "trials" => backend.MAX_SYNC_INVERSE_TRIALS,
             "n_points" => backend.MAX_SYNC_INVERSE_POINTS),
    )
        err = try
            backend.enforce_sync_atlas_request_budget(
                merge(inverse_base, Dict("refinement" => refinement)),
                :handle_run_inverse_design,
            )
            nothing
        catch caught
            caught
        end
        @test err isa ArgumentError || err isa backend.SyncBudgetExceeded
    end
    for refinement in (
        Dict("enabled" => true, "param_min" => NaN),
        Dict("enabled" => true, "param_min" => 1.0, "param_max" => 1.0),
        Dict("enabled" => true, "background_min" => true),
        Dict("enabled" => true, "flat_abs_tol" => -0.1),
        Dict("enabled" => true, "rng_seed" => 1.5),
        Dict("enabled" => true, "include_traces" => 1),
    )
        @test_throws ArgumentError backend.enforce_sync_atlas_request_budget(
            merge(inverse_base, Dict("refinement" => refinement)),
            :handle_run_inverse_design,
        )
    end

    @test post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "param_min" => 1.0,
        "param_max" => 1.0,
        "output_exprs" => ["AB"],
    )).status == 400
    @test post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "output_exprs" => fill("AB", 17),
    )).status == 422
    scan = post("/api/v1/parameter_scan_1d", Dict(
        "network" => network,
        "param_symbol" => "tA",
        "param_min" => -1.0,
        "param_max" => 1.0,
        "n_points" => 10,
        "output_exprs" => ["AB"],
    ))
    @test scan.status == 200
    scan_body = JSON3.read(String(scan.body))
    @test length(scan_body["valid"]) == 10
    @test scan_body["partial"] === false
    @test post("/api/v1/parameter_scan_2d", Dict(
        "network" => network,
        "param1_symbol" => "tA",
        "param2_symbol" => "tB",
        "output_expr" => "AB",
        "n_grid" => 80.5,
    )).status == 400
    @test post("/api/v1/rop_cloud", Dict(
        "network" => network,
        "n_samples" => 20_001,
    )).status == 422

    # Five reactions can still induce a huge 6^19 regime candidate product.
    # The free-species preflight now rejects this before exact canonicalization;
    # the capped canonical helper safely falls back when computing the IR hash.
    high_lhs = join(["S$idx" for idx in 1:19], " + ")
    high_rules = ["$high_lhs <-> P$idx" for idx in 1:5]
    high_network = Dict("reactions" => high_rules, "kd" => ones(5))
    high_ir = parse_network_ir(high_network)
    @test backend.canonical_network_code_or_nothing(high_rules) === nothing
    high_hash = network_ir_hash(high_ir)
    @test length(high_hash) == 64
    @test backend.ModelCache.get_model(high_hash) === nothing
    fake_explosive_model = (
        r=5,
        n=24,
        _L_helper=(J=[collect(1:6) for _ in 1:19],),
    )
    @test_throws backend.SyncBudgetExceeded backend.sync_model_candidate_bound(
        fake_explosive_model)
    job_candidate_error = try
        backend.model_candidate_bound(
            fake_explosive_model;
            maximum=backend.MAX_JOB_REGIME_CANDIDATES,
            label="ROP shape job",
        )
        nothing
    catch caught
        caught
    end
    @test job_candidate_error isa backend.ModelCandidateBoundExceeded
    @test occursin("enumeration was not started",
                   sprint(showerror, job_candidate_error))

    support_symbols = [Symbol("fallback_$idx") for idx in 1:8]
    support_validation = Dict(
        "supports" => Dict(symbol => Symbol[symbol] for symbol in support_symbols),
        "free_symbols" => String.(support_symbols),
    )
    support_signature_1 = backend._support_signature_from_validation(support_validation)
    support_signature_2 = backend._support_signature_from_validation(support_validation)
    @test startswith(support_signature_1, "support-positional::d=8::")
    @test support_signature_2 == support_signature_1
    @test_throws backend.SyncBudgetExceeded backend.enforce_sync_atlas_request_budget(
        Dict("networks" => Any[high_network]), :handle_build_atlas)
    @test post("/api/v1/build_atlas", Dict("networks" => Any[high_network])).status == 422
    @test post("/api/v1/find_vertices", Dict("network" => high_network)).status == 422
    @test backend.ModelCache.get_model(high_hash) === nothing

    # Each network is individually below the model candidate cap, but a single
    # atlas request cannot multiply that cost across several networks.
    aggregate_networks = Any[]
    for network_idx in 1:7
        lhs = join(["N$(network_idx)_S$idx" for idx in 1:6], " + ")
        push!(aggregate_networks, Dict(
            "reactions" => ["$lhs <-> N$(network_idx)_P$idx" for idx in 1:4],
            "kd" => ones(4),
        ))
    end
    aggregate_error = try
        backend.enforce_sync_atlas_request_budget(
            Dict("networks" => aggregate_networks), :handle_build_atlas)
        nothing
    catch err
        err
    end
    @test aggregate_error isa backend.SyncBudgetExceeded
    @test occursin("Combined explicit-network", sprint(showerror, aggregate_error))
    cache_keys_before = lock(backend.ModelCache._LOCK) do
        (Set(keys(backend.ModelCache._MODELS)), Set(keys(backend.ModelCache._IRS)))
    end
    @test post("/api/v1/build_atlas", Dict("networks" => aggregate_networks)).status == 422
    cache_keys_after = lock(backend.ModelCache._LOCK) do
        (Set(keys(backend.ModelCache._MODELS)), Set(keys(backend.ModelCache._IRS)))
    end
    @test cache_keys_after == cache_keys_before

    oversized_body = HTTP.Request(
        "POST",
        "/api/v1/design_search",
        ["Content-Type" => "application/json"],
        fill(UInt8('x'), BiocircuitsExplorerBackend.Serialization.MAX_JSON_REQUEST_BYTES + 1),
    )
    @test router(oversized_body).status == 413
end

@testset "Full SQLite merges serialize the complete read-modify-write" begin
    mktempdir() do dir
        sqlite_path = joinpath(dir, "concurrent-full.sqlite")
        atlas_a = build_behavior_atlas_from_spec(Dict("networks" => Any[Dict(
            "label" => "concurrent-a",
            "reactions" => Any["CA + CB <-> CAB"],
            "input_symbols" => Any["tCA"],
            "output_symbols" => Any["CAB"],
        )]))
        atlas_b = build_behavior_atlas_from_spec(Dict("networks" => Any[Dict(
            "label" => "concurrent-b",
            "reactions" => Any[
                "A + B <-> C_A_B",
                "C_A_B + C <-> C_A_B_C",
            ],
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["C_A_B_C"],
        )]))
        @test atlas_a["successful_network_count"] == 1
        @test atlas_b["successful_network_count"] == 1

        # Initialize schema and compile the snapshot writer before contention;
        # the assertion below is about the read-modify-write boundary itself.
        atlas_sqlite_save_library!(sqlite_path, atlas_library_default())

        db_a = BiocircuitsExplorerBackend.atlas_sqlite_connect(sqlite_path)
        db_b = BiocircuitsExplorerBackend.atlas_sqlite_connect(sqlite_path)
        try
            gate = Base.Event()
            writers = [Threads.@spawn begin
                wait(gate)
                atlas_sqlite_merge_atlas!(
                    db,
                    atlas;
                    source_label=label,
                    persist_mode=:full,
                )
            end for (db, atlas, label) in (
                (db_a, atlas_a, "writer-a"),
                (db_b, atlas_b, "writer-b"),
            )]
            notify(gate)
            fetch.(writers)

            # Public DB overloads remain composable inside a caller-owned
            # transaction; the inner full rewrite uses a savepoint.
            BiocircuitsExplorerBackend._atlas_sqlite_transaction(db_a) do
                atlas_sqlite_record_skip_only_event!(
                    db_a;
                    source_label="nested-skip",
                    skipped_existing_network_count=1,
                    persist_mode=:full,
                )
            end
        finally
            BiocircuitsExplorerBackend.SQLite.close(db_a)
            BiocircuitsExplorerBackend.SQLite.close(db_b)
        end

        library = atlas_sqlite_load_library(sqlite_path)
        @test length(library["atlas_manifests"]) == 2
        @test length(library["network_entries"]) == 2
        @test any(event -> get(event, "source_label", "") == "nested-skip",
                  library["merge_events"])
        @test Set(String(manifest["source_label"]) for manifest in library["atlas_manifests"]) ==
              Set(["writer-a", "writer-b"])
    end
end
