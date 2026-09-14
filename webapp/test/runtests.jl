using Test
using BiocircuitsExplorerBackend
using BindingAndCatalysis
using Logging
using HTTP
using JSON3
using Base64

include("schema_generation_contract.jl")
include("api_contract.jl")
include("backend_assembly_contract.jl")
include("architecture_discovery_api.jl")
include("target_design_api.jl")
include("design_model_build_contract.jl")
include("concurrency_and_budget_contract.jl")
include("input_validation_contract.jl")
include("static_security_contract.jl")
include("sbml_validation_contract.jl")
include("numerical_validity_contract.jl")
include("latent_evaluator_validity_contract.jl")
include("rop_shape_replay_contract.jl")
include("rop_shape_optimization_contract.jl")
include("rop_shape_cat_benchmark.jl")
include("rop_shape_api_contract.jl")
include("rop_shape_schema_contract.jl")
include("target_design_schema_contract.jl")
include("ro_field_api_contract.jl")
include("ro_field_chunks_contract.jl")
include("ro_field_slices_contract.jl")
include("ro_field_campaign_contract.jl")
include("ro_field_job_contract.jl")
include("ro_field_sparse_job_contract.jl")
include("ro_field_differential_contract.jl")
include("ro_field_identity_sqlite_contract.jl")
include("ro_field_behavior_contract.jl")
include("ro_field_atlas_contract.jl")
include("ro_field_signature_sqlite_contract.jl")

function _configure_design_index_fixture!()
    dir = mktempdir()
    path = joinpath(dir, "design-index.jsonl")
    # This deliberately tiny synthetic fixture keeps Designability contracts
    # independent of any paper-side or workstation dataset.
    records = [
        Dict(
            "nid" => "[1,1]+[2]<->[1,1,2]|[1]+[1]<->[1,1]|[2,3]+[2]<->[2,2,3]|[2]+[3]<->[2,3]",
            "inp" => "tA", "out" => "C_A_A",
            "d" => 3, "r" => 4, "mu" => 3,
            "exact" => ["1"],
            "base_motifs" => ["monotone_activation", "multistage_activation"],
        ),
        Dict(
            "nid" => "[1]+[2]<->[1,2]", "inp" => "tA", "out" => "C_A_B",
            "d" => 2, "r" => 2, "mu" => 3,
            "exact" => ["1 → -1 → 1"],
            "base_motifs" => ["biphasic_peak"],
        ),
        Dict(
            "nid" => "[1]+[2]<->[1,2]", "inp" => "tA", "out" => "B",
            "d" => 2, "r" => 1, "mu" => 2,
            "exact" => ["0 → -1"],
            "base_motifs" => ["thresholded_repression"],
        ),
        Dict(
            "nid" => "[1]+[2]<->[1,2]", "inp" => "tA", "out" => "A",
            "d" => 2, "r" => 1, "mu" => 2,
            "exact" => ["1"],
            "base_motifs" => ["monotone_activation"],
        ),
    ]
    open(path, "w") do io
        for record in records
            write(io, JSON3.write(record), '\n')
        end
    end
    gzip_path = path * ".gz"
    run(pipeline(`gzip -c $path`, stdout=gzip_path))
    ENV["BNE_DESIGN_INDEX"] = gzip_path
    lock(BiocircuitsExplorerBackend._DESIGN_INDEX_LOCK) do
        BiocircuitsExplorerBackend._DESIGN_INDEX_STATE[] = nothing
    end
end

_configure_design_index_fixture!()

# Keep the Designability/Design Screen API contracts runnable from the main test
# entry as well as directly; these run before the older atlas @test_broken gate
# below.
include("designability_spec_contract.jl")
include("designability_cell_budget_contract.jl")
include("design_screen_contract.jl")
include("design_screen_schema_instance_contract.jl")
include("jobs_cancellation_contract.jl")
include("jobs_cache_concurrency_contract.jl")
include("jobs_artifact_validity_contract.jl")
include("cooperative_cancel_checkpoints_contract.jl")
include("inverse_config_contract.jl")
include("d1_atlas_contract.jl")

# Atlas-dependent tests execute unconditionally. An earlier regression probe
# (Task-15b) marked these tests @test_skip when an upstream SISOPaths field
# migration broke atlas builds; that migration is complete and a fail-open
# gate would hide future regressions. If an atlas build fails, the suite must
# fail instead of silently skipping the main scientific path.
#
# Helper: wraps an atlas-dependent testset body. Usage:
#   _atlas_test() do ... end inside a @testset.
function _atlas_test(f)
    f()
end

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

const CANONICAL_RELABEL_NETWORK = Dict(
    "label" => "canonical_relabel",
    "reactions" => Any[
        "A + B <-> C_A_B",
        "B + B <-> C_B_B",
        "C_A_B + B <-> C_A_B_B",
    ],
    "input_symbols" => Any["tB"],
    "output_symbols" => Any["C_A_B_B"],
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

function drain_and_reset_job_runtime!()
    tasks = lock(BiocircuitsExplorerBackend.JOBS_LOCK) do
        collect(values(BiocircuitsExplorerBackend.JOB_TASKS))
    end
    if !isempty(tasks)
        timedwait(() -> all(istaskdone, tasks), 60.0; pollint=0.01) == :ok ||
            error("Timed out draining local job tasks in test fixture.")
        for task in tasks
            try
                wait(task)
            catch err
                err isa TaskFailedException || rethrow()
            end
        end
    end
    lock(BiocircuitsExplorerBackend.JOBS_LOCK) do
        isempty(BiocircuitsExplorerBackend.LOCAL_JOB_ADMISSIONS) ||
            error("Local job admission reservations leaked across test fixtures.")
        empty!(BiocircuitsExplorerBackend.JOBS)
        empty!(BiocircuitsExplorerBackend.JOB_CACHE_LAST_ACCESS)
        BiocircuitsExplorerBackend.JOB_CACHE_ACCESS_CLOCK[] = UInt64(0)
        BiocircuitsExplorerBackend.JOB_CACHE_CAPACITY[] = nothing
        empty!(BiocircuitsExplorerBackend.JOB_TASKS)
        empty!(BiocircuitsExplorerBackend.LOCAL_JOB_CANCEL_TOKENS)
        empty!(BiocircuitsExplorerBackend.JOB_STATUS_PROJECTION_DIRTY)
        BiocircuitsExplorerBackend.LOCAL_JOB_LIMITS[] = nothing
        BiocircuitsExplorerBackend.LOCAL_JOB_RUN_SEMAPHORE[] = nothing
    end
    lock(BiocircuitsExplorerBackend.JOB_STORE_DURABILITY_LOCK) do
        empty!(BiocircuitsExplorerBackend.JOB_STORE_PENDING_DIR_FSYNC)
        BiocircuitsExplorerBackend.JOB_STORE_DURABILITY_GENERATION[] = UInt64(0)
    end
    return nothing
end

function with_isolated_job_store(f::Function)
    previous_env = haskey(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE") ? ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] : nothing
    previous_store_dir = BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[]

    mktempdir() do dir
        try
            drain_and_reset_job_runtime!()
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = nothing
            f(dir)
        finally
            drain_and_reset_job_runtime!()
            if previous_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_env
            end
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
        end
    end
end

function response_json(resp)
    return BiocircuitsExplorerBackend._materialize(JSON3.read(String(resp.body)))
end

function wait_for_job_terminal(job_id; attempts=120, interval=0.05)
    job = get_biocircuits_job(job_id)
    for _ in 1:attempts
        String(job["status"]) in ("succeeded", "failed", "cancelled") && return job
        sleep(interval)
        job = get_biocircuits_job(job_id)
    end
    return job
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
    DL = BiocircuitsExplorerBackend.DebugLog
    lock(DL.DEBUG_LOG_LOCK) do
        empty!(DL.DEBUG_LOGS)
        DL.DEBUG_LOG_SEQ[] = 0
    end

    logger = DL.BufferingConsoleLogger(ThrowingLogger(), Logging.Info)

    @test Logging.catch_exceptions(logger) == true
    @test_nowarn Logging.handle_message(logger, Logging.Info, "test message", @__MODULE__, :tests, :event, "file", 1)
    @test logger.console_forwarding_enabled[] == false

    lock(DL.DEBUG_LOG_LOCK) do
        @test any(entry -> get(entry, "message", "") == "test message", DL.DEBUG_LOGS)
        @test any(entry -> occursin("Console log forwarding disabled", get(entry, "message", "")), DL.DEBUG_LOGS)
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

@testset "Async Job API Runs Local Query Job" begin
    with_isolated_job_store() do store_dir
        spec = Dict(
            "library" => atlas_library_default(),
            "query" => Dict("limit" => 1),
        )
        request = HTTP.Request(
            "POST",
            "/api/v1/jobs",
            ["Content-Type" => "application/json"],
            JSON3.write(Dict(
                "kind" => "query_atlas",
                "execution" => Dict("mode" => "local_async"),
                "spec" => spec,
            )),
        )

        response = router(request)
        @test response.status == 202
        submitted = response_json(response)
        job_id = String(submitted["job_id"])

        @test submitted["executor"] == "local_async"
        @test submitted["kind"] == "query_atlas"
        @test isfile(joinpath(store_dir, job_id, "input.json"))

        finished = wait_for_job_terminal(job_id)
        @test finished["status"] == "succeeded"
        @test finished["result_available"] == true

        result_payload = get_biocircuits_job_result(job_id)
        @test result_payload["job"]["job_id"] == job_id
        @test result_payload["result"]["result_count"] == 0
        @test isfile(joinpath(store_dir, job_id, "result.json"))
    end
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
    withenv("BIOCIRCUITS_EXPLORER_PARENT_PID" => "0", "ROP_PARENT_PID" => "") do
        @test BiocircuitsExplorerBackend.configured_parent_pid() === nothing
    end
    withenv("BIOCIRCUITS_EXPLORER_PARENT_PID" => "-42", "ROP_PARENT_PID" => "") do
        @test BiocircuitsExplorerBackend.configured_parent_pid() === nothing
    end
end

@testset "Server Bind Host Defaults Safely" begin
    withenv(
        "BIOCIRCUITS_EXPLORER_HOST" => nothing,
        "ROP_HOST" => nothing,
    ) do
        @test BiocircuitsExplorerBackend.resolve_host(nothing) == "0.0.0.0"
        @test BiocircuitsExplorerBackend.resolve_host(4321) == "127.0.0.1"
    end

    withenv(
        "BIOCIRCUITS_EXPLORER_HOST" => "127.0.0.2",
        "ROP_HOST" => "127.0.0.3",
    ) do
        @test BiocircuitsExplorerBackend.resolve_host(nothing) == "127.0.0.2"
        @test BiocircuitsExplorerBackend.resolve_host(4321) == "127.0.0.2"
        @test BiocircuitsExplorerBackend.resolve_host(nothing) isa String
        @test BiocircuitsExplorerBackend.resolve_host(4321) isa String
    end

    withenv(
        "BIOCIRCUITS_EXPLORER_HOST" => nothing,
        "ROP_HOST" => "::1",
    ) do
        @test BiocircuitsExplorerBackend.resolve_host(nothing) == "::1"
        @test BiocircuitsExplorerBackend.resolve_host(nothing) isa String
    end

    withenv("BIOCIRCUITS_EXPLORER_HOST" => "http://127.0.0.1") do
        @test_throws ArgumentError BiocircuitsExplorerBackend.resolve_host(nothing)
    end
    withenv("BIOCIRCUITS_EXPLORER_HOST" => "127.0.0.1:8088") do
        @test_throws ArgumentError BiocircuitsExplorerBackend.resolve_host(nothing)
    end
end

@testset "Native Runtime Store Overrides Bundle-relative Atlas Default" begin
    mktempdir() do application_support_root
        atlas_root = joinpath(application_support_root, "Biocircuits Explorer", "Runtime", "Atlas")
        withenv("BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT" => atlas_root) do
            @test BiocircuitsExplorerBackend.atlas_sqlite_default_path() ==
                normpath(joinpath(atlas_root, "atlas.sqlite"))
        end
    end
end

@testset "Local Image Rejects Cross-Origin Browser Reads" begin
    mktempdir() do dir
        image_path = joinpath(dir, "local image.png")
        write(image_path, UInt8[0x89, 0x50, 0x4e, 0x47])
        target = "/api/v1/local-image?path=$(HTTP.escapeuri(image_path))"

        no_origin = HTTP.Request("GET", target, ["Host" => "127.0.0.1:18088"])
        @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
            no_origin;
            has_parent_pid=true,
        ).status == 200

        same_origin = HTTP.Request("GET", target, [
            "Host" => "127.0.0.1:18088",
            "Origin" => "http://127.0.0.1:18088",
        ])
        @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
            same_origin;
            has_parent_pid=true,
        ).status == 200

        cross_origin = HTTP.Request("GET", target, [
            "Host" => "127.0.0.1:18088",
            "Origin" => "https://attacker.example",
        ])
        @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
            cross_origin;
            has_parent_pid=true,
        ).status == 403

        wrong_port = HTTP.Request("GET", target, [
            "Host" => "127.0.0.1:18088",
            "Origin" => "http://127.0.0.1:8088",
        ])
        @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
            wrong_port;
            has_parent_pid=true,
        ).status == 403

        dns_rebinding_origin = HTTP.Request("GET", target, [
            "Host" => "attacker.example",
            "Origin" => "http://attacker.example",
        ])
        @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
            dns_rebinding_origin;
            has_parent_pid=true,
        ).status == 403

        withenv("BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES" => "1") do
            @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
                no_origin;
                has_parent_pid=false,
            ).status == 200
            @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
                dns_rebinding_origin;
                has_parent_pid=false,
            ).status == 200
        end

        withenv(
            "BIOCIRCUITS_EXPLORER_HOST" => "0.0.0.0",
            "ROP_HOST" => nothing,
            "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES" => nothing,
        ) do
            @test !BiocircuitsExplorerBackend.StaticAssets.local_images_enabled(
                has_parent_pid=true,
            )
            @test BiocircuitsExplorerBackend.StaticAssets.handle_local_image(
                no_origin;
                has_parent_pid=true,
            ).status == 403
        end

        withenv(
            "BIOCIRCUITS_EXPLORER_HOST" => "0.0.0.0",
            "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES" => "1",
        ) do
            @test BiocircuitsExplorerBackend.StaticAssets.local_images_enabled(
                has_parent_pid=true,
            )
        end
    end
end

@testset "Router Guards Static And API Errors" begin
    traversal = router(HTTP.Request("GET", "/../Project.toml"))
    malformed = router(HTTP.Request("POST", "/api/v1/build_model", ["Content-Type" => "application/json"], "{"))
    missing_field = router(HTTP.Request("POST", "/api/v1/build_model", ["Content-Type" => "application/json"], JSON3.write(Dict(
        "kd" => Any[1.0],
    ))))
    wrong_method = router(HTTP.Request("GET", "/api/v1/build_model"))

    @test traversal.status == 404
    @test malformed.status == 400
    @test occursin("Invalid JSON", String(malformed.body))
    @test missing_field.status == 400
    @test occursin("reactions", String(missing_field.body))
    @test wrong_method.status == 405
    @test occursin("Method not allowed", String(wrong_method.body))
end

@testset "Health and Readiness Probes" begin
    # /health: liveness, must be cheap and always 200 once the module is
    # initialized. We don't assert the exact version string because it
    # changes between releases, but we do assert the shape so a future
    # refactor that drops the field gets caught.
    health = router(HTTP.Request("GET", "/health"))
    @test health.status == 200
    health_body = JSON3.read(health.body)
    @test health_body["status"] == "ok"
    @test health_body["service"] == "biocircuits-explorer-backend"
    @test haskey(health_body, "instance_nonce")
    @test haskey(health_body, "version")
    @test haskey(health_body, "uptime_seconds")
    @test health_body["uptime_seconds"] isa Real
    @test health_body["uptime_seconds"] >= 0
    # HEAD is required by some health-check tools.
    @test router(HTTP.Request("HEAD", "/health")).status == 200
    # Wrong method must be a clean 405, not a 500.
    @test router(HTTP.Request("POST", "/health")).status == 405

    # /ready requires both browser and native entrypoints before a deployment
    # should admit traffic.
    ready = router(HTTP.Request("GET", "/ready"))
    @test ready.status == 200
    ready_body = JSON3.read(ready.body)
    @test ready_body["status"] == "ready"
    @test ready_body["service"] == "biocircuits-explorer-backend"
    @test haskey(ready_body, "instance_nonce")
    @test haskey(ready_body, "checks")
    @test haskey(ready_body["checks"], "module_initialized")
    @test haskey(ready_body["checks"], "static_assets")
    @test haskey(ready_body["checks"], "browser_entrypoint")
    @test haskey(ready_body["checks"], "native_entrypoint")
    @test haskey(ready_body["checks"], "job_store")
    @test ready_body["checks"]["module_initialized"] == true
    @test ready_body["checks"]["static_assets"] == true
    @test ready_body["checks"]["browser_entrypoint"] == true
    @test ready_body["checks"]["native_entrypoint"] == true
    @test ready_body["checks"]["job_store"] == true

    withenv("BIOCIRCUITS_EXPLORER_INSTANCE_NONCE" => "native-launch-nonce-0123456789abcdef") do
        identified_health = JSON3.read(router(HTTP.Request("GET", "/health")).body)
        identified_ready = JSON3.read(router(HTTP.Request("GET", "/ready")).body)
        @test identified_health["instance_nonce"] == "native-launch-nonce-0123456789abcdef"
        @test identified_ready["instance_nonce"] == "native-launch-nonce-0123456789abcdef"
    end

    previous_static_dir = BiocircuitsExplorerBackend.StaticAssets.STATIC_DIR[]
    mktempdir() do empty_static_dir
        try
            BiocircuitsExplorerBackend.StaticAssets.STATIC_DIR[] = empty_static_dir
            unready = router(HTTP.Request("GET", "/ready"))
            @test unready.status == 503
            unready_body = JSON3.read(unready.body)
            @test unready_body["status"] == "not_ready"
            @test unready_body["checks"]["static_assets"] == true
            @test unready_body["checks"]["browser_entrypoint"] == false
            @test unready_body["checks"]["native_entrypoint"] == false
            @test unready_body["checks"]["job_store"] == true
        finally
            BiocircuitsExplorerBackend.StaticAssets.STATIC_DIR[] = previous_static_dir
        end
    end

    previous_store_dir = BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[]
    mktempdir() do parent
        blocked_store = joinpath(parent, "blocked-store")
        mkdir(blocked_store)
        chmod(blocked_store, 0o500)
        try
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = blocked_store
            unready = router(HTTP.Request("GET", "/ready"))
            @test unready.status == 503
            unready_body = JSON3.read(unready.body)
            @test unready_body["checks"]["job_store"] == false
            @test unready_body["checks"]["browser_entrypoint"] == true
            @test unready_body["checks"]["native_entrypoint"] == true
        finally
            chmod(blocked_store, 0o700)
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
        end
    end

    previous_store_dir = BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[]
    mktempdir() do parent
        non_directory = joinpath(parent, "not-a-directory")
        write(non_directory, "block nested directory creation")
        try
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = joinpath(non_directory, "job-store")
            unready = router(HTTP.Request("GET", "/ready"))
            @test unready.status == 503
            @test JSON3.read(unready.body)["checks"]["job_store"] == false
        finally
            BiocircuitsExplorerBackend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
        end
    end

    # These paths must not collide with the API surface — verify the
    # canonicalizer leaves them alone.
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/health") == "/health"
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/ready")  == "/ready"
end

@testset "API v1 canonicalization" begin
    # /api/v1/version is the canonical form.
    v1_version = router(HTTP.Request("GET", "/api/v1/version"))
    @test v1_version.status == 200
    v1_body = JSON3.read(v1_version.body)
    @test v1_body["api_version"] == BiocircuitsExplorerBackend.API_CURRENT_VERSION
    @test haskey(v1_body, "version")              # app version retained
    @test v1_body["api_supported"][1] == "v1"

    # Bare /api/* is not an API surface any more.
    @test router(HTTP.Request("GET", "/api/v1/version")).status == 404
    @test router(HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"], "{")).status == 404

    # /api/v1 (with or without trailing slash) is a discovery probe.
    @test router(HTTP.Request("GET", "/api/v1")).status == 200
    @test router(HTTP.Request("GET", "/api/v1/")).status == 200

    # Malformed body on a real POST endpoint is a 400.
    bad_v1 = router(HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"], "{"))
    @test bad_v1.status == 400

    # Unknown endpoints under v1 fall through to the static handler (404).
    @test router(HTTP.Request("GET", "/api/v1/this-does-not-exist")).status == 404

    # Canonicalization unit checks.
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/api/v1/build_model") ==
          "/api/v1/build_model"
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/api/v1") == "/api/v1"
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/api/v1/") == "/api/v1"
    @test BiocircuitsExplorerBackend._canonicalize_api_path("/static/foo.css") ==
          "/static/foo.css"
    @test BiocircuitsExplorerBackend._is_unversioned_api_path("/api/v1/build_model")
    @test !BiocircuitsExplorerBackend._is_unversioned_api_path("/api/v1/build_model")
end

@testset "SBML Export/Import Round-Trip" begin
    # Export from the legacy shape, re-import, and check the binding network
    # survives intact with no warnings.
    exp = router(HTTP.Request("POST", "/api/v1/export/sbml",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("label" => "monomer_dimer",
                         "reactions" => ["A + B <-> AB", "AB + C <-> ABC"],
                         "kd" => [1.5, 0.4]))))
    @test exp.status == 200
    sbml = JSON3.read(exp.body)["sbml"]
    @test occursin("<sbml", sbml)
    @test occursin("level=\"3\"", sbml)
    @test occursin("<listOfReactions>", sbml)
    @test occursin("Kd_r1", sbml)

    imp = router(HTTP.Request("POST", "/api/v1/import/sbml",
        ["Content-Type" => "application/json"], JSON3.write(Dict("sbml" => sbml))))
    @test imp.status == 200
    ib = JSON3.read(imp.body)
    @test ib["network_ir"]["label"] == "monomer_dimer"
    rxs = ib["network_ir"]["reactions"]
    @test length(rxs) == 2
    @test rxs[1]["formula"] == "A + B <-> AB"
    @test rxs[1]["kd"] == 1.5
    @test rxs[2]["formula"] == "AB + C <-> ABC"
    @test rxs[2]["kd"] == 0.4
    @test Set(s["name"] for s in ib["network_ir"]["species"]) == Set(["A", "B", "C", "AB", "ABC"])
    @test isempty(ib["warnings"])

    # Export also accepts a full NetworkIR payload (not just legacy).
    exp2 = router(HTTP.Request("POST", "/api/v1/export/sbml",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
                         "label" => "ir_form",
                         "reactions" => [Dict("formula" => "A + B <-> AB", "kd" => 2.0)]))))
    @test exp2.status == 200
    @test occursin("ir_form", JSON3.read(exp2.body)["sbml"])

    # SBML ids are computational identifiers; display names are metadata.  In
    # particular, names may contain spaces while ids (including a valid leading
    # underscore) are what reaction formulas and speciesReference attributes
    # must use.
    named_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <sbml xmlns="http://www.sbml.org/sbml/level3/version2/core" level="3" version="2">
      <model id="_model_1" name="Readable model">
        <listOfCompartments>
          <compartment id="default" constant="true"/>
        </listOfCompartments>
        <listOfParameters>
          <parameter id="Kd_r1" value="0.75" constant="true"/>
        </listOfParameters>
        <listOfSpecies>
          <species id="_free_A" name="Free A" compartment="default" constant="false"/>
          <species id="B" name="Binding partner" compartment="default" constant="false"/>
          <species id="_complex_AB" name="Bound complex" compartment="default" constant="false"/>
        </listOfSpecies>
        <listOfReactions>
          <reaction id="_binding_step" name="Primary binding" reversible="true">
            <listOfReactants>
              <speciesReference species="_free_A" stoichiometry="1" constant="true"/>
              <speciesReference species="B" stoichiometry="1" constant="true"/>
            </listOfReactants>
            <listOfProducts>
              <speciesReference species="_complex_AB" stoichiometry="1" constant="true"/>
            </listOfProducts>
            <kineticLaw>
              <math xmlns="http://www.w3.org/1998/Math/MathML"><ci>Kd_r1</ci></math>
            </kineticLaw>
          </reaction>
        </listOfReactions>
      </model>
    </sbml>
    """
    named_import = router(HTTP.Request("POST", "/api/v1/import/sbml",
        ["Content-Type" => "application/json"], JSON3.write(Dict("sbml" => named_xml))))
    @test named_import.status == 200
    named_body = JSON3.read(named_import.body)
    named_ir = named_body["network_ir"]
    @test isempty(named_body["warnings"])
    @test named_ir["label"] == "Readable model"
    @test named_ir["extensions"]["sbml"]["id"] == "_model_1"
    @test named_ir["extensions"]["sbml"]["name"] == "Readable model"

    named_species = Dict(String(sp["name"]) => sp for sp in named_ir["species"])
    @test Set(keys(named_species)) == Set(["_free_A", "B", "_complex_AB"])
    @test named_species["_free_A"]["metadata"]["sbml"]["name"] == "Free A"
    @test named_species["_complex_AB"]["metadata"]["sbml"]["id"] == "_complex_AB"
    @test named_ir["reactions"][1]["formula"] == "_free_A + B <-> _complex_AB"
    @test named_ir["reactions"][1]["metadata"]["sbml"]["id"] == "_binding_step"
    @test named_ir["reactions"][1]["metadata"]["sbml"]["name"] == "Primary binding"

    # The imported IR is directly buildable and export restores both identity
    # and display metadata instead of substituting the display names in formulas.
    named_build = router(HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"], JSON3.write(Dict("network" => named_ir))))
    @test named_build.status == 200

    named_export = router(HTTP.Request("POST", "/api/v1/export/sbml",
        ["Content-Type" => "application/json"], JSON3.write(Dict("network" => named_ir))))
    @test named_export.status == 200
    named_sbml = String(JSON3.read(named_export.body)["sbml"])
    @test occursin("<model id=\"_model_1\" name=\"Readable model\">", named_sbml)
    @test occursin("<species id=\"_free_A\" name=\"Free A\"", named_sbml)
    @test occursin("<reaction id=\"_binding_step\" name=\"Primary binding\"", named_sbml)
    @test occursin("speciesReference species=\"_complex_AB\"", named_sbml)

    named_reimport = router(HTTP.Request("POST", "/api/v1/import/sbml",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("sbml" => named_sbml))))
    @test named_reimport.status == 200
    named_ir2 = JSON3.read(named_reimport.body)["network_ir"]
    @test named_ir2["reactions"][1]["formula"] == "_free_A + B <-> _complex_AB"
    @test named_ir2["reactions"][1]["metadata"]["sbml"]["name"] == "Primary binding"
end

@testset "SBML Import Warnings And Lossy Constructs" begin
    # A hand-written SBML that exercises the warning paths: two compartments,
    # a modifier, and a reaction with no recoverable Kd.
    xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <sbml xmlns="http://www.sbml.org/sbml/level3/version2/core" level="3" version="2">
      <model id="lossy" name="lossy">
        <listOfCompartments>
          <compartment id="c1" constant="true"/>
          <compartment id="c2" constant="true"/>
        </listOfCompartments>
        <listOfSpecies>
          <species id="A" compartment="c1" initialConcentration="2.0" constant="false"/>
          <species id="B" compartment="c1" constant="false"/>
          <species id="AB" compartment="c1" constant="false"/>
          <species id="E" compartment="c1" constant="false"/>
        </listOfSpecies>
        <listOfReactions>
          <reaction id="bind" reversible="true">
            <listOfReactants>
              <speciesReference species="A" stoichiometry="1" constant="true"/>
              <speciesReference species="B" stoichiometry="1" constant="true"/>
            </listOfReactants>
            <listOfProducts>
              <speciesReference species="AB" stoichiometry="1" constant="true"/>
            </listOfProducts>
            <listOfModifiers>
              <modifierSpeciesReference species="E"/>
            </listOfModifiers>
          </reaction>
        </listOfReactions>
      </model>
    </sbml>
    """
    network, warnings = sbml_to_network_ir(xml)
    @test network.label == "lossy"
    @test length(network.reactions) == 1
    @test network.reactions[1].formula == "A + B <-> AB"
    @test network.reactions[1].kd == 1.0   # defaulted
    # initialConcentration carried through.
    a = first(filter(s -> s.name == "A", network.species))
    @test a.initial_total == 2.0
    # Three distinct warnings: extra compartment, modifier, defaulted Kd.
    @test any(w -> occursin("compartment", w), warnings)
    @test any(w -> occursin("modifier", w), warnings)
    @test any(w -> occursin("Kd", w), warnings)

    # A named dissociation constant in a kineticLaw is recovered.
    xml2 = """
    <?xml version="1.0" encoding="UTF-8"?>
    <sbml xmlns="http://www.sbml.org/sbml/level3/version2/core" level="3" version="2">
      <model id="kd_named">
        <listOfParameters>
          <parameter id="Kd_bind" value="0.25" constant="true"/>
        </listOfParameters>
        <listOfSpecies>
          <species id="A" constant="false"/>
          <species id="B" constant="false"/>
          <species id="AB" constant="false"/>
        </listOfSpecies>
        <listOfReactions>
          <reaction id="bind" reversible="true">
            <listOfReactants>
              <speciesReference species="A" stoichiometry="1" constant="true"/>
              <speciesReference species="B" stoichiometry="1" constant="true"/>
            </listOfReactants>
            <listOfProducts>
              <speciesReference species="AB" stoichiometry="1" constant="true"/>
            </listOfProducts>
            <kineticLaw>
              <math xmlns="http://www.w3.org/1998/Math/MathML"><ci>Kd_bind</ci></math>
            </kineticLaw>
          </reaction>
        </listOfReactions>
      </model>
    </sbml>
    """
    n2, w2 = sbml_to_network_ir(xml2)
    @test n2.reactions[1].kd == 0.25
    @test !any(w -> occursin("Kd", w), w2)   # found it, no Kd warning

    # Malformed XML is a 400 at the route, not a 500.
    bad = router(HTTP.Request("POST", "/api/v1/import/sbml",
        ["Content-Type" => "application/json"], JSON3.write(Dict("sbml" => "<nope"))))
    @test bad.status == 400
    # Missing the sbml field is also a clean 400.
    missing_field = router(HTTP.Request("POST", "/api/v1/import/sbml",
        ["Content-Type" => "application/json"], JSON3.write(Dict("foo" => "bar"))))
    @test missing_field.status == 400
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
    best_cand = refinement["best_candidate"]
    @test best_cand !== nothing && haskey(best_cand["best_trial"], "seed_source")
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
    bd1 = first["best_design"];  bd2 = second["best_design"]
    @test bd1 !== nothing && bd2 !== nothing
    if bd1 !== nothing && bd2 !== nothing
        @test bd1["candidate"]["network_id"] == bd2["candidate"]["network_id"]
        @test bd1["candidate"]["slice_id"] == bd2["candidate"]["slice_id"]
        @test bd1["candidate"]["refinement_score"] == bd2["candidate"]["refinement_score"]
    end
end

@testset "SQLite Query Prefilter Roundtrip" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "SQLite Append Atlas Reconstructs Library" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Prune SQLite Uses Lightweight Runtime Persist" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "SQLite Helpers Do Not Accumulate Registered Statements" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Change Expansion Generates Axis And Orthant Slices" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Atlas Landscape 2D Scan From Raw Rules" begin
    _atlas_test() do
    result = BiocircuitsExplorerBackend.atlas_landscape_2d_from_spec(Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1e-9],
        "output_expr" => "AB",
        "preferred_param_symbols" => Any["tA", "tB"],
        "n_grid" => 24,
    ))

    @test result["param1_symbol"] == "tA"
    @test result["param2_symbol"] == "tB"
    @test result["output_expr"] == "AB"
    @test result["param_symbol_options"] == ["tA", "tB", "Kd1"]
    @test result["output_symbol_options"] == ["A", "B", "AB"]
    @test Float64.(result["fixed_qK"]) == [0.0, 0.0, -9.0]
    @test length(result["param1_values"]) == 24
    @test length(result["param2_values"]) == 24
    @test length(result["output_grid"]) == 24
    @test length(first(result["output_grid"])) == 24
    @test length(result["regime_grid"]) == 24
    @test length(first(result["regime_grid"])) == 24
    @test length(result["bounds"]) == 24
    @test length(first(result["bounds"])) == 24
    end # _atlas_test
end

@testset "Parameter Scans Default To Session Kd" begin
    _atlas_test() do
    sid = "kd-default-scan-test"
    build_response = router(HTTP.Request(
        "POST",
        "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "session_id" => sid,
            "reactions" => Any["A + B <-> AB"],
            "kd" => Any[1e-8],
        )),
    ))
    @test build_response.status == 200
    @test Float64.(response_json(build_response)["kd"]) == [1e-8]

    scan_response = router(HTTP.Request(
        "POST",
        "/api/v1/parameter_scan_1d",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "session_id" => sid,
            "param_symbol" => "tA",
            "param_min" => -2,
            "param_max" => 2,
            "n_points" => 10,
            "output_exprs" => Any["AB"],
        )),
    ))
    @test scan_response.status == 200
    @test Float64.(response_json(scan_response)["fixed_qK"]) == [0.0, 0.0, -8.0]

    bad_fixed_response = router(HTTP.Request(
        "POST",
        "/api/v1/parameter_scan_1d",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "session_id" => sid,
            "param_symbol" => "tA",
            "output_exprs" => Any["AB"],
            "fixed_qK" => Any[0.0],
        )),
    ))
    @test bad_fixed_response.status == 400
    @test occursin("Length of `fixed_qK`", String(bad_fixed_response.body))
    end # _atlas_test
end

@testset "Atlas Query Results Preserve Source Kd Metadata" begin
    _atlas_test() do
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[merge(SIMPLE_NETWORK, Dict("kd" => Any[1e-7]))],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    query_result = query_behavior_atlas(atlas, Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
        "limit" => 1,
    ))

    @test query_result["result_count"] == 1
    @test Float64.(first(query_result["results"])["kd"]) == [1e-7]
    end # _atlas_test
end

@testset "Atlas Slices Persist Canonical IO Labels" begin
    _atlas_test() do
    BEB = BiocircuitsExplorerBackend
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[CANONICAL_RELABEL_NETWORK],
        "search_profile" => Dict(
            "allow_homomeric_templates" => true,
            "allow_higher_order_templates" => true,
            "max_support" => 3,
        ),
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => false,
            "min_volume_mean" => 0.0,
        ),
    ))

    @test atlas["successful_network_count"] == 1
    slice = only(atlas["behavior_slices"])
    @test slice["input_symbol"] == "tA"
    @test slice["output_symbol"] == "C_A_A_B"
    @test occursin("input=tA", slice["slice_id"])
    @test occursin("output=C_A_A_B", slice["slice_id"])
    @test !occursin("C_A_B_B", slice["slice_id"])
    @test slice["output_symbol"] in first(BEB._design_canonical_species(slice["network_id"]))

    result = query_behavior_atlas(atlas, Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["C_A_A_B"],
        "limit" => 1,
    ))
    @test result["result_count"] == 1

    mktempdir() do tmpdir
        sqlite_path = joinpath(tmpdir, "atlas.sqlite")
        atlas_sqlite_merge_atlas!(sqlite_path, atlas; source_label="canonical_io_test")
        via_sqlite = query_behavior_atlas_from_spec(Dict(
            "sqlite_path" => sqlite_path,
            "query" => Dict(
                "input_symbols" => Any["tA"],
                "output_symbols" => Any["C_A_A_B"],
                "limit" => 1,
            ),
        ))
        @test via_sqlite["result_count"] == 1
        @test via_sqlite["results"][1]["input_symbol"] == "tA"
        @test via_sqlite["results"][1]["output_symbol"] == "C_A_A_B"
    end
    end # _atlas_test
end

@testset "Higher Nullity Off-Path Vertices Do Not Break Materialization" begin
    _atlas_test() do
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

    # REGRESSION (change_qK keyword): Bnc_julia/src/rop/rop_change_paths.jl:214
    # throws UndefKeywordError: keyword argument `change_qK` not assigned when
    # building ChangePaths for orthant specs on d=4 networks; network build fails
    # → analysis_status="failed", regime_record_count=0.  Unrelated to R1 SISOPaths fix.
    @test atlas["successful_network_count"] == 1                # 0: change_qK regression
    @test atlas["failed_network_count"] == 0                    # 1: change_qK regression
    @test length(atlas["behavior_slices"]) == 1
    @test atlas["behavior_slices"][1]["analysis_status"] == "ok"    # "failed": change_qK regression
    @test atlas["behavior_slices"][1]["regime_record_count"] > 0    # 0: change_qK regression
    @test atlas["input_graph_slices"][1]["vertex_count"] < atlas["input_graph_slices"][1]["full_vertex_count"]
    end # _atlas_test
end

@testset "Subset Binding Enumeration Supports Higher-Order Templates" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Homomeric Templates Validate and Build" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Pairwise Plus Homomeric Enumeration Includes AA and AAA" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Pairwise Plus Homomeric Enumeration Supports Tetramer Filter" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Complex-Growth Enumeration Includes AB Plus C To ABC" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Complex-Growth Enumeration Includes Tetrameric Homomer Growth" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "Parallel Network Build Matches Serial Build" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "SQLite Query Prefilter Supports Change Signatures" begin
    _atlas_test() do
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

        # REGRESSION (change_qK keyword): same as "Higher Nullity" — ORTHANT_NETWORK
        # build fails with UndefKeywordError in rop_change_paths.jl:214; 0 results returned.
        @test in_memory["result_count"] == 1                # 0: change_qK regression
        @test via_sqlite["result_count"] == 1               # 0: change_qK regression
        if via_sqlite["result_count"] >= 1
            @test via_sqlite["results"][1]["change_signature"] == "orthant(+tA,+tB)"
        end
        @test prefiltered["sqlite_prefilter"]["candidate_slice_count"] == 1  # 0: change_qK regression
        local _slices = prefiltered["behavior_slices"]
        @test length(_slices) == 1 && only(_slices)["change_signature"] == "orthant(+tA,+tB)"  # empty: change_qK regression
    end
    end # _atlas_test
end

@testset "Previously High Nullity Slices Materialize And Reuse" begin
    _atlas_test() do
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
    end # _atlas_test
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
    @test exprs !== nothing && length(exprs) == 3

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
    @test cond_log !== nothing && !isempty(cond_log)
    @test cond_lin !== nothing && !isempty(cond_lin)
    # REGRESSION (symbolic rendering): exponents now rendered as floats (tA^2.0 not tA^2);
    # upstream Symbolics.jl change causes show_condition_qK log_space=false to emit ".0".
    @test_broken cond_lin !== nothing && all(c -> !occursin(".0", string(c)), cond_lin)
end

@testset "Leading Singular Tokens Deduplicate Without Crashing" begin
    # fix(resync): the OLD-faithful scalar _dedup is restored in the overlay
    # (rop_overlay.jl), overriding upstream SISO.jl's NaN-leading assert. Leading
    # NaN (singular/asymptotic regimes) deduplicates to a single NaN token instead
    # of crashing get_RO_paths/get_behavior_families. These assertions match the
    # OLD engine's output exactly (regime_graphs.jl) and the pre-migration test.
    scalar_path = [NaN, NaN, 1.0, 1.0, NaN, 0.0, 0.0]
    @test isequal(BindingAndCatalysis._dedup(scalar_path), [NaN, 1.0, NaN, 0.0])
    @test isequal(BindingAndCatalysis._dedup([NaN, NaN]), [NaN])
    # Non-NaN-leading scalars unchanged:
    @test isequal(BindingAndCatalysis._dedup([1.0, 1.0, NaN, 0.0, 0.0]), [1.0, NaN, 0.0])
    @test isequal(BindingAndCatalysis._dedup([1.0]), [1.0])

    # Vector overload (rop_overlay.jl) handles leading NaN:
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
    _atlas_test() do
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

    # Regression for the migrated SISOPaths path-condition backend. This
    # 232-path complex-growth network used to fail while materializing its
    # `tA` axis slice; keep the successful build as a permanent assertion.
    @test atlas["successful_network_count"] == 1
    @test atlas["failed_network_count"] == 0

    network = only(atlas["network_entries"])
    @test network["analysis_status"] == "ok"
    @test network["build_state"] == "complete"
    @test network["failure_classes"] == String[]
    end # _atlas_test
end

@testset "Orthant Change Slice Builds In Graph-Only Mode" begin
    _atlas_test() do
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "path_scope" => "all",
            "compute_volume" => false,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    # REGRESSION (change_qK keyword): ORTHANT_NETWORK has orthant change_specs;
    # rop_change_paths.jl:214 throws UndefKeywordError → build fails, 0 success.
    @test atlas["successful_network_count"] == 1    # 0: change_qK regression
    @test atlas["failed_network_count"] == 0        # 1: change_qK regression
    @test length(atlas["input_graph_slices"]) == 1
    @test length(atlas["behavior_slices"]) == 1
    @test length(atlas["path_records"]) >= 1        # 0: change_qK regression

    graph_slice = only(atlas["input_graph_slices"])
    slice = only(atlas["behavior_slices"])

    @test graph_slice["change_kind"] == "orthant"
    @test graph_slice["input_symbol"] == "+tA,+tB"
    @test graph_slice["graph_config"]["slice_mode"] == "change"
    @test graph_slice["graph_config"]["graph_schema_version"] == "orthant_v0"

    @test slice["analysis_status"] == "ok"          # "failed": change_qK regression
    @test slice["input_symbol"] == "+tA,+tB"
    @test slice["change_kind"] == "orthant"
    @test slice["feasibility_mode"] == "graph_only_unchecked"  # missing: change_qK regression
    @test any(token -> occursin("(", token), get(slice, "regime_token_union", String[]))  # empty: change_qK regression

    if !isempty(atlas["path_records"])
        first_path = first(atlas["path_records"])
        @test first_path["feasibility_checked"] == false
        @test first(first_path["exact_profile"]) isa AbstractVector
    end
    end # _atlas_test
end

@testset "Orthant Change Slice Supports Feasible Filtering" begin
    _atlas_test() do
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => false,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    # REGRESSION (change_qK keyword): same as above.
    @test atlas["successful_network_count"] == 1    # 0: change_qK regression
    @test atlas["failed_network_count"] == 0        # 1: change_qK regression
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    @test slice["analysis_status"] == "ok"          # "failed": change_qK regression
    @test slice["feasibility_mode"] == "projected_feasible"  # missing: change_qK regression
    @test get(slice, "feasible_paths", 0) >= 1      # 0: change_qK regression
    if !isempty(atlas["path_records"])
        first_path = first(atlas["path_records"])
        @test first_path["feasibility_checked"] == true
    end
    end # _atlas_test
end

@testset "Orthant Change Slice Supports Volume Filtering" begin
    _atlas_test() do
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "compute_volume" => true,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    # REGRESSION (change_qK keyword): same as above.
    @test atlas["successful_network_count"] == 1    # 0: change_qK regression
    @test atlas["failed_network_count"] == 0        # 1: change_qK regression
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    @test slice["analysis_status"] == "ok"          # "failed": change_qK regression
    @test slice["feasibility_mode"] == "projected_feasible"  # missing: change_qK regression
    @test get(slice, "included_paths", 0) >= 1      # 0: change_qK regression
    if !isempty(atlas["path_records"])
        first_path = first(atlas["path_records"])
        @test first_path["feasibility_checked"] == true
        @test first_path["volume"] !== nothing
    end
    end # _atlas_test
end

@testset "Orthant Change Slice Supports Robust Filtering" begin
    _atlas_test() do
    atlas = build_behavior_atlas_from_spec(Dict(
        "networks" => Any[ORTHANT_NETWORK],
        "behavior_config" => Dict(
            "path_scope" => "robust",
            "compute_volume" => true,
            "include_path_records" => true,
            "min_volume_mean" => 0.0,
        ),
    ))

    # REGRESSION (change_qK keyword): same as above.
    @test atlas["successful_network_count"] == 1    # 0: change_qK regression
    @test atlas["failed_network_count"] == 0        # 1: change_qK regression
    @test length(atlas["behavior_slices"]) == 1

    slice = only(atlas["behavior_slices"])
    @test slice["analysis_status"] == "ok"          # "failed": change_qK regression
    @test get(slice, "included_paths", 0) >= 1      # 0: change_qK regression
    if !isempty(atlas["path_records"])
        first_path = first(atlas["path_records"])
        @test first_path["volume"] !== nothing
    end
    end # _atlas_test
end

@testset "Orthant Witness Materialization And Refinement Degrade Gracefully" begin
    _atlas_test() do
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

    # REGRESSION (change_qK keyword): atlas build fails → 0 results → query returns empty.
    @test result["result_count"] == 1               # 0: change_qK regression
    if result["result_count"] >= 1
        @test result["results"][1]["change_signature"] == "orthant(+tA,+tB)"
        @test result["results"][1]["best_witness_path"] !== nothing
    end
    @test refined["evaluated_count"] == 1           # 0: change_qK regression
    if !isempty(get(refined, "results", []))
        @test refined["results"][1]["change_signature"] == "orthant(+tA,+tB)"
        @test refined["results"][1]["refinement_status"] == "unsupported_multidimensional_refinement"
    end
    @test refined["best_candidate"] === nothing
    end # _atlas_test
end

@testset "Refresh Demotes Historical Incomplete Slice" begin
    _atlas_test() do
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
    end # _atlas_test
end

@testset "NetworkIR Legacy Bridge" begin
    raw = Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[2.5],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
        "label" => "monomer_dimer",
    )
    net = parse_network_ir(raw)
    @test net.ir_schema_version == NETWORK_IR_SCHEMA_VERSION
    @test net.label == "monomer_dimer"
    @test net.provenance.source == "legacy_reactions_kd"
    @test length(net.species) == 3
    @test [sp.name for sp in net.species] == ["A", "B", "AB"]
    @test [sp.role for sp in net.species] == [:free, :free, :bound]
    @test length(net.reactions) == 1
    @test net.reactions[1].formula == "A + B <-> AB"
    @test net.reactions[1].kd == 2.5
    @test length(net.observables) == 1
    @test net.observables[1].expression == "AB"
    @test length(net.parameter_distributions) == 1
    @test net.parameter_distributions[1].symbol == "tA"
    @test net.parameter_distributions[1].kind == :loguniform

    bridge = network_ir_to_legacy_inputs(net)
    @test bridge.rules == ["A + B <-> AB"]
    @test bridge.kd == [2.5]
    @test bridge.input_symbols == ["tA"]
    @test bridge.output_symbols == ["AB"]
end

@testset "NetworkIR Structured Round-Trip" begin
    structured = Dict(
        "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
        "label" => "structured_demo",
        "species" => Any[
            Dict("name" => "A", "role" => "free", "initial_total" => 1.0, "unit" => "uM"),
            Dict("name" => "B", "role" => "free", "unit" => "uM"),
            Dict("name" => "AB", "role" => "bound"),
        ],
        "reactions" => Any[
            Dict("formula" => "A + B <-> AB", "kd" => 0.5, "kind" => "binding"),
        ],
        "observables" => Any[
            Dict("name" => "complex", "expression" => "AB"),
        ],
        "parameter_distributions" => Any[
            Dict("symbol" => "tA", "kind" => "loguniform", "log_min" => -3.0, "log_max" => 3.0),
            Dict("symbol" => "tB", "kind" => "point", "value" => 1.0),
        ],
        "provenance" => Dict("created_by" => "tester", "source" => "unit_test"),
    )

    net = parse_network_ir(structured)
    @test net.label == "structured_demo"
    @test length(net.species) == 3
    @test net.species[1].initial_total == 1.0
    @test net.species[1].unit == "uM"
    @test net.reactions[1].kd == 0.5
    @test net.parameter_distributions[2].value == 1.0
    @test net.provenance.created_by == "tester"

    dict_form = network_ir_to_dict(net)
    @test dict_form["ir_schema_version"] == NETWORK_IR_SCHEMA_VERSION
    @test length(dict_form["species"]) == 3
    @test dict_form["reactions"][1]["formula"] == "A + B <-> AB"

    round_trip = parse_network_ir(dict_form)
    @test network_ir_hash(round_trip) == network_ir_hash(net)
end

@testset "NetworkIR Validation Errors" begin
    @test_throws IRValidationError parse_network_ir(Dict(
        "ir_schema_version" => "sbml/v3",
        "species" => Any[],
        "reactions" => Any[],
    ))

    err = try
        parse_network_ir(Dict(
            "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
            "species" => Any[Dict("role" => "free")],
            "reactions" => Any[Dict("formula" => "A + B <-> AB", "kd" => 1.0)],
        ))
        nothing
    catch e
        e
    end
    @test err isa IRValidationError
    @test occursin("name", err.msg)
    @test occursin("species", err.path)

    err = try
        parse_network_ir(Dict(
            "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
            "species" => Any[
                Dict("name" => "A", "role" => "free"),
                Dict("name" => "B", "role" => "free"),
                Dict("name" => "AB", "role" => "bound"),
            ],
            "reactions" => Any[Dict("formula" => "A plus B", "kd" => 1.0)],
        ))
        nothing
    catch e
        e
    end
    @test err isa IRValidationError
    @test occursin("formula", err.path)

    err = try
        parse_network_ir(Dict(
            "reactions" => Any["A + B <-> AB"],
            "kd" => Any[-1.0],
        ))
        nothing
    catch e
        e
    end
    @test err isa IRValidationError
    @test occursin("kd", err.msg)

    err = try
        parse_network_ir(Dict(
            "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
            "species" => Any[
                Dict("name" => "A", "role" => "free"),
                Dict("name" => "C", "role" => "bound"),
            ],
            "reactions" => Any[Dict("formula" => "A + B <-> AB", "kd" => 1.0)],
        ))
        nothing
    catch e
        e
    end
    @test err isa IRValidationError
    @test occursin("not declared", err.msg)
end

@testset "DesignSpec Parsing And Legacy Bridge" begin
    payload = Dict(
        "ir_schema_version" => DESIGN_SPEC_SCHEMA_VERSION,
        "label" => "bandpass_AB",
        "goal" => Dict(
            "motif_labels" => Any["+ -> -"],
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
        ),
        "constraints" => Dict("max_base_species" => 3),
        "objectives" => Dict("ranking_mode" => "robustness_first"),
        "policies" => Dict(
            "refinement" => Dict("enabled" => true, "top_k" => 3),
            "search_profile" => Dict("name" => "binding_small_v0"),
        ),
        "provenance" => Dict("created_by" => "designer"),
    )
    ds = parse_design_spec(payload)
    @test ds.ir_schema_version == DESIGN_SPEC_SCHEMA_VERSION
    @test ds.label == "bandpass_AB"
    @test ds.goal["motif_labels"] == Any["+ -> -"]
    @test ds.constraints["max_base_species"] == 3
    @test ds.objectives["ranking_mode"] == "robustness_first"

    net = parse_network_ir(Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
    ))

    legacy = design_spec_to_legacy_request(ds; network=net)
    @test haskey(legacy, "query")
    @test legacy["query"]["motif_labels"] == Any["+ -> -"]
    @test legacy["query"]["max_base_species"] == 3
    @test legacy["query"]["ranking_mode"] == "robustness_first"
    @test haskey(legacy, "refinement")
    @test legacy["refinement"]["top_k"] == 3
    @test haskey(legacy, "search_profile")
    @test legacy["search_profile"]["name"] == "binding_small_v0"
    @test legacy["source_label"] == "bandpass_AB"
    @test length(legacy["networks"]) == 1
    @test legacy["networks"][1]["reactions"] == ["A + B <-> AB"]
    @test legacy["networks"][1]["kd"] == [1.0]

    @test_throws IRValidationError parse_design_spec(Dict(
        "ir_schema_version" => DESIGN_SPEC_SCHEMA_VERSION,
    ))

    @test_throws IRValidationError parse_design_spec(Dict(
        "ir_schema_version" => "sbol/v3.0",
        "goal" => Dict("motif_labels" => Any["x"]),
    ))
end

@testset "NetworkIR Hash Is Deterministic" begin
    raw = Dict(
        "reactions" => Any["A + B <-> AB"],
        "kd" => Any[1.0],
    )
    h1 = network_ir_hash(parse_network_ir(raw))
    h2 = network_ir_hash(parse_network_ir(raw))
    @test h1 == h2
    @test length(h1) == 64
end

@testset "handle_build_model Accepts Both Legacy And NetworkIR" begin
    legacy_req = HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "reactions" => ["A + B <-> AB"],
            "kd" => [1.5],
            "session_id" => "test-legacy",
        )))
    legacy_resp = BiocircuitsExplorerBackend.handle_build_model(legacy_req)
    @test legacy_resp.status == 200
    legacy_body = response_json(legacy_resp)
    @test legacy_body["kd"] == [1.5]
    @test haskey(legacy_body, "network_ir")
    @test legacy_body["network_ir"]["ir_schema_version"] == NETWORK_IR_SCHEMA_VERSION
    @test haskey(legacy_body, "network_ir_hash")

    ir_req = HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "network" => Dict(
                "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
                "species" => [
                    Dict("name" => "A", "role" => "free"),
                    Dict("name" => "B", "role" => "free"),
                    Dict("name" => "AB", "role" => "bound"),
                ],
                "reactions" => [Dict("formula" => "A + B <-> AB", "kd" => 0.5)],
            ),
            "session_id" => "test-ir",
        )))
    ir_resp = BiocircuitsExplorerBackend.handle_build_model(ir_req)
    @test ir_resp.status == 200
    ir_body = response_json(ir_resp)
    @test ir_body["kd"] == [0.5]
    @test ir_body["network_ir"]["ir_schema_version"] == NETWORK_IR_SCHEMA_VERSION
end

@testset "IR Validation Endpoints Return Structured Errors" begin
    good_req = HTTP.Request("POST", "/api/v1/ir/network/validate",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "reactions" => ["A + B <-> AB"],
            "kd" => [1.0],
        )))
    good_resp = BiocircuitsExplorerBackend.handle_ir_network_validate(good_req)
    @test good_resp.status == 200
    good_body = response_json(good_resp)
    @test good_body["valid"] == true
    @test good_body["ir_schema_version"] == NETWORK_IR_SCHEMA_VERSION
    @test haskey(good_body, "hash")

    bad_req = HTTP.Request("POST", "/api/v1/ir/network/validate",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "ir_schema_version" => NETWORK_IR_SCHEMA_VERSION,
            "species" => [Dict("role" => "free")],
            "reactions" => [Dict("formula" => "A + B <-> AB", "kd" => 1.0)],
        )))
    bad_resp = BiocircuitsExplorerBackend.handle_ir_network_validate(bad_req)
    @test bad_resp.status == 400
    bad_body = response_json(bad_resp)
    @test bad_body["valid"] == false
    @test bad_body["section"] == "network"
    @test occursin("species", String(bad_body["path"]))

    design_req = HTTP.Request("POST", "/api/v1/ir/design/validate",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict(
            "design" => Dict(
                "ir_schema_version" => DESIGN_SPEC_SCHEMA_VERSION,
                "goal" => Dict("motif_labels" => ["+"]),
            ),
            "network" => Dict(
                "reactions" => ["A + B <-> AB"],
                "kd" => [1.0],
            ),
        )))
    design_resp = BiocircuitsExplorerBackend.handle_ir_design_validate(design_req)
    @test design_resp.status == 200
    design_body = response_json(design_resp)
    @test design_body["valid"] == true
    @test haskey(design_body, "legacy_request")
    @test design_body["legacy_request"]["query"]["motif_labels"] == ["+"]
end

@testset "Content-Addressed Model Cache (Phase 1)" begin
    MC = BiocircuitsExplorerBackend.ModelCache
    NET = Dict("reactions" => ["A + B <-> AB"], "kd" => [1.0])
    ALT = Dict("reactions" => ["A + C <-> AC"], "kd" => [1.0])

    post(path, body) = router(HTTP.Request("POST", path,
        ["Content-Type" => "application/json"], JSON3.write(body)))

    # Isolate from cache state left by earlier testsets.
    MC._clear_all!()

    # 1. Build registers exactly one bundle, keyed by the NetworkIR hash.
    build = post("/api/v1/build_model", NET)
    @test build.status == 200
    bbody = response_json(build)
    sid = bbody["session_id"]
    h = bbody["network_ir_hash"]
    @test !isempty(h)
    @test MC.model_count() == 1

    # 2. Rebuilding the same IR is a cache hit; a different IR adds one bundle.
    @test response_json(post("/api/v1/build_model", NET))["network_ir_hash"] == h
    @test MC.model_count() == 1
    post("/api/v1/build_model", ALT)
    @test MC.model_count() == 2

    # 3. Downstream endpoints are stateless: a NetworkIR alone resolves a model.
    sl = post("/api/v1/find_vertices", Dict("network" => NET))
    @test sl.status == 200
    @test haskey(response_json(sl), "n_vertices")

    # 4. After the compiled model is evicted (restart / LRU), a hash rebuilds it
    #    from the retained IR side-table — no session, no resent IR needed.
    MC._clear_models!()
    @test MC.model_count() == 0
    reb = post("/api/v1/find_vertices", Dict("network_ir_hash" => h))
    @test reb.status == 200
    @test haskey(response_json(reb), "n_vertices")
    @test MC.model_count() == 1

    # 5. Legacy session_id still resolves the same bundle.
    @test post("/api/v1/find_vertices", Dict("session_id" => sid)).status == 200

    # 6. Cold miss (no model, unknown hash, no IR to rebuild from) tells the
    #    client to resend the NetworkIR rather than failing opaquely.
    MC._clear_all!()
    cold = post("/api/v1/find_vertices", Dict("network_ir_hash" => repeat("d", 64)))
    @test cold.status == 409
    @test response_json(cold)["need_network"] == true
end

@testset "Result Artifact Envelope (Phase 3)" begin
    # Envelope shape.
    m = artifact_metadata("demo";
        input_hashes = Dict("network_ir_hash" => "abc"),
        config = Dict("a" => 1, "b" => 2))
    @test m["artifact_schema_version"] == RESULT_ARTIFACT_SCHEMA_VERSION
    @test m["kind"] == "demo"
    @test m["input_hashes"]["network_ir_hash"] == "abc"
    @test m["algorithm"]["version"] == biocircuits_explorer_version()
    @test m["algorithm"]["config_hash"] !== nothing

    # config_hash is canonical: key order is irrelevant; different config differs.
    h1 = artifact_metadata("demo"; config = Dict("a" => 1, "b" => 2))["algorithm"]["config_hash"]
    h2 = artifact_metadata("demo"; config = Dict("b" => 2, "a" => 1))["algorithm"]["config_hash"]
    h3 = artifact_metadata("demo"; config = Dict("a" => 1, "b" => 3))["algorithm"]["config_hash"]
    @test h1 == h2
    @test h1 != h3
    @test artifact_metadata("demo")["algorithm"]["config_hash"] === nothing

    # attach_artifact! adds a sibling without disturbing existing fields; passes
    # non-dicts through unchanged.
    r = Dict{String, Any}("foo" => 1)
    attach_artifact!(r, "demo")
    @test r["foo"] == 1
    @test r["artifact"]["kind"] == "demo"
    @test attach_artifact!(42, "demo") == 42

    # End-to-end: build_model carries a build_model artifact tied to the IR hash.
    resp = router(HTTP.Request("POST", "/api/v1/build_model",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("reactions" => ["A + B <-> AB"], "kd" => [1.0]))))
    @test resp.status == 200
    body = response_json(resp)
    art = body["artifact"]
    @test art["artifact_schema_version"] == RESULT_ARTIFACT_SCHEMA_VERSION
    @test art["kind"] == "build_model"
    @test art["input_hashes"]["network_ir_hash"] == body["network_ir_hash"]
end

@testset "DesignSpec Constraint Typing (Phase 4)" begin
    ok = parse_design_spec(Dict(
        "constraints" => Dict("max_base_species" => 3, "max_reactions" => 2,
                              "forbid_regimes" => ["singular"])))
    @test ok.constraints["max_base_species"] == 3
    @test ok.constraints["forbid_regimes"] == ["singular"]

    # Unknown keys pass through untouched — the free-form escape hatch is intact.
    passthrough = parse_design_spec(Dict(
        "constraints" => Dict("max_base_species" => 2, "some_future_knob" => Dict("x" => 1))))
    @test haskey(passthrough.constraints, "some_future_knob")

    # Stabilized keys are shape-checked.
    @test_throws IRValidationError parse_design_spec(Dict("constraints" => Dict("max_base_species" => 0)))
    @test_throws IRValidationError parse_design_spec(Dict("constraints" => Dict("max_reactions" => 2.5)))
    @test_throws IRValidationError parse_design_spec(Dict("constraints" => Dict("forbid_regimes" => [1, 2])))
end

@testset "Generated IR Schemas Are Present And Versioned" begin
    schema_dir = normpath(joinpath(@__DIR__, "..", "..", "schemas"))
    for (file, version) in [
        ("network-ir.schema.json", NETWORK_IR_SCHEMA_VERSION),
        ("design-spec.schema.json", DESIGN_SPEC_SCHEMA_VERSION),
        ("result-artifact.schema.json", RESULT_ARTIFACT_SCHEMA_VERSION),
        ("job-result-manifest.schema.json", "bne-job-result-manifest/v1.0.0"),
        ("designability-spec.schema.json", "bne-designability/v1.0.0"),
        ("designability-screen.schema.json", "bne-design-screen/v0.3.0"),
    ]
        path = joinpath(schema_dir, file)
        @test isfile(path)
        schema = JSON3.read(read(path, String))
        key = file == "result-artifact.schema.json" ? "artifact_schema_version" :
              file in ("network-ir.schema.json", "design-spec.schema.json") ?
                  "ir_schema_version" : "schema_version"
        @test schema["properties"][key]["const"] == version
    end
end

# ── Design pipeline: DesignabilitySpec → screen → selected reaction network ──
# spec 2026-06-29-tunable-design-workflow-completion-design.md, items 1 & 2:
# label→RO input requires an explicitly configured operator-managed index; the
# old inverse-design route remains available when that index is configured.
@testset "Design Pipeline (label/sign search, operator-managed index, design_labels)" begin
    BEB = BiocircuitsExplorerBackend
    if isempty(get(ENV, "BNE_DESIGN_INDEX", ""))
        @test isempty(BEB._load_design_index())
        @test BEB.handle_design_labels(nothing).status == 503
    else
    idx = BEB._load_design_index()
    @test !isempty(idx)                                       # configured index loads
    @test !isempty(idx[1].motifs)                             # base_motifs field present
    @test BEB.design_search("label", "biphasic_peak")["designable"] === true
    @test !isempty(BEB.design_search("label", "biphasic_peak")["minimal"])
    @test BEB.design_search("sign", "+-+")["designable"] === true   # no regression
    @test_throws ErrorException BEB.design_search("unknown", [1.0])
    screen = BEB.design_screen("sign", "+-+")
    @test screen["schema_version"] == "bne-design-screen/v0.3.0"
    @test screen["designable"] === true
    @test haskey(screen, "designability_spec_normalized")
    @test haskey(screen, "constraint_audit")
    @test haskey(screen, "verified_recommendations")
    @test haskey(screen, "screened_candidates")
    @test isempty(screen["verified_recommendations"])
    @test screen["recommended"] == screen["verified_recommendations"]
    @test !isempty(screen["screened_candidates"])
    @test !isempty(screen["minimal_certificates"])
    @test all(card -> get(card, "evidence_grade", "") == "proxy_only" &&
                      card["pass"] === false,
              screen["screened_candidates"])
    all_screen = BEB.design_screen("sign", "+-+";
        candidate_budget=Dict("mode" => "all_matches", "max_recommended" => 3, "max_screened" => 3))
    @test all_screen["designability_spec_normalized"]["candidate_budget"]["mode"] == "all_matches"
    @test all_screen["eligible_count"] >= screen["eligible_count"]
    @test length(all_screen["screened_candidates"]) <= 3
    spec_screen = BEB.design_screen("sign", "+-+";
        designability_spec=Dict("target_kind" => "sign", "target" => "+-+",
                                "temporal_dynamics" => Dict(
                                    "peak_width_seconds" => Dict("min" => 1.0, "max" => 2.0))))
    @test isempty(spec_screen["verified_recommendations"])
    @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                      item["kind"] == "temporal_dynamics" &&
                      item["support_level"] == "unsupported",
              spec_screen["constraint_audit"])
    rec = first(screen["screened_candidates"])
    for key in ("nid", "inp", "out", "complexity", "constraints", "metrics",
                "parameter_recommendation", "certificate_grade",
                "active_failures", "agent_handoff")
        @test haskey(rec, key)
    end
    @test rec["out"] in first(BEB._design_canonical_species(rec["nid"]))
    @test rec["parameter_recommendation"]["theta_star"]["status"] == "not_computed"
    @test rec["parameter_recommendation"]["theta_star"]["source_type"] == "proxy"
    @test rec["constraints"]["bounds_intersection_verified"] === false
    @test haskey(rec["metrics"], "ranking_margin_proxy")
    @test haskey(rec["metrics"], "atlas_volume_proxy")
    @test !haskey(rec["metrics"], "tunable_volume")
    @test !haskey(rec["metrics"], "condition_number")
    @test !haskey(rec["metrics"], "parameter_breakpoint_sensitivity")
    @test rec["agent_handoff"]["endpoint"] == "/api/v1/design_screen"
    bad_kind_req = HTTP.Request("POST", "/api/v1/design_screen", [],
        JSON3.write(Dict("target_kind" => "unknown", "target" => Any[1.0])))
    @test BEB.router(bad_kind_req).status == 400
    bad_spec_req = HTTP.Request("POST", "/api/v1/design_screen", [],
        JSON3.write(Dict("designability_spec" => "not-an-object", "target" => "+-+")))
    @test BEB.router(bad_spec_req).status == 400
    for cell in BEB.design_search("sign", "+-+")["minimal"], net in cell["networks"]
        @test net["out"] in first(BEB._design_canonical_species(net["nid"]))
    end
    @test any(r -> r.out == "C_A_A", idx)
    @test BEB.handle_design_labels(nothing).status == 503       # no tracked label corpus
    @test haskey(BEB.API_ROUTES, "/api/v1/run_inverse_design")   # downgraded, not removed
    @test haskey(BEB.API_ROUTES, "/api/v1/design_labels")        # new endpoint wired
    @test haskey(BEB.API_ROUTES, "/api/v1/design_screen")        # tunability-aware screen wired
    end
end
