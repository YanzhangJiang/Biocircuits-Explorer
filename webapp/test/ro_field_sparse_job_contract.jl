module ROFieldSparseJobContractTests

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend
const Engine = Backend.BindingAndCatalysis

function _rofsj_network()
    return network_ir_to_dict(network_ir_from_legacy(
        ["A + B <-> AB"], [1.0];
        label="adaptive-ro-field-job-heterodimer",
        input_symbols=["tA", "tB"],
        output_symbols=["AB"],
    ))
end

function _rofsj_spec(; max_multi_indices=4,
                     max_interpolation_work=1_000_000,
                     max_replay_work_units=2_000_000_000,
                     max_payload_bytes=4 * 1024 * 1024,
                     dimension=2,
                     resume_from=nothing)
    dimension in (1, 2) || error("contract dimension must be 1 or 2")
    control_ids = dimension == 1 ? Any["input_a"] :
        Any["input_a", "input_b"]
    jacobian = dimension == 1 ? Any[
        Any[1.0], Any[0.0], Any[0.0],
    ] : Any[
        Any[1.0, 0.0],
        Any[0.0, 1.0],
        Any[0.0, 0.0],
    ]
    request = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_REQUEST_VERSION,
        "network" => _rofsj_network(),
        "chart" => Dict{String,Any}(
            "chart_id" => "two-total-affine-log10-chart",
            "control_ids" => control_ids,
            "source_coordinate_ids" => Any["tA", "tB", "Kd1"],
            "jacobian" => jacobian,
            "fixed_background" => Any[0.0, 0.0, 0.0],
        ),
        "domain" => Dict{String,Any}(
            "coordinate_space" => "engine_log10_affine_control",
            "lower" => fill(-1.0, dimension),
            "upper" => fill(1.0, dimension),
        ),
        "outputs" => Dict{String,Any}(
            "output_order" => Any["output_ab"],
            "items" => Any[
                Dict{String,Any}(
                    "output_id" => "output_ab",
                    "symbol" => "AB",
                ),
            ],
        ),
        "solver_policy" => Dict{String,Any}(
            "algorithm" =>
                "finite_equilibrium_full_source_log_jacobian",
            "output_space" => "engine_log10_concentration",
            "source_jacobian_space" =>
                "d_log10_output_d_log10_source_qK",
        ),
        "invalid_policy" =>
            "unresolved_cone_blocks_descendants_only",
        "surplus_tolerance" => 1.0e-12,
        "sampling_limits" => Dict{String,Any}(
            "max_level" => 4,
            "max_points" => 128,
            "max_multi_indices" => max_multi_indices,
            "max_interpolation_work" => max_interpolation_work,
            "max_payload_scalars" => 100_000,
        ),
        "work_budget" => Dict{String,Any}(
            "max_payload_bytes" => max_payload_bytes,
            "max_json_depth" => 32,
            "max_string_bytes" => 64 * 1024,
            "deadline_seconds" => nothing,
            "max_replay_work_units" => max_replay_work_units,
        ),
        "storage" => Dict{String,Any}("mode" => "chunked"),
    )
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_JOB_SPEC_VERSION,
        "request" => request,
        "resume_from" => resume_from,
    )
end

function _rofsj_evaluator(log; invalid_index=nothing,
                          cancel_before_ordinal=nothing,
                          arm_orphan_at=nothing,
                          orphan_armed=Ref(false))
    return function (_, _, batch, _)
        push!(log, Dict{String,Any}(
            "ordinal" => batch.batch_ordinal,
            "batch_sha256" => batch.batch_sha256,
            "multi_index" => copy(batch.multi_index),
            "point_ids" => [request.point_id for request in batch.requests],
        ))
        cancel_before_ordinal !== nothing &&
            batch.batch_ordinal >= cancel_before_ordinal &&
            throw(Backend.LocalJobCancelled("contract cancellation"))
        arm_orphan_at !== nothing && batch.batch_ordinal == arm_orphan_at &&
            (orphan_armed[] = true)
        samples = Dict{String,Any}[]
        for request in batch.requests
            u = request.control_coordinates[1]
            v = length(request.control_coordinates) == 1 ? 0.0 :
                request.control_coordinates[2]
            invalid = invalid_index !== nothing &&
                request.multi_index == invalid_index &&
                request.normalized_coordinates[1] > 0.0
            if invalid
                push!(samples, Dict{String,Any}(
                    "point_id" => request.point_id,
                    "status" => "invalid",
                    "gap" => Dict{String,Any}(
                        "reason" => "synthetic_nonconvergence",
                        "detail" => "contract gap",
                    ),
                ))
            else
                push!(samples, Dict{String,Any}(
                    "point_id" => request.point_id,
                    "status" => "valid",
                    "output_values" => Any[u + v],
                    "source_reaction_order_matrix" =>
                        Any[Any[u * v, u - v, 7.0]],
                    "regime_id" => "regime-contract",
                ))
            end
        end
        return samples
    end
end

function _with_rofsj_store(f::Function)
    mktempdir() do root
        previous = Backend.LOCAL_JOB_STORE_DIR[]
        Backend.LOCAL_JOB_STORE_DIR[] = root
        try
            return f(root)
        finally
            Backend.LOCAL_JOB_STORE_DIR[] = previous
        end
    end
end

function _rofsj_assert_outer_manifest_identity_bound(record)
    manifest_path = String(record["result_manifest_uri"])
    original = read(manifest_path)
    cases = (
        "artifact_schema_version" => (manifest ->
            (manifest["artifact_identity"]["artifact_schema_version"] =
                "bne-result/v999.0.0")),
        "algorithm_name" => (manifest ->
            (manifest["artifact_identity"]["algorithm_name"] =
                "different_algorithm")),
        "algorithm_version" => (manifest ->
            (manifest["artifact_identity"]["algorithm_version"] =
                "999.0.0")),
        "config_hash" => (manifest ->
            (manifest["artifact_identity"]["config_hash"] = "0"^64)),
        "artifact_metadata_hash" => (manifest ->
            (manifest["artifact_identity"]["artifact_metadata_hash"] =
                "0"^64)),
        "payload_key_count" => (manifest ->
            (manifest["result"]["payload_key_count"] += 1)),
    )
    for (_, mutate!) in cases
        try
            manifest = Backend._read_job_json(manifest_path)
            mutate!(manifest)
            Backend._write_job_json(manifest_path, manifest)
            verification = Backend._verify_job_result_artifact(record)
            @test verification.status == :invalid
        finally
            write(manifest_path, original)
        end
        @test Backend._verify_job_result_artifact(record).status == :valid
    end
    return nothing
end

function _rofsj_parent_record(job_id, spec, checkpoint;
                              status="cancelled", user_sub="alice")
    normalized = normalize_ro_field_job_spec(spec)
    return Dict{String,Any}(
        "job_id" => job_id,
        "kind" => "compute_ro_field",
        "status" => status,
        "executor" => "local_async",
        "user_sub" => user_sub,
        "ro_field_artifact_namespace" => "ro-field-sparse-v2",
        "ro_field_plan_sha256" => normalized["plan"]["plan_sha256"],
        "ro_field_network_ir_sha256" =>
            normalized["plan"]["identity"]["network_ir_sha256"],
        "latest_checkpoint_sha256" => checkpoint["checkpoint_sha256"],
        "committed_work_unit_count" =>
            checkpoint["committed_work_unit_count"],
        "committed_point_count" => checkpoint["committed_point_count"],
        "committed_payload_bytes" =>
            checkpoint["committed_payload_bytes"],
        "resume_from" => deepcopy(normalized["resume_from"]),
    )
end

function _restore_bytes(path, bytes)
    open(path, "w") do io
        write(io, bytes)
    end
    return path
end

@testset "adaptive v2 normalization has independent bounded identity" begin
    raw = _rofsj_spec()
    normalized = normalize_ro_field_job_spec(raw)
    @test normalized["schema_version"] == RO_FIELD_SPARSE_JOB_SPEC_VERSION
    @test normalized["plan"]["schema_version"] == RO_FIELD_SPARSE_PLAN_VERSION
    @test Backend._canonical_hash(normalized["plan"]["identity"]) ==
        normalized["plan"]["plan_sha256"]
    @test normalized["plan"]["identity"]["channel_order"] ==
        "output_major_then_input_minor"
    @test normalized["plan"]["identity"]["work_unit_semantics"] ==
        "one_adaptive_multi_index_batch"
    policy = normalized["plan"]["identity"][
        "numerical_execution_policy"]
    @test policy["schema_version"] ==
        Backend.RO_FIELD_SPARSE_NUMERICAL_POLICY_VERSION
    @test policy["equilibrium_solver"]["method"] == "homotopy"
    @test policy["equilibrium_solver"]["algorithm_type"] ==
        "OrdinaryDiffEq.Tsit5"
    @test policy["equilibrium_solver"][
        "max_solver_steps_per_point"] == 100_000
    @test policy["equilibrium_solver"][
        "max_rhs_evaluations_per_point"] == 1_000_000
    @test policy["regime_assignment_policy"] ==
        "strict_float64_closed_cell_membership_no_fallback"
    @test policy["replay_validation"]["max_replay_work_units"] ==
        2_000_000_000
    @test policy["replay_validation"]["artifact_traversal"] ==
        "one_authoritative_forward_chain_without_recursive_prefix_replay"
    @test policy["replay_validation"]["scope"] ==
        "checkpoint_and_terminal_artifact_chain_after_bounded_plan_reconstruction"
    @test occursin(r"^[0-9a-f]{64}$",
        policy["runtime_lock"]["identity_sha256"])
    active_lock = policy["runtime_lock"]["identity"]["active_lock"]
    @test active_lock == Backend._ROFSJ_LOADED_ACTIVE_LOCK_IDENTITY
    @test active_lock["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies"
    @test startswith(active_lock["manifest_sha256"], "sha256:")
    binding_source = policy["runtime_lock"]["identity"][
        "binding_source"]
    @test binding_source == Backend._ROFSJ_LOADED_BINDING_SOURCE_IDENTITY
    @test binding_source["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies"
    @test occursin(r"^[0-9a-f]{64}$",
        binding_source["tree_sha256"])
    web_source = policy["runtime_lock"]["identity"][
        "webapp_loaded_source"]
    @test web_source == Backend._ROFSJ_LOADED_WEB_SOURCE_IDENTITY
    @test web_source["path_policy"] ==
        "recursive_julia_files_relative_posix_order"
    @test web_source["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies"
    @test web_source["file_count"] > 1
    @test occursin(r"^[0-9a-f]{64}$", web_source["tree_sha256"])
    @test normalize_ro_field_job_spec(raw) == normalized
    @test !occursin("axis_coordinates",
        Backend._rofc_canonical_json(normalized))
    @test normalized["request"]["chart"]["source_coordinate_ids"] ==
        ["tA", "tB", "Kd1"]

    prepared = Backend._prepare_job_spec_and_artifact_identity(
        "compute_ro_field", raw)
    @test prepared.expected_artifact_config_hash ==
        normalized["plan"]["plan_sha256"]
    @test prepared.spec == normalized

    bad_bool = deepcopy(raw)
    bad_bool["request"]["sampling_limits"]["max_points"] = true
    @test_throws ArgumentError normalize_ro_field_job_spec(bad_bool)
    too_many = deepcopy(raw)
    too_many["request"]["sampling_limits"]["max_points"] = 4_097
    @test_throws ArgumentError normalize_ro_field_job_spec(too_many)
    too_many_batches = deepcopy(raw)
    too_many_batches["request"]["sampling_limits"][
        "max_multi_indices"] = 513
    @test_throws ArgumentError normalize_ro_field_job_spec(too_many_batches)
    partial_source = deepcopy(raw)
    pop!(partial_source["request"]["chart"]["source_coordinate_ids"])
    pop!(partial_source["request"]["chart"]["jacobian"])
    pop!(partial_source["request"]["chart"]["fixed_background"])
    @test_throws ArgumentError normalize_ro_field_job_spec(partial_source)
    supplied = deepcopy(normalized)
    supplied["plan"]["identity"]["channel_order"] = "input-major"
    @test_throws ArgumentError normalize_ro_field_job_spec(supplied)

    aws = Dict{String,Any}(
        "kind" => "compute_ro_field",
        "spec" => raw,
        "execution" => Dict("mode" => "aws_batch"),
    )
    @test_throws ArgumentError submit_biocircuits_job_from_spec(aws)
end

@testset "loaded runtime digests fail plan, resume, and final read closed" begin
    _with_rofsj_store() do _
        parent_id = "7"^32
        raw = _rofsj_spec(max_multi_indices=1)
        normalized = normalize_ro_field_job_spec(raw)
        parent_calls = Dict{String,Any}[]
        result = compute_ro_field_job(
            raw;
            job_context=Dict{String,Any}(
                "job_id" => parent_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(parent_calls),
            ),
        )
        descriptor = result["ro_field_job_result"]
        root = Backend._rofsj_data_root(parent_id)
        checkpoint = Backend._rofsj_read_canonical(
            Backend._rofsj_checkpoint_path(
                root, descriptor["checkpoint_sha256"]))
        record = _rofsj_parent_record(
            parent_id, raw, checkpoint; status="cancelled")
        Backend._job_cache_publish!(parent_id, record)
        resume = Dict{String,Any}(
            "parent_job_id" => parent_id,
            "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
        )
        child_spec = deepcopy(normalized)
        child_spec["resume_from"] = resume
        altered_web = deepcopy(Backend._ROFSJ_LOADED_WEB_SOURCE_IDENTITY)
        altered_web["tree_sha256"] =
            altered_web["tree_sha256"] == "b"^64 ? "c"^64 : "b"^64
        altered_binding = deepcopy(
            Backend._ROFSJ_LOADED_BINDING_SOURCE_IDENTITY)
        altered_binding["tree_sha256"] =
            altered_binding["tree_sha256"] == "d"^64 ? "e"^64 : "d"^64
        altered_lock = deepcopy(Backend._ROFSJ_LOADED_ACTIVE_LOCK_IDENTITY)
        altered_lock["manifest_sha256"] =
            altered_lock["manifest_sha256"] == "sha256:" * "f"^64 ?
            "sha256:" * "a"^64 : "sha256:" * "f"^64
        overrides = (
            (Backend._rofsj_with_web_source_identity_test_override,
                altered_web, "8"^32),
            (Backend._rofsj_with_binding_source_identity_test_override,
                altered_binding, "9"^32),
            (Backend._rofsj_with_active_lock_identity_test_override,
                altered_lock, "a"^32),
        )
        try
            for (with_override, altered, drift_child_id) in overrides
                resume_calls = Dict{String,Any}[]
                with_override(altered) do
                    @test_throws ArgumentError Backend._rofsj_validate_plan(
                        normalized["plan"])
                    @test_throws ArgumentError Backend._rofsj_resume_parent_snapshot(
                        child_spec, "alice")
                    @test_throws ArgumentError compute_ro_field_job(
                        child_spec;
                        job_context=Dict{String,Any}(
                            "job_id" => drift_child_id,
                            "user_sub" => "alice",
                            "sparse_batch_evaluator" =>
                                _rofsj_evaluator(resume_calls),
                        ),
                    )
                    @test isempty(resume_calls)
                    @test_throws ArgumentError validate_ro_field_job_result!(
                        result, parent_id, descriptor["plan_sha256"])
                end
            end
            @test validate_ro_field_job_result!(
                result, parent_id, descriptor["plan_sha256"]) == descriptor
        finally
            Backend._job_cache_remove!(parent_id)
        end
    end
end

@testset "equilibrium work caps and cancellation enter the homotopy solve" begin
    network = parse_network_ir(_rofsj_network())
    model = Backend.build_model_bundle(network)["model"]
    theta = Float64[0.0, 0.0, 0.0]
    @test_throws Engine.QK2XWorkLimitExceeded Engine.qK2x(
        model,
        theta;
        input_logspace=true,
        output_logspace=true,
        maxiters=100,
        max_rhs_evaluations=1,
    )

    checks = Ref(0)
    @test_throws Backend.LocalJobCancelled Engine.qK2x(
        model,
        theta;
        input_logspace=true,
        output_logspace=true,
        maxiters=100_000,
        max_rhs_evaluations=1_000_000,
        cancel_check=() -> begin
            checks[] += 1
            checks[] >= 10 && throw(Backend.LocalJobCancelled(
                "cancel inside homotopy RHS"))
        end,
    )
    @test checks[] == 10

    @test Engine.assign_regime_qK(
        model,
        fill(NaN, 3);
        input_logspace=true,
        return_idx=true,
        membership=:closed_cell,
    ) == 0
end

@testset "one sparse index is one replayable work unit with explicit gaps" begin
    _with_rofsj_store() do _
        job_id = "a"^32
        log = Dict{String,Any}[]
        result = compute_ro_field_job(
            _rofsj_spec(max_multi_indices=4);
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" => _rofsj_evaluator(
                    log; invalid_index=[2, 1]),
            ),
        )
        descriptor = result["ro_field_job_result"]
        @test descriptor["schema_version"] ==
            RO_FIELD_SPARSE_JOB_RESULT_VERSION
        @test descriptor["work_unit_count"] == length(log)
        @test descriptor["point_count"] > descriptor["work_unit_count"]
        @test descriptor["interpolation_work_consumed"] >
            descriptor["work_unit_count"]
        @test descriptor["invalid_count"] == 1
        @test descriptor["status"] == "unknown_gap"
        @test validate_ro_field_job_result!(
            result, job_id, descriptor["plan_sha256"]) == descriptor

        root = Backend._rofsj_data_root(job_id)
        replay = Backend._rofsj_load_checkpoint(
            root,
            Backend._rofsj_read_plan(root, descriptor["plan_sha256"])...,
            descriptor["checkpoint_sha256"],
        )
        @test replay.checkpoint["terminal"] === true
        @test length(replay.entries) == descriptor["work_unit_count"]
        first_chunk = Backend._rofsj_read_canonical(
            Backend._rofsj_artifact_path(
                root, "chunks", replay.entries[1]["chunk_sha256"]))
        valid_sample = only(filter(
            sample -> sample["status"] == "valid",
            first_chunk["samples"]))
        @test valid_sample["source_reaction_order_matrix"][1][3] == 7.0
        @test valid_sample["reaction_order_matrix"][1] ==
            valid_sample["source_reaction_order_matrix"][1][1:2]
        @test valid_sample["ordered_ro_components"] ==
            valid_sample["reaction_order_matrix"][1]
    end
end

@testset "adaptive job storage rejects symlinked job ancestors" begin
    (Sys.isapple() || Sys.islinux()) || return
    _with_rofsj_store() do store
        job_id = "e"^32
        outside = mktempdir()
        try
            symlink(outside, joinpath(store, job_id))
            probe = joinpath(outside, "probe.json")
            write(probe, "{}")
            @test_throws Backend.ROFieldChunkContractError begin
                Backend._rofsj_read_canonical(
                    joinpath(store, job_id, "probe.json"))
            end
            rm(probe)
            calls = Dict{String,Any}[]
            @test_throws Backend.ROFieldChunkContractError compute_ro_field_job(
                _rofsj_spec(max_multi_indices=1);
                job_context=Dict{String,Any}(
                    "job_id" => job_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" => _rofsj_evaluator(calls),
                ),
            )
            @test isempty(calls)
            @test isempty(readdir(outside))
        finally
            rm(outside; recursive=true, force=true)
        end
    end
end

@testset "replay work meters real points and interpolation independently" begin
    _with_rofsj_store() do _
        replays = Dict{Int,Any}()
        for (dimension, job_id) in ((1, "1"^32), (2, "2"^32))
            result = compute_ro_field_job(
                _rofsj_spec(max_multi_indices=3, dimension=dimension);
                job_context=Dict{String,Any}(
                    "job_id" => job_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(Dict{String,Any}[]),
                ),
            )
            descriptor = result["ro_field_job_result"]
            root = Backend._rofsj_data_root(job_id)
            plan, prepared = Backend._rofsj_read_plan(
                root, descriptor["plan_sha256"])
            replays[dimension] = Backend._rofsj_load_checkpoint(
                root, plan, prepared, descriptor["checkpoint_sha256"])
        end
        one = replays[1]
        two = replays[2]
        @test length(one.entries) == length(two.entries) == 3
        @test one.replay_work_breakdown["point_receipts"] == 5
        @test two.replay_work_breakdown["point_receipts"] == 5
        @test one.replay_work_breakdown["batch_interpolation_work"] == 30
        @test two.replay_work_breakdown["batch_interpolation_work"] == 39
        @test one.replay_work_units != two.replay_work_units
    end
end

@testset "resume and result reads share the explicit replay cap" begin
    _with_rofsj_store() do _
        parent_id = "3"^32
        child_id = "4"^32
        spec = _rofsj_spec(
            max_multi_indices=1, max_replay_work_units=1)
        result = compute_ro_field_job(
            spec;
            job_context=Dict{String,Any}(
                "job_id" => parent_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        descriptor = result["ro_field_job_result"]
        root = Backend._rofsj_data_root(parent_id)
        @test_throws ArgumentError validate_ro_field_job_result!(
            result, parent_id, descriptor["plan_sha256"])
        checkpoint = Backend._rofsj_read_canonical(
            Backend._rofsj_checkpoint_path(
                root, descriptor["checkpoint_sha256"]))
        record = _rofsj_parent_record(
            parent_id, spec, checkpoint; status="cancelled")
        Backend._job_cache_publish!(parent_id, record)
        resume_calls = Dict{String,Any}[]
        try
            child_spec = deepcopy(spec)
            child_spec["resume_from"] = Dict{String,Any}(
                "parent_job_id" => parent_id,
                "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
            )
            @test_throws ArgumentError compute_ro_field_job(
                child_spec;
                job_context=Dict{String,Any}(
                    "job_id" => child_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(resume_calls),
                ),
            )
            @test isempty(resume_calls)
        finally
            Backend._job_cache_remove!(parent_id)
        end
    end
end

@testset "resume replay and artifact copy share one cumulative meter" begin
    _with_rofsj_store() do _
        calibration_parent_id = "b"^32
        calibration_copy_id = "c"^32
        calibration_spec = _rofsj_spec(max_multi_indices=1)
        calibration_result = compute_ro_field_job(
            calibration_spec;
            job_context=Dict{String,Any}(
                "job_id" => calibration_parent_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        calibration_descriptor =
            calibration_result["ro_field_job_result"]
        calibration_root = Backend._rofsj_data_root(calibration_parent_id)
        calibration_checkpoint = Backend._rofsj_read_canonical(
            Backend._rofsj_checkpoint_path(
                calibration_root,
                calibration_descriptor["checkpoint_sha256"],
            ),
        )
        calibration_record = _rofsj_parent_record(
            calibration_parent_id,
            calibration_spec,
            calibration_checkpoint;
            status="cancelled",
        )
        Backend._job_cache_publish!(
            calibration_parent_id, calibration_record)
        single_replay_work = 0
        cumulative_replay_work = 0
        try
            calibration_child_spec =
                normalize_ro_field_job_spec(calibration_spec)
            calibration_child_spec["resume_from"] = Dict{String,Any}(
                "parent_job_id" => calibration_parent_id,
                "checkpoint_sha256" =>
                    calibration_checkpoint["checkpoint_sha256"],
            )
            snapshot = Backend._rofsj_resume_parent_snapshot(
                calibration_child_spec, "alice")
            single_replay_work = snapshot.replay.replay_work_units
            copied = Backend._rofsj_copy_committed_artifacts!(
                Backend._rofsj_data_root(calibration_copy_id),
                snapshot,
                () -> nothing,
            )
            cumulative_replay_work = copied.replay_work_units
            @test cumulative_replay_work > single_replay_work
            @test copied.replay_work_breakdown[
                "copied_artifact_documents"] > 0

            batch_hash =
                snapshot.replay.entries[1]["batch_artifact_sha256"]
            batch_path = Backend._rofsj_artifact_path(
                calibration_root, "batches", batch_hash)
            original_batch_bytes = read(batch_path)
            try
                tampered_batch = Backend._rofsj_read_canonical(batch_path)
                tampered_batch["payload"]["batch_ordinal"] = 99
                Backend._write_job_json(batch_path, tampered_batch)
                @test_throws ArgumentError begin
                    Backend._rofsj_copy_committed_artifacts!(
                        Backend._rofsj_data_root("f"^32),
                        snapshot,
                        () -> nothing,
                    )
                end
            finally
                _restore_bytes(batch_path, original_batch_bytes)
            end
        finally
            Backend._job_cache_remove!(calibration_parent_id)
        end

        middle_cap = single_replay_work +
            (cumulative_replay_work - single_replay_work) ÷ 2
        @test single_replay_work < middle_cap < cumulative_replay_work

        bounded_parent_id = "d"^32
        bounded_child_id = "e"^32
        bounded_spec = _rofsj_spec(
            max_multi_indices=1,
            max_replay_work_units=middle_cap,
        )
        bounded_result = compute_ro_field_job(
            bounded_spec;
            job_context=Dict{String,Any}(
                "job_id" => bounded_parent_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        bounded_descriptor = bounded_result["ro_field_job_result"]
        bounded_root = Backend._rofsj_data_root(bounded_parent_id)
        bounded_plan, bounded_prepared = Backend._rofsj_read_plan(
            bounded_root, bounded_descriptor["plan_sha256"])
        bounded_replay = Backend._rofsj_load_checkpoint(
            bounded_root,
            bounded_plan,
            bounded_prepared,
            bounded_descriptor["checkpoint_sha256"],
        )
        @test bounded_replay.replay_work_units <= middle_cap
        bounded_checkpoint = bounded_replay.checkpoint
        bounded_record = _rofsj_parent_record(
            bounded_parent_id,
            bounded_spec,
            bounded_checkpoint;
            status="cancelled",
        )
        Backend._job_cache_publish!(bounded_parent_id, bounded_record)
        bounded_resume_calls = Dict{String,Any}[]
        try
            bounded_child_spec = normalize_ro_field_job_spec(bounded_spec)
            bounded_child_spec["resume_from"] = Dict{String,Any}(
                "parent_job_id" => bounded_parent_id,
                "checkpoint_sha256" =>
                    bounded_checkpoint["checkpoint_sha256"],
            )
            @test_throws ArgumentError compute_ro_field_job(
                bounded_child_spec;
                job_context=Dict{String,Any}(
                    "job_id" => bounded_child_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(bounded_resume_calls),
                ),
            )
            @test isempty(bounded_resume_calls)
        finally
            Backend._job_cache_remove!(bounded_parent_id)
        end
    end
end

@testset "terminal manifest and result parsing consume the same replay cap" begin
    _with_rofsj_store() do _
        high_id = "5"^32
        high = compute_ro_field_job(
            _rofsj_spec(max_multi_indices=1);
            job_context=Dict{String,Any}(
                "job_id" => high_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        high_descriptor = high["ro_field_job_result"]
        high_root = Backend._rofsj_data_root(high_id)
        high_plan, high_prepared = Backend._rofsj_read_plan(
            high_root, high_descriptor["plan_sha256"])
        checkpoint_only = Backend._rofsj_load_checkpoint(
            high_root, high_plan, high_prepared,
            high_descriptor["checkpoint_sha256"])
        terminal_chain = Backend._rofsj_read_terminal_chain(
            high_root, high_plan, high_prepared,
            high_descriptor["checkpoint_sha256"],
            high_descriptor["dataset_manifest_sha256"])
        @test terminal_chain.replay_work_units >
            checkpoint_only.replay_work_units
        @test terminal_chain.replay_work_breakdown[
            "terminal_result_records"] == 1
        @test terminal_chain.replay_work_breakdown[
            "terminal_result_samples"] == 1

        terminal_increment = terminal_chain.replay_work_units -
            checkpoint_only.replay_work_units
        middle_cap = checkpoint_only.replay_work_units +
            max(1, terminal_increment ÷ 2)
        low_id = "6"^32
        low = compute_ro_field_job(
            _rofsj_spec(
                max_multi_indices=1,
                max_replay_work_units=middle_cap,
            );
            job_context=Dict{String,Any}(
                "job_id" => low_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        low_descriptor = low["ro_field_job_result"]
        low_root = Backend._rofsj_data_root(low_id)
        low_plan, low_prepared = Backend._rofsj_read_plan(
            low_root, low_descriptor["plan_sha256"])
        @test Backend._rofsj_load_checkpoint(
            low_root, low_plan, low_prepared,
            low_descriptor["checkpoint_sha256"]).checkpoint["terminal"]
        @test_throws ArgumentError Backend._rofsj_read_terminal_chain(
            low_root, low_plan, low_prepared,
            low_descriptor["checkpoint_sha256"],
            low_descriptor["dataset_manifest_sha256"])
    end
end

@testset "default evaluator stores the full source Jacobian before pullback" begin
    _with_rofsj_store() do _
        job_id = "0"^32
        result = compute_ro_field_job(
            _rofsj_spec(max_multi_indices=1);
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
            ),
        )
        descriptor = result["ro_field_job_result"]
        @test descriptor["point_count"] == 1
        @test descriptor["valid_count"] == 1
        root = Backend._rofsj_data_root(job_id)
        checkpoint = Backend._rofsj_read_canonical(
            Backend._rofsj_checkpoint_path(
                root, descriptor["checkpoint_sha256"]))
        chunk = Backend._rofsj_read_canonical(
            Backend._rofsj_artifact_path(
                root, "chunks",
                checkpoint["committed"][1]["chunk_sha256"]))
        sample = only(chunk["samples"])
        @test sample["status"] == "valid"
        @test length(sample["source_reaction_order_matrix"]) == 1
        @test length(sample["source_reaction_order_matrix"][1]) == 3
        @test length(sample["reaction_order_matrix"]) == 1
        @test length(sample["reaction_order_matrix"][1]) == 2
        @test sample["reaction_order_matrix"][1] ==
            sample["source_reaction_order_matrix"][1][1:2]
        @test validate_ro_field_job_result!(
            result, job_id, descriptor["plan_sha256"]) == descriptor
    end
end

@testset "cancelled orphan CAS is not committed and resume is exact" begin
    _with_rofsj_store() do _
        parent_id = "b"^32
        child_id = "c"^32
        spec = _rofsj_spec(max_multi_indices=4)
        parent_log = Dict{String,Any}[]
        last_checkpoint = Ref{Any}(nothing)
        orphan_armed = Ref(false)
        root = Backend._rofsj_data_root(parent_id)
        cancel_check = function ()
            chunks = joinpath(root, "chunks")
            if orphan_armed[] && isdir(chunks) &&
               length(readdir(chunks)) >= 2 &&
               last_checkpoint[] !== nothing &&
               last_checkpoint[]["committed_work_unit_count"] == 1
                throw(Backend.LocalJobCancelled(
                    "cancel after orphan CAS writes"))
            end
            return nothing
        end
        @test_throws Backend.LocalJobCancelled compute_ro_field_job(
            spec;
            job_context=Dict{String,Any}(
                "job_id" => parent_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" => _rofsj_evaluator(
                    parent_log; arm_orphan_at=2,
                    orphan_armed=orphan_armed),
                "publish_checkpoint" => checkpoint ->
                    (last_checkpoint[] = deepcopy(checkpoint)),
            ),
            cancel_check=cancel_check,
        )
        checkpoint = last_checkpoint[]
        @test checkpoint["committed_work_unit_count"] == 1
        @test length(readdir(joinpath(root, "chunks"))) == 2
        @test length(checkpoint["committed"]) == 1

        record = _rofsj_parent_record(parent_id, spec, checkpoint)
        Backend._job_cache_publish!(parent_id, record)
        try
            resume = Dict{String,Any}(
                "parent_job_id" => parent_id,
                "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
            )
            child_spec = deepcopy(spec)
            child_spec["resume_from"] = resume
            @test validate_ro_field_resume_parent!(
                child_spec, "alice")["resume_from"] == resume

            snapshot = Backend._rofsj_resume_parent_snapshot(
                normalize_ro_field_job_spec(child_spec), "alice")
            expected_batch = Engine.prepare_ro_sparse_index_batch_v2(
                snapshot.prepared.engine_plan, snapshot.replay.state)
            committed_parent_points = Set(String.(
                snapshot.replay.checkpoint["committed"][1]["chunk_sha256"] ==
                    snapshot.replay.entries[1]["chunk_sha256"] ?
                parent_log[1]["point_ids"] : String[]))

            child_log = Dict{String,Any}[]
            child = compute_ro_field_job(
                child_spec;
                job_context=Dict{String,Any}(
                    "job_id" => child_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(child_log),
                ),
            )
            @test first(child_log)["batch_sha256"] ==
                expected_batch.batch_sha256
            @test first(child_log)["ordinal"] == 2
            @test all(isempty(intersect(
                committed_parent_points, Set(entry["point_ids"])))
                for entry in child_log)
            descriptor = child["ro_field_job_result"]
            @test descriptor["lineage"] == resume

            child_checkpoint = Backend._rofsj_read_canonical(
                Backend._rofsj_checkpoint_path(
                    Backend._rofsj_data_root(child_id),
                    descriptor["checkpoint_sha256"]))
            contextual = _rofsj_parent_record(
                child_id, child_spec, child_checkpoint;
                status="succeeded")
            contextual["resume_from"] = resume
            contextual["ro_field_dataset_manifest_sha256"] =
                descriptor["dataset_manifest_sha256"]
            @test validate_ro_field_job_result!(
                child, child_id, descriptor["plan_sha256"];
                record=contextual) == descriptor

            # Admission deliberately does not traverse the parent's chunks.
            # Corruption is detected only when the child worker replays the
            # linearly committed transition, before its evaluator runs.
            chunk_hash = checkpoint["committed"][1]["chunk_sha256"]
            chunk_path = Backend._rofsj_artifact_path(
                root, "chunks", chunk_hash)
            tampered = Backend._rofsj_read_canonical(chunk_path)
            tampered["samples"][1]["output_values"][1] += 1.0
            Backend._write_job_json(chunk_path, tampered)
            @test validate_ro_field_resume_parent!(
                child_spec, "alice")["resume_from"] == resume
            tampered_calls = Dict{String,Any}[]
            @test_throws Exception compute_ro_field_job(
                child_spec;
                job_context=Dict{String,Any}(
                    "job_id" => "d"^32,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(tampered_calls),
                ),
            )
            @test isempty(tampered_calls)
        finally
            Backend._job_cache_remove!(parent_id)
        end
    end
end

@testset "final reads reject every tampered adaptive layer" begin
    _with_rofsj_store() do _
        job_id = "e"^32
        result = compute_ro_field_job(
            _rofsj_spec(max_multi_indices=3);
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
                "sparse_batch_evaluator" =>
                    _rofsj_evaluator(Dict{String,Any}[]),
            ),
        )
        descriptor = result["ro_field_job_result"]
        root = Backend._rofsj_data_root(job_id)
        checkpoint = Backend._rofsj_read_canonical(
            Backend._rofsj_checkpoint_path(
                root, descriptor["checkpoint_sha256"]))
        entry = checkpoint["committed"][1]
        manifest = Backend._rofsj_read_canonical(
            Backend._rofsj_manifest_path(
                root, descriptor["dataset_manifest_sha256"]))
        terminal_path = Backend._rofsj_artifact_path(
            root, "results", manifest["terminal_artifact_sha256"])
        cases = [
            (Backend._rofsj_plan_path(root, descriptor["plan_sha256"]),
                doc -> (doc["identity"]["channel_order"] = "tampered")),
            (Backend._rofsj_artifact_path(
                root, "batches", entry["batch_artifact_sha256"]),
                doc -> (doc["payload"]["batch_ordinal"] = 99)),
            (Backend._rofsj_artifact_path(
                root, "chunks", entry["chunk_sha256"]),
                doc -> (doc["samples"][1]["output_values"][1] += 1.0)),
            (Backend._rofsj_artifact_path(
                root, "states", entry["next_state_artifact_sha256"]),
                doc -> (doc["payload"]["backend_work_unit_count"] = 99)),
            (Backend._rofsj_checkpoint_path(
                root, descriptor["checkpoint_sha256"]),
                doc -> (doc["committed_point_count"] += 1)),
            (Backend._rofsj_manifest_path(
                root, descriptor["dataset_manifest_sha256"]),
                doc -> (doc["valid_count"] += 1)),
            (terminal_path,
                doc -> (doc["payload"]["status"] = "unknown_gap")),
        ]
        for (path, mutate!) in cases
            original_bytes = read(path)
            document = Backend._rofsj_read_canonical(path)
            mutate!(document)
            Backend._write_job_json(path, document)
            @test_throws Exception validate_ro_field_job_result!(
                result, job_id, descriptor["plan_sha256"])
            _restore_bytes(path, original_bytes)
            @test validate_ro_field_job_result!(
                result, job_id, descriptor["plan_sha256"]) == descriptor
        end
        altered_result = deepcopy(result)
        altered_result["ro_field_job_result"]["valid_count"] += 1
        @test_throws ArgumentError validate_ro_field_job_result!(
            altered_result, job_id, descriptor["plan_sha256"])

        # Even a semantically altered terminal token with a fresh local self
        # hash is rejected against the replayed terminal state.
        terminal = Backend._rofsj_read_canonical(terminal_path)
        altered_payload = deepcopy(terminal["payload"])
        altered_payload["status"] = "unknown_gap"
        result_body = deepcopy(altered_payload)
        pop!(result_body, "result_sha256")
        altered_payload["result_sha256"] =
            Engine._ross_sha256(result_body)
        plan, prepared = Backend._rofsj_read_plan(
            root, descriptor["plan_sha256"])
        replay = Backend._rofsj_load_checkpoint(
            root, plan, prepared, descriptor["checkpoint_sha256"])
        @test_throws ArgumentError Engine.restore_ro_sparse_result_v2(
            prepared.engine_plan, replay.state, altered_payload)
    end
end

@testset "terminal artifacts are reserved before publication" begin
    _with_rofsj_store() do _
        job_id = "9"^32
        calls = Dict{String,Any}[]
        @test_throws Backend.ROFieldJobPayloadLimitExceeded begin
            compute_ro_field_job(
                _rofsj_spec(
                    max_multi_indices=1,
                    max_interpolation_work=1,
                    max_payload_bytes=1_024,
                );
                job_context=Dict{String,Any}(
                    "job_id" => job_id,
                    "user_sub" => "alice",
                    "sparse_batch_evaluator" =>
                        _rofsj_evaluator(calls),
                ),
            )
        end
        @test isempty(calls)
        results_dir = joinpath(
            Backend._rofsj_data_root(job_id), "results")
        @test !isdir(results_dir) || isempty(readdir(results_dir))
    end
end

@testset "local_async zero-work terminal result is immutable" begin
    _with_rofsj_store() do _
        submitted = submit_biocircuits_job_from_spec(Dict{String,Any}(
            "kind" => "compute_ro_field",
            "spec" => _rofsj_spec(
                max_multi_indices=1, max_interpolation_work=1),
            "execution" => Dict("mode" => "local_async"),
        ); user_sub="adaptive-contract-user")
        job_id = submitted["job_id"]
        task = lock(Backend.JOBS_LOCK) do
            get(Backend.JOB_TASKS, job_id, nothing)
        end
        task === nothing || wait(task)
        final = get_biocircuits_job(
            job_id; user_sub="adaptive-contract-user")
        @test final["status"] == "succeeded"
        @test final["ro_field"]["artifact_namespace"] ==
            "ro-field-sparse-v2"
        @test occursin("/ro-field-sparse-v2/plans/",
            final["ro_field"]["plan_ref"])
        @test haskey(final["artifacts"], "result_manifest")
        fetched = get_biocircuits_job_result(
            job_id; user_sub="adaptive-contract-user")["result"]
        @test fetched["ro_field_job_result"]["point_count"] == 0
        record = Backend._job_record(job_id)
        @test Backend._verify_job_result_artifact(record).status == :valid
        _rofsj_assert_outer_manifest_identity_bound(record)
        before = read(Backend._job_record_path(job_id))
        transition = Backend._job_transition!(
            job_id, "succeeded";
            expected=("succeeded",),
            latest_checkpoint_sha256="f"^64,
        )
        @test transition.applied === false
        @test read(Backend._job_record_path(job_id)) == before

        # The outer result manifest is the commit marker for exact result
        # bytes; changing the result after success must fail closed on read.
        result_path = Backend._job_result_path(job_id)
        altered_outer = Backend._read_job_json(result_path)
        altered_outer["ro_field_job_result"]["status"] = "tampered"
        Backend._write_job_json(result_path, altered_outer)
        @test Backend._verify_job_result_artifact(record).status == :invalid
        @test_throws ArgumentError get_biocircuits_job_result(
            job_id; user_sub="adaptive-contract-user")
        Backend._job_cache_remove!(job_id)
    end
end

end # module
