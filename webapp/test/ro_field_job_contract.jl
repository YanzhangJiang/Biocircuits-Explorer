module ROFieldJobContractTests

using Test
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function _rofjob_network()
    return network_ir_to_dict(network_ir_from_legacy(
        ["A + B <-> AB"], [1.0];
        label="ro-field-job-heterodimer",
        input_symbols=["tA", "tB"],
        output_symbols=["AB"],
    ))
end

function _rofjob_request(; coordinates=Any[[-1.0, 1.0], [-1.0, 0.0, 1.0]])
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_REQUEST_VERSION,
        "network" => _rofjob_network(),
        "representation" => "sampled_grid",
        "domain" => Dict{String,Any}(
            "domain_kind" => "axis_aligned_log_box",
            "coordinate_space" => "dimensionless_log_ratio",
            "log_basis" => "log10",
            "axis_order" => Any["input_a", "input_b"],
            "axes" => Any[
                Dict{String,Any}(
                    "axis_id" => "input_a", "symbol" => "tA",
                    "coordinate_kind" => "conserved_total",
                    "orientation" => "increasing_physical_value",
                    "reference" => Dict("value" => 1.0, "unit" => "uM"),
                    "bounds" => Dict("lower" => -1.0, "upper" => 1.0),
                ),
                Dict{String,Any}(
                    "axis_id" => "input_b", "symbol" => "tB",
                    "coordinate_kind" => "conserved_total",
                    "orientation" => "increasing_physical_value",
                    "reference" => Dict("value" => 1.0, "unit" => "uM"),
                    "bounds" => Dict("lower" => -1.0, "upper" => 1.0),
                ),
            ],
            "fixed_background" => Any[
                Dict{String,Any}(
                    "parameter_id" => "kd1", "symbol" => "Kd1",
                    "coordinate_kind" => "binding_constant",
                    "reference" => Dict("value" => 1.0, "unit" => "uM"),
                    "log_value" => 0.0,
                ),
            ],
        ),
        "outputs" => Dict{String,Any}(
            "output_order" => Any["output_ab"],
            "items" => Any[
                Dict{String,Any}(
                    "output_id" => "output_ab", "symbol" => "AB",
                    "observable_kind" => "species_concentration",
                    "reference" => Dict("value" => 1.0, "unit" => "uM"),
                ),
            ],
        ),
        "sampling" => Dict{String,Any}(
            "scheme" => "cartesian_product",
            "axis_coordinates" => coordinates,
        ),
        "work_budget" => Dict{String,Any}(
            "work_unit_kind" => "solver_samples",
            "max_evaluated_items" => 64,
            "max_stored_items" => 64,
            "max_payload_bytes" => 1_048_576,
            "deadline_seconds" => nothing,
        ),
        "storage" => Dict{String,Any}("mode" => "chunked"),
    )
end

function _rofjob_spec(; coordinates=Any[[-1.0, 1.0], [-1.0, 0.0, 1.0]],
                      resume_from=nothing)
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_JOB_SPEC_VERSION,
        "request" => _rofjob_request(coordinates=coordinates),
        "work_unit_size" => 4,
        "resume_from" => resume_from,
    )
end

function _rofjob_evaluator(call_log; invalid_first=false, stop_after=nothing)
    return function (_, _, unit, cancel_check)
        push!(call_log, unit["ordinal"])
        stop_after !== nothing && length(call_log) > stop_after &&
            throw(Backend.LocalJobCancelled("test checkpoint stop"))
        samples = Dict{String,Any}[]
        for (position, point) in enumerate(unit["points"])
            cancel_check()
            if invalid_first && unit["ordinal"] == 1 && position == 1
                push!(samples, Dict{String,Any}(
                    "status" => "invalid",
                    "gap" => Dict{String,Any}(
                        "reason" => "synthetic_invalid",
                        "detail" => "contract gap",
                    ),
                ))
            else
                push!(samples, Dict{String,Any}(
                    "status" => "valid",
                    "output_values" => Any[sum(point)],
                    "reaction_order_matrix" => Any[Any[1.0, 1.0]],
                    "regime_id" => "regime-contract",
                ))
            end
        end
        return samples
    end
end

function _with_rofjob_store(f::Function)
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

function _rofjob_assert_outer_manifest_identity_bound(record)
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

function _rofjob_seed_record(job_id, spec, status;
                             user_sub="alice", checkpoint=nothing)
    prepared = Backend._prepare_job_spec_and_artifact_identity(
        "compute_ro_field", spec)
    now = Backend._now_iso_timestamp()
    record = Dict{String,Any}(
        "job_id" => job_id,
        "kind" => "compute_ro_field",
        "status" => status,
        "executor" => "local_async",
        "user_sub" => user_sub,
        "created_at" => now,
        "updated_at" => now,
        "state_revision" => 1,
        "result_available" => false,
        "progress" => Dict("message" => "seeded"),
        "spec" => prepared.spec,
        "expected_artifact_config_hash" =>
            prepared.expected_artifact_config_hash,
        "input_path" => Backend._job_input_path(job_id),
        "status_path" => Backend._job_status_path(job_id),
        "record_path" => Backend._job_record_path(job_id),
        "result_path" => Backend._job_result_path(job_id),
        "input_uri" => Backend._job_input_path(job_id),
        "status_uri" => Backend._job_status_path(job_id),
        "result_uri" => Backend._job_result_path(job_id),
        "result_protocol_version" => Backend.JOB_RESULT_PROTOCOL_VERSION,
        "result_manifest_path" => Backend._job_result_manifest_path(job_id),
        "result_manifest_uri" => Backend._job_result_manifest_path(job_id),
        "ro_field_plan_sha256" => prepared.spec["plan"]["plan_sha256"],
        "ro_field_network_ir_sha256" => prepared.spec["plan"]["identity"][
            "computation_spec"]["network_ir_sha256"],
        "resume_from" => deepcopy(prepared.spec["resume_from"]),
    )
    if checkpoint !== nothing
        record["latest_checkpoint_sha256"] = checkpoint["checkpoint_sha256"]
        record["committed_work_unit_count"] =
            checkpoint["committed_work_unit_count"]
        record["committed_point_count"] = checkpoint["committed_point_count"]
        record["committed_payload_bytes"] =
            checkpoint["committed_payload_bytes"]
    end
    Backend._write_job_json(record["record_path"], record)
    return record
end

@testset "RO-field job normalization freezes scientific identity" begin
    raw_spec = _rofjob_spec()
    normalized = normalize_ro_field_job_spec(raw_spec)
    @test normalized["schema_version"] == RO_FIELD_JOB_SPEC_VERSION
    @test normalized["request"]["storage"] == Dict("mode" => "chunked")
    @test normalized["plan"]["identity"]["point_count"] == 6
    @test normalized["plan"]["identity"]["work_unit_size"] == 4
    @test Backend._canonical_hash(normalized["plan"]["identity"]) ==
        normalized["plan"]["plan_sha256"]

    prepared = Backend._prepare_job_spec_and_artifact_identity(
        "compute_ro_field", raw_spec)
    @test prepared.expected_artifact_config_hash ==
        normalized["plan"]["plan_sha256"]
    @test prepared.spec == normalized

    resumed = normalize_ro_field_job_spec(_rofjob_spec(resume_from=Dict(
        "parent_job_id" => "a"^32,
        "checkpoint_sha256" => "b"^64,
    )))
    @test resumed["plan"]["plan_sha256"] ==
        normalized["plan"]["plan_sha256"]
    @test resumed["resume_from"] != nothing

    too_small = _rofjob_spec(
        coordinates=Any[[-1.0, 1.0], [-1.0, 1.0]])
    too_small["work_unit_size"] = 3
    @test_throws ArgumentError normalize_ro_field_job_spec(too_small)
    hash_source = deepcopy(_rofjob_spec())
    delete!(hash_source["request"], "network")
    hash_source["request"]["network_ir_hash"] = "a"^64
    @test_throws ArgumentError normalize_ro_field_job_spec(hash_source)
    aws = Dict{String,Any}(
        "kind" => "compute_ro_field",
        "spec" => _rofjob_spec(),
        "execution" => Dict("mode" => "aws_batch"),
    )
    @test_throws ArgumentError submit_biocircuits_job_from_spec(aws)

    tight = _rofjob_spec()
    # The semantic inline estimate is 9,152 bytes, so normalization accepts
    # this declaration.  The conservative next-chunk reservation must still
    # reject before invoking an evaluator.
    tight["request"]["work_budget"]["max_payload_bytes"] = 9_500
    calls = Int[]
    @test_throws Backend.ROFieldJobPayloadLimitExceeded compute_ro_field_job(
        tight;
        job_context=Dict{String,Any}(
            "job_id" => "9"^32,
            "user_sub" => "alice",
            "evaluator" => _rofjob_evaluator(calls),
        ),
    )
    @test isempty(calls)
end

@testset "chunked job result commits gaps and validates every artifact" begin
    _with_rofjob_store() do _
        job_id = "1"^32
        calls = Int[]
        checkpoints = String[]
        result = compute_ro_field_job(
            _rofjob_spec();
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
                "evaluator" => _rofjob_evaluator(calls; invalid_first=true),
                "publish_checkpoint" => checkpoint ->
                    push!(checkpoints, checkpoint["checkpoint_sha256"]),
            ),
        )
        descriptor = result["ro_field_job_result"]
        @test calls == [1, 2]
        @test length(checkpoints) >= 2
        @test descriptor["point_count"] == 6
        @test descriptor["valid_count"] == 5
        @test descriptor["invalid_count"] == 1
        @test descriptor["chunk_payload_bytes"] > 0
        @test descriptor["lineage"] === nothing
        @test validate_ro_field_job_result!(
            result, job_id, descriptor["plan_sha256"]) == descriptor

        manifest_path = Backend._rofjob_manifest_path(
            Backend._rofjob_data_root(job_id),
            descriptor["dataset_manifest_sha256"])
        tampered = Backend._rofjob_read_document(manifest_path)
        tampered["valid_count"] = 6
        Backend._write_job_json(manifest_path, tampered)
        @test_throws Exception validate_ro_field_job_result!(
            result, job_id, descriptor["plan_sha256"])
    end
end

@testset "chunked job storage rejects symlinked job ancestors" begin
    (Sys.isapple() || Sys.islinux()) || return
    _with_rofjob_store() do store
        job_id = "e"^32
        outside = mktempdir()
        try
            symlink(outside, joinpath(store, job_id))
            probe = joinpath(outside, "probe.json")
            write(probe, "{}")
            @test_throws Backend.ROFieldChunkContractError begin
                Backend._rofjob_read_document(
                    joinpath(store, job_id, "probe.json"))
            end
            rm(probe)
            calls = Int[]
            @test_throws Backend.ROFieldChunkContractError compute_ro_field_job(
                _rofjob_spec();
                job_context=Dict{String,Any}(
                    "job_id" => job_id,
                    "user_sub" => "alice",
                    "evaluator" => _rofjob_evaluator(calls),
                ),
            )
            @test isempty(calls)
            @test isempty(readdir(outside))
        finally
            rm(outside; recursive=true, force=true)
        end
    end
end

@testset "terminal result rejects checkpoint and manifest split brain" begin
    _with_rofjob_store() do _
        job_id = "7"^32
        calls = Int[]
        result = compute_ro_field_job(
            _rofjob_spec();
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
                "evaluator" => _rofjob_evaluator(calls),
            ),
        )
        descriptor = result["ro_field_job_result"]
        root = Backend._rofjob_data_root(job_id)
        plan = Backend._rofjob_read_document(Backend._rofjob_plan_path(root))
        manifest = Backend._rofjob_read_document(
            Backend._rofjob_manifest_path(
                root, descriptor["dataset_manifest_sha256"]))
        chunks = Backend._rofjob_chunks_from_entries(
            root, plan, manifest["chunks"])
        units = ro_field_plan_work_units(plan)

        alternate_samples = Any[
            Dict{String,Any}(
                "status" => "valid",
                "output_values" => deepcopy(sample["output_values"]),
                "reaction_order_matrix" =>
                    deepcopy(sample["reaction_order_matrix"]),
                "regime_id" => sample["regime_id"],
            )
            for sample in chunks[1]["samples"]
        ]
        alternate_samples[1]["output_values"][1] += 1.0
        alternate_chunk = build_ro_field_chunk(
            plan, units[1], alternate_samples)
        @test length(Backend._rofc_bytes(alternate_chunk)) ==
            length(Backend._rofc_bytes(chunks[1]))
        write_ro_field_chunk!(
            root, alternate_chunk; plan=plan, work_unit=units[1])

        checkpoint_chunks = copy(chunks)
        checkpoint_chunks[1] = alternate_chunk
        split_checkpoint = build_ro_field_checkpoint(plan, checkpoint_chunks)
        Backend._rofjob_write_once!(
            Backend._rofjob_checkpoint_path(
                root, split_checkpoint["checkpoint_sha256"]),
            split_checkpoint,
        )
        split_result = deepcopy(result)
        split_descriptor = split_result["ro_field_job_result"]
        split_descriptor["checkpoint_sha256"] =
            split_checkpoint["checkpoint_sha256"]
        split_descriptor["storage"]["checkpoint_ref"] =
            "job://$(job_id)/ro-field/checkpoints/" *
            split_checkpoint["checkpoint_sha256"]

        @test_throws ArgumentError validate_ro_field_job_result!(
            split_result, job_id, descriptor["plan_sha256"])
    end
end

@testset "resume creates a child and never reevaluates committed points" begin
    _with_rofjob_store() do _
        parent_id = "2"^32
        child_id = "3"^32
        parent_calls = Int[]
        captured = Ref{Any}(nothing)
        @test_throws Backend.LocalJobCancelled compute_ro_field_job(
            _rofjob_spec();
            job_context=Dict{String,Any}(
                "job_id" => parent_id,
                "user_sub" => "alice",
                "evaluator" => _rofjob_evaluator(
                    parent_calls; stop_after=1),
                "publish_checkpoint" => checkpoint ->
                    (captured[] = deepcopy(checkpoint)),
            ),
        )
        @test parent_calls == [1, 2]
        checkpoint = captured[]
        @test checkpoint !== nothing
        @test checkpoint["committed_point_count"] == 4

        parent_record = Dict{String,Any}(
            "job_id" => parent_id,
            "kind" => "compute_ro_field",
            "status" => "failed",
            "executor" => "local_async",
            "user_sub" => "alice",
            "ro_field_plan_sha256" => checkpoint["plan_sha256"],
            "latest_checkpoint_sha256" => checkpoint["checkpoint_sha256"],
        )
        Backend._job_cache_publish!(parent_id, parent_record)
        try
            resume = Dict{String,Any}(
                "parent_job_id" => parent_id,
                "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
            )
            child_spec = _rofjob_spec(resume_from=resume)
            @test validate_ro_field_resume_parent!(child_spec, "alice")[
                "plan"]["plan_sha256"] == checkpoint["plan_sha256"]
            @test_throws ArgumentError validate_ro_field_resume_parent!(
                child_spec, "mallory")

            child_calls = Int[]
            child = compute_ro_field_job(
                child_spec;
                job_context=Dict{String,Any}(
                    "job_id" => child_id,
                    "user_sub" => "alice",
                    "evaluator" => _rofjob_evaluator(child_calls),
                ),
            )
            @test child_calls == [2]
            descriptor = child["ro_field_job_result"]
            @test descriptor["point_count"] == 6
            @test descriptor["lineage"] == resume
            contextual_record = Dict{String,Any}(
                "job_id" => child_id,
                "ro_field_plan_sha256" => checkpoint["plan_sha256"],
                "resume_from" => resume,
                "latest_checkpoint_sha256" => descriptor["checkpoint_sha256"],
                "ro_field_dataset_manifest_sha256" =>
                    descriptor["dataset_manifest_sha256"],
            )
            @test validate_ro_field_job_result!(
                child, child_id, checkpoint["plan_sha256"];
                record=contextual_record) == descriptor
            wrong_lineage = deepcopy(contextual_record)
            wrong_lineage["resume_from"] = nothing
            @test_throws ArgumentError validate_ro_field_job_result!(
                child, child_id, checkpoint["plan_sha256"];
                record=wrong_lineage)
            wrong_checkpoint = deepcopy(contextual_record)
            wrong_checkpoint["latest_checkpoint_sha256"] = "f"^64
            @test_throws ArgumentError validate_ro_field_job_result!(
                child, child_id, checkpoint["plan_sha256"];
                record=wrong_checkpoint)
            wrong_manifest = deepcopy(contextual_record)
            wrong_manifest["ro_field_dataset_manifest_sha256"] = "e"^64
            @test_throws ArgumentError validate_ro_field_job_result!(
                child, child_id, checkpoint["plan_sha256"];
                record=wrong_manifest)

            full_checkpoint = Backend._rofjob_read_document(
                Backend._rofjob_checkpoint_path(
                    Backend._rofjob_data_root(child_id),
                    descriptor["checkpoint_sha256"]))
            full_record = Dict{String,Any}(
                "job_id" => child_id,
                "kind" => "compute_ro_field",
                "status" => "failed",
                "executor" => "local_async",
                "user_sub" => "alice",
                "ro_field_plan_sha256" => descriptor["plan_sha256"],
                "latest_checkpoint_sha256" =>
                    descriptor["checkpoint_sha256"],
            )
            Backend._job_cache_publish!(child_id, full_record)
            complete_resume = Dict{String,Any}(
                "parent_job_id" => child_id,
                "checkpoint_sha256" => descriptor["checkpoint_sha256"],
            )
            zero_calls = Int[]
            replay = compute_ro_field_job(
                _rofjob_spec(resume_from=complete_resume);
                job_context=Dict{String,Any}(
                    "job_id" => "6"^32,
                    "user_sub" => "alice",
                    "evaluator" => _rofjob_evaluator(zero_calls),
                ),
            )
            @test isempty(zero_calls)
            @test replay["ro_field_job_result"]["point_count"] == 6
            @test full_checkpoint["committed_point_count"] == 6
        finally
            Backend._job_cache_remove!(parent_id)
            Backend._job_cache_remove!(child_id)
        end
    end
end

@testset "local_async publishes outer manifest before success" begin
    _with_rofjob_store() do _
        submitted = submit_biocircuits_job_from_spec(Dict{String,Any}(
            "kind" => "compute_ro_field",
            "spec" => _rofjob_spec(
                coordinates=Any[[-1.0, 1.0], [-1.0, 1.0]]),
            "execution" => Dict("mode" => "local_async"),
        ); user_sub="contract-user")
        job_id = submitted["job_id"]
        task = lock(Backend.JOBS_LOCK) do
            get(Backend.JOB_TASKS, job_id, nothing)
        end
        task === nothing || wait(task)
        final = get_biocircuits_job(job_id; user_sub="contract-user")
        @test final["status"] == "succeeded"
        @test final["result_available"] === true
        @test haskey(final["artifacts"], "result_manifest")
        @test haskey(final["ro_field"], "dataset_manifest_sha256")
        @test isfile(Backend._job_result_path(job_id))
        @test isfile(Backend._job_result_manifest_path(job_id))
        record = Backend._job_record(job_id)
        @test Backend._verify_job_result_artifact(record).status == :valid
        missing_outer_anchor = deepcopy(record)
        delete!(missing_outer_anchor, "ro_field_outer_result_sha256")
        @test Backend._verify_job_result_artifact(
            missing_outer_anchor).status == :invalid
        missing_manifest_anchor = deepcopy(record)
        delete!(missing_manifest_anchor, "ro_field_dataset_manifest_sha256")
        @test Backend._verify_job_result_artifact(
            missing_manifest_anchor).status == :invalid
        result = get_biocircuits_job_result(
            job_id; user_sub="contract-user")["result"]
        @test result["ro_field_job_result"]["point_count"] == 4

        original_result_bytes = read(String(record["result_uri"]))
        original_manifest_bytes = read(String(record["result_manifest_uri"]))
        replacement = deepcopy(result)
        replacement["artifact"]["created_at"] = "2099-01-01T00:00:00Z"
        replacement_manifest = Backend._publish_job_result_with_manifest(
            replacement,
            job_id,
            "compute_ro_field",
            record["expected_artifact_config_hash"],
            record["result_uri"],
            record["result_manifest_uri"],
        )
        @test replacement_manifest["result"]["sha256"] !=
            record["ro_field_outer_result_sha256"]
        @test Backend._verify_job_result_artifact(record).status == :invalid

        write(String(record["result_uri"]), original_result_bytes)
        write(String(record["result_manifest_uri"]), original_manifest_bytes)
        @test Backend._verify_job_result_artifact(record).status == :valid
        _rofjob_assert_outer_manifest_identity_bound(record)

        descriptor = result["ro_field_job_result"]
        root = Backend._rofjob_data_root(job_id)
        manifest = Backend._rofjob_read_document(
            Backend._rofjob_manifest_path(
                root, descriptor["dataset_manifest_sha256"]))
        first_chunk_hash = manifest["chunks"][1]["chunk_sha256"]
        first_chunk_path = joinpath(root, "chunks", first_chunk_hash * ".json")
        tampered_chunk = Backend._rofjob_read_document(first_chunk_path)
        tampered_chunk["samples"][1]["output_values"][1] += 1.0
        Backend._write_job_json(first_chunk_path, tampered_chunk)
        @test Backend._verify_job_result_artifact(record).status == :invalid
        @test_throws ArgumentError get_biocircuits_job_result(
            job_id; user_sub="contract-user")
    end
end

@testset "restart and cancellation preserve only linearized checkpoint lineage" begin
    _with_rofjob_store() do _
        job_id = "4"^32
        calls = Int[]
        captured = Ref{Any}(nothing)
        @test_throws Backend.LocalJobCancelled compute_ro_field_job(
            _rofjob_spec();
            job_context=Dict{String,Any}(
                "job_id" => job_id,
                "user_sub" => "alice",
                "evaluator" => _rofjob_evaluator(calls; stop_after=1),
                "publish_checkpoint" => checkpoint ->
                    (captured[] = deepcopy(checkpoint)),
            ),
        )
        checkpoint = captured[]
        record = _rofjob_seed_record(
            job_id, _rofjob_spec(), "running"; checkpoint=checkpoint)
        Backend._job_cache_remove!(job_id)

        recovered = Backend._job_record(job_id)
        @test recovered["status"] == "failed"
        @test recovered["error_code"] == Backend.LOCAL_JOB_RESTART_ERROR_CODE
        @test recovered["latest_checkpoint_sha256"] ==
            checkpoint["checkpoint_sha256"]
        @test recovered["committed_point_count"] == 4
        @test recovered["committed_payload_bytes"] ==
            checkpoint["committed_payload_bytes"]
        bytes_before = read(Backend._job_record_path(job_id))
        unchanged = Backend._with_job_lock(job_id) do
            Backend._transition_job_record_unlocked!(
                Backend.JOBS[job_id], "failed";
                expected=("failed",),
                latest_checkpoint_sha256="f"^64)
        end
        @test unchanged === false
        @test read(Backend._job_record_path(job_id)) == bytes_before
        Backend._job_cache_remove!(job_id)

        cancel_id = "5"^32
        cancel_record = _rofjob_seed_record(
            cancel_id, _rofjob_spec(), "running"; checkpoint=checkpoint)
        Backend._job_cache_publish!(cancel_id, cancel_record)
        try
            requested = cancel_biocircuits_job(cancel_id; user_sub="alice")
            @test requested["status"] == "cancel_requested"
            late = Backend._job_transition!(
                cancel_id, "running";
                expected=("running",),
                latest_checkpoint_sha256="e"^64)
            @test late.applied === false
            settled = Backend._finish_local_job!(cancel_id; succeeded=true)
            @test settled.applied === true
            @test settled.record["status"] == "cancelled"
            @test settled.record["latest_checkpoint_sha256"] ==
                checkpoint["checkpoint_sha256"]
            @test settled.record["result_available"] === false
        finally
            Backend._job_cache_remove!(cancel_id)
        end
    end
end

end # module
