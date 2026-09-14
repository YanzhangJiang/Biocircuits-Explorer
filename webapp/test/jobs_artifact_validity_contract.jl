module JobsArtifactValidityContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function drain_and_reset_job_runtime!()
    tasks = lock(Backend.JOBS_LOCK) do
        collect(values(Backend.JOB_TASKS))
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
    lock(Backend.JOBS_LOCK) do
        isempty(Backend.LOCAL_JOB_ADMISSIONS) ||
            error("Local job admission reservations leaked across test fixtures.")
        empty!(Backend.JOBS)
        empty!(Backend.JOB_CACHE_LAST_ACCESS)
        Backend.JOB_CACHE_ACCESS_CLOCK[] = UInt64(0)
        Backend.JOB_CACHE_CAPACITY[] = nothing
        empty!(Backend.JOB_TASKS)
        empty!(Backend.LOCAL_JOB_CANCEL_TOKENS)
        empty!(Backend.JOB_STATUS_PROJECTION_DIRTY)
        Backend.LOCAL_JOB_LIMITS[] = nothing
        Backend.LOCAL_JOB_RUN_SEMAPHORE[] = nothing
    end
    lock(Backend.JOB_STORE_DURABILITY_LOCK) do
        empty!(Backend.JOB_STORE_PENDING_DIR_FSYNC)
        Backend.JOB_STORE_DURABILITY_GENERATION[] = UInt64(0)
    end
    return nothing
end

function with_isolated_job_store(f::Function)
    previous_env = haskey(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE") ?
        ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] : nothing
    previous_store_dir = Backend.LOCAL_JOB_STORE_DIR[]

    mktempdir() do dir
        try
            drain_and_reset_job_runtime!()
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            Backend.LOCAL_JOB_STORE_DIR[] = nothing
            f(dir)
        finally
            drain_and_reset_job_runtime!()
            if previous_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_env
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
        end
    end
end

@testset "Local result artifact durability contract" begin
    with_isolated_job_store() do store_dir
        @testset "local worker artifacts require directory durability" begin
            publication_root = joinpath(store_dir, "local-worker-publication")
            mkpath(publication_root)
            spec = Dict{String, Any}(
                "library" => Backend.atlas_library_default(),
                "query" => Dict("limit" => 1),
            )
            expected_hash = Backend._canonical_hash(spec)

            make_payload = function(job_id, status_path, result_path;
                                    manifest_path=nothing)
                artifacts = Dict{String, Any}(
                    "status" => status_path,
                    "result" => result_path,
                )
                payload = Dict{String, Any}(
                    "job_id" => job_id,
                    "kind" => "query_atlas",
                    "executor" => "local-worker-test",
                    "spec" => spec,
                    "artifacts" => artifacts,
                )
                if manifest_path !== nothing
                    artifacts["result_manifest"] = manifest_path
                    payload["result_protocol_version"] =
                        Backend.JOB_RESULT_PROTOCOL_VERSION
                    payload["expected_artifact_config_hash"] = expected_hash
                end
                return payload
            end

            make_ops = function(failing_directory, failures_remaining, events)
                failing_directory = abspath(failing_directory)
                return Backend._JobPersistenceOps(
                    (io, path) -> begin
                        push!(events, "file:" * abspath(path))
                        Backend._fsync_job_file_posix!(io, path)
                    end,
                    (source, destination) -> begin
                        push!(events, "rename:" * abspath(destination))
                        Backend._atomic_replace_job_file_posix!(source, destination)
                    end,
                    path -> begin
                        directory = abspath(path)
                        push!(events, "dir:" * directory)
                        should_fail = directory == failing_directory &&
                            (failures_remaining[] < 0 || failures_remaining[] > 0)
                        if should_fail
                            failures_remaining[] > 0 && (failures_remaining[] -= 1)
                            error("injected persistent artifact directory fsync failure")
                        end
                        Backend._fsync_job_directory_posix!(directory)
                    end,
                )
            end

            # Manifest-protocol publication uses three independent directories.
            # Creating the result directory must first sync its parent; a first
            # result-directory fsync failure is retried before the manifest is
            # allowed to become visible.
            manifest_root = joinpath(publication_root, "manifest-retry")
            status_dir = joinpath(manifest_root, "status")
            result_dir = joinpath(manifest_root, "result")
            manifest_dir = joinpath(manifest_root, "manifest")
            mkpath(status_dir)
            status_path = joinpath(status_dir, "status.json")
            result_path = joinpath(result_dir, "result.json")
            manifest_path = joinpath(manifest_dir, "result-manifest.json")
            events = String[]
            failures_remaining = Ref(1)
            retry_ops = make_ops(result_dir, failures_remaining, events)
            manifest_result = Backend._run_biocircuits_job_payload_with_ops(
                make_payload(
                    "local-manifest-retry",
                    status_path,
                    result_path;
                    manifest_path=manifest_path,
                ),
                retry_ops,
            )
            @test manifest_result["artifact"]["kind"] == "query_atlas"
            @test isfile(result_path)
            @test isfile(manifest_path)
            @test Backend._read_job_json(status_path)["status"] == "succeeded"
            @test failures_remaining[] == 0
            @test Backend._pending_job_store_dir_generation(result_dir) === nothing
            result_parent_sync = findfirst(
                ==("dir:" * abspath(manifest_root)),
                events,
            )
            result_staging_sync = findfirst(
                event -> startswith(event, "file:" * abspath(result_dir) * "/"),
                events,
            )
            result_rename = findfirst(
                ==("rename:" * abspath(result_path)),
                events,
            )
            result_directory_syncs = findall(
                ==("dir:" * abspath(result_dir)),
                events,
            )
            @test result_parent_sync !== nothing
            @test result_staging_sync !== nothing
            @test result_rename !== nothing
            @test length(result_directory_syncs) == 2
            @test result_parent_sync < result_staging_sync < result_rename <
                  first(result_directory_syncs)

            # The pre-manifest compatibility path applies the same strict local
            # result rule and may publish success only after its retry succeeds.
            legacy_root = joinpath(publication_root, "legacy-retry")
            legacy_status_dir = joinpath(legacy_root, "status")
            legacy_result_dir = joinpath(legacy_root, "result")
            mkpath(legacy_status_dir)
            legacy_status = joinpath(legacy_status_dir, "status.json")
            legacy_result = joinpath(legacy_result_dir, "result.json")
            legacy_failures = Ref(1)
            legacy_ops = make_ops(legacy_result_dir, legacy_failures, String[])
            Backend._run_biocircuits_job_payload_with_ops(
                make_payload(
                    "local-legacy-retry",
                    legacy_status,
                    legacy_result,
                ),
                legacy_ops,
            )
            @test isfile(legacy_result)
            @test Backend._read_job_json(legacy_status)["status"] == "succeeded"
            @test legacy_failures[] == 0
            @test Backend._pending_job_store_dir_generation(
                legacy_result_dir,
            ) === nothing

            # A persistent result-directory failure leaves the renamed result
            # visible in this process, but must stop before manifest/success.
            failed_root = joinpath(publication_root, "manifest-failure")
            failed_status_dir = joinpath(failed_root, "status")
            failed_result_dir = joinpath(failed_root, "result")
            failed_manifest_dir = joinpath(failed_root, "manifest")
            mkpath(failed_status_dir)
            failed_status = joinpath(failed_status_dir, "status.json")
            failed_result = joinpath(failed_result_dir, "result.json")
            failed_manifest = joinpath(
                failed_manifest_dir,
                "result-manifest.json",
            )
            persistent_failures = Ref(-1)
            persistent_ops = make_ops(
                failed_result_dir,
                persistent_failures,
                String[],
            )
            failure = try
                Backend._run_biocircuits_job_payload_with_ops(
                    make_payload(
                        "local-manifest-failure",
                        failed_status,
                        failed_result;
                        manifest_path=failed_manifest,
                    ),
                    persistent_ops,
                )
                nothing
            catch err
                err
            end
            @test failure isa ErrorException
            @test occursin("directory durability retry failed", sprint(showerror, failure))
            @test isfile(failed_result)
            @test !isfile(failed_manifest)
            failed_status_payload = Backend._read_job_json(failed_status)
            @test failed_status_payload["status"] == "failed"
            @test failed_status_payload["result_available"] == false
            @test !isdir(failed_manifest_dir)
            @test Backend._pending_job_store_dir_generation(
                failed_result_dir,
            ) !== nothing
            @test Backend._retry_pending_job_store_fsync_with_ops(
                Backend._DEFAULT_JOB_PERSISTENCE_OPS,
            )
        end
    end
end

end # module JobsArtifactValidityContract
