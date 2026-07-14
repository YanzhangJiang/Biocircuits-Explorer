module JobsCancellationContract

using Test
using BiocircuitsExplorerBackend
using HTTP
using JSON3

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
        isempty(Backend.JOB_DESCRIBE_IN_FLIGHT) ||
            error("AWS describe claims leaked across test fixtures.")
        empty!(Backend.JOBS)
        empty!(Backend.JOB_CACHE_LAST_ACCESS)
        Backend.JOB_CACHE_ACCESS_CLOCK[] = UInt64(0)
        Backend.JOB_CACHE_CAPACITY[] = nothing
        empty!(Backend.JOB_TASKS)
        empty!(Backend.LOCAL_JOB_CANCEL_TOKENS)
        empty!(Backend.JOB_DESCRIBE_LAST_AT)
        empty!(Backend.JOB_DESCRIBE_IN_FLIGHT)
        empty!(Backend.AWS_BATCH_INITIAL_SUBMISSIONS)
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
    previous_env = haskey(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE") ? ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] : nothing
    previous_batch_region = get(
        ENV,
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION",
        nothing,
    )
    previous_store_dir = Backend.LOCAL_JOB_STORE_DIR[]

    mktempdir() do dir
        try
            drain_and_reset_job_runtime!()
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            ENV["BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION"] = "us-west-2"
            Backend.LOCAL_JOB_STORE_DIR[] = nothing
            f(dir)
        finally
            drain_and_reset_job_runtime!()
            if previous_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_env
            end
            if previous_batch_region === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION")
            else
                ENV["BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION"] =
                    previous_batch_region
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
        end
    end
end

function seed_job(status::AbstractString;
                  executor::AbstractString="local_async",
                  batch_job_id=nothing,
                  result_available::Bool=false,
                  job_id=nothing,
                  state_revision=1)
    job_id = job_id === nothing ?
        string(rand(UInt128), base=16, pad=32) : String(job_id)
    now = Backend._now_iso_timestamp()
    record = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => "query_atlas",
        "status" => String(status),
        "executor" => String(executor),
        "user_sub" => Backend.ANONYMOUS_USER_SUB,
        "created_at" => now,
        "updated_at" => now,
        "result_available" => result_available,
        "progress" => Dict("message" => "seeded"),
        "spec" => Dict{String, Any}(),
        "input_path" => Backend._job_input_path(job_id),
        "status_path" => Backend._job_status_path(job_id),
        "record_path" => Backend._job_record_path(job_id),
        "result_path" => Backend._job_result_path(job_id),
        "input_uri" => Backend._job_input_path(job_id),
        "status_uri" => Backend._job_status_path(job_id),
        "result_uri" => Backend._job_result_path(job_id),
    )
    state_revision === nothing ||
        (record["state_revision"] = state_revision)
    batch_job_id === nothing || (record["batch_job_id"] = String(batch_job_id))
    Backend._with_job_lock(job_id) do
        Backend._persist_job_record_unlocked(record)
        Backend._job_cache_publish!(job_id, record)
        Backend._persist_job_status_unlocked(record)
    end
    return job_id
end

function simulate_process_restart()
    lock(Backend.JOBS_LOCK) do
        empty!(Backend.JOBS)
        empty!(Backend.JOB_CACHE_LAST_ACCESS)
        Backend.JOB_CACHE_ACCESS_CLOCK[] = UInt64(0)
        Backend.JOB_CACHE_CAPACITY[] = nothing
        empty!(Backend.JOB_TASKS)
        empty!(Backend.LOCAL_JOB_CANCEL_TOKENS)
        empty!(Backend.LOCAL_JOB_ADMISSIONS)
        empty!(Backend.JOB_DESCRIBE_LAST_AT)
        empty!(Backend.JOB_DESCRIBE_IN_FLIGHT)
        empty!(Backend.AWS_BATCH_INITIAL_SUBMISSIONS)
        empty!(Backend.JOB_STATUS_PROJECTION_DIRTY)
        Backend.LOCAL_JOB_LIMITS[] = nothing
        Backend.LOCAL_JOB_RUN_SEMAPHORE[] = nothing
    end
    return nothing
end

function write_blocking_aws_cli(path::AbstractString)
    open(path, "w") do io
        write(io, raw"""#!/bin/sh
set -eu

if [ "$1" = "batch" ] && [ "$2" = "${AWS_BLOCK_ON:-never}" ]; then
  : > "${AWS_CALL_STARTED:?}"
  while [ ! -f "${AWS_CALL_RELEASE:?}" ]; do
    sleep 0.01
  done
fi

if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  exit 0
fi
if [ "$1" = "s3api" ] && [ "$2" = "head-object" ]; then
  exit 1
fi
if [ "$1" = "batch" ] && [ "$2" = "submit-job" ]; then
  if [ "${AWS_SUBMIT_FAIL:-0}" = "1" ]; then
    exit 19
  fi
  job_name=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --job-name) job_name="$2"; shift 2 ;;
      *)          shift ;;
    esac
  done
  printf '{"jobId":"blocking-job-123","jobName":"%s"}\n' "$job_name"
  exit 0
fi
if [ "$1" = "batch" ] && [ "$2" = "list-jobs" ]; then
  printf '{"jobSummaryList":[]}\n'
  exit 0
fi
if [ "$1" = "batch" ] && [ "$2" = "describe-jobs" ]; then
  printf '{"jobs":[{"status":"%s","container":{}}]}\n' "${AWS_DESCRIBE_STATUS:-SUBMITTED}"
  exit 0
fi
if [ "$1" = "batch" ] && { [ "$2" = "cancel-job" ] || [ "$2" = "terminate-job" ]; }; then
  if [ -n "${AWS_CANCEL_FAIL_ONCE_FILE:-}" ] && [ ! -f "$AWS_CANCEL_FAIL_ONCE_FILE" ]; then
    : > "$AWS_CANCEL_FAIL_ONCE_FILE"
    exit 17
  fi
  exit 0
fi

printf '{}\n'
""")
    end
    chmod(path, 0o755)
    return path
end

function touch_file(path::AbstractString)
    open(path, "w") do io
        write(io, "released\n")
    end
end

function write_valid_job_result(job_id::AbstractString)
    record = Backend._job_record(job_id)
    result = Dict{String, Any}("ok" => true)
    Backend.attach_artifact!(
        result,
        String(record["kind"]);
        input_hashes=Dict{String, Any}(
            "network_ir_hashes" => Backend.artifact_network_hashes(record["spec"]),
        ),
        config=record["spec"],
    )
    Backend._write_job_json(Backend._job_result_path(job_id), result)
    return result
end

function wait_for_file(path::AbstractString; timeout=5.0)
    return timedwait(() -> isfile(path), timeout; pollint=0.01) == :ok
end

function assert_job_lock_available()
    acquired = Channel{Bool}(1)
    waiter = @async begin
        lock(Backend.JOBS_LOCK) do
            put!(acquired, true)
        end
    end
    @test timedwait(() -> isready(acquired), 1.0; pollint=0.01) == :ok
    @test take!(acquired)
    wait(waiter)
end

@testset "Cooperative Job Cancellation Contract" begin
    source = read(joinpath(@__DIR__, "..", "src", "jobs.jl"), String)
    @test !occursin("Base.throwto", source)
    @test Backend.JOB_STATUSES == Set([
        "queued",
        "running",
        "succeeded",
        "failed",
        "cancel_requested",
        "cancelled",
    ])

    @testset "atomic job persistence and durability retry" begin
        @test !occursin(
            r"\bmv\s*\([^\n]*force\s*=\s*true",
            source,
        )
        @test occursin(
            "canonical_snapshot = _with_job_lock(job_id) do",
            source,
        )
        @test !occursin(
            "canonical_snapshot = lock(JOBS_LOCK) do",
            source,
        )
        @test !occursin(
            "_aws_batch_submit(_job_snapshot(record)",
            source,
        )

        with_isolated_job_store() do store_dir
            events = String[]
            ordered_ops = Backend._JobPersistenceOps(
                (io, path) -> begin
                    push!(events, "file_fsync")
                    Backend._fsync_job_file_posix!(io, path)
                end,
                (source_path, destination_path) -> begin
                    push!(events, "rename")
                    Backend._atomic_replace_job_file_posix!(
                        source_path,
                        destination_path,
                    )
                end,
                path -> begin
                    push!(events, "dir_fsync:" * abspath(path))
                    Backend._fsync_job_directory_posix!(path)
                end,
            )
            @test !ismutabletype(typeof(ordered_ops))

            created_job_dir = joinpath(store_dir, "created-job")
            created_path = joinpath(created_job_dir, "record.json")
            created = Backend._write_job_json_with_ops(
                created_path,
                Dict("revision" => 1),
                ordered_ops,
            )
            @test created.committed
            @test created.durable
            @test events == [
                "dir_fsync:" * abspath(store_dir),
                "file_fsync",
                "rename",
                "dir_fsync:" * abspath(created_job_dir),
            ]

            empty!(events)
            overwritten = Backend._write_job_json_with_ops(
                created_path,
                Dict("revision" => 2),
                ordered_ops,
            )
            @test overwritten.committed
            @test overwritten.durable
            @test events == [
                "file_fsync",
                "rename",
                "dir_fsync:" * abspath(created_job_dir),
            ]
            @test Backend._read_job_json(created_path)["revision"] == 2

            result_destination = joinpath(created_job_dir, "result.json")
            write(result_destination, "old result bytes\n")
            result_source = joinpath(created_job_dir, "result.pending")
            write(result_source, "new result bytes\n")
            empty!(events)
            published_result = Backend._upload_local_job_result_file_with_ops(
                result_destination,
                result_source,
                ordered_ops,
            )
            @test published_result.committed
            @test published_result.durable
            @test read(result_destination, String) == "new result bytes\n"
            @test !isfile(result_source)
            @test events == [
                "file_fsync",
                "rename",
                "dir_fsync:" * abspath(created_job_dir),
            ]

            # Local input publication retries only its own pending directory
            # and does not return a usable path until that retry succeeds.
            input_dir = joinpath(store_dir, "input-durability-retry")
            input_path = joinpath(input_dir, "input.json")
            input_dir_syncs = Ref(0)
            input_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                Backend._atomic_replace_job_file_posix!,
                path -> begin
                    if abspath(path) == abspath(input_dir)
                        input_dir_syncs[] += 1
                        input_dir_syncs[] == 1 &&
                            error("injected input directory fsync failure")
                    end
                    Backend._fsync_job_directory_posix!(path)
                end,
            )
            @test Backend._write_json_uri_with_ops(
                input_path,
                Dict("job_id" => "input-durability-retry"),
                input_ops,
            ) == input_path
            @test input_dir_syncs[] == 2
            @test Backend._read_job_json(input_path)["job_id"] ==
                  "input-durability-retry"
            @test Backend._pending_job_store_dir_generation(input_dir) === nothing

            failure_dir = joinpath(store_dir, "precommit-failures")
            mkpath(failure_dir)
            target = joinpath(failure_dir, "record.json")
            write(target, "old canonical bytes\n")
            old_bytes = read(target)

            file_fsync_failed = Ref(false)
            replace_called = Ref(false)
            file_failure_ops = Backend._JobPersistenceOps(
                (_io, _path) -> begin
                    file_fsync_failed[] = true
                    error("injected file fsync failure")
                end,
                (_source, _destination) -> begin
                    replace_called[] = true
                    error("replace must not run")
                end,
                _path -> error("directory fsync must not run"),
            )
            @test_throws ErrorException Backend._write_job_json_with_ops(
                target,
                Dict("revision" => 3),
                file_failure_ops,
            )
            @test file_fsync_failed[]
            @test !replace_called[]
            @test read(target) == old_bytes
            @test readdir(failure_dir) == ["record.json"]

            rename_failure_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                (_source, _destination) -> error("injected rename failure"),
                _path -> error("directory fsync must not run"),
            )
            @test_throws ErrorException Backend._write_job_json_with_ops(
                target,
                Dict("revision" => 4),
                rename_failure_ops,
            )
            @test read(target) == old_bytes
            @test readdir(failure_dir) == ["record.json"]

            postcommit_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                Backend._atomic_replace_job_file_posix!,
                _path -> error("injected directory fsync failure"),
            )
            postcommit = Backend._write_job_json_with_ops(
                target,
                Dict("revision" => 5),
                postcommit_ops,
            )
            @test postcommit.committed
            @test !postcommit.durable
            @test occursin("injected directory fsync failure", postcommit.durability_error)
            @test Backend._read_job_json(target)["revision"] == 5
            @test lock(Backend.JOB_STORE_DURABILITY_LOCK) do
                haskey(
                    Backend.JOB_STORE_PENDING_DIR_FSYNC,
                    abspath(failure_dir),
                )
            end
            @test !Backend._local_job_store_ready_with_ops(postcommit_ops)
            @test Backend._local_job_store_ready_with_ops(
                Backend._DEFAULT_JOB_PERSISTENCE_OPS,
            )
            @test lock(Backend.JOB_STORE_DURABILITY_LOCK) do
                isempty(Backend.JOB_STORE_PENDING_DIR_FSYNC)
            end

            transition_id = seed_job("queued"; job_id="postcommit-transition")
            transition = lock(Backend.JOBS_LOCK) do
                record = Backend.JOBS[transition_id]
                Backend._transition_job_record_unlocked_with_ops!(
                    record,
                    "running",
                    postcommit_ops;
                    expected=("queued",),
                    started_at=Backend._now_iso_timestamp(),
                )
            end
            @test transition.applied
            @test transition.persistence.committed
            @test !transition.persistence.durable
            @test Backend._job_record(transition_id)["status"] == "running"
            @test Backend._job_record(transition_id)["state_revision"] == 2
            transition_projection = Backend._read_job_json(
                Backend._job_status_path(transition_id),
            )
            @test transition_projection["status"] == "running"
            @test transition_projection["state_revision"] == 2
            @test !(transition_id in Backend.JOB_STATUS_PROJECTION_DIRTY)
            @test Backend._read_job_json(
                Backend._job_record_path(transition_id),
            )["status"] == "running"
            @test lock(Backend.JOB_STORE_DURABILITY_LOCK) do
                haskey(
                    Backend.JOB_STORE_PENDING_DIR_FSYNC,
                    abspath(Backend._job_dir(transition_id)),
                )
            end
            @test Backend.local_job_store_ready()
        end
    end

    @testset "canonical state revisions and status projection repair" begin
        with_isolated_job_store() do _
            revision_id = seed_job("queued"; job_id="revision-contract")
            @test Backend._job_record(revision_id)["state_revision"] == 1
            @test get_biocircuits_job(revision_id)["state_revision"] == 1
            @test Backend._read_job_json(
                Backend._job_status_path(revision_id),
            )["state_revision"] == 1

            started = Backend._job_transition!(
                revision_id,
                "running";
                expected=("queued",),
                progress=Dict("message" => "started"),
            )
            @test started.applied
            @test started.record["state_revision"] == 2

            same_status = Backend._job_transition!(
                revision_id,
                "running";
                expected=("running",),
                progress=Dict("message" => "same-second metadata update"),
            )
            @test same_status.applied
            @test same_status.record["state_revision"] == 3
            @test Backend._read_job_json(
                Backend._job_status_path(revision_id),
            )["state_revision"] == 3

            rejected_bytes = read(Backend._job_record_path(revision_id))
            rejected = Backend._job_transition!(
                revision_id,
                "succeeded";
                expected=("queued",),
            )
            @test !rejected.applied
            @test rejected.record["state_revision"] == 3
            @test read(Backend._job_record_path(revision_id)) == rejected_bytes
            @test_throws ArgumentError Backend._job_transition!(
                revision_id,
                "running";
                expected=("running",),
                state_revision=99,
            )
            @test Backend._job_record(revision_id)["state_revision"] == 3

            precommit_id = seed_job("queued"; job_id="revision-precommit")
            record_before = read(Backend._job_record_path(precommit_id))
            status_before = read(Backend._job_status_path(precommit_id))
            rename_failure_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                (_source, destination) -> endswith(String(destination), "record.json") ?
                    error("injected canonical rename failure") :
                    Backend._atomic_replace_job_file_posix!(_source, destination),
                Backend._fsync_job_directory_posix!,
            )
            @test_throws ErrorException lock(Backend.JOBS_LOCK) do
                Backend._transition_job_record_unlocked_with_ops!(
                    Backend.JOBS[precommit_id],
                    "running",
                    rename_failure_ops;
                    expected=("queued",),
                )
            end
            @test Backend._job_record(precommit_id)["state_revision"] == 1
            @test read(Backend._job_record_path(precommit_id)) == record_before
            @test read(Backend._job_status_path(precommit_id)) == status_before

            projection_failure_id = seed_job(
                "queued";
                job_id="projection-precommit",
            )
            projection_failure_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                (source_path, destination_path) -> begin
                    endswith(String(destination_path), "status.json") &&
                        error("injected projection rename failure")
                    Backend._atomic_replace_job_file_posix!(
                        source_path,
                        destination_path,
                    )
                end,
                Backend._fsync_job_directory_posix!,
            )
            projection_transition = lock(Backend.JOBS_LOCK) do
                Backend._transition_job_record_unlocked_with_ops!(
                    Backend.JOBS[projection_failure_id],
                    "running",
                    projection_failure_ops;
                    expected=("queued",),
                )
            end
            @test projection_transition.applied
            @test Backend._read_job_json(
                Backend._job_record_path(projection_failure_id),
            )["state_revision"] == 2
            @test Backend._read_job_json(
                Backend._job_status_path(projection_failure_id),
            )["state_revision"] == 1
            @test projection_failure_id in Backend.JOB_STATUS_PROJECTION_DIRTY

            repaired_public = get_biocircuits_job(projection_failure_id)
            @test repaired_public["state_revision"] == 2
            @test !(projection_failure_id in Backend.JOB_STATUS_PROJECTION_DIRTY)
            @test Backend._read_job_json(
                Backend._job_status_path(projection_failure_id),
            )["state_revision"] == 2
            @test Backend._job_record(projection_failure_id)["state_revision"] == 2
        end

        with_isolated_job_store() do _
            legacy_id = seed_job(
                "queued";
                executor="aws_batch",
                job_id="legacy-revision",
                state_revision=nothing,
            )
            legacy_record_bytes = read(Backend._job_record_path(legacy_id))
            legacy_projection = Backend._read_job_json(
                Backend._job_status_path(legacy_id),
            )
            delete!(legacy_projection, "state_revision")
            Backend._write_job_json(
                Backend._job_status_path(legacy_id),
                legacy_projection,
            )

            simulate_process_restart()
            loaded_legacy = Backend._job_record(legacy_id)
            @test !haskey(loaded_legacy, "state_revision")
            @test Backend._job_public_record(loaded_legacy)["state_revision"] == 0
            @test read(Backend._job_record_path(legacy_id)) == legacy_record_bytes
            @test Backend._read_job_json(
                Backend._job_status_path(legacy_id),
            )["state_revision"] == 0

            migrated = Backend._job_transition!(
                legacy_id,
                "running";
                expected=("queued",),
            )
            @test migrated.applied
            @test migrated.record["state_revision"] == 1
            @test Backend._read_job_json(
                Backend._job_status_path(legacy_id),
            )["state_revision"] == 1
        end

        with_isolated_job_store() do _
            function assert_cold_projection_repair(mutate_projection::Function,
                                                   job_id::AbstractString)
                seed_job("succeeded"; job_id=job_id, state_revision=5)
                mutate_projection(job_id)
                simulate_process_restart()
                public = get_biocircuits_job(job_id)
                @test public["status"] == "succeeded"
                @test public["state_revision"] == 5
                @test lock(Backend.JOBS_LOCK) do
                    Backend._job_status_projection_matches_unlocked(
                        Backend.JOBS[String(job_id)],
                    )
                end
            end

            assert_cold_projection_repair("projection-missing") do job_id
                rm(Backend._job_status_path(job_id); force=true)
            end
            assert_cold_projection_repair("projection-corrupt") do job_id
                open(Backend._job_status_path(job_id), "w") do io
                    write(io, "{not-json")
                end
            end
            assert_cold_projection_repair("projection-behind") do job_id
                projection = Backend._read_job_json(
                    Backend._job_status_path(job_id),
                )
                projection["state_revision"] = 4
                Backend._write_job_json(Backend._job_status_path(job_id), projection)
            end
            assert_cold_projection_repair("projection-ahead") do job_id
                projection = Backend._read_job_json(
                    Backend._job_status_path(job_id),
                )
                projection["state_revision"] = 6
                Backend._write_job_json(Backend._job_status_path(job_id), projection)
            end
            assert_cold_projection_repair("projection-noninteger-revision") do job_id
                projection = Backend._read_job_json(
                    Backend._job_status_path(job_id),
                )
                projection["state_revision"] = 5.5
                Backend._write_job_json(Backend._job_status_path(job_id), projection)
            end
            assert_cold_projection_repair("projection-same-revision-wrong") do job_id
                projection = Backend._read_job_json(
                    Backend._job_status_path(job_id),
                )
                projection["progress"] = Dict("message" => "stale content")
                Backend._write_job_json(Backend._job_status_path(job_id), projection)
            end
        end

        with_isolated_job_store() do _
            status_only_id = seed_job(
                "succeeded";
                job_id="status-only-must-not-be-authority",
                result_available=true,
            )
            rm(Backend._job_record_path(status_only_id); force=true)
            simulate_process_restart()
            @test_throws ArgumentError get_biocircuits_job(status_only_id)
            @test_throws ArgumentError get_biocircuits_job_result(status_only_id)

            corrupt_id = seed_job(
                "succeeded";
                job_id="corrupt-canonical-must-not-fallback",
            )
            open(Backend._job_record_path(corrupt_id), "w") do io
                write(io, "{not-json")
            end
            simulate_process_restart()
            @test_throws Exception get_biocircuits_job(corrupt_id)

            invalid_revision_id = seed_job(
                "succeeded";
                job_id="invalid-canonical-revision",
            )
            invalid_record = Backend._read_job_json(
                Backend._job_record_path(invalid_revision_id),
            )
            invalid_record["state_revision"] = "1"
            Backend._write_job_json(
                Backend._job_record_path(invalid_revision_id),
                invalid_record,
            )
            simulate_process_restart()
            @test_throws ArgumentError get_biocircuits_job(invalid_revision_id)

            float_revision_id = seed_job(
                "succeeded";
                job_id="float-canonical-revision",
            )
            float_record = Backend._read_job_json(
                Backend._job_record_path(float_revision_id),
            )
            float_record["state_revision"] = 1.5
            Backend._write_job_json(
                Backend._job_record_path(float_revision_id),
                float_record,
            )
            simulate_process_restart()
            @test_throws ArgumentError get_biocircuits_job(float_revision_id)
        end
    end

    @testset "local job capacity configuration is strict" begin
        withenv(
            "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => nothing,
            "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => nothing,
        ) do
            @test Backend.Config.local_job_max_concurrency() ==
                  min(max(Threads.nthreads(), 1), 2)
            @test Backend.Config.local_job_admission_limit() == 64
        end

        for value in ("0", "-1", "1.5", "invalid", "65")
            withenv("BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => value) do
                @test_throws ArgumentError Backend.Config.local_job_max_concurrency()
            end
        end
        for value in ("0", "-1", "1.5", "invalid", "4097")
            withenv("BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => value) do
                @test_throws ArgumentError Backend.Config.local_job_admission_limit()
            end
        end
        withenv(
            "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => "3",
            "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => "2",
        ) do
            @test_throws ArgumentError Backend._configured_local_job_limits()
        end
    end

    @testset "capacity rejects before quota and durable job creation" begin
        with_isolated_job_store() do store_dir
            withenv(
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => "1",
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => "1",
                "BIOCIRCUITS_EXPLORER_QUOTA_TABLE" => "quota-must-not-run",
                "BIOCIRCUITS_EXPLORER_AWS_CLI" => joinpath(store_dir, "missing-aws"),
            ) do
                held_id = "held-local-admission"
                Backend._reserve_local_job_admission!(held_id)
                request = Dict(
                    "kind" => "query_atlas",
                    "execution" => Dict("mode" => "local_async"),
                    "spec" => Dict(
                        "library" => atlas_library_default(),
                        "query" => Dict("limit" => 1),
                    ),
                )
                try
                    @test_throws Backend.LocalJobCapacityExceeded submit_biocircuits_job_from_spec(request)
                    @test isempty(readdir(store_dir))

                    response = Backend.router(HTTP.Request(
                        "POST",
                        "/api/jobs",
                        ["Content-Type" => "application/json"],
                        JSON3.write(request),
                    ))
                    body = Backend._materialize(JSON3.read(String(response.body)))
                    @test response.status == 429
                    @test body["code"] == "local_job_capacity_exhausted"
                    @test body["retryable"] == true
                    @test body["limit"] == 1
                    @test HTTP.header(response, "Retry-After") == "1"
                    @test isempty(readdir(store_dir))
                    @test lock(Backend.JOBS_LOCK) do
                        Backend.LOCAL_JOB_ADMISSIONS == Set([held_id])
                    end
                finally
                    Backend._release_local_job_admission!(held_id)
                end
            end
        end
    end

    @testset "bounded local semaphore preserves queued cancellation" begin
        with_isolated_job_store() do _
            withenv(
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => "1",
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => "2",
            ) do
                first_id = "admitted-first"
                second_id = "admitted-second"
                first_semaphore = Backend._reserve_local_job_admission!(first_id)
                second_semaphore = Backend._reserve_local_job_admission!(second_id)
                @test first_semaphore === second_semaphore
                seed_job("queued"; job_id=first_id)
                seed_job("queued"; job_id=second_id)

                first_gate = Channel{Nothing}(1)
                second_gate = Channel{Nothing}(1)
                first_release = Channel{Nothing}(1)
                started = Channel{String}(2)
                executed_ids = String[]

                controlled_run = function(job_id, _kind, _spec, _token)
                    transition = Backend._job_transition!(
                        job_id,
                        "running";
                        expected=("queued",),
                        started_at=Backend._now_iso_timestamp(),
                    )
                    transition.applied || return nothing
                    push!(executed_ids, job_id)
                    put!(started, job_id)
                    job_id == first_id && take!(first_release)
                    Backend._finish_local_job!(job_id; succeeded=true)
                    return nothing
                end

                first_token = Backend.LocalJobCancelToken(first_id)
                second_token = Backend.LocalJobCancelToken(second_id)
                first_task = Threads.@spawn Backend._run_admitted_local_job!(
                    first_id,
                    "query_atlas",
                    Dict{String, Any}(),
                    first_token,
                    first_semaphore;
                    start_gate=first_gate,
                    run_job! = controlled_run,
                )
                second_task = Threads.@spawn Backend._run_admitted_local_job!(
                    second_id,
                    "query_atlas",
                    Dict{String, Any}(),
                    second_token,
                    second_semaphore;
                    start_gate=second_gate,
                    run_job! = controlled_run,
                )
                lock(Backend.JOBS_LOCK) do
                    Backend.JOB_TASKS[first_id] = first_task
                    Backend.JOB_TASKS[second_id] = second_task
                    Backend.LOCAL_JOB_CANCEL_TOKENS[first_id] = first_token
                    Backend.LOCAL_JOB_CANCEL_TOKENS[second_id] = second_token
                end

                try
                    put!(first_gate, nothing)
                    @test timedwait(() -> isready(started), 2.0; pollint=0.01) == :ok
                    @test take!(started) == first_id
                    put!(second_gate, nothing)
                    sleep(0.05)
                    @test Backend._job_record(second_id)["status"] == "queued"

                    cancelled = cancel_biocircuits_job(second_id)
                    @test cancelled["status"] == "cancelled"
                    @test executed_ids == [first_id]

                    put!(first_release, nothing)
                    wait(first_task)
                    wait(second_task)
                    @test Backend._job_record(first_id)["status"] == "succeeded"
                    @test Backend._job_record(second_id)["status"] == "cancelled"
                    @test executed_ids == [first_id]
                    @test lock(Backend.JOBS_LOCK) do
                        isempty(Backend.LOCAL_JOB_ADMISSIONS) &&
                        !haskey(Backend.JOB_TASKS, first_id) &&
                        !haskey(Backend.JOB_TASKS, second_id) &&
                        !haskey(Backend.LOCAL_JOB_CANCEL_TOKENS, first_id) &&
                        !haskey(Backend.LOCAL_JOB_CANCEL_TOKENS, second_id)
                    end
                finally
                    isready(first_release) || put!(first_release, nothing)
                    isopen(first_gate) && close(first_gate)
                    isopen(second_gate) && close(second_gate)
                    for task in (first_task, second_task)
                        try
                            wait(task)
                        catch err
                            err isa TaskFailedException || rethrow()
                        end
                    end
                end
            end
        end
    end

    @testset "admitted task failures release every runtime resource" begin
        with_isolated_job_store() do _
            withenv(
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY" => "1",
                "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT" => "1",
            ) do
                job_id = "admitted-failure"
                semaphore = Backend._reserve_local_job_admission!(job_id)
                seed_job("queued"; job_id=job_id)
                gate = Channel{Nothing}(1)
                token = Backend.LocalJobCancelToken(job_id)
                task = Threads.@spawn Backend._run_admitted_local_job!(
                    job_id,
                    "query_atlas",
                    Dict{String, Any}(),
                    token,
                    semaphore;
                    start_gate=gate,
                    run_job! = (_args...) -> error("injected runner failure"),
                )
                lock(Backend.JOBS_LOCK) do
                    Backend.JOB_TASKS[job_id] = task
                    Backend.LOCAL_JOB_CANCEL_TOKENS[job_id] = token
                end
                put!(gate, nothing)
                @test_throws TaskFailedException fetch(task)
                @test lock(Backend.JOBS_LOCK) do
                    isempty(Backend.LOCAL_JOB_ADMISSIONS) &&
                    !haskey(Backend.JOB_TASKS, job_id) &&
                    !haskey(Backend.LOCAL_JOB_CANCEL_TOKENS, job_id)
                end

                probe_id = "admitted-after-failure"
                probe_semaphore = Backend._reserve_local_job_admission!(probe_id)
                probe = @async begin
                    Base.acquire(probe_semaphore)
                    Base.release(probe_semaphore)
                end
                @test timedwait(() -> istaskdone(probe), 1.0; pollint=0.01) == :ok
                wait(probe)
                Backend._release_local_job_admission!(probe_id)
            end
        end
    end

    @testset "describe cache is monotonic, bounded, and terminal-cleaned" begin
        with_isolated_job_store() do store_dir
            lock(Backend.JOBS_LOCK) do
                Backend._remember_job_describe_unlocked!(
                    "oldest", 10.0; ttl_seconds=100.0, max_entries=2)
                Backend._remember_job_describe_unlocked!(
                    "middle", 11.0; ttl_seconds=100.0, max_entries=2)
                Backend._remember_job_describe_unlocked!(
                    "newest", 12.0; ttl_seconds=100.0, max_entries=2)
                @test Set(keys(Backend.JOB_DESCRIBE_LAST_AT)) ==
                      Set(["middle", "newest"])

                Backend.JOB_DESCRIBE_LAST_AT["expired"] = 1.0
                Backend.JOB_DESCRIBE_LAST_AT["future"] = 1000.0
                Backend._prune_job_describe_cache_unlocked!(
                    200.0; ttl_seconds=100.0, max_entries=2)
                @test isempty(Backend.JOB_DESCRIBE_LAST_AT)
            end

            terminal_id = seed_job(
                "succeeded";
                executor="aws_batch",
                batch_job_id="terminal-describe",
            )
            invalid_id = seed_job("queued"; executor="aws_batch")
            local_id = seed_job("queued"; executor="local_async")
            lock(Backend.JOBS_LOCK) do
                Backend.JOB_DESCRIBE_LAST_AT[terminal_id] = 1.0
                Backend.JOB_DESCRIBE_LAST_AT[invalid_id] = 1.0
                Backend.JOB_DESCRIBE_LAST_AT[local_id] = 1.0
            end
            Backend._refresh_aws_batch_job!(terminal_id; now_seconds=2.0)
            Backend._refresh_aws_batch_job!(invalid_id; now_seconds=2.0)
            Backend._refresh_aws_batch_job!(local_id; now_seconds=2.0)
            @test lock(Backend.JOBS_LOCK) do
                all(id -> !haskey(Backend.JOB_DESCRIBE_LAST_AT, id),
                    (terminal_id, invalid_id, local_id))
            end

            aws_cli = write_blocking_aws_cli(joinpath(store_dir, "aws-describe"))
            withenv(
                "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL" => "3",
                "AWS_BLOCK_ON" => "never",
                "AWS_DESCRIBE_STATUS" => "SUBMITTED",
            ) do
                refresh_id = seed_job(
                    "queued";
                    executor="aws_batch",
                    batch_job_id="monotonic-describe",
                )
                Backend._refresh_aws_batch_job!(refresh_id; now_seconds=100.0)
                @test Backend.JOB_DESCRIBE_LAST_AT[refresh_id] == 100.0
                Backend._refresh_aws_batch_job!(refresh_id; now_seconds=101.0)
                @test Backend.JOB_DESCRIBE_LAST_AT[refresh_id] == 100.0
                Backend._refresh_aws_batch_job!(refresh_id; now_seconds=103.0)
                @test Backend.JOB_DESCRIBE_LAST_AT[refresh_id] == 103.0

                Backend._job_transition!(refresh_id, "failed"; expected=("queued",))
                Backend._refresh_aws_batch_job!(refresh_id; now_seconds=104.0)
                @test !haskey(Backend.JOB_DESCRIBE_LAST_AT, refresh_id)
            end
        end
    end

    @testset "disk-loaded local jobs settle after process restart" begin
        with_isolated_job_store() do _
            interrupted_ids = Dict(
                status => seed_job(status; result_available=true)
                for status in ("queued", "running")
            )
            cancel_requested_id = seed_job("cancel_requested"; result_available=true)

            # Existing in-process records are not recovery candidates, even if
            # this test fixture has not registered a real worker for them.
            @test Backend._job_record(interrupted_ids["running"])["status"] == "running"

            simulate_process_restart()

            for status in ("queued", "running")
                job_id = interrupted_ids[status]
                recovered = get_biocircuits_job(job_id)
                @test recovered["status"] == "failed"
                @test recovered["result_available"] == false
                @test recovered["error_code"] == Backend.LOCAL_JOB_RESTART_ERROR_CODE
                @test recovered["error"] == Backend.LOCAL_JOB_RESTART_ERROR_MESSAGE
                @test haskey(recovered, "finished_at")
                @test !haskey(recovered, "result_ref")

                canonical = Backend._read_job_json(Backend._job_record_path(job_id))
                projection = Backend._read_job_json(Backend._job_status_path(job_id))
                @test canonical["status"] == "failed"
                @test canonical["result_available"] == false
                @test projection["status"] == "failed"
                @test projection["result_available"] == false
                @test projection["error_code"] == Backend.LOCAL_JOB_RESTART_ERROR_CODE
                @test projection["error"] == Backend.LOCAL_JOB_RESTART_ERROR_MESSAGE

                # Reloading the recovered canonical record is an idempotent
                # read: terminal timestamps and bytes do not change again.
                canonical_before = read(Backend._job_record_path(job_id), String)
                projection_before = read(Backend._job_status_path(job_id), String)
                simulate_process_restart()
                reloaded = get_biocircuits_job(job_id)
                @test reloaded["status"] == "failed"
                @test read(Backend._job_record_path(job_id), String) == canonical_before
                @test read(Backend._job_status_path(job_id), String) == projection_before
            end

            cancelled = get_biocircuits_job(cancel_requested_id)
            @test cancelled["status"] == "cancelled"
            @test cancelled["result_available"] == false
            @test haskey(cancelled, "finished_at")
            @test !haskey(cancelled, "result_ref")
            @test Backend._read_job_json(Backend._job_record_path(cancel_requested_id))["status"] == "cancelled"
            @test Backend._read_job_json(Backend._job_status_path(cancel_requested_id))["status"] == "cancelled"

            @test_throws ArgumentError get_biocircuits_job_result(interrupted_ids["running"])
            @test_throws ArgumentError get_biocircuits_job_result(cancel_requested_id)
        end
    end

    @testset "cold load does not settle a registered local worker" begin
        with_isolated_job_store() do _
            job_id = seed_job("running")
            gate = Channel{Nothing}(1)
            worker = @async take!(gate)
            token = Backend.LocalJobCancelToken(job_id)
            try
                lock(Backend.JOBS_LOCK) do
                    delete!(Backend.JOBS, job_id)
                    Backend.JOB_TASKS[job_id] = worker
                    Backend.LOCAL_JOB_CANCEL_TOKENS[job_id] = token
                end

                loaded = Backend._job_record(job_id)
                @test loaded["status"] == "running"
                @test Backend._read_job_json(Backend._job_record_path(job_id))["status"] == "running"
                @test Backend._read_job_json(Backend._job_status_path(job_id))["status"] == "running"
            finally
                put!(gate, nothing)
                wait(worker)
                lock(Backend.JOBS_LOCK) do
                    delete!(Backend.JOB_TASKS, job_id)
                    delete!(Backend.LOCAL_JOB_CANCEL_TOKENS, job_id)
                end
            end
        end
    end

    @testset "disk-loaded AWS jobs remain refreshable" begin
        with_isolated_job_store() do store_dir
            aws_cli = write_blocking_aws_cli(joinpath(store_dir, "aws"))
            withenv(
                "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL" => "0",
                "AWS_BLOCK_ON" => "never",
                "AWS_DESCRIBE_STATUS" => "RUNNING",
            ) do
                for status in ("queued", "running", "cancel_requested")
                    job_id = seed_job(
                        status;
                        executor="aws_batch",
                        batch_job_id="restart-$(status)",
                    )
                    record_before = read(Backend._job_record_path(job_id), String)
                    status_before = read(Backend._job_status_path(job_id), String)
                    simulate_process_restart()

                    loaded = Backend._job_record(job_id)
                    @test loaded["status"] == status
                    @test loaded["executor"] == "aws_batch"
                    @test read(Backend._job_record_path(job_id), String) == record_before
                    @test read(Backend._job_status_path(job_id), String) == status_before

                    refreshed = Backend._refresh_aws_batch_job!(job_id)
                    expected = status == "queued" ? "running" : status
                    @test refreshed["status"] == expected
                end
            end
        end
    end

    @testset "queued cancellation is terminal and cannot restart" begin
        with_isolated_job_store() do _
            job_id = seed_job("queued")
            cancelled = cancel_biocircuits_job(job_id)
            @test cancelled["status"] == "cancelled"
            @test cancelled["result_available"] == false

            late_start = Backend._job_transition!(job_id, "running"; expected=("queued",))
            @test !late_start.applied
            @test late_start.record["status"] == "cancelled"
            @test Backend._read_job_json(Backend._job_status_path(job_id))["status"] == "cancelled"
        end
    end

    @testset "finish and cancellation have one linearization order" begin
        with_isolated_job_store() do _
            # Cancellation wins: a late successful computation is discarded.
            cancel_first = seed_job("running")
            requested = cancel_biocircuits_job(cancel_first)
            @test requested["status"] == "cancel_requested"
            settled = Backend._finish_local_job!(cancel_first; succeeded=true)
            @test settled.applied
            @test settled.record["status"] == "cancelled"
            @test settled.record["result_available"] == false

            # Completion wins: cancelling a terminal result is an idempotent
            # read and cannot erase the published result.
            finish_first = seed_job("running")
            finished = Backend._finish_local_job!(finish_first; succeeded=true)
            @test finished.record["status"] == "succeeded"
            after_cancel = cancel_biocircuits_job(finish_first)
            @test after_cancel["status"] == "succeeded"
            @test after_cancel["result_available"] == true

            # Public snapshots must not share nested mutable values with the
            # canonical in-memory/disk record.
            public_snapshot = get_biocircuits_job(finish_first)
            public_snapshot["progress"]["message"] = "caller-mutated"
            @test Backend._job_record(finish_first)["progress"]["message"] == "Completed"
            @test Backend._read_job_json(Backend._job_record_path(finish_first))["progress"]["message"] == "Completed"
        end
    end

    @testset "terminal snapshots reject same-status mutation" begin
        with_isolated_job_store() do _
            job_id = seed_job("succeeded")
            before_record = deepcopy(Backend._job_record(job_id))
            before_disk = read(Backend._job_record_path(job_id), String)

            rejected = Backend._job_transition!(
                job_id,
                "succeeded";
                result_available=false,
                progress=Dict("message" => "late overwrite"),
            )

            @test !rejected.applied
            @test Backend._job_record(job_id) == before_record
            @test read(Backend._job_record_path(job_id), String) == before_disk
            rejected.record["progress"]["message"] = "mutated rejected snapshot"
            @test Backend._job_record(job_id) == before_record
            @test read(Backend._job_record_path(job_id), String) == before_disk
        end
    end

    @testset "running local cancellation signals the compute token" begin
        with_isolated_job_store() do _
            job_id = seed_job("running")
            token = Backend.LocalJobCancelToken(job_id)
            lock(Backend.JOBS_LOCK) do
                Backend.LOCAL_JOB_CANCEL_TOKENS[job_id] = token
            end

            requested = cancel_biocircuits_job(job_id)
            @test requested["status"] == "cancel_requested"
            @test_throws Backend.LocalJobCancelled Backend._check_cancelled(token)
        end
    end

    @testset "concurrent queued/start and running/finish races stay monotonic" begin
        with_isolated_job_store() do _
            for _ in 1:30
                job_id = seed_job("queued")
                gate = Channel{Nothing}(2)
                cancel_task = @async begin
                    take!(gate)
                    cancel_biocircuits_job(job_id)
                end
                start_task = @async begin
                    take!(gate)
                    Backend._job_transition!(job_id, "running"; expected=("queued",))
                end
                put!(gate, nothing)
                put!(gate, nothing)
                fetch(cancel_task)
                started = fetch(start_task)
                if started.applied
                    Backend._finish_local_job!(job_id; succeeded=true)
                end
                final = Backend._job_record(job_id)
                @test final["status"] == "cancelled"
                @test final["result_available"] == false
            end

            for _ in 1:30
                job_id = seed_job("running")
                gate = Channel{Nothing}(2)
                cancel_task = @async begin
                    take!(gate)
                    cancel_biocircuits_job(job_id)
                end
                finish_task = @async begin
                    take!(gate)
                    Backend._finish_local_job!(job_id; succeeded=true)
                end
                put!(gate, nothing)
                put!(gate, nothing)
                fetch(cancel_task)
                fetch(finish_task)
                final = Backend._job_record(job_id)
                @test final["status"] in ("succeeded", "cancelled")
                @test final["result_available"] == (final["status"] == "succeeded")
                rejected = Backend._job_transition!(job_id, "running")
                @test !rejected.applied
                @test rejected.record["status"] == final["status"]
            end
        end
    end

    @testset "fast local workers cannot leak JOB_TASKS registrations" begin
        with_isolated_job_store() do store_dir
            job_ids = String[]
            for _ in 1:12
                submitted = submit_biocircuits_job_from_spec(Dict(
                    "kind" => "query_atlas",
                    "execution" => Dict("mode" => "local_async"),
                    "spec" => Dict(
                        "library" => atlas_library_default(),
                        "query" => Dict("limit" => 1),
                    ),
                ))
                push!(job_ids, String(submitted["job_id"]))
            end
            @test timedwait(() -> all(job_ids) do job_id
                record = Backend._job_record(job_id)
                record !== nothing && String(record["status"]) in Backend.JOB_TERMINAL_STATUSES
            end, 30.0; pollint=0.02) == :ok
            @test timedwait(() -> lock(Backend.JOBS_LOCK) do
                all(job_id -> !haskey(Backend.JOB_TASKS, job_id) &&
                              !haskey(Backend.LOCAL_JOB_CANCEL_TOKENS, job_id) &&
                              !(job_id in Backend.LOCAL_JOB_ADMISSIONS), job_ids)
            end, 2.0; pollint=0.01) == :ok

            # Even a failure while publishing the initial queued→running
            # transition must execute the outer cleanup finally block.
            failing_id = seed_job("queued")
            invalid_record_path = joinpath(store_dir, "record-path-is-a-directory")
            mkpath(invalid_record_path)
            lock(Backend.JOBS_LOCK) do
                Backend.JOBS[failing_id]["record_path"] = invalid_record_path
            end
            start_gate = Channel{Nothing}(1)
            failing_task = @async begin
                take!(start_gate)
                Backend._run_local_job!(failing_id, "query_atlas", Dict{String, Any}())
            end
            lock(Backend.JOBS_LOCK) do
                Backend.JOB_TASKS[failing_id] = failing_task
            end
            put!(start_gate, nothing)
            @test_throws TaskFailedException fetch(failing_task)
            @test lock(Backend.JOBS_LOCK) do
                !haskey(Backend.JOB_TASKS, failing_id)
            end
            # The canonical record is committed before the shared in-memory
            # snapshot. A failed atomic replace cannot leave a ghost-running
            # job in memory or corrupt the previous public projection.
            @test Backend._job_record(failing_id)["status"] == "queued"
            @test Backend._read_job_json(Backend._job_status_path(failing_id))["status"] == "queued"
        end
    end

    @testset "AWS external calls never hold JOBS_LOCK" begin
        with_isolated_job_store() do _
            mktempdir() do dir
                aws_cli = write_blocking_aws_cli(joinpath(dir, "aws"))

                function blocked_env(f::Function, block_on)
                    started = joinpath(dir, "$(block_on)-started")
                    released = joinpath(dir, "$(block_on)-released")
                    rm(started; force=true)
                    rm(released; force=true)
                    return withenv(
                        "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE" => "queue",
                        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION" => "definition",
                        "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX" => "s3://bucket/jobs",
                        "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL" => "0",
                        "AWS_BLOCK_ON" => block_on,
                        "AWS_CALL_STARTED" => started,
                        "AWS_CALL_RELEASE" => released,
                        "AWS_DESCRIBE_STATUS" => "RUNNING",
                    ) do
                        f(started, released)
                    end
                end

                blocked_env("submit-job") do started, released
                    operation = @async submit_biocircuits_job_from_spec(Dict(
                        "kind" => "query_atlas",
                        "execution" => Dict("mode" => "aws_batch"),
                        "spec" => Dict{String, Any}(),
                    ))
                    @test wait_for_file(started)
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    submitted = fetch(operation)
                    @test submitted["external_job_id"] == "blocking-job-123"
                    @test submitted["status"] == "running"
                end

                blocked_env("submit-job") do started, released
                    before_ids = lock(Backend.JOBS_LOCK) do
                        Set(keys(Backend.JOBS))
                    end
                    operation = @async submit_biocircuits_job_from_spec(Dict(
                        "kind" => "query_atlas",
                        "execution" => Dict("mode" => "aws_batch"),
                        "spec" => Dict{String, Any}(),
                    ))
                    @test wait_for_file(started)
                    in_flight_id = lock(Backend.JOBS_LOCK) do
                        new_ids = setdiff(Set(keys(Backend.JOBS)), before_ids)
                        @test length(new_ids) == 1
                        job_id = only(new_ids)
                        @test Backend.JOBS[job_id]["state_revision"] == 2
                        @test Backend.JOBS[job_id]["submission_state"] ==
                              "dispatch_started"
                        @test Backend._read_job_json(
                            Backend._job_record_path(job_id),
                        )["state_revision"] == 2
                        @test Backend._read_job_json(
                            Backend._job_status_path(job_id),
                        )["state_revision"] == 2
                        job_id
                    end
                    requested = cancel_biocircuits_job(in_flight_id)
                    @test requested["status"] == "cancel_requested"
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    cancelled_submission = fetch(operation)
                    @test cancelled_submission["status"] == "cancel_requested"
                    ENV["AWS_DESCRIBE_STATUS"] = "FAILED"
                    @test get_biocircuits_job(in_flight_id)["status"] == "cancelled"
                end

                blocked_env("submit-job") do started, released
                    before_ids = lock(Backend.JOBS_LOCK) do
                        Set(keys(Backend.JOBS))
                    end
                    operation = @async withenv("AWS_SUBMIT_FAIL" => "1") do
                        submit_biocircuits_job_from_spec(Dict(
                            "kind" => "query_atlas",
                            "execution" => Dict("mode" => "aws_batch"),
                            "spec" => Dict{String, Any}(),
                        ))
                    end
                    @test wait_for_file(started)
                    in_flight_id = lock(Backend.JOBS_LOCK) do
                        new_ids = setdiff(Set(keys(Backend.JOBS)), before_ids)
                        @test length(new_ids) == 1
                        only(new_ids)
                    end
                    try
                        @test cancel_biocircuits_job(in_flight_id)["status"] == "cancel_requested"
                    finally
                        touch_file(released)
                    end
                    ambiguous = fetch(operation)
                    @test ambiguous["status"] == "cancel_requested"
                    @test ambiguous["submission_state"] == "reconciling"
                    record = Backend._job_record(in_flight_id)
                    @test record["status"] == "cancel_requested"
                    @test record["submission_state"] == "reconciling"
                    @test !haskey(record, "batch_job_id")
                    @test occursin(
                        "failed process",
                        lowercase(String(record["submission_last_error"])),
                    )
                end

                blocked_env("describe-jobs") do started, released
                    job_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="describe-job-123",
                    )
                    operation = @async Backend._refresh_aws_batch_job!(job_id)
                    @test wait_for_file(started)
                    @test lock(Backend.JOBS_LOCK) do
                        job_id in Backend.JOB_DESCRIBE_IN_FLIGHT
                    end
                    duplicate = @async Backend._refresh_aws_batch_job!(
                        job_id;
                        now_seconds=Backend._monotonic_seconds() + 1000,
                    )
                    @test timedwait(() -> istaskdone(duplicate), 1.0; pollint=0.01) == :ok
                    @test fetch(duplicate)["status"] == "queued"
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    refreshed = fetch(operation)
                    @test refreshed["status"] == "running"
                    @test lock(Backend.JOBS_LOCK) do
                        !(job_id in Backend.JOB_DESCRIBE_IN_FLIGHT)
                    end
                end

                blocked_env("describe-jobs") do started, released
                    job_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="stale-describe-job-123",
                    )
                    stale_refresh = @async Backend._refresh_aws_batch_job!(job_id)
                    @test wait_for_file(started)
                    cancellation = @async cancel_biocircuits_job(job_id)
                    try
                        yield()
                    finally
                        touch_file(released)
                    end
                    @test fetch(cancellation)["status"] == "cancel_requested"
                    @test fetch(stale_refresh)["status"] == "cancel_requested"
                    @test Backend._job_record(job_id)["status"] == "cancel_requested"
                    ENV["AWS_DESCRIBE_STATUS"] = "FAILED"
                    @test get_biocircuits_job(job_id)["status"] == "cancelled"
                end

                blocked_env("cancel-job") do started, released
                    job_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="cancel-job-123",
                    )
                    operation = @async cancel_biocircuits_job(job_id)
                    @test wait_for_file(started)
                    @test Backend._job_record(job_id)["status"] == "cancel_requested"
                    duplicate = @async cancel_biocircuits_job(job_id)
                    @test timedwait(() -> istaskdone(duplicate), 1.0; pollint=0.01) == :ok
                    @test fetch(duplicate)["status"] == "cancel_requested"
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    cancelled = fetch(operation)
                    @test cancelled["status"] == "cancel_requested"
                end

                blocked_env("terminate-job") do started, released
                    job_id = seed_job("running";
                        executor="aws_batch",
                        batch_job_id="terminate-job-123",
                    )
                    operation = @async cancel_biocircuits_job(job_id)
                    @test wait_for_file(started)
                    @test Backend._job_record(job_id)["status"] == "cancel_requested"
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    requested = fetch(operation)
                    @test requested["status"] == "cancel_requested"
                end

                blocked_env("terminate-job") do started, released
                    job_id = seed_job("running";
                        executor="aws_batch",
                        batch_job_id="finish-wins-job-123",
                    )
                    # A successful remote completion needs an artifact before
                    # refresh is allowed to publish `succeeded`.
                    write_valid_job_result(job_id)
                    cancellation = @async cancel_biocircuits_job(job_id)
                    @test wait_for_file(started)
                    ENV["AWS_DESCRIBE_STATUS"] = "SUCCEEDED"
                    refreshed = Backend._refresh_aws_batch_job!(job_id)
                    @test refreshed["status"] == "succeeded"
                    terminal_progress = deepcopy(refreshed["progress"])
                    touch_file(released)
                    @test fetch(cancellation)["status"] == "succeeded"
                    final = Backend._job_record(job_id)
                    @test final["status"] == "succeeded"
                    @test final["progress"] == terminal_progress
                    @test !haskey(final, "cancel_dispatched_at")
                end

                # The common queued case remains API-compatible: once AWS
                # still reports a cancellable queue state, the response is
                # immediately terminal `cancelled`.
                withenv(
                    "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                    "AWS_BLOCK_ON" => "never",
                    "AWS_DESCRIBE_STATUS" => "SUBMITTED",
                ) do
                    queued_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="ordinary-cancel-job-123",
                    )
                    @test cancel_biocircuits_job(queued_id)["status"] == "cancelled"
                end

                # A failed CLI dispatch remains cancel_requested but has no
                # success marker, so a second API call retries and can settle.
                fail_once = joinpath(dir, "cancel-failed-once")
                withenv(
                    "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                    "AWS_BLOCK_ON" => "never",
                    "AWS_DESCRIBE_STATUS" => "SUBMITTED",
                    "AWS_CANCEL_FAIL_ONCE_FILE" => fail_once,
                ) do
                    retry_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="retry-cancel-job-123",
                    )
                    @test_throws ArgumentError cancel_biocircuits_job(retry_id)
                    failed_request = Backend._job_record(retry_id)
                    @test failed_request["status"] == "cancel_requested"
                    @test !haskey(failed_request, "cancel_dispatched_at")
                    @test cancel_biocircuits_job(retry_id)["status"] == "cancelled"
                end
            end
        end
    end
end

end # module JobsCancellationContract
