module JobsCancellationContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function with_isolated_job_store(f::Function)
    previous_env = haskey(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE") ? ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] : nothing
    previous_store_dir = Backend.LOCAL_JOB_STORE_DIR[]

    mktempdir() do dir
        try
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            Backend.LOCAL_JOB_STORE_DIR[] = nothing
            lock(Backend.JOBS_LOCK) do
                empty!(Backend.JOBS)
                empty!(Backend.JOB_TASKS)
                empty!(Backend.JOB_DESCRIBE_LAST_AT)
            end
            f(dir)
        finally
            if previous_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_env
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous_store_dir
            lock(Backend.JOBS_LOCK) do
                empty!(Backend.JOBS)
                empty!(Backend.JOB_TASKS)
                empty!(Backend.JOB_DESCRIBE_LAST_AT)
            end
        end
    end
end

function seed_job(status::AbstractString;
                  executor::AbstractString="local_async",
                  batch_job_id=nothing)
    job_id = string(rand(UInt128), base=16, pad=32)
    now = Backend._now_iso_timestamp()
    record = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => "query_atlas",
        "status" => String(status),
        "executor" => String(executor),
        "user_sub" => Backend.ANONYMOUS_USER_SUB,
        "created_at" => now,
        "updated_at" => now,
        "result_available" => false,
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
    batch_job_id === nothing || (record["batch_job_id"] = String(batch_job_id))
    lock(Backend.JOBS_LOCK) do
        Backend.JOBS[job_id] = record
        Backend._persist_job_record_unlocked(record)
        Backend._persist_job_status_unlocked(record)
    end
    return job_id
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
  printf '{"jobId":"blocking-job-123","jobName":"blocking-job"}\n'
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
                all(job_id -> !haskey(Backend.JOB_TASKS, job_id), job_ids)
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
                        only(new_ids)
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
                    @test_throws TaskFailedException fetch(operation)
                    terminal = Backend._job_record(in_flight_id)
                    @test terminal["status"] == "cancelled"
                    @test !haskey(terminal, "batch_job_id")
                    @test occursin("failed process", lowercase(String(terminal["error"])))
                end

                blocked_env("describe-jobs") do started, released
                    job_id = seed_job("queued";
                        executor="aws_batch",
                        batch_job_id="describe-job-123",
                    )
                    operation = @async Backend._refresh_aws_batch_job!(job_id)
                    @test wait_for_file(started)
                    try
                        assert_job_lock_available()
                    finally
                        touch_file(released)
                    end
                    refreshed = fetch(operation)
                    @test refreshed["status"] == "running"
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
                    touch_file(Backend._job_result_path(job_id))
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
