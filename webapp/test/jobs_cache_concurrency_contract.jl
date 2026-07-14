module JobsCacheConcurrencyContract

using Test
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function reset_runtime!()
    tasks = lock(Backend.JOBS_LOCK) do
        collect(values(Backend.JOB_TASKS))
    end
    isempty(tasks) || begin
        timedwait(() -> all(istaskdone, tasks), 30.0; pollint=0.01) == :ok ||
            error("Timed out draining job-cache contract tasks.")
        foreach(wait, tasks)
    end
    lock(Backend.JOBS_LOCK) do
        isempty(Backend.LOCAL_JOB_ADMISSIONS) || error(
            "Local job admissions leaked into the cache contract fixture.")
        isempty(Backend.JOB_DESCRIBE_IN_FLIGHT) || error(
            "AWS describe claims leaked into the cache contract fixture.")
        isempty(Backend.AWS_BATCH_INITIAL_SUBMISSIONS) || error(
            "AWS submission owners leaked into the cache contract fixture.")
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
    lock(Backend.JOB_STORE_DURABILITY_LOCK) do
        empty!(Backend.JOB_STORE_PENDING_DIR_FSYNC)
        Backend.JOB_STORE_DURABILITY_GENERATION[] = UInt64(0)
    end
    return nothing
end

function with_store(f::Function; capacity::Integer=8)
    previous_store_env = get(
        ENV,
        "BIOCIRCUITS_EXPLORER_JOB_STORE",
        nothing,
    )
    previous_capacity_env = get(
        ENV,
        "BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY",
        nothing,
    )
    previous_store = Backend.LOCAL_JOB_STORE_DIR[]
    previous_batch_region = get(
        ENV,
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION",
        nothing,
    )
    mktempdir() do dir
        try
            reset_runtime!()
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            ENV["BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"] = string(capacity)
            ENV["BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION"] = "us-west-2"
            Backend.LOCAL_JOB_STORE_DIR[] = nothing
            f(dir)
        finally
            reset_runtime!()
            if previous_store_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_store_env
            end
            if previous_capacity_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"] =
                    previous_capacity_env
            end
            if previous_batch_region === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION")
            else
                ENV["BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION"] =
                    previous_batch_region
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous_store
        end
    end
end

function seed_job(job_id::AbstractString;
                  status::AbstractString="succeeded",
                  executor::AbstractString="local_async",
                  state_revision::Integer=1,
                  batch_job_id=nothing)
    id = String(job_id)
    now = Backend._now_iso_timestamp()
    record = Dict{String, Any}(
        "job_id" => id,
        "kind" => "query_atlas",
        "status" => String(status),
        "executor" => String(executor),
        "user_sub" => Backend.ANONYMOUS_USER_SUB,
        "created_at" => now,
        "updated_at" => now,
        "state_revision" => Int(state_revision),
        "result_available" => false,
        "progress" => Dict("message" => "seeded"),
        "spec" => Dict{String, Any}(),
        "input_path" => Backend._job_input_path(id),
        "status_path" => Backend._job_status_path(id),
        "record_path" => Backend._job_record_path(id),
        "result_path" => Backend._job_result_path(id),
        "input_uri" => Backend._job_input_path(id),
        "status_uri" => Backend._job_status_path(id),
        "result_uri" => Backend._job_result_path(id),
    )
    batch_job_id === nothing ||
        (record["batch_job_id"] = String(batch_job_id))
    Backend._with_job_lock(id) do
        Backend._activate_job_cache_capacity!()
        Backend._persist_job_record_unlocked(record)
        Backend._job_cache_publish!(id, record)
        Backend._persist_job_status_projection_unlocked(record)
    end
    return id
end

function cache_snapshot()
    return lock(Backend.JOBS_LOCK) do
        (
            ids=Set(keys(Backend.JOBS)),
            access_ids=Set(keys(Backend.JOB_CACHE_LAST_ACCESS)),
            capacity=Backend.JOB_CACHE_CAPACITY[],
        )
    end
end

function touch_file(path::AbstractString)
    open(path, "w") do io
        write(io, "released\n")
    end
    return path
end

function write_cache_aws_cli(path::AbstractString)
    open(path, "w") do io
        write(io, raw"""#!/bin/sh
set -eu

if [ -n "${AWS_CACHE_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$AWS_CACHE_LOG"
fi

block_if_requested() {
  label="$1"
  if [ "${AWS_CACHE_BLOCK_ON:-never}" = "$label" ]; then
    : > "${AWS_CACHE_BLOCK_STARTED:?}"
    while [ ! -f "${AWS_CACHE_BLOCK_RELEASE:?}" ]; do
      sleep 0.01
    done
  fi
}

if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  block_if_requested s3-cp
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "submit-job" ]; then
  job_name=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --job-name) job_name="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  printf '{"jobId":"cache-submit-123","jobName":"%s"}\n' "$job_name"
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "describe-jobs" ]; then
  block_if_requested describe-jobs
  printf '{"jobs":[{"jobId":"cache-job","status":"%s","container":{}}]}\n' "${AWS_CACHE_DESCRIBE_STATUS:-SUBMITTED}"
  exit 0
fi

if [ "$1" = "batch" ] && { [ "$2" = "cancel-job" ] || [ "$2" = "terminate-job" ]; }; then
  block_if_requested "$2"
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "list-jobs" ]; then
  printf '{"jobSummaryList":[]}\n'
  exit 0
fi

printf '{}\n'
""")
    end
    chmod(path, 0o755)
    return path
end

@testset "Striped job state and bounded cache contract" begin
    source = read(joinpath(@__DIR__, "..", "src", "jobs.jl"), String)
    cache_get_source = match(
        r"(?s)function _job_cache_get\(.*?\nend\n\nfunction _job_cache_publish!",
        source,
    )
    @test cache_get_source !== nothing
    @test !occursin("_prune_job_cache_unlocked!", cache_get_source.match)
    @test Backend.JOB_LOCK_STRIPE_COUNT == 128
    @test length(Backend.JOB_LOCK_STRIPES) == 128
    @test Backend._job_lock_stripe_index("abc") == 76
    @test Backend._job_lock_stripe_index("job-a") == 7
    @test Backend.Config.JOB_CACHE_CAPACITY_HARD_LIMIT == 65_536
    withenv("BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY" => nothing) do
        @test Backend.Config.job_cache_capacity() == 1024
    end
    for invalid in ("0", "-1", "1.5", "not-an-int", "65537")
        withenv("BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY" => invalid) do
            @test_throws ArgumentError Backend.Config.job_cache_capacity()
        end
    end

    @testset "blocked job I/O does not stop registry or another stripe" begin
        with_store(capacity=8) do _
            first_id = seed_job("job-a"; status="queued")
            second_id = seed_job("job-b"; status="queued")
            @test Backend._job_lock_stripe_index(first_id) !=
                  Backend._job_lock_stripe_index(second_id)

            io_started = Channel{Nothing}(1)
            io_release = Channel{Nothing}(1)
            blocked_once = Ref(false)
            blocking_ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                (source_path, destination_path) -> begin
                    result = Backend._atomic_replace_job_file_posix!(
                        source_path,
                        destination_path,
                    )
                    if !blocked_once[] &&
                       endswith(String(destination_path), "record.json")
                        blocked_once[] = true
                        put!(io_started, nothing)
                        take!(io_release)
                    end
                    return result
                end,
                Backend._fsync_job_directory_posix!,
            )
            first = @async Backend._with_job_lock(first_id) do
                record = Backend._job_record_locked(first_id)
                Backend._transition_job_record_unlocked_with_ops!(
                    record,
                    "running",
                    blocking_ops;
                    expected=("queued",),
                )
            end
            @test timedwait(() -> isready(io_started), 5.0; pollint=0.01) == :ok
            take!(io_started)

            global_probe = @async lock(Backend.JOBS_LOCK) do
                true
            end
            @test timedwait(() -> istaskdone(global_probe), 1.0; pollint=0.01) == :ok
            @test fetch(global_probe)

            second = @async Backend._job_transition!(
                second_id,
                "running";
                expected=("queued",),
            )
            @test timedwait(() -> istaskdone(second), 5.0; pollint=0.01) == :ok
            @test fetch(second).applied

            same_job = @async Backend._job_record(first_id)
            @test timedwait(() -> istaskdone(same_job), 0.2; pollint=0.01) ==
                  :timed_out
            put!(io_release, nothing)
            @test fetch(first).applied
            @test fetch(same_job)["status"] == "running"
        end
    end

    @testset "cold miss is single-flight and rejects wrong canonical id" begin
        with_store(capacity=4) do _
            job_id = seed_job("cold-single-flight")
            Backend._job_cache_remove!(job_id)
            reads = Threads.Atomic{Int}(0)
            read_started = Channel{Nothing}(1)
            read_release = Channel{Nothing}(1)
            function counted_read(path)
                Threads.atomic_add!(reads, 1)
                put!(read_started, nothing)
                take!(read_release)
                return Backend._read_job_json(path)
            end
            readers = [Threads.@spawn Backend._with_job_lock(job_id) do
                Backend._job_snapshot(Backend._job_record_locked(
                    job_id;
                    read_record=counted_read,
                ))
            end for _ in 1:8]
            @test timedwait(() -> isready(read_started), 5.0; pollint=0.01) == :ok
            take!(read_started)
            put!(read_release, nothing)
            snapshots = fetch.(readers)
            @test reads[] == 1
            @test all(record -> record["job_id"] == job_id, snapshots)

            wrong_id = seed_job("wrong-directory-id")
            wrong = Backend._read_job_json(Backend._job_record_path(wrong_id))
            wrong["job_id"] = "some-other-job"
            Backend._write_job_json(Backend._job_record_path(wrong_id), wrong)
            Backend._job_cache_remove!(wrong_id)
            @test_throws ArgumentError Backend._job_record(wrong_id)
            @test !(wrong_id in cache_snapshot().ids)
        end
    end

    @testset "hard LRU bound, reload, projection repair, and stress" begin
        with_store(capacity=2) do _
            first_id = seed_job("lru-first")
            second_id = seed_job("lru-second")
            Backend._job_record(first_id)
            third_id = seed_job("lru-third")
            snapshot = cache_snapshot()
            @test snapshot.capacity == 2
            @test snapshot.ids == Set([first_id, third_id])
            @test snapshot.access_ids == snapshot.ids
            @test Backend._job_record(second_id)["status"] == "succeeded"
            @test length(cache_snapshot().ids) == 2
        end

        with_store(capacity=1) do _
            repair_id = seed_job(
                "projection-after-eviction";
                state_revision=5,
            )
            projection = Backend._read_job_json(
                Backend._job_status_path(repair_id),
            )
            projection["state_revision"] = 4
            projection["progress"] = Dict("message" => "stale")
            Backend._write_job_json(
                Backend._job_status_path(repair_id),
                projection,
            )
            seed_job("projection-evictor")
            @test !(repair_id in cache_snapshot().ids)
            @test Backend._job_record(repair_id)["state_revision"] == 5
            repaired = Backend._read_job_json(
                Backend._job_status_path(repair_id),
            )
            @test repaired["state_revision"] == 5
            @test repaired["progress"]["message"] == "seeded"
        end

        with_store(capacity=8) do _
            ids = String[]
            for index in 1:40
                push!(ids, seed_job("stress-$(index)"))
                snapshot = cache_snapshot()
                @test length(snapshot.ids) <= 8
                @test snapshot.access_ids == snapshot.ids
            end
            for index in (40:-3:1)
                @test Backend._job_record(ids[index])["job_id"] == ids[index]
                snapshot = cache_snapshot()
                @test length(snapshot.ids) <= 8
                @test snapshot.access_ids == snapshot.ids
            end
        end
    end

    @testset "active local record may evict without restart recovery" begin
        with_store(capacity=1) do _
            job_id = seed_job("active-local"; status="running")
            token = Backend.LocalJobCancelToken(job_id)
            lock(Backend.JOBS_LOCK) do
                Backend.LOCAL_JOB_CANCEL_TOKENS[job_id] = token
            end
            try
                seed_job("active-local-evictor")
                @test !(job_id in cache_snapshot().ids)
                loaded = Backend._job_record(job_id)
                @test loaded["status"] == "running"
                @test !haskey(loaded, "error_code")
                transition = Backend._job_transition!(
                    job_id,
                    "cancel_requested";
                    expected=("running",),
                )
                @test transition.applied
                @test transition.record["state_revision"] == 2
            finally
                lock(Backend.JOBS_LOCK) do
                    delete!(Backend.LOCAL_JOB_CANCEL_TOKENS, job_id)
                end
            end
        end
    end

    @testset "post-commit cache maintenance uses frozen valid capacity" begin
        with_store(capacity=2) do _
            job_id = seed_job("capacity-freeze"; status="queued")
            previous = ENV["BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"]
            changed = Ref(false)
            ops = Backend._JobPersistenceOps(
                Backend._fsync_job_file_posix!,
                (source_path, destination_path) -> begin
                    result = Backend._atomic_replace_job_file_posix!(
                        source_path,
                        destination_path,
                    )
                    if !changed[] &&
                       endswith(String(destination_path), "record.json")
                        changed[] = true
                        ENV["BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"] =
                            "invalid-after-rename"
                    end
                    return result
                end,
                Backend._fsync_job_directory_posix!,
            )
            try
                transition = Backend._with_job_lock(job_id) do
                    record = Backend._job_record_locked(job_id)
                    Backend._transition_job_record_unlocked_with_ops!(
                        record,
                        "running",
                        ops;
                        expected=("queued",),
                    )
                end
                @test transition.applied
                @test transition.persistence.committed
                @test Backend._read_job_json(
                    Backend._job_record_path(job_id),
                )["state_revision"] == 2
            finally
                ENV["BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"] = previous
            end
            @test Backend._job_record(job_id)["status"] == "running"
        end
    end

    @testset "AWS initial-submit and cancel claims survive eviction" begin
        with_store(capacity=1) do dir
            aws_cli = write_cache_aws_cli(joinpath(dir, "aws-cache"))
            started = joinpath(dir, "s3-started")
            released = joinpath(dir, "s3-released")
            log_path = joinpath(dir, "aws.log")
            withenv(
                "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE" => "queue",
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION" => "definition",
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX" =>
                    "s3://cache-bucket/artifacts",
                "AWS_CACHE_LOG" => log_path,
                "AWS_CACHE_BLOCK_ON" => "s3-cp",
                "AWS_CACHE_BLOCK_STARTED" => started,
                "AWS_CACHE_BLOCK_RELEASE" => released,
                "AWS_CACHE_DESCRIBE_STATUS" => "SUBMITTED",
            ) do
                submission = @async Backend.submit_biocircuits_job_from_spec(
                    Dict(
                        "kind" => "query_atlas",
                        "spec" => Dict{String, Any}(),
                        "execution" => Dict("mode" => "aws_batch"),
                    ),
                )
                @test timedwait(() -> isfile(started), 5.0; pollint=0.01) == :ok
                live_id = lock(Backend.JOBS_LOCK) do
                    @test length(Backend.AWS_BATCH_INITIAL_SUBMISSIONS) == 1
                    only(Backend.AWS_BATCH_INITIAL_SUBMISSIONS)
                end
                @test Backend._job_record(live_id)["submission_state"] ==
                      "prepared"
                seed_job("aws-submit-evictor")
                @test !(live_id in cache_snapshot().ids)
                touch_file(released)
                submitted = fetch(submission)
                @test submitted["submission_state"] == "accepted"
                @test Backend._job_record(live_id)["submission_state"] ==
                      "accepted"
                @test lock(Backend.JOBS_LOCK) do
                    !(live_id in Backend.AWS_BATCH_INITIAL_SUBMISSIONS)
                end
                calls = isfile(log_path) ? readlines(log_path) : String[]
                @test count(line -> startswith(line, "batch submit-job"), calls) == 1
            end

            cancel_id = seed_job(
                "aws-cancel-eviction";
                status="queued",
                executor="aws_batch",
                batch_job_id="aws-cancel-external",
            )
            cancel_started = joinpath(dir, "cancel-started")
            cancel_released = joinpath(dir, "cancel-released")
            withenv(
                "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
                "AWS_CACHE_BLOCK_ON" => "cancel-job",
                "AWS_CACHE_BLOCK_STARTED" => cancel_started,
                "AWS_CACHE_BLOCK_RELEASE" => cancel_released,
                "AWS_CACHE_DESCRIBE_STATUS" => "SUBMITTED",
            ) do
                cancellation = @async Backend.cancel_biocircuits_job(cancel_id)
                @test timedwait(
                    () -> isfile(cancel_started),
                    5.0;
                    pollint=0.01,
                ) == :ok
                claimed = Backend._job_record(cancel_id)
                @test claimed["status"] == "cancel_requested"
                @test haskey(claimed, "cancel_dispatch_claim")
                seed_job("aws-cancel-evictor")
                @test !(cancel_id in cache_snapshot().ids)
                touch_file(cancel_released)
                @test fetch(cancellation)["status"] == "cancelled"
                final = Backend._job_record(cancel_id)
                @test final["status"] == "cancelled"
                @test !haskey(final, "cancel_dispatch_claim")
            end
        end
    end
end

end # module JobsCacheConcurrencyContract
