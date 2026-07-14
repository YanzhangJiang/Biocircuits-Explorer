module JobsSubmissionReconciliationContract

using Test
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend
const JOB_SEQUENCE = Ref(0)

function reset_runtime!()
    lock(Backend.JOBS_LOCK) do
        isempty(Backend.JOB_TASKS) || error(
            "Submission reconciliation fixture cannot reset active local jobs.")
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

function simulate_restart!()
    lock(Backend.JOBS_LOCK) do
        empty!(Backend.JOBS)
        empty!(Backend.JOB_CACHE_LAST_ACCESS)
        Backend.JOB_CACHE_ACCESS_CLOCK[] = UInt64(0)
        Backend.JOB_CACHE_CAPACITY[] = nothing
        empty!(Backend.JOB_DESCRIBE_LAST_AT)
        empty!(Backend.JOB_DESCRIBE_IN_FLIGHT)
        empty!(Backend.AWS_BATCH_INITIAL_SUBMISSIONS)
        empty!(Backend.JOB_STATUS_PROJECTION_DIRTY)
    end
    lock(Backend.JOB_STORE_DURABILITY_LOCK) do
        empty!(Backend.JOB_STORE_PENDING_DIR_FSYNC)
        Backend.JOB_STORE_DURABILITY_GENERATION[] = UInt64(0)
    end
    return nothing
end

function with_store(f::Function)
    previous_env = get(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE", nothing)
    previous_store = Backend.LOCAL_JOB_STORE_DIR[]
    mktempdir() do dir
        try
            reset_runtime!()
            ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = dir
            Backend.LOCAL_JOB_STORE_DIR[] = nothing
            f(dir)
        finally
            reset_runtime!()
            if previous_env === nothing
                delete!(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE")
            else
                ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] = previous_env
            end
            Backend.LOCAL_JOB_STORE_DIR[] = previous_store
        end
    end
end

function write_mock_aws(path::AbstractString)
    open(path, "w") do io
        write(io, raw"""#!/bin/sh
set -eu

printf '%s\n' "$*" >> "${AWS_RECONCILE_LOG:?}"

if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  if [ "${AWS_RECONCILE_S3_FAIL:-0}" = "1" ]; then
    printf 'injected S3 publication failure\n' >&2
    exit 31
  fi
  src="$3"
  dst="$4"
  case "$dst" in
    s3://*)
      target="${AWS_RECONCILE_S3_ROOT:?}/${dst#s3://}"
      mkdir -p "$(dirname "$target")"
      cp "$src" "$target"
      ;;
    *)
      source="${AWS_RECONCILE_S3_ROOT:?}/${src#s3://}"
      cp "$source" "$dst"
      ;;
  esac
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "submit-job" ]; then
  printf 'submit-max-attempts=%s\n' "${AWS_MAX_ATTEMPTS:-unset}" >> "${AWS_RECONCILE_LOG:?}"
  job_name=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --job-name) job_name="$2"; shift 2 ;;
      *)          shift ;;
    esac
  done
  if [ -n "${AWS_RECONCILE_SUBMIT_STARTED:-}" ]; then
    : > "$AWS_RECONCILE_SUBMIT_STARTED"
  fi
  if [ -n "${AWS_RECONCILE_SUBMIT_RELEASE:-}" ]; then
    while [ ! -f "$AWS_RECONCILE_SUBMIT_RELEASE" ]; do
      sleep 0.01
    done
  fi
  if [ "${AWS_RECONCILE_SUBMIT_MODE:-success}" = "ambiguous" ]; then
    printf 'accepted remotely but response was lost\n' >&2
    exit 41
  fi
  printf '{"jobId":"submit-job-1","jobName":"%s"}\n' "$job_name"
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "list-jobs" ]; then
  if [ -n "${AWS_RECONCILE_LIST_STARTED:-}" ]; then
    : > "$AWS_RECONCILE_LIST_STARTED"
  fi
  if [ -n "${AWS_RECONCILE_LIST_RELEASE:-}" ]; then
    while [ ! -f "$AWS_RECONCILE_LIST_RELEASE" ]; do
      sleep 0.01
    done
  fi
  cli_input=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --cli-input-json) cli_input="$2"; shift 2 ;;
      *)                shift ;;
    esac
  done
  case "$cli_input" in
    *'"nextToken":"page-2"'*) cat "${AWS_RECONCILE_LIST_PAGE2:?}" ;;
    *)                         cat "${AWS_RECONCILE_LIST_PAGE1:?}" ;;
  esac
  exit 0
fi

if [ "$1" = "batch" ] && [ "$2" = "describe-jobs" ]; then
  marker="${AWS_RECONCILE_DESCRIBE_PAGE2_MARKER:-}"
  if [ -n "$marker" ]; then
    case " $* " in
      *" $marker "*) cat "${AWS_RECONCILE_DESCRIBE_PAGE2:?}"; exit 0 ;;
    esac
  fi
  cat "${AWS_RECONCILE_DESCRIBE:?}"
  exit 0
fi

if [ "$1" = "batch" ] && { [ "$2" = "cancel-job" ] || [ "$2" = "terminate-job" ]; }; then
  exit 0
fi

printf 'unexpected mock AWS call: %s\n' "$*" >&2
exit 2
""")
    end
    chmod(path, 0o755)
    return path
end

function write_json(path::AbstractString, value)
    open(path, "w") do io
        JSON3.write(io, value)
        write(io, "\n")
    end
    return path
end

function with_aws(f::Function, dir::AbstractString;
                  submit_mode::AbstractString="success",
                  s3_fail::Bool=false,
                  list_started=nothing,
                  list_release=nothing,
                  submit_started=nothing,
                  submit_release=nothing)
    aws = write_mock_aws(joinpath(dir, "aws"))
    log = joinpath(dir, "aws.log")
    s3_root = joinpath(dir, "s3")
    page1 = write_json(joinpath(dir, "list-page-1.json"),
                       Dict("jobSummaryList" => Any[]))
    page2 = write_json(joinpath(dir, "list-page-2.json"),
                       Dict("jobSummaryList" => Any[]))
    describe = write_json(joinpath(dir, "describe.json"), Dict(
        "jobs" => Any[Dict(
            "jobId" => "submit-job-1",
            "status" => "SUBMITTED",
            "container" => Dict{String, Any}(),
        )],
    ))
    describe_page2 = write_json(
        joinpath(dir, "describe-page-2.json"),
        Dict("jobs" => Any[]),
    )
    pairs = Pair{String, Union{Nothing, String}}[
        "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws,
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE" => "queue-original",
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION" => "definition-original",
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX" => "s3://bucket/jobs",
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX" => "reconcile",
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => "us-west-2",
        "BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID" => "123456789012",
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL" => "0",
        "AWS_RECONCILE_LOG" => log,
        "AWS_RECONCILE_S3_ROOT" => s3_root,
        "AWS_RECONCILE_S3_FAIL" => s3_fail ? "1" : "0",
        "AWS_RECONCILE_SUBMIT_MODE" => String(submit_mode),
        "AWS_RECONCILE_LIST_PAGE1" => page1,
        "AWS_RECONCILE_LIST_PAGE2" => page2,
        "AWS_RECONCILE_DESCRIBE" => describe,
        "AWS_RECONCILE_DESCRIBE_PAGE2" => describe_page2,
        "AWS_RECONCILE_DESCRIBE_PAGE2_MARKER" => nothing,
        "AWS_RECONCILE_LIST_STARTED" => list_started,
        "AWS_RECONCILE_LIST_RELEASE" => list_release,
        "AWS_RECONCILE_SUBMIT_STARTED" => submit_started,
        "AWS_RECONCILE_SUBMIT_RELEASE" => submit_release,
    ]
    return withenv(pairs...) do
        f((
            aws=aws,
            log=log,
            s3_root=s3_root,
            page1=page1,
            page2=page2,
            describe=describe,
            describe_page2=describe_page2,
        ))
    end
end

function submit_request()
    return Dict{String, Any}(
        "kind" => "query_atlas",
        "execution" => Dict("mode" => "aws_batch"),
        "spec" => Dict{String, Any}(),
    )
end

function seed_submission(state::AbstractString;
                         reconcile_attempts::Int=0,
                         include_identity::Bool=true,
                         batch_job_id=nothing)
    JOB_SEQUENCE[] += 1
    job_id = string(JOB_SEQUENCE[], base=16, pad=32)
    prepared = Backend._prepare_job_spec_and_artifact_identity(
        "query_atlas",
        Dict{String, Any}(),
    )
    plan = include_identity ? Backend._prepare_aws_batch_submission_plan(
        job_id,
        Backend.ANONYMOUS_USER_SUB,
        "query_atlas",
        Dict{String, Any}(),
    ) : nothing
    now = Backend._now_iso_timestamp()
    record = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => "query_atlas",
        "status" => "queued",
        "executor" => "aws_batch",
        "user_sub" => Backend.ANONYMOUS_USER_SUB,
        "created_at" => now,
        "updated_at" => now,
        "state_revision" => 1,
        "result_available" => false,
        "progress" => Dict("message" => "seeded"),
        "spec" => deepcopy(prepared.spec),
        "expected_artifact_config_hash" => prepared.expected_artifact_config_hash,
        "input_path" => Backend._job_input_path(job_id),
        "status_path" => Backend._job_status_path(job_id),
        "record_path" => Backend._job_record_path(job_id),
        "result_path" => Backend._job_result_path(job_id),
        "input_uri" => Backend._job_input_path(job_id),
        "status_uri" => Backend._job_status_path(job_id),
        "result_uri" => Backend._job_result_path(job_id),
    )
    if include_identity
        record["result_protocol_version"] = Backend.JOB_RESULT_PROTOCOL_VERSION
        record["result_manifest_path"] = Backend._job_result_manifest_path(job_id)
        record["submission_protocol_version"] =
            Backend.AWS_BATCH_SUBMISSION_PROTOCOL_VERSION
        record["submission_state"] = String(state)
        record["submission_reconcile_attempts"] = reconcile_attempts
        record["submission_plan"] = deepcopy(plan)
        record["batch_job_name"] = String(plan["job_name"])
        for key in ("input_uri", "status_uri", "result_uri", "result_manifest_uri")
            record[key] = String(plan[key])
        end
    end
    batch_job_id === nothing ||
        (record["batch_job_id"] = String(batch_job_id))
    Backend._with_job_lock(job_id) do
        Backend._persist_job_record_unlocked(record)
        Backend._job_cache_publish!(job_id, record)
        Backend._persist_job_status_projection_unlocked(record)
    end
    return job_id
end

function described_resource(plan::AbstractDict,
                            key::AbstractString,
                            prefix::AbstractString;
                            add_revision::Bool=false)
    expected = String(plan[String(key)])
    startswith(expected, "arn:") && return expected
    account_id = get(plan, "account_id", nothing)
    account_id === nothing && return expected
    resource = add_revision && !occursin(':', expected) ?
        "$(expected):1" : expected
    return "arn:aws:batch:$(plan["region"]):$(account_id):$(prefix)/$(resource)"
end

function candidate_detail(record::AbstractDict, candidate_id::AbstractString;
                          tag_job_id=record["job_id"],
                          command=record["submission_plan"]["container_overrides"]["command"],
                          job_queue=nothing,
                          job_definition=nothing)
    plan = record["submission_plan"]
    return Dict{String, Any}(
        "jobId" => String(candidate_id),
        "jobName" => String(plan["job_name"]),
        "jobQueue" => job_queue === nothing ?
            described_resource(plan, "job_queue", "job-queue") :
            String(job_queue),
        "jobDefinition" => job_definition === nothing ?
            described_resource(
                plan,
                "job_definition",
                "job-definition";
                add_revision=true,
            ) : String(job_definition),
        "status" => "SUBMITTED",
        "tags" => Dict(Backend.AWS_BATCH_JOB_ID_TAG => String(tag_job_id)),
        "container" => Dict("command" => deepcopy(command)),
    )
end

function stage_candidates(files, record::AbstractDict,
                          candidate_ids::Vector{String};
                          second_page::Bool=false,
                          details=nothing)
    summaries = Any[
        Dict(
            "jobId" => id,
            "jobName" => record["submission_plan"]["job_name"],
        ) for id in candidate_ids
    ]
    if second_page
        write_json(files.page1, Dict(
            "jobSummaryList" => Any[],
            "nextToken" => "page-2",
        ))
        write_json(files.page2, Dict("jobSummaryList" => summaries))
    else
        write_json(files.page1, Dict("jobSummaryList" => summaries))
    end
    actual_details = details === nothing ?
        Any[candidate_detail(record, id) for id in candidate_ids] : details
    if length(candidate_ids) > 100
        write_json(files.describe, Dict("jobs" => actual_details[1:100]))
        write_json(files.describe_page2, Dict("jobs" => actual_details[101:end]))
        ENV["AWS_RECONCILE_DESCRIBE_PAGE2_MARKER"] = candidate_ids[101]
    else
        write_json(files.describe, Dict("jobs" => actual_details))
        write_json(files.describe_page2, Dict("jobs" => Any[]))
        ENV["AWS_RECONCILE_DESCRIBE_PAGE2_MARKER"] = ""
    end
    return nothing
end

function call_count(log_path::AbstractString, needle::AbstractString)
    isfile(log_path) || return 0
    return count(line -> occursin(needle, line),
                 split(read(log_path, String), '\n'))
end

@testset "AWS Batch submission reconciliation contract" begin
    @testset "region/account configuration is strict and ordered" begin
        withenv(
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => "eu-west-1",
            "AWS_REGION" => "us-east-1",
            "AWS_DEFAULT_REGION" => "ap-south-1",
        ) do
            @test Backend.Config.aws_batch_region() == "eu-west-1"
        end
        withenv(
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => nothing,
            "AWS_REGION" => "us-east-2",
            "AWS_DEFAULT_REGION" => "ap-south-1",
        ) do
            @test Backend.Config.aws_batch_region() == "us-east-2"
        end
        withenv(
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => nothing,
            "AWS_REGION" => nothing,
            "AWS_DEFAULT_REGION" => "ap-northeast-3",
        ) do
            @test Backend.Config.aws_batch_region() == "ap-northeast-3"
        end
        withenv(
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => "eusc-de-east-1",
        ) do
            @test Backend.Config.aws_batch_region() == "eusc-de-east-1"
        end
        for invalid in ("", "west", "US-WEST-2", "us west 2", "us-west")
            withenv(
                "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => invalid,
                "AWS_REGION" => nothing,
                "AWS_DEFAULT_REGION" => nothing,
            ) do
                @test_throws ArgumentError Backend.Config.aws_batch_region()
            end
        end
        withenv("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID" => nothing) do
            @test Backend.Config.aws_account_id() === nothing
        end
        withenv("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID" => "123456789012") do
            @test Backend.Config.aws_account_id() == "123456789012"
        end
        for invalid in ("123", "12345678901a", " 12345678901 ")
            withenv("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID" => invalid) do
                @test_throws ArgumentError Backend.Config.aws_account_id()
            end
        end
    end

    @testset "normal submission persists identity and calls SubmitJob once" begin
        with_store() do store
            with_aws(store) do files
                submitted = Backend.submit_biocircuits_job_from_spec(submit_request())
                job_id = String(submitted["job_id"])
                record = Backend._job_record(job_id)
                plan = record["submission_plan"]

                @test record["submission_protocol_version"] ==
                      Backend.AWS_BATCH_SUBMISSION_PROTOCOL_VERSION
                @test record["submission_state"] == "accepted"
                @test plan["canonical_job_id"] == job_id
                @test plan["region"] == "us-west-2"
                @test plan["account_id"] == "123456789012"
                @test submitted["submission"]["region"] == "us-west-2"
                @test submitted["submission"]["account_id"] ==
                      "123456789012"
                @test endswith(String(plan["job_name"]), job_id)
                @test length(String(plan["job_name"])) <= 128
                @test plan["tags"][Backend.AWS_BATCH_JOB_ID_TAG] == job_id
                @test plan["container_overrides"]["command"][end - 5] ==
                      "--input-uri"
                @test isfile(record["input_path"])
                @test isfile(joinpath(
                    files.s3_root,
                    split(String(record["input_uri"])[6:end], "/")...,
                ))
                @test call_count(files.log, "batch submit-job") == 1
                @test occursin("submit-max-attempts=1", read(files.log, String))
                @test occursin(
                    "batch submit-job",
                    read(files.log, String),
                )
                @test occursin("--region us-west-2", read(files.log, String))
            end
        end
    end

    @testset "input publication failure is terminal before dispatch" begin
        with_store() do store
            with_aws(store; s3_fail=true) do files
                @test_throws ProcessFailedException Backend.submit_biocircuits_job_from_spec(
                    submit_request(),
                )
                record = only(values(Backend.JOBS))
                @test record["status"] == "failed"
                @test record["submission_state"] == "failed_before_dispatch"
                @test record["error_code"] == "aws_input_publication_failed"
                @test call_count(files.log, "batch submit-job") == 0
            end
        end
    end

    @testset "dispatch authorization requires durable parent directory" begin
        with_store() do store
            with_aws(store) do files
                failed_ops = Backend._JobPersistenceOps(
                    Backend._fsync_job_file_posix!,
                    Backend._atomic_replace_job_file_posix!,
                    _path -> error("injected dispatch directory fsync failure"),
                )
                pending = Backend.submit_biocircuits_job_from_spec(
                    submit_request();
                    dispatch_persistence_ops=failed_ops,
                )
                job_id = String(pending["job_id"])
                @test pending["submission_state"] == "dispatch_started"
                @test pending["submission"]["dispatch_durability"] ==
                      "unconfirmed"
                @test call_count(files.log, "batch submit-job") == 0
                @test Backend._read_job_json(
                    Backend._job_record_path(job_id),
                )["submission_state"] == "dispatch_started"

                simulate_restart!()
                reconciled = Backend.get_biocircuits_job(job_id)
                @test reconciled["submission_state"] == "reconciling"
                @test !haskey(reconciled, "external_job_id")
                @test call_count(files.log, "batch list-jobs") == 1
                @test call_count(files.log, "batch submit-job") == 0
            end
        end

        with_store() do store
            with_aws(store) do files
                directory_syncs = Ref(0)
                retry_ops = Backend._JobPersistenceOps(
                    Backend._fsync_job_file_posix!,
                    Backend._atomic_replace_job_file_posix!,
                    path -> begin
                        directory_syncs[] += 1
                        directory_syncs[] <= 2 && error(
                            "injected pre-authorization directory fsync failure")
                        Backend._fsync_job_directory_posix!(path)
                    end,
                )
                submitted = Backend.submit_biocircuits_job_from_spec(
                    submit_request();
                    dispatch_persistence_ops=retry_ops,
                )
                @test submitted["submission_state"] == "accepted"
                @test directory_syncs[] >= 3
                @test call_count(files.log, "batch submit-job") == 1
            end
        end

        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("prepared")
                dispatch = Backend._begin_aws_batch_dispatch!(job_id)
                @test dispatch.authorization !== nothing
                accepted = Backend._aws_batch_submit(dispatch.authorization)
                @test accepted["batch_job_id"] == "submit-job-1"
                @test_throws ArgumentError Backend._aws_batch_submit(
                    dispatch.authorization,
                )
                @test call_count(files.log, "batch submit-job") == 1
            end
        end
    end

    @testset "ambiguous accepted submission is adopted without resubmit" begin
        with_store() do store
            with_aws(store; submit_mode="ambiguous") do files
                ambiguous = Backend.submit_biocircuits_job_from_spec(submit_request())
                @test ambiguous["submission_state"] == "reconciling"
                job_id = String(ambiguous["job_id"])
                record = Backend._job_record(job_id)
                stage_candidates(files, record, ["adopted-1"])

                adopted = Backend.get_biocircuits_job(job_id)
                @test adopted["external_job_id"] == "adopted-1"
                @test adopted["submission_state"] == "accepted"
                @test call_count(files.log, "batch submit-job") == 1
            end
        end
    end

    @testset "dispatch-started cold recovery reconciles but never submits" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                simulate_restart!()
                refreshed = Backend.get_biocircuits_job(job_id)
                @test refreshed["submission_state"] == "reconciling"
                @test refreshed["submission"]["reconcile_attempts"] == 1
                @test call_count(files.log, "batch list-jobs") == 1
                @test call_count(files.log, "batch submit-job") == 0
            end
        end
    end

    @testset "prepared cold recovery proves zero remote attempts" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("prepared")
                simulate_restart!()
                recovered = Backend.get_biocircuits_job(job_id)
                @test recovered["status"] == "failed"
                @test recovered["submission_state"] ==
                      "interrupted_before_dispatch"
                @test recovered["error_code"] ==
                      "aws_submission_interrupted_before_dispatch"
                @test call_count(files.log, "batch list-jobs") == 0
                @test call_count(files.log, "batch submit-job") == 0
            end
        end
    end

    @testset "pagination reaches a candidate on the second page" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                stage_candidates(files, record, ["page-two-job"]; second_page=true)
                adopted = Backend.get_biocircuits_job(job_id)
                @test adopted["external_job_id"] == "page-two-job"
                @test call_count(files.log, "batch list-jobs") == 2
            end
        end
    end

    @testset "DescribeJobs must completely cover every requested chunk" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                ids = [
                    "chunk-$(lpad(string(index), 3, '0'))" for index in 1:101
                ]
                stage_candidates(files, record, ids)
                conflict = Backend.get_biocircuits_job(job_id)
                @test conflict["submission_state"] == "conflict"
                @test length(
                    conflict["submission"]["submission_conflict_job_ids"],
                ) == 101
                @test call_count(files.log, "batch describe-jobs") == 2
            end
        end

        malformed_cases = (
            (
                "missing",
                ["missing-a", "missing-b"],
                record -> Any[candidate_detail(record, "missing-a")],
                "completely cover",
            ),
            (
                "duplicate",
                ["duplicate-detail"],
                record -> Any[
                    candidate_detail(record, "duplicate-detail"),
                    candidate_detail(record, "duplicate-detail"),
                ],
                "duplicate detail",
            ),
            (
                "external",
                ["listed-detail"],
                record -> Any[
                    candidate_detail(record, "listed-detail"),
                    candidate_detail(record, "not-requested"),
                ],
                "unrequested job ID",
            ),
            (
                "malformed",
                ["malformed-detail"],
                _record -> Any[Dict("jobId" => "malformed-detail")],
                "incomplete job detail",
            ),
        )
        for (label, ids, details_for, diagnostic) in malformed_cases
            with_store() do store
                with_aws(store) do files
                    job_id = seed_submission("dispatch_started")
                    record = Backend._job_record(job_id)
                    stage_candidates(
                        files,
                        record,
                        ids;
                        details=details_for(record),
                    )
                    retried = Backend.get_biocircuits_job(job_id)
                    @test retried["submission_state"] == "reconciling"
                    @test !haskey(retried, "external_job_id")
                    @test occursin(
                        diagnostic,
                        retried["submission"]["submission_last_error"],
                    )
                    @test call_count(files.log, "batch submit-job") == 0
                end
            end
        end
    end

    @testset "ListJobs observations fail closed before candidate counting" begin
        malformed_lists = (
            (
                "missing-array",
                _record -> Dict{String, Any}(),
                "jobSummaryList array",
            ),
            (
                "non-array",
                _record -> Dict("jobSummaryList" => "not-an-array"),
                "jobSummaryList array",
            ),
            (
                "non-object",
                _record -> Dict("jobSummaryList" => Any["not-an-object"]),
                "malformed job summary",
            ),
            (
                "empty-id",
                record -> Dict("jobSummaryList" => Any[Dict(
                    "jobId" => "",
                    "jobName" => record["submission_plan"]["job_name"],
                )]),
                "malformed jobId",
            ),
            (
                "non-string-id",
                record -> Dict("jobSummaryList" => Any[Dict(
                    "jobId" => 17,
                    "jobName" => record["submission_plan"]["job_name"],
                )]),
                "malformed jobId",
            ),
            (
                "duplicate-id",
                record -> Dict("jobSummaryList" => Any[
                    Dict(
                        "jobId" => "duplicate-list-id",
                        "jobName" => record["submission_plan"]["job_name"],
                    ),
                    Dict(
                        "jobId" => "duplicate-list-id",
                        "jobName" => record["submission_plan"]["job_name"],
                    ),
                ]),
                "duplicate job ID",
            ),
        )
        for (_label, payload_for, diagnostic) in malformed_lists
            with_store() do store
                with_aws(store) do files
                    job_id = seed_submission("dispatch_started")
                    record = Backend._job_record(job_id)
                    write_json(files.page1, payload_for(record))
                    retried = Backend.get_biocircuits_job(job_id)
                    @test retried["submission_state"] == "reconciling"
                    @test !haskey(retried, "external_job_id")
                    @test occursin(
                        diagnostic,
                        retried["submission"]["submission_last_error"],
                    )
                    @test call_count(files.log, "batch describe-jobs") == 0
                end
            end
        end
    end

    @testset "wrong identity candidates are rejected" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                wrong_command = deepcopy(
                    record["submission_plan"]["container_overrides"]["command"],
                )
                wrong_command[end] = "s3://wrong/result.json"
                details = Any[
                    candidate_detail(
                        record,
                        "wrong-tag";
                        tag_job_id=repeat("0", 32),
                    ),
                    candidate_detail(record, "wrong-command"; command=wrong_command),
                    candidate_detail(record, "not-listed"),
                ]
                stage_candidates(
                    files,
                    record,
                    ["wrong-tag", "wrong-command"];
                    details=details,
                )
                rejected = Backend.get_biocircuits_job(job_id)
                @test rejected["submission_state"] == "reconciling"
                @test !haskey(rejected, "external_job_id")
                @test occursin(
                    "unrequested job ID",
                    rejected["submission"]["submission_last_error"],
                )
            end
        end
    end

    @testset "region/account and full ARN identity cannot degrade to names" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                plan = record["submission_plan"]
                wrong_region = "arn:aws:batch:us-east-1:123456789012:job-queue/$(plan["job_queue"])"
                wrong_account = "arn:aws:batch:us-west-2:999999999999:job-definition/$(plan["job_definition"]):1"
                stage_candidates(
                    files,
                    record,
                    ["wrong-region", "wrong-account"];
                    details=Any[
                        candidate_detail(
                            record,
                            "wrong-region";
                            job_queue=wrong_region,
                        ),
                        candidate_detail(
                            record,
                            "wrong-account";
                            job_definition=wrong_account,
                        ),
                    ],
                )
                rejected = Backend.get_biocircuits_job(job_id)
                @test rejected["submission_state"] == "reconciling"
                @test !haskey(rejected, "external_job_id")
            end
        end

        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                plan = record["submission_plan"]
                nonnumeric_revision =
                    "arn:aws:batch:$(plan["region"]):$(plan["account_id"]):" *
                    "job-definition/$(plan["job_definition"]):latest"
                stage_candidates(
                    files,
                    record,
                    ["nonnumeric-revision"];
                    details=Any[candidate_detail(
                        record,
                        "nonnumeric-revision";
                        job_definition=nonnumeric_revision,
                    )],
                )
                rejected = Backend.get_biocircuits_job(job_id)
                @test rejected["submission_state"] == "reconciling"
                @test !haskey(rejected, "external_job_id")
            end
        end

        with_store() do store
            with_aws(store) do files
                queue_arn = "arn:aws:batch:us-west-2:123456789012:job-queue/queue-original"
                definition_arn = "arn:aws:batch:us-west-2:123456789012:job-definition/definition-original:7"
                job_id = withenv(
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE" => queue_arn,
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION" =>
                        definition_arn,
                ) do
                    seed_submission("dispatch_started")
                end
                record = Backend._job_record(job_id)
                stage_candidates(
                    files,
                    record,
                    ["same-name-different-arn"];
                    details=Any[candidate_detail(
                        record,
                        "same-name-different-arn";
                        job_definition=replace(definition_arn, ":7" => ":8"),
                    )],
                )
                rejected = Backend.get_biocircuits_job(job_id)
                @test rejected["submission_state"] == "reconciling"
                @test !haskey(rejected, "external_job_id")
            end
        end
    end

    @testset "zero candidates becomes unknown and remains adoptable" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission(
                    "reconciling";
                    reconcile_attempts=
                        Backend.AWS_BATCH_RECONCILE_UNKNOWN_AFTER_ATTEMPTS - 1,
                )
                unknown = Backend.get_biocircuits_job(job_id)
                @test unknown["submission_state"] == "unknown"
                record = Backend._job_record(job_id)
                stage_candidates(files, record, ["late-visible"])
                adopted = Backend.get_biocircuits_job(job_id)
                @test adopted["external_job_id"] == "late-visible"
                @test adopted["submission_state"] == "accepted"
            end
        end
    end

    @testset "multiple exact candidates become an explicit conflict" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                record = Backend._job_record(job_id)
                stage_candidates(files, record, ["duplicate-a", "duplicate-b"])
                conflict = Backend.get_biocircuits_job(job_id)
                @test conflict["submission_state"] == "conflict"
                @test Set(conflict["submission"]["submission_conflict_job_ids"]) ==
                      Set(["duplicate-a", "duplicate-b"])
                @test !haskey(conflict, "external_job_id")
            end
        end
    end

    @testset "prepared and post-dispatch cancellation keep distinct semantics" begin
        with_store() do store
            with_aws(store) do files
                prepared_id = seed_submission("prepared")
                cancelled = Backend.cancel_biocircuits_job(prepared_id)
                @test cancelled["status"] == "cancelled"
                @test cancelled["submission_state"] ==
                      "cancelled_before_dispatch"
                @test Backend._begin_aws_batch_dispatch!(prepared_id) === nothing

                dispatched_id = seed_submission("dispatch_started")
                requested = Backend.cancel_biocircuits_job(dispatched_id)
                @test requested["status"] == "cancel_requested"
                record = Backend._job_record(dispatched_id)
                stage_candidates(files, record, ["cancel-adopted"])
                settled = Backend.get_biocircuits_job(dispatched_id)
                @test settled["status"] == "cancelled"
                @test call_count(files.log, "batch cancel-job") == 1
                @test call_count(files.log, "batch submit-job") == 0
            end
        end
    end

    @testset "legacy missing identity is never guessed or resubmitted" begin
        with_store() do store
            with_aws(store) do files
                legacy_id = seed_submission(
                    "legacy";
                    include_identity=false,
                )
                simulate_restart!()
                legacy = Backend.get_biocircuits_job(legacy_id)
                @test legacy["submission_state"] ==
                      "legacy_submission_unknown"
                @test legacy["submission"]["identity_source"] ==
                      "legacy_runtime_region"
                @test call_count(files.log, "batch list-jobs") == 0
                @test call_count(files.log, "batch submit-job") == 0

                accepted_id = seed_submission(
                    "legacy";
                    include_identity=false,
                    batch_job_id="legacy-with-id",
                )
                accepted = Backend.get_biocircuits_job(accepted_id)
                @test accepted["submission_state"] == "accepted"
                @test accepted["submission"]["identity_source"] ==
                      "legacy_runtime_region"
                @test occursin("--region us-west-2", read(files.log, String))
            end
        end
    end

    @testset "concurrent reconciliation has one external owner" begin
        with_store() do store
            started = joinpath(store, "list-started")
            released = joinpath(store, "list-released")
            with_aws(
                store;
                list_started=started,
                list_release=released,
            ) do files
                job_id = seed_submission("dispatch_started")
                first = @async Backend._refresh_aws_batch_job!(job_id)
                @test timedwait(() -> isfile(started), 5.0; pollint=0.01) == :ok
                second = @async Backend._refresh_aws_batch_job!(
                    job_id;
                    now_seconds=Backend._monotonic_seconds() + 1000,
                )
                @test timedwait(() -> istaskdone(second), 1.0; pollint=0.01) == :ok
                @test fetch(second)["submission_state"] == "dispatch_started"
                open(released, "w") do io
                    write(io, "release\n")
                end
                @test fetch(first)["submission_state"] == "reconciling"
                @test call_count(files.log, "batch list-jobs") == 1
                @test lock(Backend.JOBS_LOCK) do
                    !(job_id in Backend.JOB_DESCRIBE_IN_FLIGHT)
                end
            end
        end
    end

    @testset "restart reconciliation uses the persisted plan after config drift" begin
        with_store() do store
            with_aws(store) do files
                job_id = seed_submission("dispatch_started")
                original = Backend._job_record(job_id)
                stage_candidates(files, original, ["config-stable"])
                simulate_restart!()
                withenv(
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE" => "queue-new",
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION" => "definition-new",
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX" => "new-prefix",
                    "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION" => "us-east-1",
                    "BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID" => "999999999999",
                ) do
                    adopted = Backend.get_biocircuits_job(job_id)
                    @test adopted["external_job_id"] == "config-stable"
                end
                reloaded = Backend._job_record(job_id)
                @test reloaded["submission_plan"]["job_queue"] ==
                      "queue-original"
                @test reloaded["submission_plan"]["job_definition"] ==
                      "definition-original"
                @test reloaded["submission_plan"]["region"] == "us-west-2"
                @test reloaded["submission_plan"]["account_id"] ==
                      "123456789012"
                @test occursin("queue-original", read(files.log, String))
                @test !occursin("queue-new", read(files.log, String))
                @test occursin("--region us-west-2", read(files.log, String))
                @test !occursin("--region us-east-1", read(files.log, String))
            end
        end
    end
end

end # module JobsSubmissionReconciliationContract
