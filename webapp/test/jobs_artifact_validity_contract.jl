module JobsArtifactValidityContract

using Test
using Dates
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend
const JOB_SEQUENCE = Ref(0)

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
    previous_env = haskey(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE") ?
        ENV["BIOCIRCUITS_EXPLORER_JOB_STORE"] : nothing
    previous_store_dir = Backend.LOCAL_JOB_STORE_DIR[]
    previous_batch_region = get(
        ENV,
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION",
        nothing,
    )

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

function write_artifact_aws_cli(path::AbstractString)
    open(path, "w") do io
        write(io, raw"""#!/bin/sh
set -eu

if [ -n "${AWS_ARTIFACT_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$AWS_ARTIFACT_LOG"
fi

s3_path() {
  uri="${1#s3://}"
  printf '%s/%s' "${AWS_ARTIFACT_S3_ROOT:?}" "$uri"
}

block_if_requested() {
  label="$1"
  if [ "${AWS_ARTIFACT_BLOCK_ON:-never}" = "$label" ]; then
    : > "${AWS_ARTIFACT_BLOCK_STARTED:?}"
    while [ ! -f "${AWS_ARTIFACT_BLOCK_RELEASE:?}" ]; do
      sleep 0.01
    done
  fi
}

if [ "$1" = "batch" ] && [ "$2" = "describe-jobs" ]; then
  printf '{"jobs":[{"jobId":"artifact-job","status":"SUCCEEDED","container":{}}]}\n'
  exit 0
fi

if [ "$1" = "s3api" ] && [ "$2" = "head-object" ]; then
  block_if_requested head-object
  mode="${AWS_ARTIFACT_HEAD_MODE:-auto}"
  if [ "$mode" = "access-denied" ]; then
    printf 'An error occurred (AccessDenied) when calling the HeadObject operation\n' >&2
    exit 254
  fi
  if [ "$mode" = "transient" ]; then
    printf 'EndpointConnectionError: temporary network failure\n' >&2
    exit 255
  fi
  if [ "$mode" = "profile-not-found" ]; then
    printf 'The config profile (not found) could not be loaded\n' >&2
    exit 255
  fi
  if [ "$mode" = "missing" ]; then
    printf 'An error occurred (404) when calling the HeadObject operation: Not Found\n' >&2
    exit 254
  fi

  bucket=""
  key=""
  shift 2
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --bucket) bucket="$2"; shift 2 ;;
      --key) key="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  target="${AWS_ARTIFACT_S3_ROOT:?}/$bucket/$key"
  if [ -f "$target" ]; then
    sha256=""
    if [ -f "$target.bne-sha256" ]; then
      sha256="$(cat "$target.bne-sha256")"
    fi
    content_type=""
    if [ -f "$target.content-type" ]; then
      content_type="$(cat "$target.content-type")"
    fi
    content_length="$(wc -c <"$target" | tr -d ' ')"
    if [ -f "$target.head-content-length-json" ]; then
      content_length="$(cat "$target.head-content-length-json")"
    fi
    printf '{"ContentLength":%s,"ContentType":"%s","Metadata":{"bne-result-sha256":"%s"}}\n' \
      "$content_length" "$content_type" "$sha256"
    exit 0
  fi
  printf 'An error occurred (NoSuchKey) when calling the HeadObject operation\n' >&2
  exit 254
fi

if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  block_if_requested s3-cp
  if [ "${AWS_ARTIFACT_CP_MODE:-ok}" = "error" ]; then
    printf 'download temporarily unavailable\n' >&2
    exit 23
  fi
  src="$3"
  dst="$4"
  if [ "${dst#s3://}" != "$dst" ]; then
    case "$dst" in
      */result-manifest.json) block_if_requested upload-manifest ;;
      */result.json) block_if_requested upload-result ;;
      */status.json) block_if_requested upload-status ;;
    esac
    target="$(s3_path "$dst")"
    mkdir -p "$(dirname "$target")"
    cp "$src" "$target"
    content_type=""
    shift 4
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --content-type)
          content_type="$2"
          shift 2
          ;;
        --metadata)
          metadata="$2"
          case "$metadata" in
            bne-result-sha256=*)
              printf '%s' "${metadata#bne-result-sha256=}" > "$target.bne-sha256"
              ;;
          esac
          shift 2
          ;;
        *) shift ;;
      esac
    done
    if [ -n "$content_type" ]; then
      printf '%s' "$content_type" > "$target.content-type"
    fi
  else
    cp "$(s3_path "$src")" "$dst"
  fi
  exit 0
fi

printf 'unexpected mock AWS call: %s\n' "$*" >&2
exit 2
""")
    end
    chmod(path, 0o755)
    return path
end

function s3_path(root::AbstractString, uri::AbstractString)
    startswith(String(uri), "s3://") || error("Expected S3 URI: $(uri)")
    return joinpath(root, split(String(uri)[6:end], "/")...)
end

function seed_aws_job(; status::AbstractString="running",
                      kind::AbstractString="query_atlas",
                      spec=Dict{String, Any}("query" => Dict("limit" => 3)),
                      manifest_protocol::Bool=true)
    JOB_SEQUENCE[] += 1
    job_id = string(JOB_SEQUENCE[], base=16, pad=32)
    now = Backend._now_iso_timestamp()
    prepared = Backend._prepare_job_spec_and_artifact_identity(kind, spec)
    record = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => String(kind),
        "status" => String(status),
        "executor" => "aws_batch",
        "user_sub" => Backend.ANONYMOUS_USER_SUB,
        "created_at" => now,
        "updated_at" => now,
        "state_revision" => 1,
        "started_at" => now,
        "result_available" => false,
        "progress" => Dict("message" => "seeded"),
        "spec" => deepcopy(prepared.spec),
        "expected_artifact_config_hash" => prepared.expected_artifact_config_hash,
        "batch_job_id" => "artifact-job-$(job_id)",
        "input_path" => Backend._job_input_path(job_id),
        "status_path" => Backend._job_status_path(job_id),
        "record_path" => Backend._job_record_path(job_id),
        "result_path" => Backend._job_result_path(job_id),
        "input_uri" => "s3://artifact-bucket/jobs/$(job_id)/input.json",
        "status_uri" => "s3://artifact-bucket/jobs/$(job_id)/status.json",
        "result_uri" => "s3://artifact-bucket/jobs/$(job_id)/result.json",
    )
    if manifest_protocol
        record["result_protocol_version"] = Backend.JOB_RESULT_PROTOCOL_VERSION
        record["result_manifest_uri"] =
            "s3://artifact-bucket/jobs/$(job_id)/result-manifest.json"
    end
    Backend._with_job_lock(job_id) do
        Backend._persist_job_record_unlocked(record)
        Backend._job_cache_publish!(job_id, record)
        Backend._persist_job_status_unlocked(record)
    end
    return job_id
end

function valid_result(record::AbstractDict)
    result = Dict{String, Any}("ok" => true, "job_id" => String(record["job_id"]))
    artifact_config = Backend._job_artifact_config(
        String(record["kind"]),
        record["spec"],
    )
    Backend.attach_artifact!(
        result,
        String(record["kind"]);
        input_hashes=Dict{String, Any}(
            "network_ir_hashes" => Backend.artifact_network_hashes(record["spec"]),
        ),
        config=artifact_config,
    )
    return result
end

function frozen_rop_shape_request_variant(variant::Symbol)
    fixture_path = normpath(joinpath(
        @__DIR__, "..", "..", "benchmarks", "rop_shape_control",
        "cat_fixed_topology_results.json",
    ))
    fixture = Backend._materialize(JSON3.read(read(fixture_path, String)))
    request = deepcopy(
        fixture["edits"][1]["direct_lp"]["result"]["normalized_request"])
    if variant == :legacy
        network = Backend.parse_network_ir(request["network"])
        bridge = Backend.network_ir_to_legacy_inputs(network)
        request["network"] = Dict{String, Any}(
            "label" => bridge.label,
            "reactions" => bridge.rules,
            "kd" => bridge.kd,
            "input_symbols" => bridge.input_symbols,
            "output_symbols" => bridge.output_symbols,
        )
        request["expected_network_ir_hash"] = nothing
    elseif variant == :structured_without_provenance
        delete!(request["network"], "provenance")
    else
        error("Unknown ROP request variant: $(variant)")
    end
    return request
end

function stage_result(root::AbstractString, record::AbstractDict, value)
    path = s3_path(root, String(record["result_uri"]))
    mkpath(dirname(path))
    open(path, "w") do io
        if value isa AbstractString || value isa AbstractVector{UInt8}
            write(io, value)
        else
            JSON3.pretty(io, value)
            write(io, "\n")
        end
    end
    write("$(path).content-type", Backend.JOB_RESULT_MEDIA_TYPE)
    return path
end

function stage_manifest(root::AbstractString, record::AbstractDict, manifest)
    path = s3_path(root, String(record["result_manifest_uri"]))
    mkpath(dirname(path))
    open(path, "w") do io
        if manifest isa AbstractString || manifest isa AbstractVector{UInt8}
            write(io, manifest)
        else
            JSON3.pretty(io, manifest)
            write(io, "\n")
        end
    end
    return path
end

function stage_committed_result(root::AbstractString,
                                record::AbstractDict,
                                result=valid_result(record))
    result_path = stage_result(root, record, result)
    sha256_hex = Backend._file_sha256_hex(result_path)
    write("$(result_path).bne-sha256", sha256_hex)
    manifest = Backend._job_result_manifest_payload(
        result,
        String(record["job_id"]),
        String(record["kind"]),
        String(record["expected_artifact_config_hash"]),
        String(record["result_uri"]),
        filesize(result_path),
        sha256_hex,
    )
    stage_manifest(root, record, manifest)
    return manifest
end

function assert_job_lock_available()
    acquired = Channel{Bool}(1)
    waiter = @async lock(Backend.JOBS_LOCK) do
        put!(acquired, true)
    end
    @test timedwait(() -> isready(acquired), 1.0; pollint=0.01) == :ok
    @test take!(acquired)
    wait(waiter)
end

@testset "AWS result artifact validity contract" begin
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

        aws_cli = write_artifact_aws_cli(joinpath(store_dir, "aws"))
        s3_root = joinpath(store_dir, "s3")
        aws_log = joinpath(store_dir, "aws.log")
        withenv(
            "BIOCIRCUITS_EXPLORER_AWS_CLI" => aws_cli,
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL" => "0",
            "AWS_ARTIFACT_S3_ROOT" => s3_root,
            "AWS_ARTIFACT_HEAD_MODE" => "auto",
            "AWS_ARTIFACT_CP_MODE" => "ok",
            "AWS_ARTIFACT_BLOCK_ON" => "never",
            "AWS_ARTIFACT_LOG" => aws_log,
        ) do
            @testset "presence probe distinguishes absence from operational errors" begin
                uri = "s3://artifact-bucket/probes/result.json"
                probe_path = s3_path(s3_root, uri)
                mkpath(dirname(probe_path))
                write(probe_path, "{}\n")
                @test Backend._probe_artifact_presence(uri).status == :present
                @test withenv("AWS_ARTIFACT_HEAD_MODE" => "missing") do
                    Backend._probe_artifact_presence(uri).status
                end == :missing
                @test withenv("AWS_ARTIFACT_HEAD_MODE" => "access-denied") do
                    Backend._probe_artifact_presence(uri).status
                end == :retryable_error
                @test withenv("AWS_ARTIFACT_HEAD_MODE" => "transient") do
                    Backend._probe_artifact_presence(uri).status
                end == :retryable_error
                @test withenv("AWS_ARTIFACT_HEAD_MODE" => "profile-not-found") do
                    Backend._probe_artifact_presence(uri).status
                end == :retryable_error
                @test Backend._s3_head_definitively_missing(
                    "",
                    "An error occurred (NoSuchKey) when calling HeadObject operation: missing",
                )
                @test Backend._s3_head_definitively_missing(
                    "",
                    "An error occurred (NotFound) when calling the HeadObject operation",
                )
                @test !Backend._s3_head_definitively_missing(
                    "",
                    "An error occurred (AccessDenied) when calling the HeadObject operation: status 403",
                )
                @test !Backend._s3_head_definitively_missing(
                    "",
                    "The config profile (not found) could not be loaded",
                )
            end

            @testset "valid artifact is required before terminal success" begin
                job_id = seed_aws_job()
                record = Backend._job_record(job_id)
                manifest = stage_committed_result(s3_root, record)
                @test occursin(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
                    String(manifest["created_at"]),
                )
                @test tryparse(
                    Dates.DateTime,
                    String(manifest["created_at"]),
                    dateformat"yyyy-mm-ddTHH:MM:SSZ",
                ) !== nothing
                write(aws_log, "")
                refreshed = Backend._refresh_aws_batch_job!(job_id)
                @test refreshed["status"] == "succeeded"
                @test refreshed["result_available"] == true
                @test refreshed["progress"]["artifact_status"] == "valid"
                @test refreshed["progress"]["artifact_verification_mode"] == "manifest"
                log_text = read(aws_log, String)
                @test occursin("result-manifest.json", log_text)
                @test !occursin("s3 cp $(record["result_uri"])", log_text)
            end

            @testset "ROP artifact identity is fixed before worker handoff" begin
                for variant in (:legacy, :structured_without_provenance)
                    submitted = frozen_rop_shape_request_variant(variant)
                    prepared = Backend._prepare_job_spec_and_artifact_identity(
                        "rop_shape_optimize",
                        submitted,
                    )
                    @test haskey(prepared.spec["network"], "provenance")
                    @test prepared.expected_artifact_config_hash == Backend._canonical_hash(
                        Backend._job_artifact_config("rop_shape_optimize", prepared.spec))

                    job_id = seed_aws_job(
                        kind="rop_shape_optimize",
                        spec=submitted,
                    )
                    record = Backend._job_record(job_id)
                    @test record["expected_artifact_config_hash"] == Backend._canonical_hash(
                        Backend._job_artifact_config("rop_shape_optimize", record["spec"]))
                    stage_committed_result(s3_root, record)
                    refreshed = Backend._refresh_aws_batch_job!(job_id)
                    @test refreshed["status"] == "succeeded"
                    @test refreshed["progress"]["artifact_status"] == "valid"
                end
            end

            @testset "definitive absence is terminal but probe failure retries" begin
                missing_id = seed_aws_job()
                missing = Backend._refresh_aws_batch_job!(missing_id)
                @test missing["status"] == "failed"
                @test missing["result_available"] == false
                @test missing["error_code"] == "aws_result_artifact_missing"
                @test occursin("manifest is missing", String(missing["error"]))

                retry_id = seed_aws_job()
                retry_record = Backend._job_record(retry_id)
                stage_committed_result(s3_root, retry_record)
                retrying = withenv("AWS_ARTIFACT_HEAD_MODE" => "access-denied") do
                    Backend._refresh_aws_batch_job!(retry_id)
                end
                @test retrying["status"] == "running"
                @test retrying["result_available"] == false
                @test retrying["progress"]["artifact_status"] == "retryable_error"
                @test !haskey(retrying, "finished_at")
                @test !haskey(retrying, "error")
                retried = Backend._refresh_aws_batch_job!(retry_id)
                @test retried["status"] == "succeeded"
                @test retried["result_available"] == true

                download_id = seed_aws_job()
                download_record = Backend._job_record(download_id)
                stage_committed_result(s3_root, download_record)
                download_retry = withenv("AWS_ARTIFACT_CP_MODE" => "error") do
                    Backend._refresh_aws_batch_job!(download_id)
                end
                @test download_retry["status"] == "running"
                @test download_retry["progress"]["artifact_status"] == "retryable_error"
                @test Backend._refresh_aws_batch_job!(download_id)["status"] == "succeeded"
            end

            @testset "manifest identity and result descriptor fail closed" begin
                function invalid_manifest_case(mutator, error_fragment)
                    job_id = seed_aws_job()
                    record = Backend._job_record(job_id)
                    manifest = stage_committed_result(s3_root, record)
                    mutator(manifest, record)
                    stage_manifest(s3_root, record, manifest)
                    failed = Backend._refresh_aws_batch_job!(job_id)
                    @test failed["status"] == "failed"
                    @test failed["result_available"] == false
                    @test failed["error_code"] == "aws_result_artifact_invalid"
                    @test occursin(error_fragment, lowercase(String(failed["error"])))
                end

                invalid_manifest_case((manifest, _) ->
                    (manifest["schema_version"] = "bne-job-result-manifest/v999"),
                    "schema version")
                invalid_manifest_case((manifest, _) ->
                    (manifest["job_id"] = "different-job"), "job identity")
                invalid_manifest_case((manifest, _) ->
                    (manifest["kind"] = "build_atlas"), "manifest kind")
                invalid_manifest_case((manifest, _) ->
                    (manifest["created_at"] = "2026-07-12T01:02:03"),
                    "created_at")
                invalid_manifest_case((manifest, _) ->
                    (manifest["created_at"] = "2026-02-30T01:02:03Z"),
                    "created_at")
                invalid_manifest_case((manifest, _) ->
                    (manifest["artifact_identity"]["config_hash"] = repeat("0", 64)),
                    "config identity")
                invalid_manifest_case((manifest, _) ->
                    (manifest["result"]["uri"] = "s3://artifact-bucket/wrong/result.json"),
                    "manifest uri")
                invalid_manifest_case((manifest, _) ->
                    (manifest["result"]["payload_key_count"] = 0),
                    "payload_key_count")
                invalid_manifest_case((manifest, _) ->
                    (manifest["result"]["content_length"] += 1),
                    "byte length")
                invalid_manifest_case((manifest, _) ->
                    (manifest["result"]["sha256"] = repeat("f", 64)),
                    "sha-256")
                invalid_manifest_case((manifest, _) ->
                    (manifest["unexpected"] = true), "fields must be exactly")

                missing_result_id = seed_aws_job()
                missing_result_record = Backend._job_record(missing_result_id)
                stage_committed_result(s3_root, missing_result_record)
                rm(s3_path(s3_root, String(missing_result_record["result_uri"])); force=true)
                missing_result = Backend._refresh_aws_batch_job!(missing_result_id)
                @test missing_result["status"] == "failed"
                @test missing_result["error_code"] == "aws_result_artifact_missing"
                @test occursin("missing result artifact", lowercase(String(missing_result["error"])))

                missing_sha_id = seed_aws_job()
                missing_sha_record = Backend._job_record(missing_sha_id)
                stage_committed_result(s3_root, missing_sha_record)
                rm("$(s3_path(s3_root, String(missing_sha_record["result_uri"]))).bne-sha256";
                   force=true)
                missing_sha = Backend._refresh_aws_batch_job!(missing_sha_id)
                @test missing_sha["status"] == "failed"
                @test occursin("sha-256 object metadata", lowercase(String(missing_sha["error"])))

                wrong_type_id = seed_aws_job()
                wrong_type_record = Backend._job_record(wrong_type_id)
                stage_committed_result(s3_root, wrong_type_record)
                wrong_type_path = s3_path(
                    s3_root, String(wrong_type_record["result_uri"]))
                write("$(wrong_type_path).content-type", "text/plain")
                wrong_type = Backend._refresh_aws_batch_job!(wrong_type_id)
                @test wrong_type["status"] == "failed"
                @test wrong_type["error_code"] == "aws_result_artifact_invalid"
                @test occursin("content type", lowercase(String(wrong_type["error"])))

                invalid_length_id = seed_aws_job()
                invalid_length_record = Backend._job_record(invalid_length_id)
                stage_committed_result(s3_root, invalid_length_record)
                invalid_length_path = s3_path(
                    s3_root, String(invalid_length_record["result_uri"]))
                write("$(invalid_length_path).head-content-length-json", "\"invalid\"")
                invalid_length_verification =
                    Backend._verify_job_result_artifact(invalid_length_record)
                @test invalid_length_verification.status == :retryable_error
                @test occursin(
                    "no valid contentlength",
                    lowercase(String(invalid_length_verification.error)),
                )
                invalid_length = Backend._refresh_aws_batch_job!(invalid_length_id)
                @test invalid_length["status"] == "running"
                @test invalid_length["result_available"] == false
                @test invalid_length["progress"]["artifact_status"] == "retryable_error"
                rm("$(invalid_length_path).head-content-length-json"; force=true)
                @test Backend._refresh_aws_batch_job!(invalid_length_id)["status"] ==
                    "succeeded"

                oversized_id = seed_aws_job()
                oversized_record = Backend._job_record(oversized_id)
                manifest_path = s3_path(
                    s3_root, String(oversized_record["result_manifest_uri"]))
                mkpath(dirname(manifest_path))
                write(manifest_path, repeat("x", Backend.JOB_RESULT_MANIFEST_MAX_BYTES + 1))
                write(aws_log, "")
                oversized = Backend._refresh_aws_batch_job!(oversized_id)
                @test oversized["status"] == "failed"
                @test occursin("byte limit", lowercase(String(oversized["error"])))
                @test !occursin("s3 cp $(oversized_record["result_manifest_uri"])",
                                read(aws_log, String))
            end

            @testset "nonempty JSON and result-artifact identity fail closed" begin
                function invalid_case(value, error_fragment)
                    job_id = seed_aws_job(manifest_protocol=false)
                    record = Backend._job_record(job_id)
                    stage_result(s3_root, record, value isa Function ? value(record) : value)
                    failed = Backend._refresh_aws_batch_job!(job_id)
                    @test failed["status"] == "failed"
                    @test failed["result_available"] == false
                    @test failed["error_code"] == "aws_result_artifact_invalid"
                    @test occursin(error_fragment, lowercase(String(failed["error"])))
                    return failed
                end

                invalid_case("   ", "empty")
                invalid_case(UInt8[0xff, 0xfe, 0xfd], "not valid utf-8")
                invalid_case("{not-json", "not valid json")
                invalid_case("[1,2,3]", "must be an object")
                invalid_case(Dict("ok" => true), "missing sibling")
                invalid_case(record -> Dict(
                    "artifact" => valid_result(record)["artifact"],
                ), "does not contain a computed result payload")
                invalid_case(record -> begin
                    result = valid_result(record)
                    result["artifact"]["artifact_schema_version"] = "bne-result/v999"
                    result
                end, "unsupported result artifact schema")
                invalid_case(record -> begin
                    result = valid_result(record)
                    result["artifact"]["kind"] = "build_atlas"
                    result
                end, "kind mismatch")
                invalid_case(record -> begin
                    result = valid_result(record)
                    result["artifact"]["algorithm"]["config_hash"] =
                        Backend._canonical_hash(Dict("different" => true))
                    result
                end, "config identity")
                invalid_case(record -> begin
                    result = valid_result(record)
                    result["artifact"]["algorithm"]["unexpected"] = true
                    result
                end, "unsupported field")
                invalid_case(record -> begin
                    result = valid_result(record)
                    result["artifact"]["input_hashes"]["network_ir_hashes"] = "not-an-array"
                    result
                end, "network_ir_hashes` must be an array")
            end

            @testset "result protocol compatibility is explicit" begin
                unknown_id = seed_aws_job()
                lock(Backend.JOBS_LOCK) do
                    record = Backend.JOBS[unknown_id]
                    candidate = deepcopy(record)
                    candidate["result_protocol_version"] =
                        "bne-job-result-manifest/v2.0.0"
                    Backend._commit_job_candidate_unlocked!(record, candidate)
                end
                unknown_verification = Backend._verify_job_result_artifact(
                    Backend._job_record(unknown_id),
                )
                @test unknown_verification.status == :retryable_error
                @test occursin(
                    "compatible verifier",
                    lowercase(String(unknown_verification.error)),
                )
                unknown = Backend._refresh_aws_batch_job!(unknown_id)
                @test unknown["status"] == "running"
                @test unknown["result_available"] == false
                @test unknown["progress"]["artifact_status"] == "retryable_error"
                @test unknown["progress"]["artifact_verification_mode"] == "manifest"
                @test !haskey(unknown, "finished_at")
                @test !haskey(unknown, "error")

                legacy_id = seed_aws_job(manifest_protocol=false)
                legacy_record = Backend._job_record(legacy_id)
                stage_result(s3_root, legacy_record, valid_result(legacy_record))
                write(aws_log, "")
                legacy = Backend._refresh_aws_batch_job!(legacy_id)
                @test legacy["status"] == "succeeded"
                @test legacy["result_available"] == true
                @test legacy["progress"]["artifact_status"] == "valid"
                @test legacy["progress"]["artifact_verification_mode"] ==
                    "legacy_inline"
                @test occursin(
                    "s3 cp $(legacy_record["result_uri"])",
                    read(aws_log, String),
                )
            end

            @testset "verification I/O does not hold the job registry lock" begin
                for block_on in ("head-object", "s3-cp")
                    job_id = seed_aws_job()
                    record = Backend._job_record(job_id)
                    stage_committed_result(s3_root, record)
                    started = joinpath(store_dir, "$(block_on)-started")
                    released = joinpath(store_dir, "$(block_on)-released")
                    operation = withenv(
                        "AWS_ARTIFACT_BLOCK_ON" => block_on,
                        "AWS_ARTIFACT_BLOCK_STARTED" => started,
                        "AWS_ARTIFACT_BLOCK_RELEASE" => released,
                    ) do
                        task = @async Backend._refresh_aws_batch_job!(job_id)
                        @test timedwait(() -> isfile(started), 2.0; pollint=0.01) == :ok
                        try
                            assert_job_lock_available()
                        finally
                            write(released, "released\n")
                        end
                        task
                    end
                    @test fetch(operation)["status"] == "succeeded"
                end
            end


            @testset "worker publishes result then manifest then succeeded status" begin
                job_id = "worker-publication-order"
                spec = Dict{String, Any}(
                    "library" => Backend.atlas_library_default(),
                    "query" => Dict("limit" => 1),
                )
                expected_hash = Backend._canonical_hash(spec)
                prefix = "s3://artifact-bucket/worker-order"
                status_uri = "$(prefix)/status.json"
                result_uri = "$(prefix)/result.json"
                manifest_uri = "$(prefix)/result-manifest.json"
                payload = Dict{String, Any}(
                    "job_id" => job_id,
                    "kind" => "query_atlas",
                    "executor" => "aws_batch",
                    "spec" => spec,
                    "result_protocol_version" => Backend.JOB_RESULT_PROTOCOL_VERSION,
                    "expected_artifact_config_hash" => expected_hash,
                    "artifacts" => Dict(
                        "status" => status_uri,
                        "result" => result_uri,
                        "result_manifest" => manifest_uri,
                    ),
                )
                started = joinpath(store_dir, "manifest-upload-started")
                released = joinpath(store_dir, "manifest-upload-released")
                unrelated_pending = joinpath(store_dir, "unrelated-local-pending")
                mkpath(unrelated_pending)
                Backend._mark_job_store_dir_fsync_pending!(unrelated_pending)
                write(aws_log, "")
                operation = withenv(
                    "AWS_ARTIFACT_BLOCK_ON" => "upload-manifest",
                    "AWS_ARTIFACT_BLOCK_STARTED" => started,
                    "AWS_ARTIFACT_BLOCK_RELEASE" => released,
                ) do
                    task = @async Backend.run_biocircuits_job_payload(payload)
                    @test timedwait(() -> isfile(started), 30.0; pollint=0.01) == :ok
                    @test isfile(s3_path(s3_root, result_uri))
                    @test !isfile(s3_path(s3_root, manifest_uri))
                    running = Backend._read_job_json(s3_path(s3_root, status_uri))
                    @test running["status"] == "running"
                    write(released, "released\n")
                    task
                end
                result = fetch(operation)
                @test result["artifact"]["kind"] == "query_atlas"
                result_path = s3_path(s3_root, result_uri)
                @test isfile(s3_path(s3_root, manifest_uri))
                @test read("$(result_path).content-type", String) ==
                    Backend.JOB_RESULT_MEDIA_TYPE
                succeeded = Backend._read_job_json(s3_path(s3_root, status_uri))
                @test succeeded["status"] == "succeeded"

                log_lines = split(read(aws_log, String), '\n'; keepempty=false)
                result_upload = findfirst(line ->
                    startswith(line, "s3 cp ") && occursin(result_uri, line), log_lines)
                manifest_upload = findfirst(line ->
                    startswith(line, "s3 cp ") && occursin(manifest_uri, line), log_lines)
                succeeded_uploads = findall(line ->
                    startswith(line, "s3 cp ") && occursin(status_uri, line), log_lines)
                @test result_upload !== nothing
                @test manifest_upload !== nothing
                @test length(succeeded_uploads) == 2
                @test result_upload < manifest_upload < last(succeeded_uploads)
                @test Backend._pending_job_store_dir_generation(
                    unrelated_pending,
                ) !== nothing
                lock(Backend.JOB_STORE_DURABILITY_LOCK) do
                    delete!(
                        Backend.JOB_STORE_PENDING_DIR_FSYNC,
                        abspath(unrelated_pending),
                    )
                end
            end
        end
    end
end

end # module JobsArtifactValidityContract
