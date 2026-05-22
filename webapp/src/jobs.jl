const JOBS = Dict{String, Dict{String, Any}}()
const JOBS_LOCK = ReentrantLock()
const JOB_TASKS = Dict{String, Task}()
const JOB_DESCRIBE_LAST_AT = Dict{String, Float64}()
const LOCAL_JOB_STORE_DIR = Ref{Union{Nothing, String}}(nothing)

function _aws_batch_describe_min_interval()
    raw = strip(get(ENV, "BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL", ""))
    isempty(raw) && return 3.0
    parsed = tryparse(Float64, raw)
    return parsed === nothing ? 3.0 : max(parsed, 0.0)
end

const JOB_STATUSES = Set([
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancel_requested",
    "cancelled",
])

const LOCAL_JOB_KINDS = Set([
    "build_atlas",
    "build_atlas_library",
    "merge_atlas_library",
    "query_atlas",
    "run_inverse_design",
])

struct QuotaExceeded <: Exception
    msg::String
end
Base.showerror(io::IO, e::QuotaExceeded) = print(io, "QuotaExceeded: ", e.msg)

const ANONYMOUS_USER_SUB = "anonymous"
# Cognito subs are UUIDs but we keep the allow set generous so other IdPs
# (Auth0, internal admin tokens, "anonymous") also pass cleanly. We reject any
# character that could let a hostile principal escape the S3 prefix.
const _USER_SUB_ALLOWED = r"^[A-Za-z0-9_\-.:@]{1,128}$"

function _sanitize_user_sub(raw)
    raw === nothing && return ANONYMOUS_USER_SUB
    text = strip(String(raw))
    isempty(text) && return ANONYMOUS_USER_SUB
    occursin(_USER_SUB_ALLOWED, text) || throw(ArgumentError("Invalid user_sub: must match $(_USER_SUB_ALLOWED.pattern)"))
    return text
end

# Tags propagated to AWS Batch jobs / S3 objects must be limited to the
# Cost Allocation tag character set: letters, digits, spaces, and _.:/=+-@
_sanitize_tag_value(raw) = replace(String(raw), r"[^A-Za-z0-9_.:/=+\-@]" => "_")

# DynamoDB-backed per-user submission quota. Off by default; activate by
# setting BIOCIRCUITS_EXPLORER_QUOTA_TABLE to the table name. The table must
# have HASH key `user_sub` (String) and RANGE key `window` (String). On each
# submit we conditionally bump a counter for (sub, today). If the counter
# would exceed BIOCIRCUITS_EXPLORER_QUOTA_DAILY_LIMIT (default 50) we reject.
function _quota_table_name()
    return strip(get(ENV, "BIOCIRCUITS_EXPLORER_QUOTA_TABLE", ""))
end

function _quota_daily_limit()
    raw = strip(get(ENV, "BIOCIRCUITS_EXPLORER_QUOTA_DAILY_LIMIT", ""))
    isempty(raw) && return 50
    parsed = tryparse(Int, raw)
    return parsed === nothing ? 50 : max(parsed, 0)
end

function _quota_today_window(now_epoch::Real=time())
    return Dates.format(Dates.unix2datetime(Float64(now_epoch)), dateformat"yyyymmdd")
end

function _quota_expires_at(now_epoch::Real=time())
    # Window TTL: 48h after the start of the day. DynamoDB TTL expects epoch
    # seconds in a Number attribute. We don't trim sub-second precision since
    # DynamoDB tolerates that.
    return Int(floor(Float64(now_epoch))) + 2 * 24 * 3600
end

# Returns true if quota accepted, false if rejected. Any AWS CLI error
# propagates as a thrown ArgumentError so submit fails closed.
function _check_and_consume_quota!(user_sub::AbstractString;
                                   table::AbstractString=_quota_table_name(),
                                   daily_limit::Integer=_quota_daily_limit(),
                                   now_epoch::Real=time())
    isempty(table) && return true  # quota disabled
    user_sub == ANONYMOUS_USER_SUB && return true  # don't account for unauth dev traffic
    window = _quota_today_window(now_epoch)
    expires_at = string(_quota_expires_at(now_epoch))
    expression_values = Dict(
        ":one" => Dict("N" => "1"),
        ":limit" => Dict("N" => string(daily_limit)),
        ":exp" => Dict("N" => expires_at),
    )
    try
        run(pipeline(
            Cmd([
                _aws_cli(), "dynamodb", "update-item",
                "--table-name", String(table),
                "--key", JSON3.write(Dict(
                    "user_sub" => Dict("S" => String(user_sub)),
                    "window" => Dict("S" => window),
                )),
                "--update-expression", "ADD submitted :one SET expires_at = if_not_exists(expires_at, :exp)",
                "--condition-expression", "attribute_not_exists(submitted) OR submitted < :limit",
                "--expression-attribute-values", JSON3.write(expression_values),
            ]);
            stdout=devnull, stderr=devnull,
        ))
        return true
    catch
        return false
    end
end

function local_job_store_dir()
    if LOCAL_JOB_STORE_DIR[] === nothing
        configured = strip(get(ENV, "BIOCIRCUITS_EXPLORER_JOB_STORE", ""))
        LOCAL_JOB_STORE_DIR[] = isempty(configured) ?
            normpath(joinpath(@__DIR__, "..", "job_store")) :
            abspath(configured)
    end
    return LOCAL_JOB_STORE_DIR[]
end

function _job_dir(job_id::AbstractString)
    return joinpath(local_job_store_dir(), String(job_id))
end

function _job_input_path(job_id::AbstractString)
    return joinpath(_job_dir(job_id), "input.json")
end

function _job_status_path(job_id::AbstractString)
    return joinpath(_job_dir(job_id), "status.json")
end

function _job_record_path(job_id::AbstractString)
    return joinpath(_job_dir(job_id), "record.json")
end

function _job_result_path(job_id::AbstractString)
    return joinpath(_job_dir(job_id), "result.json")
end

_is_s3_uri(uri::AbstractString) = startswith(lowercase(String(uri)), "s3://")
_is_file_uri(uri::AbstractString) = startswith(lowercase(String(uri)), "file://")
_uri_local_path(uri::AbstractString) = _is_file_uri(uri) ? String(uri)[8:end] : String(uri)
_aws_cli() = strip(get(ENV, "BIOCIRCUITS_EXPLORER_AWS_CLI", "aws"))

function _write_job_json(path::AbstractString, payload)
    mkpath(dirname(path))
    open(path, "w") do io
        JSON3.pretty(io, json_safe_value(payload))
        write(io, "\n")
    end
    return path
end

function _read_job_json(path::AbstractString)
    isfile(path) || throw(ArgumentError("Missing job artifact: $(path)"))
    return _materialize(JSON3.read(read(path, String)))
end

function _write_json_uri(uri::AbstractString, payload)
    if _is_s3_uri(uri)
        temp_path, temp_io = mktemp()
        close(temp_io)
        try
            _write_job_json(temp_path, payload)
            run(Cmd([_aws_cli(), "s3", "cp", temp_path, String(uri)]))
        finally
            isfile(temp_path) && rm(temp_path; force=true)
        end
        return String(uri)
    else
        return _write_job_json(_uri_local_path(uri), payload)
    end
end

function _read_json_uri(uri::AbstractString)
    if _is_s3_uri(uri)
        temp_path, temp_io = mktemp()
        close(temp_io)
        try
            run(Cmd([_aws_cli(), "s3", "cp", String(uri), temp_path]))
            return _read_job_json(temp_path)
        finally
            isfile(temp_path) && rm(temp_path; force=true)
        end
    else
        return _read_job_json(_uri_local_path(uri))
    end
end

function _job_public_record(record::AbstractDict)
    job_id = String(record["job_id"])
    out = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => String(record["kind"]),
        "status" => String(record["status"]),
        "executor" => String(record["executor"]),
        "user_sub" => String(get(record, "user_sub", ANONYMOUS_USER_SUB)),
        "created_at" => record["created_at"],
        "updated_at" => record["updated_at"],
        "result_available" => Bool(get(record, "result_available", false)),
        "artifacts" => Dict{String, Any}(),
    )

    haskey(record, "batch_job_id") && (out["external_job_id"] = record["batch_job_id"])
    haskey(record, "log_stream_name") && (out["log_stream_name"] = record["log_stream_name"])

    for key in ("started_at", "finished_at", "progress", "error", "cancel_requested_at")
        haskey(record, key) && (out[key] = record[key])
    end

    out["artifacts"]["input"] = "job://$(job_id)/input"
    out["artifacts"]["status"] = "job://$(job_id)/status"
    if Bool(get(record, "result_available", false))
        out["result_ref"] = "job://$(job_id)/result"
        out["artifacts"]["result"] = out["result_ref"]
    end

    return out
end

function _persist_job_record_unlocked(record::AbstractDict)
    _write_job_json(String(record["record_path"]), record)
    return nothing
end

function _persist_job_status_unlocked(record::AbstractDict)
    status = _job_public_record(record)
    _write_job_json(String(record["status_path"]), status)
    # For AWS Batch jobs the worker container is the sole writer of the
    # remote status.json artifact, so the host backend never publishes to
    # the S3 status URI — this avoids a last-writer-wins race between the
    # describe-jobs poller and the worker's progress updates.
    if String(get(record, "executor", "")) != "aws_batch"
        status_uri = get(record, "status_uri", record["status_path"])
        if String(status_uri) != String(record["status_path"])
            _write_json_uri(String(status_uri), status)
        end
    end
    return nothing
end

function _job_update!(job_id::AbstractString; updates...)
    lock(JOBS_LOCK) do
        record = get(JOBS, String(job_id), nothing)
        if record === nothing
            path = _job_record_path(job_id)
            isfile(path) || return nothing
            record = _read_job_json(path)
            JOBS[String(job_id)] = record
        end
        for (key, value) in updates
            record[String(key)] = value
        end
        record["updated_at"] = _now_iso_timestamp()
        _persist_job_record_unlocked(record)
        _persist_job_status_unlocked(record)
        return record
    end
end

function _job_record(job_id::AbstractString)
    lock(JOBS_LOCK) do
        record = get(JOBS, String(job_id), nothing)
        if record === nothing
            path = _job_record_path(job_id)
            isfile(path) || return nothing
            record = _read_job_json(path)
            JOBS[String(job_id)] = record
        end
        return copy(record)
    end
end

function _load_job_status_from_disk(job_id::AbstractString)
    path = _job_status_path(job_id)
    isfile(path) || return nothing
    return _read_job_json(path)
end

function _check_user_owns_record(record, requesting_user_sub::AbstractString, job_id::AbstractString)
    requesting_user_sub = _sanitize_user_sub(requesting_user_sub)
    owner = String(get(record, "user_sub", ANONYMOUS_USER_SUB))
    # Mirror the "404 not 403" convention: do not confirm the existence of
    # jobs owned by other users to a probing client.
    if owner != requesting_user_sub
        throw(ArgumentError("Unknown job_id: $(job_id)"))
    end
    return nothing
end

function get_biocircuits_job(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    record = _job_record(job_id)
    if record !== nothing
        _check_user_owns_record(record, user_sub, job_id)
        if String(get(record, "executor", "")) == "aws_batch"
            refreshed = _refresh_aws_batch_job!(job_id)
            refreshed !== nothing && return _job_public_record(refreshed)
        end
        return _job_public_record(record)
    end

    status = _load_job_status_from_disk(job_id)
    status === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
    _check_user_owns_record(status, user_sub, job_id)
    return status
end

function get_biocircuits_job_result(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    status = get_biocircuits_job(job_id; user_sub=user_sub)
    String(status["status"]) == "succeeded" || throw(ArgumentError("Job $(job_id) has not succeeded."))
    record = _job_record(job_id)
    record === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
    _check_user_owns_record(record, user_sub, job_id)
    result_uri = String(get(record, "result_uri", _job_result_path(job_id)))
    return Dict(
        "job" => status,
        "result" => _read_json_uri(result_uri),
    )
end

function _presign_s3_get(uri::AbstractString; expires_in::Integer=3600)
    _is_s3_uri(uri) || throw(ArgumentError("Cannot presign non-S3 URI: $(uri)"))
    output = read(Cmd([_aws_cli(), "s3", "presign", String(uri), "--expires-in", string(expires_in)]), String)
    return strip(output)
end

# Returns a short-lived pre-signed GET URL for result.json so clients can
# fetch large results directly from S3 instead of round-tripping through the
# broker. Only available for AWS Batch jobs (which store result.json in S3).
function get_biocircuits_job_result_url(job_id::AbstractString;
                                        user_sub::AbstractString=ANONYMOUS_USER_SUB,
                                        expires_in::Integer=3600)
    status = get_biocircuits_job(job_id; user_sub=user_sub)
    String(status["status"]) == "succeeded" || throw(ArgumentError("Job $(job_id) has not succeeded."))
    record = _job_record(job_id)
    record === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
    _check_user_owns_record(record, user_sub, job_id)
    result_uri = String(get(record, "result_uri", ""))
    _is_s3_uri(result_uri) ||
        throw(ArgumentError("Pre-signed URLs are only available for AWS Batch jobs."))
    expires_at = _now_iso_timestamp_after(expires_in)
    return Dict{String, Any}(
        "job_id" => String(job_id),
        "result_url" => _presign_s3_get(result_uri; expires_in=expires_in),
        "expires_at" => expires_at,
        "expires_in" => Int(expires_in),
    )
end

function _now_iso_timestamp_after(seconds::Integer)
    return Dates.format(Dates.now(Dates.UTC) + Dates.Second(seconds), dateformat"yyyy-mm-ddTHH:MM:SSZ")
end

function _execute_local_job(kind::AbstractString, spec)
    if kind == "build_atlas"
        return build_behavior_atlas_from_spec(spec)
    elseif kind == "build_atlas_library"
        return build_atlas_library_from_spec(spec)
    elseif kind == "merge_atlas_library"
        return merge_atlas_library_from_spec(spec)
    elseif kind == "query_atlas"
        return query_behavior_atlas_from_spec(spec)
    elseif kind == "run_inverse_design"
        return run_inverse_design_from_spec(spec)
    else
        throw(ArgumentError("Unsupported local job kind: $(kind)"))
    end
end

function _job_status_payload(job_id::AbstractString, kind::AbstractString, executor::AbstractString, status::AbstractString; kwargs...)
    now = _now_iso_timestamp()
    payload = Dict{String, Any}(
        "job_id" => String(job_id),
        "kind" => String(kind),
        "executor" => String(executor),
        "status" => String(status),
        "updated_at" => now,
        "result_available" => Bool(get(Dict(kwargs), :result_available, false)),
        "artifacts" => Dict(
            "input" => "job://$(job_id)/input",
            "status" => "job://$(job_id)/status",
        ),
    )
    for (key, value) in kwargs
        key === :result_available && continue
        payload[String(key)] = value
    end
    if Bool(payload["result_available"])
        payload["result_ref"] = "job://$(job_id)/result"
        payload["artifacts"]["result"] = payload["result_ref"]
    end
    return payload
end

function run_biocircuits_job_payload(payload; status_uri=nothing, result_uri=nothing)
    payload = _materialize(payload)
    job_id = String(_raw_get(payload, :job_id, string(rand(UInt128), base=16, pad=32)))
    kind = String(_raw_get(payload, :kind, ""))
    kind in LOCAL_JOB_KINDS || throw(ArgumentError("Unsupported job kind: $(kind)"))
    executor = String(_raw_get(payload, :executor, "worker"))
    spec = _materialize(_raw_get(payload, :spec, Dict{String, Any}()))
    artifacts = _raw_get(payload, :artifacts, Dict{String, Any}())
    status_uri = status_uri === nothing ? _raw_get(artifacts, :status, nothing) : status_uri
    result_uri = result_uri === nothing ? _raw_get(artifacts, :result, nothing) : result_uri
    status_uri === nothing && throw(ArgumentError("Batch job payload must include a status artifact URI."))
    result_uri === nothing && throw(ArgumentError("Batch job payload must include a result artifact URI."))

    _write_json_uri(String(status_uri), _job_status_payload(
        job_id,
        kind,
        executor,
        "running";
        started_at=_now_iso_timestamp(),
        progress=Dict("message" => "Running in worker"),
    ))

    try
        result = _execute_local_job(kind, spec)
        _write_json_uri(String(result_uri), result)
        _write_json_uri(String(status_uri), _job_status_payload(
            job_id,
            kind,
            executor,
            "succeeded";
            finished_at=_now_iso_timestamp(),
            result_available=true,
            progress=Dict("message" => "Completed"),
        ))
        return result
    catch err
        _write_json_uri(String(status_uri), _job_status_payload(
            job_id,
            kind,
            executor,
            "failed";
            finished_at=_now_iso_timestamp(),
            result_available=false,
            error=sprint(showerror, err, catch_backtrace()),
            progress=Dict("message" => "Failed"),
        ))
        rethrow()
    end
end

function run_biocircuits_job_from_uri(input_uri::AbstractString; status_uri=nothing, result_uri=nothing)
    payload = _read_json_uri(input_uri)
    return run_biocircuits_job_payload(payload; status_uri=status_uri, result_uri=result_uri)
end

function _run_local_job!(job_id::String, kind::String, spec)
    initial = _job_record(job_id)
    if initial !== nothing && String(get(initial, "status", "")) == "cancelled"
        lock(JOBS_LOCK) do
            delete!(JOB_TASKS, job_id)
        end
        return nothing
    end

    _job_update!(job_id;
        status="running",
        started_at=_now_iso_timestamp(),
        progress=Dict("message" => "Running locally"),
    )

    try
        result = _execute_local_job(kind, spec)
        record = _job_record(job_id)
        result_uri = record === nothing ? _job_result_path(job_id) : String(get(record, "result_uri", _job_result_path(job_id)))
        _write_json_uri(result_uri, result)

        latest = _job_record(job_id)
        if latest !== nothing && String(get(latest, "status", "")) == "cancel_requested"
            _job_update!(job_id;
                status="cancelled",
                finished_at=_now_iso_timestamp(),
                result_available=false,
                progress=Dict("message" => "Cancelled after local execution completed"),
            )
        else
            _job_update!(job_id;
                status="succeeded",
                finished_at=_now_iso_timestamp(),
                result_available=true,
                progress=Dict("message" => "Completed"),
            )
        end
    catch err
        if err isa InterruptException
            _job_update!(job_id;
                status="cancelled",
                finished_at=_now_iso_timestamp(),
                result_available=false,
                progress=Dict("message" => "Cancelled"),
            )
        else
            _job_update!(job_id;
                status="failed",
                finished_at=_now_iso_timestamp(),
                result_available=false,
                error=sprint(showerror, err, catch_backtrace()),
                progress=Dict("message" => "Failed"),
            )
        end
    finally
        lock(JOBS_LOCK) do
            delete!(JOB_TASKS, job_id)
        end
    end

    return nothing
end

function _job_execution_mode(raw)
    execution = _raw_get(raw, :execution, Dict{String, Any}())
    return lowercase(String(_raw_get(execution, :mode, "local_async")))
end

function _aws_cli_json(args::Vector{<:AbstractString})
    output = read(Cmd([_aws_cli(); String.(args)]), String)
    return _materialize(JSON3.read(output))
end

function _s3_uri_bucket_key(uri::AbstractString)
    text = String(uri)
    _is_s3_uri(text) || return nothing
    body = text[6:end]
    slash = findfirst('/', body)
    slash === nothing && return nothing
    bucket = body[1:slash - 1]
    key = body[slash + 1:end]
    (isempty(bucket) || isempty(key)) && return nothing
    return (bucket, key)
end

function _artifact_exists(uri::AbstractString)
    isempty(strip(String(uri))) && return false
    if !_is_s3_uri(uri)
        return isfile(_uri_local_path(uri))
    end
    parsed = _s3_uri_bucket_key(uri)
    parsed === nothing && return false
    bucket, key = parsed
    try
        run(pipeline(
            Cmd([_aws_cli(), "s3api", "head-object", "--bucket", String(bucket), "--key", String(key)]);
            stdout=devnull, stderr=devnull,
        ))
        return true
    catch
        return false
    end
end

function _required_config(value, name::AbstractString)
    value === nothing && throw(ArgumentError("Missing required AWS Batch config: $(name)."))
    text = strip(String(value))
    isempty(text) && throw(ArgumentError("Missing required AWS Batch config: $(name)."))
    return String(text)
end

function _allow_aws_batch_request_config()
    value = lowercase(strip(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_AWS_BATCH_REQUEST_CONFIG", "")))
    return value in ("1", "true", "yes", "on")
end

function _aws_batch_config_value(execution, key::Symbol, env_name::AbstractString)
    if _allow_aws_batch_request_config() && _raw_haskey(execution, key)
        return _raw_get(execution, key, nothing)
    end
    return get(ENV, env_name, "")
end

function _aws_batch_artifact_uri(prefix::AbstractString, user_sub::AbstractString, job_id::AbstractString, filename::AbstractString)
    cleaned = replace(String(prefix), r"/+$" => "")
    return "$(cleaned)/users/$(user_sub)/jobs/$(job_id)/$(filename)"
end

function _aws_batch_container_overrides(input_uri::AbstractString, status_uri::AbstractString, result_uri::AbstractString, execution)
    command = [
        "julia",
        "-t",
        "auto",
        "--project=webapp",
        "webapp/scripts/run_batch_job.jl",
        "--input-uri",
        String(input_uri),
        "--status-uri",
        String(status_uri),
        "--result-uri",
        String(result_uri),
    ]
    overrides = Dict{String, Any}(
        "command" => command,
    )

    environment = Dict{String, String}[]
    if _allow_aws_batch_request_config() && _raw_haskey(execution, :environment)
        for (key, value) in pairs(_raw_get(execution, :environment, Dict{String, Any}()))
            push!(environment, Dict("name" => String(key), "value" => String(value)))
        end
    end
    isempty(environment) || (overrides["environment"] = environment)

    resources = Dict{String, String}[]
    if _raw_haskey(execution, :vcpus)
        push!(resources, Dict("type" => "VCPU", "value" => string(_raw_get(execution, :vcpus, ""))))
    end
    if _raw_haskey(execution, :memory_mib)
        push!(resources, Dict("type" => "MEMORY", "value" => string(_raw_get(execution, :memory_mib, ""))))
    end
    isempty(resources) || (overrides["resourceRequirements"] = resources)

    return JSON3.write(overrides)
end

function _aws_batch_status_to_job_status(status::AbstractString)
    aws_status = uppercase(String(status))
    if aws_status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING")
        return "queued"
    elseif aws_status == "RUNNING"
        return "running"
    elseif aws_status == "SUCCEEDED"
        return "succeeded"
    elseif aws_status == "FAILED"
        return "failed"
    else
        return "queued"
    end
end

function _aws_batch_submit!(record::AbstractDict, execution)
    job_id = String(record["job_id"])
    user_sub = String(get(record, "user_sub", ANONYMOUS_USER_SUB))
    kind = String(record["kind"])
    job_queue = _required_config(
        _aws_batch_config_value(execution, :job_queue, "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE"),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE",
    )
    job_definition = _required_config(
        _aws_batch_config_value(execution, :job_definition, "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION"),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION",
    )
    artifact_prefix = _required_config(
        _aws_batch_config_value(execution, :artifact_prefix, "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX"),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX",
    )

    input_uri = _aws_batch_artifact_uri(artifact_prefix, user_sub, job_id, "input.json")
    status_uri = _aws_batch_artifact_uri(artifact_prefix, user_sub, job_id, "status.json")
    result_uri = _aws_batch_artifact_uri(artifact_prefix, user_sub, job_id, "result.json")
    record["input_uri"] = input_uri
    record["status_uri"] = status_uri
    record["result_uri"] = result_uri

    payload = Dict(
        "job_id" => job_id,
        "kind" => kind,
        "executor" => "aws_batch",
        "user_sub" => user_sub,
        "spec" => record["spec"],
        "artifacts" => Dict(
            "input" => input_uri,
            "status" => status_uri,
            "result" => result_uri,
        ),
    )
    _write_json_uri(input_uri, payload)
    _persist_job_record_unlocked(record)
    _persist_job_status_unlocked(record)

    job_name_prefix = strip(String(_raw_get(execution, :job_name_prefix, get(ENV, "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX", "biocircuits"))))
    short_job_id = job_id[1:min(lastindex(job_id), 24)]
    job_name = "$(isempty(job_name_prefix) ? "biocircuits" : job_name_prefix)-$(short_job_id)"
    container_overrides = _aws_batch_container_overrides(input_uri, status_uri, result_uri, execution)
    tag_value = "User=$(_sanitize_tag_value(user_sub)),JobKind=$(_sanitize_tag_value(kind))"
    response = _aws_cli_json([
        "batch",
        "submit-job",
        "--job-name",
        job_name,
        "--job-queue",
        job_queue,
        "--job-definition",
        job_definition,
        "--container-overrides",
        container_overrides,
        "--tags",
        tag_value,
        "--propagate-tags",
    ])

    record["batch_job_id"] = String(_raw_get(response, :jobId, ""))
    record["batch_job_name"] = String(_raw_get(response, :jobName, job_name))
    record["progress"] = Dict(
        "message" => "Submitted to AWS Batch",
        "aws_status" => "SUBMITTED",
    )
    _persist_job_record_unlocked(record)
    _persist_job_status_unlocked(record)
    return record
end

function _refresh_aws_batch_job!(job_id::AbstractString)
    lock(JOBS_LOCK) do
        record = get(JOBS, String(job_id), nothing)
        record === nothing && return nothing
        String(get(record, "executor", "")) == "aws_batch" || return record
        haskey(record, "batch_job_id") || return record
        String(get(record, "status", "")) in ("succeeded", "failed", "cancelled") && return record

        now_epoch = time()
        last_at = get(JOB_DESCRIBE_LAST_AT, String(job_id), 0.0)
        if now_epoch - last_at < _aws_batch_describe_min_interval()
            return record
        end
        JOB_DESCRIBE_LAST_AT[String(job_id)] = now_epoch

        response = _aws_cli_json(["batch", "describe-jobs", "--jobs", String(record["batch_job_id"])])
        jobs = collect(_raw_get(response, :jobs, Any[]))
        isempty(jobs) && return record
        aws_job = jobs[1]
        aws_status = String(_raw_get(aws_job, :status, "UNKNOWN"))
        status = _aws_batch_status_to_job_status(aws_status)

        container = _raw_get(aws_job, :container, Dict{String, Any}())
        log_stream = String(_raw_get(container, :logStreamName, ""))
        if !isempty(log_stream)
            record["log_stream_name"] = log_stream
        end

        # AWS Batch SUCCEEDED only confirms that the container exited 0. If the
        # worker failed to upload result.json (e.g. transient S3 error) we must
        # not flip the record to succeeded, or the next GET /result will 500.
        if status == "succeeded"
            result_uri = String(get(record, "result_uri", ""))
            if !isempty(result_uri) && !_artifact_exists(result_uri)
                status = "failed"
                record["error"] = "AWS Batch job exited successfully but result artifact is missing: $(result_uri)"
            end
        end

        record["status"] = status
        record["updated_at"] = _now_iso_timestamp()
        record["result_available"] = status == "succeeded"
        record["progress"] = Dict(
            "message" => "AWS Batch status: $(aws_status)",
            "aws_status" => aws_status,
        )

        if status == "running" && !haskey(record, "started_at")
            record["started_at"] = record["updated_at"]
        end
        if status in ("succeeded", "failed")
            record["finished_at"] = record["updated_at"]
        end
        if status == "failed" && !haskey(record, "error")
            reason = String(_raw_get(aws_job, :statusReason, _raw_get(container, :reason, "AWS Batch job failed")))
            record["error"] = reason
        end

        _persist_job_record_unlocked(record)
        _persist_job_status_unlocked(record)
        return record
    end
end

function _cancel_aws_batch_job!(record::AbstractDict)
    haskey(record, "batch_job_id") || throw(ArgumentError("AWS Batch job has not been submitted yet."))
    batch_job_id = String(record["batch_job_id"])
    status = String(get(record, "status", "queued"))
    try
        if status == "running"
            run(Cmd([_aws_cli(), "batch", "terminate-job", "--job-id", batch_job_id, "--reason", "Cancelled by Biocircuits Explorer user"]))
        else
            run(Cmd([_aws_cli(), "batch", "cancel-job", "--job-id", batch_job_id, "--reason", "Cancelled by Biocircuits Explorer user"]))
        end
    catch err
        throw(ArgumentError("Failed to cancel AWS Batch job $(batch_job_id): $(sprint(showerror, err))"))
    end
    return nothing
end

function submit_biocircuits_job_from_spec(raw; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    _raw_haskey(raw, :kind) || throw(ArgumentError("Job request must include `kind`."))
    _raw_haskey(raw, :spec) || throw(ArgumentError("Job request must include `spec`."))

    kind = String(_raw_get(raw, :kind, ""))
    kind in LOCAL_JOB_KINDS || throw(ArgumentError("Unsupported job kind: $(kind)"))

    execution = _raw_get(raw, :execution, Dict{String, Any}())
    mode = _job_execution_mode(raw)
    if !(mode in ("local", "local_async", "aws_batch", "batch"))
        throw(ArgumentError("Unsupported job execution mode: $(mode)"))
    end

    user_sub = _sanitize_user_sub(user_sub)
    if !_check_and_consume_quota!(user_sub)
        throw(QuotaExceeded("Daily submission quota exceeded for user $(user_sub). Limit: $(_quota_daily_limit())"))
    end
    job_id = string(rand(UInt128), base=16, pad=32)
    now = _now_iso_timestamp()
    spec = _materialize(_raw_get(raw, :spec, Dict{String, Any}()))
    executor = mode in ("aws_batch", "batch") ? "aws_batch" : "local_async"

    record = Dict{String, Any}(
        "job_id" => job_id,
        "kind" => kind,
        "status" => "queued",
        "executor" => executor,
        "user_sub" => user_sub,
        "created_at" => now,
        "updated_at" => now,
        "result_available" => false,
        "progress" => Dict("message" => "Queued"),
        "spec" => spec,
        "input_path" => _job_input_path(job_id),
        "status_path" => _job_status_path(job_id),
        "record_path" => _job_record_path(job_id),
        "result_path" => _job_result_path(job_id),
        "input_uri" => _job_input_path(job_id),
        "status_uri" => _job_status_path(job_id),
        "result_uri" => _job_result_path(job_id),
    )

    _write_json_uri(record["input_uri"], Dict(
        "job_id" => job_id,
        "kind" => kind,
        "executor" => record["executor"],
        "user_sub" => user_sub,
        "spec" => spec,
        "artifacts" => Dict(
            "input" => record["input_uri"],
            "status" => record["status_uri"],
            "result" => record["result_uri"],
        ),
    ))

    lock(JOBS_LOCK) do
        JOBS[job_id] = record
        _persist_job_record_unlocked(record)
        _persist_job_status_unlocked(record)
    end

    if executor == "aws_batch"
        lock(JOBS_LOCK) do
            record = JOBS[job_id]
            try
                _aws_batch_submit!(record, execution)
            catch err
                record["status"] = "failed"
                record["updated_at"] = _now_iso_timestamp()
                record["finished_at"] = record["updated_at"]
                record["result_available"] = false
                record["error"] = sprint(showerror, err, catch_backtrace())
                record["progress"] = Dict("message" => "AWS Batch submission failed")
                _persist_job_record_unlocked(record)
                _persist_job_status_unlocked(record)
                rethrow()
            end
        end
    else
        task = Threads.@spawn _run_local_job!(job_id, kind, spec)
        lock(JOBS_LOCK) do
            JOB_TASKS[job_id] = task
        end
    end

    return get_biocircuits_job(job_id; user_sub=user_sub)
end

function cancel_biocircuits_job(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    job_id = String(job_id)
    lock(JOBS_LOCK) do
        record = get(JOBS, job_id, nothing)
        if record === nothing
            path = _job_record_path(job_id)
            if isfile(path)
                record = _read_job_json(path)
                JOBS[job_id] = record
            end
        end
        record === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
        _check_user_owns_record(record, user_sub, job_id)
        status = String(record["status"])
        status in ("succeeded", "failed", "cancelled") && return _job_public_record(record)

        if String(get(record, "executor", "")) == "aws_batch"
            _cancel_aws_batch_job!(record)
        end

        record["status"] = status == "queued" ? "cancelled" : "cancel_requested"
        record["cancel_requested_at"] = _now_iso_timestamp()
        record["updated_at"] = _now_iso_timestamp()
        record["progress"] = Dict("message" => status == "queued" ? "Cancelled" : "Cancel requested")
        _persist_job_record_unlocked(record)
        _persist_job_status_unlocked(record)

        task = get(JOB_TASKS, job_id, nothing)
        if task !== nothing && !istaskdone(task)
            try
                Base.throwto(task, InterruptException())
            catch
            end
        end

        delete!(JOB_DESCRIBE_LAST_AT, job_id)
        return _job_public_record(record)
    end
end

function _request_header(req, name::AbstractString)
    headers = req.headers
    headers === nothing && return nothing
    target = lowercase(String(name))
    for header in headers
        lowercase(String(first(header))) == target || continue
        return String(last(header))
    end
    return nothing
end

function _bearer_token_from_request(req)
    raw = _request_header(req, "authorization")
    raw === nothing && return nothing
    text = strip(raw)
    startswith(lowercase(text), "bearer ") || return nothing
    return strip(text[8:end])
end

function _cognito_user_pool_id()
    return strip(get(ENV, "BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID", ""))
end

function _request_user_sub(req)
    # Two modes:
    #  - Production (Cognito configured): require Authorization: Bearer <JWT>
    #    and verify the RS256 signature against the user pool's JWKs. No
    #    fallback to X-User-Sub so a hostile client cannot spoof identity.
    #  - Dev / test (no Cognito): trust the X-User-Sub header; falls back to
    #    "anonymous" when neither is present.
    if !isempty(_cognito_user_pool_id())
        token = _bearer_token_from_request(req)
        token === nothing && throw(ArgumentError("Missing Authorization Bearer token"))
        claims = verify_cognito_jwt(token)
        return _sanitize_user_sub(claims["sub"])
    end
    raw = _request_header(req, "x-user-sub")
    raw === nothing && return ANONYMOUS_USER_SUB
    return _sanitize_user_sub(raw)
end

function handle_jobs_route(req, path::AbstractString)
    parts = split(strip(String(path), '/'), '/')
    user_sub = _request_user_sub(req)

    if parts == ["api", "jobs"]
        req.method == "POST" || return error_response("Method not allowed"; status=405)
        return json_response(submit_biocircuits_job_from_spec(read_json(req); user_sub=user_sub); status=202)
    end

    if length(parts) == 3 && parts[1] == "api" && parts[2] == "jobs"
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return json_response(get_biocircuits_job(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "result"
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return json_response(get_biocircuits_job_result(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "result-url"
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return json_response(get_biocircuits_job_result_url(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "cancel"
        req.method == "POST" || return error_response("Method not allowed"; status=405)
        return json_response(cancel_biocircuits_job(parts[3]; user_sub=user_sub))
    end

    return error_response("Unknown jobs route"; status=404)
end
