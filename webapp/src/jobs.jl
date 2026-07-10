const JOBS = Dict{String, Dict{String, Any}}()
const JOBS_LOCK = ReentrantLock()
const JOB_TASKS = Dict{String, Task}()
const LOCAL_JOB_CANCEL_TOKENS = Dict{String, LocalJobCancelToken}()
const JOB_DESCRIBE_LAST_AT = Dict{String, Float64}()
const LOCAL_JOB_STORE_DIR = Ref{Union{Nothing, String}}(nothing)

function _aws_batch_describe_min_interval()
    raw = Config.aws_batch_describe_min_interval_raw()
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

const JOB_TERMINAL_STATUSES = Set([
    "succeeded",
    "failed",
    "cancelled",
])

# Job state changes are linearized while JOBS_LOCK is held.  Long-running work
# (the local computation and every AWS CLI call) happens outside that lock and
# may only publish a result through one of these transitions.  In particular,
# terminal states never move again and a cancellation request cannot be
# overwritten by a late local completion.
const JOB_STATUS_TRANSITIONS = Dict(
    "queued" => Set(["running", "succeeded", "failed", "cancel_requested", "cancelled"]),
    "running" => Set(["succeeded", "failed", "cancel_requested"]),
    "cancel_requested" => Set(["succeeded", "failed", "cancelled"]),
    "succeeded" => Set{String}(),
    "failed" => Set{String}(),
    "cancelled" => Set{String}(),
)

const LOCAL_JOB_KINDS = Set([
    "build_atlas",
    "build_atlas_library",
    "merge_atlas_library",
    "query_atlas",
    "run_inverse_design",
])

const CANCEL_DISPATCH_CLAIM_TTL_SECONDS = 60.0

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
    return Config.quota_table()
end

function _quota_daily_limit()
    raw = Config.quota_daily_limit_raw()
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
        configured = Config.job_store_override()
        LOCAL_JOB_STORE_DIR[] = isempty(configured) ?
            normpath(joinpath(@__DIR__, "..", "job_store")) :
            abspath(configured)
    end
    return LOCAL_JOB_STORE_DIR[]
end

function local_job_store_ready()
    path = local_job_store_dir()
    try
        mkpath(path)
        return isdir(path) && iswritable(path)
    catch err
        @warn "Job store readiness check failed" path exception=(err, catch_backtrace())
        return false
    end
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
_aws_cli() = Config.aws_cli_binary()

function _write_job_json(path::AbstractString, payload)
    mkpath(dirname(path))
    isdir(path) && throw(ArgumentError("Job JSON path is a directory: $(path)"))
    temp_path, temp_io = mktemp(dirname(path); cleanup=false)
    try
        JSON3.pretty(temp_io, json_safe_value(payload))
        write(temp_io, "\n")
        flush(temp_io)
        ccall(:fsync, Cint, (Cint,), fd(temp_io)) == 0 || error("fsync failed for $(temp_path)")
        close(temp_io)
        mv(temp_path, path; force=true)
    catch
        isopen(temp_io) && close(temp_io)
        isfile(temp_path) && rm(temp_path; force=true)
        rethrow()
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
        haskey(record, key) && (out[key] = deepcopy(record[key]))
    end

    out["artifacts"]["input"] = "job://$(job_id)/input"
    out["artifacts"]["status"] = "job://$(job_id)/status"
    if Bool(get(record, "result_available", false))
        out["result_ref"] = "job://$(job_id)/result"
        out["artifacts"]["result"] = out["result_ref"]
    end

    return out
end

_job_snapshot(record::AbstractDict) = deepcopy(record)

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

# `record.json` is the canonical durable state. `status.json` is a public,
# rebuildable projection: a projection write must never roll back a transition
# whose canonical record was committed successfully.
function _persist_job_status_projection_unlocked(record::AbstractDict)
    try
        _persist_job_status_unlocked(record)
    catch err
        @warn "Failed to refresh derived job status projection" job_id=get(record, "job_id", nothing) exception=(err, catch_backtrace())
    end
    return nothing
end

function _job_record_unlocked(job_id::AbstractString)
    id = String(job_id)
    record = get(JOBS, id, nothing)
    if record === nothing
        path = _job_record_path(id)
        isfile(path) || return nothing
        record = _read_job_json(path)
        JOBS[id] = record
    end
    return record
end

function _status_expected(status::AbstractString, expected)
    expected === nothing && return true
    expected isa AbstractString && return status == String(expected)
    return status in expected
end

function _apply_job_transition_unlocked!(record::AbstractDict,
                                         target_status::AbstractString;
                                         expected=nothing,
                                         updates...)
    current_status = String(get(record, "status", ""))
    target_status = String(target_status)
    current_status in JOB_STATUSES || throw(ArgumentError("Unknown current job status: $(current_status)"))
    target_status in JOB_STATUSES || throw(ArgumentError("Unknown target job status: $(target_status)"))
    _status_expected(current_status, expected) || return false

    # Terminal snapshots are immutable, including same-status calls.  Nonterminal
    # same-status updates remain useful for publishing cancellation dispatch
    # metadata without weakening the terminal-state invariant.
    current_status in JOB_TERMINAL_STATUSES && return false

    if current_status != target_status
        target_status in JOB_STATUS_TRANSITIONS[current_status] || return false
        record["status"] = target_status
    end
    for (key, value) in updates
        String(key) == "status" && throw(ArgumentError("Pass the target status positionally."))
        record[String(key)] = value
    end
    record["updated_at"] = _now_iso_timestamp()
    return true
end

function _commit_job_candidate_unlocked!(record::AbstractDict, candidate::AbstractDict)
    # Persist the canonical candidate before exposing it through the shared
    # in-memory dictionary. Atomic replacement in `_write_job_json` guarantees
    # that a failed write leaves both views at the previous revision.
    _persist_job_record_unlocked(candidate)
    empty!(record)
    merge!(record, candidate)
    _persist_job_status_projection_unlocked(record)
    return record
end

function _transition_job_record_unlocked!(record::AbstractDict,
                                          target_status::AbstractString;
                                          expected=nothing,
                                          updates...)
    candidate = deepcopy(record)
    applied = _apply_job_transition_unlocked!(
        candidate,
        target_status;
        expected=expected,
        updates...,
    )
    applied && _commit_job_candidate_unlocked!(record, candidate)
    return applied
end

function _cancel_dispatch_claim_active(record::AbstractDict; now_epoch::Real=time())
    haskey(record, "cancel_dispatch_claim") || return false
    claimed_at = try
        Float64(get(record, "cancel_dispatch_claimed_at_epoch", 0.0))
    catch
        0.0
    end
    return claimed_at > 0 && Float64(now_epoch) - claimed_at < CANCEL_DISPATCH_CLAIM_TTL_SECONDS
end

function _finish_cancel_dispatch_claim!(job_id::AbstractString, claim_token::AbstractString; updates...)
    lock(JOBS_LOCK) do
        record = _job_record_unlocked(job_id)
        record === nothing && return (applied=false, record=nothing)
        String(get(record, "cancel_dispatch_claim", "")) == String(claim_token) ||
            return (applied=false, record=_job_snapshot(record))
        candidate = deepcopy(record)
        applied = _apply_job_transition_unlocked!(
            candidate,
            "cancel_requested";
            expected=("cancel_requested",),
            updates...,
        )
        if applied
            delete!(candidate, "cancel_dispatch_claim")
            delete!(candidate, "cancel_dispatch_claimed_at_epoch")
            _commit_job_candidate_unlocked!(record, candidate)
        end
        return (applied=applied, record=_job_snapshot(record))
    end
end

function _job_transition!(job_id::AbstractString,
                          target_status::AbstractString;
                          expected=nothing,
                          updates...)
    lock(JOBS_LOCK) do
        record = _job_record_unlocked(job_id)
        record === nothing && return (applied=false, record=nothing, previous_status=nothing)
        previous_status = String(get(record, "status", ""))
        applied = _transition_job_record_unlocked!(
            record,
            target_status;
            expected=expected,
            updates...,
        )
        return (applied=applied, record=_job_snapshot(record), previous_status=previous_status)
    end
end

function _job_record(job_id::AbstractString)
    lock(JOBS_LOCK) do
        record = _job_record_unlocked(job_id)
        record === nothing && return nothing
        return _job_snapshot(record)
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

function _execute_local_job(kind::AbstractString, spec; cancel_check::Function=_no_cancel_check)
    # Wrap every local-job result in the bne-result envelope (as a sibling
    # `artifact` field) so the persisted result and its `result_ref` are
    # self-describing: algorithm + version, input network hashes, and a config
    # hash of the spec for reproducibility.
    cancel_check()
    result = _dispatch_local_job(kind, spec; cancel_check=cancel_check)
    cancel_check()
    return attach_artifact!(result, kind;
        input_hashes = Dict{String, Any}("network_ir_hashes" => artifact_network_hashes(spec)),
        config = spec)
end

function _dispatch_local_job(kind::AbstractString, spec; cancel_check::Function=_no_cancel_check)
    if kind == "build_atlas"
        return build_behavior_atlas_from_spec(spec; cancel_check=cancel_check)
    elseif kind == "build_atlas_library"
        return build_atlas_library_from_spec(spec; cancel_check=cancel_check)
    elseif kind == "merge_atlas_library"
        return merge_atlas_library_from_spec(spec; cancel_check=cancel_check)
    elseif kind == "query_atlas"
        return query_behavior_atlas_from_spec(spec; cancel_check=cancel_check)
    elseif kind == "run_inverse_design"
        return run_inverse_design_from_spec(spec; cancel_check=cancel_check)
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

function _finish_local_job!(job_id::AbstractString;
                            succeeded::Bool,
                            error=nothing)
    lock(JOBS_LOCK) do
        record = _job_record_unlocked(job_id)
        record === nothing && return (applied=false, record=nothing)
        current_status = String(get(record, "status", ""))

        if current_status == "cancel_requested"
            target_status = "cancelled"
            updates = Dict{Symbol, Any}(
                :finished_at => _now_iso_timestamp(),
                :result_available => false,
                :progress => Dict("message" => "Cancelled after local execution completed"),
            )
        elseif current_status == "running"
            target_status = succeeded ? "succeeded" : "failed"
            updates = Dict{Symbol, Any}(
                :finished_at => _now_iso_timestamp(),
                :result_available => succeeded,
                :progress => Dict("message" => succeeded ? "Completed" : "Failed"),
            )
            !succeeded && (updates[:error] = error === nothing ? "Local job failed" : String(error))
        else
            return (applied=false, record=_job_snapshot(record))
        end

        applied = _transition_job_record_unlocked!(
            record,
            target_status;
            expected=(current_status,),
            updates...,
        )
        return (applied=applied, record=_job_snapshot(record))
    end
end

function _run_local_job!(job_id::String, kind::String, spec,
                         token::LocalJobCancelToken=LocalJobCancelToken(job_id))
    cancel_check = () -> _check_cancelled(token)
    try
        started = _job_transition!(job_id, "running";
            expected=("queued",),
            started_at=_now_iso_timestamp(),
            progress=Dict("message" => "Running locally"),
        )
        # A queued cancellation that wins the lock race is terminal.  The
        # worker observes it here and exits without entering the computation.
        started.applied || return nothing

        try
            # Cooperative checkpoint: cancellation never injects an asynchronous
            # exception into Julia code.  It is observed at safe job boundaries.
            before_execution = _job_record(job_id)
            if before_execution !== nothing && String(get(before_execution, "status", "")) == "cancel_requested"
                _job_transition!(job_id, "cancelled";
                    expected=("cancel_requested",),
                    finished_at=_now_iso_timestamp(),
                    result_available=false,
                    progress=Dict("message" => "Cancelled before local execution"),
                )
                return nothing
            end

            result = _execute_local_job(kind, spec; cancel_check=cancel_check)

            # Do not publish a result once cancellation has been observed.  A
            # cancellation racing the following artifact write is handled by
            # the final atomic transition below.
            after_execution = _job_record(job_id)
            if after_execution !== nothing && String(get(after_execution, "status", "")) == "cancel_requested"
                _job_transition!(job_id, "cancelled";
                    expected=("cancel_requested",),
                    finished_at=_now_iso_timestamp(),
                    result_available=false,
                    progress=Dict("message" => "Cancelled after local execution completed"),
                )
                return nothing
            end

            record = _job_record(job_id)
            result_uri = record === nothing ? _job_result_path(job_id) : String(get(record, "result_uri", _job_result_path(job_id)))
            _write_json_uri(result_uri, result)
            _finish_local_job!(job_id; succeeded=true)
        catch err
            if err isa LocalJobCancelled
                _job_transition!(job_id, "cancelled";
                    expected=("cancel_requested",),
                    finished_at=_now_iso_timestamp(),
                    result_available=false,
                    progress=Dict("message" => "Cancelled during local execution"),
                )
            else
                _finish_local_job!(job_id;
                    succeeded=false,
                    error=sprint(showerror, err, catch_backtrace()),
                )
            end
        end
    finally
        lock(JOBS_LOCK) do
            delete!(JOB_TASKS, job_id)
            delete!(LOCAL_JOB_CANCEL_TOKENS, job_id)
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
    return Config.allow_aws_batch_request_config()
end

function _aws_batch_config_value(execution, key::Symbol, env_name::AbstractString)
    if _allow_aws_batch_request_config() && _raw_haskey(execution, key)
        return _raw_get(execution, key, nothing)
    end
    return Config.aws_batch_env_value(env_name)
end

function _aws_batch_job_name_prefix(execution)
    requested = _aws_batch_config_value(
        execution,
        :job_name_prefix,
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX",
    )
    prefix = requested === nothing ? "" : strip(String(requested))
    return isempty(prefix) ? Config.aws_batch_job_name_prefix() : prefix
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
    if _allow_aws_batch_request_config() && _raw_haskey(execution, :vcpus)
        push!(resources, Dict("type" => "VCPU", "value" => string(_raw_get(execution, :vcpus, ""))))
    end
    if _allow_aws_batch_request_config() && _raw_haskey(execution, :memory_mib)
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

# Perform the remote half of a submission from an immutable snapshot.  The
# returned fields are merged into the live record under JOBS_LOCK by the
# caller.  This function must never be called while JOBS_LOCK is held.
function _aws_batch_submit(record::AbstractDict, execution)
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

    job_name_prefix = _aws_batch_job_name_prefix(execution)
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

    return Dict{String, Any}(
        "input_uri" => input_uri,
        "status_uri" => status_uri,
        "result_uri" => result_uri,
        "batch_job_id" => String(_raw_get(response, :jobId, "")),
        "batch_job_name" => String(_raw_get(response, :jobName, job_name)),
        "progress" => Dict(
            "message" => "Submitted to AWS Batch",
            "aws_status" => "SUBMITTED",
        ),
    )
end

function _refresh_aws_batch_job!(job_id::AbstractString)
    id = String(job_id)
    probe = lock(JOBS_LOCK) do
        record = _job_record_unlocked(id)
        record === nothing && return (refresh=false, record=nothing)
        String(get(record, "executor", "")) == "aws_batch" ||
            return (refresh=false, record=_job_snapshot(record))
        haskey(record, "batch_job_id") || return (refresh=false, record=_job_snapshot(record))
        String(get(record, "status", "")) in JOB_TERMINAL_STATUSES &&
            return (refresh=false, record=_job_snapshot(record))

        now_epoch = time()
        last_at = get(JOB_DESCRIBE_LAST_AT, id, 0.0)
        if now_epoch - last_at < _aws_batch_describe_min_interval()
            return (refresh=false, record=_job_snapshot(record))
        end
        JOB_DESCRIBE_LAST_AT[id] = now_epoch
        return (refresh=true, record=_job_snapshot(record))
    end
    probe.refresh || return probe.record

    # AWS calls deliberately run without JOBS_LOCK.  The response is committed
    # only if it is still a legal successor of the state observed afterwards.
    response = _aws_cli_json(["batch", "describe-jobs", "--jobs", String(probe.record["batch_job_id"])])
    jobs = collect(_raw_get(response, :jobs, Any[]))
    isempty(jobs) && return _job_record(id)
    aws_job = jobs[1]
    aws_status = String(_raw_get(aws_job, :status, "UNKNOWN"))
    aws_job_status = _aws_batch_status_to_job_status(aws_status)
    status = aws_job_status
    container = _raw_get(aws_job, :container, Dict{String, Any}())
    log_stream = String(_raw_get(container, :logStreamName, ""))
    failure_reason = String(_raw_get(aws_job, :statusReason, _raw_get(container, :reason, "AWS Batch job failed")))
    missing_result_error = nothing

    # AWS Batch SUCCEEDED only confirms that the container exited 0.  The S3
    # probe is external I/O too, so it also happens before reacquiring the lock.
    if status == "succeeded"
        result_uri = String(get(probe.record, "result_uri", ""))
        if !isempty(result_uri) && !_artifact_exists(result_uri)
            status = "failed"
            missing_result_error = "AWS Batch job exited successfully but result artifact is missing: $(result_uri)"
        end
    end

    return lock(JOBS_LOCK) do
        record = _job_record_unlocked(id)
        record === nothing && return nothing
        current_status = String(get(record, "status", ""))
        current_status in JOB_TERMINAL_STATUSES && return _job_snapshot(record)

        # A pending cancellation is monotonic while AWS remains nonterminal.
        # AWS FAILED after cancel/terminate confirms cancellation; SUCCEEDED is
        # retained because the remote job may have won the finish race.
        target_status = status
        if current_status == "cancel_requested"
            if aws_job_status == "failed" && missing_result_error === nothing
                target_status = "cancelled"
            elseif aws_job_status in ("queued", "running")
                target_status = "cancel_requested"
            end
        elseif current_status == "running" && target_status == "queued"
            target_status = "running"
        end

        updates = Dict{Symbol, Any}(
            :result_available => target_status == "succeeded",
            :progress => Dict(
                "message" => "AWS Batch status: $(aws_status)",
                "aws_status" => aws_status,
            ),
        )
        !isempty(log_stream) && (updates[:log_stream_name] = log_stream)
        if target_status == "running" && !haskey(record, "started_at")
            updates[:started_at] = _now_iso_timestamp()
        end
        if target_status in JOB_TERMINAL_STATUSES
            updates[:finished_at] = _now_iso_timestamp()
        end
        if target_status == "failed"
            updates[:error] = missing_result_error === nothing ? failure_reason : missing_result_error
        end

        applied = _transition_job_record_unlocked!(
            record,
            target_status;
            expected=(current_status,),
            updates...,
        )
        return _job_snapshot(record)
    end
end

function _cancel_aws_batch_job!(record::AbstractDict; observed_status=nothing)
    haskey(record, "batch_job_id") || throw(ArgumentError("AWS Batch job has not been submitted yet."))
    batch_job_id = String(record["batch_job_id"])
    status = observed_status === nothing ? String(get(record, "status", "queued")) : String(observed_status)
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

# Fetch only the remote lifecycle state needed to resolve a cancellation race.
# Like every other AWS helper, callers must invoke this without JOBS_LOCK.
function _describe_aws_batch_status(record::AbstractDict)
    haskey(record, "batch_job_id") || return nothing
    batch_job_id = String(record["batch_job_id"])
    isempty(batch_job_id) && return nothing
    response = _aws_cli_json(["batch", "describe-jobs", "--jobs", batch_job_id])
    jobs = collect(_raw_get(response, :jobs, Any[]))
    isempty(jobs) && return nothing
    return uppercase(String(_raw_get(jobs[1], :status, "UNKNOWN")))
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
        _persist_job_record_unlocked(record)
        JOBS[job_id] = record
        _persist_job_status_projection_unlocked(record)
    end

    if executor == "aws_batch"
        submission = try
            # The snapshot prevents the remote helper from mutating shared
            # state while S3 or Batch is slow.
            _aws_batch_submit(_job_snapshot(record), execution)
        catch err
            submission_error = sprint(showerror, err, catch_backtrace())
            failed = _job_transition!(job_id, "failed";
                expected=("queued",),
                finished_at=_now_iso_timestamp(),
                result_available=false,
                error=submission_error,
                progress=Dict("message" => "AWS Batch submission failed"),
            )
            if !failed.applied && failed.record !== nothing &&
               String(get(failed.record, "status", "")) == "cancel_requested"
                # Cancellation linearized before the failed remote submission.
                # No external id was committed, so settle the request instead
                # of leaving an unobservable cancel_requested job forever.
                _job_transition!(job_id, "cancelled";
                    expected=("cancel_requested",),
                    finished_at=_now_iso_timestamp(),
                    result_available=false,
                    error=submission_error,
                    progress=Dict("message" => "Cancelled while AWS Batch submission failed"),
                )
            end
            rethrow()
        end

        cancel_after_submit = lock(JOBS_LOCK) do
            live_record = _job_record_unlocked(job_id)
            live_record === nothing && return nothing
            candidate = deepcopy(live_record)
            for (key, value) in submission
                # Preserve the cancellation message when cancellation won the
                # race with the remote submit call.
                key == "progress" && String(candidate["status"]) != "queued" && continue
                candidate[key] = value
            end
            candidate["updated_at"] = _now_iso_timestamp()
            _commit_job_candidate_unlocked!(live_record, candidate)
            return String(live_record["status"]) in ("cancel_requested", "cancelled") ? _job_snapshot(live_record) : nothing
        end
        if cancel_after_submit !== nothing
            # A cancellation can race an in-flight submission before a Batch
            # job id exists.  Once the id arrives, honour it without holding
            # the global job lock.
            cancel_biocircuits_job(job_id; user_sub=user_sub)
        end
    else
        start_gate = Channel{Nothing}(1)
        token = LocalJobCancelToken(job_id)
        task = Threads.@spawn begin
            take!(start_gate)
            _run_local_job!(job_id, kind, spec, token)
        end
        lock(JOBS_LOCK) do
            JOB_TASKS[job_id] = task
            LOCAL_JOB_CANCEL_TOKENS[job_id] = token
        end
        # Registration happens-before worker execution, so the worker's
        # `finally` deletion cannot race a late bookkeeping insertion.
        put!(start_gate, nothing)
    end

    return get_biocircuits_job(job_id; user_sub=user_sub)
end

function cancel_biocircuits_job(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    job_id = String(job_id)
    decision = lock(JOBS_LOCK) do
        record = _job_record_unlocked(job_id)
        record === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
        _check_user_owns_record(record, user_sub, job_id)
        status = String(record["status"])
        if status in JOB_TERMINAL_STATUSES
            return (public=_job_public_record(record), aws_record=nothing,
                    observed_status=status, dispatch_claim=nothing)
        end

        executor = String(get(record, "executor", ""))
        has_external_job = executor == "aws_batch" &&
            haskey(record, "batch_job_id") &&
            !isempty(String(record["batch_job_id"]))
        if status == "cancel_requested"
            # A successful dispatch is idempotent.  A failed dispatch deliberately
            # leaves this marker absent so the next API call retries the CLI.
            should_retry = has_external_job &&
                !haskey(record, "cancel_dispatched_at") &&
                !_cancel_dispatch_claim_active(record)
            dispatch_claim = nothing
            if should_retry
                dispatch_claim = string(rand(UInt128), base=16, pad=32)
                claimed = _transition_job_record_unlocked!(
                    record,
                    "cancel_requested";
                    expected=("cancel_requested",),
                    cancel_dispatch_claim=dispatch_claim,
                    cancel_dispatch_claimed_at_epoch=time(),
                )
                claimed || (dispatch_claim = nothing)
            end
            observed_status = String(get(
                record,
                "cancel_observed_status",
                haskey(record, "started_at") ? "running" : "queued",
            ))
            return (
                public=_job_public_record(record),
                aws_record=dispatch_claim === nothing ? nothing : _job_snapshot(record),
                observed_status=observed_status,
                dispatch_claim=dispatch_claim,
            )
        end

        # For every AWS job, first publish a cancellation intent.  This also
        # covers cancellation while submit-job is still in flight and no
        # external id exists yet; the submit path observes the intent once the
        # id arrives.  A simultaneous describe response cannot overwrite it.
        target_status = status == "queued" && executor != "aws_batch" ? "cancelled" : "cancel_requested"
        dispatch_claim = has_external_job ? string(rand(UInt128), base=16, pad=32) : nothing
        transition_updates = Dict{Symbol, Any}(
            :cancel_requested_at => _now_iso_timestamp(),
            :cancel_observed_status => status,
            :result_available => false,
            :progress => Dict("message" => target_status == "cancelled" ? "Cancelled" : "Cancel requested"),
        )
        if dispatch_claim !== nothing
            transition_updates[:cancel_dispatch_claim] = dispatch_claim
            transition_updates[:cancel_dispatch_claimed_at_epoch] = time()
        end
        applied = _transition_job_record_unlocked!(
            record,
            target_status;
            expected=(status,),
            transition_updates...,
        )
        applied || return (public=_job_public_record(record), aws_record=nothing,
                           observed_status=status, dispatch_claim=nothing)
        token = get(LOCAL_JOB_CANCEL_TOKENS, job_id, nothing)
        token === nothing || _request_cancel!(token)
        delete!(JOB_DESCRIBE_LAST_AT, job_id)
        return (
            public=_job_public_record(record),
            aws_record=has_external_job ? _job_snapshot(record) : nothing,
            observed_status=status,
            dispatch_claim=dispatch_claim,
        )
    end

    decision.aws_record === nothing && return decision.public

    try
        _cancel_aws_batch_job!(decision.aws_record; observed_status=decision.observed_status)
    catch err
        # Keep the monotonic cancellation intent: a failed CLI invocation does
        # not make it safe for a late completion to overwrite the request.
        failed_update = _finish_cancel_dispatch_claim!(job_id, decision.dispatch_claim;
            cancel_error=sprint(showerror, err),
            progress=Dict("message" => "Cancel request failed"),
        )
        if !failed_update.applied && failed_update.record !== nothing &&
           String(get(failed_update.record, "status", "")) in JOB_TERMINAL_STATUSES
            return _job_public_record(failed_update.record)
        end
        rethrow()
    end

    remote_status = nothing
    if decision.observed_status == "queued"
        # CancelJob applies to queued Batch states only.  If the remote job won
        # the queued→running race, immediately escalate to TerminateJob and keep
        # the public state pending until AWS confirms a terminal outcome.
        remote_status = try
            _describe_aws_batch_status(decision.aws_record)
        catch
            nothing
        end
        if remote_status in ("STARTING", "RUNNING")
            try
                _cancel_aws_batch_job!(decision.aws_record; observed_status="running")
            catch err
                failed_update = _finish_cancel_dispatch_claim!(job_id, decision.dispatch_claim;
                    cancel_error=sprint(showerror, err),
                    progress=Dict("message" => "Cancel request failed"),
                )
                if !failed_update.applied && failed_update.record !== nothing &&
                   String(get(failed_update.record, "status", "")) in JOB_TERMINAL_STATUSES
                    return _job_public_record(failed_update.record)
                end
                rethrow()
            end
        end
    end

    dispatched = _finish_cancel_dispatch_claim!(job_id, decision.dispatch_claim;
        cancel_dispatched_at=_now_iso_timestamp(),
        cancel_error=nothing,
        cancel_aws_status=remote_status,
        progress=Dict("message" => "Cancel requested"),
    )
    if !dispatched.applied
        return dispatched.record === nothing ? decision.public : _job_public_record(dispatched.record)
    end

    if decision.observed_status == "queued" &&
       remote_status in ("SUBMITTED", "PENDING", "RUNNABLE", "FAILED")
        settled = _job_transition!(job_id, "cancelled";
            expected=("cancel_requested",),
            finished_at=_now_iso_timestamp(),
            result_available=false,
            progress=Dict("message" => "Cancelled"),
        )
        settled.record !== nothing && return _job_public_record(settled.record)
    end
    latest = _job_record(job_id)
    return latest === nothing ? decision.public : _job_public_record(latest)
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
    return Config.cognito_user_pool_id()
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
    route = _match_api_route(path)

    if route !== nothing && route.handler === :handle_jobs_route &&
       !_api_route_allows_method(route, req.method)
        return error_response("Method not allowed"; status=405)
    end

    if parts == ["api", "jobs"]
        request = _normalize_http_atlas_paths(read_json(req))
        return json_response(
            submit_biocircuits_job_from_spec(request; user_sub=user_sub);
            status=202,
        )
    end

    if length(parts) == 3 && parts[1] == "api" && parts[2] == "jobs"
        return json_response(get_biocircuits_job(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "result"
        return json_response(get_biocircuits_job_result(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "result-url"
        return json_response(get_biocircuits_job_result_url(parts[3]; user_sub=user_sub))
    end

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "cancel"
        return json_response(cancel_biocircuits_job(parts[3]; user_sub=user_sub))
    end

    return error_response("Unknown jobs route"; status=404)
end
