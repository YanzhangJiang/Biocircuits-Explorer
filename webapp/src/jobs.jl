const JOBS = Dict{String, Dict{String, Any}}()
const JOBS_LOCK = ReentrantLock()
const JOB_CACHE_LAST_ACCESS = Dict{String, UInt64}()
const JOB_CACHE_ACCESS_CLOCK = Ref{UInt64}(0)
const JOB_CACHE_CAPACITY = Ref{Union{Nothing, Int}}(nothing)
const JOB_LOCK_STRIPE_COUNT = 128
const JOB_LOCK_STRIPES = [ReentrantLock() for _ in 1:JOB_LOCK_STRIPE_COUNT]
const JOB_TASKS = Dict{String, Task}()
const LOCAL_JOB_CANCEL_TOKENS = Dict{String, LocalJobCancelToken}()
const JOB_DESCRIBE_LAST_AT = Dict{String, Float64}()
const JOB_DESCRIBE_IN_FLIGHT = Set{String}()
const AWS_BATCH_INITIAL_SUBMISSIONS = Set{String}()
const LOCAL_JOB_STORE_DIR = Ref{Union{Nothing, String}}(nothing)
const LOCAL_JOB_ADMISSIONS = Set{String}()
const LOCAL_JOB_LIMITS = Ref{Union{Nothing, Tuple{Int, Int}}}(nothing)
const LOCAL_JOB_RUN_SEMAPHORE = Ref{Union{Nothing, Base.Semaphore}}(nothing)
const JOB_STATUS_PROJECTION_DIRTY = Set{String}()
const JOB_STORE_DURABILITY_LOCK = ReentrantLock()
const JOB_STORE_PENDING_DIR_FSYNC = Dict{String, UInt64}()
const JOB_STORE_DURABILITY_GENERATION = Ref{UInt64}(0)

# Canonical state is serialized per job, not process-wide. The stripe mapping
# deliberately avoids Julia's randomized `hash` so one job_id always maps to
# the same stripe across supported Julia processes and versions. Lock order is
# job stripe -> short JOBS_LOCK registry/cache section. File I/O, JSON parsing,
# deepcopy, rename/fsync, projection inspection/repair, and external calls must
# never run while JOBS_LOCK is held.
function _job_lock_stripe_index(job_id::AbstractString)
    value = UInt64(0xcbf29ce484222325)
    for byte in codeunits(String(job_id))
        value = xor(value, UInt64(byte)) * UInt64(0x00000100000001b3)
    end
    return Int(mod(value, UInt64(JOB_LOCK_STRIPE_COUNT))) + 1
end

_job_lock(job_id::AbstractString) =
    JOB_LOCK_STRIPES[_job_lock_stripe_index(job_id)]

function _with_job_lock(f::Function, job_id::AbstractString)
    return lock(_job_lock(job_id)) do
        f()
    end
end

function _renormalize_job_cache_clock_unlocked!()
    ordered = sort!(collect(keys(JOBS)); by=id -> (
        get(JOB_CACHE_LAST_ACCESS, id, UInt64(0)),
        id,
    ))
    empty!(JOB_CACHE_LAST_ACCESS)
    for (index, id) in enumerate(ordered)
        JOB_CACHE_LAST_ACCESS[id] = UInt64(index)
    end
    JOB_CACHE_ACCESS_CLOCK[] = UInt64(length(ordered))
    return nothing
end

function _next_job_cache_tick_unlocked!()
    JOB_CACHE_ACCESS_CLOCK[] == typemax(UInt64) &&
        _renormalize_job_cache_clock_unlocked!()
    JOB_CACHE_ACCESS_CLOCK[] += UInt64(1)
    return JOB_CACHE_ACCESS_CLOCK[]
end

function _touch_job_cache_unlocked!(job_id::AbstractString)
    id = String(job_id)
    haskey(JOBS, id) || return nothing
    JOB_CACHE_LAST_ACCESS[id] = _next_job_cache_tick_unlocked!()
    return nothing
end

_active_job_cache_capacity_unlocked() = something(JOB_CACHE_CAPACITY[], 1024)

function _prune_job_cache_unlocked!(capacity::Integer=_active_job_cache_capacity_unlocked())
    capacity > 0 || throw(ArgumentError("Job cache capacity must be positive."))

    for id in collect(keys(JOB_CACHE_LAST_ACCESS))
        haskey(JOBS, id) || delete!(JOB_CACHE_LAST_ACCESS, id)
    end
    for id in sort!(collect(keys(JOBS)))
        haskey(JOB_CACHE_LAST_ACCESS, id) || _touch_job_cache_unlocked!(id)
    end

    excess = length(JOBS) - Int(capacity)
    if excess > 0
        ordered = sort!(collect(keys(JOBS)); by=id -> (
            JOB_CACHE_LAST_ACCESS[id],
            id,
        ))
        for id in @view ordered[1:excess]
            delete!(JOBS, id)
            delete!(JOB_CACHE_LAST_ACCESS, id)
        end
    end
    return nothing
end

function _activate_job_cache_capacity!()
    # Parse outside JOBS_LOCK and before any caller starts canonical I/O. Once
    # validated, publication uses only this frozen-good value, so a concurrent
    # invalid ENV edit cannot turn a successful rename into a reported failure.
    capacity = Config.job_cache_capacity()
    JOB_CACHE_CAPACITY[] == capacity && return capacity
    lock(JOBS_LOCK) do
        if JOB_CACHE_CAPACITY[] != capacity
            JOB_CACHE_CAPACITY[] = capacity
            _prune_job_cache_unlocked!(capacity)
        end
    end
    return capacity
end

function _job_cache_get(job_id::AbstractString)
    id = String(job_id)
    _activate_job_cache_capacity!()
    return lock(JOBS_LOCK) do
        record = get(JOBS, id, nothing)
        record === nothing || _touch_job_cache_unlocked!(id)
        return record
    end
end

function _job_cache_publish!(job_id::AbstractString,
                             record::Dict{String, Any})
    id = String(job_id)
    lock(JOBS_LOCK) do
        JOBS[id] = record
        _touch_job_cache_unlocked!(id)
        capacity = _active_job_cache_capacity_unlocked()
        length(JOBS) > capacity && _prune_job_cache_unlocked!(capacity)
    end
    return record
end

function _job_cache_remove!(job_id::AbstractString)
    id = String(job_id)
    return lock(JOBS_LOCK) do
        record = pop!(JOBS, id, nothing)
        delete!(JOB_CACHE_LAST_ACCESS, id)
        return record
    end
end

_job_projection_is_dirty(job_id::AbstractString) = lock(JOBS_LOCK) do
    String(job_id) in JOB_STATUS_PROJECTION_DIRTY
end

function _mark_job_projection_dirty!(job_id::AbstractString)
    lock(JOBS_LOCK) do
        push!(JOB_STATUS_PROJECTION_DIRTY, String(job_id))
    end
    return nothing
end

function _clear_job_projection_dirty!(job_id::AbstractString)
    lock(JOBS_LOCK) do
        delete!(JOB_STATUS_PROJECTION_DIRTY, String(job_id))
    end
    return nothing
end

const JOB_DESCRIBE_CACHE_MAX_ENTRIES = 4096
const JOB_DESCRIBE_CACHE_MIN_TTL_SECONDS = 300.0

function _aws_batch_describe_min_interval()
    raw = Config.aws_batch_describe_min_interval_raw()
    isempty(raw) && return 3.0
    parsed = tryparse(Float64, raw)
    return parsed === nothing || !isfinite(parsed) ? 3.0 : max(parsed, 0.0)
end

_monotonic_seconds() = Float64(time_ns()) / 1.0e9

function _job_describe_cache_ttl_seconds()
    return max(
        JOB_DESCRIBE_CACHE_MIN_TTL_SECONDS,
        2 * _aws_batch_describe_min_interval(),
    )
end

function _prune_job_describe_cache_unlocked!(now_seconds::Real;
                                               ttl_seconds::Real=_job_describe_cache_ttl_seconds(),
                                               max_entries::Integer=JOB_DESCRIBE_CACHE_MAX_ENTRIES)
    ttl_seconds > 0 || throw(ArgumentError("Describe cache TTL must be positive."))
    max_entries > 0 || throw(ArgumentError("Describe cache capacity must be positive."))
    now_value = Float64(now_seconds)

    for (job_id, last_at) in collect(JOB_DESCRIBE_LAST_AT)
        if !isfinite(last_at) || last_at > now_value || now_value - last_at >= ttl_seconds
            delete!(JOB_DESCRIBE_LAST_AT, job_id)
        end
    end

    while length(JOB_DESCRIBE_LAST_AT) > max_entries
        oldest_job_id = nothing
        oldest_at = Inf
        for (job_id, last_at) in JOB_DESCRIBE_LAST_AT
            if last_at < oldest_at
                oldest_job_id = job_id
                oldest_at = last_at
            end
        end
        oldest_job_id === nothing && break
        delete!(JOB_DESCRIBE_LAST_AT, oldest_job_id)
    end
    return nothing
end

function _remember_job_describe_unlocked!(job_id::AbstractString,
                                           now_seconds::Real=_monotonic_seconds();
                                           ttl_seconds::Real=_job_describe_cache_ttl_seconds(),
                                           max_entries::Integer=JOB_DESCRIBE_CACHE_MAX_ENTRIES)
    id = String(job_id)
    _prune_job_describe_cache_unlocked!(
        now_seconds;
        ttl_seconds=ttl_seconds,
        max_entries=max_entries,
    )
    if !haskey(JOB_DESCRIBE_LAST_AT, id)
        while length(JOB_DESCRIBE_LAST_AT) >= max_entries
            oldest_job_id = nothing
            oldest_at = Inf
            for (candidate_id, last_at) in JOB_DESCRIBE_LAST_AT
                if last_at < oldest_at
                    oldest_job_id = candidate_id
                    oldest_at = last_at
                end
            end
            oldest_job_id === nothing && break
            delete!(JOB_DESCRIBE_LAST_AT, oldest_job_id)
        end
    end
    JOB_DESCRIBE_LAST_AT[id] = Float64(now_seconds)
    return nothing
end

function _configured_local_job_limits()
    concurrency = Config.local_job_max_concurrency()
    admission_limit = Config.local_job_admission_limit()
    concurrency <= admission_limit || throw(ArgumentError(
        "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY ($(concurrency)) " *
        "must not exceed BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT ($(admission_limit))."))
    return (concurrency, admission_limit)
end

function _reserve_local_job_admission!(job_id::AbstractString)
    configured_limits = _configured_local_job_limits()
    id = String(job_id)
    return lock(JOBS_LOCK) do
        active_limits = LOCAL_JOB_LIMITS[]
        if active_limits === nothing || active_limits != configured_limits
            isempty(LOCAL_JOB_ADMISSIONS) || throw(ArgumentError(
                "Local job capacity cannot be reconfigured while jobs are admitted."))
            LOCAL_JOB_LIMITS[] = configured_limits
            LOCAL_JOB_RUN_SEMAPHORE[] = Base.Semaphore(first(configured_limits))
        end

        admission_limit = last(configured_limits)
        length(LOCAL_JOB_ADMISSIONS) < admission_limit ||
            throw(LocalJobCapacityExceeded(admission_limit))
        id in LOCAL_JOB_ADMISSIONS &&
            error("Local job $(id) already owns an admission reservation.")
        push!(LOCAL_JOB_ADMISSIONS, id)
        semaphore = LOCAL_JOB_RUN_SEMAPHORE[]
        semaphore === nothing && error("Local job run semaphore was not initialized.")
        return semaphore
    end
end

function _release_local_job_admission!(job_id::AbstractString)
    id = String(job_id)
    lock(JOBS_LOCK) do
        id in LOCAL_JOB_ADMISSIONS ||
            error("Local job $(id) does not own an admission reservation.")
        delete!(LOCAL_JOB_ADMISSIONS, id)
    end
    return nothing
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

const LOCAL_JOB_RESTART_ERROR_CODE = "local_job_interrupted_by_restart"
const LOCAL_JOB_RESTART_ERROR_MESSAGE =
    "Local job cannot resume because the backend process restarted before it completed."

const JOB_RESULT_PROTOCOL_VERSION = "bne-job-result-manifest/v1.0.0"
const JOB_RESULT_MANIFEST_MAX_BYTES = 64 * 1024
const JOB_RESULT_MEDIA_TYPE = "application/json"
const JOB_RESULT_SHA256_METADATA_KEY = "bne-result-sha256"

const AWS_BATCH_SUBMISSION_PROTOCOL_VERSION =
    "bne-aws-batch-submission/v1.1.0"
const AWS_BATCH_SUBMISSION_STATES = Set([
    "prepared",
    "dispatch_started",
    "accepted",
    "reconciling",
    "unknown",
    "conflict",
    "legacy_submission_unknown",
    "cancelled_before_dispatch",
    "interrupted_before_dispatch",
    "failed_before_dispatch",
])
const AWS_BATCH_JOB_NAME_MAX_LENGTH = 128
const AWS_BATCH_JOB_ID_TAG = "BneJobId"
const AWS_BATCH_LIST_PAGE_SIZE = 100
const AWS_BATCH_LIST_MAX_PAGES = 100
const AWS_BATCH_RECONCILE_MAX_CANDIDATES =
    AWS_BATCH_LIST_PAGE_SIZE * AWS_BATCH_LIST_MAX_PAGES
const AWS_BATCH_RECONCILE_UNKNOWN_AFTER_ATTEMPTS = 3

mutable struct _AwsBatchDispatchAuthorization
    record::Dict{String, Any}
    consumed::Bool
    lock::ReentrantLock
end

_AwsBatchDispatchAuthorization(record::Dict{String, Any}) =
    _AwsBatchDispatchAuthorization(record, false, ReentrantLock())

function _consume_aws_batch_dispatch_authorization!(
    authorization::_AwsBatchDispatchAuthorization,
)
    return lock(authorization.lock) do
        authorization.consumed && throw(ArgumentError(
            "AWS Batch dispatch authorization has already been consumed."))
        # Consume before remote I/O. An ambiguous or failed response must never
        # make the same authorization reusable.
        authorization.consumed = true
        authorization.record
    end
end

_job_result_manifest_timestamp() =
    Dates.format(Dates.now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SSZ")

function _is_valid_job_result_manifest_timestamp(value)
    value isa AbstractString || return false
    text = String(value)
    occursin(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", text) || return false
    return tryparse(
        Dates.DateTime,
        text,
        dateformat"yyyy-mm-ddTHH:MM:SSZ",
    ) !== nothing
end

# Job state changes are linearized by the job's stable stripe. Local compute
# and every AWS CLI call happen outside that stripe and may publish only through
# these transitions. JOBS_LOCK protects short-lived cache/registry/claim
# metadata only. In particular, terminal states never move again and a
# cancellation request cannot be overwritten by a late local completion.
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
    "rop_shape_optimize",
])

const CANCEL_DISPATCH_CLAIM_TTL_SECONDS = 60.0

struct QuotaExceeded <: Exception
    msg::String
end
Base.showerror(io::IO, e::QuotaExceeded) = print(io, "QuotaExceeded: ", e.msg)

struct LocalJobCapacityExceeded <: Exception
    limit::Int
end
Base.showerror(io::IO, e::LocalJobCapacityExceeded) = print(
    io,
    "Local asynchronous job capacity exhausted (limit: ",
    e.limit,
    ").",
)

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

function _job_result_manifest_path(job_id::AbstractString)
    return joinpath(_job_dir(job_id), "result-manifest.json")
end

_is_s3_uri(uri::AbstractString) = startswith(lowercase(String(uri)), "s3://")
_is_file_uri(uri::AbstractString) = startswith(lowercase(String(uri)), "file://")
_uri_local_path(uri::AbstractString) = _is_file_uri(uri) ? String(uri)[8:end] : String(uri)
_aws_cli() = Config.aws_cli_binary()

struct _JobPersistenceOps{F, R, D}
    fsync_file!::F
    replace!::R
    fsync_dir!::D
end

struct _JobPersistenceResult
    path::String
    committed::Bool
    durable::Bool
    durability_error::Union{Nothing, String}
end

function _require_posix_job_persistence()
    (Sys.isapple() || Sys.islinux()) && return nothing
    throw(ErrorException(
        "Durable local job persistence is supported only on macOS and Linux."))
end

function _fsync_job_file_posix!(io::IO, path::AbstractString)
    _require_posix_job_persistence()
    flush(io)
    result = ccall(:fsync, Cint, (Cint,), fd(io))
    Base.systemerror("fsync job file $(path)", result != 0)
    return nothing
end

function _atomic_replace_job_file_posix!(source::AbstractString,
                                         destination::AbstractString)
    _require_posix_job_persistence()
    source_parent = realpath(dirname(abspath(String(source))))
    destination_parent = realpath(dirname(abspath(String(destination))))
    source_parent == destination_parent || throw(ArgumentError(
        "Atomic job replacement requires source and destination in one directory."))
    result = ccall(
        :rename,
        Cint,
        (Cstring, Cstring),
        String(source),
        String(destination),
    )
    Base.systemerror(
        "rename job file $(source) to $(destination)",
        result != 0,
    )
    return String(destination)
end

function _fsync_job_directory_posix!(path::AbstractString)
    _require_posix_job_persistence()
    directory = abspath(String(path))
    directory_fd = ccall(:open, Cint, (Cstring, Cint), directory, 0)
    Base.systemerror("open job directory $(directory)", directory_fd < 0)
    try
        result = ccall(:fsync, Cint, (Cint,), directory_fd)
        Base.systemerror("fsync job directory $(directory)", result != 0)
    finally
        ccall(:close, Cint, (Cint,), directory_fd)
    end
    return nothing
end

function _job_directory_is_writable_posix(path::AbstractString)
    _require_posix_job_persistence()
    # POSIX access(2) uses the effective process credentials and is available
    # on both supported Julia lines; `iswritable(::String)` is Julia 1.12-only.
    return ccall(:access, Cint, (Cstring, Cint), String(path), 2) == 0
end

const _DEFAULT_JOB_PERSISTENCE_OPS = _JobPersistenceOps(
    _fsync_job_file_posix!,
    _atomic_replace_job_file_posix!,
    _fsync_job_directory_posix!,
)

function _pending_job_store_dir_generation(path::AbstractString)
    directory = abspath(String(path))
    return lock(JOB_STORE_DURABILITY_LOCK) do
        get(JOB_STORE_PENDING_DIR_FSYNC, directory, nothing)
    end
end

function _mark_job_store_dir_fsync_pending!(path::AbstractString)
    directory = abspath(String(path))
    lock(JOB_STORE_DURABILITY_LOCK) do
        JOB_STORE_DURABILITY_GENERATION[] += UInt64(1)
        JOB_STORE_PENDING_DIR_FSYNC[directory] =
            JOB_STORE_DURABILITY_GENERATION[]
    end
    return nothing
end

function _clear_job_store_dir_fsync_pending!(path::AbstractString, observed_generation)
    observed_generation === nothing && return nothing
    directory = abspath(String(path))
    lock(JOB_STORE_DURABILITY_LOCK) do
        get(JOB_STORE_PENDING_DIR_FSYNC, directory, nothing) == observed_generation &&
            delete!(JOB_STORE_PENDING_DIR_FSYNC, directory)
    end
    return nothing
end

function _fsync_job_directory_tracked!(path::AbstractString,
                                       ops::_JobPersistenceOps)
    directory = abspath(String(path))
    observed_generation = _pending_job_store_dir_generation(directory)
    try
        ops.fsync_dir!(directory)
    catch
        _mark_job_store_dir_fsync_pending!(directory)
        rethrow()
    end
    _clear_job_store_dir_fsync_pending!(directory, observed_generation)
    return nothing
end

function _ensure_job_directory_with_ops(path::AbstractString,
                                        ops::_JobPersistenceOps)
    directory = abspath(String(path))
    isdir(directory) && return directory
    ispath(directory) && throw(ArgumentError(
        "Job artifact parent is not a directory: $(directory)"))

    missing = String[]
    cursor = directory
    while !isdir(cursor)
        ispath(cursor) && throw(ArgumentError(
            "Job artifact ancestor is not a directory: $(cursor)"))
        push!(missing, cursor)
        parent = dirname(cursor)
        parent == cursor && throw(ArgumentError(
            "Cannot find an existing parent for job directory: $(directory)"))
        cursor = parent
    end

    mkpath(directory)
    # Persist each newly-created directory entry from the highest missing
    # ancestor down to the requested job directory. In the ordinary path this
    # synchronizes the job-store root after creating `<store>/<job_id>`.
    for created_directory in reverse(missing)
        _fsync_job_directory_tracked!(dirname(created_directory), ops)
    end
    return directory
end

function _retry_pending_job_store_fsync_with_ops(ops::_JobPersistenceOps)
    pending = lock(JOB_STORE_DURABILITY_LOCK) do
        collect(JOB_STORE_PENDING_DIR_FSYNC)
    end
    all_synced = true
    for (directory, generation) in pending
        try
            ops.fsync_dir!(directory)
            _clear_job_store_dir_fsync_pending!(directory, generation)
        catch err
            all_synced = false
            @warn "Job store directory durability retry failed" directory exception=(err, catch_backtrace())
        end
    end
    return all_synced && lock(JOB_STORE_DURABILITY_LOCK) do
        isempty(JOB_STORE_PENDING_DIR_FSYNC)
    end
end

function _local_job_store_ready_with_ops(ops::_JobPersistenceOps)
    path = local_job_store_dir()
    try
        _ensure_job_directory_with_ops(path, ops)
        isdir(path) && _job_directory_is_writable_posix(path) || return false
        return _retry_pending_job_store_fsync_with_ops(ops)
    catch err
        @warn "Job store readiness check failed" path exception=(err, catch_backtrace())
        return false
    end
end

local_job_store_ready() =
    _local_job_store_ready_with_ops(_DEFAULT_JOB_PERSISTENCE_OPS)

function _job_json_safe_payload(payload)
    safe_payload = json_safe_value(payload)
    if payload isa AbstractDict && haskey(payload, "state_revision")
        raw_revision = payload["state_revision"]
        if raw_revision isa Integer && !(raw_revision isa Bool)
            # The general JSON sanitizer represents Real values as Float64.
            # Job state revisions are an integer storage contract, so preserve
            # this one top-level field without changing numerical result data.
            safe_payload["state_revision"] = Int(raw_revision)
        end
    end
    return safe_payload
end

function _write_job_json_with_ops(path::AbstractString,
                                  payload,
                                  ops::_JobPersistenceOps)
    destination = String(path)
    parent = _ensure_job_directory_with_ops(dirname(destination), ops)
    isdir(destination) &&
        throw(ArgumentError("Job JSON path is a directory: $(destination)"))
    temp_path, temp_io = mktemp(parent; cleanup=false)
    committed = false
    try
        JSON3.pretty(temp_io, _job_json_safe_payload(payload))
        write(temp_io, "\n")
        ops.fsync_file!(temp_io, temp_path)
        close(temp_io)
        ops.replace!(temp_path, destination)
        committed = true

        try
            _fsync_job_directory_tracked!(parent, ops)
        catch err
            # `rename(2)` is the logical commit point. A later directory-fsync
            # failure cannot restore the old path and must not be reported as
            # an uncommitted transition. Readiness remains degraded until a
            # later directory fsync clears the pending generation.
            @warn "Job JSON committed but directory durability is pending" path=destination directory=parent exception=(err, catch_backtrace())
            return _JobPersistenceResult(
                destination,
                true,
                false,
                sprint(showerror, err),
            )
        end
        return _JobPersistenceResult(destination, true, true, nothing)
    catch
        isopen(temp_io) && close(temp_io)
        !committed && isfile(temp_path) && rm(temp_path; force=true)
        rethrow()
    finally
        isopen(temp_io) && close(temp_io)
    end
end

function _require_durable_job_artifact_with_ops!(
    result::_JobPersistenceResult,
    ops::_JobPersistenceOps,
)
    result.committed || error(
        "Job artifact persistence returned without committing or throwing.")
    result.durable && return result.path

    # Artifact publication is stricter than canonical record publication. A
    # record advances in memory at rename even when directory durability needs
    # a readiness retry; an input/result/manifest/status artifact must not let a
    # later commit marker become visible until its own directory is synced.
    directory = dirname(abspath(result.path))
    try
        _fsync_job_directory_tracked!(directory, ops)
    catch err
        throw(ErrorException(
            "Job artifact $(result.path) was committed, but its directory " *
            "durability retry failed: $(sprint(showerror, err))",
        ))
    end

    pending_generation = _pending_job_store_dir_generation(directory)
    pending_generation === nothing || throw(ErrorException(
        "Job artifact $(result.path) was committed, but directory durability " *
        "is still pending at generation $(pending_generation).",
    ))
    return result.path
end

function _write_durable_job_artifact_json_with_ops(
    path::AbstractString,
    payload,
    ops::_JobPersistenceOps,
)
    result = _write_job_json_with_ops(path, payload, ops)
    return _require_durable_job_artifact_with_ops!(result, ops)
end

function _write_job_json(path::AbstractString, payload)
    return _write_durable_job_artifact_json_with_ops(
        path,
        payload,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )
end

function _read_job_json(path::AbstractString)
    isfile(path) || throw(ArgumentError("Missing job artifact: $(path)"))
    return _materialize(JSON3.read(read(path, String)))
end

function _write_job_staging_json_with_ops(path::AbstractString,
                                          payload,
                                          ops::_JobPersistenceOps)
    staging_path = String(path)
    open(staging_path, "w") do io
        JSON3.pretty(io, _job_json_safe_payload(payload))
        write(io, "\n")
        ops.fsync_file!(io, staging_path)
    end
    return staging_path
end

function _write_json_uri_with_ops(uri::AbstractString,
                                  payload,
                                  ops::_JobPersistenceOps)
    if _is_s3_uri(uri)
        temp_path, temp_io = mktemp()
        close(temp_io)
        try
            # This local file is only staging for the S3 upload. Its contents
            # must be fsynced before `aws s3 cp` reads them, but the temporary
            # directory entry is not an application artifact and must not add
            # an unrelated directory to the job-store durability retry set.
            _write_job_staging_json_with_ops(temp_path, payload, ops)
            run(Cmd([_aws_cli(), "s3", "cp", temp_path, String(uri)]))
        finally
            isfile(temp_path) && rm(temp_path; force=true)
        end
        return String(uri)
    else
        return _write_durable_job_artifact_json_with_ops(
            _uri_local_path(uri),
            payload,
            ops,
        )
    end
end

_write_json_uri(uri::AbstractString, payload) =
    _write_json_uri_with_ops(uri, payload, _DEFAULT_JOB_PERSISTENCE_OPS)

function _file_sha256_hex(path::AbstractString)
    return open(path, "r") do io
        bytes2hex(SHA.sha256(io))
    end
end

function _upload_job_result_file_with_ops(uri::AbstractString,
                                          source_path::AbstractString,
                                          sha256_hex::AbstractString,
                                          ops::_JobPersistenceOps)
    if _is_s3_uri(uri)
        run(Cmd([
            _aws_cli(), "s3", "cp", String(source_path), String(uri),
            "--content-type", JOB_RESULT_MEDIA_TYPE,
            "--metadata", "$(JOB_RESULT_SHA256_METADATA_KEY)=$(sha256_hex)",
        ]))
        return String(uri)
    end

    result = _upload_local_job_result_file_with_ops(
        _uri_local_path(uri),
        source_path,
        ops,
    )
    return _require_durable_job_artifact_with_ops!(result, ops)
end

_upload_job_result_file(uri::AbstractString,
                        source_path::AbstractString,
                        sha256_hex::AbstractString) =
    _upload_job_result_file_with_ops(
        uri,
        source_path,
        sha256_hex,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )

function _upload_local_job_result_file_with_ops(destination_path::AbstractString,
                                                source_path::AbstractString,
                                                ops::_JobPersistenceOps)
    destination = String(destination_path)
    source = String(source_path)
    parent = _ensure_job_directory_with_ops(dirname(destination), ops)
    isdir(destination) && throw(ArgumentError(
        "Job result path is a directory: $(destination)"))
    realpath(dirname(abspath(source))) == realpath(parent) || throw(ArgumentError(
        "Atomic local job-result publication requires a same-directory temporary file."))

    # The worker normally produced `source` through `_write_job_json`, but the
    # publication primitive independently establishes the file-fsync-before-
    # rename contract for every caller.
    open(source, "r") do io
        ops.fsync_file!(io, source)
    end
    ops.replace!(source, destination)
    try
        _fsync_job_directory_tracked!(parent, ops)
    catch err
        @warn "Local job result committed but directory durability is pending" path=destination directory=parent exception=(err, catch_backtrace())
        return _JobPersistenceResult(
            destination,
            true,
            false,
            sprint(showerror, err),
        )
    end
    return _JobPersistenceResult(destination, true, true, nothing)
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

function _job_state_revision(record::AbstractDict)
    raw = get(record, "state_revision", nothing)
    raw === nothing && return 0
    job_id = get(record, "job_id", "unknown")
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "Invalid canonical job state_revision for job $(job_id): expected a non-negative integer."))
    revision = try
        Int(raw)
    catch
        throw(ArgumentError(
            "Invalid canonical job state_revision for job $(job_id): expected a non-negative integer."))
    end
    revision >= 0 || throw(ArgumentError(
        "Invalid canonical job state_revision for job $(job_id): expected a non-negative integer."))
    return revision
end

function _next_job_state_revision(record::AbstractDict)
    revision = _job_state_revision(record)
    job_id = get(record, "job_id", "unknown")
    revision < typemax(Int) || throw(ArgumentError(
        "Canonical job state_revision overflow for job $(job_id)."))
    return revision + 1
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
        "state_revision" => _job_state_revision(record),
        "result_available" => Bool(get(record, "result_available", false)),
        "artifacts" => Dict{String, Any}(),
    )

    haskey(record, "batch_job_id") && (out["external_job_id"] = record["batch_job_id"])
    haskey(record, "log_stream_name") && (out["log_stream_name"] = record["log_stream_name"])

    if String(get(record, "executor", "")) == "aws_batch"
        legacy_default_state = isempty(String(get(record, "batch_job_id", ""))) ?
            "legacy_submission_unknown" : "accepted"
        out["submission_state"] = String(get(
            record,
            "submission_state",
            legacy_default_state,
        ))
        haskey(record, "submission_protocol_version") &&
            (out["submission_protocol_version"] =
                String(record["submission_protocol_version"]))

        submission = Dict{String, Any}(
            "state" => out["submission_state"],
            "reconcile_attempts" => Int(get(
                record,
                "submission_reconcile_attempts",
                0,
            )),
        )
        raw_plan = get(record, "submission_plan", nothing)
        plan = _aws_batch_submission_plan(record)
        if plan !== nothing
            submission["identity_source"] = "persisted_submission_plan"
            for key in (
                "job_name",
                "job_queue",
                "job_definition",
                "region",
                "account_id",
            )
                haskey(plan, key) && (submission[key] = deepcopy(plan[key]))
            end
        elseif raw_plan isa AbstractDict
            submission["identity_source"] =
                "unsupported_persisted_submission_plan"
        else
            submission["identity_source"] = "legacy_runtime_region"
        end
        for key in (
            "submission_dispatch_started_at",
            "submission_last_reconcile_at",
            "submission_last_error",
            "submission_conflict_job_ids",
        )
            haskey(record, key) && (submission[key] = deepcopy(record[key]))
        end
        out["submission"] = submission
    end

    for key in ("started_at", "finished_at", "progress", "error", "error_code", "cancel_requested_at")
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

function _persist_job_record_unlocked_with_ops(record::AbstractDict,
                                               ops::_JobPersistenceOps)
    return _write_job_json_with_ops(String(record["record_path"]), record, ops)
end

function _persist_job_record_unlocked(record::AbstractDict)
    return _persist_job_record_unlocked_with_ops(
        record,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )
end

function _persist_job_status_unlocked_with_ops(record::AbstractDict,
                                               ops::_JobPersistenceOps)
    status = _job_public_record(record)
    result = _write_job_json_with_ops(String(record["status_path"]), status, ops)
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
    return result
end

function _persist_job_status_unlocked(record::AbstractDict)
    return _persist_job_status_unlocked_with_ops(
        record,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )
end

# `record.json` is the canonical durable state. `status.json` is a public,
# rebuildable projection: a projection write must never roll back a transition
# whose canonical record was committed successfully.
function _persist_job_status_projection_unlocked_with_ops(
    record::AbstractDict,
    ops::_JobPersistenceOps,
)
    job_id = String(record["job_id"])
    try
        persistence = _persist_job_status_unlocked_with_ops(record, ops)
        persistence.committed || error(
            "Job status projection persistence returned without committing or throwing.")
        # A rename is the logical projection commit. Directory durability is
        # tracked independently by JOB_STORE_PENDING_DIR_FSYNC, so a committed
        # but not-yet-durable projection is not content-dirty.
        _clear_job_projection_dirty!(job_id)
        return persistence
    catch err
        _mark_job_projection_dirty!(job_id)
        @warn "Failed to refresh derived job status projection" job_id=get(record, "job_id", nothing) exception=(err, catch_backtrace())
    end
    return nothing
end

function _persist_job_status_projection_unlocked(record::AbstractDict)
    return _persist_job_status_projection_unlocked_with_ops(
        record,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )
end

function _job_status_projection_matches_unlocked(record::AbstractDict)
    try
        path = String(record["status_path"])
        isfile(path) || return false
        actual = _read_job_json(path)
        actual isa AbstractDict || return false
        actual_revision = _job_state_revision(actual)
        expected_revision = _job_state_revision(record)
        actual_revision == expected_revision || return false
        expected = _job_json_safe_payload(_job_public_record(record))
        return actual == expected
    catch
        return false
    end
end

function _repair_job_status_projection_unlocked_with_ops!(
    record::AbstractDict,
    ops::_JobPersistenceOps;
    inspect_disk::Bool=false,
)
    job_id = String(record["job_id"])
    inspect_disk || _job_projection_is_dirty(job_id) || return true
    if _job_status_projection_matches_unlocked(record)
        _clear_job_projection_dirty!(job_id)
        return true
    end
    return _persist_job_status_projection_unlocked_with_ops(record, ops) !== nothing
end

function _repair_job_status_projection_unlocked!(record::AbstractDict;
                                                  inspect_disk::Bool=false)
    return _repair_job_status_projection_unlocked_with_ops!(
        record,
        _DEFAULT_JOB_PERSISTENCE_OPS;
        inspect_disk=inspect_disk,
    )
end

function _job_record_locked(job_id::AbstractString;
                            read_record::Function=_read_job_json)
    id = String(job_id)
    record = _job_cache_get(id)
    loaded_from_disk = false
    if record === nothing
        path = _job_record_path(id)
        isfile(path) || return nothing
        record = read_record(path)
        record isa AbstractDict || throw(ArgumentError(
            "Invalid canonical job record for $(id): expected a JSON object."))
        canonical_id = get(record, "job_id", nothing)
        canonical_id isa AbstractString && String(canonical_id) == id ||
            throw(ArgumentError(
                "Invalid canonical job record for $(id): job_id does not match its directory."))
        _job_state_revision(record)
        _recover_interrupted_local_job_unlocked!(record)
        _recover_interrupted_aws_submission_unlocked!(record)
        _job_cache_publish!(id, record)
        loaded_from_disk = true
    end
    if loaded_from_disk || _job_projection_is_dirty(id)
        _repair_job_status_projection_unlocked!(
            record;
            inspect_disk=loaded_from_disk,
        )
    end
    return record
end

function _recover_interrupted_aws_submission_unlocked!(record::AbstractDict)
    String(get(record, "executor", "")) == "aws_batch" || return false
    String(get(record, "status", "")) == "queued" || return false
    isempty(String(get(record, "batch_job_id", ""))) || return false
    String(get(record, "submission_state", "")) == "prepared" || return false
    _aws_batch_submission_plan(record) === nothing && return false
    job_id = String(get(record, "job_id", ""))
    active_submission = lock(JOBS_LOCK) do
        job_id in AWS_BATCH_INITIAL_SUBMISSIONS
    end
    active_submission && return false

    return _transition_job_record_unlocked!(
        record,
        "failed";
        expected=("queued",),
        submission_state="interrupted_before_dispatch",
        finished_at=_now_iso_timestamp(),
        result_available=false,
        error_code="aws_submission_interrupted_before_dispatch",
        error="AWS Batch submission was interrupted before the durable dispatch boundary; no SubmitJob attempt was issued.",
        progress=Dict(
            "message" => "AWS Batch submission interrupted before dispatch",
            "aws_status" => "NOT_SUBMITTED",
        ),
    )
end

function _local_job_execution_active_unlocked(job_id::AbstractString)
    id = String(job_id)
    return lock(JOBS_LOCK) do
        task = get(JOB_TASKS, id, nothing)
        task_active = task !== nothing && !istaskdone(task)
        task_active || haskey(LOCAL_JOB_CANCEL_TOKENS, id) ||
            id in LOCAL_JOB_ADMISSIONS
    end
end

# Local workers and their queue exist only in this Julia process. A nonterminal
# local record loaded from disk therefore cannot make progress after a process
# restart. Settle it exactly once through the ordinary durable transition path
# so `record.json` remains canonical and `status.json` is rebuilt from it.
# Records already present in `JOBS` never pass through this recovery path, and
# the worker/token guard protects a current-process task if its in-memory record
# was removed independently.
function _recover_interrupted_local_job_unlocked!(record::AbstractDict)
    String(get(record, "executor", "")) == "local_async" || return false
    status = String(get(record, "status", ""))
    status in ("queued", "running", "cancel_requested") || return false

    job_id = String(get(record, "job_id", ""))
    isempty(job_id) && return false
    _local_job_execution_active_unlocked(job_id) && return false

    if status == "cancel_requested"
        return _transition_job_record_unlocked!(
            record,
            "cancelled";
            expected=("cancel_requested",),
            finished_at=_now_iso_timestamp(),
            result_available=false,
            progress=Dict("message" => "Cancelled after backend restart"),
        )
    end

    return _transition_job_record_unlocked!(
        record,
        "failed";
        expected=(status,),
        finished_at=_now_iso_timestamp(),
        result_available=false,
        error_code=LOCAL_JOB_RESTART_ERROR_CODE,
        error=LOCAL_JOB_RESTART_ERROR_MESSAGE,
        progress=Dict("message" => "Local job interrupted by backend restart"),
    )
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
        key_text = String(key)
        key_text == "status" && throw(ArgumentError("Pass the target status positionally."))
        key_text == "state_revision" && throw(ArgumentError(
            "state_revision is owned by canonical job persistence."))
        record[key_text] = value
    end
    record["updated_at"] = _now_iso_timestamp()
    return true
end

function _commit_job_candidate_unlocked_with_ops!(record::AbstractDict,
                                                  candidate::AbstractDict,
                                                  ops::_JobPersistenceOps)
    # Persist the canonical candidate before exposing it through the shared
    # in-memory dictionary. A pre-rename failure leaves both views at the
    # previous revision. Once rename commits, memory advances even when the
    # following directory fsync needs a readiness-driven retry.
    candidate["state_revision"] = _next_job_state_revision(record)
    persistence = _persist_job_record_unlocked_with_ops(candidate, ops)
    persistence.committed || error(
        "Canonical job persistence returned without committing or throwing.")
    empty!(record)
    merge!(record, candidate)
    _job_cache_publish!(String(record["job_id"]), record)
    _persist_job_status_projection_unlocked_with_ops(record, ops)
    return (record=record, persistence=persistence)
end

function _commit_job_candidate_unlocked!(record::AbstractDict, candidate::AbstractDict)
    committed = _commit_job_candidate_unlocked_with_ops!(
        record,
        candidate,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )
    return committed.record
end

function _transition_job_record_unlocked_with_ops!(record::AbstractDict,
                                                   target_status::AbstractString,
                                                   ops::_JobPersistenceOps;
                                                   expected=nothing,
                                                   updates...)
    candidate = deepcopy(record)
    applied = _apply_job_transition_unlocked!(
        candidate,
        target_status;
        expected=expected,
        updates...,
    )
    applied || return (applied=false, persistence=nothing)
    committed = _commit_job_candidate_unlocked_with_ops!(record, candidate, ops)
    return (applied=true, persistence=committed.persistence)
end

function _transition_job_record_unlocked!(record::AbstractDict,
                                          target_status::AbstractString;
                                          expected=nothing,
                                          updates...)
    transition = _transition_job_record_unlocked_with_ops!(
        record,
        target_status,
        _DEFAULT_JOB_PERSISTENCE_OPS;
        expected=expected,
        updates...,
    )
    return transition.applied
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
    _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
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
    _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
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
    _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
        record === nothing && return nothing
        return _job_snapshot(record)
    end
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
    throw(ArgumentError("Unknown job_id: $(job_id)"))
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
    result isa AbstractDict && haskey(result, "artifact") && return result
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
    elseif kind == "rop_shape_optimize"
        return optimize_rop_shape_request(
            spec; synchronous=false, cancel_check=cancel_check)
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

function _job_result_identity(result,
                              job_id::AbstractString,
                              kind::AbstractString,
                              expected_config_hash::AbstractString)
    result isa AbstractDict ||
        throw(ArgumentError("Asynchronous job result must be a JSON object."))
    haskey(result, "artifact") ||
        throw(ArgumentError("Asynchronous job result is missing sibling `artifact` metadata."))
    payload_key_count = count(key -> String(key) != "artifact", keys(result))
    payload_key_count > 0 || throw(ArgumentError(
        "Asynchronous job result does not contain a computed result payload."))

    metadata = result["artifact"]
    _validate_result_artifact_metadata(metadata)
    actual_kind = String(metadata["kind"])
    actual_kind == String(kind) || throw(ArgumentError(
        "Result artifact kind mismatch: expected $(kind), got $(actual_kind)."))
    algorithm = metadata["algorithm"]
    actual_config_hash = get(algorithm, "config_hash", nothing)
    actual_config_hash isa AbstractString || throw(ArgumentError(
        "Result artifact `algorithm.config_hash` is required for an asynchronous job result."))
    String(actual_config_hash) == String(expected_config_hash) || throw(ArgumentError(
        "Result artifact config identity does not match the submitted job spec."))

    return Dict{String, Any}(
        "artifact_schema_version" => String(metadata["artifact_schema_version"]),
        "kind" => actual_kind,
        "algorithm_name" => String(algorithm["name"]),
        "algorithm_version" => String(algorithm["version"]),
        "config_hash" => String(actual_config_hash),
        "artifact_metadata_hash" => _canonical_hash(metadata),
        "payload_key_count" => payload_key_count,
        "job_id" => String(job_id),
    )
end

function _job_result_manifest_payload(result,
                                      job_id::AbstractString,
                                      kind::AbstractString,
                                      expected_config_hash::AbstractString,
                                      result_uri::AbstractString,
                                      content_length::Integer,
                                      sha256_hex::AbstractString)
    identity = _job_result_identity(
        result, job_id, kind, expected_config_hash)
    return Dict{String, Any}(
        "schema_version" => JOB_RESULT_PROTOCOL_VERSION,
        "job_id" => identity["job_id"],
        "kind" => identity["kind"],
        "created_at" => _job_result_manifest_timestamp(),
        "artifact_identity" => Dict{String, Any}(
            "artifact_schema_version" => identity["artifact_schema_version"],
            "algorithm_name" => identity["algorithm_name"],
            "algorithm_version" => identity["algorithm_version"],
            "config_hash" => identity["config_hash"],
            "artifact_metadata_hash" => identity["artifact_metadata_hash"],
        ),
        "result" => Dict{String, Any}(
            "uri" => String(result_uri),
            "content_length" => Int(content_length),
            "sha256" => String(sha256_hex),
            "media_type" => JOB_RESULT_MEDIA_TYPE,
            "payload_key_count" => Int(identity["payload_key_count"]),
        ),
    )
end

function _publish_job_result_with_manifest_with_ops(
    result,
    job_id::AbstractString,
    kind::AbstractString,
    expected_config_hash::AbstractString,
    result_uri::AbstractString,
    manifest_uri::AbstractString,
    ops::_JobPersistenceOps,
)
    occursin(r"^[0-9a-f]{64}$", String(expected_config_hash)) ||
        throw(ArgumentError("Expected result artifact config hash must be 64 lowercase hex characters."))
    String(result_uri) == String(manifest_uri) &&
        throw(ArgumentError("Result and result-manifest URIs must be distinct."))

    # Validate the in-memory result before writing any externally visible
    # object. The result is serialized exactly once; its byte identity is then
    # recorded in the small manifest published last as the commit marker.
    _job_result_identity(result, job_id, kind, expected_config_hash)
    local_result = !_is_s3_uri(result_uri)
    temp_parent = local_result ?
        _ensure_job_directory_with_ops(
            dirname(_uri_local_path(result_uri)),
            ops,
        ) :
        tempdir()
    temp_path, temp_io = mktemp(temp_parent; cleanup=false)
    close(temp_io)
    try
        # The staging entry is never a published artifact. Its file contents
        # are synced here; local publication later renames it into place and
        # syncs the destination directory, while S3 publication uploads it.
        _write_job_staging_json_with_ops(temp_path, result, ops)
        content_length = filesize(temp_path)
        content_length > 0 || throw(ArgumentError("Serialized job result is empty."))
        sha256_hex = _file_sha256_hex(temp_path)
        manifest = _job_result_manifest_payload(
            result,
            job_id,
            kind,
            expected_config_hash,
            result_uri,
            content_length,
            sha256_hex,
        )

        _upload_job_result_file_with_ops(
            result_uri,
            temp_path,
            sha256_hex,
            ops,
        )
        _write_json_uri_with_ops(manifest_uri, manifest, ops)
        return manifest
    finally
        isfile(temp_path) && rm(temp_path; force=true)
    end
end

_publish_job_result_with_manifest(result,
                                  job_id::AbstractString,
                                  kind::AbstractString,
                                  expected_config_hash::AbstractString,
                                  result_uri::AbstractString,
                                  manifest_uri::AbstractString) =
    _publish_job_result_with_manifest_with_ops(
        result,
        job_id,
        kind,
        expected_config_hash,
        result_uri,
        manifest_uri,
        _DEFAULT_JOB_PERSISTENCE_OPS,
    )

function _run_biocircuits_job_payload_with_ops(
    payload,
    ops::_JobPersistenceOps;
    status_uri=nothing,
    result_uri=nothing,
)
    payload = _materialize(payload)
    job_id = String(_raw_get(payload, :job_id, string(rand(UInt128), base=16, pad=32)))
    kind = String(_raw_get(payload, :kind, ""))
    kind in LOCAL_JOB_KINDS || throw(ArgumentError("Unsupported job kind: $(kind)"))
    executor = String(_raw_get(payload, :executor, "worker"))
    spec = _materialize(_raw_get(payload, :spec, Dict{String, Any}()))
    artifacts = _raw_get(payload, :artifacts, Dict{String, Any}())
    status_uri = status_uri === nothing ? _raw_get(artifacts, :status, nothing) : status_uri
    result_uri = result_uri === nothing ? _raw_get(artifacts, :result, nothing) : result_uri
    result_protocol_version = _raw_get(payload, :result_protocol_version, nothing)
    result_manifest_uri = _raw_get(artifacts, :result_manifest, nothing)
    expected_config_hash = _raw_get(payload, :expected_artifact_config_hash, nothing)
    status_uri === nothing && throw(ArgumentError("Batch job payload must include a status artifact URI."))
    result_uri === nothing && throw(ArgumentError("Batch job payload must include a result artifact URI."))

    _write_json_uri_with_ops(String(status_uri), _job_status_payload(
        job_id,
        kind,
        executor,
        "running";
        started_at=_now_iso_timestamp(),
        progress=Dict("message" => "Running in worker"),
    ), ops)

    try
        result = _execute_local_job(kind, spec)
        if result_protocol_version === nothing
            # Compatibility for payloads created before the manifest protocol.
            _write_json_uri_with_ops(String(result_uri), result, ops)
        else
            String(result_protocol_version) == JOB_RESULT_PROTOCOL_VERSION ||
                throw(ArgumentError(
                    "Unsupported result protocol version: $(result_protocol_version)."))
            result_manifest_uri === nothing && throw(ArgumentError(
                "Manifest-protocol job payload must include `artifacts.result_manifest`."))
            expected_config_hash isa AbstractString || throw(ArgumentError(
                "Manifest-protocol job payload must include `expected_artifact_config_hash`."))
            _publish_job_result_with_manifest_with_ops(
                result,
                job_id,
                kind,
                String(expected_config_hash),
                String(result_uri),
                String(result_manifest_uri),
                ops,
            )
        end
        _write_json_uri_with_ops(String(status_uri), _job_status_payload(
            job_id,
            kind,
            executor,
            "succeeded";
            finished_at=_now_iso_timestamp(),
            result_available=true,
            progress=Dict("message" => "Completed"),
        ), ops)
        return result
    catch err
        _write_json_uri_with_ops(String(status_uri), _job_status_payload(
            job_id,
            kind,
            executor,
            "failed";
            finished_at=_now_iso_timestamp(),
            result_available=false,
            error=sprint(showerror, err, catch_backtrace()),
            progress=Dict("message" => "Failed"),
        ), ops)
        rethrow()
    end
end

run_biocircuits_job_payload(payload; status_uri=nothing, result_uri=nothing) =
    _run_biocircuits_job_payload_with_ops(
        payload,
        _DEFAULT_JOB_PERSISTENCE_OPS;
        status_uri=status_uri,
        result_uri=result_uri,
    )

function run_biocircuits_job_from_uri(input_uri::AbstractString; status_uri=nothing, result_uri=nothing)
    payload = _read_json_uri(input_uri)
    return run_biocircuits_job_payload(payload; status_uri=status_uri, result_uri=result_uri)
end

function _finish_local_job!(job_id::AbstractString;
                            succeeded::Bool,
                            error=nothing)
    _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
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

function _run_admitted_local_job!(job_id::String,
                                  kind::String,
                                  spec,
                                  token::LocalJobCancelToken,
                                  semaphore::Base.Semaphore;
                                  start_gate::Union{Nothing, Channel{Nothing}}=nothing,
                                  run_job!::Function=_run_local_job!)
    acquired = false
    try
        start_gate === nothing || take!(start_gate)
        # Waiting jobs remain `queued`; only `_run_local_job!`, after a permit
        # is acquired, may publish the queued -> running transition.
        Base.acquire(semaphore)
        acquired = true
        return run_job!(job_id, kind, spec, token)
    finally
        try
            acquired && Base.release(semaphore)
        finally
            # `_run_local_job!` normally performs the registration cleanup too.
            # Repeating it here covers failures before that function starts and
            # keeps admission release owned by exactly one task-level finally.
            lock(JOBS_LOCK) do
                delete!(JOB_TASKS, job_id)
                delete!(LOCAL_JOB_CANCEL_TOKENS, job_id)
                job_id in LOCAL_JOB_ADMISSIONS ||
                    error("Local job $(job_id) does not own an admission reservation.")
                delete!(LOCAL_JOB_ADMISSIONS, job_id)
            end
        end
    end
end

function _job_execution_mode(raw)
    execution = _raw_get(raw, :execution, Dict{String, Any}())
    return lowercase(String(_raw_get(execution, :mode, "local_async")))
end

function _aws_cli_json(args::Vector{<:AbstractString})
    output = read(Cmd([_aws_cli(); String.(args)]), String)
    return _materialize(JSON3.read(output))
end

function _aws_cli_json_single_attempt(args::Vector{<:AbstractString})
    command = addenv(
        Cmd([_aws_cli(); String.(args)]),
        "AWS_MAX_ATTEMPTS" => "1",
    )
    output = read(command, String)
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

function _run_command_captured(cmd::Cmd)
    stdout_buffer = IOBuffer()
    stderr_buffer = IOBuffer()
    process = run(pipeline(ignorestatus(cmd); stdout=stdout_buffer, stderr=stderr_buffer))
    return (
        success=success(process),
        stdout=String(take!(stdout_buffer)),
        stderr=String(take!(stderr_buffer)),
    )
end

function _s3_head_definitively_missing(stdout::AbstractString, stderr::AbstractString)
    diagnostic = lowercase(String(stdout) * "\n" * String(stderr))
    return occursin(
        r"an error occurred \((404|nosuchkey|notfound)\) when calling (the )?headobject operation",
        diagnostic,
    )
end

# A failed HeadObject call is not equivalent to a missing object. Only an
# explicit not-found response is terminal; permissions, unavailable CLI,
# network, throttling, malformed CLI output, and other operational failures
# remain retryable.
function _head_artifact(uri::AbstractString)
    text = strip(String(uri))
    isempty(text) && return (
        status=:invalid,
        detail="Result artifact URI is empty.",
        content_length=nothing,
        content_type=nothing,
        metadata=Dict{String, Any}(),
    )

    if !_is_s3_uri(text)
        path = _uri_local_path(text)
        try
            isfile(path) || return (
                status=:missing,
                detail="Local result artifact does not exist.",
                content_length=nothing,
                content_type=nothing,
                metadata=Dict{String, Any}(),
            )
            return (
                status=:present,
                detail="",
                content_length=filesize(path),
                content_type=JOB_RESULT_MEDIA_TYPE,
                metadata=Dict{String, Any}(),
            )
        catch err
            return (
                status=:retryable_error,
                detail=sprint(showerror, err),
                content_length=nothing,
                content_type=nothing,
                metadata=Dict{String, Any}(),
            )
        end
    end

    parsed = _s3_uri_bucket_key(text)
    parsed === nothing && return (
        status=:invalid,
        detail="Result artifact S3 URI must include a bucket and object key.",
        content_length=nothing,
        content_type=nothing,
        metadata=Dict{String, Any}(),
    )
    bucket, key = parsed
    probe = try
        _run_command_captured(Cmd([
            _aws_cli(), "s3api", "head-object",
            "--bucket", String(bucket),
            "--key", String(key),
        ]))
    catch err
        return (
            status=:retryable_error,
            detail=sprint(showerror, err),
            content_length=nothing,
            content_type=nothing,
            metadata=Dict{String, Any}(),
        )
    end
    if probe.success
        payload = try
            _materialize(JSON3.read(probe.stdout))
        catch err
            return (
                status=:retryable_error,
                detail="S3 HeadObject returned invalid JSON: $(sprint(showerror, err))",
                content_length=nothing,
                content_type=nothing,
                metadata=Dict{String, Any}(),
            )
        end
        payload isa AbstractDict || return (
            status=:retryable_error,
            detail="S3 HeadObject response must be a JSON object.",
            content_length=nothing,
            content_type=nothing,
            metadata=Dict{String, Any}(),
        )
        content_length = get(payload, "ContentLength", nothing)
        metadata_raw = get(payload, "Metadata", Dict{String, Any}())
        metadata = metadata_raw isa AbstractDict ?
            Dict{String, Any}(String(k) => v for (k, v) in pairs(metadata_raw)) :
            Dict{String, Any}()
        content_type_raw = get(payload, "ContentType", nothing)
        return (
            status=:present,
            detail="",
            content_length=content_length,
            content_type=content_type_raw === nothing ? nothing : String(content_type_raw),
            metadata=metadata,
        )
    end
    _s3_head_definitively_missing(probe.stdout, probe.stderr) && return (
        status=:missing,
        detail="S3 result artifact does not exist.",
        content_length=nothing,
        content_type=nothing,
        metadata=Dict{String, Any}(),
    )
    diagnostic = strip(isempty(probe.stderr) ? probe.stdout : probe.stderr)
    return (
        status=:retryable_error,
        detail=isempty(diagnostic) ? "S3 HeadObject failed without a not-found response." : diagnostic,
        content_length=nothing,
        content_type=nothing,
        metadata=Dict{String, Any}(),
    )
end

function _probe_artifact_presence(uri::AbstractString)
    head = _head_artifact(uri)
    return (status=head.status, detail=head.detail)
end

function _read_artifact_bytes(uri::AbstractString; max_bytes=nothing)
    text = String(uri)
    if !_is_s3_uri(text)
        try
            path = _uri_local_path(text)
            if max_bytes !== nothing && filesize(path) > Int(max_bytes)
                return (
                    status=:too_large,
                    bytes=UInt8[],
                    detail="Artifact exceeds the $(Int(max_bytes))-byte read limit.",
                )
            end
            bytes = read(path)
            if max_bytes !== nothing && length(bytes) > Int(max_bytes)
                return (
                    status=:too_large,
                    bytes=UInt8[],
                    detail="Artifact exceeds the $(Int(max_bytes))-byte read limit.",
                )
            end
            return (status=:ok, bytes=bytes, detail="")
        catch err
            return (status=:retryable_error, bytes=UInt8[], detail=sprint(showerror, err))
        end
    end

    temp_path, temp_io = mktemp()
    close(temp_io)
    try
        download = try
            _run_command_captured(Cmd([_aws_cli(), "s3", "cp", text, temp_path]))
        catch err
            return (status=:retryable_error, bytes=UInt8[], detail=sprint(showerror, err))
        end
        if !download.success
            diagnostic = strip(isempty(download.stderr) ? download.stdout : download.stderr)
            return (
                status=:retryable_error,
                bytes=UInt8[],
                detail=isempty(diagnostic) ? "S3 result download failed." : diagnostic,
            )
        end
        try
            if max_bytes !== nothing && filesize(temp_path) > Int(max_bytes)
                return (
                    status=:too_large,
                    bytes=UInt8[],
                    detail="Artifact exceeds the $(Int(max_bytes))-byte read limit.",
                )
            end
            bytes = read(temp_path)
            if max_bytes !== nothing && length(bytes) > Int(max_bytes)
                return (
                    status=:too_large,
                    bytes=UInt8[],
                    detail="Artifact exceeds the $(Int(max_bytes))-byte read limit.",
                )
            end
            return (status=:ok, bytes=bytes, detail="")
        catch err
            return (status=:retryable_error, bytes=UInt8[], detail=sprint(showerror, err))
        end
    finally
        isfile(temp_path) && rm(temp_path; force=true)
    end
end

function _job_artifact_config(kind::AbstractString, spec)
    if String(kind) == "rop_shape_optimize"
        return _rop_shape_normalize_request(spec; synchronous=false).normalized
    end
    return spec
end

# Resolve the exact config identity once, before a job is persisted or handed
# to a worker. ROP shape legacy/partial NetworkIR inputs may acquire default
# provenance while they are normalized; repeating that conversion later can
# produce a different timestamp and therefore a different artifact hash.
function _prepare_job_spec_and_artifact_identity(kind::AbstractString, raw_spec)
    submitted_spec = _materialize(raw_spec)
    artifact_config = _job_artifact_config(kind, submitted_spec)
    worker_spec = String(kind) == "rop_shape_optimize" ?
        Dict{String, Any}(_materialize(artifact_config)) : submitted_spec
    return (
        spec=worker_spec,
        expected_artifact_config_hash=_canonical_hash(artifact_config),
    )
end

function _record_expected_artifact_config_hash(record::AbstractDict;
                                               allow_legacy_derivation::Bool)
    if haskey(record, "expected_artifact_config_hash")
        stored_hash = get(record, "expected_artifact_config_hash", nothing)
        if !(stored_hash isa AbstractString) ||
           !occursin(r"^[0-9a-f]{64}$", String(stored_hash))
            throw(ArgumentError(
                "Persisted expected result artifact config identity is invalid."))
        end
        return String(stored_hash)
    end
    allow_legacy_derivation || throw(ArgumentError(
        "Manifest-protocol job record is missing its expected artifact config identity."))
    expected_kind = String(get(record, "kind", ""))
    return _canonical_hash(_job_artifact_config(
        expected_kind,
        get(record, "spec", Dict{String, Any}()),
    ))
end

function _manifest_exact_keys(value, required::Set{String}, path::AbstractString)
    value isa AbstractDict ||
        throw(ArgumentError("$(path) must be an object."))
    actual = Set(String(key) for key in keys(value))
    actual == required || throw(ArgumentError(
        "$(path) fields must be exactly $(sort!(collect(required))); got $(sort!(collect(actual)))."))
    return value
end


function _validate_job_result_manifest(manifest, record::AbstractDict)
    _manifest_exact_keys(manifest, Set([
        "schema_version", "job_id", "kind", "created_at",
        "artifact_identity", "result",
    ]), "Result manifest")

    schema_version = get(manifest, "schema_version", nothing)
    schema_version isa AbstractString ||
        throw(ArgumentError("Result manifest `schema_version` must be a string."))
    String(schema_version) == JOB_RESULT_PROTOCOL_VERSION || throw(ArgumentError(
        "Unsupported result manifest schema version: $(schema_version)."))

    expected_job_id = String(get(record, "job_id", ""))
    job_id = get(manifest, "job_id", nothing)
    job_id isa AbstractString && String(job_id) == expected_job_id ||
        throw(ArgumentError("Result manifest job identity does not match the submitted job."))
    expected_kind = String(get(record, "kind", ""))
    kind = get(manifest, "kind", nothing)
    kind isa AbstractString && String(kind) == expected_kind ||
        throw(ArgumentError("Result manifest kind does not match the submitted job."))
    created_at = get(manifest, "created_at", nothing)
    _is_valid_job_result_manifest_timestamp(created_at) || throw(ArgumentError(
        "Result manifest `created_at` must be a valid UTC date-time in " *
        "YYYY-MM-DDTHH:MM:SSZ format."))

    artifact_identity = _manifest_exact_keys(
        manifest["artifact_identity"],
        Set([
            "artifact_schema_version", "algorithm_name", "algorithm_version",
            "config_hash", "artifact_metadata_hash",
        ]),
        "Result manifest `artifact_identity`",
    )
    artifact_schema_version = get(artifact_identity, "artifact_schema_version", nothing)
    artifact_schema_version isa AbstractString &&
        String(artifact_schema_version) == RESULT_ARTIFACT_SCHEMA_VERSION ||
        throw(ArgumentError("Result manifest artifact schema version is unsupported."))
    for key in ("algorithm_name", "algorithm_version")
        value = get(artifact_identity, key, nothing)
        value isa AbstractString && !isempty(String(value)) || throw(ArgumentError(
            "Result manifest `artifact_identity.$(key)` must be a non-empty string."))
    end
    expected_config_hash = _record_expected_artifact_config_hash(
        record; allow_legacy_derivation=false)
    config_hash = get(artifact_identity, "config_hash", nothing)
    config_hash isa AbstractString && String(config_hash) == expected_config_hash ||
        throw(ArgumentError(
            "Result manifest config identity does not match the submitted job spec."))
    metadata_hash = get(artifact_identity, "artifact_metadata_hash", nothing)
    metadata_hash isa AbstractString &&
        occursin(r"^[0-9a-f]{64}$", String(metadata_hash)) ||
        throw(ArgumentError(
            "Result manifest `artifact_identity.artifact_metadata_hash` must be 64 lowercase hex characters."))

    result = _manifest_exact_keys(
        manifest["result"],
        Set(["uri", "content_length", "sha256", "media_type", "payload_key_count"]),
        "Result manifest `result`",
    )
    expected_result_uri = String(get(record, "result_uri", ""))
    result_uri = get(result, "uri", nothing)
    result_uri isa AbstractString && String(result_uri) == expected_result_uri ||
        throw(ArgumentError("Result manifest URI does not match the submitted job result URI."))
    content_length = get(result, "content_length", nothing)
    content_length isa Integer && !(content_length isa Bool) && content_length > 0 ||
        throw(ArgumentError("Result manifest `result.content_length` must be a positive integer."))
    sha256_hex = get(result, "sha256", nothing)
    sha256_hex isa AbstractString &&
        occursin(r"^[0-9a-f]{64}$", String(sha256_hex)) ||
        throw(ArgumentError(
            "Result manifest `result.sha256` must be 64 lowercase hex characters."))
    media_type = get(result, "media_type", nothing)
    media_type isa AbstractString && String(media_type) == JOB_RESULT_MEDIA_TYPE ||
        throw(ArgumentError("Result manifest media type must be $(JOB_RESULT_MEDIA_TYPE)."))
    payload_key_count = get(result, "payload_key_count", nothing)
    payload_key_count isa Integer && !(payload_key_count isa Bool) && payload_key_count > 0 ||
        throw(ArgumentError(
            "Result manifest `result.payload_key_count` must be a positive integer."))

    return (
        result_uri=expected_result_uri,
        content_length=Int(content_length),
        sha256=String(sha256_hex),
    )
end

function _load_job_result_manifest(uri::AbstractString)
    head = _head_artifact(uri)
    head.status == :present || return (
        status=head.status,
        manifest=nothing,
        error=head.status == :missing ?
            "AWS Batch job exited successfully but result manifest is missing: $(uri)" :
            head.detail,
    )
    length_value = head.content_length
    if !(length_value isa Integer) || length_value isa Bool || length_value < 0
        return (
            status=:retryable_error,
            manifest=nothing,
            error="Result manifest HeadObject response has no valid ContentLength.",
        )
    end
    if length_value > JOB_RESULT_MANIFEST_MAX_BYTES
        return (
            status=:invalid,
            manifest=nothing,
            error="Result manifest exceeds the $(JOB_RESULT_MANIFEST_MAX_BYTES)-byte limit.",
        )
    end

    loaded = _read_artifact_bytes(uri; max_bytes=JOB_RESULT_MANIFEST_MAX_BYTES)
    if loaded.status == :too_large
        return (status=:invalid, manifest=nothing, error=loaded.detail)
    elseif loaded.status != :ok
        return (status=:retryable_error, manifest=nothing, error=loaded.detail)
    end
    isvalid(String, loaded.bytes) || return (
        status=:invalid,
        manifest=nothing,
        error="Result manifest is not valid UTF-8.",
    )
    text = String(loaded.bytes)
    isempty(strip(text)) && return (
        status=:invalid,
        manifest=nothing,
        error="Result manifest is empty.",
    )
    manifest = try
        _materialize(JSON3.read(text))
    catch err
        return (
            status=:invalid,
            manifest=nothing,
            error="Result manifest is not valid JSON: $(sprint(showerror, err))",
        )
    end
    return (status=:ok, manifest=manifest, error="")
end

function _verify_manifest_job_result_artifact(record::AbstractDict)
    manifest_uri_raw = get(record, "result_manifest_uri", nothing)
    manifest_uri_raw isa AbstractString || return (
        status=:invalid,
        error="Manifest-protocol job record is missing `result_manifest_uri`.",
        verification_mode=:manifest,
    )
    loaded = _load_job_result_manifest(String(manifest_uri_raw))
    loaded.status == :ok || return (
        status=loaded.status,
        error=loaded.error,
        verification_mode=:manifest,
    )
    descriptor = try
        _validate_job_result_manifest(loaded.manifest, record)
    catch err
        return (
            status=:invalid,
            error=sprint(showerror, err),
            verification_mode=:manifest,
        )
    end

    head = _head_artifact(descriptor.result_uri)
    if head.status == :missing
        return (
            status=:missing,
            error="Committed result manifest points to a missing result artifact: $(descriptor.result_uri)",
            verification_mode=:manifest,
        )
    elseif head.status == :invalid
        return (status=:invalid, error=head.detail, verification_mode=:manifest)
    elseif head.status == :retryable_error
        return (status=:retryable_error, error=head.detail, verification_mode=:manifest)
    end

    actual_length = head.content_length
    if !(actual_length isa Integer) || actual_length isa Bool || actual_length < 0
        return (
            status=:retryable_error,
            error="Result HeadObject response has no valid ContentLength.",
            verification_mode=:manifest,
        )
    end
    Int(actual_length) == descriptor.content_length || return (
        status=:invalid,
        error="Result artifact byte length does not match its committed manifest.",
        verification_mode=:manifest,
    )

    actual_sha256 = if _is_s3_uri(descriptor.result_uri)
        head.content_type == JOB_RESULT_MEDIA_TYPE || return (
            status=:invalid,
            error="Result artifact content type does not match its committed manifest.",
            verification_mode=:manifest,
        )
        metadata_value = get(head.metadata, JOB_RESULT_SHA256_METADATA_KEY, nothing)
        metadata_value isa AbstractString &&
            occursin(r"^[0-9a-fA-F]{64}$", String(metadata_value)) || return (
            status=:invalid,
            error="Result artifact is missing its committed SHA-256 object metadata.",
            verification_mode=:manifest,
        )
        lowercase(String(metadata_value))
    else
        try
            _file_sha256_hex(_uri_local_path(descriptor.result_uri))
        catch err
            return (
                status=:retryable_error,
                error="Cannot hash local result artifact: $(sprint(showerror, err))",
                verification_mode=:manifest,
            )
        end
    end
    actual_sha256 == descriptor.sha256 || return (
        status=:invalid,
        error="Result artifact SHA-256 does not match its committed manifest.",
        verification_mode=:manifest,
    )
    return (status=:valid, error="", verification_mode=:manifest)
end

function _verify_legacy_job_result_artifact(record::AbstractDict)
    result_uri = String(get(record, "result_uri", ""))
    presence = _probe_artifact_presence(result_uri)
    if presence.status == :missing
        return (
            status=:missing,
            error="AWS Batch job exited successfully but result artifact is missing: $(result_uri)",
        )
    elseif presence.status == :invalid
        return (status=:invalid, error=presence.detail)
    elseif presence.status == :retryable_error
        return (status=:retryable_error, error=presence.detail)
    end

    loaded = _read_artifact_bytes(result_uri)
    loaded.status == :ok || return (status=:retryable_error, error=loaded.detail)
    isvalid(String, loaded.bytes) || return (
        status=:invalid,
        error="Result artifact is not valid UTF-8.",
    )
    result_text = try
        String(loaded.bytes)
    catch err
        return (
            status=:invalid,
            error="Result artifact is not valid UTF-8: $(sprint(showerror, err))",
        )
    end
    isempty(strip(result_text)) &&
        return (status=:invalid, error="Result artifact is empty: $(result_uri)")

    result = try
        _materialize(JSON3.read(result_text))
    catch err
        return (
            status=:invalid,
            error="Result artifact is not valid JSON: $(sprint(showerror, err))",
        )
    end
    result isa AbstractDict ||
        return (status=:invalid, error="Result artifact JSON must be an object.")
    haskey(result, "artifact") ||
        return (status=:invalid, error="Result artifact JSON is missing sibling `artifact` metadata.")
    any(key -> String(key) != "artifact", keys(result)) || return (
        status=:invalid,
        error="Result artifact JSON does not contain a computed result payload.",
    )

    metadata = result["artifact"]
    try
        _validate_result_artifact_metadata(metadata)
    catch err
        return (status=:invalid, error=sprint(showerror, err))
    end

    expected_kind = String(get(record, "kind", ""))
    actual_kind = String(metadata["kind"])
    actual_kind == expected_kind || return (
        status=:invalid,
        error="Result artifact kind mismatch: expected $(expected_kind), got $(actual_kind).",
    )

    expected_config_hash = try
        _record_expected_artifact_config_hash(
            record; allow_legacy_derivation=true)
    catch err
        return (status=:invalid, error=sprint(showerror, err), verification_mode=:legacy_inline)
    end
    algorithm = metadata["algorithm"]
    actual_config_hash = get(algorithm, "config_hash", nothing)
    actual_config_hash isa AbstractString || return (
        status=:invalid,
        error="Result artifact `algorithm.config_hash` is required for an asynchronous job result.",
    )
    String(actual_config_hash) == expected_config_hash || return (
        status=:invalid,
        error="Result artifact config identity does not match the submitted job spec.",
    )

    return (status=:valid, error="", verification_mode=:legacy_inline)
end

function _verify_job_result_artifact(record::AbstractDict)
    protocol = get(record, "result_protocol_version", nothing)
    protocol === nothing && return _verify_legacy_job_result_artifact(record)
    protocol isa AbstractString || return (
        status=:invalid,
        error="Job result protocol version must be a string.",
        verification_mode=:manifest,
    )
    String(protocol) == JOB_RESULT_PROTOCOL_VERSION || return (
        status=:retryable_error,
        error="Unsupported job result protocol version: $(protocol); deploy a compatible verifier.",
        verification_mode=:manifest,
    )
    return _verify_manifest_job_result_artifact(record)
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

function _validated_aws_batch_job_name(execution, job_id::AbstractString)
    id = String(job_id)
    occursin(r"^[0-9a-f]{32}$", id) || throw(ArgumentError(
        "AWS Batch canonical job_id must contain exactly 32 lowercase hexadecimal characters."))

    prefix = strip(_aws_batch_job_name_prefix(execution))
    isempty(prefix) && (prefix = "biocircuits")
    occursin(r"^[A-Za-z0-9][A-Za-z0-9_-]*$", prefix) || throw(ArgumentError(
        "AWS Batch job-name prefix must start with an alphanumeric character " *
        "and contain only letters, digits, hyphens, or underscores."))
    maximum_prefix_length = AWS_BATCH_JOB_NAME_MAX_LENGTH - 1 - length(id)
    length(prefix) <= maximum_prefix_length || throw(ArgumentError(
        "AWS Batch job-name prefix is too long; maximum length is " *
        "$(maximum_prefix_length) characters when the full canonical job_id is retained."))

    job_name = "$(prefix)-$(id)"
    length(job_name) <= AWS_BATCH_JOB_NAME_MAX_LENGTH || error(
        "Internal AWS Batch job-name length invariant failed.")
    return job_name
end

function _aws_batch_arn_identity(value)
    value isa AbstractString || return nothing
    text = String(value)
    startswith(text, "arn:") || return nothing
    parts = split(text, ':'; limit=6)
    length(parts) == 6 || return nothing
    parts[1] == "arn" || return nothing
    isempty(parts[2]) && return nothing
    parts[3] == "batch" || return nothing
    Config.is_valid_aws_batch_region(parts[4]) || return nothing
    Config.is_valid_aws_account_id(parts[5]) || return nothing
    isempty(parts[6]) && return nothing
    return (
        text=text,
        partition=parts[2],
        region=parts[4],
        account_id=parts[5],
        resource=parts[6],
    )
end

function _aws_batch_expected_resource_identity_valid(
    value::AbstractString,
    resource_prefix::AbstractString,
    region::AbstractString,
    account_id,
)
    startswith(String(value), "arn:") || return true
    identity = _aws_batch_arn_identity(value)
    identity === nothing && return false
    identity.region == String(region) || return false
    account_id === nothing || identity.account_id == String(account_id) ||
        return false
    return startswith(identity.resource, "$(resource_prefix)/")
end

function _aws_batch_artifact_uri(prefix::AbstractString, user_sub::AbstractString, job_id::AbstractString, filename::AbstractString)
    cleaned = replace(String(prefix), r"/+$" => "")
    return "$(cleaned)/users/$(user_sub)/jobs/$(job_id)/$(filename)"
end

function _aws_batch_container_override_plan(input_uri::AbstractString,
                                            status_uri::AbstractString,
                                            result_uri::AbstractString,
                                            execution)
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

    return overrides
end


function _aws_batch_container_overrides(input_uri::AbstractString,
                                        status_uri::AbstractString,
                                        result_uri::AbstractString,
                                        execution)
    return JSON3.write(_aws_batch_container_override_plan(
        input_uri,
        status_uri,
        result_uri,
        execution,
    ))
end

function _prepare_aws_batch_submission_plan(job_id::AbstractString,
                                            user_sub::AbstractString,
                                            kind::AbstractString,
                                            execution)
    id = String(job_id)
    region = Config.aws_batch_region()
    account_id = Config.aws_account_id()
    queue = _required_config(
        _aws_batch_config_value(
            execution,
            :job_queue,
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE",
        ),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE",
    )
    definition = _required_config(
        _aws_batch_config_value(
            execution,
            :job_definition,
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION",
        ),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION",
    )
    artifact_prefix = _required_config(
        _aws_batch_config_value(
            execution,
            :artifact_prefix,
            "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX",
        ),
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX",
    )
    _is_s3_uri(artifact_prefix) || throw(ArgumentError(
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX must be an s3:// URI."))
    _aws_batch_expected_resource_identity_valid(
        queue,
        "job-queue",
        region,
        account_id,
    ) || throw(ArgumentError(
        "AWS Batch queue ARN does not match the configured region/account identity."))
    _aws_batch_expected_resource_identity_valid(
        definition,
        "job-definition",
        region,
        account_id,
    ) || throw(ArgumentError(
        "AWS Batch job-definition ARN does not match the configured region/account identity."))

    input_uri = _aws_batch_artifact_uri(
        artifact_prefix,
        user_sub,
        id,
        "input.json",
    )
    status_uri = _aws_batch_artifact_uri(
        artifact_prefix,
        user_sub,
        id,
        "status.json",
    )
    result_uri = _aws_batch_artifact_uri(
        artifact_prefix,
        user_sub,
        id,
        "result.json",
    )
    result_manifest_uri = _aws_batch_artifact_uri(
        artifact_prefix,
        user_sub,
        id,
        "result-manifest.json",
    )
    tags = Dict{String, String}(
        "User" => _sanitize_tag_value(user_sub),
        "JobKind" => _sanitize_tag_value(kind),
        AWS_BATCH_JOB_ID_TAG => id,
    )
    overrides = _aws_batch_container_override_plan(
        input_uri,
        status_uri,
        result_uri,
        execution,
    )

    plan = Dict{String, Any}(
        "protocol_version" => AWS_BATCH_SUBMISSION_PROTOCOL_VERSION,
        "canonical_job_id" => id,
        "region" => region,
        "job_name" => _validated_aws_batch_job_name(execution, id),
        "job_queue" => queue,
        "job_definition" => definition,
        "input_uri" => input_uri,
        "status_uri" => status_uri,
        "result_uri" => result_uri,
        "result_manifest_uri" => result_manifest_uri,
        "tags" => tags,
        "container_overrides" => overrides,
    )
    account_id === nothing || (plan["account_id"] = account_id)
    return plan
end

function _aws_batch_submission_plan(record::AbstractDict)
    String(get(record, "submission_protocol_version", "")) ==
        AWS_BATCH_SUBMISSION_PROTOCOL_VERSION || return nothing
    String(get(record, "submission_state", "")) in
        AWS_BATCH_SUBMISSION_STATES || return nothing
    plan = get(record, "submission_plan", nothing)
    plan isa AbstractDict || return nothing
    String(get(plan, "protocol_version", "")) ==
        AWS_BATCH_SUBMISSION_PROTOCOL_VERSION || return nothing
    String(get(plan, "canonical_job_id", "")) ==
        String(get(record, "job_id", "")) || return nothing
    region = get(plan, "region", nothing)
    Config.is_valid_aws_batch_region(region) || return nothing
    account_id = get(plan, "account_id", nothing)
    account_id === nothing || Config.is_valid_aws_account_id(account_id) ||
        return nothing
    for key in (
        "job_name",
        "job_queue",
        "job_definition",
        "input_uri",
        "status_uri",
        "result_uri",
        "result_manifest_uri",
    )
        value = get(plan, key, nothing)
        value isa AbstractString && !isempty(strip(String(value))) || return nothing
    end
    _aws_batch_expected_resource_identity_valid(
        String(plan["job_queue"]),
        "job-queue",
        String(region),
        account_id,
    ) || return nothing
    _aws_batch_expected_resource_identity_valid(
        String(plan["job_definition"]),
        "job-definition",
        String(region),
        account_id,
    ) || return nothing
    tags = get(plan, "tags", nothing)
    tags isa AbstractDict || return nothing
    String(get(tags, AWS_BATCH_JOB_ID_TAG, "")) ==
        String(record["job_id"]) || return nothing
    overrides = get(plan, "container_overrides", nothing)
    overrides isa AbstractDict || return nothing
    command = get(overrides, "command", nothing)
    command isa AbstractVector || return nothing
    return plan
end

function _aws_batch_region_for_record(record::AbstractDict)
    plan = _aws_batch_submission_plan(record)
    return plan === nothing ?
        Config.aws_batch_region() : String(plan["region"])
end

function _aws_batch_input_payload(record::AbstractDict)
    plan = _aws_batch_submission_plan(record)
    plan === nothing && throw(ArgumentError(
        "AWS Batch submission plan is missing or invalid."))
    return Dict{String, Any}(
        "job_id" => String(record["job_id"]),
        "kind" => String(record["kind"]),
        "executor" => "aws_batch",
        "user_sub" => String(get(record, "user_sub", ANONYMOUS_USER_SUB)),
        "spec" => deepcopy(record["spec"]),
        "result_protocol_version" => JOB_RESULT_PROTOCOL_VERSION,
        "expected_artifact_config_hash" =>
            _record_expected_artifact_config_hash(
                record;
                allow_legacy_derivation=false,
            ),
        "artifacts" => Dict{String, Any}(
            "input" => String(plan["input_uri"]),
            "status" => String(plan["status_uri"]),
            "result" => String(plan["result_uri"]),
            "result_manifest" => String(plan["result_manifest_uri"]),
        ),
    )
end

function _confirm_aws_dispatch_record_durability(
    persistence::_JobPersistenceResult,
    ops::_JobPersistenceOps,
)
    persistence.committed || error(
        "AWS dispatch persistence returned without committing or throwing.")
    persistence.durable && return (durable=true, error=nothing)

    directory = dirname(abspath(persistence.path))
    try
        _fsync_job_directory_tracked!(directory, ops)
    catch err
        return (
            durable=false,
            error="AWS dispatch boundary committed, but exact directory " *
                "durability retry failed: $(sprint(showerror, err))",
        )
    end
    pending = _pending_job_store_dir_generation(directory)
    pending === nothing || return (
        durable=false,
        error="AWS dispatch boundary committed, but directory durability " *
            "remains pending at generation $(pending).",
    )
    return (durable=true, error=nothing)
end

function _begin_aws_batch_dispatch!(
    job_id::AbstractString;
    ops::_JobPersistenceOps=_DEFAULT_JOB_PERSISTENCE_OPS,
)
    id = String(job_id)
    return _with_job_lock(id) do
        record = _job_record_locked(id)
        record === nothing && return nothing
        String(get(record, "executor", "")) == "aws_batch" || return nothing
        String(get(record, "status", "")) == "queued" || return nothing
        String(get(record, "submission_state", "")) == "prepared" || return nothing
        _aws_batch_submission_plan(record) === nothing && return nothing

        candidate = deepcopy(record)
        candidate["submission_state"] = "dispatch_started"
        candidate["submission_dispatch_started_at"] = _now_iso_timestamp()
        candidate["progress"] = Dict(
            "message" => "AWS Batch dispatch boundary committed",
            "aws_status" => "DISPATCH_STARTED",
        )
        candidate["updated_at"] = _now_iso_timestamp()
        committed = _commit_job_candidate_unlocked_with_ops!(
            record,
            candidate,
            ops,
        )
        durability = _confirm_aws_dispatch_record_durability(
            committed.persistence,
            ops,
        )
        snapshot = _job_snapshot(record)
        authorization = durability.durable ?
            _AwsBatchDispatchAuthorization(snapshot) : nothing
        return (
            record=snapshot,
            authorization=authorization,
            durability_error=durability.error,
        )
    end
end

function _record_aws_batch_submission_ambiguity!(job_id::AbstractString,
                                                 err)
    id = String(job_id)
    diagnostic = sprint(showerror, err, catch_backtrace())
    return _with_job_lock(id) do
        record = _job_record_locked(id)
        record === nothing && return nothing
        !isempty(String(get(record, "batch_job_id", ""))) &&
            return _job_snapshot(record)
        String(get(record, "status", "")) in JOB_TERMINAL_STATUSES &&
            return _job_snapshot(record)

        candidate = deepcopy(record)
        candidate["submission_state"] = "reconciling"
        candidate["submission_last_error"] = diagnostic
        candidate["progress"] = Dict(
            "message" => String(get(record, "status", "")) ==
                "cancel_requested" ?
                "Cancel requested; reconciling AWS Batch submission" :
                "AWS Batch submission outcome is ambiguous; reconciling",
            "aws_status" => "SUBMISSION_OUTCOME_UNKNOWN",
        )
        candidate["updated_at"] = _now_iso_timestamp()
        _commit_job_candidate_unlocked!(record, candidate)
        return _job_snapshot(record)
    end
end

function _accept_aws_batch_submission!(job_id::AbstractString,
                                       submission::AbstractDict)
    id = String(job_id)
    return _with_job_lock(id) do
        record = _job_record_locked(id)
        record === nothing && return (record=nothing, cancel_after_accept=false)
        current_external_id = String(get(record, "batch_job_id", ""))
        if !isempty(current_external_id)
            return (
                record=_job_snapshot(record),
                cancel_after_accept=String(get(record, "status", "")) ==
                    "cancel_requested",
            )
        end
        String(get(record, "status", "")) in JOB_TERMINAL_STATUSES &&
            return (record=_job_snapshot(record), cancel_after_accept=false)

        candidate = deepcopy(record)
        for (key, value) in submission
            key == "progress" &&
                String(get(candidate, "status", "")) != "queued" && continue
            candidate[String(key)] = deepcopy(value)
        end
        candidate["submission_state"] = "accepted"
        delete!(candidate, "submission_last_error")
        delete!(candidate, "submission_conflict_job_ids")
        candidate["updated_at"] = _now_iso_timestamp()
        _commit_job_candidate_unlocked!(record, candidate)
        return (
            record=_job_snapshot(record),
            cancel_after_accept=String(get(record, "status", "")) ==
                "cancel_requested",
        )
    end
end

function _aws_batch_resource_name(value::AbstractString)
    text = String(value)
    slash = findlast(==('/'), text)
    return slash === nothing ? text : text[nextind(text, slash):end]
end

function _aws_batch_resource_matches(
    actual,
    expected,
    resource_prefix::AbstractString,
    plan::AbstractDict;
    allow_unversioned_revision::Bool=false,
)
    actual isa AbstractString && expected isa AbstractString || return false
    actual_text = String(actual)
    expected_text = String(expected)
    actual_text == expected_text && return true

    # A persisted full ARN is already the complete identity. Never weaken it
    # to a resource-name comparison.
    startswith(expected_text, "arn:") && return false

    actual_name = if startswith(actual_text, "arn:")
        identity = _aws_batch_arn_identity(actual_text)
        identity === nothing && return false
        identity.region == String(plan["region"]) || return false
        account_id = get(plan, "account_id", nothing)
        account_id === nothing ||
            identity.account_id == String(account_id) || return false
        startswith(identity.resource, "$(resource_prefix)/") || return false
        _aws_batch_resource_name(identity.resource)
    else
        _aws_batch_resource_name(actual_text)
    end
    expected_name = _aws_batch_resource_name(expected_text)
    if allow_unversioned_revision && !occursin(':', expected_name)
        actual_name == expected_name && return true
        parts = split(actual_name, ':'; limit=2)
        return length(parts) == 2 && parts[1] == expected_name &&
               !isempty(parts[2]) && all(character ->
                   '0' <= character <= '9', parts[2])
    end
    return actual_name == expected_name
end

_aws_batch_queue_matches(actual, expected, plan::AbstractDict) =
    _aws_batch_resource_matches(actual, expected, "job-queue", plan)

_aws_batch_definition_matches(actual, expected, plan::AbstractDict) =
    _aws_batch_resource_matches(
        actual,
        expected,
        "job-definition",
        plan;
        allow_unversioned_revision=true,
    )

function _aws_batch_list_job_summaries(plan::AbstractDict)
    summaries = Any[]
    seen_job_ids = Set{String}()
    next_token = nothing
    seen_tokens = Set{String}()
    for page_number in 1:AWS_BATCH_LIST_MAX_PAGES
        request = Dict{String, Any}(
            "jobQueue" => String(plan["job_queue"]),
            "maxResults" => AWS_BATCH_LIST_PAGE_SIZE,
            "filters" => Any[Dict(
                "name" => "JOB_NAME",
                "values" => Any[String(plan["job_name"])],
            )],
        )
        next_token === nothing || (request["nextToken"] = next_token)
        response = _aws_cli_json([
            "batch",
            "list-jobs",
            "--cli-input-json",
            JSON3.write(request),
            "--no-paginate",
            "--region",
            String(plan["region"]),
        ])
        raw_summaries = _raw_get(response, :jobSummaryList, nothing)
        raw_summaries isa AbstractVector || throw(ArgumentError(
            "AWS Batch ListJobs reconciliation response must contain a jobSummaryList array."))
        for summary in raw_summaries
            summary isa AbstractDict || throw(ArgumentError(
                "AWS Batch ListJobs reconciliation returned a malformed job summary."))
            raw_id = _raw_get(summary, :jobId, nothing)
            raw_id isa AbstractString && !isempty(strip(String(raw_id))) ||
                throw(ArgumentError(
                    "AWS Batch ListJobs reconciliation returned a malformed jobId."))
            job_id = String(raw_id)
            job_id in seen_job_ids && throw(ArgumentError(
                "AWS Batch ListJobs reconciliation returned duplicate job ID: $(job_id)."))
            raw_name = _raw_get(summary, :jobName, nothing)
            raw_name isa AbstractString && !isempty(String(raw_name)) ||
                throw(ArgumentError(
                    "AWS Batch ListJobs reconciliation returned a malformed jobName for $(job_id)."))
            push!(seen_job_ids, job_id)
            push!(summaries, summary)
        end
        length(summaries) <= AWS_BATCH_RECONCILE_MAX_CANDIDATES ||
            throw(ArgumentError(
                "AWS Batch submission reconciliation candidate limit exceeded."))
        raw_next = _raw_get(response, :nextToken, nothing)
        if raw_next === nothing
            return summaries
        end
        raw_next isa AbstractString || throw(ArgumentError(
            "AWS Batch ListJobs returned a non-string nextToken."))
        isempty(strip(String(raw_next))) && return summaries
        token = String(raw_next)
        token in seen_tokens && throw(ArgumentError(
            "AWS Batch ListJobs returned the same nextToken twice."))
        push!(seen_tokens, token)
        next_token = token
        page_number < AWS_BATCH_LIST_MAX_PAGES || throw(ArgumentError(
            "AWS Batch submission reconciliation page limit exceeded."))
    end
    error("AWS Batch submission reconciliation pagination invariant failed.")
end

function _aws_batch_describe_candidate_jobs(job_ids::Vector{String},
                                            plan::AbstractDict)
    details = Any[]
    for offset in 1:100:length(job_ids)
        upper = min(offset + 99, length(job_ids))
        requested_ids = job_ids[offset:upper]
        response = _aws_cli_json(vcat(
            ["batch", "describe-jobs", "--jobs"],
            requested_ids,
            ["--region", String(plan["region"])],
        ))
        raw_details = _raw_get(response, :jobs, nothing)
        raw_details isa AbstractVector || throw(ArgumentError(
            "AWS Batch DescribeJobs reconciliation response must contain a jobs array."))
        expected_ids = Set(requested_ids)
        described_ids = Set{String}()
        for detail in raw_details
            detail isa AbstractDict || throw(ArgumentError(
                "AWS Batch DescribeJobs reconciliation returned a malformed job detail."))
            raw_id = _raw_get(detail, :jobId, nothing)
            raw_id isa AbstractString && !isempty(strip(String(raw_id))) ||
                throw(ArgumentError(
                    "AWS Batch DescribeJobs reconciliation returned a malformed jobId."))
            detail_id = String(raw_id)
            detail_id in expected_ids || throw(ArgumentError(
                "AWS Batch DescribeJobs reconciliation returned an unrequested job ID: $(detail_id)."))
            detail_id in described_ids && throw(ArgumentError(
                "AWS Batch DescribeJobs reconciliation returned duplicate detail for job ID: $(detail_id)."))
            job_name = _raw_get(detail, :jobName, nothing)
            job_queue = _raw_get(detail, :jobQueue, nothing)
            job_definition = _raw_get(detail, :jobDefinition, nothing)
            tags = _raw_get(detail, :tags, nothing)
            container = _raw_get(detail, :container, nothing)
            command = container isa AbstractDict ?
                _raw_get(container, :command, nothing) : nothing
            job_name isa AbstractString && !isempty(String(job_name)) &&
                job_queue isa AbstractString && !isempty(String(job_queue)) &&
                job_definition isa AbstractString &&
                    !isempty(String(job_definition)) &&
                tags isa AbstractDict && container isa AbstractDict &&
                command isa AbstractVector || throw(ArgumentError(
                    "AWS Batch DescribeJobs reconciliation returned an incomplete job detail for $(detail_id)."))
            push!(described_ids, detail_id)
            push!(details, detail)
        end
        described_ids == expected_ids || throw(ArgumentError(
            "AWS Batch DescribeJobs reconciliation did not completely cover the requested job IDs."))
    end
    return details
end

function _aws_batch_candidate_matches(detail, plan::AbstractDict)
    detail isa AbstractDict || return false
    String(_raw_get(detail, :jobName, "")) ==
        String(plan["job_name"]) || return false
    _aws_batch_queue_matches(
        _raw_get(detail, :jobQueue, nothing),
        plan["job_queue"],
        plan,
    ) || return false
    _aws_batch_definition_matches(
        _raw_get(detail, :jobDefinition, nothing),
        plan["job_definition"],
        plan,
    ) || return false

    tags = _raw_get(detail, :tags, nothing)
    tags isa AbstractDict || return false
    String(_raw_get(tags, Symbol(AWS_BATCH_JOB_ID_TAG), "")) ==
        String(plan["canonical_job_id"]) || return false
    container = _raw_get(detail, :container, nothing)
    container isa AbstractDict || return false
    command = _raw_get(container, :command, nothing)
    command isa AbstractVector || return false
    expected_command = get(plan["container_overrides"], "command", nothing)
    expected_command isa AbstractVector || return false
    String.(collect(command)) == String.(collect(expected_command)) || return false
    return !isempty(String(_raw_get(detail, :jobId, "")))
end

function _find_aws_batch_submission_candidates(record::AbstractDict)
    plan = _aws_batch_submission_plan(record)
    plan === nothing && throw(ArgumentError(
        "AWS Batch submission plan is missing or invalid."))
    expected_name = String(plan["job_name"])
    ids = String[]
    for summary in _aws_batch_list_job_summaries(plan)
        summary isa AbstractDict || continue
        String(_raw_get(summary, :jobName, "")) == expected_name || continue
        candidate_id = strip(String(_raw_get(summary, :jobId, "")))
        isempty(candidate_id) || push!(ids, candidate_id)
    end
    isempty(ids) && return String[]

    listed_ids = Set(ids)
    valid_ids = String[]
    for detail in _aws_batch_describe_candidate_jobs(ids, plan)
        detail_id = String(_raw_get(detail, :jobId, ""))
        detail_id in listed_ids || continue
        _aws_batch_candidate_matches(detail, plan) || continue
        push!(valid_ids, detail_id)
    end
    unique!(valid_ids)
    sort!(valid_ids)
    return valid_ids
end

function _commit_aws_batch_reconciliation!(job_id::AbstractString;
                                           candidate_ids::Vector{String}=String[],
                                           reconcile_error=nothing)
    id = String(job_id)
    return _with_job_lock(id) do
        record = _job_record_locked(id)
        record === nothing && return (record=nothing, adopted=false)
        !isempty(String(get(record, "batch_job_id", ""))) &&
            return (record=_job_snapshot(record), adopted=false)
        String(get(record, "status", "")) in JOB_TERMINAL_STATUSES &&
            return (record=_job_snapshot(record), adopted=false)
        _aws_batch_submission_plan(record) === nothing &&
            return (record=_job_snapshot(record), adopted=false)

        candidate = deepcopy(record)
        attempts = try
            Int(get(candidate, "submission_reconcile_attempts", 0)) + 1
        catch
            1
        end
        candidate["submission_reconcile_attempts"] = attempts
        candidate["submission_last_reconcile_at"] = _now_iso_timestamp()
        adopted = false

        if reconcile_error !== nothing
            candidate["submission_state"] = "reconciling"
            candidate["submission_last_error"] = String(reconcile_error)
            candidate["progress"] = Dict(
                "message" => "AWS Batch submission reconciliation will retry",
                "aws_status" => "SUBMISSION_RECONCILE_RETRY",
            )
        elseif isempty(candidate_ids)
            candidate["submission_state"] =
                attempts >= AWS_BATCH_RECONCILE_UNKNOWN_AFTER_ATTEMPTS ?
                "unknown" : "reconciling"
            delete!(candidate, "submission_last_error")
            delete!(candidate, "submission_conflict_job_ids")
            candidate["progress"] = Dict(
                "message" => candidate["submission_state"] == "unknown" ?
                    "AWS Batch submission remains unknown; reconciliation will continue" :
                    "AWS Batch submission is not yet discoverable; reconciliation will retry",
                "aws_status" => candidate["submission_state"] == "unknown" ?
                    "SUBMISSION_UNKNOWN" : "SUBMISSION_NOT_FOUND",
            )
        elseif length(candidate_ids) == 1
            batch_job_id = only(candidate_ids)
            candidate["submission_state"] = "accepted"
            candidate["submission_accepted_at"] = _now_iso_timestamp()
            candidate["batch_job_id"] = batch_job_id
            candidate["batch_job_name"] =
                String(candidate["submission_plan"]["job_name"])
            delete!(candidate, "submission_last_error")
            delete!(candidate, "submission_conflict_job_ids")
            candidate["progress"] = Dict(
                "message" => "Adopted existing AWS Batch submission",
                "aws_status" => "SUBMITTED",
            )
            adopted = true
        else
            candidate["submission_state"] = "conflict"
            candidate["submission_conflict_job_ids"] = copy(candidate_ids)
            delete!(candidate, "submission_last_error")
            candidate["progress"] = Dict(
                "message" => "Multiple AWS Batch submissions matched the canonical job",
                "aws_status" => "SUBMISSION_CONFLICT",
            )
        end
        if String(get(record, "status", "")) == "cancel_requested"
            candidate["progress"] = Dict(
                "message" => adopted ?
                    "Cancel requested; adopted AWS Batch submission" :
                    "Cancel requested; reconciling AWS Batch submission",
                "aws_status" => get(
                    candidate["progress"],
                    "aws_status",
                    "SUBMISSION_OUTCOME_UNKNOWN",
                ),
            )
        end
        candidate["updated_at"] = _now_iso_timestamp()
        _commit_job_candidate_unlocked!(record, candidate)
        return (record=_job_snapshot(record), adopted=adopted)
    end
end

function _reconcile_aws_batch_submission!(record::AbstractDict)
    job_id = String(record["job_id"])
    candidate_ids = try
        _find_aws_batch_submission_candidates(record)
    catch err
        diagnostic = sprint(showerror, err, catch_backtrace())
        return _commit_aws_batch_reconciliation!(
            job_id;
            reconcile_error=diagnostic,
        )
    end
    return _commit_aws_batch_reconciliation!(
        job_id;
        candidate_ids=candidate_ids,
    )
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

# Perform the one allowed remote SubmitJob call from a fully persisted immutable
# plan. Configuration lookup and S3 publication happen before this helper; it
# never retries at the application layer, and the CLI SDK retry count is pinned
# to one total attempt so reconciliation owns every ambiguous outcome.
function _aws_batch_submit(authorization::_AwsBatchDispatchAuthorization)
    record = _consume_aws_batch_dispatch_authorization!(authorization)
    plan = _aws_batch_submission_plan(record)
    plan === nothing && throw(ArgumentError(
        "AWS Batch submission plan is missing or invalid."))
    String(get(record, "submission_state", "")) == "dispatch_started" ||
        throw(ArgumentError(
            "AWS Batch SubmitJob requires a durable dispatch_started state."))

    tags = plan["tags"]
    tag_value = join([
        "User=$(String(tags["User"]))",
        "JobKind=$(String(tags["JobKind"]))",
        "$(AWS_BATCH_JOB_ID_TAG)=$(String(tags[AWS_BATCH_JOB_ID_TAG]))",
    ], ",")
    job_name = String(plan["job_name"])
    response = _aws_cli_json_single_attempt([
        "batch",
        "submit-job",
        "--job-name",
        job_name,
        "--job-queue",
        String(plan["job_queue"]),
        "--job-definition",
        String(plan["job_definition"]),
        "--container-overrides",
        JSON3.write(plan["container_overrides"]),
        "--tags",
        tag_value,
        "--propagate-tags",
        "--region",
        String(plan["region"]),
    ])

    batch_job_id = strip(String(_raw_get(response, :jobId, "")))
    isempty(batch_job_id) && throw(ArgumentError(
        "AWS Batch SubmitJob response did not contain a non-empty jobId."))
    returned_job_name = strip(String(_raw_get(response, :jobName, "")))
    returned_job_name == job_name || throw(ArgumentError(
        "AWS Batch SubmitJob response jobName did not match the persisted submission plan."))

    return Dict{String, Any}(
        "batch_job_id" => batch_job_id,
        "batch_job_name" => returned_job_name,
        "submission_state" => "accepted",
        "submission_accepted_at" => _now_iso_timestamp(),
        "progress" => Dict(
            "message" => "Submitted to AWS Batch",
            "aws_status" => "SUBMITTED",
        ),
    )
end

function _refresh_aws_batch_job!(job_id::AbstractString;
                                 now_seconds::Real=_monotonic_seconds())
    id = String(job_id)
    refresh_now = Float64(now_seconds)
    probe = _with_job_lock(id) do
        record = _job_record_locked(id)
        if record === nothing
            lock(JOBS_LOCK) do
                delete!(JOB_DESCRIBE_LAST_AT, id)
            end
            return (action=:none, record=nothing)
        end
        if String(get(record, "executor", "")) != "aws_batch"
            lock(JOBS_LOCK) do
                delete!(JOB_DESCRIBE_LAST_AT, id)
            end
            return (action=:none, record=_job_snapshot(record))
        end
        if String(get(record, "status", "")) in JOB_TERMINAL_STATUSES
            lock(JOBS_LOCK) do
                delete!(JOB_DESCRIBE_LAST_AT, id)
            end
            return (action=:none, record=_job_snapshot(record))
        end
        batch_job_id = String(get(record, "batch_job_id", ""))
        if isempty(batch_job_id)
            plan = _aws_batch_submission_plan(record)
            if plan === nothing
                if String(get(record, "submission_state", "")) !=
                   "legacy_submission_unknown"
                    candidate = deepcopy(record)
                    candidate["submission_state"] =
                        "legacy_submission_unknown"
                    candidate["progress"] = Dict(
                        "message" => String(get(record, "status", "")) ==
                            "cancel_requested" ?
                            "Cancel requested; legacy AWS submission outcome is unknown" :
                            "Legacy AWS submission outcome is unknown",
                        "aws_status" => "LEGACY_SUBMISSION_UNKNOWN",
                    )
                    candidate["updated_at"] = _now_iso_timestamp()
                    _commit_job_candidate_unlocked!(record, candidate)
                end
                lock(JOBS_LOCK) do
                    delete!(JOB_DESCRIBE_LAST_AT, id)
                end
                return (action=:none, record=_job_snapshot(record))
            end
            submission_state = String(get(record, "submission_state", ""))
            if !(submission_state in (
                "dispatch_started",
                "reconciling",
                "unknown",
                "accepted",
            ))
                lock(JOBS_LOCK) do
                    delete!(JOB_DESCRIBE_LAST_AT, id)
                end
                return (action=:none, record=_job_snapshot(record))
            end
        end
        acquired_claim = lock(JOBS_LOCK) do
            _prune_job_describe_cache_unlocked!(refresh_now)
            id in JOB_DESCRIBE_IN_FLIGHT && return false
            last_at = get(JOB_DESCRIBE_LAST_AT, id, 0.0)
            if haskey(JOB_DESCRIBE_LAST_AT, id) &&
               refresh_now - last_at < _aws_batch_describe_min_interval()
                return false
            end
            _remember_job_describe_unlocked!(id, refresh_now)
            push!(JOB_DESCRIBE_IN_FLIGHT, id)
            return true
        end
        if !acquired_claim
            return (action=:none, record=_job_snapshot(record))
        end
        return (
            action=isempty(batch_job_id) ? :reconcile : :describe,
            record=_job_snapshot(record),
        )
    end
    probe.action === :none && return probe.record

    if probe.action === :reconcile
        try
            outcome = _reconcile_aws_batch_submission!(probe.record)
            if outcome.adopted && outcome.record !== nothing &&
               String(get(outcome.record, "status", "")) == "cancel_requested"
                cancel_biocircuits_job(
                    id;
                    user_sub=String(get(
                        outcome.record,
                        "user_sub",
                        ANONYMOUS_USER_SUB,
                    )),
                )
                return _job_record(id)
            end
            return outcome.record
        finally
            lock(JOBS_LOCK) do
                delete!(JOB_DESCRIBE_IN_FLIGHT, id)
            end
        end
    end

    try
        # AWS calls deliberately run without JOBS_LOCK. The response is
        # committed only if it is still a legal successor of the state observed
        # afterwards. The in-flight claim is always released in `finally`.
        response = _aws_cli_json([
            "batch",
            "describe-jobs",
            "--jobs",
            String(probe.record["batch_job_id"]),
            "--region",
            _aws_batch_region_for_record(probe.record),
        ])
        jobs = collect(_raw_get(response, :jobs, Any[]))
        isempty(jobs) && return _job_record(id)
        aws_job = jobs[1]
        aws_status = String(_raw_get(aws_job, :status, "UNKNOWN"))
        aws_job_status = _aws_batch_status_to_job_status(aws_status)
        status = aws_job_status
        container = _raw_get(aws_job, :container, Dict{String, Any}())
        log_stream = String(_raw_get(container, :logStreamName, ""))
        failure_reason = String(_raw_get(aws_job, :statusReason, _raw_get(container, :reason, "AWS Batch job failed")))
        result_artifact_error = nothing
        result_artifact_error_code = nothing
        result_artifact_verification = nothing

        # AWS Batch SUCCEEDED only confirms that the container exited 0. New
        # jobs validate a bounded commit manifest and HeadObject metadata
        # without downloading the potentially large result; records predating
        # the protocol retain the inline compatibility verifier. Every S3/CLI
        # call remains outside JOBS_LOCK.
        if status == "succeeded"
            result_artifact_verification = _verify_job_result_artifact(probe.record)
            if result_artifact_verification.status == :missing
                status = "failed"
                result_artifact_error = result_artifact_verification.error
                result_artifact_error_code = "aws_result_artifact_missing"
            elseif result_artifact_verification.status == :invalid
                status = "failed"
                result_artifact_error = result_artifact_verification.error
                result_artifact_error_code = "aws_result_artifact_invalid"
            elseif result_artifact_verification.status == :retryable_error
                # Preserve the current nonterminal state. A future refresh
                # retries verification instead of converting an operational
                # probe failure into a permanent failed job.
                status = String(get(probe.record, "status", "queued"))
            end
        end

        return _with_job_lock(id) do
            record = _job_record_locked(id)
            if record === nothing
                lock(JOBS_LOCK) do
                    delete!(JOB_DESCRIBE_LAST_AT, id)
                end
                return nothing
            end
            current_status = String(get(record, "status", ""))
            if current_status in JOB_TERMINAL_STATUSES
                lock(JOBS_LOCK) do
                    delete!(JOB_DESCRIBE_LAST_AT, id)
                end
                return _job_snapshot(record)
            end

            # A pending cancellation is monotonic while AWS remains nonterminal.
            # AWS FAILED after cancel/terminate confirms cancellation; SUCCEEDED
            # is retained because the remote job may have won the finish race.
            target_status = status
            if current_status == "cancel_requested"
                if aws_job_status == "failed" && result_artifact_error === nothing
                    target_status = "cancelled"
                elseif aws_job_status in ("queued", "running")
                    target_status = "cancel_requested"
                end
            elseif current_status == "running" && target_status == "queued"
                target_status = "running"
            end

            progress_message = if result_artifact_verification !== nothing &&
                                  result_artifact_verification.status == :retryable_error
                "AWS Batch succeeded; result artifact verification will retry"
            elseif result_artifact_error !== nothing
                "AWS Batch result artifact validation failed"
            else
                "AWS Batch status: $(aws_status)"
            end
            progress = Dict{String, Any}(
                "message" => progress_message,
                "aws_status" => aws_status,
            )
            if result_artifact_verification !== nothing
                progress["artifact_status"] = String(result_artifact_verification.status)
                hasproperty(result_artifact_verification, :verification_mode) &&
                    (progress["artifact_verification_mode"] =
                        String(result_artifact_verification.verification_mode))
            end
            updates = Dict{Symbol, Any}(
                :result_available => target_status == "succeeded",
                :progress => progress,
            )
            !isempty(log_stream) && (updates[:log_stream_name] = log_stream)
            if target_status == "running" && !haskey(record, "started_at")
                updates[:started_at] = _now_iso_timestamp()
            end
            if target_status in JOB_TERMINAL_STATUSES
                updates[:finished_at] = _now_iso_timestamp()
            end
            if target_status == "failed"
                updates[:error] = result_artifact_error === nothing ?
                    failure_reason : result_artifact_error
                result_artifact_error_code === nothing ||
                    (updates[:error_code] = result_artifact_error_code)
            end

            _transition_job_record_unlocked!(
                record,
                target_status;
                expected=(current_status,),
                updates...,
            )
            if String(get(record, "status", "")) in JOB_TERMINAL_STATUSES
                lock(JOBS_LOCK) do
                    delete!(JOB_DESCRIBE_LAST_AT, id)
                end
            end
            return _job_snapshot(record)
        end
    finally
        lock(JOBS_LOCK) do
            delete!(JOB_DESCRIBE_IN_FLIGHT, id)
        end
    end
end

function _cancel_aws_batch_job!(record::AbstractDict; observed_status=nothing)
    haskey(record, "batch_job_id") || throw(ArgumentError("AWS Batch job has not been submitted yet."))
    batch_job_id = String(record["batch_job_id"])
    status = observed_status === nothing ? String(get(record, "status", "queued")) : String(observed_status)
    region = _aws_batch_region_for_record(record)
    try
        if status == "running"
            run(Cmd([_aws_cli(), "batch", "terminate-job", "--job-id", batch_job_id, "--reason", "Cancelled by Biocircuits Explorer user", "--region", region]))
        else
            run(Cmd([_aws_cli(), "batch", "cancel-job", "--job-id", batch_job_id, "--reason", "Cancelled by Biocircuits Explorer user", "--region", region]))
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
    response = _aws_cli_json([
        "batch",
        "describe-jobs",
        "--jobs",
        batch_job_id,
        "--region",
        _aws_batch_region_for_record(record),
    ])
    jobs = collect(_raw_get(response, :jobs, Any[]))
    isempty(jobs) && return nothing
    return uppercase(String(_raw_get(jobs[1], :status, "UNKNOWN")))
end

function submit_biocircuits_job_from_spec(
    raw;
    user_sub::AbstractString=ANONYMOUS_USER_SUB,
    dispatch_persistence_ops::_JobPersistenceOps=
        _DEFAULT_JOB_PERSISTENCE_OPS,
)
    _raw_haskey(raw, :kind) || throw(ArgumentError("Job request must include `kind`."))
    _raw_haskey(raw, :spec) || throw(ArgumentError("Job request must include `spec`."))

    kind = String(_raw_get(raw, :kind, ""))
    kind in LOCAL_JOB_KINDS || throw(ArgumentError("Unsupported job kind: $(kind)"))

    execution = _raw_get(raw, :execution, Dict{String, Any}())
    mode = _job_execution_mode(raw)
    if !(mode in ("local", "local_async", "aws_batch", "batch"))
        throw(ArgumentError("Unsupported job execution mode: $(mode)"))
    end

    # Parse and validate the process-local cache bound before quota consumption
    # or durable publication. Runtime configuration errors must not surface
    # after a canonical record has already committed.
    _activate_job_cache_capacity!()

    job_id = string(rand(UInt128), base=16, pad=32)
    executor = mode in ("aws_batch", "batch") ? "aws_batch" : "local_async"
    user_sub = _sanitize_user_sub(user_sub)
    prepared = _prepare_job_spec_and_artifact_identity(
        kind,
        _raw_get(raw, :spec, Dict{String, Any}()),
    )
    spec = prepared.spec
    submission_plan = executor == "aws_batch" ?
        _prepare_aws_batch_submission_plan(
            job_id,
            user_sub,
            kind,
            execution,
        ) : nothing
    local_semaphore = executor == "local_async" ?
        _reserve_local_job_admission!(job_id) : nothing
    local_admission_transferred = false
    aws_submission_owner_registered = false

    try
        # Pure request normalization precedes admission. Capacity admission then
        # precedes quota consumption and every input/record write. Any later
        # failure before task ownership is transferred releases the reservation.
        if !_check_and_consume_quota!(user_sub)
            throw(QuotaExceeded("Daily submission quota exceeded for user $(user_sub). Limit: $(_quota_daily_limit())"))
        end
        if executor == "aws_batch"
            lock(JOBS_LOCK) do
                job_id in AWS_BATCH_INITIAL_SUBMISSIONS && error(
                    "AWS Batch job $(job_id) already has an initial submission owner.")
                push!(AWS_BATCH_INITIAL_SUBMISSIONS, job_id)
            end
            aws_submission_owner_registered = true
        end
        now = _now_iso_timestamp()

        record = Dict{String, Any}(
            "job_id" => job_id,
            "kind" => kind,
            "status" => "queued",
            "executor" => executor,
            "user_sub" => user_sub,
            "created_at" => now,
            "updated_at" => now,
            "state_revision" => 1,
            "result_available" => false,
            "progress" => Dict("message" => "Queued"),
            "spec" => spec,
            "expected_artifact_config_hash" => prepared.expected_artifact_config_hash,
            "input_path" => _job_input_path(job_id),
            "status_path" => _job_status_path(job_id),
            "record_path" => _job_record_path(job_id),
            "result_path" => _job_result_path(job_id),
            "input_uri" => _job_input_path(job_id),
            "status_uri" => _job_status_path(job_id),
            "result_uri" => _job_result_path(job_id),
        )

        aws_input_payload = nothing
        if executor == "aws_batch"
            record["result_protocol_version"] = JOB_RESULT_PROTOCOL_VERSION
            record["result_manifest_path"] = _job_result_manifest_path(job_id)
            record["submission_protocol_version"] =
                AWS_BATCH_SUBMISSION_PROTOCOL_VERSION
            record["submission_state"] = "prepared"
            record["submission_reconcile_attempts"] = 0
            record["submission_plan"] = deepcopy(submission_plan)
            record["batch_job_name"] = String(submission_plan["job_name"])
            record["input_uri"] = String(submission_plan["input_uri"])
            record["status_uri"] = String(submission_plan["status_uri"])
            record["result_uri"] = String(submission_plan["result_uri"])
            record["result_manifest_uri"] =
                String(submission_plan["result_manifest_uri"])
            record["progress"] = Dict(
                "message" => "AWS Batch submission prepared",
                "aws_status" => "PREPARED",
            )
            aws_input_payload = _aws_batch_input_payload(record)
            _write_job_json(record["input_path"], aws_input_payload)
        else
            initial_payload = Dict{String, Any}(
                "job_id" => job_id,
                "kind" => kind,
                "executor" => record["executor"],
                "user_sub" => user_sub,
                "spec" => spec,
                "expected_artifact_config_hash" =>
                    prepared.expected_artifact_config_hash,
                "artifacts" => Dict{String, Any}(
                    "input" => record["input_uri"],
                    "status" => record["status_uri"],
                    "result" => record["result_uri"],
                ),
            )
            _write_json_uri(record["input_uri"], initial_payload)
        end

        canonical_snapshot = _with_job_lock(job_id) do
            _persist_job_record_unlocked(record)
            _job_cache_publish!(job_id, record)
            _persist_job_status_projection_unlocked(record)
            return _job_snapshot(record)
        end

        if executor == "aws_batch"
            # The very first canonical record already owns the complete remote
            # identity. Publish input from that immutable snapshot, then commit
            # dispatch_started before the one allowed SubmitJob call.
            try
                _write_json_uri(
                    String(canonical_snapshot["input_uri"]),
                    aws_input_payload,
                )
            catch err
                failed = _job_transition!(
                    job_id,
                    "failed";
                    expected=("queued",),
                    submission_state="failed_before_dispatch",
                    finished_at=_now_iso_timestamp(),
                    result_available=false,
                    error=sprint(showerror, err, catch_backtrace()),
                    error_code="aws_input_publication_failed",
                    progress=Dict(
                        "message" => "AWS Batch input publication failed before dispatch",
                    ),
                )
                if !failed.applied && failed.record !== nothing &&
                   String(get(failed.record, "status", "")) in
                   JOB_TERMINAL_STATUSES
                    return _job_public_record(failed.record)
                end
                rethrow()
            end

            dispatch = _begin_aws_batch_dispatch!(
                job_id;
                ops=dispatch_persistence_ops,
            )
            if dispatch === nothing
                current = _job_record(job_id)
                current === nothing && error(
                    "AWS Batch job disappeared before dispatch.")
                return _job_public_record(current)
            end

            if dispatch.authorization === nothing
                public = _job_public_record(dispatch.record)
                submission_diagnostic = public["submission"]
                submission_diagnostic["dispatch_durability"] = "unconfirmed"
                submission_diagnostic["dispatch_durability_error"] =
                    dispatch.durability_error
                return public
            end

            submission = try
                _aws_batch_submit(dispatch.authorization)
            catch err
                ambiguous = _record_aws_batch_submission_ambiguity!(
                    job_id,
                    err,
                )
                ambiguous === nothing && error(
                    "AWS Batch job disappeared after an ambiguous submission.")
                return _job_public_record(ambiguous)
            end

            accepted = _accept_aws_batch_submission!(job_id, submission)
            accepted.record === nothing && error(
                "AWS Batch job disappeared after submission was accepted.")
            if accepted.cancel_after_accept
                cancel_biocircuits_job(job_id; user_sub=user_sub)
            end
        else
            semaphore = local_semaphore
            semaphore isa Base.Semaphore ||
                error("Local job admission did not provide a run semaphore.")
            start_gate = Channel{Nothing}(1)
            token = LocalJobCancelToken(job_id)
            task = Threads.@spawn _run_admitted_local_job!(
                job_id,
                kind,
                spec,
                token,
                semaphore;
                start_gate=start_gate,
            )
            # From this point the task-level `finally` owns admission release,
            # even if registration or gate publication unexpectedly fails.
            local_admission_transferred = true
            try
                lock(JOBS_LOCK) do
                    JOB_TASKS[job_id] = task
                    LOCAL_JOB_CANCEL_TOKENS[job_id] = token
                end
                # Registration happens-before worker execution, so the worker's
                # cleanup cannot race a late bookkeeping insertion.
                put!(start_gate, nothing)
            catch
                close(start_gate)
                rethrow()
            end
        end

        return get_biocircuits_job(job_id; user_sub=user_sub)
    finally
        if local_semaphore !== nothing && !local_admission_transferred
            _release_local_job_admission!(job_id)
        end
        if aws_submission_owner_registered
            lock(JOBS_LOCK) do
                delete!(AWS_BATCH_INITIAL_SUBMISSIONS, job_id)
            end
        end
    end
end

function cancel_biocircuits_job(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    job_id = String(job_id)
    decision = _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
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

        # A prepared AWS plan has not crossed the durable dispatch boundary, so
        # queued cancellation terminates it locally and the submit path's
        # prepared -> dispatch_started commit cannot win afterwards. Once the
        # boundary is crossed, cancellation remains pending until an external
        # id is returned or reconciled.
        prepared_aws_without_external_id = executor == "aws_batch" &&
            !has_external_job &&
            String(get(record, "submission_state", "")) == "prepared"
        target_status = status == "queued" &&
            (executor != "aws_batch" || prepared_aws_without_external_id) ?
            "cancelled" : "cancel_requested"
        dispatch_claim = has_external_job ? string(rand(UInt128), base=16, pad=32) : nothing
        transition_updates = Dict{Symbol, Any}(
            :cancel_requested_at => _now_iso_timestamp(),
            :cancel_observed_status => status,
            :result_available => false,
            :progress => Dict("message" => target_status == "cancelled" ? "Cancelled" : "Cancel requested"),
        )
        prepared_aws_without_external_id &&
            (transition_updates[:submission_state] =
                "cancelled_before_dispatch")
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
        token = lock(JOBS_LOCK) do
            token = get(LOCAL_JOB_CANCEL_TOKENS, job_id, nothing)
            delete!(JOB_DESCRIBE_LAST_AT, job_id)
            token
        end
        token === nothing || _request_cancel!(token)
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
