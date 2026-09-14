const JOBS = Dict{String, Dict{String, Any}}()
const JOBS_LOCK = ReentrantLock()
const JOB_CACHE_LAST_ACCESS = Dict{String, UInt64}()
const JOB_CACHE_ACCESS_CLOCK = Ref{UInt64}(0)
const JOB_CACHE_CAPACITY = Ref{Union{Nothing, Int}}(nothing)
const JOB_LOCK_STRIPE_COUNT = 128
const JOB_LOCK_STRIPES = [ReentrantLock() for _ in 1:JOB_LOCK_STRIPE_COUNT]
const JOB_TASKS = Dict{String, Task}()
const LOCAL_JOB_CANCEL_TOKENS = Dict{String, LocalJobCancelToken}()
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
    "compute_ro_field",
    "design_network",
    "merge_atlas_library",
    "query_atlas",
    "run_inverse_design",
    "rop_shape_optimize",
])

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
# Job ownership is single-tenant: every request resolves to the anonymous
# owner. The allow set stays as input validation for persisted records.
const _USER_SUB_ALLOWED = r"^[A-Za-z0-9_\-.:@]{1,128}$"

function _sanitize_user_sub(raw)
    raw === nothing && return ANONYMOUS_USER_SUB
    text = strip(String(raw))
    isempty(text) && return ANONYMOUS_USER_SUB
    occursin(_USER_SUB_ALLOWED, text) || throw(ArgumentError("Invalid user_sub: must match $(_USER_SUB_ALLOWED.pattern)"))
    return text
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

_is_file_uri(uri::AbstractString) = startswith(lowercase(String(uri)), "file://")
_uri_local_path(uri::AbstractString) = _is_file_uri(uri) ? String(uri)[8:end] : String(uri)

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
    return _write_durable_job_artifact_json_with_ops(
        _uri_local_path(uri),
        payload,
        ops,
    )
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
    return _read_job_json(_uri_local_path(uri))
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

    for key in ("started_at", "finished_at", "progress", "error", "error_code", "cancel_requested_at")
        haskey(record, key) && (out[key] = deepcopy(record[key]))
    end

    if String(get(record, "kind", "")) == "compute_ro_field"
        namespace = String(get(
            record, "ro_field_artifact_namespace", "ro-field"))
        plan_ref = namespace == "ro-field-sparse-v2" ?
            "job://$(job_id)/$(namespace)/plans/$(record["ro_field_plan_sha256"])" :
            "job://$(job_id)/ro-field/plan"
        ro_field = Dict{String,Any}(
            "plan_sha256" => String(record["ro_field_plan_sha256"]),
            "network_ir_sha256" => String(record["ro_field_network_ir_sha256"]),
            "resume_from" => deepcopy(get(record, "resume_from", nothing)),
        )
        if namespace != "ro-field"
            ro_field["artifact_namespace"] = namespace
            ro_field["plan_ref"] = plan_ref
        end
        if haskey(record, "latest_checkpoint_sha256")
            checkpoint_hash = String(record["latest_checkpoint_sha256"])
            ro_field["checkpoint_sha256"] = checkpoint_hash
            ro_field["committed_work_unit_count"] = Int(get(
                record, "committed_work_unit_count", 0))
            ro_field["committed_point_count"] = Int(get(
                record, "committed_point_count", 0))
            ro_field["committed_payload_bytes"] = Int(get(
                record, "committed_payload_bytes", 0))
            ro_field["checkpoint_ref"] =
                "job://$(job_id)/$(namespace)/checkpoints/$(checkpoint_hash)"
        end
        if Bool(get(record, "result_available", false)) &&
           haskey(record, "ro_field_dataset_manifest_sha256")
            manifest_hash = String(record["ro_field_dataset_manifest_sha256"])
            ro_field["dataset_manifest_sha256"] = manifest_hash
            ro_field["dataset_manifest_ref"] =
                "job://$(job_id)/$(namespace)/manifests/$(manifest_hash)"
        end
        out["ro_field"] = ro_field
    end

    out["artifacts"]["input"] = "job://$(job_id)/input"
    out["artifacts"]["status"] = "job://$(job_id)/status"
    if Bool(get(record, "result_available", false))
        out["result_ref"] = "job://$(job_id)/result"
        out["artifacts"]["result"] = out["result_ref"]
        haskey(record, "result_manifest_uri") &&
            (out["artifacts"]["result_manifest"] =
                "job://$(job_id)/result-manifest")
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
    status_uri = get(record, "status_uri", record["status_path"])
    if String(status_uri) != String(record["status_path"])
        _write_json_uri(String(status_uri), status)
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
        _recover_retired_executor_unlocked!(record)
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

# Records written by a retired executor (e.g. the removed AWS Batch lane)
# cannot make progress in this build. Settle a nonterminal one exactly once
# through the ordinary durable transition path instead of leaving it queued
# forever; terminal records are historical and stay untouched.
function _recover_retired_executor_unlocked!(record::AbstractDict)
    executor = String(get(record, "executor", ""))
    executor in ("local", "local_async") && return false
    status = String(get(record, "status", ""))
    status in ("queued", "running", "cancel_requested") || return false

    return _transition_job_record_unlocked!(
        record,
        "failed";
        expected=(status,),
        finished_at=_now_iso_timestamp(),
        result_available=false,
        error_code="executor_retired",
        error="Job executor $(executor) was retired; the record cannot make progress in this build.",
        progress=Dict("message" => "Job executor retired"),
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

function _guard_compute_ro_field_job_identity!(record::AbstractDict,
                                               candidate::AbstractDict)
    String(get(record, "kind", "")) == "compute_ro_field" || return nothing
    immutable_keys = (
        "job_id", "kind", "executor", "user_sub", "created_at", "spec",
        "expected_artifact_config_hash", "ro_field_plan_sha256",
        "ro_field_network_ir_sha256", "ro_field_artifact_namespace",
        "resume_from", "input_path",
        "status_path", "record_path", "result_path", "input_uri",
        "status_uri", "result_uri", "result_protocol_version",
        "result_manifest_path", "result_manifest_uri",
    )
    for key in immutable_keys
        haskey(record, key) == haskey(candidate, key) || throw(ArgumentError(
            "compute_ro_field immutable job field $(key) cannot be added or removed"))
        haskey(record, key) || continue
        isequal(record[key], candidate[key]) || throw(ArgumentError(
            "compute_ro_field immutable job field $(key) cannot change"))
    end
    return nothing
end

function _commit_job_candidate_unlocked_with_ops!(record::AbstractDict,
                                                  candidate::AbstractDict,
                                                  ops::_JobPersistenceOps)
    # Persist the canonical candidate before exposing it through the shared
    # in-memory dictionary. A pre-rename failure leaves both views at the
    # previous revision. Once rename commits, memory advances even when the
    # following directory fsync needs a readiness-driven retry.
    _guard_compute_ro_field_job_identity!(record, candidate)
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
    if String(get(record, "kind", "")) == "compute_ro_field"
        verification = _verify_job_result_artifact(record; verify_nested=false)
        verification.status == :valid || throw(ArgumentError(
            "RO-field result artifacts no longer validate: " *
            verification.error))
    end
    result_uri = String(get(record, "result_uri", _job_result_path(job_id)))
    return Dict(
        "job" => status,
        "result" => _read_json_uri(result_uri),
    )
end

function _execute_local_job(kind::AbstractString, spec;
                            cancel_check::Function=_no_cancel_check,
                            job_context=Dict{String,Any}())
    # Wrap every local-job result in the bne-result envelope (as a sibling
    # `artifact` field) so the persisted result and its `result_ref` are
    # self-describing: algorithm + version, input network hashes, and a config
    # hash of the spec for reproducibility.
    cancel_check()
    result = _dispatch_local_job(
        kind, spec; cancel_check=cancel_check, job_context=job_context)
    cancel_check()
    result isa AbstractDict && haskey(result, "artifact") && return result
    return attach_artifact!(result, kind;
        input_hashes = Dict{String, Any}("network_ir_hashes" => artifact_network_hashes(spec)),
        config = spec)
end

function _dispatch_local_job(kind::AbstractString, spec;
                             cancel_check::Function=_no_cancel_check,
                             job_context=Dict{String,Any}())
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
    elseif kind == "compute_ro_field"
        return compute_ro_field_job(
            spec; job_context=job_context, cancel_check=cancel_check)
    elseif kind == "design_network"
        return target_design_from_spec(
            spec; job_context=job_context, cancel_check=cancel_check)
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
    if String(kind) == "compute_ro_field"
        validate_ro_field_job_result!(
            result, job_id, String(expected_config_hash))
    end

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

function _job_result_manifest_payload(identity,
                                      result_uri::AbstractString,
                                      content_length::Integer,
                                      sha256_hex::AbstractString)
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
    identity = _job_result_identity(result, job_id, kind, expected_config_hash)
    temp_parent = _ensure_job_directory_with_ops(
        dirname(_uri_local_path(result_uri)),
        ops,
    )
    temp_path, temp_io = mktemp(temp_parent; cleanup=false)
    close(temp_io)
    try
        # The staging entry is never a published artifact. Its file contents
        # are synced here; local publication later renames it into place and
        # syncs the destination directory.
        _write_job_staging_json_with_ops(temp_path, result, ops)
        content_length = filesize(temp_path)
        content_length > 0 || throw(ArgumentError("Serialized job result is empty."))
        sha256_hex = _file_sha256_hex(temp_path)
        manifest = _job_result_manifest_payload(
            identity,
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

function _finish_local_job!(job_id::AbstractString;
                            succeeded::Bool,
                            error=nothing,
                            success_updates::AbstractDict=Dict{Symbol,Any}())
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
            if succeeded
                for (key, value) in pairs(success_updates)
                    symbol = key isa Symbol ? key : Symbol(String(key))
                    symbol in (:status, :state_revision, :result_available) &&
                        throw(ArgumentError(
                            "success_updates cannot override job lifecycle fields"))
                    updates[symbol] = deepcopy(value)
                end
            end
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

            execution_record = _job_record(job_id)
            execution_record === nothing && error(
                "Local job record disappeared before execution")
            job_context = Dict{String,Any}(
                "job_id" => job_id,
                "job_root" => _job_dir(job_id),
                "user_sub" => String(get(
                    execution_record, "user_sub", ANONYMOUS_USER_SUB)),
            )
            if kind == "design_network"
                last_published = Ref(0.0)
                last_stage = Ref(("", 0, 0))
                job_context["publish_progress"] = function (progress)
                    cancel_check()
                    phase = String(get(progress, "phase", "optimizing"))
                    stage = (phase, get(progress, "restart", 0), get(progress, "prune_round", 0))
                    now = time()
                    # A durable status write every epoch would dominate small
                    # optimizations. Always publish stage boundaries and the
                    # final evaluated step; other updates are limited to 4 Hz.
                    final_step = haskey(progress, "step") && get(progress, "step", -1) == get(progress, "epochs", -2)
                    stage == last_stage[] && !final_step && now - last_published[] < 0.25 &&
                        return nothing
                    public_progress = Dict{String,Any}(_materialize(progress))
                    public_progress["message"] = replace(phase, '_' => ' ')
                    updated = _job_transition!(job_id, "running";
                        expected=("running",), progress=public_progress)
                    updated.applied || cancel_check()
                    last_published[] = now
                    last_stage[] = stage
                    return nothing
                end
            end
            if kind == "compute_ro_field"
                job_context["publish_checkpoint"] = function (checkpoint)
                    transition = _job_transition!(
                        job_id,
                        "running";
                        expected=("running",),
                        latest_checkpoint_sha256=
                            checkpoint["checkpoint_sha256"],
                        committed_work_unit_count=
                            checkpoint["committed_work_unit_count"],
                        committed_point_count=
                            checkpoint["committed_point_count"],
                        committed_payload_bytes=
                            checkpoint["committed_payload_bytes"],
                        progress=Dict{String,Any}(
                            "message" => "RO-field checkpoint committed",
                            "committed_work_unit_count" =>
                                checkpoint["committed_work_unit_count"],
                            "committed_point_count" =>
                                checkpoint["committed_point_count"],
                            "committed_payload_bytes" =>
                                checkpoint["committed_payload_bytes"],
                        ),
                    )
                    transition.applied || begin
                        cancel_check()
                        error("RO-field checkpoint could not be linearized")
                    end
                    return nothing
                end
            end
            result = _execute_local_job(
                kind, spec;
                cancel_check=cancel_check,
                job_context=job_context)

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
            result_uri = record === nothing ? _job_result_path(job_id) :
                String(get(record, "result_uri", _job_result_path(job_id)))
            success_updates = Dict{Symbol,Any}()
            if kind == "compute_ro_field"
                record === nothing && error(
                    "Local RO-field job record disappeared before publication")
                manifest_uri = String(record["result_manifest_uri"])
                expected_config_hash = String(
                    record["expected_artifact_config_hash"])
                cancel_check()
                outer_manifest = _publish_job_result_with_manifest(
                    result,
                    job_id,
                    kind,
                    expected_config_hash,
                    result_uri,
                    manifest_uri,
                )
                cancel_check()
                descriptor = result["ro_field_job_result"]
                linked = _job_transition!(
                    job_id,
                    "running";
                    expected=("running",),
                    ro_field_dataset_manifest_sha256=
                        descriptor["dataset_manifest_sha256"],
                )
                linked.applied || begin
                    cancel_check()
                    error("RO-field manifest identity could not be linearized")
                end
                record = _job_record(job_id)
                record === nothing && error(
                    "Local RO-field job record disappeared before verification")
                verification = _verify_job_result_artifact(record; verify_nested=false)
                verification.status == :valid || error(
                    "Published local RO-field result failed manifest verification: " *
                    verification.error)
                success_updates[:ro_field_dataset_manifest_sha256] =
                    descriptor["dataset_manifest_sha256"]
                success_updates[:ro_field_outer_result_sha256] =
                    outer_manifest["result"]["sha256"]
            else
                _write_json_uri(result_uri, result)
            end
            _finish_local_job!(
                job_id; succeeded=true, success_updates=success_updates)
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

function _head_artifact(uri::AbstractString)
    text = strip(String(uri))
    isempty(text) && return (
        status=:invalid,
        detail="Result artifact URI is empty.",
        content_length=nothing,
        content_type=nothing,
        metadata=Dict{String, Any}(),
    )

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

function _probe_artifact_presence(uri::AbstractString)
    head = _head_artifact(uri)
    return (status=head.status, detail=head.detail)
end

function _read_artifact_bytes(uri::AbstractString; max_bytes=nothing)
    text = String(uri)
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

function _job_artifact_config(kind::AbstractString, spec)
    if String(kind) == "design_network"
        return normalize_target_design_request(spec)
    elseif String(kind) == "rop_shape_optimize"
        return _rop_shape_normalize_request(spec; synchronous=false).normalized
    elseif String(kind) == "compute_ro_field"
        normalized = normalize_ro_field_job_spec(spec)
        return normalized["plan"]["identity"]
    end
    return spec
end

# Resolve the exact config identity once, before a job is persisted or handed
# to a worker. ROP shape legacy/partial NetworkIR inputs may acquire default
# provenance while they are normalized; repeating that conversion later can
# produce a different timestamp and therefore a different artifact hash.
function _prepare_job_spec_and_artifact_identity(kind::AbstractString, raw_spec)
    submitted_spec = _materialize(raw_spec)
    if String(kind) == "compute_ro_field"
        normalized = normalize_ro_field_job_spec(submitted_spec)
        plan_hash = String(normalized["plan"]["plan_sha256"])
        _canonical_hash(normalized["plan"]["identity"]) == plan_hash ||
            error("RO-field plan hash disagrees with the shared canonical hash")
        return (
            spec=normalized,
            expected_artifact_config_hash=plan_hash,
        )
    end
    artifact_config = _job_artifact_config(kind, submitted_spec)
    worker_spec = String(kind) in ("rop_shape_optimize", "design_network") ?
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
            "Job succeeded but result manifest is missing: $(uri)" :
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

    actual_sha256 = try
        _file_sha256_hex(_uri_local_path(descriptor.result_uri))
    catch err
        return (
            status=:retryable_error,
            error="Cannot hash local result artifact: $(sprint(showerror, err))",
            verification_mode=:manifest,
        )
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

function _verify_job_result_artifact(record::AbstractDict; verify_nested::Bool=true)
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
    verification = _verify_manifest_job_result_artifact(record)
    verification.status == :valid || return verification
    # Reading a committed result checks its bytes and original identity. Deep
    # engine replay belongs to publication, explicit audits, and resume.
    verify_nested || return verification
    String(get(record, "kind", "")) == "compute_ro_field" ||
        return verification

    result_uri = String(get(record, "result_uri", ""))
    result = try
        _read_json_uri(result_uri)
    catch err
        return (
            status=:retryable_error,
            error="Cannot reload the committed RO-field result: " *
                sprint(showerror, err),
            verification_mode=:manifest_and_nested_ro_field,
        )
    end
    try
        validate_ro_field_job_result!(
            result,
            String(get(record, "job_id", "")),
            String(get(record, "expected_artifact_config_hash", ""));
            record=record,
        )
    catch err
        return (
            status=:invalid,
            error="Committed RO-field nested artifacts are invalid: " *
                sprint(showerror, err),
            verification_mode=:manifest_and_nested_ro_field,
        )
    end
    return (
        status=:valid,
        error="",
        verification_mode=:manifest_and_nested_ro_field,
    )
end

function submit_biocircuits_job_from_spec(
    raw;
    user_sub::AbstractString=ANONYMOUS_USER_SUB,
)
    _raw_haskey(raw, :kind) || throw(ArgumentError("Job request must include `kind`."))
    _raw_haskey(raw, :spec) || throw(ArgumentError("Job request must include `spec`."))

    kind = String(_raw_get(raw, :kind, ""))
    kind in LOCAL_JOB_KINDS || throw(ArgumentError("Unsupported job kind: $(kind)"))

    mode = _job_execution_mode(raw)
    if !(mode in ("local", "local_async"))
        throw(ArgumentError("Unsupported job execution mode: $(mode)"))
    end

    # Parse and validate the process-local cache bound before durable
    # publication. Runtime configuration errors must not surface after a
    # canonical record has already committed.
    _activate_job_cache_capacity!()

    job_id = string(rand(UInt128), base=16, pad=32)
    user_sub = _sanitize_user_sub(user_sub)
    prepared = _prepare_job_spec_and_artifact_identity(
        kind,
        _raw_get(raw, :spec, Dict{String, Any}()),
    )
    spec = prepared.spec
    kind == "compute_ro_field" &&
        validate_ro_field_resume_parent!(spec, user_sub)
    local_semaphore = _reserve_local_job_admission!(job_id)
    local_admission_transferred = false

    try
        # Pure request normalization precedes admission, and admission precedes
        # every input/record write. Any later failure before task ownership is
        # transferred releases the reservation.
        now = _now_iso_timestamp()

        record = Dict{String, Any}(
            "job_id" => job_id,
            "kind" => kind,
            "status" => "queued",
            "executor" => "local_async",
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

        if kind == "compute_ro_field"
            plan = spec["plan"]
            record["ro_field_plan_sha256"] = plan["plan_sha256"]
            if spec["schema_version"] == RO_FIELD_SPARSE_JOB_SPEC_VERSION
                record["ro_field_network_ir_sha256"] =
                    plan["identity"]["network_ir_sha256"]
                record["ro_field_artifact_namespace"] =
                    "ro-field-sparse-v2"
            else
                record["ro_field_network_ir_sha256"] =
                    plan["identity"]["computation_spec"]["network_ir_sha256"]
            end
            record["resume_from"] = deepcopy(spec["resume_from"])
            record["result_protocol_version"] = JOB_RESULT_PROTOCOL_VERSION
            record["result_manifest_path"] =
                _job_result_manifest_path(job_id)
            record["result_manifest_uri"] =
                _job_result_manifest_path(job_id)
        end

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
        if kind == "compute_ro_field"
            initial_payload["result_protocol_version"] =
                JOB_RESULT_PROTOCOL_VERSION
            initial_payload["artifacts"]["result_manifest"] =
                record["result_manifest_uri"]
        end
        _write_json_uri(record["input_uri"], initial_payload)

        _with_job_lock(job_id) do
            _persist_job_record_unlocked(record)
            _job_cache_publish!(job_id, record)
            _persist_job_status_projection_unlocked(record)
            return _job_snapshot(record)
        end

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

        return get_biocircuits_job(job_id; user_sub=user_sub)
    finally
        if !local_admission_transferred
            _release_local_job_admission!(job_id)
        end
    end
end

function cancel_biocircuits_job(job_id::AbstractString; user_sub::AbstractString=ANONYMOUS_USER_SUB)
    job_id = String(job_id)
    return _with_job_lock(job_id) do
        record = _job_record_locked(job_id)
        record === nothing && throw(ArgumentError("Unknown job_id: $(job_id)"))
        _check_user_owns_record(record, user_sub, job_id)
        status = String(record["status"])
        if status in JOB_TERMINAL_STATUSES
            return _job_public_record(record)
        end

        # A queued local job settles immediately; a running one keeps the
        # cancel intent until its worker reaches a cooperative checkpoint.
        target_status = status == "queued" ? "cancelled" : "cancel_requested"
        applied = _transition_job_record_unlocked!(
            record,
            target_status;
            expected=(status,),
            cancel_requested_at=_now_iso_timestamp(),
            cancel_observed_status=status,
            result_available=false,
            progress=Dict("message" => target_status == "cancelled" ? "Cancelled" : "Cancel requested"),
        )
        applied || return _job_public_record(record)
        token = lock(JOBS_LOCK) do
            get(LOCAL_JOB_CANCEL_TOKENS, job_id, nothing)
        end
        token === nothing || _request_cancel!(token)
        return _job_public_record(record)
    end
end

function _request_user_sub(req)
    # Single-user deployment: every request resolves to the anonymous owner.
    return ANONYMOUS_USER_SUB
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

    if length(parts) == 4 && parts[1] == "api" && parts[2] == "jobs" && parts[4] == "cancel"
        return json_response(cancel_biocircuits_job(parts[3]; user_sub=user_sub))
    end

    return error_response("Unknown jobs route"; status=404)
end
