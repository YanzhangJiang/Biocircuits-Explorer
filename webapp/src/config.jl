module Config

const ATLAS_SQLITE_PERSIST_MODE_ENV = "ATLAS_SQLITE_PERSIST_MODE"
const ATLAS_SQLITE_LIGHTWEIGHT_ENV  = "ATLAS_SQLITE_LIGHTWEIGHT_PERSIST"

_env_string(key::AbstractString, default::AbstractString = "") =
    String(strip(get(ENV, String(key), String(default))))

function _first_nonempty(keys::Vector{String})
    for key in keys
        value = _env_string(key)
        !isempty(value) && return value
    end
    return ""
end
_first_nonempty(keys::AbstractString...) = _first_nonempty(String[String(k) for k in keys])

function _parse_int_or(default::Int, raw::AbstractString)
    text = strip(String(raw))
    isempty(text) && return default
    parsed = tryparse(Int, text)
    return parsed === nothing ? default : parsed
end

function _strict_positive_int(name::AbstractString,
                              raw::AbstractString,
                              default::Int;
                              maximum::Int)
    text = strip(String(raw))
    isempty(text) && return default
    parsed = tryparse(Int, text)
    if parsed === nothing || parsed < 1 || parsed > maximum
        throw(ArgumentError(
            "$(name) must be an integer between 1 and $(maximum), got $(repr(text))."))
    end
    return parsed
end

_bool_flag(raw::AbstractString) =
    lowercase(strip(String(raw))) in ("1", "true", "yes", "on")

# server / process
port() = _parse_int_or(8088, _first_nonempty("BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"))
host_override() = _first_nonempty("BIOCIRCUITS_EXPLORER_HOST", "ROP_HOST")
parent_pid_raw() = _first_nonempty("BIOCIRCUITS_EXPLORER_PARENT_PID", "ROP_PARENT_PID")
public_dir_override() = _first_nonempty("BIOCIRCUITS_EXPLORER_PUBLIC_DIR", "ROP_PUBLIC_DIR")
instance_nonce() = _env_string("BIOCIRCUITS_EXPLORER_INSTANCE_NONCE")

const LOCAL_JOB_MAX_CONCURRENCY_HARD_LIMIT = 64
const LOCAL_JOB_ADMISSION_LIMIT_HARD_LIMIT = 4096
const JOB_CACHE_CAPACITY_HARD_LIMIT = 65_536

function local_job_max_concurrency()
    default = min(max(Threads.nthreads(), 1), 2)
    return _strict_positive_int(
        "BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY",
        _env_string("BIOCIRCUITS_EXPLORER_LOCAL_JOB_MAX_CONCURRENCY"),
        default;
        maximum=LOCAL_JOB_MAX_CONCURRENCY_HARD_LIMIT,
    )
end

function local_job_admission_limit()
    return _strict_positive_int(
        "BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT",
        _env_string("BIOCIRCUITS_EXPLORER_LOCAL_JOB_ADMISSION_LIMIT"),
        64;
        maximum=LOCAL_JOB_ADMISSION_LIMIT_HARD_LIMIT,
    )
end

function job_cache_capacity()
    return _strict_positive_int(
        "BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY",
        _env_string("BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY"),
        1024;
        maximum=JOB_CACHE_CAPACITY_HARD_LIMIT,
    )
end

job_store_override() = _env_string("BIOCIRCUITS_EXPLORER_JOB_STORE")

# Atlas SQLite
atlas_sqlite_persist_mode_raw() = String(get(ENV, ATLAS_SQLITE_PERSIST_MODE_ENV, ""))
atlas_sqlite_lightweight_raw() = String(get(ENV, ATLAS_SQLITE_LIGHTWEIGHT_ENV, ""))
atlas_store_root_override() = _env_string("BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT")
allow_http_sqlite_paths() =
    _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS", ""))

# Static assets
allow_local_images() = _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES", ""))

end # module
