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

# AWS Batch
aws_batch_describe_min_interval_raw() = _env_string("BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL")

is_valid_aws_batch_region(value) =
    value isa AbstractString &&
    occursin(r"^[a-z]{2,}(?:-[a-z0-9]+)+-[0-9]+$", String(value))

function aws_batch_region()
    region = _first_nonempty(
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    )
    isempty(region) && throw(ArgumentError(
        "AWS Batch region is required; set " *
        "BIOCIRCUITS_EXPLORER_AWS_BATCH_REGION, AWS_REGION, or " *
        "AWS_DEFAULT_REGION."))
    is_valid_aws_batch_region(region) || throw(ArgumentError(
        "AWS Batch region must be a canonical region identifier, got " *
        "$(repr(region))."))
    return region
end

is_valid_aws_account_id(value) =
    value isa AbstractString && occursin(r"^[0-9]{12}$", String(value))

function aws_account_id()
    account_id = _env_string("BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID")
    isempty(account_id) && return nothing
    is_valid_aws_account_id(account_id) || throw(ArgumentError(
        "BIOCIRCUITS_EXPLORER_AWS_ACCOUNT_ID must contain exactly 12 digits, " *
        "got $(repr(account_id))."))
    return account_id
end

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

function aws_batch_job_name_prefix(default::AbstractString = "biocircuits")
    v = _env_string("BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX", default)
    isempty(v) ? String(default) : v
end

allow_aws_batch_request_config() =
    _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_AWS_BATCH_REQUEST_CONFIG", ""))

function aws_cli_binary()
    v = _env_string("BIOCIRCUITS_EXPLORER_AWS_CLI", "aws")
    isempty(v) ? "aws" : v
end

aws_batch_env_value(name::AbstractString) = get(ENV, String(name), "")

job_store_override() = _env_string("BIOCIRCUITS_EXPLORER_JOB_STORE")

# Quota
quota_table() = _env_string("BIOCIRCUITS_EXPLORER_QUOTA_TABLE")
quota_daily_limit_raw() = _env_string("BIOCIRCUITS_EXPLORER_QUOTA_DAILY_LIMIT")

# Cognito / auth
cognito_user_pool_id() = _env_string("BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID")
function cognito_region()
    v = _env_string("BIOCIRCUITS_EXPLORER_COGNITO_REGION")
    isempty(v) ? _env_string("AWS_REGION") : v
end
cognito_app_client_id() = _env_string("BIOCIRCUITS_EXPLORER_COGNITO_APP_CLIENT_ID")
cognito_domain() = _env_string("BIOCIRCUITS_EXPLORER_COGNITO_DOMAIN")
cognito_jwks_url_override() = _env_string("BIOCIRCUITS_EXPLORER_COGNITO_JWKS_URL_OVERRIDE")

# Atlas SQLite
atlas_sqlite_persist_mode_raw() = String(get(ENV, ATLAS_SQLITE_PERSIST_MODE_ENV, ""))
atlas_sqlite_lightweight_raw() = String(get(ENV, ATLAS_SQLITE_LIGHTWEIGHT_ENV, ""))
atlas_store_root_override() = _env_string("BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT")
allow_http_sqlite_paths() =
    _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS", ""))

# Static assets
allow_local_images() = _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES", ""))

end # module
