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

_bool_flag(raw::AbstractString) =
    lowercase(strip(String(raw))) in ("1", "true", "yes", "on")

# server / process
port() = _parse_int_or(8088, _first_nonempty("BIOCIRCUITS_EXPLORER_PORT", "ROP_PORT"))
host_override() = _first_nonempty("BIOCIRCUITS_EXPLORER_HOST", "ROP_HOST")
parent_pid_raw() = _first_nonempty("BIOCIRCUITS_EXPLORER_PARENT_PID", "ROP_PARENT_PID")
public_dir_override() = _first_nonempty("BIOCIRCUITS_EXPLORER_PUBLIC_DIR", "ROP_PUBLIC_DIR")

# AWS Batch
aws_batch_describe_min_interval_raw() = _env_string("BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL")

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

# Static assets
allow_local_images() = _bool_flag(get(ENV, "BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES", ""))

end # module
