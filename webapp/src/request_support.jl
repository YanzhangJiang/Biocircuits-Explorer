# Internal helper: like `fixed_qK_or_default`, but uses the raw-JSON accessors
# defined in canonicalization.jl so it can read JSON3 objects without a
# `haskey` method.
function _request_bool(raw, name::AbstractString)
    raw isa Bool || throw(ArgumentError("$name must be a boolean"))
    return raw
end

function _request_string(raw, name::AbstractString; nonempty::Bool=true)
    raw isa AbstractString || throw(ArgumentError("$name must be a string"))
    value = String(raw)
    nonempty && isempty(strip(value)) &&
        throw(ArgumentError("$name must be a non-empty string"))
    return value
end

const _SESSION_ID_PATTERN = r"^[A-Za-z0-9_.:-]+$"
const _NETWORK_IR_HASH_PATTERN = r"^[0-9A-Fa-f]{64}$"

function _request_session_id(raw)
    value = _request_string(raw, "session_id")
    ncodeunits(value) <= 128 ||
        throw(ArgumentError("session_id must be at most 128 bytes"))
    occursin(_SESSION_ID_PATTERN, value) || throw(ArgumentError(
        "session_id contains unsupported characters"))
    return value
end

function _request_network_ir_hash(raw)
    value = _request_string(raw, "network_ir_hash")
    occursin(_NETWORK_IR_HASH_PATTERN, value) || throw(ArgumentError(
        "network_ir_hash must contain exactly 64 hexadecimal characters"))
    return lowercase(value)
end

function _request_finite_real(raw, name::AbstractString)
    (raw isa Real && !(raw isa Bool)) ||
        throw(ArgumentError("$name must be a finite non-boolean number"))
    value = try
        Float64(raw)
    catch
        throw(ArgumentError("$name must be a finite non-boolean number"))
    end
    isfinite(value) || throw(ArgumentError("$name must be finite"))
    return value
end

function _http_atlas_store_root()
    configured = Config.atlas_store_root_override()
    root = isempty(configured) ?
        normpath(joinpath(@__DIR__, "..", "atlas_store")) :
        normpath(abspath(expanduser(configured)))
    return abspath(root)
end

function _path_is_within(child::AbstractString, root::AbstractString)
    relative = relpath(child, root)
    return relative == "." ||
           (relative != ".." && !startswith(relative, "..$(Base.Filesystem.path_separator)"))
end

function _nearest_existing_path(path::AbstractString)
    current = String(path)
    while !ispath(current)
        parent = dirname(current)
        parent == current && return current
        current = parent
    end
    return current
end

function _normalize_http_sqlite_path(raw)
    Config.allow_http_sqlite_paths() || throw(ArgumentError(
        "sqlite_path is disabled on HTTP APIs; use an in-memory corpus or an operator-managed offline workflow"))
    raw isa AbstractString ||
        throw(ArgumentError("sqlite_path must be a string within the configured atlas store"))
    text = strip(String(raw))
    isempty(text) && return ""

    root = _http_atlas_store_root()
    expanded = expanduser(text)
    candidate = isabspath(expanded) ?
        normpath(abspath(expanded)) :
        normpath(joinpath(root, expanded))
    candidate == root && throw(ArgumentError("sqlite_path must name a database file"))
    ispath(candidate) && !isfile(candidate) &&
        throw(ArgumentError("sqlite_path must name a database file"))
    _path_is_within(candidate, root) || throw(ArgumentError(
        "sqlite_path must stay within the configured atlas store"))

    # If the store already exists, resolve the nearest existing ancestor to
    # reject symlink escapes for both existing DBs and newly-created files.
    if ispath(root)
        root_real = realpath(root)
        ancestor_real = realpath(_nearest_existing_path(candidate))
        _path_is_within(ancestor_real, root_real) || throw(ArgumentError(
            "sqlite_path resolves outside the configured atlas store"))
    end
    return candidate
end

function _normalize_http_atlas_paths(raw, depth::Int=0)
    depth <= 4 || throw(ArgumentError("HTTP atlas specification nesting is too deep"))
    if raw isa AbstractDict || raw isa JSON3.Object
        out = Dict{String, Any}(String(key) => value for (key, value) in pairs(raw))
        if haskey(out, "sqlite_path")
            out["sqlite_path"] = _normalize_http_sqlite_path(out["sqlite_path"])
        end
        # Only these wrapper fields are executable path-bearing specs. Avoid a
        # recursive walk through arbitrary query/corpus metadata (which could
        # be deeply nested and may legitimately contain provenance keys named
        # `sqlite_path`).
        for key in ("atlas_spec", "spec")
            haskey(out, key) || continue
            value = out[key]
            if value isa AbstractDict || value isa JSON3.Object
                out[key] = _normalize_http_atlas_paths(value, depth + 1)
            end
        end
        return out
    end
    return _materialize(raw)
end

function _fixed_qK_or_default_raw(body, model, kd::AbstractVector{<:Real})
    fixed_qK = if _raw_haskey(body, :fixed_qK)
        raw = _raw_get(body, :fixed_qK, nothing)
        raw isa AbstractVector ||
            throw(ArgumentError("`fixed_qK` must be an array of finite non-boolean numbers."))
        all(value -> value isa Real && !(value isa Bool), raw) ||
            throw(ArgumentError("`fixed_qK` must be an array of finite non-boolean numbers."))
        values = Float64.(collect(raw))
        all(isfinite, values) ||
            throw(ArgumentError("`fixed_qK` must be an array of finite non-boolean numbers."))
        values
    else
        default_log_qK(model, kd)
    end
    length(fixed_qK) == model.n ||
        error("Length of `fixed_qK` must equal the full q/K dimension ($(model.n)).")
    return fixed_qK
end

function handle_debug_logs(req)
    body = read_json(req)
    result = DebugLog.read_logs(
        after_seq = sync_bounded_int(
            get(body, :after_seq, 0), "after_seq";
            min=typemin(Int), max=typemax(Int)),
        limit = sync_bounded_int(
            get(body, :limit, 300), "limit";
            min=typemin(Int), max=typemax(Int)),
        client_id = debug_client_id_from_request(req),
    )
    return json_response(Dict(
        "entries"  => result.entries,
        "next_seq" => result.next_seq,
        "total"    => result.total,
        "limit"    => result.limit,
    ))
end
