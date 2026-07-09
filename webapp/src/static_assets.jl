module StaticAssets

using HTTP

using ..Config

const STATIC_DIR = Ref{Union{Nothing, String}}(nothing)

const LOCAL_IMAGE_MIMES = Dict(
    ".png"  => "image/png",
    ".jpg"  => "image/jpeg",
    ".jpeg" => "image/jpeg",
    ".gif"  => "image/gif",
    ".svg"  => "image/svg+xml",
    ".webp" => "image/webp",
    ".bmp"  => "image/bmp",
)

const _STATIC_MIMES = Dict(
    ".html" => "text/html",
    ".js"   => "application/javascript",
    ".css"  => "text/css",
    ".json" => "application/json",
    ".png"  => "image/png",
    ".svg"  => "image/svg+xml",
    ".jpg"  => "image/jpeg",
    ".jpeg" => "image/jpeg",
    ".gif"  => "image/gif",
    ".mp4"  => "video/mp4",
    ".webm" => "video/webm",
)

function resolve_static_dir()
    candidates = String[]

    env_static_dir = Config.public_dir_override()
    if !isempty(env_static_dir)
        push!(candidates, abspath(expanduser(env_static_dir)))
    end

    program_file = try
        Base.PROGRAM_FILE
    catch
        ""
    end

    if !isempty(program_file)
        exe_dir = dirname(abspath(program_file))
        append!(candidates, [
            normpath(joinpath(exe_dir, "..", "share", "biocircuits-explorer", "public")),
            normpath(joinpath(exe_dir, "..", "Resources", "public")),
            normpath(joinpath(exe_dir, "..", "resources", "public")),
        ])
    end

    push!(candidates, normpath(joinpath(@__DIR__, "..", "public")))

    for candidate in unique(candidates)
        isdir(candidate) && return candidate
    end

    error("Could not locate web assets. Checked: $(join(unique(candidates), ", "))")
end

function static_dir()
    if STATIC_DIR[] === nothing
        STATIC_DIR[] = resolve_static_dir()
    end
    return STATIC_DIR[]::String
end

function _is_subpath(path::AbstractString, root::AbstractString)
    norm_path = normpath(path)
    norm_root = normpath(root)
    norm_path == norm_root && return true
    return startswith(norm_path, norm_root * Base.Filesystem.path_separator)
end

"""
    local_images_enabled(; has_parent_pid)

`has_parent_pid` is true when the backend was launched by a local supervisor.
That shortcut is safe only while the effective bind host remains loopback.
Public binds must opt in via `BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES=1`;
otherwise the local-image endpoint would expose arbitrary user files on a
network-reachable port.
"""
function local_images_enabled(; has_parent_pid::Bool = false)
    if has_parent_pid
        configured_host = lowercase(strip(Config.host_override()))
        (isempty(configured_host) || configured_host in ("127.0.0.1", "localhost", "::1")) &&
            return true
    end
    return Config.allow_local_images()
end

function _normalized_http_origin(uri::HTTP.URI)
    scheme = lowercase(String(uri.scheme))
    scheme in ("http", "https") || return nothing
    isempty(uri.host) && return nothing
    isempty(uri.userinfo) || return nothing
    isempty(uri.path) || uri.path == "/" || return nothing
    isempty(uri.query) || return nothing
    isempty(uri.fragment) || return nothing

    port = if isempty(uri.port)
        scheme == "https" ? 443 : 80
    else
        tryparse(Int, String(uri.port))
    end
    port === nothing && return nothing
    1 <= port <= 65535 || return nothing
    return (scheme, lowercase(String(uri.host)), port)
end

_is_loopback_origin_host(host::AbstractString) =
    lowercase(String(host)) in ("127.0.0.1", "localhost", "::1")

function _local_image_origin_allowed(req; require_loopback_origin::Bool = false)
    origin = strip(HTTP.header(req, "Origin", ""))
    isempty(origin) && return true

    host = strip(HTTP.header(req, "Host", ""))
    isempty(host) && return false
    forwarded_proto = strip(first(split(HTTP.header(req, "X-Forwarded-Proto", ""), ',')))
    scheme = isempty(forwarded_proto) ? "http" : lowercase(forwarded_proto)
    scheme in ("http", "https") || return false

    origin_uri = try
        HTTP.URI(origin)
    catch
        return false
    end
    request_uri = try
        HTTP.URI("$(scheme)://$(host)")
    catch
        return false
    end
    normalized_origin = _normalized_http_origin(origin_uri)
    normalized_request = _normalized_http_origin(request_uri)
    normalized_origin !== nothing || return false
    require_loopback_origin && !_is_loopback_origin_host(normalized_origin[2]) && return false
    return normalized_origin == normalized_request
end

function handle_local_image(req; has_parent_pid::Bool = false)
    if !local_images_enabled(; has_parent_pid = has_parent_pid)
        return HTTP.Response(403, "Local image serving is disabled in this deployment")
    end
    if !_local_image_origin_allowed(req; require_loopback_origin=has_parent_pid)
        return HTTP.Response(403, "Cross-origin local image access is forbidden")
    end

    uri = HTTP.URI(req.target)
    query = HTTP.queryparams(uri)
    raw_path = get(query, "path", "")
    isempty(raw_path) && return HTTP.Response(400, "Missing ?path=")

    requested = expanduser(String(raw_path))
    ext = lowercase(splitext(requested)[2])
    haskey(LOCAL_IMAGE_MIMES, ext) || return HTTP.Response(415, "Unsupported image extension: $ext")

    isfile(requested) || return HTTP.Response(404, "Not found")

    bytes = try
        read(requested)
    catch e
        @error "Failed to read local image" path=requested exception=(e, catch_backtrace())
        return HTTP.Response(500, "Failed to read file")
    end

    return HTTP.Response(200, [
        "Content-Type"  => LOCAL_IMAGE_MIMES[ext],
        "Cache-Control" => "private, max-age=60",
    ], bytes)
end

function serve_static(req)
    path = HTTP.URI(req.target).path
    relative_path = path == "/" ? "index.html" : lstrip(path, '/')
    root = static_dir()
    filepath = normpath(joinpath(root, relative_path))

    if !_is_subpath(filepath, root) || !isfile(filepath)
        return HTTP.Response(404, "Not found")
    end

    ext = splitext(filepath)[2]
    content_type = get(_STATIC_MIMES, ext, "application/octet-stream")
    # Disable caching for static assets. The macOS shell's WKWebView would
    # otherwise apply heuristic caching to HTML/JS modules (no Cache-Control
    # → cache for ~10% of Last-Modified age), which masks frontend updates
    # behind stale module imports even after "Reload Shell".
    return HTTP.Response(200, [
        "Content-Type"  => content_type,
        "Cache-Control" => "no-store",
    ], read(filepath))
end

end # module
