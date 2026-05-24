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

`has_parent_pid` is true when the backend was launched by the native macOS
shell (which sets a parent-PID env var). Deployed servers leave this false
and must opt in via `BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES=1`; otherwise
the local-image endpoint would expose arbitrary user files on a public port.
"""
function local_images_enabled(; has_parent_pid::Bool = false)
    has_parent_pid && return true
    return Config.allow_local_images()
end

function handle_local_image(req; has_parent_pid::Bool = false)
    if !local_images_enabled(; has_parent_pid = has_parent_pid)
        return HTTP.Response(403, "Local image serving is disabled in this deployment")
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
