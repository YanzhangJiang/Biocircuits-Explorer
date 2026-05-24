# Loaded as a plain include at the end of BiocircuitsExplorerBackend (so that
# all `handle_*` functions, `QuotaExceeded`, and `handle_jobs_route` are
# already defined in this module's namespace).

const API_ROUTES = Dict{String, Function}(
    "/api/build_atlas"          => handle_build_atlas,
    "/api/query_atlas"          => handle_query_atlas,
    "/api/build_atlas_library"  => handle_build_atlas_library,
    "/api/merge_atlas_library"  => handle_merge_atlas_library,
    "/api/run_inverse_design"   => handle_run_inverse_design,
    "/api/build_model"          => handle_build_model,
    "/api/find_vertices"        => handle_find_vertices,
    "/api/build_graph"          => handle_build_graph,
    "/api/siso_paths"           => handle_siso_paths,
    "/api/siso_polyhedra"       => handle_siso_polyhedra,
    "/api/siso_path_condition"  => handle_siso_path_condition,
    "/api/siso_trajectory"      => handle_siso_trajectory,
    "/api/behavior_families"    => handle_behavior_families,
    "/api/rop_cloud"            => handle_rop_cloud,
    "/api/vertex_detail"        => handle_vertex_detail,
    "/api/fret_heatmap"         => handle_fret_heatmap,
    "/api/parameter_scan_1d"    => handle_parameter_scan_1d,
    "/api/parameter_scan_2d"    => handle_parameter_scan_2d,
    "/api/atlas_landscape_2d"   => handle_atlas_landscape_2d,
    "/api/rop_polyhedron"       => handle_rop_polyhedron,
    "/api/ir/network/validate"  => handle_ir_network_validate,
    "/api/ir/design/validate"   => handle_ir_design_validate,
    "/api/import/sbml"          => handle_import_sbml,
    "/api/export/sbml"          => handle_export_sbml,
    "/api/debug_logs"           => handle_debug_logs,
)

const _CORS_HEADERS = [
    "Access-Control-Allow-Origin"  => "*",
    "Access-Control-Allow-Methods" => "POST, GET, OPTIONS",
    # Authorization is non-simple and triggers preflight; we have to list it
    # explicitly. Without this header the macOS WebView (origin
    # http://127.0.0.1:18088) cannot reach the EC2 broker
    # (origin https://…) for /api/jobs/* and /api/auth/config.
    "Access-Control-Allow-Headers" => "Content-Type, Authorization, X-Biocircuits-Explorer-Debug-Client, X-ROP-Debug-Client",
    "Access-Control-Expose-Headers" => "X-API-Deprecation",
    "Access-Control-Max-Age"       => "600",
]

# ─── API versioning ───
# `v1` is the canonical current API surface. The bare `/api/<endpoint>` form
# is kept as a deprecated alias so existing browser clients and the SwiftUI
# shell keep working without a coordinated rollout. Both forms route to the
# same handlers via `_canonicalize_api_path`; legacy calls additionally get an
# `X-API-Deprecation` response header so callers can detect they are on a
# sunsetting path during the migration window.
const API_CURRENT_VERSION = "v1"
const API_V1_PREFIX = "/api/v1"
const API_LEGACY_SUNSET = "2027-05-25"
const API_LEGACY_DEPRECATION_HEADER =
    "version=\"v0-legacy\"; sunset=\"$API_LEGACY_SUNSET\"; link=\"$API_V1_PREFIX/\""

# Returns (canonical_path, called_as_legacy).
#  - "/api/v1/foo"     -> ("/api/foo",     false)   canonical v1 caller
#  - "/api/v1" | "/api/v1/" -> ("/api/v1", false)   exact v1 root (discovery)
#  - "/api/foo"        -> ("/api/foo",     true)    legacy bare-/api caller
#  - anything else     -> (path,           false)   non-API path; passthrough
function _canonicalize_api_path(path::AbstractString)
    if path == API_V1_PREFIX || path == API_V1_PREFIX * "/"
        return (API_V1_PREFIX, false)
    elseif startswith(path, API_V1_PREFIX * "/")
        return ("/api/" * path[length(API_V1_PREFIX)+2:end], false)
    elseif startswith(path, "/api/")
        return (String(path), true)
    else
        return (String(path), false)
    end
end

# Quick check whether a canonical path is one we actually route. Used to
# decide whether to attach the deprecation header to a legacy response.
function _is_known_api_path(path::AbstractString)
    haskey(API_ROUTES, path) && return true
    path == "/api/version"     && return true
    path == "/api/auth/config" && return true
    path == "/api/local-image" && return true
    (path == "/api/jobs" || startswith(path, "/api/jobs/")) && return true
    return false
end

function _with_cors(resp::HTTP.Response)
    for (name, value) in _CORS_HEADERS
        # Don't clobber an explicit origin chosen by a handler (none do today,
        # but it's cheap insurance).
        HTTP.hasheader(resp, name) || push!(resp.headers, name => value)
    end
    return resp
end

# Allowed characters for client-supplied request IDs. Conservative on purpose
# — anything else could end up unescaped in log lines or response headers.
_request_id_char_ok(c::AbstractChar) =
    isletter(c) || isdigit(c) || c in ('-', '_', ':', '.')

function _ensure_request_id(req)
    incoming = HTTP.header(req, "X-Request-Id", "")
    if !isempty(incoming) && length(incoming) <= 128 &&
       all(_request_id_char_ok, incoming)
        return String(incoming)
    end
    return _generate_request_id()
end

# UUID-shaped identifier built from rand() segments. Not strictly RFC4122
# (we don't set version/variant bits) but plenty of entropy for log
# correlation and short enough to read in a terminal.
function _generate_request_id()
    h1 = string(rand(UInt32), base=16, pad=8)
    h2 = string(rand(UInt16), base=16, pad=4)
    h3 = string(rand(UInt16), base=16, pad=4)
    h4 = string(rand(UInt16), base=16, pad=4)
    h5 = string(rand(UInt32), base=16, pad=8) *
         string(rand(UInt16), base=16, pad=4)
    return string(h1, "-", h2, "-", h3, "-", h4, "-", h5)
end

# Map a raw request path to a low-cardinality label for Prometheus. Anything
# we recognize stays explicit; jobs paths collapse on the variable id;
# anything else lands in a single "static" bucket. The point is to keep the
# series count bounded — Prometheus performance degrades with unbounded
# label cardinality.
function _metric_path_label(raw_path::AbstractString)
    canonical, _ = _canonicalize_api_path(raw_path)
    canonical == "/health"          && return "/health"
    canonical == "/ready"           && return "/ready"
    canonical == "/metrics"         && return "/metrics"
    canonical == API_V1_PREFIX      && return API_V1_PREFIX
    canonical == "/api/version"     && return "/api/version"
    canonical == "/api/auth/config" && return "/api/auth/config"
    canonical == "/api/local-image" && return "/api/local-image"
    (canonical == "/api/jobs" || startswith(canonical, "/api/jobs/")) &&
        return "/api/jobs/:id"
    haskey(API_ROUTES, canonical) && return canonical
    return "static"
end

function _client_ip(req)
    fwd = HTTP.header(req, "X-Forwarded-For", "")
    if !isempty(fwd)
        return String(strip(split(fwd, ",")[1]))
    end
    return ""
end

# Safe character truncation: never splits a multi-byte codepoint. The
# trailing ellipsis flags truncation in logs.
function _truncate_for_log(s::AbstractString, n::Int)
    length(s) <= n && return String(s)
    return string(first(s, n), "…")
end

function router(req)
    request_id = _ensure_request_id(req)
    started_ns = time_ns()

    response = try
        _with_cors(_router_impl(req))
    catch e
        # _router_impl already wraps API handler errors via
        # _api_response_with_error_mapping. Reaching here means a
        # router-level bug; synthesize a 500 so metrics + logs still
        # see the request rather than the exception propagating out
        # to HTTP.jl's default handler.
        @error "Router-level exception" exception=(e, catch_backtrace())
        _with_cors(error_response("Internal server error"; status=500))
    end

    elapsed_s = (time_ns() - started_ns) / 1e9

    if !HTTP.hasheader(response, "X-Request-Id")
        push!(response.headers, "X-Request-Id" => request_id)
    end

    raw_path = HTTP.URI(req.target).path
    path_label = _metric_path_label(raw_path)
    counter_inc!("bcx_http_requests_total",
        (req.method, path_label, string(response.status)))
    hist_observe!("bcx_http_request_duration_seconds",
        (req.method, path_label), elapsed_s)

    if json_logs_enabled()
        log_request_json(stderr, Dict{String, Any}(
            "ts"         => iso_timestamp(),
            "level"      => response.status >= 500 ? "ERROR" :
                            response.status >= 400 ? "WARN"  : "INFO",
            "event"      => "http_request",
            "method"     => req.method,
            "path"       => path_label,
            "raw_path"   => raw_path,
            "status"     => response.status,
            "latency_ms" => round(elapsed_s * 1000; digits=3),
            "request_id" => request_id,
            "client_ip"  => _client_ip(req),
            "user_agent" => _truncate_for_log(HTTP.header(req, "User-Agent", ""), 200),
        ))
    end

    return response
end

function _api_response_with_error_mapping(handler, path::AbstractString)
    try
        return handler()
    catch e
        @error "API error" path exception=(e, catch_backtrace())
        if e isa QuotaExceeded
            return error_response(sprint(showerror, e); status=429)
        elseif is_request_error(e)
            return error_response("Invalid request: $(sprint(showerror, e))"; status=400)
        else
            return error_response("Internal server error"; status=500)
        end
    end
end

function _router_impl(req)
    if req.method == "OPTIONS"
        return HTTP.Response(204, _CORS_HEADERS)
    end

    raw_path = HTTP.URI(req.target).path
    canonical, is_legacy = _canonicalize_api_path(raw_path)

    response = _dispatch_api(req, canonical)
    if response === nothing
        return serve_static(req)
    end

    if is_legacy && _is_known_api_path(canonical) &&
       !HTTP.hasheader(response, "X-API-Deprecation")
        push!(response.headers, "X-API-Deprecation" => API_LEGACY_DEPRECATION_HEADER)
    end
    return response
end

# Dispatch an already-canonicalized request. Returns `nothing` if `path` is
# not part of the API surface, in which case the caller falls back to static
# asset serving. Splitting this out keeps `_router_impl` focused on the
# v1-versus-legacy bookkeeping.
function _dispatch_api(req, path::AbstractString)::Union{HTTP.Response, Nothing}
    # Liveness / readiness probes live at the root (not under /api/) so
    # container orchestrators and load balancers can hit them without
    # knowing the API surface. HEAD is allowed because some health-check
    # tools issue HEAD by default to avoid wasting bandwidth on bodies.
    if path == "/health"
        req.method in ("GET", "HEAD") || return error_response("Method not allowed"; status=405)
        return handle_health(req)
    end
    if path == "/ready"
        req.method in ("GET", "HEAD") || return error_response("Method not allowed"; status=405)
        return handle_ready(req)
    end
    if path == "/metrics"
        req.method in ("GET", "HEAD") || return error_response("Method not allowed"; status=405)
        return handle_metrics(req)
    end

    if haskey(API_ROUTES, path) && req.method != "POST"
        return error_response("Method not allowed"; status=405)
    end

    # `/api/v1` (with or without trailing slash) is a discovery endpoint:
    # it returns the same payload as /api/version so a client can probe
    # which API versions the backend speaks before issuing real requests.
    if path == API_V1_PREFIX
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return handle_version(req)
    end

    if path == "/api/version"
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return handle_version(req)
    end

    if path == "/api/auth/config"
        req.method in ("GET", "POST") || return error_response("Method not allowed"; status=405)
        return handle_auth_config(req)
    end

    if path == "/api/local-image"
        req.method == "GET" || return error_response("Method not allowed"; status=405)
        return handle_local_image(req)
    end

    if path == "/api/jobs" || startswith(path, "/api/jobs/")
        client_id = debug_client_id_from_request(req)
        return with_debug_client_scope(client_id) do
            _api_response_with_error_mapping(() -> handle_jobs_route(req, path), path)
        end
    end

    if haskey(API_ROUTES, path)
        client_id = debug_client_id_from_request(req)
        return with_debug_client_scope(client_id) do
            _api_response_with_error_mapping(() -> API_ROUTES[path](req), path)
        end
    end

    return nothing
end

# ─── Start server ───
function main()
    install_debug_logger!()
    port = resolve_port()
    expected_parent_pid = configured_parent_pid()
    @info "ROP Web Server starting on http://localhost:$port"
    @info "Static files from: $(static_dir())"
    @info "Session TTL: $(SESSION_TTL)s, cleanup interval: $(SESSION_CLEANUP_INTERVAL)s"

    @async cleanup_old_sessions()
    if expected_parent_pid !== nothing
        @async parent_watchdog_loop(expected_parent_pid)
    end

    HTTP.serve(router, "0.0.0.0", port)
end

function julia_main()::Cint
    try
        main()
        return 0
    catch err
        append_debug_log("ERROR", "Backend crashed during startup";
            module_name=:BiocircuitsExplorerBackend,
            details=sprint(showerror, err, catch_backtrace()))
        showerror(stderr, err, catch_backtrace())
        println(stderr)
        return 1
    end
end
