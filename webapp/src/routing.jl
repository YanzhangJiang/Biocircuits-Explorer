# Loaded as a plain include at the end of BiocircuitsExplorerBackend (so that
# all `handle_*` functions, `QuotaExceeded`, and `handle_jobs_route` are
# already defined in this module's namespace).

include(joinpath(@__DIR__, "api_contract.jl"))

const API_ROUTES = Dict{String, Function}(
    route.internal_path => _resolve_api_route_handler(route)
    for route in API_ROUTE_CONTRACTS if _is_ordinary_post_route(route)
)

const _CORS_HEADERS = [
    "Access-Control-Allow-Origin"  => "*",
    "Access-Control-Allow-Methods" => "POST, GET, OPTIONS",
    # Authorization is non-simple and triggers preflight; we have to list it
    # explicitly. Without this header the macOS WebView (origin
    # http://127.0.0.1:18088) cannot reach the EC2 broker
    # (origin https://…) for /api/jobs/* and /api/auth/config.
    "Access-Control-Allow-Headers" => "Content-Type, Authorization, X-Biocircuits-Explorer-Debug-Client, X-ROP-Debug-Client",
    "Access-Control-Expose-Headers" => "X-API-Deprecation, Retry-After",
    "Access-Control-Max-Age"       => "600",
]

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
    _match_api_route(path) !== nothing && return true
    # Preserve the legacy behavior for malformed paths in the jobs namespace:
    # they are still handled as API 404s and receive a deprecation header.
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
    (canonical == "/api/jobs" || startswith(canonical, "/api/jobs/")) &&
        return "/api/jobs/:id"
    route = _match_api_route(canonical)
    route !== nothing && return route.internal_path
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
        if e isa QuotaExceeded
            return error_response(sprint(showerror, e); status=429)
        elseif e isa RequestBodyTooLarge
            return json_response(Dict(
                "error" => sprint(showerror, e),
                "code" => "request_body_too_large",
                "limit_bytes" => e.limit,
                "retryable" => false,
            ); status=413)
        elseif e isa SyncCapacityExceeded
            response = json_response(Dict(
                "error" => sprint(showerror, e),
                "code" => "sync_capacity_exhausted",
                "retry_after_seconds" => 1,
                "retryable" => true,
            ); status=429)
            push!(response.headers, "Retry-After" => "1")
            return response
        elseif e isa SyncBudgetExceeded
            return json_response(Dict(
                "error" => sprint(showerror, e),
                "code" => "sync_budget_exceeded",
                "retryable" => false,
            ); status=422)
        elseif is_request_error(e)
            return error_response("Invalid request: $(sprint(showerror, e))"; status=400)
        else
            @error "API error" path exception=(e, catch_backtrace())
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
    route = _match_api_route(path)

    # Jobs retain their established authentication-before-method-check order;
    # handle_jobs_route performs the same metadata-derived check after identity
    # resolution. Every other known route can reject here.
    if route !== nothing && route.handler !== :handle_jobs_route &&
       !_api_route_allows_method(route, req.method)
        return error_response("Method not allowed"; status=405)
    end

    if route !== nothing && route.handler === :handle_jobs_route
        client_id = debug_client_id_from_request(req)
        return with_debug_client_scope(client_id) do
            _api_response_with_error_mapping(() -> handle_jobs_route(req, path), path)
        end
    end

    if route !== nothing && haskey(API_ROUTES, route.internal_path)
        client_id = debug_client_id_from_request(req)
        handler = API_ROUTES[route.internal_path]
        return with_debug_client_scope(client_id) do
            _api_response_with_error_mapping(
                () -> with_sync_work_gate(route.handler) do
                    with_request_model_bundle_lock(route.handler, req) do
                        handler(req)
                    end
                end,
                path,
            )
        end
    end

    if route !== nothing
        return _resolve_api_route_handler(route)(req)
    end

    # Preserve the existing jobs-namespace catch-all: malformed job paths are
    # authenticated and returned as JSON 404s rather than falling through to
    # static-file routing.
    if path == "/api/jobs" || startswith(path, "/api/jobs/")
        client_id = debug_client_id_from_request(req)
        return with_debug_client_scope(client_id) do
            _api_response_with_error_mapping(() -> handle_jobs_route(req, path), path)
        end
    end

    return nothing
end

# ─── Start server ───
function main()
    install_debug_logger!()
    port = resolve_port()
    expected_parent_pid = configured_parent_pid()
    host = resolve_host(expected_parent_pid)
    display_host = occursin(':', host) ? "[$(host)]" : host
    @info "ROP Web Server starting" url="http://$(display_host):$(port)" bind_host=host port
    @info "Static files from: $(static_dir())"
    @info "Session TTL: $(SESSION_TTL)s, cleanup interval: $(SESSION_CLEANUP_INTERVAL)s"

    @async cleanup_old_sessions()
    if expected_parent_pid !== nothing
        @async parent_watchdog_loop(expected_parent_pid)
    end

    HTTP.serve(router, host, port)
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
