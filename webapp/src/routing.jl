# Loaded as a plain include at the end of BiocircuitsExplorerBackend (so that
# all `handle_*` functions, the capacity/budget exception types, and
# `handle_jobs_route` are already defined in this module's namespace).

include(joinpath(@__DIR__, "api_contract.jl"))

const API_ROUTES = Dict{String, Function}(
    route.internal_path => _resolve_api_route_handler(route)
    for route in API_ROUTE_CONTRACTS if _is_ordinary_post_route(route)
)

const _CORS_HEADERS = [
    "Access-Control-Allow-Origin"  => "*",
    "Access-Control-Allow-Methods" => "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers" => "Content-Type, X-Biocircuits-Explorer-Debug-Client, X-ROP-Debug-Client",
    "Access-Control-Max-Age"       => "600",
]

# Map a request path to the internal route path.
#  - "/api/v1/foo"          -> "/api/foo"   (the internal owner path)
#  - "/api/v1" | "/api/v1/" -> "/api/v1"    (version discovery)
#  - anything else          -> unchanged    (static assets, or a 404 below)
# Only /api/v1/* is served; bare /api/* is not an API surface.
function _canonicalize_api_path(path::AbstractString)
    if path == API_V1_PREFIX || path == API_V1_PREFIX * "/"
        return API_V1_PREFIX
    elseif startswith(path, API_V1_PREFIX * "/")
        return "/api/" * path[length(API_V1_PREFIX)+2:end]
    else
        return String(path)
    end
end

_is_unversioned_api_path(path::AbstractString) =
    startswith(path, "/api/") && !startswith(path, API_V1_PREFIX)

function _with_cors(resp::HTTP.Response)
    for (name, value) in _CORS_HEADERS
        # Don't clobber an explicit origin chosen by a handler (none do today,
        # but it's cheap insurance).
        HTTP.hasheader(resp, name) || push!(resp.headers, name => value)
    end
    return resp
end

function router(req)
    return try
        _with_cors(_router_impl(req))
    catch e
        # _router_impl already wraps API handler errors via
        # _api_response_with_error_mapping. Reaching here means a
        # router-level bug; synthesize a 500 rather than letting the
        # exception propagate to HTTP.jl's default handler.
        @error "Router-level exception" exception=(e, catch_backtrace())
        _with_cors(error_response("Internal server error"; status=500))
    end
end

function _api_response_with_error_mapping(handler, path::AbstractString)
    try
        return handler()
    catch e
        if e isa LocalJobCapacityExceeded
            response = json_response(Dict(
                "error" => sprint(showerror, e),
                "code" => "local_job_capacity_exhausted",
                "limit" => e.limit,
                "retry_after_seconds" => 1,
                "retryable" => true,
            ); status=429)
            push!(response.headers, "Retry-After" => "1")
            return response
        elseif e isa RequestBodyTooLarge
            return json_response(Dict(
                "error" => sprint(showerror, e),
                "code" => "request_body_too_large",
                "limit_bytes" => e.limit,
                "retryable" => false,
            ); status=413)
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
    _is_unversioned_api_path(raw_path) &&
        return error_response("API routes are served under /api/v1"; status=404)
    canonical = _canonicalize_api_path(raw_path)

    response = _dispatch_api(req, canonical)
    if response === nothing
        return serve_static(req)
    end

    return response
end

# Dispatch an already-canonicalized request. Returns `nothing` if `path` is
# not part of the API surface, in which case the caller falls back to static
# asset serving.
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
