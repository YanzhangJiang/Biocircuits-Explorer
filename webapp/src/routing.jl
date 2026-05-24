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
    "Access-Control-Max-Age"       => "600",
]

function _with_cors(resp::HTTP.Response)
    for (name, value) in _CORS_HEADERS
        # Don't clobber an explicit origin chosen by a handler (none do today,
        # but it's cheap insurance).
        HTTP.hasheader(resp, name) || push!(resp.headers, name => value)
    end
    return resp
end

router(req) = _with_cors(_router_impl(req))

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

    path = HTTP.URI(req.target).path

    if haskey(API_ROUTES, path) && req.method != "POST"
        return error_response("Method not allowed"; status=405)
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

    return serve_static(req)
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
