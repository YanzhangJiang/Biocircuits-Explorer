# ─── API Route Handlers ───

function handle_build_atlas(req)
    body = _normalize_http_atlas_paths(read_json(req))
    enforce_sync_atlas_request_budget(body, :handle_build_atlas)
    return json_response(build_behavior_atlas_from_spec(body))
end

function handle_query_atlas(req)
    body = _normalize_http_atlas_paths(read_json(req))
    enforce_sync_atlas_request_budget(body, :handle_query_atlas)
    return json_response(query_behavior_atlas_from_spec(body))
end

function handle_build_atlas_library(req)
    body = _normalize_http_atlas_paths(read_json(req))
    enforce_sync_atlas_request_budget(body, :handle_build_atlas_library)
    return json_response(build_atlas_library_from_spec(body))
end

function handle_merge_atlas_library(req)
    body = _normalize_http_atlas_paths(read_json(req))
    enforce_sync_atlas_request_budget(body, :handle_merge_atlas_library)
    return json_response(merge_atlas_library_from_spec(body))
end

function handle_run_inverse_design(req)
    body = _normalize_http_atlas_paths(read_json(req))
    enforce_sync_atlas_request_budget(body, :handle_run_inverse_design)
    return json_response(run_inverse_design_from_spec(body))
end

# GET /health — liveness probe. Returns 200 as long as the Julia process is
# answering. Cheap, no I/O. The native shell and start.sh poll it.
function handle_health(req)
    initialized = _STARTUP_TIME_NS[] != 0
    uptime_s = initialized ? (time_ns() - _STARTUP_TIME_NS[]) / 1e9 : 0.0
    return json_response(Dict{String, Any}(
        "status"          => "ok",
        "service"         => "biocircuits-explorer-backend",
        "instance_nonce"  => Config.instance_nonce(),
        "version"         => biocircuits_explorer_version(),
        "revision"        => strip(get(ENV, "BIOCIRCUITS_EXPLORER_REVISION", "unknown")),
        "uptime_seconds"  => round(uptime_s; digits=3),
    ))
end

# GET /ready — readiness probe. Reports whether static assets and the job
# store are available; 503 until they are.
function handle_ready(req)
    initialized = _STARTUP_TIME_NS[] != 0
    static_root = try
        static_dir()
    catch err
        @warn "Readiness static asset resolution failed" exception=(err, catch_backtrace())
        nothing
    end
    static_ok = static_root !== nothing && isdir(static_root)
    browser_entrypoint_ok = static_ok && isfile(joinpath(static_root, "index.html"))
    native_entrypoint_ok = static_ok && isfile(joinpath(static_root, "index-node.html"))
    job_store_ok = local_job_store_ready()
    checks = Dict{String, Any}(
        "module_initialized" => initialized,
        "static_assets"      => static_ok,
        "browser_entrypoint" => browser_entrypoint_ok,
        "native_entrypoint"  => native_entrypoint_ok,
        "job_store"          => job_store_ok,
    )
    ready = initialized && static_ok && browser_entrypoint_ok && native_entrypoint_ok && job_store_ok
    return json_response(Dict{String, Any}(
        "status"         => ready ? "ready" : "not_ready",
        "service"        => "biocircuits-explorer-backend",
        "instance_nonce" => Config.instance_nonce(),
        "checks"         => checks,
    ); status = ready ? 200 : 503)
end

function handle_version(req)
    info = Dict{String, Any}(biocircuits_explorer_build_info())
    # API protocol identity, separate from the application build metadata above.
    # `api_version` is the canonical current surface; `api_supported` lets a
    # client decide whether to fall back.
    info["api_version"] = API_CURRENT_VERSION
    info["api_supported"] = [API_CURRENT_VERSION]
    return json_response(info)
end

# POST /api/v1/export/sbml — accepts a NetworkIR (top-level or under
# `network`) or the legacy {reactions, kd} shape, returns an SBML L3 string.
function handle_export_sbml(req)
    body = read_json(req)
    network_payload = _raw_haskey(body, :network) ? _raw_get(body, :network, nothing) : body
    network = try
        parse_network_ir(network_payload)
    catch err
        err isa IRValidationError &&
            return error_response(sprint(showerror, err); status = 400)
        rethrow(err)
    end
    return json_response(Dict(
        "sbml" => network_ir_to_sbml(network),
        "label" => network.label,
    ))
end

# POST /api/v1/import/sbml — accepts {sbml: "<xml string>"}, returns the
# parsed NetworkIR plus a list of warnings about anything not representable.
function handle_import_sbml(req)
    body = read_json(req)
    xml = _raw_get(body, :sbml, nothing)
    (xml isa AbstractString && !isempty(strip(xml))) ||
        return error_response("Missing required field `sbml` (the SBML document as a string)"; status = 400)
    network, warnings = try
        sbml_to_network_ir(String(xml))
    catch err
        err isa IRValidationError &&
            return error_response(sprint(showerror, err); status = 400)
        rethrow(err)
    end
    return json_response(Dict(
        "network_ir" => network_ir_to_dict(network),
        "warnings" => warnings,
    ))
end
