# Executable HTTP-route metadata. This file is included by `routing.jl` only
# after every handler has been defined, so handler names can be resolved and
# checked without introducing an include-order dependency on function values.

struct APIRouteContract
    canonical_path::String
    internal_path::String
    methods::Tuple{Vararg{String}}
    handler::Symbol
    legacy_alias::Union{Nothing, String}
    match_kind::Symbol
end

const API_CURRENT_VERSION = "v1"
const API_V1_PREFIX = "/api/v1"
const API_LEGACY_SUNSET = "2027-05-25"
const API_LEGACY_DEPRECATION_HEADER =
    "version=\"v0-legacy\"; sunset=\"$API_LEGACY_SUNSET\"; link=\"$API_V1_PREFIX/\""

# Keep this declaration ordered: the same order is emitted by the deterministic
# reference exporter. `internal_path` is the path after v1 canonicalization;
# templates use `{job_id}` for their one variable segment.
const API_ROUTE_CONTRACTS = APIRouteContract[
    # Version discovery.
    APIRouteContract("/api/v1", "/api/v1", ("GET", "POST"), :handle_version, nothing, :exact),

    # Ordinary JSON POST handlers (the historical API_ROUTES surface).
    APIRouteContract("/api/v1/build_atlas", "/api/build_atlas", ("POST",), :handle_build_atlas, "/api/build_atlas", :exact),
    APIRouteContract("/api/v1/query_atlas", "/api/query_atlas", ("POST",), :handle_query_atlas, "/api/query_atlas", :exact),
    APIRouteContract("/api/v1/build_atlas_library", "/api/build_atlas_library", ("POST",), :handle_build_atlas_library, "/api/build_atlas_library", :exact),
    APIRouteContract("/api/v1/merge_atlas_library", "/api/merge_atlas_library", ("POST",), :handle_merge_atlas_library, "/api/merge_atlas_library", :exact),
    APIRouteContract("/api/v1/run_inverse_design", "/api/run_inverse_design", ("POST",), :handle_run_inverse_design, "/api/run_inverse_design", :exact),
    APIRouteContract("/api/v1/build_model", "/api/build_model", ("POST",), :handle_build_model, "/api/build_model", :exact),
    APIRouteContract("/api/v1/find_vertices", "/api/find_vertices", ("POST",), :handle_find_vertices, "/api/find_vertices", :exact),
    APIRouteContract("/api/v1/build_graph", "/api/build_graph", ("POST",), :handle_build_graph, "/api/build_graph", :exact),
    APIRouteContract("/api/v1/siso_paths", "/api/siso_paths", ("POST",), :handle_siso_paths, "/api/siso_paths", :exact),
    APIRouteContract("/api/v1/siso_polyhedra", "/api/siso_polyhedra", ("POST",), :handle_siso_polyhedra, "/api/siso_polyhedra", :exact),
    APIRouteContract("/api/v1/siso_path_condition", "/api/siso_path_condition", ("POST",), :handle_siso_path_condition, "/api/siso_path_condition", :exact),
    APIRouteContract("/api/v1/siso_trajectory", "/api/siso_trajectory", ("POST",), :handle_siso_trajectory, "/api/siso_trajectory", :exact),
    APIRouteContract("/api/v1/behavior_families", "/api/behavior_families", ("POST",), :handle_behavior_families, "/api/behavior_families", :exact),
    APIRouteContract("/api/v1/phenotype_classify", "/api/phenotype_classify", ("POST",), :handle_phenotype_classify, "/api/phenotype_classify", :exact),
    APIRouteContract("/api/v1/rop_cloud", "/api/rop_cloud", ("POST",), :handle_rop_cloud, "/api/rop_cloud", :exact),
    APIRouteContract("/api/v1/vertex_detail", "/api/vertex_detail", ("POST",), :handle_vertex_detail, "/api/vertex_detail", :exact),
    APIRouteContract("/api/v1/fret_heatmap", "/api/fret_heatmap", ("POST",), :handle_fret_heatmap, "/api/fret_heatmap", :exact),
    APIRouteContract("/api/v1/parameter_scan_1d", "/api/parameter_scan_1d", ("POST",), :handle_parameter_scan_1d, "/api/parameter_scan_1d", :exact),
    APIRouteContract("/api/v1/parameter_scan_2d", "/api/parameter_scan_2d", ("POST",), :handle_parameter_scan_2d, "/api/parameter_scan_2d", :exact),
    APIRouteContract("/api/v1/place_parameters", "/api/place_parameters", ("POST",), :handle_place_parameters, "/api/place_parameters", :exact),
    APIRouteContract("/api/v1/placer_menu", "/api/placer_menu", ("POST",), :handle_placer_menu, "/api/placer_menu", :exact),
    APIRouteContract("/api/v1/placer_curve", "/api/placer_curve", ("POST",), :handle_placer_curve, "/api/placer_curve", :exact),
    APIRouteContract("/api/v1/placer_threshold", "/api/placer_threshold", ("POST",), :handle_placer_threshold, "/api/placer_threshold", :exact),
    APIRouteContract("/api/v1/placer_realize_program", "/api/placer_realize_program", ("POST",), :handle_placer_realize_program, "/api/placer_realize_program", :exact),
    APIRouteContract("/api/v1/placer_level", "/api/placer_level", ("POST",), :handle_placer_level, "/api/placer_level", :exact),
    APIRouteContract("/api/v1/design_search", "/api/design_search", ("POST",), :handle_design_search, "/api/design_search", :exact),
    APIRouteContract("/api/v1/design_screen", "/api/design_screen", ("POST",), :handle_design_screen, "/api/design_screen", :exact),
    APIRouteContract("/api/v1/validate_designability_spec", "/api/validate_designability_spec", ("POST",), :handle_validate_designability_spec, "/api/validate_designability_spec", :exact),
    APIRouteContract("/api/v1/design_labels", "/api/design_labels", ("POST",), :handle_design_labels, "/api/design_labels", :exact),
    APIRouteContract("/api/v1/atlas_landscape_2d", "/api/atlas_landscape_2d", ("POST",), :handle_atlas_landscape_2d, "/api/atlas_landscape_2d", :exact),
    APIRouteContract("/api/v1/rop_polyhedron", "/api/rop_polyhedron", ("POST",), :handle_rop_polyhedron, "/api/rop_polyhedron", :exact),
    APIRouteContract("/api/v1/ir/network/validate", "/api/ir/network/validate", ("POST",), :handle_ir_network_validate, "/api/ir/network/validate", :exact),
    APIRouteContract("/api/v1/ir/design/validate", "/api/ir/design/validate", ("POST",), :handle_ir_design_validate, "/api/ir/design/validate", :exact),
    APIRouteContract("/api/v1/import/sbml", "/api/import/sbml", ("POST",), :handle_import_sbml, "/api/import/sbml", :exact),
    APIRouteContract("/api/v1/export/sbml", "/api/export/sbml", ("POST",), :handle_export_sbml, "/api/export/sbml", :exact),
    APIRouteContract("/api/v1/debug_logs", "/api/debug_logs", ("POST",), :handle_debug_logs, "/api/debug_logs", :exact),

    # Version-adjacent public endpoints.
    APIRouteContract("/api/v1/version", "/api/version", ("GET", "POST"), :handle_version, "/api/version", :exact),
    APIRouteContract("/api/v1/auth/config", "/api/auth/config", ("GET", "POST"), :handle_auth_config, "/api/auth/config", :exact),
    APIRouteContract("/api/v1/local-image", "/api/local-image", ("GET",), :handle_local_image, "/api/local-image", :exact),

    # Root operations endpoints.
    APIRouteContract("/health", "/health", ("GET", "HEAD"), :handle_health, nothing, :exact),
    APIRouteContract("/ready", "/ready", ("GET", "HEAD"), :handle_ready, nothing, :exact),
    APIRouteContract("/metrics", "/metrics", ("GET", "HEAD"), :handle_metrics, nothing, :exact),

    # Job routes are declared as templates as one family, including its root.
    APIRouteContract("/api/v1/jobs", "/api/jobs", ("POST",), :handle_jobs_route, "/api/jobs", :template),
    APIRouteContract("/api/v1/jobs/{job_id}", "/api/jobs/{job_id}", ("GET", "POST"), :handle_jobs_route, "/api/jobs/{job_id}", :template),
    APIRouteContract("/api/v1/jobs/{job_id}/result", "/api/jobs/{job_id}/result", ("GET", "POST"), :handle_jobs_route, "/api/jobs/{job_id}/result", :template),
    APIRouteContract("/api/v1/jobs/{job_id}/result-url", "/api/jobs/{job_id}/result-url", ("GET", "POST"), :handle_jobs_route, "/api/jobs/{job_id}/result-url", :template),
    APIRouteContract("/api/v1/jobs/{job_id}/cancel", "/api/jobs/{job_id}/cancel", ("POST",), :handle_jobs_route, "/api/jobs/{job_id}/cancel", :template),
]

_template_segment(segment::AbstractString) =
    startswith(segment, "{") && endswith(segment, "}") && length(segment) > 2

function _api_template_matches(template::AbstractString, path::AbstractString)
    template_parts = split(strip(String(template), '/'), '/')
    path_parts = split(strip(String(path), '/'), '/')
    length(template_parts) == length(path_parts) || return false
    return all(zip(template_parts, path_parts)) do (expected, actual)
        _template_segment(expected) ? !isempty(actual) : expected == actual
    end
end

function _api_route_matches(route::APIRouteContract, path::AbstractString)
    route.match_kind === :exact && return route.internal_path == path
    route.match_kind === :template && return _api_template_matches(route.internal_path, path)
    throw(ArgumentError("Unsupported API route match kind: $(route.match_kind)"))
end

function _match_api_route(path::AbstractString)
    for route in API_ROUTE_CONTRACTS
        _api_route_matches(route, path) && return route
    end
    return nothing
end

_api_route_allows_method(route::APIRouteContract, method::AbstractString) =
    String(method) in route.methods

function _resolve_api_route_handler(route::APIRouteContract)
    isdefined(@__MODULE__, route.handler) ||
        throw(ArgumentError("API handler is not defined: $(route.handler)"))
    handler = getfield(@__MODULE__, route.handler)
    handler isa Function ||
        throw(ArgumentError("API handler is not callable: $(route.handler)"))
    return handler
end

# A route is an ordinary POST route precisely when it is an exact, versioned,
# POST-only handler. The five jobs entries use `:template`, so their POST root
# cannot accidentally enter this compatibility dictionary.
_is_ordinary_post_route(route::APIRouteContract) =
    route.match_kind === :exact &&
    route.methods == ("POST",) &&
    startswith(route.canonical_path, API_V1_PREFIX * "/")

function api_contract_reference_facts()
    routes = [(
        canonical_path=route.canonical_path,
        internal_path=route.internal_path,
        methods=collect(route.methods),
        handler=String(route.handler),
        legacy_alias=route.legacy_alias,
        match_kind=String(route.match_kind),
    ) for route in API_ROUTE_CONTRACTS]

    return (
        schema_version="1",
        api_version=API_CURRENT_VERSION,
        legacy_sunset=API_LEGACY_SUNSET,
        route_count=length(routes),
        routes=routes,
    )
end

api_contract_reference_json() = JSON3.write(api_contract_reference_facts())
