module APIContractTests

using Test
using HTTP
using JSON3
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

@testset "Executable API route contract" begin
    routes = Backend.API_ROUTE_CONTRACTS

    @test length(routes) == 48
    @test length(filter(Backend._is_ordinary_post_route, routes)) == 36
    @test count(route -> route.match_kind === :template, routes) == 5
    @test length(Backend.API_ROUTES) == 36

    canonical_paths = getfield.(routes, :canonical_path)
    internal_paths = getfield.(routes, :internal_path)
    legacy_aliases = [route.legacy_alias for route in routes if route.legacy_alias !== nothing]
    @test allunique(canonical_paths)
    @test allunique(internal_paths)
    @test allunique(legacy_aliases)

    for route in routes
        @test Backend._resolve_api_route_handler(route) ===
              getfield(Backend, route.handler)
        @test all(method -> Backend._api_route_allows_method(route, method), route.methods)
        @test !Backend._api_route_allows_method(route, "TRACE")

        if Backend._is_ordinary_post_route(route)
            @test Backend.API_ROUTES[route.internal_path] ===
                  Backend._resolve_api_route_handler(route)
        end

        if startswith(route.canonical_path, Backend.API_V1_PREFIX * "/")
            @test Backend._canonicalize_api_path(route.canonical_path) ==
                  (route.internal_path, false)
            @test route.legacy_alias !== nothing
            @test Backend._canonicalize_api_path(route.legacy_alias) ==
                  (route.internal_path, true)
        end
    end

    @test Backend._canonicalize_api_path("/api/v1") == ("/api/v1", false)
    @test Backend._canonicalize_api_path("/api/v1/") == ("/api/v1", false)

    jobs_root = Backend._match_api_route("/api/jobs")
    jobs_status = Backend._match_api_route("/api/jobs/job-123")
    jobs_result = Backend._match_api_route("/api/jobs/job-123/result")
    jobs_url = Backend._match_api_route("/api/jobs/job-123/result-url")
    jobs_cancel = Backend._match_api_route("/api/jobs/job-123/cancel")
    @test jobs_root.internal_path == "/api/jobs"
    @test jobs_status.internal_path == "/api/jobs/{job_id}"
    @test jobs_result.internal_path == "/api/jobs/{job_id}/result"
    @test jobs_url.internal_path == "/api/jobs/{job_id}/result-url"
    @test jobs_cancel.internal_path == "/api/jobs/{job_id}/cancel"
    @test Backend._match_api_route("/api/jobs/job-123/unknown") === nothing
    @test Backend._match_api_route("/api/jobs//result") === nothing

    # Method decisions in the router consume the metadata. These requests do
    # not need valid bodies because rejection happens before ordinary handlers.
    @test Backend.router(HTTP.Request("GET", "/api/v1/build_model")).status == 405
    @test Backend.router(HTTP.Request("DELETE", "/api/v1")).status == 405
    @test Backend.router(HTTP.Request("POST", "/health")).status == 405
    @test Backend.router(HTTP.Request("POST", "/api/v1/local-image")).status == 405
    @test Backend.router(HTTP.Request("DELETE", "/api/v1/jobs/job-123")).status == 405
    @test Backend.router(HTTP.Request("OPTIONS", "/any/path")).status == 204

    first_json = Backend.api_contract_reference_json()
    second_json = Backend.api_contract_reference_json()
    @test first_json == second_json
    @test !occursin("timestamp", lowercase(first_json))
    @test !occursin("generated_at", lowercase(first_json))

    reference = JSON3.read(first_json)
    @test reference["schema_version"] == "1"
    @test reference["route_count"] == 48
    @test length(reference["routes"]) == 48
    @test reference["routes"][1]["canonical_path"] == "/api/v1"
    @test reference["routes"][1]["methods"] == ["GET", "POST"]
end

end # module
