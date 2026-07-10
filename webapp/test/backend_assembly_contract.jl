using Test

@testset "Backend assembly stays a thin ordered include graph" begin
    source_path = normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl"))
    source = read(source_path, String)
    source_lines = split(source, '\n')

    ordered_components = [
        "runtime_lifecycle.jl",
        "request_support.jl",
        "analysis_serializers.jl",
        "analysis_computation.jl",
        "cancellation.jl",
        "atlas.jl",
        "behavior_program_codec.jl",
        "atlas_sqlite.jl",
        "inverse_design.jl",
        "ir.jl",
        "sbml.jl",
        "version.jl",
        "result_artifact.jl",
        "auth.jl",
        "jobs.jl",
        joinpath("latent_atlas", "phenotype_pipeline.jl"),
        "service_handlers.jl",
        "model_runtime.jl",
        "model_handlers.jl",
        "parameter_placement.jl",
        "design_search.jl",
        "parameter_level.jl",
        "parameter_scan_handlers.jl",
        "rop_geometry_handlers.jl",
        "designability_feasible_regions.jl",
        "designability.jl",
        "routing.jl",
    ]

    include_positions = Int[]
    for relative in ordered_components
        parts = splitpath(relative)
        arguments = join(["\"$(part)\"" for part in parts], ", ")
        statement = "include(joinpath(@__DIR__, $(arguments)))"
        @test count(==(statement), source_lines) == 1
        position = findfirst(statement, source)
        @test position !== nothing
        push!(include_positions, first(position))

        component_path = normpath(joinpath(@__DIR__, "..", "src", relative))
        @test isfile(component_path)
        @test !isempty(strip(read(component_path, String)))
    end

    @test issorted(include_positions)
    @test length(source_lines) <= 160
    @test !occursin(r"(?m)^function\s+handle_", source)
    @test !occursin(r"(?m)^handle_[A-Za-z0-9_]*\([^\n]*\)\s*=", source)
    @test !occursin(r"(?m)^function\s+(?:_?placer|_design|design_search|design_screen)", source)
    @test !occursin(r"(?m)^function\s+(?:build_model_bundle|resolve_model_bundle|_resolve_bundle_or_response)", source)
end
