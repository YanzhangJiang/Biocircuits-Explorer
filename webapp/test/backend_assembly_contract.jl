using Test
using BiocircuitsExplorerBackend

@testset "Backend assembly stays a thin ordered include graph" begin
    source_path = normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl"))
    source = read(source_path, String)
    source_lines = split(source, '\n')

    ordered_components = [
        "runtime_lifecycle.jl",
        "request_support.jl",
        "sync_work_budget.jl",
        "path_work_budget.jl",
        "analysis_serializers.jl",
        "analysis_computation.jl",
        "cancellation.jl",
        "atlas.jl",
        "behavior_program_codec.jl",
        "ro_field_identity.jl",
        "ro_field_behavior.jl",
        "atlas_sqlite.jl",
        "ro_field_atlas.jl",
        "inverse_design.jl",
        "atlas_build_budget.jl",
        "atlas_corpus_budget.jl",
        "atlas_query_budget.jl",
        "ir.jl",
        "sbml.jl",
        "version.jl",
        "result_artifact.jl",
        "auth.jl",
        "jobs.jl",
        joinpath("latent_atlas", "phenotype_pipeline.jl"),
        "service_handlers.jl",
        "model_runtime.jl",
        "ro_field_contract.jl",
        "ro_field_chunks.jl",
        "ro_field_slices.jl",
        "ro_field_campaign.jl",
        "ro_field_jobs.jl",
        "ro_field_sparse_jobs.jl",
        "ro_field_differential.jl",
        "ro_field_api.jl",
        "model_handlers.jl",
        "parameter_placement.jl",
        "rop_shape_replay.jl",
        "rop_shape_optimization.jl",
        "design_search.jl",
        "parameter_level.jl",
        "parameter_scan_handlers.jl",
        "rop_geometry_handlers.jl",
        "designability_feasible_regions.jl",
        "designability.jl",
        "rop_shape_api.jl",
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
    @test length(source_lines) <= 170
    @test !occursin(r"(?m)^function\s+handle_", source)
    @test !occursin(r"(?m)^handle_[A-Za-z0-9_]*\([^\n]*\)\s*=", source)
    @test !occursin(r"(?m)^function\s+(?:_?placer|_design|design_search|design_screen)", source)
    @test !occursin(r"(?m)^function\s+(?:build_model_bundle|resolve_model_bundle|_resolve_bundle_or_response)", source)

    # All source files above share one module namespace, so private bindings
    # must remain disjoint even when the include graph grows or is reordered.
    private_binding_names = function(component_source::AbstractString)
        names = Set{String}()
        patterns = (
            r"(?m)^(?:@inline\s+)?function\s+(_[A-Za-z][A-Za-z0-9_]*)\s*\(",
            r"(?m)^(?:@inline\s+)?(_[A-Za-z][A-Za-z0-9_]*)\s*\(",
            r"(?m)^const\s+(_[A-Za-z][A-Za-z0-9_]*)\s*=",
        )
        for pattern in patterns, matched in eachmatch(pattern, component_source)
            push!(names, only(matched.captures))
        end
        return names
    end

    behavior_source = read(normpath(joinpath(
        @__DIR__, "..", "src", "ro_field_behavior.jl")), String)
    slices_source = read(normpath(joinpath(
        @__DIR__, "..", "src", "ro_field_slices.jl")), String)
    behavior_private = private_binding_names(behavior_source)
    slices_private = private_binding_names(slices_source)

    @test isempty(intersect(behavior_private, slices_private))
    @test "_rofb_limit" in behavior_private
    @test "_rofs_limit" in slices_private
    @test !occursin(r"\b_rofs_", behavior_source)
    @test !occursin(r"\b_rofb_", slices_source)

    signature_error = try
        BiocircuitsExplorerBackend._rofb_limit(:cells, BigInt(2), 1)
        nothing
    catch err
        err
    end
    slice_error = try
        BiocircuitsExplorerBackend._rofs_limit(:slice_points, BigInt(2), 1)
        nothing
    catch err
        err
    end
    @test signature_error isa
        BiocircuitsExplorerBackend.ROFieldSignatureLimitExceeded
    @test signature_error.phase === :cells
    @test slice_error isa
        BiocircuitsExplorerBackend.ROFieldSliceLimitExceeded
    @test slice_error.phase === :slice_points
end
