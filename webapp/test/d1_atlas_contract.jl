using Test
using BiocircuitsExplorerBackend

@testset "Single-base homomer atlas contract" begin
    candidate = Dict{String, Any}(
        "label" => "d1_homomer",
        "reactions" => Any["A + A <-> AA"],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["A", "AA"],
        "source_kind" => "single_reaction",
        "source_metadata" => Dict("d" => 1, "mu" => 2),
    )
    behavior_configs = [
        "unbounded" => AtlasBehaviorConfig(
            path_scope=:feasible,
            min_volume_mean=0.0,
            include_path_records=false,
            compute_volume=false,
            keep_singular=true,
        ),
        "bounded" => AtlasBehaviorConfig(
            path_scope=:feasible,
            min_volume_mean=0.0,
            include_path_records=false,
            compute_volume=false,
            keep_singular=true,
            logqk_min=-2.0,
            logqk_max=2.0,
        ),
    ]
    profile = AtlasSearchProfile(
        name="periodic_d_mu_v0_backend_projection",
        max_base_species=1,
        max_reactions=1,
        max_support=2,
        allow_homomeric_templates=true,
        max_homomer_order=2,
        slice_mode=:change,
        input_mode=:totals_only,
    )
    expansion = AtlasChangeExpansionSpec(
        mode=:axes_only,
        max_active_dims=1,
        include_axis_slices=true,
        include_negative_directions=false,
    )

    for (scope, behavior) in behavior_configs
        @testset "$scope qK domain" begin
            atlas = build_behavior_atlas(
                Any[candidate];
                search_profile=profile,
                behavior_config=behavior,
                change_expansion=expansion,
                network_parallelism=1,
            )

            @test atlas["successful_network_count"] == 1
            @test atlas["failed_network_count"] == 0
            @test length(atlas["behavior_slices"]) == 2
            @test all(slice -> slice["analysis_status"] == "ok", atlas["behavior_slices"])
            @test Set(slice["output_symbol"] for slice in atlas["behavior_slices"]) == Set(["A", "AA"])
        end
    end
end
