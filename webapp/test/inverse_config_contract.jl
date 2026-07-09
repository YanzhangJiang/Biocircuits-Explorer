using Test

const _IDENTITY_FIELDS = (
    :ro_quantization_digits,
    :ro_quantization_scale,
    :program_identity,
    :support_semantics,
)

function _assert_config_equal_except(actual, expected, exceptions)
    for name in fieldnames(AtlasBehaviorConfig)
        name in exceptions && continue
        @test getfield(actual, name) == getfield(expected, name)
    end
end

@testset "AtlasBehaviorConfig identity survives derived configs" begin
    original = AtlasBehaviorConfig(
        path_scope=:all,
        min_volume_mean=0.125,
        deduplicate=false,
        keep_singular=false,
        keep_nonasymptotic=true,
        compute_volume=true,
        motif_zero_tol=2.5e-8,
        include_path_records=true,
        logqk_min=-8.0,
        logqk_max=9.0,
        ro_quantization_digits=7,
        ro_quantization_scale=10_000_000,
        program_identity="custom_profile_v9",
        support_semantics="custom_support_v4",
    )

    clone = BiocircuitsExplorerBackend.atlas_behavior_config_with(
        original;
        min_volume_mean=0.25,
    )
    @test clone.min_volume_mean == 0.25
    _assert_config_equal_except(clone, original, (:min_volume_mean,))

    summary = BiocircuitsExplorerBackend._summary_behavior_config(original)
    @test summary.compute_volume === false
    @test summary.include_path_records === false
    _assert_config_equal_except(summary, original, (:compute_volume, :include_path_records))

    material = BiocircuitsExplorerBackend._materialization_behavior_config(
        original;
        path_scope=:robust,
        compute_volume=false,
    )
    @test material.path_scope == :robust
    @test material.compute_volume === false
    @test material.include_path_records === true
    _assert_config_equal_except(
        material,
        original,
        (:path_scope, :compute_volume, :include_path_records),
    )

    for name in _IDENTITY_FIELDS
        @test getfield(summary, name) == getfield(original, name)
        @test getfield(material, name) == getfield(original, name)
    end
end
