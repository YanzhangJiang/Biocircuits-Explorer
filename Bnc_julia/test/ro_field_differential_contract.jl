using Test
using LinearAlgebra
using BindingAndCatalysis

function _quadratic_sampled_field(; invalidate_first=false)
    axis_u = [-1.0, 0.25, 1.0]
    axis_v = [-2.0, 0.5, 2.0]
    output = Array{Float64}(undef, 3, 3, 1)
    reaction_orders = Array{Float64}(undef, 3, 3, 1, 2)
    for i in eachindex(axis_u), j in eachindex(axis_v)
        u, v = axis_u[i], axis_v[j]
        output[i, j, 1] = u^2 + 3u * v + 2v^2
        reaction_orders[i, j, 1, 1] = 2u + 3v
        reaction_orders[i, j, 1, 2] = 3u + 4v
    end
    validity = trues(3, 3)
    regimes = ones(Int, 3, 3)
    if invalidate_first
        validity[1, 1] = false
        regimes[1, 1] = 0
        output[1, 1, :] .= NaN
        reaction_orders[1, 1, :, :] .= NaN
    end
    return SampledReactionOrderField(
        [1, 2], [axis_u, axis_v], [1], zeros(2),
        output, reaction_orders, validity, regimes)
end

function _nonintegrable_sampled_field()
    field = _quadratic_sampled_field()
    reaction_orders = similar(field.reaction_orders)
    for i in axes(reaction_orders, 1), j in axes(reaction_orders, 2)
        v = field.axis_coordinates_log10[2][j]
        reaction_orders[i, j, 1, 1] = v
        reaction_orders[i, j, 1, 2] = 0.0
    end
    output = zeros(size(field.output_log10))
    return SampledReactionOrderField(
        copy(field.axis_indices), deepcopy(field.axis_coordinates_log10),
        copy(field.output_indices), copy(field.fixed_logqK),
        output, reaction_orders, copy(field.validity), copy(field.regime_ids))
end

@testset "sampled RO-field discrete integrability certificate" begin
    field = _quadratic_sampled_field()
    certificate = certify_sampled_ro_integrability(
        field; absolute_tolerance=1e-11, relative_tolerance=1e-11)
    @test certificate.status == :consistent_on_tested_grid
    @test certificate.complete
    @test certificate.total_face_count == 4
    @test certificate.evaluated_face_count == 4
    @test certificate.invalid_face_count == 0
    @test certificate.violating_face_count == 0
    pair = only(certificate.pair_summaries)
    @test pair.axis_pair == (1, 2)
    @test pair.max_abs_circulation ≤ 1e-12
    @test pair.max_abs_mixed_partial_mismatch ≤ 1e-12
    @test pair.max_abs_output_edge_residual ≤ 1e-12

    nonintegrable = certify_sampled_ro_integrability(
        _nonintegrable_sampled_field();
        absolute_tolerance=1e-12,
        relative_tolerance=0.0,
    )
    @test nonintegrable.status == :discrete_inconsistent
    @test nonintegrable.violating_face_count == 4
    nonintegrable_pair = only(nonintegrable.pair_summaries)
    @test nonintegrable_pair.max_abs_mixed_partial_mismatch ≈ 1.0 atol=1e-12

    partial = certify_sampled_ro_integrability(
        _quadratic_sampled_field(invalidate_first=true))
    @test partial.status == :unknown_gap
    @test !partial.complete
    @test partial.invalid_face_count == 1
    @test partial.evaluated_face_count == 3

    one_dimensional = SampledReactionOrderField(
        [1], [[-1.0, 1.0]], [1], [0.0],
        reshape([-1.0, 1.0], 2, 1),
        reshape([1.0, 1.0], 2, 1, 1),
        trues(2), ones(Int, 2))
    insufficient = certify_sampled_ro_integrability(one_dimensional)
    @test insufficient.status == :insufficient_grid
    @test insufficient.total_face_count == 0

    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        field; max_faces=3)
    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        field; max_face_output_evaluations=3)

    malformed = SampledReactionOrderField(
        [1, 2], [[-1.0, 0.0, 1.0], [-1.0, 0.0, 1.0]], [1], zeros(2),
        copy(field.output_log10), copy(field.reaction_orders),
        copy(field.validity), copy(field.regime_ids))
    malformed.reaction_orders[1, 1, 1, 1] = NaN
    @test_throws ArgumentError certify_sampled_ro_integrability(malformed)
end

@testset "sampled RO-field curvature and synergy diagnostics" begin
    field = _quadratic_sampled_field()
    curvature = estimate_sampled_ro_curvature(field)
    @test curvature.status == :complete
    @test curvature.complete
    @test curvature.cell_shape == [2, 2]
    @test curvature.total_cell_count == 4
    @test all(curvature.validity)
    expected_hessian = [2.0 3.0; 3.0 4.0]
    for cell in CartesianIndices((2, 2))
        index = Tuple(cell)
        @test curvature.gradient_jacobian[index..., 1, :, :] ≈
            expected_hessian atol=1e-12
        @test curvature.symmetric_hessian[index..., 1, :, :] ≈
            expected_hessian atol=1e-12
        @test curvature.antisymmetry_residual[index..., 1] ≤ 1e-12
        @test curvature.mixed_output_curvature[index..., 1, 1, 2] ≈
            3.0 atol=1e-12
        @test curvature.mixed_output_curvature[index..., 1, 2, 1] ≈
            3.0 atol=1e-12
        @test curvature.hessian_eigenvalues[index..., 1, :] ≈
            eigvals(Symmetric(expected_hessian)) atol=1e-12
    end

    partial = estimate_sampled_ro_curvature(
        _quadratic_sampled_field(invalidate_first=true))
    @test partial.status == :partial
    @test !partial.complete
    @test partial.invalid_cell_count == 1
    @test partial.evaluated_cell_count == 3
    @test !partial.validity[1, 1]
    @test all(isnan, partial.symmetric_hessian[1, 1, 1, :, :])

    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        field; max_cells=3)
    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        field; max_corner_visits=15)
end

@testset "explicit finite-window cross-curvature synergy policy" begin
    curvature = estimate_sampled_ro_curvature(_quadratic_sampled_field())
    result = classify_finite_window_synergy(curvature; threshold=1e-10)
    @test result.policy == :positive_log_cross_curvature
    @test result.threshold == 1e-10
    summary = only(result.pair_summaries)
    @test summary.axis_pair == (1, 2)
    @test summary.output_index == 1
    @test summary.evaluated_cell_count == 4
    @test summary.positive_count == 4
    @test summary.negative_count == 0
    @test summary.neutral_count == 0
    @test summary.unknown_gap_count == 0
    @test all(==(:synergistic_under_policy),
        result.classification[:, :, 1, 1, 2])
    @test all(==(:synergistic_under_policy),
        result.classification[:, :, 1, 2, 1])
    @test all(==(:not_applicable), result.classification[:, :, 1, 1, 1])

    partial = classify_finite_window_synergy(estimate_sampled_ro_curvature(
        _quadratic_sampled_field(invalidate_first=true)))
    partial_summary = only(partial.pair_summaries)
    @test partial_summary.evaluated_cell_count == 3
    @test partial_summary.unknown_gap_count == 1
    @test partial.classification[1, 1, 1, 1, 2] == :unknown_gap
    @test_throws ArgumentError classify_finite_window_synergy(
        curvature; threshold=-1.0)
end
