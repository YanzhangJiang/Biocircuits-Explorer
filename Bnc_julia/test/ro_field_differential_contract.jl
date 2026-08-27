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

struct _RODifferentialCancelled <: Exception end
struct _RODifferentialMissedCancellation <: Exception end

struct _CountingReactionOrders{N,A<:AbstractArray{Float64,N}} <:
        AbstractArray{Float64,N}
    data::A
    reads::Base.RefValue{Int}
end

Base.size(values::_CountingReactionOrders) = size(values.data)
Base.axes(values::_CountingReactionOrders) = axes(values.data)
Base.IndexStyle(::Type{<:_CountingReactionOrders}) = IndexCartesian()

@inline function Base.getindex(values::_CountingReactionOrders, indices...)
    values.reads[] += 1
    return getindex(values.data, indices...)
end

function _zero_output_sampled_field()
    field = _quadratic_sampled_field()
    return SampledReactionOrderField(
        copy(field.axis_indices), deepcopy(field.axis_coordinates_log10),
        Int[], copy(field.fixed_logqK),
        Array{Float64}(undef, 3, 3, 0),
        Array{Float64}(undef, 3, 3, 0, 2),
        copy(field.validity), copy(field.regime_ids))
end

function _zero_input_sampled_field()
    return SampledReactionOrderField(
        Int[], Vector{Vector{Float64}}(), [1], Float64[],
        [0.0], Array{Float64}(undef, 1, 0),
        fill(true), fill(1))
end

function _two_output_sampled_field()
    field = _quadratic_sampled_field()
    return SampledReactionOrderField(
        copy(field.axis_indices), deepcopy(field.axis_coordinates_log10),
        [1, 2], copy(field.fixed_logqK),
        cat(field.output_log10, field.output_log10; dims=3),
        cat(field.reaction_orders, field.reaction_orders; dims=3),
        copy(field.validity), copy(field.regime_ids))
end

function _high_dimensional_counting_field(dimension::Int)
    grid_shape = ntuple(_ -> 2, dimension)
    reaction_reads = Ref(0)
    reaction_orders = _CountingReactionOrders(
        zeros(Float64, (grid_shape..., 1, dimension)), reaction_reads)
    field = SampledReactionOrderField(
        collect(1:dimension),
        [[0.0, 1.0] for _ in 1:dimension],
        [1],
        zeros(dimension),
        zeros(Float64, (grid_shape..., 1)),
        reaction_orders,
        trues(grid_shape),
        ones(Int, grid_shape),
    )
    return field, reaction_reads
end

function _constant_curvature(cell_shape::NTuple{2,Int})
    input_dimension = 2
    output_count = 1
    tensor_shape = (cell_shape..., output_count,
        input_dimension, input_dimension)
    gradient = zeros(Float64, tensor_shape)
    symmetric = zeros(Float64, tensor_shape)
    mixed = zeros(Float64, tensor_shape)
    for cell_index in CartesianIndices(cell_shape)
        point = Tuple(cell_index)
        mixed[point..., 1, 1, 1] = NaN
        mixed[point..., 1, 2, 2] = NaN
        mixed[point..., 1, 1, 2] = 1.0
        mixed[point..., 1, 2, 1] = 1.0
    end
    total_cells = prod(cell_shape)
    return ROFiniteDifferenceCurvature(
        :complete,
        true,
        collect(cell_shape),
        total_cells,
        total_cells,
        0,
        trues(cell_shape),
        gradient,
        symmetric,
        mixed,
        zeros(Float64, (cell_shape..., output_count)),
        zeros(Float64, (cell_shape..., output_count, input_dimension)),
    )
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

    public_worst_face = pair.worst_face_base_index
    public_worst_face[1] = 99
    @test pair.worst_face_base_index[1] != 99

    tampered_certificate = certify_sampled_ro_integrability(
        field; absolute_tolerance=1e-11, relative_tolerance=1e-11)
    tampered_pair = first(getfield(
        tampered_certificate, :pair_summaries))
    getfield(tampered_pair, :worst_face_base_index)[1] = 99
    @test_throws ArgumentError tampered_certificate.status

    tightly_bounded_certificate = RODiscreteIntegrabilityCertificate(
        getfield(certificate, :status),
        getfield(certificate, :complete),
        getfield(certificate, :input_dimension),
        getfield(certificate, :output_count),
        getfield(certificate, :total_face_count),
        getfield(certificate, :evaluated_face_count),
        getfield(certificate, :invalid_face_count),
        getfield(certificate, :violating_face_count),
        getfield(certificate, :absolute_tolerance),
        getfield(certificate, :relative_tolerance),
        getfield(certificate, :pair_summaries);
        max_input_dimension=2,
        max_tensor_elements=3,
        max_output_bytes=544,
        max_tensor_work=32,
    )
    @test tightly_bounded_certificate.status == certificate.status
    @test_throws RODifferentialLimitExceeded RODiscreteIntegrabilityCertificate(
        getfield(certificate, :status),
        getfield(certificate, :complete),
        getfield(certificate, :input_dimension),
        getfield(certificate, :output_count),
        getfield(certificate, :total_face_count),
        getfield(certificate, :evaluated_face_count),
        getfield(certificate, :invalid_face_count),
        getfield(certificate, :violating_face_count),
        getfield(certificate, :absolute_tolerance),
        getfield(certificate, :relative_tolerance),
        getfield(certificate, :pair_summaries);
        max_tensor_elements=2,
    )

    admission_checks = Ref(0)
    admission_cancel = function ()
        admission_checks[] += 1
        admission_checks[] >= 13 && throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled RODiscreteIntegrabilityCertificate(
        getfield(certificate, :status),
        getfield(certificate, :complete),
        getfield(certificate, :input_dimension),
        getfield(certificate, :output_count),
        getfield(certificate, :total_face_count),
        getfield(certificate, :evaluated_face_count),
        getfield(certificate, :invalid_face_count),
        getfield(certificate, :violating_face_count),
        getfield(certificate, :absolute_tolerance),
        getfield(certificate, :relative_tolerance),
        getfield(certificate, :pair_summaries);
        cancel_check=admission_cancel,
    )
    @test admission_checks[] == 13

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
    @test isempty(insufficient.pair_summaries)
    @test_throws ArgumentError RODiscreteIntegrabilityCertificate(
        :insufficient_grid, false, 2, 1, 0, 0, 0, 0,
        1e-8, 1e-6, ROIntegrabilityPairSummary[])

    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        field; max_faces=3)
    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        field; max_face_output_evaluations=3)
    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        field; max_tensor_work=1)
    @test certify_sampled_ro_integrability(
        field; max_tensor_work=196).complete
    @test_throws RODifferentialLimitExceeded certify_sampled_ro_integrability(
        _two_output_sampled_field(); max_tensor_work=196)

    @test_throws ArgumentError certify_sampled_ro_integrability(
        _zero_output_sampled_field())
    @test_throws ArgumentError certify_sampled_ro_integrability(
        _zero_input_sampled_field())

    cancellation_checks = Ref(0)
    cancel_check = function ()
        cancellation_checks[] += 1
        cancellation_checks[] >= 12 && throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled certify_sampled_ro_integrability(
        field; cancel_check)
    @test cancellation_checks[] == 12

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


    public_mixed_curvature = curvature.mixed_output_curvature
    public_mixed_curvature[1, 1, 1, 1, 2] = -3.0
    public_validity = curvature.validity
    public_validity[1, 1] = false
    unchanged_classification = classify_finite_window_synergy(curvature)
    @test unchanged_classification.classification[1, 1, 1, 1, 2] ==
        :synergistic_under_policy
    @test curvature.validity[1, 1]

    one_dimensional_gradient = reshape([2.0], 1, 1, 1, 1)
    one_dimensional_symmetric = copy(one_dimensional_gradient)
    one_dimensional_mixed = reshape([NaN], 1, 1, 1, 1)
    one_dimensional_residual = reshape([0.0], 1, 1)
    eigenvalue_input = reshape([2.0], 1, 1, 1)
    one_dimensional_curvature = ROFiniteDifferenceCurvature(
        :complete, true, [1], 1, 1, 0, trues(1),
        one_dimensional_gradient, one_dimensional_symmetric,
        one_dimensional_mixed, one_dimensional_residual, eigenvalue_input,
    )
    eigenvalue_input[1] = 999.0
    @test only(one_dimensional_curvature.hessian_eigenvalues) == 2.0
    published_eigenvalues = one_dimensional_curvature.hessian_eigenvalues
    published_eigenvalues[1] = 999.0
    @test only(one_dimensional_curvature.hessian_eigenvalues) == 2.0
    @test_throws ArgumentError ROFiniteDifferenceCurvature(
        :complete, true, [1], 1, 1, 0, trues(1),
        one_dimensional_gradient, one_dimensional_symmetric,
        one_dimensional_mixed, one_dimensional_residual,
        reshape([999.0], 1, 1, 1),
    )

    tampered_curvature = estimate_sampled_ro_curvature(field)
    getfield(tampered_curvature, :mixed_output_curvature)[
        1, 1, 1, 1, 2] = -3.0
    @test_throws ArgumentError classify_finite_window_synergy(
        tampered_curvature)
    @test_throws ArgumentError tampered_curvature.status

    tightly_bounded_curvature = ROFiniteDifferenceCurvature(
        getfield(curvature, :status),
        getfield(curvature, :complete),
        getfield(curvature, :cell_shape),
        getfield(curvature, :total_cell_count),
        getfield(curvature, :evaluated_cell_count),
        getfield(curvature, :invalid_cell_count),
        getfield(curvature, :validity),
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        getfield(curvature, :mixed_output_curvature),
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues);
        max_input_dimension=2,
        max_tensor_elements=64,
        max_output_bytes=545,
        max_tensor_work=416,
    )
    @test tightly_bounded_curvature.status == curvature.status
    @test_throws RODifferentialLimitExceeded ROFiniteDifferenceCurvature(
        getfield(curvature, :status),
        getfield(curvature, :complete),
        getfield(curvature, :cell_shape),
        getfield(curvature, :total_cell_count),
        getfield(curvature, :evaluated_cell_count),
        getfield(curvature, :invalid_cell_count),
        getfield(curvature, :validity),
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        getfield(curvature, :mixed_output_curvature),
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues);
        max_tensor_elements=63,
    )

    admission_checks = Ref(0)
    admission_cancel = function ()
        admission_checks[] += 1
        admission_checks[] >= 49 && throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled ROFiniteDifferenceCurvature(
        getfield(curvature, :status),
        getfield(curvature, :complete),
        getfield(curvature, :cell_shape),
        getfield(curvature, :total_cell_count),
        getfield(curvature, :evaluated_cell_count),
        getfield(curvature, :invalid_cell_count),
        getfield(curvature, :validity),
        getfield(curvature, :gradient_jacobian),
        getfield(curvature, :symmetric_hessian),
        getfield(curvature, :mixed_output_curvature),
        getfield(curvature, :antisymmetry_residual),
        getfield(curvature, :hessian_eigenvalues);
        cancel_check=admission_cancel,
    )
    @test admission_checks[] == 49

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
    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        field; max_tensor_elements=63)
    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        field; max_tensor_work=1)
    @test estimate_sampled_ro_curvature(
        field; max_tensor_elements=64, max_tensor_work=640).complete
    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        _two_output_sampled_field(); max_tensor_elements=64)
    @test_throws RODifferentialLimitExceeded estimate_sampled_ro_curvature(
        _two_output_sampled_field(); max_tensor_work=640)

    @test_throws ArgumentError estimate_sampled_ro_curvature(
        _zero_output_sampled_field())
    @test_throws ArgumentError estimate_sampled_ro_curvature(
        _zero_input_sampled_field())

    cancellation_checks = Ref(0)
    cancel_check = function ()
        cancellation_checks[] += 1
        cancellation_checks[] >= 12 && throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled estimate_sampled_ro_curvature(
        field; cancel_check)
    @test cancellation_checks[] == 12

    # One D=12 cell contains 4096 corners. The preflight finite scan reads
    # exactly corners*D reaction-order values. Cancellation is armed only
    # after that scan and must fire within the first 512 edge-loop reads; if
    # the implementation waits until an axis/eigensolve boundary, the callback
    # raises the distinct missed-checkpoint exception instead.
    dimension = 12
    high_dimensional, reaction_reads =
        _high_dimensional_counting_field(dimension)
    pre_inner_reads = (1 << dimension) * dimension
    inner_cancel_check = function ()
        reads = reaction_reads[]
        if pre_inner_reads < reads <= pre_inner_reads + 1_024
            throw(_RODifferentialCancelled())
        elseif reads > pre_inner_reads + 1_024
            throw(_RODifferentialMissedCancellation())
        end
    end
    err = try
        estimate_sampled_ro_curvature(
            high_dimensional; cancel_check=inner_cancel_check)
        nothing
    catch caught
        caught
    end
    @test err isa _RODifferentialCancelled
    @test reaction_reads[] <= pre_inner_reads + 1_024
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

    tightly_bounded = classify_finite_window_synergy(
        curvature;
        max_input_dimension=2,
        max_tensor_elements=64,
        max_classification_elements=16,
        max_output_bytes=784,
        max_tensor_work=684,
    )
    @test tightly_bounded.classification == result.classification

    tightly_bounded_result = ROFiniteWindowSynergy(
        getfield(result, :policy),
        getfield(result, :threshold),
        getfield(result, :classification),
        getfield(result, :pair_summaries);
        max_input_dimension=2,
        max_classification_elements=16,
        max_output_bytes=656,
        max_tensor_work=84,
    )
    @test tightly_bounded_result.classification == result.classification
    @test_throws RODifferentialLimitExceeded ROFiniteWindowSynergy(
        getfield(result, :policy),
        getfield(result, :threshold),
        getfield(result, :classification),
        getfield(result, :pair_summaries);
        max_classification_elements=15,
    )
    result_admission_checks = Ref(0)
    result_admission_cancel = function ()
        result_admission_checks[] += 1
        result_admission_checks[] >= 15 &&
            throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled ROFiniteWindowSynergy(
        getfield(result, :policy),
        getfield(result, :threshold),
        getfield(result, :classification),
        getfield(result, :pair_summaries);
        cancel_check=result_admission_cancel,
    )
    @test result_admission_checks[] == 15
    @test_throws RODifferentialLimitExceeded classify_finite_window_synergy(
        curvature; max_input_dimension=1)
    @test_throws RODifferentialLimitExceeded classify_finite_window_synergy(
        curvature; max_tensor_elements=63)
    classification_limit_error = try
        classify_finite_window_synergy(
            curvature; max_classification_elements=15)
        nothing
    catch caught
        caught
    end
    @test classification_limit_error isa RODifferentialLimitExceeded
    @test classification_limit_error.phase ==
        :synergy_classification_elements
    @test classification_limit_error.requested == 16
    @test_throws RODifferentialLimitExceeded classify_finite_window_synergy(
        curvature; max_output_bytes=783)
    @test_throws RODifferentialLimitExceeded classify_finite_window_synergy(
        curvature; max_tensor_work=683)

    # More than 2,100 checks cover input preflight, hashing/replay, allocation,
    # label construction, and result validation. Arming near the end proves
    # the final label-hash/admission pass remains cooperatively cancellable
    # instead of only checking at entry.
    large_curvature = _constant_curvature((32, 32))
    late_cancel_checks = Ref(0)
    late_cancel = function ()
        late_cancel_checks[] += 1
        late_cancel_checks[] >= 2_155 && throw(_RODifferentialCancelled())
    end
    @test_throws _RODifferentialCancelled classify_finite_window_synergy(
        large_curvature; cancel_check=late_cancel)
    @test late_cancel_checks[] == 2_155

    public_classification = result.classification
    public_classification[1, 1, 1, 1, 2] = :antagonistic_under_policy
    @test result.classification[1, 1, 1, 1, 2] ==
        :synergistic_under_policy

    tampered_result = classify_finite_window_synergy(curvature)
    getfield(tampered_result, :classification)[1, 1, 1, 1, 2] =
        :antagonistic_under_policy
    @test_throws ArgumentError tampered_result.policy

    partial = classify_finite_window_synergy(estimate_sampled_ro_curvature(
        _quadratic_sampled_field(invalidate_first=true)))
    partial_summary = only(partial.pair_summaries)
    @test partial_summary.evaluated_cell_count == 3
    @test partial_summary.unknown_gap_count == 1
    @test partial.classification[1, 1, 1, 1, 2] == :unknown_gap
    @test_throws ArgumentError classify_finite_window_synergy(
        curvature; threshold=-1.0)
end
