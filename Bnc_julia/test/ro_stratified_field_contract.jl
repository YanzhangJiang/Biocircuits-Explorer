using Test
using LinearAlgebra
using BindingAndCatalysis

if !isdefined(BindingAndCatalysis, :ROStratifiedFieldLimits)
    Base.include(BindingAndCatalysis,
        joinpath(@__DIR__, "..", "src", "rop", "ro_stratified_field.jl"))
end

const _ROSF = BindingAndCatalysis

struct ROStratifiedCancelProbe <: Exception end

function _max_pwa_complex(;
    second_offset=[0.0, 0.0],
    drop_facet_id=nothing,
    internal_incidence=[1, 2],
    second_matrix=[0.0 1.0; 0.0 10.0],
)
    domain = ROInputDomain2D(
        (1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(2))
    first_label = ROAffineLabel2D(
        [1], [1.0 0.0; 10.0 0.0], [0.0, 0.0])
    second_label = ROAffineLabel2D(
        [2], Matrix{Float64}(second_matrix), Float64.(second_offset))
    cells = ROCell2D[
        ROCell2D(
            1,
            [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0)],
            2.0,
            [1],
            [first_label],
            false,
        ),
        ROCell2D(
            2,
            [(-1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)],
            2.0,
            [2],
            [second_label],
            false,
        ),
    ]
    invsqrt2 = inv(sqrt(2.0))
    facets = ROFacet2D[
        ROFacet2D(
            1,
            :internal,
            ((-1.0, -1.0), (1.0, 1.0)),
            Int.(internal_incidence),
            [1],
            (invsqrt2, -invsqrt2),
            0.0,
            true,
            nothing,
        ),
        ROFacet2D(
            2,
            :domain,
            ((-1.0, -1.0), (1.0, -1.0)),
            [1],
            Int[],
            (0.0, 1.0),
            1.0,
            false,
            :axis2_lower,
        ),
        ROFacet2D(
            3,
            :domain,
            ((1.0, -1.0), (1.0, 1.0)),
            [1],
            Int[],
            (1.0, 0.0),
            -1.0,
            false,
            :axis1_upper,
        ),
        ROFacet2D(
            4,
            :domain,
            ((-1.0, 1.0), (1.0, 1.0)),
            [2],
            Int[],
            (0.0, 1.0),
            -1.0,
            false,
            :axis2_upper,
        ),
        ROFacet2D(
            5,
            :domain,
            ((-1.0, -1.0), (-1.0, 1.0)),
            [2],
            Int[],
            (1.0, 0.0),
            1.0,
            false,
            :axis1_lower,
        ),
    ]
    drop_facet_id === nothing || filter!(facet -> facet.id != drop_facet_id, facets)
    strata = ROSingularStratum2D[
        ROSingularStratum2D(
            1,
            1,
            [(-1.0, -1.0), (1.0, 1.0)],
            [99],
            [1],
            [:singular_regime],
        ),
    ]
    return ROCellComplex2D(
        domain,
        [1, 2],
        cells,
        facets,
        strata,
        3,
        2,
        4.0,
        4.0,
        0.0,
        true,
        false,
        1e-9,
    )
end

function _rosf_with_cells(complex::ROCellComplex2D, cells::Vector{ROCell2D})
    return ROCellComplex2D(
        complex.domain,
        copy(complex.output_indices),
        cells,
        copy(complex.facets),
        copy(complex.singular_strata),
        complex.candidate_regime_count,
        complex.regular_candidate_count,
        complex.domain_area,
        complex.covered_area_sum,
        complex.gap_area,
        complex.coverage_complete,
        complex.has_ambiguity,
        complex.geometry_tolerance,
    )
end

@testset "exact regular-cell PWA integrability certificate" begin
    complex = _max_pwa_complex()
    certificate = _ROSF.certify_ro_regular_extension_integrability(complex)
    @test certificate.status == :regular_cell_extension_integrable
    @test isempty(certificate.reasons)
    @test certificate.regular_limit_only
    @test !certificate.includes_singular_branch
    @test certificate.checked_cell_count == 2
    @test certificate.checked_facet_count == 5
    @test certificate.checked_internal_facet_count == 1
    @test certificate.checked_domain_facet_count == 4
    @test certificate.checked_continuity_endpoint_count == 2
    @test certificate.max_affine_continuity_residual == 0.0
    @test certificate.singular_stratum_count == 1

    tampered = _ROSF.certify_ro_regular_extension_integrability(
        _max_pwa_complex(second_offset=[1e-4, 0.0]))
    @test tampered.status == :unknown
    @test :discontinuous_affine_output in tampered.reasons
    @test tampered.regular_limit_only
    @test !tampered.includes_singular_branch

    missing = _ROSF.certify_ro_regular_extension_integrability(
        _max_pwa_complex(drop_facet_id=1))
    @test missing.status == :unknown
    @test :missing_cell_boundary_facet in missing.reasons

    wrong_incidence = _ROSF.certify_ro_regular_extension_integrability(
        _max_pwa_complex(internal_incidence=[1]))
    @test wrong_incidence.status == :unknown
    @test :invalid_internal_facet_incidence in wrong_incidence.reasons
    @test :facet_incidence_geometry_mismatch in wrong_incidence.reasons

    malformed_matrix = _ROSF.certify_ro_regular_extension_integrability(
        _max_pwa_complex(second_matrix=reshape([0.0, 1.0], 1, 2)))
    @test malformed_matrix.status == :unknown
    @test :invalid_label_matrix_shape in malformed_matrix.reasons

    base = _max_pwa_complex()
    overlapping_second = ROCell2D(
        2,
        copy(base.cells[1].vertices),
        base.cells[1].area,
        copy(base.cells[2].source_regime_ids),
        copy(base.cells[2].labels),
        false,
    )
    overlap = _ROSF.certify_ro_regular_extension_integrability(
        _rosf_with_cells(base, [base.cells[1], overlapping_second]))
    @test overlap.status == :unknown
    @test :positive_area_cell_overlap in overlap.reasons

    outside_first = ROCell2D(
        1,
        [(-1.0, -1.0), (1.1, -1.0), (1.0, 1.0)],
        2.1,
        copy(base.cells[1].source_regime_ids),
        copy(base.cells[1].labels),
        false,
    )
    outside = _ROSF.certify_ro_regular_extension_integrability(
        _rosf_with_cells(base, [outside_first, base.cells[2]]))
    @test outside.status == :unknown
    @test :cell_outside_domain in outside.reasons
end

@testset "classical and joint Clarke-matrix point queries" begin
    complex = _max_pwa_complex()
    first_matrix = [1.0 0.0; 10.0 0.0]
    second_matrix = [0.0 1.0; 0.0 10.0]

    interior = _ROSF.query_ro_stratified_jacobian(complex, (0.5, -0.5))
    @test interior.status == :classical_jacobian
    @test interior.reason === nothing
    @test interior.generator_cell_ids == [1]
    @test length(interior.jacobian_generators) == 1
    @test only(interior.jacobian_generators) == first_matrix
    @test isempty(interior.singular_stratum_ids)

    boundary = _ROSF.query_ro_stratified_jacobian(complex, (0.0, 0.0))
    @test boundary.status == :clarke_joint_matrix_hull
    @test boundary.reason === nothing
    @test boundary.generator_cell_ids == [1, 2]
    @test length(boundary.jacobian_generators) == 2
    @test boundary.jacobian_generators == [first_matrix, second_matrix]
    @test boundary.facet_ids == [1]
    @test boundary.singular_stratum_ids == [1]
    @test boundary.regular_limit_only
    @test !boundary.includes_singular_branch

    # A row-wise Cartesian product would invent four matrices. The joint
    # Clarke representation keeps exactly the two physically incident MIMO
    # Jacobians and therefore retains cross-output coupling.
    @test length(boundary.jacobian_generators) != 4
    invented = [1.0 0.0; 0.0 10.0]
    @test invented ∉ boundary.jacobian_generators

    domain_boundary = _ROSF.query_ro_stratified_jacobian(
        complex, (1.0, 0.0))
    @test domain_boundary.status == :clarke_joint_matrix_hull
    @test domain_boundary.generator_cell_ids == [1]

    outside = _ROSF.query_ro_stratified_jacobian(complex, (2.0, 0.0))
    @test outside.status == :unknown
    @test outside.reason == :outside_domain
    @test isempty(outside.jacobian_generators)

    untrusted = _ROSF.query_ro_stratified_jacobian(
        _max_pwa_complex(second_offset=[1e-4, 0.0]), (0.0, 0.0))
    @test untrusted.status == :unknown
    @test untrusted.reason == :regular_extension_integrability_unknown
    @test isempty(untrusted.jacobian_generators)
end

@testset "directional generators preserve MIMO coupling" begin
    complex = _max_pwa_complex()
    boundary = _ROSF.query_ro_stratified_jacobian(complex, (0.0, 0.0))
    directional = _ROSF.ro_directional_derivative_generators(
        boundary, (2.0, 3.0))
    @test directional.status == :clarke_joint_matrix_hull
    @test directional.generator_cell_ids == [1, 2]
    @test directional.derivative_generators == [[2.0, 20.0], [3.0, 30.0]]
    @test length(directional.derivative_generators) == 2
    @test [2.0, 30.0] ∉ directional.derivative_generators
    @test [3.0, 20.0] ∉ directional.derivative_generators
    @test directional.direction == (2.0, 3.0)
    @test directional.regular_limit_only
    @test !directional.includes_singular_branch

    direct = _ROSF.query_ro_stratified_directional_derivatives(
        complex, (0.5, -0.5), (-1.0, 4.0))
    @test direct.status == :classical_jacobian
    @test direct.generator_cell_ids == [1]
    @test direct.derivative_generators == [[-1.0, -10.0]]

    @test_throws DimensionMismatch _ROSF.ro_directional_derivative_generators(
        boundary, [1.0])
    @test_throws ArgumentError _ROSF.ro_directional_derivative_generators(
        boundary, [0.0, 0.0])
    @test_throws ArgumentError _ROSF.ro_directional_derivative_generators(
        boundary, [1.0, Inf])
    @test_throws ArgumentError _ROSF.ro_directional_derivative_generators(
        boundary, [true, false])
end

@testset "stratified limits and cooperative cancellation" begin
    complex = _max_pwa_complex()
    @test_throws _ROSF.ROStratifiedFieldLimitExceeded begin
        _ROSF.certify_ro_regular_extension_integrability(
            complex; limits=_ROSF.ROStratifiedFieldLimits(max_cells=1))
    end
    @test_throws _ROSF.ROStratifiedFieldLimitExceeded begin
        _ROSF.certify_ro_regular_extension_integrability(
            complex; limits=_ROSF.ROStratifiedFieldLimits(max_facets=4))
    end
    @test_throws _ROSF.ROStratifiedFieldLimitExceeded begin
        _ROSF.certify_ro_regular_extension_integrability(
            complex;
            limits=_ROSF.ROStratifiedFieldLimits(max_matrix_elements=43),
        )
    end
    @test_throws _ROSF.ROStratifiedFieldLimitExceeded begin
        _ROSF.query_ro_stratified_jacobian(
            complex,
            (0.0, 0.0);
            limits=_ROSF.ROStratifiedFieldLimits(max_generators=1),
        )
    end

    checks = Ref(0)
    @test_throws ROStratifiedCancelProbe begin
        _ROSF.certify_ro_regular_extension_integrability(
            complex;
            cancel_check=() -> begin
                checks[] += 1
                throw(ROStratifiedCancelProbe())
            end,
        )
    end
    @test checks[] == 1

    boundary = _ROSF.query_ro_stratified_jacobian(complex, (0.0, 0.0))
    direction_checks = Ref(0)
    @test_throws ROStratifiedCancelProbe begin
        _ROSF.ro_directional_derivative_generators(
            boundary,
            (1.0, 2.0);
            cancel_check=() -> begin
                direction_checks[] += 1
                throw(ROStratifiedCancelProbe())
            end,
        )
    end
    @test direction_checks[] == 1
end
