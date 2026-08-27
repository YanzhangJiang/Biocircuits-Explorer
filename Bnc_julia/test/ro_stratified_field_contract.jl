using Test
using LinearAlgebra
using BindingAndCatalysis

if !isdefined(BindingAndCatalysis, :ROStratifiedFieldLimits)
    Base.include(BindingAndCatalysis,
        joinpath(@__DIR__, "..", "src", "rop", "ro_stratified_field.jl"))
end

const _ROSF = BindingAndCatalysis

struct ROStratifiedCancelProbe <: Exception end

function _rosf_authoritative_complex(
    domain,
    output_indices,
    cells,
    facets,
    singular_strata,
    candidate_regime_count,
    regular_candidate_count,
    domain_area,
    covered_area_sum,
    gap_area,
    coverage_complete,
    has_ambiguity,
    geometry_tolerance;
    limits=ROCellComplexBuildLimits(),
)
    return ROCellComplex2D(
        domain,
        output_indices,
        cells,
        facets,
        singular_strata,
        candidate_regime_count,
        regular_candidate_count,
        domain_area,
        covered_area_sum,
        gap_area,
        coverage_complete,
        has_ambiguity,
        geometry_tolerance,
        nothing,
        limits,
        () -> nothing,
        _ROSF._RO2_BUILDER_SEAL_TOKEN,
    )
end

function _max_pwa_complex(;
    second_offset=[0.0, 0.0],
    drop_facet_id=nothing,
    internal_incidence=[1, 2],
    second_matrix=[0.0 1.0; 0.0 10.0],
    stratum_source_ids=[3],
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
            Int.(stratum_source_ids),
            [1],
            [:singular_regime],
        ),
    ]
    return _rosf_authoritative_complex(
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
    return _rosf_authoritative_complex(
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

function _rosf_external_copy(complex::ROCellComplex2D)
    return ROCellComplex2D(
        complex.domain,
        copy(complex.output_indices),
        copy(complex.cells),
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

function _rosf_scaled_max_pwa_complex(scale::Float64)
    base = _max_pwa_complex()
    cells = ROCell2D[
        ROCell2D(
            cell.id,
            [(scale * point[1], scale * point[2]) for point in cell.vertices],
            scale^2 * cell.area,
            copy(cell.source_regime_ids),
            copy(cell.labels),
            cell.set_valued,
        ) for cell in base.cells
    ]
    facets = ROFacet2D[
        ROFacet2D(
            facet.id,
            facet.kind,
            (
                (scale * facet.endpoints[1][1], scale * facet.endpoints[1][2]),
                (scale * facet.endpoints[2][1], scale * facet.endpoints[2][2]),
            ),
            copy(facet.incident_cell_ids),
            copy(facet.singular_stratum_ids),
            facet.normal,
            scale * facet.offset,
            facet.mixed_sign,
            facet.domain_side,
        ) for facet in base.facets
    ]
    strata = ROSingularStratum2D[
        ROSingularStratum2D(
            stratum.id,
            stratum.dimension,
            [(scale * point[1], scale * point[2]) for point in stratum.vertices],
            copy(stratum.source_regime_ids),
            copy(stratum.nullities),
            copy(stratum.reasons),
        ) for stratum in base.singular_strata
    ]
    return _rosf_authoritative_complex(
        ROInputDomain2D(
            (1, 2), (-scale, -scale), (scale, scale), zeros(2)),
        copy(base.output_indices),
        cells,
        facets,
        strata,
        base.candidate_regime_count,
        base.regular_candidate_count,
        4.0 * scale^2,
        4.0 * scale^2,
        0.0,
        true,
        false,
        2.0 * scale * 1e-6,
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

    source_reasons = [:external_unverified]
    detached_certificate = _ROSF.RORegularExtensionIntegrabilityCertificate(
        :unknown,
        source_reasons,
        true,
        false,
        nothing,
        0,
        0,
        0,
        0,
        0,
        nothing,
        0,
    )
    source_reasons[1] = :tampered
    @test detached_certificate.reasons == [:external_unverified]
    published_reasons = detached_certificate.reasons
    published_reasons[1] = :tampered
    @test detached_certificate.reasons == [:external_unverified]
    @test_throws ArgumentError _ROSF.RORegularExtensionIntegrabilityCertificate(
        :regular_cell_extension_integrable,
        [:contradictory_reason],
        true,
        false,
        1e-9,
        1,
        1,
        0,
        1,
        0,
        0.0,
        0,
    )
    @test_throws ArgumentError _ROSF.RORegularExtensionIntegrabilityCertificate(
        :unknown, Symbol[], true, false, 1e-9, 1, 1, 0, 1, 0, 0.0, 0)
    @test_throws ArgumentError _ROSF.RORegularExtensionIntegrabilityCertificate(
        :regular_cell_extension_integrable,
        Symbol[],
        true,
        false,
        1e-9,
        0,
        0,
        0,
        0,
        0,
        0.0,
        0,
    )
    lower_level_certificate = _ROSF.RORegularExtensionIntegrabilityCertificate(
        :unknown, [:external_unverified], true, false,
        nothing, 0, 0, 0, 0, 0, nothing, 0)
    getfield(lower_level_certificate, :reasons)[1] = :tampered
    @test_throws ArgumentError lower_level_certificate.status
    @test_throws ArgumentError _ROSF.RORegularExtensionIntegrabilityCertificate(
        :unknown,
        [:coverage_incomplete],
        true,
        false,
        nothing,
        0,
        0,
        0,
        0,
        0,
        nothing,
        0,
    )

    @test_throws ArgumentError _max_pwa_complex(drop_facet_id=1)
    @test_throws ArgumentError _max_pwa_complex(internal_incidence=[1])
    @test_throws ArgumentError _max_pwa_complex(
        second_matrix=reshape([0.0, 1.0], 1, 2))

    base = _max_pwa_complex()
    overlapping_second = ROCell2D(
        2,
        copy(base.cells[1].vertices),
        base.cells[1].area,
        copy(base.cells[2].source_regime_ids),
        copy(base.cells[2].labels),
        false,
    )
    @test_throws ArgumentError _rosf_with_cells(
        base, [base.cells[1], overlapping_second])

    outside_first = ROCell2D(
        1,
        [(-1.0, -1.0), (1.1, -1.0), (1.0, 1.0)],
        2.1,
        copy(base.cells[1].source_regime_ids),
        copy(base.cells[1].labels),
        false,
    )
    @test_throws ArgumentError _rosf_with_cells(
        base, [outside_first, base.cells[2]])
end

@testset "small-scale PWA geometry rejects real gaps and overlaps" begin
    scale = 1e-6
    complex = _rosf_scaled_max_pwa_complex(scale)
    certificate = _ROSF.certify_ro_regular_extension_integrability(complex)
    @test certificate.status == :regular_cell_extension_integrable
    @test isempty(certificate.reasons)

    gap_first = ROCell2D(
        1,
        [(-scale, -scale), (scale, -scale), (scale, 0.5scale)],
        1.5 * scale^2,
        copy(complex.cells[1].source_regime_ids),
        copy(complex.cells[1].labels),
        false,
    )
    @test_throws ArgumentError _rosf_with_cells(
        complex, [gap_first, complex.cells[2]])

    overlap_second = ROCell2D(
        2,
        copy(complex.cells[1].vertices),
        complex.cells[1].area,
        copy(complex.cells[2].source_regime_ids),
        copy(complex.cells[2].labels),
        false,
    )
    @test_throws ArgumentError _rosf_with_cells(
        complex, [complex.cells[1], overlap_second])
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

    detached_query = _ROSF.query_ro_stratified_jacobian(
        complex, (0.5, -0.5))
    @test detached_query.generator_cell_ids == [1]
    @test only(detached_query.jacobian_generators) ==
        [1.0 0.0; 10.0 0.0]
    published_matrices = detached_query.jacobian_generators
    published_matrices[1][1, 1] = 98.0
    @test only(detached_query.jacobian_generators) ==
        [1.0 0.0; 10.0 0.0]

    lower_level_query = _ROSF.query_ro_stratified_jacobian(
        complex, (0.5, -0.5))
    getfield(lower_level_query, :jacobian_generators)[1][1, 1] = 96.0
    @test_throws ArgumentError _ROSF.ro_directional_derivative_generators(
        lower_level_query, (1.0, 1.0))

    detached_directional = _ROSF.ro_directional_derivative_generators(
        detached_query, (1.0, 1.0))
    @test detached_directional.derivative_generators == [[1.0, 10.0]]
    published_derivatives = detached_directional.derivative_generators
    published_derivatives[1][1] = 97.0
    @test detached_directional.derivative_generators == [[1.0, 10.0]]
    lower_level_directional = _ROSF.ro_directional_derivative_generators(
        detached_query, (1.0, 1.0))
    getfield(lower_level_directional, :derivative_generators)[1][1] = 96.0
    @test_throws ArgumentError lower_level_directional.status

    @test_throws ArgumentError _ROSF.ROJointJacobianQuery2D(
        :classical_jacobian,
        :forged,
        (0.0, 0.0),
        Int[],
        Matrix{Float64}[],
        Int[],
        Int[],
        false,
        true,
    )
    @test_throws ArgumentError _ROSF.ROJointJacobianQuery2D(
        :classical_jacobian,
        nothing,
        (0.0, 0.0),
        [1],
        [[1.0 2.0]],
        [1],
        Int[],
        true,
        false,
    )
    external_unknown_query = _ROSF.ROJointJacobianQuery2D(
        :unknown,
        :external_unverified,
        (0.0, 0.0),
        Int[],
        Matrix{Float64}[],
        Int[],
        Int[],
        true,
        false,
    )
    @test external_unknown_query.reason === :external_unverified
    @test_throws ArgumentError _ROSF._rosf_validate_joint_query_state(
        :clarke_joint_matrix_hull,
        nothing,
        (0.0, 0.0),
        [1],
        [[1.0 2.0]],
        Int[],
        Int[],
        true,
        false,
        repeat("0", 64),
    )
    @test_throws ArgumentError _ROSF.ROJointJacobianQuery2D(
        :classical_jacobian,
        nothing,
        (0.0, 0.0),
        [1],
        [[1.0 2.0]],
        Int[],
        [1],
        true,
        false,
    )
    direct_query_limit_error = try
        _ROSF.ROJointJacobianQuery2D(
            :classical_jacobian,
            nothing,
            (0.0, 0.0),
            [1],
            [[1.0 2.0]],
            Int[],
            Int[],
            true,
            false;
            limits=_ROSF.ROStratifiedFieldLimits(max_matrix_elements=1),
        )
        nothing
    catch err
        err
    end
    @test direct_query_limit_error isa ArgumentError
    @test_throws ArgumentError _ROSF.RODirectionalDerivativeGenerators2D(
        :unknown,
        :forged,
        (0.0, 0.0),
        (1.0, 1.0),
        [1],
        [[2.0]],
        true,
        false,
    )
    direct_directional_limit_error = try
        _ROSF.RODirectionalDerivativeGenerators2D(
            :classical_jacobian,
            nothing,
            (0.0, 0.0),
            (1.0, 1.0),
            [1],
            [[2.0, 3.0]],
            true,
            false;
            limits=_ROSF.ROStratifiedFieldLimits(max_matrix_elements=1),
        )
        nothing
    catch err
        err
    end
    @test direct_directional_limit_error isa ArgumentError
    matrix_limit_error = try
        _ROSF.ro_directional_derivative_generators(
            detached_query,
            (1.0, 1.0);
            limits=_ROSF.ROStratifiedFieldLimits(max_matrix_elements=1),
        )
        nothing
    catch err
        err
    end
    @test matrix_limit_error isa _ROSF.ROStratifiedFieldLimitExceeded
    @test matrix_limit_error.phase === :matrix_elements
    @test matrix_limit_error.requested == 4
    @test matrix_limit_error.limit == 1
end

@testset "source-bound authority lineage and replay" begin
    first_complex = _max_pwa_complex(stratum_source_ids=[3])
    second_complex = _max_pwa_complex(stratum_source_ids=[4])
    @test first_complex.content_sha256 != second_complex.content_sha256
    @test first_complex.authority_status === :engine_replayed
    @test second_complex.authority_status === :engine_replayed

    first_certificate =
        _ROSF.certify_ro_regular_extension_integrability(first_complex)
    second_certificate =
        _ROSF.certify_ro_regular_extension_integrability(second_complex)
    @test first_certificate.source_complex_sha256 ==
        first_complex.content_sha256
    @test second_certificate.source_complex_sha256 ==
        second_complex.content_sha256
    @test first_certificate.content_sha256 != second_certificate.content_sha256
    @test _ROSF.validate_ro_regular_extension_integrability_certificate(
        first_complex, first_certificate) === first_certificate
    @test_throws ArgumentError begin
        _ROSF.validate_ro_regular_extension_integrability_certificate(
            second_complex, first_certificate)
    end

    first_query = _ROSF.query_ro_stratified_jacobian(
        first_complex, (0.5, -0.5))
    second_query = _ROSF.query_ro_stratified_jacobian(
        second_complex, (0.5, -0.5))
    @test first_query.generator_cell_ids == second_query.generator_cell_ids
    @test first_query.jacobian_generators == second_query.jacobian_generators
    @test first_query.source_complex_sha256 == first_complex.content_sha256
    @test second_query.source_complex_sha256 == second_complex.content_sha256
    @test first_query.content_sha256 != second_query.content_sha256
    @test _ROSF.validate_ro_joint_jacobian_query(
        first_complex, first_query) === first_query
    @test_throws ArgumentError _ROSF.validate_ro_joint_jacobian_query(
        second_complex, first_query)

    shifted_query = _ROSF.query_ro_stratified_jacobian(
        first_complex, (0.25, -0.25))
    first_directional = _ROSF.ro_directional_derivative_generators(
        first_query, (2.0, 3.0))
    shifted_directional = _ROSF.ro_directional_derivative_generators(
        shifted_query, (2.0, 3.0))
    @test first_directional.derivative_generators ==
        shifted_directional.derivative_generators
    @test first_directional.source_complex_sha256 ==
        first_complex.content_sha256
    @test first_directional.source_query_sha256 == first_query.content_sha256
    @test shifted_directional.source_query_sha256 ==
        shifted_query.content_sha256
    @test first_directional.content_sha256 !=
        shifted_directional.content_sha256
    @test _ROSF.validate_ro_directional_derivative_generators(
        first_query, first_directional) === first_directional
    @test_throws ArgumentError begin
        _ROSF.validate_ro_directional_derivative_generators(
            shifted_query, first_directional)
    end

    external_complex = _rosf_external_copy(first_complex)
    @test external_complex.authority_status === :external_unverified
    external_classification = classify_ro_cell_complex_point(
        external_complex, (0.5, -0.5))
    @test external_classification.status === :cell
    @test external_classification.source_authority_status ===
        :external_unverified
    external_certificate =
        _ROSF.certify_ro_regular_extension_integrability(external_complex)
    @test external_certificate.status === :unknown
    @test external_certificate.reasons == [:source_complex_unverified]
    @test external_certificate.source_complex_sha256 ==
        external_complex.content_sha256
    external_query = _ROSF.query_ro_stratified_jacobian(
        external_complex, (0.5, -0.5))
    @test external_query.status === :unknown
    @test external_query.reason === :regular_extension_integrability_unknown
    @test external_query.source_complex_sha256 ==
        external_complex.content_sha256

    public_certificate =
        _ROSF.RORegularExtensionIntegrabilityCertificate(
            :unknown,
            [:external_unverified],
            true,
            false,
            nothing,
            0,
            0,
            0,
            0,
            0,
            nothing,
            0,
        )
    @test public_certificate.source_complex_sha256 === nothing
    @test_throws ArgumentError begin
        _ROSF.validate_ro_regular_extension_integrability_certificate(
            external_complex, public_certificate)
    end
    public_query = _ROSF.ROJointJacobianQuery2D(
        :unknown,
        :external_unverified,
        (0.0, 0.0),
        Int[],
        Matrix{Float64}[],
        Int[],
        Int[],
        true,
        false,
    )
    @test public_query.source_complex_sha256 === nothing
    public_directional = _ROSF.RODirectionalDerivativeGenerators2D(
        :unknown,
        :external_unverified,
        (0.0, 0.0),
        (1.0, 0.0),
        Int[],
        Vector{Float64}[],
        true,
        false,
    )
    @test public_directional.source_complex_sha256 === nothing
    @test public_directional.source_query_sha256 === nothing
    @test_throws ArgumentError _ROSF.validate_ro_joint_jacobian_query(
        external_complex, public_query)
    @test_throws ArgumentError begin
        _ROSF.validate_ro_directional_derivative_generators(
            public_query, public_directional)
    end
end

@testset "unresolvable Float64 geometry returns unknown" begin
    large = 1.0e9
    domain = ROInputDomain2D(
        (1, 2), (large, 0.0), (large + 1.0, 1.0), zeros(2))
    label = ROAffineLabel2D([1], [1.0 0.0], [0.0])
    cell = ROCell2D(
        1,
        [(large, 0.0), (large + 1.0, 0.0), (large + 1.0, 1.0)],
        0.5,
        [1],
        [label],
        false,
    )
    facets = ROFacet2D[
        ROFacet2D(
            1,
            :domain,
            ((large, 0.0), (large + 1.0, 0.0)),
            [1],
            Int[],
            (0.0, 1.0),
            0.0,
            false,
            :axis2_lower,
        ),
        ROFacet2D(
            2,
            :domain,
            ((large + 1.0, 0.0), (large + 1.0, 1.0)),
            [1],
            Int[],
            (1.0, 0.0),
            -(large + 1.0),
            false,
            :axis1_upper,
        ),
    ]
    complex = _rosf_authoritative_complex(
        domain,
        [1],
        [cell],
        facets,
        ROSingularStratum2D[],
        1,
        1,
        1.0,
        0.5,
        0.5,
        false,
        false,
        1e-9,
    )

    @test _ROSF._rosf_geometry_tolerances(complex) === nothing
    certificate = _ROSF.certify_ro_regular_extension_integrability(complex)
    @test certificate.status === :unknown
    @test certificate.reasons ==
        [:coverage_incomplete, :unresolvable_float64_geometry]
    @test certificate.checked_facet_count == 2
    @test certificate.checked_internal_facet_count == 0
    @test certificate.checked_domain_facet_count == 2
end

@testset "raised producer limits remain replayable" begin
    output_count = 524_289
    domain = ROInputDomain2D(
        (1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(output_count))
    vertices = [
        (-1.0, -1.0),
        (1.0, -1.0),
        (1.0, 1.0),
        (-1.0, 1.0),
    ]
    label = ROAffineLabel2D(
        [1], zeros(output_count, 2), zeros(output_count))
    cell = ROCell2D(1, vertices, 4.0, [1], [label], false)
    facets = ROFacet2D[
        ROFacet2D(1, :domain, ((-1.0, -1.0), (-1.0, 1.0)),
            [1], Int[], (1.0, 0.0), 1.0, false, :axis1_lower),
        ROFacet2D(2, :domain, ((-1.0, -1.0), (1.0, -1.0)),
            [1], Int[], (0.0, 1.0), 1.0, false, :axis2_lower),
        ROFacet2D(3, :domain, ((-1.0, 1.0), (1.0, 1.0)),
            [1], Int[], (0.0, 1.0), -1.0, false, :axis2_upper),
        ROFacet2D(4, :domain, ((1.0, -1.0), (1.0, 1.0)),
            [1], Int[], (1.0, 0.0), -1.0, false, :axis1_upper),
    ]
    complex = _rosf_authoritative_complex(
        domain,
        collect(1:output_count),
        [cell],
        facets,
        ROSingularStratum2D[],
        1,
        1,
        4.0,
        4.0,
        0.0,
        true,
        false,
        1e-9;
        limits=ROCellComplexBuildLimits(
            max_outputs=output_count,
            max_matrix_elements=2 * output_count,
        ),
    )
    raised_limits = _ROSF.ROStratifiedFieldLimits(
        max_matrix_elements=10_000_000,
        max_payload_elements=11_000_000,
        max_finalization_work=45_000_000,
    )
    query = _ROSF.query_ro_stratified_jacobian(
        complex, (0.0, 0.0); limits=raised_limits)

    @test query.status === :classical_jacobian
    @test query.admission_limits == raised_limits
    @test length(only(query.jacobian_generators)) == 2 * output_count
    directional = _ROSF.ro_directional_derivative_generators(
        query, (1.0, 0.0))
    @test directional.status === :classical_jacobian
    @test directional.admission_limits == raised_limits

    lower_replay_error = try
        _ROSF.ro_directional_derivative_generators(
            query,
            (1.0, 0.0);
            limits=_ROSF.ROStratifiedFieldLimits(),
        )
        nothing
    catch err
        err
    end
    @test lower_replay_error isa _ROSF.ROStratifiedFieldLimitExceeded
    @test lower_replay_error.phase === :matrix_elements
    @test lower_replay_error.requested == 2 * output_count
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
    partial_vertices = [
        (-0.75, 0.0),
        (-0.5, -0.5),
        (0.5, -0.5),
        (0.75, 0.0),
        (0.0, 0.75),
    ]
    partial_complex = _rosf_authoritative_complex(
        ROInputDomain2D(
            (1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(2)),
        [1],
        [ROCell2D(
            1,
            partial_vertices,
            1.1875,
            [1],
            [ROAffineLabel2D([1], [1.0 0.0], [0.0])],
            false,
        )],
        ROFacet2D[],
        ROSingularStratum2D[],
        1,
        1,
        4.0,
        1.1875,
        2.8125,
        false,
        false,
        1e-9,
    )
    incidence_limit_error = try
        _ROSF.certify_ro_regular_extension_integrability(
            partial_complex;
            limits=_ROSF.ROStratifiedFieldLimits(max_incidence_checks=4),
        )
        nothing
    catch err
        err
    end
    @test incidence_limit_error isa _ROSF.ROStratifiedFieldLimitExceeded
    @test incidence_limit_error.phase === :incidence_checks
    @test incidence_limit_error.requested == 30
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
