using Test
using BindingAndCatalysis
using SHA

if !isdefined(BindingAndCatalysis, :ROCellComplex3D)
    Base.include(BindingAndCatalysis,
        joinpath(@__DIR__, "..", "src", "rop", "ro_cell_complex_3d.jl"))
end

const BNC3 = BindingAndCatalysis

struct _RO3Cancelled <: Exception end

struct _RO3VectorProbe{T} <: AbstractVector{T}
    declared_length::Int
    reads::Base.RefValue{Int}
end

Base.size(probe::_RO3VectorProbe) = (probe.declared_length,)
Base.IndexStyle(::Type{<:_RO3VectorProbe}) = IndexLinear()
function Base.getindex(probe::_RO3VectorProbe{T}, index::Int) where {T}
    probe.reads[] += 1
    error("unexpected probe read at $(index)")
end

struct _RO3MatrixProbe{T} <: AbstractMatrix{T}
    declared_size::Tuple{Int,Int}
    reads::Base.RefValue{Int}
end

Base.size(probe::_RO3MatrixProbe) = probe.declared_size
Base.IndexStyle(::Type{<:_RO3MatrixProbe}) = IndexCartesian()
function Base.getindex(probe::_RO3MatrixProbe{T}, row::Int, column::Int) where {T}
    probe.reads[] += 1
    error("unexpected matrix probe read at ($(row), $(column))")
end

function _ro3_label(regime_id, row)
    return BNC3.ROAffineLabel3D([regime_id],
        reshape(collect(Float64.(row)), 1, 3), [0.0])
end

function _ro3_dominance_specs(scores; column_order=collect(1:3), reverse_rows=false)
    specifications = BNC3.ROCellSpec3D[]
    for cell_index in axes(scores, 1)
        other_indices = [index for index in axes(scores, 1) if index != cell_index]
        A = reduce(vcat, [reshape(scores[other, :] - scores[cell_index, :], 1, 3)
            for other in other_indices])[:, column_order]
        b = zeros(length(other_indices))
        if reverse_rows
            A = A[end:-1:1, :]
            b = b[end:-1:1]
        end
        push!(specifications, BNC3.ROCellSpec3D(
            "regime-$(cell_index)", A, b, [cell_index],
            [_ro3_label(cell_index, scores[cell_index, column_order])]))
    end
    return specifications
end

function _ro3_abc_complex(; reverse_specs=false, reverse_rows=false,
    column_order=collect(1:3), axes=(1, 2, 3), annotations=true,
    tolerances=BNC3.ROCellComplex3DTolerances(),
    limits=BNC3.ROCellComplex3DLimits(), cancel_check=() -> nothing)
    # Four tetrahedrally symmetric affine regimes. Their pairwise equality
    # planes are the analytic A+B+C<->ABC fixture used by decision 0006.
    scores = [
         1.0  1.0  1.0
         1.0 -1.0 -1.0
        -1.0  1.0 -1.0
        -1.0 -1.0  1.0
    ]
    specs = _ro3_dominance_specs(scores;
        column_order=column_order, reverse_rows=reverse_rows)
    reverse_specs && reverse!(specs)
    domain = BNC3.ROInputDomain3D(axes, (-2.0, -2.0, -2.0),
        (2.0, 2.0, 2.0), zeros(4))
    interface_annotations = annotations ? [
        BNC3.ROInterfaceAnnotation3D(
            ("regime-2", "regime-3"), :singular, :singular_regime_limit),
    ] : BNC3.ROInterfaceAnnotation3D[]
    return BNC3.build_ro_cell_complex_3d(domain, specs;
        annotations=interface_annotations, tolerances=tolerances,
        limits=limits, cancel_check=cancel_check)
end

function _ro3_four_quadrant_complex()
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    specs = BNC3.ROCellSpec3D[]
    identifier = 0
    for x_high in (false, true), y_high in (false, true)
        identifier += 1
        A = [x_high ? -1.0 : 1.0 0.0 0.0;
             0.0 y_high ? -1.0 : 1.0 0.0]
        b = [x_high ? -0.5 : 0.5, y_high ? -0.5 : 0.5]
        push!(specs, BNC3.ROCellSpec3D("quadrant-$(identifier)", A, b,
            [identifier], [_ro3_label(identifier, (identifier, 0.0, 0.0))]))
    end
    return BNC3.build_ro_cell_complex_3d(domain, specs)
end

function _ro3_oblique_multiway_complex()
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    specs = BNC3.ROCellSpec3D[]
    identifier = 0
    for first_high in (false, true), second_high in (false, true)
        identifier += 1
        first_row = first_high ? [-1.0, -1.0, 0.0] : [1.0, 1.0, 0.0]
        second_row = second_high ? [0.0, -1.0, -1.0] : [0.0, 1.0, 1.0]
        A = permutedims(hcat(first_row, second_row))
        b = [first_high ? -1.0 : 1.0, second_high ? -1.0 : 1.0]
        push!(specs, BNC3.ROCellSpec3D("oblique-$(identifier)", A, b,
            [identifier], [_ro3_label(identifier, (identifier, 1.0, -1.0))]))
    end
    return BNC3.build_ro_cell_complex_3d(domain, specs)
end

function _ro3_copy_certificate(certificate;
    domain_side_coverage=certificate.domain_side_coverage,
    euler_value=certificate.euler_value,
    euler_consistent=certificate.euler_consistent,
    publishable=certificate.publishable,
    exact_pair_dimension_certified=
        certificate.exact_pair_dimension_certified,
    exact_cell_volume_sum=certificate.exact_cell_volume_sum)
    return BNC3.ROCellComplex3DCertificate(BNC3._RO3_VALIDATED,
        certificate.face_dimension_agreement,
        certificate.no_positive_volume_overlap,
        certificate.cell_facet_closure,
        certificate.facet_ridge_closure,
        certificate.ridge_vertex_links,
        domain_side_coverage,
        certificate.volume_complete,
        euler_value,
        euler_consistent,
        publishable,
        exact_pair_dimension_certified,
        certificate.exact_support_coverage_certified,
        certificate.exact_volume_coverage_certified,
        certificate.exact_pair_dimension_counts,
        exact_cell_volume_sum,
        certificate.exact_domain_volume,
        certificate.maximum_pair_overlap_unit_volume,
        certificate.maximum_cell_facet_area_residual,
        certificate.maximum_cell_facet_overlap_area,
        certificate.maximum_domain_side_area_residual,
        certificate.maximum_domain_side_overlap_area,
        certificate.unit_volume_sum,
        certificate.unit_volume_residual,
        certificate.maximum_ridge_cell_link_residual,
        certificate.maximum_facet_boundary_residual,
        certificate.maximum_vertex_link_degree_residual,
        certificate.disconnected_vertex_link_count,
        certificate.publication_length_tolerance,
        certificate.publication_area_tolerance,
        certificate.publication_volume_tolerance,
        certificate.evidence_scope,
        certificate.arbitrary_precision_certified,
        certificate.higher_dimension_certified,
        certificate.chemistry_extraction_certified)
end

function _ro3_copy_complex(complex; certificate=complex.certificate)
    return BNC3.ROCellComplex3D(BNC3._RO3_VALIDATED,
        complex.schema_version, complex.domain, complex.cells,
        complex.facets, complex.ridges, complex.vertices, complex.strata,
        certificate, complex.has_singular_strata, complex.has_ambiguity,
        complex.source_specs, complex.interface_annotations,
        complex.construction_limits, complex.canonical_payload,
        complex.canonical_identity, complex.tolerances, complex.evidence_scope)
end

function _ro3_t_junction_complex(; reverse_specs=false)
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    specs = [
        BNC3.ROCellSpec3D("bottom", [0.0 0.0 1.0], [0.5], [1],
            [_ro3_label(1, (0.0, 0.0, 1.0))]),
        BNC3.ROCellSpec3D("top-left", [0.0 0.0 -1.0; 1.0 0.0 0.0],
            [-0.5, 0.5], [2], [_ro3_label(2, (1.0, 0.0, 0.0))]),
        BNC3.ROCellSpec3D("top-right", [0.0 0.0 -1.0; -1.0 0.0 0.0],
            [-0.5, -0.5], [3], [_ro3_label(3, (0.0, 1.0, 0.0))]),
    ]
    reverse_specs && reverse!(specs)
    return BNC3.build_ro_cell_complex_3d(domain, specs)
end

function _ro3_assert_complete_incidence(complex)
    for cell in complex.cells
        @test sort(unique(cell.vertex_ids)) == cell.vertex_ids
        @test sort(unique(cell.ridge_ids)) == cell.ridge_ids
        @test sort(unique(cell.facet_ids)) == cell.facet_ids
        @test length(cell.vertex_ids) - length(cell.ridge_ids) +
            length(cell.facet_ids) == 2
        for facet_id in cell.facet_ids
            @test cell.id in complex.facets[facet_id].incident_cell_ids
        end
        for ridge_id in cell.ridge_ids
            @test cell.id in complex.ridges[ridge_id].incident_cell_ids
            @test count(facet_id -> facet_id in cell.facet_ids,
                complex.ridges[ridge_id].incident_facet_ids) == 2
        end
    end
    for facet in complex.facets
        @test length(facet.vertex_ids) == length(facet.ridge_ids)
        @test length(facet.vertex_ids) >= 3
        for ridge_id in facet.ridge_ids
            @test facet.id in complex.ridges[ridge_id].incident_facet_ids
        end
        for cell_id in facet.incident_cell_ids
            @test facet.id in complex.cells[cell_id].facet_ids
        end
    end
    for ridge in complex.ridges
        @test ridge.vertex_ids[1] < ridge.vertex_ids[2]
        @test length(ridge.incident_facet_ids) >= 2
        for vertex_id in ridge.vertex_ids
            @test ridge.id in complex.vertices[vertex_id].incident_ridge_ids
        end
        for facet_id in ridge.incident_facet_ids
            @test ridge.id in complex.facets[facet_id].ridge_ids
        end
    end
    for vertex in complex.vertices
        for ridge_id in vertex.incident_ridge_ids
            @test vertex.id in complex.ridges[ridge_id].vertex_ids
        end
        for facet_id in vertex.incident_facet_ids
            @test vertex.id in complex.facets[facet_id].vertex_ids
        end
        for cell_id in vertex.incident_cell_ids
            @test vertex.id in complex.cells[cell_id].vertex_ids
        end
    end
end

@testset "exact 3D A+B+C<->ABC face lattice" begin
    complex = _ro3_abc_complex()

    @test complex.schema_version == BNC3.RO_CELL_COMPLEX_3D_VERSION
    @test complex.domain.axis_indices == (1, 2, 3)
    @test length(complex.cells) == 4
    @test length(complex.facets) == 18
    @test length(complex.ridges) == 22
    @test length(complex.vertices) == 9
    @test all(cell -> isapprox(cell.volume, 16.0; atol=1e-10), complex.cells)
    @test isapprox(sum(getfield.(complex.cells, :volume)), 64.0; atol=1e-10)
    @test count(facet -> facet.kind === :internal, complex.facets) == 6
    @test count(facet -> facet.kind === :domain, complex.facets) == 12

    certificate = complex.certificate
    @test certificate.face_dimension_agreement
    @test certificate.no_positive_volume_overlap
    @test certificate.cell_facet_closure
    @test certificate.facet_ridge_closure
    @test certificate.ridge_vertex_links
    @test all(certificate.domain_side_coverage)
    @test certificate.volume_complete
    @test certificate.euler_value == 1
    @test certificate.euler_consistent
    @test certificate.publishable
    @test certificate.exact_pair_dimension_certified
    @test certificate.exact_support_coverage_certified
    @test certificate.exact_volume_coverage_certified
    @test certificate.exact_pair_dimension_counts == (0, 0, 0, 6, 0)
    @test certificate.exact_cell_volume_sum == "64//1"
    @test certificate.exact_domain_volume == "64//1"

    @test complex.has_singular_strata
    @test !complex.has_ambiguity
    singular = only(filter(stratum -> stratum.kind === :singular,
        complex.strata))
    @test singular.dimension == 2
    @test singular.reasons == [:singular_regime_limit]
    @test length(singular.support_face_ids) == 1
    singular_facet = complex.facets[only(singular.support_face_ids)]
    @test singular.id in singular_facet.stratum_ids
    @test singular.incident_cell_ids == singular_facet.incident_cell_ids

    center = only(filter(vertex -> vertex.coordinates == (0.0, 0.0, 0.0),
        complex.vertices))
    @test length(center.incident_cell_ids) == 4
    @test length(center.incident_ridge_ids) == 4
    _ro3_assert_complete_incidence(complex)
end

@testset "3D canonical identity and ordered-axis covariance" begin
    canonical = _ro3_abc_complex()
    reordered = _ro3_abc_complex(reverse_specs=true, reverse_rows=true)
    @test reordered.canonical_identity == canonical.canonical_identity
    @test getfield.(reordered.cells, :volume) == getfield.(canonical.cells, :volume)

    permutation = [3, 1, 2]
    permuted = _ro3_abc_complex(column_order=permutation, axes=(3, 1, 2))
    @test length(permuted.cells) == 4
    @test length(permuted.facets) == 18
    @test length(permuted.ridges) == 22
    @test length(permuted.vertices) == 9
    @test all(volume -> isapprox(volume, 16.0; atol=1e-12),
        getfield.(permuted.cells, :volume))
    @test permuted.canonical_identity != canonical.canonical_identity
    @test all(label -> size(label.reaction_order_matrix) == (1, 3),
        vcat(getfield.(permuted.cells, :labels)...))
    retoleranced = _ro3_abc_complex(tolerances=
        BNC3.ROCellComplex3DTolerances(
            incidence=2e-11, certificate=2e-8))
    @test retoleranced.canonical_identity != canonical.canonical_identity
end

@testset "3D T-junction second common refinement" begin
    complex = _ro3_t_junction_complex()
    @test length(complex.cells) == 3
    @test length(complex.facets) == 16
    @test length(complex.ridges) == 28
    @test length(complex.vertices) == 16
    @test sort(getfield.(complex.cells, :volume)) == [0.25, 0.25, 0.5]
    @test count(facet -> facet.kind === :internal, complex.facets) == 3
    @test count(facet -> facet.kind === :domain, complex.facets) == 13
    @test sort(length.(getfield.(complex.cells, :facet_ids))) == [6, 6, 7]

    t_ridge = only(filter(complex.ridges) do ridge
        points = sort([complex.vertices[id].coordinates for id in ridge.vertex_ids])
        points == [(0.5, 0.0, 0.5), (0.5, 1.0, 0.5)]
    end)
    @test t_ridge.incident_cell_ids == [1, 2, 3]
    @test length(t_ridge.incident_facet_ids) == 3
    @test isempty(t_ridge.domain_sides)
    @test complex.certificate.euler_value == 1
    @test complex.certificate.publishable
    @test _ro3_t_junction_complex(reverse_specs=true).canonical_identity ==
        complex.canonical_identity
    _ro3_assert_complete_incidence(complex)
end

@testset "3D ambiguity remains explicit and nonpublishable" begin
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    first = BNC3.ROCellSpec3D("same-a", [1.0 0.0 0.0], [1.0], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    second = BNC3.ROCellSpec3D("same-b", [1.0 0.0 0.0], [1.0], [2],
        [_ro3_label(2, (0.0, 1.0, 0.0))])
    complex = BNC3.build_ro_cell_complex_3d(domain, [second, first])

    @test length(complex.cells) == 1
    @test only(complex.cells).set_valued
    @test length(only(complex.cells).labels) == 2
    @test complex.has_ambiguity
    @test !complex.certificate.publishable
    ambiguity = only(filter(stratum -> stratum.kind === :ambiguous,
        complex.strata))
    @test ambiguity.dimension == 3
    @test ambiguity.support_face_ids == [1]
    @test ambiguity.reasons == [:coincident_distinct_affine_labels]
    @test length(complex.facets) == 6
    @test length(complex.ridges) == 12
    @test length(complex.vertices) == 8
    @test complex.certificate.euler_value == 1
end

@testset "3D overlap, sliver, and rank-grey-zone failures" begin
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    left = BNC3.ROCellSpec3D("left", [1.0 0.0 0.0], [0.75], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    right = BNC3.ROCellSpec3D("right", [-1.0 0.0 0.0], [-0.25], [2],
        [_ro3_label(2, (0.0, 1.0, 0.0))])
    overlap_error = try
        BNC3.build_ro_cell_complex_3d(domain, [left, right])
        nothing
    catch err
        err
    end
    @test overlap_error isa BNC3.ROCellComplex3DOverlap
    @test overlap_error.cell_keys == ("left", "right")

    grey = BNC3.ROCellSpec3D("grey", [1.0 0.0 0.0], [1.0], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    @test_throws BNC3.ROCellComplex3DRankAmbiguity BNC3.build_ro_cell_complex_3d(
        domain, [grey]; tolerances=BNC3.ROCellComplex3DTolerances(
            rank_relative_low=0.1, rank_relative_high=0.9))

    sliver = BNC3.ROCellSpec3D("sliver", [1.0 0.0 0.0], [1e-12], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    @test_throws BNC3.ROCellComplex3DSliver BNC3.build_ro_cell_complex_3d(
        domain, [sliver])

    @test_throws ArgumentError BNC3.ROInputDomain3D(
        (1, 1, 2), (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    @test_throws ArgumentError BNC3.ROInterfaceAnnotation3D(
        ("left", "left"), :singular, :bad)
    @test_throws ArgumentError BNC3.ROCellComplex3DTolerances(
        rank_relative_low=1e-7, rank_relative_high=1e-8)

    abc_specs = _ro3_dominance_specs([
         1.0  1.0  1.0
         1.0 -1.0 -1.0
        -1.0  1.0 -1.0
        -1.0 -1.0  1.0
    ])
    pair_error = try
        BNC3.build_ro_cell_complex_3d(domain, abc_specs;
            limits=BNC3.ROCellComplex3DLimits(max_pair_checks=5))
        nothing
    catch err
        err
    end
    @test pair_error isa BNC3.ROCellComplex3DLimitExceeded
    @test pair_error.phase == :pair_checks
    @test pair_error.requested == 6
    @test pair_error.limit == 5
    @test_throws BNC3.ROCellComplex3DLimitExceeded begin
        BNC3.build_ro_cell_complex_3d(domain, abc_specs;
            limits=BNC3.ROCellComplex3DLimits(max_cell_specs=3))
    end
    @test_throws ArgumentError BNC3.ROCellComplex3DLimits(max_facets=0)
end

@testset "3D physical representability and independent publication tolerances" begin
    huge_domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1e200, 1e200, 1e200), zeros(3))
    huge_spec = BNC3.ROCellSpec3D("huge", [1.0 0.0 0.0], [1e200], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    @test_throws ArgumentError BNC3.build_ro_cell_complex_3d(
        huge_domain, [huge_spec])

    caller_tolerance = BNC3.ROCellComplex3DTolerances(certificate=2e-7)
    independently_certified = _ro3_abc_complex(
        tolerances=caller_tolerance)
    @test independently_certified.certificate.publication_volume_tolerance <
        caller_tolerance.certificate
    @test independently_certified.certificate.publication_area_tolerance <
        caller_tolerance.certificate
    @test independently_certified.certificate.publishable
    @test_throws ArgumentError _ro3_abc_complex(tolerances=
        BNC3.ROCellComplex3DTolerances(certificate=3e-7))

    @test all(isfinite, Iterators.flatten(
        (vertex.coordinates for vertex in independently_certified.vertices)))
    @test all(facet -> isfinite(facet.area) && isfinite(facet.offset) &&
        all(isfinite, facet.normal), independently_certified.facets)
    @test all(ridge -> isfinite(ridge.length), independently_certified.ridges)
    @test all(cell -> isfinite(cell.volume), independently_certified.cells)
end

@testset "3D exact dyadic gap, overlap, and scaled-support decisions" begin
    function split_specs(left_bound, right_bound)
        return [
            BNC3.ROCellSpec3D("left", [1.0 0.0 0.0], [left_bound], [1],
                [_ro3_label(1, (1.0, 0.0, 0.0))]),
            BNC3.ROCellSpec3D("right", [-1.0 0.0 0.0], [-right_bound], [2],
                [_ro3_label(2, (0.0, 1.0, 0.0))]),
        ]
    end

    unit_domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    for delta in (1e-15, 1e-14, 1e-13, 5e-13, 1e-12)
        @test_throws BNC3.ROCellComplex3DExactDimensionMismatch begin
            BNC3.build_ro_cell_complex_3d(unit_domain,
                split_specs(0.5 - delta, 0.5 + delta))
        end
        @test_throws BNC3.ROCellComplex3DOverlap begin
            BNC3.build_ro_cell_complex_3d(unit_domain,
                split_specs(0.5 + delta, 0.5 - delta))
        end
    end

    nonunit_domain = BNC3.ROInputDomain3D((3, 1, 2),
        (-2.0, 10.0, 100.0), (3.0, 14.0, 102.0), zeros(3))
    for delta in (1e-14, 1e-13, 5e-13)
        @test_throws BNC3.ROCellComplex3DExactDimensionMismatch begin
            BNC3.build_ro_cell_complex_3d(nonunit_domain,
                split_specs(0.5 - delta, 0.5 + delta))
        end
        @test_throws BNC3.ROCellComplex3DOverlap begin
            BNC3.build_ro_cell_complex_3d(nonunit_domain,
                split_specs(0.5 + delta, 0.5 - delta))
        end
    end

    scaled_specs = [
        BNC3.ROCellSpec3D("scaled-left", [2.0 0.0 0.0], [1.0], [1],
            [_ro3_label(1, (1.0, 0.0, 0.0))]),
        BNC3.ROCellSpec3D("scaled-right", [-4.0 0.0 0.0], [-2.0], [2],
            [_ro3_label(2, (0.0, 1.0, 0.0))]),
    ]
    scaled = BNC3.build_ro_cell_complex_3d(unit_domain, scaled_specs)
    @test scaled.schema_version == "bne-ro-cell-complex-3d/v2.1.0"
    @test scaled.certificate.exact_pair_dimension_certified
    @test scaled.certificate.exact_support_coverage_certified
    @test scaled.certificate.exact_volume_coverage_certified
    @test scaled.certificate.exact_pair_dimension_counts == (0, 0, 0, 1, 0)
    @test scaled.certificate.exact_cell_volume_sum == "1//1"
    @test scaled.certificate.exact_domain_volume == "1//1"
    @test scaled.certificate.maximum_pair_overlap_unit_volume == 0.0
    @test scaled.certificate.publishable
end

@testset "3D multiway, anisotropic, oblique, and incomplete fixtures" begin
    quadrants = _ro3_four_quadrant_complex()
    central_ridge = only(filter(quadrants.ridges) do ridge
        points = [quadrants.vertices[id].unit_coordinates for id in ridge.vertex_ids]
        all(point -> point[1] == 0.5 && point[2] == 0.5, points)
    end)
    @test central_ridge.incident_cell_ids == [1, 2, 3, 4]
    @test length(central_ridge.incident_facet_ids) == 4
    @test quadrants.certificate.euler_value == 1
    @test quadrants.certificate.publishable
    _ro3_assert_complete_incidence(quadrants)

    asymmetric_domain = BNC3.ROInputDomain3D((3, 1, 2),
        (-1.0, 2.0, 10.0), (1.0, 6.0, 11.0), zeros(3))
    asymmetric_specs = [
        BNC3.ROCellSpec3D("low", [1.0 0.0 0.0], [0.0], [1],
            [_ro3_label(1, (1.0, 2.0, 3.0))]),
        BNC3.ROCellSpec3D("high", [-1.0 0.0 0.0], [0.0], [2],
            [_ro3_label(2, (4.0, 5.0, 6.0))]),
    ]
    asymmetric = BNC3.build_ro_cell_complex_3d(
        asymmetric_domain, asymmetric_specs)
    @test asymmetric.domain.axis_indices == (3, 1, 2)
    @test sort(getfield.(asymmetric.cells, :volume)) == [4.0, 4.0]
    internal = only(filter(facet -> facet.kind === :internal,
        asymmetric.facets))
    @test isapprox(internal.area, 4.0; atol=1e-12)
    @test internal.normal == (1.0, 0.0, 0.0)
    @test asymmetric.certificate.publishable

    oblique = _ro3_oblique_multiway_complex()
    oblique_ridge = only(filter(oblique.ridges) do ridge
        points = [oblique.vertices[id].unit_coordinates for id in ridge.vertex_ids]
        all(point -> isapprox(point[1] + point[2], 1.0; atol=1e-12) &&
            isapprox(point[2] + point[3], 1.0; atol=1e-12), points) &&
            length(ridge.incident_cell_ids) == 4
    end)
    @test length(oblique_ridge.incident_facet_ids) == 4
    @test isapprox(oblique_ridge.length, sqrt(3.0); atol=1e-12)
    @test oblique.certificate.publishable

    unit_domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    corner_cut = BNC3.ROCellSpec3D("corner-cut", [1.0 1.0 1.0], [2.5],
        [1], [_ro3_label(1, (1.0, 1.0, 1.0))])
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.build_ro_cell_complex_3d(unit_domain, [corner_cut])
    end
end

@testset "3D content root, defensive copies, and stale-state rejection" begin
    complex = _ro3_abc_complex()
    @test occursin(r"^sha256:[0-9a-f]{64}$", complex.canonical_identity)
    @test complex.canonical_identity == "sha256:" * bytes2hex(
        SHA.sha256(codeunits(complex.canonical_payload)))
    @test BNC3.validate_ro_cell_complex_3d(complex)
    @test complex.evidence_scope == BNC3.RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE
    @test complex.certificate.evidence_scope ==
        BNC3.RO_CELL_COMPLEX_3D_EVIDENCE_SCOPE
    @test !complex.certificate.arbitrary_precision_certified
    @test !complex.certificate.higher_dimension_certified
    @test !complex.certificate.chemistry_extraction_certified

    mutable_spec = BNC3.ROCellSpec3D("whole", [1.0 0.0 0.0], [1.0], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    unit_domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    copied = BNC3.build_ro_cell_complex_3d(unit_domain, [mutable_spec])
    mutable_spec.A[1, 1] = NaN
    @test BNC3.validate_ro_cell_complex_3d(copied)

    invalid_domain = BNC3.ROInputDomain3D((1, 2, 3),
        (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), zeros(3))
    invalid_domain.fixed_logqK[1] = NaN
    @test_throws ArgumentError BNC3.build_ro_cell_complex_3d(
        invalid_domain, [copied.source_specs[1]])
    invalid_spec = BNC3.ROCellSpec3D("invalid-source", [1.0 0.0 0.0],
        [1.0], [1], [_ro3_label(1, (1.0, 0.0, 0.0))])
    invalid_spec.source_regime_ids[1] = 0
    @test_throws ArgumentError BNC3.build_ro_cell_complex_3d(
        unit_domain, [invalid_spec])

    @test_throws MethodError BNC3.ROVertex3D(1, (0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0), Int[], Int[], Int[], Int[])
    @test_throws MethodError BNC3.ROCellComplex3DCertificate(
        true, true, true, true, true, ntuple(_ -> true, 6), true, 1,
        true, true)

    volume_tamper = _ro3_abc_complex()
    cell = volume_tamper.cells[1]
    volume_tamper.cells[1] = BNC3.ROCell3D(BNC3._RO3_VALIDATED,
        cell.id, cell.source_keys, cell.source_regime_ids, cell.labels,
        cell.vertex_ids, cell.ridge_ids, cell.facet_ids, cell.volume + 1.0,
        cell.set_valued, cell.stratum_ids)
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(volume_tamper)
    end

    link_tamper = _ro3_abc_complex()
    pop!(link_tamper.ridges[1].incident_facet_ids)
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(link_tamper)
    end

    coverage_base = _ro3_abc_complex()
    coverage = collect(coverage_base.certificate.domain_side_coverage)
    coverage[1] = false
    coverage_certificate = _ro3_copy_certificate(coverage_base.certificate;
        domain_side_coverage=Tuple(coverage))
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(_ro3_copy_complex(coverage_base;
            certificate=coverage_certificate))
    end

    euler_base = _ro3_abc_complex()
    euler_certificate = _ro3_copy_certificate(euler_base.certificate;
        euler_value=2)
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(_ro3_copy_complex(euler_base;
            certificate=euler_certificate))
    end

    exact_base = _ro3_abc_complex()
    exact_certificate = _ro3_copy_certificate(exact_base.certificate;
        exact_pair_dimension_certified=false)
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(_ro3_copy_complex(exact_base;
            certificate=exact_certificate))
    end
    exact_volume_certificate = _ro3_copy_certificate(exact_base.certificate;
        exact_cell_volume_sum="0//1")
    @test_throws BNC3.ROCellComplex3DClosureError begin
        BNC3.validate_ro_cell_complex_3d(_ro3_copy_complex(exact_base;
            certificate=exact_volume_certificate))
    end
end

@testset "3D quantitative certificate, grey boundaries, budgets, and cancel" begin
    complex = _ro3_abc_complex()
    certificate = complex.certificate
    @test certificate.maximum_pair_overlap_unit_volume == 0.0
    @test certificate.maximum_cell_facet_area_residual <=
        certificate.publication_area_tolerance
    @test certificate.maximum_cell_facet_overlap_area <=
        certificate.publication_area_tolerance
    @test certificate.maximum_domain_side_area_residual <=
        certificate.publication_area_tolerance
    @test certificate.maximum_domain_side_overlap_area <=
        certificate.publication_area_tolerance
    @test isapprox(certificate.unit_volume_sum, 1.0; atol=1e-14)
    @test certificate.unit_volume_residual <=
        certificate.publication_volume_tolerance
    @test certificate.maximum_ridge_cell_link_residual == 0
    @test certificate.maximum_facet_boundary_residual == 0
    @test certificate.maximum_vertex_link_degree_residual == 0
    @test certificate.disconnected_vertex_link_count == 0

    grey_tolerances = BNC3.ROCellComplex3DTolerances()
    low = grey_tolerances.rank_relative_low
    high = grey_tolerances.rank_relative_high
    @test BNC3._ro3_rank([1.0 0.0; 0.0 low], grey_tolerances,
        "exact low boundary") == 1
    @test BNC3._ro3_rank([1.0 0.0; 0.0 high], grey_tolerances,
        "exact high boundary") == 2
    @test_throws BNC3.ROCellComplex3DRankAmbiguity BNC3._ro3_rank(
        [1.0 0.0; 0.0 sqrt(low * high)], grey_tolerances,
        "inside grey interval")

    facet_error = try
        _ro3_abc_complex(limits=BNC3.ROCellComplex3DLimits(max_facets=20))
        nothing
    catch err
        err
    end
    @test facet_error isa BNC3.ROCellComplex3DLimitExceeded
    @test facet_error.phase == :facet_upper_bound

    ridge_error = try
        _ro3_abc_complex(limits=BNC3.ROCellComplex3DLimits(max_ridges=100))
        nothing
    catch err
        err
    end
    @test ridge_error isa BNC3.ROCellComplex3DLimitExceeded
    @test ridge_error.phase == :ridge_upper_bound

    identity_error = try
        _ro3_abc_complex(limits=BNC3.ROCellComplex3DLimits(
            max_identity_bytes=1_024))
        nothing
    catch err
        err
    end
    @test identity_error isa BNC3.ROCellComplex3DLimitExceeded
    @test identity_error.phase == :identity_reservation

    label_error = try
        _ro3_abc_complex(limits=BNC3.ROCellComplex3DLimits(max_labels=3))
        nothing
    catch err
        err
    end
    @test label_error isa BNC3.ROCellComplex3DLimitExceeded
    @test label_error.phase == :labels

    cancel_calls = Ref(0)
    @test_throws _RO3Cancelled _ro3_abc_complex(cancel_check=() -> begin
        cancel_calls[] += 1
        throw(_RO3Cancelled())
    end)
    @test cancel_calls[] == 1
end

@testset "3D public-input and pre-CDD allocation firewalls" begin
    fixed_reads = Ref(0)
    oversized_fixed = _RO3VectorProbe{Float64}(
        BNC3._RO3_MAX_FIXED_BACKGROUND + 1, fixed_reads)
    @test_throws ArgumentError BNC3.ROInputDomain3D((1, 2, 3),
        (0.0, 0.0, 0.0), (1.0, 1.0, 1.0), oversized_fixed)
    @test fixed_reads[] == 0

    source_reads = Ref(0)
    oversized_sources = _RO3VectorProbe{Int}(
        BNC3._RO3_MAX_SOURCE_REGIME_IDS + 1, source_reads)
    @test_throws ArgumentError BNC3.ROAffineLabel3D(oversized_sources,
        reshape([1.0, 0.0, 0.0], 1, 3), [0.0])
    @test source_reads[] == 0

    output_reads = Ref(0)
    oversized_output_matrix = _RO3MatrixProbe{Float64}(
        (BNC3._RO3_MAX_OUTPUTS + 1, 3), output_reads)
    @test_throws ArgumentError BNC3.ROAffineLabel3D([1],
        oversized_output_matrix, [0.0])
    @test output_reads[] == 0

    halfspace_reads = Ref(0)
    oversized_halfspaces = _RO3MatrixProbe{Float64}(
        (BNC3._RO3_MAX_HALFSPACES_PER_CELL + 1, 3), halfspace_reads)
    @test_throws ArgumentError BNC3.ROCellSpec3D("oversized-halfspaces",
        oversized_halfspaces, [0.0], [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    @test halfspace_reads[] == 0

    label_reads = Ref(0)
    oversized_labels = _RO3VectorProbe{BNC3.ROAffineLabel3D}(
        BNC3._RO3_MAX_LABELS + 1, label_reads)
    @test_throws ArgumentError BNC3.ROCellSpec3D("oversized-labels",
        [1.0 0.0 0.0], [1.0], [1], oversized_labels)
    @test label_reads[] == 0
    @test_throws ArgumentError BNC3.ROInterfaceAnnotation3D(
        ("left", "right"), :singular, Symbol(repeat("r", 1_025)))

    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(4))
    two_output_label = BNC3.ROAffineLabel3D([1],
        [1.0 0.0 0.0; 0.0 1.0 0.0], [0.0, 0.0])
    budget_spec = BNC3.ROCellSpec3D("budgeted",
        [1.0 0.0 0.0; 0.0 1.0 0.0], [1.0, 1.0], [1, 2],
        [two_output_label, BNC3.ROAffineLabel3D([2],
            [0.0 0.0 1.0; 1.0 1.0 0.0], [0.0, 0.0])])
    for (limits, phase) in (
        (BNC3.ROCellComplex3DLimits(max_fixed_background=3),
            :fixed_background),
        (BNC3.ROCellComplex3DLimits(max_halfspaces_per_cell=1),
            :halfspaces_per_cell),
        (BNC3.ROCellComplex3DLimits(max_labels=1), :labels),
        (BNC3.ROCellComplex3DLimits(max_source_regime_ids=1),
            :source_regime_ids),
        (BNC3.ROCellComplex3DLimits(max_outputs=1), :outputs),
    )
        error = try
            BNC3.build_ro_cell_complex_3d(domain, [budget_spec]; limits=limits)
            nothing
        catch err
            err
        end
        @test error isa BNC3.ROCellComplex3DLimitExceeded
        @test error.phase == phase
    end

    high_halfspace_count = 900
    high_halfspace_spec = BNC3.ROCellSpec3D("pre-cdd",
        ones(high_halfspace_count, 3), fill(1.0, high_halfspace_count), [1],
        [_ro3_label(1, (1.0, 0.0, 0.0))])
    pre_cdd_error = try
        BNC3.build_ro_cell_complex_3d(domain, [high_halfspace_spec];
            limits=BNC3.ROCellComplex3DLimits(
                max_halfspaces_per_cell=high_halfspace_count,
                max_total_halfspaces=high_halfspace_count))
        nothing
    catch err
        err
    end
    @test pre_cdd_error isa BNC3.ROCellComplex3DLimitExceeded
    @test pre_cdd_error.phase == :cell_vertex_candidates

    tiny_work_error = try
        BNC3.build_ro_cell_complex_3d(domain,
            [BNC3.ROCellSpec3D("tiny-work", [1.0 0.0 0.0], [1.0], [1],
                [_ro3_label(1, (1.0, 0.0, 0.0))])];
            limits=BNC3.ROCellComplex3DLimits(max_total_work=30))
        nothing
    catch err
        err
    end
    @test tiny_work_error isa BNC3.ROCellComplex3DLimitExceeded
    @test tiny_work_error.phase == :cell_vertex_candidates

    output_count = 1_000
    large_output_label = BNC3.ROAffineLabel3D([1],
        zeros(output_count, 3), zeros(output_count))
    large_output_spec = BNC3.ROCellSpec3D("identity-before-json",
        [1.0 0.0 0.0], [1.0], [1], [large_output_label])
    identity_error = try
        BNC3.build_ro_cell_complex_3d(domain, [large_output_spec];
            limits=BNC3.ROCellComplex3DLimits(max_identity_bytes=150_000))
        nothing
    catch err
        err
    end
    @test identity_error isa BNC3.ROCellComplex3DLimitExceeded
    @test identity_error.phase == :identity_reservation

    exact_work_error = try
        BNC3.build_ro_cell_complex_3d(domain,
            [BNC3.ROCellSpec3D("exact-work", [1.0 0.0 0.0], [1.0], [1],
                [_ro3_label(1, (1.0, 0.0, 0.0))])];
            limits=BNC3.ROCellComplex3DLimits(max_exact_bit_work=34))
        nothing
    catch err
        err
    end
    @test exact_work_error isa BNC3.ROCellComplex3DLimitExceeded
    @test exact_work_error.phase == :exact_bit_work

    support_rank_work_error = try
        BNC3.build_ro_cell_complex_3d(domain,
            [BNC3.ROCellSpec3D("support-rank-work",
                [1.0 0.0 0.0], [0.5], [1],
                [_ro3_label(1, (1.0, 0.0, 0.0))])];
            limits=BNC3.ROCellComplex3DLimits(
                max_exact_bit_work=100_000))
        nothing
    catch err
        err
    end
    @test support_rank_work_error isa BNC3.ROCellComplex3DLimitExceeded
    @test support_rank_work_error.phase == :exact_bit_work

    # The two exact geometry groups below each retain two original H sources.
    # The limit is above their cell/pair conversion receipt but below the
    # additional merged-source H-row x H-row x pair-point support receipt.
    coincident_support_specs = [
        BNC3.ROCellSpec3D("left-$(index)", [1.0 0.0 0.0], [0.5],
            [index], [_ro3_label(index, (1.0, 0.0, 0.0))])
        for index in 1:2
    ]
    append!(coincident_support_specs, [
        BNC3.ROCellSpec3D("right-$(index)", [-1.0 0.0 0.0], [-0.5],
            [index], [_ro3_label(index, (1.0, 0.0, 0.0))])
        for index in 3:4
    ])
    opposite_support_work_error = try
        BNC3.build_ro_cell_complex_3d(domain, coincident_support_specs;
            limits=BNC3.ROCellComplex3DLimits(
                max_exact_bit_work=2_000_000))
        nothing
    catch err
        err
    end
    @test opposite_support_work_error isa
        BNC3.ROCellComplex3DLimitExceeded
    @test opposite_support_work_error.phase == :exact_bit_work
    @test opposite_support_work_error.requested > 2_000_000

    closure_work_error = try
        BNC3.build_ro_cell_complex_3d(domain,
            [BNC3.ROCellSpec3D("closure-work", [1.0 0.0 0.0], [1.0], [1],
                [_ro3_label(1, (1.0, 0.0, 0.0))])];
            limits=BNC3.ROCellComplex3DLimits(max_total_work=200))
        nothing
    catch err
        err
    end
    @test closure_work_error isa BNC3.ROCellComplex3DLimitExceeded
    @test closure_work_error.phase == :closure_work

    annotation_error = try
        _ro3_abc_complex(limits=BNC3.ROCellComplex3DLimits(
            max_annotation_work=5))
        nothing
    catch err
        err
    end
    @test annotation_error isa BNC3.ROCellComplex3DLimitExceeded
    @test annotation_error.phase == :annotation_work

    annotation_cancel_calls = Ref(0)
    annotation = BNC3.ROInterfaceAnnotation3D(
        ("left", "right"), :singular, :cancel_probe)
    @test_throws _RO3Cancelled BNC3._ro3_annotation_for_pair(
        [annotation], ["left"], ["right"], () -> begin
            annotation_cancel_calls[] += 1
            throw(_RO3Cancelled())
        end)
    @test annotation_cancel_calls[] == 1

    zero_exact = BNC3._RO3Exact(0)
    one_exact = BNC3._RO3Exact(1)
    half_exact = BNC3._RO3Exact(1, 2)
    support_points = [
        (half_exact, zero_exact, zero_exact),
        (half_exact, one_exact, zero_exact),
        (half_exact, zero_exact, one_exact),
    ]
    first_source = BNC3._RO3ExactSourceHalfspaces("left",
        reshape(BNC3._RO3Exact[1, 0, 0], 1, 3), [half_exact])
    second_source = BNC3._RO3ExactSourceHalfspaces("right",
        reshape(BNC3._RO3Exact[-1, 0, 0], 1, 3), [-half_exact])

    opposite_cancel_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_exact_opposite_support(
        (source_halfspaces=[first_source],),
        (source_halfspaces=[second_source],), support_points, () -> begin
            opposite_cancel_calls[] += 1
            opposite_cancel_calls[] == 2 && throw(_RO3Cancelled())
        end)
    @test opposite_cancel_calls[] == 2

    support_cancel_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_exact_support_plane_keys(
        (A=first_source.A, b=first_source.b, vertices=support_points),
        () -> begin
            support_cancel_calls[] += 1
            support_cancel_calls[] == 5 && throw(_RO3Cancelled())
        end)
    @test support_cancel_calls[] == 5

    rank_cancel_calls = Ref(0)
    rank_probe = BNC3._RO3Exact[1 0 0; 0 1 0]
    @test_throws _RO3Cancelled BNC3._ro3_exact_rank(rank_probe, () -> begin
        rank_cancel_calls[] += 1
        rank_cancel_calls[] == 2 && throw(_RO3Cancelled())
    end)
    @test rank_cancel_calls[] == 2

    key_cancel_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_exact_geometry_key(
        reverse(support_points), () -> begin
            key_cancel_calls[] += 1
            key_cancel_calls[] == 4 && throw(_RO3Cancelled())
        end)
    @test key_cancel_calls[] == 4
end

@testset "3D delayed cancellation across large label populations" begin
    label_count = 5_000
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    shared_label = _ro3_label(1, (1.0, 0.0, 0.0))
    large_spec = BNC3.ROCellSpec3D("large-label-cancel",
        [1.0 0.0 0.0], [1.0], [1], fill(shared_label, label_count))
    raised_limits = BNC3.ROCellComplex3DLimits(
        max_labels=label_count,
        max_source_regime_ids=label_count + 1)

    # Preflight contributes label_count + 6 checkpoints for this one-row,
    # one-cell fixture. Delaying cancellation beyond that boundary proves the
    # canonical label validation/copy loop remains interruptible after the
    # caller legally raises the label limit above its default.
    delayed_calls = Ref(0)
    delayed_target = label_count + 64
    @test_throws _RO3Cancelled BNC3.build_ro_cell_complex_3d(
        domain, [large_spec]; limits=raised_limits, cancel_check=() -> begin
            delayed_calls[] += 1
            delayed_calls[] == delayed_target && throw(_RO3Cancelled())
        end)
    @test delayed_calls[] == delayed_target

    unique_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_unique_labels(
        large_spec.labels, () -> begin
            unique_calls[] += 1
            unique_calls[] == 128 && throw(_RO3Cancelled())
        end)
    @test unique_calls[] == 128

    probe_cell = BNC3._RO3CellWork(["large-label-cancel"], [1],
        large_spec.labels, zeros(0, 3), Float64[], nothing,
        NTuple{3,Float64}[], Vector{Vector{NTuple{3,Float64}}}(),
        "large-label-geometry", false)
    merge_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_merge_duplicate_cells(
        [probe_cell], () -> begin
            merge_calls[] += 1
            merge_calls[] == 128 && throw(_RO3Cancelled())
        end)
    @test merge_calls[] == 128

    base = _ro3_abc_complex()
    empty_annotations = BNC3.ROInterfaceAnnotation3D[]
    empty_cells = BNC3.ROCell3D[]
    empty_facets = BNC3.ROFacet3D[]
    empty_ridges = BNC3.RORidge3D[]
    empty_vertices = BNC3.ROVertex3D[]
    empty_strata = BNC3.ROStratum3D[]

    identity_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_identity_reservation(
        domain, [large_spec], empty_annotations, empty_cells, empty_facets,
        empty_ridges, empty_vertices, empty_strata, base.certificate,
        base.tolerances, raised_limits, () -> begin
            identity_calls[] += 1
            identity_calls[] == 128 && throw(_RO3Cancelled())
        end)
    @test identity_calls[] == 128

    work_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_payload_row_work(
        domain, [large_spec], empty_annotations, empty_cells, empty_facets,
        empty_ridges, empty_vertices, empty_strata, () -> begin
            work_calls[] += 1
            work_calls[] == 128 && throw(_RO3Cancelled())
        end)
    @test work_calls[] == 128

    payload_calls = Ref(0)
    @test_throws _RO3Cancelled BNC3._ro3_payload_object(
        domain, [large_spec], empty_annotations, empty_cells, empty_facets,
        empty_ridges, empty_vertices, empty_strata, base.certificate,
        false, false, base.tolerances, raised_limits, () -> begin
            payload_calls[] += 1
            payload_calls[] == 128 && throw(_RO3Cancelled())
        end)
    @test payload_calls[] == 128
end

@testset "3D same-key distinct-source aggregation is linearized and receipted" begin
    label_count = 5_000
    domain = BNC3.ROInputDomain3D((1, 2, 3), (0.0, 0.0, 0.0),
        (1.0, 1.0, 1.0), zeros(3))
    labels = BNC3.ROAffineLabel3D[]
    sizehint!(labels, label_count)
    for source_id in 1:label_count
        push!(labels, _ro3_label(source_id, (1.0, 0.0, 0.0)))
    end
    spec = BNC3.ROCellSpec3D("distinct-source-labels",
        [1.0 0.0 0.0], [1.0], [1], labels)
    limits = BNC3.ROCellComplex3DLimits(
        max_labels=label_count,
        max_source_regime_ids=label_count + 1)
    annotations = BNC3.ROInterfaceAnnotation3D[]
    tolerances = BNC3.ROCellComplex3DTolerances()

    preflight_calls = Ref(0)
    receipt = BNC3._ro3_preflight_inputs(domain, [spec], annotations,
        tolerances, limits, () -> (preflight_calls[] += 1))
    @test preflight_calls[] <= receipt

    view_calls = Ref(0)
    unique_labels = BNC3._ro3_unique_labels(@view(spec.labels[:]),
        () -> (view_calls[] += 1))
    @test view_calls[] <= receipt
    @test length(unique_labels) == 1
    @test only(unique_labels).source_regime_ids == collect(1:label_count)
    @test only(unique_labels).reaction_order_matrix == [1.0 0.0 0.0]
    @test only(unique_labels).output_offset == [0.0]

    completed_calls = Ref(0)
    complex = BNC3.build_ro_cell_complex_3d(domain, [spec];
        limits=limits, cancel_check=() -> (completed_calls[] += 1))
    @test completed_calls[] <= receipt
    @test complex.certificate.publishable
    merged_label = only(only(complex.cells).labels)
    @test merged_label.source_regime_ids == collect(1:label_count)
    @test BNC3.validate_ro_cell_complex_3d(complex)

    delayed_calls = Ref(0)
    delayed_target = 25_000
    @test_throws _RO3Cancelled BNC3._ro3_unique_labels(
        @view(spec.labels[end:-1:begin]), () -> begin
            delayed_calls[] += 1
            delayed_calls[] == delayed_target && throw(_RO3Cancelled())
        end)
    @test delayed_calls[] == delayed_target
end
