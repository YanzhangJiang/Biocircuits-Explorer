struct ROCellComplexCancelProbe <: Exception end

function _heterodimer_ro_complex(; axes=(1, 2), lower=(-2.0, -2.0),
    upper=(2.0, 2.0), fixed=zeros(3), kwargs...)
    model = Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :B, :AB],
        q_sym=[:tA, :tB],
        K_sym=[:Kd],
    )
    domain = ROInputDomain2D(axes, lower, upper, fixed)
    return build_ro_cell_complex(model, domain, [3]; kwargs...)
end

@testset "exact fixed-background 2D RO cell complex" begin
    complex = _heterodimer_ro_complex()

    # Independent asymptotic analysis for A+B<->AB, log10(Kd)=0 over [-2,2]^2:
    #   tA,tB <= Kd       area 4, AB order [1,1]
    #   tB >= max(tA,Kd)  area 6, AB order [1,0]
    #   tA >= max(tB,Kd)  area 6, AB order [0,1]
    @test complex.domain.axis_indices == (1, 2)
    @test complex.output_indices == [3]
    @test complex.candidate_regime_count == 4
    @test complex.regular_candidate_count == 3
    @test length(complex.cells) == 3
    @test sort(getfield.(complex.cells, :area)) == [4.0, 6.0, 6.0]
    @test complex.domain_area == 16.0
    @test complex.covered_area_sum == 16.0
    @test complex.gap_area == 0.0
    @test complex.coverage_complete
    @test !complex.has_ambiguity

    label_rows = sort([
        Tuple(vec(only(cell.labels).reaction_order_matrix))
        for cell in complex.cells
    ])
    @test label_rows == [(0.0, 1.0), (1.0, 0.0), (1.0, 1.0)]
    @test all(cell -> length(cell.source_regime_ids) == 1, complex.cells)
    @test all(cell -> only(cell.labels).source_regime_ids == cell.source_regime_ids,
        complex.cells)
    @test all(cell -> only(cell.labels).output_offset == [0.0], complex.cells)

    internal_facets = filter(facet -> facet.kind === :internal, complex.facets)
    domain_facets = filter(facet -> facet.kind === :domain, complex.facets)
    @test length(internal_facets) == 3
    @test length(domain_facets) == 6
    diagonal = only(filter(facet ->
        facet.endpoints == ((0.0, 0.0), (2.0, 2.0)), internal_facets))
    @test diagonal.mixed_sign
    @test length(diagonal.incident_cell_ids) == 2
    @test diagonal.normal[1] > 0 > diagonal.normal[2]
    @test isapprox(diagonal.offset, 0.0; atol=1e-12)

    @test length(complex.singular_strata) == 1
    singular = only(complex.singular_strata)
    @test singular.dimension == 1
    @test singular.vertices == [(0.0, 0.0), (2.0, 2.0)]
    @test singular.nullities == [1]
    @test singular.reasons == [:singular_regime]
    @test diagonal.singular_stratum_ids == [singular.id]

    low = classify_ro_cell_complex_point(complex, (-1.0, -1.0))
    @test low.status === :cell
    @test length(low.cell_ids) == 1
    @test only(low.labels).reaction_order_matrix == [1.0 1.0]

    boundary = classify_ro_cell_complex_point(complex, (1.0, 1.0))
    @test boundary.status === :boundary
    @test length(boundary.cell_ids) == 2
    @test boundary.singular_stratum_ids == [singular.id]
    @test diagonal.id in boundary.facet_ids

    triple_boundary = classify_ro_cell_complex_point(complex, (0.0, 0.0))
    @test triple_boundary.status === :boundary
    @test length(triple_boundary.cell_ids) == 3
    @test classify_ro_cell_complex_point(complex, (3.0, 0.0)).status ===
        :outside_domain
end

@testset "2D RO complex axis order, translation, and affine provenance" begin
    original = _heterodimer_ro_complex()
    swapped = _heterodimer_ro_complex(axes=(2, 1))
    @test swapped.domain.axis_indices == (2, 1)
    @test sort(getfield.(swapped.cells, :area)) == [4.0, 6.0, 6.0]
    @test sort([
        Tuple(vec(only(cell.labels).reaction_order_matrix))
        for cell in swapped.cells
    ]) == [(0.0, 1.0), (1.0, 0.0), (1.0, 1.0)]
    @test count(facet -> facet.kind === :internal && facet.mixed_sign,
        swapped.facets) == 1

    translated = _heterodimer_ro_complex(
        lower=(-1.0, -1.0),
        upper=(3.0, 3.0),
        fixed=[0.0, 0.0, 1.0],
    )
    @test sort(getfield.(translated.cells, :area)) == [4.0, 6.0, 6.0]
    translated_singular = only(translated.singular_strata)
    @test translated_singular.vertices == [(1.0, 1.0), (3.0, 3.0)]
    @test translated.coverage_complete

    for cell in original.cells
        label = only(cell.labels)
        @test label.source_regime_ids == cell.source_regime_ids
    end

    same_a = ROAffineLabel2D([4], [1.0 0.0], [0.0])
    same_b = ROAffineLabel2D([7], [1.0 0.0], [0.0])
    other = ROAffineLabel2D([9], [0.0 1.0], [0.0])
    merged = BindingAndCatalysis._ro2_unique_labels([same_a, same_b, other], 1e-9)
    @test length(merged) == 2
    @test sort(vcat(getfield.(merged, :source_regime_ids)...)) == [4, 7, 9]
    @test only(filter(label -> label.reaction_order_matrix == [1.0 0.0], merged)).source_regime_ids ==
        [4, 7]
end

@testset "2D RO complex deduplicates coincident competitive geometry" begin
    model = Bnc(
        N=[1 0 1 -1 0; 0 1 1 0 -1],
        x_sym=[:A, :B, :L, :AL, :BL],
        q_sym=[:tA, :tB, :tL],
        K_sym=[:Kd1, :Kd2],
    )
    domain = ROInputDomain2D(
        (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(5))
    complex = build_ro_cell_complex(model, domain, collect(1:5))

    @test length(complex.cells) == 3
    @test sort(getfield.(complex.cells, :area)) == [4.0, 6.0, 6.0]
    coincident = only(filter(cell -> length(cell.source_regime_ids) > 1,
        complex.cells))
    @test coincident.source_regime_ids == [1, 2, 3, 4]
    @test length(coincident.labels) == 1
    @test only(coincident.labels).source_regime_ids == [1, 2, 3, 4]
    @test !coincident.set_valued
    @test complex.coverage_complete
    @test !complex.has_ambiguity
end

@testset "2D RO complex ambiguity and gap classification" begin
    domain = ROInputDomain2D((1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(3))
    box_vertices = [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)]
    label_a = ROAffineLabel2D([1], [1.0 0.0], [0.0])
    label_b = ROAffineLabel2D([2], [0.0 1.0], [0.0])
    ambiguous_cell = ROCell2D(1, box_vertices, 4.0, [1, 2],
        [label_a, label_b], true)
    ambiguous_complex = ROCellComplex2D(
        domain, [3], [ambiguous_cell], ROFacet2D[], ROSingularStratum2D[],
        2, 2, 4.0, 4.0, 0.0, true, true, 1e-9)
    ambiguity = classify_ro_cell_complex_point(ambiguous_complex, (0.0, 0.0))
    @test ambiguity.status === :ambiguity
    @test length(ambiguity.labels) == 2
    @test sort(vcat(getfield.(ambiguity.labels, :source_regime_ids)...)) == [1, 2]

    gap_complex = ROCellComplex2D(
        domain, [3], ROCell2D[], ROFacet2D[], ROSingularStratum2D[],
        0, 0, 4.0, 0.0, 4.0, false, false, 1e-9)
    @test classify_ro_cell_complex_point(gap_complex, (0.0, 0.0)).status === :gap
end

@testset "2D RO complex budgets, validation, and cooperative cancellation" begin
    candidate_error = try
        _heterodimer_ro_complex(limits=ROCellComplexBuildLimits(
            max_candidate_regimes=3))
        nothing
    catch err
        err
    end
    @test candidate_error isa ROCellComplexLimitExceeded
    @test candidate_error.phase === :candidate_regimes
    @test candidate_error.requested == 4

    cell_error = try
        _heterodimer_ro_complex(limits=ROCellComplexBuildLimits(max_cells=2))
        nothing
    catch err
        err
    end
    @test cell_error isa ROCellComplexLimitExceeded
    @test cell_error.phase === :cells
    @test cell_error.requested == 3

    pair_error = try
        _heterodimer_ro_complex(limits=ROCellComplexBuildLimits(max_pair_checks=2))
        nothing
    catch err
        err
    end
    @test pair_error isa ROCellComplexLimitExceeded
    @test pair_error.phase === :pair_checks
    @test pair_error.requested == 3

    facet_error = try
        _heterodimer_ro_complex(limits=ROCellComplexBuildLimits(max_facets=8))
        nothing
    catch err
        err
    end
    @test facet_error isa ROCellComplexLimitExceeded
    @test facet_error.phase === :facets
    @test facet_error.requested == 9

    checks = Ref(0)
    @test_throws ROCellComplexCancelProbe _heterodimer_ro_complex(
        cancel_check=() -> begin
            checks[] += 1
            checks[] == 8 && throw(ROCellComplexCancelProbe())
        end)
    @test checks[] == 8

    @test_throws ArgumentError _heterodimer_ro_complex(axes=(1, 3))
    @test_throws DimensionMismatch _heterodimer_ro_complex(fixed=zeros(2))
    @test_throws ArgumentError ROInputDomain2D(
        (1, 1), (-1.0, -1.0), (1.0, 1.0), zeros(3))
    @test_throws ArgumentError ROInputDomain2D(
        (1, 2), (-1.0, -1.0), (1.0, Inf), zeros(3))
    @test_throws ArgumentError _heterodimer_ro_complex(
        geometry_tolerance=1e-5)
    @test_throws ArgumentError _heterodimer_ro_complex(
        lower=(-1e-6, -1e-6),
        upper=(1e-6, 1e-6),
        geometry_tolerance=1e-9,
    )
end
