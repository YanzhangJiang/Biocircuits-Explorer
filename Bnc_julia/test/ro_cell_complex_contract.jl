struct ROCellComplexCancelProbe <: Exception end

struct ROCountingIndexVector <: AbstractVector{Int}
    count::Int
    reads::Base.RefValue{Int}
end

Base.size(values::ROCountingIndexVector) = (values.count,)
Base.IndexStyle(::Type{ROCountingIndexVector}) = IndexLinear()
function Base.getindex(values::ROCountingIndexVector, index::Int)
    checkbounds(values, index)
    values.reads[] += 1
    return index
end

function _heterodimer_ro_model()
    return Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :B, :AB],
        q_sym=[:tA, :tB],
        K_sym=[:Kd],
    )
end

function _weighted_block_ro_model(block_count::Int)
    L = zeros(Int, 2 * block_count, 2 * block_count + 1)
    for block in 1:block_count
        first = 2 * block - 1
        L[first:(first + 1), first:(first + 1)] .= [100 1; 1 100]
    end
    return Bnc(L=L)
end

function _ro2_public_enumeration_preflight_allocations(model, limit::Int)
    return @allocated begin
        try
            find_all_regimes!(model; max_enumeration_work=limit)
        catch err
            err isa BindingAndCatalysis._RegimeEnumerationWorkLimitExceeded ||
                rethrow()
        end
    end
end

function _heterodimer_ro_complex(; axes=(1, 2), lower=(-2.0, -2.0),
    upper=(2.0, 2.0), fixed=zeros(3), outputs=[3], kwargs...)
    model = _heterodimer_ro_model()
    domain = ROInputDomain2D(axes, lower, upper, fixed)
    return build_ro_cell_complex(model, domain, outputs; kwargs...)
end

function _ro2_test_complex_snapshot(complex)
    return (
        output_indices=copy(complex.output_indices),
        cells=[(
            id=cell.id,
            vertices=copy(cell.vertices),
            area=cell.area,
            source_regime_ids=copy(cell.source_regime_ids),
            labels=[(
                source_regime_ids=copy(label.source_regime_ids),
                reaction_order_matrix=copy(label.reaction_order_matrix),
                output_offset=copy(label.output_offset),
            ) for label in cell.labels],
            set_valued=cell.set_valued,
        ) for cell in complex.cells],
        facets=[(
            id=facet.id,
            kind=facet.kind,
            endpoints=facet.endpoints,
            incident_cell_ids=copy(facet.incident_cell_ids),
            singular_stratum_ids=copy(facet.singular_stratum_ids),
            normal=facet.normal,
            offset=facet.offset,
            mixed_sign=facet.mixed_sign,
            domain_side=facet.domain_side,
        ) for facet in complex.facets],
        singular_strata=[(
            id=stratum.id,
            dimension=stratum.dimension,
            vertices=copy(stratum.vertices),
            source_regime_ids=copy(stratum.source_regime_ids),
            nullities=copy(stratum.nullities),
            reasons=copy(stratum.reasons),
        ) for stratum in complex.singular_strata],
    )
end

function _ro2_test_classification_snapshot(classification)
    return (
        status=classification.status,
        cell_ids=copy(classification.cell_ids),
        cell_relations=copy(classification.cell_relations),
        facet_ids=copy(classification.facet_ids),
        singular_stratum_ids=copy(classification.singular_stratum_ids),
        labels=[(
            source_regime_ids=copy(label.source_regime_ids),
            reaction_order_matrix=copy(label.reaction_order_matrix),
            output_offset=copy(label.output_offset),
        ) for label in classification.labels],
        source_complex_sha256=classification.source_complex_sha256,
        source_authority_status=classification.source_authority_status,
    )
end

function _ro2_test_rebuild_complex(complex;
    domain=complex.domain,
    output_indices=complex.output_indices,
    cells=complex.cells,
    facets=complex.facets,
    singular_strata=complex.singular_strata,
    candidate_regime_count=complex.candidate_regime_count,
    regular_candidate_count=complex.regular_candidate_count,
    domain_area=complex.domain_area,
    covered_area_sum=complex.covered_area_sum,
    gap_area=complex.gap_area,
    coverage_complete=complex.coverage_complete,
    has_ambiguity=complex.has_ambiguity,
    geometry_tolerance=complex.geometry_tolerance,
    kwargs...,
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
        geometry_tolerance;
        kwargs...,
    )
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
    @test_throws ArgumentError classify_ro_cell_complex_point(
        complex, (100.0, 100.0); tolerance=100.0)
end

@testset "2D RO geometry tolerances retain length and area units" begin
    # Every geometric coordinate is scaled by 1e-6.  A length tolerance at the
    # documented relative cap is still many orders of magnitude smaller than
    # an edge, but it must not be compared directly with a cross product.
    complex = _heterodimer_ro_complex(
        lower=(-2e-6, -2e-6),
        upper=(2e-6, 2e-6),
        geometry_tolerance=4e-12,
    )

    @test length(complex.cells) == 3
    @test sort(getfield.(complex.cells, :area)) == [4e-12, 6e-12, 6e-12]
    @test length(filter(facet -> facet.kind === :internal, complex.facets)) == 3
    @test length(filter(facet -> facet.kind === :domain, complex.facets)) == 6
    @test complex.coverage_complete
    @test !complex.has_ambiguity
    @test complex.gap_area == 0.0

    tolerance = complex.geometry_tolerance
    left = [(-2e-6, -2e-6), (0.0, -2e-6), (0.0, 2e-6), (-2e-6, 2e-6)]
    adjacent = [(0.0, -2e-6), (2e-6, -2e-6), (2e-6, 2e-6), (0.0, 2e-6)]
    gapped = [(5e-7, -2e-6), (2e-6, -2e-6), (2e-6, 2e-6), (5e-7, 2e-6)]
    overlapping = [
        (-5e-7, -2e-6), (2e-6, -2e-6), (2e-6, 2e-6), (-5e-7, 2e-6),
    ]
    _, domain_area, _, area_tolerance =
        BindingAndCatalysis._ro2_certification_tolerances(complex.domain)
    @test BindingAndCatalysis._ro2_area(
        BindingAndCatalysis._ro2_convex_intersection(left, adjacent, tolerance)) <=
        area_tolerance
    @test domain_area - BindingAndCatalysis._ro2_area(left) -
        BindingAndCatalysis._ro2_area(gapped) > area_tolerance
    @test BindingAndCatalysis._ro2_area(
        BindingAndCatalysis._ro2_convex_intersection(left, overlapping, tolerance)) >
        area_tolerance
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
    merged = BindingAndCatalysis._ro2_unique_labels([same_a, same_b, other])
    @test length(merged) == 2
    @test sort(vcat(getfield.(merged, :source_regime_ids)...)) == [4, 7, 9]
    @test only(filter(label -> label.reaction_order_matrix == [1.0 0.0], merged)).source_regime_ids ==
        [4, 7]

    # Geometry tolerance is not a scientific-identity tolerance. Distinct
    # affine maps must remain set-valued even when their coefficients are much
    # closer than the tolerance used to clip and deduplicate cell geometry.
    close_a = ROAffineLabel2D([11], [1.0 0.0], [0.0])
    close_b = ROAffineLabel2D([12], [1.0 + 4e-12 0.0], [0.0])
    close_c = ROAffineLabel2D([15], [1.0 0.0], [4e-12])
    close_labels = BindingAndCatalysis._ro2_unique_labels(
        [close_a, close_b, close_c])
    @test length(close_labels) == 3

    # Signed zero is the sole benign Float64 representation difference in an
    # affine label identity.
    signed_zero_a = ROAffineLabel2D([13], [1.0 -0.0], [0.0])
    signed_zero_b = ROAffineLabel2D([14], [1.0 0.0], [-0.0])
    signed_zero_labels = BindingAndCatalysis._ro2_unique_labels(
        [signed_zero_a, signed_zero_b])
    @test length(signed_zero_labels) == 1
    @test only(signed_zero_labels).source_regime_ids == [13, 14]
end

@testset "2D RO domain seals its fixed background" begin
    caller_background = zeros(3)
    domain = ROInputDomain2D(
        (1, 2), (-2.0, -2.0), (2.0, 2.0), caller_background)
    complex = _heterodimer_ro_complex(fixed=caller_background)
    classification_point = (-1.0, -1.0)
    classification_before = _ro2_test_classification_snapshot(
        classify_ro_cell_complex_point(complex, classification_point))
    content_before = complex.content_sha256
    hash_before = hash(complex)

    caller_background[3] = 1.0
    @test collect(domain.fixed_logqK) == zeros(3)
    @test collect(complex.domain.fixed_logqK) == zeros(3)

    published_domain_background = domain.fixed_logqK
    published_complex_background = complex.domain.fixed_logqK
    published_domain_background[3] = 1.0
    published_complex_background[3] = 1.0
    @test_throws MethodError setindex!(getfield(domain, :fixed_logqK), 1.0, 3)
    @test collect(domain.fixed_logqK) == zeros(3)
    @test collect(complex.domain.fixed_logqK) == zeros(3)
    @test complex.content_sha256 == content_before
    @test hash(complex) == hash_before
    @test _ro2_test_classification_snapshot(
        classify_ro_cell_complex_point(
            complex, classification_point)) == classification_before
end

@testset "2D RO complex and classification publish detached snapshots" begin
    complex = _heterodimer_ro_complex()
    source_snapshot = _ro2_test_complex_snapshot(complex)
    source_sha256 = complex.content_sha256
    source_hash = hash(complex)

    complex.output_indices[1] = 99
    complex.cells[1].vertices[1] = (99.0, 99.0)
    complex.cells[1].source_regime_ids[1] = 99
    complex.cells[1].labels[1].source_regime_ids[1] = 99
    complex.cells[1].labels[1].reaction_order_matrix[1, 1] = 99.0
    complex.cells[1].labels[1].output_offset[1] = 99.0
    facet_index = something(findfirst(
        facet -> !isempty(facet.singular_stratum_ids), complex.facets))
    complex.facets[facet_index].incident_cell_ids[1] = 99
    complex.facets[facet_index].singular_stratum_ids[1] = 99
    complex.singular_strata[1].vertices[1] = (99.0, 99.0)
    complex.singular_strata[1].source_regime_ids[1] = 99
    complex.singular_strata[1].nullities[1] = 99
    complex.singular_strata[1].reasons[1] = :tampered
    @test _ro2_test_complex_snapshot(complex) == source_snapshot
    @test complex.content_sha256 == source_sha256
    @test hash(complex) == source_hash

    classification_complex = _heterodimer_ro_complex()
    point = (1.0, 1.0)
    classification = classify_ro_cell_complex_point(
        classification_complex, point)
    classification_snapshot = _ro2_test_classification_snapshot(classification)
    classification_source_snapshot = _ro2_test_complex_snapshot(
        classification_complex)
    classification_source_sha256 = classification_complex.content_sha256
    classification_source_hash = hash(classification_complex)
    classification.cell_ids[1] = 99
    classification.cell_relations[1] = :outside
    classification.facet_ids[1] = 99
    classification.singular_stratum_ids[1] = 99
    for label in classification.labels
        label.source_regime_ids[1] = 99
        label.reaction_order_matrix[1, 1] = 99.0
        label.output_offset[1] = 99.0
    end
    @test _ro2_test_complex_snapshot(
        classification_complex) == classification_source_snapshot
    @test classification_complex.content_sha256 == classification_source_sha256
    @test hash(classification_complex) == classification_source_hash
    @test _ro2_test_classification_snapshot(
        classify_ro_cell_complex_point(
            classification_complex, point)) == classification_snapshot

    constructor_outputs = complex.output_indices
    constructor_cells = complex.cells
    constructor_facets = complex.facets
    constructor_strata = complex.singular_strata
    constructed = ROCellComplex2D(
        complex.domain,
        constructor_outputs,
        constructor_cells,
        constructor_facets,
        constructor_strata,
        complex.candidate_regime_count,
        complex.regular_candidate_count,
        complex.domain_area,
        complex.covered_area_sum,
        complex.gap_area,
        complex.coverage_complete,
        complex.has_ambiguity,
        complex.geometry_tolerance,
    )
    constructed_snapshot = _ro2_test_complex_snapshot(constructed)
    constructed_sha256 = constructed.content_sha256
    @test complex.authority_status === :engine_replayed
    @test constructed.authority_status === :external_unverified
    @test constructed.content_sha256 != complex.content_sha256
    engine_classification = classify_ro_cell_complex_point(
        complex, (-1.0, -1.0))
    external_classification = classify_ro_cell_complex_point(
        constructed, (-1.0, -1.0))
    @test engine_classification.source_complex_sha256 == complex.content_sha256
    @test engine_classification.source_authority_status === :engine_replayed
    @test external_classification.source_complex_sha256 ==
        constructed.content_sha256
    @test external_classification.source_authority_status ===
        :external_unverified
    @test validate_ro_point_classification(
        complex, (-1.0, -1.0), engine_classification) ===
        engine_classification
    @test_throws ArgumentError validate_ro_point_classification(
        constructed, (-1.0, -1.0), engine_classification)
    @test_throws ArgumentError validate_ro_point_classification(
        complex, (1.0, -1.0), engine_classification)
    constructor_outputs[1] = 98
    constructor_cells[1].vertices[1] = (98.0, 98.0)
    constructor_cells[1].labels[1].reaction_order_matrix[1, 1] = 98.0
    constructor_facets[1].incident_cell_ids[1] = 98
    constructor_strata[1].vertices[1] = (98.0, 98.0)
    @test _ro2_test_complex_snapshot(constructed) == constructed_snapshot
    @test constructed.content_sha256 == constructed_sha256

    lower_level_tampered = _heterodimer_ro_complex()
    getfield(getfield(lower_level_tampered, :cells)[1], :vertices)[1] =
        (97.0, 97.0)
    @test_throws ArgumentError lower_level_tampered.cells
    @test_throws ArgumentError classify_ro_cell_complex_point(
        lower_level_tampered, (-1.0, -1.0))
end

@testset "2D RO public constructors reject forged state and detach classifications" begin
    domain = ROInputDomain2D(
        (1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(2))
    square = [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)]
    forged_cell = ROCell2D(
        -7,
        square,
        4.0,
        Int[],
        [ROAffineLabel2D(Int[], zeros(1, 2), [0.0])],
        false,
    )
    forged_facets = ROFacet2D[
        ROFacet2D(
            index,
            :domain,
            (square[index], square[index == 4 ? 1 : index + 1]),
            [-7],
            Int[],
            (NaN, NaN),
            NaN,
            isodd(index),
            (:axis2_lower, :axis1_upper, :axis2_upper, :axis1_lower)[index],
        ) for index in 1:4
    ]
    @test_throws ArgumentError ROCellComplex2D(
        domain,
        [-1],
        [forged_cell],
        forged_facets,
        ROSingularStratum2D[],
        -1,
        -2,
        4.0,
        4.0,
        0.0,
        true,
        false,
        1e-6,
    )

    valid = _heterodimer_ro_complex()
    @test_throws ArgumentError _ro2_test_rebuild_complex(
        valid; candidate_regime_count=-1)
    @test_throws ArgumentError _ro2_test_rebuild_complex(
        valid; domain_area=NaN)
    @test_throws ArgumentError _ro2_test_rebuild_complex(
        valid; gap_area=1.0)
    @test_throws ArgumentError _ro2_test_rebuild_complex(
        valid; coverage_complete=false)
    @test_throws ArgumentError _ro2_test_rebuild_complex(
        valid; geometry_tolerance=Inf)

    bad_facet = valid.facets[1]
    bad_facets = valid.facets
    bad_facets[1] = ROFacet2D(
        bad_facet.id,
        bad_facet.kind,
        bad_facet.endpoints,
        [999],
        bad_facet.singular_stratum_ids,
        bad_facet.normal,
        bad_facet.offset,
        bad_facet.mixed_sign,
        bad_facet.domain_side,
    )
    @test_throws ArgumentError _ro2_test_rebuild_complex(valid; facets=bad_facets)

    bad_cell = valid.cells[1]
    bad_cells = valid.cells
    bad_cells[1] = ROCell2D(
        bad_cell.id,
        bad_cell.vertices,
        bad_cell.area,
        bad_cell.source_regime_ids,
        [ROAffineLabel2D(
            Int[],
            zeros(length(valid.output_indices), 2),
            zeros(length(valid.output_indices)),
        )],
        false,
    )
    @test_throws ArgumentError _ro2_test_rebuild_complex(valid; cells=bad_cells)

    source_label = ROAffineLabel2D([1], [2.0 3.0], [4.0])
    @test_throws ArgumentError ROPointClassification2D(
        :cell,
        [1],
        [:interior],
        Int[],
        Int[],
        [source_label],
    )
    second_label = ROAffineLabel2D([2], [3.0 2.0], [5.0])
    @test_throws ArgumentError BindingAndCatalysis._ro2_validate_classification(
        :cell,
        [1],
        [:interior],
        Int[],
        Int[],
        [source_label, second_label],
    )
    first_cell = valid.cells[1]
    point = (
        sum(vertex[1] for vertex in first_cell.vertices) /
            length(first_cell.vertices),
        sum(vertex[2] for vertex in first_cell.vertices) /
            length(first_cell.vertices),
    )
    classification = classify_ro_cell_complex_point(valid, point)
    @test classification.status in (:cell, :boundary)
    published_ids = classification.cell_ids
    isempty(published_ids) || (published_ids[1] = 98)
    @test classification.cell_ids != published_ids || isempty(published_ids)
    classification_limit_error = try
        classify_ro_cell_complex_point(
            valid,
            point;
            limits=ROCellComplexBuildLimits(max_matrix_elements=1),
        )
        nothing
    catch err
        err
    end
    @test classification_limit_error isa ROCellComplexLimitExceeded
    @test classification_limit_error.phase === :matrix_elements
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

@testset "2D RO candidate identity is metric, not a decimal bucket" begin
    limits = ROCellComplexBuildLimits(max_pair_checks=100)

    tiny_tolerance = 1e-18
    tiny_a = [
        (1.0, 1.0),
        (1.000001, 1.0),
        (1.000001, 1.000001),
        (1.0, 1.000001),
    ]
    tiny_b = [(x + 4e-15, y) for (x, y) in tiny_a]
    tiny_key = BindingAndCatalysis._ro2_key(tiny_a, 2, tiny_tolerance)
    @test tiny_key !=
        BindingAndCatalysis._ro2_key(tiny_b, 2, tiny_tolerance)
    tiny_candidates = Dict{String,BindingAndCatalysis._ROCellCandidate2D}(
        tiny_key => BindingAndCatalysis._ROCellCandidate2D(
            tiny_a, [1], ROAffineLabel2D[]),
    )
    tiny_work = Ref(BigInt(0))
    distinct_key = BindingAndCatalysis._ro2_resolve_candidate_key(
        tiny_candidates,
        tiny_key,
        tiny_b,
        2,
        tiny_tolerance,
        tiny_work,
        limits,
    )
    @test distinct_key != tiny_key
    @test tiny_work[] == 4

    tolerance = 1e-9
    left = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]
    right = [(x + tolerance / 2, y) for (x, y) in left]
    left_key = BindingAndCatalysis._ro2_key(left, 2, tolerance)
    right_key = BindingAndCatalysis._ro2_key(right, 2, tolerance)
    @test left_key != right_key
    candidates = Dict{String,BindingAndCatalysis._ROCellCandidate2D}(
        left_key => BindingAndCatalysis._ROCellCandidate2D(
            left, [1], ROAffineLabel2D[]),
    )
    work = Ref(BigInt(0))
    @test BindingAndCatalysis._ro2_resolve_candidate_key(
        candidates,
        right_key,
        right,
        2,
        tolerance,
        work,
        limits,
    ) == left_key
    @test work[] == 4

    incompatible = Dict{String,BindingAndCatalysis._ROStratumCandidate2D}()
    for index in 1:100
        incompatible[string(index)] = BindingAndCatalysis._ROStratumCandidate2D(
            1,
            [(Float64(index), 0.0)],
            [index],
            [0],
            [:lower_dimensional_slice],
        )
    end
    incompatible_work = Ref(BigInt(0))
    scan_error = try
        BindingAndCatalysis._ro2_resolve_candidate_key(
            incompatible,
            "unused",
            left,
            2,
            tolerance,
            incompatible_work,
            ROCellComplexBuildLimits(max_pair_checks=50),
        )
        nothing
    catch err
        err
    end
    @test scan_error isa ROCellComplexLimitExceeded
    @test scan_error.phase === :pair_checks
    @test scan_error.requested == 51
    @test incompatible_work[] == 51

    underflow_tolerance = 1e-200
    underflow_left = [(0.0, 0.0)]
    underflow_right = [(1e-190, 0.0)]
    @test !BindingAndCatalysis._ro2_vertices_equivalent(
        underflow_left, underflow_right, underflow_tolerance)
    @test length(BindingAndCatalysis._ro2_unique_points(
        vcat(underflow_left, underflow_right), underflow_tolerance)) == 2

    origin_tolerance = 1e-9
    opposite_near_origin = [
        (-0.9 * origin_tolerance, 0.0),
        (0.9 * origin_tolerance, 0.0),
    ]
    @test BindingAndCatalysis._ro2_distance(
        opposite_near_origin...) > origin_tolerance
    @test length(BindingAndCatalysis._ro2_unique_points(
        opposite_near_origin, origin_tolerance)) == 2

    facet_accumulators =
        Dict{String,BindingAndCatalysis._ROFacetAccumulator2D}()
    facet_work = Ref(BigInt(0))
    first_segment = ((0.0, 0.0), (1.0, 0.0))
    equivalent_segment = (
        (0.0, tolerance / 2),
        (1.0, tolerance / 2),
    )
    @test BindingAndCatalysis._ro2_segment_key(
        first_segment, tolerance) != BindingAndCatalysis._ro2_segment_key(
        equivalent_segment, tolerance)
    BindingAndCatalysis._ro2_add_facet!(
        facet_accumulators,
        :internal,
        first_segment,
        (1,),
        nothing,
        tolerance,
        ROCellComplexBuildLimits(max_pair_checks=100),
        facet_work,
    )
    BindingAndCatalysis._ro2_add_facet!(
        facet_accumulators,
        :internal,
        equivalent_segment,
        (2,),
        nothing,
        tolerance,
        ROCellComplexBuildLimits(max_pair_checks=100),
        facet_work,
    )
    @test length(facet_accumulators) == 1
    @test only(values(facet_accumulators)).incident_cell_ids == Set([1, 2])
    @test facet_work[] == 3

    crossed_accumulators =
        Dict{String,BindingAndCatalysis._ROFacetAccumulator2D}()
    crossed_work = Ref(BigInt(0))
    epsilon = tolerance / 2
    crossed_a = ((0.0, 0.0), (epsilon, 1.0))
    crossed_b = ((epsilon, 0.0), (0.0, 1.0))
    for (cell_id, segment) in enumerate((crossed_a, crossed_b))
        BindingAndCatalysis._ro2_add_facet!(
            crossed_accumulators,
            :internal,
            segment,
            (cell_id,),
            nothing,
            tolerance,
            ROCellComplexBuildLimits(max_finalization_work=100),
            crossed_work,
        )
    end
    @test length(crossed_accumulators) == 1
    @test only(values(crossed_accumulators)).incident_cell_ids == Set([1, 2])
    @test crossed_work[] == 3
end

@testset "2D RO complex ambiguity and gap classification" begin
    domain = ROInputDomain2D((1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(3))
    box_vertices = [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0)]
    label_a = ROAffineLabel2D([1], [1.0 0.0], [0.0])
    label_b = ROAffineLabel2D([2], [0.0 1.0], [0.0])
    ambiguous_cell = ROCell2D(1, box_vertices, 4.0, [1, 2],
        [label_a, label_b], true)
    box_facets = ROFacet2D[
        ROFacet2D(1, :domain, ((-1.0, -1.0), (-1.0, 1.0)), [1], Int[],
            (1.0, 0.0), 1.0, false, :axis1_lower),
        ROFacet2D(2, :domain, ((-1.0, -1.0), (1.0, -1.0)), [1], Int[],
            (0.0, 1.0), 1.0, false, :axis2_lower),
        ROFacet2D(3, :domain, ((-1.0, 1.0), (1.0, 1.0)), [1], Int[],
            (0.0, 1.0), -1.0, false, :axis2_upper),
        ROFacet2D(4, :domain, ((1.0, -1.0), (1.0, 1.0)), [1], Int[],
            (1.0, 0.0), -1.0, false, :axis1_upper),
    ]
    ambiguous_complex = ROCellComplex2D(
        domain, [3], [ambiguous_cell], box_facets, ROSingularStratum2D[],
        2, 2, 4.0, 4.0, 0.0, true, true, 1e-9)
    ambiguity = classify_ro_cell_complex_point(ambiguous_complex, (0.0, 0.0))
    @test ambiguity.status === :ambiguity
    @test length(ambiguity.labels) == 2
    @test sort(vcat(getfield.(ambiguity.labels, :source_regime_ids)...)) == [1, 2]

    interior_stratum = ROSingularStratum2D(
        1, 0, [(0.0, 0.0)], [2], [1], [:singular_regime])
    regular_cell = ROCell2D(
        1, box_vertices, 4.0, [1], [label_a], false)
    interior_stratum_complex = ROCellComplex2D(
        domain,
        [3],
        [regular_cell],
        box_facets,
        [interior_stratum],
        2,
        1,
        4.0,
        4.0,
        0.0,
        true,
        false,
        1e-9,
    )
    interior_singular = classify_ro_cell_complex_point(
        interior_stratum_complex, (0.0, 0.0))
    @test interior_singular.status === :boundary
    @test interior_singular.cell_relations == [:interior]
    @test interior_singular.singular_stratum_ids == [1]

    gap_complex = ROCellComplex2D(
        domain, [3], ROCell2D[], ROFacet2D[], ROSingularStratum2D[],
        0, 0, 4.0, 0.0, 4.0, false, false, 1e-9)
    @test classify_ro_cell_complex_point(gap_complex, (0.0, 0.0)).status === :gap
end

@testset "2D RO complex budgets, validation, and cooperative cancellation" begin
    for domain in (
        ROInputDomain2D(
            (1, 2), (-1e308, -1e308), (1e308, 1e308), zeros(3)),
        ROInputDomain2D(
            (1, 2), (0.0, 0.0), (1e-170, 1e-170), zeros(3)),
    )
        invalid_domain_model = _heterodimer_ro_model()
        @test_throws ArgumentError build_ro_cell_complex(
            invalid_domain_model,
            domain,
            [3];
            geometry_tolerance=1e-180,
        )
        @test !getfield(invalid_domain_model, :_regimes_build_complete)
        @test isnothing(getfield(invalid_domain_model, :BindRegimes))
    end

    output_reads = Ref(0)
    output_cancel_checks = Ref(0)
    output_probe_model = _heterodimer_ro_model()
    @test_throws ROCellComplexCancelProbe build_ro_cell_complex(
        output_probe_model,
        ROInputDomain2D(
            (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
        ROCountingIndexVector(3, output_reads);
        cancel_check=() -> begin
            output_cancel_checks[] += 1
            output_cancel_checks[] == 2 && throw(ROCellComplexCancelProbe())
        end,
    )
    @test output_reads[] == 0
    @test !getfield(output_probe_model, :_regimes_build_complete)

    output_error = try
        _heterodimer_ro_complex(
            outputs=[1, 2],
            limits=ROCellComplexBuildLimits(max_outputs=1),
        )
        nothing
    catch err
        err
    end
    @test output_error isa ROCellComplexLimitExceeded
    @test output_error.phase === :outputs
    @test output_error.requested == 2

    matrix_error = try
        _heterodimer_ro_complex(
            outputs=[1, 2],
            limits=ROCellComplexBuildLimits(
                max_outputs=2,
                max_matrix_elements=1,
            ),
        )
        nothing
    catch err
        err
    end
    @test matrix_error isa ROCellComplexLimitExceeded
    @test matrix_error.phase === :matrix_elements

    built_for_finalization = _heterodimer_ro_complex()
    payload_error = try
        _ro2_test_rebuild_complex(
            built_for_finalization;
            limits=ROCellComplexBuildLimits(max_payload_elements=1),
        )
        nothing
    catch err
        err
    end
    @test payload_error isa ROCellComplexLimitExceeded
    @test payload_error.phase === :payload_elements
    finalization_error = try
        _ro2_test_rebuild_complex(
            built_for_finalization;
            limits=ROCellComplexBuildLimits(max_finalization_work=1),
        )
        nothing
    catch err
        err
    end
    @test finalization_error isa ROCellComplexLimitExceeded
    @test finalization_error.phase === :finalization_work

    many_vertices = [
        (cospi(2 * (index - 1) / 100), sinpi(2 * (index - 1) / 100))
        for index in 1:100
    ]
    many_vertex_cell = ROCell2D(
        1, many_vertices, 1.0, [1], ROAffineLabel2D[], false)
    quadratic_work_error = try
        BindingAndCatalysis._ro2_preflight_geometry_work(
            [many_vertex_cell],
            0,
            0,
            ROCellComplexBuildLimits(max_finalization_work=750),
        )
        nothing
    catch err
        err
    end
    @test quadratic_work_error isa ROCellComplexLimitExceeded
    @test quadratic_work_error.phase === :finalization_work
    @test quadratic_work_error.requested == 10_100
    @test quadratic_work_error.limit == 750

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

    fresh_candidate_model = _heterodimer_ro_model()
    fresh_candidate_error = try
        build_ro_cell_complex(
            fresh_candidate_model,
            ROInputDomain2D(
                (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
            [3];
            limits=ROCellComplexBuildLimits(max_candidate_regimes=1),
        )
        nothing
    catch err
        err
    end
    @test fresh_candidate_error isa ROCellComplexLimitExceeded
    @test fresh_candidate_error.phase === :candidate_regimes
    @test fresh_candidate_error.requested == 2
    @test !getfield(fresh_candidate_model, :_regimes_build_complete)
    @test isnothing(getfield(fresh_candidate_model, :BindRegimes))
    retried_candidate = build_ro_cell_complex(
        fresh_candidate_model,
        ROInputDomain2D(
            (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
        [3],
    )
    @test retried_candidate.authority_status === :engine_replayed
    @test getfield(fresh_candidate_model, :_regimes_build_complete)
    hot_candidate_error = try
        build_ro_cell_complex(
            fresh_candidate_model,
            ROInputDomain2D(
                (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
            [3];
            limits=ROCellComplexBuildLimits(max_candidate_regimes=1),
        )
        nothing
    catch err
        err
    end
    @test hot_candidate_error isa ROCellComplexLimitExceeded
    @test hot_candidate_error.requested == 4
    @test getfield(fresh_candidate_model, :_regimes_build_complete)

    cancelled_candidate_model = _heterodimer_ro_model()
    enumeration_cancel_checks = Ref(0)
    @test_throws ROCellComplexCancelProbe build_ro_cell_complex(
        cancelled_candidate_model,
        ROInputDomain2D(
            (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
        [3];
        cancel_check=() -> begin
            enumeration_cancel_checks[] += 1
            enumeration_cancel_checks[] == 20 &&
                throw(ROCellComplexCancelProbe())
        end,
    )
    @test !getfield(cancelled_candidate_model, :_regimes_build_complete)
    @test isnothing(getfield(cancelled_candidate_model, :BindRegimes))
    @test build_ro_cell_complex(
        cancelled_candidate_model,
        ROInputDomain2D(
            (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
        [3],
    ).authority_status === :engine_replayed

    enumeration_work_model = _heterodimer_ro_model()
    enumeration_work_error = try
        build_ro_cell_complex(
            enumeration_work_model,
            ROInputDomain2D(
                (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
            [3];
            limits=ROCellComplexBuildLimits(max_enumeration_work=1),
        )
        nothing
    catch err
        err
    end
    @test enumeration_work_error isa ROCellComplexLimitExceeded
    @test enumeration_work_error.phase === :enumeration_work
    expected_initial_enumeration_work =
        BindingAndCatalysis._regime_enumeration_base_work(
            getfield(enumeration_work_model, :_L_helper))
    @test enumeration_work_error.requested == expected_initial_enumeration_work
    @test !getfield(enumeration_work_model, :_regimes_build_complete)
    @test isnothing(getfield(enumeration_work_model, :BindRegimes))

    wide_choice_count = 600
    wide_choice_model = Bnc(L=ones(Int, 1, wide_choice_count))
    wide_helper = getfield(wide_choice_model, :_L_helper)
    # This is intentionally encoded independently of the source helper: one
    # row has n choices and n(n-1) oriented weighted edges.
    manual_wide_static_work = BigInt(1)^2 + 3 * BigInt(1) +
        6 * BigInt(wide_choice_count) + BigInt(wide_choice_count) +
        2 * BigInt(wide_choice_count) * BigInt(wide_choice_count - 1) + 5
    @test BindingAndCatalysis._regime_enumeration_static_work(wide_helper) ==
        manual_wide_static_work
    direct_preflight_checks = Ref(0)
    direct_preflight_error = try
        BindingAndCatalysis._enumerate_all_regimes(
            wide_helper;
            max_enumeration_work=Int(manual_wide_static_work - 1),
            cancel_check=() -> (direct_preflight_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test direct_preflight_error isa
        BindingAndCatalysis._RegimeEnumerationWorkLimitExceeded
    @test direct_preflight_error.requested == manual_wide_static_work
    @test direct_preflight_checks[] == 1

    public_preflight_checks = Ref(0)
    public_preflight_error = try
        find_all_regimes!(
            wide_choice_model;
            max_enumeration_work=Int(manual_wide_static_work - 1),
            cancel_check=() -> (public_preflight_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test public_preflight_error isa
        BindingAndCatalysis._RegimeEnumerationWorkLimitExceeded
    @test public_preflight_error.requested == manual_wide_static_work
    @test public_preflight_checks[] == 3
    @test !getfield(wide_choice_model, :_regimes_build_complete)
    @test isnothing(getfield(wide_choice_model, :BindRegimes))

    # Warm the measurement wrapper, then prove the rejected public path no
    # longer allocates in proportion to the quadratic weighted-edge population.
    allocation_warmup_model = Bnc(L=ones(Int, 1, 16))
    allocation_warmup_limit = Int(
        BindingAndCatalysis._regime_enumeration_static_work(
            getfield(allocation_warmup_model, :_L_helper)) - 1)
    _ro2_public_enumeration_preflight_allocations(
        allocation_warmup_model, allocation_warmup_limit)
    small_allocation_model = Bnc(L=ones(Int, 1, 100))
    large_allocation_model = Bnc(L=ones(Int, 1, wide_choice_count))
    small_allocation_limit = Int(
        BindingAndCatalysis._regime_enumeration_static_work(
            getfield(small_allocation_model, :_L_helper)) - 1)
    large_allocation_limit = Int(manual_wide_static_work - 1)
    small_preflight_allocations = _ro2_public_enumeration_preflight_allocations(
        small_allocation_model, small_allocation_limit)
    large_preflight_allocations = _ro2_public_enumeration_preflight_allocations(
        large_allocation_model, large_allocation_limit)
    @test large_preflight_allocations <= small_preflight_allocations + 1_024
    @test isnothing(getfield(large_allocation_model, :BindRegimes))

    large_row_count = 600
    large_row_L = zeros(Int, large_row_count, large_row_count)
    for index in 1:large_row_count
        large_row_L[index, index] = 1
    end
    large_row_model = Bnc(L=large_row_L)
    large_row_helper = getfield(large_row_model, :_L_helper)
    large_row_base_work = BigInt(large_row_count)^2 +
        3 * BigInt(large_row_count) +
        6 * BigInt(large_row_count) + 4
    @test BindingAndCatalysis._regime_enumeration_base_work(
        large_row_helper) == large_row_base_work
    large_row_early_checks = Ref(0)
    large_row_early_error = try
        BindingAndCatalysis._enumerate_all_regimes(
            large_row_helper;
            max_enumeration_work=1,
            cancel_check=() -> (large_row_early_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test large_row_early_error isa
        BindingAndCatalysis._RegimeEnumerationWorkLimitExceeded
    @test large_row_early_error.requested == large_row_base_work
    @test large_row_early_checks[] == 0

    large_row_scan_checks = Ref(0)
    large_row_scan_work = Ref(BigInt(0))
    @test_throws ROCellComplexCancelProbe begin
        BindingAndCatalysis._enumerate_all_regimes(
            large_row_helper;
            max_enumeration_work=1_000_000_000,
            enumeration_work_counter=large_row_scan_work,
            cancel_check=() -> begin
                large_row_scan_checks[] += 1
                large_row_scan_checks[] == 2 &&
                    throw(ROCellComplexCancelProbe())
            end,
        )
    end
    @test large_row_scan_checks[] == 2
    @test large_row_scan_work[] == large_row_base_work + 2 * 256

    exact_static_model = Bnc(L=ones(Int, 1, 32))
    exact_static_helper = getfield(exact_static_model, :_L_helper)
    exact_static_work = BindingAndCatalysis._regime_enumeration_static_work(
        exact_static_helper)
    exact_static_error = try
        BindingAndCatalysis._enumerate_all_regimes(
            exact_static_helper;
            max_enumeration_work=Int(exact_static_work),
        )
        nothing
    catch err
        err
    end
    @test exact_static_error isa
        BindingAndCatalysis._RegimeEnumerationWorkLimitExceeded
    @test exact_static_error.requested == exact_static_work + 1

    block_count = 2
    pilot_model = _weighted_block_ro_model(block_count)
    pilot_work = Ref(BigInt(0))
    find_all_regimes!(
        pilot_model;
        max_asymptotic_regimes=4,
        max_enumeration_work=1_000_000_000,
        enumeration_work_counter=pilot_work,
    )
    pilot_regimes = BindingAndCatalysis._bind_regimes_data(pilot_model)
    @test length(pilot_regimes) == 9
    @test count(is_asymptotic, pilot_regimes) == 4
    exact_enumeration_work = Int(pilot_work[])
    @test exact_enumeration_work > 1
    block_domain = ROInputDomain2D(
        (1, 3), (-2.0, -2.0), (2.0, 2.0), zeros(pilot_model.n))
    bounded_block_model = _weighted_block_ro_model(block_count)
    block_limit_kwargs = (
        max_candidate_regimes=4,
        max_matrix_elements=10_000_000,
        max_payload_elements=20_000_000,
        max_finalization_work=50_000_000,
    )
    exact_enumeration_error = try
        build_ro_cell_complex(
            bounded_block_model,
            block_domain,
            [1];
            limits=ROCellComplexBuildLimits(
                ; max_enumeration_work=exact_enumeration_work - 1,
                block_limit_kwargs...),
        )
        nothing
    catch err
        err
    end
    @test exact_enumeration_error isa ROCellComplexLimitExceeded
    @test exact_enumeration_error.phase === :enumeration_work
    @test exact_enumeration_error.requested == exact_enumeration_work
    @test !getfield(bounded_block_model, :_regimes_build_complete)
    @test isnothing(getfield(bounded_block_model, :BindRegimes))
    bounded_block_complex = build_ro_cell_complex(
        bounded_block_model,
        block_domain,
        [1];
        limits=ROCellComplexBuildLimits(
            ; max_enumeration_work=exact_enumeration_work,
            block_limit_kwargs...),
    )
    @test bounded_block_complex.candidate_regime_count == 4
    @test bounded_block_complex.authority_status === :engine_replayed
    @test getfield(bounded_block_model, :_regimes_build_complete)

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
    @test pair_error.requested == 4

    pair_model = Bnc(
        N=[1 1 -1 0 0 0; 0 0 0 1 1 -1],
        L=[1 0 1 0 0 0; 0 1 1 0 0 0; 0 0 0 1 0 1; 0 0 0 0 1 1],
        x_sym=[:A, :B, :AB, :C, :D, :CD],
        q_sym=[:tA, :tB, :tC, :tD],
        K_sym=[:K1, :K2],
    )
    pair_domain = ROInputDomain2D(
        (1, 3), (-2.0, -2.0), (2.0, 2.0), zeros(6))
    pair_limit_kwargs = (
        max_matrix_elements=10_000_000,
        max_payload_elements=20_000_000,
        max_finalization_work=50_000_000,
    )
    exact_pair_error = try
        build_ro_cell_complex(
            pair_model,
            pair_domain,
            [3, 6];
            limits=ROCellComplexBuildLimits(
                ; max_pair_checks=129, pair_limit_kwargs...),
        )
        nothing
    catch err
        err
    end
    @test exact_pair_error isa ROCellComplexLimitExceeded
    @test exact_pair_error.phase === :pair_checks
    @test exact_pair_error.requested == 130
    exact_pair_complex = build_ro_cell_complex(
        pair_model,
        pair_domain,
        [3, 6];
        limits=ROCellComplexBuildLimits(
            ; max_pair_checks=130, pair_limit_kwargs...),
    )
    @test (length(exact_pair_complex.cells),
        length(exact_pair_complex.singular_strata)) == (4, 5)

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

    built = _heterodimer_ro_complex()
    scalar_preflight_checks = Ref(0)
    scalar_preflight_error = try
        _ro2_test_rebuild_complex(
            built;
            limits=ROCellComplexBuildLimits(max_candidate_regimes=3),
            cancel_check=() -> (scalar_preflight_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test scalar_preflight_error isa ROCellComplexLimitExceeded
    @test scalar_preflight_error.phase === :candidate_regimes
    @test scalar_preflight_checks[] == 1

    footprint_model = _heterodimer_ro_model()
    manual_minimum_footprint = 204 + 12 + 29
    preflight_footprint = BindingAndCatalysis._ro2_preflight_model_matrix_elements(
        footprint_model,
        1,
        ROCellComplexBuildLimits(max_matrix_elements=1_000_000),
    )
    @test preflight_footprint >= manual_minimum_footprint
    footprint_cancel_checks = Ref(0)
    footprint_error = try
        build_ro_cell_complex(
            footprint_model,
            ROInputDomain2D(
                (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
            [3];
            limits=ROCellComplexBuildLimits(
                max_matrix_elements=manual_minimum_footprint - 1),
            cancel_check=() -> (footprint_cancel_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test footprint_error isa ROCellComplexLimitExceeded
    @test footprint_error.phase === :matrix_elements
    @test footprint_error.requested == preflight_footprint
    @test footprint_cancel_checks[] == 0
    @test !getfield(footprint_model, :_regimes_build_complete)
    @test isnothing(getfield(footprint_model, :BindRegimes))
    @test build_ro_cell_complex(
        footprint_model,
        ROInputDomain2D(
            (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
        [3];
        limits=ROCellComplexBuildLimits(
            max_matrix_elements=Int(preflight_footprint)),
    ).authority_status === :engine_replayed

    projection_model = _heterodimer_ro_model()
    projection_reservation =
        BindingAndCatalysis._ro2_preflight_model_projection_work(
            projection_model,
            ROCellComplexBuildLimits(max_finalization_work=1_000_000_000),
        )
    @test projection_reservation > 1
    projection_cancel_checks = Ref(0)
    projection_error = try
        build_ro_cell_complex(
            projection_model,
            ROInputDomain2D(
                (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3)),
            [3];
            limits=ROCellComplexBuildLimits(
                max_finalization_work=Int(projection_reservation - 1)),
            cancel_check=() -> (projection_cancel_checks[] += 1),
        )
        nothing
    catch err
        err
    end
    @test projection_error isa ROCellComplexLimitExceeded
    @test projection_error.phase === :finalization_work
    @test projection_error.requested == projection_reservation
    @test projection_cancel_checks[] == 0
    @test !getfield(projection_model, :_regimes_build_complete)
    @test isnothing(getfield(projection_model, :BindRegimes))

    final_checks = Ref(0)
    @test_throws ROCellComplexCancelProbe _ro2_test_rebuild_complex(
        built;
        cancel_check=() -> begin
            final_checks[] += 1
            final_checks[] == 3 && throw(ROCellComplexCancelProbe())
        end,
    )
    @test final_checks[] == 3

    classification_hash_checks(cell_ids, facet_ids, stratum_ids) = begin
        count = Ref(0)
        BindingAndCatalysis._ro2_classification_content_sha256(
            :gap,
            cell_ids,
            Symbol[],
            facet_ids,
            stratum_ids,
            ROAffineLabel2D[],
            built.content_sha256,
            built.authority_status;
            cancel_check=() -> (count[] += 1),
        )
        count[]
    end
    empty_hash_checks = classification_hash_checks(Int[], Int[], Int[])
    many_ids = collect(1:5_000)
    @test classification_hash_checks(many_ids, Int[], Int[]) >
        empty_hash_checks
    @test classification_hash_checks(Int[], many_ids, Int[]) >
        empty_hash_checks
    @test classification_hash_checks(Int[], Int[], many_ids) >
        empty_hash_checks

    classification_checks = Ref(0)
    classify_ro_cell_complex_point(
        built,
        (-1.0, -1.0);
        cancel_check=() -> (classification_checks[] += 1),
    )
    classification_final_checkpoint = classification_checks[]
    @test classification_final_checkpoint > 1
    classification_cancel_checks = Ref(0)
    @test_throws ROCellComplexCancelProbe classify_ro_cell_complex_point(
        built,
        (-1.0, -1.0);
        cancel_check=() -> begin
            classification_cancel_checks[] += 1
            classification_cancel_checks[] == classification_final_checkpoint &&
                throw(ROCellComplexCancelProbe())
        end,
    )
    @test classification_cancel_checks[] == classification_final_checkpoint

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
