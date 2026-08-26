using Test
using BindingAndCatalysis
using BiocircuitsExplorerBackend

const _ROFA_BACKEND = BiocircuitsExplorerBackend
if !isdefined(_ROFA_BACKEND, :ROFieldSignatureConfig)
    Base.include(_ROFA_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_behavior.jl"))
end
if !isdefined(_ROFA_BACKEND, :ROFieldAtlasInput)
    Base.include(_ROFA_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_atlas.jl"))
end

const _ROFA_HETERODIMER_HASH = repeat("a", 64)
const _ROFA_OPPOSED_HASH = repeat("b", 64)

struct ROFieldAtlasCancelProbe <: Exception end

function _rofa_test_heterodimer_complex()
    model = Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :B, :AB],
        q_sym=[:tA, :tB],
        K_sym=[:Kd],
    )
    domain = ROInputDomain2D(
        (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3))
    return build_ro_cell_complex(model, domain, [3])
end

function _rofa_test_opposed_gradient_variant(base)
    cells = ROCell2D[]
    for cell in base.cells
        label = ROAffineLabel2D(
            only(cell.labels).source_regime_ids,
            [1.0 -1.0],
            [0.0],
        )
        push!(cells, ROCell2D(
            cell.id,
            cell.vertices,
            cell.area,
            cell.source_regime_ids,
            [label],
            false,
        ))
    end
    return ROCellComplex2D(
        base.domain,
        [3],
        cells,
        base.facets,
        base.singular_strata,
        base.candidate_regime_count,
        base.regular_candidate_count,
        base.domain_area,
        base.covered_area_sum,
        base.gap_area,
        base.coverage_complete,
        false,
        base.geometry_tolerance,
    )
end

function _rofa_test_inputs()
    base = _rofa_test_heterodimer_complex()
    opposed = _rofa_test_opposed_gradient_variant(base)
    heterodimer = _ROFA_BACKEND.ROFieldAtlasInput(
        "heterodimer";
        representation=:exact_cell_complex,
        field_sha256=_ROFA_HETERODIMER_HASH,
        complex=base,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    opposed_input = _ROFA_BACKEND.ROFieldAtlasInput(
        "opposed_fixture";
        representation="exact_cell_complex",
        field_sha256=_ROFA_OPPOSED_HASH,
        complex=opposed,
        axis_ids=[:tA, :tB],
        output_ids=[:AB],
    )
    return base, heterodimer, opposed_input
end

function _rofa_test_demo_config(base)
    return _ROFA_BACKEND.ROFieldAtlasConfig(
        max_fields=2,
        max_total_cells=2 * length(base.cells),
        max_total_facets=2 * length(base.facets),
    )
end

@testset "bounded explicit RO-field demo Atlas" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    config = _rofa_test_demo_config(base)
    atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [opposed, heterodimer]; config=config)

    @test atlas["schema_version"] == "bne-ro-field-atlas/v1.0.0"
    @test atlas["atlas_kind"] == "explicit_exact_2d_demo_corpus"
    @test atlas["network_space_claim"] == "none"
    @test atlas["source_population"]["selection"] ==
        "caller_declared_explicit_records"
    @test !atlas["source_population"]["enumeration_performed"]
    @test !atlas["source_population"]["topology_search_performed"]
    @test occursin(r"^[0-9a-f]{64}$", atlas["atlas_sha256"])
    @test atlas["record_order"] == ["heterodimer", "opposed_fixture"]
    @test atlas["population"]["submitted_count"] == 2
    @test atlas["population"]["eligible_count"] == 2
    @test atlas["population"]["evaluated_count"] == 2
    @test atlas["population"]["classified_count"] == 2
    @test atlas["population"]["diagnostic_count"] == 0
    @test atlas["population"]["omitted_count"] == 0
    @test atlas["population"]["ineligible_count"] == 0
    @test atlas["config"]["preflight_total_cells"] == 2length(base.cells)
    @test atlas["config"]["preflight_total_facets"] == 2length(base.facets)

    # Corpus identity is canonical over the explicit record set, not caller
    # vector order.  Both records keep the classifier's own signature identity.
    reversed = _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, opposed]; config=config)
    @test reversed["atlas_sha256"] == atlas["atlas_sha256"]
    @test getindex.(atlas["records"], "signature_sha256") ==
        getindex.(reversed["records"], "signature_sha256")
    @test all(occursin(r"^[0-9a-f]{64}$", record["signature_sha256"])
              for record in atlas["records"])
end

@testset "component, family, cell, and facet queries" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, opposed]; config=_rofa_test_demo_config(base))

    component_query = _ROFA_BACKEND.ROFieldAtlasQuerySpec(
        component_filters=[
            _ROFA_BACKEND.ROFieldComponentFilter(
                "AB", "tA", "nonnegative_variable"),
            _ROFA_BACKEND.ROFieldComponentFilter(
                "AB", "tB", "nonnegative_variable"),
        ],
    )
    component_result = _ROFA_BACKEND.query_ro_field_atlas(
        atlas, component_query)
    @test component_result["schema_version"] ==
        "bne-ro-field-atlas-query-result/v1.0.0"
    @test component_result["query_schema_version"] ==
        "bne-ro-field-atlas-query/v1.0.0"
    @test component_result["network_space_claim"] == "none"
    @test component_result["status"] ==
        "matches_found_in_evaluated_subset"
    @test only(component_result["matches"])["record_id"] == "heterodimer"
    @test occursin(r"^[0-9a-f]{64}$", component_result["query_sha256"])
    @test occursin(r"^[0-9a-f]{64}$", component_result["result_sha256"])

    family_result = _ROFA_BACKEND.query_ro_field_atlas(
        atlas,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(
            gradient_filters=[_ROFA_BACKEND.ROFieldGradientFilter(
                "AB", "opposed_axis_signs")],
        ),
    )
    @test only(family_result["matches"])["record_id"] == "opposed_fixture"

    pattern_result = _ROFA_BACKEND.query_ro_field_atlas(
        atlas,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(
            required_sign_patterns=["++"],
            required_transition_tokens=["+0<->++"],
        ),
    )
    @test only(pattern_result["matches"])["record_id"] == "heterodimer"
    @test pattern_result["query"]["required_transition_tokens"] ==
        ["++<->+0"]

    mixed_result = _ROFA_BACKEND.query_ro_field_atlas(
        atlas,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(
            min_mixed_sign_facet_count=1),
    )
    @test mixed_result["population"]["matched_count"] == 2

    coupled_result = _ROFA_BACKEND.query_ro_field_atlas(
        atlas,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(min_coupled_jump_count=1),
    )
    @test only(coupled_result["matches"])["record_id"] == "heterodimer"

    no_match = _ROFA_BACKEND.query_ro_field_atlas(
        atlas,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(
            gradient_filters=[_ROFA_BACKEND.ROFieldGradientFilter(
                "AB", "all_zero")]),
    )
    @test no_match["status"] == "no_match_in_declared_demo_corpus"
    @test no_match["scope"] == "declared_explicit_demo_corpus"
    @test isempty(no_match["matches"])

    limited = _ROFA_BACKEND.query_ro_field_atlas(
        atlas, _ROFA_BACKEND.ROFieldAtlasQuerySpec(limit=1))
    @test limited["population"]["matched_count"] == 2
    @test limited["population"]["returned_count"] == 1
    @test limited["population"]["omitted_match_count"] == 1
    @test limited["truncated"]
end

@testset "query and Atlas identities are canonical" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, opposed]; config=_rofa_test_demo_config(base))
    first_filter = _ROFA_BACKEND.ROFieldComponentFilter(
        "AB", "tA", "nonnegative_variable")
    second_filter = _ROFA_BACKEND.ROFieldComponentFilter(
        "AB", "tB", "nonnegative_variable")
    query_a = _ROFA_BACKEND.ROFieldAtlasQuerySpec(
        component_filters=[second_filter, first_filter, first_filter],
        required_sign_patterns=["+0", "++", "+0"],
    )
    query_b = _ROFA_BACKEND.ROFieldAtlasQuerySpec(
        component_filters=[first_filter, second_filter],
        required_sign_patterns=["++", "+0"],
    )
    result_a = _ROFA_BACKEND.query_ro_field_atlas(atlas, query_a)
    result_b = _ROFA_BACKEND.query_ro_field_atlas(atlas, query_b)
    @test result_a["query_sha256"] == result_b["query_sha256"]
    @test result_a["result_sha256"] == result_b["result_sha256"]

    tampered = deepcopy(atlas)
    tampered["records"][1]["signature"]["features"][
        "mixed_sign_facet_count"] = 2
    # Rehashing only the outer Atlas must not bless a stale inner signature.
    tampered["atlas_sha256"] = _ROFA_BACKEND.canonical_hash(
        _ROFA_BACKEND._rofa_atlas_identity_payload(tampered))
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(tampered)
end

@testset "sampled, unsupported, invalid, and ambiguous records are diagnostics" begin
    base, heterodimer, _ = _rofa_test_inputs()
    sampled = _ROFA_BACKEND.ROFieldAtlasInput(
        "sampled";
        representation=:sampled_grid,
        field_sha256=repeat("c", 64),
        # Even an attached complex must not be silently treated as exact.
        complex=base,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    sampled_atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [sampled];
        config=_ROFA_BACKEND.ROFieldAtlasConfig(
            max_fields=1, max_total_cells=1, max_total_facets=1),
    )
    @test sampled_atlas["population"]["submitted_count"] == 1
    @test sampled_atlas["population"]["eligible_count"] == 0
    @test sampled_atlas["population"]["evaluated_count"] == 0
    @test sampled_atlas["population"]["classified_count"] == 0
    @test sampled_atlas["population"]["diagnostic_count"] == 1
    @test sampled_atlas["population"]["omitted_count"] == 0
    @test isempty(sampled_atlas["records"])
    @test only(sampled_atlas["diagnostics"])["code"] ==
        "sampled_grid_not_classified"
    sampled_miss = _ROFA_BACKEND.query_ro_field_atlas(sampled_atlas)
    @test sampled_miss["status"] == "no_match_in_evaluated_subset"

    invalid_hash = _ROFA_BACKEND.ROFieldAtlasInput(
        "invalid_hash";
        representation=:exact_cell_complex,
        field_sha256="NOT-A-HASH",
        complex=base,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    invalid_atlas = _ROFA_BACKEND.build_ro_field_atlas([invalid_hash])
    @test only(invalid_atlas["diagnostics"])["code"] ==
        "invalid_field_sha256"
    @test invalid_atlas["population"]["evaluated_count"] == 0

    gap = ROCellComplex2D(
        base.domain,
        [3],
        ROCell2D[],
        ROFacet2D[],
        ROSingularStratum2D[],
        0,
        0,
        base.domain_area,
        0.0,
        base.domain_area,
        false,
        false,
        1e-9,
    )
    gap_input = _ROFA_BACKEND.ROFieldAtlasInput(
        "gap";
        representation=:exact_cell_complex,
        field_sha256=repeat("d", 64),
        complex=gap,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    gap_atlas = _ROFA_BACKEND.build_ro_field_atlas([gap_input])
    @test gap_atlas["population"]["eligible_count"] == 1
    @test gap_atlas["population"]["evaluated_count"] == 1
    @test gap_atlas["population"]["classified_count"] == 0
    @test gap_atlas["population"]["diagnostic_count"] == 1
    @test only(gap_atlas["diagnostics"])["code"] ==
        "unclassifiable_exact_field"
    @test "positive_area_gap" in only(gap_atlas["diagnostics"])[
        "eligibility_reasons"]

    mixed_population = _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, sampled];
        config=_ROFA_BACKEND.ROFieldAtlasConfig(
            max_fields=2,
            max_total_cells=length(base.cells),
            max_total_facets=length(base.facets),
        ),
    )
    mixed_miss = _ROFA_BACKEND.query_ro_field_atlas(
        mixed_population,
        _ROFA_BACKEND.ROFieldAtlasQuerySpec(
            gradient_filters=[_ROFA_BACKEND.ROFieldGradientFilter(
                "AB", "all_zero")]),
    )
    @test mixed_miss["status"] == "no_match_in_evaluated_subset"
end

@testset "Atlas budgets preflight before callbacks" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    callback_checks = Ref(0)
    callback = () -> (callback_checks[] += 1)

    field_error = try
        _ROFA_BACKEND.build_ro_field_atlas(
            [heterodimer, opposed];
            config=_ROFA_BACKEND.ROFieldAtlasConfig(max_fields=1),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test field_error isa _ROFA_BACKEND.ROFieldAtlasLimitExceeded
    @test field_error.phase === :fields
    @test field_error.requested == 2
    @test callback_checks[] == 0

    cell_error = try
        _ROFA_BACKEND.build_ro_field_atlas(
            [heterodimer, opposed];
            config=_ROFA_BACKEND.ROFieldAtlasConfig(
                max_fields=2,
                max_total_cells=2length(base.cells) - 1,
            ),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test cell_error isa _ROFA_BACKEND.ROFieldAtlasLimitExceeded
    @test cell_error.phase === :total_cells
    @test cell_error.requested == 2length(base.cells)
    @test callback_checks[] == 0

    facet_error = try
        _ROFA_BACKEND.build_ro_field_atlas(
            [heterodimer, opposed];
            config=_ROFA_BACKEND.ROFieldAtlasConfig(
                max_fields=2,
                max_total_cells=2length(base.cells),
                max_total_facets=2length(base.facets) - 1,
            ),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test facet_error isa _ROFA_BACKEND.ROFieldAtlasLimitExceeded
    @test facet_error.phase === :total_facets
    @test facet_error.requested == 2length(base.facets)
    @test callback_checks[] == 0

    per_field_error = try
        _ROFA_BACKEND.build_ro_field_atlas(
            [heterodimer];
            config=_ROFA_BACKEND.ROFieldAtlasConfig(
                signature_config=_ROFA_BACKEND.ROFieldSignatureConfig(
                    max_cells=length(base.cells) - 1),
            ),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test per_field_error isa _ROFA_BACKEND.ROFieldAtlasLimitExceeded
    @test per_field_error.phase === :signature_cells
    @test callback_checks[] == 0

    duplicate = _ROFA_BACKEND.ROFieldAtlasInput(
        "heterodimer";
        representation=:exact_cell_complex,
        field_sha256=repeat("e", 64),
        complex=base,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    @test_throws ArgumentError _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, duplicate];
        config=_rofa_test_demo_config(base),
        cancel_check=callback,
    )
    @test callback_checks[] == 0

    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasConfig(max_fields=9)
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasConfig(
        max_total_cells=513)
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasConfig(
        max_total_facets=1_025)
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasQuerySpec(limit=9)
end

@testset "Atlas build and query expose cooperative cancellation" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    config = _rofa_test_demo_config(base)
    build_checks = Ref(0)
    @test_throws ROFieldAtlasCancelProbe begin
        _ROFA_BACKEND.build_ro_field_atlas(
            [heterodimer, opposed];
            config=config,
            cancel_check=() -> begin
                build_checks[] += 1
                build_checks[] == 1 && throw(ROFieldAtlasCancelProbe())
            end,
        )
    end
    @test build_checks[] == 1

    atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [heterodimer, opposed]; config=config)
    query_checks = Ref(0)
    @test_throws ROFieldAtlasCancelProbe begin
        _ROFA_BACKEND.query_ro_field_atlas(
            atlas,
            _ROFA_BACKEND.ROFieldAtlasQuerySpec(),
            cancel_check=() -> begin
                query_checks[] += 1
                query_checks[] == 2 && throw(ROFieldAtlasCancelProbe())
            end,
        )
    end
    @test query_checks[] == 2
end
