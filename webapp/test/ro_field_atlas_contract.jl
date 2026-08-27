using Test
using JSON3
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

function _rofa_test_artifact(; opposed::Bool=false)
    path = joinpath(@__DIR__, "..", "..", "tests", "fixtures",
        "ro_field", "exact-cell-complex.json")
    artifact = _ROFA_BACKEND._materialize(JSON3.read(read(path, String)))
    artifact["partial"] = false
    artifact["domain"]["axis_order"] = Any["tA", "tB"]
    artifact["domain"]["axes"][1]["axis_id"] = "tA"
    artifact["domain"]["axes"][2]["axis_id"] = "tB"
    artifact["outputs"]["output_order"] = Any["AB"]
    artifact["outputs"]["items"][1]["output_id"] = "AB"
    artifact["component_order"] = Any[
        Dict("output_id" => "AB", "input_axis_id" => "tA"),
        Dict("output_id" => "AB", "input_axis_id" => "tB"),
    ]
    data = artifact["data"]
    data["singular_stratum_order"] = Any[]
    data["singular_strata"] = Any[]
    for facet in data["facets"]
        if facet["kind"] == "singular_boundary"
            facet["kind"] = "regime_transition"
            facet["singular_stratum_ids"] = Any[]
        end
    end
    if opposed
        for cell in data["cells"], label in cell["affine_labels"]
            label["reaction_order_matrix"] = Any[Any[1.0, -1.0]]
            label["output_offset"] = Any[0.0]
        end
    end

    cell_count = length(data["cells"])
    coverage = artifact["coverage"]
    coverage["eligible_count"] = cell_count
    coverage["evaluated_count"] = cell_count
    coverage["valid_count"] = cell_count
    coverage["invalid_count"] = 0
    coverage["omitted_count"] = 0
    coverage["enumeration_complete"] = true
    coverage["truncated"] = false
    coverage["truncation"] = nothing
    coverage["storage"]["complete"] = true
    coverage["storage"]["stored_count"] = cell_count
    data_bytes = _ROFA_BACKEND.canonical_ro_field_data_bytes(artifact)
    coverage["storage"]["payload_bytes"] = length(data_bytes)
    coverage["storage"]["content_sha256"] =
        _ROFA_BACKEND._ro_field_sha256(data_bytes)
    artifact["provenance"]["domain_sha256"] =
        _ROFA_BACKEND.canonical_hash(artifact["domain"])
    artifact["evidence"]["status"] = "complete"
    artifact["evidence"]["completeness_claim"] =
        "complete_over_declared_population"
    return _ROFA_BACKEND.validate_ro_field_document!(artifact)
end

function _rofa_test_inputs()
    heterodimer_artifact = _rofa_test_artifact()
    opposed_artifact = _rofa_test_artifact(opposed=true)
    heterodimer = _ROFA_BACKEND.ROFieldAtlasInput(
        "heterodimer";
        representation=:exact_cell_complex,
        field_artifact=heterodimer_artifact,
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    opposed_input = _ROFA_BACKEND.ROFieldAtlasInput(
        "opposed_fixture";
        representation="exact_cell_complex",
        field_artifact=opposed_artifact,
        axis_ids=[:tA, :tB],
        output_ids=[:AB],
    )
    return heterodimer.complex, heterodimer, opposed_input
end

function _rofa_test_demo_config(base)
    return _ROFA_BACKEND.ROFieldAtlasConfig(
        max_fields=2,
        max_total_cells=2 * length(base.cells),
        max_total_facets=2 * length(base.facets),
    )
end

function _rofa_rehash_atlas!(atlas)
    atlas["atlas_sha256"] = _ROFA_BACKEND.canonical_hash(
        _ROFA_BACKEND._rofa_atlas_identity_payload(atlas))
    return atlas
end

@testset "bounded explicit RO-field demo Atlas" begin
    base, heterodimer, opposed = _rofa_test_inputs()
    config = _rofa_test_demo_config(base)
    atlas = _ROFA_BACKEND.build_ro_field_atlas(
        [opposed, heterodimer]; config=config)

    @test atlas["schema_version"] == "bne-ro-field-atlas/v1.1.0"
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
    wire_atlas = JSON3.read(JSON3.write(atlas))
    @test _ROFA_BACKEND.query_ro_field_atlas(
        wire_atlas, query_a)["result_sha256"] == result_a["result_sha256"]

    duplicate_key = Dict{Any,Any}(pairs(atlas))
    duplicate_key[:schema_version] = atlas["schema_version"]
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        duplicate_key, query_a)

    tampered = deepcopy(atlas)
    tampered["records"][1]["signature"]["features"][
        "mixed_sign_facet_count"] = 2
    # Rehashing only the outer Atlas must not bless a stale inner signature.
    tampered["atlas_sha256"] = _ROFA_BACKEND.canonical_hash(
        _ROFA_BACKEND._rofa_atlas_identity_payload(tampered))
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(tampered)

    forged_hash = deepcopy(atlas)
    forged_hash["records"][1]["field_sha256"] = "f"^64
    forged_hash["records"][1]["signature"]["field_sha256"] = "f"^64
    forged_hash["records"][1]["signature"]["signature_sha256"] =
        _ROFA_BACKEND.canonical_hash(
            _ROFA_BACKEND.ro_field_signature_identity_payload(
                forged_hash["records"][1]["signature"]))
    forged_hash["records"][1]["signature_sha256"] =
        forged_hash["records"][1]["signature"]["signature_sha256"]
    forged_hash["atlas_sha256"] = _ROFA_BACKEND.canonical_hash(
        _ROFA_BACKEND._rofa_atlas_identity_payload(forged_hash))
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(forged_hash)

    reordered_records = deepcopy(atlas)
    reverse!(reordered_records["records"])
    reverse!(reordered_records["record_order"])
    reordered_records["atlas_sha256"] = _ROFA_BACKEND.canonical_hash(
        _ROFA_BACKEND._rofa_atlas_identity_payload(reordered_records))
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        reordered_records)

    background_a = _rofa_test_artifact()
    push!(background_a["domain"]["fixed_background"], Dict{String,Any}(
        "parameter_id" => "aa_extra",
        "symbol" => "Kextra",
        "coordinate_kind" => "binding_constant",
        "reference" => Dict("value" => 1.0, "unit" => "uM"),
        "log_value" => 0.5,
    ))
    background_a["provenance"]["domain_sha256"] =
        _ROFA_BACKEND.canonical_hash(background_a["domain"])
    background_b = deepcopy(background_a)
    reverse!(background_b["domain"]["fixed_background"])
    background_b["provenance"]["domain_sha256"] =
        _ROFA_BACKEND.canonical_hash(background_b["domain"])
    input_a = _ROFA_BACKEND.ROFieldAtlasInput(
        "background_order";
        representation=:exact_cell_complex,
        field_artifact=background_a,
    )
    input_b = _ROFA_BACKEND.ROFieldAtlasInput(
        "background_order";
        representation=:exact_cell_complex,
        field_artifact=background_b,
    )
    @test input_a.field_sha256 == input_b.field_sha256
    @test _ROFA_BACKEND.build_ro_field_atlas([input_a])["atlas_sha256"] ==
        _ROFA_BACKEND.build_ro_field_atlas([input_b])["atlas_sha256"]
end

@testset "v1.1 Atlas trust boundary reconstructs builder-only state" begin
    base, heterodimer, _ = _rofa_test_inputs()

    # The struct's positional constructor must not bypass artifact derivation.
    @test_throws MethodError _ROFA_BACKEND.ROFieldAtlasInput(
        "positional_bypass",
        :exact_cell_complex,
        "f"^64,
        nothing,
        base,
        ["tA", "tB"],
        ["AB"],
    )

    atlas = _ROFA_BACKEND.build_ro_field_atlas([heterodimer])

    # Public source views are detached reconstructions. Mutating any view after
    # construction cannot change the immutable source used by preflight/build.
    heterodimer.field_payload["data"]["cells"] = Any[]
    empty!(heterodimer.complex.cells)
    heterodimer.axis_ids[1] = "mutated_axis"
    heterodimer.output_ids[1] = "mutated_output"
    @test _ROFA_BACKEND.build_ro_field_atlas([heterodimer])[
        "atlas_sha256"] == atlas["atlas_sha256"]

    mismatched_config = deepcopy(atlas)
    wide_config = _ROFA_BACKEND.ROFieldSignatureConfig(zero_tolerance=100.0)
    wide_signature = _ROFA_BACKEND.classify_ro_cell_complex(
        heterodimer.complex,
        heterodimer.field_sha256;
        axis_ids=heterodimer.axis_ids,
        output_ids=heterodimer.output_ids,
        config=wide_config,
    )
    mismatched_config["records"][1]["signature"] = wide_signature
    mismatched_config["records"][1]["signature_sha256"] =
        wide_signature["signature_sha256"]
    _rofa_rehash_atlas!(mismatched_config)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        mismatched_config)

    unsafe_record = deepcopy(atlas)
    unsafe_record["records"][1]["record_id"] = "<invalid>"
    unsafe_record["record_order"][1] = "<invalid>"
    _rofa_rehash_atlas!(unsafe_record)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(unsafe_record)

    false_enumeration = deepcopy(atlas)
    false_enumeration["source_population"]["enumeration_performed"] = true
    _rofa_rehash_atlas!(false_enumeration)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        false_enumeration)

    impossible_population = deepcopy(atlas)
    impossible_population["population"]["submitted_count"] = 2
    impossible_population["population"]["eligible_count"] = 2
    impossible_population["population"]["evaluated_count"] = 2
    _rofa_rehash_atlas!(impossible_population)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        impossible_population)

    inflated_preflight = deepcopy(atlas)
    inflated_preflight["config"]["preflight_total_cells"] += 1
    _rofa_rehash_atlas!(inflated_preflight)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        inflated_preflight)

    for (container, key) in ((deepcopy(atlas), "unexpected_top_level"),)
        container[key] = true
        # Extra top-level data is deliberately not part of the identity helper;
        # exact-key validation must still reject it.
        @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(container)
    end

    extended_source = deepcopy(atlas)
    extended_source["source_population"]["unexpected"] = true
    _rofa_rehash_atlas!(extended_source)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        extended_source)

    sampled = _ROFA_BACKEND.ROFieldAtlasInput(
        "sampled"; representation=:sampled_grid)
    diagnostic_atlas = _ROFA_BACKEND.build_ro_field_atlas([sampled])
    diagnostic_atlas["diagnostics"][1]["record_id"] = "<invalid>"
    _rofa_rehash_atlas!(diagnostic_atlas)
    @test_throws ArgumentError _ROFA_BACKEND.query_ro_field_atlas(
        diagnostic_atlas)
end

@testset "unsupported records are diagnostics and exact sources fail closed" begin
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

    artifact = _rofa_test_artifact()
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "forged_hash";
        representation=:exact_cell_complex,
        field_artifact=artifact,
        field_sha256="f"^64,
    )
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "mismatched_complex";
        representation=:exact_cell_complex,
        field_artifact=artifact,
        complex=_rofa_test_opposed_gradient_variant(base),
    )
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "reversed_axes";
        representation=:exact_cell_complex,
        field_artifact=artifact,
        axis_ids=["tB", "tA"],
    )
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "unsafe_axes";
        representation=:exact_cell_complex,
        field_artifact=artifact,
        axis_ids=["tA", "<unsafe>"],
    )
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "mismatched_outputs";
        representation=:exact_cell_complex,
        field_artifact=artifact,
        output_ids=["different_output"],
    )

    validator_roundoff = deepcopy(artifact)
    validator_roundoff["data"]["cells"][1]["area"] -= 1.0e-9
    roundoff_bytes = _ROFA_BACKEND.canonical_ro_field_data_bytes(
        validator_roundoff)
    validator_roundoff["coverage"]["storage"]["payload_bytes"] =
        length(roundoff_bytes)
    validator_roundoff["coverage"]["storage"]["content_sha256"] =
        _ROFA_BACKEND._ro_field_sha256(roundoff_bytes)
    validated_roundoff = _ROFA_BACKEND.validate_ro_field_document!(
        validator_roundoff)
    roundoff_input = _ROFA_BACKEND.ROFieldAtlasInput(
        "validator_roundoff";
        representation=:exact_cell_complex,
        field_artifact=validated_roundoff,
    )
    roundoff_atlas = _ROFA_BACKEND.build_ro_field_atlas([roundoff_input])
    @test roundoff_atlas["population"]["classified_count"] == 1
    @test _ROFA_BACKEND.query_ro_field_atlas(roundoff_atlas)["status"] ==
        "matches_found_in_evaluated_subset"

    incomplete = deepcopy(artifact)
    incomplete["partial"] = true
    incomplete["coverage"]["eligible_count"] += 1
    incomplete["coverage"]["omitted_count"] = 1
    incomplete["coverage"]["enumeration_complete"] = false
    incomplete["coverage"]["truncated"] = true
    incomplete["coverage"]["truncation"] = Dict{String,Any}(
        "reason" => "work_budget",
        "detail" => "One exact cell-complex item exceeded the fixture budget.",
    )
    incomplete["evidence"]["status"] = "partial"
    incomplete["evidence"]["completeness_claim"] =
        "best_over_evaluated_prefix"
    @test _ROFA_BACKEND.validate_ro_field_document!(deepcopy(incomplete))[
        "partial"] === true
    @test_throws ArgumentError _ROFA_BACKEND.ROFieldAtlasInput(
        "incomplete";
        representation=:exact_cell_complex,
        field_artifact=incomplete,
    )

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
        field_artifact=_rofa_test_artifact(),
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
