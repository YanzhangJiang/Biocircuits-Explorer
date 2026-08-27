using Test
using JSON3
using BindingAndCatalysis
using BiocircuitsExplorerBackend

const _ROFS_BACKEND = BiocircuitsExplorerBackend
if !isdefined(_ROFS_BACKEND, :ROFieldSignatureConfig)
    Base.include(_ROFS_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_behavior.jl"))
end

struct ROFieldSignatureCancelProbe <: Exception end

function _signature_heterodimer_complex(; axes=(1, 2))
    model = Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :B, :AB],
        q_sym=[:tA, :tB],
        K_sym=[:Kd],
    )
    domain = ROInputDomain2D(
        axes, (-2.0, -2.0), (2.0, 2.0), zeros(3))
    return build_ro_cell_complex(model, domain, [3])
end

function _signature_with_labels(complex, output_indices, matrices)
    length(matrices) == length(complex.cells) ||
        throw(DimensionMismatch("one matrix is required per cell"))
    cells = ROCell2D[]
    for (cell, matrix) in zip(complex.cells, matrices)
        label = ROAffineLabel2D(
            only(cell.labels).source_regime_ids,
            Matrix{Float64}(matrix),
            zeros(length(output_indices)),
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
        complex.domain,
        collect(output_indices),
        cells,
        complex.facets,
        complex.singular_strata,
        complex.candidate_regime_count,
        complex.regular_candidate_count,
        complex.domain_area,
        complex.covered_area_sum,
        complex.gap_area,
        complex.coverage_complete,
        false,
        complex.geometry_tolerance,
    )
end

function _signature_component_map(signature)
    return Dict(
        (item["output_id"], item["axis_id"]) => item["classification"]
        for item in signature["component_classifications"])
end

const _ROFS_FIELD_HASH = repeat("a", 64)

@testset "versioned exact 2D regular-cell gradient signature" begin
    complex = _signature_heterodimer_complex()
    signature = _ROFS_BACKEND.classify_ro_cell_complex(
        complex,
        _ROFS_FIELD_HASH;
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )

    @test signature["schema_version"] ==
        "bne-ro-field-signature/v1.0.0"
    @test signature["classifier_version"] ==
        "regular-cell-gradient/v1.0.0"
    @test signature["scope"] ==
        "regular_cell_interiors_excluding_declared_lower_dimensional_strata"
    @test signature["classifiable"]
    @test occursin(r"^[0-9a-f]{64}$", signature["signature_sha256"])
    @test signature["diagnostics"]["eligibility_reasons"] == String[]
    @test signature["diagnostics"][
        "excluded_lower_dimensional_strata_count"] == 1
    normalized = _ROFS_BACKEND.validate_ro_field_signature!(signature)
    @test normalized == signature
    @test _ROFS_BACKEND.validate_ro_field_signature!(
        JSON3.read(JSON3.write(signature))) == signature
    @test _ROFS_BACKEND.canonical_hash(
        _ROFS_BACKEND.ro_field_signature_identity_payload(signature)) ==
        signature["signature_sha256"]

    components = _signature_component_map(signature)
    @test components[("AB", "tA")] == "nonnegative_variable"
    @test components[("AB", "tB")] == "nonnegative_variable"
    @test only(signature["gradient_families"])["gradient_family"] ==
        "all_nonnegative"
    @test signature["features"]["regular_cell_sign_patterns"] ==
        ["++", "+0", "0+"]
    @test signature["features"]["internal_facet_count"] == 3
    @test signature["features"]["mixed_sign_facet_count"] == 1
    @test signature["features"]["coupled_jump_count"] == 1
    @test signature["features"]["internal_facet_transition_tokens"] ==
        ["++<->+0", "++<->0+", "+0<->0+"]

    repeat_signature = _ROFS_BACKEND.classify_ro_cell_complex(
        complex,
        _ROFS_FIELD_HASH;
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )
    @test repeat_signature["signature_sha256"] ==
        signature["signature_sha256"]
    @test _ROFS_BACKEND.classify_ro_cell_complex(
        complex,
        repeat("b", 64);
        axis_ids=["tA", "tB"], output_ids=["AB"],
    )["signature_sha256"] != signature["signature_sha256"]
end

@testset "signature trust boundary rejects structural and hash tampering" begin
    signature = _ROFS_BACKEND.classify_ro_cell_complex(
        _signature_heterodimer_complex(),
        _ROFS_FIELD_HASH;
        axis_ids=["tA", "tB"],
        output_ids=["AB"],
    )

    stale_hash = deepcopy(signature)
    stale_hash["features"]["mixed_sign_facet_count"] = 2
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        stale_hash)

    bad_component = deepcopy(signature)
    bad_component["component_classifications"][1]["classification"] =
        "strictly_positive"
    @test_throws ArgumentError _ROFS_BACKEND.ro_field_signature_identity_payload(
        bad_component)

    wrong_component_order = deepcopy(signature)
    reverse!(wrong_component_order["component_classifications"])
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        wrong_component_order)

    wrong_family = deepcopy(signature)
    wrong_family["gradient_families"][1]["gradient_family"] = "all_zero"
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        wrong_family)

    missing_transition = deepcopy(signature)
    pop!(missing_transition["features"]["internal_facet_transition_tokens"])
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        missing_transition)

    wrong_diagnostic_count = deepcopy(signature)
    wrong_diagnostic_count["diagnostics"]["internal_facet_count"] = 2
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        wrong_diagnostic_count)

    wrong_budget = deepcopy(signature)
    wrong_budget["diagnostics"]["budgeted_matrix_elements"] -= 1
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        wrong_budget)

    # Re-hashing the whole document must not make a contradictory gap claim
    # authoritative.  A classifiable field has complete coverage and exactly
    # zero serialized gap area; positive and negative gaps require their
    # corresponding eligibility reasons.
    for forged_gap in (100.0, -100.0)
        forged = deepcopy(signature)
        forged["diagnostics"]["gap_area"] = forged_gap
        forged["signature_sha256"] = _ROFS_BACKEND.canonical_hash(
            _ROFS_BACKEND._rofb_signature_identity_from_normalized(forged))
        @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
            forged)
    end

    future = deepcopy(signature)
    future["schema_version"] = "bne-ro-field-signature/v2.0.0"
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(future)

    extension = deepcopy(signature)
    extension["unexpected"] = true
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        extension)

    duplicate_key = Dict{Any,Any}(pairs(signature))
    duplicate_key[:schema_version] = signature["schema_version"]
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        duplicate_key)

    malformed_hash = deepcopy(signature)
    malformed_hash["signature_sha256"] = repeat("f", 63)
    @test_throws ArgumentError _ROFS_BACKEND.validate_ro_field_signature!(
        malformed_hash)
end

@testset "axis order is covariant and identity-bearing" begin
    original = _ROFS_BACKEND.classify_ro_cell_complex(
        _signature_heterodimer_complex(axes=(1, 2)),
        _ROFS_FIELD_HASH;
        axis_ids=["tA", "tB"], output_ids=["AB"],
    )
    swapped = _ROFS_BACKEND.classify_ro_cell_complex(
        _signature_heterodimer_complex(axes=(2, 1)),
        _ROFS_FIELD_HASH;
        axis_ids=["tB", "tA"], output_ids=["AB"],
    )
    @test swapped["axis_ids"] == ["tB", "tA"]
    @test swapped["features"]["regular_cell_sign_patterns"] ==
        original["features"]["regular_cell_sign_patterns"]
    @test _signature_component_map(swapped)[("AB", "tB")] ==
        _signature_component_map(original)[("AB", "tA")]
    @test _signature_component_map(swapped)[("AB", "tA")] ==
        _signature_component_map(original)[("AB", "tB")]
    @test swapped["signature_sha256"] != original["signature_sha256"]
end

@testset "multiple outputs and tolerance-boundary semantics" begin
    base = _signature_heterodimer_complex()
    multi_matrices = [
        vcat(only(cell.labels).reaction_order_matrix,
            -only(cell.labels).reaction_order_matrix)
        for cell in base.cells
    ]
    multi = _signature_with_labels(base, [3, 1], multi_matrices)
    signature = _ROFS_BACKEND.classify_ro_cell_complex(
        multi, _ROFS_FIELD_HASH;
        axis_ids=["tA", "tB"], output_ids=["AB", "A_inverse"],
    )
    components = _signature_component_map(signature)
    @test components[("AB", "tA")] == "nonnegative_variable"
    @test components[("AB", "tB")] == "nonnegative_variable"
    @test components[("A_inverse", "tA")] == "nonpositive_variable"
    @test components[("A_inverse", "tB")] == "nonpositive_variable"
    @test getindex.(signature["gradient_families"], "gradient_family") ==
        ["all_nonnegative", "all_nonpositive"]
    @test all(occursin("|", token) for token in
        signature["features"]["regular_cell_sign_patterns"])

    tolerance = 1e-9
    boundary_matrices = Matrix{Float64}[
        [0.5tolerance -0.5tolerance],
        [2tolerance -2tolerance],
        [0.5tolerance -0.5tolerance],
    ]
    boundary = _signature_with_labels(base, [3], boundary_matrices)
    boundary_signature = _ROFS_BACKEND.classify_ro_cell_complex(
        boundary, _ROFS_FIELD_HASH;
        axis_ids=["u", "v"], output_ids=["y"],
        config=_ROFS_BACKEND.ROFieldSignatureConfig(
            zero_tolerance=tolerance),
    )
    boundary_components = _signature_component_map(boundary_signature)
    @test boundary_components[("y", "u")] == "nonnegative_variable"
    @test boundary_components[("y", "v")] == "nonpositive_variable"
    @test only(boundary_signature["gradient_families"])[
        "gradient_family"] == "opposed_axis_signs"
end

@testset "gaps and ambiguity never choose a first label" begin
    domain = ROInputDomain2D(
        (1, 2), (-1.0, -1.0), (1.0, 1.0), zeros(3))
    vertices = [(-1.0, -1.0), (1.0, -1.0),
        (1.0, 1.0), (-1.0, 1.0)]
    label_a = ROAffineLabel2D([1], [1.0 0.0], [0.0])
    label_b = ROAffineLabel2D([2], [0.0 1.0], [0.0])
    ambiguous_cell = ROCell2D(
        1, vertices, 4.0, [1, 2], [label_a, label_b], true)
    ambiguous = ROCellComplex2D(
        domain, [3], [ambiguous_cell], ROFacet2D[],
        ROSingularStratum2D[], 2, 2, 4.0, 4.0, 0.0, true, true, 1e-9)
    ambiguous_signature = _ROFS_BACKEND.classify_ro_cell_complex(
        ambiguous, _ROFS_FIELD_HASH;
        axis_ids=["u", "v"], output_ids=["y"],
    )
    @test !ambiguous_signature["classifiable"]
    @test all(item["classification"] == "unknown" for item in
        ambiguous_signature["component_classifications"])
    @test only(ambiguous_signature["gradient_families"])[
        "gradient_family"] == "unknown"
    @test isempty(ambiguous_signature["features"][
        "regular_cell_sign_patterns"])
    @test "ambiguous_complex" in ambiguous_signature[
        "diagnostics"]["eligibility_reasons"]
    @test "set_valued_cell" in ambiguous_signature[
        "diagnostics"]["eligibility_reasons"]

    gap = ROCellComplex2D(
        domain, [3], ROCell2D[], ROFacet2D[], ROSingularStratum2D[],
        0, 0, 4.0, 0.0, 4.0, false, false, 1e-9)
    gap_signature = _ROFS_BACKEND.classify_ro_cell_complex(
        gap, _ROFS_FIELD_HASH;
        axis_ids=["u", "v"], output_ids=["y"],
    )
    @test !gap_signature["classifiable"]
    @test all(item["classification"] == "unknown" for item in
        gap_signature["component_classifications"])
    @test "positive_area_gap" in gap_signature[
        "diagnostics"]["eligibility_reasons"]

    inconsistent_complete_gap = ROCellComplex2D(
        domain, [3], ROCell2D[], ROFacet2D[], ROSingularStratum2D[],
        0, 0, 4.0, 0.0, 4.0, true, false, 1e-9)
    inconsistent_signature = _ROFS_BACKEND.classify_ro_cell_complex(
        inconsistent_complete_gap, _ROFS_FIELD_HASH;
        axis_ids=["u", "v"], output_ids=["y"],
    )
    @test !inconsistent_signature["diagnostics"]["coverage_complete"]
    @test Set(inconsistent_signature["diagnostics"]["eligibility_reasons"]) ==
        Set(["coverage_incomplete", "positive_area_gap"])
    @test _ROFS_BACKEND.validate_ro_field_signature!(
        inconsistent_signature) == inconsistent_signature

    base = _signature_heterodimer_complex()
    with_gap_and_tolerance(gap_area, geometry_tolerance) = ROCellComplex2D(
        base.domain,
        base.output_indices,
        base.cells,
        base.facets,
        base.singular_strata,
        base.candidate_regime_count,
        base.regular_candidate_count,
        base.domain_area,
        base.covered_area_sum,
        gap_area,
        true,
        false,
        geometry_tolerance,
    )

    # Geometry coverage is an exact evidence claim. Neither the caller-owned
    # engine tolerance nor the reaction-order sign tolerance may wash a gap.
    for forged_gap in (100.0, -100.0)
        @test_throws ArgumentError _ROFS_BACKEND.classify_ro_cell_complex(
            with_gap_and_tolerance(forged_gap, 100.0),
            _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
        )
        strict_gap = _ROFS_BACKEND.classify_ro_cell_complex(
            with_gap_and_tolerance(forged_gap, 1e-9),
            _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
            config=_ROFS_BACKEND.ROFieldSignatureConfig(
                zero_tolerance=100.0),
        )
        expected_reason = forged_gap > 0 ?
            "positive_area_gap" : "negative_gap_area"
        @test !strict_gap["classifiable"]
        @test !strict_gap["diagnostics"]["coverage_complete"]
        @test strict_gap["diagnostics"]["gap_area"] == forged_gap
        @test expected_reason in strict_gap[
            "diagnostics"]["eligibility_reasons"]
        @test "coverage_incomplete" in strict_gap[
            "diagnostics"]["eligibility_reasons"]
    end
    for invalid_tolerance in (Inf, NaN, -1.0, 1.0e-5)
        @test_throws ArgumentError _ROFS_BACKEND.classify_ro_cell_complex(
            with_gap_and_tolerance(0.0, invalid_tolerance),
            _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
        )
    end

    validator_roundoff = _ROFS_BACKEND.classify_ro_cell_complex(
        with_gap_and_tolerance(1.0e-9, 1.0e-9),
        _ROFS_FIELD_HASH;
        axis_ids=["u", "v"], output_ids=["y"],
    )
    @test validator_roundoff["classifiable"]
    @test validator_roundoff["diagnostics"]["coverage_complete"]
    @test validator_roundoff["diagnostics"]["gap_area"] == 0.0
end

@testset "signature budgets preflight before callbacks and supports cancellation" begin
    complex = _signature_heterodimer_complex()
    checks = Ref(0)
    callback = () -> (checks[] += 1)

    cell_error = try
        _ROFS_BACKEND.classify_ro_cell_complex(
            complex, _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
            config=_ROFS_BACKEND.ROFieldSignatureConfig(max_cells=2),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test cell_error isa _ROFS_BACKEND.ROFieldSignatureLimitExceeded
    @test cell_error.phase === :cells
    @test cell_error.requested == 3
    @test checks[] == 0

    facet_error = try
        _ROFS_BACKEND.classify_ro_cell_complex(
            complex, _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
            config=_ROFS_BACKEND.ROFieldSignatureConfig(max_facets=8),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test facet_error isa _ROFS_BACKEND.ROFieldSignatureLimitExceeded
    @test facet_error.phase === :facets
    @test facet_error.requested == 9
    @test checks[] == 0

    matrix_error = try
        _ROFS_BACKEND.classify_ro_cell_complex(
            complex, _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
            config=_ROFS_BACKEND.ROFieldSignatureConfig(
                max_matrix_elements=41),
            cancel_check=callback,
        )
        nothing
    catch err
        err
    end
    @test matrix_error isa _ROFS_BACKEND.ROFieldSignatureLimitExceeded
    @test matrix_error.phase === :matrix_elements
    @test matrix_error.requested == 42
    @test checks[] == 0

    cancel_checks = Ref(0)
    @test_throws ROFieldSignatureCancelProbe begin
        _ROFS_BACKEND.classify_ro_cell_complex(
            complex, _ROFS_FIELD_HASH;
            axis_ids=["u", "v"], output_ids=["y"],
            cancel_check=() -> begin
                cancel_checks[] += 1
                cancel_checks[] == 4 && throw(ROFieldSignatureCancelProbe())
            end,
        )
    end
    @test cancel_checks[] == 4

    @test_throws ArgumentError _ROFS_BACKEND.ROFieldSignatureConfig(
        max_cells=257)
    @test_throws ArgumentError _ROFS_BACKEND.ROFieldSignatureConfig(
        max_facets=513)
    @test_throws ArgumentError _ROFS_BACKEND.ROFieldSignatureConfig(
        max_matrix_elements=1_048_577)
    @test_throws ArgumentError _ROFS_BACKEND.classify_ro_cell_complex(
        complex, "not-a-hash";
        axis_ids=["u", "v"], output_ids=["y"])
end
