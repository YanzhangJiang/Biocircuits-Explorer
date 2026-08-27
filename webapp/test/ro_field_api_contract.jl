module ROFieldAPIContractTests

using Test
using HTTP
using JSON3
using BindingAndCatalysis
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function _ro_field_test_network()
    return network_ir_to_dict(network_ir_from_legacy(
        ["A + B <-> AB"], [1.0];
        label="ro-field-heterodimer",
        input_symbols=["tA", "tB"],
        output_symbols=["AB"],
    ))
end

function _ro_field_competitive_network()
    return network_ir_to_dict(network_ir_from_legacy(
        ["L + A <-> AL", "L + B <-> BL"], [1.0, 1.0];
        label="ro-field-competitive",
        input_symbols=["tA", "tB", "tL"],
        output_symbols=["AL"],
    ))
end

function _ro_field_competitive_domain()
    domain = _ro_field_test_domain(bounds=((-2.0, 2.0), (-2.0, 2.0)))
    domain["fixed_background"] = Any[
        Dict{String,Any}(
            "parameter_id" => "input_l",
            "symbol" => "tL",
            "coordinate_kind" => "conserved_total",
            "reference" => Dict("value" => 1.0, "unit" => "uM"),
            "log_value" => 0.0,
        ),
        Dict{String,Any}(
            "parameter_id" => "kd1",
            "symbol" => "Kd1",
            "coordinate_kind" => "binding_constant",
            "reference" => Dict("value" => 1.0, "unit" => "uM"),
            "log_value" => 0.0,
        ),
        Dict{String,Any}(
            "parameter_id" => "kd2",
            "symbol" => "Kd2",
            "coordinate_kind" => "binding_constant",
            "reference" => Dict("value" => 1.0, "unit" => "uM"),
            "log_value" => 0.0,
        ),
    ]
    return domain
end

function _ro_field_competitive_outputs()
    outputs = _ro_field_test_outputs()
    outputs["output_order"] = Any["output_al"]
    outputs["items"][1]["output_id"] = "output_al"
    outputs["items"][1]["symbol"] = "AL"
    return outputs
end

function _ro_field_test_domain(; axis_references=(1.0, 1.0),
                               kd_reference=1.0,
                               bounds=((-1.0, 1.0), (-1.0, 1.0)),
                               basis="log10")
    return Dict{String,Any}(
        "domain_kind" => "axis_aligned_log_box",
        "coordinate_space" => "dimensionless_log_ratio",
        "log_basis" => basis,
        "axis_order" => Any["input_a", "input_b"],
        "axes" => Any[
            Dict{String,Any}(
                "axis_id" => "input_a",
                "symbol" => "tA",
                "coordinate_kind" => "conserved_total",
                "orientation" => "increasing_physical_value",
                "reference" => Dict("value" => axis_references[1], "unit" => "uM"),
                "bounds" => Dict("lower" => bounds[1][1], "upper" => bounds[1][2]),
            ),
            Dict{String,Any}(
                "axis_id" => "input_b",
                "symbol" => "tB",
                "coordinate_kind" => "conserved_total",
                "orientation" => "increasing_physical_value",
                "reference" => Dict("value" => axis_references[2], "unit" => "uM"),
                "bounds" => Dict("lower" => bounds[2][1], "upper" => bounds[2][2]),
            ),
        ],
        "fixed_background" => Any[
            Dict{String,Any}(
                "parameter_id" => "kd1",
                "symbol" => "Kd1",
                "coordinate_kind" => "binding_constant",
                "reference" => Dict("value" => kd_reference, "unit" => "uM"),
                "log_value" => 0.0,
            ),
        ],
    )
end

function _ro_field_test_outputs(; reference=1.0)
    return Dict{String,Any}(
        "output_order" => Any["output_ab"],
        "items" => Any[
            Dict{String,Any}(
                "output_id" => "output_ab",
                "symbol" => "AB",
                "observable_kind" => "species_concentration",
                "reference" => Dict("value" => reference, "unit" => "uM"),
            ),
        ],
    )
end

function _ro_field_sampled_request(; network=_ro_field_test_network(),
                                   domain=_ro_field_test_domain(),
                                   outputs=_ro_field_test_outputs(),
                                   coordinates=Any[[-1.0, 1.0], [-1.0, 0.0, 1.0]],
                                   payload_bytes=1_048_576,
                                   storage_mode="inline")
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_REQUEST_VERSION,
        "network" => network,
        "representation" => "sampled_grid",
        "domain" => domain,
        "outputs" => outputs,
        "sampling" => Dict{String,Any}(
            "scheme" => "cartesian_product",
            "axis_coordinates" => coordinates,
        ),
        "work_budget" => Dict{String,Any}(
            "work_unit_kind" => "solver_samples",
            "max_evaluated_items" => 64,
            "max_stored_items" => 64,
            "max_payload_bytes" => payload_bytes,
            "deadline_seconds" => nothing,
        ),
        "storage" => Dict{String,Any}("mode" => storage_mode),
    )
end

function _ro_field_exact_request(; network=_ro_field_test_network(),
                                 domain=_ro_field_test_domain(
                                     bounds=((-2.0, 2.0), (-2.0, 2.0))),
                                 outputs=_ro_field_test_outputs())
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_REQUEST_VERSION,
        "network" => network,
        "representation" => "exact_cell_complex",
        "domain" => domain,
        "outputs" => outputs,
        "exact_options" => Dict{String,Any}(
            "geometry_tolerance" => 1e-9,
            "limits" => Dict{String,Any}(
                "max_candidate_regimes" => 16,
                "max_cells" => 16,
                "max_singular_strata" => 16,
                "max_pair_checks" => 1_000,
                "max_facets" => 100,
            ),
        ),
        "work_budget" => Dict{String,Any}(
            "work_unit_kind" => "source_regime_candidates",
            "max_evaluated_items" => 16,
            "max_stored_items" => 32,
            "max_payload_bytes" => 1_048_576,
            "deadline_seconds" => nothing,
        ),
        "storage" => Dict{String,Any}("mode" => "inline"),
    )
end

_ro_field_bundle(request) = Backend.build_model_bundle(
    parse_network_ir(request["network"]))

function _ro_field_response_json(response)
    return Backend._materialize(JSON3.read(String(response.body)))
end

@testset "RO-field request normalization and row-major sampled serialization" begin
    domain = _ro_field_test_domain(
        axis_references=(10.0, 100.0),
        bounds=((-1.0, 0.0), (-2.0, 0.0)),
    )
    outputs = _ro_field_test_outputs(reference=10.0)
    request = _ro_field_sampled_request(
        domain=domain,
        outputs=outputs,
        coordinates=Any[[-1.0, 0.0], [-2.0, -1.0, 0.0]],
    )
    bundle = _ro_field_bundle(request)
    normalized = normalize_ro_field_request(request, bundle)
    @test normalized.axis_indices == [1, 2]
    @test normalized.output_indices == [3]
    @test normalized.axis_coordinates_declared ==
        [[-1.0, 0.0], [-2.0, -1.0, 0.0]]
    @test normalized.axis_coordinates_engine_log10 ==
        [[0.0, 1.0], [0.0, 1.0, 2.0]]
    @test normalized.fixed_engine_logqK == [1.0, 2.0, 0.0]

    bad_session = deepcopy(request)
    delete!(bad_session, "network")
    bad_session["session_id"] = ":unsafe-leading-punctuation"
    @test_throws ArgumentError normalize_ro_field_request(bad_session, bundle)

    output = Array{Float64}(undef, 2, 3, 1)
    reaction_orders = Array{Float64}(undef, 2, 3, 1, 2)
    for i in 1:2, j in 1:3
        output[i, j, 1] = 100i + 10j
        reaction_orders[i, j, 1, 1] = 1000i + 100j + 1
        reaction_orders[i, j, 1, 2] = 1000i + 100j + 2
    end
    validity = trues(2, 3)
    validity[2, 2] = false
    regimes = [1 2 3; 4 0 6]
    sampled = SampledReactionOrderField(
        [1, 2], normalized.axis_coordinates_engine_log10, [3],
        normalized.fixed_engine_logqK, output, reaction_orders, validity, regimes)
    data = Backend._ro_field_serialize_sampled(sampled, normalized)

    # Last axis is fastest: (1,1),(1,2),(1,3),(2,1),(2,2),(2,3), not
    # Julia's column-major order. The invalid (2,2) sample nulls its entire
    # output and RO block instead of exposing the finite backing-array values.
    @test data["output_values"] == Any[
        109.0, 119.0, 129.0, 209.0, nothing, 229.0,
    ]
    @test data["reaction_order_values"] == Any[
        1101.0, 1102.0,
        1201.0, 1202.0,
        1301.0, 1302.0,
        2101.0, 2102.0,
        nothing, nothing,
        2301.0, 2302.0,
    ]
    @test data["regime_ids"] == Any[
        "regime-1", "regime-2", "regime-3",
        "regime-4", nothing, "regime-6",
    ]
    @test data["validity"] == [true, true, true, true, false, true]
end

@testset "bounded sampled RO-field producer and canonical inline identity" begin
    request = _ro_field_sampled_request()
    result = produce_ro_field(request, _ro_field_bundle(request))
    @test Set(keys(result)) == Set(["ro_field", "artifact"])
    field = result["ro_field"]
    @test field["schema_version"] == RO_FIELD_SCHEMA_VERSION
    @test field["representation"] == "sampled_grid"
    @test field["data"]["grid_shape"] == [2, 3]
    @test field["data"]["output_shape"] == [2, 3, 1]
    @test field["data"]["reaction_order_shape"] == [2, 3, 1, 2]
    @test field["data"]["axis_coordinates"] ==
        request["sampling"]["axis_coordinates"]
    @test length(field["data"]["validity"]) == 6
    @test field["coverage"]["evaluated_count"] == 6
    @test field["coverage"]["eligible_count"] == 6
    @test field["coverage"]["omitted_count"] == 0
    @test field["coverage"]["evaluated_count"] ==
        field["coverage"]["valid_count"] + field["coverage"]["invalid_count"]
    @test validate_ro_field_document!(field) == field

    canonical_data = Backend._canonical_json(field["data"])
    @test field["coverage"]["storage"]["payload_bytes"] ==
        ncodeunits(canonical_data)
    @test field["coverage"]["storage"]["content_sha256"] ==
        bytes2hex(Backend.SHA.sha256(canonical_data))
    @test result["artifact"]["kind"] == "ro_field"
    @test result["artifact"]["input_hashes"]["network_ir_hash"] ==
        field["provenance"]["network_ir_sha256"]
    @test endswith(field["provenance"]["created_at"], "Z")
    @test endswith(result["artifact"]["created_at"], "Z")
end

@testset "exact 2D cell-complex fidelity, singular evidence, and reference shift" begin
    request = _ro_field_exact_request()
    result = produce_ro_field(request, _ro_field_bundle(request))
    field = result["ro_field"]
    data = field["data"]
    @test field["representation"] == "exact_cell_complex"
    @test length(data["cells"]) == 3
    @test length(data["facets"]) == 9
    @test length(data["singular_strata"]) == 1
    @test data["cell_order"] == getindex.(data["cells"], "cell_id")
    @test data["facet_order"] == getindex.(data["facets"], "facet_id")
    @test data["singular_stratum_order"] ==
        getindex.(data["singular_strata"], "stratum_id")
    @test all(cell -> haskey(cell, "vertices") && haskey(cell, "area") &&
                     haskey(cell, "source_regime_ids") &&
                     haskey(cell, "affine_labels") &&
                     haskey(cell, "set_valued"), data["cells"])
    @test all(facet -> facet["kind"] in
        ("domain_boundary", "regime_transition", "singular_boundary"),
        data["facets"])
    @test field["partial"]
    @test field["coverage"]["invalid_count"] == 1
    @test field["evidence"]["status"] == "partial"
    @test field["evidence"]["completeness_claim"] == "no_positive_claim"
    @test validate_ro_field_document!(field) == field

    shifted_request = _ro_field_exact_request(
        domain=_ro_field_test_domain(
            axis_references=(10.0, 100.0),
            bounds=((-2.0, 2.0), (-3.0, 1.0))),
        outputs=_ro_field_test_outputs(reference=10.0),
    )
    shifted = produce_ro_field(
        shifted_request, _ro_field_bundle(shifted_request))["ro_field"]
    mixed_label = only([
        label for cell in shifted["data"]["cells"]
        for label in cell["affine_labels"]
        if label["reaction_order_matrix"] == Any[Any[1.0, 1.0]]
    ])
    @test mixed_label["output_offset"] == [2.0]
    stratum = only(shifted["data"]["singular_strata"])
    @test all(point -> isapprox(point[1] - point[2], 1.0; atol=1e-8),
        stratum["vertices"])
    singular_facet = only(filter(facet ->
        facet["kind"] == "singular_boundary", shifted["data"]["facets"]))
    @test all(point -> isapprox(
        sum(singular_facet["normal"] .* point) + singular_facet["offset"],
        0.0; atol=1e-8), singular_facet["endpoints"])
    @test isapprox(sum(cell["area"] for cell in shifted["data"]["cells"]),
        16.0; atol=1e-8)
end

@testset "competitive count populations remain separated after cell deduplication" begin
    request = _ro_field_exact_request(
        network=_ro_field_competitive_network(),
        domain=_ro_field_competitive_domain(),
        outputs=_ro_field_competitive_outputs(),
    )
    request["exact_options"]["limits"]["max_candidate_regimes"] = 32
    request["work_budget"]["max_evaluated_items"] = 32
    field = produce_ro_field(request, _ro_field_bundle(request))["ro_field"]
    data = field["data"]
    coverage = field["coverage"]
    complex_items = length(data["cells"]) + length(data["singular_strata"]) +
        length(data["gaps"])
    @test data["source_candidate_regime_count"] == 12
    @test data["regular_candidate_regime_count"] == 6
    @test data["source_candidate_regime_count"] > complex_items
    @test coverage["population_kind"] == "cell_complex_items"
    @test coverage["evaluated_count"] == complex_items
    @test coverage["valid_count"] ==
        count(cell -> cell["set_valued"] === false, data["cells"])
    @test coverage["invalid_count"] ==
        count(cell -> cell["set_valued"] === true, data["cells"]) +
        length(data["singular_strata"]) + length(data["gaps"])
end

@testset "document and RPB2 payload semantics reject fabricated exact evidence" begin
    geometry_error = try
        Backend._ro_field_require_complete_exact_geometry!((
            coverage_complete=false,
            gap_area=0.5,
        ))
        nothing
    catch err
        err
    end
    @test geometry_error isa ROFieldRequestError
    @test geometry_error.code == "ro_field_exact_geometry_incomplete"
    @test geometry_error.computed
    @test !geometry_error.stored

    sampled_request = _ro_field_sampled_request()
    sampled = produce_ro_field(
        sampled_request, _ro_field_bundle(sampled_request))["ro_field"]
    stale_domain = deepcopy(sampled)
    stale_domain["domain"]["axes"][1]["bounds"]["lower"] -= 0.25
    @test_throws ArgumentError validate_ro_field_document!(stale_domain)

    bad_storage = deepcopy(sampled)
    bad_storage["coverage"]["storage"]["stored_count"] = 0
    @test_throws ArgumentError validate_ro_field_document!(bad_storage)

    bad_partial = deepcopy(sampled)
    bad_partial["partial"] = true
    bad_partial["evidence"]["status"] = "unknown"
    bad_partial["evidence"]["completeness_claim"] = "no_positive_claim"
    @test_throws ArgumentError validate_ro_field_document!(bad_partial)

    request = _ro_field_exact_request(
        network=_ro_field_competitive_network(),
        domain=_ro_field_competitive_domain(),
        outputs=_ro_field_competitive_outputs(),
    )
    request["exact_options"]["limits"]["max_candidate_regimes"] = 32
    request["work_budget"]["max_evaluated_items"] = 32
    exact = produce_ro_field(request, _ro_field_bundle(request))["ro_field"]

    bad_area = deepcopy(exact)
    bad_area["data"]["cells"][1]["area"] = 999.0
    @test_throws ArgumentError validate_ro_field_document!(bad_area)

    extra_key = deepcopy(exact)
    extra_key["data"]["future_geometry"] = true
    @test_throws ArgumentError validate_ro_field_document!(extra_key)

    payload = Dict{String,Any}(
        key => deepcopy(exact[key])
        for key in ("schema_version", "domain", "outputs", "component_order", "data")
    )
    @test validate_ro_field_payload!(payload) == payload

    no_facets = deepcopy(payload)
    empty!(no_facets["data"]["facets"])
    empty!(no_facets["data"]["facet_order"])
    @test_throws ArgumentError validate_ro_field_payload!(no_facets)

    missing_facet = deepcopy(payload)
    pop!(missing_facet["data"]["facets"])
    pop!(missing_facet["data"]["facet_order"])
    @test_throws ArgumentError validate_ro_field_payload!(missing_facet)

    overlapping_facet = deepcopy(payload)
    duplicate_facet = deepcopy(first(overlapping_facet["data"]["facets"]))
    duplicate_facet["facet_id"] *= "-duplicate"
    push!(overlapping_facet["data"]["facets"], duplicate_facet)
    push!(overlapping_facet["data"]["facet_order"], duplicate_facet["facet_id"])
    @test_throws ArgumentError validate_ro_field_payload!(overlapping_facet)

    omitted_singular = deepcopy(payload)
    singular_facet = first(filter(facet -> facet["kind"] == "singular_boundary",
        omitted_singular["data"]["facets"]))
    singular_facet["kind"] = "regime_transition"
    empty!(singular_facet["singular_stratum_ids"])
    @test_throws ArgumentError validate_ro_field_payload!(omitted_singular)

    # A canonical cell edge may be represented by adjoining facet segments.
    # The split point is intentionally not a polygon vertex, as happens on the
    # long side of a regular T-junction.
    split_facets = deepcopy(payload)
    split_index = findfirst(facet -> facet["kind"] == "regime_transition",
        split_facets["data"]["facets"])
    @test split_index !== nothing
    original_facet = split_facets["data"]["facets"][split_index]
    left_facet = deepcopy(original_facet)
    right_facet = deepcopy(original_facet)
    midpoint = Any[
        (original_facet["endpoints"][1][coordinate] +
         original_facet["endpoints"][2][coordinate]) / 2
        for coordinate in 1:2
    ]
    left_facet["facet_id"] *= "-a"
    right_facet["facet_id"] *= "-b"
    left_facet["endpoints"] = Any[original_facet["endpoints"][1], midpoint]
    right_facet["endpoints"] = Any[midpoint, original_facet["endpoints"][2]]
    delete!(left_facet, "polyhedron")
    delete!(right_facet, "polyhedron")
    splice!(split_facets["data"]["facets"], split_index,
        Any[left_facet, right_facet])
    split_facets["data"]["facet_order"] =
        getindex.(split_facets["data"]["facets"], "facet_id")
    @test validate_ro_field_payload!(split_facets) == split_facets

    discontinuous = deepcopy(payload)
    internal_facet = first(filter(facet -> facet["kind"] != "domain_boundary" &&
        all(cell_id -> !only(filter(cell -> cell["cell_id"] == cell_id,
            discontinuous["data"]["cells"]))["set_valued"],
            facet["incident_cell_ids"]), discontinuous["data"]["facets"]))
    discontinuous_cell_id = first(internal_facet["incident_cell_ids"])
    discontinuous_cell = only(filter(cell ->
        cell["cell_id"] == discontinuous_cell_id,
        discontinuous["data"]["cells"]))
    discontinuous_cell["affine_labels"][1]["output_offset"][1] += 0.25
    @test_throws ArgumentError validate_ro_field_payload!(discontinuous)

    incomplete = deepcopy(payload)
    only_cell = first(incomplete["data"]["cells"])
    incomplete["data"]["cells"] = Any[only_cell]
    incomplete["data"]["cell_order"] = Any[only_cell["cell_id"]]
    incomplete["data"]["regular_candidate_regime_count"] =
        length(only_cell["source_regime_ids"])
    incomplete["data"]["facets"] = Any[]
    incomplete["data"]["facet_order"] = Any[]
    incomplete["data"]["singular_strata"] = Any[]
    incomplete["data"]["singular_stratum_order"] = Any[]
    @test_throws ArgumentError validate_ro_field_payload!(incomplete)

    outside = deepcopy(payload)
    outside["data"]["cells"][1]["vertices"][1][1] = 99.0
    @test_throws ArgumentError validate_ro_field_payload!(outside)

    overlap = deepcopy(payload)
    duplicate = deepcopy(first(overlap["data"]["cells"]))
    duplicate["cell_id"] = "cell-overlap"
    duplicate["source_regime_ids"] = Any["regime-overlap"]
    duplicate["affine_labels"] = Any[first(duplicate["affine_labels"])]
    duplicate["affine_labels"][1]["label_id"] = "label-overlap"
    duplicate["affine_labels"][1]["source_regime_ids"] = Any["regime-overlap"]
    duplicate["label_order"] = Any["label-overlap"]
    push!(overlap["data"]["cells"], duplicate)
    push!(overlap["data"]["cell_order"], "cell-overlap")
    overlap["data"]["regular_candidate_regime_count"] += 1
    overlap["data"]["source_candidate_regime_count"] += 1
    @test_throws ArgumentError validate_ro_field_payload!(overlap)

    concave = deepcopy(payload)
    cell = concave["data"]["cells"][argmax(length.(
        getindex.(concave["data"]["cells"], "vertices")))]
    vertices = cell["vertices"]
    centroid = Any[
        sum(point[coordinate] for point in vertices) / length(vertices)
        for coordinate in 1:2
    ]
    insert!(vertices, 2, centroid)
    cell["area"] = Backend._ro_field_polygon_signed_area(vertices)
    @test_throws ArgumentError validate_ro_field_payload!(concave)
end

@testset "exact request defaults stay inside the public hard caps" begin
    request = _ro_field_exact_request()
    delete!(request, "exact_options")
    normalized = normalize_ro_field_request(request, _ro_field_bundle(request))
    @test normalized.geometry_tolerance == 1e-9
    @test normalized.exact_limits.max_candidate_regimes == 16
    # The declared max_evaluated/max_stored budgets tighten the schema-safe
    # implementation defaults without changing their public hard ceilings.
    @test normalized.exact_limits.max_cells == 32
    @test normalized.exact_limits.max_singular_strata == 32
    @test normalized.exact_limits.max_pair_checks == 8_192
    @test normalized.exact_limits.max_facets == 512
    @test normalized.estimated_payload_bytes <=
        request["work_budget"]["max_payload_bytes"]
    @test produce_ro_field(request, _ro_field_bundle(request))["ro_field"][
        "representation"] == "exact_cell_complex"
end

@testset "v1-only route, model locking, heavy admission, and structured 422" begin
    route = Backend._match_api_route("/api/ro_field")
    @test route !== nothing
    @test route.canonical_path == "/api/v1/ro_field"
    @test route.legacy_alias === nothing
    @test :handle_ro_field in Backend.SYNC_HEAVY_HANDLER_NAMES
    @test :handle_ro_field in Backend.MODEL_BUNDLE_HANDLER_NAMES
    @test Backend.router(HTTP.Request(
        "POST", "/api/ro_field", [], JSON3.write(Dict()))).status == 404

    request = _ro_field_sampled_request()
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(request)))
    @test response.status == 200
    payload = _ro_field_response_json(response)
    @test Set(keys(payload)) == Set(["ro_field", "artifact"])

    duplicate_source = deepcopy(request)
    duplicate_source["network_ir_hash"] = network_ir_hash(
        parse_network_ir(request["network"]))
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(duplicate_source)))
    @test response.status == 400

    hash_request = _ro_field_sampled_request()
    hash = network_ir_hash(parse_network_ir(hash_request["network"]))
    delete!(hash_request, "network")
    hash_request["network_ir_hash"] = uppercase(hash)
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(hash_request)))
    @test response.status == 400

    non_inline = _ro_field_sampled_request(storage_mode="chunked")
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(non_inline)))
    @test response.status == 422
    error_payload = _ro_field_response_json(response)
    @test error_payload["code"] == "ro_field_storage_mode_unsupported"
    @test error_payload["computed"] == false
    @test error_payload["stored"] == false
    @test error_payload["scientific_infeasibility"] == false
    @test occursin("No RO-field computation was started", error_payload["error"])

    unsupported_axis = _ro_field_sampled_request()
    unsupported_axis["domain"]["axes"][1]["coordinate_kind"] = "external_control"
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(unsupported_axis)))
    @test response.status == 422
    @test _ro_field_response_json(response)["code"] ==
        "ro_field_coordinate_kind_unsupported"

    unsupported_output = _ro_field_sampled_request()
    unsupported_output["outputs"]["items"][1]["observable_kind"] =
        "positive_linear_observable"
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(unsupported_output)))
    @test response.status == 422
    @test _ro_field_response_json(response)["code"] ==
        "ro_field_observable_kind_unsupported"

    tiny_payload = _ro_field_sampled_request(payload_bytes=128)
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(tiny_payload)))
    @test response.status == 422
    error_payload = _ro_field_response_json(response)
    @test error_payload["code"] == "ro_field_payload_budget_exceeded"
    @test error_payload["computed"] == false
    @test error_payload["stored"] == false

    missing_background = _ro_field_sampled_request()
    empty!(missing_background["domain"]["fixed_background"])
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(missing_background)))
    @test response.status == 400

    oversized_limits = _ro_field_exact_request()
    oversized_limits["exact_options"]["limits"]["max_cells"] =
        Backend.MAX_SYNC_RO_FIELD_EXACT_CELLS + 1
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(oversized_limits)))
    @test response.status == 422
    @test _ro_field_response_json(response)["code"] ==
        "ro_field_work_budget_exceeded"

    loose_tolerance = _ro_field_exact_request(
        domain=_ro_field_test_domain(
            bounds=((0.0, 1e-6), (0.0, 1e-6))),
    )
    loose_tolerance["exact_options"]["geometry_tolerance"] = 1.0
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field", [], JSON3.write(loose_tolerance)))
    @test response.status == 422
    error_payload = _ro_field_response_json(response)
    @test error_payload["code"] == "ro_field_geometry_tolerance_unsupported"
    @test error_payload["computed"] == false
    @test error_payload["stored"] == false
    @test error_payload["scientific_infeasibility"] == false
end

@testset "source revision provenance is explicit and never inferred" begin
    revision_key = "BIOCIRCUITS_EXPLORER_REVISION"
    dirty_key = "BIOCIRCUITS_EXPLORER_SOURCE_DIRTY"
    previous_revision = get(ENV, revision_key, nothing)
    previous_dirty = get(ENV, dirty_key, nothing)
    try
        ENV[revision_key] = "a"^40
        delete!(ENV, dirty_key)
        @test Backend._ro_field_revision_provenance() ==
            ("unknown", nothing, nothing)
        ENV[dirty_key] = "false"
        @test Backend._ro_field_revision_provenance() ==
            ("known", "a"^40, false)
    finally
        previous_revision === nothing ? delete!(ENV, revision_key) :
            (ENV[revision_key] = previous_revision)
        previous_dirty === nothing ? delete!(ENV, dirty_key) :
            (ENV[dirty_key] = previous_dirty)
    end
end

end # module
