module ROFieldDifferentialContractTests

using Test
using HTTP
using JSON3
using BindingAndCatalysis
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend

function _quadratic_ro_field_document(; invalidate_first::Bool=false)
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
    sampled = SampledReactionOrderField(
        [1, 2], [axis_u, axis_v], [1], zeros(2), output, reaction_orders,
        validity, regimes)
    domain = Dict{String,Any}(
        "domain_kind" => "axis_aligned_log_box",
        "coordinate_space" => "dimensionless_log_ratio",
        "log_basis" => "log10",
        "axis_order" => Any["u", "v"],
        "axes" => Any[
            Dict("axis_id" => "u", "symbol" => "u",
                "coordinate_kind" => "conserved_total",
                "orientation" => "increasing_physical_value",
                "reference" => Dict("value" => 1.0, "unit" => "uM"),
                "bounds" => Dict("lower" => -1.0, "upper" => 1.0)),
            Dict("axis_id" => "v", "symbol" => "v",
                "coordinate_kind" => "conserved_total",
                "orientation" => "increasing_physical_value",
                "reference" => Dict("value" => 1.0, "unit" => "uM"),
                "bounds" => Dict("lower" => -2.0, "upper" => 2.0)),
        ],
        "fixed_background" => Any[],
    )
    outputs = Dict{String,Any}(
        "output_order" => Any["z"],
        "items" => Any[Dict(
            "output_id" => "z", "symbol" => "z",
            "observable_kind" => "species_concentration",
            "reference" => Dict("value" => 1.0, "unit" => "uM"))],
    )
    normalized = NormalizedROFieldRequest(
        representation=:sampled_grid,
        network_ir_hash="1"^64,
        domain=domain,
        outputs=outputs,
        component_order=Dict{String,String}[
            Dict("output_id" => "z", "input_axis_id" => "u"),
            Dict("output_id" => "z", "input_axis_id" => "v"),
        ],
        axis_indices=[1, 2],
        axis_coordinates_declared=[axis_u, axis_v],
        axis_coordinates_engine_log10=[axis_u, axis_v],
        output_indices=[1],
        output_reference_log10=[0.0],
        fixed_engine_logqK=zeros(2),
        log_basis=:log10,
        work_budget=Dict{String,Any}(
            "work_unit_kind" => "solver_samples",
            "max_evaluated_items" => 64,
            "max_stored_items" => 64,
            "max_payload_bytes" => 1_048_576,
            "deadline_seconds" => nothing,
        ),
        normalized_configuration=Dict{String,Any}("test" => "quadratic"),
        estimated_payload_bytes=1_024,
    )
    data = Backend._ro_field_serialize_sampled(sampled, normalized)
    valid_count = count(identity, validity)
    return Backend._ro_field_finalize(
        normalized, data, valid_count, 9 - valid_count, 9,
        "sampled_numerical", "quadratic_test_sampler",
        String["Synthetic contract fixture."],
    )["ro_field"]
end

@testset "separate finite-grid differential analysis artifact" begin
    field = _quadratic_ro_field_document()
    analysis = analyze_ro_field_differential(
        field; absolute_tolerance=1e-11, relative_tolerance=1e-11,
        synergy_threshold=1e-10)
    @test analysis["schema_version"] ==
        RO_FIELD_DIFFERENTIAL_ANALYSIS_VERSION
    @test analysis["source_field_sha256"] == ro_field_artifact_sha256(field)
    @test analysis["source_data_sha256"] == ro_field_data_sha256(field)
    @test analysis["integrability"]["status"] ==
        "consistent_on_tested_grid"
    @test analysis["integrability"]["total_face_count"] == 4
    @test analysis["curvature"]["status"] == "complete"
    @test analysis["curvature"]["cell_shape"] == [2, 2]
    @test analysis["curvature"]["gradient_jacobian_shape"] ==
        [2, 2, 1, 2, 2]
    @test analysis["synergy"]["policy"] ==
        "positive_log_cross_curvature"
    @test only(analysis["synergy"]["pair_summaries"])["positive_count"] == 4
    @test analysis["evidence"]["claim_scope"] ==
        "declared_finite_grid_and_tolerances_only"
    @test validate_ro_field_differential_analysis!(analysis) === analysis

    repeated = analyze_ro_field_differential(
        field; absolute_tolerance=1e-11, relative_tolerance=1e-11,
        synergy_threshold=1e-10)
    @test repeated == analysis

    tampered = deepcopy(analysis)
    tampered["integrability"]["violating_face_count"] = 1
    @test_throws ArgumentError validate_ro_field_differential_analysis!(tampered)

    renamed = deepcopy(analysis)
    renamed["analysis_id"] = "ro-differential-forged"
    @test_throws ArgumentError validate_ro_field_differential_analysis!(renamed)
end

@testset "invalid source points remain unknown analysis gaps" begin
    field = _quadratic_ro_field_document(invalidate_first=true)
    analysis = analyze_ro_field_differential(field)
    @test analysis["integrability"]["status"] == "unknown_gap"
    @test analysis["integrability"]["invalid_face_count"] == 1
    @test analysis["curvature"]["status"] == "partial"
    @test analysis["curvature"]["invalid_cell_count"] == 1
    @test only(analysis["synergy"]["pair_summaries"])[
        "unknown_gap_count"] == 1
    @test "unknown_gap" in analysis["synergy"]["classification_values"]

    fabricated_gap_value = deepcopy(analysis)
    fabricated_gap_value["curvature"]["gradient_jacobian_values"][1] = 0.0
    @test_throws ArgumentError validate_ro_field_differential_analysis!(
        fabricated_gap_value)

    fabricated_gap_label = deepcopy(analysis)
    fabricated_gap_label["synergy"]["classification_values"][2] =
        "neutral_under_policy"
    @test_throws ArgumentError validate_ro_field_differential_analysis!(
        fabricated_gap_label)

    @test_throws RODifferentialLimitExceeded analyze_ro_field_differential(
        field; max_faces=3)
end

@testset "v1-only differential-analysis HTTP adapter" begin
    field = _quadratic_ro_field_document()
    body = Dict{String,Any}(
        "schema_version" => RO_FIELD_DIFFERENTIAL_REQUEST_VERSION,
        "ro_field" => field,
        "options" => Dict{String,Any}(
            "absolute_tolerance" => 1e-11,
            "relative_tolerance" => 1e-11,
            "synergy_threshold" => 1e-10,
            "max_faces" => 100,
            "max_face_output_evaluations" => 100,
            "max_cells" => 100,
            "max_corner_visits" => 100,
        ),
    )
    response = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field/differential",
        ["Content-Type" => "application/json"], JSON3.write(body)))
    @test response.status == 200
    decoded = Backend._materialize(JSON3.read(String(response.body)))
    @test Set(keys(decoded)) == Set(["analysis", "artifact"])
    @test decoded["analysis"]["integrability"]["status"] ==
        "consistent_on_tested_grid"
    @test decoded["artifact"]["kind"] == "ro_field_differential_analysis"

    missing_version = deepcopy(body)
    delete!(missing_version, "schema_version")
    @test Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field/differential",
        ["Content-Type" => "application/json"],
        JSON3.write(missing_version))).status == 400

    oversized = deepcopy(body)
    oversized["options"]["max_faces"] = 100_001
    rejected = Backend.router(HTTP.Request(
        "POST", "/api/v1/ro_field/differential",
        ["Content-Type" => "application/json"], JSON3.write(oversized)))
    @test rejected.status == 400
    @test Backend.router(HTTP.Request(
        "POST", "/api/ro_field/differential",
        ["Content-Type" => "application/json"], JSON3.write(body))).status == 404
end

end # module
