using Test
using JSON3
using HTTP
using BiocircuitsExplorerBackend

function _rop_shape_cat_request(; intent_kind="separate", max_cells=256,
                                require_exhaustive=false, sample_points=281)
    fixture_path = normpath(joinpath(
        @__DIR__, "..", "..", "benchmarks", "rop_shape_control",
        "cat_fixed_topology.json"))
    fixture = JSON3.read(read(fixture_path, String))
    rules = String.(collect(fixture.network.rules))
    kd = Float64.(collect(fixture.reference.kd))
    input_sym = String(fixture.network.input)
    output_sym = String(fixture.network.output)
    network = network_ir_from_legacy(
        rules, kd; label="cat-fixed-topology-v1",
        input_symbols=[input_sym], output_symbols=[output_sym])
    network_dict = network_ir_to_dict(network)
    network_hash = network_ir_hash(network)
    reference = Dict{String, Any}(
        "network_ir_hash" => network_hash,
        "operating_points_log10" =>
            Float64.(collect(fixture.reference.operating_points_log10)),
        "kd" => kd,
        "totals" => Dict(String(key) => Float64(value)
                         for (key, value) in pairs(fixture.reference.totals)),
        "path_identity" => "path:$(Int(fixture.reference.path_idx))",
    )
    reference["reference_hash"] = BiocircuitsExplorerBackend._canonical_hash(
        BiocircuitsExplorerBackend._rop_shape_reference_payload(reference))
    spec = Dict{String, Any}(
        "schema_version" => DESIGNABILITY_SPEC_VERSION,
        "source" => Dict("kind" => "test_fixture"),
        "target" => Dict("behavior_spec" => Dict(
            "input" => input_sym,
            "output" => output_sym,
            "feature_space" => "reaction_order",
            "program" => [Dict{String, Any}(
                "kind" => "reaction_order",
                "value" => Float64(value),
                "operator" => "=",
                "hard" => true,
            ) for value in fixture.target.reaction_order_program],
            "input_window" => Dict(
                "input_log10" =>
                    Float64.(collect(fixture.target.input_window_log10)),
                "min_spacing_decades" =>
                    Float64(fixture.target.min_spacing_decades),
                "hard" => true,
            ),
        )),
        "constraints" => Dict(
            "parameter_bounds" => Dict(
                "basis" => "log10_qK",
                "by_class" => Dict(
                    "kd" => Float64.(collect(fixture.parameter_bounds.by_class.kd)),
                    "total" => Float64.(collect(fixture.parameter_bounds.by_class.total)),
                ),
            ),
            "transitions" => Dict(
                "min_spacing_decades" =>
                    Float64(fixture.target.min_spacing_decades),
                "hard" => true,
            ),
        ),
        "candidate_budget" => Dict{String, Any}(),
        "ranking_policy" => Dict("verified_only" => true),
        "audit_policy" => Dict(
            "unsupported" => "block_if_hard",
            "path_format" => "json_pointer",
            "include_supported" => true,
        ),
    )
    intent = if intent_kind == "separate"
        Dict{String, Any}(
            "id" => "separate_ear_tops",
            "kind" => "separate",
            "steps" => [1, 5],
            "preserve_midpoint_tolerance_log10" => 0.2,
        )
    elseif intent_kind == "broaden"
        Dict{String, Any}(
            "id" => "broaden_both_ears",
            "kind" => "broaden",
            "left_span_steps" => [0, 2],
            "right_span_steps" => [4, 6],
            "shared_magnitude" => true,
        )
    elseif intent_kind == "widen_center"
        Dict{String, Any}(
            "id" => "widen_center",
            "kind" => "widen_center",
            "steps" => [2, 4],
            "anchor_step" => 3,
            "anchor_tolerance_log10" => 0.2,
        )
    else
        Dict{String, Any}(
            "id" => "translate_right_ear",
            "kind" => "translate_group",
            "group_steps" => [4, 5, 6],
            "preserve_steps" => [0, 1, 2, 3],
            "preserve_tolerance_log10" => 0.1,
            "sense" => "positive",
            "shared_shift" => true,
        )
    end
    return Dict{String, Any}(
        "schema_version" => ROP_SHAPE_OPTIMIZE_REQUEST_VERSION,
        "network" => network_dict,
        "expected_network_ir_hash" => network_hash,
        "designability_spec" => spec,
        "reference" => reference,
        "edit_intent" => intent,
        "optimization" => Dict(
            "minimum_parameter_margin" => 0.01,
            "effect_tolerance" => 0.02,
        ),
        "work_budget" => Dict(
            "max_paths" => 2000,
            "max_cells" => max_cells,
            "max_replays" => 1,
            "require_exhaustive" => require_exhaustive,
        ),
        "replay" => Dict(
            "input_window_log10" => [-7.0, 7.0],
            "sample_points" => sample_points,
            "require_complete" => true,
            "store_curve" => true,
            "metrics" => [Dict(
                "kind" => "two_peak",
                "min_prominence_log10" => 0.5,
            )],
        ),
    )
end

function _compile_rop_shape_normalized(normalized; kwargs...)
    return BiocircuitsExplorerBackend._rop_shape_compile_population(
        normalized.rules,
        normalized.input_sym,
        normalized.output_sym,
        normalized.target_program,
        normalized.parameter_bounds,
        normalized.input_window,
        normalized.transition_order,
        Int(normalized.work_budget["max_paths"]),
        Int(normalized.work_budget["max_cells"]),
        normalized.network_hash;
        kwargs...,
    )
end

@testset "fixed-topology ROP shape optimization API" begin
    # Production Design Screen adapter: the joint rectangle has augmented
    # radius 0.5, while conditioning tau=0.5 leaves a parameter radius of 50.
    conditional = BiocircuitsExplorerBackend._designability_conditional_parameter_margin(
        zeros(Float64, 0, 2), Float64[],
        [-1.0 0.0; 1.0 0.0; 0.0 -1.0; 0.0 1.0],
        [100.0, 0.0, 1.0, 0.0],
        [:theta], [0.5],
    )
    @test conditional !== nothing
    @test conditional.parameter_radius ≈ 50.0 atol=1e-5
    @test conditional.subspace.dimension == 1

    invalid_points = BiocircuitsExplorerBackend.feasible_region_reaction_order_program(
        ["A + B <-> C_A_B"], "tA", "C_A_B", [0.0, 1.0],
        Dict("basis" => "log10_qK", "by_class" =>
            Dict("kd" => [-3.0, 3.0], "total" => [-3.0, 3.0]));
        input_window=Dict(
            "input_log10" => [-1.0, 1.0],
            "min_spacing_decades" => 0.1,
            "operating_points_log10" => [-0.5, 2.0],
        ),
    )
    @test invalid_points.feasible === false
    @test occursin("invalid", invalid_points.reason)

    request = _rop_shape_cat_request()
    response = BiocircuitsExplorerBackend.router(HTTP.Request(
        "POST", "/api/v1/rop_shape_optimize",
        ["Content-Type" => "application/json"], JSON3.write(request)))
    @test response.status == 200
    result = JSON3.read(String(response.body), Dict{String, Any})

    @test result["schema_version"] == ROP_SHAPE_OPTIMIZATION_VERSION
    @test result["geometric_status"] == "global_optimal_over_declared_cells"
    @test result["feasible"] === true
    @test result["coverage"]["eligible_path_count"] > 0
    @test result["coverage"]["eligible_cell_count"] ==
          result["coverage"]["evaluated_cell_count"]
    @test result["coverage"]["truncated"] === false
    @test result["selected"] !== nothing
    @test result["selected"]["parameter_margin"]["value"] >= 0.01 - 1e-7
    @test result["selected"]["parameter_margin"]["basis"] ==
          "equality_feasible_log10_qK_subspace"
    @test result["selected"]["primary_effect"]["closure_support_improvement"] >=
          result["selected"]["primary_effect"]["selected_improvement"] - 1e-7
    @test result["selected"]["primary_effect"]["selected_improvement"] > 0
    @test startswith(result["selected"]["cell_id"], "sha256:")
    @test !isempty(result["selected"]["active_constraints"])
    @test haskey(result, "directional_request_interval")
    @test result["directional_request_interval"]["normalization"] == "not_normalized"
    @test result["replay"]["status"] in ("pass", "failed", "partial")
    @test result["replay"]["request"]["endpoint"] == "/api/v1/placer_curve"
    @test result["replay"]["request"]["method"] == "POST"
    @test result["replay"]["curve"]["partial"] === false
    @test all(value -> value === true, result["replay"]["curve"]["valid"])
    @test result["artifact"]["kind"] == "rop_shape_optimize"
    @test result["fixed_topology"]["topology_preserved"] === true

    # The declared max_cells policy is applied only after a finite regime/path
    # population has been materialized under independent hard bounds.
    one_cell = BiocircuitsExplorerBackend._rop_shape_normalize_request(
        _rop_shape_cat_request(max_cells=1); synchronous=false)
    one_cell_population = _compile_rop_shape_normalized(one_cell)
    @test one_cell_population.evaluated_cell_count == 1
    @test one_cell_population.truncated === true
    @test "max_cells" in one_cell_population.truncation_reasons

    path_bound_error = try
        BiocircuitsExplorerBackend._hard_bounded_siso_paths(
            one_cell_population.model,
            Symbol(one_cell.input_sym);
            label="ROP shape job",
            max_paths=1,
            max_total_nodes=
                BiocircuitsExplorerBackend.ROP_SHAPE_MAX_MATERIALIZED_PATH_NODES,
        )
        nothing
    catch err
        err
    end
    @test path_bound_error isa ArgumentError
    @test occursin("materialization hard bound",
                   sprint(showerror, path_bound_error))
    @test occursin("does not establish scientific infeasibility",
                   sprint(showerror, path_bound_error))

    # Candidate preflight rejects before exact regime construction. This is a
    # resource boundary, not an infeasibility conclusion.
    preflight_error = try
        _compile_rop_shape_normalized(one_cell; regime_candidate_max=1)
        nothing
    catch err
        err
    end
    @test preflight_error isa BiocircuitsExplorerBackend.ModelCandidateBoundExceeded
    @test occursin("enumeration was not started", sprint(showerror, preflight_error))
    @test occursin("does not establish scientific infeasibility",
                   sprint(showerror, preflight_error))

    # Design Screen emits a self-contained fixed-topology template but leaves
    # the typed edit unfilled; filling only that slot produces a valid request.
    selected = result["selected"]
    fixed_network = parse_network_ir(request["network"])
    nid = BiocircuitsExplorerBackend.network_canonical_code(fixed_network)
    @test nid !== nothing
    best = BiocircuitsExplorerBackend.DesignabilityCellResult(
        vertex_idx=first(selected["witness_vertex_indices"]),
        predicted_ro=first(selected["predicted_profile"]),
        path_idx=selected["path_idx"],
        vertex_indices=Int.(selected["full_path_vertex_indices"]),
        witness_vertex_indices=Int.(selected["witness_vertex_indices"]),
        predicted_profile=Float64.(selected["predicted_profile"]),
        qK_symbols=String.(selected["background_log_qK"]["symbols"]),
        witness_input_log10=Float64.(selected["witness_input_log10"]),
        chebyshev_radius=Float64(selected["parameter_margin"]["value"]),
        parameter_chebyshev_radius=Float64(selected["parameter_margin"]["value"]),
        augmented_chebyshev_radius=0.0,
        parameter_margin_dimension=Int(selected["parameter_margin"]["dimension"]),
        parameter_margin_equality_rank=Int(selected["parameter_margin"]["equality_rank"]),
        parameter_margin_basis="equality_feasible_log10_qK_subspace",
        log_qK=Float64.(selected["background_log_qK"]["values"]),
        kd=Float64.(selected["kd"]),
        totals=Dict{String, Float64}(String(key) => Float64(value)
                                     for (key, value) in pairs(selected["totals"])),
        solve_mode="conditional_parameter_chebyshev",
    )
    card = Dict{String, Any}(
        "nid" => String(nid),
        "inp" => result["fixed_topology"]["input"],
        "out" => result["fixed_topology"]["output"],
    )
    screen_spec = deepcopy(request["designability_spec"])
    screen_spec["constraints"]["robustness"] =
        Dict("min_chebyshev_radius" => 0.005, "hard" => true)
    normalized_spec = normalize_designability_spec(screen_spec)
    card["pass"] = true
    card["screen_status"] = "verified_exact"
    card["evidence_grade"] = "enforced_exact"
    card["certificate_grade"] = "exact-window-siso-rop-path"
    screen_card_fields = deepcopy(card)
    BiocircuitsExplorerBackend._design_attach_shape_optimization_handoff!(
        card, best, normalized_spec)
    for (key, value) in screen_card_fields
        @test card[key] == value
    end
    @test card["optimization_handoff_template"]["endpoint"] ==
          "/api/v1/rop_shape_optimize"
    @test card["optimization_handoff_template"]["body_template"]["edit_intent"] === nothing
    @test normalized_spec.constraints["robustness"]["min_chebyshev_radius"] == 0.005
    handoff_body = card["optimization_handoff_template"]["body_template"]
    handoff_constraints = handoff_body["designability_spec"]["constraints"]
    @test !haskey(handoff_constraints, "robustness")
    @test handoff_body["optimization"]["minimum_parameter_margin"] == 0.005
    filled_template = deepcopy(handoff_body)
    filled_template["edit_intent"] = request["edit_intent"]
    normalized_template = BiocircuitsExplorerBackend._rop_shape_normalize_request(
        filled_template; synchronous=false)
    @test normalized_template.network_hash ==
          card["fixed_topology_reference"]["network_ir_hash"]
    @test normalized_template.minimum_parameter_margin == 0.005

    @test "rop_shape_optimize" in BiocircuitsExplorerBackend.LOCAL_JOB_KINDS
    cancel_calls = Ref(0)
    @test_throws ErrorException BiocircuitsExplorerBackend._dispatch_local_job(
        "rop_shape_optimize", request;
        cancel_check=() -> begin
            cancel_calls[] += 1
            error("cooperative cancellation checkpoint")
        end,
    )
    @test cancel_calls[] == 1

    deep_cancel_calls = Ref(0)
    @test_throws BiocircuitsExplorerBackend.LocalJobCancelled begin
        _compile_rop_shape_normalized(
            one_cell;
            cancel_check=() -> begin
                deep_cancel_calls[] += 1
                deep_cancel_calls[] >= 5 && throw(
                    BiocircuitsExplorerBackend.LocalJobCancelled(
                        "rop-shape-deep-cancel"))
            end,
        )
    end
    @test deep_cancel_calls[] == 5

    broaden = BiocircuitsExplorerBackend._rop_shape_normalize_request(
        _rop_shape_cat_request(intent_kind="broaden"); synchronous=false)
    @test broaden.intent.effect_kind == "balanced_minimum_improvement"
    @test broaden.intent.compiled["auxiliary_coordinates"] == ["alpha"]
    widen = BiocircuitsExplorerBackend._rop_shape_normalize_request(
        _rop_shape_cat_request(intent_kind="widen_center"); synchronous=false)
    @test widen.intent.compiled["intent"]["kind"] == "widen_center"
    translate = BiocircuitsExplorerBackend._rop_shape_normalize_request(
        _rop_shape_cat_request(intent_kind="translate_group"); synchronous=false)
    @test translate.intent.compiled["intent"]["shared_shift"] === true

    wrong_hash = deepcopy(request)
    wrong_hash["expected_network_ir_hash"] = repeat("0", 64)
    @test_throws ArgumentError optimize_rop_shape_request(wrong_hash)

    oversized_sync = deepcopy(request)
    oversized_sync["work_budget"]["max_cells"] = 257
    @test_throws BiocircuitsExplorerBackend.SyncBudgetExceeded begin
        BiocircuitsExplorerBackend._rop_shape_normalize_request(
            oversized_sync; synchronous=true)
    end
    oversized_response = BiocircuitsExplorerBackend.router(HTTP.Request(
        "POST", "/api/v1/rop_shape_optimize",
        ["Content-Type" => "application/json"], JSON3.write(oversized_sync)))
    @test oversized_response.status == 422
    oversized_replays = deepcopy(request)
    oversized_replays["work_budget"]["max_replays"] = 3
    @test_throws BiocircuitsExplorerBackend.SyncBudgetExceeded begin
        BiocircuitsExplorerBackend._rop_shape_normalize_request(
            oversized_replays; synchronous=true)
    end

    ambiguous_radius = deepcopy(request)
    ambiguous_radius["designability_spec"]["constraints"]["robustness"] =
        Dict("min_chebyshev_radius" => 0.1, "hard" => true)
    @test_throws ArgumentError optimize_rop_shape_request(ambiguous_radius)
end
