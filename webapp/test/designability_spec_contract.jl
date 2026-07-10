using Test
using HTTP
using JSON3
using BiocircuitsExplorerBackend

const BEB = BiocircuitsExplorerBackend

function _with_test_parameter_bounds!(spec)
    constraints = Dict{String, Any}(get(spec, "constraints", Dict{String, Any}()))
    constraints["parameter_bounds"] = Dict(
        "basis" => "log10_qK",
        "kd_log10" => Any[-3.0, 3.0],
        "total_log10" => Any[-3.0, 3.0],
    )
    spec["constraints"] = constraints
    return spec
end

@testset "DesignabilitySpec Contract" begin
    @testset "Proxy candidates are not recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "sign", "target" => "+-+")
            ),
            "candidate_budget" => Dict("mode" => "near_minimal", "max_recommended" => 8, "max_screened" => 12),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test screen["schema_version"] == "bne-design-screen/v0.3.0"
        @test haskey(screen, "designability_spec_normalized")
        @test haskey(screen, "constraint_audit")
        @test haskey(screen, "verified_recommendations")
        @test haskey(screen, "screened_candidates")
        @test all(card -> get(card, "evidence_grade", "") != "proxy_only",
                  screen["verified_recommendations"])
        @test all(card -> card["pass"] !== true || get(card, "evidence_grade", "") != "proxy_only",
                  screen["screened_candidates"])
        @test !any(item -> item["path"] == "/target/legacy_target" &&
                           item["kind"] == "qualitative_legacy_target_feasible_region",
                  screen["constraint_audit"])
        @test !haskey(screen, "recommended") || screen["recommended"] == screen["verified_recommendations"]
    end

    @testset "Verified recommendation helper honors solver metric preference order" begin
        cards = Any[
            Dict(
                "nid" => "higher-radius",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 1, "r" => 1, "mu" => 1),
                "metrics" => Dict("chebyshev_radius" => 9.0, "tunable_volume" => 1.0),
            ),
            Dict(
                "nid" => "higher-volume",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 2, "r" => 2, "mu" => 2),
                "metrics" => Dict("chebyshev_radius" => 1.0, "tunable_volume" => 8.0),
            ),
        ]

        BEB._design_sort_verified_cards!(cards, Dict("prefer" => Any["tunable_volume", "chebyshev_radius"]))

        @test first(cards)["nid"] == "higher-volume"

        complexity_cards = Any[
            Dict(
                "nid" => "complex",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 4, "r" => 3, "mu" => 2),
                "metrics" => Dict("chebyshev_radius" => 1.0, "tunable_volume" => 1.0),
            ),
            Dict(
                "nid" => "simple",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 1, "r" => 1, "mu" => 1),
                "metrics" => Dict("chebyshev_radius" => 1.0, "tunable_volume" => 1.0),
            ),
        ]

        BEB._design_sort_verified_cards!(complexity_cards, Dict("prefer" => Any["complexity"]))

        @test first(complexity_cards)["nid"] == "simple"

        tied_volume_cards = Any[
            Dict(
                "nid" => "z-higher-radius",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 1, "r" => 1, "mu" => 1),
                "metrics" => Dict("chebyshev_radius" => 9.0, "tunable_volume" => 1.0),
            ),
            Dict(
                "nid" => "a-lower-radius",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 1, "r" => 1, "mu" => 1),
                "metrics" => Dict("chebyshev_radius" => 1.0, "tunable_volume" => 1.0),
            ),
        ]

        BEB._design_sort_verified_cards!(tied_volume_cards, Dict("prefer" => Any["tunable_volume"]))

        @test first(tied_volume_cards)["nid"] == "a-lower-radius"

        unavailable_pref_cards = Any[
            Dict(
                "nid" => "z-higher-radius",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 1, "r" => 1, "mu" => 1),
                "metrics" => Dict("chebyshev_radius" => 9.0, "tunable_volume" => 9.0),
            ),
            Dict(
                "nid" => "a-lower-radius",
                "inp" => "tA",
                "out" => "B",
                "evidence_grade" => "enforced_exact",
                "certificate_grade" => "exact-union-siso-rop",
                "complexity" => Dict("d" => 9, "r" => 9, "mu" => 9),
                "metrics" => Dict("chebyshev_radius" => 1.0, "tunable_volume" => 1.0),
            ),
        ]

        BEB._design_sort_verified_cards!(unavailable_pref_cards, Dict("prefer" => Any["condition_number"]))

        @test first(unavailable_pref_cards)["nid"] == "a-lower-radius"
    end

    @testset "Unsupported hard clauses block verified recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "sign", "target" => "+-+"),
                "temporal_dynamics" => Dict("peak_width_seconds" => Dict("min" => 10.0)),
            ),
            "audit_policy" => Dict("unsupported" => "block_if_hard", "path_format" => "json_pointer"),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Temporal dynamics are recognized unsupported clauses with soft hard flag" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "manual_config"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0]),
                ),
                "temporal_dynamics" => Dict(
                    "peak_width_seconds" => Dict("min" => 10.0),
                    "hard" => false,
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )
        _with_test_parameter_bounds!(spec)

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["kind"] == "temporal_dynamics" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] != "blocked_by_unsupported_hard_clause"

        bad_hard_spec = deepcopy(spec)
        bad_hard_spec["target"]["temporal_dynamics"]["hard"] = "false"
        bad_hard_screen = BEB.design_screen_from_spec(bad_hard_spec)

        @test isempty(bad_hard_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["kind"] == "temporal_dynamics" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  bad_hard_screen["constraint_audit"])
        @test bad_hard_screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"

        unknown_spec = deepcopy(spec)
        unknown_spec["target"]["temporal_dynamics"]["phase_margin_seconds"] = 3.0
        unknown_screen = BEB.design_screen_from_spec(unknown_spec)

        @test isempty(unknown_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/phase_margin_seconds" &&
                          item["kind"] == "unsupported_temporal_dynamics_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none",
                  unknown_screen["constraint_audit"])
        @test unknown_screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"

        escaped_spec = deepcopy(spec)
        escaped_spec["target"]["temporal_dynamics"]["bad/key~x"] = true
        escaped_screen = BEB.design_screen_from_spec(escaped_spec)

        @test isempty(escaped_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/bad~1key~0x" &&
                          item["kind"] == "unsupported_temporal_dynamics_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  escaped_screen["constraint_audit"])

        invalid_peak_spec = deepcopy(spec)
        invalid_peak_spec["target"]["temporal_dynamics"]["peak_width_seconds"] = "bad"
        invalid_peak_screen = BEB.design_screen_from_spec(invalid_peak_spec)

        @test isempty(invalid_peak_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["kind"] == "temporal_dynamics_peak_width_seconds" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  invalid_peak_screen["constraint_audit"])

        invalid_peak_bound_spec = deepcopy(spec)
        invalid_peak_bound_spec["target"]["temporal_dynamics"]["peak_width_seconds"] = Dict("min" => true)
        invalid_peak_bound_screen = BEB.design_screen_from_spec(invalid_peak_bound_spec)

        @test isempty(invalid_peak_bound_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds/min" &&
                          item["kind"] == "temporal_dynamics_peak_width_seconds" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  invalid_peak_bound_screen["constraint_audit"])

        unknown_peak_key_spec = deepcopy(spec)
        unknown_peak_key_spec["target"]["temporal_dynamics"]["peak_width_seconds"] = Dict(
            "min" => 10.0,
            "bad/key~x" => 1.0,
        )
        unknown_peak_key_screen = BEB.design_screen_from_spec(unknown_peak_key_spec)

        @test isempty(unknown_peak_key_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds/bad~1key~0x" &&
                          item["kind"] == "unsupported_temporal_peak_width_seconds_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  unknown_peak_key_screen["constraint_audit"])
    end

    @testset "Qualitative legacy targets hard-block feasible-region recommendations" begin
        cases = Any[
            Dict(
                "name" => "parameter bounds only",
                "constraints" => Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 0),
            ),
            Dict(
                "name" => "exact placement budget only",
                "constraints" => Dict{String, Any}(),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            ),
            Dict(
                "name" => "parameter bounds and exact placement budget",
                "constraints" => Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            ),
        ]
        for case in cases, (target_kind, target) in ("sign" => "+-+", "label" => "biphasic_peak")
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => target_kind, "target" => target),
                ),
                "constraints" => case["constraints"],
                "candidate_budget" => case["candidate_budget"],
            )

            screen = BEB.design_screen_from_spec(spec)

            @test screen["designable"] === true
            @test isempty(screen["verified_recommendations"])
            @test !isempty(screen["minimal_certificates"])
            @test any(item -> item["path"] == "/target/legacy_target" &&
                              item["kind"] == "qualitative_legacy_target_feasible_region" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true &&
                              item["stage"] == "compile" &&
                              item["solver"] == "none" &&
                              occursin("exact reaction-order or structured behavior_spec target", item["reason"]),
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed legacy exact target hard-blocks without numeric coercion" begin
        for malformed in Any[
            Any[true],
            Any[Inf],
            Any[NaN],
        ]
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => malformed),
                ),
                "constraints" => Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test screen["designable"] === false
            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/legacy_target/target" &&
                              item["kind"] == "legacy_exact_target" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "BehaviorSpec reaction-order program lowers to exact feasible-region target" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test screen["designable"] === true
        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["inp"] == "tA"
        @test card["out"] == "C_A_A"
        @test card["certificate_grade"] == "exact-union-siso-rop"
        @test card["evidence_grade"] == "enforced_exact"
        @test any(item -> item["path"] == "/target/behavior_spec" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/behavior_spec/program/0" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test card["constraints"]["bounds_intersection_verified"] === true
        @test card["metrics"]["tunable_volume"] > 0.0
        @test card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound"
        @test card["metrics"]["tunable_volume_dimension"] == length(card["parameter_recommendation"]["theta_star"]["log_qK"])
    end

    @testset "Malformed behavior program values hard-block before exact lowering" begin
        for bad_value in Any[true, Inf, "1.0"]
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "C_A_A",
                        "program" => Any[Dict("kind" => "reaction_order", "value" => bad_value)],
                    ),
                ),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 4,
                    "max_screened" => 4,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/behavior_spec/program/0" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test !any(item -> item["path"] == "/target/behavior_spec/program/0" &&
                               item["support_level"] == "enforced_exact",
                       screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed BehaviorSpec core fields hard-block by nested path without coercion" begin
        cases = Any[
            (
                "input must be string",
                Dict(
                    "input" => true,
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
                "/target/behavior_spec/input",
                "behavior_spec_contract",
            ),
            (
                "output must be string",
                Dict(
                    "input" => "tA",
                    "output" => Any["C_A_A"],
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
                "/target/behavior_spec/output",
                "behavior_spec_contract",
            ),
            (
                "program must be array",
                Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => "not-an-array",
                ),
                "/target/behavior_spec/program",
                "behavior_spec_contract",
            ),
            (
                "program cannot be empty",
                Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[],
                ),
                "/target/behavior_spec/program",
                "behavior_spec_contract",
            ),
            (
                "program steps must be objects",
                Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[true],
                ),
                "/target/behavior_spec/program/0",
                "behavior_step_contract",
            ),
            (
                "feature_space must be reaction_order string",
                Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "feature_space" => true,
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
                "/target/behavior_spec/feature_space",
                "behavior_feature_space",
            ),
        ]

        for (name, behavior, expected_path, expected_kind) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict("behavior_spec" => behavior),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 4,
                    "max_screened" => 4,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["kind"] == expected_kind &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true &&
                              item["stage"] == "compile" &&
                              item["solver"] == "none",
                      screen["constraint_audit"])
            @test !any(item -> item["path"] == "/target/behavior_spec" &&
                               item["support_level"] == "enforced_exact",
                       screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Legacy target cannot bypass an explicit BehaviorSpec target" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "legacy_target" => Dict(
                    "target_kind" => "exact",
                    "target" => Any[1.0],
                ),
                "behavior_spec" => Dict(
                    "input" => "tB",
                    "output" => "C_A_B",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => -1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target" &&
                          item["kind"] == "target_contract" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          occursin("legacy_target", item["reason"]) &&
                          occursin("behavior_spec", item["reason"]),
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/target/legacy_target" &&
                           item["support_level"] == "enforced_exact",
                   screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unknown BehaviorSpec top-level keys hard-block with JSON Pointer escaping" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                    "bad/key~x" => Dict("ignored" => true),
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/behavior_spec/bad~1key~0x" &&
                          item["kind"] == "unsupported_behavior_spec_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile",
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/target/behavior_spec" &&
                           item["support_level"] == "enforced_exact",
                   screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unknown contract wrapper keys hard-block with JSON Pointer escaping" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict(
                "kind" => "agent_design",
                "bad/source~x" => "not part of the contract",
            ),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
                "bad/budget~x" => 12,
            ),
            "audit_policy" => Dict(
                "unsupported" => "block_if_hard",
                "path_format" => "json_pointer",
                "bad/audit~x" => true,
            ),
            "bad/root~x" => true,
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/bad~1root~0x" &&
                          item["kind"] == "unsupported_spec_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/source/bad~1source~0x" &&
                          item["kind"] == "unsupported_source_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/candidate_budget/bad~1budget~0x" &&
                          item["kind"] == "unsupported_candidate_budget_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/audit_policy/bad~1audit~0x" &&
                          item["kind"] == "unsupported_audit_policy_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Missing source.kind hard-blocks verified recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/source" &&
                          item["kind"] == "source_contract" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unimplemented audit policy modes are rejected by contract audit" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            "audit_policy" => Dict("unsupported" => "warn", "path_format" => "json_pointer"),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/audit_policy/unsupported" &&
                          item["kind"] == "audit_policy_contract" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "max_near_misses independently limits exploratory near misses" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "sign", "target" => "+-+"),
            ),
            "candidate_budget" => Dict("max_screened" => 5, "max_near_misses" => 0),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["screened_candidates"])
        @test length(screen["screened_candidates"]) <= 5
        @test isempty(screen["near_misses"])
        @test screen["screen_summary"]["near_miss_count"] == 0
    end

    @testset "Ranking policy refuses unimplemented non-verified recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            "ranking_policy" => Dict("verified_only" => false),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test screen["recommended"] == screen["verified_recommendations"]
        @test any(item -> item["path"] == "/ranking_policy/verified_only" &&
                          item["kind"] == "ranking_policy_contract" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Schema-invalid ranking policy clauses hard-block direct API specs" begin
        base_spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        bad_cases = [
            ("non_object", 3, "/ranking_policy"),
            ("unknown_key", Dict("prefer" => Any["complexity"], "bad/rank~x" => true), "/ranking_policy/bad~1rank~0x"),
            ("non_array_prefer", Dict("prefer" => "complexity"), "/ranking_policy/prefer"),
            ("non_string_prefer", Dict("prefer" => Any["complexity", 42]), "/ranking_policy/prefer/1"),
            ("unknown_prefer", Dict("prefer" => Any["complexity", "proxy_score"]), "/ranking_policy/prefer/1"),
        ]

        for (_label, ranking_policy, expected_path) in bad_cases
            spec = deepcopy(base_spec)
            spec["ranking_policy"] = ranking_policy
            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Audit policy can hide supported audit evidence in external responses" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                ),
                "temporal_dynamics" => Dict(
                    "peak_width_seconds" => Dict("min" => 1.0),
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4),
            "audit_policy" => Dict(
                "unsupported" => "block_if_hard",
                "path_format" => "json_pointer",
                "include_supported" => false,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["constraint_audit"])
        @test all(item -> item["support_level"] == "unsupported", screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["kind"] == "temporal_dynamics",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"

        req = HTTP.Request("POST", "/api/validate_designability_spec", [], JSON3.write(spec))
        res = BEB.router(req)
        @test res.status == 200
        parsed = JSON3.read(String(copy(res.body)))
        @test !isempty(parsed["constraint_audit"])
        @test all(item -> item["support_level"] == "unsupported", parsed["constraint_audit"])
        @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width_seconds" &&
                          item["kind"] == "temporal_dynamics",
                  parsed["constraint_audit"])
        @test parsed["blocked_by_unsupported_hard_clause"] === true
    end

    @testset "Unknown BehaviorSpec input_window keys hard-block with JSON Pointer escaping" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict(
                        "input_log10" => Any[-6.0, 6.0],
                        "bad/key~x" => true,
                        "hard" => true,
                    ),
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/behavior_spec/input_window/bad~1key~0x" &&
                          item["kind"] == "unsupported_input_window_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile",
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/target/behavior_spec/input_window/input_log10" &&
                           item["support_level"] == "enforced_exact",
                   screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Top-level input_window cannot shadow BehaviorSpec input window" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-2.0, 2.0], "hard" => true),
                ),
                "input_window" => Dict("input_log10" => Any[-10.0, -9.0], "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/input_window" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          occursin("behavior_spec.input_window", String(item["reason"])),
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unknown BehaviorSpec program step keys hard-block exact lowering" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "C_A_A",
                    "program" => Any[
                        Dict(
                            "kind" => "reaction_order",
                            "value" => 1.0,
                            "slope_tolerance" => 0.1,
                        ),
                    ],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/behavior_spec/program/0/slope_tolerance" &&
                          item["kind"] == "unsupported_behavior_step_key" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile",
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/target/behavior_spec/program/0" &&
                           item["support_level"] == "enforced_exact",
                   screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "BehaviorSpec step at and window fields hard-block because they are not solver-backed" begin
        cases = Any[
            Dict("kind" => "reaction_order", "value" => 1.0, "at" => -2.0),
            Dict("kind" => "reaction_order", "value" => 1.0, "window" => Dict("input_log10" => Any[-3.0, 3.0])),
        ]
        expected_paths = [
            "/target/behavior_spec/program/0/at",
            "/target/behavior_spec/program/0/window",
        ]

        for (step, expected_path) in zip(cases, expected_paths)
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "C_A_A",
                        "program" => Any[step],
                    ),
                ),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 4,
                    "max_screened" => 4,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["kind"] == "unsupported_behavior_step_key" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "BehaviorSpec input window gets an exact witness recommendation" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
            ),
            "constraints" => Dict(
                "dynamic_range" => Dict("min_fold_change" => 10.0, "sample_points" => 81, "hard" => true),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            "ranking_policy" => Dict("prefer" => Any["dynamic_range"]),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["evidence_grade"] == "enforced_exact+sampled_forward"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["dynamic_range_verified"] === true
        @test card["metrics"]["sampled_dynamic_range_fold_change"] >= 10.0
        @test card["metrics"]["dynamic_range"] == card["metrics"]["sampled_dynamic_range_fold_change"]
        @test card["metrics"]["sampled_dynamic_range_log10"] >= log10(10.0) - 1.0e-7
        @test card["metrics"]["sampled_dynamic_range_points"] == 81
        @test card["metrics"]["sampled_dynamic_range_floor_limited"] === false
        @test card["metrics"]["tunable_volume"] > 0.0
        @test card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound"
        @test card["metrics"]["tunable_volume_dimension"] == length(card["parameter_recommendation"]["theta_star"]["background_log_qK"])
        @test haskey(card["parameter_recommendation"]["theta_star"], "witness_input_log10")
        witnesses = card["parameter_recommendation"]["theta_star"]["witness_input_log10"]
        @test length(witnesses) == 2
        @test witnesses[2] - witnesses[1] >= 0.1 - 1.0e-7
        @test any(item -> item["path"] == "/target/behavior_spec/input_window/input_log10" &&
                          item["support_level"] == "enforced_exact" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/dynamic_range/min_fold_change" &&
                          item["support_level"] == "sampled_forward" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "sampled_forward" &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "sampled-window-dose-response",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "BehaviorSpec operating points are exact-window solver backed" begin
        fixed_points = Any[-2.0, 2.0]
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict(
                        "input_log10" => Any[-6.0, 6.0],
                        "operating_points_log10" => fixed_points,
                        "hard" => true,
                    ),
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["operating_points_verified"] === true
        @test card["metrics"]["requested_operating_points_log10"] == Float64.(fixed_points)
        @test card["metrics"]["witness_input_log10"] == Float64.(fixed_points)
        @test card["parameter_recommendation"]["theta_star"]["witness_input_log10"] == Float64.(fixed_points)
        @test any(item -> item["path"] == "/target/behavior_spec/input_window/operating_points_log10" &&
                          item["support_level"] == "enforced_exact" &&
                          item["hard"] === true &&
                          item["solver"] == "exact-window-siso-rop-path",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/behavior_spec/input_window/operating_points_log10" &&
                          item["support_level"] == "enforced_exact" &&
                          item["hard"] === true,
                  card["constraints"]["supported_constraints"])
    end

    @testset "Constraint transition spacing is exact-window solver backed" begin
        requested_spacing = 0.75
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
            ),
            "constraints" => Dict(
                "transitions" => Dict("min_spacing_decades" => requested_spacing, "hard" => true),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            "ranking_policy" => Dict("prefer" => Any["transition_spacing"]),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["transition_spacing_verified"] === true
        @test card["metrics"]["requested_transition_spacing_decades"] == requested_spacing
        @test card["metrics"]["effective_transition_spacing_decades"] == requested_spacing
        witnesses = card["parameter_recommendation"]["theta_star"]["witness_input_log10"]
        @test length(witnesses) == 2
        @test witnesses[2] - witnesses[1] >= requested_spacing - 1.0e-7
        @test card["metrics"]["transition_spacing"] == card["metrics"]["witness_min_spacing_decades"]
        @test any(item -> item["path"] == "/constraints/transitions/min_spacing_decades" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "feasible_region" &&
                          item["solver"] == "exact-window-siso-rop-path",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/transitions/min_spacing_decades" &&
                          item["support_level"] == "enforced_exact",
                  card["constraints"]["supported_constraints"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "exact-window-siso-rop-path",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "Constraint transition spacing without complete input window blocks" begin
        for behavior_extra in (Dict{String, Any}(), Dict{String, Any}("input_window" => Dict("hard" => true)))
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => merge(Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                    ), behavior_extra),
                ),
                "constraints" => Dict(
                    "transitions" => Dict("min_spacing_decades" => 0.75, "hard" => true),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/transitions/min_spacing_decades" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed constraint transition spacing blocks by nested path" begin
        for bad_spacing in (Inf, -0.1, "wide", true)
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                    ),
                ),
                "constraints" => Dict(
                    "transitions" => Dict("min_spacing_decades" => bad_spacing, "hard" => false),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/transitions/min_spacing_decades" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Constraint transition order is exact-window solver backed" begin
        requested_order = Any[0, 1]
        requested_spacing = 0.5
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
            ),
            "constraints" => Dict("transitions" => Dict(
                "order" => requested_order,
                "min_spacing_decades" => requested_spacing,
                "hard" => true,
            )),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["transition_order_verified"] === true
        @test card["constraints"]["transition_spacing_verified"] === true
        @test card["metrics"]["requested_transition_order"] == Int[0, 1]
        @test card["metrics"]["transition_order_basis"] == "behavior_spec.program_index"
        witnesses = card["parameter_recommendation"]["theta_star"]["witness_input_log10"]
        @test witnesses[2] - witnesses[1] >= requested_spacing - 1.0e-7
        @test any(item -> item["path"] == "/constraints/transitions/order" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "feasible_region" &&
                          item["solver"] == "exact-window-siso-rop-path",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/transitions/order" &&
                          item["support_level"] == "enforced_exact",
                  card["constraints"]["supported_constraints"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "Unknown transition clauses hard-block even with supported keys" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
            ),
            "constraints" => Dict("transitions" => Dict(
                "order" => Any[0, 1],
                "max_spacing_decades" => 0.1,
                "hard" => true,
            )),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/transitions/max_spacing_decades" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/transitions/order" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unknown transition keys are JSON Pointer escaped" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
            ),
            "constraints" => Dict("transitions" => Dict("bad/key~x" => true, "order" => Any[0, 1], "hard" => true)),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/transitions/bad~1key~0x" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Malformed transition order hard-blocks by nested path" begin
        bad_orders = Any[
            Any[0],
            Any[0, 0],
            Any[0, 2],
            Any[true, 1],
            Any["missing", "ids"],
        ]
        for bad_order in bad_orders
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                    ),
                ),
                "constraints" => Dict("transitions" => Dict("order" => bad_order, "hard" => false)),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/transitions/order" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Transition order without complete input window blocks" begin
        for behavior_extra in (
            Dict{String, Any}(),
            Dict{String, Any}("input_window" => Dict("hard" => true)),
        )
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => merge(Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                    ), behavior_extra),
                ),
                "constraints" => Dict("transitions" => Dict("order" => Any[0, 1], "hard" => true)),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/transitions/order" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Unknown sampled and robustness clauses hard-block by nested path" begin
        base_behavior = Dict(
            "input" => "tA",
            "output" => "B",
            "program" => Any[
                Dict("kind" => "reaction_order", "value" => 0.0),
                Dict("kind" => "reaction_order", "value" => -1.0),
            ],
            "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
        )
        cases = Any[
            (
                Dict("output_feature" => Dict(
                    "feature" => "fold_change",
                    "operator" => ">=",
                    "value" => 10.0,
                    "sample_points" => 81,
                    "window_smoothing" => true,
                    "hard" => true,
                )),
                Dict{String, Any}(),
                "/target/output_feature/window_smoothing",
                "/target/output_feature",
            ),
            (
                Dict("output_feature" => Dict(
                    "feature" => "fold_change",
                    "operator" => ">=",
                    "value" => 10.0,
                    "sample_points" => 81,
                    "abs_tolerance" => 0.1,
                    "hard" => true,
                )),
                Dict{String, Any}(),
                "/target/output_feature/abs_tolerance",
                "/target/output_feature",
            ),
            (
                Dict("shape" => Dict(
                    "class" => "monotonic",
                    "monotonicity" => "decreasing",
                    "sample_points" => 81,
                    "peak_width" => 1.0,
                    "hard" => true,
                )),
                Dict{String, Any}(),
                "/target/shape/peak_width",
                "/target/shape",
            ),
            (
                Dict{String, Any}(),
                Dict("dynamic_range" => Dict(
                    "min_fold_change" => 10.0,
                    "sample_points" => 81,
                    "max_slope" => 0.5,
                    "hard" => true,
                )),
                "/constraints/dynamic_range/max_slope",
                "/constraints/dynamic_range/min_fold_change",
            ),
            (
                Dict{String, Any}(),
                Dict("robustness" => Dict(
                    "min_chebyshev_radius" => 0.1,
                    "curvature_penalty" => 100.0,
                    "hard" => true,
                )),
                "/constraints/robustness/curvature_penalty",
                "/constraints/robustness/min_chebyshev_radius",
            ),
        ]

        for (target_extra, constraints, expected_path, forbidden_supported_path) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => merge(Dict("behavior_spec" => base_behavior), target_extra),
                "constraints" => constraints,
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true &&
                              item["stage"] == "compile" &&
                              item["solver"] == "none" &&
                              occursin("active compiler/verifier does not support", item["reason"]),
                      screen["constraint_audit"])
            @test !any(item -> item["path"] == forbidden_supported_path &&
                               item["support_level"] in ("sampled_forward", "enforced_exact"),
                       screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "BehaviorSpec input window verifies sampled target output_feature" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
                "output_feature" => Dict("feature" => "fold_change", "operator" => ">=", "value" => 10.0, "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => true),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["evidence_grade"] == "enforced_exact+sampled_forward"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["output_feature_verified"] === true
        @test card["metrics"]["sampled_output_feature_status"] == "pass"
        @test card["metrics"]["sampled_output_feature_value"] >= 10.0
        @test length(card["metrics"]["sampled_output_feature_range"]) == 2
        @test card["metrics"]["sampled_output_feature_sample_points"] == 81
        @test card["metrics"]["sampled_output_feature_floor_limited"] === false
        @test any(item -> item["path"] == "/target/output_feature" &&
                          item["support_level"] == "sampled_forward" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/output_feature" &&
                          item["support_level"] == "sampled_forward",
                  card["constraints"]["supported_constraints"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "BehaviorSpec input window verifies sampled threshold output_feature" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
                "output_feature" => Dict("feature" => "threshold", "operator" => ">=", "value" => 1.0e-6, "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => true),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["constraints"]["output_feature_verified"] === true
        @test card["metrics"]["sampled_output_feature"] == "threshold"
        @test card["metrics"]["sampled_output_feature_operator"] == ">="
        @test card["metrics"]["sampled_output_feature_value"] >= 1.0e-6
        @test card["metrics"]["sampled_output_feature_sample_points"] == 81
        @test any(item -> item["path"] == "/target/output_feature" &&
                          item["support_level"] == "sampled_forward" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
    end

    @testset "Output feature equality alias does not bypass schema operator contract" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
                "output_feature" => Dict("feature" => "threshold", "operator" => "==", "value" => 1.0e-6, "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/output_feature" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          occursin("Only >=, <=, and =", String(item["reason"])),
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "BehaviorSpec input window verifies sampled monotonic target shape" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => 0.1, "hard" => true),
                ),
                "shape" => Dict("class" => "monotonic", "monotonicity" => "decreasing", "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => true),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-window-siso-rop-path"
        @test card["evidence_grade"] == "enforced_exact+sampled_forward"
        @test card["constraints"]["input_window_verified"] === true
        @test card["constraints"]["shape_verified"] === true
        @test card["metrics"]["sampled_shape"] == "monotonic"
        @test card["metrics"]["sampled_shape_status"] == "pass"
        @test card["metrics"]["sampled_shape_direction"] == "decreasing"
        @test card["metrics"]["sampled_shape_sample_points"] == 81
        @test card["metrics"]["sampled_shape_floor_limited"] === false
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "sampled_forward" &&
                          item["hard"] === true &&
                          item["solver"] == "sampled-window-dose-response",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "sampled_forward",
                  card["constraints"]["supported_constraints"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "Sampled bell-shaped target shape requires explicit prominence" begin
        bell_curve = (
            valid = true,
            ylog = Float64[0.0, 0.8, 2.0, 0.8, 0.0],
            ylinear = exp10.(Float64[0.0, 0.8, 2.0, 0.8, 0.0]),
            log10_range = 2.0,
            fold_change = 100.0,
            output_min = 1.0,
            output_max = 100.0,
            sample_points = 5,
            floor_limited = false,
            reason = "synthetic curve",
        )
        pass_shape = Dict("class" => "bell_shaped", "min_prominence_log10" => 1.0, "sample_points" => 81, "tolerance_log10" => 0.01)
        missing_prominence = Dict("class" => "bell_shaped", "sample_points" => 81, "tolerance_log10" => 0.01)
        endpoint_peak_curve = merge(bell_curve, (; ylog = Float64[2.0, 1.0, 0.0, 0.5, 1.0]))

        pass_eval = BEB._designability_evaluate_shape(bell_curve, pass_shape)
        unsupported_eval = BEB._designability_evaluate_shape(bell_curve, missing_prominence)
        endpoint_eval = BEB._designability_evaluate_shape(endpoint_peak_curve, pass_shape)

        @test pass_eval.pass === true
        @test pass_eval.status == "pass"
        @test pass_eval.class == "bell_shaped"
        @test pass_eval.peak_index == 3
        @test pass_eval.prominence_left_log10 >= 1.0
        @test pass_eval.prominence_right_log10 >= 1.0
        @test unsupported_eval.pass === false
        @test unsupported_eval.status == "unsupported"
        @test endpoint_eval.pass === false
        @test endpoint_eval.status == "fail"
    end

    @testset "Target shape without behavior input window blocks recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 0.0)],
                ),
                "shape" => Dict("class" => "monotonic", "monotonicity" => "decreasing", "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Target shape with invalid sample_points blocks even when soft" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
                "shape" => Dict("class" => "monotonic", "monotonicity" => "decreasing", "sample_points" => 1, "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/shape/sample_points" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          occursin("sample_points", item["reason"]),
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Target shape requires finite behavior input window" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-Inf, Inf], "hard" => false),
                ),
                "shape" => Dict("class" => "monotonic", "monotonicity" => "decreasing", "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/behavior_spec/input_window/input_log10" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Malformed target shape fields block instead of coercing" begin
        bad_shapes = Any[
            (Dict("class" => "bell_shaped", "min_prominence_log10" => true, "hard" => false), "/target/shape"),
            (Dict("class" => "monotonic", "hard" => false), "/target/shape"),
            (Dict("class" => true, "hard" => false), "/target/shape"),
            (Dict("class" => "monotonic", "monotonicity" => true, "hard" => false), "/target/shape"),
            (Dict("class" => "monotonic", "monotonicity" => "decreasing", "sample_points" => 81, "tolerance_log10" => true, "hard" => false), "/target/shape/tolerance_log10"),
            (Dict("class" => "monotonic", "monotonicity" => "decreasing", "min_prominence_log10" => 0.5, "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => false), "/target/shape/min_prominence_log10"),
            (Dict("class" => "bell_shaped", "min_prominence_log10" => 0.5, "min_prominence_decades" => 1.0, "sample_points" => 81, "tolerance_log10" => 0.01, "hard" => false), "/target/shape/min_prominence_decades"),
        ]
        for (shape, expected_path) in bad_shapes
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                    ),
                    "shape" => shape,
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Shape schema exposes only supported monotonicity values" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        shape_schema = schema["properties"]["target"]["properties"]["shape"]
        monotonicity_enum = shape_schema["properties"]["monotonicity"]["enum"]
        @test "class" in String.(shape_schema["required"])
        @test haskey(shape_schema, "allOf")
        @test !haskey(shape_schema["properties"]["monotonicity"], "default")
        @test !("nonmonotone" in String.(monotonicity_enum))
        @test Set(String.(monotonicity_enum)) == Set(["increasing", "decreasing", "any"])
    end

    @testset "Shape schema forbids ignored or ambiguous prominence clauses" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        shape_schema = schema["properties"]["target"]["properties"]["shape"]

        @test any(rule -> haskey(rule, "if") &&
                          haskey(rule, "then") &&
                          get(rule["if"]["properties"]["class"], "const", nothing) == "monotonic" &&
                          haskey(rule["then"], "not"),
                  shape_schema["allOf"])
        @test any(rule -> haskey(rule, "if") &&
                          haskey(rule, "then") &&
                          get(rule["if"]["properties"]["class"], "const", nothing) == "bell_shaped" &&
                          haskey(rule["then"], "oneOf"),
                  shape_schema["allOf"])
    end

    @testset "Nested sampled and robustness schema keys match backend audit keys" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        output_feature = schema["properties"]["target"]["properties"]["output_feature"]
        shape = schema["properties"]["target"]["properties"]["shape"]
        temporal = schema["properties"]["target"]["properties"]["temporal_dynamics"]
        dynamic_range = schema["properties"]["constraints"]["properties"]["dynamic_range"]
        robustness = schema["properties"]["constraints"]["properties"]["robustness"]

        @test output_feature["additionalProperties"] === false
        @test shape["additionalProperties"] === false
        @test temporal["additionalProperties"] === false
        @test dynamic_range["additionalProperties"] === false
        @test robustness["additionalProperties"] === false
        @test Set(String.(keys(output_feature["properties"]))) == Set([
            "feature", "operator", "value", "sample_points", "tolerance_log10", "hard",
        ])
        @test Set(String.(output_feature["required"])) == Set(["feature", "sample_points", "tolerance_log10"])
        @test !haskey(output_feature["properties"]["sample_points"], "default")
        @test !haskey(output_feature["properties"]["tolerance_log10"], "default")
        @test Set(String.(keys(output_feature["properties"]))) == Set(BEB.DESIGN_OUTPUT_FEATURE_KEYS)
        @test Set(String.(output_feature["properties"]["feature"]["enum"])) ==
              Set(["threshold", "level", "fold_change"])
        @test Set(String.(output_feature["properties"]["operator"]["enum"])) ==
              Set(["=", ">=", "<="])
        @test Set(String.(keys(shape["properties"]))) == Set(BEB.DESIGN_SHAPE_KEYS)
        @test Set(String.(shape["properties"]["class"]["enum"])) == Set(["monotonic", "bell_shaped"])
        @test "sample_points" in String.(shape["required"])
        @test "tolerance_log10" in String.(shape["required"])
        @test !haskey(shape["properties"]["sample_points"], "default")
        @test !haskey(shape["properties"]["tolerance_log10"], "default")
        @test Set(String.(keys(temporal["properties"]))) == Set(BEB.DESIGN_TEMPORAL_DYNAMICS_KEYS)
        @test haskey(temporal["properties"], "peak_width_seconds")
        @test temporal["properties"]["peak_width_seconds"]["additionalProperties"] === false
        @test Set(String.(keys(temporal["properties"]["peak_width_seconds"]["properties"]))) ==
              Set(BEB.DESIGN_TEMPORAL_PEAK_WIDTH_KEYS)
        @test Set(String.(keys(dynamic_range["properties"]))) == Set(BEB.DESIGN_DYNAMIC_RANGE_KEYS)
        @test Set(String.(dynamic_range["required"])) == Set(["min_fold_change", "sample_points"])
        @test Set(String.(keys(robustness["properties"]))) == Set(BEB.DESIGN_ROBUSTNESS_KEYS)
        @test haskey(robustness["properties"], "min_tunable_volume_lower_bound")
        @test occursin(
            "Chebyshev-ball lower bound",
            String(robustness["properties"]["min_tunable_volume_lower_bound"]["description"]),
        )
    end

    @testset "Design screen schema admits backend verified evidence labels" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-screen.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        defs = schema["\$defs"]
        verified_card = defs["verified_designability_card"]["allOf"][2]["properties"]
        card = defs["designability_card"]["properties"]
        card_constraints = card["constraints"]["properties"]
        verified_constraints = verified_card["constraints"]
        audit_item = defs["constraint_audit_item"]["properties"]

        @test "enforced_exact+sampled_forward" in String.(verified_card["evidence_grade"]["enum"])
        @test "enforced_exact+sampled_forward" in String.(card["evidence_grade"]["enum"])
        @test "exact-union-siso-rop-path" in String.(verified_card["certificate_grade"]["enum"])
        @test "exact-window-siso-rop-path" in String.(verified_card["certificate_grade"]["enum"])
        @test "exact-union-siso-rop-path" in String.(card["certificate_grade"]["enum"])
        @test "exact-window-siso-rop-path" in String.(card["certificate_grade"]["enum"])
        @test "theta_union_path" in String.(card["metrics"]["properties"]["chebyshev_radius_source"]["enum"])
        @test "chebyshev_ball_lower_bound" in String.(card["metrics"]["properties"]["tunable_volume_source"]["enum"])
        @test Set(String.(verified_constraints["required"])) == Set([
            "parameter_bounds_verified",
            "parameter_bounds_source",
            "effective_parameter_bounds",
        ])
        @test verified_constraints["properties"]["parameter_bounds_verified"]["const"] === true
        @test Set(String.(verified_constraints["properties"]["parameter_bounds_source"]["enum"])) ==
              Set(["declared_spec"])
        @test haskey(card_constraints, "parameter_bounds_verified")
        @test haskey(card_constraints, "parameter_bounds_source")
        @test haskey(card_constraints, "effective_parameter_bounds")
        @test haskey(card_constraints, "parameter_bounds")
        @test defs["parameter_bounds_evidence"]["properties"]["basis"]["const"] == "log10_qK"
        @test defs["parameter_bounds_evidence"]["properties"]["by_class"]["minProperties"] == 1
        @test "sampled_forward" in String.(audit_item["support_level"]["enum"])
    end

    @testset "BehaviorSpec schema keys match backend supported keys" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        behavior_spec = schema["properties"]["target"]["properties"]["behavior_spec"]
        window = schema["\$defs"]["window"]
        behavior_step = schema["\$defs"]["behavior_step"]

        @test behavior_spec["additionalProperties"] === false
        @test window["additionalProperties"] === false
        @test behavior_step["additionalProperties"] === false
        @test Set(String.(keys(behavior_spec["properties"]))) == Set(BEB.DESIGN_BEHAVIOR_SPEC_KEYS)
        @test Set(String.(keys(window["properties"]))) == Set(BEB.DESIGN_INPUT_WINDOW_KEYS)
        @test Set(String.(keys(behavior_step["properties"]))) == Set(BEB.DESIGN_BEHAVIOR_STEP_KEYS)
        @test !haskey(behavior_step["properties"], "at")
        @test !haskey(behavior_step["properties"], "window")
    end

    @testset "Top-level target and constraint schema keys match backend audit keys" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        target = schema["properties"]["target"]
        constraints = schema["properties"]["constraints"]
        source = schema["properties"]["source"]
        candidate_budget = schema["properties"]["candidate_budget"]
        ranking_policy = schema["properties"]["ranking_policy"]
        audit_policy = schema["properties"]["audit_policy"]

        @test schema["additionalProperties"] === false
        @test source["additionalProperties"] === false
        @test target["additionalProperties"] === false
        @test constraints["additionalProperties"] === false
        @test candidate_budget["additionalProperties"] === false
        @test ranking_policy["additionalProperties"] === false
        @test audit_policy["additionalProperties"] === false
        @test target["properties"]["legacy_target"]["additionalProperties"] === false
        @test Set(String.(keys(target["properties"]["legacy_target"]["properties"]))) ==
              Set(BEB.DESIGN_LEGACY_TARGET_KEYS)
        @test Set(String.(keys(source["properties"]))) == Set(BEB.DESIGN_SOURCE_KEYS)
        @test Set(String.(keys(target["properties"]))) ==
              Set(["legacy_target", "behavior_spec", "input_window", "output_feature", "shape", "temporal_dynamics"])
        @test haskey(target, "allOf")
        @test any(rule -> haskey(rule, "not") &&
                          Set(String.(get(rule["not"], "required", Any[]))) == Set(["behavior_spec", "input_window"]),
                  target["allOf"])
        @test Set(String.(keys(constraints["properties"]))) ==
              Set(["network", "parameter_bounds", "robustness", "dynamic_range", "transitions"])
        @test Set(String.(keys(candidate_budget["properties"]))) == Set(BEB.DESIGN_CANDIDATE_BUDGET_KEYS)
        @test Set(String.(keys(ranking_policy["properties"]))) == Set(BEB.DESIGN_RANKING_POLICY_KEYS)
        @test Set(String.(keys(audit_policy["properties"]))) == Set(BEB.DESIGN_AUDIT_POLICY_KEYS)
        @test Set(String.(candidate_budget["properties"]["mode"]["enum"])) == Set(["near_minimal", "all_matches"])
        @test candidate_budget["properties"]["max_screened"]["maximum"] == 64
        @test candidate_budget["properties"]["max_exact_placements"]["maximum"] == 8
        @test ranking_policy["properties"]["verified_only"]["const"] === true
        @test Set(String.(audit_policy["properties"]["unsupported"]["enum"])) == Set(["block_if_hard"])
    end

    @testset "Target schema forbids mixed legacy and structured BehaviorSpec targets" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        target = schema["properties"]["target"]

        @test haskey(target, "not")
        @test Set(String.(target["not"]["required"])) == Set(["legacy_target", "behavior_spec"])
    end

    @testset "DesignabilitySpec ranking policy schema includes backend preferences" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        prefer = schema["properties"]["ranking_policy"]["properties"]["prefer"]["items"]["enum"]

        @test "certificate_grade" in String.(prefer)
    end

    @testset "Bell-shaped target shape without prominence blocks even when soft" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
                "shape" => Dict("class" => "bell_shaped", "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Unsupported target shape class blocks recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
                "shape" => Dict("class" => "threshold", "hard" => false),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/shape" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Sampled output feature numeric comparisons are conservative" begin
        @test BEB._designability_compare_feature_value(1.0e-12, ">=", 1.0e-6, 1.0e-6) === false
        @test BEB._designability_compare_feature_value(1.0e-6, ">=", 1.0e-6, 1.0e-6) === true

        huge_curve = (
            valid = true,
            ylog = Float64[0.0, 400.0],
            ylinear = Float64[1.0, 1.0e308],
            log10_range = 400.0,
            fold_change = 1.0e308,
            output_min = 1.0,
            output_max = 1.0e308,
            sample_points = 2,
            floor_limited = false,
            reason = "synthetic curve",
        )
        le_feature = Dict("feature" => "fold_change", "operator" => "<=", "value" => 1.0e308, "sample_points" => 11, "tolerance_log10" => 0.01)
        eq_feature = Dict("feature" => "fold_change", "operator" => "=", "value" => 1.0e308, "sample_points" => 11, "tolerance_log10" => 0.01)
        @test BEB._designability_evaluate_output_feature(huge_curve, le_feature).pass === false
        @test BEB._designability_evaluate_output_feature(huge_curve, eq_feature).pass === false

        near_fold_curve = (
            valid = true,
            ylog = Float64[0.0, log10(120.0)],
            ylinear = Float64[1.0, 120.0],
            log10_range = log10(120.0),
            fold_change = 120.0,
            output_min = 1.0,
            output_max = 120.0,
            sample_points = 2,
            floor_limited = false,
            reason = "synthetic curve",
        )
        loose_log_tol = Dict("feature" => "fold_change", "operator" => "=", "value" => 100.0, "sample_points" => 11, "tolerance_log10" => 0.1)
        tight_log_tol = Dict("feature" => "fold_change", "operator" => "=", "value" => 100.0, "sample_points" => 11, "tolerance_log10" => 0.01)
        @test BEB._designability_evaluate_output_feature(near_fold_curve, loose_log_tol).pass === true
        @test BEB._designability_evaluate_output_feature(near_fold_curve, tight_log_tol).pass === false
    end

    @testset "Unsupported output_feature is hard even when declared soft" begin
        bad_features = Any[
            Dict("feature" => "threshold", "operator" => ">=", "value" => 0.5, "hard" => false),
            Dict("feature" => true, "operator" => ">=", "value" => 0.5, "hard" => false),
            Dict("feature" => "threshold", "operator" => true, "value" => 0.5, "hard" => false),
            Dict("feature" => "threshold", "operator" => ">=", "value" => true, "hard" => false),
            Dict("feature" => "fold_change", "operator" => ">=", "value" => 2.0, "sample_points" => true, "hard" => false),
            Dict("feature" => "fold_change", "operator" => ">=", "value" => 2.0, "sample_points" => 1, "hard" => false),
        ]

        for feature in bad_features
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[Dict("kind" => "reaction_order", "value" => 0.0)],
                        "input_window" => Dict("hard" => false),
                    ),
                    "output_feature" => feature,
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/output_feature" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Sampled finite-dose clauses require explicit sampling parameters" begin
        base_target = Dict(
            "behavior_spec" => Dict(
                "input" => "tA",
                "output" => "C_A_A",
                "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                "input_window" => Dict("input_log10" => Any[-2.0, 2.0], "hard" => true),
            ),
        )
        base_constraints = Dict(
            "parameter_bounds" => Dict(
                "basis" => "log10_qK",
                "kd_log10" => Any[-3.0, 3.0],
                "total_log10" => Any[-3.0, 3.0],
            ),
        )
        cases = Any[
            (
                merge(base_target, Dict("output_feature" => Dict(
                    "feature" => "threshold",
                    "operator" => ">=",
                    "value" => 0.5,
                    "tolerance_log10" => 0.01,
                    "hard" => false,
                ))),
                base_constraints,
                "/target/output_feature/sample_points",
            ),
            (
                merge(base_target, Dict("output_feature" => Dict(
                    "feature" => "threshold",
                    "operator" => ">=",
                    "value" => 0.5,
                    "sample_points" => 81,
                    "hard" => false,
                ))),
                base_constraints,
                "/target/output_feature/tolerance_log10",
            ),
            (
                merge(base_target, Dict("shape" => Dict(
                    "class" => "monotonic",
                    "monotonicity" => "decreasing",
                    "tolerance_log10" => 0.01,
                    "hard" => false,
                ))),
                base_constraints,
                "/target/shape/sample_points",
            ),
            (
                merge(base_target, Dict("shape" => Dict(
                    "class" => "monotonic",
                    "monotonicity" => "decreasing",
                    "sample_points" => 81,
                    "hard" => false,
                ))),
                base_constraints,
                "/target/shape/tolerance_log10",
            ),
            (
                base_target,
                merge(base_constraints, Dict("dynamic_range" => Dict(
                    "min_fold_change" => 10.0,
                    "hard" => false,
                ))),
                "/constraints/dynamic_range/sample_points",
            ),
        ]

        for (target, constraints, expected_path) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => target,
                "constraints" => constraints,
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed dynamic range is not silently treated as supported" begin
        bad_dynamic_ranges = Any[
            Dict("min_fold_change" => Inf, "hard" => true),
            Dict("min_fold_change" => true, "hard" => false),
            Dict("min_fold_change" => 10.0, "sample_points" => true, "hard" => false),
            Dict("min_fold_change" => 10.0, "sample_points" => 1, "hard" => false),
        ]

        for dynamic_range in bad_dynamic_ranges
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                    ),
                ),
                "constraints" => Dict("dynamic_range" => dynamic_range),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> startswith(item["path"], "/constraints/dynamic_range") &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Incomplete behavior spec input window blocks verified recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[Dict("kind" => "reaction_order", "value" => 0.0)],
                    "input_window" => Dict("hard" => true),
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/behavior_spec/input_window" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Malformed behavior spec operating points hard-block by nested path" begin
        bad_operating_points = Any[
            Any[-2.0],
            Any[-7.0, 2.0],
            Any[-2.0, Inf],
            Any[-2.0, true],
        ]

        for points in bad_operating_points
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict(
                            "input_log10" => Any[-6.0, 6.0],
                            "operating_points_log10" => points,
                            "hard" => false,
                        ),
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/behavior_spec/input_window/operating_points_log10" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed behavior input window spacing blocks by nested path" begin
        @test BEB._designability_window_spacing(Dict("input_log10" => Any[-6.0, 6.0])) == 0.0
        for bad_spacing in ("bad", Inf, -1.0, true)
            @test BEB._designability_window_spacing(Dict(
                "input_log10" => Any[-6.0, 6.0],
                "min_spacing_decades" => bad_spacing,
            )) === nothing
        end

        for bad_spacing in ("bad", Inf, true)
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "agent_design"),
                "target" => Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "B",
                        "program" => Any[
                            Dict("kind" => "reaction_order", "value" => 0.0),
                            Dict("kind" => "reaction_order", "value" => -1.0),
                        ],
                        "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "min_spacing_decades" => bad_spacing, "hard" => false),
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/behavior_spec/input_window/min_spacing_decades" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Top-level target clauses without solvers block verified recommendations" begin
        clauses = [
            (
                Dict("input_window" => Dict("input_log10" => Any[-1.0, 1.0], "hard" => true)),
                "/target/input_window/input_log10",
            ),
            (
                Dict("output_feature" => Dict("feature" => "threshold", "operator" => ">=", "value" => 0.5, "hard" => false)),
                "/target/output_feature",
            ),
            (
                Dict("shape" => Dict("class" => "bell_shaped", "hard" => true)),
                "/target/shape",
            ),
        ]
        for (extra_target, expected_path) in clauses
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => merge(
                    Dict("legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0])),
                    extra_target,
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Unknown top-level target and constraint keys hard-block by JSON Pointer path" begin
        cases = Any[
            (
                Dict("bad/key~x" => Dict("hard" => true)),
                Dict{String, Any}(),
                "/target/bad~1key~0x",
                "unsupported_target_key",
            ),
            (
                Dict{String, Any}(),
                Dict("bad/key~x" => Dict("hard" => true)),
                "/constraints/bad~1key~0x",
                "unsupported_constraint_key",
            ),
        ]

        for (target_extra, constraints_extra, expected_path, expected_kind) in cases
            constraints = merge(
                Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                ),
                constraints_extra,
            )
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => merge(
                    Dict("legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0])),
                    target_extra,
                ),
                "constraints" => constraints,
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["kind"] == expected_kind &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Non-boolean hard values cannot soften unsupported clauses" begin
        for bad_hard in Any[0, 2, "false"]
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                    "input_window" => Dict("input_log10" => Any[-1.0, 1.0], "hard" => bad_hard),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/target/input_window/input_log10" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed robustness clauses hard-block instead of being ignored" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict("legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0])),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "robustness" => 42,
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness" &&
                          item["kind"] == "robustness_contract" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Non-boolean hard values on schema clauses hard-block by nested path" begin
        behavior_target = Dict(
            "behavior_spec" => Dict(
                "input" => "tA",
                "output" => "C_A_A",
                "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                "input_window" => Dict("input_log10" => Any[-2.0, 2.0]),
            ),
        )
        two_step_target = Dict(
            "behavior_spec" => Dict(
                "input" => "tA",
                "output" => "B",
                "program" => Any[
                    Dict("kind" => "reaction_order", "value" => 0.0),
                    Dict("kind" => "reaction_order", "value" => -1.0),
                ],
                "input_window" => Dict("input_log10" => Any[-6.0, 6.0]),
            ),
        )
        bounds = Dict(
            "parameter_bounds" => Dict(
                "basis" => "log10_qK",
                "kd_log10" => Any[-3.0, 3.0],
                "total_log10" => Any[-3.0, 3.0],
            ),
        )
        cases = Any[
            (
                Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "C_A_A",
                        "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0, "hard" => "true")],
                    ),
                ),
                bounds,
                "/target/behavior_spec/program/0/hard",
            ),
            (
                Dict(
                    "behavior_spec" => Dict(
                        "input" => "tA",
                        "output" => "C_A_A",
                        "program" => Any[Dict("kind" => "reaction_order", "value" => 1.0)],
                        "input_window" => Dict("input_log10" => Any[-2.0, 2.0], "hard" => "true"),
                    ),
                ),
                bounds,
                "/target/behavior_spec/input_window/hard",
            ),
            (
                merge(behavior_target, Dict(
                    "output_feature" => Dict(
                        "feature" => "threshold",
                        "operator" => ">=",
                        "value" => 1.0e-6,
                        "sample_points" => 81,
                        "tolerance_log10" => 0.01,
                        "hard" => "true",
                    ),
                )),
                bounds,
                "/target/output_feature/hard",
            ),
            (
                merge(behavior_target, Dict(
                    "shape" => Dict(
                        "class" => "monotonic",
                        "monotonicity" => "decreasing",
                        "sample_points" => 81,
                        "tolerance_log10" => 0.01,
                        "hard" => "true",
                    ),
                )),
                bounds,
                "/target/shape/hard",
            ),
            (
                Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                    "input_window" => Dict("input_log10" => Any[-1.0, 1.0], "hard" => "true"),
                ),
                bounds,
                "/target/input_window/hard",
            ),
            (
                merge(behavior_target, Dict(
                    "temporal_dynamics" => Dict("peak_width_seconds" => Dict("min" => 1.0), "hard" => "true"),
                )),
                bounds,
                "/target/temporal_dynamics/hard",
            ),
            (
                Dict("legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0])),
                merge(bounds, Dict("robustness" => Dict("min_chebyshev_radius" => 0.0, "hard" => "true"))),
                "/constraints/robustness/hard",
            ),
            (
                behavior_target,
                merge(bounds, Dict("dynamic_range" => Dict("min_fold_change" => 2.0, "sample_points" => 81, "hard" => "true"))),
                "/constraints/dynamic_range/hard",
            ),
            (
                two_step_target,
                merge(bounds, Dict("transitions" => Dict("min_spacing_decades" => 0.5, "hard" => "true"))),
                "/constraints/transitions/hard",
            ),
        ]

        for (target, constraints, expected_path) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => target,
                "constraints" => constraints,
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["kind"] == "hard_clause_contract" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Malformed top-level input_window clauses hard-block by nested path" begin
        cases = Any[
            (
                Dict("input_log10" => Any[-1.0, 1.0], "hard" => false, "bad/key~x" => true),
                "/target/input_window/bad~1key~0x",
                "unsupported_input_window_key",
            ),
            (
                Dict("input_log10" => Any[-1.0], "hard" => false),
                "/target/input_window/input_log10",
                "input_window_contract",
            ),
            (
                Dict("input_log10" => Any[-1.0, 1.0], "min_spacing_decades" => true, "hard" => false),
                "/target/input_window/min_spacing_decades",
                "input_window_contract",
            ),
            (
                Dict("operating_points_log10" => Any[-0.5, 0.5], "hard" => false),
                "/target/input_window/operating_points_log10",
                "input_window_contract",
            ),
            (
                Dict("input_log10" => Any[-1.0, 1.0], "operating_points_log10" => Any[-0.5, true], "hard" => false),
                "/target/input_window/operating_points_log10",
                "input_window_contract",
            ),
        ]

        for (input_window, expected_path, expected_kind) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                    "input_window" => input_window,
                ),
                "constraints" => Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                ),
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["kind"] == expected_kind &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Multi-step behavior spec gets an exact path-polytope recommendation" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "manual_config"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test screen["designable"] === true
        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["inp"] == "tA"
        @test card["out"] == "B"
        @test card["certificate_grade"] == "exact-union-siso-rop-path"
        @test card["evidence_grade"] == "enforced_exact"
        @test !haskey(card, "tunability_score")
        @test card["parameter_recommendation"]["theta_star"]["status"] == "computed"
        @test haskey(card["parameter_recommendation"]["theta_star"], "background_log_qK")
        @test haskey(card["parameter_recommendation"]["theta_star"], "background_qK_symbols")
        @test card["parameter_recommendation"]["theta_star"]["log_qK"] ==
              card["parameter_recommendation"]["theta_star"]["background_log_qK"]
        @test card["constraints"]["bounds_intersection_verified"] === true
        @test any(item -> item["path"] == "/target/behavior_spec/program" &&
                          item["kind"] == "reaction_order_program_feasible_region" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "verified_recommendations_available"
    end

    @testset "Hard unsupported quantitative constraints block verified recommendations" begin
        clauses = [
            (
                Dict("dynamic_range" => Dict("min_fold_change" => 10.0, "hard" => true)),
                "/constraints/dynamic_range/sample_points",
            ),
            (
                Dict("transitions" => Dict("min_spacing_decades" => 0.5, "hard" => true)),
                "/constraints/transitions/min_spacing_decades",
            ),
        ]
        for (constraints, expected_path) in clauses
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                ),
                "constraints" => constraints,
                "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Condition number max is a recognized but currently unsupported robustness spec" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "manual_config"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0]),
                ),
            ),
            "constraints" => Dict(
                "robustness" => Dict("condition_number_max" => 100.0),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness/condition_number_max" &&
                          item["kind"] == "parameter_to_breakpoint_condition_number" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none" &&
                          occursin("unique parameter-to-breakpoint mapping", item["reason"]),
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/constraints/robustness/condition_number_max" &&
                           item["support_level"] == "enforced_exact",
                   screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"

        soft_spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "manual_config"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0]),
                ),
            ),
            "constraints" => Dict(
                "robustness" => Dict{String, Any}(
                    "condition_number_max" => 100.0,
                    "hard" => false,
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 4, "max_screened" => 4, "max_exact_placements" => 3),
        )
        _with_test_parameter_bounds!(soft_spec)
        soft_screen = BEB.design_screen_from_spec(soft_spec)

        @test !isempty(soft_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness/condition_number_max" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false,
                  soft_screen["constraint_audit"])
        @test all(card -> !haskey(card["metrics"], "condition_number"),
                  soft_screen["verified_recommendations"])
    end

    @testset "Min Chebyshev radius is enforced, not only recorded" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "robustness" => Dict("min_chebyshev_radius" => 1.0e6),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test screen["designable"] === true
        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness/min_chebyshev_radius" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/parameter_bounds" &&
                          item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
    end

    @testset "Malformed min Chebyshev radius hard-blocks without crashing" begin
        for bad_value in Any[true, Inf, -1.0, "1.0"]
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                ),
                "constraints" => Dict(
                    "robustness" => Dict("min_chebyshev_radius" => bad_value),
                ),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 4,
                    "max_screened" => 4,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/robustness/min_chebyshev_radius" &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true &&
                              item["stage"] == "compile" &&
                              item["solver"] == "none",
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Legacy min tunable volume alias is audited and enforced as lower bound" begin
        base_spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        small_spec = deepcopy(base_spec)
        small_spec["constraints"]["robustness"] = Dict("min_tunable_volume" => 0.0)
        small_screen = BEB.design_screen_from_spec(small_spec)

        @test !isempty(small_screen["verified_recommendations"])
        small_card = first(small_screen["verified_recommendations"])
        @test small_card["metrics"]["tunable_volume"] > 0.0
        @test small_card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound"
        @test small_card["metrics"]["tunable_volume_dimension"] == length(small_card["parameter_recommendation"]["theta_star"]["log_qK"])
        @test small_card["metrics"]["tunable_volume_units"] == "log10_qK_euclidean_ball"
        @test small_card["constraints"]["min_tunable_volume"] == 0.0
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume" &&
                          item["kind"] == "tunable_volume_lower_bound" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "candidate_filter" &&
                          item["solver"] == "exact-union-siso-rop" &&
                          occursin("Chebyshev-ball volume lower bound", item["reason"]),
                  small_screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume" &&
                          item["support_level"] == "enforced_exact",
                  small_card["constraints"]["supported_constraints"])

        huge_spec = deepcopy(base_spec)
        huge_spec["constraints"]["robustness"] = Dict("min_tunable_volume" => 1.0e99)
        huge_screen = BEB.design_screen_from_spec(huge_spec)

        @test huge_screen["designable"] === true
        @test isempty(huge_screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume" &&
                          item["support_level"] == "enforced_exact",
                  huge_screen["constraint_audit"])
    end

    @testset "Explicit min tunable volume lower bound is audited, enforced, and labeled" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "robustness" => Dict("min_tunable_volume_lower_bound" => 0.0),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["metrics"]["tunable_volume"] > 0.0
        @test card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound"
        @test card["constraints"]["min_tunable_volume_lower_bound"] == 0.0
        @test !haskey(card["constraints"], "min_tunable_volume")
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume_lower_bound" &&
                          item["kind"] == "tunable_volume_lower_bound" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "candidate_filter" &&
                          item["solver"] == "exact-union-siso-rop" &&
                          occursin("Chebyshev-ball volume lower bound", item["reason"]),
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume_lower_bound" &&
                          item["kind"] == "tunable_volume_lower_bound" &&
                          item["support_level"] == "enforced_exact",
                  card["constraints"]["supported_constraints"])
    end

    @testset "Canonical and legacy tunable volume fields cannot be specified together" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "robustness" => Dict(
                    "min_tunable_volume_lower_bound" => 0.0,
                    "min_tunable_volume" => 1.0e99,
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 4,
                "max_screened" => 4,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/robustness/min_tunable_volume" &&
                          item["kind"] == "tunable_volume_lower_bound_alias" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none" &&
                          occursin("Do not specify both", item["reason"]),
                  screen["constraint_audit"])
        @test !any(item -> item["path"] == "/constraints/robustness/min_tunable_volume" &&
                           item["kind"] == "tunable_volume_lower_bound" &&
                           item["support_level"] == "enforced_exact",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Chebyshev ball volume lower bound has known unit-ball values" begin
        @test BEB._design_unit_ball_volume(0) == 0.0
        @test BEB._design_unit_ball_volume(1) == 2.0
        @test isapprox(BEB._design_unit_ball_volume(2), pi; rtol = 1.0e-12)
        @test isapprox(BEB._design_unit_ball_volume(3), 4.0 * pi / 3.0; rtol = 1.0e-12)
    end

    @testset "Verified ranking uses tunable volume and ignores unavailable proxy prefs" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 6,
                "max_screened" => 6,
                "max_exact_placements" => 6,
            ),
            "ranking_policy" => Dict("prefer" => Any["tunable_volume"]),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        volumes = [Float64(card["metrics"]["tunable_volume"]) for card in screen["verified_recommendations"]]
        @test all(volumes[i] >= volumes[i + 1] - 1.0e-12 for i in 1:(length(volumes) - 1))
        @test all(card -> card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound",
                  screen["verified_recommendations"])

        missing_pref_spec = deepcopy(spec)
        missing_pref_spec["ranking_policy"] = Dict("prefer" => Any["condition_number", "sampled_robustness", "tunable_volume"])
        missing_pref_screen = BEB.design_screen_from_spec(missing_pref_spec)

        @test !isempty(missing_pref_screen["verified_recommendations"])
        @test all(card -> !haskey(card["metrics"], "condition_number") &&
                          !haskey(card["metrics"], "sampled_robustness"),
                  missing_pref_screen["verified_recommendations"])
    end

    @testset "Unavailable ranking preferences are audited by nested path" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 3,
                "max_screened" => 3,
                "max_exact_placements" => 3,
            ),
            "ranking_policy" => Dict(
                "prefer" => Any["condition_number", "sampled_robustness", "tunable_volume"],
            ),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/1" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/2" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] != "unsupported" &&
                          item["stage"] == "ranking",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] != "blocked_by_unsupported_hard_clause"
    end

    @testset "Conditional ranking preferences require metric-producing constraints" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 3,
                "max_screened" => 3,
                "max_exact_placements" => 3,
            ),
            "ranking_policy" => Dict("prefer" => Any["dynamic_range", "transition_spacing"]),
        )

        _with_test_parameter_bounds!(spec)
        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        @test all(card -> !haskey(card["metrics"], "dynamic_range") &&
                          !haskey(card["metrics"], "transition_spacing"),
                  screen["verified_recommendations"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/1" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] != "blocked_by_unsupported_hard_clause"

        bad_dynamic_spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
            ),
            "constraints" => Dict(
                "dynamic_range" => Dict(
                    "min_fold_change" => 10.0,
                    "sample_points" => 81,
                    "max_slope" => 2.0,
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 3, "max_screened" => 3, "max_exact_placements" => 3),
            "ranking_policy" => Dict("prefer" => Any["dynamic_range"]),
        )
        bad_dynamic_screen = BEB.design_screen_from_spec(bad_dynamic_spec)

        @test any(item -> item["path"] == "/constraints/dynamic_range/max_slope" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  bad_dynamic_screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  bad_dynamic_screen["constraint_audit"])
        @test bad_dynamic_screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"

        bad_transition_spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "agent_design"),
            "target" => Dict(
                "behavior_spec" => Dict(
                    "input" => "tA",
                    "output" => "B",
                    "program" => Any[
                        Dict("kind" => "reaction_order", "value" => 0.0),
                        Dict("kind" => "reaction_order", "value" => -1.0),
                    ],
                    "input_window" => Dict("input_log10" => Any[-6.0, 6.0], "hard" => true),
                ),
            ),
            "constraints" => Dict(
                "transitions" => Dict(
                    "min_spacing_decades" => 0.5,
                    "max_jitter_decades" => 0.1,
                ),
            ),
            "candidate_budget" => Dict("max_recommended" => 3, "max_screened" => 3, "max_exact_placements" => 3),
            "ranking_policy" => Dict("prefer" => Any["transition_spacing"]),
        )
        bad_transition_screen = BEB.design_screen_from_spec(bad_transition_spec)

        @test any(item -> item["path"] == "/constraints/transitions/max_jitter_decades" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true,
                  bad_transition_screen["constraint_audit"])
        @test any(item -> item["path"] == "/ranking_policy/prefer/0" &&
                          item["kind"] == "ranking_preference" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === false &&
                          item["stage"] == "ranking" &&
                          item["solver"] == "none",
                  bad_transition_screen["constraint_audit"])
        @test bad_transition_screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Malformed tunable volume lower-bound fields hard-block without crashing" begin
        for (field, path) in (
            ("min_tunable_volume_lower_bound", "/constraints/robustness/min_tunable_volume_lower_bound"),
            ("min_tunable_volume", "/constraints/robustness/min_tunable_volume"),
        )
            for bad_value in Any[true, Inf, -1.0, "1.0"]
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                ),
                "constraints" => Dict(
                    "robustness" => Dict(field => bad_value),
                ),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 4,
                    "max_screened" => 4,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true &&
                              item["stage"] == "compile" &&
                              item["solver"] == "none",
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
            end
        end
    end

    @testset "Exact union SISO RO computes theta star" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "by_class" => Dict(
                        "kd" => Any[-3.0, 3.0],
                        "total" => Any[-3.0, 3.0],
                    ),
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 5,
                "max_screened" => 12,
                "max_exact_placements" => 3,
            ),
            "ranking_policy" => Dict("prefer" => Any["certificate_grade", "chebyshev_radius", "complexity"]),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["certificate_grade"] == "exact-union-siso-rop"
        @test card["evidence_grade"] == "enforced_exact"
        @test card["screen_status"] == "verified_exact"
        @test card["pass"] === true
        @test card["parameter_recommendation"]["theta_star"]["status"] == "computed"
        @test card["parameter_recommendation"]["theta_star"]["source"] == "feasible_region_chebyshev"
        @test card["metrics"]["chebyshev_radius_source"] == "theta_union_cell"
        @test card["metrics"]["tunable_volume"] > 0.0
        @test card["metrics"]["tunable_volume_source"] == "chebyshev_ball_lower_bound"
        @test card["metrics"]["tunable_volume_dimension"] == length(card["parameter_recommendation"]["theta_star"]["log_qK"])
        @test card["constraints"]["bounds_intersection_verified"] === true
    end

    @testset "Legacy target nested clauses cannot hide unsupported requirements" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict(
                    "target_kind" => "exact",
                    "target" => Any[1.0],
                    "shape" => Dict(
                        "class" => "bell_shaped",
                        "min_prominence_log10" => 0.2,
                        "sample_points" => 51,
                        "tolerance_log10" => 0.01,
                        "hard" => true,
                    ),
                ),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "by_class" => Dict(
                        "kd" => Any[-3.0, 3.0],
                        "total" => Any[-3.0, 3.0],
                    ),
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 5,
                "max_screened" => 12,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test isempty(screen["verified_recommendations"])
        @test any(item -> item["path"] == "/target/legacy_target/shape" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none",
                  screen["constraint_audit"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
    end

    @testset "Parameter bounds by class are exact solver-backed and echoed" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "by_class" => Dict(
                        "kd" => Any[-2.0, 2.0],
                        "total" => Any[-1.0, 1.0],
                    ),
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 5,
                "max_screened" => 12,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !isempty(screen["verified_recommendations"])
        card = first(screen["verified_recommendations"])
        @test card["constraints"]["parameter_bounds_verified"] === true
        @test card["constraints"]["parameter_bounds_source"] == "declared_spec"
        @test card["constraints"]["parameter_bounds"]["basis"] == "log10_qK"
        @test card["constraints"]["parameter_bounds"]["by_class"]["kd"] == [-2.0, 2.0]
        @test card["constraints"]["parameter_bounds"]["by_class"]["total"] == [-1.0, 1.0]
        @test eltype(card["constraints"]["parameter_bounds"]["by_class"]["kd"]) == Float64
        @test eltype(card["constraints"]["parameter_bounds"]["by_class"]["total"]) == Float64
        @test any(item -> item["path"] == "/constraints/parameter_bounds/by_class/kd" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "feasible_region" &&
                          item["solver"] == "exact-union-siso-rop",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/parameter_bounds/by_class/total" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "feasible_region" &&
                          item["solver"] == "exact-union-siso-rop",
                  screen["constraint_audit"])
        @test any(item -> item["path"] == "/constraints/parameter_bounds/by_class/kd" &&
                          item["support_level"] == "enforced_exact",
                  card["constraints"]["supported_constraints"])
        @test any(item -> item["path"] == "/constraints/parameter_bounds/by_class/total" &&
                          item["support_level"] == "enforced_exact",
                  card["constraints"]["supported_constraints"])
        theta = card["parameter_recommendation"]["theta_star"]
        @test all(10.0^-2 <= value <= 10.0^2 for value in theta["kd"] if isfinite(value))
        @test all(10.0^-1 <= value <= 10.0^1 for value in values(theta["totals"]))
    end

    @testset "Malformed parameter bounds hard-block by nested path without crashing" begin
        cases = Any[
            (
                Dict("basis" => "linear_qK", "by_class" => Dict("kd" => Any[-3.0, 3.0], "total" => Any[-3.0, 3.0])),
                "/constraints/parameter_bounds/basis",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict("kd" => Any[true, 3.0], "total" => Any[-3.0, 3.0])),
                "/constraints/parameter_bounds/by_class/kd",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict("kd" => Any[-3.0, 3.0], "total" => Any[3.0, -3.0])),
                "/constraints/parameter_bounds/by_class/total",
            ),
            (
                Dict("basis" => "log10_qK", "kd_log10" => Any[-3.0, "tight"], "total_log10" => Any[-3.0, 3.0]),
                "/constraints/parameter_bounds/kd_log10",
            ),
            (
                Dict(
                    "basis" => "log10_qK",
                    "by_class" => Dict("kd" => Any[-1.0, 1.0]),
                    "kd_log10" => Any[-3.0, 3.0],
                ),
                "/constraints/parameter_bounds/kd_log10",
            ),
            (
                Dict(
                    "basis" => "log10_qK",
                    "by_class" => Dict("total" => Any[-1.0, 1.0]),
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "/constraints/parameter_bounds/total_log10",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict("kd" => Any[-Inf, 3.0], "total" => Any[-3.0, 3.0])),
                "/constraints/parameter_bounds/by_class/kd",
            ),
            (
                Dict("basis" => "log10_qK", "default" => Any[true, 3.0], "by_class" => Dict("kd" => Any[-3.0, 3.0])),
                "/constraints/parameter_bounds/default",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict("kd" => Any[-3.0, 3.0], "junk" => Any[true, 3.0])),
                "/constraints/parameter_bounds/by_class/junk",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict("kd" => Any[-3.0, 3.0], "bad/key~x" => Any[true, 3.0])),
                "/constraints/parameter_bounds/by_class/bad~1key~0x",
            ),
            (
                Dict{String, Any}(),
                "/constraints/parameter_bounds",
            ),
            (
                Dict("basis" => "log10_qK"),
                "/constraints/parameter_bounds",
            ),
            (
                Dict("basis" => "log10_qK", "by_class" => Dict{String, Any}()),
                "/constraints/parameter_bounds",
            ),
        ]

        for (bounds, expected_path) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                ),
                "constraints" => Dict("parameter_bounds" => bounds),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_recommended" => 5,
                    "max_screened" => 12,
                    "max_exact_placements" => 3,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "Omitted parameter bounds block verified recommendations without assuming a default domain" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_recommended" => 5,
                "max_screened" => 12,
                "max_exact_placements" => 3,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test !haskey(screen["designability_spec_normalized"]["constraints"], "parameter_bounds")
        @test screen["designable"] === true
        @test isempty(screen["verified_recommendations"])
        @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        @test !isempty(screen["minimal_certificates"])
        @test any(item -> item["path"] == "/constraints/parameter_bounds" &&
                          item["kind"] == "qk_box_bounds_required" &&
                          item["support_level"] == "unsupported" &&
                          item["hard"] === true &&
                          item["stage"] == "compile" &&
                          item["solver"] == "none" &&
                          occursin("require explicit constraints.parameter_bounds", item["reason"]),
                  screen["constraint_audit"])
    end

    @testset "Network structural constraints filter candidates before recommendation" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
                "network" => Dict(
                    "max_reactions" => 1,
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_verified_recommendations" => 3,
                "max_screened" => 12,
                "max_exact_placements" => 5,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test screen["n_matches"] > 0
        @test all(card -> card["complexity"]["r"] <= 1, screen["screened_candidates"])
        @test all(card -> card["complexity"]["r"] <= 1, screen["verified_recommendations"])
        @test any(item -> item["path"] == "/constraints/network/max_reactions" &&
                          item["kind"] == "network_complexity_bound" &&
                          item["support_level"] == "enforced_exact" &&
                          item["stage"] == "candidate_filter" &&
                          item["solver"] == "design_index",
                  screen["constraint_audit"])
        if !isempty(screen["verified_recommendations"])
            card = first(screen["verified_recommendations"])
            @test any(item -> item["path"] == "/constraints/network/max_reactions" &&
                              item["support_level"] == "enforced_exact",
                      card["constraints"]["supported_constraints"])
        end
    end

    @testset "Malformed or unknown network constraints hard-block verified recommendations" begin
        cases = Any[
            (Dict("bad/key~x" => 1), "/constraints/network/bad~1key~0x"),
            (Dict("max_reactions" => true), "/constraints/network/max_reactions"),
            (Dict("max_species" => 0), "/constraints/network/max_species"),
            (Dict("allow_near_minimal" => "false"), "/constraints/network/allow_near_minimal"),
        ]

        for (network, expected_path) in cases
            spec = Dict(
                "schema_version" => "bne-designability/v1.0.0",
                "source" => Dict("kind" => "hand_authored"),
                "target" => Dict(
                    "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
                ),
                "constraints" => Dict(
                    "parameter_bounds" => Dict(
                        "basis" => "log10_qK",
                        "kd_log10" => Any[-3.0, 3.0],
                        "total_log10" => Any[-3.0, 3.0],
                    ),
                    "network" => network,
                ),
                "candidate_budget" => Dict(
                    "mode" => "near_minimal",
                    "max_verified_recommendations" => 3,
                    "max_screened" => 12,
                    "max_exact_placements" => 5,
                ),
            )

            screen = BEB.design_screen_from_spec(spec)

            @test isempty(screen["verified_recommendations"])
            @test any(item -> item["path"] == expected_path &&
                              item["support_level"] == "unsupported" &&
                              item["hard"] === true,
                      screen["constraint_audit"])
            @test screen["screen_summary"]["verified_status"] == "blocked_by_unsupported_hard_clause"
        end
    end

    @testset "max_verified_recommendations limits verified recommendations" begin
        spec = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "hand_authored"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "exact", "target" => Any[1.0]),
            ),
            "constraints" => Dict(
                "parameter_bounds" => Dict(
                    "basis" => "log10_qK",
                    "kd_log10" => Any[-3.0, 3.0],
                    "total_log10" => Any[-3.0, 3.0],
                ),
            ),
            "candidate_budget" => Dict(
                "mode" => "near_minimal",
                "max_verified_recommendations" => 1,
                "max_screened" => 12,
                "max_exact_placements" => 5,
            ),
        )

        screen = BEB.design_screen_from_spec(spec)

        @test length(screen["verified_recommendations"]) == 1
        @test screen["screen_summary"]["verified_count"] == 1
        @test screen["recommended"] == screen["verified_recommendations"]
    end

    @testset "Legacy proxy evidence cleanup accepts symbol-key constraints" begin
        constraints = Dict{Symbol, Any}(
            :kd_bounds => Any[-3.0, 3.0],
            :total_bounds => Any[-3.0, 3.0],
            :min_dynamic_range => 0.0,
            :min_transition_spacing_decades => 0.0,
            :total_bounds_role => "forwarded_to_downstream_placer_not_used_by_proxy_screen",
            :supported_handles => Any["reaction_order_program", "kd_bounds", "atlas_volume", "robust_path_count"],
            :unsupported_handles => Any["temporal_dynamics"],
            :supported_constraints => Any[
                Dict(:path => "target", :kind => "reaction_order_program",
                     :support_level => "enforced", :stage => "atlas_match",
                     :solver => "design_index"),
                Dict(:path => "tuning.kd_bounds", :kind => "parameter_box",
                     :support_level => "proxy_only", :stage => "screen",
                     :solver => "design_screen_proxy"),
            ],
            :unsupported_constraints => Any[
                Dict(:path => "temporal_dynamics", :kind => "dynamics",
                     :support_level => "ignored", :stage => "screen",
                     :solver => "none"),
            ],
        )

        @test BEB._design_clear_legacy_proxy_constraint_evidence!(constraints) === constraints

        @test !haskey(constraints, :kd_bounds)
        @test !haskey(constraints, :total_bounds)
        @test !haskey(constraints, :min_dynamic_range)
        @test !haskey(constraints, :min_transition_spacing_decades)
        @test !haskey(constraints, :total_bounds_role)
        @test !haskey(constraints, :supported_handles)
        @test !haskey(constraints, :unsupported_handles)
        @test !haskey(constraints, :unsupported_constraints)
        @test isempty(constraints[:supported_constraints])
    end

    @testset "Parameter bounds schema matches backend supported keys" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        parameter_bounds = schema["properties"]["constraints"]["properties"]["parameter_bounds"]
        by_class = parameter_bounds["properties"]["by_class"]

        @test parameter_bounds["additionalProperties"] === false
        @test by_class["additionalProperties"] === false
        @test Set(String.(keys(parameter_bounds["properties"]))) == Set(["basis", "kd_log10", "total_log10", "by_class"])
        @test Set(String.(keys(by_class["properties"]))) == Set(["kd", "total"])
        @test any(branch -> "kd_log10" in String.(get(branch, "required", Any[])),
                  parameter_bounds["anyOf"])
        @test any(branch -> "total_log10" in String.(get(branch, "required", Any[])),
                  parameter_bounds["anyOf"])
        @test any(branch -> "by_class" in String.(get(branch, "required", Any[])),
                  parameter_bounds["anyOf"])
        @test any(branch -> "kd" in String.(get(branch, "required", Any[])),
                  by_class["anyOf"])
        @test any(branch -> "total" in String.(get(branch, "required", Any[])),
                  by_class["anyOf"])
        @test haskey(parameter_bounds, "allOf")
        @test any(rule -> haskey(rule, "not") &&
                          "kd_log10" in String.(get(rule["not"], "required", Any[])),
                  parameter_bounds["allOf"])
        @test any(rule -> haskey(rule, "not") &&
                          "total_log10" in String.(get(rule["not"], "required", Any[])),
                  parameter_bounds["allOf"])
    end

    @testset "Network constraint schema matches backend supported keys" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        network = schema["properties"]["constraints"]["properties"]["network"]

        @test network["additionalProperties"] === false
        @test Set(String.(keys(network["properties"]))) ==
              Set(["max_species", "max_reactions", "max_mu", "allow_near_minimal"])
        @test network["properties"]["max_species"]["minimum"] == 1
        @test network["properties"]["max_reactions"]["minimum"] == 0
        @test network["properties"]["max_mu"]["minimum"] == 1
        @test network["properties"]["allow_near_minimal"]["type"] == "boolean"
    end

    @testset "Transition order schema documents backend permutation contract" begin
        schema_path = joinpath(@__DIR__, "..", "..", "schemas", "designability-spec.schema.json")
        schema = JSON3.read(read(schema_path, String), Dict{String, Any})
        transitions = schema["properties"]["constraints"]["properties"]["transitions"]
        order = transitions["properties"]["order"]

        @test transitions["additionalProperties"] === false
        @test Set(String.(keys(transitions["properties"]))) == Set(["min_spacing_decades", "order", "hard"])
        @test order["type"] == "array"
        @test order["minItems"] == 1
        @test order["uniqueItems"] === true
        @test order["items"]["type"] == "integer"
        @test order["items"]["minimum"] == 0
        @test occursin("full 0-based integer permutation", order["description"])
    end

    @testset "Validation endpoint exposes compiler audit" begin
        good = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "target" => Dict("legacy_target" => Dict("target_kind" => "sign", "target" => "+-+")),
        )
        good_req = HTTP.Request("POST", "/api/validate_designability_spec", [], JSON3.write(good))
        good_res = BEB.router(good_req)
        @test good_res.status == 200
        parsed = JSON3.read(String(copy(good_res.body)))
        @test parsed["ok"] === true
        @test haskey(parsed, "constraint_audit")

        bad_req = HTTP.Request("POST", "/api/validate_designability_spec", [], JSON3.write(Dict("schema_version" => "wrong")))
        @test BEB.router(bad_req).status == 400
    end

    @testset "Validation and screening share candidate budget bounds" begin
        base = Dict(
            "schema_version" => "bne-designability/v1.0.0",
            "source" => Dict("kind" => "test_fixture"),
            "target" => Dict(
                "legacy_target" => Dict("target_kind" => "sign", "target" => "+"),
            ),
        )

        integral_float = merge(base, Dict(
            "candidate_budget" => Dict("max_screened" => 64.0),
        ))
        normalized = BEB.normalize_designability_spec(integral_float)
        @test normalized.candidate_budget["max_screened"] === 64
        @test !any(item -> item.path == "/candidate_budget/max_screened" &&
                          item.kind == "candidate_budget_contract",
                  normalized.audit)
        ok_req = HTTP.Request(
            "POST", "/api/validate_designability_spec", [], JSON3.write(integral_float))
        @test BEB.router(ok_req).status == 200

        over_limit = merge(base, Dict(
            "candidate_budget" => Dict("max_screened" => 65),
        ))
        over_normalized = BEB.normalize_designability_spec(over_limit)
        @test any(item -> item.path == "/candidate_budget/max_screened" &&
                          item.kind == "candidate_budget_contract" &&
                          item.support_level == "unsupported",
                  over_normalized.audit)
        for path in ("/api/validate_designability_spec", "/api/design_screen")
            req = HTTP.Request("POST", path, [], JSON3.write(over_limit))
            @test BEB.router(req).status == 422
        end
    end
end
