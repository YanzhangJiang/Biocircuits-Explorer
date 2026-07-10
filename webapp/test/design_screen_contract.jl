using Test
using HTTP
using JSON3
using BiocircuitsExplorerBackend

const BEB = BiocircuitsExplorerBackend

@testset "Design Screen Contract" begin
    screen = BEB.design_screen("sign", "+-+")
    @test screen["schema_version"] == "bne-design-screen/v0.3.0"
    @test screen["designable"] === true
    @test haskey(screen, "designability_spec_normalized")
    @test haskey(screen, "constraint_audit")
    @test haskey(screen, "verified_recommendations")
    @test haskey(screen, "screened_candidates")
    @test isempty(screen["verified_recommendations"])
    @test screen["recommended"] == screen["verified_recommendations"]
    @test !isempty(screen["screened_candidates"])
    @test all(card -> get(card, "evidence_grade", "") == "proxy_only" &&
                      card["pass"] === false,
              screen["screened_candidates"])
    @test !isempty(screen["minimal_certificates"])

    rec = first(screen["screened_candidates"])
    for key in ("nid", "inp", "out", "complexity", "constraints", "metrics",
                "parameter_recommendation", "certificate_grade",
                "active_failures", "agent_handoff")
        @test haskey(rec, key)
    end
    @test rec["out"] in first(BEB._design_canonical_species(rec["nid"]))
    @test rec["parameter_recommendation"]["theta_star"]["status"] == "not_computed"
    @test rec["parameter_recommendation"]["theta_star"]["source_type"] == "proxy"
    @test rec["constraints"]["bounds_intersection_verified"] === false
    @test !haskey(rec, "tunability_score")
    @test !haskey(rec["constraints"], "kd_bounds")
    @test !haskey(rec["constraints"], "total_bounds")
    @test !haskey(rec["metrics"], "tunable_volume")
    @test !haskey(rec["metrics"], "transition_spacing")
    @test !haskey(rec["metrics"], "condition_number")
    @test haskey(rec["metrics"], "ranking_margin_proxy")
    @test haskey(rec["metrics"], "atlas_volume_proxy")
    @test get(rec["parameter_recommendation"]["theta_star"], "log_qK", Any[]) == Any[]
    @test get(rec["parameter_recommendation"]["theta_star"], "kd", Any[]) == Any[]
    @test isempty(get(rec["parameter_recommendation"]["theta_star"], "totals", Dict()))

    all_screen = BEB.design_screen("sign", "+-+";
        candidate_budget=Dict("mode" => "all_matches", "max_recommended" => 3, "max_screened" => 3))
    @test all_screen["designability_spec_normalized"]["candidate_budget"]["mode"] == "all_matches"
    @test all_screen["eligible_count"] >= screen["eligible_count"]
    @test length(all_screen["screened_candidates"]) <= 3
    @test BEB._design_exact_placement_budget(Dict(
        "exact_placement_budget" => Dict("max_exact_placements" => 2)
    ))["max_exact_placements"] == 2

    non_bound_tuning_screen = BEB.design_screen("exact", [1.0];
        candidate_budget=Dict("max_recommended" => 5, "max_exact_placements" => 3),
        tuning=Dict("min_chebyshev_radius" => 0.0))
    @test !haskey(non_bound_tuning_screen["designability_spec_normalized"]["constraints"], "parameter_bounds")
    @test isempty(non_bound_tuning_screen["verified_recommendations"])
    @test any(item -> item["path"] == "/constraints/parameter_bounds" &&
                      item["kind"] == "qk_box_bounds_required" &&
                      item["support_level"] == "unsupported",
              non_bound_tuning_screen["constraint_audit"])

    unordered = [
        (nid = "z", inp = "tA", out = "A", vol_max = 0.1, robust_paths = 1, vol_sum = 0.1, r = 1, d = 1),
        (nid = "a", inp = "tA", out = "A", vol_max = 0.1, robust_paths = 1, vol_sum = 0.1, r = 1, d = 1),
    ]
    unique_keys = [BEB._design_candidate_key(rec) for rec in BEB._design_unique_records(unordered)]
    @test unique_keys == sort(unique_keys)

    exact_screen = BEB.design_screen("exact", [1.0];
        candidate_budget=Dict("max_recommended" => 5, "max_exact_placements" => 3),
        tuning=Dict("kd_bounds" => [-3.0, 3.0], "min_chebyshev_radius" => 0.0),
        ranking_policy=Dict("prefer" => ["certificate_grade", "chebyshev_radius", "complexity"]))
    exact_cards = [card for card in exact_screen["verified_recommendations"]
                   if card["certificate_grade"] == "exact-union-siso-rop"]
    @test !isempty(exact_cards)
    exact = first(exact_cards)
    @test exact["parameter_recommendation"]["theta_star"]["status"] == "computed"
    @test exact["parameter_recommendation"]["theta_star"]["source"] == "feasible_region_chebyshev"
    @test exact["constraints"]["bounds_intersection_verified"] === true
    @test any(item -> item["solver"] == "exact-union-siso-rop",
              exact["constraints"]["supported_constraints"])
    @test exact["metrics"]["chebyshev_radius_source"] == "theta_union_cell"
    @test exact["screen_status"] == "verified_exact"
    @test !haskey(exact, "tunability_score")
    @test first(exact["certificate_stack"])["grade"] == "exact-union-siso-rop"
    @test exact_screen["screen_summary"]["verified_status"] == "verified_recommendations_available"

    no_match = BEB.design_screen_from_spec(Dict(
        "schema_version" => "bne-designability/v1.0.0",
        "source" => Dict("kind" => "test_fixture"),
        "target" => Dict(
            "legacy_target" => Dict(
                "target_kind" => "exact",
                "target" => Any[12_345.0],
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
            "max_screened" => 4,
            "max_exact_placements" => 0,
        ),
    ))
    @test no_match["designable"] === false
    @test no_match["n_matches"] == 0
    @test no_match["screen_summary"]["verified_status"] == "not_designable"

    spec_screen = BEB.design_screen("sign", "+-+";
        designability_spec=Dict("target_kind" => "sign", "target" => "+-+",
                                "temporal_dynamics" => Dict("peak_width" => "wide")))
    @test isempty(spec_screen["verified_recommendations"])
    @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width" &&
                      item["kind"] == "unsupported_temporal_dynamics_key" &&
                      item["support_level"] == "unsupported",
              spec_screen["constraint_audit"])

    @test_throws ErrorException BEB.design_search("unknown", [1.0])
    bad_kind_req = HTTP.Request("POST", "/api/design_screen", [],
        JSON3.write(Dict("target_kind" => "unknown", "target" => Any[1.0])))
    @test BEB.router(bad_kind_req).status == 400
    bad_spec_req = HTTP.Request("POST", "/api/design_screen", [],
        JSON3.write(Dict("designability_spec" => "not-an-object", "target" => "+-+")))
    @test BEB.router(bad_spec_req).status == 400
    top_level_spec_req = HTTP.Request("POST", "/api/design_screen", [],
        JSON3.write(Dict("target_kind" => "sign", "target" => "+-+",
                         "temporal_dynamics" => Dict("peak_width" => "wide"))))
    top_level_spec = JSON3.read(String(copy(BEB.router(top_level_spec_req).body)))
    @test isempty(top_level_spec["verified_recommendations"])
    @test any(item -> item["path"] == "/target/temporal_dynamics/peak_width" &&
                      item["kind"] == "unsupported_temporal_dynamics_key" &&
                      item["support_level"] == "unsupported",
              top_level_spec["constraint_audit"])
end
