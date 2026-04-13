using JSON3
using BiocircuitsExplorerBackend
using Statistics
using Test

simple_network = Dict(
    "label" => "monomer_dimer",
    "reactions" => Any["A + B <-> AB"],
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["AB"],
)

candidate_networks = Any[
    simple_network,
    Dict(
        "label" => "hall_fail",
        "reactions" => Any[
            "A + B <-> AB",
            "A + B <-> P",
            "A + C <-> AC",
        ],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
    ),
    Dict(
        "label" => "alt_product",
        "reactions" => Any["A + B <-> P"],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["P"],
    ),
    Dict(
        "label" => "branch_without_ab",
        "reactions" => Any[
            "A + B <-> P",
            "A + C <-> AC",
        ],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["P"],
    ),
    Dict(
        "label" => "larger_support",
        "reactions" => Any[
            "A + B <-> AB",
            "A + C <-> AC",
        ],
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
    ),
]

eager_cfg = AtlasBehaviorConfig(
    path_scope = :robust,
    min_volume_mean = 0.01,
    deduplicate = true,
    keep_singular = true,
    keep_nonasymptotic = false,
    compute_volume = true,
    motif_zero_tol = 1e-6,
    include_path_records = true,
)

lazy_spec = Dict(
    "networks" => candidate_networks,
    "query" => Dict(
        "input_symbols" => Any["tA"],
        "output_symbols" => Any["AB"],
        "support_count_spec" => Dict(
            "allowed_species" => Any["AB"],
            "min_counts" => Dict("AB" => 1),
        ),
        "require_witness_feasible" => true,
        "limit" => 5,
    ),
    "refinement" => Dict(
        "enabled" => true,
        "top_k" => 1,
        "trials" => 1,
        "n_points" => 25,
        "include_traces" => false,
    ),
    "inverse_design" => Dict(
        "return_library" => true,
        "return_delta_atlas" => true,
    ),
)

eager_query = Dict(
    "input_symbols" => Any["tA"],
    "output_symbols" => Any["AB"],
    "support_count_spec" => Dict(
        "allowed_species" => Any["AB"],
        "min_counts" => Dict("AB" => 1),
    ),
    "require_witness_feasible" => true,
    "limit" => 5,
)

function measure_runs(f; repeats=2)
    samples = Float64[]
    last_value = nothing
    for _ in 1:repeats
        GC.gc()
        elapsed = @elapsed last_value = f()
        push!(samples, elapsed)
    end
    return Dict(
        "samples" => samples,
        "median_seconds" => median(samples),
        "min_seconds" => minimum(samples),
        "last_value" => last_value,
    )
end

# Warm up both workflows before measuring.
query_behavior_atlas(build_behavior_atlas(candidate_networks; behavior_config=eager_cfg), eager_query)
run_inverse_design_from_spec(lazy_spec)

eager_bench = measure_runs(() -> begin
    atlas = build_behavior_atlas(candidate_networks; behavior_config=eager_cfg)
    Dict(
        "atlas" => atlas,
        "query_result" => query_behavior_atlas(atlas, eager_query),
    )
end)
lazy_bench = measure_runs(() -> run_inverse_design_from_spec(lazy_spec))
eager_result = eager_bench["last_value"]
eager_atlas = eager_result["atlas"]
lazy_result = lazy_bench["last_value"]

query = BiocircuitsExplorerBackend.atlas_query_spec_from_raw(lazy_result["query"])
baseline_refinement = BiocircuitsExplorerBackend.refine_inverse_design_candidates(
    lazy_result["query_result"],
    BiocircuitsExplorerBackend.inverse_refinement_spec_from_raw(Dict(
        "enabled" => true,
        "top_k" => 1,
        "trials" => 1,
        "n_points" => 25,
        "include_traces" => false,
    )),
    query,
)

hall_spec = Dict(
    "networks" => Any[
        Dict(
            "label" => "hall_fail",
            "reactions" => Any[
                "A + B <-> AB",
                "A + B <-> P",
                "A + C <-> AC",
            ],
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
        ),
    ],
    "query" => Dict(
        "support_count_spec" => Dict(
            "allowed_species" => Any["AB", "P"],
            "min_counts" => Dict("AB" => 2, "P" => 1),
        ),
        "limit" => 5,
    ),
    "inverse_design" => Dict(
        "return_library" => true,
        "return_delta_atlas" => false,
    ),
)

hall_first = run_inverse_design_from_spec(hall_spec)
hall_second = run_inverse_design_from_spec(merge(hall_spec, Dict("library" => hall_first["library"])))
second_trace = hall_second["build_plan"]["candidate_traces"][1]
second_stages = second_trace["stages"]
reused_negative = any(stage -> stage["stage"] == "negative_certificate" && stage["status"] == "pruned", second_stages)

report = Dict(
    "lazy_vs_eager" => Dict(
        "eager_time_seconds" => eager_bench["median_seconds"],
        "lazy_time_seconds" => lazy_bench["median_seconds"],
        "eager_samples" => eager_bench["samples"],
        "lazy_samples" => lazy_bench["samples"],
        "eager_path_records" => length(eager_atlas["path_records"]),
        "lazy_delta_path_records" => length(lazy_result["delta_atlas"]["path_records"]),
        "lazy_materialized_path_records" => length(lazy_result["library"]["path_records"]),
        "eager_result_count" => eager_result["query_result"]["result_count"],
        "lazy_result_count" => lazy_result["query_result"]["result_count"],
    ),
    "refinement_compare" => Dict(
        "polytope_guided_score" => lazy_result["refinement_result"]["best_candidate"]["refinement_score"],
        "random_baseline_score" => baseline_refinement["best_candidate"]["refinement_score"],
        "polytope_seed_source" => lazy_result["refinement_result"]["best_candidate"]["best_trial"]["seed_source"],
    ),
    "hard_negative_reuse" => Dict(
        "first_negative_count" => length(hall_first["library"]["negative_certificate_store"]),
        "second_negative_count" => length(hall_second["library"]["negative_certificate_store"]),
        "reused_negative_certificate" => reused_negative,
        "second_build_trace" => second_trace,
    ),
)

@test eager_bench["median_seconds"] > lazy_bench["median_seconds"]
@test length(eager_atlas["path_records"]) > length(lazy_result["delta_atlas"]["path_records"])
@test eager_result["query_result"]["result_count"] == lazy_result["query_result"]["result_count"] == 1
@test lazy_result["delta_atlas"]["summary_first"] == true
@test lazy_result["refinement_result"]["best_candidate"]["refinement_score"] >= baseline_refinement["best_candidate"]["refinement_score"]
@test lazy_result["refinement_result"]["best_candidate"]["best_trial"]["seed_source"] != "random_fallback"
@test length(hall_first["library"]["negative_certificate_store"]) >= 1
@test reused_negative

println(JSON3.pretty(report))
