#!/usr/bin/env julia

using JSON3
using BiocircuitsExplorerBackend

const BEB = BiocircuitsExplorerBackend
const ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const FIXTURE_PATH = joinpath(@__DIR__, "cat_fixed_topology.json")
const DEFAULT_OUTPUT = joinpath(@__DIR__, "cat_fixed_topology_results.json")

function materialize_json(value)
    return JSON3.read(JSON3.write(value), Dict{String, Any})
end

function benchmark_inputs(fixture)
    rules = String.(collect(fixture["network"]["rules"]))
    kd = Float64.(collect(fixture["reference"]["kd"]))
    input_sym = String(fixture["network"]["input"])
    output_sym = String(fixture["network"]["output"])
    network = network_ir_from_legacy(
        rules, kd; label=String(fixture["id"]),
        input_symbols=[input_sym], output_symbols=[output_sym])
    network_hash = network_ir_hash(network)
    reference = Dict{String, Any}(
        "network_ir_hash" => network_hash,
        "operating_points_log10" =>
            Float64.(collect(fixture["reference"]["operating_points_log10"])),
        "kd" => kd,
        "totals" => Dict{String, Float64}(
            String(key) => Float64(value)
            for (key, value) in pairs(fixture["reference"]["totals"])),
        "path_identity" => "path:$(Int(fixture["reference"]["path_idx"]))",
    )
    reference["reference_hash"] = BEB._canonical_hash(
        BEB._rop_shape_reference_payload(reference))
    program = Float64.(collect(fixture["target"]["reaction_order_program"]))
    window = Float64.(collect(fixture["target"]["input_window_log10"]))
    spacing = Float64(fixture["target"]["min_spacing_decades"])
    bounds = materialize_json(fixture["parameter_bounds"])
    spec = Dict{String, Any}(
        "schema_version" => DESIGNABILITY_SPEC_VERSION,
        "source" => Dict(
            "kind" => "test_fixture",
            "provenance" => Dict(
                "benchmark" => String(fixture["id"]),
                "fixture_artifact_id" => String(fixture["reference"]["artifact_id"]),
            ),
        ),
        "target" => Dict("behavior_spec" => Dict(
            "input" => input_sym,
            "output" => output_sym,
            "feature_space" => "reaction_order",
            "program" => [Dict{String, Any}(
                "kind" => "reaction_order", "operator" => "=",
                "value" => value, "hard" => true,
            ) for value in program],
            "input_window" => Dict{String, Any}(
                "input_log10" => window,
                "min_spacing_decades" => spacing,
                "hard" => true,
            ),
        )),
        "constraints" => Dict{String, Any}(
            "parameter_bounds" => bounds,
            "transitions" => Dict{String, Any}(
                "min_spacing_decades" => spacing, "hard" => true),
        ),
        "candidate_budget" => Dict{String, Any}(),
        "ranking_policy" => Dict("verified_only" => true),
        "audit_policy" => Dict(
            "unsupported" => "block_if_hard",
            "path_format" => "json_pointer",
            "include_supported" => true,
        ),
    )
    return (; rules, kd, input_sym, output_sym, network,
            network_dict=network_ir_to_dict(network), network_hash,
            reference, program, window, spacing, bounds, spec)
end

function canonical_intent(raw)
    intent = materialize_json(raw)
    pop!(intent, "candidate_magnitudes_log10", nothing)
    if intent["kind"] == "broaden"
        intent["shared_magnitude"] = true
    elseif intent["kind"] == "translate_group"
        intent["shared_shift"] = true
    end
    return intent
end

function optimization_request(inputs, intent)
    return Dict{String, Any}(
        "schema_version" => ROP_SHAPE_OPTIMIZE_REQUEST_VERSION,
        "network" => inputs.network_dict,
        "expected_network_ir_hash" => inputs.network_hash,
        "designability_spec" => inputs.spec,
        "reference" => inputs.reference,
        "edit_intent" => intent,
        "optimization" => Dict(
            "minimum_parameter_margin" => 0.01,
            "effect_tolerance" => 0.02,
        ),
        "work_budget" => Dict(
            "max_paths" => 2000,
            "max_cells" => 10_000,
            "max_replays" => 1,
            "require_exhaustive" => true,
        ),
        "replay" => Dict(
            "input_window_log10" => inputs.window,
            "sample_points" => Int(inputs_fixture_sample_points()),
            "require_complete" => true,
            "store_curve" => true,
            "metrics" => [Dict(
                "kind" => "two_peak",
                "min_prominence_log10" => 0.5,
            )],
        ),
    )
end

const _FIXTURE_SAMPLE_POINTS = Ref(281)
inputs_fixture_sample_points() = _FIXTURE_SAMPLE_POINTS[]

function apply_candidate(reference::Vector{Float64}, intent, magnitude::Float64)
    points = copy(reference)
    kind = String(intent["kind"])
    if kind == "broaden"
        for key in ("left_span_steps", "right_span_steps")
            left, right = Int.(intent[key]) .+ 1
            points[left] -= magnitude / 2
            points[right] += magnitude / 2
        end
    elseif kind == "separate" || kind == "widen_center"
        left, right = Int.(intent["steps"]) .+ 1
        points[left] -= magnitude / 2
        points[right] += magnitude / 2
    elseif kind == "translate_group"
        for index in Int.(intent["group_steps"]) .+ 1
            points[index] += magnitude
        end
    else
        error("unsupported frozen-grid intent: $kind")
    end
    return points
end

function replay_cell(inputs, cell)
    totals = Dict{Symbol, Float64}(
        Symbol(String(key)) => Float64(value)
        for (key, value) in pairs(cell.totals))
    curve = BEB.placer_dose_response(
        inputs.rules, Float64.(cell.kd), totals, Symbol(inputs.input_sym),
        inputs.output_sym; param_min=inputs.window[1], param_max=inputs.window[2],
        n_points=inputs_fixture_sample_points())
    metrics = analyze_two_peak_curve(
        Float64.(curve["param_values"]),
        Float64[Float64(row[1]) for row in curve["output_traj"]],
        curve["valid"]; min_prominence_log10=0.5)
    complete = curve["partial"] === false &&
        all(value -> value === true, curve["valid"]) &&
        metrics["complete"] === true
    return Dict{String, Any}(
        "complete" => complete,
        "pass" => complete && metrics["pass"] === true,
        "request" => Dict(
            "rules" => inputs.rules,
            "input_sym" => inputs.input_sym,
            "output_sym" => inputs.output_sym,
            "kd" => Float64.(cell.kd),
            "totals" => Dict(String(key) => Float64(value)
                             for (key, value) in pairs(cell.totals)),
            "param_min" => inputs.window[1],
            "param_max" => inputs.window[2],
            "n_points" => inputs_fixture_sample_points(),
        ),
        "curve" => curve,
        "metrics" => metrics,
    )
end

function evaluate_grid_candidate(inputs, intent, magnitude::Float64)
    requested_points = apply_candidate(
        Float64.(inputs.reference["operating_points_log10"]), intent, magnitude)
    input_window = Dict{String, Any}(
        "input_log10" => inputs.window,
        "min_spacing_decades" => inputs.spacing,
        "operating_points_log10" => requested_points,
    )
    started = time_ns()
    region = BEB.feasible_region_reaction_order_program(
        inputs.rules, inputs.input_sym, inputs.output_sym, inputs.program,
        inputs.bounds; input_window=input_window)
    elapsed = (time_ns() - started) / 1e9
    if !region.feasible
        return Dict{String, Any}(
            "magnitude_log10" => magnitude,
            "operating_points_log10" => requested_points,
            "geometrically_feasible" => false,
            "replay" => nothing,
            "elapsed_seconds" => elapsed,
            "reason" => region.reason,
        )
    end
    best = first(region.cells)
    replay = replay_cell(inputs, best)
    return Dict{String, Any}(
        "magnitude_log10" => magnitude,
        "operating_points_log10" => requested_points,
        "geometrically_feasible" => true,
        "path_idx" => best.path_idx,
        "parameter_chebyshev_radius" => best.parameter_chebyshev_radius,
        "augmented_chebyshev_radius" => best.augmented_chebyshev_radius,
        "replay" => replay,
        "elapsed_seconds" => elapsed,
    )
end

function directional_oracle(directional)
    directional === nothing && return Dict(
        "status" => "not_available", "reason" => "intent has no declared direction")
    intervals = directional["union_intervals"]
    positive_max = nothing
    unbounded = false
    for interval in intervals
        if interval["upper_unbounded"] === true
            unbounded = true
        elseif interval["alpha_max"] !== nothing && Float64(interval["alpha_max"]) >= 0
            candidate = Float64(interval["alpha_max"])
            positive_max = positive_max === nothing ? candidate : max(positive_max, candidate)
        end
    end
    return Dict{String, Any}(
        "status" => unbounded ? "unbounded" :
            (positive_max === nothing ? "no_nonnegative_edit" : "bounded"),
        "maximum_nonnegative_alpha" => unbounded ? nothing : positive_max,
        "direction" => directional["direction"],
        "direction_l2_norm" => directional["direction_l2_norm"],
        "union_intervals" => intervals,
        "cell_interval_count" => length(directional["cell_intervals"]),
        "lp_evaluations" => 2 * length(directional["cell_intervals"]),
        "method" => "cellwise_interval_feasibility_oracle",
        "reason" => "Cell-wise intervals are used instead of a globally monotone binary search because the evaluated union may be disconnected.",
    )
end

function run_edit(inputs, raw_intent)
    intent = canonical_intent(raw_intent)
    grid = Float64.(collect(raw_intent["candidate_magnitudes_log10"]))
    baseline_candidates = [evaluate_grid_candidate(inputs, intent, magnitude)
                           for magnitude in grid]
    passing = [candidate for candidate in baseline_candidates
               if candidate["geometrically_feasible"] === true &&
                  candidate["replay"] !== nothing &&
                  candidate["replay"]["pass"] === true]
    baseline_selected = isempty(passing) ? nothing :
        last(sort(passing; by=candidate -> candidate["magnitude_log10"]))

    started = time_ns()
    direct = optimize_rop_shape_request(optimization_request(inputs, intent))
    direct_elapsed = (time_ns() - started) / 1e9
    selected = direct["selected"]
    closure_improvement = selected === nothing ? nothing :
        selected["primary_effect"]["closure_support_improvement"]
    non_grid = closure_improvement === nothing ? false :
        all(abs(Float64(closure_improvement) - magnitude) > 1.0e-6
            for magnitude in grid)
    oracle = directional_oracle(get(direct, "directional_request_interval", nothing))
    return Dict{String, Any}(
        "id" => intent["id"],
        "kind" => intent["kind"],
        "three_candidate_baseline" => Dict{String, Any}(
            "candidate_magnitudes_log10" => grid,
            "candidates" => baseline_candidates,
            "selected" => baseline_selected,
            "selection_rule" =>
                "largest frozen candidate with exact-window feasibility and complete passing two-peak replay",
        ),
        "cellwise_feasibility_oracle" => oracle,
        "direct_lp" => Dict{String, Any}(
            "elapsed_seconds" => direct_elapsed,
            "closure_support_improvement" => closure_improvement,
            "selected_realized_improvement" => selected === nothing ? nothing :
                selected["primary_effect"]["selected_improvement"],
            "parameter_chebyshev_radius" => selected === nothing ? nothing :
                selected["parameter_margin"]["value"],
            "non_grid_optimum" => non_grid,
            "result" => direct,
        ),
        "comparison" => Dict{String, Any}(
            "direct_minus_selected_grid_log10" =>
                (closure_improvement === nothing || baseline_selected === nothing) ? nothing :
                Float64(closure_improvement) -
                Float64(baseline_selected["magnitude_log10"]),
            "same_path_as_selected_grid" =>
                (selected === nothing || baseline_selected === nothing) ? nothing :
                selected["path_idx"] == baseline_selected["path_idx"],
            "surrogate_replay_pass" => direct["replay"]["pass"],
        ),
    )
end

function main()
    fixture = JSON3.read(read(FIXTURE_PATH, String), Dict{String, Any})
    _FIXTURE_SAMPLE_POINTS[] = Int(fixture["reference"]["replay"]["sample_points"])
    inputs = benchmark_inputs(fixture)
    edits = [run_edit(inputs, intent) for intent in fixture["edit_intents"]]
    non_grid_count = count(edit -> edit["direct_lp"]["non_grid_optimum"] === true, edits)
    replay_pass_count = count(edit -> edit["direct_lp"]["result"]["replay"]["pass"] === true, edits)
    output = Dict{String, Any}(
        "schema_version" => "bne-rop-shape-benchmark-result/v1.0.0",
        "benchmark_id" => fixture["id"],
        "fixture_artifact_id" => fixture["reference"]["artifact_id"],
        "evidence_scope" => fixture["evidence_scope"],
        "application_version" => biocircuits_explorer_version(),
        "network_ir_hash" => inputs.network_hash,
        "method_contract" => Dict{String, Any}(
            "three_candidate_baseline" =>
                "frozen explicit operating-point requests through the pre-optimizer exact-window feasible-region path plus fresh replay",
            "cellwise_feasibility_oracle" =>
                "two LP endpoints per evaluated cell; no invalid global monotonicity assumption across disconnected unions",
            "direct_lp" =>
                "global epsilon-lexicographic effect then conditional parameter-only margin over all declared fixed-topology cells",
        ),
        "edits" => edits,
        "summary" => Dict{String, Any}(
            "edit_count" => length(edits),
            "direct_geometric_success_count" => count(
                edit -> edit["direct_lp"]["result"]["feasible"] === true, edits),
            "direct_replay_pass_count" => replay_pass_count,
            "non_grid_optimum_count" => non_grid_count,
            "acceptance_non_grid_optimum_observed" => non_grid_count >= 1,
            "all_direct_searches_exhaustive" => all(
                edit -> edit["direct_lp"]["result"]["coverage"]["truncated"] === false,
                edits),
        ),
    )
    output["result_hash"] = BEB._canonical_hash(output)
    attach_artifact!(
        output, "rop_shape_cat_benchmark";
        input_hashes=Dict(
            "fixture" => fixture["reference"]["artifact_id"],
            "network_ir" => inputs.network_hash,
        ),
        algorithm_name="rop_shape_cat_benchmark_v1",
        config=Dict("fixture" => FIXTURE_PATH,
                    "candidate_grid" => [0.25, 0.5, 0.75]),
    )
    output_path = isempty(ARGS) ? DEFAULT_OUTPUT : abspath(first(ARGS))
    open(output_path, "w") do io
        JSON3.pretty(io, output)
        write(io, '\n')
    end
    println(output_path)
end

main()
