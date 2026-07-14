#!/usr/bin/env julia

using JSON3
using BiocircuitsExplorerBackend

const BEB = BiocircuitsExplorerBackend
const FIXTURE_PATH = joinpath(@__DIR__, "cat_fixed_topology.json")

function materialize_json(value)
    return JSON3.read(JSON3.write(value), Dict{String, Any})
end

function main()
    length(ARGS) == 1 || error(
        "usage: julia --project=webapp benchmarks/rop_shape_control/" *
        "export_reference_replay.jl OUTPUT.json")

    fixture = JSON3.read(read(FIXTURE_PATH, String), Dict{String, Any})
    rules = String.(collect(fixture["network"]["rules"]))
    input_sym = Symbol(String(fixture["network"]["input"]))
    output_sym = String(fixture["network"]["output"])
    reference = fixture["reference"]
    kd = Float64.(collect(reference["kd"]))
    totals = Dict{Symbol, Float64}(
        Symbol(String(key)) => Float64(value)
        for (key, value) in pairs(reference["totals"]))
    window = Float64.(collect(reference["replay"]["input_window_log10"]))
    sample_points = Int(reference["replay"]["sample_points"])

    curve = BEB.placer_dose_response(
        rules, kd, totals, input_sym, output_sym;
        param_min=window[1], param_max=window[2], n_points=sample_points)
    output_log10 = Float64[Float64(row[1]) for row in curve["output_traj"]]
    metrics = BEB.analyze_two_peak_curve(
        Float64.(curve["param_values"]), output_log10, curve["valid"];
        min_prominence_log10=0.5)

    expected = reference["replay"]
    actual_peaks = Float64.(metrics["peak_input_log10"])
    expected_peaks = Float64.(expected["sampled_peak_input_log10"])
    actual_range = [minimum(output_log10), maximum(output_log10)]
    expected_range = Float64.(expected["sampled_output_log10_range"])
    all(curve["valid"]) || error("reference replay contains invalid samples")
    curve["partial"] === false || error("reference replay is partial")
    length(output_log10) == sample_points || error("reference replay sample count drifted")
    isapprox(actual_peaks, expected_peaks; atol=1.0e-12, rtol=0.0) ||
        error("reference replay peak locations drifted")
    isapprox(actual_range, expected_range; atol=1.0e-12, rtol=0.0) ||
        error("reference replay output range drifted")

    payload = Dict{String, Any}(
        "schema_version" => "bne-shape-control-reference-replay/v1.0.0",
        "benchmark_id" => String(fixture["id"]),
        "julia_version" => string(VERSION),
        "application_version" => BEB.biocircuits_explorer_version(),
        "source_fixture" => "benchmarks/rop_shape_control/cat_fixed_topology.json",
        "replay_function" => "BiocircuitsExplorerBackend.placer_dose_response",
        "metric_function" => "BiocircuitsExplorerBackend.analyze_two_peak_curve",
        "request" => Dict{String, Any}(
            "endpoint" => "/api/v1/placer_curve",
            "rules" => rules,
            "input_sym" => String(input_sym),
            "output_sym" => output_sym,
            "kd" => kd,
            "totals" => Dict(String(key) => value for (key, value) in totals),
            "param_min" => window[1],
            "param_max" => window[2],
            "n_points" => sample_points,
        ),
        "curve" => Dict{String, Any}(
            "param_values" => curve["param_values"],
            "output_log10" => output_log10,
            "valid" => curve["valid"],
            "partial" => curve["partial"],
        ),
        "metrics" => materialize_json(metrics),
        "fixture_match" => Dict{String, Any}(
            "sample_count" => sample_points,
            "valid_count" => count(identity, curve["valid"]),
            "partial" => curve["partial"],
            "sampled_peak_input_log10" => actual_peaks,
            "sampled_output_log10_range" => actual_range,
            "matches_fixture_summary" => true,
        ),
    )

    output_path = abspath(first(ARGS))
    open(output_path, "w") do io
        JSON3.pretty(io, payload)
        write(io, '\n')
    end
    println(output_path)
end

main()
