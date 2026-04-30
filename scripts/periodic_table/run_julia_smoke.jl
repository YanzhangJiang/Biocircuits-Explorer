#!/usr/bin/env julia

using Dates
using JSON3
using BiocircuitsExplorerBackend

const REPO_ROOT = abspath(joinpath(@__DIR__, "..", ".."))

function parse_args(args)
    out = Dict{String, String}()
    idx = 1
    while idx <= length(args)
        key = args[idx]
        if key in ("--run-id", "--output-dir")
            idx == length(args) && error("Missing value for $(key)")
            out[key[3:end]] = args[idx + 1]
            idx += 2
        else
            error("Unknown argument: $(key)")
        end
    end
    return out
end

function utc_now()
    return Dates.format(now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SSZ")
end

function write_json(path, payload)
    mkpath(dirname(path))
    tmp = path * ".tmp"
    open(tmp, "w") do io
        JSON3.write(io, payload)
        write(io, "\n")
    end
    mv(tmp, path; force=true)
end

function append_jsonl(path, payload)
    mkpath(dirname(path))
    open(path, "a") do io
        JSON3.write(io, payload)
        write(io, "\n")
    end
end

function smoke_networks()
    return Any[
        Dict(
            "label" => "monomer_dimer",
            "reactions" => Any["A + B <-> AB"],
            "input_symbols" => Any["tA"],
            "output_symbols" => Any["AB"],
        ),
    ]
end

function summarize_atlas(atlas)
    slices = Any[]
    for slice in atlas["behavior_slices"]
        push!(slices, Dict(
            "slice_id" => slice["slice_id"],
            "network_id" => slice["network_id"],
            "input_symbol" => get(slice, "input_symbol", nothing),
            "output_symbol" => get(slice, "output_symbol", nothing),
            "analysis_status" => get(slice, "analysis_status", nothing),
            "exact_union" => get(slice, "exact_union", Any[]),
            "motif_union" => get(slice, "motif_union", Any[]),
            "path_record_count" => get(slice, "path_record_count", nothing),
        ))
    end
    return Dict(
        "network_entries" => length(atlas["network_entries"]),
        "behavior_slices" => length(atlas["behavior_slices"]),
        "regime_records" => length(atlas["regime_records"]),
        "transition_records" => length(atlas["transition_records"]),
        "family_buckets" => length(atlas["family_buckets"]),
        "path_records" => length(atlas["path_records"]),
        "slices" => slices,
    )
end

function main(args)
    parsed = parse_args(args)
    run_id = get(parsed, "run-id", Dates.format(now(Dates.UTC), dateformat"yyyymmddTHHMMSS") * "_julia_smoke")
    run_dir = get(parsed, "output-dir", joinpath(REPO_ROOT, "results", "periodic_table", "runs", run_id))
    summary_path = joinpath(run_dir, "julia_smoke_summary.json")
    event_path = joinpath(run_dir, "logs", "events.jsonl")

    config = AtlasBehaviorConfig(
        path_scope=:feasible,
        min_volume_mean=0.0,
        include_path_records=false,
        compute_volume=false,
        keep_singular=true,
    )

    payload = Dict{String, Any}(
        "time_utc" => utc_now(),
        "run_id" => run_id,
        "profile_id" => "periodic_d_mu_v0",
        "smoke_kind" => "explicit_tiny_backend_atlas",
        "storage_policy" => Dict(
            "include_path_records" => false,
            "store_full_atlas" => false,
        ),
    )

    try
        atlas = build_behavior_atlas(smoke_networks(); behavior_config=config)
        summary = summarize_atlas(atlas)
        summary["path_records"] == 0 || error("Julia smoke persisted path records unexpectedly.")
        payload["status"] = "ok"
        payload["summary"] = summary
        write_json(summary_path, payload)
        append_jsonl(event_path, Dict(
            "time_utc" => utc_now(),
            "run_id" => run_id,
            "agent_id" => "codex-main",
            "stage" => "julia_smoke",
            "status" => "completed",
            "message" => "tiny backend ROP smoke completed without persisted path records",
            "counts" => summary,
        ))
        println(JSON3.write(payload))
        return 0
    catch err
        payload["status"] = "error"
        payload["error_type"] = string(typeof(err))
        payload["message"] = sprint(showerror, err, catch_backtrace())
        write_json(summary_path, payload)
        append_jsonl(event_path, Dict(
            "time_utc" => utc_now(),
            "run_id" => run_id,
            "agent_id" => "codex-main",
            "stage" => "julia_smoke",
            "status" => "error",
            "message" => payload["message"],
        ))
        println(JSON3.write(payload))
        return 1
    end
end

exit(main(ARGS))
