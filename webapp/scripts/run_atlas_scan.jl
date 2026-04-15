using JSON3
using Logging
using Dates
using Base.Threads

include(normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl")))
using .BiocircuitsExplorerBackend

function _dict_get(raw, key::AbstractString, default=nothing)
    haskey(raw, key) && return raw[key]
    return default
end

function _count_by_string(items, key::AbstractString)
    counts = Dict{String, Int}()
    for item in items
        value = _dict_get(item, key, "unknown")
        label = value === nothing ? "nothing" : String(value)
        counts[label] = get(counts, label, 0) + 1
    end
    return sort(collect(counts); by=first) |> Dict
end

function _default_summary_path(spec_path::String)
    root, _ = splitext(spec_path)
    return root * ".summary.json"
end

function main(args)
    length(args) >= 1 || error("Usage: julia run_atlas_scan.jl <spec.json> [summary.json]")
    spec_path = abspath(args[1])
    summary_path = length(args) >= 2 ? abspath(args[2]) : _default_summary_path(spec_path)

    global_logger(SimpleLogger(stderr, Logging.Warn))

    raw_spec = read(spec_path, String)
    spec = BiocircuitsExplorerBackend._materialize(JSON3.read(raw_spec))
    haskey(ENV, "ATLAS_SQLITE_PATH") && !isempty(ENV["ATLAS_SQLITE_PATH"]) &&
        (spec["sqlite_path"] = abspath(ENV["ATLAS_SQLITE_PATH"]))
    haskey(ENV, "ATLAS_SOURCE_LABEL") && !isempty(ENV["ATLAS_SOURCE_LABEL"]) &&
        (spec["source_label"] = String(ENV["ATLAS_SOURCE_LABEL"]))

    started_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    t0 = time()
    atlas = build_behavior_atlas_from_spec(spec)
    elapsed_seconds = time() - t0
    finished_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

    network_entries = collect(get(atlas, "network_entries", Any[]))
    behavior_slices = collect(get(atlas, "behavior_slices", Any[]))

    summary = Dict(
        "spec_path" => spec_path,
        "summary_path" => summary_path,
        "started_at" => started_at,
        "finished_at" => finished_at,
        "elapsed_seconds" => elapsed_seconds,
        "julia_threads" => nthreads(),
        "network_parallelism_requested" => Int(_dict_get(spec, "network_parallelism", 1)),
        "atlas_summary" => BiocircuitsExplorerBackend._atlas_summary(atlas),
        "enumeration" => get(atlas, "enumeration", nothing),
        "network_status_counts" => _count_by_string(network_entries, "analysis_status"),
        "network_failure_class_counts" => _count_by_string(
            filter(entry -> _dict_get(entry, "analysis_status", "") == "failed", network_entries),
            "failure_class",
        ),
        "slice_status_counts" => _count_by_string(behavior_slices, "analysis_status"),
        "slice_failure_class_counts" => _count_by_string(
            filter(slice -> _dict_get(slice, "analysis_status", "") == "failed", behavior_slices),
            "failure_class",
        ),
        "sqlite_library_summary" => get(atlas, "sqlite_library_summary", nothing),
    )

    rendered = sprint(io -> JSON3.pretty(io, summary))

    open(summary_path, "w") do io
        write(io, rendered)
        write(io, "\n")
    end

    println(rendered)
end

main(ARGS)
