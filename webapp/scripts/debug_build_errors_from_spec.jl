using JSON3
using Logging
using Dates
using Base.Threads

include(normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl")))
using .BiocircuitsExplorerBackend

function main(args)
    length(args) >= 2 || error("Usage: julia debug_build_errors_from_spec.jl <spec.json> <output.json>")
    spec_path = abspath(args[1])
    output_path = abspath(args[2])

    global_logger(SimpleLogger(stderr, Logging.Warn))

    raw_spec = read(spec_path, String)
    spec = BiocircuitsExplorerBackend._materialize(JSON3.read(raw_spec))

    started_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    atlas = build_behavior_atlas_from_spec(spec)
    finished_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

    network_entries = collect(get(atlas, "network_entries", Any[]))
    behavior_slices = collect(get(atlas, "behavior_slices", Any[]))

    failed_networks = [
        Dict{String, Any}(BiocircuitsExplorerBackend._materialize(entry))
        for entry in network_entries
        if String(get(entry, "analysis_status", "")) == "failed"
    ]
    failed_slices = [
        Dict{String, Any}(BiocircuitsExplorerBackend._materialize(slice))
        for slice in behavior_slices
        if String(get(slice, "analysis_status", "")) == "failed"
    ]

    payload = Dict(
        "spec_path" => spec_path,
        "started_at" => started_at,
        "finished_at" => finished_at,
        "julia_threads" => nthreads(),
        "network_parallelism_requested" => Int(get(spec, "network_parallelism", 1)),
        "failed_network_count" => length(failed_networks),
        "failed_slice_count" => length(failed_slices),
        "failed_networks" => failed_networks,
        "failed_slices" => failed_slices,
    )

    open(output_path, "w") do io
        JSON3.pretty(io, payload)
        write(io, "\n")
    end

    println(output_path)
    println("failed_network_count=$(length(failed_networks))")
    println("failed_slice_count=$(length(failed_slices))")
end

main(ARGS)
