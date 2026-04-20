using JSON3
using Logging
using Dates

include(normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl")))
using .BiocircuitsExplorerBackend

function _write_json(path::String, payload)
    rendered = sprint(io -> JSON3.pretty(io, payload))
    open(path, "w") do io
        write(io, rendered)
        write(io, "\n")
    end
end

function _collect_input_summary(path::String)
    summary = atlas_sqlite_summary(path)
    return Dict(
        "sqlite_path" => path,
        "summary" => summary,
    )
end

function main(args)
    length(args) >= 2 || error("Usage: julia merge_atlas_sqlite_shards.jl <output.sqlite> <input1.sqlite> [input2.sqlite ...] [summary.json]")
    positional = abspath.(args)
    summary_path = endswith(lowercase(positional[end]), ".json") ? pop!(positional) : nothing
    output_path = first(positional)
    input_paths = positional[2:end]
    isempty(input_paths) && error("Provide at least one shard sqlite input.")

    global_logger(SimpleLogger(stderr, Logging.Warn))

    started_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    t0 = time()

    merged = atlas_library_default()
    inputs = Dict{String, Any}[]

    for input_path in input_paths
        isfile(input_path) || error("Missing shard sqlite: $(input_path)")
        atlas_sqlite_has_library(input_path) || error("SQLite does not contain an atlas library: $(input_path)")
        shard_library = atlas_sqlite_load_library(input_path)
        source_label = "sqlite_shard::" * basename(input_path)
        source_metadata = Dict(
            "merge_kind" => "sqlite_shard_library",
            "input_sqlite_path" => input_path,
        )
        merged = merge_atlas_library(merged, shard_library;
            source_label=source_label,
            source_metadata=source_metadata,
        )
        push!(inputs, _collect_input_summary(input_path))
    end

    atlas_sqlite_save_library!(output_path, merged)
    final_summary = atlas_sqlite_summary(output_path)

    result = Dict(
        "status" => "completed",
        "started_at" => started_at,
        "finished_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        "elapsed_seconds" => time() - t0,
        "output_sqlite_path" => output_path,
        "input_sqlite_paths" => input_paths,
        "input_count" => length(input_paths),
        "input_summaries" => inputs,
        "output_summary" => final_summary,
    )

    summary_path === nothing || _write_json(summary_path, result)
    println(sprint(io -> JSON3.pretty(io, result)))
end

try
    main(ARGS)
catch err
    showerror(stderr, err)
    write(stderr, "\n")
    rethrow()
end
