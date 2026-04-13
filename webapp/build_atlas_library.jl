#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(input_path::AbstractString)
    lower = lowercase(input_path)
    if endswith(lower, ".json")
        return input_path[1:end-5] * "_library.json"
    end
    return input_path * "_library.json"
end

function looks_like_atlas_corpus(raw)
    return (haskey(raw, :network_entries) || haskey(raw, "network_entries")) &&
           (haskey(raw, :behavior_slices) || haskey(raw, "behavior_slices")) &&
           (haskey(raw, :family_buckets) || haskey(raw, "family_buckets"))
end

function main()::Cint
    if isempty(ARGS) || length(ARGS) > 2
        println(stderr, "Usage: julia --project=webapp webapp/build_atlas_library.jl INPUT.json [OUTPUT.json]")
        return 1
    end

    input_path = abspath(ARGS[1])
    output_path = length(ARGS) == 2 ? abspath(ARGS[2]) : abspath(default_output_path(input_path))

    raw = JSON3.read(read(input_path, String))
    spec = if haskey(raw, :source_label) || haskey(raw, "source_label")
        raw
    elseif looks_like_atlas_corpus(raw)
        Dict(
            "atlas" => raw,
            "source_label" => splitext(basename(input_path))[1],
        )
    else
        Dict(
            "atlas_spec" => raw,
            "source_label" => splitext(basename(input_path))[1],
        )
    end

    library = build_atlas_library_from_spec(spec)

    open(output_path, "w") do io
        write(io, JSON3.write(library))
    end

    atlas_count = library["atlas_count"]
    network_count = library["unique_network_count"]
    slice_count = library["behavior_slice_count"]

    println("Atlas library written to: $(output_path)")
    println("Imported atlases: $(atlas_count)")
    println("Unique networks: $(network_count)")
    println("Behavior slices: $(slice_count)")

    return 0
end

exit(main())
