#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(library_path::AbstractString, input_path::AbstractString)
    library_base = endswith(lowercase(library_path), ".json") ? library_path[1:end-5] : library_path
    input_base = splitext(basename(input_path))[1]
    return library_base * "_" * input_base * "_merged.json"
end

function looks_like_atlas_corpus(raw)
    return (haskey(raw, :network_entries) || haskey(raw, "network_entries")) &&
           (haskey(raw, :behavior_slices) || haskey(raw, "behavior_slices")) &&
           (haskey(raw, :family_buckets) || haskey(raw, "family_buckets"))
end

function main()::Cint
    if length(ARGS) < 2 || length(ARGS) > 3
        println(stderr, "Usage: julia --project=webapp webapp/merge_atlas_library.jl LIBRARY.json INPUT.json [OUTPUT.json]")
        return 1
    end

    library_path = abspath(ARGS[1])
    input_path = abspath(ARGS[2])
    output_path = length(ARGS) == 3 ? abspath(ARGS[3]) : abspath(default_output_path(library_path, input_path))

    library = JSON3.read(read(library_path, String))
    raw = JSON3.read(read(input_path, String))

    spec = Dict{String, Any}(
        "library" => library,
        "source_label" => splitext(basename(input_path))[1],
    )
    if looks_like_atlas_corpus(raw)
        spec["atlas"] = raw
    else
        spec["atlas_spec"] = raw
    end

    merged = merge_atlas_library_from_spec(spec)

    open(output_path, "w") do io
        write(io, JSON3.write(merged))
    end

    atlas_count = merged["atlas_count"]
    network_count = merged["unique_network_count"]
    slice_count = merged["behavior_slice_count"]

    println("Merged atlas library written to: $(output_path)")
    println("Imported atlases: $(atlas_count)")
    println("Unique networks: $(network_count)")
    println("Behavior slices: $(slice_count)")

    return 0
end

exit(main())
