#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(input_path::AbstractString)
    lower = lowercase(input_path)
    if endswith(lower, ".json")
        return input_path[1:end-5] * "_atlas.json"
    end
    return input_path * "_atlas.json"
end

function main()::Cint
    if isempty(ARGS) || length(ARGS) > 2
        println(stderr, "Usage: julia --project=webapp webapp/build_atlas.jl INPUT.json [OUTPUT.json]")
        return 1
    end

    input_path = abspath(ARGS[1])
    output_path = length(ARGS) == 2 ? abspath(ARGS[2]) : abspath(default_output_path(input_path))

    spec = JSON3.read(read(input_path, String))
    atlas = build_behavior_atlas_from_spec(spec)

    open(output_path, "w") do io
        write(io, JSON3.write(atlas))
    end

    input_network_count = atlas["input_network_count"]
    unique_network_count = atlas["unique_network_count"]
    slice_count = length(atlas["behavior_slices"])
    family_bucket_count = length(atlas["family_buckets"])

    println("Atlas written to: $(output_path)")
    println("Networks analyzed: $(input_network_count)")
    println("Unique networks kept: $(unique_network_count)")
    println("Behavior slices: $(slice_count)")
    println("Family buckets: $(family_bucket_count)")

    return 0
end

exit(main())
