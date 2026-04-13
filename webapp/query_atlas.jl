#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(atlas_path::AbstractString, query_path::AbstractString)
    atlas_base = endswith(lowercase(atlas_path), ".json") ? atlas_path[1:end-5] : atlas_path
    query_base = splitext(basename(query_path))[1]
    return atlas_base * "_" * query_base * "_query.json"
end

function main()::Cint
    if length(ARGS) < 2 || length(ARGS) > 3
        println(stderr, "Usage: julia --project=webapp webapp/query_atlas.jl ATLAS.json|ATLAS.sqlite QUERY.json [OUTPUT.json]")
        return 1
    end

    atlas_path = abspath(ARGS[1])
    query_path = abspath(ARGS[2])
    output_path = length(ARGS) == 3 ? abspath(ARGS[3]) : abspath(default_output_path(atlas_path, query_path))

    query_spec = JSON3.read(read(query_path, String))
    result = if endswith(lowercase(atlas_path), ".sqlite") || endswith(lowercase(atlas_path), ".db")
        query_behavior_atlas_from_spec(Dict(
            "sqlite_path" => atlas_path,
            "query" => query_spec,
        ))
    else
        atlas = JSON3.read(read(atlas_path, String))
        query_behavior_atlas(atlas, BiocircuitsExplorerBackend.atlas_query_spec_from_raw(query_spec))
    end

    open(output_path, "w") do io
        write(io, JSON3.write(result))
    end

    result_count = result["result_count"]

    println("Query result written to: $(output_path)")
    println("Matches returned: $(result_count)")

    return 0
end

exit(main())
