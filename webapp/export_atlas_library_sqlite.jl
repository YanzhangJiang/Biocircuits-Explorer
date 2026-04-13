#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(sqlite_path::AbstractString)
    lower = lowercase(sqlite_path)
    if endswith(lower, ".sqlite")
        return sqlite_path[1:end-7] * "_library.json"
    elseif endswith(lower, ".db")
        return sqlite_path[1:end-3] * "_library.json"
    end
    return sqlite_path * "_library.json"
end

function main()::Cint
    if isempty(ARGS) || length(ARGS) > 2
        println(stderr, "Usage: julia --project=webapp webapp/export_atlas_library_sqlite.jl ATLAS.sqlite [OUTPUT.json]")
        return 1
    end

    sqlite_path = abspath(ARGS[1])
    output_path = length(ARGS) == 2 ? abspath(ARGS[2]) : abspath(default_output_path(sqlite_path))

    library = atlas_sqlite_load_library(sqlite_path)

    open(output_path, "w") do io
        write(io, JSON3.write(library))
    end

    println("Atlas library exported to: $(output_path)")
    println("Imported atlases: $(library["atlas_count"])")
    println("Unique networks: $(library["unique_network_count"])")
    println("Behavior slices: $(library["behavior_slice_count"])")

    return 0
end

exit(main())
