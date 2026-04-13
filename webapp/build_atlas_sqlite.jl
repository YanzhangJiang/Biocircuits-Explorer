#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function looks_like_atlas_corpus(raw)
    return (haskey(raw, :network_entries) || haskey(raw, "network_entries")) &&
           (haskey(raw, :behavior_slices) || haskey(raw, "behavior_slices")) &&
           (haskey(raw, :family_buckets) || haskey(raw, "family_buckets"))
end

function main()::Cint
    if isempty(ARGS) || length(ARGS) > 2
        println(stderr, "Usage: julia --project=webapp webapp/build_atlas_sqlite.jl INPUT.json [ATLAS.sqlite]")
        return 1
    end

    input_path = abspath(ARGS[1])
    sqlite_path = length(ARGS) == 2 ? abspath(ARGS[2]) : atlas_sqlite_default_path()

    raw = JSON3.read(read(input_path, String))
    spec = Dict{String, Any}(
        "sqlite_path" => sqlite_path,
        "source_label" => splitext(basename(input_path))[1],
    )
    if looks_like_atlas_corpus(raw)
        spec["atlas"] = raw
    else
        spec["atlas_spec"] = raw
    end

    merged = merge_atlas_library_from_spec(spec)
    summary = atlas_sqlite_summary(sqlite_path)

    println("Atlas SQLite store updated: $(sqlite_path)")
    println("Imported atlases: $(summary["atlas_count"])")
    println("Unique networks: $(summary["unique_network_count"])")
    println("Behavior slices: $(summary["behavior_slice_count"])")
    println("Library updated at: $(merged["updated_at"])")

    return 0
end

exit(main())
