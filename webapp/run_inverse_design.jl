#!/usr/bin/env julia

import Pkg

Pkg.activate(@__DIR__; io=devnull)

using JSON3
using BiocircuitsExplorerBackend

function default_output_path(input_path::AbstractString)
    lower = lowercase(input_path)
    if endswith(lower, ".json")
        return input_path[1:end-5] * "_inverse_design.json"
    end
    return input_path * "_inverse_design.json"
end

function main()::Cint
    if isempty(ARGS) || length(ARGS) > 2
        println(stderr, "Usage: julia --project=webapp webapp/run_inverse_design.jl INPUT.json [OUTPUT.json]")
        return 1
    end

    input_path = abspath(ARGS[1])
    output_path = length(ARGS) == 2 ? abspath(ARGS[2]) : abspath(default_output_path(input_path))

    spec = JSON3.read(read(input_path, String))
    result = run_inverse_design_from_spec(spec)

    open(output_path, "w") do io
        write(io, JSON3.write(result))
    end

    query_result = result["query_result"]
    result_count = query_result["result_count"]
    target_kind = result["query_target_kind"]
    build_performed = result["build_performed"]
    merge_performed = result["merge_performed"]

    println("Inverse-design result written to: $(output_path)")
    println("Query target kind: $(target_kind)")
    println("Build performed: $(build_performed)")
    println("Merge performed: $(merge_performed)")
    println("Matches returned: $(result_count)")

    return 0
end

exit(main())
