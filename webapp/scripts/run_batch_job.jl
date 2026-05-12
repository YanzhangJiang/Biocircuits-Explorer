#!/usr/bin/env julia

import Pkg

project_file = normpath(joinpath(@__DIR__, "..", "Project.toml"))
active_project = Base.active_project()
if active_project === nothing || normpath(active_project) != project_file
    Pkg.activate(normpath(joinpath(@__DIR__, "..")); io=devnull)
end

using JSON3
using BiocircuitsExplorerBackend

function _parse_args(args)
    parsed = Dict{String, String}()
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--input-uri", "--status-uri", "--result-uri")
            i == length(args) && error("Missing value for $(arg)")
            parsed[arg[3:end]] = args[i + 1]
            i += 2
        elseif arg == "--help"
            println("""
Usage:
  julia --project=webapp webapp/scripts/run_batch_job.jl \\
    --input-uri <local-or-s3-json> \\
    [--status-uri <local-or-s3-json>] \\
    [--result-uri <local-or-s3-json>]

Environment fallbacks:
  BIOCIRCUITS_EXPLORER_JOB_INPUT_URI
  BIOCIRCUITS_EXPLORER_JOB_STATUS_URI
  BIOCIRCUITS_EXPLORER_JOB_RESULT_URI
""")
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
    end
    return parsed
end

function _arg_or_env(parsed, key::AbstractString, env_name::AbstractString, default=nothing)
    value = get(parsed, key, nothing)
    value !== nothing && return value
    env_value = strip(get(ENV, env_name, ""))
    isempty(env_value) ? default : env_value
end

function main(args=ARGS)
    parsed = _parse_args(args)
    input_uri = _arg_or_env(parsed, "input-uri", "BIOCIRCUITS_EXPLORER_JOB_INPUT_URI")
    status_uri = _arg_or_env(parsed, "status-uri", "BIOCIRCUITS_EXPLORER_JOB_STATUS_URI")
    result_uri = _arg_or_env(parsed, "result-uri", "BIOCIRCUITS_EXPLORER_JOB_RESULT_URI")

    input_uri === nothing && error("Batch worker requires --input-uri or BIOCIRCUITS_EXPLORER_JOB_INPUT_URI.")
    result = run_biocircuits_job_from_uri(input_uri; status_uri=status_uri, result_uri=result_uri)
    println(JSON3.write(Dict("status" => "succeeded", "result_type" => string(typeof(result)))))
end

main()
