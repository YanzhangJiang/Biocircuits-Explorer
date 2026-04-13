#!/usr/bin/env julia

import Pkg

# Make the server resilient to how it is launched.
# This allows `julia webapp/server.jl` as well as `--project=webapp`.
project_file = normpath(joinpath(@__DIR__, "Project.toml"))
active_project = Base.active_project()
if active_project === nothing || normpath(active_project) != project_file
    Pkg.activate(@__DIR__; io=devnull)
end

using BiocircuitsExplorerBackend

BiocircuitsExplorerBackend.main()
