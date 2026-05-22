#!/usr/bin/env julia

# Disable ProgressMeter output for the server process. The @showprogress
# loops in Bnc_julia (regimes.jl, regime_graphs.jl) write terminal-control
# sequences to stderr; when the macOS shell spawns Julia and its stderr
# pipe reader closes/stalls under load, those writes hit EPIPE and tear
# down the whole API request (see find_vertices / build_graph failures).
# Each @showprogress call site in Bnc_julia honours this env var by
# passing enabled=false to Progress(). Must precede `using`.
ENV["BNC_NO_PROGRESS"] = "1"

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
