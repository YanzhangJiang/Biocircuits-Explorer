#!/usr/bin/env julia

import Pkg

project_dir = normpath(joinpath(@__DIR__, ".."))
project_file = joinpath(project_dir, "Project.toml")
active_project = Base.active_project()
if active_project === nothing || normpath(active_project) != project_file
    Pkg.activate(project_dir; io=devnull)
end

using BiocircuitsExplorerBackend

# stdout is intentionally only the stable JSON document. Human-facing build
# tools can redirect it directly into a generated reference file.
print(BiocircuitsExplorerBackend.api_contract_reference_json())
