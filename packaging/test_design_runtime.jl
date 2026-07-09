using Test
using BiocircuitsExplorerPackaging

@testset "Design Agent runtime staging" begin
    mktempdir(prefix="Biocircuits Explorer packaging ") do resource_dir
        copy_design_runtime!(resource_dir)

        script_dir = joinpath(resource_dir, "webapp", "scripts")
        @test isfile(joinpath(script_dir, "chat_api.py"))
        @test isfile(joinpath(script_dir, "reader", "reader.py"))
        @test isfile(joinpath(resource_dir, "schemas", "behavior-spec.schema.json"))
        cache_dirs = [joinpath(root, dir)
                      for (root, dirs, _) in walkdir(resource_dir)
                      for dir in dirs if dir == "__pycache__"]
        @test isempty(cache_dirs)

        python = Sys.which("python3")
        if python === nothing
            @test_skip "python3 is unavailable; staged-path checks still ran"
        else
            schema = joinpath(resource_dir, "schemas", "behavior-spec.schema.json")
            probe = "import sys; sys.path.insert(0, sys.argv[1]); import chat_api; " *
                    "assert chat_api.agent.L.SCHEMA_PATH == sys.argv[2]"
            @test success(`$python -I -c $probe $script_dir $schema`)
        end
    end
end
