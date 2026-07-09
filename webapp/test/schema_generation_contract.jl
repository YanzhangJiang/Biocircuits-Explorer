module SchemaGenerationContract

using Test
using JSON3

const REPO_ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const SOURCE_SCHEMA_DIR = joinpath(REPO_ROOT, "schemas")

include(joinpath(REPO_ROOT, "webapp", "scripts", "gen_schemas.jl"))

function copy_schemas(destination)
    mkpath(destination)
    for name in readdir(SOURCE_SCHEMA_DIR)
        endswith(name, ".schema.json") || continue
        cp(joinpath(SOURCE_SCHEMA_DIR, name), joinpath(destination, name))
    end
end

function file_snapshot(directory)
    Dict(
        name => begin
            path = joinpath(directory, name)
            info = stat(path)
            (bytes = read(path), inode = info.inode, mtime = info.mtime)
        end
        for name in sort(readdir(directory)) if endswith(name, ".schema.json")
    )
end

@testset "deterministic schema generation and read-only drift checking" begin
    @test rendered_schemas() == rendered_schemas()
    @test parse_cli(String[]).mode == :write
    @test isempty(schema_id_errors(SOURCE_SCHEMA_DIR))

    schema_files = filter(name -> endswith(name, ".schema.json"), readdir(SOURCE_SCHEMA_DIR))
    ids = String[]
    for name in schema_files
        schema = JSON3.read(read(joinpath(SOURCE_SCHEMA_DIR, name), String))
        id = get(schema, "\$id", nothing)
        @test id isa AbstractString
        @test !isempty(strip(String(id)))
        push!(ids, String(id))
    end
    @test length(unique(ids)) == length(schema_files)

    trace = JSON3.read(read(joinpath(SOURCE_SCHEMA_DIR, "design-agent-trace.schema.json"), String))
    @test trace["\$id"] == "https://biocircuits-explorer.com/schemas/design-agent-trace.schema.json"

    mktempdir() do temp_schema_dir
        copy_schemas(temp_schema_dir)
        generated_path = joinpath(temp_schema_dir, "design-spec.schema.json")
        generated_before_write = replace(read(generated_path, String),
                                         "Biocircuits Explorer DesignSpec" => "STALE DesignSpec";
                                         count = 1)
        write(generated_path, generated_before_write)
        stale_inode = stat(generated_path).inode
        @test main(["--write", "--schema-dir", temp_schema_dir];
                   out = IOBuffer(), err = IOBuffer()) == 0
        @test read(generated_path, String) == rendered_schemas()["design-spec.schema.json"]
        @test stat(generated_path).inode != stale_inode

        before = file_snapshot(temp_schema_dir)
        check_out = IOBuffer()
        check_err = IOBuffer()
        @test main(["--check", "--schema-dir=$(temp_schema_dir)"];
                   out = check_out, err = check_err) == 0
        @test file_snapshot(temp_schema_dir) == before
        @test isempty(String(take!(check_err)))
        @test occursin("schema check passed", String(take!(check_out)))

        stale_path = joinpath(temp_schema_dir, "design-spec.schema.json")
        stale = replace(read(stale_path, String),
                        "Biocircuits Explorer DesignSpec" => "STALE DesignSpec";
                        count = 1)
        write(stale_path, stale)
        stale_before_check = read(stale_path)
        stale_err = IOBuffer()
        @test main(["--check", "--schema-dir", temp_schema_dir];
                   out = IOBuffer(), err = stale_err) == 1
        @test read(stale_path) == stale_before_check
        message = String(take!(stale_err))
        @test occursin("schema drift", message)
        @test occursin("first difference at line", message)
        @test occursin("gen_schemas.jl --write", message)
    end
end

end # module
