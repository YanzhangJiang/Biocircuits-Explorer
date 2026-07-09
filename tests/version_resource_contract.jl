using Test

module VersionContractSubject
include(joinpath(@__DIR__, "..", "webapp", "src", "version.jl"))
end

const V = VersionContractSubject

@testset "packaged version resource discovery" begin
    mktempdir() do bundle
        binary_dir = joinpath(bundle, "bin")
        version_path = joinpath(bundle, "share", "biocircuits-explorer", "VERSION")
        mkpath(binary_dir)
        mkpath(dirname(version_path))
        write(version_path, "9.8.7\n")

        candidates = V._version_file_candidates(
            source_dir=joinpath(bundle, "unavailable-source", "webapp", "src"),
            program_file=joinpath(binary_dir, "biocircuits-explorer-backend"),
            bindir=binary_dir,
        )

        @test first(candidates) == version_path
        @test length(candidates) == length(unique(candidates))
        @test V._read_first_existing_text(candidates) == "9.8.7"
    end
end
