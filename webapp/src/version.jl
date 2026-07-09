const APP_VERSION_CACHE = Ref{Union{Nothing, String}}(nothing)

function _read_first_existing_text(paths::Vector{String})
    for path in paths
        if isfile(path)
            text = strip(read(path, String))
            isempty(text) || return text
        end
    end
    return nothing
end

function _version_file_candidates(;
    source_dir::AbstractString=@__DIR__,
    program_file::AbstractString=Base.PROGRAM_FILE,
    bindir::AbstractString=Sys.BINDIR,
)
    paths = String[]

    program_path = strip(String(program_file))
    if !isempty(program_path)
        push!(paths, normpath(joinpath(
            dirname(abspath(program_path)), "..", "share", "biocircuits-explorer", "VERSION")))
    end

    binary_dir = strip(String(bindir))
    if !isempty(binary_dir)
        push!(paths, normpath(joinpath(
            abspath(binary_dir), "..", "share", "biocircuits-explorer", "VERSION")))
    end

    append!(paths, [
        normpath(joinpath(source_dir, "..", "..", "VERSION")),
        normpath(joinpath(source_dir, "..", "VERSION")),
    ])
    return unique(paths)
end

function biocircuits_explorer_version()
    env_version = strip(get(ENV, "BIOCIRCUITS_EXPLORER_VERSION", ""))
    isempty(env_version) || return env_version

    if APP_VERSION_CACHE[] === nothing
        APP_VERSION_CACHE[] = something(
            _read_first_existing_text(_version_file_candidates()),
            "0.0.0-dev",
        )
    end
    return APP_VERSION_CACHE[]
end

function biocircuits_explorer_build_info()
    return Dict(
        "name" => "Biocircuits Explorer",
        "version" => biocircuits_explorer_version(),
        "revision" => strip(get(ENV, "BIOCIRCUITS_EXPLORER_REVISION", "unknown")),
        "created" => strip(get(ENV, "BIOCIRCUITS_EXPLORER_CREATED", "unknown")),
    )
end
