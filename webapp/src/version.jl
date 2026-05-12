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

function biocircuits_explorer_version()
    env_version = strip(get(ENV, "BIOCIRCUITS_EXPLORER_VERSION", ""))
    isempty(env_version) || return env_version

    if APP_VERSION_CACHE[] === nothing
        APP_VERSION_CACHE[] = something(
            _read_first_existing_text([
                normpath(joinpath(@__DIR__, "..", "..", "VERSION")),
                normpath(joinpath(@__DIR__, "..", "VERSION")),
            ]),
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
