using Pkg

const PACKAGING_DIR = @__DIR__
const REPO_ROOT = normpath(joinpath(PACKAGING_DIR, ".."))
const WEBAPP_DIR = joinpath(REPO_ROOT, "webapp")
const DIST_DIR = joinpath(REPO_ROOT, "dist")
const APP_DIR = joinpath(DIST_DIR, "BiocircuitsExplorerBackend")
const RESOURCE_DIR = joinpath(APP_DIR, "share", "biocircuits-explorer")
const HOMEBREW_PREFIX = get(ENV, "HOMEBREW_PREFIX", "/opt/homebrew")
const DEPOT_SEPARATOR = Sys.iswindows() ? ";" : ":"
const LOCAL_DEPOT = joinpath(REPO_ROOT, ".julia_packaging_depot")

mkpath(LOCAL_DEPOT)
rm(joinpath(LOCAL_DEPOT, "compiled"); recursive=true, force=true)

depot_entries = unique(vcat(LOCAL_DEPOT, DEPOT_PATH))
ENV["JULIA_DEPOT_PATH"] = join(depot_entries, DEPOT_SEPARATOR)
empty!(DEPOT_PATH)
append!(DEPOT_PATH, depot_entries)

function resolve_symlink_target(path::AbstractString)
    raw_target = readlink(path)
    candidates = String[]

    if isabspath(raw_target)
        push!(candidates, raw_target)
    else
        push!(candidates, normpath(joinpath(dirname(path), raw_target)))

        stripped = raw_target
        while startswith(stripped, "../")
            stripped = stripped[4:end]
        end
        if stripped != raw_target
            push!(candidates, normpath(joinpath(HOMEBREW_PREFIX, stripped)))
        end
    end

    for candidate in unique(candidates)
        ispath(candidate) && return abspath(candidate)
    end

    return nothing
end

function materialize_external_symlinks!(root_dir::AbstractString)
    root_dir = abspath(root_dir)
    pending = [root_dir]

    while !isempty(pending)
        current_dir = pop!(pending)

        for path in readdir(current_dir; join=true)
            if islink(path)
                target_real = try
                    resolve_symlink_target(path)
                catch err
                    @warn "Failed to resolve symlink" path exception=(err, catch_backtrace())
                    continue
                end

                target_real === nothing && begin
                    @warn "Skipping unresolved symlink" path target=readlink(path)
                    continue
                end

                rel = relpath(target_real, root_dir)
                if startswith(rel, "..")
                    rm(path; force=true, recursive=true)
                    cp(target_real, path; force=true, follow_symlinks=true)
                    @info "Materialized external symlink" path target=target_real
                end
            elseif isdir(path)
                push!(pending, path)
            end
        end
    end

    return nothing
end

Pkg.activate(PACKAGING_DIR)
Pkg.instantiate()
Pkg.activate(WEBAPP_DIR)
Pkg.resolve()
Pkg.instantiate()
Pkg.activate(PACKAGING_DIR)

using PackageCompiler
using BiocircuitsExplorerPackaging

Base.eval(PackageCompiler, quote
    function ensurecompiled(project::String, packages::Vector{String}, sysimage::String)
        return nothing
    end
end)

mkpath(DIST_DIR)

create_app(
    WEBAPP_DIR,
    APP_DIR;
    executables = ["biocircuits-explorer-backend" => "julia_main"],
    force = true,
    include_lazy_artifacts = true,
    incremental = true,
)

mkpath(RESOURCE_DIR)
cp(joinpath(WEBAPP_DIR, "public"), joinpath(RESOURCE_DIR, "public"); force = true)
materialize_external_symlinks!(APP_DIR)
repair_macos_bundle!(APP_DIR)
ad_hoc_sign_macos_bundle!(APP_DIR)

println("Backend app created at: $(APP_DIR)")
println("Static assets copied to: $(joinpath(RESOURCE_DIR, "public"))")
