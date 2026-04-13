module BiocircuitsExplorerPackaging

export ad_hoc_sign_macos_bundle!, repair_macos_bundle!

const SYSTEM_LIBRARY_PREFIXES = ("/System/Library", "/usr/lib")
const HOMEBREW_PREFIXES = ("/opt/homebrew", "/usr/local")

is_system_library(path::AbstractString) = any(prefix -> startswith(path, prefix), SYSTEM_LIBRARY_PREFIXES)
is_homebrew_library(path::AbstractString) = any(prefix -> startswith(path, prefix), HOMEBREW_PREFIXES)

function is_macho_candidate(path::AbstractString)
    isfile(path) || return false
    endswith(path, ".dylib") && return true
    return stat(path).mode & 0o111 != 0
end

function mach_o_dependencies(path::AbstractString)
    output = try
        read(`otool -L $path`, String)
    catch
        return String[]
    end

    deps = String[]
    for line in Iterators.drop(split(output, '\n'), 1)
        stripped = strip(line)
        isempty(stripped) && continue
        push!(deps, first(split(stripped, " (")))
    end
    return deps
end

function bundle_target_index(root_dir::AbstractString)
    index = Dict{String, Vector{String}}()

    for (current_dir, _, files) in walkdir(root_dir)
        for file in files
            path = joinpath(current_dir, file)
            push!(get!(index, basename(path), String[]), path)
        end
    end

    return index
end

function mach_o_files(root_dir::AbstractString)
    files = String[]

    for (current_dir, _, entries) in walkdir(root_dir)
        for entry in entries
            path = joinpath(current_dir, entry)
            islink(path) && continue
            is_macho_candidate(path) || continue
            isempty(mach_o_dependencies(path)) && continue
            push!(files, path)
        end
    end

    sort!(files)
    return files
end

function make_writable!(path::AbstractString)
    mode = stat(path).mode
    if mode & 0o200 == 0
        chmod(path, mode | 0o200)
    end
    return nothing
end

function runtime_lib_dir(root_dir::AbstractString)
    return joinpath(root_dir, "lib", "julia")
end

function pick_bundled_target(index::Dict{String, Vector{String}}, dep::AbstractString)
    candidates = get(index, basename(dep), String[])
    isempty(candidates) && return nothing

    sort!(candidates; by = path -> (
        occursin("/lib/julia/", path) ? 0 : occursin("/lib/", path) ? 1 : 2,
        length(path),
    ))
    return first(candidates)
end

function ensure_bundled_external_dependencies!(root_dir::AbstractString)
    mkpath(runtime_lib_dir(root_dir))

    index = bundle_target_index(root_dir)
    queue = mach_o_files(root_dir)
    seen = Set(queue)
    copied = Dict{String, String}()

    i = 1
    while i <= length(queue)
        file = queue[i]
        i += 1

        for dep in mach_o_dependencies(file)
            isabspath(dep) || continue
            is_system_library(dep) && continue

            target = pick_bundled_target(index, dep)
            if target === nothing
                dest = joinpath(runtime_lib_dir(root_dir), basename(dep))
                if !ispath(dest)
                    cp(dep, dest; force=true, follow_symlinks=true)
                    make_writable!(dest)
                    @info "Bundled external dylib" source=dep dest
                end
                target = dest
                push!(get!(index, basename(dest), String[]), dest)
            end

            copied[dep] = target

            if !islink(target) && !(target in seen) && is_macho_candidate(target)
                push!(queue, target)
                push!(seen, target)
            end
        end
    end

    return copied
end

function relocate_reference(source_file::AbstractString, target_file::AbstractString)
    rel = relpath(target_file, dirname(source_file))
    return "@loader_path/" * rel
end

function rewrite_install_names!(root_dir::AbstractString, copied::Dict{String, String})
    index = bundle_target_index(root_dir)

    for file in mach_o_files(root_dir)
        changed = false

        for dep in mach_o_dependencies(file)
            target = nothing

            if isabspath(dep) && !is_system_library(dep)
                target = get(copied, dep, nothing)
                target === nothing && (target = pick_bundled_target(index, dep))
            end

            target === nothing && continue
            new_dep = relocate_reference(file, target)
            dep == new_dep && continue

            make_writable!(file)
            run(`install_name_tool -change $dep $new_dep $file`)
            changed = true
        end

        changed && @info "Rewrote install names" file
    end

    return nothing
end

function repair_macos_bundle!(root_dir::AbstractString)
    Sys.isapple() || return nothing

    root_dir = abspath(root_dir)
    copied = ensure_bundled_external_dependencies!(root_dir)
    rewrite_install_names!(root_dir, copied)

    return nothing
end

function ad_hoc_sign_macos_bundle!(root_dir::AbstractString)
    Sys.isapple() || return nothing

    codesign = "/usr/bin/codesign"
    isfile(codesign) || error("Missing codesign at $(codesign)")

    sign_roots = filter(isdir, [
        joinpath(root_dir, "bin"),
        joinpath(root_dir, "lib"),
    ])

    files = String[]
    for sign_root in sign_roots
        append!(files, mach_o_files(sign_root))
    end

    unique!(files)
    sort!(files; rev=true)

    for file in files
        make_writable!(file)
        run(`$codesign --force --sign - $file`)
    end

    return nothing
end

end
