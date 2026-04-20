using JSON3

include(normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl")))
using .BiocircuitsExplorerBackend

function _delete_keys!(raw::AbstractDict, keys)
    for key in keys
        haskey(raw, key) && delete!(raw, key)
    end
    return raw
end

function _parse_chunk_indices(text::AbstractString)
    indices = Int[]
    for part in split(strip(text), ",")
        token = strip(part)
        isempty(token) && continue
        if occursin("-", token)
            bounds = split(token, "-", limit=2)
            start_idx = parse(Int, strip(bounds[1]))
            stop_idx = parse(Int, strip(bounds[2]))
            append!(indices, start_idx <= stop_idx ? (start_idx:stop_idx) : (start_idx:-1:stop_idx))
        else
            push!(indices, parse(Int, token))
        end
    end
    return sort(unique(indices))
end

function main(args)
    length(args) == 6 || error("Usage: julia build_repacked_missing_d3_spec.jl <src_spec.json> <dst_spec.json> <orig_chunk_indices_csv> <new_chunk_size> <network_parallelism> <sqlite_path>")
    src_spec = abspath(args[1])
    dst_spec = abspath(args[2])
    orig_chunk_indices = _parse_chunk_indices(String(args[3]))
    new_chunk_size = parse(Int, args[4])
    network_parallelism = parse(Int, args[5])
    sqlite_path = abspath(args[6])

    spec = BiocircuitsExplorerBackend._materialize(JSON3.read(read(src_spec, String)))
    search_profile = BiocircuitsExplorerBackend.atlas_search_profile_from_raw(get(spec, "search_profile", nothing))
    enum_spec = BiocircuitsExplorerBackend.atlas_enumeration_spec_from_raw(get(spec, "enumeration", nothing))
    original_chunk_size = Int(get(spec, "chunk_size", 64))
    networks, _ = enumerate_network_specs(enum_spec; search_profile=search_profile)

    selected = Any[]
    total_chunks = cld(length(networks), original_chunk_size)
    for idx in orig_chunk_indices
        1 <= idx <= total_chunks || error("Requested chunk index $(idx) outside 1:$(total_chunks).")
        start_idx = (idx - 1) * original_chunk_size + 1
        stop_idx = min(length(networks), start_idx + original_chunk_size - 1)
        append!(selected, networks[start_idx:stop_idx])
    end

    repacked = BiocircuitsExplorerBackend._materialize(spec)
    _delete_keys!(repacked, [
        "enumeration",
        "chunk_indices",
        "shard_count",
        "shard_index",
        "shard_mode",
        "selection_mode",
        "assigned_chunk_indices",
        "discover_only",
        "emit_chunk_specs_only",
        "write_chunk_specs_dir",
        "sqlite_path",
        "source_label",
        "source_metadata",
    ])
    repacked["networks"] = selected
    repacked["chunk_size"] = new_chunk_size
    repacked["network_parallelism"] = network_parallelism
    repacked["persist_sqlite"] = true
    repacked["skip_existing"] = false
    repacked["sqlite_path"] = sqlite_path
    repacked["source_label"] = "report_d3_complex_growth_missing_patch"
    repacked["source_metadata"] = Dict(
        "repacked_from_original_chunks" => orig_chunk_indices,
        "repacked_network_count" => length(selected),
        "original_chunk_size" => original_chunk_size,
        "repacked_chunk_size" => new_chunk_size,
    )

    open(dst_spec, "w") do io
        JSON3.pretty(io, repacked)
        write(io, "\n")
    end

    println(dst_spec)
    println("selected_network_count=$(length(selected))")
    println("original_chunk_count=$(length(orig_chunk_indices))")
    println("repacked_chunk_count=$(cld(length(selected), new_chunk_size))")
end

main(ARGS)
