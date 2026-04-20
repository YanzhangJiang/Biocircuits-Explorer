using JSON3
using Logging
using Dates
using Statistics
using Base.Threads

include(normpath(joinpath(@__DIR__, "..", "src", "BiocircuitsExplorerBackend.jl")))
using .BiocircuitsExplorerBackend

function _dict_get(raw, key::AbstractString, default=nothing)
    haskey(raw, key) && return raw[key]
    return default
end

function _count_by_string(items, key::AbstractString)
    counts = Dict{String, Int}()
    for item in items
        value = _dict_get(item, key, "unknown")
        label = value === nothing ? "nothing" : String(value)
        counts[label] = get(counts, label, 0) + 1
    end
    return sort(collect(counts); by=first) |> Dict
end

function _default_summary_path(spec_path::String)
    root, _ = splitext(spec_path)
    return root * ".summary.json"
end

function _env_int(name::AbstractString, default::Union{Nothing,Int}=nothing)
    value = get(ENV, String(name), "")
    isempty(value) && return default
    return parse(Int, value)
end

function _env_string(name::AbstractString, default::Union{Nothing,String}=nothing)
    value = get(ENV, String(name), "")
    isempty(value) && return default
    return String(value)
end

function _env_bool(name::AbstractString, default::Bool=false)::Bool
    value = lowercase(strip(get(ENV, String(name), "")))
    isempty(value) && return default
    value in ("1", "true", "yes", "on") && return true
    value in ("0", "false", "no", "off") && return false
    error("Environment $(name) must be boolean-like, got $(repr(value)).")
end

function _write_summary(path::String, payload)
    rendered = sprint(io -> JSON3.pretty(io, payload))
    open(path, "w") do io
        write(io, rendered)
        write(io, "\n")
    end
end

function _load_existing_summary(path::String)
    isfile(path) || return nothing
    try
        raw = read(path, String)
        return BiocircuitsExplorerBackend._materialize(JSON3.read(raw))
    catch
        return nothing
    end
end

function _summary_chunks(raw_summary)::Vector{Dict{String, Any}}
    raw_summary isa AbstractDict || return Dict{String, Any}[]
    raw_chunks = _dict_get(raw_summary, "chunks", Any[])
    raw_chunks isa AbstractVector || return Dict{String, Any}[]
    return [BiocircuitsExplorerBackend._materialize(chunk) for chunk in raw_chunks]
end

function _completed_chunk_indices(chunk_summaries)::Set{Int}
    indices = Int[]
    for chunk in chunk_summaries
        idx = _dict_get(chunk, "chunk_index", nothing)
        idx === nothing && continue
        push!(indices, Int(idx))
    end
    return Set(indices)
end

function _chunk_ranges(total::Int, chunk_size::Int)
    ranges = UnitRange{Int}[]
    start_idx = 1
    while start_idx <= total
        stop_idx = min(total, start_idx + chunk_size - 1)
        push!(ranges, start_idx:stop_idx)
        start_idx = stop_idx + 1
    end
    return ranges
end

function _select_chunk_indices(total_chunks::Int, shard_count::Int, shard_index::Int, shard_mode::AbstractString)
    total_chunks == 0 && return Int[]
    shard_count <= 1 && return collect(1:total_chunks)
    1 <= shard_index <= shard_count || error("Shard index $(shard_index) must lie in 1:$(shard_count).")
    mode = lowercase(String(shard_mode))
    if mode == "stride"
        return [idx for idx in 1:total_chunks if mod(idx - shard_index, shard_count) == 0]
    elseif mode == "block"
        block_ranges = _chunk_ranges(total_chunks, cld(total_chunks, shard_count))
        return shard_index <= length(block_ranges) ? collect(block_ranges[shard_index]) : Int[]
    else
        error("Unsupported shard mode=$(shard_mode). Use `stride` or `block`.")
    end
end

function _parse_chunk_indices(raw)::Vector{Int}
    raw isa AbstractVector && return sort(unique(Int[Int(value) for value in raw]))
    text = strip(String(raw))
    isempty(text) && return Int[]
    values = Int[]
    for part in split(text, ",")
        token = strip(part)
        isempty(token) && continue
        if occursin("-", token)
            bounds = split(token, "-", limit=2)
            length(bounds) == 2 || error("Invalid chunk range token $(repr(token)).")
            start_idx = parse(Int, strip(bounds[1]))
            end_idx = parse(Int, strip(bounds[2]))
            step = start_idx <= end_idx ? 1 : -1
            append!(values, start_idx:step:end_idx)
        else
            push!(values, parse(Int, token))
        end
    end
    return sort(unique(values))
end

function _validate_chunk_indices(indices::AbstractVector{<:Integer}, total_chunks::Int)::Vector{Int}
    cleaned = sort(unique(Int[Int(idx) for idx in indices]))
    invalid = [idx for idx in cleaned if idx < 1 || idx > total_chunks]
    isempty(invalid) || error("Chunk indices $(invalid) fall outside 1:$(total_chunks).")
    return cleaned
end

function _merge_metadata(base_metadata, extra::AbstractDict)
    if base_metadata === nothing
        return Dict{String, Any}(string(k) => v for (k, v) in pairs(extra))
    elseif base_metadata isa AbstractDict
        merged = BiocircuitsExplorerBackend._materialize(base_metadata)
        merge!(merged, Dict{String, Any}(string(k) => v for (k, v) in pairs(extra)))
        return merged
    else
        return Dict(
            "base_metadata" => BiocircuitsExplorerBackend._materialize(base_metadata),
            "chunk_metadata" => Dict{String, Any}(string(k) => v for (k, v) in pairs(extra)),
        )
    end
end

function _chunk_status_summary(atlas)
    network_entries = collect(get(atlas, "network_entries", Any[]))
    behavior_slices = collect(get(atlas, "behavior_slices", Any[]))
    return Dict(
        "atlas_summary" => BiocircuitsExplorerBackend._atlas_summary(atlas),
        "network_status_counts" => _count_by_string(network_entries, "analysis_status"),
        "network_failure_class_counts" => _count_by_string(
            filter(entry -> _dict_get(entry, "analysis_status", "") == "failed", network_entries),
            "failure_class",
        ),
        "slice_status_counts" => _count_by_string(behavior_slices, "analysis_status"),
        "slice_failure_class_counts" => _count_by_string(
            filter(slice -> _dict_get(slice, "analysis_status", "") == "failed", behavior_slices),
            "failure_class",
        ),
    )
end

function _chunk_elapsed_stats(chunks)
    elapsed = [Float64(_dict_get(chunk, "elapsed_seconds", 0.0)) for chunk in chunks]
    isempty(elapsed) && return Dict(
        "sum" => 0.0,
        "mean" => 0.0,
        "median" => 0.0,
        "min" => 0.0,
        "max" => 0.0,
    )
    return Dict(
        "sum" => sum(elapsed),
        "mean" => mean(elapsed),
        "median" => median(elapsed),
        "min" => minimum(elapsed),
        "max" => maximum(elapsed),
    )
end

function _delete_keys!(raw::AbstractDict, keys::AbstractVector{<:AbstractString})
    for key in keys
        haskey(raw, key) && delete!(raw, key)
    end
    return raw
end

function _build_chunk_spec(
    spec,
    chunk_networks,
    chunk_idx::Int,
    total_chunk_count::Int,
    chunk_range,
    base_source_label::AbstractString,
    base_source_metadata,
)
    chunk_spec = BiocircuitsExplorerBackend._materialize(spec)
    _delete_keys!(chunk_spec, [
        "enumeration",
        "shard_count",
        "shard_index",
        "shard_mode",
        "chunk_indices",
        "discover_only",
        "emit_chunk_specs_only",
        "write_chunk_specs_dir",
        "sqlite_path",
        "source_label",
        "source_metadata",
        "chunk_identity",
    ])
    chunk_spec["networks"] = chunk_networks
    chunk_spec["chunk_size"] = length(chunk_networks)
    chunk_spec["source_label"] = string(base_source_label, ".chunk", lpad(chunk_idx, 4, '0'))
    chunk_spec["source_metadata"] = _merge_metadata(base_source_metadata, Dict(
        "chunk_index" => chunk_idx,
        "chunk_start_network" => first(chunk_range),
        "chunk_end_network" => last(chunk_range),
        "chunk_network_count" => length(chunk_networks),
    ))
    chunk_spec["chunk_identity"] = Dict(
        "chunk_index" => chunk_idx,
        "total_chunk_count" => total_chunk_count,
        "chunk_start_network" => first(chunk_range),
        "chunk_end_network" => last(chunk_range),
        "chunk_network_count" => length(chunk_networks),
    )
    return chunk_spec
end

function _emit_chunk_specs!(
    output_dir::AbstractString,
    spec,
    network_specs,
    chunk_ranges,
    assigned_chunk_indices,
    base_source_label::AbstractString,
    base_source_metadata,
)
    mkpath(output_dir)
    chunk_spec_paths = String[]
    total_chunk_count = length(chunk_ranges)
    for chunk_idx in assigned_chunk_indices
        chunk_range = chunk_ranges[chunk_idx]
        chunk_networks = network_specs[chunk_range]
        chunk_spec = _build_chunk_spec(
            spec,
            chunk_networks,
            chunk_idx,
            total_chunk_count,
            chunk_range,
            base_source_label,
            base_source_metadata,
        )
        chunk_spec_path = joinpath(output_dir, "chunk_" * lpad(chunk_idx, 4, '0') * ".spec.json")
        _write_summary(chunk_spec_path, chunk_spec)
        push!(chunk_spec_paths, chunk_spec_path)
    end
    return chunk_spec_paths
end

function _partial_summary(
    spec_path::String,
    summary_path::String,
    started_at::String,
    status::String,
    chunk_size::Int,
    total_network_count::Int,
    total_chunk_count::Int,
    chunk_summaries,
    enumeration_summary,
    requested_parallelism::Int,
    sqlite_path,
    shard_index::Union{Nothing,Int},
    shard_count::Int,
    shard_mode::String,
    assigned_chunk_indices,
    selection_mode::String,
)
    sqlite_summary =
        sqlite_path === nothing || status == "running" ? nothing : BiocircuitsExplorerBackend.atlas_sqlite_summary(String(sqlite_path))
    return Dict(
        "status" => status,
        "spec_path" => spec_path,
        "summary_path" => summary_path,
        "started_at" => started_at,
        "updated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        "chunk_size" => chunk_size,
        "total_network_count" => total_network_count,
        "total_chunk_count" => total_chunk_count,
        "completed_chunk_count" => length(chunk_summaries),
        "assigned_chunk_count" => length(assigned_chunk_indices),
        "assigned_chunk_indices" => assigned_chunk_indices,
        "selection_mode" => selection_mode,
        "shard_index" => shard_index,
        "shard_count" => shard_count,
        "shard_mode" => shard_mode,
        "julia_threads" => nthreads(),
        "network_parallelism_requested" => requested_parallelism,
        "enumeration" => enumeration_summary,
        "chunk_elapsed_seconds" => _chunk_elapsed_stats(chunk_summaries),
        "chunks" => chunk_summaries,
        "sqlite_path" => sqlite_path,
        "sqlite_library_summary" => sqlite_summary,
    )
end

function main(args)
    length(args) >= 1 || error("Usage: julia run_atlas_scan_chunked.jl <spec.json> [summary.json]")
    spec_path = abspath(args[1])
    summary_path = length(args) >= 2 ? abspath(args[2]) : _default_summary_path(spec_path)

    global_logger(SimpleLogger(stderr, Logging.Warn))

    raw_spec = read(spec_path, String)
    spec = BiocircuitsExplorerBackend._materialize(JSON3.read(raw_spec))
    haskey(ENV, "ATLAS_SQLITE_PATH") && !isempty(ENV["ATLAS_SQLITE_PATH"]) &&
        (spec["sqlite_path"] = abspath(ENV["ATLAS_SQLITE_PATH"]))
    haskey(ENV, "ATLAS_SOURCE_LABEL") && !isempty(ENV["ATLAS_SOURCE_LABEL"]) &&
        (spec["source_label"] = String(ENV["ATLAS_SOURCE_LABEL"]))
    haskey(ENV, "ATLAS_SOURCE_METADATA_JSON") && !isempty(ENV["ATLAS_SOURCE_METADATA_JSON"]) &&
        (spec["source_metadata"] = BiocircuitsExplorerBackend._materialize(JSON3.read(ENV["ATLAS_SOURCE_METADATA_JSON"])))

    chunk_size = max(1, Int(_dict_get(spec, "chunk_size", 64)))
    started_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    wall_t0 = time()
    persist_sqlite = Bool(_dict_get(spec, "persist_sqlite", false))

    search_profile = BiocircuitsExplorerBackend.atlas_search_profile_from_raw(_dict_get(spec, "search_profile", nothing))
    network_specs = Any[]
    enumeration_summary = nothing

    haskey(spec, "networks") && append!(network_specs, collect(_dict_get(spec, "networks", Any[])))
    if haskey(spec, "enumeration")
        enum_spec = BiocircuitsExplorerBackend.atlas_enumeration_spec_from_raw(_dict_get(spec, "enumeration", nothing))
        enumerated_networks, enumeration_summary = BiocircuitsExplorerBackend.enumerate_network_specs(enum_spec;
            search_profile=search_profile,
        )
        append!(network_specs, enumerated_networks)
    end

    isempty(network_specs) && error("Atlas spec must include `networks` or `enumeration`.")

    chunk_ranges = _chunk_ranges(length(network_specs), chunk_size)
    explicit_chunk_indices = if haskey(spec, "chunk_indices")
        _validate_chunk_indices(_parse_chunk_indices(_dict_get(spec, "chunk_indices", Any[])), length(chunk_ranges))
    else
        env_chunk_indices = _env_string("ATLAS_CHUNK_INDICES", nothing)
        env_chunk_indices === nothing ? Int[] : _validate_chunk_indices(_parse_chunk_indices(env_chunk_indices), length(chunk_ranges))
    end
    shard_count = max(1, Int(_dict_get(spec, "shard_count", something(_env_int("ATLAS_SHARD_COUNT", nothing), 1))))
    shard_index = Int(_dict_get(spec, "shard_index", something(_env_int("ATLAS_SHARD_INDEX", nothing), 1)))
    shard_mode = String(_dict_get(spec, "shard_mode", something(_env_string("ATLAS_SHARD_MODE", nothing), "stride")))
    assigned_chunk_indices, selection_mode = if !isempty(explicit_chunk_indices)
        (explicit_chunk_indices, "explicit_chunk_indices")
    else
        selected = _select_chunk_indices(length(chunk_ranges), shard_count, shard_index, shard_mode)
        mode = shard_count > 1 ? "sharded_" * lowercase(shard_mode) : "all_chunks"
        (selected, mode)
    end
    sqlite_path = get(spec, "sqlite_path", nothing)
    sqlite_persist_mode = _dict_get(spec, "sqlite_persist_mode", nothing)
    base_source_label = String(_dict_get(spec, "source_label", "atlas_spec"))
    base_source_metadata = _dict_get(spec, "source_metadata", nothing)
    requested_parallelism = Int(_dict_get(spec, "network_parallelism", 1))
    discover_only = Bool(_dict_get(spec, "discover_only", _env_bool("ATLAS_DISCOVER_ONLY", false)))
    chunk_specs_dir = _dict_get(spec, "write_chunk_specs_dir", _env_string("ATLAS_WRITE_CHUNK_SPECS_DIR", nothing))
    chunk_specs_dir === nothing || (chunk_specs_dir = abspath(String(chunk_specs_dir)))
    emit_chunk_specs_only = Bool(_dict_get(spec, "emit_chunk_specs_only", _env_bool("ATLAS_EMIT_CHUNK_SPECS_ONLY", false)))
    emit_chunk_specs_only && chunk_specs_dir === nothing && error("Chunk spec emission requires `write_chunk_specs_dir` or ATLAS_WRITE_CHUNK_SPECS_DIR.")

    chunk_identity = _dict_get(spec, "chunk_identity", nothing)
    summary_total_chunk_count = length(chunk_ranges)
    summary_assigned_chunk_indices = assigned_chunk_indices
    summary_selection_mode = selection_mode
    reported_chunk_index = nothing
    reported_chunk_start = nothing
    reported_chunk_end = nothing
    reported_chunk_network_count = nothing
    if chunk_identity isa AbstractDict
        reported_chunk_index = Int(_dict_get(chunk_identity, "chunk_index", 1))
        summary_total_chunk_count = Int(_dict_get(chunk_identity, "total_chunk_count", summary_total_chunk_count))
        summary_assigned_chunk_indices = [reported_chunk_index]
        summary_selection_mode = "prechunked"
        reported_chunk_start = _dict_get(chunk_identity, "chunk_start_network", nothing)
        reported_chunk_end = _dict_get(chunk_identity, "chunk_end_network", nothing)
        reported_chunk_network_count = _dict_get(chunk_identity, "chunk_network_count", nothing)
    end

    chunk_spec_paths = chunk_specs_dir === nothing ? String[] : _emit_chunk_specs!(
        String(chunk_specs_dir),
        spec,
        network_specs,
        chunk_ranges,
        assigned_chunk_indices,
        base_source_label,
        base_source_metadata,
    )

    existing_summary = _load_existing_summary(summary_path)
    chunk_summaries = _summary_chunks(existing_summary)
    if existing_summary isa AbstractDict
        existing_started_at = _dict_get(existing_summary, "started_at", nothing)
        existing_started_at === nothing || (started_at = String(existing_started_at))
    end
    completed_indices = _completed_chunk_indices(chunk_summaries)
    remaining_chunk_indices = [idx for idx in assigned_chunk_indices if !(idx in completed_indices)]

    if existing_summary isa AbstractDict &&
       _dict_get(existing_summary, "status", nothing) == "completed" &&
       isempty(remaining_chunk_indices)
        println(sprint(io -> JSON3.pretty(io, existing_summary)))
        return existing_summary
    end

    _write_summary(summary_path, _partial_summary(
        spec_path,
        summary_path,
        started_at,
        "running",
        chunk_size,
        length(network_specs),
        summary_total_chunk_count,
        chunk_summaries,
        enumeration_summary,
        requested_parallelism,
        sqlite_path,
        shard_count > 1 ? shard_index : nothing,
        shard_count,
        shard_mode,
        summary_assigned_chunk_indices,
        summary_selection_mode,
    ))

    if discover_only || emit_chunk_specs_only
        discovery = _partial_summary(
            spec_path,
            summary_path,
            started_at,
            "planned",
            chunk_size,
            length(network_specs),
            summary_total_chunk_count,
            chunk_summaries,
            enumeration_summary,
            requested_parallelism,
            sqlite_path,
            shard_count > 1 ? shard_index : nothing,
            shard_count,
            shard_mode,
            summary_assigned_chunk_indices,
            summary_selection_mode,
        )
        if !isempty(chunk_spec_paths)
            discovery["chunk_specs_dir"] = chunk_specs_dir
            discovery["chunk_spec_count"] = length(chunk_spec_paths)
            discovery["chunk_spec_paths"] = chunk_spec_paths
        end
        discovery["finished_at"] = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
        discovery["elapsed_seconds"] = time() - wall_t0
        _write_summary(summary_path, discovery)
        println(sprint(io -> JSON3.pretty(io, discovery)))
        return discovery
    end

    for chunk_idx in remaining_chunk_indices
        chunk_range = chunk_ranges[chunk_idx]
        chunk_networks = network_specs[chunk_range]
        chunk_spec = _build_chunk_spec(
            spec,
            chunk_networks,
            chunk_idx,
            summary_total_chunk_count,
            chunk_range,
            base_source_label,
            base_source_metadata,
        )
        chunk_spec["persist_sqlite"] = false

        t0 = time()
        atlas = BiocircuitsExplorerBackend.build_behavior_atlas_from_spec(chunk_spec)
        elapsed_seconds = time() - t0
        sqlite_summary = nothing
        if persist_sqlite && sqlite_path !== nothing
            sqlite_summary = BiocircuitsExplorerBackend.atlas_sqlite_append_atlas!(String(sqlite_path), atlas;
                source_label=chunk_spec["source_label"],
                source_metadata=chunk_spec["source_metadata"],
                return_summary=false,
                persist_mode=sqlite_persist_mode,
            )
            atlas["sqlite_path"] = sqlite_path
            atlas["sqlite_persisted"] = true
            atlas["sqlite_library_summary"] = sqlite_summary
        end

        chunk_summary = Dict(
            "chunk_index" => (reported_chunk_index !== nothing && chunk_idx == 1 ? reported_chunk_index : chunk_idx),
            "chunk_start_network" => (reported_chunk_start !== nothing && chunk_idx == 1 ? reported_chunk_start : first(chunk_range)),
            "chunk_end_network" => (reported_chunk_end !== nothing && chunk_idx == 1 ? reported_chunk_end : last(chunk_range)),
            "chunk_network_count" => (reported_chunk_network_count !== nothing && chunk_idx == 1 ? reported_chunk_network_count : length(chunk_networks)),
            "elapsed_seconds" => elapsed_seconds,
            "source_label" => chunk_spec["source_label"],
        )
        merge!(chunk_summary, _chunk_status_summary(atlas))
        chunk_summary["sqlite_library_summary"] = sqlite_summary
        push!(chunk_summaries, chunk_summary)

        _write_summary(summary_path, _partial_summary(
            spec_path,
            summary_path,
            started_at,
            "running",
            chunk_size,
            length(network_specs),
            summary_total_chunk_count,
            chunk_summaries,
            enumeration_summary,
            requested_parallelism,
            sqlite_path,
            shard_count > 1 ? shard_index : nothing,
            shard_count,
            shard_mode,
            summary_assigned_chunk_indices,
            summary_selection_mode,
        ))

        GC.gc()
    end

    elapsed_seconds = time() - wall_t0
    finished_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

    summary = _partial_summary(
        spec_path,
        summary_path,
        started_at,
        "completed",
        chunk_size,
        length(network_specs),
        summary_total_chunk_count,
        chunk_summaries,
        enumeration_summary,
        requested_parallelism,
        sqlite_path,
        shard_count > 1 ? shard_index : nothing,
        shard_count,
        shard_mode,
        summary_assigned_chunk_indices,
        summary_selection_mode,
    )
    summary["finished_at"] = finished_at
    summary["elapsed_seconds"] = elapsed_seconds

    _write_summary(summary_path, summary)
    println(sprint(io -> JSON3.pretty(io, summary)))
end

main(ARGS)
