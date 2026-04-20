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

function _default_summary_path(spec_path::String)
    root, _ = splitext(spec_path)
    return root * ".summary.json"
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

function _sorted_counts(counts::Dict{String, Int})
    return sort(collect(counts); by=first) |> Dict
end

function _increment_count!(counts::Dict{String, Int}, label, delta::Int=1)
    key = label === nothing ? "nothing" : String(label)
    counts[key] = get(counts, key, 0) + delta
    return counts
end

function _elapsed_stats(elapsed::Vector{Float64})
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

function _now_timestamp()
    return Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
end

function _merge_metadata(base_metadata, extra::AbstractDict)
    rendered_extra = Dict{String, Any}(string(k) => v for (k, v) in pairs(extra))
    if base_metadata === nothing
        return rendered_extra
    elseif base_metadata isa AbstractDict
        merged = BiocircuitsExplorerBackend._materialize(base_metadata)
        merge!(merged, rendered_extra)
        return merged
    else
        return Dict(
            "base_metadata" => BiocircuitsExplorerBackend._materialize(base_metadata),
            "streaming_metadata" => rendered_extra,
        )
    end
end

function _new_batch_atlas(
    search_profile,
    behavior_config,
    change_expansion,
    network_parallelism::Int;
    pruned_against_library::Bool=false,
    pruned_against_sqlite::Bool=false,
)
    return Dict(
        "atlas_schema_version" => "0.2.0",
        "generated_at" => _now_timestamp(),
        "search_profile" => BiocircuitsExplorerBackend.atlas_search_profile_to_dict(search_profile),
        "behavior_config" => BiocircuitsExplorerBackend.atlas_behavior_config_to_dict(behavior_config),
        "change_expansion" => BiocircuitsExplorerBackend.atlas_change_expansion_spec_to_dict(change_expansion),
        "network_parallelism" => network_parallelism,
        "input_network_count" => 0,
        "unique_network_count" => 0,
        "successful_network_count" => 0,
        "failed_network_count" => 0,
        "excluded_network_count" => 0,
        "deduplicated_network_count" => 0,
        "pruned_against_library" => pruned_against_library,
        "pruned_against_sqlite" => pruned_against_sqlite,
        "skipped_existing_network_count" => 0,
        "skipped_existing_slice_count" => 0,
        "network_entries" => Dict{String, Any}[],
        "input_graph_slices" => Dict{String, Any}[],
        "behavior_slices" => Dict{String, Any}[],
        "regime_records" => Dict{String, Any}[],
        "transition_records" => Dict{String, Any}[],
        "family_buckets" => Dict{String, Any}[],
        "path_records" => Dict{String, Any}[],
        "duplicate_inputs" => Dict{String, Any}[],
        "skipped_existing_networks" => Dict{String, Any}[],
    )
end

function _record_network_entry!(state::Dict{String, Any}, entry)
    state["unique_network_count"] = Int(state["unique_network_count"]) + 1
    status = String(_dict_get(entry, "analysis_status", "unknown"))
    _increment_count!(state["network_status_counts"], status)
    if status == "ok"
        state["successful_network_count"] = Int(state["successful_network_count"]) + 1
    elseif status == "failed"
        state["failed_network_count"] = Int(state["failed_network_count"]) + 1
        _increment_count!(state["network_failure_class_counts"], _dict_get(entry, "failure_class", "unknown"))
    elseif status == "excluded_by_search_profile"
        state["excluded_network_count"] = Int(state["excluded_network_count"]) + 1
    end
    return state
end

function _append_network_entry!(atlas, entry)
    push!(atlas["network_entries"], Dict{String, Any}(BiocircuitsExplorerBackend._materialize(entry)))
    atlas["unique_network_count"] = Int(_dict_get(atlas, "unique_network_count", 0)) + 1
    status = String(_dict_get(entry, "analysis_status", "unknown"))
    if status == "ok"
        atlas["successful_network_count"] = Int(_dict_get(atlas, "successful_network_count", 0)) + 1
    elseif status == "failed"
        atlas["failed_network_count"] = Int(_dict_get(atlas, "failed_network_count", 0)) + 1
    elseif status == "excluded_by_search_profile"
        atlas["excluded_network_count"] = Int(_dict_get(atlas, "excluded_network_count", 0)) + 1
    end
    return atlas
end

function _append_invalid_entry!(atlas, entry)
    atlas["input_network_count"] = Int(_dict_get(atlas, "input_network_count", 0)) + 1
    _append_network_entry!(atlas, entry)
    return atlas
end

function _append_duplicate_input!(atlas, duplicate)
    atlas["input_network_count"] = Int(_dict_get(atlas, "input_network_count", 0)) + 1
    atlas["deduplicated_network_count"] = Int(_dict_get(atlas, "deduplicated_network_count", 0)) + 1
    push!(atlas["duplicate_inputs"], Dict{String, Any}(BiocircuitsExplorerBackend._materialize(duplicate)))
    return atlas
end

function _record_behavior_slice_counts!(state::Dict{String, Any}, slices)
    state["behavior_slice_count"] = Int(state["behavior_slice_count"]) + length(slices)
    for slice in slices
        status = String(_dict_get(slice, "analysis_status", "unknown"))
        _increment_count!(state["slice_status_counts"], status)
        status == "failed" && _increment_count!(state["slice_failure_class_counts"], _dict_get(slice, "failure_class", "unknown"))
    end
    return state
end

function _append_built_result!(atlas, built)
    atlas["input_network_count"] = Int(_dict_get(atlas, "input_network_count", 0)) + 1

    skipped_existing_networks = _dict_get(built, "skipped_existing_networks", Any[])
    append!(atlas["skipped_existing_networks"], skipped_existing_networks)
    atlas["skipped_existing_network_count"] = Int(_dict_get(atlas, "skipped_existing_network_count", 0)) + length(skipped_existing_networks)
    atlas["skipped_existing_slice_count"] = Int(_dict_get(atlas, "skipped_existing_slice_count", 0)) + Int(_dict_get(built, "skipped_existing_slice_count", 0))

    Bool(_dict_get(built, "include_network_entry", false)) && _append_network_entry!(atlas, _dict_get(built, "network_entry", Dict{String, Any}()))
    append!(atlas["input_graph_slices"], _dict_get(built, "input_graph_slices", Any[]))
    append!(atlas["behavior_slices"], _dict_get(built, "behavior_slices", Any[]))
    append!(atlas["regime_records"], _dict_get(built, "regime_records", Any[]))
    append!(atlas["transition_records"], _dict_get(built, "transition_records", Any[]))
    append!(atlas["family_buckets"], _dict_get(built, "family_buckets", Any[]))
    append!(atlas["path_records"], _dict_get(built, "path_records", Any[]))
    return atlas
end

function _record_slowest_network!(state::Dict{String, Any}, job, built, elapsed_seconds::Float64, worker_index::Int)
    entry = Bool(_dict_get(built, "include_network_entry", false)) ? _dict_get(built, "network_entry", Dict{String, Any}()) : Dict{String, Any}()
    behavior_slices = _dict_get(built, "behavior_slices", Any[])
    path_records = _dict_get(built, "path_records", Any[])
    candidate = Dict(
        "network_index" => Int(job.network_idx),
        "network_id" => String(_dict_get(entry, "network_id", job.canonical_code)),
        "analysis_status" => String(_dict_get(entry, "analysis_status", "unknown")),
        "failure_class" => _dict_get(entry, "failure_class", nothing),
        "elapsed_seconds" => elapsed_seconds,
        "behavior_slice_count" => length(behavior_slices),
        "path_record_count" => length(path_records),
        "worker_index" => worker_index,
    )
    slowest = state["slowest_networks"]
    push!(slowest, candidate)
    sort!(slowest; by=item -> Float64(_dict_get(item, "elapsed_seconds", 0.0)), rev=true)
    length(slowest) > 20 && resize!(slowest, 20)
    return state
end

function _record_built_result!(state::Dict{String, Any}, job, built, elapsed_seconds::Float64, worker_index::Int)
    state["processed_input_network_count"] = Int(state["processed_input_network_count"]) + 1
    state["completed_build_network_count"] = Int(state["completed_build_network_count"]) + 1

    input_graph_slices = _dict_get(built, "input_graph_slices", Any[])
    behavior_slices = _dict_get(built, "behavior_slices", Any[])
    regime_records = _dict_get(built, "regime_records", Any[])
    transition_records = _dict_get(built, "transition_records", Any[])
    family_buckets = _dict_get(built, "family_buckets", Any[])
    path_records = _dict_get(built, "path_records", Any[])
    skipped_existing_networks = _dict_get(built, "skipped_existing_networks", Any[])

    state["input_graph_slice_count"] = Int(state["input_graph_slice_count"]) + length(input_graph_slices)
    state["regime_record_count"] = Int(state["regime_record_count"]) + length(regime_records)
    state["transition_record_count"] = Int(state["transition_record_count"]) + length(transition_records)
    state["family_bucket_count"] = Int(state["family_bucket_count"]) + length(family_buckets)
    state["path_record_count"] = Int(state["path_record_count"]) + length(path_records)
    state["skipped_existing_network_count"] = Int(state["skipped_existing_network_count"]) + length(skipped_existing_networks)
    state["skipped_existing_slice_count"] = Int(state["skipped_existing_slice_count"]) + Int(_dict_get(built, "skipped_existing_slice_count", 0))

    Bool(_dict_get(built, "include_network_entry", false)) && _record_network_entry!(state, _dict_get(built, "network_entry", Dict{String, Any}()))
    _record_behavior_slice_counts!(state, behavior_slices)
    _record_slowest_network!(state, job, built, elapsed_seconds, worker_index)
    return state
end

function _worker_failure_result(job, err)
    network_entry = Dict{String, Any}(BiocircuitsExplorerBackend._materialize(job.network_entry))
    network_entry["analysis_status"] = "failed"
    network_entry["build_state"] = "failed"
    merge!(network_entry, BiocircuitsExplorerBackend._atlas_failure_metadata(err, "streaming_network_build"))
    return Dict(
        "include_network_entry" => true,
        "network_entry" => network_entry,
        "input_graph_slices" => Dict{String, Any}[],
        "behavior_slices" => Dict{String, Any}[],
        "regime_records" => Dict{String, Any}[],
        "transition_records" => Dict{String, Any}[],
        "family_buckets" => Dict{String, Any}[],
        "path_records" => Dict{String, Any}[],
        "skipped_existing_networks" => Dict{String, Any}[],
        "skipped_existing_slice_count" => 0,
    )
end

function _build_run_summary(
    state::Dict{String, Any},
    spec_path::String,
    summary_path::String,
    started_at::String,
    status::String,
    total_network_count::Int,
    build_network_count::Int,
    requested_parallelism::Int,
    resolved_parallelism::Int,
    flush_network_count::Int,
    enumeration_summary,
    sqlite_path,
)
    return Dict(
        "status" => status,
        "execution_mode" => "streaming",
        "spec_path" => spec_path,
        "summary_path" => summary_path,
        "started_at" => started_at,
        "updated_at" => _now_timestamp(),
        "elapsed_seconds" => time() - Float64(state["wall_t0"]),
        "julia_threads" => nthreads(),
        "network_parallelism_requested" => requested_parallelism,
        "network_parallelism_resolved" => resolved_parallelism,
        "flush_network_count" => flush_network_count,
        "total_network_count" => total_network_count,
        "build_network_count" => build_network_count,
        "completed_network_count" => Int(state["completed_build_network_count"]),
        "processed_input_network_count" => Int(state["processed_input_network_count"]),
        "pending_build_network_count" => max(0, build_network_count - Int(state["completed_build_network_count"])),
        "total_chunk_count" => 1,
        "completed_chunk_count" => status == "completed" ? 1 : 0,
        "assigned_chunk_count" => 1,
        "selection_mode" => "streaming",
        "atlas_summary" => Dict(
            "atlas_schema_version" => "0.2.0",
            "generated_at" => _now_timestamp(),
            "input_network_count" => Int(state["processed_input_network_count"]),
            "unique_network_count" => Int(state["unique_network_count"]),
            "successful_network_count" => Int(state["successful_network_count"]),
            "failed_network_count" => Int(state["failed_network_count"]),
            "excluded_network_count" => Int(state["excluded_network_count"]),
            "deduplicated_network_count" => Int(state["deduplicated_network_count"]),
            "input_graph_slice_count" => Int(state["input_graph_slice_count"]),
            "behavior_slice_count" => Int(state["behavior_slice_count"]),
            "regime_record_count" => Int(state["regime_record_count"]),
            "transition_record_count" => Int(state["transition_record_count"]),
            "family_bucket_count" => Int(state["family_bucket_count"]),
            "path_record_count" => Int(state["path_record_count"]),
            "skipped_existing_network_count" => Int(state["skipped_existing_network_count"]),
            "skipped_existing_slice_count" => Int(state["skipped_existing_slice_count"]),
        ),
        "enumeration" => enumeration_summary,
        "network_status_counts" => _sorted_counts(state["network_status_counts"]),
        "network_failure_class_counts" => _sorted_counts(state["network_failure_class_counts"]),
        "slice_status_counts" => _sorted_counts(state["slice_status_counts"]),
        "slice_failure_class_counts" => _sorted_counts(state["slice_failure_class_counts"]),
        "slowest_networks" => state["slowest_networks"],
        "flush_count" => Int(state["flush_count"]),
        "flush_elapsed_seconds" => _elapsed_stats(state["flush_elapsed_seconds"]),
        "sqlite_path" => sqlite_path,
        "sqlite_library_summary" => state["sqlite_summary"],
    )
end

function main(args)
    length(args) >= 1 || error("Usage: julia run_atlas_scan_streaming.jl <spec.json> [summary.json]")
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

    existing_summary = _load_existing_summary(summary_path)
    if existing_summary isa AbstractDict
        if _dict_get(existing_summary, "status", nothing) == "completed"
            println(sprint(io -> JSON3.pretty(io, existing_summary)))
            return existing_summary
        end
        error("Streaming atlas scans are not resumable yet. Found incomplete summary at $(summary_path). Use a fresh run root or remove the stale summary and partial sqlite before rerunning.")
    end

    search_profile = BiocircuitsExplorerBackend.atlas_search_profile_from_raw(_dict_get(spec, "search_profile", nothing))
    behavior_config = BiocircuitsExplorerBackend.atlas_behavior_config_from_raw(_dict_get(spec, "behavior_config", nothing))
    change_expansion = BiocircuitsExplorerBackend.atlas_change_expansion_spec_from_raw(_dict_get(spec, "change_expansion", nothing))
    requested_parallelism = Int(_dict_get(spec, "network_parallelism", 1))
    flush_network_count = max(1, Int(_dict_get(spec, "stream_flush_network_count", _dict_get(spec, "flush_network_count", 8))))
    discover_only = Bool(_dict_get(spec, "discover_only", _env_bool("ATLAS_DISCOVER_ONLY", false)))

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

    started_at = _now_timestamp()
    sqlite_path = get(spec, "sqlite_path", nothing)
    sqlite_persist_mode = _dict_get(spec, "sqlite_persist_mode", nothing)
    source_label = String(_dict_get(spec, "source_label", "atlas_spec"))
    source_metadata = _dict_get(spec, "source_metadata", nothing)
    library = _dict_get(spec, "library", nothing)
    skip_existing = Bool(_dict_get(spec, "skip_existing", library !== nothing || sqlite_path !== nothing))
    persist_sqlite = Bool(_dict_get(spec, "persist_sqlite", false))
    pruned_against_library = skip_existing && library !== nothing
    pruned_against_sqlite = skip_existing && sqlite_path !== nothing

    state = Dict{String, Any}(
        "wall_t0" => time(),
        "processed_input_network_count" => 0,
        "completed_build_network_count" => 0,
        "unique_network_count" => 0,
        "successful_network_count" => 0,
        "failed_network_count" => 0,
        "excluded_network_count" => 0,
        "deduplicated_network_count" => 0,
        "input_graph_slice_count" => 0,
        "behavior_slice_count" => 0,
        "regime_record_count" => 0,
        "transition_record_count" => 0,
        "family_bucket_count" => 0,
        "path_record_count" => 0,
        "skipped_existing_network_count" => 0,
        "skipped_existing_slice_count" => 0,
        "network_status_counts" => Dict{String, Int}(),
        "network_failure_class_counts" => Dict{String, Int}(),
        "slice_status_counts" => Dict{String, Int}(),
        "slice_failure_class_counts" => Dict{String, Int}(),
        "slowest_networks" => Dict{String, Any}[],
        "flush_count" => 0,
        "flush_elapsed_seconds" => Float64[],
        "sqlite_summary" => nothing,
    )

    existing_slice_ids = skip_existing ? BiocircuitsExplorerBackend._library_existing_ok_slice_ids(library) : Set{String}()
    if pruned_against_sqlite
        union!(existing_slice_ids, BiocircuitsExplorerBackend.atlas_sqlite_existing_ok_slice_ids(String(sqlite_path)))
    end

    prepared_jobs = Any[]
    build_jobs = Any[]
    duplicate_inputs = Dict{String, Any}[]
    seen_networks = Set{String}()

    for (network_idx, raw_network) in enumerate(network_specs)
        job = BiocircuitsExplorerBackend._prepare_network_build_job(raw_network, network_idx, search_profile)

        if Bool(job.validation["valid"]) && job.canonical_code in seen_networks
            duplicate = Dict(
                "source_label" => String(BiocircuitsExplorerBackend._raw_get(raw_network, :label, job.canonical_code)),
                "duplicate_of_network_id" => job.canonical_code,
                "reactions" => job.rules,
            )
            push!(duplicate_inputs, duplicate)
            state["processed_input_network_count"] = Int(state["processed_input_network_count"]) + 1
            state["deduplicated_network_count"] = Int(state["deduplicated_network_count"]) + 1
            continue
        end

        if Bool(job.validation["valid"])
            push!(seen_networks, job.canonical_code)
            push!(prepared_jobs, (
                kind=:build,
                network_idx=network_idx,
                build_job=job,
            ))
            push!(build_jobs, job)
        else
            push!(prepared_jobs, (
                kind=:invalid,
                network_idx=network_idx,
                network_entry=job.network_entry,
            ))
            state["processed_input_network_count"] = Int(state["processed_input_network_count"]) + 1
            _record_network_entry!(state, job.network_entry)
        end
    end

    resolved_parallelism = BiocircuitsExplorerBackend._resolve_network_parallelism(requested_parallelism, length(build_jobs))
    initial_summary = _build_run_summary(
        state,
        spec_path,
        summary_path,
        started_at,
        discover_only ? "planned" : "running",
        length(network_specs),
        length(build_jobs),
        requested_parallelism,
        resolved_parallelism,
        flush_network_count,
        enumeration_summary,
        sqlite_path,
    )
    _write_summary(summary_path, initial_summary)

    if discover_only
        initial_summary["finished_at"] = _now_timestamp()
        _write_summary(summary_path, initial_summary)
        println(sprint(io -> JSON3.pretty(io, initial_summary)))
        return initial_summary
    end

    batch_atlas = _new_batch_atlas(
        search_profile,
        behavior_config,
        change_expansion,
        resolved_parallelism;
        pruned_against_library=pruned_against_library,
        pruned_against_sqlite=pruned_against_sqlite,
    )
    batch_build_result_count = 0

    for prepared in prepared_jobs
        prepared.kind == :invalid || continue
        _append_invalid_entry!(batch_atlas, prepared.network_entry)
    end
    for duplicate in duplicate_inputs
        _append_duplicate_input!(batch_atlas, duplicate)
    end

    function flush_batch!()
        if Int(_dict_get(batch_atlas, "input_network_count", 0)) == 0 &&
           isempty(batch_atlas["network_entries"]) &&
           isempty(batch_atlas["input_graph_slices"]) &&
           isempty(batch_atlas["behavior_slices"]) &&
           isempty(batch_atlas["path_records"]) &&
           isempty(batch_atlas["duplicate_inputs"])
            return
        end

        flush_index = Int(state["flush_count"]) + 1
        flush_source_label = string(source_label, ".flush", lpad(flush_index, 4, '0'))
        flush_source_metadata = _merge_metadata(source_metadata, Dict(
            "execution_mode" => "streaming",
            "flush_index" => flush_index,
            "flush_build_result_count" => batch_build_result_count,
            "flush_input_network_count" => Int(_dict_get(batch_atlas, "input_network_count", 0)),
            "completed_build_network_count" => Int(state["completed_build_network_count"]),
            "resolved_network_parallelism" => resolved_parallelism,
            "flush_network_count" => flush_network_count,
        ))

        batch_atlas["generated_at"] = _now_timestamp()
        flush_t0 = time()
        if persist_sqlite && sqlite_path !== nothing
            state["sqlite_summary"] = BiocircuitsExplorerBackend.atlas_sqlite_append_atlas!(
                String(sqlite_path),
                batch_atlas;
                source_label=flush_source_label,
                source_metadata=flush_source_metadata,
                return_summary=true,
                persist_mode=sqlite_persist_mode,
            )
        end
        push!(state["flush_elapsed_seconds"], time() - flush_t0)
        state["flush_count"] = flush_index

        batch_atlas = _new_batch_atlas(
            search_profile,
            behavior_config,
            change_expansion,
            resolved_parallelism;
            pruned_against_library=pruned_against_library,
            pruned_against_sqlite=pruned_against_sqlite,
        )
        batch_build_result_count = 0
        return
    end

    if !isempty(build_jobs)
        output_parallelism = max(1, Threads.nthreads() ÷ max(1, resolved_parallelism))
        channel_size = max(1, min(length(build_jobs), max(2, resolved_parallelism * 2)))
        job_channel = Channel{Any}(channel_size)
        results_channel = Channel{Dict{String, Any}}(channel_size)

        producer = @async begin
            for job in build_jobs
                put!(job_channel, job)
            end
            close(job_channel)
        end

        workers = Task[]
        for worker_index in 1:resolved_parallelism
            push!(workers, Threads.@spawn begin
                for job in job_channel
                    started = time()
                    built = try
                        BiocircuitsExplorerBackend._build_single_network_atlas(job;
                            search_profile=search_profile,
                            behavior_config=behavior_config,
                            change_expansion=change_expansion,
                            output_parallelism=output_parallelism,
                            existing_slice_ids=existing_slice_ids,
                            skip_existing=skip_existing,
                        )
                    catch err
                        _worker_failure_result(job, err)
                    end
                    put!(results_channel, Dict(
                        "worker_index" => worker_index,
                        "elapsed_seconds" => time() - started,
                        "job" => job,
                        "built" => built,
                    ))
                end
            end)
        end

        closer = @async begin
            wait(producer)
            fetch.(workers)
            close(results_channel)
        end

        for message in results_channel
            job = message["job"]
            built = message["built"]
            worker_index = Int(message["worker_index"])
            elapsed_seconds = Float64(message["elapsed_seconds"])

            _record_built_result!(state, job, built, elapsed_seconds, worker_index)
            _append_built_result!(batch_atlas, built)
            batch_build_result_count += 1

            if batch_build_result_count >= flush_network_count
                flush_batch!()
                _write_summary(summary_path, _build_run_summary(
                    state,
                    spec_path,
                    summary_path,
                    started_at,
                    "running",
                    length(network_specs),
                    length(build_jobs),
                    requested_parallelism,
                    resolved_parallelism,
                    flush_network_count,
                    enumeration_summary,
                    sqlite_path,
                ))
            end
        end

        wait(closer)
    end

    flush_batch!()

    completed_summary = _build_run_summary(
        state,
        spec_path,
        summary_path,
        started_at,
        "completed",
        length(network_specs),
        length(build_jobs),
        requested_parallelism,
        resolved_parallelism,
        flush_network_count,
        enumeration_summary,
        sqlite_path,
    )
    completed_summary["finished_at"] = _now_timestamp()
    _write_summary(summary_path, completed_summary)
    println(sprint(io -> JSON3.pretty(io, completed_summary)))
    return completed_summary
end

main(ARGS)
