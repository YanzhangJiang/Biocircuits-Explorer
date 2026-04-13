Base.@kwdef struct AtlasSearchProfile
    name::String = "binding_small_v0"
    max_base_species::Int = 4
    max_reactions::Int = 5
    max_support::Int = 3
    allow_reversible_binding::Bool = true
    allow_catalysis::Bool = false
    allow_irreversible_steps::Bool = false
    allow_conformational_switches::Bool = false
    allow_higher_order_templates::Bool = false
    slice_mode::Symbol = :siso
    input_mode::Symbol = :totals_only
end

Base.@kwdef struct AtlasBehaviorConfig
    path_scope::Symbol = :feasible
    min_volume_mean::Float64 = 0.01
    deduplicate::Bool = true
    keep_singular::Bool = true
    keep_nonasymptotic::Bool = false
    compute_volume::Bool = false
    motif_zero_tol::Float64 = 1e-6
    include_path_records::Bool = false
end

Base.@kwdef struct AtlasEnumerationSpec
    mode::Symbol = :pairwise_binding
    base_species_counts::Vector{Int} = [2, 3]
    min_reactions::Int = 1
    max_reactions::Int = 2
    limit::Int = 0
end

Base.@kwdef struct AtlasQuerySpec
    motif_labels::Vector{String} = String[]
    exact_labels::Vector{String} = String[]
    motif_match_mode::Symbol = :any
    exact_match_mode::Symbol = :any
    input_symbols::Vector{String} = String[]
    output_symbols::Vector{String} = String[]
    require_robust::Bool = false
    min_robust_path_count::Int = 0
    max_base_species::Union{Nothing, Int} = nothing
    max_reactions::Union{Nothing, Int} = nothing
    max_support::Union{Nothing, Int} = nothing
    max_support_mass::Union{Nothing, Int} = nothing
    required_regimes::Vector{Dict{String, Any}} = Dict{String, Any}[]
    forbidden_regimes::Vector{Dict{String, Any}} = Dict{String, Any}[]
    required_transitions::Vector{Dict{String, Any}} = Dict{String, Any}[]
    forbidden_transitions::Vector{Dict{String, Any}} = Dict{String, Any}[]
    required_path_sequences::Vector{Vector{Dict{String, Any}}} = Vector{Dict{String, Any}}[]
    forbid_singular_on_witness::Bool = false
    require_witness_feasible::Bool = false
    require_witness_robust::Bool = false
    min_witness_volume_mean::Union{Nothing, Float64} = nothing
    max_witness_path_length::Union{Nothing, Int} = nothing
    ranking_mode::Symbol = :minimal_first
    collapse_by_network::Bool = false
    pareto_only::Bool = false
    limit::Int = 50
end

Base.@kwdef struct InverseDesignSpec
    source_label::String = "inverse_design_run"
    skip_existing::Bool = true
    build_library_if_missing::Bool = true
    return_library::Bool = true
    return_delta_atlas::Bool = true
end

Base.@kwdef struct InverseRefinementSpec
    enabled::Bool = false
    top_k::Int = 5
    trials::Int = 8
    param_min::Float64 = -6.0
    param_max::Float64 = 6.0
    n_points::Int = 200
    background_min::Float64 = -3.0
    background_max::Float64 = 3.0
    flat_abs_tol::Float64 = 0.01
    flat_rel_tol::Float64 = 0.05
    include_traces::Bool = true
    rerank_by_refinement::Bool = true
    rng_seed::Int = 1234
end

atlas_search_profile_binding_small_v0() = AtlasSearchProfile()
atlas_behavior_config_default() = AtlasBehaviorConfig()
atlas_enumeration_spec_default() = AtlasEnumerationSpec()
atlas_query_spec_default() = AtlasQuerySpec()
inverse_design_spec_default() = InverseDesignSpec()
inverse_refinement_spec_default() = InverseRefinementSpec()
atlas_library_default() = Dict(
    "atlas_library_schema_version" => "0.2.0",
    "atlas_schema_version" => "0.2.0",
    "generated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    "created_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    "updated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    "atlas_manifests" => Dict{String, Any}[],
    "merge_events" => Dict{String, Any}[],
    "network_entries" => Dict{String, Any}[],
    "input_graph_slices" => Dict{String, Any}[],
    "behavior_slices" => Dict{String, Any}[],
    "regime_records" => Dict{String, Any}[],
    "transition_records" => Dict{String, Any}[],
    "family_buckets" => Dict{String, Any}[],
    "path_records" => Dict{String, Any}[],
    "duplicate_inputs" => Dict{String, Any}[],
    "atlas_count" => 0,
    "input_network_count" => 0,
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
    "support_screen_cache" => Dict{String, Any}[],
    "negative_certificate_store" => Dict{String, Any}[],
    "soft_note_store" => Dict{String, Any}[],
    "materialization_events" => Dict{String, Any}[],
    "query_audit_log" => Dict{String, Any}[],
)

function _raw_haskey(raw, key::Symbol)
    return haskey(raw, key) || haskey(raw, String(key))
end

function _raw_get(raw, key::Symbol, default)
    if haskey(raw, key)
        return raw[key]
    elseif haskey(raw, String(key))
        return raw[String(key)]
    else
        return default
    end
end

_now_iso_timestamp() = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

function _materialize(value)
    if value isa AbstractDict
        out = Dict{String, Any}()
        for (k, v) in pairs(value)
            out[String(k)] = _materialize(v)
        end
        return out
    elseif value isa AbstractVector || value isa Tuple
        return Any[_materialize(v) for v in value]
    elseif value isa Symbol
        return String(value)
    else
        return value
    end
end

function _sorted_unique_strings(values)
    items = String[]
    for value in values
        value === nothing && continue
        str = String(value)
        isempty(str) && continue
        push!(items, str)
    end
    return sort!(unique(items))
end

function _materialize_dict_vector(values)
    out = Dict{String, Any}[]
    for value in values
        value isa AbstractDict || continue
        push!(out, Dict{String, Any}(_materialize(value)))
    end
    return out
end

function _materialize_predicate_sequences(values)
    sequences = Vector{Vector{Dict{String, Any}}}()
    for value in values
        value isa AbstractVector || continue
        push!(sequences, _materialize_dict_vector(value))
    end
    return sequences
end

function _query_section(raw, key::Symbol)
    raw === nothing && return nothing
    _raw_haskey(raw, key) || return nothing
    section = _raw_get(raw, key, nothing)
    return section isa AbstractDict ? section : nothing
end

function _query_value(raw, key::Symbol; section=nothing, aliases::Vector{Symbol}=Symbol[])
    raw === nothing && return begin
        if section !== nothing
            if _raw_haskey(section, key)
                return _raw_get(section, key, nothing)
            end
            for alias in aliases
                if _raw_haskey(section, alias)
                    return _raw_get(section, alias, nothing)
                end
            end
        end
        nothing
    end
    if _raw_haskey(raw, key)
        return _raw_get(raw, key, nothing)
    end
    for alias in aliases
        if _raw_haskey(raw, alias)
            return _raw_get(raw, alias, nothing)
        end
    end
    if section !== nothing
        if _raw_haskey(section, key)
            return _raw_get(section, key, nothing)
        end
        for alias in aliases
            if _raw_haskey(section, alias)
                return _raw_get(section, alias, nothing)
            end
        end
    end
    return nothing
end

function _goal_string_list(value)
    if value === nothing
        return String[]
    elseif value isa AbstractString
        return _sorted_unique_strings(split(String(value), ','))
    elseif value isa AbstractVector
        return _sorted_unique_strings(value)
    else
        return _sorted_unique_strings([value])
    end
end

function _goal_path_parts(value::AbstractString)
    text = strip(String(value))
    isempty(text) && return String[]
    return [strip(part) for part in split(text, r"\s*(?:->|=>|→)\s*") if !isempty(strip(part))]
end

function _goal_order_token_candidate(value::AbstractString)
    token = strip(String(value))
    isempty(token) && return nothing
    normalized = _normalize_query_output_order_token(token)
    normalized === nothing && return nothing
    lowered = lowercase(token)
    if occursin(r"^(?:[+\-]?\d+(?:\.\d+)?|(?:\+|-)?inf|nan)$"i, token) || lowered in ("inf", "+inf", "-inf", "nan")
        return normalized
    end
    return nothing
end

function _goal_regime_predicate_from_token(value)
    value isa AbstractDict && return Dict{String, Any}(_materialize(value))
    value === nothing && return Dict{String, Any}()

    token = strip(String(value))
    isempty(token) && return Dict{String, Any}()
    lowered = lowercase(token)
    predicate = Dict{String, Any}()

    if lowered == "singular"
        predicate["singular"] = true
        return predicate
    elseif lowered in ("regular", "nonsingular", "non_singular")
        predicate["singular"] = false
        return predicate
    elseif lowered == "asymptotic"
        predicate["asymptotic"] = true
        return predicate
    elseif lowered in ("nonasymptotic", "non_asymptotic")
        predicate["asymptotic"] = false
        return predicate
    elseif lowered in ("source", "sink", "interior", "branch", "merge", "source_sink", "branch_merge")
        predicate["role"] = lowered
        return predicate
    end

    if occursin(':', token)
        left, right = split(token, ':'; limit=2)
        role = lowercase(strip(left))
        rhs = strip(right)
        if role in ("source", "sink", "interior", "branch", "merge", "source_sink", "branch_merge")
            predicate["role"] = role
            if !isempty(rhs)
                rhs_predicate = _goal_regime_predicate_from_token(rhs)
                merge!(predicate, rhs_predicate)
            end
            return predicate
        end
    end

    if occursin('=', token)
        left, right = split(token, '='; limit=2)
        key = lowercase(strip(left))
        rhs = strip(right)
        if key in ("role", "output_order_token", "nullity")
            predicate[key] = key == "nullity" ? Int(rhs) : (key == "output_order_token" ? _normalize_query_output_order_token(rhs) : rhs)
            return predicate
        elseif key in ("singular", "asymptotic", "source", "sink", "branch", "merge", "reachable_from_source", "can_reach_sink")
            predicate[key] = lowercase(rhs) in ("1", "true", "yes", "y")
            return predicate
        end
    end

    order_token = _goal_order_token_candidate(token)
    order_token === nothing || (predicate["output_order_token"] = order_token)
    return predicate
end

function _goal_regime_predicate_list(value)
    if value === nothing
        return Dict{String, Any}[]
    elseif value isa AbstractVector
        out = Dict{String, Any}[]
        for item in value
            pred = _goal_regime_predicate_from_token(item)
            isempty(pred) || push!(out, pred)
        end
        return out
    else
        return _goal_regime_predicate_list(_goal_string_list(value))
    end
end

function _goal_transition_predicate_from_token(value)
    value isa AbstractDict && return Dict{String, Any}(_materialize(value))
    value === nothing && return Dict{String, Any}()
    parts = _goal_path_parts(String(value))
    length(parts) == 2 || return Dict{String, Any}()
    from_pred = _goal_regime_predicate_from_token(parts[1])
    to_pred = _goal_regime_predicate_from_token(parts[2])
    if isempty(from_pred) || isempty(to_pred)
        return Dict{String, Any}()
    end
    token = get(from_pred, "output_order_token", nothing) !== nothing &&
            get(to_pred, "output_order_token", nothing) !== nothing ?
            string(from_pred["output_order_token"], "->", to_pred["output_order_token"]) :
            nothing
    predicate = Dict{String, Any}(
        "from" => from_pred,
        "to" => to_pred,
    )
    token === nothing || (predicate["transition_token"] = token)
    return predicate
end

function _goal_transition_predicate_list(value)
    if value === nothing
        return Dict{String, Any}[]
    elseif value isa AbstractVector
        out = Dict{String, Any}[]
        for item in value
            pred = _goal_transition_predicate_from_token(item)
            isempty(pred) || push!(out, pred)
        end
        return out
    else
        return _goal_transition_predicate_list(_goal_string_list(value))
    end
end

function _goal_path_sequence_list(value)
    if value === nothing
        return Vector{Vector{Dict{String, Any}}}()
    elseif value isa AbstractVector
        if isempty(value)
            return Vector{Vector{Dict{String, Any}}}()
        elseif first(value) isa AbstractVector
            out = Vector{Vector{Dict{String, Any}}}()
            for seq in value
                parsed = _goal_path_sequence_list(seq)
                isempty(parsed) || push!(out, parsed[1])
            end
            return out
        else
            sequence = Dict{String, Any}[]
            for item in value
                pred = _goal_regime_predicate_from_token(item)
                isempty(pred) || push!(sequence, pred)
            end
            return isempty(sequence) ? Vector{Vector{Dict{String, Any}}}() : [sequence]
        end
    elseif value isa AbstractString
        sequence = Dict{String, Any}[]
        for item in _goal_path_parts(String(value))
            pred = _goal_regime_predicate_from_token(item)
            isempty(pred) || push!(sequence, pred)
        end
        return isempty(sequence) ? Vector{Vector{Dict{String, Any}}}() : [sequence]
    else
        return Vector{Vector{Dict{String, Any}}}()
    end
end

function _goal_merge_predicate_vectors(args...)
    out = Dict{String, Any}[]
    seen = Set{String}()
    for records in args
        for record in records
            encoded = JSON3.write(_materialize(record))
            encoded in seen && continue
            push!(seen, encoded)
            push!(out, Dict{String, Any}(_materialize(record)))
        end
    end
    return out
end

function _goal_merge_sequence_vectors(args...)
    out = Vector{Vector{Dict{String, Any}}}()
    seen = Set{String}()
    for sequences in args
        for sequence in sequences
            encoded = JSON3.write(_materialize(sequence))
            encoded in seen && continue
            push!(seen, encoded)
            push!(out, [Dict{String, Any}(_materialize(item)) for item in sequence])
        end
    end
    return out
end

function _goal_io_symbols(goal)
    goal isa AbstractDict || return (String[], String[])
    input_symbols = String[]
    output_symbols = String[]

    io_value = _query_value(goal, :io; aliases=[:io_pair])
    if io_value isa AbstractString
        parts = _goal_path_parts(String(io_value))
        if length(parts) == 2
            append!(input_symbols, _goal_string_list(parts[1]))
            append!(output_symbols, _goal_string_list(parts[2]))
        end
    elseif io_value isa AbstractDict
        append!(input_symbols, _goal_string_list(_query_value(io_value, :input; aliases=[:inputs, :input_symbols, :change_qK])))
        append!(output_symbols, _goal_string_list(_query_value(io_value, :output; aliases=[:outputs, :output_symbols, :observe_x])))
    end

    append!(input_symbols, _goal_string_list(_query_value(goal, :input; aliases=[:inputs, :input_symbols])))
    append!(output_symbols, _goal_string_list(_query_value(goal, :output; aliases=[:outputs, :output_symbols])))
    return (_sorted_unique_strings(input_symbols), _sorted_unique_strings(output_symbols))
end

function _ensure_string_vector_field!(obj::Dict{String, Any}, key::String)
    current = haskey(obj, key) ? obj[key] : String[]
    obj[key] = _sorted_unique_strings(current isa AbstractVector ? current : String[])
    return obj[key]
end

function _append_unique_string_field!(obj::Dict{String, Any}, key::String, values)
    current = haskey(obj, key) ? obj[key] : String[]
    merged = Any[]
    append!(merged, current isa AbstractVector ? collect(current) : Any[])
    append!(merged, collect(values))
    obj[key] = _sorted_unique_strings(merged)
    return obj[key]
end

function _status_priority(status::AbstractString)
    status == "ok" && return 4
    status == "failed" && return 3
    status == "excluded_by_search_profile" && return 2
    status == "pending" && return 1
    return 0
end

function _merge_status(statuses)
    best = "unknown"
    priority = -1
    for status in statuses
        str = String(status)
        p = _status_priority(str)
        if p > priority
            best = str
            priority = p
        end
    end
    return best
end

function _atlas_slice_count_map(records, id_key::AbstractString)
    counts = Dict{String, Int}()
    key_sym = Symbol(id_key)
    for record in records
        slice_id = String(_raw_get(record, key_sym, ""))
        isempty(slice_id) && continue
        counts[slice_id] = get(counts, slice_id, 0) + 1
    end
    return counts
end

function _atlas_slice_count_maps(regime_records, transition_records, family_buckets, path_records)
    return Dict(
        "regime_record_count" => _atlas_slice_count_map(regime_records, "slice_id"),
        "transition_record_count" => _atlas_slice_count_map(transition_records, "slice_id"),
        "family_bucket_count" => _atlas_slice_count_map(family_buckets, "slice_id"),
        "path_record_count" => _atlas_slice_count_map(path_records, "slice_id"),
    )
end

function _atlas_slice_counts(slice_id::AbstractString, count_maps)
    return Dict(
        "regime_record_count" => get(get(count_maps, "regime_record_count", Dict{String, Int}()), String(slice_id), 0),
        "transition_record_count" => get(get(count_maps, "transition_record_count", Dict{String, Int}()), String(slice_id), 0),
        "family_bucket_count" => get(get(count_maps, "family_bucket_count", Dict{String, Int}()), String(slice_id), 0),
        "path_record_count" => get(get(count_maps, "path_record_count", Dict{String, Int}()), String(slice_id), 0),
    )
end

function _atlas_error_text(err)::String
    return sprint(showerror, err)
end

function _atlas_error_is_high_nullity(error_text)::Bool
    lowered = lowercase(String(error_text))
    return occursin("atlas_nullity_gt_1", lowered) ||
           occursin("nullity > 1", lowered) ||
           occursin("nullity is bigger than 1", lowered) ||
           occursin("getindex(::nothing", lowered)
end

function _atlas_failure_metadata(err, stage::AbstractString; partial_result_available::Bool=false, integrity_issues=String[])
    error_text = _atlas_error_text(err)
    failure_class = _atlas_error_is_high_nullity(error_text) ? "unsupported_high_nullity" : "build_error"
    metadata = Dict{String, Any}(
        "build_state" => partial_result_available ? "partial_failed" : "failed",
        "partial_result_available" => partial_result_available,
        "failure_stage" => String(stage),
        "failure_class" => failure_class,
        "error" => error_text,
    )
    if !isempty(integrity_issues)
        metadata["integrity_issues"] = _sorted_unique_strings(integrity_issues)
    end
    if failure_class == "unsupported_high_nullity"
        metadata["unsupported_feature"] = "nullity_gt_1_output_order_materialization"
        metadata["failure_message"] = "Encountered a regime with nullity > 1 while materializing atlas slice records."
    end
    return metadata
end

function _atlas_slice_has_partial_result(slice)::Bool
    return _raw_haskey(slice, :total_paths) ||
           _raw_haskey(slice, :feasible_paths) ||
           _raw_haskey(slice, :included_paths) ||
           _raw_haskey(slice, :motif_union) ||
           _raw_haskey(slice, :exact_union) ||
           _raw_haskey(slice, :motif_family_count) ||
           _raw_haskey(slice, :exact_family_count)
end

function _atlas_slice_is_complete(slice, counts::AbstractDict=Dict{String, Int}())::Bool
    String(_raw_get(slice, :analysis_status, "failed")) == "ok" || return false
    build_state = String(_raw_get(slice, :build_state, ""))
    build_state == "complete" && return true
    build_state in ("failed", "partial_failed") && return false
    return Int(get(counts, "regime_record_count", 0)) > 0 &&
           Int(get(counts, "family_bucket_count", 0)) > 0
end

function _replace_dict_contents!(target::AbstractDict, source::AbstractDict)
    for key in collect(keys(target))
        delete!(target, key)
    end
    merge!(target, _materialize(source))
    return target
end

function _atlas_object_rank(obj)::Int
    if _atlas_slice_is_complete(obj)
        return 3
    end
    status = String(_raw_get(obj, :analysis_status, "unknown"))
    if status == "failed"
        return Bool(_raw_get(obj, :partial_result_available, false)) ? 2 : 1
    elseif status == "pending"
        return 1
    end
    return 0
end

function _prefer_incoming_atlas_object(existing, incoming)::Bool
    incoming_rank = _atlas_object_rank(incoming)
    existing_rank = _atlas_object_rank(existing)
    incoming_rank != existing_rank && return incoming_rank > existing_rank
    return _status_priority(String(_raw_get(incoming, :analysis_status, "unknown"))) >
           _status_priority(String(_raw_get(existing, :analysis_status, "unknown")))
end

function _annotate_behavior_slice!(slice::AbstractDict, counts::AbstractDict=Dict{String, Int}(); network_error=nothing)
    slice["regime_record_count"] = Int(get(counts, "regime_record_count", 0))
    slice["transition_record_count"] = Int(get(counts, "transition_record_count", 0))
    slice["family_bucket_count"] = Int(get(counts, "family_bucket_count", 0))
    slice["path_record_count"] = Int(get(counts, "path_record_count", 0))

    if _atlas_slice_is_complete(slice, counts)
        slice["analysis_status"] = "ok"
        slice["build_state"] = "complete"
        slice["partial_result_available"] = false
        for key in ("failure_class", "failure_stage", "failure_message", "unsupported_feature", "integrity_issues", "error")
            haskey(slice, key) && delete!(slice, key)
        end
        return slice
    end

    partial_result_available = _atlas_slice_has_partial_result(slice)
    integrity_issues = String[]
    Int(get(counts, "regime_record_count", 0)) == 0 && push!(integrity_issues, "missing_regime_records")
    Int(get(counts, "transition_record_count", 0)) == 0 && push!(integrity_issues, "missing_transition_records")
    Int(get(counts, "family_bucket_count", 0)) == 0 && push!(integrity_issues, "missing_family_buckets")

    error_text = String(_raw_get(slice, :error, ""))
    if isempty(error_text) && network_error !== nothing
        error_text = String(network_error)
    end

    if _atlas_error_is_high_nullity(error_text)
        slice["failure_class"] = "unsupported_high_nullity"
        slice["unsupported_feature"] = "nullity_gt_1_output_order_materialization"
        slice["failure_message"] = "Encountered a regime with nullity > 1 while materializing atlas slice records."
    elseif isempty(String(_raw_get(slice, :failure_class, "")))
        slice["failure_class"] = isempty(integrity_issues) ? "build_error" : "incomplete_slice_records"
    end

    if isempty(String(_raw_get(slice, :failure_stage, "")))
        slice["failure_stage"] = partial_result_available ? "slice_record_materialization" : "behavior_families"
    end
    isempty(error_text) || (slice["error"] = error_text)
    slice["integrity_issues"] = _sorted_unique_strings(integrity_issues)
    slice["build_state"] = partial_result_available ? "partial_failed" : "failed"
    slice["partial_result_available"] = partial_result_available
    slice["analysis_status"] = "failed"
    return slice
end

function _normalize_behavior_slices!(library::Dict{String, Any})
    behavior_slices = Vector{Any}(collect(_raw_get(library, :behavior_slices, Any[])))
    network_entries = Vector{Any}(collect(_raw_get(library, :network_entries, Any[])))
    count_maps = _atlas_slice_count_maps(
        collect(_raw_get(library, :regime_records, Any[])),
        collect(_raw_get(library, :transition_records, Any[])),
        collect(_raw_get(library, :family_buckets, Any[])),
        collect(_raw_get(library, :path_records, Any[])),
    )
    network_errors = Dict(
        String(_raw_get(entry, :network_id, "")) => String(_raw_get(entry, :error, ""))
        for entry in network_entries if !isempty(String(_raw_get(entry, :network_id, "")))
    )

    for slice_any in behavior_slices
        slice = slice_any isa AbstractDict ? slice_any : Dict{String, Any}(slice_any)
        slice_id = String(_raw_get(slice, :slice_id, ""))
        network_id = String(_raw_get(slice, :network_id, ""))
        _annotate_behavior_slice!(slice, _atlas_slice_counts(slice_id, count_maps);
            network_error=get(network_errors, network_id, nothing),
        )
    end

    library["behavior_slices"] = behavior_slices
    return library
end

function _network_build_state(summary)::String
    successful = Int(_raw_get(summary, :successful_slice_count, 0))
    failed = Int(_raw_get(summary, :failed_slice_count, 0))
    if successful > 0 && failed == 0
        return "complete"
    elseif successful > 0 && failed > 0
        return "partial_failed"
    elseif failed > 0
        return "failed"
    end
    return "pending"
end

function _is_atlas_corpus(raw)
    return _raw_haskey(raw, :network_entries) &&
           _raw_haskey(raw, :behavior_slices) &&
           _raw_haskey(raw, :family_buckets)
end

is_atlas_library(raw) = _raw_haskey(raw, :atlas_library_schema_version)

function atlas_search_profile_from_raw(raw=nothing)
    profile = atlas_search_profile_binding_small_v0()
    raw === nothing && return profile
    return AtlasSearchProfile(
        name=String(_raw_get(raw, :name, profile.name)),
        max_base_species=Int(_raw_get(raw, :max_base_species, profile.max_base_species)),
        max_reactions=Int(_raw_get(raw, :max_reactions, profile.max_reactions)),
        max_support=Int(_raw_get(raw, :max_support, profile.max_support)),
        allow_reversible_binding=Bool(_raw_get(raw, :allow_reversible_binding, profile.allow_reversible_binding)),
        allow_catalysis=Bool(_raw_get(raw, :allow_catalysis, profile.allow_catalysis)),
        allow_irreversible_steps=Bool(_raw_get(raw, :allow_irreversible_steps, profile.allow_irreversible_steps)),
        allow_conformational_switches=Bool(_raw_get(raw, :allow_conformational_switches, profile.allow_conformational_switches)),
        allow_higher_order_templates=Bool(_raw_get(raw, :allow_higher_order_templates, profile.allow_higher_order_templates)),
        slice_mode=Symbol(_raw_get(raw, :slice_mode, profile.slice_mode)),
        input_mode=Symbol(_raw_get(raw, :input_mode, profile.input_mode)),
    )
end

function atlas_behavior_config_from_raw(raw=nothing)
    config = atlas_behavior_config_default()
    raw === nothing && return config
    return AtlasBehaviorConfig(
        path_scope=Symbol(_raw_get(raw, :path_scope, config.path_scope)),
        min_volume_mean=Float64(_raw_get(raw, :min_volume_mean, config.min_volume_mean)),
        deduplicate=Bool(_raw_get(raw, :deduplicate, config.deduplicate)),
        keep_singular=Bool(_raw_get(raw, :keep_singular, config.keep_singular)),
        keep_nonasymptotic=Bool(_raw_get(raw, :keep_nonasymptotic, config.keep_nonasymptotic)),
        compute_volume=Bool(_raw_get(raw, :compute_volume, config.compute_volume)),
        motif_zero_tol=Float64(_raw_get(raw, :motif_zero_tol, config.motif_zero_tol)),
        include_path_records=Bool(_raw_get(raw, :include_path_records, config.include_path_records)),
    )
end

function atlas_enumeration_spec_from_raw(raw=nothing)
    spec = atlas_enumeration_spec_default()
    raw === nothing && return spec

    base_species_counts = if _raw_haskey(raw, :base_species_counts)
        sort!(unique(Int.(collect(_raw_get(raw, :base_species_counts, Int[])))))
    elseif _raw_haskey(raw, :base_species_count)
        [Int(_raw_get(raw, :base_species_count, 2))]
    else
        copy(spec.base_species_counts)
    end

    return AtlasEnumerationSpec(
        mode=Symbol(_raw_get(raw, :mode, spec.mode)),
        base_species_counts=base_species_counts,
        min_reactions=Int(_raw_get(raw, :min_reactions, spec.min_reactions)),
        max_reactions=Int(_raw_get(raw, :max_reactions, spec.max_reactions)),
        limit=Int(_raw_get(raw, :limit, spec.limit)),
    )
end

function atlas_query_spec_from_raw(raw=nothing)
    spec = atlas_query_spec_default()
    raw === nothing && return spec
    graph_spec = _query_section(raw, :graph_spec)
    path_spec = _query_section(raw, :path_spec)
    polytope_spec = _query_section(raw, :polytope_spec)
    goal = _query_section(raw, :goal)

    goal_inputs, goal_outputs = _goal_io_symbols(goal)
    goal_motifs = _goal_string_list(_query_value(goal, :motif; aliases=[:motifs, :motif_labels]))
    goal_exacts = _goal_string_list(_query_value(goal, :exact; aliases=[:exacts, :exact_labels]))
    goal_required_regimes = _goal_regime_predicate_list(_query_value(goal, :must_regimes; aliases=[:required_regimes, :regimes, :must_have_regimes]))
    goal_forbidden_regimes = _goal_regime_predicate_list(_query_value(goal, :forbid_regimes; aliases=[:forbidden_regimes, :forbidden_nodes]))
    goal_required_transitions = _goal_transition_predicate_list(_query_value(goal, :must_transitions; aliases=[:required_transitions, :transitions, :must_have_transitions]))
    goal_forbidden_transitions = _goal_transition_predicate_list(_query_value(goal, :forbid_transitions; aliases=[:forbidden_transitions]))
    goal_required_path_sequences = _goal_path_sequence_list(_query_value(goal, :witness; aliases=[:witness_path, :path, :required_path_sequences]))

    goal_size = _query_section(goal, :max_size)
    goal_ranking_value = _query_value(goal, :ranking; aliases=[:ranking_mode])
    goal_ranking_mode = if goal_ranking_value === nothing
        nothing
    else
        ranking_str = lowercase(String(goal_ranking_value))
        ranking_str in ("minimal", "minimal_first") ? :minimal_first :
        ranking_str in ("robust", "robustness", "robustness_first") ? :robustness_first :
        Symbol(goal_ranking_value)
    end

    goal_robust = _query_value(goal, :robust; aliases=[:require_robust])
    goal_feasible = _query_value(goal, :feasible; aliases=[:require_feasible])
    goal_collapse = _query_value(goal, :collapse; aliases=[:collapse_by_network])
    goal_pareto = _query_value(goal, :pareto; aliases=[:pareto_only])
    goal_limit = _query_value(goal, :limit)
    goal_min_volume = _query_value(goal, :min_volume; section=goal, aliases=[:min_volume_mean])
    goal_max_path_length = _query_value(goal, :max_path_length; aliases=[:max_witness_path_length])

    required_path_sequences_raw = begin
        top_level = _query_value(raw, :required_path_sequences; section=path_spec, aliases=[:required_sequences])
        if top_level !== nothing
            collect(top_level)
        else
            exists_sequence = _query_value(raw, :exists_sequence; section=path_spec)
            exists_sequence isa AbstractVector ? Any[collect(exists_sequence)] : Any[]
        end
    end

    return AtlasQuerySpec(
        motif_labels=sort!(unique(vcat(String.(collect(_raw_get(raw, :motif_labels, String[]))), goal_motifs))),
        exact_labels=sort!(unique(vcat(String.(collect(_raw_get(raw, :exact_labels, String[]))), goal_exacts))),
        motif_match_mode=Symbol(_raw_get(raw, :motif_match_mode, spec.motif_match_mode)),
        exact_match_mode=Symbol(_raw_get(raw, :exact_match_mode, spec.exact_match_mode)),
        input_symbols=sort!(unique(vcat(String.(collect(_raw_get(raw, :input_symbols, String[]))), goal_inputs))),
        output_symbols=sort!(unique(vcat(String.(collect(_raw_get(raw, :output_symbols, String[]))), goal_outputs))),
        require_robust=Bool(something(_query_value(raw, :require_robust), goal_robust, spec.require_robust)),
        min_robust_path_count=Int(_raw_get(raw, :min_robust_path_count, spec.min_robust_path_count)),
        max_base_species=begin
            value = _raw_haskey(raw, :max_base_species) ? _raw_get(raw, :max_base_species, 0) : _query_value(goal, :max_base_species; section=goal_size, aliases=[:d, :base_species])
            value === nothing ? nothing : Int(value)
        end,
        max_reactions=begin
            value = _raw_haskey(raw, :max_reactions) ? _raw_get(raw, :max_reactions, 0) : _query_value(goal, :max_reactions; section=goal_size, aliases=[:r, :reactions])
            value === nothing ? nothing : Int(value)
        end,
        max_support=begin
            value = _raw_haskey(raw, :max_support) ? _raw_get(raw, :max_support, 0) : _query_value(goal, :max_support; section=goal_size, aliases=[:support, :s])
            value === nothing ? nothing : Int(value)
        end,
        max_support_mass=begin
            value = _raw_haskey(raw, :max_support_mass) ? _raw_get(raw, :max_support_mass, 0) : _query_value(goal, :max_support_mass; section=goal_size, aliases=[:support_mass, :mass])
            value === nothing ? nothing : Int(value)
        end,
        required_regimes=_goal_merge_predicate_vectors(
            _materialize_dict_vector(collect(something(_query_value(raw, :required_regimes; section=graph_spec, aliases=[:required_nodes]), Dict{String, Any}[]))),
            goal_required_regimes,
        ),
        forbidden_regimes=_goal_merge_predicate_vectors(
            _materialize_dict_vector(collect(something(_query_value(raw, :forbidden_regimes; section=graph_spec, aliases=[:forbidden_nodes]), Dict{String, Any}[]))),
            goal_forbidden_regimes,
        ),
        required_transitions=_goal_merge_predicate_vectors(
            _materialize_dict_vector(collect(something(_query_value(raw, :required_transitions; section=graph_spec, aliases=[:required_edges]), Dict{String, Any}[]))),
            goal_required_transitions,
        ),
        forbidden_transitions=_goal_merge_predicate_vectors(
            _materialize_dict_vector(collect(something(_query_value(raw, :forbidden_transitions; section=graph_spec, aliases=[:forbidden_edges]), Dict{String, Any}[]))),
            goal_forbidden_transitions,
        ),
        required_path_sequences=_goal_merge_sequence_vectors(
            _materialize_predicate_sequences(required_path_sequences_raw),
            goal_required_path_sequences,
        ),
        forbid_singular_on_witness=Bool(something(_query_value(raw, :forbid_singular_on_witness; section=path_spec), _query_value(goal, :forbid_singular; aliases=[:forbid_singular_on_witness]), spec.forbid_singular_on_witness)),
        require_witness_feasible=Bool(something(_query_value(raw, :require_witness_feasible; section=polytope_spec, aliases=[:require_feasible]), goal_feasible, spec.require_witness_feasible)),
        require_witness_robust=Bool(something(_query_value(raw, :require_witness_robust; section=polytope_spec, aliases=[:require_robust]), goal_robust, spec.require_witness_robust)),
        min_witness_volume_mean=begin
            value = _query_value(raw, :min_witness_volume_mean; section=polytope_spec, aliases=[:min_volume_mean])
            value === nothing && (value = goal_min_volume)
            value === nothing ? nothing : Float64(value)
        end,
        max_witness_path_length=begin
            value = _query_value(raw, :max_witness_path_length; section=path_spec, aliases=[:max_path_length])
            value === nothing && (value = goal_max_path_length)
            value === nothing ? nothing : Int(value)
        end,
        ranking_mode=begin
            value = _query_value(raw, :ranking_mode)
            value === nothing && (value = goal_ranking_mode)
            value === nothing ? spec.ranking_mode : Symbol(value)
        end,
        collapse_by_network=Bool(something(_query_value(raw, :collapse_by_network), goal_collapse, spec.collapse_by_network)),
        pareto_only=Bool(something(_query_value(raw, :pareto_only), goal_pareto, spec.pareto_only)),
        limit=Int(something(_query_value(raw, :limit), goal_limit, spec.limit)),
    )
end

function inverse_design_spec_from_raw(raw=nothing)
    spec = inverse_design_spec_default()
    raw === nothing && return spec
    return InverseDesignSpec(
        source_label=String(_raw_get(raw, :source_label, spec.source_label)),
        skip_existing=Bool(_raw_get(raw, :skip_existing, spec.skip_existing)),
        build_library_if_missing=Bool(_raw_get(raw, :build_library_if_missing, spec.build_library_if_missing)),
        return_library=Bool(_raw_get(raw, :return_library, spec.return_library)),
        return_delta_atlas=Bool(_raw_get(raw, :return_delta_atlas, spec.return_delta_atlas)),
    )
end

function inverse_refinement_spec_from_raw(raw=nothing)
    spec = inverse_refinement_spec_default()
    raw === nothing && return spec
    return InverseRefinementSpec(
        enabled=Bool(_raw_get(raw, :enabled, spec.enabled)),
        top_k=Int(_raw_get(raw, :top_k, spec.top_k)),
        trials=Int(_raw_get(raw, :trials, spec.trials)),
        param_min=Float64(_raw_get(raw, :param_min, spec.param_min)),
        param_max=Float64(_raw_get(raw, :param_max, spec.param_max)),
        n_points=Int(_raw_get(raw, :n_points, spec.n_points)),
        background_min=Float64(_raw_get(raw, :background_min, spec.background_min)),
        background_max=Float64(_raw_get(raw, :background_max, spec.background_max)),
        flat_abs_tol=Float64(_raw_get(raw, :flat_abs_tol, spec.flat_abs_tol)),
        flat_rel_tol=Float64(_raw_get(raw, :flat_rel_tol, spec.flat_rel_tol)),
        include_traces=Bool(_raw_get(raw, :include_traces, spec.include_traces)),
        rerank_by_refinement=Bool(_raw_get(raw, :rerank_by_refinement, spec.rerank_by_refinement)),
        rng_seed=Int(_raw_get(raw, :rng_seed, spec.rng_seed)),
    )
end

function atlas_search_profile_to_dict(profile::AtlasSearchProfile)
    return Dict(
        "name" => profile.name,
        "max_base_species" => profile.max_base_species,
        "max_reactions" => profile.max_reactions,
        "max_support" => profile.max_support,
        "allow_reversible_binding" => profile.allow_reversible_binding,
        "allow_catalysis" => profile.allow_catalysis,
        "allow_irreversible_steps" => profile.allow_irreversible_steps,
        "allow_conformational_switches" => profile.allow_conformational_switches,
        "allow_higher_order_templates" => profile.allow_higher_order_templates,
        "slice_mode" => String(profile.slice_mode),
        "input_mode" => String(profile.input_mode),
    )
end

function atlas_behavior_config_to_dict(config::AtlasBehaviorConfig)
    return Dict(
        "path_scope" => String(config.path_scope),
        "min_volume_mean" => config.min_volume_mean,
        "deduplicate" => config.deduplicate,
        "keep_singular" => config.keep_singular,
        "keep_nonasymptotic" => config.keep_nonasymptotic,
        "compute_volume" => config.compute_volume,
        "motif_zero_tol" => config.motif_zero_tol,
        "include_path_records" => config.include_path_records,
    )
end

function atlas_enumeration_spec_to_dict(spec::AtlasEnumerationSpec)
    return Dict(
        "mode" => String(spec.mode),
        "base_species_counts" => collect(spec.base_species_counts),
        "min_reactions" => spec.min_reactions,
        "max_reactions" => spec.max_reactions,
        "limit" => spec.limit,
    )
end

function atlas_query_spec_to_dict(spec::AtlasQuerySpec)
    return Dict(
        "motif_labels" => collect(spec.motif_labels),
        "exact_labels" => collect(spec.exact_labels),
        "motif_match_mode" => String(spec.motif_match_mode),
        "exact_match_mode" => String(spec.exact_match_mode),
        "input_symbols" => collect(spec.input_symbols),
        "output_symbols" => collect(spec.output_symbols),
        "require_robust" => spec.require_robust,
        "min_robust_path_count" => spec.min_robust_path_count,
        "max_base_species" => spec.max_base_species,
        "max_reactions" => spec.max_reactions,
        "max_support" => spec.max_support,
        "max_support_mass" => spec.max_support_mass,
        "required_regimes" => Any[_materialize(item) for item in spec.required_regimes],
        "forbidden_regimes" => Any[_materialize(item) for item in spec.forbidden_regimes],
        "required_transitions" => Any[_materialize(item) for item in spec.required_transitions],
        "forbidden_transitions" => Any[_materialize(item) for item in spec.forbidden_transitions],
        "required_path_sequences" => Any[Any[_materialize(item) for item in seq] for seq in spec.required_path_sequences],
        "forbid_singular_on_witness" => spec.forbid_singular_on_witness,
        "require_witness_feasible" => spec.require_witness_feasible,
        "require_witness_robust" => spec.require_witness_robust,
        "min_witness_volume_mean" => spec.min_witness_volume_mean,
        "max_witness_path_length" => spec.max_witness_path_length,
        "graph_spec" => Dict(
            "required_regimes" => Any[_materialize(item) for item in spec.required_regimes],
            "forbidden_regimes" => Any[_materialize(item) for item in spec.forbidden_regimes],
            "required_transitions" => Any[_materialize(item) for item in spec.required_transitions],
            "forbidden_transitions" => Any[_materialize(item) for item in spec.forbidden_transitions],
        ),
        "path_spec" => Dict(
            "required_path_sequences" => Any[Any[_materialize(item) for item in seq] for seq in spec.required_path_sequences],
            "forbid_singular_on_witness" => spec.forbid_singular_on_witness,
            "max_path_length" => spec.max_witness_path_length,
        ),
        "polytope_spec" => Dict(
            "require_feasible" => spec.require_witness_feasible,
            "require_robust" => spec.require_witness_robust,
            "min_volume_mean" => spec.min_witness_volume_mean,
        ),
        "ranking_mode" => String(spec.ranking_mode),
        "collapse_by_network" => spec.collapse_by_network,
        "pareto_only" => spec.pareto_only,
        "limit" => spec.limit,
    )
end

function inverse_design_spec_to_dict(spec::InverseDesignSpec)
    return Dict(
        "source_label" => spec.source_label,
        "skip_existing" => spec.skip_existing,
        "build_library_if_missing" => spec.build_library_if_missing,
        "return_library" => spec.return_library,
        "return_delta_atlas" => spec.return_delta_atlas,
    )
end

function inverse_refinement_spec_to_dict(spec::InverseRefinementSpec)
    return Dict(
        "enabled" => spec.enabled,
        "top_k" => spec.top_k,
        "trials" => spec.trials,
        "param_min" => spec.param_min,
        "param_max" => spec.param_max,
        "n_points" => spec.n_points,
        "background_min" => spec.background_min,
        "background_max" => spec.background_max,
        "flat_abs_tol" => spec.flat_abs_tol,
        "flat_rel_tol" => spec.flat_rel_tol,
        "include_traces" => spec.include_traces,
        "rerank_by_refinement" => spec.rerank_by_refinement,
        "rng_seed" => spec.rng_seed,
    )
end

function _all_permutations(items::Vector{T}) where {T}
    length(items) <= 1 && return [copy(items)]
    perms = Vector{Vector{T}}()
    for idx in eachindex(items)
        head = items[idx]
        tail = T[items[j] for j in eachindex(items) if j != idx]
        for perm in _all_permutations(tail)
            push!(perms, vcat(T[head], perm))
        end
    end
    return perms
end

function _combinations(items::Vector{T}, k::Int) where {T}
    if k == 0
        return [T[]]
    elseif k < 0 || k > length(items)
        return Vector{Vector{T}}()
    elseif k == length(items)
        return [copy(items)]
    elseif isempty(items)
        return Vector{Vector{T}}()
    end

    head = first(items)
    tail = items[2:end]
    combos = Vector{Vector{T}}()

    for remainder in _combinations(tail, k - 1)
        push!(combos, vcat(T[head], remainder))
    end
    append!(combos, _combinations(tail, k))

    return combos
end

function _base_species_label(idx::Int)::String
    if 1 <= idx <= 26
        return string(Char('A' + idx - 1))
    end
    return "S$(idx)"
end

_base_species_symbols(count::Int) = [Symbol(_base_species_label(idx)) for idx in 1:count]

function _pairwise_complex_symbol(sym_a::Symbol, sym_b::Symbol)
    labels = sort([String(sym_a), String(sym_b)])
    return Symbol("C_" * join(labels, "_"))
end

function _pairwise_binding_templates(base_syms::Vector{Symbol})
    templates = Vector{NamedTuple}(undef, 0)
    for i in 1:length(base_syms)-1
        for j in i+1:length(base_syms)
            left = sort([String(base_syms[i]), String(base_syms[j])])
            complex_sym = _pairwise_complex_symbol(base_syms[i], base_syms[j])
            rule = left[1] * " + " * left[2] * " <-> " * String(complex_sym)
            push!(templates, (
                rule=rule,
                complex_symbol=String(complex_sym),
                reactants=copy(left),
            ))
        end
    end
    return templates
end

function _uses_all_base_species(combo, base_syms::Vector{Symbol})
    used = Set{String}()
    for template in combo
        union!(used, template.reactants)
    end
    return length(used) == length(base_syms)
end

function enumerate_network_specs(
    spec::AtlasEnumerationSpec;
    search_profile::AtlasSearchProfile=atlas_search_profile_binding_small_v0(),
)
    search_profile.slice_mode == :siso || error("Atlas enumerator currently supports only slice_mode=:siso.")
    spec.mode == :pairwise_binding || error("Unsupported atlas enumeration mode: $(spec.mode)")
    search_profile.allow_reversible_binding || error("The pairwise-binding enumerator requires allow_reversible_binding=true.")

    network_specs = Dict{Symbol, Any}[]
    generated_counts = Dict{String, Int}()
    emitted = 0

    for base_count in sort!(unique(copy(spec.base_species_counts)))
        base_count <= search_profile.max_base_species || continue
        base_syms = _base_species_symbols(base_count)
        templates = _pairwise_binding_templates(base_syms)
        max_reactions = min(spec.max_reactions, search_profile.max_reactions, length(templates))
        min_reactions = min(spec.min_reactions, max_reactions)

        for reaction_count in min_reactions:max_reactions
            for combo in _combinations(templates, reaction_count)
                _uses_all_base_species(combo, base_syms) || continue
                emitted += 1
                rules = [template.rule for template in combo]
                label = "enum_" * String(spec.mode) * "_d$(base_count)_r$(reaction_count)_$(emitted)"
                push!(network_specs, Dict{Symbol, Any}(
                    :label => label,
                    :reactions => rules,
                    :source_kind => "enumerated",
                    :source_metadata => Dict(
                        "enumeration_mode" => String(spec.mode),
                        "base_species_count" => base_count,
                        "reaction_count" => reaction_count,
                        "base_species_symbols" => string.(base_syms),
                        "template_complexes" => [template.complex_symbol for template in combo],
                    ),
                ))
                key = string(base_count)
                generated_counts[key] = get(generated_counts, key, 0) + 1

                if spec.limit > 0 && length(network_specs) >= spec.limit
                    return network_specs, Dict(
                        "enumeration_spec" => atlas_enumeration_spec_to_dict(spec),
                        "generated_network_count" => length(network_specs),
                        "generated_by_base_species_count" => generated_counts,
                        "truncated" => true,
                    )
                end
            end
        end
    end

    return network_specs, Dict(
        "enumeration_spec" => atlas_enumeration_spec_to_dict(spec),
        "generated_network_count" => length(network_specs),
        "generated_by_base_species_count" => generated_counts,
        "truncated" => false,
    )
end

function _infer_binding_supports(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    _, _, free_syms, prod_syms = parse_network_structure(rules)
    supports = Dict{Symbol, Set{Symbol}}(sym => Set([sym]) for sym in free_syms)

    progress = true
    while progress
        progress = false
        for idx in eachindex(rules)
            reactant_dict = reactants[idx]
            product_dict = products[idx]
            if length(product_dict) != 1 || any(coeff != 1 for coeff in values(product_dict))
                continue
            end
            if any(coeff != 1 for coeff in values(reactant_dict))
                continue
            end
            all(haskey(supports, sym) for sym in keys(reactant_dict)) || continue

            product_sym = first(keys(product_dict))
            merged = Set{Symbol}()
            for sym in keys(reactant_dict)
                union!(merged, supports[sym])
            end

            if !haskey(supports, product_sym)
                supports[product_sym] = merged
                progress = true
            elseif supports[product_sym] != merged
                return nothing, "inconsistent_support_assignment:$(product_sym)"
            end
        end
    end

    missing = Symbol[sym for sym in prod_syms if !haskey(supports, sym)]
    if !isempty(missing)
        return nothing, "support_inference_failed:" * join(sort(string.(missing)), ",")
    end

    return supports, nothing
end

function _support_metrics(supports::Dict{Symbol, Set{Symbol}}, free_syms::Vector{Symbol}, prod_syms::Vector{Symbol})
    support_sizes = [length(supports[sym]) for sym in prod_syms if haskey(supports, sym)]
    max_support = isempty(support_sizes) ? 1 : maximum(support_sizes)
    support_mass = isempty(support_sizes) ? 0 : sum(size - 1 for size in support_sizes)
    return Dict(
        "base_species_count" => length(free_syms),
        "total_species_count" => length(free_syms) + length(prod_syms),
        "product_species_count" => length(prod_syms),
        "max_support" => max_support,
        "support_mass" => support_mass,
        "support_map" => Dict(string(sym) => sort!(collect(string.(supports[sym]))) for sym in sort!(collect(keys(supports)))),
    )
end

function validate_rules_against_profile(rules::Vector{String}, profile::AtlasSearchProfile)
    issues = String[]
    reactants, products = try
        parse_reactions(rules)
    catch err
        return Dict(
            "valid" => false,
            "issues" => ["parse_error:" * sprint(showerror, err)],
            "metrics" => nothing,
            "free_symbols" => String[],
            "product_symbols" => String[],
            "supports" => nothing,
        )
    end

    _, _, free_syms, prod_syms = parse_network_structure(rules)

    length(rules) <= profile.max_reactions || push!(issues, "too_many_reactions")
    length(free_syms) <= profile.max_base_species || push!(issues, "too_many_base_species")

    if !profile.allow_higher_order_templates
        for idx in eachindex(rules)
            length(reactants[idx]) == 2 || push!(issues, "reaction_$(idx):requires_exactly_two_reactant_terms")
            length(products[idx]) == 1 || push!(issues, "reaction_$(idx):requires_exactly_one_product_term")
            all(coeff == 1 for coeff in values(reactants[idx])) || push!(issues, "reaction_$(idx):requires_unit_reactant_stoichiometry")
            all(coeff == 1 for coeff in values(products[idx])) || push!(issues, "reaction_$(idx):requires_unit_product_stoichiometry")
        end
    end

    supports, support_issue = _infer_binding_supports(rules)
    support_issue === nothing || push!(issues, support_issue)
    metrics = if isnothing(supports)
        nothing
    else
        _support_metrics(supports, free_syms, prod_syms)
    end

    if metrics !== nothing && metrics["max_support"] > profile.max_support
        push!(issues, "max_support_exceeds_profile")
    end

    return Dict(
        "valid" => isempty(issues),
        "issues" => unique(issues),
        "metrics" => metrics,
        "free_symbols" => string.(free_syms),
        "product_symbols" => string.(prod_syms),
        "supports" => supports,
    )
end

function _canonical_term_string(sym::Symbol, supports::Dict{Symbol, Set{Symbol}}, remap::Dict{Symbol, Int})
    term = sort(collect(remap[base] for base in supports[sym]))
    return "[" * join(term, ",") * "]"
end

function canonical_network_code(rules::Vector{String})
    reactants, products = parse_reactions(rules)
    _, _, free_syms, _ = parse_network_structure(rules)
    supports, support_issue = _infer_binding_supports(rules)
    support_issue === nothing || error("Cannot canonicalize network: $support_issue")

    candidates = String[]
    for perm in _all_permutations(copy(free_syms))
        remap = Dict(sym => idx for (idx, sym) in enumerate(perm))
        serialized_rules = String[]
        for idx in eachindex(rules)
            left_terms = String[]
            for (sym, coeff) in reactants[idx]
                term = _canonical_term_string(sym, supports, remap)
                for _ in 1:coeff
                    push!(left_terms, term)
                end
            end
            right_terms = String[]
            for (sym, coeff) in products[idx]
                term = _canonical_term_string(sym, supports, remap)
                for _ in 1:coeff
                    push!(right_terms, term)
                end
            end
            sort!(left_terms)
            sort!(right_terms)
            push!(serialized_rules, join(left_terms, "+") * "<->" * join(right_terms, "+"))
        end
        sort!(serialized_rules)
        push!(candidates, join(serialized_rules, "|"))
    end

    sort!(candidates)
    return first(candidates)
end

function _config_signature(config::AtlasBehaviorConfig)
    return join([
        "scope=" * String(config.path_scope),
        "min_volume_mean=" * string(config.min_volume_mean),
        "deduplicate=" * string(config.deduplicate),
        "keep_singular=" * string(config.keep_singular),
        "keep_nonasymptotic=" * string(config.keep_nonasymptotic),
        "compute_volume=" * string(config.compute_volume),
        "motif_zero_tol=" * string(config.motif_zero_tol),
    ], ";")
end

function _resolve_input_symbols(raw_network, model, profile::AtlasSearchProfile)
    if _raw_haskey(raw_network, :input_symbols)
        requested = Symbol.(String.(_raw_get(raw_network, :input_symbols, String[])))
        valid = Set(Symbol.(qK_sym(model)))
        invalid = filter(sym -> sym ∉ valid, requested)
        isempty(invalid) || error("Unknown input symbols: $(join(string.(invalid), ", "))")
        return requested
    end

    if profile.input_mode == :all_qk
        return Symbol.(qK_sym(model))
    elseif profile.input_mode == :totals_only
        return Symbol.(q_sym(model))
    else
        error("Unsupported atlas input_mode: $(profile.input_mode)")
    end
end

function _resolve_output_symbols(raw_network, model)
    if _raw_haskey(raw_network, :output_symbols)
        requested = Symbol.(String.(_raw_get(raw_network, :output_symbols, String[])))
        valid = Set(Symbol.(x_sym(model)))
        invalid = filter(sym -> sym ∉ valid, requested)
        isempty(invalid) || error("Unknown output symbols: $(join(string.(invalid), ", "))")
        return requested
    end
    return Symbol.(x_sym(model))
end

function _family_volume_mean(path_ids::AbstractVector{<:Integer}, path_records::AbstractVector)
    means = Float64[]
    for path_idx in path_ids
        vol = path_records[path_idx].volume
        isnothing(vol) || push!(means, vol.mean)
    end
    return isempty(means) ? nothing : sum(means) / length(means)
end

function _robust_path_count(path_ids::AbstractVector{<:Integer}, path_records::AbstractVector, min_volume_mean::Float64)
    count = 0
    for path_idx in path_ids
        rec = path_records[path_idx]
        if rec.feasible && !isnothing(rec.volume) && rec.volume.mean >= min_volume_mean
            count += 1
        end
    end
    return count
end

function _graph_slice_id(network_id::AbstractString, change_qK::AbstractString)
    return join([
        String(network_id),
        "graph_input=" * String(change_qK),
        "graphcfg=siso_v0",
    ], "::")
end

function _output_order_token(value::Real)
    val = Float64(value)
    if isnan(val)
        return "NaN"
    elseif isinf(val)
        return signbit(val) ? "-Inf" : "+Inf"
    end

    rounded = round(val; digits=3)
    abs(rounded) < 1e-6 && return "0"

    rounded_int = round(Int, rounded)
    if isapprox(rounded, rounded_int; atol=1e-6)
        return rounded_int > 0 ? "+" * string(rounded_int) : string(rounded_int)
    end

    str = string(rounded)
    return rounded > 0 ? "+" * str : str
end

function _output_order_for_vertex(model, vertex_idx::Integer, change_qK_idx, observe_x_idx::Integer)
    nullity = get_nullity(model, vertex_idx)
    nullity > 1 && error("atlas_nullity_gt_1: atlas output-order materialization does not support vertex $(vertex_idx) with nullity $(nullity)")
    if !is_singular(model, vertex_idx)
        return get_H(model, vertex_idx)[observe_x_idx, change_qK_idx] |> x -> round(Float64(x); digits=3)
    end

    ord = Float64(get_H(model, vertex_idx)[observe_x_idx, change_qK_idx])
    return abs(ord) < 1e-6 ? NaN : ord * Inf
end

function _graph_reachability_masks(g, sources::AbstractVector{<:Integer}, sinks::AbstractVector{<:Integer})
    from_sources = falses(nv(g))
    stack = collect(sources)
    while !isempty(stack)
        v = pop!(stack)
        from_sources[v] && continue
        from_sources[v] = true
        append!(stack, outneighbors(g, v))
    end

    to_sinks = falses(nv(g))
    stack = collect(sinks)
    while !isempty(stack)
        v = pop!(stack)
        to_sinks[v] && continue
        to_sinks[v] = true
        append!(stack, inneighbors(g, v))
    end

    return from_sources, to_sinks
end

function _regime_role(siso, vertex_idx::Integer)
    is_source = vertex_idx in siso.sources
    is_sink = vertex_idx in siso.sinks
    is_branch = outdegree(siso.qK_grh, vertex_idx) > 1
    is_merge = indegree(siso.qK_grh, vertex_idx) > 1

    if is_source && is_sink
        return "source_sink"
    elseif is_source
        return "source"
    elseif is_sink
        return "sink"
    elseif is_branch && is_merge
        return "branch_merge"
    elseif is_branch
        return "branch"
    elseif is_merge
        return "merge"
    else
        return "interior"
    end
end

function _build_input_graph_slice(siso, network_id::String, input_symbol::String)
    return Dict(
        "graph_slice_id" => _graph_slice_id(network_id, input_symbol),
        "network_id" => network_id,
        "input_symbol" => input_symbol,
        "graph_config" => Dict(
            "slice_mode" => "siso",
            "graph_schema_version" => "siso_v0",
        ),
        "vertex_count" => nv(siso.qK_grh),
        "edge_count" => ne(siso.qK_grh),
        "path_count" => length(siso.rgm_paths),
        "source_vertex_indices" => sort!(collect(siso.sources)),
        "sink_vertex_indices" => sort!(collect(siso.sinks)),
    )
end

function _build_slice_regime_transition_records(model, siso, network_id::String, slice_id::String, graph_slice_id::String, input_symbol::String, output_symbol::String)
    observe_x_idx = locate_sym_x(model, Symbol(output_symbol))
    reachable_from_sources, can_reach_sinks = _graph_reachability_masks(siso.qK_grh, siso.sources, siso.sinks)

    regime_records = Dict{String, Any}[]
    regime_by_vertex = Dict{Int, Dict{String, Any}}()

    for vertex_idx in sort!(collect(vertices(siso.qK_grh)))
        nullity = get_nullity(model, vertex_idx)
        asymptotic = is_asymptotic(model, vertex_idx)
        order_value = _output_order_for_vertex(model, vertex_idx, siso.change_qK_idx, observe_x_idx)
        role = _regime_role(siso, vertex_idx)
        indeg = indegree(siso.qK_grh, vertex_idx)
        outdeg = outdegree(siso.qK_grh, vertex_idx)

        record = Dict(
            "regime_record_id" => slice_id * "::regime::" * string(vertex_idx),
            "slice_id" => slice_id,
            "graph_slice_id" => graph_slice_id,
            "network_id" => network_id,
            "input_symbol" => input_symbol,
            "output_symbol" => output_symbol,
            "vertex_idx" => vertex_idx,
            "role" => role,
            "is_source" => vertex_idx in siso.sources,
            "is_sink" => vertex_idx in siso.sinks,
            "is_branch" => outdeg > 1,
            "is_merge" => indeg > 1,
            "indegree" => indeg,
            "outdegree" => outdeg,
            "reachable_from_source" => reachable_from_sources[vertex_idx],
            "can_reach_sink" => can_reach_sinks[vertex_idx],
            "singular" => nullity > 0,
            "nullity" => nullity,
            "asymptotic" => asymptotic,
            "output_order_value" => order_value,
            "output_order_token" => _output_order_token(order_value),
        )
        push!(regime_records, record)
        regime_by_vertex[vertex_idx] = record
    end

    transition_records = Dict{String, Any}[]
    transition_by_edge = Dict{Tuple{Int, Int}, Dict{String, Any}}()

    for edge in edges(siso.qK_grh)
        from_vertex = src(edge)
        to_vertex = dst(edge)
        from_record = regime_by_vertex[from_vertex]
        to_record = regime_by_vertex[to_vertex]
        record = Dict(
            "transition_record_id" => slice_id * "::transition::" * string(from_vertex) * "->" * string(to_vertex),
            "slice_id" => slice_id,
            "graph_slice_id" => graph_slice_id,
            "input_symbol" => input_symbol,
            "output_symbol" => output_symbol,
            "from_vertex_idx" => from_vertex,
            "to_vertex_idx" => to_vertex,
            "from_role" => from_record["role"],
            "to_role" => to_record["role"],
            "from_singular" => from_record["singular"],
            "to_singular" => to_record["singular"],
            "from_nullity" => from_record["nullity"],
            "to_nullity" => to_record["nullity"],
            "from_output_order_token" => from_record["output_order_token"],
            "to_output_order_token" => to_record["output_order_token"],
            "transition_token" => string(from_record["output_order_token"], "->", to_record["output_order_token"]),
        )
        push!(transition_records, record)
        transition_by_edge[(from_vertex, to_vertex)] = record
    end

    return Dict(
        "regime_records" => regime_records,
        "transition_records" => transition_records,
        "regime_by_vertex" => regime_by_vertex,
        "transition_by_edge" => transition_by_edge,
    )
end

function _path_family_maps(result)
    exact_by_path = Dict{Int, Int}()
    motif_by_path = Dict{Int, Int}()

    for family in result.exact_families
        for path_idx in family.path_indices
            exact_by_path[path_idx] = family.family_idx
        end
    end

    for family in result.motif_families
        for path_idx in family.path_indices
            motif_by_path[path_idx] = family.family_idx
        end
    end

    return exact_by_path, motif_by_path
end

function _build_family_buckets!(family_buckets, result, slice_id::String, config::AtlasBehaviorConfig)
    for family in result.exact_families
        push!(family_buckets, Dict(
            "bucket_id" => slice_id * "::exact::" * string(family.family_idx),
            "slice_id" => slice_id,
            "family_kind" => "exact",
            "family_idx" => family.family_idx,
            "family_label" => family.exact_label,
            "parent_motif" => family.motif_label,
            "path_indices" => collect(family.path_indices),
            "path_count" => family.n_paths,
            "robust_path_count" => _robust_path_count(family.path_indices, result.path_records, config.min_volume_mean),
            "volume_mean" => _family_volume_mean(family.path_indices, result.path_records),
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_path_signature" => family.representative_path_idx == 0 ? nothing : result.path_records[family.representative_path_idx].exact_label,
        ))
    end

    for family in result.motif_families
        push!(family_buckets, Dict(
            "bucket_id" => slice_id * "::motif::" * string(family.family_idx),
            "slice_id" => slice_id,
            "family_kind" => "motif",
            "family_idx" => family.family_idx,
            "family_label" => family.motif_label,
            "parent_motif" => nothing,
            "path_indices" => collect(family.path_indices),
            "path_count" => family.n_paths,
            "robust_path_count" => _robust_path_count(family.path_indices, result.path_records, config.min_volume_mean),
            "volume_mean" => _family_volume_mean(family.path_indices, result.path_records),
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_path_signature" => family.representative_path_idx == 0 ? nothing : result.path_records[family.representative_path_idx].motif_label,
            "exact_family_indices" => collect(family.exact_family_indices),
        ))
    end
end

function _build_path_records!(path_records, result, slice_id::String, graph_slice_id::String, config::AtlasBehaviorConfig, regime_by_vertex::Dict{Int, Dict{String, Any}}, transition_by_edge::Dict{Tuple{Int, Int}, Dict{String, Any}})
    config.include_path_records || return
    exact_by_path, motif_by_path = _path_family_maps(result)

    for rec in result.path_records
        regime_sequence = Any[
            Dict(
                "vertex_idx" => vertex_idx,
                "role" => regime_by_vertex[vertex_idx]["role"],
                "singular" => regime_by_vertex[vertex_idx]["singular"],
                "nullity" => regime_by_vertex[vertex_idx]["nullity"],
                "asymptotic" => regime_by_vertex[vertex_idx]["asymptotic"],
                "output_order_token" => regime_by_vertex[vertex_idx]["output_order_token"],
            )
            for vertex_idx in rec.vertex_indices
        ]
        transition_sequence = Any[
            Dict(
                "from_vertex_idx" => rec.vertex_indices[idx],
                "to_vertex_idx" => rec.vertex_indices[idx + 1],
                "transition_token" => transition_by_edge[(rec.vertex_indices[idx], rec.vertex_indices[idx + 1])]["transition_token"],
                "from_output_order_token" => transition_by_edge[(rec.vertex_indices[idx], rec.vertex_indices[idx + 1])]["from_output_order_token"],
                "to_output_order_token" => transition_by_edge[(rec.vertex_indices[idx], rec.vertex_indices[idx + 1])]["to_output_order_token"],
            )
            for idx in 1:(length(rec.vertex_indices) - 1)
        ]
        push!(path_records, Dict(
            "path_record_id" => slice_id * "::path::" * string(rec.path_idx),
            "slice_id" => slice_id,
            "graph_slice_id" => graph_slice_id,
            "path_idx" => rec.path_idx,
            "vertex_indices" => collect(rec.vertex_indices),
            "output_order_tokens" => [String(item["output_order_token"]) for item in regime_sequence],
            "transition_tokens" => [String(item["transition_token"]) for item in transition_sequence],
            "regime_sequence" => regime_sequence,
            "transition_sequence" => transition_sequence,
            "exact_profile" => json_safe_profile(rec.exact_profile),
            "exact_label" => rec.exact_label,
            "motif_profile" => collect(rec.motif_profile),
            "motif_label" => rec.motif_label,
            "exact_family_idx" => get(exact_by_path, rec.path_idx, nothing),
            "motif_family_idx" => get(motif_by_path, rec.path_idx, nothing),
            "feasible" => rec.feasible,
            "included" => rec.included,
            "robust" => rec.feasible && !isnothing(rec.volume) && rec.volume.mean >= config.min_volume_mean,
            "exclusion_reason" => rec.exclusion_reason,
            "volume" => volume_to_dict(rec.volume),
        ))
    end
end

function _atlas_slice_id(network_id::AbstractString, change_qK::AbstractString, observe_x::AbstractString, config::AtlasBehaviorConfig)
    return join([
        String(network_id),
        "input=" * String(change_qK),
        "output=" * String(observe_x),
        "cfg=" * _config_signature(config),
    ], "::")
end

function _library_existing_ok_slice_ids(library)
    ids = Set{String}()
    library === nothing && return ids

    count_maps = _atlas_slice_count_maps(
        collect(_raw_get(library, :regime_records, Any[])),
        collect(_raw_get(library, :transition_records, Any[])),
        collect(_raw_get(library, :family_buckets, Any[])),
        collect(_raw_get(library, :path_records, Any[])),
    )
    for slice in collect(_raw_get(library, :behavior_slices, Any[]))
        slice_id = String(_raw_get(slice, :slice_id, ""))
        isempty(slice_id) && continue
        _atlas_slice_is_complete(slice, _atlas_slice_counts(slice_id, count_maps)) || continue
        push!(ids, slice_id)
    end

    return ids
end

function _record_skipped_existing!(records, raw_network, canonical_code::String, slice_ids; reason::String)
    push!(records, Dict(
        "source_label" => String(_raw_get(raw_network, :label, canonical_code)),
        "network_id" => canonical_code,
        "source_kind" => String(_raw_get(raw_network, :source_kind, "explicit")),
        "reason" => reason,
        "skipped_slice_ids" => _sorted_unique_strings(slice_ids),
    ))
end

function _build_behavior_slice(model, network_id::String, graph_slice_id::String, change_qK::Symbol, observe_x::Symbol, siso, config::AtlasBehaviorConfig)
    slice_id = _atlas_slice_id(network_id, string(change_qK), string(observe_x), config)

    try
        result = get_behavior_families(
            siso;
            observe_x=observe_x,
            path_scope=config.path_scope,
            min_volume_mean=config.min_volume_mean,
            deduplicate=config.deduplicate,
            keep_singular=config.keep_singular,
            keep_nonasymptotic=config.keep_nonasymptotic,
            motif_zero_tol=config.motif_zero_tol,
            compute_volume=config.compute_volume,
        )

        return Dict(
            "slice" => Dict(
                "slice_id" => slice_id,
                "network_id" => network_id,
                "graph_slice_id" => graph_slice_id,
                "analysis_status" => "ok",
                "input_symbol" => string(change_qK),
                "output_symbol" => string(observe_x),
                "classifier_config" => atlas_behavior_config_to_dict(config),
                "path_scope" => String(result.path_scope),
                "min_volume_mean" => result.min_volume_mean,
                "deduplicate" => result.deduplicate,
                "keep_singular" => result.keep_singular,
                "keep_nonasymptotic" => result.keep_nonasymptotic,
                "compute_volume" => result.compute_volume,
                "total_paths" => result.total_paths,
                "feasible_paths" => result.feasible_paths,
                "included_paths" => result.included_paths,
                "excluded_paths" => result.excluded_paths,
                "exclusion_counts" => Dict(string(k) => v for (k, v) in result.exclusion_counts),
                "motif_union" => sort!(unique(getindex.(result.motif_families, :motif_label))),
                "exact_union" => sort!(unique(getindex.(result.exact_families, :exact_label))),
                "motif_family_count" => length(result.motif_families),
                "exact_family_count" => length(result.exact_families),
            ),
            "result" => result,
        )
    catch err
        failure = _atlas_failure_metadata(err, "behavior_families")
        return Dict(
            "slice" => Dict(
                "slice_id" => slice_id,
                "network_id" => network_id,
                "graph_slice_id" => graph_slice_id,
                "analysis_status" => "failed",
                "input_symbol" => string(change_qK),
                "output_symbol" => string(observe_x),
                "classifier_config" => atlas_behavior_config_to_dict(config),
                "error" => failure["error"],
                "build_state" => failure["build_state"],
                "partial_result_available" => failure["partial_result_available"],
                "failure_stage" => failure["failure_stage"],
                "failure_class" => failure["failure_class"],
                "failure_message" => get(failure, "failure_message", nothing),
                "unsupported_feature" => get(failure, "unsupported_feature", nothing),
                "integrity_issues" => get(failure, "integrity_issues", String[]),
                "regime_record_count" => 0,
                "transition_record_count" => 0,
                "family_bucket_count" => 0,
                "path_record_count" => 0,
            ),
            "result" => nothing,
        )
    end
end

function _materialize_behavior_slice_payload(model, network_id::String, graph_slice_id::String, change_qK::Symbol, observe_x::Symbol, siso, config::AtlasBehaviorConfig)
    slice_payload = _build_behavior_slice(model, network_id, graph_slice_id, change_qK, observe_x, siso, config)
    slice = slice_payload["slice"]
    result = slice_payload["result"]

    regime_records = Dict{String, Any}[]
    transition_records = Dict{String, Any}[]
    family_buckets = Dict{String, Any}[]
    path_records = Dict{String, Any}[]

    if result === nothing
        _annotate_behavior_slice!(slice, Dict(
            "regime_record_count" => 0,
            "transition_record_count" => 0,
            "family_bucket_count" => 0,
            "path_record_count" => 0,
        ))
        return Dict(
            "slice" => slice,
            "result" => nothing,
            "regime_records" => regime_records,
            "transition_records" => transition_records,
            "family_buckets" => family_buckets,
            "path_records" => path_records,
        )
    end

    try
        slice_graph_payload = _build_slice_regime_transition_records(
            model,
            siso,
            network_id,
            slice["slice_id"],
            graph_slice_id,
            string(change_qK),
            string(observe_x),
        )
        append!(regime_records, slice_graph_payload["regime_records"])
        append!(transition_records, slice_graph_payload["transition_records"])
        _build_family_buckets!(family_buckets, result, slice["slice_id"], config)
        _build_path_records!(
            path_records,
            result,
            slice["slice_id"],
            graph_slice_id,
            config,
            slice_graph_payload["regime_by_vertex"],
            slice_graph_payload["transition_by_edge"],
        )
        slice["regime_token_union"] = sort!(unique([String(rec["output_order_token"]) for rec in regime_records]))
        slice["transition_token_union"] = sort!(unique([String(rec["transition_token"]) for rec in transition_records]))
        slice["build_state"] = "complete"
        slice["partial_result_available"] = false
        slice["regime_record_count"] = length(regime_records)
        slice["transition_record_count"] = length(transition_records)
        slice["family_bucket_count"] = length(family_buckets)
        slice["path_record_count"] = length(path_records)
        return Dict(
            "slice" => slice,
            "result" => result,
            "regime_records" => regime_records,
            "transition_records" => transition_records,
            "family_buckets" => family_buckets,
            "path_records" => path_records,
        )
    catch err
        merge!(slice, _atlas_failure_metadata(err, "slice_record_materialization"; partial_result_available=true))
        _annotate_behavior_slice!(slice, Dict(
            "regime_record_count" => 0,
            "transition_record_count" => 0,
            "family_bucket_count" => 0,
            "path_record_count" => 0,
        ))
        return Dict(
            "slice" => slice,
            "result" => nothing,
            "regime_records" => Dict{String, Any}[],
            "transition_records" => Dict{String, Any}[],
            "family_buckets" => Dict{String, Any}[],
            "path_records" => Dict{String, Any}[],
        )
    end
end

function _build_network_summary(slice_dicts)
    motif_union = String[]
    exact_union = String[]
    ok_count = 0
    failed_count = 0
    slice_ids = String[]
    failure_classes = String[]
    partial_count = 0

    for slice in slice_dicts
        push!(slice_ids, slice["slice_id"])
        if _atlas_slice_is_complete(slice)
            ok_count += 1
            append!(motif_union, get(slice, "motif_union", String[]))
            append!(exact_union, get(slice, "exact_union", String[]))
        else
            failed_count += 1
            Bool(_raw_get(slice, :partial_result_available, false)) && (partial_count += 1)
            failure_class = String(_raw_get(slice, :failure_class, ""))
            isempty(failure_class) || push!(failure_classes, failure_class)
        end
    end

    return Dict(
        "slice_ids" => slice_ids,
        "successful_slice_count" => ok_count,
        "failed_slice_count" => failed_count,
        "partial_result_slice_count" => partial_count,
        "failure_classes" => _sorted_unique_strings(failure_classes),
        "motif_union" => sort!(unique(motif_union)),
        "exact_union" => sort!(unique(exact_union)),
    )
end

function build_behavior_atlas(network_specs;
    search_profile::AtlasSearchProfile=atlas_search_profile_binding_small_v0(),
    behavior_config::AtlasBehaviorConfig=atlas_behavior_config_default(),
    library=nothing,
    sqlite_path=nothing,
    skip_existing::Bool=false,
)
    network_entries = Dict{String, Any}[]
    input_graph_slices = Dict{String, Any}[]
    behavior_slices = Dict{String, Any}[]
    regime_records = Dict{String, Any}[]
    transition_records = Dict{String, Any}[]
    family_buckets = Dict{String, Any}[]
    path_records = Dict{String, Any}[]
    duplicate_inputs = Dict{String, Any}[]
    skipped_existing_networks = Dict{String, Any}[]

    seen_networks = Set{String}()
    existing_slice_ids = skip_existing ? _library_existing_ok_slice_ids(library) : Set{String}()
    sqlite_pruning = skip_existing && sqlite_path !== nothing
    if sqlite_pruning
        union!(existing_slice_ids, atlas_sqlite_existing_ok_slice_ids(String(sqlite_path)))
    end
    skipped_existing_slice_count = 0

    for (network_idx, raw_network) in enumerate(network_specs)
        label = String(_raw_get(raw_network, :label, "network_$(network_idx)"))
        rules = String.(_raw_get(raw_network, :reactions, String[]))
        kd = _raw_haskey(raw_network, :kd) ? Float64.(_raw_get(raw_network, :kd, Float64[])) : ones(Float64, length(rules))

        validation = validate_rules_against_profile(rules, search_profile)
        canonical_code = try
            canonical_network_code(rules)
        catch
            "uncanonicalized::" * join(sort(strip.(rules)), "|")
        end

        if validation["valid"] && canonical_code in seen_networks
            push!(duplicate_inputs, Dict(
                "source_label" => label,
                "duplicate_of_network_id" => canonical_code,
                "reactions" => rules,
            ))
            continue
        end

        metrics = validation["metrics"]
        network_entry = Dict(
            "network_id" => canonical_code,
            "source_label" => label,
            "source_kind" => String(_raw_get(raw_network, :source_kind, "explicit")),
            "source_metadata" => _raw_get(raw_network, :source_metadata, nothing),
            "canonical_code" => canonical_code,
            "raw_rules" => rules,
            "search_profile" => atlas_search_profile_to_dict(search_profile),
            "analysis_status" => validation["valid"] ? "pending" : "excluded_by_search_profile",
            "build_state" => validation["valid"] ? "pending" : "excluded_by_search_profile",
            "profile_issues" => validation["issues"],
            "base_species_count" => metrics === nothing ? nothing : metrics["base_species_count"],
            "reaction_count" => length(rules),
            "total_species_count" => metrics === nothing ? nothing : metrics["total_species_count"],
            "max_support" => metrics === nothing ? nothing : metrics["max_support"],
            "support_mass" => metrics === nothing ? nothing : metrics["support_mass"],
            "support_map" => metrics === nothing ? nothing : metrics["support_map"],
        )

        if !validation["valid"]
            push!(network_entries, network_entry)
            continue
        end

        if skip_existing && !isempty(existing_slice_ids) &&
           _raw_haskey(raw_network, :input_symbols) && _raw_haskey(raw_network, :output_symbols)
            requested_inputs = String.(_raw_get(raw_network, :input_symbols, String[]))
            requested_outputs = String.(_raw_get(raw_network, :output_symbols, String[]))
            planned_slice_ids = String[
                _atlas_slice_id(canonical_code, input_symbol, output_symbol, behavior_config)
                for input_symbol in requested_inputs for output_symbol in requested_outputs
            ]
            if !isempty(planned_slice_ids) && all(slice_id -> slice_id in existing_slice_ids, planned_slice_ids)
                skipped_existing_slice_count += length(planned_slice_ids)
                _record_skipped_existing!(skipped_existing_networks, raw_network, canonical_code, planned_slice_ids;
                    reason="all_requested_slices_present_in_library",
                )
                continue
            end
        end

        push!(seen_networks, canonical_code)

        slice_dicts = Dict{String, Any}[]
        try
            model, _, _, _ = build_model(rules, kd)
            input_symbols = _resolve_input_symbols(raw_network, model, search_profile)
            output_symbols = _resolve_output_symbols(raw_network, model)
            planned_slice_ids = String[
                _atlas_slice_id(canonical_code, string(input_symbol), string(output_symbol), behavior_config)
                for input_symbol in input_symbols for output_symbol in output_symbols
            ]

            if skip_existing && !isempty(planned_slice_ids) && all(slice_id -> slice_id in existing_slice_ids, planned_slice_ids)
                skipped_existing_slice_count += length(planned_slice_ids)
                _record_skipped_existing!(skipped_existing_networks, raw_network, canonical_code, planned_slice_ids;
                    reason="all_resolved_slices_present_in_library",
                )
                continue
            end

            siso_cache = Dict{Symbol, Any}()
            skipped_slice_ids = String[]

            for input_symbol in input_symbols
                missing_outputs = Symbol[]
                for output_symbol in output_symbols
                    slice_id = _atlas_slice_id(canonical_code, string(input_symbol), string(output_symbol), behavior_config)
                    if skip_existing && slice_id in existing_slice_ids
                        push!(skipped_slice_ids, slice_id)
                        skipped_existing_slice_count += 1
                    else
                        push!(missing_outputs, output_symbol)
                    end
                end

                isempty(missing_outputs) && continue

                siso = get!(siso_cache, input_symbol) do
                    SISOPaths(model, input_symbol)
                end
                graph_slice = _build_input_graph_slice(siso, canonical_code, string(input_symbol))
                push!(input_graph_slices, graph_slice)
                for output_symbol in missing_outputs
                    slice_payload = _materialize_behavior_slice_payload(model, canonical_code, graph_slice["graph_slice_id"], input_symbol, output_symbol, siso, behavior_config)
                    slice = slice_payload["slice"]
                    push!(behavior_slices, slice)
                    push!(slice_dicts, slice)
                    append!(regime_records, slice_payload["regime_records"])
                    append!(transition_records, slice_payload["transition_records"])
                    append!(family_buckets, slice_payload["family_buckets"])
                    append!(path_records, slice_payload["path_records"])
                end
            end

            if !isempty(skipped_slice_ids)
                _record_skipped_existing!(skipped_existing_networks, raw_network, canonical_code, skipped_slice_ids;
                    reason=isempty(slice_dicts) ? "all_resolved_slices_present_in_library" : "partial_slice_reuse_from_library",
                )
            end

            isempty(slice_dicts) && continue

            summary = _build_network_summary(slice_dicts)
            merge!(network_entry, summary)
            network_entry["analysis_status"] = if Int(summary["successful_slice_count"]) > 0
                "ok"
            elseif Int(summary["failed_slice_count"]) > 0
                "failed"
            else
                "pending"
            end
            network_entry["build_state"] = _network_build_state(summary)
        catch err
            network_entry["analysis_status"] = "failed"
            network_entry["build_state"] = "failed"
            merge!(network_entry, _atlas_failure_metadata(err, "network_build"))
        end

        push!(network_entries, network_entry)
    end

    return Dict(
        "atlas_schema_version" => "0.2.0",
        "generated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        "search_profile" => atlas_search_profile_to_dict(search_profile),
        "behavior_config" => atlas_behavior_config_to_dict(behavior_config),
        "input_network_count" => length(network_specs),
        "unique_network_count" => length(network_entries),
        "successful_network_count" => count(entry -> get(entry, "analysis_status", "") == "ok", network_entries),
        "failed_network_count" => count(entry -> get(entry, "analysis_status", "") == "failed", network_entries),
        "excluded_network_count" => count(entry -> get(entry, "analysis_status", "") == "excluded_by_search_profile", network_entries),
        "deduplicated_network_count" => length(duplicate_inputs),
        "pruned_against_library" => skip_existing && library !== nothing,
        "pruned_against_sqlite" => sqlite_pruning,
        "skipped_existing_network_count" => length(skipped_existing_networks),
        "skipped_existing_slice_count" => skipped_existing_slice_count,
        "network_entries" => network_entries,
        "input_graph_slices" => input_graph_slices,
        "behavior_slices" => behavior_slices,
        "regime_records" => regime_records,
        "transition_records" => transition_records,
        "family_buckets" => family_buckets,
        "path_records" => path_records,
        "duplicate_inputs" => duplicate_inputs,
        "skipped_existing_networks" => skipped_existing_networks,
    )
end

function _atlas_content_id(atlas)
    payload = Dict(
        "atlas_schema_version" => String(_raw_get(atlas, :atlas_schema_version, "unknown")),
        "search_profile" => _materialize(_raw_get(atlas, :search_profile, Dict{String, Any}())),
        "behavior_config" => _materialize(_raw_get(atlas, :behavior_config, Dict{String, Any}())),
        "network_ids" => sort!([String(_raw_get(entry, :network_id, "")) for entry in collect(_raw_get(atlas, :network_entries, Any[])) if !isempty(String(_raw_get(entry, :network_id, "")))]),
        "graph_slice_ids" => sort!([String(_raw_get(item, :graph_slice_id, "")) for item in collect(_raw_get(atlas, :input_graph_slices, Any[])) if !isempty(String(_raw_get(item, :graph_slice_id, "")))]),
        "slice_ids" => sort!([String(_raw_get(slice, :slice_id, "")) for slice in collect(_raw_get(atlas, :behavior_slices, Any[])) if !isempty(String(_raw_get(slice, :slice_id, "")))]),
        "regime_record_ids" => sort!([String(_raw_get(item, :regime_record_id, "")) for item in collect(_raw_get(atlas, :regime_records, Any[])) if !isempty(String(_raw_get(item, :regime_record_id, "")))]),
        "transition_record_ids" => sort!([String(_raw_get(item, :transition_record_id, "")) for item in collect(_raw_get(atlas, :transition_records, Any[])) if !isempty(String(_raw_get(item, :transition_record_id, "")))]),
        "bucket_ids" => sort!([String(_raw_get(bucket, :bucket_id, "")) for bucket in collect(_raw_get(atlas, :family_buckets, Any[])) if !isempty(String(_raw_get(bucket, :bucket_id, "")))]),
        "path_record_ids" => sort!([String(_raw_get(rec, :path_record_id, "")) for rec in collect(_raw_get(atlas, :path_records, Any[])) if !isempty(String(_raw_get(rec, :path_record_id, "")))]),
    )
    return bytes2hex(SHA.sha1(JSON3.write(payload)))
end

function _build_atlas_manifest(atlas; source_label=nothing, source_metadata=nothing)
    atlas_id = _atlas_content_id(atlas)
    default_label = "atlas_" * atlas_id[1:12]
    return Dict(
        "atlas_id" => atlas_id,
        "source_label" => source_label === nothing ? default_label : String(source_label),
        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
        "imported_at" => _now_iso_timestamp(),
        "atlas_schema_version" => String(_raw_get(atlas, :atlas_schema_version, "unknown")),
        "generated_at" => String(_raw_get(atlas, :generated_at, "unknown")),
        "search_profile" => _materialize(_raw_get(atlas, :search_profile, Dict{String, Any}())),
        "behavior_config" => _materialize(_raw_get(atlas, :behavior_config, Dict{String, Any}())),
        "input_network_count" => Int(_raw_get(atlas, :input_network_count, length(collect(_raw_get(atlas, :network_entries, Any[]))))),
        "unique_network_count" => Int(_raw_get(atlas, :unique_network_count, length(collect(_raw_get(atlas, :network_entries, Any[]))))),
        "successful_network_count" => Int(_raw_get(atlas, :successful_network_count, 0)),
        "failed_network_count" => Int(_raw_get(atlas, :failed_network_count, 0)),
        "excluded_network_count" => Int(_raw_get(atlas, :excluded_network_count, 0)),
        "deduplicated_network_count" => Int(_raw_get(atlas, :deduplicated_network_count, length(collect(_raw_get(atlas, :duplicate_inputs, Any[]))))),
        "skipped_existing_network_count" => Int(_raw_get(atlas, :skipped_existing_network_count, 0)),
        "skipped_existing_slice_count" => Int(_raw_get(atlas, :skipped_existing_slice_count, 0)),
        "input_graph_slice_count" => length(collect(_raw_get(atlas, :input_graph_slices, Any[]))),
        "behavior_slice_count" => length(collect(_raw_get(atlas, :behavior_slices, Any[]))),
        "regime_record_count" => length(collect(_raw_get(atlas, :regime_records, Any[]))),
        "transition_record_count" => length(collect(_raw_get(atlas, :transition_records, Any[]))),
        "family_bucket_count" => length(collect(_raw_get(atlas, :family_buckets, Any[]))),
        "path_record_count" => length(collect(_raw_get(atlas, :path_records, Any[]))),
    )
end

function _prepare_library_network_entry(entry, atlas_id::String)
    prepared = _materialize(entry)
    prepared["atlas_source_ids"] = [atlas_id]
    prepared["source_labels"] = _sorted_unique_strings([_raw_get(prepared, :source_label, "")])
    prepared["source_kinds"] = _sorted_unique_strings([_raw_get(prepared, :source_kind, "")])
    return prepared
end

function _merge_network_entry!(existing::Dict{String, Any}, incoming, atlas_id::String)
    _append_unique_string_field!(existing, "atlas_source_ids", [atlas_id])
    _append_unique_string_field!(existing, "source_labels", [_raw_get(incoming, :source_label, "")])
    _append_unique_string_field!(existing, "source_kinds", [_raw_get(incoming, :source_kind, "")])
    _append_unique_string_field!(existing, "profile_issues", collect(_raw_get(incoming, :profile_issues, String[])))
    existing["analysis_status"] = _merge_status([
        _raw_get(existing, :analysis_status, "unknown"),
        _raw_get(incoming, :analysis_status, "unknown"),
    ])

    for field in ["canonical_code", "raw_rules", "support_map", "base_species_count", "reaction_count", "total_species_count", "max_support", "support_mass"]
        if (!haskey(existing, field) || existing[field] === nothing) && _raw_haskey(incoming, Symbol(field))
            existing[field] = _materialize(_raw_get(incoming, Symbol(field), nothing))
        end
    end

    if !haskey(existing, "source_metadata_records")
        existing["source_metadata_records"] = Any[]
    end
    source_metadata = _raw_get(incoming, :source_metadata, nothing)
    if source_metadata !== nothing
        push!(existing["source_metadata_records"], _materialize(source_metadata))
    end

    existing["source_label"] = isempty(existing["source_labels"]) ? String(_raw_get(existing, :source_label, "")) : first(existing["source_labels"])
    existing["source_kind"] = isempty(existing["source_kinds"]) ? String(_raw_get(existing, :source_kind, "")) : first(existing["source_kinds"])
    return existing
end

function _merge_atlas_object!(objects::Vector{Any}, index::AbstractDict, incoming, id_key::String, atlas_id::String)
    object_id = String(_raw_get(incoming, Symbol(id_key), ""))
    isempty(object_id) && return false

    if haskey(index, object_id)
        existing = index[object_id]
        prepared = _materialize(incoming)
        if _prefer_incoming_atlas_object(existing, prepared)
            atlas_source_ids = _sorted_unique_strings(vcat(
                collect(_raw_get(existing, :atlas_source_ids, String[])),
                collect(_raw_get(prepared, :atlas_source_ids, String[])),
                [atlas_id],
            ))
            _replace_dict_contents!(existing, prepared)
            existing["atlas_source_ids"] = atlas_source_ids
        else
            for (key, value) in pairs(prepared)
                key == "atlas_source_ids" && continue
                if !haskey(existing, key) || existing[key] === nothing
                    existing[key] = value
                end
            end
        end
        _append_unique_string_field!(existing, "atlas_source_ids", [atlas_id])
        if _raw_haskey(incoming, :analysis_status)
            existing["analysis_status"] = _merge_status([
                _raw_get(existing, :analysis_status, "unknown"),
                _raw_get(incoming, :analysis_status, "unknown"),
            ])
        end
        return false
    end

    prepared = _materialize(incoming)
    prepared["atlas_source_ids"] = [atlas_id]
    push!(objects, prepared)
    index[object_id] = prepared
    return true
end

function _refresh_library_network_entries!(library::Dict{String, Any})
    network_entries = Vector{Any}(collect(_raw_get(library, :network_entries, Any[])))
    behavior_slices = Vector{Any}(collect(_raw_get(library, :behavior_slices, Any[])))

    grouped = Dict{String, Vector{Any}}()
    for slice in behavior_slices
        network_id = String(_raw_get(slice, :network_id, ""))
        isempty(network_id) && continue
        push!(get!(grouped, network_id, Any[]), slice)
    end

    for entry in network_entries
        network_id = String(_raw_get(entry, :network_id, ""))
        slices = get(grouped, network_id, Any[])
        if !isempty(slices)
            summary = _build_network_summary(slices)
            merge!(entry, summary)
            if Int(summary["successful_slice_count"]) > 0
                entry["analysis_status"] = "ok"
            elseif Int(summary["failed_slice_count"]) > 0
                entry["analysis_status"] = "failed"
            else
                entry["analysis_status"] = _merge_status([_raw_get(entry, :analysis_status, "pending")])
            end
            entry["build_state"] = _network_build_state(summary)
            if entry["build_state"] == "complete"
                for key in ("failure_class", "failure_stage", "failure_message", "unsupported_feature", "integrity_issues", "error")
                    haskey(entry, key) && delete!(entry, key)
                end
            end
        else
            entry["slice_ids"] = collect(_raw_get(entry, :slice_ids, String[]))
            entry["successful_slice_count"] = Int(_raw_get(entry, :successful_slice_count, 0))
            entry["failed_slice_count"] = Int(_raw_get(entry, :failed_slice_count, 0))
            entry["partial_result_slice_count"] = Int(_raw_get(entry, :partial_result_slice_count, 0))
            entry["failure_classes"] = _sorted_unique_strings(collect(_raw_get(entry, :failure_classes, String[])))
            entry["motif_union"] = _sorted_unique_strings(collect(_raw_get(entry, :motif_union, String[])))
            entry["exact_union"] = _sorted_unique_strings(collect(_raw_get(entry, :exact_union, String[])))
            entry["build_state"] = String(_raw_get(entry, :build_state, _network_build_state(entry)))
        end

        _ensure_string_vector_field!(entry, "atlas_source_ids")
        _ensure_string_vector_field!(entry, "source_labels")
        _ensure_string_vector_field!(entry, "source_kinds")
        entry["source_label"] = isempty(entry["source_labels"]) ? String(_raw_get(entry, :source_label, "")) : first(entry["source_labels"])
        entry["source_kind"] = isempty(entry["source_kinds"]) ? String(_raw_get(entry, :source_kind, "")) : first(entry["source_kinds"])
        if haskey(entry, "source_metadata_records")
            entry["source_metadata_records"] = Any[_materialize(item) for item in entry["source_metadata_records"]]
        end
    end

    sort!(network_entries; by=entry -> String(_raw_get(entry, :network_id, "")))
    library["network_entries"] = network_entries
    return library
end

function _refresh_atlas_library!(library::Dict{String, Any})
    _normalize_behavior_slices!(library)
    _refresh_library_network_entries!(library)

    sort!(library["input_graph_slices"]; by=item -> String(_raw_get(item, :graph_slice_id, "")))
    sort!(library["behavior_slices"]; by=slice -> String(_raw_get(slice, :slice_id, "")))
    sort!(library["regime_records"]; by=item -> String(_raw_get(item, :regime_record_id, "")))
    sort!(library["transition_records"]; by=item -> String(_raw_get(item, :transition_record_id, "")))
    sort!(library["family_buckets"]; by=bucket -> String(_raw_get(bucket, :bucket_id, "")))
    sort!(library["path_records"]; by=rec -> String(_raw_get(rec, :path_record_id, "")))
    sort!(library["duplicate_inputs"]; by=dup -> (String(_raw_get(dup, :source_label, "")), String(_raw_get(dup, :duplicate_of_network_id, ""))))

    manifests = Vector{Any}(collect(_raw_get(library, :atlas_manifests, Any[])))
    merge_events = Vector{Any}(collect(_raw_get(library, :merge_events, Any[])))
    network_entries = Vector{Any}(collect(_raw_get(library, :network_entries, Any[])))
    input_graph_slices = Vector{Any}(collect(_raw_get(library, :input_graph_slices, Any[])))
    behavior_slices = Vector{Any}(collect(_raw_get(library, :behavior_slices, Any[])))
    regime_records = Vector{Any}(collect(_raw_get(library, :regime_records, Any[])))
    transition_records = Vector{Any}(collect(_raw_get(library, :transition_records, Any[])))
    family_buckets = Vector{Any}(collect(_raw_get(library, :family_buckets, Any[])))
    path_records = Vector{Any}(collect(_raw_get(library, :path_records, Any[])))

    library["atlas_count"] = length(manifests)
    library["input_network_count"] = sum((Int(_raw_get(manifest, :input_network_count, 0)) for manifest in manifests); init=0)
    library["unique_network_count"] = length(network_entries)
    library["successful_network_count"] = count(entry -> String(_raw_get(entry, :analysis_status, "")) == "ok", network_entries)
    library["failed_network_count"] = count(entry -> String(_raw_get(entry, :analysis_status, "")) == "failed", network_entries)
    library["excluded_network_count"] = count(entry -> String(_raw_get(entry, :analysis_status, "")) == "excluded_by_search_profile", network_entries)
    library["deduplicated_network_count"] = sum((Int(_raw_get(manifest, :deduplicated_network_count, 0)) for manifest in manifests); init=0)
    library["input_graph_slice_count"] = length(input_graph_slices)
    library["behavior_slice_count"] = length(behavior_slices)
    library["regime_record_count"] = length(regime_records)
    library["transition_record_count"] = length(transition_records)
    library["family_bucket_count"] = length(family_buckets)
    library["path_record_count"] = length(path_records)
    library["updated_at"] = _now_iso_timestamp()
    library["atlas_library_schema_version"] = "0.2.0"
    library["atlas_schema_version"] = "0.2.0"
    library["atlas_manifests"] = manifests
    library["merge_events"] = merge_events
    return library
end

function build_atlas_library(atlas; source_label=nothing, source_metadata=nothing, library_label=nothing)
    library = atlas_library_default()
    library_label === nothing || (library["library_label"] = String(library_label))
    return merge_atlas_library(library, atlas; source_label=source_label, source_metadata=source_metadata)
end

function _is_empty_atlas_delta(atlas)
    return isempty(collect(_raw_get(atlas, :network_entries, Any[]))) &&
           isempty(collect(_raw_get(atlas, :input_graph_slices, Any[]))) &&
           isempty(collect(_raw_get(atlas, :behavior_slices, Any[]))) &&
           isempty(collect(_raw_get(atlas, :regime_records, Any[]))) &&
           isempty(collect(_raw_get(atlas, :transition_records, Any[]))) &&
           isempty(collect(_raw_get(atlas, :family_buckets, Any[]))) &&
           isempty(collect(_raw_get(atlas, :path_records, Any[])))
end

function _record_library_skip_only_event(library; source_label=nothing, source_metadata=nothing, skipped_existing_network_count::Int=0, skipped_existing_slice_count::Int=0)
    merged = _materialize(library)
    is_atlas_library(merged) || error("_record_library_skip_only_event expects an atlas library.")

    push!(merged["merge_events"], Dict(
        "merged_at" => _now_iso_timestamp(),
        "source_label" => source_label === nothing ? "atlas_spec" : String(source_label),
        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
        "status" => "skipped_all_existing",
        "skipped_existing_network_count" => skipped_existing_network_count,
        "skipped_existing_slice_count" => skipped_existing_slice_count,
    ))

    return _refresh_atlas_library!(merged)
end

function merge_atlas_library(library, atlas; source_label=nothing, source_metadata=nothing, allow_duplicate_atlas::Bool=false)
    merged = _materialize(library)
    is_atlas_library(merged) || error("merge_atlas_library expects `library` to be an atlas library object.")
    _is_atlas_corpus(atlas) || error("merge_atlas_library expects `atlas` to be an atlas or atlas-library-like corpus.")

    corpus = _materialize(atlas)
    manifest = _build_atlas_manifest(corpus; source_label=source_label, source_metadata=source_metadata)
    atlas_id = manifest["atlas_id"]

    manifest_ids = Set(String(_raw_get(item, :atlas_id, "")) for item in collect(_raw_get(merged, :atlas_manifests, Any[])))
    if atlas_id in manifest_ids && !allow_duplicate_atlas
        push!(merged["merge_events"], Dict(
            "merged_at" => _now_iso_timestamp(),
            "atlas_id" => atlas_id,
            "source_label" => manifest["source_label"],
            "status" => "skipped_duplicate_atlas",
        ))
        return _refresh_atlas_library!(merged)
    end

    network_entries = Vector{Any}(collect(_raw_get(merged, :network_entries, Any[])))
    input_graph_slices = Vector{Any}(collect(_raw_get(merged, :input_graph_slices, Any[])))
    behavior_slices = Vector{Any}(collect(_raw_get(merged, :behavior_slices, Any[])))
    regime_records = Vector{Any}(collect(_raw_get(merged, :regime_records, Any[])))
    transition_records = Vector{Any}(collect(_raw_get(merged, :transition_records, Any[])))
    family_buckets = Vector{Any}(collect(_raw_get(merged, :family_buckets, Any[])))
    path_records = Vector{Any}(collect(_raw_get(merged, :path_records, Any[])))
    duplicate_inputs = Vector{Any}(collect(_raw_get(merged, :duplicate_inputs, Any[])))

    network_index = _atlas_network_index(network_entries)
    graph_slice_index = Dict(String(_raw_get(item, :graph_slice_id, "")) => item for item in input_graph_slices)
    slice_index = _atlas_slice_index(behavior_slices)
    regime_index = Dict(String(_raw_get(item, :regime_record_id, "")) => item for item in regime_records)
    transition_index = Dict(String(_raw_get(item, :transition_record_id, "")) => item for item in transition_records)
    bucket_index = Dict(String(_raw_get(bucket, :bucket_id, "")) => bucket for bucket in family_buckets)
    path_index = Dict(String(_raw_get(rec, :path_record_id, "")) => rec for rec in path_records)

    added_networks = 0
    added_graph_slices = 0
    added_slices = 0
    added_regimes = 0
    added_transitions = 0
    added_buckets = 0
    added_paths = 0
    duplicate_network_hits = 0

    for raw_entry in collect(_raw_get(corpus, :network_entries, Any[]))
        network_id = String(_raw_get(raw_entry, :network_id, ""))
        isempty(network_id) && continue

        if haskey(network_index, network_id)
            _merge_network_entry!(network_index[network_id], raw_entry, atlas_id)
            duplicate_network_hits += 1
        else
            prepared = _prepare_library_network_entry(raw_entry, atlas_id)
            push!(network_entries, prepared)
            network_index[network_id] = prepared
            added_networks += 1
        end
    end

    for slice in collect(_raw_get(corpus, :behavior_slices, Any[]))
        added_slices += _merge_atlas_object!(behavior_slices, slice_index, slice, "slice_id", atlas_id) ? 1 : 0
    end

    for item in collect(_raw_get(corpus, :input_graph_slices, Any[]))
        added_graph_slices += _merge_atlas_object!(input_graph_slices, graph_slice_index, item, "graph_slice_id", atlas_id) ? 1 : 0
    end

    for item in collect(_raw_get(corpus, :regime_records, Any[]))
        added_regimes += _merge_atlas_object!(regime_records, regime_index, item, "regime_record_id", atlas_id) ? 1 : 0
    end

    for item in collect(_raw_get(corpus, :transition_records, Any[]))
        added_transitions += _merge_atlas_object!(transition_records, transition_index, item, "transition_record_id", atlas_id) ? 1 : 0
    end

    for bucket in collect(_raw_get(corpus, :family_buckets, Any[]))
        added_buckets += _merge_atlas_object!(family_buckets, bucket_index, bucket, "bucket_id", atlas_id) ? 1 : 0
    end

    for rec in collect(_raw_get(corpus, :path_records, Any[]))
        added_paths += _merge_atlas_object!(path_records, path_index, rec, "path_record_id", atlas_id) ? 1 : 0
    end

    for raw_dup in collect(_raw_get(corpus, :duplicate_inputs, Any[]))
        dup = _materialize(raw_dup)
        dup["atlas_source_ids"] = [atlas_id]
        push!(duplicate_inputs, dup)
    end

    merged["network_entries"] = network_entries
    merged["input_graph_slices"] = input_graph_slices
    merged["behavior_slices"] = behavior_slices
    merged["regime_records"] = regime_records
    merged["transition_records"] = transition_records
    merged["family_buckets"] = family_buckets
    merged["path_records"] = path_records
    merged["duplicate_inputs"] = duplicate_inputs
    push!(merged["atlas_manifests"], manifest)
    push!(merged["merge_events"], Dict(
        "merged_at" => _now_iso_timestamp(),
        "atlas_id" => atlas_id,
        "source_label" => manifest["source_label"],
        "status" => "merged",
        "added_network_count" => added_networks,
        "duplicate_network_count" => duplicate_network_hits,
        "added_input_graph_slice_count" => added_graph_slices,
        "added_slice_count" => added_slices,
        "added_regime_record_count" => added_regimes,
        "added_transition_record_count" => added_transitions,
        "added_family_bucket_count" => added_buckets,
        "added_path_record_count" => added_paths,
        "skipped_existing_network_count" => Int(_raw_get(manifest, :skipped_existing_network_count, 0)),
        "skipped_existing_slice_count" => Int(_raw_get(manifest, :skipped_existing_slice_count, 0)),
    ))

    return _refresh_atlas_library!(merged)
end

function _resolve_atlas_corpus_from_spec(spec)
    if _is_atlas_corpus(spec)
        return spec
    elseif _raw_haskey(spec, :atlas)
        return _raw_get(spec, :atlas, nothing)
    elseif _raw_haskey(spec, :atlas_spec)
        atlas_spec = _raw_get(spec, :atlas_spec, nothing)
        return _is_atlas_corpus(atlas_spec) ? atlas_spec : build_behavior_atlas_from_spec(atlas_spec)
    elseif _raw_haskey(spec, :networks) || _raw_haskey(spec, :enumeration)
        return build_behavior_atlas_from_spec(spec)
    else
        error("Atlas library spec must include `atlas`, `atlas_spec`, or a direct atlas build spec.")
    end
end

function build_atlas_library_from_spec(spec)
    atlas = _resolve_atlas_corpus_from_spec(spec)
    source_label = _raw_haskey(spec, :source_label) ? String(_raw_get(spec, :source_label, "")) : nothing
    source_metadata = _raw_haskey(spec, :source_metadata) ? _raw_get(spec, :source_metadata, nothing) : nothing
    library_label = _raw_haskey(spec, :library_label) ? String(_raw_get(spec, :library_label, "")) : nothing
    library = build_atlas_library(atlas;
        source_label=source_label,
        source_metadata=source_metadata,
        library_label=library_label,
    )
    sqlite_path = _sqlite_path_from_raw(spec)
    sqlite_path === nothing || atlas_sqlite_save_library!(sqlite_path, library)
    return library
end

function merge_atlas_library_from_spec(spec)
    sqlite_path = _sqlite_path_from_raw(spec)
    if !_raw_haskey(spec, :library) && sqlite_path === nothing
        error("Atlas library merge spec must include `library` or `sqlite_path`.")
    end
    library = if _raw_haskey(spec, :library)
        _raw_get(spec, :library, nothing)
    elseif atlas_sqlite_has_library(sqlite_path)
        atlas_sqlite_load_library(sqlite_path)
    else
        atlas_library_default()
    end
    source_label = _raw_haskey(spec, :source_label) ? String(_raw_get(spec, :source_label, "")) : nothing
    source_metadata = _raw_haskey(spec, :source_metadata) ? _raw_get(spec, :source_metadata, nothing) : nothing
    allow_duplicate_atlas = Bool(_raw_get(spec, :allow_duplicate_atlas, false))
    skip_existing = Bool(_raw_get(spec, :skip_existing, true))

    atlas = if _raw_haskey(spec, :atlas)
        _raw_get(spec, :atlas, nothing)
    elseif _raw_haskey(spec, :atlas_spec)
        atlas_spec = _raw_get(spec, :atlas_spec, nothing)
        if _is_atlas_corpus(atlas_spec)
            atlas_spec
        else
            request = Dict{String, Any}(
                "library" => library,
                "skip_existing" => skip_existing,
            )
            _raw_haskey(atlas_spec, :search_profile) && (request["search_profile"] = _raw_get(atlas_spec, :search_profile, nothing))
            _raw_haskey(atlas_spec, :behavior_config) && (request["behavior_config"] = _raw_get(atlas_spec, :behavior_config, nothing))
            _raw_haskey(atlas_spec, :networks) && (request["networks"] = _raw_get(atlas_spec, :networks, Any[]))
            _raw_haskey(atlas_spec, :enumeration) && (request["enumeration"] = _raw_get(atlas_spec, :enumeration, nothing))
            build_behavior_atlas_from_spec(request)
        end
    elseif _raw_haskey(spec, :networks) || _raw_haskey(spec, :enumeration)
        request = Dict{String, Any}(
            "library" => library,
            "skip_existing" => skip_existing,
        )
        _raw_haskey(spec, :search_profile) && (request["search_profile"] = _raw_get(spec, :search_profile, nothing))
        _raw_haskey(spec, :behavior_config) && (request["behavior_config"] = _raw_get(spec, :behavior_config, nothing))
        _raw_haskey(spec, :networks) && (request["networks"] = _raw_get(spec, :networks, Any[]))
        _raw_haskey(spec, :enumeration) && (request["enumeration"] = _raw_get(spec, :enumeration, nothing))
        build_behavior_atlas_from_spec(request)
    else
        error("Atlas library merge spec must include `atlas`, `atlas_spec`, or atlas build fields.")
    end

    if _is_empty_atlas_delta(atlas) && Int(_raw_get(atlas, :skipped_existing_slice_count, 0)) > 0
        merged = _record_library_skip_only_event(library;
            source_label=source_label,
            source_metadata=source_metadata,
            skipped_existing_network_count=Int(_raw_get(atlas, :skipped_existing_network_count, 0)),
            skipped_existing_slice_count=Int(_raw_get(atlas, :skipped_existing_slice_count, 0)),
        )
        sqlite_path === nothing || atlas_sqlite_save_library!(sqlite_path, merged)
        return merged
    end

    merged = merge_atlas_library(library, atlas;
        source_label=source_label,
        source_metadata=source_metadata,
        allow_duplicate_atlas=allow_duplicate_atlas,
    )
    sqlite_path === nothing || atlas_sqlite_save_library!(sqlite_path, merged)
    return merged
end

function _atlas_object_index(objects)
    index = Dict{String, Any}()
    for obj in objects
        key = String(_raw_get(obj, :slice_id, _raw_get(obj, :network_id, _raw_get(obj, :bucket_id, ""))))
        isempty(key) || (index[key] = obj)
    end
    return index
end

function _atlas_network_index(network_entries)
    index = Dict{String, Any}()
    for entry in network_entries
        index[String(_raw_get(entry, :network_id, ""))] = entry
    end
    return index
end

function _atlas_slice_index(behavior_slices)
    index = Dict{String, Any}()
    for slice in behavior_slices
        index[String(_raw_get(slice, :slice_id, ""))] = slice
    end
    return index
end

function _atlas_records_by_slice(records, id_key::Symbol=:slice_id)
    grouped = Dict{String, Vector{Any}}()
    for record in records
        slice_id = String(_raw_get(record, id_key, ""))
        isempty(slice_id) && continue
        push!(get!(grouped, slice_id, Any[]), record)
    end
    return grouped
end

function _atlas_family_buckets_by_slice(family_buckets)
    grouped = Dict{String, Vector{Any}}()
    for bucket in family_buckets
        slice_id = String(_raw_get(bucket, :slice_id, ""))
        push!(get!(grouped, slice_id, Any[]), bucket)
    end
    return grouped
end

_atlas_path_records_by_slice(path_records) = _atlas_records_by_slice(path_records)
_atlas_regime_records_by_slice(regime_records) = _atlas_records_by_slice(regime_records)
_atlas_transition_records_by_slice(transition_records) = _atlas_records_by_slice(transition_records)

function _normalize_query_output_order_token(value)
    value === nothing && return nothing
    if value isa Real
        return _output_order_token(value)
    end
    str = String(value)
    isempty(str) && return nothing
    return str
end

function _regime_record_matches_predicate(record, predicate)
    isempty(predicate) && return true
    for (key_any, value) in pairs(predicate)
        key = String(key_any)
        if key == "role"
            String(_raw_get(record, :role, "")) == String(value) || return false
        elseif key == "output_order_token"
            String(_raw_get(record, :output_order_token, "")) == _normalize_query_output_order_token(value) || return false
        elseif key == "singular"
            Bool(_raw_get(record, :singular, false)) == Bool(value) || return false
        elseif key == "asymptotic"
            Bool(_raw_get(record, :asymptotic, false)) == Bool(value) || return false
        elseif key == "nullity"
            Int(_raw_get(record, :nullity, -1)) == Int(value) || return false
        elseif key == "vertex_idx"
            Int(_raw_get(record, :vertex_idx, -1)) == Int(value) || return false
        elseif key == "is_source" || key == "source"
            Bool(_raw_get(record, :is_source, false)) == Bool(value) || return false
        elseif key == "is_sink" || key == "sink"
            Bool(_raw_get(record, :is_sink, false)) == Bool(value) || return false
        elseif key == "is_branch" || key == "branch"
            Bool(_raw_get(record, :is_branch, false)) == Bool(value) || return false
        elseif key == "is_merge" || key == "merge"
            Bool(_raw_get(record, :is_merge, false)) == Bool(value) || return false
        elseif key == "reachable_from_source"
            Bool(_raw_get(record, :reachable_from_source, false)) == Bool(value) || return false
        elseif key == "can_reach_sink"
            Bool(_raw_get(record, :can_reach_sink, false)) == Bool(value) || return false
        else
            return false
        end
    end
    return true
end

function _transition_view(record, prefix::String)
    return Dict{String, Any}(
        "vertex_idx" => Int(_raw_get(record, Symbol(prefix * "vertex_idx"), -1)),
        "role" => String(_raw_get(record, Symbol(prefix * "role"), "")),
        "singular" => Bool(_raw_get(record, Symbol(prefix * "singular"), false)),
        "nullity" => Int(_raw_get(record, Symbol(prefix * "nullity"), -1)),
        "output_order_token" => String(_raw_get(record, Symbol(prefix * "output_order_token"), "")),
    )
end

function _transition_record_matches_predicate(record, predicate)
    isempty(predicate) && return true
    for (key_any, value) in pairs(predicate)
        key = String(key_any)
        if key == "transition_token"
            String(_raw_get(record, :transition_token, "")) == String(value) || return false
        elseif key == "from"
            value isa AbstractDict || return false
            _regime_record_matches_predicate(_transition_view(record, "from_"), Dict{String, Any}(_materialize(value))) || return false
        elseif key == "to"
            value isa AbstractDict || return false
            _regime_record_matches_predicate(_transition_view(record, "to_"), Dict{String, Any}(_materialize(value))) || return false
        elseif key == "from_output_order_token"
            String(_raw_get(record, :from_output_order_token, "")) == _normalize_query_output_order_token(value) || return false
        elseif key == "to_output_order_token"
            String(_raw_get(record, :to_output_order_token, "")) == _normalize_query_output_order_token(value) || return false
        elseif key == "from_role"
            String(_raw_get(record, :from_role, "")) == String(value) || return false
        elseif key == "to_role"
            String(_raw_get(record, :to_role, "")) == String(value) || return false
        else
            return false
        end
    end
    return true
end

function _query_requires_witness(query::AtlasQuerySpec)
    return !isempty(query.required_path_sequences) ||
           query.forbid_singular_on_witness ||
           query.require_witness_feasible ||
           query.require_witness_robust ||
           query.min_witness_volume_mean !== nothing ||
           query.max_witness_path_length !== nothing
end

function _path_volume_mean(record)
    volume = _raw_get(record, :volume, nothing)
    volume === nothing && return nothing
    return _raw_haskey(volume, :mean) ? Float64(_raw_get(volume, :mean, 0.0)) : nothing
end

function _path_meets_witness_constraints(record, query::AtlasQuerySpec)
    Bool(_raw_get(record, :included, false)) || return false
    query.require_witness_feasible && !Bool(_raw_get(record, :feasible, false)) && return false
    query.require_witness_robust && !Bool(_raw_get(record, :robust, false)) && return false
    if query.min_witness_volume_mean !== nothing
        volume_mean = _path_volume_mean(record)
        volume_mean === nothing && return false
        volume_mean >= query.min_witness_volume_mean || return false
    end
    if query.max_witness_path_length !== nothing
        length(collect(_raw_get(record, :vertex_indices, Any[]))) <= query.max_witness_path_length || return false
    end
    if query.forbid_singular_on_witness
        for regime in collect(_raw_get(record, :regime_sequence, Any[]))
            Bool(_raw_get(regime, :singular, false)) && return false
        end
    end
    return true
end

function _path_sequence_matches(record, sequence::AbstractVector{<:AbstractDict})
    isempty(sequence) && return true
    regimes = collect(_raw_get(record, :regime_sequence, Any[]))
    isempty(regimes) && return false

    next_idx = 1
    for predicate in sequence
        matched = false
        while next_idx <= length(regimes)
            regime = regimes[next_idx]
            if _regime_record_matches_predicate(regime, predicate)
                matched = true
                next_idx += 1
                break
            end
            next_idx += 1
        end
        matched || return false
    end
    return true
end

function _best_witness_path(paths)
    isempty(paths) && return nothing
    sorted_paths = collect(paths)
    sort!(sorted_paths; by=path -> (
        -Int(Bool(_raw_get(path, :robust, false))),
        -Float64(something(_path_volume_mean(path), -1.0)),
        Int(_raw_get(path, :path_idx, typemax(Int))),
    ))
    return first(sorted_paths)
end

function _matching_graph_records(records, required_predicates, forbidden_predicates, matcher)
    for predicate in forbidden_predicates
        any(record -> matcher(record, predicate), records) && return Any[], false
    end

    isempty(required_predicates) && return Any[], true

    matched = Any[]
    for predicate in required_predicates
        predicate_matches = [record for record in records if matcher(record, predicate)]
        isempty(predicate_matches) && return Any[], false
        push!(matched, first(predicate_matches))
    end
    return matched, true
end

function _matching_witness_paths(paths, query::AtlasQuerySpec)
    filtered = [path for path in paths if _path_meets_witness_constraints(path, query)]

    if isempty(query.required_path_sequences)
        return _query_requires_witness(query) ? filtered : Any[], !_query_requires_witness(query) || !isempty(filtered)
    end

    matched = Any[]
    for sequence in query.required_path_sequences
        seq_matches = [path for path in filtered if _path_sequence_matches(path, sequence)]
        isempty(seq_matches) && return Any[], false
        push!(matched, _best_witness_path(seq_matches))
    end

    return matched, true
end

function _bucket_meets_robustness(bucket, query::AtlasQuerySpec)
    threshold = max(query.min_robust_path_count, query.require_robust ? 1 : 0)
    return Int(_raw_get(bucket, :robust_path_count, 0)) >= threshold
end

function _bucket_label_matches(bucket, labels::Vector{String})
    isempty(labels) && return true
    return String(_raw_get(bucket, :family_label, "")) in labels
end

function _matching_family_buckets(buckets, family_kind::String, labels::Vector{String}, match_mode::Symbol, query::AtlasQuerySpec)
    family_buckets = [bucket for bucket in buckets if String(_raw_get(bucket, :family_kind, "")) == family_kind]
    if isempty(labels)
        if query.require_robust || query.min_robust_path_count > 0
            return [bucket for bucket in family_buckets if _bucket_meets_robustness(bucket, query)], true
        end
        return family_buckets, true
    end

    matched = [bucket for bucket in family_buckets if _bucket_label_matches(bucket, labels) && _bucket_meets_robustness(bucket, query)]
    matched_labels = Set(String(_raw_get(bucket, :family_label, "")) for bucket in matched)

    ok = if match_mode == :all
        all(label -> label in matched_labels, labels)
    elseif match_mode == :any
        !isempty(matched)
    else
        error("Unsupported family match mode: $(match_mode)")
    end

    return matched, ok
end

function _passes_network_constraints(network_entry, query::AtlasQuerySpec)
    base_species_count = _raw_get(network_entry, :base_species_count, nothing)
    reaction_count = _raw_get(network_entry, :reaction_count, nothing)
    max_support = _raw_get(network_entry, :max_support, nothing)
    support_mass = _raw_get(network_entry, :support_mass, nothing)

    query.max_base_species !== nothing && (base_species_count === nothing || Int(base_species_count) > query.max_base_species) && return false
    query.max_reactions !== nothing && (reaction_count === nothing || Int(reaction_count) > query.max_reactions) && return false
    query.max_support !== nothing && (max_support === nothing || Int(max_support) > query.max_support) && return false
    query.max_support_mass !== nothing && (support_mass === nothing || Int(support_mass) > query.max_support_mass) && return false

    return true
end

function _passes_io_constraints(slice, query::AtlasQuerySpec)
    input_symbol = String(_raw_get(slice, :input_symbol, ""))
    output_symbol = String(_raw_get(slice, :output_symbol, ""))
    isempty(query.input_symbols) || input_symbol in query.input_symbols || return false
    isempty(query.output_symbols) || output_symbol in query.output_symbols || return false
    return true
end

function _slice_robustness_score(motif_buckets, exact_buckets)
    score = 0.0
    for bucket in vcat(motif_buckets, exact_buckets)
        score += Float64(_raw_get(bucket, :robust_path_count, 0))
        volume_mean = _raw_get(bucket, :volume_mean, nothing)
        volume_mean === nothing || (score += Float64(volume_mean))
    end
    return score
end

function _slice_ranking_key(network_entry, robustness_score::Float64, query::AtlasQuerySpec)
    base_species_count = Int(_raw_get(network_entry, :base_species_count, typemax(Int)))
    reaction_count = Int(_raw_get(network_entry, :reaction_count, typemax(Int)))
    max_support = Int(_raw_get(network_entry, :max_support, typemax(Int)))
    support_mass = Int(_raw_get(network_entry, :support_mass, typemax(Int)))

    if query.ranking_mode == :minimal_first
        return (base_species_count, reaction_count, max_support, support_mass, -robustness_score)
    elseif query.ranking_mode == :robustness_first
        return (-robustness_score, base_species_count, reaction_count, max_support, support_mass)
    else
        error("Unsupported atlas ranking mode: $(query.ranking_mode)")
    end
end

function _pareto_signature(result)
    return (
        Float64(_raw_get(result, :base_species_count, typemax(Int))),
        Float64(_raw_get(result, :reaction_count, typemax(Int))),
        Float64(_raw_get(result, :max_support, typemax(Int))),
        Float64(_raw_get(result, :support_mass, typemax(Int))),
        -Float64(_raw_get(result, :robustness_score, 0.0)),
    )
end

function _dominates_pareto(a, b)::Bool
    sig_a = _pareto_signature(a)
    sig_b = _pareto_signature(b)
    all(sig_a[idx] <= sig_b[idx] for idx in eachindex(sig_a)) || return false
    return any(sig_a[idx] < sig_b[idx] for idx in eachindex(sig_a))
end

function _pareto_filter_results(results)
    frontier = Dict{String, Any}[]
    for (idx, result) in enumerate(results)
        dominated = false
        for (other_idx, other) in enumerate(results)
            idx == other_idx && continue
            if _dominates_pareto(other, result)
                dominated = true
                break
            end
        end
        dominated || push!(frontier, result)
    end
    return frontier
end

function _collapse_results_by_network(results, query::AtlasQuerySpec)
    grouped = Dict{String, Vector{Dict{String, Any}}}()
    for result in results
        network_id = String(_raw_get(result, :network_id, ""))
        push!(get!(grouped, network_id, Dict{String, Any}[]), result)
    end

    collapsed = Dict{String, Any}[]
    for network_id in sort!(collect(keys(grouped)))
        slices = grouped[network_id]
        sort!(slices; by=result -> Tuple(Float64.(result["ranking_key"])))
        best = first(slices)
        push!(collapsed, Dict(
            "network_id" => network_id,
            "source_label" => String(_raw_get(best, :source_label, "")),
            "source_kind" => String(_raw_get(best, :source_kind, "")),
            "base_species_count" => _raw_get(best, :base_species_count, nothing),
            "reaction_count" => _raw_get(best, :reaction_count, nothing),
            "max_support" => _raw_get(best, :max_support, nothing),
            "support_mass" => _raw_get(best, :support_mass, nothing),
            "raw_rules" => collect(_raw_get(best, :raw_rules, Any[])),
            "best_slice_id" => String(_raw_get(best, :slice_id, "")),
            "best_input_symbol" => String(_raw_get(best, :input_symbol, "")),
            "best_output_symbol" => String(_raw_get(best, :output_symbol, "")),
            "best_ranking_key" => collect(_raw_get(best, :ranking_key, Any[])),
            "robustness_score" => Float64(_raw_get(best, :robustness_score, 0.0)),
            "matching_slice_count" => length(slices),
            "matching_slice_ids" => [String(_raw_get(slice, :slice_id, "")) for slice in slices],
            "matching_input_symbols" => sort!(unique([String(_raw_get(slice, :input_symbol, "")) for slice in slices])),
            "matching_output_symbols" => sort!(unique([String(_raw_get(slice, :output_symbol, "")) for slice in slices])),
            "matched_bucket_count" => maximum(Int(_raw_get(slice, :matched_bucket_count, 0)) for slice in slices),
            "matched_robust_path_count" => maximum(Int(_raw_get(slice, :matched_robust_path_count, 0)) for slice in slices),
            "matched_regime_count" => maximum(Int(_raw_get(slice, :matched_regime_count, 0)) for slice in slices),
            "matched_transition_count" => maximum(Int(_raw_get(slice, :matched_transition_count, 0)) for slice in slices),
            "witness_path_count" => maximum(Int(_raw_get(slice, :witness_path_count, 0)) for slice in slices),
            "motif_union" => sort!(unique(vcat([collect(_raw_get(slice, :motif_union, Any[])) for slice in slices]...))),
            "exact_union" => sort!(unique(vcat([collect(_raw_get(slice, :exact_union, Any[])) for slice in slices]...))),
            "best_matched_motif_buckets" => _raw_get(best, :matched_motif_buckets, Any[]),
            "best_matched_exact_buckets" => _raw_get(best, :matched_exact_buckets, Any[]),
            "best_matched_regime_records" => _raw_get(best, :matched_regime_records, Any[]),
            "best_matched_transition_records" => _raw_get(best, :matched_transition_records, Any[]),
            "best_witness_path" => _raw_get(best, :best_witness_path, nothing),
        ))
    end

    if query.ranking_mode == :minimal_first
        sort!(collapsed; by=result -> Tuple(Float64.(result["best_ranking_key"])))
    elseif query.ranking_mode == :robustness_first
        sort!(collapsed; by=result -> Tuple(Float64.(result["best_ranking_key"])))
    end

    return collapsed
end

function query_behavior_atlas(atlas, query_raw::AbstractDict)
    result = query_behavior_atlas_v2(atlas, query_raw; strict=true)["result"]
    if _raw_haskey(query_raw, :goal)
        result["query"] = Dict{String, Any}(_materialize(result["query"]))
        result["query"]["goal"] = _materialize(_raw_get(query_raw, :goal, Dict{String, Any}()))
    end
    return result
end

function query_behavior_atlas(atlas, query::AtlasQuerySpec=atlas_query_spec_default())
    return query_behavior_atlas_v2(atlas, query; strict=false)["result"]
end

function query_behavior_atlas_from_spec(spec)
    _raw_haskey(spec, :query) || error("Atlas query request must include `query`.")
    query_raw = _raw_get(spec, :query, nothing)
    sqlite_prefilter = nothing
    atlas = if _raw_haskey(spec, :atlas)
        _raw_get(spec, :atlas, nothing)
    elseif _raw_haskey(spec, :library)
        _raw_get(spec, :library, nothing)
    elseif (sqlite_path = _sqlite_path_from_raw(spec)) !== nothing
        corpus = atlas_sqlite_load_query_corpus(sqlite_path, query_raw)
        sqlite_prefilter = _raw_get(corpus, :sqlite_prefilter, nothing)
        corpus
    elseif _raw_haskey(spec, :atlas_spec)
        build_behavior_atlas_from_spec(_raw_get(spec, :atlas_spec, nothing))
    else
        error("Atlas query request must include `atlas`, `library`, `sqlite_path`, or `atlas_spec`.")
    end
    result = query_behavior_atlas_v2(atlas, query_raw; strict=true)["result"]
    sqlite_prefilter === nothing || (result["sqlite_prefilter"] = _materialize(sqlite_prefilter))
    if query_raw isa AbstractDict && _raw_haskey(query_raw, :goal)
        result["query"] = Dict{String, Any}(_materialize(result["query"]))
        result["query"]["goal"] = _materialize(_raw_get(query_raw, :goal, Dict{String, Any}()))
    end
    return result
end

function build_behavior_atlas_from_spec(spec)
    search_profile = atlas_search_profile_from_raw(_raw_get(spec, :search_profile, nothing))
    behavior_config = atlas_behavior_config_from_raw(_raw_get(spec, :behavior_config, nothing))
    network_specs = Any[]
    enumeration_summary = nothing
    library = _raw_haskey(spec, :library) ? _raw_get(spec, :library, nothing) : nothing
    sqlite_path = _sqlite_path_from_raw(spec)
    skip_existing = Bool(_raw_get(spec, :skip_existing, library !== nothing || sqlite_path !== nothing))
    persist_sqlite = Bool(_raw_get(spec, :persist_sqlite, false))
    source_label = _raw_haskey(spec, :source_label) ? String(_raw_get(spec, :source_label, "atlas_spec")) : "atlas_spec"
    source_metadata = _raw_haskey(spec, :source_metadata) ? _raw_get(spec, :source_metadata, nothing) : nothing
    library_label = _raw_haskey(spec, :library_label) ? String(_raw_get(spec, :library_label, "")) : nothing

    if _raw_haskey(spec, :networks)
        append!(network_specs, collect(_raw_get(spec, :networks, Any[])))
    end

    if _raw_haskey(spec, :enumeration)
        enum_spec = atlas_enumeration_spec_from_raw(_raw_get(spec, :enumeration, nothing))
        enumerated_networks, enumeration_summary = enumerate_network_specs(enum_spec;
            search_profile=search_profile,
        )
        append!(network_specs, enumerated_networks)
    end

    isempty(network_specs) && error("Atlas spec must include `networks` or `enumeration`.")

    atlas = build_behavior_atlas(network_specs;
        search_profile=search_profile,
        behavior_config=behavior_config,
        library=library,
        sqlite_path=sqlite_path,
        skip_existing=skip_existing,
    )
    enumeration_summary === nothing || (atlas["enumeration"] = enumeration_summary)

    if persist_sqlite && sqlite_path !== nothing
        if _is_empty_atlas_delta(atlas) && Int(_raw_get(atlas, :skipped_existing_slice_count, 0)) > 0
            atlas_sqlite_record_skip_only_event!(sqlite_path;
                source_label=source_label,
                source_metadata=source_metadata,
                skipped_existing_network_count=Int(_raw_get(atlas, :skipped_existing_network_count, 0)),
                skipped_existing_slice_count=Int(_raw_get(atlas, :skipped_existing_slice_count, 0)),
            )
        else
            atlas_sqlite_merge_atlas!(sqlite_path, atlas;
                source_label=source_label,
                source_metadata=source_metadata,
                library_label=isempty(library_label) ? nothing : library_label,
            )
        end
        atlas["sqlite_path"] = sqlite_path
        atlas["sqlite_persisted"] = true
        atlas["sqlite_library_summary"] = atlas_sqlite_summary(sqlite_path)
    end

    return atlas
end

function _atlas_summary(atlas)
    atlas === nothing && return nothing
    return Dict(
        "atlas_schema_version" => String(_raw_get(atlas, :atlas_schema_version, "unknown")),
        "generated_at" => String(_raw_get(atlas, :generated_at, "unknown")),
        "input_network_count" => Int(_raw_get(atlas, :input_network_count, 0)),
        "unique_network_count" => Int(_raw_get(atlas, :unique_network_count, length(collect(_raw_get(atlas, :network_entries, Any[]))))),
        "successful_network_count" => Int(_raw_get(atlas, :successful_network_count, 0)),
        "failed_network_count" => Int(_raw_get(atlas, :failed_network_count, 0)),
        "excluded_network_count" => Int(_raw_get(atlas, :excluded_network_count, 0)),
        "deduplicated_network_count" => Int(_raw_get(atlas, :deduplicated_network_count, length(collect(_raw_get(atlas, :duplicate_inputs, Any[]))))),
        "input_graph_slice_count" => length(collect(_raw_get(atlas, :input_graph_slices, Any[]))),
        "behavior_slice_count" => length(collect(_raw_get(atlas, :behavior_slices, Any[]))),
        "regime_record_count" => length(collect(_raw_get(atlas, :regime_records, Any[]))),
        "transition_record_count" => length(collect(_raw_get(atlas, :transition_records, Any[]))),
        "family_bucket_count" => length(collect(_raw_get(atlas, :family_buckets, Any[]))),
        "path_record_count" => length(collect(_raw_get(atlas, :path_records, Any[]))),
        "pruned_against_library" => Bool(_raw_get(atlas, :pruned_against_library, false)),
        "skipped_existing_network_count" => Int(_raw_get(atlas, :skipped_existing_network_count, 0)),
        "skipped_existing_slice_count" => Int(_raw_get(atlas, :skipped_existing_slice_count, 0)),
        "is_empty_delta" => _is_empty_atlas_delta(atlas),
    )
end

function _atlas_library_summary(library)
    library === nothing && return nothing
    return Dict(
        "atlas_library_schema_version" => String(_raw_get(library, :atlas_library_schema_version, "unknown")),
        "atlas_schema_version" => String(_raw_get(library, :atlas_schema_version, "unknown")),
        "created_at" => String(_raw_get(library, :created_at, "unknown")),
        "updated_at" => String(_raw_get(library, :updated_at, "unknown")),
        "atlas_count" => Int(_raw_get(library, :atlas_count, 0)),
        "input_network_count" => Int(_raw_get(library, :input_network_count, 0)),
        "unique_network_count" => Int(_raw_get(library, :unique_network_count, length(collect(_raw_get(library, :network_entries, Any[]))))),
        "successful_network_count" => Int(_raw_get(library, :successful_network_count, 0)),
        "failed_network_count" => Int(_raw_get(library, :failed_network_count, 0)),
        "excluded_network_count" => Int(_raw_get(library, :excluded_network_count, 0)),
        "deduplicated_network_count" => Int(_raw_get(library, :deduplicated_network_count, 0)),
        "input_graph_slice_count" => Int(_raw_get(library, :input_graph_slice_count, length(collect(_raw_get(library, :input_graph_slices, Any[]))))),
        "behavior_slice_count" => Int(_raw_get(library, :behavior_slice_count, length(collect(_raw_get(library, :behavior_slices, Any[]))))),
        "regime_record_count" => Int(_raw_get(library, :regime_record_count, length(collect(_raw_get(library, :regime_records, Any[]))))),
        "transition_record_count" => Int(_raw_get(library, :transition_record_count, length(collect(_raw_get(library, :transition_records, Any[]))))),
        "family_bucket_count" => Int(_raw_get(library, :family_bucket_count, length(collect(_raw_get(library, :family_buckets, Any[]))))),
        "path_record_count" => Int(_raw_get(library, :path_record_count, length(collect(_raw_get(library, :path_records, Any[]))))),
    )
end

function _copy_atlas_build_fields!(request::Dict{String, Any}, raw)
    for key in (:search_profile, :behavior_config, :networks, :enumeration, :sqlite_path)
        _raw_haskey(raw, key) || continue
        value = _raw_get(raw, key, nothing)
        value === nothing && continue
        request[String(key)] = value
    end
    return request
end

function _inverse_build_payload(spec, library, inverse::InverseDesignSpec; sqlite_path=nothing)
    if _raw_haskey(spec, :atlas)
        return Dict(
            "mode" => "precomputed_atlas",
            "atlas" => _raw_get(spec, :atlas, nothing),
        )
    elseif _raw_haskey(spec, :atlas_spec)
        atlas_spec = _raw_get(spec, :atlas_spec, nothing)
        if _is_atlas_corpus(atlas_spec)
            return Dict(
                "mode" => "precomputed_atlas",
                "atlas" => atlas_spec,
            )
        end

        request = Dict{String, Any}("skip_existing" => inverse.skip_existing)
        library === nothing || (request["library"] = library)
        sqlite_path === nothing || (request["sqlite_path"] = sqlite_path)
        _copy_atlas_build_fields!(request, atlas_spec)
        return Dict(
            "mode" => "atlas_spec",
            "build_spec" => request,
        )
    elseif _raw_haskey(spec, :networks) || _raw_haskey(spec, :enumeration)
        request = Dict{String, Any}("skip_existing" => inverse.skip_existing)
        library === nothing || (request["library"] = library)
        sqlite_path === nothing || (request["sqlite_path"] = sqlite_path)
        _copy_atlas_build_fields!(request, spec)
        return Dict(
            "mode" => "direct_build_fields",
            "build_spec" => request,
        )
    else
        return nothing
    end
end

_strip_singular_suffix(label::AbstractString) = replace(String(label), r"_with_singular_transition$" => "")

function _compress_sign_profile(tokens::Vector{String})
    isempty(tokens) && return String[]
    compressed = String[tokens[1]]
    for token in tokens[2:end]
        token == compressed[end] && continue
        push!(compressed, token)
    end
    return compressed
end

function _coarse_numeric_motif_label(motif_profile::Vector{String})
    isempty(motif_profile) && return "empty"

    if all(==("0"), motif_profile)
        return "flat"
    elseif all(==("+"), motif_profile)
        return length(motif_profile) == 1 ? "monotone_activation" : "multistage_activation"
    elseif all(==("-"), motif_profile)
        return length(motif_profile) == 1 ? "monotone_repression" : "multistage_repression"
    end

    nz = filter(!=("0"), motif_profile)
    isempty(nz) && return "flat"
    first_nz = findfirst(!=("0"), motif_profile)
    last_nz = findlast(!=("0"), motif_profile)
    sign_changes = length(nz) <= 1 ? 0 : count(i -> nz[i] != nz[i+1], 1:(length(nz)-1))

    if all(x -> x == "+" || x == "0", motif_profile)
        if first_nz > 1 && last_nz < length(motif_profile)
            return "band_pass_like"
        elseif first_nz > 1
            return "thresholded_activation"
        elseif last_nz < length(motif_profile)
            return "activation_with_saturation"
        else
            return "positive_motif"
        end
    elseif all(x -> x == "-" || x == "0", motif_profile)
        if first_nz > 1 && last_nz < length(motif_profile)
            return "window_repression"
        elseif first_nz > 1
            return "thresholded_repression"
        elseif last_nz < length(motif_profile)
            return "repression_with_floor"
        else
            return "negative_motif"
        end
    elseif sign_changes == 1 && first(nz) == "+" && last(nz) == "-"
        return "biphasic_peak"
    elseif sign_changes == 1 && first(nz) == "-" && last(nz) == "+"
        return "biphasic_valley"
    else
        return "complex_motif"
    end
end

function _scan_curve_motif(values::Vector{Float64}, refinement::InverseRefinementSpec)
    n = length(values)
    n <= 1 && return Dict(
        "motif_profile" => String["0"],
        "motif_label" => "flat",
        "token_tolerance" => refinement.flat_abs_tol,
    )

    diffs = diff(values)
    amplitude = isempty(values) ? 0.0 : maximum(values) - minimum(values)
    amplitude_per_step = isempty(diffs) ? amplitude : amplitude / length(diffs)
    token_tol = max(refinement.flat_abs_tol, refinement.flat_rel_tol * max(amplitude_per_step, 0.0))
    tokens = String[]
    for delta in diffs
        if delta > token_tol
            push!(tokens, "+")
        elseif delta < -token_tol
            push!(tokens, "-")
        else
            push!(tokens, "0")
        end
    end

    motif_profile = _compress_sign_profile(tokens)
    isempty(motif_profile) && (motif_profile = String["0"])
    return Dict(
        "motif_profile" => motif_profile,
        "motif_label" => _coarse_numeric_motif_label(motif_profile),
        "token_tolerance" => token_tol,
    )
end

function _target_motif_labels_for_result(result, query::AtlasQuerySpec)
    if !isempty(query.motif_labels)
        return _sorted_unique_strings(query.motif_labels)
    end

    buckets_key = haskey(result, "matched_motif_buckets") ? "matched_motif_buckets" :
                  haskey(result, "best_matched_motif_buckets") ? "best_matched_motif_buckets" :
                  nothing
    buckets_key === nothing && return String[]
    labels = String[]
    for bucket in collect(result[buckets_key])
        push!(labels, String(_raw_get(bucket, :family_label, "")))
    end
    return _sorted_unique_strings(labels)
end

function _scan_match_score(numeric_motif::AbstractString, target_motifs::Vector{String})
    isempty(target_motifs) && return 0.0
    numeric_base = _strip_singular_suffix(numeric_motif)
    target_bases = Set(_strip_singular_suffix(label) for label in target_motifs)
    return numeric_base in target_bases ? 1.0 : 0.0
end

function _candidate_io(result, result_unit::AbstractString)
    if result_unit == "network"
        return (
            slice_id=String(_raw_get(result, :best_slice_id, "")),
            input_symbol=String(_raw_get(result, :best_input_symbol, "")),
            output_symbol=String(_raw_get(result, :best_output_symbol, "")),
        )
    end
    return (
        slice_id=String(_raw_get(result, :slice_id, "")),
        input_symbol=String(_raw_get(result, :input_symbol, "")),
        output_symbol=String(_raw_get(result, :output_symbol, "")),
    )
end

function _candidate_refinement_trials(model, input_symbol::String, output_symbol::String, refinement::InverseRefinementSpec, query::AtlasQuerySpec, target_motifs::Vector{String})
    param_idx = locate_sym_qK(model, Symbol(input_symbol))
    param_idx === nothing && error("Unknown input symbol for refinement: $(input_symbol)")
    output_coeffs = parse_linear_combination(model, output_symbol)
    param_range = collect(range(refinement.param_min, refinement.param_max, length=max(refinement.n_points, 10)))
    qk_count = length(qK_sym(model))
    n_trials = max(refinement.trials, 1)
    rng = MersenneTwister(refinement.rng_seed)

    best_trial = nothing
    trial_summaries = Dict{String, Any}[]

    for trial_idx in 1:n_trials
        full_qK = zeros(Float64, qk_count)
        if trial_idx > 1
            for idx in 1:qk_count
                idx == param_idx && continue
                full_qK[idx] = refinement.background_min + rand(rng) * (refinement.background_max - refinement.background_min)
            end
        end

        fixed_params = deleteat!(copy(full_qK), param_idx)
        _, output_traj, regimes = scan_parameter_1d(
            model,
            param_idx,
            param_range,
            [output_coeffs],
            fixed_params;
            input_logspace=true,
            output_logspace=true,
        )
        values = vec(output_traj[:, 1])
        scan_motif = _scan_curve_motif(values, refinement)
        dynamic_range = isempty(values) ? 0.0 : maximum(values) - minimum(values)
        regime_transition_count = isempty(regimes) ? 0 : count(i -> regimes[i] != regimes[i+1], 1:(length(regimes)-1))
        motif_match = _scan_match_score(scan_motif["motif_label"], target_motifs)
        refinement_score = 100.0 * motif_match + min(dynamic_range, 20.0) + 0.1 * regime_transition_count

        trial_summary = Dict(
            "trial_idx" => trial_idx,
            "input_symbol" => input_symbol,
            "output_symbol" => output_symbol,
            "param_symbol" => input_symbol,
            "fixed_qK" => collect(full_qK),
            "numeric_motif_profile" => collect(scan_motif["motif_profile"]),
            "numeric_motif_label" => String(scan_motif["motif_label"]),
            "token_tolerance" => Float64(scan_motif["token_tolerance"]),
            "dynamic_range" => Float64(dynamic_range),
            "response_min" => isempty(values) ? nothing : Float64(minimum(values)),
            "response_max" => isempty(values) ? nothing : Float64(maximum(values)),
            "regime_transition_count" => regime_transition_count,
            "motif_match" => motif_match,
            "target_motif_labels" => collect(target_motifs),
            "refinement_score" => refinement_score,
        )
        if refinement.include_traces
            trial_summary["param_values"] = param_range
            trial_summary["output_trace"] = values
            trial_summary["regimes"] = regimes
        end

        push!(trial_summaries, trial_summary)
        if best_trial === nothing || Float64(trial_summary["refinement_score"]) > Float64(best_trial["refinement_score"])
            best_trial = trial_summary
        end
    end

    return best_trial, trial_summaries
end

function refine_inverse_design_candidates(query_result, refinement::InverseRefinementSpec, query::AtlasQuerySpec)
    results = collect(_raw_get(query_result, :results, Any[]))
    result_unit = String(_raw_get(query_result, :result_unit, "slice"))
    refinement.enabled || return Dict(
        "enabled" => false,
        "evaluated_count" => 0,
        "refined_unit" => result_unit,
        "results" => Dict{String, Any}[],
    )
    isempty(results) && return Dict(
        "enabled" => true,
        "evaluated_count" => 0,
        "refined_unit" => result_unit,
        "results" => Dict{String, Any}[],
    )

    refined_results = Dict{String, Any}[]
    max_candidates = refinement.top_k > 0 ? min(refinement.top_k, length(results)) : length(results)

    for result in results[1:max_candidates]
        io = _candidate_io(result, result_unit)
        rules = String.(_raw_get(result, :raw_rules, String[]))
        model, _, _, _ = build_model(rules, ones(Float64, length(rules)))
        target_motifs = _target_motif_labels_for_result(result, query)
        best_trial, _ = _candidate_refinement_trials(model, io.input_symbol, io.output_symbol, refinement, query, target_motifs)

        push!(refined_results, Dict(
            "network_id" => String(_raw_get(result, :network_id, "")),
            "slice_id" => io.slice_id,
            "source_rank" => Int(_raw_get(result, :rank, 0)),
            "result_unit" => result_unit,
            "input_symbol" => io.input_symbol,
            "output_symbol" => io.output_symbol,
            "raw_rules" => rules,
            "base_species_count" => _raw_get(result, :base_species_count, nothing),
            "reaction_count" => _raw_get(result, :reaction_count, nothing),
            "max_support" => _raw_get(result, :max_support, nothing),
            "support_mass" => _raw_get(result, :support_mass, nothing),
            "target_motif_labels" => target_motifs,
            "numeric_motif_label" => best_trial["numeric_motif_label"],
            "numeric_motif_profile" => best_trial["numeric_motif_profile"],
            "motif_match" => best_trial["motif_match"],
            "dynamic_range" => best_trial["dynamic_range"],
            "regime_transition_count" => best_trial["regime_transition_count"],
            "refinement_score" => best_trial["refinement_score"],
            "best_trial" => best_trial,
        ))
    end

    sort!(refined_results; by=result -> (-Float64(_raw_get(result, :refinement_score, 0.0)),
                                         Int(_raw_get(result, :base_species_count, typemax(Int))),
                                         Int(_raw_get(result, :reaction_count, typemax(Int))),
                                         Int(_raw_get(result, :max_support, typemax(Int))),
                                         Int(_raw_get(result, :support_mass, typemax(Int)))))
    for (rank, result) in enumerate(refined_results)
        result["refined_rank"] = rank
    end

    return Dict(
        "enabled" => true,
        "evaluated_count" => length(refined_results),
        "refined_unit" => result_unit,
        "reranked" => refinement.rerank_by_refinement,
        "results" => refined_results,
        "best_candidate" => isempty(refined_results) ? nothing : first(refined_results),
    )
end

function run_inverse_design(;
    library=nothing,
    atlas=nothing,
    atlas_spec=nothing,
    networks=nothing,
    enumeration=nothing,
    search_profile=nothing,
    behavior_config=nothing,
    query::AtlasQuerySpec=atlas_query_spec_default(),
    inverse::InverseDesignSpec=inverse_design_spec_default(),
    refinement::InverseRefinementSpec=inverse_refinement_spec_default(),
    source_label::Union{Nothing, AbstractString}=nothing,
    source_metadata=nothing,
    library_label::Union{Nothing, AbstractString}=nothing,
    allow_duplicate_atlas::Bool=false,
)
    request = Dict{String, Any}()
    library === nothing || (request["library"] = library)
    atlas === nothing || (request["atlas"] = atlas)
    atlas_spec === nothing || (request["atlas_spec"] = atlas_spec)
    networks === nothing || (request["networks"] = networks)
    enumeration === nothing || (request["enumeration"] = enumeration)
    search_profile === nothing || (request["search_profile"] = search_profile)
    behavior_config === nothing || (request["behavior_config"] = behavior_config)
    request["query"] = atlas_query_spec_to_dict(query)
    request["inverse_design"] = inverse_design_spec_to_dict(inverse)
    request["refinement"] = inverse_refinement_spec_to_dict(refinement)
    source_label === nothing || (request["source_label"] = String(source_label))
    source_metadata === nothing || (request["source_metadata"] = source_metadata)
    library_label === nothing || (request["library_label"] = String(library_label))
    allow_duplicate_atlas && (request["allow_duplicate_atlas"] = true)
    return run_inverse_design_from_spec(request)
end

function run_inverse_design_from_spec(spec)
    return run_inverse_design_pipeline_from_spec(spec)
end
