function _sync_query_value_node_count(value)
    stack = Any[value]
    count = 0
    while !isempty(stack)
        current = pop!(stack)
        count += 1
        count <= MAX_SYNC_ATLAS_QUERY_VALUE_NODES || return count
        if current isa AbstractDict || current isa JSON3.Object
            count += length(current) # object keys are structural nodes too
            count <= MAX_SYNC_ATLAS_QUERY_VALUE_NODES || return count
            for child in values(current)
                push!(stack, child)
            end
        elseif current isa AbstractVector || current isa Tuple
            count + length(current) <= MAX_SYNC_ATLAS_QUERY_VALUE_NODES ||
                return MAX_SYNC_ATLAS_QUERY_VALUE_NODES + 1
            append!(stack, current)
        end
    end
    return count
end

function _sync_validate_query_token(value, context::AbstractString)
    if value isa AbstractString
        ncodeunits(value) <= MAX_SYNC_ATLAS_QUERY_STRING_BYTES ||
            _sync_budget_exceeded("$context exceeds $(MAX_SYNC_ATLAS_QUERY_STRING_BYTES) bytes.")
    elseif value isa Real && !(value isa Bool)
        isfinite(value) || throw(ArgumentError("$context must be finite"))
    elseif value isa AbstractVector
        length(value) <= MAX_SYNC_ATLAS_QUERY_TOKEN_LENGTH ||
            _sync_budget_exceeded(
                "$context exceeds $(MAX_SYNC_ATLAS_QUERY_TOKEN_LENGTH) numeric components.")
        all(item -> item isa Real && !(item isa Bool) && isfinite(item), value) ||
            throw(ArgumentError("$context must contain only finite numbers"))
    else
        throw(ArgumentError("$context must be a string, finite number, or numeric array"))
    end
    return value
end

function _sync_validate_regime_predicate(predicate, context::AbstractString)
    for (key_any, value) in pairs(predicate)
        key = String(key_any)
        if key in (
            "singular", "asymptotic", "source", "sink", "branch", "merge",
            "reachable_from_source", "can_reach_sink",
        )
            value isa Bool || throw(ArgumentError("$context.$key must be a boolean"))
        elseif key == "nullity"
            sync_bounded_int(value, "$context.nullity"; min=0, max=MAX_SYNC_MODEL_N)
        elseif key == "role"
            value isa AbstractString ||
                throw(ArgumentError("$context.role must be a string"))
            ncodeunits(value) <= MAX_SYNC_ATLAS_QUERY_STRING_BYTES ||
                _sync_budget_exceeded("$context.role is too long.")
        elseif key == "output_order_token"
            _sync_validate_query_token(value, "$context.output_order_token")
        end
    end
    return predicate
end

function _sync_validate_transition_predicate(predicate, context::AbstractString)
    for (key_any, value) in pairs(predicate)
        key = String(key_any)
        if key == "from" || key == "to"
            (value isa AbstractDict || value isa JSON3.Object) ||
                throw(ArgumentError("$context.$key must be a predicate object"))
            _sync_validate_regime_predicate(value, "$context.$key")
        elseif key in ("transition_token", "from_role", "to_role")
            value isa AbstractString ||
                throw(ArgumentError("$context.$key must be a string"))
            ncodeunits(value) <= MAX_SYNC_ATLAS_QUERY_STRING_BYTES ||
                _sync_budget_exceeded("$context.$key is too long.")
        elseif key in ("from_output_order_token", "to_output_order_token")
            _sync_validate_query_token(value, "$context.$key")
        end
    end
    return predicate
end

function _sync_validate_support_count_spec(query_raw)
    goal_raw = _raw_get(query_raw, :goal, nothing)
    support_raw = if _raw_haskey(query_raw, :support_count_spec)
        _raw_get(query_raw, :support_count_spec, nothing)
    elseif (goal_raw isa AbstractDict || goal_raw isa JSON3.Object) &&
           _raw_haskey(goal_raw, :support_count_spec)
        _raw_get(goal_raw, :support_count_spec, nothing)
    else
        nothing
    end
    support_raw === nothing && return nothing
    (support_raw isa AbstractDict || support_raw isa JSON3.Object) ||
        throw(ArgumentError("query.support_count_spec must be an object"))

    normalized_counts = Dict{Symbol, Dict{String, Int}}()
    for field in (:min_counts, :max_counts)
        raw_counts = _raw_get(support_raw, field, Dict{String, Int}())
        (raw_counts isa AbstractDict || raw_counts isa JSON3.Object) ||
            throw(ArgumentError("query.support_count_spec.$(String(field)) must be an object"))
        length(raw_counts) <= MAX_SYNC_ATLAS_QUERY_ITEMS ||
            _sync_budget_exceeded(
                "query.support_count_spec.$(String(field)) exceeds $(MAX_SYNC_ATLAS_QUERY_ITEMS) species.")
        counts = Dict{String, Int}()
        for (species_any, raw_count) in pairs(raw_counts)
            species = String(species_any)
            isempty(species) &&
                throw(ArgumentError("support-count species names must not be empty"))
            ncodeunits(species) <= 128 ||
                _sync_budget_exceeded("A support-count species name is too long.")
            counts[species] = sync_bounded_int(
                raw_count,
                "query.support_count_spec.$(String(field)).$species";
                min=0,
                max=MAX_SYNC_MODEL_N,
            )
        end
        normalized_counts[field] = counts
    end
    for species in intersect(
        keys(normalized_counts[:min_counts]), keys(normalized_counts[:max_counts]))
        normalized_counts[:min_counts][species] <= normalized_counts[:max_counts][species] ||
            throw(ArgumentError(
                "support_count_spec min_counts[$species] exceeds max_counts[$species]"))
    end

    for field in (:required_species, :forbidden_species, :allowed_species)
        values = _raw_get(support_raw, field, String[])
        values isa AbstractVector ||
            throw(ArgumentError("query.support_count_spec.$(String(field)) must be an array"))
        length(values) <= MAX_SYNC_ATLAS_QUERY_ITEMS ||
            _sync_budget_exceeded(
                "query.support_count_spec.$(String(field)) exceeds $(MAX_SYNC_ATLAS_QUERY_ITEMS) species.")
        all(value -> value isa AbstractString && !isempty(value) && ncodeunits(value) <= 128,
            values) ||
            throw(ArgumentError(
                "query.support_count_spec.$(String(field)) must contain bounded nonempty strings"))
    end
    return support_raw
end

function _enforce_sync_query_budget(raw, referenced_slice_count::Integer=0)
    _raw_haskey(raw, :query) || return 1
    query_raw = _raw_get(raw, :query, nothing)
    (query_raw isa AbstractDict || query_raw isa JSON3.Object) ||
        throw(ArgumentError("query must be an object"))
    goal_raw = _raw_get(query_raw, :goal, nothing)
    if goal_raw !== nothing && !(goal_raw isa AbstractDict || goal_raw isa JSON3.Object)
        throw(ArgumentError("query.goal must be an object"))
    end
    limit_raw = if _raw_haskey(query_raw, :limit)
        _raw_get(query_raw, :limit, nothing)
    elseif goal_raw !== nothing && _raw_haskey(goal_raw, :limit)
        _raw_get(goal_raw, :limit, nothing)
    else
        atlas_query_spec_default().limit
    end
    (limit_raw isa Real && !(limit_raw isa Bool) && isfinite(limit_raw) && isinteger(limit_raw)) ||
        throw(ArgumentError("query.limit must be a finite integer"))
    limit_raw <= 0 &&
        _sync_budget_exceeded("Unbounded query.limit is not available synchronously.")
    sync_bounded_int(limit_raw, "query.limit";
        min=1, max=MAX_SYNC_ATLAS_QUERY_RESULTS)

    query_bytes = ncodeunits(JSON3.write(query_raw))
    query_bytes <= MAX_SYNC_ATLAS_QUERY_BYTES ||
        _sync_budget_exceeded(
            "Query specification exceeds $(MAX_SYNC_ATLAS_QUERY_BYTES) bytes.")
    raw_node_count = _sync_query_value_node_count(query_raw)
    raw_node_count <= MAX_SYNC_ATLAS_QUERY_VALUE_NODES ||
        _sync_budget_exceeded(
            "Query specification exceeds $(MAX_SYNC_ATLAS_QUERY_VALUE_NODES) structural value nodes.")
    _sync_validate_support_count_spec(query_raw)
    enforce_sync_cost(
        max(1, Int(referenced_slice_count)) * query_bytes,
        MAX_SYNC_ATLAS_QUERY_BYTES_COST,
        "Corpus slices * query bytes",
    )

    query = try
        _validate_raw_query_scope(query_raw)
        atlas_query_spec_from_raw(query_raw)
    catch err
        throw(ArgumentError("Invalid query specification: $(sprint(showerror, err))"))
    end
    (_query_requires_witness(query) || query.require_robust ||
     query.min_robust_path_count > 0) &&
        _sync_budget_exceeded(
            "Witness/robustness queries that may trigger lazy materialization are asynchronous-only.")
    list_counts = (
        length(query.motif_labels),
        length(query.exact_labels),
        length(query.input_symbols),
        length(query.change_signatures),
        length(query.output_symbols),
    )
    all(count -> count <= MAX_SYNC_ATLAS_QUERY_ITEMS, list_counts) ||
        _sync_budget_exceeded(
            "A query label/symbol list exceeds $(MAX_SYNC_ATLAS_QUERY_ITEMS) items.")
    list_item_count = sum(list_counts)
    list_item_count <= MAX_SYNC_ATLAS_QUERY_ITEMS ||
        _sync_budget_exceeded(
            "Combined query label/symbol count exceeds $(MAX_SYNC_ATLAS_QUERY_ITEMS).")

    length(query.required_path_sequences) <= MAX_SYNC_ATLAS_QUERY_PREDICATES ||
        _sync_budget_exceeded(
            "Query witness sequence count exceeds $(MAX_SYNC_ATLAS_QUERY_PREDICATES).")
    all(sequence -> length(sequence) <= MAX_SYNC_ATLAS_QUERY_SEQUENCE_LENGTH,
        query.required_path_sequences) ||
        _sync_budget_exceeded(
            "A query witness sequence exceeds $(MAX_SYNC_ATLAS_QUERY_SEQUENCE_LENGTH) predicates.")
    predicate_count = length(query.required_regimes) +
                      length(query.forbidden_regimes) +
                      length(query.required_transitions) +
                      length(query.forbidden_transitions) +
                      sum(length, query.required_path_sequences; init=0)
    predicate_count <= MAX_SYNC_ATLAS_QUERY_PREDICATES ||
        _sync_budget_exceeded(
            "Combined query predicate count exceeds $(MAX_SYNC_ATLAS_QUERY_PREDICATES).")
    for (idx, predicate) in enumerate(query.required_regimes)
        _sync_validate_regime_predicate(predicate, "query.required_regimes[$idx]")
    end
    for (idx, predicate) in enumerate(query.forbidden_regimes)
        _sync_validate_regime_predicate(predicate, "query.forbidden_regimes[$idx]")
    end
    for (idx, predicate) in enumerate(query.required_transitions)
        _sync_validate_transition_predicate(predicate, "query.required_transitions[$idx]")
    end
    for (idx, predicate) in enumerate(query.forbidden_transitions)
        _sync_validate_transition_predicate(predicate, "query.forbidden_transitions[$idx]")
    end
    for (sequence_idx, sequence) in enumerate(query.required_path_sequences)
        for (predicate_idx, predicate) in enumerate(sequence)
            _sync_validate_regime_predicate(
                predicate,
                "query.required_path_sequences[$sequence_idx][$predicate_idx]",
            )
        end
    end

    complexity = max(raw_node_count, 1 + list_item_count + predicate_count)
    complexity <= MAX_SYNC_ATLAS_QUERY_COMPLEXITY ||
        _sync_budget_exceeded("Query structural complexity is too large for synchronous execution.")
    enforce_sync_cost(
        max(1, Int(referenced_slice_count)) * complexity,
        MAX_SYNC_ATLAS_QUERY_COST,
        "Corpus slices * query complexity",
    )
    return complexity
end

function _enforce_sync_inverse_budget(raw)
    inverse_raw = _raw_haskey(raw, :inverse_design) ?
        _raw_get(raw, :inverse_design, nothing) : raw
    (inverse_raw isa AbstractDict || inverse_raw isa JSON3.Object) ||
        throw(ArgumentError("inverse_design must be an object"))
    source_label = _raw_get(
        inverse_raw, :source_label, inverse_design_spec_default().source_label)
    source_label isa AbstractString ||
        throw(ArgumentError("inverse_design.source_label must be a string"))
    inverse_defaults = inverse_design_spec_default()
    for (field, default) in (
        (:skip_existing, inverse_defaults.skip_existing),
        (:build_library_if_missing, inverse_defaults.build_library_if_missing),
        (:return_library, inverse_defaults.return_library),
        (:return_delta_atlas, inverse_defaults.return_delta_atlas),
    )
        _raw_get(inverse_raw, field, default) isa Bool ||
            throw(ArgumentError("inverse_design.$(String(field)) must be a boolean"))
    end
    return_library = _raw_get(
        inverse_raw, :return_library, inverse_defaults.return_library)
    return_library &&
        _sync_budget_exceeded(
            "Synchronous inverse design requires inverse_design.return_library=false.")

    refinement_raw = _raw_get(raw, :refinement, nothing)
    refinement_raw === nothing && return raw
    (refinement_raw isa AbstractDict || refinement_raw isa JSON3.Object) ||
        throw(ArgumentError("refinement must be an object"))
    defaults = inverse_refinement_spec_default()
    enabled = _raw_get(refinement_raw, :enabled, defaults.enabled)
    enabled isa Bool || throw(ArgumentError("refinement.enabled must be a boolean"))

    for (field, default) in (
        (:include_traces, defaults.include_traces),
        (:rerank_by_refinement, defaults.rerank_by_refinement),
    )
        _raw_get(refinement_raw, field, default) isa Bool ||
            throw(ArgumentError("refinement.$(String(field)) must be a boolean"))
    end

    top_k_raw = _raw_get(refinement_raw, :top_k, defaults.top_k)
    (top_k_raw isa Real && !(top_k_raw isa Bool) && isfinite(top_k_raw) && isinteger(top_k_raw)) ||
        throw(ArgumentError("refinement.top_k must be a finite integer"))
    top_k_raw <= 0 &&
        _sync_budget_exceeded("Unbounded refinement.top_k is not available synchronously.")
    top_k = sync_bounded_int(top_k_raw, "refinement.top_k";
        min=1, max=MAX_SYNC_INVERSE_TOP_K)
    trials = sync_bounded_int(_raw_get(refinement_raw, :trials, defaults.trials),
        "refinement.trials";
        min=1, max=MAX_SYNC_INVERSE_TRIALS)
    n_points = sync_bounded_int(_raw_get(refinement_raw, :n_points, defaults.n_points),
        "refinement.n_points";
        min=10, max=MAX_SYNC_INVERSE_POINTS)
    sync_finite_range(
        _raw_get(refinement_raw, :param_min, defaults.param_min),
        _raw_get(refinement_raw, :param_max, defaults.param_max),
        "refinement.param";
        abs_max=20.0,
    )
    sync_finite_range(
        _raw_get(refinement_raw, :background_min, defaults.background_min),
        _raw_get(refinement_raw, :background_max, defaults.background_max),
        "refinement.background";
        abs_max=20.0,
    )
    for (field, default) in (
        (:flat_abs_tol, defaults.flat_abs_tol),
        (:flat_rel_tol, defaults.flat_rel_tol),
    )
        tolerance = sync_finite_float(
            _raw_get(refinement_raw, field, default),
            "refinement.$(String(field))";
            abs_max=1.0,
        )
        tolerance >= 0 ||
            throw(ArgumentError("refinement.$(String(field)) must be nonnegative"))
    end
    sync_bounded_int(_raw_get(refinement_raw, :rng_seed, defaults.rng_seed),
        "refinement.rng_seed"; min=0, max=Int(typemax(Int32)))

    enabled || return raw
    _sync_budget_exceeded(
        "Enabled inverse refinement is asynchronous-only because it performs model solves and local search.")
    return raw
end

function _enforce_sync_sqlite_service_policy(raw, handler_name::Symbol)
    sqlite_path = _sqlite_path_from_raw(raw)
    if sqlite_path !== nothing
        if handler_name == :handle_query_atlas
            isfile(sqlite_path) ||
                throw(ArgumentError(
                    "Synchronous atlas query sqlite_path must reference an existing database"))
        elseif handler_name in (
            :handle_build_atlas,
            :handle_build_atlas_library,
            :handle_merge_atlas_library,
            :handle_run_inverse_design,
        )
            _sync_budget_exceeded(
                "SQLite-backed atlas build, merge, inverse-design, or persistence is asynchronous-only.")
        end
    end

    if _raw_haskey(raw, :atlas_spec)
        atlas_spec = _raw_get(raw, :atlas_spec, nothing)
        if (atlas_spec isa AbstractDict || atlas_spec isa JSON3.Object) &&
           !_sync_is_atlas_corpus(atlas_spec) &&
           _sqlite_path_from_raw(atlas_spec) !== nothing
            _sync_budget_exceeded(
                "Nested atlas_spec sqlite_path is not available on synchronous service endpoints.")
        end
    end
    return raw
end

function enforce_sync_atlas_request_budget(raw, handler_name::Symbol=:unspecified)
    (raw isa AbstractDict || raw isa JSON3.Object) ||
        throw(ArgumentError("service request must be an object"))
    _enforce_sync_sqlite_service_policy(raw, handler_name)
    _enforce_sync_atlas_build_shape(raw)
    if !_sync_is_atlas_corpus(raw) && _raw_haskey(raw, :atlas_spec)
        _enforce_sync_atlas_build_shape(_raw_get(raw, :atlas_spec, nothing))
    end

    if handler_name in (
        :handle_build_atlas,
        :handle_query_atlas,
        :handle_build_atlas_library,
        :handle_merge_atlas_library,
        :handle_run_inverse_design,
    )
        referenced_slice_count = _enforce_sync_referenced_corpus_budget(raw, handler_name)
    else
        referenced_slice_count = 0
    end
    if handler_name in (:handle_query_atlas, :handle_run_inverse_design)
        _enforce_sync_query_budget(raw, referenced_slice_count)
    end
    handler_name == :handle_run_inverse_design && _enforce_sync_inverse_budget(raw)
    return raw
end
