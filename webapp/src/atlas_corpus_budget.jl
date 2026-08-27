const _SYNC_ATLAS_CORPUS_RECORD_FIELDS = (
    :network_entries,
    :input_graph_slices,
    :behavior_slices,
    :regime_records,
    :transition_records,
    :family_buckets,
    :path_records,
)

const _SYNC_ATLAS_RECORD_STRING_FIELDS = Set((
    "source_label", "source_kind", "analysis_status", "build_state",
    "input_symbol", "output_symbol", "change_signature", "change_kind",
    "family_kind", "family_label", "parent_motif", "role",
    "output_order_token", "transition_token", "from_role", "to_role",
    "from_output_order_token", "to_output_order_token", "motif_label",
    "exact_label", "behavior_code",
))

const _SYNC_ATLAS_RECORD_BOOL_FIELDS = Set((
    "singular", "asymptotic", "is_source", "is_sink", "is_branch", "is_merge",
    "reachable_from_source", "can_reach_sink", "feasible", "robust", "included",
))

function _sync_validate_atlas_record(record, context::AbstractString)
    (record isa AbstractDict || record isa JSON3.Object) ||
        throw(ArgumentError("$context must be an object"))
    for (key_any, value) in pairs(record)
        value === nothing && continue
        key = String(key_any)
        if endswith(key, "_id") || key in _SYNC_ATLAS_RECORD_STRING_FIELDS
            value isa AbstractString ||
                throw(ArgumentError("$context.$key must be a string"))
            ncodeunits(value) <= MAX_SYNC_EXPRESSION_BYTES ||
                _sync_budget_exceeded("$context.$key is too long.")
        elseif endswith(key, "_count") ||
               key in ("nullity", "path_length", "rank", "source_rank")
            sync_bounded_int(value, "$context.$key"; min=0, max=typemax(Int))
        elseif endswith(key, "_idx")
            sync_bounded_int(value, "$context.$key"; min=-1, max=typemax(Int))
        elseif key in _SYNC_ATLAS_RECORD_BOOL_FIELDS
            value isa Bool || throw(ArgumentError("$context.$key must be a boolean"))
        end
    end
    return record
end

function _sync_validate_corpus_network_entry(entry, context::AbstractString)
    (entry isa AbstractDict || entry isa JSON3.Object) ||
        throw(ArgumentError("$context must be an object"))
    if !_raw_haskey(entry, :raw_rules)
        status = _raw_get(entry, :analysis_status, "")
        status isa AbstractString ||
            throw(ArgumentError("$context.analysis_status must be a string"))
        String(status) == "ok" &&
            throw(ArgumentError(
                "$context with analysis_status=ok must include raw_rules"))
        return entry
    end
    raw_rules = _raw_get(entry, :raw_rules, nothing)
    raw_rules isa AbstractVector ||
        throw(ArgumentError("$context.raw_rules must be an array"))
    all(rule -> rule isa AbstractString, raw_rules) ||
        throw(ArgumentError("$context.raw_rules must contain only strings"))
    enforce_sync_rule_budget(raw_rules)
    validation = validate_rules_against_profile(
        String.(raw_rules), atlas_search_profile_binding_small_v0())
    Bool(_raw_get(validation, :valid, false)) ||
        _sync_budget_exceeded(
            "$context is outside binding_small_v0: " *
            join(String.(_raw_get(validation, :issues, String[])), ", ") * ".")
    return entry
end

function _enforce_sync_corpus_slice_budget(corpus, label::AbstractString)
    _sync_is_atlas_corpus(corpus) || return corpus
    slices = _raw_get(corpus, :behavior_slices, Any[])
    slices isa AbstractVector ||
        throw(ArgumentError("$label behavior_slices must be an array"))
    length(slices) <= MAX_SYNC_ATLAS_QUERY_SLICES ||
        _sync_budget_exceeded(
            "$label exceeds the synchronous corpus limit of $(MAX_SYNC_ATLAS_QUERY_SLICES) behavior slices.")
    network_entries = _raw_get(corpus, :network_entries, Any[])
    network_entries isa AbstractVector ||
        throw(ArgumentError("$label network_entries must be an array"))
    length(network_entries) <= MAX_SYNC_ATLAS_QUERY_NETWORKS ||
        _sync_budget_exceeded(
            "$label exceeds the synchronous limit of $(MAX_SYNC_ATLAS_QUERY_NETWORKS) networks.")
    for (idx, entry) in enumerate(network_entries)
        _sync_validate_corpus_network_entry(
            entry, "$label network_entries[$idx]")
    end
    total_records = 0
    for field in _SYNC_ATLAS_CORPUS_RECORD_FIELDS
        records = _raw_get(corpus, field, Any[])
        records isa AbstractVector ||
            throw(ArgumentError("$label $(String(field)) must be an array"))
        total_records <= MAX_SYNC_ATLAS_QUERY_RECORDS - length(records) ||
            _sync_budget_exceeded(
                "$label exceeds the synchronous corpus limit of $(MAX_SYNC_ATLAS_QUERY_RECORDS) indexed records.")
        for (idx, record) in enumerate(records)
            _sync_validate_atlas_record(
                record, "$label.$(String(field))[$idx]")
        end
        total_records += length(records)
    end
    return length(slices)
end

function _sync_sqlite_uri_path(path::AbstractString)
    encoded = replace(String(path), "%" => "%25", "?" => "%3F", "#" => "%23")
    return "file:$(encoded)?mode=ro"
end

function _sync_sqlite_table_exists(db::SQLite.DB, table::AbstractString)
    query = DBInterface.execute(
        db,
        "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (String(table),),
    )
    try
        for _ in query
            return true
        end
    finally
        DBInterface.close!(query)
    end
    return false
end

function _sync_sqlite_table_count(db::SQLite.DB, table::AbstractString)
    _sync_sqlite_table_exists(db, table) || return 0
    query = DBInterface.execute(db, "SELECT COUNT(*) AS record_count FROM $(String(table))")
    try
        for row in query
            return Int(row[:record_count])
        end
    finally
        DBInterface.close!(query)
    end
    return 0
end

function _sync_sqlite_readonly_counts(path::AbstractString)
    total_bytes = filesize(path)
    wal_path = String(path) * "-wal"
    isfile(wal_path) && (total_bytes += filesize(wal_path))
    total_bytes <= MAX_SYNC_ATLAS_SQLITE_BYTES ||
        _sync_budget_exceeded(
            "SQLite corpus exceeds the synchronous file-size limit of $(MAX_SYNC_ATLAS_SQLITE_BYTES) bytes.")

    db = try
        SQLite.DB(_sync_sqlite_uri_path(path))
    catch err
        _sync_budget_exceeded(
            "SQLite corpus could not be opened read-only: $(sprint(showerror, err)).")
    end
    try
        _sync_sqlite_table_exists(db, "atlas_metadata") ||
            _sync_budget_exceeded(
                "SQLite corpus schema is unknown; synchronous preflight will not initialize or migrate it.")
        _sync_sqlite_table_exists(db, "behavior_slices") ||
            _sync_budget_exceeded(
                "SQLite corpus lacks behavior_slices; synchronous preflight will not initialize or migrate it.")
        tables = (
            "network_entries",
            "input_graph_slices",
            "behavior_slices",
            "regime_records",
            "transition_records",
            "family_buckets",
            "path_records",
            "path_only_records",
        )
        counts = Dict(table => _sync_sqlite_table_count(db, table) for table in tables)
        get(counts, "network_entries", 0) <= MAX_SYNC_ATLAS_QUERY_NETWORKS ||
            _sync_budget_exceeded(
                "SQLite corpus exceeds the synchronous network limit of $(MAX_SYNC_ATLAS_QUERY_NETWORKS).")
        if get(counts, "network_entries", 0) > 0
            query = try
                DBInterface.execute(
                    db,
                    "SELECT record_json FROM network_entries ORDER BY network_id",
                )
            catch err
                _sync_budget_exceeded(
                    "SQLite network_entries cannot be validated read-only: $(sprint(showerror, err)).")
            end
            try
                for (idx, row) in enumerate(query)
                    entry = try
                        _materialize(JSON3.read(String(row[:record_json])))
                    catch err
                        throw(ArgumentError(
                            "sqlite network_entries[$idx].record_json is invalid: $(sprint(showerror, err))"))
                    end
                    _sync_validate_corpus_network_entry(
                        entry, "sqlite network_entries[$idx]")
                end
            finally
                DBInterface.close!(query)
            end
        end
        return counts
    finally
        SQLite.close(db)
    end
end

function _enforce_sync_referenced_corpus_budget(raw, handler_name::Symbol)
    max_slice_count = 0
    _sync_is_atlas_corpus(raw) &&
        (max_slice_count = max(max_slice_count,
            _enforce_sync_corpus_slice_budget(raw, "Atlas corpus")))
    for key in (:atlas, :library, :atlas_spec)
        _raw_haskey(raw, key) || continue
        corpus = _raw_get(raw, key, nothing)
        _sync_is_atlas_corpus(corpus) || continue
        max_slice_count = max(max_slice_count,
            _enforce_sync_corpus_slice_budget(corpus, "$(String(key)) corpus"))
    end

    sqlite_path = _sqlite_path_from_raw(raw)
    if sqlite_path !== nothing && isfile(sqlite_path)
        counts = _sync_sqlite_readonly_counts(sqlite_path)
        slice_count = sync_bounded_int(
            get(counts, "behavior_slices", 0),
            "sqlite behavior_slice_count";
            max=MAX_SYNC_ATLAS_QUERY_SLICES,
        )
        max_slice_count = max(max_slice_count, slice_count)
        sync_bounded_int(get(counts, "network_entries", 0),
            "sqlite network count"; max=MAX_SYNC_ATLAS_QUERY_NETWORKS)
        total_records = 0
        for (table, raw_count) in counts
            count = sync_bounded_int(raw_count,
                "sqlite $(table) count"; max=MAX_SYNC_ATLAS_QUERY_RECORDS)
            total_records <= MAX_SYNC_ATLAS_QUERY_RECORDS - count ||
                _sync_budget_exceeded(
                    "SQLite corpus exceeds the synchronous limit of $(MAX_SYNC_ATLAS_QUERY_RECORDS) indexed records.")
            total_records += count
        end
    end
    return max_slice_count
end
