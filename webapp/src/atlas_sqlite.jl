const ATLAS_SQLITE_SCHEMA_VERSION = "0.1.0"
const ATLAS_SQLITE_SELECT_BATCH_SIZE = 400

atlas_sqlite_default_path() = normpath(joinpath(@__DIR__, "..", "atlas_store", "atlas.sqlite"))

function _sqlite_path_from_raw(raw)
    _raw_haskey(raw, :sqlite_path) || return nothing
    value = _raw_get(raw, :sqlite_path, nothing)
    value === nothing && return nothing
    path = strip(String(value))
    isempty(path) && return nothing
    return abspath(expanduser(path))
end

function _atlas_sqlite_json(value)
    return JSON3.write(_atlas_sqlite_sanitize(value))
end

function _atlas_sqlite_read_json(value)
    value === nothing && return nothing
    return _materialize(JSON3.read(String(value)))
end

function _atlas_sqlite_sanitize(value)
    if value isa AbstractDict
        out = Dict{String, Any}()
        for (k, v) in pairs(value)
            out[String(k)] = _atlas_sqlite_sanitize(v)
        end
        return out
    elseif value isa AbstractVector || value isa Tuple
        return Any[_atlas_sqlite_sanitize(v) for v in value]
    elseif value isa AbstractFloat
        if isnan(value)
            return "NaN"
        elseif isinf(value)
            return signbit(value) ? "-Inf" : "+Inf"
        end
        return value
    elseif value isa Symbol
        return String(value)
    else
        return value
    end
end

function _atlas_sqlite_text(value)
    value === nothing && return nothing
    str = String(value)
    isempty(str) && return nothing
    return str
end

function _atlas_sqlite_int(value)
    value === nothing && return nothing
    return Int(value)
end

function _atlas_sqlite_float(value)
    value === nothing && return nothing
    return Float64(value)
end

function _atlas_sqlite_bool(value)
    value === nothing && return nothing
    return Bool(value) ? 1 : 0
end

function _atlas_sqlite_volume_mean(value)
    value === nothing && return nothing
    if value isa AbstractDict
        return _raw_haskey(value, :mean) ? _atlas_sqlite_float(_raw_get(value, :mean, nothing)) : nothing
    end
    return nothing
end

function _atlas_sqlite_execute(db::SQLite.DB, sql::AbstractString, params=())
    query = DBInterface.execute(db, sql, params)
    stripped = uppercase(strip(sql))
    if startswith(stripped, "SELECT")
        return query
    end
    DBInterface.close!(query)
    return nothing
end

function _atlas_sqlite_transaction(f::Function, db::SQLite.DB)
    _atlas_sqlite_execute(db, "BEGIN IMMEDIATE TRANSACTION")
    try
        result = f()
        _atlas_sqlite_execute(db, "COMMIT")
        return result
    catch err
        try
            _atlas_sqlite_execute(db, "ROLLBACK")
        catch
        end
        rethrow(err)
    end
end

_atlas_sqlite_transaction(db::SQLite.DB, f::Function) = _atlas_sqlite_transaction(f, db)

function atlas_sqlite_init!(db::SQLite.DB)
    statements = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "CREATE TABLE IF NOT EXISTS atlas_metadata (key TEXT PRIMARY KEY, value_text TEXT NOT NULL)",
        """
        CREATE TABLE IF NOT EXISTS library_state (
            snapshot_name TEXT PRIMARY KEY,
            updated_at TEXT NOT NULL,
            summary_json TEXT NOT NULL,
            library_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS atlas_manifests (
            atlas_id TEXT PRIMARY KEY,
            source_label TEXT,
            imported_at TEXT,
            generated_at TEXT,
            behavior_slice_count INTEGER,
            manifest_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS merge_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            merged_at TEXT,
            status TEXT,
            atlas_id TEXT,
            source_label TEXT,
            event_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS network_entries (
            network_id TEXT PRIMARY KEY,
            canonical_code TEXT,
            analysis_status TEXT,
            base_species_count INTEGER,
            reaction_count INTEGER,
            total_species_count INTEGER,
            max_support INTEGER,
            support_mass INTEGER,
            source_label TEXT,
            source_kind TEXT,
            motif_union_json TEXT,
            exact_union_json TEXT,
            slice_ids_json TEXT,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS input_graph_slices (
            graph_slice_id TEXT PRIMARY KEY,
            network_id TEXT,
            input_symbol TEXT,
            vertex_count INTEGER,
            edge_count INTEGER,
            path_count INTEGER,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS behavior_slices (
            slice_id TEXT PRIMARY KEY,
            network_id TEXT,
            graph_slice_id TEXT,
            input_symbol TEXT,
            output_symbol TEXT,
            analysis_status TEXT,
            path_scope TEXT,
            min_volume_mean REAL,
            total_paths INTEGER,
            feasible_paths INTEGER,
            included_paths INTEGER,
            excluded_paths INTEGER,
            motif_union_json TEXT,
            exact_union_json TEXT,
            classifier_config_json TEXT,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS regime_records (
            regime_record_id TEXT PRIMARY KEY,
            slice_id TEXT,
            graph_slice_id TEXT,
            network_id TEXT,
            input_symbol TEXT,
            output_symbol TEXT,
            vertex_idx INTEGER,
            role TEXT,
            singular INTEGER,
            nullity INTEGER,
            asymptotic INTEGER,
            output_order_token TEXT,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS transition_records (
            transition_record_id TEXT PRIMARY KEY,
            slice_id TEXT,
            graph_slice_id TEXT,
            input_symbol TEXT,
            output_symbol TEXT,
            from_vertex_idx INTEGER,
            to_vertex_idx INTEGER,
            from_role TEXT,
            to_role TEXT,
            from_output_order_token TEXT,
            to_output_order_token TEXT,
            transition_token TEXT,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS family_buckets (
            bucket_id TEXT PRIMARY KEY,
            slice_id TEXT,
            graph_slice_id TEXT,
            network_id TEXT,
            family_kind TEXT,
            family_label TEXT,
            parent_motif TEXT,
            path_count INTEGER,
            robust_path_count INTEGER,
            volume_mean REAL,
            representative_path_idx INTEGER,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS path_records (
            path_record_id TEXT PRIMARY KEY,
            slice_id TEXT,
            graph_slice_id TEXT,
            network_id TEXT,
            input_symbol TEXT,
            output_symbol TEXT,
            path_idx INTEGER,
            path_length INTEGER,
            exact_label TEXT,
            motif_label TEXT,
            feasible INTEGER,
            robust INTEGER,
            volume_mean REAL,
            output_order_tokens_json TEXT,
            transition_tokens_json TEXT,
            record_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS duplicate_inputs (
            duplicate_key TEXT PRIMARY KEY,
            source_label TEXT,
            duplicate_of_network_id TEXT,
            record_json TEXT NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_network_status ON network_entries (analysis_status)",
        "CREATE INDEX IF NOT EXISTS idx_slice_network ON behavior_slices (network_id)",
        "CREATE INDEX IF NOT EXISTS idx_slice_io ON behavior_slices (input_symbol, output_symbol)",
        "CREATE INDEX IF NOT EXISTS idx_slice_status ON behavior_slices (analysis_status)",
        "CREATE INDEX IF NOT EXISTS idx_regime_slice ON regime_records (slice_id)",
        "CREATE INDEX IF NOT EXISTS idx_regime_token ON regime_records (output_order_token, role, singular)",
        "CREATE INDEX IF NOT EXISTS idx_transition_slice ON transition_records (slice_id)",
        "CREATE INDEX IF NOT EXISTS idx_transition_token ON transition_records (transition_token)",
        "CREATE INDEX IF NOT EXISTS idx_bucket_slice ON family_buckets (slice_id)",
        "CREATE INDEX IF NOT EXISTS idx_bucket_family ON family_buckets (family_kind, family_label)",
        "CREATE INDEX IF NOT EXISTS idx_path_slice ON path_records (slice_id)",
        "CREATE INDEX IF NOT EXISTS idx_path_labels ON path_records (motif_label, exact_label)",
    ]

    for statement in statements
        _atlas_sqlite_execute(db, statement)
    end

    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("schema_version", ATLAS_SQLITE_SCHEMA_VERSION),
    )
    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("updated_at", _now_iso_timestamp()),
    )
    return db
end

function atlas_sqlite_connect(db_path::AbstractString=atlas_sqlite_default_path(); init::Bool=true)
    path = abspath(expanduser(db_path))
    mkpath(dirname(path))
    db = SQLite.DB(path)
    init && atlas_sqlite_init!(db)
    return db
end

function _atlas_sqlite_with_db(f::Function, db::SQLite.DB)
    atlas_sqlite_init!(db)
    return f(db)
end

function _atlas_sqlite_with_db(f::Function, db_path::AbstractString)
    db = atlas_sqlite_connect(db_path)
    try
        return f(db)
    finally
        SQLite.close(db)
    end
end

function atlas_sqlite_has_library(db::SQLite.DB)
    for _ in _atlas_sqlite_execute(db, "SELECT 1 FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
        return true
    end
    return false
end

atlas_sqlite_has_library(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_has_library, db_path)

function atlas_sqlite_load_library(db::SQLite.DB)
    for row in _atlas_sqlite_execute(db, "SELECT library_json FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
        return _atlas_sqlite_read_json(row[:library_json])
    end
    return atlas_library_default()
end

atlas_sqlite_load_library(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_load_library, db_path)

function atlas_sqlite_summary(db::SQLite.DB)
    for row in _atlas_sqlite_execute(db, "SELECT summary_json FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
        summary = _atlas_sqlite_read_json(row[:summary_json])
        summary["sqlite_schema_version"] = ATLAS_SQLITE_SCHEMA_VERSION
        return summary
    end
    return Dict(
        "sqlite_schema_version" => ATLAS_SQLITE_SCHEMA_VERSION,
        "atlas_count" => 0,
        "unique_network_count" => 0,
        "behavior_slice_count" => 0,
        "family_bucket_count" => 0,
        "path_record_count" => 0,
    )
end

atlas_sqlite_summary(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_summary, db_path)

function _atlas_sqlite_placeholder_list(count::Integer)
    count > 0 || error("_atlas_sqlite_placeholder_list expects a positive count.")
    return join(fill("?", count), ", ")
end

function _atlas_sqlite_load_json_records(
    db::SQLite.DB,
    table::AbstractString,
    id_column::AbstractString,
    ids;
    order_column::AbstractString=id_column,
    batch_size::Int=ATLAS_SQLITE_SELECT_BATCH_SIZE,
)
    unique_ids = _sorted_unique_strings(ids)
    isempty(unique_ids) && return Dict{String, Any}[]

    records = Dict{String, Any}[]
    for start_idx in 1:batch_size:length(unique_ids)
        stop_idx = min(start_idx + batch_size - 1, length(unique_ids))
        batch = unique_ids[start_idx:stop_idx]
        sql = "SELECT record_json FROM $(table) WHERE $(id_column) IN (" *
              _atlas_sqlite_placeholder_list(length(batch)) * ") ORDER BY $(order_column)"
        for row in _atlas_sqlite_execute(db, sql, Tuple(batch))
            push!(records, Dict{String, Any}(_atlas_sqlite_read_json(row[:record_json])))
        end
    end
    return records
end

function _atlas_sqlite_query_slice_refs(db::SQLite.DB, query::AtlasQuerySpec)
    clauses = [
        "s.analysis_status = 'ok'",
        "n.analysis_status = 'ok'",
        "EXISTS (SELECT 1 FROM regime_records AS rr WHERE rr.slice_id = s.slice_id)",
        "EXISTS (SELECT 1 FROM family_buckets AS fb WHERE fb.slice_id = s.slice_id)",
    ]
    params = Any[]

    if !isempty(query.input_symbols)
        push!(clauses, "s.input_symbol IN (" * _atlas_sqlite_placeholder_list(length(query.input_symbols)) * ")")
        append!(params, query.input_symbols)
    end
    if !isempty(query.output_symbols)
        push!(clauses, "s.output_symbol IN (" * _atlas_sqlite_placeholder_list(length(query.output_symbols)) * ")")
        append!(params, query.output_symbols)
    end

    sql = """
    SELECT s.slice_id, s.graph_slice_id, s.network_id
    FROM behavior_slices AS s
    INNER JOIN network_entries AS n ON n.network_id = s.network_id
    WHERE $(join(clauses, " AND "))
    ORDER BY s.slice_id
    """

    slice_ids = String[]
    graph_slice_ids = String[]
    network_ids = String[]
    for row in _atlas_sqlite_execute(db, sql, Tuple(params))
        push!(slice_ids, String(row[:slice_id]))
        push!(graph_slice_ids, String(row[:graph_slice_id]))
        push!(network_ids, String(row[:network_id]))
    end

    return Dict(
        "slice_ids" => _sorted_unique_strings(slice_ids),
        "graph_slice_ids" => _sorted_unique_strings(graph_slice_ids),
        "network_ids" => _sorted_unique_strings(network_ids),
    )
end

function atlas_sqlite_load_query_corpus(db::SQLite.DB, raw_query_or_spec)
    query = raw_query_or_spec isa AtlasQuerySpec ? raw_query_or_spec : atlas_query_spec_from_raw(raw_query_or_spec)
    refs = _atlas_sqlite_query_slice_refs(db, query)
    slice_ids = collect(_raw_get(refs, :slice_ids, String[]))
    graph_slice_ids = collect(_raw_get(refs, :graph_slice_ids, String[]))
    network_ids = collect(_raw_get(refs, :network_ids, String[]))

    corpus = atlas_library_default()
    corpus["network_entries"] = _atlas_sqlite_load_json_records(db, "network_entries", "network_id", network_ids)
    corpus["input_graph_slices"] = _atlas_sqlite_load_json_records(db, "input_graph_slices", "graph_slice_id", graph_slice_ids)
    corpus["behavior_slices"] = _atlas_sqlite_load_json_records(db, "behavior_slices", "slice_id", slice_ids)
    corpus["regime_records"] = _atlas_sqlite_load_json_records(db, "regime_records", "slice_id", slice_ids; order_column="regime_record_id")
    corpus["transition_records"] = _atlas_sqlite_load_json_records(db, "transition_records", "slice_id", slice_ids; order_column="transition_record_id")
    corpus["family_buckets"] = _atlas_sqlite_load_json_records(db, "family_buckets", "slice_id", slice_ids; order_column="bucket_id")

    if _query_requires_witness(query)
        corpus["path_records"] = _atlas_sqlite_load_json_records(db, "path_records", "slice_id", slice_ids; order_column="path_record_id")
    end

    corpus["sqlite_prefilter"] = Dict(
        "mode" => "io_prefilter",
        "candidate_slice_count" => length(slice_ids),
        "candidate_graph_slice_count" => length(graph_slice_ids),
        "candidate_network_count" => length(network_ids),
        "loaded_path_record_count" => length(collect(_raw_get(corpus, :path_records, Any[]))),
    )
    return _refresh_atlas_library!(corpus)
end

atlas_sqlite_load_query_corpus(db_path::AbstractString, raw_query_or_spec) =
    _atlas_sqlite_with_db(db -> atlas_sqlite_load_query_corpus(db, raw_query_or_spec), db_path)

function atlas_sqlite_existing_ok_slice_ids(db::SQLite.DB)
    ids = Set{String}()
    sql = """
    SELECT s.slice_id
    FROM behavior_slices AS s
    WHERE s.analysis_status = 'ok'
      AND EXISTS (SELECT 1 FROM regime_records AS rr WHERE rr.slice_id = s.slice_id)
      AND EXISTS (SELECT 1 FROM family_buckets AS fb WHERE fb.slice_id = s.slice_id)
    """
    for row in _atlas_sqlite_execute(db, sql)
        push!(ids, String(row[:slice_id]))
    end
    return ids
end

atlas_sqlite_existing_ok_slice_ids(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_existing_ok_slice_ids, db_path)

function _atlas_sqlite_clear_snapshot_tables!(db::SQLite.DB)
    for table in (
        "atlas_manifests",
        "merge_events",
        "network_entries",
        "input_graph_slices",
        "behavior_slices",
        "regime_records",
        "transition_records",
        "family_buckets",
        "path_records",
        "duplicate_inputs",
    )
        _atlas_sqlite_execute(db, "DELETE FROM " * table)
    end
    _atlas_sqlite_execute(db, "DELETE FROM library_state WHERE snapshot_name = 'default'")
    return db
end

function atlas_sqlite_save_library!(db::SQLite.DB, library)
    stored_library = _refresh_atlas_library!(_materialize(library))
    is_atlas_library(stored_library) || error("atlas_sqlite_save_library! expects an atlas library object.")
    summary = _atlas_library_summary(stored_library)
    slice_index = _atlas_slice_index(collect(_raw_get(stored_library, :behavior_slices, Any[])))

    _atlas_sqlite_transaction(db) do
        _atlas_sqlite_clear_snapshot_tables!(db)
        _atlas_sqlite_execute(db,
            "INSERT INTO library_state (snapshot_name, updated_at, summary_json, library_json) VALUES (?, ?, ?, ?)",
            ("default", _now_iso_timestamp(), _atlas_sqlite_json(summary), _atlas_sqlite_json(stored_library)),
        )
        _atlas_sqlite_execute(db,
            "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
            ("updated_at", _now_iso_timestamp()),
        )

        for manifest in collect(_raw_get(stored_library, :atlas_manifests, Any[]))
            _atlas_sqlite_execute(db,
                "INSERT INTO atlas_manifests (atlas_id, source_label, imported_at, generated_at, behavior_slice_count, manifest_json) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    String(_raw_get(manifest, :atlas_id, "")),
                    _atlas_sqlite_text(_raw_get(manifest, :source_label, nothing)),
                    _atlas_sqlite_text(_raw_get(manifest, :imported_at, nothing)),
                    _atlas_sqlite_text(_raw_get(manifest, :generated_at, nothing)),
                    _atlas_sqlite_int(_raw_get(manifest, :behavior_slice_count, nothing)),
                    _atlas_sqlite_json(manifest),
                ),
            )
        end

        for event in collect(_raw_get(stored_library, :merge_events, Any[]))
            _atlas_sqlite_execute(db,
                "INSERT INTO merge_events (merged_at, status, atlas_id, source_label, event_json) VALUES (?, ?, ?, ?, ?)",
                (
                    _atlas_sqlite_text(_raw_get(event, :merged_at, nothing)),
                    _atlas_sqlite_text(_raw_get(event, :status, nothing)),
                    _atlas_sqlite_text(_raw_get(event, :atlas_id, nothing)),
                    _atlas_sqlite_text(_raw_get(event, :source_label, nothing)),
                    _atlas_sqlite_json(event),
                ),
            )
        end

        for entry in collect(_raw_get(stored_library, :network_entries, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO network_entries (
                    network_id, canonical_code, analysis_status, base_species_count, reaction_count,
                    total_species_count, max_support, support_mass, source_label, source_kind,
                    motif_union_json, exact_union_json, slice_ids_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(entry, :network_id, "")),
                    _atlas_sqlite_text(_raw_get(entry, :canonical_code, nothing)),
                    _atlas_sqlite_text(_raw_get(entry, :analysis_status, nothing)),
                    _atlas_sqlite_int(_raw_get(entry, :base_species_count, nothing)),
                    _atlas_sqlite_int(_raw_get(entry, :reaction_count, nothing)),
                    _atlas_sqlite_int(_raw_get(entry, :total_species_count, nothing)),
                    _atlas_sqlite_int(_raw_get(entry, :max_support, nothing)),
                    _atlas_sqlite_int(_raw_get(entry, :support_mass, nothing)),
                    _atlas_sqlite_text(_raw_get(entry, :source_label, nothing)),
                    _atlas_sqlite_text(_raw_get(entry, :source_kind, nothing)),
                    _atlas_sqlite_json(_raw_get(entry, :motif_union, Any[])),
                    _atlas_sqlite_json(_raw_get(entry, :exact_union, Any[])),
                    _atlas_sqlite_json(_raw_get(entry, :slice_ids, Any[])),
                    _atlas_sqlite_json(entry),
                ),
            )
        end

        for item in collect(_raw_get(stored_library, :input_graph_slices, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO input_graph_slices (
                    graph_slice_id, network_id, input_symbol, vertex_count, edge_count, path_count, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :graph_slice_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :vertex_count, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :edge_count, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :path_count, nothing)),
                    _atlas_sqlite_json(item),
                ),
            )
        end

        for slice in collect(_raw_get(stored_library, :behavior_slices, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO behavior_slices (
                    slice_id, network_id, graph_slice_id, input_symbol, output_symbol, analysis_status,
                    path_scope, min_volume_mean, total_paths, feasible_paths, included_paths, excluded_paths,
                    motif_union_json, exact_union_json, classifier_config_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(slice, :slice_id, "")),
                    _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :output_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :analysis_status, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :path_scope, nothing)),
                    _atlas_sqlite_float(_raw_get(slice, :min_volume_mean, nothing)),
                    _atlas_sqlite_int(_raw_get(slice, :total_paths, nothing)),
                    _atlas_sqlite_int(_raw_get(slice, :feasible_paths, nothing)),
                    _atlas_sqlite_int(_raw_get(slice, :included_paths, nothing)),
                    _atlas_sqlite_int(_raw_get(slice, :excluded_paths, nothing)),
                    _atlas_sqlite_json(_raw_get(slice, :motif_union, Any[])),
                    _atlas_sqlite_json(_raw_get(slice, :exact_union, Any[])),
                    _atlas_sqlite_json(_raw_get(slice, :classifier_config, Dict{String, Any}())),
                    _atlas_sqlite_json(slice),
                ),
            )
        end

        for item in collect(_raw_get(stored_library, :regime_records, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO regime_records (
                    regime_record_id, slice_id, graph_slice_id, network_id, input_symbol, output_symbol,
                    vertex_idx, role, singular, nullity, asymptotic, output_order_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :regime_record_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :output_symbol, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :vertex_idx, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :role, nothing)),
                    _atlas_sqlite_bool(_raw_get(item, :singular, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :nullity, nothing)),
                    _atlas_sqlite_bool(_raw_get(item, :asymptotic, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :output_order_token, nothing)),
                    _atlas_sqlite_json(item),
                ),
            )
        end

        for item in collect(_raw_get(stored_library, :transition_records, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO transition_records (
                    transition_record_id, slice_id, graph_slice_id, input_symbol, output_symbol,
                    from_vertex_idx, to_vertex_idx, from_role, to_role,
                    from_output_order_token, to_output_order_token, transition_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :transition_record_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :output_symbol, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :from_vertex_idx, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :to_vertex_idx, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :from_role, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :to_role, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :from_output_order_token, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :to_output_order_token, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :transition_token, nothing)),
                    _atlas_sqlite_json(item),
                ),
            )
        end

        for bucket in collect(_raw_get(stored_library, :family_buckets, Any[]))
            slice = get(slice_index, String(_raw_get(bucket, :slice_id, "")), Dict{String, Any}())
            _atlas_sqlite_execute(db,
                """
                INSERT INTO family_buckets (
                    bucket_id, slice_id, graph_slice_id, network_id, family_kind, family_label,
                    parent_motif, path_count, robust_path_count, volume_mean, representative_path_idx, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(bucket, :bucket_id, "")),
                    _atlas_sqlite_text(_raw_get(bucket, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(bucket, :family_kind, nothing)),
                    _atlas_sqlite_text(_raw_get(bucket, :family_label, nothing)),
                    _atlas_sqlite_text(_raw_get(bucket, :parent_motif, nothing)),
                    _atlas_sqlite_int(_raw_get(bucket, :path_count, nothing)),
                    _atlas_sqlite_int(_raw_get(bucket, :robust_path_count, nothing)),
                    _atlas_sqlite_float(_raw_get(bucket, :volume_mean, nothing)),
                    _atlas_sqlite_int(_raw_get(bucket, :representative_path_idx, nothing)),
                    _atlas_sqlite_json(bucket),
                ),
            )
        end

        for rec in collect(_raw_get(stored_library, :path_records, Any[]))
            slice = get(slice_index, String(_raw_get(rec, :slice_id, "")), Dict{String, Any}())
            _atlas_sqlite_execute(db,
                """
                INSERT INTO path_records (
                    path_record_id, slice_id, graph_slice_id, network_id, input_symbol, output_symbol,
                    path_idx, path_length, exact_label, motif_label, feasible, robust, volume_mean,
                    output_order_tokens_json, transition_tokens_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(rec, :path_record_id, "")),
                    _atlas_sqlite_text(_raw_get(rec, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(rec, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :output_symbol, nothing)),
                    _atlas_sqlite_int(_raw_get(rec, :path_idx, nothing)),
                    length(collect(_raw_get(rec, :vertex_indices, Any[]))),
                    _atlas_sqlite_text(_raw_get(rec, :exact_label, nothing)),
                    _atlas_sqlite_text(_raw_get(rec, :motif_label, nothing)),
                    _atlas_sqlite_bool(_raw_get(rec, :feasible, nothing)),
                    _atlas_sqlite_bool(_raw_get(rec, :robust, nothing)),
                    _atlas_sqlite_volume_mean(_raw_get(rec, :volume, nothing)),
                    _atlas_sqlite_json(_raw_get(rec, :output_order_tokens, Any[])),
                    _atlas_sqlite_json(_raw_get(rec, :transition_tokens, Any[])),
                    _atlas_sqlite_json(rec),
                ),
            )
        end

        for raw_dup in collect(_raw_get(stored_library, :duplicate_inputs, Any[]))
            dup = _materialize(raw_dup)
            duplicate_key = bytes2hex(SHA.sha1(_atlas_sqlite_json(dup)))
            _atlas_sqlite_execute(db,
                "INSERT INTO duplicate_inputs (duplicate_key, source_label, duplicate_of_network_id, record_json) VALUES (?, ?, ?, ?)",
                (
                    duplicate_key,
                    _atlas_sqlite_text(_raw_get(dup, :source_label, nothing)),
                    _atlas_sqlite_text(_raw_get(dup, :duplicate_of_network_id, nothing)),
                    _atlas_sqlite_json(dup),
                ),
            )
        end
    end

    return stored_library
end

atlas_sqlite_save_library!(db_path::AbstractString, library) = _atlas_sqlite_with_db(db -> atlas_sqlite_save_library!(db, library), db_path)

function atlas_sqlite_record_skip_only_event!(db::SQLite.DB; source_label=nothing, source_metadata=nothing, skipped_existing_network_count::Int=0, skipped_existing_slice_count::Int=0)
    library = atlas_sqlite_has_library(db) ? atlas_sqlite_load_library(db) : atlas_library_default()
    updated = _record_library_skip_only_event(library;
        source_label=source_label,
        source_metadata=source_metadata,
        skipped_existing_network_count=skipped_existing_network_count,
        skipped_existing_slice_count=skipped_existing_slice_count,
    )
    return atlas_sqlite_save_library!(db, updated)
end

function atlas_sqlite_record_skip_only_event!(db_path::AbstractString; kwargs...)
    return _atlas_sqlite_with_db(db -> atlas_sqlite_record_skip_only_event!(db; kwargs...), db_path)
end

function atlas_sqlite_merge_atlas!(db::SQLite.DB, atlas; source_label=nothing, source_metadata=nothing, library_label=nothing, allow_duplicate_atlas::Bool=false)
    library = atlas_sqlite_has_library(db) ? atlas_sqlite_load_library(db) : atlas_library_default()
    if !atlas_sqlite_has_library(db) && library_label !== nothing
        library["library_label"] = String(library_label)
    end
    merged = merge_atlas_library(library, atlas;
        source_label=source_label,
        source_metadata=source_metadata,
        allow_duplicate_atlas=allow_duplicate_atlas,
    )
    return atlas_sqlite_save_library!(db, merged)
end

function atlas_sqlite_merge_atlas!(db_path::AbstractString, atlas; kwargs...)
    return _atlas_sqlite_with_db(db -> atlas_sqlite_merge_atlas!(db, atlas; kwargs...), db_path)
end
