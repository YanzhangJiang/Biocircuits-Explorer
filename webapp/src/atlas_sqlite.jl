const ATLAS_SQLITE_SCHEMA_VERSION = "0.3.0"
const ATLAS_SQLITE_SELECT_BATCH_SIZE = 400
const ATLAS_SQLITE_BUSY_TIMEOUT_MS = 120000
const ATLAS_SQLITE_LOCK_RETRY_DELAYS = (0.1, 0.25, 0.5, 1.0, 2.0, 4.0)
const ATLAS_SQLITE_LIGHTWEIGHT_ENV = "ATLAS_SQLITE_LIGHTWEIGHT_PERSIST"
const ATLAS_SQLITE_PERSIST_MODE_ENV = "ATLAS_SQLITE_PERSIST_MODE"

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
    (value === nothing || ismissing(value)) && return nothing
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

_atlas_sqlite_is_nullish(value) = value === nothing || ismissing(value)
_atlas_sqlite_row_string(value) = _atlas_sqlite_is_nullish(value) ? nothing : String(value)
_atlas_sqlite_row_value(value) = _atlas_sqlite_is_nullish(value) ? nothing : value

function _atlas_sqlite_bool(value)
    value === nothing && return nothing
    return Bool(value) ? 1 : 0
end

function _atlas_sqlite_truthy(value)
    value === nothing && return false
    lowered = lowercase(strip(String(value)))
    isempty(lowered) && return false
    return lowered in ("1", "true", "yes", "on", "y")
end

function _atlas_sqlite_volume_mean(value)
    value === nothing && return nothing
    if value isa AbstractDict
        return _raw_haskey(value, :mean) ? _atlas_sqlite_float(_raw_get(value, :mean, nothing)) : nothing
    end
    return nothing
end

function _atlas_sqlite_is_lock_error(err)
    !(err isa SQLite.SQLiteException) && return false
    msg = lowercase(sprint(showerror, err))
    return occursin("database is locked", msg) || occursin("database table is locked", msg) || occursin("database is busy", msg)
end

struct AtlasSQLiteCursor{Q}
    stmt::SQLite.Stmt
    query::Q
end

Base.iterate(cursor::AtlasSQLiteCursor, state...) = iterate(cursor.query, state...)
Base.isempty(cursor::AtlasSQLiteCursor) = isempty(cursor.query)
Base.IteratorSize(::Type{<:AtlasSQLiteCursor}) = Base.SizeUnknown()

function DBInterface.close!(cursor::AtlasSQLiteCursor)
    try
        DBInterface.close!(cursor.query)
    finally
        DBInterface.close!(cursor.stmt)
    end
    return nothing
end

function _atlas_sqlite_execute(db::SQLite.DB, sql::AbstractString, params=())
    for (attempt, delay_seconds) in pairs((0.0, ATLAS_SQLITE_LOCK_RETRY_DELAYS...))
        stmt = SQLite.Stmt(db, sql; register=false)
        query = nothing
        try
            query = DBInterface.execute(stmt, params)
            DBInterface.close!(query)
            return nothing
        catch err
            query === nothing || try
                DBInterface.close!(query)
            catch
            end
            if !_atlas_sqlite_is_lock_error(err) || attempt == length(ATLAS_SQLITE_LOCK_RETRY_DELAYS) + 1
                rethrow(err)
            end
            sleep(delay_seconds)
        finally
            try
                DBInterface.close!(stmt)
            catch
            end
        end
    end
    return nothing
end

function _atlas_sqlite_query(db::SQLite.DB, sql::AbstractString, params=())
    for (attempt, delay_seconds) in pairs((0.0, ATLAS_SQLITE_LOCK_RETRY_DELAYS...))
        stmt = SQLite.Stmt(db, sql; register=false)
        query = nothing
        try
            query = DBInterface.execute(stmt, params)
            return AtlasSQLiteCursor(stmt, query)
        catch err
            query === nothing || try
                DBInterface.close!(query)
            catch
            end
            try
                DBInterface.close!(stmt)
            catch
            end
            if !_atlas_sqlite_is_lock_error(err) || attempt == length(ATLAS_SQLITE_LOCK_RETRY_DELAYS) + 1
                rethrow(err)
            end
            sleep(delay_seconds)
        end
    end
    return nothing
end

function _atlas_sqlite_table_columns(db::SQLite.DB, table::AbstractString)
    columns = Set{String}()
    query = _atlas_sqlite_query(db, "PRAGMA table_info(" * String(table) * ")")
    try
        for row in query
            push!(columns, String(row[:name]))
        end
    finally
        DBInterface.close!(query)
    end
    return columns
end

function _atlas_sqlite_ensure_columns!(db::SQLite.DB, table::AbstractString, columns)
    existing = _atlas_sqlite_table_columns(db, table)
    for (name, decl) in columns
        name in existing && continue
        _atlas_sqlite_execute(db, "ALTER TABLE $(table) ADD COLUMN $(name) $(decl)")
    end
    return db
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
            change_signature TEXT,
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
            change_signature TEXT,
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
            change_signature TEXT,
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
            change_signature TEXT,
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
            behavior_code TEXT,
            slice_id TEXT,
            graph_slice_id TEXT,
            network_id TEXT,
            input_symbol TEXT,
            change_signature TEXT,
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
            record_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS path_only_records (
            path_record_id TEXT PRIMARY KEY,
            behavior_code TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS classifier_configs (
            cfg INTEGER PRIMARY KEY,
            hash TEXT NOT NULL UNIQUE,
            path_scope TEXT,
            min_volume_mean REAL,
            deduplicate INTEGER,
            keep_singular INTEGER,
            keep_nonasymptotic INTEGER,
            compute_volume INTEGER,
            motif_zero_tol REAL,
            ro_quantization_digits INTEGER,
            ro_quantization_scale INTEGER,
            program_identity TEXT,
            support_semantics TEXT,
            config_json TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS network_features (
            network_id TEXT PRIMARY KEY,
            d INTEGER,
            r INTEGER,
            n_species INTEGER,
            n_complexes INTEGER,
            max_complex_size INTEGER,
            max_reactant_complex_size INTEGER,
            max_product_complex_size INTEGER,
            mean_complex_size REAL,
            assembly_depth INTEGER,
            uses_homomer INTEGER,
            uses_complex_growth INTEGER,
            uses_higher_order_template INTEGER,
            graph_density REAL,
            closure_type TEXT,
            search_profile_id TEXT,
            feature_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS behavior_programs (
            pid INTEGER PRIMARY KEY,
            cfg INTEGER NOT NULL,
            blob BLOB NOT NULL,
            hash TEXT NOT NULL,
            len INTEGER NOT NULL,
            dim INTEGER NOT NULL,
            has_singular INTEGER NOT NULL DEFAULT 0,
            has_nan INTEGER NOT NULL DEFAULT 0,
            has_inf INTEGER NOT NULL DEFAULT 0,
            exact_label TEXT,
            motif_label TEXT,
            UNIQUE(cfg, hash),
            FOREIGN KEY(cfg) REFERENCES classifier_configs(cfg)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS program_features (
            pid INTEGER PRIMARY KEY,
            c_len REAL NOT NULL,
            c_distinct REAL,
            c_sign_changes REAL,
            c_total_variation REAL,
            c_active_dim REAL,
            c_singular REAL,
            feature_json TEXT,
            FOREIGN KEY(pid) REFERENCES behavior_programs(pid)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS slice_program_support (
            sp TEXT NOT NULL,
            pid INTEGER NOT NULL,
            pc INTEGER NOT NULL,
            slice_incidence INTEGER NOT NULL DEFAULT 1,
            rpi INTEGER,
            min_pl INTEGER,
            max_pl INTEGER,
            mean_pl REAL,
            singular_path_count INTEGER DEFAULT 0,
            robust_path_count INTEGER DEFAULT 0,
            volume_mean REAL,
            volume_semantics_code INTEGER DEFAULT 0,
            PRIMARY KEY (sp, pid),
            FOREIGN KEY(pid) REFERENCES behavior_programs(pid)
        ) WITHOUT ROWID
        """,
        """
        CREATE TABLE IF NOT EXISTS network_program_support (
            np TEXT NOT NULL,
            pid INTEGER NOT NULL,
            slice_count INTEGER NOT NULL,
            path_count INTEGER NOT NULL,
            robust_path_count INTEGER DEFAULT 0,
            volume_sum REAL,
            PRIMARY KEY (np, pid),
            FOREIGN KEY(pid) REFERENCES behavior_programs(pid)
        ) WITHOUT ROWID
        """,
        """
        CREATE TABLE IF NOT EXISTS witness_paths (
            sp TEXT NOT NULL,
            pid INTEGER NOT NULL,
            path_idx INTEGER NOT NULL,
            route_blob BLOB,
            raw_token_blob BLOB,
            path_length INTEGER,
            witness_reason_code INTEGER DEFAULT 1,
            PRIMARY KEY (sp, pid),
            FOREIGN KEY(pid) REFERENCES behavior_programs(pid)
        ) WITHOUT ROWID
        """,
        """
        CREATE TABLE IF NOT EXISTS geometry_sidecar_meta (
            key TEXT PRIMARY KEY,
            value_text TEXT
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
        "CREATE INDEX IF NOT EXISTS idx_path_behavior ON path_records (behavior_code)",
        "CREATE INDEX IF NOT EXISTS idx_path_only_behavior ON path_only_records (behavior_code)",
        "CREATE INDEX IF NOT EXISTS idx_behavior_program_hash ON behavior_programs (hash)",
        "CREATE INDEX IF NOT EXISTS idx_slice_program_pid ON slice_program_support (pid)",
        "CREATE INDEX IF NOT EXISTS idx_network_program_pid ON network_program_support (pid)",
    ]

    for statement in statements
        _atlas_sqlite_execute(db, statement)
    end

    _atlas_sqlite_ensure_columns!(db, "input_graph_slices", [
        "change_signature" => "TEXT",
    ])
    _atlas_sqlite_ensure_columns!(db, "behavior_slices", [
        "change_signature" => "TEXT",
    ])
    _atlas_sqlite_ensure_columns!(db, "regime_records", [
        "change_signature" => "TEXT",
    ])
    _atlas_sqlite_ensure_columns!(db, "transition_records", [
        "change_signature" => "TEXT",
    ])
    _atlas_sqlite_ensure_columns!(db, "path_records", [
        "change_signature" => "TEXT",
        "behavior_code" => "TEXT",
    ])

    _atlas_sqlite_execute(db, "CREATE INDEX IF NOT EXISTS idx_graph_slice_change ON input_graph_slices (change_signature)")
    _atlas_sqlite_execute(db, "CREATE INDEX IF NOT EXISTS idx_slice_change ON behavior_slices (change_signature, output_symbol)")
    _atlas_sqlite_execute(db, "CREATE INDEX IF NOT EXISTS idx_regime_change ON regime_records (change_signature)")
    _atlas_sqlite_execute(db, "CREATE INDEX IF NOT EXISTS idx_transition_change ON transition_records (change_signature)")
    _atlas_sqlite_execute(db, "CREATE INDEX IF NOT EXISTS idx_path_change ON path_records (change_signature, output_symbol)")

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
    _atlas_sqlite_execute(db, "PRAGMA busy_timeout = $(ATLAS_SQLITE_BUSY_TIMEOUT_MS)")
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

function _atlas_sqlite_has_snapshot(db::SQLite.DB)
    query = _atlas_sqlite_query(db, "SELECT 1 FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
    try
        for _ in query
            return true
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    return false
end

function _atlas_sqlite_has_appended_corpus(db::SQLite.DB)
    return _atlas_sqlite_count(db, "atlas_manifests") > 0 ||
           _atlas_sqlite_count(db, "network_entries") > 0 ||
           _atlas_sqlite_count(db, "behavior_slices") > 0
end

function atlas_sqlite_has_library(db::SQLite.DB)
    return _atlas_sqlite_has_snapshot(db) || _atlas_sqlite_has_appended_corpus(db)
end

atlas_sqlite_has_library(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_has_library, db_path)

function _atlas_sqlite_load_all_json_records(
    db::SQLite.DB,
    table::AbstractString,
    json_column::AbstractString;
    order_column::AbstractString,
)
    if table == "path_records"
        return _atlas_sqlite_load_path_records(db; order_column=order_column)
    end
    records = Dict{String, Any}[]
    query = _atlas_sqlite_query(db, "SELECT $(json_column) FROM $(table) ORDER BY $(order_column)")
    try
        for row in query
            push!(records, Dict{String, Any}(_atlas_sqlite_read_json(row[Symbol(json_column)])))
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    return records
end

function _atlas_sqlite_merge_path_record_row(row)
    record_json = row[:record_json]
    compact = _atlas_sqlite_is_nullish(record_json) ? Dict{String, Any}() : _atlas_sqlite_read_json(record_json)
    compact isa AbstractDict || (compact = Dict{String, Any}())
    compact = Dict{String, Any}(String(k) => _materialize(v) for (k, v) in pairs(compact))

    output_order_tokens = _atlas_sqlite_is_nullish(row[:output_order_tokens_json]) ? Any[] : _atlas_sqlite_read_json(row[:output_order_tokens_json])
    transition_tokens = _atlas_sqlite_is_nullish(row[:transition_tokens_json]) ? Any[] : _atlas_sqlite_read_json(row[:transition_tokens_json])

    merged = Dict{String, Any}(
        "path_record_id" => String(row[:path_record_id]),
        "behavior_code" => _atlas_sqlite_row_string(row[:behavior_code]),
        "slice_id" => _atlas_sqlite_row_string(row[:slice_id]),
        "graph_slice_id" => _atlas_sqlite_row_string(row[:graph_slice_id]),
        "network_id" => _atlas_sqlite_row_string(row[:network_id]),
        "input_symbol" => _atlas_sqlite_row_string(row[:input_symbol]),
        "change_signature" => _atlas_sqlite_row_string(row[:change_signature]),
        "output_symbol" => _atlas_sqlite_row_string(row[:output_symbol]),
        "path_idx" => _atlas_sqlite_row_value(row[:path_idx]),
        "path_length" => _atlas_sqlite_row_value(row[:path_length]),
        "exact_label" => _atlas_sqlite_row_string(row[:exact_label]),
        "motif_label" => _atlas_sqlite_row_string(row[:motif_label]),
        "feasible" => Bool(something(_atlas_sqlite_row_value(row[:feasible]), 0)),
        "robust" => Bool(something(_atlas_sqlite_row_value(row[:robust]), 0)),
        "volume_mean" => _atlas_sqlite_row_value(row[:volume_mean]),
        "output_order_tokens" => output_order_tokens === nothing ? Any[] : output_order_tokens,
        "transition_tokens" => transition_tokens === nothing ? Any[] : transition_tokens,
    )
    merge!(merged, compact)
    return merged
end

function _atlas_sqlite_load_path_records(db::SQLite.DB; where_sql::Union{Nothing, String}=nothing, params=(), order_column::AbstractString="path_record_id")
    if _atlas_sqlite_path_table(db) == "path_only_records"
        sql = """
        SELECT
            path_record_id, behavior_code,
            NULL AS slice_id, NULL AS graph_slice_id, NULL AS network_id, NULL AS input_symbol, NULL AS change_signature, NULL AS output_symbol,
            NULL AS path_idx, NULL AS path_length, NULL AS exact_label, NULL AS motif_label, NULL AS feasible, NULL AS robust, NULL AS volume_mean,
            NULL AS output_order_tokens_json, NULL AS transition_tokens_json, NULL AS record_json
        FROM path_only_records
        """
        where_sql === nothing || (sql *= " WHERE " * where_sql)
        sql *= " ORDER BY " * String(order_column)
        records = Dict{String, Any}[]
        query = _atlas_sqlite_query(db, sql, params)
        try
            for row in query
                push!(records, _atlas_sqlite_merge_path_record_row(row))
            end
        finally
            query === nothing || DBInterface.close!(query)
        end
        return records
    end

    sql = """
    SELECT
        path_record_id, behavior_code, slice_id, graph_slice_id, network_id, input_symbol, change_signature, output_symbol,
        path_idx, path_length, exact_label, motif_label, feasible, robust, volume_mean,
        output_order_tokens_json, transition_tokens_json, record_json
    FROM path_records
    """
    where_sql === nothing || (sql *= " WHERE " * where_sql)
    sql *= " ORDER BY " * String(order_column)
    records = Dict{String, Any}[]
    query = _atlas_sqlite_query(db, sql, params)
    try
        for row in query
            push!(records, _atlas_sqlite_merge_path_record_row(row))
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    return records
end

function _atlas_sqlite_load_library_from_tables(db::SQLite.DB)
    library = atlas_library_default()
    library["atlas_manifests"] = _atlas_sqlite_load_all_json_records(db, "atlas_manifests", "manifest_json"; order_column="atlas_id")
    library["merge_events"] = _atlas_sqlite_load_all_json_records(db, "merge_events", "event_json"; order_column="event_id")
    library["network_entries"] = _atlas_sqlite_load_all_json_records(db, "network_entries", "record_json"; order_column="network_id")
    library["input_graph_slices"] = _atlas_sqlite_load_all_json_records(db, "input_graph_slices", "record_json"; order_column="graph_slice_id")
    library["behavior_slices"] = _atlas_sqlite_load_all_json_records(db, "behavior_slices", "record_json"; order_column="slice_id")
    library["regime_records"] = _atlas_sqlite_load_all_json_records(db, "regime_records", "record_json"; order_column="regime_record_id")
    library["transition_records"] = _atlas_sqlite_load_all_json_records(db, "transition_records", "record_json"; order_column="transition_record_id")
    library["family_buckets"] = _atlas_sqlite_load_all_json_records(db, "family_buckets", "record_json"; order_column="bucket_id")
    library["path_records"] = _atlas_sqlite_load_all_json_records(db, "path_records", "record_json"; order_column="path_record_id")
    library["duplicate_inputs"] = _atlas_sqlite_load_all_json_records(db, "duplicate_inputs", "record_json"; order_column="duplicate_key")

    library = _refresh_atlas_library!(library)

    change_expansion_json = _atlas_sqlite_metadata_text(db, "change_expansion_json")
    if change_expansion_json !== nothing && !isempty(change_expansion_json)
        library["change_expansion"] = _atlas_sqlite_read_json(change_expansion_json)
    end

    created_at = _atlas_sqlite_metadata_text(db, "created_at")
    updated_at = _atlas_sqlite_metadata_text(db, "updated_at")
    library_label = _atlas_sqlite_metadata_text(db, "library_label")

    created_at === nothing || isempty(created_at) || (library["created_at"] = created_at)
    updated_at === nothing || isempty(updated_at) || (library["updated_at"] = updated_at)
    library_label === nothing || isempty(library_label) || (library["library_label"] = library_label)

    return library
end

function atlas_sqlite_load_library(db::SQLite.DB)
    query = _atlas_sqlite_query(db, "SELECT library_json FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
    try
        for row in query
            return _atlas_sqlite_read_json(row[:library_json])
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    _atlas_sqlite_has_appended_corpus(db) && return _atlas_sqlite_load_library_from_tables(db)
    return atlas_library_default()
end

atlas_sqlite_load_library(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_load_library, db_path)

function atlas_sqlite_summary(db::SQLite.DB)
    query = _atlas_sqlite_query(db, "SELECT summary_json FROM library_state WHERE snapshot_name = 'default' LIMIT 1")
    try
        for row in query
            summary = _atlas_sqlite_read_json(row[:summary_json])
            summary["sqlite_schema_version"] = ATLAS_SQLITE_SCHEMA_VERSION
            return summary
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    summary = _atlas_sqlite_direct_summary(db)
    summary["sqlite_schema_version"] = ATLAS_SQLITE_SCHEMA_VERSION
    return summary
end

atlas_sqlite_summary(db_path::AbstractString) = _atlas_sqlite_with_db(atlas_sqlite_summary, db_path)

function _atlas_sqlite_scalar_value(db::SQLite.DB, sql::AbstractString, params=(); column::Symbol=:value, default=0)
    query = _atlas_sqlite_query(db, sql, params)
    try
        for row in query
            value = row[column]
            return value === nothing ? default : value
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    return default
end

function _atlas_sqlite_metadata_text(db::SQLite.DB, key::AbstractString)
    query = _atlas_sqlite_query(db, "SELECT value_text FROM atlas_metadata WHERE key = ? LIMIT 1", (String(key),))
    try
        for row in query
            value = row[:value_text]
            value === nothing && return nothing
            return String(value)
        end
    finally
        query === nothing || DBInterface.close!(query)
    end
    return nothing
end

function _atlas_sqlite_parse_persist_mode(value)
    value === nothing && return nothing
    lowered = lowercase(strip(String(value)))
    isempty(lowered) && return nothing
    lowered in ("full", "archive") && return :full
    lowered in ("lightweight", "prune", "prune_only") && return :lightweight
    lowered in ("path_only", "path-only", "paths_only", "paths-only", "path") && return :path_only
    lowered in ("behavior_aggregate", "behavior-aggregate", "aggregate", "b0") && return :behavior_aggregate
    error("Unsupported atlas SQLite persist mode: $(repr(value)).")
end

function _atlas_sqlite_persist_mode_text(mode::Symbol)
    mode === :full && return "full"
    mode === :lightweight && return "lightweight"
    mode === :path_only && return "path_only"
    mode === :behavior_aggregate && return "behavior_aggregate"
    error("Unsupported atlas SQLite persist mode symbol $(repr(mode)).")
end

function _atlas_sqlite_persist_mode(db::SQLite.DB; override=nothing)
    override_mode = _atlas_sqlite_parse_persist_mode(override)
    override_mode === nothing || return override_mode

    env_mode = _atlas_sqlite_parse_persist_mode(get(ENV, ATLAS_SQLITE_PERSIST_MODE_ENV, ""))
    env_mode === nothing || return env_mode

    env_value = get(ENV, ATLAS_SQLITE_LIGHTWEIGHT_ENV, "")
    !isempty(strip(env_value)) && return _atlas_sqlite_truthy(env_value) ? :lightweight : :full

    metadata_mode = _atlas_sqlite_parse_persist_mode(_atlas_sqlite_metadata_text(db, "persist_mode"))
    metadata_mode === nothing || return metadata_mode

    return _atlas_sqlite_truthy(_atlas_sqlite_metadata_text(db, "prune_only_sqlite")) ? :lightweight : :full
end

function _atlas_sqlite_lightweight_persist(db::SQLite.DB)
    return _atlas_sqlite_persist_mode(db) === :lightweight
end

function _atlas_sqlite_json_or_empty(payload)
    payload === nothing && return "{}"
    return _atlas_sqlite_json(payload)
end

function _atlas_sqlite_lightweight_record(table::Symbol, row; slice=nothing)
    if table === :network_entries
        return Dict(
            "network_id" => String(_raw_get(row, :network_id, "")),
            "canonical_code" => _atlas_sqlite_text(_raw_get(row, :canonical_code, nothing)),
            "analysis_status" => _atlas_sqlite_text(_raw_get(row, :analysis_status, nothing)),
            "base_species_count" => _atlas_sqlite_int(_raw_get(row, :base_species_count, nothing)),
            "reaction_count" => _atlas_sqlite_int(_raw_get(row, :reaction_count, nothing)),
            "total_species_count" => _atlas_sqlite_int(_raw_get(row, :total_species_count, nothing)),
            "max_support" => _atlas_sqlite_int(_raw_get(row, :max_support, nothing)),
            "support_mass" => _atlas_sqlite_int(_raw_get(row, :support_mass, nothing)),
            "source_label" => _atlas_sqlite_text(_raw_get(row, :source_label, nothing)),
            "source_kind" => _atlas_sqlite_text(_raw_get(row, :source_kind, nothing)),
            "motif_union" => _materialize(_raw_get(row, :motif_union, Any[])),
            "exact_union" => _materialize(_raw_get(row, :exact_union, Any[])),
            "slice_ids" => _materialize(_raw_get(row, :slice_ids, Any[])),
        )
    elseif table === :input_graph_slices
        return Dict(
            "graph_slice_id" => String(_raw_get(row, :graph_slice_id, "")),
            "network_id" => _atlas_sqlite_text(_raw_get(row, :network_id, nothing)),
            "input_symbol" => _atlas_sqlite_text(_raw_get(row, :input_symbol, nothing)),
            "change_signature" => _atlas_sqlite_text(_raw_get(row, :change_signature, nothing)),
            "vertex_count" => _atlas_sqlite_int(_raw_get(row, :vertex_count, nothing)),
            "edge_count" => _atlas_sqlite_int(_raw_get(row, :edge_count, nothing)),
            "path_count" => _atlas_sqlite_int(_raw_get(row, :path_count, nothing)),
        )
    elseif table === :behavior_slices
        return Dict(
            "slice_id" => String(_raw_get(row, :slice_id, "")),
            "network_id" => _atlas_sqlite_text(_raw_get(row, :network_id, nothing)),
            "graph_slice_id" => _atlas_sqlite_text(_raw_get(row, :graph_slice_id, nothing)),
            "input_symbol" => _atlas_sqlite_text(_raw_get(row, :input_symbol, nothing)),
            "change_signature" => _atlas_sqlite_text(_raw_get(row, :change_signature, nothing)),
            "output_symbol" => _atlas_sqlite_text(_raw_get(row, :output_symbol, nothing)),
            "analysis_status" => _atlas_sqlite_text(_raw_get(row, :analysis_status, nothing)),
            "path_scope" => _atlas_sqlite_text(_raw_get(row, :path_scope, nothing)),
            "min_volume_mean" => _atlas_sqlite_float(_raw_get(row, :min_volume_mean, nothing)),
            "total_paths" => _atlas_sqlite_int(_raw_get(row, :total_paths, nothing)),
            "feasible_paths" => _atlas_sqlite_int(_raw_get(row, :feasible_paths, nothing)),
            "included_paths" => _atlas_sqlite_int(_raw_get(row, :included_paths, nothing)),
            "excluded_paths" => _atlas_sqlite_int(_raw_get(row, :excluded_paths, nothing)),
            "motif_union" => _materialize(_raw_get(row, :motif_union, Any[])),
            "exact_union" => _materialize(_raw_get(row, :exact_union, Any[])),
            "classifier_config" => _materialize(_raw_get(row, :classifier_config, Dict{String, Any}())),
        )
    elseif table === :regime_records
        return Dict(
            "regime_record_id" => String(_raw_get(row, :regime_record_id, "")),
            "slice_id" => _atlas_sqlite_text(_raw_get(row, :slice_id, nothing)),
            "graph_slice_id" => _atlas_sqlite_text(_raw_get(row, :graph_slice_id, nothing)),
            "network_id" => _atlas_sqlite_text(_raw_get(row, :network_id, nothing)),
            "input_symbol" => _atlas_sqlite_text(_raw_get(row, :input_symbol, nothing)),
            "change_signature" => _atlas_sqlite_text(_raw_get(row, :change_signature, nothing)),
            "output_symbol" => _atlas_sqlite_text(_raw_get(row, :output_symbol, nothing)),
            "vertex_idx" => _atlas_sqlite_int(_raw_get(row, :vertex_idx, nothing)),
            "role" => _atlas_sqlite_text(_raw_get(row, :role, nothing)),
            "singular" => Bool(_raw_get(row, :singular, false)),
            "nullity" => _atlas_sqlite_int(_raw_get(row, :nullity, nothing)),
            "asymptotic" => Bool(_raw_get(row, :asymptotic, false)),
            "output_order_token" => _atlas_sqlite_text(_raw_get(row, :output_order_token, nothing)),
        )
    elseif table === :family_buckets
        return Dict(
            "bucket_id" => String(_raw_get(row, :bucket_id, "")),
            "slice_id" => _atlas_sqlite_text(_raw_get(row, :slice_id, nothing)),
            "graph_slice_id" => _atlas_sqlite_text(_raw_get(slice, :graph_slice_id, nothing)),
            "network_id" => _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
            "family_kind" => _atlas_sqlite_text(_raw_get(row, :family_kind, nothing)),
            "family_label" => _atlas_sqlite_text(_raw_get(row, :family_label, nothing)),
            "parent_motif" => _atlas_sqlite_text(_raw_get(row, :parent_motif, nothing)),
            "path_count" => _atlas_sqlite_int(_raw_get(row, :path_count, nothing)),
            "robust_path_count" => _atlas_sqlite_int(_raw_get(row, :robust_path_count, nothing)),
            "volume_mean" => _atlas_sqlite_float(_raw_get(row, :volume_mean, nothing)),
            "representative_path_idx" => _atlas_sqlite_int(_raw_get(row, :representative_path_idx, nothing)),
        )
    elseif table === :transition_records
        return Dict(
            "transition_record_id" => String(_raw_get(row, :transition_record_id, "")),
            "slice_id" => _atlas_sqlite_text(_raw_get(row, :slice_id, nothing)),
            "graph_slice_id" => _atlas_sqlite_text(_raw_get(row, :graph_slice_id, nothing)),
            "input_symbol" => _atlas_sqlite_text(_raw_get(row, :input_symbol, nothing)),
            "change_signature" => _atlas_sqlite_text(_raw_get(row, :change_signature, nothing)),
            "output_symbol" => _atlas_sqlite_text(_raw_get(row, :output_symbol, nothing)),
            "from_vertex_idx" => _atlas_sqlite_int(_raw_get(row, :from_vertex_idx, nothing)),
            "to_vertex_idx" => _atlas_sqlite_int(_raw_get(row, :to_vertex_idx, nothing)),
            "from_role" => _atlas_sqlite_text(_raw_get(row, :from_role, nothing)),
            "to_role" => _atlas_sqlite_text(_raw_get(row, :to_role, nothing)),
            "from_output_order_token" => _atlas_sqlite_text(_raw_get(row, :from_output_order_token, nothing)),
            "to_output_order_token" => _atlas_sqlite_text(_raw_get(row, :to_output_order_token, nothing)),
            "transition_token" => _atlas_sqlite_text(_raw_get(row, :transition_token, nothing)),
        )
    elseif table === :path_records
        return Dict(
            "path_record_id" => String(_raw_get(row, :path_record_id, "")),
            "slice_id" => _atlas_sqlite_text(_raw_get(row, :slice_id, nothing)),
            "graph_slice_id" => _atlas_sqlite_text(_raw_get(row, :graph_slice_id, nothing)),
            "network_id" => _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
            "input_symbol" => _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
            "change_signature" => _atlas_sqlite_text(_raw_get(slice, :change_signature, nothing)),
            "output_symbol" => _atlas_sqlite_text(_raw_get(slice, :output_symbol, nothing)),
            "path_idx" => _atlas_sqlite_int(_raw_get(row, :path_idx, nothing)),
            "path_length" => length(collect(_raw_get(row, :vertex_indices, Any[]))),
            "exact_label" => _atlas_sqlite_text(_raw_get(row, :exact_label, nothing)),
            "motif_label" => _atlas_sqlite_text(_raw_get(row, :motif_label, nothing)),
            "feasible" => Bool(_raw_get(row, :feasible, false)),
            "robust" => Bool(_raw_get(row, :robust, false)),
            "volume_mean" => _atlas_sqlite_volume_mean(_raw_get(row, :volume, nothing)),
            "output_order_tokens" => _materialize(_raw_get(row, :output_order_tokens, Any[])),
            "transition_tokens" => _materialize(_raw_get(row, :transition_tokens, Any[])),
        )
    end
    return Dict{String, Any}()
end

function _atlas_sqlite_path_only_record(table::Symbol, row; slice=nothing)
    if table === :path_records
        return Dict(
            "vertex_indices" => _materialize(_raw_get(row, :vertex_indices, Any[])),
            "regime_sequence" => _materialize(_raw_get(row, :regime_sequence, Any[])),
            "transition_sequence" => _materialize(_raw_get(row, :transition_sequence, Any[])),
            "exact_profile" => _materialize(_raw_get(row, :exact_profile, Any[])),
            "motif_profile" => _materialize(_raw_get(row, :motif_profile, Any[])),
            "exact_family_idx" => _raw_get(row, :exact_family_idx, nothing),
            "motif_family_idx" => _raw_get(row, :motif_family_idx, nothing),
            "included" => Bool(_raw_get(row, :included, false)),
            "feasibility_checked" => Bool(_raw_get(row, :feasibility_checked, false)),
            "exclusion_reason" => _atlas_sqlite_text(_raw_get(row, :exclusion_reason, nothing)),
            "volume" => _materialize(_raw_get(row, :volume, nothing)),
        )
    end
    return Dict{String, Any}()
end

function _atlas_sqlite_record_payload(table::Symbol, row; slice=nothing, persist_mode::Symbol=:full)
    return persist_mode in (:lightweight, :behavior_aggregate) ? _atlas_sqlite_json_or_empty(_atlas_sqlite_lightweight_record(table, row; slice=slice)) :
           persist_mode === :path_only ? _atlas_sqlite_json_or_empty(_atlas_sqlite_path_only_record(table, row; slice=slice)) :
           _atlas_sqlite_json(row)
end

function _atlas_sqlite_skip_table_in_mode(table::Symbol, persist_mode::Symbol)
    persist_mode === :lightweight && return table in (:transition_records, :path_records)
    persist_mode === :path_only && return table != :path_records
    persist_mode === :behavior_aggregate && return table in (:regime_records, :transition_records, :family_buckets, :path_records, :duplicate_inputs)
    return false
end

function _atlas_sqlite_set_metadata!(db::SQLite.DB, key::AbstractString, value)
    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        (String(key), value === nothing ? "" : String(value)),
    )
    return db
end

function _atlas_sqlite_metadata_int(db::SQLite.DB, key::AbstractString, default::Int=0)
    value = _atlas_sqlite_metadata_text(db, key)
    value === nothing && return default
    try
        return parse(Int, value)
    catch
        return default
    end
end

function _atlas_sqlite_count(db::SQLite.DB, table::AbstractString; where_clause::Union{Nothing, String}=nothing)
    sql = "SELECT COUNT(*) AS value FROM $(table)"
    where_clause === nothing || (sql *= " WHERE " * where_clause)
    return Int(_atlas_sqlite_scalar_value(db, sql; default=0))
end

function _atlas_sqlite_path_table(db::SQLite.DB)
    return _atlas_sqlite_persist_mode(db) === :path_only ? "path_only_records" : "path_records"
end

function _atlas_sqlite_base64url_encode(bytes::Vector{UInt8})
    encoded = Base64.base64encode(bytes)
    encoded = replace(encoded, '+' => '-', '/' => '_')
    return replace(encoded, r"=+$" => "")
end

function _atlas_sqlite_base36_encode(value::Integer)
    value >= 0 || error("_atlas_sqlite_base36_encode expects a non-negative integer.")
    return uppercase(string(value; base=36))
end

function _atlas_sqlite_varuint_push!(buf::Vector{UInt8}, value::Integer)
    value >= 0 || error("_atlas_sqlite_varuint_push! expects a non-negative integer.")
    current = UInt64(value)
    while current >= 0x80
        push!(buf, UInt8((current & 0x7f) | 0x80))
        current >>= 7
    end
    push!(buf, UInt8(current))
    return buf
end

function _atlas_sqlite_push_bytes!(buf::Vector{UInt8}, bytes::AbstractVector{UInt8})
    _atlas_sqlite_varuint_push!(buf, length(bytes))
    append!(buf, bytes)
    return buf
end

function _atlas_sqlite_push_text!(buf::Vector{UInt8}, text::AbstractString)
    return _atlas_sqlite_push_bytes!(buf, collect(codeunits(String(text))))
end

function _atlas_sqlite_parse_canonical_term(term::AbstractString)
    text = strip(String(term))
    startswith(text, "[") && endswith(text, "]") || error("Unrecognized canonical network term: $text")
    inner = text[2:(end - 1)]
    isempty(inner) && return Int[]
    return [parse(Int, piece) for piece in split(inner, ",")]
end

function _atlas_sqlite_parse_canonical_side(side::AbstractString)
    text = strip(String(side))
    isempty(text) && return Vector{Vector{Int}}()
    return [_atlas_sqlite_parse_canonical_term(term) for term in split(text, "+")]
end

function _atlas_sqlite_encode_network_id!(buf::Vector{UInt8}, network_id::AbstractString)
    reactions = split(String(network_id), "|")
    _atlas_sqlite_varuint_push!(buf, length(reactions))
    for reaction in reactions
        parts = split(reaction, "<->")
        length(parts) == 2 || error("Unrecognized canonical network reaction: $reaction")
        left_terms = _atlas_sqlite_parse_canonical_side(parts[1])
        right_terms = _atlas_sqlite_parse_canonical_side(parts[2])
        _atlas_sqlite_varuint_push!(buf, length(left_terms))
        for term in left_terms
            _atlas_sqlite_varuint_push!(buf, length(term))
            for atom_idx in term
                _atlas_sqlite_varuint_push!(buf, atom_idx)
            end
        end
        _atlas_sqlite_varuint_push!(buf, length(right_terms))
        for term in right_terms
            _atlas_sqlite_varuint_push!(buf, length(term))
            for atom_idx in term
                _atlas_sqlite_varuint_push!(buf, atom_idx)
            end
        end
    end
    return buf
end

function _atlas_sqlite_cfg_parts(cfg_signature::AbstractString)
    parts = Dict{String, String}()
    for segment in split(String(cfg_signature), ";")
        isempty(segment) && continue
        kv = split(segment, "="; limit=2)
        length(kv) == 2 || error("Malformed cfg signature segment: $segment")
        parts[kv[1]] = kv[2]
    end
    return parts
end

function _atlas_sqlite_encode_cfg!(buf::Vector{UInt8}, cfg_signature::AbstractString)
    parts = _atlas_sqlite_cfg_parts(cfg_signature)
    scope_codes = Dict(
        "feasible" => UInt8(0),
        "all" => UInt8(1),
        "included" => UInt8(2),
        "robust" => UInt8(3),
    )
    scope = get(parts, "scope", "feasible")
    if haskey(scope_codes, scope)
        push!(buf, scope_codes[scope])
    else
        push!(buf, UInt8(255))
        _atlas_sqlite_push_text!(buf, scope)
    end

    flags = UInt8(0)
    _atlas_sqlite_truthy(get(parts, "deduplicate", "true")) && (flags |= 0x01)
    _atlas_sqlite_truthy(get(parts, "keep_singular", "true")) && (flags |= 0x02)
    _atlas_sqlite_truthy(get(parts, "keep_nonasymptotic", "false")) && (flags |= 0x04)
    _atlas_sqlite_truthy(get(parts, "compute_volume", "false")) && (flags |= 0x08)
    push!(buf, flags)

    min_volume_mean = get(parts, "min_volume_mean", "0.0")
    if min_volume_mean in ("0", "0.0")
        push!(buf, UInt8(0))
    else
        push!(buf, UInt8(1))
        _atlas_sqlite_push_text!(buf, min_volume_mean)
    end

    motif_zero_tol = get(parts, "motif_zero_tol", "1.0e-6")
    lowered_tol = lowercase(motif_zero_tol)
    if lowered_tol in ("1.0e-6", "1e-6")
        push!(buf, UInt8(0))
    else
        push!(buf, UInt8(1))
        _atlas_sqlite_push_text!(buf, motif_zero_tol)
    end

    return buf
end

const _ATLAS_SQLITE_BEHAVIOR_TOKEN_CODES = Dict(
    "+1" => UInt8(0),
    "0" => UInt8(1),
    "-1" => UInt8(2),
    "+Inf" => UInt8(3),
    "-Inf" => UInt8(4),
    "NaN" => UInt8(5),
)

function _atlas_sqlite_behavior_numeric_milli_text(value_milli::Integer)
    value_milli == 0 && return "0"
    abs_milli = abs(Int(value_milli))
    whole = abs_milli ÷ 1000
    frac = abs_milli % 1000
    body = if frac == 0
        string(whole)
    else
        string(whole, ".", rstrip(lpad(string(frac), 3, '0'), '0'))
    end
    return value_milli > 0 ? "+" * body : "-" * body
end

function _atlas_sqlite_behavior_numeric_milli(token::AbstractString)
    text = strip(String(token))
    value = tryparse(Float64, text)
    value === nothing && return nothing
    (!isfinite(value) || isnan(value)) && return nothing
    rounded = round(value; digits=3)
    milli = round(Int, rounded * 1000)
    return milli
end

function _atlas_sqlite_parse_behavior_token(token::AbstractString)
    text = strip(String(token))
    if startswith(text, "(") && endswith(text, ")")
        inner = text[2:(end - 1)]
        coords = isempty(inner) ? String[] : split(inner, ",")
        return (:vector, [strip(coord) for coord in coords])
    end
    return (:scalar, [text])
end

function _atlas_sqlite_behavior_token_code(token::AbstractString)
    code = get(_ATLAS_SQLITE_BEHAVIOR_TOKEN_CODES, String(token), nothing)
    return code
end

function _atlas_sqlite_zigzag_varint_push!(buf::Vector{UInt8}, value::Integer)
    signed = Int(value)
    encoded = signed >= 0 ? (UInt64(signed) << 1) : ((UInt64(-signed) << 1) - 1)
    return _atlas_sqlite_varuint_push!(buf, encoded)
end

function _atlas_sqlite_encode_behavior_scalar_atom!(buf::Vector{UInt8}, token::AbstractString)
    text = strip(String(token))
    code = _atlas_sqlite_behavior_token_code(text)
    if code !== nothing
        push!(buf, UInt8(0))
        push!(buf, code)
        return buf
    end

    milli = _atlas_sqlite_behavior_numeric_milli(text)
    if milli !== nothing
        push!(buf, UInt8(1))
        _atlas_sqlite_zigzag_varint_push!(buf, milli)
        return buf
    end

    push!(buf, UInt8(2))
    _atlas_sqlite_push_text!(buf, text)
    return buf
end

function _atlas_sqlite_encode_behavior_token!(buf::Vector{UInt8}, token::AbstractString)
    kind, coords = _atlas_sqlite_parse_behavior_token(token)
    if kind == :scalar
        push!(buf, UInt8(0))
        _atlas_sqlite_encode_behavior_scalar_atom!(buf, only(coords))
    else
        push!(buf, UInt8(1))
        _atlas_sqlite_varuint_push!(buf, length(coords))
        for coord in coords
            _atlas_sqlite_encode_behavior_scalar_atom!(buf, coord)
        end
    end
    return buf
end

function _atlas_sqlite_profile_scalar_token(value)
    if value isa AbstractString
        return String(value)
    elseif value isa Real
        val = Float64(value)
        isnan(val) && return "NaN"
        isinf(val) && return signbit(val) ? "-Inf" : "+Inf"
        rounded = round(val; digits=3)
        abs(rounded) < 1e-6 && return "0"
        rounded_int = round(Int, rounded)
        if isapprox(rounded, rounded_int; atol=1e-6)
            return rounded_int > 0 ? "+" * string(rounded_int) : string(rounded_int)
        end
        text = string(rounded)
        return rounded > 0 ? "+" * text : text
    end
    return string(value)
end

function _atlas_sqlite_profile_token(value)
    if value isa AbstractVector
        return "(" * join((_atlas_sqlite_profile_scalar_token(coord) for coord in value), ",") * ")"
    end
    return _atlas_sqlite_profile_scalar_token(value)
end

function _atlas_sqlite_behavior_tokens(rec)
    exact_profile = collect(_raw_get(rec, :exact_profile, Any[]))
    if !isempty(exact_profile)
        return [_atlas_sqlite_profile_token(token) for token in exact_profile]
    end
    return [String(token) for token in collect(_raw_get(rec, :output_order_tokens, Any[]))]
end

function _atlas_sqlite_behavior_code(rec)
    tokens = _atlas_sqlite_behavior_tokens(rec)
    buf = UInt8[]
    _atlas_sqlite_varuint_push!(buf, length(tokens))
    for token in tokens
        _atlas_sqlite_encode_behavior_token!(buf, token)
    end
    return "b2." * _atlas_sqlite_base64url_encode(buf)
end

function _atlas_sqlite_config_signature_for_program(cfg)
    normalized = behavior_program_config_from_raw(cfg)
    return join([
        "scope=" * String(_raw_get(normalized, :path_scope, "feasible")),
        "min_volume_mean=" * string(_raw_get(normalized, :min_volume_mean, 0.0)),
        "deduplicate=" * string(_raw_get(normalized, :deduplicate, true)),
        "keep_singular=" * string(_raw_get(normalized, :keep_singular, true)),
        "keep_nonasymptotic=" * string(_raw_get(normalized, :keep_nonasymptotic, false)),
        "compute_volume=" * string(_raw_get(normalized, :compute_volume, false)),
        "motif_zero_tol=" * string(_raw_get(normalized, :motif_zero_tol, 1e-6)),
        "ro_quantization_digits=" * string(_raw_get(normalized, :ro_quantization_digits, DEFAULT_RO_QUANTIZATION_DIGITS)),
        "ro_quantization_scale=" * string(_raw_get(normalized, :ro_quantization_scale, DEFAULT_RO_QUANTIZATION_SCALE)),
        "program_identity=" * String(_raw_get(normalized, :program_identity, DEFAULT_PROGRAM_IDENTITY)),
        "support_semantics=" * String(_raw_get(normalized, :support_semantics, DEFAULT_SUPPORT_SEMANTICS)),
    ], ";")
end

_atlas_sqlite_sha256_hex(text::AbstractString) = bytes2hex(SHA.sha256(collect(codeunits(String(text)))))

function _atlas_sqlite_intern_classifier_config!(db::SQLite.DB, cfg_raw)
    cfg = behavior_program_config_from_raw(cfg_raw)
    hash = _atlas_sqlite_sha256_hex(_atlas_sqlite_config_signature_for_program(cfg))
    _atlas_sqlite_execute(db,
        """
        INSERT OR IGNORE INTO classifier_configs (
            hash, path_scope, min_volume_mean, deduplicate, keep_singular, keep_nonasymptotic,
            compute_volume, motif_zero_tol, ro_quantization_digits, ro_quantization_scale,
            program_identity, support_semantics, config_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            hash,
            _atlas_sqlite_text(_raw_get(cfg, :path_scope, nothing)),
            _atlas_sqlite_float(_raw_get(cfg, :min_volume_mean, nothing)),
            _atlas_sqlite_bool(_raw_get(cfg, :deduplicate, nothing)),
            _atlas_sqlite_bool(_raw_get(cfg, :keep_singular, nothing)),
            _atlas_sqlite_bool(_raw_get(cfg, :keep_nonasymptotic, nothing)),
            _atlas_sqlite_bool(_raw_get(cfg, :compute_volume, nothing)),
            _atlas_sqlite_float(_raw_get(cfg, :motif_zero_tol, nothing)),
            _atlas_sqlite_int(_raw_get(cfg, :ro_quantization_digits, nothing)),
            _atlas_sqlite_int(_raw_get(cfg, :ro_quantization_scale, nothing)),
            _atlas_sqlite_text(_raw_get(cfg, :program_identity, nothing)),
            _atlas_sqlite_text(_raw_get(cfg, :support_semantics, nothing)),
            _atlas_sqlite_json(cfg),
        ),
    )
    return Int(_atlas_sqlite_scalar_value(db, "SELECT cfg AS value FROM classifier_configs WHERE hash = ? LIMIT 1", (hash,)))
end

function _atlas_sqlite_bucket_program_profile(bucket)
    profile = _raw_get(bucket, :exact_profile, nothing)
    if profile !== nothing
        try
            collected = collect(profile)
            isempty(collected) || return collected
        catch
            return profile
        end
    end
    label = _raw_get(bucket, :family_label, "")
    return behavior_program_profile_from_label(String(label))
end

function _atlas_sqlite_route_blob(route)
    values = Int.(collect(route))
    buf = UInt8[]
    _program_varuint_push!(buf, length(values))
    for value in values
        _program_varuint_push!(buf, max(0, value))
    end
    return buf
end

function _atlas_sqlite_text_blob(text)
    return collect(codeunits(String(text)))
end

function _atlas_sqlite_program_blob_param(bytes::Vector{UInt8})
    return bytes
end

function _atlas_sqlite_intern_program!(
    db::SQLite.DB,
    profile,
    cfg_id::Integer,
    cfg;
    exact_label=nothing,
    motif_label=nothing,
)
    blob = encode_program_blob(profile, cfg)
    hash = behavior_program_hash(blob)
    features = program_features(profile, cfg)
    exact = exact_label === nothing || isempty(String(exact_label)) ? program_exact_label(profile, cfg) : String(exact_label)
    motif = motif_label === nothing || isempty(String(motif_label)) ? program_motif_label(profile, cfg) : String(motif_label)
    _atlas_sqlite_execute(db,
        """
        INSERT OR IGNORE INTO behavior_programs (
            cfg, blob, hash, len, dim, has_singular, has_nan, has_inf, exact_label, motif_label
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            Int(cfg_id),
            _atlas_sqlite_program_blob_param(blob),
            hash,
            Int(features["len"]),
            Int(features["dim"]),
            Bool(features["has_singular"]) ? 1 : 0,
            Bool(features["has_nan"]) ? 1 : 0,
            Bool(features["has_inf"]) ? 1 : 0,
            _atlas_sqlite_text(exact),
            _atlas_sqlite_text(motif),
        ),
    )
    pid = Int(_atlas_sqlite_scalar_value(db, "SELECT pid AS value FROM behavior_programs WHERE cfg = ? AND hash = ? LIMIT 1", (Int(cfg_id), hash)))
    _atlas_sqlite_execute(db,
        """
        INSERT INTO program_features (
            pid, c_len, c_distinct, c_sign_changes, c_total_variation, c_active_dim, c_singular, feature_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pid) DO UPDATE SET
            c_len = excluded.c_len,
            c_distinct = excluded.c_distinct,
            c_sign_changes = excluded.c_sign_changes,
            c_total_variation = excluded.c_total_variation,
            c_active_dim = excluded.c_active_dim,
            c_singular = excluded.c_singular,
            feature_json = excluded.feature_json
        """,
        (
            pid,
            Float64(features["c_len"]),
            Float64(features["c_distinct"]),
            Float64(features["c_sign_changes"]),
            Float64(features["c_total_variation"]),
            Float64(features["c_active_dim"]),
            Float64(features["c_singular"]),
            _atlas_sqlite_json(features),
        ),
    )
    return pid, features
end

function _atlas_sqlite_bucket_volume_mean(bucket)
    total_volume = _raw_get(bucket, :total_volume, nothing)
    total_mean = _atlas_sqlite_volume_mean(total_volume)
    total_mean === nothing || return total_mean
    return _atlas_sqlite_float(_raw_get(bucket, :volume_mean, nothing))
end

function _atlas_sqlite_insert_slice_program_support!(db::SQLite.DB, slice_id::AbstractString, pid::Integer, bucket, features)
    pc = max(0, Int(_raw_get(bucket, :path_count, 0)))
    rep_len = Int(_raw_get(bucket, :representative_path_length, 0))
    path_lengths = Int[]
    rep_len > 0 && push!(path_lengths, rep_len)
    singular_count = Bool(features["has_singular"]) ? pc : 0
    volume_mean = _atlas_sqlite_bucket_volume_mean(bucket)
    _atlas_sqlite_execute(db,
        """
        INSERT INTO slice_program_support (
            sp, pid, pc, slice_incidence, rpi, min_pl, max_pl, mean_pl,
            singular_path_count, robust_path_count, volume_mean, volume_semantics_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sp, pid) DO UPDATE SET
            pc = excluded.pc,
            slice_incidence = excluded.slice_incidence,
            rpi = excluded.rpi,
            min_pl = excluded.min_pl,
            max_pl = excluded.max_pl,
            mean_pl = excluded.mean_pl,
            singular_path_count = excluded.singular_path_count,
            robust_path_count = excluded.robust_path_count,
            volume_mean = excluded.volume_mean,
            volume_semantics_code = excluded.volume_semantics_code
        """,
        (
            String(slice_id),
            Int(pid),
            pc,
            pc > 0 ? 1 : 0,
            _atlas_sqlite_int(_raw_get(bucket, :representative_path_idx, nothing)),
            isempty(path_lengths) ? nothing : minimum(path_lengths),
            isempty(path_lengths) ? nothing : maximum(path_lengths),
            isempty(path_lengths) ? nothing : Float64(sum(path_lengths)) / length(path_lengths),
            singular_count,
            _atlas_sqlite_int(_raw_get(bucket, :robust_path_count, 0)),
            volume_mean,
            volume_mean === nothing ? 0 : 1,
        ),
    )
    return db
end

function _atlas_sqlite_insert_witness_path!(db::SQLite.DB, slice_id::AbstractString, pid::Integer, bucket, profile, cfg)
    path_idx = Int(_raw_get(bucket, :representative_path_idx, 0))
    path_idx > 0 || return db
    route = collect(_raw_get(bucket, :representative_vertex_indices, Any[]))
    route_blob = isempty(route) ? nothing : _atlas_sqlite_route_blob(route)
    raw_token_blob = _atlas_sqlite_text_blob(program_exact_label(profile, cfg))
    path_length = Int(_raw_get(bucket, :representative_path_length, isempty(route) ? 0 : length(route)))
    _atlas_sqlite_execute(db,
        """
        INSERT INTO witness_paths (
            sp, pid, path_idx, route_blob, raw_token_blob, path_length, witness_reason_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sp, pid) DO UPDATE SET
            path_idx = excluded.path_idx,
            route_blob = excluded.route_blob,
            raw_token_blob = excluded.raw_token_blob,
            path_length = excluded.path_length,
            witness_reason_code = excluded.witness_reason_code
        """,
        (
            String(slice_id),
            Int(pid),
            path_idx,
            route_blob,
            raw_token_blob,
            path_length == 0 ? nothing : path_length,
            1,
        ),
    )
    return db
end

function _atlas_sqlite_refresh_network_program_support!(db::SQLite.DB; network_id=nothing)
    where_sql = network_id === nothing ? "" : "WHERE bs.network_id = ?"
    params = network_id === nothing ? () : (String(network_id),)
    if network_id !== nothing
        _atlas_sqlite_execute(db, "DELETE FROM network_program_support WHERE np = ?", (String(network_id),))
    else
        _atlas_sqlite_execute(db, "DELETE FROM network_program_support")
    end
    _atlas_sqlite_execute(db,
        """
        INSERT INTO network_program_support (
            np, pid, slice_count, path_count, robust_path_count, volume_sum
        )
        SELECT
            bs.network_id AS np,
            sps.pid AS pid,
            SUM(sps.slice_incidence) AS slice_count,
            SUM(sps.pc) AS path_count,
            SUM(COALESCE(sps.robust_path_count, 0)) AS robust_path_count,
            SUM(COALESCE(sps.volume_mean, 0.0)) AS volume_sum
        FROM slice_program_support AS sps
        JOIN behavior_slices AS bs ON bs.slice_id = sps.sp
        $where_sql
        GROUP BY bs.network_id, sps.pid
        """,
        params,
    )
    return db
end

function _atlas_sqlite_write_behavior_aggregate!(db::SQLite.DB, atlas, slice_index)
    affected_network_ids = Set{String}()
    for bucket in collect(_raw_get(atlas, :family_buckets, Any[]))
        String(_raw_get(bucket, :family_kind, "")) == "exact" || continue
        slice_id = String(_raw_get(bucket, :slice_id, ""))
        isempty(slice_id) && continue
        slice = get(slice_index, slice_id, nothing)
        slice === nothing && continue
        String(_raw_get(slice, :analysis_status, "")) == "ok" || continue
        cfg = behavior_program_config_from_raw(_raw_get(slice, :classifier_config, Dict{String, Any}()))
        cfg_id = _atlas_sqlite_intern_classifier_config!(db, cfg)
        profile = _atlas_sqlite_bucket_program_profile(bucket)
        pid, features = _atlas_sqlite_intern_program!(
            db,
            profile,
            cfg_id,
            cfg;
            exact_label=_raw_get(bucket, :family_label, nothing),
            motif_label=_raw_get(bucket, :parent_motif, nothing),
        )
        _atlas_sqlite_insert_slice_program_support!(db, slice_id, pid, bucket, features)
        _atlas_sqlite_insert_witness_path!(db, slice_id, pid, bucket, profile, cfg)
        network_id = String(_raw_get(slice, :network_id, ""))
        isempty(network_id) || push!(affected_network_ids, network_id)
    end
    for network_id in affected_network_ids
        _atlas_sqlite_refresh_network_program_support!(db; network_id=network_id)
    end
    return db
end

function _atlas_sqlite_insert_network_features!(db::SQLite.DB, entry)
    canonical_code = String(_raw_get(entry, :canonical_code, _raw_get(entry, :network_id, "")))
    isempty(canonical_code) && return db
    reactions = isempty(canonical_code) ? String[] : split(canonical_code, "|")
    complexes = Set{Vector{Int}}()
    max_reactant = 0
    max_product = 0
    uses_homomer = false
    uses_complex_growth = false
    depths = Dict{Vector{Int}, Int}()

    for reaction in reactions
        parts = split(String(reaction), "<->")
        length(parts) == 2 || continue
        left_terms = _atlas_sqlite_parse_canonical_side(parts[1])
        right_terms = _atlas_sqlite_parse_canonical_side(parts[2])
        for term in vcat(left_terms, right_terms)
            sorted_term = sort(collect(term))
            push!(complexes, sorted_term)
            length(sorted_term) == 1 && (depths[sorted_term] = 0)
            uses_homomer |= length(unique(sorted_term)) < length(sorted_term)
        end
        isempty(left_terms) || (max_reactant = max(max_reactant, maximum(length.(left_terms))))
        isempty(right_terms) || (max_product = max(max_product, maximum(length.(right_terms))))
        uses_complex_growth |= any(term -> length(term) > 1, left_terms) && any(term -> length(term) > maximum(length.(left_terms)), right_terms)
    end

    changed = true
    while changed
        changed = false
        for reaction in reactions
            parts = split(String(reaction), "<->")
            length(parts) == 2 || continue
            left_terms = [sort(collect(term)) for term in _atlas_sqlite_parse_canonical_side(parts[1])]
            right_terms = [sort(collect(term)) for term in _atlas_sqlite_parse_canonical_side(parts[2])]
            for (sources, targets) in ((left_terms, right_terms), (right_terms, left_terms))
                isempty(sources) && continue
                all(haskey(depths, source) for source in sources) || continue
                source_depth = 1 + maximum(depths[source] for source in sources)
                for target in targets
                    current = get(depths, target, typemax(Int))
                    if source_depth < current
                        depths[target] = source_depth
                        changed = true
                    end
                end
            end
        end
    end

    complex_sizes = [length(term) for term in complexes]
    n_complexes = length(complex_sizes)
    max_complex_size = isempty(complex_sizes) ? 0 : maximum(complex_sizes)
    assembly_depth = isempty(depths) ? 0 : maximum(values(depths))
    feature = Dict{String, Any}(
        "d" => _atlas_sqlite_int(_raw_get(entry, :base_species_count, nothing)),
        "r" => _atlas_sqlite_int(_raw_get(entry, :reaction_count, length(reactions))),
        "n_species" => _atlas_sqlite_int(_raw_get(entry, :total_species_count, nothing)),
        "n_complexes" => n_complexes,
        "max_complex_size" => max_complex_size,
        "max_reactant_complex_size" => max_reactant,
        "max_product_complex_size" => max_product,
        "mean_complex_size" => isempty(complex_sizes) ? 0.0 : Float64(sum(complex_sizes)) / length(complex_sizes),
        "assembly_depth" => assembly_depth,
        "uses_homomer" => uses_homomer,
        "uses_complex_growth" => uses_complex_growth,
        "uses_higher_order_template" => max_complex_size > 2,
        "graph_density" => n_complexes == 0 ? 0.0 : Float64(length(reactions)) / n_complexes,
        "closure_type" => String(_raw_get(entry, :source_kind, "binding")),
        "search_profile_id" => String(_raw_get(entry, :search_profile_id, "")),
    )
    _atlas_sqlite_execute(db,
        """
        INSERT INTO network_features (
            network_id, d, r, n_species, n_complexes, max_complex_size,
            max_reactant_complex_size, max_product_complex_size, mean_complex_size,
            assembly_depth, uses_homomer, uses_complex_growth, uses_higher_order_template,
            graph_density, closure_type, search_profile_id, feature_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(network_id) DO UPDATE SET
            d = excluded.d,
            r = excluded.r,
            n_species = excluded.n_species,
            n_complexes = excluded.n_complexes,
            max_complex_size = excluded.max_complex_size,
            max_reactant_complex_size = excluded.max_reactant_complex_size,
            max_product_complex_size = excluded.max_product_complex_size,
            mean_complex_size = excluded.mean_complex_size,
            assembly_depth = excluded.assembly_depth,
            uses_homomer = excluded.uses_homomer,
            uses_complex_growth = excluded.uses_complex_growth,
            uses_higher_order_template = excluded.uses_higher_order_template,
            graph_density = excluded.graph_density,
            closure_type = excluded.closure_type,
            search_profile_id = excluded.search_profile_id,
            feature_json = excluded.feature_json
        """,
        (
            String(_raw_get(entry, :network_id, canonical_code)),
            feature["d"],
            feature["r"],
            feature["n_species"],
            feature["n_complexes"],
            feature["max_complex_size"],
            feature["max_reactant_complex_size"],
            feature["max_product_complex_size"],
            feature["mean_complex_size"],
            feature["assembly_depth"],
            Bool(feature["uses_homomer"]) ? 1 : 0,
            Bool(feature["uses_complex_growth"]) ? 1 : 0,
            Bool(feature["uses_higher_order_template"]) ? 1 : 0,
            feature["graph_density"],
            feature["closure_type"],
            isempty(feature["search_profile_id"]) ? nothing : feature["search_profile_id"],
            _atlas_sqlite_json(feature),
        ),
    )
    return db
end

_atlas_sqlite_path_only_should_store(rec) = Bool(_raw_get(rec, :included, false))

function _atlas_sqlite_classifier_signature(slice)
    cfg_raw = _raw_get(slice, :classifier_config, Dict{String, Any}())
    cfg = atlas_behavior_config_from_raw(cfg_raw)
    return _config_signature(cfg)
end

function _atlas_sqlite_path_selector_mode(slice)
    slice_id = String(_raw_get(slice, :slice_id, ""))
    occursin("::change=", slice_id) && return UInt8(1)
    return UInt8(0)
end

function _atlas_sqlite_compact_path_id(rec, slice)
    network_id = String(_raw_get(slice, :network_id, ""))
    isempty(network_id) && error("path_only persist mode requires slice.network_id")
    selector_mode = _atlas_sqlite_path_selector_mode(slice)
    selector_value = selector_mode == UInt8(0) ? String(_raw_get(slice, :input_symbol, "")) : String(_raw_get(slice, :change_signature, ""))
    isempty(selector_value) && error("path_only persist mode requires a selector value")
    output_symbol = String(_raw_get(slice, :output_symbol, ""))
    cfg_signature = _atlas_sqlite_classifier_signature(slice)
    path_idx = max(0, Int(_raw_get(rec, :path_idx, 0)))

    network_buf = UInt8[]
    _atlas_sqlite_encode_network_id!(network_buf, network_id)

    selector_buf = UInt8[]
    _atlas_sqlite_push_text!(selector_buf, selector_value)

    output_buf = UInt8[]
    _atlas_sqlite_push_text!(output_buf, output_symbol)

    cfg_buf = UInt8[]
    _atlas_sqlite_encode_cfg!(cfg_buf, cfg_signature)

    selector_prefix = selector_mode == UInt8(0) ? "i" : "c"

    return join([
        "p3",
        _atlas_sqlite_base64url_encode(network_buf),
        selector_prefix * _atlas_sqlite_base64url_encode(selector_buf),
        _atlas_sqlite_base64url_encode(output_buf),
        _atlas_sqlite_base64url_encode(cfg_buf),
        _atlas_sqlite_base36_encode(path_idx),
    ], ".")
end

function _atlas_sqlite_direct_summary(db::SQLite.DB)
    created_at = something(_atlas_sqlite_metadata_text(db, "created_at"), _now_iso_timestamp())
    updated_at = something(_atlas_sqlite_metadata_text(db, "updated_at"), created_at)
    change_expansion_json = _atlas_sqlite_metadata_text(db, "change_expansion_json")
    change_expansion =
        change_expansion_json === nothing || isempty(change_expansion_json) ? Dict{String, Any}() : _atlas_sqlite_read_json(change_expansion_json)
    persist_mode = _atlas_sqlite_persist_mode_text(_atlas_sqlite_persist_mode(db))

    return Dict(
        "atlas_library_schema_version" => "0.2.0",
        "atlas_schema_version" => "0.2.0",
        "created_at" => created_at,
        "updated_at" => updated_at,
        "persist_mode" => persist_mode,
        "change_expansion" => change_expansion,
        "atlas_count" => _atlas_sqlite_count(db, "atlas_manifests"),
        "input_network_count" => _atlas_sqlite_metadata_int(db, "input_network_count", 0),
        "unique_network_count" => _atlas_sqlite_count(db, "network_entries"),
        "successful_network_count" => _atlas_sqlite_count(db, "network_entries"; where_clause="analysis_status = 'ok'"),
        "failed_network_count" => _atlas_sqlite_count(db, "network_entries"; where_clause="analysis_status = 'failed'"),
        "excluded_network_count" => _atlas_sqlite_count(db, "network_entries"; where_clause="analysis_status = 'excluded_by_search_profile'"),
        "deduplicated_network_count" => _atlas_sqlite_metadata_int(db, "deduplicated_network_count", 0),
        "input_graph_slice_count" => _atlas_sqlite_count(db, "input_graph_slices"),
        "behavior_slice_count" => _atlas_sqlite_count(db, "behavior_slices"),
        "regime_record_count" => _atlas_sqlite_count(db, "regime_records"),
        "transition_record_count" => _atlas_sqlite_count(db, "transition_records"),
        "family_bucket_count" => _atlas_sqlite_count(db, "family_buckets"),
        "path_record_count" => _atlas_sqlite_count(db, _atlas_sqlite_path_table(db)),
        "classifier_config_count" => _atlas_sqlite_count(db, "classifier_configs"),
        "behavior_program_count" => _atlas_sqlite_count(db, "behavior_programs"),
        "slice_program_support_count" => _atlas_sqlite_count(db, "slice_program_support"),
        "network_program_support_count" => _atlas_sqlite_count(db, "network_program_support"),
        "witness_path_count" => _atlas_sqlite_count(db, "witness_paths"),
    )
end

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

    if table == "path_records"
        return _atlas_sqlite_load_path_records(
            db;
            where_sql="$(id_column) IN (" * _atlas_sqlite_placeholder_list(length(unique_ids)) * ")",
            params=Tuple(unique_ids),
            order_column=order_column,
        )
    end

    records = Dict{String, Any}[]
    for start_idx in 1:batch_size:length(unique_ids)
        stop_idx = min(start_idx + batch_size - 1, length(unique_ids))
        batch = unique_ids[start_idx:stop_idx]
        sql = "SELECT record_json FROM $(table) WHERE $(id_column) IN (" *
              _atlas_sqlite_placeholder_list(length(batch)) * ") ORDER BY $(order_column)"
        query = _atlas_sqlite_query(db, sql, Tuple(batch))
        try
            for row in query
                push!(records, Dict{String, Any}(_atlas_sqlite_read_json(row[:record_json])))
            end
        finally
            query === nothing || DBInterface.close!(query)
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
    if !isempty(query.change_signatures)
        push!(clauses, "s.change_signature IN (" * _atlas_sqlite_placeholder_list(length(query.change_signatures)) * ")")
        append!(params, query.change_signatures)
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
    query_rows = _atlas_sqlite_query(db, sql, Tuple(params))
    try
        for row in query_rows
            push!(slice_ids, String(row[:slice_id]))
            push!(graph_slice_ids, String(row[:graph_slice_id]))
            push!(network_ids, String(row[:network_id]))
        end
    finally
        query_rows === nothing || DBInterface.close!(query_rows)
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
    query = _atlas_sqlite_query(db, sql)
    try
        for row in query
            push!(ids, String(row[:slice_id]))
        end
    finally
        query === nothing || DBInterface.close!(query)
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
        "path_only_records",
        "slice_program_support",
        "network_program_support",
        "witness_paths",
        "program_features",
        "behavior_programs",
        "classifier_configs",
        "network_features",
        "geometry_sidecar_meta",
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
            _atlas_sqlite_insert_network_features!(db, entry)
        end

        for item in collect(_raw_get(stored_library, :input_graph_slices, Any[]))
            _atlas_sqlite_execute(db,
                """
                INSERT INTO input_graph_slices (
                    graph_slice_id, network_id, input_symbol, change_signature, vertex_count, edge_count, path_count, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :graph_slice_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
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
                    slice_id, network_id, graph_slice_id, input_symbol, change_signature, output_symbol, analysis_status,
                    path_scope, min_volume_mean, total_paths, feasible_paths, included_paths, excluded_paths,
                    motif_union_json, exact_union_json, classifier_config_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(slice, :slice_id, "")),
                    _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :change_signature, nothing)),
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
                    regime_record_id, slice_id, graph_slice_id, network_id, input_symbol, change_signature, output_symbol,
                    vertex_idx, role, singular, nullity, asymptotic, output_order_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :regime_record_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
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
                    transition_record_id, slice_id, graph_slice_id, input_symbol, change_signature, output_symbol,
                    from_vertex_idx, to_vertex_idx, from_role, to_role,
                    from_output_order_token, to_output_order_token, transition_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :transition_record_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
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
            if _atlas_sqlite_persist_mode(db) === :path_only
                _atlas_sqlite_path_only_should_store(rec) || continue
                _atlas_sqlite_execute(
                    db,
                    "INSERT INTO path_only_records (path_record_id, behavior_code) VALUES (?, ?)",
                    (
                        String(_raw_get(rec, :path_record_id, "")),
                        _atlas_sqlite_behavior_code(rec),
                    ),
                )
            else
                _atlas_sqlite_execute(db,
                    """
                    INSERT INTO path_records (
                        path_record_id, behavior_code, slice_id, graph_slice_id, network_id, input_symbol, change_signature, output_symbol,
                        path_idx, path_length, exact_label, motif_label, feasible, robust, volume_mean,
                        output_order_tokens_json, transition_tokens_json, record_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        String(_raw_get(rec, :path_record_id, "")),
                        _atlas_sqlite_text(get(rec, "behavior_code", nothing)),
                        _atlas_sqlite_text(_raw_get(rec, :slice_id, nothing)),
                        _atlas_sqlite_text(_raw_get(rec, :graph_slice_id, nothing)),
                        _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                        _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                        _atlas_sqlite_text(_raw_get(slice, :change_signature, nothing)),
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

function atlas_sqlite_record_skip_only_event!(db::SQLite.DB; source_label=nothing, source_metadata=nothing, skipped_existing_network_count::Int=0, skipped_existing_slice_count::Int=0, persist_mode=nothing)
    persist_mode_symbol = _atlas_sqlite_persist_mode(db; override=persist_mode)
    if persist_mode_symbol !== :full
        return _atlas_sqlite_transaction(db) do
            atlas_sqlite_init!(db)
            timestamp = _now_iso_timestamp()
            _atlas_sqlite_set_metadata!(db, "created_at", something(_atlas_sqlite_metadata_text(db, "created_at"), timestamp))
            _atlas_sqlite_set_metadata!(db, "updated_at", timestamp)
            _atlas_sqlite_set_metadata!(db, "persist_mode", _atlas_sqlite_persist_mode_text(persist_mode_symbol))
            _atlas_sqlite_execute(db,
                "INSERT INTO merge_events (merged_at, status, atlas_id, source_label, event_json) VALUES (?, ?, ?, ?, ?)",
                (
                    timestamp,
                    "skipped_all_existing",
                    nothing,
                    source_label === nothing ? "atlas_spec" : String(source_label),
                    _atlas_sqlite_json(Dict(
                        "merged_at" => timestamp,
                        "status" => "skipped_all_existing",
                        "source_label" => source_label === nothing ? "atlas_spec" : String(source_label),
                        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
                        "skipped_existing_network_count" => skipped_existing_network_count,
                        "skipped_existing_slice_count" => skipped_existing_slice_count,
                    )),
                ),
            )
            return _atlas_sqlite_direct_summary(db)
        end
    end
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

function atlas_sqlite_merge_atlas!(db::SQLite.DB, atlas; source_label=nothing, source_metadata=nothing, library_label=nothing, allow_duplicate_atlas::Bool=false, persist_mode=nothing)
    persist_mode_symbol = _atlas_sqlite_persist_mode(db; override=persist_mode)
    if persist_mode_symbol !== :full
        return atlas_sqlite_append_atlas!(db, atlas;
            source_label=source_label,
            source_metadata=source_metadata,
            library_label=library_label,
            return_summary=true,
            persist_mode=persist_mode_symbol,
        )
    end
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

function _build_atlas_manifest_fast(atlas; source_label=nothing, source_metadata=nothing)
    imported_at = _now_iso_timestamp()
    summary = _atlas_summary(atlas)
    source_text = source_label === nothing ? "atlas_spec" : String(source_label)
    atlas_id = bytes2hex(SHA.sha1(string(source_text, "|", imported_at, "|", rand(UInt64))))
    default_label = "atlas_" * atlas_id[1:12]
    return Dict(
        "atlas_id" => atlas_id,
        "source_label" => source_label === nothing ? default_label : source_text,
        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
        "imported_at" => imported_at,
        "atlas_schema_version" => String(_raw_get(atlas, :atlas_schema_version, "unknown")),
        "generated_at" => String(_raw_get(atlas, :generated_at, "unknown")),
        "search_profile" => _materialize(_raw_get(atlas, :search_profile, Dict{String, Any}())),
        "behavior_config" => _materialize(_raw_get(atlas, :behavior_config, Dict{String, Any}())),
        "change_expansion" => _materialize(_raw_get(atlas, :change_expansion, Dict{String, Any}())),
        "network_parallelism" => Int(_raw_get(atlas, :network_parallelism, 1)),
        "input_network_count" => Int(_raw_get(summary, :input_network_count, 0)),
        "unique_network_count" => Int(_raw_get(summary, :unique_network_count, 0)),
        "successful_network_count" => Int(_raw_get(summary, :successful_network_count, 0)),
        "failed_network_count" => Int(_raw_get(summary, :failed_network_count, 0)),
        "excluded_network_count" => Int(_raw_get(summary, :excluded_network_count, 0)),
        "deduplicated_network_count" => Int(_raw_get(summary, :deduplicated_network_count, 0)),
        "skipped_existing_network_count" => Int(_raw_get(summary, :skipped_existing_network_count, 0)),
        "skipped_existing_slice_count" => Int(_raw_get(summary, :skipped_existing_slice_count, 0)),
        "input_graph_slice_count" => Int(_raw_get(summary, :input_graph_slice_count, 0)),
        "behavior_slice_count" => Int(_raw_get(summary, :behavior_slice_count, 0)),
        "regime_record_count" => Int(_raw_get(summary, :regime_record_count, 0)),
        "transition_record_count" => Int(_raw_get(summary, :transition_record_count, 0)),
        "family_bucket_count" => Int(_raw_get(summary, :family_bucket_count, 0)),
        "path_record_count" => Int(_raw_get(summary, :path_record_count, 0)),
    )
end

function atlas_sqlite_append_atlas!(db::SQLite.DB, atlas; source_label=nothing, source_metadata=nothing, library_label=nothing, return_summary::Bool=true, persist_mode=nothing)
    atlas_sqlite_init!(db)
    atlas_summary = _atlas_summary(atlas)
    persist_mode_symbol = _atlas_sqlite_persist_mode(db; override=persist_mode)

    return _atlas_sqlite_transaction(db) do
        if _is_empty_atlas_delta(atlas) && Int(_raw_get(atlas_summary, :skipped_existing_slice_count, 0)) > 0
            _atlas_sqlite_set_metadata!(db, "created_at", something(_atlas_sqlite_metadata_text(db, "created_at"), _now_iso_timestamp()))
            _atlas_sqlite_set_metadata!(db, "updated_at", _now_iso_timestamp())
            _atlas_sqlite_execute(db,
                "INSERT INTO merge_events (merged_at, status, atlas_id, source_label, event_json) VALUES (?, ?, ?, ?, ?)",
                (
                    _now_iso_timestamp(),
                    "skipped_all_existing",
                    nothing,
                    source_label === nothing ? "atlas_spec" : String(source_label),
                    _atlas_sqlite_json(Dict(
                        "merged_at" => _now_iso_timestamp(),
                        "status" => "skipped_all_existing",
                        "source_label" => source_label === nothing ? "atlas_spec" : String(source_label),
                        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
                        "skipped_existing_network_count" => Int(_raw_get(atlas_summary, :skipped_existing_network_count, 0)),
                        "skipped_existing_slice_count" => Int(_raw_get(atlas_summary, :skipped_existing_slice_count, 0)),
                    )),
                ),
            )
            return return_summary ? _atlas_sqlite_direct_summary(db) : nothing
        end

        manifest = _build_atlas_manifest_fast(atlas; source_label=source_label, source_metadata=source_metadata)
        atlas_id = manifest["atlas_id"]
        slice_index = _atlas_slice_index(collect(_raw_get(atlas, :behavior_slices, Any[])))

        _atlas_sqlite_execute(db,
            "INSERT INTO atlas_manifests (atlas_id, source_label, imported_at, generated_at, behavior_slice_count, manifest_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                atlas_id,
                _atlas_sqlite_text(_raw_get(manifest, :source_label, nothing)),
                _atlas_sqlite_text(_raw_get(manifest, :imported_at, nothing)),
                _atlas_sqlite_text(_raw_get(manifest, :generated_at, nothing)),
                _atlas_sqlite_int(_raw_get(manifest, :behavior_slice_count, nothing)),
                _atlas_sqlite_json(manifest),
            ),
        )

        _atlas_sqlite_execute(db,
            "INSERT INTO merge_events (merged_at, status, atlas_id, source_label, event_json) VALUES (?, ?, ?, ?, ?)",
            (
                _now_iso_timestamp(),
                "merged",
                atlas_id,
                _atlas_sqlite_text(_raw_get(manifest, :source_label, nothing)),
                _atlas_sqlite_json(Dict(
                    "merged_at" => _now_iso_timestamp(),
                    "status" => "merged",
                    "atlas_id" => atlas_id,
                    "source_label" => _raw_get(manifest, :source_label, nothing),
                    "source_metadata" => _raw_get(manifest, :source_metadata, nothing),
                    "added_network_count" => Int(_raw_get(atlas_summary, :unique_network_count, 0)),
                    "added_input_graph_slice_count" => Int(_raw_get(atlas_summary, :input_graph_slice_count, 0)),
                    "added_slice_count" => Int(_raw_get(atlas_summary, :behavior_slice_count, 0)),
                    "added_regime_record_count" => Int(_raw_get(atlas_summary, :regime_record_count, 0)),
                    "added_transition_record_count" => Int(_raw_get(atlas_summary, :transition_record_count, 0)),
                    "added_family_bucket_count" => Int(_raw_get(atlas_summary, :family_bucket_count, 0)),
                    "added_path_record_count" => Int(_raw_get(atlas_summary, :path_record_count, 0)),
                    "skipped_existing_network_count" => Int(_raw_get(atlas_summary, :skipped_existing_network_count, 0)),
                    "skipped_existing_slice_count" => Int(_raw_get(atlas_summary, :skipped_existing_slice_count, 0)),
                )),
            ),
        )

        for entry in collect(_raw_get(atlas, :network_entries, Any[]))
            _atlas_sqlite_skip_table_in_mode(:network_entries, persist_mode_symbol) && break
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO network_entries (
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
                    _atlas_sqlite_record_payload(:network_entries, entry; persist_mode=persist_mode_symbol),
                ),
            )
            _atlas_sqlite_insert_network_features!(db, entry)
        end

        for item in collect(_raw_get(atlas, :input_graph_slices, Any[]))
            _atlas_sqlite_skip_table_in_mode(:input_graph_slices, persist_mode_symbol) && break
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO input_graph_slices (
                    graph_slice_id, network_id, input_symbol, change_signature, vertex_count, edge_count, path_count, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :graph_slice_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :vertex_count, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :edge_count, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :path_count, nothing)),
                    _atlas_sqlite_record_payload(:input_graph_slices, item; persist_mode=persist_mode_symbol),
                ),
            )
        end

        for slice in collect(_raw_get(atlas, :behavior_slices, Any[]))
            _atlas_sqlite_skip_table_in_mode(:behavior_slices, persist_mode_symbol) && break
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO behavior_slices (
                    slice_id, network_id, graph_slice_id, input_symbol, change_signature, output_symbol, analysis_status,
                    path_scope, min_volume_mean, total_paths, feasible_paths, included_paths, excluded_paths,
                    motif_union_json, exact_union_json, classifier_config_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(slice, :slice_id, "")),
                    _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(slice, :change_signature, nothing)),
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
                    _atlas_sqlite_record_payload(:behavior_slices, slice; persist_mode=persist_mode_symbol),
                ),
            )
        end

        for item in collect(_raw_get(atlas, :regime_records, Any[]))
            _atlas_sqlite_skip_table_in_mode(:regime_records, persist_mode_symbol) && break
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO regime_records (
                    regime_record_id, slice_id, graph_slice_id, network_id, input_symbol, change_signature, output_symbol,
                    vertex_idx, role, singular, nullity, asymptotic, output_order_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    String(_raw_get(item, :regime_record_id, "")),
                    _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :network_id, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :output_symbol, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :vertex_idx, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :role, nothing)),
                    _atlas_sqlite_bool(_raw_get(item, :singular, nothing)),
                    _atlas_sqlite_int(_raw_get(item, :nullity, nothing)),
                    _atlas_sqlite_bool(_raw_get(item, :asymptotic, nothing)),
                    _atlas_sqlite_text(_raw_get(item, :output_order_token, nothing)),
                    _atlas_sqlite_record_payload(:regime_records, item; persist_mode=persist_mode_symbol),
                ),
            )
        end

        if !_atlas_sqlite_skip_table_in_mode(:transition_records, persist_mode_symbol)
            for item in collect(_raw_get(atlas, :transition_records, Any[]))
                _atlas_sqlite_execute(db,
                    """
                    INSERT OR IGNORE INTO transition_records (
                        transition_record_id, slice_id, graph_slice_id, input_symbol, change_signature, output_symbol,
                        from_vertex_idx, to_vertex_idx, from_role, to_role,
                        from_output_order_token, to_output_order_token, transition_token, record_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        String(_raw_get(item, :transition_record_id, "")),
                        _atlas_sqlite_text(_raw_get(item, :slice_id, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :graph_slice_id, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :input_symbol, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :change_signature, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :output_symbol, nothing)),
                        _atlas_sqlite_int(_raw_get(item, :from_vertex_idx, nothing)),
                        _atlas_sqlite_int(_raw_get(item, :to_vertex_idx, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :from_role, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :to_role, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :from_output_order_token, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :to_output_order_token, nothing)),
                        _atlas_sqlite_text(_raw_get(item, :transition_token, nothing)),
                        _atlas_sqlite_record_payload(:transition_records, item; persist_mode=persist_mode_symbol),
                    ),
                )
            end
        end

        for bucket in collect(_raw_get(atlas, :family_buckets, Any[]))
            _atlas_sqlite_skip_table_in_mode(:family_buckets, persist_mode_symbol) && break
            slice = get(slice_index, String(_raw_get(bucket, :slice_id, "")), Dict{String, Any}())
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO family_buckets (
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
                    _atlas_sqlite_record_payload(:family_buckets, bucket; slice=slice, persist_mode=persist_mode_symbol),
                ),
            )
        end

        persist_mode_symbol === :behavior_aggregate && _atlas_sqlite_write_behavior_aggregate!(db, atlas, slice_index)

        if !_atlas_sqlite_skip_table_in_mode(:path_records, persist_mode_symbol)
            for rec in collect(_raw_get(atlas, :path_records, Any[]))
                slice = get(slice_index, String(_raw_get(rec, :slice_id, "")), Dict{String, Any}())
                compact_path_id = persist_mode_symbol === :path_only ? _atlas_sqlite_compact_path_id(rec, slice) : String(_raw_get(rec, :path_record_id, ""))
                if persist_mode_symbol === :path_only
                    _atlas_sqlite_path_only_should_store(rec) || continue
                    _atlas_sqlite_execute(
                        db,
                        "INSERT OR IGNORE INTO path_only_records (path_record_id, behavior_code) VALUES (?, ?)",
                        (
                            compact_path_id,
                            _atlas_sqlite_behavior_code(rec),
                        ),
                    )
                else
                    _atlas_sqlite_execute(db,
                        """
                        INSERT OR IGNORE INTO path_records (
                            path_record_id, behavior_code, slice_id, graph_slice_id, network_id, input_symbol, change_signature, output_symbol,
                            path_idx, path_length, exact_label, motif_label, feasible, robust, volume_mean,
                            output_order_tokens_json, transition_tokens_json, record_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            compact_path_id,
                            _atlas_sqlite_behavior_code(rec),
                            _atlas_sqlite_text(_raw_get(rec, :slice_id, nothing)),
                            _atlas_sqlite_text(_raw_get(rec, :graph_slice_id, nothing)),
                            _atlas_sqlite_text(_raw_get(slice, :network_id, nothing)),
                            _atlas_sqlite_text(_raw_get(slice, :input_symbol, nothing)),
                            _atlas_sqlite_text(_raw_get(slice, :change_signature, nothing)),
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
                            _atlas_sqlite_record_payload(:path_records, rec; slice=slice, persist_mode=persist_mode_symbol),
                        ),
                    )
                end
            end
        end

        for raw_dup in collect(_raw_get(atlas, :duplicate_inputs, Any[]))
            _atlas_sqlite_skip_table_in_mode(:duplicate_inputs, persist_mode_symbol) && break
            dup = _materialize(raw_dup)
            duplicate_key = bytes2hex(SHA.sha1(_atlas_sqlite_json(dup)))
            _atlas_sqlite_execute(db,
                "INSERT OR IGNORE INTO duplicate_inputs (duplicate_key, source_label, duplicate_of_network_id, record_json) VALUES (?, ?, ?, ?)",
                (
                    duplicate_key,
                    _atlas_sqlite_text(_raw_get(dup, :source_label, nothing)),
                    _atlas_sqlite_text(_raw_get(dup, :duplicate_of_network_id, nothing)),
                    _atlas_sqlite_json(dup),
                ),
            )
        end

        _atlas_sqlite_set_metadata!(db, "created_at", something(_atlas_sqlite_metadata_text(db, "created_at"), _now_iso_timestamp()))
        _atlas_sqlite_set_metadata!(db, "updated_at", _now_iso_timestamp())
        _atlas_sqlite_set_metadata!(db, "change_expansion_json", _atlas_sqlite_json(_raw_get(atlas, :change_expansion, Dict{String, Any}())))
        persist_mode_symbol === :full || _atlas_sqlite_set_metadata!(db, "persist_mode", _atlas_sqlite_persist_mode_text(persist_mode_symbol))
        persist_mode_symbol === :path_only && _atlas_sqlite_set_metadata!(db, "path_id_scheme", "stable_path_v3_segmented")
        persist_mode_symbol === :path_only && _atlas_sqlite_set_metadata!(db, "path_record_scope", "included_only")
        persist_mode_symbol === :path_only && _atlas_sqlite_set_metadata!(db, "behavior_code_scheme", "exact_profile_tokens_v3")
        if persist_mode_symbol === :behavior_aggregate
            _atlas_sqlite_set_metadata!(db, "program_codec_version", "RPB1")
            _atlas_sqlite_set_metadata!(db, "program_identity_scheme", DEFAULT_PROGRAM_IDENTITY)
            _atlas_sqlite_set_metadata!(db, "support_semantics", DEFAULT_SUPPORT_SEMANTICS)
            _atlas_sqlite_set_metadata!(db, "path_record_scope", "not_persisted")
            _atlas_sqlite_set_metadata!(db, "library_json_snapshot", "disabled")
        end
        library_label === nothing || _atlas_sqlite_set_metadata!(db, "library_label", String(library_label))
        _atlas_sqlite_set_metadata!(db, "input_network_count",
            string(_atlas_sqlite_metadata_int(db, "input_network_count", 0) + Int(_raw_get(atlas_summary, :input_network_count, 0))))
        _atlas_sqlite_set_metadata!(db, "deduplicated_network_count",
            string(_atlas_sqlite_metadata_int(db, "deduplicated_network_count", 0) + Int(_raw_get(atlas_summary, :deduplicated_network_count, 0))))

        return return_summary ? _atlas_sqlite_direct_summary(db) : nothing
    end
end

function atlas_sqlite_append_atlas!(db_path::AbstractString, atlas; kwargs...)
    return _atlas_sqlite_with_db(db -> atlas_sqlite_append_atlas!(db, atlas; kwargs...), db_path)
end
