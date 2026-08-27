const ATLAS_SQLITE_SCHEMA_VERSION = "0.5.0"
const ATLAS_SQLITE_SELECT_BATCH_SIZE = 400
const ATLAS_SQLITE_BUSY_TIMEOUT_MS = 120000
const ATLAS_SQLITE_LOCK_RETRY_DELAYS = (0.1, 0.25, 0.5, 1.0, 2.0, 4.0)
const ATLAS_SQLITE_LIGHTWEIGHT_ENV = Config.ATLAS_SQLITE_LIGHTWEIGHT_ENV
const ATLAS_SQLITE_PERSIST_MODE_ENV = Config.ATLAS_SQLITE_PERSIST_MODE_ENV

mutable struct _AtlasSqliteWriteLockEntry
    lock::ReentrantLock
    users::Int
end

const _ATLAS_SQLITE_WRITE_LOCKS = Dict{String, _AtlasSqliteWriteLockEntry}()
const _ATLAS_SQLITE_WRITE_LOCKS_GUARD = ReentrantLock()

function _atlas_sqlite_lock_key(path::AbstractString)
    text = String(path)
    text == ":memory:" && return text
    return abspath(expanduser(text))
end

function _atlas_sqlite_lock_key(db::SQLite.DB)
    path = String(getfield(db, :file))
    path == ":memory:" && return "memory:$(objectid(db))"
    return _atlas_sqlite_lock_key(path)
end

function _with_atlas_sqlite_write_lock(f::Function, db_or_path)
    key = _atlas_sqlite_lock_key(db_or_path)
    entry = lock(_ATLAS_SQLITE_WRITE_LOCKS_GUARD) do
        current = get!(_ATLAS_SQLITE_WRITE_LOCKS, key) do
            _AtlasSqliteWriteLockEntry(ReentrantLock(), 0)
        end
        current.users += 1
        current
    end
    acquired = false
    try
        lock(entry.lock)
        acquired = true
        return f()
    finally
        acquired && unlock(entry.lock)
        lock(_ATLAS_SQLITE_WRITE_LOCKS_GUARD) do
            entry.users -= 1
            entry.users == 0 && get(_ATLAS_SQLITE_WRITE_LOCKS, key, nothing) === entry &&
                delete!(_ATLAS_SQLITE_WRITE_LOCKS, key)
        end
    end
end

function atlas_sqlite_default_path()
    configured_root = Config.atlas_store_root_override()
    root = isempty(configured_root) ?
        joinpath(@__DIR__, "..", "atlas_store") : configured_root
    return normpath(joinpath(root, "atlas.sqlite"))
end

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
    if SQLite.intransaction(db)
        savepoint = "bcx_nested_" * string(rand(UInt64), base=16)
        _atlas_sqlite_execute(db, "SAVEPOINT $savepoint")
        try
            result = f()
            _atlas_sqlite_execute(db, "RELEASE SAVEPOINT $savepoint")
            return result
        catch err
            try
                _atlas_sqlite_execute(db, "ROLLBACK TO SAVEPOINT $savepoint")
                _atlas_sqlite_execute(db, "RELEASE SAVEPOINT $savepoint")
            catch
            end
            rethrow(err)
        end
    end
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
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) " *
        "ON CONFLICT(key) DO NOTHING",
        # This function is the historical 0.3 baseline.  The 0.4 migration
        # advances metadata inside its own transaction only after every new
        # table and index succeeds.
        ("schema_version", "0.3.0"),
    )
    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("updated_at", _now_iso_timestamp()),
    )
    apply_atlas_sqlite_migrations!(db)
    return db
end

# ─── Migration framework ───
# Adding new schema changes:
#   1. Append an `AtlasSqliteMigration` entry below with a fresh version tag.
#      Use a monotonically increasing `MAJOR.MINOR.PATCH` string (must compare
#      correctly with `cmp` lexicographically — pad numbers if needed).
#   2. The `apply` function gets a `SQLite.DB` and may issue any DDL/DML; it
#      runs inside a transaction. Make it idempotent when feasible
#      (`CREATE TABLE IF NOT EXISTS`, guarded ALTERs) so re-runs are safe.
#   3. Do not edit `atlas_sqlite_init!` for new schema — that captures the
#      historical baseline and existing databases will not re-run it.

struct AtlasSqliteMigration
    version::String
    description::String
    apply::Function   # (db::SQLite.DB) -> Any
end

function _apply_atlas_sqlite_0_4_0!(db::SQLite.DB)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS ro_cell_complex_identities (
            cell_complex_hash TEXT PRIMARY KEY,
            codec_magic TEXT NOT NULL CHECK(codec_magic = 'RPB2'),
            codec_version INTEGER NOT NULL,
            identity_kind TEXT NOT NULL CHECK(identity_kind = 'exact_cell_complex_v1'),
            blob BLOB NOT NULL,
            axis_count INTEGER NOT NULL,
            output_count INTEGER NOT NULL,
            cell_count INTEGER NOT NULL,
            facet_count INTEGER NOT NULL,
            singular_stratum_count INTEGER NOT NULL
        )
        """)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS ro_field_artifacts (
            artifact_sha256 TEXT PRIMARY KEY,
            field_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            representation TEXT NOT NULL,
            network_ir_sha256 TEXT NOT NULL,
            domain_sha256 TEXT NOT NULL,
            data_sha256 TEXT NOT NULL,
            cell_complex_hash TEXT REFERENCES ro_cell_complex_identities(cell_complex_hash),
            partial INTEGER NOT NULL CHECK(partial IN (0, 1)),
            storage_mode TEXT NOT NULL CHECK(storage_mode = 'inline'),
            axis_count INTEGER NOT NULL,
            output_count INTEGER NOT NULL,
            eligible_count INTEGER NOT NULL,
            evaluated_count INTEGER NOT NULL,
            valid_count INTEGER NOT NULL,
            invalid_count INTEGER NOT NULL,
            omitted_count INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            field_json TEXT NOT NULL
        )
        """)
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_fields_network_representation " *
        "ON ro_field_artifacts (network_ir_sha256, representation)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_fields_domain_sha256 " *
        "ON ro_field_artifacts (domain_sha256)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_fields_field_id " *
        "ON ro_field_artifacts (field_id)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_fields_cell_complex_hash " *
        "ON ro_field_artifacts (cell_complex_hash)")
    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) " *
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("schema_version", "0.4.0"),
    )
    return db
end

function _apply_atlas_sqlite_0_5_0!(db::SQLite.DB)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS ro_field_signatures (
            signature_sha256 TEXT PRIMARY KEY,
            artifact_sha256 TEXT NOT NULL
                REFERENCES ro_field_artifacts(artifact_sha256) ON DELETE CASCADE,
            schema_version TEXT NOT NULL
                CHECK(schema_version = 'bne-ro-field-signature/v1.0.0'),
            classifier_version TEXT NOT NULL
                CHECK(classifier_version = 'regular-cell-gradient/v1.0.0'),
            scope TEXT NOT NULL
                CHECK(scope = 'regular_cell_interiors_excluding_declared_lower_dimensional_strata'),
            config_sha256 TEXT NOT NULL,
            signature_json_sha256 TEXT NOT NULL,
            classifiable INTEGER NOT NULL CHECK(classifiable IN (0, 1)),
            zero_tolerance REAL NOT NULL,
            max_cells INTEGER NOT NULL,
            max_facets INTEGER NOT NULL,
            max_matrix_elements INTEGER NOT NULL,
            axis_count INTEGER NOT NULL,
            output_count INTEGER NOT NULL,
            internal_facet_count INTEGER NOT NULL,
            mixed_sign_facet_count INTEGER NOT NULL,
            coupled_jump_count INTEGER NOT NULL,
            excluded_stratum_count INTEGER NOT NULL,
            signature_json TEXT NOT NULL,
            UNIQUE(artifact_sha256, classifier_version, config_sha256)
        )
        """)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS ro_field_component_features (
            signature_sha256 TEXT NOT NULL
                REFERENCES ro_field_signatures(signature_sha256) ON DELETE CASCADE,
            output_position INTEGER NOT NULL CHECK(output_position >= 1),
            axis_position INTEGER NOT NULL CHECK(axis_position >= 1),
            output_id TEXT NOT NULL,
            axis_id TEXT NOT NULL,
            classification TEXT NOT NULL CHECK(classification IN (
                'zero', 'strictly_positive', 'nonnegative_variable',
                'strictly_negative', 'nonpositive_variable',
                'sign_changing', 'unknown'
            )),
            PRIMARY KEY (signature_sha256, output_position, axis_position)
        ) WITHOUT ROWID
        """)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS ro_field_output_gradient_features (
            signature_sha256 TEXT NOT NULL
                REFERENCES ro_field_signatures(signature_sha256) ON DELETE CASCADE,
            output_position INTEGER NOT NULL CHECK(output_position >= 1),
            output_id TEXT NOT NULL,
            gradient_family TEXT NOT NULL CHECK(gradient_family IN (
                'all_zero', 'all_nonnegative', 'all_nonpositive',
                'opposed_axis_signs', 'sign_changing', 'other_mixed', 'unknown'
            )),
            PRIMARY KEY (signature_sha256, output_position)
        ) WITHOUT ROWID
        """)
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_field_signatures_artifact " *
        "ON ro_field_signatures (artifact_sha256)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_field_signatures_classifiable " *
        "ON ro_field_signatures (classifiable, signature_sha256)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_field_components_classification " *
        "ON ro_field_component_features " *
        "(classification, output_id, axis_id, signature_sha256)")
    _atlas_sqlite_execute(db,
        "CREATE INDEX IF NOT EXISTS idx_ro_field_gradients_family " *
        "ON ro_field_output_gradient_features " *
        "(gradient_family, output_id, signature_sha256)")
    _atlas_sqlite_execute(db,
        "INSERT INTO atlas_metadata (key, value_text) VALUES (?, ?) " *
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("schema_version", "0.5.0"),
    )
    return db
end

const ATLAS_SQLITE_MIGRATIONS = AtlasSqliteMigration[
    AtlasSqliteMigration("0.3.0",
        "Initial schema captured by atlas_sqlite_init!",
        _ -> nothing),
    AtlasSqliteMigration("0.4.0",
        "Add inline multi-input RO-field artifacts and RPB2 exact-cell-complex identities",
        _apply_atlas_sqlite_0_4_0!),
    AtlasSqliteMigration("0.5.0",
        "Add normalized, artifact-bound multi-input RO-field behavior signatures",
        _apply_atlas_sqlite_0_5_0!),
]

function _ensure_schema_migrations_table!(db::SQLite.DB)
    _atlas_sqlite_execute(db,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,
            applied_at TEXT NOT NULL,
            description TEXT
        )
        """)
    return db
end

function _applied_migration_versions(db::SQLite.DB)
    rows = _atlas_sqlite_query(db, "SELECT version FROM schema_migrations")
    versions = Set{String}()
    try
        for row in rows
            push!(versions, String(row.version))
        end
    finally
        DBInterface.close!(rows)
    end
    return versions
end

function _record_migration!(db::SQLite.DB, m::AtlasSqliteMigration)
    _atlas_sqlite_execute(db,
        "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) VALUES (?, ?, ?)",
        (m.version, _now_iso_timestamp(), m.description))
end

function apply_atlas_sqlite_migrations!(db::SQLite.DB;
                                        migrations::AbstractVector{AtlasSqliteMigration} = ATLAS_SQLITE_MIGRATIONS)
    _ensure_schema_migrations_table!(db)
    applied = _applied_migration_versions(db)

    # If this database predates the migrations table, atlas_sqlite_init! has
    # already created the baseline schema via CREATE IF NOT EXISTS; just stamp
    # the baseline as applied so future migrations have a stable starting
    # point. Without this, a 0.4.0 migration shipped later would think the DB
    # is brand-new and re-attempt baseline DDL it can skip.
    baseline_query = _atlas_sqlite_query(db,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='atlas_metadata'")
    baseline_present = try
        !isempty(baseline_query)
    finally
        DBInterface.close!(baseline_query)
    end
    if isempty(applied) && baseline_present && !isempty(migrations)
        _record_migration!(db, first(migrations))
        push!(applied, first(migrations).version)
    end

    applied_now = String[]
    for m in migrations
        m.version in applied && continue
        _atlas_sqlite_transaction(db) do
            m.apply(db)
            _record_migration!(db, m)
        end
        push!(applied, m.version)
        push!(applied_now, m.version)
    end
    return applied_now
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

    env_mode = _atlas_sqlite_parse_persist_mode(Config.atlas_sqlite_persist_mode_raw())
    env_mode === nothing || return env_mode

    env_value = Config.atlas_sqlite_lightweight_raw()
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

function atlas_sqlite_save_library!(db::SQLite.DB, library; already_in_transaction::Bool=false)
    stored_library = _refresh_atlas_library!(_materialize(library))
    is_atlas_library(stored_library) || error("atlas_sqlite_save_library! expects an atlas library object.")
    summary = _atlas_library_summary(stored_library)
    slice_index = _atlas_slice_index(collect(_raw_get(stored_library, :behavior_slices, Any[])))

    save_snapshot! = function ()
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
    already_in_transaction ? save_snapshot!() : _atlas_sqlite_transaction(save_snapshot!, db)

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
    return _with_atlas_sqlite_write_lock(db) do
        _atlas_sqlite_transaction(db) do
            library = atlas_sqlite_has_library(db) ? atlas_sqlite_load_library(db) : atlas_library_default()
            updated = _record_library_skip_only_event(library;
                source_label=source_label,
                source_metadata=source_metadata,
                skipped_existing_network_count=skipped_existing_network_count,
                skipped_existing_slice_count=skipped_existing_slice_count,
            )
            atlas_sqlite_save_library!(db, updated; already_in_transaction=true)
        end
    end
end

function atlas_sqlite_record_skip_only_event!(db_path::AbstractString; kwargs...)
    return _with_atlas_sqlite_write_lock(db_path) do
        _atlas_sqlite_with_db(db -> atlas_sqlite_record_skip_only_event!(db; kwargs...), db_path)
    end
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
    return _with_atlas_sqlite_write_lock(db) do
        _atlas_sqlite_transaction(db) do
            has_library = atlas_sqlite_has_library(db)
            library = has_library ? atlas_sqlite_load_library(db) : atlas_library_default()
            if !has_library && library_label !== nothing
                library["library_label"] = String(library_label)
            end
            merged = merge_atlas_library(library, atlas;
                source_label=source_label,
                source_metadata=source_metadata,
                allow_duplicate_atlas=allow_duplicate_atlas,
            )
            atlas_sqlite_save_library!(db, merged; already_in_transaction=true)
        end
    end
end

function atlas_sqlite_merge_atlas!(db_path::AbstractString, atlas; kwargs...)
    return _with_atlas_sqlite_write_lock(db_path) do
        _atlas_sqlite_with_db(db -> atlas_sqlite_merge_atlas!(db, atlas; kwargs...), db_path)
    end
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

# ─── Multi-input reaction-order field artifacts (schema 0.4.0) ──────────────

function _atlas_sqlite_ro_field_counts(document)
    domain = _ro_field_identity_get(document, "domain")
    outputs = _ro_field_identity_get(document, "outputs")
    coverage = _ro_field_identity_get(document, "coverage")
    return (
        axis_count=length(_ro_field_identity_vector(domain, "axis_order")),
        output_count=length(_ro_field_identity_vector(outputs, "output_order")),
        eligible_count=_ro_field_identity_int(coverage, "eligible_count"),
        evaluated_count=_ro_field_identity_int(coverage, "evaluated_count"),
        valid_count=_ro_field_identity_int(coverage, "valid_count"),
        invalid_count=_ro_field_identity_int(coverage, "invalid_count"),
        omitted_count=_ro_field_identity_int(coverage, "omitted_count"),
    )
end

function _atlas_sqlite_single_ro_field_row(db::SQLite.DB, artifact_sha256::AbstractString)
    query = _atlas_sqlite_query(db,
        "SELECT * FROM ro_field_artifacts WHERE artifact_sha256 = ? LIMIT 1",
        (String(artifact_sha256),),
    )
    try
        for row in query
            return Dict{String, Any}(
                String(name) => row[name] for name in propertynames(row)
            )
        end
    finally
        DBInterface.close!(query)
    end
    return nothing
end

function _atlas_sqlite_single_ro_identity_row(db::SQLite.DB, cell_complex_hash::AbstractString)
    query = _atlas_sqlite_query(db,
        "SELECT * FROM ro_cell_complex_identities WHERE cell_complex_hash = ? LIMIT 1",
        (String(cell_complex_hash),),
    )
    try
        for row in query
            return Dict{String, Any}(
                String(name) => row[name] for name in propertynames(row)
            )
        end
    finally
        DBInterface.close!(query)
    end
    return nothing
end

function _atlas_sqlite_ro_field_row_string(row, key::AbstractString)
    value = row[String(key)]
    _atlas_sqlite_is_nullish(value) && return nothing
    return String(value)
end

function _atlas_sqlite_ro_field_row_int(row, key::AbstractString)
    value = row[String(key)]
    _atlas_sqlite_is_nullish(value) && return nothing
    return Int(value)
end

function _atlas_sqlite_verify_ro_identity_row!(
    row,
    expected_hash::AbstractString,
    expected_blob::Vector{UInt8},
    counts,
    document,
)
    row === nothing && _ro_field_identity_error(
        :missing_identity,
        "RO-field artifact references a missing RPB2 identity",
    )
    _atlas_sqlite_ro_field_row_string(row, "cell_complex_hash") == expected_hash ||
        _ro_field_identity_error(:identity_mismatch, "RPB2 identity hash column does not match")
    _atlas_sqlite_ro_field_row_string(row, "codec_magic") == "RPB2" ||
        _ro_field_identity_error(:identity_mismatch, "RPB2 identity has the wrong codec magic")
    _atlas_sqlite_ro_field_row_int(row, "codec_version") == RO_CELL_COMPLEX_CODEC_VERSION ||
        _ro_field_identity_error(:identity_mismatch, "RPB2 identity has the wrong codec version")
    _atlas_sqlite_ro_field_row_string(row, "identity_kind") == RO_CELL_COMPLEX_IDENTITY_KIND ||
        _ro_field_identity_error(:identity_mismatch, "RPB2 identity has the wrong identity kind")
    stored_blob = UInt8.(collect(row["blob"]))
    stored_blob == expected_blob ||
        _ro_field_identity_error(:identity_collision, "RPB2 hash conflict has different blob bytes")
    ro_cell_complex_hash(stored_blob) == expected_hash ||
        _ro_field_identity_error(:identity_hash_mismatch, "stored RPB2 bytes do not match their key")
    decoded = decode_ro_cell_complex_blob(stored_blob)
    _ro_field_canonical_json(decoded) == _ro_field_canonical_json(canonical_ro_cell_complex_payload(document)) ||
        _ro_field_identity_error(:identity_payload_mismatch, "stored RPB2 payload does not match the field")

    data = _ro_field_identity_get(document, "data")
    expected_cell_count = length(_ro_field_identity_vector(data, "cells"))
    expected_facet_count = length(_ro_field_identity_vector(data, "facets"))
    expected_stratum_count = length(_ro_field_identity_vector(data, "singular_strata"))
    _atlas_sqlite_ro_field_row_int(row, "axis_count") == counts.axis_count ||
        _ro_field_identity_error(:identity_metadata_mismatch, "RPB2 axis_count is corrupt")
    _atlas_sqlite_ro_field_row_int(row, "output_count") == counts.output_count ||
        _ro_field_identity_error(:identity_metadata_mismatch, "RPB2 output_count is corrupt")
    _atlas_sqlite_ro_field_row_int(row, "cell_count") == expected_cell_count ||
        _ro_field_identity_error(:identity_metadata_mismatch, "RPB2 cell_count is corrupt")
    _atlas_sqlite_ro_field_row_int(row, "facet_count") == expected_facet_count ||
        _ro_field_identity_error(:identity_metadata_mismatch, "RPB2 facet_count is corrupt")
    _atlas_sqlite_ro_field_row_int(row, "singular_stratum_count") == expected_stratum_count ||
        _ro_field_identity_error(:identity_metadata_mismatch, "RPB2 singular_stratum_count is corrupt")
    return nothing
end

function _atlas_sqlite_insert_or_verify_ro_identity!(
    db::SQLite.DB,
    document,
    blob::Vector{UInt8},
    cell_complex_hash::AbstractString,
    counts,
)
    data = _ro_field_identity_get(document, "data")
    _atlas_sqlite_execute(db,
        """
        INSERT OR IGNORE INTO ro_cell_complex_identities (
            cell_complex_hash, codec_magic, codec_version, identity_kind, blob,
            axis_count, output_count, cell_count, facet_count, singular_stratum_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            String(cell_complex_hash),
            "RPB2",
            RO_CELL_COMPLEX_CODEC_VERSION,
            RO_CELL_COMPLEX_IDENTITY_KIND,
            blob,
            counts.axis_count,
            counts.output_count,
            length(_ro_field_identity_vector(data, "cells")),
            length(_ro_field_identity_vector(data, "facets")),
            length(_ro_field_identity_vector(data, "singular_strata")),
        ),
    )
    row = _atlas_sqlite_single_ro_identity_row(db, cell_complex_hash)
    _atlas_sqlite_verify_ro_identity_row!(
        row,
        cell_complex_hash,
        blob,
        counts,
        document,
    )
    return nothing
end

"""
Persist one validated inline RO-field document and return its content keys.

Sampled and partial exact artifacts retain only the full artifact/data hashes.
A complete, gap-free, single-valued exact complex is additionally interned as
an RPB2 `exact_cell_complex_v1` identity before the artifact row is written.
"""
function atlas_sqlite_save_ro_field_artifact!(db::SQLite.DB, document)
    atlas_sqlite_init!(db)
    doc = _validate_ro_field_storage_document!(document)
    counts = _atlas_sqlite_ro_field_counts(doc)
    representation = String(_ro_field_identity_get(doc, "representation"))
    field_json = _ro_field_canonical_json(doc)
    artifact_sha256 = _ro_field_sha256(_ro_field_utf8_bytes(field_json))
    data_sha256 = ro_field_data_sha256(doc)

    provenance = _ro_field_identity_get(doc, "provenance")
    network_ir_sha256 = _ro_field_identity_sha256_text(
        _ro_field_identity_get(provenance, "network_ir_sha256"),
        "provenance.network_ir_sha256",
    )
    domain_sha256 = _ro_field_identity_sha256_text(
        _ro_field_identity_get(provenance, "domain_sha256"),
        "provenance.domain_sha256",
    )
    created_at = String(_ro_field_identity_get(provenance, "created_at"))
    field_id = String(_ro_field_identity_get(doc, "field_id"))
    partial = _ro_field_identity_bool(doc, "partial")

    identity_blob = nothing
    cell_complex_hash = nothing
    if representation == "exact_cell_complex" && !partial
        identity_blob = encode_ro_cell_complex_blob(doc)
        cell_complex_hash = ro_cell_complex_hash(identity_blob)
    end

    _with_atlas_sqlite_write_lock(db) do
        _atlas_sqlite_transaction(db) do
            if identity_blob !== nothing
                _atlas_sqlite_insert_or_verify_ro_identity!(
                    db,
                    doc,
                    identity_blob,
                    cell_complex_hash,
                    counts,
                )
            end

            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO ro_field_artifacts (
                    artifact_sha256, field_id, schema_version, representation,
                    network_ir_sha256, domain_sha256, data_sha256, cell_complex_hash,
                    partial, storage_mode, axis_count, output_count,
                    eligible_count, evaluated_count, valid_count, invalid_count,
                    omitted_count, created_at, field_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_sha256,
                    field_id,
                    String(_ro_field_identity_get(doc, "schema_version")),
                    representation,
                    network_ir_sha256,
                    domain_sha256,
                    data_sha256,
                    cell_complex_hash,
                    partial ? 1 : 0,
                    "inline",
                    counts.axis_count,
                    counts.output_count,
                    counts.eligible_count,
                    counts.evaluated_count,
                    counts.valid_count,
                    counts.invalid_count,
                    counts.omitted_count,
                    created_at,
                    field_json,
                ),
            )

            # INSERT OR IGNORE handles retries; a hash collision or inconsistent
            # retry must still fail closed by comparing the canonical bytes.
            row = _atlas_sqlite_single_ro_field_row(db, artifact_sha256)
            row === nothing && _ro_field_identity_error(
                :storage_failure,
                "RO-field artifact insert did not produce a row",
            )
            stored_json = _atlas_sqlite_ro_field_row_string(row, "field_json")
            stored_json == field_json || _ro_field_identity_error(
                :artifact_collision,
                "RO-field artifact hash conflict has different canonical bytes",
            )
            _atlas_sqlite_ro_field_row_string(row, "data_sha256") == data_sha256 ||
                _ro_field_identity_error(:artifact_collision, "RO-field data hash differs on retry")
            _atlas_sqlite_ro_field_row_string(row, "cell_complex_hash") == cell_complex_hash ||
                _ro_field_identity_error(:artifact_collision, "RO-field RPB2 identity differs on retry")
        end
    end

    return Dict{String, Any}(
        "artifact_sha256" => artifact_sha256,
        "data_sha256" => data_sha256,
        "cell_complex_hash" => cell_complex_hash,
    )
end

function atlas_sqlite_save_ro_field_artifact!(db_path::AbstractString, document)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_save_ro_field_artifact!(db, document),
        db_path,
    )
end

function _atlas_sqlite_verify_ro_field_columns!(row, document, counts, artifact_sha256, data_sha256, cell_complex_hash)
    provenance = _ro_field_identity_get(document, "provenance")
    coverage = _ro_field_identity_get(document, "coverage")
    expected = Dict{String, Any}(
        "artifact_sha256" => artifact_sha256,
        "field_id" => String(_ro_field_identity_get(document, "field_id")),
        "schema_version" => String(_ro_field_identity_get(document, "schema_version")),
        "representation" => String(_ro_field_identity_get(document, "representation")),
        "network_ir_sha256" => String(_ro_field_identity_get(provenance, "network_ir_sha256")),
        "domain_sha256" => String(_ro_field_identity_get(provenance, "domain_sha256")),
        "data_sha256" => data_sha256,
        "cell_complex_hash" => cell_complex_hash,
        "storage_mode" => "inline",
        "created_at" => String(_ro_field_identity_get(provenance, "created_at")),
    )
    for (key, value) in expected
        _atlas_sqlite_ro_field_row_string(row, key) == value ||
            _ro_field_identity_error(:artifact_metadata_mismatch, "RO-field $(key) column is corrupt")
    end
    int_expected = Dict{String, Int}(
        "partial" => _ro_field_identity_bool(document, "partial") ? 1 : 0,
        "axis_count" => counts.axis_count,
        "output_count" => counts.output_count,
        "eligible_count" => counts.eligible_count,
        "evaluated_count" => counts.evaluated_count,
        "valid_count" => counts.valid_count,
        "invalid_count" => counts.invalid_count,
        "omitted_count" => counts.omitted_count,
    )
    for (key, value) in int_expected
        _atlas_sqlite_ro_field_row_int(row, key) == value ||
            _ro_field_identity_error(:artifact_metadata_mismatch, "RO-field $(key) column is corrupt")
    end
    _ro_field_identity_int(coverage, "evaluated_count") == counts.evaluated_count ||
        _ro_field_identity_error(:artifact_metadata_mismatch, "RO-field coverage changed during load")
    return nothing
end

"Load one inline artifact and re-verify its data, full-document, RPB2, and FK identities."
function atlas_sqlite_load_ro_field_artifact(db::SQLite.DB, artifact_sha256::AbstractString)
    atlas_sqlite_init!(db)
    row = _atlas_sqlite_single_ro_field_row(db, artifact_sha256)
    row === nothing && return nothing
    field_json = _atlas_sqlite_ro_field_row_string(row, "field_json")
    field_json === nothing && _ro_field_identity_error(:corrupt_artifact, "RO-field field_json is NULL")
    document = try
        _ro_field_materialize(JSON3.read(field_json))
    catch err
        _ro_field_identity_error(:corrupt_artifact, "RO-field field_json is invalid: $(sprint(showerror, err))")
    end
    doc = _validate_ro_field_storage_document!(document)
    canonical_json = _ro_field_canonical_json(doc)
    canonical_json == field_json || _ro_field_identity_error(
        :noncanonical_artifact,
        "stored RO-field JSON is not canonical",
    )
    actual_artifact_sha256 = _ro_field_sha256(_ro_field_utf8_bytes(canonical_json))
    actual_artifact_sha256 == String(artifact_sha256) || _ro_field_identity_error(
        :artifact_hash_mismatch,
        "stored RO-field document does not match its artifact key",
    )
    data_sha256 = ro_field_data_sha256(doc)
    counts = _atlas_sqlite_ro_field_counts(doc)
    representation = String(_ro_field_identity_get(doc, "representation"))
    partial = _ro_field_identity_bool(doc, "partial")
    stored_cell_hash = _atlas_sqlite_ro_field_row_string(row, "cell_complex_hash")

    expected_cell_hash = nothing
    if representation == "exact_cell_complex" && !partial
        blob = encode_ro_cell_complex_blob(doc)
        expected_cell_hash = ro_cell_complex_hash(blob)
        stored_cell_hash == expected_cell_hash || _ro_field_identity_error(
            :foreign_identity_mismatch,
            "RO-field artifact does not reference its recomputed RPB2 identity",
        )
        identity_row = _atlas_sqlite_single_ro_identity_row(db, expected_cell_hash)
        _atlas_sqlite_verify_ro_identity_row!(
            identity_row,
            expected_cell_hash,
            blob,
            counts,
            doc,
        )
    elseif stored_cell_hash !== nothing
        _ro_field_identity_error(
            :unexpected_identity,
            "only complete exact cell complexes may reference an RPB2 identity",
        )
    end

    _atlas_sqlite_verify_ro_field_columns!(
        row,
        doc,
        counts,
        actual_artifact_sha256,
        data_sha256,
        expected_cell_hash,
    )
    return doc
end

function atlas_sqlite_load_ro_field_artifact(db_path::AbstractString, artifact_sha256::AbstractString)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_load_ro_field_artifact(db, artifact_sha256),
        db_path,
    )
end

"Query verified RO-field artifacts by indexed identity fields."
function atlas_sqlite_query_ro_field_artifacts(
    db::SQLite.DB;
    network_ir_sha256=nothing,
    representation=nothing,
    domain_sha256=nothing,
    field_id=nothing,
    cell_complex_hash=nothing,
    limit::Integer=100,
    include_documents::Bool=false,
)
    1 <= limit <= 1000 || throw(ArgumentError("RO-field query limit must be between 1 and 1000"))
    atlas_sqlite_init!(db)
    clauses = String[]
    params = Any[]
    for (column, value) in (
        ("network_ir_sha256", network_ir_sha256),
        ("representation", representation),
        ("domain_sha256", domain_sha256),
        ("field_id", field_id),
        ("cell_complex_hash", cell_complex_hash),
    )
        value === nothing && continue
        push!(clauses, "$(column) = ?")
        push!(params, String(value))
    end
    where_sql = isempty(clauses) ? "" : " WHERE " * join(clauses, " AND ")
    sql = "SELECT artifact_sha256 FROM ro_field_artifacts" * where_sql *
          " ORDER BY created_at DESC, artifact_sha256 ASC LIMIT ?"
    push!(params, Int(limit))

    hashes = String[]
    query = _atlas_sqlite_query(db, sql, Tuple(params))
    try
        for row in query
            push!(hashes, String(row[:artifact_sha256]))
        end
    finally
        DBInterface.close!(query)
    end

    results = Dict{String, Any}[]
    for hash in hashes
        document = atlas_sqlite_load_ro_field_artifact(db, hash)
        document === nothing && _ro_field_identity_error(
            :concurrent_delete,
            "RO-field artifact disappeared during a verified query",
        )
        provenance = _ro_field_identity_get(document, "provenance")
        result = Dict{String, Any}(
            "artifact_sha256" => hash,
            "field_id" => String(_ro_field_identity_get(document, "field_id")),
            "representation" => String(_ro_field_identity_get(document, "representation")),
            "network_ir_sha256" => String(_ro_field_identity_get(provenance, "network_ir_sha256")),
            "domain_sha256" => String(_ro_field_identity_get(provenance, "domain_sha256")),
            "data_sha256" => ro_field_data_sha256(document),
        )
        include_documents && (result["document"] = document)
        push!(results, result)
    end
    return results
end

function atlas_sqlite_query_ro_field_artifacts(db_path::AbstractString; kwargs...)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_query_ro_field_artifacts(db; kwargs...),
        db_path,
    )
end

# ─── Versioned RO-field behavior signatures (schema 0.5.0) ────────────────

const _ATLAS_SQLITE_RO_SIGNATURE_HASH_PATTERN = r"^[0-9a-f]{64}$"
const _ATLAS_SQLITE_RO_SIGNATURE_ID_PATTERN =
    r"^[A-Za-z][A-Za-z0-9._:-]{0,127}$"
const _ATLAS_SQLITE_RO_COMPONENT_CLASSES = Set((
    "zero",
    "strictly_positive",
    "nonnegative_variable",
    "strictly_negative",
    "nonpositive_variable",
    "sign_changing",
    "unknown",
))
const _ATLAS_SQLITE_RO_GRADIENT_FAMILIES = Set((
    "all_zero",
    "all_nonnegative",
    "all_nonpositive",
    "opposed_axis_signs",
    "sign_changing",
    "other_mixed",
    "unknown",
))

"A signature is valid in isolation but cannot be trusted in this SQLite store."
struct ROFieldSignatureStorageError <: Exception
    code::Symbol
    message::String
end

function Base.showerror(io::IO, err::ROFieldSignatureStorageError)
    print(io, "RO-field signature storage error [", err.code, "]: ",
        err.message)
end

_atlas_sqlite_ro_signature_error(code::Symbol, message::AbstractString) =
    throw(ROFieldSignatureStorageError(code, String(message)))

function _atlas_sqlite_validate_ro_signature!(signature)
    isdefined(@__MODULE__, :validate_ro_field_signature!) ||
        _atlas_sqlite_ro_signature_error(
            :classifier_unavailable,
            "ro_field_behavior.jl must be loaded before storing signatures",
        )
    validated = getfield(@__MODULE__, :validate_ro_field_signature!)(signature)
    # The shared trust boundary returns its normalized document.  Retain a
    # compatibility fallback to the caller value if a validation-only revision
    # returns `nothing`; either form is materialized before persistence.
    return _ro_field_materialize(validated === nothing ? signature : validated)
end

function _atlas_sqlite_ro_signature_text(value, path::AbstractString)
    value isa AbstractString || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "$(path) must be a string",
    )
    return String(value)
end

function _atlas_sqlite_ro_signature_hash(value, path::AbstractString)
    text = _atlas_sqlite_ro_signature_text(value, path)
    occursin(_ATLAS_SQLITE_RO_SIGNATURE_HASH_PATTERN, text) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "$(path) must be 64 lowercase hexadecimal characters",
        )
    return text
end

function _atlas_sqlite_ro_signature_int(value, path::AbstractString)
    (value isa Integer && !(value isa Bool)) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "$(path) must be an integer",
        )
    return try
        Int(value)
    catch
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "$(path) is outside the supported integer range",
        )
    end
end

function _atlas_sqlite_ro_signature_object(value, path::AbstractString)
    value isa AbstractDict || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "$(path) must be an object",
    )
    return value
end

function _atlas_sqlite_ro_signature_array(value, path::AbstractString)
    (value isa AbstractVector || value isa Tuple) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "$(path) must be an array",
        )
    return collect(value)
end

function _atlas_sqlite_ro_signature_ids(value, path::AbstractString)
    raw = _atlas_sqlite_ro_signature_array(value, path)
    ids = String[]
    for (index, item) in enumerate(raw)
        identifier = _atlas_sqlite_ro_signature_text(
            item, "$(path)[$(index)]")
        occursin(_ATLAS_SQLITE_RO_SIGNATURE_ID_PATTERN, identifier) ||
            _atlas_sqlite_ro_signature_error(
                :invalid_signature,
                "$(path)[$(index)] is not a safe identifier",
            )
        push!(ids, identifier)
    end
    allunique(ids) || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "$(path) must contain unique identifiers",
    )
    return ids
end

function _atlas_sqlite_ro_signature_normalized_rows(signature)
    axis_ids = _atlas_sqlite_ro_signature_ids(
        signature["axis_ids"], "axis_ids")
    output_ids = _atlas_sqlite_ro_signature_ids(
        signature["output_ids"], "output_ids")
    length(axis_ids) == 2 || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "axis_ids must contain exactly two identifiers",
    )
    isempty(output_ids) && _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "output_ids must not be empty",
    )

    component_rows = NamedTuple[]
    components = _atlas_sqlite_ro_signature_array(
        signature["component_classifications"],
        "component_classifications",
    )
    for (index, raw) in enumerate(components)
        item = _atlas_sqlite_ro_signature_object(
            raw, "component_classifications[$(index)]")
        output_id = _atlas_sqlite_ro_signature_text(
            item["output_id"],
            "component_classifications[$(index)].output_id",
        )
        axis_id = _atlas_sqlite_ro_signature_text(
            item["axis_id"],
            "component_classifications[$(index)].axis_id",
        )
        classification = _atlas_sqlite_ro_signature_text(
            item["classification"],
            "component_classifications[$(index)].classification",
        )
        output_position = findfirst(==(output_id), output_ids)
        axis_position = findfirst(==(axis_id), axis_ids)
        output_position === nothing && _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "component output_id is absent from output_ids",
        )
        axis_position === nothing && _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "component axis_id is absent from axis_ids",
        )
        classification in _ATLAS_SQLITE_RO_COMPONENT_CLASSES ||
            _atlas_sqlite_ro_signature_error(
                :unsupported_feature,
                "unknown component classification $(classification)",
            )
        push!(component_rows, (
            output_position=output_position,
            axis_position=axis_position,
            output_id=output_id,
            axis_id=axis_id,
            classification=classification,
        ))
    end
    expected_component_positions = Set(
        (output_position, axis_position)
        for output_position in eachindex(output_ids),
            axis_position in eachindex(axis_ids)
    )
    actual_component_positions = Set(
        (row.output_position, row.axis_position) for row in component_rows)
    actual_component_positions == expected_component_positions &&
        length(component_rows) == length(expected_component_positions) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "component classifications must cover each ordered output/axis pair exactly once",
        )
    sort!(component_rows; by=row ->
        (row.output_position, row.axis_position))

    gradient_rows = NamedTuple[]
    gradients = _atlas_sqlite_ro_signature_array(
        signature["gradient_families"], "gradient_families")
    for (index, raw) in enumerate(gradients)
        item = _atlas_sqlite_ro_signature_object(
            raw, "gradient_families[$(index)]")
        output_id = _atlas_sqlite_ro_signature_text(
            item["output_id"],
            "gradient_families[$(index)].output_id",
        )
        family = _atlas_sqlite_ro_signature_text(
            item["gradient_family"],
            "gradient_families[$(index)].gradient_family",
        )
        output_position = findfirst(==(output_id), output_ids)
        output_position === nothing && _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "gradient output_id is absent from output_ids",
        )
        family in _ATLAS_SQLITE_RO_GRADIENT_FAMILIES ||
            _atlas_sqlite_ro_signature_error(
                :unsupported_feature,
                "unknown gradient family $(family)",
            )
        push!(gradient_rows, (
            output_position=output_position,
            output_id=output_id,
            gradient_family=family,
        ))
    end
    Set(row.output_position for row in gradient_rows) ==
        Set(eachindex(output_ids)) &&
        length(gradient_rows) == length(output_ids) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_signature,
            "gradient families must cover each ordered output exactly once",
        )
    sort!(gradient_rows; by=row -> row.output_position)
    return axis_ids, output_ids, component_rows, gradient_rows
end

function _atlas_sqlite_ro_signature_metadata(signature)
    axis_ids, output_ids, component_rows, gradient_rows =
        _atlas_sqlite_ro_signature_normalized_rows(signature)
    config = _atlas_sqlite_ro_signature_object(
        signature["config"], "config")
    features = _atlas_sqlite_ro_signature_object(
        signature["features"], "features")
    diagnostics = _atlas_sqlite_ro_signature_object(
        signature["diagnostics"], "diagnostics")
    zero_tolerance = config["zero_tolerance"]
    (zero_tolerance isa Real && !(zero_tolerance isa Bool) &&
     isfinite(zero_tolerance)) || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "config.zero_tolerance must be finite",
    )
    classifiable = signature["classifiable"]
    classifiable isa Bool || _atlas_sqlite_ro_signature_error(
        :invalid_signature,
        "classifiable must be a boolean",
    )
    canonical_json = _ro_field_canonical_json(signature)
    return (
        signature_sha256=_atlas_sqlite_ro_signature_hash(
            signature["signature_sha256"], "signature_sha256"),
        artifact_sha256=_atlas_sqlite_ro_signature_hash(
            signature["field_sha256"], "field_sha256"),
        schema_version=_atlas_sqlite_ro_signature_text(
            signature["schema_version"], "schema_version"),
        classifier_version=_atlas_sqlite_ro_signature_text(
            signature["classifier_version"], "classifier_version"),
        scope=_atlas_sqlite_ro_signature_text(
            signature["scope"], "scope"),
        config_sha256=canonical_hash(config),
        signature_json_sha256=_ro_field_sha256(
            _ro_field_utf8_bytes(canonical_json)),
        classifiable=classifiable,
        zero_tolerance=Float64(zero_tolerance),
        max_cells=_atlas_sqlite_ro_signature_int(
            config["max_cells"], "config.max_cells"),
        max_facets=_atlas_sqlite_ro_signature_int(
            config["max_facets"], "config.max_facets"),
        max_matrix_elements=_atlas_sqlite_ro_signature_int(
            config["max_matrix_elements"],
            "config.max_matrix_elements",
        ),
        axis_count=length(axis_ids),
        output_count=length(output_ids),
        internal_facet_count=_atlas_sqlite_ro_signature_int(
            features["internal_facet_count"],
            "features.internal_facet_count",
        ),
        mixed_sign_facet_count=_atlas_sqlite_ro_signature_int(
            features["mixed_sign_facet_count"],
            "features.mixed_sign_facet_count",
        ),
        coupled_jump_count=_atlas_sqlite_ro_signature_int(
            features["coupled_jump_count"],
            "features.coupled_jump_count",
        ),
        excluded_stratum_count=_atlas_sqlite_ro_signature_int(
            diagnostics["excluded_lower_dimensional_strata_count"],
            "diagnostics.excluded_lower_dimensional_strata_count",
        ),
        canonical_json=canonical_json,
        axis_ids=axis_ids,
        output_ids=output_ids,
        component_rows=component_rows,
        gradient_rows=gradient_rows,
    )
end

function _atlas_sqlite_ro_signature_finite(value, path::AbstractString)
    (value isa Real && !(value isa Bool)) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_artifact_geometry,
            "$(path) must be numeric",
        )
    result = Float64(value)
    isfinite(result) || _atlas_sqlite_ro_signature_error(
        :invalid_artifact_geometry,
        "$(path) must be finite",
    )
    return result
end

function _atlas_sqlite_ro_signature_point(value, path::AbstractString)
    coordinates = _ro_field_identity_vector(
        Dict{String,Any}("point" => value), "point")
    length(coordinates) == 2 || _atlas_sqlite_ro_signature_error(
        :invalid_artifact_geometry,
        "$(path) must contain two coordinates",
    )
    return (
        _atlas_sqlite_ro_signature_finite(
            coordinates[1], "$(path)[1]"),
        _atlas_sqlite_ro_signature_finite(
            coordinates[2], "$(path)[2]"),
    )
end


function _atlas_sqlite_ro_signature_ordered_records(
    data,
    order_key::AbstractString,
    records_key::AbstractString,
    id_key::AbstractString,
)
    order = String.(_ro_field_identity_vector(data, order_key))
    records = _ro_field_identity_vector(data, records_key)
    by_id = Dict{String,Any}()
    for record in records
        identifier = String(_ro_field_identity_get(record, id_key))
        haskey(by_id, identifier) && _atlas_sqlite_ro_signature_error(
            :invalid_artifact_geometry,
            "$(records_key) contains duplicate $(id_key) $(identifier)",
        )
        by_id[identifier] = record
    end
    length(order) == length(records) && allunique(order) &&
        Set(order) == Set(keys(by_id)) ||
        _atlas_sqlite_ro_signature_error(
            :invalid_artifact_geometry,
            "$(order_key) does not identify $(records_key) exactly once",
        )
    return order, Any[by_id[identifier] for identifier in order]
end

"Rebuild the classifier's small exact complex from its stored canonical data."
function _atlas_sqlite_ro_signature_complex_from_artifact(
    artifact,
    output_count::Int,
)
    domain_raw = _ro_field_identity_get(artifact, "domain")
    axes = _ro_field_identity_vector(domain_raw, "axes")
    length(axes) == 2 || _atlas_sqlite_ro_signature_error(
        :invalid_artifact_geometry,
        "exact signature artifacts must contain two domain axes",
    )
    lower = Float64[]
    upper = Float64[]
    for (index, axis) in enumerate(axes)
        bounds = _ro_field_identity_get(axis, "bounds")
        push!(lower, _atlas_sqlite_ro_signature_finite(
            _ro_field_identity_get(bounds, "lower"),
            "domain.axes[$(index)].bounds.lower",
        ))
        push!(upper, _atlas_sqlite_ro_signature_finite(
            _ro_field_identity_get(bounds, "upper"),
            "domain.axes[$(index)].bounds.upper",
        ))
    end
    domain = ROInputDomain2D((1, 2), lower, upper, zeros(2))
    domain_area = (upper[1] - lower[1]) * (upper[2] - lower[2])

    data = _ro_field_identity_get(artifact, "data")
    cell_order, raw_cells = _atlas_sqlite_ro_signature_ordered_records(
        data, "cell_order", "cells", "cell_id")
    cell_id_map = Dict(
        identifier => index for (index, identifier) in enumerate(cell_order))
    cells = ROCell2D[]
    for (cell_index, raw_cell) in enumerate(raw_cells)
        vertices_raw = _ro_field_identity_vector(raw_cell, "vertices")
        vertices = NTuple{2,Float64}[
            _atlas_sqlite_ro_signature_point(
                point, "data.cells[$(cell_index)].vertices")
            for point in vertices_raw
        ]
        labels_raw = _ro_field_identity_vector(
            raw_cell, "affine_labels")
        label_order = String.(_ro_field_identity_vector(
            raw_cell, "label_order"))
        labels_by_id = Dict{String,Any}()
        for raw_label in labels_raw
            label_id = String(_ro_field_identity_get(
                raw_label, "label_id"))
            haskey(labels_by_id, label_id) &&
                _atlas_sqlite_ro_signature_error(
                    :invalid_artifact_geometry,
                    "cell contains duplicate affine label $(label_id)",
                )
            labels_by_id[label_id] = raw_label
        end
        length(label_order) == length(labels_raw) &&
            allunique(label_order) &&
            Set(label_order) == Set(keys(labels_by_id)) ||
            _atlas_sqlite_ro_signature_error(
                :invalid_artifact_geometry,
                "cell label_order does not identify affine_labels exactly once",
            )
        labels = ROAffineLabel2D[]
        for (label_index, label_id) in enumerate(label_order)
            raw_label = labels_by_id[label_id]
            matrix_raw = _ro_field_identity_vector(
                raw_label, "reaction_order_matrix")
            length(matrix_raw) == output_count ||
                _atlas_sqlite_ro_signature_error(
                    :invalid_artifact_geometry,
                    "affine label output dimension does not match output_order",
                )
            matrix = Matrix{Float64}(undef, output_count, 2)
            for output_index in 1:output_count
                row = _ro_field_identity_vector(
                    Dict{String,Any}("row" => matrix_raw[output_index]),
                    "row",
                )
                length(row) == 2 ||
                    _atlas_sqlite_ro_signature_error(
                        :invalid_artifact_geometry,
                        "affine label rows must contain two components",
                    )
                for axis_index in 1:2
                    matrix[output_index, axis_index] =
                        _atlas_sqlite_ro_signature_finite(
                            row[axis_index],
                            "affine label reaction_order_matrix",
                        )
                end
            end
            offsets_raw = _ro_field_identity_vector(
                raw_label, "output_offset")
            length(offsets_raw) == output_count ||
                _atlas_sqlite_ro_signature_error(
                    :invalid_artifact_geometry,
                    "affine label output_offset dimension is invalid",
                )
            offsets = Float64[
                _atlas_sqlite_ro_signature_finite(
                    value, "affine label output_offset")
                for value in offsets_raw
            ]
            push!(labels, ROAffineLabel2D(
                [label_index], matrix, offsets))
        end
        set_valued = _ro_field_identity_bool(raw_cell, "set_valued")
        push!(cells, ROCell2D(
            cell_index,
            vertices,
            _atlas_sqlite_ro_signature_finite(
                _ro_field_identity_get(raw_cell, "area"),
                "data.cells[$(cell_index)].area",
            ),
            [cell_index],
            labels,
            set_valued,
        ))
    end

    stratum_order, raw_strata =
        _atlas_sqlite_ro_signature_ordered_records(
            data,
            "singular_stratum_order",
            "singular_strata",
            "stratum_id",
        )
    stratum_id_map = Dict(
        identifier => index
        for (index, identifier) in enumerate(stratum_order))
    strata = ROSingularStratum2D[]
    for (stratum_index, raw_stratum) in enumerate(raw_strata)
        vertices = NTuple{2,Float64}[
            _atlas_sqlite_ro_signature_point(
                point,
                "data.singular_strata[$(stratum_index)].vertices",
            ) for point in _ro_field_identity_vector(
                raw_stratum, "vertices")
        ]
        nullities = Int[
            _atlas_sqlite_ro_signature_int(
                value, "singular_strata.nullities")
            for value in _ro_field_identity_vector(
                raw_stratum, "nullities")
        ]
        reasons = Symbol.(String.(_ro_field_identity_vector(
            raw_stratum, "reasons")))
        push!(strata, ROSingularStratum2D(
            stratum_index,
            _atlas_sqlite_ro_signature_int(
                _ro_field_identity_get(raw_stratum, "dimension"),
                "singular_strata.dimension",
            ),
            vertices,
            [stratum_index],
            nullities,
            reasons,
        ))
    end

    _, raw_facets = _atlas_sqlite_ro_signature_ordered_records(
        data, "facet_order", "facets", "facet_id")
    facets = ROFacet2D[]
    for (facet_index, raw_facet) in enumerate(raw_facets)
        endpoints_raw = _ro_field_identity_vector(raw_facet, "endpoints")
        length(endpoints_raw) == 2 ||
            _atlas_sqlite_ro_signature_error(
                :invalid_artifact_geometry,
                "facet endpoints must contain two points",
            )
        endpoints = (
            _atlas_sqlite_ro_signature_point(
                endpoints_raw[1], "facet.endpoints[1]"),
            _atlas_sqlite_ro_signature_point(
                endpoints_raw[2], "facet.endpoints[2]"),
        )
        incident_ids = Int[]
        for raw_id in _ro_field_identity_vector(
            raw_facet, "incident_cell_ids")
            identifier = String(raw_id)
            haskey(cell_id_map, identifier) ||
                _atlas_sqlite_ro_signature_error(
                    :invalid_artifact_geometry,
                    "facet references unknown cell $(identifier)",
                )
            push!(incident_ids, cell_id_map[identifier])
        end
        singular_ids = Int[]
        for raw_id in _ro_field_identity_vector(
            raw_facet, "singular_stratum_ids")
            identifier = String(raw_id)
            haskey(stratum_id_map, identifier) ||
                _atlas_sqlite_ro_signature_error(
                    :invalid_artifact_geometry,
                    "facet references unknown singular stratum $(identifier)",
                )
            push!(singular_ids, stratum_id_map[identifier])
        end
        normal_raw = _ro_field_identity_vector(raw_facet, "normal")
        length(normal_raw) == 2 ||
            _atlas_sqlite_ro_signature_error(
                :invalid_artifact_geometry,
                "facet normal must have two components",
            )
        normal = (
            _atlas_sqlite_ro_signature_finite(
                normal_raw[1], "facet.normal[1]"),
            _atlas_sqlite_ro_signature_finite(
                normal_raw[2], "facet.normal[2]"),
        )
        raw_domain_side = _ro_field_identity_optional(
            raw_facet, "domain_side", nothing)
        domain_side = raw_domain_side === nothing ? nothing :
            Symbol(String(raw_domain_side))
        serialized_kind = String(_ro_field_identity_get(raw_facet, "kind"))
        kind = serialized_kind == "domain_boundary" ? :domain : :internal
        push!(facets, ROFacet2D(
            facet_index,
            kind,
            endpoints,
            incident_ids,
            singular_ids,
            normal,
            _atlas_sqlite_ro_signature_finite(
                _ro_field_identity_get(raw_facet, "offset"),
                "facet.offset",
            ),
            _ro_field_identity_bool(raw_facet, "mixed_sign"),
            domain_side,
        ))
    end

    covered_area = sum(cell.area for cell in cells)
    geometry_tolerance = 1e-9
    area_tolerance = geometry_tolerance * max(1.0, domain_area)
    positive_area_overlap = covered_area > domain_area + area_tolerance
    gap_area = positive_area_overlap ? nothing :
        max(0.0, domain_area - covered_area)
    coverage = _ro_field_identity_get(artifact, "coverage")
    declared_complete =
        _ro_field_identity_bool(coverage, "enumeration_complete") &&
        !_ro_field_identity_bool(coverage, "truncated") &&
        _ro_field_identity_int(coverage, "omitted_count") == 0 &&
        isempty(_ro_field_identity_vector(data, "gaps"))
    geometry_complete = !positive_area_overlap &&
        abs(covered_area - domain_area) <= area_tolerance
    coverage_complete = declared_complete && geometry_complete
    has_ambiguity = positive_area_overlap ||
        any(cell -> cell.set_valued, cells)
    return ROCellComplex2D(
        domain,
        collect(1:output_count),
        cells,
        facets,
        strata,
        _atlas_sqlite_ro_signature_int(
            _ro_field_identity_get(
                data, "source_candidate_regime_count"),
            "data.source_candidate_regime_count",
        ),
        _atlas_sqlite_ro_signature_int(
            _ro_field_identity_get(
                data, "regular_candidate_regime_count"),
            "data.regular_candidate_regime_count",
        ),
        domain_area,
        covered_area,
        gap_area,
        coverage_complete,
        has_ambiguity,
        geometry_tolerance,
    )
end

function _atlas_sqlite_verify_ro_signature_artifact!(
    db::SQLite.DB,
    signature,
    metadata,
)
    artifact = atlas_sqlite_load_ro_field_artifact(
        db, metadata.artifact_sha256)
    artifact === nothing && _atlas_sqlite_ro_signature_error(
        :foreign_artifact,
        "field_sha256 does not identify a stored RO-field artifact",
    )
    String(_ro_field_identity_get(artifact, "representation")) ==
        "exact_cell_complex" || _atlas_sqlite_ro_signature_error(
            :foreign_artifact,
            "behavior signatures may reference exact_cell_complex artifacts only",
        )
    domain = _ro_field_identity_get(artifact, "domain")
    artifact_axis_ids = String.(
        _ro_field_identity_vector(domain, "axis_order"))
    artifact_axis_ids == metadata.axis_ids ||
        _atlas_sqlite_ro_signature_error(
            :foreign_artifact,
            "signature axis_ids do not match the referenced artifact axis_order",
        )
    outputs = _ro_field_identity_get(artifact, "outputs")
    artifact_output_ids = String.(
        _ro_field_identity_vector(outputs, "output_order"))
    artifact_output_ids == metadata.output_ids ||
        _atlas_sqlite_ro_signature_error(
            :foreign_artifact,
            "signature output_ids do not match the referenced artifact output_order",
        )

    # Counts are non-scientific foreign-key guards: they prove that a valid
    # signature was not merely re-hashed and attached to a different complex
    # that happens to use the same axis/output labels.
    data = _ro_field_identity_get(artifact, "data")
    cells = _ro_field_identity_vector(data, "cells")
    facets = _ro_field_identity_vector(data, "facets")
    strata = _ro_field_identity_vector(data, "singular_strata")
    diagnostics = _atlas_sqlite_ro_signature_object(
        signature["diagnostics"], "diagnostics")
    _atlas_sqlite_ro_signature_int(
        diagnostics["regular_cell_count"],
        "diagnostics.regular_cell_count",
    ) == length(cells) || _atlas_sqlite_ro_signature_error(
        :foreign_artifact,
        "signature regular-cell count does not match the referenced artifact",
    )
    _atlas_sqlite_ro_signature_int(
        diagnostics["facet_count"],
        "diagnostics.facet_count",
    ) == length(facets) || _atlas_sqlite_ro_signature_error(
        :foreign_artifact,
        "signature facet count does not match the referenced artifact",
    )
    metadata.excluded_stratum_count == length(strata) ||
        _atlas_sqlite_ro_signature_error(
            :foreign_artifact,
            "signature excluded-stratum count does not match the referenced artifact",
        )
    internal_facet_count = count(facets) do facet
        String(_ro_field_identity_get(facet, "kind")) != "domain_boundary"
    end
    metadata.internal_facet_count == internal_facet_count ||
        _atlas_sqlite_ro_signature_error(
            :foreign_artifact,
            "signature internal-facet count does not match the referenced artifact",
        )

    complex = _atlas_sqlite_ro_signature_complex_from_artifact(
        artifact, metadata.output_count)
    config_raw = _atlas_sqlite_ro_signature_object(
        signature["config"], "config")
    config_type = getfield(@__MODULE__, :ROFieldSignatureConfig)
    config = config_type(
        zero_tolerance=config_raw["zero_tolerance"],
        max_cells=config_raw["max_cells"],
        max_facets=config_raw["max_facets"],
        max_matrix_elements=config_raw["max_matrix_elements"],
    )
    expected = getfield(@__MODULE__, :classify_ro_cell_complex)(
        complex,
        metadata.artifact_sha256;
        axis_ids=metadata.axis_ids,
        output_ids=metadata.output_ids,
        config=config,
    )
    _ro_field_canonical_json(expected) ==
        _ro_field_canonical_json(signature) ||
        _atlas_sqlite_ro_signature_error(
            :signature_artifact_mismatch,
            "signature features do not match the referenced exact artifact",
        )
    return artifact
end

function _atlas_sqlite_single_ro_signature_row(
    db::SQLite.DB,
    signature_sha256::AbstractString,
)
    query = _atlas_sqlite_query(db,
        "SELECT * FROM ro_field_signatures WHERE signature_sha256 = ? LIMIT 1",
        (String(signature_sha256),),
    )
    try
        for row in query
            return Dict{String,Any}(
                String(name) => row[name] for name in propertynames(row))
        end
    finally
        DBInterface.close!(query)
    end
    return nothing
end

function _atlas_sqlite_conflicting_ro_signature_row(
    db::SQLite.DB,
    artifact_sha256::AbstractString,
    classifier_version::AbstractString,
    config_sha256::AbstractString,
)
    query = _atlas_sqlite_query(db,
        "SELECT signature_sha256 FROM ro_field_signatures " *
        "WHERE artifact_sha256 = ? AND classifier_version = ? " *
        "AND config_sha256 = ? LIMIT 1",
        (String(artifact_sha256), String(classifier_version),
         String(config_sha256)),
    )
    try
        for row in query
            return String(row[:signature_sha256])
        end
    finally
        DBInterface.close!(query)
    end
    return nothing
end

function _atlas_sqlite_ro_signature_child_rows(
    db::SQLite.DB,
    table::AbstractString,
    signature_sha256::AbstractString,
    order_columns::AbstractString,
)
    table in ("ro_field_component_features",
              "ro_field_output_gradient_features") ||
        throw(ArgumentError("unsupported RO-field signature child table"))
    query = _atlas_sqlite_query(db,
        "SELECT * FROM $(table) WHERE signature_sha256 = ? " *
        "ORDER BY $(order_columns)",
        (String(signature_sha256),),
    )
    rows = Dict{String,Any}[]
    try
        for row in query
            push!(rows, Dict{String,Any}(
                String(name) => row[name] for name in propertynames(row)))
        end
    finally
        DBInterface.close!(query)
    end
    return rows
end

function _atlas_sqlite_verify_ro_signature_row!(row, metadata, db::SQLite.DB)
    row === nothing && _atlas_sqlite_ro_signature_error(
        :missing_signature,
        "signature insert or load did not produce a row",
    )
    string_expected = (
        "signature_sha256" => metadata.signature_sha256,
        "artifact_sha256" => metadata.artifact_sha256,
        "schema_version" => metadata.schema_version,
        "classifier_version" => metadata.classifier_version,
        "scope" => metadata.scope,
        "config_sha256" => metadata.config_sha256,
        "signature_json_sha256" => metadata.signature_json_sha256,
        "signature_json" => metadata.canonical_json,
    )
    for (column, expected) in string_expected
        _atlas_sqlite_ro_field_row_string(row, column) == expected ||
            _atlas_sqlite_ro_signature_error(
                :signature_collision,
                "stored $(column) differs from the validated signature",
            )
    end
    int_expected = (
        "classifiable" => (metadata.classifiable ? 1 : 0),
        "max_cells" => metadata.max_cells,
        "max_facets" => metadata.max_facets,
        "max_matrix_elements" => metadata.max_matrix_elements,
        "axis_count" => metadata.axis_count,
        "output_count" => metadata.output_count,
        "internal_facet_count" => metadata.internal_facet_count,
        "mixed_sign_facet_count" => metadata.mixed_sign_facet_count,
        "coupled_jump_count" => metadata.coupled_jump_count,
        "excluded_stratum_count" => metadata.excluded_stratum_count,
    )
    for (column, expected) in int_expected
        _atlas_sqlite_ro_field_row_int(row, column) == expected ||
            _atlas_sqlite_ro_signature_error(
                :signature_collision,
                "stored $(column) differs from the validated signature",
            )
    end
    stored_tolerance = row["zero_tolerance"]
    (!_atlas_sqlite_is_nullish(stored_tolerance) &&
     Float64(stored_tolerance) == metadata.zero_tolerance) ||
        _atlas_sqlite_ro_signature_error(
            :signature_collision,
            "stored zero_tolerance differs from the validated signature",
        )

    component_rows = _atlas_sqlite_ro_signature_child_rows(
        db,
        "ro_field_component_features",
        metadata.signature_sha256,
        "output_position, axis_position",
    )
    length(component_rows) == length(metadata.component_rows) ||
        _atlas_sqlite_ro_signature_error(
            :normalized_feature_mismatch,
            "stored component-feature count differs from the signature",
        )
    for (stored, expected) in zip(component_rows, metadata.component_rows)
        (_atlas_sqlite_ro_field_row_int(stored, "output_position") ==
             expected.output_position &&
         _atlas_sqlite_ro_field_row_int(stored, "axis_position") ==
             expected.axis_position &&
         _atlas_sqlite_ro_field_row_string(stored, "output_id") ==
             expected.output_id &&
         _atlas_sqlite_ro_field_row_string(stored, "axis_id") ==
             expected.axis_id &&
         _atlas_sqlite_ro_field_row_string(stored, "classification") ==
             expected.classification) ||
            _atlas_sqlite_ro_signature_error(
                :normalized_feature_mismatch,
                "stored component feature differs from the signature",
            )
    end

    gradient_rows = _atlas_sqlite_ro_signature_child_rows(
        db,
        "ro_field_output_gradient_features",
        metadata.signature_sha256,
        "output_position",
    )
    length(gradient_rows) == length(metadata.gradient_rows) ||
        _atlas_sqlite_ro_signature_error(
            :normalized_feature_mismatch,
            "stored gradient-feature count differs from the signature",
        )
    for (stored, expected) in zip(gradient_rows, metadata.gradient_rows)
        (_atlas_sqlite_ro_field_row_int(stored, "output_position") ==
             expected.output_position &&
         _atlas_sqlite_ro_field_row_string(stored, "output_id") ==
             expected.output_id &&
         _atlas_sqlite_ro_field_row_string(stored, "gradient_family") ==
             expected.gradient_family) ||
            _atlas_sqlite_ro_signature_error(
                :normalized_feature_mismatch,
                "stored gradient feature differs from the signature",
            )
    end
    return nothing
end

"""
Persist one validated `bne-ro-field-signature/v1.0.0` signature.

The referenced exact RO-field artifact must already exist.  Signature identity
is recomputed by `validate_ro_field_signature!`; SQLite then binds that trusted
document to the artifact's exact axis/output order and stores normalized query
features in the same write-locked transaction.
"""
function atlas_sqlite_save_ro_field_signature!(db::SQLite.DB, signature)
    atlas_sqlite_init!(db)
    document = _atlas_sqlite_validate_ro_signature!(signature)
    metadata = _atlas_sqlite_ro_signature_metadata(document)
    _atlas_sqlite_verify_ro_signature_artifact!(db, document, metadata)

    _with_atlas_sqlite_write_lock(db) do
        _atlas_sqlite_transaction(db) do
            # Close the validation/write race without re-running the expensive
            # artifact decoder while the write lock is held.
            _atlas_sqlite_single_ro_field_row(
                db, metadata.artifact_sha256) === nothing &&
                _atlas_sqlite_ro_signature_error(
                    :concurrent_delete,
                    "referenced RO-field artifact disappeared before insert",
                )
            _atlas_sqlite_execute(db,
                """
                INSERT OR IGNORE INTO ro_field_signatures (
                    signature_sha256, artifact_sha256, schema_version,
                    classifier_version, scope, config_sha256,
                    signature_json_sha256, classifiable, zero_tolerance,
                    max_cells, max_facets, max_matrix_elements,
                    axis_count, output_count, internal_facet_count,
                    mixed_sign_facet_count, coupled_jump_count,
                    excluded_stratum_count, signature_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metadata.signature_sha256,
                    metadata.artifact_sha256,
                    metadata.schema_version,
                    metadata.classifier_version,
                    metadata.scope,
                    metadata.config_sha256,
                    metadata.signature_json_sha256,
                    metadata.classifiable ? 1 : 0,
                    metadata.zero_tolerance,
                    metadata.max_cells,
                    metadata.max_facets,
                    metadata.max_matrix_elements,
                    metadata.axis_count,
                    metadata.output_count,
                    metadata.internal_facet_count,
                    metadata.mixed_sign_facet_count,
                    metadata.coupled_jump_count,
                    metadata.excluded_stratum_count,
                    metadata.canonical_json,
                ),
            )

            row = _atlas_sqlite_single_ro_signature_row(
                db, metadata.signature_sha256)
            if row === nothing
                conflicting_hash =
                    _atlas_sqlite_conflicting_ro_signature_row(
                        db,
                        metadata.artifact_sha256,
                        metadata.classifier_version,
                        metadata.config_sha256,
                    )
                conflicting_hash === nothing &&
                    _atlas_sqlite_ro_signature_error(
                        :storage_failure,
                        "signature insert did not produce a row",
                    )
                _atlas_sqlite_ro_signature_error(
                    :determinism_conflict,
                    "artifact/config already maps to signature $(conflicting_hash)",
                )
            end

            for feature in metadata.component_rows
                _atlas_sqlite_execute(db,
                    """
                    INSERT OR IGNORE INTO ro_field_component_features (
                        signature_sha256, output_position, axis_position,
                        output_id, axis_id, classification
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        metadata.signature_sha256,
                        feature.output_position,
                        feature.axis_position,
                        feature.output_id,
                        feature.axis_id,
                        feature.classification,
                    ),
                )
            end
            for feature in metadata.gradient_rows
                _atlas_sqlite_execute(db,
                    """
                    INSERT OR IGNORE INTO ro_field_output_gradient_features (
                        signature_sha256, output_position, output_id,
                        gradient_family
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        metadata.signature_sha256,
                        feature.output_position,
                        feature.output_id,
                        feature.gradient_family,
                    ),
                )
            end
            _atlas_sqlite_verify_ro_signature_row!(row, metadata, db)
        end
    end
    return Dict{String,Any}(
        "signature_sha256" => metadata.signature_sha256,
        "artifact_sha256" => metadata.artifact_sha256,
        "config_sha256" => metadata.config_sha256,
        "signature_json_sha256" => metadata.signature_json_sha256,
    )
end

function atlas_sqlite_save_ro_field_signature!(
    db_path::AbstractString,
    signature,
)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_save_ro_field_signature!(db, signature),
        db_path,
    )
end

"Load and re-verify the full signature, normalized rows, and referenced artifact."
function atlas_sqlite_load_ro_field_signature(
    db::SQLite.DB,
    signature_sha256::AbstractString,
)
    requested_hash = _atlas_sqlite_ro_signature_hash(
        signature_sha256, "signature_sha256")
    atlas_sqlite_init!(db)
    row = _atlas_sqlite_single_ro_signature_row(db, requested_hash)
    row === nothing && return nothing
    stored_json = _atlas_sqlite_ro_field_row_string(row, "signature_json")
    stored_json === nothing && _atlas_sqlite_ro_signature_error(
        :corrupt_signature,
        "stored signature_json is NULL",
    )
    document = try
        _ro_field_materialize(JSON3.read(stored_json))
    catch err
        _atlas_sqlite_ro_signature_error(
            :corrupt_signature,
            "stored signature_json is invalid: $(sprint(showerror, err))",
        )
    end
    _ro_field_canonical_json(document) == stored_json ||
        _atlas_sqlite_ro_signature_error(
            :noncanonical_signature,
            "stored signature_json is not canonical",
        )
    document = _atlas_sqlite_validate_ro_signature!(document)
    metadata = _atlas_sqlite_ro_signature_metadata(document)
    metadata.signature_sha256 == requested_hash ||
        _atlas_sqlite_ro_signature_error(
            :signature_hash_mismatch,
            "stored signature does not match its primary key",
        )
    _atlas_sqlite_verify_ro_signature_artifact!(db, document, metadata)
    _atlas_sqlite_verify_ro_signature_row!(row, metadata, db)
    return document
end

function atlas_sqlite_load_ro_field_signature(
    db_path::AbstractString,
    signature_sha256::AbstractString,
)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_load_ro_field_signature(
            db, signature_sha256),
        db_path,
    )
end

function _atlas_sqlite_ro_signature_filter_text(
    value,
    name::AbstractString,
)
    value === nothing && return nothing
    value isa AbstractString || value isa Symbol || throw(ArgumentError(
        "$(name) must be a string or symbol"))
    return String(value)
end

"""
Query verified stored signatures by normalized component/gradient features.

An empty vector means only that no stored row matched the declared filters.  It
is retrieval evidence and never an impossibility claim about network space.
"""
function atlas_sqlite_query_ro_field_signatures(
    db::SQLite.DB;
    artifact_sha256=nothing,
    classifiable=nothing,
    component_classification=nothing,
    component_output_id=nothing,
    component_axis_id=nothing,
    gradient_family=nothing,
    gradient_output_id=nothing,
    limit::Integer=100,
    include_signatures::Bool=false,
)
    1 <= limit <= 1000 || throw(ArgumentError(
        "RO-field signature query limit must be between 1 and 1000"))
    classifiable === nothing || classifiable isa Bool ||
        throw(ArgumentError("classifiable must be a boolean or nothing"))
    artifact_filter = _atlas_sqlite_ro_signature_filter_text(
        artifact_sha256, "artifact_sha256")
    if artifact_filter !== nothing
        occursin(_ATLAS_SQLITE_RO_SIGNATURE_HASH_PATTERN,
            artifact_filter) || throw(ArgumentError(
                "artifact_sha256 must contain 64 lowercase hexadecimal characters"))
    end
    component_class = _atlas_sqlite_ro_signature_filter_text(
        component_classification, "component_classification")
    component_class === nothing ||
        component_class in _ATLAS_SQLITE_RO_COMPONENT_CLASSES ||
        throw(ArgumentError(
            "unsupported component classification: $(component_class)"))
    component_output = _atlas_sqlite_ro_signature_filter_text(
        component_output_id, "component_output_id")
    component_axis = _atlas_sqlite_ro_signature_filter_text(
        component_axis_id, "component_axis_id")
    gradient = _atlas_sqlite_ro_signature_filter_text(
        gradient_family, "gradient_family")
    gradient === nothing || gradient in _ATLAS_SQLITE_RO_GRADIENT_FAMILIES ||
        throw(ArgumentError("unsupported gradient family: $(gradient)"))
    gradient_output = _atlas_sqlite_ro_signature_filter_text(
        gradient_output_id, "gradient_output_id")
    for (name, value) in (
        ("component_output_id", component_output),
        ("component_axis_id", component_axis),
        ("gradient_output_id", gradient_output),
    )
        value === nothing && continue
        occursin(_ATLAS_SQLITE_RO_SIGNATURE_ID_PATTERN, value) ||
            throw(ArgumentError("$(name) is not a safe identifier"))
    end

    atlas_sqlite_init!(db)
    clauses = String[]
    params = Any[]
    if artifact_filter !== nothing
        push!(clauses, "s.artifact_sha256 = ?")
        push!(params, artifact_filter)
    end
    if classifiable !== nothing
        push!(clauses, "s.classifiable = ?")
        push!(params, classifiable ? 1 : 0)
    end
    if component_class !== nothing || component_output !== nothing ||
       component_axis !== nothing
        child_clauses = ["c.signature_sha256 = s.signature_sha256"]
        if component_class !== nothing
            push!(child_clauses, "c.classification = ?")
            push!(params, component_class)
        end
        if component_output !== nothing
            push!(child_clauses, "c.output_id = ?")
            push!(params, component_output)
        end
        if component_axis !== nothing
            push!(child_clauses, "c.axis_id = ?")
            push!(params, component_axis)
        end
        push!(clauses,
            "EXISTS (SELECT 1 FROM ro_field_component_features c WHERE " *
            join(child_clauses, " AND ") * ")")
    end
    if gradient !== nothing || gradient_output !== nothing
        child_clauses = ["g.signature_sha256 = s.signature_sha256"]
        if gradient !== nothing
            push!(child_clauses, "g.gradient_family = ?")
            push!(params, gradient)
        end
        if gradient_output !== nothing
            push!(child_clauses, "g.output_id = ?")
            push!(params, gradient_output)
        end
        push!(clauses,
            "EXISTS (SELECT 1 FROM ro_field_output_gradient_features g WHERE " *
            join(child_clauses, " AND ") * ")")
    end
    where_sql = isempty(clauses) ? "" :
        " WHERE " * join(clauses, " AND ")
    sql = "SELECT s.signature_sha256 FROM ro_field_signatures s" *
        where_sql * " ORDER BY s.signature_sha256 ASC LIMIT ?"
    push!(params, Int(limit))

    hashes = String[]
    query = _atlas_sqlite_query(db, sql, Tuple(params))
    try
        for row in query
            push!(hashes, String(row[:signature_sha256]))
        end
    finally
        DBInterface.close!(query)
    end
    results = Dict{String,Any}[]
    for signature_hash in hashes
        signature = atlas_sqlite_load_ro_field_signature(
            db, signature_hash)
        signature === nothing && _atlas_sqlite_ro_signature_error(
            :concurrent_delete,
            "signature disappeared during a verified query",
        )
        result = Dict{String,Any}(
            "signature_sha256" => signature_hash,
            "artifact_sha256" => String(signature["field_sha256"]),
            "schema_version" => String(signature["schema_version"]),
            "classifier_version" => String(
                signature["classifier_version"]),
            "scope" => String(signature["scope"]),
            "classifiable" => Bool(signature["classifiable"]),
            "component_classifications" =>
                signature["component_classifications"],
            "gradient_families" => signature["gradient_families"],
        )
        include_signatures && (result["signature"] = signature)
        push!(results, result)
    end
    return results
end

function atlas_sqlite_query_ro_field_signatures(
    db_path::AbstractString;
    kwargs...,
)
    return _atlas_sqlite_with_db(
        db -> atlas_sqlite_query_ro_field_signatures(db; kwargs...),
        db_path,
    )
end
