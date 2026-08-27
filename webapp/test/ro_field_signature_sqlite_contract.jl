using Test
using HTTP
using JSON3
using SQLite
using DBInterface
using BindingAndCatalysis
using BiocircuitsExplorerBackend

const _ROFSS_BACKEND = BiocircuitsExplorerBackend
if !isdefined(_ROFSS_BACKEND, :validate_ro_field_signature!)
    Base.include(_ROFSS_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_behavior.jl"))
end

function _rofss_fixture(name)
    path = joinpath(
        @__DIR__, "..", "..", "tests", "fixtures", "ro_field", name)
    return _ROFSS_BACKEND._materialize(JSON3.read(read(path, String)))
end

function _rofss_set_payload_identity!(document)
    bytes = _ROFSS_BACKEND.canonical_ro_field_data_bytes(document)
    storage = document["coverage"]["storage"]
    storage["payload_bytes"] = length(bytes)
    storage["content_sha256"] = _ROFSS_BACKEND._ro_field_sha256(bytes)
    document["provenance"]["domain_sha256"] =
        _ROFSS_BACKEND.canonical_hash(document["domain"])
    return document
end

function _rofss_exact_fixture(field_id)
    document = _rofss_fixture("exact-cell-complex.json")
    document["field_id"] = field_id
    return _rofss_set_payload_identity!(document)
end

function _rofss_ambiguous_exact_fixture(field_id)
    document = _rofss_exact_fixture(field_id)
    cell = document["data"]["cells"][1]
    first_label = cell["affine_labels"][1]
    first_label["source_regime_ids"] = Any["regime-1"]
    second_label = Dict{String,Any}(
        "label_id" => "cell-low-label-2",
        "source_regime_ids" => Any["regime-5"],
        "output_offset" => Any[0.0],
        "reaction_order_matrix" => Any[Any[2.0, 2.0]],
    )
    cell["status"] = "set_valued"
    cell["source_regime_ids"] = Any["regime-1", "regime-5"]
    push!(cell["label_order"], "cell-low-label-2")
    push!(cell["affine_labels"], second_label)
    cell["set_valued"] = true
    document["data"]["source_candidate_regime_count"] = 5
    document["data"]["regular_candidate_regime_count"] = 4
    document["coverage"]["valid_count"] = 2
    document["coverage"]["invalid_count"] = 2
    return _rofss_set_payload_identity!(document)
end

function _rofss_sampled_fixture(field_id)
    document = _rofss_fixture("sampled-grid.json")
    document["field_id"] = field_id
    return _rofss_set_payload_identity!(document)
end

function _rofss_complex(; ambiguous=false)
    model = Bnc(
        N=reshape([1, 1, -1], 1, 3),
        x_sym=[:A, :B, :AB],
        q_sym=[:tA, :tB],
        K_sym=[:Kd],
    )
    domain = ROInputDomain2D(
        (1, 2), (-2.0, -2.0), (2.0, 2.0), zeros(3))
    complex = build_ro_cell_complex(model, domain, [3])
    ambiguous || return complex

    cells = copy(complex.cells)
    first_cell = first(cells)
    first_label = only(first_cell.labels)
    alternate_source_id = complex.candidate_regime_count + 1
    alternate_label = ROAffineLabel2D(
        [alternate_source_id],
        first_label.reaction_order_matrix .+ 1.0,
        copy(first_label.output_offset),
    )
    cells[1] = ROCell2D(
        first_cell.id,
        first_cell.vertices,
        first_cell.area,
        sort!(vcat(first_cell.source_regime_ids, alternate_source_id)),
        [first_label, alternate_label],
        true,
    )
    return ROCellComplex2D(
        complex.domain,
        complex.output_indices,
        cells,
        complex.facets,
        complex.singular_strata,
        complex.candidate_regime_count + 1,
        complex.regular_candidate_count + 1,
        complex.domain_area,
        complex.covered_area_sum,
        complex.gap_area,
        complex.coverage_complete,
        true,
        complex.geometry_tolerance,
    )
end

function _rofss_signature(
    artifact_sha256;
    axis_ids=["input_a", "input_b"],
    output_ids=["output_ab"],
    ambiguous=false,
)
    complex = _rofss_complex(; ambiguous=ambiguous)
    detached = ROCellComplex2D(
        complex.domain,
        complex.output_indices,
        complex.cells,
        complex.facets,
        complex.singular_strata,
        complex.candidate_regime_count,
        complex.regular_candidate_count,
        complex.domain_area,
        complex.covered_area_sum,
        complex.gap_area,
        complex.coverage_complete,
        complex.has_ambiguity,
        complex.geometry_tolerance,
    )
    return _ROFSS_BACKEND._rofb_classify_ro_cell_complex_impl(
        _ROFSS_BACKEND._ROFB_VALIDATED_ARTIFACT_TOKEN,
        detached,
        artifact_sha256;
        axis_ids=axis_ids, output_ids=output_ids)
end

function _rofss_scalar(db, sql, params=())
    query = DBInterface.execute(db, sql, params)
    try
        for row in query
            return row[1]
        end
    finally
        DBInterface.close!(query)
    end
    return nothing
end

@testset "SQLite 0.5 migration adds normalized RO-field signatures" begin
    @test _ROFSS_BACKEND.ATLAS_SQLITE_SCHEMA_VERSION == "0.5.0"
    source = read(joinpath(@__DIR__, "..", "src", "atlas_sqlite.jl"), String)
    baseline = match(
        r"function atlas_sqlite_init!\(db::SQLite\.DB\)(.*?)# ─── Migration framework"s,
        source,
    )
    @test baseline !== nothing
    @test !occursin("ro_field_signatures", baseline.captures[1])

    mktempdir() do root
        path = joinpath(root, "legacy.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db) ==
                ["0.4.0", "0.5.0"]
            @test isempty(_ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db))
            for table in (
                "ro_field_signatures",
                "ro_field_component_features",
                "ro_field_output_gradient_features",
            )
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type='table' AND name=?", (table,)) == 1
            end
            for index in (
                "idx_ro_field_signatures_classifiable",
                "idx_ro_field_components_classification",
                "idx_ro_field_gradients_family",
            )
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type='index' AND name=?", (index,)) == 1
            end
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.5.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 3
        finally
            SQLite.close(db)
        end
    end

    # A database that already applied the P3 0.4 migration must receive the P4
    # tables exactly once rather than silently skipping them under a reused
    # migration version.
    mktempdir() do root
        path = joinpath(root, "stamped-0.4.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            p3_migrations = _ROFSS_BACKEND.ATLAS_SQLITE_MIGRATIONS[1:2]
            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(
                db; migrations=p3_migrations) == ["0.4.0"]
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.4.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='ro_field_signatures'") == 0

            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db) ==
                ["0.5.0"]
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.5.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='ro_field_signatures'") == 1
            @test isempty(
                _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db))
        finally
            SQLite.close(db)
        end
    end

    # The historical baseline itself records only 0.3.  If P3's 0.4 DDL
    # fails, it cannot advertise the newer schema before that transaction
    # commits.
    mktempdir() do root
        path = joinpath(root, "failed-0.4-migration.sqlite")
        db = SQLite.DB(path)
        try
            DBInterface.execute(db,
                "CREATE TABLE ro_field_artifacts (broken TEXT)")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='atlas_metadata'") == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='schema_migrations'") == 0
        finally
            SQLite.close(db)
        end
    end

    # A deliberately malformed pre-existing table has enough columns for all
    # 0.5 indexes to be created.  The exact postcondition (rather than a lucky
    # late SQL error) must still reject it before the migration is stamped.
    mktempdir() do root
        path = joinpath(root, "failed-migration.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(
                db;
                migrations=_ROFSS_BACKEND.ATLAS_SQLITE_MIGRATIONS[1:2],
            ) == ["0.4.0"]
            DBInterface.execute(db, """
                CREATE TABLE ro_field_signatures (
                    signature_sha256 TEXT PRIMARY KEY,
                    artifact_sha256 TEXT NOT NULL,
                    classifier_version TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    classifiable INTEGER NOT NULL
                )
                """)
            @test_throws ErrorException begin
                _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db)
            end
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.4.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations " *
                "WHERE version='0.5.0'") == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='ro_field_artifacts'") == 1
        finally
            SQLite.close(db)
        end
    end

    # Re-entering the historical baseline after success must not downgrade the
    # 0.5 marker.
    mktempdir() do root
        path = joinpath(root, "successful-migration.sqlite")
        db = _ROFSS_BACKEND.atlas_sqlite_connect(path)
        try
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.5.0"
            _ROFSS_BACKEND.atlas_sqlite_init!(db)
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.5.0"
        finally
            SQLite.close(db)
        end
    end
end

@testset "SQLite 0.3 baseline rejects partial owned tables and indexes" begin
    @test _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
        "CREATE TABLE T (v TEXT CHECK(v = 'RPB2'))") !=
        _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
            "create table t (v text check(v='rpb2'))")
    @test _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
        "CREATE TABLE IF NOT EXISTS T (v TEXT CHECK(v = 'RPB2'))") ==
        _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
            "create table t(v text check(v='RPB2'))")
    @test _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
        "CREATE TABLE T (v TEXT CHECK(v='createtableifnotexists'))") !=
        _ROFSS_BACKEND._atlas_sqlite_normalized_schema_sql(
            "CREATE TABLE T (v TEXT CHECK(v='createtable'))")

    for (name, ddl) in (
        "input_graph_slices" =>
            "CREATE TABLE input_graph_slices (junk TEXT)",
        "witness_paths" =>
            "CREATE TABLE witness_paths (junk TEXT)",
    )
        mktempdir() do root
            db = SQLite.DB(joinpath(root, "partial-$(name).sqlite"))
            try
                DBInterface.execute(db, ddl)
                before = _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master")
                @test_throws ErrorException begin
                    _ROFSS_BACKEND.atlas_sqlite_init!(db)
                end
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master") == before
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type='table' AND name='atlas_metadata'") == 0
                @test _ROFSS_BACKEND._atlas_sqlite_table_shape(db, name) == [
                    (name="junk", type="TEXT", notnull=0, pk=0),
                ]
            finally
                SQLite.close(db)
            end
        end
    end

    mktempdir() do root
        db = SQLite.DB(joinpath(root, "partial-index.sqlite"))
        try
            DBInterface.execute(db,
                "CREATE TABLE unrelated " *
                "(change_signature TEXT, output_symbol TEXT)")
            DBInterface.execute(db,
                "CREATE INDEX idx_path_change ON unrelated " *
                "(change_signature, output_symbol)")
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='atlas_metadata'") == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='index' AND name='idx_path_change'") == 1
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        db = SQLite.DB(joinpath(root, "missing-baseline-index.sqlite"))
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            DBInterface.execute(db, "DROP INDEX idx_path_change")
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='index' AND name='idx_path_change'") == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='schema_migrations'") == 0
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.3.0"
        finally
            SQLite.close(db)
        end
    end

    for (label, setup, kind, name) in (
        (
            "future-table",
            db -> DBInterface.execute(
                db, "CREATE TABLE ro_field_artifacts (junk TEXT)"),
            "table",
            "ro_field_artifacts",
        ),
        (
            "future-index",
            db -> begin
                DBInterface.execute(
                    db, "CREATE TABLE future_index_carrier (field_id TEXT)")
                DBInterface.execute(db,
                    "CREATE INDEX idx_ro_fields_field_id " *
                    "ON future_index_carrier (field_id)")
            end,
            "index",
            "idx_ro_fields_field_id",
        ),
    )
        mktempdir() do root
            db = SQLite.DB(joinpath(root, "$(label).sqlite"))
            try
                _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
                setup(db)
                before = _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master")
                @test_throws ErrorException begin
                    _ROFSS_BACKEND.atlas_sqlite_init!(db)
                end
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master") == before
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type=? AND name=?", (kind, name)) == 1
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type='table' AND name='schema_migrations'") == 0
                @test _rofss_scalar(db,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='schema_version'") == "0.3.0"
            finally
                SQLite.close(db)
            end
        end
    end
end

@testset "SQLite stamped schemas are revalidated before any repair DDL" begin
    mktempdir() do root
        db = SQLite.DB(joinpath(root, "ledger-without-unique.sqlite"))
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            DBInterface.execute(db, """
                CREATE TABLE schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL
                        CHECK('versiontextnotnullunique' <> ''),
                    applied_at TEXT NOT NULL,
                    description TEXT
                )
                """)
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master")
            @test _ROFSS_BACKEND._atlas_sqlite_table_shape(
                db, "schema_migrations") ==
                _ROFSS_BACKEND._ATLAS_SQLITE_MIGRATION_LEDGER_SHAPE
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 0
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.3.0"
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        db = SQLite.DB(joinpath(root, "ledger-only.sqlite"))
        try
            DBInterface.execute(db,
                "CREATE TABLE atlas_metadata " *
                "(key TEXT PRIMARY KEY, value_text TEXT NOT NULL)")
            DBInterface.execute(db,
                "INSERT INTO atlas_metadata (key, value_text) " *
                "VALUES ('schema_version', '0.5.0')")
            DBInterface.execute(db, """
                CREATE TABLE schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL UNIQUE,
                    applied_at TEXT NOT NULL,
                    description TEXT
                )
                """)
            for version in ("0.3.0", "0.4.0", "0.5.0")
                DBInterface.execute(db,
                    "INSERT INTO schema_migrations " *
                    "(version, applied_at, description) VALUES (?, ?, ?)",
                    (version, "2026-01-01T00:00:00Z", "fixture"))
            end
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 3
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='library_state'") == 0
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        db = SQLite.DB(joinpath(root, "spoofed-plan.sqlite"))
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            canonical = _ROFSS_BACKEND.ATLAS_SQLITE_MIGRATIONS
            spoofed_step = _ROFSS_BACKEND.AtlasSqliteMigration(
                canonical[2].version,
                canonical[2].description,
                _ -> nothing,
                _ -> nothing,
            )
            @test canonical isa Tuple
            @test_throws MethodError setindex!(canonical, spoofed_step, 2)
            mutable_plan = collect(canonical[1:2])
            validated_plan =
                _ROFSS_BACKEND._atlas_sqlite_validate_migration_plan!(
                    mutable_plan)
            mutable_plan[2] = spoofed_step
            @test validated_plan isa Tuple
            @test validated_plan[2] === canonical[2]
            spoofed = _ROFSS_BACKEND.AtlasSqliteMigration[
                canonical[1],
                spoofed_step,
            ]
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(
                    db; migrations=spoofed)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='schema_migrations'") == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='ro_field_artifacts'") == 0
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.3.0"
        finally
            SQLite.close(db)
        end
    end

    for (label, tamper, object_kind, object_name, expected_count) in (
        (
            "stamped-table",
            db -> DBInterface.execute(
                db, "DROP TABLE ro_field_output_gradient_features"),
            "table",
            "ro_field_output_gradient_features",
            0,
        ),
        (
            "stamped-index",
            db -> DBInterface.execute(
                db, "DROP INDEX idx_ro_fields_field_id"),
            "index",
            "idx_ro_fields_field_id",
            0,
        ),
        (
            "stamped-partial-index",
            db -> begin
                DBInterface.execute(
                    db, "DROP INDEX idx_ro_field_signatures_artifact")
                DBInterface.execute(db,
                    "CREATE INDEX idx_ro_field_signatures_artifact " *
                    "ON ro_field_signatures (artifact_sha256) WHERE 0")
            end,
            "index",
            "idx_ro_field_signatures_artifact",
            1,
        ),
    )
        mktempdir() do root
            db = _ROFSS_BACKEND.atlas_sqlite_connect(
                joinpath(root, "$(label).sqlite"))
            try
                tamper(db)
                before = _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master")
                @test_throws ErrorException begin
                    _ROFSS_BACKEND.atlas_sqlite_init!(db)
                end
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master") == before
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master " *
                    "WHERE type=? AND name=?",
                    (object_kind, object_name)) == expected_count
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM schema_migrations") == 3
                @test _rofss_scalar(db,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='schema_version'") == "0.5.0"
            finally
                SQLite.close(db)
            end
        end
    end

    mktempdir() do root
        db = SQLite.DB(joinpath(root, "stamped-foreign-key.sqlite"))
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            p3_migrations =
                _ROFSS_BACKEND.ATLAS_SQLITE_MIGRATIONS[1:2]
            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(
                db; migrations=p3_migrations) == ["0.4.0"]
            DBInterface.execute(db, "PRAGMA foreign_keys = OFF")
            @test _rofss_scalar(db, "PRAGMA foreign_keys") == 0
            DBInterface.execute(db, "DROP TABLE ro_field_artifacts")
            DBInterface.execute(db, """
                CREATE TABLE ro_field_artifacts (
                    artifact_sha256 TEXT PRIMARY KEY,
                    field_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    representation TEXT NOT NULL,
                    network_ir_sha256 TEXT NOT NULL,
                    domain_sha256 TEXT NOT NULL,
                    data_sha256 TEXT NOT NULL,
                    cell_complex_hash TEXT,
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
            error = try
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
                nothing
            catch err
                err
            end
            @test error isa ErrorException
            @test occursin("foreign keys", sprint(showerror, error))
            @test isempty(
                _ROFSS_BACKEND._atlas_sqlite_foreign_key_shape(
                    db, "ro_field_artifacts"))
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 2
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.4.0"
        finally
            SQLite.close(db)
        end
    end
end

@testset "SQLite migrations reject future and inconsistent version ledgers before DDL" begin
    mktempdir() do root
        path = joinpath(root, "future.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            DBInterface.execute(db,
                "UPDATE atlas_metadata SET value_text='9.0.0' " *
                "WHERE key='schema_version'")
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "9.0.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='schema_migrations'") == 0
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        path = joinpath(root, "inconsistent.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            DBInterface.execute(db,
                "UPDATE atlas_metadata SET value_text='0.5.0' " *
                "WHERE key='schema_version'")
            DBInterface.execute(db, """
                CREATE TABLE schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL UNIQUE,
                    applied_at TEXT NOT NULL,
                    description TEXT
                )
                """)
            DBInterface.execute(db,
                "INSERT INTO schema_migrations " *
                "(version, applied_at, description) VALUES (?, ?, ?)",
                ("0.3.0", "2026-01-01T00:00:00Z", "baseline"))
            DBInterface.execute(db,
                "INSERT INTO schema_migrations " *
                "(version, applied_at, description) VALUES (?, ?, ?)",
                ("0.4.0", "2026-01-01T00:00:01Z", "P3"))
            @test_throws ErrorException begin
                _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 2
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.5.0"
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        path = joinpath(root, "future-ledger.sqlite")
        db = SQLite.DB(path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(db)
            DBInterface.execute(db,
                "UPDATE atlas_metadata SET value_text='0.5.0' " *
                "WHERE key='schema_version'")
            DBInterface.execute(db, """
                CREATE TABLE schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT NOT NULL UNIQUE,
                    applied_at TEXT NOT NULL,
                    description TEXT
                )
                """)
            for version in ("0.3.0", "0.4.0", "0.5.0", "9.0.0")
                DBInterface.execute(db,
                    "INSERT INTO schema_migrations " *
                    "(version, applied_at, description) VALUES (?, ?, ?)",
                    (version, "2026-01-01T00:00:00Z", "fixture"))
            end
            before = _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            @test_throws ErrorException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'") == before
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations") == 4
        finally
            SQLite.close(db)
        end
    end

    for (label, versions) in (
        "out-of-order-ledger" => ("0.5.0", "0.3.0", "0.4.0"),
        "gapped-ledger" => ("0.3.0", "0.5.0"),
    )
        mktempdir() do root
            db = _ROFSS_BACKEND.atlas_sqlite_connect(
                joinpath(root, "$(label).sqlite"))
            try
                DBInterface.execute(db, "DELETE FROM schema_migrations")
                for version in versions
                    DBInterface.execute(db,
                        "INSERT INTO schema_migrations " *
                        "(version, applied_at, description) VALUES (?, ?, ?)",
                        (version, "2026-01-01T00:00:00Z", "fixture"))
                end
                @test _ROFSS_BACKEND._atlas_sqlite_ledger_version_list(db) ==
                    collect(versions)
                @test_throws SQLite.SQLiteException begin
                    DBInterface.execute(db,
                        "INSERT INTO schema_migrations " *
                        "(version, applied_at, description) VALUES (?, ?, ?)",
                        (first(versions), "2026-01-01T00:00:01Z", "duplicate"))
                end
                before = _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master")
                @test_throws ErrorException begin
                    _ROFSS_BACKEND.atlas_sqlite_init!(db)
                end
                @test _rofss_scalar(db,
                    "SELECT COUNT(*) FROM sqlite_master") == before
                @test _ROFSS_BACKEND._atlas_sqlite_ledger_version_list(db) ==
                    collect(versions)
                @test _rofss_scalar(db,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='schema_version'") == "0.5.0"
            finally
                SQLite.close(db)
            end
        end
    end
end

@testset "signature persistence is strict, idempotent, and normalized" begin
    mktempdir() do root
        path = joinpath(root, "atlas.sqlite")
        db = _ROFSS_BACKEND.atlas_sqlite_connect(path)
        try
            exact = _rofss_exact_fixture("signature-exact")
            exact_alias = _rofss_ambiguous_exact_fixture(
                "signature-exact-alias")
            exact_keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                db, exact)
            alias_keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                db, exact_alias)

            signature = _rofss_signature(exact_keys["artifact_sha256"])
            @test signature["classifiable"]
            keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                db, signature)
            @test keys["signature_sha256"] ==
                signature["signature_sha256"]
            @test keys["artifact_sha256"] ==
                exact_keys["artifact_sha256"]
            @test occursin(r"^[0-9a-f]{64}$", keys["config_sha256"])
            @test occursin(r"^[0-9a-f]{64}$",
                keys["signature_json_sha256"])

            loaded = _ROFSS_BACKEND.atlas_sqlite_load_ro_field_signature(
                db, keys["signature_sha256"])
            @test _ROFSS_BACKEND._ro_field_canonical_json(loaded) ==
                _ROFSS_BACKEND._ro_field_canonical_json(signature)

            # Retry performs a full collision/normalized-row check and never
            # creates duplicate parent or child rows.
            @test _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                db, signature) == keys
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_signatures") == 1
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_component_features") == 2
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_output_gradient_features") == 1

            unknown_signature = _rofss_signature(
                alias_keys["artifact_sha256"];
                ambiguous=true,
            )
            @test !unknown_signature["classifiable"]
            unknown_keys =
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, unknown_signature)
            @test unknown_keys["signature_sha256"] ==
                unknown_signature["signature_sha256"]
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_signatures") == 2

            fabricated = _rofss_signature(
                exact_keys["artifact_sha256"];
                ambiguous=true,
            )
            fabricated_error = try
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, fabricated)
                nothing
            catch err
                err
            end
            @test fabricated_error isa
                _ROFSS_BACKEND.ROFieldSignatureStorageError
            @test fabricated_error.code == :signature_artifact_mismatch

            # A valid signature cannot be rebound by changing only its ordered
            # semantic labels, field hash, or representation.
            swapped_axes = _rofss_signature(
                exact_keys["artifact_sha256"];
                axis_ids=["input_b", "input_a"],
            )
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, swapped_axes)
            end
            wrong_output = _rofss_signature(
                exact_keys["artifact_sha256"];
                output_ids=["wrong_output"],
            )
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, wrong_output)
            end
            foreign = _rofss_signature(repeat("f", 64))
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, foreign)
            end

            sampled = _rofss_sampled_fixture("signature-sampled")
            sampled_keys =
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                    db, sampled)
            sampled_signature = _rofss_signature(
                sampled_keys["artifact_sha256"];
                output_ids=["output_x"],
            )
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, sampled_signature)
            end

            unsupported = deepcopy(signature)
            unsupported["schema_version"] =
                "bne-ro-field-signature/v9.0.0"
            @test_throws Exception begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, unsupported)
            end
            wrong_hash = deepcopy(signature)
            wrong_hash["signature_sha256"] = repeat("0", 64)
            @test_throws Exception begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, wrong_hash)
            end

            # Even a caller that recomputes the complete signature hash cannot
            # alter a diagnostic derived from the referenced artifact.
            diagnostic_collision = deepcopy(signature)
            diagnostic_collision["diagnostics"][
                "excluded_lower_dimensional_strata_count"] += 1
            diagnostic_collision["signature_sha256"] =
                _ROFSS_BACKEND.canonical_hash(
                    _ROFSS_BACKEND.ro_field_signature_identity_payload(
                        diagnostic_collision))
            @test diagnostic_collision["signature_sha256"] !=
                signature["signature_sha256"]
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, diagnostic_collision)
            end
        finally
            SQLite.close(db)
        end
    end
end

@testset "SQLite DB overloads preserve transactions and enforce foreign identities" begin
    mktempdir() do root
        db = SQLite.DB(joinpath(root, "uninitialized.sqlite"))
        try
            @test_throws SQLite.SQLiteException begin
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(db)
            end
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'") == 0
            @test _rofss_scalar(db, "PRAGMA foreign_keys") == 1
        finally
            SQLite.close(db)
        end
    end

    mktempdir() do root
        path = joinpath(root, "nested.sqlite")
        db = _ROFSS_BACKEND.atlas_sqlite_connect(path)
        try
            @test _rofss_scalar(db, "PRAGMA foreign_keys") == 1
            DBInterface.execute(db,
                "UPDATE atlas_metadata SET value_text='sentinel' " *
                "WHERE key='updated_at'")
            DBInterface.execute(db, "BEGIN IMMEDIATE")
            @test SQLite.intransaction(db)

            exact = _rofss_exact_fixture("nested-save")
            keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                db, exact)
            signature = _rofss_signature(keys["artifact_sha256"])
            signature_keys =
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    db, signature)
            @test SQLite.intransaction(db)
            @test length(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_artifacts(
                    db; field_id="nested-save")) == 1
            @test length(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db; artifact_sha256=keys["artifact_sha256"])) == 1
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='updated_at'") == "sentinel"
            DBInterface.execute(db, "COMMIT")
            @test !SQLite.intransaction(db)

            @test_throws SQLite.SQLiteException begin
                DBInterface.execute(db, """
                    INSERT INTO ro_field_component_features (
                        signature_sha256, output_position, axis_position,
                        output_id, axis_id, classification
                    ) VALUES (?, 1, 1, 'output', 'input', 'zero')
                    """, (repeat("0", 64),))
            end

            DBInterface.execute(db,
                "DELETE FROM ro_field_artifacts WHERE artifact_sha256 = ?",
                (keys["artifact_sha256"],))
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_signatures " *
                "WHERE signature_sha256 = ?",
                (signature_keys["signature_sha256"],)) == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_component_features " *
                "WHERE signature_sha256 = ?",
                (signature_keys["signature_sha256"],)) == 0
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM ro_field_output_gradient_features " *
                "WHERE signature_sha256 = ?",
                (signature_keys["signature_sha256"],)) == 0
        finally
            SQLite.intransaction(db) && DBInterface.execute(db, "ROLLBACK")
            SQLite.close(db)
        end
    end
end

@testset "SQLite verified queries work through a truly read-only connection" begin
    mktempdir() do root
        path = joinpath(root, "readonly.sqlite")
        artifact_sha256 = ""
        signature_sha256 = ""
        writer = _ROFSS_BACKEND.atlas_sqlite_connect(path)
        try
            exact = _rofss_exact_fixture("readonly-query")
            artifact_keys =
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                    writer, exact)
            signature = _rofss_signature(
                artifact_keys["artifact_sha256"])
            signature_keys =
                _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(
                    writer, signature)
            artifact_sha256 = artifact_keys["artifact_sha256"]
            signature_sha256 = signature_keys["signature_sha256"]
            DBInterface.execute(writer,
                "UPDATE atlas_metadata SET value_text='readonly-sentinel' " *
                "WHERE key='updated_at'")
        finally
            SQLite.close(writer)
        end

        encoded_path = replace(
            path, "%" => "%25", "?" => "%3F", "#" => "%23")
        reader = SQLite.DB("file:$(encoded_path)?mode=ro")
        try
            @test _rofss_scalar(reader, "PRAGMA foreign_keys") == 0
            @test _ROFSS_BACKEND.atlas_sqlite_load_ro_field_artifact(
                reader, artifact_sha256) !== nothing
            @test _ROFSS_BACKEND.atlas_sqlite_load_ro_field_signature(
                reader, signature_sha256) !== nothing
            @test length(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_artifacts(
                    reader; field_id="readonly-query")) == 1
            @test length(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    reader; artifact_sha256=artifact_sha256)) == 1
            @test _rofss_scalar(reader, "PRAGMA foreign_keys") == 1
            @test _rofss_scalar(reader,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='updated_at'") == "readonly-sentinel"
        finally
            SQLite.close(reader)
        end
    end
end

@testset "path and HTTP Atlas queries use one read transaction and never migrate" begin
    mktempdir() do parent
        root = joinpath(parent, "allowed")
        mkpath(root)
        query = Dict{String,Any}("limit" => 1)
        function http_query(path)
            request = HTTP.Request(
                "POST",
                "/api/v1/query_atlas",
                ["Content-Type" => "application/json"],
                JSON3.write(Dict{String,Any}(
                    "sqlite_path" => path,
                    "query" => query,
                )),
            )
            return withenv(
                "BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS" => "1",
                "BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT" => root,
            ) do
                _ROFSS_BACKEND.router(request)
            end
        end

        missing_path = joinpath(root, "missing-query.sqlite")
        @test_throws ArgumentError _ROFSS_BACKEND.atlas_sqlite_load_query_corpus(
            missing_path, query)
        @test !ispath(missing_path)

        current_path = joinpath(root, "current % # query.sqlite")
        current_writer = _ROFSS_BACKEND.atlas_sqlite_connect(current_path)
        SQLite.close(current_writer)
        current_before = read(current_path)
        current_mtime = stat(current_path).mtime
        current_corpus = _ROFSS_BACKEND.atlas_sqlite_load_query_corpus(
            current_path, query)
        @test isempty(current_corpus["network_entries"])
        @test read(current_path) == current_before
        @test stat(current_path).mtime == current_mtime
        readonly = _ROFSS_BACKEND._atlas_sqlite_connect_readonly(current_path)
        try
            @test SQLite.C.sqlite3_db_readonly(
                getfield(readonly, :handle), "main") == 1
            @test _rofss_scalar(readonly, "PRAGMA query_only") == 1
            @test SQLite.intransaction(readonly)
            @test_throws SQLite.SQLiteException DBInterface.execute(
                readonly,
                "UPDATE atlas_metadata SET value_text='mutated' " *
                "WHERE key='schema_version'",
            )
        finally
            SQLite.close(readonly)
        end
        current_response = http_query(current_path)
        @test current_response.status == 200
        @test read(current_path) == current_before
        @test stat(current_path).mtime == current_mtime

        active_wal_path = joinpath(root, "active-wal.sqlite")
        active_writer = _ROFSS_BACKEND.atlas_sqlite_connect(active_wal_path)
        try
            DBInterface.execute(active_writer, "PRAGMA wal_autocheckpoint = 0")
            DBInterface.execute(
                active_writer,
                "UPDATE atlas_metadata SET value_text='wal-visible' " *
                "WHERE key='updated_at'",
            )
            active_wal = active_wal_path * "-wal"
            @test isfile(active_wal)
            active_main_before = read(active_wal_path)
            active_wal_before = read(active_wal)
            active_reader = _ROFSS_BACKEND._atlas_sqlite_connect_readonly(
                active_wal_path)
            try
                @test _rofss_scalar(
                    active_reader,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='updated_at'",
                ) == "wal-visible"
            finally
                SQLite.close(active_reader)
            end
            @test read(active_wal_path) == active_main_before
            @test read(active_wal) == active_wal_before

            snapshot_reader = _ROFSS_BACKEND._atlas_sqlite_connect_readonly(
                active_wal_path)
            try
                @test _rofss_scalar(
                    snapshot_reader,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='updated_at'",
                ) == "wal-visible"
                concurrent_writer = SQLite.DB(active_wal_path)
                try
                    DBInterface.execute(
                        concurrent_writer,
                        "UPDATE atlas_metadata SET value_text='writer-newer' " *
                        "WHERE key='updated_at'",
                    )
                finally
                    SQLite.close(concurrent_writer)
                end
                @test _rofss_scalar(
                    snapshot_reader,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='updated_at'",
                ) == "wal-visible"
            finally
                SQLite.close(snapshot_reader)
            end
            fresh_reader = _ROFSS_BACKEND._atlas_sqlite_connect_readonly(
                active_wal_path)
            try
                @test _rofss_scalar(
                    fresh_reader,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='updated_at'",
                ) == "writer-newer"
            finally
                SQLite.close(fresh_reader)
            end
            @test http_query(active_wal_path).status == 200
        finally
            SQLite.close(active_writer)
        end

        legacy_path = joinpath(root, "legacy-query.sqlite")
        legacy_writer = SQLite.DB(legacy_path)
        try
            _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(legacy_writer)
        finally
            SQLite.close(legacy_writer)
        end
        legacy_before = read(legacy_path)
        legacy_mtime = stat(legacy_path).mtime

        direct_error = try
            _ROFSS_BACKEND.atlas_sqlite_load_query_corpus(
                legacy_path, query)
            nothing
        catch err
            err
        end
        @test direct_error isa ArgumentError
        @test occursin("migration", lowercase(sprint(showerror, direct_error)))
        @test read(legacy_path) == legacy_before
        @test stat(legacy_path).mtime == legacy_mtime

        response = http_query(legacy_path)
        @test response.status == 400
        @test occursin("migration", lowercase(String(response.body)))
        @test read(legacy_path) == legacy_before
        @test stat(legacy_path).mtime == legacy_mtime

        inspector = SQLite.DB(
            _ROFSS_BACKEND._atlas_sqlite_readonly_uri(legacy_path))
        try
            @test SQLite.C.sqlite3_db_readonly(
                getfield(inspector, :handle), "main") == 1
            @test _rofss_scalar(inspector,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.3.0"
            @test _rofss_scalar(inspector,
                "SELECT COUNT(*) FROM sqlite_master " *
                "WHERE type='table' AND name='schema_migrations'") == 0
        finally
            SQLite.close(inspector)
        end

        future_path = joinpath(root, "future-query.sqlite")
        future_writer = _ROFSS_BACKEND.atlas_sqlite_connect(future_path)
        try
            DBInterface.execute(
                future_writer,
                "UPDATE atlas_metadata SET value_text='9.9.9' " *
                "WHERE key='schema_version'",
            )
        finally
            SQLite.close(future_writer)
        end
        future_before = read(future_path)
        future_mtime = stat(future_path).mtime
        future_error = try
            _ROFSS_BACKEND.atlas_sqlite_load_query_corpus(future_path, query)
            nothing
        catch err
            err
        end
        @test future_error isa ArgumentError
        @test occursin("unsupported", lowercase(sprint(showerror, future_error)))
        future_response = http_query(future_path)
        @test future_response.status == 400
        @test occursin("unsupported", lowercase(String(future_response.body)))
        @test read(future_path) == future_before
        @test stat(future_path).mtime == future_mtime

        corrupt_path = joinpath(root, "corrupt-query.sqlite")
        cp(current_path, corrupt_path)
        corrupt_writer = SQLite.DB(corrupt_path)
        try
            DBInterface.execute(
                corrupt_writer,
                "DROP TABLE ro_field_output_gradient_features",
            )
        finally
            SQLite.close(corrupt_writer)
        end
        corrupt_before = read(corrupt_path)
        corrupt_mtime = stat(corrupt_path).mtime
        corrupt_error = try
            _ROFSS_BACKEND.atlas_sqlite_load_query_corpus(corrupt_path, query)
            nothing
        catch err
            err
        end
        @test corrupt_error isa ArgumentError
        corrupt_response = http_query(corrupt_path)
        @test corrupt_response.status == 400
        @test read(corrupt_path) == corrupt_before
        @test stat(corrupt_path).mtime == corrupt_mtime

        if !Sys.iswindows()
            outside_name = "outside-$(basename(parent)).sqlite"
            outside_path = joinpath(parent, outside_name)
            outside_writer = SQLite.DB(outside_path)
            try
                _ROFSS_BACKEND._atlas_sqlite_create_0_3_0!(outside_writer)
            finally
                SQLite.close(outside_writer)
            end
            outside_before = read(outside_path)
            outside_mtime = stat(outside_path).mtime
            literal_backslash_path = joinpath(root, "\\..\\$(outside_name)")
            literal_writer = _ROFSS_BACKEND.atlas_sqlite_connect(
                literal_backslash_path)
            SQLite.close(literal_writer)
            literal_before = read(literal_backslash_path)
            literal_response = http_query(literal_backslash_path)
            @test literal_response.status == 200
            @test read(literal_backslash_path) == literal_before
            @test read(outside_path) == outside_before
            @test stat(outside_path).mtime == outside_mtime
            outside_inspector = SQLite.DB(
                _ROFSS_BACKEND._atlas_sqlite_readonly_uri(outside_path))
            try
                @test SQLite.C.sqlite3_db_readonly(
                    getfield(outside_inspector, :handle), "main") == 1
                @test _rofss_scalar(
                    outside_inspector,
                    "SELECT value_text FROM atlas_metadata " *
                    "WHERE key='schema_version'",
                ) == "0.3.0"
            finally
                SQLite.close(outside_inspector)
            end
        end
    end
end

@testset "indexed signature queries are verified retrieval only" begin
    mktempdir() do root
        path = joinpath(root, "atlas.sqlite")
        db = _ROFSS_BACKEND.atlas_sqlite_connect(path)
        try
            exact_keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                db, _rofss_exact_fixture("query-exact"))
            alias_keys = _ROFSS_BACKEND.atlas_sqlite_save_ro_field_artifact!(
                db, _rofss_ambiguous_exact_fixture(
                    "query-exact-alias"))
            signature = _rofss_signature(exact_keys["artifact_sha256"])
            unknown = _rofss_signature(
                alias_keys["artifact_sha256"];
                ambiguous=true,
            )
            _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(db, signature)
            _ROFSS_BACKEND.atlas_sqlite_save_ro_field_signature!(db, unknown)

            component_matches =
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db;
                    component_classification="nonnegative_variable",
                    component_output_id="output_ab",
                    component_axis_id="input_a",
                    include_signatures=true,
                )
            @test length(component_matches) == 1
            @test only(component_matches)["signature_sha256"] ==
                signature["signature_sha256"]
            @test haskey(only(component_matches), "signature")

            gradient_matches =
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db;
                    gradient_family="all_nonnegative",
                    gradient_output_id="output_ab",
                    classifiable=true,
                )
            @test length(gradient_matches) == 1
            @test only(gradient_matches)["artifact_sha256"] ==
                exact_keys["artifact_sha256"]

            unknown_matches =
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db;
                    component_classification="unknown",
                    classifiable=false,
                )
            @test length(unknown_matches) == 1
            @test only(unknown_matches)["signature_sha256"] ==
                unknown["signature_sha256"]

            @test isempty(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db;
                    component_classification="strictly_negative",
                ))
            @test isempty(
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db;
                    artifact_sha256=repeat("e", 64),
                ))
            @test_throws ArgumentError begin
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db; component_classification="impossible")
            end
            @test_throws ArgumentError begin
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db; limit=0)
            end

            # Parent JSON, full-document hash, and normalized feature rows are
            # all independently verified during load and query.
            signature_hash = signature["signature_sha256"]
            original_json = _rofss_scalar(db,
                "SELECT signature_json FROM ro_field_signatures " *
                "WHERE signature_sha256 = ?", (signature_hash,))
            DBInterface.execute(db,
                "UPDATE ro_field_signatures SET signature_json = ? " *
                "WHERE signature_sha256 = ?",
                (String(original_json) * " ", signature_hash))
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_load_ro_field_signature(
                    db, signature_hash)
            end
            DBInterface.execute(db,
                "UPDATE ro_field_signatures SET signature_json = ? " *
                "WHERE signature_sha256 = ?",
                (original_json, signature_hash))

            original_document_hash = _rofss_scalar(db,
                "SELECT signature_json_sha256 FROM ro_field_signatures " *
                "WHERE signature_sha256 = ?", (signature_hash,))
            DBInterface.execute(db,
                "UPDATE ro_field_signatures SET signature_json_sha256 = ? " *
                "WHERE signature_sha256 = ?",
                (repeat("0", 64), signature_hash))
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_load_ro_field_signature(
                    db, signature_hash)
            end
            DBInterface.execute(db,
                "UPDATE ro_field_signatures SET signature_json_sha256 = ? " *
                "WHERE signature_sha256 = ?",
                (original_document_hash, signature_hash))

            DBInterface.execute(db,
                "UPDATE ro_field_component_features " *
                "SET classification = 'strictly_negative' " *
                "WHERE signature_sha256 = ? AND output_position = 1 " *
                "AND axis_position = 1",
                (signature_hash,))
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_load_ro_field_signature(
                    db, signature_hash)
            end
            @test_throws _ROFSS_BACKEND.ROFieldSignatureStorageError begin
                _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(
                    db; classifiable=true)
            end
        finally
            SQLite.close(db)
        end
    end

    # Path overload opens a fresh connection and retains the same verified
    # empty-retrieval semantics.
    mktempdir() do root
        path = joinpath(root, "path-overload.sqlite")
        @test isempty(
            _ROFSS_BACKEND.atlas_sqlite_query_ro_field_signatures(path))
    end
end
