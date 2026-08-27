using Test
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
    alternate_label = ROAffineLabel2D(
        [999],
        first_label.reaction_order_matrix .+ 1.0,
        copy(first_label.output_offset),
    )
    cells[1] = ROCell2D(
        first_cell.id,
        first_cell.vertices,
        first_cell.area,
        first_cell.source_regime_ids,
        [first_label, alternate_label],
        true,
    )
    return ROCellComplex2D(
        complex.domain,
        complex.output_indices,
        cells,
        complex.facets,
        complex.singular_strata,
        complex.candidate_regime_count,
        complex.regular_candidate_count,
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
    return _ROFSS_BACKEND.classify_ro_cell_complex(
        _rofss_complex(; ambiguous=ambiguous),
        artifact_sha256;
        axis_ids=axis_ids,
        output_ids=output_ids,
    )
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
            DBInterface.execute(db,
                "CREATE TABLE atlas_metadata (key TEXT PRIMARY KEY, value_text TEXT NOT NULL)")
            DBInterface.execute(db,
                "INSERT INTO atlas_metadata (key, value_text) VALUES ('schema_version', '0.3.0')")
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
            DBInterface.execute(db,
                "CREATE TABLE atlas_metadata " *
                "(key TEXT PRIMARY KEY, value_text TEXT NOT NULL)")
            DBInterface.execute(db,
                "INSERT INTO atlas_metadata (key, value_text) " *
                "VALUES ('schema_version', '0.3.0')")
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
            @test_throws SQLite.SQLiteException begin
                _ROFSS_BACKEND.atlas_sqlite_init!(db)
            end
            @test _rofss_scalar(db,
                "SELECT value_text FROM atlas_metadata " *
                "WHERE key='schema_version'") == "0.3.0"
            @test _rofss_scalar(db,
                "SELECT COUNT(*) FROM schema_migrations " *
                "WHERE version='0.4.0'") == 0
        finally
            SQLite.close(db)
        end
    end

    # A deliberately malformed pre-existing table makes a 0.5 index fail late
    # in its transaction.  P3 remains committed and metadata must stay at 0.4.
    mktempdir() do root
        path = joinpath(root, "failed-migration.sqlite")
        db = SQLite.DB(path)
        try
            DBInterface.execute(db,
                "CREATE TABLE atlas_metadata " *
                "(key TEXT PRIMARY KEY, value_text TEXT NOT NULL)")
            DBInterface.execute(db,
                "INSERT INTO atlas_metadata (key, value_text) " *
                "VALUES ('schema_version', '0.3.0')")
            @test _ROFSS_BACKEND.apply_atlas_sqlite_migrations!(
                db;
                migrations=_ROFSS_BACKEND.ATLAS_SQLITE_MIGRATIONS[1:2],
            ) == ["0.4.0"]
            DBInterface.execute(db,
                "CREATE TABLE ro_field_signatures (broken TEXT)")
            @test_throws SQLite.SQLiteException begin
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
            diagnostic_collision["diagnostics"]["gap_area"] = -1e-12
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
