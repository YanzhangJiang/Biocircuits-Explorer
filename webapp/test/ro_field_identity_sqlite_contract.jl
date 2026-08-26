using Test
using JSON3
using SQLite
using DBInterface
using BiocircuitsExplorerBackend

const Backend = BiocircuitsExplorerBackend
if !isdefined(Backend, :RO_CELL_COMPLEX_MAGIC)
    Base.include(Backend, joinpath(@__DIR__, "..", "src", "ro_field_identity.jl"))
end

function _fixture(name)
    path = joinpath(@__DIR__, "..", "..", "tests", "fixtures", "ro_field", name)
    return Backend._materialize(JSON3.read(read(path, String)))
end

function _set_payload_identity!(document)
    bytes = Backend.canonical_ro_field_data_bytes(document)
    storage = document["coverage"]["storage"]
    storage["payload_bytes"] = length(bytes)
    storage["content_sha256"] = Backend._ro_field_sha256(bytes)
    document["provenance"]["domain_sha256"] =
        Backend.canonical_hash(document["domain"])
    return document
end

function _sampled_fixture(; field_id="sampled-contract")
    document = _fixture("sampled-grid.json")
    document["field_id"] = field_id
    return _set_payload_identity!(document)
end

function _exact_fixture(; field_id="exact-contract")
    document = _fixture("exact-cell-complex.json")
    document["field_id"] = field_id
    document["partial"] = false
    document["data"] = Dict{String, Any}(
        "coefficient_encoding" => "float64",
        "source_candidate_regime_count" => 1,
        "regular_candidate_regime_count" => 1,
        "cell_order" => Any["cell-all"],
        "cells" => Any[
            Dict{String, Any}(
                "cell_id" => "cell-all",
                "dimension" => 2,
                "status" => "regular",
                "vertices" => Any[
                    Any[-2.0, -2.0],
                    Any[2.0, -2.0],
                    Any[2.0, 2.0],
                    Any[-2.0, 2.0],
                ],
                "area" => 16.0,
                "source_regime_ids" => Any["regime-all"],
                "label_order" => Any["label-all"],
                "affine_labels" => Any[
                    Dict{String, Any}(
                        "label_id" => "label-all",
                        "source_regime_ids" => Any["regime-all"],
                        "output_offset" => Any[0.0],
                        "reaction_order_matrix" => Any[Any[1.0, 1.0]],
                    ),
                ],
                "set_valued" => false,
            ),
        ],
        "facet_order" => Any["left", "bottom", "right", "top"],
        "facets" => Any[
            Dict{String, Any}(
                "facet_id" => "left",
                "dimension" => 1,
                "kind" => "domain_boundary",
                "endpoints" => Any[Any[-2.0, -2.0], Any[-2.0, 2.0]],
                "incident_cell_ids" => Any["cell-all"],
                "singular_stratum_ids" => Any[],
                "normal" => Any[1.0, 0.0],
                "offset" => 2.0,
                "mixed_sign" => false,
                "domain_side" => "axis1_lower",
            ),
            Dict{String, Any}(
                "facet_id" => "bottom",
                "dimension" => 1,
                "kind" => "domain_boundary",
                "endpoints" => Any[Any[-2.0, -2.0], Any[2.0, -2.0]],
                "incident_cell_ids" => Any["cell-all"],
                "singular_stratum_ids" => Any[],
                "normal" => Any[0.0, 1.0],
                "offset" => 2.0,
                "mixed_sign" => false,
                "domain_side" => "axis2_lower",
            ),
            Dict{String, Any}(
                "facet_id" => "right",
                "dimension" => 1,
                "kind" => "domain_boundary",
                "endpoints" => Any[Any[2.0, -2.0], Any[2.0, 2.0]],
                "incident_cell_ids" => Any["cell-all"],
                "singular_stratum_ids" => Any[],
                "normal" => Any[1.0, 0.0],
                "offset" => -2.0,
                "mixed_sign" => false,
                "domain_side" => "axis1_upper",
            ),
            Dict{String, Any}(
                "facet_id" => "top",
                "dimension" => 1,
                "kind" => "domain_boundary",
                "endpoints" => Any[Any[-2.0, 2.0], Any[2.0, 2.0]],
                "incident_cell_ids" => Any["cell-all"],
                "singular_stratum_ids" => Any[],
                "normal" => Any[0.0, 1.0],
                "offset" => -2.0,
                "mixed_sign" => false,
                "domain_side" => "axis2_upper",
            ),
        ],
        "singular_stratum_order" => Any[],
        "singular_strata" => Any[],
        "gaps" => Any[],
    )
    document["coverage"]["eligible_count"] = 1
    document["coverage"]["evaluated_count"] = 1
    document["coverage"]["valid_count"] = 1
    document["coverage"]["invalid_count"] = 0
    document["coverage"]["omitted_count"] = 0
    document["coverage"]["enumeration_complete"] = true
    document["coverage"]["truncated"] = false
    document["coverage"]["truncation"] = nothing
    document["coverage"]["storage"]["complete"] = true
    document["coverage"]["storage"]["stored_count"] = 1
    document["evidence"]["status"] = "complete"
    document["evidence"]["completeness_claim"] = "complete_over_declared_population"
    return _set_payload_identity!(document)
end

function _reverse_dict_order(value)
    if value isa AbstractDict
        pairs_reversed = reverse(collect(pairs(value)))
        return Dict{String, Any}(
            String(key) => _reverse_dict_order(child) for (key, child) in pairs_reversed
        )
    elseif value isa AbstractVector
        return Any[_reverse_dict_order(child) for child in value]
    end
    return value
end

function _scalar(db, sql, params=())
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

function _unchecked_rpb2_blob(payload)
    payload_bytes = Backend._ro_field_utf8_bytes(
        Backend._ro_field_canonical_json(payload))
    identity_bytes = Backend._ro_field_utf8_bytes(
        Backend.RO_CELL_COMPLEX_IDENTITY_KIND)
    buffer = copy(Backend.RO_CELL_COMPLEX_MAGIC)
    Backend._ro_field_varuint_push!(
        buffer, Backend.RO_CELL_COMPLEX_CODEC_VERSION)
    Backend._ro_field_push_bytes!(buffer, identity_bytes)
    Backend._ro_field_push_bytes!(buffer, payload_bytes)
    return buffer
end

@testset "RPB2 exact cell-complex identity" begin
    document = _exact_fixture()
    reordered = _reverse_dict_order(document)
    blob = Backend.encode_ro_cell_complex_blob(document)
    @test blob[1:4] == UInt8['R', 'P', 'B', '2']
    @test blob == Backend.encode_ro_cell_complex_blob(reordered)
    @test Backend.ro_cell_complex_hash(blob) == Backend.ro_cell_complex_hash(document)
    @test Backend._ro_field_canonical_json(Backend.decode_ro_cell_complex_blob(blob)) ==
          Backend._ro_field_canonical_json(Backend.canonical_ro_cell_complex_payload(document))

    with_redundant_hrep = deepcopy(document)
    with_redundant_hrep["data"]["cells"][1]["polyhedron"] = Dict{String, Any}(
        "halfspaces" => Any[
            Dict{String, Any}(
                "coefficients" => Any[1.0, 0.0],
                "upper_bound" => 2.0,
                "source" => "domain_upper",
            ),
        ],
    )
    _set_payload_identity!(with_redundant_hrep)
    @test Backend.encode_ro_cell_complex_blob(with_redundant_hrep) == blob

    @test_throws Backend.ROFieldIdentityError Backend.decode_ro_cell_complex_blob(vcat(blob, 0x00))
    @test_throws Backend.ROFieldIdentityError Backend.decode_ro_cell_complex_blob(
        Backend.encode_program_blob([[1.0]])
    )
    wrong_area_payload = deepcopy(Backend.canonical_ro_cell_complex_payload(document))
    wrong_area_payload["data"]["cells"][1]["area"] += 1.0
    wrong_area_error = try
        Backend.decode_ro_cell_complex_blob(_unchecked_rpb2_blob(wrong_area_payload))
        nothing
    catch err
        err
    end
    @test wrong_area_error isa Backend.ROFieldIdentityError
    @test wrong_area_error.code == :invalid_payload

    unexpected_key_payload = deepcopy(
        Backend.canonical_ro_cell_complex_payload(document))
    unexpected_key_payload["data"]["untrusted_extension"] = true
    @test_throws Backend.ROFieldIdentityError Backend.decode_ro_cell_complex_blob(
        _unchecked_rpb2_blob(unexpected_key_payload))
    @test_throws ErrorException Backend.decode_program_blob(blob)

    # Golden RPB1 bytes prove that adding RPB2 did not alter the SISO codec.
    rpb1 = Backend.encode_program_blob([[1.0, 0.0], [-0.5, Inf]])
    @test bytes2hex(rpb1) == "5250423101020200d00f000000e70702"

    sampled = _sampled_fixture()
    @test_throws Backend.ROFieldIdentityError Backend.encode_ro_cell_complex_blob(sampled)

    partial = deepcopy(document)
    partial["partial"] = true
    @test_throws Backend.ROFieldIdentityError Backend.encode_ro_cell_complex_blob(partial)

    incomplete_storage = deepcopy(document)
    incomplete_storage["coverage"]["storage"]["complete"] = false
    @test_throws Backend.ROFieldIdentityError Backend.encode_ro_cell_complex_blob(incomplete_storage)

    with_gap = deepcopy(document)
    with_gap["data"]["gaps"] = Any[
        Dict{String, Any}(
            "gap_id" => "gap-one",
            "reason" => "unclassified",
            "region" => Dict{String, Any}("halfspaces" => Any[]),
        ),
    ]
    @test_throws Backend.ROFieldIdentityError Backend.encode_ro_cell_complex_blob(with_gap)

    set_valued = deepcopy(document)
    set_valued["data"]["cells"][1]["set_valued"] = true
    @test_throws Backend.ROFieldIdentityError Backend.encode_ro_cell_complex_blob(set_valued)
end

@testset "SQLite 0.4 migration is append-only and idempotent" begin
    source = read(joinpath(@__DIR__, "..", "src", "atlas_sqlite.jl"), String)
    baseline = match(r"function atlas_sqlite_init!\(db::SQLite\.DB\)(.*?)# ─── Migration framework"s, source)
    @test baseline !== nothing
    @test !occursin("ro_field_artifacts", baseline.captures[1])
    @test !occursin("ro_cell_complex_identities", baseline.captures[1])

    mktempdir() do root
        path = joinpath(root, "legacy.sqlite")
        db = SQLite.DB(path)
        try
            DBInterface.execute(db,
                "CREATE TABLE atlas_metadata (key TEXT PRIMARY KEY, value_text TEXT NOT NULL)")
            DBInterface.execute(db,
                "INSERT INTO atlas_metadata (key, value_text) VALUES ('schema_version', '0.3.0')")
            p3_migrations = Backend.ATLAS_SQLITE_MIGRATIONS[1:2]
            applied = Backend.apply_atlas_sqlite_migrations!(
                db; migrations=p3_migrations)
            @test applied == ["0.4.0"]
            @test isempty(Backend.apply_atlas_sqlite_migrations!(
                db; migrations=p3_migrations))
            @test _scalar(db, "SELECT value_text FROM atlas_metadata WHERE key='schema_version'") == "0.4.0"
            @test _scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ro_field_artifacts'") == 1
            @test _scalar(db,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ro_cell_complex_identities'") == 1
            @test _scalar(db, "SELECT COUNT(*) FROM schema_migrations") == 2
        finally
            SQLite.close(db)
        end
    end
end

@testset "sampled and exact RO fields round-trip with strict identities" begin
    mktempdir() do root
        path = joinpath(root, "atlas.sqlite")
        db = Backend.atlas_sqlite_connect(path)
        try
            sampled = _sampled_fixture()
            exact = _exact_fixture()
            partial_exact = _set_payload_identity!(_fixture("exact-cell-complex.json"))
            sampled_keys = Backend.atlas_sqlite_save_ro_field_artifact!(db, sampled)
            exact_keys = Backend.atlas_sqlite_save_ro_field_artifact!(db, exact)
            partial_exact_keys = Backend.atlas_sqlite_save_ro_field_artifact!(db, partial_exact)
            @test sampled_keys["cell_complex_hash"] === nothing
            @test exact_keys["cell_complex_hash"] isa String
            @test partial_exact_keys["cell_complex_hash"] === nothing

            sampled_loaded = Backend.atlas_sqlite_load_ro_field_artifact(
                db,
                sampled_keys["artifact_sha256"],
            )
            exact_loaded = Backend.atlas_sqlite_load_ro_field_artifact(
                db,
                exact_keys["artifact_sha256"],
            )
            @test Backend._ro_field_canonical_json(sampled_loaded) ==
                  Backend._ro_field_canonical_json(sampled)
            @test Backend._ro_field_canonical_json(exact_loaded) ==
                  Backend._ro_field_canonical_json(exact)
            @test Backend._ro_field_canonical_json(
                Backend.atlas_sqlite_load_ro_field_artifact(
                    db,
                    partial_exact_keys["artifact_sha256"],
                ),
            ) == Backend._ro_field_canonical_json(partial_exact)

            # A different artifact can reuse the same exact scientific identity.
            exact_alias = deepcopy(exact)
            exact_alias["field_id"] = "exact-contract-alias"
            alias_keys = Backend.atlas_sqlite_save_ro_field_artifact!(db, exact_alias)
            @test alias_keys["artifact_sha256"] != exact_keys["artifact_sha256"]
            @test alias_keys["cell_complex_hash"] == exact_keys["cell_complex_hash"]
            @test _scalar(db, "SELECT COUNT(*) FROM ro_cell_complex_identities") == 1
            @test _scalar(db, "SELECT COUNT(*) FROM ro_field_artifacts") == 4

            exact_results = Backend.atlas_sqlite_query_ro_field_artifacts(
                db;
                representation="exact_cell_complex",
                network_ir_sha256=exact["provenance"]["network_ir_sha256"],
                include_documents=true,
            )
            @test length(exact_results) == 3
            @test all(result -> haskey(result, "document"), exact_results)
            @test isempty(Backend.atlas_sqlite_query_ro_field_artifacts(
                db;
                field_id="does-not-exist",
            ))
            @test_throws ArgumentError Backend.atlas_sqlite_query_ro_field_artifacts(db; limit=0)

            # Retrying is idempotent, but an occupied hash with different bytes
            # is detected instead of silently accepting INSERT OR IGNORE.
            Backend.atlas_sqlite_save_ro_field_artifact!(db, sampled)
            original_json = _scalar(db,
                "SELECT field_json FROM ro_field_artifacts WHERE artifact_sha256 = ?",
                (sampled_keys["artifact_sha256"],),
            )
            DBInterface.execute(db,
                "UPDATE ro_field_artifacts SET field_json = ? WHERE artifact_sha256 = ?",
                (String(original_json) * " ", sampled_keys["artifact_sha256"]),
            )
            @test_throws Backend.ROFieldIdentityError Backend.atlas_sqlite_load_ro_field_artifact(
                db,
                sampled_keys["artifact_sha256"],
            )
            @test_throws Backend.ROFieldIdentityError Backend.atlas_sqlite_save_ro_field_artifact!(db, sampled)
            DBInterface.execute(db,
                "UPDATE ro_field_artifacts SET field_json = ? WHERE artifact_sha256 = ?",
                (original_json, sampled_keys["artifact_sha256"]),
            )

            DBInterface.execute(db,
                "UPDATE ro_field_artifacts SET data_sha256 = ? WHERE artifact_sha256 = ?",
                ("0"^64, sampled_keys["artifact_sha256"]),
            )
            @test_throws Backend.ROFieldIdentityError Backend.atlas_sqlite_load_ro_field_artifact(
                db,
                sampled_keys["artifact_sha256"],
            )

            identity_blob = Vector{UInt8}(_scalar(db,
                "SELECT blob FROM ro_cell_complex_identities WHERE cell_complex_hash = ?",
                (exact_keys["cell_complex_hash"],),
            ))
            corrupt_blob = copy(identity_blob)
            corrupt_blob[end] = xor(corrupt_blob[end], 0x01)
            DBInterface.execute(db,
                "UPDATE ro_cell_complex_identities SET blob = ? WHERE cell_complex_hash = ?",
                (corrupt_blob, exact_keys["cell_complex_hash"]),
            )
            @test_throws Backend.ROFieldIdentityError Backend.atlas_sqlite_load_ro_field_artifact(
                db,
                exact_keys["artifact_sha256"],
            )
        finally
            SQLite.close(db)
        end
    end
end
