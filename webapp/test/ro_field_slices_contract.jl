using Test
using BiocircuitsExplorerBackend

const _ROFSL_BACKEND = BiocircuitsExplorerBackend
if !isdefined(_ROFSL_BACKEND, :RO_FIELD_CHUNK_SCHEMA_VERSION)
    Base.include(_ROFSL_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_chunks.jl"))
end
if !isdefined(_ROFSL_BACKEND, :RO_FIELD_SLICE_SCHEMA_VERSION)
    Base.include(_ROFSL_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_slices.jl"))
end

function _rofsl_dataset(axis_ids, axis_coordinates;
                        invalid_indices=Set{Int}(), work_unit_size=3)
    plan = _ROFSL_BACKEND.build_ro_field_chunk_plan(
        axis_ids=axis_ids,
        output_ids=["sum", "weighted"],
        axis_coordinates=axis_coordinates,
        computation_spec=Dict{String,Any}(
            "algorithm" => "strict-slice-fixture",
            "network_ir_sha256" => repeat("7", 64),
        ),
        work_unit_size=work_unit_size,
    )
    units = _ROFSL_BACKEND.ro_field_plan_work_units(plan)
    chunks = Dict{String,Any}[]
    for unit in units
        samples = Dict{String,Any}[]
        for (position, point) in enumerate(unit["points"])
            point_index = unit["point_indices"][position]
            if point_index in invalid_indices
                push!(samples, Dict{String,Any}(
                    "status" => "invalid",
                    "gap" => Dict{String,Any}(
                        "reason" => "solver_nonconvergence",
                        "detail" => "fixture gap $(point_index)",
                    ),
                ))
            else
                weights = collect(1.0:length(point))
                push!(samples, Dict{String,Any}(
                    "status" => "valid",
                    "output_values" => Any[
                        sum(point),
                        sum(weights .* point),
                    ],
                    "reaction_order_matrix" => Any[
                        Any[10.0 + index for index in eachindex(point)],
                        Any[20.0 + index for index in eachindex(point)],
                    ],
                    "regime_id" => "regular-fixture",
                ))
            end
        end
        push!(chunks, _ROFSL_BACKEND.build_ro_field_chunk(
            plan, unit, samples))
    end
    manifest = _ROFSL_BACKEND.build_ro_field_dataset_manifest(plan, chunks)
    return plan, manifest, chunks
end

function _rofsl_spec(free_axis_ids, fixed_axes)
    return Dict{String,Any}(
        "schema_version" =>
            _ROFSL_BACKEND.RO_FIELD_SLICE_SPEC_SCHEMA_VERSION,
        "free_axis_ids" => free_axis_ids,
        "fixed_axes" => fixed_axes,
    )
end

function _rofsl_fixed(axis_id, coordinate_index, coordinate)
    return Dict{String,Any}(
        "axis_id" => axis_id,
        "coordinate_index" => coordinate_index,
        "coordinate" => coordinate,
    )
end

function _rofsl_loader(chunks, calls::Base.RefValue{Int})
    by_hash = Dict(
        _ROFSL_BACKEND.ro_field_chunk_sha256(chunk) => chunk
        for chunk in chunks)
    return hash -> begin
        calls[] += 1
        get(by_hash, hash, nothing)
    end
end

function _rofsl_rehash_manifest!(manifest)
    identity = copy(manifest)
    pop!(identity, "manifest_sha256")
    manifest["manifest_sha256"] = _ROFSL_BACKEND._rofc_sha256(identity)
    return manifest
end

function _rofsl_rehash_slice!(slice)
    slice["slice_sha256"] =
        _ROFSL_BACKEND._rofc_sha256(slice["identity"])
    return slice
end

@testset "strict 3D Cartesian slice identity and gap preservation" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["x", "y", "z"],
        Any[Any[-1.0, 1.0], Any[0.0, 2.0], Any[10.0, 20.0]];
        invalid_indices=Set([6]),
    )
    spec = _rofsl_spec(["x", "y"], Any[
        _rofsl_fixed("z", 2, 20.0),
    ])
    calls = Ref(0)
    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, _rofsl_loader(chunks, calls), spec;
        runtime_context=Dict{String,Any}(
            "storage_uri" => "file:///tmp/slice-a",
            "created_at" => "2026-07-17T12:00:00Z",
            "job_id" => "job-a",
        ),
    )
    @test calls[] == length(chunks)
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(slice)
    end
    @test _ROFSL_BACKEND.validate_ro_field_slice!(
        slice; source_plan=plan, allow_unverified_source=true) == slice
    @test _ROFSL_BACKEND.validate_ro_field_slice!(
        slice;
        source_plan=plan,
        source_manifest=manifest,
        chunk_source=chunks,
        require_source_provenance=true,
    ) == slice
    identity = slice["identity"]
    @test identity["shape"] == [2, 2]
    @test identity["point_count"] == 4
    @test identity["source_axis_ids"] == ["x", "y", "z"]
    @test getindex.(identity["free_axes"], "axis_id") == ["x", "y"]
    @test identity["output_ids"] == ["sum", "weighted"]
    @test identity["sample_order"] == [2, 4, 6, 8]
    @test getindex.(identity["samples"], "point") == Any[
        [-1.0, 0.0], [-1.0, 2.0], [1.0, 0.0], [1.0, 2.0],
    ]
    @test identity["coverage"] == Dict{String,Any}(
        "source_point_count" => 8,
        "expected_slice_point_count" => 4,
        "selected_point_count" => 4,
        "valid_count" => 3,
        "invalid_count" => 1,
        "complete" => true,
    )
    gap = identity["samples"][3]
    @test gap["status"] == "invalid"
    @test gap["output_values"] === nothing
    @test gap["reaction_order_matrix"] === nothing
    @test gap["regime_id"] === nothing
    @test gap["gap"] == Dict{String,Any}(
        "reason" => "solver_nonconvergence",
        "detail" => "fixture gap 6",
    )
    @test identity["samples"][1]["reaction_order_matrix"] == Any[
        [11.0, 12.0], [21.0, 22.0],
    ]
    @test identity["value_origin"] == "reused_exact"
    @test identity["interpolation"] == "none"
    @test identity["source"]["plan_sha256"] == plan["plan_sha256"]
    @test identity["source"]["manifest_sha256"] ==
        manifest["manifest_sha256"]

    runtime_changed = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec;
        runtime_context=Dict{String,Any}(
            "storage_uri" => "s3://different/location",
            "created_at" => "2035-01-01T00:00:00Z",
            "job_id" => "job-b",
        ),
    )
    @test runtime_changed["slice_sha256"] == slice["slice_sha256"]
    @test runtime_changed["identity"] == slice["identity"]
    @test runtime_changed["runtime_context"] != slice["runtime_context"]

    swapped = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks,
        _rofsl_spec(["y", "x"], Any[_rofsl_fixed("z", 2, 20.0)]))
    @test swapped["slice_sha256"] != slice["slice_sha256"]
    @test swapped["identity"]["sample_order"] == [2, 6, 4, 8]
    @test swapped["identity"]["samples"][1]["reaction_order_matrix"] ==
        Any[[12.0, 11.0], [22.0, 21.0]]

    fixed_changed = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks,
        _rofsl_spec(["x", "y"], Any[_rofsl_fixed("z", 1, 10.0)]))
    @test fixed_changed["slice_sha256"] != slice["slice_sha256"]
    @test fixed_changed["identity"]["sample_order"] == [1, 3, 5, 7]
end

@testset "non-equal 2x3x2x4 Cartesian indexing is source-authenticated" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["a", "b", "c", "d"],
        Any[
            Any[0.0, 1.0],
            Any[10.0, 20.0, 30.0],
            Any[100.0, 200.0],
            Any[1000.0, 2000.0, 3000.0, 4000.0],
        ];
        work_unit_size=7,
    )
    spec = _rofsl_spec(["d", "b"], Any[
        _rofsl_fixed("c", 2, 200.0),
        _rofsl_fixed("a", 2, 1.0),
    ])
    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec)
    @test slice["identity"]["shape"] == [4, 3]
    @test slice["identity"]["sample_order"] == [
        29, 37, 45,
        30, 38, 46,
        31, 39, 47,
        32, 40, 48,
    ]
    @test slice["identity"]["samples"][1]["source_point"] ==
        [1.0, 10.0, 200.0, 1000.0]
    @test _ROFSL_BACKEND.validate_ro_field_slice!(
        slice;
        source_plan=plan,
        source_manifest=manifest,
        chunk_source=chunks,
        require_source_provenance=true,
    ) == slice

    # The projected point still looks geometrically valid, but source index 1
    # maps to a different 4D Cartesian point.  Rehashing the slice must not
    # turn that false index claim into authenticated provenance.
    wrong_source_index = deepcopy(slice)
    wrong_source_index["identity"]["sample_order"][1] = 1
    wrong_source_index["identity"]["samples"][1]["source_point_index"] = 1
    _rofsl_rehash_slice!(wrong_source_index)
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            wrong_source_index;
            source_plan=plan,
            allow_unverified_source=true,
        )
    end
end

@testset "full source chain rejects rehashed semantic forgery" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["x", "y", "z"],
        Any[Any[0.0, 1.0], Any[2.0, 3.0], Any[4.0, 5.0]];
        invalid_indices=Set([3]),
    )
    spec = _rofsl_spec(["x", "y"], Any[
        _rofsl_fixed("z", 1, 4.0),
    ])
    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec)

    numeric_forgery = deepcopy(slice)
    numeric_forgery["identity"]["samples"][1]["output_values"][1] += 99.0
    _rofsl_rehash_slice!(numeric_forgery)
    # Self-hash and plan-only validation intentionally authenticate structure
    # and Cartesian geometry, not the claimed reused source values.
    @test _ROFSL_BACKEND.validate_ro_field_slice!(
        numeric_forgery;
        source_plan=plan,
        allow_unverified_source=true,
    ) == numeric_forgery
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            numeric_forgery;
            source_plan=plan,
            source_manifest=manifest,
            chunk_source=chunks,
            require_source_provenance=true,
        )
    end

    gap_forgery = deepcopy(slice)
    gap_forgery["identity"]["samples"][2]["gap"]["detail"] =
        "invented replacement gap"
    _rofsl_rehash_slice!(gap_forgery)
    @test _ROFSL_BACKEND.validate_ro_field_slice!(
        gap_forgery; allow_unverified_source=true) == gap_forgery
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            gap_forgery;
            source_plan=plan,
            source_manifest=manifest,
            chunk_source=chunks,
            require_source_provenance=true,
        )
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            slice; source_plan=plan, require_source_provenance=true)
    end
end

@testset "strict 4D slice canonicalizes fixed axes and free-axis order" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["a", "b", "c", "d"],
        Any[
            Any[0.0, 1.0], Any[10.0, 20.0],
            Any[100.0, 200.0], Any[1000.0, 2000.0],
        ];
        work_unit_size=5,
    )
    # Input fixed-axis order is deliberately reversed; artifact order follows
    # the source plan so equivalent specifications have one identity.
    spec = _rofsl_spec(["d", "b"], Any[
        _rofsl_fixed("c", 1, 100.0),
        _rofsl_fixed("a", 2, 1.0),
    ])
    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec)
    identity = slice["identity"]
    @test identity["shape"] == [2, 2]
    @test identity["sample_order"] == [9, 13, 10, 14]
    @test getindex.(identity["fixed_axes"], "axis_id") == ["a", "c"]
    @test getindex.(identity["free_axes"], "axis_id") == ["d", "b"]
    @test identity["samples"][1]["reaction_order_matrix"] == Any[
        [14.0, 12.0], [24.0, 22.0],
    ]
    equivalent = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks,
        _rofsl_spec(["d", "b"], Any[
            _rofsl_fixed("a", 2, 1.0),
            _rofsl_fixed("c", 1, 100.0),
        ]))
    @test equivalent["slice_sha256"] == slice["slice_sha256"]
end

@testset "source, spec, and artifact failures stay closed" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["x", "y", "z"],
        Any[Any[0.0, 1.0], Any[2.0, 3.0], Any[4.0, 5.0]],
    )
    spec = _rofsl_spec(["x", "y"], Any[
        _rofsl_fixed("z", 1, 4.0),
    ])

    tampered_manifest = deepcopy(manifest)
    tampered_manifest["manifest_sha256"] = repeat("0", 64)
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, tampered_manifest, chunks, spec)
    end
    incomplete_manifest = deepcopy(manifest)
    pop!(incomplete_manifest["chunks"])
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, incomplete_manifest, chunks, spec)
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, chunks[1:(end - 1)], spec)
    end

    tampered_chunk = deepcopy(chunks[1])
    tampered_chunk["samples"][1]["output_values"][1] += 1.0
    changed_chunks = Any[tampered_chunk; chunks[2:end]]
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, changed_chunks, spec)
    end

    explicit_plan = _ROFSL_BACKEND.build_ro_field_chunk_plan(
        axis_ids=["x", "y", "z"], output_ids=["sum"],
        explicit_points=Any[Any[0.0, 0.0, 0.0]],
        computation_spec=Dict("algorithm" => "not-cartesian"),
    )
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            explicit_plan, manifest, chunks, spec)
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, chunks,
            _rofsl_spec(["x", "y"], Any[
                _rofsl_fixed("z", 3, 6.0),
            ]))
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, chunks,
            _rofsl_spec(["x", "y"], Any[
                _rofsl_fixed("z", 1, 5.0),
            ]))
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, chunks,
            _rofsl_spec(["x", "y"], Any[
                _rofsl_fixed("z", 1, 4.0),
                _rofsl_fixed("z", 1, 4.0),
            ]))
    end

    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec)
    tampered_slice = deepcopy(slice)
    tampered_slice["identity"]["samples"][1]["status"] = "invalid"
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            tampered_slice; allow_unverified_source=true)
    end

    slice = _ROFSL_BACKEND.build_ro_field_slice(
        plan, manifest, chunks, spec)
    foreign_plan, foreign_manifest, foreign_chunks = _rofsl_dataset(
        ["x", "y", "z"],
        Any[Any[0.0, 1.0], Any[2.0, 3.0], Any[4.0, 6.0]],
    )
    foreign_calls = Ref(0)
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            slice;
            source_plan=plan,
            source_manifest=foreign_manifest,
            chunk_source=_rofsl_loader(foreign_chunks, foreign_calls),
            require_source_provenance=true,
        )
    end
    @test foreign_calls[] == 0
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            slice;
            source_plan=plan,
            source_manifest=manifest,
            chunk_source=foreign_chunks,
            require_source_provenance=true,
        )
    end
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            slice;
            source_plan=foreign_plan,
            source_manifest=foreign_manifest,
            chunk_source=foreign_chunks,
            require_source_provenance=true,
        )
    end

    # Manifest entries are exact deterministic work-unit commitments.  A
    # runtime field or re-signed positional mismatch is rejected before any
    # content loader executes.
    runtime_entry = deepcopy(manifest)
    runtime_entry["chunks"][1]["created_at"] = "2030-01-01T00:00:00Z"
    _rofsl_rehash_manifest!(runtime_entry)
    entry_calls = Ref(0)
    @test_throws _ROFSL_BACKEND.ROFieldChunkContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, runtime_entry, _rofsl_loader(chunks, entry_calls), spec)
    end
    @test entry_calls[] == 0

    deeply_nested = deepcopy(manifest)
    nested_hash = deeply_nested["chunks"][1]["chunk_sha256"]
    for _ in 1:8
        nested_hash = Any[nested_hash]
    end
    deeply_nested["chunks"][1]["chunk_sha256"] = nested_hash
    entry_calls[] = 0
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, deeply_nested, _rofsl_loader(chunks, entry_calls), spec)
    end
    @test entry_calls[] == 0

    misaligned_entry = deepcopy(manifest)
    misaligned_entry["chunks"][1]["ordinal"] = 2
    _rofsl_rehash_manifest!(misaligned_entry)
    entry_calls[] = 0
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, misaligned_entry, _rofsl_loader(chunks, entry_calls), spec)
    end
    @test entry_calls[] == 0

    wrong_raw_matrix_shape = deepcopy(slice)
    wrong_raw_matrix_shape["identity"]["samples"][1][
        "reaction_order_matrix"][1] = Any[11.0]
    _rofsl_rehash_slice!(wrong_raw_matrix_shape)
    @test_throws _ROFSL_BACKEND.ROFieldSliceContractError begin
        _ROFSL_BACKEND.validate_ro_field_slice!(
            wrong_raw_matrix_shape; allow_unverified_source=true)
    end
end

@testset "BigInt budgets reject before chunk reads or result allocation" begin
    plan, manifest, chunks = _rofsl_dataset(
        ["x", "y", "z"],
        Any[Any[0.0, 1.0], Any[2.0, 3.0], Any[4.0, 5.0]],
    )
    spec = _rofsl_spec(["x", "y"], Any[
        _rofsl_fixed("z", 1, 4.0),
    ])
    calls = Ref(0)
    cancel_checks = Ref(0)
    @test_throws _ROFSL_BACKEND.ROFieldSliceLimitExceeded begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, _rofsl_loader(chunks, calls), spec;
            slice_limits=_ROFSL_BACKEND.ROFieldSliceLimits(
                max_slice_points=3),
            cancel_check=() -> (cancel_checks[] += 1),
        )
    end
    @test calls[] == 0
    @test cancel_checks[] == 0

    # Re-signing a manifest after lowering all byte declarations cannot bypass
    # the source budget.  The conservative pre-loader reservation is derived
    # from chunk count and the configured per-chunk hard limit.
    low_report = deepcopy(manifest)
    for entry in low_report["chunks"]
        entry["chunk_payload_bytes"] = 1
    end
    low_report["chunk_payload_bytes"] = length(low_report["chunks"])
    _rofsl_rehash_manifest!(low_report)
    calls[] = 0
    cancel_checks[] = 0
    @test_throws _ROFSL_BACKEND.ROFieldSliceLimitExceeded begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, low_report, _rofsl_loader(chunks, calls), spec;
            slice_limits=_ROFSL_BACKEND.ROFieldSliceLimits(
                max_source_payload_bytes=low_report["chunk_payload_bytes"]),
            cancel_check=() -> (cancel_checks[] += 1),
        )
    end
    @test calls[] == 0
    @test cancel_checks[] == 0

    calls[] = 0
    cancel_checks[] = 0
    @test_throws _ROFSL_BACKEND.ROFieldSliceLimitExceeded begin
        _ROFSL_BACKEND.build_ro_field_slice(
            plan, manifest, _rofsl_loader(chunks, calls), spec;
            slice_limits=_ROFSL_BACKEND.ROFieldSliceLimits(
                max_source_payload_bytes=
                    manifest["chunk_payload_bytes"] - 1),
            cancel_check=() -> (cancel_checks[] += 1),
        )
    end
    @test calls[] == 0
    @test cancel_checks[] == 0

    huge = BigInt(typemax(Int)) + BigInt(1)
    error = try
        _ROFSL_BACKEND._rofs_limit(:slice_points, huge, 4096)
        nothing
    catch err
        err
    end
    @test error isa _ROFSL_BACKEND.ROFieldSliceLimitExceeded
    @test error.requested == huge
end
