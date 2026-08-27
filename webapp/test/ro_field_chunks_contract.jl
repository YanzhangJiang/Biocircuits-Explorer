using Test
using JSON3
using SHA
using BiocircuitsExplorerBackend

const _ROFCC_BACKEND = BiocircuitsExplorerBackend
if !isdefined(_ROFCC_BACKEND, :RO_FIELD_CHUNK_SCHEMA_VERSION)
    Base.include(_ROFCC_BACKEND,
        joinpath(@__DIR__, "..", "src", "ro_field_chunks.jl"))
end

struct ROFieldChunkCancelProbe <: Exception end

function _rofcc_plan(;
    runtime_context=Dict{String,Any}(),
    computation_spec=Dict{String,Any}(
        "algorithm" => "demo-sampler",
        "network_ir_sha256" => repeat("a", 64),
    ),
    points=Any[
        Any[2.0, 0.0],
        Any[0.0, 0.0],
        Any[1.0, 1.0],
        Any[0.0, 2.0],
        Any[1.0, 0.0],
    ],
    work_unit_size=2,
    limits=_ROFCC_BACKEND.ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    return _ROFCC_BACKEND.build_ro_field_chunk_plan(
        axis_ids=["input_a", "input_b"],
        output_ids=["output_ab"],
        explicit_points=points,
        computation_spec=computation_spec,
        work_unit_size=work_unit_size,
        runtime_context=runtime_context,
        limits=limits,
        cancel_check=cancel_check,
    )
end

function _rofcc_samples(work_unit; invalid_positions=Set{Int}())
    samples = Dict{String,Any}[]
    for (position, point) in enumerate(work_unit["points"])
        if position in invalid_positions
            push!(samples, Dict{String,Any}(
                "status" => "invalid",
                "gap" => Dict{String,Any}(
                    "reason" => "solver_nonconvergence",
                    "detail" => "bounded demonstration gap",
                ),
            ))
        else
            push!(samples, Dict{String,Any}(
                "status" => "valid",
                "output_values" => Any[sum(point)],
                "reaction_order_matrix" => Any[Any[1.0, 1.0]],
                "regime_id" => "regime-demo",
            ))
        end
    end
    return samples
end

function _rofcc_chunk(plan, work_unit; invalid_positions=Set{Int}())
    return _ROFCC_BACKEND.build_ro_field_chunk(
        plan,
        work_unit,
        _rofcc_samples(work_unit; invalid_positions=invalid_positions),
    )
end

function _rofcc_rehash!(document, hash_key)
    identity = deepcopy(document)
    pop!(identity, String(hash_key))
    document[String(hash_key)] = _ROFCC_BACKEND._rofc_sha256(identity)
    return document
end

@testset "deterministic bounded point and grid plans" begin
    runtime_a = Dict{String,Any}(
        "storage_uri" => "file:///tmp/run-a",
        "created_at" => "2026-07-17T00:00:00Z",
        "job_id" => "job-a",
    )
    runtime_b = Dict{String,Any}(
        "storage_uri" => "file:///different-machine/run-b",
        "created_at" => "2030-01-01T00:00:00Z",
        "job_id" => "job-b",
    )
    plan_a = _rofcc_plan(runtime_context=runtime_a)
    plan_b = _rofcc_plan(runtime_context=runtime_b)
    @test plan_a["plan_sha256"] == plan_b["plan_sha256"]
    @test plan_a["identity"] == plan_b["identity"]
    @test plan_a["runtime_context"] != plan_b["runtime_context"]
    @test _ROFCC_BACKEND.validate_ro_field_chunk_plan!(plan_a) == plan_a
    @test _ROFCC_BACKEND.validate_ro_field_chunk_plan!(plan_b) == plan_b
    @test plan_a["identity"]["explicit_points"] == Any[
        [0.0, 0.0],
        [0.0, 2.0],
        [1.0, 0.0],
        [1.0, 1.0],
        [2.0, 0.0],
    ]

    reordered = _rofcc_plan(points=reverse(
        plan_a["identity"]["explicit_points"]))
    @test reordered["plan_sha256"] == plan_a["plan_sha256"]
    units_a = _ROFCC_BACKEND.ro_field_plan_work_units(plan_a)
    units_b = _ROFCC_BACKEND.ro_field_plan_work_units(plan_b)
    @test units_a == units_b
    @test getindex.(units_a, "point_count") == [2, 2, 1]
    @test [_ROFCC_BACKEND.ro_field_work_unit_sha256(unit, plan_a)
        for unit in units_a] == [
        _ROFCC_BACKEND.ro_field_work_unit_sha256(unit, plan_b)
        for unit in units_b
    ]

    grid = _ROFCC_BACKEND.build_ro_field_chunk_plan(
        axis_ids=["x", "y"],
        output_ids=["z"],
        axis_coordinates=Any[
            Any[-1.0, 1.0],
            Any[10.0, 20.0, 30.0],
        ],
        computation_spec=Dict("algorithm" => "grid-demo"),
        work_unit_size=4,
    )
    grid_units = _ROFCC_BACKEND.ro_field_plan_work_units(grid)
    grid_points = reduce(vcat, getindex.(grid_units, "points"))
    @test grid_points == Any[
        [-1.0, 10.0], [-1.0, 20.0], [-1.0, 30.0],
        [1.0, 10.0], [1.0, 20.0], [1.0, 30.0],
    ]
    @test getindex.(grid_units, "point_count") == [4, 2]

    different_spec = _rofcc_plan(computation_spec=Dict(
        "algorithm" => "different-sampler",
        "network_ir_sha256" => repeat("a", 64),
    ))
    @test different_spec["plan_sha256"] != plan_a["plan_sha256"]
    swapped_axes = _ROFCC_BACKEND.build_ro_field_chunk_plan(
        axis_ids=["input_b", "input_a"],
        output_ids=["output_ab"],
        explicit_points=plan_a["identity"]["explicit_points"],
        computation_spec=plan_a["identity"]["computation_spec"],
        work_unit_size=2,
    )
    @test swapped_axes["plan_sha256"] != plan_a["plan_sha256"]
    for volatile_key in (
        "job_id", "job-id", "jobId", "result_uri", "resultUri",
        "Storage_URI", "storageUri", "createdAt", "wallClockTime",
        "timestamp",
    )
        @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
            _rofcc_plan(computation_spec=Dict(
                "algorithm" => "demo",
                volatile_key => "must-not-be-identity",
            ))
        end
    end
    signed_zero = _rofcc_plan(points=Any[Any[-0.0, 1.0]])
    @test signed_zero["identity"]["explicit_points"] == Any[[0.0, 1.0]]
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _rofcc_plan(points=Any[Any[-0.0, 1.0], Any[0.0, 1.0]])
    end
    duplicate_runtime_keys = Dict{Any,Any}(
        "job_id" => "job-a", :job_id => "job-b")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _rofcc_plan(runtime_context=duplicate_runtime_keys)
    end
    large_finite = _rofcc_plan(points=Any[Any[1.0e20, 0.0]])
    @test _ROFCC_BACKEND.validate_ro_field_chunk_plan!(large_finite) ==
        large_finite
    @test occursin(r"^[0-9a-f]{64}$", large_finite["plan_sha256"])

    duplicate_plan_key = Dict{Any,Any}(pairs(plan_a))
    duplicate_plan_key[:plan_sha256] = plan_a["plan_sha256"]
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_chunk_plan!(duplicate_plan_key)
    end

    tampered_plan = deepcopy(plan_a)
    reverse!(tampered_plan["identity"]["explicit_points"])
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_chunk_plan!(tampered_plan)
    end

    checks = Ref(0)
    callback = () -> (checks[] += 1)
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _rofcc_plan(
            limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_points=4),
            cancel_check=callback,
        )
    end
    @test checks[] == 0
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _ROFCC_BACKEND.build_ro_field_chunk_plan(
            axis_ids=["x", "y"], output_ids=["z"],
            axis_coordinates=Any[Any[0.0, 1.0], Any[0.0, 1.0, 2.0]],
            limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_points=5),
            cancel_check=callback,
        )
    end
    @test checks[] == 0
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _rofcc_plan(
            limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_work_units=2),
            cancel_check=callback,
        )
    end
    @test checks[] == 0

    cancel_checks = Ref(0)
    @test_throws ROFieldChunkCancelProbe begin
        _rofcc_plan(cancel_check=() -> begin
            cancel_checks[] += 1
            cancel_checks[] == 2 && throw(ROFieldChunkCancelProbe())
        end)
    end
    @test cancel_checks[] == 2
end

@testset "canonical chunks preserve explicit gaps and commit atomically" begin
    plan = _rofcc_plan()
    work_unit = first(_ROFCC_BACKEND.ro_field_plan_work_units(plan))
    raw_samples = _rofcc_samples(work_unit; invalid_positions=Set([2]))
    chunk = _ROFCC_BACKEND.build_ro_field_chunk(
        plan, work_unit, raw_samples)
    repeated = _ROFCC_BACKEND.build_ro_field_chunk(
        plan, work_unit, deepcopy(raw_samples))
    @test chunk == repeated
    @test chunk["valid_count"] == 1
    @test chunk["invalid_count"] == 1
    invalid = chunk["samples"][2]
    @test invalid["status"] == "invalid"
    @test invalid["output_values"] === nothing
    @test invalid["reaction_order_matrix"] === nothing
    @test invalid["regime_id"] === nothing
    @test invalid["gap"]["reason"] == "solver_nonconvergence"

    canonical = _ROFCC_BACKEND.canonical_ro_field_chunk_bytes(
        chunk; plan=plan, work_unit=work_unit)
    chunk_hash = _ROFCC_BACKEND.ro_field_chunk_sha256(
        chunk; plan=plan, work_unit=work_unit)
    @test chunk_hash == bytes2hex(SHA.sha256(canonical))
    @test canonical == collect(codeunits(
        _ROFCC_BACKEND._canonical_json(chunk)))

    invalid_substitution = deepcopy(raw_samples)
    invalid_substitution[2]["output_values"] = Any[0.0]
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.build_ro_field_chunk(
            plan, work_unit, invalid_substitution)
    end
    nonfinite = deepcopy(raw_samples)
    nonfinite[1]["output_values"] = Any[NaN]
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.build_ro_field_chunk(plan, work_unit, nonfinite)
    end
    stored_substitution = deepcopy(chunk)
    stored_substitution["samples"][2]["output_values"] = Any[0.0]
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_chunk!(stored_substitution;
            plan=plan, work_unit=work_unit)
    end

    tampered_point = deepcopy(chunk)
    tampered_point["samples"][1]["point"][1] += 0.5
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_chunk!(tampered_point;
            plan=plan, work_unit=work_unit)
    end
    foreign_plan = _rofcc_plan(computation_spec=Dict(
        "algorithm" => "foreign", "network_ir_sha256" => repeat("a", 64)))
    foreign_unit = first(_ROFCC_BACKEND.ro_field_plan_work_units(foreign_plan))
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_chunk!(chunk;
            plan=foreign_plan, work_unit=foreign_unit)
    end

    preflight_checks = Ref(0)
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _ROFCC_BACKEND.build_ro_field_chunk(
            plan, work_unit, raw_samples;
            limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_sample_scalars=5),
            cancel_check=() -> (preflight_checks[] += 1),
        )
    end
    @test preflight_checks[] == 0
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _ROFCC_BACKEND.build_ro_field_chunk(
            plan, work_unit, raw_samples;
            limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_chunk_bytes=128),
        )
    end
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _ROFCC_BACKEND._rofc_materialize(
            Any[1, 2, 3], "bounded document"; max_nodes=3)
    end
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND._rofc_materialize(
            Any[Any[Any[1]]], "bounded document"; max_depth=2)
    end
    @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
        _ROFCC_BACKEND._rofc_materialize(
            Any["abcd"], "bounded document";
            max_total_string_bytes=3)
    end
    long_key = repeat("k", _ROFCC_BACKEND._ROFC_HARD_MAX_STRING_BYTES)
    wide_count = 32_768
    @test ncodeunits(_ROFCC_BACKEND._rofc_object_child_path(
        "bounded document", long_key)) <=
        _ROFCC_BACKEND._ROFC_MAX_DIAGNOSTIC_PATH_BYTES
    wide_document = Dict{String,Any}(long_key => fill(1, wide_count))
    materialized_wide = _ROFCC_BACKEND._rofc_materialize(
        wide_document, "bounded document"; max_nodes=wide_count + 2)
    @test length(materialized_wide[long_key]) == wide_count

    cancel_checks = Ref(0)
    @test_throws ROFieldChunkCancelProbe begin
        _ROFCC_BACKEND.build_ro_field_chunk(
            plan, work_unit, raw_samples;
            cancel_check=() -> begin
                cancel_checks[] += 1
                cancel_checks[] == 2 && throw(ROFieldChunkCancelProbe())
            end,
        )
    end
    @test cancel_checks[] == 2

    mktempdir() do root
        path = _ROFCC_BACKEND.write_ro_field_chunk!(root, chunk;
            plan=plan, work_unit=work_unit)
        @test basename(path) == chunk_hash * ".json"
        @test read(path) == canonical
        @test _ROFCC_BACKEND.write_ro_field_chunk!(root, chunk;
            plan=plan, work_unit=work_unit) == path
        loaded = _ROFCC_BACKEND.read_ro_field_chunk(path;
            expected_sha256=chunk_hash, plan=plan, work_unit=work_unit)
        @test loaded == chunk
        @test _ROFCC_BACKEND.read_ro_field_chunk(path;
            plan=plan, work_unit=work_unit) == chunk
        @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
            _ROFCC_BACKEND.read_ro_field_chunk(path;
                expected_sha256=repeat("0", 64),
                plan=plan, work_unit=work_unit)
        end
        different_chunk = deepcopy(chunk)
        different_chunk["samples"][1]["output_values"][1] += 1.0
        different_bytes = _ROFCC_BACKEND.canonical_ro_field_chunk_bytes(
            different_chunk; plan=plan, work_unit=work_unit)
        open(path, "w") do io
            write(io, different_bytes)
        end
        @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
            _ROFCC_BACKEND.read_ro_field_chunk(path;
                plan=plan, work_unit=work_unit)
        end
        open(path, "w") do io
            write(io, "{}")
        end
        @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
            _ROFCC_BACKEND.read_ro_field_chunk(path;
                expected_sha256=chunk_hash, plan=plan, work_unit=work_unit)
        end
    end

    mktempdir() do root
        directory = joinpath(root, "chunks")
        mkpath(directory)
        path = joinpath(directory, chunk_hash * ".json")
        open(path, "w") do io
            write(io, "{}")
        end
        @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
            _ROFCC_BACKEND.write_ro_field_chunk!(root, chunk;
                plan=plan, work_unit=work_unit)
        end
    end

    mktempdir() do root
        writers = [Threads.@spawn(
            _ROFCC_BACKEND.write_ro_field_chunk!(root, chunk;
                plan=plan, work_unit=work_unit),
        ) for _ in 1:8]
        paths = fetch.(writers)
        @test length(unique(paths)) == 1
        @test read(only(unique(paths))) == canonical
    end

    mktempdir() do root
        oversized = joinpath(root, "oversized.json")
        open(oversized, "w") do io
            write(io, repeat("x", 129))
        end
        @test_throws _ROFCC_BACKEND.ROFieldChunkLimitExceeded begin
            _ROFCC_BACKEND.read_ro_field_chunk(oversized;
                limits=_ROFCC_BACKEND.ROFieldChunkLimits(max_chunk_bytes=128))
        end
    end

    if Sys.isapple() || Sys.islinux()
        @test _ROFCC_BACKEND._rofc_top_level_alias_is_trusted(
            0, 0, 0o040755)
        @test !_ROFCC_BACKEND._rofc_top_level_alias_is_trusted(
            501, 0, 0o040755)
        @test !_ROFCC_BACKEND._rofc_top_level_alias_is_trusted(
            0, 0, 0o040777)

        mktempdir() do sandbox
            outside = mktempdir()
            try
                bridge = joinpath(sandbox, "bridge")
                top_alias = joinpath(sandbox, "top-alias")
                symlink(outside, bridge)
                symlink("bridge", top_alias)
                resolved = _ROFCC_BACKEND._rofc_single_hop_alias_target(
                    top_alias)
                @test resolved == bridge
                @test islink(resolved)
            finally
                rm(outside; recursive=true, force=true)
            end
        end

        mktempdir() do sandbox
            nested_root = joinpath(sandbox, "missing", "parents", "root")
            path = _ROFCC_BACKEND.write_ro_field_chunk!(
                nested_root, chunk; plan=plan, work_unit=work_unit)
            @test isfile(path)
            @test startswith(path, nested_root)
        end

        mktempdir() do sandbox
            missing_root = joinpath(
                sandbox, "read-must-not-create", "nested", "root")
            @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                _ROFCC_BACKEND.read_ro_field_chunk(
                    joinpath(missing_root, "chunks",
                        chunk_hash * ".json");
                    expected_sha256=chunk_hash,
                    plan=plan,
                    work_unit=work_unit,
                    storage_root=missing_root)
            end
            @test !ispath(joinpath(sandbox, "read-must-not-create"))
        end

        mktempdir() do root
            outside = mktempdir()
            try
                symlink(outside, joinpath(root, "chunks"))
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.write_ro_field_chunk!(root, chunk;
                        plan=plan, work_unit=work_unit)
                end
            finally
                rm(outside; recursive=true, force=true)
            end
        end

        mktempdir() do sandbox
            outside = mktempdir()
            root_alias = joinpath(sandbox, "root-alias")
            try
                symlink(outside, root_alias)
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.write_ro_field_chunk!(root_alias, chunk;
                        plan=plan, work_unit=work_unit)
                end
                @test isempty(readdir(outside))
            finally
                rm(outside; recursive=true, force=true)
            end
        end

        mktempdir() do root
            outside = mktempdir()
            try
                mkpath(joinpath(outside, "chunks"))
                symlink(outside, joinpath(root, "alias"))
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.write_ro_field_chunk!(
                        joinpath(root, "alias"), chunk;
                        plan=plan, work_unit=work_unit)
                end
                @test isempty(readdir(joinpath(outside, "chunks")))
                outside_path = joinpath(
                    outside, "chunks", chunk_hash * ".json")
                write(outside_path, canonical)
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.read_ro_field_chunk(
                        joinpath(root, "alias", "chunks",
                            chunk_hash * ".json");
                        expected_sha256=chunk_hash,
                        plan=plan,
                        work_unit=work_unit,
                        storage_root=root)
                end
            finally
                rm(outside; recursive=true, force=true)
            end
        end

        mktempdir() do sandbox
            outside = mktempdir()
            try
                mkpath(joinpath(outside, "container", "root"))
                symlink(outside, joinpath(sandbox, "alias"))
                declared_root = joinpath(
                    sandbox, "alias", "container", "root")
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.write_ro_field_chunk!(
                        declared_root, chunk;
                        plan=plan, work_unit=work_unit)
                end
                outside_chunks = joinpath(
                    outside, "container", "root", "chunks")
                @test !ispath(outside_chunks)

                mkpath(outside_chunks)
                write(joinpath(outside_chunks, chunk_hash * ".json"),
                    canonical)
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND.read_ro_field_chunk(
                        joinpath(declared_root, "chunks",
                            chunk_hash * ".json");
                        expected_sha256=chunk_hash,
                        plan=plan,
                        work_unit=work_unit,
                        storage_root=declared_root)
                end
            finally
                rm(outside; recursive=true, force=true)
            end
        end

        mktempdir() do root
            outside = mktempdir()
            try
                symlink(outside, joinpath(root, "intermediate"))
                @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
                    _ROFCC_BACKEND._rofc_ensure_directory!(
                        joinpath(root, "intermediate", "nested");
                        storage_root=root)
                end
                @test isempty(readdir(outside))
            finally
                rm(outside; recursive=true, force=true)
            end
        end
    end
end

@testset "checkpoint verification resumes only missing work units" begin
    plan = _rofcc_plan()
    units = _ROFCC_BACKEND.ro_field_plan_work_units(plan)
    chunk_1 = _rofcc_chunk(plan, units[1]; invalid_positions=Set([2]))
    chunk_2 = _rofcc_chunk(plan, units[2])
    chunk_3 = _rofcc_chunk(plan, units[3]; invalid_positions=Set([1]))

    checkpoint = _ROFCC_BACKEND.build_ro_field_checkpoint(
        plan, Any[chunk_3, chunk_1])
    @test getindex.(checkpoint["committed"], "ordinal") == [1, 3]
    @test checkpoint["committed_work_unit_count"] == 2
    @test checkpoint["committed_point_count"] == 3
    @test checkpoint["committed_payload_bytes"] == sum(
        entry["chunk_payload_bytes"] for entry in checkpoint["committed"])
    @test all(entry -> entry["chunk_payload_bytes"] > 0,
        checkpoint["committed"])
    @test _ROFCC_BACKEND.validate_ro_field_checkpoint!(
        checkpoint, plan, Any[chunk_1, chunk_3]) == checkpoint

    missing = _ROFCC_BACKEND.resume_ro_field_work_units(
        plan, checkpoint, Any[chunk_3, chunk_1])
    @test getindex.(missing, "ordinal") == [2]
    committed_points = Set(vcat(
        units[1]["point_indices"], units[3]["point_indices"]))
    missing_points = Set(only(missing)["point_indices"])
    @test isempty(intersect(committed_points, missing_points))
    @test union(committed_points, missing_points) == Set(1:5)

    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            checkpoint, plan, Any[chunk_1])
    end
    reversed = deepcopy(checkpoint)
    reverse!(reversed["committed"])
    _rofcc_rehash!(reversed, "checkpoint_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            reversed, plan, Any[chunk_1, chunk_3])
    end
    count_tamper = deepcopy(checkpoint)
    count_tamper["committed_point_count"] += 1
    _rofcc_rehash!(count_tamper, "checkpoint_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            count_tamper, plan, Any[chunk_1, chunk_3])
    end
    byte_tamper = deepcopy(checkpoint)
    byte_tamper["committed_payload_bytes"] += 1
    _rofcc_rehash!(byte_tamper, "checkpoint_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            byte_tamper, plan, Any[chunk_1, chunk_3])
    end
    changed_chunk = deepcopy(chunk_1)
    changed_chunk["samples"][1]["output_values"][1] += 1.0
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            checkpoint, plan, Any[changed_chunk, chunk_3])
    end
    foreign_plan = _rofcc_plan(computation_spec=Dict(
        "algorithm" => "foreign", "network_ir_sha256" => repeat("a", 64)))
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_checkpoint!(
            checkpoint, foreign_plan, Any[chunk_1, chunk_3])
    end

    cancel_checks = Ref(0)
    @test_throws ROFieldChunkCancelProbe begin
        _ROFCC_BACKEND.resume_ro_field_work_units(
            plan, checkpoint, Any[chunk_1, chunk_3];
            cancel_check=() -> begin
                cancel_checks[] += 1
                cancel_checks[] == 1 && throw(ROFieldChunkCancelProbe())
            end,
        )
    end
    @test cancel_checks[] == 1

    empty_checkpoint = _ROFCC_BACKEND.build_ro_field_checkpoint(plan, Any[])
    @test _ROFCC_BACKEND.resume_ro_field_work_units(
        plan, empty_checkpoint, Any[]) == units
end

@testset "complete dataset manifests bind ordered chunks and counts" begin
    plan = _rofcc_plan()
    units = _ROFCC_BACKEND.ro_field_plan_work_units(plan)
    chunks = Any[
        _rofcc_chunk(plan, units[1]; invalid_positions=Set([2])),
        _rofcc_chunk(plan, units[2]),
        _rofcc_chunk(plan, units[3]; invalid_positions=Set([1])),
    ]
    manifest = _ROFCC_BACKEND.build_ro_field_dataset_manifest(
        plan, Any[chunks[3], chunks[1], chunks[2]])
    @test manifest["point_count"] == 5
    @test manifest["work_unit_count"] == 3
    @test manifest["chunk_count"] == 3
    @test manifest["valid_count"] == 3
    @test manifest["invalid_count"] == 2
    @test manifest["chunk_payload_bytes"] == sum(
        entry["chunk_payload_bytes"] for entry in manifest["chunks"])
    @test getindex.(manifest["chunks"], "ordinal") == [1, 2, 3]
    @test _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
        manifest, plan, chunks) == manifest

    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.build_ro_field_dataset_manifest(plan, chunks[1:2])
    end
    reversed = deepcopy(manifest)
    reverse!(reversed["chunks"])
    _rofcc_rehash!(reversed, "manifest_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
            reversed, plan, chunks)
    end
    count_tamper = deepcopy(manifest)
    count_tamper["valid_count"] += 1
    _rofcc_rehash!(count_tamper, "manifest_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
            count_tamper, plan, chunks)
    end
    byte_tamper = deepcopy(manifest)
    byte_tamper["chunk_payload_bytes"] += 1
    _rofcc_rehash!(byte_tamper, "manifest_sha256")
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
            byte_tamper, plan, chunks)
    end
    changed_chunk = deepcopy(chunks[1])
    changed_chunk["samples"][1]["output_values"][1] += 1.0
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
            manifest, plan, Any[changed_chunk, chunks[2], chunks[3]])
    end
    foreign_plan = _rofcc_plan(computation_spec=Dict(
        "algorithm" => "foreign", "network_ir_sha256" => repeat("a", 64)))
    @test_throws _ROFCC_BACKEND.ROFieldChunkContractError begin
        _ROFCC_BACKEND.validate_ro_field_dataset_manifest!(
            manifest, foreign_plan, chunks)
    end
end
