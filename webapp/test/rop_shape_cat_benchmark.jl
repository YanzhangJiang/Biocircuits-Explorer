using Test
using JSON3
using SHA
using BiocircuitsExplorerBackend

const _ROP_CAT_FIXTURE_SHA256 =
    "a37a3f8562c7d82a4d08d6032c6b22a875c4e3e8f460ca1fba2232ee820dab5c"
const _ROP_CAT_RESULT_FILE_SHA256 =
    "f91a319c1e8a9200503560daa02c40c07cf05c8d11e38f844e19b4bfee558d1b"
const _ROP_CAT_PRODUCER_SHA256 =
    "7f920ab23f4a8981b2e2de1710e6109471650670598a8c769795ead866253dca"
const _ROP_CAT_RESULT_HASH =
    "6eda30d093ea44cf841e1eefa87f51659b39e025719e70e109fdca6ddebd177f"

_rop_cat_file_sha256(path::AbstractString) = bytes2hex(SHA.sha256(read(path)))

function _rop_cat_hash_before_attached_artifact(raw)
    payload = deepcopy(raw)
    expected = pop!(payload, "result_hash")
    pop!(payload, "artifact")
    return expected, BiocircuitsExplorerBackend._canonical_hash(payload)
end

function _rop_cat_hash_before_result_hash(raw)
    payload = deepcopy(raw)
    expected = pop!(payload, "result_hash")
    return expected, BiocircuitsExplorerBackend._canonical_hash(payload)
end

function _rop_cat_forbidden_absolute_path(value::AbstractString)
    value == "/api/v1/placer_curve" && return false
    startswith(value, "/") && return true
    startswith(value, "~/") && return true
    startswith(lowercase(value), "file://") && return true
    occursin(r"^[A-Za-z]:[\\/]", value) && return true
    return any(marker -> occursin(marker, value), (
        "/Users/", "/home/", "/raid/", "/private/", "/tmp/",
        "/var/folders/", "/workspace/", "/github/workspace/",
    ))
end

function _rop_cat_collect_path_leaks!(leaks, value, path::AbstractString="\$")
    if value isa AbstractDict
        for (key, child) in pairs(value)
            key_string = String(key)
            _rop_cat_forbidden_absolute_path(key_string) &&
                push!(leaks, "$path/<key>=$key_string")
            _rop_cat_collect_path_leaks!(leaks, child, "$path/$key_string")
        end
    elseif value isa AbstractVector
        for (index, child) in enumerate(value)
            _rop_cat_collect_path_leaks!(leaks, child, "$path/$(index - 1)")
        end
    elseif value isa AbstractString && _rop_cat_forbidden_absolute_path(value)
        push!(leaks, "$path=$value")
    end
    return leaks
end

@testset "Frozen fixed-topology cat benchmark artifact contract (read-only)" begin
    benchmark_dir = normpath(joinpath(
        @__DIR__, "..", "..", "benchmarks", "rop_shape_control"))
    fixture_path = joinpath(benchmark_dir, "cat_fixed_topology.json")
    result_path = joinpath(benchmark_dir, "cat_fixed_topology_results.json")
    producer_path = joinpath(benchmark_dir, "run_benchmark.jl")

    # These byte locks make changes to the frozen input, producer, or result an
    # explicit benchmark regeneration event. This test never invokes the producer.
    @test _rop_cat_file_sha256(fixture_path) == _ROP_CAT_FIXTURE_SHA256
    @test _rop_cat_file_sha256(result_path) == _ROP_CAT_RESULT_FILE_SHA256
    @test _rop_cat_file_sha256(producer_path) == _ROP_CAT_PRODUCER_SHA256

    fixture = JSON3.read(read(fixture_path, String), Dict{String, Any})
    result = JSON3.read(read(result_path, String), Dict{String, Any})

    @test result["schema_version"] == "bne-rop-shape-benchmark-result/v1.0.0"
    @test result["benchmark_id"] == fixture["id"] == "cat-fixed-topology-v1"
    @test result["fixture_artifact_id"] == fixture["reference"]["artifact_id"]
    @test result["evidence_scope"] == fixture["evidence_scope"] ==
          "fixed-topology finite-window SISO ROP benchmark; not a global chemistry claim"
    @test result["application_version"] == "0.1.0"
    @test occursin("all declared fixed-topology cells",
                   result["method_contract"]["direct_lp"])

    artifact = result["artifact"]
    @test artifact["artifact_schema_version"] == "bne-result/v1.0.0"
    @test artifact["kind"] == "rop_shape_cat_benchmark"
    @test artifact["algorithm"]["name"] == "rop_shape_cat_benchmark_v1"
    @test artifact["algorithm"]["version"] == result["application_version"]
    @test artifact["input_hashes"]["fixture"] == result["fixture_artifact_id"]
    @test artifact["input_hashes"]["network_ir"] == result["network_ir_hash"]
    @test isempty(artifact["warnings"])

    # The producer computes result_hash before attaching the timestamped result
    # artifact envelope. Recompute exactly that documented payload boundary.
    expected_result_hash, actual_result_hash =
        _rop_cat_hash_before_attached_artifact(result)
    @test expected_result_hash == _ROP_CAT_RESULT_HASH
    @test actual_result_hash == expected_result_hash

    fixture_ids = String[String(edit["id"]) for edit in fixture["edit_intents"]]
    fixture_kinds = String[String(edit["kind"]) for edit in fixture["edit_intents"]]
    edits = result["edits"]
    @test String[String(edit["id"]) for edit in edits] == fixture_ids
    @test String[String(edit["kind"]) for edit in edits] == fixture_kinds
    @test length(unique(fixture_ids)) == length(fixture_ids) == 4

    geometric_success_count = 0
    replay_pass_count = 0
    non_grid_count = 0
    for (edit, fixture_edit) in zip(edits, fixture["edit_intents"])
        direct = edit["direct_lp"]
        response = direct["result"]
        coverage = response["coverage"]
        budget = response["normalized_request"]["work_budget"]

        # "Exhaustive" here is deliberately population-qualified: every path
        # and cell eligible for this one fixed topology was evaluated. It says
        # nothing about omitted chemistries or a global topology population.
        @test budget["require_exhaustive"] === true
        @test budget["max_paths"] == 2000
        @test budget["max_cells"] == 10_000
        @test coverage["truncated"] === false
        @test isempty(coverage["truncation_reasons"])
        @test coverage["eligible_path_count"] == coverage["evaluated_path_count"] == 18
        @test coverage["eligible_cell_count"] == coverage["evaluated_cell_count"] == 24
        @test coverage["feasible_cell_count"] == 24
        @test coverage["replay_candidate_count"] == coverage["replayed_count"] == 1
        @test response["geometric_status"] == "global_optimal_over_declared_cells"
        @test response["feasible"] === true
        @test response["directional_request_interval"]["scope"] == "declared_cells"
        @test response["directional_request_interval"][
            "complete_over_evaluated_cells"] === true
        @test response["directional_request_interval"]["numerical_error_count"] == 0
        geometric_success_count += response["feasible"] === true

        @test response["fixed_topology"]["network_ir_hash"] == result["network_ir_hash"]
        @test response["fixed_topology"]["topology_preserved"] === true
        @test response["selected"] !== nothing
        @test direct["parameter_chebyshev_radius"] >=
              response["normalized_request"]["optimization"]["minimum_parameter_margin"]
        @test direct["selected_realized_improvement"] <=
              direct["closure_support_improvement"] + 1.0e-8
        @test direct["closure_support_improvement"] -
              direct["selected_realized_improvement"] <=
              response["normalized_request"]["optimization"]["effect_tolerance"] + 1.0e-7

        candidate_grid = Float64.(fixture_edit["candidate_magnitudes_log10"])
        @test Float64.(edit["three_candidate_baseline"][
            "candidate_magnitudes_log10"]) == candidate_grid
        @test length(edit["three_candidate_baseline"]["candidates"]) == 3
        @test direct["non_grid_optimum"] === all(
            abs(Float64(direct["closure_support_improvement"]) - magnitude) > 1.0e-6
            for magnitude in candidate_grid)
        non_grid_count += direct["non_grid_optimum"] === true

        replay = response["replay"]
        @test replay["complete"] === true
        @test replay["curve"]["partial"] === false
        @test all(value -> value === true, replay["curve"]["valid"])
        @test replay["pass"] === (replay["status"] == "pass")
        @test edit["comparison"]["surrogate_replay_pass"] === replay["pass"]
        replay_pass_count += replay["pass"] === true

        # Recompute every stored content hash without executing a solver.
        @test response["request_hash"] == BiocircuitsExplorerBackend._canonical_hash(
            response["normalized_request"])
        @test response["artifact"]["algorithm"]["config_hash"] ==
              response["request_hash"]
        direct_expected, direct_actual = _rop_cat_hash_before_attached_artifact(response)
        @test direct_actual == direct_expected
        @test replay["request_hash"] == BiocircuitsExplorerBackend._canonical_hash(
            replay["request"])
        replay_expected, replay_actual = _rop_cat_hash_before_result_hash(replay)
        @test replay_actual == replay_expected
    end

    summary = result["summary"]
    @test summary["edit_count"] == length(edits) == 4
    @test summary["direct_geometric_success_count"] == geometric_success_count == 4
    @test summary["direct_replay_pass_count"] == replay_pass_count == 3
    @test summary["non_grid_optimum_count"] == non_grid_count == 4
    @test summary["acceptance_non_grid_optimum_observed"] === (non_grid_count >= 1)
    @test summary["all_direct_searches_exhaustive"] === all(
        edit -> edit["direct_lp"]["result"]["coverage"]["truncated"] === false &&
                edit["direct_lp"]["result"]["coverage"]["eligible_path_count"] ==
                edit["direct_lp"]["result"]["coverage"]["evaluated_path_count"] &&
                edit["direct_lp"]["result"]["coverage"]["eligible_cell_count"] ==
                edit["direct_lp"]["result"]["coverage"]["evaluated_cell_count"],
        edits)

    # Repository artifacts may contain relative source names and canonical API
    # routes, but never a workstation/home/temporary absolute filesystem path.
    leaks = String[]
    _rop_cat_collect_path_leaks!(leaks, fixture)
    _rop_cat_collect_path_leaks!(leaks, result)
    @test isempty(leaks)
end
