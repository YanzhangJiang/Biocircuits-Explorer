using Test
using BiocircuitsExplorerBackend
using JSON3

_rofcamp_test_hash(index::Integer) = lpad(string(index; base=16), 64, '0')

function _rofcamp_test_policy()
    return Dict{String,Any}(
        "network_ir_schema_version" => "bne-network-ir/v1.0.0",
        "field_plan_schema_version" =>
            "bne-ro-field-chunk-plan/v1.0.0",
        "chart_policy_sha256" => _rofcamp_test_hash(101),
        "domain_policy_sha256" => _rofcamp_test_hash(102),
        "background_policy_sha256" => _rofcamp_test_hash(103),
        "output_policy_sha256" => _rofcamp_test_hash(104),
        "solver_policy_sha256" => _rofcamp_test_hash(105),
        "invalidity_policy" => "invalid_is_explicit_gap",
        "signature_version" => "rpb2-field-signature/v1.0.0",
        "query_version" => "bne-ro-field-atlas-query/v1.0.0",
    )
end

function _rofcamp_test_items(count::Int=5)
    return Any[
        Dict(
            "network_ir_sha256" => _rofcamp_test_hash(2 * index),
            "field_plan_sha256" => _rofcamp_test_hash(2 * index + 1),
        )
        for index in reverse(1:count)
    ]
end

function _rofcamp_test_manifest(;
    count::Int=5,
    scope="local_demo_max_8",
    shard_size::Int=2,
    limits=ROFieldCampaignLimits(),
)
    return build_ro_field_campaign_manifest(
        campaign_name="strict-demo",
        work_items=_rofcamp_test_items(count),
        scientific_policy=_rofcamp_test_policy(),
        code_revision=repeat("a", 40),
        environment_locks=Any[
            Dict("path" => "webapp/Manifest.toml",
                 "sha256" => _rofcamp_test_hash(202)),
            Dict("path" => "Bnc_julia/Project.toml",
                 "sha256" => _rofcamp_test_hash(201)),
        ],
        merge_command="julia --project=webapp " *
            "webapp/scripts/merge_ro_field_campaign.jl " *
            "--manifest campaigns/strict-demo/manifest.json " *
            "--shard-dir campaigns/strict-demo/shards " *
            "--output campaigns/strict-demo/corpus-lock.json " *
            "--qc-output campaigns/strict-demo/qc.json",
        shard_size=shard_size,
        execution_scope=scope,
        limits=limits,
    )
end

function _rofcamp_test_evaluator(unit)
    is_gap = unit["ordinal"] == 3
    return Dict{String,Any}(
        "status" => is_gap ? "invalid_gap" : "valid",
        "artifact_sha256" => unit["work_unit_sha256"],
        "valid_count" => is_gap ? 1 : 2,
        "invalid_count" => is_gap ? 1 : 0,
        "evidence_class" => is_gap ?
            "bounded_engine_evaluation_with_explicit_gaps" :
            "bounded_engine_evaluation",
    )
end

function _rofcamp_test_results(manifest)
    return Any[
        run_ro_field_campaign_demo_shard(
            manifest, shard["shard_id"], _rofcamp_test_evaluator)
        for shard in manifest["shards"]
    ]
end

@testset "RO-field campaign manifest freezes a finite deterministic population" begin
    manifest = _rofcamp_test_manifest()
    @test manifest["schema_version"] == RO_FIELD_CAMPAIGN_MANIFEST_VERSION
    @test manifest["population_kind"] ==
        "explicit_canonical_network_field_plan_pairs"
    @test manifest["expected_work_unit_count"] == 5
    @test length(manifest["work_units"]) == 5
    @test length(manifest["shards"]) == 3
    @test manifest["authority"] ==
        "manifest_preparation_is_not_execution_authority"
    @test manifest["execution_scope"] == "local_demo_max_8"
    @test getindex.(manifest["environment_locks"], "path") == [
        "Bnc_julia/Project.toml", "webapp/Manifest.toml"]
    @test issorted([
        (item["network_ir_sha256"], item["field_plan_sha256"])
        for item in manifest["work_units"]
    ])
    @test getindex.(manifest["work_units"], "ordinal") == collect(1:5)
    @test getindex.(manifest["shards"], "work_unit_sha256s") == [
        getindex.(manifest["work_units"][1:2], "work_unit_sha256"),
        getindex.(manifest["work_units"][3:4], "work_unit_sha256"),
        getindex.(manifest["work_units"][5:5], "work_unit_sha256"),
    ]
    @test validate_ro_field_campaign_manifest!(manifest) == manifest

    reordered = build_ro_field_campaign_manifest(
        campaign_name="strict-demo",
        work_items=reverse(_rofcamp_test_items()),
        scientific_policy=_rofcamp_test_policy(),
        code_revision=repeat("a", 40),
        environment_locks=reverse(Any[
            Dict("path" => "webapp/Manifest.toml",
                 "sha256" => _rofcamp_test_hash(202)),
            Dict("path" => "Bnc_julia/Project.toml",
                 "sha256" => _rofcamp_test_hash(201)),
        ]),
        merge_command="julia --project=webapp " *
            "webapp/scripts/merge_ro_field_campaign.jl " *
            "--manifest campaigns/strict-demo/manifest.json " *
            "--shard-dir campaigns/strict-demo/shards " *
            "--output campaigns/strict-demo/corpus-lock.json " *
            "--qc-output campaigns/strict-demo/qc.json",
        shard_size=2,
        execution_scope="local_demo_max_8",
    )
    @test reordered == manifest

    tampered = deepcopy(manifest)
    tampered["work_units"][1]["field_plan_sha256"] =
        _rofcamp_test_hash(999)
    @test_throws ArgumentError validate_ro_field_campaign_manifest!(tampered)

    runtime_field = deepcopy(_rofcamp_test_items(1)[1])
    runtime_field["job_id"] = "runtime-only"
    @test_throws ArgumentError build_ro_field_campaign_manifest(
        campaign_name="runtime-field",
        work_items=Any[runtime_field],
        scientific_policy=_rofcamp_test_policy(),
        code_revision=repeat("a", 40),
        environment_locks=Any[Dict(
            "path" => "webapp/Manifest.toml",
            "sha256" => _rofcamp_test_hash(1))],
        merge_command="julia --project=webapp webapp/scripts/merge_ro_field_campaign.jl",
    )
    duplicate = _rofcamp_test_items(1)[1]
    @test_throws ArgumentError build_ro_field_campaign_manifest(
        campaign_name="duplicate",
        work_items=Any[duplicate, deepcopy(duplicate)],
        scientific_policy=_rofcamp_test_policy(),
        code_revision=repeat("a", 40),
        environment_locks=Any[Dict(
            "path" => "webapp/Manifest.toml",
            "sha256" => _rofcamp_test_hash(1))],
        merge_command="julia --project=webapp webapp/scripts/merge_ro_field_campaign.jl",
    )
end

@testset "Campaign execution authority and budgets fail closed" begin
    external = _rofcamp_test_manifest(
        scope="prepared_external_requires_separate_authorization")
    calls = Ref(0)
    evaluator = unit -> begin
        calls[] += 1
        _rofcamp_test_evaluator(unit)
    end
    @test_throws ArgumentError run_ro_field_campaign_demo_shard(
        external, external["shards"][1]["shard_id"], evaluator)
    @test calls[] == 0

    manifest = _rofcamp_test_manifest(count=3, shard_size=3)
    tight = ROFieldCampaignLimits(max_demo_work_units=2)
    @test_throws ROFieldCampaignLimitExceeded begin
        run_ro_field_campaign_demo_shard(
            manifest, manifest["shards"][1]["shard_id"], evaluator;
            limits=tight)
    end
    @test calls[] == 0

    cancelled_calls = Ref(0)
    @test_throws ErrorException run_ro_field_campaign_demo_shard(
        manifest, manifest["shards"][1]["shard_id"], evaluator;
        cancel_check=() -> begin
            cancelled_calls[] += 1
            error("cancelled before evaluation")
        end)
    @test cancelled_calls[] == 1
    @test calls[] == 0

    oversized = unit -> Dict(
        "status" => "valid",
        "artifact_sha256" => unit["work_unit_sha256"],
        "valid_count" => 3,
        "invalid_count" => 0,
        "evidence_class" => "bounded_engine_evaluation",
    )
    sample_limit = ROFieldCampaignLimits(max_samples_per_work_unit=2)
    @test_throws ArgumentError run_ro_field_campaign_demo_shard(
        manifest, manifest["shards"][1]["shard_id"], oversized;
        limits=sample_limit)

    large_policy = _rofcamp_test_policy()
    large_policy["signature_version"] = repeat("x", 128)
    @test_throws ROFieldCampaignLimitExceeded begin
        build_ro_field_campaign_manifest(
            campaign_name="policy-budget",
            work_items=_rofcamp_test_items(1),
            scientific_policy=large_policy,
            code_revision=repeat("a", 40),
            environment_locks=Any[Dict(
                "path" => "webapp/Manifest.toml",
                "sha256" => _rofcamp_test_hash(1))],
            merge_command="julia --project=webapp webapp/scripts/merge_ro_field_campaign.jl",
            limits=ROFieldCampaignLimits(max_policy_nodes=5),
        )
    end

    @test_throws ArgumentError build_ro_field_campaign_manifest(
        campaign_name="bad-lock",
        work_items=_rofcamp_test_items(1),
        scientific_policy=_rofcamp_test_policy(),
        code_revision=repeat("a", 40),
        environment_locks=Any[Dict(
            "path" => "../outside.lock",
            "sha256" => _rofcamp_test_hash(1))],
        merge_command="julia --project=webapp webapp/scripts/merge_ro_field_campaign.jl",
    )
end

@testset "Campaign shards and corpus lock retain exact finite evidence" begin
    manifest = _rofcamp_test_manifest()
    results = _rofcamp_test_results(manifest)
    @test length(results) == 3
    @test all(result ->
        validate_ro_field_campaign_shard_result!(result, manifest) == result,
        results)

    lock = merge_ro_field_campaign_shards(manifest, reverse(results))
    @test lock["schema_version"] == RO_FIELD_CAMPAIGN_CORPUS_LOCK_VERSION
    @test lock["work_unit_count"] == 5
    @test lock["valid_work_unit_count"] == 4
    @test lock["invalid_work_unit_count"] == 1
    @test lock["valid_sample_count"] == 9
    @test lock["invalid_sample_count"] == 1
    @test lock["claim_scope"] ==
        "validated_declared_demo_result_metadata_population_only"
    @test lock["external_execution_verified"] === false
    @test lock["qc"][
        "complete_declared_result_metadata_population"] === true
    @test lock["qc"]["independent_population_recount_required"] === true
    @test validate_ro_field_campaign_corpus_lock!(
        lock, manifest, results) == lock

    qc = audit_ro_field_campaign_corpus(lock, manifest, reverse(results))
    @test qc["schema_version"] == RO_FIELD_CAMPAIGN_QC_VERSION
    @test qc["observed_unique_work_unit_count"] == 5
    @test qc["checks"]["declared_population_recounted"] === true
    @test qc["checks"]["artifact_content_recomputed"] === false
    @test qc["external_execution_verified"] === false
    @test validate_ro_field_campaign_qc!(
        qc, lock, manifest, results) == qc

    tampered = deepcopy(results)
    tampered[1]["results"][1]["artifact_sha256"] =
        _rofcamp_test_hash(999)
    @test_throws ArgumentError validate_ro_field_campaign_shard_result!(
        tampered[1], manifest)
    @test_throws ArgumentError merge_ro_field_campaign_shards(
        manifest, tampered)

    reordered = deepcopy(results[1])
    reverse!(reordered["results"])
    @test_throws ArgumentError validate_ro_field_campaign_shard_result!(
        reordered, manifest)
    @test_throws ArgumentError merge_ro_field_campaign_shards(
        manifest, results[1:end-1])
    @test_throws ArgumentError merge_ro_field_campaign_shards(
        manifest, Any[results[1], results[1], results[3]])

    promoted = deepcopy(results[1])
    promoted["results"][1]["evidence_class"] = "universal_proof"
    @test_throws ArgumentError validate_ro_field_campaign_shard_result!(
        promoted, manifest)

    tight = ROFieldCampaignLimits(max_total_samples=9)
    @test_throws ROFieldCampaignLimitExceeded merge_ro_field_campaign_shards(
        manifest, results; limits=tight)

    checks = Ref(0)
    @test_throws ErrorException merge_ro_field_campaign_shards(
        manifest, results;
        cancel_check=() -> begin
            checks[] += 1
            checks[] == 2 && error("cancelled during merge")
        end)
    @test checks[] == 2

    lock_tamper = deepcopy(lock)
    lock_tamper["valid_sample_count"] += 1
    @test_throws ArgumentError validate_ro_field_campaign_corpus_lock!(
        lock_tamper, manifest, results)

    # A self-consistent outer re-hash cannot hide a semantic recount mismatch.
    rehashed_lock_tamper = deepcopy(lock)
    rehashed_lock_tamper["valid_sample_count"] += 1
    tampered_identity = Dict{String,Any}(
        key => value for (key, value) in rehashed_lock_tamper
        if key != "corpus_lock_sha256")
    rehashed_lock_tamper["corpus_lock_sha256"] =
        BiocircuitsExplorerBackend._rofc_sha256(tampered_identity)
    @test_throws ArgumentError audit_ro_field_campaign_corpus(
        rehashed_lock_tamper, manifest, results)

    qc_tamper = deepcopy(qc)
    qc_tamper["observed_unique_work_unit_count"] -= 1
    @test_throws ArgumentError validate_ro_field_campaign_qc!(
        qc_tamper, lock, manifest, results)
end

@testset "Campaign merge command is an executable local reproducer" begin
    include(joinpath(@__DIR__, "..", "scripts",
        "merge_ro_field_campaign.jl"))
    manifest = _rofcamp_test_manifest()
    results = _rofcamp_test_results(manifest)
    mktempdir() do directory
        shard_directory = joinpath(directory, "shards")
        mkpath(shard_directory)
        manifest_path = joinpath(directory, "manifest.json")
        write(manifest_path,
            BiocircuitsExplorerBackend._rofc_canonical_json(manifest))
        for result in results
            path = joinpath(shard_directory, result["shard_id"] * ".json")
            write(path,
                BiocircuitsExplorerBackend._rofc_canonical_json(result))
        end
        lock_path = joinpath(directory, "corpus-lock.json")
        qc_path = joinpath(directory, "qc.json")
        args = [
            "--manifest", manifest_path,
            "--shard-dir", shard_directory,
            "--output", lock_path,
            "--qc-output", qc_path,
        ]
        @test merge_ro_field_campaign_main(args) == 0
        @test isfile(lock_path)
        @test isfile(qc_path)
        stored_lock = JSON3.read(read(lock_path, String), Dict{String,Any})
        stored_qc = JSON3.read(read(qc_path, String), Dict{String,Any})
        @test validate_ro_field_campaign_corpus_lock!(
            stored_lock, manifest, results) == stored_lock
        @test validate_ro_field_campaign_qc!(
            stored_qc, stored_lock, manifest, results) == stored_qc
        @test merge_ro_field_campaign_main(args) == 0

        write(lock_path, "{}\n")
        @test_throws ArgumentError merge_ro_field_campaign_main(args)
    end
end

@testset "Campaign population report is enumeration-only and reproducible" begin
    include(joinpath(@__DIR__, "..", "scripts",
        "report_ro_field_campaign_population.jl"))
    report = report_ro_field_campaign_population()
    @test report["schema_version"] ==
        RO_FIELD_CAMPAIGN_POPULATION_REPORT_VERSION
    @test getindex.(report["dimensions"], "network_count") ==
        [596, 2_177, 2_467]
    @test getindex.(report["dimensions"], "output_group_count") ==
        [1_046, 4_076, 4_732]
    @test getindex.(report["dimensions"],
        "two_dimensional_field_plan_group_count") ==
        [1_046, 12_228, 28_392]
    @test getindex.(report["dimensions"],
        "all_rank_field_plan_group_count") ==
        [1_046, 16_304, 52_052]
    @test report["total_network_count"] == 5_240
    @test report["total_two_dimensional_field_plan_group_count"] == 41_666
    @test report["total_all_rank_field_plan_group_count"] == 69_402
    @test report["total_two_dimensional_dense_point_count"] == 12_041_474
    @test report["execution_status"] ==
        "enumeration_only_no_field_evaluation_or_atlas_write"
    @test occursin(r"^[0-9a-f]{64}$", report["report_sha256"])
    @test report_ro_field_campaign_population() == report
    @test_throws ArgumentError report_ro_field_campaign_population_main(
        ["unexpected"])
end
