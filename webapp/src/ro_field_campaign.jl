# Deterministic preparation and local-demo QC for a future multi-input Atlas
# campaign.  This module deliberately contains no Slurm, AWS, subprocess, or
# full-corpus executor.  Preparing a manifest is not execution authority.

const RO_FIELD_CAMPAIGN_MANIFEST_VERSION =
    "bne-ro-field-campaign-manifest/v1.0.0"
const RO_FIELD_CAMPAIGN_SHARD_RESULT_VERSION =
    "bne-ro-field-campaign-shard-result/v1.0.0"
const RO_FIELD_CAMPAIGN_CORPUS_LOCK_VERSION =
    "bne-ro-field-campaign-corpus-lock/v1.0.0"
const RO_FIELD_CAMPAIGN_QC_VERSION =
    "bne-ro-field-campaign-independent-qc/v1.0.0"

const _ROFCAMP_WORK_ITEM_KEYS = Set((
    "network_ir_sha256", "field_plan_sha256",
))
const _ROFCAMP_POLICY_KEYS = Set((
    "network_ir_schema_version", "field_plan_schema_version",
    "chart_policy_sha256", "domain_policy_sha256",
    "background_policy_sha256", "output_policy_sha256",
    "solver_policy_sha256", "invalidity_policy",
    "signature_version", "query_version",
))
const _ROFCAMP_LOCK_KEYS = Set(("path", "sha256"))
const _ROFCAMP_EVALUATION_KEYS = Set((
    "status", "artifact_sha256", "valid_count", "invalid_count",
    "evidence_class",
))
const _ROFCAMP_REVISION_PATTERN = r"^[0-9a-f]{40}$"

struct ROFieldCampaignLimits
    max_work_units::Int
    max_shards::Int
    max_environment_locks::Int
    max_policy_nodes::Int
    max_demo_work_units::Int
    max_samples_per_work_unit::Int
    max_total_samples::Int
end

function ROFieldCampaignLimits(;
    max_work_units::Integer=100_000,
    max_shards::Integer=10_000,
    max_environment_locks::Integer=64,
    max_policy_nodes::Integer=2_048,
    max_demo_work_units::Integer=8,
    max_samples_per_work_unit::Integer=1_000_000,
    max_total_samples::Integer=500_000_000,
)
    bounded(raw, name, hard) = _rofc_bounded_positive(raw, name, hard)
    return ROFieldCampaignLimits(
        bounded(max_work_units, "max_work_units", 1_000_000),
        bounded(max_shards, "max_shards", 100_000),
        bounded(max_environment_locks, "max_environment_locks", 1_024),
        bounded(max_policy_nodes, "max_policy_nodes", 65_536),
        bounded(max_demo_work_units, "max_demo_work_units", 64),
        bounded(max_samples_per_work_unit,
            "max_samples_per_work_unit", 1_000_000_000),
        bounded(max_total_samples, "max_total_samples", 1_000_000_000),
    )
end

struct ROFieldCampaignLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldCampaignLimitExceeded)
    print(io, "RO-field campaign ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

@inline function _rofcamp_limit(phase::Symbol, requested::Integer, limit::Int)
    amount = BigInt(requested)
    amount <= limit || throw(
        ROFieldCampaignLimitExceeded(phase, amount, limit))
    return nothing
end

function _rofcamp_count_nodes(raw, limits::ROFieldCampaignLimits)
    count = Ref(BigInt(0))
    function visit(value, depth)
        depth <= 32 || throw(ArgumentError(
            "scientific_policy exceeds the maximum nesting depth"))
        count[] += 1
        _rofcamp_limit(:policy_nodes, count[], limits.max_policy_nodes)
        if value isa AbstractDict
            for (key, child) in pairs(value)
                (key isa AbstractString || key isa Symbol) ||
                    throw(ArgumentError(
                        "scientific_policy keys must be strings or symbols"))
                visit(child, depth + 1)
            end
        elseif value isa AbstractVector || value isa Tuple
            for child in value
                visit(child, depth + 1)
            end
        end
    end
    visit(raw, 0)
    return count[]
end

function _rofcamp_exact(raw, expected::Set{String}, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$(path) must be an object"))
    observed = Set{String}()
    for key in keys(raw)
        (key isa AbstractString || key isa Symbol) || throw(ArgumentError(
            "$(path) keys must be strings or symbols"))
        push!(observed, String(key))
    end
    observed == expected || throw(ArgumentError(
        "$(path) fields must be exactly $(sort!(collect(expected)))"))
    return raw
end

function _rofcamp_int(raw, path::AbstractString;
                      minimum::Int=0, maximum::Int=typemax(Int))
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$(path) must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$(path) is outside the supported integer range"))
    end
    minimum <= value <= maximum || throw(ArgumentError(
        "$(path) must be in $(minimum):$(maximum)"))
    return value
end

function _rofcamp_string(raw, path::AbstractString; maximum::Int=4_096)
    raw isa AbstractString || throw(ArgumentError("$(path) must be a string"))
    value = String(raw)
    isempty(value) && throw(ArgumentError("$(path) must not be empty"))
    ncodeunits(value) <= maximum || throw(ArgumentError(
        "$(path) exceeds the $(maximum)-byte limit"))
    return value
end

function _rofcamp_sha(raw, path::AbstractString)
    value = _rofcamp_string(raw, path; maximum=64)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$(path) must be a lowercase SHA-256 string"))
    return value
end

function _rofcamp_relative_path(raw, path::AbstractString)
    value = _rofcamp_string(raw, path)
    isabspath(value) && throw(ArgumentError("$(path) must be repository-relative"))
    normalized = normpath(value)
    (normalized == "." || normalized == ".." || startswith(normalized, "../")) &&
        throw(ArgumentError("$(path) must remain below the repository root"))
    occursin('\\', value) && throw(ArgumentError(
        "$(path) must use portable POSIX separators"))
    return replace(normalized, '\\' => '/')
end

function _rofcamp_policy(raw, limits)
    _rofcamp_count_nodes(raw, limits)
    policy = _rofcamp_exact(_rofc_materialize(raw),
        _ROFCAMP_POLICY_KEYS, "scientific_policy")
    normalized = Dict{String,Any}(
        "network_ir_schema_version" => _rofcamp_string(
            policy["network_ir_schema_version"],
            "scientific_policy.network_ir_schema_version"),
        "field_plan_schema_version" => _rofcamp_string(
            policy["field_plan_schema_version"],
            "scientific_policy.field_plan_schema_version"),
        "chart_policy_sha256" => _rofcamp_sha(
            policy["chart_policy_sha256"],
            "scientific_policy.chart_policy_sha256"),
        "domain_policy_sha256" => _rofcamp_sha(
            policy["domain_policy_sha256"],
            "scientific_policy.domain_policy_sha256"),
        "background_policy_sha256" => _rofcamp_sha(
            policy["background_policy_sha256"],
            "scientific_policy.background_policy_sha256"),
        "output_policy_sha256" => _rofcamp_sha(
            policy["output_policy_sha256"],
            "scientific_policy.output_policy_sha256"),
        "solver_policy_sha256" => _rofcamp_sha(
            policy["solver_policy_sha256"],
            "scientific_policy.solver_policy_sha256"),
        "invalidity_policy" => _rofcamp_string(
            policy["invalidity_policy"],
            "scientific_policy.invalidity_policy"),
        "signature_version" => _rofcamp_string(
            policy["signature_version"],
            "scientific_policy.signature_version"),
        "query_version" => _rofcamp_string(
            policy["query_version"],
            "scientific_policy.query_version"),
    )
    normalized["invalidity_policy"] == "invalid_is_explicit_gap" ||
        throw(ArgumentError(
            "campaign invalidity_policy must be invalid_is_explicit_gap"))
    return normalized
end

function _rofcamp_environment_locks(raw, limits)
    raw isa AbstractVector || throw(ArgumentError(
        "environment_locks must be an array"))
    _rofcamp_limit(:environment_locks, length(raw),
        limits.max_environment_locks)
    locks = Dict{String,Any}[]
    seen = Set{String}()
    for (index, item) in enumerate(raw)
        lock = _rofcamp_exact(_rofc_materialize(item),
            _ROFCAMP_LOCK_KEYS, "environment_locks[$(index)]")
        path = _rofcamp_relative_path(lock["path"],
            "environment_locks[$(index)].path")
        path in seen && throw(ArgumentError(
            "environment_locks paths must be unique"))
        push!(seen, path)
        push!(locks, Dict{String,Any}(
            "path" => path,
            "sha256" => _rofcamp_sha(lock["sha256"],
                "environment_locks[$(index)].sha256"),
        ))
    end
    isempty(locks) && throw(ArgumentError(
        "at least one environment lock must be frozen"))
    sort!(locks; by=lock -> lock["path"])
    return locks
end

function _rofcamp_work_units(raw, limits)
    raw isa AbstractVector || throw(ArgumentError(
        "work_items must be an array"))
    isempty(raw) && throw(ArgumentError(
        "campaign work_items must not be empty"))
    _rofcamp_limit(:work_units, length(raw), limits.max_work_units)
    semantic = Dict{String,Any}[]
    for (index, item) in enumerate(raw)
        value = _rofcamp_exact(_rofc_materialize(item),
            _ROFCAMP_WORK_ITEM_KEYS, "work_items[$(index)]")
        push!(semantic, Dict{String,Any}(
            "network_ir_sha256" => _rofcamp_sha(
                value["network_ir_sha256"],
                "work_items[$(index)].network_ir_sha256"),
            "field_plan_sha256" => _rofcamp_sha(
                value["field_plan_sha256"],
                "work_items[$(index)].field_plan_sha256"),
        ))
    end
    sort!(semantic; by=item ->
        (item["network_ir_sha256"], item["field_plan_sha256"]))
    keys = [(item["network_ir_sha256"], item["field_plan_sha256"])
        for item in semantic]
    allunique(keys) || throw(ArgumentError(
        "campaign work_items must be scientifically unique"))
    units = Dict{String,Any}[]
    for (ordinal, item) in enumerate(semantic)
        identity = Dict{String,Any}(
            "ordinal" => ordinal,
            "network_ir_sha256" => item["network_ir_sha256"],
            "field_plan_sha256" => item["field_plan_sha256"],
        )
        push!(units, Dict{String,Any}(
            "work_unit_id" => "campaign-wu-" * lpad(string(ordinal), 8, '0'),
            identity...,
            "work_unit_sha256" => _rofc_sha256(identity),
        ))
    end
    return units
end

function _rofcamp_shards(units, shard_size::Int, limits)
    shard_count_big = cld(BigInt(length(units)), BigInt(shard_size))
    _rofcamp_limit(:shards, shard_count_big, limits.max_shards)
    shards = Dict{String,Any}[]
    shard_count = Int(shard_count_big)
    for shard_ordinal in 1:shard_count
        first_index = (shard_ordinal - 1) * shard_size + 1
        last_index = min(shard_ordinal * shard_size, length(units))
        identity = Dict{String,Any}(
            "shard_id" => "campaign-shard-" *
                lpad(string(shard_ordinal), 6, '0'),
            "ordinal" => shard_ordinal,
            "work_unit_sha256s" => String[
                units[index]["work_unit_sha256"]
                for index in first_index:last_index
            ],
        )
        push!(shards, Dict{String,Any}(
            identity...,
            "shard_plan_sha256" => _rofc_sha256(identity),
        ))
    end
    return shards
end

"Build a frozen manifest only; this function never starts campaign execution."
function build_ro_field_campaign_manifest(;
    campaign_name,
    work_items,
    scientific_policy,
    code_revision,
    environment_locks,
    merge_command,
    shard_size::Integer=64,
    execution_scope="prepared_external_requires_separate_authorization",
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits(),
)
    name = _rofcamp_string(campaign_name, "campaign_name"; maximum=128)
    occursin(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", name) ||
        throw(ArgumentError("campaign_name is not a portable identifier"))
    revision = _rofcamp_string(code_revision, "code_revision"; maximum=40)
    occursin(_ROFCAMP_REVISION_PATTERN, revision) || throw(ArgumentError(
        "code_revision must be a full lowercase Git commit"))
    size_value = _rofc_bounded_positive(
        shard_size, "shard_size", limits.max_work_units)
    scope = _rofcamp_string(execution_scope, "execution_scope"; maximum=128)
    scope in ("local_demo_max_8", "prepared_external_requires_separate_authorization") ||
        throw(ArgumentError("unsupported campaign execution_scope"))
    units = _rofcamp_work_units(work_items, limits)
    scope == "local_demo_max_8" && _rofcamp_limit(
        :demo_work_units, length(units), limits.max_demo_work_units)
    locks = _rofcamp_environment_locks(environment_locks, limits)
    policy = _rofcamp_policy(scientific_policy, limits)
    shards = _rofcamp_shards(units, size_value, limits)
    command = _rofcamp_string(merge_command, "merge_command"; maximum=2_048)
    occursin(r"(^|\s)(/|~)", command) && throw(ArgumentError(
        "merge_command must not contain an absolute or home-relative path"))
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_MANIFEST_VERSION,
        "campaign_name" => name,
        "population_kind" => "explicit_canonical_network_field_plan_pairs",
        "expected_work_unit_count" => length(units),
        "scientific_policy" => policy,
        "work_units" => units,
        "shard_size" => size_value,
        "shards" => shards,
        "code_revision" => revision,
        "environment_locks" => locks,
        "merge_command" => command,
        "execution_scope" => scope,
        "authority" => "manifest_preparation_is_not_execution_authority",
    )
    return Dict{String,Any}(
        identity...,
        "campaign_sha256" => _rofc_sha256(identity),
    )
end

function validate_ro_field_campaign_manifest!(raw;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits())
    manifest = _rofc_materialize(raw)
    expected_keys = Set((
        "schema_version", "campaign_name", "population_kind",
        "expected_work_unit_count", "scientific_policy", "work_units",
        "shard_size", "shards", "code_revision", "environment_locks",
        "merge_command", "execution_scope", "authority", "campaign_sha256",
    ))
    _rofcamp_exact(manifest, expected_keys, "campaign_manifest")
    manifest["schema_version"] == RO_FIELD_CAMPAIGN_MANIFEST_VERSION ||
        throw(ArgumentError("unsupported campaign manifest version"))
    raw_units = manifest["work_units"]
    raw_units isa AbstractVector || throw(ArgumentError(
        "campaign_manifest.work_units must be an array"))
    inputs = Dict{String,Any}[]
    for unit in raw_units
        unit isa AbstractDict || throw(ArgumentError(
            "campaign_manifest work unit must be an object"))
        push!(inputs, Dict{String,Any}(
            "network_ir_sha256" => get(unit, "network_ir_sha256", nothing),
            "field_plan_sha256" => get(unit, "field_plan_sha256", nothing),
        ))
    end
    expected = build_ro_field_campaign_manifest(
        campaign_name=manifest["campaign_name"],
        work_items=inputs,
        scientific_policy=manifest["scientific_policy"],
        code_revision=manifest["code_revision"],
        environment_locks=manifest["environment_locks"],
        merge_command=manifest["merge_command"],
        shard_size=manifest["shard_size"],
        execution_scope=manifest["execution_scope"],
        limits=limits,
    )
    _rofc_canonical_json(manifest) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "campaign manifest is noncanonical, reordered, or tampered"))
    return expected
end

function _rofcamp_normalize_evaluation(raw, path,
                                       limits::ROFieldCampaignLimits)
    value = _rofcamp_exact(_rofc_materialize(raw),
        _ROFCAMP_EVALUATION_KEYS, path)
    status = _rofcamp_string(value["status"], "$(path).status")
    status in ("valid", "invalid_gap") || throw(ArgumentError(
        "$(path).status must be valid or invalid_gap"))
    valid_count = _rofcamp_int(value["valid_count"],
        "$(path).valid_count"; maximum=limits.max_samples_per_work_unit)
    invalid_count = _rofcamp_int(value["invalid_count"],
        "$(path).invalid_count"; maximum=limits.max_samples_per_work_unit)
    sample_count = BigInt(valid_count) + BigInt(invalid_count)
    _rofcamp_limit(:samples_per_work_unit, sample_count,
        limits.max_samples_per_work_unit)
    sample_count > 0 || throw(ArgumentError(
        "$(path) must report at least one evaluated item"))
    if status == "valid"
        invalid_count == 0 || throw(ArgumentError(
            "a valid campaign result cannot contain gaps"))
    else
        invalid_count > 0 || throw(ArgumentError(
            "invalid_gap must report invalid_count > 0"))
    end
    evidence_class = _rofcamp_string(
        value["evidence_class"], "$(path).evidence_class")
    expected_evidence_class = status == "valid" ?
        "bounded_engine_evaluation" :
        "bounded_engine_evaluation_with_explicit_gaps"
    evidence_class == expected_evidence_class || throw(ArgumentError(
        "$(path).evidence_class must match status and cannot promote campaign evidence"))
    return Dict{String,Any}(
        "status" => status,
        "artifact_sha256" => _rofcamp_sha(
            value["artifact_sha256"], "$(path).artifact_sha256"),
        "valid_count" => valid_count,
        "invalid_count" => invalid_count,
        "evidence_class" => evidence_class,
    )
end

"Run only a bounded local demonstration shard; external/prepared manifests fail closed."
function run_ro_field_campaign_demo_shard(
    raw_manifest,
    shard_id::AbstractString,
    evaluator;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits(),
    cancel_check=() -> nothing,
)
    evaluator isa Function || throw(ArgumentError("evaluator must be callable"))
    manifest = validate_ro_field_campaign_manifest!(raw_manifest; limits=limits)
    manifest["execution_scope"] == "local_demo_max_8" || throw(ArgumentError(
        "campaign execution requires separate authorization; only local_demo_max_8 is executable here"))
    _rofcamp_limit(:demo_work_units,
        manifest["expected_work_unit_count"], limits.max_demo_work_units)
    matches = filter(shard -> shard["shard_id"] == String(shard_id),
        manifest["shards"])
    length(matches) == 1 || throw(ArgumentError("unknown campaign shard_id"))
    shard = only(matches)
    units_by_hash = Dict(unit["work_unit_sha256"] => unit
        for unit in manifest["work_units"])
    results = Dict{String,Any}[]
    evaluated_samples = BigInt(0)
    for (index, work_hash) in enumerate(shard["work_unit_sha256s"])
        cancel_check()
        unit = units_by_hash[work_hash]
        raw_evaluation = evaluator(deepcopy(unit))
        cancel_check()
        evaluation = _rofcamp_normalize_evaluation(
            raw_evaluation, "evaluation[$(index)]", limits)
        evaluated_samples += BigInt(evaluation["valid_count"]) +
            BigInt(evaluation["invalid_count"])
        _rofcamp_limit(:total_samples, evaluated_samples,
            limits.max_total_samples)
        push!(results, Dict{String,Any}(
            "work_unit_id" => unit["work_unit_id"],
            "work_unit_sha256" => work_hash,
            evaluation...,
        ))
    end
    cancel_check()
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_SHARD_RESULT_VERSION,
        "campaign_sha256" => manifest["campaign_sha256"],
        "shard_id" => shard["shard_id"],
        "shard_plan_sha256" => shard["shard_plan_sha256"],
        "results" => results,
    )
    return Dict{String,Any}(
        identity...,
        "shard_result_sha256" => _rofc_sha256(identity),
    )
end

function _validate_ro_field_campaign_shard_result!(raw, manifest,
    units_by_hash, shards_by_id, limits::ROFieldCampaignLimits)
    result = _rofc_materialize(raw)
    expected_keys = Set((
        "schema_version", "campaign_sha256", "shard_id",
        "shard_plan_sha256", "results", "shard_result_sha256",
    ))
    _rofcamp_exact(result, expected_keys, "campaign_shard_result")
    result["schema_version"] == RO_FIELD_CAMPAIGN_SHARD_RESULT_VERSION ||
        throw(ArgumentError("unsupported campaign shard-result version"))
    result["campaign_sha256"] == manifest["campaign_sha256"] ||
        throw(ArgumentError("campaign shard result references another manifest"))
    haskey(shards_by_id, result["shard_id"]) || throw(ArgumentError(
        "campaign shard result has an unknown shard_id"))
    shard = shards_by_id[result["shard_id"]]
    result["shard_plan_sha256"] == shard["shard_plan_sha256"] ||
        throw(ArgumentError("campaign shard-plan identity mismatch"))
    raw_results = result["results"]
    raw_results isa AbstractVector || throw(ArgumentError(
        "campaign shard results must be an array"))
    length(raw_results) == length(shard["work_unit_sha256s"]) ||
        throw(ArgumentError("campaign shard result population is incomplete"))
    normalized = Dict{String,Any}[]
    for (index, raw_evaluation) in enumerate(raw_results)
        raw_evaluation isa AbstractDict || throw(ArgumentError(
            "campaign shard result entry must be an object"))
        work_hash = shard["work_unit_sha256s"][index]
        unit = units_by_hash[work_hash]
        observed_keys = Set{String}()
        for key in keys(raw_evaluation)
            (key isa AbstractString || key isa Symbol) || throw(ArgumentError(
                "campaign shard result entry keys must be strings or symbols"))
            push!(observed_keys, String(key))
        end
        observed_keys == union(_ROFCAMP_EVALUATION_KEYS,
            Set(("work_unit_id", "work_unit_sha256"))) || throw(ArgumentError(
            "campaign shard result entry fields are unsupported"))
        raw_evaluation["work_unit_id"] == unit["work_unit_id"] &&
            raw_evaluation["work_unit_sha256"] == work_hash ||
            throw(ArgumentError("campaign shard result point order is inconsistent"))
        evaluation = _rofcamp_normalize_evaluation(Dict{String,Any}(
            key => raw_evaluation[key] for key in _ROFCAMP_EVALUATION_KEYS),
            "campaign_shard_result.results[$(index)]", limits)
        push!(normalized, Dict{String,Any}(
            "work_unit_id" => unit["work_unit_id"],
            "work_unit_sha256" => work_hash,
            evaluation...,
        ))
    end
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_SHARD_RESULT_VERSION,
        "campaign_sha256" => manifest["campaign_sha256"],
        "shard_id" => shard["shard_id"],
        "shard_plan_sha256" => shard["shard_plan_sha256"],
        "results" => normalized,
    )
    expected = Dict{String,Any}(
        identity...,
        "shard_result_sha256" => _rofc_sha256(identity),
    )
    _rofc_canonical_json(result) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "campaign shard result is noncanonical, reordered, or tampered"))
    return expected
end

function validate_ro_field_campaign_shard_result!(raw, raw_manifest;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits())
    manifest = validate_ro_field_campaign_manifest!(raw_manifest; limits=limits)
    units_by_hash = Dict(unit["work_unit_sha256"] => unit
        for unit in manifest["work_units"])
    shards_by_id = Dict(shard["shard_id"] => shard
        for shard in manifest["shards"])
    return _validate_ro_field_campaign_shard_result!(
        raw, manifest, units_by_hash, shards_by_id, limits)
end

function merge_ro_field_campaign_shards(raw_manifest, raw_shard_results;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits(),
    cancel_check=() -> nothing)
    manifest = validate_ro_field_campaign_manifest!(raw_manifest; limits=limits)
    raw_shard_results isa AbstractVector || throw(ArgumentError(
        "shard_results must be an array"))
    _rofcamp_limit(:shards, length(raw_shard_results), limits.max_shards)
    units_by_hash = Dict(unit["work_unit_sha256"] => unit
        for unit in manifest["work_units"])
    shards_by_id = Dict(shard["shard_id"] => shard
        for shard in manifest["shards"])
    normalized = Dict{String,Any}[]
    for result in raw_shard_results
        cancel_check()
        push!(normalized, _validate_ro_field_campaign_shard_result!(
            result, manifest, units_by_hash, shards_by_id, limits))
    end
    cancel_check()
    sort!(normalized; by=result -> result["shard_id"])
    expected_shards = getindex.(manifest["shards"], "shard_id")
    getindex.(normalized, "shard_id") == expected_shards ||
        throw(ArgumentError(
            "campaign merge requires exactly one result for every declared shard"))
    ordered_results = Dict{String,Any}[]
    for shard in normalized
        cancel_check()
        append!(ordered_results, shard["results"])
    end
    expected_units = getindex.(manifest["work_units"], "work_unit_sha256")
    getindex.(ordered_results, "work_unit_sha256") == expected_units ||
        throw(ArgumentError(
            "campaign merge work-unit population/order is incomplete"))
    valid_units = count(item -> item["status"] == "valid", ordered_results)
    invalid_units = length(ordered_results) - valid_units
    valid_samples_big = sum((BigInt(item["valid_count"])
        for item in ordered_results); init=BigInt(0))
    invalid_samples_big = sum((BigInt(item["invalid_count"])
        for item in ordered_results); init=BigInt(0))
    total_samples_big = valid_samples_big + invalid_samples_big
    _rofcamp_limit(:total_samples, total_samples_big,
        limits.max_total_samples)
    valid_samples = Int(valid_samples_big)
    invalid_samples = Int(invalid_samples_big)
    cancel_check()
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_CORPUS_LOCK_VERSION,
        "campaign_sha256" => manifest["campaign_sha256"],
        "population_kind" => manifest["population_kind"],
        "work_unit_count" => length(ordered_results),
        "valid_work_unit_count" => valid_units,
        "invalid_work_unit_count" => invalid_units,
        "valid_sample_count" => valid_samples,
        "invalid_sample_count" => invalid_samples,
        "ordered_artifacts" => [Dict{String,Any}(
            "work_unit_sha256" => item["work_unit_sha256"],
            "artifact_sha256" => item["artifact_sha256"],
            "status" => item["status"],
        ) for item in ordered_results],
        "shard_result_sha256s" => getindex.(normalized,
            "shard_result_sha256"),
        "qc" => Dict{String,Any}(
            "manifest_revalidated" => true,
            "shard_hashes_revalidated" => true,
            "work_unit_order_revalidated" => true,
            "complete_declared_result_metadata_population" => true,
            "independent_population_recount_required" => true,
            "duplicate_conflicts" => 0,
        ),
        "claim_scope" => manifest["execution_scope"] == "local_demo_max_8" ?
            "validated_declared_demo_result_metadata_population_only" :
            "validated_declared_campaign_result_metadata_population_only",
        "external_execution_verified" => false,
    )
    return Dict{String,Any}(
        identity...,
        "corpus_lock_sha256" => _rofc_sha256(identity),
    )
end

function validate_ro_field_campaign_corpus_lock!(raw_lock, raw_manifest,
    raw_shard_results;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits())
    observed = _rofc_materialize(raw_lock)
    expected = merge_ro_field_campaign_shards(
        raw_manifest, raw_shard_results; limits=limits)
    _rofc_canonical_json(observed) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "campaign corpus lock is noncanonical, incomplete, or tampered"))
    return expected
end

function _rofcamp_validate_corpus_lock_self_hash!(raw_lock, manifest)
    lock = _rofc_materialize(raw_lock)
    expected_keys = Set((
        "schema_version", "campaign_sha256", "population_kind",
        "work_unit_count", "valid_work_unit_count",
        "invalid_work_unit_count", "valid_sample_count",
        "invalid_sample_count", "ordered_artifacts",
        "shard_result_sha256s", "qc", "claim_scope",
        "external_execution_verified", "corpus_lock_sha256",
    ))
    _rofcamp_exact(lock, expected_keys, "campaign_corpus_lock")
    lock["schema_version"] == RO_FIELD_CAMPAIGN_CORPUS_LOCK_VERSION ||
        throw(ArgumentError("unsupported campaign corpus-lock version"))
    lock["campaign_sha256"] == manifest["campaign_sha256"] ||
        throw(ArgumentError("campaign corpus lock references another manifest"))
    expected_qc = Dict{String,Any}(
        "manifest_revalidated" => true,
        "shard_hashes_revalidated" => true,
        "work_unit_order_revalidated" => true,
        "complete_declared_result_metadata_population" => true,
        "independent_population_recount_required" => true,
        "duplicate_conflicts" => 0,
    )
    _rofcamp_exact(lock["qc"], Set(keys(expected_qc)),
        "campaign_corpus_lock.qc")
    _rofc_canonical_json(lock["qc"]) ==
        _rofc_canonical_json(expected_qc) || throw(ArgumentError(
            "campaign corpus-lock QC claims are inconsistent"))
    lock["external_execution_verified"] === false || throw(ArgumentError(
        "campaign corpus lock cannot self-assert external execution"))
    identity = Dict{String,Any}(
        key => value for (key, value) in lock
        if key != "corpus_lock_sha256"
    )
    _rofcamp_sha(lock["corpus_lock_sha256"],
        "campaign_corpus_lock.corpus_lock_sha256") ==
        _rofc_sha256(identity) || throw(ArgumentError(
            "campaign corpus-lock self hash is inconsistent"))
    return lock
end

"""
Run a second, identity-map-based population recount over supplied artifacts.

This is independent of the merge algorithm's ordered concatenation, but it is
still local artifact QC: it does not re-run scientific evaluations, inspect the
content behind each artifact hash, or prove that an external executor ran.
"""
function audit_ro_field_campaign_corpus(raw_lock, raw_manifest,
    raw_shard_results;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits(),
    cancel_check=() -> nothing)
    manifest = validate_ro_field_campaign_manifest!(raw_manifest; limits=limits)
    lock = _rofcamp_validate_corpus_lock_self_hash!(raw_lock, manifest)
    raw_shard_results isa AbstractVector || throw(ArgumentError(
        "shard_results must be an array"))
    _rofcamp_limit(:shards, length(raw_shard_results), limits.max_shards)
    length(raw_shard_results) == length(manifest["shards"]) ||
        throw(ArgumentError(
            "independent QC requires every declared shard exactly once"))

    units_by_hash = Dict(unit["work_unit_sha256"] => unit
        for unit in manifest["work_units"])
    shards_by_id = Dict(shard["shard_id"] => shard
        for shard in manifest["shards"])
    observed_by_work_unit = Dict{String,Dict{String,Any}}()
    observed_shard_hash = Dict{String,String}()
    for raw_result in raw_shard_results
        cancel_check()
        result = _validate_ro_field_campaign_shard_result!(
            raw_result, manifest, units_by_hash, shards_by_id, limits)
        shard_id = result["shard_id"]
        haskey(observed_shard_hash, shard_id) && throw(ArgumentError(
            "independent QC found a duplicate shard result"))
        observed_shard_hash[shard_id] = result["shard_result_sha256"]
        for entry in result["results"]
            work_hash = entry["work_unit_sha256"]
            haskey(observed_by_work_unit, work_hash) && throw(ArgumentError(
                "independent QC found duplicate work-unit evidence"))
            observed_by_work_unit[work_hash] = entry
        end
    end

    length(observed_by_work_unit) == length(manifest["work_units"]) ||
        throw(ArgumentError(
            "independent QC found an incomplete work-unit population"))
    valid_units = 0
    invalid_units = 0
    valid_samples_big = BigInt(0)
    invalid_samples_big = BigInt(0)
    ordered_artifacts = Dict{String,Any}[]
    for unit in manifest["work_units"]
        cancel_check()
        work_hash = unit["work_unit_sha256"]
        haskey(observed_by_work_unit, work_hash) || throw(ArgumentError(
            "independent QC is missing a declared work unit"))
        entry = observed_by_work_unit[work_hash]
        if entry["status"] == "valid"
            valid_units += 1
        else
            invalid_units += 1
        end
        valid_samples_big += BigInt(entry["valid_count"])
        invalid_samples_big += BigInt(entry["invalid_count"])
        push!(ordered_artifacts, Dict{String,Any}(
            "work_unit_sha256" => work_hash,
            "artifact_sha256" => entry["artifact_sha256"],
            "status" => entry["status"],
        ))
    end
    total_samples_big = valid_samples_big + invalid_samples_big
    _rofcamp_limit(:total_samples, total_samples_big,
        limits.max_total_samples)
    valid_samples = Int(valid_samples_big)
    invalid_samples = Int(invalid_samples_big)
    expected_shard_hashes = String[
        observed_shard_hash[shard["shard_id"]]
        for shard in manifest["shards"]
    ]
    expected_claim_scope = manifest["execution_scope"] ==
        "local_demo_max_8" ?
        "validated_declared_demo_result_metadata_population_only" :
        "validated_declared_campaign_result_metadata_population_only"

    lock["population_kind"] == manifest["population_kind"] ||
        throw(ArgumentError("independent QC found population-kind drift"))
    lock["work_unit_count"] == length(manifest["work_units"]) ||
        throw(ArgumentError("independent QC work-unit count mismatch"))
    lock["valid_work_unit_count"] == valid_units ||
        throw(ArgumentError("independent QC valid-unit count mismatch"))
    lock["invalid_work_unit_count"] == invalid_units ||
        throw(ArgumentError("independent QC invalid-unit count mismatch"))
    lock["valid_sample_count"] == valid_samples ||
        throw(ArgumentError("independent QC valid-sample count mismatch"))
    lock["invalid_sample_count"] == invalid_samples ||
        throw(ArgumentError("independent QC invalid-sample count mismatch"))
    _rofc_canonical_json(lock["ordered_artifacts"]) ==
        _rofc_canonical_json(ordered_artifacts) || throw(ArgumentError(
            "independent QC artifact population/order mismatch"))
    _rofc_canonical_json(lock["shard_result_sha256s"]) ==
        _rofc_canonical_json(expected_shard_hashes) || throw(ArgumentError(
            "independent QC shard population/order mismatch"))
    lock["claim_scope"] == expected_claim_scope || throw(ArgumentError(
        "independent QC claim scope mismatch"))
    lock["external_execution_verified"] === false || throw(ArgumentError(
        "local campaign QC cannot verify external execution"))

    cancel_check()
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_QC_VERSION,
        "campaign_sha256" => manifest["campaign_sha256"],
        "corpus_lock_sha256" => lock["corpus_lock_sha256"],
        "qc_algorithm" => "identity_map_population_recount_v1",
        "declared_work_unit_count" => length(manifest["work_units"]),
        "observed_unique_work_unit_count" => length(observed_by_work_unit),
        "valid_work_unit_count" => valid_units,
        "invalid_work_unit_count" => invalid_units,
        "valid_sample_count" => valid_samples,
        "invalid_sample_count" => invalid_samples,
        "checks" => Dict{String,Any}(
            "manifest_revalidated" => true,
            "shard_results_revalidated" => true,
            "identity_map_has_no_duplicates" => true,
            "declared_population_recounted" => true,
            "corpus_lock_self_hash_revalidated" => true,
            "artifact_content_recomputed" => false,
            "external_execution_observed" => false,
        ),
        "evidence_scope" =>
            "second_pass_recount_of_supplied_content_identities_only",
        "external_execution_verified" => false,
    )
    return Dict{String,Any}(
        identity...,
        "qc_sha256" => _rofc_sha256(identity),
    )
end

function validate_ro_field_campaign_qc!(raw_qc, raw_lock, raw_manifest,
    raw_shard_results;
    limits::ROFieldCampaignLimits=ROFieldCampaignLimits())
    observed = _rofc_materialize(raw_qc)
    expected = audit_ro_field_campaign_corpus(
        raw_lock, raw_manifest, raw_shard_results; limits=limits)
    _rofc_canonical_json(observed) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "campaign independent-QC artifact is noncanonical or tampered"))
    return expected
end
