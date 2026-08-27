# Local asynchronous orchestration for content-addressed RO-field datasets.
#
# This is deliberately a local-only first vertical.  Scientific plan identity
# excludes job/runtime lineage, while resume creates a new job and reuses only
# chunks proven by one terminal parent checkpoint.

const RO_FIELD_JOB_SPEC_VERSION = "bne-ro-field-job-spec/v1.0.0"
const RO_FIELD_JOB_RESULT_VERSION = "bne-ro-field-job-result/v1.0.0"
const RO_FIELD_JOB_ALGORITHM_VERSION = "bne-ro-field-chunked-sampler/v1.0.0"

const _ROFJOB_SPEC_KEYS = Set((
    "schema_version", "request", "work_unit_size", "plan", "resume_from",
))
const _ROFJOB_RESUME_KEYS = Set(("parent_job_id", "checkpoint_sha256"))
const _ROFJOB_RESULT_KEYS = Set((
    "schema_version", "plan_sha256", "checkpoint_sha256",
    "dataset_manifest_sha256", "network_ir_sha256", "point_count",
    "work_unit_count", "valid_count", "invalid_count",
    "chunk_payload_bytes", "storage", "lineage", "evidence",
))
const _ROFJOB_STORAGE_KEYS = Set((
    "mode", "plan_ref", "checkpoint_ref", "dataset_manifest_ref",
))
const _ROFJOB_EVIDENCE_KEYS = Set((
    "evidence_class", "claim_scope", "validity_policy", "limitations",
))
const _ROFJOB_MIN_WORK_UNIT_SIZE = 4
const _ROFJOB_MAX_WORK_UNIT_SIZE = 64
const _ROFJOB_MAX_CONTROL_DOCUMENT_BYTES = 8 * 1024 * 1024
const _ROFJOB_ID_PATTERN = r"^[0-9a-f]{32}$"
const _ROFJOB_MAX_GAP_DETAIL_BYTES = 512

struct ROFieldJobPayloadLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldJobPayloadLimitExceeded)
    print(io, "RO-field job ", err.phase, " requires ", err.requested,
        " canonical chunk bytes, exceeding declared max_payload_bytes=",
        err.limit)
end

function _rofjob_payload_limit(spec)
    budget = spec["request"]["work_budget"]
    return _rofjob_int(budget["max_payload_bytes"],
        "request.work_budget.max_payload_bytes"; minimum=1,
        maximum=MAX_SYNC_RO_FIELD_INLINE_BYTES)
end

function _rofjob_check_payload!(requested::Integer, limit::Int, phase::Symbol)
    amount = BigInt(requested)
    amount <= limit || throw(
        ROFieldJobPayloadLimitExceeded(phase, amount, limit))
    return amount
end

# Public execution uses the bounded default evaluator.  Tests may inject an
# evaluator, but it is held to the same job-local gap-detail bound.  With that
# bound, this reservation dominates the canonical representation of either a
# valid numeric sample or an invalid gap, including worst-case JSON escaping.
function _rofjob_chunk_payload_reservation(plan, unit)
    axis_count = length(plan["identity"]["axis_ids"])
    output_count = length(plan["identity"]["output_ids"])
    point_count = Int(unit["point_count"])
    per_point = 4_096 + 256 * axis_count +
        512 * output_count * (axis_count + 1)
    return BigInt(8_192) + BigInt(point_count) * BigInt(per_point)
end

function _rofjob_validate_job_local_samples!(samples)
    samples isa AbstractVector || throw(ArgumentError(
        "internal compute_ro_field evaluator must return an array"))
    for (index, sample) in enumerate(samples)
        sample isa AbstractDict || continue # canonical chunk validator reports it
        get(sample, "status", nothing) == "invalid" || continue
        gap = get(sample, "gap", nothing)
        gap isa AbstractDict || continue
        detail = get(gap, "detail", nothing)
        detail === nothing && continue
        detail isa AbstractString || continue
        ncodeunits(detail) <= _ROFJOB_MAX_GAP_DETAIL_BYTES || throw(
            ArgumentError("samples[$(index)].gap.detail exceeds the " *
                "$(_ROFJOB_MAX_GAP_DETAIL_BYTES)-byte local job limit"))
    end
    return samples
end

function _rofjob_exact_keys(raw, expected::Set{String}, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$(path) must be an object"))
    observed = Set(String(key) for key in keys(raw))
    observed == expected || throw(ArgumentError(
        "$(path) fields must be exactly $(sort!(collect(expected)))"))
    return raw
end

function _rofjob_int(raw, path::AbstractString; minimum::Int=0,
                     maximum::Int=typemax(Int))
    raw isa Integer && !(raw isa Bool) || throw(ArgumentError(
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

function _rofjob_sha(raw, path::AbstractString)
    raw isa AbstractString || throw(ArgumentError(
        "$(path) must be a lowercase SHA-256 string"))
    value = String(raw)
    occursin(r"^[0-9a-f]{64}$", value) || throw(ArgumentError(
        "$(path) must be a lowercase SHA-256 string"))
    return value
end

function _rofjob_resume(raw)
    raw === nothing && return nothing
    resume = _rofjob_exact_keys(
        _rofc_materialize(raw), _ROFJOB_RESUME_KEYS, "resume_from")
    parent = resume["parent_job_id"]
    parent isa AbstractString && occursin(_ROFJOB_ID_PATTERN, String(parent)) ||
        throw(ArgumentError(
            "resume_from.parent_job_id must be a 32-character lowercase job id"))
    return Dict{String,Any}(
        "parent_job_id" => String(parent),
        "checkpoint_sha256" => _rofjob_sha(
            resume["checkpoint_sha256"],
            "resume_from.checkpoint_sha256"),
    )
end

function _rofjob_inline_request(raw_request)
    request = _rofc_materialize(raw_request, "request")
    request isa AbstractDict || throw(ArgumentError("request must be an object"))
    _ro_field_validate_model_source(request) === :network || throw(ArgumentError(
        "compute_ro_field requires one complete inline canonical NetworkIR"))
    get(request, "representation", nothing) == "sampled_grid" ||
        throw(ArgumentError(
            "compute_ro_field v1 supports sampled_grid requests only"))
    storage = get(request, "storage", nothing)
    storage isa AbstractDict && Set(String.(keys(storage))) == Set(["mode"]) &&
        get(storage, "mode", nothing) == "chunked" || throw(ArgumentError(
            "compute_ro_field requires storage.mode=chunked"))
    work_budget = get(request, "work_budget", nothing)
    work_budget isa AbstractDict || throw(ArgumentError(
        "compute_ro_field request must contain work_budget"))
    get(work_budget, "deadline_seconds", :missing) === nothing ||
        throw(ArgumentError(
            "asynchronous compute_ro_field requires deadline_seconds=null"))

    network = parse_network_ir(request["network"])
    canonical_network = network_ir_to_dict(network)
    canonical_request = deepcopy(request)
    canonical_request["network"] = canonical_network

    inline_request = deepcopy(canonical_request)
    inline_request["storage"] = Dict{String,Any}("mode" => "inline")
    bundle = build_model_bundle(network)
    normalized = normalize_ro_field_request(inline_request, bundle)
    normalized.representation === :sampled_grid || error(
        "internal compute_ro_field representation mismatch")

    # Reconstruct the portable request from the semantic normalizer.  The
    # inline source is retained for restart portability, while the scientific
    # plan uses only its canonical hash.
    prepared_request = deepcopy(normalized.normalized_configuration)
    delete!(prepared_request, "network_ir_hash")
    prepared_request["network"] = canonical_network
    prepared_request["storage"] = Dict{String,Any}("mode" => "chunked")
    return prepared_request, normalized, bundle
end

function _rofjob_plan(normalized::NormalizedROFieldRequest,
                      work_unit_size::Int)
    scientific_configuration = deepcopy(normalized.normalized_configuration)
    scientific_configuration["storage"] = Dict{String,Any}(
        "mode" => "content_addressed_chunks_v1")
    computation_spec = Dict{String,Any}(
        "algorithm" => "finite_equilibrium_ro_field_chunked_sampler",
        "algorithm_version" => RO_FIELD_JOB_ALGORITHM_VERSION,
        "network_ir_sha256" => normalized.network_ir_hash,
        "field_configuration" => scientific_configuration,
        "validity_policy" => "invalid_is_explicit_gap",
        "evidence_scope" => "declared_finite_point_population_only",
    )
    return build_ro_field_chunk_plan(
        axis_ids=normalized.domain["axis_order"],
        output_ids=normalized.outputs["output_order"],
        axis_coordinates=normalized.axis_coordinates_declared,
        computation_spec=computation_spec,
        work_unit_size=work_unit_size,
        runtime_context=Dict{String,Any}(),
    )
end

"""Normalize and freeze one local asynchronous RO-field job specification."""
function normalize_ro_field_job_spec(raw)
    spec = _rofc_materialize(raw, "compute_ro_field spec")
    if spec isa AbstractDict &&
       get(spec, "schema_version", nothing) ==
            RO_FIELD_SPARSE_JOB_SPEC_VERSION
        return normalize_ro_field_sparse_job_spec(spec)
    end
    spec isa AbstractDict || throw(ArgumentError(
        "compute_ro_field spec must be an object"))
    observed = Set(String.(keys(spec)))
    allowed = _ROFJOB_SPEC_KEYS
    required = Set(("schema_version", "request", "work_unit_size"))
    isempty(setdiff(observed, allowed)) || throw(ArgumentError(
        "compute_ro_field spec contains unsupported fields"))
    isempty(setdiff(required, observed)) || throw(ArgumentError(
        "compute_ro_field spec is missing required fields"))
    get(spec, "schema_version", nothing) == RO_FIELD_JOB_SPEC_VERSION ||
        throw(ArgumentError(
            "schema_version must be $(RO_FIELD_JOB_SPEC_VERSION)"))
    work_unit_size = _rofjob_int(
        spec["work_unit_size"], "work_unit_size";
        minimum=_ROFJOB_MIN_WORK_UNIT_SIZE,
        maximum=_ROFJOB_MAX_WORK_UNIT_SIZE)
    request, normalized, _ = _rofjob_inline_request(spec["request"])
    plan = _rofjob_plan(normalized, work_unit_size)
    if haskey(spec, "plan")
        supplied = validate_ro_field_chunk_plan!(spec["plan"])
        supplied["plan_sha256"] == plan["plan_sha256"] &&
            _rofc_canonical_json(supplied) == _rofc_canonical_json(plan) ||
            throw(ArgumentError(
                "caller-supplied plan does not equal the derived scientific plan"))
    end
    resume = _rofjob_resume(get(spec, "resume_from", nothing))
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_JOB_SPEC_VERSION,
        "request" => request,
        "work_unit_size" => work_unit_size,
        "plan" => plan,
        "resume_from" => resume,
    )
end

function _rofjob_read_document(
    path::AbstractString;
    max_bytes::Int=_ROFJOB_MAX_CONTROL_DOCUMENT_BYTES,
    storage_root::AbstractString=local_job_store_dir(),
)
    document_path = normpath(abspath(String(path)))
    bytes = _rofc_read_bounded_file(
        storage_root, document_path, max_bytes;
        phase=:control_document_bytes)
    isvalid(String, bytes) || throw(ArgumentError(
        "RO-field control document is not valid UTF-8"))
    return try
        _rofc_materialize(JSON3.read(String(copy(bytes))))
    catch err
        throw(ArgumentError(
            "RO-field control document is invalid JSON: $(sprint(showerror, err))"))
    end
end

function _rofjob_write_once!(
    path::AbstractString,
    document;
    storage_root::AbstractString=local_job_store_dir(),
)
    destination = normpath(abspath(String(path)))
    expected = _rofc_canonical_json(document)
    bytes = Vector{UInt8}(codeunits(expected))
    function existing_matches(observed_bytes)
        isvalid(String, observed_bytes) || return false
        observed = try
            _rofc_materialize(JSON3.read(String(copy(observed_bytes))))
        catch
            return false
        end
        return _rofc_canonical_json(observed) == expected
    end
    _rofc_write_bytes_once!(
        storage_root, destination, bytes;
        max_existing_bytes=_ROFJOB_MAX_CONTROL_DOCUMENT_BYTES,
        existing_matches=existing_matches)
    observed = _rofjob_read_document(
        destination; storage_root=storage_root)
    _rofc_canonical_json(observed) == expected || error(
        "durable RO-field control publication changed document content")
    return destination
end

_rofjob_data_root(job_id::AbstractString) =
    joinpath(_job_dir(String(job_id)), "ro-field")
_rofjob_plan_path(root::AbstractString) = joinpath(String(root), "plan.json")
_rofjob_checkpoint_path(root::AbstractString, hash::AbstractString) =
    joinpath(String(root), "checkpoints", String(hash) * ".json")
_rofjob_manifest_path(root::AbstractString, hash::AbstractString) =
    joinpath(String(root), "manifests", String(hash) * ".json")

function _rofjob_units_by_hash(plan)
    units = ro_field_plan_work_units(plan)
    return units, Dict(
        ro_field_work_unit_sha256(unit, plan) => unit for unit in units)
end

function _rofjob_chunks_from_entries(root::AbstractString, plan, entries)
    units, units_by_hash = _rofjob_units_by_hash(plan)
    length(entries) <= length(units) || throw(ArgumentError(
        "RO-field control document contains too many chunks"))
    chunks = Dict{String,Any}[]
    for entry in entries
        entry isa AbstractDict || throw(ArgumentError(
            "RO-field chunk entry must be an object"))
        work_hash = _rofjob_sha(
            get(entry, "work_unit_sha256", nothing),
            "chunk entry work_unit_sha256")
        unit = get(units_by_hash, work_hash, nothing)
        unit === nothing && throw(ArgumentError(
            "RO-field control document references a foreign work unit"))
        chunk_hash = _rofjob_sha(
            get(entry, "chunk_sha256", nothing),
            "chunk entry chunk_sha256")
        path = joinpath(String(root), "chunks", chunk_hash * ".json")
        push!(chunks, read_ro_field_chunk(
            path; expected_sha256=chunk_hash, plan=plan, work_unit=unit,
            storage_root=local_job_store_dir()))
    end
    return chunks
end

function _rofjob_load_checkpoint(root::AbstractString, plan,
                                 checkpoint_sha256::AbstractString)
    expected_hash = _rofjob_sha(checkpoint_sha256, "checkpoint_sha256")
    raw = _rofjob_read_document(
        _rofjob_checkpoint_path(root, expected_hash))
    get(raw, "checkpoint_sha256", nothing) == expected_hash ||
        throw(ArgumentError(
            "checkpoint filename and document identity disagree"))
    entries = get(raw, "committed", nothing)
    entries isa AbstractVector || throw(ArgumentError(
        "checkpoint committed population must be an array"))
    chunks = _rofjob_chunks_from_entries(root, plan, entries)
    checkpoint = validate_ro_field_checkpoint!(raw, plan, chunks)
    checkpoint["checkpoint_sha256"] == expected_hash || throw(ArgumentError(
        "checkpoint content does not match requested lineage"))
    return checkpoint, chunks
end

function _rofjob_resume_parent_snapshot(spec, user_sub::AbstractString)
    resume = spec["resume_from"]
    resume === nothing && return nothing
    parent_id = String(resume["parent_job_id"])
    snapshot = _with_job_lock(parent_id) do
        record = _job_record_locked(parent_id)
        record === nothing && throw(ArgumentError("Unknown job_id: $(parent_id)"))
        _check_user_owns_record(record, user_sub, parent_id)
        String(get(record, "kind", "")) == "compute_ro_field" ||
            throw(ArgumentError("resume parent is not a compute_ro_field job"))
        String(get(record, "status", "")) in ("failed", "cancelled") ||
            throw(ArgumentError(
                "resume parent must be a terminal failed or cancelled job"))
        get(record, "ro_field_plan_sha256", nothing) ==
            spec["plan"]["plan_sha256"] || throw(ArgumentError(
                "resume parent scientific plan does not match the child plan"))
        get(record, "latest_checkpoint_sha256", nothing) ==
            resume["checkpoint_sha256"] || throw(ArgumentError(
                "resume checkpoint is not the parent's linearized checkpoint"))
        _job_snapshot(record)
    end

    root = _rofjob_data_root(parent_id)
    stored_plan = validate_ro_field_chunk_plan!(
        _rofjob_read_document(_rofjob_plan_path(root)))
    stored_plan["plan_sha256"] == spec["plan"]["plan_sha256"] ||
        throw(ArgumentError("resume parent plan artifact is inconsistent"))
    checkpoint, chunks = _rofjob_load_checkpoint(
        root, stored_plan, resume["checkpoint_sha256"])
    return (record=snapshot, root=root, plan=stored_plan,
            checkpoint=checkpoint, chunks=chunks)
end

function validate_ro_field_resume_parent!(spec, user_sub::AbstractString)
    normalized = normalize_ro_field_job_spec(spec)
    normalized["schema_version"] == RO_FIELD_SPARSE_JOB_SPEC_VERSION &&
        return validate_ro_field_sparse_resume_parent!(normalized, user_sub)
    normalized["resume_from"] === nothing && return normalized
    _rofjob_resume_parent_snapshot(normalized, user_sub)
    return normalized
end

function _rofjob_default_evaluator(normalized::NormalizedROFieldRequest,
                                   bundle, work_unit,
                                   cancel_check::Function)
    samples = Dict{String,Any}[]
    input_count = length(normalized.axis_indices)
    output_count = length(normalized.output_indices)
    for point in work_unit["points"]
        cancel_check()
        engine_point = Float64[
            _ro_field_coordinate_to_engine_log10(
                point[index], normalized.log_basis,
                log10(Float64(normalized.domain["axes"][index]["reference"]["value"])))
            for index in 1:input_count
        ]
        sampled = sample_reaction_order_field(
            bundle["model"], normalized.axis_indices,
            [[value] for value in engine_point], normalized.output_indices,
            normalized.fixed_engine_logqK;
            max_grid_points=1, cancel_check=cancel_check)
        data = _ro_field_serialize_sampled(sampled, normalized)
        if only(data["validity"])
            ro_values = Float64.(data["reaction_order_values"])
            matrix = Vector{Float64}[
                ro_values[(row - 1) * input_count + 1:row * input_count]
                for row in 1:output_count
            ]
            push!(samples, Dict{String,Any}(
                "status" => "valid",
                "output_values" => Float64.(data["output_values"]),
                "reaction_order_matrix" => matrix,
                "regime_id" => only(data["regime_ids"]),
            ))
        else
            push!(samples, Dict{String,Any}(
                "status" => "invalid",
                "gap" => Dict{String,Any}(
                    "reason" => "solver_nonconvergence_or_nonfinite",
                    "detail" => "No finite converged equilibrium/derivative was available at this declared point.",
                ),
            ))
        end
    end
    cancel_check()
    return samples
end

function _rofjob_publish_checkpoint!(root::AbstractString, checkpoint,
                                     context, cancel_check::Function)
    cancel_check()
    hash = checkpoint["checkpoint_sha256"]
    _rofjob_write_once!(_rofjob_checkpoint_path(root, hash), checkpoint)
    cancel_check()
    callback = get(context, "publish_checkpoint", nothing)
    callback === nothing || callback(checkpoint)
    cancel_check()
    return checkpoint
end

function _rofjob_result(plan, checkpoint, manifest, normalized, spec,
                        job_id::AbstractString)
    plan_hash = plan["plan_sha256"]
    checkpoint_hash = checkpoint["checkpoint_sha256"]
    manifest_hash = manifest["manifest_sha256"]
    descriptor = Dict{String,Any}(
        "schema_version" => RO_FIELD_JOB_RESULT_VERSION,
        "plan_sha256" => plan_hash,
        "checkpoint_sha256" => checkpoint_hash,
        "dataset_manifest_sha256" => manifest_hash,
        "network_ir_sha256" => normalized.network_ir_hash,
        "point_count" => manifest["point_count"],
        "work_unit_count" => manifest["work_unit_count"],
        "valid_count" => manifest["valid_count"],
        "invalid_count" => manifest["invalid_count"],
        "chunk_payload_bytes" => manifest["chunk_payload_bytes"],
        "storage" => Dict{String,Any}(
            "mode" => "content_addressed_local_job_artifacts",
            "plan_ref" => "job://$(job_id)/ro-field/plan",
            "checkpoint_ref" =>
                "job://$(job_id)/ro-field/checkpoints/$(checkpoint_hash)",
            "dataset_manifest_ref" =>
                "job://$(job_id)/ro-field/manifests/$(manifest_hash)",
        ),
        "lineage" => deepcopy(spec["resume_from"]),
        "evidence" => Dict{String,Any}(
            "evidence_class" => "sampled_numerical_chunk_dataset",
            "claim_scope" => "declared_finite_point_population_only",
            "validity_policy" => "invalid_is_explicit_gap",
            "limitations" => Any[
                "A complete manifest proves coverage of the declared finite plan, not continuum accuracy or experimental validity.",
                "Invalid solver samples remain explicit gaps and cannot support a response-shape claim.",
            ],
        ),
    )
    artifact = artifact_metadata(
        "compute_ro_field";
        input_hashes=Dict{String,Any}(
            "network_ir_hash" => normalized.network_ir_hash,
            "plan_sha256" => plan_hash,
            "dataset_manifest_sha256" => manifest_hash,
        ),
        algorithm_name="finite_equilibrium_ro_field_chunked_sampler",
        config=plan["identity"],
        warnings=manifest["invalid_count"] > 0 ? String[
            "The dataset contains explicit invalid gaps.",
        ] : String[],
    )
    artifact["algorithm"]["config_hash"] == plan_hash || error(
        "RO-field plan and generic artifact canonical hashes disagree")
    return Dict{String,Any}(
        "ro_field_job_result" => descriptor,
        "artifact" => artifact,
    )
end

"""Execute one prepared local chunked RO-field job."""
function compute_ro_field_job(raw_spec;
                              job_context=Dict{String,Any}(),
                              cancel_check::Function=_no_cancel_check)
    cancel_check()
    spec = normalize_ro_field_job_spec(raw_spec)
    spec["schema_version"] == RO_FIELD_SPARSE_JOB_SPEC_VERSION &&
        return compute_ro_field_sparse_job(
            spec; job_context=job_context, cancel_check=cancel_check)
    plan = spec["plan"]
    job_id_raw = get(job_context, "job_id", nothing)
    job_id_raw isa AbstractString && occursin(_ROFJOB_ID_PATTERN, String(job_id_raw)) ||
        throw(ArgumentError("compute_ro_field requires an internal local job context"))
    job_id = String(job_id_raw)
    root = _rofjob_data_root(job_id)
    _rofjob_write_once!(_rofjob_plan_path(root), plan)
    cancel_check()

    request, normalized, bundle = _rofjob_inline_request(spec["request"])
    request == spec["request"] || error(
        "prepared compute_ro_field request changed during execution")
    chunks = Dict{String,Any}[]
    payload_limit = _rofjob_payload_limit(spec)
    committed_payload_bytes = BigInt(0)
    resume_state = nothing
    if spec["resume_from"] !== nothing
        user_sub = get(job_context, "user_sub", nothing)
        user_sub isa AbstractString || throw(ArgumentError(
            "resume execution requires the authenticated job owner"))
        resume_state = _rofjob_resume_parent_snapshot(spec, String(user_sub))
        append!(chunks, resume_state.chunks)
        _, units_by_hash = _rofjob_units_by_hash(plan)
        for chunk in resume_state.chunks
            cancel_check()
            unit = units_by_hash[chunk["work_unit_sha256"]]
            write_ro_field_chunk!(root, chunk;
                plan=plan, work_unit=unit,
                storage_root=local_job_store_dir())
        end
        reused_checkpoint = build_ro_field_checkpoint(
            plan, chunks; cancel_check=cancel_check)
        committed_payload_bytes = _rofjob_check_payload!(
            reused_checkpoint["committed_payload_bytes"], payload_limit,
            :resume_payload)
        _rofjob_publish_checkpoint!(
            root, reused_checkpoint, job_context, cancel_check)
    end

    checkpoint = build_ro_field_checkpoint(plan, chunks;
        cancel_check=cancel_check)
    missing = resume_state === nothing ?
        ro_field_plan_work_units(plan; cancel_check=cancel_check) :
        resume_ro_field_work_units(
            plan, resume_state.checkpoint, resume_state.chunks;
            cancel_check=cancel_check)
    evaluator = get(job_context, "evaluator", _rofjob_default_evaluator)
    evaluator isa Function || throw(ArgumentError(
        "internal compute_ro_field evaluator must be callable"))

    with_model_bundle_lock(bundle) do
        for unit in missing
            cancel_check()
            reservation = _rofjob_chunk_payload_reservation(plan, unit)
            _rofjob_check_payload!(committed_payload_bytes + reservation,
                payload_limit, :pre_evaluation_reservation)
            samples = evaluator(normalized, bundle, unit, cancel_check)
            cancel_check()
            _rofjob_validate_job_local_samples!(samples)
            chunk = build_ro_field_chunk(
                plan, unit, samples; cancel_check=cancel_check)
            chunk_payload_bytes = BigInt(length(_rofc_bytes(chunk)))
            chunk_payload_bytes <= reservation || error(
                "internal RO-field chunk payload reservation was not conservative")
            committed_payload_bytes = _rofjob_check_payload!(
                committed_payload_bytes + chunk_payload_bytes,
                payload_limit, :pre_commit_payload)
            write_ro_field_chunk!(root, chunk;
                plan=plan, work_unit=unit,
                storage_root=local_job_store_dir())
            cancel_check()
            push!(chunks, chunk)
            checkpoint = build_ro_field_checkpoint(
                plan, chunks; cancel_check=cancel_check)
            checkpoint["committed_payload_bytes"] == committed_payload_bytes ||
                error("RO-field checkpoint payload accounting drifted")
            _rofjob_publish_checkpoint!(
                root, checkpoint, job_context, cancel_check)
        end
    end

    # A complete checkpoint is linearly published before the dataset commit.
    checkpoint = build_ro_field_checkpoint(
        plan, chunks; cancel_check=cancel_check)
    checkpoint["committed_work_unit_count"] ==
        length(ro_field_plan_work_units(plan)) || error(
            "internal RO-field job completed without a full checkpoint")
    checkpoint["committed_payload_bytes"] == committed_payload_bytes ||
        error("final RO-field checkpoint payload accounting drifted")
    _rofjob_publish_checkpoint!(root, checkpoint, job_context, cancel_check)
    cancel_check()
    manifest = build_ro_field_dataset_manifest(
        plan, chunks; cancel_check=cancel_check)
    manifest_path = _rofjob_manifest_path(root, manifest["manifest_sha256"])
    _rofjob_write_once!(manifest_path, manifest)
    cancel_check()
    validate_ro_field_dataset_manifest!(manifest, plan, chunks;
        cancel_check=cancel_check)
    manifest["chunk_payload_bytes"] == committed_payload_bytes ||
        error("RO-field dataset manifest payload accounting drifted")
    return _rofjob_result(plan, checkpoint, manifest, normalized, spec, job_id)
end

function _rofjob_validate_result_descriptor!(descriptor, job_id::AbstractString,
                                             expected_plan_sha256::AbstractString;
                                             record=nothing)
    value = _rofjob_exact_keys(
        _rofc_materialize(descriptor), _ROFJOB_RESULT_KEYS,
        "ro_field_job_result")
    value["schema_version"] == RO_FIELD_JOB_RESULT_VERSION ||
        throw(ArgumentError("unsupported RO-field job result version"))
    plan_hash = _rofjob_sha(value["plan_sha256"], "result.plan_sha256")
    plan_hash == expected_plan_sha256 || throw(ArgumentError(
        "RO-field result plan does not match the submitted job"))
    checkpoint_hash = _rofjob_sha(
        value["checkpoint_sha256"], "result.checkpoint_sha256")
    manifest_hash = _rofjob_sha(
        value["dataset_manifest_sha256"],
        "result.dataset_manifest_sha256")
    network_hash = _rofjob_sha(
        value["network_ir_sha256"], "result.network_ir_sha256")
    point_count = _rofjob_int(value["point_count"], "result.point_count";
        minimum=1, maximum=_ROFC_HARD_MAX_POINTS)
    work_unit_count = _rofjob_int(
        value["work_unit_count"], "result.work_unit_count";
        minimum=1, maximum=_ROFC_HARD_MAX_WORK_UNITS)
    valid_count = _rofjob_int(
        value["valid_count"], "result.valid_count";
        maximum=_ROFC_HARD_MAX_POINTS)
    invalid_count = _rofjob_int(
        value["invalid_count"], "result.invalid_count";
        maximum=_ROFC_HARD_MAX_POINTS)
    chunk_payload_bytes = _rofjob_int(
        value["chunk_payload_bytes"], "result.chunk_payload_bytes";
        minimum=1)
    valid_count + invalid_count == point_count || throw(ArgumentError(
        "RO-field result counts do not cover the plan"))

    storage = _rofjob_exact_keys(
        value["storage"], _ROFJOB_STORAGE_KEYS, "result.storage")
    storage["mode"] == "content_addressed_local_job_artifacts" ||
        throw(ArgumentError("unsupported RO-field job result storage mode"))
    storage == Dict{String,Any}(
        "mode" => "content_addressed_local_job_artifacts",
        "plan_ref" => "job://$(job_id)/ro-field/plan",
        "checkpoint_ref" =>
            "job://$(job_id)/ro-field/checkpoints/$(checkpoint_hash)",
        "dataset_manifest_ref" =>
            "job://$(job_id)/ro-field/manifests/$(manifest_hash)",
    ) || throw(ArgumentError("RO-field job artifact references are inconsistent"))

    lineage = value["lineage"]
    normalized_lineage = lineage === nothing ? nothing : _rofjob_resume(lineage)
    if record !== nothing
        record isa AbstractDict || throw(ArgumentError(
            "RO-field contextual result validation requires a job record"))
        String(get(record, "job_id", "")) == String(job_id) ||
            throw(ArgumentError("RO-field result record job identity is inconsistent"))
        get(record, "ro_field_plan_sha256", nothing) == plan_hash ||
            throw(ArgumentError("RO-field result record plan identity is inconsistent"))
        expected_lineage_raw = get(record, "resume_from", nothing)
        expected_lineage = expected_lineage_raw === nothing ? nothing :
            _rofjob_resume(expected_lineage_raw)
        normalized_lineage == expected_lineage || throw(ArgumentError(
            "RO-field result lineage does not equal the submitted resume lineage"))
        get(record, "latest_checkpoint_sha256", nothing) == checkpoint_hash ||
            throw(ArgumentError(
                "RO-field result checkpoint is not the job's linearized checkpoint"))
        get(record, "ro_field_dataset_manifest_sha256", nothing) ==
            manifest_hash || throw(ArgumentError(
                "RO-field result dataset manifest is not the job's linearized terminal manifest"))
    end
    evidence = _rofjob_exact_keys(
        value["evidence"], _ROFJOB_EVIDENCE_KEYS, "result.evidence")
    evidence["evidence_class"] == "sampled_numerical_chunk_dataset" &&
        evidence["claim_scope"] == "declared_finite_point_population_only" &&
        evidence["validity_policy"] == "invalid_is_explicit_gap" ||
        throw(ArgumentError("RO-field job evidence semantics are unsupported"))
    limitations = evidence["limitations"]
    limitations isa AbstractVector && length(limitations) >= 2 &&
        all(item -> item isa AbstractString && !isempty(item), limitations) ||
        throw(ArgumentError("RO-field job limitations are incomplete"))

    root = _rofjob_data_root(job_id)
    plan = validate_ro_field_chunk_plan!(
        _rofjob_read_document(_rofjob_plan_path(root)))
    plan["plan_sha256"] == plan_hash || throw(ArgumentError(
        "stored RO-field plan does not match the result"))
    plan["identity"]["computation_spec"]["network_ir_sha256"] == network_hash ||
        throw(ArgumentError("RO-field result network identity is inconsistent"))
    checkpoint, checkpoint_chunks = _rofjob_load_checkpoint(
        root, plan, checkpoint_hash)
    raw_manifest = _rofjob_read_document(
        _rofjob_manifest_path(root, manifest_hash))
    get(raw_manifest, "manifest_sha256", nothing) == manifest_hash ||
        throw(ArgumentError("dataset manifest filename and identity disagree"))
    entries = get(raw_manifest, "chunks", nothing)
    entries isa AbstractVector || throw(ArgumentError(
        "dataset manifest chunk population must be an array"))
    manifest_chunks = _rofjob_chunks_from_entries(root, plan, entries)
    manifest = validate_ro_field_dataset_manifest!(
        raw_manifest, plan, manifest_chunks)
    checkpoint["committed_work_unit_count"] == work_unit_count &&
        manifest["work_unit_count"] == work_unit_count &&
        manifest["point_count"] == point_count &&
        manifest["valid_count"] == valid_count &&
        manifest["invalid_count"] == invalid_count &&
        manifest["chunk_payload_bytes"] == chunk_payload_bytes &&
        checkpoint["committed_payload_bytes"] == chunk_payload_bytes ||
        throw(ArgumentError(
            "RO-field result counts disagree with committed artifacts"))
    checkpoint["committed"] == manifest["chunks"] || throw(ArgumentError(
        "final checkpoint and dataset manifest commit different ordered chunks"))
    return value
end

function validate_ro_field_job_result!(result, job_id::AbstractString,
                                       expected_plan_sha256::AbstractString;
                                       record=nothing)
    if result isa AbstractDict
        descriptor = get(result, "ro_field_job_result", nothing)
        if descriptor isa AbstractDict &&
           get(descriptor, "schema_version", nothing) ==
                RO_FIELD_SPARSE_JOB_RESULT_VERSION
            return validate_ro_field_sparse_job_result!(
                result, job_id, expected_plan_sha256; record=record)
        end
    end
    result isa AbstractDict || throw(ArgumentError(
        "RO-field job result must be an object"))
    Set(String.(keys(result))) == Set(("ro_field_job_result", "artifact")) ||
        throw(ArgumentError(
            "RO-field job result must contain descriptor and artifact only"))
    return _rofjob_validate_result_descriptor!(
        result["ro_field_job_result"], job_id, expected_plan_sha256;
        record=record)
end
