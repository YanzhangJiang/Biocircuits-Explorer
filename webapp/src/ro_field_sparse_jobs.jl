# Local-only, resumable adaptive RO-field jobs.
#
# This v2 path is deliberately disjoint from the v1 Cartesian chunk schemas
# and byte identities.  One adaptive sparse multi-index is one deterministic
# work unit.  Every committed transition is replayable from canonical plan,
# batch, ordered point-result chunk, and prior state artifacts.

const RO_FIELD_SPARSE_REQUEST_VERSION =
    "bne-ro-field-sparse-request/v2.0.0"
const RO_FIELD_SPARSE_JOB_SPEC_VERSION =
    "bne-ro-field-job-spec/v2.0.0"
const RO_FIELD_SPARSE_PLAN_VERSION =
    "bne-ro-field-sparse-plan/v2.0.0"
const RO_FIELD_SPARSE_JOB_RESULT_VERSION =
    "bne-ro-field-job-result/v2.0.0"
const RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION =
    "bne-ro-field-adaptive-smolyak-local/v2.0.0"
const RO_FIELD_SPARSE_BATCH_ARTIFACT_VERSION =
    "bne-ro-field-sparse-batch-artifact/v2.0.0"
const RO_FIELD_SPARSE_STATE_ARTIFACT_VERSION =
    "bne-ro-field-sparse-state-artifact/v2.0.0"
const RO_FIELD_SPARSE_POINT_CHUNK_VERSION =
    "bne-ro-field-sparse-point-result-chunk/v2.0.0"
const RO_FIELD_SPARSE_CHECKPOINT_VERSION =
    "bne-ro-field-sparse-checkpoint/v2.0.0"
const RO_FIELD_SPARSE_TERMINAL_ARTIFACT_VERSION =
    "bne-ro-field-sparse-terminal-result-artifact/v2.0.0"
const RO_FIELD_SPARSE_MANIFEST_VERSION =
    "bne-ro-field-sparse-manifest/v2.0.0"
const RO_FIELD_SPARSE_NUMERICAL_POLICY_VERSION =
    "bne-ro-field-sparse-numerical-execution-policy/v2.0.0"

const _ROFSJ_MAX_CONTROLS = 4
const _ROFSJ_MAX_OUTPUTS = 4
const _ROFSJ_MAX_POINTS = 4_096
const _ROFSJ_MAX_BATCHES = 512
const _ROFSJ_MAX_JSON_DEPTH = 32
const _ROFSJ_MAX_JSON_NODES = 32_768
const _ROFSJ_MAX_STRING_BYTES = 64 * 1_024
const _ROFSJ_MAX_DOCUMENT_BYTES = 8 * 1_024 * 1_024
const _ROFSJ_MAX_GAP_DETAIL_BYTES = 512
const _ROFSJ_QK2X_RELATIVE_TOLERANCE = 1.0e-8
const _ROFSJ_QK2X_ABSOLUTE_TOLERANCE = 1.0e-10
const _ROFSJ_QK2X_MAX_SOLVER_STEPS_PER_POINT = 100_000
const _ROFSJ_QK2X_MAX_RHS_EVALUATIONS_PER_POINT = 1_000_000
const _ROFSJ_DEFAULT_MAX_REPLAY_WORK_UNITS = 2_000_000_000
const _ROFSJ_HARD_MAX_REPLAY_WORK_UNITS = 2_000_000_000

const _ROFSJ_SPEC_KEYS = Set((
    "schema_version", "request", "plan", "resume_from",
))
const _ROFSJ_REQUEST_KEYS = Set((
    "schema_version", "network", "chart", "domain", "outputs",
    "solver_policy", "invalid_policy", "surplus_tolerance",
    "sampling_limits", "work_budget", "storage",
))
const _ROFSJ_CHART_KEYS = Set((
    "chart_id", "control_ids", "source_coordinate_ids", "jacobian",
    "fixed_background",
))
const _ROFSJ_DOMAIN_KEYS = Set((
    "coordinate_space", "lower", "upper",
))
const _ROFSJ_OUTPUT_KEYS = Set(("output_order", "items"))
const _ROFSJ_OUTPUT_ITEM_KEYS = Set(("output_id", "symbol"))
const _ROFSJ_SOLVER_KEYS = Set((
    "algorithm", "output_space", "source_jacobian_space",
))
const _ROFSJ_LIMIT_KEYS = Set((
    "max_level", "max_points", "max_multi_indices",
    "max_interpolation_work", "max_payload_scalars",
))
const _ROFSJ_WORK_BUDGET_KEYS = Set((
    "max_payload_bytes", "max_json_depth", "max_string_bytes",
    "deadline_seconds", "max_replay_work_units",
))
const _ROFSJ_STORAGE_KEYS = Set(("mode",))
const _ROFSJ_MANIFEST_KEYS = Set((
    "schema_version", "plan_sha256", "checkpoint_sha256",
    "terminal_artifact_sha256", "engine_result_sha256", "point_count",
    "work_unit_count", "valid_count", "invalid_count",
    "interpolation_work_consumed", "committed_payload_bytes", "chunks",
    "manifest_sha256",
))
const _ROFSJ_RESULT_KEYS = Set((
    "schema_version", "algorithm_version", "plan_sha256",
    "checkpoint_sha256", "dataset_manifest_sha256",
    "engine_result_sha256", "network_ir_sha256", "point_count",
    "work_unit_count", "valid_count", "invalid_count",
    "interpolation_work_consumed", "chunk_payload_bytes", "status",
    "stopping_reason", "storage", "lineage", "evidence",
))
const _ROFSJ_RESULT_STORAGE_KEYS = Set((
    "mode", "plan_ref", "checkpoint_ref", "dataset_manifest_ref",
))
const _ROFSJ_RESULT_EVIDENCE_KEYS = Set((
    "evidence_class", "claim_scope", "validity_policy", "limitations",
))

function _rofsj_walk_json_budget(raw;
                                 max_depth::Int=_ROFSJ_MAX_JSON_DEPTH,
                                 max_nodes::Int=_ROFSJ_MAX_JSON_NODES,
                                 max_string_bytes::Int=_ROFSJ_MAX_STRING_BYTES)
    nodes = Ref(0)
    function visit(value, depth)
        depth <= max_depth || throw(ArgumentError(
            "adaptive RO-field JSON exceeds max_json_depth=$max_depth"))
        nodes[] += 1
        nodes[] <= max_nodes || throw(ArgumentError(
            "adaptive RO-field JSON exceeds the node budget"))
        if value isa AbstractDict
            for (key, child) in pairs(value)
                text = String(key)
                ncodeunits(text) <= max_string_bytes || throw(ArgumentError(
                    "adaptive RO-field JSON contains an oversized key"))
                visit(child, depth + 1)
            end
        elseif value isa AbstractVector || value isa Tuple
            for child in value
                visit(child, depth + 1)
            end
        elseif value isa AbstractString || value isa Symbol
            ncodeunits(String(value)) <= max_string_bytes ||
                throw(ArgumentError(
                    "adaptive RO-field JSON contains an oversized string"))
        end
        return nothing
    end
    visit(raw, 0)
    return nodes[]
end

function _rofsj_exact(raw, keys::Set{String}, path::AbstractString)
    return _rofjob_exact_keys(raw, keys, path)
end

function _rofsj_finite_vector(raw, path::AbstractString)
    values = _rofc_array(raw, path)
    return Float64[_rofc_finite(value, "$(path)[]") for value in values]
end

function _rofsj_matrix(raw, rows::Int, columns::Int, path::AbstractString)
    raw_rows = _rofc_array(raw, path)
    length(raw_rows) == rows || throw(DimensionMismatch(
        "$path must contain $rows rows"))
    matrix = Matrix{Float64}(undef, rows, columns)
    for (row_index, raw_row) in enumerate(raw_rows)
        row = _rofsj_finite_vector(raw_row, "$path[$row_index]")
        length(row) == columns || throw(DimensionMismatch(
            "$path rows must contain $columns values"))
        matrix[row_index, :] = row
    end
    return matrix
end

_rofsj_matrix_payload(matrix::AbstractMatrix) = Any[
    Float64[matrix[row, column] for column in axes(matrix, 2)]
    for row in axes(matrix, 1)
]

function _rofsj_portable_engine_payload(raw, path::AbstractString)
    encoded = try
        JSON3.write(raw)
    catch err
        throw(ArgumentError(
            "$path is not a portable JSON payload: " * sprint(showerror, err)))
    end
    ncodeunits(encoded) <= _ROFSJ_MAX_DOCUMENT_BYTES || throw(ArgumentError(
        "$path exceeds the adaptive document byte budget"))
    parsed = JSON3.read(encoded)
    _rofsj_walk_json_budget(parsed)
    return _rofc_materialize(
        parsed, path;
        max_nodes=_ROFSJ_MAX_JSON_NODES,
        max_depth=_ROFSJ_MAX_JSON_DEPTH,
        max_string_bytes=_ROFSJ_MAX_STRING_BYTES,
        max_total_string_bytes=_ROFSJ_MAX_DOCUMENT_BYTES)
end

_rofsj_file_sha256(path::AbstractString) = "sha256:" *
    bytes2hex(SHA.sha256(read(String(path))))

function _rofsj_source_tree_identity(source_root::AbstractString)
    root = normpath(String(source_root))
    isdir(root) || throw(ArgumentError(
        "adaptive solver source-tree root is missing"))
    Base.include_dependency(root)
    paths = String[]
    for (directory, _, names) in walkdir(root)
        Base.include_dependency(directory)
        for name in names
            endswith(name, ".jl") || continue
            path = joinpath(directory, name)
            Base.include_dependency(path)
            push!(paths, path)
        end
    end
    sort!(paths; by=path -> replace(relpath(path, root), '\\' => '/'))
    entries = Dict{String,Any}[]
    total_bytes = BigInt(0)
    for path in paths
        relative_path = replace(relpath(path, root), '\\' => '/')
        byte_count = filesize(path)
        total_bytes += byte_count
        push!(entries, Dict{String,Any}(
            "relative_path" => relative_path,
            "byte_count" => byte_count,
            "sha256" => _rofsj_file_sha256(path),
        ))
    end
    total_bytes <= typemax(Int) || throw(ArgumentError(
        "adaptive solver source tree does not fit in Int"))
    return Dict{String,Any}(
        "path_policy" => "recursive_julia_files_relative_posix_order",
        "capture_semantics" =>
            "module_load_frozen_with_precompile_content_dependencies",
        "file_count" => length(entries),
        "total_bytes" => Int(total_bytes),
        "tree_sha256" => _rofc_sha256(Dict{String,Any}(
            "files" => entries)),
    )
end

# This digest describes the source bytes that were present when the backend
# module was loaded.  Resume and result validation compare against this frozen
# process identity instead of re-hashing a potentially changed checkout.
const _ROFSJ_LOADED_WEB_SOURCE_IDENTITY =
    _rofsj_source_tree_identity(@__DIR__)
const _ROFSJ_LOADED_SPARSE_IMPLEMENTATION_SHA256 =
    _rofsj_file_sha256(@__FILE__)
const _ROFSJ_WEB_SOURCE_IDENTITY_TEST_OVERRIDE = Ref{Any}(nothing)

function _rofsj_loaded_web_source_identity()
    override = _ROFSJ_WEB_SOURCE_IDENTITY_TEST_OVERRIDE[]
    identity = override === nothing ?
        _ROFSJ_LOADED_WEB_SOURCE_IDENTITY : override
    identity isa AbstractDict || throw(ArgumentError(
        "adaptive loaded Web source identity must be an object"))
    Set(String.(keys(identity))) == Set((
        "path_policy", "capture_semantics", "file_count", "total_bytes",
        "tree_sha256",
    )) || throw(ArgumentError(
        "adaptive loaded Web source identity fields are incomplete"))
    identity["path_policy"] ==
        "recursive_julia_files_relative_posix_order" ||
        throw(ArgumentError(
            "unsupported adaptive Web source-tree path policy"))
    identity["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies" ||
        throw(ArgumentError(
            "unsupported adaptive Web source-tree capture semantics"))
    _rofjob_int(identity["file_count"],
        "web source file_count"; minimum=1)
    _rofjob_int(identity["total_bytes"],
        "web source total_bytes"; minimum=1)
    _rofjob_sha(identity["tree_sha256"], "web source tree_sha256")
    return deepcopy(identity)
end

function _rofsj_with_web_source_identity_test_override(
    f::Function,
    identity,
)
    previous = _ROFSJ_WEB_SOURCE_IDENTITY_TEST_OVERRIDE[]
    _ROFSJ_WEB_SOURCE_IDENTITY_TEST_OVERRIDE[] = deepcopy(identity)
    try
        return f()
    finally
        _ROFSJ_WEB_SOURCE_IDENTITY_TEST_OVERRIDE[] = previous
    end
end

function _rofsj_package_version_identity(package_module::Module)
    package_id = Base.PkgId(package_module)
    version = Base.pkgversion(package_module)
    version === nothing && throw(ArgumentError(
        "adaptive solver package $(package_id.name) has no version identity"))
    return Dict{String,Any}(
        "module_name" => string(package_module),
        "package_name" => package_id.name,
        "package_uuid" => string(package_id.uuid),
        "package_version" => string(version),
    )
end

function _rofsj_binding_source_identity()
    entrypoint = pathof(BindingAndCatalysis)
    entrypoint === nothing && throw(ArgumentError(
        "BindingAndCatalysis has no source entrypoint identity"))
    return _rofsj_source_tree_identity(dirname(String(entrypoint)))
end

function _rofsj_active_lock_identity()
    project_path = Base.active_project()
    project_path === nothing && throw(ArgumentError(
        "adaptive solver requires an active Julia project"))
    project = String(project_path)
    manifest = VERSION < v"1.11" ?
        joinpath(dirname(project), "Manifest-v1.10.toml") :
        joinpath(dirname(project), "Manifest.toml")
    isfile(project) || throw(ArgumentError(
        "adaptive solver active Project.toml is missing"))
    isfile(manifest) || throw(ArgumentError(
        "adaptive solver selected Manifest is missing"))
    Base.include_dependency(project)
    Base.include_dependency(manifest)
    return Dict{String,Any}(
        "capture_semantics" =>
            "module_load_frozen_with_precompile_content_dependencies",
        "selection_policy" => VERSION < v"1.11" ?
            "julia_1_10_versioned_manifest" : "default_manifest",
        "project_sha256" => _rofsj_file_sha256(project),
        "manifest_sha256" => _rofsj_file_sha256(manifest),
    )
end

const _ROFSJ_LOADED_BINDING_SOURCE_IDENTITY =
    _rofsj_binding_source_identity()
const _ROFSJ_LOADED_ACTIVE_LOCK_IDENTITY =
    _rofsj_active_lock_identity()
const _ROFSJ_BINDING_SOURCE_IDENTITY_TEST_OVERRIDE = Ref{Any}(nothing)
const _ROFSJ_ACTIVE_LOCK_IDENTITY_TEST_OVERRIDE = Ref{Any}(nothing)

function _rofsj_loaded_binding_source_identity()
    override = _ROFSJ_BINDING_SOURCE_IDENTITY_TEST_OVERRIDE[]
    identity = override === nothing ?
        _ROFSJ_LOADED_BINDING_SOURCE_IDENTITY : override
    identity isa AbstractDict || throw(ArgumentError(
        "adaptive loaded BindingAndCatalysis source identity must be an object"))
    Set(String.(keys(identity))) == Set((
        "path_policy", "capture_semantics", "file_count", "total_bytes",
        "tree_sha256",
    )) || throw(ArgumentError(
        "adaptive loaded BindingAndCatalysis source identity fields are incomplete"))
    identity["path_policy"] ==
        "recursive_julia_files_relative_posix_order" ||
        throw(ArgumentError(
            "unsupported BindingAndCatalysis source-tree path policy"))
    identity["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies" ||
        throw(ArgumentError(
            "unsupported BindingAndCatalysis source-tree capture semantics"))
    _rofjob_int(identity["file_count"],
        "binding source file_count"; minimum=1)
    _rofjob_int(identity["total_bytes"],
        "binding source total_bytes"; minimum=1)
    _rofjob_sha(identity["tree_sha256"], "binding source tree_sha256")
    return deepcopy(identity)
end

function _rofsj_loaded_active_lock_identity()
    override = _ROFSJ_ACTIVE_LOCK_IDENTITY_TEST_OVERRIDE[]
    identity = override === nothing ?
        _ROFSJ_LOADED_ACTIVE_LOCK_IDENTITY : override
    identity isa AbstractDict || throw(ArgumentError(
        "adaptive loaded Project/Manifest identity must be an object"))
    Set(String.(keys(identity))) == Set((
        "capture_semantics", "selection_policy", "project_sha256",
        "manifest_sha256",
    )) || throw(ArgumentError(
        "adaptive loaded Project/Manifest identity fields are incomplete"))
    identity["capture_semantics"] ==
        "module_load_frozen_with_precompile_content_dependencies" ||
        throw(ArgumentError(
            "unsupported Project/Manifest capture semantics"))
    identity["selection_policy"] in (
        "julia_1_10_versioned_manifest", "default_manifest") ||
        throw(ArgumentError("unsupported manifest selection policy"))
    for (key, label) in (
        ("project_sha256", "active Project sha256"),
        ("manifest_sha256", "active Manifest sha256"),
    )
        raw = identity[key]
        raw isa AbstractString &&
            occursin(r"^sha256:[0-9a-f]{64}$", String(raw)) ||
            throw(ArgumentError(
                "$label must be a sha256:-prefixed lowercase SHA-256 string"))
    end
    return deepcopy(identity)
end

function _rofsj_with_binding_source_identity_test_override(
    f::Function,
    identity,
)
    previous = _ROFSJ_BINDING_SOURCE_IDENTITY_TEST_OVERRIDE[]
    _ROFSJ_BINDING_SOURCE_IDENTITY_TEST_OVERRIDE[] = deepcopy(identity)
    try
        return f()
    finally
        _ROFSJ_BINDING_SOURCE_IDENTITY_TEST_OVERRIDE[] = previous
    end
end

function _rofsj_with_active_lock_identity_test_override(
    f::Function,
    identity,
)
    previous = _ROFSJ_ACTIVE_LOCK_IDENTITY_TEST_OVERRIDE[]
    _ROFSJ_ACTIVE_LOCK_IDENTITY_TEST_OVERRIDE[] = deepcopy(identity)
    try
        return f()
    finally
        _ROFSJ_ACTIVE_LOCK_IDENTITY_TEST_OVERRIDE[] = previous
    end
end

function _rofsj_solver_runtime_identity()
    identity = Dict{String,Any}(
        "scope" =>
            "exact_runtime_lock_and_executed_source_tree_for_local_resume",
        "julia" => Dict{String,Any}(
            "version" => string(VERSION),
            "machine" => Sys.MACHINE,
            "word_size" => Sys.WORD_SIZE,
        ),
        "binding_and_catalysis" =>
            _rofsj_package_version_identity(BindingAndCatalysis),
        "ordinary_diffeq" => _rofsj_package_version_identity(
            BindingAndCatalysis.ODE),
        "diff_eq_callbacks" => _rofsj_package_version_identity(
            BindingAndCatalysis.CB),
        "sciml_base" => _rofsj_package_version_identity(
            BindingAndCatalysis.SciMLBase),
        "active_lock" => _rofsj_loaded_active_lock_identity(),
        "binding_source" => _rofsj_loaded_binding_source_identity(),
        "webapp_loaded_source" =>
            _rofsj_loaded_web_source_identity(),
        "web_sparse_implementation_sha256" =>
            _ROFSJ_LOADED_SPARSE_IMPLEMENTATION_SHA256,
    )
    return Dict{String,Any}(
        "identity" => identity,
        "identity_sha256" => _rofc_sha256(identity),
    )
end

function _rofsj_numerical_execution_policy(normalized_limits,
                                             normalized_work_budget)
    max_points = Int(normalized_limits["max_points"])
    max_batches = Int(normalized_limits["max_multi_indices"])
    runtime = _rofsj_solver_runtime_identity()
    total_steps = BigInt(max_points) *
        _ROFSJ_QK2X_MAX_SOLVER_STEPS_PER_POINT
    total_rhs = BigInt(max_points) *
        _ROFSJ_QK2X_MAX_RHS_EVALUATIONS_PER_POINT
    total_steps <= typemax(Int) && total_rhs <= typemax(Int) ||
        throw(ArgumentError(
            "adaptive numerical work policy does not fit in Int"))
    replay_work = normalized_work_budget["max_replay_work_units"]
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_NUMERICAL_POLICY_VERSION,
        "equilibrium_solver" => Dict{String,Any}(
            "method" => "homotopy",
            "algorithm_type" => "OrdinaryDiffEq.Tsit5",
            "anchor_policy" => "model_integration_helper_anchor",
            "manifold_projection" => true,
            "relative_tolerance" => _ROFSJ_QK2X_RELATIVE_TOLERANCE,
            "absolute_tolerance" => _ROFSJ_QK2X_ABSOLUTE_TOLERANCE,
            "max_solver_steps_per_point" =>
                _ROFSJ_QK2X_MAX_SOLVER_STEPS_PER_POINT,
            "max_rhs_evaluations_per_point" =>
                _ROFSJ_QK2X_MAX_RHS_EVALUATIONS_PER_POINT,
            "max_total_solver_steps" => Int(total_steps),
            "max_total_rhs_evaluations" => Int(total_rhs),
            "cancellation_policy" =>
                "before_point_and_every_homotopy_rhs_evaluation",
        ),
        "jacobian_policy" => Dict{String,Any}(
            "method" => "analytic_full_source_log_jacobian",
            "source_order" => "complete_compiled_qK_order",
        ),
        "regime_assignment_policy" =>
            "strict_float64_closed_cell_membership_no_fallback",
        "replay_validation" => Dict{String,Any}(
            "scope" =>
                "checkpoint_and_terminal_artifact_chain_after_bounded_plan_reconstruction",
            "work_model" =>
                "metered_canonical_bytes_json_scalars_copy_artifacts_state_records_samples_scheduler_transitions_point_receipts_and_interpolation_work",
            "artifact_traversal" =>
                "one_authoritative_forward_chain_without_recursive_prefix_replay",
            "max_committed_work_units" => max_batches,
            "max_replay_work_units" => replay_work,
        ),
        "runtime_lock" => runtime,
    )
end

function _rofsj_work_budget(raw)
    budget = _rofc_materialize(raw, "request.work_budget")
    budget isa AbstractDict || throw(ArgumentError(
        "request.work_budget must be an object"))
    observed = Set(String.(keys(budget)))
    required = Set((
        "max_payload_bytes", "max_json_depth", "max_string_bytes",
        "deadline_seconds",
    ))
    isempty(setdiff(observed, _ROFSJ_WORK_BUDGET_KEYS)) ||
        throw(ArgumentError(
            "request.work_budget contains unsupported fields"))
    isempty(setdiff(required, observed)) || throw(ArgumentError(
        "request.work_budget is missing required fields"))
    get(budget, "deadline_seconds", :missing) === nothing ||
        throw(ArgumentError(
            "adaptive compute_ro_field requires deadline_seconds=null"))
    max_payload = _rofjob_int(budget["max_payload_bytes"],
        "request.work_budget.max_payload_bytes";
        minimum=1, maximum=MAX_SYNC_RO_FIELD_INLINE_BYTES)
    max_depth = _rofjob_int(budget["max_json_depth"],
        "request.work_budget.max_json_depth";
        minimum=1, maximum=_ROFSJ_MAX_JSON_DEPTH)
    max_string = _rofjob_int(budget["max_string_bytes"],
        "request.work_budget.max_string_bytes";
        minimum=1, maximum=_ROFSJ_MAX_STRING_BYTES)
    max_replay_work = _rofjob_int(
        get(budget, "max_replay_work_units",
            _ROFSJ_DEFAULT_MAX_REPLAY_WORK_UNITS),
        "request.work_budget.max_replay_work_units";
        minimum=1, maximum=_ROFSJ_HARD_MAX_REPLAY_WORK_UNITS)
    return Dict{String,Any}(
        "max_payload_bytes" => max_payload,
        "max_json_depth" => max_depth,
        "max_string_bytes" => max_string,
        "deadline_seconds" => nothing,
        "max_replay_work_units" => max_replay_work,
    )
end

function _rofsj_sampling_limits(raw)
    limits = _rofsj_exact(raw, _ROFSJ_LIMIT_KEYS,
        "request.sampling_limits")
    max_level = _rofjob_int(limits["max_level"],
        "request.sampling_limits.max_level"; minimum=1, maximum=12)
    max_points = _rofjob_int(limits["max_points"],
        "request.sampling_limits.max_points";
        minimum=1, maximum=_ROFSJ_MAX_POINTS)
    max_indices = _rofjob_int(limits["max_multi_indices"],
        "request.sampling_limits.max_multi_indices";
        minimum=1, maximum=_ROFSJ_MAX_BATCHES)
    max_work = _rofjob_int(limits["max_interpolation_work"],
        "request.sampling_limits.max_interpolation_work";
        minimum=1, maximum=1_000_000_000)
    max_scalars = _rofjob_int(limits["max_payload_scalars"],
        "request.sampling_limits.max_payload_scalars";
        minimum=1, maximum=64_000_000)
    # Never accept a pre-built/positional limits object.  Re-enter the checked
    # keyword constructor from bounded JSON scalars only.
    engine_limits = BindingAndCatalysis.ROSparseSamplingLimits(
        max_level=max_level,
        max_points=max_points,
        max_multi_indices=max_indices,
        max_work=max_work,
        max_payload_scalars=max_scalars,
    )
    normalized = Dict{String,Any}(
        "max_level" => engine_limits.max_level,
        "max_points" => engine_limits.max_points,
        "max_multi_indices" => engine_limits.max_multi_indices,
        "max_interpolation_work" => engine_limits.max_work,
        "max_payload_scalars" => engine_limits.max_payload_scalars,
    )
    return engine_limits, normalized
end

function _rofsj_normalize_outputs(raw, model)
    outputs = _rofsj_exact(raw, _ROFSJ_OUTPUT_KEYS, "request.outputs")
    order = _rofc_ids(outputs["output_order"],
        "request.outputs.output_order", 1, _ROFSJ_MAX_OUTPUTS)
    items = _rofc_array(outputs["items"], "request.outputs.items")
    length(items) == length(order) || throw(ArgumentError(
        "request.outputs.items must match output_order"))
    normalized_items = Dict{String,Any}[]
    indices = Int[]
    for (position, raw_item) in enumerate(items)
        item = _rofsj_exact(raw_item, _ROFSJ_OUTPUT_ITEM_KEYS,
            "request.outputs.items[$position]")
        output_id = _rofc_identifier(item["output_id"],
            "request.outputs.items[$position].output_id")
        output_id == order[position] || throw(ArgumentError(
            "output_order must exactly match item order"))
        symbol = _rofc_string(item["symbol"],
            "request.outputs.items[$position].symbol")
        index = locate_sym_x(model, Symbol(symbol))
        index === nothing && throw(ArgumentError(
            "unknown output species symbol: $symbol"))
        push!(indices, Int(index))
        push!(normalized_items, Dict{String,Any}(
            "output_id" => output_id,
            "symbol" => symbol,
        ))
    end
    allunique(indices) || throw(ArgumentError(
        "adaptive RO-field outputs must map to distinct species"))
    return Dict{String,Any}(
        "output_order" => order,
        "items" => normalized_items,
    ), indices
end

function _rofsj_normalize_request(raw_request)
    _rofsj_walk_json_budget(raw_request)
    request = _rofsj_exact(
        _rofc_materialize(raw_request, "adaptive request"),
        _ROFSJ_REQUEST_KEYS, "adaptive request")
    request["schema_version"] == RO_FIELD_SPARSE_REQUEST_VERSION ||
        throw(ArgumentError(
            "adaptive request schema_version must be " *
            RO_FIELD_SPARSE_REQUEST_VERSION))
    storage = _rofsj_exact(request["storage"], _ROFSJ_STORAGE_KEYS,
        "request.storage")
    storage["mode"] == "chunked" || throw(ArgumentError(
        "adaptive compute_ro_field requires storage.mode=chunked"))
    work_budget = _rofsj_work_budget(request["work_budget"])
    _rofsj_walk_json_budget(request;
        max_depth=work_budget["max_json_depth"],
        max_string_bytes=work_budget["max_string_bytes"])

    network = parse_network_ir(request["network"])
    canonical_network = network_ir_to_dict(network)
    bundle = build_model_bundle(network)
    model = bundle["model"]
    model_candidate_bound(
        model;
        maximum=MAX_JOB_REGIME_CANDIDATES,
        label="adaptive RO-field regime candidate population",
    )
    source_ids = String.(string.(qK_sym(model)))
    1 <= length(source_ids) || error(
        "compiled model has no q/K source coordinates")

    chart = _rofsj_exact(request["chart"], _ROFSJ_CHART_KEYS,
        "request.chart")
    chart_id = _rofc_identifier(chart["chart_id"],
        "request.chart.chart_id")
    control_ids = _rofc_ids(chart["control_ids"],
        "request.chart.control_ids", 1, _ROFSJ_MAX_CONTROLS)
    supplied_sources = String[
        _rofc_string(value, "request.chart.source_coordinate_ids[]")
        for value in _rofc_array(chart["source_coordinate_ids"],
            "request.chart.source_coordinate_ids")
    ]
    supplied_sources == source_ids || throw(ArgumentError(
        "source_coordinate_ids must equal the complete compiled q/K order"))
    jacobian = _rofsj_matrix(chart["jacobian"], length(source_ids),
        length(control_ids), "request.chart.jacobian")
    background = _rofsj_finite_vector(chart["fixed_background"],
        "request.chart.fixed_background")
    length(background) == length(source_ids) || throw(DimensionMismatch(
        "fixed_background must bind every source q/K coordinate"))

    domain = _rofsj_exact(request["domain"], _ROFSJ_DOMAIN_KEYS,
        "request.domain")
    domain["coordinate_space"] == "engine_log10_affine_control" ||
        throw(ArgumentError(
            "request.domain.coordinate_space must be engine_log10_affine_control"))
    lower = _rofsj_finite_vector(domain["lower"], "request.domain.lower")
    upper = _rofsj_finite_vector(domain["upper"], "request.domain.upper")
    length(lower) == length(upper) == length(control_ids) ||
        throw(DimensionMismatch(
            "adaptive domain bounds must match control_ids"))
    all(lower .< upper) || throw(ArgumentError(
        "every adaptive domain span must be positive"))

    normalized_outputs, output_indices =
        _rofsj_normalize_outputs(request["outputs"], model)
    solver = _rofsj_exact(request["solver_policy"], _ROFSJ_SOLVER_KEYS,
        "request.solver_policy")
    solver["algorithm"] ==
        "finite_equilibrium_full_source_log_jacobian" ||
        throw(ArgumentError("unsupported adaptive solver algorithm"))
    solver["output_space"] == "engine_log10_concentration" ||
        throw(ArgumentError("unsupported adaptive output space"))
    solver["source_jacobian_space"] ==
        "d_log10_output_d_log10_source_qK" ||
        throw(ArgumentError("unsupported adaptive source Jacobian space"))
    request["invalid_policy"] ==
        "unresolved_cone_blocks_descendants_only" ||
        throw(ArgumentError("unsupported adaptive invalid policy"))
    tolerance = _rofc_finite(request["surplus_tolerance"],
        "request.surplus_tolerance")
    tolerance > 0 || throw(ArgumentError(
        "surplus_tolerance must be positive"))
    engine_limits, normalized_limits =
        _rofsj_sampling_limits(request["sampling_limits"])
    numerical_execution_policy =
        _rofsj_numerical_execution_policy(
            normalized_limits, work_budget)

    engine_plan = BindingAndCatalysis.ROSparseROChannelPlanV2(
        chart_id=chart_id,
        control_ids=control_ids,
        source_coordinate_ids=source_ids,
        chart_jacobian=jacobian,
        domain_lower=lower,
        domain_upper=upper,
        output_ids=normalized_outputs["output_order"],
        fixed_background=background,
        surplus_tolerance=tolerance,
        limits=engine_limits,
    )
    BindingAndCatalysis.validate_ro_sparse_ro_channel_plan_v2(engine_plan)
    engine_payload = _rofsj_portable_engine_payload(
        BindingAndCatalysis.ro_sparse_ro_channel_plan_v2_payload(engine_plan),
        "engine sparse plan")

    normalized_request = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_REQUEST_VERSION,
        "network" => canonical_network,
        "chart" => Dict{String,Any}(
            "chart_id" => chart_id,
            "control_ids" => control_ids,
            "source_coordinate_ids" => source_ids,
            "jacobian" => _rofsj_matrix_payload(jacobian),
            "fixed_background" => background,
        ),
        "domain" => Dict{String,Any}(
            "coordinate_space" => "engine_log10_affine_control",
            "lower" => lower,
            "upper" => upper,
        ),
        "outputs" => normalized_outputs,
        "solver_policy" => Dict{String,Any}(
            "algorithm" => "finite_equilibrium_full_source_log_jacobian",
            "output_space" => "engine_log10_concentration",
            "source_jacobian_space" =>
                "d_log10_output_d_log10_source_qK",
        ),
        "invalid_policy" =>
            "unresolved_cone_blocks_descendants_only",
        "surplus_tolerance" => tolerance,
        "sampling_limits" => normalized_limits,
        "work_budget" => work_budget,
        "storage" => Dict{String,Any}("mode" => "chunked"),
    )
    network_hash = network_ir_hash(network)
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_PLAN_VERSION,
        "algorithm" => "adaptive_sparse_multi_input_ro_field",
        "algorithm_version" => RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION,
        "network_ir_sha256" => network_hash,
        "request" => normalized_request,
        "engine_sampling_plan" => engine_payload,
        "engine_sampling_plan_sha256" => engine_plan.plan_sha256,
        "numerical_execution_policy" => numerical_execution_policy,
        "channel_order" => "output_major_then_input_minor",
        "work_unit_semantics" => "one_adaptive_multi_index_batch",
        "evidence_scope" => "finite_adaptive_policy_only",
    )
    plan = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_PLAN_VERSION,
        "plan_sha256" => _rofc_sha256(identity),
        "identity" => identity,
    )
    _canonical_hash(identity) == plan["plan_sha256"] || error(
        "adaptive plan canonical hash implementations disagree")
    return (
        request=normalized_request,
        plan=plan,
        network=network,
        bundle=bundle,
        engine_plan=engine_plan,
        output_indices=output_indices,
        chart_jacobian=jacobian,
        numerical_execution_policy=numerical_execution_policy,
    )
end

function normalize_ro_field_sparse_job_spec(raw)
    _rofsj_walk_json_budget(raw)
    spec = _rofc_materialize(raw, "adaptive compute_ro_field spec")
    spec isa AbstractDict || throw(ArgumentError(
        "adaptive compute_ro_field spec must be an object"))
    observed = Set(String.(keys(spec)))
    required = Set(("schema_version", "request"))
    isempty(setdiff(observed, _ROFSJ_SPEC_KEYS)) || throw(ArgumentError(
        "adaptive compute_ro_field spec contains unsupported fields"))
    isempty(setdiff(required, observed)) || throw(ArgumentError(
        "adaptive compute_ro_field spec is missing required fields"))
    spec["schema_version"] == RO_FIELD_SPARSE_JOB_SPEC_VERSION ||
        throw(ArgumentError("unsupported adaptive job spec version"))
    prepared = _rofsj_normalize_request(spec["request"])
    if haskey(spec, "plan")
        supplied = _rofc_materialize(spec["plan"], "adaptive plan")
        _rofc_canonical_json(supplied) ==
            _rofc_canonical_json(prepared.plan) || throw(ArgumentError(
                "caller-supplied adaptive plan differs from derived identity"))
    end
    resume = _rofjob_resume(get(spec, "resume_from", nothing))
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_JOB_SPEC_VERSION,
        "request" => prepared.request,
        "plan" => prepared.plan,
        "resume_from" => resume,
    )
end

_rofsj_data_root(job_id::AbstractString) =
    joinpath(_job_dir(String(job_id)), "ro-field-sparse-v2")
_rofsj_plan_path(root::AbstractString, hash::AbstractString) =
    joinpath(String(root), "plans", String(hash) * ".json")
_rofsj_checkpoint_path(root::AbstractString, hash::AbstractString) =
    joinpath(String(root), "checkpoints", String(hash) * ".json")
_rofsj_manifest_path(root::AbstractString, hash::AbstractString) =
    joinpath(String(root), "manifests", String(hash) * ".json")
_rofsj_artifact_path(root::AbstractString, category::AbstractString,
                     hash::AbstractString) =
    joinpath(String(root), String(category), String(hash) * ".json")

function _rofsj_with_hash(body, hash_key::AbstractString)
    document = deepcopy(body)
    document[String(hash_key)] = _rofc_sha256(body)
    return document
end

mutable struct _ROFSJReplayMeter
    limit::Int
    consumed::BigInt
    breakdown::Dict{String,BigInt}
end

function _rofsj_replay_meter(prepared)
    policy_limit = prepared.numerical_execution_policy[
        "replay_validation"]["max_replay_work_units"]
    request_limit = prepared.request["work_budget"][
        "max_replay_work_units"]
    policy_limit == request_limit || error(
        "adaptive replay policy and request limits disagree")
    return _ROFSJReplayMeter(Int(policy_limit), BigInt(0),
        Dict{String,BigInt}())
end

function _rofsj_charge_replay!(meter::_ROFSJReplayMeter,
                               category::AbstractString,
                               raw_amount::Integer)
    amount = BigInt(raw_amount)
    amount >= 0 || error("adaptive replay work charge is negative")
    requested = meter.consumed + amount
    requested <= meter.limit || throw(ArgumentError(
        "adaptive checkpoint replay exceeds max_replay_work_units=" *
        "$(meter.limit) while charging $(String(category)): " *
        "requested=$requested"))
    meter.consumed = requested
    key = String(category)
    meter.breakdown[key] = get(meter.breakdown, key, BigInt(0)) + amount
    return meter
end

function _rofsj_json_scalar_count(raw)
    if raw isa AbstractDict
        return sum((_rofsj_json_scalar_count(value)
            for value in values(raw)); init=BigInt(0))
    elseif raw isa AbstractVector || raw isa Tuple
        return sum((_rofsj_json_scalar_count(value)
            for value in raw); init=BigInt(0))
    end
    return BigInt(1)
end

function _rofsj_charge_materialized_document!(meter::_ROFSJReplayMeter,
                                               raw)
    _rofsj_charge_replay!(meter, "canonical_json_bytes",
        length(_rofc_bytes(raw)))
    _rofsj_charge_replay!(meter, "parsed_json_scalars",
        _rofsj_json_scalar_count(raw))
    return meter
end

function _rofsj_replay_summary(meter::_ROFSJReplayMeter)
    meter.consumed <= typemax(Int) || error(
        "adaptive replay work total does not fit in Int")
    breakdown = Dict{String,Int}()
    for (key, value) in meter.breakdown
        value <= typemax(Int) || error(
            "adaptive replay work category does not fit in Int")
        breakdown[key] = Int(value)
    end
    return (work_units=Int(meter.consumed), breakdown=breakdown)
end

function _rofsj_write_canonical_once!(
    path::AbstractString,
    document;
    storage_root::AbstractString=local_job_store_dir(),
)
    destination = normpath(abspath(String(path)))
    bytes = _rofc_bytes(document)
    length(bytes) <= _ROFSJ_MAX_DOCUMENT_BYTES || throw(ArgumentError(
        "adaptive RO-field artifact exceeds the document byte budget"))
    return _rofc_write_bytes_once!(
        storage_root, destination, bytes;
        max_existing_bytes=_ROFSJ_MAX_DOCUMENT_BYTES)
end

function _rofsj_validate_expected_content_hash(
    raw,
    hash_key::AbstractString,
    expected_hash::AbstractString,
    label::AbstractString,
)
    raw isa AbstractDict || throw(ArgumentError(
        "$label must be a canonical object"))
    expected = _rofjob_sha(expected_hash, "$label expected hash")
    supplied = _rofjob_sha(
        get(raw, String(hash_key), nothing), "$label $(String(hash_key))")
    supplied == expected || throw(ArgumentError(
        "$label content address changed during resume copy"))
    body = deepcopy(raw)
    pop!(body, String(hash_key))
    _rofc_sha256(body) == expected || throw(ArgumentError(
        "$label self-hash changed during resume copy"))
    return raw
end

function _rofsj_read_canonical(
    path::AbstractString;
    replay_meter=nothing,
    storage_root::AbstractString=local_job_store_dir(),
)
    artifact_path = normpath(abspath(String(path)))
    bytes = _rofc_read_bounded_file(
        storage_root, artifact_path, _ROFSJ_MAX_DOCUMENT_BYTES;
        phase=:adaptive_document_bytes)
    byte_count = length(bytes)
    replay_meter === nothing || _rofsj_charge_replay!(
        replay_meter, "canonical_json_bytes", byte_count)
    isvalid(String, bytes) || throw(ArgumentError(
        "adaptive RO-field artifact is not UTF-8"))
    parsed = try
        JSON3.read(String(copy(bytes)))
    catch err
        throw(ArgumentError(
            "adaptive RO-field artifact is invalid JSON: " *
            sprint(showerror, err)))
    end
    # Traverse the lazy JSON3 tree before allocating ordinary Dict/Vector
    # containers so hostile node/depth populations fail at the decode edge.
    _rofsj_walk_json_budget(parsed)
    raw = _rofc_materialize(
        parsed, "adaptive RO-field artifact";
        max_nodes=_ROFSJ_MAX_JSON_NODES,
        max_depth=_ROFSJ_MAX_JSON_DEPTH,
        max_string_bytes=_ROFSJ_MAX_STRING_BYTES,
        max_total_string_bytes=_ROFSJ_MAX_DOCUMENT_BYTES)
    replay_meter === nothing || _rofsj_charge_replay!(
        replay_meter, "parsed_json_scalars",
        _rofsj_json_scalar_count(raw))
    bytes == _rofc_bytes(raw) || throw(ArgumentError(
        "adaptive RO-field artifact bytes are not canonical"))
    return raw
end

function _rofsj_payload_artifact(schema::AbstractString, plan_hash,
                                  payload; hash_key="artifact_sha256")
    body = Dict{String,Any}(
        "schema_version" => String(schema),
        "plan_sha256" => String(plan_hash),
        "payload" => _rofsj_portable_engine_payload(
            payload, "adaptive engine artifact payload"),
    )
    return _rofsj_with_hash(body, hash_key)
end

function _rofsj_validate_payload_artifact(raw, schema, plan_hash, hash_key)
    document = _rofsj_exact(raw, Set((
        "schema_version", "plan_sha256", "payload", String(hash_key))),
        "adaptive payload artifact")
    document["schema_version"] == schema || throw(ArgumentError(
        "unsupported adaptive payload artifact version"))
    document["plan_sha256"] == plan_hash || throw(ArgumentError(
        "adaptive payload artifact belongs to a different plan"))
    supplied = _rofjob_sha(document[String(hash_key)], String(hash_key))
    body = deepcopy(document)
    pop!(body, String(hash_key))
    supplied == _rofc_sha256(body) || throw(ArgumentError(
        "adaptive payload artifact hash mismatch"))
    return document
end

function _rofsj_validate_plan(raw_plan)
    plan = _rofsj_exact(_rofc_materialize(raw_plan), Set((
        "schema_version", "plan_sha256", "identity")),
        "adaptive plan")
    plan["schema_version"] == RO_FIELD_SPARSE_PLAN_VERSION ||
        throw(ArgumentError("unsupported adaptive plan version"))
    plan_hash = _rofjob_sha(plan["plan_sha256"], "plan.plan_sha256")
    identity = plan["identity"]
    identity isa AbstractDict || throw(ArgumentError(
        "adaptive plan identity must be an object"))
    plan_hash == _rofc_sha256(identity) || throw(ArgumentError(
        "adaptive plan identity hash mismatch"))
    identity["schema_version"] == RO_FIELD_SPARSE_PLAN_VERSION ||
        throw(ArgumentError("adaptive plan identity version mismatch"))
    identity["algorithm"] == "adaptive_sparse_multi_input_ro_field" &&
        identity["algorithm_version"] ==
            RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION || throw(ArgumentError(
                "unsupported adaptive plan algorithm"))
    prepared = _rofsj_normalize_request(identity["request"])
    _rofc_canonical_json(prepared.plan) == _rofc_canonical_json(plan) ||
        throw(ArgumentError(
            "stored adaptive plan is not the canonical derived plan"))
    return plan, prepared
end

function _rofsj_write_plan!(root, plan)
    validated, _ = _rofsj_validate_plan(plan)
    path = _rofsj_plan_path(root, validated["plan_sha256"])
    _rofsj_write_canonical_once!(path, validated)
    return path
end

function _rofsj_read_plan(root, expected_hash)
    hash = _rofjob_sha(expected_hash, "expected adaptive plan hash")
    path = _rofsj_plan_path(root, hash)
    raw = _rofsj_read_canonical(path)
    raw["plan_sha256"] == hash || throw(ArgumentError(
        "adaptive plan filename and document disagree"))
    plan, prepared = _rofsj_validate_plan(raw)
    return plan, prepared
end

function _rofsj_state_artifact(plan, state)
    payload = BindingAndCatalysis.ro_sparse_state_v2_payload(state)
    return _rofsj_payload_artifact(
        RO_FIELD_SPARSE_STATE_ARTIFACT_VERSION,
        plan["plan_sha256"], payload;
        hash_key="state_artifact_sha256")
end

function _rofsj_batch_artifact(plan, batch)
    payload = BindingAndCatalysis.ro_sparse_index_batch_v2_payload(batch)
    return _rofsj_payload_artifact(
        RO_FIELD_SPARSE_BATCH_ARTIFACT_VERSION,
        plan["plan_sha256"], payload;
        hash_key="batch_artifact_sha256")
end

function _rofsj_terminal_artifact(plan, result)
    payload = BindingAndCatalysis.ro_sparse_result_v2_payload(result)
    return _rofsj_payload_artifact(
        RO_FIELD_SPARSE_TERMINAL_ARTIFACT_VERSION,
        plan["plan_sha256"], payload;
        hash_key="terminal_artifact_sha256")
end

function _rofsj_write_payload_artifact!(root, category, document, hash_key)
    hash = _rofjob_sha(document[String(hash_key)], String(hash_key))
    path = _rofsj_artifact_path(root, category, hash)
    _rofsj_write_canonical_once!(path, document)
    return path
end

function _rofsj_read_payload_artifact(root, category, hash, schema,
                                      plan_hash, hash_key;
                                      replay_meter=nothing)
    expected = _rofjob_sha(hash, String(hash_key))
    raw = _rofsj_read_canonical(
        _rofsj_artifact_path(root, category, expected);
        replay_meter=replay_meter)
    raw[String(hash_key)] == expected || throw(ArgumentError(
        "adaptive artifact filename and document disagree"))
    return _rofsj_validate_payload_artifact(
        raw, schema, plan_hash, hash_key)
end

function _rofsj_restore_state(engine_plan, artifact)
    return BindingAndCatalysis.restore_ro_sparse_state_v2(
        engine_plan, artifact["payload"])
end

function _rofsj_restore_batch(engine_plan, state, artifact;
                              validate_prior_state::Bool=true)
    return BindingAndCatalysis.restore_ro_sparse_index_batch_v2(
        engine_plan, state, artifact["payload"];
        validate_prior_state=validate_prior_state)
end

function _rofsj_expected_numerical_failure(err)
    return err isa DomainError || err isa OverflowError ||
        err isa LinearAlgebra.SingularException ||
        err isa LinearAlgebra.LAPACKException
end

function _rofsj_default_batch_evaluator(prepared, bundle, batch,
                                        cancel_check)
    model = bundle["model"]
    outputs = prepared.output_indices
    solver_policy =
        prepared.numerical_execution_policy["equilibrium_solver"]
    source_count = length(
        prepared.request["chart"]["source_coordinate_ids"])
    samples = Dict{String,Any}[]
    for request in batch.requests
        cancel_check()
        theta = copy(request.source_coordinates)
        length(theta) == source_count || error(
            "adaptive request source coordinate dimension drifted")
        status = Ref(:pending)
        solver_limit_reached = Ref(false)
        logx = try
            qK2x(model, theta;
                input_logspace=true,
                output_logspace=true,
                method=:homotopy,
                reltol=solver_policy["relative_tolerance"],
                abstol=solver_policy["absolute_tolerance"],
                ensure_manifold=solver_policy["manifold_projection"],
                maxiters=solver_policy["max_solver_steps_per_point"],
                max_rhs_evaluations=
                    solver_policy["max_rhs_evaluations_per_point"],
                cancel_check=cancel_check,
                status=status)
        catch err
            if err isa BindingAndCatalysis.QK2XWorkLimitExceeded
                solver_limit_reached[] = true
            else
                _rofsj_expected_numerical_failure(err) || rethrow()
            end
            nothing
        end
        if logx === nothing || status[] !== :success ||
                !all(isfinite, logx)
            push!(samples, Dict{String,Any}(
                "point_id" => request.point_id,
                "status" => "invalid",
                "gap" => Dict{String,Any}(
                    "reason" => solver_limit_reached[] ?
                        "solver_work_limit" : "solver_nonconvergence",
                    "detail" => solver_limit_reached[] ?
                        "The plan-bound equilibrium work cap was reached." :
                        "No finite converged equilibrium was available.",
                ),
            ))
            continue
        end
        jacobian = try
            ∂logx_∂logqK(model;
                x=logx,
                qK=theta,
                input_logspace=true)
        catch err
            _rofsj_expected_numerical_failure(err) || rethrow()
            nothing
        end
        selected_outputs = Float64.(logx[outputs])
        selected_source = jacobian === nothing ? nothing :
            Matrix{Float64}(jacobian[outputs, 1:source_count])
        if selected_source === nothing ||
                !all(isfinite, selected_outputs) ||
                !all(isfinite, selected_source)
            push!(samples, Dict{String,Any}(
                "point_id" => request.point_id,
                "status" => "invalid",
                "gap" => Dict{String,Any}(
                    "reason" => "nonfinite_output_or_jacobian",
                    "detail" =>
                        "The equilibrium output or full source Jacobian was non-finite.",
                ),
            ))
            continue
        end
        regime_index = assign_regime_qK(
            model,
            theta;
            input_logspace=true,
            return_idx=true,
            membership=:closed_cell,
        )
        if regime_index < 1
            push!(samples, Dict{String,Any}(
                "point_id" => request.point_id,
                "status" => "invalid",
                "gap" => Dict{String,Any}(
                    "reason" => "missing_regime",
                    "detail" =>
                        "No finite regime identifier was available.",
                ),
            ))
            continue
        end
        push!(samples, Dict{String,Any}(
            "point_id" => request.point_id,
            "status" => "valid",
            "output_values" => selected_outputs,
            "source_reaction_order_matrix" =>
                _rofsj_matrix_payload(selected_source),
            "regime_id" => "regime-$(Int(regime_index))",
        ))
    end
    cancel_check()
    return samples
end

function _rofsj_gap(raw, path)
    gap = _rofsj_exact(raw, Set(("reason", "detail")), path)
    reason = _rofc_identifier(gap["reason"], "$path.reason")
    detail_raw = gap["detail"]
    detail = detail_raw === nothing ? nothing :
        _rofc_string(detail_raw, "$path.detail"; nonempty=false)
    detail === nothing || ncodeunits(detail) <= _ROFSJ_MAX_GAP_DETAIL_BYTES ||
        throw(ArgumentError(
            "$path.detail exceeds the bounded adaptive gap limit"))
    return Dict{String,Any}("reason" => reason, "detail" => detail)
end

function _rofsj_build_point_chunk(plan, prepared, state, batch, raw_samples)
    samples = _rofc_array(raw_samples, "adaptive batch samples")
    length(samples) == length(batch.requests) || throw(ArgumentError(
        "adaptive batch samples must cover every request exactly"))
    output_count = length(prepared.request["outputs"]["output_order"])
    source_count = length(prepared.request["chart"]["source_coordinate_ids"])
    control_count = length(prepared.request["chart"]["control_ids"])
    chart = prepared.chart_jacobian
    normalized = Dict{String,Any}[]
    for (position, request) in enumerate(batch.requests)
        raw = samples[position]
        raw isa AbstractDict || throw(ArgumentError(
            "adaptive batch sample must be an object"))
        point_id = _rofc_string(get(raw, "point_id", nothing),
            "adaptive sample point_id")
        point_id == request.point_id || throw(ArgumentError(
            "adaptive sample point order or identity differs from the batch"))
        status = _rofc_string(get(raw, "status", nothing),
            "adaptive sample status")
        common = Dict{String,Any}(
            "point_id" => request.point_id,
            "multi_index" => copy(request.multi_index),
            "node_ids" => copy(request.node_ids),
            "normalized_coordinates" => copy(request.normalized_coordinates),
            "control_coordinates" => copy(request.control_coordinates),
            "source_coordinates" => copy(request.source_coordinates),
        )
        if status == "valid"
            _rofsj_exact(raw, Set((
                "point_id", "status", "output_values",
                "source_reaction_order_matrix", "regime_id")),
                "adaptive valid sample")
            outputs = _rofsj_finite_vector(raw["output_values"],
                "adaptive sample output_values")
            length(outputs) == output_count || throw(DimensionMismatch(
                "adaptive output_values shape differs from output order"))
            source_matrix = _rofsj_matrix(
                raw["source_reaction_order_matrix"],
                output_count, source_count,
                "adaptive sample source_reaction_order_matrix")
            pulled = source_matrix * chart
            size(pulled) == (output_count, control_count) || error(
                "adaptive RO pullback shape drifted")
            all(isfinite, pulled) || throw(ArgumentError(
                "adaptive RO pullback produced non-finite values"))
            ordered = Float64[pulled[row, column]
                for row in 1:output_count for column in 1:control_count]
            merge!(common, Dict{String,Any}(
                "status" => "valid",
                "output_values" => outputs,
                "source_reaction_order_matrix" =>
                    _rofsj_matrix_payload(source_matrix),
                "reaction_order_matrix" => _rofsj_matrix_payload(pulled),
                "ordered_ro_components" => ordered,
                "regime_id" => _rofc_identifier(raw["regime_id"],
                    "adaptive sample regime_id"),
                "gap" => nothing,
            ))
        elseif status == "invalid"
            _rofsj_exact(raw, Set(("point_id", "status", "gap")),
                "adaptive invalid sample")
            merge!(common, Dict{String,Any}(
                "status" => "invalid",
                "output_values" => nothing,
                "source_reaction_order_matrix" => nothing,
                "reaction_order_matrix" => nothing,
                "ordered_ro_components" => nothing,
                "regime_id" => nothing,
                "gap" => _rofsj_gap(raw["gap"], "adaptive sample gap"),
            ))
        else
            throw(ArgumentError(
                "adaptive sample status must be valid or invalid"))
        end
        push!(normalized, common)
    end
    valid_count = count(sample -> sample["status"] == "valid", normalized)
    body = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_POINT_CHUNK_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "engine_sampling_plan_sha256" => prepared.engine_plan.plan_sha256,
        "prior_state_sha256" => state.state_sha256,
        "engine_batch_sha256" => batch.batch_sha256,
        "batch_ordinal" => batch.batch_ordinal,
        "multi_index" => copy(batch.multi_index),
        "point_ids" => [request.point_id for request in batch.requests],
        "point_count" => length(normalized),
        "samples" => normalized,
        "valid_count" => valid_count,
        "invalid_count" => length(normalized) - valid_count,
    )
    return _rofsj_with_hash(body, "chunk_sha256")
end

function _rofsj_validate_point_chunk(raw, plan, prepared, state, batch)
    chunk = _rofsj_exact(raw, Set((
        "schema_version", "plan_sha256", "engine_sampling_plan_sha256",
        "prior_state_sha256", "engine_batch_sha256", "batch_ordinal",
        "multi_index", "point_ids", "point_count", "samples",
        "valid_count", "invalid_count", "chunk_sha256")),
        "adaptive point-result chunk")
    chunk["schema_version"] == RO_FIELD_SPARSE_POINT_CHUNK_VERSION ||
        throw(ArgumentError("unsupported adaptive point chunk version"))
    chunk["plan_sha256"] == plan["plan_sha256"] &&
        chunk["engine_sampling_plan_sha256"] ==
            prepared.engine_plan.plan_sha256 &&
        chunk["prior_state_sha256"] == state.state_sha256 &&
        chunk["engine_batch_sha256"] == batch.batch_sha256 ||
        throw(ArgumentError(
            "adaptive point chunk belongs to a foreign transition"))
    raw_for_builder = Dict{String,Any}[]
    for raw_sample in _rofc_array(chunk["samples"], "chunk.samples")
        sample = raw_sample::AbstractDict
        if sample["status"] == "valid"
            push!(raw_for_builder, Dict{String,Any}(
                "point_id" => sample["point_id"],
                "status" => "valid",
                "output_values" => sample["output_values"],
                "source_reaction_order_matrix" =>
                    sample["source_reaction_order_matrix"],
                "regime_id" => sample["regime_id"],
            ))
        else
            push!(raw_for_builder, Dict{String,Any}(
                "point_id" => sample["point_id"],
                "status" => "invalid",
                "gap" => sample["gap"],
            ))
        end
    end
    expected = _rofsj_build_point_chunk(
        plan, prepared, state, batch, raw_for_builder)
    _rofc_canonical_json(chunk) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "adaptive point chunk is reordered, tampered, or noncanonical"))
    return expected
end

function _rofsj_chunk_receipts(chunk, batch)
    receipts = Any[]
    for (sample, request) in zip(chunk["samples"], batch.requests)
        evaluation = sample["status"] == "valid" ?
            BindingAndCatalysis.ro_sparse_valid(
                Float64.(sample["ordered_ro_components"])) :
            BindingAndCatalysis.ro_sparse_invalid(
                Symbol(sample["gap"]["reason"]))
        push!(receipts,
            BindingAndCatalysis.ro_sparse_ordered_evaluation_v2(
                request, evaluation))
    end
    return receipts
end

function _rofsj_charge_state_shape!(meter::_ROFSJReplayMeter, payload)
    records = _rofc_array(payload["index_records"],
        "replayed state index_records")
    samples = _rofc_array(payload["samples"],
        "replayed state samples")
    _rofsj_charge_replay!(meter, "parsed_state_records", length(records))
    _rofsj_charge_replay!(meter, "parsed_state_samples", length(samples))
    return meter
end

function _rofsj_charge_transition_compute!(
    meter::_ROFSJReplayMeter,
    state,
    batch,
)
    record_count = length(state.index_records)
    sample_count = length(state.samples)
    scheduler_items = record_count +
        length(state.accepted_multi_indices) +
        length(state.refinement_order) +
        length(state.pending_candidates) +
        length(state.unresolved_regions) + 1
    # Batch restoration and commit each validate the canonical scheduler
    # transition.  Their shallow state checks scan the current state, while
    # commit also constructs and checks the next state.
    _rofsj_charge_replay!(meter, "scheduler_transition_items",
        2 * scheduler_items)
    _rofsj_charge_replay!(meter, "state_record_validation_items",
        3 * record_count + 1)
    _rofsj_charge_replay!(meter, "state_sample_validation_items",
        3 * sample_count + batch.point_count)
    _rofsj_charge_replay!(meter, "transition_copy_items",
        record_count + sample_count + batch.point_count + 1)
    _rofsj_charge_replay!(meter, "point_receipts", batch.point_count)
    _rofsj_charge_replay!(meter, "batch_payload_scalars",
        batch.payload_scalar_count)
    _rofsj_charge_replay!(meter, "batch_interpolation_work",
        batch.interpolation_work)
    return meter
end

function _rofsj_charge_terminal_scheduler!(meter::_ROFSJReplayMeter, state)
    scheduler_items = length(state.index_records) +
        length(state.accepted_multi_indices) +
        length(state.refinement_order) +
        length(state.pending_candidates) +
        length(state.unresolved_regions) + 1
    _rofsj_charge_replay!(meter, "terminal_scheduler_items",
        scheduler_items)
    _rofsj_charge_replay!(meter, "terminal_state_records",
        length(state.index_records))
    _rofsj_charge_replay!(meter, "terminal_state_samples",
        length(state.samples))
    return meter
end

function _rofsj_charge_terminal_result!(meter::_ROFSJReplayMeter,
                                        state, payload)
    records = _rofc_array(payload["index_records"],
        "terminal result index_records")
    samples = _rofc_array(payload["samples"],
        "terminal result samples")
    accepted = _rofc_array(payload["accepted_multi_indices"],
        "terminal result accepted_multi_indices")
    active = _rofc_array(payload["active_frontier"],
        "terminal result active_frontier")
    refinements = _rofc_array(payload["refinement_order"],
        "terminal result refinement_order")
    unresolved = _rofc_array(payload["unresolved_regions"],
        "terminal result unresolved_regions")
    _rofsj_charge_replay!(meter, "terminal_result_records",
        length(records))
    _rofsj_charge_replay!(meter, "terminal_result_samples",
        length(samples))
    _rofsj_charge_replay!(meter, "terminal_result_scheduler_items",
        length(state.index_records) +
        length(state.accepted_multi_indices) +
        length(state.refinement_order) +
        length(state.pending_candidates) +
        length(state.unresolved_regions) + 1)
    _rofsj_charge_replay!(meter, "terminal_result_validation_items",
        length(records) + length(samples) + length(accepted) +
        length(active) + length(refinements) + length(unresolved))
    return meter
end

const _ROFSJ_TRANSITION_KEYS = Set((
    "ordinal", "prior_state_sha256", "prior_state_artifact_sha256",
    "engine_batch_sha256", "batch_artifact_sha256", "chunk_sha256",
    "next_state_sha256", "next_state_artifact_sha256", "point_count",
    "valid_count", "invalid_count", "transition_payload_bytes",
))

function _rofsj_transition_entry(state, state_artifact, batch,
                                  batch_artifact, chunk, next_state,
                                  next_state_artifact)
    payload_bytes = length(_rofc_bytes(batch_artifact)) +
        length(_rofc_bytes(chunk)) + length(_rofc_bytes(next_state_artifact))
    return Dict{String,Any}(
        "ordinal" => batch.batch_ordinal,
        "prior_state_sha256" => state.state_sha256,
        "prior_state_artifact_sha256" =>
            state_artifact["state_artifact_sha256"],
        "engine_batch_sha256" => batch.batch_sha256,
        "batch_artifact_sha256" =>
            batch_artifact["batch_artifact_sha256"],
        "chunk_sha256" => chunk["chunk_sha256"],
        "next_state_sha256" => next_state.state_sha256,
        "next_state_artifact_sha256" =>
            next_state_artifact["state_artifact_sha256"],
        "point_count" => chunk["point_count"],
        "valid_count" => chunk["valid_count"],
        "invalid_count" => chunk["invalid_count"],
        "transition_payload_bytes" => payload_bytes,
    )
end

function _rofsj_checkpoint(plan, prepared, initial_state,
                           initial_state_artifact, transitions,
                           terminal::Bool)
    entries = Dict{String,Any}[deepcopy(entry) for entry in transitions]
    current_state_hash = isempty(entries) ? initial_state.state_sha256 :
        entries[end]["next_state_sha256"]
    current_artifact_hash = isempty(entries) ?
        initial_state_artifact["state_artifact_sha256"] :
        entries[end]["next_state_artifact_sha256"]
    body = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_CHECKPOINT_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "engine_sampling_plan_sha256" => prepared.engine_plan.plan_sha256,
        "initial_state_sha256" => initial_state.state_sha256,
        "initial_state_artifact_sha256" =>
            initial_state_artifact["state_artifact_sha256"],
        "current_state_sha256" => current_state_hash,
        "current_state_artifact_sha256" => current_artifact_hash,
        "committed_work_unit_count" => length(entries),
        "committed_point_count" => sum(
            entry["point_count"] for entry in entries; init=0),
        "committed_payload_bytes" => sum(
            entry["transition_payload_bytes"] for entry in entries; init=0),
        "terminal" => terminal,
        "committed" => entries,
    )
    return _rofsj_with_hash(body, "checkpoint_sha256")
end

function _rofsj_write_checkpoint!(root, checkpoint)
    hash = _rofjob_sha(checkpoint["checkpoint_sha256"],
        "checkpoint_sha256")
    _rofsj_write_canonical_once!(
        _rofsj_checkpoint_path(root, hash), checkpoint)
    return checkpoint
end

function _rofsj_publish_checkpoint!(root, checkpoint, context,
                                    cancel_check)
    cancel_check()
    _rofsj_write_checkpoint!(root, checkpoint)
    cancel_check()
    callback = get(context, "publish_checkpoint", nothing)
    callback === nothing || callback(checkpoint)
    cancel_check()
    return checkpoint
end

function _rofsj_read_transition_artifacts(root, entry, plan, prepared,
                                          state, state_artifact,
                                          cancel_check, replay_meter)
    cancel_check()
    state_artifact["state_artifact_sha256"] ==
        entry["prior_state_artifact_sha256"] &&
        state.state_sha256 == entry["prior_state_sha256"] ||
        throw(ArgumentError(
            "adaptive checkpoint prior-state binding is inconsistent"))

    batch_artifact = _rofsj_read_payload_artifact(
        root, "batches", entry["batch_artifact_sha256"],
        RO_FIELD_SPARSE_BATCH_ARTIFACT_VERSION, plan["plan_sha256"],
        "batch_artifact_sha256"; replay_meter=replay_meter)
    batch = _rofsj_restore_batch(
        prepared.engine_plan, state, batch_artifact;
        validate_prior_state=false)
    batch.batch_sha256 == entry["engine_batch_sha256"] ||
        throw(ArgumentError(
            "adaptive checkpoint engine-batch binding is inconsistent"))
    batch.batch_ordinal == entry["ordinal"] || throw(ArgumentError(
        "adaptive checkpoint batch ordinal is inconsistent"))

    chunk_hash = _rofjob_sha(entry["chunk_sha256"], "chunk_sha256")
    chunk = _rofsj_read_canonical(
        _rofsj_artifact_path(root, "chunks", chunk_hash);
        replay_meter=replay_meter)
    chunk["chunk_sha256"] == chunk_hash || throw(ArgumentError(
        "adaptive point chunk filename and identity disagree"))
    chunk = _rofsj_validate_point_chunk(
        chunk, plan, prepared, state, batch)
    _rofsj_charge_transition_compute!(replay_meter, state, batch)
    receipts = _rofsj_chunk_receipts(chunk, batch)
    next_state = BindingAndCatalysis.commit_ro_sparse_index_batch_v2(
        prepared.engine_plan, state, batch, receipts;
        cancel_check=cancel_check, validate_prior_state=false)
    next_state.state_sha256 == entry["next_state_sha256"] ||
        throw(ArgumentError(
            "adaptive checkpoint next-state binding is inconsistent"))
    next_artifact = _rofsj_read_payload_artifact(
        root, "states", entry["next_state_artifact_sha256"],
        RO_FIELD_SPARSE_STATE_ARTIFACT_VERSION, plan["plan_sha256"],
        "state_artifact_sha256"; replay_meter=replay_meter)
    _rofsj_charge_state_shape!(replay_meter, next_artifact["payload"])
    expected_next_payload = _rofsj_portable_engine_payload(
        BindingAndCatalysis.ro_sparse_state_v2_payload(next_state),
        "replayed next sparse state")
    _rofc_canonical_json(next_artifact["payload"]) ==
        _rofc_canonical_json(expected_next_payload) ||
        throw(ArgumentError(
            "adaptive checkpoint stored next state differs from replay"))
    expected_entry = _rofsj_transition_entry(
        state, state_artifact, batch, batch_artifact, chunk,
        next_state, next_artifact)
    _rofc_canonical_json(entry) == _rofc_canonical_json(expected_entry) ||
        throw(ArgumentError(
            "adaptive checkpoint transition entry is tampered"))
    return (
        state=next_state,
        state_artifact=next_artifact,
        batch=batch,
        batch_artifact=batch_artifact,
        chunk=chunk,
        entry=expected_entry,
    )
end

function _rofsj_replay_checkpoint(root, plan, prepared, raw_checkpoint;
                                  cancel_check=() -> nothing,
                                  replay_meter=nothing)
    meter = replay_meter === nothing ?
        _rofsj_replay_meter(prepared) : replay_meter
    replay_meter === nothing &&
        _rofsj_charge_materialized_document!(meter, raw_checkpoint)
    checkpoint = _rofsj_exact(
        _rofc_materialize(raw_checkpoint), Set((
            "schema_version", "plan_sha256",
            "engine_sampling_plan_sha256", "initial_state_sha256",
            "initial_state_artifact_sha256", "current_state_sha256",
            "current_state_artifact_sha256", "committed_work_unit_count",
            "committed_point_count", "committed_payload_bytes", "terminal",
            "committed", "checkpoint_sha256")),
        "adaptive checkpoint")
    checkpoint["schema_version"] == RO_FIELD_SPARSE_CHECKPOINT_VERSION ||
        throw(ArgumentError("unsupported adaptive checkpoint version"))
    checkpoint["plan_sha256"] == plan["plan_sha256"] &&
        checkpoint["engine_sampling_plan_sha256"] ==
            prepared.engine_plan.plan_sha256 || throw(ArgumentError(
                "adaptive checkpoint belongs to a foreign plan"))
    checkpoint["terminal"] isa Bool || throw(ArgumentError(
        "adaptive checkpoint terminal must be Boolean"))
    entries = _rofc_array(checkpoint["committed"],
        "adaptive checkpoint committed")
    entry_count = length(entries)
    entry_count == _rofjob_int(
        checkpoint["committed_work_unit_count"],
        "checkpoint.committed_work_unit_count";
        maximum=_ROFSJ_MAX_BATCHES) || throw(ArgumentError(
            "adaptive checkpoint work-unit count mismatch"))
    entry_count <= prepared.request["sampling_limits"][
        "max_multi_indices"] || throw(ArgumentError(
            "adaptive checkpoint exceeds the plan-bound work-unit limit"))
    initial_artifact = _rofsj_read_payload_artifact(
        root, "states", checkpoint["initial_state_artifact_sha256"],
        RO_FIELD_SPARSE_STATE_ARTIFACT_VERSION, plan["plan_sha256"],
        "state_artifact_sha256"; replay_meter=meter)
    _rofsj_charge_state_shape!(meter, initial_artifact["payload"])
    initial_state = _rofsj_restore_state(
        prepared.engine_plan, initial_artifact)
    initial_state.state_sha256 == checkpoint["initial_state_sha256"] ||
        throw(ArgumentError(
            "adaptive checkpoint initial-state binding is inconsistent"))
    empty_state = BindingAndCatalysis.initialize_ro_sparse_state_v2(
        prepared.engine_plan; cancel_check=cancel_check)
    BindingAndCatalysis.ro_sparse_state_v2_payload(initial_state) ==
        BindingAndCatalysis.ro_sparse_state_v2_payload(empty_state) ||
        throw(ArgumentError(
            "adaptive checkpoint initial state is not canonical"))

    state = initial_state
    state_artifact = initial_artifact
    normalized_entries = Dict{String,Any}[]
    for (position, raw_entry) in enumerate(entries)
        cancel_check()
        entry = _rofsj_exact(raw_entry, _ROFSJ_TRANSITION_KEYS,
            "adaptive checkpoint committed[$position]")
        entry["ordinal"] == position || throw(ArgumentError(
            "adaptive checkpoint transition order is not contiguous"))
        replayed = _rofsj_read_transition_artifacts(
            root, entry, plan, prepared, state, state_artifact,
            cancel_check, meter)
        state = replayed.state
        state_artifact = replayed.state_artifact
        push!(normalized_entries, replayed.entry)
    end
    checkpoint["current_state_sha256"] == state.state_sha256 &&
        checkpoint["current_state_artifact_sha256"] ==
            state_artifact["state_artifact_sha256"] || throw(ArgumentError(
                "adaptive checkpoint current-state binding is inconsistent"))
    expected = _rofsj_checkpoint(
        plan, prepared, initial_state, initial_artifact,
        normalized_entries, checkpoint["terminal"])
    _rofc_canonical_json(checkpoint) == _rofc_canonical_json(expected) ||
        throw(ArgumentError(
            "adaptive checkpoint counts, order, or hash are inconsistent"))
    if checkpoint["terminal"]
        _rofsj_charge_terminal_scheduler!(meter, state)
        BindingAndCatalysis.prepare_ro_sparse_index_batch_v2(
            prepared.engine_plan, state; cancel_check=cancel_check,
            validate_state=false) === nothing ||
            throw(ArgumentError(
                "adaptive checkpoint claims terminal state with pending work"))
    end
    replay_summary = _rofsj_replay_summary(meter)
    return (
        checkpoint=expected,
        initial_state=initial_state,
        initial_state_artifact=initial_artifact,
        state=state,
        state_artifact=state_artifact,
        entries=normalized_entries,
        replay_work_units=replay_summary.work_units,
        replay_work_breakdown=replay_summary.breakdown,
        replay_meter=meter,
    )
end

function _rofsj_load_checkpoint(root, plan, prepared, checkpoint_hash;
                                cancel_check=() -> nothing,
                                replay_meter=nothing)
    hash = _rofjob_sha(checkpoint_hash, "checkpoint_sha256")
    meter = replay_meter === nothing ?
        _rofsj_replay_meter(prepared) : replay_meter
    raw = _rofsj_read_canonical(
        _rofsj_checkpoint_path(root, hash); replay_meter=meter)
    raw["checkpoint_sha256"] == hash || throw(ArgumentError(
        "adaptive checkpoint filename and identity disagree"))
    return _rofsj_replay_checkpoint(
        root, plan, prepared, raw; cancel_check=cancel_check,
        replay_meter=meter)
end

function _rofsj_manifest(plan, checkpoint, terminal_artifact,
                         terminal_result)
    body = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_MANIFEST_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
        "terminal_artifact_sha256" =>
            terminal_artifact["terminal_artifact_sha256"],
        "engine_result_sha256" => terminal_result.result_sha256,
        "point_count" => terminal_result.evaluated_point_count,
        "work_unit_count" => terminal_result.backend_work_unit_count,
        "valid_count" => terminal_result.valid_point_count,
        "invalid_count" => terminal_result.invalid_point_count,
        "interpolation_work_consumed" =>
            terminal_result.interpolation_work_consumed,
        "committed_payload_bytes" =>
            checkpoint["committed_payload_bytes"],
        "chunks" => Any[Dict{String,Any}(
            "ordinal" => entry["ordinal"],
            "engine_batch_sha256" => entry["engine_batch_sha256"],
            "batch_artifact_sha256" => entry["batch_artifact_sha256"],
            "chunk_sha256" => entry["chunk_sha256"],
            "prior_state_sha256" => entry["prior_state_sha256"],
            "next_state_sha256" => entry["next_state_sha256"],
            "point_count" => entry["point_count"],
            "valid_count" => entry["valid_count"],
            "invalid_count" => entry["invalid_count"],
        ) for entry in checkpoint["committed"]],
    )
    return _rofsj_with_hash(body, "manifest_sha256")
end

function _rofsj_resume_parent_admission(spec, user_sub)
    resume = spec["resume_from"]
    resume === nothing && return nothing
    parent_id = String(resume["parent_job_id"])
    snapshot = _with_job_lock(parent_id) do
        record = _job_record_locked(parent_id)
        record === nothing && throw(ArgumentError(
            "Unknown job_id: $parent_id"))
        _check_user_owns_record(record, user_sub, parent_id)
        String(get(record, "kind", "")) == "compute_ro_field" ||
            throw(ArgumentError(
                "adaptive resume parent is not compute_ro_field"))
        String(get(record, "status", "")) in ("failed", "cancelled") ||
            throw(ArgumentError(
                "adaptive resume parent must be failed or cancelled"))
        get(record, "ro_field_artifact_namespace", nothing) ==
            "ro-field-sparse-v2" || throw(ArgumentError(
                "resume parent is not an adaptive v2 RO-field job"))
        get(record, "ro_field_plan_sha256", nothing) ==
            spec["plan"]["plan_sha256"] || throw(ArgumentError(
                "adaptive resume parent plan differs from child plan"))
        get(record, "latest_checkpoint_sha256", nothing) ==
            resume["checkpoint_sha256"] || throw(ArgumentError(
                "adaptive resume checkpoint is not the parent's linearized checkpoint"))
        _job_snapshot(record)
    end
    root = _rofsj_data_root(parent_id)
    for path in (
        _rofsj_plan_path(root, spec["plan"]["plan_sha256"]),
        _rofsj_checkpoint_path(root, resume["checkpoint_sha256"]),
    )
        _rofc_read_bounded_file(
            local_job_store_dir(), path, _ROFSJ_MAX_DOCUMENT_BYTES;
            phase=:adaptive_document_bytes)
    end
    return (record=snapshot, root=root)
end

function _rofsj_resume_parent_snapshot(spec, user_sub;
                                       cancel_check=() -> nothing)
    admission = _rofsj_resume_parent_admission(spec, user_sub)
    admission === nothing && return nothing
    resume = spec["resume_from"]
    root = admission.root
    cancel_check()
    plan, prepared = _rofsj_read_plan(root, spec["plan"]["plan_sha256"])
    _rofc_canonical_json(plan) == _rofc_canonical_json(spec["plan"]) ||
        throw(ArgumentError("adaptive parent stored plan differs"))
    replay = _rofsj_load_checkpoint(
        root, plan, prepared, resume["checkpoint_sha256"];
        cancel_check=cancel_check)
    return (record=admission.record, root=root, plan=plan,
        prepared=prepared, replay=replay)
end

function validate_ro_field_sparse_resume_parent!(spec, user_sub)
    normalized = normalize_ro_field_sparse_job_spec(spec)
    normalized["resume_from"] === nothing && return normalized
    _rofsj_resume_parent_admission(normalized, String(user_sub))
    return normalized
end

function _rofsj_copy_committed_artifacts!(destination_root, snapshot,
                                          cancel_check)
    source_root = snapshot.root
    replay = snapshot.replay
    meter = replay.replay_meter
    state_hashes = Set{String}([
        replay.checkpoint["initial_state_artifact_sha256"],
    ])
    for entry in replay.entries
        push!(state_hashes, entry["prior_state_artifact_sha256"])
        push!(state_hashes, entry["next_state_artifact_sha256"])
    end
    for hash in sort!(collect(state_hashes))
        cancel_check()
        _rofsj_charge_replay!(
            meter, "copied_artifact_documents", 1)
        raw = _rofsj_read_canonical(
            _rofsj_artifact_path(source_root, "states", hash);
            replay_meter=meter)
        _rofsj_validate_expected_content_hash(
            raw, "state_artifact_sha256", hash,
            "adaptive copied state artifact")
        _rofsj_write_canonical_once!(
            _rofsj_artifact_path(destination_root, "states", hash), raw)
    end
    for entry in replay.entries
        cancel_check()
        batch_hash = entry["batch_artifact_sha256"]
        _rofsj_charge_replay!(
            meter, "copied_artifact_documents", 1)
        batch = _rofsj_read_canonical(
            _rofsj_artifact_path(source_root, "batches", batch_hash);
            replay_meter=meter)
        _rofsj_validate_expected_content_hash(
            batch, "batch_artifact_sha256", batch_hash,
            "adaptive copied batch artifact")
        _rofsj_write_canonical_once!(
            _rofsj_artifact_path(
                destination_root, "batches", batch_hash), batch)
        chunk_hash = entry["chunk_sha256"]
        _rofsj_charge_replay!(
            meter, "copied_artifact_documents", 1)
        chunk = _rofsj_read_canonical(
            _rofsj_artifact_path(source_root, "chunks", chunk_hash);
            replay_meter=meter)
        _rofsj_validate_expected_content_hash(
            chunk, "chunk_sha256", chunk_hash,
            "adaptive copied point chunk")
        _rofsj_write_canonical_once!(
            _rofsj_artifact_path(
                destination_root, "chunks", chunk_hash), chunk)
    end
    checkpoint = replay.checkpoint
    _rofsj_write_checkpoint!(destination_root, checkpoint)
    cancel_check()
    replay_summary = _rofsj_replay_summary(meter)
    return merge(replay, (
        replay_work_units=replay_summary.work_units,
        replay_work_breakdown=replay_summary.breakdown,
        replay_meter=meter,
    ))
end

function _rofsj_payload_reservation(prepared, state_artifact,
                                    batch_artifact, batch)
    source_count = length(
        prepared.request["chart"]["source_coordinate_ids"])
    control_count = length(prepared.request["chart"]["control_ids"])
    output_count = length(prepared.request["outputs"]["output_order"])
    point_count = batch.point_count
    per_point = 8_192 + 256 * source_count +
        512 * output_count * (source_count + control_count + 1)
    next_state_bound = 32_768 + length(_rofc_bytes(state_artifact)) +
        point_count * (4_096 + 128 * output_count * control_count)
    return BigInt(length(_rofc_bytes(batch_artifact))) +
        BigInt(next_state_bound) + BigInt(point_count) * BigInt(per_point)
end

function _rofsj_build_result(plan, prepared, spec, checkpoint, manifest,
                             terminal_result, job_id)
    descriptor = Dict{String,Any}(
        "schema_version" => RO_FIELD_SPARSE_JOB_RESULT_VERSION,
        "algorithm_version" => RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "checkpoint_sha256" => checkpoint["checkpoint_sha256"],
        "dataset_manifest_sha256" => manifest["manifest_sha256"],
        "engine_result_sha256" => terminal_result.result_sha256,
        "network_ir_sha256" => plan["identity"]["network_ir_sha256"],
        "point_count" => manifest["point_count"],
        "work_unit_count" => manifest["work_unit_count"],
        "valid_count" => manifest["valid_count"],
        "invalid_count" => manifest["invalid_count"],
        "interpolation_work_consumed" =>
            manifest["interpolation_work_consumed"],
        "chunk_payload_bytes" => manifest["committed_payload_bytes"],
        "status" => String(terminal_result.status),
        "stopping_reason" => String(terminal_result.stopping_reason),
        "storage" => Dict{String,Any}(
            "mode" => "content_addressed_local_sparse_transitions_v2",
            "plan_ref" =>
                "job://$job_id/ro-field-sparse-v2/plans/$(plan["plan_sha256"])",
            "checkpoint_ref" =>
                "job://$job_id/ro-field-sparse-v2/checkpoints/$(checkpoint["checkpoint_sha256"])",
            "dataset_manifest_ref" =>
                "job://$job_id/ro-field-sparse-v2/manifests/$(manifest["manifest_sha256"])",
        ),
        "lineage" => deepcopy(spec["resume_from"]),
        "evidence" => Dict{String,Any}(
            "evidence_class" => "adaptive_sparse_numerical_transition_dataset",
            "claim_scope" => "finite_adaptive_policy_only",
            "validity_policy" =>
                "invalid_whole_index_is_explicit_unresolved_gap",
            "limitations" => Any[
                "The terminal result proves only the declared finite adaptive policy, not a continuum error bound.",
                "Invalid solver points block only their descendant cone and remain explicit gaps.",
            ],
        ),
    )
    artifact = artifact_metadata(
        "compute_ro_field";
        input_hashes=Dict{String,Any}(
            "network_ir_hash" => plan["identity"]["network_ir_sha256"],
            "plan_sha256" => plan["plan_sha256"],
            "dataset_manifest_sha256" => manifest["manifest_sha256"],
        ),
        algorithm_name="adaptive_sparse_multi_input_ro_field",
        config=plan["identity"],
        warnings=terminal_result.invalid_point_count > 0 ? String[
            "The adaptive result contains explicit invalid gaps.",
        ] : String[],
    )
    artifact["algorithm"]["config_hash"] == plan["plan_sha256"] ||
        error("adaptive result artifact and plan hashes disagree")
    return Dict{String,Any}(
        "ro_field_job_result" => descriptor,
        "artifact" => artifact,
    )
end

function compute_ro_field_sparse_job(raw_spec;
                                     job_context=Dict{String,Any}(),
                                     cancel_check::Function=_no_cancel_check)
    cancel_check()
    spec = normalize_ro_field_sparse_job_spec(raw_spec)
    plan = spec["plan"]
    job_id_raw = get(job_context, "job_id", nothing)
    job_id_raw isa AbstractString &&
        occursin(_ROFJOB_ID_PATTERN, String(job_id_raw)) ||
        throw(ArgumentError(
            "adaptive compute_ro_field requires a local job context"))
    job_id = String(job_id_raw)
    root = _rofsj_data_root(job_id)
    _rofsj_write_plan!(root, plan)
    _, prepared = _rofsj_validate_plan(plan)
    payload_limit = prepared.request["work_budget"]["max_payload_bytes"]

    initial_state = BindingAndCatalysis.initialize_ro_sparse_state_v2(
        prepared.engine_plan; cancel_check=cancel_check)
    initial_artifact = _rofsj_state_artifact(plan, initial_state)
    state = initial_state
    state_artifact = initial_artifact
    transitions = Dict{String,Any}[]
    resume_snapshot = nothing
    if spec["resume_from"] !== nothing
        user_sub = get(job_context, "user_sub", nothing)
        user_sub isa AbstractString || throw(ArgumentError(
            "adaptive resume requires the authenticated owner"))
        resume_snapshot = _rofsj_resume_parent_snapshot(
            spec, String(user_sub); cancel_check=cancel_check)
        replay = _rofsj_copy_committed_artifacts!(
            root, resume_snapshot, cancel_check)
        initial_state = replay.initial_state
        initial_artifact = replay.initial_state_artifact
        state = replay.state
        state_artifact = replay.state_artifact
        append!(transitions, deepcopy(replay.entries))
    else
        _rofsj_write_payload_artifact!(
            root, "states", initial_artifact,
            "state_artifact_sha256")
    end

    checkpoint = _rofsj_checkpoint(
        plan, prepared, initial_state, initial_artifact,
        transitions, false)
    _rofjob_check_payload!(
        checkpoint["committed_payload_bytes"], payload_limit,
        :adaptive_resume_payload)
    _rofsj_publish_checkpoint!(
        root, checkpoint, job_context, cancel_check)

    evaluator = get(job_context, "sparse_batch_evaluator",
        _rofsj_default_batch_evaluator)
    evaluator isa Function || throw(ArgumentError(
        "adaptive sparse_batch_evaluator must be callable"))
    bundle = prepared.bundle
    with_model_bundle_lock(bundle) do
        default_evaluator_ready = false
        while true
            cancel_check()
            batch = BindingAndCatalysis.prepare_ro_sparse_index_batch_v2(
                prepared.engine_plan, state; cancel_check=cancel_check,
                validate_state=false)
            batch === nothing && break
            if evaluator === _rofsj_default_batch_evaluator &&
               !default_evaluator_ready
                cancel_check()
                find_all_regimes!(bundle["model"];
                    cancel_check=cancel_check)
                cancel_check()
                default_evaluator_ready = true
            end
            batch.point_count <= _ROFSJ_MAX_POINTS || throw(ArgumentError(
                "adaptive index batch exceeds the point bound"))
            batch_artifact = _rofsj_batch_artifact(plan, batch)
            reservation = _rofsj_payload_reservation(
                prepared, state_artifact, batch_artifact, batch)
            _rofjob_check_payload!(
                BigInt(checkpoint["committed_payload_bytes"]) + reservation,
                payload_limit, :adaptive_pre_evaluation_reservation)
            raw_samples = evaluator(
                prepared, bundle, batch, cancel_check)
            cancel_check()
            chunk = _rofsj_build_point_chunk(
                plan, prepared, state, batch, raw_samples)
            receipts = _rofsj_chunk_receipts(chunk, batch)
            next_state =
                BindingAndCatalysis.commit_ro_sparse_index_batch_v2(
                    prepared.engine_plan, state, batch, receipts;
                    cancel_check=cancel_check,
                    validate_prior_state=false)
            next_artifact = _rofsj_state_artifact(plan, next_state)
            entry = _rofsj_transition_entry(
                state, state_artifact, batch, batch_artifact, chunk,
                next_state, next_artifact)
            BigInt(entry["transition_payload_bytes"]) <= reservation ||
                error("adaptive transition payload reservation was not conservative")
            exact_payload = BigInt(checkpoint["committed_payload_bytes"]) +
                BigInt(entry["transition_payload_bytes"])
            _rofjob_check_payload!(exact_payload, payload_limit,
                :adaptive_pre_commit_payload)
            cancel_check()
            _rofsj_write_payload_artifact!(
                root, "batches", batch_artifact,
                "batch_artifact_sha256")
            _rofsj_write_canonical_once!(
                _rofsj_artifact_path(
                    root, "chunks", chunk["chunk_sha256"]), chunk)
            _rofsj_write_payload_artifact!(
                root, "states", next_artifact,
                "state_artifact_sha256")
            cancel_check()
            push!(transitions, entry)
            state = next_state
            state_artifact = next_artifact
            checkpoint = _rofsj_checkpoint(
                plan, prepared, initial_state, initial_artifact,
                transitions, false)
            checkpoint["committed_payload_bytes"] == Int(exact_payload) ||
                error("adaptive checkpoint payload accounting drifted")
            _rofsj_publish_checkpoint!(
                root, checkpoint, job_context, cancel_check)
        end
    end

    terminal_result = BindingAndCatalysis.finalize_ro_sparse_state_v2(
        prepared.engine_plan, state; cancel_check=cancel_check,
        validate_state=false)
    terminal_artifact = _rofsj_terminal_artifact(plan, terminal_result)
    terminal_checkpoint = _rofsj_checkpoint(
        plan, prepared, initial_state, initial_artifact,
        transitions, true)
    manifest = _rofsj_manifest(
        plan, terminal_checkpoint, terminal_artifact, terminal_result)
    terminal_payload_bytes = BigInt(length(_rofc_bytes(terminal_artifact))) +
        BigInt(length(_rofc_bytes(terminal_checkpoint))) +
        BigInt(length(_rofc_bytes(manifest)))
    _rofjob_check_payload!(
        BigInt(terminal_checkpoint["committed_payload_bytes"]) +
            terminal_payload_bytes,
        payload_limit, :adaptive_terminal_artifact_reservation)
    cancel_check()
    _rofsj_write_payload_artifact!(
        root, "results", terminal_artifact,
        "terminal_artifact_sha256")
    checkpoint = terminal_checkpoint
    _rofsj_publish_checkpoint!(
        root, checkpoint, job_context, cancel_check)
    _rofsj_write_canonical_once!(
        _rofsj_manifest_path(root, manifest["manifest_sha256"]), manifest)
    cancel_check()
    return _rofsj_build_result(
        plan, prepared, spec, checkpoint, manifest,
        terminal_result, job_id)
end

function _rofsj_read_terminal_chain(root, plan, prepared,
                                    checkpoint_hash, manifest_hash;
                                    cancel_check=() -> nothing)
    replay = _rofsj_load_checkpoint(
        root, plan, prepared, checkpoint_hash;
        cancel_check=cancel_check)
    checkpoint = replay.checkpoint
    meter = replay.replay_meter
    checkpoint["terminal"] || throw(ArgumentError(
        "adaptive result checkpoint is not terminal"))
    payload_limit = prepared.request["work_budget"]["max_payload_bytes"]
    checkpoint["committed_payload_bytes"] <= payload_limit ||
        throw(ArgumentError(
            "adaptive result checkpoint exceeds its declared payload budget"))

    expected_manifest_hash = _rofjob_sha(
        manifest_hash, "dataset_manifest_sha256")
    raw_manifest = _rofsj_read_canonical(
        _rofsj_manifest_path(root, expected_manifest_hash);
        replay_meter=meter)
    manifest = _rofsj_exact(
        raw_manifest, _ROFSJ_MANIFEST_KEYS, "adaptive manifest")
    manifest["schema_version"] == RO_FIELD_SPARSE_MANIFEST_VERSION ||
        throw(ArgumentError("unsupported adaptive manifest version"))
    manifest["manifest_sha256"] == expected_manifest_hash ||
        throw(ArgumentError(
            "adaptive manifest filename and identity disagree"))
    manifest["plan_sha256"] == plan["plan_sha256"] &&
        manifest["checkpoint_sha256"] == checkpoint["checkpoint_sha256"] ||
        throw(ArgumentError(
            "adaptive manifest belongs to a foreign terminal transition"))
    manifest_body = deepcopy(manifest)
    pop!(manifest_body, "manifest_sha256")
    _rofc_sha256(manifest_body) == expected_manifest_hash ||
        throw(ArgumentError("adaptive manifest hash mismatch"))

    terminal_hash = _rofjob_sha(
        manifest["terminal_artifact_sha256"],
        "manifest.terminal_artifact_sha256")
    terminal_artifact = _rofsj_read_payload_artifact(
        root, "results", terminal_hash,
        RO_FIELD_SPARSE_TERMINAL_ARTIFACT_VERSION,
        plan["plan_sha256"], "terminal_artifact_sha256";
        replay_meter=meter)
    terminal_payload_bytes = BigInt(length(_rofc_bytes(terminal_artifact))) +
        BigInt(length(_rofc_bytes(checkpoint))) +
        BigInt(length(_rofc_bytes(manifest)))
    _rofjob_check_payload!(
        BigInt(checkpoint["committed_payload_bytes"]) +
            terminal_payload_bytes,
        payload_limit, :adaptive_terminal_artifact_validation)
    _rofsj_charge_terminal_result!(
        meter, replay.state, terminal_artifact["payload"])
    terminal_result = BindingAndCatalysis.restore_ro_sparse_result_v2(
        prepared.engine_plan, replay.state, terminal_artifact["payload"];
        cancel_check=cancel_check, validate_terminal_state=false)
    expected_manifest = _rofsj_manifest(
        plan, checkpoint, terminal_artifact, terminal_result)
    _rofc_canonical_json(manifest) ==
        _rofc_canonical_json(expected_manifest) || throw(ArgumentError(
            "adaptive manifest does not equal replayed plan, chunks, and terminal state"))
    cancel_check()
    replay_summary = _rofsj_replay_summary(meter)
    return (
        replay=replay,
        checkpoint=checkpoint,
        manifest=expected_manifest,
        terminal_artifact=terminal_artifact,
        terminal_result=terminal_result,
        replay_work_units=replay_summary.work_units,
        replay_work_breakdown=replay_summary.breakdown,
    )
end

function _rofsj_validate_result_artifact!(artifact, plan, manifest,
                                          terminal_result)
    metadata = _validate_result_artifact_metadata(artifact)
    metadata["kind"] == "compute_ro_field" || throw(ArgumentError(
        "adaptive result artifact kind is inconsistent"))
    inputs = metadata["input_hashes"]
    Set(String.(keys(inputs))) == Set((
        "network_ir_hash", "plan_sha256", "dataset_manifest_sha256",
    )) || throw(ArgumentError(
        "adaptive result artifact input hashes are incomplete"))
    inputs == Dict{String,Any}(
        "network_ir_hash" => plan["identity"]["network_ir_sha256"],
        "plan_sha256" => plan["plan_sha256"],
        "dataset_manifest_sha256" => manifest["manifest_sha256"],
    ) || throw(ArgumentError(
        "adaptive result artifact input hashes are inconsistent"))
    algorithm = metadata["algorithm"]
    algorithm["name"] == "adaptive_sparse_multi_input_ro_field" &&
        algorithm["config_hash"] == plan["plan_sha256"] ||
        throw(ArgumentError(
            "adaptive result artifact algorithm identity is inconsistent"))
    warnings = get(metadata, "warnings", nothing)
    expected_warnings = terminal_result.invalid_point_count > 0 ? String[
        "The adaptive result contains explicit invalid gaps.",
    ] : String[]
    warnings == expected_warnings || throw(ArgumentError(
        "adaptive result artifact warnings are inconsistent"))
    return metadata
end

function validate_ro_field_sparse_job_result!(
    result, job_id::AbstractString, expected_plan_sha256::AbstractString;
    record=nothing, cancel_check=() -> nothing,
)
    result isa AbstractDict || throw(ArgumentError(
        "adaptive RO-field job result must be an object"))
    Set(String.(keys(result))) == Set(("ro_field_job_result", "artifact")) ||
        throw(ArgumentError(
            "adaptive RO-field result must contain descriptor and artifact only"))
    descriptor = _rofsj_exact(
        _rofc_materialize(result["ro_field_job_result"]),
        _ROFSJ_RESULT_KEYS, "adaptive ro_field_job_result")
    descriptor["schema_version"] == RO_FIELD_SPARSE_JOB_RESULT_VERSION ||
        throw(ArgumentError("unsupported adaptive result version"))
    descriptor["algorithm_version"] ==
        RO_FIELD_SPARSE_JOB_ALGORITHM_VERSION || throw(ArgumentError(
            "unsupported adaptive result algorithm"))
    plan_hash = _rofjob_sha(
        descriptor["plan_sha256"], "result.plan_sha256")
    plan_hash == _rofjob_sha(
        expected_plan_sha256, "expected plan_sha256") || throw(ArgumentError(
            "adaptive result plan does not match the submitted job"))
    checkpoint_hash = _rofjob_sha(
        descriptor["checkpoint_sha256"], "result.checkpoint_sha256")
    manifest_hash = _rofjob_sha(
        descriptor["dataset_manifest_sha256"],
        "result.dataset_manifest_sha256")
    _rofjob_sha(descriptor["engine_result_sha256"],
        "result.engine_result_sha256")
    _rofjob_sha(descriptor["network_ir_sha256"],
        "result.network_ir_sha256")

    storage = _rofsj_exact(
        descriptor["storage"], _ROFSJ_RESULT_STORAGE_KEYS,
        "adaptive result.storage")
    expected_storage = Dict{String,Any}(
        "mode" => "content_addressed_local_sparse_transitions_v2",
        "plan_ref" =>
            "job://$job_id/ro-field-sparse-v2/plans/$plan_hash",
        "checkpoint_ref" =>
            "job://$job_id/ro-field-sparse-v2/checkpoints/$checkpoint_hash",
        "dataset_manifest_ref" =>
            "job://$job_id/ro-field-sparse-v2/manifests/$manifest_hash",
    )
    storage == expected_storage || throw(ArgumentError(
        "adaptive result artifact references are inconsistent"))
    lineage = descriptor["lineage"] === nothing ? nothing :
        _rofjob_resume(descriptor["lineage"])
    evidence = _rofsj_exact(
        descriptor["evidence"], _ROFSJ_RESULT_EVIDENCE_KEYS,
        "adaptive result.evidence")
    evidence["evidence_class"] ==
        "adaptive_sparse_numerical_transition_dataset" &&
        evidence["claim_scope"] == "finite_adaptive_policy_only" &&
        evidence["validity_policy"] ==
            "invalid_whole_index_is_explicit_unresolved_gap" ||
        throw(ArgumentError(
            "adaptive result evidence semantics are unsupported"))
    limitations = evidence["limitations"]
    limitations isa AbstractVector && length(limitations) == 2 &&
        all(item -> item isa AbstractString && !isempty(item), limitations) ||
        throw(ArgumentError(
            "adaptive result limitations are incomplete"))

    root = _rofsj_data_root(job_id)
    plan, prepared = _rofsj_read_plan(root, plan_hash)
    chain = _rofsj_read_terminal_chain(
        root, plan, prepared, checkpoint_hash, manifest_hash;
        cancel_check=cancel_check)
    expected_descriptor = _rofsj_build_result(
        plan, prepared, Dict{String,Any}("resume_from" => lineage),
        chain.checkpoint, chain.manifest, chain.terminal_result,
        String(job_id))["ro_field_job_result"]
    _rofc_canonical_json(descriptor) ==
        _rofc_canonical_json(expected_descriptor) || throw(ArgumentError(
            "adaptive result descriptor disagrees with replayed artifacts"))
    _rofsj_validate_result_artifact!(
        result["artifact"], plan, chain.manifest,
        chain.terminal_result)

    if record !== nothing
        record isa AbstractDict || throw(ArgumentError(
            "adaptive contextual result validation requires a job record"))
        String(get(record, "job_id", "")) == String(job_id) ||
            throw(ArgumentError(
                "adaptive result record job identity is inconsistent"))
        String(get(record, "kind", "")) == "compute_ro_field" &&
            get(record, "ro_field_artifact_namespace", nothing) ==
                "ro-field-sparse-v2" || throw(ArgumentError(
                    "adaptive result record namespace is inconsistent"))
        get(record, "ro_field_plan_sha256", nothing) == plan_hash &&
            get(record, "ro_field_network_ir_sha256", nothing) ==
                descriptor["network_ir_sha256"] || throw(ArgumentError(
                    "adaptive result record scientific identity is inconsistent"))
        get(record, "latest_checkpoint_sha256", nothing) ==
            checkpoint_hash || throw(ArgumentError(
                "adaptive result checkpoint is not the linearized record checkpoint"))
        expected_lineage_raw = get(record, "resume_from", nothing)
        expected_lineage = expected_lineage_raw === nothing ? nothing :
            _rofjob_resume(expected_lineage_raw)
        lineage == expected_lineage || throw(ArgumentError(
            "adaptive result lineage differs from the submitted child lineage"))
        for (key, value) in (
            "committed_work_unit_count" =>
                chain.checkpoint["committed_work_unit_count"],
            "committed_point_count" =>
                chain.checkpoint["committed_point_count"],
            "committed_payload_bytes" =>
                chain.checkpoint["committed_payload_bytes"],
        )
            get(record, key, nothing) == value || throw(ArgumentError(
                "adaptive result record $key is inconsistent"))
        end
        haskey(record, "ro_field_dataset_manifest_sha256") ||
            throw(ArgumentError(
                "adaptive terminal result record is missing its manifest identity"))
        record["ro_field_dataset_manifest_sha256"] == manifest_hash ||
            throw(ArgumentError(
                "adaptive result record manifest identity is inconsistent"))
    end
    cancel_check()
    return descriptor
end
