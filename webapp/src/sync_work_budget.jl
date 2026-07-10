# Hard limits for synchronous, non-cancellable API work. Requests beyond these
# bounds belong on `/api/v1/jobs`, where identity, quota, status, and
# cancellation are available. The limits remain at or above the tracked atlas's
# current maximum model size (n=9, r=5).
struct SyncBudgetExceeded <: Exception
    msg::String
end
Base.showerror(io::IO, err::SyncBudgetExceeded) = print(io, err.msg)

struct SyncCapacityExceeded <: Exception
    msg::String
end
Base.showerror(io::IO, err::SyncCapacityExceeded) = print(io, err.msg)

const MAX_SYNC_MODEL_N = 24
const MAX_SYNC_REACTIONS = 5
const MAX_SYNC_REGIME_CANDIDATES = 20_000
const MAX_SYNC_DESIGN_CARDS = 64
const MAX_SYNC_EXACT_PLACEMENTS = 8
const MAX_SYNC_SCAN_OUTPUTS = 16
const MAX_SYNC_EXPRESSION_BYTES = 1024
const MAX_SYNC_ROP_SAMPLES = 20_000
const MAX_SYNC_ROP_POINTS = 20_000
const MAX_SYNC_SCAN_SOLVE_COST = 50_000_000
const MAX_SYNC_ROP_CLOUD_COST = 20_000_000
const MAX_SYNC_ROP_GEOMETRY_COST = 5_000_000

_sync_jobs_hint() = "Reduce the request. Atlas/library/query/inverse-design workloads may use /api/v1/jobs."

function _sync_budget_exceeded(message::AbstractString)
    throw(SyncBudgetExceeded("$(message) $(_sync_jobs_hint())"))
end

function sync_bounded_int(value, name::AbstractString; min::Int=0, max::Int)
    (value isa Real && !(value isa Bool) && isfinite(value) && isinteger(value)) ||
        throw(ArgumentError("$name must be a finite integer"))
    result = try
        Int(value)
    catch
        _sync_budget_exceeded("$name is outside the supported integer range.")
    end
    result < min && throw(ArgumentError("$name must be >= $min"))
    result > max && _sync_budget_exceeded("$name exceeds the synchronous limit of $max.")
    return result
end

function sync_finite_float(value, name::AbstractString; abs_max::Real=20.0)
    (value isa Real && !(value isa Bool)) || throw(ArgumentError("$name must be a number"))
    result = Float64(value)
    isfinite(result) || throw(ArgumentError("$name must be finite"))
    abs(result) <= abs_max ||
        throw(ArgumentError("$name must be within [-$(Float64(abs_max)), $(Float64(abs_max))]"))
    return result
end

function sync_finite_range(lo, hi, name::AbstractString; abs_max::Real=20.0)
    lower = sync_finite_float(lo, "$(name)_min"; abs_max=abs_max)
    upper = sync_finite_float(hi, "$(name)_max"; abs_max=abs_max)
    upper > lower || throw(ArgumentError("$(name)_max must be greater than $(name)_min"))
    return lower, upper
end

function enforce_sync_rule_budget(rules)
    length(rules) <= MAX_SYNC_REACTIONS ||
        _sync_budget_exceeded("Reaction count exceeds the synchronous limit of $(MAX_SYNC_REACTIONS).")
    _, species, _, _ = parse_network_structure(String.(rules))
    length(species) <= MAX_SYNC_MODEL_N ||
        _sync_budget_exceeded("Model dimension exceeds the synchronous limit of $(MAX_SYNC_MODEL_N).")
    return rules
end

function enforce_sync_model_budget(model)
    model.r <= MAX_SYNC_REACTIONS ||
        _sync_budget_exceeded("Model reaction count exceeds the synchronous limit of $(MAX_SYNC_REACTIONS).")
    model.n <= MAX_SYNC_MODEL_N ||
        _sync_budget_exceeded("Model dimension exceeds the synchronous limit of $(MAX_SYNC_MODEL_N).")
    sync_model_candidate_bound(model)
    return model
end

function sync_model_candidate_bound(model)
    candidate_bound = 1
    for choices in model._L_helper.J
        count = length(choices)
        count == 0 && return 0
        candidate_bound > MAX_SYNC_REGIME_CANDIDATES ÷ count &&
            _sync_budget_exceeded(
                "The regime-enumeration candidate bound exceeds $(MAX_SYNC_REGIME_CANDIDATES).")
        candidate_bound *= count
    end
    return candidate_bound
end

function enforce_sync_cost(cost::Integer, limit::Integer, label::AbstractString)
    cost <= limit || _sync_budget_exceeded("$label exceeds its synchronous work budget.")
    return cost
end

# Backpressure is process-wide because all of these handlers compete for the
# same Julia thread pool. The counter lock is held only while reserving a slot.
const SYNC_HEAVY_HANDLER_NAMES = Set{Symbol}((
    :handle_build_model,
    :handle_ir_network_validate,
    :handle_ir_design_validate,
    :handle_build_atlas,
    :handle_query_atlas,
    :handle_build_atlas_library,
    :handle_merge_atlas_library,
    :handle_run_inverse_design,
    :handle_find_vertices,
    :handle_build_graph,
    :handle_siso_paths,
    :handle_siso_polyhedra,
    :handle_siso_path_condition,
    :handle_siso_trajectory,
    :handle_behavior_families,
    :handle_phenotype_classify,
    :handle_rop_cloud,
    :handle_vertex_detail,
    :handle_fret_heatmap,
    :handle_parameter_scan_1d,
    :handle_parameter_scan_2d,
    :handle_atlas_landscape_2d,
    :handle_rop_polyhedron,
    :handle_place_parameters,
    :handle_placer_menu,
    :handle_placer_curve,
    :handle_placer_threshold,
    :handle_placer_realize_program,
    :handle_placer_level,
    :handle_design_search,
    :handle_design_screen,
))

const _SYNC_HEAVY_GATE_LOCK = ReentrantLock()
const _SYNC_HEAVY_ACTIVE = Ref(0)
const _SYNC_HEAVY_LIMIT = 2
const _SYNC_REQUEST_CONTEXT_TLS_KEY = :biocircuits_explorer_sync_request_context

_in_sync_request_context() =
    get(task_local_storage(), _SYNC_REQUEST_CONTEXT_TLS_KEY, false) === true

function _with_sync_request_context(f::Function, enabled::Bool)
    enabled || return f()
    return task_local_storage(_SYNC_REQUEST_CONTEXT_TLS_KEY, true) do
        f()
    end
end

function with_sync_work_gate(f::Function, handler_name::Symbol)
    handler_name in SYNC_HEAVY_HANDLER_NAMES || return f()
    admitted = lock(_SYNC_HEAVY_GATE_LOCK) do
        _SYNC_HEAVY_ACTIVE[] >= _SYNC_HEAVY_LIMIT && return false
        _SYNC_HEAVY_ACTIVE[] += 1
        true
    end
    admitted || throw(SyncCapacityExceeded(
        "Synchronous compute capacity is full. Retry later."))
    try
        return _with_sync_request_context(f, true)
    finally
        lock(_SYNC_HEAVY_GATE_LOCK) do
            _SYNC_HEAVY_ACTIVE[] -= 1
        end
    end
end
