# Fixed-topology ROP-native shape optimization.
#
# This owner compiles the existing exact finite-window SISO path geometry into
# the standalone LP core in `rop_shape_optimization.jl`.  It deliberately does
# not participate in catalogue discovery: Design Screen may produce a pinned
# fixed-topology reference, while this endpoint optimizes only that explicitly
# supplied network.  Polyhedral geometry and sampled nonlinear replay remain
# separate evidence layers in the response.

const ROP_SHAPE_OPTIMIZE_REQUEST_VERSION =
    "bne-rop-shape-optimize-request/v1.0.0"
const ROP_SHAPE_OPTIMIZATION_VERSION =
    "bne-rop-shape-optimization/v1.0.0"
const ROP_SHAPE_COMPILER_VERSION = "bne-rop-shape-compiler/v1.0.0"
const ROP_SHAPE_CERTIFICATE_GRADE =
    "exact-window-siso-rop-path-optimization"
const ROP_SHAPE_MAX_SYNC_CELLS = 256
const ROP_SHAPE_MAX_JOB_CELLS = 10_000
const ROP_SHAPE_MAX_SYNC_REPLAYS = 2
const ROP_SHAPE_MAX_JOB_REPLAYS = 16
const ROP_SHAPE_MAX_JOB_REGIME_CANDIDATES = MAX_JOB_REGIME_CANDIDATES
const ROP_SHAPE_MAX_MATERIALIZED_PATHS = MAX_WEB_REGIME_PATHS
const ROP_SHAPE_MAX_MATERIALIZED_PATH_NODES =
    MAX_WEB_MATERIALIZED_PATH_NODES

const _ROP_SHAPE_REQUEST_KEYS = Set((
    "schema_version", "network", "expected_network_ir_hash",
    "designability_spec", "reference", "edit_intent",
    "optimization", "work_budget", "replay",
))

Base.@kwdef struct ROPShapeCellMetadata
    path_idx::Int
    witness_vertices::Vector{Int}
    full_path_vertices::Vector{Int}
    predicted_profile::Vector{Float64}
    qK_symbols::Vector{String}
    cell_id::String
end

Base.@kwdef struct ROPShapePopulation
    cells::Vector{ROPShapeOptimization.DesignabilityCellGeometry} =
        ROPShapeOptimization.DesignabilityCellGeometry[]
    metadata::Vector{ROPShapeCellMetadata} = ROPShapeCellMetadata[]
    eligible_path_count::Int = 0
    evaluated_path_count::Int = 0
    eligible_cell_count::Int = 0
    evaluated_cell_count::Int = 0
    truncated::Bool = false
    truncation_reasons::Vector{String} = String[]
    model::Any = nothing
    input_idx::Int = 0
    output_idx::Int = 0
    projected_symbols::Vector{Symbol} = Symbol[]
end

function _rop_shape_unknown_keys(raw, allowed::Set{String}, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$path must be an object"))
    unknown = sort!(String[String(key) for key in keys(raw) if !(String(key) in allowed)])
    isempty(unknown) || throw(ArgumentError(
        "$path contains unsupported keys: $(join(unknown, ", "))"))
    return nothing
end

function _rop_shape_required_object(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || throw(ArgumentError("$path.$(String(key)) is required"))
    value = _raw_get(raw, key, nothing)
    value isa AbstractDict || throw(ArgumentError("$path.$(String(key)) must be an object"))
    return Dict{String, Any}(_materialize(value))
end

function _rop_shape_required_string(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || throw(ArgumentError("$path.$(String(key)) is required"))
    value = _raw_get(raw, key, nothing)
    (value isa AbstractString && !isempty(strip(String(value)))) ||
        throw(ArgumentError("$path.$(String(key)) must be a non-empty string"))
    return String(value)
end

function _rop_shape_finite(raw, path::AbstractString; minimum=nothing)
    (raw isa Real && !(raw isa Bool)) ||
        throw(ArgumentError("$path must be a finite non-boolean number"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$path must be finite"))
    minimum === nothing || value >= Float64(minimum) ||
        throw(ArgumentError("$path must be at least $(Float64(minimum))"))
    return value
end

function _rop_shape_finite_vector(raw, path::AbstractString; length_required=nothing)
    (raw isa AbstractVector || raw isa Tuple) ||
        throw(ArgumentError("$path must be an array"))
    values = Float64[_rop_shape_finite(value, "$path[$idx]")
                     for (idx, value) in enumerate(raw)]
    length_required === nothing || length(values) == Int(length_required) ||
        throw(ArgumentError("$path must contain exactly $(Int(length_required)) values"))
    return values
end

function _rop_shape_positive_vector(raw, path::AbstractString; length_required=nothing)
    values = _rop_shape_finite_vector(raw, path; length_required=length_required)
    all(>(0.0), values) || throw(ArgumentError("every $path entry must be positive"))
    return values
end

function _rop_shape_sha256(raw, path::AbstractString; prefix::Bool=false)
    raw isa AbstractString || throw(ArgumentError("$path must be a SHA-256 string"))
    value = lowercase(String(raw))
    if prefix
        startswith(value, "sha256:") ||
            throw(ArgumentError("$path must use the sha256:<64hex> form"))
        digest = value[8:end]
        length(digest) == 64 && all(isxdigit, digest) ||
            throw(ArgumentError("$path must use the sha256:<64hex> form"))
    else
        length(value) == 64 && all(isxdigit, value) ||
            throw(ArgumentError("$path must contain exactly 64 hexadecimal characters"))
    end
    return value
end

function _rop_shape_int(raw, path::AbstractString; minimum::Int, maximum::Int)
    (raw isa Integer && !(raw isa Bool)) ||
        throw(ArgumentError("$path must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$path is outside the supported integer range"))
    end
    minimum <= value <= maximum ||
        throw(ArgumentError("$path must be in [$minimum, $maximum]"))
    return value
end

function _rop_shape_step(raw, witness_count::Int, path::AbstractString)
    return _rop_shape_int(raw, path; minimum=0, maximum=witness_count - 1)
end

function _rop_shape_steps(raw, witness_count::Int, path::AbstractString;
                          minimum_length::Int=1, exact_length=nothing)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    steps = Int[_rop_shape_step(value, witness_count, "$path[$idx]")
                for (idx, value) in enumerate(raw)]
    length(steps) >= minimum_length ||
        throw(ArgumentError("$path must contain at least $minimum_length steps"))
    exact_length === nothing || length(steps) == Int(exact_length) ||
        throw(ArgumentError("$path must contain exactly $(Int(exact_length)) steps"))
    length(unique(steps)) == length(steps) ||
        throw(ArgumentError("$path must not contain duplicate steps"))
    return steps
end

_rop_shape_tau(step::Integer) = Symbol("tau_$(Int(step))")

function _rop_shape_reference_payload(reference::AbstractDict)
    payload = Dict{String, Any}(
        "network_ir_hash" => _raw_get(reference, :network_ir_hash, nothing),
        "operating_points_log10" => _raw_get(reference, :operating_points_log10, nothing),
        "kd" => _raw_get(reference, :kd, nothing),
        "totals" => _raw_get(reference, :totals, nothing),
    )
    for key in (:path_identity, :cell_id)
        _raw_haskey(reference, key) &&
            (payload[String(key)] = _raw_get(reference, key, nothing))
    end
    return payload
end

function _rop_shape_normalize_reference(raw, witness_count::Int, reaction_count::Int)
    raw isa AbstractDict || throw(ArgumentError("reference must be an object"))
    allowed = Set((
        "reference_hash", "network_ir_hash", "artifact_ref", "path_identity",
        "cell_id", "operating_points_log10", "kd", "totals",
    ))
    _rop_shape_unknown_keys(raw, allowed, "reference")
    points = _rop_shape_finite_vector(
        _raw_get(raw, :operating_points_log10, nothing),
        "reference.operating_points_log10"; length_required=witness_count)
    kd = _rop_shape_positive_vector(
        _raw_get(raw, :kd, nothing), "reference.kd";
        length_required=reaction_count)
    totals_raw = _raw_get(raw, :totals, nothing)
    totals_raw isa AbstractDict || throw(ArgumentError("reference.totals must be an object"))
    totals = Dict{String, Float64}()
    for (key, value) in pairs(totals_raw)
        name = strip(String(key))
        isempty(name) && throw(ArgumentError("reference.totals keys must be non-empty"))
        concentration = _rop_shape_finite(value, "reference.totals.$name"; minimum=0.0)
        concentration > 0 || throw(ArgumentError("reference.totals.$name must be positive"))
        totals[name] = concentration
    end
    isempty(totals) && throw(ArgumentError("reference.totals must not be empty"))
    normalized = Dict{String, Any}(
        "network_ir_hash" => _rop_shape_sha256(
            _raw_get(raw, :network_ir_hash, nothing), "reference.network_ir_hash"),
        "operating_points_log10" => points,
        "kd" => kd,
        "totals" => totals,
    )
    artifact_ref = _raw_get(raw, :artifact_ref, nothing)
    if artifact_ref !== nothing
        artifact_ref isa AbstractString && !isempty(strip(String(artifact_ref))) ||
            throw(ArgumentError("reference.artifact_ref must be a non-empty string"))
        normalized["artifact_ref"] = String(artifact_ref)
    end
    for key in (:path_identity, :cell_id)
        if _raw_haskey(raw, key)
            value = _raw_get(raw, key, nothing)
            value isa AbstractString && !isempty(strip(String(value))) ||
                throw(ArgumentError("reference.$(String(key)) must be a non-empty string"))
            normalized[String(key)] = String(value)
        end
    end
    expected_hash = _rop_shape_sha256(
        _raw_get(raw, :reference_hash, nothing), "reference.reference_hash")
    actual_hash = _canonical_hash(_rop_shape_reference_payload(normalized))
    expected_hash == actual_hash || throw(ArgumentError(
        "reference.reference_hash does not match the inline reference snapshot"))
    normalized["reference_hash"] = actual_hash
    return normalized
end

function _rop_shape_bounded_product_count(choices::Vector{Vector{Int}})
    isempty(choices) && return 0
    count = 1
    for values in choices
        isempty(values) && return 0
        count > typemax(Int) ÷ length(values) && return typemax(Int)
        count *= length(values)
    end
    return count
end

function _rop_shape_append_c_rows!(Aeq_rows, beq, eq_ids, Aineq_rows, bineq, ineq_ids,
                                   C, C0, nullity::Integer, n_aug::Integer,
                                   id_prefix::AbstractString)
    matrix = Matrix{Float64}(C)
    rhs = Vector{Float64}(C0)
    for row_idx in axes(matrix, 1)
        row = zeros(Float64, Int(n_aug))
        row[1:size(matrix, 2)] .= -matrix[row_idx, :]
        if row_idx <= Int(nullity)
            push!(Aeq_rows, row)
            push!(beq, rhs[row_idx])
            push!(eq_ids, "$id_prefix:eq:$row_idx")
        else
            push!(Aineq_rows, row)
            push!(bineq, rhs[row_idx])
            push!(ineq_ids, "$id_prefix:ineq:$(row_idx - Int(nullity))")
        end
    end
    return nothing
end

function _rop_shape_append_parameter_bounds!(Aineq_rows, bineq, ineq_ids,
                                             model, projected_symbols,
                                             bounds::AbstractDict, n_aug::Int)
    default_bounds = _designability_bounds_tuple(
        _raw_get(bounds, :default, nothing), (-Inf, Inf))
    class_bounds = _raw_get(bounds, :by_class, Dict{String, Any}())
    kd_bounds = _designability_bounds_tuple(
        _raw_get(class_bounds, :kd, nothing), default_bounds)
    total_bounds = _designability_bounds_tuple(
        _raw_get(class_bounds, :total, nothing), default_bounds)
    total_symbols = Set(Symbol.(string.(q_sym(model))))
    kd_symbols = Set(Symbol.(string.(K_sym(model))))
    for (idx, sym) in enumerate(Symbol.(projected_symbols))
        lo, hi = sym in total_symbols ? total_bounds :
                 (sym in kd_symbols ? kd_bounds : default_bounds)
        if isfinite(hi)
            row = zeros(Float64, n_aug); row[idx] = 1.0
            push!(Aineq_rows, row); push!(bineq, hi)
            push!(ineq_ids, "parameter_bound:$(String(sym)):upper")
        end
        if isfinite(lo)
            row = zeros(Float64, n_aug); row[idx] = -1.0
            push!(Aineq_rows, row); push!(bineq, -lo)
            push!(ineq_ids, "parameter_bound:$(String(sym)):lower")
        end
    end
    return nothing
end

function _rop_shape_append_vertex_rows!(Aeq_rows, beq, eq_ids, Aineq_rows, bineq,
                                        ineq_ids, model, vertex_idx::Int,
                                        input_idx::Int, projected_full_indices,
                                        witness_idx::Int, witness_count::Int,
                                        path_idx::Int)
    C_raw, C0_raw, nullity = get_C_C0_nullity_qK(model, vertex_idx)
    C = Matrix{Float64}(C_raw)
    C0 = Vector{Float64}(C0_raw)
    n_bg = length(projected_full_indices)
    n_aug = n_bg + witness_count
    prefix = "path:$path_idx:witness:$(witness_idx - 1):vertex:$vertex_idx"
    for row_idx in axes(C, 1)
        # Engine rows use -C*z <= C0 (or equality for the nullity prefix).
        # Build the core convention A*z <= b directly.
        row = zeros(Float64, n_aug)
        for (bg_idx, full_idx) in enumerate(projected_full_indices)
            row[bg_idx] = -C[row_idx, full_idx]
        end
        row[n_bg + witness_idx] = -C[row_idx, input_idx]
        if row_idx <= nullity
            push!(Aeq_rows, row); push!(beq, C0[row_idx])
            push!(eq_ids, "$prefix:eq:$row_idx")
        else
            push!(Aineq_rows, row); push!(bineq, C0[row_idx])
            push!(ineq_ids, "$prefix:ineq:$(row_idx - nullity)")
        end
    end
    return nothing
end

function _rop_shape_compile_geometry(model, siso, pr, profile, witness_vertices,
                                     projected_symbols, projected_full_indices,
                                     input_idx::Int, parameter_bounds,
                                     window_bounds, window_spacing::Float64,
                                     witness_order::Vector{Int}, network_hash::String)
    n_bg = length(projected_symbols)
    n_witness = length(witness_vertices)
    n_aug = n_bg + n_witness
    Aeq_rows = Vector{Vector{Float64}}()
    beq = Float64[]
    eq_ids = String[]
    Aineq_rows = Vector{Vector{Float64}}()
    bineq = Float64[]
    ineq_ids = String[]

    pC, pC0, pnull = try
        get_C_C0_nullity_qK(siso, Int(pr.path_idx))
    catch
        get_C_C0_nullity(get_polyhedron(siso, Int(pr.path_idx)))
    end
    _rop_shape_append_c_rows!(
        Aeq_rows, beq, eq_ids, Aineq_rows, bineq, ineq_ids,
        pC, pC0, pnull, n_aug, "path:$(Int(pr.path_idx)):projected")
    _rop_shape_append_parameter_bounds!(
        Aineq_rows, bineq, ineq_ids, model, projected_symbols,
        parameter_bounds, n_aug)
    for (witness_idx, vertex_idx) in enumerate(witness_vertices)
        _rop_shape_append_vertex_rows!(
            Aeq_rows, beq, eq_ids, Aineq_rows, bineq, ineq_ids,
            model, Int(vertex_idx), input_idx, projected_full_indices,
            witness_idx, n_witness, Int(pr.path_idx))
        lo, hi = window_bounds
        t_col = n_bg + witness_idx
        upper = zeros(Float64, n_aug); upper[t_col] = 1.0
        lower = zeros(Float64, n_aug); lower[t_col] = -1.0
        push!(Aineq_rows, upper); push!(bineq, hi)
        push!(ineq_ids, "input_window:witness:$(witness_idx - 1):upper")
        push!(Aineq_rows, lower); push!(bineq, -lo)
        push!(ineq_ids, "input_window:witness:$(witness_idx - 1):lower")
    end
    for order_idx in 1:(length(witness_order) - 1)
        previous = witness_order[order_idx]
        following = witness_order[order_idx + 1]
        row = zeros(Float64, n_aug)
        row[n_bg + previous] = 1.0
        row[n_bg + following] = -1.0
        push!(Aineq_rows, row); push!(bineq, -window_spacing)
        push!(ineq_ids,
              "witness_order:$(previous - 1):before:$(following - 1):min_spacing")
    end

    Aeq = _designability_rows_matrix(Aeq_rows, n_aug)
    Aineq = _designability_rows_matrix(Aineq_rows, n_aug)
    parameter_coordinates = Symbol.(projected_symbols)
    witness_coordinates = [_rop_shape_tau(idx) for idx in 0:(n_witness - 1)]
    coordinates = vcat(parameter_coordinates, witness_coordinates)
    witness_identity = ["step:$idx:vertex:$(witness_vertices[idx + 1])"
                        for idx in 0:(n_witness - 1)]
    path_identity = "path:$(Int(pr.path_idx))"
    cell_id_payload = Dict(
        "network_ir_hash" => network_hash,
        "path_idx" => Int(pr.path_idx),
        "witness_vertices" => Int.(witness_vertices),
    )
    cell_id = "sha256:$(_canonical_hash(cell_id_payload))"
    geometry = ROPShapeOptimization.DesignabilityCellGeometry(
        Aeq, beq, Aineq, bineq;
        coordinates=coordinates,
        parameter_coordinates=parameter_coordinates,
        witness_coordinates=witness_coordinates,
        equality_row_ids=eq_ids,
        inequality_row_ids=ineq_ids,
        path_identity=path_identity,
        witness_identity=witness_identity,
    )
    metadata = ROPShapeCellMetadata(
        path_idx=Int(pr.path_idx),
        witness_vertices=Int.(witness_vertices),
        full_path_vertices=Int.(collect(pr.vertex_indices)),
        predicted_profile=Float64.(profile),
        qK_symbols=String.(string.(projected_symbols)),
        cell_id=cell_id,
    )
    return geometry, metadata
end

function _rop_shape_compile_population(rules::Vector{String}, input_sym::String,
                                       output_sym::String, target_profile,
                                       parameter_bounds, input_window,
                                       transition_order, max_paths::Int, max_cells::Int,
                                       network_hash::String;
                                       cancel_check=_no_cancel_check,
                                       regime_candidate_max::Int=
                                           ROP_SHAPE_MAX_JOB_REGIME_CANDIDATES,
                                       materialization_max_paths::Int=
                                           ROP_SHAPE_MAX_MATERIALIZED_PATHS,
                                       materialization_max_total_nodes::Int=
                                           ROP_SHAPE_MAX_MATERIALIZED_PATH_NODES)
    cancel_check()
    model, _, _, _ = build_model(rules, fill(1.0, length(rules)))
    if _in_sync_request_context()
        enforce_sync_model_budget(model)
    else
        model_candidate_bound(
            model;
            maximum=regime_candidate_max,
            label="ROP shape job",
        )
    end
    cancel_check()
    find_all_vertices!(model; cancel_check=cancel_check)
    cancel_check()
    input_idx_raw = locate_sym_qK(model, Symbol(input_sym))
    output_idx_raw = locate_sym_x(model, Symbol(output_sym))
    input_idx_raw === nothing && throw(ArgumentError("unknown input symbol: $input_sym"))
    output_idx_raw === nothing && throw(ArgumentError("unknown output symbol: $output_sym"))
    input_idx = Int(input_idx_raw)
    output_idx = Int(output_idx_raw)
    input_idx <= model.d || throw(ArgumentError("input symbol must be a total"))

    siso = _hard_bounded_siso_paths(
        model, Symbol(input_sym);
        label="ROP shape job",
        cancel_check=cancel_check,
        max_paths=materialization_max_paths,
        max_total_nodes=materialization_max_total_nodes,
    )
    cancel_check()
    behavior = get_behavior_families(
        siso; observe_x=Symbol(output_sym), path_scope=:feasible,
        deduplicate=false, keep_singular=true, keep_nonasymptotic=true,
        compute_volume=false, cancel_check=cancel_check)
    cancel_check()
    projected_symbols = Symbol.(qK_sym(siso))
    projected_full_indices =
        _designability_projected_indices_for_siso(model, siso, input_idx)
    window_bounds = _designability_finite_input_window_tuple(input_window)
    window_bounds === nothing &&
        throw(ArgumentError("a finite behavior_spec.input_window.input_log10 is required"))
    spacing = _designability_window_spacing(input_window)
    spacing === nothing && throw(ArgumentError("invalid finite-window spacing"))
    order = transition_order === nothing ? collect(1:length(target_profile)) :
        Int.(transition_order .+ 1)

    eligible = NamedTuple[]
    eligible_cell_count = 0
    for pr in behavior.path_records
        cancel_check()
        pr.included || continue
        collapsed = _designability_collapse_profile_vertices(
            pr.exact_profile, pr.vertex_indices, target_profile; tol=0.05)
        collapsed === nothing && continue
        profile, choices = collapsed
        count = _rop_shape_bounded_product_count(choices)
        count == 0 && continue
        eligible_cell_count = if eligible_cell_count == typemax(Int) ||
                                 count == typemax(Int) ||
                                 eligible_cell_count > typemax(Int) - count
            typemax(Int)
        else
            eligible_cell_count + count
        end
        push!(eligible, (; pr, profile, choices, count))
    end

    geometries = ROPShapeOptimization.DesignabilityCellGeometry[]
    metadata = ROPShapeCellMetadata[]
    evaluated_paths = Set{Int}()
    stop = false
    evaluated_entries = @view eligible[1:min(length(eligible), max_paths)]
    for entry in evaluated_entries
        cancel_check()
        for tuple in Iterators.product(entry.choices...)
            cancel_check()
            if length(geometries) >= max_cells
                stop = true
                break
            end
            geometry, meta = _rop_shape_compile_geometry(
                model, siso, entry.pr, entry.profile, Int.(collect(tuple)),
                projected_symbols, projected_full_indices, input_idx,
                parameter_bounds, window_bounds, Float64(spacing), order,
                network_hash)
            push!(geometries, geometry)
            push!(metadata, meta)
            push!(evaluated_paths, meta.path_idx)
        end
        stop && break
    end
    path_truncated = length(eligible) > max_paths
    cell_truncated = eligible_cell_count > length(geometries)
    truncated = path_truncated || cell_truncated
    reasons = String[]
    path_truncated && push!(reasons, "max_paths")
    cell_truncated && push!(reasons, "max_cells")
    return ROPShapePopulation(
        cells=geometries,
        metadata=metadata,
        eligible_path_count=length(eligible),
        evaluated_path_count=length(evaluated_paths),
        eligible_cell_count=eligible_cell_count,
        evaluated_cell_count=length(geometries),
        truncated=truncated,
        truncation_reasons=reasons,
        model=model,
        input_idx=input_idx,
        output_idx=output_idx,
        projected_symbols=projected_symbols,
    )
end

function _rop_shape_terms(raw, witness_count::Int, path::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    isempty(raw) && throw(ArgumentError("$path must not be empty"))
    terms = ROPShapeOptimization.WitnessTerm[]
    serialized = Dict{String, Any}[]
    seen = Set{Int}()
    for (idx, item) in enumerate(raw)
        item isa AbstractDict || throw(ArgumentError("$path[$idx] must be an object"))
        _rop_shape_unknown_keys(item, Set(("step", "coefficient")), "$path[$idx]")
        step = _rop_shape_step(
            _raw_get(item, :step, nothing), witness_count, "$path[$idx].step")
        step in seen && throw(ArgumentError("$path must not repeat a step"))
        push!(seen, step)
        coefficient = _rop_shape_finite(
            _raw_get(item, :coefficient, nothing), "$path[$idx].coefficient")
        coefficient != 0 || throw(ArgumentError("$path[$idx].coefficient must be nonzero"))
        push!(terms, ROPShapeOptimization.WitnessTerm(_rop_shape_tau(step), coefficient))
        push!(serialized, Dict("step" => step, "coefficient" => coefficient))
    end
    return terms, serialized
end

function _rop_shape_constraint(id::AbstractString, terms, relation, rhs::Real)
    return ROPShapeOptimization.LinearWitnessConstraint(id, terms, relation, rhs)
end

function _rop_shape_anchor_constraints!(constraints, serialized, id::AbstractString,
                                        terms, serialized_terms, center::Float64,
                                        tolerance::Float64)
    push!(constraints, _rop_shape_constraint("$id:upper", terms, :le, center + tolerance))
    push!(constraints, _rop_shape_constraint("$id:lower", terms, :ge, center - tolerance))
    push!(serialized, Dict{String, Any}(
        "id" => "$id:upper", "kind" => "linear", "terms" => serialized_terms,
        "operator" => "<=", "rhs_log10" => center + tolerance, "hard" => true,
    ))
    push!(serialized, Dict{String, Any}(
        "id" => "$id:lower", "kind" => "linear", "terms" => serialized_terms,
        "operator" => ">=", "rhs_log10" => center - tolerance, "hard" => true,
    ))
    return nothing
end

function _rop_shape_linear_value(terms, points::Vector{Float64})
    value = 0.0
    for term in terms
        name = String(term.coordinate)
        startswith(name, "tau_") || error("non-witness term in compiled objective")
        step = parse(Int, name[5:end])
        value += term.coefficient * points[step + 1]
    end
    return value
end

function _rop_shape_direction_payload(direction::Vector{Float64})
    all(iszero, direction) && return nothing
    return Dict{String, Any}(
        "values" => direction,
        "l2_norm" => norm(direction),
        "normalization" => "not_normalized",
        "alpha_units" => "declared_raw_direction_scale",
    )
end

function _rop_shape_compile_linear_intent(intent, witness_count::Int,
                                          reference_points::Vector{Float64})
    allowed = Set(("id", "kind", "objective", "constraints"))
    _rop_shape_unknown_keys(intent, allowed, "edit_intent")
    objective_raw = _rop_shape_required_object(intent, :objective, "edit_intent")
    _rop_shape_unknown_keys(
        objective_raw, Set(("id", "sense", "terms")), "edit_intent.objective")
    objective_id = _rop_shape_required_string(objective_raw, :id, "edit_intent.objective")
    sense = String(_raw_get(objective_raw, :sense, ""))
    sense in ("maximize", "minimize") || throw(ArgumentError(
        "edit_intent.objective.sense must be maximize or minimize"))
    terms, serialized_terms = _rop_shape_terms(
        _raw_get(objective_raw, :terms, nothing), witness_count,
        "edit_intent.objective.terms")
    objective = ROPShapeOptimization.LinearWitnessObjective(
        objective_id, terms; sense=Symbol(sense))
    constraints = ROPShapeOptimization.LinearWitnessConstraint[]
    serialized_constraints = Dict{String, Any}[]
    constraints_raw = _raw_get(intent, :constraints, Any[])
    constraints_raw isa AbstractVector ||
        throw(ArgumentError("edit_intent.constraints must be an array"))
    used_ids = Set{String}()
    for (idx, raw) in enumerate(constraints_raw)
        raw isa AbstractDict ||
            throw(ArgumentError("edit_intent.constraints[$idx] must be an object"))
        _rop_shape_unknown_keys(
            raw, Set(("id", "terms", "operator", "rhs_log10", "hard")),
            "edit_intent.constraints[$idx]")
        id = _rop_shape_required_string(raw, :id, "edit_intent.constraints[$idx]")
        id in used_ids && throw(ArgumentError("edit_intent constraint IDs must be unique"))
        push!(used_ids, id)
        cterms, cserialized = _rop_shape_terms(
            _raw_get(raw, :terms, nothing), witness_count,
            "edit_intent.constraints[$idx].terms")
        operator = String(_raw_get(raw, :operator, ""))
        _raw_get(raw, :hard, nothing) === true || throw(ArgumentError(
            "edit_intent.constraints[$idx].hard must be literal true"))
        relation = operator == "<=" ? :le : operator == ">=" ? :ge :
                   operator in ("=", "==") ? :eq : nothing
        relation === nothing && throw(ArgumentError(
            "edit_intent.constraints[$idx].operator must be <=, >=, or ="))
        rhs = _rop_shape_finite(
            _raw_get(raw, :rhs_log10, nothing),
            "edit_intent.constraints[$idx].rhs_log10")
        push!(constraints, _rop_shape_constraint(id, cterms, relation, rhs))
        push!(serialized_constraints, Dict{String, Any}(
            "id" => id,
            "kind" => "linear",
            "terms" => cserialized,
            "operator" => operator == "==" ? "=" : operator,
            "rhs_log10" => rhs,
            "hard" => true,
        ))
    end
    baseline = _rop_shape_linear_value(terms, reference_points)
    compiled = Dict{String, Any}(
        "compiler_version" => ROP_SHAPE_COMPILER_VERSION,
        "source_intent_id" => String(_raw_get(intent, :id, "")),
        "intent" => Dict{String, Any}(_materialize(intent)),
        "objective" => Dict{String, Any}(
            "id" => objective_id,
            "kind" => "linear_operating_point",
            "sense" => sense,
            "terms" => serialized_terms,
            "reference_value" => baseline,
        ),
        "constraints" => serialized_constraints,
        "direction" => nothing,
        "index_basis" => "zero_based_program_step",
        "units" => "log10_input",
        "auxiliary_coordinates" => String[],
    )
    return (; objective, constraints, compiled, baseline_effect=baseline,
            direction=nothing,
            effect_kind="linear")
end

function _rop_shape_compile_intent(intent::AbstractDict, witness_count::Int,
                                   reference_points::Vector{Float64})
    kind = _rop_shape_required_string(intent, :kind, "edit_intent")
    id = _rop_shape_required_string(intent, :id, "edit_intent")
    kind == "linear_witness" &&
        return _rop_shape_compile_linear_intent(intent, witness_count, reference_points)

    constraints = ROPShapeOptimization.LinearWitnessConstraint[]
    serialized_constraints = Dict{String, Any}[]
    direction = zeros(Float64, witness_count)
    objective = nothing
    serialized_objective = Dict{String, Any}()
    baseline = 0.0
    effect_kind = "linear"

    if kind == "separate"
        _rop_shape_unknown_keys(
            intent, Set(("id", "kind", "steps", "preserve_midpoint_tolerance_log10")),
            "edit_intent")
        steps = _rop_shape_steps(
            _raw_get(intent, :steps, nothing), witness_count, "edit_intent.steps";
            exact_length=2)
        left, right = steps
        left < right || throw(ArgumentError("edit_intent.steps must be in left-to-right order"))
        terms = [
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(right), 1.0),
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(left), -1.0),
        ]
        objective = ROPShapeOptimization.LinearWitnessObjective(id, terms; sense=:maximize)
        baseline = reference_points[right + 1] - reference_points[left + 1]
        direction[left + 1] = -0.5
        direction[right + 1] = 0.5
        tolerance = _rop_shape_finite(
            _raw_get(intent, :preserve_midpoint_tolerance_log10, 0.0),
            "edit_intent.preserve_midpoint_tolerance_log10"; minimum=0.0)
        anchor_terms = [
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(left), 0.5),
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(right), 0.5),
        ]
        anchor_serialized = [
            Dict("step" => left, "coefficient" => 0.5),
            Dict("step" => right, "coefficient" => 0.5),
        ]
        center = (reference_points[left + 1] + reference_points[right + 1]) / 2
        _rop_shape_anchor_constraints!(
            constraints, serialized_constraints, "$id:midpoint_anchor",
            anchor_terms, anchor_serialized, center, tolerance)
        serialized_objective = Dict(
            "id" => id, "kind" => "linear_operating_point", "sense" => "maximize",
            "terms" => [Dict("step" => right, "coefficient" => 1.0),
                         Dict("step" => left, "coefficient" => -1.0)],
            "reference_value" => baseline,
        )
    elseif kind == "widen_center"
        _rop_shape_unknown_keys(
            intent, Set(("id", "kind", "steps", "anchor_step", "anchor_tolerance_log10")),
            "edit_intent")
        steps = _rop_shape_steps(
            _raw_get(intent, :steps, nothing), witness_count, "edit_intent.steps";
            exact_length=2)
        left, right = steps
        left < right || throw(ArgumentError("edit_intent.steps must be in left-to-right order"))
        anchor_step = _rop_shape_step(
            _raw_get(intent, :anchor_step, nothing), witness_count,
            "edit_intent.anchor_step")
        anchor_step in steps &&
            throw(ArgumentError("edit_intent.anchor_step must differ from the gap endpoints"))
        terms = [
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(right), 1.0),
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(left), -1.0),
        ]
        objective = ROPShapeOptimization.LinearWitnessObjective(id, terms; sense=:maximize)
        baseline = reference_points[right + 1] - reference_points[left + 1]
        direction[left + 1] = -0.5
        direction[right + 1] = 0.5
        tolerance = _rop_shape_finite(
            _raw_get(intent, :anchor_tolerance_log10, 0.0),
            "edit_intent.anchor_tolerance_log10"; minimum=0.0)
        anchor_terms = [ROPShapeOptimization.WitnessTerm(_rop_shape_tau(anchor_step), 1.0)]
        anchor_serialized = [Dict("step" => anchor_step, "coefficient" => 1.0)]
        _rop_shape_anchor_constraints!(
            constraints, serialized_constraints, "$id:center_anchor",
            anchor_terms, anchor_serialized, reference_points[anchor_step + 1], tolerance)
        serialized_objective = Dict(
            "id" => id, "kind" => "linear_operating_point", "sense" => "maximize",
            "terms" => [Dict("step" => right, "coefficient" => 1.0),
                         Dict("step" => left, "coefficient" => -1.0)],
            "reference_value" => baseline,
        )
    elseif kind == "translate_group"
        _rop_shape_unknown_keys(
            intent, Set(("id", "kind", "group_steps", "preserve_steps",
                         "preserve_tolerance_log10", "sense", "shared_shift")),
            "edit_intent")
        _raw_get(intent, :shared_shift, nothing) === true || throw(ArgumentError(
            "edit_intent.shared_shift must be literal true"))
        group = _rop_shape_steps(
            _raw_get(intent, :group_steps, nothing), witness_count,
            "edit_intent.group_steps"; minimum_length=1)
        preserve = _rop_shape_steps(
            _raw_get(intent, :preserve_steps, nothing), witness_count,
            "edit_intent.preserve_steps"; minimum_length=1)
        isempty(intersect(Set(group), Set(preserve))) || throw(ArgumentError(
            "edit_intent.group_steps and preserve_steps must be disjoint"))
        sense = String(_raw_get(intent, :sense, ""))
        sense in ("positive", "negative") || throw(ArgumentError(
            "edit_intent.sense must be positive or negative"))
        scale = 1.0 / length(group)
        terms = [ROPShapeOptimization.WitnessTerm(_rop_shape_tau(step), scale)
                 for step in group]
        objective_sense = sense == "positive" ? :maximize : :minimize
        objective = ROPShapeOptimization.LinearWitnessObjective(
            id, terms; sense=objective_sense)
        baseline = sum(reference_points[step + 1] for step in group) / length(group)
        direction[group .+ 1] .= sense == "positive" ? 1.0 : -1.0
        first_step = first(group)
        for step in Iterators.drop(group, 1)
            row_terms = [
                ROPShapeOptimization.WitnessTerm(_rop_shape_tau(step), 1.0),
                ROPShapeOptimization.WitnessTerm(_rop_shape_tau(first_step), -1.0),
            ]
            rhs = reference_points[step + 1] - reference_points[first_step + 1]
            cid = "$id:shared_shift:$step"
            push!(constraints, _rop_shape_constraint(cid, row_terms, :eq, rhs))
            push!(serialized_constraints, Dict(
                "id" => cid, "kind" => "linear",
                "terms" => [Dict("step" => step, "coefficient" => 1.0),
                             Dict("step" => first_step, "coefficient" => -1.0)],
                "operator" => "=", "rhs_log10" => rhs,
                "hard" => true,
            ))
        end
        tolerance = _rop_shape_finite(
            _raw_get(intent, :preserve_tolerance_log10, 0.0),
            "edit_intent.preserve_tolerance_log10"; minimum=0.0)
        for step in preserve
            anchor_terms = [ROPShapeOptimization.WitnessTerm(_rop_shape_tau(step), 1.0)]
            anchor_serialized = [Dict("step" => step, "coefficient" => 1.0)]
            _rop_shape_anchor_constraints!(
                constraints, serialized_constraints, "$id:preserve:$step",
                anchor_terms, anchor_serialized, reference_points[step + 1], tolerance)
        end
        serialized_objective = Dict(
            "id" => id, "kind" => "linear_operating_point",
            "sense" => String(objective_sense),
            "terms" => [Dict("step" => step, "coefficient" => scale) for step in group],
            "reference_value" => baseline,
        )
    elseif kind == "broaden"
        _rop_shape_unknown_keys(
            intent, Set(("id", "kind", "left_span_steps", "right_span_steps",
                         "shared_magnitude")),
            "edit_intent")
        _raw_get(intent, :shared_magnitude, nothing) === true || throw(ArgumentError(
            "edit_intent.shared_magnitude must be literal true"))
        left = _rop_shape_steps(
            _raw_get(intent, :left_span_steps, nothing), witness_count,
            "edit_intent.left_span_steps"; exact_length=2)
        right = _rop_shape_steps(
            _raw_get(intent, :right_span_steps, nothing), witness_count,
            "edit_intent.right_span_steps"; exact_length=2)
        left[1] < left[2] && right[1] < right[2] || throw(ArgumentError(
            "broaden span steps must be in left-to-right order"))
        isempty(intersect(Set(left), Set(right))) || throw(ArgumentError(
            "broaden left and right spans must be disjoint"))
        isdefined(ROPShapeOptimization, :BalancedMaxMinWitnessObjective) ||
            throw(ArgumentError("balanced broaden objective is unavailable in this build"))
        objective_constructor = getfield(
            ROPShapeOptimization, :BalancedMaxMinWitnessObjective)
        improvement_constructor = getfield(
            ROPShapeOptimization, :WitnessImprovement)
        left_terms = [
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(left[2]), 1.0),
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(left[1]), -1.0),
        ]
        right_terms = [
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(right[2]), 1.0),
            ROPShapeOptimization.WitnessTerm(_rop_shape_tau(right[1]), -1.0),
        ]
        baselines = [
            reference_points[left[2] + 1] - reference_points[left[1] + 1],
            reference_points[right[2] + 1] - reference_points[right[1] + 1],
        ]
        objective = objective_constructor(id, [
            improvement_constructor("$id:left", left_terms, baselines[1]),
            improvement_constructor("$id:right", right_terms, baselines[2]),
        ])
        baseline = 0.0
        effect_kind = "balanced_minimum_improvement"
        direction[left[1] + 1] = -0.5
        direction[left[2] + 1] = 0.5
        direction[right[1] + 1] = -0.5
        direction[right[2] + 1] = 0.5
        serialized_objective = Dict(
            "id" => id,
            "kind" => "max_min_linear_operating_point_improvement",
            "sense" => "maximize",
            "groups" => [
                Dict("terms" => [Dict("step" => left[2], "coefficient" => 1.0),
                                  Dict("step" => left[1], "coefficient" => -1.0)],
                     "reference_value" => baselines[1]),
                Dict("terms" => [Dict("step" => right[2], "coefficient" => 1.0),
                                  Dict("step" => right[1], "coefficient" => -1.0)],
                     "reference_value" => baselines[2]),
            ],
            "reference_value" => 0.0,
        )
    else
        throw(ArgumentError(
            "edit_intent.kind must be broaden, separate, widen_center, " *
            "translate_group, or linear_witness"))
    end

    compiled = Dict{String, Any}(
        "compiler_version" => ROP_SHAPE_COMPILER_VERSION,
        "source_intent_id" => id,
        "intent" => Dict{String, Any}(_materialize(intent)),
        "objective" => serialized_objective,
        "constraints" => serialized_constraints,
        "direction" => _rop_shape_direction_payload(direction),
        "index_basis" => "zero_based_program_step",
        "units" => "log10_input",
        "auxiliary_coordinates" =>
            effect_kind == "balanced_minimum_improvement" ? ["alpha"] : String[],
    )
    return (; objective, constraints, compiled, baseline_effect=baseline,
            direction, effect_kind)
end

function _rop_shape_normalize_work_budget(raw; synchronous::Bool)
    raw isa AbstractDict || throw(ArgumentError("work_budget must be an object"))
    _rop_shape_unknown_keys(
        raw, Set(("max_paths", "max_cells", "max_replays", "require_exhaustive")),
        "work_budget")
    max_cell_cap = synchronous ? ROP_SHAPE_MAX_SYNC_CELLS : ROP_SHAPE_MAX_JOB_CELLS
    max_replay_cap = synchronous ? ROP_SHAPE_MAX_SYNC_REPLAYS : ROP_SHAPE_MAX_JOB_REPLAYS
    raw_cells = _raw_get(raw, :max_cells, ROP_SHAPE_MAX_SYNC_CELLS)
    raw_replays = _raw_get(raw, :max_replays, 1)
    raw_paths = _raw_get(raw, :max_paths, 2000)
    max_paths = _rop_shape_int(
        raw_paths, "work_budget.max_paths"; minimum=1, maximum=2000)
    if synchronous
        max_cells = sync_bounded_int(
            raw_cells, "work_budget.max_cells"; min=1, max=max_cell_cap)
        max_replays = sync_bounded_int(
            raw_replays, "work_budget.max_replays"; min=1, max=max_replay_cap)
    else
        max_cells = _rop_shape_int(
            raw_cells, "work_budget.max_cells"; minimum=1, maximum=max_cell_cap)
        max_replays = _rop_shape_int(
            raw_replays, "work_budget.max_replays"; minimum=1, maximum=max_replay_cap)
    end
    require_exhaustive = _raw_get(raw, :require_exhaustive, false)
    require_exhaustive isa Bool ||
        throw(ArgumentError("work_budget.require_exhaustive must be true or false"))
    return Dict{String, Any}(
        "max_paths" => max_paths,
        "max_cells" => max_cells,
        "max_replays" => max_replays,
        "require_exhaustive" => require_exhaustive,
    )
end

function _rop_shape_normalize_replay(raw, default_window)
    raw isa AbstractDict || throw(ArgumentError("replay must be an object"))
    _rop_shape_unknown_keys(
        raw, Set(("input_window_log10", "sample_points", "require_complete",
                  "store_curve", "metrics")),
        "replay")
    window = _rop_shape_finite_vector(
        _raw_get(raw, :input_window_log10, collect(default_window)),
        "replay.input_window_log10"; length_required=2)
    window[1] < window[2] ||
        throw(ArgumentError("replay.input_window_log10 must be strictly increasing"))
    all(abs(value) <= 20.0 for value in window) ||
        throw(ArgumentError("replay.input_window_log10 must lie within [-20, 20]"))
    sample_points = _rop_shape_int(
        _raw_get(raw, :sample_points, 281), "replay.sample_points";
        minimum=11, maximum=1000)
    require_complete = _raw_get(raw, :require_complete, true)
    require_complete === true || throw(ArgumentError(
        "replay.require_complete must be literal true for finite-shape verification"))
    _raw_get(raw, :store_curve, nothing) === true || throw(ArgumentError(
        "replay.store_curve must be literal true for durable replay evidence"))
    metrics_raw = _raw_get(raw, :metrics, Any[])
    metrics_raw isa AbstractVector || throw(ArgumentError("replay.metrics must be an array"))
    length(metrics_raw) == 1 || throw(ArgumentError(
        "replay.metrics must contain exactly one supported two_peak metric"))
    metric_raw = first(metrics_raw)
    metric_raw isa AbstractDict || throw(ArgumentError("replay.metrics[1] must be an object"))
    _rop_shape_unknown_keys(
        metric_raw, Set(("kind", "min_prominence_log10")), "replay.metrics[1]")
    kind = _rop_shape_required_string(metric_raw, :kind, "replay.metrics[1]")
    kind == "two_peak" ||
        throw(ArgumentError("replay.metrics[1].kind must be two_peak"))
    prominence = _rop_shape_finite(
        _raw_get(metric_raw, :min_prominence_log10, 0.0),
        "replay.metrics[1].min_prominence_log10"; minimum=0.0)
    return Dict{String, Any}(
        "input_window_log10" => window,
        "sample_points" => sample_points,
        "require_complete" => true,
        "store_curve" => true,
        "metrics" => [Dict{String, Any}(
            "kind" => "two_peak",
            "min_prominence_log10" => prominence,
        )],
    )
end

function _rop_shape_normalize_request(raw; synchronous::Bool)
    raw isa AbstractDict || throw(ArgumentError("ROP shape request must be an object"))
    _rop_shape_unknown_keys(raw, _ROP_SHAPE_REQUEST_KEYS, "request")
    version = _rop_shape_required_string(raw, :schema_version, "request")
    version == ROP_SHAPE_OPTIMIZE_REQUEST_VERSION || throw(ArgumentError(
        "unsupported ROP shape request schema_version: $version"))

    network_raw = _raw_get(raw, :network, nothing)
    network = parse_network_ir(network_raw)
    network_dict = network_ir_to_dict(network)
    network_hash = network_ir_hash(network)
    expected_raw = _raw_get(raw, :expected_network_ir_hash, nothing)
    legacy_network = is_legacy_network_payload(network_raw)
    expected_hash = if expected_raw === nothing && legacy_network
        network_hash
    else
        _rop_shape_sha256(expected_raw, "expected_network_ir_hash")
    end
    expected_hash == network_hash || throw(ArgumentError(
        "expected_network_ir_hash does not match the server-normalized NetworkIR"))
    bridge = network_ir_to_legacy_inputs(network)
    rules = String.(bridge.rules)
    isempty(rules) && throw(ArgumentError("network must contain at least one reaction"))
    synchronous && enforce_sync_rule_budget(rules)

    raw_spec = _raw_get(raw, :designability_spec, nothing)
    spec = normalize_designability_spec(raw_spec)
    designability_has_unsupported_hard_clause(spec) && throw(ArgumentError(
        "designability_spec contains unsupported hard clauses"))
    target_program = _design_exact_ro_program(
        spec.legacy_target_kind, spec.legacy_target)
    target_program === nothing && throw(ArgumentError(
        "designability_spec must lower to an exact reaction-order program"))
    input_sym = spec.required_input
    output_sym = spec.required_output
    input_sym === nothing && throw(ArgumentError(
        "designability_spec.target.behavior_spec.input is required"))
    output_sym === nothing && throw(ArgumentError(
        "designability_spec.target.behavior_spec.output is required"))
    input_window = _design_effective_behavior_input_window(spec)
    input_window === nothing && throw(ArgumentError(
        "designability_spec requires a finite behavior_spec.input_window"))
    _raw_haskey(input_window, :operating_points_log10) && throw(ArgumentError(
        "shape optimization requires free witnesses; remove operating_points_log10 from the spec"))
    window_bounds = _designability_finite_input_window_tuple(input_window)
    window_bounds === nothing && throw(ArgumentError(
        "designability_spec input window must contain finite input_log10 bounds"))
    transition_order = _design_supported_transition_order_program_indices(spec)
    parameter_bounds = _design_effective_parameter_bounds(spec)
    robustness = _raw_get(spec.constraints, :robustness, nothing)
    if robustness isa AbstractDict && _raw_haskey(robustness, :min_chebyshev_radius)
        throw(ArgumentError(
            "ambiguous min_chebyshev_radius is not accepted by shape optimization; " *
            "use minimum_parameter_margin"))
    end

    reference_raw = _raw_get(raw, :reference, nothing)
    reference = _rop_shape_normalize_reference(
        reference_raw, length(target_program), length(rules))
    reference["network_ir_hash"] == network_hash || throw(ArgumentError(
        "reference.network_ir_hash does not match the fixed NetworkIR"))
    points = Float64.(reference["operating_points_log10"])
    lo, hi = window_bounds
    all(point -> lo <= point <= hi, points) || throw(ArgumentError(
        "reference operating points must lie inside the design input window"))
    spacing = _designability_window_spacing(input_window)
    order = transition_order === nothing ? collect(0:(length(points) - 1)) : transition_order
    for idx in 1:(length(order) - 1)
        points[order[idx + 1] + 1] - points[order[idx] + 1] + 1e-9 >= spacing ||
            throw(ArgumentError(
                "reference operating points violate the declared witness order or spacing"))
    end

    intent_raw = _raw_get(raw, :edit_intent, nothing)
    intent_raw isa AbstractDict || throw(ArgumentError("edit_intent must be an object"))
    intent = _rop_shape_compile_intent(
        Dict{String, Any}(_materialize(intent_raw)), length(target_program), points)
    optimization_raw = _rop_shape_required_object(raw, :optimization, "request")
    _rop_shape_unknown_keys(
        optimization_raw, Set(("minimum_parameter_margin", "effect_tolerance")),
        "optimization")
    minimum_parameter_margin = _rop_shape_finite(
        _raw_get(optimization_raw, :minimum_parameter_margin, nothing),
        "optimization.minimum_parameter_margin"; minimum=0.0)
    effect_tolerance = _rop_shape_finite(
        _raw_get(optimization_raw, :effect_tolerance, nothing),
        "optimization.effect_tolerance"; minimum=0.0)
    work_budget = _rop_shape_normalize_work_budget(
        _raw_get(raw, :work_budget, nothing); synchronous=synchronous)
    replay = _rop_shape_normalize_replay(
        _raw_get(raw, :replay, nothing), window_bounds)

    normalized = Dict{String, Any}(
        "schema_version" => ROP_SHAPE_OPTIMIZE_REQUEST_VERSION,
        "network" => network_dict,
        "expected_network_ir_hash" => network_hash,
        "designability_spec" => designability_spec_to_dict(spec),
        "reference" => reference,
        "edit_intent" => Dict{String, Any}(_materialize(intent_raw)),
        "optimization" => Dict{String, Any}(
            "minimum_parameter_margin" => minimum_parameter_margin,
            "effect_tolerance" => effect_tolerance,
        ),
        "work_budget" => work_budget,
        "replay" => replay,
    )
    return (; normalized, network, network_hash,
            network_code=network_canonical_code(network), rules,
            input_sym=String(input_sym), output_sym=String(output_sym),
            target_program, input_window, transition_order, parameter_bounds,
            reference, intent, minimum_parameter_margin, effect_tolerance,
            work_budget, replay)
end

function _rop_shape_active_row_dict(row)
    out = Dict{String, Any}(
        "row_id" => row.row_id,
        "row_kind" => String(row.row_kind),
        "point_residual" => row.point_residual,
        "ball_residual" => row.ball_residual,
        "normalized_residual" => row.normalized_residual,
    )
    if hasfield(typeof(row), :dual)
        out["dual"] = row.dual
    end
    if hasfield(typeof(row), :shadow_price)
        out["shadow_price"] = row.shadow_price
        out["shadow_price_semantics"] =
            "derivative_of_objective_value_with_respect_to_compiled_rhs"
    end
    return out
end

function _rop_shape_matrix_rows(matrix::AbstractMatrix)
    return [Float64.(collect(row)) for row in eachrow(matrix)]
end

function _rop_shape_improvement(effect::Float64, baseline::Float64, intent)
    intent.effect_kind == "balanced_minimum_improvement" && return effect
    objective = intent.objective
    sense = hasfield(typeof(objective), :sense) ? objective.sense : :maximize
    return sense == :minimize ? baseline - effect : effect - baseline
end

function _rop_shape_directional_union(population::ROPShapePopulation, intent,
                                      reference_points::Vector{Float64};
                                      cancel_check=_no_cancel_check)
    intent.direction === nothing && return nothing
    intervals = Dict{String, Any}[]
    numeric_intervals = Tuple{Float64, Float64}[]
    numerical_error_count = 0
    for (cell, meta) in zip(population.cells, population.metadata)
        cancel_check()
        interval = ROPShapeOptimization.directional_request_interval(
            cell, reference_points, intent.direction;
            constraints=intent.constraints)
        lower = interval.lower_status == ROPShapeOptimization.UNBOUNDED ? -Inf :
            interval.alpha_min
        upper = interval.upper_status == ROPShapeOptimization.UNBOUNDED ? Inf :
            interval.alpha_max
        if lower !== nothing && upper !== nothing
            push!(numeric_intervals, (Float64(lower), Float64(upper)))
        end
        interval.status == ROPShapeOptimization.NUMERICAL_ERROR &&
            (numerical_error_count += 1)
        push!(intervals, Dict{String, Any}(
            "cell_id" => meta.cell_id,
            "path_identity" => interval.path_identity,
            "status" => ROPShapeOptimization.status_name(interval.status),
            "lower_status" => ROPShapeOptimization.status_name(interval.lower_status),
            "upper_status" => ROPShapeOptimization.status_name(interval.upper_status),
            "alpha_min" => interval.alpha_min,
            "alpha_max" => interval.alpha_max,
            "message" => interval.message,
        ))
    end
    sort!(numeric_intervals; by=first)
    merged = Tuple{Float64, Float64}[]
    for interval in numeric_intervals
        if isempty(merged) || interval[1] > last(merged)[2] + 1e-8
            push!(merged, interval)
        else
            previous = pop!(merged)
            push!(merged, (previous[1], max(previous[2], interval[2])))
        end
    end
    union_intervals = [Dict{String, Any}(
        "alpha_min" => isfinite(lo) ? lo : nothing,
        "alpha_max" => isfinite(hi) ? hi : nothing,
        "lower_unbounded" => !isfinite(lo),
        "upper_unbounded" => !isfinite(hi),
    ) for (lo, hi) in merged]
    return Dict{String, Any}(
        "direction" => Float64.(intent.direction),
        "direction_l2_norm" => norm(intent.direction),
        "normalization" => "not_normalized",
        "alpha_units" => "declared_raw_direction_scale",
        "cell_intervals" => intervals,
        "union_intervals" => union_intervals,
        "complete_over_evaluated_cells" => numerical_error_count == 0,
        "numerical_error_count" => numerical_error_count,
        "scope" => population.truncated ? "evaluated_cells" : "declared_cells",
    )
end

function _rop_shape_geometric_status(status, truncated::Bool)
    if status == ROPShapeOptimization.OPTIMAL
        return truncated ? "best_over_evaluated_cells" :
            "global_optimal_over_declared_cells"
    elseif status == ROPShapeOptimization.INFEASIBLE
        return truncated ? "infeasible_over_evaluated_cells" :
            "infeasible_over_declared_cells"
    elseif status == ROPShapeOptimization.UNBOUNDED
        return truncated ? "unbounded_over_evaluated_cells" :
            "unbounded_over_declared_cells"
    end
    return "numerical_error"
end

function _rop_shape_selected_payload(population::ROPShapePopulation, union_result,
                                     selected_index::Int, intent)
    cell_result = union_result.cell_results[selected_index]
    margin = cell_result.margin
    margin === nothing && return nothing
    margin.status == ROPShapeOptimization.OPTIMAL || return nothing
    solution = margin.solution
    solution === nothing && return nothing
    geometry = population.cells[selected_index]
    metadata = population.metadata[selected_index]
    values = Float64.(solution)
    coordinate_values = Dict(
        String(coordinate) => values[idx]
        for (idx, coordinate) in enumerate(geometry.coordinates)
    )
    background = [coordinate_values[String(symbol)]
                  for symbol in geometry.parameter_coordinates]
    witnesses = [coordinate_values[String(symbol)]
                 for symbol in geometry.witness_coordinates]
    kd, totals = _designability_split_projected_log_qK(
        population.model, population.projected_symbols, background)
    subspace = margin.subspace
    selected_effect = Float64(something(margin.effect))
    primary_effect = Float64(something(cell_result.primary.effect))
    global_effect = Float64(something(union_result.global_effect))
    active_rows = [_rop_shape_active_row_dict(row) for row in margin.active_rows]
    objective_id = String(getfield(intent.objective, :id))
    objective_sense = hasfield(typeof(intent.objective), :sense) ?
        String(intent.objective.sense) : "maximize"
    return Dict{String, Any}(
        "cell_id" => metadata.cell_id,
        "path_identity" => cell_result.path_identity,
        "path_idx" => metadata.path_idx,
        "witness_identity" => cell_result.witness_identity,
        "witness_vertex_indices" => metadata.witness_vertices,
        "full_path_vertex_indices" => metadata.full_path_vertices,
        "predicted_profile" => metadata.predicted_profile,
        "witness_input_log10" => witnesses,
        "background_log_qK" => Dict{String, Any}(
            "symbols" => metadata.qK_symbols,
            "values" => background,
        ),
        "kd" => Float64.(kd),
        "totals" => Dict{String, Float64}(String(key) => Float64(value)
                                           for (key, value) in pairs(totals)),
        "primary_effect" => Dict{String, Any}(
            "objective_id" => objective_id,
            "sense" => objective_sense,
            "value" => selected_effect,
            "effect_bound" => margin.effect_bound,
            "semantics" => "closed_polyhedral_support_limit_and_secondary_realization",
            "effect_kind" => intent.effect_kind,
            "reference_value" => intent.baseline_effect,
            "closure_support_value" => global_effect,
            "cell_primary_value" => primary_effect,
            "selected_value" => selected_effect,
            "closure_support_improvement" => _rop_shape_improvement(
                global_effect, intent.baseline_effect, intent),
            "selected_improvement" => _rop_shape_improvement(
                selected_effect, intent.baseline_effect, intent),
            "effect_tolerance" => union_result.effect_tolerance,
        ),
        "parameter_margin" => Dict{String, Any}(
            "value" => Float64(something(margin.parameter_margin)),
            "basis" => "equality_feasible_log10_qK_subspace",
            "coordinate_basis" => "unweighted_euclidean_log10_qK",
            "coordinates" => String.(subspace.coordinates),
            "dimension" => subspace.dimension,
            "equality_rank" => subspace.equality_rank,
            "basis_matrix" => _rop_shape_matrix_rows(subspace.basis),
            "rank_relative_tolerance" => subspace.rank_relative_tolerance,
            "rank_absolute_threshold" => subspace.rank_absolute_threshold,
            "zero_dimensional_convention" =>
                subspace.dimension == 0 ? "radius_zero" : "not_applicable",
        ),
        "active_constraints" => active_rows,
        "solver" => Dict{String, Any}(
            "name" => "Clarabel",
            "version" => string(Base.pkgversion(ROPShapeOptimization.Clarabel)),
            "termination_status" => margin.solver_status,
            "validation_tolerance" => 1.0e-7,
            "active_tolerance" => 1.0e-7,
            "rank_tolerance" => sqrt(eps(Float64)),
            "primary_termination_status" => cell_result.primary.solver_status,
            "primary_message" => cell_result.primary.message,
            "secondary_message" => margin.message,
            "core_status" => ROPShapeOptimization.status_name(cell_result.status),
        ),
    )
end

function _rop_shape_replay_selected(
    selected,
    normalized,
    replay_policy;
    execution_policy::Symbol=:asynchronous,
    cancel_check=_no_cancel_check,
)
    selected === nothing && return nothing
    cancel_check()
    window = Float64.(replay_policy["input_window_log10"])
    request_body = Dict{String, Any}(
        "rules" => [String(rx.formula) for rx in normalized.network.reactions],
        "input_sym" => normalized.input_sym,
        "output_sym" => normalized.output_sym,
        "kd" => Float64.(selected["kd"]),
        "totals" => selected["totals"],
        "param_min" => window[1],
        "param_max" => window[2],
        "n_points" => Int(replay_policy["sample_points"]),
    )
    request = Dict{String, Any}(
        "endpoint" => "/api/v1/placer_curve",
        "method" => "POST",
        "body" => request_body,
    )
    request_hash = _canonical_hash(request)
    totals = Dict{Symbol, Float64}(
        Symbol(String(key)) => Float64(value)
        for (key, value) in pairs(selected["totals"])
    )
    curve = placer_dose_response(
        request_body["rules"], Float64.(request_body["kd"]), totals,
        Symbol(normalized.input_sym), normalized.output_sym;
        param_min=window[1], param_max=window[2],
        n_points=Int(replay_policy["sample_points"]),
        execution_policy=execution_policy,
        cancel_check=cancel_check,
    )
    cancel_check()
    trajectory = curve["output_traj"]
    ys = Float64[Float64(row[1]) for row in trajectory]
    metric_request = first(replay_policy["metrics"])
    metrics = analyze_two_peak_curve(
        Float64.(curve["param_values"]), ys, curve["valid"];
        min_prominence_log10=Float64(metric_request["min_prominence_log10"]),
    )
    complete = curve["partial"] === false &&
        all(value -> value === true, curve["valid"]) &&
        get(metrics, "complete", false) === true
    passed = complete && get(metrics, "pass", false) === true
    curve_payload = Dict{String, Any}(
        "param_values" => curve["param_values"],
        "output_traj" => curve["output_traj"],
        "valid" => curve["valid"],
        "partial" => curve["partial"],
    )
    payload = Dict{String, Any}(
        "status" => passed ? "pass" : (complete ? "failed" : "partial"),
        "request" => request,
        "request_hash" => request_hash,
        "curve" => curve_payload,
        "metrics" => metrics,
        "complete" => complete,
        "pass" => passed,
    )
    payload["result_hash"] = _canonical_hash(payload)
    return payload
end

function _rop_shape_replay_not_run()
    return Dict{String, Any}(
        "status" => "not_run",
        "request" => nothing,
        "request_hash" => nothing,
        "curve" => nothing,
        "metrics" => nothing,
        "result_hash" => nothing,
        "complete" => false,
        "pass" => false,
    )
end

function _rop_shape_replay_numerical_error(message::AbstractString)
    payload = Dict{String, Any}(
        "status" => "numerical_error",
        "request" => nothing,
        "request_hash" => nothing,
        "curve" => nothing,
        "metrics" => nothing,
        "complete" => false,
        "pass" => false,
    )
    payload["result_hash"] = _canonical_hash(Dict(
        "status" => "numerical_error", "message" => String(message)))
    return payload
end

function _rop_shape_coverage(population::ROPShapePopulation, union_result,
                             replay_candidate_count::Int, replayed_count::Int)
    feasible_cells = union_result === nothing ? 0 : count(
        result -> result.primary.status == ROPShapeOptimization.OPTIMAL,
        union_result.cell_results)
    return Dict{String, Any}(
        "eligible_path_count" => population.eligible_path_count,
        "evaluated_path_count" => population.evaluated_path_count,
        "eligible_cell_count" => population.eligible_cell_count,
        "evaluated_cell_count" => population.evaluated_cell_count,
        "feasible_cell_count" => feasible_cells,
        "replay_candidate_count" => replay_candidate_count,
        "replayed_count" => replayed_count,
        "truncated" => population.truncated,
        "truncation_reasons" => population.truncation_reasons,
    )
end

"""
    optimize_rop_shape_request(raw; synchronous=false,
                               cancel_check=_no_cancel_check)

Execute one stateless fixed-topology ROP shape request. `synchronous=true`
enforces the interactive 256-cell / two-replay caps; asynchronous callers keep
their own finite declared budget and must pass a cooperative cancellation
checkpoint.
"""
function optimize_rop_shape_request(raw; synchronous::Bool=false,
                                    cancel_check=_no_cancel_check)
    normalized = _rop_shape_normalize_request(raw; synchronous=synchronous)
    request_hash = _canonical_hash(normalized.normalized)
    cancel_check()
    population = _rop_shape_compile_population(
        normalized.rules, normalized.input_sym, normalized.output_sym,
        normalized.target_program, normalized.parameter_bounds,
        normalized.input_window, normalized.transition_order,
        Int(normalized.work_budget["max_paths"]),
        Int(normalized.work_budget["max_cells"]), normalized.network_hash;
        cancel_check=cancel_check)
    if population.truncated && normalized.work_budget["require_exhaustive"] === true
        message = "require_exhaustive=true but the declared max_paths/max_cells " *
                  "budget omitted eligible path cells"
        synchronous ? _sync_budget_exceeded(message) : throw(ArgumentError(message))
    end
    cancel_check()

    union_result = isempty(population.cells) ? nothing :
        ROPShapeOptimization.optimize_cell_union(
            population.cells, normalized.intent.objective;
            constraints=normalized.intent.constraints,
            effect_tolerance=normalized.effect_tolerance,
            minimum_parameter_margin=normalized.minimum_parameter_margin,
            cancel_check=cancel_check,
        )
    core_status = union_result === nothing ? ROPShapeOptimization.INFEASIBLE :
        union_result.status
    geometric_status = _rop_shape_geometric_status(core_status, population.truncated)
    selected = if union_result !== nothing &&
                  union_result.status == ROPShapeOptimization.OPTIMAL &&
                  union_result.selected_cell_index !== nothing
        _rop_shape_selected_payload(
            population, union_result, Int(union_result.selected_cell_index),
            normalized.intent)
    else
        nothing
    end
    directional = isempty(population.cells) ? nothing :
        _rop_shape_directional_union(
            population, normalized.intent,
            Float64.(normalized.reference["operating_points_log10"]);
            cancel_check=cancel_check)
    cancel_check()
    replay_candidate_count = selected === nothing ? 0 : 1
    replay = if selected === nothing || Int(normalized.work_budget["max_replays"]) < 1
        _rop_shape_replay_not_run()
    else
        try
            _rop_shape_replay_selected(
                selected, normalized, normalized.replay;
                execution_policy=synchronous ? :synchronous : :asynchronous,
                cancel_check=cancel_check)
        catch error
            (error isa SyncBudgetExceeded ||
             error isa ModelCandidateBoundExceeded ||
             error isa JobWorkBoundExceeded ||
             error isa LocalJobCancelled) && rethrow()
            _rop_shape_replay_numerical_error(sprint(showerror, error))
        end
    end
    replayed_count = replay["status"] == "not_run" ? 0 : 1
    coverage = _rop_shape_coverage(
        population, union_result, replay_candidate_count, replayed_count)
    warnings = String[]
    population.truncated && push!(warnings,
        "Optimization is best over evaluated cells; omitted cells remain unknown.")
    replay["status"] != "not_run" && replay["pass"] !== true && push!(warnings,
        "The exact-path geometric result did not pass complete sampled finite replay.")
    core_status == ROPShapeOptimization.NUMERICAL_ERROR && push!(warnings,
        "At least one evaluated LP had an unresolved numerical status.")
    result = Dict{String, Any}(
        "schema_version" => ROP_SHAPE_OPTIMIZATION_VERSION,
        "request_hash" => request_hash,
        "normalized_request" => normalized.normalized,
        "fixed_topology" => Dict{String, Any}(
            "normalized_network" => network_ir_to_dict(normalized.network),
            "network_ir_hash" => normalized.network_hash,
            "network_canonical_code" => normalized.network_code,
            "network_identity_semantics" => normalized.network_code === nothing ?
                "positional_content_hash_only" : "canonical_code_available",
            "input" => normalized.input_sym,
            "output" => normalized.output_sym,
            "topology_preserved" => true,
        ),
        "geometric_status" => geometric_status,
        "feasible" => core_status == ROPShapeOptimization.OPTIMAL,
        "geometric_status_message" => union_result === nothing ?
            "no eligible compiled cell was available within the declared population" :
            union_result.message,
        "coverage" => coverage,
        "compiled_edit" => normalized.intent.compiled,
        "selected" => selected,
        "replay" => replay,
        "certificate_grade" => ROP_SHAPE_CERTIFICATE_GRADE,
        "geometric_evidence_grade" => "exact_path_polyhedral",
        "finite_replay_evidence_grade" => replay["status"] == "pass" ?
            "sampled-forward-complete" : replay["status"] == "not_run" ?
            "not_run" : replay["status"] == "partial" ?
            "sampled-forward-partial" : "sampled-forward-failed",
        "solver_contract" => Dict{String, Any}(
            "lp_backend" => "Clarabel",
            "objective_policy" => "global_epsilon_lexicographic_effect_then_parameter_margin",
            "parameter_margin_basis" => "equality_feasible_log10_qK_subspace",
            "effect_limit_semantics" => "closed_polyhedral_support_limit",
            "active_row_shadow_price_semantics" =>
                "objective_derivative_with_respect_to_compiled_rhs_not_primal_parameter_derivative",
            "compiler_version" => ROP_SHAPE_COMPILER_VERSION,
        ),
        "warnings" => warnings,
    )
    directional === nothing ||
        (result["directional_request_interval"] = directional)
    result["result_hash"] = _canonical_hash(result)
    attach_artifact!(
        result, "rop_shape_optimize";
        input_hashes=Dict{String, Any}(
            "request" => request_hash,
            "network_ir" => normalized.network_hash,
            "designability_spec" => _canonical_hash(
                normalized.normalized["designability_spec"]),
            "reference" => normalized.reference["reference_hash"],
        ),
        algorithm_name="fixed_topology_rop_shape_optimizer",
        config=normalized.normalized,
        warnings=warnings,
    )
    return result
end

function handle_rop_shape_optimize(req)
    body = read_json(req)
    result = optimize_rop_shape_request(body; synchronous=true)
    return json_response(result)
end
