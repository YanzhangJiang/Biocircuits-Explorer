# === ROP periodic-table layer (LOCAL OVERLAY): change-path abstraction ============
# Re-synced onto upstream b30087f. Upstream initialize.jl no longer defines
# `AbstractChangePaths` and upstream's `SISOPaths` is NOT a subtype of it (it cannot be
# re-parented). So we DEFINE the abstract type here for our own `ChangePaths` and dispatch
# every overlay function on `Union{SISOPaths, ChangePaths}` (see rop_periodic_table.jl) so
# the SISOPaths parity path is covered too.

abstract type AbstractChangePaths{T} end

# --- Small helpers that used to live in the old engine's regime_graphs.jl ----------
# Upstream dropped these (the ROP overlay was relocated), so they are redefined here.
function _has_extra_constraint(constraint_C, constraint_C0)::Bool
    if isnothing(constraint_C) != isnothing(constraint_C0)
        error("constraint_C and constraint_C0 must be provided together")
    end
    return !isnothing(constraint_C)
end

_round_ro_value(x::Real) = round(Float64(x); digits=3)

function _all_nan(values::AbstractVector{<:Real})
    return !isempty(values) && all(isnan, values)
end

# --- Multi-axis / orthant (signed) reaction-order machinery -----------------------
# Ported from git-HEAD pre-swap engine (Bnc_julia/src/regime_graphs.jl):
#   _calc_change_response_for_vertex (scalar ~:973, vector ~:983)
#   _calc_RO_for_single_path (5-arg vector form ~:1102)
#   get_RO_path(model::Bnc; change_qK_indices, change_qK_signs, ...) (~:1206)
# REWIRED onto the migrated engine: data access via upstream accessors get_nullity /
# is_singular / get_H / _get_mask / locate_sym_qK / locate_sym_x / get_idx. H is used
# AS-IS — upstream's new nullity-1 (singular) regime H sign is now canonical and is NOT
# re-flipped, so the signed RO values follow the migrated engine's sign convention.
# These cover the multimodal-ROP / orthant / negative-direction ChangePaths RO path,
# which the migrated SISO.jl only supplies in scalar (single-axis) form.

"""
    _calc_change_response_for_vertex(model, vertex_idx, change_qK_idx, observe_x_idx, sign=1) -> Float64

Signed reaction order of `observe_x_idx` w.r.t. `change_qK_idx` at a single vertex.
For non-singular regimes returns the rounded `sign * H[observe_x, change_qK]`; for the
nullity-1 singular case returns a directed `Inf` (or `NaN` when the response vanishes).
"""
function _calc_change_response_for_vertex(model, vertex_idx::Integer, change_qK_idx::Integer, observe_x_idx::Integer, sign::Integer=1)::Float64
    nullity = get_nullity(model, vertex_idx)
    nullity > 1 && error("atlas_nullity_gt_1: reaction-order path materialization does not support vertex $(vertex_idx) with nullity $(nullity)")
    if !is_singular(model, vertex_idx)
        return _round_ro_value(Float64(sign) * get_H(model, vertex_idx)[observe_x_idx, change_qK_idx])
    end

    ord = Float64(sign) * Float64(get_H(model, vertex_idx)[observe_x_idx, change_qK_idx])
    return abs(ord) < 1e-6 ? NaN : ord * Inf
end

function _calc_change_response_for_vertex(model, vertex_idx::Integer, change_qK_indices::AbstractVector{<:Integer}, observe_x_idx::Integer, change_qK_signs::AbstractVector{<:Integer})::Vector{Float64}
    return Float64[
        _calc_change_response_for_vertex(model, vertex_idx, idx, observe_x_idx, sign)
        for (idx, sign) in zip(change_qK_indices, change_qK_signs)
    ]
end

"""
    _calc_RO_for_single_path(model, path, change_qK_indices, observe_x_idx, change_qK_signs) -> Vector{Vector{Float64}}

Vector-valued (one entry per change axis) reaction-order profile along a single regime path.
"""
function _calc_RO_for_single_path(
    model,
    path::AbstractVector{<:Integer},
    change_qK_indices::AbstractVector{<:Integer},
    observe_x_idx,
    change_qK_signs::AbstractVector{<:Integer},
    ;
    cancel_check=_NO_CANCEL_CHECK,
)::Vector{Vector{Float64}}
    cancel_check()
    result = Vector{Vector{Float64}}(undef, length(path))
    for i in eachindex(path)
        (Int(i) & 0xff) == 0 && cancel_check()
        result[i] = _calc_change_response_for_vertex(
            model, path[i], change_qK_indices, observe_x_idx,
            change_qK_signs)
    end
    cancel_check()
    return result
end

"""
    get_RO_path(model::Bnc, rgm_idx_shift_pth; change_qK=nothing, change_qK_indices=nothing,
        observe_x, change_qK_signs=nothing, deduplicate=false, keep_singular=true,
        keep_nonasymptotic=true) -> Vector

Multi-axis / orthant reaction-order profile for a single regime path. Use either
`change_qK=<symbol>` for a scalar SISO profile or
`change_qK_indices=<indices>, change_qK_signs=<signs>` for a signed vector-valued profile.
"""
function get_RO_path(
    model::Bnc, rgm_idx_shift_pth::AbstractVector;
    change_qK=nothing,
    change_qK_indices::Union{Nothing, AbstractVector{<:Integer}}=nothing,
    observe_x,
    change_qK_signs::Union{Nothing, AbstractVector{<:Integer}}=nothing,
    deduplicate::Bool=false,
    keep_singular::Bool=true,
    keep_nonasymptotic::Bool=true,
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    rgm_idx_shift_pth = get_idx.(Ref(model), rgm_idx_shift_pth)
    observe_x_idx = locate_sym_x(model, observe_x)

    if !isnothing(change_qK) && !isnothing(change_qK_indices)
        error("Specify either change_qK or change_qK_indices, but not both.")
    end

    ord_path = if isnothing(change_qK_indices)
        isnothing(change_qK) && error("change_qK is required when change_qK_indices is not provided.")
        change_qK_idx = locate_sym_qK(model, change_qK)
        _calc_RO_for_single_path(
            model, rgm_idx_shift_pth, change_qK_idx, observe_x_idx;
            cancel_check=cancel_check)
    else
        sign_vals = isnothing(change_qK_signs) ? fill(Int8(1), length(change_qK_indices)) : Int8[Int8(sign) for sign in change_qK_signs]
        _calc_RO_for_single_path(
            model, rgm_idx_shift_pth,
            Int[Int(idx) for idx in change_qK_indices], observe_x_idx,
            sign_vals; cancel_check=cancel_check)
    end

    mask = _get_mask(model, rgm_idx_shift_pth;
        singular=keep_singular ? nothing : false,
        asymptotic=keep_nonasymptotic ? nothing : true)
    ord_path = ord_path[mask]

    if deduplicate
        ord_path = _dedup(ord_path)
    end

    return ord_path
end

struct ChangePaths{T} <: AbstractChangePaths{T}
    bn::Bnc{T}   # binding network
    qK_grh::SimpleDiGraph
    change_kind::Symbol
    change_label::String
    change_qK_indices::Vector{T}
    change_qK_signs::Vector{Int8}

    sources::Vector{Int}
    sinks::Vector{Int}
    paths_dict::Dict{Vector{Int},Int}
    rgm_paths::Vector{Vector{Int}}
    path_polys::Vector{Any}
    path_volume::Vector{Volume}

    path_volume_is_calc::BitVector
    path_polys_is_calc::BitVector

    function ChangePaths(
        model::Bnc{T},
        qK_grh,
        change_kind::Symbol,
        change_label::AbstractString,
        change_qK_indices::AbstractVector,
        change_qK_signs::AbstractVector,
        sources,
        sinks,
        rgm_paths,
    ) where T
        path_polys = Vector{Any}(undef, length(rgm_paths))
        path_volume = Vector{Volume}(undef, length(rgm_paths))
        path_volume_is_calc = falses(length(rgm_paths))
        path_polys_is_calc = falses(length(rgm_paths))
        paths_dict = sizehint!(Dict{Vector{Int},Int}(), length(rgm_paths))
        indices = T[T(idx) for idx in change_qK_indices]
        signs = Int8[Int8(sign) for sign in change_qK_signs]
        new{T}(
            model,
            qK_grh,
            change_kind,
            String(change_label),
            indices,
            signs,
            collect(sources),
            collect(sinks),
            paths_dict,
            rgm_paths,
            path_polys,
            path_volume,
            path_volume_is_calc,
            path_polys_is_calc,
        )
    end
end

# Accessor method dispatches for ChangePaths
get_neighbor_graph_qK(grh::ChangePaths; kwargs...) = grh.qK_grh
change_qK_indices(grh::ChangePaths) = Int[Int(idx) for idx in grh.change_qK_indices]
change_qK_signs(grh::ChangePaths) = Int8[Int8(sign) for sign in grh.change_qK_signs]
change_label(grh::ChangePaths) = grh.change_label
change_kind(grh::ChangePaths) = grh.change_kind

# Polymorphic accessors matching the upstream SISOPaths accessor API so that
# consumer code works for BOTH SISOPaths and ChangePaths without field access.
get_binding_network(cp::ChangePaths, args...) = cp.bn
get_SISO_graph(cp::ChangePaths) = cp.qK_grh
get_sources(cp::ChangePaths) = cp.sources
get_sinks(cp::ChangePaths) = cp.sinks

# --- Multi-axis path access / polyhedra / volumes ---------------------------------
# Ported from git-HEAD pre-swap engine (Bnc_julia/src/regime_graphs.jl, the
# AbstractChangePaths methods get_path/get_idx/_ensure_paths_dict!/get_polyhedra/
# get_polyhedron/get_volumes/get_volume/get_C_C0_nullity_qK, ~:798-:993). The
# migration quarantine dropped these public wrappers — only the polyhedra COMPUTATION
# core survived (rewired as `_calc_constrained_polyhedra_for_path` in
# rop_periodic_table.jl). Without the wrappers, get_behavior_families(::ChangePaths)
# on the unconstrained / projected-feasible path threw MethodError on get_polyhedra.
# REWIRED: the old `_calc_polyhedra_for_path(bn, paths, change_qK_indices)` (vector
# form) is now `_calc_constrained_polyhedra_for_path(...; constraint_*=nothing/0)`,
# which is the byte-faithful port of that algorithm; everything else is identical to
# git HEAD (same caching via path_polys / path_volume, same empty-poly zero-volume
# handling, same rebase_K / rebase_mat support). Defined on ChangePaths concretely
# (not AbstractChangePaths) since SISOPaths keeps its own upstream implementations.

function _ensure_paths_dict!(grh::ChangePaths)
    length(grh.paths_dict) == length(grh.rgm_paths) && return grh.paths_dict
    isempty(grh.paths_dict) || empty!(grh.paths_dict)
    for (i, p) in enumerate(grh.rgm_paths)
        grh.paths_dict[p] = i
    end
    return grh.paths_dict
end

get_idx(grh::ChangePaths, pth::Integer) = pth
get_idx(grh::ChangePaths, pth::AbstractVector) = let
    bn = get_binding_network(grh)
    idxs = get_idx.(Ref(bn), pth)
    _ensure_paths_dict!(grh)[idxs]
end

function get_path(grh::ChangePaths, pth_idx::Integer; return_idx::Bool=false)
    rgm_idxs = grh.rgm_paths[pth_idx]
    return_idx && return rgm_idxs
    bn = get_binding_network(grh)
    return get_perm.(Ref(bn), rgm_idxs)
end

function get_path(grh::ChangePaths, pth::AbstractVector; return_idx::Bool=false)
    bn = get_binding_network(grh)
    return return_idx ? get_idx.(Ref(bn), pth) : get_perm.(Ref(bn), pth)
end

"""
    get_polyhedra(grh::ChangePaths, pth_idx=nothing; constraint_C=nothing,
        constraint_C0=nothing, constraint_nullity=0) -> Vector

Projected qK-space path polyhedra for the selected (multi-axis) change paths.
"""
function get_polyhedra(
    grh::ChangePaths,
    pth_idx::Union{AbstractVector,Nothing}=nothing;
    constraint_C=nothing,
    constraint_C0=nothing,
    constraint_nullity::Int=0,
)::Vector
    pth_idx = isnothing(pth_idx) ? collect(1:length(grh.rgm_paths)) : get_idx.(Ref(grh), pth_idx)

    if _has_extra_constraint(constraint_C, constraint_C0)
        return _calc_constrained_polyhedra_for_path(
            get_binding_network(grh),
            grh.rgm_paths[pth_idx],
            change_qK_indices(grh);
            constraint_C=constraint_C,
            constraint_C0=constraint_C0,
            constraint_nullity=constraint_nullity,
        )
    end

    pth_poly_to_calc = filter(x -> !grh.path_polys_is_calc[x], pth_idx)
    if !isempty(pth_poly_to_calc)
        polys = _calc_constrained_polyhedra_for_path(
            get_binding_network(grh),
            grh.rgm_paths[pth_poly_to_calc],
            change_qK_indices(grh);
            constraint_C=nothing,
            constraint_C0=nothing,
            constraint_nullity=0,
        )
        grh.path_polys[pth_poly_to_calc] .= polys
        grh.path_polys_is_calc[pth_poly_to_calc] .= true
    end
    return grh.path_polys[pth_idx]
end

get_polyhedron(grh::ChangePaths, pth; kwargs...) = get_polyhedra(grh, [get_idx(grh, pth)]; kwargs...)[1]

get_C_C0_nullity_qK(grh::ChangePaths, pth_idx) = get_polyhedron(grh, pth_idx) |> get_C_C0_nullity

"""
    get_volumes(grh::ChangePaths, pth_idx=nothing; rebase_K=false, rebase_mat=nothing,
        recalculate=false, constraint_C=nothing, constraint_C0=nothing,
        constraint_nullity=0, kwargs...) -> Vector{Volume}

Volumes of the projected path polyhedra for the selected change paths. Only the
unrebased estimator with no explicit estimator keywords uses the legacy
single-value path cache. Rebased or customized estimates are computed
ephemerally.
"""
function get_volumes(
    grh::ChangePaths,
    pth_idx::Union{AbstractVector,Nothing}=nothing;
    rebase_K::Bool=false,
    rebase_mat=nothing,
    recalculate::Bool=false,
    constraint_C=nothing,
    constraint_C0=nothing,
    constraint_nullity::Int=0,
    kwargs...,
)
    pth_idx = isnothing(pth_idx) ? collect(1:length(grh.rgm_paths)) : get_idx.(Ref(grh), pth_idx)
    unique_pth_idx = unique(pth_idx)
    has_extra_constraint = _has_extra_constraint(constraint_C, constraint_C0)

    rebase_mat = if !isnothing(rebase_mat)
        @assert !rebase_K "Cannot specify both rebase_K and providing rebase_mat"
        rebase_mat
    elseif rebase_K
        bn = get_binding_network(grh)
        Q = rebase_mat_lgK(bn.N)
        blockdiag(spdiagm(fill(Rational(1), bn.d - 1)), Q)
    else
        nothing
    end
    isempty(unique_pth_idx) && return Volume[]

    if has_extra_constraint
        polys = get_polyhedra(
            grh,
            unique_pth_idx;
            constraint_C=constraint_C,
            constraint_C0=constraint_C0,
            constraint_nullity=constraint_nullity,
        )
        unique_volumes = _constrained_volumes(polys; rebase_mat=rebase_mat, kwargs...)
        result_by_path = Dict(zip(unique_pth_idx, unique_volumes))
        return [result_by_path[idx] for idx in pth_idx]
    end

    use_volume_cache = _volume_request_is_cacheable(rebase_K, rebase_mat, kwargs)
    if !use_volume_cache
        polys = get_polyhedra(grh, unique_pth_idx)
        unique_volumes = fill(Volume(0.0, 0.0), length(unique_pth_idx))
        nonempty_positions = findall(!isempty, polys)
        if !isempty(nonempty_positions)
            calculated = calc_volume(
                Polyhedron[polys[position] for position in nonempty_positions];
                rebase_mat=rebase_mat,
                kwargs...,
            )
            unique_volumes[nonempty_positions] .= calculated
        end
        result_by_path = Dict(zip(unique_pth_idx, unique_volumes))
        return [result_by_path[idx] for idx in pth_idx]
    end

    idxes_to_calculate = recalculate ? unique_pth_idx :
        filter(x -> !grh.path_volume_is_calc[x], unique_pth_idx)
    if !isempty(idxes_to_calculate)
        polys = get_polyhedra(grh, idxes_to_calculate)
        nonempty_pairs = [(idx, poly) for (idx, poly) in zip(idxes_to_calculate, polys) if !isempty(poly)]
        for idx in idxes_to_calculate
            grh.path_volume[idx] = Volume(0.0, 0.0)
            grh.path_volume_is_calc[idx] = true
        end
        if !isempty(nonempty_pairs)
            rlts = calc_volume(Polyhedron[poly for (_, poly) in nonempty_pairs]; rebase_mat=rebase_mat, kwargs...)
            for ((idx, _), vol) in zip(nonempty_pairs, rlts)
                grh.path_volume[idx] = vol
            end
        end
    end
    return grh.path_volume[pth_idx]
end

get_volume(grh::ChangePaths, pth; kwargs...) = get_volumes(grh, [get_idx(grh, pth)]; kwargs...)[1]

# Accessor method dispatches for upstream SISOPaths (not <:AbstractChangePaths).
# Upstream exposes the change index via get_change_qK_idx(grh) and the model via
# get_binding_network(grh); SISOPaths is always a single-axis change.
change_qK_indices(grh::SISOPaths) = Int[Int(get_change_qK_idx(grh))]
change_qK_signs(::SISOPaths) = Int8[1]
change_label(grh::SISOPaths) = string(qK_sym(get_binding_network(grh))[get_change_qK_idx(grh)])
change_kind(::SISOPaths) = :axis

function _classify_change_projection(change_dir_qK, change_qK_indices::AbstractVector{<:Integer}, change_qK_signs::AbstractVector{<:Integer}; tol::Real=1e-6)
    isnothing(change_dir_qK) && return 0
    length(change_qK_indices) == length(change_qK_signs) || error("change_qK_indices and change_qK_signs must have the same length")

    projected = Float64[]
    for (idx, sign) in zip(change_qK_indices, change_qK_signs)
        push!(projected, Float64(sign) * Float64(change_dir_qK[idx]))
    end

    isempty(projected) && return 0
    if all(val -> val >= -tol, projected) && any(val -> val > tol, projected)
        return 1
    elseif all(val -> val <= tol, projected) && any(val -> val < -tol, projected)
        return -1
    else
        return 0
    end
end

"""
    get_change_graph(grh::VertexGraph, change_qK_indices, change_qK_signs) -> SimpleDiGraph

Build a monotone change graph for a selected qK-coordinate cone.

NOTE(resync): this is the ChangePaths (multi-parameter) path; the SISOPaths parity
fixtures do NOT build ChangePaths, so this is parity-off-path. Rewired onto upstream:
the VertexEdge no longer carries `.change_dir_qK` or `.from`; instead the change
direction is fetched via `_edge_qK_interface(grh, edge)` (sign already folded in) and
guarded with `_edge_has_qK_interface(edge)`. The directionality (the old `e.to < i`
skip) is preserved by only considering each undirected edge from its lower endpoint.
"""
function get_change_graph(
    grh::VertexGraph,
    change_qK_indices::AbstractVector{<:Integer},
    change_qK_signs::AbstractVector{<:Integer};
    cancel_check=_NO_CANCEL_CHECK,
)::SimpleDiGraph
    cancel_check()
    bn = get_binding_network(grh)
    _ensure_full_regimes_graph!(grh; cancel_check=cancel_check)

    n = length(grh.neighbors)
    g = SimpleDiGraph(n)
    for (i, edges) in enumerate(grh.neighbors)
        cancel_check()
        nlt = get_nullity(bn, i)
        if nlt > 1
            continue
        end
        for e in edges
            if e.to < i || !_edge_has_qK_interface(e)
                continue
            end
            interface = _edge_qK_interface(grh, e)
            change_dir_qK = isnothing(interface) ? nothing : interface[1]
            direction = _classify_change_projection(change_dir_qK, change_qK_indices, change_qK_signs)
            if direction > 0
                add_edge!(g, i, e.to)
            elseif direction < 0
                add_edge!(g, e.to, i)
            end
        end
    end
    cancel_check()
    return g
end

function get_change_graph(
    Bnc::Bnc,
    change_qK_indices::AbstractVector{<:Integer},
    change_qK_signs::AbstractVector{<:Integer};
    cancel_check=_NO_CANCEL_CHECK,
)
    graph = get_regimes_graph!(Bnc; full=true, cancel_check=cancel_check)
    return get_change_graph(
        graph, change_qK_indices, change_qK_signs;
        cancel_check=cancel_check)
end

"""
    ChangePaths(model::Bnc, change_qK_syms; signs=nothing, label=nothing, rgm_paths=nothing, kind=:orthant)
        -> ChangePaths

Construct a monotone multi-parameter path bundle for a chosen qK cone.
"""
function ChangePaths(
    model::Bnc{T},
    change_qK_syms::AbstractVector;
    signs=nothing,
    label=nothing,
    rgm_paths=nothing,
    kind::Symbol=:orthant,
    max_paths::Union{Nothing,Integer}=nothing,
    max_total_nodes::Union{Nothing,Integer}=nothing,
    cancel_check=_NO_CANCEL_CHECK,
) where {T}
    cancel_check()
    change_qK_indices = T[]
    for sym in change_qK_syms
        cancel_check()
        idx = locate_sym_qK(model, sym)
        isnothing(idx) && error("Unknown qK symbol in change specification: $(sym)")
        push!(change_qK_indices, idx)
    end
    isempty(change_qK_indices) && error("change_qK_syms cannot be empty")

    sign_vals = if isnothing(signs)
        fill(Int8(1), length(change_qK_indices))
    else
        length(signs) == length(change_qK_indices) || error("signs length must match change_qK_syms length")
        Int8[Int8(sign) for sign in signs]
    end
    label_str = if isnothing(label)
        join([string(sign > 0 ? "+" : "-", qK_sym(model)[idx]) for (idx, sign) in zip(change_qK_indices, sign_vals)], ",")
    else
        String(label)
    end

    if rgm_paths === nothing
        qK_grh = get_change_graph(
            model, change_qK_indices, sign_vals;
            cancel_check=cancel_check)
        cancel_check()
        sources, sinks = get_sources_sinks(model, qK_grh)
        rgm_paths = _enumerate_paths(
            qK_grh;
            sources,
            sinks,
            max_paths,
            max_total_nodes,
            cancel_check,
        )
    else
        cancel_check()
        find_all_regimes!(model; cancel_check=cancel_check)
        cancel_check()
        qK_grh = graph_from_paths(rgm_paths, n_regimes(model))
        sources, sinks = get_sources_sinks(qK_grh)
    end

    cancel_check()
    result = ChangePaths(
        model, qK_grh, kind, label_str, change_qK_indices, sign_vals,
        sources, sinks, rgm_paths)
    cancel_check()
    return result
end

function get_RO_paths(
    model::ChangePaths,
    pth_idx::Union{Nothing,AbstractVector}=nothing;
    observe_x,
    cancel_check=nothing,
    kwargs...,
)
    rgm_paths = isnothing(pth_idx) ? model.rgm_paths : get_path.(Ref(model), pth_idx; return_idx=true)
    observe_x_idx = locate_sym_x(model.bn, observe_x)
    profiles = Vector{Any}(undef, length(rgm_paths))
    if _use_parallel_ro_paths(cancel_check)
        Threads.@threads for i in eachindex(rgm_paths)
            profiles[i] = get_RO_path(
                model.bn,
                rgm_paths[i];
                change_qK_indices=change_qK_indices(model),
                change_qK_signs=change_qK_signs(model),
                observe_x=observe_x_idx,
                kwargs...,
            )
        end
    else
        for i in eachindex(rgm_paths)
            cancel_check()
            profiles[i] = get_RO_path(
                model.bn,
                rgm_paths[i];
                change_qK_indices=change_qK_indices(model),
                change_qK_signs=change_qK_signs(model),
                observe_x=observe_x_idx,
                cancel_check=cancel_check,
                kwargs...,
            )
        end
        cancel_check()
    end
    return profiles
end

get_RO_path(model::ChangePaths, pth_idx, args...; kwargs...) = get_RO_paths(model, [get_idx(model,pth_idx)], args... ; kwargs...)[1]
