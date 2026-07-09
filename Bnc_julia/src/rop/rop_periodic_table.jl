# === ROP periodic-table layer (LOCAL OVERLAY): behavior families + ROP geometry ====
# Depends only on the public engine API; relocated so the engine base can be re-synced verbatim.

"""
    _normalize_behavior_scope(scope) -> Symbol

Normalize behavior-family path filtering scope.
"""
function _normalize_behavior_scope(scope)::Symbol
    scope_sym = Symbol(scope)
    scope_sym in (:all, :feasible, :robust) || error("Unsupported path_scope=$scope. Use :all, :feasible, or :robust.")
    return scope_sym
end

# === Extra-constraint behavior families (RE-IMPLEMENTED in overlay) =================
# Upstream's resynced get_polyhedra/get_volumes dropped the
# constraint_C/constraint_C0/constraint_nullity kwargs (and the projected path
# polyhedra they return have already eliminated the change axis, so the full-qK
# constraint can no longer be intersected onto them). The webapp (atlas.jl:1620)
# still passes a constraint. To restore the OLD semantics faithfully, the functions
# below replicate the pre-swap engine's logic verbatim (git HEAD:Bnc_julia/src/
# regime_graphs.jl): build the per-vertex (node) polyhedra in FULL qK-space,
# intersect each with the constraint polyhedron BEFORE eliminating the change axis,
# then project, then calc_volume. Same halfspace orientation (-C x <= C0) and
# nullity handling (linset = 1:nullity) as upstream get_polyhedron(C, C0, nullity).

# Detect linearity and remove genuine half-space redundancy on real polyhedra,
# while passing through empty-set sentinels.  CDDLib's redundancy pass is both
# unnecessary and pathological for a pure affine set (for example, the d=1
# homomer transition is represented by two opposite half-spaces which collapse
# to one hyperplane).  It can allocate tens of gigabytes on that tiny input.
# Likewise, a lone nonzero half-space and an all-zero tautology cannot contain
# pairwise redundancy, so avoid entering the external solver for those cases.
function _clean_polyhedron_if_possible(p)
    p isa Polyhedron || return p
    detecthlinearity!(p)
    representation = hrep(p)
    constraints = collect(halfspaces(representation))
    isempty(constraints) && return p
    all(constraint -> all(iszero, constraint.a), constraints) && return p
    length(constraints) == 1 && !hashyperplanes(representation) && return p
    removehredundancy!(p)
    return p
end

# Faithful copy of the old `_constraint_polyhedron`: build the extra-constraint
# polyhedron {x : C x <= C0} (with `nullity` leading equality rows) via the upstream
# get_polyhedron(C, C0, nullity), which uses hrep(-C, C0) — identical to the old engine.
function _constraint_polyhedron(constraint_C, constraint_C0, constraint_nullity::Int)
    _has_extra_constraint(constraint_C, constraint_C0) || return nothing
    return _clean_polyhedron_if_possible(get_polyhedron(constraint_C, constraint_C0, constraint_nullity))
end

"""
    _calc_constrained_polyhedra_for_path(model, paths, change_qK_indices;
        constraint_C, constraint_C0, constraint_nullity) -> Vector{Any}

Compute the projected qK-space path polyhedra under an extra constraint, replicating
the old engine's `_calc_polyhedra_for_path` (git HEAD:regime_graphs.jl) exactly: the
constraint is intersected onto the FULL-dimensional per-vertex polyhedra before the
change axis is eliminated. Paths whose feasible region is empty keep the empty-set
object. Used only on the `has_constraint == true` path; the unconstrained path stays
on upstream `get_polyhedra`/`get_volumes` (parity-green).
"""
function _calc_constrained_polyhedra_for_path(
    model::Bnc,
    paths::AbstractVector{<:AbstractVector{<:Integer}},
    change_qK_indices::AbstractVector{<:Integer};
    constraint_C,
    constraint_C0,
    constraint_nullity::Int,
    cancel_check::Function=()->nothing,
)::Vector{Any}
    cancel_check()
    el_dim = BitSet(Int[idx for idx in change_qK_indices])
    projected_cleanup = length(change_qK_indices) == 1
    additional_poly = _constraint_polyhedron(constraint_C, constraint_C0, constraint_nullity)

    # 1) node polyhedron per unique regime, intersected with the constraint in full qK-space
    node_polyhedra = let
        unique_rgms = unique(vcat(paths...))
        dic = Dict{Int,Any}()
        for r in unique_rgms
            cancel_check()
            pr = get_polyhedron(model, r)
            if additional_poly !== nothing
                pr = intersect(pr, additional_poly)
                if isempty(pr)
                    dic[Int(r)] = pr
                    continue
                end
            end
            pr = _clean_polyhedron_if_possible(pr)
            dic[Int(r)] = pr
        end
        dic
    end

    # 2) unique undirected edges + edge index map (key = (min(u,v), max(u,v)))
    (edges, edge_dict) = let
        edges = Tuple{Int,Int}[]
        edge_dict = Dict{Tuple{Int,Int},Int}()
        for path in paths
            cancel_check()
            n = length(path)
            @inbounds for i in 1:(n-1)
                u = Int(path[i]); v = Int(path[i+1])
                a, b = u < v ? (u, v) : (v, u)
                k = (a, b)
                if !haskey(edge_dict, k)
                    push!(edges, k)
                    edge_dict[k] = length(edges)
                end
            end
        end
        (edges, edge_dict)
    end

    # 3) edge poly = intersect(node[u], node[v]), then eliminate the change axis
    edge_poly = let
        edge_poly = Vector{Any}(undef, length(edge_dict))
        for i in eachindex(edges)
            cancel_check()
            (u, v) = edges[i]
            p = intersect(node_polyhedra[u], node_polyhedra[v])
            if isempty(p)
                edge_poly[i] = p
                continue
            end
            p = _clean_polyhedron_if_possible(p)
            q = eliminate(p, el_dim)
            edge_poly[i] = projected_cleanup ? _clean_polyhedron_if_possible(q) : q
        end
        edge_poly
    end

    edge_paths = let
        function path_to_edge_idxs(path)
            n = length(path)
            idxs = Vector{Int}(undef, n-1)
            @inbounds for i in 1:(n-1)
                u = Int(path[i]); v = Int(path[i+1])
                a, b = u < v ? (u, v) : (v, u)
                idxs[i] = edge_dict[(a, b)]
            end
            return idxs
        end
        path_to_edge_idxs.(paths)
    end

    # 4) path poly = intersect of its edge polys
    out = Vector{Any}(undef, length(edge_paths))
    for i in eachindex(edge_paths)
        cancel_check()
        path_edge_polys = edge_poly[edge_paths[i]]
        empty_idx = findfirst(isempty, path_edge_polys)
        if !isnothing(empty_idx)
            out[i] = path_edge_polys[empty_idx]
            continue
        end
        p = intersect(path_edge_polys...)
        out[i] = isempty(p) ? p : (projected_cleanup ? _clean_polyhedron_if_possible(p) : p)
    end
    return out
end

"""
    _constrained_polyhedra(grh, pth_idx; constraint_C, constraint_C0, constraint_nullity)
        -> Vector{Any}

Constraint-aware analogue of upstream `get_polyhedra(grh, pth_idx)`. Replaces the old
`get_polyhedra(...; constraint_*...)` path. Not cached (the constraint is request-scoped).
"""
function _constrained_polyhedra(
    grh::Union{SISOPaths, ChangePaths},
    pth_idx::AbstractVector{<:Integer};
    constraint_C,
    constraint_C0,
    constraint_nullity::Int,
    cancel_check::Function=()->nothing,
)::Vector{Any}
    return _calc_constrained_polyhedra_for_path(
        get_binding_network(grh),
        grh.rgm_paths[pth_idx],
        change_qK_indices(grh);
        constraint_C=constraint_C,
        constraint_C0=constraint_C0,
        constraint_nullity=constraint_nullity,
        cancel_check=cancel_check,
    )
end

"""
    _constrained_volumes(polys; rebase_mat=nothing, kwargs...) -> Vector{Volume}

Constraint-aware analogue of upstream `get_volumes`, replicating the old engine's
constrained branch: zero-volume for empty path polyhedra, `calc_volume` over the
non-empty ones (same rebase_mat / kwargs forwarding as the old get_volumes).
"""
function _constrained_volumes(polys::AbstractVector; rebase_mat=nothing, kwargs...)
    out = [Volume(0.0, 0.0) for _ in eachindex(polys)]
    nonempty_pairs = [(idx, poly) for (idx, poly) in enumerate(polys) if !isempty(poly)]
    if !isempty(nonempty_pairs)
        rlts = calc_volume(Polyhedron[poly for (_, poly) in nonempty_pairs]; rebase_mat=rebase_mat, kwargs...)
        for ((idx, _), vol) in zip(nonempty_pairs, rlts)
            out[idx] = vol
        end
    end
    return out
end

"""
    _ro_token(x; zero_tol=1e-6) -> String

Map a reaction-order value to a motif token.
"""
function _ro_token(x::Real; zero_tol::Real=1e-6)::String
    if isnan(x)
        return "NaN"
    elseif isinf(x)
        return signbit(x) ? "-Inf" : "+Inf"
    elseif abs(x) <= zero_tol
        return "0"
    else
        return x > 0 ? "+" : "-"
    end
end

"""
    _motif_profile(ord_path; zero_tol=1e-6) -> Vector{String}

Convert an exact reaction-order profile to a coarse motif profile.
"""
_motif_profile(ord_path::AbstractVector{<:Real}; zero_tol::Real=1e-6)::Vector{String} =
    map(x -> _ro_token(x; zero_tol=zero_tol), ord_path)

function _motif_profile(ord_path::AbstractVector{<:AbstractVector{<:Real}}; zero_tol::Real=1e-6)::Vector{String}
    return [
        "(" * join((_ro_token(x; zero_tol=zero_tol) for x in coords), ",") * ")"
        for coords in ord_path
    ]
end

"""
    _motif_label(motif_profile) -> String

Assign a coarse behavior label to a motif profile.
"""
function _motif_label(motif_profile::AbstractVector{<:AbstractString})::String
    isempty(motif_profile) && return "empty"
    if any(token -> startswith(token, "("), motif_profile)
        return "vector_motif::" * join(motif_profile, " -> ")
    end

    has_singular = any(x -> x == "NaN" || x == "+Inf" || x == "-Inf", motif_profile)
    coarse = map(motif_profile) do token
        token == "+Inf" ? "+" : token == "-Inf" ? "-" : token == "NaN" ? "0" : token
    end

    label = if all(==("0"), coarse)
        "flat"
    elseif all(==("+"), coarse)
        length(coarse) == 1 ? "monotone_activation" : "multistage_activation"
    elseif all(==("-"), coarse)
        length(coarse) == 1 ? "monotone_repression" : "multistage_repression"
    else
        nz = filter(!=("0"), coarse)
        first_nz = findfirst(!=("0"), coarse)
        last_nz = findlast(!=("0"), coarse)
        sign_changes = length(nz) <= 1 ? 0 : count(i -> nz[i] != nz[i+1], 1:(length(nz)-1))

        if !isempty(nz) && all(x -> x == "+" || x == "0", coarse)
            if first_nz > 1 && last_nz < length(coarse)
                "band_pass_like"
            elseif first_nz > 1
                "thresholded_activation"
            elseif last_nz < length(coarse)
                "activation_with_saturation"
            else
                "positive_motif"
            end
        elseif !isempty(nz) && all(x -> x == "-" || x == "0", coarse)
            if first_nz > 1 && last_nz < length(coarse)
                "window_repression"
            elseif first_nz > 1
                "thresholded_repression"
            elseif last_nz < length(coarse)
                "repression_with_floor"
            else
                "negative_motif"
            end
        elseif sign_changes == 1 && first(nz) == "+" && last(nz) == "-"
            "biphasic_peak"
        elseif sign_changes == 1 && first(nz) == "-" && last(nz) == "+"
            "biphasic_valley"
        else
            "complex_motif"
        end
    end

    return has_singular ? label * "_with_singular_transition" : label
end

"""
    _sum_volumes(volumes) -> Union{Nothing, Volume}

Sum `Volume` objects, returning `nothing` for an empty list.
"""
function _sum_volumes(volumes::AbstractVector)
    isempty(volumes) && return nothing
    return foldl(+, volumes; init=zero(first(volumes)))
end

"""
    _representative_path_idx(path_ids, path_records) -> Int

Pick the path with the largest known volume, falling back to the first path when
volume data is unavailable.
"""
function _representative_path_idx(path_ids::AbstractVector{<:Integer}, path_records::AbstractVector)::Int
    isempty(path_ids) && return 0
    scores = map(path_ids) do path_idx
        vol = path_records[path_idx].volume
        isnothing(vol) ? -Inf : vol.mean
    end
    max_score = maximum(scores)
    return isfinite(max_score) ? path_ids[argmax(scores)] : first(path_ids)
end

"""
    _profile_label(profile) -> String

Render a profile vector with arrows for display.
"""
_profile_label(profile::AbstractVector{<:Real})::String = format_arrow(profile; digits=3)
_profile_label(profile::AbstractVector{<:AbstractString})::String = join(profile, " -> ")
function _profile_label(profile::AbstractVector{<:AbstractVector{<:Real}})::String
    return join([
        "[" * join((string(_round_ro_value(x)) for x in coords), ",") * "]"
        for coords in profile
    ], " -> ")
end

function _profile_fingerprint(profile::AbstractVector{<:Real})::UInt
    h = hash(:real_profile)
    h = hash(length(profile), h)
    for value in profile
        h = hash(value, h)
    end
    return h
end

function _profile_fingerprint(profile::AbstractVector{<:AbstractString})::UInt
    h = hash(:string_profile)
    h = hash(length(profile), h)
    for value in profile
        h = hash(value, h)
    end
    return h
end

function _profile_fingerprint(profile::AbstractVector{<:AbstractVector{<:Real}})::UInt
    h = hash(:vector_profile)
    h = hash(length(profile), h)
    for coords in profile
        h = hash(length(coords), h)
        for value in coords
            h = hash(value, h)
        end
    end
    return h
end

function _profile_group_index!(
    profiles::Vector{Any},
    bucketed_group_ids::Dict{UInt,Vector{Int}},
    profile,
)::Int
    fingerprint = _profile_fingerprint(profile)
    candidate_group_ids = get!(bucketed_group_ids, fingerprint, Int[])
    for group_idx in candidate_group_ids
        isequal(profiles[group_idx], profile) && return group_idx
    end

    push!(profiles, profile)
    group_idx = length(profiles)
    push!(candidate_group_ids, group_idx)
    return group_idx
end

function _path_total_volume(path_records::AbstractVector, path_ids::AbstractVector{<:Integer})
    volumes = Volume[]
    sizehint!(volumes, length(path_ids))
    for path_idx in path_ids
        vol = path_records[path_idx].volume
        isnothing(vol) || push!(volumes, vol)
    end
    return _sum_volumes(volumes)
end

function _path_volume_mean(path_records::AbstractVector, path_ids::AbstractVector{<:Integer})
    mean_values = Float64[]
    sizehint!(mean_values, length(path_ids))
    for path_idx in path_ids
        vol = path_records[path_idx].volume
        isnothing(vol) || push!(mean_values, vol.mean)
    end
    return isempty(mean_values) ? nothing : sum(mean_values) / length(mean_values)
end

function _robust_path_count(path_records::AbstractVector, path_ids::AbstractVector{<:Integer}, min_volume_mean::Real)::Int
    count = 0
    for path_idx in path_ids
        rec = path_records[path_idx]
        if rec.feasible && !isnothing(rec.volume) && rec.volume.mean >= min_volume_mean
            count += 1
        end
    end
    return count
end

"""
    _should_include_behavior_path(ro_profile, feasible, volume, path_scope, min_volume_mean)
        -> (Bool, Union{Nothing, String})

Decide whether a path should be included in behavior-family grouping.
"""
function _should_include_behavior_path(
    ro_profile::AbstractVector,
    feasible::Bool,
    volume::Union{Nothing,Volume},
    path_scope::Symbol,
    min_volume_mean::Real,
)::Tuple{Bool,Union{Nothing,String}}
    isempty(ro_profile) && return (false, "empty_behavior_profile")

    if path_scope == :all
        return (true, nothing)
    elseif !feasible
        return (false, "empty_path_polyhedron")
    elseif path_scope == :feasible
        return (true, nothing)
    elseif isnothing(volume)
        return (false, "volume_not_computed")
    elseif volume.mean < min_volume_mean
        return (false, "volume_below_threshold")
    else
        return (true, nothing)
    end
end

"""
    get_behavior_families(grh::AbstractChangePaths; observe_x, path_scope=:feasible,
        min_volume_mean=0.0, deduplicate=true, keep_singular=true,
        keep_nonasymptotic=false, motif_zero_tol=1e-6, compute_volume=true,
        volume_kwargs...) -> NamedTuple

Enumerate exact and motif behavior families for one SISO path bundle and one output species.

`path_scope` controls which paths contribute to families:
- `:all`: include every graph path, even if its path polyhedron is empty.
- `:feasible`: include only paths with non-empty path polyhedra.
- `:robust`: include only feasible paths whose estimated volume mean is at least
  `min_volume_mean`.

All paths are still reported in the returned diagnostics, along with explicit
`included` / `exclusion_reason` fields, so filtering is never silent.
"""
function get_behavior_families(
    grh::Union{SISOPaths, ChangePaths};
    observe_x,
    path_scope::Symbol=:feasible,
    min_volume_mean::Real=0.0,
    deduplicate::Bool=true,
    keep_singular::Bool=true,
    keep_nonasymptotic::Bool=false,
    motif_zero_tol::Real=1e-6,
    compute_volume::Bool=true,
    include_path_labels::Bool=true,
    include_path_details::Bool=true,
    include_family_path_indices::Bool=true,
    constraint_C=nothing,
    constraint_C0=nothing,
    constraint_nullity::Int=0,
    cancel_check::Function=()->nothing,
    volume_kwargs...,
)
    cancel_check()
    path_scope = _normalize_behavior_scope(path_scope)
    path_scope == :robust && !compute_volume && error("path_scope=:robust requires compute_volume=true")
    has_constraint = _has_extra_constraint(constraint_C, constraint_C0)
    graph_only_multi_change = grh isa ChangePaths && path_scope == :all && !compute_volume && !has_constraint

    total_paths = length(grh.rgm_paths)
    ord_paths = get_RO_paths(
        grh;
        observe_x=observe_x,
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic,
    )
    cancel_check()

    # When an extra constraint is present, upstream get_polyhedra/get_volumes no longer
    # accept it, so the overlay computes constraint-aware path polyhedra (and their
    # volumes) directly, replicating the old engine (see _calc_constrained_polyhedra_for_path).
    # Constrained polyhedra are not cached, so we keep them to feed the volume pass.
    #
    # fix(resync): also route the UNCONSTRAINED SISOPaths case through the overlay's
    # OLD-faithful _calc_constrained_polyhedra_for_path (constraint=nothing). Upstream's
    # get_polyhedra(::SISOPaths) -> _store_pair_polyhedra! THROWS
    # ("missing from the shared path-condition backend") on any structurally-enumerated
    # path whose recursive path-condition is empty (infeasible): the recursive backend
    # prunes infeasible paths (every isempty/isnothing `continue` in
    # _find_pair_path_conditions!), so the requested path key is legitimately absent and
    # upstream errors instead of recording an empty polyhedron. The OLD engine returned an
    # EMPTY polyhedron for such paths (regime_graphs.jl `_calc_polyhedra_for_path`), which
    # is exactly what get_behavior_families needs for `feasible_mask`. The overlay path is
    # proven byte-identical to upstream on every feasible path (5/5 parity fixtures: same
    # feasible mask, same volume means), and merely yields `isempty == true` instead of a
    # throw for the pruned/infeasible paths. ChangePaths already dispatches to overlay
    # get_polyhedra/get_volumes (rop_change_paths.jl) so its `else` branch stays unchanged.
    use_overlay_polys = has_constraint || grh isa SISOPaths
    constrained_polys = nothing
    feasible_mask = graph_only_multi_change ? trues(total_paths) : begin
        if use_overlay_polys
            constrained_polys = _constrained_polyhedra(
                grh,
                collect(1:total_paths);
                constraint_C=constraint_C,
                constraint_C0=constraint_C0,
                constraint_nullity=constraint_nullity,
                cancel_check=cancel_check,
            )
            map(poly -> !isempty(poly), constrained_polys)
        else
            polys = get_polyhedra(grh)
            cancel_check()
            map(poly -> !isempty(poly), polys)
        end
    end
    feasible_ids = compute_volume ? findall(feasible_mask) : Int[]

    volumes_by_path = Vector{Union{Nothing,Volume}}(undef, total_paths)
    fill!(volumes_by_path, nothing)
    if compute_volume && !isempty(feasible_ids)
        cancel_check()
        # fix(resync): use the overlay volume pass whenever the overlay polyhedra were
        # computed above (has_constraint OR unconstrained SISOPaths). This consumes the
        # already-built `constrained_polys` (empty-poly-safe; proven volume-parity on the
        # 5 fixtures) and avoids upstream get_volumes(::SISOPaths) -> get_polyhedra which
        # would re-throw on the infeasible/pruned paths. The ChangePaths-unconstrained
        # case keeps using its own overlay get_volumes via the `else` branch.
        vols = if use_overlay_polys
            # constrained_polys is computed above whenever use_overlay_polys && !graph_only_multi_change;
            # (graph_only_multi_change is false when use_overlay_polys, by construction).
            # Replicate the old get_volumes rebase handling (works for SISOPaths & ChangePaths
            # alike via get_binding_network; the webapp passes no rebase kwargs).
            rebase_K = get(volume_kwargs, :rebase_K, false)
            rebase_mat = get(volume_kwargs, :rebase_mat, nothing)
            rebasing = if !isnothing(rebase_mat)
                @assert !rebase_K "Cannot specify both rebase_K and providing rebase_mat"
                rebase_mat
            elseif rebase_K
                bn = get_binding_network(grh)
                Q = rebase_mat_lgK(bn.N)
                blockdiag(spdiagm(fill(Rational(1), bn.d - 1)), Q)
            else
                nothing
            end
            # volume_kwargs is a Base.Pairs (kwargs collection); convert to NamedTuple
            # before stripping the rebase keys we handled explicitly above.
            other_kwargs = Base.structdiff(NamedTuple(volume_kwargs), NamedTuple{(:rebase_K, :rebase_mat)})
            _constrained_volumes(constrained_polys[feasible_ids]; rebase_mat=rebasing, other_kwargs...)
        else
            get_volumes(
                grh,
                feasible_ids;
                volume_kwargs...,
            )
        end
        for (path_idx, vol) in zip(feasible_ids, vols)
            cancel_check()
            volumes_by_path[path_idx] = vol
        end
    end

    path_records = Vector{NamedTuple}(undef, total_paths)
    exclusion_counts = Dict{String,Int}()
    included_paths = 0
    exact_profiles = Any[]
    exact_group_paths = Vector{Vector{Int}}()
    exact_profile_buckets = Dict{UInt,Vector{Int}}()
    exact_group_to_motif = Int[]
    motif_profiles = Any[]
    motif_group_paths = Vector{Vector{Int}}()
    motif_profile_buckets = Dict{UInt,Vector{Int}}()
    motif_group_to_exact_groups = Vector{Vector{Int}}()

    for path_idx in 1:total_paths
        cancel_check()
        ord_profile = ord_paths[path_idx]
        motif_profile = _motif_profile(ord_profile; zero_tol=motif_zero_tol)
        feasible = feasible_mask[path_idx]
        volume = volumes_by_path[path_idx]
        included, exclusion_reason = _should_include_behavior_path(
            ord_profile, feasible, volume, path_scope, min_volume_mean)

        if included
            included_paths += 1

            exact_group_idx = _profile_group_index!(exact_profiles, exact_profile_buckets, ord_profile)
            while length(exact_group_paths) < exact_group_idx
                push!(exact_group_paths, Int[])
                push!(exact_group_to_motif, 0)
            end
            push!(exact_group_paths[exact_group_idx], path_idx)

            motif_group_idx = _profile_group_index!(motif_profiles, motif_profile_buckets, motif_profile)
            while length(motif_group_paths) < motif_group_idx
                push!(motif_group_paths, Int[])
                push!(motif_group_to_exact_groups, Int[])
            end
            exact_group_to_motif[exact_group_idx] = motif_group_idx
            push!(motif_group_paths[motif_group_idx], path_idx)
            exact_groups = motif_group_to_exact_groups[motif_group_idx]
            exact_group_idx in exact_groups || push!(exact_groups, exact_group_idx)
        elseif !isnothing(exclusion_reason)
            exclusion_counts[exclusion_reason] = get(exclusion_counts, exclusion_reason, 0) + 1
        end

        path_records[path_idx] = if include_path_details
            (
                path_idx=path_idx,
                vertex_indices=copy(grh.rgm_paths[path_idx]),
                exact_profile=ord_profile,
                exact_label=include_path_labels ? _profile_label(ord_profile) : nothing,
                motif_profile=motif_profile,
                motif_label=include_path_labels ? _motif_label(motif_profile) : nothing,
                feasible=feasible,
                feasibility_checked=!graph_only_multi_change,
                included=included,
                exclusion_reason=exclusion_reason,
                volume=volume,
            )
        else
            (
                path_idx=path_idx,
                feasible=feasible,
                included=included,
                exclusion_reason=exclusion_reason,
                volume=volume,
            )
        end
    end

    exact_family_stats = NamedTuple[]
    for exact_group_idx in eachindex(exact_group_paths)
        cancel_check()
        path_ids = exact_group_paths[exact_group_idx]
        profile = exact_profiles[exact_group_idx]
        motif_profile = motif_profiles[exact_group_to_motif[exact_group_idx]]
        total_volume = _path_total_volume(path_records, path_ids)
        rep_path = if isempty(path_ids)
            0
        elseif isnothing(total_volume)
            first(path_ids)
        else
            _representative_path_idx(path_ids, path_records)
        end

        push!(exact_family_stats, (
            exact_group_idx=exact_group_idx,
            exact_profile=profile,
            exact_label=_profile_label(profile),
            motif_profile=motif_profile,
            motif_label=_motif_label(motif_profile),
            path_indices=include_family_path_indices ? copy(path_ids) : Int[],
            n_paths=length(path_ids),
            robust_path_count=_robust_path_count(path_records, path_ids, min_volume_mean),
            volume_mean=_path_volume_mean(path_records, path_ids),
            total_volume=total_volume,
            representative_path_idx=rep_path,
            representative_volume=rep_path == 0 ? nothing : path_records[rep_path].volume,
        ))
    end

    sort!(exact_family_stats; by=family -> isnothing(family.total_volume) ? family.n_paths : family.total_volume.mean, rev=true)
    exact_family_idx_by_group = zeros(Int, length(exact_family_stats))
    exact_families = NamedTuple[]
    sizehint!(exact_families, length(exact_family_stats))
    for (family_idx, family) in enumerate(exact_family_stats)
        cancel_check()
        exact_family_idx_by_group[family.exact_group_idx] = family_idx
        push!(exact_families, (
            family_idx=family_idx,
            exact_profile=family.exact_profile,
            exact_label=family.exact_label,
            motif_profile=family.motif_profile,
            motif_label=family.motif_label,
            path_indices=family.path_indices,
            n_paths=family.n_paths,
            robust_path_count=family.robust_path_count,
            volume_mean=family.volume_mean,
            total_volume=family.total_volume,
            representative_path_idx=family.representative_path_idx,
            representative_volume=family.representative_volume,
        ))
    end

    motif_family_stats = NamedTuple[]
    for motif_group_idx in eachindex(motif_group_paths)
        path_ids = motif_group_paths[motif_group_idx]
        motif_profile = motif_profiles[motif_group_idx]
        total_volume = _path_total_volume(path_records, path_ids)
        rep_path = if isempty(path_ids)
            0
        elseif isnothing(total_volume)
            first(path_ids)
        else
            _representative_path_idx(path_ids, path_records)
        end

        push!(motif_family_stats, (
            motif_profile=motif_profile,
            motif_label=_motif_label(motif_profile),
            path_indices=include_family_path_indices ? copy(path_ids) : Int[],
            n_paths=length(path_ids),
            exact_family_indices=sort([exact_family_idx_by_group[exact_group_idx] for exact_group_idx in motif_group_to_exact_groups[motif_group_idx]]),
            robust_path_count=_robust_path_count(path_records, path_ids, min_volume_mean),
            volume_mean=_path_volume_mean(path_records, path_ids),
            total_volume=total_volume,
            representative_path_idx=rep_path,
            representative_volume=rep_path == 0 ? nothing : path_records[rep_path].volume,
        ))
    end

    sort!(motif_family_stats; by=family -> isnothing(family.total_volume) ? family.n_paths : family.total_volume.mean, rev=true)
    motif_families = [(; family..., family_idx=i) for (i, family) in enumerate(motif_family_stats)]

    return (
        change_kind=change_kind(grh),
        change_label=change_label(grh),
        change_qK_indices=change_qK_indices(grh),
        change_qK_signs=change_qK_signs(grh),
        change_qK_idx=length(change_qK_indices(grh)) == 1 ? only(change_qK_indices(grh)) : nothing,
        observe_x_idx=locate_sym_x(get_binding_network(grh), observe_x),
        path_scope=path_scope,
        min_volume_mean=Float64(min_volume_mean),
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic,
        compute_volume=compute_volume,
        feasibility_mode=graph_only_multi_change ? :graph_only_unchecked : :projected_feasible,
        total_paths=total_paths,
        feasible_paths=count(feasible_mask),
        included_paths=included_paths,
        excluded_paths=total_paths - included_paths,
        exclusion_counts=exclusion_counts,
        path_records=path_records,
        exact_families=exact_families,
        motif_families=motif_families,
    )
end

"""
    get_behavior_families(model::Bnc, change_qK; kwargs...) -> NamedTuple

Convenience wrapper that constructs `SISOPaths` first.
"""
get_behavior_families(model::Bnc, change_qK; kwargs...) = get_behavior_families(SISOPaths(model, change_qK); kwargs...)

"""
    summary_behavior_families(grh::AbstractChangePaths; observe_x, level=:exact, kwargs...) -> nothing

Print exact or motif behavior families.
"""
function summary_behavior_families(
    grh::Union{SISOPaths, ChangePaths};
    observe_x,
    level::Symbol=:exact,
    kwargs...,
)
    result = get_behavior_families(grh; observe_x=observe_x, kwargs...)
    if level == :exact
        print_paths(
            getindex.(result.exact_families, :exact_profile);
            prefix="",
            ids=getindex.(result.exact_families, :family_idx),
            volumes=getindex.(result.exact_families, :total_volume),
        )
    elseif level == :motif
        for family in result.motif_families
            volume_str = isnothing(family.total_volume) ? "n/a" :
                Printf.@sprintf("%.4f ± %.4f", family.total_volume.mean, sqrt(family.total_volume.var))
            println("Motif $(family.family_idx): $(_profile_label(family.motif_profile))  Label=$(family.motif_label)  Volume=$(volume_str)")
        end
    else
        error("Unsupported level=$level. Use :exact or :motif.")
    end
    return nothing
end


#-----------------------------------------------------------------
# ROP Polyhedron functions
#-----------------------------------------------------------------

"""
    get_vertex_rop_coords(model::Bnc, vertex_idx::Int, output_coeffs::Vector{Float64},
                          param_idx1::Int, param_idx2::Int)
    -> (Float64, Float64)

Get reaction order coordinates for a vertex in 2D ROP space.

# Arguments
- `model`: Bnc model
- `vertex_idx`: Vertex index
- `output_coeffs`: Coefficient vector for linear combination of species
- `param_idx1`: First parameter index (q or K)
- `param_idx2`: Second parameter index (q or K)

# Returns
- `(ro1, ro2)`: Reaction orders ∂log(y)/∂log(q1), ∂log(y)/∂log(q2)
"""
function get_vertex_rop_coords(model::Bnc, vertex_idx::Int, output_coeffs::Vector{Float64},
                               param_idx1::Int, param_idx2::Int)
    nullity = get_nullity(model, vertex_idx)

    # Skip highly singular vertices
    if nullity > 1
        return (NaN, NaN)
    end

    # Get H matrix (works for both nullity=0 and nullity=1)
    H = get_H(model, vertex_idx)

    # Compute reaction orders as dot product: RO = coeffs' * H[:, param_idx]
    ro1 = dot(output_coeffs, H[:, param_idx1])
    ro2 = dot(output_coeffs, H[:, param_idx2])

    return (ro1, ro2)
end


"""
    get_edge_rop_segment(model::Bnc, vertex_idx1::Int, vertex_idx2::Int,
                         output_coeffs::Vector{Float64}, param_idx1::Int, param_idx2::Int)
    -> Union{Nothing, Tuple{Vector{Float64}, Vector{Float64}}}

Get ROP coordinates for an edge between two vertices.

# Returns
- `nothing` if vertices are not neighbors
- `(ro1_coords, ro2_coords)`: Arrays of ROP coordinates along edge
"""
function get_edge_rop_segment(model::Bnc, vertex_idx1::Int, vertex_idx2::Int,
                              output_coeffs::Vector{Float64}, param_idx1::Int, param_idx2::Int)
    # Check if vertices are neighbors
    if !is_neighbor(model, vertex_idx1, vertex_idx2)
        return nothing
    end

    # Get ROP coordinates for both vertices
    ro1_start, ro2_start = get_vertex_rop_coords(model, vertex_idx1, output_coeffs, param_idx1, param_idx2)
    ro1_end, ro2_end = get_vertex_rop_coords(model, vertex_idx2, output_coeffs, param_idx1, param_idx2)

    # Check for invalid coordinates
    if any(isnan.([ro1_start, ro2_start, ro1_end, ro2_end])) ||
       any(isinf.([ro1_start, ro2_start, ro1_end, ro2_end]))
        return nothing
    end

    # Return line segment
    return ([ro1_start, ro1_end], [ro2_start, ro2_end])
end


"""
    compute_rop_polyhedron(model::Bnc, output_coeffs::Vector{Float64},
                           param_idx1::Int, param_idx2::Int;
                           asymptotic_only::Bool=true, max_vertices::Int=1000)
    -> Dict

Compute analytical ROP polyhedron vertices and edges.

# Returns
Dictionary with:
- `vertices`: Vector of (ro1, ro2, vertex_idx, nullity, perm) tuples
- `edges`: Vector of edge dictionaries
"""
function compute_rop_polyhedron(model::Bnc, output_coeffs::Vector{Float64},
                                param_idx1::Int, param_idx2::Int;
                                asymptotic_only::Bool=true, max_vertices::Int=1000)
    # Ensure vertices are enumerated
    if model.vertices_perm === nothing
        find_all_vertices!(model)
    end

    # Get vertex indices (filter by asymptotic and nullity ≤ 1)
    all_indices = 1:n_vertices(model)
    vertex_indices = Int[]

    for idx in all_indices
        nullity = get_nullity(model, idx)
        asymp = is_asymptotic(model, idx)

        # Filter: nullity ≤ 1, and optionally asymptotic only
        if nullity <= 1 && (!asymptotic_only || asymp)
            push!(vertex_indices, idx)
        end
    end

    if length(vertex_indices) > max_vertices
        @warn "Too many vertices ($(length(vertex_indices))), limiting to $max_vertices"
        vertex_indices = vertex_indices[1:max_vertices]
    end

    # Compute vertex coordinates
    vertices = []
    for idx in vertex_indices
        ro1, ro2 = get_vertex_rop_coords(model, idx, output_coeffs, param_idx1, param_idx2)
        if !any(isnan.([ro1, ro2])) && !any(isinf.([ro1, ro2]))
            nullity = get_nullity(model, idx)
            perm = collect(get_perm(model, idx))
            push!(vertices, (ro1, ro2, idx, nullity, perm))
        end
    end

    # Build vertex graph if not already built
    if model.vertices_graph === nothing
        get_vertices_graph!(model; full=true)
    end

    # Compute edges
    edges = []
    processed_pairs = Set{Tuple{Int,Int}}()

    for (ro1_v1, ro2_v1, idx1, nullity1, perm1) in vertices
        # Get neighbors as VertexEdge objects
        neighbors_list = model.vertices_graph.neighbors[idx1]

        for neighbor_edge in neighbors_list
            idx2 = neighbor_edge.to

            # Skip if not in our vertex list
            if !(idx2 in vertex_indices)
                continue
            end

            # Avoid duplicate edges
            pair = idx1 < idx2 ? (idx1, idx2) : (idx2, idx1)
            if pair in processed_pairs
                continue
            end
            push!(processed_pairs, pair)

            # Get edge segment
            segment = get_edge_rop_segment(model, idx1, idx2, output_coeffs, param_idx1, param_idx2)
            if !isnothing(segment)
                push!(edges, Dict(
                    "ro1" => segment[1],
                    "ro2" => segment[2],
                    "from_idx" => idx1,
                    "to_idx" => idx2,
                ))
            end
        end
    end

    return Dict(
        "vertices" => vertices,
        "edges" => edges,
    )
end
