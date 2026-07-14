"""
Axis-Aligned SISO Path-Condition Backend
========================================

This file contains the SISO-specific graph, path, polyhedron, and reaction-order
helpers used by `SISOPaths`.

High-level idea
----------------
1. Build a directed regime graph by orienting each qK interface along one chosen
   `change_qK` coordinate.
2. Enumerate all source-to-sink regime paths in that DAG.
3. Reuse a memoized pair solver (`SISOHelper`) to compute path conditions in
   reduced qK-space.
4. Expose higher-level APIs for path polyhedra, volumes, expression tracing, and
   reaction-order summaries.
"""

export SISOPaths, get_polyhedra, get_polyhedron, get_SISO_graph
export PathEnumerationLimitExceeded
export get_sources, get_sinks, get_sources_sinks
export get_regimes_graph!
export get_path, get_edge, get_intersect
export get_neighbor_graph_x, get_neighbor_graph_qK, get_neighbor_graph
export get_RO_path, group_sum, get_RO_paths, summary_RO_path
export get_volume


# ============================================================================
# Regime Graph Access
# ============================================================================

"""
    get_binding_network(grh::VertexGraph, args...) -> Bnc

Return the model backing a vertex graph.
"""
get_binding_network(grh::VertexGraph, args...) = grh.bn

"""
    _ensure_full_regimes_graph!(grh::VertexGraph) -> nothing

Ensure qK change directions are computed for a vertex graph.
"""
function _ensure_full_regimes_graph!(
    grh::VertexGraph;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    if !grh.change_dir_qK_computed
        @info "Calculating vertices neighbor graph with qK change dir"
        _fulfill_regimes_graph!(grh; cancel_check=cancel_check)
        cancel_check()
        grh.change_dir_qK_computed = true
    end
    cancel_check()
    return nothing
end

function _ensure_full_regimes_graph!(model::Bnc; cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    isnothing(model.vertices_graph) && error("Regime graph is not initialized.")
    return _ensure_full_regimes_graph!(
        model.vertices_graph; cancel_check=cancel_check)
end

"""
    get_regimes_graph!(bnc::Bnc; full=false) -> VertexGraph

Ensure the vertex graph is built; when `full=true`, also compute qK change
directions.
"""
function get_regimes_graph!(
    bnc::Bnc;
    full::Bool=false,
    cancel_check=_NO_CANCEL_CHECK,
)::VertexGraph
    # Always pass through the construction lock. `vertices_graph !== nothing`
    # is not a completion marker because the builder assembles several caches
    # before its final commit point.
    find_all_regimes!(bnc; cancel_check=cancel_check)
    cancel_check()
    full && _ensure_full_regimes_graph!(
        bnc.vertices_graph; cancel_check=cancel_check)
    cancel_check()
    return bnc.vertices_graph
end

"""
    get_edge(grh::VertexGraph, from, to; kwargs...) -> Union{Nothing, VertexEdge}

Return the edge between two vertices.
"""
function get_edge(grh::VertexGraph, from, to; kwargs...)::Union{Nothing,VertexEdge}
    from_idx = get_idx(get_binding_network(grh), from)
    to_idx = get_idx(get_binding_network(grh), to)
    pos = get(grh.edge_pos[from_idx], to_idx, nothing)
    pos === nothing && return nothing
    return grh.neighbors[from_idx][pos]
end

"""
    get_edge(bnc, from, to; kwargs...) -> Union{Nothing, VertexEdge}

Convenience wrapper to fetch an edge from a model.
"""
function get_edge(bnc, from, to; kwargs...)
    vtx_grh = get_regimes_graph!(bnc; full=false)
    return get_edge(vtx_grh, from, to; kwargs...)
end

"""
    get_neighbor_graph_x(grh::VertexGraph) -> SimpleGraph

Return the x-space neighbor graph for a vertex graph.
"""
function get_neighbor_graph_x(grh::VertexGraph)
    neighbors = getfield(grh, :neighbors)
    g = SimpleGraph(length(neighbors))
    for i in eachindex(neighbors)
        edges = neighbors[i]
        for e in edges
            add_edge!(g, i, e.to)
        end
    end
    return g
end

"""
    get_neighbor_graph_x(bnc::Bnc) -> SimpleGraph

Return the x-space neighbor graph for a model.
"""
get_neighbor_graph_x(bnc::Bnc) = get_neighbor_graph_x(get_regimes_graph!(bnc; full=false))

"""
    get_neighbor_graph_qK(grh::VertexGraph; both_side=false) -> SimpleDiGraph

Return the raw qK-space neighbor graph for a vertex graph.
"""
function get_neighbor_graph_qK(grh::VertexGraph; both_side::Bool=false)::SimpleDiGraph
    _ensure_full_regimes_graph!(grh)

    bn = get_binding_network(grh)
    g = SimpleDiGraph(length(grh.neighbors))
    for (i, edges) in enumerate(grh.neighbors)
        get_nullity(bn, i) > 1 && continue
        for e in edges
            if !_edge_has_qK_interface(e) || (!both_side && e.to < i)
                continue
            end
            add_edge!(g, i, e.to)
        end
    end
    return g
end

"""
    get_neighbor_graph_qK(bnc::Bnc; kwargs...) -> SimpleDiGraph

Return the qK neighbor graph for a model.
"""
get_neighbor_graph_qK(bnc::Bnc; kwargs...) = get_neighbor_graph_qK(get_regimes_graph!(bnc; full=true); kwargs...)

"""
    get_neighbor_graph(args...; kwargs...) -> SimpleDiGraph

Alias for `get_neighbor_graph_qK`.
"""
get_neighbor_graph(args...; kwargs...) = get_neighbor_graph_qK(args...; kwargs...)


# ============================================================================
# Graph Utilities
# ============================================================================

"""
    get_sources(g::AbstractGraph) -> Set{Int}

Return source vertices with zero indegree.
"""
get_sources(g::AbstractGraph) = Set(v for v in vertices(g) if indegree(g, v) == 0)

"""
    get_sinks(g::AbstractGraph) -> Set{Int}

Return sink vertices with zero outdegree.
"""
get_sinks(g::AbstractGraph) = Set(v for v in vertices(g) if outdegree(g, v) == 0)

"""
    get_sources_sinks(g::AbstractGraph) -> (Set{Int}, Set{Int})

Return sources and sinks for a graph.
"""
get_sources_sinks(g::AbstractGraph) = (get_sources(g), get_sinks(g))

function _filter_singular_isolated_vertices!(
    sources_all::Set{Int},
    sinks_all::Set{Int},
    is_singular::F,
) where {F}
    common_vs = intersect(sources_all, sinks_all)
    filter!(is_singular, common_vs)
    sources = sort!(collect(setdiff(sources_all, common_vs)))
    sinks = sort!(collect(setdiff(sinks_all, common_vs)))
    return sources, sinks
end

"""
    get_sources_sinks(model::Bnc, g::AbstractGraph) -> (Vector{Int}, Vector{Int})

Return sources and sinks while excluding singular isolated regimes.
"""
function get_sources_sinks(model::Bnc, g::AbstractGraph)
    return _filter_singular_isolated_vertices!(
        get_sources(g),
        get_sinks(g),
        v -> get_nullity(model, v) > 0,
    )
end

"""
    get_sources_sinks(model::Bnc, connectome::AbstractMatrix{Bool}) -> (Vector{Int}, Vector{Int})

Return sources and sinks for a boolean adjacency matrix while excluding
singular isolated regimes.
"""
function get_sources_sinks(model::Bnc, connectome::AbstractMatrix{Bool})
    n = size(connectome, 1)
    size(connectome, 2) == n || error("connectome must be square, got size $(size(connectome)).")

    sources_all = Set(v for v in 1:n if !any(@view connectome[:, v]))
    sinks_all = Set(v for v in 1:n if !any(@view connectome[v, :]))
    regimes = _bind_regimes_data(model)
    return _filter_singular_isolated_vertices!(
        sources_all,
        sinks_all,
        v -> regimes[v].nullity > 0,
    )
end

"""
    _reachable_from_sources(g, sources) -> Vector{Bool}

Return a boolean mask of vertices reachable from `sources`.
"""
function _reachable_from_sources(g::AbstractGraph, sources::AbstractVector{Int})
    seen = falses(nv(g))
    stack = Int[]
    for s in sources
        if !seen[s]
            seen[s] = true
            push!(stack, s)
            while !isempty(stack)
                v = pop!(stack)
                for nb in outneighbors(g, v)
                    if !seen[nb]
                        seen[nb] = true
                        push!(stack, nb)
                    end
                end
            end
        end
    end
    return seen
end

"""
    _can_reach_sinks(g, sinks) -> Vector{Bool}

Return a boolean mask of vertices that can reach `sinks`.
"""
function _can_reach_sinks(g::AbstractGraph, sinks::AbstractVector{Int})
    seen = falses(nv(g))
    stack = Int[]
    for t in sinks
        if !seen[t]
            seen[t] = true
            push!(stack, t)
            while !isempty(stack)
                v = pop!(stack)
                for nb in inneighbors(g, v)
                    if !seen[nb]
                        seen[nb] = true
                        push!(stack, nb)
                    end
                end
            end
        end
    end
    return seen
end

struct PathEnumerationLimitExceeded <: Exception
    resource::Symbol
    maximum::Int
    observed::Int
end

Base.showerror(io::IO, err::PathEnumerationLimitExceeded) = print(
    io,
    "SISO path enumeration exceeded $(err.resource) limit $(err.maximum) " *
    "(observed at least $(err.observed))",
)

"""
    _enumerate_paths(g; sources, sinks, max_paths=nothing, max_total_nodes=nothing)
        -> Vector{Vector{Int}}

Enumerate all paths in a DAG from `sources` to `sinks`. Optional limits are
checked before each path-vector allocation so callers can put a hard bound on
both the number of returned paths and the cumulative path nodes materialized by
the dynamic program.
"""
function _enumerate_paths(
    g::AbstractGraph;
    sources::AbstractVector{Int},
    sinks::AbstractVector{Int},
    max_paths::Union{Nothing,Integer}=nothing,
    max_total_nodes::Union{Nothing,Integer}=nothing,
    cancel_check=_NO_CANCEL_CHECK,
)::Vector{Vector{Int}}
    cancel_check()
    max_paths === nothing || max_paths >= 1 ||
        throw(ArgumentError("max_paths must be positive or nothing"))
    max_total_nodes === nothing || max_total_nodes >= 1 ||
        throw(ArgumentError("max_total_nodes must be positive or nothing"))
    path_limit = max_paths === nothing ? nothing : Int(max_paths)
    node_limit = max_total_nodes === nothing ? nothing : Int(max_total_nodes)
    materialized_nodes = 0

    @info "sources: $sources"
    @info "sinks: $sinks"

    active = _reachable_from_sources(g, sources) .& _can_reach_sinks(g, sinks)
    cancel_check()
    is_sink = falses(nv(g))
    for t in sinks
        is_sink[t] = true
    end

    topo = topological_sort_by_dfs(g)
    cancel_check()
    memo = Vector{Union{Nothing,Vector{Vector{Int}}}}(undef, nv(g))
    fill!(memo, nothing)

    @info "Start enumerating paths from sources to sinks. This may take a while if there are many paths."
    @info "Total vertices to process in topological order: $(length(topo))"
    path_checkpoint_count = 0
    @showprogress for v in Iterators.reverse(topo)
        cancel_check()
        active[v] || continue
        if is_sink[v]
            node_limit !== nothing && materialized_nodes >= node_limit &&
                throw(PathEnumerationLimitExceeded(
                    :materialized_path_nodes, node_limit, materialized_nodes + 1))
            memo[v] = [[v]]
            materialized_nodes += 1
            continue
        end

        acc = Vector{Vector{Int}}()
        for nb in outneighbors(g, v)
            active[nb] || continue
            paths_nb = memo[nb]
            paths_nb === nothing && continue
            for p in paths_nb
                path_checkpoint_count += 1
                (path_checkpoint_count & 0xff) == 0 && cancel_check()
                path_limit !== nothing && length(acc) >= path_limit &&
                    throw(PathEnumerationLimitExceeded(
                        :paths, path_limit, length(acc) + 1))
                next_length = length(p) + 1
                if node_limit !== nothing && next_length > node_limit - materialized_nodes
                    throw(PathEnumerationLimitExceeded(
                        :materialized_path_nodes, node_limit, node_limit + 1))
                end
                np = Vector{Int}(undef, next_length)
                np[1] = v
                copyto!(np, 2, p, 1, length(p))
                push!(acc, np)
                materialized_nodes += next_length
            end
        end
        memo[v] = isempty(acc) ? nothing : acc
    end

    @info "Finished enumerating paths. Now collecting paths from sources. Total sources: $(length(sources))"
    out = Vector{Vector{Int}}()
    @showprogress for s in sources
        cancel_check()
        active[s] || continue
        ps = memo[s]
        ps === nothing && continue
        if path_limit !== nothing && length(ps) > path_limit - length(out)
            throw(PathEnumerationLimitExceeded(
                :paths, path_limit, path_limit + 1))
        end
        append!(out, ps)
    end

    sort!(out)
    cancel_check()
    return out
end


# ============================================================================
# SISO-Oriented Graph Construction
# ============================================================================

@inline _direction_score(dir::SparseVector{Float64,Int}, change_qK_idx::Integer) = get(dir, Int(change_qK_idx), 0.0)

function _collect_oriented_edge_pairs(
    grh::VertexGraph,
    change_qK_idx::Integer;
    tol::Float64=1e-6,
)
    _ensure_full_regimes_graph!(grh)
    regimes = _bind_regimes_data(get_binding_network(grh))
    thread_edges = [Tuple{Int,Int}[] for _ in 1:Threads.maxthreadid()]
    idx = Int(change_qK_idx)

    Threads.@threads for i in eachindex(grh.neighbors)
        regimes[i].nullity > 1 && continue
        local_edges = thread_edges[Threads.threadid()]
        for e in grh.neighbors[i]
            (!_edge_has_qK_interface(e) || e.to < i) && continue
            iface = _edge_qK_interface(grh, e)
            iface === nothing && continue
            score = _direction_score(iface[1], idx)
            if score > tol
                push!(local_edges, (i, e.to))
            elseif score < -tol
                push!(local_edges, (e.to, i))
            end
        end
    end

    return reduce(vcat, thread_edges; init=Tuple{Int,Int}[])
end

function _edge_pairs_to_connectome(
    n_vertices::Integer,
    edge_pairs::AbstractVector{<:Tuple{Int,Int}},
)::Matrix{Bool}
    connectome = falses(Int(n_vertices), Int(n_vertices))
    for (from, to) in edge_pairs
        connectome[from, to] = true
    end
    return connectome
end

function _edge_pairs_to_digraph(
    n_vertices::Integer,
    edge_pairs::AbstractVector{<:Tuple{Int,Int}},
)::SimpleDiGraph
    g = SimpleDiGraph(Int(n_vertices))
    for (from, to) in edge_pairs
        add_edge!(g, from, to)
    end
    return g
end

function _oriented_connectome(
    grh::VertexGraph,
    change_qK_idx::Integer;
    tol::Float64=1e-6,
)::Matrix{Bool}
    edge_pairs = _collect_oriented_edge_pairs(grh, change_qK_idx; tol=tol)
    return _edge_pairs_to_connectome(length(grh.neighbors), edge_pairs)
end

function _oriented_digraph(
    grh::VertexGraph,
    change_qK_idx::Integer;
    tol::Float64=1e-6,
)::SimpleDiGraph
    edge_pairs = _collect_oriented_edge_pairs(grh, change_qK_idx; tol=tol)
    return _edge_pairs_to_digraph(length(grh.neighbors), edge_pairs)
end

"""
    get_SISO_graph(grh::VertexGraph, change_qK) -> SimpleDiGraph

Build a SISO graph from a vertex graph for a chosen qK coordinate.
"""
function get_SISO_graph(grh::VertexGraph, change_qK)::SimpleDiGraph
    change_qK_idx = locate_sym_qK(get_binding_network(grh), change_qK)
    return _oriented_digraph(grh, change_qK_idx)
end

"""
    get_SISO_graph(model::Bnc, change_qK) -> SimpleDiGraph

Return a SISO graph for a chosen qK coordinate.
"""
function get_SISO_graph(model::Bnc, change_qK; cancel_check=_NO_CANCEL_CHECK)
    graph = get_regimes_graph!(model; full=true, cancel_check=cancel_check)
    cancel_check()
    oriented = get_SISO_graph(graph, change_qK)
    cancel_check()
    return oriented
end


# ============================================================================
# Polyhedron Utilities
# ============================================================================

_clean_polyhedron!(p::Polyhedron) = (detecthlinearity!(p); removehredundancy!(p); p)

"""
    Polyhedra.intersect(p::Polyhedron) -> Polyhedron

Identity overload for single-polyhedron intersections.
"""
Polyhedra.intersect(p::Polyhedron) = p

function _project_polyhedron(poly::Polyhedron, change_qK_idx::Int)::Polyhedron
    _clean_polyhedron!(poly)
    isempty(poly) && return poly
    poly = eliminate(poly, change_qK_idx)
    removehredundancy!(poly)
    return poly
end

"""
    _get_interface_prism(model, from, to, change_qK_idx) -> Polyhedron

Project the interface `poly(from) ∩ poly(to)` by eliminating `change_qK_idx`.
"""
function _get_interface_prism(
    bnc_sys::Bnc,
    vertex_idx_from::Int,
    vertex_idx_to::Int,
    change_qK_idx::Int,
)::Polyhedra.Polyhedron
    poly = intersect(get_polyhedron(bnc_sys, vertex_idx_from), get_polyhedron(bnc_sys, vertex_idx_to))
    return _project_polyhedron(poly, change_qK_idx)
end

"""
    _get_polyhedron_prism(model, vertex_idx, change_qK_idx) -> Polyhedron

Project a single regime polyhedron by eliminating `change_qK_idx`.
"""
function _get_polyhedron_prism(
    bnc_sys::Bnc,
    vertex_idx::Int,
    change_qK_idx::Int,
)::Polyhedra.Polyhedron
    return _project_polyhedron(get_polyhedron(bnc_sys, vertex_idx), change_qK_idx)
end

function _intersect_nonempty(polys::Polyhedra.Polyhedron...)::Union{Nothing,Polyhedra.Polyhedron}
    poly = intersect(polys...) |> _clean_polyhedron!
    return isempty(poly) ? nothing : poly
end


# ============================================================================
# SISO Problem / Helper
# ============================================================================

const SISOPathKey = Tuple{Vararg{Int}}
const SISOPairKey = NTuple{2,Int}
const SISOPathConditionMap = Dict{SISOPathKey,Polyhedron}
const SISOConditionSolver = Symbol

struct SISODAG
    graph::SimpleDiGraph
    sources::Vector{Int}
    sinks::Vector{Int}
    reachable::BitMatrix
end

struct SISOProblem{T}
    bn::Bnc{T}
    change_qK_idx::Int
    dag::SISODAG
end

mutable struct SISODAGProfile
    planning_ns::UInt64
    pair_solve_ns::UInt64
    middle_collect_ns::UInt64
    middle_compute_ns::UInt64
    middle_merge_ns::UInt64
    pair_solve_calls::Int
    planned_pairs::Int
    middle_parallel_nodes::Int
    middle_serial_nodes::Int
    middle_join_pairs::Int
    queue_pair_tasks::Int
    queue_chunk_tasks::Int
    queue_chunked_pairs::Int
    queue_finalize_tasks::Int
    queue_max_chunks_per_pair::Int
    queue_max_chunk_estimated_entries::Int
    queue_total_chunk_estimated_entries::Int
    queue_max_chunk_seconds::Float64
    queue_total_chunk_seconds::Float64
    queue_finalize_ns::UInt64
    queue_chunk_candidate_pairs::Int
    queue_chunk_size_gate_skips::Int
    queue_chunk_width_gate_skips::Int
    queue_chunk_thread_gate_skips::Int
    queue_estimator_entries_per_second::Float64
    queue_estimator_target_entries::Int
    queue_estimator_min_parallel_entries::Float64
    queue_estimator_target_seconds::Float64
    weighted_work_done::Float64
    weighted_work_total::Float64
    weighted_progress_units::Int
    largest_pair_seconds::Float64
    largest_pair_from::Int
    largest_pair_to::Int
    current_pair_from::Int
    current_pair_to::Int
    current_pair_branch::Symbol
    current_pair_weight::Float64
    current_pair_start_ns::UInt64
    current_pair_elapsed_seconds::Float64
    current_pair_output_entries::Int
end

SISODAGProfile() = SISODAGProfile(
    0, 0, 0, 0, 0,
    0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0,
    0, 0, 0, 0, _siso_dag_fallback_entries_per_second(), 0, 0.0, _siso_dag_target_chunk_seconds(),
    0.0, 0.0, 0,
    0.0, 0, 0,
    0, 0, :none, 0.0, UInt64(0), 0.0, 0,
)

mutable struct SISOHelper{T}
    problem::SISOProblem{T}
    vertex_prisms::Vector{Union{Nothing,Polyhedron}}
    interface_prisms::Dict{SISOPairKey,Polyhedron}
    pair_conditions::Dict{SISOPairKey,SISOPathConditionMap}
    cache_lock::ReentrantLock
    dag_profile::Union{Nothing,SISODAGProfile}
end

@inline _pair_key(from::Int, to::Int)::SISOPairKey = (from, to)
@inline _undirected_pair_key(a::Int, b::Int)::SISOPairKey = a <= b ? (a, b) : (b, a)
@inline _path_key(path::AbstractVector{<:Integer})::SISOPathKey = Tuple(Int.(path))
@inline _prepend_vertex(v::Int, key::SISOPathKey)::SISOPathKey = (v, key...)
@inline _append_vertex(key::SISOPathKey, v::Int)::SISOPathKey = (key..., v)
@inline _wrap_vertices(left::Int, key::SISOPathKey, right::Int)::SISOPathKey = (left, key..., right)

function _connectome_to_digraph(connectome::AbstractMatrix{Bool})::SimpleDiGraph
    n = size(connectome, 1)
    size(connectome, 2) == n || error("connectome must be square, got size $(size(connectome)).")
    g = SimpleDiGraph(n)
    for i in 1:n, j in 1:n
        connectome[i, j] && add_edge!(g, i, j)
    end
    return g
end

function _build_reachability(
    g::SimpleDiGraph;
    cancel_check=_NO_CANCEL_CHECK,
)::BitMatrix
    cancel_check()
    n = nv(g)
    reachable = falses(n, n)
    topo = topological_sort_by_dfs(g)

    for v in Iterators.reverse(topo)
        cancel_check()
        row_v = @view reachable[v, :]
        for nb in outneighbors(g, v)
            row_v[nb] = true
            row_nb = @view reachable[nb, :]
            @inbounds for j in 1:n
                row_v[j] |= row_nb[j]
            end
        end
    end
    cancel_check()
    return reachable
end

function _build_siso_problem(
    bnc_sys::Bnc{T},
    change_qK_idx::Integer,
    qK_grh::SimpleDiGraph,
    sources::AbstractVector{<:Integer},
    sinks::AbstractVector{<:Integer},
    ;
    cancel_check=_NO_CANCEL_CHECK,
) where {T}
    dag = SISODAG(
        qK_grh,
        sort!(Int.(collect(sources))),
        sort!(Int.(collect(sinks))),
        _build_reachability(qK_grh; cancel_check=cancel_check),
    )
    return SISOProblem{T}(bnc_sys, Int(change_qK_idx), dag)
end

function SISOHelper(problem::SISOProblem{T}) where {T}
    n_vtx = nv(problem.dag.graph)
    vertex_prisms = Vector{Union{Nothing,Polyhedron}}(undef, n_vtx)
    fill!(vertex_prisms, nothing)
    return SISOHelper{T}(
        problem,
        vertex_prisms,
        Dict{SISOPairKey,Polyhedron}(),
        Dict{SISOPairKey,SISOPathConditionMap}(),
        ReentrantLock(),
        nothing,
    )
end

function SISOHelper(
    bnc_sys::Bnc{T},
    change_qK;
    connectome=nothing,
)::SISOHelper{T} where {T}
    change_qK_idx = change_qK isa Integer ? Int(change_qK) : locate_sym_qK(bnc_sys, change_qK)
    if isnothing(connectome)
        qK_grh = get_SISO_graph(bnc_sys, change_qK_idx)
        sources, sinks = get_sources_sinks(bnc_sys, qK_grh)
    else
        connectome_bool = Matrix{Bool}(connectome)
        qK_grh = _connectome_to_digraph(connectome_bool)
        sources, sinks = get_sources_sinks(bnc_sys, connectome_bool)
    end
    return SISOHelper(_build_siso_problem(bnc_sys, change_qK_idx, qK_grh, sources, sinks))
end

get_binding_network(problem::SISOProblem, args...) = problem.bn
get_binding_network(helper::SISOHelper, args...) = get_binding_network(helper.problem)
get_SISO_graph(problem::SISOProblem) = problem.dag.graph
get_SISO_graph(helper::SISOHelper) = get_SISO_graph(helper.problem)
get_sources(problem::SISOProblem) = copy(problem.dag.sources)
get_sources(helper::SISOHelper) = get_sources(helper.problem)
get_sinks(problem::SISOProblem) = copy(problem.dag.sinks)
get_sinks(helper::SISOHelper) = get_sinks(helper.problem)
get_change_qK_idx(problem::SISOProblem) = problem.change_qK_idx
get_change_qK_idx(helper::SISOHelper) = get_change_qK_idx(helper.problem)
get_dag_profile(helper::SISOHelper) = helper.dag_profile

@inline _edge_exists(helper::SISOHelper, from::Int, to::Int) = has_edge(get_SISO_graph(helper), from, to)
@inline _pair_is_cached(helper::SISOHelper, from::Int, to::Int) =
    lock(helper.cache_lock) do
        haskey(helper.pair_conditions, _pair_key(from, to))
    end
@inline _pair_conditions(helper::SISOHelper, from::Int, to::Int) =
    lock(helper.cache_lock) do
        get(helper.pair_conditions, _pair_key(from, to), nothing)
    end
@inline _can_reach(helper::SISOHelper, from::Int, to::Int) = helper.problem.dag.reachable[from, to]

function _get_vertex_prism!(
    helper::SISOHelper,
    vertex_idx::Int,
)::Polyhedra.Polyhedron
    prism = helper.vertex_prisms[vertex_idx]
    if !isnothing(prism)
        return prism
    end

    prism = _get_polyhedron_prism(helper.problem.bn, vertex_idx, helper.problem.change_qK_idx) |> _clean_polyhedron!
    helper.vertex_prisms[vertex_idx] = prism
    return prism
end

function _get_interface_prism!(
    helper::SISOHelper,
    vertex_idx_from::Int,
    vertex_idx_to::Int,
)::Polyhedra.Polyhedron
    key = _undirected_pair_key(vertex_idx_from, vertex_idx_to)
    prism = get(helper.interface_prisms, key, nothing)
    if !isnothing(prism)
        return prism
    end

    prism = _get_interface_prism(
        helper.problem.bn,
        vertex_idx_from,
        vertex_idx_to,
        helper.problem.change_qK_idx,
    ) |> _clean_polyhedron!

    helper.interface_prisms[key] = prism
    return prism
end

function _bridge_successors(helper::SISOHelper, from::Int, to::Int)::Vector{Int}
    out = Int[]
    for successor in outneighbors(get_SISO_graph(helper), from)
        successor == to && continue
        _can_reach(helper, successor, to) || continue
        push!(out, successor)
    end
    return out
end

function _bridge_predecessors(helper::SISOHelper, from::Int, to::Int)::Vector{Int}
    out = Int[]
    for predecessor in inneighbors(get_SISO_graph(helper), to)
        predecessor == from && continue
        _can_reach(helper, from, predecessor) || continue
        push!(out, predecessor)
    end
    return out
end

function _cache_pair_conditions!(
    helper::SISOHelper,
    from::Int,
    to::Int,
    conditions::SISOPathConditionMap,
)::SISOPathConditionMap
    lock(helper.cache_lock) do
        helper.pair_conditions[_pair_key(from, to)] = conditions
    end
    return conditions
end

function _maybe_store_direct_path!(
    conditions::SISOPathConditionMap,
    helper::SISOHelper,
    from::Int,
    to::Int,
)::Nothing
    _edge_exists(helper, from, to) || return nothing
    condition = _get_interface_prism!(helper, from, to)
    isempty(condition) && return nothing
    conditions[(from, to)] = condition
    return nothing
end

function _find_pair_path_conditions!(
    helper::SISOHelper,
    from::Int,
    to::Int,
)::SISOPathConditionMap
    cached = _pair_conditions(helper, from, to)
    !isnothing(cached) && return cached

    conditions = SISOPathConditionMap()
    if from == to
        condition = _get_vertex_prism!(helper, from)
        isempty(condition) || (conditions[(from,)] = condition)
        return _cache_pair_conditions!(helper, from, to, conditions)
    end

    _maybe_store_direct_path!(conditions, helper, from, to)

    successors = _bridge_successors(helper, from, to)
    predecessors = _bridge_predecessors(helper, from, to)
    if isempty(successors) || isempty(predecessors)
        return _cache_pair_conditions!(helper, from, to, conditions)
    end

    n_solved_successors = count(successor -> _pair_is_cached(helper, successor, to), successors)
    n_solved_predecessors = count(predecessor -> _pair_is_cached(helper, from, predecessor), predecessors)
    solved_successor_ratio = n_solved_successors / length(successors)
    solved_predecessor_ratio = n_solved_predecessors / length(predecessors)

    if n_solved_successors == 0 && n_solved_predecessors == 0
        for successor in successors
            left_condition = _get_interface_prism!(helper, from, successor)
            isempty(left_condition) && continue
            for predecessor in predecessors
                right_condition = _get_interface_prism!(helper, predecessor, to)
                isempty(right_condition) && continue
                middle_conditions = _find_pair_path_conditions!(helper, successor, predecessor)
                isempty(middle_conditions) && continue
                for (middle_path, middle_condition) in middle_conditions
                    full_condition = _intersect_nonempty(left_condition, middle_condition, right_condition)
                    isnothing(full_condition) && continue
                    conditions[_wrap_vertices(from, middle_path, to)] = full_condition
                end
            end
        end
        return _cache_pair_conditions!(helper, from, to, conditions)
    end

    if solved_successor_ratio > solved_predecessor_ratio
        for successor in successors
            suffix_conditions = _find_pair_path_conditions!(helper, successor, to)
            isempty(suffix_conditions) && continue
            left_condition = _get_interface_prism!(helper, from, successor)
            isempty(left_condition) && continue
            for (suffix_path, suffix_condition) in suffix_conditions
                full_condition = _intersect_nonempty(left_condition, suffix_condition)
                isnothing(full_condition) && continue
                conditions[_prepend_vertex(from, suffix_path)] = full_condition
            end
        end
        return _cache_pair_conditions!(helper, from, to, conditions)
    end

    for predecessor in predecessors
        prefix_conditions = _find_pair_path_conditions!(helper, from, predecessor)
        isempty(prefix_conditions) && continue
        right_condition = _get_interface_prism!(helper, predecessor, to)
        isempty(right_condition) && continue
        for (prefix_path, prefix_condition) in prefix_conditions
            full_condition = _intersect_nonempty(prefix_condition, right_condition)
            isnothing(full_condition) && continue
            conditions[_append_vertex(prefix_path, to)] = full_condition
        end
    end

    return _cache_pair_conditions!(helper, from, to, conditions)
end

"""
    _find_all_path_conditions!(helper) -> SISOHelper

Solve all source-to-sink pair conditions stored in a helper, with progress.
"""
function _find_all_path_conditions!(helper::SISOHelper)::SISOHelper
    pair_queries = [(source, sink) for source in helper.problem.dag.sources for sink in helper.problem.dag.sinks]
    isempty(pair_queries) && return helper

    if length(pair_queries) == 1
        source, sink = only(pair_queries)
        _find_pair_path_conditions!(helper, source, sink)
        return helper
    end

    @info "Start finding all possible path conditions across $(length(pair_queries)) source-sink pairs."
    @showprogress dt=0.1 desc="Finding path conditions" for (source, sink) in pair_queries
        _find_pair_path_conditions!(helper, source, sink)
    end
    return helper
end

function _pair_condition_entry_count(
    helper::SISOHelper,
    pairs::AbstractVector{<:Integer},
    fixed::Int;
    use_suffix::Bool,
)::Int
    total = 0
    for vertex in pairs
        cached = use_suffix ? _pair_conditions(helper, vertex, fixed) : _pair_conditions(helper, fixed, vertex)
        isnothing(cached) && error("Missing cached pair condition while building DAG-scheduled SISO path conditions.")
        total += length(cached)
    end
    return total
end

struct SISOPairPlan
    branch::Symbol
    successors::Vector{Int}
    predecessors::Vector{Int}
    dependencies::Vector{SISOPairKey}
end

const SISO_DAG_MIDDLE_PARALLEL_THRESHOLD = 8
const SISO_DAG_PROGRESS_UNITS = 10_000

_siso_dag_scheduler() = Symbol(lowercase(get(ENV, "BNC_SISO_DAG_SCHEDULER", "auto")))
_siso_dag_layer_parallel_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_LAYER_PARALLEL", "false"))
_siso_dag_pair_queue_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_PAIR_QUEUE", "false"))
_siso_dag_chunk_queue_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_CHUNK_QUEUE", "true"))
_siso_dag_layer_inner_parallel_width() = max(0, parse(Int, get(ENV, "BNC_SISO_DAG_LAYER_INNER_PARALLEL_WIDTH", "1")))
_siso_dag_inner_parallel_min_weight() = parse(Float64, get(ENV, "BNC_SISO_DAG_INNER_PARALLEL_MIN_WEIGHT", "50000"))
_siso_dag_chunk_size_gate_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_CHUNK_SIZE_GATE", "true"))
_siso_dag_chunk_width_gate_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_CHUNK_WIDTH_GATE", "true"))
_siso_dag_chunk_thread_gate_enabled() = parse(Bool, get(ENV, "BNC_SISO_DAG_CHUNK_THREAD_GATE", "true"))
_siso_dag_target_chunk_seconds() =
    max(0.1, parse(Float64, get(ENV, "BNC_SISO_DAG_TARGET_CHUNK_SECONDS", "40")))
_siso_dag_fallback_entries_per_second() =
    max(1.0, parse(Float64, get(ENV, "BNC_SISO_DAG_FALLBACK_ENTRIES_PER_SECOND", "125")))
_siso_dag_chunk_rate_alpha() =
    min(1.0, max(0.0, parse(Float64, get(ENV, "BNC_SISO_DAG_CHUNK_RATE_ALPHA", "0.2"))))
_siso_dag_chunk_width_factor() =
    max(0.0, parse(Float64, get(ENV, "BNC_SISO_DAG_CHUNK_WIDTH_FACTOR", "2")))
_siso_dag_inner_parallel_max_pairs_per_layer() =
    max(0, parse(Int, get(ENV, "BNC_SISO_DAG_INNER_PARALLEL_MAX_PAIRS_PER_LAYER", "2")))
_siso_dag_inner_parallel_target_entries() =
    haskey(ENV, "BNC_SISO_DAG_INNER_PARALLEL_TARGET_ENTRIES") ?
    max(1, parse(Int, ENV["BNC_SISO_DAG_INNER_PARALLEL_TARGET_ENTRIES"])) :
    max(1, round(Int, _siso_dag_fallback_entries_per_second() * _siso_dag_target_chunk_seconds()))
_siso_dag_inner_parallel_max_chunks() =
    max(1, parse(Int, get(ENV, "BNC_SISO_DAG_INNER_PARALLEL_MAX_CHUNKS", string(4 * Threads.nthreads()))))

function _siso_dag_use_queue_scheduler()::Bool
    scheduler = _siso_dag_scheduler()
    scheduler === :serial && return false
    scheduler === :queue && return Threads.nthreads() > 1
    scheduler === :auto && return Threads.nthreads() > 1
    scheduler === :layer && return false
    error("Unsupported BNC_SISO_DAG_SCHEDULER=$(scheduler). Use auto, serial, or queue.")
end

function _siso_dag_use_layer_scheduler()::Bool
    _siso_dag_scheduler() === :layer && return Threads.nthreads() > 1
    return _siso_dag_layer_parallel_enabled() && Threads.nthreads() > 1
end

mutable struct SISODAGProgressState
    pair_total::Int
    pair_done::Int
    static_pair_weight::Dict{SISOPairKey,Float64}
    weighted_done::Float64
    weighted_total::Float64
    displayed_units::Int
    largest_pair_seconds::Float64
    largest_pair::SISOPairKey
    cached_condition_entries::Int
end

mutable struct SISOPairSolveStats
    pair_solve_ns::UInt64
    middle_collect_ns::UInt64
    middle_compute_ns::UInt64
    middle_merge_ns::UInt64
    middle_join_pairs::Int
    middle_parallel_nodes::Int
    middle_serial_nodes::Int
end

SISOPairSolveStats() = SISOPairSolveStats(0, 0, 0, 0, 0, 0, 0)

function _add_pair_solve_stats!(profile::SISODAGProfile, stats::SISOPairSolveStats)::Nothing
    profile.pair_solve_ns += stats.pair_solve_ns
    profile.middle_collect_ns += stats.middle_collect_ns
    profile.middle_compute_ns += stats.middle_compute_ns
    profile.middle_merge_ns += stats.middle_merge_ns
    profile.middle_join_pairs += stats.middle_join_pairs
    profile.middle_parallel_nodes += stats.middle_parallel_nodes
    profile.middle_serial_nodes += stats.middle_serial_nodes
    return nothing
end

function _static_pair_weight(plan::SISOPairPlan)::Float64
    if plan.branch === :diagonal || plan.branch === :no_bridge
        return 1.0
    end
    return Float64(max(1, length(plan.dependencies)))
end

function SISODAGProgressState(
    scheduled_pairs::AbstractVector{SISOPairKey},
    plans::Dict{SISOPairKey,SISOPairPlan},
)::SISODAGProgressState
    static_pair_weight = Dict{SISOPairKey,Float64}()
    weighted_total = 0.0
    for pair in scheduled_pairs
        weight = _static_pair_weight(plans[pair])
        static_pair_weight[pair] = weight
        weighted_total += weight
    end
    return SISODAGProgressState(
        length(scheduled_pairs),
        0,
        static_pair_weight,
        0.0,
        max(weighted_total, 1.0),
        0,
        0.0,
        (0, 0),
        0,
    )
end

@inline function _cached_pair_condition_count(helper::SISOHelper, pair::SISOPairKey)::Int
    cached = lock(helper.cache_lock) do
        get(helper.pair_conditions, pair, nothing)
    end
    return isnothing(cached) ? 0 : length(cached)
end

function _adaptive_pair_weight(helper::SISOHelper, plan::SISOPairPlan)::Float64
    if plan.branch === :diagonal || plan.branch === :no_bridge
        return 1.0
    end
    child_entries = 0
    for dependency in plan.dependencies
        child_entries += _cached_pair_condition_count(helper, dependency)
    end
    return Float64(max(1, length(plan.dependencies) + child_entries))
end

function _progress_showvalues(
    state::SISODAGProgressState,
    profile::SISODAGProfile,
)
    weighted_pct = 100 * state.weighted_done / max(state.weighted_total, 1.0)
    pair_pct = 100 * state.pair_done / max(state.pair_total, 1)
    current_pair = (profile.current_pair_from, profile.current_pair_to)
    largest_pair = state.largest_pair
    return [
        (:weighted, Printf.@sprintf("%.1f%%", weighted_pct)),
        (:pairs, "$(state.pair_done)/$(state.pair_total) ($(Printf.@sprintf("%.1f%%", pair_pct)))"),
        (:current, "$(current_pair) $(profile.current_pair_branch) $(Printf.@sprintf("%.1fs", profile.current_pair_elapsed_seconds))"),
        (:largest, "$(largest_pair) $(Printf.@sprintf("%.1fs", state.largest_pair_seconds))"),
        (:cached_entries, state.cached_condition_entries),
    ]
end

function _begin_weighted_pair!(
    state::SISODAGProgressState,
    profile::SISODAGProfile,
    helper::SISOHelper,
    pair::SISOPairKey,
    plan::SISOPairPlan,
)::Float64
    adaptive_weight = _adaptive_pair_weight(helper, plan)
    state.weighted_total += adaptive_weight - state.static_pair_weight[pair]
    from, to = pair
    profile.current_pair_from = from
    profile.current_pair_to = to
    profile.current_pair_branch = plan.branch
    profile.current_pair_weight = adaptive_weight
    profile.current_pair_start_ns = time_ns()
    profile.current_pair_elapsed_seconds = 0.0
    profile.current_pair_output_entries = 0
    return adaptive_weight
end

function _finish_weighted_pair!(
    state::SISODAGProgressState,
    profile::SISODAGProfile,
    progress::ProgressMeter.Progress,
    pair::SISOPairKey,
    weight::Float64,
    pair_seconds::Float64,
    output_entries::Int,
)::Nothing
    state.pair_done += 1
    state.weighted_done += weight
    state.cached_condition_entries += output_entries
    if pair_seconds > state.largest_pair_seconds
        state.largest_pair_seconds = pair_seconds
        state.largest_pair = pair
    end

    from, to = pair
    profile.weighted_work_done = state.weighted_done
    profile.weighted_work_total = state.weighted_total
    profile.current_pair_from = from
    profile.current_pair_to = to
    profile.current_pair_start_ns = UInt64(0)
    profile.current_pair_elapsed_seconds = pair_seconds
    profile.current_pair_output_entries = output_entries
    profile.largest_pair_seconds = state.largest_pair_seconds
    profile.largest_pair_from = state.largest_pair[1]
    profile.largest_pair_to = state.largest_pair[2]

    units = floor(Int, SISO_DAG_PROGRESS_UNITS * state.weighted_done / max(state.weighted_total, 1.0))
    state.displayed_units = max(state.displayed_units, min(SISO_DAG_PROGRESS_UNITS, units))
    profile.weighted_progress_units = state.displayed_units
    ProgressMeter.update!(progress, state.displayed_units; showvalues=_progress_showvalues(state, profile))
    return nothing
end

function _build_pair_plan!(
    helper::SISOHelper,
    plans::Dict{SISOPairKey,SISOPairPlan},
    from::Int,
    to::Int,
)::SISOPairPlan
    key = _pair_key(from, to)
    cached = get(plans, key, nothing)
    !isnothing(cached) && return cached

    if from == to
        plan = SISOPairPlan(:diagonal, Int[], Int[], SISOPairKey[])
        plans[key] = plan
        return plan
    end

    successors = _bridge_successors(helper, from, to)
    predecessors = _bridge_predecessors(helper, from, to)
    if isempty(successors) || isempty(predecessors)
        plan = SISOPairPlan(:no_bridge, successors, predecessors, SISOPairKey[])
        plans[key] = plan
        return plan
    end

    n_solved_successors = count(successor -> haskey(plans, _pair_key(successor, to)), successors)
    n_solved_predecessors = count(predecessor -> haskey(plans, _pair_key(from, predecessor)), predecessors)
    solved_successor_ratio = n_solved_successors / length(successors)
    solved_predecessor_ratio = n_solved_predecessors / length(predecessors)

    if n_solved_successors == 0 && n_solved_predecessors == 0
        dependencies = SISOPairKey[]
        plan = SISOPairPlan(:middle, successors, predecessors, dependencies)
        plans[key] = plan
        for successor in successors
            for predecessor in predecessors
                child_key = _pair_key(successor, predecessor)
                push!(dependencies, child_key)
                _build_pair_plan!(helper, plans, successor, predecessor)
            end
        end
        return plan
    end

    if solved_successor_ratio > solved_predecessor_ratio
        dependencies = SISOPairKey[]
        plan = SISOPairPlan(:suffix, successors, predecessors, dependencies)
        plans[key] = plan
        for successor in successors
            child_key = _pair_key(successor, to)
            push!(dependencies, child_key)
            _build_pair_plan!(helper, plans, successor, to)
        end
        return plan
    end

    dependencies = SISOPairKey[]
    plan = SISOPairPlan(:prefix, successors, predecessors, dependencies)
    plans[key] = plan
    for predecessor in predecessors
        child_key = _pair_key(from, predecessor)
        push!(dependencies, child_key)
        _build_pair_plan!(helper, plans, from, predecessor)
    end
    return plan
end

function _append_pair_postorder!(
    pair::SISOPairKey,
    plans::Dict{SISOPairKey,SISOPairPlan},
    seen::Set{SISOPairKey},
    out::Vector{SISOPairKey},
)::Nothing
    pair in seen && return nothing
    push!(seen, pair)
    plan = plans[pair]
    for dependency in plan.dependencies
        _append_pair_postorder!(dependency, plans, seen, out)
    end
    push!(out, pair)
    return nothing
end

function _collect_pair_plan(
    helper::SISOHelper,
    roots::AbstractVector{<:Tuple{<:Integer,<:Integer}},
)::Tuple{Dict{SISOPairKey,SISOPairPlan},Vector{SISOPairKey}}
    plans = Dict{SISOPairKey,SISOPairPlan}()
    roots_int = SISOPairKey[(Int(from), Int(to)) for (from, to) in roots]
    for (from, to) in roots_int
        _build_pair_plan!(helper, plans, from, to)
    end

    order = SISOPairKey[]
    seen = Set{SISOPairKey}()
    for root in roots_int
        _append_pair_postorder!(root, plans, seen, order)
    end
    return plans, order
end

function _pair_plan_depth!(
    depths::Dict{SISOPairKey,Int},
    pair::SISOPairKey,
    plans::Dict{SISOPairKey,SISOPairPlan},
)::Int
    cached = get(depths, pair, nothing)
    cached === nothing || return cached
    plan = plans[pair]
    depth = isempty(plan.dependencies) ? 1 : 1 + maximum(_pair_plan_depth!(depths, dep, plans) for dep in plan.dependencies)
    depths[pair] = depth
    return depth
end

function _pair_plan_layers(
    scheduled_pairs::AbstractVector{SISOPairKey},
    plans::Dict{SISOPairKey,SISOPairPlan},
)::Vector{Vector{SISOPairKey}}
    depths = Dict{SISOPairKey,Int}()
    max_depth = 0
    for pair in scheduled_pairs
        max_depth = max(max_depth, _pair_plan_depth!(depths, pair, plans))
    end

    layers = [SISOPairKey[] for _ in 1:max_depth]
    scheduled_set = Set(scheduled_pairs)
    for pair in scheduled_pairs
        push!(layers[depths[pair]], pair)
    end
    for layer in layers
        filter!(pair -> pair in scheduled_set, layer)
    end
    return layers
end

function _prewarm_pair_plan_prisms!(
    helper::SISOHelper,
    pair::SISOPairKey,
    plan::SISOPairPlan,
)::Nothing
    from, to = pair
    if plan.branch === :diagonal
        _get_vertex_prism!(helper, from)
        return nothing
    end

    if _edge_exists(helper, from, to)
        _get_interface_prism!(helper, from, to)
    end

    if plan.branch === :middle
        for successor in plan.successors
            _get_interface_prism!(helper, from, successor)
        end
        for predecessor in plan.predecessors
            _get_interface_prism!(helper, predecessor, to)
        end
    elseif plan.branch === :suffix
        for successor in plan.successors
            _get_interface_prism!(helper, from, successor)
        end
    elseif plan.branch === :prefix
        for predecessor in plan.predecessors
            _get_interface_prism!(helper, predecessor, to)
        end
    end
    return nothing
end

function _prewarm_pair_plan_layer_prisms!(
    helper::SISOHelper,
    layer::AbstractVector{SISOPairKey},
    plans::Dict{SISOPairKey,SISOPairPlan},
)::Nothing
    for pair in layer
        _prewarm_pair_plan_prisms!(helper, pair, plans[pair])
    end
    return nothing
end

function _prewarm_pair_plan_prisms!(
    helper::SISOHelper,
    scheduled_pairs::AbstractVector{SISOPairKey},
    plans::Dict{SISOPairKey,SISOPairPlan},
)::Nothing
    for pair in scheduled_pairs
        _prewarm_pair_plan_prisms!(helper, pair, plans[pair])
    end
    return nothing
end

function _merge_middle_join!(
    conditions::SISOPathConditionMap,
    helper::SISOHelper,
    from::Int,
    successor::Int,
    predecessor::Int,
    to::Int,
)::SISOPathConditionMap
    profile = helper.dag_profile
    profile !== nothing && (profile.middle_join_pairs += 1)
    start_ns = profile === nothing ? UInt64(0) : time_ns()
    left_condition = _get_interface_prism!(helper, from, successor)
    isempty(left_condition) && return conditions
    right_condition = _get_interface_prism!(helper, predecessor, to)
    isempty(right_condition) && return conditions
    middle_conditions = _pair_conditions(helper, successor, predecessor)
    middle_conditions === nothing && error("Missing cached middle condition for pair ($(successor), $(predecessor)).")
    isempty(middle_conditions) && return conditions

    for (middle_path, middle_condition) in middle_conditions
        full_condition = _intersect_nonempty(left_condition, middle_condition, right_condition)
        isnothing(full_condition) && continue
        conditions[_wrap_vertices(from, middle_path, to)] = full_condition
    end
    profile !== nothing && (profile.middle_compute_ns += time_ns() - start_ns)
    return conditions
end

function _merge_middle_join_local!(
    conditions::SISOPathConditionMap,
    helper::SISOHelper,
    from::Int,
    successor::Int,
    predecessor::Int,
    to::Int,
    stats::SISOPairSolveStats,
)::SISOPathConditionMap
    stats.middle_join_pairs += 1
    start_ns = time_ns()
    left_condition = _get_interface_prism!(helper, from, successor)
    isempty(left_condition) && return conditions
    right_condition = _get_interface_prism!(helper, predecessor, to)
    isempty(right_condition) && return conditions
    middle_conditions = _pair_conditions(helper, successor, predecessor)
    middle_conditions === nothing && error("Missing cached middle condition for pair ($(successor), $(predecessor)).")
    isempty(middle_conditions) && return conditions

    for (middle_path, middle_condition) in middle_conditions
        full_condition = _intersect_nonempty(left_condition, middle_condition, right_condition)
        isnothing(full_condition) && continue
        conditions[_wrap_vertices(from, middle_path, to)] = full_condition
    end
    stats.middle_compute_ns += time_ns() - start_ns
    return conditions
end

function _merge_middle_join_chunk_indices_local!(
    conditions::SISOPathConditionMap,
    helper::SISOHelper,
    from::Int,
    child_pairs::AbstractVector{<:Tuple{Int,Int}},
    chunk_indices::AbstractVector{Int},
    to::Int,
    stats::SISOPairSolveStats,
)::SISOPathConditionMap
    for idx in chunk_indices
        successor, predecessor = child_pairs[idx]
        _merge_middle_join_local!(conditions, helper, from, successor, predecessor, to, stats)
    end
    return conditions
end

function _middle_join_weighted_chunks(
    helper::SISOHelper,
    child_pairs::AbstractVector{<:Tuple{Int,Int}},
    target_entries::Int=_siso_dag_inner_parallel_target_entries(),
    max_chunks::Int=_siso_dag_inner_parallel_max_chunks(),
)::Vector{Vector{Int}}
    n_items = length(child_pairs)
    n_items == 0 && return Vector{Int}[]

    weights = [max(1, _cached_pair_condition_count(helper, pair)) for pair in child_pairs]
    total_weight = sum(weights; init=0)
    target_entries = max(1, target_entries)
    max_chunks = min(n_items, max(1, max_chunks))
    n_chunks = min(max_chunks, max(1, cld(total_weight, target_entries)))

    chunks = [Int[] for _ in 1:n_chunks]
    chunk_loads = zeros(Int, n_chunks)
    for idx in sortperm(weights; rev=true)
        chunk_idx = argmin(chunk_loads)
        push!(chunks[chunk_idx], idx)
        chunk_loads[chunk_idx] += weights[idx]
    end
    return filter!(!isempty, chunks)
end

function _middle_join_chunk_entry_loads(
    helper::SISOHelper,
    child_pairs::AbstractVector{<:Tuple{Int,Int}},
    chunks::AbstractVector{<:AbstractVector{Int}},
)::Vector{Int}
    weights = [max(1, _cached_pair_condition_count(helper, pair)) for pair in child_pairs]
    return [sum((weights[idx] for idx in chunk); init=0) for chunk in chunks]
end

function _queue_estimator_target_entries(profile::SISODAGProfile)::Int
    if haskey(ENV, "BNC_SISO_DAG_INNER_PARALLEL_TARGET_ENTRIES")
        return _siso_dag_inner_parallel_target_entries()
    end
    target_entries = round(Int, profile.queue_estimator_entries_per_second * _siso_dag_target_chunk_seconds())
    return max(1, target_entries)
end

function _queue_estimator_min_parallel_entries(target_entries::Int)::Float64
    return max(_siso_dag_inner_parallel_min_weight(), 4.0 * target_entries)
end

function _update_queue_chunk_rate!(
    profile::SISODAGProfile,
    estimated_entries::Int,
    chunk_seconds::Float64,
)::Nothing
    estimated_entries > 0 || return nothing
    chunk_seconds > 0 || return nothing
    sample_rate = estimated_entries / chunk_seconds
    alpha = _siso_dag_chunk_rate_alpha()
    profile.queue_estimator_entries_per_second =
        (1 - alpha) * profile.queue_estimator_entries_per_second + alpha * sample_rate
    return nothing
end

function _collect_middle_join_pairs(
    helper::SISOHelper,
    from::Int,
    to::Int,
    successors::AbstractVector{Int},
    predecessors::AbstractVector{Int},
)::Vector{Tuple{Int,Int}}
    child_pairs = Tuple{Int,Int}[]
    for successor in successors
        left_condition = _get_interface_prism!(helper, from, successor)
        isempty(left_condition) && continue
        for predecessor in predecessors
            right_condition = _get_interface_prism!(helper, predecessor, to)
            isempty(right_condition) && continue
            middle_conditions = _pair_conditions(helper, successor, predecessor)
            middle_conditions === nothing && error("Missing cached middle condition for pair ($(successor), $(predecessor)).")
            isempty(middle_conditions) && continue
            push!(child_pairs, (successor, predecessor))
        end
    end
    return child_pairs
end

function _compute_pair_plan_conditions!(
    helper::SISOHelper,
    from::Int,
    to::Int,
    plan::SISOPairPlan,
    stats::SISOPairSolveStats;
    use_inner_parallel::Bool=true,
)::SISOPathConditionMap
    conditions = SISOPathConditionMap()
    if plan.branch === :diagonal
        condition = _get_vertex_prism!(helper, from)
        isempty(condition) || (conditions[(from,)] = condition)
        return conditions
    end

    _maybe_store_direct_path!(conditions, helper, from, to)
    if plan.branch === :no_bridge
        return conditions
    end

    if plan.branch === :middle
        collect_start_ns = time_ns()
        child_pairs = _collect_middle_join_pairs(helper, from, to, plan.successors, plan.predecessors)
        stats.middle_collect_ns += time_ns() - collect_start_ns

        if use_inner_parallel && Threads.nthreads() > 1 && length(child_pairs) >= SISO_DAG_MIDDLE_PARALLEL_THRESHOLD
            stats.middle_parallel_nodes += 1
            chunks = _middle_join_weighted_chunks(helper, child_pairs)
            local_maps = [SISOPathConditionMap() for _ in eachindex(chunks)]
            local_stats = [SISOPairSolveStats() for _ in eachindex(chunks)]
            merge_start_ns = time_ns()
            Threads.@threads :dynamic for chunk_idx in eachindex(chunks)
                _merge_middle_join_chunk_indices_local!(
                    local_maps[chunk_idx],
                    helper,
                    from,
                    child_pairs,
                    chunks[chunk_idx],
                    to,
                    local_stats[chunk_idx],
                )
            end
            for local_map in local_maps
                merge!(conditions, local_map)
            end
            stats.middle_merge_ns += time_ns() - merge_start_ns
            for chunk_stats in local_stats
                stats.middle_compute_ns += chunk_stats.middle_compute_ns
                stats.middle_join_pairs += chunk_stats.middle_join_pairs
            end
        else
            stats.middle_serial_nodes += 1
            for (successor, predecessor) in child_pairs
                _merge_middle_join_local!(conditions, helper, from, successor, predecessor, to, stats)
            end
        end
        return conditions
    end

    if plan.branch === :suffix
        for successor in plan.successors
            suffix_conditions = _pair_conditions(helper, successor, to)
            suffix_conditions === nothing && error("Missing cached suffix condition for pair ($(successor), $(to)).")
            isempty(suffix_conditions) && continue
            left_condition = _get_interface_prism!(helper, from, successor)
            isempty(left_condition) && continue
            for (suffix_path, suffix_condition) in suffix_conditions
                full_condition = _intersect_nonempty(left_condition, suffix_condition)
                isnothing(full_condition) && continue
                conditions[_prepend_vertex(from, suffix_path)] = full_condition
            end
        end
        return conditions
    end

    if plan.branch === :prefix
        for predecessor in plan.predecessors
            prefix_conditions = _pair_conditions(helper, from, predecessor)
            prefix_conditions === nothing && error("Missing cached prefix condition for pair ($(from), $(predecessor)).")
            isempty(prefix_conditions) && continue
            right_condition = _get_interface_prism!(helper, predecessor, to)
            isempty(right_condition) && continue
            for (prefix_path, prefix_condition) in prefix_conditions
                full_condition = _intersect_nonempty(prefix_condition, right_condition)
                isnothing(full_condition) && continue
                conditions[_append_vertex(prefix_path, to)] = full_condition
            end
        end
        return conditions
    end

    error("Unsupported SISO pair plan branch $(plan.branch) for pair ($(from), $(to)).")
end

function _solve_pair_plan!(
    helper::SISOHelper,
    from::Int,
    to::Int,
    plan::SISOPairPlan,
)::SISOPathConditionMap
    pair_start_ns = time_ns()
    cached = _pair_conditions(helper, from, to)
    !isnothing(cached) && return cached
    profile = helper.dag_profile
    profile !== nothing && (profile.pair_solve_calls += 1)

    stats = SISOPairSolveStats()
    conditions = _compute_pair_plan_conditions!(helper, from, to, plan, stats)
    stats.pair_solve_ns += time_ns() - pair_start_ns
    profile !== nothing && _add_pair_solve_stats!(profile, stats)
    return _cache_pair_conditions!(helper, from, to, conditions)
end

function _pair_plan_dependents(
    scheduled_pairs::AbstractVector{SISOPairKey},
    plans::Dict{SISOPairKey,SISOPairPlan},
)::Tuple{Dict{SISOPairKey,Int},Vector{Vector{Int}},Vector{Int}}
    pair_index = Dict{SISOPairKey,Int}(pair => idx for (idx, pair) in enumerate(scheduled_pairs))
    dependents = [Int[] for _ in eachindex(scheduled_pairs)]
    remaining_deps = zeros(Int, length(scheduled_pairs))
    for (idx, pair) in enumerate(scheduled_pairs)
        for dependency in plans[pair].dependencies
            dep_idx = get(pair_index, dependency, nothing)
            dep_idx === nothing && error("Missing scheduled dependency $(dependency) for pair $(pair).")
            push!(dependents[dep_idx], idx)
            remaining_deps[idx] += 1
        end
    end
    return pair_index, dependents, remaining_deps
end

function _solve_pair_plan_queue!(
    helper::SISOHelper,
    plans::Dict{SISOPairKey,SISOPairPlan},
    scheduled_pairs::AbstractVector{SISOPairKey},
    progress_state::SISODAGProgressState,
    progress::ProgressMeter.Progress,
)::Nothing
    profile = helper.dag_profile
    profile === nothing && error("DAG profile must be initialized before queue solving.")

    _prewarm_pair_plan_prisms!(helper, scheduled_pairs, plans)
    _, dependents, remaining_deps = _pair_plan_dependents(scheduled_pairs, plans)
    n_pairs = length(scheduled_pairs)
    ready = Channel{Tuple{Symbol,Int,Int}}(n_pairs + Threads.nthreads())
    scheduler_lock = ReentrantLock()
    progress_lock = ReentrantLock()
    completed = Ref(0)
    ready_pair_count = Ref(0)
    n_workers = Threads.nthreads()
    pair_weights = zeros(Float64, n_pairs)
    pair_start_ns = zeros(UInt64, n_pairs)
    pair_base_conditions = Vector{Union{Nothing,SISOPathConditionMap}}(nothing, n_pairs)
    pair_stats = [SISOPairSolveStats() for _ in 1:n_pairs]
    chunk_pairs_by_pair = Vector{Any}(nothing, n_pairs)
    chunk_indices_by_pair = Vector{Any}(nothing, n_pairs)
    chunk_loads_by_pair = Vector{Any}(nothing, n_pairs)
    chunk_maps_by_pair = Vector{Any}(nothing, n_pairs)
    chunk_stats_by_pair = Vector{Any}(nothing, n_pairs)
    chunks_remaining = zeros(Int, n_pairs)

    for idx in eachindex(scheduled_pairs)
        if remaining_deps[idx] == 0
            ready_pair_count[] += 1
            put!(ready, (:pair, idx, 0))
        end
    end

    function enqueue_dependents_or_stop!(idx::Int)::Nothing
        newly_ready = Int[]
        should_stop = false
        lock(scheduler_lock) do
            completed[] += 1
            for dependent_idx in dependents[idx]
                remaining_deps[dependent_idx] -= 1
                if remaining_deps[dependent_idx] == 0
                    ready_pair_count[] += 1
                    push!(newly_ready, dependent_idx)
                end
            end
            should_stop = completed[] == n_pairs
        end
        for ready_idx in newly_ready
            put!(ready, (:pair, ready_idx, 0))
        end
        if should_stop
            for _ in 1:n_workers
                put!(ready, (:stop, 0, 0))
            end
        end
        return nothing
    end

    function finish_queue_pair!(
        idx::Int,
        pair_seconds::Float64,
        output_entries::Int,
        stats::SISOPairSolveStats;
        conditions::Union{Nothing,SISOPathConditionMap}=nothing,
        was_cached::Bool=false,
    )::Nothing
        pair = scheduled_pairs[idx]
        from, to = pair
        if !was_cached
            conditions === nothing && error("Missing computed conditions for queued pair ($(from), $(to)).")
            _cache_pair_conditions!(helper, from, to, conditions)
        end
        lock(progress_lock) do
            if !was_cached
                profile.pair_solve_calls += 1
                _add_pair_solve_stats!(profile, stats)
            end
            _finish_weighted_pair!(
                progress_state,
                profile,
                progress,
                pair,
                pair_weights[idx],
                pair_seconds,
                output_entries,
            )
        end
        enqueue_dependents_or_stop!(idx)
        return nothing
    end

    function chunk_estimator_params()::Tuple{Int,Float64}
        lock(progress_lock) do
            target_entries = _queue_estimator_target_entries(profile)
            profile.queue_estimator_target_entries = target_entries
            profile.queue_estimator_target_seconds = _siso_dag_target_chunk_seconds()
            profile.queue_estimator_min_parallel_entries = _queue_estimator_min_parallel_entries(target_entries)
            return target_entries, profile.queue_estimator_min_parallel_entries
        end
    end

    function should_chunk_pair(idx::Int, plan::SISOPairPlan, target_entries::Int, min_entries::Float64)::Bool
        _siso_dag_chunk_queue_enabled() || return false
        plan.branch === :middle || return false
        lock(progress_lock) do
            profile.queue_chunk_candidate_pairs += 1
        end
        if _siso_dag_chunk_size_gate_enabled() && pair_weights[idx] < min_entries
            lock(progress_lock) do
                profile.queue_chunk_size_gate_skips += 1
            end
            return false
        end
        if _siso_dag_chunk_width_gate_enabled()
            queued_pairs = lock(scheduler_lock) do
                ready_pair_count[]
            end
            if queued_pairs > _siso_dag_chunk_width_factor() * n_workers
                lock(progress_lock) do
                    profile.queue_chunk_width_gate_skips += 1
                end
                return false
            end
        end
        if _siso_dag_chunk_thread_gate_enabled() && cld(max(1, round(Int, pair_weights[idx])), target_entries) <= 1
            lock(progress_lock) do
                profile.queue_chunk_thread_gate_skips += 1
            end
            return false
        end
        return true
    end

    @info "Solving DAG pair plans with a global dependency queue across $(n_workers) worker threads."
    @sync for _ in 1:n_workers
        Threads.@spawn begin
            while true
                task = take!(ready)
                kind, idx, chunk_idx = task
                kind === :stop && break

                if kind === :chunk
                    pair = scheduled_pairs[idx]
                    from, to = pair
                    child_pairs = chunk_pairs_by_pair[idx]::Vector{Tuple{Int,Int}}
                    chunk_maps = chunk_maps_by_pair[idx]::Vector{Union{Nothing,SISOPathConditionMap}}
                    stats_vec = chunk_stats_by_pair[idx]::Vector{SISOPairSolveStats}
                    chunk_loads = chunk_loads_by_pair[idx]::Vector{Int}
                    chunk_indices = (chunk_indices_by_pair[idx]::Vector{Vector{Int}})[chunk_idx]
                    local_map = SISOPathConditionMap()
                    local_stats = SISOPairSolveStats()
                    chunk_start_ns = time_ns()
                    _merge_middle_join_chunk_indices_local!(
                        local_map,
                        helper,
                        from,
                        child_pairs,
                        chunk_indices,
                        to,
                        local_stats,
                    )
                    chunk_seconds = (time_ns() - chunk_start_ns) / 1e9
                    lock(scheduler_lock) do
                        chunk_maps[chunk_idx] = local_map
                        stats_vec[chunk_idx] = local_stats
                        chunks_remaining[idx] -= 1
                        chunks_remaining[idx] == 0 && put!(ready, (:finalize, idx, 0))
                    end
                    lock(progress_lock) do
                        profile.queue_chunk_tasks += 1
                        profile.queue_max_chunk_seconds = max(profile.queue_max_chunk_seconds, chunk_seconds)
                        profile.queue_total_chunk_seconds += chunk_seconds
                        _update_queue_chunk_rate!(profile, chunk_loads[chunk_idx], chunk_seconds)
                    end
                    continue
                end

                if kind === :finalize
                    finalize_start_ns = time_ns()
                    conditions = pair_base_conditions[idx]
                    conditions === nothing && (conditions = SISOPathConditionMap())
                    local_maps = chunk_maps_by_pair[idx]::Vector{Union{Nothing,SISOPathConditionMap}}
                    local_stats = chunk_stats_by_pair[idx]::Vector{SISOPairSolveStats}
                    stats = pair_stats[idx]
                    for local_map in local_maps
                        local_map === nothing && error("Missing queued chunk result for pair $(scheduled_pairs[idx]).")
                        merge!(conditions, local_map)
                    end
                    for chunk_stats in local_stats
                        stats.middle_compute_ns += chunk_stats.middle_compute_ns
                        stats.middle_join_pairs += chunk_stats.middle_join_pairs
                    end
                    pair_seconds = (time_ns() - pair_start_ns[idx]) / 1e9
                    stats.pair_solve_ns += round(UInt64, pair_seconds * 1e9)
                    lock(progress_lock) do
                        profile.queue_finalize_tasks += 1
                        profile.queue_finalize_ns += time_ns() - finalize_start_ns
                    end
                    finish_queue_pair!(idx, pair_seconds, length(conditions), stats; conditions)
                    continue
                end

                pair = scheduled_pairs[idx]
                from, to = pair
                plan = plans[pair]
                lock(scheduler_lock) do
                    ready_pair_count[] = max(0, ready_pair_count[] - 1)
                end
                lock(progress_lock) do
                    profile.queue_pair_tasks += 1
                end
                pair_weights[idx] = lock(progress_lock) do
                    _begin_weighted_pair!(progress_state, profile, helper, pair, plan)
                end

                cached = _pair_conditions(helper, from, to)
                if !isnothing(cached)
                    finish_queue_pair!(idx, 0.0, length(cached), SISOPairSolveStats(); was_cached=true)
                    continue
                end

                target_entries, min_entries = chunk_estimator_params()
                if should_chunk_pair(idx, plan, target_entries, min_entries)
                    pair_start_ns[idx] = time_ns()
                    stats = pair_stats[idx]
                    base_conditions = SISOPathConditionMap()
                    _maybe_store_direct_path!(base_conditions, helper, from, to)
                    pair_base_conditions[idx] = base_conditions
                    collect_start_ns = time_ns()
                    child_pairs = _collect_middle_join_pairs(helper, from, to, plan.successors, plan.predecessors)
                    stats.middle_collect_ns += time_ns() - collect_start_ns
                    if length(child_pairs) >= SISO_DAG_MIDDLE_PARALLEL_THRESHOLD
                        chunks = _middle_join_weighted_chunks(
                            helper,
                            child_pairs,
                            target_entries,
                            _siso_dag_inner_parallel_max_chunks(),
                        )
                        if length(chunks) > 1
                            stats.middle_parallel_nodes += 1
                            chunk_loads = _middle_join_chunk_entry_loads(helper, child_pairs, chunks)
                            chunk_pairs_by_pair[idx] = child_pairs
                            chunk_indices_by_pair[idx] = chunks
                            chunk_loads_by_pair[idx] = chunk_loads
                            chunk_maps_by_pair[idx] =
                                Vector{Union{Nothing,SISOPathConditionMap}}(nothing, length(chunks))
                            chunk_stats_by_pair[idx] = [SISOPairSolveStats() for _ in eachindex(chunks)]
                            lock(scheduler_lock) do
                                chunks_remaining[idx] = length(chunks)
                            end
                            lock(progress_lock) do
                                profile.queue_chunked_pairs += 1
                                profile.queue_max_chunks_per_pair =
                                    max(profile.queue_max_chunks_per_pair, length(chunks))
                                if !isempty(chunk_loads)
                                    profile.queue_max_chunk_estimated_entries =
                                        max(profile.queue_max_chunk_estimated_entries, maximum(chunk_loads))
                                    profile.queue_total_chunk_estimated_entries += sum(chunk_loads; init=0)
                                end
                            end
                            for chunk_task_idx in eachindex(chunks)
                                put!(ready, (:chunk, idx, chunk_task_idx))
                            end
                            continue
                        end
                    end
                end

                stats = SISOPairSolveStats()
                pair_start = time_ns()
                conditions = _compute_pair_plan_conditions!(
                    helper,
                    from,
                    to,
                    plan,
                    stats;
                    use_inner_parallel=false,
                )
                pair_seconds = (time_ns() - pair_start) / 1e9
                stats.pair_solve_ns += round(UInt64, pair_seconds * 1e9)
                finish_queue_pair!(idx, pair_seconds, length(conditions), stats; conditions)
            end
        end
    end
    return nothing
end

function _solve_pair_plan_layers!(
    helper::SISOHelper,
    plans::Dict{SISOPairKey,SISOPairPlan},
    scheduled_pairs::AbstractVector{SISOPairKey},
    progress_state::SISODAGProgressState,
    progress::ProgressMeter.Progress,
)::Nothing
    layers = _pair_plan_layers(scheduled_pairs, plans)
    profile = helper.dag_profile
    profile === nothing && error("DAG profile must be initialized before layer-parallel solving.")

    @info "Solving DAG pair plans across $(length(layers)) dependency layers with staged layer-parallel commits."
    for (layer_idx, layer) in enumerate(layers)
        isempty(layer) && continue
        _prewarm_pair_plan_layer_prisms!(helper, layer, plans)

        n_layer_pairs = length(layer)
        conditions_by_pair = Vector{Union{Nothing,SISOPathConditionMap}}(nothing, n_layer_pairs)
        stats_by_pair = [SISOPairSolveStats() for _ in 1:n_layer_pairs]
        seconds_by_pair = zeros(Float64, n_layer_pairs)
        output_entries = zeros(Int, n_layer_pairs)
        pair_weights = zeros(Float64, n_layer_pairs)
        was_cached = falses(n_layer_pairs)

        for idx in eachindex(layer)
            pair = layer[idx]
            pair_weights[idx] = _begin_weighted_pair!(progress_state, profile, helper, pair, plans[pair])
            cached = _pair_conditions(helper, pair[1], pair[2])
            if !isnothing(cached)
                was_cached[idx] = true
                output_entries[idx] = length(cached)
            end
        end

        inner_parallel_idxs = Set{Int}()
        if n_layer_pairs <= _siso_dag_layer_inner_parallel_width()
            union!(inner_parallel_idxs, eachindex(layer))
        else
            max_inner_pairs = _siso_dag_inner_parallel_max_pairs_per_layer()
            min_inner_weight = _siso_dag_inner_parallel_min_weight()
            if max_inner_pairs > 0
                candidates = Int[
                    idx for idx in eachindex(layer)
                    if !was_cached[idx] &&
                        plans[layer[idx]].branch === :middle &&
                        pair_weights[idx] >= min_inner_weight
                ]
                sort!(candidates; by=idx -> pair_weights[idx], rev=true)
                for idx in Iterators.take(candidates, max_inner_pairs)
                    push!(inner_parallel_idxs, idx)
                end
            end
        end

        for idx in sort!(collect(inner_parallel_idxs))
            pair = layer[idx]
            from, to = pair
            if was_cached[idx]
                _finish_weighted_pair!(
                    progress_state,
                    profile,
                    progress,
                    pair,
                    pair_weights[idx],
                    seconds_by_pair[idx],
                    output_entries[idx],
                )
                continue
            end

            plan = plans[pair]
            stats = stats_by_pair[idx]
            pair_start_ns = time_ns()
            conditions = _compute_pair_plan_conditions!(
                helper,
                from,
                to,
                plan,
                stats;
                use_inner_parallel=true,
            )
            pair_seconds = (time_ns() - pair_start_ns) / 1e9
            stats.pair_solve_ns += round(UInt64, pair_seconds * 1e9)
            conditions_by_pair[idx] = conditions
            seconds_by_pair[idx] = pair_seconds
            output_entries[idx] = length(conditions)
            _cache_pair_conditions!(helper, from, to, conditions)
            profile.pair_solve_calls += 1
            _add_pair_solve_stats!(profile, stats)
            _finish_weighted_pair!(
                progress_state,
                profile,
                progress,
                pair,
                pair_weights[idx],
                pair_seconds,
                output_entries[idx],
            )
        end

        outer_parallel_idxs = Int[idx for idx in eachindex(layer) if !(idx in inner_parallel_idxs)]

        Threads.@threads :dynamic for idx_idx in eachindex(outer_parallel_idxs)
            idx = outer_parallel_idxs[idx_idx]
            was_cached[idx] && continue
            pair = layer[idx]
            from, to = pair
            plan = plans[pair]
            stats = stats_by_pair[idx]
            pair_start_ns = time_ns()
            conditions = _compute_pair_plan_conditions!(
                helper,
                from,
                to,
                plan,
                stats;
                use_inner_parallel=false,
            )
            pair_seconds = (time_ns() - pair_start_ns) / 1e9
            stats.pair_solve_ns += round(UInt64, pair_seconds * 1e9)
            conditions_by_pair[idx] = conditions
            seconds_by_pair[idx] = pair_seconds
            output_entries[idx] = length(conditions)
        end

        for idx in outer_parallel_idxs
            pair = layer[idx]
            from, to = pair
            if !was_cached[idx]
                conditions = conditions_by_pair[idx]
                conditions === nothing && error("Missing computed conditions for pair ($(from), $(to)) in layer $(layer_idx).")
                _cache_pair_conditions!(helper, from, to, conditions)
                profile.pair_solve_calls += 1
                _add_pair_solve_stats!(profile, stats_by_pair[idx])
            end
            _finish_weighted_pair!(
                progress_state,
                profile,
                progress,
                pair,
                pair_weights[idx],
                seconds_by_pair[idx],
                output_entries[idx],
            )
        end
    end
    return nothing
end

"""
    _find_all_path_conditions_dag!(helper) -> SISOHelper

Selectively discover the same pair subproblems the recursive solver touches,
then solve them bottom-up. Heavy middle-join nodes are split into parallel
local join tasks while keeping pair ownership unique.
"""
function _find_all_path_conditions_dag!(
    helper::SISOHelper,
    pair_queries::AbstractVector{<:Tuple{<:Integer,<:Integer}},
)::SISOHelper
    helper.dag_profile = SISODAGProfile()
    planning_start_ns = time_ns()
    plans, scheduled_pairs = _collect_pair_plan(helper, pair_queries)
    helper.dag_profile.planning_ns += time_ns() - planning_start_ns
    helper.dag_profile.planned_pairs = length(scheduled_pairs)
    isempty(scheduled_pairs) && return helper

    if length(scheduled_pairs) == 1
        pair = only(scheduled_pairs)
        from, to = pair
        plan = plans[pair]
        pair_weight = _adaptive_pair_weight(helper, plan)
        helper.dag_profile.weighted_work_total = pair_weight
        helper.dag_profile.current_pair_from = from
        helper.dag_profile.current_pair_to = to
        helper.dag_profile.current_pair_branch = plan.branch
        helper.dag_profile.current_pair_weight = pair_weight
        helper.dag_profile.current_pair_start_ns = time_ns()
        pair_start_ns = time_ns()
        conditions = _solve_pair_plan!(helper, from, to, plan)
        pair_seconds = (time_ns() - pair_start_ns) / 1e9
        helper.dag_profile.weighted_work_done = pair_weight
        helper.dag_profile.weighted_progress_units = SISO_DAG_PROGRESS_UNITS
        helper.dag_profile.current_pair_start_ns = UInt64(0)
        helper.dag_profile.current_pair_elapsed_seconds = pair_seconds
        helper.dag_profile.current_pair_output_entries = length(conditions)
        helper.dag_profile.largest_pair_seconds = pair_seconds
        helper.dag_profile.largest_pair_from = from
        helper.dag_profile.largest_pair_to = to
        return helper
    end

    @info "Start finding all possible path conditions across $(length(scheduled_pairs)) selectively planned DAG pairs."
    progress_state = SISODAGProgressState(scheduled_pairs, plans)
    helper.dag_profile.weighted_work_total = progress_state.weighted_total
    progress = ProgressMeter.Progress(
        SISO_DAG_PROGRESS_UNITS;
        dt=1.0,
        desc="Finding path conditions (weighted)",
        showspeed=true,
    )
    ProgressMeter.update!(progress, 0; showvalues=_progress_showvalues(progress_state, helper.dag_profile))
    if _siso_dag_pair_queue_enabled() || _siso_dag_use_queue_scheduler()
        _solve_pair_plan_queue!(helper, plans, scheduled_pairs, progress_state, progress)
    elseif _siso_dag_use_layer_scheduler()
        _solve_pair_plan_layers!(helper, plans, scheduled_pairs, progress_state, progress)
    else
        for (from, to) in scheduled_pairs
            pair = (from, to)
            plan = plans[pair]
            pair_weight = _begin_weighted_pair!(progress_state, helper.dag_profile, helper, pair, plan)
            pair_start_ns = time_ns()
            conditions = _solve_pair_plan!(helper, from, to, plan)
            pair_seconds = (time_ns() - pair_start_ns) / 1e9
            _finish_weighted_pair!(
                progress_state,
                helper.dag_profile,
                progress,
                pair,
                pair_weight,
                pair_seconds,
                length(conditions),
            )
        end
    end
    ProgressMeter.finish!(progress; showvalues=_progress_showvalues(progress_state, helper.dag_profile))
    return helper
end

function _find_all_path_conditions_dag!(helper::SISOHelper)::SISOHelper
    pair_queries = Tuple{Int,Int}[(source, sink) for source in helper.problem.dag.sources for sink in helper.problem.dag.sinks]
    isempty(pair_queries) && return helper
    return _find_all_path_conditions_dag!(helper, pair_queries)
end

get_path_conditions(helper::SISOHelper, from::Integer, to::Integer) = _find_pair_path_conditions!(helper, Int(from), Int(to))


# ============================================================================
# SISOPaths
# ============================================================================

mutable struct SISOPaths{T}
    problem::SISOProblem{T}
    rgm_paths::Vector{Vector{Int}}
    condition_solver::SISOConditionSolver
    path_index::Union{Nothing,Dict{SISOPathKey,Int}}
    condition_helper::Union{Nothing,SISOHelper{T}}
    path_polys::Vector{Polyhedron}
    path_volume::Vector{Volume}
    path_volume_is_calc::BitVector
    path_polys_is_calc::BitVector

    function SISOPaths(problem::SISOProblem{T}, rgm_paths; condition_solver::Symbol=:recursive) where {T}
        rgm_paths_int = [Int.(path) for path in rgm_paths]
        n_paths = length(rgm_paths_int)
        return new{T}(
            problem,
            rgm_paths_int,
            condition_solver,
            nothing,
            nothing,
            Vector{Polyhedron}(undef, n_paths),
            Vector{Volume}(undef, n_paths),
            falses(n_paths),
            falses(n_paths),
        )
    end
end

"""
    get_binding_network(grh::SISOPaths, args...) -> Bnc

Return the model backing a SISO path object.
"""
get_binding_network(grh::SISOPaths, args...) = get_binding_network(grh.problem)
get_sources(grh::SISOPaths) = get_sources(grh.problem)
get_sinks(grh::SISOPaths) = get_sinks(grh.problem)
get_change_qK_idx(grh::SISOPaths) = get_change_qK_idx(grh.problem)

function _build_paths_dict(rgm_paths::AbstractVector{<:AbstractVector{<:Integer}})
    paths_dict = Dict{SISOPathKey,Int}()
    sizehint!(paths_dict, length(rgm_paths))
    for (i, path) in enumerate(rgm_paths)
        paths_dict[_path_key(path)] = i
    end
    return paths_dict
end

function _ensure_paths_dict!(grh::SISOPaths)
    isnothing(grh.path_index) || return grh.path_index
    grh.path_index = _build_paths_dict(grh.rgm_paths)
    return grh.path_index
end

function _normalize_path_indices(
    grh::SISOPaths,
    pth_idx::Union{Nothing,AbstractVector},
)::Vector{Int}
    return isnothing(pth_idx) ? collect(1:length(grh.rgm_paths)) : Int.(get_idx.(Ref(grh), pth_idx))
end

function _group_path_indices_by_endpoints(
    grh::SISOPaths,
    path_idxs::AbstractVector{<:Integer},
)
    groups = Dict{Tuple{Int,Int},Vector{Int}}()
    for idx in Int.(path_idxs)
        path = grh.rgm_paths[idx]
        push!(get!(groups, (first(path), last(path)), Int[]), idx)
    end
    return collect(groups)
end

function _ensure_condition_helper!(grh::SISOPaths)::SISOHelper
    if isnothing(grh.condition_helper)
        grh.condition_helper = SISOHelper(grh.problem)
    end
    return grh.condition_helper
end

function _store_pair_polyhedra!(
    grh::SISOPaths,
    helper::SISOHelper,
    from::Int,
    to::Int,
    idxs::AbstractVector{<:Integer},
)::Nothing
    pair_map = _find_pair_path_conditions!(helper, from, to)
    isempty(pair_map) && error("No feasible condition found for requested path pair ($(from), $(to)).")
    for idx in idxs
        key = _path_key(grh.rgm_paths[idx])
        poly = get(pair_map, key, nothing)
        poly === nothing && error("Requested path $(collect(key)) is missing from the shared path-condition backend.")
        grh.path_polys[idx] = poly
        grh.path_polys_is_calc[idx] = true
    end
    return nothing
end

function _ensure_path_polyhedra!(
    grh::SISOPaths,
    path_idxs::AbstractVector{<:Integer},
)::Nothing
    helper = _ensure_condition_helper!(grh)
    pair_entries = _group_path_indices_by_endpoints(grh, path_idxs)
    isempty(pair_entries) && return nothing
    needs_dag_solve = lock(helper.cache_lock) do
        isempty(helper.pair_conditions)
    end
    if grh.condition_solver === :dag && needs_dag_solve
        _find_all_path_conditions_dag!(helper, first.(pair_entries))
    end

    if length(pair_entries) == 1
        ((from, to), idxs) = only(pair_entries)
        _store_pair_polyhedra!(grh, helper, from, to, idxs)
        return nothing
    end

    @info "Start finding path conditions for $(length(path_idxs)) paths across $(length(pair_entries)) source-sink pairs."
    @showprogress dt=0.1 desc="Finding path conditions" for ((from, to), idxs) in pair_entries
        _store_pair_polyhedra!(grh, helper, from, to, idxs)
    end
    return nothing
end

"""
    _calc_polyhedra_for_path(model, paths, change_qK_idx) -> Vector{Polyhedron}

Compute qK-space polyhedra for each regime path using the shared recursive
path-condition backend.
"""
function _calc_polyhedra_for_path(
    model::Bnc,
    paths::AbstractVector{<:AbstractVector{<:Integer}},
    change_qK_idx::Integer;
    condition_solver::Symbol=:recursive,
)::Vector{Polyhedron}
    siso = SISOPaths(model, Int(change_qK_idx); rgm_paths=[Int.(path) for path in paths], condition_solver=condition_solver)
    return get_polyhedra(siso)
end

function _calc_polyhedra_for_path(
    model::Bnc,
    path::AbstractVector{<:Integer},
    change_qK,
)::Polyhedron
    change_qK_idx = change_qK isa Integer ? Int(change_qK) : locate_sym_qK(model, change_qK)
    return _calc_polyhedra_for_path(model, [Int.(path)], change_qK_idx)[1]
end

"""
    SISOPaths(model::Bnc, change_qK; rgm_paths=nothing) -> SISOPaths

Construct a `SISOPaths` object for a chosen qK coordinate.
"""
function SISOPaths(
    model::Bnc{T},
    change_qK;
    rgm_paths=nothing,
    condition_solver::Symbol=:recursive,
    max_paths::Union{Nothing,Integer}=nothing,
    max_total_nodes::Union{Nothing,Integer}=nothing,
    cancel_check=_NO_CANCEL_CHECK,
) where {T}
    cancel_check()
    change_qK_idx = locate_sym_qK(model, change_qK)

    if isnothing(rgm_paths)
        qK_grh = get_SISO_graph(model, change_qK; cancel_check=cancel_check)
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
        sources_all, sinks_all = get_sources_sinks(qK_grh)
        sources = sort!(collect(sources_all))
        sinks = sort!(collect(sinks_all))
    end

    cancel_check()
    problem = _build_siso_problem(
        model, change_qK_idx, qK_grh, sources, sinks;
        cancel_check=cancel_check,
    )
    cancel_check()
    result = SISOPaths(problem, rgm_paths; condition_solver=condition_solver)
    cancel_check()
    return result
end

"""
    get_neighbor_graph_qK(grh::SISOPaths; kwargs...) -> SimpleDiGraph

Return the qK neighbor graph for a SISO path object.
"""
get_neighbor_graph_qK(grh::SISOPaths; kwargs...) = get_SISO_graph(grh.problem)

"""
    get_SISO_graph(grh::SISOPaths) -> SimpleDiGraph

Return the SISO graph stored in a `SISOPaths` object.
"""
get_SISO_graph(grh::SISOPaths) = get_SISO_graph(grh.problem)

"""
    get_path(grh::SISOPaths, pth_idx; return_idx=false) -> Vector

Return a path by index, optionally as vertex indices.
"""
function get_path(grh::SISOPaths, pth_idx::Integer; return_idx::Bool=false)
    rgm_idxs = grh.rgm_paths[pth_idx]
    return return_idx ? rgm_idxs : get_perm.(Ref(get_binding_network(grh)), rgm_idxs)
end

"""
    get_path(grh::SISOPaths, pth::AbstractVector; return_idx=false) -> Vector

Normalize a path representation to indices or permutations.
"""
function get_path(grh::SISOPaths, pth::AbstractVector; return_idx::Bool=false)
    bn = get_binding_network(grh)
    return return_idx ? get_idx.(Ref(bn), pth) : get_perm.(Ref(bn), pth)
end

"""
    get_C_C0_nullity_qK(grh::SISOPaths, pth_idx) -> (Matrix, Vector, Int)

Return constraints for a SISO path polyhedron.
"""
get_C_C0_nullity_qK(grh::SISOPaths, pth_idx) = get_C_C0_nullity(get_polyhedron(grh, pth_idx))

"""
    get_idx(grh::SISOPaths, pth) -> Int

Return the index for a SISO path specification.
"""
function get_idx(grh::SISOPaths, pth::AbstractVector)
    idxs = get_idx.(Ref(get_binding_network(grh)), pth)
    return _ensure_paths_dict!(grh)[_path_key(idxs)]
end

"""
    get_idx(grh::SISOPaths, pth::Integer) -> Int

Return the provided path index.
"""
get_idx(grh::SISOPaths, pth::Integer) = pth

"""
    get_polyhedra(grh::SISOPaths, pth_idx=nothing) -> Vector{Polyhedron}

Return polyhedra for selected SISO paths.
"""
function get_polyhedra(grh::SISOPaths, pth_idx::Union{AbstractVector,Nothing}=nothing)::Vector{Polyhedron}
    selected_idxs = _normalize_path_indices(grh, pth_idx)
    pending = filter(idx -> !grh.path_polys_is_calc[idx], selected_idxs)
    isempty(pending) || _ensure_path_polyhedra!(grh, pending)
    return grh.path_polys[selected_idxs]
end

"""
    get_polyhedron(grh::SISOPaths, pth) -> Polyhedron

Return the polyhedron for a single SISO path.
"""
get_polyhedron(grh::SISOPaths, pth) = get_polyhedra(grh, [get_idx(grh, pth)])[1]

function _prepare_rebase_matrix(grh::SISOPaths; rebase_K::Bool=false, rebase_mat=nothing)
    if !isnothing(rebase_mat)
        @assert !rebase_K "Cannot specify both rebase_K and providing rebase_mat"
        return rebase_mat
    end
    if rebase_K
        bn = get_binding_network(grh)
        Q = rebase_mat_lgK(bn.N)
        return blockdiag(spdiagm(fill(Rational(1), bn.d - 1)), Q)
    end
    return nothing
end

"""
    get_volumes(grh::SISOPaths, pth_idx=nothing; kwargs...) -> Vector{Volume}

Compute volumes for SISO paths. Only the unrebased estimator with no explicit
estimator keywords uses the legacy single-value path cache. Rebased or
customized estimates are computed ephemerally.
"""
function get_volumes(
    grh::SISOPaths,
    pth_idx::Union{AbstractVector,Nothing}=nothing;
    rebase_K=false,
    rebase_mat=nothing,
    recalculate=false,
    kwargs...,
)
    selected_idxs = _normalize_path_indices(grh, pth_idx)
    unique_selected_idxs = unique(selected_idxs)
    use_volume_cache = _volume_request_is_cacheable(rebase_K, rebase_mat, kwargs)
    pending = (!use_volume_cache || recalculate) ? unique_selected_idxs :
        filter(idx -> !grh.path_volume_is_calc[idx], unique_selected_idxs)

    if !isempty(pending)
        polys = get_polyhedra(grh, pending)
        rebasing = _prepare_rebase_matrix(grh; rebase_K=rebase_K, rebase_mat=rebase_mat)
        volumes = calc_volume(polys; rebase_mat=rebasing, kwargs...)
        if use_volume_cache
            for (i, idx) in enumerate(pending)
                grh.path_volume[idx] = volumes[i]
                grh.path_volume_is_calc[idx] = true
            end
        else
            result_by_path = Dict(zip(pending, volumes))
            return [result_by_path[idx] for idx in selected_idxs]
        end
    end

    use_volume_cache || return Volume[]
    return grh.path_volume[selected_idxs]
end

"""
    get_volume(grh::SISOPaths, pth; kwargs...) -> Volume

Return the volume for a single SISO path.
"""
get_volume(grh::SISOPaths, pth; kwargs...) = get_volumes(grh, [get_idx(grh, pth)]; kwargs...)[1]


# ============================================================================
# Path Inspection
# ============================================================================

"""
    show_regime_path(grh::SISOPaths, pth) -> nothing

Print a formatted regime path with optional volume.
"""
function show_regime_path(grh::SISOPaths, pth)
    pth_idx = get_idx(grh, pth)
    path = get_path(grh, pth_idx; return_idx=true)
    volume = grh.path_volume_is_calc[pth_idx] ? grh.path_volume[pth_idx] : nothing
    print_path(path; prefix="#", id=pth_idx, volume=volume)
    return nothing
end

"""
    get_expression_path(grh::SISOPaths, pth; observe_x=nothing) -> (Vector, Vector)

Return expression coefficients and interfaces along a SISO path.
"""
function get_expression_path(grh::SISOPaths, pth; observe_x=nothing)
    bn = get_binding_network(grh)
    rgm_path = get_path(grh, pth; return_idx=true)
    rgm_nullities = get_nullities(bn, rgm_path)

    change_qK_idx = get_change_qK_idx(grh)
    observe_x_idx = isnothing(observe_x) ? (1:bn.n) : locate_sym_x.(Ref(bn), observe_x)
    rgm_interface = get_interface.(Ref(bn), rgm_path[1:end-1], rgm_path[2:end])

    H_H0 = Vector{Any}(undef, length(rgm_path))
    for i in eachindex(rgm_path)
        rgm = rgm_path[i]
        nlt = rgm_nullities[i]
        if nlt == 0
            H, H0 = get_H_H0(bn, rgm)
            H_H0[i] = (H[observe_x_idx, :], H0[observe_x_idx])
        elseif nlt == 1
            H_H0[i] = (get_H(bn, rgm)[observe_x_idx, change_qK_idx], nothing)
        else
            error("Nullity > 1 is not supported for expression path.")
        end
    end
    return H_H0, rgm_interface
end


# ============================================================================
# Reaction Orders
# ============================================================================

"""
    _calc_RO_for_single_path(model, path, change_qK_idx, observe_x_idx) -> Vector

Compute the reaction-order profile along a single path.
"""
function _calc_RO_for_single_path(
    model,
    path::AbstractVector{<:Integer},
    change_qK_idx,
    observe_x_idx,
    ;
    cancel_check=_NO_CANCEL_CHECK,
)::Vector{<:Real}
    cancel_check()
    r_ord = Vector{Float64}(undef, length(path))
    for i in eachindex(path)
        (Int(i) & 0xff) == 0 && cancel_check()
        if !is_singular(model, path[i])
            r_ord[i] = round(Float64(get_H(model, path[i])[observe_x_idx, change_qK_idx]); digits=3)
        else
            ord = get_H(model, path[i])[observe_x_idx, change_qK_idx]
            r_ord[i] = abs(ord) < 1e-6 ? NaN : Float64(ord) * Inf
        end
    end
    cancel_check()
    return r_ord
end

"""
    _dedup(ord_path) -> Vector

Deduplicate consecutive reaction-order values while preserving discontinuities.
"""
function _dedup(ord_path::AbstractVector{T})::Vector{T} where {T<:Real}
    isempty(ord_path) && return T[]
    out = T[ord_path[1]]
    pending_nan = false
    last_out = out[1]
    @assert !isnan(last_out) "The first element cannot be NaN for deduplication."

    for x in @view ord_path[2:end]
        if isnan(x)
            pending_nan = true
            continue
        end
        if x != last_out
            if pending_nan
                push!(out, NaN)
                pending_nan = false
            end
            push!(out, x)
            last_out = x
        else
            pending_nan = false
        end
    end
    return out
end

"""
    get_RO_path(model::Bnc, rgm_idx_shift_pth; change_qK, observe_x, kwargs...) -> Vector

Calculate the reaction-order profile for a single regime path.
"""
function get_RO_path(
    model::Bnc,
    rgm_idx_shift_pth::AbstractVector;
    change_qK,
    observe_x,
    deduplicate::Bool=false,
    keep_singular::Bool=true,
    keep_nonasymptotic::Bool=true,
    cancel_check=_NO_CANCEL_CHECK,
)::Vector{<:Real}
    cancel_check()
    rgm_idx_shift_pth = get_idx.(Ref(model), rgm_idx_shift_pth)

    ord_path = _calc_RO_for_single_path(
        model,
        rgm_idx_shift_pth,
        locate_sym_qK(model, change_qK),
        locate_sym_x(model, observe_x),
        cancel_check=cancel_check,
    )

    mask = _get_mask(
        model,
        rgm_idx_shift_pth;
        singular=keep_singular ? nothing : false,
        asymptotic=keep_nonasymptotic ? nothing : true,
    )
    ord_path = ord_path[mask]

    return deduplicate ? _dedup(ord_path) : ord_path
end

function _ensure_ro_regimes_materialized!(
    model::Bnc,
    rgm_idx_for_each_paths::AbstractVector{<:AbstractVector{<:Integer}},
    ;
    cancel_check=nothing,
)
    seen = Set{Int}()
    ordered_idxs = Int[]
    for path in rgm_idx_for_each_paths, idx in path
        cancel_check === nothing || cancel_check()
        idx = Int(idx)
        if !(idx in seen)
            push!(ordered_idxs, idx)
            push!(seen, idx)
        end
    end
    for idx in ordered_idxs
        cancel_check === nothing || cancel_check()
        get_regime(model, idx; inv_info=true)
    end
    return nothing
end

@inline _use_parallel_ro_paths(cancel_check) =
    cancel_check === nothing || cancel_check === _NO_CANCEL_CHECK

"""
    get_RO_paths(model::Bnc, rgm_paths, args...; kwargs...) -> Vector{Vector}

Calculate reaction-order profiles for multiple regime paths.
"""
function get_RO_paths(
    model::Bnc,
    rgm_paths::AbstractVector{<:AbstractVector},
    args...;
    cancel_check=nothing,
    kwargs...,
)::Vector{Vector{<:Real}}
    rgm_idx_for_each_paths = rgm_paths .|> path -> get_idx.(Ref(model), path)
    _ensure_ro_regimes_materialized!(
        model, rgm_idx_for_each_paths; cancel_check=cancel_check)

    ord_for_each_paths = Vector{Vector{<:Real}}(undef, length(rgm_idx_for_each_paths))
    if _use_parallel_ro_paths(cancel_check)
        Threads.@threads for i in eachindex(rgm_idx_for_each_paths)
            ord_for_each_paths[i] = get_RO_path(
                model, rgm_idx_for_each_paths[i], args...; kwargs...)
        end
    else
        # Cancellation callbacks can throw typed caller exceptions. Keep the
        # cancellable path on the parent task so LocalJobCancelled is not
        # wrapped in TaskFailedException/CompositeException.
        for i in eachindex(rgm_idx_for_each_paths)
            cancel_check()
            ord_for_each_paths[i] = get_RO_path(
                model, rgm_idx_for_each_paths[i], args...;
                cancel_check=cancel_check, kwargs...)
        end
        cancel_check()
    end
    return ord_for_each_paths
end

"""
    get_RO_paths(model::SISOPaths, pth_idx=nothing; observe_x, kwargs...) -> Vector{Vector}

Calculate reaction-order profiles for paths in a `SISOPaths` object.
"""
function get_RO_paths(
    model::SISOPaths,
    pth_idx::Union{Nothing,AbstractVector}=nothing;
    observe_x,
    kwargs...,
)
    selected_idxs = _normalize_path_indices(model, pth_idx)
    rgm_paths = model.rgm_paths[selected_idxs]
    observe_x_idx = locate_sym_x(get_binding_network(model), observe_x)
    return get_RO_paths(
        get_binding_network(model),
        rgm_paths;
        change_qK=get_change_qK_idx(model),
        observe_x=observe_x_idx,
        kwargs...,
    )
end

"""
    get_RO_path(model::SISOPaths, pth_idx, args...; kwargs...) -> Vector

Single-path wrapper for `get_RO_paths`.
"""
get_RO_path(model::SISOPaths, pth_idx, args...; kwargs...) = get_RO_paths(model, [get_idx(model, pth_idx)], args...; kwargs...)[1]


# ============================================================================
# Summaries
# ============================================================================

"""
    summary(grh::SISOPaths; show_volume=true, prefix="#", kwargs...) -> nothing

Print the paths stored in `SISOPaths`, optionally with volumes.
"""
function summary(grh::SISOPaths; show_volume::Bool=true, prefix::AbstractString="#", kwargs...)
    if show_volume
        print_paths(grh.rgm_paths; prefix=prefix, volumes=get_volumes(grh; kwargs...), ids=1:length(grh.rgm_paths))
    else
        print_paths(grh.rgm_paths; prefix=prefix, ids=1:length(grh.rgm_paths))
    end
    return nothing
end

"""
    summary_RO_path(grh::SISOPaths; observe_x, show_volume=true, kwargs...) -> nothing

Summarize reaction-order paths grouped by profile.
"""
function summary_RO_path(
    grh::SISOPaths;
    observe_x,
    show_volume::Bool=true,
    deduplicate::Bool=true,
    keep_singular::Bool=true,
    keep_nonasymptotic::Bool=true,
    kwargs...,
)
    ord_paths = get_RO_paths(
        grh;
        observe_x=observe_x,
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic,
    )

    volumes = show_volume ? get_volumes(grh; kwargs...) : fill(nothing, length(grh.rgm_paths))
    grouped = group_sum(ord_paths, volumes)

    ids = getindex.(grouped, 1)
    ords = getindex.(grouped, 2)
    vols = getindex.(grouped, 3)
    print_paths(ords; prefix="", ids=ids, volumes=vols)
    return nothing
end
