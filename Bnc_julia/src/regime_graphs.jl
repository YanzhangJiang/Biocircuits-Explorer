#-----------------------------------------------------------------------------------------------
#This is graph associated functions for Bnc models and archetyple behaviors associated code
#-----------------------------------------------------------------------------------------------
"""
    _calc_vertices_graph(bnc::Bnc) -> VertexGraph

Build a `VertexGraph` from vertex permutations, connecting vertices that differ
in exactly one row.
"""
function  _calc_vertices_graph(Bnc::Bnc{T}) where {T} # optimized by GPT-5, not fullly understood yet.
    perms = Bnc.vertices_perm
    n = Bnc.n    
    L = Bnc.L

    n_vtxs = length(perms)
    d=length(perms[1])
    thread_edges = [Vector{Tuple{Int, VertexEdge{T}}}() for _ in 1:Threads.maxthreadid()]

    # 按行分桶：key 为去掉该行后的签名（Tuple），值为该签名下的 (顶点索引, 该行取值)
    @showprogress for i in 1:d
        buckets = Dict{Tuple{Vararg{T}}, Vector{Tuple{Int,T}}}()

        # 构建桶
        @inbounds for q in 1:n_vtxs
            v = perms[q]
            sig = if i == 1
                    Tuple(v[2:end])
                elseif i == d
                    Tuple(v[1:end-1])
                else
                    Tuple((v[1:i-1]..., v[i+1:end]...))
                end
            push!(get!(buckets, sig) do
                Vector{Tuple{Int,T}}()
            end, (q, v[i]))
        end

        groups = collect(values(buckets))

        # 并行生成边：同桶内所有不同取值的顶点两两相连
        Threads.@threads for gi in 1:length(groups)
            tid = Threads.threadid()
            local_edges = thread_edges[tid]
            group = groups[gi]  # ::Vector{Tuple{Int,T}}
            m = length(group)
            m <= 1 && continue

            @inbounds for a in 1:m-1
                p1, j1 = group[a]
                for b in a+1:m
                    p2, j2 = group[b]
                    j1 == j2 && continue

                    dx = if j1 < j2   # go from p2 to p1, decrease x_{j2}, increase x_{j1}
                        SparseVector(n, [j1, j2], Int8[1, -1]) 
                    else
                         SparseVector(n, [j2, j1], Int8[-1, 1])
                    end

                    ins_x = log10(L[i, j1]) - log10(L[i, j2]) # go from p2 to p1

                    push!(local_edges, (p2, VertexEdge(p1, i, dx, ins_x))) # p2 to p1,
                    push!(local_edges, (p1, VertexEdge(p2, i, -dx, -ins_x)))  # p1 to p2
                end
            end
        end
    end

    # 归并线程本地边
    all_edges = reduce(vcat, thread_edges; init=Tuple{Int, VertexEdge{T}}[])
    neighbors = [Vector{VertexEdge{T}}() for _ in 1:n_vtxs]
    for (from, e) in all_edges
        push!(neighbors[from], e)
    end
    return VertexGraph(Bnc, neighbors)
end


"""
    _fulfill_vertices_graph!(vtx_graph::VertexGraph) -> nothing

Compute qK-space change directions for edges in the vertex graph.
"""
function _fulfill_vertices_graph!(vtx_graph::VertexGraph)
    Bnc = vtx_graph.bn
    """
    fill the qK space change dir matrix for all vertices in Bnc.
    """
    function _calc_change_dir_qK(Bnc, p1, p2, i, j1, j2, ins_x)
        n1 = get_nullity(Bnc, p1)
        n2 = get_nullity(Bnc, p2)
        if n1 > 1 || n2 > 1
            return (nothing, nothing)
        end

        if n1 == 0 || n2 == 0
            p = (n1 == 0) ? p1 : p2
            H, H0 = get_H_H0(Bnc, p)
            dir = H[j2, :] .- H[j1, :]
            ins_qK = H0[j2] - H0[j1] + ins_x
        else
            H  = get_H(Bnc, p1)
            M0 = get_M0(Bnc, p1)
            dir = H[j2, :] .- H[j1, :]
            ins_qK = -dot(dir, M0)
        end

        droptol!(dir, 1e-10)
        return nnz(dir) == 0 ? (nothing, nothing) : (dir, ins_qK)
    end

    # pre compute H for all vertices with nullity 0 or 1
    Threads.@threads for idx in eachindex(vtx_graph.neighbors)
        if Bnc.vertices_nullity[idx] <= 1
            get_H(Bnc, idx)
        end
    end

    @showprogress Threads.@threads for p1 in eachindex(vtx_graph.neighbors)
        edges = vtx_graph.neighbors[p1]
        if Bnc.vertices_nullity[p1] > 1 # jump off those regimes with nullity >1
            continue
        end
        for e in edges
            if !isnothing(e.change_dir_qK) # pass if have been computed
                continue
            end
            # from p1 to p2, and change happens on ith row that "1" goes from j1 position to j2 position.
            p2 = e.to # target 
            ins_x = e.intersect_x
            i = e.diff_r # different row
            (j1,j2) = let 
                I,V = findnz(e.change_dir_x) # should be two elements
                V[1] > V[2] ? (I[2], I[1]) : (I[1], I[2])
            end
            # calculate their direction based on formula
            (e.change_dir_qK, e.intersect_qK) = _calc_change_dir_qK(Bnc, p1, p2, i, j1,j2, ins_x)
        end
    end
    return nothing
end

#---------------------------------------------------------------------------------------------------
#             Helper functions: Functions for construct the regime graph paths
#----------------------------------------------------------------------------------------------------


"""
    get_sources(g::AbstractGraph) -> Set{Int}

Return source vertices with zero indegree.
"""
get_sources(g::AbstractGraph) = Set(v for v in vertices(g) if indegree(g, v) == 0)
"""
    get_sinks(g::AbstractGraph) -> Set{Int}

Return sink vertices with zero outdegree.
"""
get_sinks(g::AbstractGraph)   = Set(v for v in vertices(g) if outdegree(g, v) == 0)
"""
    get_sources_sinks(g::AbstractGraph) -> (Set{Int}, Set{Int})

Return sources and sinks for a graph.
"""
get_sources_sinks(g::AbstractGraph) = (get_sources(g), get_sinks(g))

"""
    get_sources_sinks(model::Bnc, g::AbstractGraph) -> (Vector{Int}, Vector{Int})

Return sources and sinks while excluding singular regimes.
"""
function get_sources_sinks(model::Bnc, g::AbstractGraph)
    sources_all = get_sources(g) 
    sinks_all   = get_sinks(g) 
    common_vs = intersect(sources_all, sinks_all)
    filter!(common_vs) do v
        get_nullity(model, v) > 0
    end
    sources = setdiff(sources_all, common_vs)
    sinks = setdiff(sinks_all, common_vs)
    return (collect(sources), collect(sinks))
end

# 只遍历子图：sources 可达 & 能到 sinks
"""
    _reachable_from_sources(g::AbstractGraph, sources) -> Vector{Bool}

Return a boolean mask of vertices reachable from sources.
"""
function _reachable_from_sources(g::AbstractGraph, sources::AbstractVector{Int})
    n = nv(g)
    seen = falses(n)
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
    _can_reach_sinks(g::AbstractGraph, sinks) -> Vector{Bool}

Return a boolean mask of vertices that can reach sinks.
"""
function _can_reach_sinks(g::AbstractGraph, sinks::AbstractVector{Int})
    n = nv(g)
    seen = falses(n)
    stack = Int[]
    for t in sinks
        if !seen[t]
            seen[t] = true
            push!(stack, t)
            while !isempty(stack)
                v = pop!(stack)
                for nb in inneighbors(g, v)   # 反向走
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
    _enumerate_paths(g; sources, sinks) -> Vector{Vector{Int}}

Enumerate all paths in a DAG from `sources` to `sinks`.
"""
function _enumerate_paths(
    g::AbstractGraph;
    sources::AbstractVector{Int},
    sinks::AbstractVector{Int},
)::Vector{Vector{Int}}

    @info "sources: $sources"
    @info "sinks: $sinks"
    n = nv(g)

    # 剪枝：只处理相关子图
    fromS = _reachable_from_sources(g, sources)
    toT   = _can_reach_sinks(g, sinks)
    active = fromS .& toT

    is_sink = falses(n)
    @inbounds for t in sinks
        is_sink[t] = true
    end

    # 拓扑排序（DAG）
    topo = topological_sort_by_dfs(g)   # Graphs.jl
    # memo[v] = Vector{Vector{Int}} 或 nothing
    memo = Vector{Union{Nothing, Vector{Vector{Int}}}}(undef, n)
    fill!(memo, nothing)

    @info "Start enumerating paths from sources to sinks. This may take a while if there are many paths."
    # 逆拓扑：先算子节点，再算父节点

    @info "Total vertices to process in topological order: $(length(topo))"
    @showprogress for v in Iterators.reverse(topo)
        active[v] || continue

        if is_sink[v]
            memo[v] = Vector{Vector{Int}}(undef, 1)
            memo[v][1] = [v]
            continue
        end

        # 收集所有 nb 的路径，并在前面加 v
        acc = Vector{Vector{Int}}()
        # 你也可以在这里做 sizehint!（需要先统计 path 数量，会多一次循环；看你取舍）
        for nb in outneighbors(g, v)
            active[nb] || continue
            paths_nb = memo[nb]
            paths_nb === nothing && continue
            for p in paths_nb
                L = length(p)
                np = Vector{Int}(undef, L + 1)
                np[1] = v
                @inbounds copyto!(np, 2, p, 1, L)
                push!(acc, np)
            end
        end

        memo[v] = isempty(acc) ? nothing : acc
    end

    # 汇总 sources 的结果
    @info "Finished enumerating paths. Now collecting paths from sources. Total sources: $(length(sources))"
    out = Vector{Vector{Int}}()
    @showprogress for s in sources
        active[s] || continue
        ps = memo[s]
        ps === nothing && continue
        append!(out, ps)
    end

    sort!(out)
    return out
end



_clean_polyhedron_if_possible(p) = p isa Polyhedron ? (detecthlinearity!(p); removehredundancy!(p); p) : p

"""
    _calc_polyhedra_for_path(model::Bnc, paths, change_qK_indices) -> Vector

Compute qK-space polyhedra for each regime path. Paths whose feasible region is
empty keep the empty-set object instead of throwing during conversion.
"""
function _calc_polyhedra_for_path(
    model::Bnc,
    paths::AbstractVector{<:AbstractVector{<:Integer}},
    change_qK_indices::AbstractVector{<:Integer},
)::Vector{Any}

    el_dim = BitSet(Int[idx for idx in change_qK_indices])
    projected_cleanup = length(change_qK_indices) == 1

    #dict: node: polyhedron 
    node_polyhedra = let
                        unique_rgms = unique(vcat(paths...))
                        dic = Dict{Int,Polyhedron}()
                        for r in unique_rgms
                            pr = _clean_polyhedron_if_possible(get_polyhedron(model, r))
                            dic[Int(r)] = pr        
                        end
                        dic
                    end
    # -------------------------
    # 2) Build unique undirected edges and edge index map
    # key = (min(u,v), max(u,v))
    # -------------------------
    
    #dict: (u,v): edge_idx
    (edges, edge_dict) = let
        edges = Tuple{Int,Int}[]
        edge_dict = Dict{Tuple{Int,Int},Int}()
        for path in paths
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

    # -------------------------
    # 3) Compute poly for each edge = intersect(poly_of[u], poly_of[v])
    # -------------------------

    edge_poly = let 
        edge_poly = Vector{Any}(undef, length(edge_dict))
        @info "Start building polyhedra for edges (total: $(length(edge_dict)))"
        @showprogress Threads.@threads  for i in eachindex(edges)
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

    

    out = Vector{Any}(undef, length(edge_paths))
    @info "Start building polyhedra for paths (total: $(length(edge_paths)))"
    @showprogress Threads.@threads for i in eachindex(edge_paths)
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
    Polyhedra.intersect(p::Polyhedron) -> Polyhedron

Identity overload for single-polyhedron intersections.
"""
Polyhedra.intersect(p::Polyhedron)= p # a fix for above function for if only one edge, no need to intersect


"""
    _ensure_full_vertices_graph!(grh::VertexGraph) -> nothing

Ensure qK change directions are computed for a vertex graph.
"""
function _ensure_full_vertices_graph!(grh::VertexGraph)
    if !grh.change_dir_qK_computed
        @info "Calculating vertices neighbor graph with qK change dir"
        _fulfill_vertices_graph!(grh)
        grh.change_dir_qK_computed = true
    end
    return nothing
end

change_qK_indices(grh::SISOPaths) = Int[Int(grh.change_qK_idx)]
change_qK_indices(grh::ChangePaths) = Int[Int(idx) for idx in grh.change_qK_indices]

change_qK_signs(::SISOPaths) = Int8[1]
change_qK_signs(grh::ChangePaths) = Int8[Int8(sign) for sign in grh.change_qK_signs]

change_label(grh::SISOPaths) = string(qK_sym(grh.bn)[grh.change_qK_idx])
change_label(grh::ChangePaths) = grh.change_label

change_kind(::SISOPaths) = :axis
change_kind(grh::ChangePaths) = grh.change_kind

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




#---------------------------------------------------------------------------
#              Binding Network Graph
#-------------------------------------------------------------------------
"""
    get_binding_network_grh(bnc::Bnc) -> SimpleGraph

Build the bipartite binding network graph between q and x symbols.
"""
function get_binding_network_grh(Bnc::Bnc)::SimpleGraph
    g = SimpleGraph(Bnc.d + Bnc.n)
    for vi in eachindex(Bnc._valid_L_idx)
        for vj in Bnc._valid_L_idx[vi]
            add_edge!(g, vi, vj+Bnc.d)
        end
    end
    return g # get first d nodes as total, last n nodes as x
end




#------------------------------------------------------------------------------
#                  Getting the Graph of of regimes
#----------------------------------------------------------------------------
"""
    get_vertices_graph!(bnc::Bnc; full=false) -> VertexGraph

Ensure the vertex graph is built; when `full=true`, also compute qK change directions.
"""
function get_vertices_graph!(Bnc::Bnc; full::Bool=false)::VertexGraph

    initalize_vertices_graph!(Bnc) = let
        find_all_vertices!(Bnc)# Ensure vertices are calculated
        @info "Start calculating vertices neighbor graph, It may takes a while."
        Bnc.vertices_graph =  _calc_vertices_graph(Bnc)
        nothing
    end

    if full
        vtx_graph = get_vertices_graph!(Bnc; full=false)
        _ensure_full_vertices_graph!(vtx_graph)
    else
        if isnothing(Bnc.vertices_graph)
            initalize_vertices_graph!(Bnc)
        end
    end

    return Bnc.vertices_graph
end


"""
    get_edge(grh::VertexGraph, from, to; full=false) -> Union{Nothing, VertexEdge}

Return the edge between two vertices, optionally computing qK directions.
"""
function get_edge(grh::VertexGraph, from, to; full=false)::Union{Nothing, VertexEdge}
    
    from = get_idx(get_binding_network(grh), from)
    to = get_idx(get_binding_network(grh), to)
    
    if full
        _ensure_full_vertices_graph!(grh)
    end
    pos = get(grh.edge_pos[from], to, nothing)
    return pos === nothing ? nothing : grh.neighbors[from][pos]
end


"""
    get_edge(bnc, from, to; kwargs...) -> Union{Nothing, VertexEdge}

Convenience wrapper to fetch an edge from a model.
"""
get_edge(Bnc, from, to; kwargs...)= let
    vtx_grh = get_vertices_graph!(Bnc; full=false)
    bn = get_binding_network(Bnc)
    from = get_idx(Bnc, from)
    to = get_idx(Bnc, to)
    get_edge(vtx_grh, from, to; kwargs...)
end

"""
    get_binding_network(grh::VertexGraph, args...) -> Bnc

Return the model backing a vertex graph.
"""
get_binding_network(grh::VertexGraph,args...) = grh.bn
# get_vertices_graph!(grh::VertexGraph,args...; kwargs...) = grh






#-----------------------------------------------------------------------------------
"""
    get_neighbor_graph_x(grh::VertexGraph) -> SimpleGraph

Return the x-space neighbor graph for a vertex graph.
"""
get_neighbor_graph_x(grh::VertexGraph) = grh.x_grh
"""
    get_neighbor_graph_x(bnc::Bnc) -> SimpleGraph

Return the x-space neighbor graph for a model.
"""
get_neighbor_graph_x(Bnc::Bnc) = get_neighbor_graph_x(get_vertices_graph!(Bnc; full=false))

"""
    get_neighbor_graph_qK(grh::VertexGraph; both_side=false) -> SimpleDiGraph

Return the qK-space neighbor graph for a vertex graph.
"""
get_neighbor_graph_qK(grh::VertexGraph; both_side::Bool=false)::SimpleDiGraph = let
    _ensure_full_vertices_graph!(grh)

    qK_grh = let # construct the qK_graph
        Bnc = get_binding_network(grh)
        n = length(grh.neighbors)
        g = SimpleDiGraph(n)
        for (i, edges) in enumerate(grh.neighbors)
            if get_nullity(Bnc,i) >1
                continue
            end
            for e in edges
                if isnothing(e.change_dir_qK) || (!both_side && e.to < i)
                    continue
                end
                add_edge!(g, i, e.to)
            end
        end
        g
    end

    return qK_grh
end
"""
    get_neighbor_graph_qK(bnc::Bnc; kwargs...) -> SimpleDiGraph

Return the qK neighbor graph for a model.
"""
get_neighbor_graph_qK(Bnc::Bnc; kwargs...) = get_neighbor_graph_qK(get_vertices_graph!(Bnc; full=true); kwargs...)
"""
    get_neighbor_graph_qK(grh::SISOPaths; kwargs...) -> SimpleDiGraph

Return the qK neighbor graph for a SISO path object.
"""
get_neighbor_graph_qK(grh::SISOPaths; kwargs...) = grh.qK_grh
get_neighbor_graph_qK(grh::ChangePaths; kwargs...) = grh.qK_grh
"""
    get_neighbor_graph(args...; kwargs...) -> SimpleDiGraph

Alias for `get_neighbor_graph_qK`.
"""
get_neighbor_graph(args...; kwargs...) = get_neighbor_graph_qK(args...; kwargs...)

"""
    get_change_graph(grh::VertexGraph, change_qK_indices, change_qK_signs) -> SimpleDiGraph

Build a monotone change graph for a selected qK-coordinate cone.
"""
function get_change_graph(grh::VertexGraph, change_qK_indices::AbstractVector{<:Integer}, change_qK_signs::AbstractVector{<:Integer})::SimpleDiGraph
    bn = get_binding_network(grh)
    _ensure_full_vertices_graph!(grh)

    n = length(grh.neighbors)
    g = SimpleDiGraph(n)
    for (i, edges) in enumerate(grh.neighbors)
        nlt = get_nullity(bn, i)
        if nlt > 1
            continue
        end
        for e in edges
            if isnothing(e.change_dir_qK) || e.to < i
                continue
            end
            direction = _classify_change_projection(e.change_dir_qK, change_qK_indices, change_qK_signs)
            if direction > 0
                add_edge!(g, i, e.to)
            elseif direction < 0
                add_edge!(g, e.to, i)
            end
        end
    end
    return g
end

get_change_graph(Bnc::Bnc, change_qK_indices::AbstractVector{<:Integer}, change_qK_signs::AbstractVector{<:Integer}) =
    get_change_graph(get_vertices_graph!(Bnc; full=true), change_qK_indices, change_qK_signs)



"""
    get_SISO_graph(grh::SISOPaths) -> SimpleDiGraph

Return the SISO graph stored in a `SISOPaths` object.
"""

get_SISO_graph(grh::SISOPaths) = grh.qK_grh
"""
    get_SISO_graph(model::Bnc, change_qK) -> SimpleDiGraph

Return a SISO graph for a chosen qK coordinate.
"""
get_SISO_graph(model::Bnc, change_qK) = get_SISO_graph(get_vertices_graph!(model; full=true), change_qK)
"""
    get_SISO_graph(grh::VertexGraph, change_qK) -> SimpleDiGraph

Build a SISO graph from a vertex graph for a chosen qK coordinate.
"""
function get_SISO_graph(grh::VertexGraph, change_qK)::SimpleDiGraph
    bn = get_binding_network(grh)
    change_qK_idx = locate_sym_qK(bn, change_qK)
    return get_change_graph(grh, [change_qK_idx], Int8[1])
end



#------------------------------------------------------------------------------
# Higher wrapper for regime graph paths
#------------------------------------------------------------------------------------------

"""
    SISOPaths(model::Bnc, change_qK; rgm_paths=nothing) -> SISOPaths

Construct a `SISOPaths` object for a chosen qK coordinate.
"""
function SISOPaths(model::Bnc{T}, change_qK; rgm_paths=nothing) where {T}
    change_qK_idx = locate_sym_qK(model, change_qK)

    if rgm_paths === nothing
        qK_grh = get_SISO_graph(model, change_qK)
        sources, sinks = get_sources_sinks(model, qK_grh)
        rgm_paths = _enumerate_paths(qK_grh; sources, sinks)
    else
        qK_grh = graph_from_paths(rgm_paths, length(model.vertices_perm))
        sources, sinks = get_sources_sinks(qK_grh)
    end

    return SISOPaths(model, qK_grh, change_qK_idx, sources, sinks, rgm_paths)
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
) where {T}
    change_qK_indices = T[]
    for sym in change_qK_syms
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
        qK_grh = get_change_graph(model, change_qK_indices, sign_vals)
        sources, sinks = get_sources_sinks(model, qK_grh)
        rgm_paths = _enumerate_paths(qK_grh; sources, sinks)
    else
        qK_grh = graph_from_paths(rgm_paths, length(model.vertices_perm))
        sources, sinks = get_sources_sinks(qK_grh)
    end

    return ChangePaths(model, qK_grh, kind, label_str, change_qK_indices, sign_vals, sources, sinks, rgm_paths)
end

"""
    get_path(grh::AbstractChangePaths, pth_idx; return_idx=false) -> Vector

Return a path by index, optionally as vertex indices.
"""
function get_path(grh::AbstractChangePaths, pth_idx::Integer; return_idx::Bool=false)
    rgm_idxs = grh.rgm_paths[pth_idx]
    if return_idx
        return rgm_idxs
    else
        bn = get_binding_network(grh)
        return get_perm.(Ref(bn), rgm_idxs)
    end
    return perms
end
"""
    get_path(grh::AbstractChangePaths, pth::AbstractVector; return_idx=false) -> Vector

Normalize a path representation to indices or permutations.
"""
function get_path(grh::AbstractChangePaths, pth::AbstractVector; return_idx::Bool=false)
    bn = get_binding_network(grh)
    return return_idx ? get_idx.(Ref(bn), pth) : get_perm.(Ref(bn), pth)
end

"""
    get_binding_network(grh::AbstractChangePaths, args...) -> Bnc

Return the model backing a SISO path object.
"""
get_binding_network(grh::AbstractChangePaths,args...)= grh.bn
"""
    get_C_C0_nullity_qK(grh::AbstractChangePaths, pth_idx) -> (Matrix, Vector, Int)

Return constraints for a SISO path polyhedron.
"""
get_C_C0_nullity_qK(grh::AbstractChangePaths, pth_idx) = get_polyhedron(grh, pth_idx) |> get_C_C0_nullity



"""
    get_idx(grh::AbstractChangePaths, pth) -> Int

Return the index for a SISO path specification.
"""
function _ensure_paths_dict!(grh::AbstractChangePaths)
    length(grh.paths_dict) == length(grh.rgm_paths) && return grh.paths_dict
    isempty(grh.paths_dict) || empty!(grh.paths_dict)
    for (i, p) in enumerate(grh.rgm_paths)
        grh.paths_dict[p] = i
    end
    return grh.paths_dict
end

get_idx(grh::AbstractChangePaths, pth::AbstractVector) = let
    bn = get_binding_network(grh)
    idxs = get_idx.(Ref(bn), pth)
    _ensure_paths_dict!(grh)[idxs]
end
"""
    get_idx(grh::AbstractChangePaths, pth::Integer) -> Int

Return the provided path index.
"""
get_idx(grh::AbstractChangePaths, pth::Integer) = pth





"""
    get_polyhedra(grh::AbstractChangePaths, pth_idx=nothing) -> Vector

Return polyhedra for selected SISO paths.
"""
function get_polyhedra(grh::AbstractChangePaths, pth_idx::Union{AbstractVector,Nothing} = nothing)::Vector
    pth_idx = let 
            if isnothing(pth_idx)
                1:length(grh.rgm_paths)
            else
                get_idx.(Ref(grh), pth_idx)
            end
        end
    
    pth_poly_to_calc = filter(x -> !grh.path_polys_is_calc[x], pth_idx)
    
    if !isempty(pth_poly_to_calc)
        polys = _calc_polyhedra_for_path(get_binding_network(grh), grh.rgm_paths[pth_poly_to_calc], change_qK_indices(grh))
        grh.path_polys[pth_poly_to_calc] .= polys
        grh.path_polys_is_calc[pth_poly_to_calc] .= true
    end

    return grh.path_polys[pth_idx]
end
"""
    get_polyhedron(grh::AbstractChangePaths, pth)

Return the polyhedron for a single SISO path.
"""
get_polyhedron(grh::AbstractChangePaths, pth)= get_polyhedra(grh, [get_idx(grh, pth)])[1]



"""
    get_volumes(grh::AbstractChangePaths, pth_idx=nothing; asymptotic=true, recalculate=false, kwargs...) -> Vector{Volume}

Compute volumes for SISO paths.
"""
function get_volumes(grh::AbstractChangePaths, pth_idx::Union{AbstractVector,Nothing}=nothing; 
    rebase_K = false,
    rebase_mat = nothing,
    recalculate=false, kwargs...)

    pth_idx = let 
            if isnothing(pth_idx)
                1:length(grh.rgm_paths)
            else
                get_idx.(Ref(grh), pth_idx)
            end
        end
    
    idxes_to_calculate = recalculate ? pth_idx : filter(x -> !grh.path_volume_is_calc[x], pth_idx)
    
    if !isempty(idxes_to_calculate)

        rebase_mat = if  !isnothing(rebase_mat)
                    @assert !rebase_K "Cannot specify both rebase_K and providing rebase_mat"
                    rebase_mat
                elseif rebase_K
                    Bnc = get_binding_network(grh) 
                    Q = rebase_mat_lgK(Bnc.N)
                    blockdiag(spdiagm(fill(Rational(1), Bnc.d-1)), Q)
                else
                    nothing
                end

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

"""
    get_volume(grh::AbstractChangePaths, pth; kwargs...) -> Volume

Return the volume for a single SISO path.
"""
get_volume(grh::AbstractChangePaths, pth; kwargs...) = get_volumes(grh, [get_idx(grh, pth)]; kwargs...)[1]



#-------------------------------------------------------------------------------------
# Regime shifting associated functions
#-------------------------------------------------------------------------------------

"""
    show_regime_path(grh::SISOPaths, pth) -> nothing

Print a formatted regime path with optional volume.
"""
function show_regime_path(grh::SISOPaths, pth)
    pth_idx = get_idx(grh, pth)
    pth = get_path(grh, pth_idx; return_idx=true)
    vol_is_calc = grh.path_volume_is_calc[pth_idx]
    volume = vol_is_calc ? grh.path_volume[pth_idx] : nothing
    print_path(pth; prefix="#",id = pth_idx,volume=volume)
    return nothing
end


"""
    get_expression_path(grh::SISOPaths, pth; observe_x=nothing) -> (Vector, Vector)

Return expression coefficients and interfaces along a SISO path.
"""
function get_expression_path(grh::SISOPaths, pth; observe_x=nothing)
    
    bn = get_binding_network(grh)
    rgm_pth = get_path(grh, pth; return_idx=true)
    # @show rgm_pth
    rgm_nlt = get_nullities(bn, rgm_pth)
    
    change_qK_idx = grh.change_qK_idx
    observe_x_idx = isnothing(observe_x) ? (1:bn.n) : locate_sym_x.(Ref(bn), observe_x)
    
    rgm_interface = get_interface.(Ref(bn),rgm_pth[1:end-1], rgm_pth[2:end])
    
    H_H0 = Vector{Any}(undef, length(rgm_pth))
    for i in eachindex(rgm_pth)
        rgm = rgm_pth[i]
        nlt = rgm_nlt[i]
        if nlt == 0 # for non-singular regime, we care about the expression, tells by the H[i，：]
            H,H0 = get_H_H0(bn, rgm)
            # @show H,H0, observe_x_idx
            H_H0[i] = (H[observe_x_idx, :], H0[observe_x_idx]) 
        elseif nlt == 1 # for singular regime, we care about the contiuity, tells by the H[i,j]
            H = get_H(bn,rgm)
            H_H0[i] = (H[observe_x_idx, change_qK_idx], nothing)
        else
            error("Nullity > 1 is not supported for expression path.") # should ne change if under constrain.
        end
    end
    return H_H0, rgm_interface
end





# function show_expression_path(grh::SISOPaths, pth_idx::Integer; observe_x=nothing)
#     bn = get_binding_network(grh)
#     observe_x_idx = isnothing(observe_x) ? bn.n : locate_sym_x.(Ref(bn), observe_x)
#     rgm_pth = get_path(grh, pth_idx; return_idx=true)
#     pth = map(rgm_pth) do r
#         is_singular(bn, r) ? fill(NaN, length(observe_x_idx)) : get_expression(bn, r)[observe_x_idx]
#     end
#     pth = get_path(grh, pth_idx; return_idx=false)
#     print_expression_path(get_binding_network(grh), pth; prefix="#")
#     return nothing
# end

#-------------------------------------------------------------------------------------------
# 
_round_ro_value(x::Real) = round(Float64(x); digits=3)

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
    _calc_RO_for_single_path(model, path, change_qK_idx, observe_x_idx) -> Vector

Compute the reaction-order profile along a single path.
"""
function _calc_RO_for_single_path(model, path::AbstractVector{<:Integer}, change_qK_idx::Integer, observe_x_idx)::Vector{Float64}
    r_ord = Vector{Float64}(undef, length(path))
    for i in eachindex(path)
        r_ord[i] = _calc_change_response_for_vertex(model, path[i], change_qK_idx, observe_x_idx)
    end
    return r_ord
end

function _calc_RO_for_single_path(
    model,
    path::AbstractVector{<:Integer},
    change_qK_indices::AbstractVector{<:Integer},
    observe_x_idx,
    change_qK_signs::AbstractVector{<:Integer},
)::Vector{Vector{Float64}}
    return [_calc_change_response_for_vertex(model, vertex_idx, change_qK_indices, observe_x_idx, change_qK_signs) for vertex_idx in path]
end
"""
    _dedup(ord_path) -> Vector

Deduplicate consecutive reaction-order values while preserving discontinuities.
"""
function _dedup(ord_path::AbstractVector{T})::Vector{T} where T<:Real
    isempty(ord_path) && return T[]

    first_finite_idx = findfirst(x -> !isnan(x), ord_path)
    if isnothing(first_finite_idx)
        return T[T(NaN)]
    end

    out = T[]
    if first_finite_idx > firstindex(ord_path)
        push!(out, T(NaN))
    end

    push!(out, ord_path[first_finite_idx])
    pending_nan = false
    last_out = out[end]

    for x in @view ord_path[(first_finite_idx+1):end]
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

function _all_nan(values::AbstractVector{<:Real})
    return !isempty(values) && all(isnan, values)
end

function _dedup(ord_path::AbstractVector{<:AbstractVector{<:Real}})::Vector{Vector{Float64}}
    isempty(ord_path) && return Vector{Float64}[]

    first_finite_idx = findfirst(x -> !_all_nan(x), ord_path)
    if isnothing(first_finite_idx)
        return [fill(NaN, length(ord_path[1]))]
    end

    first_vec = Float64[Float64(x) for x in ord_path[first_finite_idx]]
    out = Vector{Float64}[]
    if first_finite_idx > firstindex(ord_path)
        push!(out, fill(NaN, length(first_vec)))
    end
    push!(out, first_vec)
    pending_nan = false
    last_out = first_vec

    for x_raw in @view ord_path[(first_finite_idx+1):end]
        x = Float64[Float64(v) for v in x_raw]
        if _all_nan(x)
            pending_nan = true
            continue
        end
        if x != last_out
            if pending_nan
                push!(out, fill(NaN, length(x)))
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
    get_RO_path(model::Bnc, rgm_idx_shift_pth; change_qK, observe_x,
        deduplicate=false, keep_singular=true, keep_nonasymptotic=true) -> Vector

Calculate the reaction-order profile for a single regime path. Use either
`change_qK=<symbol>` for a scalar SISO profile or
`change_qK_indices=<indices>, change_qK_signs=<signs>` for a vector-valued
multi-change profile.
"""
function get_RO_path(
    model::Bnc,rgm_idx_shift_pth::AbstractVector;
    change_qK=nothing,
    change_qK_indices::Union{Nothing, AbstractVector{<:Integer}}=nothing,
    observe_x,
    change_qK_signs::Union{Nothing, AbstractVector{<:Integer}}=nothing,
    deduplicate::Bool=false,
    keep_singular::Bool=true,
    keep_nonasymptotic::Bool=true
    )
    rgm_idx_shift_pth = get_idx.(Ref(model), rgm_idx_shift_pth)
    observe_x_idx = locate_sym_x(model, observe_x)

    if !isnothing(change_qK) && !isnothing(change_qK_indices)
        error("Specify either change_qK or change_qK_indices, but not both.")
    end

    ord_path = if isnothing(change_qK_indices)
        isnothing(change_qK) && error("change_qK is required when change_qK_indices is not provided.")
        change_qK_idx = locate_sym_qK(model, change_qK)
        _calc_RO_for_single_path(model, rgm_idx_shift_pth, change_qK_idx, observe_x_idx)
    else
        sign_vals = isnothing(change_qK_signs) ? fill(Int8(1), length(change_qK_indices)) : Int8[Int8(sign) for sign in change_qK_signs]
        _calc_RO_for_single_path(model, rgm_idx_shift_pth, Int[Int(idx) for idx in change_qK_indices], observe_x_idx, sign_vals)
    end

    # apply the regime filter
    mask = _get_mask(model, rgm_idx_shift_pth;
        singular=keep_singular ? nothing : false,
        asymptotic=keep_nonasymptotic ? nothing : true)
    
    ord_path = ord_path[mask]

    # remove redundency
    if deduplicate
        ord_path = _dedup(ord_path)
    end

    return ord_path
end

"""
    get_RO_paths(model::Bnc, rgm_paths, args...; kwargs...) -> Vector{Vector}

Calculate reaction-order profiles for multiple regime paths.
"""
function get_RO_paths(model::Bnc, rgm_paths::AbstractVector{<:AbstractVector}, args...; kwargs...)
    
    rgm_idx_for_each_paths = rgm_paths .|> x -> get_idx.(Ref(model), x)

    ord_for_each_paths = Vector{Any}(undef, length(rgm_idx_for_each_paths))
    Threads.@threads for i in eachindex(rgm_idx_for_each_paths)
        ord_for_each_paths[i] = get_RO_path(model, rgm_idx_for_each_paths[i], args...; kwargs...)
    end
    return ord_for_each_paths
end
"""
    get_RO_paths(model::SISOPaths, pth_idx=nothing; observe_x, kwargs...) -> Vector{Vector}

Calculate reaction-order profiles for paths in a `SISOPaths` object.
"""
function get_RO_paths(model::SISOPaths, pth_idx::Union{Nothing, AbstractVector}=nothing ; observe_x, kwargs...)
    rgm_paths = isnothing(pth_idx) ? model.rgm_paths : get_path.(Ref(model), pth_idx; return_idx=true)
    observe_x_idx = locate_sym_x(model.bn, observe_x)
    return get_RO_paths(model.bn, rgm_paths; 
        change_qK=model.change_qK_idx, observe_x=observe_x_idx, kwargs...)
end

function get_RO_paths(model::ChangePaths, pth_idx::Union{Nothing, AbstractVector}=nothing ; observe_x, kwargs...)
    rgm_paths = isnothing(pth_idx) ? model.rgm_paths : get_path.(Ref(model), pth_idx; return_idx=true)
    observe_x_idx = locate_sym_x(model.bn, observe_x)
    profiles = Vector{Any}(undef, length(rgm_paths))
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
    return profiles
end
"""
    get_RO_path(model::SISOPaths, pth_idx, args...; kwargs...) -> Vector

Single-path wrapper for `get_RO_paths`.
"""
get_RO_path(model::SISOPaths, pth_idx, args...; kwargs...) = get_RO_paths(model, [get_idx(model,pth_idx)], args... ; kwargs...)[1]
get_RO_path(model::ChangePaths, pth_idx, args...; kwargs...) = get_RO_paths(model, [get_idx(model,pth_idx)], args... ; kwargs...)[1]
    


"""
    group_sum(keys, vals; sort_values=true) -> Vector{Tuple}

Group values by keys, returning indices, key, and summed values.
"""
function group_sum(keys::AbstractVector{I}, vals::AbstractVector{J}; 
    sort_values::Bool=true
    ) :: Vector{Tuple{Vector{Int}, I, J}} where {I,J}

    @assert length(keys) == length(vals)
    # Dictionary to accumulate sum of values for each key
    dict = Dict{I,J}()
    # Store indices of keys for later reference
    index_dict = Dict{I, Vector{Int}}()
    
    @inbounds for (i, (k, v)) in enumerate(zip(keys, vals))
        dict[k] = get(dict, k, zero(v)) + v
        push!(get!(index_dict, k, Int[]), i)  # Store the index
    end
    
    # Collect and sort if needed
    dict_vec = collect(dict)
    
    if sort_values
        # Sort by values (sum of vals)
        sort!(dict_vec, by=x->x[2], rev=true)
    end
    
    # Create a Vector of Tuples with (index, key, summed value)
    result = Vector{Tuple{Vector{Int}, I, J}}(undef, length(dict))
    
    # @show dict, index_dict
    for i in eachindex(dict_vec)
        key, sum_val = dict_vec[i]
        group = index_dict[key]
        result[i] = (group, key, sum_val)
    end
    
    return result
end



"""
    summary(grh::SISOPaths; show_volume=true, prefix="#", kwargs...) -> nothing

Print the paths stored in `SISOPaths`, optionally with volumes.
"""
function summary(grh::SISOPaths; show_volume::Bool=true, prefix::AbstractString="#", kwargs...)
    paths = grh.rgm_paths
    if show_volume
        vols = get_volumes(grh; kwargs...)
        print_paths(paths; prefix=prefix, volumes = vols, ids = 1:length(paths))
    else
        print_paths(paths; prefix=prefix, ids = 1:length(paths))
    end
    return nothing
end



"""
    summary_RO_path(grh::AbstractChangePaths; observe_x, show_volume=true, deduplicate=true,
        keep_singular=true, keep_nonasymptotic=true, kwargs...) -> nothing

Summarize reaction-order paths grouped by profile.
"""
function summary_RO_path(grh::AbstractChangePaths;observe_x, show_volume::Bool=true,

    deduplicate::Bool=true,keep_singular::Bool=true,keep_nonasymptotic::Bool=true,kwargs...)

    ord_pth = get_RO_paths(grh; observe_x=observe_x, 
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic)

    volumes = if show_volume
        get_volumes(grh; kwargs...)
    else
        fill(nothing, length(grh.rgm_paths))
    end



    rsts = group_sum(ord_pth, volumes)
    # for (id, pth, volume) in rsts
    #      print_path(pth; prefix="",id=id, volume=volume)
    # end

    # print 
    ids = getindex.(rsts, 1)
    ords = getindex.(rsts, 2)
    vols = getindex.(rsts, 3)
    print_paths(ords; prefix="", ids=ids, volumes=vols)
    return nothing
end


"""
    _normalize_behavior_scope(scope) -> Symbol

Normalize behavior-family path filtering scope.
"""
function _normalize_behavior_scope(scope)::Symbol
    scope_sym = Symbol(scope)
    scope_sym in (:all, :feasible, :robust) || error("Unsupported path_scope=$scope. Use :all, :feasible, or :robust.")
    return scope_sym
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
    grh::AbstractChangePaths;
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
    volume_kwargs...,
)
    path_scope = _normalize_behavior_scope(path_scope)
    path_scope == :robust && !compute_volume && error("path_scope=:robust requires compute_volume=true")
    graph_only_multi_change = grh isa ChangePaths && path_scope == :all && !compute_volume

    total_paths = length(grh.rgm_paths)
    ord_paths = get_RO_paths(
        grh;
        observe_x=observe_x,
        deduplicate=deduplicate,
        keep_singular=keep_singular,
        keep_nonasymptotic=keep_nonasymptotic,
    )

    feasible_mask = graph_only_multi_change ? trues(total_paths) : begin
        polys = get_polyhedra(grh)
        map(poly -> !isempty(poly), polys)
    end
    feasible_ids = compute_volume ? findall(feasible_mask) : Int[]

    volumes_by_path = Vector{Union{Nothing,Volume}}(undef, total_paths)
    fill!(volumes_by_path, nothing)
    if compute_volume && !isempty(feasible_ids)
        vols = get_volumes(grh, feasible_ids; volume_kwargs...)
        for (path_idx, vol) in zip(feasible_ids, vols)
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
        observe_x_idx=locate_sym_x(grh.bn, observe_x),
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
    grh::AbstractChangePaths;
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
