
"""
    VertexEdge

Edge metadata connecting neighboring vertices in a regime graph.
"""
mutable struct VertexEdge
    to::Int
    i ::Int # different row index
    c_c0_x_idx::Int
    c_c0_x_sign::Int8
    qK_interface_idx::Int
    qK_interface_sign::Int8

    function VertexEdge(to::Int, i::Int, c_c0_x_idx::Int, c_c0_x_sign::Int8)
        return new(to, i, c_c0_x_idx, c_c0_x_sign, 0, 0)
    end

end

@inline _edge_has_qK_interface(edge::VertexEdge) =
    edge.qK_interface_idx != 0



struct RegimeHyperplane
    change_dir_qK::SparseVector{Float64, Int}
    intersect_qK::Float64
end


# Adjacency list + optional caches
"""
    VertexGraph

Adjacency structure for vertices with optional caches for change directions.
"""
mutable struct VertexGraph{Tv}
    bn::Union{AbstractBnc, Nothing}
    neighbors::Vector{Vector{VertexEdge}}

    edge_pos::Vector{Dict{Int, Int}}  # (u,v) -> (u,edge_pos[u][v]) to locate the VertexEdge.

    qK_interface_pool::Vector{RegimeHyperplane}
    x_interface_pool::Vector{Hyperplane_perm{Tv}}
    change_dir_qK_computed::Bool

    function VertexGraph(L_helper::MatrixHelper{Tv}, neighbors::Vector{Vector{VertexEdge}}) where {Tv}
        
        edge_pos = let
            edge_pos = Vector{Dict{Int,Int}}(undef, length(neighbors))
                for i in eachindex(neighbors)
                    edges = neighbors[i]
                    d = Dict{Int,Int}()
                    sizehint!(d, length(edges))
                    for (k, e) in enumerate(edges)
                        d[e.to] = k
                    end
                    edge_pos[i] = d
                end
                edge_pos
            end

        return new{Tv}(nothing, neighbors, edge_pos, RegimeHyperplane[], L_helper.hyperplanes, false)
    end
end


#-----------------------------------------------------------------------------------------------
#This is graph associated functions for Bnc models and archetyple behaviors associated code
#-----------------------------------------------------------------------------------------------
"""
    _calc_regimes_graph(bnc::Bnc, perms) -> VertexGraph

Build a `VertexGraph` from regime permutations, connecting regimes that differ
in exactly one row.
"""
function _calc_regimes_graph(helper::MatrixHelper, perms::Vector{<:AbstractVector{T}}) where {T<:Integer}
    # n = helper.n
    n_vtxs = length(perms)
    d = length(perms[1])
    thread_edges = [Vector{Tuple{Int, VertexEdge}}() for _ in 1:Threads.maxthreadid()]

    # 按行分桶：key 为去掉该行后的签名（Tuple），值为该签名下的 (regime idx, row choice)
    @showprogress enabled=!haskey(ENV, "BNC_NO_PROGRESS") for i in 1:d
        buckets = Dict{Tuple{Vararg{T}}, Vector{Tuple{Int,T}}}()
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

        # 同桶内两两相连：沿边方向表示“增加 target dominant term，减少 source dominant term”
        Threads.@threads for gi in eachindex(groups)
            tid = Threads.threadid()
            local_edges = thread_edges[tid]
            group = groups[gi]
            m = length(group)
            m <= 1 && continue

            @inbounds for a in 1:m-1
                from_idx, j_from = group[a]
                for b in a+1:m
                    to_idx, j_to = group[b]
                    j_from == j_to && continue

                    hp_id = choiceineq_between(helper, i, j_to, j_from)
                    push!(local_edges, (from_idx, VertexEdge(to_idx, i, hp_id.hid, hp_id.sign)))
                    push!(local_edges, (to_idx, VertexEdge(from_idx, i, hp_id.hid, -hp_id.sign)))
                end
            end
        end
    end

    all_edges = reduce(vcat, thread_edges; init=Tuple{Int, VertexEdge}[])
    neighbors = [Vector{VertexEdge}() for _ in 1:n_vtxs]
    for (from, e) in all_edges
        push!(neighbors[from], e)
    end
    return VertexGraph(helper, neighbors)
end
#=============================================================================================#
#          Calc qK-space change directions for edges with nullity <= 1 regimes This part is pure AI
#=============================================================================================#

function _edge_qK_interface(grh::VertexGraph, edge::VertexEdge)
    edge.qK_interface_idx == 0 && return nothing

    hp = grh.qK_interface_pool[edge.qK_interface_idx]
    if edge.qK_interface_sign >= 0
        return hp.change_dir_qK, hp.intersect_qK
    else
        return -hp.change_dir_qK, -hp.intersect_qK
    end
end



# function _materialize_edge_qK_interface!(grh::VertexGraph, edge::VertexEdge)
#     return edge
# end

@inline function _dense_hyperplane_sign_and_scale(dir::SparseVector{Float64,Int}; atol::Float64=1e-10)
    I, V = findnz(dir)
    @inbounds for k in eachindex(V)
        if abs(V[k]) > atol
            sgn = V[k] >= 0 ? Int8(1) : Int8(-1)
            return sgn, abs(V[k])
        end
    end
    return Int8(1), 0.0
end

function _canonicalize_qK_interface(
    dir::SparseVector{Float64,Int},
    intersect::Real;
    atol::Float64=1e-10,
    round_digits::Int=10,
)
    droptol!(dir, atol)
    nnz(dir) == 0 && return nothing

    sign, scale = _dense_hyperplane_sign_and_scale(dir; atol=atol)
    scale <= atol && return nothing

    I, V = findnz(dir)
    Vnorm = (Float64.(V) .* sign) ./ scale
    dir_norm = SparseArrays.sparsevec(I, Vnorm, length(dir))
    droptol!(dir_norm, atol)
    I2, V2 = findnz(dir_norm)
    bnorm = Float64(intersect) * sign / scale
    key = (Tuple(Int.(I2)), Tuple(round.(Float64.(V2); digits=round_digits)), round(bnorm; digits=round_digits))
    return dir_norm, bnorm, sign, key
end

function _intern_qK_interface!(
    grh::VertexGraph,
    key_to_id::Dict,
    dir::SparseVector{Float64,Int},
    intersect::Real;
    atol::Float64=1e-10,
    round_digits::Int=10,
)
    canon = _canonicalize_qK_interface(dir, intersect; atol=atol, round_digits=round_digits)
    canon === nothing && return 0, Int8(0)
    dir_norm, bnorm, sign, key = canon
    hid = get!(key_to_id, key) do
        push!(grh.qK_interface_pool, RegimeHyperplane(dir_norm, bnorm))
        length(grh.qK_interface_pool)
    end
    return hid, sign
end




"""
    _fulfill_regimes_graph!(vtx_graph::VertexGraph) -> nothing

Compute qK-space change directions for edges in the vertex graph.
"""
function _fulfill_regimes_graph!(vtx_graph::VertexGraph)
    Bnc = vtx_graph.bn
    regimes = _bind_regimes_data(Bnc)
    empty!(vtx_graph.qK_interface_pool)
    key_to_id = Dict{Any,Int}()

    for edges in vtx_graph.neighbors
        for e in edges
            e.qK_interface_idx = 0
            e.qK_interface_sign = 0
        end
    end

    @showprogress enabled=!haskey(ENV, "BNC_NO_PROGRESS") for p1 in eachindex(vtx_graph.neighbors)
        edges = vtx_graph.neighbors[p1]
        for e in edges
            p2 = e.to
            p1 < p2 || continue

            rev_pos = get(vtx_graph.edge_pos[p2], p1, nothing)
            rev_pos === nothing && continue
            e_rev = vtx_graph.neighbors[p2][rev_pos]

            src_idx, src_edge, dst_edge = let
                nlt1 = regimes[p1].nullity
                nlt2 = regimes[p2].nullity
                if nlt1 <= 1
                    (p1, e, e_rev)
                elseif nlt2 <= 1
                    (p2, e_rev, e)
                else
                    (0, nothing, nothing)
                end
            end
            src_idx == 0 && continue

            src_rgm = regimes[src_idx]
            c_c0 = vtx_graph.x_interface_pool[src_edge.c_c0_x_idx]
            
            dir_qK, intersect_qK = _calc_dir(
                src_rgm.nullity,
                src_rgm.H,
                src_rgm.H0,
                c_c0,
                src_edge.c_c0_x_sign,
            )

            hid, sign = _intern_qK_interface!(vtx_graph, key_to_id, dir_qK, intersect_qK)
            hid == 0 && continue

            src_edge.qK_interface_idx = hid
            src_edge.qK_interface_sign = sign
            dst_edge.qK_interface_idx = hid
            dst_edge.qK_interface_sign = Int8(-sign)
        end
    end
    return nothing
end


@inline function _calc_dir(
    nlt::Int,
    H::SparseMatrixCSC{<:Real,Int},
    H0::AbstractVector{<:Real},
    c_c0::Hyperplane_perm,
    sign::Int8,
    drop_tol::Float64=1e-10,
)
    c_qK = c_c0 * H .* sign
    c0_qK = nlt ==0 ? c_c0 * H0 * sign : mul(c_c0, H0; with_c0=false) * sign 

    I, V = findnz(c_qK)
    c_qK = SparseArrays.sparsevec(I, Float64.(V), length(c_qK))
    if drop_tol > 0 
        droptol!(c_qK, drop_tol) 
        c0_qK = droptol!(Float64(c0_qK), drop_tol)
    else
        c0_qK = Float64(c0_qK)
    end

    return c_qK, c0_qK
end
