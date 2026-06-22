export find_all_regimes!, get_bind_regimes_dict, get_nullities, get_volumes, have_perm
export get_regimes, get_perms, get_indices, get_regimes_neighbor_mat
export is_singular, is_asymptotic, n_regimes
export get_idx, get_perm, get_regime, get_neighbors, get_nullity, get_one_inner_point
export get_P_P0, get_P, get_P0
export get_M_M0, get_M, get_M0
export get_H_H0, get_H, get_H0
export get_C_C0_x, get_C_x, get_C0_x
export get_C_C0_nullity_qK, get_C_C0_qK, get_C_qK, get_C0_qK
export get_C_C0_nullity, get_C_C0, get_C, get_C0
export check_feasibility_with_constraint, feasible_vertieces_with_constraint
export get_polyhedron, get_volume
export is_neighbor, get_interface, get_change_dir
export get_function

#--------------Core computation functions-------------------------





"""
    _calc_C_C0_qK_singular(bnc::Bnc, vtx) -> (SparseMatrixCSC, Vector)

Build qK-space constraints `(C_qK, C0_qK)` for singular vertices via affine mapping.
"""
function _calc_C_C0_qK_singular(Bnc::Bnc, vtx)
    M,M0 = get_M_M0(Bnc,vtx)
    C,C0 = get_C_C0_x(Bnc,vtx)
    _affine_mapping_polyhedra(C,C0,M,M0)
end

function _affine_mapping_polyhedra(C,C0,M,M0)
    # n = Bnc.n
    poly_x = hrep(-C,C0) |> x->polyhedron(x,CDDLib.Library())
    poly_elim = M * poly_x  # If for convenience, one can write `translate(M * poly_x, M0)`, and then C0qK = b
    rlt = MixedMatHRep(hrep(poly_elim))
    A, b, linset = (rlt.A, rlt.b, rlt.linset)
    # @show linset
    @assert linset == BitSet(1:maximum(linset)) "linear rows are not the first top n rows, code fix is needed"
    # perm = [collect(linset) ; [i for i in 1:size(A,1) if i ∉ linset]]
    CqK = sparse(-A) |> x->droptol!(x,1e-10)
    C0qK = (b+A*M0)
    return CqK, C0qK, linset
end




#------------------Helper functions -------------------------------------------
"""
    _regime_graph_to_sparse(g::VertexGraph; weight_fn=e->1) -> SparseMatrixCSC

Convert a `VertexGraph` to a sparse adjacency matrix.
"""
function _regime_graph_to_sparse(G::VertexGraph; weight_fn = e -> 1)
    n = length(G.neighbors)
    sample_edge = nothing
    for edges in G.neighbors
        if !isempty(edges)
            sample_edge = first(edges)
            break
        end
    end
    isnothing(sample_edge) && return spzeros(Int, n, n)
    Ty = typeof(weight_fn(sample_edge))
    # 预分配估计：平均度 × n
    nnz = sum(length(v) for v in G.neighbors)
    I = Vector{Int}(undef, nnz)
    J = Vector{Int}(undef, nnz)
    V = Vector{Ty}(undef, nnz)
    idx = 0
    for i in 1:n
        for e in G.neighbors[i] #Edge
            idx += 1
            I[idx] = i
            J[idx] = e.to
            V[idx] = weight_fn(e)
        end
    end
    return sparse(I,J,V, n, n) |> dropzeros!
end

@inline is_bind_regimes_built(model::Bnc) = !isnothing(model.BindRegimes)

#------------------------------------------------------------------------------
#             1. Functions find all regimes and return properties
# ------------------------------------------------------------------------------


find_bind_regimes!(model::Bnc{T}; H_mode::Symbol=_affine_mode(model)) where T = find_all_regimes!(model; H_mode=H_mode)
"""
    find_all_regimes!(bnc::Bnc) -> Vector{Vector{Int}}

Compute and cache all regime permutations, the x-neighbor graph, and regime
objects. Low-nullity (`0/1`) affine data are inferred directly from the graph;
only deferred high-nullity perms are sent to `_calc_nullity`.

Keyword `H_mode` controls how binding-regime affine coefficients are stored:
- `:float`    uses floating-point `H` / `C_qK`
- `:rational` uses exact rational coefficients for `H` / `C_qK`
"""
function find_all_regimes!(model::Bnc{T}; H_mode::Symbol=_affine_mode(model)) where T

    
    # Decide what type should we used to store H.
    H_mode = _normalize_affine_mode(H_mode)
    is_bind_regimes_built(model) && model.affine_coeff_mode == H_mode && return nothing
    
    _remove_regime_data!(model)
    model.affine_coeff_mode = H_mode

    
    @info "---------------------Start finding all regimes--------------------"
    
    (all_perms, is_asymptotic) =  let
        perms, is_asymp  = _enumerate_all_regimes(model._L_helper)
        perms = [Vector{T}(v) for v in perms]
        (perms, is_asymp)
    end


    n_vertices = length(all_perms)
    n_asym_rgms = sum(is_asymptotic)
    @info "Finished, with $(n_vertices) regimes found and $(n_asym_rgms) asymptotic regimes."

    @info "2.Building x-neighbor regime graph..."
    model.vertices_graph = let
        grh = _calc_regimes_graph(model._L_helper, all_perms)
        grh.bn = model
        grh
    end
        

    @info "3.Building regime objects..."
    model.BindRegimes = let
        regimes = _build_bind_regimes(model, all_perms, is_asymptotic, fill(T(-1), n_vertices))
        vertices_perm_dict = Dict(perm => idx for (idx, perm) in enumerate(all_perms))
        Regimes(vertices_perm_dict, regimes)
    end

    @info "4.Propagating affine data and deferred nullity labels..."
    _prefill_affine_cache!(model; ensure_built=false)

    _ensure_full_regimes_graph!(model)

    @info "Finished."
    return nothing
end

@inline function _build_bind_regimes(model::Bnc{T}, all_perms, is_asymptotic, nullity) where T
    n_vertices = length(all_perms)
    regimes = Vector{BindRegime}(undef, n_vertices)
    Threads.@threads for i in 1:n_vertices
        regimes[i] = BindRegime(
            network = model,
            perm = all_perms[i],
            idx = i,
            is_asymptotic = is_asymptotic[i],
            nullity = nullity[i]
        )
    end
    return regimes
end


"""
    _initialize_regime!(vtx::BindRegime) -> BindRegime

Fill the basic linear-algebra fields of a lazily-created `BindRegime`.
"""
function _initialize_regime!(vtx::BindRegime)::BindRegime
    if !isnothing(vtx.P)
        return vtx
    end
    Bnc = vtx.network
    helper = Bnc._L_helper
    perm = vtx.perm

    P, P0 = _calc_P_P0(perm, helper)
    C_x, C0_x = _calc_C_C0(perm, helper)

    vtx.P = P
    vtx.P0 = P0
    vtx.C_x = C_x
    vtx.C0_x = C0_x
    vtx.M = vcat(P, Bnc.N)
    vtx.M0 = vcat(P0, zeros(eltype(P0), Bnc.r))
    return vtx
end

function _materialize_qK_conditions!(rgm::BindRegime)
    _initialize_regime!(rgm)
    (!isnothing(rgm.C_qK) && !isnothing(rgm.C0_qK)) && return nothing

    if rgm.nullity == 0
        C_qK = sparse(rgm.C_x * rgm.H)
        if eltype(C_qK) <: AbstractFloat
            droptol!(C_qK, 1e-10)
        else
            dropzeros!(C_qK)
        end
        rgm.C_qK = C_qK
        rgm.C0_qK = Float64.(rgm.C0_x + rgm.C_x * rgm.H0)
    else
        rgm.C_qK, rgm.C0_qK, _ = _calc_C_C0_qK_singular(rgm.network, rgm.perm)
    end

    return nothing
end

"""
    _fill_all_info!(vtx::BindRegime) -> nothing

Ensure a `BindRegime` has `H/H0` and qK constraints computed and cached.
"""
function _fill_all_info!(vtx::BindRegime)
    _initialize_regime!(vtx)
    _materialize_qK_conditions!(vtx)
    return nothing
end










#===============================================================================================================#
# Binding Regime related APIs.
#===============================================================================================================#


"""
    get_binding_network(bnc_or_vertex, args...) -> Bnc

Return the binding network associated with a vertex or the model itself.
"""
get_binding_network(Bnc::Bnc,args...)=Bnc
get_binding_network(vtx::BindRegime,args...)=vtx.network


"""
    get_bind_regimes_dict(args...;kwargs...) -> Dict

Return a dictionary mapping permutation vectors to vertex indices.
"""
get_regimes_dict(model::Regimes) = model.vertices_perm_dict
get_bind_regimes_dict(args...; kwargs...) =let 
    bn = get_binding_network(args...; kwargs...)
    find_all_regimes!(bn; kwargs...)
    _bind_regimes_perm_dict(bn)
end





#-------------------------------------------------------------------------------------
#         functions involving single vertex and lazy calculate  its properties, act as keys for higher level functions
# ------------------------------------------------------------------------------------


"""
    get_idx(bnc::Bnc, idx::Integer; check=false) -> Integer

Return the vertex index, optionally validating it.
"""
function get_idx(Bnc::Bnc, idx::T;check::Bool=false) where T<:Integer
    if check
        find_all_regimes!(Bnc)
        @assert idx ≥ 1 && idx ≤ n_regimes(Bnc) "The given index is out of range."
    end
   return idx
end
get_idx(Bnc::Bnc,perm::AbstractVector;kwargs...)=(get_bind_regimes_dict(Bnc)[get_perm(Bnc, perm)])
get_idx(vtx::BindRegime) = vtx.idx
get_idx(Bnc::Bnc, vtx::BindRegime;kwargs...)= get_idx(vtx)

"""
    get_perm(bnc::Bnc, perm; check=false) -> Vector

Return the permutation vector, optionally validating it.
"""
function get_perm(Bnc::Bnc,perm::Vector{<:Integer};check::Bool=false)
    if check
        @assert haskey(get_bind_regimes_dict(Bnc), perm) "The given perm is not in Bnc"
    end
    return perm
end
get_perm(Bnc::Bnc, perm::AbstractVector) = get_perm(Bnc, locate_sym_x.(Ref(Bnc), perm))
get_perm(Bnc::Bnc, idx::Integer; kwargs...)=(find_all_regimes!(Bnc); _bind_regimes_data(Bnc)[idx].perm)
get_perm(vtx::BindRegime) = vtx.perm
get_perm(Bnc::Bnc, vtx::BindRegime;kwargs...)= get_perm(vtx)


"""
    get_regime(bnc::Bnc, perm; check=false, kwargs...) -> BindRegime

Retrieve a vertex from cache or create it if missing.
"""
function get_regime(Bnc::Bnc, perm; check::Bool=false, kwargs...)::BindRegime
    find_all_regimes!(Bnc) #initialize perm_data
    
    vtx = begin
        idx = get_idx(Bnc, perm; check=check)          
        _initialize_regime!(_bind_regimes_data(Bnc)[idx])
    end
    return get_regime(vtx; kwargs...)
end
"""
    get_regime(vtx::BindRegime; inv_info=true, kwargs...) -> BindRegime

Ensure a vertex has requested cached fields and return it.
"""
function get_regime(vtx::BindRegime; inv_info::Bool=true,kwargs...)::BindRegime
    _initialize_regime!(vtx)
    if inv_info
        _fill_all_info!(vtx)
    end
    return vtx
end


#---------------------------------------------------------------------------------------------
#   Functions involving vertices relationships, (neighbors finding and changedir finding)
#---------------------------------------------------------------------------------------------
"""
    get_regimes_neighbor_mat_x(bnc::Bnc) -> SparseMatrixCSC

Return the x-space adjacency matrix of the vertex graph.
"""
function get_regimes_neighbor_mat_x(Bnc::Bnc)
    grh = get_regimes_graph!(Bnc;full=false)
    spmat = _regime_graph_to_sparse(grh; weight_fn = e -> 1)
    return spmat
end

"""
    get_regimes_neighbor_mat_qK(bnc::Bnc) -> SparseMatrixCSC

Return the qK-space adjacency matrix of the vertex graph.
"""
function get_regimes_neighbor_mat_qK(Bnc::Bnc)
    grh = get_regimes_graph!(Bnc;full=true)
    f(x::VertexEdge) = _edge_has_qK_interface(x) ? 1 : 0
    spmat = _regime_graph_to_sparse(grh; weight_fn = f)
    return spmat
end

get_regimes_neighbor_mat(args...;kwargs...) =  get_regimes_neighbor_mat_qK(args...;kwargs...)




"""
    get_volumes(bnc::Bnc, vtxs=nothing; recalculate=false, kwargs...) -> Vector{Volume}

Return volumes for selected vertices, computing missing volumes as needed.
"""
function get_volumes(Bnc::Bnc,vtxs::Union{AbstractVector,Nothing}=nothing; 
    recalculate::Bool=false, 
    rebase_K::Bool = false, 
    rebase_mat:: Union{AbstractMatrix{<:Real},Nothing} = nothing,
    kwargs...)

    all_vtxs = isnothing(vtxs) ? get_regimes(Bnc;return_idx=true) : [get_idx(Bnc, vtx) for vtx in vtxs]

    vtxs_to_calc = 
        if recalculate
            all_vtxs
        else
            vertices = _bind_regimes_data(Bnc)
            filter(i -> isnothing(vertices[i].volume), all_vtxs)
        end
    
    if !isempty(vtxs_to_calc)

        rebase_mat = if  !isnothing(rebase_mat)
                    @assert !rebase_K "Cannot specify both rebase_K and providing rebase_mat"
                    rebase_mat
                elseif rebase_K
                    Q = rebase_mat_lgK(Bnc.N)
                    blockdiag(spdiagm(fill(Rational(1), Bnc.d)), Q)
                else
                    nothing
                end
        
        #ensure conditions for volume calculation are calced, may further replaced by other functions
        Threads.@threads for idx in vtxs_to_calc
           get_regime(Bnc,idx; inv_info=true)
        end
        
        vtx_data = @view _bind_regimes_data(Bnc)[vtxs_to_calc]
        rlts = calc_volume(vtx_data; rebase_mat=rebase_mat, kwargs...)
        for (i,idx) in enumerate(vtxs_to_calc)
            vtx = get_regime(Bnc,idx; inv_info=false)
            vtx.volume = rlts[i]
        end
    end
    return [vtx.volume for vtx in _bind_regimes_data(Bnc)[all_vtxs]]
end





#-------------------------------------------------------------------------------------------------------------




"""
    have_perm(bnc::Bnc, perm_or_idx) -> Bool

Return `true` when a permutation or index exists in the model.
"""
have_perm(Bnc::Bnc, perm::AbstractVector) = haskey(get_bind_regimes_dict(Bnc), get_perm(Bnc, perm))
have_perm(Bnc::Bnc, idx::Integer) = (find_all_regimes!(Bnc); 1 <= idx <= n_regimes(Bnc))
have_perm(Bnc::Bnc, rgm::BindRegime) = have_perm(Bnc, get_idx(rgm))


# --------------------------These properties are stored in Bnc as vector form when finding regimes, so we can access them directly.----------------------------
# """
# Gets the nullity of a vertex
# eg: get_nullity(model,perm)
#     get_nullity(vtx)
# """
# get_nullity(args...) = begin
#     model = get_binding_network(args...)
#     find_all_regimes!(model)
#     return model.vertices_nullity[get_idx(args...)]
# end::Integer

"""
    is_singular(args...) -> Bool

Return `true` if the vertex has nonzero nullity.
"""
is_singular(args...)= get_nullity(args...) > 0


"""
    is_asymptotic(args...) -> Bool

Return `true` if the vertex is asymptotic (real).
"""
is_asymptotic(args...) = begin
    model = get_binding_network(args...)
    find_all_regimes!(model)
    return _bind_regimes_data(model)[get_idx(args...)].is_asymptotic
end::Bool
is_asymptotic(vtx::BindRegime) = vtx.is_asymptotic



#---------------------------------These properties are calculate when creating BindRegime object---------------------------------------
"""
    get_P_P0(args...) -> (SparseMatrixCSC, Vector)

Return `(P, P0)` for a vertex, creating it if needed.
"""
get_P_P0(args...) = get_regime(args...; inv_info=false) |> vtx -> (vtx.P, vtx.P0)
"""
    get_P(args...) -> SparseMatrixCSC

Return `P` for a vertex.
"""
get_P(args...) = get_P_P0(args...)[1]
"""
    get_P0(args...) -> Vector

Return `P0` for a vertex.
"""
get_P0(args...) = get_P_P0(args...)[2]

"""
    get_M_M0(args...) -> (SparseMatrixCSC, Vector)

Return `(M, M0)` for a vertex, creating it if needed.
"""
get_M_M0(args...) = get_regime(args...; inv_info=false) |> vtx -> (vtx.M, vtx.M0)
"""
    get_M(args...) -> SparseMatrixCSC

Return `M` for a vertex.
"""
get_M(args...) = get_M_M0(args...)[1]
"""
    get_M0(args...) -> Vector

Return `M0` for a vertex.
"""
get_M0(args...) = get_M_M0(args...)[2]

"""
    get_C_C0_x(args...) -> (SparseMatrixCSC, Vector)

Return `(C_x, C0_x)` for a vertex.
"""
get_C_C0_x(args...) = get_regime(args...; inv_info=false) |> vtx -> (vtx.C_x, vtx.C0_x)
"""
    get_C_x(args...) -> SparseMatrixCSC

Return `C_x` for a vertex.
"""
get_C_x(args...) = get_C_C0_x(args...)[1]
"""
    get_C0_x(args...) -> Vector

Return `C0_x` for a vertex.
"""
get_C0_x(args...) = get_C_C0_x(args...)[2]


"""
    get_C_C0_nullity_qK(args...) -> (SparseMatrixCSC, Vector, Int)

Return `(C_qK, C0_qK, nullity)` for a vertex.
"""
get_C_C0_nullity_qK(args...) = get_regime(args...; inv_info=true) |> vtx -> (vtx.C_qK, vtx.C0_qK, vtx.nullity)
"""
    get_C_C0_qK(args...) -> (SparseMatrixCSC, Vector)

Return `(C_qK, C0_qK)` for a vertex.
"""
get_C_C0_qK(args...) = get_C_C0_nullity_qK(args...)[1:2]
"""
    get_C_qK(args...) -> SparseMatrixCSC

Return `C_qK` for a vertex.
"""
get_C_qK(args...) = get_C_C0_nullity_qK(args...)[1]
"""
    get_C0_qK(args...) -> Vector

Return `C0_qK` for a vertex.
"""
get_C0_qK(args...) = get_C_C0_nullity_qK(args...)[2]

"""
    get_nullity(args...) -> Int

Return the nullity of a vertex.
"""
get_nullity(args...) = get_regime(args...; inv_info=true).nullity

"""
    get_H_H0(args...) -> (SparseMatrixCSC, Vector)

Return `(H, H0)` for a regime with nullity at most 1.
"""
get_H_H0(args...) = get_nullity(args...) > 1 ? @error("BindRegime's nullity is bigger than 1, cannot get H0") : get_regime(args...; inv_info=true) |> rgm -> (rgm.H, rgm.H0)
"""
    get_H(args...) -> SparseMatrixCSC

Return `H` for a vertex when nullity <= 1.
"""
get_H(args...) = get_nullity(args...) > 1 ? @error("BindRegime's nullity is bigger than 1, cannot get H") : get_regime(args...; inv_info=true).H
"""
    get_H0(args...) -> Vector

Return `H0` for a vertex.
"""
get_H0(args...) = get_H_H0(args...)[2]

"""
    get_C_C0_nullity(args...; kwargs...) -> (Matrix, Vector, Int)

Return `(C, C0, nullity)` for a vertex or polyhedron.
"""
get_C_C0_nullity(args...;kwargs...) = get_C_C0_nullity_qK(args...;kwargs...)
"""
    get_C_C0(args...; kwargs...) -> (Matrix, Vector)

Return `(C, C0)` for a vertex or polyhedron.
"""
get_C_C0(args...;kwargs...) = get_C_C0_nullity(args...;kwargs...) |> x->(x[1], x[2]) 
"""
    get_C(args...; kwargs...) -> Matrix

Return `C` for a vertex or polyhedron.
"""
get_C(args...;kwargs...) = get_C_C0_nullity(args...;kwargs...)[1]
"""
    get_C0(args...; kwargs...) -> Vector

Return `C0` for a vertex or polyhedron.
"""
get_C0(args...;kwargs...) = get_C_C0_nullity(args...;kwargs...)[2]


"""
    n_regimes(bnc::Bnc) -> Int

Return the number of vertices in the model.
"""
n_regimes(Bnc::Bnc) = (find_all_regimes!(Bnc); length(_bind_regimes_data(Bnc)))

"""
    get_volume(args...; kwargs...) -> Volume

Return the volume for a single vertex.
"""
function get_volume(args...;  kwargs...)
    model = get_binding_network(args...)
    idx = get_idx(args...)
    return get_volumes(model, [idx]; kwargs...)[1]
end


#--------------------------------------------------------------------------------------------------------------------------------------
#          relationships between two vertices, based on regime graphs.
#----------------------------------------------------------------------------------------------------------------------------------------

"""
    _is_regime_graph_neighbor(bnc, vtx1, vtx2) -> Bool

Return `true` if vertices are neighbors in the vertex graph.
"""
function _is_regime_graph_neighbor(Bnc, vtx1, vtx2)::Bool
    grh = get_regimes_graph!(Bnc; full=true)
    edge = get_edge(grh, vtx1, vtx2; full=true)
    if edge === nothing || !_edge_has_qK_interface(edge)
        return false
    else
        return true
    end
end



"""
    get_interface_qK(bnc, from, to) -> (SparseVector, Float64)

Return the interface hyperplane between two vertices in qK space.
"""
function get_interface_qK(Bnc, from, to)::Tuple{SparseVector{Float64,Int}, Float64}
    grh = get_regimes_graph!(Bnc; full=true)
    edge = get_edge(grh, from, to; full=true)
    if edge === nothing
        @info "no directly edge found, judge using Polyhedra.jl, could be problematic if you concerning changing direction"
        return get_interface_direct(Bnc, from, to)
    elseif !_edge_has_qK_interface(edge)
        @error("Vertices $get_perm(Bnc, from) and $get_perm(Bnc, to) are neighbors in x space but not in qK space")
    else
        return _edge_qK_interface(grh, edge)
    end   
end

"""
    get_interface(args...; kwargs...) -> (SparseVector, Float64)

Convenience wrapper for `get_interface_qK`.
"""
get_interface(args...;kwargs...) = get_interface_qK(args...;kwargs...)
"""
    get_change_dir_qK(args...; kwargs...) -> SparseVector

Return the qK change direction between neighboring vertices.
"""
get_change_dir_qK(args...;kwargs...) = get_interface(args...;kwargs...)[1] # relys on the inner behavior of get_interface, 
"""
    get_change_dir(args...; kwargs...) -> SparseVector

Alias for `get_change_dir_qK`.
"""
get_change_dir(args...;kwargs...) = get_change_dir_qK(args...;kwargs...)

"""
    is_neighbor_qK(bnc, vtx1, vtx2) -> Bool

Return `true` if two vertices are neighbors in qK space.
"""
function is_neighbor_qK(Bnc, vtx1, vtx2)::Bool
    try get_interface_qK(Bnc, vtx1, vtx2)
        return true
    catch
        return false
    end
end

"""
    is_neighbor(args...; kwargs...) -> Bool

Alias for `is_neighbor_qK`.
"""
is_neighbor(args...;kwargs...) = is_neighbor_qK(args...;kwargs...)


"""
    get_interface_x(bnc::Bnc, from, to) -> (SparseVector, Float64)

Return the interface hyperplane between two vertices in x space.
"""
function get_interface_x(Bnc::Bnc, from, to)
    edge = get_edge(Bnc, from, to)
    if edge === nothing 
        @error("Vertices $get_perm(Bnc, from) and $get_perm(Bnc, to) are not neighbors in x space.")
    else 
        grh = get_regimes_graph!(Bnc; full=false)
        hp = grh.x_interface_pool[edge.c_c0_x_idx]
        return sparsevec(hp, Bnc.n, edge.c_c0_x_sign), hp.c0 * edge.c_c0_x_sign
    end
end

"""
    get_change_dir_x(args...; kwargs...) -> SparseVector

Return the x-space change direction between neighboring vertices.
"""
get_change_dir_x(args...;kwargs...) = get_interface_x(args...;kwargs...)[1]



"""
    get_neighbors(args...; singular=nothing, asymptotic=nothing, return_idx=false) -> Vector

Return neighbors of a vertex filtered by singularity and asymptotic flags.

# Keyword Arguments
- `singular`: `true`, `false`, integer threshold, or `nothing`.
- `asymptotic`: `true`, `false`, or `nothing`.
- `return_idx`: Return indices when `true`; otherwise return permutations.
"""
function get_neighbors(args...; singular::Union{Bool,Int,Nothing}=nothing, asymptotic::Union{Bool,Nothing}=nothing, return_idx::Bool=false)
    Bnc = get_binding_network(args...)
    grh = get_regimes_graph!(Bnc;full=true)
    rgm_idx = get_idx(args...)

    idx = keys(grh.edge_pos[rgm_idx]) |> collect
    
    vertices = _bind_regimes_data(Bnc)
    idx = filter(idx) do i
        vtx = vertices[i]
        nlt = vtx.nullity
        flag_asym = vtx.is_asymptotic

        ok_singular = isnothing(singular) || (
            (singular === true  && nlt > 0) ||
            (singular === false && nlt == 0) ||
            (singular isa Int   && nlt ≤ singular)
        )

        ok_asym = isnothing(asymptotic) || (asymptotic == flag_asym)
        return ok_singular && ok_asym 
    end

    sort!(idx)

    return return_idx ? idx : getfield.(vertices[idx], :perm)
end




#-------------------------------------------------------------------------------------
#         functions of getting regimes with certain properties
# -------------------------------------------------------------------------------------

# if pass vector of regimes
function _get_mask(rgms::AbstractVector{<:BindRegime};
     singular::Union{Bool,Integer,Nothing}=nothing, 
     asymptotic::Union{Bool,Nothing}=nothing)::Vector{Bool}
    
    @inline f(nlt) = isnothing(singular) || (
        (singular === true  && nlt > 0) ||
        (singular === false && nlt == 0) ||
        (singular isa Int   && nlt ≤ singular)
    )

    @inline g(flag_asym) = isnothing(asymptotic) || (asymptotic == flag_asym)

    return map(rgms) do vtx
        f(get_nullity(vtx)) && g(is_asymptotic(vtx))
    end
end
function filter_regimes(rgms::AbstractVector{<:BindRegime}; kwargs...)
    masks = _get_mask(rgms; kwargs...)
    return rgms[masks]
end


function _get_mask(model::Bnc,vtxs::AbstractVector{T};kwargs...)::Vector{Bool} where T
    rgms = get_regime.(Ref(model), vtxs)
    return _get_mask(rgms; kwargs...)
end
function filter_regimes(model::Bnc, vtxs::AbstractVector{T}; kwargs...)::Vector{T} where T
    rgms = get_regime.(Ref(model), vtxs)
    return filter_regimes(rgms; kwargs...)
end

_get_regimes_mask(args...; kwargs...) = _get_mask(args...; kwargs...)
get_idxes(args...; kwargs...) = get_indices(args...; kwargs...)



"""
    get_indices(bnc::Bnc; singular=nothing, asymptotic=nothing) -> Vector{Int}

Return regime indices that satisfy singularity/asymptotic filters.
"""
function get_indices(Bnc::Bnc; kwargs...)
    find_all_regimes!(Bnc)
    idx_all = eachindex(_bind_regimes_data(Bnc))
    masks = _get_mask(Bnc, idx_all; kwargs...)
    return findall(masks)
end

"""
    get_perms(bnc::Bnc; singular=nothing, asymptotic=nothing) -> Vector

Return regime permutations that satisfy singularity/asymptotic filters.
"""
function get_perms(Bnc::Bnc; kwargs...)
    idxs = get_indices(Bnc; kwargs...)
    return getfield.(_bind_regimes_data(Bnc)[idxs], :perm)
end

get_perms(rgms::AbstractVector{<:BindRegime};kwargs...) = let
    rgms = filter_regimes(rgms; kwargs...)
    return getfield.(rgms, :perm)
end

get_indices(rgms::AbstractVector{<:BindRegime};kwargs...) = let
    rgms = filter_regimes(rgms; kwargs...)
    return getfield.(rgms, :idx)
end

"""
    get_regimes(bnc::Bnc; singular=nothing, asymptotic=nothing, return_idx=false) -> Vector

Return cached `BindRegime`s that satisfy singularity/asymptotic filters.
Use `get_perms` or `get_indices` for permutation/index lists.
"""
function get_regimes(Bnc::Bnc; return_idx::Bool=false, kwargs...)
    idxs = get_indices(Bnc; kwargs...)
    return return_idx ? idxs : _bind_regimes_data(Bnc)[idxs]
end

"""
    get_nullities(bnc::Bnc, rgms=nothing) -> Vector

Return nullity values for selected regimes.
"""
function get_nullities(Bnc::Bnc, rgms::Union{AbstractVector,Nothing}=nothing; kwargs...) 
    all_rgms = isnothing(rgms) ? get_regimes(Bnc; return_idx=true,kwargs...) : [get_idx(Bnc, rgm) for rgm in rgms]
    [get_nullity(Bnc, idx) for idx in all_rgms]
end











#-------------------------------------------------------------
#Other higher lever functions
#----------------------------------------------------------------
"""
    summary_regime(args...) -> nothing

Print a detailed summary for a single vertex.
"""
function summary_regime(args...)
    idx= get_idx(args...)
    perm = get_perm(args...)
    is_real = is_asymptotic(args...)
    nullity = get_nullity(args...)
    volume = get_volume(args...)
    println("idx=$idx,perm=$perm, asymptotic=$is_real, nullity=$nullity")
    println("volume=$(volume.mean) +- $(sqrt(volume.var))")
    println("Dominante Relation")
    display.(show_dominant_condition(args...;log_space=false))
    println("Expression")
    try
        display.(show_expression_x(args...;log_space=false))
    catch
    end
    println("Condition:")
    display.(show_condition_qK(args...;log_space=false))
    
    return nothing
end

"""
    summary(bnc::Bnc, perm) -> nothing

Alias for `summary_regime`.
"""
summary(Bnc::Bnc, perm)= summary_regime(Bnc, perm)
"""
    summary(vtx::BindRegime) -> nothing

Alias for `summary_regime`.
"""
summary(vtx::BindRegime)= summary_regime(vtx)

@inline function _regime_display_dominant_mode(rgm::BindRegime)
    return "perm=$(get_perm(rgm))"
end

function Base.show(io::IO, rgm::BindRegime)
    print(
        io,
        "BindRegime(",
        _regime_display_dominant_mode(rgm),
        ", nullity=",
        get_nullity(rgm),
        ", asymptotic=",
        is_asymptotic(rgm),
        ")",
    )
end

function Base.show(io::IO, ::MIME"text/plain", rgm::BindRegime)
    println(io, "BindRegime")
    println(io, "  dominant mode: ", _regime_display_dominant_mode(rgm))
    println(io, "  nullity: ", get_nullity(rgm))
    print(io, "  asymptotic: ", is_asymptotic(rgm))
end


# function summary_vertices(Bnc::Bnc;kwargs...)
#     vtx = get_regimes(Bnc;kwargs...)
#     vtx .|> x->summary_regime(Bnc,x)
#     return nothing
# end


function get_function(vtx::BindRegime)
    H,H0 = get_H_H0(vtx)
    
    f = function(qK::AbstractArray{<:Real}; input_logspace::Bool=false, output_logspace::Bool=false)
            lgqK = input_logspace ? qK : log10.(qK)
            lgx = H * lgqK .+ H0
        return output_logspace ? lgx : exp10.(lgx)
    end

    return f
end







#===============================================================================================================#
# Polyhedron-related helper functions
#===============================================================================================================#


"""
    get_C_C0_nullity(poly::Polyhedron) -> (Matrix, Vector, Int)

Extract `(C, C0, nullity)` from a polyhedron in H-representation.
"""
function get_C_C0_nullity(poly::Polyhedron) #Have to make sure the polyhedron has been already detecthlinearity.
    p = MixedMatHRep(hrep(poly))
    C = -p.A
    C0 = p.b
    nullity = begin
        linset = p.linset
        if !isempty(linset)
            nty = maximum(linset)
            @assert linset == BitSet(1:nty)
        else
            nty = 0
        end
        nty
    end
    return (C, C0, nullity)
end
"""
    get_nullity(poly::Polyhedron, args...; kwargs...) -> Int

Return the nullity encoded in a polyhedron's linear constraints.
"""
get_nullity(poly::Polyhedron,args...;kwargs...) = get_C_C0_nullity(poly::Polyhedron,args...;kwargs...)[3]


"""
    get_polyhedron(C, C0, nullity=0) -> Polyhedron

Construct a polyhedron from inequality constraints in qK space.
"""
function get_polyhedron(C::AbstractMatrix{<:Real}, C0::AbstractVector{<:Real}, nullity::Integer=0)::Polyhedron 
    Cf = Matrix(Float64.(C))
    C0f = vec(Float64.(C0))
    if nullity ==0
        return hrep(-Cf, C0f) |> x-> polyhedron(x,CDDLib.Library())
    else
        linset = BitSet(1:nullity)
        return hrep(-Cf, C0f, linset) |> x-> polyhedron(x,CDDLib.Library())
    end
end
"""
    get_polyhedron(args...) -> Polyhedron

Convenience wrapper that pulls constraints from a vertex or model.
"""
get_polyhedron(args...)=get_polyhedron(get_C_C0_nullity_qK(args...)...)


"""
    get_intersect(bnc, vtx1, vtx2) -> Polyhedron

Return the intersection polyhedron between two vertices in qK space.
"""
function get_intersect(Bnc,vtx1,vtx2)::Polyhedron
    p1 = get_polyhedron(Bnc, vtx1)
    dim1 = dim(p1)
    p2 = get_polyhedron(Bnc, vtx2)
    dim2 = dim(p2)

    p = intersect(p1,p2)
    detecthlinearity!(p)
    # @show dim1, dim2, dim(p)
    if dim(p)< max(dim1,dim2)-1
        error("Vertices $(get_perm(Bnc, vtx1)) and $(get_perm(Bnc, vtx2)) do not have dim-1 intersect.")
    end
    return p
end


"""
    get_interface_direct(bnc::Bnc, from, to) -> (SparseVector, Float64)

Compute the interface hyperplane directly from polyhedral intersection.
"""
function get_interface_direct(Bnc::Bnc, from, to)::Tuple{SparseVector{Float64,Int}, Float64}
    p = get_intersect(Bnc, from, to)
    hplanes = hyperplanes(p)
    # @show hplanes
    hp = collect(hplanes)[end]
    a = droptol!(sparse(hp.a), 1e-10)
    b = -hp.β
    return a, b
end


"""
    hyperplane_project_func(polyhedra::Polyhedron) -> Function

Return a projection function onto the affine subspace defined by polyhedron hyperplanes.
"""
function hyperplane_project_func(polyhedra::T)::Function where T<:Polyhedron
    if !hashyperplanes(polyhedra)
        error("polyhedra doesn't have hyperplanes")
    end
    # A^⊤y =b to project to this subspace   
    A = stack([i.a for i in hyperplanes(polyhedra)])
    b = stack([i.β for i in hyperplanes(polyhedra)])
    @show A,b
    # Now we need to generate a function to project a point into this hyperplanes
    AAtA_inv = A*pinv(A'*A)
    b0 = AAtA_inv*b
    P0 = I(size(A,1))-AAtA_inv*A'
    return x -> P0*x+b0
end



"""
    get_one_inner_point(poly::Polyhedron; rand_line=true, rand_ray=true, extend=3) -> Vector

Return a point guaranteed to lie inside the polyhedron.
"""
function get_one_inner_point(poly::T;rand_line=true,rand_ray=true,extend=3) where T<:Polyhedron
    vrep_poly = MixedMatVRep(vrep(poly))
    point = [mean(p) for p in eachcol(vrep_poly.V)]
    ray_avg = zeros(size(point,1))
    for (i, ray) in enumerate(eachrow(vrep_poly.R))
        if i ∉ vrep_poly.Rlinset
            norm_ray = norm(ray)
            sigma = rand_ray ? (rand()+0.5)*extend : extend
            ray_avg .+= (ray ./ norm_ray .* sigma )
        else
            if rand_line
                norm_ray = norm(ray)
                sigma = (rand()-0.5)*extend
                ray_avg .+= (ray ./ norm_ray * sigma)
            end
        end
    end
    return (point.+ ray_avg)
end
"""
    get_one_inner_point(args...; kwargs...) -> Vector

Convenience wrapper that builds a polyhedron from a vertex/model.
"""
get_one_inner_point(args...;kwargs...)=get_one_inner_point(get_polyhedron(args...);kwargs...)


"""
    check_feasibility_with_constraint(args...; C, C0, nullity=0) -> Bool

Check whether a vertex/polyhedron remains feasible under extra constraints.
"""
function check_feasibility_with_constraint(args...;C::AbstractMatrix{<:Real},C0::AbstractVector{<:Real},nullity::Int=0)
    poly_additional = get_polyhedron(C,C0,nullity)
    poly = get_polyhedron(args...)
    ins = intersect(poly,poly_additional)
    @info "The dimension of the intersected polyhedra is $(dim(ins))"
    return !isempty(ins)
end

"""
    feasible_vertieces_with_constraint(bnc::Bnc; C, C0, nullity=0, kwargs...) -> Vector

Return vertices feasible under additional constraints.
"""
function feasible_vertieces_with_constraint(Bnc::Bnc; C::AbstractMatrix{<:Real},C0::AbstractVector{<:Real},nullity::Int=0,kwargs...)
    all_vtx = get_regimes(Bnc;kwargs...)
    feasible_vtx = Vector{eltype(all_vtx)}()
    for perm in all_vtx
        if check_feasibility_with_constraint(Bnc, perm; C=C, C0=C0, nullity=nullity)
            push!(feasible_vtx, perm)
        end
    end
    return feasible_vtx
end
