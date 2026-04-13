
# using GLMakie
# using Plots
using Symbolics
using Parameters
using LinearAlgebra
# using DifferentialEquations
import OrdinaryDiffEq as ODE
import DiffEqCallbacks as CB
using StatsBase
using SparseArrays
# using JuMP
# using CUDA # Speedup calculation for distance matrix
using DataStructures:Queue,enqueue!,dequeue!,isempty
# using Interpolations
using NonlinearSolve
using Statistics:quantile
using Distributions:Uniform, Normal

using Polyhedra#:vrep,hrep,eliminate,MixedMatHRep,MixedMatVRep,polyhedron,Polyhedron
import CDDLib

using Graphs
import Printf
import JSON3
import ImageFiltering: imfilter, Kernel

import Random
import Base: summary,show

#---------------------------plot dependency-----------------------------
using Makie
using GraphMakie
using GraphMakie.NetworkLayout
using Latexify

using ProgressMeter



# ---------------------Define the struct of binding and catalysis networks----------------------------------


abstract type AbstractBnc end


"""
    CatalysisData

Container for catalysis network metadata, including stoichiometric changes,
reaction orders, and rate constants.
"""
struct CatalysisData
    # Parameters for the catalysis networks
    S::Matrix{Int} # catalysis change in qK space, each column is a reaction
    aT::Matrix{Int} # catalysis index and coefficients, rate will be vⱼ=kⱼ∏xᵢ^aT_{j,i}, denote what species catalysis the reaction.

    k::Vector{<:Real} # rate constants for catalysis reactions
    cat_x_idx::Vector{Int} # index of the species that catalysis the reaction, if not provided, will be inferred from S

    r_cat::Int # number of catalysis reactions/species
    _S_sparse::SparseMatrixCSC{Float64,Int} # sparse version of S, used for fast calculation
    _aT_sparse::SparseMatrixCSC{Float64,Int}  # sparse version of aT, used for fast calculation

    function CatalysisData(n::Int, S, aT, k, cat_x_idx)
        
        # fufill aT and cat_x_idx, either derive one from another
        if isnothing(aT) && !isnothing(cat_x_idx)
            println("catalysis coefficients is set to 1 for all catalysis species as aT is not provided.")
            aT = _idx_val2Mtx(cat_x_idx, 1, n)
        elseif isnothing(cat_x_idx) && !isnothing(aT)
            cat_x_idx, _ = _Mtx2idx_val(aT)
        elseif !isnothing(aT) && !isnothing(cat_x_idx)
            tmp,_ = _Mtx2idx_val(aT)
            @assert tmp == cat_x_idx "cat_x_idx must be the same as the index of aT"
        end 

        if isnothing(S)
            @error "S must be provided"
        end
        if isnothing(aT)
            @error "aT or cat_x_idx must be provided"
        end

        #fufill k if not provided.
        if isnothing(k) 
            k = ones(size(aT, 1))
            @warn("k is not provided, initialized to ones")
        end

        #fufill S to fulllength to contain K, could be nothing
        if !isnothing(S) && size(S, 1) < n
             S = vcat(S, zeros(Int, n - size(S, 1), size(S, 2)))
        end
        
        # Validation
        n_cat,r_cat = size(S)
        @assert n_cat == n "S should have n rows"
        @assert size(aT, 1) == r_cat "Mismatch in catalysis reaction count"
        @assert size(aT,2) == n "Mismatch catalysis species, aT should have n columns"
        @assert length(k) == r_cat "Mismatch in catalysis reaction count"

        # Create sparse matrices
        _S_sparse = sparse(Float64.(S))
        _aT_sparse = sparse(Float64.(aT))

        new(S, aT, k, cat_x_idx, r_cat, _S_sparse, _aT_sparse)
    end
end

struct Volume
    mean::Float64
    var::Float64
end
"""
    fetch_mean_re(V::Volume) -> (Float64, Float64)

Return the mean and relative error (standard deviation / mean) for a `Volume`.
"""
fetch_mean_re(V::Volume) = (V.mean, sqrt(V.var)/V.mean)
"""
    Base.display(V::Volume)

Display a compact summary of a `Volume`.
"""
Base.display(V::Volume) = Printf.@sprintf("Volume(mean=%.3e, var=%.3e, rel_error=%.2f%%)", V.mean, V.var, (sqrt(V.var)/V.mean)*100)
"""
    Base.:+(v1::Volume, v2::Volume) -> Volume

Add two `Volume` values by summing means and variances.
"""
Base.:+(v1::Volume, v2::Volume) = Volume(v1.mean + v2.mean, v1.var + v2.var)
"""
    Base.:-(v1::Volume, v2::Volume) -> Volume

Add two `Volume` values by summing means and variances.
"""
Base.:-(v1::Volume, v2::Volume) = Volume(v1.mean - v2.mean, v1.var + v2.var)
"""
    Base.isless(a::Volume, b::Volume) -> Bool

Compare `Volume` objects by mean value.
"""
Base.isless(a::Volume, b::Volume) = a.mean < b.mean
"""
    Base.:(==)(a::Volume, b::Volume) -> Bool

Return `true` when two `Volume` objects have identical means.
"""
Base.:(==)(a::Volume, b::Volume) = a.mean == b.mean 
"""
    Base.zero(::Volume) -> Volume

Return a zero `Volume` with zero mean and variance.
"""
Base.zero(::Volume) = Volume(0.0, 0.0)

"""
    Vertex

Representation of a regime/vertex in a binding network, including cached
linear maps and polyhedral conditions.
"""
mutable struct Vertex{F,T}
    #--- Parent Bnc model reference ---
    bn::Union{AbstractBnc,Nothing} # Reference to the parent Bnc model
    # --- Initial / Identifying Properties ---
    perm::Vector{T} # The regime vector
    idx::Int # Index of the vertex in the Bnc.vertices list
    real::Bool # Whether the vertex is real or fake vertex.
    
    # --- Basic Calculated Properties ---
    P::SparseMatrixCSC{Int, Int}
    P0::Vector{F} 
    M::SparseMatrixCSC{Int, Int}
    M0::Vector{F} #
    C_x::SparseMatrixCSC{Int, Int}
    C0_x::Vector{F} 

    # --- Expensive Calculated Properties ---
    nullity::T
    H::SparseMatrixCSC{Float64, Int} # Taking inverse, can have Float.
    H0::Vector{F} 
    C_qK::SparseMatrixCSC{Float64, Int}
    C0_qK::Vector{F} 
    
    #---Realizibility Index
    volume::Volume

    # The inner constructor also needs to be updated for the parametric type
    function Vertex(;bn::Union{AbstractBnc,Nothing}=nothing, perm, P, P0::Vector{F}, M, M0, C_x, C0_x, idx,real,nullity::T) where {T<:Integer,F<:Real}
        # _M_lu = lu(M, check=false) # It's good practice to ensure M is Float64 for LU
        # Use new{T} to construct an instance of Vertex{T}
        return new{F,T}(bn, perm, idx,real, P, P0, M, M0, C_x, C0_x,
            nullity,
            SparseMatrixCSC{Float64, Int}(undef, 0, 0), # H
            Vector{F}(undef, 0),          # H0
            SparseMatrixCSC{Float64, Int}(undef, 0, 0), # C_qK
            Vector{F}(undef, 0),          # C0_qK
            Volume(0.0, 0.0) # volume
        )
    end
end

"""
    VertexEdge

Edge metadata connecting neighboring vertices in a regime graph.
"""
mutable struct VertexEdge{T}
    to::Int
    diff_r::Int
    change_dir_x::SparseVector{Int8, T}
    intersect_x::Float64
    change_dir_qK::Union{Nothing, SparseVector{Float64, T}}
    intersect_qK::Union{Nothing, Float64}
    function VertexEdge(to::Int, diff_r::Int, change_dir_x::SparseVector{Int8, T}, intersect_x::Float64) where {T}
        return new{T}(to, diff_r, change_dir_x, intersect_x,nothing,nothing)
    end
end

# Adjacency list + optional caches
"""
    VertexGraph

Adjacency structure for vertices with optional caches for change directions.
"""
mutable struct VertexGraph{T}
    bn::AbstractBnc
    x_grh::SimpleGraph 
    neighbors::Vector{Vector{VertexEdge{T}}}
    change_dir_qK_computed::Bool
    edge_pos::Vector{Dict{Int, Int}}  # (u,v) -> (u,edge_pos[u][v]) to locate the VertexEdge.
    function VertexGraph(bn::AbstractBnc, neighbors::Vector{Vector{VertexEdge{T}}}) where {T}
        edge_pos = [Dict{Int, Int}() for _ in 1:length(neighbors)]
        g = SimpleGraph(length(neighbors))
        for i in 1:length(neighbors)
            edges = neighbors[i]
            for (k, e) in enumerate(edges)
                edge_pos[i][e.to] = k
                add_edge!(g, i, e.to)
            end
        end
        return new{T}(bn, g, neighbors, false, edge_pos)
    end
end

"""
    Bnc

Binding network model with stoichiometry, conservation laws, and derived
structures for regime analysis.
"""
mutable struct Bnc{T} <: AbstractBnc # T is the int type to save all the indices
    # ----Parameters of the binding networks------
    N::Matrix{Int} # binding reaction matrix
    L::Matrix{Int} # conservation law matrix

    r::Int # number of reactions
    n::Int # number of variables
    d::Int # number of conserved quantities

    #-------symbols of species -----------
    x_sym::Vector{Num} # species symbols, each column is a species
    q_sym::Vector{Num}
    K_sym::Vector{Num}

    #-------Parameters of the catalysis networks------
    catalysis::Union{Any,Nothing} # Using Any for placeholder for CatalysisData

    #--------Vertex data--------

    #The following four are computed when finding regimes.
    vertices_perm::Vector{Vector{T}} # all feasible regimes.
    vertices_perm_dict::Dict{Vector{T},Int} # map from permutation vector to its idx in the vertices list
    vertices_asymptotic_flag::Vector{Bool} # While this vertice is real
    vertices_nullity::Vector{T} # nullity of one vertex.
    
    #The following are computed when building graphs.
    vertices_graph::Union{Any,Nothing} # Using Any for placeholder for VertexGraph
    vertices_data::Vector{Vertex} # Using Any for placeholder for Vertex
    _vertices_is_initialized::BitVector
    _vertices_volume_is_calced::BitVector
    _vertices_Nρ_inv_dict::Dict{Vector{T}, Tuple{SparseMatrixCSC{Float64, Int},T}} # cache the N_inv for each vertex permutation

    #------other helper parameters------
    direction::Int8 # direction of the binding reactions, determine the ray direction for invertible regime, calculated by sign of det[L;N]

    # Parameters act as the starting points used for qk mapping
    _anchor_log_x::Vector{<:Real}
    _anchor_log_qK::Vector{<:Real}

    #Parameters for mimic calculation process
    _is_change_of_K_involved::Bool  # whether the K is involved in the calculation process

    
    
    # sparse matrix for speeding up the calculation
    _L_sparse::SparseMatrixCSC{Int,Int} # sparse version of L, used for fast calculation
    _L_sparse_val_one::SparseMatrixCSC{Int,Int} # sparse version of L with only non-zero elements set to 1, used for fast calculation
    _valid_L_idx::Vector{Vector{Int}} #record the non-zero column position for each row.
    _C_partition_idx::Vector{Int}# record the row partition of C matrix of invertible regimes. L[i,:] will stands for C[_C_partition_idx[i]:C_partition_idx[i+1]-1,:]

    _N_sparse::SparseMatrixCSC{Int,Int} # sparse version of N transpose, used for fast calculation
    _LN_sparse::SparseMatrixCSC{Float64,Int} # sparse version of [L;N], used for fast calculation

    #------------below are helper parameters for fast updating  value of matrix of the form [L;N] ------------------
    _LN_top_idx::Vector{Int} # first d row index of _LN_sparse
    _LN_top_rows::Vector{Int} # the corresponding row number in L for _LN_top_idx
    _LN_top_cols::Vector{Int} # the corresponding column number in L for _LN_top_idx

    _LN_bottom_idx::Vector{Int} # last r row index of _LN_sparse
    _LN_bottom_rows::Vector{Int} # the corresponding row number in N for _LN_bottom_idx
    _LN_bottom_cols::Vector{Int} # the corresponding column number in N for _LN_bottom_idx
    _LN_top_diag_idx::Vector{Int} # the diagonal index of the top d rows of _LN_sparse, used for fast calculation

    _LN_lu::SparseArrays.UMFPACK.UmfpackLU{Float64,Int} # LU decomposition of _LNt_sparse, used for fast calculation
    # _val_num_L::Int # number of non-zero elements in the sparse matrix L
    

    # Inner constructor 
    function Bnc{T}(N, L, x_sym, q_sym, K_sym, catalysis) where {T<:Integer}
        # get desired values
        r, n = size(N)
        d, n_L = size(L)

        # Validate dimensions for binding network, check if its legal.
        @assert n == d + r "d+r is not equal to n"
        @assert n_L == n "L must have the same number of columns as N"

        @assert length(x_sym) == n "x_sym length must equal number of species (n)"
        @assert length(q_sym) == d "q_sym length must equal number of conserved quantities (d)"
        @assert length(K_sym) == r "K_sym length must equal number of reactions (r)"

        #The direction
        direction = sign(det([L;N])) # Ensure matrix is Float64 for det
        
        # A simplified check for catalysis.S - replace with your actual logic
        _is_change_of_K_involved = !isnothing(catalysis) #&& !all(@view(catalysis.S[r+1:end, :]) .== 0)

        #-------helper parameters-------------
        # paramters for default homotopcontinuous starting point.
        _anchor_log_x = zeros(n)
        _anchor_log_qK = vcat(vec(log10.(sum(L; dims=2))), zeros(r))

        # pre-calculate the non-zero position for L

        _L_sparse = sparse(L) # sparse version of L
        _L_sparse_val_one = sparse(sign.(L)) # sparse version of L with only non-zero elements set to 1
        _valid_L_idx = [findall(!iszero, @view L[i,:]) for i in 1:d]
        _C_partition_idx = Vector{Int}(undef, d+1)
        _C_partition_idx[1] = 1
        for i in 1:d
            _C_partition_idx[i+1] = _C_partition_idx[i] + length(_valid_L_idx[i])-1
        end  
        
        _N_sparse = sparse(N) # sparse version of N
        _LN_sparse = Float64.([_L_sparse; _N_sparse])
        (_LN_top_rows, _LN_top_cols, _LN_top_idx) = rowmask_indices(_LN_sparse, 1,d) # record the position of non-zero elements in L within _LN_sparse
        (_LN_bottom_rows, _LN_bottom_cols, _LN_bottom_idx) = rowmask_indices(_LN_sparse, d+1,n) # record the position of non-zero elements in N within _LN_sparse
        _LN_top_diag_idx = diag_indices(_LN_sparse, d)

        _LN_lu = lu(_LN_sparse) # LU decomposition of _LNt_sparse, used for fast calculation
        # _N_sparse = sparse(N) # sparse version of N, used for fast calculation

        new(
            # Fields 1-5
            N, L, r, n, d,
            # Fields 6-9
            x_sym, q_sym, K_sym, catalysis,
            # Fields 10-12 (Initialized empty)
            Vector{T}[],                # vertices_perm
            Dict{Vector{T},Int}(),            # verices_idx
            Bool[],                          # vertices_asymptotic_flag
            T[],                          # vertices_nullity
            nothing,                         # vertices_graph
            # SparseMatrixCSC{Bool, Int}(undef, 0, 0),             # vertices_neighbor_mat
            # SparseMatrixCSC{SparseVector{Int8,T}, Int}(undef, 0, 0),             # vertices_change_dir_x
            # SparseMatrixCSC{SparseVector{Float64,T}, Int}(undef, 0, 0),             # vertices_change_dir_qK
            # Int[],                           # _vertices_sym_invperm
            Vector{Vertex}(),              # vertices_data
            BitVector(),                     # _vertices_is_initialized
            BitVector(),                     # _R_idx_is_calced
            Dict{Vector{T}, Tuple{SparseMatrixCSC{Float64, Int},T}}(), # _vertices_perm_Ninv_dict
            # Fields 13-28 (Calculated values)
            direction,
            _anchor_log_x, _anchor_log_qK,
            _is_change_of_K_involved,

            _L_sparse,
            _L_sparse_val_one,
            _valid_L_idx,
            _C_partition_idx,

            _N_sparse,
            _LN_sparse,

            _LN_top_idx,_LN_top_rows,_LN_top_cols,
            _LN_bottom_idx,_LN_bottom_rows,_LN_bottom_cols,
            _LN_top_diag_idx,

            _LN_lu,
        )
    end
end



struct SISOPaths{T} 
    bn::Bnc{T}   # binding Newtork
    qK_grh::SimpleDiGraph # SimpleDiGraph in qK space
    change_qK_idx::T  # which qK is changing in this SISO graph

    sources::Vector{Int}  # source vertices in the graph
    sinks::Vector{Int}    # sink vertices in the graph
    paths_dict::Dict{Vector{Int},Int} # map from path (vector of vertex idx) to its idx in rgm_paths
    rgm_paths::Vector{Vector{Int}} #All paths from sources to sinks, each path is represented as a vector of vertex idx. Grows exponentially
    path_polys::Vector{Polyhedron} # the polyhedron for each path, lazily calculated when needed, stored in the same order as rgm_paths
    path_volume::Vector{Volume}# the volume for each path, lazily calculated when needed, stored in the same order as rgm_paths

    path_volume_is_calc::BitVector # whether the volume for each path is calculated, stored in the same order as rgm_paths
    path_polys_is_calc::BitVector # whether the polyhedron for each path is calculated, stored in the same order as rgm_paths
    
     function SISOPaths(model::Bnc{T}, qK_grh, change_qK_idx, sources, sinks, rgm_paths) where T
        path_polys = Vector{Polyhedron}(undef, length(rgm_paths))
        path_volume = Vector{Volume}(undef, length(rgm_paths))
        path_volume_is_calc = falses(length(rgm_paths))
        path_polys_is_calc = falses(length(rgm_paths))
        paths_dict = Dict{Vector{Int},Int}()
        for (i, p) in enumerate(rgm_paths)
            paths_dict[p] = i
        end  
        new{T}(model, qK_grh, change_qK_idx, 
            sources, sinks, 
            paths_dict,
            rgm_paths, path_polys, path_volume,
            path_volume_is_calc, path_polys_is_calc)
    end
end





"""
    Bnc(; N=nothing, L=nothing, x_sym=nothing, q_sym=nothing, K_sym=nothing,
        S=nothing, aT=nothing, k=nothing, cat_x_idx=nothing) -> Bnc

Construct a binding network model from stoichiometry (`N`) or conservation (`L`)
matrices and optional symbol metadata. Catalysis data can be attached through
`S`, `aT`, and `k`.

# Keyword Arguments
- `N`: Stoichiometry matrix (reactions × species).
- `L`: Conservation matrix (totals × species).
- `x_sym`: Symbols for species concentrations.
- `q_sym`: Symbols for total concentrations.
- `K_sym`: Symbols for binding constants.
- `S`: Catalysis change matrix in qK space.
- `aT`: Catalysis index and coefficient matrix.
- `k`: Catalysis rate constants.
- `cat_x_idx`: Index of catalytic species.

# Returns
- A `Bnc` model with derived matrices and caches initialized.
"""
function Bnc(;N=nothing,L=nothing,
    x_sym=nothing,q_sym=nothing,K_sym=nothing,
    S=nothing,aT=nothing,k=nothing,
    cat_x_idx=nothing,
)::Bnc
    # if N is not provided, derive it from L, if provided, check its linear indenpendency
    isnothing(N) ? (N = N_from_L(L)) : begin 
        r = size(N,1)
        row_idx = independent_row_idx(N)
        r_new = length(row_idx)
        r != r_new ? @warn("N has been reduced from $r to $r_new rows, for linear dependent.") : nothing
        N = N[row_idx, :] # reduce N to independent rows
        if !isnothing(K_sym) && length(K_sym) == r
            K_sym = K_sym[row_idx] # reduce K_sym to independent rows 
        end
    end

    !isnothing(L) || (L = L_from_N(N)) # if L is not provided, derive it from N

    r,n = size(N)
    d = size(L, 1)

    # Call the inner constructor
    # Number of variables in the binding network
    x_sym = isnothing(x_sym) ? Symbolics.variables(:x, 1:n) : name_converter(x_sym) # convert x_sym to a vector of symbols
    q_sym = isnothing(q_sym) ? Symbolics.variables(:q, 1:d) : name_converter(q_sym) # convert q_sym to a vector of symbols
    K_sym = isnothing(K_sym) ? Symbolics.variables(:K, 1:r) : name_converter(K_sym) # convert K_sym to a vector of symbols

    local catalysis_data::Union{CatalysisData,Nothing}
    if !isnothing(S) || !isnothing(aT) || !isnothing(k) || !isnothing(cat_x_idx)
        catalysis_data = CatalysisData(n, S, aT, k, cat_x_idx)
    else
        catalysis_data = nothing
    end

    T = get_int_type(n) 
    Bnc{T}(N, L, x_sym, q_sym, K_sym, catalysis_data)
end





"""
    update_catalysis!(bnc::Bnc; S=nothing, aT=nothing, k=nothing, cat_x_idx=nothing) -> Bnc

Attach or update catalysis data on a `Bnc` model in-place.

# Arguments
- `bnc`: Binding network model to update.

# Keyword Arguments
- `S`: Catalysis change matrix in qK space.
- `aT`: Catalysis index and coefficient matrix.
- `k`: Rate constants.
- `cat_x_idx`: Index of catalytic species.

# Returns
- The updated `bnc`.
"""
function update_catalysis!(Bnc::Bnc;
    S::Union{Matrix{Int},Nothing}=nothing,
    aT::Union{Matrix{Int},Nothing}=nothing,
    k::Union{Vector{<:Real},Nothing}=nothing,
    cat_x_idx::Union{Vector{Int},Nothing}=nothing,
    )
    if isnothing(bnc.catalysis)
        bnc.catalysis = CatalysisData(bnc.n, S, aT, k, cat_x_idx)
    else
        S = isnothing(S) ? bnc.catalysis.S : S
        aT = isnothing(aT) ? bnc.catalysis.aT : aT
        k = isnothing(k) ? bnc.catalysis.k : k
        cat_x_idx = isnothing(cat_x_idx) ? bnc.catalysis.cat_x_idx : cat_x_idx
        bnc.catalysis = CatalysisData(bnc.n, S, aT, k, cat_x_idx)
    end
    return bnc
end



include(joinpath(@__DIR__,"helperfunctions.jl"))
include(joinpath(@__DIR__,"qK_x_mapping.jl"))
include(joinpath(@__DIR__,"volume_calc.jl"))
include(joinpath(@__DIR__,"numeric.jl"))
include(joinpath(@__DIR__,"regime_enumerate.jl")) # before regimes.jl
include(joinpath(@__DIR__,"regimes.jl"))
include(joinpath(@__DIR__,"regime_assign.jl"))
include(joinpath(@__DIR__,"symbolics.jl"))
include(joinpath(@__DIR__,"regime_graphs.jl"))
include(joinpath(@__DIR__,"visualize.jl"))


"""
    summary(bnc::Bnc) -> String

Print a summary of a binding network model to standard output.
"""
function summary(Bnc::Bnc)
    println("----------Binding Network Summary:-------------")
    println("Number of species (n): ", Bnc.n)
    println("Number of conserved quantities (d): ", Bnc.d)
    println("Number of reactions (r): ", Bnc.r)
    println("L matrix: ", Bnc.L)
    println("N matrix: ", Bnc.N)
    println("Direction of binding reactions: ", Bnc.direction > 0 ? "forward" : "backward")
    catalysis_str = isnothing(Bnc.catalysis) ? "No" : "Yes"
    println("Catalysis involved: ", catalysis_str)
    is_regimes_built = isempty(Bnc.vertices_perm) ? "No" : "Yes"
    println("Regimes constructed: ", is_regimes_built)
    if !isempty(Bnc.vertices_perm)
        map = zip(Bnc.vertices_asymptotic_flag, Bnc.vertices_nullity .> 0) |> countmap
        println("Number of regimes: ", length(Bnc.vertices_perm))
        println("  - Invertible + Asymptotic: ", get(map, (true, false), 0))
        println("  - Singular +  Asymptotic: ", get(map, (true, true), 0))
        println("  - Invertible +  Non-Asymptotic: ", get(map, (false, false), 0))
        println("  - Singular +  Non-Asymptotic: ", get(map, (false, true), 0))
    end
    println("-----------------------------------------------")
end

"""
    show(io::IO, ::MIME"text/plain", bnc::Bnc)

Pretty-print a `Bnc` model in plain text contexts.
"""
function show(io::IO, ::MIME"text/plain", bnc::Bnc)
    println(io, "----------Binding Network Summary:-------------")
    println(io, "Number of species (n): ", bnc.n)
    println(io, "Number of conserved quantities (d): ", bnc.d)
    println(io, "Number of reactions (r): ", bnc.r)
    println(io, "L matrix: ", bnc.L)
    println(io, "N matrix: ", bnc.N)
    println(io, "Direction of binding reactions: ", bnc.direction > 0 ? "forward" : "backward")
    catalysis_str = isnothing(bnc.catalysis) ? "No" : "Yes"
    println(io, "Catalysis involved: ", catalysis_str)
    is_regimes_built = isempty(bnc.vertices_perm) ? "No" : "Yes"
    println(io, "Regimes constructed: ", is_regimes_built)
    if !isempty(bnc.vertices_perm)
        map = zip(bnc.vertices_asymptotic_flag, bnc.vertices_nullity .> 0) |> countmap
        println(io, "Number of regimes: ", length(bnc.vertices_perm))
        println(io, "  - Invertible + Asymptotic: ", get(map, (true, false), 0))
        println(io, "  - Singular +  Asymptotic: ", get(map, (true, true), 0))
        println(io, "  - Invertible +  Non-Asymptotic: ", get(map, (false, false), 0))
        println(io, "  - Singular +  Non-Asymptotic: ", get(map, (false, true), 0))
    end
    print(io, "-----------------------------------------------") # 最后一行可用 print 避免额外空行
end
