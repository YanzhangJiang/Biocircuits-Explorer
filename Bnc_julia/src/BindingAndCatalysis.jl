__precompile__(false)
module BindingAndCatalysis

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
# using IntegerSmithNormalForm # to get the maximum of denum 
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
const _BNC_HEADLESS = lowercase(get(ENV, "BNC_HEADLESS", "")) in ("1", "true", "yes", "on")

if !_BNC_HEADLESS
    import ImageFiltering: imfilter, Kernel
end

import Random
import Base: summary,show

export Bnc, update_catalysis!

#---------------------------plot dependency-----------------------------
if !_BNC_HEADLESS
    using Makie
    using GraphMakie
    using GraphMakie.NetworkLayout
end
using Latexify

using ProgressMeter



# ---------------------Define the struct of binding and catalysis networks----------------------------------

#===============================================================================================#
# Volume struct and associated operations for uncertainty quantification in regime volumes.
#===============================================================================================#
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
Base.display(V::Volume) = Printf.@sprintf("Volume(Mean=%.3e, STD=%.3e, RelError=%.2f%%)", V.mean, sqrt(V.var), (sqrt(V.var)/V.mean)*100)
Base.show(io::IO, V::Volume) = print(io, Printf.@sprintf("Volume(Mean=%.3e, STD=%.3e, RelError=%.2f%%)", V.mean, sqrt(V.var), (sqrt(V.var)/V.mean)*100))
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

Base.:*(c::Real, v::Volume) = Volume(c * v.mean, c^2 * v.var)
Base.:*(v::Volume, c::Real) = c * v
Base.:/(v::Volume, c::Real) = Volume(v.mean / c, v.var / c^2)


#===============================================================================================#
# Integration Helper struct
#===============================================================================================#

"""
    IntegrationHelper

Container for cached integration starting points and sparse matrix index helpers.
Used for Integration during homotopy continuation.
"""
mutable struct IntegrationHelper
    _anchor_log_x::Vector{<:Real}
    _anchor_log_qK::Vector{<:Real}

    _LN_top_idx::Vector{Int} # first d row index of _LN_sparse
    _LN_top_rows::Vector{Int} # the corresponding row number in L for _LN_top_idx
    _LN_top_cols::Vector{Int} # the corresponding column number in L for _LN_top_idx

    _LN_bottom_idx::Vector{Int} # last r row index of _LN_sparse
    _LN_bottom_rows::Vector{Int} # the corresponding row number in N for _LN_bottom_idx
    _LN_bottom_cols::Vector{Int} # the corresponding column number in N for _LN_bottom_idx
    _LN_top_diag_idx::Vector{Int} # the diagonal index of the top d rows of _LN_sparse, used for fast calculation

    _LN_sparse::SparseMatrixCSC{Float64,Int} # cached Float64.(sparse([L; N])) for numerical integration
    _LN_lu::Union{SparseArrays.UMFPACK.UmfpackLU{Float64,Int}, Nothing} # LU decomposition of _LNt_sparse, used for fast calculation
end


@inline function calc_integration_helper(L,N)
    n = size(L,2)
    d = size(L,1)
    r = size(N,1)
    _anchor_log_x = zeros(n)
    _anchor_log_qK = vcat(vec(log10.(sum(L; dims=2))), zeros(r))
    
    _LN_sparse = Float64.(sparse([L; N]))
    (_LN_top_rows, _LN_top_cols, _LN_top_idx) = rowmask_indices(_LN_sparse, 1,d) # record the position of non-zero elements in L within _LN_sparse
    (_LN_bottom_rows, _LN_bottom_cols, _LN_bottom_idx) = rowmask_indices(_LN_sparse, d+1,n) # record the position of non-zero elements in N within _LN_sparse
    _LN_top_diag_idx = diag_indices(_LN_sparse, d)
    
    _LN_lu = rank(_LN_sparse)== n ? lu(_LN_sparse) : nothing # LU decomposition of _LNt_sparse, used for fast calculation

    IntegrationHelper(
        _anchor_log_x,
        _anchor_log_qK,
        _LN_top_idx,
        _LN_top_rows,
        _LN_top_cols,
        _LN_bottom_idx,
        _LN_bottom_rows,
        _LN_bottom_cols,
        _LN_top_diag_idx,
        _LN_sparse,
        _LN_lu,
    )
end


struct NρCacheEntry
    deficiency::Int                    # row-rank deficiency of Nρ; for square Nρ this is nullity(Nρ)
    kind::UInt8                        # 0x00 = deficiency only, 0x01 = explicit inverse, 0x02 = rank-1 adjugate factors
    inv::SparseMatrixCSC{Float64,Int}  # valid iff kind == 0x01
    α::Float64                         # valid iff kind == 0x02
    u::Vector{Float64}                 # left null vector  (length r)
    v::Vector{Float64}                 # right null vector (length r)
end

const ExactAffineCoeff = Rational{Int}
const BindAffineMatrix = Union{
    SparseMatrixCSC{Float64,Int},
    SparseMatrixCSC{ExactAffineCoeff,Int},
}

abstract type AbstractBnc end
abstract type AbstractRegime end

@inline function _normalize_affine_mode(mode::Symbol)
    mode in (:float, :rational) || error("Unsupported H_mode=$mode. Use :float or :rational.")
    return mode
end

@inline _affine_mode(::AbstractBnc) = :float
@inline _affine_is_exact(model::AbstractBnc) = _affine_mode(model) === :rational
#=================================================================================#
# f(L) -> {P,P0,C,C0} associated structs and helpers
#=================================================================================#

"""
Canonical hyperplane

Stored in canonical form with `u < v`:

    z_u - z_v + log10(num/den) = 0

where `(num, den)` is the reduced integer ratio.
"""
struct Hyperplane_perm{Tv<:Integer} 
    u::Int # fast access #j2 by default 
    v::Int # fast access #j1 by default 

    num::Tv # reduced positive integer L_{i,j2}
    den::Tv # reduced positive integer L_{i,j1}
    c0::Float64 # pre-logarithm log10(num/den)
end

function Base.:*(hp::Hyperplane_perm, M::AbstractMatrix{<:Real})
    return M[hp.u, :] -M[hp.v, :]
end

function mul(hp::Hyperplane_perm, q::AbstractVector{<:Real}; with_c0::Bool=true)
    if with_c0
        return q[hp.u] - q[hp.v] .+ hp.c0
    else
        return q[hp.u] - q[hp.v]
    end
end

Base.:*(hp::Hyperplane_perm, q::AbstractVector{<:Real}) = mul(hp, q; with_c0=true)

@inline _calc_c(hp::Hyperplane_perm,n::Int,sign::Int8) = let 
    if sign > 0 
        return sparsevec([hp.u, hp.v], Int8[1 -1], n)
    else
        return sparsevec([hp.u, hp.v], Int8[-1, 1], n)
    end
end


"""
One oriented inequality induced by choosing p in row i.
If `sign == +1`, use the canonical side:
    crow * z + c0 > 0
If `sign == -1`, use the opposite side:
    crow_neg * z - c0 > 0

`competitor` is the losing column k compared against the perm dominant p.
`oriented_c0 = log10(L[i,p] / L[i,k])`

so the actual inequality is:
    z_p - z_k + oriented_c0 > 0
"""
struct ChoiceIneq
    hid::Int  # index into global hyperplane pool
    sign::Int8 # +1 for canonical side, -1 for opposite side
end

"""
Helper struct for managing matrix operations.
- `J[i]`: positive columns in row i
- `choice_slot[i][p]`: local slot of column p inside J[i], or 0 if p ∉ J[i]
- `choice_map[i][t]`: all oriented inequalities for choosing p = J[i][t]
- `hyperplanes`: global deduplicated hyperplane pool
- `asymptotic`: all asymptotic regimes
- `feasible`: all regimes feasible under the weighted constraints
"""
struct MatrixHelper{Tv<:Integer}
    n::Int # number of columns
    J::Vector{Vector{Int}} # positive columns idx for each row

    # Fast access from column index to "local slot" in J[i]/ choice_logcoeff[i]
    choice_slot::Vector{Vector{Int}} # k = choice_slot[i][p] denotes p is the k th positive column in row i, or 0 if p ∉ J[i]
    choice_logcoeff::Vector{Vector{Float64}} # choice_logcoeff[i] = [log10(L[i, j]) for j in Ji]

    rowptr::Vector{Int} # rowptr[i] gives the starting index of constraints for row i in the global constraint list

    total_constraints::Int # total number of constraints across all rows
    choice_map::Vector{Vector{Vector{ChoiceIneq}}} # choice_map[i][t] gives the list of oriented inequalities for choosing p = J[i][t]
    hyperplanes::Vector{Hyperplane_perm{Tv}} # global deduplicated hyperplane pool
end



#=================================================================================#
# Regimes associated structs, including regimes for binding, catalysis and the combined Bnc regimes, 
#=================================================================================#


struct Regimes{T,R<:AbstractRegime,A<:AbstractArray{R}}
    vertices_perm_dict::Dict{Vector{T},Int}
    vertices_data::A
end



"""
    BindRegime

Representation of a regime/vertex in a binding network, including cached
linear maps and polyhedral conditions.
"""
mutable struct BindRegime{F,T} <: AbstractRegime
    #--- Parent Bnc model reference ---
    network::Union{AbstractBnc,Nothing} # Reference to the parent Bnc model

    # --- Initial / Identifying Properties ---
    perm::Vector{T} # The regime vector
    idx::Int # Index of the vertex in the Bnc.vertices list
    is_asymptotic::Bool # Whether the vertex is asymptotic or not.

    # --- Basic Properties ---
    P::Union{SparseMatrixCSC{Int, Int}, Nothing}
    P0::Union{Vector{F}, Nothing}
    M::Union{SparseMatrixCSC{Int, Int}, Nothing}
    M0::Union{Vector{F}, Nothing}
    C_x::Union{SparseMatrixCSC{Int, Int}, Nothing}
    C0_x::Union{Vector{F}, Nothing}

    # --- Expensive Calculated Properties ---
    nullity::T
    H::Union{BindAffineMatrix, Nothing}
    H0::Union{Vector{F}, Nothing} 
    C_qK::Union{BindAffineMatrix, Nothing}
    C0_qK::Union{Vector{F}, Nothing} 
    
    volume::Union{Volume, Nothing}

    function BindRegime(; network=nothing, perm, idx, is_asymptotic, nullity::T) where {T<:Integer}
        return new{Float64,T}(network, perm, idx, is_asymptotic,
            nothing, nothing, nothing, nothing, nothing, nothing, # P, P0, M, M0, C_x, C0_x
            nullity,
            nothing, nothing, # H, H0
            nothing, nothing, # C_qK,C0_qK
            nothing
        )
    end
end




mutable struct CatalysisRegime <:AbstractRegime
    network::Union{AbstractBnc,Nothing} # Reference to the parent Bnc model
    perm::Vector{Int} # The regime vector
    idx::Int # Index of the vertex in the Catalysis.vertices list
    is_asymptotic::Bool # Whether the vertex is asymptotic or not.

    #--- Basic Properties ---
    P_pos_neg::Union{SparseMatrixCSC{Int, Int}, Nothing} # the vcat of P_pos and P_neg
    P0_pos_neg::Union{Vector{Float64}, Nothing} # the vcat of P0_pos and P0_neg
    
    P:: Union{SparseMatrixCSC{Int, Int}, Nothing} # P_pos - P_neg
    P0::Union{Vector{Float64}, Nothing} # P0_pos - P0_neg
    C::Union{SparseMatrixCSC{Int, Int}, Nothing} # the vcat of C_pos and C_neg
    C0::Union{Vector{Float64}, Nothing} # the vcat of C0_pos and C0_neg

    CΠ:: Union{SparseMatrixCSC{Int, Int}, Nothing} # the vcat of C_pos*Π and C_neg*Π
    PΠ:: Union{SparseMatrixCSC{Int, Int}, Nothing} # the vcat of (P_pos - P_neg)*Π
    function CatalysisRegime(; network=nothing, perm, idx, is_asymptotic) 
        return new(network, perm, idx, is_asymptotic,
            nothing, # P_pos_neg
            nothing, # P0_pos_neg
            nothing, # P
            nothing, # P0
            nothing, # C
            nothing, # C0
            nothing, # CΠ
            nothing  # PΠ
        )
    end
end

# for BncRegime, the x /xk conditions are already within bind_rgm or catalysis_rgm, 
# H_ss, H_0ss, C_qKk_ss, C_0qKk_ss
# C_qKk_cat, C_0qKk_cat, 
# C_xk_ss


mutable struct BncRegime <:AbstractRegime
    bind_rgm::BindRegime
    catalysis_rgm::CatalysisRegime

    H_bd::SparseMatrixCSC{Float64, Int} 
    is_stable::Int8 # 1 for stable, 0 for unstable, -1 for unknown # judge from d_stable

    #
    nlt::Int  
    H::Union{SparseMatrixCSC{Float64, Int}, Nothing}
    H0::Union{Vector{Float64}, Nothing}


    # Conditions
    ## x, k base
    # Directly extract from bind_rgm and catalysis_rgm, no need to calculate separately.
    
    ## q_cat, K, k base
    # Binding could directly extract from bind_rgm, catalysis needs to calculate seperately
    # If binding is singular, we need to Combine with M,M0 to do the elimination again
    
    C_qKk_cat::Union{SparseMatrixCSC{Float64, Int}, Nothing}
    C0_qKk_cat::Union{Vector{Float64}, Nothing}
    nlt_qKk_cat::Int

    ## q_ss, K, k base
    C_qKk_ss::Union{SparseMatrixCSC{Float64, Int}, Nothing}
    C0_qKk_ss::Union{Vector{Float64}, Nothing}
    function BncRegime(bind_rgm, catalysis_rgm)
        PΠ = get_PΠ(catalysis_rgm)
        H = get_H(bind_rgm)
        r_v = size(PΠ,1)
        H_bd = sparse(Float64.(PΠ * H[:,1:r_v]))
        return new(bind_rgm, catalysis_rgm, 
            H_bd, Int8(-1),
            -1,nothing, nothing,
            nothing,nothing, -1,  
            nothing, nothing)
    end
end



"""
    CatalysisData

Container for catalysis network metadata, including stoichiometric changes,
reaction orders, and rate constants.
"""
mutable struct CatalysisData <:AbstractBnc
    # Parameters for the catalysis networks
    bn::AbstractBnc # reference to the parent Bnc model, used for validation and consistency checks

    # Catalysis determining Matrix
    Γ::SparseMatrixCSC{Int,Int} # catalysis change in qK space, each column is a reaction
    Π::SparseMatrixCSC{Int,Int} # catalysis index and coefficients, rate will be vⱼ=kⱼ∏xᵢ^Π_{j,i}, denote what species catalysis the reaction.

    # Derived matrices 
    S::SparseMatrixCSC{Int,Int} # the full row rank version of Γ
    L_Γ::SparseMatrixCSC{Int,Int} # the left null space of Γ such that L_Γ^⊤ * Γ = 0

    # Derived parameters
    r_v::Int # number of independent catalysis reactions
    n_v::Int # number of flux
    d_w::Int # number of dependent conserved quantities.
    d_para::Int # number of parameter total concentrations

    # symbols of k
    k_sym::Vector{Num}


    # helper parameters for fast calculation, used for fast calculation of H and C_qK
    _S_sparse::SparseMatrixCSC{Float64,Int} # sparse version of Γ, used for fast calculation
    _Π_sparse::SparseMatrixCSC{Float64,Int}  # sparse version of Π, used for fast calculation

    #Catalysis regimes
    S_pos_neg::SparseMatrixCSC{Int,Int} # the vcat of positive and negative parts of S
    _S_helper::MatrixHelper

    CatalysisRegimes::Union{Regimes,Nothing} # Using Any for placeholder for CatalysisRegimes

    function CatalysisData(bn,Γ, Π, k_sym)
        Γ = sparse(Γ)
        Π = sparse(Π)
        d_wv, nv = size(Γ)
        n = size(Π,2)
        # Validation
        @assert size(Π,1) == length(k_sym) == nv "Γ's column number have to meet with total flux number and k_sym"
        @assert n == bn.n "Π's column number have to meet with the number of species n in the binding network"
        L_Γ, pivits = left_nullspace_integer(Γ)

        r_v = length(pivits)
        d_w = size(L_Γ,2)
        d_para = bn.d - r_v

        # reorder and fix the binding network
        no_pivits = setdiff(1:d_wv, pivits)
        S = Γ[pivits, :]
        new_ord = vcat(pivits,no_pivits)
        Γ = Γ[new_ord, :]
        L_Γ = L_Γ[new_ord, :]
        fix_bn_catalysis!(bn, new_ord, L_Γ)

        # Create sparse matrices
        _S_sparse = sparse(Float64.(S))
        _Π_sparse = sparse(Float64.(Π))

        S_pos_neg = S_to_S_pos_neg(S)
        _S_helper = _build_matrix_helper(S_pos_neg)

        new(bn, Γ, Π, S, L_Γ,
            r_v, nv, d_w, d_para,    
            k_sym, _S_sparse, _Π_sparse,
            S_pos_neg, _S_helper, nothing)
    end
end






"""
    Bnc

Binding network model with stoichiometry, conservation laws, and derived
structures for regime analysis.
"""
mutable struct Bnc{T} <: AbstractBnc # T is the int type to save all the indices
    # ----Parameters of the binding networks------
    N::SparseMatrixCSC{Int,Int} # binding reaction matrix
    L::SparseMatrixCSC{Int,Int} # conservation law matrix

    r::Int # number of reactions
    n::Int # number of variables
    d::Int # number of conserved quantities
    # lcm::Int # least common multiple of [L;N]^{-1}

    #-------symbols of species -----------
    x_sym::Vector{Num} # species symbols, each column is a species
    q_sym::Vector{Num}
    K_sym::Vector{Num}

    #-------Parameters of the catalysis networks------
    catalysis::Union{Any,Nothing} # Using Any for placeholder for CatalysisData

    #--------Binding regimes data--------
    BindRegimes::Union{Regimes, Nothing}

    #-------Mixed regimes data--------
    BncRegimes::Union{Any, Nothing}

    #The following are computed when building graphs.
    vertices_graph::Union{Any,Nothing} # Using Any for placeholder for VertexGraph
    # _vertices_Nρ_inv_dict::Dict{Vector{T}, Tuple{SparseMatrixCSC{Float64, Int},T}} # cache the N_inv for each vertex permutation
    _vertices_Nρ_inv_dict :: Union{Any,Nothing}
    _regimes_affine_ready::Bool
    _regimes_affine_lock::ReentrantLock
    _integration_helper_lock::ReentrantLock

    #------other helper parameters------
    direction::Int8 # direction of the binding reactions, determine the ray direction for invertible regime, calculated by sign of det[L;N]
    affine_coeff_mode::Symbol
    IntegrationHelper::Union{IntegrationHelper,Nothing}
    _L_helper::MatrixHelper

    # Inner constructor 
    function Bnc{T}(N, L, x_sym, q_sym, K_sym, catalysis) where {T<:Integer}
        N_sparse = sparse(N)
        L_sparse = sparse(L)
        N_dense = Matrix{Int}(N)
        L_dense = Matrix{Int}(L)

        # get desired values
        r, n = size(N_dense)
        d, n_L = size(L_dense)
        # Validate dimensions for binding network, check if its legal.
        let 
            @assert n == d + r "d+r is not equal to n"
            @assert n_L == n "L must have the same number of columns as N"
            @assert length(x_sym) == n "x_sym length must equal number of species (n)"
            @assert length(q_sym) == d "q_sym length must equal number of conserved quantities (d)"
            @assert length(K_sym) == r "K_sym length must equal number of reactions (r)"
        end

        #The direction and lcm
        M = vcat(L_dense, N_dense)
        direction = sign(det(M)) # Ensure matrix is Float64 for det
        # lcm = get_max_denom(M)
        #-------helper parameters-------------
        # paramters for default homotopcontinuous starting point.
        _L_helper = _build_matrix_helper(L)
        new(
            # Fields 1-5
            N_sparse, L_sparse, r, n, d,# lcm,
            # Fields 6-9
            x_sym, q_sym, K_sym, catalysis,
            # Fields 10-12 (Initialized empty)
            nothing,                         # BindRegimes
            nothing,                         # BncRegimes
            nothing,                         # vertices_graph
            nothing,                         # _vertices_perm_Ninv_dict
            false,                           # _regimes_affine_ready
            ReentrantLock(),                 # _regimes_affine_lock
            ReentrantLock(),                 # _integration_helper_lock
            # Fields 13-28 (Calculated values)
            direction,
            :float,                          # affine_coeff_mode
            nothing,
            _L_helper,

        )
    end
end


@inline _affine_mode(model::Bnc) = getfield(model, :affine_coeff_mode)
@inline _bind_regimes(model::Bnc) = getfield(model, :BindRegimes)
@inline _bind_regimes_built(model::Bnc) = !isnothing(_bind_regimes(model))
@inline _bind_regimes_data(model::Bnc)= _bind_regimes(model).vertices_data


@inline function _bind_regimes_perm_dict(model::Bnc{T}) where T
    regimes = _bind_regimes(model)
    return isnothing(regimes) ? Dict{Vector{T},Int}() : regimes.vertices_perm_dict
end

function Base.getproperty(model::Bnc{T}, sym::Symbol) where {T}
    sym === :vertices_perm && return getfield.(_bind_regimes_data(model), :perm)
    sym === :vertices_perm_dict && return _bind_regimes_perm_dict(model)
    sym === :vertices_data && return _bind_regimes_data(model)
    return getfield(model, sym)
end
function Base.propertynames(model::Bnc, private::Bool=false)
    names = Symbol[fieldnames(typeof(model))...,
        :vertices_perm,
        :vertices_perm_dict,
        :vertices_data,
    ]
    return private ? Tuple(unique(names)) : Tuple(sym for sym in unique(names) if !startswith(String(sym), "_"))
end




pth1 = joinpath(@__DIR__,"Mathcore/")

include(joinpath(@__DIR__, "initialize.jl"))
include(joinpath(pth1,"find_matrix_vertex.jl")) # before regimes.jl
include(joinpath(pth1,"d_stable.jl"))
include(joinpath(pth1,"perm_graph_core.jl"))
include(joinpath(pth1,"SparseSparse_modified.jl"))

include(joinpath(@__DIR__,"helperfunctions.jl"))
include(joinpath(pth1,"matrix_inverse.jl"))
include(joinpath(pth1,"graph_propagate.jl"))
include(joinpath(@__DIR__,"qK_x_mapping.jl"))
include(joinpath(@__DIR__,"volume_calc.jl"))
include(joinpath(@__DIR__,"numeric.jl"))

include(joinpath(@__DIR__,"regimes.jl"))
include(joinpath(@__DIR__,"Catalysis_regime.jl"))
include(joinpath(@__DIR__,"Bnc_regime.jl"))

include(joinpath(@__DIR__,"regime_assign.jl"))
include(joinpath(@__DIR__,"SISO.jl"))
include(joinpath(@__DIR__,"symbolics.jl"))
if !_BNC_HEADLESS
    include(joinpath(@__DIR__,"visualize.jl"))
end
include(joinpath(@__DIR__,"old_api.jl"))

# === ROP periodic-table overlay (LOCAL, re-synced onto upstream API) ==============
# Relocated user code, included AFTER old_api.jl (dependencies first).
include(joinpath(@__DIR__, "rop", "rop_change_paths.jl"))
include(joinpath(@__DIR__, "rop", "rop_overlay.jl"))
include(joinpath(@__DIR__, "rop", "rop_periodic_table.jl"))
include(joinpath(@__DIR__, "rop", "rop_plot.jl"))
include(joinpath(@__DIR__, "rop", "rop_exports.jl"))




end # module
