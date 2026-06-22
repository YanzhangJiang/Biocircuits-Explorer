export match_regimes!, get_bnc_regime, get_bnc_regimes, n_bnc_regimes
export get_binding_regime, get_binding_perm, get_catalysis_perm, get_steady_state_perm
export get_C_C0_xk, get_C0_xk, get_C_xk
export get_C_C0_qKk, get_C0_qKk, get_C_qKk, get_C_C0_nullity_qKk
export get_C_C0_qssKk, get_C0_qssKk, get_C_qssKk, get_C_C0_nullity_qssKk
export get_H_bd, get_qcat_F_F0
export judge_stability!, is_stable

#============================================================================================#
#                            Functions related to BncRegime
#
#============================================================================================#

@inline _spI(T, n) = spdiagm(0 => ones(T, n))
@inline _float_sparse(A::SparseMatrixCSC{Float64,Int}) = A
@inline _float_sparse(A::SparseMatrixCSC{<:Real,Int}) = sparse(Float64.(A))
@inline _float_vec(v::AbstractVector{<:Real}) = Float64.(v)


get_binding_regime(rgm::BncRegime) = rgm.bind_rgm
get_catalysis_regime(rgm::BncRegime) = rgm.catalysis_rgm

get_binding_perm(rgm::BncRegime) = get_perm(rgm.bind_rgm)
get_catalysis_perm(rgm::BncRegime) = get_perm(rgm.catalysis_rgm)

"""
    get_perm(rgm::BncRegime)

Extract the binding permutation restricted to the steady-state coordinates
q_ss = (w, q_para), i.e. drop the first r_v catalysis-active rows.
"""
function get_perm(rgm::BncRegime)
    r_v = size(rgm.catalysis_rgm.P, 1)
    return rgm.bind_rgm.perm[r_v+1:end]
end
get_steady_state_perm(rgm::BncRegime) = get_perm(rgm)

get_idx(rgm::BncRegime) = CartesianIndex(get_idx(rgm.catalysis_rgm), get_idx(rgm.bind_rgm))


@inline is_bnc_regimes_built(model::Bnc) = !isnothing(model.BncRegimes)

function _build_BncRegime(cat_rgms::Regimes, bind_rgms::Regimes)
    n_cat_rgms = length(cat_rgms.vertices_data)
    n_bind_rgms = length(bind_rgms.vertices_data)
    bncrgms = Matrix{Union{BncRegime,Nothing}}(undef, n_cat_rgms, n_bind_rgms)

    @info "Matching Catalysis Regimes and Binding Regimes to build BncRegimes..."
    Threads.@threads for i in 1:n_cat_rgms
        cat_rgm = cat_rgms.vertices_data[i]
        for j in 1:n_bind_rgms
            bind_rgm = bind_rgms.vertices_data[j]
            bncrgms[i, j] = bind_rgm.nullity > 1 ? nothing : BncRegime(bind_rgm, cat_rgm)
        end
    end
    @info "Finished matching BncRegimes."
    return bncrgms
end


function get_idx(model::Bnc, bind, cat; check::Bool=false)
    cat_idx = get_idx(_require_catalysis_network(model), cat; check=check)
    bind_idx = get_idx(model, bind; check=check)
    return CartesianIndex(cat_idx, bind_idx)
end

function have_perm(model::Bnc, bind, cat)
    if !have_perm(model, bind)
        return false
    end
    cn = get_catalysis_network(model)
    if isnothing(cn) || !have_perm(cn, cat)
        return false
    end
    match_regimes!(model)
    return !isnothing(model.BncRegimes[get_idx(model, bind, cat)])
end

function get_bnc_regime(model::Bnc, bind, cat; check::Bool=false)
    match_regimes!(model)
    idx = get_idx(model, bind, cat; check=check)
    rgm = model.BncRegimes[idx]
    if isnothing(rgm)
        check && error("No BncRegime is stored for the requested binding/catalysis pair.")
        return nothing
    end
    return rgm
end
get_regime(model::Bnc, bind, cat; kwargs...) = get_bnc_regime(model, bind, cat; kwargs...)
get_regime(rgm::BncRegime; kwargs...) = rgm

function get_bnc_regimes(model::Bnc; return_idx::Bool=false, singular::Union{Bool,Integer,Nothing}=nothing)
    match_regimes!(model)
    idxs = CartesianIndex[]
    rgms = BncRegime[]

    for I in CartesianIndices(model.BncRegimes)
        rgm = model.BncRegimes[I]
        isnothing(rgm) && continue

        nlt = rgm.nlt
        keep = isnothing(singular) || (
            (singular === true  && nlt > 0) ||
            (singular === false && nlt == 0) ||
            (singular isa Int   && nlt <= singular)
        )
        keep || continue

        push!(idxs, I)
        push!(rgms, rgm)
    end

    return return_idx ? idxs : rgms
end

n_bnc_regimes(model::Bnc; kwargs...) = length(get_bnc_regimes(model; kwargs...))


function get_H_H0(rgm::BncRegime)
    rgm.nlt <= 1 || error("BncRegime nullity is bigger than 1, cannot get H0.")
    return rgm.H, rgm.H0
end
get_H(rgm::BncRegime) = rgm.H
get_H0(rgm::BncRegime) = get_H_H0(rgm)[2]
get_H_bd(rgm::BncRegime) = rgm.H_bd

function judge_stability!(rgm::BncRegime; kwargs...)
    rgm.is_stable = Int8(judge_dstable(rgm.H_bd; kwargs...))
    return rgm.is_stable
end

function is_stable(rgm::BncRegime; recalculate::Bool=false, return_code::Bool=false, kwargs...)
    code = (recalculate || rgm.is_stable < 0) ? judge_stability!(rgm; kwargs...) : rgm.is_stable
    return return_code ? code : (code == 1 ? true : code == 0 ? false : missing)
end
is_stable(model::Bnc, bind, cat; kwargs...) = is_stable(get_bnc_regime(model, bind, cat; check=true); kwargs...)


function _binding_C_qKk(bind_rgm::BindRegime, n_v::Int)
    C_qK, C0_qK, nlt = get_C_C0_nullity_qK(bind_rgm)
    C_qK = _float_sparse(C_qK)
    C = hcat(C_qK, spzeros(Float64, size(C_qK, 1), n_v))
    return C, _float_vec(C0_qK), nlt
end

function _calc_C_qKk_catalysis_only_regular(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    H, H0 = get_H_H0(bind_rgm)
    H = _float_sparse(H)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)
    C = hcat(CΠ * H, Cθ)
    C0 = CΠ * H0 + C0θ
    return C, Vector{Float64}(C0), 0
end

function _calc_C_qKk_catalysis_only_singular(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)
    M, M0 = get_M_M0(bind_rgm)

    n_qK = size(M, 1)
    n_x = size(M, 2)
    n_v = size(Cθ, 2)
    d_cat = size(CΠ, 1)

    Eq = hcat(-_spI(Int, n_qK), spzeros(n_qK, n_v), M)
    In_cat = hcat(spzeros(d_cat, n_qK), Cθ, CΠ)

    C = vcat(Eq, In_cat)
    C0 = vcat(M0, C0θ)

    p = get_polyhedron(C, C0, n_qK)
    delset = BitSet((n_qK + n_v + 1):(n_qK + n_v + n_x))
    p2 = eliminate(p, delset)

    return get_C_C0_nullity(p2)
end

function _calc_C_qKk_catalysis_only(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    if is_singular(bind_rgm)
        return _calc_C_qKk_catalysis_only_singular(bind_rgm, cat_rgm)
    else
        return _calc_C_qKk_catalysis_only_regular(bind_rgm, cat_rgm)
    end
end


function get_C_C0_nullity_xk(rgm::BncRegime, kind::Symbol=:combined)
    bind_rgm = rgm.bind_rgm
    cat_rgm = rgm.catalysis_rgm
    n_v = size(cat_rgm.P, 2)

    if kind === :binding
        C_x, C0_x = get_C_C0_x(bind_rgm)
        C = hcat(C_x, spzeros(Float64, size(C_x, 1), n_v))
        return C, Float64.(C0_x), 0
    elseif kind === :catalysis
        return get_C_C0_nullity_xk(cat_rgm)
    elseif kind === :combined
        Ceq = get_P_xk(cat_rgm)
        Ccat = get_C_xk(cat_rgm)
        Cbind_x, C0bind_x = get_C_C0_x(bind_rgm)
        Cbind = hcat(Cbind_x, spzeros(Float64, size(Cbind_x, 1), n_v))
        C = vcat(Ceq, Cbind, Ccat)
        C0 = vcat(
            Float64.(get_P0(cat_rgm)),
            Float64.(C0bind_x),
            Float64.(get_C0(cat_rgm)),
        )
        return C, C0, size(Ceq, 1)
    else
        error("Unsupported kind=$kind. Use :binding, :catalysis, or :combined.")
    end
end

function get_C_C0_xk(rgm::BncRegime, kind::Symbol=:combined)
    C, C0, _ = get_C_C0_nullity_xk(rgm, kind)
    return C, C0
end
get_C_xk(rgm::BncRegime, kind::Symbol=:combined) = get_C_C0_nullity_xk(rgm, kind)[1]
get_C0_xk(rgm::BncRegime, kind::Symbol=:combined) = get_C_C0_nullity_xk(rgm, kind)[2]


function get_C_C0_nullity_qKk(rgm::BncRegime, kind::Symbol=:combined)
    n_v = size(rgm.catalysis_rgm.P, 2)

    if kind === :binding
        return _binding_C_qKk(rgm.bind_rgm, n_v)
    elseif kind === :catalysis
        return _calc_C_qKk_catalysis_only(rgm.bind_rgm, rgm.catalysis_rgm)
    elseif kind === :combined
        return rgm.C_qKk_cat, rgm.C0_qKk_cat, rgm.nlt_qKk_cat
    else
        error("Unsupported kind=$kind. Use :binding, :catalysis, or :combined.")
    end
end

function get_C_C0_qKk(rgm::BncRegime, kind::Symbol=:combined)
    C, C0, _ = get_C_C0_nullity_qKk(rgm, kind)
    return C, C0
end
get_C_qKk(rgm::BncRegime, kind::Symbol=:combined) = get_C_C0_nullity_qKk(rgm, kind)[1]
get_C0_qKk(rgm::BncRegime, kind::Symbol=:combined) = get_C_C0_nullity_qKk(rgm, kind)[2]


function get_C_C0_nullity_qssKk(rgm::BncRegime)
    return rgm.C_qKk_ss, rgm.C0_qKk_ss, rgm.nlt
end

function get_C_C0_qssKk(rgm::BncRegime)
    C, C0, _ = get_C_C0_nullity_qssKk(rgm)
    return C, C0
end
get_C_qssKk(rgm::BncRegime) = get_C_C0_nullity_qssKk(rgm)[1]
get_C0_qssKk(rgm::BncRegime) = get_C_C0_nullity_qssKk(rgm)[2]


function get_qcat_F_F0(rgm::BncRegime)
    rgm.nlt == 0 || error("The reduced steady-state system is singular, so q_cat has no affine expression in (q_ss, K, k).")
    r_v = size(rgm.catalysis_rgm.P, 1)
    P_cat = rgm.bind_rgm.P[1:r_v, :]
    P0_cat = rgm.bind_rgm.P0[1:r_v]
    F = P_cat * rgm.H
    F0 = P0_cat + P_cat * rgm.H0
    return F, Vector{Float64}(F0)
end


"""
    _first_nonempty_regime(rgms)

Return the first non-`nothing` BncRegime in a matrix of candidate mixed regimes.
Useful for reading shared dimensions safely.
"""
function _first_nonempty_regime(rgms::AbstractMatrix{<:Union{BncRegime,Nothing}})
    pos = findfirst(x -> !isnothing(x), rgms)
    pos === nothing && return nothing
    return rgms[pos]::BncRegime
end

"""
    _row_valid_columns(rgms, i)

For a fixed catalysis regime row `i`, return the binding-regime columns that are
actually present (i.e. not `nothing`).
"""
function _row_valid_columns(rgms::AbstractMatrix{<:Union{BncRegime,Nothing}}, i::Int)
    return [j for j in axes(rgms, 2) if !isnothing(rgms[i, j])]
end

function _row_unique_perm_data(perms)
    perm_keys = Vector{Tuple{Vararg{Int}}}(undef, length(perms))
    unique_keys = Tuple{Vararg{Int}}[]
    first_pos = Int[]
    key_to_pos = Dict{Tuple{Vararg{Int}},Int}()

    for (k, perm) in enumerate(perms)
        key = Tuple(Int.(perm))
        perm_keys[k] = key
        if !haskey(key_to_pos, key)
            key_to_pos[key] = length(unique_keys) + 1
            push!(unique_keys, key)
            push!(first_pos, k)
        end
    end

    return perm_keys, unique_keys, first_pos
end

function _build_row_affine_cache(rgms, i, valid_js, perms, nlt_valid, N_ss, r_v, direction, cache)
    perm_keys, unique_keys, first_pos = _row_unique_perm_data(perms)
    Hs = Vector{Union{Nothing,SparseMatrixCSC{Float64,Int}}}(undef, length(unique_keys))
    H0s = Vector{Union{Nothing,Vector{Float64}}}(undef, length(unique_keys))

    Threads.@threads for t in eachindex(unique_keys)
        k = first_pos[t]
        nlt = nlt_valid[k]
        if nlt > 1
            Hs[t] = nothing
            H0s[t] = nothing
            continue
        end

        rgm = rgms[i, valid_js[k]]::BncRegime
        _initialize_regime!(rgm.bind_rgm)
        perm = perms[k]
        _, M0_ss = _steady_state_offsets(rgm, r_v, N_ss)

        H_ss = if nlt == 0
            _calc_H(N_ss, cache, perm)
        else
            M_ss = vcat(rgm.bind_rgm.P[r_v+1:end, :], N_ss)
            if allunique(perm)
                _calc_H(N_ss, cache, perm; scale = direction)
            else
                H_tmp = _adj_singular_matrix(M_ss)[1]
                droptol!(sparse(H_tmp), 1e-10) .* direction
            end
        end

        Hs[t] = H_ss
        H0s[t] = vec(-(H_ss * M0_ss))
    end

    affine_by_perm = Dict{Tuple{Vararg{Int}},Tuple{SparseMatrixCSC{Float64,Int},Vector{Float64}}}()
    for t in eachindex(unique_keys)
        Hs[t] === nothing && continue
        affine_by_perm[unique_keys[t]] = (Hs[t], H0s[t])
    end

    return perm_keys, affine_by_perm
end


"""
    _build_row_context(rgms, i, r_v)

Build row-shared data for the i-th catalysis regime. All mixed regimes in the same
row share the same catalysis regime, hence they share:
- N_ss = [N; PΠ]
- direction = sign(det([L_ss; N_ss]))
- nullity/cache data computed from the steady-state permutations.
"""
function _build_row_context(rgms::AbstractMatrix{<:Union{BncRegime,Nothing}}, i::Int, r_v::Int)
    valid_js = _row_valid_columns(rgms, i)
    isempty(valid_js) && return nothing

    ref_vtx = rgms[i, first(valid_js)]::BncRegime
    bn = ref_vtx.bind_rgm.network

    N_ss = vcat(bn.N, ref_vtx.catalysis_rgm.PΠ)
    L_ss = bn.L[r_v+1:end, :]
    direction = sign(det(Matrix{Float64}(vcat(L_ss, N_ss))))

    perms = [get_perm(rgms[i, j]::BncRegime) for j in valid_js]
    nlt_valid, cache = _calc_nullity(perms, N_ss)
    perm_keys, affine_by_perm = _build_row_affine_cache(rgms, i, valid_js, perms, nlt_valid, N_ss, r_v, direction, cache)

    return (
        bn = bn,
        r_v = r_v,
        N_ss = N_ss,
        L_ss = L_ss,
        direction = direction,
        valid_js = valid_js,
        perms = perms,
        perm_keys = perm_keys,
        nlt_valid = nlt_valid,
        cache = cache,
        affine_by_perm = affine_by_perm,
    )
end


"""
    _steady_state_offsets(vtx, r_v, N_ss)

Extract P0_ss and M0_ss for the steady-state reduced binding network:
    M_ss  = [P_ss; N_ss]
    M0_ss = [P0_ss; 0]
where P_ss is obtained from the binding regime by dropping the first r_v rows.
"""
function _steady_state_offsets(vtx::BncRegime, r_v::Int, N_ss)
    P0_ss = vtx.bind_rgm.P0[r_v+1:end]
    M0_ss = vcat(P0_ss, zeros(eltype(P0_ss), size(N_ss, 1)))
    return P0_ss, M0_ss
end


"""
    _expand_Hss_to_qssKk(H_ss, H0_ss, Pθ, P0θ)

Convert
    log x = H_ss * log(q_ss, K_ss) + H0_ss
with
    log K_ss = [log K; -(Pθ * log k + P0θ)]
into
    log x = H_ssk * log(q_ss, K, k) + H0_ssk.
"""
function _expand_Hss_to_qssKk(H_ss, H0_ss, Pθ, P0θ)
    r_v = size(Pθ, 1)
    split = size(H_ss, 2) - r_v
    H_left = H_ss[:, 1:split]
    H_right = H_ss[:, split+1:end]
    H_ssk = hcat(H_left, -(H_right * Pθ))
    H0_ssk = H0_ss - H_right * P0θ
    return H_ssk, vec(H0_ssk)
end


# ------------------------------------------------
# Catalytic consistency conditions in (q, K, k)
# ------------------------------------------------

"""
    _calc_C_qKk_cat_regular(bind_rgm, cat_rgm)

Regular binding regime:
- binding regime condition is already available in (q, K) coordinates:
      C_qK * log(q, K) + C0_qK >= 0
- catalytic dominance condition is
      CΠ * log x + Cθ * log k >= 0
  and we substitute
      log x = H * log(q, K) + H0.

Output variables are ordered as (q, K, k).
"""
function _calc_C_qKk_cat_regular(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    H, H0 = get_H_H0(bind_rgm)
    H = _float_sparse(H)
    C_qK, C0_qK = get_C_C0_qK(bind_rgm)
    C_qK = _float_sparse(C_qK)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)

    n_v = size(Cθ, 2)

    C1 = hcat(C_qK, spzeros(size(C_qK, 1), n_v))
    C2 = hcat(CΠ * H, Cθ)

    C = vcat(C1, C2)
    C0 = vcat(_float_vec(C0_qK), CΠ * H0 + C0θ)

    return C, C0, 0
end


"""
    _calc_C_qKk_cat_singular(bind_rgm, cat_rgm)

Singular binding regime:
we work in the extended variables
    z = (log(q, K), log k, log x)
and encode
    -I * log(q, K) + M * log x + M0 = 0
    C_x * log x + C0_x >= 0
    CΠ * log x + Cθ * log k >= 0
then eliminate log x.

Output variables are ordered as (q, K, k).
"""
function _calc_C_qKk_cat_singular(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    C_x, C0_x = get_C_C0_x(bind_rgm)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)
    M, M0 = get_M_M0(bind_rgm)

    n_qK = size(M, 1)
    n_x = size(M, 2)
    n_v = size(Cθ, 2)
    d_bind = size(C_x, 1)
    d_cat = size(CΠ, 1)

    Eq = hcat(-_spI(Int, n_qK), spzeros(n_qK, n_v), M)
    In_bind = hcat(spzeros(d_bind, n_qK + n_v), C_x)
    In_cat = hcat(spzeros(d_cat, n_qK), Cθ, CΠ)

    C = vcat(Eq, In_bind, In_cat)
    C0 = vcat(M0, C0_x, C0θ)

    p = get_polyhedron(C, C0, n_qK)
    delset = BitSet((n_qK + n_v + 1):(n_qK + n_v + n_x))
    p2 = eliminate(p, delset)

    return get_C_C0_nullity(p2)
end


"""
    _calc_C_qKk_cat(bind_rgm, cat_rgm)

Dispatch to the regular or singular implementation.
"""
function _calc_C_qKk_cat(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    if is_singular(bind_rgm)
        return _calc_C_qKk_cat_singular(bind_rgm, cat_rgm)
    else
        return _calc_C_qKk_cat_regular(bind_rgm, cat_rgm)
    end
end


# ------------------------------------------------
# Steady-state consistency conditions in (q_ss, K, k)
# ------------------------------------------------

"""
    _calc_C_qKk_ss_regular(bind_rgm, cat_rgm, H_ssk, H0_ss)

Regular steady-state reduced regime:
    log x = H_ssk * log(q_ss, K, k) + H0_ss

We combine
1) binding dominance condition in x-space
2) catalytic dominance condition in x,k-space
and push them into the variables (q_ss, K, k).
"""
function _calc_C_qKk_ss_regular(
    bind_rgm::BindRegime,
    cat_rgm::CatalysisRegime,
    H_ssk,
    H0_ssk,
)
    C_x_bind, C0_x_bind = get_C_C0_x(bind_rgm)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)

    n_v = size(Cθ, 2)

    C_bind = C_x_bind * H_ssk
    C0_bind = C0_x_bind + C_x_bind * H0_ssk

    C_cat = copy(CΠ * H_ssk)
    @views C_cat[:, end-n_v+1:end] .+= Cθ
    C0_cat = CΠ * H0_ssk + C0θ

    return vcat(C_bind, C_cat), vcat(C0_bind, C0_cat)
end

"""
    _calc_C_qKk_ss_singular(bind_rgm, cat_rgm)

Singular steady-state reduced regime:
we work in the extended variables
    z = (log q_ss, log K, log k, log x)
and encode
    -I * log q_ss + P_ss * log x + P0_ss = 0
    -I * log K    + N    * log x        = 0
     Pθ * log k   + PΠ   * log x + P0θ  = 0
    C_x * log x + C0_x >= 0
    CΠ * log x + Cθ * log k + C0θ >= 0
then eliminate log x.

Output variables are ordered as (q_ss, K, k).
"""
function _calc_C_qKk_ss_singular(bind_rgm::BindRegime, cat_rgm::CatalysisRegime)
    bn = bind_rgm.network
    r_v = size(cat_rgm.P, 1)

    P_ss = bind_rgm.P[r_v+1:end, :]
    P0_ss = bind_rgm.P0[r_v+1:end]
    N = bn.N
    Pθ = cat_rgm.P
    P0θ = get_P0(cat_rgm)
    PΠ = cat_rgm.PΠ

    C_x_bind, C0_x_bind = get_C_C0_x(bind_rgm)
    CΠ = get_CΠ(cat_rgm)
    Cθ = get_C_k(cat_rgm)
    C0θ = get_C0(cat_rgm)

    d_ss = size(P_ss, 1)
    r = size(N, 1)
    n_v = size(Pθ, 2)
    n_x = size(P_ss, 2)
    r_cat = size(Pθ, 1)

    Eq_qss = hcat(-_spI(Int, d_ss), spzeros(d_ss, r + n_v), P_ss)
    Eq_K = hcat(spzeros(r, d_ss), -_spI(Int, r), spzeros(r, n_v), N)
    Eq_cat = hcat(spzeros(r_cat, d_ss + r), Pθ, PΠ)

    In_bind = hcat(spzeros(size(C_x_bind, 1), d_ss + r + n_v), C_x_bind)
    In_cat = hcat(spzeros(size(CΠ, 1), d_ss + r), Cθ, CΠ)

    C = vcat(Eq_qss, Eq_K, Eq_cat, In_bind, In_cat)
    C0 = vcat(
        P0_ss,
        zeros(eltype(P0_ss), r),
        P0θ,
        C0_x_bind,
        C0θ,
    )

    n_eq = d_ss + r + r_cat
    p = get_polyhedron(C, C0, n_eq)
    delset = BitSet((d_ss + r + n_v + 1):(d_ss + r + n_v + n_x))
    p2 = eliminate(p, delset)

    return get_C_C0_nullity(p2)
end


# ------------------------------------------------
# Per-regime initialization: regular / singular
# ------------------------------------------------

"""
    _init_regular_bnc_regime!(vtx, perm, rowctx)

Initialize one mixed regime whose steady-state reduced matrix M_ss is invertible.
This computes:
- H  : map (q_ss, K, k) -> x
- H0 : affine offset
- C_qKk_cat / C0_qKk_cat : catalysis consistency in (q, K, k)
- C_qKk_ss  / C0_qKk_ss  : steady-state consistency in (q_ss, K, k)
"""
function _init_regular_bnc_regime!(vtx::BncRegime, perm, rowctx)
    C_qKk_cat, C0_qKk_cat, nlt_qKk_cat = _calc_C_qKk_cat(vtx.bind_rgm, vtx.catalysis_rgm)

    H_ss, H0_ss = rowctx.affine_by_perm[Tuple(Int.(perm))]
    Pθ = vtx.catalysis_rgm.P
    P0θ = get_P0(vtx.catalysis_rgm)
    H_ssk, H0_ssk = _expand_Hss_to_qssKk(H_ss, H0_ss, Pθ, P0θ)
    C_qKk_ss, C0_qKk_ss = _calc_C_qKk_ss_regular(vtx.bind_rgm, vtx.catalysis_rgm, H_ssk, H0_ssk)

    vtx.H = H_ssk
    vtx.H0 = H0_ssk
    vtx.C_qKk_cat = C_qKk_cat
    vtx.C0_qKk_cat = C0_qKk_cat
    vtx.nlt_qKk_cat = nlt_qKk_cat
    vtx.C_qKk_ss = C_qKk_ss
    vtx.C0_qKk_ss = C0_qKk_ss

    return nothing
end


"""
    _calc_singular_H_ss(bind_rgm, cat_rgm, perm, rowctx)

Build the nullity-1 ray/adjugate-like matrix for the reduced steady-state system.
This does NOT return an affine offset H0.
"""
function _calc_singular_H_ss(bind_rgm::BindRegime, cat_rgm::CatalysisRegime, perm, rowctx)
    r_v = rowctx.r_v
    M_ss = vcat(bind_rgm.P[r_v+1:end, :], rowctx.N_ss)
    P0_ss = bind_rgm.P0[r_v+1:end]
    M0_ss = vcat(P0_ss, zeros(eltype(P0_ss), size(rowctx.N_ss, 1)))

    H_ray = if allunique(perm)
        _calc_H(rowctx.N_ss, rowctx.cache, perm; scale = rowctx.direction)
    else
        H_tmp = _adj_singular_matrix(M_ss)[1]
        droptol!(sparse(H_tmp), 1e-10) .* rowctx.direction
    end

    H0_ray = vec(-(H_ray * M0_ss))

    return M_ss, H_ray, H0_ray
end


"""
    _init_singular_bnc_regime!(vtx, perm, rowctx)

Initialize one mixed regime whose steady-state reduced matrix M_ss has nullity 1.
This computes:
- H  : a ray/adjugate-like matrix, not an affine inverse
- H0 : `-H * M0_ss`, useful for interface reconstruction
- C_qKk_cat / C0_qKk_cat : catalysis consistency in (q, K, k)
- C_qKk_ss  / C0_qKk_ss  : steady-state consistency in (q_ss, K, k),
                           obtained by explicit elimination of x
"""
function _init_singular_bnc_regime!(vtx::BncRegime, perm, rowctx)
    C_qKk_cat, C0_qKk_cat, nlt_qKk_cat = _calc_C_qKk_cat(vtx.bind_rgm, vtx.catalysis_rgm)

    H_ray, H0_ss = rowctx.affine_by_perm[Tuple(Int.(perm))]
    Pθ = get_P(vtx.catalysis_rgm)
    P0θ = get_P0(vtx.catalysis_rgm)
    H_ssk, H0_ssk = _expand_Hss_to_qssKk(H_ray, H0_ss, Pθ, P0θ)

    C_qKk_ss, C0_qKk_ss, _ = _calc_C_qKk_ss_singular(vtx.bind_rgm, vtx.catalysis_rgm)

    vtx.H = H_ssk
    vtx.H0 = H0_ssk
    vtx.C_qKk_cat = C_qKk_cat
    vtx.C0_qKk_cat = C0_qKk_cat
    vtx.nlt_qKk_cat = nlt_qKk_cat
    vtx.C_qKk_ss = C_qKk_ss
    vtx.C0_qKk_ss = C0_qKk_ss

    return nothing
end


# ------------------------------------------------
# Initialize all mixed regimes
# ------------------------------------------------

"""
    _initialize_regime!(rgms)

Initialize all candidate mixed regimes.
High-level flow:
1) build row-level shared data for each catalysis regime row;
2) for each valid mixed regime in that row:
   - compute its reduced steady-state nullity;
   - dispatch to the regular or singular initializer.
"""
function _initialize_regime!(rgms::AbstractMatrix{<:Union{BncRegime,Nothing}})
    first_vtx = _first_nonempty_regime(rgms)
    first_vtx === nothing && return nothing

    r_v = size(first_vtx.catalysis_rgm.P, 1)

    @info "Initializing BncRegimes..."

    for i in axes(rgms, 1)
        rowctx = _build_row_context(rgms, i, r_v)
        rowctx === nothing && continue

        valid_js = rowctx.valid_js
        perms = rowctx.perms
        nlt_valid = rowctx.nlt_valid

        Threads.@threads for k in eachindex(valid_js)
            j = valid_js[k]
            vtx = rgms[i, j]::BncRegime
            perm = perms[k]
            nlt = nlt_valid[k]

            vtx.nlt = nlt
            nlt > 1 && continue

            if nlt == 0
                _init_regular_bnc_regime!(vtx, perm, rowctx)
            else
                _init_singular_bnc_regime!(vtx, perm, rowctx)
            end
        end
    end

    @info "Finished initializing BncRegimes."
    return nothing
end


function summary_regime(rgm::BncRegime)
    rgm = get_regime(rgm)
    println("bind_idx=$(get_idx(rgm.bind_rgm)), cat_idx=$(get_idx(rgm.catalysis_rgm)), nlt=$(rgm.nlt), stable=$(is_stable(rgm))")
    println("Binding / catalysis conditions in (x, k):")
    display.(show_condition_xk(rgm; kind=:binding, log_space=false))
    display.(show_condition_xk(rgm; kind=:catalysis, log_space=false))
    println("Combined consistency in (q_ss, K, k):")
    display.(show_condition_qssKk(rgm; log_space=false))
    return nothing
end

summary(rgm::BncRegime) = summary_regime(rgm)
summary_regime(model::Bnc, bind, cat) = summary_regime(get_bnc_regime(model, bind, cat; check=true))
summary(model::Bnc, bind, cat) = summary_regime(model, bind, cat)

@inline function _is_asymptotic(rgm::BncRegime)
    return is_asymptotic(rgm.bind_rgm) && is_asymptotic(rgm.catalysis_rgm)
end

@inline function _regime_display_dominant_mode(rgm::BncRegime)
    return "bind=$(get_binding_perm(rgm)), cat=$(get_catalysis_perm(rgm)), ss=$(get_perm(rgm))"
end

function Base.show(io::IO, rgm::BncRegime)
    print(
        io,
        "BncRegime(",
        _regime_display_dominant_mode(rgm),
        ", nullity=",
        rgm.nlt,
        ", asymptotic=",
        _is_asymptotic(rgm),
        ")",
    )
end

function Base.show(io::IO, ::MIME"text/plain", rgm::BncRegime)
    println(io, "BncRegime")
    println(io, "  dominant mode: ", _regime_display_dominant_mode(rgm))
    println(io, "  nullity: ", rgm.nlt)
    print(io, "  asymptotic: ", _is_asymptotic(rgm))
end


# ------------------------------------------------
# Top-level entry
# ------------------------------------------------

function match_regimes!(model::Bnc)
    if is_bnc_regimes_built(model)
        return model.BncRegimes
    end

    find_all_regimes!(model)
    find_catalysis_regimes!(model)

    model.BncRegimes = _build_BncRegime(
        model.catalysis.CatalysisRegimes,
        model.BindRegimes,
    )
    _initialize_regime!(model.BncRegimes)

    return model.BncRegimes
end
