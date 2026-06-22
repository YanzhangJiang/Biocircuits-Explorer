# #--------------Matrix inverse helpers-------------------------

const _EMPTY_SPM64 = spzeros(Float64, 0, 0)
const _EMPTY_VEC64 = Float64[]
const NρKey = Tuple{Vararg{Int}}
const NρCache = Dict{NρKey,NρCacheEntry}

@inline _entry_only_def(def::Int) = NρCacheEntry(def, 0x00, _EMPTY_SPM64, 0.0, _EMPTY_VEC64, _EMPTY_VEC64)
@inline _entry_inv(inv::SparseMatrixCSC{Float64,Int}) = NρCacheEntry(0, 0x01, inv, 0.0, _EMPTY_VEC64, _EMPTY_VEC64)
@inline _entry_rank1(def::Int, α::Float64, u::Vector{Float64}, v::Vector{Float64}) =
    NρCacheEntry(def, 0x02, _EMPTY_SPM64, α, u, v)

# -----------------------------------------------------------------------------
# perm -> key / perm nullity
# -----------------------------------------------------------------------------

@inline function _key_from_perm!(
    keybuf::Vector{Int},
    seen::Vector{UInt8},
    touched::Vector{Int},
    perm::AbstractVector{<:Integer},
    n::Int,
)
    ntouched = 0
    nunique  = 0

    @inbounds for p0 in perm
        p = Int(p0)
        if seen[p] == 0x00
            seen[p] = 0x01
            ntouched += 1
            touched[ntouched] = p
            nunique += 1
        end
    end

    k = 0
    @inbounds for j in 1:n
        if seen[j] == 0x00
            k += 1
            keybuf[k] = j
        end
    end

    @inbounds for t in 1:ntouched
        seen[touched[t]] = 0x00
    end

    return k, (length(perm) - nunique)
end

@inline function _tuple_from_prefix(buf::Vector{Int}, k::Int)::NρKey
    return ntuple(i -> @inbounds(buf[i]), k)
end

function _get_Nρ_key(perm::AbstractVector{<:Integer}, n::Int)::Vector{Int}
    seen    = zeros(UInt8, n)
    touched = Vector{Int}(undef, length(perm))
    keybuf  = Vector{Int}(undef, n)
    k, _    = _key_from_perm!(keybuf, seen, touched, perm, n)
    return copy(@view keybuf[1:k])
end

function _get_Nρ_key_and_perm_nullity(perm::AbstractVector{<:Integer}, n::Int)
    seen    = zeros(UInt8, n)
    touched = Vector{Int}(undef, length(perm))
    keybuf  = Vector{Int}(undef, n)
    k, pdef = _key_from_perm!(keybuf, seen, touched, perm, n)
    return copy(@view keybuf[1:k]), pdef
end
# -----------------------------------------------------------------------------
# permutation sign for exact adj(A) when A is singular and A * Π = M
# -----------------------------------------------------------------------------

@inline function _perm_sign_perm_key(
    perm::AbstractVector{<:Integer},
    key::AbstractVector{<:Integer},
)::Float64
    d = length(perm)
    n = d + length(key)
    visited = falses(n)
    s = 1.0

    @inbounds for i in 1:n
        if !visited[i]
            j = i
            clen = 0
            while !visited[j]
                visited[j] = true
                j = j <= d ? Int(perm[j]) : Int(key[j - d])
                clen += 1
            end
            if isodd(clen - 1)
                s = -s
            end
        end
    end
    return s
end
# -----------------------------------------------------------------------------
# Nρ analysis / factorization cache
# -----------------------------------------------------------------------------

function _rank1_adjugate_data!(A::Matrix{Float64}; atol::Float64=1e-12, rtol::Float64=1e-10)
    F = svd!(A)
    S = F.S

    σmax = isempty(S) ? 0.0 : maximum(S)
    tol  = max(atol, rtol * σmax)
    rk   = count(σ -> σ > tol, S)
    def  = size(A, 1) - rk

    if size(A, 1) == size(A, 2) && def == 1
        k = findfirst(σ -> σ <= tol, S)
        @assert k !== nothing

        logσprod = 0.0
        @inbounds for i in eachindex(S)
            if i != k
                logσprod += log(S[i])
            end
        end

        # For square SVD, adj(A) = det(U) * det(V) * prod(nonzero singular values) * v * u'
        # Since det(Vt) == det(V), we can use det(Vt) directly.
        α = exp(logσprod) * (det(F.U) * det(F.Vt))
        u = copy(@view F.U[:, k])
        v = copy(@view F.Vt[k, :])  # kth row of Vt == v_k'
        return def, α, u, v
    else
        return def, 0.0, _EMPTY_VEC64, _EMPTY_VEC64
    end
end

function _factor_Nρ(
    Nρ::SparseMatrixCSC{Tv,Int};
    atol::Float64=1e-12,
    rtol::Float64=1e-10,
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    r, c = size(Nρ)

    # Square case: try sparse LU first. If it succeeds, cache the explicit inverse.
    if r == c
        F = lu(Nρ; check=false)
        if issuccess(F)
            # X = F \ Matrix{Float64}(I, r, r)
            # Xsp = sparse(X)
            Xsp = luFac(F) \ spdiagm(0=>ones(Float64,r))
            # return  H, 0
            drop_tol > 0 && droptol!(Xsp, drop_tol)
            return _entry_inv(Xsp)
        end

        # Singular square case: dense SVD only on the failure path.
        A = Matrix{Float64}(Nρ)
        def, α, u, v = _rank1_adjugate_data!(A; atol=atol, rtol=rtol)
        if def == 1
            return _entry_rank1(def, α, u, v)
        else
            return _entry_only_def(def)
        end
    end

    # Rectangular case: only the deficiency matters for nullity prefiltering.
    # We deliberately do NOT cache any inverse-like object here.
    A   = Matrix{Float64}(Nρ)
    rk  = rank(A; atol=atol, rtol=rtol)
    def = r - rk
    return _entry_only_def(def)
end

function _get_Nρ_entry!(
    cache::NρCache,
    N::AbstractMatrix{Tv},
    key::AbstractVector{<:Integer};
    atol::Float64=1e-12,
    rtol::Float64=1e-10,
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    tkey = Tuple(Int.(key))
    return get!(cache, tkey) do
        Nρ = sparse(N[:, collect(key)])
        _factor_Nρ(Nρ; atol=atol, rtol=rtol, drop_tol=drop_tol)
    end
end

@inline function _get_Nρ_entry_from_perm!(
    cache::NρCache,
    N::AbstractMatrix{Tv},
    perm;
    kwargs...,
) where {Tv<:Real}
    key = _get_Nρ_key(perm, size(N, 2))
    return _get_Nρ_entry!(cache, N, key; kwargs...), key
end

function _build_Nρ_cache_parallel!(
    cache::NρCache,
    N::AbstractMatrix{Tv},
    perms::Vector{<:AbstractVector{<:Integer}};
    atol::Float64=1e-12,
    rtol::Float64=1e-10,
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    nperm = length(perms)
    n = size(N, 2)
    nperm == 0 && return NρKey[], Int[]

    perm_keys = Vector{NρKey}(undef, nperm)
    perm_defs = Vector{Int}(undef, nperm)

    nt = Threads.maxthreadid()
    seen_locals = [zeros(UInt8, n) for _ in 1:nt]
    touched_locals = [Vector{Int}(undef, max(length(perms[1]), 1)) for _ in 1:nt]
    keybuf_locals = [Vector{Int}(undef, n) for _ in 1:nt]

    Threads.@threads :greedy for i in eachindex(perms)
        tid = Threads.threadid()
        seen = seen_locals[tid]
        touched = touched_locals[tid]
        keybuf = keybuf_locals[tid]

        k, pdef = _key_from_perm!(keybuf, seen, touched, perms[i], n)
        perm_keys[i] = _tuple_from_prefix(keybuf, k)
        perm_defs[i] = pdef
    end

    uniq_index = Dict{NρKey,Int}()
    keys = Vector{Vector{Int}}()

    sizehint!(uniq_index, nperm)
    sizehint!(keys, nperm)

    for tkey in perm_keys
        if !haskey(uniq_index, tkey)
            uniq_index[tkey] = length(keys) + 1
            push!(keys, collect(tkey))
        end
    end

    entries = Vector{NρCacheEntry}(undef, length(keys))
    Threads.@threads :greedy for i in eachindex(keys)
        Nρ = sparse(N[:, keys[i]])
        entries[i] = _factor_Nρ(Nρ; atol=atol, rtol=rtol, drop_tol=drop_tol)
    end

    empty!(cache)
    sizehint!(cache, length(keys))
    for i in eachindex(keys)
        cache[Tuple(keys[i])] = entries[i]
    end

    return perm_keys, perm_defs
end

@inline _perm_total_nullity(perm_def::Int, entry::NρCacheEntry) = perm_def + entry.deficiency
@inline _is_regular_seed(perm_def::Int, entry::NρCacheEntry) = perm_def == 0 && entry.deficiency == 0

function _build_regular_H_from_key_entry(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{Tv},
    key::AbstractVector{<:Integer},
    entry::NρCacheEntry;
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    perm_int = perm isa Vector{Int} ? perm : Int.(perm)
    n = size(N, 2)

    BR = entry.inv
    Nc = sparse(N[:, perm_int])
    BL = -(BR * Nc)
    drop_tol > 0 && SparseArrays.droptol!(BL, drop_tol)

    return _assemble_H_from_blocks(perm_int, Int.(collect(key)), BL, BR, n)
end

function _build_regular_H_from_key_entry_exact(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{<:Integer},
    key::AbstractVector{<:Integer},
    _entry=nothing;
    drop_tol::Float64=1e-12,
)
    perm_int = perm isa Vector{Int} ? perm : Int.(perm)
    n = size(N, 2)

    BR = _exact_inverse_matrix(N[:, Int.(collect(key))])
    Nc = sparse(ExactAffineCoeff.(Matrix{Int}(N[:, perm_int])))
    BL = -(BR * Nc)
    dropzeros!(BL)

    return _assemble_H_from_blocks(perm_int, Int.(collect(key)), BL, BR, n)
end

function _build_singular_H_from_perm(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{Tv},
    scale::Real=1;
    atol::Float64=1e-12,
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    d = length(perm)
    n = size(N, 2)
    P = sparse(1:d, Int.(perm), ones(Int, d), d, n)
    M = [Matrix(P); Matrix(N)]

    H, nlt = direct_inverse_or_adjugate(M; atol=atol)
    if nlt == 1 && scale != 1
        H = sparse(scale .* H)
        drop_tol > 0 && SparseArrays.droptol!(H, drop_tol)
    end

    return H, nlt
end

function _build_singular_H_from_perm_exact(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{<:Integer},
    scale::Integer=1;
    atol::Float64=1e-12,
    drop_tol::Float64=1e-12,
)
    d = length(perm)
    n = size(N, 2)
    P = sparse(1:d, Int.(perm), ones(Int, d), d, n)
    M = [Matrix(P); Matrix(N)]
    return _exact_direct_inverse_or_adjugate(M, scale)
end

# -----------------------------------------------------------------------------
# Batch nullity interface
# -----------------------------------------------------------------------------

function _calc_nullity(
    perms::Vector{<:AbstractVector{<:Integer}},
    N::AbstractMatrix{Tv},
    atol::Float64=1e-12,
    rtol::Float64=1e-10,
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    cache = NρCache()
    perm_keys, perm_defs = _build_Nρ_cache_parallel!(
        cache,
        N,
        perms;
        atol=atol,
        rtol=rtol,
        drop_tol=drop_tol,
    )

    nullity = Vector{Int}(undef, length(perms))
    Threads.@threads for i in eachindex(perms)
        nullity[i] = perm_defs[i] + cache[perm_keys[i]].deficiency
    end

    return nullity,cache
end


function _calc_nullity(
    perms::Vector{<:AbstractVector{<:Integer}},
    model::Bnc;
    kwargs...,
)
    nullity,cache = _calc_nullity(
        perms,
        model.N;
        kwargs...,
    )
    model._vertices_Nρ_inv_dict = cache
    return nullity
end

# -----------------------------------------------------------------------------
# Sparse H assembly for the nonsingular case
# -----------------------------------------------------------------------------

function _append_block_triplets!(
    I::Vector{Int},
    J::Vector{Int},
    V::Vector{Tc},
    p::Int,
    rowmap::Vector{Int},
    coloffset::Int,
    A::SparseMatrixCSC{Tc,Int},
) where {Tc<:Real}
    @inbounds for col in 1:size(A, 2)
        for ptr in A.colptr[col]:(A.colptr[col + 1] - 1)
            p += 1
            I[p] = rowmap[A.rowval[ptr]]
            J[p] = coloffset + col - 1
            V[p] = A.nzval[ptr]
        end
    end
    return p
end

function _assemble_H_from_blocks(
    perm::AbstractVector{<:Integer},
    key::Vector{Int},
    BL::SparseMatrixCSC{Tc,Int},
    BR::SparseMatrixCSC{Tc,Int},
    n::Int,
) where {Tc<:Real}
    d = length(perm)
    nnzH = d + nnz(BL) + nnz(BR)

    I = Vector{Int}(undef, nnzH)
    J = Vector{Int}(undef, nnzH)
    V = Vector{Tc}(undef, nnzH)

    p = 0
    @inbounds for i in 1:d
        p += 1
        I[p] = Int(perm[i])
        J[p] = i
        V[p] = one(Tc)
    end

    p = _append_block_triplets!(I, J, V, p, key, 1,     BL)
    p = _append_block_triplets!(I, J, V, p, key, d + 1, BR)

    @assert p == nnzH
    return sparse(I, J, V, n, n)
end

# -----------------------------------------------------------------------------
# Singular case: apply adj([P;N]) to a known direction without materializing H
# -----------------------------------------------------------------------------
function _materialize_rank1_adjugate(
    perm::AbstractVector{<:Integer},
    key::AbstractVector{<:Integer},
    Nc::AbstractMatrix{Tv},
    α::Float64,
    u::AbstractVector{<:Real},
    v::AbstractVector{<:Real};
    scale::Real = 1.0,
    drop_tol::Float64 = 0.0,
) where {Tv<:Real}

    d = length(perm)
    r = length(key)
    n = d + r

    # right = [ -Nc' * u ; u ]
    tmp = Nc' * u
    right = Vector{Float64}(undef, n)

    @inbounds begin
        for i in 1:d
            right[i] = -Float64(tmp[i])
        end
        for j in 1:r
            right[d + j] = Float64(u[j])
        end
    end

    γ = Float64(scale) * _perm_sign_perm_key(perm, key) * α

    # 先筛掉明显无效的行/列
    active_rows = Vector{Int}(undef, r)
    row_coeffs  = Vector{Float64}(undef, r)
    nr = 0
    @inbounds for j in 1:r
        c = γ * Float64(v[j])
        if drop_tol <= 0
            if c != 0.0
                nr += 1
                active_rows[nr] = Int(key[j])
                row_coeffs[nr]  = c
            end
        else
            if abs(c) > drop_tol
                nr += 1
                active_rows[nr] = Int(key[j])
                row_coeffs[nr]  = c
            end
        end
    end

    active_cols = Vector{Int}(undef, n)
    nc = 0
    @inbounds for k in 1:n
        x = right[k]
        if drop_tol <= 0
            if x != 0.0
                nc += 1
                active_cols[nc] = k
            end
        else
            if abs(x) > drop_tol
                nc += 1
                active_cols[nc] = k
            end
        end
    end

    if nr == 0 || nc == 0
        return spzeros(Float64, n, n)
    end

    # 精确按最终乘积阈值计数，避免后面再 droptol!
    nnzA = 0
    if drop_tol <= 0
        nnzA = nr * nc
    else
        @inbounds for a in 1:nr
            ca = row_coeffs[a]
            for b in 1:nc
                if abs(ca * right[active_cols[b]]) > drop_tol
                    nnzA += 1
                end
            end
        end
    end

    if nnzA == 0
        return spzeros(Float64, n, n)
    end

    I = Vector{Int}(undef, nnzA)
    J = Vector{Int}(undef, nnzA)
    V = Vector{Float64}(undef, nnzA)

    p = 0
    @inbounds for a in 1:nr
        row = active_rows[a]
        ca  = row_coeffs[a]
        for b in 1:nc
            col = active_cols[b]
            val = ca * right[col]
            if drop_tol <= 0 || abs(val) > drop_tol
                p += 1
                I[p] = row
                J[p] = col
                V[p] = val
            end
        end
    end

    return sparse(I, J, V, n, n)
end
# -----------------------------------------------------------------------------
# Generic numerical cleanup helpers
# -----------------------------------------------------------------------------

function droptol!(A::AbstractArray, tol)
    @inbounds for i in eachindex(A)
        if abs(A[i]) < tol
            A[i] = zero(eltype(A))
        end
    end
    return A
end

function droptol!(A::Real, tol)
    return A = abs(A) < tol ? zero(eltype(A)) : A
end


# -----------------------------------------------------------------------------
# Main H interface
# -----------------------------------------------------------------------------

function _calc_H(
    N::AbstractMatrix{Tv},
    cache::NρCache,
    perm::AbstractVector{<:Integer};

    scale::Real=1.0,
    atol::Float64=1e-12,
    rtol::Float64=1e-10,
    drop_tol::Float64=1e-12,
    kwargs...
) where {Tv<:Real}
    n = size(N, 2)
    key, perm_def = _get_Nρ_key_and_perm_nullity(perm, n)
    perm_def == 0 || error("_calc_H only supports perms with unique entries; use _calc_nullity first to prefilter invalid perms.")

    entry = _get_Nρ_entry!(cache, N, key; atol=atol, rtol=rtol, drop_tol=drop_tol)

    if entry.deficiency == 0
        entry.kind == 0x01 || error("Internal cache inconsistency: expected explicit inverse for deficiency == 0.")

        BR = entry.inv
        Nc = sparse(N[:, Int.(perm)])
        BL = -(BR * Nc)
        drop_tol > 0 && droptol!(BL, drop_tol)
        return _assemble_H_from_blocks(perm, key, BL, BR, n)
    end

    if entry.deficiency == 1
        entry.kind == 0x02 || error("Internal cache inconsistency: expected rank-1 adjugate factors for deficiency == 1.")

        Nc = sparse(N[:, Int.(perm)])

        return _materialize_rank1_adjugate(
                perm,
                key,
                Nc,
                entry.α,
                entry.u,
                entry.v;
                scale=scale,
                kwargs...,
            )
    end

    error("nullity([P;N]) >= 2 is not supported by _calc_H; call _calc_nullity first and skip those perms.")
end



function _calc_H(
    model::Bnc,
    perm::AbstractVector{<:Integer};
    kwargs...
)
    if isnothing(model._vertices_Nρ_inv_dict)
        error("Nρ cache is not initialized. Call _calc_nullity first to populate the cache before calling _calc_H.")
    end

    return _calc_H(
        model.N,
        model._vertices_Nρ_inv_dict,
        perm;
        scale= model.direction,
        kwargs...
    )
end


function calc_H_and_nullity(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{Tv},
    scale::Real=1;
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    H, nullity, _, _, _ = _calc_H_and_nullity_uncached(
        perm,
        N,
        scale;
        drop_tol=drop_tol,
    )
    return H, nullity
end

function _calc_H_and_nullity_uncached(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{Tv},
    scale::Real=1;
    drop_tol::Float64=1e-12,
) where {Tv<:Real}
    atol     = 1e-12
    rtol     = 1e-10

    n = size(N, 2)
    perm_int = perm isa Vector{Int} ? perm : Int.(perm)
    seen    = zeros(UInt8, n)
    touched = Vector{Int}(undef, length(perm_int))
    keybuf  = Vector{Int}(undef, n)

    k, perm_def = _key_from_perm!(keybuf, seen, touched, perm_int, n)
    key = copy(@view keybuf[1:k])
    Nρ = sparse(N[:, key])
    entry = _factor_Nρ(Nρ; atol=atol, rtol=rtol, drop_tol=drop_tol)
    nullity = _perm_total_nullity(perm_def, entry)

    if perm_def != 0
        return nothing, nullity, key, perm_def, entry
    end

    if entry.deficiency == 0
        H = _build_regular_H_from_key_entry(perm_int, N, key, entry; drop_tol=drop_tol)
        return H, nullity, key, perm_def, entry
    end

    if entry.deficiency == 1
        Nc = sparse(N[:, perm_int])

        H = _materialize_rank1_adjugate(
            perm_int,
            key,
            Nc,
            entry.α,
                entry.u,
                entry.v;
                scale=scale,
        )

        return H, nullity, key, perm_def, entry
    end

    return nothing, nullity, key, perm_def, entry
end












# helper funtions to taking inverse when the matrix is singular.
"""
    _adj_singular_matrix(A::AbstractMatrix; atol=1e-12) -> (SparseMatrixCSC, Int)

Compute a sparse adjugate-like matrix for a near-singular square matrix using
its smallest singular vector, and return the inferred nullity.

# Arguments
- A: Square matrix to analyze.

# Keyword Arguments
- atol: Absolute tolerance for identifying zero singular values.

# Returns
- Tuple (adj_A, nullity).
"""
function _adj_singular_matrix(A::AbstractMatrix; atol=1e-12)::Tuple{SparseMatrixCSC,Int}
    n, m = size(A)
    @assert n == m "A must be square"
    F = svd(Array(A))
    S = F.S
    thresh = atol * maximum(S)
    zero_ids = findall(σ -> σ ≤ thresh, S)
    nullity = length(zero_ids)
    if nullity == 1
        k = zero_ids[1]
        logσprod = sum(log, S[setdiff(1:n, [k])])
        σprod = exp(logσprod)
        sign_correction = det(F.U) * det(F.V) # to ensure the sign is right!!!!!!
        u = F.U[:, k]   # 左奇异向量
        v = F.V[:, k]   # 右奇异向量
        adj_A = (sign_correction * σprod) * (sparse(v) * sparse(u)')
        return adj_A, 1  # rank-1 矩阵
    else
        return spzeros(0, 0), nullity
    end
end

#=======================================================================================================#
# Bareiss algorithm for exact determinant of integer matrices, used in the exact adjugate computation.
#=======================================================================================================#

function _bareiss_det_big(A::AbstractMatrix{<:Integer})::BigInt
    n, m = size(A)
    @assert n == m "A must be square."
    n == 0 && return BigInt(1)
    n == 1 && return BigInt(A[1, 1])

    B = Matrix{BigInt}(A)
    sign = BigInt(1)
    prev = BigInt(1)

    @inbounds for k in 1:(n - 1)
        if B[k, k] == 0
            swap = findfirst(i -> B[i, k] != 0, (k + 1):n)
            swap === nothing && return BigInt(0)
            B[k, :], B[swap, :] = B[swap, :], B[k, :]
            sign = -sign
        end

        pivot = B[k, k]
        pivot == 0 && return BigInt(0)

        for i in (k + 1):n
            for j in (k + 1):n
                B[i, j] = (B[i, j] * pivot - B[i, k] * B[k, j]) ÷ prev
            end
        end
        prev = pivot
    end

    return sign * B[n, n]
end

function _minor_matrix(A::AbstractMatrix{<:Integer}, row::Int, col::Int)
    n = size(A, 1)
    B = Matrix{Int}(undef, n - 1, n - 1)
    bi = 0
    @inbounds for i in 1:n
        i == row && continue
        bi += 1
        bj = 0
        for j in 1:n
            j == col && continue
            bj += 1
            B[bi, bj] = Int(A[i, j])
        end
    end
    return B
end

function _exact_inverse_matrix(A::AbstractMatrix{<:Integer})
    AQ = ExactAffineCoeff.(Matrix{Int}(A))
    H = sparse(inv(AQ))
    dropzeros!(H)
    return H
end

function _exact_calc_H_regular(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{<:Integer},
)
    n = size(N, 2)
    key, perm_def = _get_Nρ_key_and_perm_nullity(perm, n)
    perm_def == 0 || return nothing

    BR = _exact_inverse_matrix(N[:, key])
    Nc = sparse(ExactAffineCoeff.(Matrix{Int}(N[:, Int.(perm)])))
    BL = -(BR * Nc)
    dropzeros!(BL)

    return _assemble_H_from_blocks(perm, key, BL, BR, n)
end

function _exact_adjugate_matrix(A::AbstractMatrix{<:Integer})
    n, m = size(A)
    @assert n == m "A must be square."

    if n == 1
        return sparse(reshape(ExactAffineCoeff[one(ExactAffineCoeff)], 1, 1))
    end

    Adj = Matrix{ExactAffineCoeff}(undef, n, n)
    @inbounds for i in 1:n
        for j in 1:n
            cof = _bareiss_det_big(_minor_matrix(A, j, i))
            if isodd(i + j)
                cof = -cof
            end
            Adj[i, j] = ExactAffineCoeff(Int(cof), 1)
        end
    end

    H = sparse(Adj)
    dropzeros!(H)
    return H
end

function _exact_direct_inverse_or_adjugate(A::AbstractMatrix{<:Integer}, scale::Integer=1)
    detA = _bareiss_det_big(A)
    if detA != 0
        return _exact_inverse_matrix(A), 0
    end

    H = _exact_adjugate_matrix(A)
    if nnz(H) == 0
        return spzeros(ExactAffineCoeff, size(A, 1), size(A, 2)), 2
    end

    if scale != 1
        H = sparse(scale .* H)
        dropzeros!(H)
    end
    return H, 1
end

function _exact_direct_inverse_or_adjugate(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{<:Integer},
    scale::Integer=1;
    allow_singular::Bool=true,
)
    H = _exact_calc_H_regular(perm, N)
    if !isnothing(H)
        return H, 0
    end

    if !allow_singular
        n = size(N, 2)
        return spzeros(ExactAffineCoeff, n, n), 1
    end

    d = length(perm)
    P = sparse(1:d, Int.(perm), ones(Int, d), d, size(N, 2))
    M = [Matrix(P); Matrix(N)]
    return _exact_direct_inverse_or_adjugate(M, scale)
end


function direct_inverse_or_adjugate(A::AbstractMatrix; atol::Float64=1e-12)::Tuple{SparseMatrixCSC,Int}
    n, m = size(A)
    @assert n == m "A must be square"
    F = lu(sparse(A); check=false)
    if issuccess(F)
        H = luFac(F) \ spdiagm(0=>ones(Float64,n))
        return  H, 0
    else
        adj_A, nullity = _adj_singular_matrix(A; atol=atol)
        return adj_A, nullity
    end
end

function direct_inverse_or_adjugate(
    perm::AbstractVector{<:Integer},
    N::AbstractMatrix{Tv},
    scale::Real=1;
    atol::Float64=1e-12,
    drop_tol::Float64=1e-12,
)::Tuple{SparseMatrixCSC,Int} where {Tv<:Real}

    H_regular, nlt = calc_H_and_nullity(perm, N, scale; drop_tol=drop_tol)
    if !isnothing(H_regular)
        return H_regular, nlt
    end

    nlt == 1 && return _build_singular_H_from_perm(perm, N, scale; atol=atol, drop_tol=drop_tol)
    return spzeros(Float64, size(N, 2), size(N, 2)), nlt
end
