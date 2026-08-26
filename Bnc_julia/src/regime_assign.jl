export assign_regime, assign_regime_qK, assign_regime_x

#-----------------------------------------------------------------
# Functions for assigning vertices
#-----------------------------------------------------------------

"""
    assign_regime_x(bnc::Bnc, x; input_logspace=false, asymptotic_only=true, return_idx=false)

Assign a regime given a point in x space.
"""
function assign_regime_x(Bnc::Bnc{T}, x::AbstractVector{<:Real};
    input_logspace::Bool=false,
    asymptotic_only::Bool=true,
    return_idx::Bool=false) where T
    # x = input_logspace ? exp10.(x) : x
    helper = _integration_helper!(Bnc)
    L = Bnc.L
    d = Bnc.d
    n = Bnc.n
    max_indices = zeros(T, d)
    max_val = fill(-Inf, d)
    colptr = L.colptr
    rowval = L.rowval

    if asymptotic_only
        nzval = @view(x[helper._LN_top_cols])
    else
        x = input_logspace ? exp10.(x) : x # linear or log space only matters when not asymptotic
        nzval = @view(x[helper._LN_top_cols]) .* L.nzval
    end

    @inbounds for col in 1:n
        col_start_idx = colptr[col]
        col_end_idx   = colptr[col+1] - 1
        if col_start_idx <= col_end_idx #escape empty column
            @inbounds for idx in col_start_idx:col_end_idx
                v = nzval[idx]
                row = rowval[idx]
                if v > max_val[row]
                    max_val[row] = v
                    max_indices[row] = col
                end
            end
        end
    end
    return return_idx ? get_idx(Bnc,max_indices) : max_indices
end
# function get_vertex_qK(Bnc::Bnc, x::AbstractMatrix{<:Real}; kwargs...) 
#     [get_vertex_qK_slow(Bnc, row; kwargs...) for row in eachrow(x)]
# end

"""
    assign_regime_qK(bnc::Bnc; x, input_logspace=false, kwargs...) -> Vector

Assign a regime given a point in x space by first mapping to qK.
"""
function assign_regime_qK(Bnc::Bnc ; x::AbstractVector{<:Real}, input_logspace::Bool=false, kwargs...) 
    # @show all_vertice_idx
    logqK = x2qK(Bnc,x; input_logspace=input_logspace, output_logspace=true)
    return assign_regime_qK(Bnc, logqK; input_logspace=true, kwargs...)
end
"""
    assign_regime_qK(bnc::Bnc, qK; input_logspace=false, asymptotic_only=false,
        eps=0, return_idx=false, strict=false, membership=nothing)

Assign a regime given qK coordinates. The legacy `membership=:relaxed` policy
admits the caller's `eps` tolerance and retains the documented best-fit
fallback. `membership=:closed_cell` uses the Float64 closed-cell inequalities,
requires `eps == 0`, and returns `0` for an index request (or `nothing` for a
permutation request) when no cell contains the point. `strict=true` is a
compatibility alias for `membership=:closed_cell` and likewise rejects a
nonzero `eps`; it is not an exact-real arithmetic claim.
"""
function assign_regime_qK(
    Bnc::Bnc,
    qK::AbstractVector{<:Real};
    input_logspace::Bool=false,
    asymptotic_only::Bool=false,
    eps=0,
    return_idx::Bool=false,
    strict::Bool=false,
    membership::Union{Nothing,Symbol}=nothing,
)
    membership_policy = if membership === nothing
        strict ? :closed_cell : :relaxed
    else
        membership
    end
    membership_policy in (:relaxed, :closed_cell) || throw(ArgumentError(
        "membership must be :relaxed or :closed_cell"))
    strict && membership_policy != :closed_cell && throw(ArgumentError(
        "strict=true is compatible only with membership=:closed_cell"))
    membership_policy == :closed_cell && eps != 0 && throw(ArgumentError(
        "membership=:closed_cell requires eps == 0"))

    real_only = asymptotic_only ? true : nothing
    all_vertice_idx = get_regimes(Bnc, singular=false, asymptotic = real_only, return_idx = true)
    # @show all_vertice_idx
    logqK = input_logspace ? qK : log10.(qK)
    
    record = Vector{Float64}(undef,length(all_vertice_idx))
    for (i, idx) in enumerate(all_vertice_idx)
        C, C0 = get_C_C0_qK(Bnc, idx) 
        min_val = if !asymptotic_only
            minimum(C * logqK .+ C0)
        else
            minimum(C * logqK)
        end
        record[i] = min_val

        if record[i] >= -eps
            return return_idx ? idx : get_perm(Bnc, idx)
        end
    end
    membership_policy == :closed_cell &&
        return return_idx ? 0 : nothing
    @warn("All vertex conditions failed for logqK=$logqK. Returning the best-fit vertex.")
    idx = all_vertice_idx[findmax(record)[2]]
    return return_idx ? idx : get_perm(Bnc, idx)
end

"""
    assign_regime(args...; kwargs...) -> Vector

Alias for `assign_regime_qK`.
"""
assign_regime(args...;kwargs...)=assign_regime_qK(args...;kwargs...)


#-------------------------------------------------------------------------------------------------------------------------------------------------------

# Trying speedup assign_regime_qK, but not success yet.
"""
    get_i_j(model::Bnc, perm, t) -> (Int, Int, Int)

Return row/column indices for a constraint index `t`.
"""
function get_i_j(model::Bnc,perm::Vector{<:Integer}, t::Integer)
    i = findfirst(>(t),model._C_partition_idx) - 1
    j1 = perm[i]
    cth = t - model._C_partition_idx[i] + 1
    j2 = model._valid_L_idx[i][cth]
    j2 < j1 ? nothing : j2 += 1
    return i, j1, j2
end

"""
    assign_regime_qK_test(bnc::Bnc, qK; input_logspace=false, asymptotic=true, eps=0)

Experimental qK regime assignment using constraint violation updates.
"""
function assign_regime_qK_test(Bnc::Bnc{T}, qK::AbstractVector{<:Real};
                               input_logspace::Bool=false,
                               asymptotic::Bool=true, eps=0) where T
    logqK = input_logspace ? qK : log10.(qK)
    Perm_tried = Set{UInt64}()  # 存放哈希值

    function try_perm!(perm1)
        (C, C0) = get_C_C0_qK(Bnc, perm1)
        err = C * logqK .+ C0
        ts = findall(er -> er <= -eps, err)

        # 没有违反不等式，返回
        if isempty(ts)
            return perm1
        end

        h = hash(perm1)
        if h in Perm_tried
            error("Cyclic permutation detected! Tried permutations: $(collect(Perm_tried))")
        end
        push!(Perm_tried, h)

        # 对所有违反的约束更新 perm
        for t in ts
            i, j1, j2 = get_i_j(Bnc, perm1, t)
            perm1[i] = j2
            if !haskey(_bind_regimes_perm_dict(Bnc), perm1) 
                perm1[i] = j1  # 恢复原值
            end
            try_perm!(perm1)
        end
        # else
        #         @show perm1
    end

    # 假设初始 perm1 为 1:Bnc.d 或者外部传入
    perm0 = collect(1:Bnc.d) .|> x->T(x)
    return try_perm!(perm0)
end
