"""
    Bnc(; N=nothing, L=nothing, x_sym=nothing, q_sym=nothing, K_sym=nothing,
        Γ=nothing, Π=nothing, k=nothing, cat_x_idx=nothing) -> Bnc

Construct a binding network model from stoichiometry (`N`) or conservation (`L`)
matrices and optional symbol metadata. Catalysis data can be attached through
`Γ`, `Π`, and `k`.

# Keyword Arguments
- `N`: Stoichiometry matrix (reactions × species).
- `L`: Conservation matrix (totals × species).
- `x_sym`: Symbols for species concentrations.
- `q_sym`: Symbols for total concentrations.
- `K_sym`: Symbols for binding constants.
- `Γ`: Catalysis change matrix in qK space.
- `Π`: Catalysis index and coefficient matrix.
- `k`: Catalysis rate constants.
- `cat_x_idx`: Index of catalytic species.

# Returns
- A `Bnc` model with derived matrices and caches initialized.
"""
function Bnc(;N=nothing,L=nothing,
    x_sym=nothing,q_sym=nothing,K_sym=nothing,
    kwargs...
)::Bnc
    # if N is not provided, derive it from L, if provided, check its linear indenpendency
    
    N = isnothing(N) ? N_from_L(L) : N
    row_idx = independent_row_idx(N)
    r = length(row_idx)

    if isnothing(L)
        if r != size(N,1) @warn("N has been reduced from $r to $r_new rows, for linear dependent.") : nothing
            N = N[row_idx, :] # reduce N to independent rows
            if !isnothing(K_sym) && length(K_sym) == r
                K_sym = K_sym[row_idx] # reduce K_sym to independent rows 
            end
        end
        L = L_from_N(N)
    else # L is provided
        if r!= size(N,1) && size(N,1) +size(L,1) ==size(N,2)
            @warn "N is not full row rank and can't be reduced, numerical issures could happen"
        end
    end

    r,n = size(N)
    d = size(L,1)
    

    # Call the inner constructor
    # Number of variables in the binding network
    x_sym = isnothing(x_sym) ? Symbolics.variables(:x, 1:n) : name_converter(x_sym) # convert x_sym to a vector of symbols
    q_sym = isnothing(q_sym) ? Symbolics.variables(:q, 1:d) : name_converter(q_sym) # convert q_sym to a vector of symbols
    K_sym = isnothing(K_sym) ? Symbolics.variables(:K, 1:r) : name_converter(K_sym) # convert K_sym to a vector of symbols

    model = Bnc{Int}(N, L, x_sym, q_sym, K_sym, nothing)
    update_catalysis!(model; kwargs...)
    return model
end



"""
    update_catalysis!(bnc::Bnc; Γ=nothing, Π=nothing, k=nothing, cat_x_idx=nothing) -> Bnc

Attach or update catalysis data on a `Bnc` model in-place.

# Arguments
- `bnc`: Binding network model to update.

# Keyword Arguments
- `Γ`: Catalysis change matrix in qK space.
- `Π`: Catalysis index and coefficient matrix.
- `k`: Rate constants.
- `cat_x_idx`: Index of catalytic species.

# Returns
- The updated `bnc`.
"""
function update_catalysis!(model::Bnc;
    Γ::Union{<:AbstractMatrix{Int},Nothing}=nothing,
    Π::Union{<:AbstractMatrix{Int},Nothing}=nothing,
    k_sym::Union{<:AbstractVector,Nothing}=nothing,
    x_picked::Union{<:AbstractVector,Nothing}=nothing,
    q_picked::Union{<:AbstractVector,Nothing}=nothing,
    )
    if isnothing(Γ) && isnothing(Π)
        return nothing
    else 
        @assert !isnothing(Γ) && !isnothing(Π) "You shall provide both Γ and Π"
    end

    Π = if isnothing(x_picked)
            Π
        else
            x_idx = locate_sym_x.(Ref(model), x_picked)
            Π2 = zeros(Int, (size(Π,1),model.n))
            for (i,x) in enumerate(x_idx)
                Π2[:,x] .= Π[:, i]
            end
            Π2
        end

    if !isnothing(q_picked)
        q_idx = locate_sym_qK.(Ref(model), q_picked)
        new_order = vcat(q_idx, setdiff(1:model.d, q_idx))
        _change_q_L_order!(model, new_order) # reorder the q and L in the model to make the picked q first, since the catalysis will involve the first r_v q.
        _remove_regime_data!(model) # remove the cached regime data, since the regimes will be changed after reordering q and L.
    else
        @info "q_cat is not picked, the catalysis will involve the first r_v q by default"
    end

    k_sym = isnothing(k_sym) ? Symbolics.variables(:k, 1:size(Π,1)) : name_converter(k_sym)
    model.catalysis = CatalysisData(model,Γ,Π, k_sym)
    return nothing
end

function fix_bn_catalysis!(bn::Bnc, new_ord::Vector{Int},L_Γ::AbstractMatrix{Int})
    if new_ord !== collect(1:length(new_ord)) # no reording should be made
        _change_q_L_order!(bn, new_ord)

        @info "q is reordered to make catalysis-involving species first"
        d_dep = size(L_Γ,2)
        d_cat_full = length(new_ord)
        d_cat = d_cat_full - d_dep

        if d_dep >0
            @info "New conservation forms as catalysis involves"
            #update the name of q_sym to make the first d_cat are q_cat, and the rest are q_dep
            bn.q_sym[(d_cat+1):d_cat_full] = Symbolics.variables(:w, 1:d_dep)
            # Calculate the L_w
            L_w = L_Γ' * bn.L[1:d_cat_full,:]
            @assert all(L_w .>=0) "L_w should be non-negative"
            #update L_w to replace L_dep
            bn.L[(d_cat+1):d_cat_full,:] = L_w
        end

        dropzeros!(bn.L)
        _remove_regime_data!(bn) # remove the cached regime data, since the regimes will be changed.
        #other initializing
    end

    _rebuild_helper!(bn) # rebuild the helper parameters since L has been changed.
    return nothing
end


@inline function _change_q_L_order!(bn::Bnc, new_ord::Vector{Int})
    bn.q_sym[1:length(new_ord)] = bn.q_sym[new_ord]
    bn.L[1:length(new_ord),:] = bn.L[new_ord, :]
end

@inline function _rebuild_helper!(bn::Bnc)
    bn.direction = sign(det([bn.L;bn.N])) # recalculate the direction, since L has been changed.
    bn.IntegrationHelper = nothing # lazily rebuild integration helper on first numerical integration.
    bn._L_helper = _build_matrix_helper(bn.L)
    return nothing
end

@inline function _integration_helper!(bn::Bnc)
    helper = bn.IntegrationHelper
    if !isnothing(helper)
        return helper
    end

    lock(bn._integration_helper_lock)
    try
        helper = bn.IntegrationHelper
        if isnothing(helper)
            helper = calc_integration_helper(bn.L, bn.N)
            bn.IntegrationHelper = helper
        end
        return helper
    finally
        unlock(bn._integration_helper_lock)
    end
end

@inline function _remove_regime_data!(bn::Bnc{T}) where T 
    bn.BindRegimes = nothing
    bn.BncRegimes = nothing
    bn.vertices_graph = nothing
    bn._vertices_Nρ_inv_dict = Dict{Vector{T}, Tuple{SparseMatrixCSC{Float64, Int},T}}()
    bn._regimes_affine_ready = false
    return nothing
end


"""
    summary(bnc::Bnc) -> String

Print a summary of a binding network model to standard output.
"""
function summary(model::Bnc)
    println("----------Binding Network Summary:-------------")
    println("Number of species (n): ", model.n)
    println("Number of conserved quantities (d): ", model.d)
    println("Number of reactions (r): ", model.r)
    println("L matrix: ", model.L)
    println("N matrix: ", model.N)
    println("Direction of binding reactions: ", model.direction > 0 ? "forward" : "backward")
    catalysis_str = isnothing(model.catalysis) ? "No" : "Yes"
    println("Catalysis involved: ", catalysis_str)
    is_regimes_built = _bind_regimes_built(model) ? "Yes" : "No"
    println("Regimes constructed: ", is_regimes_built)
    if _bind_regimes_built(model)
        vertices = _bind_regimes_data(model)
        map = countmap((vtx.is_asymptotic, vtx.nullity > 0) for vtx in vertices)
        println("Number of regimes: ", length(vertices))
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
    is_regimes_built = _bind_regimes_built(bnc) ? "Yes" : "No"
    println(io, "Regimes constructed: ", is_regimes_built)
    if _bind_regimes_built(bnc)
        vertices = _bind_regimes_data(bnc)
        map = countmap((vtx.is_asymptotic, vtx.nullity > 0) for vtx in vertices)
        println(io, "Number of regimes: ", length(vertices))
        println(io, "  - Invertible + Asymptotic: ", get(map, (true, false), 0))
        println(io, "  - Singular +  Asymptotic: ", get(map, (true, true), 0))
        println(io, "  - Invertible +  Non-Asymptotic: ", get(map, (false, false), 0))
        println(io, "  - Singular +  Non-Asymptotic: ", get(map, (false, true), 0))
    end
    print(io, "-----------------------------------------------") # 最后一行可用 print 避免额外空行
end
