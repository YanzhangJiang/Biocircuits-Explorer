export find_all_regimes

const _NO_CANCEL_CHECK = () -> nothing





@inline function _reduced_ratio(a::T, b::T) where {T<:Integer}
    g = gcd(a, b)
    return div(a, g), div(b, g)
end

function _build_matrix_helper(L::AbstractMatrix{Tv}) where {Tv<:Integer}
    d, n = size(L)
    J = Vector{Vector{Int}}(undef, d)
    choice_slot = [zeros(Int, n) for _ in 1:d]
    choice_logcoeff = Vector{Vector{Float64}}(undef, d)
    choice_map = Vector{Vector{Vector{ChoiceIneq}}}(undef, d)

    # Global deduplicated hyperplane pool
    key_to_id = Dict{Tuple{Int,Int,Tv,Tv}, Int}()
    hyperplanes = Hyperplane_perm{Tv}[]

    # First build J / choice_map
    @inbounds for i in 1:d
        Ji = Int[]
        sizehint!(Ji, n)
        for j in 1:n
            if L[i, j] > 0
                push!(Ji, j)
            end
        end
        isempty(Ji) && throw(ArgumentError("row $i of L has no positive entry"))

        J[i] = Ji
        choice_logcoeff[i] = [log10(Float64(L[i, j])) for j in Ji]

        row_choices = Vector{Vector{ChoiceIneq}}(undef, length(Ji))

        for (t, p) in pairs(Ji)
            choice_slot[i][p] = t

            refs = Vector{ChoiceIneq}(undef, max(length(Ji) - 1, 0))
            ptr = 1
            Lp = L[i, p]

            for k in Ji
                k == p && continue
                Lk = L[i, k]

                # Canonicalize the hyperplane by ordering the variable pair.
                if p < k
                    u, v = p, k
                    num, den = _reduced_ratio(Lp, Lk)
                    sign = Int8(+1)
                else
                    u, v = k, p
                    num, den = _reduced_ratio(Lk, Lp)
                    sign = Int8(-1)
                end

                key = (u, v, num, den)
                hid = get(key_to_id, key, 0)

                if hid == 0
                    # crow = sparsevec([u, v], Int8[1, -1], n)
                    # crow_neg = sparsevec([v, u], Int8[1, -1], n)
                    c0 = log10(Float64(num)) - log10(Float64(den))
                    push!(hyperplanes, Hyperplane_perm{Tv}(u, v, num, den, c0))
                    hid = length(hyperplanes)
                    key_to_id[key] = hid
                end

                refs[ptr] = ChoiceIneq(hid,sign)
                ptr += 1
            end

            row_choices[t] = refs
        end

        choice_map[i] = row_choices
    end

    # Row block pointers for regime constraints:
    # row i contributes |J_i|-1 inequalities, independent of the chosen dominant.
    rowptr = Vector{Int}(undef, d + 1)
    rowptr[1] = 1
    @inbounds for i in 1:d
        rowptr[i + 1] = rowptr[i] + (length(J[i]) - 1)
    end
    total_constraints = rowptr[end] - 1
    
    return MatrixHelper(
        n, J, choice_slot, choice_logcoeff, rowptr, total_constraints,choice_map,hyperplanes
    )
end

function sparsevec(hp::Hyperplane_perm{Tv}, n::Int, sign::Int8) where {Tv<:Integer}
    I = [hp.u, hp.v]
    J = [1, 1]
    V = Int8[sign, -sign]
    return sparse(I, J, V, n, 1)
end


function _enumerate_asymptotic_regimes(helper::MatrixHelper)
    d = length(helper.J)
    n = helper.n

    order = sortperm(helper.J; by = length, rev = true)

    # Asymptotic graph: for choosing v in a row, add k -> v for all competitors k != v.
    graph = [Int[] for _ in 1:n]
    chosen = Vector{Int}(undef, d)
    regimes = Vector{Int}[]

    # Reusable DFS stack for reachability
    stack = Vector{Int}(undef, n)
    visited_stamp = zeros(Int, n)
    stamp_ref = Ref(0)

    function reachable(start::Int, target::Int)::Bool
        stamp_ref[] += 1
        curstamp = stamp_ref[]
        top = 1
        stack[1] = start
        visited_stamp[start] = curstamp

        while top > 0
            u = stack[top]
            top -= 1
            u == target && return true

            @inbounds for w in graph[u]
                if visited_stamp[w] != curstamp
                    visited_stamp[w] = curstamp
                    top += 1
                    stack[top] = w
                end
            end
        end
        return false
    end

    function dfs(r::Int)
        if r > d
            push!(regimes,copy(chosen))
            return
        end

        i = order[r]
        row_choices = helper.J[i]

        @inbounds for v in row_choices
            bad = false
            for k in row_choices
                k == v && continue
                if reachable(v, k)
                    bad = true
                    break
                end
            end
            bad && continue

            for k in row_choices
                k == v && continue
                push!(graph[k], v)
            end

            chosen[i] = v
            dfs(r + 1)

            for k in reverse(row_choices)
                k == v && continue
                pop!(graph[k])
            end
        end
    end

    dfs(1)
    return regimes
end

# ============================================================
# Feasible-regime enumeration
# ============================================================

function _enumerate_all_regimes(
    helper::MatrixHelper,
    eps::Float64 = 1e-9,
    ;
    cancel_check=_NO_CANCEL_CHECK,
)
    cancel_check()
    checkpoint_count = Ref(0)
    function cancellation_checkpoint!()
        checkpoint_count[] += 1
        (checkpoint_count[] & 0xff) == 0 && cancel_check()
        return nothing
    end

    d = length(helper.J)
    n = helper.n
    order = sortperm(helper.J; by = length, rev = true)

    weighted_edges = Vector{Vector{Vector{Tuple{Int,Float64}}}}(undef, d)
    @inbounds for i in 1:d
        cancellation_checkpoint!()
        by_choice = Vector{Vector{Tuple{Int,Float64}}}(undef, length(helper.J[i]))
        for t in eachindex(helper.J[i])
            refs = helper.choice_map[i][t]
            edges = Vector{Tuple{Int,Float64}}(undef, length(refs))
            for s in eachindex(refs)
                ref = refs[s]
                h = helper.hyperplanes[ref.hid]
                competitor = ref.sign == 1 ? h.v : h.u
                oriented_c0 = ref.sign == 1 ? h.c0 : -h.c0
                edges[s] = (competitor, oriented_c0 - eps)
            end
            by_choice[t] = edges
        end
        weighted_edges[i] = by_choice
    end

    adj = [Vector{Tuple{Int,Float64}}() for _ in 1:n]
    dag_adj = [Int[] for _ in 1:n]

    chosen = Vector{Int}(undef, d)
    regimes = Vector{Int}[]
    asymptotic = Bool[]
    

    stack = Vector{Int}(undef, n)
    visited_stamp = zeros(Int, n)
    stamp_ref = Ref(0)

    function dag_reachable(start::Int, target::Int)::Bool
        stamp_ref[] += 1
        curstamp = stamp_ref[]
        top = 1
        stack[1] = start
        visited_stamp[start] = curstamp

        while top > 0
            cancellation_checkpoint!()
            u = stack[top]
            top -= 1
            u == target && return true

            @inbounds for w in dag_adj[u]
                if visited_stamp[w] != curstamp
                    visited_stamp[w] = curstamp
                    top += 1
                    stack[top] = w
                end
            end
        end
        return false
    end

    function has_neg_cycle(seeds::Vector{Int})::Bool
        dist = fill(Inf, n)
        inq = falses(n)
        cnt = zeros(Int, n)
        q = Int[]
        sizehint!(q, max(length(seeds), 8))

        @inbounds for u in seeds
            if !inq[u]
                dist[u] = 0.0
                push!(q, u)
                inq[u] = true
            else
                dist[u] = 0.0
            end
        end

        head = 1
        while head <= length(q)
            cancellation_checkpoint!()
            u = q[head]
            head += 1
            inq[u] = false
            du = dist[u]

            @inbounds for (v, w) in adj[u]
                nd = du + w
                if nd + 1e-15 < dist[v]
                    dist[v] = nd
                    if !inq[v]
                        push!(q, v)
                        inq[v] = true
                        cnt[v] += 1
                        if cnt[v] > n
                            return true
                        end
                    end
                end
            end
        end

        return false
    end

    function dfs(r::Int, still_acyclic::Bool)
        if r > d
            push!(regimes, copy(chosen))
            push!(asymptotic, still_acyclic)
            return
        end

        i = order[r]
        row_choices = helper.J[i]

        @inbounds for v in row_choices
            cancellation_checkpoint!()
            t = helper.choice_slot[i][v]

            local_acyclic = still_acyclic
            if still_acyclic
                for k in row_choices
                    k == v && continue
                    if dag_reachable(v, k)
                        local_acyclic = false
                        break
                    end
                end
            end

            oldlen_w = length(adj[v])
            append!(adj[v], weighted_edges[i][t])

            for k in row_choices
                k == v && continue
                push!(dag_adj[k], v)
            end

            if !has_neg_cycle(row_choices)
                chosen[i] = v
                dfs(r + 1, local_acyclic)
            end

            resize!(adj[v], oldlen_w)

            for k in reverse(row_choices)
                k == v && continue
                pop!(dag_adj[k])
            end
        end
    end

    dfs(1, true)
    cancel_check()
    return regimes, asymptotic
end




function _perm_process(
    perm::Vector{<:Integer},
    helper::MatrixHelper{Tv},
) where {Tv<:Integer}
    d = length(helper.J)
    # n = helper.n
    P0 = Vector{Float64}(undef, d)
    hyperplane_id_signs = Vector{Tuple{Int,Int8}}(undef, helper.total_constraints)
    # signs = Vector{Int8}(undef, helper.total_constraints)

    @inbounds for i in 1:d
        p = perm[i]
        t = helper.choice_slot[i][p]
        P0[i] = helper.choice_logcoeff[i][t]

        refs = helper.choice_map[i][t]
        block_start = helper.rowptr[i]
        for s in eachindex(refs)
            hyperplane_id_signs[block_start + s - 1] = (refs[s].hid, refs[s].sign)
        end
    end

    return P0, hyperplane_id_signs
end


function _calc_P_P0(perm::Vector{<:Integer},
    helper::MatrixHelper{Tv},
) where {Tv<:Integer}
    d = length(perm)
    n = helper.n
    P0 = Vector{Float64}(undef, d)
    @inbounds for i in 1:d
        p = perm[i]
        t = helper.choice_slot[i][p]
        P0[i] = helper.choice_logcoeff[i][t]
    end
    I = collect(1:d)
    J = copy(perm)
    V = ones(Int8, d)
    P = sparse(I, J, V, d, n)
    return P, P0
end

function _calc_C_C0(
    perm::Vector{<:Integer},
    helper::MatrixHelper{Tv},
) where {Tv<:Integer}
    d = length(perm)
    m = helper.total_constraints
    n = helper.n

    # 稀疏矩阵三元组
    I = Vector{Int}(undef, 2m)
    J = Vector{Int}(undef, 2m)
    V = Vector{Int8}(undef, 2m)

    C0 = Vector{Float64}(undef, m)

    ptr = 1
    @inbounds for i in 1:d
        p = perm[i]
        t = helper.choice_slot[i][p]

        refs = helper.choice_map[i][t]
        block_start = helper.rowptr[i]

        for s in eachindex(refs)
            row = block_start + s - 1
            ref = refs[s]
            h = helper.hyperplanes[ref.hid]

            if ref.sign == 1
                I[ptr] = row; J[ptr] = h.u; V[ptr] = Int8(1);  ptr += 1
                I[ptr] = row; J[ptr] = h.v; V[ptr] = Int8(-1); ptr += 1
                C0[row] = h.c0
            else
                I[ptr] = row; J[ptr] = h.v; V[ptr] = Int8(1);  ptr += 1
                I[ptr] = row; J[ptr] = h.u; V[ptr] = Int8(-1); ptr += 1
                C0[row] = -h.c0
            end
        end
    end

    C = sparse(I, J, V, m, n)
    return C, C0
end

@inline function _calc_perm_nullity(perm)
    perm_nullity = 0
    seen = falses(length(perm))
    @inbounds for p in perm
        if seen[p]
            perm_nullity += 1
        else
            seen[p] = true
        end
    end
    return perm_nullity
end




# ============================================================
# Top-level compiler
# ============================================================

"""
    find_all_regimes(L; eps=1e-9, dominance_ratio=Inf,
                       enumerate_asymptotic_only=false)

Compile all regime-related structures from a nonnegative integer matrix `L`.

Returns a `RegimeCatalog` containing:

1. `hyperplanes`:
   global deduplicated hyperplane pool

2. `choice_map` + `choice_slot`:
   from `(i,p)` to the corresponding hyperplanes and orientations

3. `asymptotic`:
   all regimes whose recession cone is full-dimensional

4. `feasible`:
   all regimes feasible under the weighted difference constraints

Notes:
- `dominance_ratio == Inf` means use `eps` directly
- otherwise, feasible mode uses `eps_eff = log10(dominance_ratio)`
"""
function find_all_regimes(
    L::AbstractMatrix{Tv};
    eps::Real = 1e-9,
    dominance_ratio::Real = Inf,
    enumerate_asymptotic_only::Bool = false,
) where {Tv<:Integer}
    L_helper = _build_matrix_helper(L)
    if enumerate_asymptotic_only
        perms =  _enumerate_asymptotic_regimes(L_helper)
        asymptotic = [true for _ in perms]
    else
        if dominance_ratio != Inf && dominance_ratio < 1
            throw(ArgumentError("dominance_ratio must be >= 1 or Inf"))
        end
        eps_eff = dominance_ratio == Inf ? Float64(eps) : log10(Float64(dominance_ratio))
        perms, asymptotic = _enumerate_all_regimes(L_helper, eps_eff)
    end
    return perms, asymptotic

end


#=============================================================#
# Utils
#==============================================================#
@inline function choiceineq_between(helper::MatrixHelper, i::Int, j2::Int, j1::Int)
    tp = helper.choice_slot[i][j2]
    tk = helper.choice_slot[i][j1]

    # tp == 0 && throw(ArgumentError("p=$j2 ∉ J[$i]"))
    # tk == 0 && throw(ArgumentError("k=$j1 ∉ J[$i]"))
    # j2 == j1 && throw(ArgumentError("p and k must be different"))

    s = tk < tp ? tk : tk - 1
    return helper.choice_map[i][tp][s]
end
