"""
    _enumerate_vertices_nonasymptotic(L, ::Val{T}; eps=1e-9) -> Vector{Vector{T}}

Enumerate non-asymptotic regime assignments based on `L`.
"""
function _enumerate_vertices_nonasymptotic(L, ::Val{T} ;eps=1e-9) where T
    d, n = size(L)
    # T = get_int_type(n)  # Determine integer type based on n
    # Nonzero indices of each row
    J = [findall(!iszero, row) for row in eachrow(L)]
    
    order = sortperm(J, by=length, rev=true)
    inv_order = invperm(order)
    J_ord = J[order]

    # Precompute VertexEdge weights: Dict[u => edges] for each row
    row_edges = map(1:d) do i
        Ji = J[i]
        logL = log.(L[i, Ji])  # compute once per row
        Dict(Ji[k] => [(Ji[m], -((logL[m] - logL[k]) + eps)) for m in eachindex(Ji) if m != k]
             for k in eachindex(Ji))
    end

    adj = [Vector{Tuple{T,Float64}}() for _ in 1:n]
    results = Vector{Vector{T}}()

    function has_neg_cycle(seeds)
        # dist_local = zeros(Float64, n)   # rollback safety
        dist_local = fill(Inf, n)
        q = Queue{T}()
        inq = falses(n)
        cnt = zeros(T, n)
        for u in seeds
            dist_local[u] = 0.0
            enqueue!(q, u)
            inq[u] = true
        end
        while !isempty(q)
            u = dequeue!(q)
            inq[u] = false
            du = dist_local[u]
            for (v, w) in adj[u]
                nd = du + w
                if nd + 1e-15 < dist_local[v]
                    dist_local[v] = nd
                    if !inq[v]
                        enqueue!(q, v)
                        inq[v] = true
                        cnt[v] += 1
                        if cnt[v] > n
                            return true  # negative cycle detected
                        end
                    end
                end
            end
        end
        return false
    end

    function dfs(r, chosen)
        if r > d
            # @show adj
            push!(results, chosen[inv_order])
            return
        end
        for u in J_ord[r]
            oldlen = length(adj[u])
            append!(adj[u], row_edges[order[r]][u])
            if !has_neg_cycle(J_ord[r])
                push!(chosen, u)
                dfs(r+1, chosen)
                pop!(chosen)
            end
            resize!(adj[u], oldlen)  # rollback edges
        end
    end

    dfs(1, Int[])
    return results
end

"""
    _enumerate_vertices_asymptotic(L, ::Val{T}) -> Vector{Vector{T}}

Enumerate asymptotic regime assignments based on `L`.
"""
function _enumerate_vertices_asymptotic(L, ::Val{T}) where T
    d, n = size(L)
    J = [findall(x -> x != 0, row) for row in eachrow(L)]
    order = sortperm(J, by = length, rev=true)
    inv_order = invperm(order)
    J_ord = J[order]

    graph = [T[] for _ in 1:n]
    results = Vector{Vector{T}}()


    stack = Vector{T}(undef, n)                 # DFS stack (reused)
    visited_stamp = zeros(Int, n)                 # visited stamps per node
    stamp_ref = Ref(0)
    function reachable(start, target)::Bool
        stamp_ref[] += 1
        curstamp = stamp_ref[]
        top = 1
        stack[1] = start
        visited_stamp[start] = curstamp
        while top > 0
            u = stack[top]; top -= 1
            if u == target
                return true
            end
            for w in graph[u]
                if visited_stamp[w] != curstamp
                    visited_stamp[w] = curstamp
                    top += 1
                    stack[top] = w
                end
            end
        end
        return false
    end

    # fast reachable using stamp to avoid clearing visited array
    function dfs(r,chosen)
        if r > d
            push!(results, chosen[inv_order])
            # @show graph
            return
        end

        row_choices = J_ord[r]
        any_success = false
        for v in row_choices
            
            # if v can reach some k in this row, adding k->v would create cycle
            bad = false
            for k in row_choices
                k == v && continue
                if reachable(v, k)
                    bad = true
                    break
                end
            end
            bad && continue

            # add edges k -> v for k != v
            for k in row_choices
                k == v && continue
                push!(graph[k], v)
            end

            push!(chosen, v)
            dfs(r + 1,chosen)
            pop!(chosen)

            # remove added edges
            for k in reverse(row_choices)
                k == v && continue
                pop!(graph[k])
            end
            any_success = true
        end
    end
    dfs(1,T[])
    return results
end

"""
    find_all_vertices_nonasym(L; kwargs...) -> Vector{Vector{Int}}

Convenience wrapper for non-asymptotic vertex enumeration.
"""
find_all_vertices_nonasym(L;kwargs...) = _enumerate_vertices_nonasymptotic(L,Val(get_int_type(size(L)[2]));kwargs...) 
"""
    find_all_vertices_asym(L; kwargs...) -> Vector{Vector{Int}}

Convenience wrapper for asymptotic vertex enumeration.
"""
find_all_vertices_asym(L;kwargs...) = _enumerate_vertices_asymptotic(L,Val(get_int_type(size(L)[2]));kwargs...)
"""
    find_all_vertices(L; eps=1e-9, dominance_ratio=Inf, asymptotic=nothing) -> Vector{Vector{Int}}

Find all feasible regime assignments for conservation matrix `L`.

# Keyword Arguments
- `eps`: Slack used for non-asymptotic mode.
- `dominance_ratio`: Dominance ratio (Inf for exact).
- `asymptotic`: Force asymptotic (`true`) or non-asymptotic (`false`) enumeration.
"""
function find_all_vertices(L::Matrix{Int} ; eps=1e-9, dominance_ratio=Inf, asymptotic::Union{Bool,Nothing}=nothing)
    asymptotic =  (isnothing(asymptotic) && dominance_ratio == Inf) || (asymptotic == true)
    eps = asymptotic ? nothing : (dominance_ratio == Inf ? eps : log(dominance_ratio))  # extra slack for weighted mode 
    if asymptotic
        return find_all_vertices_asym(L)
    else
        return find_all_vertices_nonasym(L,eps=eps)
    end
end
