export locate_sym_x, locate_sym_qK, pythonprint, N_generator, L_generator, randomize
export same_polyhedron

"""
    L_from_N(N::Matrix{Int}) -> Matrix{Int}

Compute a conservation matrix `L` from a stoichiometry matrix `N` such that
`N * L' = 0`.

# Arguments
- `N`: Stoichiometry matrix with reactions as rows.

# Returns
- A conservation matrix `L` whose rows span the left nullspace of `N`.
"""
function L_from_N(N::Matrix{Int})::Matrix{Int}
    r, n = size(N)
    d = n - r
    N_1 = @view N[:, 1:d]
    N_2 = @view N[:, d+1:n]
    hcat(Matrix(I, d, d), -(N_2 \ N_1)')
end

"""
    N_from_L(L::Matrix{Int}) -> Matrix{Int}

Recover a stoichiometry matrix `N` from a conservation matrix `L` assuming the
standard block structure used in this package.

# Arguments
- `L`: Conservation matrix where rows correspond to conserved totals.

# Returns
- A stoichiometry matrix `N` compatible with `L`.
"""
function N_from_L(L::Matrix{Int})::Matrix{Int}
    d, n = size(L)
    r = n - d
    L2 = @view L[:,d+1:n]
    hcat(L2',Matrix(-I,r,r))
end

"""
    name_converter(name::Vector{<:T}) -> Vector{Num}

Convert a vector of symbols or numbers into Symbolics variables.

# Arguments
- `name`: Vector of `Symbol`, `String`, or `Num`.

# Returns
- Vector of `Symbolics.Num` objects.
"""
function name_converter(name::Vector{<:T})::Vector{Num} where T 
    if T <: Num
        return name
    else
        return [Symbolics.variable(x; T=Real) for x in name]
    end
end


"""
    rowmask_indices(A::SparseMatrixCSC, start_row::Int, end_row::Int)

Return the row indices, column indices, and nzval positions for nonzeros in a
row range of a sparse matrix.

# Arguments
- `A`: Sparse matrix to scan.
- `start_row`: First row (inclusive).
- `end_row`: Last row (inclusive).

# Returns
- Tuple `(rows, cols, idxs)` where `idxs` indexes `A.nzval`.
"""
function rowmask_indices(A::SparseMatrixCSC, start_row::Int, end_row::Int)
    # 获取稀疏矩阵 A 中前 d行 的非零元素的行、列索引及其在 nzval 中的位置
    rows = Int[]        # 存储行坐标
    cols = Int[]        # 存储列坐标
    idxs = Int[]        # 存储 nzval 的索引位置

    for j in 1:size(A,2)              # 遍历列
        for k in A.colptr[j]:(A.colptr[j+1]-1)  # 遍历列的非零
            i = A.rowval[k]
            if i >= start_row && i <= end_row
                push!(rows, i)
                push!(cols, j)
                push!(idxs, k)
            end
        end
    end
    return rows, cols, idxs
end

"""
    diag_indices(A::SparseMatrixCSC, end_row::Int) -> Vector{Int}

Return indices into `A.nzval` for diagonal entries up to `end_row`.

# Arguments
- `A`: Sparse matrix.
- `end_row`: Last diagonal entry to include.

# Returns
- Vector of nzval indices for the diagonal.
"""
function diag_indices(A::SparseMatrixCSC,end_row::Int)
    # 获取稀疏矩阵 A 中对角线前end_row行元素在 nzval 中的位置
    idxs = Int[]        # 存储 nzval 的索引位置

    for j in 1:size(A,2)              # 遍历列
        for k in A.colptr[j]:(A.colptr[j+1]-1)  # 遍历列的非零
            i = A.rowval[k]
            if i == j && i <= end_row
                push!(idxs, k)
            end
        end
    end
    return idxs
end

"""
    randomize(n::Int, size; kwargs...) -> Array{Vector{Float64}}

Generate an array of random vectors in log space.

# Arguments
- `n`: Length of each random vector.
- `size`: Dimensions of the output array.

# Keyword Arguments
- `log_lower`: Lower bound in log10 space.
- `log_upper`: Upper bound in log10 space.
- `output_logspace`: Return log10 values when `true`.

# Returns
- Array of random vectors.
"""
function randomize(n::Int, size; kwargs...)::Array{Vector{Float64}}
    N = Array{Vector{Float64}}(undef, size...)
    
    Threads.@threads for i in eachindex(N)
        N[i] = randomize(n,; kwargs...)
    end
    return N
end

"""
    randomize(n::Int; log_lower=-6, log_upper=6, output_logspace=true) -> Vector{Float64}

Generate a random vector with entries sampled uniformly in log10 space.

# Arguments
- `n`: Length of the vector.

# Keyword Arguments
- `log_lower`: Lower bound in log10 space.
- `log_upper`: Upper bound in log10 space.
- `output_logspace`: When `false`, return values in linear space.

# Returns
- Vector of random values (log10 or linear).
"""
function randomize(n::Int; log_lower=-6, log_upper=6, output_logspace::Bool=true)::Vector{Float64}
    #turn lowerbound and upperbound into bases of e
    if !output_logspace
        exp10.(rand(n) .* (log_upper - log_lower) .+ log_lower)
    else
        rand(n) .* (log_upper - log_lower) .+ log_lower
    end
end
randomize(Bnc::Bnc, size; kwargs...) = randomize(Bnc.n, size; kwargs...)


"""
    arr_to_vector(arr)

Convert a multidimensional array into nested vectors, preserving the first
dimension as the outer vector.

# Arguments
- `arr`: Array of any dimension.

# Returns
- Nested vector representation.
"""
function arr_to_vector(arr)
    d = ndims(arr)
    if d == 0
        return arr[]  # 处理0维数组（标量）
    elseif d == 1
        return [x for x in arr]  # 1维数组转列表
    else
        # 沿第一维切片，递归处理每个切片（降维后）
        return [arr_to_vector(s) for s in eachslice(arr, dims=1)]
    end
end
#= Unused helper retained for optional console pretty-printing.
=#
"""
    pythonprint(arr) -> nothing

Pretty-print an array in JSON format for easy inspection.

# Arguments
- `arr`: Array-like input.
"""
function pythonprint(arr)
    txt = JSON3.write(arr_to_vector(arr), pretty=true, indent=4, escape_unicode=false)
    println(txt)
    return nothing
end



"""
    N_generator(r::Int, n::Int; min_binder=2, max_binder=2) -> Matrix{Int}

Generate a random stoichiometry matrix with `r` reactions and `n` species.

# Arguments
- `r`: Number of reactions.
- `n`: Number of species.

# Keyword Arguments
- `min_binder`: Minimum number of binders per reaction.
- `max_binder`: Maximum number of binders per reaction.

# Returns
- Random stoichiometry matrix `N`.
"""
function N_generator(r::Int, n::Int; min_binder::Int=2, max_binder::Int=2)::Matrix{Int}
    @assert n > r "n must be greater than r"
    @assert min_binder >= 1 && max_binder >= min_binder "min_binder and max_binder must be at least 1"
    @assert min_binder <= n - r "min_binder must be smaller than n-r"
    #initialize the matrix
    d = n-r
    N = [zeros(r,d) -I(r)]
    Threads.@threads for i in 1:r
        idx = sample(1:d+i-1,rand(min_binder:max_binder); replace=true)
        for j in idx
            N[i,j] +=1
        end
    end
    return N
end

"""
    L_generator(d::Int, n::Int; kwargs...) -> Matrix{Int}

Generate a random conservation matrix `L` with `d` conserved totals.

# Arguments
- `d`: Number of conservation laws.
- `n`: Number of species.

# Keyword Arguments
- Passed through to [`N_generator`](@ref).

# Returns
- Conservation matrix `L`.
"""
function L_generator(d::Int, n::Int; kwargs...)::Matrix{Int}
    N = N_generator(n - d, n; kwargs...)
    L = L_from_N(N)
    return L
end


"""
    rebase_mat_lgK(N::AbstractMatrix) -> AbstractMatrix
give a rebase matrix "Q" for logK from  Nlogx = logK to   ̃N logx = log ̃K 
where logK = Q log ̃K,

# Based on  ̃N is v^⊤ from svd of N which span same space as N, Q= Σ^{-1}U^⊤

Based on independent energy for complexes
"""

function rebase_mat_lgK(N::AbstractMatrix)
    N2 = N_from_L(L_from_N(N))
    Q_inv = Rational.(round.(Int, N2/N))
    return sparse(inv(Q_inv))
end


# function rebase_mat_lgK(N::AbstractMatrix)::AbstractMatrix
#     U,σ,V = svd(N)
#     Q = U' ./σ
#     return Q
# end


"""
    independent_row_idx(N::AbstractMatrix)

Return indices of linearly independent rows in `N`.

# Arguments
- `N`: Input matrix.

# Returns
- Vector of row indices.
"""
function independent_row_idx(N::AbstractMatrix{T}) where T
    # find linear independent rows of a matrix N and return the index
    Nt_lu = lu(N',check=false)
    issuccess(Nt_lu) && return collect(1:size(N, 1))
    tol = 1e-8
    pivot_indices = findall(abs.(diag(Nt_lu.U)) .> tol)
    return pivot_indices
end

"""
    _ode_solution_wrapper(solution::ODESolution) -> (Vector{Float64}, Vector{Vector{Float64}})

Convert a DifferentialEquations.jl solution into time and state arrays.

# Arguments
- `solution`: ODESolution object.

# Returns
- Tuple `(t, u)` where `t` is the time vector and `u` is the state history.
"""
function _ode_solution_wrapper(
    solution::ODESolution
    )::Tuple{Vector{Float64}, Vector{Vector{Float64}}}
    return solution.t, solution.u
end


#= Unused helper retained for potential distance-matrix utilities.
function pairwise_distance(data::AbstractVector, dist_func::Function; is_symmetric::Bool=true)
    n = length(data)
    # Determine the output type by calculating one distance value first.
    # This creates a type-stable matrix, which is more efficient.
    T = typeof(dist_func(data[1], data[1]))
    dist_matrix = Matrix{T}(undef, n, n)
    if is_symmetric
        Threads.@threads for i in 1:n
            for j in i:n
                d = dist_func(data[i], data[j])
                dist_matrix[i, j] = d
                dist_matrix[j, i] = d
            end
        end
    else
        Threads.@threads for i in 1:n
            for j in 1:n
                dist_matrix[i, j] = dist_func(data[i], data[j])
            end
        end
    end
    return dist_matrix
end
=#


"""
    log10_sym(x) -> Num

Symbolic `log10` wrapper that preserves `Num(0)` for unity.
"""
log10_sym(x) = x==1 ? Num(0) : Symbolics.wrap(Symbolics.Term(log10, [x,]))

"""
    exp10_sym(x) -> Num

Symbolic `exp10` wrapper.
"""
exp10_sym(x) = Symbolics.wrap(Symbolics.Term(exp10, [x,]))


"""
    get_int_type(n) -> Type

Select the smallest signed integer type that can represent `n + 1`.

# Arguments
- `n`: Maximum value to represent.

# Returns
- Integer type (`Int8`, `Int16`, `Int32`, `Int64`, or `Int128`).
"""
function get_int_type(n)
    # Get the integer type based on the number of bits
    m = n+1
    if m <= typemax(Int8)
        return Int8
    elseif m <= typemax(Int16)
        return Int16
    elseif m <= typemax(Int32)
        return Int32
    elseif m <= typemax(Int64)
        return Int64
    else
        return Int128
    end
end



#Pure helper functions for converting between matrix and index-value pairs.
"""
    _Mtx2idx_val(Mtx::Matrix) -> (Vector{Int}, Vector)

Convert a single-nonzero-per-row matrix into index and value vectors.

# Arguments
- `Mtx`: Matrix with at most one nonzero per row.

# Returns
- Tuple `(idx, val)` storing column index and value for each row.
"""
function _Mtx2idx_val(Mtx::Matrix{<:T}) where T
    row_num, col_num  = size(Mtx)
    idx = Vector{Int}(undef, row_num)
    val = Vector{T}(undef, row_num)
    for i in 1:row_num
        for j in 1:col_num
            if Mtx[i, j] != 0
                idx[i] = j
                val[i] = Mtx[i, j]
                break 
            end
        end
    end
    return idx,val
end
"""
    _idx_val2Mtx(idx::Vector{Int}, val::T=1; col_num=nothing) -> Matrix

Create a matrix with one nonzero per row from index and value vectors.

# Arguments
- `idx`: Column indices per row.
- `val`: Scalar value for each row.
- `col_num`: Optional number of columns.

# Returns
- Dense matrix with specified nonzeros.
"""
function _idx_val2Mtx(idx::Vector{Int}, val::T=1, col_num::Union{Int,Nothing}=nothing) where T
    n = length(idx)
    col_num = isnothing(col_num) ? n : col_num # if col_num is not provided, use the maximum idx value
    Mtx = zeros(T, n, col_num)
    for i in 1:n
        if idx[i] != 0
            Mtx[i, idx[i]] = val
        end
    end
    return Mtx
    
end
"""
    _idx_val2Mtx(idx::Vector{Int}, val::Vector; col_num=nothing) -> Matrix

Create a matrix with one nonzero per row using per-row values.

# Arguments
- `idx`: Column indices per row.
- `val`: Values per row.
- `col_num`: Optional number of columns.

# Returns
- Dense matrix with specified nonzeros.
"""
function _idx_val2Mtx(idx::Vector{Int}, val::Vector{<:T}, col_num::Union{Int,Nothing}=nothing) where T
    # Convert idx and val to a matrix of size (d, n)
    n = length(idx)
    col_num = isnothing(col_num) ? n : col_num
    @assert length(val) == n "val must have the same length as idx"
    Mtx = zeros(T, n, col_num)
    for i in 1:n
        if idx[i] != 0
            Mtx[i, idx[i]] = val[i]
        end
    end
    return Mtx
end




"""
    matrix_iter(f, M; byrow=true, multithread=true) -> Matrix

Apply a function to each row or column of a matrix and collect results in a
matrix.

# Arguments
- `f`: Function applied to each slice.
- `M`: Input matrix.

# Keyword Arguments
- `byrow`: When `true`, apply to rows; otherwise columns.
- `multithread`: Use multithreading when available.

# Returns
- Matrix of results with one row/column per input slice.
"""
function matrix_iter(f::Function, M::AbstractArray{<:Any,2}; byrow::Bool=true,multithread::Bool=true)
    # Get the number of rows from the input matrix
    if byrow
        num_rows = size(M, 1)
        # Check if the matrix is empty
        if num_rows == 0
            return Matrix{Any}(undef, 0, 0) # Or return an appropriately typed empty matrix
        end
        first_row = first(eachrow(M))
        # Apply the function to the first row to get a sample result
        first_result = f(first_row)
        # Determine the size of the output matrix
        # The number of rows in the result matrix is the length of the vector returned by f.
        # The number of columns is the number of rows in the input matrix.
        result_cols = length(first_result)
        result_rows = num_rows
        # Pre-allocate the result matrix with the correct type and size
        # This is key for performance!
        result = Matrix{eltype(first_result)}(undef, result_rows, result_cols)
        # Place the first result in the first column
        result[1,:] = first_result
        # Loop through the rest of the rows (from the 2nd row onwards)
        # We use `enumerate` to get the index `i` (starting from 2)
        # and `Iterators.drop` to skip the first row which we've already processed.
        if multithread
            current_BLAS_threads = BLAS.get_num_threads()
            BLAS.set_num_threads(1) # Set to 1 to avoid multithreading
            # --- FIXED PART (byrow) ---
            Threads.@threads for i in 2:num_rows
                result[i, :] = f(@view M[i, :])
            end
            BLAS.set_num_threads(current_BLAS_threads) # Restore the original number of threads
        else
            # This part was already okay, but we use views for consistency
            for i in 2:num_rows
                result[i, :] = f(@view M[i, :])
            end
        end
        return result
    else
        num_cols = size(M, 2)
        # Check if the matrix is empty
        if num_cols == 0
            return Matrix{Any}(undef, 0, 0) # Or return an appropriately typed empty matrix
        end
        first_col = first(eachcol(M))
        first_result = f(first_col)
        result_rows = length(first_result)
        result_cols = num_cols
        result = Matrix{eltype(first_result)}(undef, result_rows, result_cols)
        result[:, 1] = first_result
        if multithread
            current_BLAS_threads = BLAS.get_num_threads()
            Threads.@threads for j in 2:num_cols
                result[:, j] = f(@view M[:, j])
            end
            BLAS.set_num_threads(current_BLAS_threads)
        else
            for j in 2:num_cols
                result[:, j] = f(@view M[:, j])
            end
        end
        return result
    end
end


"""
    vector_difference(v1, v2) -> Vector

Summarize the differences between two vectors as counts of value transitions.

# Arguments
- `v1`: First vector.
- `v2`: Second vector of the same length.

# Returns
- Sorted vector of `(pair => count)` entries.
"""
function vector_difference(v1::AbstractVector{T}, v2::AbstractVector{T}) where T
    diff_index = findall(v1 .!= v2)
    mp = countmap(zip(v1[diff_index], v2[diff_index]))
    mp_sort = sort(collect(mp), by=x->x.second, rev=true)
    return mp_sort
end

"""
CUDA helper to get access to SM number and maximum threads per SM.
"""
#= Unused CUDA helper kept as reference for device introspection.
function GPU_SM_threads_num()
    dev = CUDA.device()
    SM = attribute(dev, CUDA.DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT)
    max_threads = attribute(dev, CUDA.DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK)
    return SM, max_threads
end
=#



#------------------------------
# some other helper functions 
#------------------------------
"""
    locate_sym(syms, target_sym) -> Int

Locate a symbol in a list of Symbolics variables.

# Arguments
- `syms`: Vector of Symbolics variables.
- `target_sym`: Symbol, string, or integer.

# Returns
- Index of the symbol in `syms`.
"""
function locate_sym(syms, target_sym)
    target_sym = Symbol(target_sym)
    return findfirst(x -> x.val.name == target_sym, syms)
end
"""
    locate_sym(syms, target_sym::Integer) -> Integer

Return the provided index directly for convenience.
"""
locate_sym(syms, target_sym::Integer) = target_sym
locate_sym_x(model::Bnc,target_sym) = locate_sym(x_sym(model), target_sym)
locate_sym_qK(model::Bnc,target_sym) = locate_sym(qK_sym(model), target_sym)


#--------------------------------------------
#heler functions to handle strings
#---------------------------------------------
"""
    strip_before_bracket(s::AbstractString) -> String

Remove everything before the first `[` character, including the bracket.
"""
strip_before_bracket(s::AbstractString) =    replace(s, r"^[^\[]*" => "")


#------------------------------------------------------------
# Helper function for construcing graphs
#----------------------------------------------------------
"""
    graph_from_paths(paths; nv=nothing) -> SimpleDiGraph

Construct a directed graph from a collection of vertex paths.

# Arguments
- `paths`: Vector of vertex index paths.
- `nv`: Optional number of vertices.

# Returns
- Directed graph with edges following the paths.
"""
function graph_from_paths(paths::AbstractVector{<:AbstractVector{<:Integer}}, nv=nothing)::SimpleDiGraph
    nv = nv === nothing ? maximum(Iterators.flatten(paths)) : nv

    grh = SimpleDiGraph(nv)
    for p in paths
        n = length(p)
        for i in 1:(n-1)
            add_edge!(grh, p[i], p[i+1])
        end
    end
    return grh
end

"""
    sources_sinks_from_paths(paths) -> (Vector{Int}, Vector{Int})

Return unique source and sink vertices for a collection of paths.

# Arguments
- `paths`: Vector of vertex index paths.

# Returns
- Tuple `(sources, sinks)`.
"""
function sources_sinks_from_paths(paths::AbstractVector{<:AbstractVector{<:Integer}})::Tuple{Vector{Int}, Vector{Int}}
    sources = unique(p -> p[1], paths)
    sinks = unique(p -> p[end], paths)
    return sources, sinks
end

"""
    norm_vec_space(x::AbstractVector{<:Real}) -> Vector{Float64}

Normalize a vector to have unit length in Euclidean space.
"""
function norm_vec_space(x::AbstractVector{<:Real})::Vector{Float64}
    n = length(x)
    num_to_norm = median!(filter!(>(1e-9), abs.(x)))
    return x./num_to_norm
end

"""
    render_array(M, empty_posi_subs=nothing) -> String

Render an array as a formatted string, optionally replacing specified entries.

# Arguments
- `M`: Array to render.
- `empty_posi_subs`: Optional subscripts to mark as empty.

# Returns
- Pretty-printed string.
"""
function render_array(M::AbstractArray,empty_posi_subs=nothing)
    A = Array{Any}(M)
    f(x) = begin
            a = try 
                    Int(round(x;digits=3))
                catch
                    round(x;digits=5)
                end
            a == 0 ? empty_posi_subs : a
        end
    A = f.(A)
    return latexify(A)
end



#--------------------------------------------------------
# Helper functions to handle adjency matrix compress
#---------------------------------------------------------

function compress_adjacency(
    A::SparseMatrixCSC,
    keep::AbstractVector{<:Integer};
    drop_stored_zeros::Bool = true,
)
    n = size(A, 1)
    size(A, 2) == n || throw(ArgumentError("A must be square"))

    A2 = drop_stored_zeros ? dropzeros(A) : A

    keep_set = Set(keep)
    length(keep_set) == length(keep) || throw(ArgumentError("keep contains duplicates"))
    all(1 <= v <= n for v in keep) || throw(ArgumentError("keep contains out-of-range indices"))

    m = length(keep)

    # 原图编号 -> 压缩后编号；非 keep 节点记 0
    keep_pos = zeros(Int, n)
    for (i, v) in enumerate(keep)
        keep_pos[v] = i
    end

    iskeep = falses(n)
    isdrop = trues(n)
    for v in keep
        iskeep[v] = true
        isdrop[v] = false
    end

    rows = rowvals(A2)

    I = Int[]
    J = Int[]

    # 1) 保留 keep-keep 原始边
    @inbounds for j in keep
        jj = keep_pos[j]
        for p in nzrange(A2, j)
            i = rows[p]
            if i != j && iskeep[i]
                ii = keep_pos[i]
                push!(I, ii)
                push!(J, jj)
            end
        end
    end

    # 2) 在 drop 子图中找连通块；每个连通块接触到的 keep 点两两补边
    visited = falses(n)
    stack = Int[]

    touched = Int[]
    touched_mark = zeros(Int, m)
    stamp = 0

    @inbounds for s in 1:n
        if !isdrop[s] || visited[s]
            continue
        end

        empty!(stack)
        push!(stack, s)
        visited[s] = true

        empty!(touched)
        stamp += 1

        while !isempty(stack)
            u = pop!(stack)

            for p in nzrange(A2, u)
                v = rows[p]
                v == u && continue

                if isdrop[v]
                    if !visited[v]
                        visited[v] = true
                        push!(stack, v)
                    end
                else
                    kv = keep_pos[v]
                    if kv != 0 && touched_mark[kv] != stamp
                        touched_mark[kv] = stamp
                        push!(touched, kv)
                    end
                end
            end
        end

        t = length(touched)
        for a in 1:t-1
            ia = touched[a]
            for b in a+1:t
                ib = touched[b]
                push!(I, ia); push!(J, ib)
                push!(I, ib); push!(J, ia)
            end
        end
    end

    # 用逻辑或合并重复边，得到 Bool 稀疏邻接矩阵
    B = sparse(I, J, fill(true, length(I)), m, m, |)

    # 去掉自环
    if nnz(B) > 0
        B = B - spdiagm(0 => diag(B))
        dropzeros!(B)
    end

    return B
end



function connected_components_sparse(A::SparseMatrixCSC)
    n = size(A, 1)
    size(A, 2) == n || throw(ArgumentError("A must be square"))

    rows = rowvals(A)
    visited = falses(n)
    labels = zeros(Int, n)
    groups = Vector{Vector{Int}}()
    cid = 0

    for s in 1:n
        visited[s] && continue

        cid += 1
        labels[s] = cid
        stack = [s]
        visited[s] = true
        comp = Int[]

        while !isempty(stack)
            u = pop!(stack)
            push!(comp, u)

            # 遍历第 u 列所有非零行号
            for p in nzrange(A, u)
                v = rows[p]
                if v != u && !visited[v]
                    visited[v] = true
                    labels[v] = cid
                    push!(stack, v)
                end
            end
        end

        push!(groups, comp)
    end

    return Set.(groups), labels
end


#============================================================#
# Helper functions to calculate a matrix's left nullspace
#============================================================#


# Row-reduced echelon form over an arbitrary exact scalar type,
# e.g. Rational{Int}. Returns:
#   M          : RREF(A)
#   pivotcols  : pivot columns of A
function rref_exact(A::AbstractMatrix{T}) where {T<:Number}
    M = copy(A)
    m, n = size(M)
    pivotcols = Int[]
    row = 1

    @inbounds for col in 1:n
        row > m && break

        # find pivot row
        pivot = 0
        for r in row:m
            if M[r, col] != 0
                pivot = r
                break
            end
        end
        pivot == 0 && continue

        # swap rows manually (avoids slice allocations)
        if pivot != row
            for j in 1:n
                M[row, j], M[pivot, j] = M[pivot, j], M[row, j]
            end
        end

        # normalize pivot row
        piv = M[row, col]
        if piv != one(T)
            for j in col:n
                M[row, j] /= piv
            end
        end

        # eliminate other rows
        for r in 1:m
            r == row && continue
            c = M[r, col]
            c == 0 && continue
            for j in col:n
                M[r, j] -= c * M[row, j]
            end
        end

        push!(pivotcols, col)
        row += 1
    end

    return M, pivotcols
end


# Convert a rational vector to a primitive integer vector.
# Optionally flips sign so the first nonzero entry is positive.
function primitive_integer(v::AbstractVector{<:Rational})
    dens = denominator.(v)
    L = foldl(lcm, dens; init=1)

    w = Int.(L .* v)

    g = foldl(gcd, abs.(w); init=0)
    g = g == 0 ? 1 : g
    w = div.(w, g)

    # canonical sign choice: first nonzero entry positive
    for x in w
        if x != 0
            if x < 0
                w = .-w
            end
            break
        end
    end

    return w
end


# Left nullspace basis of S:
# finds B such that B' * S = 0
# Returns:
#   B          : columns are primitive integer basis vectors for left nullspace
#   pivotrows  : pivot row indices of S
function left_nullspace_integer(S::AbstractMatrix{Int})
    A = Rational{Int}.(transpose(S))   # nullspace of S'
    M, pivotrows = rref_exact(A)

    m, n = size(M)                     # n = number of rows of S
    ispivot = falses(n)
    for c in pivotrows
        ispivot[c] = true
    end
    freecols = findall(!, ispivot)

    B = Matrix{Int}(undef, n, length(freecols))

    @inbounds for (j, fc) in enumerate(freecols)
        x = zeros(Rational{Int}, n)
        x[fc] = 1
        for (i, pc) in enumerate(pivotrows)
            x[pc] = -M[i, fc]
        end
        B[:, j] = primitive_integer(x)
    end

    return B, pivotrows
end



function S_to_S_pos_neg(S::SparseMatrixCSC{T,Ti}) where {T<:Real,Ti<:Integer}
    m, n = size(S)
    nnzS = nnz(S)

    colptr = Vector{Ti}(undef, n + 1)
    rowval = Vector{Ti}(undef, nnzS)
    nzval  = Vector{T}(undef, nnzS)

    pos = 1
    colptr[1] = 1

    @inbounds for j in 1:n
        for p in S.colptr[j]:(S.colptr[j+1] - 1)
            i = S.rowval[p]
            v = S.nzval[p]

            if v > zero(T)
                rowval[pos] = i
                nzval[pos]  = v
                pos += 1
            elseif v < zero(T)
                rowval[pos] = i + m
                nzval[pos]  = -v
                pos += 1
            end
            # skip exact zeros if any are stored
        end
        colptr[j+1] = pos
    end

    resize!(rowval, pos - 1)
    resize!(nzval,  pos - 1)

    return SparseMatrixCSC(2m, n, colptr, rowval, nzval)
end



# function get_max_denom(M::AbstractMatrix{<:Integer})
#     F = snf(M)
#     return F[2][end,end]
# end



function same_polyhedron(P, Q)
    HP = hrep(P)
    HQ = hrep(Q)
    all(h -> issubset(P, h), allhalfspaces(HQ)) &&
    all(h -> issubset(Q, h), allhalfspaces(HP))
end


"""
    group_sum(keys, vals; sort_values=true) -> Vector{Tuple}

Group values by keys, returning indices, key, and summed values.
"""
function group_sum(keys::AbstractVector{I}, vals::AbstractVector{J}; 
    sort_values::Bool=true
    ) :: Vector{Tuple{Vector{Int}, I, J}} where {I,J}

    @assert length(keys) == length(vals)
    # Dictionary to accumulate sum of values for each key
    dict = Dict{I,J}()
    # Store indices of keys for later reference
    index_dict = Dict{I, Vector{Int}}()
    
    @inbounds for (i, (k, v)) in enumerate(zip(keys, vals))
        dict[k] = get(dict, k, zero(v)) + v
        push!(get!(index_dict, k, Int[]), i)  # Store the index
    end
    
    # Collect and sort if needed
    dict_vec = collect(dict)
    
    if sort_values
        # Sort by values (sum of vals)
        sort!(dict_vec, by=x->x[2], rev=true)
    end
    
    # Create a Vector of Tuples with (index, key, summed value)
    result = Vector{Tuple{Vector{Int}, I, J}}(undef, length(dict))
    
    # @show dict, index_dict
    for i in eachindex(dict_vec)
        key, sum_val = dict_vec[i]
        group = index_dict[key]
        result[i] = (group, key, sum_val)
    end
    
    return result
end

function group_sum(
    keys::AbstractVector{I},
    vals::AbstractVector{Nothing};
    sort_values::Bool=true,
)::Vector{Tuple{Vector{Int}, I, Nothing}} where {I}

    @assert length(keys) == length(vals)

    index_dict = Dict{I, Vector{Int}}()
    order = I[]

    @inbounds for (i, k) in enumerate(keys)
        if !haskey(index_dict, k)
            push!(order, k)
        end
        push!(get!(index_dict, k, Int[]), i)
    end

    if sort_values
        sort!(order, by = k -> length(index_dict[k]), rev = true)
    end

    return [(index_dict[k], k, nothing) for k in order]
end

