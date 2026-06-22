export logder_x_qK, logder_qK_x, ∂logx_∂logqK, ∂logqK_∂logx

#----------------Functions for calculates the derivative of log(x) with respect to log(qK) and vice versa----------------------

"""
    ∂logqK_∂logx(bnc::Bnc; x=nothing, qK=nothing, q=nothing) -> Matrix

Compute the Jacobian of `log(q,K)` with respect to `log(x)` at a given point.

# Keyword Arguments
- `x`: Species concentrations in linear space.
- `qK`: Totals/binding constants in linear space.

# Returns
- Jacobian matrix of `logqK` with respect to `logx`.
"""
function ∂logqK_∂logx(Bnc::Bnc;
    x::Union{AbstractVector{<:Real},Nothing}=nothing,
    qK::Union{AbstractVector{<:Real},Nothing}=nothing,
    input_logspace::Bool=false)::Matrix{Float64}

    x = if isnothing(x)
            if isnothing(qK)
                error("Either x or qK must be provided")
            else
                qK2x(Bnc, qK; input_logspace=input_logspace, output_logspace=false) # Derive x from qK
            end
        elseif input_logspace
            exp10.(x) # Convert from log space to linear space
        else
            x
        end

    q = if isnothing(qK)
            Bnc.L * x
        elseif input_logspace
            exp10.(qK[1:Bnc.d])
        else
            qK[1:Bnc.d]
        end

    return vcat(
        x' .* Matrix{Float64}(Bnc.L) ./ q,
        Matrix{Float64}(Bnc.N)
    )
end
"""
    ∂logx_∂logqK(bnc::Bnc; x=nothing, qK=nothing, q=nothing) -> Matrix

Compute the Jacobian of `log(x)` with respect to `log(q,K)`.
"""
∂logx_∂logqK(args...;kwargs...) = inv(∂logqK_∂logx(args...;kwargs...))

"""
    logder_x_qK(args...; kwargs...) -> Matrix

Alias for `∂logx_∂logqK`.
"""
logder_x_qK(args...;kwargs...) = ∂logx_∂logqK(args...;kwargs...)
"""
    logder_qK_x(args...; kwargs...) -> Matrix

Alias for `∂logqK_∂logx`.
"""
logder_qK_x(args...;kwargs...) = ∂logqK_∂logx(args...;kwargs...)

# ---------------------------------------------------------------Get regime data from resulting matrix---------------------------------------

"""
    get_reaction_order(bnc::Bnc, x_mat, q_mat=nothing; x_idx=nothing, qK_idx=nothing, only_q=false) -> Array{Float64,3}

Compute reaction-order-like sensitivities over a trajectory.

# Arguments
- `bnc`: Binding network model.
- `x_mat`: Matrix of species concentrations (rows = time points).
- `q_mat`: Optional matrix of totals/binding constants.

# Keyword Arguments
- `x_idx`: Indices of species to include.
- `qK_idx`: Indices of `qK` to include.
- `only_q`: Restrict to totals `q` when `true`.

# Returns
- 3D array of sensitivities with shape `(time, x_idx, qK_idx)`.
"""
function get_reaction_order(Bnc::Bnc, x_mat::Matrix{<:Real}, q_mat::Union{Matrix{<:Real},Nothing}=nothing;
    x_idx::Union{Vector{Int},Nothing}=nothing,
    qK_idx::Union{Vector{Int},Nothing}=nothing,
    only_q::Bool=false,
)::Array{Float64,3}
    # Get the reaction order from the resulting matrix, where a regime is calculated for each row by formula.
    # x_mat: Matrix of x values, each row is a different time point
    # q_mat: Matrix of qK values, each row is a different time point
    # x_idx: Indices of x to be calculated, default is all indices
    # qK_idx: Indices of qK to be calculated, default is all indices
    q_mat = isnothing(q_mat) ? x2qK(Bnc, x_mat'; input_logspace=false, output_logspace=false, only_q=true)' : q_mat
    x_idx = isnothing(x_idx) ? (1:Bnc.n) : x_idx
    qK_idx = isnothing(qK_idx) ? (1:Bnc.n) : qK_idx
    if only_q
        qK_idx = qK_idx[findall(i->i<=Bnc.d, qK_idx)] # only keep the indices for q
    end

    flag = length(qK_idx) <= length(x_idx)

    A = sparse(_idx_val2Mtx(collect(x_idx),1,Bnc.n)) # x*n
    B = sparse(_idx_val2Mtx(collect(qK_idx),1,Bnc.n)) # q * n

    regimes = Array{Float64,3}(undef,size(x_mat, 1),length(x_idx), length(qK_idx)) # initialize the regimes array to storage. t have to be the last value to make storage continuous
    tmp_regime = flag ? Matrix{Float64}(undef, Bnc.n, length(qK_idx)) : Matrix{Float64}(undef, Bnc.n, length(x_idx))
    # temporary matrix to store the result of regime
    # Get the regimes from the resulting matrix,where a regime is calculated for each row.
    
    Jt = copy(Bnc._LNt_sparse)
    Jt_lu = copy(Bnc._LNt_lu)

    Jt_left = @view(Jt.nzval[1:length(Bnc.L.nzval)])
    
    function _update_Jt!(Jt_left, x::AbstractArray{<:Real}, q::AbstractArray{<:Real})
        x_view = @view(x[Bnc._I])
        @. Jt_left = x_view * Bnc._Lt_sparse.nzval ./ q[Bnc._J]
        return nothing
    end

    if flag # qK_idx is shorter
        for (i, (x, q)) in enumerate(zip(eachrow(x_mat), eachrow(q_mat)))
            _update_Jt!(Jt_left, x, q)
            lu!(Jt_lu, Jt) # recalculate the LU decomposition of Jt
            ldiv!(tmp_regime, Jt_lu', B')
            regimes[i,:,:] .= A * tmp_regime
        end
    else
        for (i, (x, q)) in enumerate(zip(eachrow(x_mat), eachrow(q_mat)))
            _update_Jt!(Jt_left, x, q)
            lu!(Jt_lu, Jt) # recalculate the LU decomposition of Jt
            ldiv!(tmp_regime, Jt_lu, A')
            regimes[i,:,:] .= (B * tmp_regime)'
        end
    end
    return regimes
end














#-----------------------------------------------------------------
# Function of calculating volume of vertices
#-----------------------------------------------------------------
# function calc_volume(C::AbstractMatrix{<:Real}, C0::AbstractVector{<:Real}; 
#     confidence_level::Float64=0.95,
#     N=1_000_000,
#     batch_size::Int=100_000,
#     log_lower=-6,
#     log_upper=6
# )::Tuple{Float64,Float64}
#     N = Int(N)

#     n = size(C, 2)
#     dist = Uniform(log_lower, log_upper)

#     n_batches = cld(N, batch_size)  # 向上取整批次数
#     counts = zeros(Int, n_batches)  # 每批结果

#     Threads.@threads for b in 1:n_batches
#         m = (b == n_batches) ? (N - (n_batches-1)*batch_size) : batch_size
#         samples = rand(dist, n, m)
#         vals = C * samples .+ C0

#         local_count = 0
#         @inbounds for j in 1:m
#             if all(@view(vals[:, j]) .> 0)
#                 local_count += 1
#             end
#         end
#         counts[b] = local_count
#     end

#     count = sum(counts)
#     P_hat = count / N
#     z = quantile(Normal(), (1 + confidence_level) / 2)

#     # Wilson 置信区间
#     denom = 1 + z^2 / N
#     center = (P_hat + z^2/(2N)) / denom
#     margin = (z / denom) * sqrt(P_hat*(1-P_hat)/N + z^2/(4N^2))

#     return (center, margin)
# end

# The core calculate function:
# function calc_volume(Cs::AbstractVector{<:AbstractMatrix{<:Real}}, C0s::AbstractVector{<:AbstractVector{<:Real}};
#     confidence_level::Float64=0.95,
#     N::Int=1_000_000,
#     batch_size::Int=100_000,
#     log_lower=-6,
#     log_upper=6,
#     tol::Float64=1e-10,
# )::Vector{Tuple{Float64,Float64}}

#     n_batches = cld(N, batch_size)
#     dist = Uniform(log_lower, log_upper)
#     n_threads = Threads.nthreads()

#     n = size(Cs[1], 2)

#     # --- 每线程局部计数 ---
#     thread_counts = [zeros(Int, length(Cs)) for _ in 1:n_threads]

#     Threads.@threads for b in 1:n_batches
#         m = (b == n_batches) ? (N - (n_batches-1)*batch_size) : batch_size
#         samples = rand(dist, n, m)
#         local_counts = thread_counts[Threads.threadid()]

#         @inbounds for j in 1:m
#             @views x = samples[:, j]
#             for i in eachindex(Cs)
#                 @views A = Cs[i]
#                 @views b = C0s[i]
#                 if all(A * x .+ b .> - tol)
#                     local_counts[i] += 1
#                 end
#             end
#         end
#     end

#     # --- 汇总 ---
#     total_counts = zeros(Int, length(Cs))
    
#     for c in thread_counts
#         @inbounds total_counts .+= c
#     end
#     # --- Wilson 区间 ---
#     function get_center_margin(count::Int, N::Int)
#         if count == 0
#             return (0.0, 0.0)
#         end
#         P_hat = count / N
#         z = quantile(Normal(), (1 + confidence_level) / 2)
#         denom = 1 + z^2 / N
#         center = (P_hat + z^2/(2*N)) / denom
#         margin = (z / denom) * sqrt(P_hat*(1 - P_hat)/N + z^2/(4*N^2))
#         return center, margin
#     end

#     @show total_counts
#     return [get_center_margin(c, N) for c in total_counts]
# end






# for now as the perm is not defined , we shall 




















