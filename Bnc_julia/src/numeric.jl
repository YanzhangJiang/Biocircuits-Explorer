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
- `q_mat`: Optional matrix whose first `bnc.d` columns are the conserved totals.
  When omitted, the totals are reconstructed from each row of `x_mat`.

# Keyword Arguments
- `x_idx`: Indices of species to include.
- `qK_idx`: Indices of `qK` to include.
- `only_q`: Restrict to totals `q` when `true`.

# Returns
- 3D array of sensitivities with shape `(time, x_idx, qK_idx)`.
"""
function get_reaction_order(Bnc::Bnc, x_mat::AbstractMatrix{<:Real}, q_mat::Union{AbstractMatrix{<:Real},Nothing}=nothing;
    x_idx::Union{AbstractVector{<:Integer},Nothing}=nothing,
    qK_idx::Union{AbstractVector{<:Integer},Nothing}=nothing,
    only_q::Bool=false,
)::Array{Float64,3}
    size(x_mat, 2) == Bnc.n || throw(DimensionMismatch(
        "x_mat must have $(Bnc.n) species columns; got $(size(x_mat, 2))"))
    if !isnothing(q_mat)
        size(q_mat, 1) == size(x_mat, 1) || throw(DimensionMismatch(
            "q_mat and x_mat must have the same number of rows"))
        size(q_mat, 2) >= Bnc.d || throw(DimensionMismatch(
            "q_mat must have at least $(Bnc.d) conserved-total columns"))
    end

    x_idx = isnothing(x_idx) ? collect(1:Bnc.n) : Int.(x_idx)
    qK_idx = isnothing(qK_idx) ? collect(1:Bnc.n) : Int.(qK_idx)
    if only_q
        qK_idx = filter(<=(Bnc.d), qK_idx)
    end
    all(i -> 1 <= i <= Bnc.n, x_idx) || throw(BoundsError(1:Bnc.n, x_idx))
    all(i -> 1 <= i <= Bnc.n, qK_idx) || throw(BoundsError(1:Bnc.n, qK_idx))

    reaction_orders = Array{Float64,3}(
        undef, size(x_mat, 1), length(x_idx), length(qK_idx))
    for i in axes(x_mat, 1)
        x = @view x_mat[i, :]
        jacobian = if isnothing(q_mat)
            ∂logx_∂logqK(Bnc; x=x)
        else
            ∂logx_∂logqK(Bnc; x=x, qK=@view(q_mat[i, :]))
        end
        @views reaction_orders[i, :, :] .= jacobian[x_idx, qK_idx]
    end
    return reaction_orders
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



















