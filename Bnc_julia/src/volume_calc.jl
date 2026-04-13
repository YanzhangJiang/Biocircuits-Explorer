"""
    calc_volume(Cs, C0s; kwargs...) -> Vector{Volume}

Monte Carlo 估计：默认使用 N 维高斯抽样 `x ~ 𝒩(μ, σ²I)` 估计各 polyhedron 的概率质量；
若 `sampler=:uniform_box` 则估计盒上均匀抽样下的体积（概率×盒体积）。

polyhedron 约束：A*x + b >= -tol
"""
function calc_volume(
    Cs::AbstractVector{<:AbstractMatrix{<:Real}},
    C0s::AbstractVector{<:AbstractVector{<:Real}};
    # --- sampling ---
    sampler::Symbol = :gaussian,               # :gaussian (default) or :uniform_box
    μ::Union{Nothing,AbstractVector{<:Real}} = nothing,
    σ::Float64 = 1.0,                          # for gaussian: std (isotropic)
    log_lower::Float64 = -6.0,                 # for uniform_box
    log_upper::Float64 = 6.0,                  # for uniform_box

    # --- estimation control ---
    confidence_level::Float64 = 0.95,
    contain_overlap::Bool = false,
    regime_judge_tol::Float64 = 0.0,
    batch_size::Int = 100_000,
    abs_tol::Float64 = 1.0e-8,
    rel_tol::Float64 = 0.005,
    time_limit::Float64 = 120.0,

    # --- perf/UX ---
    show_progress::Bool = false,

    # --- rebase---
    rebase_mat:: Union{AbstractMatrix{<:Real},Nothing} = nothing
)::Vector{Volume}

    @assert length(Cs) == length(C0s) "Cs and C0s must have same length"
    n_regimes = length(Cs)
    @info "Number of polyhedra to calc volume: $n_regimes"
    n_regimes == 0 && return Volume[]

    # Dimensions & sanity
    n_dim = size(Cs[1], 2)
    for i in 1:n_regimes
        @assert size(Cs[i], 2) == n_dim "All Cs must have same column dimension"
        @assert size(Cs[i], 1) == length(C0s[i]) "size(Cs[$i],1) must match length(C0s[$i])"
    end

    Cs = if isnothing(rebase_mat)
           Cs
        else
            [ Cs[i] * rebase_mat for i in 1:n_regimes ]
        end
    # z for Wilson interval
    z = quantile(Normal(), (1 + confidence_level) / 2)

    @inline function wilson_center_margin(count::Int, N::Int)
        # N must be > 0 when called
        p̂ = count / N
        z2 = z*z
        denom = 1 + z2 / N
        center = (p̂ + z2 / (2N)) / denom
        margin = (z / denom) * sqrt(p̂ * (1 - p̂) / N + z2 / (4N*N))
        return center, margin
    end

    # Convert b to Float64 once
    b64 = Vector{Vector{Float64}}(undef, n_regimes)
    for i in 1:n_regimes
        bi = C0s[i]
        b64[i] = (bi isa Vector{Float64}) ? bi : Float64.(bi)
    end

    # Sampling params
    μ64 = Vector{Float64}(undef, n_dim)
    if sampler === :gaussian
        if μ === nothing
            fill!(μ64, 0.0)
        else
            @assert length(μ) == n_dim "length(μ) must equal n_dim"
            @inbounds for k in 1:n_dim
                μ64[k] = Float64(μ[k])
            end
        end
        @assert σ > 0 "σ must be > 0"
    elseif sampler === :uniform_box
        @assert log_upper > log_lower "log_upper must be > log_lower"
    else
        error("sampler must be :gaussian or :uniform_box, got $sampler")
    end

    regime_judge_tol = abs(regime_judge_tol)

    # Global stats
    total_counts = zeros(Int, n_regimes)
    total_N = 0
    stats = [Volume(0.0, 0.0) for _ in 1:n_regimes]
    active_ids = collect(1:n_regimes)

    # Thread-local storage
    n_slots = Threads.maxthreadid()
    thread_counts = [zeros(Int, n_regimes) for _ in 1:n_slots]
    thread_rng = [Random.MersenneTwister(0x12345678 + tid) for tid in 1:n_slots]
    thread_x = [Vector{Float64}(undef, n_dim) for _ in 1:n_slots]
    thread_y = [
        [Vector{Float64}(undef, size(Cs[i], 1)) for i in 1:n_regimes]
        for _ in 1:n_slots
    ]

    # Scaling: uniform_box -> volume fraction in the box; gaussian -> probability mass (scale=1)
    box_width = log_upper - log_lower

    # optional progress (keep minimal overhead when off)
    p = show_progress ? Progress(n_regimes, desc="Calculating...", dt=1.0) : nothing

    start_time = time()

    while true
        (time() - start_time > time_limit) && (@info "Reached time limit ($(round(time() - start_time, digits=2)) s). Stopping."; break)
        isempty(active_ids) && (@info "All regimes converged after $total_N samples."; break)

        # snapshot active_ids for threaded read-only access
        active_snapshot = active_ids

        Threads.@threads for _ in 1:batch_size
            tid = Threads.threadid()
            rng = thread_rng[tid]
            x = thread_x[tid]
            local_counts = thread_counts[tid]
            ywork = thread_y[tid]

            # draw x
            if sampler === :gaussian
                @inbounds @simd for k in 1:n_dim
                    x[k] = μ64[k] + σ * randn(rng)
                end
            else
                @inbounds @simd for k in 1:n_dim
                    x[k] = log_lower + box_width * rand(rng)
                end
            end

            # test regimes
            @inbounds for idx in active_snapshot
                A = Cs[idx]
                b = b64[idx]
                y = ywork[idx]

                mul!(y, A, x)

                ok = true
                @inbounds for k in 1:length(y)
                    if y[k] + b[k] < -regime_judge_tol
                        ok = false
                        break
                    end
                end
                ok || continue

                local_counts[idx] += 1
                contain_overlap || break
            end
        end

        # reduce counts
        @inbounds for c in thread_counts
            for idx in active_ids
                total_counts[idx] += c[idx]
                c[idx] = 0
            end
        end
        total_N += batch_size

        # update CI & prune
        new_active = Int[]
        sizehint!(new_active, length(active_ids))

        @inbounds for idx in active_ids
            center, margin = wilson_center_margin(total_counts[idx], total_N)
            v_center = center 
            v_margin = margin 
            stats[idx] = Volume(v_center, v_margin^2)

            re = (v_center == 0.0) ? Inf : (v_margin / v_center)
            if re > rel_tol && v_margin > abs_tol
                push!(new_active, idx)
            end
        end

        if show_progress
            next!(p, step = length(active_ids) - length(new_active))
        end
        active_ids = new_active
    end

    show_progress && finish!(p)
    return stats
end




# """
#     calc_volume(Cs, C0s; kwargs...) -> Vector{Volume}

# Monte Carlo estimate for polyhedron volumes defined by `A*x + b >= -tol`.
# """
# function calc_volume(
#     Cs::AbstractVector{<:AbstractMatrix{<:Real}},
#     C0s::AbstractVector{<:AbstractVector{<:Real}};
#     confidence_level::Float64 = 0.95,
#     contain_overlap::Bool = false,
#     regime_judge_tol::Float64 = 0.0,
#     batch_size::Int = 100_000,
#     log_lower::Float64 = -6.0,
#     log_upper::Float64 = 6.0,
#     abs_tol::Float64 = 1.0e-8,
#     rel_tol::Float64 = 0.005,
#     time_limit::Float64 = 120.0,
# )::Vector{Volume}


#     @assert length(Cs) == length(C0s) "Cs and C0s must have same length"
#     n_regimes = length(Cs)
#     @info "Number of polyhedra to calc volume: $n_regimes"
#     n_regimes == 0 && return Volume[]

#     # Dimensions & sanity
#     n_dim = size(Cs[1], 2)
#     for i in 1:n_regimes
#         @assert size(Cs[i], 2) == n_dim "All Cs must have same column dimension"
#         @assert size(Cs[i], 1) == length(C0s[i]) "size(Cs[$i],1) must match length(C0s[$i])"
#     end

#     # Wilson interval parameter
#     z = quantile(Normal(), (1 + confidence_level) / 2)

#     @inline function wilson_center_margin(count::Int, N::Int)
#         count == 0 && return 0.0, 0.0
#         P̂ = count / N
#         denom = 1 + z^2 / N
#         center = (P̂ + z^2 / (2N)) / denom
#         margin = (z / denom) * sqrt(P̂ * (1 - P̂) / N + z^2 / (4N^2))
#         return center, margin
#     end

#     # Global stats
#     total_counts = zeros(Int, n_regimes)
#     total_N = 0
    
#     stats = [Volume(0.0, 0.0) for _ in 1:n_regimes]

#     active_ids = collect(1:n_regimes)

#     # Thread-local slot count (important: maxthreadid, not nthreads)
#     n_slots = Threads.maxthreadid()
#     thread_counts = [zeros(Int, n_regimes) for _ in 1:n_slots]

#     # Thread-local RNG + x workspace
#     # Use a stable seed per thread to avoid contention and keep reproducibility-ish.
#     thread_rng = [Random.MersenneTwister(0x12345678 + tid) for tid in 1:n_slots]
#     thread_x = [Vector{Float64}(undef, n_dim) for _ in 1:n_slots]

#     # Thread-local y workspaces: one vector per regime (length = m_i)
#     # Use Float64 for speed; if your b is Float64 this is perfect.
#     thread_y = [
#         [Vector{Float64}(undef, size(Cs[i], 1)) for i in 1:n_regimes]
#         for _ in 1:n_slots
#     ]

#     # Pre-grab b as Float64 vectors if possible (avoids repeated Real->Float64 conversions)
#     # If b is already Vector{Float64}, this is just a cheap reference.
#     b64 = Vector{Vector{Float64}}(undef, n_regimes)
#     for i in 1:n_regimes
#         bi = C0s[i]
#         if bi isa Vector{Float64}
#             b64[i] = bi
#         else
#             b64[i] = Float64.(bi)
#         end
#     end

#     start_time = time()
#     width = log_upper - log_lower

#     p = Progress(n_regimes, desc="Calculating volumes...", dt=1.0)

#     regime_judge_tol = abs(regime_judge_tol) # ensure non-negative

#     while true
#         (time() - start_time > time_limit) && (@info "Reached time limit ($(round(time() - start_time, digits=2)) s). Stopping.";break)
#         isempty(active_ids) && (@info "All regimes converged after $total_N samples.";break)

#         # Monte Carlo batch
#         Threads.@threads for _ in 1:batch_size
#             tid = Threads.threadid()
#             rng = thread_rng[tid]
#             x = thread_x[tid]
#             local_counts = thread_counts[tid]
#             ywork = thread_y[tid]

#             # x ~ Uniform(log_lower, log_upper)^n_dim
#             @inbounds @simd for k in 1:n_dim
#                 x[k] = log_lower + width * rand(rng)
#             end

#             # test regimes
#             for idx in active_ids
#                 A = Cs[idx]
#                 b = b64[idx]
#                 y = ywork[idx]

#                 # y = A*x  (sparse gemv)
#                 mul!(y, A, x)

#                 # check y + b >= -tol  (fuse add+check)
#                 ok = true
#                 @inbounds for k in 1:length(y)
#                     if y[k] + b[k] < -regime_judge_tol
#                         ok = false
#                         break
#                     end
#                 end
#                 ok || continue

#                 local_counts[idx] += 1
#                 contain_overlap || break
#             end
#         end

#         # Reduce and reset counts; update N
#         for c in thread_counts
#             @inbounds for i in active_ids
#                 total_counts[i] += c[i]
#                 c[i] = 0
#             end
#         end
#         total_N += batch_size

#         # Update CI and prune active_ids
#         new_active = Int[]
#         sizehint!(new_active, length(active_ids))
#         for i in active_ids
#             center, margin = wilson_center_margin(total_counts[i], total_N)
#             stats[i] = Volume(center, margin^2)
#             re = (center == 0.0) ? Inf : (margin / center)
#             if re > rel_tol && margin > abs_tol
#                 push!(new_active, i)
#             end
#         end
#         next!(p, step = length(active_ids) - length(new_active))
#         active_ids = new_active
#     end
#     finish!(p)
#     return stats
# end


"""
    calc_volume(C, C0; kwargs...) -> Volume

Compute volume for a single polyhedron.
"""
calc_volume(C::AbstractMatrix{<:Real}, C0::AbstractVector{<:Real}; kwargs...)::Tuple{Float64,Float64} = calc_volume([C], [C0]; kwargs...)[1]

# calc_vertex_volume(Bnc::Bnc, perm;kwargs...) = calc_vertices_volume(Bnc,[perm]; kwargs...)[1]



#-------------------------------------------------------------------------------------
# Volume calculation for polyhedras
#--------------------------------------------------------------------------------------

# filter and then calculate volumes for polyhedra
"""
    _remove_poly_intersect(poly::Polyhedron) -> Polyhedron

Remove intersection offsets to test asymptoticity in polyhedra.
"""
function _remove_poly_intersect(poly::Polyhedron)
    (A,b,linset) = MixedMatHRep(hrep(poly)) |> p->(p.A, p.b,p.linset)
    p_new = hrep(A, zeros(size(b)), linset) |> x-> polyhedron(x,CDDLib.Library())
    return p_new
end

"""
    _get_mask(polys; singular=nothing, asymptotic=nothing) -> Vector{Bool}

Return a boolean mask for polyhedra matching singularity/asymptotic filters.
"""
function _get_mask(polys::AbstractVector{<:Polyhedron};
     singular::Union{Bool,Integer,Nothing}=nothing, 
     asymptotic::Union{Bool,Nothing}=nothing)::Vector{Bool}
    # ensure nullity and asymptotic flags are calculated

    n = length(polys)

    full_dim = fulldim(polys[1])
    dims = dim.(polys)
    nlt = full_dim .- dims

    flag_asym =
        if isnothing(asymptotic)
            fill(false, n)               # 不使用 asym 标准
        else
            # only compute if needed
            polys_asym = _remove_poly_intersect.(polys)
            nlt_new = full_dim .- dim.(polys_asym)
            nlt_new .== nlt               # asym condition
        end

    check_singular(nlt) = isnothing(singular) || (
        (singular === true  && nlt > 0) ||
        (singular === false && nlt == 0) ||
        (singular isa Int   && nlt ≤ singular)
    )

    check_asym(flag_asym) = isnothing(asymptotic) || (asymptotic == flag_asym)
    
    return [ check_singular(nlt[i]) && check_asym(flag_asym[i]) for i in 1:n ]
end

"""
    filter_polys(polys; return_idx=false, kwargs...) -> Vector

Filter polyhedra by singularity/asymptotic criteria.
"""
function filter_polys(polys; return_idx::Bool=false, kwargs...)
    mask = _get_mask(polys; kwargs...)
    return return_idx ? findall(mask) : polys[mask]
end

#------------------------------------------------------------------------------------------------
# calculate volume for Bnc regimes,
#------------------------------------------------------------------------------------------------

"""
    calc_volume(rgms::Union{AbstractVector{<:Vertex}, AbstractVector{<:Polyhedron}}; asymptotic=true, kwargs...) -> Vector{Volume}

Compute volumes for a collection of polyhedra or vertices.

    calc_volume(model::Bnc, perms=nothing; asymptotic=true, kwargs...) -> Vector{Volume}

Compute volumes for selected regimes in a model.
"""
function calc_volume(rgms::Union{AbstractVector{<:Vertex}, AbstractVector{<:Polyhedron}};
    # model::Bnc, perms=nothing;
    asymptotic::Bool=true,
    kwargs...
) # singular/ asymptotic not be put here, as dimensions could reduce and change.

    n_all = length(rgms)

    idxs = _get_mask(rgms; 
        singular=false, 
        asymptotic= asymptotic ? true : nothing)

    C_C0s = rgms[idxs] .|> get_C_C0
    
    vals = [Volume(0.0, 0.0) for _ in 1:n_all]

    if isempty(C_C0s)
        return vals
    end    

    Cs = getindex.(C_C0s, 1)
    C0s = asymptotic ? [zeros(size(rep[2])) for rep in C_C0s] : getindex.(C_C0s, 2)
    
    
    vals[idxs] .= calc_volume(Cs, C0s; kwargs...)
    return vals
end

"""
    calc_volume(poly::Polyhedron; kwargs...) -> Volume

Compute the volume for a single polyhedron.
"""
calc_volume(poly::Polyhedron;kwargs...) = calc_volume([poly]; kwargs...)[1]
