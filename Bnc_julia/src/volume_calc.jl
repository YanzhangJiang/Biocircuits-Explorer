export calc_volume

const _VOLUME_DEFAULT_SEED = UInt64(0x12345678)
const _VOLUME_MIX_GAMMA = UInt64(0x9e3779b97f4a7c15)
const _VOLUME_MIX_FACTOR_1 = UInt64(0xbf58476d1ce4e5b9)
const _VOLUME_MIX_FACTOR_2 = UInt64(0x94d049bb133111eb)
const _VOLUME_SAMPLE_FACTOR = UInt64(0xd2b74407b1ce6e93)
const _VOLUME_LANE_FACTOR = UInt64(0xca5a826395121157)
const _VOLUME_INV_2_POW_52 = 1.0 / 4_503_599_627_370_496.0

"""
Return `true` only for the one estimator configuration stored by the legacy
single-value volume caches on regimes and paths.  Those caches do not carry a
configuration key, so an explicit estimator keyword or coordinate rebasing
must be computed ephemerally instead of reading or overwriting the canonical
default estimate.
"""
@inline function _volume_request_is_cacheable(
    rebase_K::Bool,
    rebase_mat,
    estimator_kwargs,
)::Bool
    return !rebase_K && isnothing(rebase_mat) && isempty(estimator_kwargs)
end

@inline function _volume_mix64(x::UInt64)::UInt64
    z = x + _VOLUME_MIX_GAMMA
    z = (z ⊻ (z >> 30)) * _VOLUME_MIX_FACTOR_1
    z = (z ⊻ (z >> 27)) * _VOLUME_MIX_FACTOR_2
    return z ⊻ (z >> 31)
end

function _normalize_volume_seed(seed::Integer)::UInt64
    (0 <= seed <= typemax(UInt64)) ||
        throw(ArgumentError("seed must be an integer in 0:typemax(UInt64)"))
    return UInt64(seed)
end

@inline function _volume_uniform01(
    seed::UInt64,
    sample_index::Int,
    lane::Int,
)::Float64
    counter = seed + UInt64(sample_index) * _VOLUME_SAMPLE_FACTOR +
        UInt64(lane) * _VOLUME_LANE_FACTOR
    bits = _volume_mix64(counter)
    # Use 52 payload bits so the half-unit offset remains exactly representable
    # across the whole range. The result is therefore strictly inside (0, 1),
    # including at the largest payload; Box-Muller can never see log(0).
    return (Float64(bits >> 12) + 0.5) * _VOLUME_INV_2_POW_52
end

@inline function _volume_wilson_center_margin(
    count::Int,
    sample_count::Int,
    z::Float64,
)
    sample_count > 0 || throw(ArgumentError("sample_count must be > 0"))
    0 <= count <= sample_count ||
        throw(ArgumentError("count must be in 0:sample_count"))
    isfinite(z) && z > 0 || throw(ArgumentError("z must be finite and > 0"))

    inv_n = inv(Float64(sample_count))
    p = Float64(count) * inv_n
    z2_inv_n = z * z * inv_n
    denom = 1.0 + z2_inv_n
    center = (p + 0.5 * z2_inv_n) / denom
    # This algebraically equivalent form never evaluates an integer N*N, so a
    # large valid sample population cannot overflow before conversion.
    variance_term = (p * (1.0 - p) + 0.25 * z2_inv_n) * inv_n
    margin = (z / denom) * sqrt(max(variance_term, 0.0))
    return center, margin
end

function _validate_volume_geometry(
    Cs::AbstractVector{<:AbstractMatrix{<:Real}},
    offsets::AbstractVector{<:AbstractVector{<:Real}},
)
    length(Cs) == length(offsets) ||
        throw(ArgumentError("Cs and C0s must have the same length"))
    isempty(Cs) && return 0

    n_dim = size(first(Cs), 2)
    for idx in eachindex(Cs)
        C = Cs[idx]
        offset = offsets[idx]
        size(C, 2) == n_dim ||
            throw(ArgumentError("all Cs must have the same column dimension"))
        size(C, 1) == length(offset) ||
            throw(ArgumentError("size(Cs[$idx], 1) must match length(C0s[$idx])"))
        all(isfinite, C) ||
            throw(ArgumentError("Cs[$idx] must contain only finite values"))
        all(isfinite, offset) ||
            throw(ArgumentError("C0s[$idx] must contain only finite values"))
    end
    return n_dim
end

@inline function _fill_volume_sample!(
    x::Vector{Float64},
    sampler::Symbol,
    μ::Vector{Float64},
    σ::Float64,
    log_lower::Float64,
    box_width::Float64,
    seed::UInt64,
    sample_index::Int,
)
    if sampler === :gaussian
        k = 1
        lane = 1
        @inbounds while k <= length(x)
            u1 = _volume_uniform01(seed, sample_index, lane)
            u2 = _volume_uniform01(seed, sample_index, lane + 1)
            radius = sqrt(-2.0 * log(u1))
            sine, cosine = sincos(2π * u2)
            x[k] = μ[k] + σ * radius * cosine
            if k < length(x)
                x[k + 1] = μ[k + 1] + σ * radius * sine
            end
            k += 2
            lane += 2
        end
    else
        @inbounds for k in eachindex(x)
            x[k] = log_lower + box_width * _volume_uniform01(seed, sample_index, k)
        end
    end
    return x
end

struct _VolumeWorkerWorkspace
    counts::Vector{Int}
    x::Vector{Float64}
    y::Vector{Float64}
end

function _volume_workspaces(
    worker_count::Int,
    n_regimes::Int,
    n_dim::Int,
    max_constraints::Int,
)
    worker_count > 0 || throw(ArgumentError("worker_count must be > 0"))
    return [
        _VolumeWorkerWorkspace(
            zeros(Int, n_regimes),
            Vector{Float64}(undef, n_dim),
            Vector{Float64}(undef, max_constraints),
        )
        for _ in 1:worker_count
    ]
end

function _count_volume_batch!(
    workspaces::Vector{_VolumeWorkerWorkspace},
    Cs::AbstractVector{<:AbstractMatrix{<:Real}},
    b64::AbstractVector{<:AbstractVector{<:Real}},
    active_ids::AbstractVector{<:Integer},
    active_mask::AbstractVector{Bool},
    sampler::Symbol,
    μ::Vector{Float64},
    σ::Float64,
    log_lower::Float64,
    box_width::Float64,
    regime_judge_tol::Float64,
    contain_overlap::Bool,
    seed::UInt64,
    sample_start::Int,
    sample_count::Int,
)
    n_workers = length(workspaces)
    classification_ids = contain_overlap ? active_ids : eachindex(Cs)
    base_count, extra_count = divrem(sample_count, n_workers)
    Threads.@threads :dynamic for worker_id in 1:n_workers
        workspace = workspaces[worker_id]
        fill!(workspace.counts, 0)

        preceding_workers = worker_id - 1
        first_offset = preceding_workers * base_count +
            min(preceding_workers, extra_count) + 1
        worker_sample_count = base_count + (worker_id <= extra_count ? 1 : 0)
        last_offset = first_offset + worker_sample_count - 1
        @inbounds for offset in first_offset:last_offset
            sample_index = Base.checked_add(sample_start, offset)
            _fill_volume_sample!(
                workspace.x,
                sampler,
                μ,
                σ,
                log_lower,
                box_width,
                seed,
                sample_index,
            )
            all(isfinite, workspace.x) ||
                throw(DomainError(workspace.x, "volume sampler produced a non-finite point"))

            for idx in classification_ids
                A = Cs[idx]
                b = b64[idx]
                row_count = size(A, 1)
                y = @view workspace.y[1:row_count]
                mul!(y, A, workspace.x)

                ok = true
                for k in 1:row_count
                    membership = y[k] + b[k]
                    isfinite(membership) ||
                        throw(DomainError(membership, "non-finite polyhedron membership"))
                    if membership < -regime_judge_tol
                        ok = false
                        break
                    end
                end
                ok || continue

                active_mask[idx] && (workspace.counts[idx] += 1)
                contain_overlap || break
            end
        end
    end
    return workspaces
end

function _reduce_volume_batch_counts!(
    batch_counts::Vector{Int},
    workspaces::Vector{_VolumeWorkerWorkspace},
    active_ids::AbstractVector{<:Integer},
)
    fill!(batch_counts, 0)
    # The merge order is deliberately logical-worker order, not completion or
    # physical-thread order. Counts are integers, so the result is exact.
    @inbounds for workspace in workspaces
        for idx in active_ids
            batch_counts[idx] += workspace.counts[idx]
        end
    end
    return batch_counts
end

"""
Internal one-batch reference used by the reproducibility contract. Samples are
identified by `(seed, sample_start + offset)`, so changing `worker_count` only
changes the partition of the same finite sample population.
"""
function _volume_batch_counts(
    Cs::AbstractVector{<:AbstractMatrix{<:Real}},
    b64::AbstractVector{<:AbstractVector{<:Real}},
    active_ids::AbstractVector{<:Integer};
    sampler::Symbol,
    μ::AbstractVector{<:Real},
    σ::Float64,
    log_lower::Float64,
    log_upper::Float64,
    regime_judge_tol::Float64,
    contain_overlap::Bool,
    seed::Integer,
    sample_start::Int,
    sample_count::Int,
    worker_count::Int,
)::Vector{Int}
    sample_start >= 0 || throw(ArgumentError("sample_start must be >= 0"))
    sample_count > 0 || throw(ArgumentError("sample_count must be > 0"))
    sample_start <= typemax(Int) - sample_count ||
        throw(ArgumentError("sample_start + sample_count exceeds Int range"))
    worker_count > 0 || throw(ArgumentError("worker_count must be > 0"))
    sampler in (:gaussian, :uniform_box) ||
        throw(ArgumentError("sampler must be :gaussian or :uniform_box"))
    isfinite(σ) && σ > 0 || throw(ArgumentError("σ must be finite and > 0"))
    isfinite(regime_judge_tol) ||
        throw(ArgumentError("regime_judge_tol must be finite"))
    box_width = 0.0
    if sampler === :uniform_box
        isfinite(log_lower) && isfinite(log_upper) && log_upper > log_lower ||
            throw(ArgumentError("uniform-box bounds must be finite and increasing"))
        box_width = log_upper - log_lower
        isfinite(box_width) && box_width > 0 ||
            throw(ArgumentError("uniform-box width must be finite and > 0"))
    end

    n_dim = _validate_volume_geometry(Cs, b64)
    length(μ) == n_dim || throw(ArgumentError("length(μ) must equal n_dim"))
    all(isfinite, μ) || throw(ArgumentError("μ must contain only finite values"))
    active = Int.(active_ids)
    all(idx -> 1 <= idx <= length(Cs), active) ||
        throw(ArgumentError("active_ids contains an out-of-range regime index"))
    length(unique(active)) == length(active) ||
        throw(ArgumentError("active_ids must not contain duplicates"))

    max_constraints = maximum((size(C, 1) for C in Cs); init=0)
    workspaces = _volume_workspaces(
        min(worker_count, sample_count),
        length(Cs),
        n_dim,
        max_constraints,
    )
    counts = zeros(Int, length(Cs))
    μ64 = Float64.(μ)
    active_mask = falses(length(Cs))
    active_mask[active] .= true
    _count_volume_batch!(
        workspaces,
        Cs,
        b64,
        active,
        active_mask,
        sampler,
        μ64,
        σ,
        log_lower,
        box_width,
        regime_judge_tol,
        contain_overlap,
        _normalize_volume_seed(seed),
        sample_start,
        sample_count,
    )
    return _reduce_volume_batch_counts!(counts, workspaces, active)
end

"""
    calc_volume(Cs, C0s; kwargs...) -> Vector{Volume}

Estimate the normalized measure of each polyhedron `A*x + b >= -tol` by
Monte Carlo sampling. With `sampler=:gaussian`, `Volume.mean` is Gaussian
probability mass. With `sampler=:uniform_box`, it is the occupied fraction of
the configured box and therefore remains in `[0, 1]`; it is **not** multiplied
by the box hypervolume. Multiply by `(log_upper - log_lower)^n` explicitly if
dimensional box volume is required. `Volume.var` is the squared Wilson-interval
half-width on the same normalized scale. `seed` selects a counter-based sample
stream: for a fixed finite sample population and deterministic stopping rule,
the result is independent of Julia thread scheduling and worker count. A
monotonic `time_limit` can still stop otherwise identical runs after different
numbers of complete batches; an early stop is warned and the returned Wilson
variance records the achieved precision. Use `Inf` when exact cross-run
reproducibility is required.
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
    seed::Integer = _VOLUME_DEFAULT_SEED,

    # --- perf/UX ---
    show_progress::Bool = false,

    # --- rebase---
    rebase_mat:: Union{AbstractMatrix{<:Real},Nothing} = nothing
)::Vector{Volume}

    length(Cs) == length(C0s) ||
        throw(ArgumentError("Cs and C0s must have the same length"))
    n_regimes = length(Cs)
    @info "Number of polyhedra to calc volume: $n_regimes"
    n_regimes == 0 && return Volume[]
    batch_size > 0 || throw(ArgumentError("batch_size must be > 0"))
    isfinite(confidence_level) && 0.0 < confidence_level < 1.0 ||
        throw(ArgumentError("confidence_level must be finite and in (0, 1)"))
    z = quantile(Normal(), (1 + confidence_level) / 2)
    isfinite(z) && z > 0 ||
        throw(ArgumentError("confidence_level is too close to an endpoint for Float64 Wilson intervals"))
    isfinite(abs_tol) && abs_tol >= 0 ||
        throw(ArgumentError("abs_tol must be finite and >= 0"))
    isfinite(rel_tol) && rel_tol >= 0 ||
        throw(ArgumentError("rel_tol must be finite and >= 0"))
    (abs_tol > 0 || rel_tol > 0) ||
        throw(ArgumentError("at least one of abs_tol or rel_tol must be > 0"))
    (time_limit == Inf || (isfinite(time_limit) && time_limit > 0)) ||
        throw(ArgumentError("time_limit must be finite and > 0, or Inf"))
    isfinite(regime_judge_tol) ||
        throw(ArgumentError("regime_judge_tol must be finite"))
    sampler in (:gaussian, :uniform_box) ||
        throw(ArgumentError("sampler must be :gaussian or :uniform_box, got $sampler"))
    box_width = 0.0
    if sampler === :gaussian
        isfinite(σ) && σ > 0 || throw(ArgumentError("σ must be finite and > 0"))
    else
        isfinite(log_lower) && isfinite(log_upper) && log_upper > log_lower ||
            throw(ArgumentError("uniform-box bounds must be finite and increasing"))
        box_width = log_upper - log_lower
        isfinite(box_width) && box_width > 0 ||
            throw(ArgumentError("uniform-box width must be finite and > 0"))
    end
    seed64 = _normalize_volume_seed(seed)

    # Dimensions & sanity
    n_dim = _validate_volume_geometry(Cs, C0s)

    Cs = if isnothing(rebase_mat)
           Cs
        else
            size(rebase_mat, 1) == n_dim ||
                throw(ArgumentError("rebase_mat row dimension must equal n_dim"))
            size(rebase_mat, 2) == n_dim ||
                throw(ArgumentError("rebase_mat must be square in the sampling dimension"))
            all(isfinite, rebase_mat) ||
                throw(ArgumentError("rebase_mat must contain only finite values"))
            [ Cs[i] * rebase_mat for i in 1:n_regimes ]
        end
    _validate_volume_geometry(Cs, C0s)
    # Convert b to Float64 once
    b64 = Vector{Vector{Float64}}(undef, n_regimes)
    for i in 1:n_regimes
        bi = C0s[i]
        b64[i] = (bi isa Vector{Float64}) ? bi : Float64.(bi)
        all(isfinite, b64[i]) ||
            throw(ArgumentError("C0s[$i] must remain finite after Float64 conversion"))
    end

    # Sampling params
    μ64 = Vector{Float64}(undef, n_dim)
    if sampler === :gaussian
        if μ === nothing
            fill!(μ64, 0.0)
        else
            length(μ) == n_dim || throw(ArgumentError("length(μ) must equal n_dim"))
            @inbounds for k in 1:n_dim
                μ64[k] = Float64(μ[k])
            end
        end
        all(isfinite, μ64) || throw(ArgumentError("μ must contain only finite values"))
    end

    regime_judge_tol = abs(regime_judge_tol)

    # Global stats
    total_counts = zeros(Int, n_regimes)
    total_N = 0
    stats = [Volume(0.0, 0.0) for _ in 1:n_regimes]
    active_ids = collect(1:n_regimes)
    active_mask = trues(n_regimes)

    # Logical worker storage. This is sized by workers that can participate in
    # this batch, not maxthreadid() (which includes unused thread-pool slots).
    # Each worker needs one y buffer sized to the largest polyhedron, rather
    # than one y buffer per polyhedron.
    n_workers = min(Threads.nthreads(), batch_size)
    max_constraints = maximum((size(C, 1) for C in Cs); init=0)
    worker_workspaces = _volume_workspaces(
        n_workers,
        n_regimes,
        n_dim,
        max_constraints,
    )
    batch_counts = zeros(Int, n_regimes)

    # Both samplers return normalized measures: uniform-box occupied fraction
    # or Gaussian probability mass.  No box-hypervolume scaling is applied.
    # optional progress (keep minimal overhead when off)
    p = show_progress ? Progress(n_regimes, desc="Calculating...", dt=1.0) : nothing

    start_time_ns = time_ns()
    elapsed_seconds() = Float64(time_ns() - start_time_ns) / 1.0e9

    while true
        isempty(active_ids) && (@info "All regimes converged after $total_N samples."; break)
        if total_N > 0 && elapsed_seconds() > time_limit
            @warn "Volume estimation reached its time limit before every regime met the requested tolerance" elapsed_seconds=round(elapsed_seconds(), digits=2) sample_count=total_N unconverged_regime_count=length(active_ids)
            break
        end

        # snapshot active_ids for threaded read-only access
        active_snapshot = active_ids
        fill!(active_mask, false)
        active_mask[active_snapshot] .= true
        next_total_N = try
            Base.checked_add(total_N, batch_size)
        catch err
            err isa OverflowError || rethrow()
            throw(ArgumentError("sample count exceeds Int range"))
        end

        _count_volume_batch!(
            worker_workspaces,
            Cs,
            b64,
            active_snapshot,
            active_mask,
            sampler,
            μ64,
            σ,
            log_lower,
            box_width,
            regime_judge_tol,
            contain_overlap,
            seed64,
            total_N,
            batch_size,
        )
        _reduce_volume_batch_counts!(batch_counts, worker_workspaces, active_ids)
        @inbounds for idx in active_ids
            total_counts[idx] = Base.checked_add(total_counts[idx], batch_counts[idx])
        end
        total_N = next_total_N

        # update CI & prune
        new_active = Int[]
        sizehint!(new_active, length(active_ids))

        @inbounds for idx in active_ids
            center, margin = _volume_wilson_center_margin(total_counts[idx], total_N, z)
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


"""
    calc_volume(C, C0; kwargs...) -> Volume

Compute volume for a single polyhedron.
"""
calc_volume(C::AbstractMatrix{<:Real}, C0::AbstractVector{<:Real}; kwargs...)::Volume =
    calc_volume([C], [C0]; kwargs...)[1]

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
    calc_volume(rgms::Union{AbstractVector{<:BindRegime}, AbstractVector{<:Polyhedron}}; asymptotic=true, kwargs...) -> Vector{Volume}

Compute volumes for a collection of polyhedra or vertices.

    calc_volume(model::Bnc, perms=nothing; asymptotic=true, kwargs...) -> Vector{Volume}

Compute volumes for selected regimes in a model.
"""
function calc_volume(rgms::Union{AbstractVector{<:BindRegime}, AbstractVector{<:Polyhedron}};
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
