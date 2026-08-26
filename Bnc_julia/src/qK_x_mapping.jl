export x2qK, qK2x, x_traj_with_qK_change, x_traj_with_q_change, x_traj_cat, qK_traj_cat, q_traj_cat
export QK2XWorkLimitExceeded

struct QK2XWorkLimitExceeded <: Exception
    phase::Symbol
    requested::Int
    limit::Int
end

function Base.showerror(io::IO, error::QK2XWorkLimitExceeded)
    print(io, "qK2x ", error.phase, " requires ", error.requested,
        ", exceeding limit=", error.limit)
end

function _qk2x_optional_positive_limit(value, label::AbstractString)
    value === nothing && return nothing
    value isa Integer && !(value isa Bool) || throw(ArgumentError(
        "$label must be an integer or nothing"))
    converted = try
        Int(value)
    catch
        throw(ArgumentError("$label must fit in Int"))
    end
    converted > 0 || throw(ArgumentError("$label must be positive"))
    return converted
end

# ----------------Functions for mapping between qK space and x space----------------------------------

"""
    x2qK(bnc::Bnc, x; input_logspace=false, output_logspace=false, only_q=false)

Map concentrations `x` to totals/binding constants `qK`.

# Arguments
- `bnc`: Binding network model.
- `x`: Species concentrations (vector or matrix).

# Keyword Arguments
- `input_logspace`: Treat `x` as log10 values when `true`.
- `output_logspace`: Return log10 values when `true`.
- `only_q`: Return only `q` (conservation totals) when `true`.

# Returns
- Vector or array containing `q` (and `K` if `only_q=false`).
"""
function x2qK(Bnc::Bnc, x::AbstractArray{<:Real};
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    only_q::Bool=false,
)::AbstractArray{<:Real}
    if !only_q
        if input_logspace
            if output_logspace
                K = Bnc.N * x
                q = log10.(Bnc.L * exp10.(x))
            else
                K = exp10.(Bnc.N * x)
                q = Bnc.L * exp10.(x)
            end
        else
            if output_logspace
                K = Bnc.N * log10.(x)
                q = log10.(Bnc.L * x)
            else
                K = exp10.(Bnc.N * log10.(x))
                q = Bnc.L * x
            end
        end
        return vcat(q, K)
    else
        if input_logspace
            if output_logspace
                q = log10.(Bnc.L * exp10.(x))
            else
                q = Bnc.L * exp10.(x)
            end
        else
            if output_logspace
                q = log10.(Bnc.L * x)
            else
                q = Bnc.L * x
            end
        end
        return q
    end
end



"""
    qK2x(bnc::Bnc, qK; K=nothing, logK=nothing, input_logspace=false, output_logspace=false,
        startlogx=nothing, startlogqK=nothing, use_vtx=false, method=:homotopy,
        reltol=1e-8, abstol=1e-10, cancel_check=nothing,
        max_rhs_evaluations=nothing, kwargs...) -> Vector

Map from totals/binding constants (`qK`) to species concentrations `x`.

# Arguments
- `bnc`: Binding network model.
- `qK`: Vector of totals (and optionally binding constants).

# Keyword Arguments
- `K`: Binding constants in linear space.
- `logK`: Binding constants in log10 space.
- `input_logspace`: Treat inputs as log10 values when `true`.
- `output_logspace`: Return log10 values when `true`.
- `startlogx`: Initial guess for log10(x).
- `startlogqK`: Initial log10(qK) for homotopy.
- `use_vtx`: Use regime-based closed form when `true`.
- `method`: Solver method (`:homotopy` or NonlinearSolve symbol).
- `reltol`, `abstol`: Solver tolerances.
- `cancel_check`: Optional cooperative cancellation callback.  The homotopy
  path checks it before the solve and at every right-hand-side evaluation.
- `max_rhs_evaluations`: Optional positive hard cap for homotopy right-hand-
  side evaluations.
- `kwargs...`: Passed through to the solver.

# Returns
- Vector of `x` values in log10 or linear space.
"""
function qK2x(Bnc::Bnc, qK::AbstractVector{<:Real};
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    startlogx::Union{Vector{<:Real},Nothing}=nothing,
    startlogqK::Union{Vector{<:Real},Nothing}=nothing,
    use_vtx::Bool=false,
    method::Union{Symbol,Missing} = :homotopy,
    reltol = 1e-8,
    abstol = 1e-10,
    status::Union{Nothing,Base.RefValue{Symbol}}=nothing,
    cancel_check=nothing,
    max_rhs_evaluations=nothing,
    kwargs...
)::Vector{Float64}
    # Map from qK space to x space using homotopy or nonlinear solving.
    #---Solve the homotopy ODE to find x from qK.---

    # Define the start point


    cancel_check === nothing || cancel_check()
    rhs_limit = _qk2x_optional_positive_limit(
        max_rhs_evaluations, "max_rhs_evaluations")
    endlogqK = input_logspace ? qK : log10.(qK)

    helper = use_vtx ? nothing : _integration_helper!(Bnc)

    logx = if use_vtx
            perm = assign_regime_qK(Bnc,endlogqK; input_logspace=true,asymptotic_only=false)
            H,H0 = get_H_H0(Bnc,perm)
            status !== nothing && (status[] = :success)
            H* endlogqK .+ H0
        elseif ismissing(method) || method != :homotopy
            _logqK2logx_nlsolve(Bnc,
                endlogqK;
                startlogx = isnothing(startlogx) ? copy(helper._anchor_log_x) : Float64.(startlogx),
                method=method,
                reltol=reltol,
                abstol=abstol,
                status=status,
                kwargs...
            )
        else
            if isnothing(startlogqK) || isnothing(startlogx)
                # If no starting point is provided, use the default
                # Make deep copies to avoid shared state in threaded environment
                startlogx = copy(helper._anchor_log_x)
                startlogqK = copy(helper._anchor_log_qK)
            end
            sol = _logx_traj_with_logqK_change(Bnc,
                startlogqK,
                endlogqK;
                startlogx=startlogx,
                alg=ODE.Tsit5(),
                save_everystep=false,
                save_start=false,
                reltol = reltol,
                abstol = abstol,
                cancel_check=cancel_check,
                max_rhs_evaluations=rhs_limit,
                kwargs...
            )
            status !== nothing && (status[] = SciMLBase.successful_retcode(sol.retcode) ? :success : :failure)
            sol.u[end]
        end

    cancel_check === nothing || cancel_check()
    logx = output_logspace ? logx : exp10.(logx)
    return logx
end

"""
    qK2x(bnc::Bnc, qK::AbstractArray{<:Real,2}; kwargs...) -> AbstractArray

Batch mapping from qK space to x space for each column of `qK`.
"""
function qK2x(Bnc::Bnc, qK::AbstractArray{<:Real,2};kwargs...)::AbstractArray{<:Real}
    # batch mapping of qK2x for each column of qK and return as matrix.
    # Make thread-safe by creating separate copies for each thread
    f = x -> qK2x(Bnc, x; kwargs...)
    return matrix_iter(f, qK;byrow=false,multithread=true)
end















#----------------------------------------------------------------
# Playground for mapping different methods for solving the nonlinear system
# of equations to find x from qK.
#-----------------------------------------------------------------


"""
    _logqK2logx_nlsolve(bnc::Bnc, logqK; startlogx=nothing, method=missing, kwargs...) -> Vector

Solve for `logx` given `logqK` using a nonlinear solver.

# Arguments
- `bnc`: Binding network model.
- `logqK`: Log10 values of q and K.

# Keyword Arguments
- `startlogx`: Initial guess for log10(x).
- `method`: NonlinearSolve algorithm symbol.
- `kwargs...`: Passed through to `solve`.

# Returns
- Estimated log10(x) vector.
"""
function _logqK2logx_nlsolve(Bnc::Bnc, logqK::AbstractArray{<:Real,1};
    startlogx::Union{Vector{<:Real},Nothing}=nothing,
    method ::Union{Symbol,Missing} = missing,
    status::Union{Nothing,Base.RefValue{Symbol}}=nothing,
    kwargs...
)::Vector{<:Real}
    n = Bnc.n
    d = Bnc.d
    #---Solve the nonlinear equation to find x from qK.---
    helper = _integration_helper!(Bnc)

    startlogx = isnothing(startlogx) ? copy(helper._anchor_log_x) : startlogx

    resid = Vector{Float64}(undef, n)

    logq = @view logqK[1:d]
    logK = @view logqK[d+1:end]

    J = copy(helper._LN_sparse)
    x = Vector{Float64}(undef, n)
    q = Vector{Float64}(undef, d)
    x_M_view = @view x[helper._LN_top_cols] # view for faster updating J
    q_M_view = @view q[helper._LN_top_rows] # view for faster updating J
    M_top = @view J.nzval[helper._LN_top_idx] # view for faster updating J
    L_nzval = copy(helper._LN_sparse.nzval[helper._LN_top_idx])

    params = (; x, q, logq, logK, J, x_M_view, q_M_view, M_top)


    keep_manifold! = function(resid, u, p) 
        logq, logK = p
        resid[1:d] .= log10.(Bnc.L * exp10.(u)) .- logq
        resid[d+1:end] .= Bnc.N * u .- logK
        return resid
    end

    manifold_jac! = function(J,u,p) # to have the same signature as keep_manifold!()
        @unpack x,q,logq,J,x_M_view,q_M_view, M_top = p
        # update jac for the current logx     
        @. x = exp10(u) # update x
        q .= Bnc.L * x #update q
        @. M_top = x_M_view * L_nzval / q_M_view
        return J
    end

    prob = NonlinearProblem(keep_manifold!, startlogx, params; resid_prototype=zeros(n), jac = manifold_jac!, jac_prototype=J)
    
    sol = solve(prob, method; kwargs...)
    converged = SciMLBase.successful_retcode(sol.retcode)
    if !converged
        @warn("Nonlinear solver did not converge successfully. Retcode: $(sol.retcode)")
    end
    status !== nothing && (status[] = converged ? :success : :failure)
    return sol.u
end








#----------------Functions using homotopyContinuous to moving across x space along with qK change----------------------

"""
    x_traj_with_qK_change(bnc::Bnc, start_point, end_point; input_logspace=false, output_logspace=false, kwargs...)

Compute a trajectory in `x` space while `qK` changes linearly in log10 space.

# Arguments
- `bnc`: Binding network model.
- `start_point`: Starting `qK` values.
- `end_point`: Ending `qK` values.

# Keyword Arguments
- `input_logspace`: Treat inputs as log10 values when `true`.
- `output_logspace`: Return `x` in log10 space when `true`.
- `kwargs...`: Passed to the ODE solver.

# Returns
- Tuple `(t, x_traj)` containing time points and state vectors.
"""
function x_traj_with_qK_change(
    Bnc::Bnc,
    start_point::Vector{<:Real},
    end_point::Vector{<:Real};
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    kwargs...
)
    # println("x_traj_with_qK_change get kwargs: ", kwargs)

    startlogqK = input_logspace ? start_point : log10.(start_point)
    endlogqK = input_logspace ? end_point : log10.(end_point)

    solution = _logx_traj_with_logqK_change(Bnc, startlogqK, endlogqK; dense=false, kwargs...)

    if !output_logspace
        foreach(u -> u .= exp10.(u), solution.u)
    end

    return _ode_solution_wrapper(solution)
end


"""
    x_traj_with_q_change(bnc::Bnc, start_q, end_q; K=nothing, logK=nothing, input_logspace=false, kwargs...)

Compute an `x` trajectory for a change in `q` while holding `K` fixed.
"""
function x_traj_with_q_change(
    Bnc::Bnc,
    start_q::Vector{<:Real},
    end_q::Vector{<:Real};
    K::Union{Vector{<:Real},Nothing}=nothing,
    logK::Union{Vector{<:Real},Nothing}=nothing,
    input_logspace::Bool=false,
    kwargs...
)
    K_prepared = input_logspace ? (isnothing(logK) ? log10.(K) : logK) : (isnothing(K) ? K : exp10.(K))
    x_traj_with_qK_change(Bnc, [start_q;K_prepared], [end_q;K_prepared]; input_logspace=input_logspace,kwargs...)
end



"""
    HomotopyParams

Cache container for homotopy-based qK→x integration.
"""
struct HomotopyParams{V<:Vector{Float64},SV1<:SubArray,SV2<:SubArray}
    ### Constants
    startlogqK::V
    ΔlogqK::V
    logx::V
    logqK::V
    logq::SV1
    logK::SV1
    logqK_max::Float64

    M::SparseMatrixCSC{Float64,Int} 
    M_lu::SparseArrays.UMFPACK.UmfpackLU{Float64,Int}

    logx_M_view::SV2
    logq_M_view::SV2
    M_top::SV2
    M_top_diag::SV2
end


"""
    get_homotopy_param(bnc::Bnc, startlogqK, endlogqK; startlogx=nothing)

构造 homotopy ODE 所需的参数/缓存（线程局部可变对象）。

返回：
- p::HomotopyParams
- startlogqK0::Vector{Float64}  （用于 ODE 右端项里构造 logqK(t)）
- startlogx0::Vector{Float64}   （ODE 初值）
"""
function get_homotopy_param(Bnc::Bnc, startlogqK::Vector{<:Real}, endlogqK::Vector{<:Real})
    helper = _integration_helper!(Bnc)
    logqK_max = maximum([20.0,maximum(startlogqK), maximum(endlogqK)])
    n = Bnc.n
    d = Bnc.d
    startlogqK = Float64.(startlogqK)
    ΔlogqK = Float64.(endlogqK - startlogqK)
    # Create thread-local copies of all mutable data structures
    logx = Vector{Float64}(undef, n)
    logqK = Vector{Float64}(undef, n)
    logq = @view logqK[1:d]
    logK = @view logqK[d+1:end]
    M = copy(helper._LN_sparse)
    M_lu = deepcopy(helper._LN_lu)

    logx_M_view = @view logx[helper._LN_top_cols] # view for faster updating J
    logq_M_view = @view logqK[helper._LN_top_rows] # view for faster updating J
    M_top = @view M.nzval[helper._LN_top_idx] # view for faster updating J
    M_top_diag = @view M.nzval[helper._LN_top_diag_idx] # view for perturb when J is singular

    p = HomotopyParams(startlogqK, ΔlogqK, logx, logqK,logq,logK, logqK_max, M, M_lu, 
        logx_M_view, logq_M_view, M_top, M_top_diag
        # logx_local,logx_M_view_local,logLx_local, logLx_M_view_local
        )
    return p
end


function get_homotopy_ode(Bnc::Bnc)
    # Constants helps for updating mutable datas
    helper = _integration_helper!(Bnc)
    L_nzval = log10.(helper._LN_sparse.nzval[helper._LN_top_idx]) # copy the nzval to avoid shared access

    @inline function update_M_lu(M_lu,M,max_try=100)
        lu!(M_lu, M,check=false) # recalculate the LU decomposition of J
        try_count = 0
        while !issuccess(M_lu) && try_count < max_try
            @.M_top_diag += eps() # perturb the diagonal elements a bit to avoid singularity
            lu!(M_lu, M,check=false)
            try_count += 1
        end
        if try_count == max_try
            @error("M is still singular after maximum perturbation attempts.")
            @show M
        end
    end

    function(du, u, p, t)
        @unpack startlogqK, ΔlogqK, logx, logqK,logqK_max, M, M_lu, logx_M_view, logq_M_view, M_top,M_top_diag = p
        #update q & x
        clamp!(u,-Inf,logqK_max) # make sure not overflow.
        @. logx = u
        @. logqK = startlogqK + t * ΔlogqK
        #update M_top(sparse version) - use the local copy of nzval
        @. M_top = exp10(logx_M_view - logq_M_view + L_nzval)
        # Update the dlogx
        update_M_lu(M_lu,M)
        ldiv!(du, M_lu, ΔlogqK)
    end
end


"""
    _logx_traj_with_logqK_change(bnc::Bnc, startlogqK, endlogqK; startlogx=nothing,
        alg=nothing, reltol=1e-8, abstol=1e-9, ensure_manifold=true, npoints=nothing, kwargs...) -> ODESolution

Integrate a homotopy path in log space to map qK changes to x trajectories.
"""
function _logx_traj_with_logqK_change(Bnc::Bnc,
    startlogqK::Vector{<:Real},
    endlogqK::Vector{<:Real};
    # Optional parameters for the initial log(x) values,act as initial point for ode solving
    startlogx::Union{Vector{<:Real},Nothing}=nothing,
    # Optional parameters for the ODE solver
    alg=nothing, # Default to nothing, will use Tsit5() if not provided
    reltol=1e-8,
    abstol=1e-9,
    ensure_manifold::Bool=true, # Make sure the trajectory stays on the manifold defined by Lx=q and Nlogx=logK
    npoints::Union{Nothing, Integer}=nothing,
    cancel_check=nothing,
    max_rhs_evaluations=nothing,
    kwargs... #other Optional arguments for ODE solver
)::ODESolution
    # println("_logx_traj_with_logqK_change get kwargs: ", kwargs)
    #---Solve the homotopy ODE to find x from qK.---

    
    # Prepare starting x if not given
    cancel_check === nothing || cancel_check()
    rhs_limit = _qk2x_optional_positive_limit(
        max_rhs_evaluations, "max_rhs_evaluations")
    rhs_evaluations = Ref(0)
    u0 = isnothing(startlogx) ? qK2x(
        Bnc,
        startlogqK;
        input_logspace=true,
        output_logspace=true,
        cancel_check=cancel_check,
        max_rhs_evaluations=rhs_limit,
    ) : startlogx
    p = get_homotopy_param(Bnc, startlogqK, endlogqK)
    base_f! = get_homotopy_ode(Bnc)
    f! = if cancel_check === nothing && rhs_limit === nothing
        base_f!
    else
        function (du, u, parameters, time)
            cancel_check === nothing || cancel_check()
            rhs_evaluations[] += 1
            rhs_limit === nothing || rhs_evaluations[] <= rhs_limit ||
                throw(QK2XWorkLimitExceeded(
                    :rhs_evaluations, rhs_evaluations[], rhs_limit))
            return base_f!(du, u, parameters, time)
        end
    end

    callback = if !ensure_manifold
            CB.CallbackSet()
        else
            n = Bnc.n
            d = Bnc.d
            keep_manifold! = function(resid, u, p)  # Can not write to forms like log_sum_exp10!(logLx_local, Bnc.L, u) for Autodiff.
                @unpack logq,logK = p
                resid[1:d] .= log10.(Bnc.L * exp10.(u)) .- logq
                resid[d+1:end] .= Bnc.N * u .- logK
            end
            equilibrium_cb = CB.ManifoldProjection(keep_manifold!;
                save=false,
                resid_prototype=zeros(n),
                # manifold_jacobian=manifold_jac!,
                # jac_prototype = [Bnc.L;Bnc.N],
                autodiff = AutoForwardDiff(),
                abstol=1e-12,
                reltol=1e-10
            )
            CB.CallbackSet(equilibrium_cb)
        end

    # Solve the ODE using the DifferentialEquations.jl package

    prob = ODE.ODEProblem(f!, u0, (0.0, 1.0), p)

    sol =  if isnothing(npoints) 
                ODE.solve(prob, alg; reltol=reltol, abstol=abstol, callback=callback, kwargs...)
            else
                ODE.solve(prob, alg; reltol=reltol, abstol=abstol, callback=callback,
                saveat=range(0,1,npoints),tstops=range(0,1,npoints),
                 kwargs...)
            end
    cancel_check === nothing || cancel_check()
    return sol
end




#--------------------------------------------------------------------------------
#      Functions for modeling when envolving catalysis reactions, 
#--------------------------------------------------------------------------------



"""
    x_traj_cat(bnc::Bnc, qK0_or_q0, tspan; K=nothing, logK=nothing,
        input_logspace=false, output_logspace=false, kwargs...) -> (Vector, Vector)

Simulate species trajectories under catalysis dynamics.
"""
function x_traj_cat(Bnc::Bnc, x0::Vector{<:Real}, tspan::Tuple{Real,Real};
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    kwargs...
    )
    x0 = input_logspace ? x0 : log10.(x0)
    # startlogx = qK2x(Bnc, qK0; input_logspace=input_logspace, output_logspace=true)
    #---Solve the ODE to find the time curve of log(x) as catalysis happens
    sol = catalysis_logx(Bnc, x0, tspan;
        dense = false, #manually handle later
        kwargs...
    )
    if !output_logspace
        foreach(u -> u .= exp10.(u), sol.u)
    end
    
    return _ode_solution_wrapper(sol)
end

"""
    qK_traj_cat(bnc::Bnc, args...; only_q=false, output_logspace=false, kwargs...) -> (Vector{Float64}, Matrix{Float64})

Simulate catalysis dynamics and return trajectories in q/K space.
"""
function qK_traj_cat(Bnc::Bnc, qK0::Vector{<:Real}, args...;
    only_q::Bool=false,
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    kwargs...
    )

    logx0 = qK2x(Bnc, qK0; input_logspace=input_logspace, output_logspace=true)
    t,u = x_traj_cat(Bnc, logx0, args...; input_logspace=true,output_logspace=true, kwargs...)
    u = x2qK.(Ref(Bnc), u;input_logspace=true,output_logspace=output_logspace, only_q=only_q)
    return (t,u)
end

q_traj_cat(args...;kwargs...) = qK_traj_cat(args...;only_q=true,kwargs...)


function have_catalysis(model::Bnc)
    return !isnothing(model.catalysis)
end




#--------------------------------------------------------------------------
#   Below are most AI generated code, which is more experimental and less tested, especially for the catalysis part. Use with caution and report any issues.
#--------------------------------------------------------------------------









"""
    TimecurveParam
    ### Constant
    # logk: R^rcat  # Changed to log10(k) for stability

    ### Cache
    # x: R^n  # Now used as buffer for scaled computations
    # q: R^d  # Buffer for q_scaled or log_q if needed
    # v: R^rcat  # Buffer for log(v_cat) = Π * u + logk
    # f: R^n  # First d values: Λ_q^{-1} Γ v_cat(x)
    # M: SparseMatrixCSC{Float64,Int}  # Jacobian matrix buffer [diag(1/q) L diag(x); N]
    # M_lu: SparseArrays.UMFPACK.UmfpackLU{Float64,Int}  # LU decomposition of M
Cache container for catalysis time-course integration.
"""
struct TimecurveParam{V<:Vector{Float64}}
    logk::V  # log10(k)
    x::V 
    q::V
    v::V 
    f::V 
    M::SparseMatrixCSC{Float64,Int}
    M_lu::SparseArrays.UMFPACK.UmfpackLU{Float64,Int}
end

"""
Get the catalysis parameter for ODE f construction
"""
function get_catalysis_param(model::Bnc, k)
    @assert have_catalysis(model) "Should fill catalysis data first"
    helper = _integration_helper!(model)
    logk = log10.(k)
    x = Vector{Float64}(undef, model.n)  # Buffer
    q = Vector{Float64}(undef, model.d)  # Buffer
    v = Vector{Float64}(undef, length(logk))  # Catalysis flux buffer (log scale)
    f = zeros(model.n)  # Catalysis rate vector
    M = copy(helper._LN_sparse)  # Sparse [L; N]
    M_lu = deepcopy(helper._LN_lu)  # LU decomp
    TimecurveParam(logk, x, q, v, f, M, M_lu)
end

"""
return the f(du,u,p,t) for ODE solver
"""
function get_catalysis_ode(model::Bnc)
    @assert have_catalysis(model) "Should fill catalysis data first"
    helper = _integration_helper!(model)
    # No longer need L_nzval as log10, since we avoid exp10 in matrix updates

    @inline function update_M_lu(M_lu, M, max_try=100)
        lu!(M_lu, M, check=false)
        try_count = 0
        while !issuccess(M_lu) && try_count < max_try
            # Clamp to prevent extreme values (though less needed now)
            # clamp!(M.nzval, 1e-100, 1e100)
            # Perturb diagonal elements slightly
            @. M.nzval[helper._LN_top_diag_idx] += 1e-10 * rand()  # Random small perturbation
            lu!(M_lu, M, check=false)
            try_count += 1
        end
        if try_count == max_try
            @error("M is still singular after maximum perturbation attempts.")
        end
    end

    function f(du, u, p::TimecurveParam, t) 
        @unpack logk, x, q, v, f, M, M_lu = p

        # Compute v = Π * u + logk  (log10(v_cat))
        mul!(v, model.catalysis._Π_sparse, u)
        v .+= logk

        # Stably compute f[1:d] = Λ_q^{-1} * Γ * 10^v  (where 10^v = v_cat)
        # And simultaneously compute scaled x and q for M update
        # Scale for x: exp10(u) = 10^{max_u} * exp10(u - max_u)
        max_u = maximum(u)
        @. x = exp10.(u - max_u)  # x_scaled in (0,1]
        mul!(q, model.L, x)  # q_scaled = L * x_scaled
        @. q = max(q, 1e-300)  # Floor to avoid div-by-zero or tiny denoms

        # Update M top block: diag(1/q_scaled) * L * diag(x_scaled)
        # This is equivalent to (L * exp10(u)) ./ q but scaled: since exp10(u) = 10^{max_u} x_scaled, q = 10^{max_u} q_scaled
        # So (L exp10(u)) ./ q = (L * 10^{max_u} x_scaled) ./ (10^{max_u} q_scaled) = (L x_scaled) ./ q_scaled
        # We scale L copy in-place for efficiency
        # M_top = @view M[1:model.d, :]  # View of top block (L part)
        # Reset M_top to original L values (assuming M was initialized with [L; N] as Float64)
        M.nzval[helper._LN_top_idx] .= model.L.nzval  # Reset to L values
        # Scale columns by x_scaled
        for j = 1:model.n
            for p = M.colptr[j]:(M.colptr[j+1]-1)
                if M.rowval[p] <= model.d  # Only top block
                    M.nzval[p] *= x[j]
                end
            end
        end
        # Scale rows by 1 ./ q
        for j = 1:model.n
            for p = M.colptr[j]:(M.colptr[j+1]-1)
                row = M.rowval[p]
                if row <= model.d
                    M.nzval[p] /= q[row]
                end
            end
        end
        # Bottom block remains N (unchanged)

        # Now update LU
        update_M_lu(M_lu, M)

        # Compute f[1:d] using stable_Linv_Γexp10: (Γ * 10^v) ./ (L * 10^u) = Λ_q^{-1} Γ v_cat
        @view(f[1:model.d]) .= stable_Linv_Γexp10(model.L, model.catalysis._Γ_sparse, u, v)
        fill!(@view(f[model.d+1:end]), 0.0)  # Last r are 0

        # Solve du = M_lu \ f
        ldiv!(du, M_lu, f)
        if any(isnan, du)
            @error("du has NaN values, cannot proceed.")
        end
    end
end

"""
Compute  Λ_{L*exp10(a)}^{-1} * Γ*exp10(b) in a stable way
(No changes needed, but included for completeness)
"""
function stable_Linv_Γexp10(L::SparseMatrixCSC{<:Real,Int},
                            Γ::SparseMatrixCSC{<:Real,Int},
                            a::AbstractVector{<:Real},
                            b::AbstractVector{<:Real};
                            q_floor::Float64 = 1e-300)
    # Scale exp10(a) to avoid overflow/underflow: exp10(a) = 10^c * exp10(a-c)
    c = maximum(a)
    xscaled = exp10.(Float64.(a) .- c)               # in (0,1]
    qscaled = Vector{Float64}(undef, size(L,1))
    mul!(qscaled, sparse(Float64.(L)), xscaled)      # qscaled = L * exp10(a-c)
    # q = 10^c * qscaled, so 1/q = 10^(-c) ./ qscaled
    @inbounds @. qscaled = max(qscaled, q_floor)
    # Scale exp10(b): exp10(b) = 10^d * exp10(b-d)
    d = maximum(b)
    vscaled = exp10.(Float64.(b) .- d)               # in (0,1]
    yscaled = Vector{Float64}(undef, size(Γ,1))
    mul!(yscaled, sparse(Float64.(Γ)), vscaled)      # yscaled = Γ * exp10(b-d)
    # Combine scales: (Γ*10^b) ./ (L*10^a) = 10^(d-c) * (yscaled ./ qscaled)
    scale = exp10(d - c)
    out = Vector{Float64}(undef, length(yscaled))
    @inbounds @. out = (yscaled / qscaled) * scale
    return out
end



"""
    catalysis_logx(bnc::Bnc, logx0, tspan; alg=nothing, reltol=1e-8, abstol=1e-9, kwargs...) -> ODESolution

Solve the catalysis ODE system in log space.
"""
function catalysis_logx(Bnc::Bnc, logx0::Vector{<:Real}, tspan::Tuple{Real,Real};
    k::AbstractVector{<:Real},
    alg=nothing, # Default to nothing, will use Tsit5() if not provided
    reltol=1e-8,
    abstol=1e-9,
    kwargs...
)::ODESolution
    # ---Solve the ODE to find the time curve of log(x) with respect to qK change.---
    p = get_catalysis_param(Bnc, k)
    f = get_catalysis_ode(Bnc)
    # Create the ODE problem
    prob = ODE.ODEProblem(f, logx0, tspan, p)
    sol = ODE.solve(prob, alg; reltol=reltol, abstol=abstol, kwargs...)
    return sol
end






# Helper functions to scale sparse matrices
function scale_columns!(A::SparseMatrixCSC{Float64, Int}, s::Vector{Float64})
    n = size(A, 2)
    @inbounds for j = 1:n
        for p = A.colptr[j]:(A.colptr[j+1]-1)
            A.nzval[p] *= s[j]
        end
    end
    return A
end

function scale_rows!(A::SparseMatrixCSC{Float64, Int}, s::Vector{Float64})
    @inbounds for j = 1:size(A, 2)
        for p = A.colptr[j]:(A.colptr[j+1]-1)
            row = A.rowval[p]
            A.nzval[p] *= s[row]
        end
    end
    return A
end

# The right-hand side function for the ODE: dy/dt = f(y)
function ode_rhs!(dy::Vector{Float64}, y::Vector{Float64}, p, t)
    Lf, Γf, Nf, Πf, k::Vector{Float64}, q_floor::Float64 = p

    n = length(y)
    d = size(Lf, 1)
    r = size(Nf, 1)

    # Scale x = 10.^y
    max_y = maximum(y)
    xscaled = exp10.(y .- max_y)

    # qscaled = L * xscaled
    qscaled = Vector{Float64}(undef, d)
    mul!(qscaled, Lf, xscaled)
    @. qscaled = max(qscaled, q_floor)

    # Build A = Λ_q^{-1} * L * Λ_x (stably: diag(1./qscaled) * L * diag(xscaled))
    L_scaled = copy(Lf)
    scale_columns!(L_scaled, xscaled)
    scale_rows!(L_scaled, 1.0 ./ qscaled)  # Now L_scaled is A

    # Build M = [A; N]
    M = vcat(L_scaled, Nf)

    # Compute v_cat stably: v_cat = k .* exp10.(Π * y)
    u = Πf * y
    c = maximum(u)
    vscaled = exp10.(u .- c)
    v_cat_scaled = k .* vscaled

    # sv_scaled = Γ * v_cat_scaled
    sv_scaled = Vector{Float64}(undef, d)
    mul!(sv_scaled, Γf, v_cat_scaled)

    # zscaled = (1 ./ qscaled) .* sv_scaled
    zscaled = (1.0 ./ qscaled) .* sv_scaled

    # Full scale for z = exp10(c - max_y) * zscaled
    scale = exp10(c - max_y)
    z = scale .* zscaled

    # w = [z; zeros(r)]
    w = vcat(z, zeros(Float64, r))

    # Solve M * dy = w (using factorization for sparse matrix)
    fact = factorize(M)
    dy[:] = fact \ w

    return nothing
end

# Main simulation function
function simulate_ode(L::SparseMatrixCSC{<:Real, Int},
                      Γ::SparseMatrixCSC{<:Real, Int},
                      N::SparseMatrixCSC{<:Real, Int},
                      Π::SparseMatrixCSC{<:Real, Int},
                      k::AbstractVector{<:Real},  # Assuming Lambda_k is a vector k
                      y0::AbstractVector{<:Real},  # Initial log10(x)
                      tspan::Tuple{<:Real, <:Real};
                      q_floor::Float64 = 1e-300,
                      rtol::Float64 = 1e-6,
                      atol::Float64 = 1e-6,
                      solver = ODE.Tsit5())  # Can change to other solvers like Rodas5() for stiff systems
    # Convert to Float64 sparse matrices
    Lf = sparse(Float64.(L))
    Γf = sparse(Float64.(Γ))
    Nf = sparse(Float64.(N))
    Πf = sparse(Float64.(Π))

    # Convert vectors to Float64
    kf = Float64.(k)
    y0f = Float64.(y0)

    # Pack parameters
    p = (Lf, Γf, Nf, Πf, kf, q_floor)

    # Define ODE problem
    prob = ODE.ODEProblem(ode_rhs!, y0f, tspan, p)

    # Solve
    sol = ODE.solve(prob, solver; reltol=rtol, abstol=atol)

    return sol
end

# function catalysis_logx(Bnc::Bnc, logx0::Vector{<:Real}, tspan::Tuple{Real,Real};
#     k::AbstractVector{<:Real},
#     alg=nothing, # Default to nothing, will use Tsit5() if not provided
#     reltol=1e-8,
#     abstol=1e-9,
#     kwargs...
# )::ODESolution
#     return simulate_ode(Bnc.L, 
#             Bnc.catalysis._Γ_sparse, 
#             Bnc.N, 
#             sparse(Bnc.catalysis.Π), 
#             k, 
#             logx0,
#             tspan; 
#             rtol=reltol, atol=abstol, solver=alg === nothing ? ODE.Tsit5() : alg)
# end
