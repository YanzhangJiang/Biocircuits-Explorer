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
                K = Bnc._N_sparse * x
                q = log10.(Bnc._L_sparse * exp10.(x))
            else
                K = exp10.(Bnc._N_sparse * x)
                q = Bnc._L_sparse * exp10.(x)
            end
        else
            if output_logspace
                K = Bnc._N_sparse * log10.(x)
                q = log10.(Bnc._L_sparse * x)
            else
                K = exp10.(Bnc._N_sparse * log10.(x))
                q = Bnc._L_sparse * x
            end
        end
        return vcat(q, K)
    else
        if input_logspace
            if output_logspace
                q = log10.(Bnc._L_sparse * exp10.(x))
            else
                q = Bnc._L_sparse * exp10.(x)
            end
        else
            if output_logspace
                q = log10.(Bnc._L_sparse * x)
            else
                q = Bnc._L_sparse * x
            end
        end
        return q
    end
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
    kwargs...
)::Vector{<:Real}
    n = Bnc.n
    d = Bnc.d
    #---Solve the nonlinear equation to find x from qK.---

    startlogx = isnothing(startlogx) ? copy(Bnc._anchor_log_x) : startlogx

    resid = Vector{Float64}(undef, n)

    logq = @view logqK[1:d]
    logK = @view logqK[d+1:end]

    J = deepcopy(Bnc._LN_sparse)# Make deep copies of sparse matrices to avoid shared state
    x = Vector{Float64}(undef, n)
    q = Vector{Float64}(undef, d)
    x_J_view = @view x[Bnc._LN_top_cols] # view for faster updating J
    q_J_view = @view q[Bnc._LN_top_rows] # view for faster updating J
    J_top = @view J.nzval[Bnc._LN_top_idx] # view for faster updating J
    L_nzval = copy(Bnc._LN_sparse.nzval[Bnc._LN_top_idx])

    params = (; x, q, logq, logK, J, x_J_view, q_J_view, J_top)


    keep_manifold! = function(resid, u, p) 
        logq, logK = p
        resid[1:d] .= log10.(Bnc._L_sparse * exp10.(u)) .- logq
        resid[d+1:end] .= Bnc._N_sparse * u .- logK
        return resid
    end

    manifold_jac! = function(J,u,p) # to have the same signature as keep_manifold!()
        @unpack x,q,logq,J,x_J_view,q_J_view, J_top = p
        # update jac for the current logx     
        @. x = exp10(u) # update x
        q .= Bnc._L_sparse * x #update q
        @. J_top = x_J_view * L_nzval / q_J_view
        return J
    end

    prob = NonlinearProblem(keep_manifold!, startlogx, params; resid_prototype=zeros(n), jac = manifold_jac!, jac_prototype=J)
    
    sol = solve(prob, method; kwargs...)
    if !SciMLBase.successful_retcode(sol.retcode)
        @warn("Nonlinear solver did not converge successfully. Retcode: $(sol.retcode)")
    end
    return sol.u
end

"""
    qK2x(bnc::Bnc, qK; K=nothing, logK=nothing, input_logspace=false, output_logspace=false,
        startlogx=nothing, startlogqK=nothing, use_vtx=false, method=:homotopy,
        reltol=1e-8, abstol=1e-10, kwargs...) -> Vector

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
- `kwargs...`: Passed through to the solver.

# Returns
- Vector of `x` values in log10 or linear space.
"""
function qK2x(Bnc::Bnc, qK::AbstractArray{<:Real,1};
    K::Union{Vector{<:Real},Nothing}=nothing,
    logK::Union{Vector{<:Real},Nothing}=nothing,
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    startlogx::Union{Vector{<:Real},Nothing}=nothing,
    startlogqK::Union{Vector{<:Real},Nothing}=nothing,
    use_vtx::Bool=false,
    method::Union{Symbol,Missing} = :homotopy,
    reltol = 1e-8,
    abstol = 1e-10,
    kwargs...)::Vector{<:Real}
    # Map from qK space to x space using homotopy or nonlinear solving.
    #---Solve the homotopy ODE to find x from qK.---

    # Define the start point 
    if isnothing(startlogqK) || isnothing(startlogx)
        # If no starting point is provided, use the default
        # Make deep copies to avoid shared state in threaded environment
        startlogx = copy(Bnc._anchor_log_x)
        startlogqK = copy(Bnc._anchor_log_qK)
    end

    # Define the end point
    processed_logqK = input_logspace ? qK : log10.(qK)
    local log_K_to_append = nothing
    if !isnothing(logK)
        if !isnothing(K)
            @warn("Both K and logK are provided; using logK.")
        end
        log_K_to_append = logK
    elseif !isnothing(K)
        log_K_to_append = log.(K)
    end
    endlogqK = isnothing(log_K_to_append) ? processed_logqK : vcat(processed_logqK, log_K_to_append)


    if use_vtx
        perm = assign_vertex_qK(Bnc,endlogqK; input_logspace=true,asymptotic_only=false)
        H,H0 = get_H_H0(Bnc,perm)
        x = H* endlogqK .+ H0
    elseif ismissing(method) || method != :homotopy
        x = _logqK2logx_nlsolve(Bnc, 
            endlogqK;
            startlogx=startlogx,
            method=method,
            reltol = reltol,
            abstol = abstol,
            kwargs...
        )
    else
        sol = _logx_traj_with_logqK_change(Bnc,
            startlogqK,
            endlogqK;
            startlogx=startlogx,
            alg=ODE.Tsit5(),
            save_everystep=false,
            save_start=false,
            reltol = reltol,
            abstol = abstol,
            kwargs...
        )
        x = sol.u[end]
    end

    x = output_logspace ? x : exp10.(x)
    return x
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

    solution = _logx_traj_with_logqK_change(Bnc, startlogqK, endlogqK;
        dense=false,
        kwargs...
    )
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

    ΔlogqK::V
    logx::V
    logqK::V
    logq::SV1
    logK::SV1

    J::SparseMatrixCSC{Float64,Int} 
    J_lu::SparseArrays.UMFPACK.UmfpackLU{Float64,Int}

    logx_J_view::SV2
    logq_J_view::SV2
    J_top::SV2
    J_top_diag::SV2
    
    # logx_local::V
    # logx_J_view_local::SV2
    # logLx_local::V
    # logLx_J_view_local::SV2
end

"""
    _logx_traj_with_logqK_change(bnc::Bnc, startlogqK, endlogqK; startlogx=nothing,
        alg=nothing, reltol=1e-8, abstol=1e-9, ensure_manifold=true, npoints=nothing, kwargs...) -> ODESolution

Integrate a homotopy path in log space to map qK changes to x trajectories.
"""
function _logx_traj_with_logqK_change(Bnc::Bnc,
    startlogqK::Union{Vector{<:Real},Nothing},
    endlogqK::Vector{<:Real};
    # Optional parameters for the initial log(x) values,act as initial point for ode solving
    startlogx::Union{Vector{<:Real},Nothing}=nothing,
    # Optional parameters for the ODE solver
    alg=nothing, # Default to nothing, will use Tsit5() if not provided
    reltol=1e-8,
    abstol=1e-9,
    ensure_manifold::Bool=true, # Make sure the trajectory stays on the manifold defined by Lx=q and Nlogx=logK
    npoints::Union{Nothing, Integer}=nothing,
    kwargs... #other Optional arguments for ODE solver
)::ODESolution
    # println("_logx_traj_with_logqK_change get kwargs: ", kwargs)
    #---Solve the homotopy ODE to find x from qK.---

    n = Bnc.n
    d = Bnc.d
    # Prepare starting x if not given
    startlogx = isnothing(startlogx) ? qK2x(Bnc, startlogqK; input_logspace=true, output_logspace=true) : startlogx
    
    #Homotopy path in log-space( a straight line)
    ΔlogqK = Float64.(endlogqK - startlogqK)

    # Create thread-local copies of all mutable data structures
    logx = Vector{Float64}(undef, n)
    logqK = Vector{Float64}(undef, n)
    logq = @view logqK[1:d]
    logK = @view logqK[d+1:end]
    J= deepcopy(Bnc._LN_sparse)# Make deep copies of sparse matrices to avoid shared state
    J_lu = deepcopy(Bnc._LN_lu)

    logx_J_view = @view logx[Bnc._LN_top_cols] # view for faster updating J
    logq_J_view = @view logqK[Bnc._LN_top_rows] # view for faster updating J
    J_top = @view J.nzval[Bnc._LN_top_idx] # view for faster updating J
    J_top_diag = @view J.nzval[Bnc._LN_top_diag_idx] # view for perturb when J is singular

    #Parameters helps for manifold projection
    # logx_local = Vector{Float64}(undef, n)
    # logx_J_view_local = @view logx_local[Bnc._LN_top_cols]
    # logLx_local = Vector{Float64}(undef, d)
    # logLx_J_view_local = @view logLx_local[Bnc._LN_top_rows]

    # Constants helps for updating mutable datas
    L_nzval = log10.(Bnc._LN_sparse.nzval[Bnc._LN_top_idx]) # copy the nzval to avoid shared access

    params = HomotopyParams(ΔlogqK, logx, logqK,logq,logK, J, J_lu, logx_J_view, logq_J_view, J_top, J_top_diag,
        # logx_local,logx_J_view_local,logLx_local, logLx_J_view_local
        )

   callback = if !ensure_manifold
         CB.CallbackSet()
    else
        keep_manifold! = function(resid, u, p)  #  Can not write to forms like log_sum_exp10!(logLx_local, Bnc._L_sparse, u) for Autodiff.
            @unpack logq,logK = p
            resid[1:d] .= log10.(Bnc._L_sparse * exp10.(u)) .- logq
            resid[d+1:end] .= Bnc._N_sparse * u .- logK
        end
        # manifold_jac! = function(J,u,p) # to have the same signature as keep_manifold!()
        #     @unpack logx_local, J,logx_J_view_local, J_top, logLx_local,logLx_J_view_local = p
        #     # update jac for the current logx     
        #     @. logx_local = exp10(u) # though name logx , it is actually x here
        #     logLx_local .= Bnc._L_sparse * logx_local # update logLx_local avoid modify logq that involving in "keep_manifold!"
        #     @. J_top = logx_J_view_local * L_nzval / logLx_J_view_local
        #     return J
        # end

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

    @inline function update_J_lu(J_lu,J,max_try=100)
        lu!(J_lu, J,check=false) # recalculate the LU decomposition of J
        try_count = 0
        while !issuccess(J_lu) && try_count < max_try
            @.J_top_diag += eps() # perturb the diagonal elements a bit to avoid singularity
            lu!(J_lu, J,check=false)
            try_count += 1
        end
        if try_count == max_try
            @show logx logqK
            @show J
            @warn("Jacobian is still singular after maximum perturbation attempts.")
        end
    end
    homotopy_process! = function(du, u, p, t)
        @unpack ΔlogqK, logx, logqK, J, J_lu, logx_J_view, logq_J_view, J_top,J_top_diag = p
        #update q & x
        clamp!(u,-Inf,20) # make sure not overflow.
        @. logx = u
        @. logqK = startlogqK + t * ΔlogqK
        #update J_top(sparse version) - use the local copy of nzval
        @. J_top = exp10(logx_J_view - logq_J_view + L_nzval)
        # Update the dlogx
        update_J_lu(J_lu,J)
        ldiv!(du, J_lu, ΔlogqK)
    end
    
    # Define the ODE system for the homotopy process
    # Solve the ODE using the DifferentialEquations.jl package
    tspan = (0.0, 1.0)
    prob = ODE.ODEProblem(homotopy_process!, startlogx, tspan, params)
    sol =  if isnothing(npoints) 
                ODE.solve(prob, alg; reltol=reltol, abstol=abstol, callback=callback, kwargs...)
            else
                ODE.solve(prob, alg; reltol=reltol, abstol=abstol, callback=callback,
                saveat=range(0,1,npoints),tstops=range(0,1,npoints),
                 kwargs...)
            end
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
function x_traj_cat(Bnc::Bnc, qK0_or_q0::Vector{<:Real}, tspan::Tuple{Real,Real};
    K::Union{Vector{<:Real},Nothing}=nothing,
    logK::Union{Vector{<:Real},Nothing}=nothing,
    input_logspace::Bool=false,
    output_logspace::Bool=false,
    kwargs...
    )
    # prepare the qK0 and calculate start logx0
    if isnothing(K) && isnothing(logK)
        @assert length(qK0_or_q0) == Bnc.n "qK0 must have length n, or you shall pass K as keyword argument"
        qK0 = qK0_or_q0
    else
        @assert length(qK0_or_q0)== Bnc.d "q0 must have length d"
        K_prepared = input_logspace ? (isnothing(logK) ? log10.(K) : logK) : (isnothing(K) ? K : exp10.(K))
        qK0 = vcat(qK0_or_q0, K_prepared)
    end
    startlogx = qK2x(Bnc, qK0; input_logspace=input_logspace, output_logspace=true)
    
    #---Solve the ODE to find the time curve of log(x) as catalysis happens
    sol = catalysis_logx(Bnc, startlogx, tspan;
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
function qK_traj_cat(Bnc::Bnc, args...; only_q::Bool=false, output_logspace::Bool=false, kwargs...)::Tuple{Vector{Float64}, Matrix{Float64}}
    t,u = x_traj_cat(Bnc, args...; output_logspace=true, kwargs...)
    u = x2qK(Bnc, u',input_logspace=true, output_logspace=output_logspace, only_q=only_q)'
    return (t,u)
end


"""
    TimecurveParam

Cache container for catalysis time-course integration.
"""
struct TimecurveParam{V<:Vector{Float64},
    SV1<:SubArray,SV2<:SubArray,SV3<:SubArray,SV4<:SubArray}

    x::V # Buffer for x values
    K::V # Buffer for K values
    v::V # Buffer for the catalysis flux vector
    Sv::V # Buffer for the catalysis rate vector multiplied by S
    J::SparseMatrixCSC{Float64,Int} # Jacobian matrix buffer
    J_lu::SparseArrays.UMFPACK.UmfpackLU{Float64,Int} # LU decomposition of J

    x_view::SV1 # View for x
    K_view::SV2 # View for K
    J_top::SV3 # View for the left part of the Jacobian matrix
    J_bottom::SV4 # View for the right part of the Jacobian matrix
end

"""
    catalysis_logx(bnc::Bnc, logx0, tspan; alg=nothing, reltol=1e-8, abstol=1e-9, kwargs...) -> ODESolution

Solve the catalysis ODE system in log space.
"""
function catalysis_logx(Bnc::Bnc, logx0::Vector{<:Real}, tspan::Tuple{Real,Real};
    alg=nothing, # Default to nothing, will use Tsit5() if not provided
    reltol=1e-8,
    abstol=1e-9,
    kwargs...
)::ODESolution
    # ---Solve the ODE to find the time curve of log(x) with respect to qK change.---
    if isnothing(Bnc.catalysis.S)||isnothing(Bnc.catalysis.aT)||isnothing(Bnc.catalysis.k)
        @error("S or aT or k is not defined, cannot perform catalysis logx calculation")
    end

    k = Bnc.k
    
    x = Vector{Float64}(undef, Bnc.n)
    K = Vector{Float64}(undef, Bnc.r)
    v = Vector{Float64}(undef, length(k)) # catalysis flux vector
    Sv = Vector{Float64}(undef, Bnc.n) # catalysis rate vector
    J = deepcopy(Bnc._LN_sparse) # Use the sparse version of the Jacobian matrix
    J_lu = deepcopy(Bnc._LN_lu) # LU decomposition of J

    x_view = @view x[Bnc._LN_top_cols]
    K_view = @view K[Bnc._LN_bottom_rows]
    J_top = @view J.nzval[Bnc._LN_top_idx]
    J_bottom = @view J.nzval[Bnc._LN_bottom_idx]
    # create view for the J_buffer , for updating [LΛ_x; Λ_KN]
    params = TimecurveParam(
        x, # x_buffer
        K, # K_buffer
        v, # v buffer / flux
        Sv, # Sv buffer
        J, # J_buffer
        J_lu, # Jt_lu
        #Views for updating J
        x_view,
        K_view,
        J_top,
        J_bottom
    )


    L_nzval = copy(Bnc._LN_sparse.nzval[Bnc._LN_top_idx]) # copy the nzval to avoid shared access
    N_nzval = copy(Bnc._LN_sparse.nzval[Bnc._LN_bottom_idx]) # copy the nzval to avoid shared access
    aT_sparse = Bnc.catalysis._aT_sparse # copy the aT_sparse to avoid shared access
    S_sparse = Bnc.catalysis._S_sparse # copy the S_sparse to avoid shared access
    N_sparse = Bnc._N_sparse # copy the N_sparse to avoid shared access
    _is_change_of_K_involved = Bnc._is_change_of_K_involved

    # Define the ODE system for the time curve
    if _is_change_of_K_involved
        Catalysis_process! = function (dlogx, logx, p, t)
            @unpack x, K, v, Sv, J, J_lu, J_top, J_bottom = p
            #update the values
            x .= exp10.(logx)
            K .= exp10.(N_sparse * logx)

            # Update the Jacobian matrix J
            @. J_top = x_view * L_nzval 
            @. J_bottom = N_nzval * K_view
            lu!(J_lu, J) # recalculate the LU decomposition of J

            # dlogx .= J \ (S * (k .* exp10.(aT * logx)))
            mul!(v, aT_sparse, logx)
            @. v = k * exp10(v) # calculate the catalysis rate vector
            mul!(Sv, S_sparse, v) # reuse x as a temporary buffer, but need change if x is used in other places, like to act call back for ODESolution
            ldiv!(dlogx, J_lu, Sv) # Use the LU decomposition for fast calculation
        end
    else
        # If K is not involved, we can skip the K update
        params.K .= exp10.(N_sparse * logx0) #initialize K_view once
        # @show length(Bnc._Nt_sparse.nzval) length(params.K_view) length(params.Jt_right)
        @. params.J_bottom = N_nzval * params.K_view #initialize Jt_right once

        Catalysis_process! = function (dlogx, logx, p, t)
            @unpack x, v, Sv, J, J_lu, J_top = p
            #update the values
            x .= exp10.(logx)
            # Update the Jacobian matrix J
            @. J_top = x_view * L_nzval
            lu!(J_lu, J) # recalculate the LU decomposition of J

            mul!(v, aT_sparse, logx)
            @. v = k * exp10(v) # calculate the catalysis rate vector
            mul!(Sv, S_sparse, v) # reuse x as a temporary buffer, but need change if x is used in other places, like to act call back for ODESolution
            ldiv!(dlogx, J_lu, Sv)
        end
    end

    # Create the ODE problem
    prob = ODE.ODEProblem(Catalysis_process!, logx0, tspan, params)
    sol = ODE.solve(prob, alg; reltol=reltol, abstol=abstol, kwargs...)
    return sol
end
