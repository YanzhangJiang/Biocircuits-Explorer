# Build a conservation matrix L anchored on free species:
# L = [I_d  Z'] with N_bound * Z = -N_free so that N * L' = 0.
function derive_atomic_totals_matrix(N::Matrix{Int}, n_free::Int)
    r, n = size(N)
    d = n_free
    d > 0 || throw(ArgumentError("At least one free species is required"))
    n == d + r || throw(ArgumentError(
        "x-space closed-form requires n = d + r, got n=$n, d=$d, r=$r"))

    N_free = Rational{Int}.(N[:, 1:d])
    N_bound = Rational{Int}.(N[:, d+1:end])
    size(N_bound, 1) == size(N_bound, 2) ||
        throw(ArgumentError("Bound block of N must be square for anchored totals"))
    rank(Float64.(N_bound)) == size(N_bound, 1) ||
        throw(ArgumentError("Bound block of N is singular; cannot derive anchored totals"))

    Z = -(N_bound \ N_free) # r × d
    L_rat = hcat(Matrix{Rational{Int}}(I, d, d), transpose(Z))

    residual = Rational{Int}.(N) * transpose(L_rat)
    all(iszero, residual) ||
        throw(ArgumentError("Failed to derive valid conservation matrix (N * L' != 0)"))

    L = Float64.(L_rat)
    L[abs.(L) .< 1e-12] .= 0.0
    return L
end

function compute_rop_cloud_xspace(rules::Vector{String}, n_samples::Int, logx_min::Float64, logx_max::Float64;
    target_species::Union{Nothing,Symbol}=nothing)
    logx_max > logx_min ||
        throw(ArgumentError("logx_max must be greater than logx_min"))

    N, species, free_syms, prod_syms = parse_network_structure(rules)
    L = derive_atomic_totals_matrix(N, length(free_syms))

    r, n = size(N)
    d = size(L, 1)
    n == d + r || throw(ArgumentError("x-space model must satisfy n=d+r"))
    r <= MAX_SYNC_REACTIONS ||
        _sync_budget_exceeded("Model reaction count exceeds the synchronous limit of $(MAX_SYNC_REACTIONS).")
    n <= MAX_SYNC_MODEL_N ||
        _sync_budget_exceeded("Model dimension exceeds the synchronous limit of $(MAX_SYNC_MODEL_N).")
    enforce_sync_cost(n_samples * n^3, MAX_SYNC_ROP_CLOUD_COST, "x-space ROP cloud")

    target_sym = if isnothing(target_species)
        !isempty(prod_syms) ? prod_syms[1] : species[end]
    else
        target_species
    end
    target_idx = findfirst(==(target_sym), species)
    target_idx === nothing && throw(ArgumentError(
        "target_species '$target_sym' not found in species: $(join(string.(species), ", "))"))

    reaction_orders = Matrix{Float64}(undef, n_samples, d)
    target_values = Vector{Float64}(undef, n_samples)

    Nf = Float64.(N)
    A = Matrix{Float64}(undef, n, n)
    A[d+1:end, :] .= Nf
    e_target = zeros(Float64, n)
    e_target[target_idx] = 1.0

    accepted = 0
    attempts = 0
    max_attempts = max(10 * n_samples, n_samples + 1000)

    while accepted < n_samples && attempts < max_attempts
        attempts += 1

        logx = logx_min .+ (logx_max - logx_min) .* rand(n)
        x = exp10.(logx)
        t = L * x
        if any(!isfinite, t) || any(t .<= 0.0)
            continue
        end

        @views A[1:d, :] .= (L .* transpose(x)) ./ t
        y = try
            A' \ e_target
        catch err
            if err isa SingularException || err isa LinearAlgebra.LAPACKException
                continue
            else
                rethrow(err)
            end
        end

        if any(!isfinite, y)
            continue
        end

        accepted += 1
        @views reaction_orders[accepted, :] .= y[1:d]
        target_values[accepted] = x[target_idx]
    end

    accepted == n_samples || throw(DomainError(
        accepted,
        "Only accepted $accepted / $n_samples samples (attempts=$attempts)",
    ))

    q_sym = Symbol.("t" .* String.(free_syms))
    return reaction_orders, target_values, q_sym, d, species[target_idx]
end

# ─── ROP point cloud computation ───
function compute_rop_cloud(model, kd, prod_syms_sym, n_samples, span)
    logK = log10.(kd)
    logq_min = minimum(logK) - span
    logq_max = maximum(logK) + span

    d = model.d
    prod_idx = [locate_sym_x(model, s) for s in prod_syms_sym]

    # Allocate output
    reaction_orders = Matrix{Float64}(undef, n_samples, d)
    fret_values = Vector{Float64}(undef, n_samples)
    valid = falses(n_samples)

    for k in 1:n_samples
        logq = logq_min .+ (logq_max - logq_min) .* rand(d)
        logqK = vcat(logq, logK)
        status = Ref{Symbol}(:not_run)
        x = qK2x(
            model, logqK;
            input_logspace=true,
            output_logspace=false,
            status=status,
        )
        if status[] !== :success || any(!isfinite, x)
            reaction_orders[k, :] .= NaN
            fret_values[k] = NaN
            continue
        end
        J = try
            # Reuse the equilibrium state whose convergence was just checked.
            # Supplying qK here would run a second, untracked nonlinear solve.
            ∂logx_∂logqK(model; x=x, input_logspace=false)
        catch err
            if err isa SingularException || err isa LinearAlgebra.LAPACKException ||
               err isa DomainError
                reaction_orders[k, :] .= NaN
                fret_values[k] = NaN
                continue
            end
            rethrow()
        end

        # Weighted reaction orders for FRET proxy
        fret_total = sum(x[prod_idx])
        if !(isfinite(fret_total) && fret_total > eps(Float64)) || any(!isfinite, J)
            reaction_orders[k, :] .= NaN
            fret_values[k] = NaN
            continue
        end
        w = x[prod_idx] ./ fret_total
        ro = vec(w' * J[prod_idx, 1:d])
        if any(!isfinite, ro)
            reaction_orders[k, :] .= NaN
            fret_values[k] = NaN
            continue
        end
        reaction_orders[k, :] = ro
        fret_values[k] = fret_total
        valid[k] = true
    end

    return reaction_orders, fret_values, valid
end

# ─── SISO trajectory computation ───
function compute_siso_trajectory(model, siso, path_idx; npoints=500, start_val=-6, stop_val=6)
    path_idx_int = path_idx
    poly = get_polyhedron(siso, path_idx_int)
    params = get_one_inner_point(poly; rand_line=false, rand_ray=false, extend=4)

    change_idx = get_change_qK_idx(siso)

    start_logqK = copy(params) |> x -> insert!(x, change_idx, start_val)
    end_logqK = copy(params) |> x -> insert!(x, change_idx, stop_val)

    t_ode, logx_traj = x_traj_with_qK_change(model, start_logqK, end_logqK;
        input_logspace=true, output_logspace=true, npoints=npoints, ensure_manifold=true)

    # t_ode is in [0,1], map to actual logqK change values
    change_values = start_val .+ t_ode .* (stop_val - start_val)

    regimes = [assign_vertex_x(model, lx; input_logspace=true, return_idx=true) for lx in logx_traj]

    # Convert to matrix
    logx_mat = reduce(hcat, logx_traj)'  # npoints × n

    return Dict(
        "change_values" => collect(change_values),
        "logx" => mat2vv(Matrix(logx_mat)),
        "regimes" => regimes,
        "x_sym" => string.(model.x_sym),
        "change_sym" => string(qK_sym(model)[change_idx]),
        "parameters" => params,
    )
end

volume_to_dict(vol) = isnothing(vol) ? nothing : Dict(
    "mean" => vol.mean,
    "var" => vol.var,
    "std" => sqrt(vol.var),
    "rel_error" => vol.mean == 0 ? nothing : sqrt(vol.var) / vol.mean,
)

function behavior_scope_note(path_scope::Symbol, min_volume_mean::Float64)
    if path_scope == :all
        return "Including every graph path, even when the corresponding path polyhedron is empty. Use this only for graph-level overviews."
    elseif path_scope == :feasible
        return "Including only paths with non-empty path polyhedra. Paths excluded here are graph-theoretic paths that have no common parameter region after all path constraints are intersected."
    else
        return "Including only feasible paths whose estimated volume mean is at least $(min_volume_mean). This is a robustness filter on top of feasibility."
    end
end

function behavior_result_to_dict(model, siso, result)
    path_dicts = Vector{Dict{String,Any}}(undef, length(result.path_records))
    for rec in result.path_records
        path_dicts[rec.path_idx] = Dict(
            "path_idx" => rec.path_idx,
            "vertex_indices" => collect(rec.vertex_indices),
            "perms" => [collect(get_perm(model, idx)) for idx in rec.vertex_indices],
            "exact_profile" => json_safe_profile(rec.exact_profile),
            "exact_label" => rec.exact_label,
            "motif_profile" => collect(rec.motif_profile),
            "motif_label" => rec.motif_label,
            "feasible" => rec.feasible,
            "feasibility_checked" => rec.feasibility_checked,
            "included" => rec.included,
            "exclusion_reason" => rec.exclusion_reason,
            "volume" => volume_to_dict(rec.volume),
        )
    end

    exact_families = map(result.exact_families) do family
        Dict(
            "family_idx" => family.family_idx,
            "exact_profile" => json_safe_profile(family.exact_profile),
            "exact_label" => family.exact_label,
            "motif_profile" => collect(family.motif_profile),
            "motif_label" => family.motif_label,
            "path_indices" => collect(family.path_indices),
            "n_paths" => family.n_paths,
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_volume" => volume_to_dict(family.representative_volume),
        )
    end

    motif_families = map(result.motif_families) do family
        Dict(
            "family_idx" => family.family_idx,
            "motif_profile" => collect(family.motif_profile),
            "motif_label" => family.motif_label,
            "path_indices" => collect(family.path_indices),
            "exact_family_indices" => collect(family.exact_family_indices),
            "n_paths" => family.n_paths,
            "total_volume" => volume_to_dict(family.total_volume),
            "representative_path_idx" => family.representative_path_idx,
            "representative_volume" => volume_to_dict(family.representative_volume),
        )
    end

    change_qK_indices = Int[Int(idx) for idx in result.change_qK_indices]
    change_qK_signs = Int[Int(sign) for sign in result.change_qK_signs]
    change_qK_symbols = [string(qK_sym(model)[idx]) for idx in change_qK_indices]
    change_qK = result.change_qK_idx === nothing ? nothing : string(qK_sym(model)[result.change_qK_idx])

    return Dict(
        "change_kind" => string(result.change_kind),
        "change_label" => string(result.change_label),
        "change_qK" => change_qK,
        "change_qK_idx" => result.change_qK_idx,
        "change_qK_indices" => change_qK_indices,
        "change_qK_signs" => change_qK_signs,
        "change_qK_symbols" => change_qK_symbols,
        "observe_x" => string(x_sym(model)[result.observe_x_idx]),
        "observe_x_idx" => result.observe_x_idx,
        "path_scope" => string(result.path_scope),
        "scope_note" => behavior_scope_note(result.path_scope, result.min_volume_mean),
        "min_volume_mean" => result.min_volume_mean,
        "deduplicate" => result.deduplicate,
        "keep_singular" => result.keep_singular,
        "keep_nonasymptotic" => result.keep_nonasymptotic,
        "compute_volume" => result.compute_volume,
        "feasibility_mode" => string(result.feasibility_mode),
        "total_paths" => result.total_paths,
        "feasible_paths" => result.feasible_paths,
        "included_paths" => result.included_paths,
        "excluded_paths" => result.excluded_paths,
        "exclusion_counts" => Dict(string(k) => v for (k, v) in result.exclusion_counts),
        "paths" => path_dicts,
        "exact_families" => exact_families,
        "motif_families" => motif_families,
        "sources" => collect(get_sources(siso)),
        "sinks" => collect(get_sinks(siso)),
    )
end
