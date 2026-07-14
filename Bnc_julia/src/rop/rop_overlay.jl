# === ROP periodic-table layer (LOCAL OVERLAY) — not from upstream =================
# Quarantined here so the engine base can be re-synced from upstream verbatim.
# Depends only on the public / old_api.jl surface.

#-----------------------------------------------------------------
# Parameter scanning functions
#-----------------------------------------------------------------

"""
    _scan_observable(coeffs, x_linear, output_logspace) -> (value, valid)

Evaluate one linear observable for a parameter scan. A logarithmic observable
is defined only for a finite, strictly positive linear value. Invalid output
domains are represented as `NaN` rather than a finite floor so downstream
consumers cannot plot or rank them as computed evidence.
"""
@inline function _scan_observable(output_coeffs::Vector{Float64},
                                  x_linear::Vector{Float64},
                                  output_logspace::Bool)
    output_value = dot(output_coeffs, x_linear)
    output_valid = isfinite(output_value) && (!output_logspace || output_value > 0.0)
    output_valid || return (NaN, false)
    value = output_logspace ? log10(output_value) : output_value
    return isfinite(value) ? (value, true) : (NaN, false)
end

"""
    scan_parameter_1d(model::Bnc, param_idx::Int, param_range::Vector{Float64},
                      output_coeffs::Vector{Vector{Float64}}, fixed_params::Vector{Float64};
                      input_logspace::Bool=true, output_logspace::Bool=true)
    -> (Vector{Float64}, Matrix{Float64}, Vector{Int})

Scan a single parameter (q or K) and compute output trajectory.

# Arguments
- `model`: Bnc model
- `param_idx`: Index in qK vector (1:d for q, d+1:n for K)
- `param_range`: Values to scan (log10 space if input_logspace=true)
- `output_coeffs`: Vector of coefficient vectors for multiple outputs
- `fixed_params`: Fixed qK values (excluding scanned parameter)

# Returns
- `param_values`: Scanned parameter values
- `output_traj`: Output values (n_points × n_outputs)
- `regimes`: Regime index at each point
- With `track_validity=true`, `valid[i]` is true only when the equilibrium solve
  succeeded and every requested observable at point `i` is in its output
  domain. An invalid observable is `NaN` only in its own output column. Output
  domain failures do not discard a successful equilibrium warm-start seed.
"""
function scan_parameter_1d(model::Bnc, param_idx::Int, param_range::Vector{Float64},
                           output_coeffs::Vector{Vector{Float64}}, fixed_params::Vector{Float64};
                           input_logspace::Bool=true, output_logspace::Bool=true,
                           track_validity::Bool=false, warm_start::Bool=true,
                           cancel_check=_NO_CANCEL_CHECK)
    cancel_check()
    # Build the mutable regime caches before sampling so cancellation can reach
    # exact candidate enumeration instead of first entering it indirectly from
    # `assign_vertex_qK` inside a broad error-catching block.
    find_all_regimes!(model; cancel_check=cancel_check)
    cancel_check()
    n_points = length(param_range)
    n_outputs = length(output_coeffs)

    output_traj = Matrix{Float64}(undef, n_points, n_outputs)
    regimes = Vector{Int}(undef, n_points)
    # Per-point numerical validity. `qK2x` previously returned a non-converged
    # ODE state silently; with `track_validity=true` the caller gets a Bool
    # vector that also rejects any requested output outside its declared domain.
    # The status Ref is allocated once and reused per point.
    valid = track_validity ? Vector{Bool}(undef, n_points) : Bool[]
    st = track_validity ? Ref(:success) : nothing

    # Warm-start continuation. Adjacent grid points differ in one qK coordinate by a
    # tiny step, so the previous converged equilibrium is a near-perfect homotopy start
    # — instead of re-tracing the whole path from the fixed canonical anchor every
    # point (which re-factorizes the sparse Jacobian on every ODE step). The qK->x map
    # is single-valued (manifold L*x=q, N*logx=logK; [L*diag(x);N] nonsingular), so this
    # is exact, not an approximation — empirically agreeing with the cold solve to ~1e-12
    # incl. across all regime transitions of the prozone/hook fixture. Guard: re-anchor
    # (cold solve) whenever the regime index changes, is unassignable, or the previous
    # solve did not converge, so we never continue across a regime boundary or off a
    # bad point. `warm_start=false` restores the legacy always-from-anchor behaviour.
    prev_logx = nothing
    prev_logqK = Float64[]
    prev_regime = -1

    for (i, param_val) in enumerate(param_range)
        cancel_check()
        # Construct full qK vector
        qK = copy(fixed_params)
        insert!(qK, param_idx, param_val)

        # Assign regime FIRST (a pure function of qK, not x — cheap, vertex data cached):
        # it gates warm-start and is the value stored in `regimes[i]`. `return_idx=true`
        # is REQUIRED — the default returns a permutation Vector, which cannot be stored
        # into this Vector{Int} slot, so the previous unqualified call always threw and
        # the catch silently wrote 0 (which dead-ended the phenotyper's regime-transition
        # auto-bracketing). Now it stores the real vertex index.
        regime_i = try
            assign_vertex_qK(model, qK; input_logspace=input_logspace, return_idx=true)
        catch
            0  # genuinely unassignable point
        end
        regimes[i] = regime_i

        # Compute x — warm-start only within a single regime, else re-anchor.
        track_validity && (st[] = :success)
        use_warm = warm_start && prev_logx !== nothing && regime_i != 0 && regime_i == prev_regime
        x = if use_warm
            qK2x(model, qK; input_logspace=input_logspace, output_logspace=output_logspace,
                 startlogx=prev_logx, startlogqK=prev_logqK, status=st)
        else
            qK2x(model, qK; input_logspace=input_logspace, output_logspace=output_logspace,
                 status=st)
        end
        solver_valid = !track_validity || (st[] === :success)

        # Invalid solves are explicit when validity tracking is requested; do
        # not expose their terminal ODE state as a trustworthy sample. Solver
        # validity and observable-domain validity are deliberately separate: a
        # successful equilibrium remains a useful warm-start even when one
        # requested logarithmic expression is non-positive. Compute exp10(x)
        # once per point rather than once per output expression.
        outputs_valid = solver_valid
        if track_validity && !solver_valid
            output_traj[i, :] .= NaN
        else
            x_linear = output_logspace ? exp10.(x) : x
            for j in 1:n_outputs
                value, output_valid = _scan_observable(
                    output_coeffs[j], x_linear, output_logspace)
                output_traj[i, j] = value
                outputs_valid &= output_valid
            end
        end
        track_validity && (valid[i] = solver_valid && outputs_valid)

        # Cache this point as the next warm-start seed (only if it converged); the seed
        # is always kept in log space regardless of the caller's output_logspace flag.
        if solver_valid
            prev_logx = output_logspace ? x : log10.(max.(x, 1e-100))
            prev_logqK = input_logspace ? qK : log10.(qK)
            prev_regime = regime_i
        else
            prev_logx = nothing  # force a cold re-anchor at the next point
            prev_regime = -1
        end
    end

    cancel_check()
    return track_validity ? (param_range, output_traj, regimes, valid) :
                            (param_range, output_traj, regimes)
end


"""
    scan_parameter_2d(model::Bnc, param_idx1::Int, param_idx2::Int,
                      param_range1::Vector{Float64}, param_range2::Vector{Float64},
                      output_coeffs::Vector{Float64}, fixed_params::Vector{Float64};
                      input_logspace::Bool=true, output_logspace::Bool=true)
    -> (Vector{Float64}, Vector{Float64}, Matrix{Float64}, Matrix{Int})

Scan two parameters and compute output heatmap.

# Returns
- `param1_values`: First parameter values
- `param2_values`: Second parameter values
- `output_grid`: Output values (n1 × n2)
- `regime_grid`: Regime indices (n1 × n2)
- With `track_validity=true`, a grid point is valid only when the equilibrium
  solve succeeds and the requested observable is finite (and strictly positive
  for logarithmic output). Observable-domain failures are `NaN` gaps but do not
  prevent a successful equilibrium from seeding the next point in the row.
"""
function scan_parameter_2d(model::Bnc, param_idx1::Int, param_idx2::Int,
                           param_range1::Vector{Float64}, param_range2::Vector{Float64},
                           output_coeffs::Vector{Float64}, fixed_params::Vector{Float64};
                           input_logspace::Bool=true, output_logspace::Bool=true,
                           warm_start::Bool=true, track_validity::Bool=false)
    n1, n2 = length(param_range1), length(param_range2)

    output_grid = Matrix{Float64}(undef, n1, n2)
    regime_grid = Matrix{Int}(undef, n1, n2)
    valid = track_validity ? Matrix{Bool}(undef, n1, n2) : falses(0, 0)

    # Do the one-time mutable regime/affine initialization before row workers
    # start reading the model concurrently.
    find_all_regimes!(model)

    # Warm-start continuation ALONG EACH ROW (fixed param1, sweeping param2): the inner-j
    # neighbour is a near-perfect homotopy start, exactly as in scan_parameter_1d, and the
    # qK->x map is single-valued so this is exact, not an approximation. Rows are the
    # @threads axis and are independent, so each thread keeps its own continuation state.
    # Re-anchor (cold solve) at the start of a row, on a regime change, or after a
    # non-converged solve, so continuation never crosses a regime boundary or a bad point.
    # warm_start=false restores the legacy always-from-anchor behaviour.
    Threads.@threads for i in 1:n1
        prev_logx = nothing
        prev_logqK = Float64[]
        prev_regime = -1
        st = Ref(:success)
        for j in 1:n2
            # Construct full qK vector
            qK = copy(fixed_params)
            if param_idx1 < param_idx2
                insert!(qK, param_idx1, param_range1[i])
                insert!(qK, param_idx2, param_range2[j])
            else
                insert!(qK, param_idx2, param_range2[j])
                insert!(qK, param_idx1, param_range1[i])
            end

            # Regime first (pure function of qK) — gates warm-start and fills regime_grid.
            regime_ij = try
                assign_vertex_qK(model, qK; input_logspace=input_logspace, return_idx=true)
            catch
                0
            end
            regime_grid[i, j] = regime_ij

            st[] = :success
            use_warm = warm_start && prev_logx !== nothing && regime_ij != 0 && regime_ij == prev_regime
            x = if use_warm
                qK2x(model, qK; input_logspace=input_logspace, output_logspace=output_logspace,
                     startlogx=prev_logx, startlogqK=prev_logqK, status=st)
            else
                qK2x(model, qK; input_logspace=input_logspace, output_logspace=output_logspace, status=st)
            end

            solver_valid = st[] === :success

            # Extract output using linear combination
            output_valid = solver_valid
            if track_validity && !solver_valid
                output_grid[i, j] = NaN
            else
                x_linear = output_logspace ? exp10.(x) : x
                output_grid[i, j], output_valid = _scan_observable(
                    output_coeffs, x_linear, output_logspace)
            end
            track_validity && (valid[i, j] = solver_valid && output_valid)

            # Cache as the next-in-row warm-start seed (log space) only if it converged.
            if solver_valid
                prev_logx = output_logspace ? x : log10.(max.(x, 1e-100))
                prev_logqK = input_logspace ? qK : log10.(qK)
                prev_regime = regime_ij
            else
                prev_logx = nothing
                prev_regime = -1
            end
        end
    end

    return track_validity ?
        (param_range1, param_range2, output_grid, regime_grid, valid) :
        (param_range1, param_range2, output_grid, regime_grid)
end


#------------------------------------------------------------
# Expression parser for linear combinations
#------------------------------------------------------------

const MAX_LINEAR_COMBINATION_ABS_COEFFICIENT = sqrt(floatmax(Float64))

"""
    parse_linear_combination(model::Bnc, expr::String) -> Vector{Float64}

Parse a linear combination expression and return coefficient vector.

# Examples
- "C_ES" → [0, 0, 1, 0, ...] (if C_ES is at index 3)
- "2*C_ES + C_EP" → [0, 0, 2, 1, ...] (if C_ES at 3, C_EP at 4)
- "C_ES + C_EP + C_EI" → [0, 0, 1, 1, 1, ...] (sum of products)

# Supported syntax
- Species names: must match model.x_sym
- Operators: +, -, *
- Numbers: integers and floats (e.g., 2, 0.5, 1.5)
- Whitespace: ignored

# Returns
- Vector{Float64} of length model.n with coefficients
"""
function parse_linear_combination(model::Bnc, expr::String)::Vector{Float64}
    # Remove all whitespace
    expr = replace(expr, r"\s+" => "")

    # Check for empty expression
    isempty(expr) && error("Empty expression")

    # Initialize coefficient vector
    coeffs = zeros(Float64, model.n)

    # Split by + and - while keeping the operators
    terms = split_with_operators(expr)

    for term in terms
        # Parse each term: [sign][coeff]*species or [sign]species
        sign, coeff, species = parse_term(term)

        # Find species index
        idx = locate_sym_x(model, Symbol(species))
        idx === nothing && error("Unknown species: $species")

        # Add to coefficient vector
        coeffs[idx] += sign * coeff
        isfinite(coeffs[idx]) || error(
            "Linear combination coefficient is non-finite after accumulation for $species")
    end

    return coeffs
end

"""
    split_with_operators(expr::String) -> Vector{String}

Split expression by + and -, keeping track of signs.
"""
function split_with_operators(expr::String)::Vector{String}
    terms = String[]
    current = ""
    sign = "+"

    for c in expr
        if c == '+' || c == '-'
            if !isempty(current)
                push!(terms, sign * current)
                current = ""
            end
            sign = string(c)
        else
            current *= c
        end
    end

    if !isempty(current)
        push!(terms, sign * current)
    end

    isempty(terms) && error("Invalid expression: no terms found")

    return terms
end

"""
    parse_term(term::String) -> (Float64, Float64, String)

Parse a single term: [+/-][number]*species or [+/-]species.

# Returns
- Tuple (sign, coefficient, species_name)
"""
function parse_term(term::String)::Tuple{Float64, Float64, String}
    # Parse: [+/-][number]*species or [+/-]species
    sign = term[1] == '-' ? -1.0 : 1.0
    term = term[1] in ['+', '-'] ? term[2:end] : term

    isempty(term) && error("Invalid term: empty after sign")

    if '*' in term
        parts = split(term, '*')
        length(parts) == 2 || error("Invalid term: $term (multiple * operators)")
        try
            coeff = parse(Float64, parts[1])
            isfinite(coeff) || error("Coefficient must be finite")
            abs(coeff) <= MAX_LINEAR_COMBINATION_ABS_COEFFICIENT ||
                error("Coefficient magnitude is too large")
            species = parts[2]
            isempty(species) && error("Invalid term: $term (no species after *)")
            return sign, coeff, species
        catch e
            error("Invalid term: $term (cannot parse coefficient)")
        end
    else
        coeff = 1.0
        species = term
        return sign, coeff, species
    end
end


#-----------------------------------------------------------------
# _dedup scalar overload (OLD-faithful; overrides upstream SISO.jl)
#-----------------------------------------------------------------
#
# fix(resync): OLD-faithful leading-NaN _dedup. Upstream SISO.jl's scalar _dedup
# seeds `out` with `ord_path[1]` and hard-asserts `!isnan(last_out)`, which throws
# on RO profiles that begin with NaN (singular/asymptotic regimes — ~5% of d>=3
# networks). The crash fires inside the `@threads` loop of get_RO_paths and blocks
# the atlas rebuild (compute_volume=true asserts before the volume branch). The OLD
# engine (regime_graphs.jl) handled leading NaN gracefully: it seeds at the first
# finite value, prepends a single NaN token when the profile did not start finite,
# and collapses consecutive duplicates (with one NaN token inserted across each gap
# between distinct finite values). This overlay method is include()d AFTER SISO.jl,
# so it overrides upstream for ALL callers (get_RO_paths included). It reproduces
# OLD's OUTPUT exactly on every non-NaN-leading profile and restores OLD's lenient
# behavior on NaN-leading profiles. A "method overwritten" warning at load is
# expected and benign.
function _dedup(ord_path::AbstractVector{T})::Vector{T} where {T<:Real}
    isempty(ord_path) && return T[]

    first_finite_idx = findfirst(x -> !isnan(x), ord_path)
    if isnothing(first_finite_idx)
        return T[T(NaN)]
    end

    out = T[]
    if first_finite_idx > firstindex(ord_path)
        push!(out, T(NaN))
    end

    push!(out, ord_path[first_finite_idx])
    pending_nan = false
    last_out = out[end]

    for x in @view ord_path[(first_finite_idx+1):end]
        if isnan(x)
            pending_nan = true
            continue
        end
        if x != last_out
            if pending_nan
                push!(out, T(NaN))
                pending_nan = false
            end
            push!(out, x)
            last_out = x
        else
            pending_nan = false
        end
    end
    return out
end

#-----------------------------------------------------------------
# _dedup vector-of-vectors overload
#-----------------------------------------------------------------

function _dedup(ord_path::AbstractVector{<:AbstractVector{<:Real}})::Vector{Vector{Float64}}
    isempty(ord_path) && return Vector{Float64}[]

    first_finite_idx = findfirst(x -> !_all_nan(x), ord_path)
    if isnothing(first_finite_idx)
        return [fill(NaN, length(ord_path[1]))]
    end

    first_vec = Float64[Float64(x) for x in ord_path[first_finite_idx]]
    out = Vector{Float64}[]
    if first_finite_idx > firstindex(ord_path)
        push!(out, fill(NaN, length(first_vec)))
    end
    push!(out, first_vec)
    pending_nan = false
    last_out = first_vec

    for x_raw in @view ord_path[(first_finite_idx+1):end]
        x = Float64[Float64(v) for v in x_raw]
        if _all_nan(x)
            pending_nan = true
            continue
        end
        if x != last_out
            if pending_nan
                push!(out, fill(NaN, length(x)))
                pending_nan = false
            end
            push!(out, x)
            last_out = x
        else
            pending_nan = false
        end
    end
    return out
end
