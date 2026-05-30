# phenotype_pipeline.jl — Latent Atlas phenotyper, v0.3.0 (Phase 1).
#
# Implements the reference phenotyper of the roadmap
# (doc/Biocircuits_Latent_Atlas_Roadmap.tex, "Phenotype Measurement"):
#
#   * per-curve, deterministic metrics at a fixed parameter draw θ, and
#   * distributional labels over a parameter prior Π:
#       shape_support = fraction of the prior exhibiting the qualitative shape
#                     = the distributional robustness number,
#     plus conditional quantiles of each metric over the in-shape draws.
#
# A `PhenotyperPolicy` pins the vocabulary, tolerances, draw count K, sampler +
# seed, quantile level α and grid policy, so the same network under the same
# `phenotyper_version` yields identical labels (Phase-1 determinism requirement).
#
# v0.3.0 changes (review fixes):
#   * Real `thresholded_activation` (leading-flat shoulder + steepness) and
#     `bandpass_with_plateau` (plateau-width) gates — they were previously
#     byte-identical to their base class, so the benchmark recorded false passes.
#   * Low-discrepancy (Halton) quasi-Monte-Carlo sampler is the default; plain
#     fixed-seed Monte-Carlo remains available via `sampler = :mc`. (The doc's
#     "fixed-seed low-discrepancy" language now matches the code; Sobol is a
#     drop-in future swap.)
#   * Parameter prior Π is a first-class, serializable object (`prior_descriptor`)
#     and the `kd_profile` Kd semantics (`weak_fraction_min`, `allow_strong_outliers`)
#     are actually honored — `reshape_prior` warns on any kd_profile key it cannot.
#   * `output_fold_change_log10` is flagged `floor_limited` (and reported NaN) when
#     the low asymptote is set by the output floor rather than a real interior
#     minimum, so a floored curve no longer manufactures ~10–12 decades of range.
#   * `input_operating_range_log10` is direction-aware (works for repression).
#   * `dominant_shape` tie-break is deterministic (fixed class order, not Dict hash).
#   * Non-converged equilibrium solves (engine `track_validity`) dilute shape_support
#     like a failed draw instead of folding a bogus point into metrics.
#
# Depends only on the exported Bnc engine surface, so it is includable both
# inside BiocircuitsExplorerBackend and standalone for tests:
#   scan_parameter_1d (now with `track_validity`), parse_linear_combination, locate_sym_qK.

module PhenotypePipeline

using BindingAndCatalysis
using Random
using Statistics

export PhenotyperPolicy, ParameterPrior, PointMass, LogUniform, LogNormal10, KdProfile,
       phenotype, phenotype_profile, classify_shape, reshape_prior, prior_descriptor,
       per_curve_metrics, response_order, scan_curve,
       PHENOTYPE_VOCAB_V0

# ── Phenotype Vocabulary v0 ───────────────────────────────────────────────────
# Each class is recognised by the compressed sign sequence of ρ(u) (rise=+1,
# fall=-1, flat dropped) plus the extra gates noted. `bandpass_like` is a
# user-language alias resolved to `biphasic_peak` + a prominence/baseline test;
# it is intentionally not its own verifier label.
const PHENOTYPE_VOCAB_V0 = (
    monotone_activation   = "rise to high plateau; sign seq [+]",
    monotone_repression   = "fall to low floor; sign seq [-]",
    thresholded_activation= "flat-low shoulder then sharp rise; [+] with leading flat run ≥ thresh_flat_min_frac and rise_slope ≥ thresh_slope_min",
    biphasic_peak         = "canonical peak; sign seq [+,-] with prominence ≥ P_min",
    bandpass_with_plateau = "biphasic_peak with a broad high region; plateau_width_log10_input ≥ plateau_min",
    biphasic_valley       = "valley/notch; sign seq [-,+]",
)

# ── Parameter prior (log10 space, matching qK convention) ─────────────────────
abstract type ParamPrior end
struct PointMass   <: ParamPrior; logval::Float64; end                 # fixed log10 value
struct LogUniform  <: ParamPrior; lo::Float64; hi::Float64; end        # log10 ~ U[lo,hi]
struct LogNormal10 <: ParamPrior; mu::Float64; sigma::Float64; end     # log10 ~ Normal(mu,σ)

# Inverse-CDF maps (u ∈ [0,1) → a log10 value); used by BOTH the QMC and MC paths.
_invcdf(p::PointMass,   ::Float64) = p.logval
_invcdf(p::LogUniform,  u::Float64) = p.lo + (p.hi - p.lo) * u
_invcdf(p::LogNormal10, u::Float64) = p.mu + p.sigma * _norm_quantile(clamp(u, 1e-9, 1 - 1e-9))
_ndims(::PointMass) = 0
_ndims(::ParamPrior) = 1

"""
    KdProfile

Honored `kd_profile` semantics for the MVP `mostly_weak` mode: a fraction
`weak_fraction_min` of the Kd's are drawn weak (log10 Kd ∈ [weak_lo, hi]) and at
most `allow_strong_outliers` are drawn strong (∈ [lo, weak_lo)). Per draw the
strong count is chosen in `0:min(allow_strong_outliers, floor((1-weak_fraction_min)·r))`
and the strong indices are the ones with the largest sampler coordinate, so the
mixture is deterministic under the sampler + seed.
"""
Base.@kwdef struct KdProfile
    lo::Float64 = -3.0          # strong-Kd lower bound (log10)
    weak_lo::Float64 = 0.0      # weak/strong split (log10): weak ≥ weak_lo
    hi::Float64 = 3.0           # weak-Kd upper bound (log10)
    weak_fraction_min::Float64 = 0.75
    allow_strong_outliers::Int = 0
end

"""
    ParameterPrior

Prior Π over the qK vector. `per_symbol` overrides individual totals/Kd by their
qK symbol (e.g. `:tA`, `:Kd1`); anything unset falls back to `default_total`
(indices 1:d) or `default_kd` (indices d+1:n). If `kd_profile` is set it governs
the Kd block (overriding `default_kd` for non-`per_symbol` Kd indices). All values
are log10. This object is serialized into the dataset manifest (`prior_descriptor`)
so every label records the prior it was measured against.
"""
Base.@kwdef struct ParameterPrior
    per_symbol::Dict{Symbol,ParamPrior} = Dict{Symbol,ParamPrior}()
    default_total::ParamPrior = PointMass(0.0)
    default_kd::ParamPrior    = LogUniform(-3.0, 3.0)
    kd_profile::Union{Nothing,KdProfile} = nothing
end

# Reshape the Kd portion of a prior from a behavior_spec `kd_profile` block. v0
# honors `mostly_weak` with its `weak_fraction_min` / `allow_strong_outliers`
# fractional semantics; it WARNS on any mode or key it does not implement so a
# spec is never silently verified against the wrong prior.
function reshape_prior(prior::ParameterPrior, kd_profile)
    kd_profile === nothing && return prior
    mode = String(get(kd_profile, "mode", ""))
    known_keys = Set(["mode", "log10_kd_min", "log10_kd_max", "weak_split_log10",
                      "weak_fraction_min", "allow_strong_outliers"])
    for k in keys(kd_profile)
        String(k) in known_keys || @warn "reshape_prior: ignoring unhonored kd_profile key" key=String(k)
    end
    if mode == "mostly_weak"
        lo      = Float64(get(kd_profile, "log10_kd_min", -3.0))
        hi      = Float64(get(kd_profile, "log10_kd_max", 3.0))
        weak_lo = Float64(get(kd_profile, "weak_split_log10", max(lo, 0.0)))
        wf      = Float64(get(kd_profile, "weak_fraction_min", 0.75))
        outl    = Int(get(kd_profile, "allow_strong_outliers", 0))
        kp = KdProfile(lo = lo, weak_lo = weak_lo, hi = hi,
                       weak_fraction_min = wf, allow_strong_outliers = outl)
        return ParameterPrior(prior.per_symbol, prior.default_total, prior.default_kd, kp)
    elseif !isempty(mode)
        @warn "reshape_prior: unhonored kd_profile mode (using base prior)" mode=mode
    end
    return prior
end

# JSON-able description of the prior (for the dataset manifest / provenance).
_prior_kind(p::PointMass)   = Dict("kind"=>"point_mass", "logval"=>p.logval)
_prior_kind(p::LogUniform)  = Dict("kind"=>"log_uniform", "lo"=>p.lo, "hi"=>p.hi)
_prior_kind(p::LogNormal10) = Dict("kind"=>"log_normal10", "mu"=>p.mu, "sigma"=>p.sigma)
function prior_descriptor(prior::ParameterPrior)
    d = Dict{String,Any}(
        "default_total" => _prior_kind(prior.default_total),
        "default_kd"    => _prior_kind(prior.default_kd),
        "per_symbol"    => Dict(String(k)=>_prior_kind(v) for (k,v) in prior.per_symbol),
    )
    if prior.kd_profile !== nothing
        kp = prior.kd_profile
        d["kd_profile"] = Dict("mode"=>"mostly_weak", "lo"=>kp.lo, "weak_lo"=>kp.weak_lo,
                               "hi"=>kp.hi, "weak_fraction_min"=>kp.weak_fraction_min,
                               "allow_strong_outliers"=>kp.allow_strong_outliers)
    end
    return d
end

# ── Policy (pins determinism + the phenotyper_version) ────────────────────────
Base.@kwdef struct PhenotyperPolicy
    version::String   = "bne-phenotyper/v0.3.0"
    # grid
    npoints::Int      = 161
    u_lo::Float64     = -6.0          # initial bracket, log10 input total
    u_hi::Float64     = 6.0
    # auto-bracketing: focus the window on the regime-transition (active) region
    auto_bracket::Bool         = true
    recon_lo::Float64          = -8.0   # reconnaissance scan window (log10 input)
    recon_hi::Float64          = 8.0
    bracket_npoints::Int       = 61     # recon scan resolution
    bracket_margin::Float64    = 1.5    # log10-input padding around the active region
    bracket_max_width::Float64 = 8.0    # hard cap on final window width
    bracket_min_width::Float64 = 2.0    # floor on final window width
    # metric tolerances
    eps_pl::Float64       = 0.1       # plateau: y ≥ (1-eps_pl)·y_peak
    P_min::Float64        = 0.15      # min peak prominence for a genuine peak
    rho_zero::Float64     = 0.05      # |ρ| < rho_zero treated as flat (sign 0)
    output_floor::Float64 = 1e-12     # floor for fold-change ratios
    plateau_min::Float64       = 0.5  # bandpass_with_plateau: min plateau width (log10 input)
    thresh_flat_min_frac::Float64 = 0.25  # thresholded_activation: min leading-flat input fraction
    thresh_slope_min::Float64     = 1.0   # thresholded_activation: min rising response order
    # distributional
    sampler::Symbol = :halton         # :halton (low-discrepancy QMC) | :mc (Monte-Carlo)
    K::Int          = 64              # prior draws
    seed::Int       = 1234
    alpha::Float64  = 0.10            # lower-quantile level for conditional stats
end

# ── Low-discrepancy / inverse-normal helpers ──────────────────────────────────
# Halton radical-inverse (dependency-free QMC). Index i≥1 → value in (0,1).
function _halton(i::Int, base::Int)
    f = 1.0; r = 0.0; n = i
    while n > 0
        f /= base
        r += f * (n % base)
        n = div(n, base)
    end
    return r
end

# First `m` primes (m is small: one per varying qK dimension + a few spares).
function _primes(m::Int)
    m <= 0 && return Int[]
    ps = Int[2]; c = 3
    while length(ps) < m
        isp = true
        for p in ps
            p*p > c && break
            (c % p == 0) && (isp = false; break)
        end
        isp && push!(ps, c)
        c += 2
    end
    return ps
end

# Acklam's rational approximation of the inverse standard-normal CDF (Φ⁻¹).
function _norm_quantile(p::Float64)
    p <= 0.0 && return -Inf
    p >= 1.0 && return Inf
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
          1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
          6.680131188771972e+01, -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00,  4.374664141464968e+00,  2.938163982698783e+00)
    d = ( 7.784695709041462e-03,  3.224671290700398e-01,  2.445134137142996e+00,
          3.754408661907416e+00)
    plow = 0.02425; phigh = 1 - plow
    if p < plow
        q = sqrt(-2*log(p))
        return (((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) /
               ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1)
    elseif p <= phigh
        q = p - 0.5; r = q*q
        return (((((a[1]*r+a[2])*r+a[3])*r+a[4])*r+a[5])*r+a[6])*q /
               (((((b[1]*r+b[2])*r+b[3])*r+b[4])*r+b[5])*r+1)
    else
        q = sqrt(-2*log(1-p))
        return -(((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) /
                ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1)
    end
end

# ── qK helpers ────────────────────────────────────────────────────────────────
qK_symbols(model) = Symbol.(string.(vcat(model.q_sym, model.K_sym)))  # length n

# Resolve the per-index prior for the standard (non-kd_profile) path.
function _index_priors(model, prior::ParameterPrior)
    names = qK_symbols(model)
    return ParamPrior[ get(prior.per_symbol, names[i],
                           i <= model.d ? prior.default_total : prior.default_kd)
                       for i in 1:model.n ]
end

"""
    draw_log_qK(model, prior, k, policy; rng) -> Vector{Float64}

Draw `k`-th log10 qK vector. With `policy.sampler == :halton` the totals/Kd are a
low-discrepancy point mapped through each prior's inverse-CDF (deterministic by
index, no RNG). With `:mc`, or whenever a `kd_profile` mixture is active, the
draw uses `rng` (seeded once per phenotype call). Both paths are deterministic
under the same policy+seed.
"""
function draw_log_qK(model, prior::ParameterPrior, k::Int, policy::PhenotyperPolicy, rng::AbstractRNG)
    n, d = model.n, model.d
    out = Vector{Float64}(undef, n)
    priors = _index_priors(model, prior)

    # Totals + (when no kd_profile) Kd via the chosen sampler.
    if policy.sampler == :halton
        primes = _primes(n)
        @inbounds for i in 1:n
            u = _halton(k + policy.seed, primes[i])     # seed shifts the QMC start index
            out[i] = _invcdf(priors[i], u)
        end
    else
        @inbounds for i in 1:n
            out[i] = _invcdf(priors[i], rand(rng))
        end
    end

    # kd_profile mixture (always RNG-driven so the discrete weak/strong partition
    # is well-defined); overrides the Kd block (indices d+1:n) drawn above.
    if prior.kd_profile !== nothing && n > d
        kp = prior.kd_profile
        r = n - d
        max_strong = min(kp.allow_strong_outliers, floor(Int, (1 - kp.weak_fraction_min) * r))
        max_strong = max(0, max_strong)
        n_strong = max_strong == 0 ? 0 : rand(rng, 0:max_strong)
        # the n_strong Kd indices with the largest uniform draw become outliers
        u = rand(rng, r)
        order = sortperm(u; rev = true)
        strong = Set(order[1:n_strong])
        @inbounds for j in 1:r
            idx = d + j
            out[idx] = j in strong ?
                kp.lo + (kp.weak_lo - kp.lo) * rand(rng) :     # strong: [lo, weak_lo)
                kp.weak_lo + (kp.hi - kp.weak_lo) * rand(rng)  # weak:   [weak_lo, hi]
        end
    end
    return out
end

# ── Curve + response order ────────────────────────────────────────────────────
"""
    scan_curve(model, input_idx, coeffs, log_qK, u_lo, u_hi, npoints)
        -> (u::Vector, ylog::Vector, regimes::Vector{Int}, valid::Vector{Bool})

Log-log dose-response over the input total at qK index `input_idx`, holding the
remaining qK fixed at `log_qK`. `ylog` is log10 of the observable; `regimes` is
the ROP regime index at each grid point (0 = unassigned); `valid[i]` is false
when the equilibrium solve at that point did not converge (so callers can discard
unreliable curves instead of trusting a non-converged state).
"""
function scan_curve(model, input_idx::Int, coeffs::Vector{Vector{Float64}},
                    log_qK::Vector{Float64}, u_lo::Real, u_hi::Real, npoints::Int)
    fixed = deleteat!(copy(log_qK), input_idx)          # length n-1
    u = collect(range(Float64(u_lo), Float64(u_hi), length=npoints))
    _, traj, regimes, valid = scan_parameter_1d(model, input_idx, u, coeffs, fixed;
                                                input_logspace=true, output_logspace=true,
                                                track_validity=true)
    return u, traj[:, 1], regimes, valid
end

"ρ(u) = dlog y / dlog u via central differences; same length as `u`."
function response_order(u::Vector{Float64}, ylog::Vector{Float64})
    n = length(u)
    rho = zeros(Float64, n)
    n < 2 && return rho
    @inbounds for i in 1:n
        if i == 1
            rho[i] = (ylog[2] - ylog[1]) / (u[2] - u[1])
        elseif i == n
            rho[i] = (ylog[n] - ylog[n-1]) / (u[n] - u[n-1])
        else
            rho[i] = (ylog[i+1] - ylog[i-1]) / (u[i+1] - u[i-1])
        end
    end
    return rho
end

# Compressed sign sequence: deadband to {-1,0,+1}, drop zeros, collapse repeats.
function sign_sequence(rho::Vector{Float64}, dz::Float64)
    out = Int[]
    @inbounds for r in rho
        s = abs(r) < dz ? 0 : (r > 0 ? 1 : -1)
        s == 0 && continue
        (isempty(out) || out[end] != s) && push!(out, s)
    end
    return out
end

# Leading-flat input fraction: width of the initial run where |ρ| < dz, as a
# fraction of the total input span. Used to recognise a thresholded shoulder.
function _leading_flat_frac(u::Vector{Float64}, rho::Vector{Float64}, dz::Float64)
    n = length(u)
    n < 2 && return 0.0
    i = 1
    @inbounds while i <= n && abs(rho[i]) < dz
        i += 1
    end
    i <= 1 && return 0.0
    flat_to = min(i, n)
    span = u[end] - u[1]
    span <= 0 && return 0.0
    return (u[flat_to] - u[1]) / span
end

# Auto-bracket: focus the scan window on the regime-transition (active) region,
# padded and width-capped. Falls back to the 5%–95% output-swing span if no
# regime change is seen in the recon window.
function auto_bracket(model, input_idx, coeffs, log_qK, policy::PhenotyperPolicy)
    lo, hi = policy.recon_lo, policy.recon_hi
    local u, ylog, regimes
    try
        u, ylog, regimes, _ = scan_curve(model, input_idx, coeffs, log_qK, lo, hi, policy.bracket_npoints)
    catch
        return (policy.u_lo, policy.u_hi)
    end

    t_lo = NaN; t_hi = NaN
    @inbounds for i in 2:length(regimes)
        if regimes[i] != regimes[i-1]
            isnan(t_lo) && (t_lo = u[i-1])
            t_hi = u[i]
        end
    end

    if isnan(t_lo)                                   # no regime change → output-swing fallback
        y = exp10.(ylog); ylo = y[1]; yhi = y[end]; rng = yhi - ylo
        abs(rng) < 1e-12 && return (policy.u_lo, policy.u_hi)   # genuinely flat
        band_lo = ylo + 0.05 * rng; band_hi = ylo + 0.95 * rng
        lb, ub = min(band_lo, band_hi), max(band_lo, band_hi)
        idxs = findall(v -> lb <= v <= ub, y)
        isempty(idxs) && return (policy.u_lo, policy.u_hi)
        t_lo = u[first(idxs)]; t_hi = u[last(idxs)]
    end

    c_lo = t_lo - policy.bracket_margin
    c_hi = t_hi + policy.bracket_margin
    mid = (c_lo + c_hi) / 2
    w = c_hi - c_lo
    if w > policy.bracket_max_width
        c_lo, c_hi = mid - policy.bracket_max_width / 2, mid + policy.bracket_max_width / 2
    elseif w < policy.bracket_min_width
        c_lo, c_hi = mid - policy.bracket_min_width / 2, mid + policy.bracket_min_width / 2
    end
    return (c_lo, c_hi)
end

# ── Per-curve metrics (deterministic, given one parameter draw) ───────────────
function _plateau_width(u, y, ipeak, thr)
    n = length(u)
    l = ipeak; r = ipeak
    @inbounds while l > 1 && y[l-1] >= thr; l -= 1; end
    @inbounds while r < n && y[r+1] >= thr; r += 1; end
    return u[r] - u[l]
end

# log10-input width between the `lo_frac` and `hi_frac` output-swing crossings on
# the principal monotone flank. Direction-aware: for an activation curve the flank
# runs from the start up to the peak; for a repression curve it runs from the start
# down to the trough — so repression no longer returns a degenerate 0.
function _operating_range(u, y, iend, lo_frac, hi_frac)
    n = length(u)
    iend <= 1 && return 0.0
    y0 = y[1]; yE = y[iend]
    rng = yE - y0
    abs(rng) < 1e-12 && return 0.0
    target_lo = y0 + lo_frac * rng
    target_hi = y0 + hi_frac * rng
    cross(t) = begin
        uc = NaN
        @inbounds for i in 1:(iend-1)
            if (y[i] - t) * (y[i+1] - t) <= 0 && y[i+1] != y[i]
                uc = u[i] + (t - y[i]) * (u[i+1] - u[i]) / (y[i+1] - y[i]); break
            end
        end
        uc
    end
    a = cross(target_lo); b = cross(target_hi)
    (isnan(a) || isnan(b)) && return 0.0
    return abs(b - a)
end

"""
    per_curve_metrics(u, ylog, rho, policy) -> NamedTuple

Deterministic shape metrics for one curve. Ratio metrics use linear y=10^ylog;
slope metrics use ρ (already a log-log derivative). Fields back the BehaviorSpec
shape_preferences of the same names. `output_fold_change_log10` is `NaN` and
`output_fold_change_floor_limited=true` when the low value is set by the output
floor rather than a real interior minimum.
"""
function per_curve_metrics(u::Vector{Float64}, ylog::Vector{Float64},
                           rho::Vector{Float64}, policy::PhenotyperPolicy)
    y = exp10.(ylog)
    n = length(u)
    ylo = y[1]; yhi = y[end]
    ipeak = argmax(y); itrough = argmin(y)
    ypeak = y[ipeak]; ytrough = y[itrough]
    upeak = u[ipeak]
    span = ypeak - ytrough
    denom = span <= 0 ? eps() : span

    peak_prominence = (ypeak - max(ylo, yhi)) / denom
    baseline_return = (yhi - ylo) / denom

    rise_mask = u .< upeak
    fall_mask = u .> upeak
    rise_slope = any(rise_mask) ? maximum(rho[rise_mask]; init=0.0) : 0.0
    fall_slope = any(fall_mask) ? maximum(abs.(rho[fall_mask]); init=0.0) : 0.0
    asymmetry  = rise_slope > 0 ? fall_slope / rise_slope : 0.0

    raw_ymin = minimum(y); ymax = maximum(y)
    floor_limited = raw_ymin < policy.output_floor
    output_fold_change_log10 = floor_limited ? NaN : log10(ymax / max(raw_ymin, policy.output_floor))

    plateau_width_log10_input = _plateau_width(u, y, ipeak, (1 - policy.eps_pl) * ypeak)

    # principal monotone flank: rising curves end at the peak, falling at the trough
    op_end = (yhi >= ylo) ? ipeak : itrough
    input_operating_range_log10 = _operating_range(u, y, op_end, 0.1, 0.9)

    leading_flat_frac = _leading_flat_frac(u, rho, policy.rho_zero)
    seq = sign_sequence(rho, policy.rho_zero)

    return (; ylo, yhi, ypeak, ytrough, upeak,
            peak_prominence, baseline_return,
            rise_slope, fall_slope, asymmetry,
            output_fold_change_log10, output_fold_change_floor_limited = floor_limited,
            plateau_width_log10_input, input_operating_range_log10,
            leading_flat_frac, sign_seq = seq)
end

# Vocabulary membership gate (Tier-2-style, on the deterministic curve).
function shape_gate(m, cls::Symbol, policy::PhenotyperPolicy)
    seq = m.sign_seq
    if cls === :monotone_activation
        return seq == [1]
    elseif cls === :monotone_repression
        return seq == [-1]
    elseif cls === :thresholded_activation
        # flat-low shoulder then a genuinely sharp rise (distinct from a gentle
        # monotone activation): leading flat run + steep maximum response order.
        return seq == [1] && m.leading_flat_frac >= policy.thresh_flat_min_frac &&
               m.rise_slope >= policy.thresh_slope_min
    elseif cls === :biphasic_peak
        return seq == [1, -1] && m.peak_prominence >= policy.P_min
    elseif cls === :bandpass_with_plateau
        # a biphasic peak whose high region is genuinely broad
        return seq == [1, -1] && m.peak_prominence >= policy.P_min &&
               m.plateau_width_log10_input >= policy.plateau_min
    elseif cls === :biphasic_valley
        return seq == [-1, 1]
    else
        return false
    end
end

# Exact ROP order cross-check hook (doc: "where exposed"). v0 returns nothing;
# a follow-on can compare finite-difference ρ against get_RO_paths tokens.
rop_order_crosscheck(model, input_idx, u, ylog) = nothing

# ── Distributional aggregation ────────────────────────────────────────────────
function _quantile_sorted(sorted::Vector{Float64}, q::Float64)
    isempty(sorted) && return NaN
    n = length(sorted)
    n == 1 && return sorted[1]
    h = (n - 1) * clamp(q, 0.0, 1.0) + 1
    lo = floor(Int, h); hi = min(lo + 1, n)
    return sorted[lo] + (h - lo) * (sorted[hi] - sorted[lo])
end

const _METRIC_KEYS = (:peak_prominence, :baseline_return, :rise_slope, :fall_slope,
                      :asymmetry, :output_fold_change_log10,
                      :plateau_width_log10_input, :input_operating_range_log10)

# median, IQR and the one-sided lower quantile q_α over the in-shape draws.
# Non-finite metric values (e.g. floor-limited fold-change = NaN) are dropped so
# they cannot poison the quantiles.
function conditional_quantiles(rows::Vector, in_shape::AbstractVector{Bool}, alpha::Float64)
    stats = Dict{Symbol,NamedTuple}()
    idx = findall(in_shape)
    for key in _METRIC_KEYS
        vals = Float64[getfield(rows[i], key) for i in idx]
        filter!(isfinite, vals)
        sort!(vals)
        stats[key] = (; median = _quantile_sorted(vals, 0.5),
                        q25    = _quantile_sorted(vals, 0.25),
                        q75    = _quantile_sorted(vals, 0.75),
                        q_alpha= _quantile_sorted(vals, alpha),
                        n      = length(vals))
    end
    return stats
end

# One parameter draw → (metrics, in_shape?, ok?). `ok` is false when the solve
# did not converge at any grid point (the curve is not trustworthy).
function _evaluate_draw(model, input_idx, coeffs, log_qK, policy, target_class)
    u_lo, u_hi = policy.auto_bracket ?
        auto_bracket(model, input_idx, coeffs, log_qK, policy) : (policy.u_lo, policy.u_hi)
    local u, ylog, valid
    try
        u, ylog, _regimes, valid = scan_curve(model, input_idx, coeffs, log_qK, u_lo, u_hi, policy.npoints)
    catch
        return (nothing, false, false)            # raised → failed draw
    end
    all(valid) || return (nothing, false, false)  # non-converged point(s) → failed draw
    rho = response_order(u, ylog)
    m = per_curve_metrics(u, ylog, rho, policy)
    in_shape = target_class === nothing ? true : shape_gate(m, target_class, policy)
    return (m, in_shape, true)
end

"""
    phenotype(model; input_sym, output_expr, prior, policy, target_class) -> NamedTuple

Distributional phenotype of a `Bnc` model for one (input,output) assignment.
Draws K parameter vectors from `prior` (low-discrepancy by default), scans each,
gates shape membership against `target_class`, and returns:

  * `shape_support`  — fraction of draws in the target shape (= robustness number;
                       failed/non-converged draws count as not-in-shape). With
                       `target_class === nothing` every successful draw counts.
  * `stats`          — per-metric median/IQR/q_α over the in-shape draws.
  * `n_failed`       — draws whose solve raised or did not converge.
  * `prior`          — the serialized prior descriptor it was measured against.
  * `phenotyper_version`.
"""
function phenotype(model; input_sym::Symbol, output_expr::AbstractString,
                   prior::ParameterPrior = ParameterPrior(),
                   policy::PhenotyperPolicy = PhenotyperPolicy(),
                   target_class::Union{Nothing,Symbol} = nothing)
    input_idx = locate_sym_qK(model, input_sym)
    input_idx === nothing && error("unknown input qK symbol: $input_sym")
    coeffs = Vector{Vector{Float64}}([parse_linear_combination(model, String(output_expr))])

    rng = MersenneTwister(policy.seed)
    rows = Vector{NamedTuple}()
    in_shape = Bool[]
    n_failed = 0

    for k in 1:policy.K
        log_qK = draw_log_qK(model, prior, k, policy, rng)
        m, is, ok = _evaluate_draw(model, input_idx, coeffs, log_qK, policy, target_class)
        if !ok
            n_failed += 1
            continue
        end
        push!(rows, m)
        push!(in_shape, is)
    end

    shape_support = count(in_shape) / policy.K        # failures dilute support
    stats = conditional_quantiles(rows, in_shape, policy.alpha)

    return (; shape_support,
            stats,
            n_draws = policy.K,
            n_evaluated = length(rows),
            n_failed,
            target_class,
            input_sym,
            output = String(output_expr),
            sampler = policy.sampler,
            prior = prior_descriptor(prior),
            phenotyper_version = policy.version)
end

# ── Single-pass distributional profile (for dataset generation) ───────────────
# Classify one curve's compressed sign sequence into a vocabulary class.
function classify_shape(seq::Vector{Int}, m, policy::PhenotyperPolicy)
    if seq == [1]
        return (m.leading_flat_frac >= policy.thresh_flat_min_frac &&
                m.rise_slope >= policy.thresh_slope_min) ?
               :thresholded_activation : :monotone_activation
    elseif seq == [-1]
        return :monotone_repression
    elseif seq == [1, -1]
        if m.peak_prominence >= policy.P_min
            return m.plateau_width_log10_input >= policy.plateau_min ?
                   :bandpass_with_plateau : :biphasic_peak
        else
            return :monotone_activation
        end
    elseif seq == [-1, 1]
        return :biphasic_valley
    elseif isempty(seq)
        return :flat
    else
        return :complex
    end
end

# Single source of truth: the profile classes ARE the vocabulary keys, plus the
# two non-vocabulary catch-alls. Derives from PHENOTYPE_VOCAB_V0 so the classifier
# and the vocabulary can never silently diverge (the review flagged parallel,
# unsynchronized vocabularies). CLASS_MAP in run_benchmark.jl maps user-facing
# aliases onto these same keys.
const _PROFILE_CLASSES = (keys(PHENOTYPE_VOCAB_V0)..., :flat, :complex)

"""
    phenotype_profile(model; input_sym, output_expr, prior, policy) -> NamedTuple

Single-pass distributional phenotype label for dataset generation. Draws K
parameter vectors, classifies each curve's shape, and returns the shape-fraction
distribution (`shape_fractions`), the `dominant_shape` (deterministic tie-break
by fixed class order), and per-metric stats over all successful draws.
"""
function phenotype_profile(model; input_sym::Symbol, output_expr::AbstractString,
                           prior::ParameterPrior = ParameterPrior(),
                           policy::PhenotyperPolicy = PhenotyperPolicy())
    input_idx = locate_sym_qK(model, input_sym)
    input_idx === nothing && error("unknown input qK symbol: $input_sym")
    coeffs = Vector{Vector{Float64}}([parse_linear_combination(model, String(output_expr))])
    rng = MersenneTwister(policy.seed)
    rows = NamedTuple[]; shapes = Symbol[]; n_failed = 0
    for k in 1:policy.K
        log_qK = draw_log_qK(model, prior, k, policy, rng)
        u_lo, u_hi = policy.auto_bracket ?
            auto_bracket(model, input_idx, coeffs, log_qK, policy) : (policy.u_lo, policy.u_hi)
        local u, ylog, valid
        try
            u, ylog, _r, valid = scan_curve(model, input_idx, coeffs, log_qK, u_lo, u_hi, policy.npoints)
        catch
            n_failed += 1; continue
        end
        all(valid) || (n_failed += 1; continue)     # non-converged → failed draw
        rho = response_order(u, ylog)
        m = per_curve_metrics(u, ylog, rho, policy)
        push!(rows, m); push!(shapes, classify_shape(m.sign_seq, m, policy))
    end
    K = policy.K
    shape_fractions = Dict(c => count(==(c), shapes) / K for c in _PROFILE_CLASSES)
    stats = conditional_quantiles(rows, trues(length(rows)), policy.alpha)
    # deterministic tie-break: scan the fixed class order, keep the strict max
    dominant = :none; best = -1.0
    for c in _PROFILE_CLASSES
        f = shape_fractions[c]
        f > best && (best = f; dominant = c)
    end
    isempty(shapes) && (dominant = :none)
    return (; shape_fractions, dominant_shape = dominant, stats,
            n_draws = K, n_evaluated = length(rows), n_failed,
            input_sym, output = String(output_expr),
            sampler = policy.sampler, prior = prior_descriptor(prior),
            phenotyper_version = policy.version)
end

end # module PhenotypePipeline
