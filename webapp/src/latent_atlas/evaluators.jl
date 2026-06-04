module BehaviorEvaluators
# Behavior Evaluator Registry (roadmap: "Evaluator Registry Rather than a Monolithic
# Feature Table"). Each evaluator declares substrate / behavior_family / arity / backend /
# supported / trainable. DoseShapeEvaluator (Suite A) delegates to the existing phenotyper
# (PhenotypePipeline.phenotype) — registered, not re-implemented. LogicTruthTableEvaluator
# (Suite B) is implemented here on the existing scan_parameter_2d 2-input grid primitive.
#
#   include("webapp/src/latent_atlas/evaluators.jl"); using .BehaviorEvaluators
using BindingAndCatalysis
using Random

export EvaluatorSpec, REGISTRY, get_evaluator, evaluate_logic, evaluate_analog, evaluate_contextual, LOGIC_TABLES

struct EvaluatorSpec
    key::String
    substrate::String
    behavior_family::String
    arity::String
    backend::String
    supported::Bool
    trainable::Bool
    note::String
end

# First registry entries (supported by the current equilibrium backend) + declared-but-
# deferred entries (marked unsupported), so the system never pretends one scan answers
# every request. Adding a backend later = adding evaluators, not rewriting the language.
const REGISTRY = EvaluatorSpec[
    EvaluatorSpec("DoseShapeEvaluator", "equilibrium_binding", "dose_shape", "one_input", "equilibrium_scan", true, true,
        "phenotyper v0.3.0: SISO dose-response per-curve metrics + distributional shape_support (PhenotypePipeline.phenotype)"),
    EvaluatorSpec("LogicTruthTableEvaluator", "competitive_dimerization", "logic", "two_input", "multi_input_grid_scan", true, true,
        "2-input Boolean gate: distributional truth-table agreement + on/off margin via scan_parameter_2d (this module)"),
    EvaluatorSpec("AnalogSurfaceEvaluator", "competitive_dimerization", "analog_surface", "two_input", "multi_input_grid_scan", true, true,
        "2-input response-surface descriptors (interior-bump fraction, dynamic range, ratio/coactivation correlation) via scan_parameter_2d + 2D warm-start (this module)"),
    EvaluatorSpec("ContextualVersatilityEvaluator", "competitive_dimerization", "contextual_versatility", "context_indexed", "expression_context_sweep", true, true,
        "does one fixed affinity network realise different gates under different accessory-expression contexts? sweeps a context total + re-runs the logic evaluator (this module)"),
    EvaluatorSpec("TemporalFilterEvaluator", "catalytic_crn", "temporal_filter", "one_input", "ode_sim", false, false, "deferred: needs kinetic backend"),
    EvaluatorSpec("AdaptationEvaluator", "catalytic_crn", "adaptation", "one_input", "ode_sim", false, false, "deferred"),
    EvaluatorSpec("OscillatorEvaluator", "catalytic_crn", "oscillation", "one_input", "ode_sim", false, false, "deferred"),
    EvaluatorSpec("MemoryBistabilityEvaluator", "catalytic_crn", "memory_bistability", "one_input", "ode_sim", false, false, "deferred"),
    EvaluatorSpec("NoiseRobustnessEvaluator", "catalytic_crn", "noise_robustness", "one_input", "stochastic_sim", false, false, "deferred"),
    EvaluatorSpec("SpatialPatternEvaluator", "spatial_multicellular", "spatial_pattern", "spatial_coordinate", "ode_sim", false, false, "deferred"),
    EvaluatorSpec("GeneCircuitMotifEvaluator", "gene_regulatory", "gene_circuit_motif", "n_input", "ode_sim", false, false, "deferred"),
]

get_evaluator(behavior_family::AbstractString) =
    (i = findfirst(e -> e.behavior_family == behavior_family, REGISTRY); i === nothing ? nothing : REGISTRY[i])

# Target truth tables over inputs (A,B) in corner order (00, 01, 10, 11); output high = 1.
const LOGIC_TABLES = Dict{String,NTuple{4,Int}}(
    "AND" => (0,0,0,1), "OR" => (0,1,1,1), "NAND" => (1,1,1,0), "NOR" => (1,0,0,0),
    "XOR" => (0,1,1,0), "XNOR" => (1,0,0,1),
    "NIMPLY" => (0,0,1,0),  # A AND NOT B
    "IMPLY"  => (1,1,0,1),  # NOT A OR B
    "NOT_A"  => (1,1,0,0),  # NOT A (B-independent)
    "NOT_B"  => (1,0,1,0),  # NOT B (A-independent)
    "A"      => (0,0,1,1),  # output high iff A (B-independent passthrough)
    "B"      => (0,1,0,1),  # output high iff B (A-independent passthrough)
    "CIMPLY" => (1,0,1,1),  # A OR NOT B  (converse implication B->A)
    "BNIMPLY"=> (0,1,0,0),  # B AND NOT A
    "TRUE"   => (1,1,1,1), "FALSE" => (0,0,0,0),
)

_median(v) = (s = sort(v); n = length(s); n == 0 ? 0.0 : (isodd(n) ? float(s[(n+1)÷2]) : (s[n÷2] + s[n÷2+1]) / 2))
_mode(v) = (best = v[1]; bc = 0; for t in unique(v); c = count(==(t), v); if c > bc; bc = c; best = t; end; end; best)
function _gate_name(tt::NTuple{4,Int})
    for (k, v) in LOGIC_TABLES
        v == tt && return k
    end
    return "none" * string(tt)
end

# Resolve an input symbol ("A", :A, "tA", :tA) to its total index in free_syms.
function _total_idx(sym, free_syms)
    fs = Symbol.(free_syms); s = Symbol(sym)
    i = findfirst(==(s), fs); i !== nothing && return i
    ss = String(sym)
    if length(ss) > 1 && startswith(ss, "t")
        i = findfirst(==(Symbol(ss[2:end])), fs); i !== nothing && return i
    end
    return nothing
end

"""
    evaluate_logic(model, species, free_syms; input_syms, output_sym, target=nothing, ...)

LogicTruthTableEvaluator. Sweeps the two input totals on an `npoints`×`npoints` grid via
`scan_parameter_2d`, over `K` Kd-draws (totals pinned at the model anchor; Kd ~ U(kd_lo,kd_hi),
seeded). Per draw it Booleanizes the four corners by a separating midpoint threshold, builds
the realized (A,B) truth table, and records the on/off margin (decades). Returns the modal
realized gate, the distributional truth-table agreement with `target` (fraction of draws
matching — the logic analogue of the phenotyper's shape_support), and the median margin.
"""
function evaluate_logic(model, species, free_syms; input_syms, output_sym,
        target::Union{Nothing,String}=nothing, npoints::Int=7, K::Int=8,
        kd_lo::Float64=-3.0, kd_hi::Float64=3.0, in_lo::Float64=-3.0, in_hi::Float64=3.0, seed::Int=20260603,
        fixed_totals::AbstractDict=Dict{String,Float64}())
    iA = _total_idx(input_syms[1], free_syms)
    iB = _total_idx(input_syms[2], free_syms)
    oi = findfirst(==(Symbol(output_sym)), Symbol.(species))
    (iA === nothing || iB === nothing || oi === nothing) &&
        error("symbol not found: inputs=$(input_syms) output=$(output_sym); free=$(free_syms) species=$(species)")
    base = copy(model._anchor_log_qK); d = length(free_syms); r = length(base) - d
    for (s, v) in fixed_totals          # pin accessory/context totals (e.g. for ContextualVersatilityEvaluator)
        ti = _total_idx(s, free_syms); ti === nothing || (base[ti] = Float64(v))
    end
    oc = zeros(Float64, length(species)); oc[oi] = 1.0
    rng = collect(range(in_lo, in_hi, length=npoints))
    rs = MersenneTwister(seed)
    tables = NTuple{4,Int}[]; margins = Float64[]
    for _ in 1:K
        qK = copy(base)
        qK[d+1:end] .= kd_lo .+ (kd_hi - kd_lo) .* rand(rs, r)         # draw Kd
        fp = copy(qK); for ix in sort([iA, iB]; rev=true); deleteat!(fp, ix); end
        _, _, grid, _ = scan_parameter_2d(model, iA, iB, rng, rng, oc, fp;
                                          input_logspace=true, output_logspace=true)
        c = (grid[1, 1], grid[1, end], grid[end, 1], grid[end, end])  # (00,01,10,11) over (A,B)
        thr = (maximum(c) + minimum(c)) / 2
        tt = (Int(c[1] > thr), Int(c[2] > thr), Int(c[3] > thr), Int(c[4] > thr))
        hi = [c[m] for m in 1:4 if tt[m] == 1]; lo = [c[m] for m in 1:4 if tt[m] == 0]
        margin = (isempty(hi) || isempty(lo)) ? 0.0 : (minimum(hi) - maximum(lo))
        push!(tables, tt); push!(margins, margin)
    end
    realized = _mode(tables)
    res = Dict{String,Any}(
        "realized_table" => realized,
        "realized_gate"  => _gate_name(realized),
        "median_margin_decades" => _median(margins),
        "tables" => tables, "margins" => margins,
    )
    if target !== nothing
        haskey(LOGIC_TABLES, target) || error("unknown target gate $target")
        res["target"] = target
        res["truth_table_agreement"] = count(==(LOGIC_TABLES[target]), tables) / K
    end
    return res
end

_mean(v) = isempty(v) ? 0.0 : sum(v) / length(v)
function _corr(x::Vector{Float64}, y::Vector{Float64})
    n = length(x); n == 0 && return 0.0
    mx = _mean(x); my = _mean(y)
    sxy = 0.0; sxx = 0.0; syy = 0.0
    for k in 1:n
        dx = x[k] - mx; dy = y[k] - my
        sxy += dx * dy; sxx += dx * dx; syy += dy * dy
    end
    (sxx <= 0 || syy <= 0) ? 0.0 : sxy / sqrt(sxx * syy)
end

"""
    evaluate_analog(model, species, free_syms; input_syms, output_sym, npoints=21, K=8, ...)

AnalogSurfaceEvaluator. Sweeps the two input totals on an `npoints`×`npoints` grid via
`scan_parameter_2d` (now warm-started), over `K` Kd-draws, and returns distributional
response-surface descriptors treating the heatmap as a function object (not scalars):
`bump_fraction` (fraction of draws whose output maximum is interior, not on an edge =
two-input-bump-ness), `median_dynamic_range_decades`, and the median Pearson correlation
of the surface with log(A/B) (ratio-sensing) and with min(logA,logB) (AND-like
coactivation). Mirrors the phenotyper's distributional style for 2-D surfaces.
"""
function evaluate_analog(model, species, free_syms; input_syms, output_sym,
        npoints::Int=21, K::Int=8, kd_lo::Float64=-3.0, kd_hi::Float64=3.0,
        in_lo::Float64=-3.0, in_hi::Float64=3.0, seed::Int=20260604)
    iA = _total_idx(input_syms[1], free_syms)
    iB = _total_idx(input_syms[2], free_syms)
    oi = findfirst(==(Symbol(output_sym)), Symbol.(species))
    (iA === nothing || iB === nothing || oi === nothing) &&
        error("symbol not found: inputs=$(input_syms) output=$(output_sym); free=$(free_syms) species=$(species)")
    base = copy(model._anchor_log_qK); d = length(free_syms); r = length(base) - d
    oc = zeros(Float64, length(species)); oc[oi] = 1.0
    rng = collect(range(in_lo, in_hi, length=npoints))
    Acol = Float64[rng[i] for i in 1:npoints, j in 1:npoints]   # input-A value at (i,j)
    Bcol = Float64[rng[j] for i in 1:npoints, j in 1:npoints]   # input-B value at (i,j)
    ratio_x = vec(Acol .- Bcol); coact_x = vec(min.(Acol, Bcol))
    rs = MersenneTwister(seed)
    bump = Int[]; dyn = Float64[]; rcorr = Float64[]; ccorr = Float64[]
    for _ in 1:K
        qK = copy(base)
        qK[d+1:end] .= kd_lo .+ (kd_hi - kd_lo) .* rand(rs, r)
        fp = copy(qK); for ix in sort([iA, iB]; rev=true); deleteat!(fp, ix); end
        _, _, grid, _ = scan_parameter_2d(model, iA, iB, rng, rng, oc, fp;
                                          input_logspace=true, output_logspace=true)
        _, idx = findmax(grid); mi, mj = Tuple(idx)
        push!(bump, (1 < mi < npoints && 1 < mj < npoints) ? 1 : 0)
        push!(dyn, maximum(grid) - minimum(grid))
        v = vec(grid)
        push!(rcorr, _corr(ratio_x, v)); push!(ccorr, _corr(coact_x, v))
    end
    return Dict{String,Any}(
        "bump_fraction" => _mean(bump),
        "median_dynamic_range_decades" => _median(dyn),
        "median_ratio_corr" => _median(rcorr),
        "median_coactivation_corr" => _median(ccorr),
    )
end

"""
    evaluate_contextual(model, species, free_syms; input_syms, output_sym, context_sym,
                        context_levels=[-2.0,0.0,2.0], ...)

ContextualVersatilityEvaluator. Asks whether ONE fixed affinity network computes DIFFERENT
2-input logic under different accessory-expression contexts: it pins the `context_sym` total
to each level in `context_levels` and re-runs the logic evaluator on `input_syms`->`output_sym`.
Returns the realised gate (+ support, margin) per context, the set of distinct robustly-realised
gates, and `reprogrammable` (true if >=2 distinct gates with support >= `min_support`) --- the
expression-tuning analogue of Parres-Gold's cell-type-specific computation.
"""
function evaluate_contextual(model, species, free_syms; input_syms, output_sym, context_sym,
        context_levels=[-2.0, 0.0, 2.0], npoints::Int=7, K::Int=8, min_support::Float64=0.5)
    per = NamedTuple[]
    for ctx in context_levels
        r = evaluate_logic(model, species, free_syms; input_syms=input_syms, output_sym=output_sym,
                           target=nothing, npoints=npoints, K=K,
                           fixed_totals=Dict(String(context_sym) => Float64(ctx)))
        support = count(==(r["realized_table"]), r["tables"]) / length(r["tables"])
        push!(per, (context=Float64(ctx), gate=r["realized_gate"], support=round(support, digits=3),
                    margin=round(r["median_margin_decades"], digits=3)))
    end
    robust = [x.gate for x in per if x.support >= min_support]
    distinct = unique(robust)
    return Dict{String,Any}(
        "context_sym" => String(context_sym),
        "per_context" => [Dict("context" => x.context, "gate" => x.gate, "support" => x.support, "margin" => x.margin) for x in per],
        "distinct_robust_gates" => distinct,
        "n_distinct_robust_gates" => length(distinct),
        "reprogrammable" => length(distinct) >= 2,
    )
end

end # module
