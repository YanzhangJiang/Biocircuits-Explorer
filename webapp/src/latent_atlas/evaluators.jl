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

export EvaluatorSpec, REGISTRY, get_evaluator, evaluate_logic, evaluate_analog,
       evaluate_contextual, is_complete_evaluator_evidence, LOGIC_TABLES

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
        "phenotyper v0.4.0: SISO dose-response per-curve metrics + distributional shape_support, incl. first-class multimodal (PhenotypePipeline.phenotype)"),
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

function _evaluator_anchor_log_qK(model)
    # The numerical anchor is owned by the lazily initialized integration
    # helper, not by `Bnc` itself. Keep evaluator code on that current engine
    # owner so label generation does not depend on the removed legacy field.
    helper = BindingAndCatalysis._integration_helper!(model)
    return Float64.(copy(getfield(helper, :_anchor_log_qK)))
end

_finite_evaluator_value(value) =
    value isa Real && !(value isa Bool) && isfinite(Float64(value))

function _draw_evidence_counts(draws)
    requested = length(draws)
    valid = count(draw -> draw.valid, draws)
    invalid = requested - valid
    status = valid == 0 ? "no_evidence" : invalid == 0 ? "complete" : "partial"
    reasons = Dict{String, Int}()
    for draw in draws
        draw.valid && continue
        reason = String(draw.reason)
        reasons[reason] = get(reasons, reason, 0) + 1
    end
    return (; requested, valid, invalid, status, partial=invalid > 0, reasons)
end

"""
    is_complete_evaluator_evidence(result; unit=:draw) -> Bool

Return true only when an evaluator result explicitly reports a positive,
complete population with no invalid member. Offline label producers use this
single predicate before publishing a formal row.
"""
function is_complete_evaluator_evidence(result; unit::Symbol=:draw)
    result isa AbstractDict || return false
    unit in (:draw, :context) || throw(ArgumentError("unit must be :draw or :context"))
    prefix = unit === :draw ? "draw" : "context"
    requested = get(result, "requested_$(prefix)_count", nothing)
    valid = get(result, "valid_$(prefix)_count", nothing)
    invalid = get(result, "invalid_$(prefix)_count", nothing)
    for value in (requested, valid, invalid)
        value isa Integer && !(value isa Bool) || return false
    end
    return requested > 0 && valid == requested && invalid == 0 &&
        get(result, "partial", true) === false &&
        get(result, "evidence_status", nothing) == "complete"
end

"""
    _logic_draw_from_grid(grid, validity)

Pure one-draw truth-table reducer. Logic uses only the four input corners; each
corner must have the literal validity marker `true` and a finite output. An
invalid corner returns an invalid draw without constructing a truth table, so a
missing numerical sample can never collapse into the `FALSE` gate.
"""
function _logic_draw_from_grid(grid, validity)
    if !(grid isa AbstractMatrix) || !(validity isa AbstractMatrix) ||
       size(grid) != size(validity) || size(grid, 1) < 2 || size(grid, 2) < 2
        return (valid=false, table=nothing, margin=nothing,
                reason="grid_validity_shape_mismatch")
    end
    corner_indices = (
        (1, 1),
        (1, size(grid, 2)),
        (size(grid, 1), 1),
        (size(grid, 1), size(grid, 2)),
    )
    corners = Float64[]
    for (i, j) in corner_indices
        validity[i, j] === true || return (
            valid=false, table=nothing, margin=nothing,
            reason="invalid_required_corner",
        )
        value = grid[i, j]
        _finite_evaluator_value(value) || return (
            valid=false, table=nothing, margin=nothing,
            reason="nonfinite_required_corner",
        )
        push!(corners, Float64(value))
    end
    c = (corners[1], corners[2], corners[3], corners[4])
    threshold = (maximum(c) + minimum(c)) / 2
    isfinite(threshold) || return (
        valid=false, table=nothing, margin=nothing,
        reason="nonfinite_logic_threshold",
    )
    table = (
        Int(c[1] > threshold),
        Int(c[2] > threshold),
        Int(c[3] > threshold),
        Int(c[4] > threshold),
    )
    high = [c[idx] for idx in 1:4 if table[idx] == 1]
    low = [c[idx] for idx in 1:4 if table[idx] == 0]
    margin = (isempty(high) || isempty(low)) ? 0.0 : minimum(high) - maximum(low)
    return (valid=true, table=table, margin=Float64(margin), reason="valid")
end

function _summarize_logic_draws(draws; target::Union{Nothing,String}=nothing)
    target !== nothing && !haskey(LOGIC_TABLES, target) &&
        error("unknown target gate $target")
    counts = _draw_evidence_counts(draws)
    tables = NTuple{4, Int}[]
    margins = Float64[]
    for draw in draws
        draw.valid || continue
        push!(tables, draw.table)
        push!(margins, Float64(draw.margin))
    end
    realized = isempty(tables) ? nothing : _mode(tables)
    result = Dict{String, Any}(
        "realized_table" => realized,
        "realized_gate" => realized === nothing ? nothing : _gate_name(realized),
        "median_margin_decades" => isempty(margins) ? nothing : _median(margins),
        "tables" => tables,
        "margins" => margins,
        "requested_draw_count" => counts.requested,
        "valid_draw_count" => counts.valid,
        "invalid_draw_count" => counts.invalid,
        "partial" => counts.partial,
        "evidence_status" => counts.status,
        "validity_basis" => "four_truth_table_corners",
        "invalid_draw_reasons" => counts.reasons,
    )
    if target !== nothing
        result["target"] = target
        result["truth_table_agreement"] = isempty(tables) ? nothing :
            count(==(LOGIC_TABLES[target]), tables) / length(tables)
    end
    return result
end

"""
    evaluate_logic(model, species, free_syms; input_syms, output_sym, target=nothing, ...)

LogicTruthTableEvaluator. Sweeps the two input totals on an `npoints`×`npoints` grid via
`scan_parameter_2d`, over `K` Kd-draws (totals pinned at the model anchor; Kd ~ U(kd_lo,kd_hi),
seeded). Per draw it Booleanizes the four valid finite corners by a separating midpoint
threshold, builds the realized (A,B) truth table, and records the on/off margin (decades).
Invalid draws are excluded from every gate/margin denominator and reported explicitly. A
zero-valid-draw result has `evidence_status="no_evidence"` and no realized gate.
"""
function evaluate_logic(model, species, free_syms; input_syms, output_sym,
        target::Union{Nothing,String}=nothing, npoints::Int=7, K::Int=8,
        kd_lo::Float64=-3.0, kd_hi::Float64=3.0, in_lo::Float64=-3.0, in_hi::Float64=3.0, seed::Int=20260603,
        fixed_totals::AbstractDict=Dict{String,Float64}())
    npoints >= 2 || throw(ArgumentError("npoints must be at least 2"))
    K >= 1 || throw(ArgumentError("K must be at least 1"))
    target !== nothing && !haskey(LOGIC_TABLES, target) &&
        error("unknown target gate $target")
    iA = _total_idx(input_syms[1], free_syms)
    iB = _total_idx(input_syms[2], free_syms)
    oi = findfirst(==(Symbol(output_sym)), Symbol.(species))
    (iA === nothing || iB === nothing || oi === nothing) &&
        error("symbol not found: inputs=$(input_syms) output=$(output_sym); free=$(free_syms) species=$(species)")
    base = _evaluator_anchor_log_qK(model); d = length(free_syms); r = length(base) - d
    for (s, v) in fixed_totals          # pin accessory/context totals (e.g. for ContextualVersatilityEvaluator)
        ti = _total_idx(s, free_syms); ti === nothing || (base[ti] = Float64(v))
    end
    oc = zeros(Float64, length(species)); oc[oi] = 1.0
    rng = collect(range(in_lo, in_hi, length=npoints))
    rs = MersenneTwister(seed)
    draws = NamedTuple[]
    for _ in 1:K
        qK = copy(base)
        qK[d+1:end] .= kd_lo .+ (kd_hi - kd_lo) .* rand(rs, r)         # draw Kd
        fp = copy(qK); for ix in sort([iA, iB]; rev=true); deleteat!(fp, ix); end
        _, _, grid, _, validity = scan_parameter_2d(
            model, iA, iB, rng, rng, oc, fp;
            input_logspace=true,
            output_logspace=true,
            track_validity=true,
        )
        push!(draws, _logic_draw_from_grid(grid, validity))
    end
    return _summarize_logic_draws(draws; target=target)
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
    _analog_draw_from_grid(grid, validity, param1_values, param2_values)

Pure one-draw analog-surface reducer. Every grid point participates in at least
one of `findmax`, dynamic range, or correlation, so every validity marker and
every output must be valid and finite before any metric is constructed.
"""
function _analog_draw_from_grid(grid, validity, param1_values, param2_values)
    if !(grid isa AbstractMatrix) || !(validity isa AbstractMatrix) ||
       size(grid) != size(validity) ||
       length(param1_values) != size(grid, 1) ||
       length(param2_values) != size(grid, 2) ||
       size(grid, 1) < 2 || size(grid, 2) < 2
        return (
            valid=false,
            bump=nothing,
            dynamic_range=nothing,
            ratio_corr=nothing,
            coactivation_corr=nothing,
            reason="grid_validity_shape_mismatch",
        )
    end
    all(value -> value === true, validity) || return (
        valid=false,
        bump=nothing,
        dynamic_range=nothing,
        ratio_corr=nothing,
        coactivation_corr=nothing,
        reason="invalid_required_grid_point",
    )
    all(_finite_evaluator_value, grid) || return (
        valid=false,
        bump=nothing,
        dynamic_range=nothing,
        ratio_corr=nothing,
        coactivation_corr=nothing,
        reason="nonfinite_required_grid_point",
    )
    all(_finite_evaluator_value, param1_values) &&
        all(_finite_evaluator_value, param2_values) || return (
            valid=false,
            bump=nothing,
            dynamic_range=nothing,
            ratio_corr=nothing,
            coactivation_corr=nothing,
            reason="nonfinite_parameter_grid",
        )

    values = Matrix{Float64}(grid)
    axis1 = Float64.(collect(param1_values))
    axis2 = Float64.(collect(param2_values))
    axis1_grid = Float64[axis1[i] for i in eachindex(axis1), _ in eachindex(axis2)]
    axis2_grid = Float64[axis2[j] for _ in eachindex(axis1), j in eachindex(axis2)]
    ratio_x = vec(axis1_grid .- axis2_grid)
    coactivation_x = vec(min.(axis1_grid, axis2_grid))
    _, index = findmax(values)
    row, column = Tuple(index)
    bump = (1 < row < size(values, 1) && 1 < column < size(values, 2)) ? 1 : 0
    dynamic_range = maximum(values) - minimum(values)
    flattened = vec(values)
    ratio_corr = _corr(ratio_x, flattened)
    coactivation_corr = _corr(coactivation_x, flattened)
    all(isfinite, (dynamic_range, ratio_corr, coactivation_corr)) || return (
        valid=false,
        bump=nothing,
        dynamic_range=nothing,
        ratio_corr=nothing,
        coactivation_corr=nothing,
        reason="nonfinite_analog_metric",
    )
    return (
        valid=true,
        bump=bump,
        dynamic_range=Float64(dynamic_range),
        ratio_corr=Float64(ratio_corr),
        coactivation_corr=Float64(coactivation_corr),
        reason="valid",
    )
end

function _summarize_analog_draws(draws)
    counts = _draw_evidence_counts(draws)
    bump = Int[]
    dynamic_ranges = Float64[]
    ratio_correlations = Float64[]
    coactivation_correlations = Float64[]
    for draw in draws
        draw.valid || continue
        push!(bump, Int(draw.bump))
        push!(dynamic_ranges, Float64(draw.dynamic_range))
        push!(ratio_correlations, Float64(draw.ratio_corr))
        push!(coactivation_correlations, Float64(draw.coactivation_corr))
    end
    return Dict{String, Any}(
        "bump_fraction" => isempty(bump) ? nothing : _mean(bump),
        "median_dynamic_range_decades" =>
            isempty(dynamic_ranges) ? nothing : _median(dynamic_ranges),
        "median_ratio_corr" =>
            isempty(ratio_correlations) ? nothing : _median(ratio_correlations),
        "median_coactivation_corr" =>
            isempty(coactivation_correlations) ? nothing :
                _median(coactivation_correlations),
        "requested_draw_count" => counts.requested,
        "valid_draw_count" => counts.valid,
        "invalid_draw_count" => counts.invalid,
        "partial" => counts.partial,
        "evidence_status" => counts.status,
        "validity_basis" => "complete_two_input_grid",
        "invalid_draw_reasons" => counts.reasons,
    )
end

"""
    evaluate_analog(model, species, free_syms; input_syms, output_sym, npoints=21, K=8, ...)

AnalogSurfaceEvaluator. Sweeps the two input totals on an `npoints`×`npoints` grid via
`scan_parameter_2d` (now warm-started), over `K` Kd-draws, and returns distributional
response-surface descriptors only for complete valid grids, treating the heatmap as a
function object (not scalars):
`bump_fraction` (fraction of draws whose output maximum is interior, not on an edge =
two-input-bump-ness), `median_dynamic_range_decades`, and the median Pearson correlation
of the surface with log(A/B) (ratio-sensing) and with min(logA,logB) (AND-like
coactivation). Mirrors the phenotyper's distributional style for 2-D surfaces.
"""
function evaluate_analog(model, species, free_syms; input_syms, output_sym,
        npoints::Int=21, K::Int=8, kd_lo::Float64=-3.0, kd_hi::Float64=3.0,
        in_lo::Float64=-3.0, in_hi::Float64=3.0, seed::Int=20260604)
    npoints >= 2 || throw(ArgumentError("npoints must be at least 2"))
    K >= 1 || throw(ArgumentError("K must be at least 1"))
    iA = _total_idx(input_syms[1], free_syms)
    iB = _total_idx(input_syms[2], free_syms)
    oi = findfirst(==(Symbol(output_sym)), Symbol.(species))
    (iA === nothing || iB === nothing || oi === nothing) &&
        error("symbol not found: inputs=$(input_syms) output=$(output_sym); free=$(free_syms) species=$(species)")
    base = _evaluator_anchor_log_qK(model); d = length(free_syms); r = length(base) - d
    oc = zeros(Float64, length(species)); oc[oi] = 1.0
    rng = collect(range(in_lo, in_hi, length=npoints))
    rs = MersenneTwister(seed)
    draws = NamedTuple[]
    for _ in 1:K
        qK = copy(base)
        qK[d+1:end] .= kd_lo .+ (kd_hi - kd_lo) .* rand(rs, r)
        fp = copy(qK); for ix in sort([iA, iB]; rev=true); deleteat!(fp, ix); end
        _, _, grid, _, validity = scan_parameter_2d(
            model, iA, iB, rng, rng, oc, fp;
            input_logspace=true,
            output_logspace=true,
            track_validity=true,
        )
        push!(draws, _analog_draw_from_grid(grid, validity, rng, rng))
    end
    return _summarize_analog_draws(draws)
end

"""
    evaluate_contextual(model, species, free_syms; input_syms, output_sym, context_sym,
                        context_levels=[-2.0,0.0,2.0], ...)

ContextualVersatilityEvaluator. Asks whether ONE fixed affinity network computes DIFFERENT
2-input logic under different accessory-expression contexts: it pins the `context_sym` total
to each level in `context_levels` and re-runs the logic evaluator on `input_syms`->`output_sym`.
Returns the realised gate (+ support, margin) per context, the set of distinct robustly-realised
gates, and `reprogrammable` (true if >=2 distinct gates with support >= `min_support`) --- the
expression-tuning analogue of Parres-Gold's cell-type-specific computation. A context counts
only when its complete logic evaluation has no invalid Kd draw. Partial/no-evidence contexts
are returned diagnostically with no gate and make the contextual result partial.
"""
function _summarize_contextual_results(context_sym, context_levels, logic_results;
                                       min_support::Float64)
    length(context_levels) == length(logic_results) ||
        throw(ArgumentError("context levels and logic results must have equal length"))
    requested = length(context_levels)
    per_context = Dict{String, Any}[]
    robust_gates = String[]
    valid_contexts = 0
    for (context, result) in zip(context_levels, logic_results)
        result isa AbstractDict || throw(ArgumentError("logic result must be a dictionary"))
        logic_status = String(get(result, "evidence_status", "no_evidence"))
        requested_draws = Int(get(result, "requested_draw_count", 0))
        valid_draws = Int(get(result, "valid_draw_count", 0))
        invalid_draws = Int(get(result, "invalid_draw_count", requested_draws))
        tables = get(result, "tables", NTuple{4, Int}[])
        complete = logic_status == "complete" && requested_draws > 0 &&
            valid_draws == requested_draws && invalid_draws == 0 &&
            tables isa AbstractVector && length(tables) == valid_draws &&
            get(result, "realized_table", nothing) !== nothing &&
            get(result, "realized_gate", nothing) isa AbstractString

        gate = nothing
        support = nothing
        margin = nothing
        if complete
            valid_contexts += 1
            gate = String(result["realized_gate"])
            realized_table = result["realized_table"]
            support = count(==(realized_table), tables) / valid_draws
            margin_value = get(result, "median_margin_decades", nothing)
            margin = _finite_evaluator_value(margin_value) ?
                round(Float64(margin_value), digits=3) : nothing
            support >= min_support && push!(robust_gates, gate)
        end
        push!(per_context, Dict{String, Any}(
            "context" => Float64(context),
            "gate" => gate,
            "support" => support === nothing ? nothing : round(support, digits=3),
            "margin" => margin,
            "evidence_status" => logic_status,
            "requested_draw_count" => requested_draws,
            "valid_draw_count" => valid_draws,
            "invalid_draw_count" => invalid_draws,
            "partial" => get(result, "partial", invalid_draws > 0),
        ))
    end
    invalid_contexts = requested - valid_contexts
    evidence_status = valid_contexts == 0 ? "no_evidence" :
        invalid_contexts == 0 ? "complete" : "partial"
    distinct = unique(robust_gates)
    return Dict{String, Any}(
        "context_sym" => String(context_sym),
        "per_context" => per_context,
        "distinct_robust_gates" => distinct,
        "n_distinct_robust_gates" => length(distinct),
        "reprogrammable" => evidence_status == "complete" ? length(distinct) >= 2 : nothing,
        "requested_context_count" => requested,
        "valid_context_count" => valid_contexts,
        "invalid_context_count" => invalid_contexts,
        "partial" => invalid_contexts > 0,
        "evidence_status" => evidence_status,
        "validity_basis" => "complete_logic_evaluation_per_context",
    )
end

function evaluate_contextual(model, species, free_syms; input_syms, output_sym, context_sym,
        context_levels=[-2.0, 0.0, 2.0], npoints::Int=7, K::Int=8,
        min_support::Float64=0.5, logic_evaluator::Function=evaluate_logic)
    isempty(context_levels) && throw(ArgumentError("context_levels must not be empty"))
    0.0 <= min_support <= 1.0 ||
        throw(ArgumentError("min_support must be between 0 and 1"))
    results = Any[]
    for ctx in context_levels
        push!(results, logic_evaluator(
            model,
            species,
            free_syms;
            input_syms=input_syms,
            output_sym=output_sym,
            target=nothing,
            npoints=npoints,
            K=K,
            fixed_totals=Dict(String(context_sym) => Float64(ctx)),
        ))
    end
    return _summarize_contextual_results(
        context_sym,
        context_levels,
        results;
        min_support=min_support,
    )
end

end # module
