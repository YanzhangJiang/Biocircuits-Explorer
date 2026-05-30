#!/usr/bin/env julia
# Phase-1 deterministic baseline runner.
#
# For each benchmark task: take a candidate-network pool, phenotype every
# (network, input/output) assignment, rerank ROBUST-FIRST (shape_support, then
# the user objective), and report verified pass@1/5/20 + oracle-call efficiency.
# The phenotyper (exact ROP scan over a parameter prior) is the verifier; the
# candidate source is a coarse prefilter — exactly the roadmap's deterministic
# baseline ("a complete verified-candidate finder for the in-scope family").
#
# Candidate sources (--source):
#   seeds  (default)  curated pool in benchmarks/seed_networks.json — needs no atlas.
#   atlas             a NON-path_only atlas sqlite with populated network_entries
#                     + behavior_slices (the current path_only exports are empty
#                     of reconstructable networks, so this warns and yields none).
#
#   julia --project=webapp webapp/scripts/run_benchmark.jl \
#       --tasks benchmarks/tasks --seeds benchmarks/seed_networks.json \
#       --out benchmarks/reports/baseline.json [--K 64 --seed 1234 --max-tasks 0]

using JSON3, Dates, Statistics
using SQLite, DBInterface

const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
include(joinpath(HERE, "atlas_read.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using .AtlasRead: to_plain, atlas_fullness, load_network_rules, slice_rows
using BindingAndCatalysis: locate_sym_qK

const REPORT_SCHEMA = "bne-benchmark-report/v0.1.0"

const CLASS_MAP = Dict(
    "monotone_activation"      => :monotone_activation,
    "activation_with_saturation" => :monotone_activation,
    "monotone_repression"      => :monotone_repression,
    "repression_with_floor"    => :monotone_repression,
    "thresholded_activation"   => :thresholded_activation,
    "biphasic_peak"            => :biphasic_peak,
    "bandpass_with_plateau"    => :bandpass_with_plateau,
    "bandpass_like"            => :bandpass_with_plateau,
    "biphasic_valley"          => :biphasic_valley,
    "window_repression"        => :biphasic_valley,
)

rd(x) = (x isa Real && isfinite(x)) ? round(Float64(x); digits = 3) : x

# `to_plain` (JSON3 → plain Dict/Array, so string-keyed lookups + reshape_prior
# work) is shared from AtlasRead.

# ── args ──────────────────────────────────────────────────────────────────────
function parse_args(args)
    o = Dict{String,Any}("tasks"=>"benchmarks/tasks", "seeds"=>"benchmarks/seed_networks.json",
                         "atlas"=>"", "source"=>"seeds", "out"=>"",
                         "K"=>nothing, "seed"=>nothing, "max_tasks"=>0, "max_candidates"=>2000)
    i = 1
    while i <= length(args)
        a = args[i]
        if     a == "--tasks";          o["tasks"]=args[i+1]; i+=2
        elseif a == "--seeds";          o["seeds"]=args[i+1]; i+=2
        elseif a == "--atlas";          o["atlas"]=args[i+1]; o["source"]="atlas"; i+=2
        elseif a == "--source";         o["source"]=args[i+1]; i+=2
        elseif a == "--out";            o["out"]=args[i+1]; i+=2
        elseif a == "--K";              o["K"]=parse(Int,args[i+1]); i+=2
        elseif a == "--seed";           o["seed"]=parse(Int,args[i+1]); i+=2
        elseif a == "--max-tasks";      o["max_tasks"]=parse(Int,args[i+1]); i+=2
        elseif a == "--max-candidates"; o["max_candidates"]=parse(Int,args[i+1]); i+=2
        else error("unknown arg: $a")
        end
    end
    return o
end

# ── candidate pools ───────────────────────────────────────────────────────────
function load_seed_pool(path)
    data = to_plain(JSON3.read(read(path, String)))
    pool = NamedTuple[]
    modelcache = Dict{String,Any}()
    for net in data["networks"]
        nid = String(net["id"]); rules = String.(net["rules"])
        model = get!(modelcache, nid) do
            try
                m, = build_model(rules, ones(Float64, length(rules))); m
            catch e
                @warn "build_model failed for $nid" error=e; nothing
            end
        end
        model === nothing && continue
        for io in net["io"]
            push!(pool, (network_id=nid, rules=rules, model=model,
                         input=String(io["input"]), output=String(io["output"])))
        end
    end
    return pool
end

function load_atlas_pool(path, max_candidates)
    isfile(path) || error("atlas not found: $path")
    db = SQLite.DB(path)
    is_full, ne, bs = atlas_fullness(db)
    if !is_full
        @warn "atlas has no reconstructable networks (network_entries=$ne, behavior_slices=$bs). " *
              "This is a path_only export — use a full (non-path_only) atlas or --source seeds."
        return NamedTuple[]
    end
    rules_of = load_network_rules(db)
    pool = NamedTuple[]; modelcache = Dict{String,Any}()
    for row in slice_rows(db)
        nid = String(row.network_id); haskey(rules_of, nid) || continue
        rules = rules_of[nid]
        model = get!(modelcache, nid) do
            try; m, = build_model(rules, ones(Float64, length(rules))); m; catch; nothing; end
        end
        model === nothing && continue
        push!(pool, (network_id=nid, rules=rules, model=model,
                     input=String(row.input_symbol), output=String(row.output_symbol)))
        length(pool) >= max_candidates && break
    end
    return pool
end

# ── gate evaluation against a phenotype result ────────────────────────────────
# Per the roadmap: *_min gates use the lower quantile q_alpha (guarantee-style),
# *_max / range gates use the median. Returns (hard_pass, reasons, design_score),
# where design_score ∈ [0,1] is the fraction of applicable gates satisfied (the
# shape_support gate included). A candidate passing every gate has design_score=1.0;
# this is the quantity the task's `min_design_score` is checked against (previously
# that success field was silently ignored).
function evaluate_gates(bspec, pr, min_rob)
    reasons = String[]
    hard_pass = true
    ngates = 0
    st = pr.stats
    medof(m) = (haskey(st, m) && isfinite(st[m].median)) ? st[m].median : NaN
    qaof(m)  = (haskey(st, m) && isfinite(st[m].q_alpha)) ? st[m].q_alpha : NaN

    ngates += 1
    if pr.shape_support < min_rob
        hard_pass = false; push!(reasons, "shape_support $(rd(pr.shape_support)) < $min_rob")
    end
    check_min(key, metric) = begin
        if haskey(bspec, key)
            ngates += 1
            v = qaof(metric); m = bspec[key]
            if !(isfinite(v) && v >= m)
                hard_pass = false; push!(reasons, "$metric.q_alpha $(rd(v)) < $m"); end
        end
    end
    check_max(key, metric) = begin
        if haskey(bspec, key)
            ngates += 1
            v = medof(metric); m = bspec[key]
            if !(isfinite(v) && v <= m)
                hard_pass = false; push!(reasons, "$metric.median $(rd(v)) > $m"); end
        end
    end
    check_min("fall_slope_min", :fall_slope)
    check_min("plateau_width_log10_input_min", :plateau_width_log10_input)
    check_min("peak_prominence_min", :peak_prominence)
    check_max("rise_slope_max", :rise_slope)
    check_max("baseline_return_max", :baseline_return)
    if haskey(bspec, "dynamic_range_log10")
        ngates += 1
        rg = bspec["dynamic_range_log10"]; v = medof(:output_fold_change_log10)
        if !(isfinite(v) && rg[1] <= v <= rg[2])
            hard_pass = false; push!(reasons, "output_fold_change_log10.median $(rd(v)) not in [$(rg[1]),$(rg[2])]")
        end
    end
    design_score = ngates == 0 ? 1.0 : 1.0 - length(reasons) / ngates
    return hard_pass, reasons, design_score
end

# Continuous objective fit (higher = better) for robust-first tie-breaking among
# equally-robust candidates. Decoupled from the pass/fail boolean so pass@k is
# non-degenerate: order by shape_support, then by this objective margin.
function objective_score(bspec, pr)
    st = pr.stats
    medof(m) = (haskey(st, m) && isfinite(st[m].median)) ? st[m].median : NaN
    qaof(m)  = (haskey(st, m) && isfinite(st[m].q_alpha)) ? st[m].q_alpha : NaN
    s = 0.0; ngate = 0
    addmin(k, m) = (if haskey(bspec, k); v = qaof(m); s += isfinite(v) ? (v - bspec[k]) : -10.0; ngate += 1; end)
    addmax(k, m) = (if haskey(bspec, k); v = medof(m); s += isfinite(v) ? (bspec[k] - v) : -10.0; ngate += 1; end)
    addmin("fall_slope_min", :fall_slope)
    addmin("plateau_width_log10_input_min", :plateau_width_log10_input)
    addmin("peak_prominence_min", :peak_prominence)
    addmax("rise_slope_max", :rise_slope)
    addmax("baseline_return_max", :baseline_return)
    if haskey(bspec, "dynamic_range_log10")
        rg = bspec["dynamic_range_log10"]; v = medof(:output_fold_change_log10); c = (rg[1] + rg[2]) / 2
        s += isfinite(v) ? -abs(v - c) : -10.0; ngate += 1
    end
    if ngate == 0   # class-only task: prefer prominent + wide responses
        pp = qaof(:peak_prominence); pw = qaof(:plateau_width_log10_input)
        s = (isfinite(pp) ? pp : 0.0) + 0.1 * (isfinite(pw) ? pw : 0.0)
    end
    return s
end

failing_metric(reason) = String(first(split(reason)))   # leading token = metric/gate name

# ── main ──────────────────────────────────────────────────────────────────────
function main(args)
    o = parse_args(args)
    dp = PhenotyperPolicy()
    policy = PhenotyperPolicy(; K = something(o["K"], dp.K), seed = something(o["seed"], dp.seed))

    pool = o["source"] == "atlas" ? load_atlas_pool(o["atlas"], o["max_candidates"]) :
                                    load_seed_pool(o["seeds"])
    println("candidate pool: $(length(pool)) (network,io) assignments from source=$(o["source"])")

    taskfiles = sort([joinpath(o["tasks"], f) for f in readdir(o["tasks"]) if endswith(f, ".json")])
    o["max_tasks"] > 0 && (taskfiles = taskfiles[1:min(o["max_tasks"], length(taskfiles))])

    cache = Dict{String,Any}(); oracle_calls = Ref(0)
    function pheno(cand, target_class, prior, prior_key)
        key = join([cand.network_id, cand.input, cand.output, String(target_class), prior_key], "|")
        get!(cache, key) do
            oracle_calls[] += 1
            try
                phenotype(cand.model; input_sym = Symbol(cand.input), output_expr = cand.output,
                          prior = prior, policy = policy, target_class = target_class)
            catch e
                (; shape_support = 0.0, stats = Dict{Symbol,Any}(), n_failed = policy.K, n_evaluated = 0)
            end
        end
    end

    task_reports = Any[]
    agg = Dict("pass@1"=>0, "pass@5"=>0, "pass@20"=>0, "n"=>0)

    for tf in taskfiles
        task = to_plain(JSON3.read(read(tf, String)))
        bspec = task["behavior_spec"]
        cls_str = String(bspec["behavior_class"])
        # Refuse unknown classes: mapping to `nothing` would make the phenotyper
        # count EVERY successful draw as in-shape (shape gate disabled), so a typo'd
        # behavior_class would silently "pass any shape". Fail loudly instead.
        haskey(CLASS_MAP, cls_str) ||
            error("unknown behavior_class '$cls_str' in $(basename(tf)); add it to CLASS_MAP " *
                  "(refusing to silently disable the shape gate).")
        target_class = CLASS_MAP[cls_str]
        max_rxn = Int(get(bspec, "max_reactions", 99))
        kd_profile = get(bspec, "kd_profile", nothing)
        # The cache/prior key must capture the FULL kd_profile, not just its mode:
        # now that weak_fraction_min / allow_strong_outliers actually reshape Π, two
        # `mostly_weak` tasks with different params yield different phenotypes and
        # must not share a cache entry.
        prior_key = kd_profile === nothing ? "default" :
                    join(sort(["$k=$(kd_profile[k])" for k in keys(kd_profile)]), ",")
        prior = PhenotypePipeline.reshape_prior(ParameterPrior(), kd_profile)
        succ = get(task, "success", Dict())
        min_rob = Float64(get(succ, "min_robustness_score", 0.2))
        min_design = Float64(get(succ, "min_design_score", 0.0))      # was silently ignored

        results = NamedTuple[]
        for cand in pool
            length(cand.rules) <= max_rxn || continue                 # SQL/motif prefilter (reaction count)
            locate_sym_qK(cand.model, Symbol(cand.input)) === nothing && continue
            pr = pheno(cand, target_class, prior, prior_key)
            hard_pass, reasons, design_score = evaluate_gates(bspec, pr, min_rob)
            pass = hard_pass && design_score >= min_design            # enforce min_design_score
            (hard_pass && !pass) && push!(reasons, "design_score $(rd(design_score)) < min_design_score $min_design")
            osc = objective_score(bspec, pr)
            push!(results, (network_id=cand.network_id, input=cand.input, output=cand.output,
                            shape_support=pr.shape_support, pass=pass, design_score=design_score,
                            objective_score=osc, reasons=reasons))
        end

        # robust-first: shape_support, then user-objective fit (NOT the pass boolean)
        ranked = sort(results; by = r -> (-r.shape_support, -r.objective_score))
        passk(k) = any(r.pass for r in ranked[1:min(k, length(ranked))])
        p1, p5, p20 = passk(1), passk(5), passk(20)
        agg["n"] += 1; agg["pass@1"] += p1; agg["pass@5"] += p5; agg["pass@20"] += p20

        # relaxation hint when nothing passes: which gate failed most often
        attribution = Dict{String,Int}()
        if !p20
            for r in results, rsn in r.reasons
                attribution[failing_metric(rsn)] = get(attribution, failing_metric(rsn), 0) + 1
            end
        end

        top = [Dict("network_id"=>r.network_id, "input"=>r.input, "output"=>r.output,
                    "shape_support"=>rd(r.shape_support), "design_score"=>rd(r.design_score),
                    "pass"=>r.pass, "reasons"=>r.reasons) for r in ranked[1:min(5, length(ranked))]]

        push!(task_reports, Dict(
            "task_id"=>task["task_id"], "behavior_class"=>cls_str,
            "candidates_considered"=>length(results),
            "pass@1"=>p1, "pass@5"=>p5, "pass@20"=>p20,
            "min_robustness_score"=>min_rob, "min_design_score"=>min_design,
            "best_shape_support"=>isempty(ranked) ? 0.0 : rd(ranked[1].shape_support),
            "prior"=>prior_descriptor(prior),     # Π pinned per task (was unrecorded)
            "relaxation_hint"=>isempty(attribution) ? nothing :
                sort(collect(attribution); by = kv -> -kv[2])[1][1],
            "top"=>top,
        ))
        println("  $(rpad(task["task_id"], 38)) pass@1=$(p1) pass@5=$(p5) pass@20=$(p20)  " *
                "cands=$(length(results))  best_ss=$(isempty(ranked) ? 0.0 : rd(ranked[1].shape_support))")
    end

    n = max(agg["n"], 1)
    report = Dict(
        "report_schema"=>REPORT_SCHEMA, "created_at"=>string(now()),
        "phenotyper_version"=>policy.version, "source"=>o["source"],
        "policy"=>Dict("K"=>policy.K, "seed"=>policy.seed, "alpha"=>policy.alpha, "npoints"=>policy.npoints),
        "candidate_pool_size"=>length(pool), "oracle_calls"=>oracle_calls[],
        "aggregate"=>Dict("pass@1_rate"=>rd(agg["pass@1"]/n), "pass@5_rate"=>rd(agg["pass@5"]/n),
                          "pass@20_rate"=>rd(agg["pass@20"]/n), "n_tasks"=>agg["n"]),
        "tasks"=>task_reports,
    )

    println("\nAGGREGATE  pass@1=$(rd(agg["pass@1"]/n))  pass@5=$(rd(agg["pass@5"]/n))  " *
            "pass@20=$(rd(agg["pass@20"]/n))  over $(agg["n"]) tasks; oracle_calls=$(oracle_calls[])")

    if !isempty(o["out"])
        mkpath(dirname(o["out"]))
        open(o["out"], "w") do io; JSON3.pretty(io, report); end
        println("wrote report -> $(o["out"])")
    end
    return report
end

main(ARGS)
