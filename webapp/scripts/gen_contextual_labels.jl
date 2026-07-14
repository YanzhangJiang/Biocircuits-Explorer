#!/usr/bin/env julia
# Suite-D contextual-versatility LABEL pass (bounded). For networks with >=3 base species,
# pick 2 inputs + a leftover base as the accessory CONTEXT, and run ContextualVersatilityEvaluator
# (sweep the context total over levels, re-run the logic evaluator each level) to record whether
# one fixed network realises DIFFERENT gates under different expression contexts. Writes a NEW
# latent-atlas-contextual-v0 dataset (never the atlas.sqlite). Heavier per slice than logic
# (contexts x logic), so bounded by max_slices.
#   julia --project=webapp webapp/scripts/gen_contextual_labels.jl <siso.jsonl> <out.jsonl> [i/n] [max_slices]
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis, JSON3
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

const EVAL_VERSION = "bne-contextual-eval/v0.2.0"
dspath   = ARGS[1]; outpath = ARGS[2]
shardarg = length(ARGS) >= 3 ? ARGS[3] : "0/1"
maxslice = length(ARGS) >= 4 ? parse(Int, ARGS[4]) : typemax(Int)
si, sn = parse.(Int, split(shardarg, "/"))

nets = Dict{String,Any}()
for line in eachline(dspath)
    isempty(strip(line)) && continue
    r = JSON3.read(line); nid = String(r.network_id)
    e = get!(nets, nid) do
        Dict("rules" => String.(collect(r.rules)), "ins" => Set{String}(), "outs" => Set{String}())
    end
    push!(e["ins"], String(r.input_symbol)); push!(e["outs"], String(r.output_symbol))
end
_stripT(s) = (ss = String(s); (length(ss) > 1 && startswith(ss, "t")) ? ss[2:end] : ss)
pairs2(v) = [(v[i], v[j]) for i in 1:length(v) for j in (i+1):length(v)]

mkpath(dirname(outpath)); io = open(outpath, "w")
nslice = 0; nok = 0; nskipped = 0; nerr = 0; nrepro = 0; t0 = time(); outer = false
for (nid, e) in nets
    outer && break
    mod(abs(hash(nid)), sn) == si || continue
    ins = sort(collect(e["ins"])); outs = sort(collect(e["outs"]))
    length(ins) < 3 && continue          # need >=3 base species (2 inputs + a context)
    local model, species, free
    try; model, species, free, _ = build_model(e["rules"], ones(Float64, length(e["rules"]))); catch; continue; end
    bases = sort(_stripT.(ins))
    for (a, b) in pairs2(ins)
        ctxs = [c for c in bases if c != _stripT(a) && c != _stripT(b)]
        isempty(ctxs) && continue
        ctx = "t" * first(ctxs)
        for out in outs
            nslice >= maxslice && (global outer = true; break)
            global nslice += 1
            try
                r = evaluate_contextual(model, species, free; input_syms=[a, b], output_sym=out,
                                        context_sym=ctx, context_levels=[-3.0, 0.0, 3.0], npoints=7, K=8)
                if !is_complete_evaluator_evidence(r; unit=:context) ||
                   !(r["reprogrammable"] isa Bool)
                    global nskipped += 1
                    continue
                end
                row = Dict("network_id" => nid, "rules" => e["rules"], "inputs" => [a, b],
                           "context_sym" => ctx, "output" => out,
                           "reprogrammable" => r["reprogrammable"],
                           "distinct_robust_gates" => r["distinct_robust_gates"],
                           "n_distinct_robust_gates" => r["n_distinct_robust_gates"],
                           "per_context" => r["per_context"],
                           "requested_context_count" => r["requested_context_count"],
                           "valid_context_count" => r["valid_context_count"],
                           "invalid_context_count" => r["invalid_context_count"],
                           "partial" => r["partial"],
                           "evidence_status" => r["evidence_status"],
                           "evaluator_version" => EVAL_VERSION)
                println(io, JSON3.write(row)); flush(io); global nok += 1
                r["reprogrammable"] && (global nrepro += 1)
            catch
                global nerr += 1
            end
        end
        outer && break
    end
end
close(io)
println("LABELS=$nok skipped_incomplete=$nskipped errors=$nerr reprogrammable=$nrepro slices_seen=$nslice wall=$(round(time()-t0,digits=1))s")
println("GENCTX.DONE")
