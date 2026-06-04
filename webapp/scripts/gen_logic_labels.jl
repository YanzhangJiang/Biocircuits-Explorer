#!/usr/bin/env julia
# Suite-B logic LABEL pass. Reads the lightweight SISO dataset for the network list +
# per-network input/output symbols (NOT the 262GB ROP atlas — that stays immutable; labels
# are a separate derived layer), enumerates (network, input-pair, output) two-input slices,
# runs the LogicTruthTableEvaluator, and writes one row per slice with the realized gate,
# gate_support (fraction of K Kd-draws realizing the modal gate — the logic shape_support),
# and on/off margin. Output is a NEW dataset (latent-atlas-logic-v0), never the atlas.sqlite.
#   julia --project=webapp webapp/scripts/gen_logic_labels.jl <siso_dataset.jsonl> <out.jsonl> [max_slices]
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis, JSON3
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

const EVAL_VERSION = "bne-logic-eval/v0.1.0"

dspath   = length(ARGS) >= 1 ? ARGS[1] : error("need SISO dataset.jsonl path")
outpath  = length(ARGS) >= 2 ? ARGS[2] : error("need output jsonl path")
shardarg = length(ARGS) >= 3 ? ARGS[3] : "0/1"   # "i/n": process only networks with hash%n==i
maxslice = length(ARGS) >= 4 ? parse(Int, ARGS[4]) : typemax(Int)
si, sn = parse.(Int, split(shardarg, "/"))

# Group SISO rows by network -> (rules, input symbols seen, output symbols seen).
println("reading network list from $dspath ...")
nets = Dict{String,Any}()
for line in eachline(dspath)
    isempty(strip(line)) && continue
    r = JSON3.read(line)
    nid = String(r.network_id)
    e = get!(nets, nid) do
        Dict("rules" => String.(collect(r.rules)), "ins" => Set{String}(), "outs" => Set{String}())
    end
    push!(e["ins"], String(r.input_symbol)); push!(e["outs"], String(r.output_symbol))
end
println("networks=$(length(nets))")

pairs2(v) = [(v[i], v[j]) for i in 1:length(v) for j in (i+1):length(v)]

mkpath(dirname(outpath))
io = open(outpath, "w")
nslice = 0; nok = 0; nerr = 0
gatehist = Dict{String,Int}()
t0 = time()
outer = false
for (nid, e) in nets
    outer && break
    mod(abs(hash(nid)), sn) == si || continue    # network-level shard partition
    ins = sort(collect(e["ins"])); outs = sort(collect(e["outs"]))
    length(ins) < 2 && continue            # need >=2 inputs for a 2-input gate
    local model, species, free
    try
        model, species, free, _ = build_model(e["rules"], ones(Float64, length(e["rules"])))
    catch
        continue
    end
    for (a, b) in pairs2(ins), out in outs
        nslice >= maxslice && (global outer = true; break)
        global nslice += 1
        try
            r = evaluate_logic(model, species, free; input_syms=[a, b], output_sym=out,
                               target=nothing, npoints=7, K=8)
            support = count(==(r["realized_table"]), r["tables"]) / length(r["tables"])
            gate = r["realized_gate"]
            row = Dict("network_id" => nid, "rules" => e["rules"], "inputs" => [a, b], "output" => out,
                       "realized_gate" => gate, "gate_support" => round(support, digits=3),
                       "median_margin_decades" => round(r["median_margin_decades"], digits=3),
                       "evaluator_version" => EVAL_VERSION)
            println(io, JSON3.write(row)); flush(io)
            global nok += 1
            gatehist[gate] = get(gatehist, gate, 0) + 1
        catch err
            global nerr += 1
        end
    end
end
close(io)
dt = time() - t0
println("LABELS=$nok  errors=$nerr  slices_seen=$nslice  wall=$(round(dt,digits=1))s  rate=$(round(nok/max(dt,1e-9),digits=1)) slices/s")
println("--- realized 2-input gate distribution (mu<=5 family) ---")
for (g, c) in sort(collect(gatehist); by = x -> -x[2])
    println("  $(rpad(g,12)) $c  ($(round(100c/max(nok,1),digits=1))%)")
end
println("GENLOGIC.DONE")
