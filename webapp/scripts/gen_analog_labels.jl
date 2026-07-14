#!/usr/bin/env julia
# Suite-C analog-surface LABEL pass (sibling of gen_logic_labels.jl). For each (network,
# input-pair, output) it runs the AnalogSurfaceEvaluator (2D grid via scan_parameter_2d +
# 2D warm-start) and records the continuous-surface descriptors instead of a Boolean gate:
# bump_fraction (interior max), dynamic range, ratio-sensing and coactivation correlation.
# Writes a NEW derived dataset (latent-atlas-analog-v0); never the 262GB atlas.sqlite.
#   julia --project=webapp webapp/scripts/gen_analog_labels.jl <siso.jsonl> <out.jsonl> [i/n] [max] [npoints]
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis, JSON3
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

const EVAL_VERSION = "bne-analog-eval/v0.2.0"

dspath   = length(ARGS) >= 1 ? ARGS[1] : error("need SISO dataset.jsonl path")
outpath  = length(ARGS) >= 2 ? ARGS[2] : error("need output jsonl path")
shardarg = length(ARGS) >= 3 ? ARGS[3] : "0/1"
maxslice = length(ARGS) >= 4 ? parse(Int, ARGS[4]) : typemax(Int)
npoints  = length(ARGS) >= 5 ? parse(Int, ARGS[5]) : 21
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

pairs2(v) = [(v[i], v[j]) for i in 1:length(v) for j in (i+1):length(v)]
mkpath(dirname(outpath))
io = open(outpath, "w")
nslice = 0; nok = 0; nskipped = 0; nerr = 0; t0 = time(); outer = false
for (nid, e) in nets
    outer && break
    mod(abs(hash(nid)), sn) == si || continue
    ins = sort(collect(e["ins"])); outs = sort(collect(e["outs"]))
    length(ins) < 2 && continue
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
            r = evaluate_analog(model, species, free; input_syms=[a, b], output_sym=out, npoints=npoints, K=8)
            if !is_complete_evaluator_evidence(r)
                global nskipped += 1
                continue
            end
            row = Dict("network_id" => nid, "rules" => e["rules"], "inputs" => [a, b], "output" => out,
                       "bump_fraction" => r["bump_fraction"],
                       "dynamic_range_decades" => round(r["median_dynamic_range_decades"], digits=3),
                       "ratio_corr" => round(r["median_ratio_corr"], digits=3),
                       "coactivation_corr" => round(r["median_coactivation_corr"], digits=3),
                       "requested_draw_count" => r["requested_draw_count"],
                       "valid_draw_count" => r["valid_draw_count"],
                       "invalid_draw_count" => r["invalid_draw_count"],
                       "partial" => r["partial"],
                       "evidence_status" => r["evidence_status"],
                       "evaluator_version" => EVAL_VERSION)
            println(io, JSON3.write(row)); flush(io)
            global nok += 1
        catch
            global nerr += 1
        end
    end
end
close(io)
dt = time() - t0
println("LABELS=$nok skipped_incomplete=$nskipped errors=$nerr slices_seen=$nslice wall=$(round(dt,digits=1))s rate=$(round(nok/max(dt,1e-9),digits=2)) slices/s")
println("GENANALOG.DONE")
