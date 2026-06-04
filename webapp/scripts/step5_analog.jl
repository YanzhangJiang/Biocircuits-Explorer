#!/usr/bin/env julia
# Step 5 validation: (1) 2D warm-start in scan_parameter_2d must agree with the cold scan
# (numerical equivalence) and be faster; (2) AnalogSurfaceEvaluator demo on real nets.
#   julia --project=webapp webapp/scripts/step5_analog.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

# resolve a base-species total index the same way the evaluators do
function _idx(sym, free)
    fs = Symbol.(free); s = Symbol(sym)
    i = findfirst(==(s), fs); i !== nothing && return i
    ss = String(sym); (length(ss) > 1 && startswith(ss, "t")) ? findfirst(==(Symbol(ss[2:end])), fs) : nothing
end

function warm_vs_cold(label, rules, inA, inB, outsym; npoints=21)
    model, species, free, _ = build_model(rules, ones(Float64, length(rules)))
    iA = _idx(inA, free); iB = _idx(inB, free); oi = findfirst(==(Symbol(outsym)), Symbol.(species))
    base = copy(model._anchor_log_qK); d = length(free)
    oc = zeros(Float64, length(species)); oc[oi] = 1.0
    rng = collect(range(-4.0, 4.0, length=npoints))
    fp = copy(base); for ix in sort([iA, iB]; rev=true); deleteat!(fp, ix); end
    # warmup (JIT) both paths
    scan_parameter_2d(model, iA, iB, rng[1:2], rng[1:2], oc, fp; warm_start=false)
    scan_parameter_2d(model, iA, iB, rng[1:2], rng[1:2], oc, fp; warm_start=true)
    tc = @elapsed (_, _, gcold, _) = scan_parameter_2d(model, iA, iB, rng, rng, oc, fp; warm_start=false)
    tw = @elapsed (_, _, gwarm, _) = scan_parameter_2d(model, iA, iB, rng, rng, oc, fp; warm_start=true)
    maxd = maximum(abs.(gcold .- gwarm))
    noff = count(>(1e-4), abs.(gcold .- gwarm))
    println("[2D equiv] $label  grid=$(npoints)x$(npoints)  t_cold=$(round(tc,digits=3))s t_warm=$(round(tw,digits=3))s  speedup=$(round(tc/max(tw,1e-9),digits=1))x  maxdiff=$(maxd)  n_offpath(>1e-4)=$noff/$(npoints^2)")
    flush(stdout)
end

function analog_demo(label, rules, inA, inB, outsym)
    model, species, free, _ = build_model(rules, ones(Float64, length(rules)))
    r = evaluate_analog(model, species, free; input_syms=[inA, inB], output_sym=outsym, npoints=21, K=8)
    println("[analog] $label  ($inA,$inB)->$outsym : bump_fraction=$(round(r["bump_fraction"],digits=2))  dyn_range=$(round(r["median_dynamic_range_decades"],digits=2)) dec  ratio_corr=$(round(r["median_ratio_corr"],digits=2))  coact_corr=$(round(r["median_coactivation_corr"],digits=2))")
    flush(stdout)
end

const HEAVY = ["A + B <-> C_A_B", "A + C <-> C_A_C", "B + C_A_B <-> C_A_B_B",
               "B + C_A_B_B <-> C_A_B_B_B", "C + C_A_B_B_B <-> C_A_B_B_B_C"]

println("=== 2D warm-start equivalence + speedup ===")
warm_vs_cold("AND A+B<->C_A_B", ["A + B <-> C_A_B"], "A", "B", "C_A_B")
warm_vs_cold("heavy mu5", HEAVY, "A", "B", "C_A_B")
println("\n=== AnalogSurfaceEvaluator demo ===")
analog_demo("AND coincidence", ["A + B <-> C_A_B"], "A", "B", "C_A_B")
analog_demo("heavy mu5", HEAVY, "A", "B", "C_A_B")
println("\nSTEP5.DONE")
