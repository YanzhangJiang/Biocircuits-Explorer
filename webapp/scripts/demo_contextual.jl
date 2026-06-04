#!/usr/bin/env julia
# Step 6 demo: ContextualVersatilityEvaluator. Does one fixed affinity network compute
# different 2-input logic under different accessory-expression (context) levels?
#   julia --project=webapp webapp/scripts/demo_contextual.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

function demo(label, rules, inA, inB, out, ctx)
    model, species, free, _ = build_model(rules, ones(Float64, length(rules)))
    r = evaluate_contextual(model, species, free; input_syms=[inA, inB], output_sym=out,
                            context_sym=ctx, context_levels=[-3.0, 0.0, 3.0], npoints=7, K=8)
    println("\n[$label] ($inA,$inB)->$out  context=$ctx  reprogrammable=$(r["reprogrammable"])  distinct_gates=$(r["distinct_robust_gates"])")
    for p in r["per_context"]
        println("   context $(rpad(p["context"],5)) -> gate=$(rpad(p["gate"],10)) support=$(p["support"]) margin=$(p["margin"]) dec")
    end
end

demo("heavy1", ["A + A <-> C_A_A", "A + C_A_A <-> C_A_A_A", "B + C <-> C_B_C",
                "B + C_A_A_A <-> C_A_A_A_B", "B + C_A_A_A_B <-> C_A_A_A_B_B"], "A", "B", "C_A_A_A_B", "C")
demo("heavy3", ["A + B <-> C_A_B", "A + C <-> C_A_C", "B + C_A_B <-> C_A_B_B",
                "B + C_A_B_B <-> C_A_B_B_B", "C + C_A_B_B_B <-> C_A_B_B_B_C"], "A", "B", "C_A_B", "C")
println("\nCONTEXTUAL.DONE")
