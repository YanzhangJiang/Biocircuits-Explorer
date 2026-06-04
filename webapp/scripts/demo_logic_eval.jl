#!/usr/bin/env julia
# Demo for step 3 (Behavior Evaluator Registry) + step 4 (LogicTruthTableEvaluator).
# Dumps the registry, then evaluates 2-input logic on real equilibrium-binding networks
# via scan_parameter_2d. The A+B<->C_A_B coincidence net should realize AND.
#   julia --project=webapp webapp/scripts/demo_logic_eval.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"));            using .ReactionParser: build_model
include(joinpath(@__DIR__, "..", "src", "latent_atlas", "evaluators.jl")); using .BehaviorEvaluators

println("=== Behavior Evaluator Registry ===")
for e in REGISTRY
    mark = e.supported ? "OK " : "-- "
    println("  $mark $(rpad(e.key,30)) family=$(rpad(e.behavior_family,22)) arity=$(rpad(e.arity,14)) backend=$(e.backend)")
end

function demo(label, rules, input_syms, output_sym, target)
    model, species, free_syms, _ = build_model(rules, ones(Float64, length(rules)))
    r = evaluate_logic(model, species, free_syms; input_syms=input_syms, output_sym=output_sym,
                       target=target, npoints=7, K=8)
    tgt = target === nothing ? "(exploratory)" : target
    agree = get(r, "truth_table_agreement", "-")
    println("\n[$label]  inputs=$(input_syms) -> out=$(output_sym)  target=$tgt")
    println("  realized_gate=$(r["realized_gate"])  truth_table_agreement=$(agree)  median_margin=$(round(r["median_margin_decades"], digits=2)) decades")
    println("  modal_table(00,01,10,11)=$(r["realized_table"])")
    println("  per-draw tables: $(r["tables"])")
end

demo("AND coincidence", ["A + B <-> C_A_B"], ["A", "B"], "C_A_B", "AND")
demo("heavy mu5 (A,B)->C_A_B",
     ["A + B <-> C_A_B", "A + C <-> C_A_C", "B + C_A_B <-> C_A_B_B",
      "B + C_A_B_B <-> C_A_B_B_B", "C + C_A_B_B_B <-> C_A_B_B_B_C"],
     ["A", "B"], "C_A_B", nothing)
println("\nLOGIC_EVAL.DONE")
