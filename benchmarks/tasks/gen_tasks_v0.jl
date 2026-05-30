# Emits the 20 Phase-1 benchmark tasks (roadmap Appendix C) as individual JSON
# files next to this script. Pure data; no compute.
#   julia --project=webapp benchmarks/tasks/gen_tasks_v0.jl
using JSON3

const OUT = @__DIR__

# Each task: task_id, natural_language, behavior_spec (flat shape gates per the
# roadmap Task Format), oracle, budget, success. Optional gate keys:
#   rise_slope_max, fall_slope_min, plateau_width_log10_input_min,
#   dynamic_range_log10 [lo,hi], peak_prominence_min, baseline_return_max.
budget() = Dict("max_model_candidates" => 1000, "max_exact_verifier_calls" => 50)
succ(; r = 0.2, s = 0.8) = Dict("verified_pass" => true, "min_robustness_score" => r, "min_design_score" => s)
weak_kd() = Dict("mode" => "mostly_weak", "weak_fraction_min" => 0.75, "allow_strong_outliers" => 1)

tasks = [
  ("01_monotone_activation_r3", "Find a binding network with a monotone activation that rises to a saturating high plateau, at most three reactions.",
   Dict("behavior_class"=>"activation_with_saturation", "max_reactions"=>3)),

  ("02_monotone_repression_r3", "Find a network whose output is repressed down to a low floor as the input increases, at most three reactions.",
   Dict("behavior_class"=>"repression_with_floor", "max_reactions"=>3)),

  ("03_ultrasensitive_moderate_dr", "Find an ultrasensitive (thresholded) activation with a moderate dynamic range of about two orders of magnitude.",
   Dict("behavior_class"=>"thresholded_activation", "max_reactions"=>4, "dynamic_range_log10"=>[1.5, 3.0])),

  ("04_biphasic_peak_plain", "Find a bandpass-like biphasic peak response; no plateau constraint.",
   Dict("behavior_class"=>"biphasic_peak", "max_reactions"=>4, "peak_prominence_min"=>0.3)),

  ("05_bandpass_slow_rise_fast_fall", "Find a bandpass with a gentle rise and a sharp fall, at most four reactions.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "rise_slope_max"=>0.8, "fall_slope_min"=>1.0)),

  ("06_bandpass_wide_plateau", "Find a bandpass with a wide input-axis plateau.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "plateau_width_log10_input_min"=>0.5)),

  ("07_bandpass_dr_2_3", "Find a bandpass whose output fold-change is between two and three orders of magnitude.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "dynamic_range_log10"=>[2.0, 3.0])),

  ("08_biphasic_peak_weak_kd", "Find a biphasic peak using mostly weak dissociation constants.",
   Dict("behavior_class"=>"biphasic_peak", "max_reactions"=>4, "peak_prominence_min"=>0.3, "kd_profile"=>weak_kd())),

  ("09_bandpass_r3", "Find a bandpass with at most three reactions (compare later against r<=4).",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>3)),

  ("10_reference_curve_asym_bandpass", "Match an asymmetric bandpass sketch (slow rise, sharp fall). [reference-curve mode; v0 treats as bandpass_with_plateau gates]",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "rise_slope_max"=>0.8, "fall_slope_min"=>1.2)),

  ("11_plateau_only_no_peak", "Find a plateau-like response (rise to a broad high region) without a strong peak.",
   Dict("behavior_class"=>"activation_with_saturation", "max_reactions"=>4, "plateau_width_log10_input_min"=>0.7)),

  ("12_sharp_cutoff_after_saturation", "Find a response that saturates high then cuts off sharply at high input.",
   Dict("behavior_class"=>"biphasic_peak", "max_reactions"=>4, "fall_slope_min"=>1.5)),

  ("13_low_base_high_mid_nonzero_tail", "Low baseline, high middle response, with a nonzero high-input tail.",
   Dict("behavior_class"=>"biphasic_peak", "max_reactions"=>4, "peak_prominence_min"=>0.2, "baseline_return_max"=>0.6)),

  ("14_robust_bandpass", "Find a bandpass that is robust across the parameter prior.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4), succ(; r = 0.5)),

  ("15_family_holdout_bandpass", "Find a bandpass in a topology family held out from training.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4)),

  ("16_size_holdout_threshold", "Find a threshold response; trained on smaller networks, tested at r=4.",
   Dict("behavior_class"=>"thresholded_activation", "max_reactions"=>4)),

  ("17_composition_weakkd_wideplateau_sharpfall", "Weak Kd AND wide input-axis plateau AND sharp fall (objective-composition holdout).",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "plateau_width_log10_input_min"=>0.5, "fall_slope_min"=>1.0, "kd_profile"=>weak_kd())),

  ("18_paraphrase_bandpass", "Bandpass with gentle rise and sharp fall (same spec, multiple phrasings).",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>4, "rise_slope_max"=>0.8, "fall_slope_min"=>1.0,
        "paraphrases"=>["a hump-shaped response that comes up slowly and drops off fast",
                        "band-pass: slow on, sharp off",
                        "peak in the middle, soft left edge, hard right edge",
                        "rise gently then crash after the optimum",
                        "narrow high band with an abrupt high-dose cutoff"])),

  ("19_infeasible_overconstrained", "Bandpass with at most two reactions, a very wide plateau, AND a very sharp fall (intentionally over-constrained).",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>2, "plateau_width_log10_input_min"=>2.0, "fall_slope_min"=>3.0)),

  ("20_tradeoff_relaxation", "When no candidate satisfies all hard constraints, report the best minimal relaxation.",
   Dict("behavior_class"=>"bandpass_with_plateau", "max_reactions"=>3, "plateau_width_log10_input_min"=>1.5, "fall_slope_min"=>2.5)),
]

written = String[]
for t in tasks
  task_id, nl, bspec = t[1], t[2], t[3]
  success = length(t) >= 4 ? t[4] : succ()
  rec = Dict(
    "task_id" => task_id,
    "natural_language" => nl,
    "behavior_spec" => bspec,
    "oracle" => "phenotyper_v0",
    "budget" => budget(),
    "success" => success,
  )
  path = joinpath(OUT, task_id * ".json")
  open(path, "w") do io
    JSON3.pretty(io, rec)
  end
  push!(written, basename(path))
end
println("wrote $(length(written)) task files to $OUT:")
foreach(f -> println("  ", f), written)
