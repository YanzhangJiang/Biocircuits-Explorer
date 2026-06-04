#!/usr/bin/env julia
# Non-invasive profile + correctness prototype for the phenotyper qK2x speedup.
# For each golden fixture: time the CURRENT per-point COLD sweep (qK2x with no
# startlogx -> homotopy from the canonical anchor each point) vs a WARM-started
# sweep (thread the previous point's solution into startlogx/startlogqK -> short
# homotopy hops). Reports speedup AND, the key risk check, whether warm agrees
# with cold (max |Δlogx| + count of off-path points). Touches nothing in the
# engine; pure measurement.
#   julia --project=webapp webapp/scripts/profile_warmstart.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"))
using .ReactionParser: build_model

const NETS = [
    ("mono", ["L + A <-> AL"], [1.0]),                                            # monotone, 4 vtx
    ("coop", ["A + L <-> AL", "AL + L <-> AL2"], [1.0, 1.0]),                      # two-site / thresholded
    ("hook", ["L + A <-> AL", "L + B <-> BL", "AL + B <-> ALB"], [1.0, 1.0, 1.0]), # prozone/hook, 25 vtx/15 regimes
]

function run_net(label, rules, kd; npoints=161, lo=-4.0, hi=4.0, sweep_idx=1)
    model, = build_model(rules, kd)
    base = copy(model._anchor_log_qK)
    rng = collect(range(lo, hi, length=npoints))
    println("$label: |qK|=$(length(base)) |x|=$(length(model._anchor_log_x)) sweep_idx=$sweep_idx npoints=$npoints")
    flush(stdout)
    # warmup (JIT) — not timed
    let q = copy(base); q[sweep_idx] = rng[1]
        qK2x(model, q; input_logspace=true, output_logspace=true)
    end
    # COLD: every point from the canonical anchor (current behaviour)
    cold = Vector{Vector{Float64}}(undef, npoints)
    t_cold = @elapsed for (i, v) in enumerate(rng)
        q = copy(base); q[sweep_idx] = v
        cold[i] = qK2x(model, q; input_logspace=true, output_logspace=true)
    end
    # WARM: thread previous point's (logx, logqK) as the homotopy start
    warm = Vector{Vector{Float64}}(undef, npoints)
    t_warm = @elapsed begin
        plx = nothing; plqK = nothing
        for (i, v) in enumerate(rng)
            q = copy(base); q[sweep_idx] = v
            x = plx === nothing ?
                qK2x(model, q; input_logspace=true, output_logspace=true) :
                qK2x(model, q; input_logspace=true, output_logspace=true, startlogx=plx, startlogqK=plqK)
            warm[i] = x; plx = x; plqK = q
        end
    end
    diffs = [maximum(abs.(cold[i] .- warm[i])) for i in 1:npoints]
    maxd = maximum(diffs); noff = count(>(1e-4), diffs)
    sp = t_cold / max(t_warm, 1e-9)
    println("$label: t_cold=$(round(t_cold, digits=3))s  t_warm=$(round(t_warm, digits=3))s  speedup=$(round(sp, digits=1))x  maxdiff=$(maxd)  n_offpath(>1e-4)=$noff/$npoints")
    flush(stdout)
end

for (l, r, k) in NETS
    try
        run_net(l, r, k)
    catch e
        println("$l FAILED: $(sprint(showerror, e))")
        flush(stdout)
    end
end
println("PROFILE.DONE")
