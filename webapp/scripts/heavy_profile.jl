#!/usr/bin/env julia
# At-scale validation of the warm-start speedup on REAL heavy d3/mu5 atlas networks
# (5 reactions, complexes up to size 5) — fixtures were tiny (|qK|=3-6). Confirms the
# speedup scales UP and warm-continuation stays exact (zero off-path) on nets with many
# regimes. Cold (per-point from canonical anchor) vs warm (thread previous solution).
#   julia --project=webapp webapp/scripts/heavy_profile.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl"))
using .ReactionParser: build_model

const NETS = [
    ("heavy1", ["A + A <-> C_A_A", "A + C_A_A <-> C_A_A_A", "B + C <-> C_B_C", "B + C_A_A_A <-> C_A_A_A_B", "B + C_A_A_A_B <-> C_A_A_A_B_B"]),
    ("heavy2", ["A + A <-> C_A_A", "A + B <-> C_A_B", "A + C_A_A_B <-> C_A_A_A_B", "A + C_A_B <-> C_A_A_B", "C + C_A_A_A_B <-> C_A_A_A_B_C"]),
    ("heavy3", ["A + B <-> C_A_B", "A + C <-> C_A_C", "B + C_A_B <-> C_A_B_B", "B + C_A_B_B <-> C_A_B_B_B", "C + C_A_B_B_B <-> C_A_B_B_B_C"]),
]

function run_net(label, rules; npoints=161, lo=-5.0, hi=5.0, sweep_idx=1)
    model, = build_model(rules, ones(Float64, length(rules)))
    base = copy(model._anchor_log_qK)
    rng = collect(range(lo, hi, length=npoints))
    println("$label: |qK|=$(length(base)) |x|=$(length(model._anchor_log_x)) reactions=$(length(rules)) sweep_idx=$sweep_idx")
    flush(stdout)
    let q = copy(base); q[sweep_idx] = rng[1]; qK2x(model, q; input_logspace=true, output_logspace=true); end  # JIT warmup
    cold = Vector{Vector{Float64}}(undef, npoints)
    t_cold = @elapsed for (i, v) in enumerate(rng)
        q = copy(base); q[sweep_idx] = v
        cold[i] = qK2x(model, q; input_logspace=true, output_logspace=true)
    end
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

for (l, r) in NETS
    try
        run_net(l, r)
    catch e
        println("$l FAILED: $(sprint(showerror, e))"); flush(stdout)
    end
end
println("HEAVY.DONE")
