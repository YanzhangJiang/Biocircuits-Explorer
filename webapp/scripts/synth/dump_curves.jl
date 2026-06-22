# dump_curves.jl — for each (G,o) in a topo list, find the C_S-maximizing kd and dump its dose-response
# curve (input u vs output, log10) so the certified recoveries can be plotted. Uses the phenotyper's own
# internal scan_curve so the curve matches the label.
#   julia --project=webapp webapp/scripts/synth/dump_curves.jl --topos recovered.json --out curves.json

const HERE = @__DIR__
include(joinpath(HERE, "..", "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using BindingAndCatalysis: locate_sym_qK
import JSON3
using Random, Statistics
const PP = PhenotypePipeline
const TGT = :bandpass_with_plateau

function argmax_theta(model, insym, outexpr, nrx; n_kd=48, K=8, impl=0.2, seed=7)
    rng = MersenneTwister(seed); best = -1.0; bestθ = nothing
    for _ in 1:n_kd
        θ = -3.0 .+ 6.0 .* rand(rng, nrx)
        lp = ParameterPrior(per_symbol = Dict(Symbol("Kd$i") => LogNormal10(θ[i], impl) for i in 1:nrx),
                            default_total = PointMass(0.0))
        r = get(phenotype_profile(model; input_sym=insym, output_expr=outexpr, prior=lp,
                                  policy=PhenotyperPolicy(K=K, seed=seed+1)).shape_fractions, TGT, 0.0)
        r > best && (best = r; bestθ = θ)
    end
    best, bestθ
end

function curve_at(model, insym, outexpr, θ)
    input_idx = locate_sym_qK(model, insym)
    coeffs = Vector{Vector{Float64}}([PP.parse_linear_combination(model, String(outexpr))])
    pol = PhenotyperPolicy()
    prior = ParameterPrior(per_symbol = Dict(Symbol("Kd$i") => PointMass(θ[i]) for i in 1:length(θ)),
                           default_total = PointMass(0.0))
    log_qK = PP.draw_log_qK(model, prior, 1, pol, MersenneTwister(1))
    u_lo, u_hi = pol.auto_bracket ? PP.auto_bracket(model, input_idx, coeffs, log_qK, pol) : (pol.u_lo, pol.u_hi)
    u, ylog, _r, valid = PP.scan_curve(model, input_idx, coeffs, log_qK, u_lo, u_hi, pol.npoints)
    collect(Float64.(u)), collect(Float64.(ylog))
end

function main(args)
    topos = JSON3.read(read(args[findfirst(==("--topos"), args)+1], String))
    outp = args[findfirst(==("--out"), args)+1]
    out = []
    for (i, t) in enumerate(topos)
        try
            rules = [String(x) for x in t.reactions]; nrx = length(rules)
            insym = Symbol(String(t.input_symbol)); outexpr = String(t.observe_species)
            model, = build_model(rules, ones(Float64, nrx))
            best, θ = argmax_theta(model, insym, outexpr, nrx)
            θ === nothing && continue
            u, y = curve_at(model, insym, outexpr, θ)
            push!(out, Dict("input_symbol"=>String(t.input_symbol), "observe_species"=>String(t.observe_species),
                            "dominant"=>get(t,:dominant,nothing)===nothing ? nothing : String(t.dominant),
                            "C_S_local"=>round(best;digits=3), "u"=>round.(u;digits=4), "ylog"=>round.(y;digits=4)))
            println("  [$i/$(length(topos))] $(t.input_symbol)->$(t.observe_species) C_S=$(round(best;digits=2)) npts=$(length(u))"); flush(stdout)
        catch e
            println("  [$i] ERROR $e"); flush(stdout)
        end
    end
    open(outp, "w") do io; JSON3.write(io, out); end
    println("wrote $outp  ($(length(out)) curves)")
end
main(ARGS)
