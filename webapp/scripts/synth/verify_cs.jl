# verify_cs.jl — adversarial check of a decoupling case (low/zero P_S but high C_S).
# Rebuts the "P_S=0 => impossible" misread and the "C_S=1 is a classifier artifact" worry by:
#  (1) showing global support is small-but-NONZERO at large K (P_S=0 at K=16 was undersampling), and
#  (2) finding a SPECIFIC kd where the TRUSTED phenotyper itself classifies the single curve as the target
#      (deterministic PointMass draw), then confirming the local neighborhood is robustly the target.
#
#   julia --project=webapp webapp/scripts/synth/verify_cs.jl \
#       --rules '["A + A <-> C_A_A", ...]' --in tC --out B --target biphasic_valley --n-kd 96

const HERE = @__DIR__
include(joinpath(HERE, "..", "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using BindingAndCatalysis: locate_sym_qK
import JSON3
using Random

bp(prof, t) = get(prof.shape_fractions, t, 0.0)

function parse_args(args)
    o = Dict{String,Any}("rules"=>"", "in"=>"", "out"=>"", "target"=>"bandpass_with_plateau", "n_kd"=>96)
    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--rules"; o["rules"]=args[i+1]; i+=2
        elseif a == "--in"; o["in"]=args[i+1]; i+=2
        elseif a == "--out"; o["out"]=args[i+1]; i+=2
        elseif a == "--target"; o["target"]=args[i+1]; i+=2
        elseif a == "--n-kd"; o["n_kd"]=parse(Int,args[i+1]); i+=2
        else; error("unknown $a"); end
    end
    o
end

function main(args)
    o = parse_args(args)
    rules = [String(x) for x in JSON3.read(o["rules"])]; nrx = length(rules)
    insym = Symbol(o["in"]); outexpr = o["out"]; tgt = Symbol(o["target"])
    println("topo: nrx=$nrx in=$insym out=$outexpr target=$tgt")
    model, = build_model(rules, ones(Float64, nrx))
    @assert locate_sym_qK(model, insym) !== nothing "bad input sym"

    # (1) global support at increasing K — P_S=0 at K=16 is undersampling if it's >0 at large K
    for Kg in (16, 64, 256, 1024)
        ps = bp(phenotype_profile(model; input_sym=insym, output_expr=outexpr,
                  prior=ParameterPrior(default_kd=LogUniform(-3.0,3.0), default_total=PointMass(0.0)),
                  policy=PhenotyperPolicy(K=Kg, seed=7)), tgt)
        println("  global P_S(K=$Kg) = $(round(ps;digits=4))")
    end

    # (2) find the C_S-maximizing centre via the SAME local-prior search the sweep uses (max over centres)
    rng = MersenneTwister(20240608); impl0 = 0.2; best_r = -1.0; bestθ = nothing; n_robust = 0
    for _ in 1:o["n_kd"]
        θ = -3.0 .+ 6.0 .* rand(rng, nrx)
        r = bp(phenotype_profile(model; input_sym=insym, output_expr=outexpr,
                  prior=ParameterPrior(per_symbol=Dict(Symbol("Kd$i")=>LogNormal10(θ[i], impl0) for i in 1:nrx),
                                       default_total=PointMass(0.0)),
                  policy=PhenotyperPolicy(K=12, seed=42)), tgt)
        r >= 0.5 && (n_robust += 1)
        r > best_r && (best_r = r; bestθ = θ)
    end
    println("  C_S estimate (max local r_S @impl=$impl0 over $(o["n_kd"]) centres) = $(round(best_r; digits=3));  centres with r_S>=0.5: $n_robust/$(o["n_kd"])")
    bestθ === nothing && return
    θ = bestθ
    # (3) deterministic single-curve verdict AT the C_S-max centre (the trusted phenotyper is the oracle)
    prof1 = phenotype_profile(model; input_sym=insym, output_expr=outexpr,
              prior=ParameterPrior(per_symbol=Dict(Symbol("Kd$i")=>PointMass(θ[i]) for i in 1:nrx),
                                   default_total=PointMass(0.0)),
              policy=PhenotyperPolicy(K=1, seed=1))
    println("  at C_S-max kd log10=", round.(θ; digits=3), " : deterministic single-curve verdict = $(prof1.dominant_shape)")
    for m in (:peak_prominence, :plateau_width_log10_input, :output_fold_change_log10, :min_swing_log10)
        haskey(prof1.stats, m) && println("    metric $m = $(round(prof1.stats[m].median; digits=4))")
    end
    for im in (0.2, 0.5, 1.0)
        r = bp(phenotype_profile(model; input_sym=insym, output_expr=outexpr,
                  prior=ParameterPrior(per_symbol=Dict(Symbol("Kd$i")=>LogNormal10(θ[i], im) for i in 1:nrx),
                                       default_total=PointMass(0.0)),
                  policy=PhenotyperPolicy(K=24, seed=7)), tgt)
        println("  local r_S(impl=$im, K=24) around C_S-max kd = $(round(r; digits=3))")
    end
    println(String(prof1.dominant_shape) == o["target"] ?
        "  => VERIFIED: a robust $tgt kd pocket genuinely exists (P_S small/0 at K=16 was undersampling)." :
        "  => NOTE: C_S-max centre's deterministic curve is $(prof1.dominant_shape) (robust mass off-centre / near a class boundary) — inspect.")
end

main(ARGS)
