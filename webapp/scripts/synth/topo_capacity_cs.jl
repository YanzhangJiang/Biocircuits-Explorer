# topo_capacity_cs.jl — in-process topology robust-capacity (C_S) estimator. NO HTTP, NO sessions
# (the same in-process path that phenotyped 5120 slices without dying). For each (G,o) it computes,
# from the SAME phenotype_profile machinery (so P_S and C_S are definitionally consistent):
#   P_S = bandpass fraction under the GLOBAL prior Kd~LogUniform(kd_lo,kd_hi)          (global support)
#   C_S = max over n_kd centres θ of r_S(θ), r_S(θ) = bandpass fraction under the LOCAL prior
#         Kd_i ~ LogNormal10(θ_i, impl)                                                 (best local robustness)
# Decisive Phase-1 gate: does a LOW-P_S / HIGH-C_S topology exist (=> local robustness adds signal
# beyond shape_support ranking), or does C_S just track P_S (=> P_S suffices)?
#
#   julia --project=webapp -t auto webapp/scripts/synth/topo_capacity_cs.jl \
#       --topos webapp/scripts/synth/topos_cap.json --out /tmp/topo_cs.jsonl --n-kd 48 --K 8 --impl 0.2

const HERE = @__DIR__
include(joinpath(HERE, "..", "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using BindingAndCatalysis: locate_sym_qK
import JSON3
using Random, Statistics
using LinearAlgebra: BLAS
BLAS.set_num_threads(1)   # pin per-process BLAS: N concurrent shards must NOT each grab all cores
                          # (the HPC oversubscription bug: 128 procs x 192 BLAS threads thrashes)

bp(prof, target) = get(prof.shape_fractions, target, 0.0)

function p_s(model, insym, outexpr; target, K, kd_lo, kd_hi, seed)
    prior = ParameterPrior(default_kd = LogUniform(kd_lo, kd_hi), default_total = PointMass(0.0))
    bp(phenotype_profile(model; input_sym = insym, output_expr = outexpr,
                         prior = prior, policy = PhenotyperPolicy(K = K, seed = seed)), target)
end

# build-once multi-impl C_S: for n_kd shared centres θ (common random numbers across impls), compute
# r_S(θ, impl) at each implementation-error level → C_S[impl] = max_θ r_S. Reuses the (expensive-to-build,
# ROP-enumerated) model across all impls, so the sweep costs ~1 build + n_kd*|impls|*K cheap scan_curves.
function c_s_multi(model, insym, outexpr, nrx; target, n_kd, K, impls, kd_lo, kd_hi, seed)
    rng = MersenneTwister(seed)
    best = Dict(im => 0.0 for im in impls); sums = Dict(im => 0.0 for im in impls); rob = Dict(im => 0 for im in impls)
    for j in 1:n_kd
        θ = kd_lo .+ (kd_hi - kd_lo) .* rand(rng, nrx)
        for im in impls
            lp = ParameterPrior(per_symbol = Dict(Symbol("Kd$i") => LogNormal10(θ[i], im) for i in 1:nrx),
                                default_total = PointMass(0.0))
            r = bp(phenotype_profile(model; input_sym = insym, output_expr = outexpr,
                                     prior = lp, policy = PhenotyperPolicy(K = K, seed = seed + j)), target)
            best[im] = max(best[im], r); sums[im] += r; rob[im] += (r >= 0.5 ? 1 : 0)
        end
    end
    Dict(string(im) => Dict("C_S" => round(best[im]; digits=4), "mean_r" => round(sums[im]/n_kd; digits=4),
                            "frac_robust" => round(rob[im]/n_kd; digits=4)) for im in impls)
end

function parse_args(args)
    o = Dict{String,Any}("topos"=>"", "out"=>"/tmp/topo_cs.jsonl", "n_kd"=>48, "K"=>8, "Kg"=>16,
                         "impl"=>0.2, "impls"=>"", "target"=>"bandpass_with_plateau",
                         "num_shards"=>1, "shard_index"=>0,
                         "kd_lo"=>-3.0, "kd_hi"=>3.0, "max"=>0, "smoke"=>false, "serial"=>false)
    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--topos"; o["topos"] = args[i+1]; i += 2
        elseif a == "--out"; o["out"] = args[i+1]; i += 2
        elseif a == "--n-kd"; o["n_kd"] = parse(Int, args[i+1]); i += 2
        elseif a == "--K"; o["K"] = parse(Int, args[i+1]); i += 2
        elseif a == "--Kg"; o["Kg"] = parse(Int, args[i+1]); i += 2
        elseif a == "--impl"; o["impl"] = parse(Float64, args[i+1]); i += 2
        elseif a == "--impls"; o["impls"] = args[i+1]; i += 2
        elseif a == "--target"; o["target"] = args[i+1]; i += 2
        elseif a == "--max"; o["max"] = parse(Int, args[i+1]); i += 2
        elseif a == "--num-shards"; o["num_shards"] = parse(Int, args[i+1]); i += 2
        elseif a == "--shard-index"; o["shard_index"] = parse(Int, args[i+1]); i += 2
        elseif a == "--smoke"; o["smoke"] = true; i += 1
        elseif a == "--serial"; o["serial"] = true; i += 1
        else; error("unknown arg $a"); end
    end
    o
end

f64(x) = x === nothing ? NaN : Float64(x)

cor_safe(x, y) = (length(unique(x)) > 1 && length(unique(y)) > 1) ? round(cor(x, y); digits=3) : NaN

function analyze(rows, impls, tgt)
    ok = [r for r in rows if haskey(r, "by_impl")]
    println("\n=== Phase-1 GATE analysis (n=$(length(ok)) of $(length(rows))) target=$tgt ===")
    length(ok) < 4 && (println("(too few)"); return)
    P = Float64[r["P_S"] for r in ok]; Pa = Float64[f64(r["P_S_atlas"]) for r in ok]
    println("recomputed-P_S vs atlas-P_S pearson = ", cor_safe(P, Pa), "  (sanity: same pipeline)")
    db = [r for r in ok if r["dominant"] == String(tgt) && r["volume_mean"] !== nothing]
    println("impl |  C_S~P_S | lowP&highC | C_S>=1.0 | ROP_vol~C_S(dom-$tgt) | C_S quantiles[min,p25,p50,p75,max]")
    for im in impls
        ims = string(im)
        C = Float64[r["by_impl"][ims]["C_S"] for r in ok]
        lowhi = count(i -> P[i] <= 0.40 && C[i] >= 0.60, 1:length(ok))
        sat = count(>=(0.999), C); q = round.(quantile(C, [0.0,.25,.5,.75,1.0]); digits=3)
        vc = length(db) >= 4 ?
            cor_safe(Float64[f64(r["volume_mean"]) for r in db], Float64[r["by_impl"][ims]["C_S"] for r in db]) : NaN
        println("$ims  |  $(cor_safe(P, C))  |  $lowhi/$(length(ok))  |  $sat/$(length(ok))  |  $vc  |  $q")
    end
end

function main(args)
    o = parse_args(args)
    topos = JSON3.read(read(o["topos"], String))
    o["smoke"] && (topos = topos[1:min(length(topos), 4)])
    o["max"] > 0 && (topos = topos[1:min(length(topos), o["max"])])
    nkd = o["smoke"] ? 8 : o["n_kd"]; K = o["smoke"] ? 4 : o["K"]
    impls = isempty(o["impls"]) ? [o["impl"]] : [parse(Float64, s) for s in split(o["impls"], ",")]
    o["smoke"] && (impls = [0.2, 0.5])
    tgt = Symbol(o["target"])
    ns = o["num_shards"]; si = o["shard_index"]
    gidxs = [gi for gi in 1:length(topos) if (gi - 1) % ns == si]   # deterministic round-robin shard partition
    n = length(gidxs)
    println("shard $si/$ns  topos=$n (of $(length(topos)))  n_kd=$nkd K=$K Kg=$(o["Kg"]) impls=$impls target=$tgt serial=$(o["serial"])")
    results = Vector{Any}(undef, n)
    work = function(li)
        gi = gidxs[li]; t = topos[gi]
        try
            rules = [String(x) for x in t.reactions]; nrx = length(rules)
            insym = Symbol(String(t.input_symbol)); outexpr = String(t.observe_species)
            model, = build_model(rules, ones(Float64, nrx))
            if locate_sym_qK(model, insym) === nothing
                results[li] = Dict("input_symbol"=>String(t.input_symbol),
                                   "observe_species"=>String(t.observe_species), "error"=>"bad_input_sym"); return
            end
            ps = p_s(model, insym, outexpr; target=tgt, K=o["Kg"], kd_lo=o["kd_lo"], kd_hi=o["kd_hi"], seed=7)
            cs = c_s_multi(model, insym, outexpr, nrx; target=tgt, n_kd=nkd, K=K, impls=impls,
                           kd_lo=o["kd_lo"], kd_hi=o["kd_hi"], seed=1000 + gi)
            results[li] = Dict(
                "input_symbol"=>String(t.input_symbol), "observe_species"=>String(t.observe_species), "n_reactions"=>nrx,
                "dominant"=>(get(t,:dominant,nothing)===nothing ? nothing : String(t.dominant)),
                "P_S_atlas"=>get(t,:P_S_atlas,nothing), "P_S"=>round(ps; digits=4), "by_impl"=>cs,
                "volume_mean"=>get(t,:volume_mean,nothing),
                "robust_path_count"=>get(t,:robust_path_count,nothing), "reactions"=>rules)
        catch e
            results[li] = Dict("error"=>string(e))
        end
    end
    t0 = time()
    if o["serial"] || Threads.nthreads() == 1
        for li in 1:n; work(li); li % 25 == 0 && (println("  ...$li/$n  ($(round(time()-t0;digits=1))s)"); flush(stdout)); end
    else
        done = Threads.Atomic{Int}(0)
        Threads.@threads for li in 1:n
            work(li)
            d = Threads.atomic_add!(done, 1) + 1
            d % 25 == 0 && (println("  ...$d/$n  ($(round(time()-t0;digits=1))s)"); flush(stdout))
        end
    end
    open(o["out"], "w") do io
        for r in results; println(io, JSON3.write(r)); end
    end
    nerr = count(r -> haskey(r, "error"), results)
    println("wrote $(o["out"])  ($(n - nerr) ok, $nerr errors)  in $(round(time()-t0; digits=1))s")
    ns == 1 && analyze(results, impls, tgt)
end

main(ARGS)
