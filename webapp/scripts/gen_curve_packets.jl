#!/usr/bin/env julia
# gen_curve_packets.jl — Function-Space Atlas spike (S0+S1).
#
# Emits the K-draw curve PACKET the distributional phenotyper already computes
# (instead of keeping only summary metrics), on a SHARED fixed log-input grid so
# curves are directly comparable in function space. Storage keeps arrays OUT of
# JSON (a binary float32 sidecar) so a later Parquet/Zarr migration is trivial:
#   <out>/curves.f32           binary float32, slice-major / draw-major, grid_n each
#   <out>/packets_index.jsonl  one record per (network, IO) slice [spike-curve-packet/v0]
#   <out>/manifest.json        reproducibility header (grid, prior, policy, versions)
#
#   julia --project=webapp webapp/scripts/gen_curve_packets.jl \
#       --in datasets/latent-atlas-v0/dataset.jsonl --out datasets/curve-packets-v0 \
#       [--limit 200 --K 8 --grid-lo -6 --grid-hi 6 --grid-n 64 --seed 1234]

using JSON3, SHA, Dates, Statistics
const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline

function parse_args(a)
    o = Dict{String,Any}("in"=>"datasets/latent-atlas-v0/dataset.jsonl",
                         "out"=>"datasets/curve-packets-v0",
                         "limit"=>0, "K"=>8, "grid_lo"=>-6.0, "grid_hi"=>6.0,
                         "grid_n"=>64, "seed"=>1234)
    i = 1
    while i <= length(a)
        k = a[i]
        if     k == "--in";      o["in"]=a[i+1]; i+=2
        elseif k == "--out";     o["out"]=a[i+1]; i+=2
        elseif k == "--limit";   o["limit"]=parse(Int,a[i+1]); i+=2
        elseif k == "--K";       o["K"]=parse(Int,a[i+1]); i+=2
        elseif k == "--grid-lo"; o["grid_lo"]=parse(Float64,a[i+1]); i+=2
        elseif k == "--grid-hi"; o["grid_hi"]=parse(Float64,a[i+1]); i+=2
        elseif k == "--grid-n";  o["grid_n"]=parse(Int,a[i+1]); i+=2
        elseif k == "--seed";    o["seed"]=parse(Int,a[i+1]); i+=2
        else error("unknown arg: $k") end
    end
    return o
end

# round, but map non-finite / nothing to JSON null (so the index never carries NaN)
rnan(x) = (x === nothing || (x isa AbstractFloat && !isfinite(x))) ? nothing : round(Float64(x); digits=4)

function draw_dict(d)
    m = d.metrics
    mm(f) = m === nothing ? nothing : rnan(getfield(m, f))
    return Dict("di"=>d.draw_id, "ok"=>d.valid, "cls"=>String(d.shape_class),
                "sc"=>rnan(d.shape_score), "theta"=>round.(d.log_qK; digits=4),
                "pp"=>mm(:peak_prominence), "br"=>mm(:baseline_return),
                "rs"=>mm(:rise_slope), "fs"=>mm(:fall_slope),
                "pw"=>mm(:plateau_width_log10_input), "fc"=>mm(:output_fold_change_log10),
                "nsc"=> m === nothing ? nothing : m.n_sign_changes, "msw"=>mm(:min_swing_log10))
end

function main(args)
    o = parse_args(args)
    isfile(o["in"]) || error("input not found: $(o["in"])")
    mkpath(o["out"])
    K = o["K"]; gl = o["grid_lo"]; gh = o["grid_hi"]; gn = o["grid_n"]
    policy = PhenotyperPolicy(; K = K, seed = o["seed"])
    prior  = ParameterPrior()        # canonical Π: Kd ~ LogUniform(-3,3), totals pinned at 0
    grid_id   = "log_input_grid_v0(lo=$(gl),hi=$(gh),n=$(gn))"
    policy_id = "theta_policy/halton_kd_loguniform_-3_3_totals_pinned/v0"

    srcman = joinpath(dirname(o["in"]), "manifest.json")     # provenance: hash the source manifest
    source_hash = isfile(srcman) ? bytes2hex(sha256(read(srcman))) : bytes2hex(sha256(o["in"]))

    cio = open(joinpath(o["out"], "curves.f32"), "w")
    iio = open(joinpath(o["out"], "packets_index.jsonl"), "w")
    offset = 0; n_read = 0; n_written = 0; n_skipped = 0
    modelcache = Dict{String,Any}()

    for line in eachline(o["in"])
        isempty(strip(line)) && continue
        (o["limit"] > 0 && n_written >= o["limit"]) && break
        n_read += 1
        row = JSON3.read(line, Dict{String,Any})
        rules = String.(row["rules"]); nid = String(row["network_id"]); sid = String(row["slice_id"])
        insym = Symbol(String(row["input_symbol"])); outsym = String(row["output_symbol"])
        model = get!(modelcache, nid) do
            try; m, = build_model(rules, ones(Float64, length(rules))); m; catch; nothing; end
        end
        model === nothing && (n_skipped += 1; continue)
        pk = try
            phenotype_packet(model; input_sym = insym, output_expr = outsym, prior = prior,
                             policy = policy, grid_lo = gl, grid_hi = gh, grid_n = gn)
        catch
            n_skipped += 1; continue
        end
        for d in pk.draws                       # K curves, draw-major, Float32
            write(cio, Float32.(d.y))
        end
        record_id = bytes2hex(sha256("$(sid)|$(String(row["input_symbol"]))>$(outsym)|$(policy_id)"))
        rec = Dict(
            "schema"=>"spike-curve-packet/v0", "record_id"=>record_id,
            "network_id"=>nid, "slice_id"=>sid,
            "io_assignment"=>Dict("input_symbol"=>String(row["input_symbol"]), "output_symbol"=>outsym),
            "n_reactions"=>length(rules), "rules"=>rules,
            "phenotyper_version"=>pk.phenotyper_version,
            "parameter_policy_id"=>policy_id, "u_grid_id"=>grid_id,
            "curve_array_pointer"=>Dict("file"=>"curves.f32", "offset_bytes"=>offset,
                "n_draws"=>K, "n_points"=>gn, "dtype"=>"float32", "layout"=>"draw_major"),
            "draw_count"=>K,
            "validity_summary"=>Dict("n_draws"=>K, "n_valid"=>pk.n_valid, "n_failed"=>pk.n_failed),
            "dominant_shape"=>String(pk.dominant_shape),
            "shape_fractions"=>Dict(String(k)=>round(v; digits=4) for (k,v) in pk.shape_fractions if v > 0),
            "medoid_draw_id"=>pk.medoid_draw_id,
            "rop_summary"=>Dict("atlas_family_label"=>get(row, "atlas_family_label", nothing),
                "atlas_volume_mean"=>get(row, "atlas_volume_mean", nothing),
                "atlas_robust_path_count"=>get(row, "atlas_robust_path_count", nothing)),
            "source_manifest_hash"=>source_hash,
            "draws"=>[draw_dict(d) for d in pk.draws],
        )
        println(iio, JSON3.write(rec)); flush(iio)
        offset += K * gn * 4
        n_written += 1
        (n_written % 50 == 0) && println("  … $(n_written) packets")
    end
    close(cio); close(iio)

    manifest = Dict(
        "schema"=>"spike-curve-packet-manifest/v0", "created_at"=>string(now()),
        "source_in"=>o["in"], "source_manifest_hash"=>source_hash,
        "n_read"=>n_read, "n_written"=>n_written, "n_skipped"=>n_skipped,
        "curves_file"=>"curves.f32", "index_file"=>"packets_index.jsonl",
        "dtype"=>"float32", "layout"=>"slice_major/draw_major",
        "grid"=>Dict("id"=>grid_id, "lo"=>gl, "hi"=>gh, "n_points"=>gn, "scale"=>"log10_input_total"),
        "output_transform"=>"log10", "normalization"=>"none_raw_log10_observable",
        "K"=>K, "seed"=>o["seed"], "sampler"=>String(policy.sampler),
        "parameter_policy_id"=>policy_id, "prior"=>prior_descriptor(prior),
        "phenotyper_version"=>policy.version,
        "engine"=>Dict("julia"=>string(VERSION), "bnc"=>"path:../Bnc_julia"),
    )
    open(joinpath(o["out"], "manifest.json"), "w") do io; JSON3.pretty(io, manifest); end
    println("wrote $(n_written) packets ($(n_skipped) skipped, $(n_read) read) -> $(o["out"])  " *
            "curves=$(round(offset/1e6; digits=2)) MB")
end

main(ARGS)
