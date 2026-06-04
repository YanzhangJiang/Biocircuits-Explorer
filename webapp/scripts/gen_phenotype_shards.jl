#!/usr/bin/env julia
# Phase-2 dataset generation (source B: a FULL non-path_only atlas).
#
# Streams behavior_slices from one atlas sqlite, deterministically partitions
# them by --shard-index/--num-shards, reconstructs each network, runs the
# single-pass distributional phenotyper, and writes:
#   <out>/shard_<i>.jsonl       one label row per slice
#   <out>/shard_<i>.meta.json   shard counts + provenance
# merge_phenotype_shards.jl assembles these into the dataset + manifest.
#
#   julia --project=webapp webapp/scripts/gen_phenotype_shards.jl \
#       --atlas atlas_full/atlas.sqlite --out datasets/latent-atlas-v0 \
#       --num-shards 32 --shard-index 0 [--K 64 --seed 1234]

using JSON3, Dates, SQLite, DBInterface
const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
include(joinpath(HERE, "..", "src", "latent_atlas", "phenotype_pipeline.jl"))
include(joinpath(HERE, "atlas_read.jl"))
using .ReactionParser: build_model
using .PhenotypePipeline
using .AtlasRead: to_plain, rules_from_record, atlas_fullness, load_network_rules, slice_rows
using BindingAndCatalysis: locate_sym_qK

function parse_args(args)
    o = Dict{String,Any}("atlas"=>"", "out"=>"datasets/latent-atlas-v0",
                         "num_shards"=>1, "shard_index"=>0, "K"=>nothing, "seed"=>nothing,
                         "shard_by"=>"slice",
                         # Π-sweep / sampling knobs (all optional; defaults = the canonical prior).
                         "kd_lo"=>-3.0, "kd_hi"=>3.0, "total_lo"=>nothing, "total_hi"=>nothing,
                         "max_slices"=>0)
    i = 1
    while i <= length(args)
        a = args[i]
        if     a == "--atlas";       o["atlas"]=args[i+1]; i+=2
        elseif a == "--out";         o["out"]=args[i+1]; i+=2
        elseif a == "--num-shards";  o["num_shards"]=parse(Int,args[i+1]); i+=2
        elseif a == "--shard-index"; o["shard_index"]=parse(Int,args[i+1]); i+=2
        elseif a == "--K";           o["K"]=parse(Int,args[i+1]); i+=2
        elseif a == "--seed";        o["seed"]=parse(Int,args[i+1]); i+=2
        # "slice" (default): partition by slice index mod num_shards.
        # "network": partition whole networks (by a stable hash of network_id) so a
        # network's slices stay in ONE shard and its (expensive) build_model is reused
        # across them instead of recomputed per slice.
        elseif a == "--shard-by";    o["shard_by"]=args[i+1]; i+=2
        # Π-sweep: override the default Kd LogUniform range and/or unpin the totals;
        # --max-slices caps slices written PER SHARD (a spread subsample for the sweep).
        elseif a == "--kd-lo";       o["kd_lo"]=parse(Float64,args[i+1]); i+=2
        elseif a == "--kd-hi";       o["kd_hi"]=parse(Float64,args[i+1]); i+=2
        elseif a == "--total-lo";    o["total_lo"]=parse(Float64,args[i+1]); i+=2
        elseif a == "--total-hi";    o["total_hi"]=parse(Float64,args[i+1]); i+=2
        elseif a == "--max-slices";  o["max_slices"]=parse(Int,args[i+1]); i+=2
        else error("unknown arg: $a")
        end
    end
    o["shard_by"] in ("slice","network") || error("--shard-by must be slice|network")
    return o
end

# to_plain / rules_from_record now come from AtlasRead (shared with run_benchmark).

# best (max-volume) family-bucket evidence per slice, if family_buckets is present
function load_family_evidence(db)
    ev = Dict{String,NamedTuple}()
    try
        # SQLite-side aggregation: GROUP BY slice_id + max(volume_mean) returns ONE row
        # per slice (the bare family_label/robust_path_count come from the max-volume row
        # — documented SQLite min/max behaviour). This hands ~59k rows to Julia instead
        # of streaming all 2.67M family_buckets rows here, which was an allocation storm
        # -> constant GC -> the whole 128-worker fleet stalling in futex_do_wait.
        for row in DBInterface.execute(db,
                "SELECT slice_id, family_label, max(volume_mean) AS volume_mean, robust_path_count " *
                "FROM family_buckets GROUP BY slice_id")
            ev[String(row.slice_id)] =
                (volume_mean = row.volume_mean === missing ? 0.0 : Float64(row.volume_mean),
                 robust_path_count = row.robust_path_count === missing ? 0 : Int(row.robust_path_count),
                 family_label = row.family_label === missing ? "" : String(row.family_label))
        end
    catch e
        @warn "family_buckets evidence unavailable" error=e
    end
    return ev
end

mq(stats, m) = (haskey(stats, m) && isfinite(stats[m].median)) ? round(stats[m].median; digits=4) : nothing
qa(stats, m) = (haskey(stats, m) && isfinite(stats[m].q_alpha)) ? round(stats[m].q_alpha; digits=4) : nothing

function main(args)
    o = parse_args(args)
    isfile(o["atlas"]) || error("atlas not found: $(o["atlas"])")
    dp = PhenotyperPolicy()
    policy = PhenotyperPolicy(; K = something(o["K"], dp.K), seed = something(o["seed"], dp.seed))
    ns, si = o["num_shards"], o["shard_index"]
    shard_by = o["shard_by"]
    # stable, cross-process partition of a network onto a shard (FNV-1a, not Julia's
    # session-salted hash, so every shard process agrees on the assignment)
    function _fnv(s::AbstractString)
        h = UInt64(0xcbf29ce484222325)
        for b in codeunits(s); h = (h ⊻ UInt64(b)) * UInt64(0x100000001b3); end
        # MurmurHash3 fmix64 finalizer: FNV-1a's low bits don't avalanche, so a bare
        # `% ns` (low bits) bucketed badly (most shards empty). This spreads entropy
        # across all 64 bits for a near-uniform partition.
        h ⊻= h >> 33; h *= 0xff51afd7ed558ccd
        h ⊻= h >> 33; h *= 0xc4ceb9fe1a85ec53
        h ⊻= h >> 33
        return h
    end
    net_shard(nid) = Int(_fnv(nid) % UInt64(ns))

    # Open READ-ONLY + IMMUTABLE: 128 shard processes opening the same WAL DB
    # read-write serialized on the SQLite database lock (~48-worker ceiling,
    # disk-independent — SSD did not help). immutable=1 skips all locking and the
    # -shm/-wal machinery, giving lock-free concurrent reads. Safe: the atlas is
    # read-only here and was cleanly closed (no pending -wal to apply).
    db = SQLite.DB("file:" * abspath(o["atlas"]) * "?immutable=1")
    is_full, ne, bs = atlas_fullness(db)
    is_full || error("atlas has empty network_entries=$ne / behavior_slices=$bs — this is a path_only " *
                     "export. Rebuild with sqlite_persist_mode=full (Phase-2 source B).")
    println("atlas: network_entries=$ne behavior_slices=$bs  shard $si/$ns  by=$shard_by  K=$(policy.K)")

    rules_of = load_network_rules(db)
    family_ev = load_family_evidence(db)

    # Prior Π actually used (default = canonical: Kd~LogUniform(-3,3), totals pinned).
    default_total = (o["total_lo"] === nothing || o["total_hi"] === nothing) ?
        PointMass(0.0) : LogUniform(Float64(o["total_lo"]), Float64(o["total_hi"]))
    prior = ParameterPrior(; default_kd = LogUniform(Float64(o["kd_lo"]), Float64(o["kd_hi"])),
                             default_total = default_total)
    max_slices = o["max_slices"]

    mkpath(o["out"])
    shard_path = joinpath(o["out"], "shard_$(si).jsonl")
    modelcache = Dict{String,Any}()
    n_slices = 0; n_written = 0; n_skip_build = 0; n_skip_locate = 0

    open(shard_path, "w") do io
        idx = -1
        for row in slice_rows(db)
            idx += 1
            nid = String(row.network_id)
            keep = shard_by == "network" ? (net_shard(nid) == si) : (idx % ns == si)
            keep || continue                       # deterministic shard partition
            n_slices += 1
            haskey(rules_of, nid) || (n_skip_build += 1; continue)
            rules = rules_of[nid]
            model = get!(modelcache, nid) do
                try; m, = build_model(rules, ones(Float64, length(rules))); m; catch; nothing; end
            end
            model === nothing && (n_skip_build += 1; continue)
            insym = Symbol(String(row.input_symbol))
            locate_sym_qK(model, insym) === nothing && (n_skip_locate += 1; continue)

            prof = try
                phenotype_profile(model; input_sym = insym,
                                  output_expr = String(row.output_symbol), policy = policy, prior = prior)
            catch e
                n_skip_build += 1; continue
            end
            ev = get(family_ev, String(row.slice_id), nothing)
            rec = Dict(
                "slice_id"=>String(row.slice_id), "network_id"=>nid,
                "input_symbol"=>String(row.input_symbol), "output_symbol"=>String(row.output_symbol),
                "n_reactions"=>length(rules), "rules"=>rules,
                "dominant_shape"=>String(prof.dominant_shape),
                "shape_fractions"=>Dict(String(k)=>v for (k,v) in prof.shape_fractions),
                "shape_support"=>prof.shape_fractions[prof.dominant_shape == :none ? :flat : prof.dominant_shape],
                "metrics_median"=>Dict(
                    "peak_prominence"=>mq(prof.stats,:peak_prominence),
                    "rise_slope"=>mq(prof.stats,:rise_slope),
                    "fall_slope"=>mq(prof.stats,:fall_slope),
                    "plateau_width_log10_input"=>mq(prof.stats,:plateau_width_log10_input),
                    "output_fold_change_log10"=>mq(prof.stats,:output_fold_change_log10),
                    "baseline_return"=>mq(prof.stats,:baseline_return)),
                "metrics_q_alpha"=>Dict(
                    "fall_slope"=>qa(prof.stats,:fall_slope),
                    "plateau_width_log10_input"=>qa(prof.stats,:plateau_width_log10_input),
                    "peak_prominence"=>qa(prof.stats,:peak_prominence)),
                "atlas_volume_mean"=>ev === nothing ? nothing : ev.volume_mean,
                "atlas_robust_path_count"=>ev === nothing ? nothing : ev.robust_path_count,
                "atlas_family_label"=>ev === nothing ? nothing : ev.family_label,
                "n_failed_draws"=>prof.n_failed,
                "phenotyper_version"=>prof.phenotyper_version,
            )
            println(io, JSON3.write(rec)); flush(io)   # flush per row: real-time progress
            n_written += 1                              # visibility + crash-resilience (a
                                                        # buffered IOStream loses a shard's
                                                        # rows if the worker dies mid-shard)
            (max_slices > 0 && n_written >= max_slices) && break   # Π-sweep subsample cap
        end
    end

    meta = Dict("shard_index"=>si, "num_shards"=>ns, "shard_by"=>shard_by,
                "atlas"=>abspath(o["atlas"]),
                "n_slices_in_shard"=>n_slices, "n_written"=>n_written,
                "n_skipped_build"=>n_skip_build, "n_skipped_locate"=>n_skip_locate,
                "phenotyper_version"=>policy.version, "K"=>policy.K, "seed"=>policy.seed,
                "sampler"=>String(policy.sampler),
                # Π — the parameter prior every label is measured against — pinned here
                # (the ACTUAL prior used, incl. any --kd-lo/hi / --total-lo/hi override).
                "prior"=>prior_descriptor(prior),
                "kd_range"=>[Float64(o["kd_lo"]), Float64(o["kd_hi"])],
                "max_slices_per_shard"=>max_slices,
                "created_at"=>string(now()))
    open(joinpath(o["out"], "shard_$(si).meta.json"), "w") do io; JSON3.pretty(io, meta); end

    println("shard $si: wrote $n_written / $n_slices slices (skip_build=$n_skip_build, " *
            "skip_locate=$n_skip_locate) -> $shard_path")
    return meta
end

main(ARGS)
