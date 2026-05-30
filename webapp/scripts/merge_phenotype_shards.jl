#!/usr/bin/env julia
# Phase-2: merge phenotype shards into one dataset + write the reproducibility
# manifest (latent-atlas-manifest/v0.1.0).
#   julia --project=webapp webapp/scripts/merge_phenotype_shards.jl --dir datasets/latent-atlas-v0 [--atlas atlas_full/atlas.sqlite]

using JSON3, Dates, SHA

function parse_args(args)
    o = Dict{String,Any}("dir"=>"datasets/latent-atlas-v0", "atlas"=>"", "dataset_id"=>"latent-atlas-v0")
    i = 1
    while i <= length(args)
        a = args[i]
        if     a == "--dir";        o["dir"]=args[i+1]; i+=2
        elseif a == "--atlas";      o["atlas"]=args[i+1]; i+=2
        elseif a == "--dataset-id"; o["dataset_id"]=args[i+1]; i+=2
        else error("unknown arg: $a")
        end
    end
    return o
end

git_commit() = try; strip(read(`git rev-parse HEAD`, String)); catch; ""; end

function main(args)
    o = parse_args(args)
    dir = o["dir"]
    shard_files = sort([joinpath(dir, f) for f in readdir(dir)
                        if startswith(f, "shard_") && endswith(f, ".jsonl")])
    isempty(shard_files) && error("no shard_*.jsonl in $dir")

    dataset_path = joinpath(dir, "dataset.jsonl")
    n = 0
    open(dataset_path, "w") do io
        for sf in shard_files, line in eachline(sf)
            isempty(strip(line)) && continue
            println(io, line); n += 1
        end
    end

    metas = NamedTuple[]
    prior = nothing; sampler = ""; meta_atlas = ""
    for f in readdir(dir)
        (startswith(f, "shard_") && endswith(f, ".meta.json")) || continue
        m = JSON3.read(read(joinpath(dir, f), String))
        push!(metas, (n_written = Int(get(m, :n_written, 0)),
                      phenotyper_version = String(get(m, :phenotyper_version, "")),
                      K = Int(get(m, :K, 0)), seed = Int(get(m, :seed, 0))))
        prior === nothing && haskey(m, :prior) && (prior = m[:prior])
        isempty(sampler) && haskey(m, :sampler) && (sampler = String(m[:sampler]))
        isempty(meta_atlas) && haskey(m, :atlas) && (meta_atlas = String(m[:atlas]))
    end
    pv  = isempty(metas) ? "" : metas[1].phenotyper_version
    K   = isempty(metas) ? 0  : metas[1].K
    seed= isempty(metas) ? 0  : metas[1].seed

    # Atlas snapshot hash (reproducibility). Prefer an explicit --atlas; else recover
    # the atlas path the shards recorded in their meta (works when merging on the same
    # machine that built the atlas). A null hash means the dataset is NOT pinned to an
    # atlas snapshot — warn loudly rather than degrade silently. The schema allows null,
    # so the (documented) merge-without-atlas path still produces a VALID manifest.
    atlas_path = !isempty(o["atlas"]) ? o["atlas"] : meta_atlas
    atlas_hash = (!isempty(atlas_path) && isfile(atlas_path)) ?
        bytes2hex(sha256(read(atlas_path))) : nothing
    atlas_hash === nothing && @warn "manifest sqlite_snapshot_hash is null — dataset is NOT " *
        "reproducibility-pinned to an atlas snapshot; pass --atlas <built atlas.sqlite> " *
        "(or merge where the shard-recorded atlas path is reachable)." meta_atlas

    manifest = Dict(
        "manifest_schema_version" => "latent-atlas-manifest/v0.1.0",
        "dataset_id" => o["dataset_id"],
        "code_commit" => git_commit(),
        "source_atlas" => Dict("sqlite_snapshot_hash" => atlas_hash,
                               "build_profile" => "binding_small_v0"),
        "oracle" => Dict("phenotyper_version" => pv, "scan_policy" => "auto-bracket-log-grid-v0",
                         "sampler" => sampler),
        "sampling" => Dict("entity_kind" => "behavior_slice", "num_shards" => length(shard_files),
                           "K" => K, "seed" => seed,
                           # Π pinned: the prior every distributional label was measured against.
                           "parameter_prior" => prior),
        "splits" => Dict{String,Any}(),
        "created_at" => string(now()),
    )
    open(joinpath(dir, "manifest.json"), "w") do io; JSON3.pretty(io, manifest); end

    println("merged $n rows from $(length(shard_files)) shards -> $dataset_path")
    println("manifest -> $(joinpath(dir, "manifest.json"))  (phenotyper=$pv, K=$K, atlas_hash=$(atlas_hash === nothing ? "—" : atlas_hash[1:12]))")
    return manifest
end

main(ARGS)
