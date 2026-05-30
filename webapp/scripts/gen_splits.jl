#!/usr/bin/env julia
# Phase-2: leakage-aware train/test splits over a phenotype dataset.
# ALL slices of one canonical network stay on the same side (no network/IO leak).
#   julia --project=webapp webapp/scripts/gen_splits.jl --dataset datasets/latent-atlas-v0 --out datasets/latent-atlas-v0/splits [--seed 1234]
#
# Produces iid.json, family_holdout.json, size_holdout.json — each:
#   {"policy":..., "grouped_by":..., "train":[slice_ids], "test":[slice_ids]}
#
# family_holdout (the roadmap's most important split, doc §Splits) holds out whole
# TOPOLOGY FAMILIES, not individual networks: a family key is the graph invariant
# `(#base, #reactions, base-degree-sequence)` from canonicalization.jl, coarser
# than the per-network canonical code. (Before this fix family_holdout was just an
# IID reshuffle with seed+1 and tested nothing IID did not.)

using JSON3, Random, SHA, Dates

const HERE = @__DIR__
include(joinpath(HERE, "..", "src", "reaction_parser.jl"))
using .ReactionParser: parse_reactions, parse_network_structure
include(joinpath(HERE, "..", "src", "canonicalization.jl"))   # topology_family_key

function parse_args(args)
    o = Dict{String,Any}("dataset"=>"datasets/latent-atlas-v0", "out"=>"", "seed"=>1234)
    i = 1
    while i <= length(args)
        a = args[i]
        if     a == "--dataset"; o["dataset"]=args[i+1]; i+=2
        elseif a == "--out";     o["out"]=args[i+1]; i+=2
        elseif a == "--seed";    o["seed"]=parse(Int,args[i+1]); i+=2
        else error("unknown arg: $a")
        end
    end
    isempty(o["out"]) && (o["out"] = joinpath(o["dataset"], "splits"))
    return o
end

slices_of(nets, net2slices) = sort(reduce(vcat, [net2slices[n] for n in nets]; init=String[]))

function write_split(path, policy, train_nets, test_nets, net2slices; extra=Dict{String,Any}())
    payload = Dict{String,Any}("policy"=>policy, "grouped_by"=>"network_id",
                               "n_train_networks"=>length(train_nets), "n_test_networks"=>length(test_nets),
                               "train"=>slices_of(train_nets, net2slices),
                               "test"=>slices_of(test_nets, net2slices))
    merge!(payload, extra)
    open(path, "w") do io; JSON3.pretty(io, payload); end
    println("  $(basename(path)): $(length(train_nets)) train / $(length(test_nets)) test networks")
end

function main(args)
    o = parse_args(args)
    ds = joinpath(o["dataset"], "dataset.jsonl")
    isfile(ds) || error("dataset not found: $ds (run merge_phenotype_shards.jl first)")

    net2slices = Dict{String,Vector{String}}(); net2rxn = Dict{String,Int}()
    net2family = Dict{String,String}()
    for line in eachline(ds)
        isempty(strip(line)) && continue
        r = JSON3.read(line)
        nid = String(r.network_id); sid = String(r.slice_id)
        push!(get!(net2slices, nid, String[]), sid)
        net2rxn[nid] = Int(r.n_reactions)
        if !haskey(net2family, nid)
            rules = hasproperty(r, :rules) ? String.(r.rules) : String[]
            net2family[nid] = isempty(rules) ? "net:" * nid : topology_family_key(rules)
        end
    end
    nets = collect(keys(net2slices))
    fams = unique(values(net2family))
    println("dataset: $(length(nets)) networks, $(sum(length, values(net2slices))) slices, " *
            "$(length(fams)) topology families")

    mkpath(o["out"])

    # iid: 80/20 random split BY NETWORK (keeps all IO slices of a network together)
    rng = MersenneTwister(o["seed"]); shuffled = shuffle(rng, nets)
    cut = max(1, floor(Int, 0.8 * length(shuffled)))
    write_split(joinpath(o["out"], "iid.json"), "iid 80/20 by canonical network",
                shuffled[1:cut], shuffled[cut+1:end], net2slices)

    # family_holdout: hold out whole TOPOLOGY FAMILIES (graph-invariant key), so no
    # network in test shares a family with any network in train.
    fam2nets = Dict{String,Vector{String}}()
    for n in nets; push!(get!(fam2nets, net2family[n], String[]), n); end
    rng2 = MersenneTwister(o["seed"] + 1); shuffled_fams = shuffle(rng2, collect(keys(fam2nets)))
    fcut = max(1, floor(Int, 0.8 * length(shuffled_fams)))
    train_fams = shuffled_fams[1:fcut]; test_fams = shuffled_fams[fcut+1:end]
    fam_train = reduce(vcat, [fam2nets[f] for f in train_fams]; init=String[])
    fam_test  = reduce(vcat, [fam2nets[f] for f in test_fams];  init=String[])
    write_split(joinpath(o["out"], "family_holdout.json"),
                "family holdout: hold out whole topology families (graph-invariant key)",
                fam_train, fam_test, net2slices;
                extra=Dict{String,Any}("grouped_by"=>"topology_family",
                                       "n_train_families"=>length(train_fams),
                                       "n_test_families"=>length(test_fams),
                                       "family_key"=>"(#base,#reactions,base-degree-seq)"))
    if length(fams) <= 1
        @warn "family_holdout: only $(length(fams)) topology family in dataset — split is degenerate " *
              "(every network shares one family). Broaden the candidate corpus before trusting this gate."
    end

    # size_holdout: train on smaller networks, test on the largest reaction count
    Rmax = maximum(values(net2rxn))
    train_nets = [n for n in nets if net2rxn[n] < Rmax]
    test_nets  = [n for n in nets if net2rxn[n] == Rmax]
    write_split(joinpath(o["out"], "size_holdout.json"),
                "size holdout: train r<$Rmax, test r==$Rmax", train_nets, test_nets, net2slices)

    println("splits -> $(o["out"])")
end

main(ARGS)
