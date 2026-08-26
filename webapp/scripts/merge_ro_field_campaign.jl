#!/usr/bin/env julia

# Local, deterministic merge and second-pass QC for already-produced campaign
# shard-result JSON files.  This script never launches workers or external
# compute and therefore does not confer campaign execution authority.

using JSON3
using BiocircuitsExplorerBackend

const _ROFCAMPAIGN_MERGE_FLAGS = Set((
    "--manifest", "--shard-dir", "--output", "--qc-output",
))

function _rofcampaign_merge_usage()
    return "usage: julia --project=webapp " *
        "webapp/scripts/merge_ro_field_campaign.jl " *
        "--manifest MANIFEST.json --shard-dir SHARDS " *
        "--output CORPUS-LOCK.json --qc-output QC.json"
end

function _rofcampaign_merge_args(args)
    iseven(length(args)) || throw(ArgumentError(
        _rofcampaign_merge_usage()))
    parsed = Dict{String,String}()
    for index in 1:2:length(args)
        flag = String(args[index])
        flag in _ROFCAMPAIGN_MERGE_FLAGS || throw(ArgumentError(
            "unsupported option $(flag); $(_rofcampaign_merge_usage())"))
        haskey(parsed, flag) && throw(ArgumentError(
            "duplicate option $(flag)"))
        value = String(args[index + 1])
        isempty(value) && throw(ArgumentError("$(flag) must not be empty"))
        parsed[flag] = value
    end
    Set(keys(parsed)) == _ROFCAMPAIGN_MERGE_FLAGS || throw(ArgumentError(
        _rofcampaign_merge_usage()))
    parsed["--output"] != parsed["--qc-output"] || throw(ArgumentError(
        "--output and --qc-output must be different files"))
    return parsed
end

function _rofcampaign_read_json(path::AbstractString)
    isfile(path) || throw(ArgumentError("missing JSON file: $(path)"))
    return JSON3.read(read(path, String), Dict{String,Any})
end

function _rofcampaign_canonical_bytes(value)
    canonical = BiocircuitsExplorerBackend._rofc_canonical_json(value)
    return collect(codeunits(canonical * "\n"))
end

function _rofcampaign_write_once(path::AbstractString, value)
    bytes = _rofcampaign_canonical_bytes(value)
    if isfile(path)
        read(path) == bytes || throw(ArgumentError(
            "refusing to replace different existing artifact: $(path)"))
        return path
    end
    directory = dirname(path)
    mkpath(directory)
    temporary_path, io = mktemp(directory; cleanup=true)
    committed = false
    try
        write(io, bytes)
        flush(io)
        close(io)
        mv(temporary_path, path; force=false)
        committed = true
    finally
        isopen(io) && close(io)
        !committed && isfile(temporary_path) && rm(temporary_path)
    end
    return path
end

function merge_ro_field_campaign_main(args=ARGS)
    parsed = _rofcampaign_merge_args(args)
    manifest = _rofcampaign_read_json(parsed["--manifest"])
    shard_directory = parsed["--shard-dir"]
    isdir(shard_directory) || throw(ArgumentError(
        "missing shard directory: $(shard_directory)"))
    shard_paths = sort!(filter(
        path -> endswith(lowercase(path), ".json") && isfile(path),
        readdir(shard_directory; join=true),
    ))
    isempty(shard_paths) && throw(ArgumentError(
        "shard directory contains no JSON results"))
    shard_results = Any[_rofcampaign_read_json(path) for path in shard_paths]
    corpus_lock = merge_ro_field_campaign_shards(manifest, shard_results)
    qc = audit_ro_field_campaign_corpus(
        corpus_lock, manifest, shard_results)
    _rofcampaign_write_once(parsed["--output"], corpus_lock)
    _rofcampaign_write_once(parsed["--qc-output"], qc)
    println("corpus_lock_sha256=", corpus_lock["corpus_lock_sha256"])
    println("qc_sha256=", qc["qc_sha256"])
    return 0
end

if abspath(PROGRAM_FILE) == @__FILE__
    exit(merge_ro_field_campaign_main())
end
