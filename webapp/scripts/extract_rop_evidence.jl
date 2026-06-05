#!/usr/bin/env julia
# extract_rop_evidence.jl — per-slice ROP evidence (max-volume family bucket) from an
# atlas SQLite, for attaching rop_summary to curve packets after the fact. One SQL
# GROUP BY (SQLite-side aggregation → ~one row per slice, NOT all family_buckets to
# Julia). Writes slice_id -> {atlas_volume_mean, atlas_robust_path_count, atlas_family_label}.
#
#   julia --project=webapp webapp/scripts/extract_rop_evidence.jl <atlas.sqlite> <rop_evidence.jsonl>

using JSON3, SQLite, DBInterface

function main(args)
    length(args) >= 2 || error("usage: extract_rop_evidence.jl <atlas.sqlite> <out.jsonl>")
    atlas, out = args[1], args[2]
    isfile(atlas) || error("atlas not found: $atlas")
    db = SQLite.DB("file:" * abspath(atlas) * "?immutable=1")
    n = 0
    open(out, "w") do io
        for row in DBInterface.execute(db,
                "SELECT slice_id, family_label, max(volume_mean) AS volume_mean, robust_path_count " *
                "FROM family_buckets GROUP BY slice_id")
            rec = Dict("slice_id" => String(row.slice_id),
                       "atlas_volume_mean" => row.volume_mean === missing ? nothing : Float64(row.volume_mean),
                       "atlas_robust_path_count" => row.robust_path_count === missing ? nothing : Int(row.robust_path_count),
                       "atlas_family_label" => row.family_label === missing ? nothing : String(row.family_label))
            println(io, JSON3.write(rec)); n += 1
            (n % 20000 == 0) && (flush(io); println("  … $n slices"))
        end
    end
    println("wrote $n rop-evidence rows -> $out")
end

main(ARGS)
