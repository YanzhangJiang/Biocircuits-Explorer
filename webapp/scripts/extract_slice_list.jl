#!/usr/bin/env julia
# extract_slice_list.jl — emit a lightweight (network, IO) slice list from an atlas
# SQLite, for the Function-Space-Atlas curve-packet build. Metadata ONLY (rules +
# input/output per slice) — NO phenotyping and NO 495 GB warm: reads just the small
# network_entries + behavior_slices tables. gen_curve_packets.jl consumes this and
# does the phenotyping by rebuilding each model from `rules`.
#
#   julia --project=webapp webapp/scripts/extract_slice_list.jl <atlas.sqlite> <out.jsonl>

using JSON3, SQLite
const HERE = @__DIR__
include(joinpath(HERE, "atlas_read.jl"))
using .AtlasRead: load_network_rules, slice_rows, atlas_fullness

function main(args)
    length(args) >= 2 || error("usage: extract_slice_list.jl <atlas.sqlite> <out.jsonl>")
    atlas, out = args[1], args[2]
    isfile(atlas) || error("atlas not found: $atlas")
    db = SQLite.DB("file:" * abspath(atlas) * "?immutable=1")     # read-only, lock-free
    is_full, ne, bs = atlas_fullness(db)
    is_full || error("atlas has empty network_entries=$ne / behavior_slices=$bs (path_only export)")
    println("atlas: network_entries=$ne behavior_slices=$bs")
    rules_of = load_network_rules(db)
    n = 0; nskip = 0
    open(out, "w") do io
        for row in slice_rows(db)
            nid = String(row.network_id)
            haskey(rules_of, nid) || (nskip += 1; continue)
            rules = rules_of[nid]
            rec = Dict("network_id" => nid, "slice_id" => String(row.slice_id),
                       "input_symbol" => String(row.input_symbol),
                       "output_symbol" => String(row.output_symbol),
                       "n_reactions" => length(rules), "rules" => rules)
            println(io, JSON3.write(rec)); n += 1
            (n % 10000 == 0) && (flush(io); println("  … $n slices"))
        end
    end
    println("wrote $n slice records ($nskip skipped) -> $out")
end

main(ARGS)
