# atlas_read.jl — a thin, lightweight reader for a FULL (non-path_only) atlas
# sqlite, shared by the Phase-2 scripts (run_benchmark.jl --source atlas,
# gen_phenotype_shards.jl) so the network-reconstruction + path_only guard live in
# ONE place instead of being duplicated as raw SQL in each script.
#
# Deliberately depends only on SQLite/DBInterface/JSON3 (NOT the heavy backend
# module) so cluster data-gen jobs start fast. It returns name-form reaction rules
# (`network_entries.record_json.raw_rules`, directly `build_model`-able) and the
# behavior-slice (input,output) assignments — the typed-IR boundary is a separate,
# heavier concern not needed for streaming label generation.

module AtlasRead

using SQLite, DBInterface, JSON3

export to_plain, rules_from_record, atlas_fullness, load_network_rules, slice_rows

to_plain(x::JSON3.Object) = Dict{String,Any}(String(k) => to_plain(v) for (k, v) in x)
to_plain(x::JSON3.Array)  = Any[to_plain(v) for v in x]
to_plain(x) = x

# name-form rules (build_model-able) from a network_entries record
rules_from_record(rec) = haskey(rec, "raw_rules") ? String.(rec["raw_rules"]) :
                         haskey(rec, "reactions") ? String.(rec["reactions"]) : String[]

# (is_full, n_network_entries, n_behavior_slices). is_full=false ⇒ a path_only
# export with no reconstructable topology.
function atlas_fullness(db::SQLite.DB)
    ne = first(DBInterface.execute(db, "SELECT count(*) AS c FROM network_entries")).c
    bs = first(DBInterface.execute(db, "SELECT count(*) AS c FROM behavior_slices")).c
    return (ne > 0 && bs > 0, ne, bs)
end

# network_id => name-form rules (skips records with no reconstructable rules)
function load_network_rules(db::SQLite.DB)
    rules_of = Dict{String,Vector{String}}()
    for row in DBInterface.execute(db, "SELECT network_id, record_json FROM network_entries")
        r = rules_from_record(to_plain(JSON3.read(row.record_json)))
        isempty(r) || (rules_of[String(row.network_id)] = r)
    end
    return rules_of
end

# behavior-slice rows, deterministically ordered by slice_id
slice_rows(db::SQLite.DB) = DBInterface.execute(db,
    "SELECT slice_id, network_id, input_symbol, output_symbol FROM behavior_slices ORDER BY slice_id")

end # module AtlasRead
