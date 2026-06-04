#!/usr/bin/env julia
# Sizing probe for a d<=4, mu<=5 extension: (1) enumerate the d in {2,3,4} mu5 family and
# report the d=4 network count, (2) time a few d=4 network builds — build_model + ROP
# vertex enumeration (the expensive, super-linear-in-d step) + one qK2x solve — to anchor
# per-network build and phenotype cost. Pure enumeration + a tiny sample; no atlas write.
#   julia --project=webapp webapp/scripts/est_d4.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BindingAndCatalysis
using BiocircuitsExplorerBackend
const BNE = BiocircuitsExplorerBackend
include(joinpath(@__DIR__, "..", "src", "reaction_parser.jl")); using .ReactionParser: build_model
import Random

espec = BNE.AtlasEnumerationSpec(mode=:complex_growth_binding, base_species_counts=[2, 3, 4],
    min_reactions=1, max_reactions=5, min_template_order=2, max_template_order=5, limit=20_000_000)
profile = BNE.atlas_search_profile_from_raw(Dict(
    "name" => "binding_complex_growth_d4mu5", "max_base_species" => 4, "max_reactions" => 5,
    "max_support" => 5, "slice_mode" => "change", "input_mode" => "totals_only",
    "allow_higher_order_templates" => true, "allow_homomeric_templates" => true, "max_homomer_order" => 5))
t = @elapsed ((specs, summary) = BNE.enumerate_network_specs(espec; search_profile=profile))
n = length(specs)
bycount = get(summary, "generated_by_base_species_count", get(summary, "generated_counts", Dict()))
println("ENUM d∈{2,3,4} μ5: total=$n  by_base_species=$(bycount)  truncated=$(get(summary,"truncated",false))  t=$(round(t,digits=1))s")

bsc(s) = Int(get(get(s, :source_metadata, Dict()), "base_species_count", 0))
d4 = [s for s in specs if bsc(s) == 4]
println("d4 networks = $(length(d4))")
isempty(d4) && (println("EST_D4.DONE"); exit())

Random.seed!(1)
idx = unique(rand(1:length(d4), 12))[1:min(6, end)]
println("--- sample d4 builds (build_model + vertex enumeration + 1 qK2x solve) ---")
for (k, i) in enumerate(idx)
    rules = String.(d4[i][:reactions])
    try
        model = first(build_model(rules, ones(Float64, length(rules))))
        q = copy(model._anchor_log_qK)
        tv = @elapsed assign_vertex_qK(model, q; input_logspace=true, return_idx=true)   # triggers find_all_vertices!
        nv = length(model.vertices_perm)
        ts = @elapsed qK2x(model, q; input_logspace=true, output_logspace=true)
        println("  net$k |rxn|=$(length(rules)) |x|=$(length(model._anchor_log_x)) vtx_enum=$(round(tv,digits=2))s nverts=$nv solve=$(round(ts,digits=3))s")
    catch e
        println("  net$k FAILED: $(sprint(showerror, e))")
    end
end
println("EST_D4.DONE")
