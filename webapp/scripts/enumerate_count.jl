#!/usr/bin/env julia
# Phase-3 reopen-trigger measurement: enumerate-only network counts for the in-scope
# family at mu=5 (validation: must reproduce the build's 2773) and the mu>5 scale-up
# (mu=6,7). Pure enumeration — NO phenotyper, NO atlas DB. The point is the
# combinatorial growth that defines "when exact enumerate+label stops being feasible".
#   julia --project=webapp webapp/scripts/enumerate_count.jl
import Pkg
Pkg.activate(joinpath(@__DIR__, "..", "..", "webapp"); io=devnull)
using BiocircuitsExplorerBackend
const BNE = BiocircuitsExplorerBackend

const CAP = 2_000_000  # bound memory; report truncated if hit

for mu in (5, 6, 7)
    espec = BNE.AtlasEnumerationSpec(
        mode = :complex_growth_binding,
        base_species_counts = [2, 3],
        min_reactions = 1, max_reactions = mu,
        min_template_order = 2, max_template_order = mu,
        limit = CAP,
    )
    profile = BNE.atlas_search_profile_from_raw(Dict(
        "name" => "binding_complex_growth_mu$(mu)_count",
        "max_base_species" => 3, "max_reactions" => mu, "max_support" => mu,
        "slice_mode" => "change", "input_mode" => "totals_only",
        "allow_higher_order_templates" => true, "allow_homomeric_templates" => true,
        "max_homomer_order" => mu,
    ))
    local n, summary, t
    t = @elapsed begin
        specs, summary = BNE.enumerate_network_specs(espec; search_profile = profile)
        n = length(specs)
    end
    bycount = get(summary, "generated_by_base_species_count", get(summary, "generated_counts", Dict()))
    trunc = get(summary, "truncated", false)
    println("mu=$mu  networks=$n  by_base_species=$(bycount)  truncated=$(trunc)  t=$(round(t, digits=1))s")
    flush(stdout)
end
println("ENUMCOUNT.DONE")
