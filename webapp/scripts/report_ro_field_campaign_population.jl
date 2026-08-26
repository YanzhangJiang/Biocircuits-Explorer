#!/usr/bin/env julia

# Read-only preparation report for candidate multi-input campaign scopes.  It
# enumerates the current finite grammar and counts potential field-plan groups;
# it never builds a field, writes an Atlas, or launches a worker.

using JSON3
using BiocircuitsExplorerBackend

const RO_FIELD_CAMPAIGN_POPULATION_REPORT_VERSION =
    "bne-ro-field-campaign-population-report/v1.0.0"

function report_ro_field_campaign_population()
    profile = AtlasSearchProfile(
        name="multi_input_mu5_d234_prepare",
        max_base_species=4,
        max_reactions=5,
        max_support=5,
        slice_mode=:change,
        input_mode=:totals_only,
        allow_higher_order_templates=true,
        allow_homomeric_templates=true,
        max_homomer_order=5,
    )
    dimensions = Dict{String,Any}[]
    total_networks = 0
    total_all_rank_plans = 0
    total_two_dimensional_plans = 0
    for dimension in 2:4
        enumeration = AtlasEnumerationSpec(
            mode=:complex_growth_binding,
            base_species_counts=[dimension],
            min_reactions=1,
            max_reactions=5,
            min_template_order=2,
            max_template_order=5,
            require_homomeric_template=false,
            require_complex_growth_template=false,
            require_product_support_at_least=0,
            limit=0,
        )
        networks, summary = enumerate_network_specs(
            enumeration; search_profile=profile)
        reaction_counts = Dict(reaction_count => 0 for reaction_count in 1:5)
        output_group_count = 0
        for network in networks
            reaction_count = network[:source_metadata]["reaction_count"]
            reaction_counts[reaction_count] += 1
            output_group_count += cld(reaction_count, 4)
        end
        network_count = length(networks)
        summary["generated_network_count"] == network_count || error(
            "enumerator summary count drifted from returned population")
        two_dimensional_axis_subsets = binomial(dimension, 2)
        all_axis_subsets = sum(binomial(dimension, rank)
            for rank in 2:dimension)
        two_dimensional_plans =
            output_group_count * two_dimensional_axis_subsets
        all_rank_plans = output_group_count * all_axis_subsets
        push!(dimensions, Dict{String,Any}(
            "input_dimension" => dimension,
            "network_count" => network_count,
            "reaction_count_distribution" => Dict(
                string(key) => reaction_counts[key] for key in 1:5),
            "output_group_count" => output_group_count,
            "two_dimensional_axis_subset_count" =>
                two_dimensional_axis_subsets,
            "all_axis_subset_count_2_to_d" => all_axis_subsets,
            "two_dimensional_field_plan_group_count" =>
                two_dimensional_plans,
            "all_rank_field_plan_group_count" => all_rank_plans,
        ))
        total_networks += network_count
        total_two_dimensional_plans += two_dimensional_plans
        total_all_rank_plans += all_rank_plans
    end
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CAMPAIGN_POPULATION_REPORT_VERSION,
        "grammar" => Dict{String,Any}(
            "mode" => "complex_growth_binding",
            "base_species_counts" => [2, 3, 4],
            "min_reactions" => 1,
            "max_reactions" => 5,
            "min_template_order" => 2,
            "max_template_order" => 5,
            "max_support" => 5,
            "allow_higher_order_templates" => true,
            "allow_homomeric_templates" => true,
            "max_homomer_order" => 5,
        ),
        "candidate_field_policy" => Dict{String,Any}(
            "axis_subsets" => "every_unordered_subset_from_2_to_d",
            "outputs" => "one_per_reaction_product",
            "maximum_outputs_per_plan" => 4,
            "dense_2d_grid_shape" => [17, 17],
            "higher_rank_point_population" =>
                "unknown_until_sparse_policy_is_frozen",
        ),
        "dimensions" => dimensions,
        "total_network_count" => total_networks,
        "total_two_dimensional_field_plan_group_count" =>
            total_two_dimensional_plans,
        "total_all_rank_field_plan_group_count" => total_all_rank_plans,
        "total_two_dimensional_dense_point_count" =>
            total_two_dimensional_plans * 17 * 17,
        "execution_status" =>
            "enumeration_only_no_field_evaluation_or_atlas_write",
    )
    return Dict{String,Any}(
        identity...,
        "report_sha256" => BiocircuitsExplorerBackend._rofc_sha256(identity),
    )
end

function report_ro_field_campaign_population_main(args=ARGS)
    isempty(args) || throw(ArgumentError(
        "population report accepts no command-line arguments"))
    report = report_ro_field_campaign_population()
    println(BiocircuitsExplorerBackend._rofc_canonical_json(report))
    return 0
end

if abspath(PROGRAM_FILE) == @__FILE__
    exit(report_ro_field_campaign_population_main())
end
