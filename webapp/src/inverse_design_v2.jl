const DEFAULT_COMPILER_VERSION = "gamma_q_v0.1.0"
const DEFAULT_SCREEN_POLICY_VERSION = "support_screen_v0.1.0"
const DEFAULT_MATERIALIZATION_POLICY_VERSION = "lazy_witness_v0.1.0"
const DEFAULT_REFINEMENT_POLICY_VERSION = "polytope_guided_v0.1.0"
const DEFAULT_VOLUME_POLICY_VERSION = "volume_policy_v0.1.0"
const INVERSE_DESIGN_PIPELINE_VERSION = "inverse_design_pipeline_v0.2.0"

const _ALLOWED_QUERY_TOP_LEVEL_KEYS = Set([
    "motif_labels", "exact_labels", "motif_match_mode", "exact_match_mode",
    "input_symbols", "output_symbols", "require_robust", "min_robust_path_count",
    "max_base_species", "max_reactions", "max_support", "max_support_mass",
    "required_regimes", "forbidden_regimes", "required_transitions", "forbidden_transitions",
    "required_path_sequences", "required_sequences", "exists_sequence",
    "forbid_singular_on_witness", "require_witness_feasible", "require_witness_robust",
    "min_witness_volume_mean", "max_witness_path_length", "ranking_mode",
    "collapse_by_network", "pareto_only", "limit",
    "graph_spec", "path_spec", "polytope_spec", "goal",
    "support_count_spec",
])

const _ALLOWED_GRAPH_SPEC_KEYS = Set([
    "required_regimes", "required_nodes", "forbidden_regimes", "forbidden_nodes",
    "required_transitions", "required_edges", "forbidden_transitions", "forbidden_edges",
])

const _ALLOWED_PATH_SPEC_KEYS = Set([
    "required_path_sequences", "required_sequences", "exists_sequence",
    "forbid_singular_on_witness", "max_path_length", "max_witness_path_length",
])

const _ALLOWED_POLYTOPE_SPEC_KEYS = Set([
    "require_feasible", "require_witness_feasible",
    "require_robust", "require_witness_robust",
    "min_volume_mean", "min_witness_volume_mean",
])

const _ALLOWED_GOAL_KEYS = Set([
    "motif", "motifs", "motif_labels",
    "exact", "exacts", "exact_labels",
    "io", "io_pair",
    "input", "inputs", "input_symbols",
    "output", "outputs", "output_symbols",
    "must_regimes", "required_regimes", "regimes", "must_have_regimes",
    "forbid_regimes", "forbidden_regimes", "forbidden_nodes",
    "must_transitions", "required_transitions", "transitions", "must_have_transitions",
    "forbid_transitions", "forbidden_transitions",
    "witness", "witness_path", "path", "required_path_sequences",
    "max_size", "ranking", "ranking_mode",
    "robust", "require_robust",
    "feasible", "require_feasible",
    "collapse", "collapse_by_network",
    "pareto", "pareto_only",
    "limit", "min_volume", "min_volume_mean",
    "max_path_length", "max_witness_path_length",
    "support_count_spec",
])

const _ALLOWED_SUPPORT_COUNT_KEYS = Set([
    "min_counts", "max_counts",
    "required_species", "forbidden_species", "allowed_species",
])

const _ALLOWED_GOAL_MAX_SIZE_KEYS = Set([
    "d", "base_species", "max_base_species",
    "r", "reactions", "max_reactions",
    "support", "s", "max_support",
    "support_mass", "mass", "max_support_mass",
])

const _ALLOWED_REGIME_PREDICATE_KEYS = Set([
    "role", "singular", "asymptotic",
    "output_order_token", "nullity",
    "source", "sink", "branch", "merge",
    "reachable_from_source", "can_reach_sink",
])

const _ALLOWED_TRANSITION_PREDICATE_KEYS = Set([
    "transition_token",
    "from", "to",
    "from_output_order_token", "to_output_order_token",
    "from_role", "to_role",
])

const _DISALLOWED_RAW_IDENTIFIER_KEYS = Set([
    "vertex_idx", "regime_record_id", "transition_record_id",
    "path_idx", "path_record_id", "from_vertex_idx", "to_vertex_idx",
])

function _stable_canonical_value(value)
    if value isa AbstractDict
        out = Dict{String, Any}()
        for key in sort!(String.(collect(keys(value))))
            out[key] = _stable_canonical_value(value[key])
        end
        return out
    elseif value isa AbstractVector || value isa Tuple
        return Any[_stable_canonical_value(item) for item in value]
    elseif value isa Symbol
        return String(value)
    else
        return value
    end
end

function stable_hash(value)
    return bytes2hex(SHA.sha1(JSON3.write(_stable_canonical_value(value))))
end

function _audit_versions(profile::AtlasSearchProfile, compiler_version::AbstractString, policies)
    return Dict(
        "profile_version" => profile.name,
        "compiler_version" => String(compiler_version),
        "screen_policy_version" => String(_raw_get(policies, :screen_policy_version, DEFAULT_SCREEN_POLICY_VERSION)),
        "materialization_policy_version" => String(_raw_get(policies, :materialization_policy_version, DEFAULT_MATERIALIZATION_POLICY_VERSION)),
        "volume_policy_version" => String(_raw_get(policies, :volume_policy_version, DEFAULT_VOLUME_POLICY_VERSION)),
        "refinement_policy_version" => String(_raw_get(policies, :refinement_policy_version, DEFAULT_REFINEMENT_POLICY_VERSION)),
    )
end

function _assert_supported_profile(profile::AtlasSearchProfile)
    profile.name == "binding_small_v0" || throw(ArgumentError("out_of_scope: only `binding_small_v0` is supported in this inverse-design workflow."))
    profile.allow_catalysis && throw(ArgumentError("out_of_scope: catalysis is not supported in `binding_small_v0`."))
    profile.allow_irreversible_steps && throw(ArgumentError("out_of_scope: irreversible steps are not supported in `binding_small_v0`."))
    profile.allow_conformational_switches && throw(ArgumentError("out_of_scope: conformational switching is not supported in `binding_small_v0`."))
    profile.slice_mode == :siso || throw(ArgumentError("out_of_scope: only SISO slices are supported."))
    return profile
end

function _validate_allowed_keys(raw, allowed::Set{String}, context::AbstractString)
    raw isa AbstractDict || return nothing
    for key_any in keys(raw)
        key = String(key_any)
        key in allowed || throw(ArgumentError("unsupported query feature: `$context.$key`"))
    end
    return nothing
end

function _validate_regime_predicate(raw, context::AbstractString)
    raw isa AbstractDict || return nothing
    for (key_any, value) in pairs(raw)
        key = String(key_any)
        key in _DISALLOWED_RAW_IDENTIFIER_KEYS && throw(ArgumentError("unsupported query feature: raw identifiers are not allowed in `$context.$key`"))
        key in _ALLOWED_REGIME_PREDICATE_KEYS || throw(ArgumentError("unsupported query feature: `$context.$key`"))
        value isa AbstractDict && _validate_regime_predicate(value, context * "." * key)
    end
    return nothing
end

function _validate_transition_predicate(raw, context::AbstractString)
    raw isa AbstractDict || return nothing
    for (key_any, value) in pairs(raw)
        key = String(key_any)
        key in _DISALLOWED_RAW_IDENTIFIER_KEYS && throw(ArgumentError("unsupported query feature: raw identifiers are not allowed in `$context.$key`"))
        key in _ALLOWED_TRANSITION_PREDICATE_KEYS || throw(ArgumentError("unsupported query feature: `$context.$key`"))
        if key == "from" || key == "to"
            value isa AbstractDict || throw(ArgumentError("unsupported query feature: `$context.$key` must be a predicate object."))
            _validate_regime_predicate(value, context * "." * key)
        end
    end
    return nothing
end

function _validate_predicate_vectors(values, validator::Function, context::AbstractString)
    values === nothing && return nothing
    values isa AbstractVector || return nothing
    for (idx, value) in enumerate(values)
        value isa AbstractDict || continue
        validator(value, string(context, "[", idx, "]"))
    end
    return nothing
end

function _validate_path_sequence_vectors(values, context::AbstractString)
    values === nothing && return nothing
    values isa AbstractVector || return nothing
    for (idx, sequence) in enumerate(values)
        sequence isa AbstractVector || continue
        for (jdx, predicate) in enumerate(sequence)
            predicate isa AbstractDict || continue
            _validate_regime_predicate(predicate, string(context, "[", idx, "][", jdx, "]"))
        end
    end
    return nothing
end

function _validate_raw_query_scope(raw_query)
    raw_query isa AbstractDict || return nothing
    _validate_allowed_keys(raw_query, _ALLOWED_QUERY_TOP_LEVEL_KEYS, "query")

    if _raw_haskey(raw_query, :graph_spec)
        graph_spec = _raw_get(raw_query, :graph_spec, nothing)
        _validate_allowed_keys(graph_spec, _ALLOWED_GRAPH_SPEC_KEYS, "query.graph_spec")
        _validate_predicate_vectors(_query_value(raw_query, :required_regimes; section=graph_spec, aliases=[:required_nodes]), _validate_regime_predicate, "query.graph_spec.required_regimes")
        _validate_predicate_vectors(_query_value(raw_query, :forbidden_regimes; section=graph_spec, aliases=[:forbidden_nodes]), _validate_regime_predicate, "query.graph_spec.forbidden_regimes")
        _validate_predicate_vectors(_query_value(raw_query, :required_transitions; section=graph_spec, aliases=[:required_edges]), _validate_transition_predicate, "query.graph_spec.required_transitions")
        _validate_predicate_vectors(_query_value(raw_query, :forbidden_transitions; section=graph_spec, aliases=[:forbidden_edges]), _validate_transition_predicate, "query.graph_spec.forbidden_transitions")
    end

    if _raw_haskey(raw_query, :path_spec)
        path_spec = _raw_get(raw_query, :path_spec, nothing)
        _validate_allowed_keys(path_spec, _ALLOWED_PATH_SPEC_KEYS, "query.path_spec")
        values = _query_value(raw_query, :required_path_sequences; section=path_spec, aliases=[:required_sequences])
        _validate_path_sequence_vectors(values, "query.path_spec.required_path_sequences")
        exists_sequence = _query_value(raw_query, :exists_sequence; section=path_spec)
        exists_sequence === nothing || _validate_path_sequence_vectors([exists_sequence], "query.path_spec.exists_sequence")
    end

    if _raw_haskey(raw_query, :polytope_spec)
        polytope_spec = _raw_get(raw_query, :polytope_spec, nothing)
        _validate_allowed_keys(polytope_spec, _ALLOWED_POLYTOPE_SPEC_KEYS, "query.polytope_spec")
    end

    if _raw_haskey(raw_query, :goal)
        goal = _raw_get(raw_query, :goal, nothing)
        _validate_allowed_keys(goal, _ALLOWED_GOAL_KEYS, "query.goal")
        if goal isa AbstractDict && _raw_haskey(goal, :max_size)
            _validate_allowed_keys(_raw_get(goal, :max_size, nothing), _ALLOWED_GOAL_MAX_SIZE_KEYS, "query.goal.max_size")
        end
        _validate_predicate_vectors(_query_value(goal, :must_regimes; aliases=[:required_regimes, :regimes, :must_have_regimes]), _validate_regime_predicate, "query.goal.must_regimes")
        _validate_predicate_vectors(_query_value(goal, :forbid_regimes; aliases=[:forbidden_regimes, :forbidden_nodes]), _validate_regime_predicate, "query.goal.forbid_regimes")
        _validate_predicate_vectors(_query_value(goal, :must_transitions; aliases=[:required_transitions, :transitions, :must_have_transitions]), _validate_transition_predicate, "query.goal.must_transitions")
        _validate_predicate_vectors(_query_value(goal, :forbid_transitions; aliases=[:forbidden_transitions]), _validate_transition_predicate, "query.goal.forbid_transitions")
        _validate_path_sequence_vectors(_goal_path_sequence_list(_query_value(goal, :witness; aliases=[:witness_path, :path, :required_path_sequences])), "query.goal.witness")
        if _raw_haskey(goal, :support_count_spec)
            _validate_allowed_keys(_raw_get(goal, :support_count_spec, nothing), _ALLOWED_SUPPORT_COUNT_KEYS, "query.goal.support_count_spec")
        end
    end

    if _raw_haskey(raw_query, :support_count_spec)
        _validate_allowed_keys(_raw_get(raw_query, :support_count_spec, nothing), _ALLOWED_SUPPORT_COUNT_KEYS, "query.support_count_spec")
    end

    _validate_predicate_vectors(_query_value(raw_query, :required_regimes; aliases=[:required_nodes]), _validate_regime_predicate, "query.required_regimes")
    _validate_predicate_vectors(_query_value(raw_query, :forbidden_regimes; aliases=[:forbidden_nodes]), _validate_regime_predicate, "query.forbidden_regimes")
    _validate_predicate_vectors(_query_value(raw_query, :required_transitions; aliases=[:required_edges]), _validate_transition_predicate, "query.required_transitions")
    _validate_predicate_vectors(_query_value(raw_query, :forbidden_transitions; aliases=[:forbidden_edges]), _validate_transition_predicate, "query.forbidden_transitions")
    _validate_path_sequence_vectors(_query_value(raw_query, :required_path_sequences; aliases=[:required_sequences]), "query.required_path_sequences")

    return nothing
end

function _compiled_predicate_token(predicate)
    return stable_hash(predicate)[1:12]
end

function compile_behavior_filters(query::AtlasQuerySpec)
    return Dict(
        "motif_labels" => collect(query.motif_labels),
        "exact_labels" => collect(query.exact_labels),
        "motif_match_mode" => String(query.motif_match_mode),
        "exact_match_mode" => String(query.exact_match_mode),
        "require_robust" => query.require_robust,
        "min_robust_path_count" => query.min_robust_path_count,
        "input_symbols" => collect(query.input_symbols),
        "output_symbols" => collect(query.output_symbols),
    )
end

function compile_graph_predicates(query::AtlasQuerySpec)
    return (
        Any[_materialize(item) for item in query.required_regimes],
        Any[_materialize(item) for item in query.forbidden_regimes],
        Any[_materialize(item) for item in query.required_transitions],
        Any[_materialize(item) for item in query.forbidden_transitions],
    )
end

function compile_path_automaton(query::AtlasQuerySpec)
    sequences = Dict{String, Any}[]
    state_count = 1
    for (idx, sequence) in enumerate(query.required_path_sequences)
        predicates = Any[_materialize(predicate) for predicate in sequence]
        tokens = [_compiled_predicate_token(predicate) for predicate in predicates]
        push!(sequences, Dict(
            "sequence_idx" => idx,
            "predicate_tokens" => tokens,
            "predicates" => predicates,
        ))
        state_count += length(tokens)
    end
    return Dict(
        "automaton_kind" => "predicate_sequence_nfa",
        "token_domain" => "predicate_tokens",
        "accept_mode" => isempty(sequences) ? "trivial" : "exists_per_required_sequence",
        "state_count" => state_count,
        "sequences" => sequences,
    )
end

function compile_polytope_constraints(query::AtlasQuerySpec)
    return Dict(
        "require_feasible" => query.require_witness_feasible,
        "require_robust" => query.require_witness_robust,
        "min_volume_mean" => query.min_witness_volume_mean,
        "max_path_length" => query.max_witness_path_length,
        "forbid_singular_on_witness" => query.forbid_singular_on_witness,
    )
end

function _normalized_string_int_dict(raw)
    raw isa AbstractDict || return Dict{String, Int}()
    out = Dict{String, Int}()
    for (key, value) in pairs(raw)
        out[String(key)] = Int(value)
    end
    return out
end

function compile_count_envelope(raw_query, query::AtlasQuerySpec)
    basis = ["base_species_count", "total_species_count", "max_support", "support_mass"]
    A = Vector{Vector{Float64}}()
    b = Float64[]
    reasons = String[]

    min_base_species = isempty(query.input_symbols) ? 0 : length(unique(query.input_symbols))
    min_total_species = length(unique(vcat(query.input_symbols, query.output_symbols)))

    if min_base_species > 0
        push!(A, [-1.0, 0.0, 0.0, 0.0])
        push!(b, -Float64(min_base_species))
        push!(reasons, "min_base_species_from_requested_inputs")
    end
    if min_total_species > 0
        push!(A, [0.0, -1.0, 0.0, 0.0])
        push!(b, -Float64(min_total_species))
        push!(reasons, "min_total_species_from_requested_io")
    end
    if query.max_base_species !== nothing
        push!(A, [1.0, 0.0, 0.0, 0.0])
        push!(b, Float64(query.max_base_species))
        push!(reasons, "max_base_species")
    end
    if query.max_support !== nothing
        push!(A, [0.0, 0.0, 1.0, 0.0])
        push!(b, Float64(query.max_support))
        push!(reasons, "max_support")
    end
    if query.max_support_mass !== nothing
        push!(A, [0.0, 0.0, 0.0, 1.0])
        push!(b, Float64(query.max_support_mass))
        push!(reasons, "max_support_mass")
    end

    support_count_raw = if _raw_haskey(raw_query, :support_count_spec)
        _raw_get(raw_query, :support_count_spec, nothing)
    elseif _raw_haskey(raw_query, :goal) && _raw_haskey(_raw_get(raw_query, :goal, nothing), :support_count_spec)
        _raw_get(_raw_get(raw_query, :goal, nothing), :support_count_spec, nothing)
    else
        nothing
    end

    min_counts = support_count_raw === nothing ? Dict{String, Int}() : _normalized_string_int_dict(_raw_get(support_count_raw, :min_counts, Dict{String, Int}()))
    max_counts = support_count_raw === nothing ? Dict{String, Int}() : _normalized_string_int_dict(_raw_get(support_count_raw, :max_counts, Dict{String, Int}()))
    required_species = support_count_raw === nothing ? String[] : _sorted_unique_strings(_raw_get(support_count_raw, :required_species, String[]))
    forbidden_species = support_count_raw === nothing ? String[] : _sorted_unique_strings(_raw_get(support_count_raw, :forbidden_species, String[]))
    allowed_species = support_count_raw === nothing ? String[] : _sorted_unique_strings(_raw_get(support_count_raw, :allowed_species, String[]))

    for species in required_species
        min_counts[species] = max(get(min_counts, species, 0), 1)
    end
    for species in forbidden_species
        max_counts[species] = 0
    end

    return Dict(
        "kind" => "compiled_count_envelope",
        "species_constraint_mode" => isempty(min_counts) && isempty(max_counts) && isempty(allowed_species) ? "trivial" : "explicit_species_counts",
        "species_constraints" => Dict(
            "min_counts" => min_counts,
            "max_counts" => max_counts,
            "allowed_species" => allowed_species,
            "forbidden_species" => forbidden_species,
        ),
        "feature_constraints" => Dict(
            "basis" => basis,
            "A" => A,
            "b" => b,
            "constraint_reasons" => reasons,
            "min_base_species" => min_base_species,
            "min_total_species" => min_total_species,
        ),
        "required_input_symbols" => collect(query.input_symbols),
        "required_output_symbols" => collect(query.output_symbols),
    )
end

function compile_query(raw_query, profile::AtlasSearchProfile=atlas_search_profile_binding_small_v0(), compiler_version::AbstractString=DEFAULT_COMPILER_VERSION; strict::Bool=true, query_spec::Union{Nothing, AtlasQuerySpec}=nothing)
    _assert_supported_profile(profile)
    strict && _validate_raw_query_scope(raw_query)
    query = query_spec === nothing ? atlas_query_spec_from_raw(raw_query) : query_spec

    qb = compile_behavior_filters(query)
    phi_v_pos, phi_v_neg, phi_e_pos, phi_e_neg = compile_graph_predicates(query)
    a_q = compile_path_automaton(query)
    lambda_q = compile_polytope_constraints(query)
    q_s = compile_count_envelope(raw_query, query)
    kappa = Dict(
        "ranking_mode" => String(query.ranking_mode),
        "collapse_by_network" => query.collapse_by_network,
        "pareto_only" => query.pareto_only,
        "limit" => query.limit,
        "max_reactions" => query.max_reactions,
        "max_base_species" => query.max_base_species,
        "max_support" => query.max_support,
        "max_support_mass" => query.max_support_mass,
    )

    gamma_q = Dict(
        "schema_version" => "0.1.0",
        "profile_version" => profile.name,
        "compiler_version" => String(compiler_version),
        "Q_B" => qb,
        "Phi_V_positive" => phi_v_pos,
        "Phi_V_negative" => phi_v_neg,
        "Phi_E_positive" => phi_e_pos,
        "Phi_E_negative" => phi_e_neg,
        "A_Q" => a_q,
        "Lambda_Q" => lambda_q,
        "Q_s" => q_s,
        "kappa" => kappa,
        "query" => atlas_query_spec_to_dict(query),
    )
    gamma_q["h_Q"] = stable_hash(gamma_q)
    return gamma_q
end

function _raw_rules_from_candidate(raw_network_or_rules)
    if raw_network_or_rules isa AbstractVector
        return String.(collect(raw_network_or_rules))
    elseif raw_network_or_rules isa AbstractDict
        return String.(_raw_get(raw_network_or_rules, :raw_rules, _raw_get(raw_network_or_rules, :reactions, String[])))
    else
        return String[]
    end
end

function _support_signature_from_validation(validation)
    supports = validation["supports"]
    supports === nothing && return "unsupported::missing_supports"
    free_syms = Symbol.(validation["free_symbols"])
    species_syms = sort!(collect(keys(supports)); by=string)

    isempty(species_syms) && return "support::empty_species::d=$(length(free_syms))"

    candidates = String[]
    for perm in _all_permutations(copy(free_syms))
        remap = Dict(sym => idx for (idx, sym) in enumerate(perm))
        serialized = sort!([_canonical_term_string(sym, supports, remap) for sym in species_syms])
        push!(candidates, join(serialized, "|"))
    end

    sort!(candidates)
    return "support::d=$(length(free_syms))::" * first(candidates)
end

function _support_graph_payload(validation)
    supports = validation["supports"]
    supports === nothing && return Dict(
        "base_symbols" => String[],
        "species_symbols" => String[],
        "product_symbols" => String[],
        "species_supports" => Vector{Vector{Int}}(),
        "product_supports" => Vector{Vector{Int}}(),
        "degree_vector" => Int[],
        "assignment_count" => 0,
    )

    base_symbols = sort!(String.(validation["free_symbols"]))
    base_lookup = Dict(sym => idx for (idx, sym) in enumerate(Symbol.(base_symbols)))
    species_symbols = sort!(String.(collect(keys(supports))))
    product_symbols = sort!(String.(Symbol[sym for sym in keys(supports) if String(sym) ∉ base_symbols]))
    species_supports = Vector{Vector{Int}}()
    product_supports = Vector{Vector{Int}}()

    for sym_str in species_symbols
        support_set = sort!(collect(base_lookup[base] for base in supports[Symbol(sym_str)]))
        push!(species_supports, support_set)
        sym_str in product_symbols && push!(product_supports, support_set)
    end

    degree_vector = Int[
        length(species_supports[idx]) for idx in eachindex(species_supports)
    ]

    return Dict(
        "base_symbols" => base_symbols,
        "species_symbols" => species_symbols,
        "product_symbols" => product_symbols,
        "species_supports" => species_supports,
        "product_supports" => product_supports,
        "degree_vector" => degree_vector,
        "assignment_count" => length(species_symbols),
    )
end

function canonicalize_network(raw_network_or_rules; profile::AtlasSearchProfile=atlas_search_profile_binding_small_v0())
    rules = _raw_rules_from_candidate(raw_network_or_rules)
    validation = validate_rules_against_profile(rules, profile)
    metrics = validation["metrics"]

    canonical_code = if validation["valid"]
        try
            canonical_network_code(rules)
        catch
            "uncanonicalized::" * join(sort(strip.(rules)), "|")
        end
    else
        "invalid::" * stable_hash(rules)
    end

    support_graph = _support_graph_payload(validation)
    support_signature = _support_signature_from_validation(validation)

    return Dict(
        "network_id" => canonical_code,
        "canonical_code" => canonical_code,
        "raw_rules" => rules,
        "validation" => _materialize(validation),
        "base_species_count" => metrics === nothing ? 0 : Int(metrics["base_species_count"]),
        "total_species_count" => metrics === nothing ? 0 : Int(metrics["total_species_count"]),
        "max_support" => metrics === nothing ? 0 : Int(metrics["max_support"]),
        "support_mass" => metrics === nothing ? 0 : Int(metrics["support_mass"]),
        "support_map" => metrics === nothing ? Dict{String, Any}() : _materialize(metrics["support_map"]),
        "support_signature" => support_signature,
        "support_graph" => support_graph,
        "graph_class" => profile.name,
        "profile_version" => profile.name,
    )
end

emit_support_signature(canonical_network) = String(_raw_get(canonical_network, :support_signature, ""))

function _support_feature_vector(canonical_network)
    return Float64[
        Float64(_raw_get(canonical_network, :base_species_count, 0)),
        Float64(_raw_get(canonical_network, :total_species_count, 0)),
        Float64(_raw_get(canonical_network, :max_support, 0)),
        Float64(_raw_get(canonical_network, :support_mass, 0)),
    ]
end

function _feature_constraint_result(canonical_network, q_s)
    feature_constraints = _raw_get(q_s, :feature_constraints, Dict{String, Any}())
    basis = collect(_raw_get(feature_constraints, :basis, String[]))
    A = collect(_raw_get(feature_constraints, :A, Any[]))
    b = collect(_raw_get(feature_constraints, :b, Any[]))
    feature_vector = _support_feature_vector(canonical_network)

    for idx in eachindex(A)
        row = Float64.(collect(A[idx]))
        rhs = Float64(b[idx])
        lhs = dot(row, feature_vector)
        if lhs > rhs + 1e-9
            return Dict(
                "status" => "fail",
                "pass" => false,
                "proof_artifact" => Dict(
                    "kind" => "support_feature_envelope_violation",
                    "row_index" => idx,
                    "basis" => basis,
                    "row" => row,
                    "rhs" => rhs,
                    "lhs" => lhs,
                    "feature_vector" => feature_vector,
                ),
                "feature_vector" => feature_vector,
            )
        end
    end

    return Dict(
        "status" => "pass",
        "pass" => true,
        "feature_vector" => feature_vector,
        "proof_artifact" => Dict(
            "kind" => "support_feature_envelope_feasible",
            "basis" => basis,
            "feature_vector" => feature_vector,
        ),
    )
end

function _species_constraint_maps(q_s)
    species_constraints = _raw_get(q_s, :species_constraints, Dict{String, Any}())
    return (
        Dict{String, Int}(_materialize(_raw_get(species_constraints, :min_counts, Dict{String, Int}()))),
        Dict{String, Int}(_materialize(_raw_get(species_constraints, :max_counts, Dict{String, Int}()))),
        Set(_sorted_unique_strings(_raw_get(species_constraints, :allowed_species, String[]))),
        Set(_sorted_unique_strings(_raw_get(species_constraints, :forbidden_species, String[]))),
    )
end

function _enumerate_bounding_count_vectors(canonical_network, q_s)
    support_graph = _raw_get(canonical_network, :support_graph, Dict{String, Any}())
    species_symbols = collect(_raw_get(support_graph, :species_symbols, String[]))
    degree_vector = Int.(collect(_raw_get(support_graph, :degree_vector, Int[])))
    d = Int(_raw_get(canonical_network, :base_species_count, 0))
    min_counts, max_counts, allowed_species, forbidden_species = _species_constraint_maps(q_s)

    length(species_symbols) == length(degree_vector) || return Dict(
        "status" => "fail",
        "vectors" => Vector{Vector{Int}}(),
        "reason" => "support_graph_dimension_mismatch",
    )

    lower_bounds = Int[]
    upper_bounds = Int[]
    for (idx, symbol) in enumerate(species_symbols)
        lower = get(min_counts, symbol, 0)
        upper = get(max_counts, symbol, degree_vector[idx])
        !isempty(allowed_species) && !(symbol in allowed_species) && (upper = 0)
        symbol in forbidden_species && (upper = 0)
        lower > upper && return Dict(
            "status" => "fail",
            "vectors" => Vector{Vector{Int}}(),
            "reason" => "inconsistent_species_bounds",
            "species" => symbol,
            "lower" => lower,
            "upper" => upper,
        )
        push!(lower_bounds, lower)
        push!(upper_bounds, min(upper, degree_vector[idx]))
    end

    sum(lower_bounds) > d && return Dict(
        "status" => "fail",
        "vectors" => Vector{Vector{Int}}(),
        "reason" => "lower_bounds_exceed_total",
        "lower_bound_sum" => sum(lower_bounds),
        "total_base_species" => d,
    )
    sum(upper_bounds) < d && return Dict(
        "status" => "fail",
        "vectors" => Vector{Vector{Int}}(),
        "reason" => "upper_bounds_below_total",
        "upper_bound_sum" => sum(upper_bounds),
        "total_base_species" => d,
    )

    vectors = Vector{Vector{Int}}()
    current = zeros(Int, length(species_symbols))

    function dfs(idx::Int, remaining::Int)
        if idx > length(species_symbols)
            remaining == 0 && push!(vectors, copy(current))
            return
        end

        lower = lower_bounds[idx]
        upper = min(upper_bounds[idx], remaining)
        tail_min = idx < length(species_symbols) ? sum(lower_bounds[(idx + 1):end]) : 0
        tail_max = idx < length(species_symbols) ? sum(upper_bounds[(idx + 1):end]) : 0
        for value in lower:upper
            rem = remaining - value
            rem < tail_min && continue
            rem > tail_max && continue
            current[idx] = value
            dfs(idx + 1, rem)
        end
        current[idx] = 0
    end

    dfs(1, d)
    return Dict(
        "status" => isempty(vectors) ? "fail" : "pass",
        "vectors" => vectors,
        "species_symbols" => species_symbols,
        "degree_vector" => degree_vector,
        "total_base_species" => d,
        "lower_bounds" => lower_bounds,
        "upper_bounds" => upper_bounds,
        "reason" => isempty(vectors) ? "bounding_polytope_empty" : "feasible",
    )
end

function separate_countp(candidate_s::Vector{Int}, canonical_network)
    support_graph = _raw_get(canonical_network, :support_graph, Dict{String, Any}())
    species_symbols = collect(_raw_get(support_graph, :species_symbols, String[]))
    species_supports = Vector{Vector{Int}}(collect(_raw_get(support_graph, :species_supports, Vector{Vector{Int}}())))
    positive = [idx for idx in eachindex(candidate_s) if candidate_s[idx] > 0]

    isempty(positive) && return Dict("status" => "feasible", "candidate" => candidate_s)

    subset_count = 1 << length(positive)
    for mask in 1:(subset_count - 1)
        subset_indices = Int[]
        lhs = 0
        neighbors = Set{Int}()
        for bit in 1:length(positive)
            ((mask >> (bit - 1)) & 1) == 1 || continue
            idx = positive[bit]
            push!(subset_indices, idx)
            lhs += candidate_s[idx]
            union!(neighbors, species_supports[idx])
        end
        rhs = length(neighbors)
        if lhs > rhs
            return Dict(
                "status" => "violated",
                "subset_indices" => subset_indices,
                "subset_species" => [species_symbols[idx] for idx in subset_indices],
                "lhs" => lhs,
                "rhs" => rhs,
            )
        end
    end

    return Dict(
        "status" => "feasible",
        "candidate" => candidate_s,
    )
end

function _support_screen_versions(gamma_q, policies)
    return Dict(
        "profile_version" => String(_raw_get(gamma_q, :profile_version, "unknown")),
        "compiler_version" => String(_raw_get(gamma_q, :compiler_version, DEFAULT_COMPILER_VERSION)),
        "policy_version" => String(_raw_get(policies, :screen_policy_version, DEFAULT_SCREEN_POLICY_VERSION)),
    )
end

function run_bounding_screen(canonical_network, q_s, profile::AtlasSearchProfile)
    feature_result = _feature_constraint_result(canonical_network, q_s)
    if !Bool(_raw_get(feature_result, :pass, false))
        return Dict(
            "status" => "fail",
            "pass" => false,
            "scope" => "support",
            "proof_artifact" => _raw_get(feature_result, :proof_artifact, Dict{String, Any}()),
            "feature_vector" => _raw_get(feature_result, :feature_vector, Float64[]),
        )
    end

    bounding = _enumerate_bounding_count_vectors(canonical_network, q_s)
    if String(_raw_get(bounding, :status, "fail")) != "pass"
        return Dict(
            "status" => "fail",
            "pass" => false,
            "scope" => "support",
            "proof_artifact" => Dict(
                "kind" => "bounding_count_polytope_empty",
                "reason" => _raw_get(bounding, :reason, "unknown"),
                "species_symbols" => collect(_raw_get(bounding, :species_symbols, String[])),
                "degree_vector" => collect(_raw_get(bounding, :degree_vector, Int[])),
                "lower_bounds" => collect(_raw_get(bounding, :lower_bounds, Int[])),
                "upper_bounds" => collect(_raw_get(bounding, :upper_bounds, Int[])),
            ),
            "feature_vector" => _raw_get(feature_result, :feature_vector, Float64[]),
        )
    end

    vectors = collect(_raw_get(bounding, :vectors, Vector{Vector{Int}}()))
    return Dict(
        "status" => "pass",
        "pass" => true,
        "scope" => "support",
        "feasible_witness" => Dict(
            "feature_vector" => _raw_get(feature_result, :feature_vector, Float64[]),
            "count_vector" => isempty(vectors) ? Int[] : first(vectors),
            "species_symbols" => collect(_raw_get(bounding, :species_symbols, String[])),
        ),
        "proof_artifact" => Dict(
            "kind" => "bounding_count_polytope_feasible",
            "candidate_count" => length(vectors),
            "species_symbols" => collect(_raw_get(bounding, :species_symbols, String[])),
            "degree_vector" => collect(_raw_get(bounding, :degree_vector, Int[])),
        ),
        "feature_vector" => _raw_get(feature_result, :feature_vector, Float64[]),
        "candidate_count_vectors" => vectors,
    )
end

function run_exact_support_screen(canonical_network, q_s, graph_class, policy)
    bounding = _enumerate_bounding_count_vectors(canonical_network, q_s)
    vectors = collect(_raw_get(bounding, :vectors, Vector{Vector{Int}}()))

    isempty(vectors) && return Dict(
        "status" => "fail",
        "pass" => false,
        "scope" => "support",
        "proof_artifact" => Dict(
            "kind" => "bounding_count_polytope_empty",
            "reason" => _raw_get(bounding, :reason, "unknown"),
            "graph_class" => String(graph_class),
        ),
        "theorem_scope" => "binding_small_hall_enumeration",
    )

    feasible_vectors = Vector{Vector{Int}}()
    first_violation = nothing
    for vector in vectors
        separation = separate_countp(vector, canonical_network)
        if String(_raw_get(separation, :status, "violated")) == "feasible"
            push!(feasible_vectors, vector)
        elseif first_violation === nothing
            first_violation = separation
        end
    end

    isempty(feasible_vectors) && return Dict(
        "status" => "fail",
        "pass" => false,
        "scope" => "support",
        "proof_artifact" => Dict(
            "kind" => "hall_type_separation",
            "violated_constraint" => _materialize(first_violation),
            "candidate_count" => length(vectors),
            "graph_class" => String(graph_class),
        ),
        "theorem_scope" => "binding_small_hall_enumeration",
    )

    return Dict(
        "status" => "pass",
        "pass" => true,
        "scope" => "support",
        "feasible_vertices" => Any[feasible_vectors[min(idx, length(feasible_vectors))] for idx in 1:min(length(feasible_vectors), 8)],
        "proof_artifact" => Dict(
            "kind" => "hall_type_feasible",
            "count_vector_count" => length(feasible_vectors),
            "graph_class" => String(graph_class),
        ),
        "theorem_scope" => "binding_small_hall_enumeration",
    )
end

function _ensure_inverse_design_fields!(corpus::AbstractDict)
    for key in ("support_screen_cache", "negative_certificate_store", "soft_note_store", "materialization_events", "query_audit_log")
        haskey(corpus, key) || (corpus[key] = Dict{String, Any}[])
    end
    return corpus
end

function _replace_or_push_record!(records::Vector, key_fields::Vector{String}, record::Dict{String, Any})
    for idx in eachindex(records)
        candidate = records[idx]
        all(String(_raw_get(candidate, Symbol(field), "")) == String(_raw_get(record, Symbol(field), "")) for field in key_fields) || continue
        records[idx] = record
        return record
    end
    push!(records, record)
    return record
end

function record_negative(corpus, scope, scope_id, h_Q, hardness, reason, proof_artifact, versions)
    _ensure_inverse_design_fields!(corpus)
    store_key = lowercase(String(hardness)) == "hard" ? "negative_certificate_store" : "soft_note_store"
    record = Dict(
        "recorded_at" => _now_iso_timestamp(),
        "scope" => String(scope),
        "scope_id" => String(scope_id),
        "h_Q" => String(h_Q),
        "hardness" => String(hardness),
        "reason" => String(reason),
        "proof_artifact" => _materialize(proof_artifact),
        "profile_version" => String(_raw_get(versions, :profile_version, "unknown")),
        "compiler_version" => String(_raw_get(versions, :compiler_version, "unknown")),
        "policy_version" => String(_raw_get(versions, :policy_version, "unknown")),
    )
    return _replace_or_push_record!(corpus[store_key], ["scope", "scope_id", "h_Q", "profile_version", "compiler_version", "policy_version"], record)
end

function check_negative(corpus, scope, scope_id, h_Q, versions)
    _ensure_inverse_design_fields!(corpus)
    for record in collect(corpus["negative_certificate_store"])
        String(_raw_get(record, :scope, "")) == String(scope) || continue
        String(_raw_get(record, :scope_id, "")) == String(scope_id) || continue
        String(_raw_get(record, :h_Q, "")) == String(h_Q) || continue
        String(_raw_get(record, :profile_version, "")) == String(_raw_get(versions, :profile_version, "unknown")) || continue
        String(_raw_get(record, :compiler_version, "")) == String(_raw_get(versions, :compiler_version, "unknown")) || continue
        String(_raw_get(record, :policy_version, "")) == String(_raw_get(versions, :policy_version, "unknown")) || continue
        return record
    end
    return nothing
end

function _support_screen_cache_key(support_signature::AbstractString, h_Q::AbstractString, versions)
    return Dict(
        "support_signature" => String(support_signature),
        "h_Q" => String(h_Q),
        "profile_version" => String(_raw_get(versions, :profile_version, "unknown")),
        "compiler_version" => String(_raw_get(versions, :compiler_version, "unknown")),
        "screen_policy" => String(_raw_get(versions, :policy_version, DEFAULT_SCREEN_POLICY_VERSION)),
    )
end

function _check_support_screen_cache(corpus, support_signature::AbstractString, h_Q::AbstractString, versions; stage=nothing)
    _ensure_inverse_design_fields!(corpus)
    for record in collect(corpus["support_screen_cache"])
        String(_raw_get(record, :support_signature, "")) == String(support_signature) || continue
        String(_raw_get(record, :h_Q, "")) == String(h_Q) || continue
        String(_raw_get(record, :profile_version, "")) == String(_raw_get(versions, :profile_version, "unknown")) || continue
        String(_raw_get(record, :compiler_version, "")) == String(_raw_get(versions, :compiler_version, "unknown")) || continue
        String(_raw_get(record, :screen_policy, "")) == String(_raw_get(versions, :policy_version, DEFAULT_SCREEN_POLICY_VERSION)) || continue
        stage === nothing || String(_raw_get(record, :stage, "")) == String(stage) || continue
        return record
    end
    return nothing
end

function _record_support_screen_cache!(corpus, support_signature::AbstractString, h_Q::AbstractString, versions, stage::AbstractString, result)
    _ensure_inverse_design_fields!(corpus)
    record = merge(
        _support_screen_cache_key(support_signature, h_Q, versions),
        Dict(
            "recorded_at" => _now_iso_timestamp(),
            "stage" => String(stage),
            "result" => _materialize(result),
        ),
    )
    return _replace_or_push_record!(corpus["support_screen_cache"], ["support_signature", "h_Q", "profile_version", "compiler_version", "screen_policy", "stage"], record)
end

function _summary_behavior_config(config::AtlasBehaviorConfig)
    return AtlasBehaviorConfig(
        path_scope=config.path_scope,
        min_volume_mean=config.min_volume_mean,
        deduplicate=config.deduplicate,
        keep_singular=config.keep_singular,
        keep_nonasymptotic=config.keep_nonasymptotic,
        compute_volume=false,
        motif_zero_tol=config.motif_zero_tol,
        include_path_records=false,
    )
end

function _resolve_build_candidates_from_spec(spec, profile::AtlasSearchProfile)
    network_specs = Any[]
    enumeration_summary = nothing

    if _raw_haskey(spec, :networks)
        append!(network_specs, collect(_raw_get(spec, :networks, Any[])))
    end
    if _raw_haskey(spec, :enumeration)
        enum_spec = atlas_enumeration_spec_from_raw(_raw_get(spec, :enumeration, nothing))
        enumerated_networks, enumeration_summary = enumerate_network_specs(enum_spec; search_profile=profile)
        append!(network_specs, enumerated_networks)
    end

    return network_specs, enumeration_summary
end

function plan_delta_build(raw_candidates, library, gamma_q, policies; profile::AtlasSearchProfile=atlas_search_profile_binding_small_v0())
    corpus = _ensure_inverse_design_fields!(_materialize(library === nothing ? atlas_library_default() : library))
    versions = _support_screen_versions(gamma_q, policies)
    build_candidates = Any[]
    candidate_traces = Dict{String, Any}[]
    negative_updates = Dict{String, Any}[]
    cache_updates = Dict{String, Any}[]

    for (idx, raw_candidate) in enumerate(raw_candidates)
        label = String(_raw_get(raw_candidate, :label, "candidate_$(idx)"))
        trace = Dict(
            "candidate_label" => label,
            "status" => "pending",
            "stages" => Dict{String, Any}[],
        )

        canonical = canonicalize_network(raw_candidate; profile=profile)
        validation = _raw_get(canonical, :validation, Dict{String, Any}())
        valid = Bool(_raw_get(validation, :valid, false))
        network_id = String(_raw_get(canonical, :network_id, ""))
        support_signature = emit_support_signature(canonical)

        trace["network_id"] = network_id
        trace["support_signature"] = support_signature

        if !valid
            push!(trace["stages"], Dict(
                "stage" => "profile_validation",
                "status" => "excluded",
                "reason" => "excluded_by_search_profile",
                "issues" => collect(_raw_get(validation, :issues, Any[])),
            ))
            trace["status"] = "excluded"
            push!(candidate_traces, trace)
            continue
        end

        support_negative = check_negative(corpus, "support", support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions)
        if support_negative !== nothing
            push!(trace["stages"], Dict(
                "stage" => "negative_certificate",
                "status" => "pruned",
                "reason" => String(_raw_get(support_negative, :reason, "cached_hard_negative")),
                "certificate" => _materialize(support_negative),
            ))
            trace["status"] = "pruned"
            push!(candidate_traces, trace)
            continue
        end

        bounding_result = let
            cached = _check_support_screen_cache(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions; stage="bounding")
            if cached !== nothing
                Dict{String, Any}(_materialize(_raw_get(cached, :result, Dict{String, Any}())))
            else
                result = run_bounding_screen(canonical, _raw_get(gamma_q, :Q_s, Dict{String, Any}()), profile)
                record = _record_support_screen_cache!(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions, "bounding", result)
                push!(cache_updates, record)
                result
            end
        end

        push!(trace["stages"], Dict(
            "stage" => "bounding_screen",
            "status" => Bool(_raw_get(bounding_result, :pass, false)) ? "pass" : "pruned",
            "result" => _materialize(bounding_result),
        ))

        if !Bool(_raw_get(bounding_result, :pass, false))
            cert = record_negative(
                corpus,
                "support",
                support_signature,
                String(_raw_get(gamma_q, :h_Q, "")),
                "hard",
                "bounding_screen_empty",
                _raw_get(bounding_result, :proof_artifact, Dict{String, Any}()),
                versions,
            )
            push!(negative_updates, cert)
            trace["status"] = "pruned"
            push!(candidate_traces, trace)
            continue
        end

        exact_result = let
            cached = _check_support_screen_cache(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions; stage="exact")
            if cached !== nothing
                Dict{String, Any}(_materialize(_raw_get(cached, :result, Dict{String, Any}())))
            else
                result = run_exact_support_screen(canonical, _raw_get(gamma_q, :Q_s, Dict{String, Any}()), String(_raw_get(canonical, :graph_class, profile.name)), policies)
                record = _record_support_screen_cache!(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions, "exact", result)
                push!(cache_updates, record)
                result
            end
        end

        push!(trace["stages"], Dict(
            "stage" => "exact_support_screen",
            "status" => Bool(_raw_get(exact_result, :pass, false)) ? "pass" : "pruned",
            "result" => _materialize(exact_result),
        ))

        if !Bool(_raw_get(exact_result, :pass, false))
            cert = record_negative(
                corpus,
                "support",
                support_signature,
                String(_raw_get(gamma_q, :h_Q, "")),
                "hard",
                "exact_support_screen_empty",
                _raw_get(exact_result, :proof_artifact, Dict{String, Any}()),
                versions,
            )
            push!(negative_updates, cert)
            trace["status"] = "pruned"
            push!(candidate_traces, trace)
            continue
        end

        candidate = Dict{String, Any}(_materialize(raw_candidate))
        candidate["planned_network_id"] = network_id
        candidate["support_signature"] = support_signature
        push!(build_candidates, candidate)
        push!(trace["stages"], Dict(
            "stage" => "summary_delta_build",
            "status" => "queued",
            "reason" => "survived_support_first_pruning",
        ))
        trace["status"] = "queued"
        push!(candidate_traces, trace)
    end

    return Dict(
        "plan_version" => "0.1.0",
        "candidate_count" => length(raw_candidates),
        "build_candidate_count" => length(build_candidates),
        "build_candidates" => build_candidates,
        "candidate_traces" => candidate_traces,
        "support_screen_cache_updates" => cache_updates,
        "negative_certificate_updates" => negative_updates,
    )
end

function _empty_summary_delta(profile::AtlasSearchProfile, config::AtlasBehaviorConfig; plan=nothing, policies=Dict{String, Any}(), enumeration=nothing)
    delta = Dict(
        "atlas_schema_version" => "0.2.0",
        "generated_at" => _now_iso_timestamp(),
        "search_profile" => atlas_search_profile_to_dict(profile),
        "behavior_config" => atlas_behavior_config_to_dict(_summary_behavior_config(config)),
        "input_network_count" => 0,
        "unique_network_count" => 0,
        "successful_network_count" => 0,
        "failed_network_count" => 0,
        "excluded_network_count" => 0,
        "deduplicated_network_count" => 0,
        "pruned_against_library" => false,
        "pruned_against_sqlite" => false,
        "skipped_existing_network_count" => 0,
        "skipped_existing_slice_count" => 0,
        "network_entries" => Dict{String, Any}[],
        "input_graph_slices" => Dict{String, Any}[],
        "behavior_slices" => Dict{String, Any}[],
        "regime_records" => Dict{String, Any}[],
        "transition_records" => Dict{String, Any}[],
        "family_buckets" => Dict{String, Any}[],
        "path_records" => Dict{String, Any}[],
        "duplicate_inputs" => Dict{String, Any}[],
        "support_screen_cache" => plan === nothing ? Dict{String, Any}[] : Dict{String, Any}[Dict{String, Any}(_materialize(item)) for item in collect(_raw_get(plan, :support_screen_cache_updates, Any[]))],
        "negative_certificate_store" => plan === nothing ? Dict{String, Any}[] : Dict{String, Any}[Dict{String, Any}(_materialize(item)) for item in collect(_raw_get(plan, :negative_certificate_updates, Any[]))],
        "soft_note_store" => Dict{String, Any}[],
        "materialization_events" => Dict{String, Any}[],
        "query_audit_log" => Dict{String, Any}[],
        "build_plan" => plan === nothing ? nothing : _materialize(plan),
        "summary_first" => true,
        "materialization_policy_version" => String(_raw_get(policies, :materialization_policy_version, DEFAULT_MATERIALIZATION_POLICY_VERSION)),
        "volume_policy" => String(_raw_get(policies, :volume_policy, "none")),
    )
    enumeration === nothing || (delta["enumeration"] = enumeration)
    return delta
end

function _augment_summary_delta!(delta, profile::AtlasSearchProfile, plan, policies)
    _ensure_inverse_design_fields!(delta)

    network_by_id = Dict{String, Dict{String, Any}}()
    for entry in delta["network_entries"]
        canonical = canonicalize_network(entry; profile=profile)
        entry["support_signature"] = emit_support_signature(canonical)
        entry["support_graph"] = _materialize(_raw_get(canonical, :support_graph, Dict{String, Any}()))
        entry["profile_version"] = profile.name
        entry["canonical_code"] = String(_raw_get(canonical, :canonical_code, _raw_get(entry, :network_id, "")))
        network_by_id[String(_raw_get(entry, :network_id, ""))] = entry
    end

    slice_graph_index = Dict(String(_raw_get(item, :graph_slice_id, "")) => item for item in collect(_raw_get(delta, :input_graph_slices, Any[])))
    paths_by_slice = _atlas_path_records_by_slice(collect(_raw_get(delta, :path_records, Any[])))

    for slice in delta["behavior_slices"]
        slice_id = String(_raw_get(slice, :slice_id, ""))
        mat_state = isempty(get(paths_by_slice, slice_id, Any[])) ? "summary" : "full"
        slice["mat_state"] = mat_state
        slice["profile_version"] = profile.name
        graph_slice = get(slice_graph_index, String(_raw_get(slice, :graph_slice_id, "")), nothing)
        graph_slice === nothing || (slice["path_capacity"] = Int(_raw_get(graph_slice, :path_count, 0)))
    end

    slice_index = _atlas_slice_index(collect(_raw_get(delta, :behavior_slices, Any[])))
    for bucket in delta["family_buckets"]
        slice = get(slice_index, String(_raw_get(bucket, :slice_id, "")), Dict{String, Any}())
        bucket["graph_slice_id"] = _raw_get(slice, :graph_slice_id, nothing)
        bucket["network_id"] = _raw_get(slice, :network_id, nothing)
        bucket["classified_path_count"] = Int(_raw_get(bucket, :path_count, 0))
        bucket["known_robust_lower_bound"] = Int(_raw_get(bucket, :robust_path_count, 0))
        bucket["proxy_state"] = if _raw_get(bucket, :volume_mean, nothing) !== nothing
            "estimated"
        elseif _raw_get(bucket, :proxy_margin_max, nothing) !== nothing || Int(_raw_get(bucket, :proxy_feasible_path_count, 0)) > 0
            "proxy"
        else
            "none"
        end
        bucket["mat_state"] = String(_raw_get(slice, :mat_state, "summary"))
        rep_sig = _raw_get(bucket, :representative_path_signature, nothing)
        bucket["representative_witness_hashes"] = rep_sig === nothing ? String[] : [stable_hash(rep_sig)[1:12]]
    end

    delta["summary_first"] = true
    delta["support_screen_cache"] = plan === nothing ? Dict{String, Any}[] : Dict{String, Any}.(_materialize.(collect(_raw_get(plan, :support_screen_cache_updates, Any[]))))
    delta["negative_certificate_store"] = plan === nothing ? Dict{String, Any}[] : Dict{String, Any}.(_materialize.(collect(_raw_get(plan, :negative_certificate_updates, Any[]))))
    delta["build_plan"] = plan === nothing ? nothing : _materialize(plan)
    delta["compiler_version"] = String(_raw_get(policies, :compiler_version, DEFAULT_COMPILER_VERSION))
    delta["screen_policy_version"] = String(_raw_get(policies, :screen_policy_version, DEFAULT_SCREEN_POLICY_VERSION))
    delta["materialization_policy_version"] = String(_raw_get(policies, :materialization_policy_version, DEFAULT_MATERIALIZATION_POLICY_VERSION))
    delta["volume_policy"] = String(_raw_get(policies, :volume_policy, "none"))
    return delta
end

function build_summary_delta(candidates, profile::AtlasSearchProfile, classifier_cfg::AtlasBehaviorConfig, library; sqlite_path=nothing, skip_existing::Bool=true, plan=nothing, policies=Dict{String, Any}())
    isempty(candidates) && return _empty_summary_delta(profile, classifier_cfg; plan=plan, policies=policies)

    delta = build_behavior_atlas(
        candidates;
        search_profile=profile,
        behavior_config=_summary_behavior_config(classifier_cfg),
        library=library,
        sqlite_path=sqlite_path,
        skip_existing=skip_existing,
    )
    return _augment_summary_delta!(delta, profile, plan, policies)
end

function _merge_auxiliary_store!(target::AbstractVector, incoming)
    for record in incoming
        push!(target, Dict{String, Any}(_materialize(record)))
    end
    return target
end

function merge_atlas_delta(library, delta; source_label=nothing, source_metadata=nothing, allow_duplicate_atlas::Bool=false)
    base_library = library === nothing ? atlas_library_default() : _materialize(library)
    _ensure_inverse_design_fields!(base_library)
    merged = merge_atlas_library(base_library, delta;
        source_label=source_label,
        source_metadata=source_metadata,
        allow_duplicate_atlas=allow_duplicate_atlas,
    )
    _ensure_inverse_design_fields!(merged)
    _merge_auxiliary_store!(merged["support_screen_cache"], collect(_raw_get(delta, :support_screen_cache, Any[])))
    _merge_auxiliary_store!(merged["negative_certificate_store"], collect(_raw_get(delta, :negative_certificate_store, Any[])))
    _merge_auxiliary_store!(merged["soft_note_store"], collect(_raw_get(delta, :soft_note_store, Any[])))
    _merge_auxiliary_store!(merged["materialization_events"], collect(_raw_get(delta, :materialization_events, Any[])))
    _merge_auxiliary_store!(merged["query_audit_log"], collect(_raw_get(delta, :query_audit_log, Any[])))
    return merged
end

function _bucket_known_robust_count(bucket)
    return Int(_raw_get(bucket, :known_robust_lower_bound, _raw_get(bucket, :robust_path_count, 0)))
end

function _bucket_robust_exact(bucket)
    return String(_raw_get(bucket, :mat_state, "summary")) == "full" &&
           String(_raw_get(bucket, :proxy_state, "none")) == "estimated"
end

function _dedup_bucket_records(records)
    seen = Set{String}()
    out = Any[]
    for record in records
        bucket_id = String(_raw_get(record, :bucket_id, ""))
        bucket_id in seen && continue
        push!(seen, bucket_id)
        push!(out, record)
    end
    return out
end

function _matching_family_buckets_v2(buckets, family_kind::String, labels::Vector{String}, match_mode::Symbol, query::AtlasQuerySpec)
    family_buckets = [bucket for bucket in buckets if String(_raw_get(bucket, :family_kind, "")) == family_kind]
    threshold = max(query.min_robust_path_count, query.require_robust ? 1 : 0)

    if isempty(labels)
        if threshold == 0
            return family_buckets, true, false
        end
        qualifying = [bucket for bucket in family_buckets if _bucket_known_robust_count(bucket) >= threshold]
        unresolved = any(!_bucket_robust_exact(bucket) for bucket in family_buckets)
        return !isempty(qualifying) ? qualifying : family_buckets, !isempty(qualifying) || unresolved, unresolved
    end

    matched = [bucket for bucket in family_buckets if _bucket_label_matches(bucket, labels)]
    matched_labels = Set(String(_raw_get(bucket, :family_label, "")) for bucket in matched)

    label_ok = if match_mode == :all
        all(label -> label in matched_labels, labels)
    elseif match_mode == :any
        !isempty(matched)
    else
        error("Unsupported family match mode: $(match_mode)")
    end
    label_ok || return Any[], false, false

    if threshold == 0
        return matched, true, false
    end

    qualifying = [bucket for bucket in matched if _bucket_known_robust_count(bucket) >= threshold]
    unresolved = any(!_bucket_robust_exact(bucket) for bucket in matched)

    if match_mode == :all
        ok = true
        selected = Any[]
        for label in labels
            label_group = [bucket for bucket in matched if String(_raw_get(bucket, :family_label, "")) == label]
            if any(_bucket_known_robust_count(bucket) >= threshold for bucket in label_group)
                append!(selected, [bucket for bucket in label_group if _bucket_known_robust_count(bucket) >= threshold])
            elseif any(!_bucket_robust_exact(bucket) for bucket in label_group)
                unresolved = true
                append!(selected, label_group)
            else
                ok = false
            end
        end
        return _dedup_bucket_records(selected), ok || unresolved, unresolved
    end

    return !isempty(qualifying) ? qualifying : matched, !isempty(qualifying) || unresolved, unresolved
end

function _matching_automaton_witness_paths(paths, gamma_q, query::AtlasQuerySpec)
    filtered = [path for path in paths if _path_meets_witness_constraints(path, query)]
    sequences = collect(_raw_get(_raw_get(gamma_q, :A_Q, Dict{String, Any}()), :sequences, Any[]))

    if isempty(sequences)
        return _query_requires_witness(query) ? filtered : Any[], !_query_requires_witness(query) || !isempty(filtered)
    end

    matched = Any[]
    for sequence in sequences
        predicates = Vector{Dict{String, Any}}(collect(_raw_get(sequence, :predicates, Any[])))
        seq_matches = [path for path in filtered if _path_sequence_matches(path, predicates)]
        isempty(seq_matches) && return Any[], false
        push!(matched, _best_witness_path(seq_matches))
    end
    return matched, true
end

function _append_unique_path_records!(corpus, records)
    existing_ids = Set(String(_raw_get(record, :path_record_id, "")) for record in collect(_raw_get(corpus, :path_records, Any[])))
    added = Dict{String, Any}[]
    for record in records
        rec = Dict{String, Any}(_materialize(record))
        path_id = String(_raw_get(rec, :path_record_id, ""))
        path_id in existing_ids && continue
        push!(existing_ids, path_id)
        push!(corpus["path_records"], rec)
        push!(added, rec)
    end
    return added
end

function _bucket_paths(paths, bucket)
    kind = String(_raw_get(bucket, :family_kind, ""))
    family_idx = _raw_get(bucket, :family_idx, nothing)
    if kind == "exact"
        return [path for path in paths if _raw_get(path, :exact_family_idx, nothing) == family_idx]
    end
    return [path for path in paths if _raw_get(path, :motif_family_idx, nothing) == family_idx]
end

function _update_bucket_materialization!(bucket, bucket_paths, persisted_paths; fully_materialized::Bool, volume_policy::AbstractString)
    current_known = _bucket_known_robust_count(bucket)
    current_mat_state = String(_raw_get(bucket, :mat_state, "summary"))
    new_known = max(current_known, count(path -> Bool(_raw_get(path, :robust, false)), persisted_paths))
    bucket["known_robust_lower_bound"] = new_known
    bucket["robust_path_count"] = fully_materialized ? count(path -> Bool(_raw_get(path, :robust, false)), bucket_paths) : new_known
    bucket["mat_state"] = fully_materialized ? "full" : (current_mat_state == "full" ? "full" : "partial")
    current_proxy_state = String(_raw_get(bucket, :proxy_state, "none"))
    proxy_feasible_path_count = 0
    proxy_margins = Float64[]
    for path in persisted_paths
        proxy = _raw_get(path, :volume_proxy, nothing)
        proxy === nothing && continue
        Bool(_raw_get(proxy, :feasible, false)) && (proxy_feasible_path_count += 1)
        margin = _raw_get(proxy, :interior_margin_estimate, nothing)
        margin === nothing || push!(proxy_margins, Float64(margin))
    end
    bucket["proxy_feasible_path_count"] = max(Int(_raw_get(bucket, :proxy_feasible_path_count, 0)), proxy_feasible_path_count)
    if !isempty(proxy_margins)
        prev_margin = _raw_get(bucket, :proxy_margin_max, nothing)
        bucket["proxy_margin_max"] = prev_margin === nothing ? maximum(proxy_margins) : max(Float64(prev_margin), maximum(proxy_margins))
    end
    bucket["proxy_state"] = if volume_policy == "estimated"
        "estimated"
    elseif volume_policy == "proxy" && (proxy_feasible_path_count > 0 || !isempty(proxy_margins))
        "proxy"
    else
        current_proxy_state
    end
    if fully_materialized
        volume_means = Float64[]
        for path in bucket_paths
            vol = _path_volume_mean(path)
            vol === nothing || push!(volume_means, vol)
        end
        bucket["volume_mean"] = isempty(volume_means) ? nothing : sum(volume_means) / length(volume_means)
    end
    return bucket
end

function _refresh_slice_materialization_state!(corpus, slice_id::AbstractString)
    buckets = [bucket for bucket in collect(_raw_get(corpus, :family_buckets, Any[])) if String(_raw_get(bucket, :slice_id, "")) == String(slice_id)]
    slice_index = _atlas_slice_index(collect(_raw_get(corpus, :behavior_slices, Any[])))
    slice = get(slice_index, String(slice_id), nothing)
    slice === nothing && return nothing
    if isempty(buckets)
        slice["mat_state"] = "summary"
    elseif all(String(_raw_get(bucket, :mat_state, "summary")) == "full" for bucket in buckets)
        slice["mat_state"] = "full"
    elseif any(String(_raw_get(bucket, :mat_state, "summary")) != "summary" for bucket in buckets)
        slice["mat_state"] = "partial"
    else
        slice["mat_state"] = "summary"
    end
    return slice
end

function _polyhedron_proxy_summary(poly_dict)
    haskey(poly_dict, "dimension") || return Dict(
        "feasible" => false,
        "reason" => "polyhedron_unavailable",
    )

    summary = Dict{String, Any}(
        "feasible" => true,
        "dimension" => Int(_raw_get(poly_dict, :dimension, -1)),
        "n_constraints" => Int(_raw_get(poly_dict, :n_constraints, 0)),
        "n_vertices" => Int(_raw_get(poly_dict, :n_vertices, 0)),
    )

    seeds = _polyhedron_seed_candidates(poly_dict)
    margins = Float64[]
    for seed in seeds
        margin = _polyhedron_slack_margin(poly_dict, Float64.(seed["point"]))
        margin === nothing || push!(margins, Float64(margin))
    end
    summary["interior_margin_estimate"] = isempty(margins) ? nothing : maximum(margins)
    return summary
end

function _attach_proxy_metrics!(paths, model, input_symbol::AbstractString)
    for path in paths
        path_idx = Int(_raw_get(path, :path_idx, 0))
        path_idx > 0 || continue
        try
            poly_dict = _candidate_polyhedron(model, String(input_symbol), path_idx)
            path["volume_proxy"] = _polyhedron_proxy_summary(poly_dict)
        catch err
            path["volume_proxy"] = Dict(
                "feasible" => false,
                "reason" => "proxy_construction_failed",
                "error" => sprint(showerror, err),
            )
        end
    end
    return paths
end

function materialize_witnesses(corpus, bucket_id::AbstractString, gamma_q, budget, reason; policies=Dict{String, Any}())
    _ensure_inverse_design_fields!(corpus)
    query = atlas_query_spec_from_raw(_raw_get(gamma_q, :query, Dict{String, Any}()))
    bucket_index = Dict(String(_raw_get(bucket, :bucket_id, "")) => bucket for bucket in collect(_raw_get(corpus, :family_buckets, Any[])))
    bucket = get(bucket_index, String(bucket_id), nothing)
    bucket === nothing && return Dict(
        "bucket_id" => String(bucket_id),
        "status" => "missing_bucket",
        "accepted_paths" => Dict{String, Any}[],
        "materialized_paths" => Dict{String, Any}[],
        "exhaustive" => false,
    )

    slice_id = String(_raw_get(bucket, :slice_id, ""))
    slice_index = _atlas_slice_index(collect(_raw_get(corpus, :behavior_slices, Any[])))
    slice = get(slice_index, slice_id, nothing)
    slice === nothing && return Dict(
        "bucket_id" => String(bucket_id),
        "status" => "missing_slice",
        "accepted_paths" => Dict{String, Any}[],
        "materialized_paths" => Dict{String, Any}[],
        "exhaustive" => false,
    )

    existing_paths_by_slice = _atlas_path_records_by_slice(collect(_raw_get(corpus, :path_records, Any[])))
    existing_bucket_paths = _bucket_paths(get(existing_paths_by_slice, slice_id, Any[]), bucket)
    if String(_raw_get(bucket, :mat_state, "summary")) == "full" && !isempty(existing_bucket_paths)
        accepted_existing, ok_existing = _matching_automaton_witness_paths(existing_bucket_paths, gamma_q, query)
        return Dict(
            "bucket_id" => String(bucket_id),
            "slice_id" => slice_id,
            "status" => ok_existing && !isempty(accepted_existing) ? "accept" : "exact_reject",
            "accepted_paths" => Any[Dict{String, Any}(_materialize(path)) for path in accepted_existing],
            "materialized_paths" => Any[Dict{String, Any}(_materialize(path)) for path in existing_bucket_paths],
            "exhaustive" => true,
        )
    end

    network_by_id = _atlas_network_index(collect(_raw_get(corpus, :network_entries, Any[])))
    network_entry = get(network_by_id, String(_raw_get(slice, :network_id, "")), nothing)
    network_entry === nothing && return Dict(
        "bucket_id" => String(bucket_id),
        "slice_id" => slice_id,
        "status" => "missing_network",
        "accepted_paths" => Dict{String, Any}[],
        "materialized_paths" => Dict{String, Any}[],
        "exhaustive" => false,
    )

    rules = String.(_raw_get(network_entry, :raw_rules, String[]))
    input_symbol = Symbol(String(_raw_get(slice, :input_symbol, "")))
    output_symbol = Symbol(String(_raw_get(slice, :output_symbol, "")))
    classifier_config = atlas_behavior_config_from_raw(_raw_get(slice, :classifier_config, Dict{String, Any}()))
    volume_policy = String(_raw_get(policies, :volume_policy, query.require_witness_robust || query.min_witness_volume_mean !== nothing ? "estimated" : "none"))
    material_path_scope = query.require_witness_robust || query.min_witness_volume_mean !== nothing ? :robust : :feasible

    material_config = AtlasBehaviorConfig(
        path_scope=material_path_scope,
        min_volume_mean=classifier_config.min_volume_mean,
        deduplicate=classifier_config.deduplicate,
        keep_singular=classifier_config.keep_singular,
        keep_nonasymptotic=classifier_config.keep_nonasymptotic,
        compute_volume=volume_policy == "estimated",
        motif_zero_tol=classifier_config.motif_zero_tol,
        include_path_records=true,
    )

    model, _, _, _ = build_model(rules, ones(Float64, length(rules)))
    siso = SISOPaths(model, input_symbol)
    result = get_behavior_families(
        siso;
        observe_x=output_symbol,
        path_scope=material_config.path_scope,
        min_volume_mean=material_config.min_volume_mean,
        deduplicate=material_config.deduplicate,
        keep_singular=material_config.keep_singular,
        keep_nonasymptotic=material_config.keep_nonasymptotic,
        motif_zero_tol=material_config.motif_zero_tol,
        compute_volume=material_config.compute_volume,
    )

    slice_graph_payload = _build_slice_regime_transition_records(
        model,
        siso,
        String(_raw_get(slice, :network_id, "")),
        slice_id,
        String(_raw_get(slice, :graph_slice_id, "")),
        String(input_symbol),
        String(output_symbol),
    )

    temp_paths = Dict{String, Any}[]
    _build_path_records!(
        temp_paths,
        result,
        slice_id,
        String(_raw_get(slice, :graph_slice_id, "")),
        material_config,
        slice_graph_payload["regime_by_vertex"],
        slice_graph_payload["transition_by_edge"],
    )
    volume_policy == "proxy" && _attach_proxy_metrics!(temp_paths, model, String(input_symbol))

    bucket_paths = _bucket_paths(temp_paths, bucket)
    accepted_paths, accepted = _matching_automaton_witness_paths(bucket_paths, gamma_q, query)
    path_budget = max(Int(budget), 1)

    persist_paths = if !isempty(accepted_paths) && length(bucket_paths) > path_budget
        Any[accepted_paths[idx] for idx in 1:min(path_budget, length(accepted_paths))]
    else
        bucket_paths
    end
    fully_materialized = isempty(bucket_paths) || isempty(accepted_paths) || length(bucket_paths) <= path_budget || lowercase(String(reason)) == "refinement_seed"
    fully_materialized && (persist_paths = bucket_paths)

    added_paths = _append_unique_path_records!(corpus, persist_paths)
    _update_bucket_materialization!(bucket, bucket_paths, persist_paths; fully_materialized=fully_materialized, volume_policy=volume_policy)
    _refresh_slice_materialization_state!(corpus, slice_id)

    event = Dict(
        "materialized_at" => _now_iso_timestamp(),
        "bucket_id" => String(bucket_id),
        "slice_id" => slice_id,
        "reason" => String(reason),
        "budget" => path_budget,
        "materialized_path_count" => length(added_paths),
        "accepted_path_count" => length(accepted_paths),
        "mat_state" => String(_raw_get(bucket, :mat_state, "summary")),
        "volume_policy" => volume_policy,
    )
    push!(corpus["materialization_events"], event)

    return Dict(
        "bucket_id" => String(bucket_id),
        "slice_id" => slice_id,
        "status" => accepted && !isempty(accepted_paths) ? "accept" : (fully_materialized ? "exact_reject" : "partial"),
        "accepted_paths" => Any[Dict{String, Any}(_materialize(path)) for path in accepted_paths],
        "materialized_paths" => Any[Dict{String, Any}(_materialize(path)) for path in persist_paths],
        "exhaustive" => fully_materialized,
        "event" => event,
    )
end

function _result_robustness_lower_bound(buckets)
    score = 0.0
    for bucket in buckets
        score += Float64(_bucket_known_robust_count(bucket))
        volume_mean = _raw_get(bucket, :volume_mean, nothing)
        volume_mean === nothing || (score += Float64(volume_mean))
    end
    return score
end

function _candidate_trace_record(slice, network_entry, trace_status::AbstractString, trace_stages)
    return Dict(
        "slice_id" => String(_raw_get(slice, :slice_id, "")),
        "network_id" => String(_raw_get(network_entry, :network_id, "")),
        "input_symbol" => String(_raw_get(slice, :input_symbol, "")),
        "output_symbol" => String(_raw_get(slice, :output_symbol, "")),
        "status" => String(trace_status),
        "stages" => trace_stages,
    )
end

function _effective_volume_policy(requested_policy, query::AtlasQuerySpec, refinement::InverseRefinementSpec)
    exact_volume_required = query.require_witness_robust || query.min_witness_volume_mean !== nothing
    default_policy = exact_volume_required ? "estimated" : (refinement.enabled ? "proxy" : "none")
    requested = requested_policy === nothing ? default_policy : lowercase(String(requested_policy))
    requested in ("none", "proxy", "estimated") || throw(ArgumentError("unsupported volume policy: `$requested`"))
    if exact_volume_required && requested != "estimated"
        return Dict(
            "requested" => requested,
            "effective" => "estimated",
            "coercion_reason" => "exact_volume_semantics_require_estimated_policy",
        )
    end
    return Dict(
        "requested" => requested_policy === nothing ? "auto" : requested,
        "effective" => requested,
        "coercion_reason" => nothing,
    )
end

function _query_policies_from_spec(spec, refinement::InverseRefinementSpec, query::AtlasQuerySpec)
    inverse_raw = _raw_haskey(spec, :inverse_design) ? _raw_get(spec, :inverse_design, nothing) : spec
    screen_policy = _raw_haskey(spec, :screen_policy) ? _raw_get(spec, :screen_policy, nothing) : _raw_get(inverse_raw, :screen_policy, nothing)
    materialization_policy = _raw_haskey(spec, :materialization_policy) ? _raw_get(spec, :materialization_policy, nothing) : _raw_get(inverse_raw, :materialization_policy, nothing)
    volume_policy = _raw_haskey(spec, :volume_policy) ? _raw_get(spec, :volume_policy, nothing) : _raw_get(inverse_raw, :volume_policy, nothing)

    screen_name = screen_policy === nothing ? "support_first" : String(_raw_get(screen_policy, :name, "support_first"))
    screen_version = screen_policy === nothing ? DEFAULT_SCREEN_POLICY_VERSION : String(_raw_get(screen_policy, :version, DEFAULT_SCREEN_POLICY_VERSION))
    materialization_name = materialization_policy === nothing ? "lazy_witness" : String(_raw_get(materialization_policy, :name, "lazy_witness"))
    materialization_version = materialization_policy === nothing ? DEFAULT_MATERIALIZATION_POLICY_VERSION : String(_raw_get(materialization_policy, :version, DEFAULT_MATERIALIZATION_POLICY_VERSION))
    default_budget = materialization_policy === nothing ? 1 : Int(_raw_get(materialization_policy, :default_budget, 1))
    refinement_budget = materialization_policy === nothing ? 2 : Int(_raw_get(materialization_policy, :refinement_budget, 2))
    volume_policy_info = _effective_volume_policy(volume_policy, query, refinement)

    return Dict(
        "pipeline_version" => INVERSE_DESIGN_PIPELINE_VERSION,
        "compiler_version" => String(_raw_get(spec, :compiler_version, DEFAULT_COMPILER_VERSION)),
        "screen_policy_version" => screen_version,
        "screen_policy" => Dict(
            "name" => screen_name,
            "version" => screen_version,
            "cheap_mode" => "bounding_count_polytope_with_feature_guard",
            "exact_mode" => "binding_small_hall_separation",
        ),
        "materialization_policy_version" => materialization_version,
        "materialization_policy" => Dict(
            "name" => materialization_name,
            "version" => materialization_version,
            "default_budget" => default_budget,
            "refinement_budget" => refinement_budget,
            "automaton_mode" => "predicate_sequence_scan",
        ),
        "volume_policy_version" => DEFAULT_VOLUME_POLICY_VERSION,
        "volume_policy_requested" => String(_raw_get(volume_policy_info, :requested, "auto")),
        "volume_policy" => String(_raw_get(volume_policy_info, :effective, "none")),
        "volume_policy_coercion_reason" => _raw_get(volume_policy_info, :coercion_reason, nothing),
        "refinement_policy_version" => DEFAULT_REFINEMENT_POLICY_VERSION,
        "refinement_policy" => Dict(
            "name" => "polytope_guided",
            "version" => DEFAULT_REFINEMENT_POLICY_VERSION,
            "top_k" => refinement.top_k,
            "trials" => refinement.trials,
            "fallback" => "random_background_scan",
        ),
    )
end

function retrieve_candidates(gamma_q, library, profile::AtlasSearchProfile; policies=Dict{String, Any}())
    corpus = _refresh_atlas_library!(_ensure_inverse_design_fields!(_materialize(library)))
    query = atlas_query_spec_from_raw(_raw_get(gamma_q, :query, Dict{String, Any}()))
    versions = _support_screen_versions(gamma_q, policies)

    network_entries = collect(_raw_get(corpus, :network_entries, Any[]))
    behavior_slices = collect(_raw_get(corpus, :behavior_slices, Any[]))
    regime_records = collect(_raw_get(corpus, :regime_records, Any[]))
    transition_records = collect(_raw_get(corpus, :transition_records, Any[]))
    family_buckets = collect(_raw_get(corpus, :family_buckets, Any[]))
    slice_count_maps = _atlas_slice_count_maps(regime_records, transition_records, family_buckets, collect(_raw_get(corpus, :path_records, Any[])))

    network_by_id = _atlas_network_index(network_entries)
    buckets_by_slice = _atlas_family_buckets_by_slice(family_buckets)
    regimes_by_slice = _atlas_regime_records_by_slice(regime_records)
    transitions_by_slice = _atlas_transition_records_by_slice(transition_records)

    results = Dict{String, Any}[]
    candidate_traces = Dict{String, Any}[]
    new_negative_certs = Dict{String, Any}[]

    for slice in behavior_slices
        slice_id = String(_raw_get(slice, :slice_id, ""))
        _atlas_slice_is_complete(slice, _atlas_slice_counts(slice_id, slice_count_maps)) || continue
        stages = Dict{String, Any}[]
        _passes_io_constraints(slice, query) || continue

        network_id = String(_raw_get(slice, :network_id, ""))
        network_entry = get(network_by_id, network_id, nothing)
        network_entry === nothing && continue
        String(_raw_get(network_entry, :analysis_status, "failed")) == "ok" || continue

        push!(stages, Dict("stage" => "io_prefilter", "status" => "pass"))
        if !_passes_network_constraints(network_entry, query)
            push!(stages, Dict("stage" => "network_constraints", "status" => "pruned", "reason" => "structural_constraints"))
            push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
            continue
        end
        push!(stages, Dict("stage" => "network_constraints", "status" => "pass"))

        slice_id = String(_raw_get(slice, :slice_id, ""))
        buckets = get(buckets_by_slice, slice_id, Any[])
        matched_motif_buckets, motif_ok, motif_unresolved = _matching_family_buckets_v2(buckets, "motif", query.motif_labels, query.motif_match_mode, query)
        matched_exact_buckets, exact_ok, exact_unresolved = _matching_family_buckets_v2(buckets, "exact", query.exact_labels, query.exact_match_mode, query)
        if !(motif_ok && exact_ok)
            push!(stages, Dict("stage" => "family_prefilter", "status" => "pruned", "reason" => "family_filters"))
            push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
            continue
        end
        push!(stages, Dict(
            "stage" => "family_prefilter",
            "status" => (motif_unresolved || exact_unresolved) ? "pass_unresolved" : "pass",
            "motif_bucket_count" => length(matched_motif_buckets),
            "exact_bucket_count" => length(matched_exact_buckets),
        ))

        matched_regime_records, regime_ok = _matching_graph_records(
            get(regimes_by_slice, slice_id, Any[]),
            query.required_regimes,
            query.forbidden_regimes,
            _regime_record_matches_predicate,
        )
        matched_transition_records, transition_ok = _matching_graph_records(
            get(transitions_by_slice, slice_id, Any[]),
            query.required_transitions,
            query.forbidden_transitions,
            _transition_record_matches_predicate,
        )
        if !(regime_ok && transition_ok)
            push!(stages, Dict("stage" => "graph_prefilter", "status" => "pruned", "reason" => "graph_predicates"))
            push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
            continue
        end
        push!(stages, Dict("stage" => "graph_prefilter", "status" => "pass"))

        support_signature = haskey(network_entry, "support_signature") ? String(network_entry["support_signature"]) :
                            emit_support_signature(canonicalize_network(network_entry; profile=profile))
        support_negative = check_negative(corpus, "support", support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions)
        if support_negative !== nothing
            push!(stages, Dict("stage" => "support_negative", "status" => "pruned", "certificate" => _materialize(support_negative)))
            push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
            continue
        end

        exact_cached = _check_support_screen_cache(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions; stage="exact")
        if exact_cached !== nothing
            exact_result = Dict{String, Any}(_materialize(_raw_get(exact_cached, :result, Dict{String, Any}())))
            push!(stages, Dict(
                "stage" => "exact_support_screen",
                "status" => Bool(_raw_get(exact_result, :pass, false)) ? "pass_cached" : "pruned_cached",
                "result" => _materialize(exact_result),
            ))
            if !Bool(_raw_get(exact_result, :pass, false))
                cert = record_negative(
                    corpus,
                    "support",
                    support_signature,
                    String(_raw_get(gamma_q, :h_Q, "")),
                    "hard",
                    "exact_support_screen_empty",
                    _raw_get(exact_result, :proof_artifact, Dict{String, Any}()),
                    versions,
                )
                push!(new_negative_certs, cert)
                push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
                continue
            end
        else
            canonical = canonicalize_network(network_entry; profile=profile)
            bounding_result = let
                cached = _check_support_screen_cache(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions; stage="bounding")
                if cached !== nothing
                    Dict{String, Any}(_materialize(_raw_get(cached, :result, Dict{String, Any}())))
                else
                    result = run_bounding_screen(canonical, _raw_get(gamma_q, :Q_s, Dict{String, Any}()), profile)
                    _record_support_screen_cache!(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions, "bounding", result)
                    result
                end
            end
            push!(stages, Dict(
                "stage" => "bounding_screen",
                "status" => Bool(_raw_get(bounding_result, :pass, false)) ? "pass" : "pruned",
                "result" => _materialize(bounding_result),
            ))
            if !Bool(_raw_get(bounding_result, :pass, false))
                cert = record_negative(
                    corpus,
                    "support",
                    support_signature,
                    String(_raw_get(gamma_q, :h_Q, "")),
                    "hard",
                    "bounding_screen_empty",
                    _raw_get(bounding_result, :proof_artifact, Dict{String, Any}()),
                    versions,
                )
                push!(new_negative_certs, cert)
                push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
                continue
            end

            exact_result = run_exact_support_screen(canonical, _raw_get(gamma_q, :Q_s, Dict{String, Any}()), String(_raw_get(canonical, :graph_class, profile.name)), policies)
            _record_support_screen_cache!(corpus, support_signature, String(_raw_get(gamma_q, :h_Q, "")), versions, "exact", exact_result)
            push!(stages, Dict(
                "stage" => "exact_support_screen",
                "status" => Bool(_raw_get(exact_result, :pass, false)) ? "pass" : "pruned",
                "result" => _materialize(exact_result),
            ))
            if !Bool(_raw_get(exact_result, :pass, false))
                cert = record_negative(
                    corpus,
                    "support",
                    support_signature,
                    String(_raw_get(gamma_q, :h_Q, "")),
                    "hard",
                    "exact_support_screen_empty",
                    _raw_get(exact_result, :proof_artifact, Dict{String, Any}()),
                    versions,
                )
                push!(new_negative_certs, cert)
                push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
                continue
            end
        end

        slice_negative = check_negative(corpus, "slice", slice_id, String(_raw_get(gamma_q, :h_Q, "")), versions)
        if slice_negative !== nothing
            push!(stages, Dict("stage" => "slice_negative", "status" => "pruned", "certificate" => _materialize(slice_negative)))
            push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
            continue
        end

        paths_by_slice = _atlas_path_records_by_slice(collect(_raw_get(corpus, :path_records, Any[])))
        matched_witness_paths, witness_ok = _matching_automaton_witness_paths(get(paths_by_slice, slice_id, Any[]), gamma_q, query)
        needs_materialization = _query_requires_witness(query) || motif_unresolved || exact_unresolved || (!witness_ok && _query_requires_witness(query))

        materialization_records = Dict{String, Any}[]
        if needs_materialization
            relevant_bucket_ids = _sorted_unique_strings(vcat(
                [String(_raw_get(bucket, :bucket_id, "")) for bucket in matched_motif_buckets],
                [String(_raw_get(bucket, :bucket_id, "")) for bucket in matched_exact_buckets],
                isempty(matched_motif_buckets) && isempty(matched_exact_buckets) ? [String(_raw_get(bucket, :bucket_id, "")) for bucket in buckets] : String[],
            ))
            relevant_bucket_ids = isempty(relevant_bucket_ids) ? [String(_raw_get(bucket, :bucket_id, "")) for bucket in buckets] : relevant_bucket_ids

            any_accept = false
            all_exhaustive = true
            for bucket_id in relevant_bucket_ids
                mat = materialize_witnesses(
                    corpus,
                    bucket_id,
                    gamma_q,
                    Int(_raw_get(_raw_get(policies, :materialization_policy, Dict{String, Any}()), :default_budget, 1)),
                    "query_resolution";
                    policies=policies,
                )
                push!(materialization_records, mat)
                all_exhaustive &= Bool(_raw_get(mat, :exhaustive, false))
                !isempty(collect(_raw_get(mat, :accepted_paths, Any[]))) && (any_accept = true)
                if any_accept && _query_requires_witness(query)
                    break
                end
            end

            paths_by_slice = _atlas_path_records_by_slice(collect(_raw_get(corpus, :path_records, Any[])))
            matched_witness_paths, witness_ok = _matching_automaton_witness_paths(get(paths_by_slice, slice_id, Any[]), gamma_q, query)
            push!(stages, Dict(
                "stage" => "lazy_materialization",
                "status" => witness_ok ? "pass" : (all_exhaustive ? "exact_reject" : "partial"),
                "materialized_bucket_count" => length(materialization_records),
            ))

            if !witness_ok && _query_requires_witness(query) && all_exhaustive
                cert = record_negative(
                    corpus,
                    "slice",
                    slice_id,
                    String(_raw_get(gamma_q, :h_Q, "")),
                    "hard",
                    "exhaustive_lazy_witness_reject",
                    Dict(
                        "bucket_ids" => relevant_bucket_ids,
                        "materialization_records" => _materialize(materialization_records),
                    ),
                    versions,
                )
                push!(new_negative_certs, cert)
                push!(candidate_traces, _candidate_trace_record(slice, network_entry, "pruned", stages))
                continue
            elseif !witness_ok && _query_requires_witness(query)
                soft_note = record_negative(
                    corpus,
                    "slice",
                    slice_id,
                    String(_raw_get(gamma_q, :h_Q, "")),
                    "soft",
                    "lazy_materialization_budget_exhausted",
                    Dict("bucket_ids" => relevant_bucket_ids),
                    versions,
                )
                push!(candidate_traces, _candidate_trace_record(slice, network_entry, "soft_fail", vcat(stages, [Dict("stage" => "soft_note", "status" => "recorded", "note" => soft_note)])))
                continue
            end
        else
            push!(stages, Dict("stage" => "lazy_materialization", "status" => "not_needed"))
        end

        matched_bucket_union = _dedup_bucket_records(vcat(matched_motif_buckets, matched_exact_buckets))
        robustness_score = _result_robustness_lower_bound(matched_bucket_union)
        ranking_key = _slice_ranking_key(network_entry, robustness_score, query)
        best_witness_path = isempty(matched_witness_paths) ? nothing : _best_witness_path(matched_witness_paths)

        push!(results, Dict(
            "slice_id" => slice_id,
            "network_id" => network_id,
            "source_label" => String(_raw_get(network_entry, :source_label, "")),
            "source_kind" => String(_raw_get(network_entry, :source_kind, "")),
            "input_symbol" => String(_raw_get(slice, :input_symbol, "")),
            "output_symbol" => String(_raw_get(slice, :output_symbol, "")),
            "base_species_count" => _raw_get(network_entry, :base_species_count, nothing),
            "reaction_count" => _raw_get(network_entry, :reaction_count, nothing),
            "max_support" => _raw_get(network_entry, :max_support, nothing),
            "support_mass" => _raw_get(network_entry, :support_mass, nothing),
            "support_signature" => support_signature,
            "raw_rules" => collect(_raw_get(network_entry, :raw_rules, Any[])),
            "motif_union" => collect(_raw_get(slice, :motif_union, Any[])),
            "exact_union" => collect(_raw_get(slice, :exact_union, Any[])),
            "matched_motif_buckets" => matched_motif_buckets,
            "matched_exact_buckets" => matched_exact_buckets,
            "matched_regime_records" => matched_regime_records,
            "matched_transition_records" => matched_transition_records,
            "matched_witness_paths" => matched_witness_paths,
            "best_witness_path" => best_witness_path,
            "matched_bucket_count" => length(matched_motif_buckets) + length(matched_exact_buckets),
            "matched_robust_path_count" => sum(_bucket_known_robust_count(bucket) for bucket in matched_bucket_union),
            "matched_regime_count" => length(matched_regime_records),
            "matched_transition_count" => length(matched_transition_records),
            "witness_path_count" => length(matched_witness_paths),
            "robustness_score" => robustness_score,
            "robustness_lower_bound" => robustness_score,
            "ranking_key" => collect(ranking_key),
            "pruning_trace" => stages,
            "materialization_records" => materialization_records,
        ))
        push!(candidate_traces, _candidate_trace_record(slice, network_entry, "accepted", stages))
    end

    sort!(results; by=result -> Tuple(Float64.(result["ranking_key"])))
    output_unit = query.collapse_by_network ? "network" : "slice"
    output_results = query.collapse_by_network ? _collapse_results_by_network(results, query) : results
    query.pareto_only && (output_results = _pareto_filter_results(output_results))

    if query.collapse_by_network
        sort!(output_results; by=result -> Tuple(Float64.(result["best_ranking_key"])))
    else
        sort!(output_results; by=result -> Tuple(Float64.(result["ranking_key"])))
    end

    if query.limit > 0 && length(output_results) > query.limit
        output_results = output_results[1:query.limit]
    end

    for (rank, result) in enumerate(output_results)
        result["rank"] = rank
        result["pareto_signature"] = collect(_pareto_signature(result))
    end

    audit_record = Dict(
        "queried_at" => _now_iso_timestamp(),
        "h_Q" => String(_raw_get(gamma_q, :h_Q, "")),
        "profile_version" => String(_raw_get(gamma_q, :profile_version, profile.name)),
        "compiler_version" => String(_raw_get(gamma_q, :compiler_version, DEFAULT_COMPILER_VERSION)),
        "screen_policy_version" => String(_raw_get(policies, :screen_policy_version, DEFAULT_SCREEN_POLICY_VERSION)),
        "materialization_policy_version" => String(_raw_get(policies, :materialization_policy_version, DEFAULT_MATERIALIZATION_POLICY_VERSION)),
        "volume_policy_requested" => String(_raw_get(policies, :volume_policy_requested, "auto")),
        "volume_policy" => String(_raw_get(policies, :volume_policy, "none")),
        "volume_policy_coercion_reason" => _raw_get(policies, :volume_policy_coercion_reason, nothing),
        "refinement_policy_version" => String(_raw_get(policies, :refinement_policy_version, DEFAULT_REFINEMENT_POLICY_VERSION)),
        "result_count" => length(output_results),
    )
    push!(corpus["query_audit_log"], audit_record)

    return Dict(
        "updated_corpus" => corpus,
        "result" => Dict(
            "atlas_schema_version" => String(_raw_get(corpus, :atlas_schema_version, "unknown")),
            "query" => _materialize(_raw_get(gamma_q, :query, Dict{String, Any}())),
            "compiled_query" => _materialize(gamma_q),
            "versions" => _audit_versions(profile, String(_raw_get(gamma_q, :compiler_version, DEFAULT_COMPILER_VERSION)), policies),
            "result_unit" => output_unit,
            "result_count" => length(output_results),
            "results" => output_results,
            "candidate_traces" => candidate_traces,
            "new_negative_certificates" => new_negative_certs,
            "materialization_events" => collect(_raw_get(corpus, :materialization_events, Any[])),
        ),
    )
end

function _polyhedron_slack_margin(poly_dict, point::Vector{Float64})
    haskey(poly_dict, "A") && haskey(poly_dict, "b") || return nothing
    A = Matrix{Float64}(hcat([Float64.(row) for row in poly_dict["A"]]...)')
    b = Float64.(poly_dict["b"])
    size(A, 2) == length(point) || return nothing
    margins = b .- A * point
    isempty(margins) && return nothing
    return minimum(margins)
end

function _polyhedron_seed_candidates(poly_dict)
    seeds = Dict{String, Any}[]
    if haskey(poly_dict, "vertices")
        vertices = collect(poly_dict["vertices"])
        if !isempty(vertices)
            mat = reduce(vcat, [reshape(Float64.(vertex), 1, :) for vertex in vertices])
            centroid = vec(sum(mat; dims=1) ./ size(mat, 1))
            push!(seeds, Dict("seed_source" => "analytic_center_estimate", "point" => centroid))

            best_point = centroid
            best_margin = something(_polyhedron_slack_margin(poly_dict, centroid), -Inf)
            for vertex in vertices
                candidate = Float64.(vertex)
                margin = something(_polyhedron_slack_margin(poly_dict, candidate), -Inf)
                if margin > best_margin
                    best_point = candidate
                    best_margin = margin
                end
            end
            push!(seeds, Dict("seed_source" => "chebyshev_center_estimate", "point" => best_point))
        end
    end

    seen = Set{String}()
    deduped = Dict{String, Any}[]
    for seed in seeds
        key = join(round.(Float64.(seed["point"]); digits=6), ",")
        key in seen && continue
        push!(seen, key)
        push!(deduped, seed)
    end
    return deduped
end

function _evaluate_refinement_background(model, param_idx::Int, param_range, output_coeffs, fixed_params, refinement::InverseRefinementSpec, target_motifs::Vector{String}; seed_source::AbstractString, seed_point=nothing, interior_margin=nothing, polyhedron_summary=nothing)
    _, output_traj, regimes = scan_parameter_1d(
        model,
        param_idx,
        param_range,
        [output_coeffs],
        fixed_params;
        input_logspace=true,
        output_logspace=true,
    )

    values = vec(output_traj[:, 1])
    scan_motif = _scan_curve_motif(values, refinement)
    dynamic_range = isempty(values) ? 0.0 : maximum(values) - minimum(values)
    regime_transition_count = isempty(regimes) ? 0 : count(i -> regimes[i] != regimes[i + 1], 1:(length(regimes) - 1))
    motif_match = _scan_match_score(scan_motif["motif_label"], target_motifs)
    margin_bonus = interior_margin === nothing ? 0.0 : max(Float64(interior_margin), 0.0)
    refinement_score = 100.0 * motif_match + min(dynamic_range, 20.0) + 0.1 * regime_transition_count + 0.01 * margin_bonus

    trial = Dict(
        "seed_source" => String(seed_source),
        "fixed_qK_background" => collect(fixed_params),
        "numeric_motif_profile" => collect(scan_motif["motif_profile"]),
        "numeric_motif_label" => String(scan_motif["motif_label"]),
        "token_tolerance" => Float64(scan_motif["token_tolerance"]),
        "dynamic_range" => Float64(dynamic_range),
        "regime_transition_count" => regime_transition_count,
        "motif_match" => motif_match,
        "target_motif_labels" => target_motifs,
        "refinement_score" => refinement_score,
        "interior_margin" => interior_margin,
    )
    seed_point === nothing || (trial["seed_point"] = collect(seed_point))
    polyhedron_summary === nothing || (trial["polyhedron_summary"] = polyhedron_summary)
    if refinement.include_traces
        trial["param_values"] = param_range
        trial["output_trace"] = values
        trial["regimes"] = regimes
    end
    return trial
end

function _coordinate_search_background(model, param_idx::Int, param_range, output_coeffs, start_fixed, refinement::InverseRefinementSpec, target_motifs::Vector{String}; seed_source::AbstractString, seed_point=nothing, interior_margin=nothing, polyhedron_summary=nothing)
    current = copy(start_fixed)
    best = _evaluate_refinement_background(
        model,
        param_idx,
        param_range,
        output_coeffs,
        current,
        refinement,
        target_motifs;
        seed_source=seed_source,
        seed_point=seed_point,
        interior_margin=interior_margin,
        polyhedron_summary=polyhedron_summary,
    )

    for step in (1.0, 0.5)
        improved = true
        while improved
            improved = false
            for idx in eachindex(current)
                for delta in (-step, step)
                    candidate = copy(current)
                    candidate[idx] = clamp(candidate[idx] + delta, refinement.background_min, refinement.background_max)
                    trial = _evaluate_refinement_background(
                        model,
                        param_idx,
                        param_range,
                        output_coeffs,
                        candidate,
                        refinement,
                        target_motifs;
                        seed_source=seed_source,
                        seed_point=seed_point,
                        interior_margin=interior_margin,
                        polyhedron_summary=polyhedron_summary,
                    )
                    if Float64(trial["refinement_score"]) > Float64(best["refinement_score"]) + 1e-9
                        best = trial
                        current = candidate
                        improved = true
                    end
                end
            end
        end
    end

    best["local_search"] = "coordinate_search"
    return best
end

function _candidate_polyhedron(model, input_symbol::String, path_idx::Int)
    siso = SISOPaths(model, Symbol(input_symbol))
    poly = get_polyhedra(siso, [path_idx])[1]
    return polyhedron_to_dict(poly)
end

function _best_seeded_trial(result, gamma_q, refinement::InverseRefinementSpec, query::AtlasQuerySpec)
    io = _candidate_io(result, String(_raw_get(result, :result_unit, "slice")))
    rules = String.(_raw_get(result, :raw_rules, String[]))
    model, _, _, _ = build_model(rules, ones(Float64, length(rules)))
    param_idx = locate_sym_qK(model, Symbol(io.input_symbol))
    param_idx === nothing && return nothing, "unknown_input_symbol"
    output_coeffs = parse_linear_combination(model, io.output_symbol)
    param_range = collect(range(refinement.param_min, refinement.param_max, length=max(refinement.n_points, 10)))
    target_motifs = _target_motif_labels_for_result(result, query)
    witness = _raw_get(result, :best_witness_path, nothing)
    witness === nothing && return nothing, "missing_witness_path"
    path_idx = Int(_raw_get(witness, :path_idx, 0))
    path_idx > 0 || return nothing, "invalid_witness_path"

    poly_dict = _candidate_polyhedron(model, io.input_symbol, path_idx)
    haskey(poly_dict, "dimension") || return nothing, "polyhedron_unavailable"
    qk_count = length(qK_sym(model))
    poly_dim = Int(_raw_get(poly_dict, :dimension, -1))
    background_dim = max(qk_count - 1, 0)

    seeds = _polyhedron_seed_candidates(poly_dict)
    seeds = [seed for seed in seeds if length(Float64.(seed["point"])) in (background_dim, qk_count)]
    isempty(seeds) && return nothing, "polyhedron_point_dimension_mismatch"

    best_trial = nothing
    for seed in seeds
        point = Float64.(seed["point"])
        fixed_params = if length(point) == qk_count
            deleteat!(copy(point), param_idx)
        elseif length(point) == background_dim
            copy(point)
        else
            continue
        end
        trial = _coordinate_search_background(
            model,
            param_idx,
            param_range,
            output_coeffs,
            fixed_params,
            refinement,
            target_motifs;
            seed_source=String(seed["seed_source"]),
            seed_point=point,
            interior_margin=_polyhedron_slack_margin(poly_dict, point),
            polyhedron_summary=Dict(
                "intrinsic_dimension" => poly_dim,
                "ambient_dimension" => length(point),
                "background_dimension" => background_dim,
                "n_constraints" => Int(_raw_get(poly_dict, :n_constraints, 0)),
                "n_vertices" => Int(_raw_get(poly_dict, :n_vertices, 0)),
            ),
        )
        if best_trial === nothing || Float64(trial["refinement_score"]) > Float64(best_trial["refinement_score"])
            best_trial = trial
        end
    end

    return best_trial, nothing
end

function refine_top_k(query_result, gamma_q, refinement_policy::InverseRefinementSpec)
    query = atlas_query_spec_from_raw(_raw_get(gamma_q, :query, Dict{String, Any}()))
    results = collect(_raw_get(query_result, :results, Any[]))
    result_unit = String(_raw_get(query_result, :result_unit, "slice"))

    refinement_policy.enabled || return Dict(
        "enabled" => false,
        "evaluated_count" => 0,
        "refined_unit" => result_unit,
        "results" => Dict{String, Any}[],
    )
    isempty(results) && return Dict(
        "enabled" => true,
        "evaluated_count" => 0,
        "refined_unit" => result_unit,
        "results" => Dict{String, Any}[],
    )

    refined_results = Dict{String, Any}[]
    max_candidates = refinement_policy.top_k > 0 ? min(refinement_policy.top_k, length(results)) : length(results)

    for result in results[1:max_candidates]
        seeded_trial, fallback_reason = _best_seeded_trial(result, gamma_q, refinement_policy, query)
        best_trial = seeded_trial

        if best_trial === nothing
            io = _candidate_io(result, result_unit)
            rules = String.(_raw_get(result, :raw_rules, String[]))
            model, _, _, _ = build_model(rules, ones(Float64, length(rules)))
            target_motifs = _target_motif_labels_for_result(result, query)
            fallback_trial, _ = _candidate_refinement_trials(model, io.input_symbol, io.output_symbol, refinement_policy, query, target_motifs)
            fallback_trial["seed_source"] = "random_fallback"
            fallback_trial["fallback_reason"] = fallback_reason
            fallback_trial["local_search"] = "random_background_scan"
            best_trial = fallback_trial
        end

        push!(refined_results, Dict(
            "network_id" => String(_raw_get(result, :network_id, "")),
            "slice_id" => result_unit == "network" ? String(_raw_get(result, :best_slice_id, "")) : String(_raw_get(result, :slice_id, "")),
            "source_rank" => Int(_raw_get(result, :rank, 0)),
            "result_unit" => result_unit,
            "input_symbol" => result_unit == "network" ? String(_raw_get(result, :best_input_symbol, "")) : String(_raw_get(result, :input_symbol, "")),
            "output_symbol" => result_unit == "network" ? String(_raw_get(result, :best_output_symbol, "")) : String(_raw_get(result, :output_symbol, "")),
            "raw_rules" => String.(_raw_get(result, :raw_rules, String[])),
            "base_species_count" => _raw_get(result, :base_species_count, nothing),
            "reaction_count" => _raw_get(result, :reaction_count, nothing),
            "max_support" => _raw_get(result, :max_support, nothing),
            "support_mass" => _raw_get(result, :support_mass, nothing),
            "target_motif_labels" => _target_motif_labels_for_result(result, query),
            "numeric_motif_label" => best_trial["numeric_motif_label"],
            "numeric_motif_profile" => best_trial["numeric_motif_profile"],
            "motif_match" => best_trial["motif_match"],
            "dynamic_range" => best_trial["dynamic_range"],
            "regime_transition_count" => best_trial["regime_transition_count"],
            "refinement_score" => best_trial["refinement_score"],
            "best_trial" => best_trial,
        ))
    end

    sort!(refined_results; by=result -> (-Float64(_raw_get(result, :refinement_score, 0.0)),
                                         Int(_raw_get(result, :base_species_count, typemax(Int))),
                                         Int(_raw_get(result, :reaction_count, typemax(Int))),
                                         Int(_raw_get(result, :max_support, typemax(Int))),
                                         Int(_raw_get(result, :support_mass, typemax(Int)))))
    for (rank, result) in enumerate(refined_results)
        result["refined_rank"] = rank
    end

    return Dict(
        "enabled" => true,
        "policy_version" => DEFAULT_REFINEMENT_POLICY_VERSION,
        "evaluated_count" => length(refined_results),
        "refined_unit" => result_unit,
        "reranked" => true,
        "results" => refined_results,
        "best_candidate" => isempty(refined_results) ? nothing : first(refined_results),
    )
end

function _result_bucket_ids_for_seed(result)
    if haskey(result, "matched_motif_buckets")
        return _sorted_unique_strings(vcat(
            [String(_raw_get(bucket, :bucket_id, "")) for bucket in collect(_raw_get(result, :matched_motif_buckets, Any[]))],
            [String(_raw_get(bucket, :bucket_id, "")) for bucket in collect(_raw_get(result, :matched_exact_buckets, Any[]))],
        ))
    end
    return _sorted_unique_strings(vcat(
        [String(_raw_get(bucket, :bucket_id, "")) for bucket in collect(_raw_get(result, :best_matched_motif_buckets, Any[]))],
        [String(_raw_get(bucket, :bucket_id, "")) for bucket in collect(_raw_get(result, :best_matched_exact_buckets, Any[]))],
    ))
end

function _ensure_refinement_seed_witnesses!(query_result, corpus, gamma_q, refinement::InverseRefinementSpec; policies=Dict{String, Any}())
    refinement.enabled || return query_result
    results = collect(_raw_get(query_result, :results, Any[]))
    max_candidates = refinement.top_k > 0 ? min(refinement.top_k, length(results)) : length(results)

    for result in results[1:max_candidates]
        _raw_get(result, :best_witness_path, nothing) === nothing || continue
        bucket_ids = _result_bucket_ids_for_seed(result)
        for bucket_id in bucket_ids
            mat = materialize_witnesses(
                corpus,
                bucket_id,
                gamma_q,
                Int(_raw_get(_raw_get(policies, :materialization_policy, Dict{String, Any}()), :refinement_budget, 2)),
                "refinement_seed";
                policies=policies,
            )
            materialized = collect(_raw_get(mat, :materialized_paths, Any[]))
            if !isempty(materialized)
                result["best_witness_path"] = _best_witness_path(materialized)
                break
            end
        end
    end
    return query_result
end

function _resolve_precomputed_query_target(spec, sqlite_path)
    if _raw_haskey(spec, :library)
        return _materialize(_raw_get(spec, :library, nothing)), "library"
    elseif _raw_haskey(spec, :atlas)
        return _materialize(_raw_get(spec, :atlas, nothing)), "atlas"
    elseif sqlite_path !== nothing && atlas_sqlite_has_library(sqlite_path)
        return atlas_sqlite_load_library(sqlite_path), "sqlite_library"
    else
        return nothing, "none"
    end
end

function run_inverse_design_pipeline_from_spec(spec)
    _raw_haskey(spec, :query) || error("Inverse design request must include `query`.")
    query_raw = _raw_get(spec, :query, nothing)

    inverse_raw = _raw_haskey(spec, :inverse_design) ? _raw_get(spec, :inverse_design, nothing) : spec
    inverse = inverse_design_spec_from_raw(inverse_raw)
    refinement = inverse_refinement_spec_from_raw(_raw_get(spec, :refinement, nothing))

    profile_source = if _raw_haskey(spec, :search_profile)
        _raw_get(spec, :search_profile, nothing)
    elseif _raw_haskey(spec, :atlas_spec) && _raw_haskey(_raw_get(spec, :atlas_spec, nothing), :search_profile)
        _raw_get(_raw_get(spec, :atlas_spec, nothing), :search_profile, nothing)
    else
        nothing
    end
    profile = atlas_search_profile_from_raw(profile_source)
    _assert_supported_profile(profile)

    query = atlas_query_spec_from_raw(query_raw)
    policies = _query_policies_from_spec(spec, refinement, query)
    gamma_q = compile_query(query_raw, profile, String(_raw_get(policies, :compiler_version, DEFAULT_COMPILER_VERSION)); strict=true, query_spec=query)

    sqlite_path = _sqlite_path_from_raw(spec)
    working_library, initial_target_kind = _resolve_precomputed_query_target(spec, sqlite_path)
    working_library !== nothing && _ensure_inverse_design_fields!(working_library)
    if working_library !== nothing && is_atlas_library(working_library)
        _ensure_inverse_design_fields!(working_library)
    end

    source_label = _raw_haskey(spec, :source_label) ? String(_raw_get(spec, :source_label, inverse.source_label)) : inverse.source_label
    source_metadata = _raw_haskey(spec, :source_metadata) ? _raw_get(spec, :source_metadata, nothing) : nothing
    library_label = _raw_haskey(spec, :library_label) ? String(_raw_get(spec, :library_label, "")) : nothing
    allow_duplicate_atlas = Bool(_raw_get(spec, :allow_duplicate_atlas, false))

    behavior_config = if _raw_haskey(spec, :behavior_config)
        atlas_behavior_config_from_raw(_raw_get(spec, :behavior_config, nothing))
    elseif _raw_haskey(spec, :atlas_spec) && _raw_haskey(_raw_get(spec, :atlas_spec, nothing), :behavior_config)
        atlas_behavior_config_from_raw(_raw_get(_raw_get(spec, :atlas_spec, nothing), :behavior_config, nothing))
    else
        atlas_behavior_config_default()
    end

    build_requested = false
    build_performed = false
    merge_performed = false
    library_was_missing = working_library === nothing
    library_created = false
    delta_atlas = nothing
    build_plan = nothing
    enumeration_summary = nothing

    if _raw_haskey(spec, :atlas_spec) && !_is_atlas_corpus(_raw_get(spec, :atlas_spec, nothing))
        build_spec = _raw_get(spec, :atlas_spec, nothing)
        raw_candidates, enumeration_summary = _resolve_build_candidates_from_spec(build_spec, profile)
        build_requested = true
        build_plan = plan_delta_build(raw_candidates, working_library === nothing ? atlas_library_default() : working_library, gamma_q, policies; profile=profile)
        delta_atlas = build_summary_delta(
            collect(_raw_get(build_plan, :build_candidates, Any[])),
            profile,
            behavior_config,
            working_library;
            sqlite_path=sqlite_path,
            skip_existing=inverse.skip_existing,
            plan=build_plan,
            policies=policies,
        )
        enumeration_summary === nothing || (delta_atlas["enumeration"] = enumeration_summary)
        build_performed = !isempty(collect(_raw_get(build_plan, :build_candidates, Any[])))
    elseif _raw_haskey(spec, :networks) || _raw_haskey(spec, :enumeration)
        raw_candidates, enumeration_summary = _resolve_build_candidates_from_spec(spec, profile)
        build_requested = true
        build_plan = plan_delta_build(raw_candidates, working_library === nothing ? atlas_library_default() : working_library, gamma_q, policies; profile=profile)
        delta_atlas = build_summary_delta(
            collect(_raw_get(build_plan, :build_candidates, Any[])),
            profile,
            behavior_config,
            working_library;
            sqlite_path=sqlite_path,
            skip_existing=inverse.skip_existing,
            plan=build_plan,
            policies=policies,
        )
        enumeration_summary === nothing || (delta_atlas["enumeration"] = enumeration_summary)
        build_performed = !isempty(collect(_raw_get(build_plan, :build_candidates, Any[])))
    elseif _raw_haskey(spec, :atlas) && _is_atlas_corpus(_raw_get(spec, :atlas, nothing))
        delta_atlas = _materialize(_raw_get(spec, :atlas, nothing))
        _ensure_inverse_design_fields!(delta_atlas)
        build_requested = true
    end

    if delta_atlas !== nothing
        _ensure_inverse_design_fields!(delta_atlas)
        if working_library === nothing
            if inverse.build_library_if_missing
                working_library = merge_atlas_delta(atlas_library_default(), delta_atlas;
                    source_label=source_label,
                    source_metadata=source_metadata,
                    allow_duplicate_atlas=allow_duplicate_atlas,
                )
                library_created = true
                merge_performed = true
            end
        else
            working_library = merge_atlas_delta(working_library, delta_atlas;
                source_label=source_label,
                source_metadata=source_metadata,
                allow_duplicate_atlas=allow_duplicate_atlas,
            )
            merge_performed = true
        end
    elseif working_library === nothing && inverse.build_library_if_missing
        working_library = atlas_library_default()
        _ensure_inverse_design_fields!(working_library)
        library_created = true
    end

    query_target = working_library === nothing ? delta_atlas : working_library
    query_target === nothing && error("Inverse design request must include an atlas library, atlas, atlas spec, or atlas build fields.")

    candidate_set = retrieve_candidates(gamma_q, query_target, profile; policies=policies)
    query_result = candidate_set["result"]
    updated_target = candidate_set["updated_corpus"]
    query_result = _ensure_refinement_seed_witnesses!(query_result, updated_target, gamma_q, refinement; policies=policies)
    refinement_result = refine_top_k(query_result, gamma_q, refinement)

    if working_library !== nothing
        working_library = updated_target
    else
        delta_atlas = updated_target
    end

    sqlite_path !== nothing && working_library !== nothing && atlas_sqlite_save_library!(sqlite_path, working_library)

    best_design = if Bool(_raw_get(refinement_result, :enabled, false)) && !isempty(collect(_raw_get(refinement_result, :results, Any[])))
        Dict(
            "selection_source" => "refinement",
            "candidate" => _materialize(first(collect(_raw_get(refinement_result, :results, Any[])))),
        )
    elseif !isempty(collect(_raw_get(query_result, :results, Any[])))
        Dict(
            "selection_source" => "query",
            "candidate" => _materialize(first(collect(_raw_get(query_result, :results, Any[])))),
        )
    else
        nothing
    end

    result = Dict(
        "inverse_design_schema_version" => "0.2.0",
        "generated_at" => _now_iso_timestamp(),
        "pipeline_version" => INVERSE_DESIGN_PIPELINE_VERSION,
        "source_label" => source_label,
        "source_metadata" => source_metadata === nothing ? nothing : _materialize(source_metadata),
        "inverse_design" => inverse_design_spec_to_dict(inverse),
        "refinement" => inverse_refinement_spec_to_dict(refinement),
        "policies" => _materialize(policies),
        "query" => atlas_query_spec_to_dict(query),
        "compiled_query" => gamma_q,
        "build_requested" => build_requested,
        "build_performed" => build_performed,
        "merge_performed" => merge_performed,
        "library_was_missing" => library_was_missing,
        "library_created" => library_created,
        "query_target_kind" => working_library === nothing ? "atlas" : "atlas_library",
        "build_source_mode" => delta_atlas === nothing ? initial_target_kind : "summary_delta",
        "build_plan" => build_plan,
        "delta_atlas_summary" => _atlas_summary(delta_atlas),
        "library_summary" => _atlas_library_summary(working_library),
        "query_result" => query_result,
        "refinement_result" => refinement_result,
        "best_design" => best_design,
    )

    if query_raw isa AbstractDict && _raw_haskey(query_raw, :goal)
        result["query"] = Dict{String, Any}(_materialize(result["query"]))
        result["query"]["goal"] = _materialize(_raw_get(query_raw, :goal, Dict{String, Any}()))
    end

    sqlite_path === nothing || (result["sqlite_path"] = sqlite_path)
    sqlite_path === nothing || (result["sqlite_summary"] = atlas_sqlite_summary(sqlite_path))
    inverse.return_delta_atlas && delta_atlas !== nothing && (result["delta_atlas"] = delta_atlas)
    inverse.return_library && working_library !== nothing && (result["library"] = working_library)
    library_label !== nothing && !isempty(library_label) && (result["library_label"] = library_label)

    return result
end

function query_behavior_atlas_v2(atlas, raw_query_or_spec; strict::Bool=false, compiler_version::AbstractString=DEFAULT_COMPILER_VERSION)
    corpus = _ensure_inverse_design_fields!(_materialize(atlas))
    profile = atlas_search_profile_from_raw(_raw_get(corpus, :search_profile, nothing))
    _assert_supported_profile(profile)
    query = raw_query_or_spec isa AtlasQuerySpec ? raw_query_or_spec : atlas_query_spec_from_raw(raw_query_or_spec)
    raw_query = raw_query_or_spec isa AtlasQuerySpec ? atlas_query_spec_to_dict(raw_query_or_spec) : raw_query_or_spec
    policies = Dict(
        "compiler_version" => String(compiler_version),
        "screen_policy_version" => DEFAULT_SCREEN_POLICY_VERSION,
        "materialization_policy_version" => DEFAULT_MATERIALIZATION_POLICY_VERSION,
        "refinement_policy_version" => DEFAULT_REFINEMENT_POLICY_VERSION,
        "volume_policy_version" => DEFAULT_VOLUME_POLICY_VERSION,
        "volume_policy" => query.require_witness_robust || query.min_witness_volume_mean !== nothing ? "estimated" : "none",
        "materialization_policy" => Dict("default_budget" => 1, "refinement_budget" => 2),
    )
    gamma_q = compile_query(raw_query, profile, compiler_version; strict=strict, query_spec=query)
    return retrieve_candidates(gamma_q, corpus, profile; policies=policies)
end
