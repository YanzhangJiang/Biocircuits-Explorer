# Atlas/library/query/inverse-design request-shape budgets. Included only after
# the atlas, SQLite, and inverse-design substrates are defined; common scalar,
# model, scan, and process-wide gate limits remain in sync_work_budget.jl.

const MAX_SYNC_ATLAS_NETWORKS = 8
const MAX_SYNC_ATLAS_SELECTORS = 24
const MAX_SYNC_ATLAS_CHANGE_DIMS = 4
const MAX_SYNC_ATLAS_CHANGE_EXPANSION_CANDIDATES = 2_000
const MAX_SYNC_ATLAS_PLANNED_SLICES = 512
const MAX_SYNC_ATLAS_REGIME_CANDIDATES = 100_000
const MAX_SYNC_ATLAS_ANALYSIS_COST = 10_000_000
const MAX_SYNC_ATLAS_QUERY_RESULTS = 100
const MAX_SYNC_ATLAS_QUERY_SLICES = 100
const MAX_SYNC_ATLAS_QUERY_RECORDS = 10_000
const MAX_SYNC_ATLAS_QUERY_NETWORKS = 100
const MAX_SYNC_ATLAS_SQLITE_BYTES = 128 * 1024 * 1024
const MAX_SYNC_ATLAS_QUERY_ITEMS = 64
const MAX_SYNC_ATLAS_QUERY_PREDICATES = 64
const MAX_SYNC_ATLAS_QUERY_SEQUENCE_LENGTH = 16
const MAX_SYNC_ATLAS_QUERY_COMPLEXITY = 128
const MAX_SYNC_ATLAS_QUERY_COST = 256_000
const MAX_SYNC_ATLAS_QUERY_BYTES = 16 * 1024
const MAX_SYNC_ATLAS_QUERY_BYTES_COST = 32 * 1024 * 1024
const MAX_SYNC_ATLAS_QUERY_VALUE_NODES = 256
const MAX_SYNC_ATLAS_QUERY_TOKEN_LENGTH = 64
const MAX_SYNC_ATLAS_QUERY_STRING_BYTES = 1024
const MAX_SYNC_INVERSE_TOP_K = 8
const MAX_SYNC_INVERSE_TRIALS = 16
const MAX_SYNC_INVERSE_POINTS = 512

# Atlas service requests have several wrappers around the same build shape:
# direct `networks`/`enumeration` fields and a nested `atlas_spec`. Pre-built
# atlas/library/SQLite references do not create new model work and must remain
# valid on the synchronous endpoints.
function _sync_is_atlas_corpus(raw)
    (raw isa AbstractDict || raw isa JSON3.Object) || return false
    return _raw_haskey(raw, :network_entries) &&
           _raw_haskey(raw, :behavior_slices) &&
           _raw_haskey(raw, :family_buckets)
end

function _sync_validate_symbol_list(values, context::AbstractString; max_items::Int)
    values isa AbstractVector || throw(ArgumentError("$context must be an array"))
    1 <= length(values) <= max_items ||
        _sync_budget_exceeded("$context must contain 1 to $max_items symbols.")
    all(value -> value isa AbstractString && !isempty(value) && ncodeunits(value) <= 128,
        values) ||
        throw(ArgumentError("$context must contain bounded nonempty strings"))
    length(unique(String.(values))) == length(values) ||
        throw(ArgumentError("$context must not contain duplicate symbols"))
    return values
end

function _sync_validate_change_spec(raw_change, context::AbstractString, model_n::Int)
    if raw_change isa AbstractString
        isempty(raw_change) && throw(ArgumentError("$context must not be empty"))
        ncodeunits(raw_change) <= 128 || _sync_budget_exceeded("$context is too long.")
        return raw_change
    end
    (raw_change isa AbstractDict || raw_change isa JSON3.Object) ||
        throw(ArgumentError("$context must be a string or object"))
    kind_raw = _raw_get(raw_change, :kind, "axis")
    kind_raw isa AbstractString || throw(ArgumentError("$context.kind must be a string"))
    kind = lowercase(String(kind_raw))
    kind in ("axis", "orthant") ||
        throw(ArgumentError("$context.kind must be axis or orthant"))

    symbols = if _raw_haskey(raw_change, :qk_symbols)
        _raw_get(raw_change, :qk_symbols, nothing)
    elseif _raw_haskey(raw_change, :input_symbols)
        _raw_get(raw_change, :input_symbols, nothing)
    elseif _raw_haskey(raw_change, :change_qK)
        value = _raw_get(raw_change, :change_qK, nothing)
        value isa AbstractString ||
            throw(ArgumentError("$context.change_qK must be a string"))
        Any[value]
    else
        throw(ArgumentError("$context must include qk_symbols, input_symbols, or change_qK"))
    end
    max_dims = min(model_n, MAX_SYNC_ATLAS_CHANGE_DIMS)
    _sync_validate_symbol_list(symbols, "$context.qk_symbols"; max_items=max_dims)
    kind == "axis" && length(symbols) != 1 &&
        throw(ArgumentError("$context axis changes require exactly one symbol"))

    signs = if _raw_haskey(raw_change, :qk_signs)
        _raw_get(raw_change, :qk_signs, nothing)
    elseif _raw_haskey(raw_change, :signs)
        _raw_get(raw_change, :signs, nothing)
    else
        nothing
    end
    if signs !== nothing
        signs isa AbstractVector || throw(ArgumentError("$context signs must be an array"))
        length(signs) == length(symbols) ||
            throw(ArgumentError("$context signs length must match qk_symbols"))
        for sign in signs
            valid = (sign isa Integer && !(sign isa Bool) && sign != 0) ||
                    (sign isa AbstractString && lowercase(strip(String(sign))) in
                        ("+", "-", "plus", "minus", "positive", "negative",
                         "inc", "dec", "up", "down", "increase", "decrease"))
            valid || throw(ArgumentError("$context signs must be positive/negative directions"))
        end
    end
    if _raw_haskey(raw_change, :label)
        label = _raw_get(raw_change, :label, nothing)
        (label isa AbstractString && ncodeunits(label) <= 128) ||
            throw(ArgumentError("$context.label must be a bounded string"))
    end
    return raw_change
end

function sync_change_expansion_candidate_bound(
    input_count::Int,
    mode::Symbol,
    max_active_dims::Int,
    include_axes::Bool,
    include_negative_directions::Bool,
)
    max_dims = min(max_active_dims, input_count)
    combo_sizes = Int[]
    (include_axes || max_dims <= 1) && push!(combo_sizes, 1)
    mode == :orthant && max_dims >= 2 && append!(combo_sizes, 2:max_dims)
    candidate_count = 0
    for dims in combo_sizes
        sign_variants = include_negative_directions ? (1 << dims) : 1
        term = binomial(input_count, dims) * sign_variants
        candidate_count <= MAX_SYNC_ATLAS_CHANGE_EXPANSION_CANDIDATES - term ||
            _sync_budget_exceeded(
                "Change expansion exceeds $(MAX_SYNC_ATLAS_CHANGE_EXPANSION_CANDIDATES) pre-limit candidates.")
        candidate_count += term
    end
    return candidate_count
end

function _enforce_sync_explicit_networks_budget(
    networks;
    generated_change_cap::Union{Nothing, Int}=nothing,
    generated_change_mode::Symbol=:axes_only,
    generated_max_active_dims::Int=1,
    generated_include_axes::Bool=true,
    include_negative_directions::Bool=false,
)
    networks isa AbstractVector ||
        throw(ArgumentError("networks must be an array"))
    length(networks) <= MAX_SYNC_ATLAS_NETWORKS ||
        _sync_budget_exceeded(
            "Explicit network count exceeds the synchronous limit of $(MAX_SYNC_ATLAS_NETWORKS).")

    total_candidate_bound = 0
    total_planned_slices = 0
    total_analysis_cost = 0
    for (idx, network) in enumerate(networks)
        (network isa AbstractDict || network isa JSON3.Object) ||
            throw(ArgumentError("networks[$idx] must be an object"))
        _raw_haskey(network, :reactions) ||
            throw(ArgumentError("networks[$idx] must include `reactions`"))
        rules = _raw_get(network, :reactions, Any[])
        rules isa AbstractVector ||
            throw(ArgumentError("networks[$idx].reactions must be an array"))
        all(rule -> rule isa AbstractString, rules) ||
            throw(ArgumentError("networks[$idx].reactions must contain only strings"))
        rules = String.(rules)
        enforce_sync_rule_budget(rules)

        kd = if _raw_haskey(network, :kd)
            raw_kd = _raw_get(network, :kd, nothing)
            raw_kd isa AbstractVector ||
                throw(ArgumentError("networks[$idx].kd must be an array"))
            length(raw_kd) == length(rules) ||
                throw(ArgumentError("networks[$idx].kd length must match reactions"))
            all(value -> value isa Real && !(value isa Bool) && isfinite(value) && value > 0,
                raw_kd) ||
                throw(ArgumentError("networks[$idx].kd must contain finite positive numbers"))
            Float64.(raw_kd)
        else
            ones(Float64, length(rules))
        end

        # Reaction/species limits are insufficient: a small-looking binding
        # network can still induce an exponential regime candidate product.
        # Build the bounded (r <= 5, n <= 24) model only far enough to inspect
        # that helper, then fail before atlas canonicalization/enumeration.
        model, species, free_syms, _ = build_model(rules, kd)
        enforce_sync_model_budget(model)
        candidate_bound = sync_model_candidate_bound(model)
        total_candidate_bound <= MAX_SYNC_ATLAS_REGIME_CANDIDATES - candidate_bound ||
            _sync_budget_exceeded(
                "Combined explicit-network regime candidate bound exceeds $(MAX_SYNC_ATLAS_REGIME_CANDIDATES).")
        total_candidate_bound += candidate_bound

        for field in (:input_symbols, :output_symbols, :change_specs)
            _raw_haskey(network, field) || continue
            values = _raw_get(network, field, nothing)
            values isa AbstractVector ||
                throw(ArgumentError("networks[$idx].$(String(field)) must be an array"))
            length(values) <= MAX_SYNC_ATLAS_SELECTORS ||
                _sync_budget_exceeded(
                    "networks[$idx].$(String(field)) exceeds $(MAX_SYNC_ATLAS_SELECTORS) selectors.")
        end
        for field in (:input_symbols, :output_symbols)
            _raw_haskey(network, field) || continue
            _sync_validate_symbol_list(
                _raw_get(network, field, nothing),
                "networks[$idx].$(String(field))";
                max_items=MAX_SYNC_ATLAS_SELECTORS,
            )
        end
        if _raw_haskey(network, :change_specs)
            for (change_idx, change_spec) in
                enumerate(_raw_get(network, :change_specs, Any[]))
                _sync_validate_change_spec(
                    change_spec,
                    "networks[$idx].change_specs[$change_idx]",
                    model.n,
                )
            end
        end
        input_count = _raw_haskey(network, :input_symbols) ?
            length(_raw_get(network, :input_symbols, Any[])) : length(free_syms)
        if !_raw_haskey(network, :change_specs)
            sync_change_expansion_candidate_bound(
                input_count,
                generated_change_mode,
                generated_max_active_dims,
                generated_include_axes,
                include_negative_directions,
            )
        end
        change_count = if _raw_haskey(network, :change_specs)
            length(_raw_get(network, :change_specs, Any[]))
        elseif generated_change_cap !== nothing
            generated_change_cap
        else
            input_count * (include_negative_directions ? 2 : 1)
        end
        output_count = _raw_haskey(network, :output_symbols) ?
            length(_raw_get(network, :output_symbols, Any[])) : length(species)
        planned_slices = change_count * output_count
        total_planned_slices <= MAX_SYNC_ATLAS_PLANNED_SLICES - planned_slices ||
            _sync_budget_exceeded(
                "Explicit networks exceed $(MAX_SYNC_ATLAS_PLANNED_SLICES) planned behavior slices.")
        total_planned_slices += planned_slices
        if planned_slices > 0 && candidate_bound > 0
            candidate_bound <= MAX_SYNC_ATLAS_ANALYSIS_COST ÷ planned_slices ||
                _sync_budget_exceeded(
                    "One explicit network exceeds the candidate bound * planned slices analysis budget.")
            analysis_cost = candidate_bound * planned_slices
            total_analysis_cost <= MAX_SYNC_ATLAS_ANALYSIS_COST - analysis_cost ||
                _sync_budget_exceeded(
                    "Explicit-network candidate bound * planned slices exceeds the synchronous atlas analysis budget.")
            total_analysis_cost += analysis_cost
        end
    end
    return networks
end

function _enforce_sync_atlas_build_shape(raw)
    (raw isa AbstractDict || raw isa JSON3.Object) ||
        throw(ArgumentError("atlas build specification must be an object"))
    _sync_is_atlas_corpus(raw) && return raw

    _raw_haskey(raw, :enumeration) &&
        _sync_budget_exceeded(
            "Atlas enumeration is not available on synchronous service endpoints.")

    for field in (:skip_existing, :persist_sqlite, :allow_duplicate_atlas)
        _raw_haskey(raw, field) || continue
        _raw_get(raw, field, nothing) isa Bool ||
            throw(ArgumentError("$(String(field)) must be a boolean"))
    end
    for field in (:source_label, :library_label, :sqlite_persist_mode)
        _raw_haskey(raw, field) || continue
        value = _raw_get(raw, field, nothing)
        (value === nothing || value isa AbstractString) ||
            throw(ArgumentError("$(String(field)) must be a string"))
    end
    if _raw_haskey(raw, :network_parallelism)
        sync_bounded_int(_raw_get(raw, :network_parallelism, nothing),
            "network_parallelism"; min=1, max=MAX_SYNC_ATLAS_NETWORKS)
    end

    if _raw_haskey(raw, :search_profile)
        profile_raw = _raw_get(raw, :search_profile, nothing)
        (profile_raw isa AbstractDict || profile_raw isa JSON3.Object) ||
            throw(ArgumentError("search_profile must be an object"))
        for field in (
            :allow_reversible_binding,
            :allow_catalysis,
            :allow_irreversible_steps,
            :allow_conformational_switches,
            :allow_higher_order_templates,
            :allow_homomeric_templates,
        )
            _raw_haskey(profile_raw, field) || continue
            _raw_get(profile_raw, field, nothing) isa Bool ||
                throw(ArgumentError("search_profile.$(String(field)) must be a boolean"))
        end
        for (field, maximum) in (
            (:max_base_species, MAX_EXACT_CANONICAL_FREE_SPECIES),
            (:max_reactions, MAX_SYNC_REACTIONS),
            (:max_support, MAX_SYNC_MODEL_N),
            (:max_homomer_order, MAX_SYNC_MODEL_N),
        )
            _raw_haskey(profile_raw, field) || continue
            sync_bounded_int(_raw_get(profile_raw, field, nothing),
                "search_profile.$(String(field))"; min=1, max=maximum)
        end
        for field in (:name, :slice_mode, :input_mode)
            _raw_haskey(profile_raw, field) || continue
            _raw_get(profile_raw, field, nothing) isa AbstractString ||
                throw(ArgumentError("search_profile.$(String(field)) must be a string"))
        end
        profile = try
            atlas_search_profile_from_raw(profile_raw)
        catch err
            throw(ArgumentError("Invalid search_profile: $(sprint(showerror, err))"))
        end
        _assert_supported_profile(profile)
        profile_defaults = atlas_search_profile_binding_small_v0()
        profile.max_base_species <= profile_defaults.max_base_species ||
            _sync_budget_exceeded(
                "Expanded search_profile.max_base_species is asynchronous-only.")
        profile.max_reactions <= profile_defaults.max_reactions ||
            _sync_budget_exceeded(
                "Expanded search_profile.max_reactions is asynchronous-only.")
        profile.max_support <= profile_defaults.max_support ||
            _sync_budget_exceeded(
                "Expanded search_profile.max_support is asynchronous-only.")
        profile.max_homomer_order <= profile_defaults.max_homomer_order ||
            _sync_budget_exceeded(
                "Expanded search_profile.max_homomer_order is asynchronous-only.")
        profile.allow_higher_order_templates &&
            _sync_budget_exceeded(
                "Higher-order template search profiles are asynchronous-only.")
        profile.allow_homomeric_templates &&
            _sync_budget_exceeded(
                "Homomeric template search profiles are asynchronous-only.")
    end

    if _raw_haskey(raw, :behavior_config)
        behavior_raw = _raw_get(raw, :behavior_config, nothing)
        (behavior_raw isa AbstractDict || behavior_raw isa JSON3.Object) ||
            throw(ArgumentError("behavior_config must be an object"))
        behavior_defaults = atlas_behavior_config_default()
        for field in (
            :deduplicate,
            :keep_singular,
            :keep_nonasymptotic,
            :compute_volume,
            :include_path_records,
        )
            value = _raw_get(behavior_raw, field, getfield(behavior_defaults, field))
            value isa Bool ||
                throw(ArgumentError("behavior_config.$(String(field)) must be a boolean"))
        end
        Bool(_raw_get(behavior_raw, :compute_volume, behavior_defaults.compute_volume)) &&
            _sync_budget_exceeded(
                "behavior_config.compute_volume is available through asynchronous jobs only.")
        Bool(_raw_get(behavior_raw, :include_path_records,
            behavior_defaults.include_path_records)) &&
            _sync_budget_exceeded(
                "behavior_config.include_path_records is available through asynchronous jobs only.")
        path_scope = _raw_get(behavior_raw, :path_scope, String(behavior_defaults.path_scope))
        path_scope isa AbstractString ||
            throw(ArgumentError("behavior_config.path_scope must be a string"))
        lowercase(String(path_scope)) == "feasible" ||
            _sync_budget_exceeded(
                "Only behavior_config.path_scope=feasible is available synchronously.")
        for (field, default) in (
            (:min_volume_mean, behavior_defaults.min_volume_mean),
            (:motif_zero_tol, behavior_defaults.motif_zero_tol),
        )
            value = sync_finite_float(_raw_get(behavior_raw, field, default),
                "behavior_config.$(String(field))"; abs_max=1.0)
            value >= 0 ||
                throw(ArgumentError("behavior_config.$(String(field)) must be nonnegative"))
        end
        logqk_min = _raw_get(behavior_raw, :logqk_min, behavior_defaults.logqk_min)
        logqk_max = _raw_get(behavior_raw, :logqk_max, behavior_defaults.logqk_max)
        logqk_min === nothing || sync_finite_float(
            logqk_min, "behavior_config.logqk_min"; abs_max=20.0)
        logqk_max === nothing || sync_finite_float(
            logqk_max, "behavior_config.logqk_max"; abs_max=20.0)
        if logqk_min !== nothing && logqk_max !== nothing
            Float64(logqk_max) > Float64(logqk_min) ||
                throw(ArgumentError(
                    "behavior_config.logqk_max must exceed behavior_config.logqk_min"))
        end
        sync_bounded_int(_raw_get(behavior_raw, :ro_quantization_digits,
                behavior_defaults.ro_quantization_digits),
            "behavior_config.ro_quantization_digits"; min=0, max=9)
        sync_bounded_int(_raw_get(behavior_raw, :ro_quantization_scale,
                behavior_defaults.ro_quantization_scale),
            "behavior_config.ro_quantization_scale"; min=1, max=1_000_000)
        for (field, default) in (
            (:program_identity, behavior_defaults.program_identity),
            (:support_semantics, behavior_defaults.support_semantics),
        )
            value = _raw_get(behavior_raw, field, default)
            value isa AbstractString ||
                throw(ArgumentError("behavior_config.$(String(field)) must be a string"))
            ncodeunits(value) <= 256 ||
                _sync_budget_exceeded("behavior_config.$(String(field)) is too long.")
        end
    end

    generated_change_cap = nothing
    generated_change_mode = :axes_only
    generated_max_active_dims = 1
    generated_include_axes = true
    include_negative_directions = false
    if _raw_haskey(raw, :change_expansion)
        expansion = _raw_get(raw, :change_expansion, nothing)
        (expansion isa AbstractDict || expansion isa JSON3.Object) ||
            throw(ArgumentError("change_expansion must be an object"))
        mode_raw = _raw_get(expansion, :mode, "axes_only")
        mode_raw isa AbstractString ||
            throw(ArgumentError("change_expansion.mode must be a string"))
        mode = lowercase(String(mode_raw))
        mode in ("axes_only", "orthant") ||
            throw(ArgumentError("change_expansion.mode must be axes_only or orthant"))
        for field in (:include_axis_slices, :include_negative_directions)
            _raw_haskey(expansion, field) || continue
            _raw_get(expansion, field, nothing) isa Bool ||
                throw(ArgumentError("change_expansion.$(String(field)) must be a boolean"))
        end
        generated_change_mode = Symbol(mode)
        generated_include_axes = Bool(
            _raw_get(expansion, :include_axis_slices, true))
        include_negative_directions = Bool(
            _raw_get(expansion, :include_negative_directions, false))
        generated_max_active_dims = sync_bounded_int(
            _raw_get(expansion, :max_active_dims, 1),
            "change_expansion.max_active_dims";
            min=1, max=MAX_SYNC_ATLAS_CHANGE_DIMS,
        )
        limit_per_network = sync_bounded_int(
            _raw_get(expansion, :limit_per_network, 0),
            "change_expansion.limit_per_network";
            min=0, max=MAX_SYNC_ATLAS_PLANNED_SLICES,
        )
        if mode == "orthant"
            limit_per_network > 0 ||
                _sync_budget_exceeded(
                    "Synchronous orthant change expansion requires a positive limit_per_network.")
            generated_change_cap = limit_per_network
        end
    end
    if _raw_haskey(raw, :networks)
        _enforce_sync_explicit_networks_budget(
            _raw_get(raw, :networks, nothing);
            generated_change_cap=generated_change_cap,
            generated_change_mode=generated_change_mode,
            generated_max_active_dims=generated_max_active_dims,
            generated_include_axes=generated_include_axes,
            include_negative_directions=include_negative_directions,
        )
    end
    return raw
end
