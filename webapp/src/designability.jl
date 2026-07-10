const DESIGNABILITY_SPEC_VERSION = "bne-designability/v1.0.0"
const DESIGN_SCREEN_SCHEMA_VERSION = "bne-design-screen/v0.3.0"
const DESIGN_OUTPUT_FEATURE_KEYS = (
    "feature", "operator", "value", "sample_points", "tolerance_log10", "hard",
)
const DESIGN_SHAPE_KEYS = (
    "class", "monotonicity", "sample_points", "tolerance_log10",
    "min_prominence_log10", "min_prominence_decades", "hard",
)
const DESIGN_TEMPORAL_DYNAMICS_KEYS = (
    "stimulus", "trace", "peak_width_seconds", "hard",
)
const DESIGN_TEMPORAL_PEAK_WIDTH_KEYS = ("min", "max")
const DESIGN_DYNAMIC_RANGE_KEYS = ("min_fold_change", "sample_points", "hard")
const DESIGN_NETWORK_KEYS = (
    "max_species", "max_reactions", "max_mu", "allow_near_minimal",
)
const DESIGN_ROBUSTNESS_KEYS = (
    "min_chebyshev_radius", "min_tunable_volume_lower_bound", "min_tunable_volume",
    "condition_number_max", "min_sampled_pass_fraction", "hard",
)
const DESIGN_BEHAVIOR_SPEC_KEYS = (
    "input", "output", "program", "feature_space", "input_window",
)
const DESIGN_INPUT_WINDOW_KEYS = (
    "input_log10", "hard", "min_spacing_decades", "operating_points_log10",
)
const DESIGN_BEHAVIOR_STEP_KEYS = (
    "kind", "value", "operator", "hard",
)
const DESIGN_TRANSITION_KEYS = (
    "min_spacing_decades", "order", "hard",
)
const DESIGN_TARGET_KEYS = (
    "legacy_target", "behavior_spec", "input_window", "output_feature",
    "shape", "temporal_dynamics",
)
const DESIGN_LEGACY_TARGET_KEYS = (
    "target_kind", "target",
)
const DESIGN_CONSTRAINT_KEYS = (
    "network", "parameter_bounds", "robustness", "dynamic_range", "transitions",
)
const DESIGN_RANKING_PREFERENCE_KEYS = (
    "evidence_grade", "certificate_grade", "chebyshev_radius", "tunable_volume",
    "dynamic_range", "transition_spacing", "sampled_robustness", "condition_number",
    "complexity",
)
const DESIGN_SPEC_ROOT_KEYS = (
    "schema_version", "source", "target", "constraints", "candidate_budget",
    "ranking_policy", "audit_policy",
)
const DESIGN_SOURCE_KEYS = (
    "kind", "node_id", "agent_message_id", "provenance",
)
const DESIGN_SOURCE_KIND_VALUES = (
    "manual_config", "agent_design", "legacy_shorthand", "imported_json",
    "test_fixture", "hand_authored",
)
const DESIGN_CANDIDATE_BUDGET_KEYS = (
    "mode", "max_extra_species", "max_extra_reactions", "max_extra_mu",
    "max_screened", "max_verified_recommendations", "max_recommended",
    "max_near_misses", "max_exact_placements",
)
const DESIGN_RANKING_POLICY_KEYS = (
    "verified_only", "prefer",
)
const DESIGN_AUDIT_POLICY_KEYS = (
    "unsupported", "path_format", "include_supported",
)

Base.@kwdef struct DesignabilityAuditItem
    path::String
    kind::String
    support_level::String
    hard::Bool = true
    stage::String = "compile"
    solver::String = "none"
    reason::String = ""
end

Base.@kwdef struct NormalizedDesignabilitySpec
    schema_version::String = DESIGNABILITY_SPEC_VERSION
    source::Dict{String, Any} = Dict{String, Any}()
    target::Dict{String, Any} = Dict{String, Any}()
    constraints::Dict{String, Any} = Dict{String, Any}()
    candidate_budget::Dict{String, Any} = Dict{String, Any}()
    ranking_policy::Dict{String, Any} = Dict{String, Any}()
    audit_policy::Dict{String, Any} = Dict{String, Any}()
    has_legacy_target::Bool = false
    has_search_target::Bool = false
    legacy_target_kind::String = "sign"
    legacy_target::Any = "+-+"
    target_source_path::String = "/target/legacy_target"
    required_input::Union{Nothing, String} = nothing
    required_output::Union{Nothing, String} = nothing
    audit::Vector{DesignabilityAuditItem} = DesignabilityAuditItem[]
end

audit_to_dict(item::DesignabilityAuditItem) = Dict(
    "path" => item.path,
    "kind" => item.kind,
    "support_level" => item.support_level,
    "hard" => item.hard,
    "stage" => item.stage,
    "solver" => item.solver,
    "reason" => item.reason,
)

function legacy_designability_spec(target_kind, target; candidate_budget=Dict{String,Any}(),
                                   tuning=Dict{String,Any}(), ranking_policy=Dict{String,Any}())
    constraints = Dict{String, Any}()
    bounds = Dict{String, Any}("basis" => "log10_qK")
    by_class = Dict{String, Any}()
    if haskey(tuning, "kd_bounds") || haskey(tuning, :kd_bounds)
        by_class["kd"] = collect(_raw_get(tuning, :kd_bounds, [-3.0, 3.0]))
    end
    if haskey(tuning, "total_bounds") || haskey(tuning, :total_bounds)
        by_class["total"] = collect(_raw_get(tuning, :total_bounds, [-3.0, 3.0]))
    end
    if !isempty(by_class)
        bounds["by_class"] = by_class
        constraints["parameter_bounds"] = bounds
    end
    # Keep every container JSON-faithful and open to heterogeneous values.
    # An inferred Dict here used to make `target` a
    # Dict{String,Dict{String,String}} for string legacy targets, so merging a
    # structured extension such as temporal_dynamics failed before validation.
    return Dict{String, Any}(
        "schema_version" => DESIGNABILITY_SPEC_VERSION,
        "source" => Dict{String, Any}("kind" => "legacy_shorthand"),
        "target" => Dict{String, Any}(
            "legacy_target" => Dict{String, Any}(
                "target_kind" => String(target_kind),
                "target" => target,
            ),
        ),
        "constraints" => constraints,
        "candidate_budget" => Dict{String, Any}(String(k) => v for (k, v) in pairs(candidate_budget)),
        "ranking_policy" => Dict{String, Any}(String(k) => v for (k, v) in pairs(ranking_policy)),
        "audit_policy" => Dict{String, Any}(
            "unsupported" => "block_if_hard",
            "path_format" => "json_pointer",
            "include_supported" => true,
        ),
    )
end

function _design_push_invalid_legacy_exact_target!(audit::Vector{DesignabilityAuditItem},
                                                   reason::AbstractString)
    push!(audit, DesignabilityAuditItem(
        path = "/target/legacy_target/target",
        kind = "legacy_exact_target",
        support_level = "unsupported",
        hard = true,
        stage = "compile",
        solver = "none",
        reason = String(reason),
    ))
    return nothing
end

function _design_normalize_legacy_exact_target(target, audit::Vector{DesignabilityAuditItem})
    target === nothing &&
        return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target must be an array of finite non-Bool real numbers.")
    target isa AbstractString &&
        return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target must be an array, not a string.")
    (target isa AbstractVector || target isa Tuple) ||
        return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target must be an array of finite non-Bool real numbers.")
    vals = Float64[]
    for value in collect(target)
        if value isa Bool || !(value isa Real)
            return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target values must be finite non-Bool real numbers.")
        end
        f = Float64(value)
        isfinite(f) ||
            return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target values must be finite non-Bool real numbers.")
        push!(vals, f)
    end
    isempty(vals) &&
        return _design_push_invalid_legacy_exact_target!(audit, "Exact legacy target must contain at least one finite number.")
    return vals
end

function _design_qualitative_legacy_feasible_region_requested(constraints_raw, candidate_budget::AbstractDict)
    explicit_bounds = constraints_raw isa AbstractDict && _raw_haskey(constraints_raw, :parameter_bounds)
    max_exact = max(0, _design_int(candidate_budget, :max_exact_placements, 0))
    return explicit_bounds || max_exact > 0
end

function normalize_designability_spec(raw)::NormalizedDesignabilitySpec
    raw isa AbstractDict || error("designability_spec must be an object")
    version = String(_raw_get(raw, :schema_version, ""))
    version == DESIGNABILITY_SPEC_VERSION || error("unsupported designability schema_version: $version")

    source_raw = _raw_get(raw, :source, nothing)
    source = source_raw isa AbstractDict ? Dict{String, Any}(_materialize(source_raw)) : Dict{String, Any}()
    target_raw = _raw_get(raw, :target, nothing)
    target_raw isa AbstractDict || error("target must be an object")
    target = Dict{String, Any}(_materialize(target_raw))
    constraints_raw = _raw_get(raw, :constraints, nothing)
    candidate_budget_raw = _raw_get(raw, :candidate_budget, nothing)
    ranking_policy_raw = _raw_get(raw, :ranking_policy, nothing)
    audit_policy_raw = _raw_get(raw, :audit_policy, nothing)
    constraints = constraints_raw isa AbstractDict ? Dict{String, Any}(_materialize(constraints_raw)) : Dict{String, Any}()
    candidate_budget = candidate_budget_raw isa AbstractDict ? Dict{String, Any}(_materialize(candidate_budget_raw)) : Dict{String, Any}()
    ranking_policy = ranking_policy_raw isa AbstractDict ? Dict{String, Any}(_materialize(ranking_policy_raw)) : Dict{String, Any}()
    audit_policy = audit_policy_raw isa AbstractDict ? Dict{String, Any}(_materialize(audit_policy_raw)) : Dict{String, Any}()
    legacy = _raw_get(target, :legacy_target, nothing)
    has_legacy = legacy isa AbstractDict
    ambiguous_legacy_behavior = has_legacy && _raw_haskey(target, :behavior_spec)
    ambiguous_behavior_input_window = _raw_haskey(target, :behavior_spec) && _raw_haskey(target, :input_window)
    has_search_target = false
    kind = "sign"
    val = "+-+"
    target_source_path = "/target/legacy_target"
    required_input = nothing
    required_output = nothing
    audit = DesignabilityAuditItem[]
    _design_audit_unknown_nested_keys!(
        audit,
        raw,
        "",
        DESIGN_SPEC_ROOT_KEYS,
        "unsupported_spec_key",
    )
    _design_audit_source_contract!(source_raw, audit)
    _design_audit_candidate_budget_contract!(candidate_budget_raw, candidate_budget, audit)
    _design_audit_audit_policy_contract!(audit_policy_raw, audit)
    _design_audit_unknown_nested_keys!(
        audit,
        target,
        "/target",
        DESIGN_TARGET_KEYS,
        "unsupported_target_key",
    )
    if has_legacy
        _design_audit_unknown_nested_keys!(
            audit,
            legacy,
            "/target/legacy_target",
            DESIGN_LEGACY_TARGET_KEYS,
            "unsupported_legacy_target_key",
        )
    end
    _design_audit_unknown_nested_keys!(
        audit,
        constraints,
        "/constraints",
        DESIGN_CONSTRAINT_KEYS,
        "unsupported_constraint_key",
    )
    _design_normalize_parameter_bounds!(constraints, audit)
    if ambiguous_legacy_behavior
        push!(audit, DesignabilityAuditItem(
            path = "/target",
            kind = "target_contract",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "target.legacy_target and target.behavior_spec cannot be combined; legacy shorthand must not bypass an explicit structured behavior_spec target.",
        ))
    elseif has_legacy
        raw_kind = String(_raw_get(legacy, :target_kind, ""))
        raw_target = _raw_get(legacy, :target, nothing)
        if raw_kind == "exact"
            kind = "exact"
            normalized_exact = _design_normalize_legacy_exact_target(raw_target, audit)
            if normalized_exact !== nothing
                val = normalized_exact
                has_search_target = true
            end
        else
            kind, val = _design_normalize_design_target(raw_kind, raw_target)
            has_search_target = true
            if kind in ("sign", "label") &&
               _design_qualitative_legacy_feasible_region_requested(constraints_raw, candidate_budget)
                push!(audit, DesignabilityAuditItem(
                    path = "/target/legacy_target",
                    kind = "qualitative_legacy_target_feasible_region",
                    support_level = "unsupported",
                    hard = true,
                    stage = "compile",
                    solver = "none",
                    reason = "Qualitative legacy sign/label targets are atlas/minimal-certificate only; use an exact reaction-order or structured behavior_spec target for feasible-region recommendations.",
                ))
            end
        end
        has_search_target && push!(audit, DesignabilityAuditItem(
            path = "/target/legacy_target",
            kind = "reaction_order_program",
            support_level = "enforced_exact",
            stage = "atlas_match",
            solver = "design_index",
            reason = "Legacy reaction-order target lowers to the enumerated design index.",
        ))
    else
        isempty(target) && error("target must declare at least one behavior clause")
        if _raw_haskey(target, :behavior_spec)
            lowered = _design_lower_behavior_spec(_raw_get(target, :behavior_spec, nothing), audit)
            if lowered !== nothing
                kind, val, required_input, required_output = lowered
                has_search_target = true
                target_source_path = "/target/behavior_spec"
            end
        end
        has_search_target || push!(audit, DesignabilityAuditItem(
            path = "/target",
            kind = "target_lowering",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "No active backend compiler can lower the declared target to a searchable atlas behavior.",
        ))
    end

    if _raw_haskey(target, :behavior_spec)
        _design_audit_behavior_spec_side_clauses!(
            _raw_get(target, :behavior_spec, nothing),
            audit,
            _design_exact_ro_program(kind, val),
            constraints,
        )
    end

    if has_search_target && required_input !== nothing
        push!(audit, DesignabilityAuditItem(
            path = "/target/behavior_spec/input",
            kind = "input_symbol_filter",
            support_level = "enforced_exact",
            stage = "candidate_filter",
            solver = "design_index",
            reason = "Candidate slices are filtered to the requested input total symbol.",
        ))
    end
    if has_search_target && required_output !== nothing
        push!(audit, DesignabilityAuditItem(
            path = "/target/behavior_spec/output",
            kind = "output_symbol_filter",
            support_level = "enforced_exact",
            stage = "candidate_filter",
            solver = "design_index",
            reason = "Candidate slices are filtered to the requested output species symbol.",
        ))
    end

    if _raw_haskey(target, :input_window)
        win = _raw_get(target, :input_window, Dict{String, Any}())
        _design_audit_input_window_schema_contract!(audit, win, "/target/input_window")
        win_path = ambiguous_behavior_input_window ? "/target/input_window" :
                   (_raw_haskey(win, :input_log10) ? "/target/input_window/input_log10" : "/target/input_window")
        push!(audit, DesignabilityAuditItem(
            path = win_path,
            kind = "finite_input_window",
            support_level = "unsupported",
            hard = ambiguous_behavior_input_window ? true : _design_hard_clause(win),
            stage = "compile",
            solver = "none",
            reason = ambiguous_behavior_input_window ?
                     "target.input_window cannot shadow target.behavior_spec.input_window; exact-window solver consumes only behavior_spec.input_window." :
                     "Finite input-window realization is not yet enforced by the active exact feasible-region solver.",
        ))
    end
    if _raw_haskey(target, :output_feature)
        feature = _raw_get(target, :output_feature, Dict{String, Any}())
        _design_audit_hard_field_contract!(audit, feature, "/target/output_feature")
        unsupported_feature_keys = _design_audit_unknown_nested_keys!(
            audit,
            feature,
            "/target/output_feature",
            DESIGN_OUTPUT_FEATURE_KEYS,
            "unsupported_output_feature_key",
        )
        supported = !unsupported_feature_keys &&
            has_search_target &&
            target_source_path == "/target/behavior_spec" &&
            _design_target_has_behavior_input_window(target) &&
            _design_output_feature_supported(feature)
        if !unsupported_feature_keys
            push!(audit, DesignabilityAuditItem(
                path = _design_output_feature_audit_path(target, target_source_path, feature),
                kind = "output_feature",
                support_level = supported ? "sampled_forward" : "unsupported",
                hard = supported ? _design_hard_clause(feature) : true,
                stage = supported ? "forward_verification" : "compile",
                solver = supported ? "sampled-window-dose-response" : "none",
                reason = supported ?
                    "Output feature is certified by scanning the exact-window theta across behavior_spec.input_window and rejecting invalid or floor-limited curves." :
                    _design_output_feature_unsupported_reason(target, feature),
            ))
        end
    end
    if _raw_haskey(target, :temporal_dynamics)
        temporal_dynamics = _raw_get(target, :temporal_dynamics, Dict{String, Any}())
        _design_audit_hard_field_contract!(audit, temporal_dynamics, "/target/temporal_dynamics")
        unsupported_temporal_keys = _design_audit_unknown_nested_keys!(
            audit,
            temporal_dynamics,
            "/target/temporal_dynamics",
            DESIGN_TEMPORAL_DYNAMICS_KEYS,
            "unsupported_temporal_dynamics_key",
        )
        invalid_temporal_contract = _design_audit_temporal_dynamics_contract!(
            audit,
            temporal_dynamics,
        )
        if !unsupported_temporal_keys && !invalid_temporal_contract
            push!(audit, DesignabilityAuditItem(
                path = _design_temporal_dynamics_audit_path(temporal_dynamics),
                kind = "temporal_dynamics",
                support_level = "unsupported",
                hard = _design_hard_clause(temporal_dynamics),
                stage = "compile",
                solver = "none",
                reason = "The active equilibrium binding backend has no temporal dynamics solver.",
            ))
        end
    end
    if _raw_haskey(target, :shape)
        shape = _raw_get(target, :shape, Dict{String, Any}())
        _design_audit_hard_field_contract!(audit, shape, "/target/shape")
        unsupported_shape_keys = _design_audit_unknown_nested_keys!(
            audit,
            shape,
            "/target/shape",
            DESIGN_SHAPE_KEYS,
            "unsupported_shape_key",
        )
        supported = !unsupported_shape_keys &&
            has_search_target &&
            target_source_path == "/target/behavior_spec" &&
            _design_target_has_behavior_input_window(target) &&
            _designability_shape_supported(shape)
        if !unsupported_shape_keys
            push!(audit, DesignabilityAuditItem(
                path = _design_shape_audit_path(target, target_source_path, shape),
                kind = "finite_dose_shape",
                support_level = supported ? "sampled_forward" : "unsupported",
                hard = supported ? _design_hard_clause(shape) : true,
                stage = supported ? "forward_verification" : "compile",
                solver = supported ? "sampled-window-dose-response" : "none",
                reason = supported ?
                    "Finite-dose shape is certified by scanning the exact-window theta across behavior_spec.input_window and rejecting invalid or floor-limited curves." :
                    _design_shape_unsupported_reason(target, target_source_path, shape),
            ))
        end
    end
    exact_program = has_search_target ? _design_exact_ro_program(kind, val) : nothing
    single_ro = has_search_target ? _design_exact_single_ro(kind, val) : nothing
    if has_search_target && single_ro !== nothing
        push!(audit, DesignabilityAuditItem(
            path = target_source_path,
            kind = "reaction_order_single_slope",
            support_level = "enforced_exact",
            stage = "feasible_region",
            solver = "exact-union-siso-rop",
            reason = "Single exact RO targets can be represented as a union of SISO regime cells intersected with qK bounds.",
        ))
    elseif has_search_target && exact_program !== nothing
        push!(audit, DesignabilityAuditItem(
            path = target_source_path == "/target/behavior_spec" ? "/target/behavior_spec/program" : target_source_path,
            kind = "reaction_order_program_feasible_region",
            support_level = "enforced_exact",
            hard = true,
            stage = "feasible_region",
            solver = "exact-union-siso-rop-path",
            reason = "Multi-step reaction-order programs are certified by nonempty SISO path polyhedra intersected with qK bounds.",
        ))
    end
    _design_audit_designability_constraints!(constraints, audit, target, target_source_path, exact_program)
    _design_audit_ranking_policy!(ranking_policy_raw, ranking_policy, audit, target, target_source_path, exact_program, constraints)

    return NormalizedDesignabilitySpec(
        source = source,
        target = target,
        constraints = constraints,
        candidate_budget = candidate_budget,
        ranking_policy = ranking_policy,
        audit_policy = audit_policy,
        has_legacy_target = has_legacy,
        has_search_target = has_search_target,
        legacy_target_kind = kind,
        legacy_target = val,
        target_source_path = target_source_path,
        required_input = required_input,
        required_output = required_output,
        audit = audit,
    )
end

function _design_hard_clause(raw; default::Bool = true)
    raw isa AbstractDict || return default
    _raw_haskey(raw, :hard) || return default
    hard = _raw_get(raw, :hard, default)
    hard isa Bool && return hard
    return true
end

function _design_temporal_dynamics_audit_path(raw)
    raw isa AbstractDict || return "/target/temporal_dynamics"
    for key in (:peak_width_seconds, :stimulus, :trace)
        _raw_haskey(raw, key) &&
            return "/target/temporal_dynamics/$(_design_json_pointer_token(String(key)))"
    end
    return "/target/temporal_dynamics"
end

function _design_push_temporal_contract_audit!(audit::Vector{DesignabilityAuditItem},
                                               path::AbstractString,
                                               kind::AbstractString,
                                               reason::AbstractString)
    push!(audit, DesignabilityAuditItem(
        path = String(path),
        kind = String(kind),
        support_level = "unsupported",
        hard = true,
        stage = "compile",
        solver = "none",
        reason = String(reason),
    ))
    return true
end

function _design_audit_temporal_dynamics_contract!(audit::Vector{DesignabilityAuditItem}, temporal)
    if !(temporal isa AbstractDict)
        return _design_push_temporal_contract_audit!(
            audit,
            "/target/temporal_dynamics",
            "temporal_dynamics_contract",
            "target.temporal_dynamics must be an object before unsupported temporal requirements can be audited.",
        )
    end

    invalid = false
    if _raw_haskey(temporal, :stimulus)
        stimulus = _raw_get(temporal, :stimulus, nothing)
        if !(stimulus isa AbstractDict)
            invalid |= _design_push_temporal_contract_audit!(
                audit,
                "/target/temporal_dynamics/stimulus",
                "temporal_dynamics_stimulus",
                "temporal_dynamics.stimulus must be an object to match the DesignabilitySpec schema.",
            )
        end
    end
    if _raw_haskey(temporal, :trace)
        trace = _raw_get(temporal, :trace, nothing)
        if !(trace isa AbstractVector || trace isa Tuple)
            invalid |= _design_push_temporal_contract_audit!(
                audit,
                "/target/temporal_dynamics/trace",
                "temporal_dynamics_trace",
                "temporal_dynamics.trace must be an array to match the DesignabilitySpec schema.",
            )
        end
    end
    if _raw_haskey(temporal, :peak_width_seconds)
        peak = _raw_get(temporal, :peak_width_seconds, nothing)
        peak_path = "/target/temporal_dynamics/peak_width_seconds"
        if !(peak isa AbstractDict)
            invalid |= _design_push_temporal_contract_audit!(
                audit,
                peak_path,
                "temporal_dynamics_peak_width_seconds",
                "temporal_dynamics.peak_width_seconds must be an object with finite nonnegative min and/or max seconds.",
            )
        else
            invalid |= _design_audit_unknown_nested_keys!(
                audit,
                peak,
                peak_path,
                DESIGN_TEMPORAL_PEAK_WIDTH_KEYS,
                "unsupported_temporal_peak_width_seconds_key",
            )
            has_min = _raw_haskey(peak, :min)
            has_max = _raw_haskey(peak, :max)
            if !has_min && !has_max
                invalid |= _design_push_temporal_contract_audit!(
                    audit,
                    peak_path,
                    "temporal_dynamics_peak_width_seconds",
                    "temporal_dynamics.peak_width_seconds requires finite nonnegative min and/or max seconds.",
                )
            end
            min_value = nothing
            max_value = nothing
            for key in (:min, :max)
                _raw_haskey(peak, key) || continue
                value = _design_nonnegative_finite_real(_raw_get(peak, key, nothing))
                if value === nothing
                    invalid |= _design_push_temporal_contract_audit!(
                        audit,
                        "$peak_path/$(String(key))",
                        "temporal_dynamics_peak_width_seconds",
                        "temporal_dynamics.peak_width_seconds.$(String(key)) must be a finite nonnegative non-Bool real value.",
                    )
                elseif key == :min
                    min_value = value
                else
                    max_value = value
                end
            end
            if min_value !== nothing && max_value !== nothing && min_value > max_value
                invalid |= _design_push_temporal_contract_audit!(
                    audit,
                    peak_path,
                    "temporal_dynamics_peak_width_seconds",
                    "temporal_dynamics.peak_width_seconds min must be <= max.",
                )
            end
        end
    end
    return invalid
end

function _design_json_pointer_token(raw_key)
    token = String(raw_key)
    return replace(replace(token, "~" => "~0"), "/" => "~1")
end

function _design_audit_unknown_nested_keys!(audit::Vector{DesignabilityAuditItem},
                                            raw_clause,
                                            base_path::AbstractString,
                                            allowed_keys,
                                            kind::AbstractString)
    raw_clause isa AbstractDict || return false
    found = false
    for key in keys(raw_clause)
        key_s = String(key)
        key_s in allowed_keys && continue
        found = true
        push!(audit, DesignabilityAuditItem(
            path = "$(base_path)/$(_design_json_pointer_token(key_s))",
            kind = String(kind),
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "The active compiler/verifier does not support nested key $(base_path)/$(_design_json_pointer_token(key_s)).",
        ))
    end
    return found
end

function _design_push_contract_audit!(audit::Vector{DesignabilityAuditItem},
                                      path::AbstractString,
                                      kind::AbstractString,
                                      reason::AbstractString;
                                      hard::Bool = true,
                                      stage::AbstractString = "compile")
    push!(audit, DesignabilityAuditItem(
        path = String(path),
        kind = String(kind),
        support_level = "unsupported",
        hard = hard,
        stage = String(stage),
        solver = "none",
        reason = String(reason),
    ))
    return audit
end

function _design_audit_hard_field_contract!(audit::Vector{DesignabilityAuditItem},
                                            raw_clause,
                                            base_path::AbstractString)
    raw_clause isa AbstractDict || return false
    _raw_haskey(raw_clause, :hard) || return false
    hard = _raw_get(raw_clause, :hard, nothing)
    hard isa Bool && return false
    _design_push_contract_audit!(
        audit,
        "$(base_path)/hard",
        "hard_clause_contract",
        "$(base_path)/hard must be boolean when present.",
    )
    return true
end

function _design_audit_input_window_schema_contract!(audit::Vector{DesignabilityAuditItem},
                                                     input_window,
                                                     base_path::AbstractString)
    if !(input_window isa AbstractDict)
        _design_push_contract_audit!(
            audit,
            base_path,
            "input_window_contract",
            "$(base_path) must be an object when present.",
        )
        return true
    end
    invalid = false
    invalid |= _design_audit_unknown_nested_keys!(
        audit,
        input_window,
        base_path,
        DESIGN_INPUT_WINDOW_KEYS,
        "unsupported_input_window_key",
    )
    invalid |= _design_audit_hard_field_contract!(audit, input_window, base_path)
    if _raw_haskey(input_window, :input_log10) &&
       _design_input_window_log10_bounds(input_window) === nothing
        invalid = true
        _design_push_contract_audit!(
            audit,
            "$(base_path)/input_log10",
            "input_window_contract",
            "$(base_path)/input_log10 must be two finite non-Bool numeric bounds with lower <= upper.",
        )
    end
    if _raw_haskey(input_window, :min_spacing_decades)
        if _design_input_window_spacing_value(input_window) === nothing
            invalid = true
            _design_push_contract_audit!(
                audit,
                "$(base_path)/min_spacing_decades",
                "input_window_contract",
                "$(base_path)/min_spacing_decades must be a finite nonnegative non-Bool real value.",
            )
        elseif !_raw_haskey(input_window, :input_log10)
            invalid = true
            _design_push_contract_audit!(
                audit,
                "$(base_path)/min_spacing_decades",
                "input_window_contract",
                "$(base_path)/min_spacing_decades requires $(base_path)/input_log10 before it can constrain finite-window witnesses.",
            )
        end
    end
    if _raw_haskey(input_window, :operating_points_log10)
        raw_points = _raw_get(input_window, :operating_points_log10, nothing)
        malformed_points = !(raw_points isa AbstractVector || raw_points isa Tuple)
        if !malformed_points
            for value in collect(raw_points)
                if value isa Bool || !(value isa Real) || !isfinite(Float64(value))
                    malformed_points = true
                    break
                end
            end
        end
        if malformed_points
            invalid = true
            _design_push_contract_audit!(
                audit,
                "$(base_path)/operating_points_log10",
                "input_window_contract",
                "$(base_path)/operating_points_log10 must be an array of finite non-Bool real values.",
            )
        elseif _design_input_window_log10_bounds(input_window) === nothing
            invalid = true
            _design_push_contract_audit!(
                audit,
                "$(base_path)/operating_points_log10",
                "input_window_contract",
                "$(base_path)/operating_points_log10 requires finite $(base_path)/input_log10 bounds.",
            )
        end
    end
    return invalid
end

function _design_audit_source_contract!(source_raw, audit::Vector{DesignabilityAuditItem})
    if source_raw === nothing
        _design_push_contract_audit!(
            audit,
            "/source",
            "source_contract",
            "DesignabilitySpec.source with a valid source.kind is required so manual config and Agent Design share one explicit contract.",
        )
        return audit
    end
    if !(source_raw isa AbstractDict)
        _design_push_contract_audit!(
            audit,
            "/source",
            "source_contract",
            "DesignabilitySpec.source must be an object with a supported kind.",
        )
        return audit
    end
    _design_audit_unknown_nested_keys!(
        audit,
        source_raw,
        "/source",
        DESIGN_SOURCE_KEYS,
        "unsupported_source_key",
    )
    if !_raw_haskey(source_raw, :kind)
        _design_push_contract_audit!(
            audit,
            "/source/kind",
            "source_contract",
            "DesignabilitySpec.source.kind is required.",
        )
    else
        kind = _raw_get(source_raw, :kind, nothing)
        if !(kind isa AbstractString && String(kind) in DESIGN_SOURCE_KIND_VALUES)
            _design_push_contract_audit!(
                audit,
                "/source/kind",
                "source_contract",
                "DesignabilitySpec.source.kind must be one of $(join(DESIGN_SOURCE_KIND_VALUES, ", ")).",
            )
        end
    end
    for key in (:node_id, :agent_message_id)
        if _raw_haskey(source_raw, key)
            value = _raw_get(source_raw, key, nothing)
            value isa AbstractString || _design_push_contract_audit!(
                audit,
                "/source/$(String(key))",
                "source_contract",
                "source.$(String(key)) must be a string when present.",
            )
        end
    end
    if _raw_haskey(source_raw, :provenance)
        provenance = _raw_get(source_raw, :provenance, nothing)
        provenance isa AbstractDict || _design_push_contract_audit!(
            audit,
            "/source/provenance",
            "source_contract",
            "source.provenance must be an object when present.",
        )
    end
    return audit
end

function _design_audit_nonnegative_int_field!(raw_clause, normalized::Dict{String, Any},
                                              audit::Vector{DesignabilityAuditItem},
                                              key::Symbol,
                                              base_path::AbstractString,
                                              kind::AbstractString;
                                              maximum::Union{Nothing, Int}=nothing)
    _raw_haskey(raw_clause, key) || return audit
    value = _design_int_bound(_raw_get(raw_clause, key, nothing), 0)
    if value === nothing
        _design_push_contract_audit!(
            audit,
            "$(base_path)/$(String(key))",
            kind,
            "$(base_path)/$(String(key)) must be an integer >= 0 when present.",
        )
        _design_delete_raw_key!(normalized, String(key))
        return audit
    end

    # JSON Schema treats an integral JSON number such as `64.0` as an
    # integer. Canonicalize it here so the compiler and the Python-facing
    # contract make the same decision.
    normalized[String(key)] = value
    if maximum !== nothing && value > maximum
        _design_push_contract_audit!(
            audit,
            "$(base_path)/$(String(key))",
            kind,
            "$(base_path)/$(String(key)) must be <= $(maximum) for synchronous evaluation.",
        )
    end
    return audit
end

function _design_audit_candidate_budget_contract!(candidate_budget_raw,
                                                  candidate_budget::Dict{String, Any},
                                                  audit::Vector{DesignabilityAuditItem})
    candidate_budget_raw === nothing && return audit
    if !(candidate_budget_raw isa AbstractDict)
        _design_push_contract_audit!(
            audit,
            "/candidate_budget",
            "candidate_budget_contract",
            "candidate_budget must be an object when present.",
        )
        return audit
    end
    _design_audit_unknown_nested_keys!(
        audit,
        candidate_budget_raw,
        "/candidate_budget",
        DESIGN_CANDIDATE_BUDGET_KEYS,
        "unsupported_candidate_budget_key",
    )
    if _raw_haskey(candidate_budget_raw, :mode)
        mode = _raw_get(candidate_budget_raw, :mode, nothing)
        if !(mode isa AbstractString && String(mode) in ("near_minimal", "all_matches"))
            _design_push_contract_audit!(
                audit,
                "/candidate_budget/mode",
                "candidate_budget_contract",
                "candidate_budget.mode must be near_minimal or all_matches.",
            )
            candidate_budget["mode"] = "near_minimal"
        end
    end
    bounded_maxima = Dict{Symbol, Int}(
        :max_screened => MAX_SYNC_DESIGN_CARDS,
        :max_verified_recommendations => MAX_SYNC_DESIGN_CARDS,
        :max_recommended => MAX_SYNC_DESIGN_CARDS,
        :max_near_misses => MAX_SYNC_DESIGN_CARDS,
        :max_exact_placements => MAX_SYNC_EXACT_PLACEMENTS,
    )
    for key in (
        :max_extra_species,
        :max_extra_reactions,
        :max_extra_mu,
        :max_screened,
        :max_verified_recommendations,
        :max_recommended,
        :max_near_misses,
        :max_exact_placements,
    )
        _design_audit_nonnegative_int_field!(
            candidate_budget_raw,
            candidate_budget,
            audit,
            key,
            "/candidate_budget",
            "candidate_budget_contract",
            maximum = get(bounded_maxima, key, nothing),
        )
    end
    return audit
end

function _design_audit_audit_policy_contract!(audit_policy_raw,
                                              audit::Vector{DesignabilityAuditItem})
    audit_policy_raw === nothing && return audit
    if !(audit_policy_raw isa AbstractDict)
        _design_push_contract_audit!(
            audit,
            "/audit_policy",
            "audit_policy_contract",
            "audit_policy must be an object when present.",
        )
        return audit
    end
    _design_audit_unknown_nested_keys!(
        audit,
        audit_policy_raw,
        "/audit_policy",
        DESIGN_AUDIT_POLICY_KEYS,
        "unsupported_audit_policy_key",
    )
    if _raw_haskey(audit_policy_raw, :unsupported)
        unsupported = _raw_get(audit_policy_raw, :unsupported, nothing)
        unsupported isa AbstractString && String(unsupported) == "block_if_hard" ||
            _design_push_contract_audit!(
                audit,
                "/audit_policy/unsupported",
                "audit_policy_contract",
                "audit_policy.unsupported must be block_if_hard; warn and ignore are not implemented by the verifier.",
            )
    end
    if _raw_haskey(audit_policy_raw, :path_format)
        path_format = _raw_get(audit_policy_raw, :path_format, nothing)
        path_format isa AbstractString && String(path_format) == "json_pointer" ||
            _design_push_contract_audit!(
                audit,
                "/audit_policy/path_format",
                "audit_policy_contract",
                "audit_policy.path_format must be json_pointer.",
            )
    end
    if _raw_haskey(audit_policy_raw, :include_supported)
        include_supported = _raw_get(audit_policy_raw, :include_supported, nothing)
        include_supported isa Bool ||
            _design_push_contract_audit!(
                audit,
                "/audit_policy/include_supported",
                "audit_policy_contract",
                "audit_policy.include_supported must be boolean.",
            )
    end
    return audit
end

function _design_has_only_nested_keys(raw_clause, allowed_keys)
    raw_clause isa AbstractDict || return false
    return all(key -> String(key) in allowed_keys, keys(raw_clause))
end

function _design_ranking_preference_support(pref::AbstractString, target::AbstractDict,
                                            target_source_path::AbstractString,
                                            exact_program, constraints::AbstractDict)
    pref in ("evidence_grade", "certificate_grade") && return (
        support_level = "enforced_exact",
        solver = "verified-card-certificate",
        reason = "Ranking uses the verified card certificate/evidence label produced by exact or sampled proof construction.",
    )
    pref in ("chebyshev_radius", "tunable_volume") && return (
        support_level = "enforced_exact",
        solver = "exact-union-siso-rop",
        reason = "Ranking uses geometry derived from exact feasible-region cells, with tunable_volume labeled as a Chebyshev-ball lower bound.",
    )
    if pref == "transition_spacing"
        transitions = _raw_get(constraints, :transitions, nothing)
        _design_has_only_nested_keys(transitions, DESIGN_TRANSITION_KEYS) &&
            _design_transition_spacing_supported(target, target_source_path, exact_program, transitions) && return (
            support_level = "enforced_exact",
            solver = "exact-window-siso-rop-path",
            reason = "Ranking uses transition spacing from exact finite-window witness spacing evidence produced by this spec.",
        )
        return (
            support_level = "unsupported",
            solver = "none",
            reason = "transition_spacing ranking requires a supported constraints.transitions.min_spacing_decades clause that produces verified-card witness spacing.",
        )
    end
    if pref == "dynamic_range"
        dynamic_range = _raw_get(constraints, :dynamic_range, nothing)
        _design_has_only_nested_keys(dynamic_range, DESIGN_DYNAMIC_RANGE_KEYS) &&
            _design_dynamic_range_supported(target, dynamic_range) && return (
            support_level = "sampled_forward",
            solver = "sampled-window-dose-response",
            reason = "Ranking uses dynamic range from downstream sampled finite-dose evidence produced by this spec.",
        )
        return (
            support_level = "unsupported",
            solver = "none",
            reason = "dynamic_range ranking requires a supported constraints.dynamic_range clause that produces verified-card sampled dynamic-range evidence.",
        )
    end
    pref == "complexity" && return (
        support_level = "enforced_exact",
        solver = "design_index",
        reason = "Ranking uses explicit candidate complexity from the design index.",
    )
    pref in ("condition_number", "sampled_robustness") && return (
        support_level = "unsupported",
        solver = "none",
        reason = "No solver-backed verified-card metric is currently produced for this ranking preference, so it is skipped during ordering.",
    )
    return (
        support_level = "unsupported",
        solver = "none",
        reason = "Unknown ranking preference; it is skipped during verified recommendation ordering.",
    )
end

function _design_audit_ranking_policy!(ranking_policy_raw, ranking_policy, audit::Vector{DesignabilityAuditItem},
                                       target::AbstractDict, target_source_path::AbstractString,
                                       exact_program, constraints::AbstractDict)
    ranking_policy_raw === nothing && return audit
    if !(ranking_policy_raw isa AbstractDict)
        push!(audit, DesignabilityAuditItem(
            path = "/ranking_policy",
            kind = "ranking_policy_contract",
            support_level = "unsupported",
            hard = true,
            stage = "ranking",
            solver = "none",
            reason = "ranking_policy must be an object when present.",
        ))
        return audit
    end
    for key in keys(ranking_policy)
        key_s = String(key)
        key_s in ("verified_only", "prefer") && continue
        push!(audit, DesignabilityAuditItem(
            path = "/ranking_policy/$(_design_json_pointer_token(key_s))",
            kind = "unsupported_ranking_policy_key",
            support_level = "unsupported",
            hard = true,
            stage = "ranking",
            solver = "none",
            reason = "Unknown ranking_policy keys are not part of the DesignabilitySpec schema.",
        ))
    end
    if _raw_haskey(ranking_policy, :verified_only)
        verified_only = _raw_get(ranking_policy, :verified_only, nothing)
        if verified_only !== true
            push!(audit, DesignabilityAuditItem(
                path = "/ranking_policy/verified_only",
                kind = "ranking_policy_contract",
                support_level = "unsupported",
                hard = true,
                stage = "ranking",
                solver = "none",
                reason = "ranking_policy.verified_only must be literal true; non-verified/exploratory recommendations are not implemented, and verified recommendations remain the only supported recommendation mode.",
            ))
        end
    end
    _raw_haskey(ranking_policy, :prefer) || return audit
    prefer = _raw_get(ranking_policy, :prefer, Any[])
    (prefer isa AbstractVector || prefer isa Tuple) || begin
        push!(audit, DesignabilityAuditItem(
            path = "/ranking_policy/prefer",
            kind = "ranking_policy_contract",
            support_level = "unsupported",
            hard = true,
            stage = "ranking",
            solver = "none",
            reason = "ranking_policy.prefer must be an array of DesignabilitySpec ranking preference strings.",
        ))
        return audit
    end
    for (idx, raw) in enumerate(collect(prefer))
        if !(raw isa AbstractString)
            push!(audit, DesignabilityAuditItem(
                path = "/ranking_policy/prefer/$(idx - 1)",
                kind = "ranking_policy_contract",
                support_level = "unsupported",
                hard = true,
                stage = "ranking",
                solver = "none",
                reason = "ranking_policy.prefer entries must be strings.",
            ))
            continue
        end
        pref = String(raw)
        if !(pref in DESIGN_RANKING_PREFERENCE_KEYS)
            push!(audit, DesignabilityAuditItem(
                path = "/ranking_policy/prefer/$(idx - 1)",
                kind = "ranking_policy_contract",
                support_level = "unsupported",
                hard = true,
                stage = "ranking",
                solver = "none",
                reason = "Ranking preference is not part of the DesignabilitySpec ranking policy schema.",
            ))
            continue
        end
        support = _design_ranking_preference_support(pref, target, target_source_path, exact_program, constraints)
        push!(audit, DesignabilityAuditItem(
            path = "/ranking_policy/prefer/$(idx - 1)",
            kind = "ranking_preference",
            support_level = support.support_level,
            hard = false,
            stage = "ranking",
            solver = support.solver,
            reason = support.reason,
        ))
    end
    return audit
end

function _design_parameter_bounds_audit!(audit::Vector{DesignabilityAuditItem}, path::AbstractString;
                                         support_level::AbstractString,
                                         kind::AbstractString = "qk_box_bounds",
                                         hard::Bool = true,
                                         stage::AbstractString = "compile",
                                         solver::AbstractString = "none",
                                         reason::AbstractString = "")
    push!(audit, DesignabilityAuditItem(
        path = String(path),
        kind = String(kind),
        support_level = String(support_level),
        hard = hard,
        stage = String(stage),
        solver = String(solver),
        reason = String(reason),
    ))
end

function _design_parameter_bound_pair(raw)
    (raw isa AbstractVector || raw isa Tuple) || return nothing
    values = collect(raw)
    length(values) == 2 || return nothing
    out = Float64[]
    for value in values
        value isa Bool && return nothing
        value isa Real || return nothing
        f = Float64(value)
        isfinite(f) || return nothing
        push!(out, f)
    end
    out[1] <= out[2] || return nothing
    return out
end

function _design_validate_parameter_bound!(by_class::Dict{String, Any},
                                           audit::Vector{DesignabilityAuditItem},
                                           raw_bounds::AbstractDict,
                                           key::Symbol,
                                           path::AbstractString)
    _raw_haskey(raw_bounds, key) || return true
    pair = _design_parameter_bound_pair(_raw_get(raw_bounds, key, nothing))
    if pair === nothing
        _design_parameter_bounds_audit!(
            audit,
            path;
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "parameter_bounds entries must be exactly two finite non-Bool real values with lower <= upper.",
        )
        return false
    end
    by_class[String(key)] = pair
    return true
end

function _design_normalize_parameter_bounds!(constraints::Dict{String, Any},
                                              audit::Vector{DesignabilityAuditItem})
    if !_raw_haskey(constraints, :parameter_bounds)
        _design_parameter_bounds_audit!(
            audit,
            "/constraints/parameter_bounds";
            support_level = "unsupported",
            kind = "qk_box_bounds_required",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "Verified recommendations require explicit constraints.parameter_bounds; no default qK domain is assumed.",
        )
        return true
    end
    raw_bounds = _raw_get(constraints, :parameter_bounds, nothing)
    if !(raw_bounds isa AbstractDict)
        _design_parameter_bounds_audit!(
            audit,
            "/constraints/parameter_bounds";
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "constraints.parameter_bounds must be an object.",
        )
        return false
    end

    valid = true
    allowed_top_keys = ("basis", "kd_log10", "total_log10", "by_class")
    for key in keys(raw_bounds)
        key_s = String(key)
        if !(key_s in allowed_top_keys)
            _design_parameter_bounds_audit!(
                audit,
                "/constraints/parameter_bounds/$(_design_json_pointer_token(key_s))";
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "Unsupported parameter_bounds key `$key_s`; supported keys are basis, kd_log10, total_log10, and by_class.",
            )
            valid = false
        end
    end

    basis = _raw_get(raw_bounds, :basis, nothing)
    if basis !== nothing && !(basis isa AbstractString && String(basis) == "log10_qK")
        _design_parameter_bounds_audit!(
            audit,
            "/constraints/parameter_bounds/basis";
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "parameter_bounds.basis must be absent or `log10_qK`.",
        )
        valid = false
    end

    by_class = Dict{String, Any}()
    raw_by_class = _raw_get(raw_bounds, :by_class, nothing)
    if raw_by_class !== nothing
        if raw_by_class isa AbstractDict
            allowed_class_keys = ("kd", "total")
            for key in keys(raw_by_class)
                key_s = String(key)
                if !(key_s in allowed_class_keys)
                    _design_parameter_bounds_audit!(
                        audit,
                        "/constraints/parameter_bounds/by_class/$(_design_json_pointer_token(key_s))";
                        support_level = "unsupported",
                        hard = true,
                        stage = "compile",
                        solver = "none",
                        reason = "Unsupported parameter_bounds.by_class key `$key_s`; the active solver supports kd and total class bounds.",
                    )
                    valid = false
                end
            end
            for key in (:kd, :total)
                if _raw_haskey(raw_by_class, key)
                    ok = _design_validate_parameter_bound!(
                        by_class,
                        audit,
                        raw_by_class,
                        key,
                        "/constraints/parameter_bounds/by_class/$(String(key))",
                    )
                    valid = valid && ok
                end
            end
        else
            _design_parameter_bounds_audit!(
                audit,
                "/constraints/parameter_bounds/by_class";
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "parameter_bounds.by_class must be an object.",
            )
            valid = false
        end
    end

    duplicate_aliases = Set{Symbol}()
    if raw_by_class isa AbstractDict
        for (alias, class_key) in ((:kd_log10, :kd), (:total_log10, :total))
            if _raw_haskey(raw_bounds, alias) && _raw_haskey(raw_by_class, class_key)
                _design_parameter_bounds_audit!(
                    audit,
                    "/constraints/parameter_bounds/$(String(alias))";
                    support_level = "unsupported",
                    hard = true,
                    stage = "compile",
                    solver = "none",
                    reason = "Do not specify both parameter_bounds.$(String(alias)) and parameter_bounds.by_class.$(String(class_key)); declare each qK parameter class once.",
                )
                push!(duplicate_aliases, alias)
                valid = false
            end
        end
    end

    for (alias, class_key) in ((:kd_log10, "kd"), (:total_log10, "total"))
        _raw_haskey(raw_bounds, alias) || continue
        alias in duplicate_aliases && continue
        pair = _design_parameter_bound_pair(_raw_get(raw_bounds, alias, nothing))
        if pair === nothing
            _design_parameter_bounds_audit!(
                audit,
                "/constraints/parameter_bounds/$(String(alias))";
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "parameter_bounds aliases must be exactly two finite non-Bool real values with lower <= upper.",
            )
            valid = false
        else
            by_class[class_key] = pair
        end
    end

    valid || return false
    if isempty(by_class)
        _design_parameter_bounds_audit!(
            audit,
            "/constraints/parameter_bounds";
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "constraints.parameter_bounds must declare at least one finite kd or total class bound; no default qK domain is assumed.",
        )
        return false
    end
    constraints["parameter_bounds"] = Dict{String, Any}(
        "basis" => "log10_qK",
        "by_class" => by_class,
    )

    _design_parameter_bounds_audit!(
        audit,
        "/constraints/parameter_bounds";
        support_level = "enforced_exact",
        hard = true,
        stage = "feasible_region",
        solver = "exact-union-siso-rop",
        reason = "Declared qK class bounds are intersected with each exact feasible-region cell.",
    )
    for key in sort!(collect(keys(by_class)))
        _design_parameter_bounds_audit!(
            audit,
            "/constraints/parameter_bounds/by_class/$key";
            support_level = "enforced_exact",
            hard = true,
            stage = "feasible_region",
            solver = "exact-union-siso-rop",
            reason = "Declared $key log10 qK bounds are intersected with each exact feasible-region cell.",
        )
    end
    return true
end

function _design_finite_non_bool_real(raw)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    value = Float64(raw)
    isfinite(value) || return nothing
    return value
end

function _design_nonnegative_finite_real(raw)
    value = _design_finite_non_bool_real(raw)
    value === nothing && return nothing
    value >= 0.0 || return nothing
    return value
end

function _design_int_bound(raw, minimum::Integer)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    isfinite(raw) || return nothing
    isinteger(raw) || return nothing
    value = try
        Int(raw)
    catch
        return nothing
    end
    value >= minimum || return nothing
    return value
end

function _design_exact_ro_program(kind, val)
    String(kind) == "exact" || return nothing
    (val isa AbstractVector || val isa Tuple) || return nothing
    vals = Float64[]
    for raw in collect(val)
        value = _design_finite_non_bool_real(raw)
        value === nothing && return nothing
        push!(vals, value)
    end
    isempty(vals) && return nothing
    return vals
end

function _design_lower_behavior_spec(raw, audit::Vector{DesignabilityAuditItem})
    if !(raw isa AbstractDict)
        _design_push_contract_audit!(
            audit,
            "/target/behavior_spec",
            "behavior_spec_contract",
            "target.behavior_spec must be an object before exact lowering.",
        )
        return nothing
    end
    behavior = Dict{String, Any}(_materialize(raw))
    unsupported_top_level = _design_audit_unknown_nested_keys!(
        audit,
        behavior,
        "/target/behavior_spec",
        DESIGN_BEHAVIOR_SPEC_KEYS,
        "unsupported_behavior_spec_key",
    )
    unsupported_top_level && return nothing

    invalid_core = false
    input_raw = _raw_get(behavior, :input, nothing)
    output_raw = _raw_get(behavior, :output, nothing)
    input = input_raw isa AbstractString ? strip(String(input_raw)) : ""
    output = output_raw isa AbstractString ? strip(String(output_raw)) : ""
    if isempty(input)
        invalid_core = true
        _design_push_contract_audit!(
            audit,
            "/target/behavior_spec/input",
            "behavior_spec_contract",
            "target.behavior_spec.input must be a non-blank string before exact lowering.",
        )
    end
    if isempty(output)
        invalid_core = true
        _design_push_contract_audit!(
            audit,
            "/target/behavior_spec/output",
            "behavior_spec_contract",
            "target.behavior_spec.output must be a non-blank string before exact lowering.",
        )
    end

    feature_space_raw = _raw_get(behavior, :feature_space, nothing)
    feature_space_valid = feature_space_raw === nothing ||
                          (feature_space_raw isa AbstractString &&
                           String(feature_space_raw) == "reaction_order")
    if !feature_space_valid
        invalid_core = true
        push!(audit, DesignabilityAuditItem(
            path = "/target/behavior_spec/feature_space",
            kind = "behavior_feature_space",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "Only reaction_order behavior programs currently lower to the exact atlas solver.",
        ))
    end

    program_raw = _raw_get(behavior, :program, nothing)
    program = Any[]
    if !(program_raw isa AbstractVector || program_raw isa Tuple)
        invalid_core = true
        _design_push_contract_audit!(
            audit,
            "/target/behavior_spec/program",
            "behavior_spec_contract",
            "target.behavior_spec.program must be a non-empty array before exact lowering.",
        )
    else
        program = collect(program_raw)
        if isempty(program)
            invalid_core = true
            _design_push_contract_audit!(
                audit,
                "/target/behavior_spec/program",
                "behavior_spec_contract",
                "target.behavior_spec.program must not be empty before exact lowering.",
            )
        end
    end
    for (idx, step_raw) in enumerate(program)
        if !(step_raw isa AbstractDict)
            invalid_core = true
            _design_push_contract_audit!(
                audit,
                "/target/behavior_spec/program/$(idx - 1)",
                "behavior_step_contract",
                "target.behavior_spec.program[$(idx - 1)] must be an object before exact lowering.",
            )
        end
    end
    invalid_core && return nothing

    values = Float64[]
    unsupported = false
    for (idx, step_raw) in enumerate(program)
        step = Dict{String, Any}(_materialize(step_raw))
        path = "/target/behavior_spec/program/$(idx - 1)"
        if _design_audit_unknown_nested_keys!(
            audit,
            step,
            path,
            DESIGN_BEHAVIOR_STEP_KEYS,
            "unsupported_behavior_step_key",
        )
            unsupported = true
            continue
        end
        if _design_audit_hard_field_contract!(audit, step, path)
            unsupported = true
            continue
        end
        kind_raw = _raw_get(step, :kind, "")
        op_raw = _raw_get(step, :operator, "=")
        step_kind = kind_raw isa AbstractString ? String(kind_raw) : ""
        op = op_raw isa AbstractString ? String(op_raw) : ""
        value = _design_finite_non_bool_real(_raw_get(step, :value, nothing))
        if step_kind == "reaction_order" && op in ("=", "==") && value !== nothing
            push!(values, value)
            push!(audit, DesignabilityAuditItem(
                path = path,
                kind = "reaction_order_step",
                support_level = "enforced_exact",
                stage = "compile",
                solver = "behavior_spec_reaction_order_lowering",
                reason = "Exact reaction_order step lowers to an exact RO program coordinate.",
            ))
        else
            unsupported = true
            push!(audit, DesignabilityAuditItem(
                path = path,
                kind = step_kind,
                support_level = "unsupported",
                hard = _design_hard_clause(step),
                stage = "compile",
                solver = "none",
                reason = "Only exact finite non-Bool reaction_order steps currently lower to the exact atlas solver.",
            ))
        end
    end
    unsupported && return nothing
    push!(audit, DesignabilityAuditItem(
        path = "/target/behavior_spec",
        kind = "reaction_order_program",
        support_level = "enforced_exact",
        stage = "compile",
        solver = "behavior_spec_reaction_order_lowering",
        reason = "Structured behavior_spec reaction_order program lowers to the enumerated design index.",
    ))
    return ("exact", values, input, output)
end

function _design_audit_behavior_spec_side_clauses!(raw, audit::Vector{DesignabilityAuditItem},
                                                   exact_program = nothing,
                                                   constraints = Dict{String, Any}())
    raw isa AbstractDict || return audit
    behavior = Dict{String, Any}(_materialize(raw))
    if _raw_haskey(behavior, :input_window)
        win = _raw_get(behavior, :input_window, Dict{String, Any}())
        unsupported_input_window_keys = _design_audit_unknown_nested_keys!(
            audit,
            win,
            "/target/behavior_spec/input_window",
            DESIGN_INPUT_WINDOW_KEYS,
            "unsupported_input_window_key",
        )
        unsupported_input_window_keys && return audit
        _design_audit_hard_field_contract!(audit, win, "/target/behavior_spec/input_window")
        has_bounds = win isa AbstractDict && _raw_haskey(win, :input_log10)
        win_path = has_bounds ? "/target/behavior_spec/input_window/input_log10" : "/target/behavior_spec/input_window"
        has_exact_program = exact_program isa AbstractVector
        supported = has_exact_program && _design_input_window_log10_bounds(win) !== nothing
        push!(audit, DesignabilityAuditItem(
            path = win_path,
            kind = "finite_input_window",
            support_level = supported ? "enforced_exact" : "unsupported",
            hard = supported ? _design_hard_clause(win) : true,
            stage = supported ? "feasible_region" : "compile",
            solver = supported ? "exact-window-siso-rop-path" : "none",
            reason = supported ?
                "Finite input-window realization is certified by an augmented SISO path polyhedron with explicit input witnesses." :
                (!has_exact_program ?
                    "Finite input-window clauses require target.behavior_spec.program to lower to an exact reaction-order program before they can be certified." :
                has_bounds ?
                    "Finite input-window clauses require input_log10 to be two finite numeric bounds before they can be certified." :
                    "Finite input-window clauses require input_log10 bounds before they can be certified."),
        ))
        if win isa AbstractDict && _raw_haskey(win, :min_spacing_decades) &&
           _design_input_window_spacing_value(win) === nothing
            push!(audit, DesignabilityAuditItem(
                path = "/target/behavior_spec/input_window/min_spacing_decades",
                kind = "finite_input_window_spacing",
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "input_window.min_spacing_decades must be finite and nonnegative before exact-window enforcement.",
            ))
        end
        if win isa AbstractDict && _raw_haskey(win, :operating_points_log10)
            operating_points = _design_valid_behavior_operating_points(win, exact_program, constraints)
            op_supported = operating_points !== nothing
            push!(audit, DesignabilityAuditItem(
                path = "/target/behavior_spec/input_window/operating_points_log10",
                kind = "finite_input_operating_points",
                support_level = op_supported ? "enforced_exact" : "unsupported",
                hard = op_supported ? _design_hard_clause(win) : true,
                stage = op_supported ? "feasible_region" : "compile",
                solver = op_supported ? "exact-window-siso-rop-path" : "none",
                reason = op_supported ?
                    "Explicit finite-window operating points are enforced as equality constraints on ordered input witnesses in the augmented SISO path polyhedron." :
                    _design_operating_points_unsupported_reason(win, exact_program, constraints),
            ))
        end
    end
    return audit
end

function _design_input_window_log10_bounds(input_window)
    input_window isa AbstractDict || return nothing
    _raw_haskey(input_window, :input_log10) || return nothing
    raw = _raw_get(input_window, :input_log10, nothing)
    (raw isa AbstractVector || raw isa Tuple) || return nothing
    values = collect(raw)
    length(values) == 2 || return nothing
    bounds = Float64[]
    for value in values
        value isa Bool && return nothing
        value isa Real || return nothing
        push!(bounds, Float64(value))
    end
    all(isfinite, bounds) || return nothing
    bounds[1] <= bounds[2] || return nothing
    return (bounds[1], bounds[2])
end

function _design_target_has_behavior_input_window(target::AbstractDict)
    behavior = _raw_get(target, :behavior_spec, nothing)
    behavior isa AbstractDict || return false
    win = _raw_get(behavior, :input_window, nothing)
    return _design_input_window_log10_bounds(win) !== nothing
end

function _design_transition_spacing_value(transitions)
    transitions isa AbstractDict || return nothing
    _raw_haskey(transitions, :min_spacing_decades) || return nothing
    raw = _raw_get(transitions, :min_spacing_decades, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    spacing = Float64(raw)
    isfinite(spacing) && spacing >= 0.0 || return nothing
    return spacing
end

function _design_transition_order_program_indices(transitions, exact_program)
    transitions isa AbstractDict || return nothing
    _raw_haskey(transitions, :order) || return nothing
    exact_program isa AbstractVector || return nothing
    raw = _raw_get(transitions, :order, nothing)
    (raw isa AbstractVector || raw isa Tuple) || return nothing
    values = collect(raw)
    n = length(exact_program)
    length(values) == n || return nothing
    order = Int[]
    seen = falses(n)
    for value in values
        value isa Bool && return nothing
        value isa Integer || return nothing
        idx = Int(value)
        0 <= idx < n || return nothing
        seen[idx + 1] && return nothing
        seen[idx + 1] = true
        push!(order, idx)
    end
    all(seen) || return nothing
    return order
end

function _design_transition_order_supported(target::AbstractDict, target_source_path::AbstractString,
                                            exact_program, transitions)
    _design_transition_order_program_indices(transitions, exact_program) === nothing && return false
    target_source_path == "/target/behavior_spec" || return false
    _design_target_has_behavior_input_window(target) || return false
    exact_program isa AbstractVector || return false
    return true
end

function _design_transition_order_unsupported_reason(target::AbstractDict, target_source_path::AbstractString,
                                                     exact_program, transitions)
    _design_transition_order_program_indices(transitions, exact_program) === nothing &&
        return "Transition order must be a full 0-based integer permutation of behavior_spec.program witness indices."
    target_source_path == "/target/behavior_spec" ||
        return "Transition order can only be certified when the target lowers from target.behavior_spec to an exact reaction-order program."
    _design_target_has_behavior_input_window(target) ||
        return "Transition order requires target.behavior_spec.input_window.input_log10 so the exact-window solver has witness input variables."
    exact_program isa AbstractVector ||
        return "Transition order requires target.behavior_spec.program to lower to an exact reaction-order program."
    return "Transition order is not certified by the active exact-window solver for this target."
end

function _design_operating_points_effective_spacing(input_window, constraints)
    input_spacing = _design_input_window_spacing_value(input_window)
    input_spacing === nothing && return nothing
    spacing = input_spacing
    transitions = constraints isa AbstractDict ? _raw_get(constraints, :transitions, nothing) : nothing
    if transitions isa AbstractDict && _raw_haskey(transitions, :min_spacing_decades)
        transition_spacing = _design_transition_spacing_value(transitions)
        transition_spacing === nothing && return nothing
        spacing = max(spacing, transition_spacing)
    end
    return spacing
end

function _design_valid_behavior_operating_points(input_window, exact_program, constraints)
    exact_program isa AbstractVector || return nothing
    bounds = _design_input_window_log10_bounds(input_window)
    bounds === nothing && return nothing
    spacing = _design_operating_points_effective_spacing(input_window, constraints)
    spacing === nothing && return nothing
    return _designability_operating_points(input_window, length(exact_program);
        input_bounds = bounds,
        min_spacing = spacing)
end

function _design_operating_points_unsupported_reason(input_window, exact_program, constraints)
    exact_program isa AbstractVector ||
        return "operating_points_log10 requires target.behavior_spec.program to lower to an exact reaction-order program."
    _design_input_window_log10_bounds(input_window) !== nothing ||
        return "operating_points_log10 requires behavior_spec.input_window.input_log10 to be two finite numeric bounds."
    _design_input_window_spacing_value(input_window) !== nothing ||
        return "operating_points_log10 requires input_window.min_spacing_decades to be finite and nonnegative when declared."
    transitions = constraints isa AbstractDict ? _raw_get(constraints, :transitions, nothing) : nothing
    if transitions isa AbstractDict && _raw_haskey(transitions, :min_spacing_decades) &&
       _design_transition_spacing_value(transitions) === nothing
        return "operating_points_log10 requires constraints.transitions.min_spacing_decades to be finite and nonnegative when declared."
    end
    raw_points = _raw_get(input_window, :operating_points_log10, nothing)
    (raw_points isa AbstractVector || raw_points isa Tuple) ||
        return "operating_points_log10 must be an array or tuple."
    length(collect(raw_points)) == length(exact_program) ||
        return "operating_points_log10 length must match the exact reaction-order program length."
    return "operating_points_log10 entries must be finite non-Bool real values within input_log10 bounds and ordered by the effective min_spacing_decades."
end

function _design_transition_spacing_supported(target::AbstractDict, target_source_path::AbstractString,
                                              exact_program, transitions)
    _design_transition_spacing_value(transitions) === nothing && return false
    target_source_path == "/target/behavior_spec" || return false
    _design_target_has_behavior_input_window(target) || return false
    exact_program isa AbstractVector || return false
    return length(exact_program) >= 2
end

function _design_transition_spacing_unsupported_reason(target::AbstractDict, target_source_path::AbstractString,
                                                       exact_program, transitions)
    _design_transition_spacing_value(transitions) === nothing &&
        return "Transition min-spacing requires a finite nonnegative min_spacing_decades value before exact-window enforcement."
    target_source_path == "/target/behavior_spec" ||
        return "Transition min-spacing can only be certified when the target lowers from target.behavior_spec to an exact reaction-order program."
    _design_target_has_behavior_input_window(target) ||
        return "Transition min-spacing requires target.behavior_spec.input_window.input_log10 so the exact-window solver has witness input variables."
    exact_program isa AbstractVector && length(exact_program) >= 2 ||
        return "Transition min-spacing requires an exact reaction-order program with at least two finite-window witnesses."
    return "Transition min-spacing is not certified by the active exact-window solver for this target."
end

function _design_output_feature_operator(output_feature)
    output_feature isa AbstractDict || return ""
    raw = _raw_get(output_feature, :operator, "=")
    op = raw isa AbstractString ? String(raw) : ""
    return op
end

function _design_output_feature_value(output_feature)
    output_feature isa AbstractDict || return nothing
    _raw_haskey(output_feature, :value) || return nothing
    raw = _raw_get(output_feature, :value, nothing)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    value = Float64(raw)
    isfinite(value) || return nothing
    return value
end

function _design_output_feature_supported(output_feature)
    output_feature isa AbstractDict || return false
    feature_raw = _raw_get(output_feature, :feature, "")
    feature = feature_raw isa AbstractString ? String(feature_raw) : ""
    feature in ("threshold", "level", "fold_change") || return false
    op = _design_output_feature_operator(output_feature)
    op in (">=", "<=", "=") || return false
    value = _design_output_feature_value(output_feature)
    value === nothing && return false
    feature == "fold_change" && value <= 0.0 && return false
    _designability_sample_points(output_feature) !== nothing || return false
    _designability_output_feature_tolerance(output_feature, value) !== nothing || return false
    return true
end

function _design_output_feature_audit_path(target::AbstractDict,
                                           target_source_path::AbstractString,
                                           output_feature)
    target_source_path == "/target/behavior_spec" || return "/target/output_feature"
    _design_target_has_behavior_input_window(target) || return "/target/output_feature"
    output_feature isa AbstractDict || return "/target/output_feature"
    feature_raw = _raw_get(output_feature, :feature, "")
    feature = feature_raw isa AbstractString ? String(feature_raw) : ""
    feature in ("threshold", "level", "fold_change") || return "/target/output_feature"
    op = _design_output_feature_operator(output_feature)
    op in (">=", "<=", "=") || return "/target/output_feature"
    value = _design_output_feature_value(output_feature)
    value === nothing && return "/target/output_feature"
    feature == "fold_change" && value <= 0.0 && return "/target/output_feature"
    _designability_sample_points(output_feature) === nothing &&
        return "/target/output_feature/sample_points"
    _designability_output_feature_tolerance(output_feature, value) === nothing &&
        return "/target/output_feature/tolerance_log10"
    return "/target/output_feature"
end

function _design_output_feature_unsupported_reason(target::AbstractDict, output_feature)
    output_feature isa AbstractDict ||
        return "Output feature constraints must be objects before sampled finite-dose verification can certify them."
    feature_raw = _raw_get(output_feature, :feature, "")
    feature = feature_raw isa AbstractString ? String(feature_raw) : ""
    feature in ("threshold", "level", "fold_change") ||
        return "Only threshold, level, and fold_change output_feature constraints are supported by the sampled finite-dose verifier."
    op = _design_output_feature_operator(output_feature)
    op in (">=", "<=", "=") ||
        return "Only >=, <=, and = output_feature operators are supported by the sampled finite-dose verifier."
    value = _design_output_feature_value(output_feature)
    value === nothing &&
        return "Output feature constraints require a finite value before sampled finite-dose verification can certify them."
    feature == "fold_change" && value <= 0.0 &&
        return "fold_change output_feature constraints require a positive finite value."
    _designability_sample_points(output_feature) !== nothing ||
        return "Output feature constraints require sample_points to be an integer from 11 to 1001 before sampled finite-dose verification can certify them."
    _designability_output_feature_tolerance(output_feature, value) !== nothing ||
        return "Output feature tolerances must be finite nonnegative numbers before sampled finite-dose verification can certify them."
    _design_target_has_behavior_input_window(target) ||
        return "Output feature constraints require target.behavior_spec.input_window.input_log10 before sampled finite-dose verification can certify them."
    return "Output feature constraints are not supported by the active sampled finite-dose verifier."
end

function _design_dynamic_range_supported(target::AbstractDict, dynamic_range)
    dynamic_range isa AbstractDict || return false
    _designability_dynamic_range_min_log10(dynamic_range) !== nothing || return false
    _designability_sample_points(dynamic_range) !== nothing || return false
    _design_target_has_behavior_input_window(target) || return false
    return true
end

function _design_dynamic_range_audit_path(dynamic_range)
    dynamic_range isa AbstractDict || return "/constraints/dynamic_range"
    if _designability_dynamic_range_min_log10(dynamic_range) !== nothing &&
       _designability_sample_points(dynamic_range) === nothing
        return "/constraints/dynamic_range/sample_points"
    end
    if _raw_haskey(dynamic_range, :sample_points) &&
       _designability_sample_points(dynamic_range) === nothing
        return "/constraints/dynamic_range/sample_points"
    end
    return _raw_haskey(dynamic_range, :min_fold_change) ?
        "/constraints/dynamic_range/min_fold_change" :
        "/constraints/dynamic_range"
end

function _design_dynamic_range_unsupported_reason(target::AbstractDict, dynamic_range)
    dynamic_range isa AbstractDict ||
        return "Dynamic-range constraints must be objects before sampled finite-dose verification can certify them."
    _designability_dynamic_range_min_log10(dynamic_range) !== nothing ||
        return "Dynamic-range constraints require min_fold_change to be a finite non-Bool numeric value before sampled finite-dose verification can certify them."
    _designability_sample_points(dynamic_range) !== nothing ||
        return "Dynamic-range constraints require sample_points to be an integer from 11 to 1001 before sampled finite-dose verification can certify them."
    _design_target_has_behavior_input_window(target) ||
        return "Dynamic-range constraints require a behavior_spec.input_window before the sampled forward verifier can certify them."
    return "Dynamic-range constraints are not supported by the active sampled finite-dose verifier."
end

function _design_shape_audit_path(target::AbstractDict,
                                  target_source_path::AbstractString,
                                  shape)
    target_source_path == "/target/behavior_spec" || return "/target/shape"
    _design_target_has_behavior_input_window(target) || return "/target/shape"
    shape isa AbstractDict || return "/target/shape"
    cls = _designability_shape_class(shape)
    if cls == "monotonic"
        _raw_haskey(shape, :min_prominence_log10) &&
            return "/target/shape/min_prominence_log10"
        _raw_haskey(shape, :min_prominence_decades) &&
            return "/target/shape/min_prominence_decades"
        _designability_shape_monotonicity(shape) in ("increasing", "decreasing", "any") ||
            return "/target/shape"
    elseif cls == "bell_shaped"
        _raw_haskey(shape, :min_prominence_log10) &&
            _raw_haskey(shape, :min_prominence_decades) &&
            return "/target/shape/min_prominence_decades"
        _designability_shape_min_prominence_log10(shape) !== nothing ||
            return "/target/shape"
    else
        return "/target/shape"
    end
    _designability_shape_sample_points(shape) === nothing &&
        return "/target/shape/sample_points"
    _designability_shape_tolerance_log10(shape) === nothing &&
        return "/target/shape/tolerance_log10"
    return "/target/shape"
end

function _design_shape_unsupported_reason(target::AbstractDict, target_source_path::AbstractString, shape)
    target_source_path == "/target/behavior_spec" ||
        return "Shape constraints can only be certified when the target lowers from target.behavior_spec to an exact reaction-order program."
    _design_target_has_behavior_input_window(target) ||
        return "Shape constraints require target.behavior_spec.input_window.input_log10 before sampled finite-dose verification can certify them."
    shape isa AbstractDict ||
        return "Shape constraints must be objects before sampled finite-dose verification can certify them."
    _designability_shape_sample_points(shape) !== nothing ||
        return "Shape constraints require sample_points to be an integer from 11 to 1001 before sampled finite-dose verification can certify them."
    _designability_shape_tolerance_log10(shape) !== nothing ||
        return "Shape constraints require a finite nonnegative tolerance_log10 value before sampled finite-dose verification can certify them."
    cls = _designability_shape_class(shape)
    if cls == "monotonic"
        _raw_haskey(shape, :min_prominence_log10) &&
            return "min_prominence_log10 is not supported for monotonic target.shape clauses."
        _raw_haskey(shape, :min_prominence_decades) &&
            return "min_prominence_decades is not supported for monotonic target.shape clauses."
        monotonicity = _designability_shape_monotonicity(shape)
        monotonicity == "nonmonotone" &&
            return "monotonicity = nonmonotone is not supported by the sampled finite-dose shape verifier."
        monotonicity in ("increasing", "decreasing", "any") ||
            return "Monotonic shape constraints support monotonicity increasing, decreasing, or any."
        return "Monotonic shape constraints are not supported by the active sampled finite-dose verifier."
    elseif cls == "bell_shaped"
        _raw_haskey(shape, :min_prominence_log10) &&
            _raw_haskey(shape, :min_prominence_decades) &&
            return "bell_shaped target.shape must declare exactly one prominence alias, not both min_prominence_log10 and min_prominence_decades."
        _designability_shape_min_prominence_log10(shape) !== nothing ||
            return "bell_shaped target shape requires explicit finite nonnegative min_prominence_log10 before sampled finite-dose verification can certify it."
        return "bell_shaped shape constraints are not supported by the active sampled finite-dose verifier."
    elseif cls in ("threshold", "level", "fold_change")
        return "threshold, level, and fold_change requirements are supported as target.output_feature clauses, not target.shape clauses."
    elseif cls in ("pulse", "custom")
        return "pulse and custom shape constraints are not supported by the active sampled finite-dose verifier."
    end
    return "Only monotonic and explicitly prominent bell_shaped target.shape clauses are supported by the sampled finite-dose verifier."
end

function _design_network_bound_value(network::AbstractDict, key::Symbol, minimum::Integer)
    _raw_haskey(network, key) || return nothing
    return _design_int_bound(_raw_get(network, key, nothing), minimum)
end

function _design_audit_network_bound!(audit::Vector{DesignabilityAuditItem},
                                      network::AbstractDict,
                                      key::Symbol,
                                      minimum::Integer,
                                      label::AbstractString)
    path = "/constraints/network/$(String(key))"
    value = _design_network_bound_value(network, key, minimum)
    if value === nothing
        push!(audit, DesignabilityAuditItem(
            path = path,
            kind = "network_complexity_bound",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "$(String(key)) must be an integer >= $(minimum) before the design index can enforce the $label bound.",
        ))
    else
        push!(audit, DesignabilityAuditItem(
            path = path,
            kind = "network_complexity_bound",
            support_level = "enforced_exact",
            hard = true,
            stage = "candidate_filter",
            solver = "design_index",
            reason = "Candidate records are filtered to $label <= $(value) before Pareto cells, screening, or verified recommendation.",
        ))
    end
    return audit
end

function _design_audit_network_constraints!(constraints::AbstractDict,
                                            audit::Vector{DesignabilityAuditItem})
    _raw_haskey(constraints, :network) || return audit
    network = _raw_get(constraints, :network, nothing)
    if !(network isa AbstractDict)
        push!(audit, DesignabilityAuditItem(
            path = "/constraints/network",
            kind = "network_constraints",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "constraints.network must be an object before the design index can enforce structural candidate filters.",
        ))
        return audit
    end
    _design_audit_unknown_nested_keys!(
        audit,
        network,
        "/constraints/network",
        DESIGN_NETWORK_KEYS,
        "unsupported_network_key",
    )
    _raw_haskey(network, :max_species) &&
        _design_audit_network_bound!(audit, network, :max_species, 1, "d")
    _raw_haskey(network, :max_reactions) &&
        _design_audit_network_bound!(audit, network, :max_reactions, 0, "r")
    _raw_haskey(network, :max_mu) &&
        _design_audit_network_bound!(audit, network, :max_mu, 1, "μ")
    if _raw_haskey(network, :allow_near_minimal)
        raw_allow = _raw_get(network, :allow_near_minimal, nothing)
        if raw_allow isa Bool
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/network/allow_near_minimal",
                kind = "network_near_minimal_policy",
                support_level = "enforced_exact",
                hard = true,
                stage = "candidate_filter",
                solver = "design_index",
                reason = raw_allow ?
                    "Near-minimal candidate records remain eligible after structural network bounds." :
                    "Candidate records are restricted to Pareto-minimal d/r/μ cells after structural network bounds.",
            ))
        else
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/network/allow_near_minimal",
                kind = "network_near_minimal_policy",
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "allow_near_minimal must be boolean before the design index can enforce the near-minimal eligibility policy.",
            ))
        end
    end
    return audit
end

function _design_audit_designability_constraints!(constraints::AbstractDict, audit::Vector{DesignabilityAuditItem},
                                                  target::AbstractDict,
                                                  target_source_path::AbstractString,
    exact_program)
    _design_audit_network_constraints!(constraints, audit)
    robustness = _raw_get(constraints, :robustness, nothing)
    if _raw_haskey(constraints, :robustness) && !(robustness isa AbstractDict)
        push!(audit, DesignabilityAuditItem(
            path = "/constraints/robustness",
            kind = "robustness_contract",
            support_level = "unsupported",
            hard = true,
            stage = "compile",
            solver = "none",
            reason = "constraints.robustness must be an object before robustness requirements can be audited or enforced.",
        ))
    elseif robustness isa AbstractDict
        _design_audit_hard_field_contract!(audit, robustness, "/constraints/robustness")
        unsupported_robustness_keys = _design_audit_unknown_nested_keys!(
            audit,
            robustness,
            "/constraints/robustness",
            DESIGN_ROBUSTNESS_KEYS,
            "unsupported_robustness_key",
        )
        if !unsupported_robustness_keys && _raw_haskey(robustness, :min_chebyshev_radius)
            radius = _design_nonnegative_finite_real(_raw_get(robustness, :min_chebyshev_radius, nothing))
            if radius === nothing
                push!(audit, DesignabilityAuditItem(
                    path = "/constraints/robustness/min_chebyshev_radius",
                    kind = "robustness_margin",
                    support_level = "unsupported",
                    hard = true,
                    stage = "compile",
                    solver = "none",
                    reason = "min_chebyshev_radius must be a finite nonnegative non-Bool real value before exact feasible-region filtering.",
                ))
            else
                push!(audit, DesignabilityAuditItem(
                    path = "/constraints/robustness/min_chebyshev_radius",
                    kind = "robustness_margin",
                    support_level = "enforced_exact",
                    stage = "candidate_filter",
                    solver = "exact-union-siso-rop",
                    reason = "Verified candidates must have an exact feasible-region Chebyshev radius at least this value.",
                ))
            end
        end
        if !unsupported_robustness_keys &&
           _raw_haskey(robustness, :min_tunable_volume_lower_bound) &&
           _raw_haskey(robustness, :min_tunable_volume)
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/robustness/min_tunable_volume",
                kind = "tunable_volume_lower_bound_alias",
                support_level = "unsupported",
                hard = true,
                stage = "compile",
                solver = "none",
                reason = "Do not specify both min_tunable_volume_lower_bound and legacy min_tunable_volume; use the explicit lower-bound field.",
            ))
        else
            for (field, path) in (
                (:min_tunable_volume_lower_bound, "/constraints/robustness/min_tunable_volume_lower_bound"),
                (:min_tunable_volume, "/constraints/robustness/min_tunable_volume"),
            )
                if !unsupported_robustness_keys && _raw_haskey(robustness, field)
                    volume = _design_nonnegative_finite_real(_raw_get(robustness, field, nothing))
                    if volume === nothing
                        push!(audit, DesignabilityAuditItem(
                            path = path,
                            kind = "tunable_volume_lower_bound",
                            support_level = "unsupported",
                            hard = true,
                            stage = "compile",
                            solver = "none",
                            reason = "$(String(field)) must be a finite nonnegative non-Bool real value before exact feasible-region filtering.",
                        ))
                    else
                        push!(audit, DesignabilityAuditItem(
                            path = path,
                            kind = "tunable_volume_lower_bound",
                            support_level = "enforced_exact",
                            stage = "candidate_filter",
                            solver = "exact-union-siso-rop",
                            reason = "Verified candidates must have a conservative Chebyshev-ball volume lower bound over exact feasible-region cells at least this value.",
                        ))
                    end
                end
            end
        end
        if !unsupported_robustness_keys && _raw_haskey(robustness, :condition_number_max)
            max_condition = _design_nonnegative_finite_real(_raw_get(robustness, :condition_number_max, nothing))
            reason = max_condition === nothing ?
                "condition_number_max must be a finite nonnegative non-Bool real value before it can be considered by a parameter-to-breakpoint verifier." :
                "condition_number_max is recognized, but the active exact-window solver does not yet produce a unique parameter-to-breakpoint mapping condition number; witness breakpoints are existential variables unless an equality block uniquely determines them."
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/robustness/condition_number_max",
                kind = "parameter_to_breakpoint_condition_number",
                support_level = "unsupported",
                hard = max_condition === nothing ? true : _design_hard_clause(robustness),
                stage = "compile",
                solver = "none",
                reason = reason,
            ))
        end
        if !unsupported_robustness_keys && _raw_haskey(robustness, :min_sampled_pass_fraction)
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/robustness/min_sampled_pass_fraction",
                kind = "min_sampled_pass_fraction",
                support_level = "unsupported",
                hard = _design_hard_clause(robustness),
                stage = "compile",
                solver = "none",
                reason = "This robustness constraint is not yet backed by an exact or sampled designability solver.",
            ))
        end
    end

    if _raw_haskey(constraints, :dynamic_range)
        dynamic_range = _raw_get(constraints, :dynamic_range, Dict{String, Any}())
        _design_audit_hard_field_contract!(audit, dynamic_range, "/constraints/dynamic_range")
        unsupported_dynamic_range_keys = _design_audit_unknown_nested_keys!(
            audit,
            dynamic_range,
            "/constraints/dynamic_range",
            DESIGN_DYNAMIC_RANGE_KEYS,
            "unsupported_dynamic_range_key",
        )
        dyn_path = _design_dynamic_range_audit_path(dynamic_range)
        supported = !unsupported_dynamic_range_keys &&
            _design_dynamic_range_supported(target, dynamic_range)
        if !unsupported_dynamic_range_keys
            push!(audit, DesignabilityAuditItem(
                path = dyn_path,
                kind = "dynamic_range",
                support_level = supported ? "sampled_forward" : "unsupported",
                hard = supported ? _design_hard_clause(dynamic_range) : true,
                stage = supported ? "forward_verification" : "compile",
                solver = supported ? "sampled-window-dose-response" : "none",
                reason = supported ?
                    "Dynamic range is enforced by scanning the exact-window theta across behavior_spec.input_window and rejecting invalid or floor-limited curves." :
                    _design_dynamic_range_unsupported_reason(target, dynamic_range),
            ))
        end
    end
    if _raw_haskey(constraints, :transitions)
        transitions = _raw_get(constraints, :transitions, Dict{String, Any}())
        if transitions isa AbstractDict
            _design_audit_hard_field_contract!(audit, transitions, "/constraints/transitions")
            for key in keys(transitions)
                key_s = String(key)
                key_s in DESIGN_TRANSITION_KEYS && continue
                push!(audit, DesignabilityAuditItem(
                    path = "/constraints/transitions/$(_design_json_pointer_token(key_s))",
                    kind = "unsupported_transition_clause",
                    support_level = "unsupported",
                    hard = true,
                    stage = "compile",
                    solver = "none",
                    reason = "Unknown transition constraint keys are not solver-backed by the active exact-window compiler.",
                ))
            end
        end
        if transitions isa AbstractDict && _raw_haskey(transitions, :min_spacing_decades)
            supported = _design_transition_spacing_supported(target, target_source_path, exact_program, transitions)
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/transitions/min_spacing_decades",
                kind = "transition_spacing",
                support_level = supported ? "enforced_exact" : "unsupported",
                hard = supported ? _design_hard_clause(transitions) : true,
                stage = supported ? "feasible_region" : "compile",
                solver = supported ? "exact-window-siso-rop-path" : "none",
                reason = supported ?
                    "Transition min-spacing is enforced exactly as linear inequalities between finite-window witness input variables in the augmented SISO path polyhedron." :
                    _design_transition_spacing_unsupported_reason(target, target_source_path, exact_program, transitions),
            ))
        end
        if transitions isa AbstractDict && _raw_haskey(transitions, :order)
            supported = _design_transition_order_supported(target, target_source_path, exact_program, transitions)
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/transitions/order",
                kind = "transition_order",
                support_level = supported ? "enforced_exact" : "unsupported",
                hard = supported ? _design_hard_clause(transitions) : true,
                stage = supported ? "feasible_region" : "compile",
                solver = supported ? "exact-window-siso-rop-path" : "none",
                reason = supported ?
                    "Transition order is enforced exactly as linear inequalities between finite-window witness input variables in behavior_spec.program index order." :
                    _design_transition_order_unsupported_reason(target, target_source_path, exact_program, transitions),
            ))
        end
        if !(transitions isa AbstractDict) || (!_raw_haskey(transitions, :min_spacing_decades) && !_raw_haskey(transitions, :order))
            push!(audit, DesignabilityAuditItem(
                path = "/constraints/transitions",
                kind = "transitions",
                support_level = "unsupported",
                hard = _design_hard_clause(transitions),
                stage = "compile",
                solver = "none",
                reason = "Transition constraints must use a solver-backed nested clause before verified recommendation.",
            ))
        end
    end
    return audit
end

function designability_has_unsupported_hard_clause(spec::NormalizedDesignabilitySpec)
    return any(item -> item.hard && item.support_level == "unsupported", spec.audit)
end

function _design_external_constraint_audit(spec::NormalizedDesignabilitySpec)
    if _raw_get(spec.audit_policy, :include_supported, true) === false
        return [audit_to_dict(item) for item in spec.audit if item.support_level == "unsupported"]
    end
    return [audit_to_dict(item) for item in spec.audit]
end

function designability_spec_to_dict(spec::NormalizedDesignabilitySpec)
    return Dict(
        "schema_version" => spec.schema_version,
        "source" => spec.source,
        "target" => spec.target,
        "constraints" => spec.constraints,
        "candidate_budget" => spec.candidate_budget,
        "ranking_policy" => spec.ranking_policy,
        "audit_policy" => spec.audit_policy,
        "has_legacy_target" => spec.has_legacy_target,
        "has_search_target" => spec.has_search_target,
        "legacy_target_kind" => spec.legacy_target_kind,
        "legacy_target" => spec.legacy_target,
        "target_source_path" => spec.target_source_path,
        "required_input" => spec.required_input,
        "required_output" => spec.required_output,
    )
end

function _design_network_constraints(spec::NormalizedDesignabilitySpec)
    network = _raw_get(spec.constraints, :network, nothing)
    network isa AbstractDict || return nothing
    return network
end

function _design_network_allow_near_minimal(network)
    network isa AbstractDict || return true
    _raw_haskey(network, :allow_near_minimal) || return true
    raw_allow = _raw_get(network, :allow_near_minimal, true)
    raw_allow isa Bool || return true
    return raw_allow
end

function _design_record_satisfies_network_constraints(rec, network)
    network isa AbstractDict || return true
    max_species = _design_network_bound_value(network, :max_species, 1)
    max_species !== nothing && Int(rec.d) > max_species && return false
    max_reactions = _design_network_bound_value(network, :max_reactions, 0)
    max_reactions !== nothing && Int(rec.r) > max_reactions && return false
    max_mu = _design_network_bound_value(network, :max_mu, 1)
    max_mu !== nothing && Int(rec.mu) > max_mu && return false
    return true
end

function _design_apply_network_minimal_policy(records, pareto_cells, network)
    _design_network_allow_near_minimal(network) && return records
    pareto_cell_set = Set(pareto_cells)
    return [rec for rec in records if (rec.d, rec.r, rec.mu) in pareto_cell_set]
end

function _design_records_from_budget(matched, pareto_cells, candidate_budget, constraints=Dict{String, Any}())
    mode = String(_raw_get(candidate_budget, :mode, "near_minimal"))
    mode in ("near_minimal", "all_matches") ||
        error("candidate_budget.mode must be `near_minimal` or `all_matches`")
    network = constraints isa AbstractDict ? _raw_get(constraints, :network, nothing) : nothing
    if mode == "all_matches"
        return _design_apply_network_minimal_policy(matched, pareto_cells, network)
    end
    max_extra_d = max(0, _design_int(candidate_budget, :max_extra_species, 1))
    max_extra_r = max(0, _design_int(candidate_budget, :max_extra_reactions, 1))
    max_extra_mu = max(0, _design_int(candidate_budget, :max_extra_mu, 1))
    records = [rec for rec in matched if _design_near_minimal(rec, pareto_cells, max_extra_d, max_extra_r, max_extra_mu)]
    records = isempty(records) ? matched : records
    return _design_apply_network_minimal_policy(records, pareto_cells, network)
end

function _design_downgrade_proxy_card!(card)
    delete!(card, "tunability_score")
    card["pass"] = false
    card["screen_status"] = "screened_proxy"
    card["evidence_grade"] = "proxy_only"
    card["certificate_grade"] = "proxy-only"
    metrics = card["metrics"]
    if haskey(metrics, "chebyshev_radius")
        metrics["ranking_margin_proxy"] = metrics["chebyshev_radius"]
        delete!(metrics, "chebyshev_radius")
        delete!(metrics, "chebyshev_radius_source")
        delete!(metrics, "chebyshev_units")
    end
    haskey(metrics, "tunable_volume") &&
        (metrics["atlas_volume_proxy"] = metrics["tunable_volume"])
    haskey(metrics, "tunable_volume_sum") &&
        (metrics["atlas_volume_sum_proxy"] = metrics["tunable_volume_sum"])
    haskey(metrics, "transition_spacing") &&
        (metrics["transition_spacing_proxy"] = metrics["transition_spacing"])
    for key in ("tunable_volume", "tunable_volume_sum", "transition_spacing",
                "parameter_extremeness", "condition_number",
                "parameter_breakpoint_sensitivity")
        delete!(metrics, key)
    end
    _design_clear_legacy_proxy_constraint_evidence!(card["constraints"])
    card["reason"] = "Exploratory candidate only: no full DesignabilitySpec feasible-region certificate was computed."
    theta = card["parameter_recommendation"]["theta_star"]
    theta["status"] = "not_computed"
    theta["source"] = "not_computed_no_parameter_bounds"
    theta["source_type"] = "proxy"
    theta["bounds_verified"] = false
    theta["log_qK"] = Any[]
    theta["kd"] = Any[]
    theta["totals"] = Dict{String, Any}()
    theta["note"] = "No parameter seed is emitted for exploratory proxy candidates without explicit parameter bounds."
    return card
end

function _design_parameter_bounds(spec::NormalizedDesignabilitySpec)
    bounds = _raw_get(spec.constraints, :parameter_bounds, Dict{String, Any}())
    if !isempty(bounds)
        by_class = Dict{String, Any}()
        raw_by_class = _raw_get(bounds, :by_class, Dict{String, Any}())
        if raw_by_class isa AbstractDict
            for key in (:kd, :total)
                if _raw_haskey(raw_by_class, key)
                    pair = _design_parameter_bound_pair(_raw_get(raw_by_class, key, nothing))
                    pair === nothing || (by_class[String(key)] = pair)
                end
            end
        end
        if _raw_haskey(bounds, :kd_log10)
            pair = _design_parameter_bound_pair(_raw_get(bounds, :kd_log10, nothing))
            pair === nothing || (by_class["kd"] = pair)
        end
        if _raw_haskey(bounds, :total_log10)
            pair = _design_parameter_bound_pair(_raw_get(bounds, :total_log10, nothing))
            pair === nothing || (by_class["total"] = pair)
        end
        return Dict{String, Any}(
            "basis" => "log10_qK",
            "by_class" => by_class,
        )
    end
    return nothing
end

function _design_default_parameter_bounds()
    error("verified recommendations require explicit constraints.parameter_bounds")
end

function _design_effective_parameter_bounds(spec::NormalizedDesignabilitySpec)
    declared = _design_parameter_bounds(spec)
    declared === nothing && error("verified recommendations require explicit constraints.parameter_bounds")
    return declared
end

function _design_parameter_bounds_source(spec::NormalizedDesignabilitySpec)
    _design_parameter_bounds(spec) === nothing &&
        error("verified recommendations require explicit constraints.parameter_bounds")
    return "declared_spec"
end

function _design_parameter_bounds_phrase(spec::NormalizedDesignabilitySpec)
    _design_parameter_bounds_source(spec)
    return "declared qK bounds"
end

function _design_delete_raw_key!(dict::AbstractDict, key::AbstractString)
    delete!(dict, String(key))
    delete!(dict, Symbol(key))
    return dict
end

function _design_vector_field!(dict::AbstractDict, key::AbstractString)
    existing = _raw_get(dict, Symbol(key), nothing)
    existing isa AbstractVector && return existing
    dict[String(key)] = Any[]
    return dict[String(key)]
end

function _design_clear_legacy_proxy_constraint_evidence!(constraints::AbstractDict)
    for key in ("kd_bounds", "total_bounds", "min_dynamic_range",
                "min_transition_spacing_decades", "total_bounds_role",
                "supported_handles", "unsupported_handles", "unsupported_constraints")
        _design_delete_raw_key!(constraints, key)
    end
    supported = _design_vector_field!(constraints, "supported_constraints")
    filter!(supported) do item
        item isa AbstractDict || return true
        path = string(_raw_get(item, :path, ""))
        support_level = string(_raw_get(item, :support_level, ""))
        stage = string(_raw_get(item, :stage, ""))
        return !(support_level in ("proxy_only", "enforced") ||
                 startswith(path, "tuning.") ||
                 stage == "screen")
    end
    return constraints
end

function _design_clear_inherited_proxy_card_evidence!(card)
    delete!(card, "tunability_score")
    card["active_failures"] = String[]
    card["active_failure_details"] = Any[]
    return card
end

function _design_attach_parameter_bounds_evidence!(card, spec::NormalizedDesignabilitySpec)
    _design_clear_legacy_proxy_constraint_evidence!(card["constraints"])
    card["constraints"]["parameter_bounds_verified"] = true
    card["constraints"]["effective_parameter_bounds"] = _design_effective_parameter_bounds(spec)
    declared_bounds = _design_parameter_bounds(spec)
    declared_bounds === nothing &&
        error("verified recommendations require explicit constraints.parameter_bounds")
    card["constraints"]["parameter_bounds_source"] = "declared_spec"
    card["constraints"]["parameter_bounds"] = declared_bounds
    supported = _design_vector_field!(card["constraints"], "supported_constraints")
    for item in spec.audit
        if item.kind == "qk_box_bounds" &&
               item.support_level == "enforced_exact" &&
               startswith(item.path, "/constraints/parameter_bounds/by_class/")
            push!(supported, audit_to_dict(item))
        end
    end
    return card
end

function _design_attach_network_constraint_evidence!(card, spec::NormalizedDesignabilitySpec)
    network = _design_network_constraints(spec)
    network isa AbstractDict || return card
    supported = _design_vector_field!(card["constraints"], "supported_constraints")
    emitted = false
    network_constraints = Dict{String, Any}()
    for (key, minimum) in (
        (:max_species, 1),
        (:max_reactions, 0),
        (:max_mu, 1),
    )
        value = _design_network_bound_value(network, key, minimum)
        if value !== nothing
            network_constraints[String(key)] = value
        end
    end
    if _raw_haskey(network, :allow_near_minimal)
        raw_allow = _raw_get(network, :allow_near_minimal, true)
        raw_allow isa Bool && (network_constraints["allow_near_minimal"] = raw_allow)
    end
    for item in spec.audit
        if startswith(item.path, "/constraints/network/") &&
           item.support_level == "enforced_exact"
            push!(supported, audit_to_dict(item))
            emitted = true
        end
    end
    if emitted || !isempty(network_constraints)
        card["constraints"]["network_constraints_verified"] = true
        card["constraints"]["network_constraints"] = network_constraints
    end
    return card
end

function _design_min_chebyshev_radius(spec::NormalizedDesignabilitySpec)
    robustness = _raw_get(spec.constraints, :robustness, Dict{String, Any}())
    robustness isa AbstractDict || return 0.0
    value = _design_nonnegative_finite_real(_raw_get(robustness, :min_chebyshev_radius, 0.0))
    return value === nothing ? 0.0 : value
end

function _design_min_tunable_volume(spec::NormalizedDesignabilitySpec)
    robustness = _raw_get(spec.constraints, :robustness, Dict{String, Any}())
    robustness isa AbstractDict || return 0.0
    raw_value = _raw_haskey(robustness, :min_tunable_volume_lower_bound) ?
        _raw_get(robustness, :min_tunable_volume_lower_bound, 0.0) :
        _raw_get(robustness, :min_tunable_volume, 0.0)
    value = _design_nonnegative_finite_real(raw_value)
    return value === nothing ? 0.0 : value
end

function _design_unit_ball_volume(d::Integer)
    d <= 0 && return 0.0
    d == 1 && return 2.0
    prev2 = 1.0
    prev1 = 2.0
    for k in 2:Int(d)
        current = (2.0 * pi / k) * prev2
        prev2 = prev1
        prev1 = current
    end
    return prev1
end

function _design_tunable_volume_lower_bound(cell::DesignabilityCellResult)
    d = length(cell.log_qK)
    r = Float64(cell.chebyshev_radius)
    (d <= 0 || !isfinite(r) || r <= 0.0) && return 0.0
    volume = _design_unit_ball_volume(d) * r^d
    return isfinite(volume) && volume > 0.0 ? volume : 0.0
end

function _design_attach_tunable_volume_metrics!(metrics::AbstractDict, cell::DesignabilityCellResult)
    metrics["tunable_volume"] = _design_tunable_volume_lower_bound(cell)
    metrics["tunable_volume_source"] = "chebyshev_ball_lower_bound"
    metrics["tunable_volume_dimension"] = length(cell.log_qK)
    metrics["tunable_volume_units"] = "log10_qK_euclidean_ball"
    return metrics
end

function _design_attach_tunable_volume_constraint_evidence!(card, spec::NormalizedDesignabilitySpec)
    min_volume = _design_min_tunable_volume(spec)
    robustness = _raw_get(spec.constraints, :robustness, Dict{String, Any}())
    if robustness isa AbstractDict && _raw_haskey(robustness, :min_tunable_volume_lower_bound)
        card["constraints"]["min_tunable_volume_lower_bound"] = min_volume
        path = "/constraints/robustness/min_tunable_volume_lower_bound"
    else
        card["constraints"]["min_tunable_volume"] = min_volume
        (robustness isa AbstractDict && _raw_haskey(robustness, :min_tunable_volume)) || return card
        path = "/constraints/robustness/min_tunable_volume"
    end
    audit_item = _design_supported_audit_item(
        spec,
        path,
        "tunable_volume_lower_bound",
    )
    audit_item === nothing || push!(get!(card["constraints"], "supported_constraints", Any[]), audit_to_dict(audit_item))
    return card
end

function _design_verified_ranking_preferences(ranking_policy)
    ranking_policy isa AbstractDict || return String[]
    prefer = _raw_get(ranking_policy, :prefer, Any[])
    (prefer isa AbstractVector || prefer isa Tuple) || return String[]
    out = String[]
    for raw in prefer
        raw isa AbstractString || continue
        pref = String(raw)
        pref in out || push!(out, pref)
    end
    return out
end

function _design_verified_number(raw)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    value = Float64(raw)
    return isfinite(value) ? value : nothing
end

function _design_verified_metric(card, key::AbstractString)
    card isa AbstractDict || return nothing
    metrics = _raw_get(card, :metrics, nothing)
    metrics isa AbstractDict || return nothing
    return _design_verified_number(_raw_get(metrics, Symbol(key), nothing))
end

function _design_verified_complexity(card)
    card isa AbstractDict || return nothing
    complexity = _raw_get(card, :complexity, nothing)
    complexity isa AbstractDict || return nothing
    total = 0.0
    for key in (:d, :r, :mu)
        value = _design_verified_number(_raw_get(complexity, key, nothing))
        value === nothing && return nothing
        total += value
    end
    return total
end

function _design_verified_evidence_rank(card)
    card isa AbstractDict || return nothing
    _raw_haskey(card, :evidence_grade) || return nothing
    grade = String(_raw_get(card, :evidence_grade, ""))
    grade == "enforced_exact+sampled_forward" && return 2.0
    grade == "enforced_exact" && return 1.0
    return 0.0
end

function _design_verified_certificate_rank(card)
    card isa AbstractDict || return nothing
    _raw_haskey(card, :certificate_grade) || return nothing
    grade = String(_raw_get(card, :certificate_grade, ""))
    grade == "exact-window-siso-rop-path" && return 4.0
    grade == "exact-union-siso-rop-path" && return 3.0
    grade == "exact-union-siso-rop" && return 2.0
    return 0.0
end

function _design_verified_preference_value(card, pref::AbstractString)
    pref == "evidence_grade" && return _design_verified_evidence_rank(card)
    pref == "certificate_grade" && return _design_verified_certificate_rank(card)
    pref == "chebyshev_radius" && return _design_verified_metric(card, "chebyshev_radius")
    pref == "tunable_volume" && return _design_verified_metric(card, "tunable_volume")
    pref == "dynamic_range" && return _design_verified_metric(card, "dynamic_range")
    pref == "transition_spacing" && return _design_verified_metric(card, "transition_spacing")
    pref == "complexity" && return _design_verified_complexity(card)
    return nothing
end

function _design_verified_preference_less(a, b, pref::AbstractString)
    av = _design_verified_preference_value(a, pref)
    bv = _design_verified_preference_value(b, pref)
    (av === nothing || bv === nothing || av == bv) && return nothing
    pref == "complexity" && return av < bv
    return av > bv
end

function _design_verified_candidate_key(card)
    card isa AbstractDict || return ("", "", "")
    return (
        string(_raw_get(card, :nid, "")),
        string(_raw_get(card, :inp, "")),
        string(_raw_get(card, :out, "")),
    )
end

function _design_verified_card_less(a, b, prefs)
    for pref in prefs
        decision = _design_verified_preference_less(a, b, pref)
        decision === nothing || return decision
    end
    return _design_verified_candidate_key(a) < _design_verified_candidate_key(b)
end

function _design_sort_verified_cards!(cards, ranking_policy)
    prefs = _design_verified_ranking_preferences(ranking_policy)
    sort!(cards; lt = (a, b) -> _design_verified_card_less(a, b, prefs))
    return cards
end

function _design_behavior_input_window(spec::NormalizedDesignabilitySpec)
    behavior = _raw_get(spec.target, :behavior_spec, nothing)
    behavior isa AbstractDict || return nothing
    win = _raw_get(behavior, :input_window, nothing)
    win isa AbstractDict || return nothing
    _design_input_window_log10_bounds(win) !== nothing || return nothing
    return Dict{String, Any}(_materialize(win))
end

function _design_input_window_spacing_value(input_window)
    input_window isa AbstractDict || return 0.0
    _raw_haskey(input_window, :min_spacing_decades) || return 0.0
    raw = _raw_get(input_window, :min_spacing_decades, 0.0)
    raw isa Bool && return nothing
    raw isa Real || return nothing
    spacing = Float64(raw)
    isfinite(spacing) && spacing >= 0.0 || return nothing
    return spacing
end

function _design_input_window_min_spacing_decades(input_window)
    spacing = _design_input_window_spacing_value(input_window)
    return spacing === nothing ? 0.0 : spacing
end

function _design_supported_transition_spacing_decades(spec::NormalizedDesignabilitySpec)
    any(item -> item.path == "/constraints/transitions/min_spacing_decades" &&
                item.support_level == "enforced_exact",
        spec.audit) || return nothing
    transitions = _raw_get(spec.constraints, :transitions, nothing)
    return _design_transition_spacing_value(transitions)
end

function _design_supported_transition_order_program_indices(spec::NormalizedDesignabilitySpec)
    any(item -> item.path == "/constraints/transitions/order" &&
                item.kind == "transition_order" &&
                item.support_level == "enforced_exact",
        spec.audit) || return nothing
    target_program = _design_exact_ro_program(spec.legacy_target_kind, spec.legacy_target)
    target_program === nothing && return nothing
    transitions = _raw_get(spec.constraints, :transitions, nothing)
    return _design_transition_order_program_indices(transitions, target_program)
end

function _design_supported_transition_order_solver_indices(spec::NormalizedDesignabilitySpec)
    order = _design_supported_transition_order_program_indices(spec)
    order === nothing && return nothing
    return Int.(order .+ 1)
end

function _design_effective_behavior_input_window(spec::NormalizedDesignabilitySpec)
    input_window = _design_behavior_input_window(spec)
    input_window === nothing && return nothing
    spacing = _design_supported_transition_spacing_decades(spec)
    spacing === nothing && return input_window
    behavior_spacing = _design_input_window_min_spacing_decades(input_window)
    input_window["min_spacing_decades"] = max(behavior_spacing, spacing)
    return input_window
end

function _design_requested_operating_points_log10(spec::NormalizedDesignabilitySpec)
    target_program = _design_exact_ro_program(spec.legacy_target_kind, spec.legacy_target)
    target_program === nothing && return nothing
    input_window = _design_effective_behavior_input_window(spec)
    input_window === nothing && return nothing
    return _designability_operating_points(input_window, length(target_program))
end

function _design_operating_points_verified(requested, witness)
    requested === nothing && return false
    length(witness) == length(requested) || return false
    return all(abs(Float64(w) - Float64(r)) <= 1e-7 for (w, r) in zip(witness, requested))
end

function _design_supported_audit_item(spec::NormalizedDesignabilitySpec, path::AbstractString,
                                      kind::AbstractString)
    for item in spec.audit
        if item.path == path && item.kind == kind && item.support_level == "enforced_exact"
            return item
        end
    end
    return nothing
end

function _design_dynamic_range_constraint(spec::NormalizedDesignabilitySpec)
    dynamic_range = _raw_get(spec.constraints, :dynamic_range, nothing)
    dynamic_range isa AbstractDict || return nothing
    _raw_haskey(dynamic_range, :min_fold_change) || return nothing
    _designability_dynamic_range_min_log10(dynamic_range) !== nothing || return nothing
    _designability_dynamic_range_sample_points(dynamic_range) !== nothing || return nothing
    return Dict{String, Any}(_materialize(dynamic_range))
end

function _design_output_feature_constraint(spec::NormalizedDesignabilitySpec)
    output_feature = _raw_get(spec.target, :output_feature, nothing)
    output_feature isa AbstractDict || return nothing
    spec.target_source_path == "/target/behavior_spec" || return nothing
    _design_target_has_behavior_input_window(spec.target) || return nothing
    _design_output_feature_supported(output_feature) || return nothing
    return Dict{String, Any}(_materialize(output_feature))
end

function _design_shape_constraint(spec::NormalizedDesignabilitySpec)
    shape = _raw_get(spec.target, :shape, nothing)
    shape isa AbstractDict || return nothing
    spec.target_source_path == "/target/behavior_spec" || return nothing
    _design_target_has_behavior_input_window(spec.target) || return nothing
    _designability_shape_supported(shape) || return nothing
    return Dict{String, Any}(_materialize(shape))
end

function _design_exact_union_card(card, region::FeasibleRegionResult, spec::NormalizedDesignabilitySpec)
    best = first(region.cells)
    min_radius = _design_min_chebyshev_radius(spec)
    bounds_phrase = _design_parameter_bounds_phrase(spec)
    _design_clear_inherited_proxy_card_evidence!(card)
    card["pass"] = true
    card["screen_status"] = "verified_exact"
    card["evidence_grade"] = "enforced_exact"
    card["certificate_grade"] = "exact-union-siso-rop"
    card["reason"] = "Exact SISO RO feasible region is nonempty under $bounds_phrase."
    card["metrics"] = Dict(
        "chebyshev_radius" => round(best.chebyshev_radius; digits = 4),
        "chebyshev_radius_source" => "theta_union_cell",
        "chebyshev_units" => "log10_qK_euclidean",
        "feasible_cell_count" => length(region.cells),
        "exact_vertex_idx" => best.vertex_idx,
        "predicted_RO" => round(best.predicted_ro; digits = 4),
    )
    _design_attach_tunable_volume_metrics!(card["metrics"], best)
    card["constraints"]["bounds_intersection_verified"] = true
    _design_attach_parameter_bounds_evidence!(card, spec)
    _design_attach_network_constraint_evidence!(card, spec)
    card["constraints"]["min_chebyshev_radius"] = min_radius
    _design_attach_tunable_volume_constraint_evidence!(card, spec)
    supported = get!(card["constraints"], "supported_constraints", Any[])
    push!(supported, Dict(
        "path" => spec.target_source_path,
        "kind" => "reaction_order_single_slope",
        "support_level" => "enforced_exact",
        "hard" => true,
        "stage" => "feasible_region",
        "solver" => "exact-union-siso-rop",
        "reason" => "The feasible region cell intersects $bounds_phrase and has a nonempty Chebyshev center.",
    ))
    theta = card["parameter_recommendation"]["theta_star"]
    theta["status"] = "computed"
    theta["source"] = "feasible_region_chebyshev"
    theta["source_type"] = "exact_solver"
    theta["bounds_verified"] = true
    theta["log_qK"] = best.log_qK
    theta["kd"] = best.kd
    theta["totals"] = best.totals
    card["certificate_stack"] = Any[Dict(
        "grade" => "exact-union-siso-rop",
        "scope" => "siso_rop_union",
        "soundness" => "exact_polyhedral_union",
        "source" => "feasible_region_single_ro",
        "supports_exact_placement" => true,
    )]
    return card
end

function _design_exact_program_card(card, region::FeasibleRegionResult, spec::NormalizedDesignabilitySpec)
    best = first(region.cells)
    min_radius = _design_min_chebyshev_radius(spec)
    bounds_phrase = _design_parameter_bounds_phrase(spec)
    _design_clear_inherited_proxy_card_evidence!(card)
    window_verified = !isempty(best.witness_input_log10)
    requested_transition_spacing = _design_supported_transition_spacing_decades(spec)
    requested_transition_order = _design_supported_transition_order_program_indices(spec)
    behavior_input_window = _design_behavior_input_window(spec)
    behavior_window_spacing = behavior_input_window === nothing ? 0.0 : _design_input_window_min_spacing_decades(behavior_input_window)
    effective_transition_spacing = requested_transition_spacing === nothing ?
        nothing :
        max(behavior_window_spacing, requested_transition_spacing)
    transition_spacing_verified = window_verified && requested_transition_spacing !== nothing
    transition_order_verified = window_verified && requested_transition_order !== nothing
    requested_operating_points = _design_requested_operating_points_log10(spec)
    operating_points_verified = window_verified &&
        _design_operating_points_verified(requested_operating_points, best.witness_input_log10)
    witness_input_log10 = operating_points_verified ? requested_operating_points : best.witness_input_log10
    dynamic_range_verified = isfinite(best.sampled_dynamic_range_log10)
    output_feature_verified = best.sampled_output_feature_status == "pass"
    shape_verified = best.sampled_shape_status == "pass"
    sampled_forward_verified = dynamic_range_verified || output_feature_verified || shape_verified
    certificate_grade = window_verified ? "exact-window-siso-rop-path" : region.certificate_grade
    card["pass"] = true
    card["screen_status"] = "verified_exact"
    card["evidence_grade"] = sampled_forward_verified ? "enforced_exact+sampled_forward" : "enforced_exact"
    card["certificate_grade"] = certificate_grade
    card["reason"] = window_verified ?
        "Exact SISO RO path has finite-window input witnesses under $bounds_phrase." :
        "Exact SISO RO path feasible region is nonempty under $bounds_phrase."
    card["metrics"] = Dict(
        "chebyshev_radius" => round(best.chebyshev_radius; digits = 4),
        "chebyshev_radius_source" => "theta_union_path",
        "chebyshev_units" => "projected_log10_qK_euclidean",
        "feasible_path_count" => length(region.cells),
        "exact_path_idx" => best.path_idx,
        "exact_vertex_indices" => best.vertex_indices,
        "predicted_RO_program" => best.predicted_profile,
        "background_qK_symbols" => best.qK_symbols,
    )
    _design_attach_tunable_volume_metrics!(card["metrics"], best)
    if window_verified
        card["metrics"]["witness_input_log10"] = witness_input_log10
    end
    if requested_operating_points !== nothing
        card["metrics"]["requested_operating_points_log10"] = requested_operating_points
    end
    if transition_spacing_verified
        card["metrics"]["requested_transition_spacing_decades"] = requested_transition_spacing
        card["metrics"]["behavior_input_window_min_spacing_decades"] = behavior_window_spacing
        card["metrics"]["effective_transition_spacing_decades"] = effective_transition_spacing
        if length(best.witness_input_log10) >= 2
            spacing_order = requested_transition_order === nothing ?
                collect(0:(length(best.witness_input_log10) - 1)) :
                requested_transition_order
            ordered_witnesses = [best.witness_input_log10[idx + 1] for idx in spacing_order]
            witness_deltas = ordered_witnesses[2:end] .- ordered_witnesses[1:(end - 1)]
            card["metrics"]["witness_min_spacing_decades"] = minimum(witness_deltas)
            card["metrics"]["transition_spacing"] = card["metrics"]["witness_min_spacing_decades"]
            card["metrics"]["witness_spacing_order"] = spacing_order
            card["metrics"]["witness_spacing_basis"] = requested_transition_order === nothing ?
                "behavior_spec.program_index" :
                "constraints.transitions.order"
        end
    end
    if transition_order_verified
        card["metrics"]["requested_transition_order"] = requested_transition_order
        card["metrics"]["transition_order_basis"] = "behavior_spec.program_index"
    end
    if dynamic_range_verified
        card["metrics"]["sampled_dynamic_range_log10"] = round(best.sampled_dynamic_range_log10; digits = 4)
        card["metrics"]["sampled_dynamic_range_fold_change"] = round(best.sampled_dynamic_range_fold_change; digits = 4)
        card["metrics"]["dynamic_range"] = card["metrics"]["sampled_dynamic_range_fold_change"]
        card["metrics"]["sampled_dynamic_range_points"] = best.sampled_dynamic_range_points
        card["metrics"]["sampled_dynamic_range_floor_limited"] = best.sampled_dynamic_range_floor_limited
        card["metrics"]["sampled_dynamic_range_source"] = "sampled_forward_dose_response"
    end
    if output_feature_verified
        card["metrics"]["sampled_output_feature_status"] = best.sampled_output_feature_status
        card["metrics"]["sampled_output_feature"] = best.sampled_output_feature_name
        card["metrics"]["sampled_output_feature_operator"] = best.sampled_output_feature_operator
        card["metrics"]["sampled_output_feature_target"] = best.sampled_output_feature_target
        card["metrics"]["sampled_output_feature_value"] = round(best.sampled_output_feature_value; digits = 4)
        card["metrics"]["sampled_output_feature_range"] = round.(best.sampled_output_feature_range; digits = 4)
        card["metrics"]["sampled_output_feature_log10_range"] = round(best.sampled_output_feature_log10_range; digits = 4)
        card["metrics"]["sampled_output_feature_fold_change"] = round(best.sampled_output_feature_fold_change; digits = 4)
        card["metrics"]["sampled_output_feature_sample_points"] = best.sampled_output_feature_sample_points
        card["metrics"]["sampled_output_feature_floor_limited"] = best.sampled_output_feature_floor_limited
        card["metrics"]["sampled_output_feature_source"] = "sampled_forward_dose_response"
    end
    if shape_verified
        card["metrics"]["sampled_shape"] = best.sampled_shape_class
        card["metrics"]["sampled_shape_status"] = best.sampled_shape_status
        !isempty(best.sampled_shape_direction) &&
            (card["metrics"]["sampled_shape_direction"] = best.sampled_shape_direction)
        best.sampled_shape_peak_index > 0 &&
            (card["metrics"]["sampled_shape_peak_index"] = best.sampled_shape_peak_index)
        card["metrics"]["sampled_shape_sample_points"] = best.sampled_shape_sample_points
        card["metrics"]["sampled_shape_floor_limited"] = best.sampled_shape_floor_limited
        if isfinite(best.sampled_shape_min_prominence_log10)
            card["metrics"]["sampled_shape_min_prominence_log10"] = best.sampled_shape_min_prominence_log10
            card["metrics"]["sampled_shape_prominence_left_log10"] = round(best.sampled_shape_prominence_left_log10; digits = 4)
            card["metrics"]["sampled_shape_prominence_right_log10"] = round(best.sampled_shape_prominence_right_log10; digits = 4)
        end
        card["metrics"]["sampled_shape_source"] = "sampled_forward_dose_response"
    end
    card["constraints"]["bounds_intersection_verified"] = true
    _design_attach_parameter_bounds_evidence!(card, spec)
    _design_attach_network_constraint_evidence!(card, spec)
    card["constraints"]["input_window_verified"] = window_verified
    requested_operating_points !== nothing &&
        (card["constraints"]["operating_points_verified"] = operating_points_verified)
    transition_spacing_verified &&
        (card["constraints"]["transition_spacing_verified"] = true)
    transition_order_verified &&
        (card["constraints"]["transition_order_verified"] = true)
    card["constraints"]["dynamic_range_verified"] = dynamic_range_verified
    card["constraints"]["output_feature_verified"] = output_feature_verified
    card["constraints"]["shape_verified"] = shape_verified
    card["constraints"]["min_chebyshev_radius"] = min_radius
    _design_attach_tunable_volume_constraint_evidence!(card, spec)
    supported = get!(card["constraints"], "supported_constraints", Any[])
    push!(supported, Dict(
        "path" => spec.target_source_path == "/target/behavior_spec" ? "/target/behavior_spec/program" : spec.target_source_path,
        "kind" => "reaction_order_program_feasible_region",
        "support_level" => "enforced_exact",
        "hard" => true,
        "stage" => "feasible_region",
        "solver" => window_verified ? "exact-window-siso-rop-path" : "exact-union-siso-rop-path",
        "reason" => window_verified ?
            "An augmented SISO path polyhedron with explicit finite-window input witnesses is nonempty." :
            "A full SISO path polyhedron for the requested RO program intersects $bounds_phrase and has a nonempty Chebyshev center.",
    ))
    if operating_points_verified
        operating_points_audit = _design_supported_audit_item(
            spec,
            "/target/behavior_spec/input_window/operating_points_log10",
            "finite_input_operating_points",
        )
        operating_points_audit === nothing || push!(supported, audit_to_dict(operating_points_audit))
    end
    if transition_spacing_verified
        transitions = _raw_get(spec.constraints, :transitions, Dict{String, Any}())
        push!(supported, Dict(
            "path" => "/constraints/transitions/min_spacing_decades",
            "kind" => "transition_spacing",
            "support_level" => "enforced_exact",
            "hard" => _design_hard_clause(transitions),
            "stage" => "feasible_region",
            "solver" => "exact-window-siso-rop-path",
            "reason" => "The exact-window augmented path polyhedron enforced adjacent witness input spacing at least the effective min_spacing_decades value.",
        ))
    end
    if transition_order_verified
        transition_order_audit = _design_supported_audit_item(
            spec,
            "/constraints/transitions/order",
            "transition_order",
        )
        transition_order_audit === nothing || push!(supported, audit_to_dict(transition_order_audit))
    end
    if dynamic_range_verified
        dyn = _raw_get(spec.constraints, :dynamic_range, Dict{String, Any}())
        push!(supported, Dict(
            "path" => "/constraints/dynamic_range/min_fold_change",
            "kind" => "dynamic_range",
            "support_level" => "sampled_forward",
            "hard" => _design_hard_clause(dyn),
            "stage" => "forward_verification",
            "solver" => "sampled-window-dose-response",
            "reason" => "The recommended theta was scanned across the finite input window and met min_fold_change without invalid or floor-limited samples.",
        ))
    end
    if output_feature_verified
        feature = _raw_get(spec.target, :output_feature, Dict{String, Any}())
        push!(supported, Dict(
            "path" => "/target/output_feature",
            "kind" => "output_feature",
            "support_level" => "sampled_forward",
            "hard" => _design_hard_clause(feature),
            "stage" => "forward_verification",
            "solver" => "sampled-window-dose-response",
            "reason" => "The recommended theta was scanned across the finite input window and satisfied the declared output_feature without invalid or floor-limited samples.",
        ))
    end
    if shape_verified
        shape = _raw_get(spec.target, :shape, Dict{String, Any}())
        push!(supported, Dict(
            "path" => "/target/shape",
            "kind" => "finite_dose_shape",
            "support_level" => "sampled_forward",
            "hard" => _design_hard_clause(shape),
            "stage" => "forward_verification",
            "solver" => "sampled-window-dose-response",
            "reason" => "The recommended theta was scanned across the finite input window and satisfied the declared target.shape without invalid or floor-limited samples.",
        ))
    end
    theta = card["parameter_recommendation"]["theta_star"]
    theta["status"] = "computed"
    theta["source"] = "feasible_region_path_chebyshev"
    theta["source_type"] = "exact_solver"
    theta["bounds_verified"] = true
    theta["log_qK"] = best.log_qK
    theta["background_log_qK"] = best.log_qK
    theta["background_qK_symbols"] = best.qK_symbols
    theta["kd"] = best.kd
    theta["totals"] = best.totals
    theta["path_idx"] = best.path_idx
    theta["vertex_indices"] = best.vertex_indices
    theta["predicted_RO_program"] = best.predicted_profile
    window_verified && (theta["witness_input_log10"] = witness_input_log10)
    if dynamic_range_verified
        theta["sampled_dynamic_range_log10"] = best.sampled_dynamic_range_log10
        theta["sampled_dynamic_range_fold_change"] = best.sampled_dynamic_range_fold_change
        theta["sampled_dynamic_range_points"] = best.sampled_dynamic_range_points
    end
    if output_feature_verified
        theta["sampled_output_feature"] = Dict(
            "status" => best.sampled_output_feature_status,
            "feature" => best.sampled_output_feature_name,
            "operator" => best.sampled_output_feature_operator,
            "target" => best.sampled_output_feature_target,
            "value" => best.sampled_output_feature_value,
            "range" => best.sampled_output_feature_range,
            "log10_range" => best.sampled_output_feature_log10_range,
            "fold_change" => best.sampled_output_feature_fold_change,
            "sample_points" => best.sampled_output_feature_sample_points,
            "floor_limited" => best.sampled_output_feature_floor_limited,
        )
    end
    if shape_verified
        theta["sampled_shape"] = Dict(
            "status" => best.sampled_shape_status,
            "class" => best.sampled_shape_class,
            "direction" => best.sampled_shape_direction,
            "peak_index" => best.sampled_shape_peak_index,
            "sample_points" => best.sampled_shape_sample_points,
            "floor_limited" => best.sampled_shape_floor_limited,
            "min_prominence_log10" => best.sampled_shape_min_prominence_log10,
            "prominence_left_log10" => best.sampled_shape_prominence_left_log10,
            "prominence_right_log10" => best.sampled_shape_prominence_right_log10,
        )
    end
    card["certificate_stack"] = Any[Dict(
        "grade" => certificate_grade,
        "scope" => window_verified ? "siso_rop_path_window_witness_union" : "siso_rop_path_union",
        "soundness" => "exact_polyhedral_union",
        "source" => window_verified ? "feasible_region_reaction_order_program_window" : "feasible_region_reaction_order_program",
        "supports_exact_placement" => true,
    )]
    return card
end

function _design_verified_cards_from_spec(spec::NormalizedDesignabilitySpec, records, pareto_cell_set)
    target_program = _design_exact_ro_program(spec.legacy_target_kind, spec.legacy_target)
    target_program === nothing && return Dict{String,Any}[]
    target_ro = _design_exact_single_ro(spec.legacy_target_kind, spec.legacy_target)
    input_window = _design_effective_behavior_input_window(spec)
    transition_order = _design_supported_transition_order_solver_indices(spec)
    max_exact = sync_bounded_int(
        _raw_get(spec.candidate_budget, :max_exact_placements, 0),
        "candidate_budget.max_exact_placements";
        min=0,
        max=MAX_SYNC_EXACT_PLACEMENTS,
    )
    max_exact == 0 && return Dict{String,Any}[]
    bounds = _design_effective_parameter_bounds(spec)
    min_radius = _design_min_chebyshev_radius(spec)
    min_volume = _design_min_tunable_volume(spec)
    dynamic_range = input_window === nothing ? nothing : _design_dynamic_range_constraint(spec)
    output_feature = input_window === nothing ? nothing : _design_output_feature_constraint(spec)
    shape = input_window === nothing ? nothing : _design_shape_constraint(spec)
    out = Dict{String,Any}[]
    attempts = 0
    # The caller already supplies the deterministic de-duplicated order. Do not
    # allocate a second full key table for an all-matches request.
    for rec in records
        attempts >= max_exact && break
        attempts += 1
        use_program_solver = input_window !== nothing || target_ro === nothing
        region = use_program_solver ?
            feasible_region_reaction_order_program(_design_nid_to_rules(rec.nid), rec.inp, rec.out, target_program, bounds; input_window = input_window, transition_order = transition_order, dynamic_range = dynamic_range, output_feature = output_feature, shape = shape) :
            feasible_region_single_ro(_design_nid_to_rules(rec.nid), rec.inp, rec.out, target_ro, bounds)
        region.feasible || continue
        eligible_cells = [
            cell for cell in region.cells
            if cell.chebyshev_radius + 1e-9 >= min_radius &&
               _design_tunable_volume_lower_bound(cell) + 1e-12 >= min_volume
        ]
        isempty(eligible_cells) && continue
        region = FeasibleRegionResult(
            feasible = true,
            certificate_grade = region.certificate_grade,
            cells = eligible_cells,
            reason = region.reason,
        )
        base = _design_screen_card(rec, pareto_cell_set, Dict{String,Any}(), Dict{String,Any}())
        push!(out, use_program_solver ?
            _design_exact_program_card(base, region, spec) :
            _design_exact_union_card(base, region, spec))
    end
    _design_sort_verified_cards!(out, spec.ranking_policy)
    return out
end

function _design_screen_response_from_buckets(spec::NormalizedDesignabilitySpec, matched, cells,
                                              verified, screened; max_recommended::Int,
                                              max_screened::Int, max_near_misses::Int,
                                              screened_total::Int=length(screened))
    search = _design_search_response(matched, cells)
    minimal_certificates = Any[]
    for cell in search["minimal"]
        nets = Any[]
        for nw in cell["networks"]
            push!(nets, merge(nw, Dict(
                "minimal" => true,
                "certificate_grade" => "minimal-structural-certificate",
                "reason" => "Pareto-minimal over d, r, μ; proves qualitative realizability but not tuning optimality",
            )))
        end
        push!(minimal_certificates, merge(cell, Dict("networks" => nets)))
    end
    blocked = designability_has_unsupported_hard_clause(spec)
    verified_out = blocked ? Dict{String,Any}[] : verified[1:min(length(verified), max_recommended)]
    screened_out = screened[1:min(length(screened), max_screened)]
    near_misses_out = screened[1:min(length(screened), max_near_misses)]
    return Dict(
        "schema_version" => DESIGN_SCREEN_SCHEMA_VERSION,
        "designable" => !isempty(matched),
        "verified_designable" => !isempty(verified_out),
        "n_matches" => length(matched),
        "screened_count" => length(screened),
        "eligible_count" => screened_total,
        "evaluated_count" => length(screened),
        "truncated" => length(screened) < screened_total,
        "target_kind" => spec.legacy_target_kind,
        "target" => spec.legacy_target,
        "designability_spec_normalized" => designability_spec_to_dict(spec),
        "constraint_audit" => _design_external_constraint_audit(spec),
        "verified_recommendations" => verified_out,
        "recommended" => verified_out,
        "screened_candidates" => screened_out,
        "near_misses" => near_misses_out,
        "minimal_certificates" => minimal_certificates,
        "minimal" => search["minimal"],
        "all_cells" => search["all_cells"],
        "screen_summary" => Dict(
            "verified_status" => blocked ? "blocked_by_unsupported_hard_clause" :
                                 (isempty(matched) ? "not_designable" :
                                  (isempty(verified_out) ? "screened_only" :
                                   "verified_recommendations_available")),
            "verified_count" => length(verified_out),
            "screened_proxy_count" => length(screened_out),
            "near_miss_count" => length(near_misses_out),
            "eligible_count" => screened_total,
            "evaluated_count" => length(screened),
            "truncated" => length(screened) < screened_total,
            "screen_semantics" => "verified_recommendations require exact or sampled evidence; screened_candidates are exploratory and never proof.",
        ),
        "relaxations" => Any[],
    )
end

function _design_filter_records_for_spec(records, spec::NormalizedDesignabilitySpec)
    filtered = records
    if spec.required_input !== nothing
        filtered = [rec for rec in filtered if String(rec.inp) == spec.required_input]
    end
    if spec.required_output !== nothing
        filtered = [rec for rec in filtered if String(rec.out) == spec.required_output]
    end
    network = _design_network_constraints(spec)
    filtered = [rec for rec in filtered if _design_record_satisfies_network_constraints(rec, network)]
    return filtered
end

function _design_max_verified_recommendations(candidate_budget)
    if _raw_haskey(candidate_budget, :max_verified_recommendations)
        return sync_bounded_int(
            _raw_get(candidate_budget, :max_verified_recommendations, 24),
            "candidate_budget.max_verified_recommendations";
            min=0,
            max=MAX_SYNC_DESIGN_CARDS,
        )
    end
    return sync_bounded_int(
        _raw_get(candidate_budget, :max_recommended, 24),
        "candidate_budget.max_recommended";
        min=0,
        max=MAX_SYNC_DESIGN_CARDS,
    )
end

function _design_sync_candidate_budget(candidate_budget)
    max_recommended = _design_max_verified_recommendations(candidate_budget)
    max_screened = sync_bounded_int(
        _raw_get(candidate_budget, :max_screened, 24),
        "candidate_budget.max_screened";
        min=0,
        max=MAX_SYNC_DESIGN_CARDS,
    )
    max_near_misses = sync_bounded_int(
        _raw_get(candidate_budget, :max_near_misses, max_screened),
        "candidate_budget.max_near_misses";
        min=0,
        max=MAX_SYNC_DESIGN_CARDS,
    )
    max_exact_placements = sync_bounded_int(
        _raw_get(candidate_budget, :max_exact_placements, 0),
        "candidate_budget.max_exact_placements";
        min=0,
        max=MAX_SYNC_EXACT_PLACEMENTS,
    )
    return (;
        max_recommended,
        max_screened,
        max_near_misses,
        max_exact_placements,
    )
end

function design_screen_from_spec(raw_spec)
    spec = normalize_designability_spec(raw_spec)
    budget = _design_sync_candidate_budget(spec.candidate_budget)
    max_recommended = budget.max_recommended
    max_screened = budget.max_screened
    max_near_misses = budget.max_near_misses
    if !spec.has_search_target
        return _design_screen_response_from_buckets(spec, Any[], Any[], Dict{String,Any}[], Dict{String,Any}[];
            max_recommended = max_recommended,
            max_screened = max_screened,
            max_near_misses = max_near_misses)
    end
    matched = _design_filter_records_for_spec(
        _design_matches_normalized(spec.legacy_target_kind, spec.legacy_target),
        spec,
    )
    cells = [(m.d, m.r, m.mu) for m in matched]
    pareto_cells = _design_pareto(cells)
    pareto_cell_set = Set(pareto_cells)
    records = _design_records_from_budget(matched, pareto_cells, spec.candidate_budget, spec.constraints)
    unique_records = _design_unique_records(records)
    screen_limit = min(length(unique_records), max(max_screened, max_near_misses))
    screened = [_design_downgrade_proxy_card!(_design_screen_card(rec, pareto_cell_set, Dict{String,Any}(), spec.ranking_policy))
                for rec in @view(unique_records[1:screen_limit])]
    verified = designability_has_unsupported_hard_clause(spec) ?
        Dict{String,Any}[] :
        _design_verified_cards_from_spec(spec, unique_records, pareto_cell_set)
    return _design_screen_response_from_buckets(spec, matched, cells, verified, screened;
        max_recommended = max_recommended,
        max_screened = max_screened,
        max_near_misses = max_near_misses,
        screened_total = length(unique_records))
end

function handle_validate_designability_spec(req)
    body = read_json(req)
    spec = try
        normalize_designability_spec(body)
    catch e
        return error_response("invalid DesignabilitySpec: $(sprint(showerror, e))"; status = 400)
    end
    # Validation and execution must agree on the bounded synchronous fields.
    # In particular, do not return `ok = true` for a spec that design_screen
    # will immediately reject with a budget error.
    _design_sync_candidate_budget(spec.candidate_budget)
    return json_response(Dict(
        "ok" => true,
        "schema_version" => spec.schema_version,
        "legacy_target_kind" => spec.legacy_target_kind,
        "legacy_target" => spec.legacy_target,
        "constraint_audit" => _design_external_constraint_audit(spec),
        "blocked_by_unsupported_hard_clause" => designability_has_unsupported_hard_clause(spec),
    ))
end
