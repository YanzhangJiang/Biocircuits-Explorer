using Test
using JSON3

# This intentionally small validator covers every assertion keyword used by the
# two ROP shape schemas and their three referenced repository schemas. Keeping
# it local avoids making a hand-authored wire contract depend on an optional
# JSON Schema package while still exercising real instances, external refs, and
# cross-field evidence conditionals in the Julia 1.12 CI lane.
function _rop_schema_pointer(root, pointer::AbstractString)
    startswith(pointer, "#/") || error("unsupported JSON Schema pointer: $pointer")
    node = root
    for raw_token in split(pointer[3:end], '/')
        token = replace(replace(raw_token, "~1" => "/"), "~0" => "~")
        node = node[token]
    end
    return node
end

function _rop_schema_resolve(ref::AbstractString, root, registry)
    startswith(ref, "#/") && return root, _rop_schema_pointer(root, ref)
    parts = split(String(ref), '#'; limit=2)
    document_name = first(parts)
    fragment = length(parts) == 2 ? last(parts) : ""
    haskey(registry, document_name) || error("unregistered JSON Schema reference: $ref")
    target_root = registry[document_name]
    target = isempty(fragment) ? target_root :
             _rop_schema_pointer(target_root, "#$fragment")
    return target_root, target
end

function _rop_schema_type_matches(instance, expected::AbstractString)
    expected == "object" && return instance isa AbstractDict
    expected == "array" && return instance isa AbstractVector
    expected == "string" && return instance isa AbstractString
    expected == "boolean" && return instance isa Bool
    expected == "integer" && return instance isa Integer && !(instance isa Bool)
    expected == "number" && return instance isa Real && !(instance isa Bool)
    expected == "null" && return instance === nothing
    return false
end

function _rop_schema_errors(instance, schema, root, registry;
                            path::AbstractString="\$")
    errors = String[]
    schema isa AbstractDict || return errors

    if haskey(schema, "\$ref")
        target_root, target = _rop_schema_resolve(String(schema["\$ref"]), root, registry)
        append!(errors, _rop_schema_errors(
            instance, target, target_root, registry; path=path))
    end
    for branch in get(schema, "allOf", Any[])
        append!(errors, _rop_schema_errors(instance, branch, root, registry; path=path))
    end
    if haskey(schema, "anyOf")
        valid_count = count(branch -> isempty(_rop_schema_errors(
            instance, branch, root, registry; path=path)), schema["anyOf"])
        valid_count >= 1 || push!(errors, "$path: does not satisfy anyOf")
    end
    if haskey(schema, "oneOf")
        valid_count = count(branch -> isempty(_rop_schema_errors(
            instance, branch, root, registry; path=path)), schema["oneOf"])
        valid_count == 1 || push!(errors, "$path: satisfies $valid_count oneOf branches")
    end
    if haskey(schema, "not") && isempty(_rop_schema_errors(
        instance, schema["not"], root, registry; path=path))
        push!(errors, "$path: satisfies a forbidden `not` schema")
    end
    if haskey(schema, "if")
        condition_matches = isempty(_rop_schema_errors(
            instance, schema["if"], root, registry; path=path))
        if condition_matches && haskey(schema, "then")
            append!(errors, _rop_schema_errors(
                instance, schema["then"], root, registry; path=path))
        elseif !condition_matches && haskey(schema, "else")
            append!(errors, _rop_schema_errors(
                instance, schema["else"], root, registry; path=path))
        end
    end

    if haskey(schema, "type")
        raw_types = schema["type"]
        expected_types = raw_types isa AbstractVector ? String.(raw_types) : [String(raw_types)]
        if !any(kind -> _rop_schema_type_matches(instance, kind), expected_types)
            push!(errors, "$path: expected type $(join(expected_types, " or "))")
            return errors
        end
    end
    haskey(schema, "const") && !isequal(instance, schema["const"]) &&
        push!(errors, "$path: does not match const")
    haskey(schema, "enum") &&
        !any(value -> isequal(instance, value), schema["enum"]) &&
        push!(errors, "$path: is not in enum")

    if instance isa AbstractDict
        properties = get(schema, "properties", Dict{String, Any}())
        for required in get(schema, "required", Any[])
            haskey(instance, String(required)) ||
                push!(errors, "$path: missing required property $(required)")
        end
        for (key, value) in pairs(instance)
            key_string = String(key)
            child_path = "$path/$key_string"
            if haskey(properties, key_string)
                append!(errors, _rop_schema_errors(
                    value, properties[key_string], root, registry; path=child_path))
            elseif get(schema, "additionalProperties", true) === false
                push!(errors, "$child_path: additional property is forbidden")
            elseif get(schema, "additionalProperties", true) isa AbstractDict
                append!(errors, _rop_schema_errors(
                    value, schema["additionalProperties"], root, registry;
                    path=child_path))
            end
        end
        haskey(schema, "minProperties") && length(instance) < Int(schema["minProperties"]) &&
            push!(errors, "$path: has fewer than $(schema["minProperties"]) properties")
    elseif instance isa AbstractVector
        if haskey(schema, "items")
            for (index, value) in enumerate(instance)
                append!(errors, _rop_schema_errors(
                    value, schema["items"], root, registry; path="$path/$(index - 1)"))
            end
        end
        haskey(schema, "minItems") && length(instance) < Int(schema["minItems"]) &&
            push!(errors, "$path: has fewer than $(schema["minItems"]) items")
        haskey(schema, "maxItems") && length(instance) > Int(schema["maxItems"]) &&
            push!(errors, "$path: has more than $(schema["maxItems"]) items")
        if get(schema, "uniqueItems", false) === true
            for left in eachindex(instance), right in (left + 1):lastindex(instance)
                isequal(instance[left], instance[right]) &&
                    push!(errors, "$path: items are not unique")
            end
        end
    elseif instance isa AbstractString
        haskey(schema, "pattern") &&
            !occursin(Regex(String(schema["pattern"])), instance) &&
            push!(errors, "$path: does not match pattern $(schema["pattern"])")
        haskey(schema, "minLength") && length(instance) < Int(schema["minLength"]) &&
            push!(errors, "$path: is shorter than minLength $(schema["minLength"])")
    elseif instance isa Real && !(instance isa Bool)
        haskey(schema, "minimum") && instance < schema["minimum"] &&
            push!(errors, "$path: is below minimum $(schema["minimum"])")
        haskey(schema, "maximum") && instance > schema["maximum"] &&
            push!(errors, "$path: is above maximum $(schema["maximum"])")
        haskey(schema, "exclusiveMinimum") && instance <= schema["exclusiveMinimum"] &&
            push!(errors, "$path: is not above exclusiveMinimum $(schema["exclusiveMinimum"])")
    end
    return errors
end

_rop_shape_hash(char::AbstractString) = repeat(char, 64)

function _rop_shape_full_network()
    return Dict{String, Any}(
        "ir_schema_version" => "bne-ir/v1.0.0",
        "label" => "schema-contract-network",
        "species" => [
            Dict("name" => "A", "role" => "free"),
            Dict("name" => "B", "role" => "free"),
            Dict("name" => "C_A_B", "role" => "complex"),
        ],
        "reactions" => [Dict(
            "formula" => "A + B <-> C_A_B",
            "kd" => 1.0,
            "kind" => "binding",
            "reversible" => true,
            "metadata" => Dict{String, Any}(),
        )],
        "observables" => [Dict(
            "name" => "C_A_B", "expression" => "C_A_B",
            "unit" => "concentration", "metadata" => Dict{String, Any}(),
        )],
        "parameter_distributions" => [Dict(
            "symbol" => "tA", "kind" => "loguniform", "value" => nothing,
            "log_min" => -3.0, "log_max" => 3.0, "mu" => nothing,
            "sigma" => nothing, "metadata" => Dict{String, Any}(),
        )],
        "compartments" => Any[],
        "provenance" => Dict(
            "created_at" => "2026-07-11T00:00:00Z",
            "created_by" => "rop-shape-schema-contract",
            "source" => "test_fixture",
            "parent_ir_hash" => nothing,
            "notes" => "schema-only fixture",
        ),
        "extensions" => Dict{String, Any}(),
    )
end

function _rop_shape_designability_spec()
    program = [1, 0, -1, 0, 1, 0, -1]
    return Dict{String, Any}(
        "schema_version" => "bne-designability/v1.0.0",
        "source" => Dict("kind" => "test_fixture"),
        "target" => Dict("behavior_spec" => Dict(
            "input" => "tA",
            "output" => "C_A_B",
            "feature_space" => "reaction_order",
            "program" => [Dict(
                "kind" => "reaction_order", "operator" => "=", "value" => value,
            ) for value in program],
            "input_window" => Dict(
                "input_log10" => [-3.0, 3.0],
                "hard" => true,
                "min_spacing_decades" => 0.1,
            ),
        )),
        "constraints" => Dict("parameter_bounds" => Dict(
            "basis" => "log10_qK",
            "by_class" => Dict("kd" => [-3.0, 3.0], "total" => [-3.0, 3.0]),
        )),
    )
end

function _rop_shape_intents()
    return Dict{String, Any}[
        Dict(
            "id" => "broaden-both-ears", "kind" => "broaden",
            "left_span_steps" => [0, 2], "right_span_steps" => [4, 6],
            "shared_magnitude" => true,
        ),
        Dict(
            "id" => "separate-ear-tops", "kind" => "separate",
            "steps" => [1, 5], "preserve_midpoint_tolerance_log10" => 0.2,
        ),
        Dict(
            "id" => "widen-center", "kind" => "widen_center",
            "steps" => [2, 4], "anchor_step" => 3,
            "anchor_tolerance_log10" => 0.2,
        ),
        Dict(
            "id" => "translate-right-ear", "kind" => "translate_group",
            "group_steps" => [4, 5, 6], "preserve_steps" => [0, 1, 2, 3],
            "preserve_tolerance_log10" => 0.1, "sense" => "positive",
            "shared_shift" => true,
        ),
        Dict(
            "id" => "canonical-linear", "kind" => "linear_witness",
            "constraints" => [Dict(
                "id" => "midpoint-upper",
                "terms" => [
                    Dict("step" => 1, "coefficient" => 0.5),
                    Dict("step" => 5, "coefficient" => 0.5),
                ],
                "operator" => "<=", "rhs_log10" => 0.2, "hard" => true,
            )],
            "objective" => Dict(
                "id" => "separation", "sense" => "maximize",
                "terms" => [
                    Dict("step" => 5, "coefficient" => 1.0),
                    Dict("step" => 1, "coefficient" => -1.0),
                ],
            ),
        ),
    ]
end

function _rop_shape_request(; legacy::Bool=false, intent=_rop_shape_intents()[5])
    network = legacy ? Dict{String, Any}(
        "label" => "legacy-schema-contract-network",
        "reactions" => ["A + B <-> C_A_B"],
        "kd" => [1.0],
        "input_symbols" => ["tA"],
        "output_symbols" => ["C_A_B"],
    ) : _rop_shape_full_network()
    return Dict{String, Any}(
        "schema_version" => "bne-rop-shape-optimize-request/v1.0.0",
        "network" => network,
        "expected_network_ir_hash" => legacy ? nothing : _rop_shape_hash("a"),
        "designability_spec" => _rop_shape_designability_spec(),
        "reference" => Dict(
            "reference_hash" => _rop_shape_hash("b"),
            "artifact_ref" => "sha256:$( _rop_shape_hash("b") )",
            "network_ir_hash" => _rop_shape_hash("a"),
            "operating_points_log10" => [-2.5, -1.8, -1.0, 0.0, 1.0, 1.8, 2.5],
            "kd" => [1.0],
            "totals" => Dict("tB" => 1.0),
            "path_identity" => "path:1",
            "cell_id" => "sha256:$( _rop_shape_hash("c") )",
        ),
        "edit_intent" => deepcopy(intent),
        "optimization" => Dict(
            "minimum_parameter_margin" => 0.1,
            "effect_tolerance" => 0.02,
        ),
        "work_budget" => Dict(
            "max_paths" => 16,
            "max_cells" => 128,
            "max_replays" => 2,
            "require_exhaustive" => true,
        ),
        "replay" => Dict(
            "input_window_log10" => [-3.0, 3.0],
            "sample_points" => 11,
            "require_complete" => true,
            "store_curve" => true,
            "metrics" => [Dict(
                "kind" => "two_peak", "min_prominence_log10" => 0.1,
            )],
        ),
    )
end

function _rop_shape_pass_metrics()
    return Dict{String, Any}(
        "schema_version" => "bne-rop-shape-replay/v1.0.0",
        "status" => "pass",
        "reason" => "two complete sampled peaks met the declared prominence",
        "sample_points" => 11,
        "complete" => true,
        "pass" => true,
        "peak_candidate_count" => 2,
        "peak_indices" => [3, 9],
        "peak_input_log10" => [-1.8, 1.8],
        "peak_output_log10" => [1.0, 1.0],
        "valley_index" => 6,
        "valley_input_log10" => 0.0,
        "valley_output_log10" => 0.2,
        "peak_separation_log10" => 3.6,
        "left_prominence_log10" => 0.8,
        "right_prominence_log10" => 0.8,
        "left_half_prominence_width_log10" => 1.0,
        "right_half_prominence_width_log10" => 1.0,
        "central_half_prominence_interval_log10" => 1.6,
        "half_prominence_crossings_log10" => [-2.4, -1.2, 1.2, 2.4],
        "min_prominence_log10" => 0.1,
    )
end

function _rop_shape_response()
    intent = _rop_shape_intents()[1]
    normalized_request = _rop_shape_request(intent=intent)
    xs = collect(range(-3.0, 3.0; length=11))
    ys = [[0.1 + abs(sin(value))] for value in xs]
    return Dict{String, Any}(
        "schema_version" => "bne-rop-shape-optimization/v1.0.0",
        "request_hash" => _rop_shape_hash("d"),
        "normalized_request" => normalized_request,
        "fixed_topology" => Dict(
            "normalized_network" => _rop_shape_full_network(),
            "network_ir_hash" => _rop_shape_hash("a"),
            "network_canonical_code" => "[1]+[2]<->[1,2]",
            "network_identity_semantics" => "canonical_code_available",
            "input" => "tA",
            "output" => "C_A_B",
            "topology_preserved" => true,
        ),
        "geometric_status" => "global_optimal_over_declared_cells",
        "geometric_status_message" => "global epsilon-lexicographic optimum",
        "feasible" => true,
        "coverage" => Dict(
            "eligible_path_count" => 4,
            "evaluated_path_count" => 4,
            "eligible_cell_count" => 16,
            "evaluated_cell_count" => 16,
            "feasible_cell_count" => 3,
            "replay_candidate_count" => 2,
            "replayed_count" => 1,
            "truncated" => false,
            "truncation_reasons" => String[],
        ),
        "compiled_edit" => Dict(
            "compiler_version" => "bne-rop-shape-compiler/v1.0.0",
            "source_intent_id" => "broaden-both-ears",
            "intent" => deepcopy(intent),
            "constraints" => Any[],
            "objective" => Dict(
                "id" => "broaden-both-ears",
                "kind" => "max_min_linear_operating_point_improvement",
                "sense" => "maximize",
                "groups" => [
                    Dict(
                        "id" => "left-ear",
                        "terms" => [
                            Dict("step" => 2, "coefficient" => 1.0),
                            Dict("step" => 0, "coefficient" => -1.0),
                        ],
                        "reference_value" => 1.5,
                    ),
                    Dict(
                        "id" => "right-ear",
                        "terms" => [
                            Dict("step" => 6, "coefficient" => 1.0),
                            Dict("step" => 4, "coefficient" => -1.0),
                        ],
                        "reference_value" => 1.5,
                    ),
                ],
                "reference_value" => 0.0,
            ),
            "direction" => Dict(
                "values" => [-0.5, 0.0, 0.5, 0.0, -0.5, 0.0, 0.5],
                "l2_norm" => 1.0,
                "normalization" => "not_normalized",
                "alpha_units" => "declared_raw_direction_scale",
            ),
            "auxiliary_coordinates" => ["alpha"],
            "index_basis" => "zero_based_program_step",
            "units" => "log10_input",
        ),
        "selected" => Dict(
            "cell_id" => "sha256:$( _rop_shape_hash("c") )",
            "path_identity" => "path:1",
            "path_idx" => 1,
            "witness_identity" => ["step:$idx:vertex:$(idx + 1)" for idx in 0:6],
            "witness_vertex_indices" => [1, 2, 3, 4, 5, 6, 7],
            "full_path_vertex_indices" => [1, 2, 3, 4, 5, 6, 7],
            "predicted_profile" => [1.0, 0.0, -1.0, 0.0, 1.0, 0.0, -1.0],
            "witness_input_log10" => [-2.6, -1.9, -0.9, 0.0, 0.9, 1.9, 2.6],
            "background_log_qK" => Dict(
                "symbols" => ["log_q_B", "log_K_1"], "values" => [0.0, 0.0],
            ),
            "kd" => [1.0],
            "totals" => Dict("tB" => 1.0),
            "primary_effect" => Dict(
                "objective_id" => "broaden-both-ears",
                "sense" => "maximize",
                "value" => 0.1,
                "effect_bound" => 0.08,
                "semantics" => "closed_polyhedral_support_limit_and_secondary_realization",
                "effect_kind" => "balanced_minimum_improvement",
                "reference_value" => 0.0,
                "closure_support_value" => 0.1,
                "cell_primary_value" => 0.1,
                "selected_value" => 0.1,
                "closure_support_improvement" => 0.1,
                "selected_improvement" => 0.1,
                "effect_tolerance" => 0.02,
            ),
            "parameter_margin" => Dict(
                "value" => 0.2,
                "basis" => "equality_feasible_log10_qK_subspace",
                "coordinate_basis" => "unweighted_euclidean_log10_qK",
                "dimension" => 1,
                "equality_rank" => 1,
                "coordinates" => ["log_q_B", "log_K_1"],
                "basis_matrix" => [[1.0], [0.0]],
                "rank_relative_tolerance" => 1.0e-8,
                "rank_absolute_threshold" => 1.0e-10,
                "zero_dimensional_convention" => "not_applicable",
            ),
            "active_constraints" => [Dict(
                "row_id" => "input-window-lower",
                "row_kind" => "cell_inequality",
                "point_residual" => -0.2,
                "ball_residual" => 0.0,
                "normalized_residual" => 0.0,
                "dual" => nothing,
                "shadow_price" => nothing,
                "shadow_price_semantics" =>
                    "derivative_of_objective_value_with_respect_to_compiled_rhs",
            )],
            "solver" => Dict(
                "name" => "Clarabel",
                "version" => "0.11",
                "termination_status" => "OPTIMAL",
                "validation_tolerance" => 1.0e-7,
                "active_tolerance" => 1.0e-7,
                "rank_tolerance" => 1.0e-9,
                "primary_termination_status" => "OPTIMAL",
                "primary_message" => "primary optimum",
                "secondary_message" => "conditional margin optimum",
                "core_status" => "optimal",
            ),
        ),
        "directional_request_interval" => Dict(
            "direction" => [-0.5, 0.0, 0.5, 0.0, -0.5, 0.0, 0.5],
            "direction_l2_norm" => 1.0,
            "normalization" => "not_normalized",
            "alpha_units" => "declared_raw_direction_scale",
            "cell_intervals" => [Dict(
                "cell_id" => "sha256:$( _rop_shape_hash("c") )",
                "path_identity" => "path:1",
                "status" => "optimal",
                "lower_status" => "optimal",
                "upper_status" => "optimal",
                "alpha_min" => -0.2,
                "alpha_max" => 0.3,
                "message" => "bounded directional interval",
            )],
            "union_intervals" => [Dict(
                "alpha_min" => -0.2,
                "alpha_max" => 0.3,
                "lower_unbounded" => false,
                "upper_unbounded" => false,
            )],
            "complete_over_evaluated_cells" => true,
            "numerical_error_count" => 0,
            "scope" => "declared_cells",
        ),
        "replay" => Dict(
            "status" => "pass",
            "request" => Dict(
                "endpoint" => "/api/v1/placer_curve",
                "method" => "POST",
                "body" => Dict(
                    "rules" => ["A + B <-> C_A_B"],
                    "input_sym" => "tA",
                    "output_sym" => "C_A_B",
                    "kd" => [1.0],
                    "totals" => Dict("tB" => 1.0),
                    "param_min" => -3.0,
                    "param_max" => 3.0,
                    "n_points" => 11,
                ),
            ),
            "request_hash" => _rop_shape_hash("e"),
            "curve" => Dict(
                "param_values" => xs,
                "output_traj" => ys,
                "valid" => fill(true, 11),
                "partial" => false,
            ),
            "metrics" => _rop_shape_pass_metrics(),
            "result_hash" => _rop_shape_hash("f"),
            "complete" => true,
            "pass" => true,
        ),
        "certificate_grade" => "exact-window-siso-rop-path-optimization",
        "geometric_evidence_grade" => "exact_path_polyhedral",
        "finite_replay_evidence_grade" => "sampled-forward-complete",
        "solver_contract" => Dict(
            "lp_backend" => "Clarabel",
            "objective_policy" =>
                "global_epsilon_lexicographic_effect_then_parameter_margin",
            "parameter_margin_basis" => "equality_feasible_log10_qK_subspace",
            "effect_limit_semantics" => "closed_polyhedral_support_limit",
            "active_row_shadow_price_semantics" =>
                "objective_derivative_with_respect_to_compiled_rhs_not_primal_parameter_derivative",
            "compiler_version" => "bne-rop-shape-compiler/v1.0.0",
        ),
        "result_hash" => _rop_shape_hash("1"),
        "artifact" => Dict(
            "artifact_schema_version" => "bne-result/v1.0.0",
            "kind" => "rop_shape_optimize",
            "input_hashes" => Dict(
                "network_ir_hash" => _rop_shape_hash("a"),
                "request_hash" => _rop_shape_hash("d"),
            ),
            "algorithm" => Dict(
                "name" => "rop_shape_optimize",
                "version" => "1.0.0",
                "config_hash" => _rop_shape_hash("d"),
            ),
            "warnings" => String[],
            "created_at" => "2026-07-11T00:00:00Z",
        ),
        "warnings" => String[],
    )
end

@testset "ROP shape JSON Schema instances" begin
    schema_dir = normpath(joinpath(@__DIR__, "..", "..", "schemas"))
    load_schema(name) = JSON3.read(
        read(joinpath(schema_dir, name), String), Dict{String, Any})
    request_schema = load_schema("rop-shape-optimize-request.schema.json")
    response_schema = load_schema("rop-shape-optimization.schema.json")
    registry = Dict{String, Any}(
        "network-ir.schema.json" => load_schema("network-ir.schema.json"),
        "designability-spec.schema.json" => load_schema("designability-spec.schema.json"),
        "result-artifact.schema.json" => load_schema("result-artifact.schema.json"),
        "rop-shape-optimize-request.schema.json" => request_schema,
        "rop-shape-optimization.schema.json" => response_schema,
    )
    validate(instance, schema) = _rop_schema_errors(instance, schema, schema, registry)

    @test request_schema["\$schema"] ==
          "https://json-schema.org/draft/2020-12/schema"

    for intent in _rop_shape_intents()
        @test isempty(validate(_rop_shape_request(intent=intent), request_schema))
    end
    @test isempty(validate(_rop_shape_request(legacy=true), request_schema))
    sparse_full_ir = _rop_shape_request()
    sparse_full_ir["network"]["species"] = Any[]
    sparse_full_ir["network"]["observables"] = Any[]
    sparse_full_ir["network"]["parameter_distributions"] = Any[]
    @test isempty(validate(sparse_full_ir, request_schema))

    invalid_requests = Pair{String, Dict{String, Any}}[]
    extra_root = _rop_shape_request(); extra_root["debug"] = true
    push!(invalid_requests, "unknown root field" => extra_root)
    rules_only = _rop_shape_request(legacy=true)
    rules_only["network"] = Dict("reactions" => ["A + B <-> C_A_B"])
    push!(invalid_requests, "rules-only legacy network" => rules_only)
    full_without_hash = _rop_shape_request(); full_without_hash["expected_network_ir_hash"] = nothing
    push!(invalid_requests, "full NetworkIR without expected hash" => full_without_hash)
    unknown_intent = _rop_shape_request(); unknown_intent["edit_intent"] = Dict(
        "id" => "guess", "kind" => "move_some_stuff")
    push!(invalid_requests, "unknown intent" => unknown_intent)
    false_shared = _rop_shape_request(intent=_rop_shape_intents()[1])
    false_shared["edit_intent"]["shared_magnitude"] = false
    push!(invalid_requests, "unbalanced broaden" => false_shared)
    zero_term = _rop_shape_request()
    zero_term["edit_intent"]["objective"]["terms"][1]["coefficient"] = 0.0
    push!(invalid_requests, "zero objective term" => zero_term)
    relation_alias = _rop_shape_request()
    constraint = relation_alias["edit_intent"]["constraints"][1]
    delete!(constraint, "operator"); constraint["relation"] = "<="
    push!(invalid_requests, "noncanonical relation key" => relation_alias)
    small_replay = _rop_shape_request(); small_replay["replay"]["sample_points"] = 10
    push!(invalid_requests, "undersized replay" => small_replay)
    no_curve = _rop_shape_request(); no_curve["replay"]["store_curve"] = false
    push!(invalid_requests, "curve storage disabled" => no_curve)
    zero_budget = _rop_shape_request(); zero_budget["work_budget"]["max_cells"] = 0
    push!(invalid_requests, "zero cell budget" => zero_budget)
    unbounded_job_budget = _rop_shape_request()
    unbounded_job_budget["work_budget"]["max_cells"] = 10_001
    push!(invalid_requests, "cell budget above finite job cap" => unbounded_job_budget)
    empty_totals = _rop_shape_request(); empty!(empty_totals["reference"]["totals"])
    push!(invalid_requests, "empty inline reference totals" => empty_totals)
    caller_solver_policy = _rop_shape_request()
    caller_solver_policy["replay"]["solver_policy"] = Dict(
        "engine" => "caller-selected-solver")
    push!(invalid_requests, "caller-authored replay solver policy" => caller_solver_policy)
    ambiguous_parameter_margin = _rop_shape_request()
    ambiguous_parameter_margin["designability_spec"]["constraints"]["robustness"] =
        Dict("min_chebyshev_radius" => 0.1, "hard" => true)
    push!(invalid_requests,
          "embedded ambiguous min_chebyshev_radius" => ambiguous_parameter_margin)
    for (description, request) in invalid_requests
        @testset "$description" begin
            @test !isempty(validate(request, request_schema))
        end
    end

    response = _rop_shape_response()
    response_errors = validate(response, response_schema)
    @test isempty(response_errors)
    positional_identity = deepcopy(response)
    positional_identity["fixed_topology"]["network_canonical_code"] = nothing
    positional_identity["fixed_topology"]["network_identity_semantics"] =
        "positional_content_hash_only"
    @test isempty(validate(positional_identity, response_schema))

    invalid_responses = Pair{String, Dict{String, Any}}[]
    extra_response = deepcopy(response); extra_response["debug"] = true
    push!(invalid_responses, "unknown response field" => extra_response)
    legacy_normalized = deepcopy(response)
    legacy_normalized["normalized_request"] = _rop_shape_request(legacy=true)
    push!(invalid_responses, "legacy network retained after normalization" => legacy_normalized)
    wrong_auxiliary = deepcopy(response)
    wrong_auxiliary["compiled_edit"]["auxiliary_coordinates"] = String[]
    push!(invalid_responses, "missing max-min auxiliary" => wrong_auxiliary)
    truncated_global = deepcopy(response)
    truncated_global["coverage"]["truncated"] = true
    truncated_global["coverage"]["truncation_reasons"] = ["max_cells"]
    push!(invalid_responses, "truncated global optimum" => truncated_global)
    no_selection = deepcopy(response); no_selection["selected"] = nothing
    push!(invalid_responses, "optimal response without selection" => no_selection)
    wrong_margin = deepcopy(response)
    wrong_margin["selected"]["parameter_margin"]["basis"] = "augmented_parameter_plus_witness"
    push!(invalid_responses, "augmented radius labeled parameter margin" => wrong_margin)
    partial_pass = deepcopy(response); partial_pass["replay"]["curve"]["partial"] = true
    push!(invalid_responses, "partial replay passed" => partial_pass)
    invalid_sample_pass = deepcopy(response)
    invalid_sample_pass["replay"]["curve"]["valid"][4] = false
    push!(invalid_responses, "invalid replay sample passed" => invalid_sample_pass)
    incomplete_metrics = deepcopy(response)
    delete!(incomplete_metrics["replay"]["metrics"], "peak_indices")
    push!(invalid_responses, "passing replay missing measurements" => incomplete_metrics)
    flat_replay = deepcopy(response)
    flat_replay["replay"]["request"] = flat_replay["replay"]["request"]["body"]
    push!(invalid_responses, "non-executable flat replay request" => flat_replay)
    wrong_artifact = deepcopy(response); wrong_artifact["artifact"]["kind"] = "design_screen"
    push!(invalid_responses, "wrong artifact kind" => wrong_artifact)
    wrong_identity_semantics = deepcopy(response)
    wrong_identity_semantics["fixed_topology"]["network_identity_semantics"] =
        "positional_content_hash_only"
    push!(invalid_responses, "canonical code with positional-only identity" =>
        wrong_identity_semantics)
    wrong_replay_grade = deepcopy(response)
    wrong_replay_grade["finite_replay_evidence_grade"] = "not_run"
    push!(invalid_responses, "passing replay downgraded to not-run evidence" =>
        wrong_replay_grade)
    evaluated_without_truncation = deepcopy(response)
    evaluated_without_truncation["geometric_status"] = "best_over_evaluated_cells"
    push!(invalid_responses, "evaluated-only optimum without truncation" =>
        evaluated_without_truncation)
    wrong_zero_dimensional_convention = deepcopy(response)
    wrong_zero_dimensional_convention["selected"]["parameter_margin"][
        "zero_dimensional_convention"] = "radius_zero"
    push!(invalid_responses, "positive-dimensional margin labeled radius-zero" =>
        wrong_zero_dimensional_convention)
    stale_not_run_payload = deepcopy(response)
    stale_not_run_payload["replay"]["status"] = "not_run"
    stale_not_run_payload["finite_replay_evidence_grade"] = "not_run"
    stale_not_run_payload["replay"]["complete"] = false
    stale_not_run_payload["replay"]["pass"] = false
    push!(invalid_responses, "not-run replay retaining a stale curve" => stale_not_run_payload)
    for (description, invalid_response) in invalid_responses
        @testset "$description" begin
            @test !isempty(validate(invalid_response, response_schema))
        end
    end
end
