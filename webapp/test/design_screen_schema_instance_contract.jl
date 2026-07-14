using Test
using JSON3
using BiocircuitsExplorerBackend

function _design_schema_pointer(root, pointer::AbstractString)
    startswith(pointer, "#/") || error("unsupported local JSON Schema reference: $pointer")
    node = root
    for raw_token in split(pointer[3:end], '/')
        token = replace(replace(raw_token, "~1" => "/"), "~0" => "~")
        node = node[token]
    end
    return node
end

function _design_schema_type_matches(instance, expected::AbstractString)
    expected == "object" && return instance isa AbstractDict
    expected == "array" && return instance isa AbstractVector
    expected == "string" && return instance isa AbstractString
    expected == "boolean" && return instance isa Bool
    expected == "integer" && return instance isa Integer && !(instance isa Bool)
    expected == "number" && return instance isa Real && !(instance isa Bool)
    expected == "null" && return instance === nothing
    return false
end

function _design_schema_instance_errors(instance, schema, root, registry;
                                        path::AbstractString = "\$")
    errors = String[]
    schema isa AbstractDict || return errors

    if haskey(schema, "\$ref")
        ref = String(schema["\$ref"])
        if startswith(ref, "#/")
            target_root = root
            target = _design_schema_pointer(root, ref)
        else
            haskey(registry, ref) || error("unregistered JSON Schema reference: $ref")
            target_root = registry[ref]
            target = target_root
        end
        append!(errors, _design_schema_instance_errors(
            instance, target, target_root, registry; path = path,
        ))
    end

    for branch in get(schema, "allOf", Any[])
        append!(errors, _design_schema_instance_errors(
            instance, branch, root, registry; path = path,
        ))
    end
    if haskey(schema, "anyOf")
        valid_count = count(branch -> isempty(_design_schema_instance_errors(
            instance, branch, root, registry; path = path,
        )), schema["anyOf"])
        valid_count >= 1 || push!(errors, "$path: does not satisfy anyOf")
    end
    if haskey(schema, "oneOf")
        valid_count = count(branch -> isempty(_design_schema_instance_errors(
            instance, branch, root, registry; path = path,
        )), schema["oneOf"])
        valid_count == 1 || push!(errors, "$path: satisfies $valid_count oneOf branches")
    end
    if haskey(schema, "not") && isempty(_design_schema_instance_errors(
        instance, schema["not"], root, registry; path = path,
    ))
        push!(errors, "$path: satisfies a forbidden `not` schema")
    end
    if haskey(schema, "if") && isempty(_design_schema_instance_errors(
        instance, schema["if"], root, registry; path = path,
    ))
        haskey(schema, "then") && append!(errors, _design_schema_instance_errors(
            instance, schema["then"], root, registry; path = path,
        ))
    end

    if haskey(schema, "type")
        expected = schema["type"]
        expected_types = expected isa AbstractVector ? String.(expected) : [String(expected)]
        any(kind -> _design_schema_type_matches(instance, kind), expected_types) || begin
            push!(errors, "$path: expected type $(join(expected_types, " or "))")
            return errors
        end
    end
    if haskey(schema, "const") && !isequal(instance, schema["const"])
        push!(errors, "$path: does not match const")
    end
    if haskey(schema, "enum") && !any(value -> isequal(instance, value), schema["enum"])
        push!(errors, "$path: is not in enum")
    end

    if instance isa AbstractDict
        properties = get(schema, "properties", Dict{String, Any}())
        for required in get(schema, "required", Any[])
            haskey(instance, String(required)) || push!(errors, "$path: missing required property $(required)")
        end
        for (key, value) in pairs(instance)
            key_string = String(key)
            child_path = "$path/$key_string"
            if haskey(properties, key_string)
                append!(errors, _design_schema_instance_errors(
                    value, properties[key_string], root, registry; path = child_path,
                ))
            elseif get(schema, "additionalProperties", true) === false
                push!(errors, "$child_path: additional property is forbidden")
            elseif get(schema, "additionalProperties", true) isa AbstractDict
                append!(errors, _design_schema_instance_errors(
                    value, schema["additionalProperties"], root, registry; path = child_path,
                ))
            end
        end
        if haskey(schema, "minProperties") && length(instance) < Int(schema["minProperties"])
            push!(errors, "$path: has fewer than $(schema["minProperties"]) properties")
        end
    elseif instance isa AbstractVector
        if haskey(schema, "items")
            for (index, value) in enumerate(instance)
                append!(errors, _design_schema_instance_errors(
                    value, schema["items"], root, registry; path = "$path/$(index - 1)",
                ))
            end
        end
        haskey(schema, "minItems") && length(instance) < Int(schema["minItems"]) &&
            push!(errors, "$path: has fewer than $(schema["minItems"]) items")
        haskey(schema, "maxItems") && length(instance) > Int(schema["maxItems"]) &&
            push!(errors, "$path: has more than $(schema["maxItems"]) items")
        if get(schema, "uniqueItems", false) === true
            for i in eachindex(instance), j in (i + 1):lastindex(instance)
                isequal(instance[i], instance[j]) && push!(errors, "$path: items are not unique")
            end
        end
    elseif instance isa AbstractString && haskey(schema, "pattern")
        occursin(Regex(String(schema["pattern"])), instance) ||
            push!(errors, "$path: does not match pattern $(schema["pattern"])")
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

@testset "Design Screen JSON Schema instance contract" begin
    schema_dir = normpath(joinpath(@__DIR__, "..", "..", "schemas"))
    screen_schema = JSON3.read(
        read(joinpath(schema_dir, "designability-screen.schema.json"), String),
        Dict{String, Any},
    )
    spec_schema = JSON3.read(
        read(joinpath(schema_dir, "designability-spec.schema.json"), String),
        Dict{String, Any},
    )
    registry = Dict{String, Any}("designability-spec.schema.json" => spec_schema)

    # Validate the JSON wire representation of a response from the production
    # producer, not a hand-maintained fixture.
    produced = BiocircuitsExplorerBackend.design_screen("sign", "+-+")
    instance = JSON3.read(JSON3.write(produced), Dict{String, Any})
    errors = _design_schema_instance_errors(
        instance, screen_schema, screen_schema, registry,
    )
    @test isempty(errors)

    public_spec_keys = Set(String.(BiocircuitsExplorerBackend.DESIGN_SPEC_ROOT_KEYS))
    @test Set(String.(keys(instance["designability_spec_normalized"]))) == public_spec_keys

    leaked_spec_field = deepcopy(instance)
    leaked_spec_field["designability_spec_normalized"]["has_search_target"] = true
    leaked_spec_errors = _design_schema_instance_errors(
        leaked_spec_field, screen_schema, screen_schema, registry,
    )
    @test any(error -> occursin("\$/designability_spec_normalized/has_search_target", error) &&
                       occursin("additional property is forbidden", error),
              leaked_spec_errors)

    leaked_screen_field = deepcopy(instance)
    leaked_screen_field["runtime_debug"] = Dict("candidate_count" => 1)
    leaked_screen_errors = _design_schema_instance_errors(
        leaked_screen_field, screen_schema, screen_schema, registry,
    )
    @test any(error -> occursin("\$/runtime_debug", error) &&
                       occursin("additional property is forbidden", error),
              leaked_screen_errors)
end
