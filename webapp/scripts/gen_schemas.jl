#!/usr/bin/env julia
# Generate JSON Schemas for the Biocircuits Explorer IR *from the Julia structs*,
# keeping the structs the single source of truth (no hand-maintained parallel
# file to drift). Field names come from `fieldnames`, allowed enum values from
# the module's `_IR_*` constants, and version strings from the schema-version
# constants.
#
#   julia --project=webapp webapp/scripts/gen_schemas.jl
#
# Writes schemas/network-ir.schema.json and schemas/design-spec.schema.json with
# deterministic (sorted, pretty) output so CI can `git diff --exit-code schemas/`
# to catch drift between the structs and the committed schemas.

using BiocircuitsExplorerBackend
const BNE = BiocircuitsExplorerBackend
using JSON3

const SCHEMA_DIR = normpath(joinpath(@__DIR__, "..", "..", "schemas"))
const BASE_ID = "https://biocircuits-explorer.com/schemas"

# (struct, field) → allowed enum value set, sourced from the module internals so
# adding an allowed value flows through automatically.
const ENUMS = Dict(
    (:SpeciesDecl, :role)           => BNE._IR_SPECIES_ROLES,
    (:ReactionDecl, :kind)          => BNE._IR_REACTION_KINDS,
    (:ParameterDistribution, :kind) => BNE._IR_PARAM_DIST_KINDS,
)

# Fields the parser requires (no usable default). Small + stable.
const REQUIRED = Dict(
    :SpeciesDecl           => ["name"],
    :ReactionDecl          => ["formula", "kd"],
    :ObservableDecl        => ["name", "expression"],
    :ParameterDistribution => ["symbol"],
    :NetworkIR             => String[],
    :DesignSpec            => String[],
    :Provenance            => String[],
)

const NESTED = Set([:SpeciesDecl, :ReactionDecl, :ObservableDecl, :ParameterDistribution, :Provenance])

# Map a Julia field type to a JSON Schema fragment.
function schema_for(T, sname::Symbol, field::Symbol)
    haskey(ENUMS, (sname, field)) &&
        return Dict("type" => "string", "enum" => sort(String.(collect(ENUMS[(sname, field)]))))
    T === String                          && return Dict("type" => "string")
    T === Float64                         && return Dict("type" => "number")
    T === Bool                            && return Dict("type" => "boolean")
    T === Symbol                          && return Dict("type" => "string")
    T === Dict{String, Any}               && return Dict("type" => "object")
    T === Union{Nothing, Float64}         && return Dict("type" => ["number", "null"])
    T === Union{Nothing, String}          && return Dict("type" => ["string", "null"])
    T === Union{Nothing, Dict{String, Any}} && return Dict("type" => ["object", "null"])
    if T <: AbstractVector
        el = eltype(T)
        elname = nameof(el)
        elname in NESTED && return Dict("type" => "array", "items" => Dict("\$ref" => "#/\$defs/$(elname)"))
        el === Dict{String, Any} && return Dict("type" => "array", "items" => Dict("type" => "object"))
        return Dict("type" => "array")
    end
    nameof(T) === :Provenance && return Dict("\$ref" => "#/\$defs/Provenance")
    return Dict()  # permissive fallback for anything unmodeled
end

function object_schema(S)
    sname = nameof(S)
    props = Dict{String, Any}()
    for (f, T) in zip(fieldnames(S), fieldtypes(S))
        props[String(f)] = schema_for(T, sname, f)
    end
    obj = Dict{String, Any}("type" => "object", "properties" => props,
                            "additionalProperties" => true)
    req = get(REQUIRED, sname, String[])
    isempty(req) || (obj["required"] = req)
    return obj
end

function network_ir_schema()
    defs = Dict{String, Any}()
    for S in (BNE.SpeciesDecl, BNE.ReactionDecl, BNE.ObservableDecl, BNE.ParameterDistribution, BNE.Provenance)
        defs[String(nameof(S))] = object_schema(S)
    end
    base = object_schema(BNE.NetworkIR)
    base["properties"]["ir_schema_version"] = Dict("const" => BNE.NETWORK_IR_SCHEMA_VERSION)
    base["properties"]["species"]["items"] = Dict("\$ref" => "#/\$defs/SpeciesDecl")
    base["properties"]["reactions"]["items"] = Dict("\$ref" => "#/\$defs/ReactionDecl")
    base["properties"]["observables"]["items"] = Dict("\$ref" => "#/\$defs/ObservableDecl")
    base["properties"]["parameter_distributions"]["items"] = Dict("\$ref" => "#/\$defs/ParameterDistribution")
    base["properties"]["provenance"] = Dict("\$ref" => "#/\$defs/Provenance")
    base["\$schema"] = "http://json-schema.org/draft-07/schema#"
    base["\$id"] = "$(BASE_ID)/network-ir.schema.json"
    base["title"] = "Biocircuits Explorer NetworkIR"
    base["description"] = "Generated from the NetworkIR Julia structs by webapp/scripts/gen_schemas.jl — edit the structs, not this file."
    base["\$defs"] = defs
    return base
end

function design_spec_schema()
    defs = Dict{String, Any}("Provenance" => object_schema(BNE.Provenance))
    base = object_schema(BNE.DesignSpec)
    base["properties"]["ir_schema_version"] = Dict("const" => BNE.DESIGN_SPEC_SCHEMA_VERSION)
    base["properties"]["provenance"] = Dict("\$ref" => "#/\$defs/Provenance")
    # Stabilized, typed inner shapes (kept in sync with parse_design_spec).
    base["properties"]["constraints"] = Dict(
        "type" => "object",
        "description" => "Hard limits. Known keys are typed; unknown keys are preserved (free-form escape hatch).",
        "properties" => Dict(
            "max_base_species"   => Dict("type" => "integer", "minimum" => 1),
            "max_reactions"      => Dict("type" => "integer", "minimum" => 1),
            "forbid_regimes"     => Dict("type" => "array", "items" => Dict("type" => "string")),
            "forbid_transitions" => Dict("type" => "array", "items" => Dict("type" => "string")),
        ),
        "additionalProperties" => true,
    )
    base["\$schema"] = "http://json-schema.org/draft-07/schema#"
    base["\$id"] = "$(BASE_ID)/design-spec.schema.json"
    base["title"] = "Biocircuits Explorer DesignSpec"
    base["description"] = "Generated from the DesignSpec Julia struct by webapp/scripts/gen_schemas.jl — edit the struct/parser, not this file."
    base["\$defs"] = defs
    return base
end

# Deterministic, sorted, pretty JSON so committed schemas diff cleanly.
function emit_json(io, v, ind = 0)
    pad = repeat("  ", ind)
    if v isa AbstractDict
        ks = sort(collect(keys(v)); by = string)
        if isempty(ks); print(io, "{}"); return; end
        println(io, "{")
        for (i, k) in enumerate(ks)
            print(io, pad, "  ", JSON3.write(string(k)), ": ")
            emit_json(io, v[k], ind + 1)
            println(io, i < length(ks) ? "," : "")
        end
        print(io, pad, "}")
    elseif v isa AbstractVector
        if isempty(v); print(io, "[]"); return; end
        println(io, "[")
        for (i, x) in enumerate(v)
            print(io, pad, "  ")
            emit_json(io, x, ind + 1)
            println(io, i < length(v) ? "," : "")
        end
        print(io, pad, "]")
    else
        print(io, JSON3.write(v))
    end
end

function write_schema(name, schema)
    mkpath(SCHEMA_DIR)
    path = joinpath(SCHEMA_DIR, name)
    open(path, "w") do io
        emit_json(io, schema)
        println(io)
    end
    println("wrote $(path)")
end

function main()
    write_schema("network-ir.schema.json", network_ir_schema())
    write_schema("design-spec.schema.json", design_spec_schema())
end

main()
