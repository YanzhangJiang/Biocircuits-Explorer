#!/usr/bin/env julia
# Generate JSON Schemas for the Biocircuits Explorer IR *from the Julia structs*,
# keeping the structs the single source of truth (no hand-maintained parallel
# file to drift). Field names come from `fieldnames`, allowed enum values from
# the module's `_IR_*` constants, and version strings from the schema-version
# constants.
#
#   julia --project=webapp webapp/scripts/gen_schemas.jl --check
#   julia --project=webapp webapp/scripts/gen_schemas.jl --write
#
# Renders schemas/network-ir.schema.json and schemas/design-spec.schema.json with
# deterministic (sorted, pretty) output. `--check` compares bytes without writing;
# `--write` updates changed files atomically. With no mode flag the historical
# write behavior is preserved.

using BiocircuitsExplorerBackend
const BNE = BiocircuitsExplorerBackend
using JSON3

const SCHEMA_DIR = normpath(joinpath(@__DIR__, "..", "..", "schemas"))
const BASE_ID = "https://biocircuits-explorer.com/schemas"
const GENERATED_SCHEMA_NAMES = ("design-spec.schema.json", "network-ir.schema.json")

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

function render_schema(schema)
    io = IOBuffer()
    emit_json(io, schema)
    write(io, '\n')
    return String(take!(io))
end

"Return every generated schema as deterministic, in-memory UTF-8 bytes."
function rendered_schemas()
    rendered = Dict(
        "network-ir.schema.json" => render_schema(network_ir_schema()),
        "design-spec.schema.json" => render_schema(design_spec_schema()),
    )
    @assert Tuple(sort!(collect(keys(rendered)))) == GENERATED_SCHEMA_NAMES
    return rendered
end

function schema_names(schema_dir::AbstractString, overrides = Dict{String, String}())
    names = Set{String}()
    if isdir(schema_dir)
        union!(names, filter(name -> endswith(name, ".schema.json"), readdir(schema_dir)))
    end
    union!(names, keys(overrides))
    return sort!(collect(names))
end

"Validate that every schema has one nonempty, repository-unique `\$id`."
function schema_id_errors(schema_dir::AbstractString;
                          overrides::AbstractDict{String, String} = Dict{String, String}())
    errors = String[]
    owners = Dict{String, String}()
    for name in schema_names(schema_dir, overrides)
        path = joinpath(schema_dir, name)
        source = if haskey(overrides, name)
            overrides[name]
        elseif isfile(path)
            read(path, String)
        else
            push!(errors, "$(path): schema file is missing")
            continue
        end

        parsed = try
            JSON3.read(source)
        catch err
            push!(errors, "$(path): invalid JSON ($(sprint(showerror, err)))")
            continue
        end
        id = get(parsed, "\$id", nothing)
        if !(id isa AbstractString) || isempty(strip(id))
            push!(errors, "$(path): missing or empty \$id")
            continue
        end
        id = String(id)
        if haskey(owners, id)
            push!(errors, "$(path): duplicate \$id $(repr(id)); already used by $(joinpath(schema_dir, owners[id]))")
        else
            owners[id] = name
        end
    end
    return errors
end

function first_difference(actual::String, expected::String)
    actual_lines = split(actual, '\n'; keepempty = true)
    expected_lines = split(expected, '\n'; keepempty = true)
    n = max(length(actual_lines), length(expected_lines))
    for line in 1:n
        a = line <= length(actual_lines) ? actual_lines[line] : "<end of file>"
        e = line <= length(expected_lines) ? expected_lines[line] : "<end of file>"
        a == e || return (line, a, e)
    end
    return nothing
end

"Read-only byte comparison of committed files against deterministic renders."
function check_schemas(schema_dir::AbstractString; out::IO = stdout, err::IO = stderr)
    rendered = rendered_schemas()
    ok = true

    for problem in schema_id_errors(schema_dir)
        println(err, "schema identity error: ", problem)
        ok = false
    end

    for name in sort!(collect(keys(rendered)))
        path = joinpath(schema_dir, name)
        if !isfile(path)
            println(err, "schema drift: missing $(path)")
            ok = false
            continue
        end
        actual = read(path, String)
        expected = rendered[name]
        actual == expected && continue
        difference = first_difference(actual, expected)
        println(err, "schema drift: $(path) differs from the deterministic generator")
        if difference !== nothing
            line, committed, generated = difference
            println(err, "  first difference at line $(line)")
            println(err, "  - committed: ", repr(committed))
            println(err, "  + generated: ", repr(generated))
        end
        ok = false
    end

    if ok
        println(out, "schema check passed: $(length(rendered)) generated files match; all schema \$id values are nonempty and unique")
    else
        println(err, "regenerate with: julia --project=webapp webapp/scripts/gen_schemas.jl --write")
    end
    return ok
end

function atomic_write(path::AbstractString, contents::String)
    mkpath(dirname(path))
    temp_path, io = mktemp(dirname(path); cleanup = false)
    try
        write(io, contents)
        flush(io)
        close(io)
        mode = isfile(path) ? stat(path).mode & 0o777 : 0o644
        chmod(temp_path, mode)
        # Same-directory rename is an atomic replacement on supported platforms.
        Base.Filesystem.rename(temp_path, path)
    catch
        isopen(io) && close(io)
        ispath(temp_path) && rm(temp_path; force = true)
        rethrow()
    end
    return path
end

function write_schemas(schema_dir::AbstractString; out::IO = stdout, err::IO = stderr)
    rendered = rendered_schemas()
    identity_errors = schema_id_errors(schema_dir; overrides = rendered)
    if !isempty(identity_errors)
        foreach(problem -> println(err, "schema identity error: ", problem), identity_errors)
        println(err, "no files written")
        return false
    end

    changed = 0
    for name in sort!(collect(keys(rendered)))
        path = joinpath(schema_dir, name)
        if isfile(path) && read(path, String) == rendered[name]
            println(out, "unchanged $(path)")
            continue
        end
        atomic_write(path, rendered[name])
        println(out, "wrote $(path)")
        changed += 1
    end
    println(out, "schema write complete: $(changed) changed, $(length(rendered) - changed) unchanged")
    return true
end

function usage(io::IO = stdout)
    println(io, "Usage: julia --project=webapp webapp/scripts/gen_schemas.jl [--check|--write] [--schema-dir PATH]")
    println(io, "  --check       compare generated bytes without modifying files")
    println(io, "  --write       atomically update changed generated files (default)")
    println(io, "  --schema-dir  override the schema directory (primarily for tests)")
end

function parse_cli(args)
    mode = nothing
    schema_dir = SCHEMA_DIR
    help = false
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--check" || arg == "--write"
            next_mode = arg == "--check" ? :check : :write
            mode === nothing || mode == next_mode || throw(ArgumentError("--check and --write are mutually exclusive"))
            mode = next_mode
        elseif arg == "--schema-dir"
            i == length(args) && throw(ArgumentError("--schema-dir requires a path"))
            i += 1
            schema_dir = normpath(abspath(args[i]))
        elseif startswith(arg, "--schema-dir=")
            value = split(arg, '='; limit = 2)[2]
            isempty(value) && throw(ArgumentError("--schema-dir requires a path"))
            schema_dir = normpath(abspath(value))
        elseif arg == "--help" || arg == "-h"
            help = true
        else
            throw(ArgumentError("unknown argument: $(arg)"))
        end
        i += 1
    end
    return (mode = something(mode, :write), schema_dir = schema_dir, help = help)
end

function main(args = ARGS; out::IO = stdout, err::IO = stderr)
    options = try
        parse_cli(args)
    catch ex
        println(err, "schema generator argument error: ", sprint(showerror, ex))
        usage(err)
        return 2
    end
    if options.help
        usage(out)
        return 0
    end
    ok = options.mode == :check ?
         check_schemas(options.schema_dir; out = out, err = err) :
         write_schemas(options.schema_dir; out = out, err = err)
    return ok ? 0 : 1
end

if abspath(PROGRAM_FILE) == @__FILE__
    exit(main())
end
