# ─── Biocircuits Explorer IR v1 ───────────────────────────────────────────────
#
# Versioned, JSON-faithful representations of (1) the binding-network artifact
# the user is designing and (2) the design-intent specification that asks the
# inverse-design pipeline what to look for.
#
# This module is the single declared place where the "implicit IR" of the
# existing webapp endpoints (`reactions: string[]`, `kd: number[]`, scattered
# `goal/query/refinement` dicts) is given a stable shape, a schema version, and
# light JSON-shape validation with clear errors. Older inputs are accepted via
# legacy bridges so existing endpoints keep working unchanged.
#
# Design notes:
#   • Flat-include into BiocircuitsExplorerBackend (matches atlas.jl /
#     inverse_design.jl convention). Relies on the parent module having already
#     defined `_raw_get`, `_raw_haskey`, `_materialize`, `_now_iso_timestamp`
#     (atlas.jl) and `ReactionParser.parse_reactions`.
#   • Pure data + validation. No simulation, no DB access.
#   • Version field uses a "<family>/<semver>" pattern so future incompatible
#     schemas can fork the family.

const NETWORK_IR_SCHEMA_VERSION = "bne-ir/v1.0.0"
const NETWORK_IR_SCHEMA_FAMILY = "bne-ir"
const DESIGN_SPEC_SCHEMA_VERSION = "bne-design/v1.0.0"
const DESIGN_SPEC_SCHEMA_FAMILY = "bne-design"

const _NETWORK_IR_LEGACY_SOURCE = "legacy_reactions_kd"

const _IR_SPECIES_ROLES = Set([:auto, :free, :bound, :complex, :catalyst, :other])
const _IR_REACTION_KINDS = Set([:binding, :catalysis, :conformation, :other])
const _IR_PARAM_DIST_KINDS = Set([:point, :uniform, :loguniform, :normal, :lognormal])

# ─── Errors ────────────────────────────────────────────────────────────────────

struct IRValidationError <: Exception
    msg::String
    path::String
end
IRValidationError(msg::AbstractString) = IRValidationError(String(msg), "")

function Base.showerror(io::IO, err::IRValidationError)
    if isempty(err.path)
        print(io, "IRValidationError: ", err.msg)
    else
        print(io, "IRValidationError at `", err.path, "`: ", err.msg)
    end
end

_ir_path_join(parent::AbstractString, child::AbstractString) =
    isempty(parent) ? String(child) : string(parent, ".", child)
_ir_path_index(parent::AbstractString, idx::Integer) =
    isempty(parent) ? string("[", idx, "]") : string(parent, "[", idx, "]")

# ─── Validation primitives ─────────────────────────────────────────────────────

function _ir_require_dict(raw, path::AbstractString)
    raw isa AbstractDict || throw(IRValidationError("expected a JSON object", String(path)))
    return raw
end

function _ir_require_list(raw, path::AbstractString)
    (raw isa AbstractVector || raw isa Tuple) ||
        throw(IRValidationError("expected a JSON array", String(path)))
    return raw
end

function _ir_optional_string(raw, key::Symbol, default::AbstractString, path::AbstractString)
    _raw_haskey(raw, key) || return String(default)
    value = _raw_get(raw, key, default)
    value === nothing && return String(default)
    value isa AbstractString || throw(IRValidationError(
        "expected a string", _ir_path_join(path, String(key))))
    return String(value)
end

function _ir_optional_string_or_nothing(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || return nothing
    value = _raw_get(raw, key, nothing)
    value === nothing && return nothing
    value isa AbstractString || throw(IRValidationError(
        "expected a string or null", _ir_path_join(path, String(key))))
    return String(value)
end

function _ir_require_string(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || throw(IRValidationError(
        "missing required field `$(String(key))`", String(path)))
    value = _raw_get(raw, key, nothing)
    (value isa AbstractString && !isempty(strip(String(value)))) ||
        throw(IRValidationError("expected a non-empty string",
                                _ir_path_join(path, String(key))))
    return String(value)
end

function _ir_optional_number(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || return nothing
    value = _raw_get(raw, key, nothing)
    value === nothing && return nothing
    value isa Real || throw(IRValidationError(
        "expected a number or null", _ir_path_join(path, String(key))))
    return Float64(value)
end

function _ir_require_number(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || throw(IRValidationError(
        "missing required field `$(String(key))`", String(path)))
    value = _raw_get(raw, key, nothing)
    value isa Real || throw(IRValidationError(
        "expected a number", _ir_path_join(path, String(key))))
    return Float64(value)
end

function _ir_optional_bool(raw, key::Symbol, default::Bool, path::AbstractString)
    _raw_haskey(raw, key) || return default
    value = _raw_get(raw, key, default)
    value === nothing && return default
    value isa Bool || throw(IRValidationError(
        "expected true/false", _ir_path_join(path, String(key))))
    return value
end

function _ir_optional_symbol(raw, key::Symbol, default::Symbol, allowed::Set{Symbol}, path::AbstractString)
    _raw_haskey(raw, key) || return default
    value = _raw_get(raw, key, default)
    value === nothing && return default
    sym = value isa Symbol ? value : Symbol(String(value))
    sym in allowed || throw(IRValidationError(
        "value `$(String(sym))` is not one of $(sort!(String.(collect(allowed))))",
        _ir_path_join(path, String(key))))
    return sym
end

function _ir_optional_metadata(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || return Dict{String, Any}()
    value = _raw_get(raw, key, nothing)
    value === nothing && return Dict{String, Any}()
    value isa AbstractDict || throw(IRValidationError(
        "expected an object", _ir_path_join(path, String(key))))
    return Dict{String, Any}(_materialize(value))
end

# ─── Component declarations ────────────────────────────────────────────────────

"""
    SpeciesDecl(name; role=:auto, initial_total=nothing, unit="concentration", metadata=Dict())

A declared species in the network. `role` is one of `:auto`, `:free`, `:bound`,
`:complex`, `:catalyst`, `:other`; `:auto` defers role assignment to the
downstream parser (reactants→free, products→bound). `initial_total` is the
nominal total concentration in `unit`-encoded units; leave `nothing` if the
total is a swept parameter (then declare a `ParameterDistribution`).
"""
Base.@kwdef struct SpeciesDecl
    name::String
    role::Symbol = :auto
    initial_total::Union{Nothing, Float64} = nothing
    unit::String = "concentration"
    metadata::Dict{String, Any} = Dict{String, Any}()
end

"""
    ReactionDecl(formula, kd; kind=:binding, kd_distribution=nothing, reversible=true, metadata=Dict())

A reaction is declared by its `formula` ("A + B <-> AB", coefficients allowed
on either side) and a positive nominal `kd`. `kind` is one of `:binding`,
`:catalysis`, `:conformation`, `:other` — only `:binding` is honored by the
current ROP engine; the others are reserved for forward-compatibility.
`kd_distribution` captures uncertainty (kept verbatim; consumed by upstream
sweeps).
"""
Base.@kwdef struct ReactionDecl
    formula::String
    kind::Symbol = :binding
    kd::Float64
    kd_distribution::Union{Nothing, Dict{String, Any}} = nothing
    reversible::Bool = true
    metadata::Dict{String, Any} = Dict{String, Any}()
end

"""
    ObservableDecl(name, expression; unit="concentration", metadata=Dict())

An observable maps the latent state vector `x` to a scalar quantity the user
wants to reason about (FRET signal, total bound complex, etc.). `expression`
is a linear combination of declared species ("AB", "AB + 2 ABC"); it is
syntactically validated here and semantically validated when bound to a model.
"""
Base.@kwdef struct ObservableDecl
    name::String
    expression::String
    unit::String = "concentration"
    metadata::Dict{String, Any} = Dict{String, Any}()
end

"""
    ParameterDistribution(symbol; kind=:point, value=nothing, log_min=..., log_max=..., mu=..., sigma=..., metadata=Dict())

How a parameter ranges across the design space. `kind` ∈ {`:point`, `:uniform`,
`:loguniform`, `:normal`, `:lognormal`}.  `:point` requires `value`; `:uniform`
and `:loguniform` require `log_min < log_max` (interpreted as log10 bounds);
`:normal` and `:lognormal` require `mu, sigma > 0`. `symbol` should refer to a
declared total ("tA") or Kd ("Kd1").
"""
Base.@kwdef struct ParameterDistribution
    symbol::String
    kind::Symbol = :point
    value::Union{Nothing, Float64} = nothing
    log_min::Union{Nothing, Float64} = nothing
    log_max::Union{Nothing, Float64} = nothing
    mu::Union{Nothing, Float64} = nothing
    sigma::Union{Nothing, Float64} = nothing
    metadata::Dict{String, Any} = Dict{String, Any}()
end

Base.@kwdef struct Provenance
    created_at::String = ""
    created_by::String = ""
    source::String = ""
    parent_ir_hash::Union{Nothing, String} = nothing
    notes::String = ""
end

"""
    NetworkIR

The versioned representation of a binding-network artifact: declared species,
reactions, observables, parameter distributions, optional compartments (the
default is a single well-mixed compartment), provenance, and a free-form
`extensions` bag for forward-compatibility.

`ir_schema_version` is the family/version string `"bne-ir/vMAJOR.MINOR.PATCH"`;
parsers reject payloads whose family prefix does not match. The hash returned
by [`network_ir_hash`](@ref) is a SHA-256 over the canonicalized JSON form and
is suitable for cache keys / provenance links.
"""
Base.@kwdef struct NetworkIR
    ir_schema_version::String = NETWORK_IR_SCHEMA_VERSION
    label::String = ""
    species::Vector{SpeciesDecl} = SpeciesDecl[]
    reactions::Vector{ReactionDecl} = ReactionDecl[]
    observables::Vector{ObservableDecl} = ObservableDecl[]
    parameter_distributions::Vector{ParameterDistribution} = ParameterDistribution[]
    compartments::Vector{Dict{String, Any}} = Dict{String, Any}[]
    provenance::Provenance = Provenance()
    extensions::Dict{String, Any} = Dict{String, Any}()
end

"""
    DesignSpec

The versioned representation of a design intent. Separates four concerns:

- `goal`     — what behavior the circuit should realize (motif/exact labels,
               IO symbols, must-have regimes/transitions, witness paths).
- `constraints` — hard limits the result must not exceed (max base species,
               max reactions, forbidden regimes/transitions, support caps).
- `objectives` — how to rank multiple candidates (ranking_mode, pareto,
               min_volume_mean, robustness).
- `policies`   — how to search (search_profile, behavior_config,
               inverse_design, refinement, enumeration).

The four sections are free-form dict bags so that the existing
`atlas_query_spec_from_raw` / `inverse_design_spec_from_raw` parsers can
continue to consume them after [`design_spec_to_legacy_request`](@ref) merges
the sections into the legacy pipeline shape.
"""
Base.@kwdef struct DesignSpec
    ir_schema_version::String = DESIGN_SPEC_SCHEMA_VERSION
    label::String = ""
    goal::Dict{String, Any} = Dict{String, Any}()
    constraints::Dict{String, Any} = Dict{String, Any}()
    objectives::Dict{String, Any} = Dict{String, Any}()
    policies::Dict{String, Any} = Dict{String, Any}()
    provenance::Provenance = Provenance()
    extensions::Dict{String, Any} = Dict{String, Any}()
end

# ─── Helpers for the parsers ───────────────────────────────────────────────────

function _parse_provenance(raw, path::AbstractString)
    raw === nothing && return Provenance(created_at = _now_iso_timestamp())
    _ir_require_dict(raw, path)
    created_at = _ir_optional_string(raw, :created_at, "", path)
    if isempty(created_at)
        created_at = _now_iso_timestamp()
    end
    return Provenance(
        created_at = created_at,
        created_by = _ir_optional_string(raw, :created_by, "", path),
        source = _ir_optional_string(raw, :source, "", path),
        parent_ir_hash = _ir_optional_string_or_nothing(raw, :parent_ir_hash, path),
        notes = _ir_optional_string(raw, :notes, "", path),
    )
end

function _provenance_to_dict(prov::Provenance)
    out = Dict{String, Any}(
        "created_at" => prov.created_at,
        "created_by" => prov.created_by,
        "source" => prov.source,
        "parent_ir_hash" => prov.parent_ir_hash,
        "notes" => prov.notes,
    )
    return out
end

function _parse_species_decl(raw, path::AbstractString)
    _ir_require_dict(raw, path)
    return SpeciesDecl(
        name = _ir_require_string(raw, :name, path),
        role = _ir_optional_symbol(raw, :role, :auto, _IR_SPECIES_ROLES, path),
        initial_total = _ir_optional_number(raw, :initial_total, path),
        unit = _ir_optional_string(raw, :unit, "concentration", path),
        metadata = _ir_optional_metadata(raw, :metadata, path),
    )
end

function _species_to_dict(sp::SpeciesDecl)
    return Dict{String, Any}(
        "name" => sp.name,
        "role" => String(sp.role),
        "initial_total" => sp.initial_total,
        "unit" => sp.unit,
        "metadata" => sp.metadata,
    )
end

function _parse_reaction_decl(raw, path::AbstractString)
    _ir_require_dict(raw, path)
    kd_dist_raw = _raw_haskey(raw, :kd_distribution) ?
        _raw_get(raw, :kd_distribution, nothing) : nothing
    if kd_dist_raw !== nothing && !(kd_dist_raw isa AbstractDict)
        throw(IRValidationError("expected an object or null",
            _ir_path_join(path, "kd_distribution")))
    end
    kd_dist = kd_dist_raw === nothing ? nothing :
              Dict{String, Any}(_materialize(kd_dist_raw))

    formula = _ir_require_string(raw, :formula, path)
    occursin(r"<->|<=>|↔", formula) || throw(IRValidationError(
        "reaction `formula` must contain one of `<->`, `<=>`, `↔`",
        _ir_path_join(path, "formula")))

    return ReactionDecl(
        formula = formula,
        kind = _ir_optional_symbol(raw, :kind, :binding, _IR_REACTION_KINDS, path),
        kd = _ir_require_number(raw, :kd, path),
        kd_distribution = kd_dist,
        reversible = _ir_optional_bool(raw, :reversible, true, path),
        metadata = _ir_optional_metadata(raw, :metadata, path),
    )
end

function _reaction_to_dict(rx::ReactionDecl)
    return Dict{String, Any}(
        "formula" => rx.formula,
        "kind" => String(rx.kind),
        "kd" => rx.kd,
        "kd_distribution" => rx.kd_distribution,
        "reversible" => rx.reversible,
        "metadata" => rx.metadata,
    )
end

function _parse_observable_decl(raw, path::AbstractString)
    _ir_require_dict(raw, path)
    return ObservableDecl(
        name = _ir_require_string(raw, :name, path),
        expression = _ir_require_string(raw, :expression, path),
        unit = _ir_optional_string(raw, :unit, "concentration", path),
        metadata = _ir_optional_metadata(raw, :metadata, path),
    )
end

function _observable_to_dict(obs::ObservableDecl)
    return Dict{String, Any}(
        "name" => obs.name,
        "expression" => obs.expression,
        "unit" => obs.unit,
        "metadata" => obs.metadata,
    )
end

function _parse_parameter_distribution(raw, path::AbstractString)
    _ir_require_dict(raw, path)
    kind = _ir_optional_symbol(raw, :kind, :point, _IR_PARAM_DIST_KINDS, path)
    value = _ir_optional_number(raw, :value, path)
    log_min = _ir_optional_number(raw, :log_min, path)
    log_max = _ir_optional_number(raw, :log_max, path)
    mu = _ir_optional_number(raw, :mu, path)
    sigma = _ir_optional_number(raw, :sigma, path)

    if kind === :point && value === nothing
        throw(IRValidationError(
            "parameter distribution kind=`point` requires a `value`", String(path)))
    end
    if kind in (:uniform, :loguniform)
        (log_min === nothing || log_max === nothing) && throw(IRValidationError(
            "parameter distribution kind=`$(String(kind))` requires `log_min` and `log_max`",
            String(path)))
        log_max > log_min || throw(IRValidationError(
            "parameter distribution requires log_max > log_min", String(path)))
    end
    if kind in (:normal, :lognormal)
        (mu === nothing || sigma === nothing) && throw(IRValidationError(
            "parameter distribution kind=`$(String(kind))` requires `mu` and `sigma`",
            String(path)))
        sigma > 0 || throw(IRValidationError(
            "parameter distribution requires sigma > 0", String(path)))
    end

    return ParameterDistribution(
        symbol = _ir_require_string(raw, :symbol, path),
        kind = kind,
        value = value,
        log_min = log_min,
        log_max = log_max,
        mu = mu,
        sigma = sigma,
        metadata = _ir_optional_metadata(raw, :metadata, path),
    )
end

function _parameter_distribution_to_dict(pd::ParameterDistribution)
    return Dict{String, Any}(
        "symbol" => pd.symbol,
        "kind" => String(pd.kind),
        "value" => pd.value,
        "log_min" => pd.log_min,
        "log_max" => pd.log_max,
        "mu" => pd.mu,
        "sigma" => pd.sigma,
        "metadata" => pd.metadata,
    )
end

# ─── NetworkIR parser ──────────────────────────────────────────────────────────

is_network_ir(raw) = raw isa AbstractDict &&
    (_raw_haskey(raw, :ir_schema_version) || _raw_haskey(raw, :species))

is_legacy_network_payload(raw) = raw isa AbstractDict &&
    _raw_haskey(raw, :reactions) &&
    !_raw_haskey(raw, :ir_schema_version) &&
    !_raw_haskey(raw, :species)

function _ir_check_schema_version(raw, family::AbstractString, current::AbstractString, path::AbstractString)
    declared = _ir_optional_string(raw, :ir_schema_version, current, path)
    isempty(declared) && return current
    prefix = string(family, "/")
    startswith(declared, prefix) || throw(IRValidationError(
        "ir_schema_version `$declared` does not belong to family `$family`", String(path)))
    declared
end

"""
    parse_network_ir(raw) -> NetworkIR

Accept either a structured payload (with an `ir_schema_version` of the
`bne-ir/...` family or with declared `species`) or the legacy
`{reactions: [...], kd: [...], input_symbols?, output_symbols?, label?}`
shape. Legacy payloads are bridged to a fully-formed `NetworkIR` with
`provenance.source == "legacy_reactions_kd"`. Throws `IRValidationError`
with a `.path` field on any shape violation.
"""
function parse_network_ir(raw)
    raw === nothing && throw(IRValidationError("expected a JSON object", ""))
    raw isa AbstractDict || throw(IRValidationError("expected a JSON object", ""))

    if is_legacy_network_payload(raw)
        return _network_ir_from_legacy_payload(raw)
    end

    path = "network"
    version = _ir_check_schema_version(raw, NETWORK_IR_SCHEMA_FAMILY, NETWORK_IR_SCHEMA_VERSION, path)
    label = _ir_optional_string(raw, :label, "", path)

    species_path = _ir_path_join(path, "species")
    species_raw = _raw_haskey(raw, :species) ? _raw_get(raw, :species, Any[]) : Any[]
    _ir_require_list(species_raw, species_path)
    species_list = SpeciesDecl[]
    seen_names = Set{String}()
    for (idx, item) in enumerate(species_raw)
        item_path = _ir_path_index(species_path, idx)
        sp = _parse_species_decl(item, item_path)
        sp.name in seen_names && throw(IRValidationError(
            "duplicate species name `$(sp.name)`", item_path))
        push!(seen_names, sp.name)
        push!(species_list, sp)
    end

    reactions_path = _ir_path_join(path, "reactions")
    reactions_raw = _raw_haskey(raw, :reactions) ? _raw_get(raw, :reactions, Any[]) : Any[]
    _ir_require_list(reactions_raw, reactions_path)
    reactions = ReactionDecl[]
    for (idx, item) in enumerate(reactions_raw)
        push!(reactions, _parse_reaction_decl(item, _ir_path_index(reactions_path, idx)))
    end

    observables_path = _ir_path_join(path, "observables")
    obs_raw = _raw_haskey(raw, :observables) ? _raw_get(raw, :observables, Any[]) : Any[]
    _ir_require_list(obs_raw, observables_path)
    observables = ObservableDecl[]
    for (idx, item) in enumerate(obs_raw)
        push!(observables, _parse_observable_decl(item, _ir_path_index(observables_path, idx)))
    end

    pd_path = _ir_path_join(path, "parameter_distributions")
    pd_raw = _raw_haskey(raw, :parameter_distributions) ?
             _raw_get(raw, :parameter_distributions, Any[]) : Any[]
    _ir_require_list(pd_raw, pd_path)
    parameter_distributions = ParameterDistribution[]
    for (idx, item) in enumerate(pd_raw)
        push!(parameter_distributions,
              _parse_parameter_distribution(item, _ir_path_index(pd_path, idx)))
    end

    compartments_raw = _raw_haskey(raw, :compartments) ? _raw_get(raw, :compartments, Any[]) : Any[]
    _ir_require_list(compartments_raw, _ir_path_join(path, "compartments"))
    compartments = Dict{String, Any}[]
    for item in compartments_raw
        item isa AbstractDict || throw(IRValidationError(
            "compartment entries must be objects", _ir_path_join(path, "compartments")))
        push!(compartments, Dict{String, Any}(_materialize(item)))
    end

    provenance = _parse_provenance(
        _raw_haskey(raw, :provenance) ? _raw_get(raw, :provenance, nothing) : nothing,
        _ir_path_join(path, "provenance"),
    )

    extensions = _ir_optional_metadata(raw, :extensions, path)

    network = NetworkIR(
        ir_schema_version = version,
        label = label,
        species = species_list,
        reactions = reactions,
        observables = observables,
        parameter_distributions = parameter_distributions,
        compartments = compartments,
        provenance = provenance,
        extensions = extensions,
    )

    _validate_network_ir_semantics(network)
    return network
end

function _network_ir_from_legacy_payload(raw)
    path = "network(legacy)"
    rules_raw = _raw_get(raw, :reactions, Any[])
    _ir_require_list(rules_raw, _ir_path_join(path, "reactions"))
    rules = String[String(r) for r in rules_raw]
    isempty(rules) && throw(IRValidationError(
        "legacy payload `reactions` must be a non-empty array",
        _ir_path_join(path, "reactions")))

    kd_raw = _raw_haskey(raw, :kd) ? _raw_get(raw, :kd, nothing) :
             (_raw_haskey(raw, :Kd) ? _raw_get(raw, :Kd, nothing) : nothing)
    kd = if kd_raw === nothing
        ones(Float64, length(rules))
    else
        _ir_require_list(kd_raw, _ir_path_join(path, "kd"))
        out = Float64[]
        for (idx, val) in enumerate(kd_raw)
            val isa Real || throw(IRValidationError(
                "kd entries must be numbers", _ir_path_index(_ir_path_join(path, "kd"), idx)))
            push!(out, Float64(val))
        end
        out
    end
    length(kd) == length(rules) || throw(IRValidationError(
        "legacy payload requires length(kd) == length(reactions)",
        _ir_path_join(path, "kd")))
    all(x -> x > 0, kd) || throw(IRValidationError(
        "all kd values must be > 0", _ir_path_join(path, "kd")))

    label_raw = _raw_haskey(raw, :label) ? _raw_get(raw, :label, "") : ""
    label = label_raw === nothing ? "" : String(label_raw)

    input_symbols = _ir_optional_string_list(raw, :input_symbols, _ir_path_join(path, "input_symbols"))
    output_symbols = _ir_optional_string_list(raw, :output_symbols, _ir_path_join(path, "output_symbols"))

    return network_ir_from_legacy(rules, kd;
        label = label,
        input_symbols = input_symbols,
        output_symbols = output_symbols,
    )
end

function _ir_optional_string_list(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || return String[]
    value = _raw_get(raw, key, nothing)
    value === nothing && return String[]
    _ir_require_list(value, String(path))
    out = String[]
    for (idx, item) in enumerate(value)
        item isa AbstractString || throw(IRValidationError(
            "entries must be strings", _ir_path_index(String(path), idx)))
        push!(out, String(item))
    end
    return out
end

# ─── Legacy bridge: build a NetworkIR from (reactions, kd) ─────────────────────

function network_ir_from_legacy(rules::Vector{String}, kd::Vector{Float64};
                                 label::AbstractString = "",
                                 input_symbols::Vector{String} = String[],
                                 output_symbols::Vector{String} = String[])
    length(kd) == length(rules) || throw(IRValidationError(
        "length(kd) must match length(reactions)"))
    all(x -> x > 0, kd) || throw(IRValidationError("all kd values must be > 0"))

    reactants, products = parse_reactions(rules)
    species_order = String[]
    bound_set = Set{String}()
    for pd in products
        for s in keys(pd)
            push!(bound_set, String(s))
        end
    end
    seen = Set{String}()
    free_set = String[]
    for rd in reactants
        for s in keys(rd)
            s_str = String(s)
            if !(s_str in bound_set) && !(s_str in seen)
                push!(free_set, s_str)
                push!(seen, s_str)
            end
        end
    end
    bound_list = sort!(collect(bound_set))

    species_list = SpeciesDecl[]
    for name in free_set
        push!(species_list, SpeciesDecl(name = name, role = :free))
    end
    for name in bound_list
        push!(species_list, SpeciesDecl(name = name, role = :bound))
    end

    reactions = ReactionDecl[]
    for (idx, rule) in enumerate(rules)
        push!(reactions, ReactionDecl(formula = rule, kind = :binding, kd = kd[idx]))
    end

    observables = ObservableDecl[]
    for sym in output_symbols
        isempty(sym) && continue
        push!(observables, ObservableDecl(
            name = string("observable_", sym),
            expression = sym,
        ))
    end

    parameter_distributions = ParameterDistribution[]
    for sym in input_symbols
        isempty(sym) && continue
        push!(parameter_distributions, ParameterDistribution(
            symbol = sym,
            kind = :loguniform,
            log_min = -3.0,
            log_max = 3.0,
        ))
    end

    return NetworkIR(
        ir_schema_version = NETWORK_IR_SCHEMA_VERSION,
        label = String(label),
        species = species_list,
        reactions = reactions,
        observables = observables,
        parameter_distributions = parameter_distributions,
        compartments = Dict{String, Any}[],
        provenance = Provenance(
            created_at = _now_iso_timestamp(),
            source = _NETWORK_IR_LEGACY_SOURCE,
        ),
        extensions = Dict{String, Any}(),
    )
end

# ─── Semantic validation: parsed reactions must reference declared species ────

function _validate_network_ir_semantics(net::NetworkIR)
    isempty(net.reactions) && throw(IRValidationError(
        "network must declare at least one reaction", "network.reactions"))

    declared = Set(sp.name for sp in net.species)
    rules = [rx.formula for rx in net.reactions]
    reactants, products = try
        parse_reactions(rules)
    catch err
        rethrow(IRValidationError(
            "failed to parse reactions: $(sprint(showerror, err))",
            "network.reactions"))
    end

    referenced = Set{String}()
    for rd in reactants
        for s in keys(rd)
            push!(referenced, String(s))
        end
    end
    for pd in products
        for s in keys(pd)
            push!(referenced, String(s))
        end
    end

    if !isempty(declared)
        missing_decls = sort!(collect(setdiff(referenced, declared)))
        isempty(missing_decls) || throw(IRValidationError(
            "reactions reference species that are not declared: $(join(missing_decls, ", "))",
            "network.species"))
    end

    for rx in net.reactions
        rx.kd > 0 || throw(IRValidationError(
            "reaction kd must be > 0 (formula=`$(rx.formula)`)",
            "network.reactions"))
    end

    for (idx, pd) in enumerate(net.parameter_distributions)
        isempty(pd.symbol) && throw(IRValidationError(
            "parameter distribution must have a non-empty symbol",
            _ir_path_index("network.parameter_distributions", idx)))
    end
    return net
end

# ─── NetworkIR → dict ──────────────────────────────────────────────────────────

function network_ir_to_dict(net::NetworkIR)
    return Dict{String, Any}(
        "ir_schema_version" => net.ir_schema_version,
        "label" => net.label,
        "species" => [_species_to_dict(sp) for sp in net.species],
        "reactions" => [_reaction_to_dict(rx) for rx in net.reactions],
        "observables" => [_observable_to_dict(obs) for obs in net.observables],
        "parameter_distributions" => [_parameter_distribution_to_dict(pd) for pd in net.parameter_distributions],
        "compartments" => copy(net.compartments),
        "provenance" => _provenance_to_dict(net.provenance),
        "extensions" => net.extensions,
    )
end

# ─── Legacy view: NetworkIR → (rules, kd, …) the existing engine consumes ─────

function network_ir_to_legacy_inputs(net::NetworkIR)
    isempty(net.reactions) && throw(IRValidationError(
        "network has no reactions; cannot build legacy inputs",
        "network.reactions"))
    rules = [rx.formula for rx in net.reactions]
    kd = [rx.kd for rx in net.reactions]
    input_symbols = [pd.symbol for pd in net.parameter_distributions]
    output_symbols = [obs.expression for obs in net.observables]
    return (
        rules = rules,
        kd = kd,
        input_symbols = input_symbols,
        output_symbols = output_symbols,
        label = net.label,
    )
end

# ─── Stable hash for provenance / cache keys ───────────────────────────────────

function network_ir_hash(net::NetworkIR)
    # Content identity excludes provenance: `created_at`/`created_by`/`notes` are
    # metadata about *when/who*, not *what* the network is. Excluding them makes
    # the same network always hash the same way, which is what lets the hash be
    # used as a stable cache key and parent-lineage reference.
    d = network_ir_to_dict(net)
    delete!(d, "provenance")
    return bytes2hex(SHA.sha256(JSON3.write(_stable_canonical_value(d))))
end

# ─── DesignSpec parsing ────────────────────────────────────────────────────────

function _ir_optional_object(raw, key::Symbol, path::AbstractString)
    _raw_haskey(raw, key) || return Dict{String, Any}()
    value = _raw_get(raw, key, nothing)
    value === nothing && return Dict{String, Any}()
    value isa AbstractDict || throw(IRValidationError(
        "expected an object", _ir_path_join(path, String(key))))
    return Dict{String, Any}(_materialize(value))
end

# Light typing for the *stabilized* constraint keys. Validates only the keys we
# have committed to a shape for; every other key is left untouched so the
# constraints bag stays a free-form escape hatch and the legacy bridge keeps
# working. Returns the (unmodified) dict.
function _validate_design_constraints(constraints::AbstractDict, path::AbstractString)
    cpath = _ir_path_join(path, "constraints")
    for key in ("max_base_species", "max_reactions")
        haskey(constraints, key) || continue
        v = constraints[key]
        (v isa Real && isfinite(v) && v > 0 && isinteger(v)) || throw(IRValidationError(
            "expected a positive integer", _ir_path_join(cpath, key)))
    end
    for key in ("forbid_regimes", "forbid_transitions")
        haskey(constraints, key) || continue
        v = constraints[key]
        (v isa AbstractVector && all(x -> x isa AbstractString, v)) || throw(IRValidationError(
            "expected an array of strings", _ir_path_join(cpath, key)))
    end
    return constraints
end

"""
    parse_design_spec(raw) -> DesignSpec

Parse a `bne-design/v1` payload with four sections (`goal`, `constraints`,
`objectives`, `policies`); requires at least one of the first three to be
non-empty. Sections are kept as raw dicts and bridged to the existing pipeline
shape via [`design_spec_to_legacy_request`](@ref). Stabilized `constraints` keys
(`max_base_species`, `max_reactions`, `forbid_regimes`, `forbid_transitions`)
are shape-checked; all other keys pass through untouched.
"""
function parse_design_spec(raw)
    raw === nothing && throw(IRValidationError("expected a JSON object", ""))
    raw isa AbstractDict || throw(IRValidationError("expected a JSON object", ""))

    path = "design"
    version = _ir_check_schema_version(raw, DESIGN_SPEC_SCHEMA_FAMILY, DESIGN_SPEC_SCHEMA_VERSION, path)
    label = _ir_optional_string(raw, :label, "", path)

    goal = _ir_optional_object(raw, :goal, path)
    constraints = _validate_design_constraints(_ir_optional_object(raw, :constraints, path), path)
    objectives = _ir_optional_object(raw, :objectives, path)
    policies = _ir_optional_object(raw, :policies, path)

    provenance = _parse_provenance(
        _raw_haskey(raw, :provenance) ? _raw_get(raw, :provenance, nothing) : nothing,
        _ir_path_join(path, "provenance"),
    )
    extensions = _ir_optional_metadata(raw, :extensions, path)

    isempty(goal) && isempty(constraints) && isempty(objectives) &&
        throw(IRValidationError(
            "design spec must specify at least one of `goal`, `constraints`, or `objectives`",
            path))

    return DesignSpec(
        ir_schema_version = version,
        label = label,
        goal = goal,
        constraints = constraints,
        objectives = objectives,
        policies = policies,
        provenance = provenance,
        extensions = extensions,
    )
end

function design_spec_to_dict(ds::DesignSpec)
    return Dict{String, Any}(
        "ir_schema_version" => ds.ir_schema_version,
        "label" => ds.label,
        "goal" => ds.goal,
        "constraints" => ds.constraints,
        "objectives" => ds.objectives,
        "policies" => ds.policies,
        "provenance" => _provenance_to_dict(ds.provenance),
        "extensions" => ds.extensions,
    )
end

function design_spec_hash(ds::DesignSpec)
    # As with network_ir_hash, provenance is metadata, not content identity.
    d = design_spec_to_dict(ds)
    delete!(d, "provenance")
    return bytes2hex(SHA.sha256(JSON3.write(_stable_canonical_value(d))))
end

# ─── DesignSpec → legacy request dict consumed by run_inverse_design_from_spec ─
#
# The existing pipeline expects a single flat request dict with keys like
# `query`, `inverse_design`, `refinement`, `search_profile`, `behavior_config`,
# plus `networks`/`atlas`/`library`. This bridge merges goal/constraints/
# objectives into a single query object (because the existing
# `atlas_query_spec_from_raw` already accepts a free-form goal-shaped dict) and
# routes `policies` keys to their conventional destinations.

const _POLICY_TO_REQUEST_KEYS = Dict(
    "search_profile" => "search_profile",
    "profile" => "search_profile",
    "behavior_config" => "behavior_config",
    "inverse_design" => "inverse_design",
    "inverse" => "inverse_design",
    "refinement" => "refinement",
    "enumeration" => "enumeration",
)

function design_spec_to_legacy_request(ds::DesignSpec;
                                       network::Union{Nothing, NetworkIR} = nothing,
                                       extra::Union{Nothing, AbstractDict} = nothing)
    request = Dict{String, Any}()

    merged_query = Dict{String, Any}()
    for (k, v) in ds.goal
        merged_query[String(k)] = v
    end
    for (k, v) in ds.constraints
        merged_query[String(k)] = v
    end
    for (k, v) in ds.objectives
        merged_query[String(k)] = v
    end
    isempty(merged_query) || (request["query"] = merged_query)

    for (policy_key, value) in ds.policies
        dst = get(_POLICY_TO_REQUEST_KEYS, String(policy_key), nothing)
        if dst === nothing
            request[String(policy_key)] = value
        else
            request[dst] = value
        end
    end

    if network !== nothing
        bridge = network_ir_to_legacy_inputs(network)
        request["networks"] = Any[Dict{String, Any}(
            "label" => isempty(bridge.label) ? "design_target" : bridge.label,
            "reactions" => bridge.rules,
            "kd" => bridge.kd,
            "input_symbols" => bridge.input_symbols,
            "output_symbols" => bridge.output_symbols,
        )]
    end

    isempty(ds.label) || (request["source_label"] = ds.label)

    if extra !== nothing
        for (k, v) in extra
            request[String(k)] = v
        end
    end
    return request
end
