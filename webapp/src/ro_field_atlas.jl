# Demonstration-scale Atlas for exact two-input reaction-order fields.
#
# This owner deliberately consumes only explicitly supplied cell complexes.  It
# never invokes the network grammar or topology enumeration, and it never turns
# a sampled tensor into a surface signature.  The complete input population is
# at most eight records, with aggregate cell/facet limits checked before any
# cancellation callback or classifier work.

const RO_FIELD_ATLAS_SCHEMA_VERSION = "bne-ro-field-atlas/v1.0.0"
const RO_FIELD_ATLAS_QUERY_SCHEMA_VERSION =
    "bne-ro-field-atlas-query/v1.0.0"
const RO_FIELD_ATLAS_QUERY_RESULT_SCHEMA_VERSION =
    "bne-ro-field-atlas-query-result/v1.0.0"

const _RO_FIELD_ATLAS_HARD_MAX_FIELDS = 8
const _RO_FIELD_ATLAS_HARD_MAX_TOTAL_CELLS = 512
const _RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS = 1_024
const _RO_FIELD_ATLAS_HARD_MAX_OUTPUTS = 4
const _RO_FIELD_ATLAS_HARD_MAX_QUERY_FILTERS = 16
const _RO_FIELD_ATLAS_HARD_MAX_QUERY_PATTERNS = 16
const _RO_FIELD_ATLAS_HARD_MAX_QUERY_TRANSITIONS = 32
const _RO_FIELD_ATLAS_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9._:-]{0,127}$"
const _RO_FIELD_ATLAS_SHA256_PATTERN = r"^[0-9a-f]{64}$"
const _RO_FIELD_ATLAS_SIGN_PATTERN = r"^[+0-]+(?:\|[+0-]+)*$"

const _RO_FIELD_ATLAS_COMPONENT_CLASSES = Set((
    "zero",
    "strictly_positive",
    "nonnegative_variable",
    "strictly_negative",
    "nonpositive_variable",
    "sign_changing",
    "unknown",
))
const _RO_FIELD_ATLAS_GRADIENT_FAMILIES = Set((
    "all_zero",
    "all_nonnegative",
    "all_nonpositive",
    "opposed_axis_signs",
    "sign_changing",
    "other_mixed",
    "unknown",
))

"""One explicitly submitted candidate for the bounded RO-field Atlas."""
struct ROFieldAtlasInput
    record_id::String
    representation::Symbol
    field_sha256::Union{Nothing,String}
    complex::Union{Nothing,ROCellComplex2D}
    axis_ids::Vector{String}
    output_ids::Vector{String}
end

function ROFieldAtlasInput(
    record_id;
    representation,
    field_sha256=nothing,
    complex=nothing,
    axis_ids=String[],
    output_ids=String[],
)
    record_id isa AbstractString || throw(ArgumentError(
        "record_id must be a string"))
    normalized_record_id = String(record_id)
    occursin(_RO_FIELD_ATLAS_ID_PATTERN, normalized_record_id) ||
        throw(ArgumentError("invalid RO-field Atlas record_id: $normalized_record_id"))

    normalized_representation = if representation isa Symbol
        representation
    elseif representation isa AbstractString
        Symbol(String(representation))
    else
        throw(ArgumentError("representation must be a string or symbol"))
    end
    normalized_hash = if field_sha256 === nothing
        nothing
    elseif field_sha256 isa AbstractString
        String(field_sha256)
    else
        throw(ArgumentError("field_sha256 must be a string or nothing"))
    end
    complex === nothing || complex isa ROCellComplex2D ||
        throw(ArgumentError("complex must be an ROCellComplex2D or nothing"))

    normalize_ids(raw, name) = begin
        raw isa AbstractVector || raw isa Tuple || throw(ArgumentError(
            "$name must be an ordered vector"))
        result = String[]
        for item in raw
            (item isa AbstractString || item isa Symbol) || throw(ArgumentError(
                "$name entries must be strings or symbols"))
            push!(result, String(item))
        end
        result
    end
    return ROFieldAtlasInput(
        normalized_record_id,
        normalized_representation,
        normalized_hash,
        complex,
        normalize_ids(axis_ids, "axis_ids"),
        normalize_ids(output_ids, "output_ids"),
    )
end

"""Hard limits and classifier policy for one explicit demo Atlas build."""
struct ROFieldAtlasConfig
    max_fields::Int
    max_total_cells::Int
    max_total_facets::Int
    signature_config::ROFieldSignatureConfig
end

function _rofa_positive_bounded_integer(raw, name::AbstractString, hard_max::Int)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$name must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$name is outside the supported integer range"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$name must be in 1:$hard_max"))
    return value
end

function ROFieldAtlasConfig(;
    max_fields::Integer=_RO_FIELD_ATLAS_HARD_MAX_FIELDS,
    max_total_cells::Integer=_RO_FIELD_ATLAS_HARD_MAX_TOTAL_CELLS,
    max_total_facets::Integer=_RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS,
    signature_config::ROFieldSignatureConfig=ROFieldSignatureConfig(),
)
    return ROFieldAtlasConfig(
        _rofa_positive_bounded_integer(
            max_fields, "max_fields", _RO_FIELD_ATLAS_HARD_MAX_FIELDS),
        _rofa_positive_bounded_integer(
            max_total_cells, "max_total_cells",
            _RO_FIELD_ATLAS_HARD_MAX_TOTAL_CELLS),
        _rofa_positive_bounded_integer(
            max_total_facets, "max_total_facets",
            _RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS),
        signature_config,
    )
end

"""Raised by the Atlas preflight before classification begins."""
struct ROFieldAtlasLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldAtlasLimitExceeded)
    print(io, "RO-field Atlas ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

@inline function _rofa_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROFieldAtlasLimitExceeded(
        phase, requested, limit))
    return nothing
end

function _rofa_config_dict(config::ROFieldAtlasConfig)
    return Dict{String,Any}(
        "max_fields" => config.max_fields,
        "max_total_cells" => config.max_total_cells,
        "max_total_facets" => config.max_total_facets,
        "signature_config" => _rofb_config_dict(config.signature_config),
    )
end

function _rofa_ids_are_valid(ids::Vector{String}; expected=nothing)
    expected === nothing || length(ids) == expected || return false
    isempty(ids) && return false
    all(identifier -> occursin(_RO_FIELD_ATLAS_ID_PATTERN, identifier), ids) ||
        return false
    return allunique(ids)
end

function _rofa_diagnostic(
    record::ROFieldAtlasInput,
    stage::AbstractString,
    code::AbstractString,
    detail::AbstractString;
    eligibility_reasons=String[],
)
    return Dict{String,Any}(
        "record_id" => record.record_id,
        "representation" => String(record.representation),
        "stage" => String(stage),
        "code" => String(code),
        "detail" => String(detail),
        "eligibility_reasons" => sort!(unique(String.(eligibility_reasons))),
    )
end

function _rofa_input_ineligibility(record::ROFieldAtlasInput)
    if record.representation !== :exact_cell_complex
        code = record.representation === :sampled_grid ?
            "sampled_grid_not_classified" : "unsupported_representation"
        return _rofa_diagnostic(
            record,
            "eligibility",
            code,
            "Only explicitly declared exact two-input cell complexes are " *
            "classified; this record was retained as a diagnostic and was " *
            "not projected.",
        )
    elseif record.field_sha256 === nothing ||
           !occursin(_RO_FIELD_ATLAS_SHA256_PATTERN, record.field_sha256)
        return _rofa_diagnostic(
            record,
            "eligibility",
            "invalid_field_sha256",
            "An exact Atlas record requires a 64-character lowercase SHA-256 " *
            "field identity.",
        )
    elseif record.complex === nothing
        return _rofa_diagnostic(
            record,
            "eligibility",
            "missing_exact_cell_complex",
            "The exact_cell_complex declaration did not include an " *
            "ROCellComplex2D value.",
        )
    elseif length(record.axis_ids) != 2 ||
           !_rofa_ids_are_valid(record.axis_ids; expected=2)
        return _rofa_diagnostic(
            record,
            "eligibility",
            "invalid_axis_ids",
            "Exact Atlas records require two distinct ordered safe axis IDs.",
        )
    elseif !(1 <= length(record.output_ids) <=
             _RO_FIELD_ATLAS_HARD_MAX_OUTPUTS)
        return _rofa_diagnostic(
            record,
            "eligibility",
            "unsupported_output_count",
            "The demonstration Atlas accepts between one and " *
            "$(_RO_FIELD_ATLAS_HARD_MAX_OUTPUTS) ordered outputs per field.",
        )
    elseif !_rofa_ids_are_valid(record.output_ids)
        return _rofa_diagnostic(
            record,
            "eligibility",
            "invalid_output_ids",
            "Exact Atlas records require one or more distinct ordered safe " *
            "output IDs.",
        )
    elseif length(record.output_ids) != length(record.complex.output_indices)
        return _rofa_diagnostic(
            record,
            "eligibility",
            "output_count_mismatch",
            "output_ids does not match the exact complex output order.",
        )
    end
    return nothing
end

function _rofa_preflight!(
    records::AbstractVector{<:ROFieldAtlasInput},
    config::ROFieldAtlasConfig,
)
    isempty(records) && throw(ArgumentError(
        "an explicit RO-field Atlas corpus must submit at least one record"))
    _rofa_limit(:fields, BigInt(length(records)), config.max_fields)

    record_ids = getfield.(records, :record_id)
    allunique(record_ids) || throw(ArgumentError(
        "RO-field Atlas record_id values must be unique"))

    total_cells = BigInt(0)
    total_facets = BigInt(0)
    for record in records
        record.representation === :exact_cell_complex || continue
        record.complex === nothing && continue
        complex = record.complex
        cells = BigInt(length(complex.cells))
        facets = BigInt(length(complex.facets))
        total_cells += cells
        total_facets += facets

        # Mirror the per-field signature bounds here so every limit is rejected
        # before the outer build invokes a cancellation callback or classifier.
        _rofa_limit(:signature_cells, cells,
            config.signature_config.max_cells)
        _rofa_limit(:signature_facets, facets,
            config.signature_config.max_facets)
        matrix_elements = cells * BigInt(length(record.output_ids)) * 2 +
            facets * BigInt(length(record.output_ids)) * 4
        _rofa_limit(:signature_matrix_elements, matrix_elements,
            config.signature_config.max_matrix_elements)
    end
    _rofa_limit(:total_cells, total_cells, config.max_total_cells)
    _rofa_limit(:total_facets, total_facets, config.max_total_facets)
    return total_cells, total_facets
end

function _rofa_population_dict(
    submitted::Int,
    eligible::Int,
    evaluated::Int,
    classified::Int,
    diagnostic::Int,
)
    return Dict{String,Any}(
        "submitted_count" => submitted,
        "eligible_count" => eligible,
        "evaluated_count" => evaluated,
        "classified_count" => classified,
        "diagnostic_count" => diagnostic,
        "omitted_count" => eligible - evaluated,
        "ineligible_count" => submitted - eligible,
        "count_semantics" => Dict{String,Any}(
            "submitted_count" => "explicit records supplied by the caller",
            "eligible_count" =>
                "records with an exact 2D complex, field hash, axes, and outputs",
            "evaluated_count" =>
                "eligible records passed to the versioned classifier",
            "classified_count" =>
                "evaluated records with complete regular-cell classifications",
            "diagnostic_count" =>
                "submitted records retained with an ineligibility or " *
                "unclassifiable diagnostic",
            "omitted_count" =>
                "eligible records not evaluated; zero for a successful build",
        ),
    )
end

function _rofa_atlas_identity_payload(
    config,
    population,
    records,
    diagnostics,
    record_order,
)
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_ATLAS_SCHEMA_VERSION,
        "atlas_kind" => "explicit_exact_2d_demo_corpus",
        "network_space_claim" => "none",
        "source_population" => Dict{String,Any}(
            "selection" => "caller_declared_explicit_records",
            "enumeration_performed" => false,
            "topology_search_performed" => false,
        ),
        "config" => config,
        "population" => population,
        "record_order" => record_order,
        "records" => records,
        "diagnostics" => diagnostics,
    )
end

function _rofa_atlas_identity_payload(atlas::AbstractDict)
    getvalue(key) = haskey(atlas, key) ? atlas[key] :
        haskey(atlas, Symbol(key)) ? atlas[Symbol(key)] :
        throw(ArgumentError("RO-field Atlas is missing $key"))
    return Dict{String,Any}(
        "schema_version" => getvalue("schema_version"),
        "atlas_kind" => getvalue("atlas_kind"),
        "network_space_claim" => getvalue("network_space_claim"),
        "source_population" => getvalue("source_population"),
        "config" => getvalue("config"),
        "population" => getvalue("population"),
        "record_order" => getvalue("record_order"),
        "records" => getvalue("records"),
        "diagnostics" => getvalue("diagnostics"),
    )
end

"""
    build_ro_field_atlas(records; config=ROFieldAtlasConfig(), cancel_check=()->nothing)

Classify a caller-declared population of at most eight exact 2D fields.  All
aggregate and per-field dimensions are preflighted before callbacks or feature
work.  Unsupported records remain visible as diagnostics and are never coerced
into an exact representation.
"""
function build_ro_field_atlas(
    records::AbstractVector{<:ROFieldAtlasInput};
    config::ROFieldAtlasConfig=ROFieldAtlasConfig(),
    cancel_check=() -> nothing,
)
    total_cells, total_facets = _rofa_preflight!(records, config)
    ordered_inputs = sort!(collect(records); by=record -> record.record_id)

    classified_records = Dict{String,Any}[]
    diagnostics = Dict{String,Any}[]
    eligible_count = 0
    evaluated_count = 0
    classified_count = 0

    for record in ordered_inputs
        cancel_check()
        diagnostic = _rofa_input_ineligibility(record)
        if diagnostic !== nothing
            push!(diagnostics, diagnostic)
            continue
        end

        eligible_count += 1
        evaluated_count += 1
        signature = classify_ro_cell_complex(
            record.complex,
            record.field_sha256;
            axis_ids=record.axis_ids,
            output_ids=record.output_ids,
            config=config.signature_config,
            cancel_check=cancel_check,
        )
        if signature["classifiable"] === true
            classified_count += 1
            push!(classified_records, Dict{String,Any}(
                "record_id" => record.record_id,
                "field_sha256" => record.field_sha256,
                "signature_sha256" => signature["signature_sha256"],
                "signature" => signature,
            ))
        else
            push!(diagnostics, _rofa_diagnostic(
                record,
                "classification",
                "unclassifiable_exact_field",
                "The exact field was evaluated but its complete regular-cell " *
                "signature is unknown; no first label or partial feature was indexed.";
                eligibility_reasons=signature["diagnostics"][
                    "eligibility_reasons"],
            ))
        end
    end
    cancel_check()

    population = _rofa_population_dict(
        length(records),
        eligible_count,
        evaluated_count,
        classified_count,
        length(diagnostics),
    )
    config_dict = _rofa_config_dict(config)
    config_dict["preflight_total_cells"] = Int(total_cells)
    config_dict["preflight_total_facets"] = Int(total_facets)
    record_order = getindex.(classified_records, "record_id")
    identity = _rofa_atlas_identity_payload(
        config_dict,
        population,
        classified_records,
        diagnostics,
        record_order,
    )
    atlas = copy(identity)
    atlas["atlas_sha256"] = canonical_hash(identity)
    return atlas
end

"""One conjunctive output/axis component-class filter."""
struct ROFieldComponentFilter
    output_id::String
    axis_id::String
    classification::String

    function ROFieldComponentFilter(output_id, axis_id, classification)
        values = (output_id, axis_id, classification)
        all(value -> value isa AbstractString || value isa Symbol, values) ||
            throw(ArgumentError(
                "component filter fields must be strings or symbols"))
        output = String(output_id)
        axis = String(axis_id)
        class = String(classification)
        occursin(_RO_FIELD_ATLAS_ID_PATTERN, output) ||
            throw(ArgumentError("invalid component-filter output_id: $output"))
        occursin(_RO_FIELD_ATLAS_ID_PATTERN, axis) ||
            throw(ArgumentError("invalid component-filter axis_id: $axis"))
        class in _RO_FIELD_ATLAS_COMPONENT_CLASSES || throw(ArgumentError(
            "unsupported component classification: $class"))
        return new(output, axis, class)
    end
end

"""One conjunctive output gradient-family filter."""
struct ROFieldGradientFilter
    output_id::String
    gradient_family::String

    function ROFieldGradientFilter(output_id, gradient_family)
        (output_id isa AbstractString || output_id isa Symbol) ||
            throw(ArgumentError("gradient-filter output_id must be a string"))
        (gradient_family isa AbstractString || gradient_family isa Symbol) ||
            throw(ArgumentError("gradient_family must be a string"))
        output = String(output_id)
        family = String(gradient_family)
        occursin(_RO_FIELD_ATLAS_ID_PATTERN, output) ||
            throw(ArgumentError("invalid gradient-filter output_id: $output"))
        family in _RO_FIELD_ATLAS_GRADIENT_FAMILIES || throw(ArgumentError(
            "unsupported gradient family: $family"))
        return new(output, family)
    end
end

function _rofa_normalize_pattern(raw, name::AbstractString)
    (raw isa AbstractString || raw isa Symbol) || throw(ArgumentError(
        "$name entries must be strings or symbols"))
    pattern = String(raw)
    occursin(_RO_FIELD_ATLAS_SIGN_PATTERN, pattern) || throw(ArgumentError(
        "invalid $name token: $pattern"))
    return pattern
end

function _rofa_normalize_transition(raw)
    (raw isa AbstractString || raw isa Symbol) || throw(ArgumentError(
        "required_transition_tokens entries must be strings or symbols"))
    token = String(raw)
    endpoints = split(token, "<->")
    length(endpoints) == 2 || throw(ArgumentError(
        "invalid transition token: $token"))
    normalized = sort!([
        _rofa_normalize_pattern(endpoint, "transition endpoint")
        for endpoint in endpoints
    ])
    return normalized[1] * "<->" * normalized[2]
end

function _rofa_nonnegative_bounded_integer(
    raw,
    name::AbstractString,
    hard_max::Int,
)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$name must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$name is outside the supported integer range"))
    end
    0 <= value <= hard_max || throw(ArgumentError(
        "$name must be in 0:$hard_max"))
    return value
end

"""Bounded conjunctive query over stored exact-field signatures."""
struct ROFieldAtlasQuerySpec
    component_filters::Vector{ROFieldComponentFilter}
    gradient_filters::Vector{ROFieldGradientFilter}
    required_sign_patterns::Vector{String}
    required_transition_tokens::Vector{String}
    min_mixed_sign_facet_count::Int
    min_coupled_jump_count::Int
    limit::Int
end

function ROFieldAtlasQuerySpec(;
    component_filters=ROFieldComponentFilter[],
    gradient_filters=ROFieldGradientFilter[],
    required_sign_patterns=String[],
    required_transition_tokens=String[],
    min_mixed_sign_facet_count::Integer=0,
    min_coupled_jump_count::Integer=0,
    limit::Integer=_RO_FIELD_ATLAS_HARD_MAX_FIELDS,
)
    component_filters isa AbstractVector || throw(ArgumentError(
        "component_filters must be a vector"))
    gradient_filters isa AbstractVector || throw(ArgumentError(
        "gradient_filters must be a vector"))
    all(item -> item isa ROFieldComponentFilter, component_filters) ||
        throw(ArgumentError(
            "component_filters must contain ROFieldComponentFilter values"))
    all(item -> item isa ROFieldGradientFilter, gradient_filters) ||
        throw(ArgumentError(
            "gradient_filters must contain ROFieldGradientFilter values"))
    length(component_filters) <= _RO_FIELD_ATLAS_HARD_MAX_QUERY_FILTERS ||
        throw(ArgumentError("too many component filters"))
    length(gradient_filters) <= _RO_FIELD_ATLAS_HARD_MAX_QUERY_FILTERS ||
        throw(ArgumentError("too many gradient filters"))

    components_by_key = Dict{NTuple{3,String},ROFieldComponentFilter}()
    for item in component_filters
        components_by_key[(item.output_id, item.axis_id, item.classification)] = item
    end
    gradients_by_key = Dict{NTuple{2,String},ROFieldGradientFilter}()
    for item in gradient_filters
        gradients_by_key[(item.output_id, item.gradient_family)] = item
    end
    components = [components_by_key[key] for key in
        sort!(collect(keys(components_by_key)))]
    gradients = [gradients_by_key[key] for key in
        sort!(collect(keys(gradients_by_key)))]

    required_sign_patterns isa AbstractVector || throw(ArgumentError(
        "required_sign_patterns must be a vector"))
    required_transition_tokens isa AbstractVector || throw(ArgumentError(
        "required_transition_tokens must be a vector"))
    length(required_sign_patterns) <= _RO_FIELD_ATLAS_HARD_MAX_QUERY_PATTERNS ||
        throw(ArgumentError("too many required sign patterns"))
    length(required_transition_tokens) <=
        _RO_FIELD_ATLAS_HARD_MAX_QUERY_TRANSITIONS ||
        throw(ArgumentError("too many required transition tokens"))
    patterns = sort!(unique([
        _rofa_normalize_pattern(item, "required_sign_patterns")
        for item in required_sign_patterns
    ]))
    transitions = sort!(unique([
        _rofa_normalize_transition(item)
        for item in required_transition_tokens
    ]))

    mixed = _rofa_nonnegative_bounded_integer(
        min_mixed_sign_facet_count,
        "min_mixed_sign_facet_count",
        _RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS,
    )
    coupled = _rofa_nonnegative_bounded_integer(
        min_coupled_jump_count,
        "min_coupled_jump_count",
        _RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS,
    )
    normalized_limit = _rofa_positive_bounded_integer(
        limit, "limit", _RO_FIELD_ATLAS_HARD_MAX_FIELDS)
    return ROFieldAtlasQuerySpec(
        components,
        gradients,
        patterns,
        transitions,
        mixed,
        coupled,
        normalized_limit,
    )
end

function _rofa_query_dict(query::ROFieldAtlasQuerySpec)
    return Dict{String,Any}(
        "component_filters" => [
            Dict{String,Any}(
                "output_id" => item.output_id,
                "axis_id" => item.axis_id,
                "classification" => item.classification,
            ) for item in query.component_filters
        ],
        "gradient_filters" => [
            Dict{String,Any}(
                "output_id" => item.output_id,
                "gradient_family" => item.gradient_family,
            ) for item in query.gradient_filters
        ],
        "required_sign_patterns" => query.required_sign_patterns,
        "required_transition_tokens" => query.required_transition_tokens,
        "min_mixed_sign_facet_count" => query.min_mixed_sign_facet_count,
        "min_coupled_jump_count" => query.min_coupled_jump_count,
        "limit" => query.limit,
    )
end

function _rofa_dict_get(raw::AbstractDict, key::AbstractString)
    haskey(raw, key) && return raw[key]
    haskey(raw, Symbol(key)) && return raw[Symbol(key)]
    throw(ArgumentError("RO-field Atlas value is missing $key"))
end

function _rofa_validate_atlas!(atlas::AbstractDict)
    _rofa_dict_get(atlas, "schema_version") == RO_FIELD_ATLAS_SCHEMA_VERSION ||
        throw(ArgumentError("unsupported RO-field Atlas schema version"))
    _rofa_dict_get(atlas, "atlas_kind") ==
        "explicit_exact_2d_demo_corpus" || throw(ArgumentError(
        "unsupported RO-field Atlas kind"))
    _rofa_dict_get(atlas, "network_space_claim") == "none" ||
        throw(ArgumentError("RO-field Atlas network_space_claim must be none"))
    atlas_hash = _rofa_dict_get(atlas, "atlas_sha256")
    atlas_hash isa AbstractString &&
        occursin(_RO_FIELD_ATLAS_SHA256_PATTERN, atlas_hash) ||
        throw(ArgumentError("invalid RO-field Atlas SHA-256 identity"))

    records = _rofa_dict_get(atlas, "records")
    diagnostics = _rofa_dict_get(atlas, "diagnostics")
    population = _rofa_dict_get(atlas, "population")
    order = _rofa_dict_get(atlas, "record_order")
    config = _rofa_dict_get(atlas, "config")
    records isa AbstractVector || throw(ArgumentError(
        "RO-field Atlas records must be a vector"))
    diagnostics isa AbstractVector || throw(ArgumentError(
        "RO-field Atlas diagnostics must be a vector"))
    population isa AbstractDict || throw(ArgumentError(
        "RO-field Atlas population must be an object"))
    order isa AbstractVector || throw(ArgumentError(
        "RO-field Atlas record_order must be a vector"))
    config isa AbstractDict || throw(ArgumentError(
        "RO-field Atlas config must be an object"))
    length(records) <= _RO_FIELD_ATLAS_HARD_MAX_FIELDS ||
        throw(ArgumentError("RO-field Atlas contains too many records"))
    length(diagnostics) <= _RO_FIELD_ATLAS_HARD_MAX_FIELDS ||
        throw(ArgumentError("RO-field Atlas contains too many diagnostics"))
    length(order) <= _RO_FIELD_ATLAS_HARD_MAX_FIELDS ||
        throw(ArgumentError("RO-field Atlas record_order is too large"))
    all(record -> record isa AbstractDict, records) || throw(ArgumentError(
        "RO-field Atlas record entries must be objects"))

    count(name) = begin
        value = _rofa_dict_get(population, name)
        (value isa Integer && !(value isa Bool) && value >= 0) ||
            throw(ArgumentError("population.$name must be nonnegative"))
        try
            Int(value)
        catch
            throw(ArgumentError("population.$name is outside the supported range"))
        end
    end
    submitted = count("submitted_count")
    eligible = count("eligible_count")
    evaluated = count("evaluated_count")
    classified = count("classified_count")
    diagnostic = count("diagnostic_count")
    omitted = count("omitted_count")
    ineligible = count("ineligible_count")
    submitted <= _RO_FIELD_ATLAS_HARD_MAX_FIELDS || throw(ArgumentError(
        "RO-field Atlas submitted_count exceeds the hard field limit"))
    submitted >= eligible >= evaluated >= classified || throw(ArgumentError(
        "RO-field Atlas population counts are inconsistent"))
    omitted == eligible - evaluated || throw(ArgumentError(
        "RO-field Atlas omitted_count is inconsistent"))
    ineligible == submitted - eligible || throw(ArgumentError(
        "RO-field Atlas ineligible_count is inconsistent"))
    classified == length(records) || throw(ArgumentError(
        "RO-field Atlas classified_count does not match records"))
    diagnostic == length(diagnostics) || throw(ArgumentError(
        "RO-field Atlas diagnostic_count does not match diagnostics"))
    diagnostic <= submitted || throw(ArgumentError(
        "RO-field Atlas diagnostic_count exceeds submitted_count"))
    String.(order) == [String(_rofa_dict_get(record, "record_id"))
                       for record in records] || throw(ArgumentError(
        "RO-field Atlas record_order does not match records"))
    allunique(String.(order)) || throw(ArgumentError(
        "RO-field Atlas record_order contains duplicates"))

    config_integer(name, lower, upper) = begin
        value = _rofa_dict_get(config, name)
        (value isa Integer && !(value isa Bool)) || throw(ArgumentError(
            "RO-field Atlas config.$name must be an integer"))
        lower <= value <= upper || throw(ArgumentError(
            "RO-field Atlas config.$name is outside the demo bound"))
        Int(value)
    end
    max_fields = config_integer(
        "max_fields", 1, _RO_FIELD_ATLAS_HARD_MAX_FIELDS)
    max_total_cells = config_integer(
        "max_total_cells", 1, _RO_FIELD_ATLAS_HARD_MAX_TOTAL_CELLS)
    max_total_facets = config_integer(
        "max_total_facets", 1, _RO_FIELD_ATLAS_HARD_MAX_TOTAL_FACETS)
    preflight_cells = config_integer(
        "preflight_total_cells", 0, max_total_cells)
    preflight_facets = config_integer(
        "preflight_total_facets", 0, max_total_facets)
    submitted <= max_fields || throw(ArgumentError(
        "RO-field Atlas submitted_count exceeds config.max_fields"))
    preflight_cells <= max_total_cells || throw(ArgumentError(
        "RO-field Atlas cell population exceeds its declared config"))
    preflight_facets <= max_total_facets || throw(ArgumentError(
        "RO-field Atlas facet population exceeds its declared config"))

    for record in records
        signature = _rofa_dict_get(record, "signature")
        signature isa AbstractDict || throw(ArgumentError(
            "RO-field Atlas signatures must be objects"))
        normalized_signature = validate_ro_field_signature!(signature)
        normalized_signature["classifiable"] === true ||
            throw(ArgumentError(
                "RO-field Atlas searchable records must be classifiable"))
        _rofa_dict_get(record, "field_sha256") ==
            normalized_signature["field_sha256"] || throw(ArgumentError(
            "RO-field Atlas record and signature field hashes differ"))
        _rofa_dict_get(record, "signature_sha256") ==
            normalized_signature["signature_sha256"] || throw(ArgumentError(
            "RO-field Atlas record and signature hashes differ"))

        axes = normalized_signature["axis_ids"]
        outputs = normalized_signature["output_ids"]
        components = normalized_signature["component_classifications"]
        families = normalized_signature["gradient_families"]
        features = normalized_signature["features"]
        axes isa AbstractVector && length(axes) == 2 || throw(ArgumentError(
            "RO-field Atlas signature must retain exactly two axes"))
        outputs isa AbstractVector &&
            1 <= length(outputs) <= _RO_FIELD_ATLAS_HARD_MAX_OUTPUTS ||
            throw(ArgumentError(
                "RO-field Atlas signature output count exceeds the demo bound"))
        components isa AbstractVector &&
            length(components) <= 2 * _RO_FIELD_ATLAS_HARD_MAX_OUTPUTS ||
            throw(ArgumentError(
                "RO-field Atlas signature has too many component features"))
        families isa AbstractVector &&
            length(families) <= _RO_FIELD_ATLAS_HARD_MAX_OUTPUTS ||
            throw(ArgumentError(
                "RO-field Atlas signature has too many gradient families"))
        features isa AbstractDict || throw(ArgumentError(
            "RO-field Atlas signature features must be an object"))
        patterns = _rofa_dict_get(features, "regular_cell_sign_patterns")
        transitions = _rofa_dict_get(
            features, "internal_facet_transition_tokens")
        patterns isa AbstractVector &&
            length(patterns) <= _RO_FIELD_SIGNATURE_MAX_CELLS ||
            throw(ArgumentError(
                "RO-field Atlas signature has too many cell patterns"))
        transitions isa AbstractVector &&
            length(transitions) <= _RO_FIELD_SIGNATURE_MAX_FACETS ||
            throw(ArgumentError(
                "RO-field Atlas signature has too many facet transitions"))
    end
    canonical_hash(_rofa_atlas_identity_payload(atlas)) == atlas_hash ||
        throw(ArgumentError("RO-field Atlas content does not match atlas_sha256"))
    return records, population, String(atlas_hash)
end

function _rofa_signature_matches(
    signature::AbstractDict,
    query::ROFieldAtlasQuerySpec,
)
    component_tokens = Set(
        (String(_rofa_dict_get(item, "output_id")),
         String(_rofa_dict_get(item, "axis_id")),
         String(_rofa_dict_get(item, "classification")))
        for item in _rofa_dict_get(signature, "component_classifications")
    )
    all((item.output_id, item.axis_id, item.classification) in component_tokens
        for item in query.component_filters) || return false

    gradient_tokens = Set(
        (String(_rofa_dict_get(item, "output_id")),
         String(_rofa_dict_get(item, "gradient_family")))
        for item in _rofa_dict_get(signature, "gradient_families")
    )
    all((item.output_id, item.gradient_family) in gradient_tokens
        for item in query.gradient_filters) || return false

    features = _rofa_dict_get(signature, "features")
    sign_patterns = Set(String.(_rofa_dict_get(
        features, "regular_cell_sign_patterns")))
    all(pattern in sign_patterns for pattern in query.required_sign_patterns) ||
        return false
    transitions = Set(String.(_rofa_dict_get(
        features, "internal_facet_transition_tokens")))
    all(token in transitions for token in query.required_transition_tokens) ||
        return false
    _rofa_dict_get(features, "mixed_sign_facet_count") >=
        query.min_mixed_sign_facet_count || return false
    _rofa_dict_get(features, "coupled_jump_count") >=
        query.min_coupled_jump_count || return false
    return true
end

"""
    query_ro_field_atlas(atlas, query; cancel_check=()->nothing)

Run a bounded conjunctive query over classified exact-field signatures.  A zero
result is scoped either to the complete caller-declared demo corpus or to the
evaluated subset when diagnostics/omissions prevent that stronger finite claim.
"""
function query_ro_field_atlas(
    atlas::AbstractDict,
    query::ROFieldAtlasQuerySpec=ROFieldAtlasQuerySpec();
    cancel_check=() -> nothing,
)
    records, population, atlas_hash = _rofa_validate_atlas!(atlas)
    cancel_check()
    matches = Dict{String,Any}[]
    for record in records
        cancel_check()
        signature = _rofa_dict_get(record, "signature")
        _rofa_signature_matches(signature, query) || continue
        push!(matches, Dict{String,Any}(
            "record_id" => _rofa_dict_get(record, "record_id"),
            "field_sha256" => _rofa_dict_get(record, "field_sha256"),
            "signature_sha256" => _rofa_dict_get(record, "signature_sha256"),
            "signature" => signature,
        ))
    end
    cancel_check()

    matched_count = length(matches)
    returned = matches[1:min(query.limit, matched_count)]
    submitted = Int(_rofa_dict_get(population, "submitted_count"))
    eligible = Int(_rofa_dict_get(population, "eligible_count"))
    evaluated = Int(_rofa_dict_get(population, "evaluated_count"))
    classified = Int(_rofa_dict_get(population, "classified_count"))
    diagnostic = Int(_rofa_dict_get(population, "diagnostic_count"))
    omitted = Int(_rofa_dict_get(population, "omitted_count"))
    complete_declared_demo = submitted == eligible == evaluated == classified &&
        diagnostic == 0 && omitted == 0
    status = if matched_count > 0
        "matches_found_in_evaluated_subset"
    elseif complete_declared_demo
        "no_match_in_declared_demo_corpus"
    else
        "no_match_in_evaluated_subset"
    end

    query_dict = _rofa_query_dict(query)
    query_identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_ATLAS_QUERY_SCHEMA_VERSION,
        "atlas_sha256" => atlas_hash,
        "query" => query_dict,
    )
    result = Dict{String,Any}(
        "schema_version" => RO_FIELD_ATLAS_QUERY_RESULT_SCHEMA_VERSION,
        "query_schema_version" => RO_FIELD_ATLAS_QUERY_SCHEMA_VERSION,
        "atlas_sha256" => atlas_hash,
        "query_sha256" => canonical_hash(query_identity),
        "network_space_claim" => "none",
        "status" => status,
        "scope" => complete_declared_demo ?
            "declared_explicit_demo_corpus" : "evaluated_exact_2d_subset",
        "query" => query_dict,
        "population" => Dict{String,Any}(
            "atlas_submitted_count" => submitted,
            "atlas_eligible_count" => eligible,
            "atlas_evaluated_count" => evaluated,
            "atlas_classified_count" => classified,
            "atlas_diagnostic_count" => diagnostic,
            "atlas_omitted_count" => omitted,
            "examined_count" => length(records),
            "matched_count" => matched_count,
            "returned_count" => length(returned),
            "omitted_match_count" => matched_count - length(returned),
        ),
        "truncated" => matched_count > length(returned),
        "matches" => returned,
    )
    result["result_sha256"] = canonical_hash(result)
    return result
end
