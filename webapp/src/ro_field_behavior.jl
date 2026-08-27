const RO_FIELD_SIGNATURE_SCHEMA_VERSION =
    "bne-ro-field-signature/v1.0.0"
const RO_FIELD_SIGNATURE_CLASSIFIER_VERSION =
    "regular-cell-gradient/v1.0.0"
const RO_FIELD_SIGNATURE_SCOPE =
    "regular_cell_interiors_excluding_declared_lower_dimensional_strata"

const _RO_FIELD_SIGNATURE_MAX_CELLS = 256
const _RO_FIELD_SIGNATURE_MAX_FACETS = 512
const _RO_FIELD_SIGNATURE_MAX_MATRIX_ELEMENTS = 1_048_576
const _RO_FIELD_SIGNATURE_MAX_GEOMETRY_TOLERANCE =
    BindingAndCatalysis._RO2_MAX_GEOMETRY_TOLERANCE
const _RO_FIELD_SIGNATURE_ID_PATTERN =
    r"^[A-Za-z][A-Za-z0-9._:-]{0,127}$"
const _RO_FIELD_SIGNATURE_SHA256_PATTERN = r"^[0-9a-f]{64}$"
const _RO_FIELD_SIGNATURE_COMPONENT_CLASSES = Set((
    "zero",
    "strictly_positive",
    "nonnegative_variable",
    "strictly_negative",
    "nonpositive_variable",
    "sign_changing",
    "unknown",
))
const _RO_FIELD_SIGNATURE_GRADIENT_FAMILIES = Set((
    "all_zero",
    "all_nonnegative",
    "all_nonpositive",
    "opposed_axis_signs",
    "sign_changing",
    "other_mixed",
    "unknown",
))
const _RO_FIELD_SIGNATURE_ELIGIBILITY_REASONS = Set((
    "invalid_domain_area",
    "coverage_incomplete",
    "gap_area_unknown",
    "gap_area_nonfinite",
    "positive_area_gap",
    "negative_gap_area",
    "ambiguous_complex",
    "duplicate_cell_id",
    "set_valued_cell",
    "unlabelled_cell",
    "multi_label_cell",
    "invalid_label_matrix_shape",
    "nonfinite_label_matrix",
    "invalid_label_offset",
    "invalid_internal_facet_incidence",
))

"""Hard, demonstration-scale limits for a regular-cell gradient signature."""
struct ROFieldSignatureConfig
    zero_tolerance::Float64
    max_cells::Int
    max_facets::Int
    max_matrix_elements::Int
end

function ROFieldSignatureConfig(;
    zero_tolerance::Real=1e-9,
    max_cells::Integer=_RO_FIELD_SIGNATURE_MAX_CELLS,
    max_facets::Integer=_RO_FIELD_SIGNATURE_MAX_FACETS,
    max_matrix_elements::Integer=_RO_FIELD_SIGNATURE_MAX_MATRIX_ELEMENTS,
)
    tolerance = Float64(zero_tolerance)
    isfinite(tolerance) && tolerance >= 0 || throw(ArgumentError(
        "zero_tolerance must be finite and nonnegative"))

    cells = Int(max_cells)
    facets = Int(max_facets)
    matrix_elements = Int(max_matrix_elements)
    0 < cells <= _RO_FIELD_SIGNATURE_MAX_CELLS || throw(ArgumentError(
        "max_cells must be in 1:$(_RO_FIELD_SIGNATURE_MAX_CELLS)"))
    0 < facets <= _RO_FIELD_SIGNATURE_MAX_FACETS || throw(ArgumentError(
        "max_facets must be in 1:$(_RO_FIELD_SIGNATURE_MAX_FACETS)"))
    0 < matrix_elements <= _RO_FIELD_SIGNATURE_MAX_MATRIX_ELEMENTS ||
        throw(ArgumentError(
            "max_matrix_elements must be in 1:" *
            string(_RO_FIELD_SIGNATURE_MAX_MATRIX_ELEMENTS)))
    return ROFieldSignatureConfig(tolerance, cells, facets, matrix_elements)
end

"""Raised before classifying a complex that exceeds a declared signature limit."""
struct ROFieldSignatureLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldSignatureLimitExceeded)
    print(io, "RO field signature ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

@inline function _rofb_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROFieldSignatureLimitExceeded(
        phase, requested, limit))
    return nothing
end

function _rofb_config_dict(config::ROFieldSignatureConfig)
    return Dict{String,Any}(
        "zero_tolerance" => config.zero_tolerance,
        "max_cells" => config.max_cells,
        "max_facets" => config.max_facets,
        "max_matrix_elements" => config.max_matrix_elements,
    )
end

function _rofb_normalize_ids(raw_ids, expected::Int, name::AbstractString)
    length(raw_ids) == expected || throw(DimensionMismatch(
        "$name must contain exactly $expected ordered identifiers"))
    identifiers = String[]
    sizehint!(identifiers, expected)
    for raw in raw_ids
        (raw isa AbstractString || raw isa Symbol) || throw(ArgumentError(
            "$name entries must be strings or symbols"))
        identifier = String(raw)
        occursin(_RO_FIELD_SIGNATURE_ID_PATTERN, identifier) ||
            throw(ArgumentError("invalid $name identifier: $identifier"))
        push!(identifiers, identifier)
    end
    allunique(identifiers) || throw(ArgumentError(
        "$name identifiers must be unique"))
    return identifiers
end

@inline function _rofb_value_sign(value::Float64, tolerance::Float64)
    value > tolerance && return :positive
    value < -tolerance && return :negative
    return :zero
end

@inline _rofb_sign_character(sign::Symbol) =
    sign === :positive ? "+" : sign === :negative ? "-" : "0"

function _rofb_component_classification(signs)
    has_positive = any(==(:positive), signs)
    has_negative = any(==(:negative), signs)
    has_zero = any(==(:zero), signs)
    has_positive && has_negative && return "sign_changing"
    has_positive && !has_negative && !has_zero && return "strictly_positive"
    has_positive && !has_negative && has_zero && return "nonnegative_variable"
    has_negative && !has_positive && !has_zero && return "strictly_negative"
    has_negative && !has_positive && has_zero && return "nonpositive_variable"
    !has_positive && !has_negative && has_zero && return "zero"
    return "unknown"
end

function _rofb_gradient_family(component_classes::Vector{String})
    any(==("unknown"), component_classes) && return "unknown"
    any(==("sign_changing"), component_classes) && return "sign_changing"
    all(==("zero"), component_classes) && return "all_zero"

    nonnegative = Set(("zero", "strictly_positive", "nonnegative_variable"))
    nonpositive = Set(("zero", "strictly_negative", "nonpositive_variable"))
    all(in(nonnegative), component_classes) && return "all_nonnegative"
    all(in(nonpositive), component_classes) && return "all_nonpositive"

    positive_only = Set(("strictly_positive", "nonnegative_variable"))
    negative_only = Set(("strictly_negative", "nonpositive_variable"))
    length(component_classes) == 2 &&
        ((component_classes[1] in positive_only &&
          component_classes[2] in negative_only) ||
         (component_classes[2] in positive_only &&
          component_classes[1] in negative_only)) &&
        return "opposed_axis_signs"
    return "other_mixed"
end

function _rofb_unknown_components(axis_ids::Vector{String}, output_ids::Vector{String})
    components = Dict{String,Any}[]
    for output_id in output_ids, axis_id in axis_ids
        push!(components, Dict{String,Any}(
            "output_id" => output_id,
            "axis_id" => axis_id,
            "classification" => "unknown",
        ))
    end
    families = Dict{String,Any}[
        Dict{String,Any}(
            "output_id" => output_id,
            "gradient_family" => "unknown",
        ) for output_id in output_ids
    ]
    return components, families
end

function _rofb_cell_pattern(signs::Matrix{Symbol})
    rows = String[]
    sizehint!(rows, size(signs, 1))
    for output_index in axes(signs, 1)
        push!(rows, join((_rofb_sign_character(signs[output_index, axis_index])
            for axis_index in axes(signs, 2))))
    end
    return join(rows, "|")
end

function _rofb_signature_object(raw, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$path must be an object"))
    return raw
end

function _rofb_signature_get(raw::AbstractDict, key::AbstractString,
                             path::AbstractString)
    matching_keys = Any[
        actual for actual in keys(raw)
        if (actual isa AbstractString || actual isa Symbol) &&
           String(actual) == key
    ]
    length(matching_keys) <= 1 || throw(ArgumentError(
        "$path contains duplicate string/symbol forms of $key"))
    length(matching_keys) == 1 && return raw[only(matching_keys)]
    throw(ArgumentError("$path is missing $key"))
end

function _rofb_signature_exact_keys(raw, expected, path::AbstractString)
    object = _rofb_signature_object(raw, path)
    observed = String[]
    for key in keys(object)
        (key isa AbstractString || key isa Symbol) || throw(ArgumentError(
            "$path keys must be strings or symbols"))
        push!(observed, String(key))
    end
    length(observed) == length(expected) && Set(observed) == Set(expected) ||
        throw(ArgumentError("$path has an unexpected key set"))
    return object
end

function _rofb_signature_string(raw, path::AbstractString)
    raw isa AbstractString || throw(ArgumentError("$path must be a string"))
    return String(raw)
end

function _rofb_signature_bool(raw, path::AbstractString)
    raw isa Bool || throw(ArgumentError("$path must be a boolean"))
    return raw
end

function _rofb_signature_nonnegative_int(raw, path::AbstractString;
                                         maximum::Union{Nothing,Int}=nothing)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$path must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$path is outside the supported integer range"))
    end
    value >= 0 || throw(ArgumentError("$path must be nonnegative"))
    maximum === nothing || value <= maximum || throw(ArgumentError(
        "$path exceeds the supported maximum $maximum"))
    return value
end

function _rofb_signature_positive_int(raw, path::AbstractString, maximum::Int)
    value = _rofb_signature_nonnegative_int(raw, path; maximum=maximum)
    value > 0 || throw(ArgumentError("$path must be positive"))
    return value
end

function _rofb_signature_identifier_vector(raw, path::AbstractString;
                                           expected::Union{Nothing,Int}=nothing)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    expected === nothing || length(raw) == expected || throw(ArgumentError(
        "$path must contain exactly $expected identifiers"))
    identifiers = String[]
    for item in raw
        identifier = _rofb_signature_string(item, "$path[]")
        occursin(_RO_FIELD_SIGNATURE_ID_PATTERN, identifier) ||
            throw(ArgumentError("invalid identifier in $path: $identifier"))
        push!(identifiers, identifier)
    end
    isempty(identifiers) && throw(ArgumentError("$path must not be empty"))
    allunique(identifiers) || throw(ArgumentError(
        "$path identifiers must be unique"))
    return identifiers
end

function _rofb_signature_pattern_rows(raw, output_count::Int,
                                      path::AbstractString)
    pattern = _rofb_signature_string(raw, path)
    rows = String.(split(pattern, "|"; keepempty=true))
    length(rows) == output_count || throw(ArgumentError(
        "$path must contain one sign row per output"))
    for row in rows
        ncodeunits(row) == 2 && all(character -> character in ('+', '-', '0'), row) ||
            throw(ArgumentError(
                "$path rows must contain exactly two +, -, or 0 signs"))
    end
    return pattern, rows
end

@inline function _rofb_signature_character_sign(character::Char)
    character == '+' && return :positive
    character == '-' && return :negative
    character == '0' && return :zero
    error("internal error: validated sign character was lost")
end

function _rofb_signature_config(raw)
    object = _rofb_signature_exact_keys(raw, (
        "zero_tolerance", "max_cells", "max_facets", "max_matrix_elements",
    ), "signature.config")
    tolerance_raw = _rofb_signature_get(
        object, "zero_tolerance", "signature.config")
    (tolerance_raw isa Real && !(tolerance_raw isa Bool)) ||
        throw(ArgumentError("signature.config.zero_tolerance must be numeric"))
    tolerance = Float64(tolerance_raw)
    isfinite(tolerance) && tolerance >= 0 || throw(ArgumentError(
        "signature.config.zero_tolerance must be finite and nonnegative"))
    return Dict{String,Any}(
        "zero_tolerance" => tolerance,
        "max_cells" => _rofb_signature_positive_int(
            _rofb_signature_get(object, "max_cells", "signature.config"),
            "signature.config.max_cells", _RO_FIELD_SIGNATURE_MAX_CELLS),
        "max_facets" => _rofb_signature_positive_int(
            _rofb_signature_get(object, "max_facets", "signature.config"),
            "signature.config.max_facets", _RO_FIELD_SIGNATURE_MAX_FACETS),
        "max_matrix_elements" => _rofb_signature_positive_int(
            _rofb_signature_get(
                object, "max_matrix_elements", "signature.config"),
            "signature.config.max_matrix_elements",
            _RO_FIELD_SIGNATURE_MAX_MATRIX_ELEMENTS),
    )
end

function _rofb_signature_diagnostics(raw, config, classifiable::Bool)
    object = _rofb_signature_exact_keys(raw, (
        "eligibility_reasons",
        "coverage_complete",
        "gap_area",
        "has_ambiguity",
        "regular_cell_count",
        "facet_count",
        "internal_facet_count",
        "excluded_lower_dimensional_strata_count",
        "budgeted_matrix_elements",
    ), "signature.diagnostics")
    reasons_raw = _rofb_signature_get(
        object, "eligibility_reasons", "signature.diagnostics")
    reasons_raw isa AbstractVector || throw(ArgumentError(
        "signature.diagnostics.eligibility_reasons must be an array"))
    reasons = String[]
    for raw_reason in reasons_raw
        reason = _rofb_signature_string(
            raw_reason, "signature.diagnostics.eligibility_reasons[]")
        reason in _RO_FIELD_SIGNATURE_ELIGIBILITY_REASONS ||
            throw(ArgumentError("unsupported signature eligibility reason: $reason"))
        push!(reasons, reason)
    end
    issorted(reasons) && allunique(reasons) || throw(ArgumentError(
        "signature eligibility reasons must be sorted and unique"))
    classifiable == isempty(reasons) || throw(ArgumentError(
        "signature.classifiable must equal emptiness of eligibility_reasons"))

    coverage_complete = _rofb_signature_bool(
        _rofb_signature_get(object, "coverage_complete", "signature.diagnostics"),
        "signature.diagnostics.coverage_complete")
    ("coverage_incomplete" in reasons) == !coverage_complete ||
        throw(ArgumentError(
            "coverage_complete and coverage_incomplete reason disagree"))
    has_ambiguity = _rofb_signature_bool(
        _rofb_signature_get(object, "has_ambiguity", "signature.diagnostics"),
        "signature.diagnostics.has_ambiguity")
    ("ambiguous_complex" in reasons) == has_ambiguity ||
        throw(ArgumentError(
            "has_ambiguity and ambiguous_complex reason disagree"))

    gap_raw = _rofb_signature_get(object, "gap_area", "signature.diagnostics")
    gap_area = if gap_raw === nothing
        gap_reasons = count(reason -> reason in (
            "gap_area_unknown", "gap_area_nonfinite"), reasons)
        gap_reasons == 1 || throw(ArgumentError(
            "null gap_area requires exactly one unknown/nonfinite reason"))
        nothing
    else
        (gap_raw isa Real && !(gap_raw isa Bool)) || throw(ArgumentError(
            "signature.diagnostics.gap_area must be finite numeric or null"))
        gap = Float64(gap_raw)
        isfinite(gap) || throw(ArgumentError(
            "signature.diagnostics.gap_area must be finite numeric or null"))
        any(reason -> reason in (
            "gap_area_unknown", "gap_area_nonfinite"), reasons) &&
            throw(ArgumentError(
                "finite gap_area cannot carry an unknown/nonfinite reason"))
        gap
    end
    positive_gap = gap_area !== nothing && gap_area > 0
    negative_gap = gap_area !== nothing && gap_area < 0
    ("positive_area_gap" in reasons) == positive_gap ||
        throw(ArgumentError(
            "gap_area and positive_area_gap reason disagree"))
    ("negative_gap_area" in reasons) == negative_gap ||
        throw(ArgumentError(
            "gap_area and negative_gap_area reason disagree"))
    gap_is_zero = gap_area !== nothing && iszero(gap_area)
    coverage_complete && !gap_is_zero && throw(ArgumentError(
        "complete coverage requires exactly zero gap_area"))
    classifiable && !(coverage_complete && gap_is_zero) &&
        throw(ArgumentError(
            "classifiable signatures require complete zero-gap coverage"))

    regular_cells = _rofb_signature_nonnegative_int(
        _rofb_signature_get(object, "regular_cell_count", "signature.diagnostics"),
        "signature.diagnostics.regular_cell_count";
        maximum=config["max_cells"])
    facets = _rofb_signature_nonnegative_int(
        _rofb_signature_get(object, "facet_count", "signature.diagnostics"),
        "signature.diagnostics.facet_count";
        maximum=config["max_facets"])
    internal_facets = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            object, "internal_facet_count", "signature.diagnostics"),
        "signature.diagnostics.internal_facet_count";
        maximum=facets)
    strata = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            object, "excluded_lower_dimensional_strata_count",
            "signature.diagnostics"),
        "signature.diagnostics.excluded_lower_dimensional_strata_count")
    budgeted = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            object, "budgeted_matrix_elements", "signature.diagnostics"),
        "signature.diagnostics.budgeted_matrix_elements";
        maximum=config["max_matrix_elements"])

    return Dict{String,Any}(
        "eligibility_reasons" => reasons,
        "coverage_complete" => coverage_complete,
        "gap_area" => gap_area,
        "has_ambiguity" => has_ambiguity,
        "regular_cell_count" => regular_cells,
        "facet_count" => facets,
        "internal_facet_count" => internal_facets,
        "excluded_lower_dimensional_strata_count" => strata,
        "budgeted_matrix_elements" => budgeted,
    )
end

function _rofb_validate_signature_structure(signature)
    object = _rofb_signature_exact_keys(signature, (
        "schema_version",
        "classifier_version",
        "scope",
        "field_sha256",
        "signature_sha256",
        "axis_ids",
        "output_ids",
        "config",
        "classifiable",
        "component_classifications",
        "gradient_families",
        "features",
        "diagnostics",
    ), "signature")
    schema_version = _rofb_signature_string(
        _rofb_signature_get(object, "schema_version", "signature"),
        "signature.schema_version")
    schema_version == RO_FIELD_SIGNATURE_SCHEMA_VERSION || throw(ArgumentError(
        "unsupported RO-field signature schema version"))
    classifier_version = _rofb_signature_string(
        _rofb_signature_get(object, "classifier_version", "signature"),
        "signature.classifier_version")
    classifier_version == RO_FIELD_SIGNATURE_CLASSIFIER_VERSION ||
        throw(ArgumentError("unsupported RO-field signature classifier version"))
    scope = _rofb_signature_string(
        _rofb_signature_get(object, "scope", "signature"), "signature.scope")
    scope == RO_FIELD_SIGNATURE_SCOPE || throw(ArgumentError(
        "unsupported RO-field signature scope"))
    field_sha256 = _rofb_signature_string(
        _rofb_signature_get(object, "field_sha256", "signature"),
        "signature.field_sha256")
    occursin(_RO_FIELD_SIGNATURE_SHA256_PATTERN, field_sha256) ||
        throw(ArgumentError("invalid signature.field_sha256"))
    signature_sha256 = _rofb_signature_string(
        _rofb_signature_get(object, "signature_sha256", "signature"),
        "signature.signature_sha256")
    occursin(_RO_FIELD_SIGNATURE_SHA256_PATTERN, signature_sha256) ||
        throw(ArgumentError("invalid signature.signature_sha256"))

    axis_ids = _rofb_signature_identifier_vector(
        _rofb_signature_get(object, "axis_ids", "signature"),
        "signature.axis_ids"; expected=2)
    output_ids = _rofb_signature_identifier_vector(
        _rofb_signature_get(object, "output_ids", "signature"),
        "signature.output_ids")
    output_count = length(output_ids)
    config = _rofb_signature_config(
        _rofb_signature_get(object, "config", "signature"))
    classifiable = _rofb_signature_bool(
        _rofb_signature_get(object, "classifiable", "signature"),
        "signature.classifiable")

    components_raw = _rofb_signature_get(
        object, "component_classifications", "signature")
    components_raw isa AbstractVector || throw(ArgumentError(
        "signature.component_classifications must be an array"))
    length(components_raw) == 2output_count || throw(ArgumentError(
        "component classifications must cover output_ids × axis_ids exactly"))
    components = Dict{String,Any}[]
    component_classes = [String[] for _ in 1:output_count]
    component_index = 0
    for (output_index, output_id) in enumerate(output_ids),
        (axis_index, axis_id) in enumerate(axis_ids)
        component_index += 1
        item = _rofb_signature_exact_keys(
            components_raw[component_index],
            ("output_id", "axis_id", "classification"),
            "signature.component_classifications[$component_index]")
        stored_output = _rofb_signature_string(
            _rofb_signature_get(item, "output_id", "signature component"),
            "signature component output_id")
        stored_axis = _rofb_signature_string(
            _rofb_signature_get(item, "axis_id", "signature component"),
            "signature component axis_id")
        stored_output == output_id && stored_axis == axis_id ||
            throw(ArgumentError(
                "component classifications are not in output × axis order"))
        classification = _rofb_signature_string(
            _rofb_signature_get(item, "classification", "signature component"),
            "signature component classification")
        classification in _RO_FIELD_SIGNATURE_COMPONENT_CLASSES ||
            throw(ArgumentError(
                "unsupported component classification: $classification"))
        push!(component_classes[output_index], classification)
        push!(components, Dict{String,Any}(
            "output_id" => output_id,
            "axis_id" => axis_id,
            "classification" => classification,
        ))
    end

    families_raw = _rofb_signature_get(object, "gradient_families", "signature")
    families_raw isa AbstractVector && length(families_raw) == output_count ||
        throw(ArgumentError(
            "signature.gradient_families must cover output_ids exactly"))
    families = Dict{String,Any}[]
    for (output_index, output_id) in enumerate(output_ids)
        item = _rofb_signature_exact_keys(
            families_raw[output_index], ("output_id", "gradient_family"),
            "signature.gradient_families[$output_index]")
        stored_output = _rofb_signature_string(
            _rofb_signature_get(item, "output_id", "signature gradient family"),
            "signature gradient-family output_id")
        stored_output == output_id || throw(ArgumentError(
            "gradient families are not in output order"))
        family = _rofb_signature_string(
            _rofb_signature_get(
                item, "gradient_family", "signature gradient family"),
            "signature gradient_family")
        family in _RO_FIELD_SIGNATURE_GRADIENT_FAMILIES || throw(ArgumentError(
            "unsupported gradient family: $family"))
        family == _rofb_gradient_family(component_classes[output_index]) ||
            throw(ArgumentError(
                "gradient family disagrees with component classifications"))
        push!(families, Dict{String,Any}(
            "output_id" => output_id,
            "gradient_family" => family,
        ))
    end

    features_raw = _rofb_signature_exact_keys(
        _rofb_signature_get(object, "features", "signature"), (
            "regular_cell_sign_patterns",
            "internal_facet_transition_tokens",
            "internal_facet_count",
            "mixed_sign_facet_count",
            "coupled_jump_count",
        ), "signature.features")
    patterns_raw = _rofb_signature_get(
        features_raw, "regular_cell_sign_patterns", "signature.features")
    patterns_raw isa AbstractVector || throw(ArgumentError(
        "regular_cell_sign_patterns must be an array"))
    patterns = String[]
    pattern_rows = Vector{Vector{String}}()
    for (index, raw_pattern) in enumerate(patterns_raw)
        pattern, rows = _rofb_signature_pattern_rows(
            raw_pattern, output_count,
            "signature.features.regular_cell_sign_patterns[$index]")
        push!(patterns, pattern)
        push!(pattern_rows, rows)
    end
    issorted(patterns) || throw(ArgumentError(
        "regular_cell_sign_patterns must be sorted"))

    transitions_raw = _rofb_signature_get(
        features_raw, "internal_facet_transition_tokens", "signature.features")
    transitions_raw isa AbstractVector || throw(ArgumentError(
        "internal_facet_transition_tokens must be an array"))
    transitions = String[]
    transition_endpoints = Vector{NTuple{2,String}}()
    for (index, raw_token) in enumerate(transitions_raw)
        token = _rofb_signature_string(
            raw_token,
            "signature.features.internal_facet_transition_tokens[$index]")
        endpoints = String.(split(token, "<->"; keepempty=true))
        length(endpoints) == 2 || throw(ArgumentError(
            "invalid internal facet transition token: $token"))
        for endpoint in endpoints
            _rofb_signature_pattern_rows(
                endpoint, output_count, "facet transition endpoint")
        end
        endpoints[1] <= endpoints[2] || throw(ArgumentError(
            "facet transition endpoints must be in canonical order"))
        push!(transitions, token)
        push!(transition_endpoints, (endpoints[1], endpoints[2]))
    end
    issorted(transitions) || throw(ArgumentError(
        "internal_facet_transition_tokens must be sorted"))

    internal_count = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            features_raw, "internal_facet_count", "signature.features"),
        "signature.features.internal_facet_count";
        maximum=config["max_facets"])
    mixed_count = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            features_raw, "mixed_sign_facet_count", "signature.features"),
        "signature.features.mixed_sign_facet_count"; maximum=internal_count)
    coupled_count = _rofb_signature_nonnegative_int(
        _rofb_signature_get(
            features_raw, "coupled_jump_count", "signature.features"),
        "signature.features.coupled_jump_count"; maximum=internal_count)

    diagnostics = _rofb_signature_diagnostics(
        _rofb_signature_get(object, "diagnostics", "signature"),
        config,
        classifiable,
    )
    diagnostics["internal_facet_count"] == internal_count ||
        throw(ArgumentError(
            "feature and diagnostic internal_facet_count values disagree"))
    output_factor = BigInt(output_count)
    expected_matrix_elements =
        BigInt(diagnostics["regular_cell_count"]) * output_factor * 2 +
        BigInt(diagnostics["facet_count"]) * output_factor * 4
    expected_matrix_elements == diagnostics["budgeted_matrix_elements"] ||
        throw(ArgumentError(
            "budgeted_matrix_elements does not match cells/facets/outputs"))

    if classifiable
        length(patterns) == diagnostics["regular_cell_count"] ||
            throw(ArgumentError(
                "cell pattern count does not match regular_cell_count"))
        length(transitions) == internal_count || throw(ArgumentError(
            "transition-token count does not match internal_facet_count"))
        pattern_set = Set(patterns)
        all(endpoint -> endpoint[1] in pattern_set && endpoint[2] in pattern_set,
            transition_endpoints) || throw(ArgumentError(
            "facet transition references an absent cell sign pattern"))

        derived_signs = [Symbol[] for _ in 1:(2output_count)]
        for rows in pattern_rows
            for output_index in 1:output_count, axis_index in 1:2
                character = rows[output_index][axis_index]
                push!(derived_signs[(output_index - 1) * 2 + axis_index],
                    _rofb_signature_character_sign(character))
            end
        end
        for output_index in 1:output_count, axis_index in 1:2
            expected_class = _rofb_component_classification(
                derived_signs[(output_index - 1) * 2 + axis_index])
            component_classes[output_index][axis_index] == expected_class ||
                throw(ArgumentError(
                    "component classification disagrees with cell sign patterns"))
        end
        all(classification -> classification != "unknown",
            Iterators.flatten(component_classes)) || throw(ArgumentError(
            "classifiable signatures cannot contain unknown components"))
    else
        isempty(patterns) && isempty(transitions) || throw(ArgumentError(
            "unclassifiable signatures cannot index cell/facet sign tokens"))
        coupled_count == 0 || throw(ArgumentError(
            "unclassifiable signatures cannot claim coupled jumps"))
        all(==("unknown"), Iterators.flatten(component_classes)) ||
            throw(ArgumentError(
                "unclassifiable signatures must use unknown components"))
        all(item -> item["gradient_family"] == "unknown", families) ||
            throw(ArgumentError(
                "unclassifiable signatures must use unknown gradient families"))
    end

    features = Dict{String,Any}(
        "regular_cell_sign_patterns" => patterns,
        "internal_facet_transition_tokens" => transitions,
        "internal_facet_count" => internal_count,
        "mixed_sign_facet_count" => mixed_count,
        "coupled_jump_count" => coupled_count,
    )
    return Dict{String,Any}(
        "schema_version" => schema_version,
        "classifier_version" => classifier_version,
        "scope" => scope,
        "field_sha256" => field_sha256,
        "signature_sha256" => signature_sha256,
        "axis_ids" => axis_ids,
        "output_ids" => output_ids,
        "config" => config,
        "classifiable" => classifiable,
        "component_classifications" => components,
        "gradient_families" => families,
        "features" => features,
        "diagnostics" => diagnostics,
    )
end

function _rofb_signature_identity_from_normalized(signature::AbstractDict)
    return Dict{String,Any}(
        key => signature[key] for key in (
            "schema_version",
            "classifier_version",
            "scope",
            "field_sha256",
            "axis_ids",
            "output_ids",
            "config",
            "classifiable",
            "component_classifications",
            "gradient_families",
            "features",
            "diagnostics",
        )
    )
end

"""
    ro_field_signature_identity_payload(signature)

Validate and materialize an exact v1 RO-field signature, then return the
canonical content-identity payload (every field except `signature_sha256`).
"""
function ro_field_signature_identity_payload(signature)
    normalized = _rofb_validate_signature_structure(signature)
    return _rofb_signature_identity_from_normalized(normalized)
end

"""
    validate_ro_field_signature!(signature)

Strict trust-boundary validation for an RO-field signature. The returned
`Dict{String,Any}` is normalized and detached from JSON3/symbol-keyed inputs.
"""
function validate_ro_field_signature!(signature)
    normalized = _rofb_validate_signature_structure(signature)
    expected = canonical_hash(
        _rofb_signature_identity_from_normalized(normalized))
    normalized["signature_sha256"] == expected || throw(ArgumentError(
        "RO-field signature content does not match signature_sha256"))
    return normalized
end

function _rofb_result(
    field_sha256::String,
    axis_ids::Vector{String},
    output_ids::Vector{String},
    config::ROFieldSignatureConfig,
    classifiable::Bool,
    components::Vector{Dict{String,Any}},
    families::Vector{Dict{String,Any}},
    features::Dict{String,Any},
    diagnostics::Dict{String,Any},
)
    config_dict = _rofb_config_dict(config)
    result = Dict{String,Any}(
        "schema_version" => RO_FIELD_SIGNATURE_SCHEMA_VERSION,
        "classifier_version" => RO_FIELD_SIGNATURE_CLASSIFIER_VERSION,
        "scope" => RO_FIELD_SIGNATURE_SCOPE,
        "field_sha256" => field_sha256,
        "signature_sha256" => repeat("0", 64),
        "axis_ids" => axis_ids,
        "output_ids" => output_ids,
        "config" => config_dict,
        "classifiable" => classifiable,
        "component_classifications" => components,
        "gradient_families" => families,
        "features" => features,
        "diagnostics" => diagnostics,
    )
    result["signature_sha256"] = canonical_hash(
        ro_field_signature_identity_payload(result))
    return validate_ro_field_signature!(result)
end

"""
    classify_ro_cell_complex(complex, field_sha256;
        axis_ids, output_ids, config=ROFieldSignatureConfig(), cancel_check=()->nothing)

Create a versioned gradient-sign signature for the full-dimensional regular
cells of an exact two-input RO cell complex. The result deliberately makes no
claim about singular strata, finite-domain global monotonicity, logic gates,
or synergy. If coverage has a positive-area gap, geometry is ambiguous, or a
cell is set-valued, every scientific classification is `"unknown"` and no
label is selected from that cell.
"""
function classify_ro_cell_complex(
    complex::ROCellComplex2D,
    field_sha256;
    axis_ids,
    output_ids,
    config::ROFieldSignatureConfig=ROFieldSignatureConfig(),
    cancel_check=() -> nothing,
)
    field_hash = String(field_sha256)
    occursin(_RO_FIELD_SIGNATURE_SHA256_PATTERN, field_hash) ||
        throw(ArgumentError("field_sha256 must contain 64 lowercase hex digits"))

    axis_count = length(axis_ids)
    output_count = length(output_ids)
    axis_count == 2 || throw(DimensionMismatch(
        "axis_ids must contain exactly two ordered identifiers"))
    output_count == length(complex.output_indices) || throw(DimensionMismatch(
        "output_ids must match complex.output_indices"))
    output_count > 0 || throw(ArgumentError(
        "at least one output identifier is required"))

    # Every declared limit is checked from dimensions alone, before callbacks,
    # label inspection, or any cell/facet feature loop. BigInt prevents the
    # preflight itself from overflowing for adversarial dimensions.
    cell_count = length(complex.cells)
    facet_count = length(complex.facets)
    _rofb_limit(:cells, BigInt(cell_count), config.max_cells)
    _rofb_limit(:facets, BigInt(facet_count), config.max_facets)
    matrix_elements = BigInt(cell_count) * BigInt(output_count) * 2 +
        BigInt(facet_count) * BigInt(output_count) * 4
    _rofb_limit(:matrix_elements, matrix_elements,
        config.max_matrix_elements)

    ordered_axis_ids = _rofb_normalize_ids(axis_ids, 2, "axis_ids")
    ordered_output_ids = _rofb_normalize_ids(
        output_ids, output_count, "output_ids")
    isfinite(complex.geometry_tolerance) &&
        0 <= complex.geometry_tolerance <=
            _RO_FIELD_SIGNATURE_MAX_GEOMETRY_TOLERANCE ||
        throw(ArgumentError(
            "complex.geometry_tolerance is outside the certified engine bound"))
    isdefined(@__MODULE__, :_ro_field_exact_domain_tolerances) ||
        throw(ArgumentError(
            "the exact-domain coverage certifier is unavailable"))
    domain_bounds = [
        (complex.domain.lower_log10[index], complex.domain.upper_log10[index])
        for index in 1:2
    ]
    _, _, _, certified_area_tolerance = getfield(
        @__MODULE__, :_ro_field_exact_domain_tolerances)(domain_bounds)
    cancel_check()
    reasons = String[]
    isfinite(complex.domain_area) && complex.domain_area > 0 ||
        push!(reasons, "invalid_domain_area")
    gap_reason = if complex.gap_area === nothing
        "gap_area_unknown"
    elseif !isfinite(complex.gap_area)
        "gap_area_nonfinite"
    elseif complex.gap_area > certified_area_tolerance
        "positive_area_gap"
    elseif complex.gap_area < -certified_area_tolerance
        "negative_gap_area"
    else
        nothing
    end
    gap_reason === nothing || push!(reasons, gap_reason)
    effective_coverage_complete = complex.coverage_complete &&
        gap_reason === nothing
    effective_coverage_complete || push!(reasons, "coverage_incomplete")
    complex.has_ambiguity && push!(reasons, "ambiguous_complex")

    labels_by_cell = Dict{Int,ROAffineLabel2D}()
    cell_ids = Set{Int}()
    for cell in complex.cells
        cancel_check()
        if cell.id in cell_ids
            push!(reasons, "duplicate_cell_id")
        else
            push!(cell_ids, cell.id)
        end
        if cell.set_valued
            push!(reasons, "set_valued_cell")
            continue
        elseif length(cell.labels) != 1
            push!(reasons, isempty(cell.labels) ?
                "unlabelled_cell" : "multi_label_cell")
            continue
        end

        label = only(cell.labels)
        matrix = label.reaction_order_matrix
        if size(matrix) != (output_count, 2)
            push!(reasons, "invalid_label_matrix_shape")
            continue
        elseif !all(isfinite, matrix)
            push!(reasons, "nonfinite_label_matrix")
            continue
        elseif length(label.output_offset) != output_count ||
               !all(isfinite, label.output_offset)
            push!(reasons, "invalid_label_offset")
            continue
        end
        labels_by_cell[cell.id] = label
    end

    internal_facets = ROFacet2D[]
    for facet in complex.facets
        cancel_check()
        facet.kind === :internal || continue
        push!(internal_facets, facet)
        incident_ids = unique(facet.incident_cell_ids)
        if length(incident_ids) != 2 ||
           any(cell_id -> !(cell_id in cell_ids), incident_ids)
            push!(reasons, "invalid_internal_facet_incidence")
        end
    end

    reasons = sort!(unique(reasons))
    classifiable = isempty(reasons)
    serialized_gap_area = if complex.gap_area === nothing
        nothing
    elseif isfinite(complex.gap_area)
        gap_reason === nothing ? 0.0 : complex.gap_area
    else
        # Preserve the distinction in `gap_area_nonfinite` while keeping the
        # stored trust-boundary document valid canonical JSON.
        nothing
    end
    diagnostics = Dict{String,Any}(
        "eligibility_reasons" => reasons,
        "coverage_complete" => effective_coverage_complete,
        "gap_area" => serialized_gap_area,
        "has_ambiguity" => complex.has_ambiguity,
        "regular_cell_count" => cell_count,
        "facet_count" => facet_count,
        "internal_facet_count" => length(internal_facets),
        "excluded_lower_dimensional_strata_count" =>
            length(complex.singular_strata),
        "budgeted_matrix_elements" => matrix_elements,
    )

    if !classifiable
        components, families = _rofb_unknown_components(
            ordered_axis_ids, ordered_output_ids)
        features = Dict{String,Any}(
            "regular_cell_sign_patterns" => String[],
            "internal_facet_transition_tokens" => String[],
            "internal_facet_count" => length(internal_facets),
            "mixed_sign_facet_count" =>
                count(facet -> facet.mixed_sign, internal_facets),
            "coupled_jump_count" => 0,
        )
        return _rofb_result(field_hash, ordered_axis_ids,
            ordered_output_ids, config, false, components, families,
            features, diagnostics)
    end

    cell_signs = Dict{Int,Matrix{Symbol}}()
    cell_patterns = Dict{Int,String}()
    component_signs = [Symbol[] for _ in 1:(output_count * 2)]
    for cell in complex.cells
        cancel_check()
        matrix = labels_by_cell[cell.id].reaction_order_matrix
        signs = Matrix{Symbol}(undef, output_count, 2)
        for output_index in 1:output_count, axis_index in 1:2
            sign = _rofb_value_sign(
                matrix[output_index, axis_index], config.zero_tolerance)
            signs[output_index, axis_index] = sign
            push!(component_signs[(output_index - 1) * 2 + axis_index], sign)
        end
        cell_signs[cell.id] = signs
        cell_patterns[cell.id] = _rofb_cell_pattern(signs)
    end

    components = Dict{String,Any}[]
    families = Dict{String,Any}[]
    for output_index in 1:output_count
        cancel_check()
        output_component_classes = String[]
        for axis_index in 1:2
            classification = _rofb_component_classification(
                component_signs[(output_index - 1) * 2 + axis_index])
            push!(output_component_classes, classification)
            push!(components, Dict{String,Any}(
                "output_id" => ordered_output_ids[output_index],
                "axis_id" => ordered_axis_ids[axis_index],
                "classification" => classification,
            ))
        end
        push!(families, Dict{String,Any}(
            "output_id" => ordered_output_ids[output_index],
            "gradient_family" =>
                _rofb_gradient_family(output_component_classes),
        ))
    end

    transition_tokens = String[]
    mixed_sign_facet_count = 0
    coupled_jump_count = 0
    for facet in internal_facets
        cancel_check()
        mixed_sign_facet_count += facet.mixed_sign
        first_id, second_id = sort(unique(facet.incident_cell_ids))
        endpoint_patterns = sort(String[
            cell_patterns[first_id], cell_patterns[second_id]])
        push!(transition_tokens,
            endpoint_patterns[1] * "<->" * endpoint_patterns[2])

        first_matrix = labels_by_cell[first_id].reaction_order_matrix
        second_matrix = labels_by_cell[second_id].reaction_order_matrix
        changed_components = count(
            abs(first_matrix[index] - second_matrix[index]) >
                config.zero_tolerance for index in eachindex(first_matrix))
        coupled_jump_count += changed_components >= 2
    end

    features = Dict{String,Any}(
        "regular_cell_sign_patterns" =>
            sort!(collect(values(cell_patterns))),
        "internal_facet_transition_tokens" => sort!(transition_tokens),
        "internal_facet_count" => length(internal_facets),
        "mixed_sign_facet_count" => mixed_sign_facet_count,
        "coupled_jump_count" => coupled_jump_count,
    )
    return _rofb_result(field_hash, ordered_axis_ids, ordered_output_ids,
        config, true, components, families, features, diagnostics)
end
