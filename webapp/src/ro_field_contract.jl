# Bounded multi-input reaction-order field producer and interchange serializer.
#
# This owner is intentionally separate from the legacy RPB1 path codec.  It
# produces only the additive `bne-ro-field/v1.0.0` family and only returns an
# inline artifact after the complete requested demonstration-scale computation
# and serialization have succeeded.

const RO_FIELD_REQUEST_VERSION = "bne-ro-field-request/v1.0.0"
const RO_FIELD_SAMPLER_VERSION = "bne-ro-field-sampler/v1.0.0"
const RO_FIELD_CELL_COMPLEX_VERSION = "bne-ro-cell-complex-2d/v1.0.0"

const MAX_SYNC_RO_FIELD_AXES = 4
const MAX_SYNC_RO_FIELD_OUTPUTS = 4
const MAX_SYNC_RO_FIELD_POINTS = 4_096
const MAX_SYNC_RO_FIELD_INLINE_BYTES = 4 * 1024 * 1024
const MAX_SYNC_RO_FIELD_DEADLINE_SECONDS = 30.0

const MAX_SYNC_RO_FIELD_EXACT_CANDIDATES = 4_096
const MAX_SYNC_RO_FIELD_EXACT_CELLS = 128
const MAX_SYNC_RO_FIELD_EXACT_SINGULAR_STRATA = 128
const MAX_SYNC_RO_FIELD_EXACT_PAIR_CHECKS = 8_192
const MAX_SYNC_RO_FIELD_EXACT_FACETS = 512
const MAX_SYNC_RO_FIELD_EXACT_STORED_ITEMS = 4_096
const MAX_SYNC_RO_FIELD_GEOMETRY_TOLERANCE = 1e-6
const MAX_SYNC_RO_FIELD_RELATIVE_GEOMETRY_TOLERANCE = 1e-6

"""A structured synchronous RO-field capability or resource rejection."""
struct ROFieldRequestError <: Exception
    code::String
    msg::String
    computed::Bool
    stored::Bool
end

ROFieldRequestError(code::AbstractString, msg::AbstractString;
                    computed::Bool=false, stored::Bool=false) =
    ROFieldRequestError(String(code), String(msg), computed, stored)
Base.showerror(io::IO, err::ROFieldRequestError) = print(io, err.msg)

Base.@kwdef struct NormalizedROFieldRequest
    representation::Symbol
    network_ir_hash::String
    domain::Dict{String,Any}
    outputs::Dict{String,Any}
    component_order::Vector{Dict{String,String}}
    axis_indices::Vector{Int}
    axis_coordinates_declared::Vector{Vector{Float64}}
    axis_coordinates_engine_log10::Vector{Vector{Float64}}
    output_indices::Vector{Int}
    output_reference_log10::Vector{Float64}
    fixed_engine_logqK::Vector{Float64}
    log_basis::Symbol
    work_budget::Dict{String,Any}
    exact_limits::Union{Nothing,ROCellComplexBuildLimits} = nothing
    geometry_tolerance::Float64 = 1e-9
    normalized_configuration::Dict{String,Any}
    estimated_payload_bytes::Int
end

const _RO_FIELD_TOP_LEVEL_KEYS = Set((
    "schema_version", "network", "network_ir_hash", "session_id",
    "representation", "domain", "outputs", "sampling", "exact_options",
    "work_budget", "storage",
))
const _RO_FIELD_NETWORK_KEYS = Set((
    "ir_schema_version", "label", "species", "reactions", "observables",
    "parameter_distributions", "compartments", "provenance", "extensions",
))
const _RO_FIELD_DOMAIN_KEYS = Set((
    "domain_kind", "coordinate_space", "log_basis", "axis_order", "axes",
    "fixed_background",
))
const _RO_FIELD_AXIS_KEYS = Set((
    "axis_id", "symbol", "coordinate_kind", "orientation", "reference", "bounds",
))
const _RO_FIELD_BACKGROUND_KEYS = Set((
    "parameter_id", "symbol", "coordinate_kind", "reference", "log_value",
))
const _RO_FIELD_REFERENCE_KEYS = Set(("value", "unit"))
const _RO_FIELD_BOUNDS_KEYS = Set(("lower", "upper"))
const _RO_FIELD_OUTPUTS_KEYS = Set(("output_order", "items"))
const _RO_FIELD_OUTPUT_KEYS = Set((
    "output_id", "symbol", "observable_kind", "reference",
))
const _RO_FIELD_SAMPLING_KEYS = Set(("scheme", "axis_coordinates"))
const _RO_FIELD_WORK_BUDGET_KEYS = Set((
    "work_unit_kind", "max_evaluated_items", "max_stored_items",
    "max_payload_bytes", "deadline_seconds",
))
const _RO_FIELD_STORAGE_KEYS = Set(("mode",))
const _RO_FIELD_EXACT_OPTIONS_KEYS = Set(("geometry_tolerance", "limits"))
const _RO_FIELD_EXACT_LIMIT_KEYS = Set((
    "max_candidate_regimes", "max_cells", "max_singular_strata",
    "max_pair_checks", "max_facets",
))
const _RO_FIELD_IDENTIFIER_PATTERN = r"^[A-Za-z][A-Za-z0-9._:-]{0,127}$"
const _RO_FIELD_SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
const _RO_FIELD_SHA256_PATTERN = r"^[0-9a-f]{64}$"

function _ro_field_object(raw, path::AbstractString)
    raw isa AbstractDict || throw(ArgumentError("$path must be an object"))
    return raw
end

function _ro_field_array(raw, path::AbstractString)
    raw isa AbstractVector || throw(ArgumentError("$path must be an array"))
    return raw
end

function _ro_field_exact_keys(raw, allowed::Set{String}, path::AbstractString;
                              required::Set{String}=allowed)
    object = _ro_field_object(raw, path)
    observed = Set(String(key) for key in keys(object))
    unknown = sort!(collect(setdiff(observed, allowed)))
    isempty(unknown) || throw(ArgumentError(
        "$path contains unsupported keys: $(join(unknown, ", "))"))
    missing = sort!(collect(setdiff(required, observed)))
    isempty(missing) || throw(ArgumentError(
        "$path is missing required keys: $(join(missing, ", "))"))
    return object
end

function _ro_field_identifier(raw, path::AbstractString)
    value = _request_string(raw, path)
    occursin(_RO_FIELD_IDENTIFIER_PATTERN, value) || throw(ArgumentError(
        "$path must start with a letter and contain at most 128 safe identifier characters"))
    return value
end

function _ro_field_safe_id(raw, path::AbstractString)
    value = _request_string(raw, path)
    occursin(_RO_FIELD_SAFE_ID_PATTERN, value) || throw(ArgumentError(
        "$path must start with an ASCII letter or digit and contain at most " *
        "128 safe identifier characters"))
    return value
end

function _ro_field_lowercase_sha256(raw, path::AbstractString)
    value = _request_string(raw, path)
    occursin(_RO_FIELD_SHA256_PATTERN, value) || throw(ArgumentError(
        "$path must be exactly 64 lowercase hexadecimal characters"))
    return value
end

function _ro_field_positive_int(raw, path::AbstractString; maximum::Int)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$path must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$path is outside the supported integer range"))
    end
    value >= 1 || throw(ArgumentError("$path must be positive"))
    value <= maximum || throw(ROFieldRequestError(
        "ro_field_work_budget_exceeded",
        "$path exceeds the synchronous demonstration limit of $maximum. " *
        "No RO-field computation was started and nothing was stored; this " *
        "resource bound is not evidence of scientific infeasibility.",
    ))
    return value
end

function _ro_field_reference(raw, path::AbstractString)
    object = _ro_field_exact_keys(raw, _RO_FIELD_REFERENCE_KEYS, path)
    value = _request_finite_real(_raw_get(object, :value, nothing), "$path.value")
    value > 0 || throw(ArgumentError("$path.value must be positive"))
    unit = _request_string(_raw_get(object, :unit, nothing), "$path.unit")
    return Dict{String,Any}("value" => value, "unit" => unit), log10(value)
end

function _ro_field_coordinate_to_engine_log10(value::Real, basis::Symbol,
                                               reference_log10::Real)
    basis === :log10 && return Float64(value) + Float64(reference_log10)
    basis === :natural_log &&
        return Float64(value) / log(10.0) + Float64(reference_log10)
    error("internal error: unsupported RO-field log basis")
end

function _ro_field_engine_log10_to_coordinate(value::Real, basis::Symbol,
                                               reference_log10::Real)
    relative = Float64(value) - Float64(reference_log10)
    basis === :log10 && return relative
    basis === :natural_log && return log(10.0) * relative
    error("internal error: unsupported RO-field log basis")
end

function _ro_field_coordinate_kind(model, index::Int)
    index <= model.d ? "conserved_total" : "binding_constant"
end

function _ro_field_validate_model_source(raw)
    source_keys = Symbol[key for key in (:network, :network_ir_hash, :session_id)
                         if _raw_haskey(raw, key)]
    length(source_keys) == 1 || throw(ArgumentError(
        "exactly one of network, network_ir_hash, or session_id is required"))
    source = only(source_keys)
    if source === :network
        network = _ro_field_exact_keys(
            _raw_get(raw, :network, nothing), _RO_FIELD_NETWORK_KEYS, "network")
        _raw_get(network, :ir_schema_version, nothing) == NETWORK_IR_SCHEMA_VERSION ||
            throw(ArgumentError(
                "network must be a complete $(NETWORK_IR_SCHEMA_VERSION) NetworkIR"))
        parse_network_ir(network)
    elseif source === :network_ir_hash
        _ro_field_lowercase_sha256(
            _raw_get(raw, :network_ir_hash, nothing), "network_ir_hash")
    else
        _ro_field_safe_id(_raw_get(raw, :session_id, nothing), "session_id")
    end
    return source
end

function _ro_field_bundle_hash(bundle, source::Symbol, raw)
    haskey(bundle, "network_ir_hash") || throw(ArgumentError(
        "the resolved model does not retain a NetworkIR content hash"))
    hash = lowercase(String(bundle["network_ir_hash"]))
    occursin(r"^[0-9a-f]{64}$", hash) || throw(ArgumentError(
        "the resolved model has an invalid NetworkIR content hash"))
    if source === :network_ir_hash
        requested = _ro_field_lowercase_sha256(
            _raw_get(raw, :network_ir_hash, nothing), "network_ir_hash")
        requested == hash || throw(ArgumentError(
            "network_ir_hash resolved to a model with a different content hash"))
    end
    return hash
end

function _ro_field_normalize_work_budget(raw, representation::Symbol)
    object = _ro_field_exact_keys(
        raw, _RO_FIELD_WORK_BUDGET_KEYS, "work_budget")
    expected_kind = representation === :sampled_grid ?
        "solver_samples" : "source_regime_candidates"
    kind = _request_string(
        _raw_get(object, :work_unit_kind, nothing), "work_budget.work_unit_kind")
    kind == expected_kind || throw(ArgumentError(
        "work_budget.work_unit_kind must be $expected_kind for this representation"))
    eval_max = representation === :sampled_grid ?
        MAX_SYNC_RO_FIELD_POINTS : MAX_SYNC_RO_FIELD_EXACT_CANDIDATES
    stored_max = representation === :sampled_grid ?
        MAX_SYNC_RO_FIELD_POINTS : MAX_SYNC_RO_FIELD_EXACT_STORED_ITEMS
    max_evaluated = _ro_field_positive_int(
        _raw_get(object, :max_evaluated_items, nothing),
        "work_budget.max_evaluated_items"; maximum=eval_max)
    max_stored = _ro_field_positive_int(
        _raw_get(object, :max_stored_items, nothing),
        "work_budget.max_stored_items"; maximum=stored_max)
    max_payload = _ro_field_positive_int(
        _raw_get(object, :max_payload_bytes, nothing),
        "work_budget.max_payload_bytes"; maximum=MAX_SYNC_RO_FIELD_INLINE_BYTES)
    deadline_raw = _raw_get(object, :deadline_seconds, nothing)
    deadline = if deadline_raw === nothing
        nothing
    else
        value = _request_finite_real(deadline_raw, "work_budget.deadline_seconds")
        value > 0 || throw(ArgumentError(
            "work_budget.deadline_seconds must be positive or null"))
        value <= MAX_SYNC_RO_FIELD_DEADLINE_SECONDS || throw(ROFieldRequestError(
            "ro_field_work_budget_exceeded",
            "work_budget.deadline_seconds exceeds the synchronous limit of " *
            "$(MAX_SYNC_RO_FIELD_DEADLINE_SECONDS). No RO-field computation was " *
            "started and nothing was stored; this resource bound is not evidence " *
            "of scientific infeasibility.",
        ))
        value
    end
    return Dict{String,Any}(
        "work_unit_kind" => kind,
        "max_evaluated_items" => max_evaluated,
        "max_stored_items" => max_stored,
        "max_payload_bytes" => max_payload,
        "deadline_seconds" => deadline,
    )
end

function _ro_field_normalize_exact_options(raw, work_budget, domain, basis::Symbol)
    defaults = Dict{String,Int}(
        "max_candidate_regimes" => MAX_SYNC_RO_FIELD_EXACT_CANDIDATES,
        "max_cells" => MAX_SYNC_RO_FIELD_EXACT_CELLS,
        "max_singular_strata" => MAX_SYNC_RO_FIELD_EXACT_SINGULAR_STRATA,
        "max_pair_checks" => MAX_SYNC_RO_FIELD_EXACT_PAIR_CHECKS,
        "max_facets" => MAX_SYNC_RO_FIELD_EXACT_FACETS,
    )
    declared_spans = Float64[
        Float64(axis["bounds"]["upper"]) - Float64(axis["bounds"]["lower"])
        for axis in domain["axes"]
    ]
    # `geometry_tolerance` is consumed by the log10 engine. Natural-log
    # coordinates therefore have a correspondingly smaller engine span.
    engine_span_scale = basis === :natural_log ? log(10.0) : 1.0
    minimum_engine_span = minimum(declared_spans) / engine_span_scale
    maximum_geometry_tolerance = min(
        MAX_SYNC_RO_FIELD_GEOMETRY_TOLERANCE,
        MAX_SYNC_RO_FIELD_RELATIVE_GEOMETRY_TOLERANCE * minimum_engine_span,
    )
    isfinite(maximum_geometry_tolerance) && maximum_geometry_tolerance > 0 ||
        throw(ROFieldRequestError(
            "ro_field_geometry_tolerance_unsupported",
            "The declared exact domain is too small to derive a positive " *
            "Float64 geometry tolerance. No RO-field computation was started " *
            "and nothing was stored; this unresolved geometry cannot support " *
            "a coverage certificate.",
        ))
    geometry_tolerance = min(1e-9, maximum_geometry_tolerance / 10.0)
    geometry_tolerance > 0 || throw(ROFieldRequestError(
        "ro_field_geometry_tolerance_unsupported",
        "The declared exact domain underflows the bounded Float64 geometry " *
        "policy. No RO-field computation was started and nothing was stored; " *
        "this unresolved geometry cannot support a coverage certificate.",
    ))
    requested = copy(defaults)
    if raw !== nothing
        options = _ro_field_exact_keys(
            raw, _RO_FIELD_EXACT_OPTIONS_KEYS, "exact_options")
        geometry_tolerance = _request_finite_real(
            _raw_get(options, :geometry_tolerance, nothing),
            "exact_options.geometry_tolerance")
        geometry_tolerance > 0 || throw(ArgumentError(
            "exact_options.geometry_tolerance must be positive"))
        geometry_tolerance <= maximum_geometry_tolerance ||
            throw(ROFieldRequestError(
                "ro_field_geometry_tolerance_unsupported",
                "exact_options.geometry_tolerance must not exceed " *
                "$(maximum_geometry_tolerance) for this domain (absolute " *
                "cap=$(MAX_SYNC_RO_FIELD_GEOMETRY_TOLERANCE), relative cap=" *
                "$(MAX_SYNC_RO_FIELD_RELATIVE_GEOMETRY_TOLERANCE) of the " *
                "shortest engine-coordinate side). No RO-field computation " *
                "was started and nothing was stored; a looser tolerance " *
                "cannot be used as a coverage certificate.",
            ))
        limits = _ro_field_exact_keys(
            _raw_get(options, :limits, nothing),
            _RO_FIELD_EXACT_LIMIT_KEYS, "exact_options.limits")
        for (key, hard_max) in defaults
            requested[key] = _ro_field_positive_int(
                _raw_get(limits, Symbol(key), nothing),
                "exact_options.limits.$key"; maximum=hard_max)
        end
    end
    effective_candidates = min(
        requested["max_candidate_regimes"],
        Int(work_budget["max_evaluated_items"]),
    )
    effective_cells = min(
        requested["max_cells"], Int(work_budget["max_stored_items"]))
    effective_strata = min(
        requested["max_singular_strata"], Int(work_budget["max_stored_items"]))
    limits = ROCellComplexBuildLimits(
        max_candidate_regimes=effective_candidates,
        max_cells=effective_cells,
        max_singular_strata=effective_strata,
        max_pair_checks=requested["max_pair_checks"],
        max_facets=requested["max_facets"],
    )
    normalized = Dict{String,Any}(
        "geometry_tolerance" => geometry_tolerance,
        "limits" => requested,
    )
    return limits, geometry_tolerance, normalized
end

function _ro_field_normalize_domain(raw, model, representation::Symbol)
    object = _ro_field_exact_keys(raw, _RO_FIELD_DOMAIN_KEYS, "domain")
    _raw_get(object, :domain_kind, nothing) == "axis_aligned_log_box" ||
        throw(ArgumentError("domain.domain_kind must be axis_aligned_log_box"))
    _raw_get(object, :coordinate_space, nothing) == "dimensionless_log_ratio" ||
        throw(ArgumentError(
            "domain.coordinate_space must be dimensionless_log_ratio"))
    basis_text = _request_string(
        _raw_get(object, :log_basis, nothing), "domain.log_basis")
    basis_text in ("log10", "natural_log") || throw(ArgumentError(
        "domain.log_basis must be log10 or natural_log"))
    basis = Symbol(basis_text)

    axes_raw = _ro_field_array(_raw_get(object, :axes, nothing), "domain.axes")
    minimum = representation === :exact_cell_complex ? 2 : 1
    maximum = representation === :exact_cell_complex ? 2 : MAX_SYNC_RO_FIELD_AXES
    minimum <= length(axes_raw) <= maximum || throw(ArgumentError(
        "domain.axes must contain between $minimum and $maximum ordered axes"))
    order_raw = _ro_field_array(
        _raw_get(object, :axis_order, nothing), "domain.axis_order")
    length(order_raw) == length(axes_raw) || throw(ArgumentError(
        "domain.axis_order length must equal domain.axes length"))

    axes = Dict{String,Any}[]
    axis_ids = String[]
    axis_indices = Int[]
    axis_reference_logs = Float64[]
    parameter_units = String[]
    for (position, raw_axis) in enumerate(axes_raw)
        path = "domain.axes[$position]"
        axis = _ro_field_exact_keys(raw_axis, _RO_FIELD_AXIS_KEYS, path)
        axis_id = _ro_field_identifier(_raw_get(axis, :axis_id, nothing), "$path.axis_id")
        symbol = _request_string(_raw_get(axis, :symbol, nothing), "$path.symbol")
        kind = _request_string(
            _raw_get(axis, :coordinate_kind, nothing), "$path.coordinate_kind")
        kind == "external_control" && throw(ROFieldRequestError(
            "ro_field_coordinate_kind_unsupported",
            "$path.coordinate_kind external_control is not implemented by the " *
            "v1 bounded producer. No RO-field computation was started and " *
            "nothing was stored; this capability boundary is not evidence of " *
            "scientific infeasibility.",
        ))
        index = locate_sym_qK(model, Symbol(symbol))
        index === nothing && throw(ArgumentError(
            "$path.symbol is not a q/K symbol in the resolved model: $symbol"))
        index = Int(index)
        expected_kind = _ro_field_coordinate_kind(model, index)
        kind == expected_kind || throw(ArgumentError(
            "$path.coordinate_kind must be $expected_kind for model symbol $symbol"))
        representation === :exact_cell_complex && kind != "conserved_total" &&
            throw(ArgumentError(
                "exact_cell_complex axes must both be conserved_total coordinates"))
        _raw_get(axis, :orientation, nothing) == "increasing_physical_value" ||
            throw(ArgumentError(
                "$path.orientation must be increasing_physical_value"))
        reference, reference_log = _ro_field_reference(
            _raw_get(axis, :reference, nothing), "$path.reference")
        bounds_raw = _ro_field_exact_keys(
            _raw_get(axis, :bounds, nothing), _RO_FIELD_BOUNDS_KEYS, "$path.bounds")
        lower = _request_finite_real(
            _raw_get(bounds_raw, :lower, nothing), "$path.bounds.lower")
        upper = _request_finite_real(
            _raw_get(bounds_raw, :upper, nothing), "$path.bounds.upper")
        lower < upper || throw(ArgumentError(
            "$path.bounds.lower must be below $path.bounds.upper"))
        push!(axis_ids, axis_id)
        push!(axis_indices, index)
        push!(axis_reference_logs, reference_log)
        push!(parameter_units, String(reference["unit"]))
        push!(axes, Dict{String,Any}(
            "axis_id" => axis_id,
            "symbol" => symbol,
            "coordinate_kind" => kind,
            "orientation" => "increasing_physical_value",
            "reference" => reference,
            "bounds" => Dict{String,Any}("lower" => lower, "upper" => upper),
        ))
    end
    order = String[_request_string(item, "domain.axis_order[$idx]")
                   for (idx, item) in enumerate(order_raw)]
    order == axis_ids || throw(ArgumentError(
        "domain.axis_order must exactly equal the domain.axes axis_id sequence"))
    length(unique(axis_ids)) == length(axis_ids) || throw(ArgumentError(
        "domain axis_id values must be unique"))
    length(unique(axis_indices)) == length(axis_indices) || throw(ArgumentError(
        "domain axis symbols must map to distinct q/K coordinates"))

    background_raw = _ro_field_array(
        _raw_get(object, :fixed_background, nothing), "domain.fixed_background")
    backgrounds = Dict{String,Any}[]
    background_ids = String[]
    background_indices = Int[]
    fixed = fill(NaN, model.n)
    for (position, raw_background) in enumerate(background_raw)
        path = "domain.fixed_background[$position]"
        item = _ro_field_exact_keys(
            raw_background, _RO_FIELD_BACKGROUND_KEYS, path)
        parameter_id = _ro_field_identifier(
            _raw_get(item, :parameter_id, nothing), "$path.parameter_id")
        symbol = _request_string(_raw_get(item, :symbol, nothing), "$path.symbol")
        kind = _request_string(
            _raw_get(item, :coordinate_kind, nothing), "$path.coordinate_kind")
        kind == "external_control" && throw(ROFieldRequestError(
            "ro_field_coordinate_kind_unsupported",
            "$path.coordinate_kind external_control is not implemented by the " *
            "v1 bounded producer. No RO-field computation was started and " *
            "nothing was stored; this capability boundary is not evidence of " *
            "scientific infeasibility.",
        ))
        index = locate_sym_qK(model, Symbol(symbol))
        index === nothing && throw(ArgumentError(
            "$path.symbol is not a q/K symbol in the resolved model: $symbol"))
        index = Int(index)
        index in axis_indices && throw(ArgumentError(
            "$path.symbol is swept and cannot also be fixed background"))
        expected_kind = _ro_field_coordinate_kind(model, index)
        kind == expected_kind || throw(ArgumentError(
            "$path.coordinate_kind must be $expected_kind for model symbol $symbol"))
        reference, reference_log = _ro_field_reference(
            _raw_get(item, :reference, nothing), "$path.reference")
        log_value = _request_finite_real(
            _raw_get(item, :log_value, nothing), "$path.log_value")
        fixed[index] = _ro_field_coordinate_to_engine_log10(
            log_value, basis, reference_log)
        push!(background_ids, parameter_id)
        push!(background_indices, index)
        push!(parameter_units, String(reference["unit"]))
        push!(backgrounds, Dict{String,Any}(
            "parameter_id" => parameter_id,
            "symbol" => symbol,
            "coordinate_kind" => kind,
            "reference" => reference,
            "log_value" => log_value,
        ))
    end
    length(unique(background_ids)) == length(background_ids) || throw(ArgumentError(
        "fixed background parameter_id values must be unique"))
    length(unique(background_indices)) == length(background_indices) ||
        throw(ArgumentError(
            "fixed background symbols must map to distinct q/K coordinates"))
    expected_background = sort!(setdiff(collect(1:model.n), axis_indices))
    sort(background_indices) == expected_background || throw(ArgumentError(
        "domain.fixed_background must contain every non-swept model q/K symbol " *
        "exactly once"))
    length(unique(parameter_units)) <= 1 || throw(ArgumentError(
        "all q/K references must use one common concentration unit"))
    for (position, index) in enumerate(axis_indices)
        fixed[index] = axis_reference_logs[position]
    end
    all(isfinite, fixed) || error(
        "internal error: complete fixed-background normalization left a gap")

    domain = Dict{String,Any}(
        "domain_kind" => "axis_aligned_log_box",
        "coordinate_space" => "dimensionless_log_ratio",
        "log_basis" => basis_text,
        "axis_order" => axis_ids,
        "axes" => axes,
        "fixed_background" => backgrounds,
    )
    return domain, basis, axis_indices, axis_reference_logs, fixed
end

function _ro_field_normalize_outputs(raw, model, parameter_unit::String)
    object = _ro_field_exact_keys(raw, _RO_FIELD_OUTPUTS_KEYS, "outputs")
    items_raw = _ro_field_array(_raw_get(object, :items, nothing), "outputs.items")
    1 <= length(items_raw) <= MAX_SYNC_RO_FIELD_OUTPUTS || throw(ArgumentError(
        "outputs.items must contain between 1 and $(MAX_SYNC_RO_FIELD_OUTPUTS) outputs"))
    order_raw = _ro_field_array(
        _raw_get(object, :output_order, nothing), "outputs.output_order")
    length(order_raw) == length(items_raw) || throw(ArgumentError(
        "outputs.output_order length must equal outputs.items length"))
    items = Dict{String,Any}[]
    ids = String[]
    indices = Int[]
    reference_logs = Float64[]
    for (position, raw_item) in enumerate(items_raw)
        path = "outputs.items[$position]"
        item = _ro_field_exact_keys(raw_item, _RO_FIELD_OUTPUT_KEYS, path)
        output_id = _ro_field_identifier(
            _raw_get(item, :output_id, nothing), "$path.output_id")
        symbol = _request_string(_raw_get(item, :symbol, nothing), "$path.symbol")
        observable_kind = _request_string(
            _raw_get(item, :observable_kind, nothing), "$path.observable_kind")
        observable_kind == "species_concentration" || throw(ROFieldRequestError(
            "ro_field_observable_kind_unsupported",
            "$path.observable_kind positive_linear_observable is not implemented " *
            "by the v1 bounded producer. No RO-field computation was started and " *
            "nothing was stored; this capability boundary is not evidence of " *
            "scientific infeasibility.",
        ))
        index = locate_sym_x(model, Symbol(symbol))
        index === nothing && throw(ArgumentError(
            "$path.symbol is not a species in the resolved model: $symbol"))
        reference, reference_log = _ro_field_reference(
            _raw_get(item, :reference, nothing), "$path.reference")
        String(reference["unit"]) == parameter_unit || throw(ArgumentError(
            "$path.reference.unit must match the q/K concentration unit $parameter_unit"))
        push!(ids, output_id)
        push!(indices, Int(index))
        push!(reference_logs, reference_log)
        push!(items, Dict{String,Any}(
            "output_id" => output_id,
            "symbol" => symbol,
            "observable_kind" => observable_kind,
            "reference" => reference,
        ))
    end
    order = String[_request_string(item, "outputs.output_order[$idx]")
                   for (idx, item) in enumerate(order_raw)]
    order == ids || throw(ArgumentError(
        "outputs.output_order must exactly equal the outputs.items output_id sequence"))
    length(unique(ids)) == length(ids) || throw(ArgumentError(
        "output_id values must be unique"))
    length(unique(indices)) == length(indices) || throw(ArgumentError(
        "output symbols must map to distinct model species"))
    return Dict{String,Any}(
        "output_order" => ids,
        "items" => items,
    ), indices, reference_logs
end

function _ro_field_sample_payload_estimate(point_count::Int, input_count::Int,
                                           output_count::Int)
    per_point = 64 + 32 * output_count * (1 + input_count)
    return 8_192 + point_count * per_point
end

function _ro_field_exact_payload_estimate(candidate_bound::Int, output_count::Int,
                                          limits::ROCellComplexBuildLimits)
    cells = min(candidate_bound, limits.max_cells)
    strata = min(candidate_bound, limits.max_singular_strata)
    possible_facets = min(
        limits.max_facets,
        4 * cells + 16 * cells * max(cells - 1, 0) ÷ 2,
    )
    return 16_384 + cells * (768 + 64 * output_count) +
        strata * 384 + possible_facets * 448
end

function _ro_field_reject_estimated_payload(estimate::Integer, work_budget)
    limit = Int(work_budget["max_payload_bytes"])
    estimate <= limit || throw(ROFieldRequestError(
        "ro_field_payload_budget_exceeded",
        "The estimated inline RO-field data payload ($estimate bytes) exceeds " *
        "the declared limit ($limit bytes). No RO-field computation was started " *
        "and nothing was stored; this storage/resource bound is not evidence of " *
        "scientific infeasibility.",
    ))
end

"""
    normalize_ro_field_request(raw, bundle) -> NormalizedROFieldRequest

Fail-closed semantic normalization for the synchronous v1 producer. JSON Schema
is intentionally not loaded at runtime; this boundary enforces the cross-field
rank, ordering, model-symbol, reference, fixed-background, and work-limit rules
that structural JSON Schema cannot express on its own.
"""
function normalize_ro_field_request(raw, bundle)
    body = _ro_field_exact_keys(
        raw, _RO_FIELD_TOP_LEVEL_KEYS, "request";
        required=Set((
            "schema_version", "representation", "domain", "outputs",
            "work_budget", "storage",
        )))
    _raw_get(body, :schema_version, nothing) == RO_FIELD_REQUEST_VERSION ||
        throw(ArgumentError(
            "schema_version must be $(RO_FIELD_REQUEST_VERSION)"))
    source = _ro_field_validate_model_source(body)
    network_hash = _ro_field_bundle_hash(bundle, source, body)

    representation_text = _request_string(
        _raw_get(body, :representation, nothing), "representation")
    representation_text in ("sampled_grid", "exact_cell_complex") ||
        throw(ArgumentError(
            "representation must be sampled_grid or exact_cell_complex"))
    representation = Symbol(representation_text)
    model = bundle["model"]
    enforce_sync_model_budget(model)

    work_budget = _ro_field_normalize_work_budget(
        _raw_get(body, :work_budget, nothing), representation)
    storage = _ro_field_exact_keys(
        _raw_get(body, :storage, nothing), _RO_FIELD_STORAGE_KEYS, "storage")
    storage_mode = _request_string(
        _raw_get(storage, :mode, nothing), "storage.mode")
    storage_mode in ("inline", "chunked", "artifact_reference") ||
        throw(ArgumentError(
            "storage.mode must be inline, chunked, or artifact_reference"))
    storage_mode == "inline" || throw(ROFieldRequestError(
        "ro_field_storage_mode_unsupported",
        "The synchronous demonstration producer supports inline RO-field " *
        "responses only. No RO-field computation was started and nothing was " *
        "stored; this capability boundary is not evidence of scientific " *
        "infeasibility.",
    ))

    domain, basis, axis_indices, axis_reference_logs, fixed =
        _ro_field_normalize_domain(
            _raw_get(body, :domain, nothing), model, representation)
    parameter_unit = String(domain["axes"][1]["reference"]["unit"])
    outputs, output_indices, output_reference_logs = _ro_field_normalize_outputs(
        _raw_get(body, :outputs, nothing), model, parameter_unit)
    component_order = Dict{String,String}[
        Dict("output_id" => output_id, "input_axis_id" => axis_id)
        for output_id in outputs["output_order"]
        for axis_id in domain["axis_order"]
    ]

    declared_coordinates = Vector{Vector{Float64}}()
    engine_coordinates = Vector{Vector{Float64}}()
    exact_limits = nothing
    geometry_tolerance = 1e-9
    representation_options = Dict{String,Any}()
    estimate = 0
    if representation === :sampled_grid
        _raw_haskey(body, :sampling) || throw(ArgumentError(
            "sampling is required for sampled_grid"))
        !_raw_haskey(body, :exact_options) || throw(ArgumentError(
            "exact_options is not allowed for sampled_grid"))
        sampling = _ro_field_exact_keys(
            _raw_get(body, :sampling, nothing),
            _RO_FIELD_SAMPLING_KEYS, "sampling")
        _raw_get(sampling, :scheme, nothing) == "cartesian_product" ||
            throw(ArgumentError("sampling.scheme must be cartesian_product"))
        coordinates_raw = _ro_field_array(
            _raw_get(sampling, :axis_coordinates, nothing),
            "sampling.axis_coordinates")
        length(coordinates_raw) == length(axis_indices) || throw(ArgumentError(
            "sampling.axis_coordinates must contain one vector per domain axis"))
        for (position, raw_axis_coordinates) in enumerate(coordinates_raw)
            path = "sampling.axis_coordinates[$position]"
            values_raw = _ro_field_array(raw_axis_coordinates, path)
            isempty(values_raw) && throw(ArgumentError("$path must not be empty"))
            values = Float64[
                _request_finite_real(value, "$path[$idx]")
                for (idx, value) in enumerate(values_raw)
            ]
            all(values[idx] < values[idx + 1] for idx in 1:(length(values) - 1)) ||
                throw(ArgumentError("$path must be strictly increasing"))
            bounds = domain["axes"][position]["bounds"]
            all(value -> bounds["lower"] <= value <= bounds["upper"], values) ||
                throw(ArgumentError("$path values must stay within declared bounds"))
            push!(declared_coordinates, values)
            push!(engine_coordinates, [
                _ro_field_coordinate_to_engine_log10(
                    value, basis, axis_reference_logs[position])
                for value in values
            ])
        end
        point_count_big = prod(BigInt(length(values)) for values in declared_coordinates)
        point_count_big <= typemax(Int) || throw(ROFieldRequestError(
            "ro_field_work_budget_exceeded",
            "The requested Cartesian grid cannot be represented by this runtime. " *
            "No RO-field computation was started and nothing was stored; this " *
            "resource bound is not evidence of scientific infeasibility.",
        ))
        point_count = Int(point_count_big)
        point_count <= MAX_SYNC_RO_FIELD_POINTS || throw(ROFieldRequestError(
            "ro_field_work_budget_exceeded",
            "The requested Cartesian grid has $point_count points, exceeding the " *
            "synchronous limit of $(MAX_SYNC_RO_FIELD_POINTS). No RO-field " *
            "computation was started and nothing was stored; this resource bound " *
            "is not evidence of scientific infeasibility.",
        ))
        point_count <= Int(work_budget["max_evaluated_items"]) ||
            throw(ROFieldRequestError(
                "ro_field_work_budget_exceeded",
                "The requested Cartesian grid exceeds max_evaluated_items. No " *
                "RO-field computation was started and nothing was stored; this " *
                "resource bound is not evidence of scientific infeasibility.",
            ))
        point_count <= Int(work_budget["max_stored_items"]) ||
            throw(ROFieldRequestError(
                "ro_field_storage_budget_exceeded",
                "The requested Cartesian grid exceeds max_stored_items. No " *
                "RO-field computation was started and nothing was stored; this " *
                "storage bound is not evidence of scientific infeasibility.",
            ))
        representation_options["sampling"] = Dict{String,Any}(
            "scheme" => "cartesian_product",
            "axis_coordinates" => declared_coordinates,
        )
        estimate = _ro_field_sample_payload_estimate(
            point_count, length(axis_indices), length(output_indices))
    else
        !_raw_haskey(body, :sampling) || throw(ArgumentError(
            "sampling is not allowed for exact_cell_complex"))
        exact_limits, geometry_tolerance, exact_normalized =
            _ro_field_normalize_exact_options(
                _raw_get(body, :exact_options, nothing), work_budget, domain, basis)
        representation_options["exact_options"] = exact_normalized
        candidate_bound = try
            model_candidate_bound(
                model;
                maximum=exact_limits.max_candidate_regimes,
                label="RO-field exact request",
            )
        catch err
            err isa ModelCandidateBoundExceeded || rethrow()
            throw(ROFieldRequestError(
                "ro_field_work_budget_exceeded",
                "The exact-cell candidate bound exceeds the declared synchronous " *
                "limit before enumeration. No RO-field computation was started " *
                "and nothing was stored; this resource bound is not evidence of " *
                "scientific infeasibility.",
            ))
        end
        estimate = _ro_field_exact_payload_estimate(
            candidate_bound, length(output_indices), exact_limits)
    end
    _ro_field_reject_estimated_payload(estimate, work_budget)

    configuration = Dict{String,Any}(
        "schema_version" => RO_FIELD_REQUEST_VERSION,
        "network_ir_hash" => network_hash,
        "representation" => representation_text,
        "domain" => domain,
        "outputs" => outputs,
        "work_budget" => work_budget,
        "storage" => Dict{String,Any}("mode" => "inline"),
    )
    merge!(configuration, representation_options)
    return NormalizedROFieldRequest(
        representation=representation,
        network_ir_hash=network_hash,
        domain=domain,
        outputs=outputs,
        component_order=component_order,
        axis_indices=axis_indices,
        axis_coordinates_declared=declared_coordinates,
        axis_coordinates_engine_log10=engine_coordinates,
        output_indices=output_indices,
        output_reference_log10=output_reference_logs,
        fixed_engine_logqK=fixed,
        log_basis=basis,
        work_budget=work_budget,
        exact_limits=exact_limits,
        geometry_tolerance=geometry_tolerance,
        normalized_configuration=configuration,
        estimated_payload_bytes=estimate,
    )
end

function _ro_field_row_major_indices(shape::Tuple)
    reversed_shape = Tuple(reverse(collect(shape)))
    return (CartesianIndex(Tuple(reverse(collect(Tuple(index)))))
            for index in CartesianIndices(reversed_shape))
end

_ro_field_point(values) = Float64[Float64(values[1]), Float64(values[2])]
_ro_field_regime_id(index::Integer) = "regime-$(Int(index))"
_ro_field_cell_id(index::Integer) = "cell-$(Int(index))"
_ro_field_facet_id(index::Integer) = "facet-$(Int(index))"
_ro_field_stratum_id(index::Integer) = "stratum-$(Int(index))"

function _ro_field_serialize_sampled(field::SampledReactionOrderField,
                                     normalized::NormalizedROFieldRequest)
    grid_shape = size(field.validity)
    length(grid_shape) == length(normalized.axis_indices) || error(
        "sampled RO-field grid rank disagrees with the normalized request")
    output_count = length(normalized.output_indices)
    input_count = length(normalized.axis_indices)
    size(field.output_log10) == (grid_shape..., output_count) || error(
        "sampled RO-field output tensor shape is inconsistent")
    size(field.reaction_orders) == (grid_shape..., output_count, input_count) ||
        error("sampled RO-field reaction-order tensor shape is inconsistent")

    validity = Bool[]
    regime_ids = Union{Nothing,String}[]
    output_values = Union{Nothing,Float64}[]
    reaction_order_values = Union{Nothing,Float64}[]
    for grid_index in _ro_field_row_major_indices(grid_shape)
        valid = Bool(field.validity[grid_index])
        push!(validity, valid)
        regime = Int(field.regime_ids[grid_index])
        push!(regime_ids, valid && regime >= 1 ? _ro_field_regime_id(regime) : nothing)
        point = Tuple(grid_index)
        for output_position in 1:output_count
            if valid
                value = field.output_log10[point..., output_position]
                isfinite(value) || error(
                    "valid sampled RO-field point contains a non-finite output")
                push!(output_values, _ro_field_engine_log10_to_coordinate(
                    value, normalized.log_basis,
                    normalized.output_reference_log10[output_position]))
            else
                push!(output_values, nothing)
            end
        end
        for output_position in 1:output_count, input_position in 1:input_count
            if valid
                value = field.reaction_orders[
                    point..., output_position, input_position]
                isfinite(value) || error(
                    "valid sampled RO-field point contains a non-finite reaction order")
                push!(reaction_order_values, Float64(value))
            else
                push!(reaction_order_values, nothing)
            end
        end
    end
    return Dict{String,Any}(
        "sampling_scheme" => "cartesian_product",
        "axis_coordinates" => normalized.axis_coordinates_declared,
        "grid_shape" => collect(grid_shape),
        "output_shape" => [collect(grid_shape); output_count],
        "reaction_order_shape" => [collect(grid_shape); output_count; input_count],
        "flatten_order" => "row_major_last_axis_fastest",
        "output_values" => output_values,
        "reaction_order_values" => reaction_order_values,
        "regime_ids" => regime_ids,
        "validity" => validity,
    )
end

function _ro_field_transform_point(point, normalized)
    return Float64[
        _ro_field_engine_log10_to_coordinate(
            point[position], normalized.log_basis,
            log10(Float64(normalized.domain["axes"][position]["reference"]["value"])))
        for position in 1:2
    ]
end

function _ro_field_polygon_halfspaces(vertices, normalized)
    halfspaces = Dict{String,Any}[]
    tolerance = normalized.geometry_tolerance
    axes = normalized.domain["axes"]
    for index in eachindex(vertices)
        following = index == length(vertices) ? 1 : index + 1
        a, b = vertices[index], vertices[following]
        dx, dy = b[1] - a[1], b[2] - a[2]
        coefficients = Float64[dy, -dx]
        upper_bound = coefficients[1] * a[1] + coefficients[2] * a[2]
        source = if abs(a[1] - axes[1]["bounds"]["lower"]) <= tolerance &&
                    abs(b[1] - axes[1]["bounds"]["lower"]) <= tolerance
            "domain_lower"
        elseif abs(a[1] - axes[1]["bounds"]["upper"]) <= tolerance &&
               abs(b[1] - axes[1]["bounds"]["upper"]) <= tolerance
            "domain_upper"
        elseif abs(a[2] - axes[2]["bounds"]["lower"]) <= tolerance &&
               abs(b[2] - axes[2]["bounds"]["lower"]) <= tolerance
            "domain_lower"
        elseif abs(a[2] - axes[2]["bounds"]["upper"]) <= tolerance &&
               abs(b[2] - axes[2]["bounds"]["upper"]) <= tolerance
            "domain_upper"
        else
            "regime"
        end
        push!(halfspaces, Dict{String,Any}(
            "coefficients" => coefficients,
            "upper_bound" => upper_bound,
            "source" => source,
        ))
    end
    return Dict{String,Any}("halfspaces" => halfspaces)
end

function _ro_field_segment_geometry(endpoints)
    a, b = endpoints
    dx, dy = b[1] - a[1], b[2] - a[2]
    magnitude = hypot(dx, dy)
    magnitude > 0 || error("RO-field facet has zero length")
    normal = Float64[dy / magnitude, -dx / magnitude]
    if normal[1] < 0 || (iszero(normal[1]) && normal[2] < 0)
        normal .*= -1
    end
    offset = -(normal[1] * a[1] + normal[2] * a[2])
    return normal, offset, normal[1] * normal[2] < 0
end

function _ro_field_segment_polyhedron(endpoints, kind::String)
    a, b = endpoints
    normal, offset, _ = _ro_field_segment_geometry(endpoints)
    dx, dy = b[1] - a[1], b[2] - a[2]
    magnitude = hypot(dx, dy)
    tangent = Float64[dx / magnitude, dy / magnitude]
    ta = tangent[1] * a[1] + tangent[2] * a[2]
    tb = tangent[1] * b[1] + tangent[2] * b[2]
    source = kind == "singular_boundary" ? "singular_boundary" : "regime"
    return Dict{String,Any}("halfspaces" => Dict{String,Any}[
        Dict("coefficients" => normal, "upper_bound" => -offset, "source" => source),
        Dict("coefficients" => -normal, "upper_bound" => offset, "source" => source),
        Dict("coefficients" => tangent, "upper_bound" => max(ta, tb), "source" => source),
        Dict("coefficients" => -tangent, "upper_bound" => -min(ta, tb), "source" => source),
    ])
end

function _ro_field_adjust_affine_offset(label::ROAffineLabel2D,
                                        normalized::NormalizedROFieldRequest)
    axis_reference_logs = Float64[
        log10(Float64(axis["reference"]["value"]))
        for axis in normalized.domain["axes"]
    ]
    base = label.output_offset + label.reaction_order_matrix * axis_reference_logs .-
        normalized.output_reference_log10
    normalized.log_basis === :natural_log && (base .*= log(10.0))
    return Float64.(base)
end

function _ro_field_serialize_exact(complex::ROCellComplex2D,
                                   normalized::NormalizedROFieldRequest)
    complex.authority_status === :engine_replayed || throw(ArgumentError(
        "exact RO-field serialization requires an engine-replayed cell complex"))
    cells = Dict{String,Any}[]
    for cell in complex.cells
        cell_id = _ro_field_cell_id(cell.id)
        vertices = [_ro_field_transform_point(point, normalized) for point in cell.vertices]
        labels = Dict{String,Any}[]
        for (position, label) in enumerate(cell.labels)
            push!(labels, Dict{String,Any}(
                "label_id" => "$cell_id-label-$position",
                "source_regime_ids" => _ro_field_regime_id.(label.source_regime_ids),
                "output_offset" => _ro_field_adjust_affine_offset(label, normalized),
                "reaction_order_matrix" => [
                    Float64.(collect(label.reaction_order_matrix[row, :]))
                    for row in axes(label.reaction_order_matrix, 1)
                ],
            ))
        end
        scale = normalized.log_basis === :natural_log ? log(10.0) : 1.0
        push!(cells, Dict{String,Any}(
            "cell_id" => cell_id,
            "dimension" => 2,
            "status" => cell.set_valued ? "set_valued" : "regular",
            "vertices" => vertices,
            "area" => Float64(cell.area) * scale^2,
            "polyhedron" => _ro_field_polygon_halfspaces(vertices, normalized),
            "source_regime_ids" => _ro_field_regime_id.(cell.source_regime_ids),
            "label_order" => String[label["label_id"] for label in labels],
            "affine_labels" => labels,
            "set_valued" => Bool(cell.set_valued),
        ))
    end

    strata = Dict{String,Any}[]
    for stratum in complex.singular_strata
        push!(strata, Dict{String,Any}(
            "stratum_id" => _ro_field_stratum_id(stratum.id),
            "dimension" => Int(stratum.dimension),
            "vertices" => [
                _ro_field_transform_point(point, normalized)
                for point in stratum.vertices
            ],
            "source_regime_ids" => _ro_field_regime_id.(stratum.source_regime_ids),
            "nullities" => Int.(stratum.nullities),
            "reasons" => String.(stratum.reasons),
        ))
    end

    facets = Dict{String,Any}[]
    for facet in complex.facets
        endpoints = [
            _ro_field_transform_point(point, normalized)
            for point in facet.endpoints
        ]
        sort!(endpoints; by=point -> (point[1], point[2]))
        singular_ids = _ro_field_stratum_id.(facet.singular_stratum_ids)
        kind = facet.kind === :domain ? "domain_boundary" :
            (isempty(singular_ids) ? "regime_transition" : "singular_boundary")
        normal, offset, mixed_sign = _ro_field_segment_geometry(endpoints)
        push!(facets, Dict{String,Any}(
            "facet_id" => _ro_field_facet_id(facet.id),
            "dimension" => 1,
            "kind" => kind,
            "endpoints" => endpoints,
            "polyhedron" => _ro_field_segment_polyhedron(endpoints, kind),
            "incident_cell_ids" => _ro_field_cell_id.(facet.incident_cell_ids),
            "singular_stratum_ids" => singular_ids,
            "normal" => normal,
            "offset" => offset,
            "mixed_sign" => mixed_sign,
            "domain_side" => facet.domain_side === nothing ? nothing :
                String(facet.domain_side),
        ))
    end

    return Dict{String,Any}(
        "coefficient_encoding" => "float64",
        "source_candidate_regime_count" => complex.candidate_regime_count,
        "regular_candidate_regime_count" => complex.regular_candidate_count,
        "cell_order" => String[cell["cell_id"] for cell in cells],
        "cells" => cells,
        "facet_order" => String[facet["facet_id"] for facet in facets],
        "facets" => facets,
        "singular_stratum_order" => String[
            stratum["stratum_id"] for stratum in strata
        ],
        "singular_strata" => strata,
        "gaps" => Dict{String,Any}[],
    )
end

function _ro_field_require_complete_exact_geometry!(complex)
    complex.coverage_complete && return complex
    detail = complex.gap_area === nothing ?
        "overlapping positive-area geometry remains" :
        "an unclassified region of area $(complex.gap_area) remains"
    throw(ROFieldRequestError(
        "ro_field_exact_geometry_incomplete",
        "The exact-cell computation completed, but $detail and the producer " *
        "cannot serialize honest v1 gap geometry for it. No artifact was " *
        "published or stored; this geometry outcome is not evidence of " *
        "scientific infeasibility.";
        computed=true,
    ))
end

const _RO_FIELD_DOCUMENT_KEYS = Set((
    "schema_version", "field_id", "representation", "partial", "domain",
    "outputs", "component_order", "data", "coverage", "evidence", "provenance",
))
const _RO_FIELD_SAMPLED_DATA_KEYS = Set((
    "sampling_scheme", "axis_coordinates", "grid_shape", "output_shape",
    "reaction_order_shape", "flatten_order", "output_values",
    "reaction_order_values", "regime_ids", "validity",
))
const _RO_FIELD_EXACT_DATA_KEYS = Set((
    "coefficient_encoding", "source_candidate_regime_count",
    "regular_candidate_regime_count", "cell_order", "cells", "facet_order",
    "facets", "singular_stratum_order", "singular_strata", "gaps",
))
const _RO_FIELD_COMPONENT_KEYS = Set(("output_id", "input_axis_id"))
const _RO_FIELD_CELL_KEYS = Set((
    "cell_id", "dimension", "status", "vertices", "area", "polyhedron",
    "source_regime_ids", "label_order", "affine_labels", "set_valued",
))
const _RO_FIELD_CELL_REQUIRED_KEYS = setdiff(_RO_FIELD_CELL_KEYS, Set(("polyhedron",)))
const _RO_FIELD_LABEL_KEYS = Set((
    "label_id", "source_regime_ids", "output_offset", "reaction_order_matrix",
))
const _RO_FIELD_FACET_KEYS = Set((
    "facet_id", "dimension", "kind", "endpoints", "polyhedron",
    "incident_cell_ids", "singular_stratum_ids", "normal", "offset",
    "mixed_sign", "domain_side",
))
const _RO_FIELD_FACET_REQUIRED_KEYS = setdiff(_RO_FIELD_FACET_KEYS, Set(("polyhedron",)))
const _RO_FIELD_STRATUM_KEYS = Set((
    "stratum_id", "dimension", "vertices", "source_regime_ids", "nullities",
    "reasons",
))
const _RO_FIELD_GAP_KEYS = Set(("gap_id", "reason", "region", "detail"))
const _RO_FIELD_GAP_REQUIRED_KEYS = setdiff(_RO_FIELD_GAP_KEYS, Set(("detail",)))
const _RO_FIELD_POLYHEDRON_KEYS = Set(("halfspaces",))
const _RO_FIELD_HALFSPACE_KEYS = Set(("coefficients", "upper_bound", "source"))
const _RO_FIELD_COVERAGE_KEYS = Set((
    "population_kind", "eligible_count", "evaluated_count", "valid_count",
    "invalid_count", "omitted_count", "enumeration_complete", "truncated",
    "truncation", "budget", "storage",
))
const _RO_FIELD_BUDGET_KEYS = Set((
    "work_unit_kind", "max_evaluated_items", "max_stored_items",
    "max_payload_bytes", "deadline_seconds",
))
const _RO_FIELD_RESPONSE_STORAGE_KEYS = Set((
    "mode", "complete", "stored_count", "payload_bytes", "content_sha256",
    "artifacts",
))
const _RO_FIELD_ARTIFACT_REFERENCE_KEYS = Set((
    "artifact_id", "media_type", "sha256", "byte_length", "item_count",
))
const _RO_FIELD_TRUNCATION_KEYS = Set(("reason", "detail"))
const _RO_FIELD_EVIDENCE_KEYS = Set((
    "evidence_class", "status", "claim_scope", "validity_policy",
    "completeness_claim", "limitations",
))
const _RO_FIELD_PROVENANCE_KEYS = Set((
    "producer", "source_revision_status", "source_commit", "source_dirty",
    "network_ir_sha256", "domain_sha256", "algorithm",
    "parent_artifact_sha256", "created_at", "reproduce_command",
))
const _RO_FIELD_PROVENANCE_REQUIRED_KEYS =
    setdiff(_RO_FIELD_PROVENANCE_KEYS, Set(("parent_artifact_sha256",)))
const _RO_FIELD_ALGORITHM_KEYS = Set(("name", "version", "configuration_sha256"))

function _ro_field_document_string(raw, path::AbstractString; nonempty::Bool=true)
    raw isa AbstractString || throw(ArgumentError("$path must be a string"))
    value = String(raw)
    nonempty && isempty(value) && throw(ArgumentError("$path must not be empty"))
    return value
end

function _ro_field_document_int(raw, path::AbstractString; minimum::Int=0)
    (raw isa Integer && !(raw isa Bool)) || throw(ArgumentError(
        "$path must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$path is outside the supported integer range"))
    end
    value >= minimum || throw(ArgumentError("$path must be at least $minimum"))
    return value
end

function _ro_field_document_number(raw, path::AbstractString)
    (raw isa Real && !(raw isa Bool)) || throw(ArgumentError(
        "$path must be a finite number"))
    value = Float64(raw)
    isfinite(value) || throw(ArgumentError("$path must be a finite number"))
    return value
end

function _ro_field_document_bool(raw, path::AbstractString)
    raw isa Bool || throw(ArgumentError("$path must be Boolean"))
    return raw
end

function _ro_field_document_sha(raw, path::AbstractString)
    value = _ro_field_document_string(raw, path)
    occursin(_RO_FIELD_SHA256_PATTERN, value) || throw(ArgumentError(
        "$path must be a lowercase SHA-256 value"))
    return value
end

function _ro_field_document_identifier(raw, path::AbstractString; safe::Bool=false)
    value = _ro_field_document_string(raw, path)
    pattern = safe ? _RO_FIELD_SAFE_ID_PATTERN : _RO_FIELD_IDENTIFIER_PATTERN
    occursin(pattern, value) || throw(ArgumentError("$path is not a valid identifier"))
    return value
end

function _ro_field_document_string_array(raw, path::AbstractString;
                                         minimum::Int=0,
                                         identifier::Bool=false)
    items = collect(_ro_field_array(raw, path))
    length(items) >= minimum || throw(ArgumentError(
        "$path must contain at least $minimum item(s)"))
    values = String[
        identifier ?
            _ro_field_document_identifier(item, "$path[$index]") :
            _ro_field_document_string(item, "$path[$index]")
        for (index, item) in enumerate(items)
    ]
    length(unique(values)) == length(values) || throw(ArgumentError(
        "$path values must be unique"))
    return values
end

function _ro_field_document_point(raw, path::AbstractString)
    values = collect(_ro_field_array(raw, path))
    length(values) == 2 || throw(ArgumentError("$path must be a rank-2 point"))
    return Float64[
        _ro_field_document_number(value, "$path[$index]")
        for (index, value) in enumerate(values)
    ]
end

_ro_field_document_close(left::Real, right::Real; tolerance::Float64=1e-8) =
    abs(Float64(left) - Float64(right)) <=
        tolerance * max(1.0, abs(Float64(left)), abs(Float64(right)))

function _ro_field_document_coefficient(raw, encoding::String, path::AbstractString)
    if encoding == "float64"
        return _ro_field_document_number(raw, path)
    end
    raw isa AbstractString || throw(ArgumentError(
        "$path must use integer/rational string encoding"))
    value = String(raw)
    occursin(r"^-?[0-9]+(?:/[1-9][0-9]*)?$", value) || throw(ArgumentError(
        "$path is not an integer/rational coefficient string"))
    parts = split(value, '/')
    result = Float64(parse(BigInt, parts[1])) /
        (length(parts) == 1 ? 1.0 : Float64(parse(BigInt, parts[2])))
    isfinite(result) || throw(ArgumentError(
        "$path is outside the supported finite coefficient range"))
    return result
end

function _ro_field_validate_reference!(raw, path::AbstractString)
    object = _ro_field_exact_keys(raw, _RO_FIELD_REFERENCE_KEYS, path)
    value = _ro_field_document_number(get(object, "value", nothing), "$path.value")
    value > 0 || throw(ArgumentError("$path.value must be positive"))
    _ro_field_document_string(get(object, "unit", nothing), "$path.unit")
    return nothing
end

function _ro_field_validate_polyhedron!(raw, encoding::String, rank::Int,
                                        path::AbstractString;
                                        require_nonempty::Bool=false)
    object = _ro_field_exact_keys(raw, _RO_FIELD_POLYHEDRON_KEYS, path)
    halfspaces = collect(_ro_field_array(get(object, "halfspaces", nothing),
        "$path.halfspaces"))
    require_nonempty && isempty(halfspaces) && throw(ArgumentError(
        "$path.halfspaces must contain real gap/geometry constraints"))
    for (index, raw_halfspace) in enumerate(halfspaces)
        current = "$path.halfspaces[$index]"
        halfspace = _ro_field_exact_keys(
            raw_halfspace, _RO_FIELD_HALFSPACE_KEYS, current)
        coefficients = collect(_ro_field_array(
            get(halfspace, "coefficients", nothing), "$current.coefficients"))
        length(coefficients) == rank || throw(ArgumentError(
            "$current.coefficients must have rank $rank"))
        for (coefficient_index, coefficient) in enumerate(coefficients)
            _ro_field_document_coefficient(
                coefficient, encoding, "$current.coefficients[$coefficient_index]")
        end
        _ro_field_document_coefficient(
            get(halfspace, "upper_bound", nothing), encoding,
            "$current.upper_bound")
        source = _ro_field_document_string(
            get(halfspace, "source", nothing), "$current.source")
        source in ("regime", "domain_lower", "domain_upper", "singular_boundary") ||
            throw(ArgumentError("$current.source is unsupported"))
    end
    return nothing
end

function _ro_field_polygon_signed_area(vertices)
    origin_x, origin_y = vertices[1]
    twice_area = 0.0
    for index in eachindex(vertices)
        following = index == length(vertices) ? 1 : index + 1
        twice_area +=
            (vertices[index][1] - origin_x) *
                (vertices[following][2] - origin_y) -
            (vertices[following][1] - origin_x) *
                (vertices[index][2] - origin_y)
    end
    return twice_area / 2
end

function _ro_field_polygon_is_convex(vertices;
                                     length_tolerance::Float64=1e-9)
    for index in eachindex(vertices)
        second = index == length(vertices) ? 1 : index + 1
        third = second == length(vertices) ? 1 : second + 1
        a, b, c = vertices[index], vertices[second], vertices[third]
        cross = (b[1] - a[1]) * (c[2] - b[2]) -
            (b[2] - a[2]) * (c[1] - b[1])
        first_length = hypot(b[1] - a[1], b[2] - a[2])
        second_length = hypot(c[1] - b[1], c[2] - b[2])
        cross_tolerance = max(first_length, second_length) * length_tolerance +
            64.0 * eps(Float64) * first_length * second_length
        cross >= -cross_tolerance || return false
    end
    return true
end

_ro_field_edge_cross(a, b, point) =
    (b[1] - a[1]) * (point[2] - a[2]) -
        (b[2] - a[2]) * (point[1] - a[1])

function _ro_field_edge_cross_tolerance(a, b, point,
                                        length_tolerance::Float64)
    edge_length = hypot(b[1] - a[1], b[2] - a[2])
    point_distance = hypot(point[1] - a[1], point[2] - a[2])
    # Preserve genuine sub-tolerance overlap/gap area for the global budget.
    # The domain-derived distance is only an upper bound on arithmetic
    # roundoff; it must not expand every polygon edge independently.
    return min(
        edge_length * length_tolerance,
        64.0 * eps(Float64) * edge_length * point_distance,
    )
end

function _ro_field_clip_convex_polygon(subject, clip;
                                       length_tolerance::Float64=1e-9)
    output = Vector{Float64}[copy(point) for point in subject]
    for clip_index in eachindex(clip)
        isempty(output) && break
        following = clip_index == length(clip) ? 1 : clip_index + 1
        a, b = clip[clip_index], clip[following]
        input = output
        output = Vector{Float64}[]
        previous = input[end]
        previous_distance = _ro_field_edge_cross(a, b, previous)
        previous_inside = previous_distance >=
            -_ro_field_edge_cross_tolerance(a, b, previous, length_tolerance)
        for current in input
            current_distance = _ro_field_edge_cross(a, b, current)
            current_inside = current_distance >=
                -_ro_field_edge_cross_tolerance(a, b, current, length_tolerance)
            if current_inside != previous_inside
                denominator = previous_distance - current_distance
                denominator_tolerance = 64.0 * eps(Float64) * max(
                    abs(previous_distance), abs(current_distance), floatmin(Float64))
                if abs(denominator) > denominator_tolerance
                    fraction = previous_distance / denominator
                    push!(output, Float64[
                        previous[coordinate] +
                            fraction * (current[coordinate] - previous[coordinate])
                        for coordinate in 1:2
                    ])
                end
            end
            current_inside && push!(output, copy(current))
            previous = current
            previous_distance = current_distance
            previous_inside = current_inside
        end
    end
    return output
end

function _ro_field_convex_intersection_area(left, right;
                                            length_tolerance::Float64=1e-9)
    intersection = _ro_field_clip_convex_polygon(
        left, right; length_tolerance=length_tolerance)
    length(intersection) >= 3 || return 0.0
    return abs(_ro_field_polygon_signed_area(intersection))
end

function _ro_field_exact_domain_tolerances(domain_bounds)
    spans = Float64[upper - lower for (lower, upper) in domain_bounds]
    minimum_span = minimum(spans)
    coordinate_scale = maximum((abs(value)
        for bounds in domain_bounds for value in bounds); init=minimum_span)
    # This is a validator-owned tolerance derived from Float64 resolution and
    # the declared domain. It is deliberately independent of the request's
    # geometry_tolerance, which must never relax a completeness certificate.
    length_tolerance = max(
        1e-10 * minimum_span,
        64.0 * eps(Float64) * max(coordinate_scale, minimum_span),
    )
    length_tolerance <= 1e-5 * minimum_span || throw(ArgumentError(
        "the exact domain is not resolvable tightly enough for Float64 " *
        "geometry certification"))
    domain_area = prod(spans)
    area_tolerance = 8.0 * length_tolerance * sum(spans) +
        128.0 * eps(Float64) * domain_area
    return spans, domain_area, length_tolerance, area_tolerance
end

function _ro_field_segment_interval_on_edge(segment, edge, tolerance::Float64)
    a, b = edge
    dx, dy = b[1] - a[1], b[2] - a[2]
    length_squared = dx^2 + dy^2
    edge_length = sqrt(length_squared)
    edge_length > tolerance || return nothing
    parameter_tolerance = max(128.0 * eps(Float64), tolerance / edge_length)
    parameters = Float64[]
    for point in segment
        cross = dx * (point[2] - a[2]) - dy * (point[1] - a[1])
        abs(cross) / edge_length <= tolerance || return nothing
        parameter = ((point[1] - a[1]) * dx + (point[2] - a[2]) * dy) /
            length_squared
        -parameter_tolerance <= parameter <= 1.0 + parameter_tolerance ||
            return nothing
        push!(parameters, clamp(parameter, 0.0, 1.0))
    end
    lower, upper = minmax(parameters...)
    (upper - lower) * edge_length > tolerance || return nothing
    return (lower, upper, parameter_tolerance)
end

function _ro_field_facet_domain_side(endpoints, domain_bounds,
                                     tolerance::Float64)
    for (axis, (lower, upper)) in enumerate(domain_bounds)
        if all(point -> abs(point[axis] - lower) <= tolerance, endpoints)
            return "axis$(axis)_lower"
        elseif all(point -> abs(point[axis] - upper) <= tolerance, endpoints)
            return "axis$(axis)_upper"
        end
    end
    return nothing
end

function _ro_field_segments_have_positive_overlap(left, right,
                                                   tolerance::Float64)
    a, b = left
    dx, dy = b[1] - a[1], b[2] - a[2]
    length_squared = dx^2 + dy^2
    segment_length = sqrt(length_squared)
    segment_length > tolerance || return false
    for point in right
        cross = dx * (point[2] - a[2]) - dy * (point[1] - a[1])
        abs(cross) / segment_length <= tolerance || return false
    end
    parameters = [
        ((point[1] - a[1]) * dx + (point[2] - a[2]) * dy) / length_squared
        for point in right
    ]
    lower = max(0.0, minimum(parameters))
    upper = min(1.0, maximum(parameters))
    return (upper - lower) * segment_length > tolerance
end

function _ro_field_regular_affine_continuity!(left, right, endpoints,
                                              facet_path::AbstractString)
    left_offsets, left_matrix = left
    right_offsets, right_matrix = right
    length(left_offsets) == length(right_offsets) || error(
        "internal RO-field affine-output shape mismatch")
    for (endpoint_index, endpoint) in enumerate(endpoints)
        for output_index in eachindex(left_offsets)
            left_value = left_offsets[output_index] +
                sum(left_matrix[output_index, axis] * endpoint[axis]
                    for axis in eachindex(endpoint))
            right_value = right_offsets[output_index] +
                sum(right_matrix[output_index, axis] * endpoint[axis]
                    for axis in eachindex(endpoint))
            scale = max(1.0, abs(left_value), abs(right_value),
                abs(left_offsets[output_index]), abs(right_offsets[output_index]))
            abs(left_value - right_value) <=
                (1e-10 + 128.0 * eps(Float64)) * scale ||
                throw(ArgumentError(
                    "$facet_path has discontinuous regular affine output " *
                    "$output_index at endpoint $endpoint_index"))
        end
    end
    return nothing
end

function _ro_field_validate_facet_closure!(cell_vertices, cell_regular_affine,
                                           facets, domain_bounds,
                                           tolerance::Float64)
    coverage = Dict{Tuple{String,Int},
        Vector{Tuple{Float64,Float64,Float64,Int}}}()
    for (cell_id, vertices) in cell_vertices
        for edge_index in eachindex(vertices)
            coverage[(cell_id, edge_index)] =
                Tuple{Float64,Float64,Float64,Int}[]
        end
    end

    for (facet_index, facet) in enumerate(facets)
        actual_incident = String[]
        memberships = Tuple{String,Int,NTuple{3,Float64}}[]
        for (cell_id, vertices) in cell_vertices
            matching_edges = Tuple{Int,NTuple{3,Float64}}[]
            for edge_index in eachindex(vertices)
                following = edge_index == length(vertices) ? 1 : edge_index + 1
                interval = _ro_field_segment_interval_on_edge(
                    facet.endpoints,
                    (vertices[edge_index], vertices[following]),
                    tolerance,
                )
                interval === nothing || push!(matching_edges, (edge_index, interval))
            end
            length(matching_edges) <= 1 || throw(ArgumentError(
                "$(facet.path) lies on multiple canonical edges of cell $cell_id"))
            isempty(matching_edges) && continue
            edge_index, interval = only(matching_edges)
            push!(actual_incident, cell_id)
            push!(memberships, (cell_id, edge_index, interval))
        end
        sort!(actual_incident)
        sort(facet.incident) == actual_incident || throw(ArgumentError(
            "$(facet.path).incident_cell_ids does not equal the cells whose " *
            "polygon boundaries contain the facet"))

        actual_domain_side = _ro_field_facet_domain_side(
            facet.endpoints, domain_bounds, tolerance)
        if actual_domain_side === nothing
            length(actual_incident) == 2 || throw(ArgumentError(
                "$(facet.path) is an internal facet but does not have exactly " *
                "two geometric incident cells"))
            facet.domain_side === nothing || throw(ArgumentError(
                "$(facet.path).domain_side must be null for an internal facet"))
            expected_kind = isempty(facet.singular) ?
                "regime_transition" : "singular_boundary"
            facet.kind == expected_kind || throw(ArgumentError(
                "$(facet.path).kind is inconsistent with its internal " *
                "singular-stratum incidence"))
            left = get(cell_regular_affine, actual_incident[1], nothing)
            right = get(cell_regular_affine, actual_incident[2], nothing)
            if left !== nothing && right !== nothing
                _ro_field_regular_affine_continuity!(
                    left, right, facet.endpoints, facet.path)
            end
        else
            length(actual_incident) == 1 || throw(ArgumentError(
                "$(facet.path) is on the rectangular domain boundary but does " *
                "not have exactly one geometric incident cell"))
            facet.kind == "domain_boundary" || throw(ArgumentError(
                "$(facet.path).kind must be domain_boundary"))
            facet.domain_side == actual_domain_side || throw(ArgumentError(
                "$(facet.path).domain_side does not match its geometry"))
        end

        for (cell_id, edge_index, interval) in memberships
            lower, upper, parameter_tolerance = interval
            push!(coverage[(cell_id, edge_index)],
                (lower, upper, parameter_tolerance, facet_index))
        end
    end

    total_uncovered_length = 0.0
    total_overlapping_length = 0.0
    for ((cell_id, edge_index), raw_intervals) in coverage
        isempty(raw_intervals) && throw(ArgumentError(
            "cell $cell_id edge $edge_index is not represented by any facet"))
        intervals = sort!(raw_intervals; by=item -> (item[1], item[2], item[4]))
        vertices = cell_vertices[cell_id]
        following = edge_index == length(vertices) ? 1 : edge_index + 1
        edge_length = hypot(
            vertices[following][1] - vertices[edge_index][1],
            vertices[following][2] - vertices[edge_index][2],
        )
        cursor = 0.0
        for (lower, upper, _, _) in intervals
            if lower > cursor
                total_uncovered_length += (lower - cursor) * edge_length
            elseif lower < cursor
                total_overlapping_length +=
                    (min(cursor, upper) - lower) * edge_length
            end
            cursor = max(cursor, upper)
        end
        cursor < 1.0 &&
            (total_uncovered_length += (1.0 - cursor) * edge_length)
    end
    total_uncovered_length <= tolerance || throw(ArgumentError(
        "cumulative uncovered facet length exceeds the global tolerance"))
    total_overlapping_length <= tolerance || throw(ArgumentError(
        "cumulative overlapping facet length exceeds the global tolerance"))
    return nothing
end

"""
    validate_ro_field_document!(raw)

Validate a materialized sampled/exact v1 field as a production trust boundary.
The validator admits only the currently implemented Cartesian row-major sampled
layout and exact rank-2 complex, checks every consumed object key, tensor/value
block, exact geometry and incidence relation, coverage/evidence/storage claim,
and recomputes canonical data/domain hashes. It returns a materialized copy.
"""
function validate_ro_field_document!(raw)
    document = _materialize(raw)
    document = _ro_field_exact_keys(document, _RO_FIELD_DOCUMENT_KEYS, "RO-field")
    get(document, "schema_version", nothing) == RO_FIELD_SCHEMA_VERSION ||
        throw(ArgumentError("unsupported RO-field schema version"))
    _ro_field_document_identifier(get(document, "field_id", nothing),
        "field_id"; safe=true)
    representation = _ro_field_document_string(
        get(document, "representation", nothing), "representation")
    representation in ("sampled_grid", "exact_cell_complex") ||
        throw(ArgumentError(
            "runtime validation supports sampled_grid and exact_cell_complex"))
    partial = _ro_field_document_bool(get(document, "partial", nothing), "partial")

    domain = _ro_field_exact_keys(
        get(document, "domain", nothing), _RO_FIELD_DOMAIN_KEYS, "domain")
    get(domain, "domain_kind", nothing) == "axis_aligned_log_box" ||
        throw(ArgumentError("domain.domain_kind is unsupported"))
    get(domain, "coordinate_space", nothing) == "dimensionless_log_ratio" ||
        throw(ArgumentError("domain.coordinate_space is unsupported"))
    get(domain, "log_basis", nothing) in ("log10", "natural_log") ||
        throw(ArgumentError("domain.log_basis is unsupported"))
    axes = collect(_ro_field_array(get(domain, "axes", nothing), "domain.axes"))
    input_count = length(axes)
    1 <= input_count <= MAX_SYNC_RO_FIELD_AXES || throw(ArgumentError(
        "domain.axes must contain between 1 and $MAX_SYNC_RO_FIELD_AXES axes"))
    representation == "exact_cell_complex" && input_count != 2 &&
        throw(ArgumentError("exact_cell_complex v1 requires exactly two axes"))
    axis_ids = String[]
    axis_symbols = String[]
    for (index, raw_axis) in enumerate(axes)
        path = "domain.axes[$index]"
        axis = _ro_field_exact_keys(raw_axis, _RO_FIELD_AXIS_KEYS, path)
        push!(axis_ids, _ro_field_document_identifier(
            get(axis, "axis_id", nothing), "$path.axis_id"))
        push!(axis_symbols, _ro_field_document_string(
            get(axis, "symbol", nothing), "$path.symbol"))
        kind = _ro_field_document_string(
            get(axis, "coordinate_kind", nothing), "$path.coordinate_kind")
        kind in ("conserved_total", "binding_constant", "external_control") ||
            throw(ArgumentError("$path.coordinate_kind is unsupported"))
        representation == "exact_cell_complex" && kind != "conserved_total" &&
            throw(ArgumentError("exact_cell_complex axes must be conserved_total"))
        get(axis, "orientation", nothing) == "increasing_physical_value" ||
            throw(ArgumentError("$path.orientation is unsupported"))
        _ro_field_validate_reference!(get(axis, "reference", nothing),
            "$path.reference")
        bounds = _ro_field_exact_keys(
            get(axis, "bounds", nothing), _RO_FIELD_BOUNDS_KEYS, "$path.bounds")
        lower = _ro_field_document_number(
            get(bounds, "lower", nothing), "$path.bounds.lower")
        upper = _ro_field_document_number(
            get(bounds, "upper", nothing), "$path.bounds.upper")
        lower < upper || throw(ArgumentError("$path.bounds must satisfy lower < upper"))
    end
    length(unique(axis_ids)) == input_count || throw(ArgumentError(
        "domain axis ids must be unique"))
    length(unique(axis_symbols)) == input_count || throw(ArgumentError(
        "domain axis symbols must be unique"))
    axis_order = _ro_field_document_string_array(
        get(domain, "axis_order", nothing), "domain.axis_order"; identifier=true)
    axis_order == axis_ids || throw(ArgumentError(
        "domain.axis_order does not equal the axis_id sequence"))

    backgrounds = collect(_ro_field_array(
        get(domain, "fixed_background", nothing), "domain.fixed_background"))
    background_ids = String[]
    background_symbols = String[]
    for (index, raw_background) in enumerate(backgrounds)
        path = "domain.fixed_background[$index]"
        background = _ro_field_exact_keys(
            raw_background, _RO_FIELD_BACKGROUND_KEYS, path)
        push!(background_ids, _ro_field_document_identifier(
            get(background, "parameter_id", nothing), "$path.parameter_id"))
        push!(background_symbols, _ro_field_document_string(
            get(background, "symbol", nothing), "$path.symbol"))
        kind = _ro_field_document_string(
            get(background, "coordinate_kind", nothing), "$path.coordinate_kind")
        kind in ("conserved_total", "binding_constant", "external_control") ||
            throw(ArgumentError("$path.coordinate_kind is unsupported"))
        _ro_field_validate_reference!(get(background, "reference", nothing),
            "$path.reference")
        _ro_field_document_number(
            get(background, "log_value", nothing), "$path.log_value")
    end
    length(unique(background_ids)) == length(background_ids) || throw(ArgumentError(
        "fixed_background parameter ids must be unique"))
    length(unique(background_symbols)) == length(background_symbols) ||
        throw(ArgumentError("fixed_background symbols must be unique"))
    isempty(intersect(Set(axis_symbols), Set(background_symbols))) ||
        throw(ArgumentError("a swept symbol also appears in fixed_background"))

    outputs = _ro_field_exact_keys(
        get(document, "outputs", nothing), _RO_FIELD_OUTPUTS_KEYS, "outputs")
    output_items = collect(_ro_field_array(get(outputs, "items", nothing),
        "outputs.items"))
    output_count = length(output_items)
    1 <= output_count <= MAX_SYNC_RO_FIELD_OUTPUTS || throw(ArgumentError(
        "outputs.items must contain between 1 and $MAX_SYNC_RO_FIELD_OUTPUTS outputs"))
    output_ids = String[]
    output_symbols = String[]
    for (index, raw_output) in enumerate(output_items)
        path = "outputs.items[$index]"
        output = _ro_field_exact_keys(raw_output, _RO_FIELD_OUTPUT_KEYS, path)
        push!(output_ids, _ro_field_document_identifier(
            get(output, "output_id", nothing), "$path.output_id"))
        push!(output_symbols, _ro_field_document_string(
            get(output, "symbol", nothing), "$path.symbol"))
        kind = _ro_field_document_string(
            get(output, "observable_kind", nothing), "$path.observable_kind")
        kind in ("species_concentration", "positive_linear_observable") ||
            throw(ArgumentError("$path.observable_kind is unsupported"))
        _ro_field_validate_reference!(get(output, "reference", nothing),
            "$path.reference")
    end
    length(unique(output_ids)) == output_count || throw(ArgumentError(
        "output ids must be unique"))
    length(unique(output_symbols)) == output_count || throw(ArgumentError(
        "output symbols must be unique"))
    output_order = _ro_field_document_string_array(
        get(outputs, "output_order", nothing), "outputs.output_order"; identifier=true)
    output_order == output_ids || throw(ArgumentError(
        "outputs.output_order does not equal the output_id sequence"))

    components = collect(_ro_field_array(
        get(document, "component_order", nothing), "component_order"))
    expected_components = [(output_id, axis_id)
        for output_id in output_order for axis_id in axis_order]
    length(components) == length(expected_components) || throw(ArgumentError(
        "component_order is not the output-major Cartesian product"))
    for (index, raw_component) in enumerate(components)
        path = "component_order[$index]"
        component = _ro_field_exact_keys(
            raw_component, _RO_FIELD_COMPONENT_KEYS, path)
        actual = (
            _ro_field_document_identifier(
                get(component, "output_id", nothing), "$path.output_id"),
            _ro_field_document_identifier(
                get(component, "input_axis_id", nothing), "$path.input_axis_id"),
        )
        actual == expected_components[index] || throw(ArgumentError(
            "component_order is not the output-major Cartesian product"))
    end

    data = _ro_field_object(get(document, "data", nothing), "data")
    actual_evaluated = 0
    actual_valid = 0
    actual_invalid = 0
    source_candidate_count = nothing
    if representation == "sampled_grid"
        data = _ro_field_exact_keys(data, _RO_FIELD_SAMPLED_DATA_KEYS, "data")
        get(data, "sampling_scheme", nothing) == "cartesian_product" ||
            throw(ArgumentError(
                "data.sampling_scheme must be cartesian_product in the v1 runtime"))
        get(data, "flatten_order", nothing) ==
            "row_major_last_axis_fastest" || throw(ArgumentError(
                "data.flatten_order must be row_major_last_axis_fastest in the v1 runtime"))
        grid_shape_raw = collect(_ro_field_array(
            get(data, "grid_shape", nothing), "data.grid_shape"))
        grid_shape = Int[
            _ro_field_document_int(value, "data.grid_shape[$index]"; minimum=1)
            for (index, value) in enumerate(grid_shape_raw)
        ]
        length(grid_shape) == input_count || throw(ArgumentError(
            "sampled grid_shape has the wrong rank"))
        point_count_big = prod(BigInt(value) for value in grid_shape)
        point_count_big <= typemax(Int) || throw(ArgumentError(
            "sampled grid is outside the supported integer range"))
        point_count = Int(point_count_big)
        point_count <= MAX_SYNC_RO_FIELD_POINTS || throw(ArgumentError(
            "sampled grid exceeds the inline v1 point limit"))
        coordinates = collect(_ro_field_array(
            get(data, "axis_coordinates", nothing), "data.axis_coordinates"))
        length(coordinates) == input_count || throw(ArgumentError(
            "sampled axis_coordinates has the wrong rank"))
        for (axis_index, raw_values) in enumerate(coordinates)
            path = "data.axis_coordinates[$axis_index]"
            values_raw = collect(_ro_field_array(raw_values, path))
            length(values_raw) == grid_shape[axis_index] || throw(ArgumentError(
                "$path length does not match grid_shape"))
            values = Float64[
                _ro_field_document_number(value, "$path[$index]")
                for (index, value) in enumerate(values_raw)
            ]
            all(values[index] < values[index + 1]
                for index in 1:(length(values) - 1)) || throw(ArgumentError(
                    "$path must be strictly increasing"))
            bounds = axes[axis_index]["bounds"]
            all(value -> bounds["lower"] <= value <= bounds["upper"], values) ||
                throw(ArgumentError("$path must stay inside the declared domain"))
        end
        output_shape = Int[
            _ro_field_document_int(value, "data.output_shape[$index]"; minimum=1)
            for (index, value) in enumerate(collect(_ro_field_array(
                get(data, "output_shape", nothing), "data.output_shape")))
        ]
        output_shape == [grid_shape; output_count] || throw(ArgumentError(
            "sampled output_shape is inconsistent"))
        reaction_shape = Int[
            _ro_field_document_int(value, "data.reaction_order_shape[$index]"; minimum=1)
            for (index, value) in enumerate(collect(_ro_field_array(
                get(data, "reaction_order_shape", nothing),
                "data.reaction_order_shape")))
        ]
        reaction_shape == [grid_shape; output_count; input_count] ||
            throw(ArgumentError("sampled reaction_order_shape is inconsistent"))
        validity = collect(_ro_field_array(get(data, "validity", nothing),
            "data.validity"))
        regimes = collect(_ro_field_array(get(data, "regime_ids", nothing),
            "data.regime_ids"))
        output_values = collect(_ro_field_array(
            get(data, "output_values", nothing), "data.output_values"))
        reaction_values = collect(_ro_field_array(
            get(data, "reaction_order_values", nothing),
            "data.reaction_order_values"))
        length(validity) == point_count && length(regimes) == point_count ||
            throw(ArgumentError("sample validity/regime lengths are inconsistent"))
        length(output_values) == point_count * output_count || throw(ArgumentError(
            "sample output_values length is inconsistent"))
        length(reaction_values) == point_count * output_count * input_count ||
            throw(ArgumentError("sample reaction_order_values length is inconsistent"))
        for point in 1:point_count
            valid = _ro_field_document_bool(validity[point], "data.validity[$point]")
            output_start = (point - 1) * output_count + 1
            reaction_start = (point - 1) * output_count * input_count + 1
            output_block = output_values[
                output_start:(output_start + output_count - 1)]
            reaction_block = reaction_values[
                reaction_start:(reaction_start + output_count * input_count - 1)]
            if valid
                _ro_field_document_identifier(regimes[point], "data.regime_ids[$point]")
                for (index, value) in enumerate(output_block)
                    _ro_field_document_number(value,
                        "data.output_values[$(output_start + index - 1)]")
                end
                for (index, value) in enumerate(reaction_block)
                    _ro_field_document_number(value,
                        "data.reaction_order_values[$(reaction_start + index - 1)]")
                end
            else
                regimes[point] === nothing || throw(ArgumentError(
                    "invalid sample must use a null regime id"))
                all(isnothing, output_block) || throw(ArgumentError(
                    "invalid sample contains a non-null output value"))
                all(isnothing, reaction_block) || throw(ArgumentError(
                    "invalid sample contains a non-null reaction-order value"))
            end
        end
        actual_evaluated = point_count
        actual_valid = count(value -> value === true, validity)
        actual_invalid = point_count - actual_valid
    else
        data = _ro_field_exact_keys(data, _RO_FIELD_EXACT_DATA_KEYS, "data")
        encoding = _ro_field_document_string(
            get(data, "coefficient_encoding", nothing), "data.coefficient_encoding")
        encoding in ("float64", "integer_or_rational_string") || throw(ArgumentError(
            "data.coefficient_encoding is unsupported"))
        source_candidate_count = _ro_field_document_int(
            get(data, "source_candidate_regime_count", nothing),
            "data.source_candidate_regime_count")
        regular_candidate_count = _ro_field_document_int(
            get(data, "regular_candidate_regime_count", nothing),
            "data.regular_candidate_regime_count")
        regular_candidate_count <= source_candidate_count || throw(ArgumentError(
            "regular_candidate_regime_count exceeds source_candidate_regime_count"))

        domain_bounds = [
            (Float64(axis["bounds"]["lower"]), Float64(axis["bounds"]["upper"]))
            for axis in axes
        ]
        _, domain_area, exact_length_tolerance, exact_area_tolerance =
            _ro_field_exact_domain_tolerances(domain_bounds)

        cells = collect(_ro_field_array(get(data, "cells", nothing), "data.cells"))
        cell_ids = String[]
        cell_vertices = Dict{String,Vector{Vector{Float64}}}()
        cell_regular_affine = Dict{String,Any}()
        cell_sources = Set{String}()
        cell_area_sum = 0.0
        regular_cells = 0
        set_valued_cells = 0
        for (index, raw_cell) in enumerate(cells)
            path = "data.cells[$index]"
            cell = _ro_field_exact_keys(raw_cell, _RO_FIELD_CELL_KEYS, path;
                required=_RO_FIELD_CELL_REQUIRED_KEYS)
            cell_id = _ro_field_document_identifier(
                get(cell, "cell_id", nothing), "$path.cell_id")
            push!(cell_ids, cell_id)
            get(cell, "dimension", nothing) == 2 || throw(ArgumentError(
                "$path.dimension must be 2"))
            raw_vertices = collect(_ro_field_array(
                get(cell, "vertices", nothing), "$path.vertices"))
            length(raw_vertices) >= 3 || throw(ArgumentError(
                "$path.vertices must contain at least three points"))
            vertices = Vector{Float64}[
                _ro_field_document_point(vertex, "$path.vertices[$vertex_index]")
                for (vertex_index, vertex) in enumerate(raw_vertices)
            ]
            length(unique(vertices)) == length(vertices) || throw(ArgumentError(
                "$path.vertices must be unique"))
            first(sort(copy(vertices); by=point -> (point[1], point[2]))) == vertices[1] ||
                throw(ArgumentError(
                    "$path.vertices must start at the lexicographically smallest point"))
            signed_area = _ro_field_polygon_signed_area(vertices)
            signed_area > 0 || throw(ArgumentError(
                "$path.vertices must be counter-clockwise with positive area"))
            _ro_field_polygon_is_convex(
                vertices; length_tolerance=exact_length_tolerance) ||
                throw(ArgumentError(
                "$path.vertices must describe a convex polygon"))
            for (vertex_index, vertex) in enumerate(vertices), coordinate in 1:2
                bounds = axes[coordinate]["bounds"]
                lower = Float64(bounds["lower"])
                upper = Float64(bounds["upper"])
                lower - exact_length_tolerance <= vertex[coordinate] <=
                    upper + exact_length_tolerance ||
                    throw(ArgumentError(
                        "$path.vertices[$vertex_index] lies outside the declared domain"))
            end
            declared_area = _ro_field_document_number(
                get(cell, "area", nothing), "$path.area")
            declared_area > 0 &&
                abs(declared_area - signed_area) <= exact_area_tolerance ||
                throw(ArgumentError("$path.area does not match the polygon area"))
            haskey(cell, "polyhedron") && _ro_field_validate_polyhedron!(
                cell["polyhedron"], encoding, 2, "$path.polyhedron";
                require_nonempty=true)

            sources = _ro_field_document_string_array(
                get(cell, "source_regime_ids", nothing), "$path.source_regime_ids";
                minimum=1, identifier=true)
            for source in sources
                source in cell_sources && throw(ArgumentError(
                    "source regime $source owns more than one full-dimensional cell"))
                push!(cell_sources, source)
            end
            labels = collect(_ro_field_array(
                get(cell, "affine_labels", nothing), "$path.affine_labels"))
            label_ids = String[]
            label_source_union = Set{String}()
            label_signatures = String[]
            numeric_labels = Any[]
            for (label_index, raw_label) in enumerate(labels)
                label_path = "$path.affine_labels[$label_index]"
                label = _ro_field_exact_keys(
                    raw_label, _RO_FIELD_LABEL_KEYS, label_path)
                push!(label_ids, _ro_field_document_identifier(
                    get(label, "label_id", nothing), "$label_path.label_id"))
                label_sources = _ro_field_document_string_array(
                    get(label, "source_regime_ids", nothing),
                    "$label_path.source_regime_ids"; minimum=1, identifier=true)
                for source in label_sources
                    source in label_source_union && throw(ArgumentError(
                        "$path affine-label source partitions overlap"))
                    push!(label_source_union, source)
                end
                offsets = collect(_ro_field_array(
                    get(label, "output_offset", nothing), "$label_path.output_offset"))
                length(offsets) == output_count || throw(ArgumentError(
                    "$label_path.output_offset has the wrong shape"))
                numeric_offsets = Float64[
                    _ro_field_document_coefficient(value, encoding,
                        "$label_path.output_offset[$offset_index]")
                    for (offset_index, value) in enumerate(offsets)
                ]
                matrix = collect(_ro_field_array(
                    get(label, "reaction_order_matrix", nothing),
                    "$label_path.reaction_order_matrix"))
                length(matrix) == output_count || throw(ArgumentError(
                    "$label_path.reaction_order_matrix has the wrong row count"))
                numeric_matrix = Matrix{Float64}(undef, output_count, input_count)
                for (row_index, raw_row) in enumerate(matrix)
                    row = collect(_ro_field_array(raw_row,
                        "$label_path.reaction_order_matrix[$row_index]"))
                    length(row) == input_count || throw(ArgumentError(
                        "$label_path.reaction_order_matrix[$row_index] has the wrong rank"))
                    for (column_index, value) in enumerate(row)
                        numeric_matrix[row_index, column_index] =
                            _ro_field_document_coefficient(value, encoding,
                            "$label_path.reaction_order_matrix[$row_index][$column_index]")
                    end
                end
                push!(numeric_labels, (numeric_offsets, numeric_matrix))
                push!(label_signatures, _canonical_json(Dict(
                    "output_offset" => offsets,
                    "reaction_order_matrix" => matrix,
                )))
            end
            length(unique(label_ids)) == length(label_ids) || throw(ArgumentError(
                "$path affine-label ids must be unique"))
            get(cell, "label_order", nothing) == label_ids || throw(ArgumentError(
                "$path.label_order does not equal the affine-label id sequence"))
            label_source_union == Set(sources) || throw(ArgumentError(
                "$path affine-label sources must partition source_regime_ids exactly"))
            set_valued = _ro_field_document_bool(
                get(cell, "set_valued", nothing), "$path.set_valued")
            status = _ro_field_document_string(
                get(cell, "status", nothing), "$path.status")
            status == (set_valued ? "set_valued" : "regular") ||
                throw(ArgumentError("$path status and set_valued disagree"))
            expected_label_condition = set_valued ? length(labels) >= 2 : length(labels) == 1
            expected_label_condition || throw(ArgumentError(
                "$path affine-label multiplicity is inconsistent"))
            set_valued && length(unique(label_signatures)) != length(label_signatures) &&
                throw(ArgumentError("$path set-valued affine labels must be distinct"))
            set_valued ? (set_valued_cells += 1) : (regular_cells += 1)
            cell_vertices[cell_id] = vertices
            cell_regular_affine[cell_id] = set_valued ? nothing : only(numeric_labels)
            cell_area_sum += signed_area
        end
        length(unique(cell_ids)) == length(cell_ids) || throw(ArgumentError(
            "data cell ids must be unique"))
        get(data, "cell_order", nothing) == cell_ids || throw(ArgumentError(
            "data.cell_order does not equal the cell_id sequence"))
        gaps = collect(_ro_field_array(get(data, "gaps", nothing), "data.gaps"))
        cumulative_overlap_area = 0.0
        for left_index in 1:length(cell_ids),
            right_index in (left_index + 1):length(cell_ids)
            right_index > length(cell_ids) && continue
            overlap_area = _ro_field_convex_intersection_area(
                cell_vertices[cell_ids[left_index]],
                cell_vertices[cell_ids[right_index]];
                length_tolerance=exact_length_tolerance)
            cumulative_overlap_area += overlap_area
            overlap_area <= exact_area_tolerance || throw(ArgumentError(
                "cells $(cell_ids[left_index]) and $(cell_ids[right_index]) " *
                "overlap with positive area"))
        end
        cumulative_overlap_area <= exact_area_tolerance || throw(ArgumentError(
            "cumulative cell overlap with positive area exceeds the global tolerance"))
        isempty(gaps) && abs(cell_area_sum - domain_area) > exact_area_tolerance &&
            throw(ArgumentError(
                "gap-free exact cells do not cover the complete declared domain"))
        uncovered_area_upper_bound = max(0.0, domain_area - cell_area_sum) +
            cumulative_overlap_area
        isempty(gaps) && uncovered_area_upper_bound > exact_area_tolerance &&
            throw(ArgumentError(
                "gap-free exact cells exceed the combined global overlap/gap budget"))
        regular_candidate_count == length(cell_sources) || throw(ArgumentError(
            "regular_candidate_regime_count must equal the full-cell source population"))

        strata = collect(_ro_field_array(
            get(data, "singular_strata", nothing), "data.singular_strata"))
        stratum_ids = String[]
        stratum_vertices = Dict{String,Vector{Vector{Float64}}}()
        serialized_sources = copy(cell_sources)
        for (index, raw_stratum) in enumerate(strata)
            path = "data.singular_strata[$index]"
            stratum = _ro_field_exact_keys(
                raw_stratum, _RO_FIELD_STRATUM_KEYS, path)
            stratum_id = _ro_field_document_identifier(
                get(stratum, "stratum_id", nothing), "$path.stratum_id")
            push!(stratum_ids, stratum_id)
            dimension = _ro_field_document_int(
                get(stratum, "dimension", nothing), "$path.dimension")
            dimension in (0, 1) || throw(ArgumentError(
                "$path.dimension must be 0 or 1"))
            raw_vertices = collect(_ro_field_array(
                get(stratum, "vertices", nothing), "$path.vertices"))
            length(raw_vertices) == dimension + 1 || throw(ArgumentError(
                "$path.vertices count is inconsistent with dimension"))
            vertices = Vector{Float64}[
                _ro_field_document_point(vertex, "$path.vertices[$vertex_index]")
                for (vertex_index, vertex) in enumerate(raw_vertices)
            ]
            length(unique(vertices)) == length(vertices) || throw(ArgumentError(
                "$path.vertices must be unique"))
            sort(copy(vertices); by=point -> (point[1], point[2])) == vertices ||
                throw(ArgumentError("$path.vertices must be lexicographically ordered"))
            sources = _ro_field_document_string_array(
                get(stratum, "source_regime_ids", nothing), "$path.source_regime_ids";
                minimum=1, identifier=true)
            union!(serialized_sources, sources)
            nullities_raw = collect(_ro_field_array(
                get(stratum, "nullities", nothing), "$path.nullities"))
            isempty(nullities_raw) && throw(ArgumentError(
                "$path.nullities must not be empty"))
            nullities = Int[
                _ro_field_document_int(value, "$path.nullities[$nullity_index]")
                for (nullity_index, value) in enumerate(nullities_raw)
            ]
            length(unique(nullities)) == length(nullities) || throw(ArgumentError(
                "$path.nullities must be unique"))
            reasons = _ro_field_document_string_array(
                get(stratum, "reasons", nothing), "$path.reasons"; minimum=1)
            all(reason -> reason in ("singular_regime", "lower_dimensional_slice"),
                reasons) || throw(ArgumentError("$path.reasons contains an unsupported value"))
            stratum_vertices[stratum_id] = vertices
        end
        length(unique(stratum_ids)) == length(stratum_ids) || throw(ArgumentError(
            "singular stratum ids must be unique"))
        get(data, "singular_stratum_order", nothing) == stratum_ids ||
            throw(ArgumentError(
                "singular_stratum_order does not equal the stratum_id sequence"))
        length(serialized_sources) <= source_candidate_count || throw(ArgumentError(
            "serialized source regimes exceed source_candidate_regime_count"))

        facets = collect(_ro_field_array(get(data, "facets", nothing), "data.facets"))
        facet_ids = String[]
        facet_geometry = Any[]
        known_cells = Set(cell_ids)
        known_strata = Set(stratum_ids)
        for (index, raw_facet) in enumerate(facets)
            path = "data.facets[$index]"
            facet = _ro_field_exact_keys(raw_facet, _RO_FIELD_FACET_KEYS, path;
                required=_RO_FIELD_FACET_REQUIRED_KEYS)
            push!(facet_ids, _ro_field_document_identifier(
                get(facet, "facet_id", nothing), "$path.facet_id"))
            get(facet, "dimension", nothing) == 1 || throw(ArgumentError(
                "$path.dimension must be 1"))
            kind = _ro_field_document_string(
                get(facet, "kind", nothing), "$path.kind")
            kind in ("domain_boundary", "regime_transition", "singular_boundary") ||
                throw(ArgumentError("$path.kind is unsupported"))
            raw_endpoints = collect(_ro_field_array(
                get(facet, "endpoints", nothing), "$path.endpoints"))
            length(raw_endpoints) == 2 || throw(ArgumentError(
                "$path.endpoints must contain exactly two points"))
            endpoints = Vector{Float64}[
                _ro_field_document_point(point, "$path.endpoints[$endpoint_index]")
                for (endpoint_index, point) in enumerate(raw_endpoints)
            ]
            hypot(endpoints[2][1] - endpoints[1][1],
                endpoints[2][2] - endpoints[1][2]) > exact_length_tolerance ||
                throw(ArgumentError(
                "$path.endpoints must have positive length"))
            sort(copy(endpoints); by=point -> (point[1], point[2])) == endpoints ||
                throw(ArgumentError("$path.endpoints must be lexicographically ordered"))
            haskey(facet, "polyhedron") && _ro_field_validate_polyhedron!(
                facet["polyhedron"], encoding, 2, "$path.polyhedron";
                require_nonempty=true)
            incident = _ro_field_document_string_array(
                get(facet, "incident_cell_ids", nothing), "$path.incident_cell_ids";
                minimum=1, identifier=true)
            issubset(Set(incident), known_cells) || throw(ArgumentError(
                "$path references an unknown incident cell"))
            required_incident_count = kind == "domain_boundary" ? 1 : 2
            length(incident) == required_incident_count || throw(ArgumentError(
                "$path incident-cell count is inconsistent with kind"))
            singular = _ro_field_document_string_array(
                get(facet, "singular_stratum_ids", nothing),
                "$path.singular_stratum_ids"; identifier=true)
            issubset(Set(singular), known_strata) || throw(ArgumentError(
                "$path references an unknown singular stratum"))
            kind == "singular_boundary" && isempty(singular) && throw(ArgumentError(
                "$path singular boundary must reference a singular stratum"))
            kind == "regime_transition" && !isempty(singular) && throw(ArgumentError(
                "$path regime transition cannot reference a singular stratum"))
            expected_singular = sort!(String[
                stratum_id for (stratum_id, vertices) in stratum_vertices
                if length(vertices) == 2 &&
                   _ro_field_segments_have_positive_overlap(
                       endpoints, vertices, exact_length_tolerance)
            ])
            sort(singular) == expected_singular || throw(ArgumentError(
                "$path.singular_stratum_ids does not equal the positively " *
                "overlapping one-dimensional strata"))
            normal = _ro_field_document_point(
                get(facet, "normal", nothing), "$path.normal")
            offset = _ro_field_document_number(
                get(facet, "offset", nothing), "$path.offset")
            dx = endpoints[2][1] - endpoints[1][1]
            dy = endpoints[2][2] - endpoints[1][2]
            magnitude = hypot(dx, dy)
            expected_normal = Float64[dy / magnitude, -dx / magnitude]
            if expected_normal[1] < 0 ||
               (iszero(expected_normal[1]) && expected_normal[2] < 0)
                expected_normal .*= -1
            end
            all(_ro_field_document_close(normal[coordinate],
                    expected_normal[coordinate]) for coordinate in 1:2) ||
                throw(ArgumentError("$path.normal is not the canonical unit normal"))
            expected_offset = -(normal[1] * endpoints[1][1] +
                normal[2] * endpoints[1][2])
            abs(offset - expected_offset) <= exact_length_tolerance ||
                throw(ArgumentError(
                    "$path.offset does not place both endpoints on the facet line"))
            all(endpoint -> abs(normal[1] * endpoint[1] +
                    normal[2] * endpoint[2] + offset) <= exact_length_tolerance,
                endpoints) || throw(ArgumentError(
                    "$path normal/offset does not contain both endpoints"))
            mixed_sign = _ro_field_document_bool(
                get(facet, "mixed_sign", nothing), "$path.mixed_sign")
            mixed_sign == (normal[1] * normal[2] < 0) || throw(ArgumentError(
                "$path.mixed_sign disagrees with the canonical normal"))
            domain_side = get(facet, "domain_side", :missing)
            if kind == "domain_boundary"
                domain_side isa AbstractString || throw(ArgumentError(
                    "$path.domain_side is required for a domain boundary"))
                side = String(domain_side)
                side in ("axis1_lower", "axis1_upper", "axis2_lower", "axis2_upper") ||
                    throw(ArgumentError("$path.domain_side is unsupported"))
                axis_index = startswith(side, "axis1") ? 1 : 2
                bound = endswith(side, "lower") ?
                    domain_bounds[axis_index][1] : domain_bounds[axis_index][2]
                all(endpoint -> abs(endpoint[axis_index] - bound) <=
                        exact_length_tolerance,
                    endpoints) || throw(ArgumentError(
                        "$path.domain_side does not match its endpoints"))
            else
                domain_side === nothing || throw(ArgumentError(
                    "$path.domain_side must be null for an internal facet"))
            end
            push!(facet_geometry, (
                path=path,
                endpoints=endpoints,
                incident=incident,
                singular=singular,
                kind=kind,
                domain_side=domain_side,
            ))
        end
        length(unique(facet_ids)) == length(facet_ids) || throw(ArgumentError(
            "data facet ids must be unique"))
        get(data, "facet_order", nothing) == facet_ids || throw(ArgumentError(
            "data.facet_order does not equal the facet_id sequence"))
        _ro_field_validate_facet_closure!(
            cell_vertices, cell_regular_affine, facet_geometry,
            domain_bounds, exact_length_tolerance)

        gap_ids = String[]
        for (index, raw_gap) in enumerate(gaps)
            path = "data.gaps[$index]"
            gap = _ro_field_exact_keys(raw_gap, _RO_FIELD_GAP_KEYS, path;
                required=_RO_FIELD_GAP_REQUIRED_KEYS)
            push!(gap_ids, _ro_field_document_identifier(
                get(gap, "gap_id", nothing), "$path.gap_id"))
            reason = _ro_field_document_string(
                get(gap, "reason", nothing), "$path.reason")
            reason in ("singular", "higher_nullity", "invalid_geometry",
                "unclassified", "truncated") || throw(ArgumentError(
                    "$path.reason is unsupported"))
            _ro_field_validate_polyhedron!(get(gap, "region", nothing),
                encoding, 2, "$path.region"; require_nonempty=true)
            haskey(gap, "detail") && _ro_field_document_string(
                gap["detail"], "$path.detail"; nonempty=false)
        end
        length(unique(gap_ids)) == length(gap_ids) || throw(ArgumentError(
            "data gap ids must be unique"))
        actual_valid = regular_cells
        actual_invalid = set_valued_cells + length(strata) + length(gaps)
        actual_evaluated = actual_valid + actual_invalid
    end

    coverage = _ro_field_exact_keys(
        get(document, "coverage", nothing), _RO_FIELD_COVERAGE_KEYS, "coverage")
    expected_population = representation == "sampled_grid" ?
        "grid_points" : "cell_complex_items"
    get(coverage, "population_kind", nothing) == expected_population ||
        throw(ArgumentError("coverage.population_kind is inconsistent"))
    eligible = _ro_field_document_int(
        get(coverage, "eligible_count", nothing), "coverage.eligible_count")
    evaluated = _ro_field_document_int(
        get(coverage, "evaluated_count", nothing), "coverage.evaluated_count")
    valid = _ro_field_document_int(
        get(coverage, "valid_count", nothing), "coverage.valid_count")
    invalid = _ro_field_document_int(
        get(coverage, "invalid_count", nothing), "coverage.invalid_count")
    omitted = _ro_field_document_int(
        get(coverage, "omitted_count", nothing), "coverage.omitted_count")
    evaluated == valid + invalid || throw(ArgumentError(
        "coverage.evaluated_count must equal valid_count + invalid_count"))
    eligible == evaluated + omitted || throw(ArgumentError(
        "coverage.eligible_count must equal evaluated_count + omitted_count"))
    (evaluated, valid, invalid) == (actual_evaluated, actual_valid, actual_invalid) ||
        throw(ArgumentError(
            "coverage counts do not match the serialized $expected_population population"))
    enumeration_complete = _ro_field_document_bool(
        get(coverage, "enumeration_complete", nothing),
        "coverage.enumeration_complete")
    truncated = _ro_field_document_bool(
        get(coverage, "truncated", nothing), "coverage.truncated")
    truncation = get(coverage, "truncation", :missing)
    if truncated
        object = _ro_field_exact_keys(
            truncation, _RO_FIELD_TRUNCATION_KEYS, "coverage.truncation")
        reason = _ro_field_document_string(
            get(object, "reason", nothing), "coverage.truncation.reason")
        reason in ("work_budget", "time_budget", "storage_budget", "cancelled",
            "operator_limit", "other") || throw(ArgumentError(
                "coverage.truncation.reason is unsupported"))
        _ro_field_document_string(
            get(object, "detail", nothing), "coverage.truncation.detail")
        !enumeration_complete && omitted > 0 || throw(ArgumentError(
            "truncated coverage must be incomplete and omit at least one item"))
    else
        truncation === nothing || throw(ArgumentError(
            "coverage.truncation must be null when not truncated"))
    end
    enumeration_complete && (truncated || omitted != 0) && throw(ArgumentError(
        "complete enumeration cannot be truncated or omit items"))

    budget = _ro_field_exact_keys(
        get(coverage, "budget", nothing), _RO_FIELD_BUDGET_KEYS, "coverage.budget")
    expected_work_kind = representation == "sampled_grid" ?
        "solver_samples" : "source_regime_candidates"
    get(budget, "work_unit_kind", nothing) == expected_work_kind ||
        throw(ArgumentError("coverage.budget.work_unit_kind is inconsistent"))
    max_evaluated = _ro_field_document_int(
        get(budget, "max_evaluated_items", nothing),
        "coverage.budget.max_evaluated_items"; minimum=1)
    max_stored = _ro_field_document_int(
        get(budget, "max_stored_items", nothing),
        "coverage.budget.max_stored_items"; minimum=1)
    max_payload = _ro_field_document_int(
        get(budget, "max_payload_bytes", nothing),
        "coverage.budget.max_payload_bytes"; minimum=1)
    deadline = get(budget, "deadline_seconds", :missing)
    deadline === nothing || (_ro_field_document_number(
        deadline, "coverage.budget.deadline_seconds") > 0) || throw(ArgumentError(
            "coverage.budget.deadline_seconds must be positive or null"))
    representation == "sampled_grid" && evaluated > max_evaluated &&
        throw(ArgumentError("sampled evaluated population exceeds its work budget"))
    representation == "exact_cell_complex" &&
        source_candidate_count > max_evaluated && throw(ArgumentError(
            "exact source-candidate population exceeds its work budget"))

    storage = _ro_field_exact_keys(
        get(coverage, "storage", nothing), _RO_FIELD_RESPONSE_STORAGE_KEYS,
        "coverage.storage")
    mode = _ro_field_document_string(
        get(storage, "mode", nothing), "coverage.storage.mode")
    mode in ("inline", "chunked", "artifact_reference") || throw(ArgumentError(
        "coverage.storage.mode is unsupported"))
    storage_complete = _ro_field_document_bool(
        get(storage, "complete", nothing), "coverage.storage.complete")
    stored = _ro_field_document_int(
        get(storage, "stored_count", nothing), "coverage.storage.stored_count")
    payload_bytes = _ro_field_document_int(
        get(storage, "payload_bytes", nothing), "coverage.storage.payload_bytes")
    0 <= stored <= evaluated || throw(ArgumentError(
        "coverage.storage.stored_count is outside the evaluated population"))
    stored <= max_stored || throw(ArgumentError(
        "coverage.storage.stored_count exceeds the storage budget"))
    payload_bytes <= max_payload || throw(ArgumentError(
        "coverage.storage.payload_bytes exceeds the payload budget"))
    artifacts = collect(_ro_field_array(
        get(storage, "artifacts", nothing), "coverage.storage.artifacts"))
    for (index, raw_artifact) in enumerate(artifacts)
        path = "coverage.storage.artifacts[$index]"
        artifact = _ro_field_exact_keys(
            raw_artifact, _RO_FIELD_ARTIFACT_REFERENCE_KEYS, path)
        _ro_field_document_string(get(artifact, "artifact_id", nothing),
            "$path.artifact_id")
        _ro_field_document_string(get(artifact, "media_type", nothing),
            "$path.media_type")
        _ro_field_document_sha(get(artifact, "sha256", nothing), "$path.sha256")
        _ro_field_document_int(get(artifact, "byte_length", nothing),
            "$path.byte_length")
        _ro_field_document_int(get(artifact, "item_count", nothing),
            "$path.item_count")
    end
    content_hash = get(storage, "content_sha256", :missing)
    if mode == "inline"
        isempty(artifacts) || throw(ArgumentError(
            "inline storage cannot contain artifact references"))
        content_hash = _ro_field_document_sha(
            content_hash, "coverage.storage.content_sha256")
        if storage_complete
            stored == evaluated || throw(ArgumentError(
                "complete inline storage must store every evaluated item"))
            data_bytes = collect(codeunits(_canonical_json(data)))
            payload_bytes == length(data_bytes) || throw(ArgumentError(
                "coverage.storage.payload_bytes does not match canonical data"))
            content_hash == bytes2hex(SHA.sha256(data_bytes)) || throw(ArgumentError(
                "coverage.storage.content_sha256 does not match canonical data"))
        end
    else
        !isempty(artifacts) || throw(ArgumentError(
            "non-inline storage requires at least one artifact reference"))
        content_hash === nothing || _ro_field_document_sha(
            content_hash, "coverage.storage.content_sha256")
    end

    partial_expected = invalid > 0 || omitted > 0 || truncated ||
        !enumeration_complete || !storage_complete
    partial == partial_expected || throw(ArgumentError(
        "partial does not agree with coverage and storage state"))

    evidence = _ro_field_exact_keys(
        get(document, "evidence", nothing), _RO_FIELD_EVIDENCE_KEYS, "evidence")
    expected_evidence = representation == "sampled_grid" ?
        "sampled_numerical" : "exact_polyhedral"
    get(evidence, "evidence_class", nothing) == expected_evidence ||
        throw(ArgumentError("evidence.evidence_class is inconsistent"))
    status = _ro_field_document_string(
        get(evidence, "status", nothing), "evidence.status")
    status in ("complete", "partial", "failed", "unknown") || throw(ArgumentError(
        "evidence.status is unsupported"))
    get(evidence, "claim_scope", nothing) ==
        "declared_domain_model_configuration_only" || throw(ArgumentError(
            "evidence.claim_scope is unsupported"))
    get(evidence, "validity_policy", nothing) == "invalid_is_gap" ||
        throw(ArgumentError("evidence.validity_policy is unsupported"))
    completeness = _ro_field_document_string(
        get(evidence, "completeness_claim", nothing),
        "evidence.completeness_claim")
    completeness in ("complete_over_declared_population",
        "best_over_evaluated_prefix", "no_positive_claim") || throw(ArgumentError(
            "evidence.completeness_claim is unsupported"))
    if partial
        status != "complete" || throw(ArgumentError(
            "partial evidence cannot have complete status"))
        invalid > 0 && completeness != "no_positive_claim" && throw(ArgumentError(
            "invalid evaluated items require no_positive_claim"))
        completeness != "complete_over_declared_population" || throw(ArgumentError(
            "partial evidence cannot claim complete declared-population coverage"))
    else
        status == "complete" &&
            completeness == "complete_over_declared_population" ||
            throw(ArgumentError(
                "complete evidence requires complete status and completeness claim"))
    end
    _ro_field_document_string_array(
        get(evidence, "limitations", nothing), "evidence.limitations"; minimum=1)

    provenance = _ro_field_exact_keys(
        get(document, "provenance", nothing), _RO_FIELD_PROVENANCE_KEYS,
        "provenance"; required=_RO_FIELD_PROVENANCE_REQUIRED_KEYS)
    _ro_field_document_string(get(provenance, "producer", nothing),
        "provenance.producer")
    revision_status = _ro_field_document_string(
        get(provenance, "source_revision_status", nothing),
        "provenance.source_revision_status")
    if revision_status == "known"
        commit = _ro_field_document_string(
            get(provenance, "source_commit", nothing), "provenance.source_commit")
        occursin(r"^[0-9a-f]{40}$", commit) || throw(ArgumentError(
            "provenance.source_commit must be a lowercase Git object id"))
        _ro_field_document_bool(
            get(provenance, "source_dirty", nothing), "provenance.source_dirty")
    elseif revision_status == "unknown"
        get(provenance, "source_commit", :missing) === nothing &&
            get(provenance, "source_dirty", :missing) === nothing ||
            throw(ArgumentError("unknown source revision must use null fields"))
    else
        throw(ArgumentError("provenance.source_revision_status is unsupported"))
    end
    _ro_field_document_sha(get(provenance, "network_ir_sha256", nothing),
        "provenance.network_ir_sha256")
    domain_sha = _ro_field_document_sha(
        get(provenance, "domain_sha256", nothing), "provenance.domain_sha256")
    domain_sha == _canonical_hash(domain) || throw(ArgumentError(
        "provenance.domain_sha256 does not match canonical domain"))
    algorithm = _ro_field_exact_keys(
        get(provenance, "algorithm", nothing), _RO_FIELD_ALGORITHM_KEYS,
        "provenance.algorithm")
    _ro_field_document_string(get(algorithm, "name", nothing),
        "provenance.algorithm.name")
    _ro_field_document_string(get(algorithm, "version", nothing),
        "provenance.algorithm.version")
    _ro_field_document_sha(get(algorithm, "configuration_sha256", nothing),
        "provenance.algorithm.configuration_sha256")
    if haskey(provenance, "parent_artifact_sha256") &&
       provenance["parent_artifact_sha256"] !== nothing
        _ro_field_document_sha(provenance["parent_artifact_sha256"],
            "provenance.parent_artifact_sha256")
    end
    created_at = _ro_field_document_string(
        get(provenance, "created_at", nothing), "provenance.created_at")
    occursin(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$",
        created_at) || throw(ArgumentError(
            "provenance.created_at must be a UTC RFC3339 timestamp"))
    _ro_field_document_string(get(provenance, "reproduce_command", nothing),
        "provenance.reproduce_command")
    return document
end

const _RO_FIELD_IDENTITY_PAYLOAD_KEYS = Set((
    "schema_version", "domain", "outputs", "component_order", "data",
))

"""
    validate_ro_field_payload!(raw)

Validate the coverage-free canonical exact payload carried by an RPB2 identity.
The payload intentionally omits artifact evidence, storage, and provenance; this
adapter supplies internally consistent envelope claims and delegates all shared
axis/output/data/count/geometry semantics to `validate_ro_field_document!`.
"""
function validate_ro_field_payload!(raw)
    payload = _materialize(raw)
    payload = _ro_field_exact_keys(
        payload, _RO_FIELD_IDENTITY_PAYLOAD_KEYS, "RO-field identity payload")
    get(payload, "schema_version", nothing) == RO_FIELD_SCHEMA_VERSION ||
        throw(ArgumentError("unsupported RO-field identity payload schema version"))
    data = _ro_field_object(get(payload, "data", nothing), "data")
    cells = collect(_ro_field_array(get(data, "cells", nothing), "data.cells"))
    strata = collect(_ro_field_array(
        get(data, "singular_strata", nothing), "data.singular_strata"))
    gaps = collect(_ro_field_array(get(data, "gaps", nothing), "data.gaps"))
    valid_count = count(cell -> cell isa AbstractDict &&
        get(cell, "set_valued", true) === false, cells)
    evaluated_count = length(cells) + length(strata) + length(gaps)
    invalid_count = evaluated_count - valid_count
    partial = invalid_count > 0
    source_count_raw = get(data, "source_candidate_regime_count", 0)
    source_budget = source_count_raw isa Integer && !(source_count_raw isa Bool) &&
        source_count_raw >= 0 ? max(1, Int(source_count_raw)) : 1
    data_json = _canonical_json(data)
    data_bytes = collect(codeunits(data_json))
    content_sha = bytes2hex(SHA.sha256(data_bytes))
    envelope = Dict{String,Any}(
        "schema_version" => payload["schema_version"],
        "field_id" => "rpb2-semantic-validation",
        "representation" => "exact_cell_complex",
        "partial" => partial,
        "domain" => payload["domain"],
        "outputs" => payload["outputs"],
        "component_order" => payload["component_order"],
        "data" => data,
        "coverage" => Dict{String,Any}(
            "population_kind" => "cell_complex_items",
            "eligible_count" => evaluated_count,
            "evaluated_count" => evaluated_count,
            "valid_count" => valid_count,
            "invalid_count" => invalid_count,
            "omitted_count" => 0,
            "enumeration_complete" => true,
            "truncated" => false,
            "truncation" => nothing,
            "budget" => Dict{String,Any}(
                "work_unit_kind" => "source_regime_candidates",
                "max_evaluated_items" => source_budget,
                "max_stored_items" => max(1, evaluated_count),
                "max_payload_bytes" => max(1, length(data_bytes)),
                "deadline_seconds" => nothing,
            ),
            "storage" => Dict{String,Any}(
                "mode" => "inline",
                "complete" => true,
                "stored_count" => evaluated_count,
                "payload_bytes" => length(data_bytes),
                "content_sha256" => content_sha,
                "artifacts" => Any[],
            ),
        ),
        "evidence" => Dict{String,Any}(
            "evidence_class" => "exact_polyhedral",
            "status" => partial ? "partial" : "complete",
            "claim_scope" => "declared_domain_model_configuration_only",
            "validity_policy" => "invalid_is_gap",
            "completeness_claim" => partial ? "no_positive_claim" :
                "complete_over_declared_population",
            "limitations" => Any[
                "RPB2 payload semantic validation uses the canonical exact-field contract.",
            ],
        ),
        "provenance" => Dict{String,Any}(
            "producer" => "RPB2 semantic validator",
            "source_revision_status" => "unknown",
            "source_commit" => nothing,
            "source_dirty" => nothing,
            "network_ir_sha256" => "0"^64,
            "domain_sha256" => _canonical_hash(payload["domain"]),
            "algorithm" => Dict{String,Any}(
                "name" => "rpb2_semantic_validation",
                "version" => RO_FIELD_CELL_COMPLEX_VERSION,
                "configuration_sha256" => "0"^64,
            ),
            "created_at" => "1970-01-01T00:00:00Z",
            "reproduce_command" => "decode and validate the canonical RPB2 payload",
        ),
    )
    validate_ro_field_document!(envelope)
    return payload
end

function _ro_field_revision_provenance()
    revision = lowercase(strip(get(ENV, "BIOCIRCUITS_EXPLORER_REVISION", "")))
    dirty_raw = lowercase(strip(get(
        ENV, "BIOCIRCUITS_EXPLORER_SOURCE_DIRTY", "")))
    if occursin(r"^[0-9a-f]{40}$", revision) &&
       dirty_raw in ("true", "false")
        return "known", revision, dirty_raw == "true"
    end
    return "unknown", nothing, nothing
end

function _ro_field_created_at()
    return Dates.format(Dates.now(Dates.UTC),
        dateformat"yyyy-mm-ddTHH:MM:SS.sss") * "Z"
end

function _ro_field_deadline_check(start_ns::UInt64, deadline)
    deadline === nothing && return nothing
    elapsed = (time_ns() - start_ns) / 1e9
    elapsed <= deadline || throw(ROFieldRequestError(
        "ro_field_deadline_exceeded",
        "The declared synchronous RO-field deadline elapsed. No partial artifact " *
        "was published or stored; the unevaluated region remains unknown and this " *
        "resource outcome is not evidence of scientific infeasibility.",
        computed=true,
    ))
    return nothing
end

function _ro_field_finalize(normalized, data, valid_count::Int, invalid_count::Int,
                            stored_count::Int, evidence_class::String,
                            algorithm_name::String, limitations::Vector{String})
    evaluated_count = valid_count + invalid_count
    stored_count <= evaluated_count || error(
        "internal RO-field coverage error: stored_count exceeds evaluated_count")
    stored_count <= Int(normalized.work_budget["max_stored_items"]) ||
        throw(ROFieldRequestError(
            "ro_field_storage_budget_exceeded",
            "The completed in-memory RO-field contains $stored_count population " *
            "items, exceeding max_stored_items. No artifact was published or " *
            "stored; this resource outcome is not evidence of scientific " *
            "infeasibility.";
            computed=true,
        ))
    data_json = _canonical_json(data)
    payload_bytes = ncodeunits(data_json)
    payload_limit = Int(normalized.work_budget["max_payload_bytes"])
    payload_bytes <= payload_limit || throw(ROFieldRequestError(
        "ro_field_payload_budget_exceeded",
        "The completed in-memory RO-field data payload is $payload_bytes bytes, " *
        "exceeding the declared $payload_limit-byte inline limit. No artifact was " *
        "published or stored; this storage/resource outcome is not evidence of " *
        "scientific infeasibility.";
        computed=true,
    ))
    content_sha = bytes2hex(SHA.sha256(data_json))
    partial = invalid_count > 0
    status = partial ? "partial" : "complete"
    completeness = partial ? "no_positive_claim" :
        "complete_over_declared_population"
    storage = Dict{String,Any}(
        "mode" => "inline",
        "complete" => true,
        "stored_count" => stored_count,
        "payload_bytes" => payload_bytes,
        "content_sha256" => content_sha,
        "artifacts" => Any[],
    )
    population_kind = normalized.representation === :sampled_grid ?
        "grid_points" : "cell_complex_items"
    coverage = Dict{String,Any}(
        "population_kind" => population_kind,
        "eligible_count" => evaluated_count,
        "evaluated_count" => evaluated_count,
        "valid_count" => valid_count,
        "invalid_count" => invalid_count,
        "omitted_count" => 0,
        "enumeration_complete" => true,
        "truncated" => false,
        "truncation" => nothing,
        "budget" => normalized.work_budget,
        "storage" => storage,
    )
    config_sha = _canonical_hash(normalized.normalized_configuration)
    revision_status, source_commit, source_dirty = _ro_field_revision_provenance()
    field_id_hash = _canonical_hash(Dict{String,Any}(
        "network_ir_hash" => normalized.network_ir_hash,
        "configuration_sha256" => config_sha,
        "content_sha256" => content_sha,
    ))
    field = Dict{String,Any}(
        "schema_version" => RO_FIELD_SCHEMA_VERSION,
        "field_id" => "ro-field-$(field_id_hash[1:24])",
        "representation" => String(normalized.representation),
        "partial" => partial,
        "domain" => normalized.domain,
        "outputs" => normalized.outputs,
        "component_order" => normalized.component_order,
        "data" => data,
        "coverage" => coverage,
        "evidence" => Dict{String,Any}(
            "evidence_class" => evidence_class,
            "status" => status,
            "claim_scope" => "declared_domain_model_configuration_only",
            "validity_policy" => "invalid_is_gap",
            "completeness_claim" => completeness,
            "limitations" => limitations,
        ),
        "provenance" => Dict{String,Any}(
            "producer" => "BiocircuitsExplorerBackend /api/v1/ro_field",
            "source_revision_status" => revision_status,
            "source_commit" => source_commit,
            "source_dirty" => source_dirty,
            "network_ir_sha256" => normalized.network_ir_hash,
            "domain_sha256" => _canonical_hash(normalized.domain),
            "algorithm" => Dict{String,Any}(
                "name" => algorithm_name,
                "version" => normalized.representation === :sampled_grid ?
                    RO_FIELD_SAMPLER_VERSION : RO_FIELD_CELL_COMPLEX_VERSION,
                "configuration_sha256" => config_sha,
            ),
            "parent_artifact_sha256" => nothing,
            "created_at" => _ro_field_created_at(),
            "reproduce_command" =>
                "POST /api/v1/ro_field with the normalized " *
                "$(RO_FIELD_REQUEST_VERSION) request",
        ),
    )
    validate_ro_field_document!(field)
    warnings = partial ? String[
        "The RO field contains invalid, singular, ambiguous, or unclassified " *
        "items and is diagnostic only; no positive behavior claim is made.",
    ] : String[]
    artifact = artifact_metadata(
        "ro_field";
        input_hashes=Dict{String,Any}(
            "network_ir_hash" => normalized.network_ir_hash,
            "ro_field_data" => content_sha,
        ),
        algorithm_name=algorithm_name,
        config=normalized.normalized_configuration,
        warnings=warnings,
    )
    # The generic result envelope predates the RO-field RFC3339 requirement.
    # Override its timezone-less timestamp at this stricter boundary.
    artifact["created_at"] = _ro_field_created_at()
    return Dict{String,Any}("ro_field" => field, "artifact" => artifact)
end

"""
    produce_ro_field(raw, bundle) -> Dict

Produce one bounded inline field. Limit, cancellation/deadline, or final payload
failures throw before any response artifact is published; no bounded-search
failure is translated into a scientific infeasibility claim.
"""
function produce_ro_field(raw, bundle)
    normalized = normalize_ro_field_request(raw, bundle)
    model = bundle["model"]
    started_ns = time_ns()
    cancel_check = () -> _ro_field_deadline_check(
        started_ns, normalized.work_budget["deadline_seconds"])
    cancel_check()
    if normalized.representation === :sampled_grid
        sampled = try
            sample_reaction_order_field(
                model,
                normalized.axis_indices,
                normalized.axis_coordinates_engine_log10,
                normalized.output_indices,
                normalized.fixed_engine_logqK;
                max_grid_points=MAX_SYNC_RO_FIELD_POINTS,
                cancel_check=cancel_check,
            )
        catch err
            err isa ROFieldGridLimitExceeded || rethrow()
            throw(ROFieldRequestError(
                "ro_field_work_budget_exceeded",
                "The sampled-grid work limit was exceeded before a complete " *
                "artifact could be published. Nothing was stored and this " *
                "resource outcome is not evidence of scientific infeasibility.";
                computed=true,
            ))
        end
        cancel_check()
        data = _ro_field_serialize_sampled(sampled, normalized)
        valid_count = count(identity, data["validity"])
        point_count = length(data["validity"])
        limitations = String[
            "This is a bounded finite-equilibrium Cartesian sample, not an " *
            "interpolation guarantee, a full high-dimensional Atlas, or " *
            "experimental evidence.",
        ]
        valid_count == point_count || push!(limitations,
            "Invalid or non-converged samples are retained as null gaps and " *
            "cannot support a response-shape claim.")
        return _ro_field_finalize(
            normalized, data, valid_count, point_count - valid_count, point_count,
            "sampled_numerical", "finite_equilibrium_ro_field_sampler",
            limitations,
        )
    end

    axes = normalized.domain["axes"]
    lower_engine = Float64[
        _ro_field_coordinate_to_engine_log10(
            Float64(axis["bounds"]["lower"]), normalized.log_basis,
            log10(Float64(axis["reference"]["value"])))
        for axis in axes
    ]
    upper_engine = Float64[
        _ro_field_coordinate_to_engine_log10(
            Float64(axis["bounds"]["upper"]), normalized.log_basis,
            log10(Float64(axis["reference"]["value"])))
        for axis in axes
    ]
    domain = ROInputDomain2D(
        Tuple(normalized.axis_indices), Tuple(lower_engine), Tuple(upper_engine),
        normalized.fixed_engine_logqK)
    complex = try
        build_ro_cell_complex(
            model, domain, normalized.output_indices;
            limits=normalized.exact_limits,
            geometry_tolerance=normalized.geometry_tolerance,
            cancel_check=cancel_check,
        )
    catch err
        err isa ROCellComplexLimitExceeded || rethrow()
        throw(ROFieldRequestError(
            "ro_field_work_budget_exceeded",
            "The exact-cell construction exceeded its declared hard limit before " *
            "a complete artifact could be published. Nothing was stored, omitted " *
            "regions remain unknown, and this resource outcome is not evidence " *
            "of scientific infeasibility.";
            computed=true,
        ))
    end
    cancel_check()
    _ro_field_require_complete_exact_geometry!(complex)
    data = try
        _ro_field_serialize_exact(complex, normalized)
    catch err
        err isa ROFieldRequestError && rethrow()
        (err isa ArgumentError || err isa ErrorException || err isa DomainError) ||
            rethrow()
        throw(ROFieldRequestError(
            "ro_field_exact_geometry_incomplete",
            "The exact-cell computation completed, but its geometry was " *
            "degenerate or could not be serialized as a closed v1 complex. " *
            "No artifact was published or stored; this geometry outcome is " *
            "not evidence of scientific infeasibility.";
            computed=true,
        ))
    end
    regular_count = count(cell -> cell["set_valued"] === false, data["cells"])
    invalid_count = count(cell -> cell["set_valued"] === true, data["cells"]) +
        length(data["singular_strata"]) + length(data["gaps"])
    stored_count = regular_count + invalid_count
    limitations = String[
        "Exact means complete Float64 polyhedral construction over the declared " *
        "asymptotic binding model, fixed background, and 2D domain; it is not " *
        "finite-nonlinear, biological, arbitrary-precision, or full-Atlas proof.",
    ]
    invalid_count > 0 && push!(limitations,
        "Singular strata, set-valued cells, or geometry gaps are diagnostic only " *
        "and prevent a positive completeness claim.")
    try
        return _ro_field_finalize(
            normalized, data, regular_count, invalid_count, stored_count,
            "exact_polyhedral", "fixed_background_ro_cell_complex_2d", limitations,
        )
    catch err
        err isa ROFieldRequestError && rethrow()
        err isa ArgumentError || rethrow()
        throw(ROFieldRequestError(
            "ro_field_exact_geometry_incomplete",
            "The exact-cell computation completed, but the serialized cells, " *
            "facets, incidence, or affine continuity did not form a valid " *
            "closed v1 complex. No artifact was published or stored; this " *
            "geometry outcome is not evidence of scientific infeasibility.";
            computed=true,
        ))
    end
end
