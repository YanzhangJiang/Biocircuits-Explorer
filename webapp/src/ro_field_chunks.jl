# Pure-local, content-addressed chunk substrate for bounded RO-field runs.
#
# This file intentionally has no HTTP, job-store, or object-store dependency.
# Scientific plan identity excludes runtime location/time/job hints; chunks are
# canonical JSON byte strings addressed by SHA-256 and committed atomically.

const RO_FIELD_CHUNK_PLAN_SCHEMA_VERSION =
    "bne-ro-field-chunk-plan/v1.0.0"
const RO_FIELD_WORK_UNIT_SCHEMA_VERSION =
    "bne-ro-field-work-unit/v1.0.0"
const RO_FIELD_CHUNK_SCHEMA_VERSION =
    "bne-ro-field-chunk/v1.0.0"
const RO_FIELD_CHECKPOINT_SCHEMA_VERSION =
    "bne-ro-field-checkpoint/v1.0.0"
const RO_FIELD_DATASET_MANIFEST_SCHEMA_VERSION =
    "bne-ro-field-dataset-manifest/v1.0.0"

const _ROFC_HARD_MAX_DIMENSIONS = 4
const _ROFC_HARD_MAX_OUTPUTS = 4
const _ROFC_HARD_MAX_POINTS = 4096
const _ROFC_HARD_MAX_WORK_UNITS = 4096
const _ROFC_HARD_MAX_POINTS_PER_WORK_UNIT = 256
const _ROFC_HARD_MAX_SAMPLE_SCALARS = 65_536
const _ROFC_HARD_MAX_CHUNK_BYTES = 4 * 1024 * 1024
const _ROFC_HARD_MAX_SPEC_NODES = 1024
const _ROFC_HARD_MAX_STRING_BYTES = 64 * 1024
const _ROFC_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9._:-]{0,127}$"
const _ROFC_SHA_PATTERN = r"^[0-9a-f]{64}$"
const _ROFC_VOLATILE_SPEC_KEYS = Set((
    "uri", "url", "created_at", "updated_at", "started_at", "finished_at",
    "completed_at", "submitted_at", "timestamp", "wall_time",
    "wall_clock", "wallclock", "wall_clock_time", "wallclock_time",
    "job_id", "jobid", "run_id",
))
const _ROFC_RUNTIME_KEYS = Set(("storage_uri", "created_at", "job_id"))

struct ROFieldChunkLimits
    max_dimensions::Int
    max_outputs::Int
    max_points::Int
    max_work_units::Int
    max_points_per_work_unit::Int
    max_sample_scalars::Int
    max_chunk_bytes::Int
    max_spec_nodes::Int
end

function _rofc_bounded_positive(raw, name::AbstractString, hard_max::Int)
    (raw isa Integer && !(raw isa Bool)) ||
        throw(ArgumentError("$(name) must be an integer"))
    value = try
        Int(raw)
    catch
        throw(ArgumentError("$(name) is outside the supported integer range"))
    end
    0 < value <= hard_max || throw(ArgumentError(
        "$(name) must be in 1:$(hard_max)"))
    return value
end

function ROFieldChunkLimits(;
    max_dimensions::Integer=_ROFC_HARD_MAX_DIMENSIONS,
    max_outputs::Integer=_ROFC_HARD_MAX_OUTPUTS,
    max_points::Integer=_ROFC_HARD_MAX_POINTS,
    max_work_units::Integer=_ROFC_HARD_MAX_WORK_UNITS,
    max_points_per_work_unit::Integer=_ROFC_HARD_MAX_POINTS_PER_WORK_UNIT,
    max_sample_scalars::Integer=_ROFC_HARD_MAX_SAMPLE_SCALARS,
    max_chunk_bytes::Integer=_ROFC_HARD_MAX_CHUNK_BYTES,
    max_spec_nodes::Integer=_ROFC_HARD_MAX_SPEC_NODES,
)
    return ROFieldChunkLimits(
        _rofc_bounded_positive(max_dimensions, "max_dimensions",
            _ROFC_HARD_MAX_DIMENSIONS),
        _rofc_bounded_positive(max_outputs, "max_outputs",
            _ROFC_HARD_MAX_OUTPUTS),
        _rofc_bounded_positive(max_points, "max_points",
            _ROFC_HARD_MAX_POINTS),
        _rofc_bounded_positive(max_work_units, "max_work_units",
            _ROFC_HARD_MAX_WORK_UNITS),
        _rofc_bounded_positive(max_points_per_work_unit,
            "max_points_per_work_unit",
            _ROFC_HARD_MAX_POINTS_PER_WORK_UNIT),
        _rofc_bounded_positive(max_sample_scalars,
            "max_sample_scalars", _ROFC_HARD_MAX_SAMPLE_SCALARS),
        _rofc_bounded_positive(max_chunk_bytes, "max_chunk_bytes",
            _ROFC_HARD_MAX_CHUNK_BYTES),
        _rofc_bounded_positive(max_spec_nodes, "max_spec_nodes",
            _ROFC_HARD_MAX_SPEC_NODES),
    )
end

struct ROFieldChunkLimitExceeded <: Exception
    phase::Symbol
    requested::BigInt
    limit::Int
end

function Base.showerror(io::IO, err::ROFieldChunkLimitExceeded)
    print(io, "RO-field chunk ", err.phase, " requires ", err.requested,
        ", exceeding limit=", err.limit)
end

struct ROFieldChunkContractError <: Exception
    code::Symbol
    message::String
end

function Base.showerror(io::IO, err::ROFieldChunkContractError)
    print(io, "RO-field chunk contract error [", err.code, "]: ",
        err.message)
end

@inline function _rofc_limit(phase::Symbol, requested::BigInt, limit::Int)
    requested <= limit || throw(ROFieldChunkLimitExceeded(
        phase, requested, limit))
    return nothing
end

_rofc_error(code::Symbol, message::AbstractString) =
    throw(ROFieldChunkContractError(code, String(message)))

function _rofc_normalized_key(raw_key, path::AbstractString)
    (raw_key isa AbstractString || raw_key isa Symbol) || _rofc_error(
        :invalid_document, "$(path) keys must be strings or symbols")
    key = String(raw_key)
    ncodeunits(key) <= _ROFC_HARD_MAX_STRING_BYTES || _rofc_error(
        :string_too_large,
        "$(path) contains an oversized object key")
    return key
end

"Recursively materialize JSON values without silently merging duplicate keys."
function _rofc_materialize(value, path::AbstractString="document")
    if value isa AbstractDict
        out = Dict{String,Any}()
        for (raw_key, child) in pairs(value)
            key = _rofc_normalized_key(raw_key, path)
            haskey(out, key) && _rofc_error(
                :duplicate_key,
                "$(path) contains duplicate string/symbol forms of $(key)")
            out[key] = _rofc_materialize(child, "$(path).$(key)")
        end
        return out
    elseif value isa AbstractVector || value isa Tuple
        return Any[_rofc_materialize(child, "$(path)[]") for child in value]
    elseif value isa Symbol
        return _rofc_string(String(value), path; nonempty=false)
    elseif value isa AbstractString
        return _rofc_string(value, path; nonempty=false)
    end
    return value
end

"Private canonical JSON matching the project policy with BigInt-safe floats."
function _rofc_canonical_json(value)::String
    if value isa AbstractDict
        entries = Pair{String,Any}[]
        seen = Set{String}()
        for (raw_key, child) in pairs(value)
            key = _rofc_normalized_key(raw_key, "canonical JSON object")
            key in seen && _rofc_error(
                :duplicate_key,
                "canonical JSON object contains duplicate key $(key)")
            push!(seen, key)
            push!(entries, key => child)
        end
        sort!(entries; by=first)
        return "{" * join((
            JSON3.write(key) * ":" * _rofc_canonical_json(child)
            for (key, child) in entries
        ), ",") * "}"
    elseif value isa AbstractVector || value isa Tuple
        return "[" * join((_rofc_canonical_json(child) for child in value), ",") * "]"
    elseif value isa Symbol
        return JSON3.write(String(value))
    elseif value isa Bool
        return value ? "true" : "false"
    elseif value isa Integer
        return string(value)
    elseif value isa AbstractFloat
        isfinite(value) || _rofc_error(
            :nonfinite_value, "non-finite numbers cannot be canonicalized")
        return isinteger(value) ? string(BigInt(value)) : JSON3.write(Float64(value))
    elseif value === nothing
        return "null"
    elseif value isa AbstractString
        return JSON3.write(String(value))
    end
    _rofc_error(:invalid_document,
        "canonical JSON contains an unsupported value")
end

_rofc_bytes(value) = collect(codeunits(_rofc_canonical_json(value)))
_rofc_sha256_bytes(bytes::AbstractVector{UInt8}) =
    bytes2hex(SHA.sha256(bytes))
_rofc_sha256(value) = _rofc_sha256_bytes(_rofc_bytes(value))

function _rofc_object(raw, path::AbstractString)
    raw isa AbstractDict || _rofc_error(
        :invalid_document, "$(path) must be an object")
    return raw
end

function _rofc_array(raw, path::AbstractString)
    (raw isa AbstractVector || raw isa Tuple) || _rofc_error(
        :invalid_document, "$(path) must be an array")
    return collect(raw)
end

function _rofc_exact_keys(raw, expected, path::AbstractString)
    object = _rofc_object(raw, path)
    observed = String[]
    for key in keys(object)
        (key isa AbstractString || key isa Symbol) || _rofc_error(
            :invalid_document, "$(path) keys must be strings or symbols")
        push!(observed, String(key))
    end
    length(observed) == length(expected) &&
        Set(observed) == Set(expected) || _rofc_error(
            :invalid_document, "$(path) has an unexpected key set")
    return object
end

function _rofc_get(raw::AbstractDict, key::AbstractString,
                   path::AbstractString)
    has_string = haskey(raw, key)
    has_symbol = haskey(raw, Symbol(key))
    has_string && has_symbol && _rofc_error(
        :invalid_document,
        "$(path) contains duplicate string/symbol forms of $(key)")
    has_string && return raw[key]
    has_symbol && return raw[Symbol(key)]
    _rofc_error(:invalid_document, "$(path) is missing $(key)")
end

function _rofc_string(raw, path::AbstractString; nonempty::Bool=true)
    raw isa AbstractString || _rofc_error(
        :invalid_document, "$(path) must be a string")
    value = String(raw)
    nonempty && isempty(value) && _rofc_error(
        :invalid_document, "$(path) must not be empty")
    ncodeunits(value) <= _ROFC_HARD_MAX_STRING_BYTES || _rofc_error(
        :string_too_large,
        "$(path) exceeds $(_ROFC_HARD_MAX_STRING_BYTES) UTF-8 bytes")
    return value
end

function _rofc_identifier(raw, path::AbstractString)
    value = _rofc_string(raw, path)
    occursin(_ROFC_ID_PATTERN, value) || _rofc_error(
        :invalid_document, "$(path) is not a safe identifier")
    return value
end

function _rofc_hash(raw, path::AbstractString)
    value = _rofc_string(raw, path)
    occursin(_ROFC_SHA_PATTERN, value) || _rofc_error(
        :invalid_document,
        "$(path) must contain 64 lowercase hexadecimal characters")
    return value
end

function _rofc_int(raw, path::AbstractString; minimum::Int=0)
    (raw isa Integer && !(raw isa Bool)) || _rofc_error(
        :invalid_document, "$(path) must be an integer")
    value = try
        Int(raw)
    catch
        _rofc_error(:invalid_document,
            "$(path) is outside the supported integer range")
    end
    value >= minimum || _rofc_error(
        :invalid_document, "$(path) must be at least $(minimum)")
    return value
end

function _rofc_finite(raw, path::AbstractString)
    (raw isa Real && !(raw isa Bool)) || _rofc_error(
        :invalid_document, "$(path) must be numeric")
    value = Float64(raw)
    isfinite(value) || _rofc_error(
        :nonfinite_value, "$(path) must be finite")
    # Canonical JSON intentionally gives `-0.0` and `0.0` one identity.  Keep
    # the normalized in-memory document equally unambiguous so an explicit
    # point set cannot smuggle in two byte-identical signed-zero points.
    return value == 0.0 ? 0.0 : value
end

function _rofc_ids(raw, path::AbstractString, minimum::Int,
                   maximum::Int)
    values = _rofc_array(raw, path)
    minimum <= length(values) <= maximum || _rofc_error(
        :invalid_document,
        "$(path) must contain $(minimum):$(maximum) identifiers")
    result = String[_rofc_identifier(value, "$(path)[]") for value in values]
    allunique(result) || _rofc_error(
        :invalid_document, "$(path) identifiers must be unique")
    return result
end

function _rofc_count_json_nodes(raw, limit::Int)
    count = Ref(BigInt(0))
    function visit(value, path, depth)
        depth <= 32 || _rofc_error(
            :invalid_spec, "$(path) exceeds maximum nesting depth")
        count[] += 1
        _rofc_limit(:spec_nodes, count[], limit)
        if value isa AbstractDict
            for (key, child) in pairs(value)
                normalized_key = _rofc_normalized_key(key, path)
                visit(child, "$(path).$(normalized_key)", depth + 1)
            end
        elseif value isa AbstractVector || value isa Tuple
            for child in value
                visit(child, "$(path)[]", depth + 1)
            end
        end
    end
    visit(raw, "computation_spec", 0)
    return count[]
end

function _rofc_metadata_key_token(key::AbstractString)
    characters = collect(String(key))
    io = IOBuffer()
    for index in eachindex(characters)
        character = characters[index]
        if character == '-' || character == '.' || isspace(character)
            position(io) > 0 && write(io, '_')
            continue
        end
        if isuppercase(character) && index > firstindex(characters)
            previous = characters[index - 1]
            following_is_lower = index < lastindex(characters) &&
                islowercase(characters[index + 1])
            (islowercase(previous) || isdigit(previous) ||
                (isuppercase(previous) && following_is_lower)) && write(io, '_')
        end
        write(io, lowercase(character))
    end
    return String(take!(io))
end

function _rofc_is_volatile_spec_key(key::AbstractString)
    normalized = _rofc_metadata_key_token(key)
    return normalized in _ROFC_VOLATILE_SPEC_KEYS ||
        endswith(normalized, "_uri") ||
        endswith(normalized, "_url") ||
        endswith(normalized, "_job_id") ||
        endswith(normalized, "_run_id") ||
        endswith(normalized, "_timestamp") ||
        any(endswith(normalized, suffix) for suffix in (
            "_created_at", "_updated_at", "_started_at", "_finished_at",
            "_completed_at", "_submitted_at",
        ))
end

function _rofc_normalize_spec(raw, limits::ROFieldChunkLimits)
    _rofc_count_json_nodes(raw, limits.max_spec_nodes)
    function normalize(value, path)
        if value isa AbstractDict
            out = Dict{String,Any}()
            for (raw_key, child) in pairs(value)
                key = _rofc_normalized_key(raw_key, path)
                _rofc_is_volatile_spec_key(key) && _rofc_error(
                    :volatile_identity_field,
                    "$(path).$(key) belongs in runtime_context, not the plan identity",
                )
                haskey(out, key) && _rofc_error(
                    :invalid_spec, "$(path) contains duplicate key $(key)")
                out[key] = normalize(child, "$(path).$(key)")
            end
            return out
        elseif value isa AbstractVector || value isa Tuple
            return Any[normalize(child, "$(path)[]") for child in value]
        elseif value isa AbstractFloat
            return _rofc_finite(value, path)
        elseif value isa Integer && !(value isa Bool)
            return value
        elseif value isa Bool || value === nothing
            return value
        elseif value isa AbstractString
            return _rofc_string(value, path; nonempty=false)
        elseif value isa Symbol
            return _rofc_string(String(value), path; nonempty=false)
        end
        _rofc_error(:invalid_spec,
            "$(path) contains an unsupported JSON value")
    end
    return normalize(raw, "computation_spec")
end

function _rofc_runtime_context(raw)
    object = _rofc_object(raw, "runtime_context")
    observed = Set{String}()
    for raw_key in keys(object)
        key = _rofc_normalized_key(raw_key, "runtime_context")
        key in observed && _rofc_error(
            :invalid_runtime_context,
            "runtime_context contains duplicate key $(key)",
        )
        push!(observed, key)
    end
    issubset(observed, _ROFC_RUNTIME_KEYS) || _rofc_error(
        :invalid_runtime_context,
        "runtime_context supports storage_uri, created_at, and job_id only",
    )
    out = Dict{String,Any}()
    for key in sort!(collect(_ROFC_RUNTIME_KEYS))
        value = haskey(object, key) ? object[key] :
            haskey(object, Symbol(key)) ? object[Symbol(key)] : nothing
        value === nothing || value isa AbstractString || _rofc_error(
            :invalid_runtime_context,
            "runtime_context.$(key) must be a string or null",
        )
        out[key] = value === nothing ? nothing :
            _rofc_string(value, "runtime_context.$(key)"; nonempty=false)
    end
    return out
end

function _rofc_normalize_point(raw, dimensions::Int, path::AbstractString)
    values = _rofc_array(raw, path)
    length(values) == dimensions || _rofc_error(
        :invalid_point, "$(path) must contain $(dimensions) coordinates")
    return Float64[_rofc_finite(value, "$(path)[]") for value in values]
end

function _rofc_plan_preflight(
    axis_ids,
    output_ids,
    explicit_points,
    axis_coordinates,
    work_unit_size,
    computation_spec,
    limits::ROFieldChunkLimits,
)
    (axis_ids isa AbstractVector || axis_ids isa Tuple) ||
        _rofc_error(:invalid_plan, "axis_ids must be an array")
    (output_ids isa AbstractVector || output_ids isa Tuple) ||
        _rofc_error(:invalid_plan, "output_ids must be an array")
    dimensions = length(axis_ids)
    outputs = length(output_ids)
    1 <= dimensions || _rofc_error(:invalid_plan,
        "at least one axis is required")
    1 <= outputs || _rofc_error(:invalid_plan,
        "at least one output is required")
    _rofc_limit(:dimensions, BigInt(dimensions), limits.max_dimensions)
    _rofc_limit(:outputs, BigInt(outputs), limits.max_outputs)
    size_value = _rofc_int(work_unit_size, "work_unit_size"; minimum=1)
    _rofc_limit(:points_per_work_unit, BigInt(size_value),
        limits.max_points_per_work_unit)
    (explicit_points === nothing) != (axis_coordinates === nothing) ||
        _rofc_error(:invalid_plan,
            "provide exactly one of explicit_points or axis_coordinates")

    point_count = if explicit_points !== nothing
        points = _rofc_array(explicit_points, "explicit_points")
        isempty(points) && _rofc_error(:invalid_plan,
            "explicit_points must not be empty")
        BigInt(length(points))
    else
        axes = _rofc_array(axis_coordinates, "axis_coordinates")
        length(axes) == dimensions || _rofc_error(
            :invalid_plan,
            "axis_coordinates must contain one coordinate array per axis")
        product = BigInt(1)
        for (index, raw_axis) in enumerate(axes)
            coordinates = _rofc_array(raw_axis,
                "axis_coordinates[$(index)]")
            isempty(coordinates) && _rofc_error(:invalid_plan,
                "Cartesian coordinate arrays must not be empty")
            product *= BigInt(length(coordinates))
        end
        product
    end
    _rofc_limit(:points, point_count, limits.max_points)
    work_units = cld(point_count, BigInt(size_value))
    _rofc_limit(:work_units, work_units, limits.max_work_units)
    _rofc_count_json_nodes(computation_spec, limits.max_spec_nodes)
    return dimensions, outputs, Int(point_count), size_value,
        Int(work_units)
end

"""
Build a bounded deterministic plan for an explicit point set or Cartesian grid.

`runtime_context` is retained for operators but is deliberately excluded from
`plan_sha256`; location, wall-clock, and job identity therefore cannot change
scientific work-unit identity.
"""
function build_ro_field_chunk_plan(;
    axis_ids,
    output_ids,
    explicit_points=nothing,
    axis_coordinates=nothing,
    computation_spec=Dict{String,Any}(),
    work_unit_size::Integer=64,
    runtime_context=Dict{String,Any}(),
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    dimensions, outputs, point_count, unit_size, _ =
        _rofc_plan_preflight(
            axis_ids, output_ids, explicit_points, axis_coordinates,
            work_unit_size, computation_spec, limits)
    ordered_axis_ids = _rofc_ids(
        axis_ids, "axis_ids", 1, limits.max_dimensions)
    ordered_output_ids = _rofc_ids(
        output_ids, "output_ids", 1, limits.max_outputs)
    cancel_check()

    normalized_points = nothing
    normalized_axes = nothing
    plan_kind = explicit_points === nothing ?
        "cartesian_grid" : "explicit_points"
    if explicit_points !== nothing
        normalized_points = Vector{Float64}[]
        for (index, raw_point) in enumerate(
            _rofc_array(explicit_points, "explicit_points"))
            cancel_check()
            push!(normalized_points, _rofc_normalize_point(
                raw_point, dimensions, "explicit_points[$(index)]"))
        end
        sort!(normalized_points; by=point -> Tuple(point))
        length(unique(Tuple.(normalized_points))) == length(normalized_points) ||
            _rofc_error(:duplicate_point,
                "explicit_points must describe a mathematical set")
    else
        normalized_axes = Vector{Float64}[]
        for (axis_index, raw_axis) in enumerate(
            _rofc_array(axis_coordinates, "axis_coordinates"))
            cancel_check()
            coordinates = Float64[
                _rofc_finite(value,
                    "axis_coordinates[$(axis_index)][]")
                for value in _rofc_array(
                    raw_axis, "axis_coordinates[$(axis_index)]")
            ]
            all(coordinates[index] < coordinates[index + 1]
                for index in 1:(length(coordinates) - 1)) ||
                _rofc_error(:invalid_grid_axis,
                    "Cartesian coordinates must be strictly increasing")
            push!(normalized_axes, coordinates)
        end
    end
    normalized_spec = _rofc_normalize_spec(computation_spec, limits)
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CHUNK_PLAN_SCHEMA_VERSION,
        "plan_kind" => plan_kind,
        "axis_ids" => ordered_axis_ids,
        "output_ids" => ordered_output_ids,
        "point_count" => point_count,
        "work_unit_size" => unit_size,
        "explicit_points" => normalized_points,
        "axis_coordinates" => normalized_axes,
        "computation_spec" => normalized_spec,
    )
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_CHUNK_PLAN_SCHEMA_VERSION,
        "plan_sha256" => _rofc_sha256(identity),
        "identity" => identity,
        "runtime_context" => _rofc_runtime_context(runtime_context),
    )
end

function validate_ro_field_chunk_plan!(
    raw;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = _rofc_exact_keys(_rofc_materialize(raw), (
        "schema_version", "plan_sha256", "identity", "runtime_context",
    ), "plan")
    _rofc_get(plan, "schema_version", "plan") ==
        RO_FIELD_CHUNK_PLAN_SCHEMA_VERSION || _rofc_error(
            :unsupported_version, "unsupported chunk-plan version")
    identity = _rofc_exact_keys(_rofc_get(plan, "identity", "plan"), (
        "schema_version", "plan_kind", "axis_ids", "output_ids",
        "point_count", "work_unit_size", "explicit_points",
        "axis_coordinates", "computation_spec",
    ), "plan.identity")
    kind = _rofc_string(_rofc_get(identity, "plan_kind", "plan.identity"),
        "plan.identity.plan_kind")
    kind in ("explicit_points", "cartesian_grid") || _rofc_error(
        :invalid_plan, "unsupported point-source kind")
    expected = build_ro_field_chunk_plan(
        axis_ids=_rofc_get(identity, "axis_ids", "plan.identity"),
        output_ids=_rofc_get(identity, "output_ids", "plan.identity"),
        explicit_points=kind == "explicit_points" ?
            _rofc_get(identity, "explicit_points", "plan.identity") : nothing,
        axis_coordinates=kind == "cartesian_grid" ?
            _rofc_get(identity, "axis_coordinates", "plan.identity") : nothing,
        computation_spec=_rofc_get(
            identity, "computation_spec", "plan.identity"),
        work_unit_size=_rofc_get(
            identity, "work_unit_size", "plan.identity"),
        runtime_context=_rofc_get(plan, "runtime_context", "plan"),
        limits=limits,
        cancel_check=cancel_check,
    )
    _rofc_int(_rofc_get(identity, "point_count", "plan.identity"),
        "plan.identity.point_count"; minimum=1) ==
        expected["identity"]["point_count"] || _rofc_error(
            :plan_count_mismatch, "plan point_count is not derived from its points")
    supplied_hash = _rofc_hash(
        _rofc_get(plan, "plan_sha256", "plan"), "plan.plan_sha256")
    supplied_hash == expected["plan_sha256"] || _rofc_error(
        :plan_hash_mismatch, "plan identity does not match plan_sha256")
    _rofc_canonical_json(plan) == _rofc_canonical_json(expected) ||
        _rofc_error(:noncanonical_plan,
            "plan is not in canonical normalized form")
    return expected
end

ro_field_chunk_plan_sha256(plan) =
    validate_ro_field_chunk_plan!(plan)["plan_sha256"]

function _rofc_plan_point(plan, point_index::Int)
    identity = plan["identity"]
    1 <= point_index <= identity["point_count"] ||
        throw(BoundsError(1:identity["point_count"], point_index))
    if identity["plan_kind"] == "explicit_points"
        return copy(identity["explicit_points"][point_index])
    end
    coordinates = identity["axis_coordinates"]
    remaining = point_index - 1
    positions = Vector{Int}(undef, length(coordinates))
    for axis_index in reverse(eachindex(coordinates))
        axis_length = length(coordinates[axis_index])
        positions[axis_index] = mod(remaining, axis_length) + 1
        remaining = div(remaining, axis_length)
    end
    return Float64[
        coordinates[index][positions[index]] for index in eachindex(coordinates)
    ]
end

function _rofc_plan_work_unit(plan, ordinal::Int)
    identity = plan["identity"]
    point_count = identity["point_count"]
    unit_size = identity["work_unit_size"]
    unit_count = cld(point_count, unit_size)
    1 <= ordinal <= unit_count || _rofc_error(
        :foreign_work_unit, "work-unit ordinal is outside the plan")
    point_start = (ordinal - 1) * unit_size + 1
    point_stop = min(point_count, ordinal * unit_size)
    indices = collect(point_start:point_stop)
    return Dict{String,Any}(
        "schema_version" => RO_FIELD_WORK_UNIT_SCHEMA_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "work_unit_id" => "wu-" * lpad(string(ordinal), 6, '0'),
        "ordinal" => ordinal,
        "point_start" => point_start,
        "point_count" => length(indices),
        "point_indices" => indices,
        "points" => [_rofc_plan_point(plan, index) for index in indices],
    )
end

function ro_field_plan_work_units(
    raw_plan;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    identity = plan["identity"]
    unit_count = cld(identity["point_count"], identity["work_unit_size"])
    _rofc_limit(:work_units, BigInt(unit_count), limits.max_work_units)
    units = Dict{String,Any}[]
    for ordinal in 1:unit_count
        cancel_check()
        push!(units, _rofc_plan_work_unit(plan, ordinal))
    end
    return units
end

function validate_ro_field_work_unit!(
    raw_work_unit,
    raw_plan;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    unit = _rofc_exact_keys(_rofc_materialize(raw_work_unit), (
        "schema_version", "plan_sha256", "work_unit_id", "ordinal",
        "point_start", "point_count", "point_indices", "points",
    ), "work_unit")
    _rofc_get(unit, "schema_version", "work_unit") ==
        RO_FIELD_WORK_UNIT_SCHEMA_VERSION || _rofc_error(
            :unsupported_version, "unsupported work-unit version")
    _rofc_hash(_rofc_get(unit, "plan_sha256", "work_unit"),
        "work_unit.plan_sha256")
    supplied_point_count = _rofc_int(
        _rofc_get(unit, "point_count", "work_unit"),
        "work_unit.point_count"; minimum=1)
    _rofc_limit(:points_per_work_unit, BigInt(supplied_point_count),
        limits.max_points_per_work_unit)
    length(_rofc_array(_rofc_get(unit, "point_indices", "work_unit"),
        "work_unit.point_indices")) == supplied_point_count || _rofc_error(
            :work_unit_count_mismatch,
            "work_unit point_count and point_indices disagree")
    length(_rofc_array(_rofc_get(unit, "points", "work_unit"),
        "work_unit.points")) == supplied_point_count || _rofc_error(
            :work_unit_count_mismatch,
            "work_unit point_count and points disagree")
    ordinal = _rofc_int(_rofc_get(unit, "ordinal", "work_unit"),
        "work_unit.ordinal"; minimum=1)
    unit_count = cld(plan["identity"]["point_count"],
        plan["identity"]["work_unit_size"])
    ordinal <= unit_count || _rofc_error(
        :foreign_work_unit, "work-unit ordinal is outside the plan")
    cancel_check()
    expected = _rofc_plan_work_unit(plan, ordinal)
    _rofc_canonical_json(unit) == _rofc_canonical_json(expected) ||
        _rofc_error(:foreign_work_unit,
            "work unit is not the deterministic unit for this plan")
    return expected
end

function ro_field_work_unit_sha256(work_unit, plan)
    validated = validate_ro_field_work_unit!(work_unit, plan)
    return _rofc_sha256(validated)
end

function _rofc_normalize_gap(raw, path::AbstractString)
    gap = _rofc_exact_keys(raw, ("reason", "detail"), path)
    reason = _rofc_identifier(_rofc_get(gap, "reason", path),
        "$(path).reason")
    raw_detail = _rofc_get(gap, "detail", path)
    detail = raw_detail === nothing ? nothing :
        _rofc_string(raw_detail, "$(path).detail"; nonempty=false)
    return Dict{String,Any}("reason" => reason, "detail" => detail)
end

function _rofc_normalize_raw_sample(raw, point_index::Int, point,
                                    axis_count::Int, output_count::Int,
                                    path::AbstractString)
    sample = _rofc_object(raw, path)
    status = _rofc_string(_rofc_get(sample, "status", path),
        "$(path).status")
    if status == "valid"
        _rofc_exact_keys(sample, (
            "status", "output_values", "reaction_order_matrix", "regime_id",
        ), path)
        outputs = _rofc_array(
            _rofc_get(sample, "output_values", path),
            "$(path).output_values")
        length(outputs) == output_count || _rofc_error(
            :invalid_sample_shape,
            "$(path).output_values has the wrong shape")
        output_values = Float64[
            _rofc_finite(value, "$(path).output_values[]")
            for value in outputs
        ]
        raw_matrix = _rofc_array(
            _rofc_get(sample, "reaction_order_matrix", path),
            "$(path).reaction_order_matrix")
        length(raw_matrix) == output_count || _rofc_error(
            :invalid_sample_shape,
            "$(path).reaction_order_matrix has the wrong row count")
        matrix = Vector{Float64}[]
        for (row_index, raw_row) in enumerate(raw_matrix)
            row = _rofc_array(raw_row,
                "$(path).reaction_order_matrix[$(row_index)]")
            length(row) == axis_count || _rofc_error(
                :invalid_sample_shape,
                "reaction-order rows must match axis_ids")
            push!(matrix, Float64[
                _rofc_finite(value, "reaction_order_matrix[]")
                for value in row
            ])
        end
        raw_regime = _rofc_get(sample, "regime_id", path)
        regime_id = raw_regime === nothing ? nothing :
            _rofc_identifier(raw_regime, "$(path).regime_id")
        return Dict{String,Any}(
            "point_index" => point_index,
            "point" => copy(point),
            "status" => "valid",
            "output_values" => output_values,
            "reaction_order_matrix" => matrix,
            "regime_id" => regime_id,
            "gap" => nothing,
        )
    elseif status == "invalid"
        _rofc_exact_keys(sample, ("status", "gap"), path)
        return Dict{String,Any}(
            "point_index" => point_index,
            "point" => copy(point),
            "status" => "invalid",
            "output_values" => nothing,
            "reaction_order_matrix" => nothing,
            "regime_id" => nothing,
            "gap" => _rofc_normalize_gap(
                _rofc_get(sample, "gap", path), "$(path).gap"),
        )
    end
    _rofc_error(:invalid_sample_status,
        "$(path).status must be valid or invalid")
end

function build_ro_field_chunk(
    raw_plan,
    raw_work_unit,
    raw_samples;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    # Size rejection precedes any cooperative callback or full plan rebuild.
    samples = _rofc_array(raw_samples, "samples")
    raw_plan_object = _rofc_object(raw_plan, "plan")
    raw_identity = _rofc_object(
        _rofc_get(raw_plan_object, "identity", "plan"), "plan.identity")
    raw_axis_count = length(_rofc_array(
        _rofc_get(raw_identity, "axis_ids", "plan.identity"),
        "plan.identity.axis_ids"))
    raw_output_count = length(_rofc_array(
        _rofc_get(raw_identity, "output_ids", "plan.identity"),
        "plan.identity.output_ids"))
    raw_unit = _rofc_object(raw_work_unit, "work_unit")
    raw_point_count = _rofc_int(
        _rofc_get(raw_unit, "point_count", "work_unit"),
        "work_unit.point_count"; minimum=1)
    _rofc_limit(:points_per_work_unit, BigInt(raw_point_count),
        limits.max_points_per_work_unit)
    length(samples) == raw_point_count || _rofc_error(
        :sample_count_mismatch,
        "samples must cover every work-unit point exactly once")
    scalar_count = BigInt(length(samples)) * BigInt(raw_output_count) *
        BigInt(raw_axis_count + 1)
    _rofc_limit(:sample_scalars, scalar_count, limits.max_sample_scalars)

    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    work_unit = validate_ro_field_work_unit!(
        raw_work_unit, plan; limits=limits, cancel_check=cancel_check)
    axis_count = length(plan["identity"]["axis_ids"])
    output_count = length(plan["identity"]["output_ids"])
    cancel_check()
    normalized_samples = Dict{String,Any}[]
    for index in eachindex(samples)
        cancel_check()
        push!(normalized_samples, _rofc_normalize_raw_sample(
            samples[index],
            work_unit["point_indices"][index],
            work_unit["points"][index],
            axis_count,
            output_count,
            "samples[$(index)]",
        ))
    end
    valid_count = count(sample -> sample["status"] == "valid",
        normalized_samples)
    invalid_count = length(normalized_samples) - valid_count
    chunk = Dict{String,Any}(
        "schema_version" => RO_FIELD_CHUNK_SCHEMA_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "work_unit_sha256" => _rofc_sha256(work_unit),
        "work_unit_id" => work_unit["work_unit_id"],
        "ordinal" => work_unit["ordinal"],
        "axis_ids" => plan["identity"]["axis_ids"],
        "output_ids" => plan["identity"]["output_ids"],
        "point_start" => work_unit["point_start"],
        "point_count" => work_unit["point_count"],
        "sample_order" => work_unit["point_indices"],
        "samples" => normalized_samples,
        "valid_count" => valid_count,
        "invalid_count" => invalid_count,
    )
    _rofc_limit(:chunk_bytes, BigInt(length(_rofc_bytes(chunk))),
        limits.max_chunk_bytes)
    return chunk
end

function _rofc_validate_stored_sample(raw, axis_count::Int,
                                      output_count::Int,
                                      point_index::Int, point,
                                      path::AbstractString)
    sample = _rofc_exact_keys(raw, (
        "point_index", "point", "status", "output_values",
        "reaction_order_matrix", "regime_id", "gap",
    ), path)
    _rofc_int(_rofc_get(sample, "point_index", path),
        "$(path).point_index"; minimum=1) == point_index || _rofc_error(
            :sample_order_mismatch, "$(path) has the wrong point_index")
    normalized_point = _rofc_normalize_point(
        _rofc_get(sample, "point", path), axis_count, "$(path).point")
    normalized_point == point || _rofc_error(
        :sample_point_mismatch, "$(path) has the wrong point coordinates")
    status = _rofc_string(_rofc_get(sample, "status", path),
        "$(path).status")
    raw_for_builder = if status == "valid"
        _rofc_get(sample, "gap", path) === nothing || _rofc_error(
            :fabricated_numeric_gap,
            "valid samples cannot carry a gap record")
        Dict{String,Any}(
            "status" => "valid",
            "output_values" => _rofc_get(sample, "output_values", path),
            "reaction_order_matrix" =>
                _rofc_get(sample, "reaction_order_matrix", path),
            "regime_id" => _rofc_get(sample, "regime_id", path),
        )
    elseif status == "invalid"
        _rofc_get(sample, "output_values", path) === nothing &&
            _rofc_get(sample, "reaction_order_matrix", path) === nothing &&
            _rofc_get(sample, "regime_id", path) === nothing ||
            _rofc_error(:fabricated_numeric_gap,
                "invalid samples must retain null numeric fields")
        Dict{String,Any}(
            "status" => "invalid",
            "gap" => _rofc_get(sample, "gap", path),
        )
    else
        _rofc_error(:invalid_sample_status,
            "$(path).status must be valid or invalid")
    end
    return _rofc_normalize_raw_sample(raw_for_builder, point_index, point,
        axis_count, output_count, path)
end

function validate_ro_field_chunk!(
    raw_chunk;
    plan=nothing,
    work_unit=nothing,
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    (plan === nothing) == (work_unit === nothing) || _rofc_error(
        :incomplete_validation_context,
        "plan and work_unit must be supplied together")
    chunk = _rofc_exact_keys(_rofc_materialize(raw_chunk), (
        "schema_version", "plan_sha256", "work_unit_sha256",
        "work_unit_id", "ordinal", "axis_ids", "output_ids",
        "point_start", "point_count", "sample_order", "samples",
        "valid_count", "invalid_count",
    ), "chunk")
    _rofc_get(chunk, "schema_version", "chunk") ==
        RO_FIELD_CHUNK_SCHEMA_VERSION || _rofc_error(
            :unsupported_version, "unsupported chunk version")
    plan_hash = _rofc_hash(_rofc_get(chunk, "plan_sha256", "chunk"),
        "chunk.plan_sha256")
    work_hash = _rofc_hash(
        _rofc_get(chunk, "work_unit_sha256", "chunk"),
        "chunk.work_unit_sha256")
    axis_ids = _rofc_ids(_rofc_get(chunk, "axis_ids", "chunk"),
        "chunk.axis_ids", 1, limits.max_dimensions)
    output_ids = _rofc_ids(_rofc_get(chunk, "output_ids", "chunk"),
        "chunk.output_ids", 1, limits.max_outputs)
    point_count = _rofc_int(_rofc_get(chunk, "point_count", "chunk"),
        "chunk.point_count"; minimum=1)
    _rofc_limit(:points_per_work_unit, BigInt(point_count),
        limits.max_points_per_work_unit)
    sample_order = Int[
        _rofc_int(value, "chunk.sample_order[]"; minimum=1)
        for value in _rofc_array(
            _rofc_get(chunk, "sample_order", "chunk"),
            "chunk.sample_order")
    ]
    samples = _rofc_array(_rofc_get(chunk, "samples", "chunk"),
        "chunk.samples")
    length(sample_order) == point_count == length(samples) ||
        _rofc_error(:sample_count_mismatch,
            "chunk point_count, sample_order, and samples disagree")
    point_start = _rofc_int(
        _rofc_get(chunk, "point_start", "chunk"),
        "chunk.point_start"; minimum=1)
    sample_order == collect(point_start:(point_start + point_count - 1)) ||
        _rofc_error(:sample_order_mismatch,
            "chunk sample_order must be one contiguous work-unit range")
    ordinal = _rofc_int(_rofc_get(chunk, "ordinal", "chunk"),
        "chunk.ordinal"; minimum=1)
    work_unit_id = _rofc_identifier(
        _rofc_get(chunk, "work_unit_id", "chunk"),
        "chunk.work_unit_id")
    work_unit_id == "wu-" * lpad(string(ordinal), 6, '0') ||
        _rofc_error(:work_unit_identity_mismatch,
            "chunk work_unit_id does not match ordinal")

    expected_points = if plan === nothing || work_unit === nothing
        Vector{Float64}[
            _rofc_normalize_point(
                _rofc_get(_rofc_object(sample, "chunk.samples[]"),
                    "point", "chunk.samples[]"),
                length(axis_ids), "chunk.samples[].point")
            for sample in samples
        ]
    else
        validated_plan = validate_ro_field_chunk_plan!(plan;
            limits=limits, cancel_check=cancel_check)
        validated_unit = validate_ro_field_work_unit!(
            work_unit, validated_plan;
            limits=limits, cancel_check=cancel_check)
        plan_hash == validated_plan["plan_sha256"] || _rofc_error(
            :foreign_chunk, "chunk references a different plan")
        work_hash == _rofc_sha256(validated_unit) || _rofc_error(
            :foreign_chunk, "chunk references a different work unit")
        axis_ids == validated_plan["identity"]["axis_ids"] &&
            output_ids == validated_plan["identity"]["output_ids"] ||
            _rofc_error(:foreign_chunk,
                "chunk axis/output order differs from its plan")
        sample_order == validated_unit["point_indices"] || _rofc_error(
            :foreign_chunk, "chunk point order differs from its work unit")
        _rofc_get(chunk, "work_unit_id", "chunk") ==
            validated_unit["work_unit_id"] &&
            _rofc_get(chunk, "ordinal", "chunk") ==
                validated_unit["ordinal"] &&
            _rofc_get(chunk, "point_start", "chunk") ==
                validated_unit["point_start"] || _rofc_error(
                    :foreign_chunk,
                    "chunk work-unit metadata differs from its work unit")
        validated_unit["points"]
    end
    normalized_samples = Dict{String,Any}[]
    for index in eachindex(samples)
        cancel_check()
        push!(normalized_samples, _rofc_validate_stored_sample(
            samples[index], length(axis_ids), length(output_ids),
            sample_order[index], expected_points[index],
            "chunk.samples[$(index)]"))
    end
    valid_count = count(sample -> sample["status"] == "valid",
        normalized_samples)
    invalid_count = length(samples) - valid_count
    _rofc_int(_rofc_get(chunk, "valid_count", "chunk"),
        "chunk.valid_count") == valid_count &&
        _rofc_int(_rofc_get(chunk, "invalid_count", "chunk"),
            "chunk.invalid_count") == invalid_count || _rofc_error(
                :chunk_count_mismatch,
                "chunk valid/invalid counts do not match samples")
    normalized = Dict{String,Any}(
        "schema_version" => RO_FIELD_CHUNK_SCHEMA_VERSION,
        "plan_sha256" => plan_hash,
        "work_unit_sha256" => work_hash,
        "work_unit_id" => work_unit_id,
        "ordinal" => ordinal,
        "axis_ids" => axis_ids,
        "output_ids" => output_ids,
        "point_start" => point_start,
        "point_count" => point_count,
        "sample_order" => sample_order,
        "samples" => normalized_samples,
        "valid_count" => valid_count,
        "invalid_count" => invalid_count,
    )
    _rofc_canonical_json(chunk) == _rofc_canonical_json(normalized) ||
        _rofc_error(:noncanonical_chunk,
            "chunk is not in canonical normalized form")
    _rofc_limit(:chunk_bytes, BigInt(length(_rofc_bytes(normalized))),
        limits.max_chunk_bytes)
    return normalized
end

function _rofc_validate_chunk_context!(chunk, plan, work_unit)
    chunk["plan_sha256"] == plan["plan_sha256"] || _rofc_error(
        :foreign_chunk, "chunk references a different plan")
    chunk["work_unit_sha256"] == _rofc_sha256(work_unit) || _rofc_error(
        :foreign_chunk, "chunk references a different work unit")
    chunk["axis_ids"] == plan["identity"]["axis_ids"] &&
        chunk["output_ids"] == plan["identity"]["output_ids"] || _rofc_error(
            :foreign_chunk,
            "chunk axis/output order differs from its plan")
    chunk["sample_order"] == work_unit["point_indices"] || _rofc_error(
        :foreign_chunk, "chunk point order differs from its work unit")
    chunk["work_unit_id"] == work_unit["work_unit_id"] &&
        chunk["ordinal"] == work_unit["ordinal"] &&
        chunk["point_start"] == work_unit["point_start"] &&
        chunk["point_count"] == work_unit["point_count"] || _rofc_error(
            :foreign_chunk,
            "chunk work-unit metadata differs from its work unit")
    for index in eachindex(chunk["samples"])
        chunk["samples"][index]["point"] == work_unit["points"][index] ||
            _rofc_error(:sample_point_mismatch,
                "chunk sample point differs from its work unit")
    end
    return chunk
end

function canonical_ro_field_chunk_bytes(
    chunk;
    plan=nothing,
    work_unit=nothing,
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
)
    normalized = validate_ro_field_chunk!(chunk;
        plan=plan, work_unit=work_unit, limits=limits)
    return _rofc_bytes(normalized)
end

function ro_field_chunk_sha256(chunk; kwargs...)
    return _rofc_sha256_bytes(canonical_ro_field_chunk_bytes(chunk; kwargs...))
end

function _rofc_fsync_file!(io::IO, path::AbstractString)
    (Sys.isapple() || Sys.islinux()) || throw(ErrorException(
        "durable RO-field chunk storage requires macOS or Linux"))
    flush(io)
    result = ccall(:fsync, Cint, (Cint,), fd(io))
    Base.systemerror("fsync RO-field chunk $(path)", result != 0)
    return nothing
end

function _rofc_fsync_directory!(path::AbstractString)
    (Sys.isapple() || Sys.islinux()) || throw(ErrorException(
        "durable RO-field chunk storage requires macOS or Linux"))
    directory = abspath(String(path))
    directory_fd = ccall(:open, Cint, (Cstring, Cint), directory, 0)
    Base.systemerror("open RO-field chunk directory $(directory)",
        directory_fd < 0)
    try
        result = ccall(:fsync, Cint, (Cint,), directory_fd)
        Base.systemerror("fsync RO-field chunk directory $(directory)",
            result != 0)
    finally
        ccall(:close, Cint, (Cint,), directory_fd)
    end
    return nothing
end

function _rofc_fsync_existing_file!(path::AbstractString)
    open(String(path), "r") do io
        result = ccall(:fsync, Cint, (Cint,), fd(io))
        Base.systemerror("fsync existing RO-field chunk $(path)", result != 0)
    end
    return nothing
end

function _rofc_ensure_directory!(path::AbstractString)
    directory = abspath(String(path))
    islink(directory) && _rofc_error(
        :invalid_storage_root,
        "chunk storage path must not be a symbolic link")
    if isdir(directory)
        # Re-syncing the parent also repairs the durability boundary when this
        # directory survived a prior writer that crashed before parent fsync.
        _rofc_fsync_directory!(dirname(directory))
        return directory
    end
    ispath(directory) && _rofc_error(
        :invalid_storage_root, "chunk storage path is not a directory")

    missing = String[]
    cursor = directory
    while !isdir(cursor)
        islink(cursor) && _rofc_error(
            :invalid_storage_root,
            "chunk storage path contains a symbolic-link leaf")
        ispath(cursor) && _rofc_error(
            :invalid_storage_root,
            "chunk storage ancestor is not a directory")
        push!(missing, cursor)
        parent = dirname(cursor)
        parent == cursor && _rofc_error(
            :invalid_storage_root,
            "chunk storage path has no existing directory ancestor")
        cursor = parent
    end
    mkpath(directory)
    for created_directory in reverse(missing)
        isdir(created_directory) || _rofc_error(
            :invalid_storage_root,
            "chunk storage directory creation was incomplete")
        _rofc_fsync_directory!(dirname(created_directory))
    end
    return directory
end

function _rofc_existing_content_matches(path::AbstractString,
                                        expected::Vector{UInt8})
    islink(path) && _rofc_error(:unsafe_existing_path,
        "content-addressed destination is a symbolic link")
    isfile(path) || _rofc_error(:chunk_hash_collision,
        "content-addressed destination exists but is not a regular file")
    filesize(path) == length(expected) || _rofc_error(
        :chunk_hash_collision,
        "existing content has the wrong byte length at the same chunk SHA-256 path")
    observed = open(path, "r") do io
        read(io, length(expected) + 1)
    end
    observed == expected || _rofc_error(:chunk_hash_collision,
        "existing content differs at the same chunk SHA-256 path")
    _rofc_fsync_existing_file!(path)
    return true
end

"Atomically commit canonical chunk bytes under `root/chunks/<sha256>.json`."
function write_ro_field_chunk!(
    root::AbstractString,
    chunk;
    plan=nothing,
    work_unit=nothing,
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
)
    bytes = canonical_ro_field_chunk_bytes(chunk;
        plan=plan, work_unit=work_unit, limits=limits)
    chunk_hash = _rofc_sha256_bytes(bytes)
    directory = _rofc_ensure_directory!(joinpath(String(root), "chunks"))
    destination = joinpath(directory, chunk_hash * ".json")
    if ispath(destination)
        _rofc_existing_content_matches(destination, bytes)
        _rofc_fsync_directory!(directory)
        return destination
    end

    temp_path, temp_io = mktemp(directory; cleanup=false)
    try
        write(temp_io, bytes)
        _rofc_fsync_file!(temp_io, temp_path)
        close(temp_io)
        result = ccall(:link, Cint, (Cstring, Cstring),
            temp_path, destination)
        if result != 0
            ispath(destination) || Base.systemerror(
                "link RO-field chunk $(temp_path) to $(destination)", true)
            _rofc_existing_content_matches(destination, bytes)
            _rofc_fsync_directory!(directory)
        else
            _rofc_fsync_directory!(directory)
        end
        return destination
    finally
        isopen(temp_io) && close(temp_io)
        if isfile(temp_path)
            rm(temp_path; force=true)
            _rofc_fsync_directory!(directory)
        end
    end
end

function read_ro_field_chunk(
    path::AbstractString;
    expected_sha256=nothing,
    plan=nothing,
    work_unit=nothing,
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
)
    chunk_path = String(path)
    islink(chunk_path) && _rofc_error(
        :invalid_chunk_path, "chunk path must not be a symbolic link")
    isfile(chunk_path) || _rofc_error(
        :invalid_chunk_path, "chunk path is not a regular file")
    _rofc_limit(:chunk_bytes, BigInt(filesize(chunk_path)),
        limits.max_chunk_bytes)
    filename_match = match(r"^([0-9a-f]{64})\.json$", basename(chunk_path))
    path_hash = filename_match === nothing ? nothing : filename_match.captures[1]
    expected = if expected_sha256 === nothing
        path_hash === nothing && _rofc_error(
            :missing_chunk_identity,
            "expected_sha256 is required when the filename is not <sha256>.json")
        path_hash
    else
        supplied = _rofc_hash(expected_sha256, "expected_sha256")
        path_hash !== nothing && path_hash != supplied && _rofc_error(
            :chunk_path_hash_mismatch,
            "expected_sha256 disagrees with the content-addressed filename")
        supplied
    end
    raw_bytes = open(chunk_path, "r") do io
        read(io, limits.max_chunk_bytes + 1)
    end
    # Repeat the bound after the open/read so a concurrent replacement cannot
    # bypass the preflight `filesize` check and trigger an unbounded read.
    _rofc_limit(:chunk_bytes, BigInt(length(raw_bytes)),
        limits.max_chunk_bytes)
    document = try
        # `String(::Vector{UInt8})` may take ownership and empty its input.
        # Preserve the authoritative stored bytes for the exact canonical-byte
        # and content-hash checks below.
        _rofc_materialize(JSON3.read(String(copy(raw_bytes))))
    catch err
        _rofc_error(:invalid_chunk_json,
            "chunk JSON cannot be parsed: $(sprint(showerror, err))")
    end
    normalized = validate_ro_field_chunk!(document;
        plan=plan, work_unit=work_unit, limits=limits)
    canonical = _rofc_bytes(normalized)
    raw_bytes == canonical || _rofc_error(
        :noncanonical_chunk_bytes,
        "stored chunk bytes are not canonical JSON bytes")
    actual_hash = _rofc_sha256_bytes(canonical)
    actual_hash == expected || _rofc_error(
        :chunk_hash_mismatch,
        "stored chunk bytes do not match the required content identity")
    return normalized
end

function _rofc_units_and_chunks(
    plan,
    chunks;
    limits::ROFieldChunkLimits,
    cancel_check,
)
    units = ro_field_plan_work_units(plan;
        limits=limits, cancel_check=cancel_check)
    raw_chunks = _rofc_array(chunks, "chunks")
    _rofc_limit(:work_units, BigInt(length(raw_chunks)),
        limits.max_work_units)
    units_by_hash = Dict(_rofc_sha256(unit) => unit for unit in units)
    normalized_by_unit = Dict{String,Dict{String,Any}}()
    for raw_chunk in raw_chunks
        cancel_check()
        object = _rofc_object(raw_chunk, "chunks[]")
        work_hash = _rofc_hash(
            _rofc_get(object, "work_unit_sha256", "chunks[]"),
            "chunks[].work_unit_sha256")
        haskey(units_by_hash, work_hash) || _rofc_error(
            :foreign_chunk, "chunk references a work unit outside the plan")
        haskey(normalized_by_unit, work_hash) && _rofc_error(
            :duplicate_chunk, "more than one chunk commits one work unit")
        normalized = validate_ro_field_chunk!(object;
            limits=limits, cancel_check=cancel_check)
        normalized_by_unit[work_hash] = _rofc_validate_chunk_context!(
            normalized, plan, units_by_hash[work_hash])
    end
    return units, units_by_hash, normalized_by_unit
end

function _rofc_commit_entry(unit, chunk)
    return Dict{String,Any}(
        "ordinal" => unit["ordinal"],
        "work_unit_id" => unit["work_unit_id"],
        "work_unit_sha256" => _rofc_sha256(unit),
        "chunk_sha256" => _rofc_sha256(chunk),
        "point_start" => unit["point_start"],
        "point_count" => unit["point_count"],
        "valid_count" => chunk["valid_count"],
        "invalid_count" => chunk["invalid_count"],
        # This is the exact canonical byte length of the addressed chunk, not
        # an estimate and not a filesystem allocation size.  Keeping it in
        # every authenticated commit entry lets checkpoints and manifests
        # prove the cumulative logical dataset payload across resume.
        "chunk_payload_bytes" => length(_rofc_bytes(chunk)),
    )
end

function _rofc_with_self_hash(identity, hash_key::AbstractString)
    document = copy(identity)
    document[String(hash_key)] = _rofc_sha256(identity)
    return document
end

function build_ro_field_checkpoint(
    raw_plan,
    chunks;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    units, units_by_hash, chunks_by_unit = _rofc_units_and_chunks(
        plan, chunks; limits=limits, cancel_check=cancel_check)
    entries = Dict{String,Any}[]
    for unit in units
        cancel_check()
        work_hash = _rofc_sha256(unit)
        haskey(chunks_by_unit, work_hash) || continue
        push!(entries, _rofc_commit_entry(
            units_by_hash[work_hash], chunks_by_unit[work_hash]))
    end
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_CHECKPOINT_SCHEMA_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "committed_work_unit_count" => length(entries),
        "committed_point_count" => sum(
            entry["point_count"] for entry in entries; init=0),
        "committed_payload_bytes" => sum(
            entry["chunk_payload_bytes"] for entry in entries; init=0),
        "committed" => entries,
    )
    return _rofc_with_self_hash(identity, "checkpoint_sha256")
end

function validate_ro_field_checkpoint!(
    raw_checkpoint,
    raw_plan,
    chunks;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    checkpoint = _rofc_exact_keys(_rofc_materialize(raw_checkpoint), (
        "schema_version", "plan_sha256", "committed_work_unit_count",
        "committed_point_count", "committed_payload_bytes", "committed",
        "checkpoint_sha256",
    ), "checkpoint")
    _rofc_get(checkpoint, "schema_version", "checkpoint") ==
        RO_FIELD_CHECKPOINT_SCHEMA_VERSION || _rofc_error(
            :unsupported_version, "unsupported checkpoint version")
    _rofc_hash(_rofc_get(checkpoint, "plan_sha256", "checkpoint"),
        "checkpoint.plan_sha256")
    supplied_unit_count = _rofc_int(
        _rofc_get(checkpoint, "committed_work_unit_count", "checkpoint"),
        "checkpoint.committed_work_unit_count")
    supplied_point_count = _rofc_int(
        _rofc_get(checkpoint, "committed_point_count", "checkpoint"),
        "checkpoint.committed_point_count")
    supplied_payload_bytes = _rofc_int(
        _rofc_get(checkpoint, "committed_payload_bytes", "checkpoint"),
        "checkpoint.committed_payload_bytes")
    _rofc_limit(:work_units, BigInt(supplied_unit_count),
        limits.max_work_units)
    _rofc_limit(:points, BigInt(supplied_point_count), limits.max_points)
    _rofc_limit(:dataset_payload_bytes, BigInt(supplied_payload_bytes),
        limits.max_work_units * limits.max_chunk_bytes)
    length(_rofc_array(_rofc_get(checkpoint, "committed", "checkpoint"),
        "checkpoint.committed")) == supplied_unit_count || _rofc_error(
            :checkpoint_count_mismatch,
            "checkpoint committed_work_unit_count and committed disagree")
    expected = build_ro_field_checkpoint(raw_plan, chunks;
        limits=limits, cancel_check=cancel_check)
    supplied_hash = _rofc_hash(
        _rofc_get(checkpoint, "checkpoint_sha256", "checkpoint"),
        "checkpoint.checkpoint_sha256")
    supplied_hash == expected["checkpoint_sha256"] || _rofc_error(
        :checkpoint_hash_mismatch,
        "checkpoint content does not match checkpoint_sha256")
    _rofc_canonical_json(checkpoint) == _rofc_canonical_json(expected) ||
        _rofc_error(:checkpoint_mismatch,
            "checkpoint is unordered, tampered, or refers to foreign chunks")
    return expected
end

"Return only deterministic work units not proven committed by verified chunks."
function resume_ro_field_work_units(
    raw_plan,
    raw_checkpoint,
    chunks;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    checkpoint = validate_ro_field_checkpoint!(
        raw_checkpoint, plan, chunks;
        limits=limits, cancel_check=cancel_check)
    committed = Set(String(entry["work_unit_sha256"])
        for entry in checkpoint["committed"])
    missing = Dict{String,Any}[]
    for unit in ro_field_plan_work_units(plan;
        limits=limits, cancel_check=cancel_check)
        cancel_check()
        _rofc_sha256(unit) in committed || push!(missing, unit)
    end
    return missing
end

function build_ro_field_dataset_manifest(
    raw_plan,
    chunks;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    plan = validate_ro_field_chunk_plan!(raw_plan;
        limits=limits, cancel_check=cancel_check)
    units, _, chunks_by_unit = _rofc_units_and_chunks(
        plan, chunks; limits=limits, cancel_check=cancel_check)
    length(chunks_by_unit) == length(units) || _rofc_error(
        :incomplete_dataset,
        "dataset manifest requires exactly one chunk for every work unit")
    entries = Dict{String,Any}[]
    for unit in units
        cancel_check()
        work_hash = _rofc_sha256(unit)
        haskey(chunks_by_unit, work_hash) || _rofc_error(
            :incomplete_dataset, "dataset is missing a work-unit chunk")
        push!(entries, _rofc_commit_entry(unit, chunks_by_unit[work_hash]))
    end
    valid_count = sum(entry["valid_count"] for entry in entries; init=0)
    invalid_count = sum(entry["invalid_count"] for entry in entries; init=0)
    identity = Dict{String,Any}(
        "schema_version" => RO_FIELD_DATASET_MANIFEST_SCHEMA_VERSION,
        "plan_sha256" => plan["plan_sha256"],
        "point_count" => plan["identity"]["point_count"],
        "work_unit_count" => length(units),
        "chunk_count" => length(entries),
        "valid_count" => valid_count,
        "invalid_count" => invalid_count,
        "chunk_payload_bytes" => sum(
            entry["chunk_payload_bytes"] for entry in entries; init=0),
        "chunks" => entries,
    )
    valid_count + invalid_count == identity["point_count"] ||
        _rofc_error(:dataset_count_mismatch,
            "dataset sample counts do not cover the plan")
    return _rofc_with_self_hash(identity, "manifest_sha256")
end

function validate_ro_field_dataset_manifest!(
    raw_manifest,
    raw_plan,
    chunks;
    limits::ROFieldChunkLimits=ROFieldChunkLimits(),
    cancel_check=() -> nothing,
)
    manifest = _rofc_exact_keys(_rofc_materialize(raw_manifest), (
        "schema_version", "plan_sha256", "point_count",
        "work_unit_count", "chunk_count", "valid_count", "invalid_count",
        "chunk_payload_bytes", "chunks", "manifest_sha256",
    ), "manifest")
    _rofc_get(manifest, "schema_version", "manifest") ==
        RO_FIELD_DATASET_MANIFEST_SCHEMA_VERSION || _rofc_error(
            :unsupported_version, "unsupported dataset-manifest version")
    _rofc_hash(_rofc_get(manifest, "plan_sha256", "manifest"),
        "manifest.plan_sha256")
    supplied_point_count = _rofc_int(
        _rofc_get(manifest, "point_count", "manifest"),
        "manifest.point_count"; minimum=1)
    supplied_unit_count = _rofc_int(
        _rofc_get(manifest, "work_unit_count", "manifest"),
        "manifest.work_unit_count"; minimum=1)
    supplied_chunk_count = _rofc_int(
        _rofc_get(manifest, "chunk_count", "manifest"),
        "manifest.chunk_count"; minimum=1)
    supplied_valid_count = _rofc_int(
        _rofc_get(manifest, "valid_count", "manifest"),
        "manifest.valid_count")
    supplied_invalid_count = _rofc_int(
        _rofc_get(manifest, "invalid_count", "manifest"),
        "manifest.invalid_count")
    supplied_payload_bytes = _rofc_int(
        _rofc_get(manifest, "chunk_payload_bytes", "manifest"),
        "manifest.chunk_payload_bytes")
    _rofc_limit(:points, BigInt(supplied_point_count), limits.max_points)
    _rofc_limit(:work_units, BigInt(supplied_unit_count),
        limits.max_work_units)
    _rofc_limit(:work_units, BigInt(supplied_chunk_count),
        limits.max_work_units)
    _rofc_limit(:points,
        BigInt(supplied_valid_count) + BigInt(supplied_invalid_count),
        limits.max_points)
    _rofc_limit(:dataset_payload_bytes, BigInt(supplied_payload_bytes),
        limits.max_work_units * limits.max_chunk_bytes)
    length(_rofc_array(_rofc_get(manifest, "chunks", "manifest"),
        "manifest.chunks")) == supplied_chunk_count || _rofc_error(
            :manifest_count_mismatch,
            "manifest chunk_count and chunks disagree")
    expected = build_ro_field_dataset_manifest(raw_plan, chunks;
        limits=limits, cancel_check=cancel_check)
    supplied_hash = _rofc_hash(
        _rofc_get(manifest, "manifest_sha256", "manifest"),
        "manifest.manifest_sha256")
    supplied_hash == expected["manifest_sha256"] || _rofc_error(
        :manifest_hash_mismatch,
        "manifest content does not match manifest_sha256")
    _rofc_canonical_json(manifest) == _rofc_canonical_json(expected) ||
        _rofc_error(:manifest_mismatch,
            "manifest is unordered, tampered, or refers to foreign chunks")
    return expected
end
