# Versioned content identity for multi-input reaction-order fields.
#
# RPB1 remains the SISO path/profile codec in behavior_program_codec.jl.  RPB2
# is deliberately a separate identity: it encodes a complete, single-valued
# exact 2-D cell complex and never silently projects it to a sequence.

const RO_CELL_COMPLEX_MAGIC = UInt8['R', 'P', 'B', '2']
const RO_CELL_COMPLEX_CODEC_VERSION = 1
const RO_CELL_COMPLEX_IDENTITY_KIND = "exact_cell_complex_v1"
const RO_FIELD_SCHEMA_VERSION = "bne-ro-field/v1.0.0"

struct ROFieldIdentityError <: Exception
    code::Symbol
    message::String
end

function Base.showerror(io::IO, err::ROFieldIdentityError)
    print(io, "RO field identity error [", err.code, "]: ", err.message)
end

_ro_field_identity_error(code::Symbol, message::AbstractString) =
    throw(ROFieldIdentityError(code, String(message)))

function _ro_field_identity_get(raw, key::AbstractString)
    raw isa AbstractDict || _ro_field_identity_error(
        :invalid_document,
        "expected an object while reading $(key)",
    )
    if haskey(raw, key)
        return raw[key]
    end
    symbol = Symbol(key)
    haskey(raw, symbol) && return raw[symbol]
    _ro_field_identity_error(:invalid_document, "missing required field $(key)")
end

function _ro_field_identity_optional(raw, key::AbstractString, default=nothing)
    raw isa AbstractDict || return default
    haskey(raw, key) && return raw[key]
    symbol = Symbol(key)
    haskey(raw, symbol) && return raw[symbol]
    return default
end

function _ro_field_identity_bool(raw, key::AbstractString)
    value = _ro_field_identity_get(raw, key)
    value isa Bool || _ro_field_identity_error(
        :invalid_document,
        "$(key) must be a boolean",
    )
    return value
end

function _ro_field_identity_int(raw, key::AbstractString)
    value = _ro_field_identity_get(raw, key)
    (value isa Integer && !(value isa Bool)) || _ro_field_identity_error(
        :invalid_document,
        "$(key) must be an integer",
    )
    return Int(value)
end

function _ro_field_identity_sha256_text(value, path::AbstractString)
    value isa AbstractString || _ro_field_identity_error(
        :invalid_sha256,
        "$(path) must be a lowercase SHA-256 string",
    )
    text = String(value)
    occursin(r"^[0-9a-f]{64}$", text) || _ro_field_identity_error(
        :invalid_sha256,
        "$(path) must be 64 lowercase hexadecimal characters",
    )
    return text
end

function _ro_field_identity_vector(raw, key::AbstractString)
    value = _ro_field_identity_get(raw, key)
    (value isa AbstractVector || value isa Tuple) || _ro_field_identity_error(
        :invalid_document,
        "$(key) must be an array",
    )
    return collect(value)
end

function _ro_field_materialize(raw)
    if isdefined(@__MODULE__, :_materialize)
        return getfield(@__MODULE__, :_materialize)(raw)
    end
    if raw isa AbstractDict
        return Dict{String, Any}(
            String(key) => _ro_field_materialize(value) for (key, value) in pairs(raw)
        )
    elseif raw isa AbstractVector || raw isa Tuple
        return Any[_ro_field_materialize(value) for value in raw]
    elseif raw isa Symbol
        return String(raw)
    end
    return raw
end

function _ro_field_canonical_json(raw)::String
    if isdefined(@__MODULE__, :_canonical_json)
        return getfield(@__MODULE__, :_canonical_json)(raw)
    end
    if raw isa AbstractDict
        key_strings = sort!(String[String(key) for key in keys(raw)])
        parts = String[]
        for key in key_strings
            value = haskey(raw, key) ? raw[key] : raw[Symbol(key)]
            push!(parts, JSON3.write(key) * ":" * _ro_field_canonical_json(value))
        end
        return "{" * join(parts, ",") * "}"
    elseif raw isa AbstractVector || raw isa Tuple
        return "[" * join((_ro_field_canonical_json(value) for value in raw), ",") * "]"
    elseif raw isa Symbol
        return JSON3.write(String(raw))
    elseif raw isa Bool
        return raw ? "true" : "false"
    elseif raw isa Integer
        return string(raw)
    elseif raw isa AbstractFloat
        isfinite(raw) || _ro_field_identity_error(
            :non_finite_value,
            "non-finite numbers cannot participate in a reaction-order field identity",
        )
        return isinteger(raw) ? string(Integer(raw)) : JSON3.write(Float64(raw))
    elseif raw === nothing
        return "null"
    elseif raw isa AbstractString
        return JSON3.write(String(raw))
    end
    return JSON3.write(raw)
end

_ro_field_sha256(bytes::AbstractVector{UInt8}) = bytes2hex(SHA.sha256(bytes))
_ro_field_utf8_bytes(text::AbstractString) = collect(codeunits(String(text)))

"Canonical bytes of the representation-specific inline `data` member."
function canonical_ro_field_data_bytes(document)
    data = _ro_field_identity_get(document, "data")
    return _ro_field_utf8_bytes(_ro_field_canonical_json(data))
end

ro_field_data_sha256(document) = _ro_field_sha256(canonical_ro_field_data_bytes(document))

"Canonical bytes of the complete field document, including evidence/provenance."
canonical_ro_field_document_bytes(document) =
    _ro_field_utf8_bytes(_ro_field_canonical_json(_ro_field_materialize(document)))

ro_field_artifact_sha256(document) =
    _ro_field_sha256(canonical_ro_field_document_bytes(document))

function _validate_ro_field_storage_document!(document)
    doc = _ro_field_materialize(document)

    # Reuse the API/schema semantic validator when the backend assembly has
    # already loaded it.  This file remains independently testable during the
    # staged P3 integration.
    if isdefined(@__MODULE__, :validate_ro_field_document!)
        try
            getfield(@__MODULE__, :validate_ro_field_document!)(doc)
        catch err
            err isa ROFieldIdentityError && rethrow()
            _ro_field_identity_error(
                :invalid_document,
                "RO-field semantic validation failed: $(sprint(showerror, err))",
            )
        end
    end

    String(_ro_field_identity_get(doc, "schema_version")) == RO_FIELD_SCHEMA_VERSION ||
        _ro_field_identity_error(:unsupported_schema, "only $(RO_FIELD_SCHEMA_VERSION) is supported")

    representation = String(_ro_field_identity_get(doc, "representation"))
    representation in ("sampled_grid", "exact_cell_complex") ||
        _ro_field_identity_error(
            :unsupported_representation,
            "SQLite v0.4 stores sampled_grid and exact_cell_complex artifacts only",
        )

    domain = _ro_field_identity_get(doc, "domain")
    axis_order = _ro_field_identity_vector(domain, "axis_order")
    axes = _ro_field_identity_vector(domain, "axes")
    length(axis_order) == length(axes) && !isempty(axis_order) ||
        _ro_field_identity_error(:invalid_axis_order, "domain axis_order and axes must have equal non-zero length")

    outputs = _ro_field_identity_get(doc, "outputs")
    output_order = _ro_field_identity_vector(outputs, "output_order")
    output_items = _ro_field_identity_vector(outputs, "items")
    length(output_order) == length(output_items) && !isempty(output_order) ||
        _ro_field_identity_error(:invalid_output_order, "output_order and items must have equal non-zero length")

    components = _ro_field_identity_vector(doc, "component_order")
    length(components) == length(axis_order) * length(output_order) ||
        _ro_field_identity_error(:invalid_component_order, "component_order is not the output/input Cartesian product")

    coverage = _ro_field_identity_get(doc, "coverage")
    eligible = _ro_field_identity_int(coverage, "eligible_count")
    evaluated = _ro_field_identity_int(coverage, "evaluated_count")
    valid = _ro_field_identity_int(coverage, "valid_count")
    invalid = _ro_field_identity_int(coverage, "invalid_count")
    omitted = _ro_field_identity_int(coverage, "omitted_count")
    minimum((eligible, evaluated, valid, invalid, omitted)) >= 0 ||
        _ro_field_identity_error(:invalid_coverage, "coverage counts must be non-negative")
    evaluated == valid + invalid ||
        _ro_field_identity_error(:invalid_coverage, "evaluated_count must equal valid_count + invalid_count")
    eligible == evaluated + omitted ||
        _ro_field_identity_error(:invalid_coverage, "eligible_count must equal evaluated_count + omitted_count")

    storage = _ro_field_identity_get(coverage, "storage")
    String(_ro_field_identity_get(storage, "mode")) == "inline" ||
        _ro_field_identity_error(:unsupported_storage_mode, "only inline RO-field storage is implemented")
    isempty(_ro_field_identity_vector(storage, "artifacts")) ||
        _ro_field_identity_error(:invalid_storage, "inline storage cannot carry external artifact references")

    data_bytes = canonical_ro_field_data_bytes(doc)
    expected_length = _ro_field_identity_int(storage, "payload_bytes")
    expected_length == length(data_bytes) || _ro_field_identity_error(
        :payload_length_mismatch,
        "coverage.storage.payload_bytes does not match canonical inline data bytes",
    )
    expected_hash = _ro_field_identity_sha256_text(
        _ro_field_identity_get(storage, "content_sha256"),
        "coverage.storage.content_sha256",
    )
    actual_hash = _ro_field_sha256(data_bytes)
    expected_hash == actual_hash || _ro_field_identity_error(
        :payload_hash_mismatch,
        "coverage.storage.content_sha256 does not match canonical inline data bytes",
    )

    stored_count = _ro_field_identity_int(storage, "stored_count")
    0 <= stored_count <= evaluated ||
        _ro_field_identity_error(:invalid_coverage, "stored_count must be between zero and evaluated_count")

    provenance = _ro_field_identity_get(doc, "provenance")
    _ro_field_identity_sha256_text(
        _ro_field_identity_get(provenance, "network_ir_sha256"),
        "provenance.network_ir_sha256",
    )
    _ro_field_identity_sha256_text(
        _ro_field_identity_get(provenance, "domain_sha256"),
        "provenance.domain_sha256",
    )

    return doc
end

function validate_ro_cell_complex_identity_eligibility!(document)
    doc = _validate_ro_field_storage_document!(document)
    String(_ro_field_identity_get(doc, "representation")) == "exact_cell_complex" ||
        _ro_field_identity_error(:not_exact_cell_complex, "RPB2 accepts exact_cell_complex only")
    !_ro_field_identity_bool(doc, "partial") ||
        _ro_field_identity_error(:partial_artifact, "RPB2 requires partial=false")

    coverage = _ro_field_identity_get(doc, "coverage")
    _ro_field_identity_bool(coverage, "enumeration_complete") ||
        _ro_field_identity_error(:incomplete_enumeration, "RPB2 requires complete enumeration")
    !_ro_field_identity_bool(coverage, "truncated") ||
        _ro_field_identity_error(:truncated_artifact, "RPB2 rejects truncated enumeration")
    _ro_field_identity_int(coverage, "invalid_count") == 0 ||
        _ro_field_identity_error(:invalid_evidence, "RPB2 requires invalid_count=0")
    _ro_field_identity_int(coverage, "omitted_count") == 0 ||
        _ro_field_identity_error(:omitted_evidence, "RPB2 requires omitted_count=0")

    storage = _ro_field_identity_get(coverage, "storage")
    _ro_field_identity_bool(storage, "complete") ||
        _ro_field_identity_error(:incomplete_storage, "RPB2 requires complete storage")

    data = _ro_field_identity_get(doc, "data")
    isempty(_ro_field_identity_vector(data, "gaps")) ||
        _ro_field_identity_error(:has_gaps, "RPB2 rejects cell complexes with gaps")
    isempty(_ro_field_identity_vector(data, "singular_strata")) ||
        _ro_field_identity_error(:has_singular_strata, "RPB2 rejects singular strata")
    isempty(_ro_field_identity_vector(data, "singular_stratum_order")) ||
        _ro_field_identity_error(:has_singular_strata, "RPB2 rejects singular-stratum identities")
    for cell in _ro_field_identity_vector(data, "cells")
        _ro_field_identity_bool(cell, "set_valued") && _ro_field_identity_error(
            :set_valued_cell,
            "RPB2 requires every cell to have one affine value",
        )
        length(_ro_field_identity_vector(cell, "affine_labels")) == 1 ||
            _ro_field_identity_error(
                :set_valued_cell,
                "RPB2 requires exactly one affine label per cell",
            )
    end
    return doc
end

function _ro_field_identity_exact_data(data)
    canonical = _ro_field_materialize(data)
    for collection_name in ("cells", "facets")
        for item in _ro_field_identity_vector(canonical, collection_name)
            item isa AbstractDict || _ro_field_identity_error(
                :invalid_document,
                "exact cell-complex $(collection_name) items must be objects",
            )
            # The H-representation is an optional redundant rendering of the
            # canonical vertex/endpoint geometry.  Including it would make one
            # scientific complex acquire two identities solely because a
            # serializer chose to attach (or omit) redundant halfspaces.
            pop!(item, "polyhedron", nothing)
            pop!(item, :polyhedron, nothing)
        end
    end
    return canonical
end

function canonical_ro_cell_complex_payload(document)
    doc = validate_ro_cell_complex_identity_eligibility!(document)
    return Dict{String, Any}(
        "schema_version" => _ro_field_identity_get(doc, "schema_version"),
        "domain" => _ro_field_identity_get(doc, "domain"),
        "outputs" => _ro_field_identity_get(doc, "outputs"),
        "component_order" => _ro_field_identity_get(doc, "component_order"),
        "data" => _ro_field_identity_exact_data(_ro_field_identity_get(doc, "data")),
    )
end

function _ro_field_varuint_push!(buffer::Vector{UInt8}, value::Integer)
    value >= 0 || _ro_field_identity_error(:invalid_codec_value, "varuint must be non-negative")
    current = UInt64(value)
    while current >= 0x80
        push!(buffer, UInt8((current & 0x7f) | 0x80))
        current >>= 7
    end
    push!(buffer, UInt8(current))
    return buffer
end

function _ro_field_varuint_read(bytes::Vector{UInt8}, position::Int)
    start = position
    shift = 0
    value = UInt64(0)
    while true
        position <= length(bytes) || _ro_field_identity_error(:truncated_blob, "unexpected end of RPB2 varuint")
        byte = bytes[position]
        position += 1
        shift <= 63 || _ro_field_identity_error(:invalid_blob, "RPB2 varuint is too large")
        value |= UInt64(byte & 0x7f) << shift
        if (byte & 0x80) == 0
            value <= UInt64(typemax(Int)) || _ro_field_identity_error(:invalid_blob, "RPB2 varuint exceeds Int")
            decoded = Int(value)
            canonical = UInt8[]
            _ro_field_varuint_push!(canonical, decoded)
            bytes[start:(position - 1)] == canonical ||
                _ro_field_identity_error(:noncanonical_blob, "RPB2 varuint is not minimally encoded")
            return decoded, position
        end
        shift += 7
    end
end

function _ro_field_push_bytes!(buffer::Vector{UInt8}, bytes::Vector{UInt8})
    _ro_field_varuint_push!(buffer, length(bytes))
    append!(buffer, bytes)
    return buffer
end

function _ro_field_read_bytes(bytes::Vector{UInt8}, position::Int, label::AbstractString)
    length_value, position = _ro_field_varuint_read(bytes, position)
    stop = position + length_value - 1
    stop <= length(bytes) || _ro_field_identity_error(:truncated_blob, "unexpected end of RPB2 $(label)")
    return bytes[position:stop], stop + 1
end

function encode_ro_cell_complex_blob(document)
    payload = canonical_ro_cell_complex_payload(document)
    payload_bytes = _ro_field_utf8_bytes(_ro_field_canonical_json(payload))
    identity_bytes = _ro_field_utf8_bytes(RO_CELL_COMPLEX_IDENTITY_KIND)

    buffer = copy(RO_CELL_COMPLEX_MAGIC)
    _ro_field_varuint_push!(buffer, RO_CELL_COMPLEX_CODEC_VERSION)
    _ro_field_push_bytes!(buffer, identity_bytes)
    _ro_field_push_bytes!(buffer, payload_bytes)
    return buffer
end

function decode_ro_cell_complex_blob(blob)
    bytes = try
        UInt8.(collect(blob))
    catch
        _ro_field_identity_error(:invalid_blob, "RPB2 blob must be a byte vector")
    end
    length(bytes) >= length(RO_CELL_COMPLEX_MAGIC) ||
        _ro_field_identity_error(:invalid_magic, "RPB2 blob is too short")
    bytes[1:length(RO_CELL_COMPLEX_MAGIC)] == RO_CELL_COMPLEX_MAGIC ||
        _ro_field_identity_error(:invalid_magic, "invalid RPB2 magic header")

    position = length(RO_CELL_COMPLEX_MAGIC) + 1
    version, position = _ro_field_varuint_read(bytes, position)
    version == RO_CELL_COMPLEX_CODEC_VERSION || _ro_field_identity_error(
        :unsupported_codec_version,
        "unsupported RPB2 codec version $(version)",
    )
    identity_bytes, position = _ro_field_read_bytes(bytes, position, "identity kind")
    identity = try
        String(identity_bytes)
    catch
        _ro_field_identity_error(:invalid_blob, "RPB2 identity kind is not valid UTF-8")
    end
    identity == RO_CELL_COMPLEX_IDENTITY_KIND || _ro_field_identity_error(
        :unsupported_identity,
        "unsupported RPB2 identity kind $(repr(identity))",
    )

    payload_bytes, position = _ro_field_read_bytes(bytes, position, "payload")
    position == length(bytes) + 1 ||
        _ro_field_identity_error(:trailing_bytes, "RPB2 blob has trailing bytes")
    payload_text = try
        String(payload_bytes)
    catch
        _ro_field_identity_error(:invalid_blob, "RPB2 payload is not valid UTF-8")
    end
    payload = try
        _ro_field_materialize(JSON3.read(payload_text))
    catch err
        _ro_field_identity_error(:invalid_json, "RPB2 payload is not JSON: $(sprint(showerror, err))")
    end
    payload isa AbstractDict ||
        _ro_field_identity_error(:invalid_payload, "RPB2 payload must be an object")
    Set(String.(keys(payload))) == Set(["schema_version", "domain", "outputs", "component_order", "data"]) ||
        _ro_field_identity_error(:invalid_payload, "RPB2 payload has an unexpected key set")
    _ro_field_canonical_json(payload) == payload_text ||
        _ro_field_identity_error(:noncanonical_blob, "RPB2 payload is not canonical JSON")
    String(_ro_field_identity_get(payload, "schema_version")) == RO_FIELD_SCHEMA_VERSION ||
        _ro_field_identity_error(:unsupported_schema, "RPB2 payload uses an unsupported field schema")
    isdefined(@__MODULE__, :validate_ro_field_payload!) ||
        _ro_field_identity_error(
            :validator_unavailable,
            "RPB2 payload semantics cannot be trusted before the RO-field validator is loaded",
        )
    try
        getfield(@__MODULE__, :validate_ro_field_payload!)(payload)
    catch err
        err isa ROFieldIdentityError && rethrow()
        _ro_field_identity_error(
            :invalid_payload,
            "RPB2 payload semantic validation failed: $(sprint(showerror, err))",
        )
    end
    return payload
end

ro_cell_complex_hash(blob::AbstractVector{UInt8}) = _ro_field_sha256(UInt8.(blob))
ro_cell_complex_hash(document) = ro_cell_complex_hash(encode_ro_cell_complex_blob(document))
